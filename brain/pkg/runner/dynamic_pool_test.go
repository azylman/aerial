package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockVoiceSession struct {
	sessID    string
	targetKey string
}

func (s *mockVoiceSession) Send(prompt string, turn *TurnContext) error {
	return nil
}

func (s *mockVoiceSession) SessionID() string {
	return s.sessID
}

type mockVoicePool struct {
	mu               sync.Mutex
	model            string
	prewarmedTargets []string
	initCalled       int32
	initErr          error
	initDelay        time.Duration
	closeCalled      int32
	closeDelay       time.Duration
	markDirtyCalled  int32
	sessions         map[string]*mockVoiceSession
}

func newMockVoicePool(model string) *mockVoicePool {
	return &mockVoicePool{
		model:    model,
		sessions: make(map[string]*mockVoiceSession),
	}
}

func (m *mockVoicePool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok := m.sessions[targetKey]; ok {
		return sess, nil
	}
	sid := sessionID
	if sid == "" {
		sid = targetKey
	}
	s := &mockVoiceSession{sessID: sid, targetKey: targetKey}
	m.sessions[targetKey] = s
	return s, nil
}

func (m *mockVoicePool) EvictSession(targetKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, targetKey)
	return nil
}

func (m *mockVoicePool) Initialize(ctx context.Context) error {
	atomic.AddInt32(&m.initCalled, 1)
	if m.initDelay > 0 {
		select {
		case <-time.After(m.initDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m.initErr
}

func (m *mockVoicePool) Close() error {
	atomic.AddInt32(&m.closeCalled, 1)
	if m.closeDelay > 0 {
		time.Sleep(m.closeDelay)
	}
	return nil
}

func (m *mockVoicePool) Model() string {
	return m.model
}

func (m *mockVoicePool) UpdatePrewarmedTargets(targets []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prewarmedTargets = append([]string(nil), targets...)
}

func (m *mockVoicePool) MarkDirty() {
	atomic.AddInt32(&m.markDirtyCalled, 1)
}

func TestDynamicVoicePool_Passthrough(t *testing.T) {
	t.Parallel()

	mock1 := newMockVoicePool("gemini-2.5-flash")
	factory := func() (AgentPool, string, error) {
		return mock1, "fp-1", nil
	}

	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	if dyn.Model() != "gemini-2.5-flash" {
		t.Fatalf("expected Model 'gemini-2.5-flash', got %q", dyn.Model())
	}
	if dyn.Fingerprint() != "fp-1" {
		t.Fatalf("expected Fingerprint 'fp-1', got %q", dyn.Fingerprint())
	}

	ctx := context.Background()
	if err := dyn.Initialize(ctx); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}
	if atomic.LoadInt32(&mock1.initCalled) != 1 {
		t.Fatalf("expected mock1.Initialize called once, got %d", atomic.LoadInt32(&mock1.initCalled))
	}

	dyn.UpdatePrewarmedTargets([]string{"kiosk-kitchen", "kiosk-living-room"})
	mock1.mu.Lock()
	targets := append([]string(nil), mock1.prewarmedTargets...)
	mock1.mu.Unlock()
	if len(targets) != 2 || targets[0] != "kiosk-kitchen" || targets[1] != "kiosk-living-room" {
		t.Fatalf("unexpected prewarmed targets forwarded: %v", targets)
	}

	sess, err := dyn.GetOrCreateSession(ctx, "kiosk-kitchen", "sess-k1")
	if err != nil {
		t.Fatalf("unexpected GetOrCreateSession error: %v", err)
	}
	if sess.SessionID() != "sess-k1" {
		t.Fatalf("expected session ID 'sess-k1', got %q", sess.SessionID())
	}

	if err := dyn.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	if atomic.LoadInt32(&mock1.closeCalled) != 1 {
		t.Fatalf("expected mock1.Close called once, got %d", atomic.LoadInt32(&mock1.closeCalled))
	}
}

func TestDynamicVoicePool_MarkDirty_NoopWhenUnchanged(t *testing.T) {
	t.Parallel()

	mock1 := newMockVoicePool("model-v1")
	factory := func() (AgentPool, string, error) {
		return mock1, "fingerprint-fixed", nil
	}

	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	if dyn.CurrentPool() != mock1 {
		t.Fatalf("expected CurrentPool to return mock1")
	}

	// Trigger MarkDirty with identical fingerprint
	dyn.MarkDirty()

	// Should forward MarkDirty to mock1 without swapping
	if atomic.LoadInt32(&mock1.markDirtyCalled) != 1 {
		t.Fatalf("expected mock1.MarkDirty called once, got %d", atomic.LoadInt32(&mock1.markDirtyCalled))
	}
	if dyn.CurrentPool() != mock1 {
		t.Fatalf("expected CurrentPool to remain mock1")
	}
	if atomic.LoadInt32(&mock1.closeCalled) != 0 {
		t.Fatalf("expected mock1.Close NOT called, got %d", atomic.LoadInt32(&mock1.closeCalled))
	}
}

func TestDynamicVoicePool_MarkDirty_SwapsOnConfigChange(t *testing.T) {
	t.Parallel()

	mock1 := newMockVoicePool("model-v1")
	mock2 := newMockVoicePool("model-v2")

	var currentMu sync.Mutex
	activeMock := mock1
	activeFP := "fp-v1"

	factory := func() (AgentPool, string, error) {
		currentMu.Lock()
		defer currentMu.Unlock()
		return activeMock, activeFP, nil
	}

	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	if dyn.CurrentPool() != mock1 {
		t.Fatalf("expected initial pool to be mock1")
	}

	// Update factory target to mock2 with new fingerprint
	currentMu.Lock()
	activeMock = mock2
	activeFP = "fp-v2"
	currentMu.Unlock()

	// Trigger MarkDirty
	dyn.MarkDirty()

	if dyn.CurrentPool() != mock2 {
		t.Fatalf("expected active pool to swap to mock2, got %p", dyn.CurrentPool())
	}
	if dyn.Fingerprint() != "fp-v2" {
		t.Fatalf("expected fingerprint 'fp-v2', got %q", dyn.Fingerprint())
	}
	if atomic.LoadInt32(&mock2.initCalled) != 1 {
		t.Fatalf("expected candidate mock2 to be initialized, got %d", atomic.LoadInt32(&mock2.initCalled))
	}

	// Wait briefly for asynchronous retirement of old pool
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&mock1.closeCalled) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&mock1.closeCalled) != 1 {
		t.Fatalf("expected old pool mock1 to be retired and closed, got %d", atomic.LoadInt32(&mock1.closeCalled))
	}
}

func TestDynamicVoicePool_MarkDirty_RetainsLKGPOnFailure(t *testing.T) {
	t.Parallel()

	mockGood := newMockVoicePool("model-good")
	mockFailing := newMockVoicePool("model-failing")
	mockFailing.initErr = errors.New("mock initialization failure")

	var currentMu sync.Mutex
	activeMock := mockGood
	activeFP := "fp-good"
	var factoryErr error

	factory := func() (AgentPool, string, error) {
		currentMu.Lock()
		defer currentMu.Unlock()
		if factoryErr != nil {
			return nil, "", factoryErr
		}
		return activeMock, activeFP, nil
	}

	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	// Case 1: Candidate initialization fails
	currentMu.Lock()
	activeMock = mockFailing
	activeFP = "fp-bad"
	currentMu.Unlock()

	dyn.MarkDirty()

	// Should retain LKGP
	if dyn.CurrentPool() != mockGood {
		t.Fatalf("expected LKGP mockGood to be retained, got %p", dyn.CurrentPool())
	}
	if dyn.Fingerprint() != "fp-good" {
		t.Fatalf("expected fingerprint 'fp-good' retained, got %q", dyn.Fingerprint())
	}
	if atomic.LoadInt32(&mockGood.closeCalled) != 0 {
		t.Fatalf("expected mockGood.Close NOT called")
	}
	if atomic.LoadInt32(&mockFailing.closeCalled) != 1 {
		t.Fatalf("expected failing candidate to be closed and cleaned up, got %d", atomic.LoadInt32(&mockFailing.closeCalled))
	}

	// Case 2: Factory returns error
	currentMu.Lock()
	factoryErr = errors.New("cannot create pool")
	currentMu.Unlock()

	dyn.MarkDirty()

	if dyn.CurrentPool() != mockGood {
		t.Fatalf("expected LKGP mockGood to be retained on factory error, got %p", dyn.CurrentPool())
	}
}

func TestDynamicVoicePool_ConcurrentAccessDuringReload(t *testing.T) {
	t.Parallel()

	mockA := newMockVoicePool("model-A")
	mockB := newMockVoicePool("model-B")

	var toggle atomic.Int32
	factory := func() (AgentPool, string, error) {
		if toggle.Load()%2 == 0 {
			return mockA, "fp-A", nil
		}
		return mockB, "fp-B", nil
	}

	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	const numWorkers = 25
	const numIterations = 50
	var wg sync.WaitGroup

	ctx := context.Background()

	// Readers requesting sessions concurrently
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				target := fmt.Sprintf("target-%d", (workerID+j)%5)
				sess, err := dyn.GetOrCreateSession(ctx, target, "")
				if err != nil {
					t.Errorf("worker %d unexpected GetOrCreateSession error: %v", workerID, err)
					return
				}
				if sess == nil {
					t.Errorf("worker %d got nil session", workerID)
					return
				}
			}
		}(i)
	}

	// Concurrent reloader triggering MarkDirty
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 20; j++ {
			toggle.Add(1)
			dyn.MarkDirty()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
}

func TestDynamicVoicePool_SerializedReloads(t *testing.T) {
	t.Parallel()

	var activeReloads int32
	var maxConcurrentReloads int32

	factory := func() (AgentPool, string, error) {
		cur := atomic.AddInt32(&activeReloads, 1)
		defer atomic.AddInt32(&activeReloads, -1)

		// Record max concurrency
		for {
			oldMax := atomic.LoadInt32(&maxConcurrentReloads)
			if cur <= oldMax || atomic.CompareAndSwapInt32(&maxConcurrentReloads, oldMax, cur) {
				break
			}
		}

		time.Sleep(10 * time.Millisecond)
		return newMockVoicePool("pool"), fmt.Sprintf("fp-%d", time.Now().UnixNano()), nil
	}

	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dyn.MarkDirty()
		}()
	}

	wg.Wait()

	if maxConcurrentReloads > 1 {
		t.Fatalf("expected serialized reloads (max 1), got %d concurrent reloads", maxConcurrentReloads)
	}
}

func TestDynamicVoicePool_GracefulShutdownDrainsRetiring(t *testing.T) {
	t.Parallel()

	mock1 := newMockVoicePool("model-v1")
	mock1.closeDelay = 50 * time.Millisecond
	mock2 := newMockVoicePool("model-v2")

	var currentMu sync.Mutex
	activeMock := mock1
	activeFP := "fp-v1"

	factory := func() (AgentPool, string, error) {
		currentMu.Lock()
		defer currentMu.Unlock()
		return activeMock, activeFP, nil
	}

	dyn := NewDynamicVoicePool(factory)

	// Swap to mock2, retiring mock1 with a 50ms closeDelay
	currentMu.Lock()
	activeMock = mock2
	activeFP = "fp-v2"
	currentMu.Unlock()

	dyn.MarkDirty()

	// Immediately close the DynamicVoicePool
	start := time.Now()
	if err := dyn.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	elapsed := time.Since(start)

	// Close must have waited for mock1's retiringWg to complete
	if atomic.LoadInt32(&mock1.closeCalled) != 1 {
		t.Fatalf("expected mock1 to be closed")
	}
	if atomic.LoadInt32(&mock2.closeCalled) != 1 {
		t.Fatalf("expected mock2 to be closed")
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected Close() to block for retiring pool drain, finished in %v", elapsed)
	}
}

func TestDynamicVoicePool_NilAndClosedSafety(t *testing.T) {
	t.Parallel()

	var nilPool *DynamicVoicePool

	if err := nilPool.Close(); err != nil {
		t.Errorf("expected nil from nilPool.Close()")
	}
	if nilPool.Model() != "" {
		t.Errorf("expected empty string from nilPool.Model()")
	}
	if nilPool.Fingerprint() != "" {
		t.Errorf("expected empty string from nilPool.Fingerprint()")
	}
	if nilPool.CurrentPool() != nil {
		t.Errorf("expected nil from nilPool.CurrentPool()")
	}
	nilPool.UpdatePrewarmedTargets([]string{"t"}) // Should not panic
	nilPool.MarkDirty()                           // Should not panic

	ctx := context.Background()
	if _, err := nilPool.GetOrCreateSession(ctx, "k", ""); err == nil {
		t.Errorf("expected error from nilPool.GetOrCreateSession()")
	}
	if err := nilPool.Initialize(ctx); err == nil {
		t.Errorf("expected error from nilPool.Initialize()")
	}

	// Closed pool safety
	mock := newMockVoicePool("model")
	dyn := NewDynamicVoicePool(func() (AgentPool, string, error) {
		return mock, "fp", nil
	})
	if err := dyn.Close(); err != nil {
		t.Fatalf("unexpected error closing pool: %v", err)
	}

	if _, err := dyn.GetOrCreateSession(ctx, "k", ""); err == nil {
		t.Errorf("expected error from closed dyn.GetOrCreateSession()")
	}
	if err := dyn.Initialize(ctx); err == nil {
		t.Errorf("expected error from closed dyn.Initialize()")
	}
	dyn.MarkDirty() // Should not panic
	if err := dyn.Close(); err != nil {
		t.Errorf("expected nil from repeated dyn.Close()")
	}
}

type plainPoolWithoutModel struct{}

func (p *plainPoolWithoutModel) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	return nil, nil
}
func (p *plainPoolWithoutModel) EvictSession(targetKey string) error { return nil }
func (p *plainPoolWithoutModel) Initialize(ctx context.Context) error { return nil }
func (p *plainPoolWithoutModel) Close() error                         { return nil }

type errClosePool struct {
	mockVoicePool
}

func (p *errClosePool) Close() error {
	return errors.New("close error")
}

func TestDynamicVoicePool_EdgeCasesAndUncoveredBranches(t *testing.T) {
	t.Parallel()

	// 1. Factory returning error on NewDynamicVoicePool
	errFactory := func() (AgentPool, string, error) {
		return nil, "", errors.New("factory init fail")
	}
	dynErr := NewDynamicVoicePool(errFactory)
	if dynErr.CurrentPool() != nil {
		t.Errorf("expected nil CurrentPool when factory fails")
	}
	ctx := context.Background()
	if _, err := dynErr.GetOrCreateSession(ctx, "k", ""); err == nil {
		t.Errorf("expected error from GetOrCreateSession with nil pool")
	}
	if err := dynErr.Initialize(ctx); err == nil {
		t.Errorf("expected error from Initialize with nil pool")
	}
	if dynErr.Model() != "" {
		t.Errorf("expected empty string from Model with nil pool")
	}
	dynErr.UpdatePrewarmedTargets([]string{"k"}) // no-op, should not panic

	// 2. Model() with pool that does not implement Model()
	dynPlain := NewDynamicVoicePoolWithInitial(&plainPoolWithoutModel{}, "fp", nil)
	if dynPlain.Model() != "" {
		t.Errorf("expected empty string from Model() on pool without Model method")
	}

	// 3. MarkDirty with nil factory
	mock := newMockVoicePool("m")
	dynNoFactory := NewDynamicVoicePoolWithInitial(mock, "fp", nil)
	dynNoFactory.MarkDirty()
	if atomic.LoadInt32(&mock.markDirtyCalled) != 1 {
		t.Errorf("expected mock.MarkDirty to be called when factory is nil")
	}

	// 4. MarkDirty with nil candidate pool on fingerprint change
	dynNilCandidate := NewDynamicVoicePoolWithInitial(mock, "fp-old", func() (AgentPool, string, error) {
		return nil, "fp-new", nil
	})
	dynNilCandidate.MarkDirty()
	if dynNilCandidate.Fingerprint() != "fp-old" {
		t.Errorf("expected LKGP retention when candidate pool is nil")
	}

	// 5. MarkDirty when candidate pool close errors
	candidateErrClose := &errClosePool{mockVoicePool: *newMockVoicePool("m2")}
	candidateErrClose.initErr = errors.New("candidate init fail")
	dynErrClose := NewDynamicVoicePoolWithInitial(mock, "fp-old", func() (AgentPool, string, error) {
		return candidateErrClose, "fp-new", nil
	})
	dynErrClose.MarkDirty()
	if dynErrClose.Fingerprint() != "fp-old" {
		t.Errorf("expected LKGP retention when candidate pool fails initialization")
	}

	// 6. MarkDirty when pool is closed during candidate initialization
	mockInitBlock := newMockVoicePool("m3")
	mockInitBlock.initDelay = 50 * time.Millisecond
	dynCloseRace := NewDynamicVoicePoolWithInitial(mock, "fp-old", func() (AgentPool, string, error) {
		return mockInitBlock, "fp-new", nil
	})
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = dynCloseRace.Close()
	}()
	dynCloseRace.MarkDirty()
}
