package runner

import (
	"context"
	"errors"
	"fmt"
	"runtime"
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
	onInit           func()
	closeCalled      int32
	closeGate        chan struct{}
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

func (m *mockVoicePool) Initialize(ctx context.Context) error {
	atomic.AddInt32(&m.initCalled, 1)
	if m.onInit != nil {
		m.onInit()
	}
	return m.initErr
}

func (m *mockVoicePool) Close() error {
	atomic.AddInt32(&m.closeCalled, 1)
	if m.closeGate != nil {
		<-m.closeGate
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

type plainPoolWithoutModel struct{}

func (p *plainPoolWithoutModel) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	return nil, nil
}
func (p *plainPoolWithoutModel) Initialize(ctx context.Context) error { return nil }
func (p *plainPoolWithoutModel) Close() error                         { return nil }

type errClosePool struct {
	mockVoicePool
}

func (p *errClosePool) Close() error {
	return errors.New("close error")
}

type mockRotatorVoicePool struct {
	mockVoicePool
	shouldRotate bool
	rotReason    string
	rotatedSess  AgentSession
	rotateErr    error
}

func (m *mockRotatorVoicePool) ShouldRotateSession(sess AgentSession) (bool, string) {
	return m.shouldRotate, m.rotReason
}

func (m *mockRotatorVoicePool) RotateSession(ctx context.Context, targetKey string) (AgentSession, error) {
	return m.rotatedSess, m.rotateErr
}

type mockLeasedVoicePool struct {
	mockVoicePool
	lease    SessionLease
	leaseErr error
}

func (m *mockLeasedVoicePool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	return m.lease, m.leaseErr
}

type fakeSessionLease struct {
	sessID string
}

func (f *fakeSessionLease) SessionID() string         { return f.sessID }
func (f *fakeSessionLease) PreviousSessionID() string { return "" }
func (f *fakeSessionLease) IsCold() bool              { return false }
func (f *fakeSessionLease) IsEphemeral() bool         { return false }
func (f *fakeSessionLease) TurnCount() int            { return 1 }
func (f *fakeSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	return &TurnResult{Response: "fake lease ok"}, nil
}
func (f *fakeSessionLease) CancelRotation() {}
func (f *fakeSessionLease) Release() error  { return nil }

func TestDynamicVoicePool_DelegationAndSafety(t *testing.T) {
	t.Parallel()

	// 1. Nil receiver safety
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
	nilPool.UpdatePrewarmedTargets([]string{"t"})
	nilPool.MarkDirty()

	ctx := context.Background()
	if _, err := nilPool.GetOrCreateSession(ctx, "k", ""); err == nil {
		t.Errorf("expected error from nilPool.GetOrCreateSession()")
	}
	if err := nilPool.Initialize(ctx); err == nil {
		t.Errorf("expected error from nilPool.Initialize()")
	}
	if should, _ := nilPool.ShouldRotateSession(nil); should {
		t.Error("expected nil pool ShouldRotateSession to return false")
	}
	if _, err := nilPool.RotateSession(ctx, "k"); err == nil {
		t.Error("expected nil pool RotateSession to return error")
	}
	if _, err := nilPool.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error from nil pool AcquireLease")
	}

	// 2. Nominal Passthrough
	mock1 := newMockVoicePool("gemini-2.5-flash")
	factory := func() (AgentPool, string, error) {
		return mock1, "fp-1", nil
	}

	dyn := NewDynamicVoicePool(factory)
	if dyn.Model() != "gemini-2.5-flash" {
		t.Fatalf("expected Model 'gemini-2.5-flash', got %q", dyn.Model())
	}
	if dyn.Fingerprint() != "fp-1" {
		t.Fatalf("expected Fingerprint 'fp-1', got %q", dyn.Fingerprint())
	}

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

	// 3. Non-rotator and Non-leased delegation
	if should, _ := dyn.ShouldRotateSession(nil); should {
		t.Error("expected non-rotator pool ShouldRotateSession to return false")
	}
	rot, err := dyn.RotateSession(ctx, "k")
	if err != nil || rot != nil {
		t.Errorf("expected RotateSession on non-rotator to return nil, nil, got (%v, %v)", rot, err)
	}
	if _, err := dyn.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error when underlying does not implement LeasedAgentPool")
	}

	// 4. Closed dynamic pool safety
	if err := dyn.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	if atomic.LoadInt32(&mock1.closeCalled) != 1 {
		t.Fatalf("expected mock1.Close called once, got %d", atomic.LoadInt32(&mock1.closeCalled))
	}
	if _, err := dyn.GetOrCreateSession(ctx, "k", ""); err == nil {
		t.Errorf("expected error from closed dyn.GetOrCreateSession()")
	}
	if err := dyn.Initialize(ctx); err == nil {
		t.Errorf("expected error from closed dyn.Initialize()")
	}
	dyn.MarkDirty()
	if err := dyn.Close(); err != nil {
		t.Errorf("expected nil from repeated dyn.Close()")
	}
	if should, _ := dyn.ShouldRotateSession(sess); should {
		t.Error("expected closed pool ShouldRotateSession to return false")
	}
	if _, err := dyn.RotateSession(ctx, "k"); err == nil {
		t.Error("expected closed pool RotateSession to return error")
	}
	if _, err := dyn.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error on closed dynamic pool AcquireLease")
	}

	// 5. Rotator and Leased pool delegation
	fakeSess := &mockVoiceSession{sessID: "rot-s", targetKey: "k"}
	fakeL := &fakeSessionLease{sessID: "lease-123"}
	rotPool := &mockRotatorVoicePool{
		mockVoicePool: *newMockVoicePool("m2"),
		shouldRotate:  true,
		rotReason:     "test reason",
		rotatedSess:   fakeSess,
	}
	dynRot := NewDynamicVoicePoolWithInitial(rotPool, "fp-2", nil)
	defer dynRot.Close()
	if should, reason := dynRot.ShouldRotateSession(fakeSess); !should || reason != "test reason" {
		t.Errorf("expected ShouldRotateSession to delegate, got (%v, %q)", should, reason)
	}
	rotSess, err := dynRot.RotateSession(ctx, "k")
	if err != nil || rotSess != fakeSess {
		t.Errorf("expected RotateSession to delegate, got (%v, %v)", rotSess, err)
	}

	mockLeased := &mockLeasedVoicePool{
		mockVoicePool: *newMockVoicePool("m3"),
		lease:         fakeL,
	}
	dynLeased := NewDynamicVoicePoolWithInitial(mockLeased, "fp-3", nil)
	defer dynLeased.Close()
	lease, err := dynLeased.AcquireLease(ctx, "k")
	if err != nil {
		t.Fatalf("unexpected AcquireLease error: %v", err)
	}
	if lease != fakeL {
		t.Errorf("expected lease %v, got %v", fakeL, lease)
	}
}

func TestDynamicVoicePool_MarkDirtyAndReloadEdgeCases(t *testing.T) {
	t.Parallel()

	// 1. MarkDirty_NoopWhenUnchanged
	mock1 := newMockVoicePool("model-v1")
	factory := func() (AgentPool, string, error) {
		return mock1, "fingerprint-fixed", nil
	}
	dyn := NewDynamicVoicePool(factory)
	defer dyn.Close()

	if dyn.CurrentPool() != mock1 {
		t.Fatalf("expected CurrentPool to return mock1")
	}
	dyn.MarkDirty()
	if atomic.LoadInt32(&mock1.markDirtyCalled) != 1 {
		t.Fatalf("expected mock1.MarkDirty called once, got %d", atomic.LoadInt32(&mock1.markDirtyCalled))
	}
	if dyn.CurrentPool() != mock1 {
		t.Fatalf("expected CurrentPool to remain mock1")
	}
	if atomic.LoadInt32(&mock1.closeCalled) != 0 {
		t.Fatalf("expected mock1.Close NOT called, got %d", atomic.LoadInt32(&mock1.closeCalled))
	}

	// 2. MarkDirty_SwapsOnConfigChange
	mock2 := newMockVoicePool("model-v2")
	var currentMu sync.Mutex
	activeMock := mock1
	activeFP := "fp-v1"
	swapFactory := func() (AgentPool, string, error) {
		currentMu.Lock()
		defer currentMu.Unlock()
		return activeMock, activeFP, nil
	}
	dynSwap := NewDynamicVoicePool(swapFactory)
	defer dynSwap.Close()

	currentMu.Lock()
	activeMock = mock2
	activeFP = "fp-v2"
	currentMu.Unlock()

	dynSwap.MarkDirty()
	if dynSwap.CurrentPool() != mock2 {
		t.Fatalf("expected active pool to swap to mock2, got %p", dynSwap.CurrentPool())
	}
	if dynSwap.Fingerprint() != "fp-v2" {
		t.Fatalf("expected fingerprint 'fp-v2', got %q", dynSwap.Fingerprint())
	}
	if atomic.LoadInt32(&mock2.initCalled) != 1 {
		t.Fatalf("expected candidate mock2 to be initialized, got %d", atomic.LoadInt32(&mock2.initCalled))
	}
	dynSwap.retiringWg.Wait()
	if atomic.LoadInt32(&mock1.closeCalled) != 1 {
		t.Fatalf("expected old pool mock1 to be retired and closed, got %d", atomic.LoadInt32(&mock1.closeCalled))
	}

	// 3. MarkDirty_RetainsLKGPOnFailure
	mockGood := newMockVoicePool("model-good")
	mockFailing := newMockVoicePool("model-failing")
	mockFailing.initErr = errors.New("mock initialization failure")
	var lkgpMu sync.Mutex
	activeLKGP := mockGood
	activeLKGPFP := "fp-good"
	var factoryErr error
	lkgpFactory := func() (AgentPool, string, error) {
		lkgpMu.Lock()
		defer lkgpMu.Unlock()
		if factoryErr != nil {
			return nil, "", factoryErr
		}
		return activeLKGP, activeLKGPFP, nil
	}
	dynLKGP := NewDynamicVoicePool(lkgpFactory)
	defer dynLKGP.Close()

	lkgpMu.Lock()
	activeLKGP = mockFailing
	activeLKGPFP = "fp-bad"
	lkgpMu.Unlock()

	dynLKGP.MarkDirty()
	if dynLKGP.CurrentPool() != mockGood {
		t.Fatalf("expected LKGP mockGood to be retained, got %p", dynLKGP.CurrentPool())
	}
	if dynLKGP.Fingerprint() != "fp-good" {
		t.Fatalf("expected fingerprint 'fp-good' retained, got %q", dynLKGP.Fingerprint())
	}
	if atomic.LoadInt32(&mockFailing.closeCalled) != 1 {
		t.Fatalf("expected failing candidate to be closed, got %d", atomic.LoadInt32(&mockFailing.closeCalled))
	}

	lkgpMu.Lock()
	factoryErr = errors.New("cannot create pool")
	lkgpMu.Unlock()

	dynLKGP.MarkDirty()
	if dynLKGP.CurrentPool() != mockGood {
		t.Fatalf("expected LKGP mockGood to be retained on factory error, got %p", dynLKGP.CurrentPool())
	}

	// 4. Edge Cases: Factory error on NewDynamicVoicePool
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
	dynErr.UpdatePrewarmedTargets([]string{"k"})

	// Model() with pool that does not implement Model()
	dynPlain := NewDynamicVoicePoolWithInitial(&plainPoolWithoutModel{}, "fp", nil)
	if dynPlain.Model() != "" {
		t.Errorf("expected empty string from Model() on pool without Model method")
	}

	// MarkDirty with nil factory
	mockNoFac := newMockVoicePool("m")
	dynNoFactory := NewDynamicVoicePoolWithInitial(mockNoFac, "fp", nil)
	dynNoFactory.MarkDirty()
	if atomic.LoadInt32(&mockNoFac.markDirtyCalled) != 1 {
		t.Errorf("expected mock.MarkDirty to be called when factory is nil")
	}

	// MarkDirty with nil candidate pool on fingerprint change
	dynNilCandidate := NewDynamicVoicePoolWithInitial(mockNoFac, "fp-old", func() (AgentPool, string, error) {
		return nil, "fp-new", nil
	})
	dynNilCandidate.MarkDirty()
	if dynNilCandidate.Fingerprint() != "fp-old" {
		t.Errorf("expected LKGP retention when candidate pool is nil")
	}

	// MarkDirty when candidate pool close errors
	candidateErrClose := &errClosePool{mockVoicePool: *newMockVoicePool("m2")}
	candidateErrClose.initErr = errors.New("candidate init fail")
	dynErrClose := NewDynamicVoicePoolWithInitial(mockNoFac, "fp-old", func() (AgentPool, string, error) {
		return candidateErrClose, "fp-new", nil
	})
	dynErrClose.MarkDirty()
	if dynErrClose.Fingerprint() != "fp-old" {
		t.Errorf("expected LKGP retention when candidate pool fails initialization")
	}

	// MarkDirty when pool is closed during candidate initialization
	mockInitBlock := newMockVoicePool("m3")
	var dynCloseRace *DynamicVoicePool
	mockInitBlock.onInit = func() {
		if dynCloseRace != nil {
			_ = dynCloseRace.Close()
		}
	}
	dynCloseRace = NewDynamicVoicePoolWithInitial(mockNoFac, "fp-old", func() (AgentPool, string, error) {
		return mockInitBlock, "fp-new", nil
	})
	dynCloseRace.MarkDirty()
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 20; j++ {
			toggle.Add(1)
			dyn.MarkDirty()
			runtime.Gosched()
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

		for {
			oldMax := atomic.LoadInt32(&maxConcurrentReloads)
			if cur <= oldMax || atomic.CompareAndSwapInt32(&maxConcurrentReloads, oldMax, cur) {
				break
			}
		}

		runtime.Gosched()
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
	mock1.closeGate = make(chan struct{})
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

	currentMu.Lock()
	activeMock = mock2
	activeFP = "fp-v2"
	currentMu.Unlock()

	dyn.MarkDirty()

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- dyn.Close()
	}()

	<-closeStarted
	runtime.Gosched()
	close(mock1.closeGate)

	if err := <-closeDone; err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}

	if atomic.LoadInt32(&mock1.closeCalled) != 1 {
		t.Fatalf("expected mock1 to be closed")
	}
	if atomic.LoadInt32(&mock2.closeCalled) != 1 {
		t.Fatalf("expected mock2 to be closed")
	}
}
