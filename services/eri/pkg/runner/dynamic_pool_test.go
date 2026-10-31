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

// Fingerprint returns the current configuration fingerprint under RLock.
func (p *DynamicVoicePool) Fingerprint() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentKey
}

// CurrentPool returns the active AgentPool under RLock for test assertions.
func (p *DynamicVoicePool) CurrentPool() AgentPool {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentPool
}

type mockVoiceSession struct {
	sessID, targetKey string
	sendFn            func(string, *TurnContext) error
}

func (s *mockVoiceSession) SessionID() string { return s.sessID }
func (s *mockVoiceSession) Send(p string, t *TurnContext) error {
	if s.sendFn != nil {
		return s.sendFn(p, t)
	}
	return nil
}

type mockVoicePool struct {
	mu                                       sync.Mutex
	model                                    string
	prewarmedTargets                         []string
	initCalled, closeCalled, markDirtyCalled int32
	initErr, closeErr, getErr                error
	onInit                                   func()
	closeGate                                chan struct{}
	sessions                                 map[string]*mockVoiceSession
	lease                                    SessionLease
}

func newMockVoicePool(m string) *mockVoicePool {
	return &mockVoicePool{model: m, sessions: make(map[string]*mockVoiceSession)}
}

func (m *mockVoicePool) GetOrCreateSession(ctx context.Context, targetKey, sessionID string) (AgentSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, m.getErr
	}
	if sess, ok := m.sessions[targetKey]; ok {
		return sess, nil
	}
	if sessionID == "" {
		sessionID = targetKey
	}
	s := &mockVoiceSession{sessID: sessionID, targetKey: targetKey}
	m.sessions[targetKey] = s
	return s, nil
}

func (m *mockVoicePool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	m.mu.Lock()
	if m.lease != nil || m.getErr != nil {
		l, err := m.lease, m.getErr
		m.mu.Unlock()
		return l, err
	}
	m.mu.Unlock()
	sess, err := m.GetOrCreateSession(ctx, targetKey, "")
	if err != nil {
		return nil, err
	}
	return &fakeSessionLease{sessID: sess.SessionID()}, nil
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
	return m.closeErr
}

func (m *mockVoicePool) MarkDirty() { atomic.AddInt32(&m.markDirtyCalled, 1) }
func (m *mockVoicePool) UpdatePrewarmedTargets(t []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prewarmedTargets = append([]string(nil), t...)
}

type fakeSessionLease struct{ sessID string }

func (f *fakeSessionLease) SessionID() string         { return f.sessID }
func (f *fakeSessionLease) PreviousSessionID() string { return "" }
func (f *fakeSessionLease) IsCold() bool              { return false }
func (f *fakeSessionLease) IsEphemeral() bool         { return false }
func (f *fakeSessionLease) TurnCount() int            { return 1 }
func (f *fakeSessionLease) MarkDirty()                {}
func (f *fakeSessionLease) CancelRotation()           {}
func (f *fakeSessionLease) Release() error            { return nil }
func (f *fakeSessionLease) Execute(context.Context, *TurnContext) (*TurnResult, error) {
	return &TurnResult{Response: "fake lease ok"}, nil
}

type swappablePoolFactory struct {
	mu   sync.Mutex
	pool AgentPool
	fp   string
	err  error
}

func (f *swappablePoolFactory) set(p AgentPool, fp string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pool, f.fp, f.err = p, fp, err
}

func (f *swappablePoolFactory) factory() (AgentPool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pool, f.fp, f.err
}

func TestDynamicVoicePool_DelegationAndSafety(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var nilPool *DynamicVoicePool
	nilPool.UpdatePrewarmedTargets([]string{"t"})
	nilPool.MarkDirty()
	_, lErr := nilPool.AcquireLease(ctx, "k")
	if nilPool.Initialize(ctx) == nil || lErr == nil ||
		nilPool.Close() != nil || nilPool.Fingerprint() != "" || nilPool.CurrentPool() != nil {
		t.Error("unexpected nilPool safety behavior")
	}

	mock1 := newMockVoicePool("gemini-2.5-flash")
	dyn := NewDynamicVoicePool(func() (AgentPool, string, error) { return mock1, "fp-1", nil })
	dyn.UpdatePrewarmedTargets([]string{"kiosk-kitchen", "kiosk-living-room"})
	lease, err := dyn.AcquireLease(ctx, "kiosk-kitchen")

	mock1.mu.Lock()
	targets := append([]string(nil), mock1.prewarmedTargets...)
	mock1.mu.Unlock()

	if dyn.Fingerprint() != "fp-1" || dyn.Initialize(ctx) != nil || atomic.LoadInt32(&mock1.initCalled) != 1 ||
		len(targets) != 2 || targets[0] != "kiosk-kitchen" || targets[1] != "kiosk-living-room" ||
		err != nil || lease.SessionID() != "kiosk-kitchen" {
		t.Fatalf("unexpected delegation/passthrough behavior")
	}

	dyn.Close()
	dyn.MarkDirty()
	_, clErr := dyn.AcquireLease(ctx, "k")
	if atomic.LoadInt32(&mock1.closeCalled) != 1 || dyn.Close() != nil || dyn.Initialize(ctx) == nil || clErr == nil {
		t.Error("expected error on closed dynamic pool methods")
	}

	fakeL := &fakeSessionLease{sessID: "lease-123"}
	mockLeased := newMockVoicePool("m3")
	mockLeased.lease = fakeL
	dynLeased := NewDynamicVoicePoolWithInitial(mockLeased, "fp-3", nil)
	defer func() {
		_ = dynLeased.Close()
	}()

	lease2, leaseErr2 := dynLeased.AcquireLease(ctx, "k")
	if leaseErr2 != nil || lease2 != fakeL {
		t.Errorf("unexpected leased: (%v, %v)", leaseErr2, lease2)
	}
}

func TestDynamicVoicePool_MarkDirtyAndReloadEdgeCases(t *testing.T) {
	t.Parallel()
	mock1, mock2 := newMockVoicePool("model-v1"), newMockVoicePool("model-v2")
	sf := &swappablePoolFactory{pool: mock1, fp: "fp-v1"}
	dyn := NewDynamicVoicePool(sf.factory)
	defer dyn.Close()

	dyn.MarkDirty()
	sf.set(mock2, "fp-v2", nil)
	dyn.MarkDirty()
	dyn.retiringWg.Wait()

	mockGood, mockFailing := newMockVoicePool("model-good"), &mockVoicePool{initErr: errors.New("mock initialization failure")}
	sf.set(mockGood, "fp-good", nil)
	dyn.MarkDirty()
	sf.set(mockFailing, "fp-bad", nil)
	dyn.MarkDirty()
	sf.set(nil, "", errors.New("cannot create pool"))
	dyn.MarkDirty()

	if atomic.LoadInt32(&mock1.markDirtyCalled) != 1 || atomic.LoadInt32(&mock2.initCalled) != 1 || atomic.LoadInt32(&mock1.closeCalled) != 1 ||
		dyn.CurrentPool() != mockGood || dyn.Fingerprint() != "fp-good" || atomic.LoadInt32(&mockFailing.closeCalled) != 1 {
		t.Fatalf("unexpected counts or LKGP retention failure")
	}

	dynErr := NewDynamicVoicePool(func() (AgentPool, string, error) { return nil, "", errors.New("factory init fail") })
	ctx := context.Background()
	_, geErr := dynErr.AcquireLease(ctx, "k")
	dynErr.UpdatePrewarmedTargets([]string{"k"})

	mockNoFac := newMockVoicePool("m")
	NewDynamicVoicePoolWithInitial(mockNoFac, "fp", nil).MarkDirty()
	dNil := NewDynamicVoicePoolWithInitial(mockNoFac, "fp-old", func() (AgentPool, string, error) { return nil, "fp-new", nil })
	dNil.MarkDirty()

	candErrClose := &mockVoicePool{initErr: errors.New("candidate init fail"), closeErr: errors.New("close error")}
	dErr := NewDynamicVoicePoolWithInitial(mockNoFac, "fp-old", func() (AgentPool, string, error) { return candErrClose, "fp-new", nil })
	dErr.MarkDirty()

	mockInitBlock := newMockVoicePool("m3")
	var dynCloseRace *DynamicVoicePool
	mockInitBlock.onInit = func() { _ = dynCloseRace.Close() }
	dynCloseRace = NewDynamicVoicePoolWithInitial(mockNoFac, "fp-old", func() (AgentPool, string, error) { return mockInitBlock, "fp-new", nil })
	dynCloseRace.MarkDirty()

	if dynErr.CurrentPool() != nil || geErr == nil || dynErr.Initialize(ctx) == nil ||
		atomic.LoadInt32(&mockNoFac.markDirtyCalled) != 1 || dNil.Fingerprint() != "fp-old" || dErr.Fingerprint() != "fp-old" {
		t.Errorf("unexpected edge reload behavior")
	}
}

func TestDynamicVoicePool_ConcurrentAccessDuringReload(t *testing.T) {
	t.Parallel()
	mockA, mockB := newMockVoicePool("model-A"), newMockVoicePool("model-B")
	var toggle atomic.Int32
	dyn := NewDynamicVoicePool(func() (AgentPool, string, error) {
		if toggle.Load()%2 == 0 {
			return mockA, "fp-A", nil
		}
		return mockB, "fp-B", nil
	})
	defer dyn.Close()

	const numWorkers, numIterations = 25, 50
	var wg sync.WaitGroup
	ctx := context.Background()
	wg.Add(numWorkers + 1)
	for i := range numWorkers {
		go func(wID int) {
			defer wg.Done()
			for j := range numIterations {
				if l, err := dyn.AcquireLease(ctx, fmt.Sprintf("t-%d", (wID+j)%5)); err != nil || l == nil {
					t.Errorf("worker %d AcquireLease error: %v", wID, err)
					return
				}
			}
		}(i)
	}
	go func() {
		defer wg.Done()
		for range 20 {
			toggle.Add(1)
			dyn.MarkDirty()
			runtime.Gosched()
		}
	}()
	wg.Wait()
}

func TestDynamicVoicePool_SerializedReloadsAndDrain(t *testing.T) {
	t.Parallel()
	var active, maxConc int32
	dyn := NewDynamicVoicePool(func() (AgentPool, string, error) {
		cur := atomic.AddInt32(&active, 1)
		defer atomic.AddInt32(&active, -1)
		for old := atomic.LoadInt32(&maxConc); cur > old && !atomic.CompareAndSwapInt32(&maxConc, old, cur); old = atomic.LoadInt32(&maxConc) {
		}
		runtime.Gosched()
		return newMockVoicePool("pool"), fmt.Sprintf("fp-%d", time.Now().UnixNano()), nil
	})
	defer dyn.Close()

	var wg sync.WaitGroup
	wg.Add(10)
	for range 10 {
		go func() {
			defer wg.Done()
			dyn.MarkDirty()
		}()
	}
	wg.Wait()
	if maxConc > 1 {
		t.Fatalf("expected serialized reloads (max 1), got %d concurrent reloads", maxConc)
	}

	mock1, mock2 := newMockVoicePool("model-v1"), newMockVoicePool("model-v2")
	mock1.closeGate = make(chan struct{})
	sf := &swappablePoolFactory{pool: mock1, fp: "fp-v1"}
	dyn2 := NewDynamicVoicePool(sf.factory)
	sf.set(mock2, "fp-v2", nil)
	dyn2.MarkDirty()

	closeStarted, closeDone := make(chan struct{}), make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- dyn2.Close()
	}()
	<-closeStarted
	runtime.Gosched()
	close(mock1.closeGate)

	if err := <-closeDone; err != nil || atomic.LoadInt32(&mock1.closeCalled) != 1 || atomic.LoadInt32(&mock2.closeCalled) != 1 {
		t.Fatalf("unexpected Close outcome: err=%v, c1=%d, c2=%d", err, mock1.closeCalled, mock2.closeCalled)
	}
}
