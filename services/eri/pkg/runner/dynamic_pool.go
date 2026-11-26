package runner

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/session"
)

// DynamicVoicePoolFactory produces a candidate AgentPool and its configuration fingerprint.
type DynamicVoicePoolFactory func() (AgentPool, string, error)

// DynamicVoicePool wraps an AgentPool, managing seamless, atomic in-process hot-reloads.
type DynamicVoicePool struct {
	mu          sync.RWMutex
	reloadMu    sync.Mutex
	currentPool AgentPool
	currentKey  string
	factory     DynamicVoicePoolFactory
	retiringWg  sync.WaitGroup
	closed      bool
	draining    atomic.Bool

	transcriptRescuer    func(context.Context, string, time.Time) string
	sessionManager       *session.Manager
	acquireSessionRecord func(context.Context, string) (SessionRecord, error)
	getSessionRecord     func(context.Context, string) (SessionRecord, error)
	onSessionRotated     func(context.Context, string, string, string) error
	onSessionRotatedCAS  func(context.Context, string, string, string, int64) error
}

var _ AgentPool = (*DynamicVoicePool)(nil)

// NewDynamicVoicePool constructs a DynamicVoicePool using the provided factory.
func NewDynamicVoicePool(factory DynamicVoicePoolFactory) *DynamicVoicePool {
	var initialPool AgentPool
	var initialKey string
	if factory != nil {
		if pool, key, err := factory(); err == nil {
			initialPool, initialKey = pool, key
		} else {
			log.Printf("[DynamicVoicePool] Warning: initial factory invocation failed: %v", err)
		}
	}
	return NewDynamicVoicePoolWithInitial(initialPool, initialKey, factory)
}

// NewDynamicVoicePoolWithInitial constructs a DynamicVoicePool with an explicitly supplied initial pool and fingerprint.
func NewDynamicVoicePoolWithInitial(initialPool AgentPool, initialKey string, factory DynamicVoicePoolFactory) *DynamicVoicePool {
	p := &DynamicVoicePool{currentPool: initialPool, currentKey: initialKey, factory: factory}
	p.applyDependenciesToPool(initialPool)
	return p
}

// Underlying returns the wrapped UnifiedProcessPool, or nil.
func (p *DynamicVoicePool) Underlying() *UnifiedProcessPool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.currentPool == nil {
		return nil
	}
	if u, ok := p.currentPool.(*UnifiedProcessPool); ok {
		return u
	}
	if u, ok := p.currentPool.(interface{ Underlying() *UnifiedProcessPool }); ok {
		return u.Underlying()
	}
	return nil
}

func (p *DynamicVoicePool) applyDependenciesToPool(target AgentPool) {
	if target == nil {
		return
	}
	if p.transcriptRescuer != nil {
		if s, ok := target.(interface {
			SetTranscriptRescuer(rescuer func(ctx context.Context, sessionID string, since time.Time) string)
		}); ok {
			s.SetTranscriptRescuer(p.transcriptRescuer)
		}
	}
	if p.sessionManager != nil {
		if s, ok := target.(interface{ SetSessionManager(sm *session.Manager) }); ok {
			s.SetSessionManager(p.sessionManager)
		}
	}
	if p.acquireSessionRecord != nil || p.onSessionRotatedCAS != nil {
		switch s := target.(type) {
		case interface {
			SetSessionCallbacksCAS(acqRec, getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRotCAS func(ctx context.Context, targetKey, oldSessionID, newSessionID string, leaseGen int64) error)
		}:
			s.SetSessionCallbacksCAS(p.acquireSessionRecord, p.getSessionRecord, p.onSessionRotatedCAS)
			return
		case interface {
			SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error)
		}:
			s.SetSessionCallbacks(p.getSessionRecord, p.onSessionRotated)
			return
		}
	}
	if p.getSessionRecord != nil || p.onSessionRotated != nil {
		if s, ok := target.(interface {
			SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error)
		}); ok {
			s.SetSessionCallbacks(p.getSessionRecord, p.onSessionRotated)
		}
	}
}

// SetTranscriptRescuer configures a transcript rescuer callback and applies it to the active pool.
func (p *DynamicVoicePool) SetTranscriptRescuer(r func(context.Context, string, time.Time) string) {
	p.mu.Lock()
	p.transcriptRescuer = r
	pool := p.currentPool
	p.mu.Unlock()
	if pool != nil {
		if s, ok := pool.(interface {
			SetTranscriptRescuer(rescuer func(ctx context.Context, sessionID string, since time.Time) string)
		}); ok {
			s.SetTranscriptRescuer(r)
		}
	}
}

// SetSessionManager updates the session manager for rotation guardrail checks.
func (p *DynamicVoicePool) SetSessionManager(sm *session.Manager) {
	p.mu.Lock()
	p.sessionManager = sm
	pool := p.currentPool
	p.mu.Unlock()
	if pool != nil {
		if s, ok := pool.(interface{ SetSessionManager(mgr *session.Manager) }); ok {
			s.SetSessionManager(sm)
		}
	}
}

// SetSessionCallbacks updates the session record lookup and rotation callbacks.
func (p *DynamicVoicePool) SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error) {
	p.mu.Lock()
	p.acquireSessionRecord = nil
	p.getSessionRecord = getRec
	p.onSessionRotated = onRot
	p.onSessionRotatedCAS = nil
	pool := p.currentPool
	p.mu.Unlock()
	if pool != nil {
		if s, ok := pool.(interface {
			SetSessionCallbacks(getFn func(ctx context.Context, targetKey string) (SessionRecord, error), onRotFn func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error)
		}); ok {
			s.SetSessionCallbacks(getRec, onRot)
		}
	}
}

// SetSessionCallbacksCAS updates the session record lookup and CAS rotation callbacks.
func (p *DynamicVoicePool) SetSessionCallbacksCAS(acqRec, getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRotCAS func(ctx context.Context, targetKey, oldSessionID, newSessionID string, leaseGen int64) error) {
	var fallbackRot func(context.Context, string, string, string) error
	if onRotCAS != nil {
		fallbackRot = func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			return onRotCAS(ctx, targetKey, oldSessionID, newSessionID, 0)
		}
	}
	p.mu.Lock()
	p.acquireSessionRecord = acqRec
	p.getSessionRecord = getRec
	p.onSessionRotated = fallbackRot
	p.onSessionRotatedCAS = onRotCAS
	pool := p.currentPool
	p.mu.Unlock()
	if pool != nil {
		switch s := pool.(type) {
		case interface {
			SetSessionCallbacksCAS(acqFn, getFn func(context.Context, string) (SessionRecord, error), onRotFn func(context.Context, string, string, string, int64) error)
		}:
			s.SetSessionCallbacksCAS(acqRec, getRec, onRotCAS)
		case interface {
			SetSessionCallbacks(getFn func(context.Context, string) (SessionRecord, error), onRotFn func(context.Context, string, string, string) error)
		}:
			s.SetSessionCallbacks(getRec, fallbackRot)
		}
	}
}

// UpdateModel updates the model used for newly spawned or rotated daemons in the pool.
func (p *DynamicVoicePool) UpdateModel(model string) {
	p.mu.RLock()
	pool := p.currentPool
	p.mu.RUnlock()
	if pool != nil {
		if u, ok := pool.(interface{ UpdateModel(model string) }); ok {
			u.UpdateModel(model)
		}
	}
}

// UpdateEngine updates the engine and CLI binaries used for newly spawned or rotated daemons in the pool.
func (p *DynamicVoicePool) UpdateEngine(engine, claudeBin string, extraBins ...string) {
	p.mu.RLock()
	pool := p.currentPool
	p.mu.RUnlock()
	if pool != nil {
		if u, ok := pool.(interface {
			UpdateEngine(engine, claudeBin string, extraBins ...string)
		}); ok {
			u.UpdateEngine(engine, claudeBin, extraBins...)
		}
	}
}

func (p *DynamicVoicePool) activePool() (AgentPool, error) {
	if p == nil {
		return nil, errors.New("dynamic voice pool is nil")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errors.New("dynamic voice pool is closed")
	}
	if p.currentPool == nil {
		return nil, errors.New("no active voice pool")
	}
	return p.currentPool, nil
}

// Initialize pre-warms the currently active pool.
func (p *DynamicVoicePool) Initialize(ctx context.Context) error {
	pool, err := p.activePool()
	if err != nil {
		return err
	}
	return pool.Initialize(ctx)
}

// Close acquires the write Lock, sets closed=true, closes the active pool,
// and blocks until all retiring background pools complete their shutdown.
func (p *DynamicVoicePool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	pool := p.currentPool
	p.currentPool = nil
	p.mu.Unlock()

	var err error
	if pool != nil {
		err = pool.Close()
	}
	p.retiringWg.Wait()
	return err
}

// UpdatePrewarmedTargets forwards the prewarmed targets list to the active pool if supported.
func (p *DynamicVoicePool) UpdatePrewarmedTargets(targets []string) {
	pool, err := p.activePool()
	if err != nil {
		return
	}
	if u, ok := pool.(interface{ UpdatePrewarmedTargets(targets []string) }); ok {
		u.UpdatePrewarmedTargets(targets)
	}
}

// Model returns the configured model of the active voice pool, if available.
func (p *DynamicVoicePool) Model() string {
	pool, err := p.activePool()
	if err != nil || pool == nil {
		return ""
	}
	if m, ok := pool.(interface{ Model() string }); ok {
		return m.Model()
	}
	return ""
}

// MarkDirty serializes reloads using reloadMu.
func (p *DynamicVoicePool) MarkDirty() {
	if p == nil {
		return
	}
	p.reloadMu.Lock()
	defer p.reloadMu.Unlock()

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return
	}
	currentPool, currentKey, factory := p.currentPool, p.currentKey, p.factory
	p.mu.RUnlock()

	if factory == nil {
		if dm, ok := currentPool.(interface{ MarkDirty() }); ok {
			dm.MarkDirty()
		}
		return
	}

	candidatePool, newKey, err := factory()
	if err != nil {
		log.Printf("[DynamicVoicePool] Factory error during reload: %v; retaining LKGP", err)
		return
	}

	if newKey == currentKey {
		if dm, ok := currentPool.(interface{ MarkDirty() }); ok {
			dm.MarkDirty()
		}
		if candidatePool != nil && candidatePool != currentPool {
			if cErr := candidatePool.Close(); cErr != nil {
				log.Printf("[DynamicVoicePool] Warning closing unused candidate pool: %v", cErr)
			}
		}
		return
	}

	if candidatePool == nil {
		log.Printf("[DynamicVoicePool] Warning: candidate pool is nil for changed fingerprint; retaining LKGP")
		return
	}

	p.mu.RLock()
	p.applyDependenciesToPool(candidatePool)
	p.mu.RUnlock()

	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	initErr := candidatePool.Initialize(initCtx)
	initCancel()
	if initErr != nil {
		log.Printf("[DynamicVoicePool] Candidate pool initialization failed: %v; retaining LKGP", initErr)
		if cErr := candidatePool.Close(); cErr != nil {
			log.Printf("[DynamicVoicePool] Warning closing failed candidate pool: %v", cErr)
		}
		return
	}
	propagateDrainToCandidate(candidatePool, p.draining.Load())

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if cErr := candidatePool.Close(); cErr != nil {
			log.Printf("[DynamicVoicePool] Warning closing candidate pool on closed parent: %v", cErr)
		}
		return
	}
	oldPool := p.currentPool
	p.currentPool, p.currentKey = candidatePool, newKey
	if oldPool != nil {
		p.retiringWg.Add(1)
	}
	p.mu.Unlock()

	if oldPool != nil {
		go func(retiring AgentPool) {
			defer p.retiringWg.Done()
			if cErr := retiring.Close(); cErr != nil {
				log.Printf("[DynamicVoicePool] Error closing retired pool: %v", cErr)
			}
		}(oldPool)
	}
}

// propagateDrainToCandidate latches drain on a freshly adopted pool when the
// wrapper already drained, so the new primary resumes the same sessions.
func propagateDrainToCandidate(candidatePool AgentPool, draining bool) {
	if !draining {
		return
	}
	if d, ok := candidatePool.(interface{ BeginDrain() }); ok {
		d.BeginDrain()
	}
}

// AcquireLease delegates lease acquisition to the underlying pool.
func (p *DynamicVoicePool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	pool, err := p.activePool()
	if err != nil {
		return nil, err
	}
	return pool.AcquireLease(ctx, targetKey)
}

// Draining reports whether BeginDrain has latched on this pool.
func (p *DynamicVoicePool) Draining() bool {
	return p != nil && p.draining.Load()
}

// BeginDrain latches drain on the active pool and propagates to any pool adopted by
// a later reload, so the new primary resumes the same sessions.
func (p *DynamicVoicePool) BeginDrain() {
	if p == nil {
		return
	}
	p.draining.Store(true)
	if pool, err := p.activePool(); err == nil {
		if d, ok := pool.(interface{ BeginDrain() }); ok {
			d.BeginDrain()
		}
	}
}
