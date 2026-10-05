package runner

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
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
}

var _ AgentPool = (*DynamicVoicePool)(nil)

// NewDynamicVoicePool constructs a DynamicVoicePool using the provided factory.
// It invokes the factory immediately to obtain the initial active pool and fingerprint.
func NewDynamicVoicePool(factory DynamicVoicePoolFactory) *DynamicVoicePool {
	var initialPool AgentPool
	var initialKey string
	if factory != nil {
		if pool, key, err := factory(); err == nil {
			initialPool = pool
			initialKey = key
		} else {
			log.Printf("[DynamicVoicePool] Warning: initial factory invocation failed: %v", err)
		}
	}
	return NewDynamicVoicePoolWithInitial(initialPool, initialKey, factory)
}

// NewDynamicVoicePoolWithInitial constructs a DynamicVoicePool with an explicitly supplied initial pool and fingerprint.
func NewDynamicVoicePoolWithInitial(initialPool AgentPool, initialKey string, factory DynamicVoicePoolFactory) *DynamicVoicePool {
	return &DynamicVoicePool{
		currentPool: initialPool,
		currentKey:  initialKey,
		factory:     factory,
	}
}

// GetOrCreateSession captures currentPool under RLock, releases the lock immediately,
// and delegates to the pool unlocked to avoid write starvation or deadlock during swaps.
func (p *DynamicVoicePool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	if p == nil {
		return nil, errors.New("dynamic voice pool is nil")
	}

	p.mu.RLock()
	closed := p.closed
	pool := p.currentPool
	p.mu.RUnlock()

	if closed {
		return nil, errors.New("dynamic voice pool is closed")
	}
	if pool == nil {
		return nil, errors.New("no active voice pool")
	}

	return pool.GetOrCreateSession(ctx, targetKey, sessionID)
}

// Initialize pre-warms the currently active pool.
func (p *DynamicVoicePool) Initialize(ctx context.Context) error {
	if p == nil {
		return errors.New("dynamic voice pool is nil")
	}

	p.mu.RLock()
	closed := p.closed
	pool := p.currentPool
	p.mu.RUnlock()

	if closed {
		return errors.New("dynamic voice pool is closed")
	}
	if pool == nil {
		return errors.New("no active voice pool")
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

// Model returns the active pool's model identifier if available.
func (p *DynamicVoicePool) Model() string {
	if p == nil {
		return ""
	}

	p.mu.RLock()
	pool := p.currentPool
	p.mu.RUnlock()

	if pool == nil {
		return ""
	}
	if m, ok := pool.(interface{ Model() string }); ok {
		return m.Model()
	}
	return ""
}

// UpdatePrewarmedTargets forwards the prewarmed targets list to the active pool if supported.
func (p *DynamicVoicePool) UpdatePrewarmedTargets(targets []string) {
	if p == nil {
		return
	}

	p.mu.RLock()
	pool := p.currentPool
	p.mu.RUnlock()

	if pool == nil {
		return
	}
	if u, ok := pool.(interface{ UpdatePrewarmedTargets([]string) }); ok {
		u.UpdatePrewarmedTargets(targets)
	}
}

// MarkDirty serializes reloads using reloadMu. If the configuration fingerprint is unchanged,
// it forwards MarkDirty to the current pool. If the fingerprint changed, it instantiates and
// initializes a candidate pool with a 30s timeout, atomically swaps currentPool, and asynchronously
// retires the old pool with retiringWg tracking. If candidate initialization fails, LKGP is retained.
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
	currentPool := p.currentPool
	currentKey := p.currentKey
	factory := p.factory
	p.mu.RUnlock()

	if factory == nil {
		if dirtyMarker, ok := currentPool.(interface{ MarkDirty() }); ok {
			dirtyMarker.MarkDirty()
		}
		return
	}

	candidatePool, newKey, err := factory()
	if err != nil {
		log.Printf("[DynamicVoicePool] Factory error during reload: %v; retaining LKGP", err)
		return
	}

	if newKey == currentKey {
		// Fingerprint is unchanged: forward MarkDirty to active pool without tearing it down
		if dirtyMarker, ok := currentPool.(interface{ MarkDirty() }); ok {
			dirtyMarker.MarkDirty()
		}
		// If candidatePool was newly created by factory and differs from currentPool, close it
		if candidatePool != nil && candidatePool != currentPool {
			if cErr := candidatePool.Close(); cErr != nil {
				log.Printf("[DynamicVoicePool] Warning closing unused candidate pool: %v", cErr)
			}
		}
		return
	}

	// Fingerprint changed: initialize candidate pool with 30s timeout
	if candidatePool == nil {
		log.Printf("[DynamicVoicePool] Warning: candidate pool is nil for changed fingerprint; retaining LKGP")
		return
	}

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

	// Swap currentPool under write Lock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if cErr := candidatePool.Close(); cErr != nil {
			log.Printf("[DynamicVoicePool] Warning closing candidate pool on closed parent: %v", cErr)
		}
		return
	}
	oldPool := p.currentPool
	p.currentPool = candidatePool
	p.currentKey = newKey
	if oldPool != nil {
		p.retiringWg.Add(1)
	}
	p.mu.Unlock()

	// Asynchronously retire old pool with retiringWg tracking
	if oldPool != nil {
		go func(retiring AgentPool) {
			defer p.retiringWg.Done()
			if cErr := retiring.Close(); cErr != nil {
				log.Printf("[DynamicVoicePool] Error closing retired pool: %v", cErr)
			}
		}(oldPool)
	}
}

// CurrentPool returns the active AgentPool under RLock.
func (p *DynamicVoicePool) CurrentPool() AgentPool {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentPool
}

// Fingerprint returns the current configuration fingerprint under RLock.
func (p *DynamicVoicePool) Fingerprint() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentKey
}
