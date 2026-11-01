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
	return &DynamicVoicePool{currentPool: initialPool, currentKey: initialKey, factory: factory}
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

// AcquireLease delegates lease acquisition to the underlying pool.
func (p *DynamicVoicePool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	pool, err := p.activePool()
	if err != nil {
		return nil, err
	}
	return pool.AcquireLease(ctx, targetKey)
}
