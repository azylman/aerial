package runner

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// PoolConfig specifies parameters for initializing and configuring UnifiedProcessPool.
type PoolConfig struct {
	PrewarmedTargets []string
	DefaultModel     string
	AgyBin           string
	Cwd              string
	Env              []string
	MaxIdle          time.Duration
}

// UnifiedProcessPool manages pinned daemons and singleflight pre-warming.
type UnifiedProcessPool struct {
	cfg       PoolConfig
	spawner   DaemonSpawner
	daemons   map[string]*StreamingDaemon
	mu        sync.RWMutex
	sf        singleflight.Group
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

// NewUnifiedProcessPool creates a new process pool with the given configuration and spawner.
func NewUnifiedProcessPool(cfg PoolConfig, spawner DaemonSpawner) *UnifiedProcessPool {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}
	if cfg.MaxIdle <= 0 {
		cfg.MaxIdle = 24 * time.Hour
	}
	return &UnifiedProcessPool{
		cfg:     cfg,
		spawner: spawner,
		daemons: make(map[string]*StreamingDaemon),
	}
}

// Initialize pre-warms configured target daemons in background goroutines.
func (p *UnifiedProcessPool) Initialize(ctx context.Context) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return fmt.Errorf("process pool is closed")
	}
	p.mu.RUnlock()

	for _, target := range p.cfg.PrewarmedTargets {
		targetKey := target
		go func() {
			initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := p.GetOrCreate(initCtx, targetKey); err != nil {
				log.Printf("[UnifiedProcessPool] Warning: pre-warming target %q failed: %v", targetKey, err)
			} else {
				log.Printf("[UnifiedProcessPool] Pre-warmed target %q successfully", targetKey)
			}
		}()
	}
	return nil
}

// GetOrCreate retrieves an active StreamingDaemon for targetKey, or starts one via singleflight.
func (p *UnifiedProcessPool) GetOrCreate(ctx context.Context, targetKey string) (*StreamingDaemon, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("process pool is closed")
	}
	if d, exists := p.daemons[targetKey]; exists && d != nil && d.State() != StateClosed {
		p.mu.RUnlock()
		return d, nil
	}
	p.mu.RUnlock()

	res, err, _ := p.sf.Do(targetKey, func() (any, error) {
		p.mu.RLock()
		if p.closed {
			p.mu.RUnlock()
			return nil, fmt.Errorf("process pool is closed")
		}
		if d, exists := p.daemons[targetKey]; exists && d != nil && d.State() != StateClosed {
			p.mu.RUnlock()
			return d, nil
		}
		p.mu.RUnlock()

		daemonCfg := DaemonConfig{
			ThreadID: targetKey,
			Model:    p.cfg.DefaultModel,
			AgyBin:   p.cfg.AgyBin,
			Cwd:      p.cfg.Cwd,
			Env:      p.cfg.Env,
		}

		daemon, spawnErr := StartStreamingDaemon(ctx, daemonCfg, p.spawner)
		if spawnErr != nil {
			return nil, fmt.Errorf("failed to start daemon for target %q: %w", targetKey, spawnErr)
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			if closeErr := daemon.Close(); closeErr != nil {
				log.Printf("[UnifiedProcessPool] Warning: failed closing daemon after pool closed: %v", closeErr)
			}
			return nil, fmt.Errorf("process pool is closed")
		}
		p.daemons[targetKey] = daemon
		p.mu.Unlock()

		return daemon, nil
	})

	if err != nil {
		return nil, err
	}
	daemon, ok := res.(*StreamingDaemon)
	if !ok || daemon == nil {
		return nil, fmt.Errorf("unexpected nil daemon from singleflight")
	}
	return daemon, nil
}

// Close gracefully terminates all daemons tracked by the pool.
func (p *UnifiedProcessPool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		toClose := p.daemons
		p.daemons = make(map[string]*StreamingDaemon)
		p.mu.Unlock()

		var errs []error
		for target, d := range toClose {
			if d != nil {
				if err := d.Close(); err != nil {
					errs = append(errs, fmt.Errorf("error closing daemon %q: %w", target, err))
				}
			}
		}
		if len(errs) > 0 {
			p.closeErr = fmt.Errorf("errors closing pool daemons: %v", errs)
		}
	})
	return p.closeErr
}

// Session rotation thresholds for memory pressure mitigation and context compaction.
const (
	DefaultMaxSessionTurns   = 10
	DefaultMaxSessionSteps   = 180
	DefaultMaxTranscriptByte = 500 * 1024
)

// ShouldRotate evaluates whether a daemon has reached its session lifetime thresholds
// (10 turns or 180 steps) and is safe to rotate (no in-flight turns).
func (p *UnifiedProcessPool) ShouldRotate(d *StreamingDaemon) (bool, string) {
	if d == nil {
		return false, ""
	}
	if d.InflightCount() > 0 {
		return false, "" // Never rotate during in-flight turn
	}
	if d.TurnCount() >= DefaultMaxSessionTurns {
		return true, fmt.Sprintf("turn count threshold exceeded (%d >= %d)", d.TurnCount(), DefaultMaxSessionTurns)
	}
	if d.StepCount() >= DefaultMaxSessionSteps {
		return true, fmt.Sprintf("step count threshold exceeded (%d >= %d)", d.StepCount(), DefaultMaxSessionSteps)
	}
	return false, ""
}

// RotateDaemon evicts and closes the existing daemon for targetKey, then spawns a fresh
// replacement daemon via GetOrCreate. If summary is non-empty, context compaction logging is recorded.
func (p *UnifiedProcessPool) RotateDaemon(ctx context.Context, targetKey string, summary string) (*StreamingDaemon, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("process pool is closed")
	}
	oldDaemon, exists := p.daemons[targetKey]
	delete(p.daemons, targetKey)
	p.mu.Unlock()

	if summary != "" {
		log.Printf("[UnifiedProcessPool] Rotating daemon %q with summary compaction (%d bytes)", targetKey, len(summary))
	}

	if exists && oldDaemon != nil {
		if closeErr := oldDaemon.Close(); closeErr != nil {
			log.Printf("[UnifiedProcessPool] Warning: error closing old daemon %q during rotation: %v", targetKey, closeErr)
		}
	}

	newDaemon, err := p.GetOrCreate(ctx, targetKey)
	if err != nil {
		return nil, fmt.Errorf("failed creating rotated daemon for %q: %w", targetKey, err)
	}

	return newDaemon, nil
}
