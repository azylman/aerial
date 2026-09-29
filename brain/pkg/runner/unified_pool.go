package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// PoolConfig specifies parameters for initializing and configuring UnifiedProcessPool.
type PoolConfig struct {
	AgyBin           string
	Model            string
	DefaultModel     string            // Deprecated: use Model instead. Preserved for backward compatibility.
	TargetModels     map[string]string // Deprecated: pools are now model-bound. Ignored.
	Cwd              string
	Env              []string
	GeminiHomeDir    string
	PrewarmedTargets []string
	MaxIdle          time.Duration
}

// UnifiedProcessPool manages pinned daemons and singleflight pre-warming.
type UnifiedProcessPool struct {
	cfg       PoolConfig
	spawner   DaemonSpawner
	daemons   map[string]*StreamingDaemon
	mu        sync.RWMutex
	sf        singleflight.Group
	bgWg      sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeErr  error
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewUnifiedProcessPool creates a new process pool with the given configuration and spawner.
func NewUnifiedProcessPool(cfg PoolConfig, spawner DaemonSpawner) *UnifiedProcessPool {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}
	if cfg.MaxIdle <= 0 {
		cfg.MaxIdle = 24 * time.Hour
	}
	ctx, cancel := context.WithCancel(context.Background())
	resolvedModel := cfg.Model
	if resolvedModel == "" {
		resolvedModel = cfg.DefaultModel
	}
	cfg.Model = resolvedModel
	cfg.DefaultModel = resolvedModel
	return &UnifiedProcessPool{
		cfg:     cfg,
		spawner: spawner,
		daemons: make(map[string]*StreamingDaemon),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Model returns the immutable model name bound to this process pool.
func (p *UnifiedProcessPool) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
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

		daemonEnv := p.cfg.Env
		if home := strings.TrimSpace(p.cfg.GeminiHomeDir); home != "" {
			geminiDir := filepath.Join(home, ".gemini")
			if err := os.MkdirAll(geminiDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create runtime home directory %q: %w", geminiDir, err)
			}
			daemonEnv = BuildAgyEnv(AgyEnvInput{
				BaseEnv:  p.cfg.Env,
				HomeDir:  home,
				TargetID: targetKey,
			})
		}

		daemonCfg := DaemonConfig{
			ThreadID: targetKey,
			Model:    p.cfg.Model,
			AgyBin:   p.cfg.AgyBin,
			Cwd:      p.cfg.Cwd,
			Env:      daemonEnv,
		}

		daemon, spawnErr := StartStreamingDaemon(p.ctx, daemonCfg, p.spawner)
		if spawnErr != nil {
			p.mu.RLock()
			closed := p.closed
			p.mu.RUnlock()
			if closed {
				return nil, fmt.Errorf("process pool is closed")
			}
			return nil, fmt.Errorf("failed to start daemon for target %q: %w", targetKey, spawnErr)
		}

		daemon.SetOnTurnFinished(func(d *StreamingDaemon) {
			p.mu.RLock()
			if p.closed {
				p.mu.RUnlock()
				return
			}
			should, _ := p.ShouldRotate(d)
			tracked := p.daemons[targetKey] == d
			p.mu.RUnlock()

			if should && tracked {
				p.bgWg.Add(1)
				go func(tKey string) {
					defer p.bgWg.Done()
					rotCtx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
					defer cancel()
					if _, rotErr := p.RotateDaemon(rotCtx, tKey, ""); rotErr != nil {
						if !errors.Is(rotErr, context.Canceled) {
							log.Printf("[UnifiedProcessPool] Warning: rotation failed on turn completion for %q: %v", tKey, rotErr)
						}
					}
				}(targetKey)
			}
		})

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
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		toClose := p.daemons
		p.daemons = make(map[string]*StreamingDaemon)
		p.mu.Unlock()

		if p.cancel != nil {
			p.cancel()
		}

		var wg sync.WaitGroup
		var errMu sync.Mutex
		var errs []error
		for target, d := range toClose {
			if d != nil {
				wg.Add(1)
				go func(tKey string, daemon *StreamingDaemon) {
					defer wg.Done()
					if err := daemon.Close(); err != nil {
						errMu.Lock()
						errs = append(errs, fmt.Errorf("error closing daemon %q: %w", tKey, err))
						errMu.Unlock()
					}
				}(target, d)
			}
		}
		wg.Wait()
		if len(errs) > 0 {
			p.closeErr = fmt.Errorf("errors closing pool daemons: %v", errs)
		}
	})
	return p.closeErr
}

// Get returns the active StreamingDaemon for targetKey if present and not closed.
func (p *UnifiedProcessPool) Get(targetKey string) (*StreamingDaemon, bool) {
	if p == nil {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	d, ok := p.daemons[targetKey]
	if !ok || d == nil || d.State() == StateClosed {
		return nil, false
	}
	return d, true
}

// HasDaemon reports whether an active StreamingDaemon exists for targetKey.
func (p *UnifiedProcessPool) HasDaemon(targetKey string) bool {
	_, ok := p.Get(targetKey)
	return ok
}

// MarkDirty marks all active daemons as dirty.
func (p *UnifiedProcessPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, d := range p.daemons {
		if d != nil {
			d.MarkDirty()
		}
	}
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
	if d.IsDirty() {
		return true, "daemon marked dirty during in-flight turn"
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

// ExecuteEphemeral dispatches a prompt to targetKey's daemon and synchronously awaits the response.
// It performs proactive input validation and automatically triggers asynchronous session rotation
// if the daemon reaches its lifetime thresholds (10 turns).
func (p *UnifiedProcessPool) ExecuteEphemeral(ctx context.Context, targetKey, prompt string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("process pool is nil")
	}
	if strings.TrimSpace(targetKey) == "" {
		return "", fmt.Errorf("target key cannot be empty")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	daemon, err := p.GetOrCreate(ctx, targetKey)
	if err != nil {
		return "", fmt.Errorf("failed to acquire ephemeral daemon for %q: %w", targetKey, err)
	}
	sink := NewThrowawayTurnSink()
	turnCtx := &TurnContext{
		TurnID:    uuid.New().String(),
		Prompt:    prompt,
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send(prompt, turnCtx); err != nil {
		return "", fmt.Errorf("failed sending turn to ephemeral daemon %q: %w", targetKey, err)
	}
	res, err := sink.ResultContext(ctx)
	if err != nil {
		return "", err
	}

	// Context hygiene: rotate ephemeral daemon if turn limits reached
	if should, _ := p.ShouldRotate(daemon); should {
		go func() {
			rotCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, rotErr := p.RotateDaemon(rotCtx, targetKey, ""); rotErr != nil {
				log.Printf("[UnifiedProcessPool] Warning: ephemeral daemon rotation failed for %q: %v", targetKey, rotErr)
			}
		}()
	}

	return res, nil
}

// EphemeralLLMFunc returns a runner.LLMFunc that executes prompts against targetKey.
func (p *UnifiedProcessPool) EphemeralLLMFunc(targetKey string) LLMFunc {
	return func(ctx context.Context, model, prompt string) (string, error) {
		if p == nil {
			return "", fmt.Errorf("process pool is nil")
		}
		return p.ExecuteEphemeral(ctx, targetKey, prompt)
	}
}
