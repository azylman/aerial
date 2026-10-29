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

	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// PoolConfig specifies parameters for initializing and configuring UnifiedProcessPool.
type PoolConfig struct {
	AgyBin, Model, DefaultModel, Cwd, GeminiHomeDir, Timezone     string
	TargetModels                                                  map[string]string // Deprecated: pools are now model-bound. Ignored.
	Env, PrewarmedTargets                                         []string
	MaxIdle, TargetLockTimeout, HandshakeTimeout                  time.Duration
	TranscriptRescuer                                             func(convID string, since time.Time) string
	MemoryRetriever                                               MemoryRetriever
	AmbientContextRetriever                                       AmbientContextRetriever
	SessionManager                                                *session.Manager
	OnSessionRotated                                              func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error
	GetSessionRecord                                              func(ctx context.Context, targetKey string) (SessionRecord, error)
	MaxSessionTurns, MinSessionTurns, EmergencyRotationMultiplier int
}

// UnifiedProcessPool manages pinned daemons and singleflight pre-warming.
type UnifiedProcessPool struct {
	cfg         PoolConfig
	spawner     DaemonSpawner
	daemons     map[string]*StreamingDaemon
	prewarming  map[string]bool
	targetLocks map[string]*targetLock
	mu          sync.RWMutex
	targetMu    sync.Mutex
	sf          singleflight.Group
	bgWg        sync.WaitGroup
	closed      bool
	closeOnce   sync.Once
	closeErr    error
	ctx         context.Context
	cancel      context.CancelFunc
}

var (
	_ AgentPool    = (*UnifiedProcessPool)(nil)
	_ AgentSession = (*StreamingDaemon)(nil)
)

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
	cfg.Model, cfg.DefaultModel = resolvedModel, resolvedModel
	return &UnifiedProcessPool{
		cfg:         cfg,
		spawner:     spawner,
		daemons:     make(map[string]*StreamingDaemon),
		prewarming:  make(map[string]bool),
		targetLocks: make(map[string]*targetLock),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (p *UnifiedProcessPool) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

func (p *UnifiedProcessPool) SetTranscriptRescuer(rescuer func(convID string, since time.Time) string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.TranscriptRescuer = rescuer
	for _, d := range p.daemons {
		d.SetTranscriptRescuer(rescuer)
	}
}

// SetSessionManager updates the session manager for rotation guardrail checks.
func (p *UnifiedProcessPool) SetSessionManager(sm *session.Manager) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.cfg.SessionManager = sm
	p.mu.Unlock()
}

// SetSessionCallbacks updates the session record lookup and rotation callbacks.
func (p *UnifiedProcessPool) SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.cfg.GetSessionRecord, p.cfg.OnSessionRotated = getRec, onRot
	p.mu.Unlock()
}

// Initialize pre-warms configured target daemons sequentially in the caller's context.
func (p *UnifiedProcessPool) Initialize(ctx context.Context) error {
	if p.isClosed() {
		return fmt.Errorf("process pool is closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, target := range p.cfg.PrewarmedTargets {
		if p.isClosed() {
			return fmt.Errorf("process pool is closed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		initCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
		if err := p.prewarmTarget(initCtx, target); err != nil {
			log.Printf("[UnifiedProcessPool] Warning: pre-warming target %q failed: %v", target, err)
		} else {
			log.Printf("[UnifiedProcessPool] Pre-warmed target %q successfully", target)
		}
		cancel()
	}
	return nil
}

func (p *UnifiedProcessPool) prewarmTarget(ctx context.Context, targetKey string) error {
	trimmed := strings.TrimSpace(targetKey)
	if trimmed == "" {
		return nil
	}
	if d, ok := p.Get(trimmed); ok && !d.IsDirty() {
		return nil
	}
	tl, acquired := p.tryAcquireTargetLock(trimmed)
	if !acquired {
		return nil
	}
	defer p.releaseTargetLock(trimmed, tl, true)
	if d, ok := p.Get(trimmed); ok && !d.IsDirty() {
		return nil
	}
	_, err := p.GetOrCreate(ctx, trimmed, "")
	return err
}

func (p *UnifiedProcessPool) closeEvictedDaemon(oldD *StreamingDaemon, targetKey string) {
	if oldD == nil {
		return
	}
	oldD.MarkDirty()
	if oldD.InflightCount() > 0 {
		oldD.SetOnTurnFinished(func(d *StreamingDaemon) {
			if closeErr := d.Close(); closeErr != nil {
				log.Printf("[UnifiedProcessPool] Warning: error closing drained daemon %q: %v", targetKey, closeErr)
			}
		})
		if oldD.InflightCount() > 0 {
			return
		}
	}
	p.bgWg.Add(1)
	go func(d *StreamingDaemon, tKey string) {
		defer p.bgWg.Done()
		if closeErr := d.Close(); closeErr != nil {
			log.Printf("[UnifiedProcessPool] Warning: error closing old daemon %q: %v", tKey, closeErr)
		}
	}(oldD, targetKey)
}

func (p *UnifiedProcessPool) canReuseDaemon(d *StreamingDaemon, trimmedSess string) bool {
	if !isDaemonMatch(d, trimmedSess) {
		return false
	}
	if trimmedSess != "" {
		return true
	}
	should, _ := p.ShouldRotate(d)
	return !should
}

func (p *UnifiedProcessPool) checkExistingDaemonForCreateLocked(targetKey, trimmedSess string) (*StreamingDaemon, string, bool) {
	d, exists := p.daemons[targetKey]
	if !exists || d == nil {
		return nil, trimmedSess, false
	}
	should, rotReason := p.ShouldRotate(d)
	if isDaemonMatch(d, trimmedSess) && (trimmedSess != "" || !should) {
		return d, trimmedSess, true
	}
	if !should && trimmedSess == "" && d.SessionID() != "" && !strings.Contains(targetKey, ":") {
		trimmedSess = d.SessionID()
	}
	delete(p.daemons, targetKey)
	p.closeEvictedDaemon(d, targetKey)
	if should && trimmedSess == "" {
		metrics.RecordSessionRotation("pre_flight", "pool", rotReason)
	}
	return nil, trimmedSess, false
}

func (p *UnifiedProcessPool) buildDaemonConfig(targetKey, trimmedSess string, initialTurns []int) (DaemonConfig, error) {
	daemonEnv := filterVanadisChunkEnv(p.cfg.Env)
	if home := strings.TrimSpace(p.cfg.GeminiHomeDir); home != "" {
		geminiDir := filepath.Join(home, ".gemini")
		if err := os.MkdirAll(geminiDir, 0o755); err != nil {
			return DaemonConfig{}, fmt.Errorf("failed to create runtime home directory %q: %w", geminiDir, err)
		}
		daemonEnv = BuildAgyEnv(AgyEnvInput{
			BaseEnv:  p.cfg.Env,
			HomeDir:  home,
			TargetID: targetKey,
			Timezone: p.cfg.Timezone,
		})
	}

	initTurnCount := 0
	if len(initialTurns) > 0 && initialTurns[0] > 0 {
		initTurnCount = initialTurns[0]
	}

	return DaemonConfig{
		ThreadID: targetKey, SessionID: trimmedSess, InitialTurnCount: initTurnCount,
		Model: p.cfg.Model, AgyBin: p.cfg.AgyBin, Cwd: p.cfg.Cwd, Env: daemonEnv,
		GeminiHomeDir: p.cfg.GeminiHomeDir, Timeout: p.cfg.HandshakeTimeout,
		TranscriptRescuer: p.cfg.TranscriptRescuer, MemoryRetriever: p.cfg.MemoryRetriever,
		AmbientContextRetriever: p.cfg.AmbientContextRetriever,
	}, nil
}

func (p *UnifiedProcessPool) spawnStreamingDaemonWithRetry(ctx context.Context, targetKey string, daemonCfg DaemonConfig) (*StreamingDaemon, error) {
	daemon, spawnErr := StartStreamingDaemon(p.ctx, daemonCfg, p.spawner)
	if spawnErr != nil && strings.Contains(spawnErr.Error(), "daemon startup timed out waiting for init event") && p.ctx.Err() == nil && ctx.Err() == nil {
		log.Printf("[UnifiedProcessPool] Warning: cold-start handshake timed out for target %q; retrying once...", targetKey)
		daemon, spawnErr = StartStreamingDaemon(p.ctx, daemonCfg, p.spawner)
	}
	if spawnErr != nil {
		if p.isClosed() {
			return nil, fmt.Errorf("process pool is closed")
		}
		return nil, fmt.Errorf("failed to start daemon for target %q: %w", targetKey, spawnErr)
	}
	return daemon, nil
}

func (p *UnifiedProcessPool) registerSpawnedDaemon(targetKey string, daemon *StreamingDaemon) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		if closeErr := daemon.Close(); closeErr != nil {
			log.Printf("[UnifiedProcessPool] Warning: failed closing daemon after pool closed: %v", closeErr)
		}
		return fmt.Errorf("process pool is closed")
	}
	if prev, exists := p.daemons[targetKey]; exists && prev != nil && prev != daemon {
		p.closeEvictedDaemon(prev, targetKey)
	}
	p.daemons[targetKey] = daemon
	return nil
}

// GetOrCreate retrieves an active StreamingDaemon for targetKey, or starts one via singleflight.
func (p *UnifiedProcessPool) GetOrCreate(ctx context.Context, targetKey string, sessionID string, initialTurns ...int) (*StreamingDaemon, error) {
	trimmedSess := strings.TrimSpace(sessionID)
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("process pool is closed")
	}
	if d := p.daemons[targetKey]; p.canReuseDaemon(d, trimmedSess) {
		p.mu.RUnlock()
		return d, nil
	}
	p.mu.RUnlock()

	res, err, _ := p.sf.Do(targetKey, func() (any, error) {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("process pool is closed")
		}
		reusable, updatedSess, reused := p.checkExistingDaemonForCreateLocked(targetKey, trimmedSess)
		trimmedSess = updatedSess
		p.mu.Unlock()
		if reused {
			return reusable, nil
		}

		daemonCfg, err := p.buildDaemonConfig(targetKey, trimmedSess, initialTurns)
		if err != nil {
			return nil, err
		}

		daemon, err := p.spawnStreamingDaemonWithRetry(ctx, targetKey, daemonCfg)
		if err != nil {
			return nil, err
		}

		if err := p.registerSpawnedDaemon(targetKey, daemon); err != nil {
			return nil, err
		}
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

// EvictSession closes and removes the active daemon for targetKey.
func (p *UnifiedProcessPool) EvictSession(targetKey string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	d, exists := p.daemons[targetKey]
	delete(p.daemons, targetKey)
	p.mu.Unlock()
	if exists && d != nil {
		return d.Close()
	}
	return nil
}

func isDaemonMatch(d *StreamingDaemon, trimmedSess string) bool {
	if d == nil || d.State() == StateClosed || d.IsDirty() {
		return false
	}
	if trimmedSess == "" {
		return true
	}
	return d.SessionID() == "" || d.SessionID() == trimmedSess
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
		p.bgWg.Wait()

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

func (p *UnifiedProcessPool) Get(targetKey string) (*StreamingDaemon, bool) {
	if p == nil {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	d, ok := p.daemons[targetKey]
	return d, ok && d != nil && d.State() != StateClosed
}

func (p *UnifiedProcessPool) PrewarmedTargets() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.cfg.PrewarmedTargets == nil {
		return nil
	}
	targets := make([]string, len(p.cfg.PrewarmedTargets))
	copy(targets, p.cfg.PrewarmedTargets)
	return targets
}

func (p *UnifiedProcessPool) UpdatePrewarmedTargets(targets []string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if targets == nil {
		p.cfg.PrewarmedTargets = nil
		return
	}
	seen := make(map[string]bool, len(targets))
	newTargets := make([]string, 0, len(targets))
	for _, t := range targets {
		if trimmed := strings.TrimSpace(t); trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			newTargets = append(newTargets, trimmed)
		}
	}
	p.cfg.PrewarmedTargets = newTargets
}

type evictedEntry struct {
	targetKey string
	daemon    *StreamingDaemon
}

func (p *UnifiedProcessPool) shouldPrewarmTargetLocked(target string) bool {
	if p.prewarming[target] || p.isTargetLocked(target) {
		return false
	}
	d, exists := p.daemons[target]
	return !exists || d == nil || d.State() == StateClosed
}

func (p *UnifiedProcessPool) collectDirtyEvictionsAndPrewarmsLocked() ([]evictedEntry, []string) {
	var toClose []evictedEntry
	var toPrewarm []string

	prewarmedSet := make(map[string]bool, len(p.cfg.PrewarmedTargets))
	for _, target := range p.cfg.PrewarmedTargets {
		prewarmedSet[target] = true
	}

	for target, d := range p.daemons {
		if d == nil {
			continue
		}
		if d.InflightCount() == 0 && !p.isTargetLocked(target) {
			delete(p.daemons, target)
			toClose = append(toClose, evictedEntry{targetKey: target, daemon: d})
			d.MarkDirty()
			if prewarmedSet[target] && !p.prewarming[target] {
				p.prewarming[target] = true
				toPrewarm = append(toPrewarm, target)
			}
		} else {
			d.MarkDirty()
		}
	}

	for _, rawTarget := range p.cfg.PrewarmedTargets {
		target := strings.TrimSpace(rawTarget)
		if target != "" && p.shouldPrewarmTargetLocked(target) {
			p.prewarming[target] = true
			toPrewarm = append(toPrewarm, target)
		}
	}

	p.bgWg.Add(len(toClose) + len(toPrewarm))
	return toClose, toPrewarm
}

// MarkDirty marks all active daemons as dirty and optimistically rotates idle daemons.
func (p *UnifiedProcessPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	toClose, toPrewarm := p.collectDirtyEvictionsAndPrewarmsLocked()
	p.mu.Unlock()

	for _, entry := range toClose {
		go func(e evictedEntry) {
			defer p.bgWg.Done()
			if err := e.daemon.Close(); err != nil {
				log.Printf("[UnifiedProcessPool] Warning: error closing evicted daemon %q: %v", e.targetKey, err)
			}
		}(entry)
	}

	for _, target := range toPrewarm {
		go func(tKey string) {
			defer p.bgWg.Done()
			defer func() {
				p.mu.Lock()
				delete(p.prewarming, tKey)
				p.mu.Unlock()
			}()
			initCtx, cancel := context.WithTimeout(p.ctx, 35*time.Second)
			defer cancel()
			if err := p.prewarmTarget(initCtx, tKey); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[UnifiedProcessPool] Warning: re-warming prewarmed target %q failed: %v", tKey, err)
			}
		}(target)
	}
}

// Session rotation thresholds for memory pressure mitigation and context compaction.
const (
	DefaultMinSessionTurns             = session.DefaultMinSessionTurns
	DefaultEmergencyRotationMultiplier = session.DefaultEmergencyRotationMultiplier
	DefaultMaxSessionTurns             = session.DefaultMaxSessionTurns
	DefaultMaxSessionSteps             = session.DefaultMaxSessionSteps
	DefaultMaxTranscriptByte           = session.DefaultMaxTranscriptBytes
	DefaultMaxTranscriptBytes          = session.DefaultMaxTranscriptBytes
	DefaultMaxSessionDBBytes           = session.DefaultMaxSessionDBBytes
)

// ShouldRotate evaluates whether a daemon has reached its session lifetime thresholds.
func (p *UnifiedProcessPool) ShouldRotate(d *StreamingDaemon) (bool, string) {
	if d == nil || d.InflightCount() > 0 {
		return false, ""
	}
	if d.IsDirty() {
		return true, "daemon marked dirty during in-flight turn"
	}

	minTurns, maxTurns, mult := DefaultMinSessionTurns, DefaultMaxSessionTurns, DefaultEmergencyRotationMultiplier
	if p != nil {
		if p.cfg.MinSessionTurns > 0 {
			minTurns = p.cfg.MinSessionTurns
		}
		if p.cfg.MaxSessionTurns > 0 {
			maxTurns = p.cfg.MaxSessionTurns
		}
		if p.cfg.EmergencyRotationMultiplier > 0 {
			mult = p.cfg.EmergencyRotationMultiplier
		}
	}

	currentTurns := d.TurnCount()
	var transcriptTurns, steps int
	var size, dbSize int64
	if p != nil && p.cfg.SessionManager != nil && d.SessionID() != "" {
		if transcriptTurns = p.cfg.SessionManager.CountTranscriptTurns(d.SessionID()); transcriptTurns > currentTurns {
			currentTurns = transcriptTurns
		}
		steps = p.cfg.SessionManager.CountTranscriptSteps(d.SessionID())
		size = p.cfg.SessionManager.GetTranscriptSize(d.SessionID())
		dbSize = p.cfg.SessionManager.GetSessionDBSize(d.SessionID())
	}

	if currentTurns >= maxTurns {
		if d.TurnCount() >= maxTurns {
			return true, fmt.Sprintf("turn count threshold exceeded (%d >= %d)", d.TurnCount(), maxTurns)
		}
		return true, fmt.Sprintf("transcript turn count threshold exceeded (%d >= %d)", transcriptTurns, maxTurns)
	}

	stepThresh, sizeThresh, dbThresh := DefaultMaxSessionSteps, int64(DefaultMaxTranscriptBytes), int64(DefaultMaxSessionDBBytes)
	prefix := ""
	if currentTurns < minTurns {
		stepThresh *= mult
		sizeThresh *= int64(mult)
		dbThresh *= int64(mult)
		prefix = " emergency"
	}
	if steps >= stepThresh {
		return true, fmt.Sprintf("transcript step count%s threshold exceeded (%d >= %d)", prefix, steps, stepThresh)
	}
	if size >= sizeThresh {
		return true, fmt.Sprintf("transcript file size%s threshold exceeded (%d >= %d bytes)", prefix, size, sizeThresh)
	}
	if dbSize >= dbThresh {
		return true, fmt.Sprintf("session DB size%s threshold exceeded (%d >= %d bytes)", prefix, dbSize, dbThresh)
	}
	return false, ""
}

// RotateDaemon evicts and closes the existing daemon for targetKey, then spawns a fresh daemon.
func (p *UnifiedProcessPool) RotateDaemon(ctx context.Context, targetKey string, summary string) (*StreamingDaemon, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("process pool is closed")
	}
	oldDaemon := p.daemons[targetKey]
	delete(p.daemons, targetKey)
	p.mu.Unlock()

	if summary != "" {
		log.Printf("[UnifiedProcessPool] Rotating daemon %q with summary compaction (%d bytes)", targetKey, len(summary))
	}
	if oldDaemon != nil {
		if err := oldDaemon.Close(); err != nil {
			log.Printf("[UnifiedProcessPool] Warning: error closing old daemon %q during rotation: %v", targetKey, err)
		}
	}
	newDaemon, err := p.GetOrCreate(ctx, targetKey, "")
	if err != nil {
		return nil, fmt.Errorf("failed creating rotated daemon for %q: %w", targetKey, err)
	}
	return newDaemon, nil
}

// ExecuteEphemeral dispatches a prompt to targetKey's daemon and synchronously awaits the response.
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
	daemon, err := p.GetOrCreate(ctx, targetKey, "")
	if err != nil {
		return "", fmt.Errorf("failed to acquire ephemeral daemon for %q: %w", targetKey, err)
	}
	sink := NewThrowawayTurnSink()
	turnCtx := &TurnContext{TurnID: uuid.New().String(), Prompt: prompt, Sink: sink, CreatedAt: time.Now()}
	if err := daemon.Send(prompt, turnCtx); err != nil {
		return "", fmt.Errorf("failed sending turn to ephemeral daemon %q: %w", targetKey, err)
	}
	res, err := sink.ResultContext(ctx)
	if err != nil {
		return "", err
	}
	if should, _ := p.ShouldRotate(daemon); should {
		go func() {
			rotCtx, cancel := context.WithTimeout(p.ctx, 35*time.Second)
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

type targetLock struct {
	sem  chan struct{}
	refs int32
}

func (p *UnifiedProcessPool) isTargetLocked(targetKey string) bool {
	p.targetMu.Lock()
	defer p.targetMu.Unlock()
	tl, ok := p.targetLocks[targetKey]
	return ok && tl != nil && len(tl.sem) > 0
}

func (p *UnifiedProcessPool) tryAcquireTargetLock(targetKey string) (*targetLock, bool) {
	p.targetMu.Lock()
	defer p.targetMu.Unlock()
	if p.targetLocks == nil {
		p.targetLocks = make(map[string]*targetLock)
	}
	tl, ok := p.targetLocks[targetKey]
	if !ok {
		tl = &targetLock{sem: make(chan struct{}, 1)}
		p.targetLocks[targetKey] = tl
	}
	select {
	case tl.sem <- struct{}{}:
		tl.refs++
		return tl, true
	default:
		if tl.refs <= 0 {
			delete(p.targetLocks, targetKey)
		}
		return nil, false
	}
}

func acquirePoolTargetLock(ctx context.Context, mu *sync.Mutex, locks *map[string]*targetLock, targetKey string, timeout time.Duration) (*targetLock, error) {
	if mu == nil || locks == nil {
		return nil, errors.New("target lock state is uninitialized")
	}
	mu.Lock()
	if *locks == nil {
		*locks = make(map[string]*targetLock)
	}
	tl, ok := (*locks)[targetKey]
	if !ok {
		tl = &targetLock{sem: make(chan struct{}, 1)}
		(*locks)[targetKey] = tl
	}
	tl.refs++
	mu.Unlock()

	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	lockCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case tl.sem <- struct{}{}:
		return tl, nil
	case <-lockCtx.Done():
		releasePoolTargetLock(mu, *locks, targetKey, tl, false)
		return nil, fmt.Errorf("timeout waiting for target lock for %s: %w", targetKey, lockCtx.Err())
	}
}

func releasePoolTargetLock(mu *sync.Mutex, locks map[string]*targetLock, targetKey string, tl *targetLock, held bool) {
	if tl == nil {
		return
	}
	if held {
		select {
		case <-tl.sem:
		default:
		}
	}
	if mu == nil {
		return
	}
	mu.Lock()
	tl.refs--
	if tl.refs <= 0 && locks != nil {
		delete(locks, targetKey)
	}
	mu.Unlock()
}

func (p *UnifiedProcessPool) acquireTargetLock(ctx context.Context, targetKey string) (*targetLock, error) {
	return acquirePoolTargetLock(ctx, &p.targetMu, &p.targetLocks, targetKey, p.cfg.TargetLockTimeout)
}

func (p *UnifiedProcessPool) releaseTargetLock(targetKey string, tl *targetLock, held bool) {
	releasePoolTargetLock(&p.targetMu, p.targetLocks, targetKey, tl, held)
}

// AcquireLease blocks until the target lock is acquired and returns an exclusive SessionLease.
func (p *UnifiedProcessPool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	if p == nil {
		return nil, errors.New("unified process pool is uninitialized")
	}
	trimmedKey := strings.TrimSpace(targetKey)
	if trimmedKey == "" {
		return nil, errors.New("targetKey cannot be empty")
	}

	tl, err := p.acquireTargetLock(ctx, trimmedKey)
	if err != nil {
		return nil, err
	}

	var rec SessionRecord
	if p.cfg.GetSessionRecord != nil {
		if r, rErr := p.cfg.GetSessionRecord(ctx, trimmedKey); rErr == nil {
			rec = r
		}
	}
	d, dErr := p.GetOrCreate(ctx, trimmedKey, rec.ActiveSessionID, rec.TurnCount)
	if dErr != nil {
		p.releaseTargetLock(trimmedKey, tl, true)
		return nil, fmt.Errorf("failed to get or create daemon for %s: %w", trimmedKey, dErr)
	}

	isCold, turnCount := rec.TurnCount <= 1 || rec.ActiveSessionID == "", rec.TurnCount
	if isCold {
		turnCount = 0
	}
	sessID := d.SessionID()
	if sessID == "" {
		sessID = rec.ActiveSessionID
	}

	return &unifiedSessionLease{
		pool:       p,
		daemon:     d,
		targetKey:  trimmedKey,
		sessionID:  sessID,
		prevSessID: rec.PreviousSessionID,
		isCold:     isCold,
		turnCount:  turnCount,
		lock:       tl,
	}, nil
}

type unifiedSessionLease struct {
	pool                             *UnifiedProcessPool
	daemon                           *StreamingDaemon
	lock                             *targetLock
	targetKey, sessionID, prevSessID string
	turnCount                        int
	isCold, cancelRotation           bool
	releaseOnce                      sync.Once
	mu                               sync.RWMutex
}

var _ SessionLease = (*unifiedSessionLease)(nil)

func (l *unifiedSessionLease) SessionID() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.daemon != nil && l.daemon.SessionID() != "" {
		return l.daemon.SessionID()
	}
	return l.sessionID
}

func (l *unifiedSessionLease) PreviousSessionID() string { return l.prevSessID }
func (l *unifiedSessionLease) IsCold() bool              { return l.isCold }
func (l *unifiedSessionLease) IsEphemeral() bool         { return false }
func (l *unifiedSessionLease) TurnCount() int            { return l.turnCount }
func (l *unifiedSessionLease) MarkDirty() {
	if l != nil {
		l.mu.RLock()
		d := l.daemon
		l.mu.RUnlock()
		if d != nil {
			d.MarkDirty()
		}
	}
}

type leaseTurnSinkWrapper struct {
	inner TurnSink
	buf   *BufferingTurnSink
}

func (s *leaseTurnSinkWrapper) OnTurnStarted() {
	if s.inner != nil {
		s.inner.OnTurnStarted()
	}
	s.buf.OnTurnStarted()
}

func (s *leaseTurnSinkWrapper) OnThinking() {
	if s.inner != nil {
		s.inner.OnThinking()
	}
	s.buf.OnThinking()
}

func (s *leaseTurnSinkWrapper) OnStepStarted(idx int) {
	if s.inner != nil {
		if sa, ok := s.inner.(StepAwareSink); ok {
			sa.OnStepStarted(idx)
		}
	}
	if s.buf != nil {
		s.buf.OnStepStarted(idx)
	}
}

func (s *leaseTurnSinkWrapper) OnToolCall(tool, cmd string) {
	if s.inner != nil {
		s.inner.OnToolCall(tool, cmd)
	}
	s.buf.OnToolCall(tool, cmd)
}

func (s *leaseTurnSinkWrapper) OnToolCompleted(tool, mcp string, d time.Duration, status string) {
	if s.inner != nil {
		s.inner.OnToolCompleted(tool, mcp, d, status)
	}
	s.buf.OnToolCompleted(tool, mcp, d, status)
}

func (s *leaseTurnSinkWrapper) OnSkillActivated(skill, src string) {
	if s.inner != nil {
		s.inner.OnSkillActivated(skill, src)
	}
	s.buf.OnSkillActivated(skill, src)
}

func (s *leaseTurnSinkWrapper) OnTextDelta(delta string) {
	if s.inner != nil {
		s.inner.OnTextDelta(delta)
	}
	s.buf.OnTextDelta(delta)
}

func (s *leaseTurnSinkWrapper) OnResult(res *TurnResult) {
	if s.inner != nil {
		s.inner.OnResult(res)
	}
	s.buf.OnResult(res)
}

func (s *leaseTurnSinkWrapper) OnError(err error) {
	if s.inner != nil {
		s.inner.OnError(err)
	}
	s.buf.OnError(err)
}

func (l *unifiedSessionLease) ensureActiveDaemon(ctx context.Context) error {
	l.mu.RLock()
	d := l.daemon
	pool := l.pool
	targetKey := l.targetKey
	sessID := l.sessionID
	turnCount := l.turnCount
	l.mu.RUnlock()
	if pool == nil || (d != nil && d.State() != StateClosed && !d.IsDirty()) {
		return nil
	}
	refreshed, err := pool.GetOrCreate(ctx, targetKey, sessID, turnCount)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.daemon = refreshed
	if refreshed.SessionID() != "" {
		l.sessionID = refreshed.SessionID()
	}
	l.mu.Unlock()
	return nil
}

func (l *unifiedSessionLease) evaluateEmptyResponseOutcome(ctx context.Context, turn *TurnContext, res *TurnResult) {
	if res == nil || IsSubstantiveResponse(res.Response) {
		return
	}
	l.mu.RLock()
	sessID := l.sessionID
	d := l.daemon
	l.mu.RUnlock()
	isQuota := IsQuotaPause(res.Stderr, "") || IsCapacityBlip(res.Stderr, "")
	if !isQuota && l.pool != nil && l.pool.cfg.SessionManager != nil && sessID != "" {
		if tErr, err := l.pool.cfg.SessionManager.ExtractLastTurnError(ctx, sessID, turn.CreatedAt); err == nil && (IsQuotaPause(tErr, "") || IsCapacityBlip(tErr, "")) {
			isQuota = true
		}
	}
	if isQuota {
		l.mu.Lock()
		l.cancelRotation = true
		l.mu.Unlock()
	} else if d != nil {
		d.MarkDirty()
	}
}

func (l *unifiedSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	if turn == nil {
		return nil, errors.New("turn context cannot be nil")
	}
	l.mu.Lock()
	l.cancelRotation = false
	l.mu.Unlock()

	turnCopy := *turn
	if turnCopy.Ctx == nil {
		turnCopy.Ctx = ctx
	}

	handleErr := func(err error) {
		l.mu.Lock()
		l.cancelRotation = true
		d := l.daemon
		l.mu.Unlock()
		if !IsQuotaPause(err.Error(), "") && !IsCapacityBlip(err.Error(), "") && d != nil {
			d.MarkDirty()
		}
	}

	if err := ctx.Err(); err != nil {
		handleErr(err)
		return nil, err
	}

	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	origSink := turn.Sink
	turnCopy.Sink = &leaseTurnSinkWrapper{inner: origSink, buf: bufSink}

	if err := l.ensureActiveDaemon(ctx); err != nil {
		handleErr(err)
		return nil, err
	}

	l.mu.RLock()
	d := l.daemon
	l.mu.RUnlock()
	if err := d.Send(compileLeasePrompt(&turnCopy, l.isCold, l.prevSessID), &turnCopy); err != nil {
		handleErr(err)
		return nil, err
	}

	res, waitErr := bufSink.Wait(ctx)
	l.mu.Lock()
	if l.daemon != nil && l.daemon.SessionID() != "" {
		l.sessionID = l.daemon.SessionID()
	}
	if res != nil && res.ConversationID != "" {
		l.sessionID = res.ConversationID
	}
	l.mu.Unlock()
	if waitErr != nil {
		handleErr(waitErr)
		return nil, waitErr
	}
	l.evaluateEmptyResponseOutcome(ctx, &turnCopy, res)
	l.turnCount++
	l.isCold = false
	return res, nil
}

func (l *unifiedSessionLease) Release() error {
	var releaseErr error
	l.releaseOnce.Do(func() {
		defer l.pool.releaseTargetLock(l.targetKey, l.lock, true)
		l.mu.RLock()
		cancelRot := l.cancelRotation
		targetKey := l.targetKey
		d := l.daemon
		poolClosed := l.pool.isClosed()
		turnCount := l.turnCount
		oldSessionID := l.sessionID
		l.mu.RUnlock()
		if cancelRot || strings.Contains(targetKey, "-ondemand-") || (d != nil && d.State() == StateClosed && !poolClosed) {
			return
		}
		shouldRotate, reason := l.pool.ShouldRotate(d)
		maxTurns := DefaultMaxSessionTurns
		if l.pool != nil && l.pool.cfg.MaxSessionTurns > 0 {
			maxTurns = l.pool.cfg.MaxSessionTurns
		}
		if !shouldRotate && turnCount >= maxTurns {
			shouldRotate, reason = true, fmt.Sprintf("reached maximum turn threshold (%d)", maxTurns)
		}
		if !shouldRotate {
			return
		}

		log.Printf("[UnifiedProcessPool] Session rotation triggered for %s (reason: %s)", targetKey, reason)
		metrics.RecordSessionRotation("turn_end", "pool", reason)

		newDaemon, rotErr := l.pool.RotateDaemon(context.Background(), targetKey, "")
		if rotErr != nil {
			log.Printf("[UnifiedProcessPool] Warning rotating daemon for %s: %v", targetKey, rotErr)
			if evictErr := l.pool.EvictSession(targetKey); evictErr != nil {
				log.Printf("[UnifiedProcessPool] Warning evicting session for %s: %v", targetKey, evictErr)
			}
			releaseErr = rotErr
			return
		}

		if newSessionID := newDaemon.SessionID(); l.pool.cfg.OnSessionRotated != nil && newSessionID != "" {
			if rotErr := l.pool.cfg.OnSessionRotated(context.Background(), targetKey, oldSessionID, newSessionID); rotErr != nil {
				log.Printf("[UnifiedProcessPool] Persistence callback failed during rotation for %s: %v. Evicting daemon to maintain DB truth.", targetKey, rotErr)
				if evictErr := l.pool.EvictSession(targetKey); evictErr != nil {
					log.Printf("[UnifiedProcessPool] Warning evicting session for %s: %v", targetKey, evictErr)
				}
				releaseErr = rotErr
			}
		}
	})
	return releaseErr
}
