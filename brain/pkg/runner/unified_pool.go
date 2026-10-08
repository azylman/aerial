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

	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/session"
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
	PrewarmedTargets  []string
	MaxIdle           time.Duration
	TranscriptRescuer       func(convID string, since time.Time) string
	MemoryRetriever         MemoryRetriever
	AmbientContextRetriever AmbientContextRetriever
	SessionManager          *session.Manager
	OnSessionRotated        func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error
	GetSessionRecord        func(ctx context.Context, targetKey string) (SessionRecord, error)
	MaxSessionTurns         int
	TargetLockTimeout       time.Duration
}

// UnifiedProcessPool manages pinned daemons and singleflight pre-warming.
type UnifiedProcessPool struct {
	cfg        PoolConfig
	spawner    DaemonSpawner
	daemons    map[string]*StreamingDaemon
	prewarming map[string]bool
	mu         sync.RWMutex
	sf         singleflight.Group
	bgWg       sync.WaitGroup
	closed     bool
	closeOnce  sync.Once
	closeErr   error
	ctx         context.Context
	cancel      context.CancelFunc
	targetLocks map[string]*targetLock
	targetMu    sync.Mutex
}

var (
	_ AgentPool       = (*UnifiedProcessPool)(nil)
	_ SessionRotator  = (*UnifiedProcessPool)(nil)
	_ LeasedAgentPool = (*UnifiedProcessPool)(nil)
	_ AgentSession    = (*StreamingDaemon)(nil)
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
	cfg.Model = resolvedModel
	cfg.DefaultModel = resolvedModel
	return &UnifiedProcessPool{
		cfg:        cfg,
		spawner:    spawner,
		daemons:     make(map[string]*StreamingDaemon),
		prewarming:  make(map[string]bool),
		targetLocks: make(map[string]*targetLock),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Model returns the immutable model name bound to this process pool.
func (p *UnifiedProcessPool) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

// SetTranscriptRescuer updates the transcript rescuer callback on the pool and active daemons.
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
	defer p.mu.Unlock()
	p.cfg.SessionManager = sm
}

// SetSessionCallbacks updates the session record lookup and rotation callbacks.
func (p *UnifiedProcessPool) SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.GetSessionRecord = getRec
	p.cfg.OnSessionRotated = onRot
}

// Initialize pre-warms configured target daemons sequentially in the caller's context.
func (p *UnifiedProcessPool) Initialize(ctx context.Context) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return fmt.Errorf("process pool is closed")
	}
	p.mu.RUnlock()

	if ctx == nil {
		ctx = context.Background()
	}

	for _, target := range p.cfg.PrewarmedTargets {
		p.mu.RLock()
		closed := p.closed
		p.mu.RUnlock()
		if closed {
			return fmt.Errorf("process pool is closed")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		initCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
		if _, err := p.GetOrCreate(initCtx, target, ""); err != nil {
			log.Printf("[UnifiedProcessPool] Warning: pre-warming target %q failed: %v", target, err)
		} else {
			log.Printf("[UnifiedProcessPool] Pre-warmed target %q successfully", target)
		}
		cancel()
	}
	return nil
}

// GetOrCreate retrieves an active StreamingDaemon for targetKey, or starts one via singleflight.
// If sessionID is non-empty, it is passed to the daemon config to resume an existing conversation.
func (p *UnifiedProcessPool) GetOrCreate(ctx context.Context, targetKey string, sessionID string) (*StreamingDaemon, error) {
	trimmedSess := strings.TrimSpace(sessionID)
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("process pool is closed")
	}
	if d, exists := p.daemons[targetKey]; exists {
		if should, _ := p.ShouldRotate(d); !should && isDaemonMatch(d, trimmedSess) {
			p.mu.RUnlock()
			return d, nil
		}
	}
	p.mu.RUnlock()

	res, err, _ := p.sf.Do(targetKey, func() (any, error) {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("process pool is closed")
		}
		if d, exists := p.daemons[targetKey]; exists && d != nil {
			should, rotReason := p.ShouldRotate(d)
			if !should && isDaemonMatch(d, trimmedSess) {
				p.mu.Unlock()
				return d, nil
			}
			delete(p.daemons, targetKey)
			p.bgWg.Add(1)
			go func(oldD *StreamingDaemon, tKey string) {
				defer p.bgWg.Done()
				if closeErr := oldD.Close(); closeErr != nil {
					log.Printf("[UnifiedProcessPool] Warning: error closing old daemon %q: %v", tKey, closeErr)
				}
			}(d, targetKey)
			if should {
				metrics.RecordSessionRotation("pre_flight", "pool", rotReason)
				trimmedSess = ""
			}
		}
		p.mu.Unlock()

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
			ThreadID:                targetKey,
			SessionID:               trimmedSess,
			Model:                   p.cfg.Model,
			AgyBin:                  p.cfg.AgyBin,
			Cwd:                     p.cfg.Cwd,
			Env:                     daemonEnv,
			GeminiHomeDir:           p.cfg.GeminiHomeDir,
			TranscriptRescuer:       p.cfg.TranscriptRescuer,
			MemoryRetriever:         p.cfg.MemoryRetriever,
			AmbientContextRetriever: p.cfg.AmbientContextRetriever,
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

// GetOrCreateSession retrieves an active AgentSession for targetKey, satisfying runner.AgentPool.
func (p *UnifiedProcessPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	d, err := p.GetOrCreate(ctx, targetKey, sessionID)
	if err != nil {
		return nil, err // return untyped nil interface
	}
	return d, nil
}

// EvictSession closes and removes the active daemon for targetKey.
func (p *UnifiedProcessPool) EvictSession(targetKey string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	d, exists := p.daemons[targetKey]
	if exists {
		delete(p.daemons, targetKey)
	}
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

// WaitBackground waits for all active background eviction and pre-warming operations to complete.
func (p *UnifiedProcessPool) WaitBackground() {
	if p == nil {
		return
	}
	p.bgWg.Wait()
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

// PrewarmedTargets returns a defensive copy of configured prewarmed targets.
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

// UpdatePrewarmedTargets dynamically updates the configured prewarmed targets.
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

	type evictedEntry struct {
		targetKey string
		daemon    *StreamingDaemon
	}
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
		if d.InflightCount() == 0 {
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

	// Reconcile prewarmed targets: ensure newly added or missing prewarmed targets
	// that are not currently running or already prewarming get eagerly spawned.
	for _, target := range p.cfg.PrewarmedTargets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		d, exists := p.daemons[target]
		if (!exists || d == nil || d.State() == StateClosed) && !p.prewarming[target] {
			p.prewarming[target] = true
			toPrewarm = append(toPrewarm, target)
		}
	}

	for range toClose {
		p.bgWg.Add(1)
	}
	for range toPrewarm {
		p.bgWg.Add(1)
	}
	p.mu.Unlock()

	for _, entry := range toClose {
		go func(e evictedEntry) {
			defer p.bgWg.Done()
			if closeErr := e.daemon.Close(); closeErr != nil {
				log.Printf("[UnifiedProcessPool] Warning: error closing evicted daemon %q: %v", e.targetKey, closeErr)
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
			if _, err := p.GetOrCreate(initCtx, tKey, ""); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("[UnifiedProcessPool] Warning: re-warming prewarmed target %q failed: %v", tKey, err)
				}
			}
		}(target)
	}
}

// Session rotation thresholds for memory pressure mitigation and context compaction.
const (
	DefaultMaxSessionTurns    = session.DefaultMaxSessionTurns
	DefaultMaxSessionSteps    = session.DefaultMaxSessionSteps
	DefaultMaxTranscriptByte  = session.DefaultMaxTranscriptBytes
	DefaultMaxTranscriptBytes = session.DefaultMaxTranscriptBytes
	DefaultMaxSessionDBBytes  = session.DefaultMaxSessionDBBytes
)

// ShouldRotate evaluates whether a daemon has reached its session lifetime thresholds
// (DefaultMaxSessionTurns turns, DefaultMaxSessionSteps steps, DefaultMaxTranscriptBytes,
// or DefaultMaxSessionDBBytes) and is safe to rotate (no in-flight turns).
func (p *UnifiedProcessPool) ShouldRotate(d *StreamingDaemon) (bool, string) {
	if d == nil {
		return false, ""
	}
	if d.InflightCount() > 0 {
		return false, "" // Never rotate during in-flight turn
	}
	if d.TaskTracker() != nil && d.TaskTracker().ActiveCount() > 0 {
		return false, "" // Never rotate while background tasks are running
	}
	if d.IsDirty() {
		return true, "daemon marked dirty during in-flight turn"
	}
	if d.TurnCount() >= DefaultMaxSessionTurns {
		return true, fmt.Sprintf("turn count threshold exceeded (%d >= %d)", d.TurnCount(), DefaultMaxSessionTurns)
	}
	// Note: Step count limits are evaluated canonically against transcript.jsonl via SessionManager.CountTranscriptSteps below.
	// d.StepCount() tracks stdout NDJSON streaming chunk volume, which must not be confused with trajectory steps.
	if p != nil && p.cfg.SessionManager != nil && d.SessionID() != "" {
		if turns := p.cfg.SessionManager.CountTranscriptTurns(d.SessionID()); turns >= DefaultMaxSessionTurns {
			return true, fmt.Sprintf("transcript turn count threshold exceeded (%d >= %d)", turns, DefaultMaxSessionTurns)
		}
		if steps := p.cfg.SessionManager.CountTranscriptSteps(d.SessionID()); steps >= DefaultMaxSessionSteps {
			return true, fmt.Sprintf("transcript step count threshold exceeded (%d >= %d)", steps, DefaultMaxSessionSteps)
		}
		if size := p.cfg.SessionManager.GetTranscriptSize(d.SessionID()); size >= DefaultMaxTranscriptBytes {
			return true, fmt.Sprintf("transcript file size threshold exceeded (%d >= %d bytes)", size, DefaultMaxTranscriptBytes)
		}
		if dbSize := p.cfg.SessionManager.GetSessionDBSize(d.SessionID()); dbSize >= DefaultMaxSessionDBBytes {
			return true, fmt.Sprintf("session DB size threshold exceeded (%d >= %d bytes)", dbSize, DefaultMaxSessionDBBytes)
		}
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

	newDaemon, err := p.GetOrCreate(ctx, targetKey, "")
	if err != nil {
		return nil, fmt.Errorf("failed creating rotated daemon for %q: %w", targetKey, err)
	}

	return newDaemon, nil
}

// ShouldRotateSession checks if the given AgentSession exceeds lifetime rotation thresholds.
func (p *UnifiedProcessPool) ShouldRotateSession(sess AgentSession) (bool, string) {
	if p == nil || sess == nil {
		return false, ""
	}
	if d, ok := sess.(*StreamingDaemon); ok {
		return p.ShouldRotate(d)
	}
	return false, ""
}

// RotateSession evicts the existing daemon for targetKey and synchronously spawns a fresh replacement daemon.
func (p *UnifiedProcessPool) RotateSession(ctx context.Context, targetKey string) (AgentSession, error) {
	d, err := p.RotateDaemon(ctx, targetKey, "")
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, nil
	}
	return d, nil
}

// ExecuteEphemeral dispatches a prompt to targetKey's daemon and synchronously awaits the response.
// It performs proactive input validation and automatically triggers asynchronous session rotation
// if the daemon reaches its lifetime thresholds (DefaultMaxSessionTurns turns).
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

func (p *UnifiedProcessPool) acquireTargetLock(ctx context.Context, targetKey string) (*targetLock, error) {
	p.targetMu.Lock()
	if p.targetLocks == nil {
		p.targetLocks = make(map[string]*targetLock)
	}
	tl, ok := p.targetLocks[targetKey]
	if !ok {
		tl = &targetLock{
			sem: make(chan struct{}, 1),
		}
		p.targetLocks[targetKey] = tl
	}
	tl.refs++
	p.targetMu.Unlock()

	timeout := p.cfg.TargetLockTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	lockCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case tl.sem <- struct{}{}:
		return tl, nil
	case <-lockCtx.Done():
		p.releaseTargetLock(targetKey, tl, false)
		return nil, fmt.Errorf("timeout waiting for target lock for %s: %w", targetKey, lockCtx.Err())
	}
}

func (p *UnifiedProcessPool) releaseTargetLock(targetKey string, tl *targetLock, held bool) {
	if held {
		select {
		case <-tl.sem:
		default:
		}
	}
	p.targetMu.Lock()
	tl.refs--
	if tl.refs <= 0 {
		delete(p.targetLocks, targetKey)
	}
	p.targetMu.Unlock()
}

// AcquireLease blocks until the target lock is acquired, ensures a daemon is initialized,
// and returns an exclusive SessionLease.
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

	d, dErr := p.GetOrCreate(ctx, trimmedKey, rec.ActiveSessionID)
	if dErr != nil {
		p.releaseTargetLock(trimmedKey, tl, true)
		return nil, fmt.Errorf("failed to get or create daemon for %s: %w", trimmedKey, dErr)
	}

	isCold := false
	turnCount := 0
	if rec.TurnCount <= 1 || rec.ActiveSessionID == "" {
		isCold = true
	} else {
		turnCount = rec.TurnCount
	}

	sessID := d.SessionID()
	if sessID == "" {
		sessID = rec.ActiveSessionID
	}

	lease := &unifiedSessionLease{
		pool:       p,
		daemon:     d,
		targetKey:  trimmedKey,
		sessionID:  sessID,
		prevSessID: rec.PreviousSessionID,
		isCold:     isCold,
		turnCount:  turnCount,
		lock:       tl,
	}
	return lease, nil
}

type unifiedSessionLease struct {
	pool        *UnifiedProcessPool
	daemon      *StreamingDaemon
	targetKey   string
	sessionID   string
	prevSessID  string
	isCold      bool
	turnCount      int
	cancelRotation bool
	lock           *targetLock
	releaseOnce    sync.Once
}

var _ SessionLease = (*unifiedSessionLease)(nil)

func (l *unifiedSessionLease) SessionID() string {
	if l.daemon != nil && l.daemon.SessionID() != "" {
		return l.daemon.SessionID()
	}
	return l.sessionID
}

func (l *unifiedSessionLease) PreviousSessionID() string {
	return l.prevSessID
}

func (l *unifiedSessionLease) IsCold() bool {
	return l.isCold
}

func (l *unifiedSessionLease) TurnCount() int {
	return l.turnCount
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

func (s *leaseTurnSinkWrapper) OnStepStarted(stepIndex int) {
	if s.inner != nil {
		if sa, ok := s.inner.(StepAwareSink); ok {
			sa.OnStepStarted(stepIndex)
		}
	}
	if s.buf != nil {
		s.buf.OnStepStarted(stepIndex)
	}
}

func (s *leaseTurnSinkWrapper) OnToolCall(toolName, commandName string) {
	if s.inner != nil {
		s.inner.OnToolCall(toolName, commandName)
	}
	s.buf.OnToolCall(toolName, commandName)
}

func (s *leaseTurnSinkWrapper) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	if s.inner != nil {
		s.inner.OnToolCompleted(toolName, mcpServer, duration, status)
	}
	s.buf.OnToolCompleted(toolName, mcpServer, duration, status)
}

func (s *leaseTurnSinkWrapper) OnSkillActivated(skillName, source string) {
	if s.inner != nil {
		s.inner.OnSkillActivated(skillName, source)
	}
	s.buf.OnSkillActivated(skillName, source)
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

func (l *unifiedSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	if turn == nil {
		return nil, errors.New("turn context cannot be nil")
	}
	if turn.Ctx == nil {
		turn.Ctx = ctx
	}

	compiledPrompt := turn.Prompt
	if l.isCold {
		scopeInst := strings.TrimSpace(turn.ScopeInstructions)
		if scopeInst == "" {
			scopeInst = strings.TrimSpace(turn.ChannelInstructions)
		}
		if scopeInst != "" && !strings.Contains(compiledPrompt, "<SCOPE_INSTRUCTIONS>") && !strings.Contains(compiledPrompt, "<CHANNEL_INSTRUCTIONS>") {
			scopeBlock := fmt.Sprintf("<SCOPE_INSTRUCTIONS>\nScope-specific guidelines for this conversation:\n\n%s\n</SCOPE_INSTRUCTIONS>", scopeInst)
			compiledPrompt = scopeBlock + "\n\n" + compiledPrompt
		}
		if l.prevSessID != "" && !strings.Contains(compiledPrompt, "<PREVIOUS_SESSION>") {
			prevBlock := fmt.Sprintf("<PREVIOUS_SESSION>\nPrevious Session ID: %s\n</PREVIOUS_SESSION>", l.prevSessID)
			compiledPrompt = prevBlock + "\n\n" + compiledPrompt
		}
		if strings.TrimSpace(turn.ThreadSummary) != "" && !strings.Contains(compiledPrompt, "<THREAD_SUMMARY>") {
			sumBlock := fmt.Sprintf("<THREAD_SUMMARY>\n%s\n</THREAD_SUMMARY>", strings.TrimSpace(turn.ThreadSummary))
			compiledPrompt = sumBlock + "\n\n" + compiledPrompt
		}
	}
	if strings.TrimSpace(turn.CoordinationContext) != "" && !strings.Contains(compiledPrompt, "<COORDINATION_CONTEXT>") {
		coordBlock := fmt.Sprintf("<COORDINATION_CONTEXT>\n%s\n</COORDINATION_CONTEXT>", strings.TrimSpace(turn.CoordinationContext))
		compiledPrompt = coordBlock + "\n\n" + compiledPrompt
	}

	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	combinedSink := &leaseTurnSinkWrapper{
		inner: turn.Sink,
		buf:   bufSink,
	}
	origSink := turn.Sink
	turn.Sink = combinedSink
	defer func() {
		turn.Sink = origSink
	}()

	if err := l.daemon.Send(compiledPrompt, turn); err != nil {
		if IsQuotaPause(err.Error(), "") || IsCapacityBlip(err.Error(), "") {
			l.cancelRotation = true
		}
		return nil, err
	}

	res, waitErr := bufSink.Wait(ctx)
	if l.daemon != nil && l.daemon.SessionID() != "" {
		l.sessionID = l.daemon.SessionID()
	}
	if res != nil && res.ConversationID != "" {
		l.sessionID = res.ConversationID
	}
	if waitErr != nil {
		if IsQuotaPause(waitErr.Error(), "") || IsCapacityBlip(waitErr.Error(), "") {
			l.cancelRotation = true
		}
		return nil, waitErr
	}
	if res != nil {
		if IsQuotaPause(res.Stderr, "") || IsCapacityBlip(res.Stderr, "") || IsQuotaPause(res.Response, "") || IsCapacityBlip(res.Response, "") {
			l.cancelRotation = true
		} else if strings.TrimSpace(res.Response) == "" && l.pool != nil && l.pool.cfg.SessionManager != nil && l.sessionID != "" {
			if tErr, err := l.pool.cfg.SessionManager.ExtractLastTurnError(ctx, l.sessionID, turn.CreatedAt); err == nil && tErr != "" {
				if IsQuotaPause(tErr, "") || IsCapacityBlip(tErr, "") {
					l.cancelRotation = true
				}
			}
		}
	}
	l.turnCount++
	l.isCold = false
	return res, nil
}

func (l *unifiedSessionLease) Release() error {
	var releaseErr error
	l.releaseOnce.Do(func() {
		defer l.pool.releaseTargetLock(l.targetKey, l.lock, true)

		if l.cancelRotation {
			return
		}
		if l.daemon != nil && l.daemon.TaskTracker() != nil && l.daemon.TaskTracker().ActiveCount() > 0 {
			return
		}

		shouldRotate, reason := l.pool.ShouldRotate(l.daemon)
		if !shouldRotate && l.pool.cfg.MaxSessionTurns > 0 && l.turnCount >= l.pool.cfg.MaxSessionTurns {
			shouldRotate = true
			reason = fmt.Sprintf("reached maximum turn threshold (%d)", l.pool.cfg.MaxSessionTurns)
		}

		if shouldRotate {
			log.Printf("[UnifiedProcessPool] Session rotation triggered for %s (reason: %s)", l.targetKey, reason)
			newSessionID := uuid.New().String()
			oldSessionID := l.sessionID

			if l.pool.cfg.OnSessionRotated != nil {
				if rotErr := l.pool.cfg.OnSessionRotated(context.Background(), l.targetKey, oldSessionID, newSessionID); rotErr != nil {
					log.Printf("[UnifiedProcessPool] Persistence callback failed during rotation for %s: %v. Evicting daemon to maintain DB truth.", l.targetKey, rotErr)
					if evictErr := l.pool.EvictSession(l.targetKey); evictErr != nil {
						log.Printf("[UnifiedProcessPool] Warning evicting session for %s: %v", l.targetKey, evictErr)
					}
					releaseErr = rotErr
					return
				}
			}

			if _, rotErr := l.pool.RotateDaemon(context.Background(), l.targetKey, ""); rotErr != nil {
				log.Printf("[UnifiedProcessPool] Warning rotating daemon for %s: %v", l.targetKey, rotErr)
				if evictErr := l.pool.EvictSession(l.targetKey); evictErr != nil {
					log.Printf("[UnifiedProcessPool] Warning evicting session for %s: %v", l.targetKey, evictErr)
				}
			}
		}
	})
	return releaseErr
}
