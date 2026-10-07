package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/google/uuid"
)

// InterchangeablePoolConfig specifies sizing and naming configuration for an InterchangeablePool.
type InterchangeablePoolConfig struct {
	WorkerCount int    // defaults to 2 if <= 0
	KeyPrefix   string // defaults to "ephemeral:worker" if empty
}

// InterchangeablePool manages a fixed set of interchangeable low-effort worker daemons.
// Callers lease an available worker slot, execute a turn, and upon turn completion
// (or context cancellation / failure), the worker is automatically rotated in the background
// to ensure a fresh session and returned to the available pool.
type InterchangeablePool struct {
	underlying *UnifiedProcessPool
	workers    []string
	available  chan string
	mu         sync.Mutex
	closed     atomic.Bool
	closeOnce  sync.Once
	bgWg       sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

var (
	_ AgentPool       = (*InterchangeablePool)(nil)
	_ SessionRotator  = (*InterchangeablePool)(nil)
	_ LeasedAgentPool = (*InterchangeablePool)(nil)
)

// NewInterchangeablePool constructs an InterchangeablePool wrapping an underlying UnifiedProcessPool.
// It generates worker keys, registers them as pre-warmed targets on the underlying pool,
// and fills the available channel with all worker keys.
func NewInterchangeablePool(underlying *UnifiedProcessPool, cfg InterchangeablePoolConfig) *InterchangeablePool {
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 2
	}
	keyPrefix := strings.TrimSpace(cfg.KeyPrefix)
	if keyPrefix == "" {
		keyPrefix = "ephemeral:worker"
	}

	workers := make([]string, workerCount)
	available := make(chan string, workerCount)
	for i := 0; i < workerCount; i++ {
		key := fmt.Sprintf("%s-%d", keyPrefix, i)
		workers[i] = key
		available <- key
	}

	if underlying != nil {
		underlying.UpdatePrewarmedTargets(workers)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &InterchangeablePool{
		underlying: underlying,
		workers:    workers,
		available:  available,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Initialize pre-warms configured worker targets via the underlying process pool.
func (p *InterchangeablePool) Initialize(ctx context.Context) error {
	if p == nil {
		return errors.New("interchangeable pool is nil")
	}
	if p.closed.Load() {
		return errors.New("interchangeable pool is closed")
	}
	if p.underlying == nil {
		return nil
	}
	return p.underlying.Initialize(ctx)
}

// GetOrCreateSession leases an available worker from the pool, retrieves its StreamingDaemon,
// and returns a leasedSession that automatically triggers rotation and worker return upon turn completion.
func (p *InterchangeablePool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	if p == nil || p.closed.Load() {
		return nil, errors.New("interchangeable pool is closed")
	}

	var workerKey string
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, errors.New("interchangeable pool is closed")
	case key := <-p.available:
		workerKey = key
	}

	if p.closed.Load() {
		select {
		case p.available <- workerKey:
		default:
		}
		return nil, errors.New("interchangeable pool is closed")
	}

	if p.underlying == nil {
		select {
		case p.available <- workerKey:
		default:
		}
		return nil, errors.New("underlying pool is nil")
	}

	daemon, err := p.underlying.GetOrCreate(ctx, workerKey, "")
	if err != nil {
		if !p.closed.Load() {
			select {
			case p.available <- workerKey:
			default:
			}
		}
		return nil, err
	}
	if daemon == nil {
		if !p.closed.Load() {
			select {
			case p.available <- workerKey:
			default:
			}
		}
		return nil, errors.New("underlying pool returned nil daemon")
	}

	sess := &leasedSession{
		pool:      p,
		workerKey: workerKey,
		daemon:    daemon,
		completed: make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.Done():
			sess.release()
		case <-sess.completed:
		case <-p.ctx.Done():
			sess.release()
		}
	}()

	return sess, nil
}

// ShouldRotateSession reports whether the session needs rotation.
func (p *InterchangeablePool) ShouldRotateSession(sess AgentSession) (bool, string) {
	return false, ""
}

// RotateSession is a no-op for InterchangeablePool since worker rotation is handled per lease.
func (p *InterchangeablePool) RotateSession(ctx context.Context, targetKey string) (AgentSession, error) {
	return nil, nil
}

func (p *InterchangeablePool) rotateAndRelease(workerKey string) {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return
	}
	p.bgWg.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.bgWg.Done()
		rotCtx, cancel := context.WithTimeout(p.ctx, 35*time.Second)
		defer cancel()

		if p.underlying != nil {
			if _, err := p.underlying.RotateDaemon(rotCtx, workerKey, ""); err != nil {
				if !errors.Is(err, context.Canceled) && !p.closed.Load() {
					log.Printf("[InterchangeablePool] Warning: worker rotation failed for %q: %v", workerKey, err)
				}
			}
		}

		if !p.closed.Load() {
			select {
			case p.available <- workerKey:
			default:
			}
		}
	}()
}

// Close marks the pool as closed, terminates background routines, and closes the underlying pool.
func (p *InterchangeablePool) Close() error {
	if p == nil {
		return nil
	}
	var err error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed.Store(true)
		p.mu.Unlock()

		if p.cancel != nil {
			p.cancel()
		}
		if p.underlying != nil {
			err = p.underlying.Close()
		}
		p.bgWg.Wait()
	})
	return err
}

// ExecuteEphemeral leases a session, dispatches prompt synchronously, awaits result, and auto-rotates.
func (p *InterchangeablePool) ExecuteEphemeral(ctx context.Context, targetKey, prompt string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("interchangeable pool is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	sess, err := p.GetOrCreateSession(ctx, targetKey, "")
	if err != nil {
		return "", fmt.Errorf("failed to acquire leased session: %w", err)
	}
	sink := NewThrowawayTurnSink()
	turnCtx := &TurnContext{
		TurnID:    uuid.New().String(),
		SessionID: sess.SessionID(),
		Prompt:    prompt,
		Sink:      sink,
		CreatedAt: time.Now(),
		Ctx:       ctx,
	}
	if err := sess.Send(prompt, turnCtx); err != nil {
		return "", fmt.Errorf("failed sending turn to leased session: %w", err)
	}
	res, err := sink.ResultContext(ctx)
	if err != nil {
		return "", err
	}
	return res, nil
}

// EphemeralLLMFunc returns an LLMFunc bound to ExecuteEphemeral for targetKey.
func (p *InterchangeablePool) EphemeralLLMFunc(targetKey string) LLMFunc {
	return func(ctx context.Context, model, prompt string) (string, error) {
		if p == nil {
			return "", fmt.Errorf("interchangeable pool is nil")
		}
		return p.ExecuteEphemeral(ctx, targetKey, prompt)
	}
}

// Underlying returns the wrapped UnifiedProcessPool.
func (p *InterchangeablePool) Underlying() *UnifiedProcessPool {
	if p == nil {
		return nil
	}
	return p.underlying
}

// Model returns the configured model of the underlying pool.
func (p *InterchangeablePool) Model() string {
	if p == nil || p.underlying == nil {
		return ""
	}
	return p.underlying.Model()
}

// SetTranscriptRescuer updates the transcript rescuer callback on the underlying pool.
func (p *InterchangeablePool) SetTranscriptRescuer(rescuer func(convID string, since time.Time) string) {
	if p == nil || p.underlying == nil {
		return
	}
	p.underlying.SetTranscriptRescuer(rescuer)
}

// SetSessionManager updates the session manager on the underlying pool if supported.
func (p *InterchangeablePool) SetSessionManager(sm *session.Manager) {
	if p == nil || p.underlying == nil {
		return
	}
	p.underlying.SetSessionManager(sm)
}

// SetSessionCallbacks updates the session record lookup and rotation callbacks on the underlying pool if supported.
func (p *InterchangeablePool) SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error) {
	if p == nil || p.underlying == nil {
		return
	}
	p.underlying.SetSessionCallbacks(getRec, onRot)
}

// MarkDirty delegates dirty marking and eviction to the underlying pool.
func (p *InterchangeablePool) MarkDirty() {
	if p == nil || p.underlying == nil {
		return
	}
	p.underlying.MarkDirty()
}

// WaitBackground waits for in-flight worker rotations and underlying pool background tasks.
func (p *InterchangeablePool) WaitBackground() {
	if p == nil {
		return
	}
	p.bgWg.Wait()
	if p.underlying != nil {
		p.underlying.WaitBackground()
	}
}

// Get returns the StreamingDaemon for targetKey from the underlying pool if active.
func (p *InterchangeablePool) Get(targetKey string) (*StreamingDaemon, bool) {
	if p == nil || p.underlying == nil {
		return nil, false
	}
	return p.underlying.Get(targetKey)
}

// Workers returns a copy of the configured worker target keys.
func (p *InterchangeablePool) Workers() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.workers))
	copy(out, p.workers)
	return out
}

// AvailableCount returns the number of workers currently ready to be leased.
func (p *InterchangeablePool) AvailableCount() int {
	if p == nil {
		return 0
	}
	return len(p.available)
}

// leasedSession wraps an active StreamingDaemon leased from InterchangeablePool.
type leasedSession struct {
	pool        *InterchangeablePool
	workerKey   string
	daemon      *StreamingDaemon
	releaseOnce sync.Once
	completed   chan struct{}
}

var _ AgentSession = (*leasedSession)(nil)

func (s *leasedSession) release() {
	s.releaseOnce.Do(func() {
		if s.completed != nil {
			close(s.completed)
		}
		if s.pool != nil {
			s.pool.rotateAndRelease(s.workerKey)
		}
	})
}

// SessionID returns the latched session UUID of the underlying daemon.
func (s *leasedSession) SessionID() string {
	if s.daemon == nil {
		return ""
	}
	return s.daemon.SessionID()
}

// Send delegates prompt execution to the underlying daemon, wrapping the TurnContext.Sink
// so that rotation and worker return are triggered as soon as OnResult or OnError fires.
func (s *leasedSession) Send(prompt string, turnCtx *TurnContext) error {
	if turnCtx == nil {
		defer s.release()
		if s.daemon == nil {
			return errors.New("daemon is nil")
		}
		return s.daemon.Send(prompt, nil)
	}

	wrappedSink := &releaseTurnSink{
		inner:   turnCtx.Sink,
		release: s.release,
	}

	turnCopy := *turnCtx
	turnCopy.Sink = wrappedSink

	if s.daemon == nil {
		s.release()
		return errors.New("daemon is nil")
	}

	if err := s.daemon.Send(prompt, &turnCopy); err != nil {
		s.release()
		return err
	}
	return nil
}

// Close immediately triggers release and background rotation if not already triggered.
func (s *leasedSession) Close() error {
	s.release()
	return nil
}

// releaseTurnSink intercepts terminal turn events to release the leased worker.
type releaseTurnSink struct {
	inner       TurnSink
	releaseOnce sync.Once
	release     func()
}

func (s *releaseTurnSink) doRelease() {
	s.releaseOnce.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

func (s *releaseTurnSink) OnTurnStarted() {
	if s.inner != nil {
		s.inner.OnTurnStarted()
	}
}

func (s *releaseTurnSink) OnThinking() {
	if s.inner != nil {
		s.inner.OnThinking()
	}
}

func (s *releaseTurnSink) OnStepStarted(stepIndex int) {
	if s.inner != nil {
		if sa, ok := s.inner.(StepAwareSink); ok {
			sa.OnStepStarted(stepIndex)
		}
	}
}

func (s *releaseTurnSink) OnToolCall(toolName, commandName string) {
	if s.inner != nil {
		s.inner.OnToolCall(toolName, commandName)
	}
}

func (s *releaseTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	if s.inner != nil {
		s.inner.OnToolCompleted(toolName, mcpServer, duration, status)
	}
}

func (s *releaseTurnSink) OnSkillActivated(skillName, source string) {
	if s.inner != nil {
		s.inner.OnSkillActivated(skillName, source)
	}
}

func (s *releaseTurnSink) OnTextDelta(delta string) {
	if s.inner != nil {
		s.inner.OnTextDelta(delta)
	}
}

func (s *releaseTurnSink) OnResult(res *TurnResult) {
	defer s.doRelease()
	if s.inner != nil {
		s.inner.OnResult(res)
	}
}

func (s *releaseTurnSink) OnError(err error) {
	defer s.doRelease()
	if s.inner != nil {
		s.inner.OnError(err)
	}
}

// AcquireLease leases an available worker slot and delegates lease acquisition to the underlying pool.
func (p *InterchangeablePool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	if p == nil {
		return nil, errors.New("interchangeable pool is uninitialized")
	}
	if p.closed.Load() {
		return nil, errors.New("interchangeable pool is closed")
	}

	var workerKey string
	var pDone <-chan struct{}
	if p.ctx != nil {
		pDone = p.ctx.Done()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pDone:
		return nil, errors.New("interchangeable pool is closed")
	case key := <-p.available:
		workerKey = key
	}

	if p.closed.Load() {
		select {
		case p.available <- workerKey:
		default:
		}
		return nil, errors.New("interchangeable pool is closed")
	}

	if p.underlying == nil {
		select {
		case p.available <- workerKey:
		default:
		}
		return nil, errors.New("underlying pool is nil")
	}

	lease, err := p.underlying.AcquireLease(ctx, workerKey)
	if err != nil {
		if !p.closed.Load() {
			select {
			case p.available <- workerKey:
			default:
			}
		}
		return nil, err
	}

	return &interchangeableSessionLease{
		inner:     lease,
		pool:      p,
		workerKey: workerKey,
	}, nil
}

type interchangeableSessionLease struct {
	inner     SessionLease
	pool      *InterchangeablePool
	workerKey string
	once      sync.Once
}

var _ SessionLease = (*interchangeableSessionLease)(nil)

func (l *interchangeableSessionLease) SessionID() string {
	return l.inner.SessionID()
}

func (l *interchangeableSessionLease) PreviousSessionID() string {
	return l.inner.PreviousSessionID()
}

func (l *interchangeableSessionLease) IsCold() bool {
	return l.inner.IsCold()
}

func (l *interchangeableSessionLease) TurnCount() int {
	return l.inner.TurnCount()
}

func (l *interchangeableSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	return l.inner.Execute(ctx, turn)
}

func (l *interchangeableSessionLease) Release() error {
	var err error
	l.once.Do(func() {
		err = l.inner.Release()
		if !l.pool.closed.Load() {
			select {
			case l.pool.available <- l.workerKey:
			default:
			}
		}
	})
	return err
}
