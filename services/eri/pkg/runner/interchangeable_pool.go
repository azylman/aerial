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

	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/google/uuid"
)

// InterchangeablePoolConfig specifies sizing and naming configuration for an InterchangeablePool.
type InterchangeablePoolConfig struct {
	WorkerCount int    // defaults to 2 if <= 0
	KeyPrefix   string // defaults to "ephemeral:worker" if empty
}

// InterchangeablePool manages a fixed set of interchangeable low-effort worker daemons.
type InterchangeablePool struct {
	underlying  *UnifiedProcessPool
	workers     []string
	keyPrefix   string
	overflowSeq atomic.Uint64
	available   chan string
	mu          sync.Mutex
	closed      atomic.Bool
	closeOnce   sync.Once
	bgWg        sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

var (
	_ AgentPool       = (*InterchangeablePool)(nil)
	_ SessionRotator  = (*InterchangeablePool)(nil)
	_ LeasedAgentPool = (*InterchangeablePool)(nil)
)

// NewInterchangeablePool constructs an InterchangeablePool wrapping an underlying UnifiedProcessPool.
func NewInterchangeablePool(underlying *UnifiedProcessPool, cfg InterchangeablePoolConfig) *InterchangeablePool {
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 2
	}
	keyPrefix := strings.TrimSpace(cfg.KeyPrefix)
	if keyPrefix == "" {
		keyPrefix = "ephemeral:worker"
	}

	workers, available := make([]string, workerCount), make(chan string, workerCount)
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
		underlying: underlying, workers: workers, keyPrefix: keyPrefix,
		available: available, ctx: ctx, cancel: cancel,
	}
}

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

func (p *InterchangeablePool) acquireWorkerSlot(ctx context.Context) (string, bool, error) {
	if p.closed.Load() || (p.ctx != nil && p.ctx.Err() != nil) {
		return "", false, errors.New("interchangeable pool is closed")
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	select {
	case key := <-p.available:
		return key, false, nil
	default:
		prefix := p.keyPrefix
		if prefix == "" {
			prefix = "ephemeral:worker"
		}
		return fmt.Sprintf("%s-ondemand-%d", prefix, p.overflowSeq.Add(1)), true, nil
	}
}

// GetOrCreateSession leases an available worker from the pool and returns a leasedSession.
func (p *InterchangeablePool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	if p == nil {
		return nil, errors.New("interchangeable pool is closed")
	}
	workerKey, isOverflow, err := p.acquireWorkerSlot(ctx)
	if err != nil {
		return nil, err
	}
	if p.underlying == nil {
		p.returnWorker(workerKey)
		return nil, errors.New("underlying pool is nil")
	}
	daemon, err := p.underlying.GetOrCreate(ctx, workerKey, "")
	if err != nil {
		p.returnWorker(workerKey)
		return nil, err
	}
	if daemon == nil {
		p.returnWorker(workerKey)
		return nil, errors.New("underlying pool returned nil daemon")
	}

	sess := &leasedSession{
		pool: p, workerKey: workerKey, isOverflow: isOverflow,
		daemon: daemon, completed: make(chan struct{}),
	}
	var pDone <-chan struct{}
	if p.ctx != nil {
		pDone = p.ctx.Done()
	}
	go func() {
		select {
		case <-ctx.Done():
			sess.release()
		case <-sess.completed:
		case <-pDone:
			sess.release()
		}
	}()
	return sess, nil
}

func (p *InterchangeablePool) ShouldRotateSession(sess AgentSession) (bool, string) { return false, "" }

func (p *InterchangeablePool) RotateSession(ctx context.Context, targetKey string) (AgentSession, error) {
	return nil, nil
}

func (p *InterchangeablePool) releaseWorkerKey(workerKey string, isOverflow bool) {
	if p == nil {
		return
	}
	if isOverflow {
		p.evictOverflow(workerKey)
	} else {
		p.rotateAndRelease(workerKey)
	}
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
			if _, err := p.underlying.RotateDaemon(rotCtx, workerKey, ""); err != nil && !errors.Is(err, context.Canceled) && !p.closed.Load() {
				log.Printf("[InterchangeablePool] Warning: worker rotation failed for %q: %v", workerKey, err)
			}
		}
		p.returnWorker(workerKey)
	}()
}

func (p *InterchangeablePool) evictOverflow(workerKey string) {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return
	}
	p.bgWg.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.bgWg.Done()
		if p.underlying != nil {
			if err := p.underlying.EvictSession(workerKey); err != nil && !p.closed.Load() {
				log.Printf("[InterchangeablePool] Warning: overflow worker eviction failed for %q: %v", workerKey, err)
			}
		}
	}()
}

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
		TurnID: uuid.New().String(), SessionID: sess.SessionID(),
		Prompt: prompt, Sink: sink, CreatedAt: time.Now(), Ctx: ctx,
	}
	if err := sess.Send(prompt, turnCtx); err != nil {
		return "", fmt.Errorf("failed sending turn to leased session: %w", err)
	}
	return sink.ResultContext(ctx)
}

func (p *InterchangeablePool) EphemeralLLMFunc(targetKey string) LLMFunc {
	return func(ctx context.Context, model, prompt string) (string, error) {
		if p == nil {
			return "", fmt.Errorf("interchangeable pool is nil")
		}
		return p.ExecuteEphemeral(ctx, targetKey, prompt)
	}
}

func (p *InterchangeablePool) Underlying() *UnifiedProcessPool {
	if p == nil {
		return nil
	}
	return p.underlying
}

func (p *InterchangeablePool) Model() string {
	if p == nil || p.underlying == nil {
		return ""
	}
	return p.underlying.Model()
}

func (p *InterchangeablePool) SetTranscriptRescuer(r func(string, time.Time) string) {
	if p != nil && p.underlying != nil {
		p.underlying.SetTranscriptRescuer(r)
	}
}

func (p *InterchangeablePool) SetSessionManager(sm *session.Manager) {
	if p != nil && p.underlying != nil {
		p.underlying.SetSessionManager(sm)
	}
}

func (p *InterchangeablePool) SetSessionCallbacks(getRec func(ctx context.Context, targetKey string) (SessionRecord, error), onRot func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error) {
	if p != nil && p.underlying != nil {
		p.underlying.SetSessionCallbacks(getRec, onRot)
	}
}

func (p *InterchangeablePool) MarkDirty() {
	if p != nil && p.underlying != nil {
		p.underlying.MarkDirty()
	}
}

func (p *InterchangeablePool) WaitBackground() {
	if p == nil {
		return
	}
	p.bgWg.Wait()
	if p.underlying != nil {
		p.underlying.WaitBackground()
	}
}

func (p *InterchangeablePool) Get(targetKey string) (*StreamingDaemon, bool) {
	if p == nil || p.underlying == nil {
		return nil, false
	}
	return p.underlying.Get(targetKey)
}

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

func (p *InterchangeablePool) AvailableCount() int {
	if p == nil {
		return 0
	}
	return len(p.available)
}

func (p *InterchangeablePool) returnWorker(workerKey string) {
	if p == nil || strings.Contains(workerKey, "-ondemand-") {
		return
	}
	select {
	case p.available <- workerKey:
	default:
	}
}

type leasedSession struct {
	pool        *InterchangeablePool
	workerKey   string
	isOverflow  bool
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
			s.pool.releaseWorkerKey(s.workerKey, s.isOverflow)
		}
	})
}

func (s *leasedSession) SessionID() string {
	if s.daemon == nil {
		return ""
	}
	return s.daemon.SessionID()
}

func (s *leasedSession) Send(prompt string, turnCtx *TurnContext) error {
	if turnCtx == nil {
		defer s.release()
		if s.daemon == nil {
			return errors.New("daemon is nil")
		}
		return s.daemon.Send(prompt, nil)
	}
	wrappedSink := &releaseTurnSink{inner: turnCtx.Sink, release: s.release}
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

func (s *leasedSession) Close() error {
	s.release()
	return nil
}

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
	if sa, ok := s.inner.(StepAwareSink); ok {
		sa.OnStepStarted(stepIndex)
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
	s.doRelease()
	if s.inner != nil {
		s.inner.OnResult(res)
	}
}

func (s *releaseTurnSink) OnError(err error) {
	s.doRelease()
	if s.inner != nil {
		s.inner.OnError(err)
	}
}

func (p *InterchangeablePool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	if p == nil {
		return nil, errors.New("interchangeable pool is uninitialized")
	}
	workerKey, isOverflow, err := p.acquireWorkerSlot(ctx)
	if err != nil {
		return nil, err
	}
	if p.underlying == nil {
		p.returnWorker(workerKey)
		return nil, errors.New("underlying pool is nil")
	}
	lease, err := p.underlying.AcquireLease(ctx, workerKey)
	if err != nil {
		p.returnWorker(workerKey)
		return nil, err
	}
	return &interchangeableSessionLease{
		inner: lease, pool: p, workerKey: workerKey, isOverflow: isOverflow,
	}, nil
}

type interchangeableSessionLease struct {
	inner      SessionLease
	pool       *InterchangeablePool
	workerKey  string
	isOverflow bool
	once       sync.Once
}

var _ SessionLease = (*interchangeableSessionLease)(nil)

func (l *interchangeableSessionLease) SessionID() string {
	if l == nil || l.inner == nil {
		return ""
	}
	return l.inner.SessionID()
}
func (l *interchangeableSessionLease) PreviousSessionID() string { return "" }
func (l *interchangeableSessionLease) IsCold() bool              { return false }
func (l *interchangeableSessionLease) IsEphemeral() bool         { return true }
func (l *interchangeableSessionLease) TurnCount() int {
	if l == nil || l.inner == nil {
		return 0
	}
	return l.inner.TurnCount()
}

func (l *interchangeableSessionLease) MarkDirty() {
	if l != nil && l.inner != nil {
		l.inner.MarkDirty()
	}
}

func (l *interchangeableSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("interchangeable session lease is uninitialized")
	}
	if turn != nil && turn.ThreadSummary != "" {
		turnCopy := *turn
		turnCopy.ThreadSummary = ""
		return l.inner.Execute(ctx, &turnCopy)
	}
	return l.inner.Execute(ctx, turn)
}

func (l *interchangeableSessionLease) Release() error {
	if l == nil {
		return nil
	}
	var err error
	l.once.Do(func() {
		if l.inner != nil {
			err = l.inner.Release()
		}
		if l.pool != nil {
			l.pool.releaseWorkerKey(l.workerKey, l.isOverflow)
		}
	})
	return err
}
