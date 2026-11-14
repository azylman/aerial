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

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// InterchangeablePoolConfig specifies sizing and naming configuration for an InterchangeablePool.
type InterchangeablePoolConfig struct {
	WorkerCount int    // defaults to 2 if <= 0
	KeyPrefix   string // defaults to "ephemeral:worker" if empty
}

// attrEphemeralTarget names the target key an ephemeral run (caption, summary) was issued for.
const attrEphemeralTarget = attribute.Key("aerial.ephemeral.target")

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

var _ AgentPool = (*InterchangeablePool)(nil)

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
	for i := range workerCount {
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

// ExecuteEphemeral runs prompt on a leased worker and returns the response. When ctx carries a
// span, the run is traced as an invoke_agent child span (wrapping lease acquisition, daemon
// startup and the chat/execute_tool spans) that records the configured model, provider and token
// usage; without one it records nothing, so untraced callers never open stray root traces.
func (p *InterchangeablePool) ExecuteEphemeral(ctx context.Context, targetKey, prompt string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("interchangeable pool is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	spanCtx, span := startChildOf(ctx, OperationInvokeAgent, trace.WithAttributes(
		AttrGenAIOperationName.String(OperationInvokeAgent), attrEphemeralTarget.String(targetKey)))
	res, err := p.executeEphemeral(spanCtx, targetKey, prompt)
	endEphemeralSpan(span, p.Model(), res, err)
	if err != nil || res == nil {
		return "", err
	}
	return res.Response, nil
}

func (p *InterchangeablePool) executeEphemeral(ctx context.Context, targetKey, prompt string) (*TurnResult, error) {
	lease, err := p.AcquireLease(ctx, targetKey)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire leased session: %w", err)
	}
	res, err := lease.Execute(ctx, &TurnContext{
		TurnID: uuid.New().String(), SessionID: lease.SessionID(),
		Prompt: prompt, CreatedAt: time.Now(), Ctx: ctx, Sink: NewTracingTurnSink(ctx, nil),
	})
	return res, errors.Join(err, lease.Release())
}

// endEphemeralSpan records an ephemeral run's model, provider, usage and outcome, then ends span.
func endEphemeralSpan(span trace.Span, model string, res *TurnResult, err error) {
	var usage AgyUsage
	if res != nil {
		usage = res.Usage
	}
	attrs := UsageAttributes(usage)
	if model != "" {
		attrs = append(attrs, AttrGenAIRequestModel.String(model))
	}
	if provider := ProviderForModel(model); provider != "" {
		attrs = append(attrs, AttrGenAIProviderName.String(provider))
	}
	span.SetAttributes(attrs...)
	if err != nil {
		span.SetStatus(codes.Error, tracing.Redact(err.Error()))
	}
	span.End()
}

func (p *InterchangeablePool) EphemeralLLMFunc(targetKey string) LLMFunc {
	return func(ctx context.Context, model, prompt string) (string, error) {
		if p == nil {
			return "", fmt.Errorf("interchangeable pool is nil")
		}
		return p.ExecuteEphemeral(ctx, targetKey, prompt)
	}
}

// Model returns the model the worker daemons run, or "" when the pool is uninitialized.
func (p *InterchangeablePool) Model() string {
	if p == nil {
		return ""
	}
	return p.underlying.Model()
}

func (p *InterchangeablePool) Underlying() *UnifiedProcessPool {
	if p == nil {
		return nil
	}
	return p.underlying
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

func (p *InterchangeablePool) UpdateModel(model string) {
	if p != nil && p.underlying != nil {
		p.underlying.UpdateModel(model)
	}
}

func (p *InterchangeablePool) UpdateEngine(engine, claudeBin string) {
	if p != nil && p.underlying != nil {
		p.underlying.UpdateEngine(engine, claudeBin)
	}
}

func (p *InterchangeablePool) MarkDirty() {
	if p != nil && p.underlying != nil {
		p.underlying.MarkDirty()
	}
}

func (p *InterchangeablePool) Get(targetKey string) (*StreamingDaemon, bool) {
	if p == nil || p.underlying == nil {
		return nil, false
	}
	return p.underlying.Get(targetKey)
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

var (
	_ SessionLease    = (*interchangeableSessionLease)(nil)
	_ ActiveTurnOwner = (*interchangeableSessionLease)(nil)
)

// ActiveTurn forwards the worker daemon's identity; zero when the inner lease has none.
func (l *interchangeableSessionLease) ActiveTurn() ActiveTurn {
	if owner, ok := l.inner.(ActiveTurnOwner); ok {
		return owner.ActiveTurn()
	}
	return ActiveTurn{}
}

func (l *interchangeableSessionLease) SessionID() string {
	if l == nil || l.inner == nil {
		return ""
	}
	return l.inner.SessionID()
}

// IsOverflow reports whether the lease runs on an on-demand worker because every fixed worker was busy.
func (l *interchangeableSessionLease) IsOverflow() bool { return l != nil && l.isOverflow }

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
