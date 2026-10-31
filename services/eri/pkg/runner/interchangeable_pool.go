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

func (p *InterchangeablePool) ExecuteEphemeral(ctx context.Context, targetKey, prompt string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("interchangeable pool is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	lease, err := p.AcquireLease(ctx, targetKey)
	if err != nil {
		return "", fmt.Errorf("failed to acquire leased session: %w", err)
	}
	res, err := lease.Execute(ctx, &TurnContext{
		TurnID: uuid.New().String(), SessionID: lease.SessionID(),
		Prompt: prompt, CreatedAt: time.Now(), Ctx: ctx,
	})
	if err = errors.Join(err, lease.Release()); err != nil || res == nil {
		return "", err
	}
	return res.Response, nil
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
