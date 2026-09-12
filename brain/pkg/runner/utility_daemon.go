package runner

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/session"
)

// DefaultTurnBudget defines the default number of turns executed before rotating a persistent utility worker.
const DefaultTurnBudget = session.DefaultMaxSessionTurns

type DaemonOption func(*UtilityDaemon)

func WithSpawner(fn func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error)) DaemonOption {
	return func(d *UtilityDaemon) {
		d.spawner = fn
	}
}

func WithTurnBudget(n int) DaemonOption {
	return func(d *UtilityDaemon) {
		if n > 0 {
			d.turnBudget = n
		}
	}
}

func WithTurnTimeout(timeout time.Duration) DaemonOption {
	return func(d *UtilityDaemon) {
		if timeout > 0 {
			d.turnTimeout = timeout
		}
	}
}

func WithSessionRoots(roots ...string) DaemonOption {
	return func(d *UtilityDaemon) {
		d.roots = roots
	}
}

func WithMaxRSSBytes(limit uint64) DaemonOption {
	return func(d *UtilityDaemon) {
		d.maxRSSBytes = limit
	}
}

type UtilityDaemon struct {
	mu            sync.Mutex
	cfg           *config.Config
	activeWorker  *WorkerInstance
	standbyWorker *WorkerInstance
	spawner       func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error)
	turnBudget    int
	turnTimeout   time.Duration
	maxRSSBytes   uint64
	roots         []string
	closed        atomic.Bool

	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	standbyPending bool

	reloadMu    sync.Mutex
	reloadTimer *time.Timer
}

// NewUtilityDaemon constructs and pre-warms a persistent utility worker daemon.
func NewUtilityDaemon(cfg *config.Config, opts ...DaemonOption) *UtilityDaemon {
	daemonCtx, daemonCancel := context.WithCancel(context.Background())
	d := &UtilityDaemon{
		cfg:         cfg,
		turnBudget:  DefaultTurnBudget,
		turnTimeout: 15 * time.Second,
		maxRSSBytes: 500 * 1024 * 1024,
		spawner:     NewWorkerInstance,
		ctx:         daemonCtx,
		cancel:      daemonCancel,
	}

	for _, opt := range opts {
		opt(d)
	}

	ctx, cancel := context.WithTimeout(d.ctx, 20*time.Second)
	defer cancel()

	// Initialize active worker
	workerOpts := d.workerOpts()
	w, err := d.spawner(ctx, workerOpts)
	if err != nil {
		log.Printf("[UtilityDaemon] Warning: initial worker spawn failed: %v (will retry on demand)", err)
	} else {
		d.activeWorker = w
	}

	// Pre-warm standby worker in background
	d.ensureStandbyLocked()

	return d
}

func (d *UtilityDaemon) workerOpts() WorkerOptions {
	agyBin := "agy"
	model := "Gemini 3.8 Flash (Low)"
	apiKey := ""
	homeDir := ""

	if d.cfg != nil {
		cur := d.cfg.Current()
		if cur != nil {
			if strings.TrimSpace(cur.AgyBin) != "" {
				agyBin = strings.TrimSpace(cur.AgyBin)
			}
			if strings.TrimSpace(cur.LowEffortModel) != "" {
				model = strings.TrimSpace(cur.LowEffortModel)
			} else if strings.TrimSpace(cur.ClassifierModel) != "" {
				model = strings.TrimSpace(cur.ClassifierModel)
			}
			if strings.TrimSpace(cur.APIKey) != "" {
				apiKey = strings.TrimSpace(cur.APIKey)
			}
			if strings.TrimSpace(cur.GeminiHomeDir) != "" {
				homeDir = strings.TrimSpace(cur.GeminiHomeDir)
			}
		}
	}

	return WorkerOptions{
		AgyBin:       agyBin,
		Model:        model,
		APIKey:       apiKey,
		HomeDir:      homeDir,
		SessionRoots: d.roots,
		Timeout:      d.turnTimeout,
	}
}

func (d *UtilityDaemon) ensureStandbyLocked() {
	if d.closed.Load() {
		return
	}
	if d.standbyWorker != nil && !d.standbyWorker.IsDead() {
		return
	}
	if d.standbyPending {
		return
	}
	d.standbyPending = true

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() {
			d.mu.Lock()
			d.standbyPending = false
			d.mu.Unlock()
		}()

		ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
		defer cancel()

		opts := d.workerOpts()
		standby, err := d.spawner(ctx, opts)
		if err != nil {
			if !d.closed.Load() {
				log.Printf("[UtilityDaemon] Standby pre-warming failed: %v", err)
			}
			return
		}

		d.mu.Lock()
		defer d.mu.Unlock()
		if d.closed.Load() {
			standby.Close()
			return
		}
		if d.standbyWorker != nil && !d.standbyWorker.IsDead() {
			standby.Close()
			return
		}
		oldStandby := d.standbyWorker
		d.standbyWorker = standby
		if oldStandby != nil {
			d.wg.Add(1)
			go func(w *WorkerInstance) {
				defer d.wg.Done()
				w.Close()
			}(oldStandby)
		}
	}()
}

func (d *UtilityDaemon) promoteStandbyLocked() {
	if d.closed.Load() {
		return
	}

	oldWorker := d.activeWorker

	if d.standbyWorker != nil && !d.standbyWorker.IsDead() {
		d.activeWorker = d.standbyWorker
		d.standbyWorker = nil
	} else {
		if d.standbyWorker != nil {
			deadStandby := d.standbyWorker
			d.standbyWorker = nil
			d.wg.Add(1)
			go func(w *WorkerInstance) {
				defer d.wg.Done()
				w.Close()
			}(deadStandby)
		}

		// Standby not ready; spawn synchronously bound to daemon lifecycle context
		ctx, cancel := context.WithTimeout(d.ctx, 20*time.Second)
		defer cancel()
		fresh, err := d.spawner(ctx, d.workerOpts())
		if err != nil {
			log.Printf("[UtilityDaemon] Failed to spawn synchronous replacement worker: %v", err)
		} else {
			if d.closed.Load() {
				fresh.Close()
			} else {
				d.activeWorker = fresh
			}
		}
	}

	if oldWorker != nil {
		d.wg.Add(1)
		go func(w *WorkerInstance) {
			defer d.wg.Done()
			w.Close()
		}(oldWorker)
	}

	d.ensureStandbyLocked()
}

// ActiveConvID returns the conversation ID of the active worker.
func (d *UtilityDaemon) ActiveConvID() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.activeWorker != nil {
		return d.activeWorker.ConversationID()
	}
	return ""
}

// ActiveTurnsUsed returns the number of turns executed by the active worker.
func (d *UtilityDaemon) ActiveTurnsUsed() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.activeWorker != nil {
		return d.activeWorker.TurnsUsed()
	}
	return 0
}

// Execute dispatches a prompt turn to the active worker with automatic broken-pipe retry and turn budget rotation.
func (d *UtilityDaemon) Execute(ctx context.Context, prompt string) (string, error) {
	if d.closed.Load() {
		return "", ErrWorkerDead
	}

	for attempt := 0; attempt < 2; attempt++ {
		if d.closed.Load() {
			return "", ErrWorkerDead
		}

		d.mu.Lock()
		if d.closed.Load() {
			d.mu.Unlock()
			return "", ErrWorkerDead
		}
		if d.activeWorker == nil || d.activeWorker.IsDead() {
			d.promoteStandbyLocked()
		}
		worker := d.activeWorker
		d.mu.Unlock()

		if worker == nil {
			return "", fmt.Errorf("no utility worker available")
		}

		turnTimeout := d.turnTimeout
		if turnTimeout <= 0 {
			turnTimeout = 15 * time.Second
		}
		callCtx, cancel := context.WithTimeout(ctx, turnTimeout)
		resp, err := worker.Execute(callCtx, prompt)
		cancel()

		if err != nil {
			if d.closed.Load() {
				return "", ErrWorkerDead
			}

			// If worker crashed or pipe broke, promote standby and retry once
			d.mu.Lock()
			if d.closed.Load() {
				d.mu.Unlock()
				return "", ErrWorkerDead
			}
			if d.activeWorker == worker {
				d.promoteStandbyLocked()
			}
			d.mu.Unlock()

			if attempt == 0 {
				log.Printf("[UtilityDaemon] Worker failed (%v); retrying turn on fresh standby", err)
				continue
			}
			return "", err
		}

		// Turn succeeded; check RSS ceiling and turn budget for pre-warmed rotation
		d.mu.Lock()
		if !d.closed.Load() && d.activeWorker == worker {
			if d.maxRSSBytes > 0 && worker.RSSBytes() > d.maxRSSBytes {
				log.Printf("[UtilityDaemon] Worker RSS (%d bytes) exceeded ceiling (%d bytes); rotating worker", worker.RSSBytes(), d.maxRSSBytes)
				d.promoteStandbyLocked()
			} else if worker.TurnsUsed() >= d.turnBudget {
				d.promoteStandbyLocked()
			}
		}
		d.mu.Unlock()

		return resp.Response, nil
	}

	return "", fmt.Errorf("utility execution failed after retry")
}

// RunnerFunc returns a runner.RunnerFunc that routes utility calls (sessionID == "") to the daemon
// and conversational calls (sessionID != "") to the standard RunAgyWithWatchdog.
func (d *UtilityDaemon) RunnerFunc() RunnerFunc {
	return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
		if strings.TrimSpace(sessionID) != "" {
			// Interactive session turn: bypass utility daemon
			return RunAgyWithWatchdog(ctx, agyBin, prompt, sessionID, apiKey, model, DefaultWatchdogOptions(timeoutMinutes))
		}

		// Stateless one-off utility turn: route through persistent daemon
		out, err := d.Execute(ctx, prompt)
		if err != nil {
			return "", fmt.Sprintf("utility daemon error: %v", err), -1, err
		}

		// Format synthetic result NDJSON event so ParseAgyOutput parses response seamlessly
		syntheticResult := fmt.Sprintf(`{"event":"result","result":{"status":"SUCCESS","response":%q,"conversation_id":%q}}`, out, d.ActiveConvID())
		return syntheticResult, "", 0, nil
	}
}

// TriggerRestart triggers a debounced graceful rotation of active and standby workers.
func (d *UtilityDaemon) TriggerRestart(reason string) {
	d.TriggerRestartWithDebounce(reason, 1000*time.Millisecond)
}

// TriggerRestartNow immediately forces rotation without debounce delay.
func (d *UtilityDaemon) TriggerRestartNow(reason string) {
	d.TriggerRestartWithDebounce(reason, 0)
}

// TriggerRestartWithDebounce triggers rotation with a custom debounce duration.
func (d *UtilityDaemon) TriggerRestartWithDebounce(reason string, debounce time.Duration) {
	d.reloadMu.Lock()
	defer d.reloadMu.Unlock()

	if d.reloadTimer != nil {
		d.reloadTimer.Stop()
	}

	if debounce <= 0 {
		log.Printf("[UtilityDaemon] Performing immediate reload (reason: %s)", reason)
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.closed.Load() {
			d.promoteStandbyLocked()
		}
		return
	}

	d.reloadTimer = time.AfterFunc(debounce, func() {
		log.Printf("[UtilityDaemon] Performing graceful reload (reason: %s)", reason)
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.closed.Load() {
			d.promoteStandbyLocked()
		}
	})
}

// Close gracefully terminates all running workers.
func (d *UtilityDaemon) Close() {
	if d.closed.Swap(true) {
		return
	}

	if d.cancel != nil {
		d.cancel()
	}

	d.reloadMu.Lock()
	if d.reloadTimer != nil {
		d.reloadTimer.Stop()
	}
	d.reloadMu.Unlock()

	d.mu.Lock()
	active := d.activeWorker
	d.activeWorker = nil
	standby := d.standbyWorker
	d.standbyWorker = nil
	d.mu.Unlock()

	if active != nil {
		active.Close()
	}
	if standby != nil {
		standby.Close()
	}

	d.wg.Wait()
}
