package runner

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/session"
)

func newMockSpawner(t *testing.T) func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
	return func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{
			Roots: opts.SessionRoots,
		})
		return w, nil
	}
}

func TestUtilityDaemon_TurnBudgetRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	daemon := NewUtilityDaemon(nil,
		WithSpawner(newMockSpawner(t)),
		WithTurnBudget(2),
	)
	defer daemon.Close()

	initialConvID := daemon.ActiveConvID()
	if initialConvID == "" {
		t.Fatal("expected initial conversation ID to be non-empty")
	}

	// Turn 1
	out1, err := daemon.Execute(ctx, "turn-1")
	if err != nil {
		t.Fatalf("turn 1 failed: %v", err)
	}
	if !strings.Contains(out1, "turn-1") {
		t.Errorf("unexpected turn 1 output: %s", out1)
	}

	// Turn 2 (reaches budget of 2)
	out2, err := daemon.Execute(ctx, "turn-2")
	if err != nil {
		t.Fatalf("turn 2 failed: %v", err)
	}
	if !strings.Contains(out2, "turn-2") {
		t.Errorf("unexpected turn 2 output: %s", out2)
	}

	// Wait briefly for async swap
	time.Sleep(50 * time.Millisecond)

	// Turn 3 should execute on the rotated worker
	out3, err := daemon.Execute(ctx, "turn-3")
	if err != nil {
		t.Fatalf("turn 3 failed: %v", err)
	}
	if !strings.Contains(out3, "turn-3") {
		t.Errorf("unexpected turn 3 output: %s", out3)
	}

	newConvID := daemon.ActiveConvID()
	if initialConvID == newConvID && daemon.ActiveTurnsUsed() >= 3 {
		t.Errorf("expected worker to rotate after 2 turns, but was %s with %d turns", newConvID, daemon.ActiveTurnsUsed())
	}
}

func TestUtilityDaemon_BrokenPipeAutoRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var firstSpawn atomic.Bool
	spawner := func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		if firstSpawn.CompareAndSwap(false, true) {
			w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{
				CrashOnce: true,
				Roots:     opts.SessionRoots,
			})
			return w, nil
		}
		w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{
			Roots: opts.SessionRoots,
		})
		return w, nil
	}

	daemon := NewUtilityDaemon(nil,
		WithSpawner(spawner),
		WithTurnBudget(10),
	)
	defer daemon.Close()

	// Ensure standby is ready before testing crash handoff
	time.Sleep(50 * time.Millisecond)

	initialConvID := daemon.ActiveConvID()

	// Prompt with CRASH_ONCE to cause active worker to exit 1 only on the first call
	// The daemon must detect the broken pipe, promote standby, and retry
	out, err := daemon.Execute(ctx, "hello after CRASH_ONCE")
	if err != nil {
		t.Fatalf("expected auto-retry to succeed on standby, got error: %v", err)
	}
	if !strings.Contains(out, "hello after") {
		t.Errorf("unexpected response: %s", out)
	}

	// Verify conversation ID rotated
	if daemon.ActiveConvID() == initialConvID {
		t.Errorf("expected active worker to rotate after crash from %s", initialConvID)
	}
}

func TestUtilityDaemon_RunnerFuncPassthrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	daemon := NewUtilityDaemon(nil,
		WithSpawner(newMockSpawner(t)),
	)
	defer daemon.Close()

	fn := daemon.RunnerFunc()

	// 1. One-off prompt (sessionID == "") -> routes to daemon
	stdout, stderr, exitCode, err := fn(ctx, "mock", "one-off prompt", "", "", "", 1)
	if err != nil || exitCode != 0 {
		t.Fatalf("runnerFunc one-off failed (exit %d, stderr %s): %v", exitCode, stderr, err)
	}
	resp, parseErr := ParseAgyOutput(stdout)
	if parseErr != nil {
		t.Fatalf("failed to parse runnerFunc output: %v (raw: %s)", parseErr, stdout)
	}
	if !strings.Contains(resp.Response, "one-off prompt") {
		t.Errorf("unexpected response: %s", resp.Response)
	}
}

func TestUtilityDaemon_TriggerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var spawnCount atomic.Int32
	customSpawner := func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		spawnCount.Add(1)
		w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{Roots: opts.SessionRoots})
		return w, nil
	}

	daemon := NewUtilityDaemon(nil,
		WithSpawner(customSpawner),
	)
	defer daemon.Close()

	// Execute turn 1
	if _, err := daemon.Execute(ctx, "before restart"); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	beforeConvID := daemon.ActiveConvID()
	daemon.TriggerRestartNow("config changed")

	afterConvID := daemon.ActiveConvID()
	if beforeConvID == afterConvID {
		t.Errorf("expected worker to rotate after TriggerRestart, still %s", afterConvID)
	}
}

func TestUtilityDaemon_RSSRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rssSpawner := func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{
			RSSBytes: 100,
			Roots:    opts.SessionRoots,
		})
		return w, nil
	}

	// Set tiny RSS threshold (1 byte) so any running process exceeds it
	daemon := NewUtilityDaemon(nil,
		WithSpawner(rssSpawner),
		WithTurnBudget(100),
		WithMaxRSSBytes(1),
	)
	defer daemon.Close()

	initialConvID := daemon.ActiveConvID()

	// Turn 1 should execute and trigger rotation due to RSS > 1 byte
	_, err := daemon.Execute(ctx, "rss test")
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	newConvID := daemon.ActiveConvID()
	if initialConvID == newConvID {
		t.Errorf("expected worker to rotate when RSS exceeded ceiling, still %s", newConvID)
	}
}

func TestUtilityDaemon_OptionsAndAccessors(t *testing.T) {
	daemon := NewUtilityDaemon(nil,
		WithSpawner(newMockSpawner(t)),
		WithTurnBudget(5),
		WithTurnTimeout(2*time.Second),
		WithSessionRoots("/tmp/test-roots"),
		WithMaxRSSBytes(100*1024*1024),
	)
	defer daemon.Close()

	if daemon.ActiveTurnsUsed() != 0 {
		t.Errorf("expected 0 turns used, got %d", daemon.ActiveTurnsUsed())
	}
	if daemon.ActiveConvID() == "" {
		t.Error("expected non-empty active conv ID")
	}

	// Close is idempotent
	daemon.Close()
	daemon.Close()

	// Execute when closed
	_, err := daemon.Execute(context.Background(), "test closed")
	if err != ErrWorkerDead {
		t.Errorf("expected ErrWorkerDead when closed, got %v", err)
	}
}

func TestUtilityDaemon_WorkerOptsConfigFallback(t *testing.T) {
	// 1. Nil config
	d1 := &UtilityDaemon{roots: []string{"/tmp/r1"}}
	opts1 := d1.workerOpts()
	if opts1.AgyBin != "agy" || opts1.Model != "Gemini 3.8 Flash (Low)" {
		t.Errorf("unexpected opts for nil config: %+v", opts1)
	}

	// 2. Config with ClassifierModel but no LowEffortModel
	c2 := config.NewFromData(&config.ConfigData{
		AgyBin:          "/custom/agy",
		ClassifierModel: "gemini-classifier",
		LowEffortModel:  "",
		APIKey:          "secret-key",
		GeminiHomeDir:   "/home/test",
	})
	d2 := &UtilityDaemon{cfg: c2}
	opts2 := d2.workerOpts()
	if opts2.AgyBin != "/custom/agy" || opts2.Model != "gemini-classifier" || opts2.APIKey != "secret-key" || opts2.HomeDir != "/home/test" {
		t.Errorf("unexpected opts for fallback config: %+v", opts2)
	}

	// 3. Config with LowEffortModel overriding ClassifierModel
	c3 := config.NewFromData(&config.ConfigData{
		LowEffortModel:  "gemini-low-effort",
		ClassifierModel: "gemini-classifier",
	})
	d3 := &UtilityDaemon{cfg: c3}
	opts3 := d3.workerOpts()
	if opts3.Model != "gemini-low-effort" {
		t.Errorf("expected LowEffortModel to take precedence, got %s", opts3.Model)
	}
}

func TestUtilityDaemon_RunnerFuncSessionBypassAndError(t *testing.T) {
	daemon := NewUtilityDaemon(nil,
		WithSpawner(newMockSpawner(t)),
	)
	defer daemon.Close()

	fn := daemon.RunnerFunc()

	// 1. Non-empty sessionID -> routes to RunAgyWithWatchdog (bypasses daemon)
	// We pass a dummy binary that fails fast or exits to test sessionID != "" branch
	_, _, _, err := fn(context.Background(), "/nonexistent/agy", "hello", "session-12345", "", "", 1)
	if err == nil {
		t.Error("expected error for nonexistent agy in watchdog branch")
	}

	// 2. Closed daemon -> Execute returns error in RunnerFunc
	daemon.Close()
	_, stderr, exitCode, err := fn(context.Background(), "mock", "hello", "", "", "", 1)
	if err == nil || exitCode != -1 || !strings.Contains(stderr, "utility daemon error") {
		t.Errorf("expected daemon error response on closed daemon, got exitCode=%d, stderr=%q, err=%v", exitCode, stderr, err)
	}
}

func TestUtilityDaemon_DebouncedRestartAndEnsureStandby(t *testing.T) {
	daemon := NewUtilityDaemon(nil,
		WithSpawner(newMockSpawner(t)),
	)
	defer daemon.Close()

	// Trigger restart with short debounce (50ms)
	daemon.TriggerRestartWithDebounce("test debounce", 50*time.Millisecond)
	// Immediately trigger standard TriggerRestart (1000ms debounce) to test cancel/overwrite
	daemon.TriggerRestart("test standard restart")

	// Trigger again with 10ms to let timer fire
	daemon.TriggerRestartWithDebounce("test fire", 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	// Ensure standby already exists branch (line 139)
	daemon.mu.Lock()
	daemon.ensureStandbyLocked()
	daemon.mu.Unlock()

	// promoteStandbyLocked when standby is nil (synchronous fallback)
	daemon.mu.Lock()
	daemon.standbyWorker = nil
	daemon.promoteStandbyLocked()
	daemon.mu.Unlock()

	if daemon.ActiveConvID() == "" {
		t.Error("expected active worker after synchronous promotion fallback")
	}
}

func TestUtilityDaemon_ExecuteNoWorker(t *testing.T) {
	failSpawner := func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		return nil, os.ErrInvalid
	}

	daemon := NewUtilityDaemon(nil,
		WithSpawner(failSpawner),
	)
	defer daemon.Close()

	_, err := daemon.Execute(context.Background(), "test")
	if err == nil || !strings.Contains(err.Error(), "no utility worker available") {
		t.Errorf("expected 'no utility worker available' error, got %v", err)
	}
}

func TestUtilityDaemon_ExecuteRetryExhaustion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	crashSpawner := func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{
			CrashNow: true,
			Roots:    opts.SessionRoots,
		})
		return w, nil
	}

	daemon := NewUtilityDaemon(nil,
		WithSpawner(crashSpawner),
	)
	defer daemon.Close()

	// CRASH_ALWAYS causes initial attempt to fail and retry attempt to fail
	_, err := daemon.Execute(ctx, "CRASH_ALWAYS")
	if err == nil {
		t.Fatal("expected error on retry exhaustion, got nil")
	}
}

func TestUtilityDaemon_NilActiveWorkerAccessors(t *testing.T) {
	daemon := &UtilityDaemon{}
	if daemon.ActiveConvID() != "" {
		t.Errorf("expected empty convID, got %s", daemon.ActiveConvID())
	}
	if daemon.ActiveTurnsUsed() != 0 {
		t.Errorf("expected 0 turns used, got %d", daemon.ActiveTurnsUsed())
	}
}

func TestUtilityDaemon_DefaultTurnBudgetConstant(t *testing.T) {
	if DefaultTurnBudget != session.DefaultMaxSessionTurns {
		t.Errorf("expected DefaultTurnBudget (%d) to match session.DefaultMaxSessionTurns (%d)", DefaultTurnBudget, session.DefaultMaxSessionTurns)
	}
	if DefaultTurnBudget != 10 {
		t.Errorf("expected DefaultTurnBudget to be 10, got %d", DefaultTurnBudget)
	}

	d := NewUtilityDaemon(nil,
		WithSpawner(func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
			return &WorkerInstance{}, nil
		}),
	)
	defer d.Close()
	if d.turnBudget != 10 {
		t.Errorf("expected default turnBudget to be 10, got %d", d.turnBudget)
	}
}

func TestUtilityDaemon_StandbyPendingAndDeadStandbyReplacement(t *testing.T) {
	daemon := NewUtilityDaemon(nil,
		WithSpawner(newMockSpawner(t)),
	)
	defer daemon.Close()

	// Wait for initial standby to warm
	time.Sleep(50 * time.Millisecond)

	daemon.mu.Lock()
	// Test standbyPending guard
	daemon.standbyPending = true
	daemon.ensureStandbyLocked()
	daemon.standbyPending = false

	// Test promoteStandbyLocked when standby is dead
	if daemon.standbyWorker != nil {
		daemon.standbyWorker.isDead.Store(true)
	}
	daemon.promoteStandbyLocked()
	daemon.mu.Unlock()

	// Wait for standby to prewarm replacing old dead standby
	time.Sleep(50 * time.Millisecond)

	// Test closed daemon guards
	daemon.Close()
	daemon.mu.Lock()
	daemon.promoteStandbyLocked()
	daemon.ensureStandbyLocked()
	daemon.mu.Unlock()
}

func TestUtilityDaemon_ExecuteClosedMidAttempt(t *testing.T) {
	sleepSpawner := func(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
		w, _ := NewMockStreamWorker(t, MockStreamWorkerConfig{
			SleepDelay: 500 * time.Millisecond,
			Roots:      opts.SessionRoots,
		})
		return w, nil
	}

	daemon := NewUtilityDaemon(nil,
		WithSpawner(sleepSpawner),
	)

	// Close daemon mid-attempt
	go func() {
		time.Sleep(10 * time.Millisecond)
		daemon.Close()
	}()

	_, _ = daemon.Execute(context.Background(), "SLEEP")
}
