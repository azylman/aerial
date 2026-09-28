package queue

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWorkerPool_LowEffortModelResolution(t *testing.T) {
	t.Parallel()

	// 1. Nil pool fallback
	var nilPool *WorkerPool
	if got := nilPool.LowEffortModel(); got != config.DefaultConfigData().LowEffortModel {
		t.Errorf("expected default LowEffortModel, got %q", got)
	}

	// 2. Pool with custom LowEffortModel in cfg
	p1 := New(nil, WorkerPoolConfig{
		LowEffortModel: "gemini-flash-low-test",
	})
	if got := p1.LowEffortModel(); got != "gemini-flash-low-test" {
		t.Errorf("expected gemini-flash-low-test, got %q", got)
	}

	// 3. Pool with appCfg override
	appCfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.LowEffortModel = "app-cfg-low-model"
	})
	p2 := New(appCfg, WorkerPoolConfig{})
	if got := p2.LowEffortModel(); got != "app-cfg-low-model" {
		t.Errorf("expected app-cfg-low-model, got %q", got)
	}

	// 4. Pool with appCfg having empty LowEffortModel, falling back to p.cfg.LowEffortModel
	appCfgEmpty := config.NewTestConfig(func(d *config.ConfigData) {
		d.LowEffortModel = ""
	})
	p3 := New(appCfgEmpty, WorkerPoolConfig{
		LowEffortModel: "cfg-fallback-model",
	})
	if got := p3.LowEffortModel(); got != "cfg-fallback-model" {
		t.Errorf("expected cfg-fallback-model, got %q", got)
	}

	// 5. Pool with appCfg empty and p.cfg empty, falling back to default
	p4 := New(appCfgEmpty, WorkerPoolConfig{
		LowEffortModel: "",
	})
	if got := p4.LowEffortModel(); got != config.DefaultConfigData().LowEffortModel {
		t.Errorf("expected default LowEffortModel, got %q", got)
	}

	// 6. Pool with appCfg nil and p.cfg empty, falling back to default
	p5 := New(nil, WorkerPoolConfig{})
	if got := p5.LowEffortModel(); got != config.DefaultConfigData().LowEffortModel {
		t.Errorf("expected default LowEffortModel, got %q", got)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_CustomRunner(t *testing.T) {
	t.Parallel()

	var recordedModel string
	var recordedPrompt string
	var recordedSession string

	pool := New(nil, WorkerPoolConfig{
		Store:          db.NewFakeStore(),
		LowEffortModel: "low-effort-fast-model",
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			recordedModel = model
			recordedPrompt = prompt
			recordedSession = sessionID
			return `{"event":"result","result":{"status":"SUCCESS","response":"turned off fan"}}`, "", 0, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	var statusReceived string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "turn off the fan", "sess-custom-1", func(status string) {
		statusReceived = status
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "turned off fan" {
		t.Errorf("expected reply 'turned off fan', got %q", reply)
	}
	if convID != "sess-custom-1" {
		t.Errorf("expected convID 'sess-custom-1', got %q", convID)
	}
	if recordedModel != "low-effort-fast-model" {
		t.Errorf("expected low-effort-fast-model, got %q", recordedModel)
	}
	if recordedPrompt != "turn off the fan" {
		t.Errorf("expected prompt 'turn off the fan', got %q", recordedPrompt)
	}
	if recordedSession != "sess-custom-1" {
		t.Errorf("expected session 'sess-custom-1', got %q", recordedSession)
	}
	if !strings.Contains(statusReceived, "⚡") {
		t.Errorf("expected status notification, got %q", statusReceived)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_CustomRunner_Branches(t *testing.T) {
	t.Parallel()

	// 1. Runner returns error
	poolErr := New(nil, WorkerPoolConfig{
		Store: db.NewFakeStore(),
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "", "exec error", 1, errors.New("exec failed")
		},
	})
	poolErr.Start()
	defer poolErr.Stop()

	_, _, err := poolErr.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "exec failed") {
		t.Errorf("expected exec failed error, got: %v", err)
	}

	// 2. Runner returns non-zero exit code
	poolExit := New(nil, WorkerPoolConfig{
		Store: db.NewFakeStore(),
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "", "process died", 42, nil
		},
	})
	poolExit.Start()
	defer poolExit.Stop()

	_, _, err = poolExit.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "exit code 42") {
		t.Errorf("expected exit code 42 error, got: %v", err)
	}

	// 3. Runner returns plain text without json result
	poolPlain := New(nil, WorkerPoolConfig{
		Store: db.NewFakeStore(),
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "plain text response", "", 0, nil
		},
	})
	poolPlain.Start()
	defer poolPlain.Stop()

	reply, _, err := poolPlain.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "plain text response" {
		t.Errorf("expected 'plain text response', got %q", reply)
	}

	// 4. Runner returns json with empty response field
	poolEmptyResp := New(nil, WorkerPoolConfig{
		Store: db.NewFakeStore(),
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return `{"event":"result","result":{"status":"SUCCESS","response":""}}`, "", 0, nil
		},
	})
	poolEmptyResp.Start()
	defer poolEmptyResp.Stop()

	reply, _, err = poolEmptyResp.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(reply, "event") {
		t.Errorf("expected raw stdout fallback on empty response, got %q", reply)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_VoiceRunnerHook(t *testing.T) {
	t.Parallel()

	pool := New(nil, WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Custom hook running...")
			}
			return "voice hook reply", sessionID, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	var statusCalled string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "sess-hook-1", func(s string) {
		statusCalled = s
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "voice hook reply" || convID != "sess-hook-1" {
		t.Errorf("unexpected result: reply=%q convID=%q", reply, convID)
	}
	if statusCalled != "⚡ Custom hook running..." {
		t.Errorf("unexpected status: %q", statusCalled)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPool_Success(t *testing.T) {
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			go func() {
				_, _ = io.Copy(io.Discard, inR)
			}()
			go func() {
				defer outW.Close()
				defer errW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\"}\n"))
				time.Sleep(10 * time.Millisecond)
				_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"tool_name\":\"ha_call_service\",\"tool_info\":{\"parameters\":{\"CommandLine\":\"\"}}}}\n"))
				_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool\",\"tool_name\":\"\",\"tool_info\":{\"parameters\":{\"CommandLine\":\"system-health\"}}}}\n"))
				_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"tool_name\":\"ha_call_service\"}}\n"))
				_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"state\":\"ERROR\",\"type\":\"tool_call\",\"tool_name\":\"ha_call_service\"}}\n"))
				_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"daemon voice turn reply\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"usage\":{\"input_tokens\":60,\"output_tokens\":40,\"total_tokens\":100}}}\n"))
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	pool := New(nil, WorkerPoolConfig{
		ProcessPool: procPool,
	})
	pool.Start()
	defer pool.Stop()

	var statuses []string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "turn on living room light", "", func(status string) {
		statuses = append(statuses, status)
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "daemon voice turn reply" {
		t.Errorf("expected 'daemon voice turn reply', got %q", reply)
	}
	if convID == "" || !runner.IsValidUUID(convID) {
		t.Errorf("expected valid session UUID from daemon, got %q", convID)
	}
	if len(statuses) < 2 {
		t.Errorf("expected at least 2 status notifications from tool_call and tool events, got: %v", statuses)
	}

	// Test without status callback (hits onStatus == nil branch in stepHandler)
	reply2, _, err2 := pool.ExecuteVoiceTurn(context.Background(), "turn on living room light", "sess-no-status", nil)
	if err2 != nil || reply2 != "daemon voice turn reply" {
		t.Errorf("unexpected error on nil onStatus: %v", err2)
	}

	// Test invalid lockVal type assertion recovery in scopeLocks
	pool.scopeLocks.Store("voice-sess-bad-lock", "not-a-mutex")
	reply3, _, err3 := pool.ExecuteVoiceTurn(context.Background(), "test", "sess-bad-lock", nil)
	if err3 != nil || reply3 != "daemon voice turn reply" {
		t.Errorf("unexpected error on recovered bad scopeLock: %v", err3)
	}

	// Verify Prometheus metrics for voice runner execution and tokens
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, `source="voice"`) {
		t.Errorf("expected metrics to contain source=voice, got body:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_runner_executions_total{`) || !strings.Contains(body, `source="voice"`) {
		t.Errorf("expected runner executions metric with source=voice, got body:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_runner_duration_seconds_bucket{`) || !strings.Contains(body, `source="voice"`) {
		t.Errorf("expected runner duration bucket metric with source=voice, got body:\n%s", body)
	}
	if !strings.Contains(body, `channel="voice"`) {
		t.Errorf("expected token metrics with channel=voice, got body:\n%s", body)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPool_Errors(t *testing.T) {
	// 1. Nil ProcessPool error
	poolNoDaemon := &WorkerPool{
		ctx: context.Background(),
	}
	_, _, err := poolNoDaemon.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "voice daemon pool unavailable") {
		t.Errorf("expected daemon pool unavailable error, got: %v", err)
	}

	// 2. Daemon acquisition failure (spawner returns error)
	spawnerErr := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("spawner failed to start")
		},
	}
	procPoolErr := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawnerErr)
	defer func() {
		if err := procPoolErr.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()
	poolBadDaemon := New(nil, WorkerPoolConfig{
		ProcessPool: procPoolErr,
	})
	poolBadDaemon.Start()
	defer poolBadDaemon.Stop()

	_, _, err = poolBadDaemon.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "failed to acquire voice daemon") {
		t.Errorf("expected failed to acquire voice daemon error, got: %v", err)
	}

	// 3. Daemon execution error
	spawnerTurnErr := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			go func() {
				_, _ = io.Copy(io.Discard, inR)
			}()
			go func() {
				defer outW.Close()
				defer errW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440001\"}\n"))
				time.Sleep(10 * time.Millisecond)
				_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"simulated failure in daemon\"}}\n"))
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPoolTurnErr := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawnerTurnErr)
	defer func() {
		if err := procPoolTurnErr.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()
	poolTurnErr := New(nil, WorkerPoolConfig{
		ProcessPool: procPoolTurnErr,
	})
	poolTurnErr.Start()
	defer poolTurnErr.Stop()

	_, _, err = poolTurnErr.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "simulated failure in daemon") {
		t.Errorf("expected simulated failure in daemon error, got: %v", err)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ContextCancellations(t *testing.T) {
	t.Parallel()

	// 1. Caller context canceled before execution
	pool := New(nil, WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "reply", sessionID, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := pool.ExecuteVoiceTurn(cancelCtx, "test", "", nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}

	// 2. Pool background context canceled before execution
	poolDeadCtx := &WorkerPool{
		stopped: false,
	}
	deadCtx, deadCancel := context.WithCancel(context.Background())
	deadCancel()
	poolDeadCtx.ctx = deadCtx

	_, _, err = poolDeadCtx.ExecuteVoiceTurn(context.Background(), "test", "", nil)
	if err == nil || !strings.Contains(err.Error(), "brain is shutting down") {
		t.Errorf("expected brain is shutting down error, got: %v", err)
	}

	// 3. Pool nil receiver
	var nilPool *WorkerPool
	_, _, err = nilPool.ExecuteVoiceTurn(context.Background(), "test", "", nil)
	if err == nil || !strings.Contains(err.Error(), "worker pool is uninitialized") {
		t.Errorf("expected uninitialized error, got: %v", err)
	}

	// 4. Pool stopped flag
	poolStopped := &WorkerPool{
		stopped: true,
	}
	_, _, err = poolStopped.ExecuteVoiceTurn(context.Background(), "test", "", nil)
	if err == nil || !strings.Contains(err.Error(), "brain is shutting down") {
		t.Errorf("expected brain is shutting down error, got: %v", err)
	}
}

func TestContainsToken_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		token    string
		expected bool
	}{
		{"empty token", "foo bar baz", "", false},
		{"empty string", "", "foo", false},
		{"matching single token", "foo", "foo", true},
		{"matching middle token", "foo bar baz", "bar", true},
		{"matching first token", "foo bar baz", "foo", true},
		{"matching last token", "foo bar baz", "baz", true},
		{"non-matching token", "foo bar baz", "qux", false},
		{"partial substring non-match", "foobar baz", "foo", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := containsToken(tt.input, tt.token); got != tt.expected {
				t.Errorf("containsToken(%q, %q) = %v; want %v", tt.input, tt.token, got, tt.expected)
			}
		})
	}
}

func TestWorkerPool_ProcessPool_And_MarkDirty(t *testing.T) {
	var nilPool *WorkerPool
	if nilPool.ProcessPool() != nil {
		t.Errorf("expected nil from nil WorkerPool.ProcessPool()")
	}
	nilPool.MarkDirty()

	mockSpawner := runner.NewMockDaemonSpawner()
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	pool := New(nil, WorkerPoolConfig{
		ProcessPool: procPool,
	})
	if pool.ProcessPool() == nil {
		t.Errorf("expected non-nil ProcessPool")
	}
	pool.MarkDirty()
	pool.Stop()
}

func TestWorkerPool_ExecuteVoiceTurn_PoolUnavailable(t *testing.T) {
	pool := New(nil, WorkerPoolConfig{})
	pool.processPool = nil
	_, _, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "voice daemon pool unavailable") {
		t.Errorf("expected voice daemon pool unavailable error, got %v", err)
	}
}

func TestWorkerPool_Stop_ProcessPoolCloseError(t *testing.T) {
	t.Parallel()
	mockHandle := runner.NewMockProcessHandle(99999)
	mockHandle.SetKillErr(errors.New("simulated kill process failure"))

	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			go func() {
				_, _ = io.Copy(io.Discard, inR)
			}()
			go func() {
				defer outW.Close()
				defer errW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440001\"}\n"))
			}()
			return inW, outR, errR, mockHandle, nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, mockSpawner)

	// Pre-spawn a daemon so Close() actually has a daemon to kill
	_, err := procPool.GetOrCreate(context.Background(), "kiosk")
	if err != nil {
		t.Fatalf("failed to spawn daemon: %v", err)
	}

	pool := New(nil, WorkerPoolConfig{
		ProcessPool: procPool,
	})
	pool.Start()
	pool.Stop()
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPool_AdditionalBranches(t *testing.T) {
	t.Parallel()

	// 1. Send failure: daemon stdin pipe broken
	t.Run("send_failure", func(t *testing.T) {
		t.Parallel()
		mockSpawner := &runner.MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				inR, inW := io.Pipe()
				outR, outW := io.Pipe()
				errR, errW := io.Pipe()
				// Close inR immediately so any write to inW returns io.ErrClosedPipe
				_ = inR.Close()
				go func() {
					defer outW.Close()
					defer errW.Close()
					_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440002\"}\n"))
				}()
				return inW, outR, errR, runner.NewMockProcessHandle(1234), nil
			},
		}
		procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
		defer procPool.Close()

		pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
		pool.Start()
		defer pool.Stop()

		_, _, err := pool.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
		if err == nil || !strings.Contains(err.Error(), "failed sending turn to voice daemon") {
			t.Fatalf("expected failed sending turn error, got: %v", err)
		}
	})

	// 2. Caller context cancellation while waiting for turn result
	t.Run("context_canceled_waiting_for_result", func(t *testing.T) {
		t.Parallel()
		doneCh := make(chan struct{})
		defer close(doneCh)

		mockSpawner := &runner.MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				inR, inW := io.Pipe()
				outR, outW := io.Pipe()
				errR, errW := io.Pipe()
				go func() {
					_, _ = io.Copy(io.Discard, inR)
				}()
				go func() {
					_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440003\"}\n"))
					// Keep outW open until test finishes to prevent EOF before cancellation
					<-doneCh
					_ = outW.Close()
					_ = errW.Close()
				}()
				return inW, outR, errR, runner.NewMockProcessHandle(1235), nil
			},
		}
		procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
		defer procPool.Close()

		pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
		pool.Start()
		defer pool.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		_, _, err := pool.ExecuteVoiceTurn(ctx, "test", "sess-cancel", nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})

	// 3. Non-zero exit code and empty session ID fallback
	t.Run("exit_code_nonzero_and_empty_session_fallback", func(t *testing.T) {
		t.Parallel()
		mockSpawner := &runner.MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				inR, inW := io.Pipe()
				outR, outW := io.Pipe()
				errR, errW := io.Pipe()
				go func() {
					_, _ = io.Copy(io.Discard, inR)
				}()
				go func() {
					defer outW.Close()
					defer errW.Close()
					_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440004\"}\n"))
					time.Sleep(10 * time.Millisecond)
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"turn failed\",\"exit_code\":1}}\n"))
				}()
				return inW, outR, errR, runner.NewMockProcessHandle(1236), nil
			},
		}
		procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
		defer procPool.Close()

		// Pre-spawn and clear session ID to trigger activeSession == "" fallback
		d, err := procPool.GetOrCreate(context.Background(), "voice-fallback-conv-id")
		if err != nil {
			t.Fatalf("failed pre-spawning daemon: %v", err)
		}
		d.SetSessionID("")

		pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
		pool.Start()
		defer pool.Stop()

		reply, sessionID, err := pool.ExecuteVoiceTurn(context.Background(), "test", "fallback-conv-id", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if reply != "turn failed" {
			t.Errorf("expected 'turn failed', got %q", reply)
		}
		if sessionID != "fallback-conv-id" {
			t.Errorf("expected fallback session ID 'fallback-conv-id', got %q", sessionID)
		}
	})
}

func TestVoiceTurnSink_Callbacks(t *testing.T) {
	t.Parallel()
	var statusMsg string
	sink := &voiceTurnSink{
		onStatus: func(s string) {
			statusMsg = s
		},
		resCh: make(chan *runner.TurnResult, 1),
		errCh: make(chan error, 1),
	}
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnTextDelta("delta-test")
	sink.OnToolCall("ha_call_service", "light.turn_on")
	if !strings.Contains(statusMsg, "light.turn_on") {
		t.Errorf("expected statusMsg to contain light.turn_on, got %q", statusMsg)
	}
}

