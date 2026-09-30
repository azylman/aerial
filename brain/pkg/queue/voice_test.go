package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
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

	_, _, err = poolEmptyResp.ExecuteVoiceTurn(context.Background(), "test", "sess-1", nil)
	if err == nil || !strings.Contains(err.Error(), "empty response") {
		t.Errorf("expected empty response error on empty result, got %v", err)
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

func TestWorkerPool_ExecuteVoiceTurn_VoiceStreamRunnerHook(t *testing.T) {
	t.Parallel()

	pool := New(nil, WorkerPoolConfig{
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Streaming hook running...")
			}
			if onSentence != nil {
				onSentence("Streaming sentence.")
			}
			return "voice stream reply", sessionID, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	var statusCalled string
	var sentenceCalled string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "sess-stream-hook-1", func(s string) {
		statusCalled = s
	}, func(sent string) {
		sentenceCalled = sent
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "voice stream reply" || convID != "sess-stream-hook-1" {
		t.Errorf("unexpected result: reply=%q convID=%q", reply, convID)
	}
	if statusCalled != "⚡ Streaming hook running..." {
		t.Errorf("unexpected status: %q", statusCalled)
	}
	if sentenceCalled != "Streaming sentence." {
		t.Errorf("unexpected sentence: %q", sentenceCalled)
	}

	// Error branch test
	poolErr := New(nil, WorkerPoolConfig{
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
			return "", sessionID, errors.New("simulated stream failure")
		},
	})
	poolErr.Start()
	defer poolErr.Stop()

	_, _, err = poolErr.ExecuteVoiceTurn(context.Background(), "hello", "sess-err", nil, func(sent string) {})
	if err == nil {
		t.Fatal("expected error, got nil")
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
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "turn on living room light", "kiosk", func(status string) {
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

	_, _, err := pool.ExecuteVoiceTurn(cancelCtx, "test", "sess-cancel", nil)
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

	_, _, err = poolDeadCtx.ExecuteVoiceTurn(context.Background(), "test", "sess-dead", nil)
	if err == nil || !strings.Contains(err.Error(), "brain is shutting down") {
		t.Errorf("expected brain is shutting down error, got: %v", err)
	}

	// 3. Pool nil receiver
	var nilPool *WorkerPool
	_, _, err = nilPool.ExecuteVoiceTurn(context.Background(), "test", "sess-nil", nil)
	if err == nil || !strings.Contains(err.Error(), "worker pool is uninitialized") {
		t.Errorf("expected uninitialized error, got: %v", err)
	}

	// 4. Pool stopped flag
	poolStopped := &WorkerPool{
		stopped: true,
	}
	_, _, err = poolStopped.ExecuteVoiceTurn(context.Background(), "test", "sess-stopped", nil)
	if err == nil || !strings.Contains(err.Error(), "brain is shutting down") {
		t.Errorf("expected brain is shutting down error, got: %v", err)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_RequiresSessionID(t *testing.T) {
	t.Parallel()
	pool := New(nil, WorkerPoolConfig{})
	_, _, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "", nil)
	if err == nil || !strings.Contains(err.Error(), "session_id is required") {
		t.Fatalf("expected session_id required error for empty sessionID, got %v", err)
	}
	_, _, err = pool.ExecuteVoiceTurn(context.Background(), "hello", "   ", nil)
	if err == nil || !strings.Contains(err.Error(), "session_id is required") {
		t.Fatalf("expected session_id required error for whitespace sessionID, got %v", err)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_VoiceProcessPool_DeviceRouting(t *testing.T) {
	t.Parallel()
	var spawnedTarget string
	mockSpawner := runner.NewMockDaemonSpawner()
	mockSpawner.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
		spawnedTarget = cfg.ThreadID
		inR, inW := io.Pipe()
		outR, outW := io.Pipe()
		errR, errW := io.Pipe()
		go func() {
			defer inR.Close()
			_, _ = io.Copy(io.Discard, inR)
		}()
		go func() {
			defer outW.Close()
			defer errW.Close()
			_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\"}\n"))
			time.Sleep(5 * time.Millisecond)
			_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"kiosk device reply\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\"}}\n"))
		}()
		return inW, outR, errR, runner.NewMockProcessHandle(8888), nil
	}

	voicePool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, mockSpawner)
	defer voicePool.Close()

	pool := New(nil, WorkerPoolConfig{
		VoiceProcessPool: voicePool,
	})
	pool.Start()
	defer pool.Stop()

	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "what is the weather", "kiosk", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "kiosk device reply" {
		t.Errorf("expected reply 'kiosk device reply', got %q", reply)
	}
	if convID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected convID '550e8400-e29b-41d4-a716-446655440000', got %q", convID)
	}
	if spawnedTarget != "kiosk" {
		t.Errorf("expected daemon spawned with target 'kiosk', got %q", spawnedTarget)
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
	_, err := procPool.GetOrCreate(context.Background(), "kiosk", "")
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
		d, err := procPool.GetOrCreate(context.Background(), "fallback-conv-id", "")
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
	sink := newVoiceTurnSink(context.Background(), func(s string) {
		statusMsg = s
	}, nil, nil)
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnTextDelta("delta-test")
	sink.OnToolCall("ha_call_service", "light.turn_on")
	if !strings.Contains(statusMsg, "light.turn_on") {
		t.Errorf("expected statusMsg to contain light.turn_on, got %q", statusMsg)
	}
}

func TestVoiceTurnSink_DeltaAccumulationAndErrorRecovery(t *testing.T) {
	t.Parallel()

	// Case 1: Streamed deltas with empty terminal response falls back to accumulated text
	var sentences []string
	sink := newVoiceTurnSink(context.Background(), nil, func(s string) {
		sentences = append(sentences, s)
	}, NewSentenceDetector())
	sink.OnTextDelta("Hello ")
	sink.OnTextDelta("world.")
	sink.OnResult(&runner.TurnResult{Response: ""})

	res, err := sink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Response != "Hello world." {
		t.Errorf("expected accumulated 'Hello world.', got %q", res.Response)
	}

	// Case 2: Streamed deltas followed by OnError recovers substantive deltas on resCh
	var sentences2 []string
	sink2 := newVoiceTurnSink(context.Background(), nil, func(s string) {
		sentences2 = append(sentences2, s)
	}, NewSentenceDetector())
	sink2.OnTextDelta("The quick brown fox jumps over the lazy dog.")
	sink2.OnError(errors.New("API error (attempt 2): RESOURCE_EXHAUSTED (code 429)"))

	res2, err2 := sink2.Wait(context.Background())
	if err2 != nil {
		t.Fatalf("expected resCh recovery, got errCh: %v", err2)
	}
	if res2.Response != "The quick brown fox jumps over the lazy dog." {
		t.Errorf("expected recovered deltas on resCh, got %q", res2.Response)
	}

	// Case 3: Empty deltas with OnError properly forwards to errCh
	sink3 := newVoiceTurnSink(context.Background(), nil, nil, nil)
	expectedErr := errors.New("hard failure without deltas")
	sink3.OnError(expectedErr)

	res3, err3 := sink3.Wait(context.Background())
	if res3 != nil {
		t.Fatalf("unexpected result on resCh: %+v", res3)
	}
	if !errors.Is(err3, expectedErr) {
		t.Errorf("expected %v on errCh, got %v", expectedErr, err3)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_TranscriptRecoveryOnError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sessMgr := session.New(tempDir, tempDir)
	convID := "550e8400-e29b-41d4-a716-446655449999"

	logsDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("mkdir logs failed: %v", err)
	}

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
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"" + convID + "\"}\n"))
				time.Sleep(10 * time.Millisecond)
				now := time.Now().UTC().Format(time.RFC3339Nano)
				transcript := fmt.Sprintf(`{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"What is the status?","created_at":%q}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"All systems operational and nominal.","created_at":%q}
`, now, now)
				_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644)
				// Daemon returns ERROR with empty response (e.g. CLI aborted or dropped result line)
				_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"RESOURCE_EXHAUSTED (code 429)\",\"response\":\"\"}}\n"))
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel:      "gemini-2.5-flash",
		TranscriptRescuer: sessMgr.ExtractResponseSince,
	}, mockSpawner)
	defer procPool.Close()

	pool := New(nil, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: sessMgr,
	})
	pool.Start()
	defer pool.Stop()

	var sentences []string
	reply, sessionID, err := pool.ExecuteVoiceTurn(context.Background(), "What is the status?", convID, nil, func(s string) {
		sentences = append(sentences, s)
	})

	if err != nil {
		t.Fatalf("expected transcript recovery without error, got: %v", err)
	}
	if reply != "All systems operational and nominal." {
		t.Errorf("expected recovered transcript reply, got %q", reply)
	}
	if sessionID != convID {
		t.Errorf("expected sessionID %q, got %q", convID, sessionID)
	}
	if len(sentences) == 0 {
		t.Errorf("expected sentences emitted to voice callback on transcript recovery, got 0")
	}
}

type queueMockVoiceSession struct {
	sessionID string
	sendFn    func(prompt string, turn *runner.TurnContext) error
}

func (s *queueMockVoiceSession) SessionID() string {
	return s.sessionID
}

func (s *queueMockVoiceSession) Send(prompt string, turn *runner.TurnContext) error {
	if s.sendFn != nil {
		return s.sendFn(prompt, turn)
	}
	if turn != nil && turn.Sink != nil {
		turn.Sink.OnTurnStarted()
		turn.Sink.OnTextDelta("Hello from mock voice.")
		turn.Sink.OnResult(&runner.TurnResult{
			Response: "Hello from mock voice.",
		})
	}
	return nil
}

type queueMockVoiceProcessPool struct {
	sessions    map[string]*queueMockVoiceSession
	initCalled  bool
	closeCalled bool
}

func (p *queueMockVoiceProcessPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (runner.AgentSession, error) {
	if p.closeCalled {
		return nil, errors.New("pool closed")
	}
	if s, ok := p.sessions[targetKey]; ok {
		return s, nil
	}
	sess := &queueMockVoiceSession{sessionID: "sess-" + targetKey}
	p.sessions[targetKey] = sess
	return sess, nil
}

func (p *queueMockVoiceProcessPool) Initialize(ctx context.Context) error {
	p.initCalled = true
	return nil
}

func (p *queueMockVoiceProcessPool) Close() error {
	p.closeCalled = true
	return nil
}

var _ runner.AgentPool = (*queueMockVoiceProcessPool)(nil)
var _ runner.AgentSession = (*queueMockVoiceSession)(nil)

func TestWorkerPool_ExecuteVoiceTurn_MockVoiceProcessPool(t *testing.T) {
	t.Parallel()

	mockPool := &queueMockVoiceProcessPool{
		sessions: make(map[string]*queueMockVoiceSession),
	}

	pool := New(nil, WorkerPoolConfig{
		VoiceProcessPool: mockPool,
	})
	pool.Start()
	defer pool.Stop()

	if pool.VoiceProcessPool() != mockPool {
		t.Fatalf("expected VoiceProcessPool to return mockPool")
	}

	var sentenceReceived string
	reply, sessID, err := pool.ExecuteVoiceTurn(
		context.Background(),
		"ping",
		"mock-dev-1",
		nil,
		func(sentence string) {
			sentenceReceived = sentence
		},
	)
	if err != nil {
		t.Fatalf("ExecuteVoiceTurn failed with mock VoiceProcessPool: %v", err)
	}
	if reply != "Hello from mock voice." {
		t.Errorf("expected 'Hello from mock voice.', got %q", reply)
	}
	if sessID != "sess-mock-dev-1" {
		t.Errorf("expected session ID 'sess-mock-dev-1', got %q", sessID)
	}
	if sentenceReceived != "Hello from mock voice." {
		t.Errorf("expected sentence callback 'Hello from mock voice.', got %q", sentenceReceived)
	}

	// Verify MarkDirty handles mock pool without panic
	pool.MarkDirty()

	// Verify Stop closes the voice process pool
	pool.Stop()
	if !mockPool.closeCalled {
		t.Errorf("expected mockPool.Close() to be called on Stop")
	}
}
