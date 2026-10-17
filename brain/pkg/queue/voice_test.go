package queue

import (
	"bufio"
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

	appCfgVal := "app-cfg-low-model"
	appCfgEmpty := ""
	defaultModel := config.DefaultConfigData().LowEffortModel

	tests := []struct {
		name       string
		poolNil    bool
		appCfgVal  *string
		cfgModel   string
		wantResult string
	}{
		{
			name:       "NilPoolFallback",
			poolNil:    true,
			wantResult: defaultModel,
		},
		{
			name:       "CustomModelInCfg",
			cfgModel:   "gemini-flash-low-test",
			wantResult: "gemini-flash-low-test",
		},
		{
			name:       "AppCfgOverride",
			appCfgVal:  &appCfgVal,
			wantResult: "app-cfg-low-model",
		},
		{
			name:       "AppCfgEmptyWithCfgFallback",
			appCfgVal:  &appCfgEmpty,
			cfgModel:   "cfg-fallback-model",
			wantResult: "cfg-fallback-model",
		},
		{
			name:       "AppCfgEmptyAndCfgEmptyDefaults",
			appCfgVal:  &appCfgEmpty,
			cfgModel:   "",
			wantResult: defaultModel,
		},
		{
			name:       "AppCfgNilAndCfgEmptyDefaults",
			cfgModel:   "",
			wantResult: defaultModel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.poolNil {
				var nilPool *WorkerPool
				if got := nilPool.LowEffortModel(); got != tt.wantResult {
					t.Errorf("LowEffortModel() = %q, want %q", got, tt.wantResult)
				}
				return
			}

			var appCfg *config.Config
			if tt.appCfgVal != nil {
				val := *tt.appCfgVal
				appCfg = config.NewTestConfig(func(d *config.ConfigData) {
					d.LowEffortModel = val
				})
			}

			p := New(appCfg, WorkerPoolConfig{LowEffortModel: tt.cfgModel})
			if got := p.LowEffortModel(); got != tt.wantResult {
				t.Errorf("LowEffortModel() = %q, want %q", got, tt.wantResult)
			}
		})
	}
}

func TestWorkerPool_MediumEffortModelResolution(t *testing.T) {
	t.Parallel()

	appCfgVal := "app-cfg-med-model"
	appCfgEmpty := ""
	defaultModel := config.DefaultConfigData().MediumEffortModel

	tests := []struct {
		name       string
		poolNil    bool
		appCfgVal  *string
		cfgModel   string
		wantResult string
	}{
		{name: "NilPoolFallback", poolNil: true, wantResult: defaultModel},
		{name: "CustomModelInCfg", cfgModel: "gemini-flash-med-test", wantResult: "gemini-flash-med-test"},
		{name: "AppCfgOverride", appCfgVal: &appCfgVal, wantResult: "app-cfg-med-model"},
		{name: "AppCfgEmptyWithCfgFallback", appCfgVal: &appCfgEmpty, cfgModel: "cfg-med-fallback", wantResult: "cfg-med-fallback"},
		{name: "AppCfgEmptyAndCfgEmptyDefaults", appCfgVal: &appCfgEmpty, cfgModel: "", wantResult: defaultModel},
		{name: "AppCfgNilAndCfgEmptyDefaults", cfgModel: "", wantResult: defaultModel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.poolNil {
				var nilPool *WorkerPool
				if got := nilPool.MediumEffortModel(); got != tt.wantResult {
					t.Errorf("MediumEffortModel() = %q, want %q", got, tt.wantResult)
				}
				return
			}

			var appCfg *config.Config
			if tt.appCfgVal != nil {
				val := *tt.appCfgVal
				appCfg = config.NewTestConfig(func(d *config.ConfigData) {
					d.MediumEffortModel = val
				})
			}

			p := New(appCfg, WorkerPoolConfig{MediumEffortModel: tt.cfgModel})
			if got := p.MediumEffortModel(); got != tt.wantResult {
				t.Errorf("MediumEffortModel() = %q, want %q", got, tt.wantResult)
			}
		})
	}
}

func TestWorkerPool_ExecuteVoiceTurn_CustomRunnerOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, runnerOut, runnerErrOut string
		runnerCode                    int
		runnerErr                     error
		prompt, sessionID             string
		wantReply, wantErrSub         string
		wantConvID, wantStatusSub     string
	}{
		{
			name:          "SuccessTurn",
			runnerOut:     `{"event":"result","result":{"status":"SUCCESS","response":"turned off fan"}}`,
			prompt:        "turn off the fan",
			sessionID:     "sess-custom-1",
			wantReply:     "turned off fan",
			wantConvID:    "sess-custom-1",
			wantStatusSub: "⚡",
		},
		{
			name:         "RunnerError",
			runnerErrOut: "exec error",
			runnerCode:   1,
			runnerErr:    errors.New("exec failed"),
			prompt:       "turn off the fan",
			sessionID:    "sess-custom-1",
			wantErrSub:   "exec failed",
		},
		{
			name:         "ExitCodeNonZero",
			runnerErrOut: "process died",
			runnerCode:   42,
			prompt:       "turn off the fan",
			sessionID:    "sess-custom-1",
			wantErrSub:   "exit code 42",
		},
		{
			name:       "PlainTextFallback",
			runnerOut:  "plain text response",
			prompt:     "turn off the fan",
			sessionID:  "sess-custom-1",
			wantReply:  "plain text response",
			wantConvID: "sess-custom-1",
		},
		{
			name:       "EmptyResponseField",
			runnerOut:  `{"event":"result","result":{"status":"SUCCESS","response":""}}`,
			prompt:     "turn off the fan",
			sessionID:  "sess-custom-1",
			wantErrSub: "empty response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var recordedModel string
			var recordedPrompt string
			var recordedSession string
			var statusReceived string

			cfg := WorkerPoolConfig{
				LowEffortModel: "low-effort-fast-model",
				Store:          db.NewFakeStore(),
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					recordedModel = model
					recordedPrompt = prompt
					recordedSession = sessionID
					return tt.runnerOut, tt.runnerErrOut, tt.runnerCode, tt.runnerErr
				},
			}

			pool := New(nil, cfg)
			pool.Start()
			defer pool.Stop()

			var onStatus func(string)
			if tt.wantStatusSub != "" {
				onStatus = func(s string) { statusReceived = s }
			}

			reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), tt.prompt, tt.sessionID, onStatus, nil)

			if tt.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("expected error containing %q, got: %v", tt.wantErrSub, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if reply != tt.wantReply {
				t.Errorf("expected reply %q, got %q", tt.wantReply, reply)
			}
			if tt.wantConvID != "" && convID != tt.wantConvID {
				t.Errorf("expected convID %q, got %q", tt.wantConvID, convID)
			}
			if recordedModel != "low-effort-fast-model" {
				t.Errorf("expected low-effort-fast-model, got %q", recordedModel)
			}
			if recordedPrompt != tt.prompt {
				t.Errorf("expected prompt %q, got %q", tt.prompt, recordedPrompt)
			}
			if recordedSession != tt.sessionID {
				t.Errorf("expected session %q, got %q", tt.sessionID, recordedSession)
			}
			if tt.wantStatusSub != "" && !strings.Contains(statusReceived, tt.wantStatusSub) {
				t.Errorf("expected status containing %q, got %q", tt.wantStatusSub, statusReceived)
			}
		})
	}
}

func TestWorkerPool_ExecuteVoiceTurn_VoiceRunnerHook(t *testing.T) {
	t.Parallel()

	var statusReceived string
	cfg := WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Custom hook running...")
			}
			return "voice hook reply", sessionID, nil
		},
	}

	pool := New(nil, cfg)
	pool.Start()
	defer pool.Stop()

	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "sess-hook-1", func(s string) {
		statusReceived = s
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "voice hook reply" {
		t.Errorf("expected reply %q, got %q", "voice hook reply", reply)
	}
	if convID != "sess-hook-1" {
		t.Errorf("expected convID %q, got %q", "sess-hook-1", convID)
	}
	if !strings.Contains(statusReceived, "⚡ Custom hook running...") {
		t.Errorf("expected status containing %q, got %q", "⚡ Custom hook running...", statusReceived)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_VoiceStreamRunnerHook(t *testing.T) {
	t.Parallel()

	var statusReceived string
	var sentenceReceived string

	cfg := WorkerPoolConfig{
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Streaming hook running...")
			}
			if onSentence != nil {
				onSentence("Streaming sentence.")
			}
			return "voice stream reply", sessionID, nil
		},
	}

	pool := New(nil, cfg)
	pool.Start()
	defer pool.Stop()

	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "sess-stream-hook-1", func(s string) {
		statusReceived = s
	}, func(s string) {
		sentenceReceived = s
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "voice stream reply" {
		t.Errorf("expected reply %q, got %q", "voice stream reply", reply)
	}
	if convID != "sess-stream-hook-1" {
		t.Errorf("expected convID %q, got %q", "sess-stream-hook-1", convID)
	}
	if !strings.Contains(statusReceived, "⚡ Streaming hook running...") {
		t.Errorf("expected status containing %q, got %q", "⚡ Streaming hook running...", statusReceived)
	}
	if sentenceReceived != "Streaming sentence." {
		t.Errorf("expected sentence %q, got %q", "Streaming sentence.", sentenceReceived)
	}

	// Stream error case
	errPool := New(nil, WorkerPoolConfig{
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
			return "", sessionID, errors.New("simulated stream failure")
		},
	})
	errPool.Start()
	defer errPool.Stop()

	_, _, err = errPool.ExecuteVoiceTurn(context.Background(), "hello", "sess-err", nil, func(s string) {})
	if err == nil || !strings.Contains(err.Error(), "simulated stream failure") {
		t.Fatalf("expected error containing simulated stream failure, got: %v", err)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ForwardCleanPrompt(t *testing.T) {
	t.Parallel()

	var recordedPrompt string
	cfg := WorkerPoolConfig{
		Store: setupTestStore(t),
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			recordedPrompt = prompt
			return "Ayda is in Emeryville.", sessionID, nil
		},
	}

	pool := New(nil, cfg)
	pool.Start()
	defer pool.Stop()

	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "where does my daughter go to school?", "dev-voice-1", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "Ayda is in Emeryville." {
		t.Errorf("expected reply %q, got %q", "Ayda is in Emeryville.", reply)
	}
	if convID != "dev-voice-1" {
		t.Errorf("expected convID %q, got %q", "dev-voice-1", convID)
	}
	if recordedPrompt != "where does my daughter go to school?" {
		t.Errorf("expected captured prompt %q, got %q", "where does my daughter go to school?", recordedPrompt)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_Guards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sessionID   string
		cancelCtx   bool
		deadPoolCtx bool
		nilPool     bool
		stoppedPool bool
		nilProcPool bool
		wantErrSub  string
		wantErrIs   error
	}{
		{
			name:       "emptySession",
			sessionID:  "",
			wantErrSub: "session_id is required",
		},
		{
			name:       "whitespaceSession",
			sessionID:  "   ",
			wantErrSub: "session_id is required",
		},
		{
			name:      "canceledCallerCtx",
			sessionID: "sess-cancel",
			cancelCtx: true,
			wantErrIs: context.Canceled,
		},
		{
			name:        "deadPoolCtx",
			sessionID:   "sess-dead",
			deadPoolCtx: true,
			wantErrSub:  "brain is shutting down",
		},
		{
			name:       "nilPool",
			sessionID:  "sess-nil",
			nilPool:    true,
			wantErrSub: "worker pool is uninitialized",
		},
		{
			name:        "stoppedPool",
			sessionID:   "sess-stopped",
			stoppedPool: true,
			wantErrSub:  "brain is shutting down",
		},
		{
			name:        "nilProcessPool",
			sessionID:   "sess-1",
			nilProcPool: true,
			wantErrSub:  "voice daemon pool unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			var pool *WorkerPool
			if !tt.nilPool {
				if tt.deadPoolCtx {
					deadCtx, deadCancel := context.WithCancel(context.Background())
					deadCancel()
					pool = &WorkerPool{ctx: deadCtx, stopped: false}
				} else if tt.stoppedPool {
					pool = &WorkerPool{stopped: true}
				} else if tt.nilProcPool {
					pool = New(nil, WorkerPoolConfig{})
					pool.processPool = nil
				} else {
					pool = New(nil, WorkerPoolConfig{
						VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
							return "reply", sessionID, nil
						},
					})
					pool.Start()
					defer pool.Stop()
				}
			}

			_, _, err := pool.ExecuteVoiceTurn(ctx, "hello", tt.sessionID, nil)

			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tt.wantErrIs, err)
				}
				return
			}

			if tt.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("expected error containing %q, got: %v", tt.wantErrSub, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

type voiceProcessPoolSpawnerConfig struct {
	spawnerErr               error
	closeInRPipeImmediately  bool
	initSessionID            string
	stepUpdates              []string
	resultStatus             string
	resultResponse           string
	resultError              string
	resultExitCode           int
	tokens                   runner.TokenUsage
	writeTranscriptLogsDir   string
	cancelCallerOnScan       func()
	blockUntilDone           chan struct{}
}

func newEventDrivenVoiceDaemonSpawner(cfg voiceProcessPoolSpawnerConfig) *runner.MockDaemonSpawner {
	return &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, daemonCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			if cfg.spawnerErr != nil {
				return nil, nil, nil, nil, cfg.spawnerErr
			}
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()

			if cfg.closeInRPipeImmediately {
				_ = inR.Close()
			}

			go func() {
				defer outW.Close()
				defer errW.Close()

				sessID := cfg.initSessionID
				if sessID == "" {
					sessID = "550e8400-e29b-41d4-a716-446655440000"
				}
				_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"init\",\"session_id\":%q,\"conversation_id\":%q}\n", sessID, sessID)))

				scanner := bufio.NewScanner(inR)
				if scanner.Scan() {
					if cfg.cancelCallerOnScan != nil {
						cfg.cancelCallerOnScan()
					}
				}

				if cfg.blockUntilDone != nil {
					<-cfg.blockUntilDone
					return
				}

				for _, step := range cfg.stepUpdates {
					_, _ = outW.Write([]byte(step + "\n"))
				}

				if cfg.writeTranscriptLogsDir != "" {
					now := time.Now().UTC().Format(time.RFC3339Nano)
					transcript := fmt.Sprintf("{\"type\":\"USER_INPUT\",\"source\":\"USER_EXPLICIT\",\"content\":\"What is the status?\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":%q,\"created_at\":%q}\n", now, cfg.resultResponse, now)
					_ = os.WriteFile(filepath.Join(cfg.writeTranscriptLogsDir, "transcript.jsonl"), []byte(transcript), 0644)
				}

				status := cfg.resultStatus
				if status == "" {
					status = "SUCCESS"
				}
				exitCodePart := ""
				if cfg.resultExitCode != 0 {
					exitCodePart = fmt.Sprintf(",\"exit_code\":%d", cfg.resultExitCode)
				}
				usagePart := ""
				if cfg.tokens.TotalTokens > 0 {
					usagePart = fmt.Sprintf(",\"usage\":{\"input_tokens\":%d,\"output_tokens\":%d,\"total_tokens\":%d}", cfg.tokens.InputTokens, cfg.tokens.OutputTokens, cfg.tokens.TotalTokens)
				}
				resultJSON := fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":%q,\"response\":%q,\"error\":%q,\"conversation_id\":%q%s%s}}\n",
					status, cfg.resultResponse, cfg.resultError, sessID, exitCodePart, usagePart)
				_, _ = outW.Write([]byte(resultJSON))
			}()

			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPoolSuccessAndMetrics(t *testing.T) {
	t.Parallel()

	stepUpdates := []string{
		"{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool_call\",\"tool_name\":\"ha_call_service\",\"tool_info\":{\"parameters\":{\"CommandLine\":\"\"}}}}",
		"{\"event\":\"step_update\",\"step_update\":{\"state\":\"RUNNING\",\"type\":\"tool\",\"tool_name\":\"\",\"tool_info\":{\"parameters\":{\"CommandLine\":\"system-health\"}}}}",
		"{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"type\":\"tool_call\",\"tool_name\":\"ha_call_service\"}}",
		"{\"event\":\"step_update\",\"step_update\":{\"state\":\"ERROR\",\"type\":\"tool_call\",\"tool_name\":\"ha_call_service\"}}",
	}

	spawner := newEventDrivenVoiceDaemonSpawner(voiceProcessPoolSpawnerConfig{
		stepUpdates:    stepUpdates,
		resultStatus:   "SUCCESS",
		resultResponse: "daemon voice turn reply",
		tokens:         runner.TokenUsage{InputTokens: 60, OutputTokens: 40, TotalTokens: 100},
	})

	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
	defer procPool.Close()

	pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
	pool.Start()
	defer pool.Stop()

	var statuses []string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "turn on living room light", "kiosk", func(status string) {
		statuses = append(statuses, status)
	}, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "daemon voice turn reply" {
		t.Errorf("expected reply %q, got %q", "daemon voice turn reply", reply)
	}
	if !runner.IsValidUUID(convID) || len(statuses) < 2 {
		t.Fatalf("unexpected success result: convID=%q statuses=%v", convID, statuses)
	}

	reply2, _, err2 := pool.ExecuteVoiceTurn(context.Background(), "turn on living room light", "sess-no-status", nil, nil)
	if err2 != nil || reply2 != "daemon voice turn reply" {
		t.Fatalf("unexpected nil onStatus error: %v", err2)
	}

	pool.scopeLocks.Store("voice-sess-bad-lock", "not-a-mutex")
	reply3, _, err3 := pool.ExecuteVoiceTurn(context.Background(), "test", "sess-bad-lock", nil, nil)
	if err3 != nil || reply3 != "daemon voice turn reply" {
		t.Fatalf("unexpected error on recovered bad lock: %v", err3)
	}

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `source="voice"`) || !strings.Contains(body, `channel="voice"`) {
		t.Errorf("expected metrics to contain source=voice and channel=voice")
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPoolDeviceRouting(t *testing.T) {
	t.Parallel()

	var spawnedTarget string
	spawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			spawnedTarget = cfg.ThreadID
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			go func() {
				defer outW.Close()
				defer errW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\"}\n"))
				scanner := bufio.NewScanner(inR)
				if scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"kiosk device reply\",\"conversation_id\":\"550e8400-e29b-41d4-a716-446655440000\"}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(8888), nil
		},
	}

	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
	defer procPool.Close()

	pool := New(nil, WorkerPoolConfig{VoiceProcessPool: procPool})
	pool.Start()
	defer pool.Stop()

	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "what is the weather", "kiosk", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "kiosk device reply" {
		t.Errorf("expected reply %q, got %q", "kiosk device reply", reply)
	}
	if convID != "550e8400-e29b-41d4-a716-446655440000" || spawnedTarget != "kiosk" {
		t.Fatalf("unexpected routing result: convID=%q target=%q", convID, spawnedTarget)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPoolTranscriptRecovery(t *testing.T) {
	t.Parallel()

	targetSess := "550e8400-e29b-41d4-a716-446655449999"
	tempDir := t.TempDir()
	logsDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", targetSess, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("mkdir logs failed: %v", err)
	}

	spawner := newEventDrivenVoiceDaemonSpawner(voiceProcessPoolSpawnerConfig{
		initSessionID:          targetSess,
		resultStatus:           "ERROR",
		resultError:            "RESOURCE_EXHAUSTED (code 429)",
		resultResponse:         "All systems operational and nominal.",
		writeTranscriptLogsDir: logsDir,
	})

	sessMgr := session.New(tempDir, tempDir)
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel:      "gemini-2.5-flash",
		TranscriptRescuer: sessMgr.ExtractResponseSince,
	}, spawner)
	defer procPool.Close()

	pool := New(nil, WorkerPoolConfig{
		SessionManager: sessMgr,
		ProcessPool:    procPool,
	})
	pool.Start()
	defer pool.Stop()

	var sentences []string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "What is the status?", targetSess, nil, func(s string) {
		sentences = append(sentences, s)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "All systems operational and nominal." {
		t.Errorf("expected reply %q, got %q", "All systems operational and nominal.", reply)
	}
	if convID != targetSess || len(sentences) == 0 {
		t.Fatalf("unexpected transcript recovery: sessionID=%q sentences=%v", convID, sentences)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPoolErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		spawnerErr               error
		closeInRPipeImmediately  bool
		cancelCallerWaiting      bool
		clearPreSpawnedSessionID bool
		resultStatus             string
		resultError              string
		resultExitCode           int
		wantErrSub               string
	}{
		{
			name:       "AcquisitionFailure",
			spawnerErr: errors.New("spawner failed to start"),
			wantErrSub: "failed to acquire voice daemon",
		},
		{
			name:         "DaemonExecutionError",
			resultStatus: "ERROR",
			resultError:  "simulated failure in daemon",
			wantErrSub:   "simulated failure in daemon",
		},
		{
			name:                    "SendFailureBrokenPipe",
			closeInRPipeImmediately: true,
			wantErrSub:              "failed sending turn to voice daemon",
		},
		{
			name:                "ContextCanceledWaitingForResult",
			cancelCallerWaiting: true,
			wantErrSub:          "context canceled",
		},
		{
			name:                     "ExitCodeNonZeroAndEmptySessionFallback",
			resultStatus:             "SUCCESS",
			resultExitCode:           1,
			clearPreSpawnedSessionID: true,
			wantErrSub:               "empty response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var blockCh chan struct{}
			var onScan func()
			if tt.cancelCallerWaiting {
				blockCh = make(chan struct{})
				defer close(blockCh)
				onScan = func() { cancel() }
			}

			spawner := newEventDrivenVoiceDaemonSpawner(voiceProcessPoolSpawnerConfig{
				spawnerErr:              tt.spawnerErr,
				closeInRPipeImmediately: tt.closeInRPipeImmediately,
				resultStatus:            tt.resultStatus,
				resultError:             tt.resultError,
				resultExitCode:          tt.resultExitCode,
				cancelCallerOnScan:      onScan,
				blockUntilDone:          blockCh,
			})

			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
			defer procPool.Close()

			pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
			pool.Start()
			defer pool.Stop()

			targetSess := "sess-test"
			if tt.clearPreSpawnedSessionID {
				targetSess = "fallback-conv-id"
				d, err := procPool.GetOrCreate(context.Background(), targetSess, "")
				if err != nil {
					t.Fatalf("failed pre-spawning: %v", err)
				}
				d.SetSessionID("")
			}

			_, convID, err := pool.ExecuteVoiceTurn(ctx, "test prompt", targetSess, nil, nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErrSub, err)
			}
			if tt.clearPreSpawnedSessionID && convID != targetSess {
				t.Errorf("expected fallback sessID %q, got %q", targetSess, convID)
			}
		})
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

func TestWorkerPool_Stop_ProcessPoolCloseError(t *testing.T) {
	t.Parallel()
	mockHandle := runner.NewMockProcessHandle(99999)
	mockHandle.SetKillErr(errors.New("simulated kill process failure"))

	spawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			go func() {
				defer outW.Close()
				defer errW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440001\"}\n"))
				_, _ = io.Copy(io.Discard, inR)
			}()
			return inW, outR, errR, mockHandle, nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
	_, err := procPool.GetOrCreate(context.Background(), "kiosk", "")
	if err != nil {
		t.Fatalf("failed to spawn daemon: %v", err)
	}

	pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
	pool.Start()
	pool.Stop()
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
	if err != nil || reply != "Hello from mock voice." || sessID != "sess-mock-dev-1" || sentenceReceived != "Hello from mock voice." {
		t.Fatalf("unexpected mock voice result: reply=%q sessID=%q sentence=%q err=%v", reply, sessID, sentenceReceived, err)
	}

	pool.MarkDirty()
	pool.Stop()
	if !mockPool.closeCalled {
		t.Errorf("expected mockPool.Close() to be called on Stop")
	}
}
