package queue

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/azylman/aerial/brain/pkg/discord"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
)

func TestWorkerPool_LowEffortModelResolution(t *testing.T) {
	t.Parallel()
	appCfgVal, appCfgEmpty := "app-cfg-low-model", ""
	defaultModel := config.DefaultConfigData().LowEffortModel

	tests := []struct {
		name, cfgModel string
		poolNil        bool
		appCfgVal      *string
		wantResult     string
	}{
		{"NilPoolFallback", "", true, nil, defaultModel},
		{"CustomModelInCfg", "gemini-flash-low-test", false, nil, "gemini-flash-low-test"},
		{"AppCfgOverride", "", false, &appCfgVal, "app-cfg-low-model"},
		{"AppCfgEmptyWithCfgFallback", "cfg-fallback-model", false, &appCfgEmpty, "cfg-fallback-model"},
		{"AppCfgEmptyAndCfgEmptyDefaults", "", false, &appCfgEmpty, defaultModel},
		{"AppCfgNilAndCfgEmptyDefaults", "", false, nil, defaultModel},
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
				appCfg = config.NewTestConfig(func(d *config.ConfigData) { d.LowEffortModel = val })
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
	appCfgVal, appCfgEmpty := "app-cfg-med-model", ""
	defaultModel := config.DefaultConfigData().MediumEffortModel

	tests := []struct {
		name, cfgModel string
		poolNil        bool
		appCfgVal      *string
		wantResult     string
	}{
		{"NilPoolFallback", "", true, nil, defaultModel},
		{"CustomModelInCfg", "gemini-flash-med-test", false, nil, "gemini-flash-med-test"},
		{"AppCfgOverride", "", false, &appCfgVal, "app-cfg-med-model"},
		{"AppCfgEmptyWithCfgFallback", "cfg-med-fallback", false, &appCfgEmpty, "cfg-med-fallback"},
		{"AppCfgEmptyAndCfgEmptyDefaults", "", false, &appCfgEmpty, defaultModel},
		{"AppCfgNilAndCfgEmptyDefaults", "", false, nil, defaultModel},
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
				appCfg = config.NewTestConfig(func(d *config.ConfigData) { d.MediumEffortModel = val })
			}
			p := New(appCfg, WorkerPoolConfig{MediumEffortModel: tt.cfgModel})
			if got := p.MediumEffortModel(); got != tt.wantResult {
				t.Errorf("MediumEffortModel() = %q, want %q", got, tt.wantResult)
			}
		})
	}
}

func TestWorkerPool_ExecuteVoiceTurn_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, prompt, sessionID             string
		poolNil, poolStopped, poolDeadCtx   bool
		cancelCaller, nilProcessPool        bool
		useVoiceHook, useVoiceStreamHook    bool
		hookErr                             error
		runnerOut, runnerErrOut             string
		runnerCode                          int
		runnerErr, spawnerErr               error
		closePipeImmediately                bool
		cancelCallerOnScan                  bool
		clearPreSpawnedSessionID            bool
		resultStatus, resultError           string
		resultExitCode                      int
		wantReply, wantConvID, wantErrSub   string
		wantErrIs                           error
		wantStatusSub, wantSentenceSub      string
	}{
		// Guards
		{name: "emptySession", prompt: "hello", sessionID: "", wantErrSub: "session_id is required"},
		{name: "whitespaceSession", prompt: "hello", sessionID: "   ", wantErrSub: "session_id is required"},
		{name: "canceledCallerCtx", prompt: "hello", sessionID: "sess-cancel", cancelCaller: true, wantErrIs: context.Canceled},
		{name: "deadPoolCtx", prompt: "hello", sessionID: "sess-dead", poolDeadCtx: true, wantErrSub: "brain is shutting down"},
		{name: "nilPool", prompt: "hello", sessionID: "sess-nil", poolNil: true, wantErrSub: "worker pool is uninitialized"},
		{name: "stoppedPool", prompt: "hello", sessionID: "sess-stopped", poolStopped: true, wantErrSub: "brain is shutting down"},
		{name: "nilProcessPool", prompt: "hello", sessionID: "sess-1", nilProcessPool: true, wantErrSub: "voice daemon pool unavailable"},

		// CustomRunnerOptions
		{name: "SuccessTurn", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: `{"event":"result","result":{"status":"SUCCESS","response":"turned off fan"}}`, wantReply: "turned off fan", wantConvID: "sess-custom-1", wantStatusSub: "⚡"},
		{name: "RunnerError", prompt: "turn off fan", sessionID: "sess-custom-1", runnerErrOut: "exec error", runnerCode: 1, runnerErr: errors.New("exec failed"), wantErrSub: "exec failed"},
		{name: "ExitCodeNonZero", prompt: "turn off fan", sessionID: "sess-custom-1", runnerErrOut: "process died", runnerCode: 42, wantErrSub: "exit code 42"},
		{name: "PlainTextFallback", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: "plain text response", wantReply: "plain text response", wantConvID: "sess-custom-1"},
		{name: "EmptyResponseField", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: `{"event":"result","result":{"status":"SUCCESS","response":""}}`, wantErrSub: "empty response"},

		// Hooks
		{name: "VoiceRunnerHook", prompt: "hello", sessionID: "sess-hook-1", useVoiceHook: true, wantReply: "voice hook reply", wantConvID: "sess-hook-1", wantStatusSub: "⚡ Custom hook"},
		{name: "VoiceStreamRunnerHook", prompt: "hello", sessionID: "sess-stream-1", useVoiceStreamHook: true, wantReply: "voice stream reply", wantConvID: "sess-stream-1", wantStatusSub: "⚡ Streaming hook", wantSentenceSub: "Streaming sentence."},
		{name: "VoiceStreamRunnerHookError", prompt: "hello", sessionID: "sess-err", useVoiceStreamHook: true, hookErr: errors.New("simulated stream failure"), wantErrSub: "simulated stream failure"},
		{name: "ForwardCleanPrompt", prompt: "where does my daughter go to school?", sessionID: "dev-voice-1", useVoiceHook: true, wantReply: "Ayda is in Emeryville.", wantConvID: "dev-voice-1"},

		// ProcessPoolErrors
		{name: "AcquisitionFailure", prompt: "test", sessionID: "sess-acq-fail", spawnerErr: errors.New("spawner failed to start"), wantErrSub: "failed to acquire voice daemon"},
		{name: "DaemonExecutionError", prompt: "test", sessionID: "sess-daemon-err", resultStatus: "ERROR", resultError: "simulated failure in daemon", wantErrSub: "simulated failure in daemon"},
		{name: "SendFailureBrokenPipe", prompt: "test", sessionID: "sess-pipe-err", closePipeImmediately: true, wantErrSub: "failed sending turn to voice daemon"},
		{name: "ContextCanceledWaitingForResult", prompt: "test", sessionID: "sess-ctx-cancel", cancelCallerOnScan: true, wantErrSub: "context canceled"},
		{name: "ExitCodeNonZeroAndEmptySessionFallback", prompt: "test", sessionID: "sess-exit-fallback", clearPreSpawnedSessionID: true, resultStatus: "SUCCESS", resultExitCode: 1, wantErrSub: "empty response"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelCaller {
				cancel()
			}

			var pool *WorkerPool
			var procPool *runner.UnifiedProcessPool
			var recordedPrompt, statusReceived, sentenceReceived string

			if tt.poolNil {
				pool = nil
			} else if tt.poolDeadCtx {
				deadCtx, deadCancel := context.WithCancel(context.Background())
				deadCancel()
				pool = &WorkerPool{ctx: deadCtx}
			} else if tt.poolStopped {
				pool = &WorkerPool{stopped: true}
			} else if tt.nilProcessPool {
				pool = New(nil, WorkerPoolConfig{})
				pool.processPool = nil
			} else if tt.useVoiceHook {
				pool = New(nil, WorkerPoolConfig{
					VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
						recordedPrompt = prompt
						if onStatus != nil {
							onStatus("⚡ Custom hook running...")
						}
						reply := "voice hook reply"
						if prompt == "where does my daughter go to school?" {
							reply = "Ayda is in Emeryville."
						}
						return reply, sessionID, nil
					},
				})
				pool.Start()
				defer pool.Stop()
			} else if tt.useVoiceStreamHook {
				pool = New(nil, WorkerPoolConfig{
					VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
						if tt.hookErr != nil {
							return "", sessionID, tt.hookErr
						}
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
			} else if tt.runnerOut != "" || tt.runnerErr != nil || tt.runnerCode != 0 {
				pool = New(nil, WorkerPoolConfig{
					LowEffortModel: "low-effort-fast-model",
					Store:          db.NewFakeStore(),
					RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
						return tt.runnerOut, tt.runnerErrOut, tt.runnerCode, tt.runnerErr
					},
				})
				pool.Start()
				defer pool.Stop()
			} else {
				var blockCh chan struct{}
				var onScan func()
				if tt.cancelCallerOnScan {
					blockCh = make(chan struct{})
					defer close(blockCh)
					onScan = func() { cancel() }
				}
				spawner := newEventDrivenVoiceDaemonSpawner(voiceProcessPoolSpawnerConfig{
					spawnerErr:              tt.spawnerErr,
					closeInRPipeImmediately: tt.closePipeImmediately,
					resultStatus:            tt.resultStatus,
					resultError:             tt.resultError,
					resultExitCode:          tt.resultExitCode,
					cancelCallerOnScan:      onScan,
					blockUntilDone:          blockCh,
				})
				procPool = runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
				defer procPool.Close()

				pool = New(nil, WorkerPoolConfig{ProcessPool: procPool})
				pool.Start()
				defer pool.Stop()
			}

			targetSess := tt.sessionID
			if tt.clearPreSpawnedSessionID && procPool != nil {
				targetSess = "fallback-conv-id"
				d, err := procPool.GetOrCreate(context.Background(), targetSess, "")
				if err != nil {
					t.Fatalf("failed pre-spawning: %v", err)
				}
				d.SetSessionID("")
			}

			reply, convID, err := pool.ExecuteVoiceTurn(ctx, tt.prompt, targetSess, func(s string) { statusReceived = s }, func(s string) { sentenceReceived = s })

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
			if tt.wantReply != "" && reply != tt.wantReply {
				t.Errorf("reply = %q, want %q", reply, tt.wantReply)
			}
			if tt.wantConvID != "" && convID != tt.wantConvID {
				t.Errorf("convID = %q, want %q", convID, tt.wantConvID)
			}
			if tt.wantStatusSub != "" && !strings.Contains(statusReceived, tt.wantStatusSub) {
				t.Errorf("status = %q, want substring %q", statusReceived, tt.wantStatusSub)
			}
			if tt.wantSentenceSub != "" && sentenceReceived != tt.wantSentenceSub {
				t.Errorf("sentence = %q, want %q", sentenceReceived, tt.wantSentenceSub)
			}
			if tt.name == "ForwardCleanPrompt" && recordedPrompt != tt.prompt {
				t.Errorf("recorded prompt = %q, want %q", recordedPrompt, tt.prompt)
			}
		})
	}
}

type voiceProcessPoolSpawnerConfig struct {
	spawnerErr              error
	closeInRPipeImmediately bool
	initSessionID           string
	stepUpdates             []string
	resultStatus            string
	resultResponse          string
	resultError             string
	resultExitCode          int
	tokens                  runner.TokenUsage
	writeTranscriptLogsDir  string
	cancelCallerOnScan      func()
	blockUntilDone          chan struct{}
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
				if scanner.Scan() && cfg.cancelCallerOnScan != nil {
					cfg.cancelCallerOnScan()
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
				exitCodePart, usagePart := "", ""
				if cfg.resultExitCode != 0 {
					exitCodePart = fmt.Sprintf(",\"exit_code\":%d", cfg.resultExitCode)
				}
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
		stepUpdates: stepUpdates, resultStatus: "SUCCESS", resultResponse: "daemon voice turn reply",
		tokens: runner.TokenUsage{InputTokens: 60, OutputTokens: 40, TotalTokens: 100},
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
	if err != nil || reply != "daemon voice turn reply" || !runner.IsValidUUID(convID) || len(statuses) < 2 {
		t.Fatalf("unexpected success result: convID=%q statuses=%v err=%v", convID, statuses, err)
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
	if body := rec.Body.String(); !strings.Contains(body, `source="voice"`) || !strings.Contains(body, `channel="voice"`) {
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
	if err != nil || reply != "kiosk device reply" || convID != "550e8400-e29b-41d4-a716-446655440000" || spawnedTarget != "kiosk" {
		t.Fatalf("unexpected routing result: convID=%q target=%q err=%v", convID, spawnedTarget, err)
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
		initSessionID: targetSess, resultStatus: "ERROR", resultError: "RESOURCE_EXHAUSTED (code 429)",
		resultResponse: "All systems operational and nominal.", writeTranscriptLogsDir: logsDir,
	})
	sessMgr := session.New(tempDir, tempDir)
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash", TranscriptRescuer: sessMgr.ExtractResponseSince}, spawner)
	defer procPool.Close()

	pool := New(nil, WorkerPoolConfig{SessionManager: sessMgr, ProcessPool: procPool})
	pool.Start()
	defer pool.Stop()

	var sentences []string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "What is the status?", targetSess, nil, func(s string) {
		sentences = append(sentences, s)
	})
	if err != nil || reply != "All systems operational and nominal." || convID != targetSess || len(sentences) == 0 {
		t.Fatalf("unexpected transcript recovery: sessionID=%q sentences=%v err=%v", convID, sentences, err)
	}
}

func TestContainsToken_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, input, token string
		expected           bool
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
	_, _ = procPool.GetOrCreate(context.Background(), "kiosk", "")
	pool := New(nil, WorkerPoolConfig{ProcessPool: procPool})
	pool.Start()
	pool.Stop()
}

type queueMockVoiceSession struct {
	sessionID string
	sendFn    func(prompt string, turn *runner.TurnContext) error
}

func (s *queueMockVoiceSession) SessionID() string { return s.sessionID }
func (s *queueMockVoiceSession) Send(prompt string, turn *runner.TurnContext) error {
	if s.sendFn != nil {
		return s.sendFn(prompt, turn)
	}
	if turn != nil && turn.Sink != nil {
		turn.Sink.OnTurnStarted()
		turn.Sink.OnTextDelta("Hello from mock voice.")
		turn.Sink.OnResult(&runner.TurnResult{Response: "Hello from mock voice."})
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
func (p *queueMockVoiceProcessPool) Initialize(ctx context.Context) error { p.initCalled = true; return nil }
func (p *queueMockVoiceProcessPool) Close() error                         { p.closeCalled = true; return nil }

var _ runner.AgentPool = (*queueMockVoiceProcessPool)(nil)
var _ runner.AgentSession = (*queueMockVoiceSession)(nil)

func TestWorkerPool_ExecuteVoiceTurn_MockVoiceProcessPool(t *testing.T) {
	t.Parallel()
	mockPool := &queueMockVoiceProcessPool{sessions: make(map[string]*queueMockVoiceSession)}
	pool := New(nil, WorkerPoolConfig{VoiceProcessPool: mockPool})
	pool.Start()
	defer pool.Stop()

	if pool.VoiceProcessPool() != mockPool {
		t.Fatalf("expected VoiceProcessPool to return mockPool")
	}
	var sentenceReceived string
	reply, sessID, err := pool.ExecuteVoiceTurn(context.Background(), "ping", "mock-dev-1", nil, func(sentence string) { sentenceReceived = sentence })
	if err != nil || reply != "Hello from mock voice." || sessID != "sess-mock-dev-1" || sentenceReceived != "Hello from mock voice." {
		t.Fatalf("unexpected mock voice result: reply=%q sessID=%q sentence=%q err=%v", reply, sessID, sentenceReceived, err)
	}
	pool.MarkDirty()
	pool.Stop()
	if !mockPool.closeCalled {
		t.Errorf("expected mockPool.Close() to be called on Stop")
	}
}

func TestVoiceAndPoolHelperCoverage(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	defer fake.Close()

	// 1. Store and getters
	for _, v := range []interface{}{nil, "invalid", (*db.FakeStore)(nil), (*db.SQLStore)(nil), (*sql.DB)(nil)} {
		_ = resolveStore(v)
	}
	_ = resolveStore(fake)

	var nilPool *WorkerPool
	_ = nilPool.Store(); _ = nilPool.SummaryGroup(); _ = nilPool.AgentPool(); _ = nilPool.DeliverDirect("c", "t"); _, _ = nilPool.DeliverStatusWidget(discord.StatusWidgetRequest{})
	pEmpty := &WorkerPool{}
	_ = pEmpty.Store(); _ = pEmpty.SummaryGroup(); _ = pEmpty.Classifier(); _ = pEmpty.SessionManager(); _ = pEmpty.DeliverDirect("c", "t"); _, _ = pEmpty.DeliverStatusWidget(discord.StatusWidgetRequest{})
	pFake := &WorkerPool{cfg: WorkerPoolConfig{Store: fake}}
	_ = pFake.Store()
	realDB := &sql.DB{}
	pDB := &WorkerPool{cfg: WorkerPoolConfig{DB: realDB}}
	_ = pDB.Store(); _ = resolveStore(realDB)

	// 2. Activity and TurnExecution helpers
	_, _, _ = GetSessionLastActivity("invalid-type", "t1")
	for _, tid := range []string{"thread-1", ""} {
		_, _, _ = GetSessionLastActivity(nil, tid)
	}
	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	_, _, _ = GetSessionLastActivity(closedStore, "thread-1")
	_ = insertMessage(fake, db.Message{ID: "m-last-act", ThreadID: "t-last-act", AuthorID: "u1", Status: db.StatusCompleted, CreatedAt: time.Now().UTC()})
	_, _, _ = GetSessionLastActivity(fake, "t-last-act")

	for _, te := range []*turnExecution{
		{},
		{pool: pFake},
		{pool: &WorkerPool{cfg: WorkerPoolConfig{Store: &errQueueStore{Store: fake}}}},
	} {
		te.updateMessageStatus("m1", "failed", "err")
		te.updateScheduleRunStatus(db.UpdateRunParams{RunID: "r1"})
		te.updateMessageCompleted("m1", "resp")
		te.incrementMessageRetry("m1", "retry-err")
		te.saveSessionID("t1", "s1")
		te.rotateSessionID("t1", "s2")
		_, _ = te.getSessionTurnCount("t1")
		_, _ = te.incrementSessionTurnCount("t1")
		_, _ = te.getSessionID("t1")
		_, _ = te.getPreviousSessionID("t1")
		_ = te.createOneShotSchedule(db.OneShotSchedule{ID: "os1"})
		_, _, _ = te.getThreadSummary("t1")
		_ = te.saveThreadSummary("t1", "sum", "m1")
		_, _ = te.getRecentThreadMessages("t1", 10)
	}

	// 3. Summaries, history, and classifier alerts
	sumRes, sumErr := SummarizeThreadHistory(context.Background(), func(ctx context.Context, model, prompt string) (string, error) {
		return "<THREAD_SUMMARY>\nSummary result\n</THREAD_SUMMARY>", nil
	}, "test-model", "thread-sum-wrapper", []HistoryMessage{{ID: "m1", Content: "fresh", CreatedAt: time.Now().UTC()}})
	if sumErr != nil || !strings.Contains(sumRes, "Summary result") {
		t.Errorf("SummarizeThreadHistory failed: res=%q err=%v", sumRes, sumErr)
	}
	_, _ = SummarizeThreadHistory(context.Background(), nil, "", "t1", nil)

	_, _ = fetchHistoryFromDB(fake, "role-test-chan", "msg-role-7", 50)
	_, _ = fetchHistoryFromDB(fake, "role-test-chan", "123456789012345678", 50)
	_ = FilterAssistantMessages(nil)
	_ = FilterAssistantMessages([]HistoryMessage{})

	appCfg := config.NewFromData(&config.ConfigData{SystemChannel: "test-alerts-chan", APIKey: "test-key", AgyBin: "agy"})
	poolAlert := New(appCfg, WorkerPoolConfig{SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, alertBody string) error { return nil }})
	if poolAlert.cfg.Classifier != nil && poolAlert.cfg.Classifier.OnParseError != nil {
		poolAlert.cfg.Classifier.OnParseError("gemini-flash", strings.Repeat("x", 700)+"\n```json\n@everyone @here\n```", errors.New("parse error 1"))
		poolAlert.cfg.Classifier.OnParseError("gemini-flash", "short", errors.New("parse error 2"))
	}

	// 4. Runner options fallbacks & pool lifecycle
	ctx := context.Background()
	pNew1 := New(nil, WorkerPoolConfig{
		Model: "custom-model",
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "out", "", 0, nil
		},
	})
	_, _, _, _ = pNew1.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{})
	pNew2 := New(nil, WorkerPoolConfig{
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return "opt-out", "", 0, nil
		},
	})
	_, _, _, _ = pNew2.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 15)
	pNew3 := New(nil, WorkerPoolConfig{})
	_, _, _, _ = pNew3.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 10)
	_, _, _, _ = pNew3.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{})

	mockDrain := &mockDrainErrStore{Store: fake, messages: []db.Message{{ID: "m1", ThreadID: "t1", Status: db.StatusProcessing, RestartCount: 5, RetryCount: 5}}}
	pDrainMock := New(nil, WorkerPoolConfig{Store: mockDrain})
	RecoverInterrupted(mockDrain, pDrainMock)

	pDrain := New(nil, WorkerPoolConfig{Store: &mockDrainErrStore{Store: fake}, DrainTimeout: 10 * time.Millisecond})
	pDrain.StopWithTimeout(0)

	pBurst := New(nil, WorkerPoolConfig{})
	pBurst.EnqueueBurst()
	pBurst.processBurst(nil)
	pBurst.processBurst([]db.Message{})
	pBurst.StopWithTimeout(10 * time.Millisecond)

	// 5. Status updater branches
	var nilSu *StatusUpdater
	nilSu.MarkTurnStarted(); nilSu.Stop(); nilSu.Reset(); nilSu.DeleteStatusMessage()
	nilSu.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})

	uDisabled := NewStatusUpdater(nil, "thread-1", false)
	uDisabled.Start()
	uEnabled := NewStatusUpdater(nil, "thread-2", true, WithStatusInterval(50*time.Millisecond))
	uEnabled.Start(); uEnabled.Stop()

	formatted := FormatToolStatus("run_command", "really_long_command_name_that_exceeds_twenty_four_runes", 1500*time.Millisecond)
	if !strings.Contains(formatted, "really_long_command_name") {
		t.Errorf("expected command truncation in %s", formatted)
	}
	for _, tool := range []string{"bash", "sh", "execute_command"} {
		_ = FormatToolStatus(tool, "echo hi", 500*time.Millisecond)
	}
	_ = FormatToolStatus(strings.Repeat("x", 120), "", 500*time.Millisecond)

	s, _ := discordgo.New("Bot dummy-token")
	s.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		u := req.URL.String()
		switch {
		case req.Method == "POST" && strings.Contains(u, "/messages"):
			b, _ := json.Marshal(&discordgo.Message{ID: "msg-created-123"})
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		case req.Method == "PATCH":
			b, _ := json.Marshal(&discordgo.Message{ID: "msg-edited-123"})
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		case req.Method == "DELETE":
			return &http.Response{StatusCode: 204, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		default:
			return &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewReader([]byte(`{"message":"bad request"}`)))}, nil
		}
	})
	uREST := NewStatusUpdater(s, "thread-rest-123", true)
	_, _ = uREST.sendFunc("thread-rest-123", "hello")
	_ = uREST.editFunc("thread-rest-123", "msg-created-123", "edited")
	_ = uREST.deleteFunc("thread-rest-123", "msg-created-123")

	uStep := NewStatusUpdater(nil, "thread-step", true, WithStatusInterval(10*time.Hour))
	uStep.Stop(); uStep.disabled = true; uStep.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	uStep.disabled = false
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	uStep.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool_call", ToolName: "view_file", State: "ACTIVE",
		ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "ls"}},
	})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "other_tool", State: "DONE"})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "view_file", State: "ERROR"})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "agent_response"})

	// 6. Bot roles, prompt assembly, and tier-1 wakes
	dgBot, _ := discordgo.New("Bot dummy")
	dgBot.State.User = &discordgo.User{ID: "bot-1", Username: "AerialBot"}
	guild1 := &discordgo.Guild{
		ID:    "guild-1",
		Roles: []*discordgo.Role{nil, {ID: "role-aerial", Name: "aerial"}, {ID: "role-gundam", Name: "gundam"}, {ID: "role-username", Name: "AerialBot"}, {ID: "role-other", Name: "moderator"}},
		Members: []*discordgo.Member{{User: &discordgo.User{ID: "bot-1"}, Roles: []string{"role-member-assigned", ""}}},
	}
	_ = dgBot.State.GuildAdd(guild1)
	_ = ResolveBotRoleIDs(dgBot, "guild-1", "bot-1")
	_ = ResolveBotRoleIDs(dgBot, "", "bot-1")
	_ = ResolveBotRoleIDs(nil, "", "")
	_ = ResolveBotRoleIDs(&discordgo.Session{}, "", "")

	origFetcher := guildMemberFetcher
	defer func() { guildMemberFetcher = origFetcher }()
	guildMemberFetcher = func(s *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
		return &discordgo.Member{GuildID: guildID, User: &discordgo.User{ID: userID}, Roles: []string{"role1"}}, nil
	}
	_ = ResolveBotRoleIDs(&discordgo.Session{State: discordgo.NewState()}, "guild-not-in-state", "bot-1")

	for _, tc := range []struct {
		in   TurnPromptInput
		want string
	}{
		{TurnPromptInput{ThreadSummary: "summary text"}, "summary text"},
		{TurnPromptInput{LookbackHistory: []HistoryMessage{{AuthorName: "alice", Content: "hi", CreatedAt: time.Now()}}}, "@alice"},
		{TurnPromptInput{IsColdStart: true, PreviousSessionID: "prev-sess-123"}, "<PREVIOUS_SESSION>"},
		{TurnPromptInput{ChannelInstructions: "instructions"}, "instructions"},
		{TurnPromptInput{InjectedHookContext: "hook context"}, "hook context"},
	} {
		if got := AssembleTurnPrompt(tc.in); !strings.Contains(got, tc.want) {
			t.Errorf("AssembleTurnPrompt(%+v): expected %q in %q", tc.in, tc.want, got)
		}
	}

	wakeTests := []struct {
		name      string
		msg       db.Message
		botUserID string
		botRoles  []string
		expected  bool
	}{
		{"http-client author", db.Message{AuthorID: "http-client"}, "", nil, true},
		{"scheduler author", db.Message{AuthorID: "scheduler"}, "", nil, true},
		{"schedule run ID", db.Message{ScheduleRunID: "run-123"}, "", nil, true},
		{"direct user mention", db.Message{Content: "Hey <@bot-123>"}, "bot-123", nil, true},
		{"direct nickname mention", db.Message{Content: "Hey <@!bot-123>"}, "bot-123", nil, true},
		{"direct role mention", db.Message{Content: "Alert <@&role-999>"}, "", []string{"role-999"}, true},
		{"mentions aerial in header", db.Message{Content: "<USER_REQUEST>\n- mentions: [aerial, alice]\n</USER_REQUEST>"}, "", nil, true},
		{"mentions botUserID in header", db.Message{Content: "<USER_REQUEST>\n- mentions: [bot-123]\n</USER_REQUEST>"}, "bot-123", nil, true},
		{"mentions role in header", db.Message{Content: "<USER_REQUEST>\n- mentions: [role-999]\n</USER_REQUEST>"}, "", []string{"role-999"}, true},
		{"replying to aerial", db.Message{Content: "<USER_REQUEST>\n- replying_to:\n  author: aerial\n  content: hi\n- content: yes</USER_REQUEST>"}, "", nil, true},
		{"replying to bot ID", db.Message{Content: "<USER_REQUEST>\n- replying_to:\n  author: bot-123\n  content: hi\n- content: yes</USER_REQUEST>"}, "bot-123", nil, true},
		{"body mention prefix", db.Message{Content: "Hey <@aerial how are you"}, "", nil, true},
		{"body nick mention prefix", db.Message{Content: "Hey <@!aerial how are you"}, "", nil, true},
		{"plaintext keyword mention mode", db.Message{Content: "hello aerial"}, "", nil, false},
		{"quoted role without mention_role_ids", db.Message{Content: "<USER_REQUEST>\n- content: check this '<@&999888>'\n- mention_role_ids: []\n</USER_REQUEST>"}, "123", []string{"999888"}, false},
		{"structured role in mention_role_ids", db.Message{Content: "<USER_REQUEST>\n- content: check this\n- mention_role_ids: [999888]\n</USER_REQUEST>"}, "123", []string{"999888"}, true},
		{"bot in mention_user_ids", db.Message{Content: "<USER_REQUEST>\n- content: please help\n- mention_user_ids: [123]\n</USER_REQUEST>"}, "123", nil, true},
		{"body direct snowflake mention", db.Message{Content: "hey <@12345> check this"}, "12345", nil, true},
		{"body role snowflake mention", db.Message{Content: "hey <@&role999> check this"}, "99999", []string{"role999"}, true},
	}
	for _, tc := range wakeTests {
		if got := isTier1Wake(tc.msg, tc.botUserID, tc.botRoles); got != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expected, got)
		}
	}

	// 7. Session callbacks, burst execution, & recovery
	getRec, onRot := makeSessionCallbacks(fake)
	_, _ = getRec(context.Background(), "t1")
	_ = onRot(context.Background(), "t1", "old", "new")
	getRecErr, _ := makeSessionCallbacks(&errQueueStore{Store: fake})
	_, _ = getRecErr(context.Background(), "t1")

	_ = unwrapUnifiedPool(nil)
	_ = unwrapUnifiedPool(&testRunnerAgentPool{})

	pTrailNoClass := New(nil, WorkerPoolConfig{
		Store: fake,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	f05 := 0.5
	pTrailNoClass.cfg.Classifier = nil
	pTrailNoClass.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy {
		return config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: &f05}
	}
	_ = insertMessage(fake, db.Message{ID: "m-w-lead", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "<@aerial> start", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	_ = insertMessage(fake, db.Message{ID: "m-trail-query", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "why is the ocean blue", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mwLead, _ := getMessage(fake, "m-w-lead")
	mtQuery, _ := getMessage(fake, "m-trail-query")
	pTrailNoClass.processBurst([]db.Message{*mwLead, *mtQuery})

	_ = insertMessage(fake, db.Message{ID: "m-def-pol", ThreadID: "t-def-pol", AuthorID: "u1", Content: "test prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mDef, _ := getMessage(fake, "m-def-pol")
	pDefPol := New(nil, WorkerPoolConfig{
		Store: fake,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "OK"), "", 0, nil
		},
	})
	pDefPol.cfg.ResolveChannelPolicy = nil
	pDefPol.processBurst([]db.Message{*mDef})

	runIgnored := "run-ignored-extra"
	_ = createScheduleRun(fake, db.ScheduleRun{ID: runIgnored, ScheduleID: "s-ign", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	_ = insertMessage(fake, db.Message{ID: "m-ign-extra", ThreadID: "t-ign-extra", AuthorID: "u1", ScheduleRunID: runIgnored, Content: "ignored prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mIgn, _ := getMessage(fake, "m-ign-extra")
	pIgn := New(nil, WorkerPoolConfig{Store: fake, StalenessTTL: -1})
	pIgn.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy { return config.ChannelPolicy{Mode: "ignore"} }
	pIgn.processBurst([]db.Message{*mIgn})

	// 8. FetchRecentThreadHistory, EnqueueBurst, & RecoverInterrupted
	ctxFetch := context.Background()
	for _, limit := range []int{0, 150} {
		_, _ = FetchRecentThreadHistory(ctxFetch, nil, fake, "", limit)
	}
	_ = insertMessage(fake, db.Message{ID: "msg-fast-1", ThreadID: "thread-fast-1", AuthorID: "user-1", AuthorName: "Alice", Content: "Hello fast path", CreatedAt: time.Now().UTC()})
	_, _ = FetchRecentThreadHistory(ctxFetch, nil, fake, "thread-fast-1", 10)
	_, _ = FetchRecentThreadHistory(ctxFetch, nil, fake, "123456789012345678", 10)

	dgHist, _ := discordgo.New("Bot test")
	dgHist.State = discordgo.NewState()
	dgHist.State.User = &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}
	dgHist.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := json.Marshal([]*discordgo.Message{
			nil,
			{ID: "123456789012345678", Content: "Aerial msg", Timestamp: time.Now().UTC(), Author: &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}},
			{ID: "123456789012345682", Content: "Zero ts", Timestamp: time.Time{}, Author: &discordgo.User{ID: "user-789", Username: "Alice"}},
		})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	emptyStore := setupTestStore(t)
	defer emptyStore.Close()
	_, _ = FetchRecentThreadHistory(ctxFetch, dgHist, emptyStore, "123456789012345678", 50)

	mockClaimErr := &mockClaimStore{Store: fake, claimErr: errors.New("db lock error")}
	pClaimErr := New(nil, WorkerPoolConfig{Store: mockClaimErr})
	pClaimErr.processBurst([]db.Message{{ID: "m-err-claim", ThreadID: "t-claim"}})

	mockUnclaimed := &mockClaimStore{Store: fake, claimed: false}
	pUnclaimed := New(nil, WorkerPoolConfig{Store: mockUnclaimed})
	pUnclaimed.processBurst([]db.Message{{ID: "m-unclaimed", ThreadID: "t-claim"}})

	runStale := "run-stale-burst-1"
	_ = createScheduleRun(fake, db.ScheduleRun{ID: runStale, ScheduleID: "sched-stale", ScheduleType: "cron", StartedAt: time.Now().UTC().Add(-2 * time.Hour), Status: "running"})
	_ = insertMessage(fake, db.Message{ID: "msg-stale-burst-1", ThreadID: "thread-stale-burst", AuthorID: "user-1", ScheduleRunID: runStale, Content: "old prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(-2 * time.Hour)})
	mStale, _ := getMessage(fake, "msg-stale-burst-1")
	pStale := New(nil, WorkerPoolConfig{Store: fake})
	pStale.processBurst([]db.Message{*mStale})

	_ = insertMessage(fake, db.Message{ID: "m-badparse", ThreadID: "t-badparse", AuthorID: "u1", Content: "hello", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mb, _ := getMessage(fake, "m-badparse")
	pBadParse := New(nil, WorkerPoolConfig{
		Store: fake, MaxAttempts: 1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "definitely not json", "", 0, nil
		},
	})
	pBadParse.processBurst([]db.Message{*mb})

	runRate := "run-ratelimit-1"
	_ = createScheduleRun(fake, db.ScheduleRun{ID: runRate, ScheduleID: "s-rate", ScheduleType: "cron", StartedAt: time.Now().UTC(), Status: "running"})
	_ = insertMessage(fake, db.Message{ID: "m-rate-1", ThreadID: "t-rate", AuthorID: "u1", ScheduleRunID: runRate, Content: "query", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	mr, _ := getMessage(fake, "m-rate-1")
	pRate := New(nil, WorkerPoolConfig{
		Store: fake, BackoffBase: 1 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "Resource has been exhausted: rate limit exceeded 429", 1, errors.New("exit 1")
		},
	})
	pRate.processBurst([]db.Message{*mr})

	up404 := NewStatusUpdater(nil, "thread-404", true)
	up404.turnStart = time.Now().Add(10 * time.Second)
	_ = up404.currentStatusText()
	up404.phase = "thinking"; up404.dirty = true; up404.lastDelivered = "old text"; up404.statusMessageID = "msg-edit-404"
	up404.editFunc = func(c, m, txt string) error { return errors.New("HTTP 404 Not Found, 10008 Unknown Message") }
	up404.flush()

	// 9. Extra boundary boosts
	CacheDiscordChannel(nil)
	CacheDiscordChannel(&discordgo.Channel{})
	CacheDiscordChannel(&discordgo.Channel{ID: "parent-ch", GuildID: "guild-parent"})
	CacheDiscordChannel(&discordgo.Channel{ID: "child-th", ParentID: "parent-ch", Type: discordgo.ChannelTypeGuildPublicThread})
	InvalidateChannelCache("parent-ch")
	InvalidateChannelCache("child-th")

	_ = extractHeaderTokens("- mentions: [unclosed", "- mentions: [")
	_ = prependPromptLayer("", "lonely layer")
	uDisFlush := NewStatusUpdater(nil, "thread-dis", false)
	uDisFlush.triggerFlush()

	now := time.Now().UTC()
	_ = FormatChannelHistory([]HistoryMessage{
		{ID: "m1", AuthorName: "User[\n\rName]", Role: "", Content: "Hello world", CreatedAt: now},
		{ID: "m2", AuthorName: "", Role: "Assistant", Content: "Aerial response", CreatedAt: now},
		{ID: "m3", AuthorName: "", Role: "User", Content: "Anon user", CreatedAt: now},
	})

	_, _ = guildMemberFetcher(nil, "g", "u")
	_, _ = guildMemberFetcher(&discordgo.Session{}, "g", "u")
	_, _ = guildMemberFetcher(&discordgo.Session{Token: "test"}, "g", "u")

	fetcher := DefaultHistoryFetcher(dgHist, fake)
	_, _ = fetcher(ctxFetch, "123456789012345678", "non-num", 0)
	_, _ = fetcher(ctxFetch, "123456789012345678", "123456789012345678", 150)

	_ = resolveMessageEffort(db.Message{Effort: "custom"})
	_ = resolveMessageEffort(db.Message{Metadata: db.MessageMetadata{Model: "claude-medium"}})

	pStopped := New(nil, WorkerPoolConfig{})
	pStopped.stopped = true
	pStopped.EnqueueBurst(db.Message{ID: "m-stopped-burst"})

	pFull := New(nil, WorkerPoolConfig{})
	pFull.cancel()
	stF := &threadWorkerState{ch: make(chan db.Message, 1)}
	stF.ch <- db.Message{ID: "m0"}
	pFull.threadChs["t-full"] = stF
	pFull.EnqueueBurst(db.Message{ID: "m-blocked", ThreadID: "t-full"})

	RecoverInterrupted(nil, nil)
	RecoverInterrupted(fake, nil)
	RecoverInterrupted(nil, &WorkerPool{})

	recStore := setupTestStore(t)
	defer recStore.Close()
	_ = recStore.InsertMessage(context.Background(), db.Message{
		ID: "m-rec-stale-run", ThreadID: "t-rec-stale", AuthorID: "user-1",
		ScheduleRunID: "run-stale-rec", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
	})
	_ = recStore.CreateScheduleRun(context.Background(), db.ScheduleRun{
		ID: "run-stale-rec", ScheduleID: "s1", ScheduleType: "cron", StartedAt: time.Now().UTC().Add(-2 * time.Hour), Status: "running",
	})
	_ = recStore.InsertMessage(context.Background(), db.Message{
		ID: "m-rec-poison-retry", ThreadID: "t-rec-poison", AuthorID: "user-1",
		Content: "poison content with a rather long snippet that exceeds sixty characters easily",
		Status: db.StatusProcessing, RetryCount: 5,
	})
	pRec := New(nil, WorkerPoolConfig{
		Store: recStore,
		NotifierFunc: func(b, k, d string) string { return "poison alert" },
		DeliveryFunc: func(s *discordgo.Session, c, t string) error { return nil },
	})
	RecoverInterrupted(recStore, pRec)
}
