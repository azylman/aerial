package queue

import (
	"bufio"
	"bytes"
	"context"
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
		{name: "ForwardCleanPrompt", prompt: "what is the cluster node status?", sessionID: "dev-voice-1", useVoiceHook: true, wantReply: "All three nodes are online.", wantConvID: "dev-voice-1"},

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
						if prompt == "what is the cluster node status?" {
							reply = "All three nodes are online."
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

func TestGetSessionLastActivity(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	_ = insertMessage(fake, db.Message{ID: "m-last-act", ThreadID: "t-last-act", AuthorID: "u1", Status: db.StatusCompleted, CreatedAt: time.Now().UTC()})
	closedStore := setupTestStore(t)
	_ = closedStore.Close()

	tests := []struct {
		name           string
		store          any
		threadID       string
		wantNonZeroTS  bool
		wantIncomplete bool
		wantErr        bool
	}{
		{name: "invalid_store_type", store: "invalid-type", threadID: "t1", wantNonZeroTS: false, wantIncomplete: true, wantErr: false},
		{name: "nil_store_non_empty_thread", store: nil, threadID: "thread-1", wantNonZeroTS: false, wantIncomplete: true, wantErr: false},
		{name: "nil_store_empty_thread", store: nil, threadID: "", wantNonZeroTS: false, wantIncomplete: true, wantErr: false},
		{name: "closed_store_error", store: closedStore, threadID: "thread-1", wantNonZeroTS: false, wantIncomplete: false, wantErr: true},
		{name: "completed_message_found", store: fake, threadID: "t-last-act", wantNonZeroTS: true, wantIncomplete: false, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts, hasIncomplete, err := GetSessionLastActivity(tc.store, tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Errorf("GetSessionLastActivity() err = %v, wantErr=%v", err, tc.wantErr)
			}
			if (!ts.IsZero()) != tc.wantNonZeroTS || hasIncomplete != tc.wantIncomplete {
				t.Errorf("GetSessionLastActivity() = (%v, %v), wantNonZero=%v wantIncomplete=%v", ts, hasIncomplete, tc.wantNonZeroTS, tc.wantIncomplete)
			}
		})
	}
}

func TestTurnExecution_StoreDelegates(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	pFake := &WorkerPool{cfg: WorkerPoolConfig{Store: fake}}
	pErr := &WorkerPool{cfg: WorkerPoolConfig{Store: &errQueueStore{Store: fake}}}

	tests := []struct {
		name      string
		te        *turnExecution
		wantCount int
	}{
		{name: "nil_pool_delegates", te: &turnExecution{}, wantCount: 0},
		{name: "valid_store_delegates", te: &turnExecution{pool: pFake}, wantCount: 1},
		{name: "error_store_delegates", te: &turnExecution{pool: pErr}, wantCount: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			threadID := "t-" + tc.name
			tc.te.updateMessageStatus("m1", "failed", "err")
			tc.te.updateScheduleRunStatus(db.UpdateRunParams{RunID: "r1"})
			tc.te.updateMessageCompleted("m1", "resp")
			tc.te.incrementMessageRetry("m1", "retry-err")
			tc.te.saveSessionID(threadID, "s1")
			tc.te.rotateSessionID(threadID, "s2")
			cnt, err := tc.te.incrementSessionTurnCount(threadID)
			if err != nil || cnt != tc.wantCount {
				t.Errorf("incrementSessionTurnCount() = (%d, %v), want %d", cnt, err, tc.wantCount)
			}
			gotCnt, _ := tc.te.getSessionTurnCount(threadID)
			if gotCnt != tc.wantCount {
				t.Errorf("getSessionTurnCount() = %d, want %d", gotCnt, tc.wantCount)
			}
			_, _ = tc.te.getSessionID(threadID)
			_, _ = tc.te.getPreviousSessionID(threadID)
			_ = tc.te.createOneShotSchedule(db.OneShotSchedule{ID: "os-" + tc.name})
			_ = tc.te.saveThreadSummary(threadID, "sum", "m1")
			_, _, _ = tc.te.getThreadSummary(threadID)
			_, _ = tc.te.getRecentThreadMessages(threadID, 10)
		})
	}
}

func TestSummarizeAndHistoryHelpers(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	now := time.Now().UTC()
	_ = insertMessage(fake, db.Message{ID: "msg-fast-1", ThreadID: "thread-fast-1", AuthorID: "user-1", AuthorName: "Alice", Content: "Hello fast path", CreatedAt: now})

	dgHist, _ := discordgo.New("Bot test")
	dgHist.State = discordgo.NewState()
	dgHist.State.User = &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}
	dgHist.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := json.Marshal([]*discordgo.Message{
			nil,
			{ID: "123456789012345678", Content: "Aerial msg", Timestamp: now, Author: &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}},
			{ID: "123456789012345682", Content: "Zero ts", Timestamp: time.Time{}, Author: &discordgo.User{ID: "user-789", Username: "Alice"}},
		})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	emptyStore := setupTestStore(t)

	tests := []struct {
		name           string
		threadID       string
		beforeID       string
		limit          int
		useEmptyStore  bool
		withLLM        bool
		wantSummarySub string
	}{
		{name: "fast_db_path_with_summary", threadID: "thread-fast-1", beforeID: "msg-role-7", limit: 10, withLLM: true, wantSummarySub: "Summary result"},
		{name: "rest_fallback_snowflake_zero_limit", threadID: "123456789012345678", beforeID: "123456789012345678", limit: 0, useEmptyStore: true, withLLM: false},
		{name: "rest_fallback_clamped_limit", threadID: "123456789012345678", beforeID: "non-num", limit: 150, useEmptyStore: true, withLLM: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			targetStore := fake
			if tc.useEmptyStore {
				targetStore = emptyStore
			}
			var llm runner.LLMFunc
			var msgs []HistoryMessage
			if tc.withLLM {
				llm = func(ctx context.Context, model, prompt string) (string, error) {
					return "<THREAD_SUMMARY>\nSummary result\n</THREAD_SUMMARY>", nil
				}
				msgs = []HistoryMessage{{ID: "m1", Content: "fresh", CreatedAt: now}}
			}
			sumRes, sumErr := SummarizeThreadHistory(ctx, llm, "test-model", tc.threadID, msgs)
			if tc.withLLM && (sumErr != nil || !strings.Contains(sumRes, tc.wantSummarySub)) {
				t.Errorf("SummarizeThreadHistory() = (%q, %v), wantSub=%q", sumRes, sumErr, tc.wantSummarySub)
			}
			if _, err := fetchHistoryFromDB(targetStore, tc.threadID, tc.beforeID, tc.limit); err != nil {
				t.Errorf("fetchHistoryFromDB() unexpected err: %v", err)
			}
			if _, err := FetchRecentThreadHistory(ctx, dgHist, targetStore, tc.threadID, tc.limit); err != nil {
				t.Errorf("FetchRecentThreadHistory() unexpected err: %v", err)
			}
			_, _ = FetchRecentThreadHistory(ctx, nil, fake, "", tc.limit)
			fetcher := DefaultHistoryFetcher(dgHist, targetStore)
			if _, err := fetcher(ctx, tc.threadID, tc.beforeID, tc.limit); err != nil {
				t.Errorf("DefaultHistoryFetcher() unexpected err: %v", err)
			}
			if got := FilterAssistantMessages(nil); len(got) != 0 {
				t.Errorf("expected empty slice from FilterAssistantMessages(nil), got %v", got)
			}
		})
	}
}

func TestWorkerPool_RunnerFallbackAdaptersAndLifecycle(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)

	tests := []struct {
		name    string
		cfg     WorkerPoolConfig
		wantOut string
	}{
		{
			name: "runner_func_synthesizes_with_options",
			cfg: WorkerPoolConfig{
				Model: "custom-model",
				Store: fake,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return "out-from-runner", "", 0, nil
				},
			},
			wantOut: "out-from-runner",
		},
		{
			name: "runner_with_options_synthesizes_runner_func",
			cfg: WorkerPoolConfig{
				Store: fake,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					return "out-from-options", "", 0, nil
				},
			},
			wantOut: "out-from-options",
		},
		{
			name:    "default_mock_runner_synthesis",
			cfg:     WorkerPoolConfig{Store: fake},
			wantOut: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			p := New(nil, tc.cfg)
			out1, _, _, err1 := p.cfg.RunnerFunc(ctx, "bin", "prompt", "sess", "key", "model", 10)
			out2, _, _, err2 := p.cfg.RunnerWithOptionsFunc(ctx, "bin", "prompt", "sess", "key", "model", runner.WatchdogOptions{})
			if tc.wantOut != "" && (err1 != nil || err2 != nil || out1 != tc.wantOut || out2 != tc.wantOut) {
				t.Errorf("adapter outputs = (%q, %q), errs = (%v, %v), want %q", out1, out2, err1, err2, tc.wantOut)
			}
			if p.Classifier() == nil || p.SessionManager() == nil {
				t.Errorf("expected non-nil Classifier and SessionManager")
			}
			var nilPool *WorkerPool
			if err := nilPool.DeliverDirect("c", "t"); err == nil {
				t.Errorf("expected error from nilPool.DeliverDirect")
			}
			if _, err := nilPool.DeliverStatusWidget(discord.StatusWidgetRequest{}); err == nil {
				t.Errorf("expected error from nilPool.DeliverStatusWidget")
			}
			pEmpty := &WorkerPool{}
			if err := pEmpty.DeliverDirect("c", "t"); err == nil {
				t.Errorf("expected error from pEmpty.DeliverDirect")
			}
			if _, err := pEmpty.DeliverStatusWidget(discord.StatusWidgetRequest{}); err == nil {
				t.Errorf("expected error from pEmpty.DeliverStatusWidget")
			}

			getRec, onRot := makeSessionCallbacks(fake)
			_, _ = getRec(ctx, "t1")
			if err := onRot(ctx, "t1", "old", "new"); err != nil {
				t.Errorf("onRot error: %v", err)
			}
			getRecErr, _ := makeSessionCallbacks(&errQueueStore{Store: fake})
			_, _ = getRecErr(ctx, "t1")
			if unwrapUnifiedPool(nil) != nil || unwrapUnifiedPool(&testRunnerAgentPool{}) != nil {
				t.Errorf("expected nil from unwrapUnifiedPool on non-unified pools")
			}
			mockDrain := &mockDrainErrStore{Store: fake, messages: []db.Message{{ID: "m1", ThreadID: "t1", Status: db.StatusProcessing, RestartCount: 5, RetryCount: 5}}}
			pDrain := New(nil, WorkerPoolConfig{Store: mockDrain, DrainTimeout: 10 * time.Millisecond})
			RecoverInterrupted(mockDrain, pDrain)
			RecoverInterrupted(nil, nil)
			RecoverInterrupted(fake, nil)
			RecoverInterrupted(nil, &WorkerPool{})
			pDrain.StopWithTimeout(0)

			recStore := setupTestStore(t)
			_ = recStore.InsertMessage(ctx, db.Message{ID: "m-rec-stale", ThreadID: "t-rec-stale", AuthorID: "u1", ScheduleRunID: "run-stale-rec", Status: db.StatusPending, CreatedAt: time.Now().UTC().Add(-2 * time.Hour)})
			_ = recStore.CreateScheduleRun(ctx, db.ScheduleRun{ID: "run-stale-rec", ScheduleID: "s1", ScheduleType: "cron", StartedAt: time.Now().UTC().Add(-2 * time.Hour), Status: "running"})
			_ = recStore.InsertMessage(ctx, db.Message{ID: "m-rec-poison", ThreadID: "t-rec-poison", AuthorID: "u1", Content: "poison content with a rather long snippet that exceeds sixty characters easily", Status: db.StatusProcessing, RetryCount: 5})
			pRec := New(nil, WorkerPoolConfig{Store: recStore, NotifierFunc: func(b, k, d string) string { return "poison alert" }, DeliveryFunc: func(s *discordgo.Session, c, txt string) error { return nil }})
			RecoverInterrupted(recStore, pRec)

			p.EnqueueBurst()
			p.processBurst(nil)
			p.processBurst([]db.Message{})
			p.cancel()
			stF := &threadWorkerState{ch: make(chan db.Message, 1)}
			stF.ch <- db.Message{ID: "m0"}
			p.threadChs["t-full"] = stF
			p.EnqueueBurst(db.Message{ID: "m-blocked", ThreadID: "t-full"})
			p.StopWithTimeout(10 * time.Millisecond)
			p.EnqueueBurst(db.Message{ID: "m-after-stop"})
		})
	}
}

func TestStatusUpdater_GuardrailsAndRESTTransport(t *testing.T) {
	t.Parallel()
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

	tests := []struct {
		name       string
		tool       string
		arg        string
		dur        time.Duration
		wantSubstr string
	}{
		{name: "long_command_truncated", tool: "run_command", arg: "really_long_command_name_that_exceeds_twenty_four_runes", dur: 1500 * time.Millisecond, wantSubstr: "really_long_command_name"},
		{name: "bash_alias", tool: "bash", arg: "echo hi", dur: 500 * time.Millisecond, wantSubstr: "echo"},
		{name: "execute_command_alias", tool: "execute_command", arg: "ls", dur: 500 * time.Millisecond, wantSubstr: "ls"},
		{name: "long_generic_tool", tool: strings.Repeat("x", 120), arg: "", dur: 500 * time.Millisecond, wantSubstr: "xxxx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatToolStatus(tc.tool, tc.arg, tc.dur); !strings.Contains(got, tc.wantSubstr) {
				t.Errorf("FormatToolStatus(%q, %q) = %q, want substring %q", tc.tool, tc.arg, got, tc.wantSubstr)
			}
			var nilSu *StatusUpdater
			nilSu.MarkTurnStarted()
			nilSu.Stop()
			nilSu.Reset()
			nilSu.DeleteStatusMessage()
			nilSu.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})

			uDis := NewStatusUpdater(nil, "thread-dis", false)
			uDis.Start()
			uDis.triggerFlush()
			uEn := NewStatusUpdater(nil, "thread-en", true, WithStatusInterval(50*time.Millisecond))
			uEn.Start()
			uEn.Stop()

			uStep := NewStatusUpdater(nil, "thread-step", true, WithStatusInterval(10*time.Hour))
			uStep.Stop()
			uStep.disabled = true
			uStep.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
			uStep.disabled = false
			uStep.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
			uStep.HandleStep(&runner.StepUpdateEvent{
				StepType: "tool_call", ToolName: "view_file", State: "ACTIVE",
				ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "ls"}},
			})
			uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "other_tool", State: "DONE"})
			uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "view_file", State: "ERROR"})
			uStep.HandleStep(&runner.StepUpdateEvent{StepType: "agent_response"})

			uREST := NewStatusUpdater(s, "thread-rest-123", true)
			msgID, err := uREST.sendFunc("thread-rest-123", "hello")
			if err != nil || msgID != "msg-created-123" {
				t.Errorf("sendFunc() = (%q, %v), want msg-created-123", msgID, err)
			}
			if err := uREST.editFunc("thread-rest-123", msgID, "edited"); err != nil {
				t.Errorf("editFunc() err = %v", err)
			}
			if err := uREST.deleteFunc("thread-rest-123", msgID); err != nil {
				t.Errorf("deleteFunc() err = %v", err)
			}

			up404 := NewStatusUpdater(nil, "thread-404", true)
			up404.turnStart = time.Now().Add(10 * time.Second)
			_ = up404.currentStatusText()
			up404.phase = "thinking"
			up404.dirty = true
			up404.lastDelivered = "old text"
			up404.statusMessageID = "msg-edit-404"
			up404.editFunc = func(c, m, txt string) error { return errors.New("HTTP 404 Not Found, 10008 Unknown Message") }
			up404.flush()
			if up404.statusMessageID != "" {
				t.Errorf("expected statusMessageID cleared on 404, got %q", up404.statusMessageID)
			}
		})
	}
}

func TestChannelCacheAndMessageEffort(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	dgBot, _ := discordgo.New("Bot dummy")
	dgBot.State.User = &discordgo.User{ID: "bot-1", Username: "AerialBot"}
	guild1 := &discordgo.Guild{
		ID:      "guild-1",
		Roles:   []*discordgo.Role{nil, {ID: "role-aerial", Name: "aerial"}, {ID: "role-gundam", Name: "gundam"}, {ID: "role-username", Name: "AerialBot"}, {ID: "role-other", Name: "moderator"}},
		Members: []*discordgo.Member{{User: &discordgo.User{ID: "bot-1"}, Roles: []string{"role-member-assigned", ""}}},
	}
	_ = dgBot.State.GuildAdd(guild1)

	tests := []struct {
		name       string
		msg        db.Message
		wantEffort string
	}{
		{name: "explicit_custom_effort", msg: db.Message{Effort: "custom"}, wantEffort: "high"},
		{name: "model_derived_medium_effort", msg: db.Message{Metadata: db.MessageMetadata{Model: "claude-medium"}}, wantEffort: "medium"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveMessageEffort(tc.msg); got != tc.wantEffort {
				t.Errorf("resolveMessageEffort() = %q, want %q", got, tc.wantEffort)
			}
			if roles := ResolveBotRoleIDs(dgBot, "", "bot-1"); len(roles) == 0 {
				t.Errorf("expected non-empty roles when guildID is empty")
			}
			if roles := ResolveBotRoleIDs(&discordgo.Session{}, "", ""); roles != nil {
				t.Errorf("expected nil roles for nil State")
			}
			CacheDiscordChannel(nil)
			CacheDiscordChannel(&discordgo.Channel{})
			CacheDiscordChannel(&discordgo.Channel{ID: "parent-" + tc.name, GuildID: "guild-parent"})
			CacheDiscordChannel(&discordgo.Channel{ID: "child-" + tc.name, ParentID: "parent-" + tc.name, Type: discordgo.ChannelTypeGuildPublicThread})
			InvalidateChannelCache("parent-" + tc.name)
			InvalidateChannelCache("child-" + tc.name)

			if tok := extractHeaderTokens("- mentions: [unclosed", "- mentions: ["); tok != nil {
				t.Errorf("expected nil from unclosed extractHeaderTokens, got %v", tok)
			}
			if layer := prependPromptLayer("", "lonely layer"); layer != "lonely layer" {
				t.Errorf("prependPromptLayer() = %q, want 'lonely layer'", layer)
			}
			hist := FormatChannelHistory([]HistoryMessage{
				{ID: "m1", AuthorName: "User[\n\rName]", Role: "", Content: "Hello world", CreatedAt: now},
				{ID: "m2", AuthorName: "", Role: "Assistant", Content: "Aerial response", CreatedAt: now},
				{ID: "m3", AuthorName: "", Role: "User", Content: "Anon user", CreatedAt: now},
			})
			if !strings.Contains(hist, "@UserName (User)") {
				t.Errorf("expected sanitized author name in %q", hist)
			}
		})
	}
}

func TestWorkerPool_ProcessBurstEdgeCases(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	f05 := 0.5

	tests := []struct {
		name          string
		msg           db.Message
		trailing      *db.Message
		run           *db.ScheduleRun
		runnerOut     string
		runnerErrOut  string
		runnerCode    int
		runnerErr     error
		policy        *config.ChannelPolicy
		nilClassifier bool
		claimErr      error
		unclaimed     bool
		wantMsgStatus string
		wantRunStatus string
	}{
		{
			name:      "claim_error_aborts_burst",
			msg:       db.Message{ID: "m-err-claim", ThreadID: "t-claim", Status: db.StatusPending, CreatedAt: now},
			claimErr:  errors.New("db lock error"),
			unclaimed: false,
		},
		{
			name:      "unclaimed_message_skipped",
			msg:       db.Message{ID: "m-unclaimed", ThreadID: "t-unclaimed", Status: db.StatusPending, CreatedAt: now},
			unclaimed: true,
		},
		{
			name:          "stale_schedule_run_expired",
			msg:           db.Message{ID: "msg-stale-burst-1", ThreadID: "thread-stale-burst", AuthorID: "user-1", ScheduleRunID: "run-stale-burst-1", Content: "old prompt", Status: db.StatusPending, CreatedAt: now.Add(-2 * time.Hour)},
			run:           &db.ScheduleRun{ID: "run-stale-burst-1", ScheduleID: "sched-stale", ScheduleType: "cron", StartedAt: now.Add(-2 * time.Hour), Status: "running"},
			wantMsgStatus: db.StatusCompleted,
			wantRunStatus: "completed",
		},
		{
			name:          "ignored_channel_completes_schedule_run",
			msg:           db.Message{ID: "m-ign-extra", ThreadID: "t-ign-extra", AuthorID: "u1", ScheduleRunID: "run-ignored-extra", Content: "ignored prompt", Status: db.StatusPending, CreatedAt: now},
			run:           &db.ScheduleRun{ID: "run-ignored-extra", ScheduleID: "s-ign", ScheduleType: "cron", StartedAt: now, Status: "running"},
			policy:        &config.ChannelPolicy{Mode: "ignore"},
			wantMsgStatus: db.StatusCompleted,
			wantRunStatus: "completed",
		},
		{
			name:          "bad_json_output_fails_after_attempt",
			msg:           db.Message{ID: "m-badparse", ThreadID: "t-badparse", AuthorID: "u1", Content: "hello", Status: db.StatusPending, CreatedAt: now},
			runnerOut:     "definitely not json",
			wantMsgStatus: db.StatusFailed,
		},
		{
			name:          "rate_limit_fails_schedule_run",
			msg:           db.Message{ID: "m-rate-1", ThreadID: "t-rate", AuthorID: "u1", ScheduleRunID: "run-ratelimit-1", Content: "query", Status: db.StatusPending, CreatedAt: now},
			run:           &db.ScheduleRun{ID: "run-ratelimit-1", ScheduleID: "s-rate", ScheduleType: "cron", StartedAt: now, Status: "running"},
			runnerErrOut:  "Resource has been exhausted: rate limit exceeded 429",
			runnerCode:    1,
			runnerErr:     errors.New("exit 1"),
			wantMsgStatus: db.StatusFailed,
			wantRunStatus: "failed",
		},
		{
			name:          "trailing_query_with_nil_classifier_and_default_policy",
			msg:           db.Message{ID: "m-w-lead", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "<@aerial> start", Status: db.StatusPending, CreatedAt: now},
			trailing:      &db.Message{ID: "m-trail-query", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "why is the ocean blue", Status: db.StatusPending, CreatedAt: now},
			runnerOut:     mockJSONResponse("", "OK"),
			policy:        &config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: &f05},
			nilClassifier: true,
			wantMsgStatus: db.StatusCompleted,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := setupTestStore(t)
			if tc.run != nil {
				_ = createScheduleRun(fake, *tc.run)
			}
			_ = insertMessage(fake, tc.msg)
			burst := []db.Message{tc.msg}
			if tc.trailing != nil {
				_ = insertMessage(fake, *tc.trailing)
				burst = append(burst, *tc.trailing)
			}
			var store db.Store = fake
			if tc.claimErr != nil || tc.unclaimed {
				store = &mockClaimStore{Store: fake, claimed: !tc.unclaimed, claimErr: tc.claimErr}
			}
			pool := New(nil, WorkerPoolConfig{
				Store:       store,
				MaxAttempts: 1,
				BackoffBase: 1 * time.Millisecond,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.runnerOut, tc.runnerErrOut, tc.runnerCode, tc.runnerErr
				},
			})
			if tc.nilClassifier {
				pool.cfg.Classifier = nil
			}
			if tc.policy != nil {
				pol := *tc.policy
				pool.cfg.ResolveChannelPolicy = func(c, n string) config.ChannelPolicy { return pol }
			} else {
				pool.cfg.ResolveChannelPolicy = nil
			}
			pool.processBurst(burst)

			if tc.wantMsgStatus != "" {
				gotMsg, err := getMessage(fake, tc.msg.ID)
				if err != nil || gotMsg == nil || gotMsg.Status != tc.wantMsgStatus {
					t.Errorf("message %s status = %v (err=%v), want %s", tc.msg.ID, gotMsg, err, tc.wantMsgStatus)
				}
			}
			if tc.wantRunStatus != "" && tc.run != nil {
				gotRun, err := getScheduleRun(fake, tc.run.ID)
				if err != nil || gotRun == nil || gotRun.Status != tc.wantRunStatus {
					t.Errorf("run %s status = %v (err=%v), want %s", tc.run.ID, gotRun, err, tc.wantRunStatus)
				}
			}
		})
	}
}
