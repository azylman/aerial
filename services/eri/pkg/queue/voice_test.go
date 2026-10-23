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

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
)

func TestWorkerPool_EffortModelResolution(t *testing.T) {
	t.Parallel()
	defaultLow := config.DefaultConfigData().LowEffortModel
	appLow, appEmpty := "app-cfg-low-model", ""

	tests := []struct {
		name, cfgModel string
		poolNil        bool
		appCfgVal      *string
		wantResult     string
	}{
		{"Low_NilPoolFallback", "", true, nil, defaultLow},
		{"Low_CustomModelInCfg", "gemini-flash-low-test", false, nil, "gemini-flash-low-test"},
		{"Low_AppCfgOverride", "", false, &appLow, "app-cfg-low-model"},
		{"Low_AppCfgEmptyWithCfgFallback", "cfg-fallback-model", false, &appEmpty, "cfg-fallback-model"},
		{"Low_AppCfgEmptyAndCfgEmptyDefaults", "", false, &appEmpty, defaultLow},
		{"Low_AppCfgNilAndCfgEmptyDefaults", "", false, nil, defaultLow},
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
				appCfg = newTestConfig(func(d *config.ConfigData) {
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

type voiceTurnTestCase struct {
	name, prompt, sessionID, runnerOut, runnerErrOut, wantReply, wantConvID, wantErrSub, wantStatusSub, wantSentenceSub string
	runnerCode, resultExitCode                                                                                          int
	runnerErr, spawnerErr, wantErrIs, hookErr                                                                           error
	poolNil, poolStopped, poolDeadCtx, cancelCaller, nilProcessPool, useVoiceHook, useVoiceStreamHook                   bool
	closePipeImmediately, cancelCallerOnScan, clearPreSpawnedSessionID                                                  bool
	resultStatus, resultError                                                                                           string
}

func setupVoiceTestPool(t *testing.T, tt *voiceTurnTestCase, cancelCaller context.CancelFunc, onRecordPrompt func(string)) (*WorkerPool, *runner.UnifiedProcessPool, bool) {
	if tt.poolNil {
		return nil, nil, false
	}
	if tt.poolDeadCtx {
		deadCtx, deadCancel := context.WithCancel(context.Background())
		deadCancel()
		return &WorkerPool{ctx: deadCtx}, nil, false
	}
	if tt.poolStopped {
		return &WorkerPool{stopped: true}, nil, false
	}
	if tt.nilProcessPool {
		p := New(nil, WorkerPoolConfig{})
		p.processPool = nil
		return p, nil, false
	}
	if tt.useVoiceHook {
		p := New(nil, WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				onRecordPrompt(prompt)
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
		p.Start()
		return p, nil, true
	}
	if tt.useVoiceStreamHook {
		p := New(nil, WorkerPoolConfig{
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
		p.Start()
		return p, nil, true
	}
	if tt.runnerOut != "" || tt.runnerErr != nil || tt.runnerCode != 0 {
		p := New(nil, WorkerPoolConfig{
			LowEffortModel: "low-effort-fast-model", Store: dbtest.NewFakeStore(),
			RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return tt.runnerOut, tt.runnerErrOut, tt.runnerCode, tt.runnerErr
			},
		})
		p.Start()
		return p, nil, true
	}
	var blockCh chan struct{}
	var onScan func()
	if tt.cancelCallerOnScan {
		blockCh = make(chan struct{})
		t.Cleanup(func() { close(blockCh) })
		onScan = func() { cancelCaller() }
	}
	spawner := newEventDrivenVoiceDaemonSpawner(voiceProcessPoolSpawnerConfig{
		spawnerErr: tt.spawnerErr, closeInRPipeImmediately: tt.closePipeImmediately,
		resultStatus: tt.resultStatus, resultError: tt.resultError, resultExitCode: tt.resultExitCode,
		cancelCallerOnScan: onScan, blockUntilDone: blockCh,
	})
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
	p := New(nil, WorkerPoolConfig{ProcessPool: procPool})
	p.Start()
	return p, procPool, true
}

func TestWorkerPool_ExecuteVoiceTurn_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []voiceTurnTestCase{
		{name: "emptySession", prompt: "hello", sessionID: "", wantErrSub: "session_id is required"},
		{name: "whitespaceSession", prompt: "hello", sessionID: "   ", wantErrSub: "session_id is required"},
		{name: "canceledCallerCtx", prompt: "hello", sessionID: "sess-cancel", cancelCaller: true, wantErrIs: context.Canceled},
		{name: "deadPoolCtx", prompt: "hello", sessionID: "sess-dead", poolDeadCtx: true, wantErrSub: "brain is shutting down"},
		{name: "nilPool", prompt: "hello", sessionID: "sess-nil", poolNil: true, wantErrSub: "worker pool is uninitialized"},
		{name: "stoppedPool", prompt: "hello", sessionID: "sess-stopped", poolStopped: true, wantErrSub: "brain is shutting down"},
		{name: "nilProcessPool", prompt: "hello", sessionID: "sess-1", nilProcessPool: true, wantErrSub: "voice daemon pool unavailable"},
		{name: "SuccessTurn", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: `{"event":"result","result":{"status":"SUCCESS","response":"turned off fan"}}`, wantReply: "turned off fan", wantConvID: "sess-custom-1", wantStatusSub: "⚡"},
		{name: "RunnerError", prompt: "turn off fan", sessionID: "sess-custom-1", runnerErrOut: "exec error", runnerCode: 1, runnerErr: errors.New("exec failed"), wantErrSub: "exec failed"},
		{name: "ExitCodeNonZero", prompt: "turn off fan", sessionID: "sess-custom-1", runnerErrOut: "process died", runnerCode: 42, wantErrSub: "exit code 42"},
		{name: "PlainTextFallback", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: "plain text response", wantReply: "plain text response", wantConvID: "sess-custom-1"},
		{name: "EmptyResponseField", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: `{"event":"result","result":{"status":"SUCCESS","response":""}}`, wantErrSub: "empty response"},
		{name: "VoiceRunnerHook", prompt: "hello", sessionID: "sess-hook-1", useVoiceHook: true, wantReply: "voice hook reply", wantConvID: "sess-hook-1", wantStatusSub: "⚡ Custom hook"},
		{name: "VoiceStreamRunnerHook", prompt: "hello", sessionID: "sess-stream-1", useVoiceStreamHook: true, wantReply: "voice stream reply", wantConvID: "sess-stream-1", wantStatusSub: "⚡ Streaming hook", wantSentenceSub: "Streaming sentence."},
		{name: "VoiceStreamRunnerHookError", prompt: "hello", sessionID: "sess-err", useVoiceStreamHook: true, hookErr: errors.New("simulated stream failure"), wantErrSub: "simulated stream failure"},
		{name: "ForwardCleanPrompt", prompt: "what is the cluster node status?", sessionID: "dev-voice-1", useVoiceHook: true, wantReply: "All three nodes are online.", wantConvID: "dev-voice-1"},
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

			var recordedPrompt, statusReceived, sentenceReceived string
			pool, procPool, shouldStop := setupVoiceTestPool(t, &tt, cancel, func(p string) { recordedPrompt = p })
			if shouldStop && pool != nil {
				defer pool.Stop()
			}
			if procPool != nil {
				defer procPool.Close()
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

func newEventDrivenVoiceDaemonSpawner(cfg voiceProcessPoolSpawnerConfig) *mockDaemonSpawner {
	return &mockDaemonSpawner{
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
					_ = os.WriteFile(filepath.Join(cfg.writeTranscriptLogsDir, "transcript.jsonl"), []byte(transcript), 0o644)
				}
				status := cfg.resultStatus
				if status == "" {
					status = "SUCCESS"
				}
				exitPart, usagePart := "", ""
				if cfg.resultExitCode != 0 {
					exitPart = fmt.Sprintf(",\"exit_code\":%d", cfg.resultExitCode)
				}
				if cfg.tokens.TotalTokens > 0 {
					usagePart = fmt.Sprintf(",\"usage\":{\"input_tokens\":%d,\"output_tokens\":%d,\"total_tokens\":%d}", cfg.tokens.InputTokens, cfg.tokens.OutputTokens, cfg.tokens.TotalTokens)
				}
				_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":%q,\"response\":%q,\"error\":%q,\"conversation_id\":%q%s%s}}\n", status, cfg.resultResponse, cfg.resultError, sessID, exitPart, usagePart)))
			}()
			return inW, outR, errR, newMockProcessHandle(12345), nil
		},
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPoolSuccessAndMetrics(t *testing.T) {
	t.Parallel()
	stepUpdates := []string{
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","tool_name":"ha_call_service","tool_info":{"parameters":{"CommandLine":""}}}}`,
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool","tool_name":"","tool_info":{"parameters":{"CommandLine":"system-health"}}}}`,
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","tool_name":"ha_call_service"}}`,
		`{"event":"step_update","step_update":{"state":"ERROR","type":"tool_call","tool_name":"ha_call_service"}}`,
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
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "turn on living room light", "kiosk", func(s string) { statuses = append(statuses, s) }, nil)
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
	spawner := &mockDaemonSpawner{
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
			return inW, outR, errR, newMockProcessHandle(8888), nil
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
	targetSess, tempDir := "550e8400-e29b-41d4-a716-446655449999", t.TempDir()
	logsDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", targetSess, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0o755)
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
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "What is the status?", targetSess, nil, func(s string) { sentences = append(sentences, s) })
	if err != nil || reply != "All systems operational and nominal." || convID != targetSess || len(sentences) == 0 {
		t.Fatalf("unexpected transcript recovery result: reply=%q convID=%q sentences=%v err=%v", reply, convID, sentences, err)
	}
}

func TestWorkerPool_Stop_ProcessPoolCloseError(t *testing.T) {
	t.Parallel()
	mockHandle := newMockProcessHandle(99999)
	mockHandle.SetKillErr(errors.New("simulated kill process failure"))
	spawner := &mockDaemonSpawner{
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

func (p *queueMockVoiceProcessPool) Initialize(ctx context.Context) error {
	p.initCalled = true
	return nil
}

func (p *queueMockVoiceProcessPool) Close() error {
	p.closeCalled = true
	return nil
}
func (p *queueMockVoiceProcessPool) EvictSession(targetKey string) { delete(p.sessions, targetKey) }

func (p *queueMockVoiceProcessPool) AcquireLease(ctx context.Context, targetKey string) (runner.SessionLease, error) {
	if p.closeCalled {
		return nil, errors.New("pool closed")
	}
	sess, err := p.GetOrCreateSession(ctx, targetKey, "")
	if err != nil {
		return nil, err
	}
	return &mockVoiceSessionLease{pool: p, sess: sess.(*queueMockVoiceSession), targetKey: targetKey}, nil
}

type mockVoiceSessionLease struct {
	pool      *queueMockVoiceProcessPool
	sess      *queueMockVoiceSession
	targetKey string
}

func (l *mockVoiceSessionLease) SessionID() string         { return l.sess.SessionID() }
func (l *mockVoiceSessionLease) PreviousSessionID() string { return "" }
func (l *mockVoiceSessionLease) IsCold() bool              { return false }
func (l *mockVoiceSessionLease) IsEphemeral() bool         { return false }
func (l *mockVoiceSessionLease) TurnCount() int            { return 1 }
func (l *mockVoiceSessionLease) MarkDirty()                { l.pool.EvictSession(l.targetKey) }
func (l *mockVoiceSessionLease) Release() error            { return nil }
func (l *mockVoiceSessionLease) Execute(ctx context.Context, turn *runner.TurnContext) (*runner.TurnResult, error) {
	if err := l.sess.Send(turn.Prompt, turn); err != nil {
		return nil, err
	}
	if turn != nil && turn.Sink != nil {
		if buf, ok := turn.Sink.(*runner.BufferingTurnSink); ok {
			return buf.Wait(ctx)
		}
	}
	return &runner.TurnResult{ConversationID: l.SessionID(), Response: "Hello from mock voice."}, nil
}

var (
	_ runner.AgentPool    = (*queueMockVoiceProcessPool)(nil)
	_ runner.AgentSession = (*queueMockVoiceSession)(nil)
)

func TestWorkerPool_ExecuteVoiceTurn_MockVoiceProcessPool(t *testing.T) {
	t.Parallel()
	mockPool := &queueMockVoiceProcessPool{sessions: make(map[string]*queueMockVoiceSession)}
	pool := New(nil, WorkerPoolConfig{VoiceProcessPool: mockPool})
	pool.Start()
	defer pool.Stop()

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
		store          db.Store
		threadID       string
		wantNonZeroTS  bool
		wantIncomplete bool
		wantErr        bool
	}{
		{name: "nil_store_non_empty_thread", store: nil, threadID: "thread-1", wantIncomplete: true},
		{name: "nil_store_empty_thread", store: nil, threadID: "", wantIncomplete: true},
		{name: "closed_store_error", store: closedStore, threadID: "thread-1", wantErr: true},
		{name: "completed_message_found", store: fake, threadID: "t-last-act", wantNonZeroTS: true},
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
	closedFake := setupTestStore(t)
	_ = closedFake.Close()
	pFake := &WorkerPool{cfg: WorkerPoolConfig{Store: fake}}
	pErr := &WorkerPool{cfg: WorkerPoolConfig{Store: &errQueueStore{Store: closedFake}}}

	tests := []struct {
		name      string
		te        *turnExecution
		wantCount int
		wantErr   bool
	}{
		{name: "nil_pool_delegates", te: &turnExecution{}, wantCount: 0},
		{name: "valid_store_delegates", te: &turnExecution{pool: pFake}, wantCount: 1},
		{name: "error_store_delegates", te: &turnExecution{pool: pErr}, wantCount: 0, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			threadID := "t-" + tc.name
			tc.te.updateMessageStatus("m1", "failed", "err")
			tc.te.updateMessageCompleted("m1", "resp")
			tc.te.incrementMessageRetry("m1", "retry-err")
			tc.te.saveSessionID(threadID, "s1")
			tc.te.rotateSessionID(threadID, "s2")
			cnt, err := tc.te.incrementSessionTurnCount(threadID)
			if (err != nil) != tc.wantErr || cnt != tc.wantCount {
				t.Errorf("incrementSessionTurnCount() = (%d, %v), want (%d, err=%v)", cnt, err, tc.wantCount, tc.wantErr)
			}
			_, _ = tc.te.getSessionID(threadID)
			_ = tc.te.getPreviousSessionID(threadID)
			_ = tc.te.createOneShotSchedule(db.OneShotSchedule{ID: "os-" + tc.name})
			tc.te.saveThreadSummary(threadID, "sum", "m1")
			_, _ = tc.te.getThreadSummary(threadID)
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
		name, threadID, beforeID, wantSummarySub string
		limit                                    int
		useEmptyStore, withLLM                   bool
	}{
		{name: "fast_db_summary", threadID: "thread-fast-1", beforeID: "msg-role-7", limit: 10, withLLM: true, wantSummarySub: "Summary result"},
		{name: "rest_snowflake_zero_limit", threadID: "123456789012345678", beforeID: "123456789012345678", limit: 0, useEmptyStore: true},
		{name: "rest_clamped_limit", threadID: "123456789012345678", beforeID: "non-num", limit: 150, useEmptyStore: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			targetStore := fake
			if tc.useEmptyStore {
				targetStore = emptyStore
			}
			if tc.withLLM {
				llm := func(ctx context.Context, model, prompt string) (string, error) {
					return "<THREAD_SUMMARY>\nSummary result\n</THREAD_SUMMARY>", nil
				}
				msgs := []HistoryMessage{{ID: "m1", Content: "fresh", CreatedAt: now}}
				sumRes, sumErr := SummarizeThreadHistoryWithGroup(ctx, nil, llm, "test-model", tc.threadID, msgs)
				if sumErr != nil || !strings.Contains(sumRes, tc.wantSummarySub) {
					t.Errorf("SummarizeThreadHistoryWithGroup() = (%q, %v), wantSub=%q", sumRes, sumErr, tc.wantSummarySub)
				}
			}
			_, _ = fetchHistoryFromDB(targetStore, tc.threadID, tc.beforeID, tc.limit)
			_, _ = FetchRecentThreadHistory(ctx, dgHist, targetStore, tc.threadID, tc.limit)
			_, _ = FetchRecentThreadHistory(ctx, nil, fake, "", tc.limit)
			fetcher := DefaultHistoryFetcher(dgHist, targetStore)
			_, _ = fetcher(ctx, tc.threadID, tc.beforeID, tc.limit)
			if got := FilterAssistantMessages(nil); len(got) != 0 {
				t.Errorf("expected empty slice from FilterAssistantMessages(nil)")
			}
		})
	}

	teNil := &turnExecution{pool: &WorkerPool{}}
	_, _ = teNil.resolveWebhookDispatcher()
	_ = teNil.resolveTurnPolicy()
	teTrail := &turnExecution{
		pool:          &WorkerPool{cfg: WorkerPoolConfig{Store: fake, OnMessageCompleted: func(db.Message, string) {}}},
		isQuotaPaused: true, trailingMsgs: []db.Message{{ID: "m-tr1", ScheduleRunID: "run-tr1", Content: "tr1"}, {ID: "m-tr2", Content: "tr2"}},
		trailingInfos: []WakeInfo{{IsWake: true}, {IsWake: false}},
	}
	teTrail.handleTrailing()
	teSess := &turnExecution{threadID: "t-pers", pool: &WorkerPool{cfg: WorkerPoolConfig{Store: fake}}}
	_ = teSess.resolveAndPersistSessionID("sess-1", "", "", true)
	_ = teSess.resolveAndPersistSessionID("sess-1", "", "", false)
	teSess.isEphemeral = true
	_ = teSess.resolveAndPersistSessionID("sess-2", "", "", true)
	teFail := &turnExecution{threadID: "t-fail", pool: &WorkerPool{cfg: WorkerPoolConfig{NotifierFunc: func(b, k, d string) string { return d }, DeliveryFunc: func(s *discordgo.Session, c, txt string) error { return errors.New("err") }}}}
	teFail.deliverFailureNotice("bin", "key", "err "+strings.Repeat("x", 400), "rate limit 429", "prefix: ", "lbl")
	teFail.deliverFailureNotice("bin", "key", "simple", "", "", "lbl")
}

func TestWorkerPool_RunnerFallbackAdaptersAndLifecycle(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		cfg     WorkerPoolConfig
		wantOut string
	}{
		{
			name: "runner_func_synthesizes_with_options",
			cfg: WorkerPoolConfig{
				Model: "custom-model", Store: fake,
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
		{name: "default_mock_runner_synthesis", cfg: WorkerPoolConfig{Store: fake}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := New(nil, tc.cfg)
			if tc.wantOut != "" {
				lease, err := p.AgentPool().AcquireLease(ctx, "t-adapter")
				if err != nil {
					t.Fatalf("AcquireLease failed: %v", err)
				}
				res, execErr := lease.Execute(ctx, &runner.TurnContext{Prompt: "prompt", Ctx: ctx})
				_ = lease.Release()
				if execErr != nil || res == nil || res.Response != tc.wantOut {
					t.Errorf("adapter output = %v, err = %v, want %q", res, execErr, tc.wantOut)
				}
			}
			if p.Classifier() == nil {
				t.Errorf("expected non-nil Classifier")
			}
		})
	}

	var nilPool *WorkerPool
	_ = nilPool.DeliverDirect("c", "t")
	_, _ = nilPool.DeliverStatusWidget(discord.StatusWidgetRequest{})
	pEmpty := &WorkerPool{}
	_ = pEmpty.DeliverDirect("c", "t")
	_, _ = pEmpty.DeliverStatusWidget(discord.StatusWidgetRequest{})

	getRec, onRot := makeSessionCallbacks(fake)
	_, _ = getRec(ctx, "t1")
	if err := onRot(ctx, "t1", "old", "new"); err != nil {
		t.Errorf("onRot error: %v", err)
	}
	getRecErr, _ := makeSessionCallbacks(&errQueueStore{Store: fake})
	_, _ = getRecErr(ctx, "t1")
	if unwrapUnifiedPool(nil) != nil || unwrapUnifiedPool(&testRunnerAgentPool{}) != nil {
		t.Errorf("expected nil from unwrapUnifiedPool")
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
	recStore.SeedScheduleRun(db.ScheduleRun{ID: "run-stale-rec", ScheduleID: "s1", ScheduleType: "cron", StartedAt: time.Now().UTC().Add(-2 * time.Hour), Status: "running"})
	_ = recStore.InsertMessage(ctx, db.Message{ID: "m-rec-poison", ThreadID: "t-rec-poison", AuthorID: "u1", Content: "poison content with a rather long snippet that exceeds sixty characters easily", Status: db.StatusProcessing, RetryCount: 5})
	pRec := New(nil, WorkerPoolConfig{Store: recStore, NotifierFunc: func(b, k, d string) string { return "poison alert" }, DeliveryFunc: func(s *discordgo.Session, c, txt string) error { return nil }})
	RecoverInterrupted(recStore, pRec)

	p := New(nil, WorkerPoolConfig{Store: fake})
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
		name, tool, arg, wantSubstr string
		dur                         time.Duration
	}{
		{"long_cmd", "run_command", "really_long_command_name_that_exceeds_twenty_four_runes", "really_long_command_name", 1500 * time.Millisecond},
		{"bash", "bash", "echo hi", "echo", 500 * time.Millisecond},
		{"exec", "execute_command", "ls", "ls", 500 * time.Millisecond},
		{"generic", strings.Repeat("x", 120), "", "xxxx", 500 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatToolStatus(tc.tool, tc.arg, tc.dur); !strings.Contains(got, tc.wantSubstr) {
				t.Errorf("FormatToolStatus(%q, %q) = %q, want substring %q", tc.tool, tc.arg, got, tc.wantSubstr)
			}
		})
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
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "view_file", State: "ACTIVE", ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "ls"}}})
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
	up404.phase, up404.dirty, up404.lastDelivered, up404.statusMessageID = "thinking", true, "old text", "msg-edit-404"
	up404.editFunc = func(c, m, txt string) error { return errors.New("HTTP 404 Not Found, 10008 Unknown Message") }
	up404.flush()
	if up404.statusMessageID != "" {
		t.Errorf("expected statusMessageID cleared on 404, got %q", up404.statusMessageID)
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
		{"explicit_custom_effort", db.Message{Effort: "custom"}, "high"},
		{"model_derived_medium_effort", db.Message{Metadata: db.MessageMetadata{Model: "claude-medium"}}, "medium"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveMessageEffort(tc.msg); got != tc.wantEffort {
				t.Errorf("resolveMessageEffort() = %q, want %q", got, tc.wantEffort)
			}
		})
	}

	if roles := ResolveBotRoleIDs(dgBot, "", "bot-1"); len(roles) == 0 {
		t.Errorf("expected non-empty roles")
	}
	if roles := ResolveBotRoleIDs(&discordgo.Session{}, "", ""); roles != nil {
		t.Errorf("expected nil roles for nil State")
	}
	CacheDiscordChannel(nil)
	CacheDiscordChannel(&discordgo.Channel{})
	CacheDiscordChannel(&discordgo.Channel{ID: "parent-ch", GuildID: "guild-parent"})
	CacheDiscordChannel(&discordgo.Channel{ID: "child-ch", ParentID: "parent-ch", Type: discordgo.ChannelTypeGuildPublicThread})
	InvalidateChannelCache("parent-ch")
	InvalidateChannelCache("child-ch")

	if tok := extractHeaderTokens("- mentions: [unclosed", "- mentions: ["); tok != nil {
		t.Errorf("expected nil from unclosed extractHeaderTokens")
	}
	if layer := prependPromptLayer("", "lonely layer"); layer != "lonely layer" {
		t.Errorf("prependPromptLayer() = %q", layer)
	}
	hist := FormatChannelHistory([]HistoryMessage{
		{ID: "m1", AuthorName: "User[\n\rName]", Role: "", Content: "Hello world", CreatedAt: now},
		{ID: "m2", AuthorName: "", Role: "Assistant", Content: "Aerial response", CreatedAt: now},
		{ID: "m3", AuthorName: "", Role: "User", Content: "Anon user", CreatedAt: now},
	})
	if !strings.Contains(hist, "@UserName (User)") {
		t.Errorf("expected sanitized author name in %q", hist)
	}
}

func TestWorkerPool_ProcessBurstEdgeCases(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	f05 := 0.5

	tests := []struct {
		name, runnerOut, runnerErrOut, wantMsgStatus, wantRunStatus string
		msg                                                         db.Message
		trailing                                                    *db.Message
		run                                                         *db.ScheduleRun
		runnerCode                                                  int
		runnerErr, claimErr                                         error
		policy                                                      *config.ChannelPolicy
		nilClassifier, unclaimed                                    bool
	}{
		{name: "claim_error_aborts_burst", msg: db.Message{ID: "m-err-claim", ThreadID: "t-claim", Status: db.StatusPending, CreatedAt: now}, claimErr: errors.New("db lock error")},
		{name: "unclaimed_message_skipped", msg: db.Message{ID: "m-unclaimed", ThreadID: "t-unclaimed", Status: db.StatusPending, CreatedAt: now}, unclaimed: true},
		{name: "stale_schedule_run_expired", msg: db.Message{ID: "msg-stale-burst-1", ThreadID: "thread-stale-burst", AuthorID: "user-1", ScheduleRunID: "run-stale-burst-1", Content: "old prompt", Status: db.StatusPending, CreatedAt: now.Add(-2 * time.Hour)}, run: &db.ScheduleRun{ID: "run-stale-burst-1", ScheduleID: "sched-stale", ScheduleType: "cron", StartedAt: now.Add(-2 * time.Hour), Status: "running"}, wantMsgStatus: db.StatusCompleted, wantRunStatus: "completed"},
		{name: "ignored_channel_completes_schedule_run", msg: db.Message{ID: "m-ign-extra", ThreadID: "t-ign-extra", AuthorID: "u1", ScheduleRunID: "run-ignored-extra", Content: "ignored prompt", Status: db.StatusPending, CreatedAt: now}, run: &db.ScheduleRun{ID: "run-ignored-extra", ScheduleID: "s-ign", ScheduleType: "cron", StartedAt: now, Status: "running"}, policy: &config.ChannelPolicy{Mode: "ignore"}, wantMsgStatus: db.StatusCompleted, wantRunStatus: "completed"},
		{name: "bad_json_output_fails_after_attempt", msg: db.Message{ID: "m-badparse", ThreadID: "t-badparse", AuthorID: "u1", Content: "hello", Status: db.StatusPending, CreatedAt: now}, runnerOut: "definitely not json", wantMsgStatus: db.StatusFailed},
		{name: "rate_limit_fails_schedule_run", msg: db.Message{ID: "m-rate-1", ThreadID: "t-rate", AuthorID: "u1", ScheduleRunID: "run-ratelimit-1", Content: "query", Status: db.StatusPending, CreatedAt: now}, run: &db.ScheduleRun{ID: "run-ratelimit-1", ScheduleID: "s-rate", ScheduleType: "cron", StartedAt: now, Status: "running"}, runnerErrOut: "Resource has been exhausted: rate limit exceeded 429", runnerCode: 1, runnerErr: errors.New("exit 1"), wantMsgStatus: db.StatusFailed, wantRunStatus: "failed"},
		{name: "trailing_query_with_nil_classifier_and_default_policy", msg: db.Message{ID: "m-w-lead", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "<@aerial> start", Status: db.StatusPending, CreatedAt: now}, trailing: &db.Message{ID: "m-trail-query", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "why is the ocean blue", Status: db.StatusPending, CreatedAt: now}, runnerOut: mockJSONResponse("", "OK"), policy: &config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: &f05}, nilClassifier: true, wantMsgStatus: db.StatusCompleted},
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
				Store: store, MaxAttempts: 1, BackoffBase: time.Millisecond,
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
			}
			pool.processBurst(burst)

			if tc.wantMsgStatus != "" {
				if gotMsg, err := getMessage(fake, tc.msg.ID); err != nil || gotMsg == nil || gotMsg.Status != tc.wantMsgStatus {
					t.Errorf("msg %s status = %v (err=%v), want %s", tc.msg.ID, gotMsg, err, tc.wantMsgStatus)
				}
			}
		})
	}
}
