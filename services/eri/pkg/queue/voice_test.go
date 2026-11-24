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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

type mockVoicePoolWithModel struct {
	runner.AgentPool

	model string
}

func (m *mockVoicePoolWithModel) Model() string {
	if m == nil {
		return ""
	}
	return m.model
}

type effortModelResolutionCase struct {
	name, cfgLow, cfgMed string
	poolNil              bool
	appLowVal, appMedVal *string
	appVoiceVal          *string
	voicePoolModel       string
	wantLow, wantMed     string
	wantVoice            string
}

func runEffortModelResolutionCase(t *testing.T, tt effortModelResolutionCase) {
	t.Helper()
	if tt.poolNil {
		var nilPool *WorkerPool
		if got := nilPool.LowEffortModel(); got != tt.wantLow {
			t.Errorf("LowEffortModel() = %q, want %q", got, tt.wantLow)
		}
		if got := nilPool.MediumEffortModel(); got != tt.wantMed {
			t.Errorf("MediumEffortModel() = %q, want %q", got, tt.wantMed)
		}
		if got := nilPool.VoiceModel(); got != tt.wantVoice {
			t.Errorf("VoiceModel() = %q, want %q", got, tt.wantVoice)
		}
		return
	}
	var appCfg *config.Config
	if tt.appLowVal != nil || tt.appMedVal != nil || tt.appVoiceVal != nil {
		appCfg = newTestConfig(func(d *config.ConfigData) {
			if tt.appLowVal != nil {
				d.LowEffortModel = *tt.appLowVal
			}
			if tt.appMedVal != nil {
				d.MediumEffortModel = *tt.appMedVal
			}
			if tt.appVoiceVal != nil {
				d.Voice.Model = *tt.appVoiceVal
			}
		})
	}
	cfg := WorkerPoolConfig{LowEffortModel: tt.cfgLow, MediumEffortModel: tt.cfgMed}
	if tt.voicePoolModel != "" {
		cfg.VoiceProcessPool = &mockVoicePoolWithModel{model: tt.voicePoolModel}
	}
	p := New(appCfg, cfg)
	if got := p.LowEffortModel(); got != tt.wantLow {
		t.Errorf("LowEffortModel() = %q, want %q", got, tt.wantLow)
	}
	if got := p.MediumEffortModel(); got != tt.wantMed {
		t.Errorf("MediumEffortModel() = %q, want %q", got, tt.wantMed)
	}
	if got := p.VoiceModel(); got != tt.wantVoice {
		t.Errorf("VoiceModel() = %q, want %q", got, tt.wantVoice)
	}
}

func TestWorkerPool_EffortModelResolution(t *testing.T) {
	t.Parallel()
	// Verifies worker pool message processing and voice lifecycle models.
	defaultLow := config.DefaultConfigData().LowEffortModel
	defaultMed := config.DefaultConfigData().MediumEffortModel
	appLow, appMed, appEmpty, appVoice := "app-cfg-low-model", "app-cfg-med-model", "", "app-voice-model"

	tests := []effortModelResolutionCase{
		{"NilPoolFallback", "", "", true, nil, nil, nil, "", defaultLow, defaultMed, defaultLow},
		{"CustomModelInCfg", "gemini-flash-low-test", "gemini-flash-med-test", false, nil, nil, nil, "", "gemini-flash-low-test", "gemini-flash-med-test", "gemini-flash-low-test"},
		{"AppCfgOverride", "", "", false, &appLow, &appMed, nil, "", "app-cfg-low-model", "app-cfg-med-model", "app-cfg-low-model"},
		{"AppCfgEmptyWithCfgFallback", "cfg-fallback-low", "cfg-fallback-med", false, &appEmpty, &appEmpty, nil, "", "cfg-fallback-low", "cfg-fallback-med", "cfg-fallback-low"},
		{"AppCfgEmptyAndCfgEmptyDefaults", "", "", false, &appEmpty, &appEmpty, nil, "", defaultLow, defaultMed, defaultLow},
		{"AppCfgNilAndCfgEmptyDefaults", "", "", false, nil, nil, nil, "", defaultLow, defaultMed, defaultLow},
		{"AppCfgVoiceModelSet", "", "", false, nil, nil, &appVoice, "", defaultLow, defaultMed, "app-voice-model"},
		{"VoiceProcessPoolModelWhenAppCfgNil", "", "", false, nil, nil, nil, "pool-voice-model", defaultLow, defaultMed, "pool-voice-model"},
		{"AppCfgVoiceModelEmptyIgnoresVoicePool", "", "", false, &appLow, nil, &appEmpty, "stale-pool-model", "app-cfg-low-model", defaultMed, "app-cfg-low-model"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runEffortModelResolutionCase(t, tt)
		})
	}
}

// voiceTraceHarness is the tracing side of a voice test pool: the recorder its spans land in,
// the active-turn registry it registers daemons in, and whether the daemon saw its turn
// registered (with tool spans attached) while executing.
type voiceTraceHarness struct {
	rec        *tracetest.SpanRecorder
	tp         *sdktrace.TracerProvider
	registry   *runner.ActiveTurnRegistry
	registered atomic.Bool
}

func newVoiceTraceHarness() *voiceTraceHarness {
	rec := tracetest.NewSpanRecorder()
	return &voiceTraceHarness{rec: rec, tp: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)), registry: runner.NewActiveTurnRegistry()}
}

// observeRegistration records whether the voice daemon is registered under its turn span.
func (h *voiceTraceHarness) observeRegistration() {
	daemonID, ok := h.registry.SoleDaemonInTarget(voiceTestTarget)
	if !ok {
		return
	}
	_, hasSpan := h.registry.Get(daemonID)
	h.registered.Store(hasSpan)
}

// voiceTestTarget names the voice test pool's runtime home, so its daemons register under it.
const voiceTestTarget = "voice"

func buildVoiceTestAppConfig(appVoiceModel, appLowModel string) *config.Config {
	if appVoiceModel == "" && appLowModel == "" {
		return nil
	}
	return newTestConfig(func(d *config.ConfigData) {
		if appLowModel != "" {
			d.LowEffortModel = appLowModel
		}
		d.Voice.Model = appVoiceModel
	})
}

func assertVoiceTurnChatSpan(t *testing.T, ended []sdktrace.ReadOnlySpan, parentSpanID trace.SpanID, wantChatSpan bool) {
	t.Helper()
	hasChat := false
	for _, s := range ended {
		if s.Name() == "chat" && s.Parent().SpanID() == parentSpanID {
			hasChat = true
		}
	}
	if hasChat != wantChatSpan {
		t.Errorf("chat child span = %v, want %v", hasChat, wantChatSpan)
	}
}

type voiceGuardCase struct {
	name           string
	sessionID      string
	cancelCaller   bool
	poolDeadCtx    bool
	poolNil        bool
	poolStopped    bool
	nilProcessPool bool
	wantErrSub     string
	wantErrIs      error
	wantTurnStatus string
}

func assertVoiceGuardTurnStatus(t *testing.T, h *voiceTraceHarness, callerSpanID trace.SpanID, wantStatus string) {
	t.Helper()
	var turn sdktrace.ReadOnlySpan
	for _, s := range h.rec.Ended() {
		if s.Name() == "invoke_agent" && s.Parent().SpanID() == callerSpanID {
			turn = s
		}
	}
	if turn == nil {
		t.Fatalf("no invoke_agent span parented to caller span")
	}
	attrs := map[string]string{}
	for _, kv := range turn.Attributes() {
		val := kv.Value.AsString()
		if val == "" {
			val = kv.Value.String()
		}
		attrs[string(kv.Key)] = val
	}
	if attrs["aerial.turn.status"] != wantStatus {
		t.Errorf("invoke_agent status = %q, want %q", attrs["aerial.turn.status"], wantStatus)
	}
}

func runVoiceGuardCase(t *testing.T, tc voiceGuardCase) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if tc.cancelCaller {
		cancel()
	}

	h := newVoiceTraceHarness()
	ctx, caller := h.tp.Tracer("test").Start(ctx, "POST /voice/ask")

	var pool *WorkerPool
	if !tc.poolNil {
		switch {
		case tc.poolDeadCtx:
			deadCtx, deadCancel := context.WithCancel(context.Background())
			deadCancel()
			pool = &WorkerPool{ctx: deadCtx}
		case tc.poolStopped:
			pool = &WorkerPool{stopped: true}
		case tc.nilProcessPool:
			pool = New(nil, WorkerPoolConfig{TracerProvider: h.tp, ActiveTurns: h.registry})
			pool.pools = nil
		default:
			pool = &WorkerPool{ctx: context.Background()}
		}
	}

	_, _, err := pool.ExecuteVoiceTurn(ctx, "hello", tc.sessionID, nil, nil)
	caller.End()

	if _, ok := h.registry.SoleDaemonInTarget(voiceTestTarget); ok {
		t.Errorf("voice daemon still registered after the turn")
	}

	switch {
	case tc.wantErrIs != nil:
		if !errors.Is(err, tc.wantErrIs) {
			t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
		}
	case tc.wantErrSub != "":
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
		}
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	}

	if tc.wantTurnStatus != "" {
		assertVoiceGuardTurnStatus(t, h, caller.SpanContext().SpanID(), tc.wantTurnStatus)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_Guards(t *testing.T) {
	t.Parallel()
	tests := []voiceGuardCase{
		{name: "emptySession", sessionID: "", wantErrSub: "session_id is required"},
		{name: "whitespaceSession", sessionID: "   ", wantErrSub: "session_id is required"},
		{name: "canceledCallerCtx", sessionID: "sess-cancel", cancelCaller: true, wantErrIs: context.Canceled},
		{name: "deadPoolCtx", sessionID: "sess-dead", poolDeadCtx: true, wantErrSub: "brain is shutting down"},
		{name: "nilPool", sessionID: "sess-nil", poolNil: true, wantErrSub: "worker pool is uninitialized"},
		{name: "stoppedPool", sessionID: "sess-stopped", poolStopped: true, wantErrSub: "brain is shutting down"},
		{name: "nilProcessPool", sessionID: "sess-1", nilProcessPool: true, wantErrSub: "voice daemon pool unavailable", wantTurnStatus: "failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceGuardCase(t, tc)
		})
	}
}

type voiceHookTurnCase struct {
	name               string
	prompt             string
	sessionID          string
	useVoiceHook       bool
	useVoiceStreamHook bool
	hookErr            error
	runnerOut          string
	runnerErrOut       string
	runnerCode         int
	runnerErr          error
	wantReply          string
	wantConvID         string
	wantErrSub         string
	wantStatusSub      string
	wantSentenceSub    string
	wantPrompt         string
}

func buildVoiceHookPool(tc voiceHookTurnCase, h *voiceTraceHarness, onRecordPrompt func(string)) *WorkerPool {
	switch {
	case tc.useVoiceHook:
		return New(nil, WorkerPoolConfig{
			TracerProvider: h.tp, ActiveTurns: h.registry,
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
	case tc.useVoiceStreamHook:
		return New(nil, WorkerPoolConfig{
			TracerProvider: h.tp, ActiveTurns: h.registry,
			VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
				if tc.hookErr != nil {
					return "", sessionID, tc.hookErr
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
	default:
		return New(nil, WorkerPoolConfig{
			LowEffortModel: "low-effort-fast-model", TracerProvider: h.tp, ActiveTurns: h.registry,
			RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return tc.runnerOut, tc.runnerErrOut, tc.runnerCode, tc.runnerErr
			},
		})
	}
}

func assertVoiceHookOutputs(t *testing.T, tc voiceHookTurnCase, reply, convID, statusReceived, sentenceReceived, recordedPrompt string) {
	t.Helper()
	if tc.wantReply != "" && reply != tc.wantReply {
		t.Errorf("reply = %q, want %q", reply, tc.wantReply)
	}
	if tc.wantConvID != "" && convID != tc.wantConvID {
		t.Errorf("convID = %q, want %q", convID, tc.wantConvID)
	}
	if tc.wantPrompt != "" && recordedPrompt != tc.wantPrompt {
		t.Errorf("recorded prompt = %q, want %q", recordedPrompt, tc.wantPrompt)
	}
	if tc.wantStatusSub != "" && !strings.Contains(statusReceived, tc.wantStatusSub) {
		t.Errorf("status = %q, want substring %q", statusReceived, tc.wantStatusSub)
	}
	if tc.wantSentenceSub != "" && sentenceReceived != tc.wantSentenceSub {
		t.Errorf("sentence = %q, want %q", sentenceReceived, tc.wantSentenceSub)
	}
}

func runVoiceHookTurnCase(t *testing.T, tc voiceHookTurnCase) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newVoiceTraceHarness()
	ctx, caller := h.tp.Tracer("test").Start(ctx, "POST /voice/ask")

	var recordedPrompt, statusReceived, sentenceReceived string
	pool := buildVoiceHookPool(tc, h, func(p string) { recordedPrompt = p })
	pool.Start()
	defer pool.Stop()

	reply, convID, err := pool.ExecuteVoiceTurn(ctx, tc.prompt, tc.sessionID, func(s string) { statusReceived = s }, func(s string) { sentenceReceived = s })
	caller.End()
	pool.MarkDirty()

	if tc.useVoiceHook {
		for _, s := range h.rec.Ended() {
			if s.Name() == "invoke_agent" && s.Parent().SpanID() == caller.SpanContext().SpanID() {
				t.Errorf("custom voice runner opened an invoke_agent span")
			}
		}
	}
	if _, ok := h.registry.SoleDaemonInTarget(voiceTestTarget); ok {
		t.Errorf("voice daemon still registered after the turn")
	}

	if tc.wantErrSub != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertVoiceHookOutputs(t, tc, reply, convID, statusReceived, sentenceReceived, recordedPrompt)
}

func TestWorkerPool_ExecuteVoiceTurn_RunnerHooks(t *testing.T) {
	t.Parallel()
	tests := []voiceHookTurnCase{
		{name: "VoiceRunnerHook", prompt: "hello", sessionID: "sess-hook-1", useVoiceHook: true, wantReply: "voice hook reply", wantConvID: "sess-hook-1", wantStatusSub: "⚡ Custom hook"},
		{name: "ForwardCleanPrompt", prompt: "what is the cluster node status?", sessionID: "dev-voice-1", useVoiceHook: true, wantReply: "All three nodes are online.", wantConvID: "dev-voice-1", wantPrompt: "what is the cluster node status?"},
		{name: "VoiceStreamRunnerHook", prompt: "hello", sessionID: "sess-stream-1", useVoiceStreamHook: true, wantReply: "voice stream reply", wantConvID: "sess-stream-1", wantStatusSub: "⚡ Streaming hook", wantSentenceSub: "Streaming sentence."},
		{name: "VoiceStreamRunnerHookError", prompt: "hello", sessionID: "sess-err", useVoiceStreamHook: true, hookErr: errors.New("simulated stream failure"), wantErrSub: "simulated stream failure"},
		{name: "SuccessTurn", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"turned off fan\"}}", wantReply: "turned off fan", wantConvID: "sess-custom-1", wantStatusSub: "⚡"},
		{name: "RunnerError", prompt: "turn off fan", sessionID: "sess-custom-1", runnerErrOut: "exec error", runnerCode: 1, runnerErr: errors.New("exec failed"), wantErrSub: "exec failed"},
		{name: "ExitCodeNonZero", prompt: "turn off fan", sessionID: "sess-custom-1", runnerErrOut: "process died", runnerCode: 42, wantErrSub: "exit code 42"},
		{name: "PlainTextFallback", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: "plain text response", wantReply: "plain text response", wantConvID: "sess-custom-1"},
		{name: "EmptyResponseField", prompt: "turn off fan", sessionID: "sess-custom-1", runnerOut: "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"\"}}", wantErrSub: "empty response"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceHookTurnCase(t, tc)
		})
	}
}

type voiceDaemonTurnCase struct {
	name                 string
	sessionID            string
	spawnerErr           error
	closePipeImmediately bool
	cancelCallerOnScan   bool
	resultStatus         string
	resultError          string
	resultResponse       string
	resultExitCode       int
	appVoiceModel        string
	appLowModel          string
	wantReply            string
	wantErrSub           string
	wantTurnStatus       string
	wantChatSpan         bool
}

func assertVoiceDaemonTurnSpan(t *testing.T, tc voiceDaemonTurnCase, h *voiceTraceHarness, callerSpanID trace.SpanID) {
	t.Helper()
	if tc.wantTurnStatus == "" {
		return
	}
	var turn sdktrace.ReadOnlySpan
	for _, s := range h.rec.Ended() {
		if s.Name() == "invoke_agent" && s.Parent().SpanID() == callerSpanID {
			turn = s
		}
	}
	if turn == nil {
		t.Fatalf("no invoke_agent span parented to the caller span")
	}
	attrs := map[string]string{}
	for _, kv := range turn.Attributes() {
		val := kv.Value.AsString()
		if val == "" {
			val = kv.Value.String()
		}
		attrs[string(kv.Key)] = val
	}
	if attrs["aerial.turn.trigger"] != "voice" || attrs["aerial.turn.status"] != tc.wantTurnStatus {
		t.Errorf("invoke_agent trigger=%q status=%q, want voice/%q", attrs["aerial.turn.trigger"], attrs["aerial.turn.status"], tc.wantTurnStatus)
	}

	wantModel := "gemini-2.5-flash"
	wantProvider := "gcp.gemini"
	if tc.appVoiceModel != "" {
		wantModel = tc.appVoiceModel
		wantProvider = "anthropic"
	} else if tc.appLowModel != "" {
		wantModel = tc.appLowModel
		wantProvider = "anthropic"
	}
	if tc.wantTurnStatus == "success" {
		if attrs["gen_ai.request.model"] != wantModel {
			t.Errorf("invoke_agent model = %q, want %q", attrs["gen_ai.request.model"], wantModel)
		}
		if attrs["gen_ai.provider.name"] != wantProvider {
			t.Errorf("invoke_agent provider = %q, want %q", attrs["gen_ai.provider.name"], wantProvider)
		}
	}

	if wantErr := tc.wantTurnStatus != "success"; wantErr != (turn.Status().Code == codes.Error) {
		t.Errorf("invoke_agent span status = %v, want error=%v", turn.Status().Code, wantErr)
	}

	assertVoiceTurnChatSpan(t, h.rec.Ended(), turn.SpanContext().SpanID(), tc.wantChatSpan)
}

func runVoiceDaemonTurnCase(t *testing.T, tc voiceDaemonTurnCase) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newVoiceTraceHarness()
	ctx, caller := h.tp.Tracer("test").Start(ctx, "POST /voice/ask")

	var blockCh chan struct{}
	onScan := h.observeRegistration
	if tc.cancelCallerOnScan {
		blockCh = make(chan struct{})
		t.Cleanup(func() { close(blockCh) })
		onScan = func() { h.observeRegistration(); cancel() }
	}

	spawner := newEventDrivenVoiceDaemonSpawner(voiceProcessPoolSpawnerConfig{
		spawnerErr:              tc.spawnerErr,
		closeInRPipeImmediately: tc.closePipeImmediately,
		resultStatus:            tc.resultStatus,
		resultError:             tc.resultError,
		resultResponse:          tc.resultResponse,
		resultExitCode:          tc.resultExitCode,
		cancelCallerOnScan:      onScan,
		blockUntilDone:          blockCh,
	})
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel:  "gemini-2.5-flash",
		GeminiHomeDir: filepath.Join(t.TempDir(), voiceTestTarget),
	}, spawner)
	defer procPool.Close()

	appCfg := buildVoiceTestAppConfig(tc.appVoiceModel, tc.appLowModel)
	pool := New(appCfg, WorkerPoolConfig{
		ProcessPool:      procPool,
		VoiceProcessPool: procPool,
		TracerProvider:   h.tp,
		ActiveTurns:      h.registry,
	})
	pool.Start()
	defer pool.Stop()

	reply, _, err := pool.ExecuteVoiceTurn(ctx, "test", tc.sessionID, nil, nil)
	caller.End()
	pool.MarkDirty()

	wantRegistered := tc.spawnerErr == nil && !tc.closePipeImmediately
	if wantRegistered != h.registered.Load() {
		t.Errorf("registered during Execute = %v, want %v", h.registered.Load(), wantRegistered)
	}
	if _, ok := h.registry.SoleDaemonInTarget(voiceTestTarget); ok {
		t.Errorf("voice daemon still registered after the turn")
	}

	assertVoiceDaemonTurnSpan(t, tc, h, caller.SpanContext().SpanID())

	if tc.wantErrSub != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantReply != "" && reply != tc.wantReply {
		t.Errorf("reply = %q, want %q", reply, tc.wantReply)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_DaemonPool(t *testing.T) {
	t.Parallel()
	tests := []voiceDaemonTurnCase{
		{name: "AcquisitionFailure", sessionID: "sess-acq-fail", spawnerErr: errors.New("spawner failed to start"), wantErrSub: "failed to acquire voice daemon", wantTurnStatus: "failed"},
		{name: "DaemonExecutionError", sessionID: "sess-daemon-err", resultStatus: "ERROR", resultError: "simulated failure in daemon", wantErrSub: "simulated failure in daemon", wantTurnStatus: "failed", wantChatSpan: true},
		{name: "DaemonTurnTraced", sessionID: "sess-daemon-ok", resultStatus: "SUCCESS", resultResponse: "lights on", wantReply: "lights on", wantTurnStatus: "success", wantChatSpan: true},
		{name: "SendFailureBrokenPipe", sessionID: "sess-pipe-err", closePipeImmediately: true, wantErrSub: "failed sending turn to voice daemon"},
		{name: "ContextCanceledWaitingForResult", sessionID: "sess-ctx-cancel", cancelCallerOnScan: true, wantErrSub: "context canceled", wantTurnStatus: "cancelled"},
		{name: "ExitCodeNonZeroAndEmptySessionFallback", sessionID: "sess-exit-fallback", resultStatus: "SUCCESS", resultExitCode: 1, wantErrSub: "empty response"},
		{name: "VoiceModelSet", sessionID: "sess-voice-model", appVoiceModel: "haiku", appLowModel: "opus-low", resultStatus: "SUCCESS", resultResponse: "lights on", wantReply: "lights on", wantTurnStatus: "success", wantChatSpan: true},
		{name: "VoiceModelEmptyFallbackLow", sessionID: "sess-voice-empty", appVoiceModel: "", appLowModel: "opus-low", resultStatus: "SUCCESS", resultResponse: "lights on", wantReply: "lights on", wantTurnStatus: "success", wantChatSpan: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceDaemonTurnCase(t, tc)
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
	onSpawn                 func(cfg runner.DaemonConfig)
}

func runMockVoiceDaemonPump(inR io.Reader, outW, errW io.WriteCloser, cfg voiceProcessPoolSpawnerConfig) {
	defer outW.Close()
	defer errW.Close()
	sessID := cfg.initSessionID
	if sessID == "" {
		sessID = "550e8400-e29b-41d4-a716-446655440000"
	}
	_, _ = outW.Write(fmt.Appendf(nil, "{\"event\":\"init\",\"session_id\":%q,\"conversation_id\":%q}\n", sessID, sessID))
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
		if err := os.WriteFile(filepath.Join(cfg.writeTranscriptLogsDir, "transcript.jsonl"), []byte(transcript), 0o644); err != nil {
			return
		}
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
	_, _ = outW.Write(fmt.Appendf(nil, "{\"event\":\"result\",\"result\":{\"status\":%q,\"response\":%q,\"error\":%q,\"conversation_id\":%q%s%s}}\n", status, cfg.resultResponse, cfg.resultError, sessID, exitPart, usagePart))
}

func newEventDrivenVoiceDaemonSpawner(cfg voiceProcessPoolSpawnerConfig) *mockDaemonSpawner {
	return &mockDaemonSpawner{
		SpawnFn: func(ctx context.Context, daemonCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			if cfg.onSpawn != nil {
				cfg.onSpawn(daemonCfg)
			}
			if cfg.spawnerErr != nil {
				return nil, nil, nil, nil, cfg.spawnerErr
			}
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			if cfg.closeInRPipeImmediately {
				_ = inR.Close()
			}
			go runMockVoiceDaemonPump(inR, outW, errW, cfg)
			return inW, outR, errR, newMockProcessHandle(12345), nil
		},
	}
}

type voiceProcessPoolTestCase struct {
	name              string
	prompt            string
	deviceKey         string
	spawnerCfg        voiceProcessPoolSpawnerConfig
	useVoicePoolField bool
	withSessionMgr    bool
	trackMetrics      bool
	wantReply         string
	wantConvID        string
	wantMinStatuses   int
	wantSentenceSub   string
	wantSpawnedTarget string
}

func setupVoiceSessionManager(t *testing.T, deviceKey string, cfg *voiceProcessPoolSpawnerConfig) *session.Manager {
	t.Helper()
	tempDir := t.TempDir()
	logsDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", deviceKey, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	cfg.writeTranscriptLogsDir = logsDir
	return session.New(tempDir, tempDir)
}

func assertVoiceProcessPoolMetrics(t *testing.T, pool *WorkerPool, prompt, wantReply, convID string) {
	t.Helper()
	if !runner.IsValidUUID(convID) {
		t.Errorf("expected valid UUID convID, got %q", convID)
	}
	reply2, _, err2 := pool.ExecuteVoiceTurn(context.Background(), prompt, "sess-no-status", nil, nil)
	if err2 != nil || reply2 != wantReply {
		t.Errorf("unexpected nil onStatus error: %v, reply2=%q", err2, reply2)
	}
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if body := rec.Body.String(); !strings.Contains(body, `source="voice"`) || !strings.Contains(body, `channel="voice"`) {
		t.Errorf("expected metrics to contain source=voice and channel=voice")
	}
}

func runVoiceProcessPoolCase(t *testing.T, tc voiceProcessPoolTestCase) {
	t.Helper()
	var spawnedTarget string
	cfg := tc.spawnerCfg
	cfg.onSpawn = func(dCfg runner.DaemonConfig) {
		spawnedTarget = dCfg.ThreadID
	}

	var sessMgr *session.Manager
	if tc.withSessionMgr {
		sessMgr = setupVoiceSessionManager(t, tc.deviceKey, &cfg)
	}

	spawner := newEventDrivenVoiceDaemonSpawner(cfg)
	var rescuer func(ctx context.Context, sessionID string, since time.Time) string
	if sessMgr != nil {
		rescuer = sessMgr.ExtractResponseSinceContext
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel:      "gemini-2.5-flash",
		TranscriptRescuer: rescuer,
	}, spawner)
	defer procPool.Close()

	wpCfg := WorkerPoolConfig{SessionManager: sessMgr}
	if tc.useVoicePoolField {
		wpCfg.VoiceProcessPool = procPool
	} else {
		wpCfg.ProcessPool = procPool
	}
	pool := New(nil, wpCfg)
	pool.Start()
	defer pool.Stop()

	var statuses, sentences []string
	reply, convID, err := pool.ExecuteVoiceTurn(
		context.Background(),
		tc.prompt,
		tc.deviceKey,
		func(s string) { statuses = append(statuses, s) },
		func(s string) { sentences = append(sentences, s) },
	)
	if err != nil {
		t.Fatalf("ExecuteVoiceTurn failed: %v", err)
	}
	if reply != tc.wantReply {
		t.Errorf("reply = %q, want %q", reply, tc.wantReply)
	}
	if tc.wantConvID != "" && convID != tc.wantConvID {
		t.Errorf("convID = %q, want %q", convID, tc.wantConvID)
	}
	if tc.wantSpawnedTarget != "" && spawnedTarget != tc.wantSpawnedTarget {
		t.Errorf("spawnedTarget = %q, want %q", spawnedTarget, tc.wantSpawnedTarget)
	}
	if tc.wantMinStatuses > 0 && len(statuses) < tc.wantMinStatuses {
		t.Errorf("got %d statuses, want at least %d", len(statuses), tc.wantMinStatuses)
	}
	if tc.wantSentenceSub != "" {
		joined := strings.Join(sentences, " ")
		if !strings.Contains(joined, tc.wantSentenceSub) {
			t.Errorf("sentences %q missing %q", joined, tc.wantSentenceSub)
		}
	}
	if tc.trackMetrics {
		assertVoiceProcessPoolMetrics(t, pool, tc.prompt, tc.wantReply, convID)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_ProcessPool(t *testing.T) {
	t.Parallel()
	stepUpdates := []string{
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","tool_name":"ha_call_service","tool_info":{"parameters":{"CommandLine":""}}}}`,
		`{"event":"step_update","step_update":{"state":"RUNNING","type":"tool","tool_name":"","tool_info":{"parameters":{"CommandLine":"system-health"}}}}`,
		`{"event":"step_update","step_update":{"state":"DONE","type":"tool_call","tool_name":"ha_call_service"}}`,
		`{"event":"step_update","step_update":{"state":"ERROR","type":"tool_call","tool_name":"ha_call_service"}}`,
	}

	tests := []voiceProcessPoolTestCase{
		{
			name:      "success_and_metrics",
			prompt:    "turn on living room light",
			deviceKey: "kiosk",
			spawnerCfg: voiceProcessPoolSpawnerConfig{
				stepUpdates:    stepUpdates,
				resultStatus:   "SUCCESS",
				resultResponse: "daemon voice turn reply",
				tokens:         runner.TokenUsage{InputTokens: 60, OutputTokens: 40, TotalTokens: 100},
			},
			trackMetrics:    true,
			wantReply:       "daemon voice turn reply",
			wantMinStatuses: 2,
		},
		{
			name:              "device_routing",
			prompt:            "what is the weather",
			deviceKey:         "kiosk",
			useVoicePoolField: true,
			spawnerCfg: voiceProcessPoolSpawnerConfig{
				initSessionID:  "550e8400-e29b-41d4-a716-446655440000",
				resultStatus:   "SUCCESS",
				resultResponse: "kiosk device reply",
			},
			wantReply:         "kiosk device reply",
			wantConvID:        "550e8400-e29b-41d4-a716-446655440000",
			wantSpawnedTarget: "kiosk",
		},
		{
			name:           "transcript_recovery",
			prompt:         "What is the status?",
			deviceKey:      "550e8400-e29b-41d4-a716-446655449999",
			withSessionMgr: true,
			spawnerCfg: voiceProcessPoolSpawnerConfig{
				initSessionID:  "550e8400-e29b-41d4-a716-446655449999",
				resultStatus:   "ERROR",
				resultError:    "RESOURCE_EXHAUSTED (code 429)",
				resultResponse: "All systems operational and nominal.",
			},
			wantReply:       "All systems operational and nominal.",
			wantConvID:      "550e8400-e29b-41d4-a716-446655449999",
			wantSentenceSub: "All systems operational and nominal.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceProcessPoolCase(t, tc)
		})
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
				if _, err := io.Copy(io.Discard, inR); err != nil {
					return
				}
			}()
			return inW, outR, errR, mockHandle, nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
	if _, err := procPool.GetOrCreate(context.Background(), "kiosk", ""); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	voiceProcPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, spawner)
	if _, err := voiceProcPool.GetOrCreate(context.Background(), "kiosk-voice", ""); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	pool := New(nil, WorkerPoolConfig{ProcessPool: procPool, VoiceProcessPool: voiceProcPool})
	pool.Start()
	pool.MarkDirty()
	pool.Stop()
}

func TestTurnExecution_StoreDelegates(t *testing.T) {
	t.Parallel()
	fake := testStore(t)
	closedFake, closedDB := newTestStore(t)
	_ = closedDB.Close()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	if err := pubsub.EnsureCoreStreams(context.Background(), memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	pFake := &WorkerPool{eventBus: memBus, cfg: WorkerPoolConfig{Store: fake, EventBus: memBus}}
	pErr := &WorkerPool{cfg: WorkerPoolConfig{Store: &errQueueStore{Store: closedFake}}}

	tests := []struct {
		name         string
		te           *turnExecution
		wantCount    int
		wantErr      bool
		wantSchedErr bool
	}{
		{name: "nil_pool_delegates", te: &turnExecution{}, wantCount: 0, wantErr: false, wantSchedErr: true},
		{name: "valid_store_delegates", te: &turnExecution{pool: pFake}, wantCount: 1, wantErr: false, wantSchedErr: false},
		{name: "error_store_delegates", te: &turnExecution{pool: pErr}, wantCount: 0, wantErr: true, wantSchedErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			threadID := "t-" + tc.name
			tc.te.updateMessageStatus("m1", "failed", "err")
			tc.te.updateMessageCompleted("m1", "resp")
			tc.te.saveSessionID(t.Context(), threadID, "s1")
			tc.te.rotateSessionID(threadID, "s2")
			cnt, err := tc.te.incrementSessionTurnCount(threadID)
			if (err != nil) != tc.wantErr || cnt != tc.wantCount {
				t.Errorf("incrementSessionTurnCount() = (%d, %v), want (%d, err=%v)", cnt, err, tc.wantCount, tc.wantErr)
			}
			if _, err := tc.te.getSessionID(threadID); (err != nil) != tc.wantErr {
				t.Errorf("getSessionID() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if prev := tc.te.getPreviousSessionID(t.Context(), threadID); tc.wantErr && prev != "" {
				t.Errorf("getPreviousSessionID() = %q, want empty on error", prev)
			}
			schedErr := tc.te.createOneShotSchedule(oneShotScheduleRequest{ID: "os-" + tc.name, ThreadID: threadID, Prompt: "p", RunAt: time.Now().UTC()})
			if (schedErr != nil) != tc.wantSchedErr {
				t.Errorf("createOneShotSchedule() err = %v, wantSchedErr = %v", schedErr, tc.wantSchedErr)
			}
			tc.te.saveThreadSummary(t.Context(), threadID, "sum", "m1")
			if sum, msgID := tc.te.getThreadSummary(t.Context(), threadID); tc.wantErr && (sum != "" || msgID != "") {
				t.Errorf("getThreadSummary() = (%q, %q), want empty on error", sum, msgID)
			}
			if msgs, err := tc.te.getRecentThreadMessages(threadID, 10); err != nil || len(msgs) != 0 {
				t.Errorf("getRecentThreadMessages() = (%v, %v), want empty/nil", msgs, err)
			}
		})
	}
}

type summarizeHistoryTestCase struct {
	name, threadID, beforeID, wantSummarySub string
	limit                                    int
	withLLM                                  bool
	wantSnowflake                            bool
}

func runSummarizeHistoryCase(t *testing.T, tc summarizeHistoryTestCase, dgHist *discordgo.Session, now time.Time) {
	t.Helper()
	ctx := context.Background()
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
	recent, err := FetchRecentThreadHistory(ctx, dgHist, tc.threadID, tc.limit)
	if tc.wantSnowflake {
		if err != nil || len(recent) != 2 {
			t.Errorf("FetchRecentThreadHistory() = (%v, %v), want (2 msgs, nil err)", len(recent), err)
		}
	} else {
		if err == nil {
			t.Errorf("FetchRecentThreadHistory() expected error for non-snowflake threadID %q", tc.threadID)
		}
	}
	if _, nilErr := FetchRecentThreadHistory(ctx, nil, "", tc.limit); nilErr == nil {
		t.Error("FetchRecentThreadHistory(nil) expected error")
	}
	fetcher := DefaultHistoryFetcher(dgHist)
	fetched, fErr := fetcher(ctx, tc.threadID, tc.beforeID, tc.limit)
	if tc.wantSnowflake {
		if fErr != nil || len(fetched) != 2 {
			t.Errorf("DefaultHistoryFetcher() = (%v, %v), want (2 msgs, nil err)", len(fetched), fErr)
		}
	} else {
		if fErr == nil {
			t.Errorf("DefaultHistoryFetcher() expected error for non-snowflake threadID %q", tc.threadID)
		}
	}
}

func TestSummarizeAndHistoryHelpers(t *testing.T) {
	t.Parallel()
	fake := testStore(t)
	now := time.Now().UTC()
	if err := insertMessage(fake, db.Message{ID: "msg-fast-1", ThreadID: "thread-fast-1", AuthorID: "user-1", AuthorName: "Alice", Content: "Hello fast path", CreatedAt: now}); err != nil {
		t.Fatalf("insertMessage: %v", err)
	}

	dgHist, err := discordgo.New("Bot test")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	dgHist.State = discordgo.NewState()
	dgHist.State.User = &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}
	dgHist.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, err := json.Marshal([]*discordgo.Message{
			nil,
			{ID: "123456789012345678", Content: "Aerial msg", Timestamp: now, Author: &discordgo.User{ID: "bot-user-123", Username: "aerial", Bot: true}},
			{ID: "123456789012345682", Content: "Zero ts", Timestamp: time.Time{}, Author: &discordgo.User{ID: "user-789", Username: "Alice"}},
		})
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	tests := []summarizeHistoryTestCase{
		{name: "fast_db_summary", threadID: "thread-fast-1", beforeID: "msg-role-7", limit: 10, withLLM: true, wantSummarySub: "Summary result", wantSnowflake: false},
		{name: "rest_snowflake_zero_limit", threadID: "123456789012345678", beforeID: "123456789012345678", limit: 0, wantSnowflake: true},
		{name: "rest_clamped_limit", threadID: "123456789012345678", beforeID: "non-num", limit: 150, wantSnowflake: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSummarizeHistoryCase(t, tc, dgHist, now)
		})
	}

	assertSummarizeTurnResolution(t, dgHist, fake)
	assertSummarizeTurnSessionAndDelivery(t, fake, now)
}

func assertSummarizeTurnResolution(t *testing.T, dgHist *discordgo.Session, fake db.Store) {
	t.Helper()
	teNil := &turnExecution{pool: &WorkerPool{}}
	if disp, pCtx := teNil.resolveWebhookDispatcher(); disp == nil || pCtx == nil {
		t.Errorf("resolveWebhookDispatcher() = (%v, %v), want non-nil", disp, pCtx)
	}
	if !teNil.resolveTurnPolicy() {
		t.Error("expected resolveTurnPolicy to return true")
	}
	if _, _, _, err := teNil.executeLeaseAttemptCtx(t.Context(), "test-model", "prompt"); err == nil {
		t.Fatal("expected error from executeLeaseAttemptCtx on nil pool")
	}
	teSumClassifier := &turnExecution{
		pool: &WorkerPool{
			cfg: WorkerPoolConfig{
				LowEffortModel: "flash-low",
				Classifier: &classifier.Classifier{
					LLMFunc: func(ctx context.Context, model, prompt string) (string, error) { return "ok", nil },
				},
				AmbientResolver: func(ctx context.Context, cfg *config.AmbientContextConfig) (string, error) { return "   ", nil },
			},
		},
		policy: config.ChannelPolicy{AmbientContext: &config.AmbientContextConfig{}},
	}
	if llmFn, model := teSumClassifier.resolveSummarizerLLMAndModel(t.Context()); llmFn == nil || model != "flash-low" {
		t.Errorf("resolveSummarizerLLMAndModel() = (%v, %q), want non-nil/flash-low", llmFn, model)
	}
	if pCtx := teSumClassifier.resolveAmbientPromptContext(context.Background()); pCtx != "" {
		t.Errorf("resolveAmbientPromptContext() = %q, want empty", pCtx)
	}
	if hist := teSumClassifier.fetchTurnHistory(context.Background(), 5); hist != nil {
		t.Errorf("fetchTurnHistory() = %v, want nil", hist)
	}
	teSnowflakeHist := &turnExecution{threadID: "123456789012345678", pool: &WorkerPool{cfg: WorkerPoolConfig{DiscordSession: dgHist, Store: fake}}}
	if hist := teSnowflakeHist.fetchTurnHistory(context.Background(), 5); len(hist) == 0 {
		t.Errorf("fetchTurnHistory() returned empty, want > 0")
	}
	if msgs, err := teSnowflakeHist.getRecentThreadMessages("123456789012345678", 5); err != nil || len(msgs) == 0 {
		t.Errorf("getRecentThreadMessages() = (%v, %v), want non-empty", msgs, err)
	}
	if dbMsg := convertDiscordMessageToDBMessage(&discordgo.Message{ID: "non-num", WebhookID: "wh-1"}, "t-1"); dbMsg.AuthorName != "Webhook" {
		t.Errorf("convertDiscordMessageToDBMessage().AuthorName = %q, want Webhook", dbMsg.AuthorName)
	}
}

func assertSummarizeTurnSessionAndDelivery(t *testing.T, fake db.Store, now time.Time) {
	t.Helper()
	teSess := &turnExecution{threadID: "t-pers", pool: &WorkerPool{cfg: WorkerPoolConfig{Store: fake}}}
	if sess := teSess.probeCandidateSessionID("", "", ""); sess != "" {
		t.Errorf("probeCandidateSessionID() = %q, want empty", sess)
	}
	teSess.currentSessionID = "sess-cur"
	if sess := teSess.probeCandidateSessionID("", "", ""); sess != "sess-cur" {
		t.Errorf("probeCandidateSessionID() = %q, want sess-cur", sess)
	}
	if s := teSess.resolveAndPersistSessionID("sess-1", "", "", true); s != "sess-1" {
		t.Errorf("resolveAndPersistSessionID() = %q, want sess-1", s)
	}
	if s := teSess.resolveAndPersistSessionID("sess-1", "", "", false); s != "sess-1" {
		t.Errorf("resolveAndPersistSessionID() = %q, want sess-1", s)
	}
	teSess.isEphemeral = true
	if s := teSess.resolveAndPersistSessionID("sess-2", "", "", true); s != "sess-2" {
		t.Errorf("resolveAndPersistSessionID() = %q, want sess-2", s)
	}
	teFail := &turnExecution{threadID: "t-fail", pool: &WorkerPool{cfg: WorkerPoolConfig{NotifierFunc: func(b, k, d string) string { return d }, DeliveryFunc: func(s *discordgo.Session, c, txt string) error { return errors.New("err") }}}}
	teFail.deliverFailureNotice("bin", "key", "err "+strings.Repeat("x", 400), "rate limit 429", "prefix: ", "lbl")
	teFail.deliverFailureNotice("bin", "key", "simple", "", "", "lbl")
	if ok, failReason := teFail.deliverSuccessAndFinalize(&runner.AgyResponse{Response: "   "}, runner.TokenUsage{}, "m", 1, 1); ok || failReason == "" {
		t.Errorf("deliverSuccessAndFinalize() = (%v, %q), want false/non-empty", ok, failReason)
	}
	teFail.deliverDiscordSuccess("hello", nil)
	if err := teFail.createOneShotSchedule(oneShotScheduleRequest{ID: "os-bad", ThreadID: "t-fail", Prompt: "p", RunAt: now}); err == nil {
		t.Error("createOneShotSchedule() expected error, got nil")
	}
}

func TestWorkerPool_RunnerFallbackAdaptersAndLifecycle(t *testing.T) {
	t.Parallel()
	fake := testStore(t)
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
				if err := lease.Release(); err != nil {
					t.Errorf("Release() error: %v", err)
				}
				if execErr != nil || res == nil || res.Response != tc.wantOut {
					t.Errorf("adapter output = %v, err = %v, want %q", res, execErr, tc.wantOut)
				}
			}
			if p.Classifier() == nil {
				t.Errorf("expected non-nil Classifier")
			}
		})
	}

	assertWorkerPoolLifecycleAndCallbacks(t, fake)
}

func assertWorkerPoolLifecycleAndCallbacks(t *testing.T, fake db.Store) {
	t.Helper()
	ctx := context.Background()
	var nilPool *WorkerPool
	if err := nilPool.DeliverDirect("c", "t"); err == nil {
		t.Error("nilPool.DeliverDirect expected error, got nil")
	}
	pEmpty := &WorkerPool{}
	if err := pEmpty.DeliverDirect("c", "t"); err == nil {
		t.Error("pEmpty.DeliverDirect expected error, got nil")
	}

	getRec, onRot := makeSessionCallbacks(fake)
	if _, err := getRec(ctx, "t1"); err != nil {
		t.Fatalf("getRec(t1): %v", err)
	}
	if err := onRot(ctx, "t1", "old", "new"); err != nil {
		t.Errorf("onRot error: %v", err)
	}
	if rec, err := getRec(ctx, "discord:worker-0"); err != nil || rec.ActiveSessionID != "" {
		t.Errorf("expected empty record for worker key with colon, got rec=%+v, err=%v", rec, err)
	}
	if err := onRot(ctx, "discord:worker-0", "old", "new"); err != nil {
		t.Errorf("expected nil error on onRot for worker key with colon, got %v", err)
	}
	getRecErr, onRotErr := makeSessionCallbacks(&errQueueStore{Store: fake})
	if _, err := getRecErr(ctx, "t1"); err != nil {
		t.Fatalf("getRecErr: %v", err)
	}
	if err := onRotErr(ctx, "t1", "old", "new"); err == nil {
		t.Error("onRotErr expected error, got nil")
	}
	if unwrapUnifiedPool(nil) != nil || unwrapUnifiedPool(&testRunnerAgentPool{}) != nil {
		t.Errorf("expected nil from unwrapUnifiedPool")
	}
	upWrap := runner.NewUnifiedProcessPool(runner.PoolConfig{}, nil)
	t.Cleanup(func() { _ = upWrap.Close() })
	ipWrap := runner.NewInterchangeablePool(upWrap, runner.InterchangeablePoolConfig{WorkerCount: 1})
	t.Cleanup(func() { _ = ipWrap.Close() })
	if got := unwrapUnifiedPool(ipWrap); got != upWrap {
		t.Errorf("expected unwrapUnifiedPool(ipWrap) = %p, want %p", got, upWrap)
	}
	wrappedPools := map[PoolKey]runner.AgentPool{
		{Ephemeral: false, Effort: config.EffortHigh}: ipWrap,
	}
	wirePoolSessionDependencies(wrappedPools, nil, fake)
}

func TestStatusUpdater_GuardrailsAndRESTTransport(t *testing.T) {
	t.Parallel()
	s, err := discordgo.New("Bot dummy-token")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	s.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		u := req.URL.String()
		switch {
		case req.Method == http.MethodPost && strings.Contains(u, "/messages"):
			b, mErr := json.Marshal(&discordgo.Message{ID: "msg-created-123"})
			if mErr != nil {
				return nil, mErr
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		case req.Method == http.MethodPatch:
			b, mErr := json.Marshal(&discordgo.Message{ID: "msg-edited-123"})
			if mErr != nil {
				return nil, mErr
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		case req.Method == http.MethodDelete:
			return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		default:
			return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewReader([]byte(`{"message":"bad request"}`)))}, nil
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
	uStep.MarkTurnStarted()
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "view_file", State: "ACTIVE", ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "ls"}}})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "other_tool", State: "DONE"})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "view_file", State: "ERROR"})
	uStep.HandleStep(&runner.StepUpdateEvent{StepType: "agent_response"})

	uREST := NewStatusUpdater(s, "thread-rest-123", true)
	t.Cleanup(uREST.Stop)
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
	t.Cleanup(up404.Stop)
	up404.turnStart = time.Now().Add(10 * time.Second)
	if txt := up404.currentStatusText(); txt == "" {
		t.Error("expected non-empty status text")
	}
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

	dgBot, err := discordgo.New("Bot dummy")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	dgBot.State.User = &discordgo.User{ID: "bot-1", Username: "AerialBot"}
	guild1 := &discordgo.Guild{
		ID:      "guild-1",
		Roles:   []*discordgo.Role{nil, {ID: "role-aerial", Name: "aerial"}, {ID: "role-gundam", Name: "gundam"}, {ID: "role-username", Name: "AerialBot"}, {ID: "role-other", Name: "moderator"}},
		Members: []*discordgo.Member{{User: &discordgo.User{ID: "bot-1"}, Roles: []string{"role-member-assigned", ""}}},
	}
	if err := dgBot.State.GuildAdd(guild1); err != nil {
		t.Fatalf("GuildAdd: %v", err)
	}

	tests := []struct {
		name       string
		msg        db.Message
		wantEffort string
	}{
		{"explicit_custom_effort", db.Message{Effort: "custom"}, "high"},
		{"model_derived_medium_effort", db.Message{Model: "claude-medium"}, "medium"},
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

type burstPolicyCase struct {
	name, runnerOut, runnerErrOut, wantMsgStatus string
	msg                                          db.Message
	trailing                                     *db.Message
	runnerCode                                   int
	runnerErr, claimErr                          error
	policy                                       *config.ChannelPolicy
	nilClassifier, unclaimed                     bool
}

func burstPolicyCases() []burstPolicyCase {
	now := time.Now().UTC()
	f05 := 0.5
	return []burstPolicyCase{
		{name: "claim_error_aborts_burst", msg: db.Message{ID: "m-err-claim", ThreadID: "t-claim", Status: db.StatusPending, CreatedAt: now}, claimErr: errors.New("db lock error")},
		{name: "unclaimed_message_skipped", msg: db.Message{ID: "m-unclaimed", ThreadID: "t-unclaimed", Status: db.StatusPending, CreatedAt: now}, unclaimed: true},
		{name: "ignored_channel_completes_schedule_run", msg: db.Message{ID: "m-ign-extra", ThreadID: "t-ign-extra", AuthorID: "u1", Content: "ignored prompt", Status: db.StatusPending, CreatedAt: now}, policy: &config.ChannelPolicy{Mode: "ignore"}, wantMsgStatus: db.StatusCompleted},
		{name: "bad_json_output_fails_after_attempt", msg: db.Message{ID: "m-badparse", ThreadID: "t-badparse", AuthorID: "u1", Content: "hello", Status: db.StatusPending, CreatedAt: now}, runnerOut: "definitely not json", wantMsgStatus: db.StatusFailed},
		{name: "rate_limit_fails_schedule_run", msg: db.Message{ID: "m-rate-1", ThreadID: "t-rate", AuthorID: "u1", Content: "query", Status: db.StatusPending, CreatedAt: now}, runnerErrOut: "Resource has been exhausted: rate limit exceeded 429", runnerCode: 1, runnerErr: errors.New("exit 1"), wantMsgStatus: db.StatusFailed},
		{name: "trailing_query_with_nil_classifier_and_default_policy", msg: db.Message{ID: "m-w-lead", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "<@aerial> start", Status: db.StatusPending, CreatedAt: now}, trailing: &db.Message{ID: "m-trail-query", ThreadID: "t-trail-noclass", AuthorID: "u1", Content: "why is the ocean blue", Status: db.StatusPending, CreatedAt: now}, runnerOut: mockJSONResponse("", "OK"), policy: &config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: &f05}, nilClassifier: true, wantMsgStatus: db.StatusCompleted},
	}
}

// setupBurstPolicyCase seeds tc's rows and builds its pool; it returns the store to read back and
// the burst in order.
func setupBurstPolicyCase(t *testing.T, tc burstPolicyCase) (*WorkerPool, db.Store, []db.Message) {
	t.Helper()
	fake := testStore(t)
	if err := insertMessage(fake, tc.msg); err != nil {
		t.Fatalf("insertMessage: %v", err)
	}
	burst := []db.Message{tc.msg}
	if tc.trailing != nil {
		if err := insertMessage(fake, *tc.trailing); err != nil {
			t.Fatalf("insertMessage: %v", err)
		}
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
	t.Cleanup(pool.Stop)
	return pool, fake, burst
}

func assertBurstPolicyCase(t *testing.T, tc burstPolicyCase, store db.Store) {
	t.Helper()
	if tc.wantMsgStatus == "" {
		return
	}
	if gotMsg, err := getMessage(store, tc.msg.ID); err != nil || gotMsg == nil || gotMsg.Status != tc.wantMsgStatus {
		t.Errorf("msg %s status = %v (err=%v), want %s", tc.msg.ID, gotMsg, err, tc.wantMsgStatus)
	}
}

func TestWorkerPool_ProcessBurstFailuresAndPolicies(t *testing.T) {
	t.Parallel()
	for _, tc := range burstPolicyCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, store, burst := setupBurstPolicyCase(t, tc)
			ids := make([]string, len(burst))
			for i, m := range burst {
				ids[i] = m.ID
			}
			if lt := runLiveBurst(context.Background(), pool, tc.msg.ThreadID, "turn-1", ids...); lt.outcome == "" {
				t.Error("empty outcome")
			}
			assertBurstPolicyCase(t, tc, store)
		})
	}
}
