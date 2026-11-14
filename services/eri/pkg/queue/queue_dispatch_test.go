package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"go.opentelemetry.io/otel/attribute"
)

func TestWorkerPool_ResolveSynapseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pool *WorkerPool
		want string
	}{
		{name: "nil pool", pool: nil, want: ""},
		{name: "configured URL trimmed", pool: &WorkerPool{cfg: WorkerPoolConfig{SynapseURL: "http://synapse.local:4020///"}}, want: "http://synapse.local:4020"},
		{name: "appCfg fallback", pool: &WorkerPool{appCfg: config.NewFromData(&config.ConfigData{SynapseURL: "http://synapse.cfg:4020///"})}, want: "http://synapse.cfg:4020"},
		{name: "empty when unconfigured", pool: &WorkerPool{}, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.pool.resolveSynapseURL(); got != tc.want {
				t.Fatalf("resolveSynapseURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDispatchPromptAssembly(t *testing.T) {
	t.Parallel()

	input := DispatchPromptInput{
		Dispatch: db.Dispatch{
			Prompt: "<SYSTEM_DIRECTIVE>\nalready wrapped\n</SYSTEM_DIRECTIVE>",
		},
	}
	res := AssembleDispatchPrompt(input)
	if res != input.Dispatch.Prompt {
		t.Fatalf("AssembleDispatchPrompt() = %q, want %q", res, input.Dispatch.Prompt)
	}
	if strings.Count(res, "<SYSTEM_DIRECTIVE>") != 1 || strings.Count(res, "</SYSTEM_DIRECTIVE>") != 1 {
		t.Fatalf("expected exactly one SYSTEM_DIRECTIVE block in %q", res)
	}
}

func TestFormatDispatchHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		dispatches []db.Dispatch
	}{
		{name: "nil dispatches", sessionID: "s", dispatches: nil},
		{name: "empty dispatches", sessionID: "s", dispatches: []db.Dispatch{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatDispatchHistory(tc.sessionID, tc.dispatches)
			if got != "" {
				t.Fatalf("expected empty string, got %q", got)
			}
		})
	}
}

func TestBuildDispatchPrompt(t *testing.T) {
	t.Parallel()
	const directive = "<SYSTEM_DIRECTIVE>\nrun background task\n\nOnly formulate and output your final response once all immediate work is complete. This is a headless dispatch turn; your response will not be delivered to Discord.\n</SYSTEM_DIRECTIVE>"
	runAt := time.Date(2026, 11, 13, 9, 30, 0, 0, time.UTC)

	tests := []struct {
		name          string
		nilTE         bool
		isCold        bool
		sessionID     string
		prevSessionID string
		recent        []string // summaries of completed dispatches already in the session
		failRecent    error
		wantPrompt    string
		wantSpans     []wantPromptSpan
		wantAbsent    []string
	}{
		{name: "nil turnExecution", nilTE: true, wantAbsent: []string{spanPromptRecentDispatches, spanPromptPrevSessionID}},
		{
			name:       "warm dispatch prompt populated",
			sessionID:  "sess-warm",
			recent:     []string{"ignored on warm turns"},
			wantPrompt: directive,
			wantSpans:  []wantPromptSpan{{name: spanPrompt, attrs: []attribute.KeyValue{attrPromptColdStart.Bool(false), attrPromptChars.Int(224)}}},
			wantAbsent: []string{spanPromptRecentDispatches, spanPromptPrevSessionID},
		},
		{
			name:      "cold dispatch loads recent session history",
			isCold:    true,
			sessionID: "sess-cold",
			recent:    []string{"first run", "second run"},
			wantPrompt: "<DISPATCH_HISTORY>\nRecent runs for session sess-cold:\n" +
				"- [2026-11-13T09:30:00Z] status=COMPLETED: first run\n" +
				"- [2026-11-13T09:30:00Z] status=COMPLETED: second run\n" +
				"</DISPATCH_HISTORY>\n\n" + directive,
			wantSpans: []wantPromptSpan{
				{name: spanPrompt, attrs: []attribute.KeyValue{attrPromptColdStart.Bool(true)}},
				{name: spanPromptPrevSessionID, parent: spanPrompt},
				{name: spanPromptRecentDispatches, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptDispatchCount.Int(2)}},
			},
		},
		{
			name:          "cold dispatch survives recent history failure",
			isCold:        true,
			sessionID:     "sess-fail",
			prevSessionID: "prev-sess-1",
			recent:        []string{"never read"},
			failRecent:    errors.New("connection reset"),
			wantPrompt: "<PREVIOUS_SESSION>\nNote: This thread was recently rotated to preserve token budget and optimize context window performance.\n" +
				"Previous conversation ID: prev-sess-1\n" +
				"Prior conversation context, active decisions, and recent channel history are summarized in thread summary and channel message history. " +
				"Semantic facts and user preferences are preserved in long-term memory. " +
				"If continuity details are missing, you may inspect the prior session (prev-sess-1) via transcript-search or thread history.\n" +
				"</PREVIOUS_SESSION>\n\n" + directive,
			wantSpans: []wantPromptSpan{
				{name: spanPromptRecentDispatches, parent: spanPrompt, errored: true},
			},
			wantAbsent: []string{spanPromptPrevSessionID},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			for i, summary := range tc.recent {
				d := db.Dispatch{ID: fmt.Sprintf("%s-%d", tc.sessionID, i), SessionID: tc.sessionID, Status: db.StatusCompleted, Summary: summary, CreatedAt: runAt}
				if _, err := store.InsertDispatch(context.Background(), d); err != nil {
					t.Fatalf("seed dispatch: %v", err)
				}
			}
			if tc.failRecent != nil {
				store.FailNext("GetRecentSessionDispatches", tc.failRecent)
			}
			rec, tp, ctx, root := newPromptBuildRecorder()
			var te *turnExecution
			if !tc.nilTE {
				te = &turnExecution{
					pool:              &WorkerPool{ctx: context.Background(), cfg: WorkerPoolConfig{Store: store, TracerProvider: tp}},
					dispatch:          &db.Dispatch{ID: "d-current", SessionID: tc.sessionID, Prompt: "run background task"},
					isCold:            tc.isCold,
					previousSessionID: tc.prevSessionID,
				}
			}
			got := te.buildDispatchPrompt(ctx)
			root.End()
			if got != tc.wantPrompt {
				t.Errorf("buildDispatchPrompt() =\n%q\nwant\n%q", got, tc.wantPrompt)
			}
			assertPromptSpans(t, rec.Ended(), tc.wantSpans, tc.wantAbsent)
		})
	}
}

func TestCancelBurstOnShutdown_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		failScheduleRun bool
		wantTurnErr     string
	}{
		{name: "shutdown interruption", failScheduleRun: false, wantTurnErr: "interrupted by shutdown"},
		{name: "context cancelled during execution", failScheduleRun: true, wantTurnErr: "context cancelled during execution"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			ctx := context.Background()
			interruptedDisp := db.Dispatch{ID: "disp-interrupted-" + tc.name, Status: db.StatusProcessing}
			_, _ = store.InsertDispatch(ctx, interruptedDisp)

			pool := New(nil, WorkerPoolConfig{Store: store})
			defer pool.Stop()

			te := &turnExecution{
				pool:     pool,
				dispatch: &interruptedDisp,
			}
			te.cancelBurstOnShutdown("failed", tc.failScheduleRun)
			if te.turnError != tc.wantTurnErr {
				t.Fatalf("expected turnError %q, got %q", tc.wantTurnErr, te.turnError)
			}
			if te.turnStatus != "cancelled" {
				t.Fatalf("expected turnStatus %q, got %q", "cancelled", te.turnStatus)
			}

			refreshed, err := store.GetDispatch(ctx, interruptedDisp.ID)
			if err != nil || refreshed == nil {
				t.Fatalf("failed to fetch dispatch: %v", err)
			}
			if refreshed.Status != db.StatusProcessing {
				t.Fatalf("expected interrupted dispatch to remain %s in DB for startup recovery, got %+v", db.StatusProcessing, refreshed)
			}
			if refreshed.RestartCount != 0 {
				t.Fatalf("expected restart count to remain 0 until RecoverInterrupted runs, got %d", refreshed.RestartCount)
			}
		})
	}
}

func TestResolveExecutionPool_NilReceiverAndMissingPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		te      *turnExecution
		wantNil bool
	}{
		{name: "nil turnExecution", te: nil, wantNil: true},
		{name: "te without pool", te: &turnExecution{dispatch: &db.Dispatch{SessionID: "s1"}}, wantNil: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, k, err := tc.te.resolveExecutionPool()
			if (p == nil) != tc.wantNil || err != nil {
				t.Fatalf("unexpected pool or error: pool=%v, key=%+v, err=%v", p, k, err)
			}
		})
	}
}

func TestMakeSessionCallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		closeStore bool
		wantErr    bool
	}{
		{name: "active store", closeStore: false, wantErr: false},
		{name: "closed store", closeStore: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			ctx := context.Background()
			if tc.closeStore {
				_ = store.Close()
			}
			getRec, setRec := makeSessionCallbacks(store)
			_, err := getRec(ctx, "session-test-key")
			if (err != nil) != tc.wantErr {
				t.Fatalf("getRec error mismatch: got %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.closeStore {
				if err := setRec(ctx, "session-test-key", "new-conv", "prev-conv"); err != nil {
					t.Fatalf("setRec unexpected error: %v", err)
				}
			}
		})
	}
}

type mockDaemonGetterPool struct {
	runner.AgentPool

	daemons map[string]*runner.StreamingDaemon
}

func (m *mockDaemonGetterPool) Get(key string) (*runner.StreamingDaemon, bool) {
	if m == nil || m.daemons == nil {
		return nil, false
	}
	d, ok := m.daemons[key]
	return d, ok
}

func TestStreamingDaemonHelpers(t *testing.T) {
	t.Parallel()

	d1 := &runner.StreamingDaemon{}
	d1.SetSessionID("sess-1")
	dNorm := &runner.StreamingDaemon{}
	dNorm.SetSessionID("sess-norm")
	dTrimmed := &runner.StreamingDaemon{}
	dTrimmed.SetSessionID("sess-trimmed")

	poolDirect := &mockDaemonGetterPool{daemons: map[string]*runner.StreamingDaemon{"thread-1": d1}}
	poolNorm := &mockDaemonGetterPool{daemons: map[string]*runner.StreamingDaemon{"discord-12345": dNorm}}
	poolTrimmed := &mockDaemonGetterPool{daemons: map[string]*runner.StreamingDaemon{"12345": dTrimmed}}

	tests := []struct {
		name       string
		pool       runner.AgentPool
		threadID   string
		wantDaemon bool
		wantSessID string
	}{
		{name: "nil pool", pool: nil, threadID: "thread-1", wantDaemon: false, wantSessID: ""},
		{name: "pool missing key", pool: poolDirect, threadID: "non-existent", wantDaemon: false, wantSessID: ""},
		{name: "pool direct match", pool: poolDirect, threadID: "thread-1", wantDaemon: true, wantSessID: "sess-1"},
		{name: "pool normalized discord snowflake fallback", pool: poolNorm, threadID: "12345", wantDaemon: true, wantSessID: "sess-norm"},
		{name: "pool trimmed discord prefix fallback", pool: poolTrimmed, threadID: "discord-12345", wantDaemon: true, wantSessID: "sess-trimmed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if d := getStreamingDaemon(tc.pool, tc.threadID); (d != nil) != tc.wantDaemon {
				t.Errorf("getStreamingDaemon() got %v, wantDaemon %v", d, tc.wantDaemon)
			}
			if s := daemonSessionID(tc.pool, tc.threadID); s != tc.wantSessID {
				t.Errorf("daemonSessionID() got %q, want %q", s, tc.wantSessID)
			}
		})
	}
}

func TestWorkerPool_ProcessPoolAccessors(t *testing.T) {
	t.Parallel()

	mockUp := &runner.UnifiedProcessPool{}
	tests := []struct {
		name       string
		pool       *WorkerPool
		wantLowNil bool
		wantMedNil bool
	}{
		{
			name:       "nil pools in WorkerPool",
			pool:       &WorkerPool{},
			wantLowNil: true,
			wantMedNil: true,
		},
		{
			name: "custom configured pools",
			pool: &WorkerPool{
				pools: map[PoolKey]runner.AgentPool{
					{Ephemeral: false, Effort: "low"}:    mockUp,
					{Ephemeral: false, Effort: "medium"}: mockUp,
				},
			},
			wantLowNil: false,
			wantMedNil: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			low := tc.pool.LowEffortProcessPool()
			if (low == nil) != tc.wantLowNil {
				t.Errorf("LowEffortProcessPool() nil = %v, want %v", low == nil, tc.wantLowNil)
			}
			med := tc.pool.MediumEffortProcessPool()
			if (med == nil) != tc.wantMedNil {
				t.Errorf("MediumEffortProcessPool() nil = %v, want %v", med == nil, tc.wantMedNil)
			}
		})
	}
}

func TestExtractVerifiedDiskSessionID(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sessMgr := session.New(tempDir, "")
	validSessID := "11111111-1111-1111-1111-111111111111"
	missingSessID := "22222222-2222-2222-2222-222222222222"
	sessDir, _ := ensureTestSessionDir(sessMgr, validSessID)
	_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("{\"step_index\":0}\n"), 0o644)

	pool := &WorkerPool{sessionMgr: sessMgr}

	tests := []struct {
		name   string
		pool   *WorkerPool
		stdout string
		stderr string
		want   string
	}{
		{name: "empty strings", pool: pool, stdout: "", stderr: "", want: ""},
		{name: "session exists on disk", pool: pool, stdout: `{"conversation_id":"` + validSessID + `"}`, want: validSessID},
		{name: "session does not exist on disk", pool: pool, stdout: `{"conversation_id":"` + missingSessID + `"}`, want: ""},
		{name: "nil pool returns empty", pool: nil, stdout: `{"conversation_id":"` + validSessID + `"}`, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := &turnExecution{pool: tc.pool}
			if got := te.extractVerifiedDiskSessionID(tc.stdout, tc.stderr); got != tc.want {
				t.Fatalf("extractVerifiedDiskSessionID() got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFinalizeVoiceTurnOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cancelCtx  bool
		outcome    runner.TurnOutcome
		wantResp   string
		wantErrIs  error
		checkVoice bool
	}{
		{
			name:       "success outcome",
			cancelCtx:  false,
			outcome:    runner.TurnOutcome{IsSuccess: true, Response: "Hello world. How are you?"},
			wantResp:   "Hello world. How are you?",
			wantErrIs:  nil,
			checkVoice: true,
		},
		{
			name:       "cancelled context",
			cancelCtx:  true,
			outcome:    runner.TurnOutcome{IsSuccess: false},
			wantResp:   "",
			wantErrIs:  context.Canceled,
			checkVoice: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}

			sink := runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{})
			det := NewSentenceDetector()
			var heardSentences []string
			sentenceCb := func(s string) { heardSentences = append(heardSentences, s) }

			resp, _, err := finalizeVoiceTurnOutcome(ctx, tc.outcome, nil, "v-sess", "gemini", time.Now(), sink, det, sentenceCb)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp != tc.wantResp {
				t.Fatalf("got response %q, want %q", resp, tc.wantResp)
			}
			if tc.checkVoice && len(heardSentences) == 0 {
				t.Fatalf("expected sentences to be detected")
			}
		})
	}
}

func TestProbeCandidateSessionID(t *testing.T) {
	t.Parallel()

	dNorm := &runner.StreamingDaemon{}
	dNorm.SetSessionID("sess-norm-daemon")
	poolNorm := &mockDaemonGetterPool{daemons: map[string]*runner.StreamingDaemon{"discord-12345": dNorm}}

	tests := []struct {
		name             string
		threadID         string
		activePool       runner.AgentPool
		currentSessionID string
		candidate        string
		want             string
	}{
		{name: "direct candidate preferred", currentSessionID: "sess-probe-1", candidate: "direct-candidate", want: "direct-candidate"},
		{name: "fallback to current session", currentSessionID: "sess-probe-1", candidate: "", want: "sess-probe-1"},
		{name: "empty fallback", currentSessionID: "", candidate: "", want: ""},
		{name: "fallback to active pool via leaseKey", threadID: "12345", activePool: poolNorm, candidate: "", want: "sess-norm-daemon"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := &turnExecution{threadID: tc.threadID, activePool: tc.activePool, currentSessionID: tc.currentSessionID}
			if got := te.probeCandidateSessionID(tc.candidate, "", ""); got != tc.want {
				t.Fatalf("probeCandidateSessionID() got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_ResolveAttemptConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		burst     []db.Message
		dispatch  *db.Dispatch
		override  string
		isEphem   bool
		lowModel  string
		medModel  string
		wantModel string
	}{
		{
			name:      "dispatch with explicit model overrides effort tier",
			dispatch:  &db.Dispatch{Model: "custom-gemini-pro", Effort: "low"},
			lowModel:  "gemini-flash-low",
			medModel:  "gemini-flash-medium",
			wantModel: "custom-gemini-pro",
		},
		{
			name:      "dispatch with low effort selects low effort model",
			dispatch:  &db.Dispatch{Effort: "low"},
			lowModel:  "gemini-flash-low",
			medModel:  "gemini-flash-medium",
			wantModel: "gemini-flash-low",
		},
		{
			name:      "dispatch with medium effort selects medium effort model",
			dispatch:  &db.Dispatch{Effort: "medium"},
			lowModel:  "gemini-flash-low",
			medModel:  "gemini-flash-medium",
			wantModel: "gemini-flash-medium",
		},
		{
			name:      "dispatch ephemeral defaults to medium effort model",
			dispatch:  &db.Dispatch{},
			isEphem:   true,
			lowModel:  "gemini-flash-low",
			medModel:  "gemini-flash-medium",
			wantModel: "gemini-flash-medium",
		},
		{
			name:      "burst message with model override",
			burst:     []db.Message{{Content: "hello", Model: "burst-model"}},
			wantModel: "burst-model",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			appCfg := config.NewFromData(&config.ConfigData{
				LowEffortModel:    tc.lowModel,
				MediumEffortModel: tc.medModel,
			})
			te := &turnExecution{
				burst:       tc.burst,
				dispatch:    tc.dispatch,
				isEphemeral: tc.isEphem,
				pool:        New(appCfg, WorkerPoolConfig{}),
			}
			defer te.pool.Stop()

			cfg := te.resolveAttemptConfig(0)
			if cfg.model != tc.wantModel {
				t.Fatalf("cfg.model = %q, want %q", cfg.model, tc.wantModel)
			}
		})
	}
}

func TestTurnExecution_HandleQuotaPause_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dispatch   *db.Dispatch
		burst      []db.Message
		errDetail  string
		wantStatus string
	}{
		{
			name: "dispatch quota pause does not panic on nil burst",
			dispatch: &db.Dispatch{
				ID:     "disp-quota",
				Prompt: "test prompt",
			},
			errDetail:  "RESOURCE_EXHAUSTED (code 429): quota reset in 30s",
			wantStatus: "failed",
		},
		{
			name: "dispatch retry prompt does not reschedule",
			dispatch: &db.Dispatch{
				ID:     "disp-quota-retry",
				Prompt: "[QUOTA_RETRY] test prompt",
			},
			errDetail:  "RESOURCE_EXHAUSTED (code 429): quota reset in 30s",
			wantStatus: "failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			ctx := context.Background()
			if tc.dispatch != nil {
				_, _ = store.InsertDispatch(ctx, *tc.dispatch)
			}
			pool := New(nil, WorkerPoolConfig{Store: store})
			defer pool.Stop()

			te := &turnExecution{
				pool:       pool,
				dispatch:   tc.dispatch,
				burst:      tc.burst,
				threadID:   "session-1",
				turnPrompt: "test prompt",
				execStart:  time.Now().UTC(),
			}

			// Must not panic with index out of range
			handled := te.handleQuotaPause(tc.errDetail, "", "test-model", "low", 1, 1)
			if handled {
				t.Fatalf("expected handleQuotaPause to return false on capacity exhaustion")
			}
		})
	}
}

func TestResolveDispatchEffort_Matrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		d       db.Dispatch
		isEphem bool
		wantEff string
	}{
		{"explicit valid low", db.Dispatch{Effort: "low"}, false, "low"},
		{"explicit valid medium", db.Dispatch{Effort: "medium"}, false, "medium"},
		{"explicit valid high", db.Dispatch{Effort: "high"}, false, "high"},
		{"explicit high clamped on ephemeral", db.Dispatch{Effort: "high"}, true, "medium"},
		{"infer from model flash", db.Dispatch{Model: "gemini-2.5-flash"}, false, "low"},
		{"infer from model medium", db.Dispatch{Model: "gemini-3.8-flash-medium"}, false, "medium"},
		{"infer from model pro", db.Dispatch{Model: "gemini-2.5-pro"}, false, "high"},
		{"infer from model pro ephemeral clamped", db.Dispatch{Model: "gemini-2.5-pro"}, true, "medium"},
		{"default ephemeral", db.Dispatch{}, true, "medium"},
		{"default persistent", db.Dispatch{}, false, "low"},
		{"invalid effort ephemeral", db.Dispatch{Effort: "extreme"}, true, "medium"},
		{"invalid effort persistent", db.Dispatch{Effort: "extreme"}, false, "low"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveDispatchEffort(tc.d, tc.isEphem)
			if got != tc.wantEff {
				t.Fatalf("got %q, want %q", got, tc.wantEff)
			}
		})
	}
}
