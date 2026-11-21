package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestWorkerPool_Accessors(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	noopLLM := func(context.Context, string, string) (string, error) { return "", nil }
	appCfg := config.NewFromData(&config.ConfigData{NATSURL: "nats://127.0.0.1:4222"})
	tests := []struct {
		name                                                           string
		pool                                                           *WorkerPool
		wantStoreNil, wantSummaryNil, wantAgentPoolNil, wantLLMFuncNil bool
		wantNATSURL                                                    string
		wantAppCfgNil                                                  bool
	}{
		{"nil", nil, true, true, true, true, "", true},
		{"empty", &WorkerPool{}, true, false, true, true, "", true},
		{"with_fake_and_agentpool", &WorkerPool{cfg: WorkerPoolConfig{Store: fake, LLMFunc: noopLLM}, pools: map[PoolKey]runner.AgentPool{{Ephemeral: false, Effort: "high"}: &testRunnerAgentPool{}}}, false, false, false, false, "", true},
		{"with_appcfg_and_nats", &WorkerPool{appCfg: appCfg, cfg: WorkerPoolConfig{NATSURL: "nats://fallback:4222"}}, true, false, true, true, "nats://127.0.0.1:4222", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if (tc.pool.Store() == nil) != tc.wantStoreNil || (tc.pool.SummaryGroup() == nil) != tc.wantSummaryNil || (tc.pool.AgentPool() == nil) != tc.wantAgentPoolNil || (tc.pool.LLMFunc() == nil) != tc.wantLLMFuncNil {
				t.Errorf("accessor mismatch: store=%v summary=%v agent=%v llm=%v", tc.pool.Store() == nil, tc.pool.SummaryGroup() == nil, tc.pool.AgentPool() == nil, tc.pool.LLMFunc() == nil)
			}
			if tc.pool.NATSURL() != tc.wantNATSURL {
				t.Errorf("NATSURL() = %q, want %q", tc.pool.NATSURL(), tc.wantNATSURL)
			}
			if (tc.pool.AppConfig() == nil) != tc.wantAppCfgNil {
				t.Errorf("AppConfig() nil = %v, want %v", tc.pool.AppConfig() == nil, tc.wantAppCfgNil)
			}
		})
	}
}

func TestWorkerPool_ClassifierAlertDebouncing(t *testing.T) {
	t.Parallel()
	appCfg := config.NewFromData(&config.ConfigData{SystemChannel: "alerts", APIKey: "k", AgyBin: "agy"})
	dg, _ := discordgo.New("Bot fake")

	tests := []struct {
		name, model, rawOutput, endpoint, wantParseSubstr, wantSysSubstr string
	}{
		{"TruncatedAndSanitizedOutput", "gemini-flash", strings.Repeat("x", 700) + "\n```json\n@everyone @here\n```", "http://systemone.test:8000/v1/systemone", "... (truncated)", "http://systemone.test:8000/v1/systemone"},
		{"ShortOutputAndMentions", "gemini-pro", "```json\n@everyone @here\n```", "http://127.0.0.1:8000/v1/systemone", "'''json", "http://127.0.0.1:8000/v1/systemone"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var alerts []string
			alertCh := make(chan struct{}, 4)
			pool := New(appCfg, WorkerPoolConfig{
				DiscordSession: dg,
				SystemAlertFunc: func(s *discordgo.Session, ch, title, text string) error {
					mu.Lock()
					alerts = append(alerts, text)
					mu.Unlock()
					alertCh <- struct{}{}
					return nil
				},
			})

			pool.cfg.Classifier.OnParseError(tc.model, tc.rawOutput, errors.New("parse 1"))
			waitForCompletion(t, alertCh, time.Second, tc.name+"/parse")
			pool.cfg.Classifier.OnParseError(tc.model, "duplicate", errors.New("parse 2"))

			pool.cfg.Classifier.OnSystemAlert(tc.endpoint, errors.New("refused 1"))
			waitForCompletion(t, alertCh, time.Second, tc.name+"/sys")
			pool.cfg.Classifier.OnSystemAlert(tc.endpoint, errors.New("refused 2"))

			mu.Lock()
			defer mu.Unlock()
			if len(alerts) != 2 {
				t.Fatalf("expected 2 debounced alerts, got %d", len(alerts))
			}
			if !strings.Contains(alerts[0], tc.wantParseSubstr) {
				t.Errorf("parse alert missing %q", tc.wantParseSubstr)
			}
			if !strings.Contains(alerts[1], tc.wantSysSubstr) {
				t.Errorf("sys alert missing %q", tc.wantSysSubstr)
			}
		})
	}
}

func TestWorkerPool_ResolveChannelID(t *testing.T) {
	t.Parallel()
	mockSess, _ := discordgo.New("Bot fake-token")
	mockSess.State = discordgo.NewState()
	_ = mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "1564150557705248879", Name: "prospera"},
		},
	})
	pool := New(nil, WorkerPoolConfig{DiscordSession: mockSess})
	nilSessPool := New(nil, WorkerPoolConfig{})

	tests := []struct {
		name       string
		pool       *WorkerPool
		input      string
		want       string
		wantErr    bool
		wantErrSub string
	}{
		{name: "nil pool with snowflake", pool: nil, input: "1564150557705248879", want: "1564150557705248879"},
		{name: "nil pool with hash snowflake", pool: nil, input: "#1564150557705248879", want: "1564150557705248879"},
		{name: "nil pool with name", pool: nil, input: "prospera", wantErr: true, wantErrSub: "worker pool is nil"},
		{name: "direct numeric snowflake", pool: pool, input: "1564150557705248879", want: "1564150557705248879"},
		{name: "hash numeric snowflake", pool: pool, input: "#1564150557705248879", want: "1564150557705248879"},
		{name: "whitespace padded snowflake", pool: pool, input: "  1564150557705248879  ", want: "1564150557705248879"},
		{name: "channel name resolved", pool: pool, input: "prospera", want: "1564150557705248879"},
		{name: "hash channel name resolved", pool: pool, input: "#prospera", want: "1564150557705248879"},
		{name: "nil discord session error", pool: nilSessPool, input: "prospera", wantErr: true, wantErrSub: "discord session is nil"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.pool.ResolveChannelID(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveChannelID(%q) error = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if tc.wantErrSub != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSub)) {
				t.Fatalf("ResolveChannelID(%q) error = %v, want error containing %q", tc.input, err, tc.wantErrSub)
			}
			if got != tc.want {
				t.Fatalf("ResolveChannelID(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestWorkerPool_DeliverDirectWithAttachments(t *testing.T) {
	t.Parallel()
	mockPool := New(nil, WorkerPoolConfig{
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, atts []*delivery.Attachment) error {
			if channelID == "error-channel" {
				return errors.New("delivery failed")
			}
			return nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			if channelID == "error-channel" {
				return errors.New("direct delivery failed")
			}
			return nil
		},
	})
	nilHandlerPool := &WorkerPool{}
	nilSessWithHandlerPool := &WorkerPool{
		cfg: WorkerPoolConfig{
			DeliveryWithAttachmentsFunc: nil,
		},
	}

	att := &delivery.Attachment{Filename: "audio.wav", ContentType: "audio/wav", Data: []byte("wavdata")}

	tests := []struct {
		name        string
		pool        *WorkerPool
		channelID   string
		text        string
		attachments []*delivery.Attachment
		wantErr     bool
		wantErrSub  string
	}{
		{name: "nil pool with attachments", pool: nil, channelID: "1564150557705248879", text: "hello", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "worker pool is nil"},
		{name: "nil pool with zero attachments delegates to DeliverDirect", pool: nil, channelID: "1564150557705248879", text: "hello", attachments: nil, wantErr: true, wantErrSub: "worker pool is nil"},
		{name: "zero attachments delegates to DeliverDirect success", pool: mockPool, channelID: "1564150557705248879", text: "text only", attachments: nil, wantErr: false},
		{name: "zero attachments delegates to DeliverDirect error", pool: mockPool, channelID: "error-channel", text: "text only", attachments: nil, wantErr: true, wantErrSub: "direct delivery failed"},
		{name: "valid delivery with attachments success", pool: mockPool, channelID: "1564150557705248879", text: "with audio", attachments: []*delivery.Attachment{att}, wantErr: false},
		{name: "valid delivery with attachments error", pool: mockPool, channelID: "error-channel", text: "with audio", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "delivery failed"},
		{name: "nil discord session and nil handler returns error", pool: nilHandlerPool, channelID: "1564150557705248879", text: "hello", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "discord session is not connected"},
		{name: "nil discord session with nil handler on struct", pool: nilSessWithHandlerPool, channelID: "1564150557705248879", text: "hello", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "discord session is not connected"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.pool.DeliverDirectWithAttachments(tc.channelID, tc.text, tc.attachments)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeliverDirectWithAttachments error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErrSub != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSub)) {
				t.Fatalf("DeliverDirectWithAttachments error = %v, want error containing %q", err, tc.wantErrSub)
			}
		})
	}
}

func TestWorkerPool_ActiveTurnSessionID(t *testing.T) {
	t.Parallel()
	leaseWithSess := &testSessionLease{sessionID: "lease-ephem-sess-1"}
	emptyLease := &testSessionLease{sessionID: ""}

	tests := []struct {
		name       string
		nilPool    bool
		msgID      string
		activeTurn *turnExecution
		wantSess   string
		wantOK     bool
	}{
		{name: "nil pool", nilPool: true, msgID: "msg-1", wantSess: "", wantOK: false},
		{name: "empty msgID", msgID: "", wantSess: "", wantOK: false},
		{name: "unmatched msgID", msgID: "msg-missing", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-other"}}, activeSessionID: "sess-1"}, wantSess: "", wantOK: false},
		{name: "matched activeSessionID priority", msgID: "msg-1", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-1"}}, activeSessionID: "active-sess-1", leaseSessionID: "lease-ephem-sess-1", lease: leaseWithSess}, wantSess: "active-sess-1", wantOK: true},
		{name: "matched ephemeral lease session ID", msgID: "msg-2", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-2"}}, leaseSessionID: "lease-ephem-sess-1", lease: leaseWithSess, isEphemeral: true, currentSessionID: "parent-sess-ignored"}, wantSess: "lease-ephem-sess-1", wantOK: true},
		{name: "matched stateful currentSessionID fallback", msgID: "msg-3", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-3"}}, lease: emptyLease, isEphemeral: false, currentSessionID: "stateful-sess-1"}, wantSess: "stateful-sess-1", wantOK: true},
		{name: "matched ephemeral pre-lease suppresses parent session", msgID: "msg-4", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-4"}}, lease: emptyLease, isEphemeral: true, currentSessionID: "parent-sess-ignored"}, wantSess: "", wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p *WorkerPool
			if !tc.nilPool {
				p = New(nil, WorkerPoolConfig{})
				if tc.activeTurn != nil {
					p.registerActiveTurn("scope-1", tc.activeTurn)
				}
			}
			gotSess, gotOK := p.ActiveTurnSessionID(tc.msgID)
			if gotSess != tc.wantSess || gotOK != tc.wantOK {
				t.Fatalf("ActiveTurnSessionID(%q) = (%q, %v), want (%q, %v)", tc.msgID, gotSess, gotOK, tc.wantSess, tc.wantOK)
			}
		})
	}
}

func TestTurnExecution_FailureResetsActiveSessionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		isCorruption  bool
		wantActive    string
		wantLeaseSess string
	}{
		{name: "corruption retry clears active and lease session", isCorruption: true, wantActive: "", wantLeaseSess: ""},
		{name: "non-transient failure clears active and lease session", isCorruption: false, wantActive: "", wantLeaseSess: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			pool := New(nil, WorkerPoolConfig{Store: store, BackoffBase: time.Nanosecond})
			te := &turnExecution{
				pool:              pool,
				threadID:          "th-1",
				burst:             []db.Message{{ID: "m-1", ThreadID: "th-1"}},
				activeSessionID:   "stale-active-sess",
				leaseSessionID:    "stale-lease-sess",
				currentSessionID:  "curr-sess",
				previousSessionID: "prev-sess",
			}
			if tc.isCorruption {
				te.handleSessionCorruptionRetry(1, 2, "", "", "corrupted", "", "model-1")
			} else {
				te.handleNonTransientFailure(1, 1, "", "", "fatal error", "", "model-1")
			}
			te.activeSessionMu.Lock()
			gotActive := te.activeSessionID
			gotLease := te.leaseSessionID
			te.activeSessionMu.Unlock()
			if gotActive != tc.wantActive || gotLease != tc.wantLeaseSess {
				t.Fatalf("after failure, activeSessionID=%q leaseSessionID=%q, want (%q, %q)", gotActive, gotLease, tc.wantActive, tc.wantLeaseSess)
			}
		})
	}
}

type dispatchExecutionHeadlessCase struct {
	name         string
	source       string
	sessionID    string
	prompt       string
	respJSON     string
	wantStatus   string
	wantRespText string
}

func setupHeadlessWorkerPool(t *testing.T, tc dispatchExecutionHeadlessCase, store db.Store, doneCh chan struct{}, bus pubsub.Bus) *WorkerPool {
	t.Helper()
	return NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
		EventBus:       bus,
		DeliveryFunc: func(s *discordgo.Session, c, text string) error {
			t.Errorf("unexpected Discord DeliveryFunc call for headless dispatch")
			return nil
		},
		TypingFunc: func(s *discordgo.Session, c string) func() {
			t.Errorf("unexpected Discord TypingFunc call for headless dispatch")
			return func() {}
		},
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			defer close(doneCh)
			return tc.respJSON, "", 0, nil
		},
	})
}

func assertHeadlessDispatchResult(t *testing.T, store db.Store, dispID string, tc dispatchExecutionHeadlessCase) {
	t.Helper()
	d, err := store.GetDispatch(context.Background(), dispID)
	if err != nil || d == nil {
		t.Fatalf("failed to fetch dispatch: %v", err)
	}
	if d.Status != tc.wantStatus {
		t.Fatalf("dispatch %s status mismatch: got %s, want %s", dispID, d.Status, tc.wantStatus)
	}
	if d.ResponseText != tc.wantRespText {
		t.Fatalf("response text mismatch: got %q, want %q", d.ResponseText, tc.wantRespText)
	}
}

func runDispatchExecutionHeadlessCase(t *testing.T, tc dispatchExecutionHeadlessCase) {
	t.Helper()
	store := dbtest.NewFakeStore()
	doneCh := make(chan struct{})

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(context.Background(), memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	pool := setupHeadlessWorkerPool(t, tc, store, doneCh, memBus)
	defer pool.Stop()

	disp := db.Dispatch{
		ID:        uuid.New().String(),
		Source:    tc.source,
		SessionID: tc.sessionID,
		Prompt:    tc.prompt,
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	_, _ = store.InsertDispatch(context.Background(), disp)
	pool.EnqueueDispatch(disp)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for dispatch runner execution")
	}

	waitPoolIdle(t, pool, 3*time.Second)
	pool.Stop()

	assertHeadlessDispatchResult(t, store, disp.ID, tc)
}

func TestDispatchExecution_HeadlessAndNoDiscordCalls(t *testing.T) {
	t.Parallel()
	tests := []dispatchExecutionHeadlessCase{
		{
			name:         "ephemeral vanadis chunk",
			source:       "vanadis",
			sessionID:    "",
			prompt:       "[VANADIS BUILD CHUNK EXECUTION] Chunk 2",
			respJSON:     `{"conversation_id":"99999999-9999-9999-9999-999999999999","status":"SUCCESS","response":"chunk 2 passed"}`,
			wantStatus:   db.StatusCompleted,
			wantRespText: "chunk 2 passed",
		},
		{
			name:         "stateful heartbeat run",
			source:       "heartbeat",
			sessionID:    "heartbeat-health",
			prompt:       "run health heartbeat",
			respJSON:     `{"conversation_id":"88888888-8888-8888-8888-888888888888","status":"SUCCESS","response":"all systems normal"}`,
			wantStatus:   db.StatusCompleted,
			wantRespText: "all systems normal",
		},
		{
			name:         "vanadis chunk execution completes successfully",
			source:       "vanadis",
			sessionID:    "vanadis-chunk-step-err",
			prompt:       "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-err\nChunk ID: chunk-step-err",
			respJSON:     `{"conversation_id":"77777777-7777-7777-7777-777777777777","status":"SUCCESS","response":"done"}`,
			wantStatus:   db.StatusCompleted,
			wantRespText: "done",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchExecutionHeadlessCase(t, tc)
		})
	}
}

func TestDispatchExecution_ColdStartAndRotation(t *testing.T) {
	t.Parallel()
	store := dbtest.NewFakeStore()
	ctx := context.Background()

	_, _ = store.InsertDispatch(ctx, db.Dispatch{
		ID:        "prev-run-1",
		Source:    "heartbeat",
		SessionID: "heartbeat-rot",
		Prompt:    "initial check",
		Summary:   "checked cluster status",
		Status:    db.StatusCompleted,
		CreatedAt: time.Now().UTC().Add(-10 * time.Minute),
	})

	var capturedPrompt string
	var promptMu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			promptMu.Lock()
			capturedPrompt = prompt
			promptMu.Unlock()
			defer close(doneCh)
			return `{"conversation_id":"77777777-7777-7777-7777-777777777777","status":"SUCCESS","response":"done"}`, "", 0, nil
		},
	})
	defer pool.Stop()

	for range session.DefaultMaxSessionTurns - 1 {
		_, _ = store.IncrementSessionTurnCount(ctx, "heartbeat-rot")
	}

	disp := db.Dispatch{
		ID:        "disp-rot-test",
		Source:    "heartbeat",
		SessionID: "heartbeat-rot",
		Prompt:    "hourly maintenance",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	_, _ = store.InsertDispatch(ctx, disp)
	pool.EnqueueDispatch(disp)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for dispatch execution")
	}

	promptMu.Lock()
	p := capturedPrompt
	promptMu.Unlock()

	if !strings.Contains(p, "<DISPATCH_HISTORY>") || !strings.Contains(p, "Recent runs for session heartbeat-rot:") {
		t.Errorf("expected <DISPATCH_HISTORY> in cold-start dispatch prompt, got:\n%s", p)
	}
	if !strings.Contains(p, "<SYSTEM_DIRECTIVE>") || !strings.Contains(p, "This is a headless dispatch turn; your response will not be delivered to Discord.") {
		t.Errorf("expected <SYSTEM_DIRECTIVE> in prompt, got:\n%s", p)
	}

	waitPoolIdle(t, pool, 3*time.Second)
	pool.Stop()

	count, _ := store.GetSessionTurnCount(ctx, "heartbeat-rot")
	if count != 0 {
		t.Fatalf("expected session turn count to be reset after rotation, but still > 0: %d", count)
	}
}

func TestDispatchExecution_ScopeLockIsolation(t *testing.T) {
	t.Parallel()
	store := dbtest.NewFakeStore()

	dispatchRunning := make(chan struct{})
	dispatchCanFinish := make(chan struct{})
	messageFinished := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
		DeliveryFunc: func(s *discordgo.Session, c, text string) error {
			return nil
		},
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			if strings.Contains(prompt, "headless-dispatch") {
				close(dispatchRunning)
				<-dispatchCanFinish
				return `{"conversation_id":"66666666-6666-6666-6666-666666666666","status":"SUCCESS","response":"ok"}`, "", 0, nil
			}
			close(messageFinished)
			return `{"conversation_id":"55555555-5555-5555-5555-555555555555","status":"SUCCESS","response":"reply"}`, "", 0, nil
		},
	})
	defer pool.Stop()

	disp := db.Dispatch{
		ID:        "disp-iso-1",
		Source:    "heartbeat",
		SessionID: "heartbeat-scope",
		Prompt:    "headless-dispatch running",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_, _ = store.InsertDispatch(context.Background(), disp)
	pool.EnqueueDispatch(disp)

	<-dispatchRunning

	msg := db.Message{
		ID:        "msg-iso-1",
		ThreadID:  "1542423172400291873",
		ChannelID: "1542423172400291873",
		Content:   "concurrent discord turn",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-messageFinished:
	case <-time.After(2 * time.Second):
		t.Fatalf("discord message turn contended on dispatch scope lock")
	}

	close(dispatchCanFinish)
}

func TestRecoverInterrupted_Dispatches(t *testing.T) {
	t.Parallel()
	store := dbtest.NewFakeStore()
	ctx := context.Background()

	_, _ = store.InsertDispatch(ctx, db.Dispatch{
		ID:        "disp-rec-pending",
		Source:    "heartbeat",
		SessionID: "heartbeat-rec-1",
		Prompt:    "pending prompt",
		Status:    db.StatusPending,
	})
	_, _ = store.InsertDispatch(ctx, db.Dispatch{
		ID:        "disp-rec-processing",
		Source:    "vanadis",
		SessionID: "",
		Prompt:    "processing prompt",
		Status:    db.StatusProcessing,
	})
	_, _ = store.InsertDispatch(ctx, db.Dispatch{
		ID:           "disp-rec-poison",
		Source:       "webhook",
		SessionID:    "webhook-poison",
		Prompt:       "poison prompt",
		Status:       db.StatusProcessing,
		RestartCount: DefaultMaxRestarts + 1,
	})

	var recoveredIDs []string
	var mu sync.Mutex
	doneWg := sync.WaitGroup{}
	doneWg.Add(2)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	outcomePublished := make(chan pubsub.DispatchOutcomeEvent, 1)
	_, _ = memBus.Subscribe(ctx, "aerial.dispatch.failed", "test-sub", func(msgCtx context.Context, msg pubsub.Message) error {
		var ev pubsub.DispatchOutcomeEvent
		if uErr := json.Unmarshal(msg.Data(), &ev); uErr == nil {
			select {
			case outcomePublished <- ev:
			default:
			}
		}
		return msg.Ack()
	})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
		EventBus:       memBus,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			if strings.Contains(prompt, "pending prompt") {
				recoveredIDs = append(recoveredIDs, "disp-rec-pending")
				doneWg.Done()
			} else if strings.Contains(prompt, "processing prompt") {
				recoveredIDs = append(recoveredIDs, "disp-rec-processing")
				doneWg.Done()
			}
			mu.Unlock()
			return `{"conversation_id":"44444444-4444-4444-4444-444444444444","status":"SUCCESS","response":"ok"}`, "", 0, nil
		},
	})
	defer pool.Stop()

	RecoverInterrupted(t.Context(), store, pool)

	doneWg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(recoveredIDs) != 2 {
		t.Fatalf("expected 2 recovered dispatches, got %d (%v)", len(recoveredIDs), recoveredIDs)
	}

	poisonDisp, err := store.GetDispatch(ctx, "disp-rec-poison")
	if err != nil || poisonDisp == nil || poisonDisp.Status != db.StatusFailed {
		t.Fatalf("expected poison pill dispatch to be marked FAILED, got %+v", poisonDisp)
	}

	var poisonEv pubsub.DispatchOutcomeEvent
	select {
	case poisonEv = <-outcomePublished:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for poison pill dispatch outcome event")
	}

	if poisonEv.DispatchID != "disp-rec-poison" || poisonEv.Status != "failed" {
		t.Errorf("unexpected delivered event: %+v", poisonEv)
	}
}

func TestDispatchExecution_RetriesAndFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		source         string
		sessionID      string
		prompt         string
		wantStatus     string
		wantMinRetries int
	}{
		{
			name:           "heartbeat dispatch exhausts retries to failed status",
			source:         "heartbeat",
			sessionID:      "heartbeat-failing",
			prompt:         "fail this dispatch",
			wantStatus:     db.StatusFailed,
			wantMinRetries: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			ctx := context.Background()

			disp := db.Dispatch{
				ID:        uuid.New().String(),
				Source:    tc.source,
				SessionID: tc.sessionID,
				Prompt:    tc.prompt,
				Status:    db.StatusPending,
			}
			_, _ = store.InsertDispatch(ctx, disp)

			exhausted := make(chan struct{})
			var attempts atomic.Int32
			pool := New(nil, WorkerPoolConfig{
				Store:          store,
				MaxAttempts:    2,
				BackoffBase:    1 * time.Millisecond,
				TimeoutMinutes: 1,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					if attempts.Add(1) == 2 {
						close(exhausted)
					}
					return "", "fatal error", 1, errors.New("simulated dispatch execution error")
				},
			})
			defer pool.Stop()

			pool.EnqueueDispatch(disp)

			select {
			case <-exhausted:
			case <-time.After(3 * time.Second):
				t.Fatalf("timeout waiting for retries to exhaust")
			}

			waitPoolIdle(t, pool, 3*time.Second)
			pool.Stop()

			d, err := store.GetDispatch(ctx, disp.ID)
			if err != nil || d == nil {
				t.Fatalf("failed to get dispatch %s: %v", disp.ID, err)
			}
			if d.Status != tc.wantStatus {
				t.Fatalf("dispatch %s did not reach status %s after retries, got %s", disp.ID, tc.wantStatus, d.Status)
			}
			if d.RetryCount < tc.wantMinRetries {
				t.Fatalf("expected retry_count >= %d, got %d", tc.wantMinRetries, d.RetryCount)
			}
		})
	}
}
