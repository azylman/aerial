package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

type mockErrSessionStore struct {
	db.Store

	findErr error
}

func (m *mockErrSessionStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]db.SessionInfo, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}
	if m.Store != nil {
		return m.Store.FindUnrotatedSessions(ctx, minTurns, maxAge...)
	}
	return nil, nil
}

type checkUnrotatedCase struct {
	name                                string
	nilPool, nilStore, cancelContext    bool
	storeErr                            error
	dormantBackdate                     time.Duration
	sessionTurns                        []int
	expectAlertCalls, expectReportCount int
	expectOverflowMsg                   string
}

func setupCheckUnrotatedStore(tc checkUnrotatedCase) (db.Store, func()) {
	if tc.storeErr != nil {
		return &mockErrSessionStore{findErr: tc.storeErr}, func() {}
	}
	fake := dbtest.NewFakeStore()
	for i, turns := range tc.sessionTurns {
		thID := fmt.Sprintf("th-scen-%d", i+1)
		_ = fake.SaveSessionID(context.Background(), thID, fmt.Sprintf("sess-scen-%d", i+1))
		for range turns {
			_, _ = fake.IncrementSessionTurnCount(context.Background(), thID)
		}
		if tc.dormantBackdate > 0 {
			fake.SetSessionUpdatedAt(thID, time.Now().UTC().Add(-tc.dormantBackdate))
		}
	}
	return fake, func() { _ = fake.Close() }
}

func runCheckUnrotatedSessionsCase(t *testing.T, tc checkUnrotatedCase) {
	t.Helper()
	if tc.nilPool {
		var nilPool *WorkerPool
		nilPool.checkUnrotatedSessions(context.Background(), make(map[string]int))
		return
	}
	if tc.nilStore {
		(&WorkerPool{}).checkUnrotatedSessions(context.Background(), make(map[string]int))
		return
	}
	ctx := context.Background()
	if tc.cancelContext {
		cCtx, cancel := context.WithCancel(ctx)
		cancel()
		ctx = cCtx
	}
	var mu sync.Mutex
	var alertCalls int
	var lastBody string
	recordAlert := func(s *discordgo.Session, ch, title, body string) error {
		mu.Lock()
		alertCalls++
		lastBody = body
		mu.Unlock()
		return nil
	}

	store, cleanup := setupCheckUnrotatedStore(tc)
	defer cleanup()

	pool := &WorkerPool{cfg: WorkerPoolConfig{Store: store, SystemAlertFunc: recordAlert}}
	lastReported := make(map[string]int)
	pool.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	calls, body := alertCalls, lastBody
	mu.Unlock()
	if calls != tc.expectAlertCalls {
		t.Fatalf("expected %d alert calls, got %d", tc.expectAlertCalls, calls)
	}
	if tc.expectReportCount > 0 && len(lastReported) != tc.expectReportCount {
		t.Fatalf("expected %d recorded, got %d", tc.expectReportCount, len(lastReported))
	}
	if tc.expectOverflowMsg != "" && !strings.Contains(body, tc.expectOverflowMsg) {
		t.Fatalf("overflow message missing in:\n%s", body)
	}
}

func TestWorkerPool_CheckUnrotatedSessions_Scenarios(t *testing.T) {
	t.Parallel()
	tests := []checkUnrotatedCase{
		{name: "nil worker pool", nilPool: true},
		{name: "nil store", nilStore: true},
		{name: "store error handled gracefully", storeErr: errors.New("database connection failed")},
		{name: "cancelled context exits early", cancelContext: true},
		{name: "excludes dormant sessions older than horizon", sessionTurns: []int{15}, dormantBackdate: 3 * time.Hour},
		{name: "top 5 display cap with overflow message", sessionTurns: []int{10, 11, 12, 13, 14, 15, 16}, expectAlertCalls: 1, expectReportCount: 7, expectOverflowMsg: "...and 2 more runaway session(s)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCheckUnrotatedSessionsCase(t, tc)
		})
	}
}

func TestWorkerPool_CheckUnrotatedSessions_DeduplicationAndPruning(t *testing.T) {
	t.Parallel()
	fake := dbtest.NewFakeStore()
	defer func() { _ = fake.Close() }()
	ctx := context.Background()
	var mu sync.Mutex
	var alertCalls int
	var lastTitle, lastBody string
	recordAlert := func(s *discordgo.Session, ch, title, body string) error {
		mu.Lock()
		alertCalls++
		lastTitle, lastBody = title, body
		mu.Unlock()
		return nil
	}
	p := &WorkerPool{cfg: WorkerPoolConfig{Store: fake, SystemAlertFunc: recordAlert}}
	lastReported := make(map[string]int)

	_ = fake.SaveSessionID(ctx, "th-test", "sess-1")
	for range 5 {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls := alertCalls
	mu.Unlock()
	if calls != 0 || len(lastReported) != 0 {
		t.Fatalf("expected 0 alerts, got %d", calls)
	}

	for range 4 {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls, body, title := alertCalls, lastBody, lastTitle
	mu.Unlock()
	if calls != 1 || !strings.Contains(body, "th-test") || !strings.Contains(body, "**9** turns") || title != "Unrotated Runaway Sessions" {
		t.Fatalf("unexpected alert: calls=%d body=%s title=%s", calls, body, title)
	}
	if lastReported["th-test"] != 9 {
		t.Errorf("expected 9, got %d", lastReported["th-test"])
	}

	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls = alertCalls
	mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls, body = alertCalls, lastBody
	mu.Unlock()
	if calls != 2 || !strings.Contains(body, "**10** turns") {
		t.Fatalf("expected 2 calls, got %d body=%s", calls, body)
	}

	_ = fake.RotateSessionID(ctx, "th-test", "sess-fresh")
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls = alertCalls
	mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
	if _, exists := lastReported["th-test"]; exists {
		t.Errorf("expected pruned: %v", lastReported)
	}
}

func TestWorkerPool_MaintenanceTicker_FiresCheck(t *testing.T) {
	t.Parallel()
	fake := dbtest.NewFakeStore()
	defer func() { _ = fake.Close() }()
	ctx := context.Background()
	_ = fake.SaveSessionID(ctx, "th-ticker-test", "sess-ticker")
	for range 11 {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-ticker-test")
	}

	alertCh := make(chan string, 5)
	p := NewWorkerPool(WorkerPoolConfig{
		Store: fake, MaintenanceInterval: 10 * time.Millisecond,
		SystemAlertFunc: func(s *discordgo.Session, ch, title, body string) error {
			select {
			case alertCh <- body:
			default:
			}
			return nil
		},
	})
	p.Start()
	select {
	case body := <-alertCh:
		if !strings.Contains(body, "th-ticker-test") || !strings.Contains(body, "**11** turns") {
			t.Errorf("unexpected alert body: %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for maintenance ticker")
	}
	p.Stop()
}

func TestFormatUnrotatedSessionLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		cachedChans []*discordgo.Channel
		session     db.SessionInfo
		expected    string
	}{
		{name: "non-numeric fallback", session: db.SessionInfo{ThreadID: "th-custom", TurnCount: 9, InternalSessionID: "sess-1"}, expected: "- Thread `th-custom`: **9** turns (session: `sess-1`)"},
		{name: "channel snowflake", cachedChans: []*discordgo.Channel{{ID: "1464358486849622120", Type: discordgo.ChannelTypeGuildText, Name: "general"}}, session: db.SessionInfo{ThreadID: "1464358486849622120", TurnCount: 10, InternalSessionID: "sess-chan"}, expected: "- Channel <#1464358486849622120>: **10** turns (session: `sess-chan`)"},
		{name: "thread with parent", cachedChans: []*discordgo.Channel{{ID: "1542423172400291873", Type: discordgo.ChannelTypeGuildText, Name: "aerial-dev"}, {ID: "1557949887948005398", ParentID: "1542423172400291873", Type: discordgo.ChannelTypeGuildPublicThread, Name: "Thread"}}, session: db.SessionInfo{ThreadID: "1557949887948005398", TurnCount: 12, InternalSessionID: "sess-th"}, expected: "- Thread <#1557949887948005398> (in <#1542423172400291873>): **12** turns (session: `sess-th`)"},
		{name: "orphan thread", cachedChans: []*discordgo.Channel{{ID: "1557949887948005399", Type: discordgo.ChannelTypeGuildPublicThread, Name: "Orphan"}}, session: db.SessionInfo{ThreadID: "1557949887948005399", TurnCount: 15, InternalSessionID: "sess-orph"}, expected: "- Thread <#1557949887948005399>: **15** turns (session: `sess-orph`)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, ch := range tc.cachedChans {
				CacheDiscordChannel(ch)
				defer InvalidateChannelCache(ch.ID)
			}
			if got := formatUnrotatedSessionLine(nil, tc.session); got != tc.expected {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestWorkerPool_Accessors(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	noopLLM := func(context.Context, string, string) (string, error) { return "", nil }
	tests := []struct {
		name                                                           string
		pool                                                           *WorkerPool
		wantStoreNil, wantSummaryNil, wantAgentPoolNil, wantLLMFuncNil bool
	}{
		{"nil", nil, true, true, true, true},
		{"empty", &WorkerPool{}, true, false, true, true},
		{"with_fake_and_agentpool", &WorkerPool{cfg: WorkerPoolConfig{Store: fake, LLMFunc: noopLLM}, pools: map[PoolKey]runner.AgentPool{{Ephemeral: false, Effort: "high"}: &testRunnerAgentPool{}}}, false, false, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if (tc.pool.Store() == nil) != tc.wantStoreNil || (tc.pool.SummaryGroup() == nil) != tc.wantSummaryNil || (tc.pool.AgentPool() == nil) != tc.wantAgentPoolNil || (tc.pool.LLMFunc() == nil) != tc.wantLLMFuncNil {
				t.Errorf("accessor mismatch: store=%v summary=%v agent=%v llm=%v", tc.pool.Store() == nil, tc.pool.SummaryGroup() == nil, tc.pool.AgentPool() == nil, tc.pool.LLMFunc() == nil)
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
		{"TruncatedAndSanitizedOutput", "gemini-flash", strings.Repeat("x", 700) + "\n```json\n@everyone @here\n```", "http://192.168.1.70:8000/v1/systemone", "... (truncated)", "http://192.168.1.70:8000/v1/systemone"},
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
			pool := New(nil, WorkerPoolConfig{Store: store})
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
	name               string
	source             string
	sessionID          string
	prompt             string
	respJSON           string
	wantStatus         string
	wantRespText       string
	seedWidget         bool
	coldWidgetSync     bool
	seedOutdatedWidget bool
	syncConnErr        bool
	syncBadURL         bool
	emitStep           bool
	wantSyncedRepo     string
	wantSyncedPRNumber int
}

type headlessSyncedPayload struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
}

func setupHeadlessRouterServer(poolPtr **WorkerPool, payload *headlessSyncedPayload, widgetDeliveredCh chan struct{}, syncConnErr bool) *httptest.Server {
	routerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(payload)
		if *poolPtr != nil {
			_, _ = (*poolPtr).DeliverStatusWidget(discord.StatusWidgetRequest{
				ChannelID: "chan-build",
				ID:        "build-streaming-live",
				MessageID: "widget-msg-seed",
				Status:    "running",
				Chunks: []discord.StatusWidgetChunk{
					{ID: "build-streaming-live-chunk-1", Index: 0, Title: "Step Streaming", Status: "failed", PRNumber: 733},
				},
			})
		}
		select {
		case <-widgetDeliveredCh:
		default:
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	if syncConnErr {
		routerSrv.Close()
	}
	return routerSrv
}

func seedHeadlessWidget(pool *WorkerPool, seedOutdated bool, widgetDeliveredCh chan struct{}) {
	seedStatus := "running"
	seedChunkID := "chunk-step-live"
	if seedOutdated {
		seedStatus = "pr_opened"
		seedChunkID = "build-streaming-live-chunk-1"
	}
	_, _ = pool.DeliverStatusWidget(discord.StatusWidgetRequest{
		ChannelID: "chan-build",
		ID:        "build-streaming-live",
		Status:    "running",
		Chunks: []discord.StatusWidgetChunk{
			{ID: seedChunkID, Index: 0, Title: "Step Streaming", Status: seedStatus},
		},
	})
	// Drain the notification from the seed delivery so that RunnerWithOptionsFunc waits for the active step update.
	select {
	case <-widgetDeliveredCh:
	default:
	}
}

func assertDeliveredWidgetsStep(t *testing.T, deliveredWidgets []discord.StatusWidgetRequest) {
	t.Helper()
	stepsSeen := 0
	hasFinalClear := false
	for i, w := range deliveredWidgets {
		t.Logf("widget[%d]: Chunks=%+v", i, w.Chunks)
		if len(w.Chunks) == 0 {
			continue
		}
		if strings.Contains(w.Chunks[0].ActiveStep, "Executing `git`") {
			stepsSeen++
		}
		if stepsSeen > 0 && w.Chunks[0].ActiveStep == "" {
			hasFinalClear = true
		}
	}
	if stepsSeen == 0 {
		summaries := make([]string, 0, len(deliveredWidgets))
		for _, w := range deliveredWidgets {
			summaries = append(summaries, fmt.Sprintf("(Chunks=%+v)", w.Chunks))
		}
		t.Fatalf("expected at least one widget delivery with active step 'Executing `git`', saw %d widget updates: %s", len(deliveredWidgets), strings.Join(summaries, "; "))
	}
	if !hasFinalClear {
		t.Fatalf("expected final widget clear with ActiveStep='', saw %d widget updates", len(deliveredWidgets))
	}
}

func setupHeadlessWorkerPool(t *testing.T, tc dispatchExecutionHeadlessCase, store db.Store, routerURL string, deliveredWidgetsMu *sync.Mutex, deliveredWidgets *[]discord.StatusWidgetRequest, widgetDeliveredCh chan struct{}, doneCh chan struct{}) *WorkerPool {
	t.Helper()
	return NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
		SynapseURL:     routerURL,
		DeliveryFunc: func(s *discordgo.Session, c, text string) error {
			t.Errorf("unexpected Discord DeliveryFunc call for headless dispatch")
			return nil
		},
		TypingFunc: func(s *discordgo.Session, c string) func() {
			t.Errorf("unexpected Discord TypingFunc call for headless dispatch")
			return func() {}
		},
		WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
			deliveredWidgetsMu.Lock()
			*deliveredWidgets = append(*deliveredWidgets, req)
			deliveredWidgetsMu.Unlock()
			if len(req.Chunks) > 0 && strings.Contains(req.Chunks[0].ActiveStep, "Executing") {
				select {
				case widgetDeliveredCh <- struct{}{}:
				default:
				}
			}
			if req.MessageID != "" {
				return req.MessageID, nil
			}
			return "widget-msg-seed", nil
		},
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			if tc.emitStep && opts.StepUpdateHandler != nil {
				opts.StepUpdateHandler(&runner.StepUpdateEvent{
					Type:     "tool_call",
					ToolName: "run_command",
					ToolInfo: &runner.StepToolInfo{
						Parameters: runner.StepToolParameters{CommandLine: "git status"},
					},
				})
				select {
				case <-widgetDeliveredCh:
				case <-time.After(5 * time.Second):
				}
			}
			defer close(doneCh)
			return tc.respJSON, "", 0, nil
		},
	})
}

func assertHeadlessDispatchResult(t *testing.T, store db.Store, dispID string, tc dispatchExecutionHeadlessCase, syncedPayload headlessSyncedPayload) {
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

	if tc.wantSyncedRepo != "" && (syncedPayload.Repo != tc.wantSyncedRepo || syncedPayload.PRNumber != tc.wantSyncedPRNumber) {
		t.Fatalf("syncedPayload = %+v, want repo=%q pr=%d", syncedPayload, tc.wantSyncedRepo, tc.wantSyncedPRNumber)
	}
}

func runDispatchExecutionHeadlessCase(t *testing.T, tc dispatchExecutionHeadlessCase) {
	t.Helper()
	store := dbtest.NewFakeStore()
	doneCh := make(chan struct{})
	var deliveredWidgetsMu sync.Mutex
	var deliveredWidgets []discord.StatusWidgetRequest
	widgetDeliveredCh := make(chan struct{}, 8)
	var syncedPayload headlessSyncedPayload
	var pool *WorkerPool

	var routerURL string
	if tc.coldWidgetSync || tc.syncConnErr {
		srv := setupHeadlessRouterServer(&pool, &syncedPayload, widgetDeliveredCh, tc.syncConnErr)
		if !tc.syncConnErr {
			defer srv.Close()
		}
		routerURL = srv.URL
	}

	pool = setupHeadlessWorkerPool(t, tc, store, routerURL, &deliveredWidgetsMu, &deliveredWidgets, widgetDeliveredCh, doneCh)
	defer pool.Stop()

	if tc.syncBadURL {
		pBad := &WorkerPool{cfg: WorkerPoolConfig{SynapseURL: "://bad-url"}}
		pBad.ensureChunkWidgetHydrated(context.Background(), "Build ID: b1", "c1", "b1")
	}

	if tc.seedWidget || tc.seedOutdatedWidget {
		seedHeadlessWidget(pool, tc.seedOutdatedWidget, widgetDeliveredCh)
	}

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

	assertHeadlessDispatchResult(t, store, disp.ID, tc, syncedPayload)

	if tc.emitStep {
		deliveredWidgetsMu.Lock()
		copiedWidgets := make([]discord.StatusWidgetRequest, len(deliveredWidgets))
		copy(copiedWidgets, deliveredWidgets)
		deliveredWidgetsMu.Unlock()
		assertDeliveredWidgetsStep(t, copiedWidgets)
	}
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
			name:         "vanadis chunk step streaming to build card widget",
			source:       "vanadis",
			sessionID:    "vanadis-chunk-step-live",
			prompt:       "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-streaming-live\nChunk ID: chunk-step-live\n=== Active Chunk (Chunk 1 of 1) ===\nImplement step streaming",
			respJSON:     `{"conversation_id":"77777777-7777-7777-7777-777777777777","status":"SUCCESS","response":"step streaming succeeded"}`,
			wantStatus:   db.StatusCompleted,
			wantRespText: "step streaming succeeded",
			seedWidget:   true,
			emitStep:     true,
		},
		{
			name:               "cold widget cache on PR CI remediation lazily syncs widget and streams steps",
			source:             "webhook",
			sessionID:          "vanadis-build-streaming-live-chunk-1",
			prompt:             "❌ **GitHub Check Failed (`build`)** on azylman/mirrormere (commit: abc1234, PR #733, branch: `feat/test`).\n\nChunk ID: build-streaming-live-chunk-1",
			respJSON:           `{"conversation_id":"77777777-7777-7777-7777-777777777777","status":"SUCCESS","response":"remediated"}`,
			wantStatus:         db.StatusCompleted,
			wantRespText:       "remediated",
			coldWidgetSync:     true,
			seedOutdatedWidget: true,
			emitStep:           true,
			wantSyncedRepo:     "azylman/mirrormere",
			wantSyncedPRNumber: 733,
		},
		{
			name:         "cold widget cache handles unreachable router and invalid URL gracefully",
			source:       "vanadis",
			sessionID:    "vanadis-chunk-step-err",
			prompt:       "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-err\nChunk ID: chunk-step-err",
			respJSON:     `{"conversation_id":"77777777-7777-7777-7777-777777777777","status":"SUCCESS","response":"done"}`,
			wantStatus:   db.StatusCompleted,
			wantRespText: "done",
			syncConnErr:  true,
			syncBadURL:   true,
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

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
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

	RecoverInterrupted(store, pool)

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
