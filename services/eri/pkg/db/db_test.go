package db

import (
	"context"
	"errors"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

func mustOK(t *testing.T, err error, desc string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s failed: %v", desc, err)
	}
}

func assertErr(t *testing.T, err error, desc string) {
	t.Helper()
	if err == nil {
		t.Errorf("expected error on %s", desc)
	}
}

type dummyTestCloser struct {
	err error
}

func (d *dummyTestCloser) Close() error {
	return d.err
}

// pgtestDatabaseDSN returns a DSN on this package's pgtest server naming
// database instead of the private clone.
func pgtestDatabaseDSN(t *testing.T, database string) string {
	t.Helper()
	u, err := url.Parse(pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("parse pgtest DSN: %v", err)
	}
	u.Path = "/" + database
	return u.String()
}

func TestDBInitAndDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  *config.Config
		// pgtestDatabase, when set, points the config at that database on the
		// package's real Postgres server instead of cfg.
		pgtestDatabase string
		errSub         string
	}{
		{name: "nil_config", cfg: nil, errSub: "config cannot be nil"},
		{name: "zero_config_has_no_url", cfg: &config.Config{}, errSub: "database URL cannot be empty"},
		{name: "empty_url", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: "  "}), errSub: "database URL cannot be empty"},
		{name: "mysql_scheme", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: "mysql://user:pass@localhost/db"}), errSub: "only postgres://"},
		{name: "sqlite_scheme", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: "sqlite:///tmp/aerial.db"}), errSub: "only postgres://"},
		{name: "sqlite_memory", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: ":memory:"}), errSub: "only postgres://"},
		{name: "bad_syntax", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: "postgres://invalid user@localhost:5432/aerial"}), errSub: "failed to ping postgres database"},
		{name: "invalid_percent", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: "postgres://%zz"}), errSub: "failed to ping postgres database"},
		{name: "unreachable_host", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: "postgresql://user:pass@127.0.0.1:1/nonexistent?sslmode=disable&connect_timeout=1"}), errSub: "failed to ping postgres database"},
		{name: "missing_database_fails_ping", pgtestDatabase: "pgtest_missing_database", errSub: "failed to ping postgres database"},
		{name: "existing_database_opens", pgtestDatabase: "postgres"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.cfg
			if tc.pgtestDatabase != "" {
				cfg = config.NewFromData(&config.ConfigData{DatabaseURL: pgtestDatabaseDSN(t, tc.pgtestDatabase)})
			}
			database, err := New(cfg)
			if tc.errSub == "" {
				if err != nil {
					t.Fatalf("New: unexpected error: %v", err)
				}
				if pingErr := database.PingContext(context.Background()); pingErr != nil {
					t.Errorf("ping opened database: %v", pingErr)
				}
				if closeErr := database.Close(); closeErr != nil {
					t.Errorf("close opened database: %v", closeErr)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errSub) {
				t.Fatalf("New error = %v, want containing %q", err, tc.errSub)
			}
			if database != nil {
				t.Errorf("New returned a database alongside error %v", err)
			}
		})
	}
}

func TestStoreUnavailableContract(t *testing.T) {
	t.Parallel()
	assertStoreUnavailable(t, closedTestStore(t))
	assertStoreUnavailable(t, &SQLStore{})
	closeWarn(nil, "test")
}

func assertMessageQueries(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	mustOK(t, store.InsertMessage(ctx, Message{ID: "msg-2", ThreadID: "th-1", Content: "Message 2", Status: StatusPending}), "InsertMessage msg-2")
	if recent, err := store.GetRecentThreadMessages(ctx, "th-1", 10); err != nil || len(recent) != 2 {
		t.Errorf("GetRecentThreadMessages = %+v, %v", recent, err)
	}
	if tasks, err := store.GetActiveTasks(ctx); err != nil || len(tasks) != 1 || tasks[0].ID != "msg-2" {
		t.Errorf("GetActiveTasks = %+v, %v", tasks, err)
	}
}

func assertMessageEmptyIDGuards(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	assertErr(t, store.InsertMessage(ctx, Message{ID: ""}), "InsertMessage empty ID")
	assertErr(t, store.UpdateMessageStatus(ctx, "", StatusCompleted, ""), "UpdateMessageStatus empty ID")
	assertErr(t, store.UpdateMessageCompleted(ctx, "", "resp", ""), "UpdateMessageCompleted empty ID")
	if m, err := store.GetMessage(ctx, ""); err != nil || m != nil {
		t.Errorf("expected nil, nil on empty ID GetMessage")
	}
	if exists, err := store.MessageExists(ctx, ""); err != nil || exists {
		t.Errorf("expected false, nil on empty ID MessageExists")
	}
	if claimed, err := store.ClaimPendingMessage(ctx, ""); err != nil || claimed {
		t.Errorf("expected false, nil on empty ID ClaimPendingMessage")
	}
}

func assertMessageUpdatesAndTelemetry(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	mustOK(t, store.UpdateMessageStatus(ctx, "msg-1", StatusProcessing, "working"), "UpdateMessageStatus")
	mustOK(t, store.UpdateMessageCompleted(ctx, "msg-1", "Task complete", "conv-msg-1", TurnTelemetry{
		DurationMs:      1200,
		InputTokens:     100,
		OutputTokens:    50,
		ThinkingTokens:  10,
		CacheReadTokens: 5,
		TotalTokens:     165,
	}), "UpdateMessageCompleted")
	got, err := store.GetMessage(ctx, "msg-1")
	mustOK(t, err, "GetMessage msg-1")
	if got.Status != StatusCompleted || got.ResponseText != "Task complete" || got.ConversationID != "conv-msg-1" || got.DurationMs != 1200 || got.TotalTokens != 165 {
		t.Errorf("completed mismatch: %+v", got)
	}
}

func TestMessageLifecycle(t *testing.T) {
	t.Parallel()
	store := NewSQLStore(newTestDB(t))
	ctx := context.Background()

	msg := Message{
		ID: "msg-1", ThreadID: "th-1", GuildID: "guild-1", AuthorID: "author-1", AuthorName: "Alice", Content: "Hello Aerial", Status: StatusPending,
		Mentions: []string{"@aerial"}, MentionUserIDs: []string{"1001"}, MentionRoleIDs: []string{"2001"}, Attachments: []string{"https://example.com/file.png"}, IsAdmin: true,
	}

	mustOK(t, store.InsertMessage(ctx, msg), "InsertMessage")
	mustOK(t, store.InsertMessage(ctx, msg), "Duplicate InsertMessage")

	got, err := store.GetMessage(ctx, "msg-1")
	mustOK(t, err, "GetMessage msg-1")
	if got == nil || got.Content != "Hello Aerial" || !got.IsAdmin || len(got.Mentions) != 1 {
		t.Fatalf("GetMessage unexpected: %+v", got)
	}

	if exists, err := store.MessageExists(ctx, "msg-1"); err != nil || !exists {
		t.Errorf("MessageExists(msg-1) = %v, %v, want true", exists, err)
	}
	if exists, err := store.MessageExists(ctx, "msg-none"); err != nil || exists {
		t.Errorf("MessageExists(msg-none) = %v, %v, want false", exists, err)
	}

	if claimed, err := store.ClaimPendingMessage(ctx, "msg-1"); err != nil || !claimed {
		t.Fatalf("ClaimPendingMessage failed: %v, claimed=%v", err, claimed)
	}
	if claimed, err := store.ClaimPendingMessage(ctx, "msg-1"); err != nil || claimed {
		t.Errorf("ClaimPendingMessage already claimed = %v, err = %v, want false and nil", claimed, err)
	}

	assertMessageUpdatesAndTelemetry(ctx, t, store)
	assertMessageQueries(ctx, t, store)
	assertMessageEmptyIDGuards(ctx, t, store)
}

func assertSessionTurnCountAndRotation(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	if id, err := store.GetSessionID(ctx, "th-1"); err != nil || id != "" {
		t.Errorf("expected empty session ID, got %q, %v", id, err)
	}
	mustOK(t, store.SaveSessionID(ctx, "th-1", "sess-1"), "SaveSessionID")
	if id, err := store.GetSessionID(ctx, "th-1"); err != nil || id != "sess-1" {
		t.Errorf("expected sess-1, got %q, %v", id, err)
	}

	if cnt, err := store.IncrementSessionTurnCount(ctx, "th-1"); err != nil || cnt != 1 {
		t.Errorf("IncrementSessionTurnCount = %d, %v, want 1", cnt, err)
	}
	if _, err := store.IncrementSessionTurnCount(ctx, "th-1"); err != nil {
		t.Fatalf("IncrementSessionTurnCount: %v", err)
	}
	if cnt, err := store.GetSessionTurnCount(ctx, "th-1"); err != nil || cnt != 2 {
		t.Errorf("GetSessionTurnCount = %d, %v, want 2", cnt, err)
	}

	mustOK(t, store.RotateSessionID(ctx, "th-1", "sess-2"), "RotateSessionID")
	curr, errCurr := store.GetSessionID(ctx, "th-1")
	mustOK(t, errCurr, "GetSessionID")
	prev, errPrev := store.GetPreviousSessionID(ctx, "th-1")
	mustOK(t, errPrev, "GetPreviousSessionID")
	turns, errTurns := store.GetSessionTurnCount(ctx, "th-1")
	mustOK(t, errTurns, "GetSessionTurnCount")
	if curr != "sess-2" || prev != "sess-1" || turns != 0 {
		t.Errorf("post-rotation: curr=%q, prev=%q, turns=%d", curr, prev, turns)
	}

	mustOK(t, store.SaveSessionID(ctx, "th-1", "sess-1"), "SaveSessionID stale previous session")
	currStale, errCurrStale := store.GetSessionID(ctx, "th-1")
	mustOK(t, errCurrStale, "GetSessionID stale")
	prevStale, errPrevStale := store.GetPreviousSessionID(ctx, "th-1")
	mustOK(t, errPrevStale, "GetPreviousSessionID stale")
	turnsStale, errTurnsStale := store.GetSessionTurnCount(ctx, "th-1")
	mustOK(t, errTurnsStale, "GetSessionTurnCount stale")
	if currStale != "sess-2" || prevStale != "sess-1" || turnsStale != 0 {
		t.Errorf("post-stale-save: curr=%q, prev=%q, turns=%d", currStale, prevStale, turnsStale)
	}

	mustOK(t, store.SaveSessionID(ctx, "th-1", ""), "SaveSessionID clear empty")
	cleared, errCleared := store.GetSessionID(ctx, "th-1")
	mustOK(t, errCleared, "GetSessionID cleared")
	prevAfterClear, errPrevAfterClear := store.GetPreviousSessionID(ctx, "th-1")
	mustOK(t, errPrevAfterClear, "GetPreviousSessionID preserved")
	if cleared != "" || prevAfterClear != "sess-1" {
		t.Errorf("post-clear: cleared=%q, prevAfterClear=%q", cleared, prevAfterClear)
	}
	mustOK(t, store.SaveSessionID(ctx, "th-1", "sess-2"), "SaveSessionID restore sess-2")
	assertSessionCASRotation(ctx, t, store)
}

func assertSessionCASRotation(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	// CAS rotation with matching expectedOldSessionID succeeds
	mustOK(t, store.RotateSessionID(ctx, "th-1", "sess-3", "sess-2"), "RotateSessionID CAS match")
	currCAS, errCurrCAS := store.GetSessionID(ctx, "th-1")
	mustOK(t, errCurrCAS, "GetSessionID CAS")
	if currCAS != "sess-3" {
		t.Errorf("expected sess-3 after CAS rotation, got %q", currCAS)
	}

	// CAS rotation with mismatching expectedOldSessionID fails with ErrSessionSuperseded and leaves record untouched
	errMismatch := store.RotateSessionID(ctx, "th-1", "sess-4", "sess-stale")
	if !errors.Is(errMismatch, ErrSessionSuperseded) {
		t.Errorf("expected ErrSessionSuperseded, got %v", errMismatch)
	}
	currAfterMismatch, errCurrMismatch := store.GetSessionID(ctx, "th-1")
	mustOK(t, errCurrMismatch, "GetSessionID after mismatch")
	if currAfterMismatch != "sess-3" {
		t.Errorf("expected sess-3 unchanged after mismatch CAS rotation, got %q", currAfterMismatch)
	}
}

func assertSessionEmptyKeyGuards(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	if id, err := store.GetSessionID(ctx, ""); err != nil || id != "" {
		t.Errorf("expected empty GetSessionID")
	}
	if err := store.SaveSessionID(ctx, "", "s1"); err != nil {
		t.Fatalf("unexpected error on empty key guard: %v", err)
	}
	_, errEmptyTurn := store.IncrementSessionTurnCount(ctx, "")
	assertErr(t, errEmptyTurn, "IncrementSessionTurnCount empty key")
	if cnt, err := store.GetSessionTurnCount(ctx, ""); err != nil || cnt != 0 {
		t.Errorf("GetSessionTurnCount empty key = %d, %v", cnt, err)
	}
	if cnt, err := store.GetSessionTurnCount(ctx, "nonexistent-thread"); err != nil || cnt != 0 {
		t.Errorf("GetSessionTurnCount nonexistent = %d, %v", cnt, err)
	}
	assertErr(t, store.RotateSessionID(ctx, "", "s2"), "RotateSessionID empty key")
	if prev, err := store.GetPreviousSessionID(ctx, ""); err != nil || prev != "" {
		t.Errorf("GetPreviousSessionID empty key = %q, %v", prev, err)
	}
	if err := store.SaveThreadSummary(ctx, "", "sum", "mid"); err != nil {
		t.Fatalf("unexpected error on empty key guard: %v", err)
	}
	if sum, last, err := store.GetThreadSummary(ctx, ""); err != nil || sum != "" || last != "" {
		t.Errorf("GetThreadSummary empty thread = %q, %q, %v", sum, last, err)
	}
}

func TestSessionLifecycle(t *testing.T) {
	t.Parallel()
	store := NewSQLStore(newTestDB(t))
	ctx := context.Background()

	// 1. Session turn count and rotation
	assertSessionTurnCountAndRotation(ctx, t, store)

	// 2. Thread summary
	if sum, wm, err := store.GetThreadSummary(ctx, "th-1"); err != nil || sum != "" || wm != "" {
		t.Errorf("GetThreadSummary empty = %q, %q, %v", sum, wm, err)
	}
	mustOK(t, store.SaveThreadSummary(ctx, "th-1", "Thread summary", "msg-100"), "SaveThreadSummary")
	if sum, wm, err := store.GetThreadSummary(ctx, "th-1"); err != nil || sum != "Thread summary" || wm != "msg-100" {
		t.Errorf("GetThreadSummary = %q, %q, %v", sum, wm, err)
	}

	// 3. External conversation mapping
	mustOK(t, store.SaveSessionID(ctx, "ext-1", "int-1"), "SaveSessionID ext-1")

	// 4. Empty key validations
	assertSessionEmptyKeyGuards(ctx, t, store)
}

func TestCleanTaskSummary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, input, want, wantSuffix string
	}{
		{name: "plain", input: "Fix bug in auth service", want: "Fix bug in auth service"},
		{name: "xml_envelope", input: "<USER_REQUEST>Check logs</USER_REQUEST>", want: "Check logs"},
		{name: "markdown_heading", input: "# Deploy service\nNow deploy", want: "Deploy service Now deploy"},
		{name: "tag_fallback", input: "<tag>@mention</tag>", want: "Agent Task"},
		{name: "whitespace_fallback", input: "   ", want: "Agent Task"},
		{name: "long_text_truncated", input: "This is a very long prompt string " + strings.Repeat("with lots of extra words ", 10), wantSuffix: "..."},
		{name: "empty", input: "", want: "Agent Task"},
		{name: "role_and_channel_mentions", input: "Alert <@!12345> and notify <@&67890> in <#1122334455>!", want: "Alert and notify in !"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CleanTaskSummary(tc.input)
			if tc.wantSuffix != "" {
				if !strings.HasSuffix(got, tc.wantSuffix) || len([]rune(got)) > 140 {
					t.Errorf("CleanTaskSummary(%q) = %q, want <=140 runes ending %q", tc.input, got, tc.wantSuffix)
				}
				return
			}
			if got != tc.want {
				t.Errorf("CleanTaskSummary(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestInferTriggerType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, authorID, want string
	}{
		{name: "dispatch_prefix", authorID: "dispatch:heartbeat", want: "heartbeat"},
		{name: "http_client", authorID: "http-client", want: "http"},
		{name: "user_message", authorID: "user-1", want: "discord"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := InferTriggerType(tc.authorID); got != tc.want {
				t.Errorf("InferTriggerType(%q) = %q, want %q", tc.authorID, got, tc.want)
			}
		})
	}
}

func TestCloseWarn(t *testing.T) {
	t.Parallel()
	closeWarn(&dummyTestCloser{err: errors.New("dummy close error")}, "test closer")
	closeWarn(&dummyTestCloser{}, "test closer")
}

// activeTaskWant is the observable projection of one ActiveTask row.
type activeTaskWant struct {
	ID, ThreadID, SessionID, PreviousSessionID, ThreadSummary string
	Summary, ResponseText, TriggerType                        string
	DurationMs                                                int64
	TotalTokens                                               int
}

var taskBase = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// filterSessionSQL maps thread th-filter to a session with a previous
// conversation and summary; rows insert under the default agent tenant.
const filterSessionSQL = `INSERT INTO sessions (session_id, conversation_id, previous_conversation_id, summary) VALUES ('discord-th-filter', 'int-sess-1', 'prev-sess-0', 'Summary of thread filter')`

// Seeded in order, so row_id is 1, 2, 3 in each private database.
var filterMessages = []Message{
	{ID: "msg-f1", ThreadID: "th-filter", Content: "Prompt 1", Summary: "Summary 1", ResponseText: "Response 1", Status: StatusCompleted, IsAdmin: true, CreatedAt: taskBase.Add(-3 * time.Minute), UpdatedAt: taskBase.Add(-3 * time.Minute)},
	{ID: "msg-f2", ThreadID: "th-filter", Content: "Prompt 2", Summary: "Summary 2", ResponseText: "Response 2", Status: StatusPending, Effort: "medium", CreatedAt: taskBase.Add(-2 * time.Minute), UpdatedAt: taskBase.Add(-2 * time.Minute)},
	{ID: "msg-f3", ThreadID: "th-other", Content: "Prompt 3", Summary: "Summary 3", ResponseText: "Response 3", Status: StatusProcessing, IsAdmin: true, CreatedAt: taskBase.Add(-1 * time.Minute), UpdatedAt: taskBase.Add(-1 * time.Minute)},
}

var (
	taskF1 = activeTaskWant{ID: "msg-f1", ThreadID: "th-filter", SessionID: "int-sess-1", PreviousSessionID: "prev-sess-0", ThreadSummary: "Summary of thread filter", Summary: "Summary 1", ResponseText: "Response 1", TriggerType: "discord"}
	taskF2 = activeTaskWant{ID: "msg-f2", ThreadID: "th-filter", SessionID: "int-sess-1", PreviousSessionID: "prev-sess-0", ThreadSummary: "Summary of thread filter", Summary: "Summary 2", ResponseText: "Response 2", TriggerType: "discord"}
	taskF3 = activeTaskWant{ID: "msg-f3", ThreadID: "th-other", Summary: "Summary 3", ResponseText: "Response 3", TriggerType: "discord"}
)

var unionMessages = []Message{
	{ID: "msg-live-1", ThreadID: "123456", Content: "Discord user prompt", Status: StatusPending, DurationMs: 1500, InputTokens: 120, OutputTokens: 60, ThinkingTokens: 15, CacheReadTokens: 10, TotalTokens: 205, CreatedAt: taskBase.Add(-2 * time.Minute), UpdatedAt: taskBase.Add(-2 * time.Minute)},
}

var unionDispatches = []Dispatch{
	{ID: "disp-live-stateful", Source: "heartbeat", SessionID: "heartbeat-hb-1", Prompt: "Heartbeat prompt", Status: StatusPending, DurationMs: 3000, InputTokens: 250, OutputTokens: 100, ThinkingTokens: 30, CacheReadTokens: 20, TotalTokens: 400, CreatedAt: taskBase.Add(-1 * time.Minute), UpdatedAt: taskBase.Add(-1 * time.Minute)},
	{ID: "disp-live-ephem", Source: "webhook", Prompt: "Webhook prompt", Status: StatusPending, CreatedAt: taskBase, UpdatedAt: taskBase},
	{ID: "disp-live-done", Source: "webhook", Prompt: "Finished prompt", Status: StatusCompleted, CreatedAt: taskBase, UpdatedAt: taskBase},
}

var (
	taskLiveMsg      = activeTaskWant{ID: "msg-live-1", ThreadID: "123456", Summary: "Discord user prompt", TriggerType: "discord", DurationMs: 1500, TotalTokens: 205}
	taskLiveStateful = activeTaskWant{ID: "disp-live-stateful", ThreadID: "heartbeat-hb-1", Summary: "Heartbeat prompt", TriggerType: "heartbeat", DurationMs: 3000, TotalTokens: 400}
	taskLiveEphem    = activeTaskWant{ID: "disp-live-ephem", ThreadID: "dispatch-disp-live-ephem", Summary: "Webhook prompt", TriggerType: "webhook"}
)

func TestGetActiveTasks(t *testing.T) {
	t.Parallel()
	adminTrue, adminFalse := true, false
	tests := []struct {
		name           string
		seedSQL        string
		seedMessages   []Message
		seedDispatches []Dispatch
		filter         TasksFilter
		sabotageSQL    string
		closeDB        bool
		wantErr        string
		want           []activeTaskWant
	}{
		{name: "thread_id filter and session join", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{ThreadID: "th-filter", Statuses: []string{"COMPLETED"}}, want: []activeTaskWant{taskF1}},
		{name: "is_admin false filter matches thread session", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{IsAdmin: &adminFalse}, want: []activeTaskWant{taskF2}},
		{name: "is_admin true filter with status", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{IsAdmin: &adminTrue, Statuses: []string{"processing"}}, want: []activeTaskWant{taskF3}},
		{name: "multiple statuses filter skips blanks", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{Statuses: []string{"PENDING", " ", "PROCESSING"}}, want: []activeTaskWant{taskF3, taskF2}},
		{name: "clamped limit above 200", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{Limit: 250}, want: []activeTaskWant{taskF3, taskF2, taskF1}},
		{name: "explicit limit", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{Limit: 1}, want: []activeTaskWant{taskF3}},
		{name: "after_row_id orders asc", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{AfterRowID: 1}, want: []activeTaskWant{taskF2, taskF3}},
		{name: "since filter matches recent", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{Since: taskBase.Add(-90 * time.Second)}, want: []activeTaskWant{taskF3}},
		{name: "before_row_id filter", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{BeforeRowID: 1}, want: []activeTaskWant{taskF1}},
		{name: "explicit order asc", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{Order: TasksOrderAsc}, want: []activeTaskWant{taskF1, taskF2, taskF3}},
		{name: "explicit order desc", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{Order: TasksOrderDesc}, want: []activeTaskWant{taskF3, taskF2, taskF1}},
		{name: "explicit order desc overrides after_row_id asc", seedSQL: filterSessionSQL, seedMessages: filterMessages, filter: TasksFilter{AfterRowID: 1, Order: TasksOrderDesc}, want: []activeTaskWant{taskF3, taskF2}},
		{name: "unfiltered returns active messages oldest first", seedSQL: filterSessionSQL, seedMessages: filterMessages, want: []activeTaskWant{taskF2, taskF3}},
		{name: "unfiltered unions active messages and dispatches", seedMessages: unionMessages, seedDispatches: unionDispatches, want: []activeTaskWant{taskLiveMsg, taskLiveStateful, taskLiveEphem}},
		{name: "filtered query excludes dispatches", seedMessages: unionMessages, seedDispatches: unionDispatches, filter: TasksFilter{Statuses: []string{"PENDING"}}, want: []activeTaskWant{taskLiveMsg}},
		{
			name:    "empty stored summary falls back to cleaned prompt",
			seedSQL: `INSERT INTO messages (id, thread_id, content, summary, status, created_at, updated_at) VALUES ('m-empty-summ', 't-task', 'Prompt from task content', '', 'PENDING', now(), now())`,
			want:    []activeTaskWant{{ID: "m-empty-summ", ThreadID: "t-task", Summary: "Prompt from task content", TriggerType: "discord"}},
		},
		{name: "empty database", want: []activeTaskWant{}},
		{name: "closed handle fails the query", closeDB: true, wantErr: "query active tasks"},
		{name: "null created_at fails the scan", seedMessages: filterMessages, sabotageSQL: `ALTER TABLE messages ALTER COLUMN created_at DROP NOT NULL; UPDATE messages SET created_at = NULL`, wantErr: "scan active task"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, database := newTestStore(t)
			store.WithAgent(DefaultAgent)
			execSQL(t, database, tc.seedSQL)
			seedMessages(t, store, tc.seedMessages)
			seedDispatches(t, store, tc.seedDispatches)
			execSQL(t, database, tc.sabotageSQL)
			if tc.closeDB {
				mustOK(t, database.Close(), "close database")
			}
			tasks, err := store.GetActiveTasks(context.Background(), tc.filter)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("GetActiveTasks error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetActiveTasks: %v", err)
			}
			got := make([]activeTaskWant, 0, len(tasks))
			for _, task := range tasks {
				got = append(got, activeTaskWant{
					ID: task.ID, ThreadID: task.ThreadID, SessionID: task.SessionID, PreviousSessionID: task.PreviousSessionID, ThreadSummary: task.ThreadSummary,
					Summary: task.Summary, ResponseText: task.ResponseText, TriggerType: task.TriggerType, DurationMs: task.DurationMs, TotalTokens: task.TotalTokens,
				})
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("GetActiveTasks =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}

func TestSchemaContainsVoiceTables(t *testing.T) {
	t.Parallel()
	schemaBytes, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("failed to read schema.sql: %v", err)
	}
	schema := string(schemaBytes)

	tests := []struct {
		name       string
		targetText string
	}{
		{name: "voice_calls table", targetText: "CREATE TABLE IF NOT EXISTS voice_calls"},
		{name: "voice_messages table", targetText: "CREATE TABLE IF NOT EXISTS voice_messages"},
		{name: "idx_voice_calls_to_started", targetText: "idx_voice_calls_to_started"},
		{name: "idx_voice_calls_state", targetText: "idx_voice_calls_state"},
		{name: "idx_voice_messages_created", targetText: "idx_voice_messages_created"},
		{name: "e164 regex check", targetText: "CHECK (from_number ~ '^\\+[1-9][0-9]{1,14}$')"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !strings.Contains(schema, tc.targetText) {
				t.Fatalf("schema.sql missing expected definition %q", tc.targetText)
			}
		})
	}
}

func TestDiscordSessionID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, input, want string
	}{
		{name: "empty", input: "", want: ""},
		{name: "whitespace", input: "   ", want: ""},
		{name: "numeric snowflake", input: "123456789012345678", want: "discord-123456789012345678"},
		{name: "numeric with spaces", input: "  987654321  ", want: "discord-987654321"},
		{name: "already discord prefixed", input: "discord-123456", want: "discord-123456"},
		{name: "heartbeat prefixed", input: "heartbeat-test-id", want: "heartbeat-test-id"},
		{name: "dispatch prefixed", input: "dispatch-disp-1", want: "dispatch-disp-1"},
		{name: "non-numeric slug", input: "general-chat", want: "general-chat"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := DiscordSessionID(tc.input)
			if got != tc.want {
				t.Errorf("DiscordSessionID(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func mustInsertDispatch(ctx context.Context, t *testing.T, store Store, d Dispatch, msg string) {
	t.Helper()
	_, err := store.InsertDispatch(ctx, d)
	mustOK(t, err, msg)
}

func assertDispatchValidationGuards(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	_, errEmptyDisp := store.InsertDispatch(ctx, Dispatch{})
	assertErr(t, errEmptyDisp, "InsertDispatch empty ID")
	assertErr(t, store.UpdateDispatchStatus(ctx, "", "PROCESSING", ""), "UpdateDispatchStatus empty ID")

	gotEmpty, err := store.GetDispatch(ctx, "")
	if err != nil || gotEmpty != nil {
		t.Errorf("expected nil, nil for empty dispatch id")
	}
	claimedEmpty, err := store.ClaimPendingDispatch(ctx, "")
	if err != nil || claimedEmpty {
		t.Errorf("expected false, nil for empty dispatch id claim")
	}
	recentEmpty, err := store.GetRecentSessionDispatches(ctx, "", 10)
	if err != nil || recentEmpty != nil {
		t.Errorf("expected nil, nil for empty session id in GetRecentSessionDispatches")
	}
	scannedNil, err := scanDispatches(nil)
	if err != nil || scannedNil != nil {
		t.Errorf("expected nil, nil for scanDispatches(nil)")
	}
}

func assertDispatchClaim(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	claimed, err := store.ClaimPendingDispatch(ctx, "disp-1")
	mustOK(t, err, "ClaimPendingDispatch disp-1")
	if !claimed {
		t.Errorf("expected claimed=true")
	}
	claimedAgain, err := store.ClaimPendingDispatch(ctx, "disp-1")
	mustOK(t, err, "ClaimPendingDispatch again")
	if claimedAgain {
		t.Errorf("expected claimed=false for already processing dispatch")
	}

	mustOK(t, store.UpdateDispatchStatus(ctx, "disp-1", "PROCESSING", "in flight"), "UpdateDispatchStatus")
	got, err := store.GetDispatch(ctx, "disp-1")
	mustOK(t, err, "GetDispatch disp-1")
	if got.Status != StatusProcessing || got.ErrorMessage != "in flight" {
		t.Errorf("unexpected processing state: %+v", got)
	}
}

func assertDispatchStoreLifecycleTransitions(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	d1 := Dispatch{
		ID:          "disp-1",
		Source:      "heartbeat",
		SessionID:   "heartbeat-hb-1",
		Prompt:      "Prompt for dispatch 1",
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	mustInsertDispatch(ctx, t, store, d1, "InsertDispatch disp-1")

	got, err := store.GetDispatch(ctx, "disp-1")
	mustOK(t, err, "GetDispatch disp-1")
	if got == nil || got.ID != "disp-1" || got.Status != StatusPending || got.Summary == "" || got.Traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("unexpected dispatch retrieved: %+v", got)
	}

	assertDispatchClaim(ctx, t, store)

	dispTel := TurnTelemetry{DurationMs: 2500, InputTokens: 200, OutputTokens: 80, ThinkingTokens: 20, CacheReadTokens: 10, TotalTokens: 310}
	mustOK(t, store.UpdateDispatchCompleted(ctx, "disp-1", "Result text", "conv-uuid-123", dispTel), "UpdateDispatchCompleted")
	got, err = store.GetDispatch(ctx, "disp-1")
	mustOK(t, err, "GetDispatch disp-1")
	if got.Status != StatusCompleted || got.ResponseText != "Result text" || got.ConversationID != "conv-uuid-123" {
		t.Errorf("unexpected completed state: %+v", got)
	}
	if got.DurationMs != 2500 || got.InputTokens != 200 || got.OutputTokens != 80 || got.ThinkingTokens != 20 || got.CacheReadTokens != 10 || got.TotalTokens != 310 {
		t.Errorf("unexpected telemetry on dispatch: %+v", got)
	}
}

func assertDispatchRecent(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	d2 := Dispatch{
		ID:        "disp-2",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Prompt for dispatch 2",
	}
	mustInsertDispatch(ctx, t, store, d2, "InsertDispatch disp-2")
	mustOK(t, store.UpdateDispatchCompleted(ctx, "disp-2", "Result 2", ""), "UpdateDispatchCompleted disp-2")

	recent, err := store.GetRecentSessionDispatches(ctx, "heartbeat-hb-1", 10)
	mustOK(t, err, "GetRecentSessionDispatches")
	if len(recent) != 2 || recent[0].ID != "disp-1" || recent[1].ID != "disp-2" {
		t.Errorf("unexpected recent session dispatches: %+v", recent)
	}
}

func assertDispatchPendingAndMessageCompleted(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	d4 := Dispatch{
		ID:        "disp-4-pending",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-2",
		Prompt:    "Prompt 4",
		Status:    StatusPending,
	}
	mustInsertDispatch(ctx, t, store, d4, "InsertDispatch disp-4-pending")

	mustOK(t, store.UpdateDispatchCompleted(ctx, "disp-4-pending", "Done 4", ""), "UpdateDispatchCompleted without conv")
	got, err := store.GetDispatch(ctx, "disp-4-pending")
	mustOK(t, err, "GetDispatch disp-4-pending")
	if got.Status != StatusCompleted || got.ResponseText != "Done 4" {
		t.Errorf("unexpected completed state without conv: %+v", got)
	}

	mustOK(t, store.InsertMessage(ctx, Message{ID: "msg-cov-1", Content: "Coverage"}), "InsertMessage")
	mustOK(t, store.UpdateMessageCompleted(ctx, "msg-cov-1", "Done without conv", ""), "UpdateMessageCompleted without conv")
	covMsg, err := store.GetMessage(ctx, "msg-cov-1")
	mustOK(t, err, "GetMessage msg-cov-1")
	if covMsg.ResponseText != "Done without conv" {
		t.Errorf("unexpected response text")
	}
	mustOK(t, store.UpdateMessageCompleted(ctx, "msg-cov-1", "Done with conv", "conv-cov-uuid"), "UpdateMessageCompleted with conv")
	covMsg, err = store.GetMessage(ctx, "msg-cov-1")
	mustOK(t, err, "GetMessage msg-cov-1")
	if covMsg.ResponseText != "Done with conv" || covMsg.ConversationID != "conv-cov-uuid" {
		t.Errorf("unexpected response text or conversation ID: %+v", covMsg)
	}
}

func TestDispatchLifecycle(t *testing.T) {
	t.Parallel()
	store := NewSQLStore(newTestDB(t))
	ctx := context.Background()

	assertDispatchStoreLifecycleTransitions(ctx, t, store)
	assertDispatchRecent(ctx, t, store)
	assertDispatchPendingAndMessageCompleted(ctx, t, store)
	assertDispatchValidationGuards(ctx, t, store)
}
