package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared&_busy_timeout=5000", t.Name(), time.Now().UnixNano())
	database, err := initTestSQLite(dsn)
	if err != nil {
		t.Fatalf("Failed to initialize hermetic SQLite test database: %v", err)
	}
	database.SetMaxOpenConns(1)
	return database
}

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

func TestDBInitAndDSN(t *testing.T) {
	tests := []struct {
		name, url, errSub        string
		cfgNil, snapNil, wantErr bool
	}{
		{name: "nil_config", cfgNil: true, wantErr: true},
		{name: "empty_url", url: "", wantErr: true},
		{name: "mysql_scheme", url: "mysql://user:pass@localhost/db", wantErr: true, errSub: "only postgres://"},
		{name: "sqlite_prod", url: "sqlite:///tmp/aerial.db", wantErr: true, errSub: "only postgres://"},
		{name: "nil_snapshot", snapNil: true, wantErr: true},
		{name: "bad_syntax", url: "postgres://invalid user@localhost:5432/aerial", wantErr: true, errSub: "failed to ping postgres database"},
		{name: "invalid_percent", url: "postgres://%zz", wantErr: true, errSub: "failed to ping postgres database"},
		{name: "unreachable_host", url: "postgres://user:pass@127.0.0.1:54399/nonexistent?sslmode=disable&connect_timeout=1", wantErr: true, errSub: "failed to ping postgres database"},
	}

	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	defer func() { sqliteTestInitHook = oldHook }()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.cfgNil {
				_, err = New(nil)
			} else if tc.snapNil {
				_, err = New(&config.Config{})
			} else {
				cfg := config.NewFromData(&config.ConfigData{DatabaseURL: tc.url})
				_, err = New(cfg)
			}
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.errSub != "" && (err == nil || !strings.Contains(err.Error(), tc.errSub)) {
				t.Fatalf("expected error containing %q, got %v", tc.errSub, err)
			}
		})
	}

	sqliteTestInitHook = oldHook
	if oldHook != nil {
		tmpFile := filepath.Join(t.TempDir(), "subfolder", "test.db")
		cfgHooked := config.NewFromData(&config.ConfigData{DatabaseURL: "sqlite://" + tmpFile})
		dbHooked, err := New(cfgHooked)
		mustOK(t, err, "New with hooked sqlite")
		_ = dbHooked.Close()
		if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
			t.Errorf("expected sqlite db file to exist at %s", tmpFile)
		}
	}
	for _, dsn := range []string{"file:mem_1?mode=memory&cache=shared", "file:mem_2?mode=memory&_pragma=busy_timeout(5000)", "sqlite://:memory:"} {
		dbPragma, err := Open(dsn)
		mustOK(t, err, "Open")
		_ = dbPragma.Close()
	}

	oldSqlOpen := sqlOpen
	defer func() { sqlOpen = oldSqlOpen }()
	sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
		return nil, errors.New("simulated open error")
	}
	_, errOpen := Open("postgres://localhost/test")
	assertErr(t, errOpen, "sqlOpen failure")
	sqlOpen = oldSqlOpen

	database := setupTestDB(t)
	defer database.Close()
	_, err := database.Exec("SELECT id, row_id, thread_id, guild_id, author_id, author_name, content, status, retry_count, restart_count, error_message, response_text, created_at, updated_at FROM messages LIMIT 0")
	mustOK(t, err, "messages schema check")
	var fk int
	if err := database.QueryRow("PRAGMA foreign_keys;").Scan(&fk); err != nil || fk != 1 {
		t.Errorf("expected foreign_keys=1, got %d, %v", fk, err)
	}
	closedDB, _ := initTestSQLite(filepath.Join(t.TempDir(), "closed-mig.db"))
	_ = closedDB.Close()
	assertErr(t, initSchemaSQLite(closedDB), "closed db initSchemaSQLite")
}

func TestStoreUnavailableContract(t *testing.T) {
	database := setupTestDB(t)
	_ = database.Close()
	assertStoreUnavailable(t, NewSQLStore(database))
	assertStoreUnavailable(t, &SQLStore{})
	closeWarn(nil, "test")
}

func TestMessageLifecycle(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
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
	if claimed, _ := store.ClaimPendingMessage(ctx, "msg-1"); claimed {
		t.Errorf("ClaimPendingMessage already claimed = true, want false")
	}

	mustOK(t, store.UpdateMessageStatus(ctx, "msg-1", StatusProcessing, "working"), "UpdateMessageStatus")
	mustOK(t, store.IncrementMessageRetry(ctx, "msg-1", "timeout"), "IncrementMessageRetry")
	got, _ = store.GetMessage(ctx, "msg-1")
	if got.RetryCount != 1 || got.ErrorMessage != "timeout" {
		t.Errorf("retry mismatch: %+v", got)
	}

	mustOK(t, store.ResetMessageToPendingWithRestart(ctx, "msg-1", "recovered"), "ResetMessageToPendingWithRestart")
	got, _ = store.GetMessage(ctx, "msg-1")
	if got.Status != StatusPending || got.RestartCount != 1 {
		t.Errorf("restart mismatch: %+v", got)
	}

	mustOK(t, store.UpdateMessageCompleted(ctx, "msg-1", "Task complete"), "UpdateMessageCompleted")
	got, _ = store.GetMessage(ctx, "msg-1")
	if got.Status != StatusCompleted || got.ResponseText != "Task complete" {
		t.Errorf("completed mismatch: %+v", got)
	}

	_ = store.InsertMessage(ctx, Message{ID: "msg-2", ThreadID: "th-1", Content: "Message 2", Status: StatusPending})
	if pending, err := store.GetPendingOrProcessingMessages(ctx, 10); err != nil || len(pending) != 1 || pending[0].ID != "msg-2" {
		t.Errorf("GetPendingOrProcessingMessages = %+v, %v", pending, err)
	}
	if threadIDs, err := store.GetActiveRecentThreadIDs(ctx, time.Hour); err != nil || len(threadIDs) != 1 || threadIDs[0] != "th-1" {
		t.Errorf("GetActiveRecentThreadIDs = %v, %v", threadIDs, err)
	}
	if recent, err := store.GetRecentThreadMessages(ctx, "th-1", 10); err != nil || len(recent) != 2 {
		t.Errorf("GetRecentThreadMessages = %+v, %v", recent, err)
	}
	if tasks, err := store.GetActiveTasks(ctx); err != nil || len(tasks) != 1 || tasks[0].ID != "msg-2" {
		t.Errorf("GetActiveTasks = %+v, %v", tasks, err)
	}

	if active, err := store.HasActiveThreadTurn(ctx, ""); err != nil || active {
		t.Errorf("HasActiveThreadTurn empty = %v, %v, want false, nil", active, err)
	}
	if active, err := store.HasActiveThreadTurn(ctx, "th-none"); err != nil || active {
		t.Errorf("HasActiveThreadTurn th-none = %v, %v, want false, nil", active, err)
	}
	if active, err := store.HasActiveThreadTurn(ctx, "th-1"); err != nil || !active {
		t.Errorf("HasActiveThreadTurn th-1 = %v, %v, want true, nil", active, err)
	}

	// Empty ID validation
	assertErr(t, store.InsertMessage(ctx, Message{ID: ""}), "InsertMessage empty ID")
	assertErr(t, store.UpdateMessageStatus(ctx, "", StatusCompleted, ""), "UpdateMessageStatus empty ID")
	assertErr(t, store.UpdateMessageCompleted(ctx, "", "resp"), "UpdateMessageCompleted empty ID")
	assertErr(t, store.IncrementMessageRetry(ctx, "", "err"), "IncrementMessageRetry empty ID")
	assertErr(t, store.ResetMessageToPendingWithRestart(ctx, "", "reason"), "ResetMessageToPendingWithRestart empty ID")
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

func TestSessionLifecycle(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Session turn count and rotation
	if id, err := store.GetSessionID(ctx, "th-1"); err != nil || id != "" {
		t.Errorf("expected empty session ID, got %q, %v", id, err)
	}
	mustOK(t, store.SaveSessionID(ctx, "th-1", "sess-1"), "SaveSessionID")
	if id, _ := store.GetSessionID(ctx, "th-1"); id != "sess-1" {
		t.Errorf("expected sess-1, got %q", id)
	}

	if cnt, err := store.IncrementSessionTurnCount(ctx, "th-1"); err != nil || cnt != 1 {
		t.Errorf("IncrementSessionTurnCount = %d, %v, want 1", cnt, err)
	}
	_, _ = store.IncrementSessionTurnCount(ctx, "th-1")
	if cnt, _ := store.GetSessionTurnCount(ctx, "th-1"); cnt != 2 {
		t.Errorf("GetSessionTurnCount = %d, want 2", cnt)
	}

	mustOK(t, store.RotateSessionID(ctx, "th-1", "sess-2"), "RotateSessionID")
	curr, _ := store.GetSessionID(ctx, "th-1")
	prev, _ := store.GetPreviousSessionID(ctx, "th-1")
	turns, _ := store.GetSessionTurnCount(ctx, "th-1")
	if curr != "sess-2" || prev != "sess-1" || turns != 0 {
		t.Errorf("post-rotation: curr=%q, prev=%q, turns=%d", curr, prev, turns)
	}

	// 2. Thread summary
	if sum, wm, err := store.GetThreadSummary(ctx, "th-1"); err != nil || sum != "" || wm != "" {
		t.Errorf("GetThreadSummary empty = %q, %q, %v", sum, wm, err)
	}
	mustOK(t, store.SaveThreadSummary(ctx, "th-1", "Thread summary", "msg-100"), "SaveThreadSummary")
	if sum, wm, _ := store.GetThreadSummary(ctx, "th-1"); sum != "Thread summary" || wm != "msg-100" {
		t.Errorf("GetThreadSummary = %q, %q", sum, wm)
	}

	// 3. Session activity stats
	_ = store.InsertMessage(ctx, Message{ID: "m-s-1", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "Active"})
	_ = store.InsertMessage(ctx, Message{ID: "m-s-2", ThreadID: "th-1", Status: StatusCompleted, ResponseText: "[EXPIRED_STALE]"})
	stats, err := store.GetSessionActivityStats(ctx, "th-1")
	if err != nil || stats.InternalSessionID != "sess-2" || stats.CompletedTurns != 1 {
		t.Errorf("GetSessionActivityStats = %+v, %v", stats, err)
	}

	// 4. External conversation mapping
	_ = store.SaveSessionID(ctx, "ext-1", "int-1")

	// 5. Unrotated sessions
	for i := 0; i < 10; i++ {
		_, _ = store.IncrementSessionTurnCount(ctx, "th-1")
	}
	if unrotated, err := store.FindUnrotatedSessions(ctx, 5); err != nil || len(unrotated) != 1 || unrotated[0].ThreadID != "th-1" {
		t.Errorf("FindUnrotatedSessions = %+v, %v", unrotated, err)
	}
	_, _ = store.FindUnrotatedSessions(ctx, 0)

	// 6. Table-driven ParseDBTime
	now := time.Now().Truncate(time.Second).UTC()
	timeTests := []struct {
		name   string
		input  any
		wantOK bool
	}{
		{name: "nil", input: nil, wantOK: false},
		{name: "time_val", input: now, wantOK: true},
		{name: "time_ptr", input: &now, wantOK: true},
		{name: "nil_time_ptr", input: (*time.Time)(nil), wantOK: false},
		{name: "empty_str", input: "", wantOK: false},
		{name: "whitespace_str", input: "   ", wantOK: false},
		{name: "rfc3339_str", input: now.Format(time.RFC3339), wantOK: true},
		{name: "sqlite_str", input: "2026-10-19 12:00:00", wantOK: true},
		{name: "bytes", input: []byte(now.Format(time.RFC3339)), wantOK: true},
		{name: "invalid_str", input: "not-a-time", wantOK: false},
		{name: "unsupported_type", input: 12345, wantOK: false},
	}
	for _, tc := range timeTests {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := ParseDBTime(tc.input)
			if ok != tc.wantOK {
				t.Errorf("ParseDBTime(%v) ok = %v, wantOK = %v", tc.input, ok, tc.wantOK)
			}
		})
	}

	// 7. Empty key validations
	if id, err := store.GetSessionID(ctx, ""); err != nil || id != "" {
		t.Errorf("expected empty GetSessionID")
	}
	_ = store.SaveSessionID(ctx, "", "s1")
	_, errEmptyTurn := store.IncrementSessionTurnCount(ctx, "")
	assertErr(t, errEmptyTurn, "IncrementSessionTurnCount empty key")
	if cnt, err := store.GetSessionTurnCount(ctx, ""); err != nil || cnt != 0 {
		t.Errorf("GetSessionTurnCount empty key = %d, %v", cnt, err)
	}
	assertErr(t, store.RotateSessionID(ctx, "", "s2"), "RotateSessionID empty key")
	if prev, err := store.GetPreviousSessionID(ctx, ""); err != nil || prev != "" {
		t.Errorf("GetPreviousSessionID empty key = %q, %v", prev, err)
	}
}

func TestTaskHelpers(t *testing.T) {
	// Table-driven CleanTaskSummary
	summaryTests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "plain", input: "Fix bug in auth service", expected: "Fix bug in auth service"},
		{name: "xml_envelope", input: "<USER_REQUEST>Check logs</USER_REQUEST>", expected: "Check logs"},
		{name: "markdown_heading", input: "# Deploy service\nNow deploy", expected: "Deploy service Now deploy"},
		{name: "tag_fallback", input: "<tag>@mention</tag>", expected: "Agent Task"},
		{name: "whitespace_fallback", input: "   ", expected: "Agent Task"},
		{name: "long_text_truncated", input: "This is a very long prompt string " + strings.Repeat("with lots of extra words ", 10), expected: ""},
		{name: "empty", input: "", expected: "Agent Task"},
	}
	for _, tc := range summaryTests {
		t.Run(tc.name, func(t *testing.T) {
			got := CleanTaskSummary(tc.input)
			if tc.name == "long_text_truncated" {
				if !strings.HasSuffix(got, "...") {
					t.Errorf("CleanTaskSummary expected ... suffix, got %q", got)
				}
			} else if got != tc.expected {
				t.Errorf("CleanTaskSummary(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}

	// Auto-cleaned task summary in GetActiveTasks
	database := setupTestDB(t)
	defer database.Close()
	_, _ = database.Exec("INSERT INTO messages (id, thread_id, content, summary, status, created_at, updated_at) VALUES ('m-empty-summ', 't-task', 'Prompt from task content', '', 'PENDING', datetime('now'), datetime('now'))")
	store := NewSQLStore(database)
	tasks, err := store.GetActiveTasks(context.Background())
	if err != nil || len(tasks) != 1 || tasks[0].Summary != "Prompt from task content" {
		t.Errorf("GetActiveTasks auto-cleaned summary failed: %+v, %v", tasks, err)
	}

	// Table-driven InferTriggerType
	triggerTests := []struct {
		name          string
		authorID      string
		scheduleRunID string
		expected      string
	}{
		{name: "http_client", authorID: "http-client", scheduleRunID: "", expected: "http"},
		{name: "cron_prefix", authorID: "user-1", scheduleRunID: "cron-123", expected: "cron"},
		{name: "reminder_run", authorID: "user-1", scheduleRunID: "run-456", expected: "reminder"},
		{name: "user_message", authorID: "user-1", scheduleRunID: "", expected: "discord"},
	}
	for _, tc := range triggerTests {
		t.Run(tc.name, func(t *testing.T) {
			got := InferTriggerType(tc.authorID, tc.scheduleRunID)
			if got != tc.expected {
				t.Errorf("InferTriggerType(%q, %q) = %q, want %q", tc.authorID, tc.scheduleRunID, got, tc.expected)
			}
		})
	}

	// CloseWarn
	closer := &dummyTestCloser{err: errors.New("dummy close error")}
	closeWarn(closer, "test closer")
}

func TestGetActiveTasks_FiltersAndOrdering(t *testing.T) {
	database, err := initTestSQLite(filepath.Join(t.TempDir(), "aerial_tasks_test.db"))
	if err != nil {
		t.Fatalf("initTestSQLite failed: %v", err)
	}
	defer database.Close()
	store := NewSQLStore(database)
	ctx := context.Background()

	// Seed session metadata
	_, err = database.Exec(`INSERT INTO sessions (thread_id, internal_session_id, previous_session_id, summary) VALUES ('th-filter', 'int-sess-1', 'prev-sess-0', 'Summary of thread filter');`)
	if err != nil {
		t.Fatalf("seed session failed: %v", err)
	}

	adminTrue := true
	adminFalse := false
	now := time.Now().UTC().Truncate(time.Second)

	_ = store.InsertMessage(ctx, Message{
		ID:           "msg-f1",
		ThreadID:     "th-filter",
		Content:      "Prompt 1",
		Summary:      "Summary 1",
		ResponseText: "Response 1",
		Status:       StatusCompleted,
		IsAdmin:      true,
		CreatedAt:    now.Add(-3 * time.Minute),
		UpdatedAt:    now.Add(-3 * time.Minute),
	})
	_ = store.InsertMessage(ctx, Message{
		ID:           "msg-f2",
		ThreadID:     "th-filter",
		Content:      "Prompt 2",
		Summary:      "Summary 2",
		ResponseText: "Response 2",
		Status:       StatusPending,
		IsAdmin:      false,
		CreatedAt:    now.Add(-2 * time.Minute),
		UpdatedAt:    now.Add(-2 * time.Minute),
	})
	_ = store.InsertMessage(ctx, Message{
		ID:           "msg-f3",
		ThreadID:     "th-other",
		Content:      "Prompt 3",
		Summary:      "Summary 3",
		ResponseText: "Response 3",
		Status:       StatusProcessing,
		IsAdmin:      true,
		CreatedAt:    now.Add(-1 * time.Minute),
		UpdatedAt:    now.Add(-1 * time.Minute),
	})

	tests := []struct {
		name          string
		filter        TasksFilter
		wantCount     int
		wantFirstID   string
		wantPrevSess  string
		wantThreadSum string
		wantRespText  string
	}{
		{
			name:          "thread_id filter and session join",
			filter:        TasksFilter{ThreadID: "th-filter", Statuses: []string{"COMPLETED"}},
			wantCount:     1,
			wantFirstID:   "msg-f1",
			wantPrevSess:  "prev-sess-0",
			wantThreadSum: "Summary of thread filter",
			wantRespText:  "Response 1",
		},
		{
			name:        "is_admin false filter",
			filter:      TasksFilter{IsAdmin: &adminFalse},
			wantCount:   1,
			wantFirstID: "msg-f2",
		},
		{
			name:        "is_admin true filter with status",
			filter:      TasksFilter{IsAdmin: &adminTrue, Statuses: []string{"PROCESSING"}},
			wantCount:   1,
			wantFirstID: "msg-f3",
		},
		{
			name:        "after_row_id orders asc",
			filter:      TasksFilter{AfterRowID: 1},
			wantCount:   2,
			wantFirstID: "msg-f2",
		},
		{
			name:        "since filter matches recent",
			filter:      TasksFilter{Since: now.Add(-90 * time.Second)},
			wantCount:   1,
			wantFirstID: "msg-f3",
		},
		{
			name:        "before_row_id filter",
			filter:      TasksFilter{BeforeRowID: 1},
			wantCount:   1,
			wantFirstID: "msg-f1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tasks, err := store.GetActiveTasks(ctx, tc.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tasks) != tc.wantCount {
				t.Fatalf("got count %d, want %d", len(tasks), tc.wantCount)
			}
			if tc.wantCount > 0 {
				if tasks[0].ID != tc.wantFirstID {
					t.Errorf("got first ID %q, want %q", tasks[0].ID, tc.wantFirstID)
				}
				if tc.wantPrevSess != "" && tasks[0].PreviousSessionID != tc.wantPrevSess {
					t.Errorf("got PreviousSessionID %q, want %q", tasks[0].PreviousSessionID, tc.wantPrevSess)
				}
				if tc.wantThreadSum != "" && tasks[0].ThreadSummary != tc.wantThreadSum {
					t.Errorf("got ThreadSummary %q, want %q", tasks[0].ThreadSummary, tc.wantThreadSum)
				}
				if tc.wantRespText != "" && tasks[0].ResponseText != tc.wantRespText {
					t.Errorf("got ResponseText %q, want %q", tasks[0].ResponseText, tc.wantRespText)
				}
			}
		})
	}
}
