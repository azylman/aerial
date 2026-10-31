package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
	for range 10 {
		_, _ = store.IncrementSessionTurnCount(ctx, "th-1")
	}
	if unrotated, err := store.FindUnrotatedSessions(ctx, 5); err != nil || len(unrotated) != 1 || unrotated[0].ThreadID != "th-1" {
		t.Errorf("FindUnrotatedSessions = %+v, %v", unrotated, err)
	}
	_, _ = store.FindUnrotatedSessions(ctx, 0)
	_, _ = store.FindUnrotatedSessions(ctx, 5, time.Hour)
	_, _ = store.FindUnrotatedSessions(ctx, 5, -1)

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
		{name: "monotonic_clock_str", input: "2026-10-19 12:00:00 m=+0.000000001", wantOK: true},
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
	if cnt, err := store.GetSessionTurnCount(ctx, "nonexistent-thread"); err != nil || cnt != 0 {
		t.Errorf("GetSessionTurnCount nonexistent = %d, %v", cnt, err)
	}
	assertErr(t, store.RotateSessionID(ctx, "", "s2"), "RotateSessionID empty key")
	if prev, err := store.GetPreviousSessionID(ctx, ""); err != nil || prev != "" {
		t.Errorf("GetPreviousSessionID empty key = %q, %v", prev, err)
	}
	_ = store.SaveThreadSummary(ctx, "", "sum", "mid")
	if sum, last, err := store.GetThreadSummary(ctx, ""); err != nil || sum != "" || last != "" {
		t.Errorf("GetThreadSummary empty thread = %q, %q, %v", sum, last, err)
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
		name     string
		authorID string
		expected string
	}{
		{name: "dispatch_prefix", authorID: "dispatch:heartbeat", expected: "heartbeat"},
		{name: "http_client", authorID: "http-client", expected: "http"},
		{name: "user_message", authorID: "user-1", expected: "discord"},
	}
	for _, tc := range triggerTests {
		t.Run(tc.name, func(t *testing.T) {
			got := InferTriggerType(tc.authorID)
			if got != tc.expected {
				t.Errorf("InferTriggerType(%q) = %q, want %q", tc.authorID, got, tc.expected)
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
	_, err = database.Exec(`INSERT INTO sessions (session_id, conversation_id, previous_conversation_id, summary) VALUES ('discord-th-filter', 'int-sess-1', 'prev-sess-0', 'Summary of thread filter');`)
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
		Effort:       "medium",
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
		wantSess      string
		checkSess     bool
		wantPrevSess  string
		wantThreadSum string
		wantRespText  string
	}{
		{
			name:          "thread_id filter and session join",
			filter:        TasksFilter{ThreadID: "th-filter", Statuses: []string{"COMPLETED"}},
			wantCount:     1,
			wantFirstID:   "msg-f1",
			wantSess:      "int-sess-1",
			checkSess:     true,
			wantPrevSess:  "prev-sess-0",
			wantThreadSum: "Summary of thread filter",
			wantRespText:  "Response 1",
		},
		{
			name:        "is_admin false filter matches thread session",
			filter:      TasksFilter{IsAdmin: &adminFalse},
			wantCount:   1,
			wantFirstID: "msg-f2",
			wantSess:    "int-sess-1",
			checkSess:   true,
		},
		{
			name:        "is_admin true filter with status",
			filter:      TasksFilter{IsAdmin: &adminTrue, Statuses: []string{"PROCESSING"}},
			wantCount:   1,
			wantFirstID: "msg-f3",
		},
		{
			name:        "multiple statuses filter",
			filter:      TasksFilter{Statuses: []string{"PENDING", "PROCESSING"}},
			wantCount:   2,
			wantFirstID: "msg-f3",
		},
		{
			name:        "clamped limit above 200",
			filter:      TasksFilter{Limit: 250},
			wantCount:   3,
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
				if tc.wantFirstID != "" && tasks[0].ID != tc.wantFirstID {
					t.Errorf("got first ID %q, want %q", tasks[0].ID, tc.wantFirstID)
				}
				if tc.checkSess && tasks[0].SessionID != tc.wantSess {
					t.Errorf("got SessionID %q, want %q", tasks[0].SessionID, tc.wantSess)
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

func TestDispatchStore_SQLite(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()
	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Insert & Get
	d1 := Dispatch{
		ID:        "disp-1",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Prompt for dispatch 1",
	}
	mustOK(t, store.InsertDispatch(ctx, d1), "InsertDispatch disp-1")

	got, err := store.GetDispatch(ctx, "disp-1")
	mustOK(t, err, "GetDispatch disp-1")
	if got == nil || got.ID != "disp-1" || got.Status != StatusPending || got.Summary == "" {
		t.Fatalf("unexpected dispatch retrieved: %+v", got)
	}

	// 2. Claim pending
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

	// 3. Update status & retry
	mustOK(t, store.UpdateDispatchStatus(ctx, "disp-1", "PROCESSING", "in flight"), "UpdateDispatchStatus")
	mustOK(t, store.IncrementDispatchRetry(ctx, "disp-1", "network error"), "IncrementDispatchRetry")
	got, _ = store.GetDispatch(ctx, "disp-1")
	if got.RetryCount != 1 || got.ErrorMessage != "network error" {
		t.Errorf("unexpected retry state: %+v", got)
	}

	// 4. Reset to pending with restart
	mustOK(t, store.ResetDispatchToPendingWithRestart(ctx, "disp-1", "runner restart"), "ResetDispatchToPendingWithRestart")
	got, _ = store.GetDispatch(ctx, "disp-1")
	if got.Status != StatusPending || got.RestartCount != 1 {
		t.Errorf("unexpected restart state: %+v", got)
	}

	// 5. Update completed with conversation ID
	mustOK(t, store.UpdateDispatchCompleted(ctx, "disp-1", "Result text", "conv-uuid-123"), "UpdateDispatchCompleted")
	got, _ = store.GetDispatch(ctx, "disp-1")
	if got.Status != StatusCompleted || got.ResponseText != "Result text" || got.ConversationID != "conv-uuid-123" {
		t.Errorf("unexpected completed state: %+v", got)
	}

	// 6. Recent session dispatches
	d2 := Dispatch{
		ID:        "disp-2",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Prompt for dispatch 2",
	}
	mustOK(t, store.InsertDispatch(ctx, d2), "InsertDispatch disp-2")
	mustOK(t, store.UpdateDispatchCompleted(ctx, "disp-2", "Result 2"), "UpdateDispatchCompleted disp-2")

	recent, err := store.GetRecentSessionDispatches(ctx, "heartbeat-hb-1", 10)
	mustOK(t, err, "GetRecentSessionDispatches")
	if len(recent) != 2 || recent[0].ID != "disp-1" || recent[1].ID != "disp-2" {
		t.Errorf("unexpected recent session dispatches: %+v", recent)
	}

	// 7. Active check
	d3 := Dispatch{
		ID:        "disp-3",
		Source:    "vanadis",
		SessionID: "build-1",
		Prompt:    "Build ID: b-123\nChunk 1",
	}
	mustOK(t, store.InsertDispatch(ctx, d3), "InsertDispatch disp-3")
	hasActive, err := store.HasActiveDispatchTurn(ctx, "build-1", "")
	mustOK(t, err, "HasActiveDispatchTurn")
	if !hasActive {
		t.Errorf("expected active dispatch for session build-1")
	}

	// 8. Cancel pending dispatches
	cancelled, err := store.CancelPendingDispatches(ctx, "", "b-123", "aborted build")
	mustOK(t, err, "CancelPendingDispatches")
	if len(cancelled) != 1 || cancelled[0] != "disp-3" {
		t.Errorf("expected cancelled [disp-3], got %v", cancelled)
	}
	got, _ = store.GetDispatch(ctx, "disp-3")
	if got.Status != "FAILED" || got.ErrorMessage != "aborted build" {
		t.Errorf("unexpected status after cancel: %+v", got)
	}

	// 9. Pending or processing dispatches
	d4 := Dispatch{
		ID:        "disp-4-pending",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-2",
		Prompt:    "Prompt 4",
		Status:    StatusPending,
	}
	mustOK(t, store.InsertDispatch(ctx, d4), "InsertDispatch disp-4-pending")
	pendingDisps, err := store.GetPendingOrProcessingDispatches(ctx, 10)
	mustOK(t, err, "GetPendingOrProcessingDispatches")
	if len(pendingDisps) != 1 || pendingDisps[0].ID != "disp-4-pending" {
		t.Errorf("unexpected pending dispatches: %+v", pendingDisps)
	}
	pendingLimited, _ := store.GetPendingOrProcessingDispatches(ctx, 1)
	if len(pendingLimited) != 1 {
		t.Errorf("expected 1 limited pending dispatch")
	}

	// 10. UpdateDispatchCompleted without conversationID
	mustOK(t, store.UpdateDispatchCompleted(ctx, "disp-4-pending", "Done 4"), "UpdateDispatchCompleted without conv")
	got, _ = store.GetDispatch(ctx, "disp-4-pending")
	if got.Status != StatusCompleted || got.ResponseText != "Done 4" {
		t.Errorf("unexpected completed state without conv: %+v", got)
	}

	// 11. Edge cases and validation guards
	assertErr(t, store.InsertDispatch(ctx, Dispatch{}), "InsertDispatch empty ID")
	assertErr(t, store.UpdateDispatchStatus(ctx, "", "PROCESSING", ""), "UpdateDispatchStatus empty ID")
	assertErr(t, store.IncrementDispatchRetry(ctx, "", "err"), "IncrementDispatchRetry empty ID")
	assertErr(t, store.ResetDispatchToPendingWithRestart(ctx, "", "rst"), "ResetDispatchToPendingWithRestart empty ID")

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
	activeEmpty, err := store.HasActiveDispatchTurn(ctx, "", "")
	if err != nil || activeEmpty {
		t.Errorf("expected false, nil for empty HasActiveDispatchTurn")
	}
	cancelEmpty, err := store.CancelPendingDispatches(ctx, "", "", "reason")
	if err != nil || cancelEmpty != nil {
		t.Errorf("expected nil, nil for empty CancelPendingDispatches")
	}
	scannedNil, err := scanDispatches(nil)
	if err != nil || scannedNil != nil {
		t.Errorf("expected nil, nil for scanDispatches(nil)")
	}

	// 12. UpdateMessageCompleted with and without conversationID
	mustOK(t, store.InsertMessage(ctx, Message{ID: "msg-cov-1", Content: "Coverage"}), "InsertMessage")
	mustOK(t, store.UpdateMessageCompleted(ctx, "msg-cov-1", "Done without conv"), "UpdateMessageCompleted without conv")
	covMsg, _ := store.GetMessage(ctx, "msg-cov-1")
	if covMsg.ResponseText != "Done without conv" {
		t.Errorf("unexpected response text")
	}
	mustOK(t, store.UpdateMessageCompleted(ctx, "msg-cov-1", "Done with conv", "conv-cov-uuid"), "UpdateMessageCompleted with conv")
	covMsg, _ = store.GetMessage(ctx, "msg-cov-1")
	if covMsg.ResponseText != "Done with conv" || covMsg.ConversationID != "conv-cov-uuid" {
		t.Errorf("unexpected response text or conversation ID: %+v", covMsg)
	}
}

func TestGetActiveRecentThreadIDs_StrippingDiscordPrefix(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()
	store := NewSQLStore(database)
	ctx := context.Background()

	mustOK(t, store.SaveSessionID(ctx, "112233445566778899", "conv-sess-1"), "SaveSessionID numeric")
	mustOK(t, store.SaveSessionID(ctx, "discord-998877665544332211", "conv-sess-2"), "SaveSessionID discord prefixed")

	threadIDs, err := store.GetActiveRecentThreadIDs(ctx, time.Hour)
	mustOK(t, err, "GetActiveRecentThreadIDs")

	found1 := slices.Contains(threadIDs, "112233445566778899")
	found2 := slices.Contains(threadIDs, "998877665544332211")
	if !found1 || !found2 {
		t.Errorf("GetActiveRecentThreadIDs missing stripped thread IDs: got %v", threadIDs)
	}
}

func TestGetActiveTasks_DispatchesUnion_SQLite(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()
	store := NewSQLStore(database)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)

	// Insert message
	mustOK(t, store.InsertMessage(ctx, Message{
		ID:        "msg-live-1",
		ThreadID:  "123456",
		Content:   "Discord user prompt",
		Status:    StatusPending,
		CreatedAt: now.Add(-2 * time.Minute),
		UpdatedAt: now.Add(-2 * time.Minute),
	}), "InsertMessage")

	// Insert stateful dispatch
	mustOK(t, store.InsertDispatch(ctx, Dispatch{
		ID:        "disp-live-stateful",
		Source:    "heartbeat",
		SessionID: "heartbeat-hb-1",
		Prompt:    "Heartbeat prompt",
		Status:    StatusPending,
		CreatedAt: now.Add(-1 * time.Minute),
		UpdatedAt: now.Add(-1 * time.Minute),
	}), "InsertDispatch stateful")

	// Insert ephemeral dispatch
	mustOK(t, store.InsertDispatch(ctx, Dispatch{
		ID:        "disp-live-ephem",
		Source:    "webhook",
		SessionID: "",
		Prompt:    "Webhook prompt",
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}), "InsertDispatch ephem")

	// Unfiltered query unions messages and dispatches
	unfiltered, err := store.GetActiveTasks(ctx)
	mustOK(t, err, "GetActiveTasks unfiltered")
	if len(unfiltered) != 3 {
		t.Fatalf("expected 3 tasks in unfiltered HUD, got %d", len(unfiltered))
	}
	if unfiltered[0].ID != "msg-live-1" || unfiltered[1].ID != "disp-live-stateful" || unfiltered[2].ID != "disp-live-ephem" {
		t.Errorf("unexpected task order: %+v", unfiltered)
	}
	if unfiltered[1].TriggerType != "heartbeat" || unfiltered[2].TriggerType != "webhook" {
		t.Errorf("unexpected trigger types: %s, %s", unfiltered[1].TriggerType, unfiltered[2].TriggerType)
	}
	if unfiltered[2].ThreadID != "dispatch-disp-live-ephem" {
		t.Errorf("unexpected threadID for ephemeral dispatch: %s", unfiltered[2].ThreadID)
	}

	// Filtered query excludes dispatches
	filtered, err := store.GetActiveTasks(ctx, TasksFilter{Statuses: []string{"PENDING"}})
	mustOK(t, err, "GetActiveTasks filtered")
	if len(filtered) != 1 || filtered[0].ID != "msg-live-1" {
		t.Errorf("expected filtered tasks to contain only messages, got %+v", filtered)
	}
}
