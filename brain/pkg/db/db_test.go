package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
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

type dummyTestCloser struct {
	err error
}

func (d *dummyTestCloser) Close() error {
	return d.err
}

func TestDBInitAndDSN(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		cfgNil  bool
		snapNil bool
		wantErr bool
		errSub  string
	}{
		{name: "nil_config", cfgNil: true, wantErr: true},
		{name: "empty_url", url: "", wantErr: true},
		{name: "mysql_scheme", url: "mysql://user:pass@localhost/db", wantErr: true, errSub: "only postgres://"},
		{name: "sqlite_prod", url: "sqlite:///tmp/aerial.db", wantErr: true, errSub: "only postgres://"},
		{name: "nil_snapshot", snapNil: true, wantErr: true},
		{name: "bad_syntax", url: "postgres://invalid user@localhost:5432/aerial", wantErr: true, errSub: "invalid postgres"},
		{name: "invalid_percent", url: "postgres://%zz", wantErr: true},
		{name: "unreachable_host", url: "postgres://user:pass@127.0.0.1:54399/nonexistent?sslmode=disable&connect_timeout=1", wantErr: true, errSub: "could not connect to PostgreSQL"},
	}

	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	defer func() { sqliteTestInitHook = oldHook }()

	origAttempts, origBase := postgresMaxAttempts, postgresRetryBase
	postgresMaxAttempts, postgresRetryBase = 1, 1*time.Millisecond
	defer func() { postgresMaxAttempts, postgresRetryBase = origAttempts, origBase }()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.cfgNil {
				_, err = New(nil)
			} else if tc.snapNil {
				_, err = New(&config.Config{})
			} else if tc.name == "invalid_percent" {
				_, err = initDB(tc.url)
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
		if dbHooked, err := New(cfgHooked); err != nil {
			t.Fatalf("expected New with hooked sqlite to succeed: %v", err)
		} else {
			_ = dbHooked.Close()
		}
		if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
			t.Errorf("expected sqlite db file to exist at %s", tmpFile)
		}
	}
	for _, dsn := range []string{"file:mem_1?mode=memory&cache=shared", "file:mem_2?mode=memory&_pragma=busy_timeout(5000)", "sqlite://:memory:"} {
		if dbPragma, err := InitDB(dsn); err != nil {
			t.Fatalf("InitDB(%q) failed: %v", dsn, err)
		} else {
			_ = dbPragma.Close()
		}
	}
	if _, err := InitDB(12345); err == nil {
		t.Fatalf("expected error for non-string InitDB")
	}

	database := setupTestDB(t)
	defer database.Close()
	_, err := database.Exec("SELECT id, row_id, thread_id, guild_id, author_id, author_name, content, status, retry_count, restart_count, error_message, response_text, created_at, updated_at FROM messages LIMIT 0")
	if err != nil {
		t.Fatalf("messages schema check failed: %v", err)
	}
	var fk int
	if err := database.QueryRow("PRAGMA foreign_keys;").Scan(&fk); err != nil || fk != 1 {
		t.Errorf("expected foreign_keys=1, got %d, %v", fk, err)
	}
	closedDB, _ := initTestSQLite(filepath.Join(t.TempDir(), "closed-mig.db"))
	_ = closedDB.Close()
	if err := initSchemaSQLite(closedDB); err == nil {
		t.Errorf("expected error on closed db initSchemaSQLite")
	}
}

func TestStoreUnavailableContract(t *testing.T) {
	database := setupTestDB(t)
	_ = database.Close()
	store := NewSQLStore(database)
	assertStoreUnavailable(t, store)

	assertStoreUnavailable(t, &SQLStore{})

	closeWarn(nil, "test")
	rollbackWarn(nil, "test")
	if isPostgres(nil) {
		t.Errorf("expected isPostgres(nil) to be false")
	}
	if isPostgres(database) {
		t.Errorf("expected isPostgres(sqlite) to be false")
	}
}

func TestMessageLifecycle(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	msg := Message{
		ID:         "msg-1",
		ThreadID:   "th-1",
		GuildID:    "guild-1",
		AuthorID:   "author-1",
		AuthorName: "Alice",
		Content:    "Hello Aerial",
		Status:     StatusPending,
		Metadata: MessageMetadata{
			Mentions:       []string{"@aerial"},
			MentionUserIDs: []string{"1001"},
			MentionRoleIDs: []string{"2001"},
			Attachments:    []string{"https://example.com/file.png"},
			IsAdmin:        true,
		},
	}

	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	// Idempotent duplicate insert
	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("Duplicate InsertMessage failed: %v", err)
	}

	got, err := store.GetMessage(ctx, "msg-1")
	if err != nil || got == nil || got.Content != "Hello Aerial" {
		t.Fatalf("GetMessage failed: %v, got: %+v", err, got)
	}
	if !got.Metadata.IsAdmin || len(got.Metadata.Mentions) != 1 {
		t.Errorf("Metadata mismatch: %+v", got.Metadata)
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

	if err := store.UpdateMessageStatus(ctx, "msg-1", StatusProcessing, "working"); err != nil {
		t.Fatalf("UpdateMessageStatus failed: %v", err)
	}
	if err := store.IncrementMessageRetry(ctx, "msg-1", "timeout"); err != nil {
		t.Fatalf("IncrementMessageRetry failed: %v", err)
	}
	got, _ = store.GetMessage(ctx, "msg-1")
	if got.RetryCount != 1 || got.ErrorMessage != "timeout" {
		t.Errorf("retry mismatch: %+v", got)
	}

	if err := store.ResetMessageToPendingWithRestart(ctx, "msg-1", "recovered"); err != nil {
		t.Fatalf("ResetMessageToPendingWithRestart failed: %v", err)
	}
	got, _ = store.GetMessage(ctx, "msg-1")
	if got.Status != StatusPending || got.RestartCount != 1 {
		t.Errorf("restart mismatch: %+v", got)
	}

	if err := store.UpdateMessageCompleted(ctx, "msg-1", "Task complete"); err != nil {
		t.Fatalf("UpdateMessageCompleted failed: %v", err)
	}
	got, _ = store.GetMessage(ctx, "msg-1")
	if got.Status != StatusCompleted || got.ResponseText != "Task complete" {
		t.Errorf("completed mismatch: %+v", got)
	}

	// Additional messages for querying
	msg2 := Message{ID: "msg-2", ThreadID: "th-1", Content: "Message 2", Status: StatusPending}
	_ = store.InsertMessage(ctx, msg2)
	pending, err := store.GetPendingOrProcessingMessages(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != "msg-2" {
		t.Errorf("GetPendingOrProcessingMessages = %+v, %v", pending, err)
	}

	maxRow, err := store.GetMaxMessageRowID(ctx, "th-1")
	if err != nil || maxRow != 1 {
		t.Errorf("GetMaxMessageRowID = %d, %v, want 1", maxRow, err)
	}

	threadIDs, err := store.GetActiveRecentThreadIDs(ctx, time.Hour)
	if err != nil || len(threadIDs) != 1 || threadIDs[0] != "th-1" {
		t.Errorf("GetActiveRecentThreadIDs = %v, %v", threadIDs, err)
	}

	recent, err := store.GetRecentThreadMessages(ctx, "th-1", 10)
	if err != nil || len(recent) != 2 {
		t.Errorf("GetRecentThreadMessages = %+v, %v", recent, err)
	}

	tasks, err := store.GetActiveTasks(ctx)
	if err != nil || len(tasks) != 1 || tasks[0].ID != "msg-2" {
		t.Errorf("GetActiveTasks = %+v, %v", tasks, err)
	}

	// Empty ID validation
	if err := store.InsertMessage(ctx, Message{ID: ""}); err == nil {
		t.Errorf("expected error inserting empty ID message")
	}
	if err := store.UpdateMessageStatus(ctx, "", StatusCompleted, ""); err == nil {
		t.Errorf("expected error on empty ID UpdateMessageStatus")
	}
	if err := store.UpdateMessageCompleted(ctx, "", "resp"); err == nil {
		t.Errorf("expected error on empty ID UpdateMessageCompleted")
	}
	if err := store.IncrementMessageRetry(ctx, "", "err"); err == nil {
		t.Errorf("expected error on empty ID IncrementMessageRetry")
	}
	if err := store.ResetMessageToPendingWithRestart(ctx, "", "reason"); err == nil {
		t.Errorf("expected error on empty ID ResetMessageToPendingWithRestart")
	}
	if m, err := store.GetMessage(ctx, ""); err != nil || m != nil {
		t.Errorf("expected nil, nil on empty ID GetMessage")
	}
	if exists, err := store.MessageExists(ctx, ""); err != nil || exists {
		t.Errorf("expected false, nil on empty ID MessageExists")
	}
	if claimed, err := store.ClaimPendingMessage(ctx, ""); err != nil || claimed {
		t.Errorf("expected false, nil on empty ID ClaimPendingMessage")
	}
	if maxID, err := store.GetMaxMessageRowID(ctx, ""); err != nil || maxID != 0 {
		t.Errorf("expected 0, nil on empty ID GetMaxMessageRowID")
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
	if err := store.SaveSessionID(ctx, "th-1", "sess-1"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}
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

	if err := store.RotateSessionID(ctx, "th-1", "sess-2"); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}
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
	if err := store.SaveThreadSummary(ctx, "th-1", "Thread summary", "msg-100"); err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}
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
	if ext, _ := store.GetExternalConversationID(ctx, "int-1"); ext != "ext-1" {
		t.Errorf("GetExternalConversationID = %q, want ext-1", ext)
	}

	// 5. Unrotated sessions
	for i := 0; i < 10; i++ {
		_, _ = store.IncrementSessionTurnCount(ctx, "th-1")
	}
	unrotated, err := store.FindUnrotatedSessions(ctx, 5)
	if err != nil || len(unrotated) != 1 || unrotated[0].ThreadID != "th-1" {
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
	if err := store.SaveSessionID(ctx, "", "s1"); err != nil {
		t.Errorf("SaveSessionID empty key err: %v", err)
	}
	if _, err := store.IncrementSessionTurnCount(ctx, ""); err == nil {
		t.Errorf("expected error IncrementSessionTurnCount empty key")
	}
	if cnt, err := store.GetSessionTurnCount(ctx, ""); err != nil || cnt != 0 {
		t.Errorf("GetSessionTurnCount empty key = %d, %v", cnt, err)
	}
	if err := store.RotateSessionID(ctx, "", "s2"); err == nil {
		t.Errorf("expected error RotateSessionID empty key")
	}
	if prev, err := store.GetPreviousSessionID(ctx, ""); err != nil || prev != "" {
		t.Errorf("GetPreviousSessionID empty key = %q, %v", prev, err)
	}
	if ext, err := store.GetExternalConversationID(ctx, ""); err != nil || ext != "" {
		t.Errorf("GetExternalConversationID empty key = %q, %v", ext, err)
	}
}

func TestScheduleLifecycle(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	// 1. OneShot Schedule CRUD & consumption
	osSched := OneShotSchedule{
		ID:        "os-1",
		ThreadID:  "th-1",
		Prompt:    "Run once prompt",
		RunAt:     now.Add(-time.Minute),
		CreatedAt: now,
		Effort:    "low",
	}
	if err := store.CreateOneShotSchedule(ctx, osSched); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}
	dueOS, err := store.GetDueOneShotSchedules(ctx)
	if err != nil || len(dueOS) != 1 || dueOS[0].ID != "os-1" {
		t.Fatalf("GetDueOneShotSchedules = %+v, %v", dueOS, err)
	}
	allOS, err := store.GetAllOneShotSchedules(ctx, "th-1")
	if err != nil || len(allOS) != 1 {
		t.Fatalf("GetAllOneShotSchedules = %+v, %v", allOS, err)
	}
	_, _ = store.GetAllOneShotSchedules(ctx, "")

	msg := Message{ID: "msg-os-1", ThreadID: "th-1", Content: "Executed", Status: StatusPending}
	if err := store.InsertMessageAndConsumeOneShot(ctx, "os-1", msg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}
	if err := store.InsertMessageAndConsumeOneShot(ctx, "non-existent", msg); err == nil {
		t.Errorf("expected error on consuming non-existent one-shot")
	}

	// 2. Cron Schedule CRUD
	cronSched := CronSchedule{
		ID:          "cron-1",
		TargetID:    "chan-1",
		TitlePrefix: "Weather",
		CronExpr:    "0 6 * * *",
		Prompt:      "Morning forecast",
		NextRunAt:   now.Add(-time.Minute),
		Enabled:     true,
		Effort:      "high",
		CreatedAt:   now,
	}
	_, err = database.ExecContext(ctx, `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		cronSched.ID, cronSched.TargetID, cronSched.TitlePrefix, cronSched.CronExpr, cronSched.Prompt, "America/Los_Angeles", cronSched.NextRunAt, cronSched.Enabled, cronSched.CreatedAt, cronSched.Effort)
	if err != nil {
		t.Fatalf("insert cron failed: %v", err)
	}

	dueCron, err := store.GetDueCronSchedules(ctx)
	if err != nil || len(dueCron) != 1 || dueCron[0].Effort != "high" {
		t.Fatalf("GetDueCronSchedules = %+v, %v", dueCron, err)
	}
	allCron, err := store.GetAllCronSchedules(ctx, "chan-1")
	if err != nil || len(allCron) != 1 {
		t.Fatalf("GetAllCronSchedules = %+v, %v", allCron, err)
	}
	_, _ = store.GetAllCronSchedules(ctx, "")
	if err := store.UpdateCronNextRun(ctx, "cron-1", now.Add(time.Hour)); err != nil {
		t.Fatalf("UpdateCronNextRun failed: %v", err)
	}

	// 3. Schedule Runs CRUD
	run := ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		ThreadID:     "th-1",
		Status:       "running",
		StartedAt:    now.Add(-2 * time.Hour),
		Effort:       "high",
		Model:        "claude-3-5",
	}
	if err := store.CreateScheduleRun(ctx, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	updateParams := UpdateRunParams{
		RunID:       "run-1",
		Status:      "completed",
		DurationMs:  1200,
		CompletedAt: now,
		Model:       "claude-3-5",
	}
	if err := store.UpdateScheduleRunStatus(ctx, updateParams); err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}
	_ = store.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-1"})

	runs, total, err := store.GetScheduleRunsPaginated(ctx, 10, 0, "cron-1", "completed")
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("GetScheduleRunsPaginated = len %d, total %d, %v", len(runs), total, err)
	}
	runsClamp, _, _ := store.GetScheduleRunsPaginated(ctx, -5, -10, "", "")
	if len(runsClamp) != 1 {
		t.Errorf("GetScheduleRunsPaginated clamping failed: len=%d", len(runsClamp))
	}

	// 4. Summary metrics
	metrics, err := store.GetScheduleSummaryMetrics(ctx)
	if err != nil || metrics.CronCount != 1 || metrics.TotalRuns24h != 1 {
		t.Errorf("GetScheduleSummaryMetrics = %+v, %v", metrics, err)
	}

	// 5. Orphan reconciliation & pruning
	_ = store.CreateScheduleRun(ctx, ScheduleRun{ID: "run-orphan", ScheduleID: "cron-1", ScheduleType: "cron", Status: "running", StartedAt: now.Add(-3 * time.Hour)})
	reconciled, err := store.ReconcileOrphanedScheduleRuns(ctx)
	if err != nil || reconciled == 0 {
		t.Errorf("ReconcileOrphanedScheduleRuns = %d, %v", reconciled, err)
	}
	pruned, err := store.PruneScheduleRuns(ctx, 100, time.Hour)
	if err != nil {
		t.Errorf("PruneScheduleRuns failed: %v", err)
	}
	_ = pruned

	// 6. Empty validations
	if err := store.CreateScheduleRun(ctx, ScheduleRun{ID: ""}); err == nil {
		t.Errorf("expected error on empty ID CreateScheduleRun")
	}
	if err := store.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: ""}); err == nil {
		t.Errorf("expected error on empty RunID UpdateScheduleRunStatus")
	}
	if err := store.UpdateCronNextRun(ctx, "", time.Now()); err != nil {
		t.Errorf("UpdateCronNextRun empty id = %v", err)
	}
}

func TestFactLifecycle(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	emb1 := make([]float32, ExpectedEmbeddingDim)
	emb1[0] = 1.0
	emb2 := make([]float32, ExpectedEmbeddingDim)
	emb2[0] = 0.9
	emb2[1] = 0.1

	// 1. InsertFact & defaults
	fID1, err := store.InsertFact(ctx, "personal", "Likes matcha latte", 0.9, emb1)
	if err != nil || fID1 <= 0 {
		t.Fatalf("InsertFact 1 failed: %v, id=%d", err, fID1)
	}
	fID2, err := store.InsertFact(ctx, "", "Default cat and imp", -1.0, emb2)
	if err != nil || fID2 <= 0 {
		t.Fatalf("InsertFact 2 with defaults failed: %v, id=%d", err, fID2)
	}
	if _, err := store.InsertFact(ctx, "cat", "", 1.0, nil); err == nil {
		t.Errorf("expected error for empty fact text")
	}

	// 2. SearchSimilarFacts & FindDuplicateFact (tested on in-memory FakeStore, and query paths on SQLStore)
	fakeStore := NewFakeStore()
	_, _ = fakeStore.InsertFact(ctx, "personal", "Likes matcha latte", 0.9, emb1)
	similar, err := fakeStore.SearchSimilarFacts(ctx, emb1, "matcha", 5, 0.5)
	if err != nil || len(similar) == 0 || similar[0].FactText != "Likes matcha latte" {
		t.Fatalf("SearchSimilarFacts failed: %v, similar=%+v", err, similar)
	}
	dup, sim, err := fakeStore.FindDuplicateFact(ctx, emb1, 0.85)
	if err != nil || dup == nil || sim < 0.85 {
		t.Fatalf("FindDuplicateFact failed: dup=%+v, sim=%f, %v", dup, sim, err)
	}
	if resEmpty, err := store.SearchSimilarFacts(ctx, nil, "", 10, 0.2); err != nil || resEmpty != nil {
		t.Errorf("expected nil, nil for empty query and embedding, got %v, %v", resEmpty, err)
	}
	if _, err := store.SearchSimilarFacts(ctx, emb1, "", 10, 0.2); err == nil {
		t.Errorf("expected error running pgvector query on sqlite")
	}

	// 4. GetFactsPaginated
	res, err := store.GetFactsPaginated(ctx, FactsFilter{Category: "personal", Limit: 10})
	if err != nil || res.Total != 1 || len(res.Facts) != 1 {
		t.Fatalf("GetFactsPaginated category failed: %+v, %v", res, err)
	}
	resQ, _ := store.GetFactsPaginated(ctx, FactsFilter{Query: "Default"})
	if len(resQ.Facts) != 1 {
		t.Errorf("GetFactsPaginated query failed: %+v", resQ)
	}
	resOffsetOnly, _ := store.GetFactsPaginated(ctx, FactsFilter{Offset: 1})
	if resOffsetOnly == nil {
		t.Errorf("GetFactsPaginated offset only failed")
	}
	resClamped, _ := store.GetFactsPaginated(ctx, FactsFilter{Limit: -1, Offset: -5})
	if resClamped == nil {
		t.Errorf("GetFactsPaginated clamped failed")
	}
	resEscape, _ := store.GetFactsPaginated(ctx, FactsFilter{Query: "test\\_%", Category: "cat"})
	if resEscape == nil {
		t.Errorf("GetFactsPaginated escape failed")
	}

	// 5. Watermarks & Active conversations
	adminMeta := MessageMetadata{IsAdmin: true}
	_ = store.InsertMessage(ctx, Message{ID: "m-ext-1", ThreadID: "th-1", Status: StatusCompleted, Content: "user prompt", ResponseText: "aerial answer", Metadata: adminMeta})
	_ = store.InsertMessage(ctx, Message{ID: "m-ext-2", ThreadID: "th-1", Status: StatusCompleted, Content: "ambient prompt", ResponseText: "[AMBIENT_SILENT]", Metadata: adminMeta})
	activeConvs, err := store.GetActiveConversationsForExtraction(ctx, 24)
	if err != nil || len(activeConvs) != 1 || activeConvs[0] != "th-1" {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v, active=%v", err, activeConvs)
	}
	turns, err := store.GetUnextractedAdminTurns(ctx, "th-1")
	if err != nil || len(turns) != 1 || turns[0].ID != "m-ext-1" {
		t.Fatalf("GetUnextractedAdminTurns failed: %v, turns=%+v", err, turns)
	}
	if err := store.UpdateConversationFactWatermark(ctx, "th-1", 5); err != nil {
		t.Fatalf("UpdateConversationFactWatermark failed: %v", err)
	}
	if err := store.UpdateConversationFactExtractedAt(ctx, "th-1"); err != nil {
		t.Fatalf("UpdateConversationFactExtractedAt failed: %v", err)
	}
	if convsAfter, _ := store.GetActiveConversationsForExtraction(ctx, 24); len(convsAfter) != 0 {
		t.Errorf("expected 0 active conversations after watermark catch-up, got %d", len(convsAfter))
	}

	// 6. UpdateFactEmbedding
	embUpdated := make([]float32, ExpectedEmbeddingDim)
	embUpdated[1] = 1.0
	if err := store.UpdateFactEmbedding(ctx, fID2, embUpdated); err != nil {
		t.Fatalf("UpdateFactEmbedding failed: %v", err)
	}
	if err := store.UpdateFactEmbedding(ctx, fID2, []float32{1.0, 2.0}); err == nil {
		t.Errorf("expected error updating fact embedding with invalid dim")
	}

	// 7. ReinforceFact & DecayAndPruneFacts
	_ = fakeStore.ReinforceFact(ctx, 1, "Likes iced matcha latte", emb1, 0.1)
	if err := store.ReinforceFact(ctx, fID1, "Likes iced matcha latte", emb1, 0.1); err == nil {
		t.Errorf("expected error on SQLite for ::numeric syntax in ReinforceFact")
	}
	if _, _, err := store.DecayAndPruneFacts(ctx, 0.1, 0.2, 30); err == nil {
		t.Errorf("expected error on SQLite for ::numeric syntax in DecayAndPruneFacts")
	}

	// 8. Missing embeddings & nil context
	missing, err := store.GetFactsMissingEmbeddings(ctx, 10)
	if err != nil {
		t.Fatalf("GetFactsMissingEmbeddings failed: %v", err)
	}
	_ = missing

	idNilCtx, err := store.InsertFact(nil, "cat", "fact text nil ctx", 1.0, emb1)
	if err != nil || idNilCtx <= 0 {
		t.Fatalf("InsertFact with nil ctx failed: %v", err)
	}
	resNilCtx, err := store.GetFactsPaginated(nil, FactsFilter{Limit: 5})
	if err != nil || resNilCtx.Total == 0 {
		t.Fatalf("GetFactsPaginated with nil ctx failed: %v", err)
	}
}

func TestVectorAndArrayHelpers(t *testing.T) {
	// 1. Table-driven cosineSimilarity
	simTests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float64
	}{
		{name: "both_nil", a: nil, b: nil, expected: 0.0},
		{name: "one_empty", a: []float32{}, b: []float32{1.0}, expected: 0.0},
		{name: "mismatched_len", a: []float32{1.0}, b: []float32{1.0, 2.0}, expected: 0.0},
		{name: "zero_vector", a: []float32{0.0, 0.0}, b: []float32{1.0, 1.0}, expected: 0.0},
		{name: "identical", a: []float32{1.0, 0.0}, b: []float32{1.0, 0.0}, expected: 1.0},
	}
	for _, tc := range simTests {
		t.Run(tc.name, func(t *testing.T) {
			got := cosineSimilarity(tc.a, tc.b)
			if math.Abs(got-tc.expected) > 0.001 {
				t.Errorf("cosineSimilarity() = %f, want %f", got, tc.expected)
			}
		})
	}

	// 2. Table-driven NullVector Scan
	nvTests := []struct {
		name    string
		input   any
		wantErr bool
		valid   bool
		len     int
	}{
		{name: "nil", input: nil, wantErr: false, valid: false, len: 0},
		{name: "invalid_type", input: 12345, wantErr: true},
		{name: "invalid_json", input: "[bad", wantErr: true},
		{name: "valid_string", input: "[0.1,0.2,0.3]", wantErr: false, valid: true, len: 3},
		{name: "valid_bytes", input: []byte("[0.1,0.2]"), wantErr: false, valid: true, len: 2},
	}
	for _, tc := range nvTests {
		t.Run(tc.name, func(t *testing.T) {
			var nv NullVector
			err := nv.Scan(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NullVector.Scan() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (nv.Valid != tc.valid || len(nv.Vector) != tc.len) {
				t.Errorf("NullVector state mismatch: valid=%v, len=%d", nv.Valid, len(nv.Vector))
			}
		})
	}

	// 3. Table-driven FactTime Scan
	now := time.Now()
	ftTests := []struct {
		name    string
		input   any
		wantErr bool
		valid   bool
	}{
		{name: "nil", input: nil, wantErr: false, valid: false},
		{name: "time_val", input: now, wantErr: false, valid: true},
		{name: "time_ptr", input: &now, wantErr: false, valid: true},
		{name: "nil_time_ptr", input: (*time.Time)(nil), wantErr: false, valid: false},
		{name: "empty_str", input: "", wantErr: false, valid: false},
		{name: "empty_bytes", input: []byte(""), wantErr: false, valid: false},
		{name: "valid_bytes", input: []byte(now.Format(time.RFC3339)), wantErr: false, valid: true},
		{name: "invalid_str", input: "not-a-time", wantErr: true},
	}
	for _, tc := range ftTests {
		t.Run(tc.name, func(t *testing.T) {
			var ft FactTime
			err := ft.Scan(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FactTime.Scan() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && ft.Valid != tc.valid {
				t.Errorf("FactTime.Valid = %v, want %v", ft.Valid, tc.valid)
			}
		})
	}

	// 4. Table-driven parsePostgresTextArray
	arrTests := []struct {
		name     string
		input    string
		expected []string
	}{
		{name: "empty", input: "", expected: []string{}},
		{name: "braces_only", input: "{}", expected: []string{}},
		{name: "spaces_in_braces", input: "{   }", expected: []string{}},
		{name: "elements", input: "{a,b,c}", expected: []string{"a", "b", "c"}},
	}
	for _, tc := range arrTests {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresTextArray(tc.input)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("parsePostgresTextArray(%q) = %#v, want %#v", tc.input, got, tc.expected)
			}
		})
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

	// CloseWarn and RollbackWarn
	closer := &dummyTestCloser{err: errors.New("dummy close error")}
	closeWarn(closer, "test closer")
	tx, _ := database.Begin()
	_ = tx.Rollback()
	rollbackWarn(tx, "done tx")
}

func TestIsExtractableAdminTurn(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want bool
	}{
		{
			name: "not_completed",
			msg:  Message{Status: StatusPending, Metadata: MessageMetadata{IsAdmin: true}, ResponseText: "hello"},
			want: false,
		},
		{
			name: "not_admin",
			msg:  Message{Status: StatusCompleted, Metadata: MessageMetadata{IsAdmin: false}, ResponseText: "hello"},
			want: false,
		},
		{
			name: "empty_response",
			msg:  Message{Status: StatusCompleted, Metadata: MessageMetadata{IsAdmin: true}, ResponseText: "   "},
			want: false,
		},
		{
			name: "starts_with_bracket",
			msg:  Message{Status: StatusCompleted, Metadata: MessageMetadata{IsAdmin: true}, ResponseText: "[thinking] done"},
			want: false,
		},
		{
			name: "valid_admin_turn",
			msg:  Message{Status: StatusCompleted, Metadata: MessageMetadata{IsAdmin: true}, ResponseText: "All done!"},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsExtractableAdminTurn(tc.msg); got != tc.want {
				t.Fatalf("IsExtractableAdminTurn() = %v, want %v", got, tc.want)
			}
		})
	}
}
