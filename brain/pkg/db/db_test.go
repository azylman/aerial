package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDBInitializationAndMigrations(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Verify messages table exists with response_text
	_, err = database.Exec("SELECT id, thread_id, guild_id, author_id, author_name, content, status, retry_count, error_message, response_text, created_at, updated_at FROM messages LIMIT 0")
	if err != nil {
		t.Fatalf("Messages table schema error: %v", err)
	}

	// Verify sessions table exists
	_, err = database.Exec("SELECT thread_id, internal_session_id, created_at, updated_at FROM sessions LIMIT 0")
	if err != nil {
		t.Fatalf("Sessions table schema error: %v", err)
	}

	// Verify busy_timeout PRAGMA
	var busyTimeout int
	if err := database.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("Failed to query PRAGMA busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("Expected busy_timeout=5000, got %d", busyTimeout)
	}
}

func TestDuplicateMessageInsertDoesNotResetStatus(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	msg := Message{
		ID:         "msg-dup-1",
		ThreadID:   "thread-1",
		GuildID:    "guild-1",
		AuthorID:   "author-1",
		AuthorName: "Alice",
		Content:    "Original prompt",
		Status:     StatusProcessing,
	}

	if err := InsertMessage(database, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	// Attempt duplicate insertion with PENDING status (e.g. gateway re-delivery)
	dupMsg := Message{
		ID:         "msg-dup-1",
		ThreadID:   "thread-1",
		GuildID:    "guild-1",
		AuthorID:   "author-1",
		AuthorName: "Alice",
		Content:    "Duplicate event prompt",
		Status:     StatusPending,
	}
	if err := InsertMessage(database, dupMsg); err != nil {
		t.Fatalf("Failed to insert duplicate message: %v", err)
	}

	// Verify status is still PROCESSING and content unchanged
	fetched, err := GetMessage(database, "msg-dup-1")
	if err != nil || fetched == nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	if fetched.Status != StatusProcessing {
		t.Errorf("Expected status to remain PROCESSING, got %s", fetched.Status)
	}
	if fetched.Content != "Original prompt" {
		t.Errorf("Expected content to remain 'Original prompt', got %s", fetched.Content)
	}
}

func TestUpdateMessageCompleted(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	msg := Message{
		ID:       "msg-comp-1",
		ThreadID: "thread-1",
		Content:  "Some question",
		Status:   StatusProcessing,
	}
	if err := InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	if err := UpdateMessageCompleted(database, "msg-comp-1", "This is the final AI response"); err != nil {
		t.Fatalf("UpdateMessageCompleted failed: %v", err)
	}

	fetched, _ := GetMessage(database, "msg-comp-1")
	if fetched.Status != StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", fetched.Status)
	}
	if fetched.ResponseText != "This is the final AI response" {
		t.Errorf("Expected ResponseText to be set, got %q", fetched.ResponseText)
	}
}

func TestMessageCRUD(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	t1 := time.Now().UTC().Add(-10 * time.Minute)
	t2 := time.Now().UTC().Add(-5 * time.Minute)

	msg1 := Message{
		ID:         "msg-1",
		ThreadID:   "thread-1",
		GuildID:    "guild-1",
		AuthorID:   "author-1",
		AuthorName: "Alice",
		Content:    "First prompt",
		Status:     StatusPending,
		CreatedAt:  t1,
		UpdatedAt:  t1,
	}

	msg2 := Message{
		ID:         "msg-2",
		ThreadID:   "thread-2",
		GuildID:    "guild-1",
		AuthorID:   "author-2",
		AuthorName: "Bob",
		Content:    "Second prompt",
		Status:     StatusProcessing,
		CreatedAt:  t2,
		UpdatedAt:  t2,
	}

	// 1. Insert messages
	if err := InsertMessage(database, msg1); err != nil {
		t.Fatalf("Failed to insert msg1: %v", err)
	}
	if err := InsertMessage(database, msg2); err != nil {
		t.Fatalf("Failed to insert msg2: %v", err)
	}

	// 2. GetMessage
	fetched, err := GetMessage(database, "msg-1")
	if err != nil {
		t.Fatalf("Failed to get msg-1: %v", err)
	}
	if fetched == nil || fetched.ID != "msg-1" || fetched.AuthorName != "Alice" || fetched.Status != StatusPending {
		t.Fatalf("Unexpected message retrieved: %+v", fetched)
	}

	// 3. GetPendingOrProcessingMessages (chronological order)
	pending, err := GetPendingOrProcessingMessages(database)
	if err != nil {
		t.Fatalf("Failed to get pending messages: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("Expected 2 pending messages, got %d", len(pending))
	}
	if pending[0].ID != "msg-1" || pending[1].ID != "msg-2" {
		t.Errorf("Expected msg-1 then msg-2, got %s, %s", pending[0].ID, pending[1].ID)
	}

	// 4. Increment retry
	if err := IncrementMessageRetry(database, "msg-1", "503 Unavailable"); err != nil {
		t.Fatalf("Failed to increment retry: %v", err)
	}
	fetched, _ = GetMessage(database, "msg-1")
	if fetched.RetryCount != 1 || fetched.ErrorMessage != "503 Unavailable" {
		t.Errorf("Expected retry_count=1 and error message set, got count=%d, err=%s", fetched.RetryCount, fetched.ErrorMessage)
	}

	// 5. Update status to COMPLETED
	if err := UpdateMessageStatus(database, "msg-1", StatusCompleted, ""); err != nil {
		t.Fatalf("Failed to update status to COMPLETED: %v", err)
	}

	// 6. Update status of msg2 to FAILED
	if err := UpdateMessageStatus(database, "msg-2", StatusFailed, "fatal error"); err != nil {
		t.Fatalf("Failed to update status to FAILED: %v", err)
	}

	// Verify GetPendingOrProcessingMessages returns empty now
	pending, err = GetPendingOrProcessingMessages(database)
	if err != nil {
		t.Fatalf("Failed to query pending: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("Expected 0 pending messages after completion/failure, got %d", len(pending))
	}
}

func TestSessionCRUD(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// 1. Query non-existent session
	sessID, err := GetSessionID(database, "thread-100")
	if err != nil {
		t.Fatalf("Error querying non-existent session: %v", err)
	}
	if sessID != "" {
		t.Errorf("Expected empty session ID, got %s", sessID)
	}

	// 2. Save session
	if err := SaveSessionID(database, "thread-100", "uuid-abc"); err != nil {
		t.Fatalf("Failed to save session ID: %v", err)
	}
	sessID, err = GetSessionID(database, "thread-100")
	if err != nil || sessID != "uuid-abc" {
		t.Errorf("Expected uuid-abc, got %s (err: %v)", sessID, err)
	}

	// 3. Update session (upsert)
	if err := SaveSessionID(database, "thread-100", "uuid-def"); err != nil {
		t.Fatalf("Failed to update session ID: %v", err)
	}
	sessID, err = GetSessionID(database, "thread-100")
	if err != nil || sessID != "uuid-def" {
		t.Errorf("Expected uuid-def after update, got %s", sessID)
	}

	// 4. Delete session
	if err := DeleteSessionID(database, "thread-100"); err != nil {
		t.Fatalf("Failed to delete session ID: %v", err)
	}
	sessID, err = GetSessionID(database, "thread-100")
	if err != nil || sessID != "" {
		t.Errorf("Expected empty session after delete, got %s", sessID)
	}
}

func TestIncrementSessionTurnCountAndRotation(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionKey := "channel-rotation-test"

	// 1. Initial increment on non-existent record should return 1
	c1, err := IncrementSessionTurnCount(database, sessionKey)
	if err != nil {
		t.Fatalf("IncrementSessionTurnCount 1 failed: %v", err)
	}
	if c1 != 1 {
		t.Errorf("Expected turn_count=1, got %d", c1)
	}

	// 2. Increments should be atomic and sequential
	c2, err := IncrementSessionTurnCount(database, sessionKey)
	if err != nil || c2 != 2 {
		t.Fatalf("Expected turn_count=2, got %d (err: %v)", c2, err)
	}
	c3, err := IncrementSessionTurnCount(database, sessionKey)
	if err != nil || c3 != 3 {
		t.Fatalf("Expected turn_count=3, got %d (err: %v)", c3, err)
	}

	// 3. Set a session ID and semantic memory watermarks
	if err := SaveSessionID(database, sessionKey, "sess-old-uuid"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}
	if err := UpdateConversationFactWatermark(database, sessionKey, 42); err != nil {
		t.Fatalf("UpdateConversationFactWatermark failed: %v", err)
	}

	// Verify pre-rotation state
	countBefore, _ := GetSessionTurnCount(database, sessionKey)
	if countBefore != 3 {
		t.Errorf("Expected turn_count=3 before rotation, got %d", countBefore)
	}
	sessBefore, _ := GetSessionID(database, sessionKey)
	if sessBefore != "sess-old-uuid" {
		t.Errorf("Expected sess-old-uuid before rotation, got %s", sessBefore)
	}

	// 4. Rotate session ID
	newSessionID := "sess-new-uuid"
	if err := RotateSessionID(database, sessionKey, newSessionID); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}

	// Verify post-rotation state: new session ID, turn_count reset to 0
	sessAfter, err := GetSessionID(database, sessionKey)
	if err != nil || sessAfter != newSessionID {
		t.Errorf("Expected rotated session ID %s, got %s (err: %v)", newSessionID, sessAfter, err)
	}
	countAfter, err := GetSessionTurnCount(database, sessionKey)
	if err != nil || countAfter != 0 {
		t.Errorf("Expected turn_count=0 after rotation, got %d (err: %v)", countAfter, err)
	}

	// 5. Verify semantic memory watermarks are PRESERVED after rotation!
	var lastRowID int64
	var factExtractedAt sql.NullTime
	err = database.QueryRow("SELECT last_extracted_rowid, fact_extracted_at FROM sessions WHERE thread_id = ?", sessionKey).Scan(&lastRowID, &factExtractedAt)
	if err != nil {
		t.Fatalf("Failed to query sessions table for watermarks: %v", err)
	}
	if lastRowID != 42 {
		t.Errorf("Expected last_extracted_rowid=42 preserved after rotation, got %d", lastRowID)
	}
	if !factExtractedAt.Valid {
		t.Errorf("Expected fact_extracted_at to be preserved after rotation, got NULL")
	}

	// 6. Test rotation on completely new session key
	newKey := "brand-new-channel"
	if err := RotateSessionID(database, newKey, "brand-new-uuid"); err != nil {
		t.Fatalf("RotateSessionID on new key failed: %v", err)
	}
	newSess, _ := GetSessionID(database, newKey)
	if newSess != "brand-new-uuid" {
		t.Errorf("Expected brand-new-uuid, got %s", newSess)
	}
	newCount, _ := GetSessionTurnCount(database, newKey)
	if newCount != 0 {
		t.Errorf("Expected turn_count=0 on brand new rotated session, got %d", newCount)
	}
}

func TestDBNilAndErrorHandling(t *testing.T) {
	// Nil DB handling
	if err := InsertMessage(nil, Message{ID: "1"}); err == nil {
		t.Error("Expected error inserting into nil DB")
	}
	if err := UpdateMessageStatus(nil, "1", StatusCompleted, ""); err == nil {
		t.Error("Expected error updating status in nil DB")
	}
	if err := IncrementMessageRetry(nil, "1", ""); err == nil {
		t.Error("Expected error incrementing retry in nil DB")
	}
	if _, err := GetPendingOrProcessingMessages(nil); err == nil {
		t.Error("Expected error getting pending messages from nil DB")
	}
	if _, err := GetMessage(nil, "1"); err == nil {
		t.Error("Expected error getting message from nil DB")
	}
	if _, err := GetSessionID(nil, "1"); err != nil {
		t.Errorf("Expected nil error for GetSessionID with nil DB, got: %v", err)
	}
	if err := SaveSessionID(nil, "1", "s"); err != nil {
		t.Errorf("Expected nil error for SaveSessionID with nil DB, got: %v", err)
	}
	if err := DeleteSessionID(nil, "1"); err != nil {
		t.Errorf("Expected nil error for DeleteSessionID with nil DB, got: %v", err)
	}
	if _, err := IncrementSessionTurnCount(nil, "1"); err == nil {
		t.Error("Expected error for IncrementSessionTurnCount with nil DB")
	}
	if err := RotateSessionID(nil, "1", "s"); err == nil {
		t.Error("Expected error for RotateSessionID with nil DB")
	}

	// Closed DB handling
	database, _ := InitDB(":memory:")
	if _, err := IncrementSessionTurnCount(database, ""); err == nil {
		t.Error("Expected error for IncrementSessionTurnCount with empty key")
	}
	if err := RotateSessionID(database, "", "s"); err == nil {
		t.Error("Expected error for RotateSessionID with empty key")
	}

	_ = database.Close()

	if err := InsertMessage(database, Message{ID: "1"}); err == nil {
		t.Error("Expected error on closed DB")
	}
	if _, err := GetSessionID(database, "1"); err == nil {
		t.Error("Expected error getting session from closed DB")
	}
}

func TestSchedulesCompatibility(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// 1. Test OneShotSchedule CRUD
	oneShot := OneShotSchedule{
		ID:        "sched-1",
		ThreadID:  "thread-1",
		Prompt:    "test reminder",
		RunAt:     time.Now().UTC().Add(-1 * time.Minute),
		CreatedAt: time.Now().UTC(),
	}
	if err := CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("Failed to create one-shot schedule: %v", err)
	}

	futureOneShot := OneShotSchedule{
		ID:        "sched-future",
		ThreadID:  "thread-1",
		Prompt:    "future reminder",
		RunAt:     time.Now().UTC().Add(1 * time.Hour),
		CreatedAt: time.Now().UTC(),
	}
	if err := CreateOneShotSchedule(database, futureOneShot); err != nil {
		t.Fatalf("Failed to create future one-shot schedule: %v", err)
	}

	dueOneShots, err := GetDueOneShotSchedules(database)
	if err != nil || len(dueOneShots) != 1 {
		t.Fatalf("Expected 1 due one-shot schedule, got %d (err: %v)", len(dueOneShots), err)
	}
	if dueOneShots[0].ID != "sched-1" || dueOneShots[0].Prompt != "test reminder" {
		t.Errorf("Unexpected due schedule retrieved: %+v", dueOneShots[0])
	}

	allOneShots, err := GetAllOneShotSchedules(database, "thread-1")
	if err != nil || len(allOneShots) != 2 {
		t.Fatalf("Expected 2 one-shots for thread-1, got %d (err: %v)", len(allOneShots), err)
	}

	if err := DeleteOneShotSchedule(database, "sched-1"); err != nil {
		t.Fatalf("Failed to delete schedule: %v", err)
	}
	dueAfterDelete, _ := GetDueOneShotSchedules(database)
	if len(dueAfterDelete) != 0 {
		t.Errorf("Expected 0 due one-shots after deletion, got %d", len(dueAfterDelete))
	}

	// 2. Test CronSchedule CRUD with TitlePrefix and Timezone
	now := time.Now().UTC()
	cronSched := CronSchedule{
		ID:          "cron-1",
		TargetID:    "channel-100",
		TitlePrefix: "Weekly Meal Plan",
		CronExpr:    "0 20 * * 5",
		Prompt:      "Plan meals for the week",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(-5 * time.Minute),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	// Test default timezone fallback when Timezone is empty
	cronDefaultTZ := CronSchedule{
		ID:          "cron-default-tz",
		TargetID:    "channel-100",
		TitlePrefix: "Default TZ Routine",
		CronExpr:    "0 12 * * *",
		Prompt:      "Routine with default timezone",
		Timezone:    "",
		NextRunAt:   now.Add(1 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := CreateCronSchedule(database, cronDefaultTZ); err != nil {
		t.Fatalf("Failed to create cron schedule with empty timezone: %v", err)
	}

	futureCronSched := CronSchedule{
		ID:          "cron-2",
		TargetID:    "channel-100",
		TitlePrefix: "Daily Briefing",
		CronExpr:    "0 9 * * *",
		Prompt:      "Morning news update",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(2 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := CreateCronSchedule(database, futureCronSched); err != nil {
		t.Fatalf("Failed to create future cron schedule: %v", err)
	}

	dueCrons, err := GetDueCronSchedules(database)
	if err != nil || len(dueCrons) != 1 {
		t.Fatalf("Expected 1 due cron schedule, got %d (err: %v)", len(dueCrons), err)
	}
	if dueCrons[0].ID != "cron-1" {
		t.Errorf("Expected cron-1 to be due, got %s", dueCrons[0].ID)
	}
	if dueCrons[0].TitlePrefix != "Weekly Meal Plan" {
		t.Errorf("Expected TitlePrefix 'Weekly Meal Plan', got %q", dueCrons[0].TitlePrefix)
	}
	if dueCrons[0].Timezone != "America/Los_Angeles" {
		t.Errorf("Expected Timezone 'America/Los_Angeles', got %q", dueCrons[0].Timezone)
	}

	// Verify empty timezone was defaulted to America/Los_Angeles in DB
	allCrons, err := GetAllCronSchedules(database, "channel-100")
	if err != nil || len(allCrons) != 3 {
		t.Fatalf("Expected 3 crons for channel-100, got %d (err: %v)", len(allCrons), err)
	}
	for _, c := range allCrons {
		if c.ID == "cron-default-tz" && c.Timezone != "America/Los_Angeles" {
			t.Errorf("Expected default timezone 'America/Los_Angeles' for cron-default-tz, got %q", c.Timezone)
		}
	}

	// Update next run at
	newNextRun := now.Add(7 * 24 * time.Hour)
	if err := UpdateCronNextRun(database, "cron-1", newNextRun); err != nil {
		t.Fatalf("Failed to update cron next run: %v", err)
	}
	dueAfterUpdate, _ := GetDueCronSchedules(database)
	if len(dueAfterUpdate) != 0 {
		t.Errorf("Expected 0 due cron schedules after next_run update, got %d", len(dueAfterUpdate))
	}

	if err := DeleteCronSchedule(database, "cron-1"); err != nil {
		t.Fatalf("Failed to delete cron schedule: %v", err)
	}
	remainingCrons, _ := GetAllCronSchedules(database, "channel-100")
	if len(remainingCrons) != 2 {
		t.Errorf("Expected 2 remaining crons, got %d", len(remainingCrons))
	}
}

func TestCronMigrationOnExistingDB(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Verify columns title_prefix and timezone exist and are queryable
	var id, targetID, titlePrefix, cronExpr, prompt, timezone string
	var nextRunAt, createdAt time.Time
	var enabled bool

	query := `SELECT id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at FROM cron_schedules LIMIT 0`
	err = database.QueryRow(query).Scan(&id, &targetID, &titlePrefix, &cronExpr, &prompt, &timezone, &nextRunAt, &enabled, &createdAt)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("Failed to query cron_schedules columns: %v", err)
	}
}

func TestAtomicInsertMessageAndConsumeOneShot(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// 1. Create a one-shot schedule
	schedID := "atomic-sched-1"
	oneShot := OneShotSchedule{
		ID:        schedID,
		ThreadID:  "thread-atomic-1",
		Prompt:    "Atomic reminder test",
		RunAt:     time.Now().UTC().Add(-1 * time.Minute),
		CreatedAt: time.Now().UTC(),
	}
	if err := CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("Failed to create one-shot schedule: %v", err)
	}

	// 2. Perform atomic insert and consume
	msg := Message{
		ID:         "msg-atomic-1",
		ThreadID:   "thread-atomic-1",
		GuildID:    "scheduled",
		AuthorID:   "scheduler",
		AuthorName: "Scheduler",
		Content:    "Atomic reminder test",
		Status:     StatusPending,
	}

	if err := InsertMessageAndConsumeOneShot(database, schedID, msg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}

	// 3. Verify message is inserted
	fetchedMsg, err := GetMessage(database, "msg-atomic-1")
	if err != nil || fetchedMsg == nil {
		t.Fatalf("Expected message to be in DB: %v", err)
	}
	if fetchedMsg.GuildID != "scheduled" || fetchedMsg.AuthorID != "scheduler" {
		t.Errorf("Unexpected message fields: %+v", fetchedMsg)
	}

	// 4. Verify schedule is consumed (deleted)
	dueSchedules, err := GetDueOneShotSchedules(database)
	if err != nil {
		t.Fatalf("GetDueOneShotSchedules error: %v", err)
	}
	if len(dueSchedules) != 0 {
		t.Errorf("Expected 0 due schedules after atomic consume, got %d", len(dueSchedules))
	}
}

func TestFileDBInitializationAndPragmas(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbtest-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbFile := filepath.Join(tmpDir, "test_aerial.db")
	database, err := InitDB(dbFile)
	if err != nil {
		t.Fatalf("Failed to initialize file-based database: %v", err)
	}
	defer func() { _ = database.Close() }()

	var busyTimeout int
	if err := database.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("Failed to query busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("Expected busy_timeout=5000, got %d", busyTimeout)
	}

	var journalMode string
	if err := database.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("Failed to query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("Expected journal_mode=wal, got %s", journalMode)
	}
}

func TestGetFactsPaginated(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Insert test facts
	_, err = InsertFact(database, "user_preference", "User prefers dark mode UI", 0.95, "thread-1", []float32{0.1, 0.2})
	if err != nil {
		t.Fatalf("InsertFact 1 failed: %v", err)
	}
	_, err = InsertFact(database, "system_config", "Home Assistant token is active", 0.8, "thread-1", nil)
	if err != nil {
		t.Fatalf("InsertFact 2 failed: %v", err)
	}
	_, err = InsertFact(database, "user_preference", "User drinks green tea with 50% sugar", 0.7, "thread-2", nil)
	if err != nil {
		t.Fatalf("InsertFact 3 failed: %v", err)
	}

	// 1. Test pagination without filter
	res, err := GetFactsPaginated(database, FactsFilter{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if res.Total != 3 {
		t.Errorf("Expected total 3, got %d", res.Total)
	}
	if len(res.Facts) != 2 {
		t.Errorf("Expected 2 facts, got %d", len(res.Facts))
	}

	// 2. Test category filter
	resCat, err := GetFactsPaginated(database, FactsFilter{Category: "user_preference", Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated with category failed: %v", err)
	}
	if resCat.Total != 2 {
		t.Errorf("Expected total 2 for user_preference, got %d", resCat.Total)
	}

	// 3. Test text search
	resQuery, err := GetFactsPaginated(database, FactsFilter{Query: "dark mode", Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated with query failed: %v", err)
	}
	if resQuery.Total != 1 {
		t.Errorf("Expected total 1 for 'dark mode', got %d", resQuery.Total)
	}
	if len(resQuery.Facts) != 1 || resQuery.Facts[0].Category != "user_preference" {
		t.Errorf("Unexpected fact retrieved: %+v", resQuery.Facts)
	}

	// 4. Test wildcard search safety (50% should not match 500)
	resWildcard, err := GetFactsPaginated(database, FactsFilter{Query: "50%", Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated with wildcard failed: %v", err)
	}
	if resWildcard.Total != 1 {
		t.Errorf("Expected total 1 for literal '50%%', got %d", resWildcard.Total)
	}
}

func TestFactsMigrationOnExistingDB(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Verify columns id, category, fact_text, importance, thread_id, embedding, created_at exist
	var id int64
	var category, factText, threadID string
	var importance float64
	var embedding []byte
	var createdAt time.Time

	query := `SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts LIMIT 0`
	err = database.QueryRow(query).Scan(&id, &category, &factText, &importance, &threadID, &embedding, &createdAt)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("Failed to query facts table columns: %v", err)
	}
}

func TestMessageExistsAndClaimPending(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	exists, err := MessageExists(database, "msg-test-1")
	if err != nil || exists {
		t.Fatalf("Expected msg-test-1 not to exist, got exists=%t, err=%v", exists, err)
	}

	msg := Message{
		ID:         "msg-test-1",
		ThreadID:   "th-1",
		GuildID:    "g-1",
		AuthorID:   "u-1",
		AuthorName: "Alice",
		Content:    "Hello",
		Status:     StatusPending,
	}
	if err := InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	exists, err = MessageExists(database, "msg-test-1")
	if err != nil || !exists {
		t.Fatalf("Expected msg-test-1 to exist, got exists=%t, err=%v", exists, err)
	}

	// First claim: should succeed (PENDING -> PROCESSING)
	claimed, err := ClaimPendingMessage(database, "msg-test-1")
	if err != nil || !claimed {
		t.Fatalf("Expected ClaimPendingMessage to succeed, got claimed=%t, err=%v", claimed, err)
	}

	// Second claim (e.g. concurrent race): should return false because it's already in PROCESSING
	claimed, err = ClaimPendingMessage(database, "msg-test-1")
	if err != nil || claimed {
		t.Fatalf("Expected subsequent concurrent claim on PROCESSING message to return false, got %t, err=%v", claimed, err)
	}

	// Complete message
	_ = UpdateMessageCompleted(database, "msg-test-1", "Done")

	// Third claim on COMPLETED message: should return false
	claimed, err = ClaimPendingMessage(database, "msg-test-1")
	if err != nil || claimed {
		t.Fatalf("Expected claim on COMPLETED message to fail, got %t, err=%v", claimed, err)
	}
}

func TestGetActiveRecentThreadIDs(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Insert messages in different threads
	now := time.Now().UTC()
	_ = InsertMessage(database, Message{
		ID: "m1", ThreadID: "th-recent-1", Status: StatusCompleted, UpdatedAt: now,
	})
	_ = InsertMessage(database, Message{
		ID: "m2", ThreadID: "th-recent-2", Status: StatusCompleted, UpdatedAt: now.Add(-10 * time.Minute),
	})
	_ = InsertMessage(database, Message{
		ID: "m3", ThreadID: "th-old-3", Status: StatusCompleted, UpdatedAt: now.Add(-5 * time.Hour),
	})

	threadIDs, err := GetActiveRecentThreadIDs(database, 2*time.Hour)
	if err != nil {
		t.Fatalf("GetActiveRecentThreadIDs failed: %v", err)
	}

	if len(threadIDs) != 2 {
		t.Fatalf("Expected 2 recent thread IDs, got %d: %v", len(threadIDs), threadIDs)
	}
	if threadIDs[0] != "th-recent-1" || threadIDs[1] != "th-recent-2" {
		t.Errorf("Unexpected thread IDs order: %v", threadIDs)
	}
}

func TestFactExtractionWatermarkAndFiltering(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	// Insert completed message in thread-1
	_ = InsertMessage(database, Message{
		ID: "m1", ThreadID: "thread-1", Status: StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})

	// 1. Thread-1 should be eligible for extraction
	tids, err := GetActiveConversationsForExtraction(database, 12)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(tids) != 1 || tids[0] != "thread-1" {
		t.Fatalf("Expected thread-1 to be eligible, got %v", tids)
	}

	maxRowID, err := GetMaxMessageRowID(database, "thread-1")
	if err != nil || maxRowID == 0 {
		t.Fatalf("Expected valid maxRowID > 0, got %d, err=%v", maxRowID, err)
	}

	// 2. Advance watermark for thread-1
	err = UpdateConversationFactWatermark(database, "thread-1", maxRowID)
	if err != nil {
		t.Fatalf("UpdateConversationFactWatermark failed: %v", err)
	}

	// 3. Thread-1 should now be skipped (watermarked)
	tids, err = GetActiveConversationsForExtraction(database, 12)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(tids) != 0 {
		t.Fatalf("Expected 0 eligible threads after watermark advance, got %v", tids)
	}

	// 4. Insert new completed message in thread-1
	_ = InsertMessage(database, Message{
		ID: "m2", ThreadID: "thread-1", Status: StatusCompleted, CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(time.Minute),
	})

	// 5. Thread-1 should become eligible again
	tids, err = GetActiveConversationsForExtraction(database, 12)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(tids) != 1 || tids[0] != "thread-1" {
		t.Fatalf("Expected thread-1 to be eligible after new message, got %v", tids)
	}
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize test database: %v", err)
	}
	return database
}

func TestScheduleRunsCRUD(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	now := time.Now().UTC()
	run := ScheduleRun{
		ID:           "run-test-1",
		ScheduleID:   "cron-123",
		ScheduleType: "cron",
		MessageID:    "msg-123",
		TargetID:     "chan-1",
		ThreadID:     "thread-1",
		Title:        "Morning Brief",
		Prompt:       "Check systems",
		Status:       "enqueued",
		StartedAt:    now,
	}

	if err := CreateScheduleRun(db, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	// Update status to running then completed
	updateParams := UpdateRunParams{
		RunID:       "run-test-1",
		Status:      "completed",
		CompletedAt: now.Add(5 * time.Second),
		DurationMs:  5000,
		Error:       "",
	}
	if err := UpdateScheduleRunStatus(db, updateParams); err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}

	runs, total, err := GetScheduleRunsPaginated(db, 10, 0, "", "")
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("GetScheduleRunsPaginated failed: %v (total=%d, len=%d)", err, total, len(runs))
	}
	if runs[0].Status != "completed" || runs[0].DurationMs != 5000 {
		t.Errorf("Unexpected run fields: %+v", runs[0])
	}
	if runs[0].CompletedAt == nil || runs[0].CompletedAt.IsZero() {
		t.Errorf("Expected CompletedAt to be set, got nil or zero")
	}

	// Filter by schedule_id
	runsSched, totalSched, err := GetScheduleRunsPaginated(db, 10, 0, "cron-123", "")
	if err != nil || totalSched != 1 || len(runsSched) != 1 {
		t.Errorf("Expected 1 run for cron-123, got %d (err: %v)", totalSched, err)
	}

	// Filter by non-matching status
	runsFailed, totalFailed, err := GetScheduleRunsPaginated(db, 10, 0, "", "failed")
	if err != nil || totalFailed != 0 || len(runsFailed) != 0 {
		t.Errorf("Expected 0 runs for failed status, got %d", totalFailed)
	}
}

func TestReconcileOrphanedScheduleRuns(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	now := time.Now().UTC()
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-stuck-1", ScheduleID: "cron-1", ScheduleType: "cron", TargetID: "c1", ThreadID: "t1", Prompt: "p1", Status: "running", StartedAt: now})
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-stuck-2", ScheduleID: "cron-2", ScheduleType: "cron", TargetID: "c2", ThreadID: "t2", Prompt: "p2", Status: "enqueued", StartedAt: now})
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-ok-3", ScheduleID: "cron-3", ScheduleType: "cron", TargetID: "c3", ThreadID: "t3", Prompt: "p3", Status: "completed", StartedAt: now})

	reconciled, err := ReconcileOrphanedScheduleRuns(db)
	if err != nil {
		t.Fatalf("ReconcileOrphanedScheduleRuns failed: %v", err)
	}
	if reconciled != 2 {
		t.Errorf("Expected 2 reconciled runs, got %d", reconciled)
	}

	runs, _, _ := GetScheduleRunsPaginated(db, 10, 0, "", "")
	for _, r := range runs {
		if (r.ID == "run-stuck-1" || r.ID == "run-stuck-2") && r.Status != "failed" {
			t.Errorf("Run %s expected status failed, got %s", r.ID, r.Status)
		}
		if (r.ID == "run-stuck-1" || r.ID == "run-stuck-2") && r.Error != "Interrupted by server restart" {
			t.Errorf("Run %s expected crash recovery error message, got %q", r.ID, r.Error)
		}
		if r.ID == "run-ok-3" && r.Status != "completed" {
			t.Errorf("Run run-ok-3 status should remain completed, got %s", r.Status)
		}
	}
}

func TestScheduleSummaryMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	now := time.Now().UTC()

	// Initial metrics with empty DB
	metrics, err := GetScheduleSummaryMetrics(db)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics on empty DB failed: %v", err)
	}
	if metrics.TotalActive != 0 || metrics.CronCount != 0 || metrics.OneShotCount != 0 || metrics.TotalRuns24h != 0 || metrics.SuccessRate24h != 100.0 || metrics.NextRunAt != nil {
		t.Errorf("Unexpected empty DB metrics: %+v", metrics)
	}

	// Insert enabled cron schedule
	cron1Next := now.Add(2 * time.Hour)
	_ = CreateCronSchedule(db, CronSchedule{
		ID:          "cron-m1",
		TargetID:    "chan-1",
		TitlePrefix: "Cron 1",
		CronExpr:    "0 * * * *",
		Prompt:      "hourly prompt",
		NextRunAt:   cron1Next,
		Enabled:     true,
	})

	// Insert disabled cron schedule (should not count towards active or next run)
	_ = CreateCronSchedule(db, CronSchedule{
		ID:          "cron-disabled",
		TargetID:    "chan-1",
		TitlePrefix: "Disabled",
		CronExpr:    "0 * * * *",
		Prompt:      "disabled prompt",
		NextRunAt:   now.Add(10 * time.Minute),
		Enabled:     false,
	})

	// Insert one-shot schedule earlier than cron1
	oneShotNext := now.Add(30 * time.Minute)
	_ = CreateOneShotSchedule(db, OneShotSchedule{
		ID:       "once-m1",
		ThreadID: "th-1",
		Prompt:   "one-shot prompt",
		RunAt:    oneShotNext,
	})

	// Insert runs in the last 24h
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-m1", ScheduleID: "cron-m1", ScheduleType: "cron", TargetID: "c1", ThreadID: "t1", Prompt: "p1", Status: "completed", StartedAt: now.Add(-2 * time.Hour)})
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-m2", ScheduleID: "cron-m1", ScheduleType: "cron", TargetID: "c1", ThreadID: "t1", Prompt: "p1", Status: "completed", StartedAt: now.Add(-1 * time.Hour)})
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-m3", ScheduleID: "cron-m1", ScheduleType: "cron", TargetID: "c1", ThreadID: "t1", Prompt: "p1", Status: "failed", StartedAt: now.Add(-30 * time.Minute)})

	// Insert run older than 24h (should not count towards 24h stats)
	_ = CreateScheduleRun(db, ScheduleRun{ID: "run-old", ScheduleID: "cron-m1", ScheduleType: "cron", TargetID: "c1", ThreadID: "t1", Prompt: "p1", Status: "failed", StartedAt: now.Add(-25 * time.Hour)})

	metrics, err = GetScheduleSummaryMetrics(db)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed: %v", err)
	}

	if metrics.CronCount != 1 {
		t.Errorf("Expected CronCount=1, got %d", metrics.CronCount)
	}
	if metrics.OneShotCount != 1 {
		t.Errorf("Expected OneShotCount=1, got %d", metrics.OneShotCount)
	}
	if metrics.TotalActive != 2 {
		t.Errorf("Expected TotalActive=2, got %d", metrics.TotalActive)
	}
	if metrics.TotalRuns24h != 3 {
		t.Errorf("Expected TotalRuns24h=3, got %d", metrics.TotalRuns24h)
	}
	// 2 completed out of 3 runs = 66.666...% -> rounded to ~66.7%
	if metrics.SuccessRate24h < 66.0 || metrics.SuccessRate24h > 67.0 {
		t.Errorf("Expected SuccessRate24h ~66.7, got %f", metrics.SuccessRate24h)
	}
	if metrics.NextRunAt == nil {
		t.Fatal("Expected NextRunAt to not be nil")
	}
	if metrics.NextRunAt.Sub(oneShotNext).Abs() > time.Second {
		t.Errorf("Expected NextRunAt to match earliest oneShotNext (%v), got %v", oneShotNext, *metrics.NextRunAt)
	}
}

func TestPruneScheduleRuns(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	now := time.Now().UTC()

	// Insert 10 runs
	for i := 1; i <= 10; i++ {
		started := now.Add(time.Duration(-i) * time.Hour)
		_ = CreateScheduleRun(db, ScheduleRun{
			ID:           "run-prune-" + string(rune('a'+i-1)),
			ScheduleID:   "cron-1",
			ScheduleType: "cron",
			TargetID:     "c1",
			ThreadID:     "t1",
			Prompt:       "prompt",
			Status:       "completed",
			StartedAt:    started,
		})
	}

	// Prune to keep only 5 newest runs
	pruned, err := PruneScheduleRuns(db, 5, 30*24*time.Hour)
	if err != nil {
		t.Fatalf("PruneScheduleRuns failed: %v", err)
	}
	if pruned != 5 {
		t.Errorf("Expected 5 runs pruned, got %d", pruned)
	}

	runs, total, err := GetScheduleRunsPaginated(db, 20, 0, "", "")
	if err != nil || total != 5 || len(runs) != 5 {
		t.Fatalf("Expected 5 remaining runs, got total=%d, len=%d (err: %v)", total, len(runs), err)
	}

	// Insert a very old run (>30 days)
	_ = CreateScheduleRun(db, ScheduleRun{
		ID:           "run-very-old",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "c1",
		ThreadID:     "t1",
		Prompt:       "old prompt",
		Status:       "completed",
		StartedAt:    now.Add(-40 * 24 * time.Hour),
	})

	// Prune by age (maxAge = 30 days, maxCount = 100)
	prunedOld, err := PruneScheduleRuns(db, 100, 30*24*time.Hour)
	if err != nil {
		t.Fatalf("PruneScheduleRuns by age failed: %v", err)
	}
	if prunedOld != 1 {
		t.Errorf("Expected 1 old run pruned, got %d", prunedOld)
	}
}

func TestScheduleRunsNilDB(t *testing.T) {
	if err := CreateScheduleRun(nil, ScheduleRun{ID: "r1"}); err == nil {
		t.Error("Expected error for CreateScheduleRun with nil DB")
	}
	if err := UpdateScheduleRunStatus(nil, UpdateRunParams{RunID: "r1"}); err == nil {
		t.Error("Expected error for UpdateScheduleRunStatus with nil DB")
	}
	if _, _, err := GetScheduleRunsPaginated(nil, 10, 0, "", ""); err == nil {
		t.Error("Expected error for GetScheduleRunsPaginated with nil DB")
	}
	if _, err := GetScheduleSummaryMetrics(nil); err == nil {
		t.Error("Expected error for GetScheduleSummaryMetrics with nil DB")
	}
	if _, err := ReconcileOrphanedScheduleRuns(nil); err == nil {
		t.Error("Expected error for ReconcileOrphanedScheduleRuns with nil DB")
	}
	if _, err := PruneScheduleRuns(nil, 10, time.Hour); err == nil {
		t.Error("Expected error for PruneScheduleRuns with nil DB")
	}
}

func TestInferTriggerType(t *testing.T) {
	tests := []struct {
		authorID      string
		scheduleRunID string
		expected      string
	}{
		{"http-client", "", "http"},
		{"http-client", "cron-123", "http"},
		{"user-123", "cron-run-456", "cron"},
		{"scheduler", "cron-999", "cron"},
		{"scheduler", "rem-123", "reminder"},
		{"scheduler", "once-456", "reminder"},
		{"user-123", "", "discord"},
		{"", "", "discord"},
	}

	for _, tc := range tests {
		got := InferTriggerType(tc.authorID, tc.scheduleRunID)
		if got != tc.expected {
			t.Errorf("InferTriggerType(%q, %q) = %q; want %q", tc.authorID, tc.scheduleRunID, got, tc.expected)
		}
	}
}

func TestGetActiveTasks(t *testing.T) {
	// Nil DB check
	if _, err := GetActiveTasks(nil); err == nil {
		t.Error("expected error for GetActiveTasks with nil DB, got nil")
	}

	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Insert test messages with various statuses
	now := time.Now().UTC()
	msgs := []Message{
		{
			ID:          "msg-1",
			ThreadID:    "thread-1",
			AuthorID:    "user-123",
			AuthorName:  "UserA",
			Content:     "Hello agent",
			Status:      StatusPending,
			CreatedAt:   now.Add(-2 * time.Minute),
			UpdatedAt:   now.Add(-2 * time.Minute),
		},
		{
			ID:            "msg-2",
			ThreadID:      "thread-2",
			AuthorID:      "scheduler",
			AuthorName:    "Scheduler",
			Content:       "Daily backup check",
			Status:        StatusProcessing,
			ScheduleRunID: "cron-abc-123",
			CreatedAt:     now.Add(-1 * time.Minute),
			UpdatedAt:     now.Add(-30 * time.Second),
		},
		{
			ID:          "msg-3",
			ThreadID:    "thread-3",
			AuthorID:    "http-client",
			AuthorName:  "HTTP Client",
			Content:     "API prompt",
			Status:      StatusCompleted,
			CreatedAt:   now.Add(-5 * time.Minute),
			UpdatedAt:   now.Add(-4 * time.Minute),
		},
		{
			ID:          "msg-4",
			ThreadID:    "thread-4",
			AuthorID:    "user-456",
			AuthorName:  "User456",
			Content:     "Failed task",
			Status:      StatusFailed,
			CreatedAt:   now.Add(-10 * time.Minute),
			UpdatedAt:   now.Add(-9 * time.Minute),
		},
	}

	for _, m := range msgs {
		if err := InsertMessage(database, m); err != nil {
			t.Fatalf("InsertMessage failed: %v", err)
		}
	}

	// Save session for thread-2
	if err := SaveSessionID(database, "thread-2", "sess-uuid-456"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}

	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected 2 active tasks, got %d", len(tasks))
	}

	// Verify FIFO ordering by created_at ASC
	if tasks[0].ID != "msg-1" || tasks[0].Status != StatusPending || tasks[0].TriggerType != "discord" || tasks[0].SessionID != "" || tasks[0].Summary != "Hello agent" {
		t.Errorf("unexpected task[0]: %+v", tasks[0])
	}

	if tasks[1].ID != "msg-2" || tasks[1].Status != StatusProcessing || tasks[1].TriggerType != "cron" || tasks[1].SessionID != "sess-uuid-456" || tasks[1].Summary != "Daily backup check" {
		t.Errorf("unexpected task[1]: %+v", tasks[1])
	}
}

func TestCleanTaskSummary(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Empty input",
			input:    "",
			expected: "Agent Task",
		},
		{
			name:     "Simple text",
			input:    "Deploy latest update",
			expected: "Deploy latest update",
		},
		{
			name: "Discord XML wrapper",
			input: `<USER_REQUEST>
Here's a message someone sent you from Discord:
- id: 123456
- author_username: tester
- content: What is the current system status?
- timestamp: 2026-08-30T12:00:00Z
</USER_REQUEST>`,
			expected: "What is the current system status?",
		},
		{
			name: "Cron prompt wrapper",
			input: `<USER_REQUEST>
Scheduled trigger: Daily News Summary
Prompt:
Fetch and summarize today's political, tech, and AI news.
</USER_REQUEST>`,
			expected: "Fetch and summarize today's political, tech, and AI news.",
		},
		{
			name:     "Markdown and mentions",
			input:    "<@!123456789> ### Check *server* `status` > now!",
			expected: "Check server status now!",
		},
		{
			name:     "Long text truncation",
			input:    "This is an extremely long prompt designed to test whether the CleanTaskSummary function correctly caps the rune length to at most one hundred and forty runes without breaking or panicking or truncating multibyte characters inappropriately in Go.",
			expected: "This is an extremely long prompt designed to test whether the CleanTaskSummary function correctly caps the rune length to at most one hun...",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CleanTaskSummary(tc.input)
			if got != tc.expected {
				t.Errorf("CleanTaskSummary(%q) = %q; want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestGetRecentThreadMessages(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	// 1. Edge case: nil database
	if _, err := GetRecentThreadMessages(nil, "thread-lounge", 10); err == nil {
		t.Error("expected error when querying with nil database, got nil")
	}

	// 2. Edge case: non-existent thread returns empty slice without error
	nonExistent, err := GetRecentThreadMessages(database, "thread-nonexistent", 10)
	if err != nil {
		t.Fatalf("unexpected error for non-existent thread: %v", err)
	}
	if len(nonExistent) != 0 {
		t.Errorf("expected 0 messages for non-existent thread, got %d", len(nonExistent))
	}

	// Seed 15 messages for thread-lounge
	now := time.Now().UTC()
	for i := 1; i <= 15; i++ {
		msg := Message{
			ID:         fmt.Sprintf("msg-%d", i),
			ThreadID:   "thread-lounge",
			GuildID:    "guild-1",
			AuthorID:   "user-1",
			AuthorName: "Alice",
			Content:    fmt.Sprintf("Message number %d", i),
			Status:     StatusCompleted,
			CreatedAt:  now.Add(time.Duration(i) * time.Minute),
			UpdatedAt:  now.Add(time.Duration(i) * time.Minute),
		}
		if i%3 == 0 {
			msg.ErrorMessage = fmt.Sprintf("error %d", i)
			msg.ResponseText = fmt.Sprintf("response %d", i)
			msg.ScheduleRunID = fmt.Sprintf("sched-%d", i)
		}
		if err := InsertMessage(database, msg); err != nil {
			t.Fatalf("InsertMessage failed: %v", err)
		}
	}

	// Seed 3 messages for another thread to test isolation and fewer messages than limit
	for i := 1; i <= 3; i++ {
		msg := Message{
			ID:         fmt.Sprintf("other-msg-%d", i),
			ThreadID:   "thread-other",
			GuildID:    "guild-1",
			AuthorID:   "user-2",
			AuthorName: "Bob",
			Content:    fmt.Sprintf("Other message %d", i),
			Status:     StatusCompleted,
			CreatedAt:  now.Add(time.Duration(i) * time.Minute),
			UpdatedAt:  now.Add(time.Duration(i) * time.Minute),
		}
		if err := InsertMessage(database, msg); err != nil {
			t.Fatalf("InsertMessage failed for other thread: %v", err)
		}
	}

	// 3. Normal fetch: fetch recent 10 messages from thread-lounge
	recent, err := GetRecentThreadMessages(database, "thread-lounge", 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages failed: %v", err)
	}
	if len(recent) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(recent))
	}
	// Verify ascending chronological order: msg-6 to msg-15
	for idx, expectedNum := range []int{6, 7, 8, 9, 10, 11, 12, 13, 14, 15} {
		expectedID := fmt.Sprintf("msg-%d", expectedNum)
		if recent[idx].ID != expectedID {
			t.Errorf("at index %d: expected ID %s, got %s", idx, expectedID, recent[idx].ID)
		}
		// Verify null handling / set values
		if expectedNum%3 == 0 {
			if recent[idx].ErrorMessage != fmt.Sprintf("error %d", expectedNum) {
				t.Errorf("expected ErrorMessage 'error %d', got %q", expectedNum, recent[idx].ErrorMessage)
			}
			if recent[idx].ResponseText != fmt.Sprintf("response %d", expectedNum) {
				t.Errorf("expected ResponseText 'response %d', got %q", expectedNum, recent[idx].ResponseText)
			}
			if recent[idx].ScheduleRunID != fmt.Sprintf("sched-%d", expectedNum) {
				t.Errorf("expected ScheduleRunID 'sched-%d', got %q", expectedNum, recent[idx].ScheduleRunID)
			}
		} else {
			if recent[idx].ErrorMessage != "" {
				t.Errorf("expected empty ErrorMessage, got %q", recent[idx].ErrorMessage)
			}
			if recent[idx].ResponseText != "" {
				t.Errorf("expected empty ResponseText, got %q", recent[idx].ResponseText)
			}
			if recent[idx].ScheduleRunID != "" {
				t.Errorf("expected empty ScheduleRunID, got %q", recent[idx].ScheduleRunID)
			}
		}
	}

	// 4. Edge case: thread with fewer messages than limit returns all messages in ascending order
	otherMsgs, err := GetRecentThreadMessages(database, "thread-other", 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages for fewer messages failed: %v", err)
	}
	if len(otherMsgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(otherMsgs))
	}
	if otherMsgs[0].ID != "other-msg-1" || otherMsgs[1].ID != "other-msg-2" || otherMsgs[2].ID != "other-msg-3" {
		t.Errorf("expected other-msg-1..3 in order, got %s, %s, %s", otherMsgs[0].ID, otherMsgs[1].ID, otherMsgs[2].ID)
	}

	// 5. Edge case: limit <= 0 defaults to 10
	limitZero, err := GetRecentThreadMessages(database, "thread-lounge", 0)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages with limit 0 failed: %v", err)
	}
	if len(limitZero) != 10 {
		t.Fatalf("expected 10 messages for limit 0, got %d", len(limitZero))
	}
	if limitZero[0].ID != "msg-6" || limitZero[9].ID != "msg-15" {
		t.Errorf("expected msg-6 to msg-15 for limit 0, got %s to %s", limitZero[0].ID, limitZero[9].ID)
	}

	limitNegative, err := GetRecentThreadMessages(database, "thread-lounge", -5)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages with negative limit failed: %v", err)
	}
	if len(limitNegative) != 10 {
		t.Fatalf("expected 10 messages for limit -5, got %d", len(limitNegative))
	}
	if limitNegative[0].ID != "msg-6" || limitNegative[9].ID != "msg-15" {
		t.Errorf("expected msg-6 to msg-15 for limit -5, got %s to %s", limitNegative[0].ID, limitNegative[9].ID)
	}
}
