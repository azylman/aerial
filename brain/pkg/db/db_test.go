package db

import (
	"database/sql"
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

	// Closed DB handling
	database, _ := InitDB(":memory:")
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
		Timezone:    "America/New_York",
		NextRunAt:   now.Add(-5 * time.Minute),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	futureCronSched := CronSchedule{
		ID:          "cron-2",
		TargetID:    "channel-100",
		TitlePrefix: "Daily Briefing",
		CronExpr:    "0 9 * * *",
		Prompt:      "Morning news update",
		Timezone:    "America/New_York",
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
	if dueCrons[0].Timezone != "America/New_York" {
		t.Errorf("Expected Timezone 'America/New_York', got %q", dueCrons[0].Timezone)
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

	allCrons, err := GetAllCronSchedules(database, "channel-100")
	if err != nil || len(allCrons) != 2 {
		t.Fatalf("Expected 2 crons for channel-100, got %d (err: %v)", len(allCrons), err)
	}

	if err := DeleteCronSchedule(database, "cron-1"); err != nil {
		t.Fatalf("Failed to delete cron schedule: %v", err)
	}
	remainingCrons, _ := GetAllCronSchedules(database, "channel-100")
	if len(remainingCrons) != 1 || remainingCrons[0].ID != "cron-2" {
		t.Errorf("Expected 1 remaining cron (cron-2), got %d", len(remainingCrons))
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
