package db

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		dsn = "postgres://postgres:aerial_test@127.0.0.1:54329/aerial_test?sslmode=disable"
	}
	database, err := InitDB(dsn)
	if err != nil {
		t.Skipf("Skipping integration test: PostgreSQL not reachable at %s: %v", dsn, err)
		return nil
	}

	_, err = database.Exec("TRUNCATE TABLE messages, sessions, one_shot_schedules, cron_schedules, schedule_runs, facts RESTART IDENTITY CASCADE;")
	if err != nil {
		t.Fatalf("Failed to truncate test tables: %v", err)
	}
	return database
}

func TestDBInitializationAndMigrations(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	// Verify messages table exists with response_text
	_, err := database.Exec("SELECT id, row_id, thread_id, guild_id, author_id, author_name, content, status, retry_count, error_message, response_text, created_at, updated_at FROM messages LIMIT 0")
	if err != nil {
		t.Fatalf("Messages table schema error: %v", err)
	}

	// Verify sessions table exists
	_, err = database.Exec("SELECT thread_id, internal_session_id, turn_count, last_extracted_rowid, fact_extracted_at, created_at, updated_at FROM sessions LIMIT 0")
	if err != nil {
		t.Fatalf("Sessions table schema error: %v", err)
	}

	// Verify facts table with pgvector column
	_, err = database.Exec("SELECT id, category, fact_text, importance, thread_id, embedding, created_at FROM facts LIMIT 0")
	if err != nil {
		t.Fatalf("Facts table schema error: %v", err)
	}
}

func TestDuplicateMessageInsertDoesNotResetStatus(t *testing.T) {
	database := setupTestDB(t)
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
	database := setupTestDB(t)
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
	database := setupTestDB(t)
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

	if err := InsertMessage(database, msg1); err != nil {
		t.Fatalf("Failed to insert msg1: %v", err)
	}
	if err := InsertMessage(database, msg2); err != nil {
		t.Fatalf("Failed to insert msg2: %v", err)
	}

	fetched, err := GetMessage(database, "msg-1")
	if err != nil {
		t.Fatalf("Failed to get msg-1: %v", err)
	}
	if fetched == nil || fetched.ID != "msg-1" || fetched.AuthorName != "Alice" || fetched.Status != StatusPending {
		t.Fatalf("Unexpected message retrieved: %+v", fetched)
	}

	pending, err := GetPendingOrProcessingMessages(database)
	if err != nil {
		t.Fatalf("Failed to get pending messages: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("Expected 2 pending messages, got %d", len(pending))
	}

	if err := IncrementMessageRetry(database, "msg-1", "503 Unavailable"); err != nil {
		t.Fatalf("Failed to increment retry: %v", err)
	}
	fetched, _ = GetMessage(database, "msg-1")
	if fetched.RetryCount != 1 || fetched.ErrorMessage != "503 Unavailable" {
		t.Errorf("Expected retry_count=1 and error message set, got count=%d, err=%s", fetched.RetryCount, fetched.ErrorMessage)
	}

	if err := UpdateMessageStatus(database, "msg-1", StatusCompleted, ""); err != nil {
		t.Fatalf("Failed to update status to COMPLETED: %v", err)
	}
	if err := UpdateMessageStatus(database, "msg-2", StatusFailed, "fatal error"); err != nil {
		t.Fatalf("Failed to update status to FAILED: %v", err)
	}
}

func TestSessionCRUD(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	threadID := "thread-123"
	sessID := "sess-abc"

	if err := SaveSessionID(database, threadID, sessID); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}

	got, err := GetSessionID(database, threadID)
	if err != nil {
		t.Fatalf("GetSessionID failed: %v", err)
	}
	if got != sessID {
		t.Errorf("Expected %s, got %s", sessID, got)
	}

	if err := DeleteSessionID(database, threadID); err != nil {
		t.Fatalf("DeleteSessionID failed: %v", err)
	}
	got, _ = GetSessionID(database, threadID)
	if got != "" {
		t.Errorf("Expected empty sessionID after deletion, got %s", got)
	}
}

func TestIncrementSessionTurnCountAndRotation(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	key := "chan-test-1"

	c1, err := IncrementSessionTurnCount(database, key)
	if err != nil {
		t.Fatalf("IncrementSessionTurnCount 1 failed: %v", err)
	}
	if c1 != 1 {
		t.Errorf("Expected turn_count=1, got %d", c1)
	}

	c2, err := IncrementSessionTurnCount(database, key)
	if err != nil {
		t.Fatalf("IncrementSessionTurnCount 2 failed: %v", err)
	}
	if c2 != 2 {
		t.Errorf("Expected turn_count=2, got %d", c2)
	}

	// Rotate session ID
	if err := RotateSessionID(database, key, "new-session-uuid-123"); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}

	cRotated, _ := GetSessionTurnCount(database, key)
	if cRotated != 0 {
		t.Errorf("Expected turn_count=0 after rotation, got %d", cRotated)
	}

	sessID, _ := GetSessionID(database, key)
	if sessID != "new-session-uuid-123" {
		t.Errorf("Expected new-session-uuid-123, got %s", sessID)
	}
}

func TestSchedulesCompatibility(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// 1. One-Shot Schedules
	oneShot := OneShotSchedule{
		ID:        "one-shot-1",
		ThreadID:  "thread-sched-1",
		Prompt:    "Remind me about meeting",
		RunAt:     now.Add(-1 * time.Minute),
		CreatedAt: now,
	}
	if err := CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	dueOneShots, err := GetDueOneShotSchedules(database)
	if err != nil {
		t.Fatalf("GetDueOneShotSchedules failed: %v", err)
	}
	if len(dueOneShots) != 1 || dueOneShots[0].ID != "one-shot-1" {
		t.Fatalf("Expected 1 due one-shot, got %+v", dueOneShots)
	}

	// 2. Cron Schedules
	cron := CronSchedule{
		ID:          "cron-1",
		TargetID:    "chan-general",
		TitlePrefix: "Weather",
		CronExpr:    "0 8 * * *",
		Prompt:      "Daily forecast",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(-5 * time.Minute),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := CreateCronSchedule(database, cron); err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}

	dueCrons, err := GetDueCronSchedules(database)
	if err != nil {
		t.Fatalf("GetDueCronSchedules failed: %v", err)
	}
	if len(dueCrons) != 1 || dueCrons[0].ID != "cron-1" {
		t.Fatalf("Expected 1 due cron, got %+v", dueCrons)
	}
}

func TestAtomicInsertMessageAndConsumeOneShot(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	oneShot := OneShotSchedule{
		ID:        "oneshot-consume-1",
		ThreadID:  "thread-1",
		Prompt:    "Alert",
		RunAt:     now,
		CreatedAt: now,
	}
	if err := CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	msg := Message{
		ID:       "msg-consumed-1",
		ThreadID: "thread-1",
		Content:  "Alert",
		Status:   StatusPending,
	}

	if err := InsertMessageAndConsumeOneShot(database, "oneshot-consume-1", msg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}

	// Ensure one-shot was deleted
	due, _ := GetDueOneShotSchedules(database)
	if len(due) != 0 {
		t.Errorf("Expected 0 due one-shots after consume, got %d", len(due))
	}

	// Ensure message exists
	m, _ := GetMessage(database, "msg-consumed-1")
	if m == nil || m.Status != StatusPending {
		t.Errorf("Expected message to exist with status PENDING, got %+v", m)
	}
}

func TestGetFactsPaginatedCaseInsensitive(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	emb := make([]float32, 384)
	emb[0] = 0.5

	_, err := InsertFact(database, "user_preference", "Alex loves Matcha and Dark Roast Coffee", 1.0, "thread-1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	// Case-insensitive search using lowercase "matcha" and uppercase "ALEX"
	res1, err := GetFactsPaginated(database, FactsFilter{Query: "matcha"})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if res1.Total != 1 {
		t.Errorf("Expected 1 matching fact for lowercase 'matcha', got %d (check ILIKE)", res1.Total)
	}

	res2, err := GetFactsPaginated(database, FactsFilter{Query: "ALEX"})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if res2.Total != 1 {
		t.Errorf("Expected 1 matching fact for uppercase 'ALEX', got %d (check ILIKE)", res2.Total)
	}
}

func TestMessageExistsAndClaimPending(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	msg := Message{
		ID:       "msg-cas-1",
		ThreadID: "thread-1",
		Content:  "CAS Test",
		Status:   StatusPending,
	}
	if err := InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	exists, err := MessageExists(database, "msg-cas-1")
	if err != nil || !exists {
		t.Errorf("Expected exists=true, got %v, err=%v", exists, err)
	}

	// First claim from PENDING should succeed
	claimed1, err := ClaimPendingMessage(database, "msg-cas-1")
	if err != nil || !claimed1 {
		t.Errorf("Expected claim 1 to succeed, got %v, err=%v", claimed1, err)
	}

	// Second claim should fail (already PROCESSING)
	claimed2, err := ClaimPendingMessage(database, "msg-cas-1")
	if err != nil || claimed2 {
		t.Errorf("Expected claim 2 to fail (already claimed), got %v, err=%v", claimed2, err)
	}
}

func TestFactExtractionWatermarkAndFiltering(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	msg1 := Message{
		ID:        "msg-wm-1",
		ThreadID:  "thread-watermark",
		Content:   "First turn",
		Status:    StatusCompleted,
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
	}
	if err := InsertMessage(database, msg1); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	maxRow1, err := GetMaxMessageRowID(database, "thread-watermark")
	if err != nil {
		t.Fatalf("GetMaxMessageRowID failed: %v", err)
	}
	if maxRow1 <= 0 {
		t.Fatalf("Expected positive row_id sequence, got %d", maxRow1)
	}

	// Active conversations for extraction
	convs, err := GetActiveConversationsForExtraction(database, 24)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(convs) != 1 || convs[0] != "thread-watermark" {
		t.Fatalf("Expected thread-watermark in active extractions, got %+v", convs)
	}

	// Advance watermark
	if err := UpdateConversationFactWatermark(database, "thread-watermark", maxRow1); err != nil {
		t.Fatalf("UpdateConversationFactWatermark failed: %v", err)
	}

	// Re-query: should now be 0 since watermark matches latest message
	convsAfter, err := GetActiveConversationsForExtraction(database, 24)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction after watermark failed: %v", err)
	}
	if len(convsAfter) != 0 {
		t.Errorf("Expected 0 active conversations after watermark catch-up, got %d", len(convsAfter))
	}
}

func TestScheduleRunsCRUD(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	run := ScheduleRun{
		ID:           "run-test-1",
		ScheduleID:   "cron-weather",
		ScheduleType: "cron",
		TargetID:     "chan-weather",
		ThreadID:     "thread-weather-1",
		Title:        "Morning Forecast",
		Prompt:       "Forecast prompt",
		Status:       "enqueued",
		StartedAt:    now,
	}

	if err := CreateScheduleRun(database, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	// Update to completed
	compTime := now.Add(15 * time.Second)
	updateParams := UpdateRunParams{
		RunID:       "run-test-1",
		MessageID:   "msg-run-1",
		Status:      "completed",
		CompletedAt: compTime,
		DurationMs:  15000,
	}
	if err := UpdateScheduleRunStatus(database, updateParams); err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}

	runs, total, err := GetScheduleRunsPaginated(database, 10, 0, "cron-weather", "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated failed: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("Expected 1 run, got total=%d, len=%d", total, len(runs))
	}
	if runs[0].Status != "completed" || runs[0].DurationMs != 15000 {
		t.Errorf("Unexpected run state: %+v", runs[0])
	}
}

func TestNativeVectorSearchHNSW(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	// Create 3 vectors
	vecAlex := make([]float32, 384)
	vecAlex[0] = 1.0 // Alex preference vector

	vecWeather := make([]float32, 384)
	vecWeather[1] = 1.0 // Weather vector

	vecMusic := make([]float32, 384)
	vecMusic[0] = 0.9 // Close to Alex preference

	_, err := InsertFact(database, "preference", "Alex likes dark roast coffee", 1.0, "thread-1", vecAlex)
	if err != nil {
		t.Fatalf("InsertFact 1 failed: %v", err)
	}
	_, err = InsertFact(database, "routine", "Daily weather forecast at 8am", 1.0, "", vecWeather)
	if err != nil {
		t.Fatalf("InsertFact 2 failed: %v", err)
	}
	_, err = InsertFact(database, "preference", "Alex listens to synthwave while coding", 0.8, "thread-1", vecMusic)
	if err != nil {
		t.Fatalf("InsertFact 3 failed: %v", err)
	}

	// Search for query vector close to Alex (vecAlex)
	results, err := SearchSimilarFacts(database, vecAlex, 5, 0.5, "thread-1")
	if err != nil {
		t.Fatalf("SearchSimilarFacts failed: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("Expected at least 2 similar facts for Alex, got %d", len(results))
	}
	if !strings.Contains(results[0].FactText, "dark roast coffee") {
		t.Errorf("Expected top result to be dark roast coffee, got %s", results[0].FactText)
	}
}
