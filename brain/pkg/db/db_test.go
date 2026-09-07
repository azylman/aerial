package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
)

func TestNew_ConfigPointerInjection(t *testing.T) {
	// 1. Nil config rejected
	if _, err := New(nil); err == nil {
		t.Errorf("expected error when cfg is nil, got nil")
	}

	// 2. Empty connection string rejected
	cfgEmpty := config.NewFromData(&config.ConfigData{})
	if _, err := New(cfgEmpty); err == nil {
		t.Errorf("expected error for empty connection string, got nil")
	}

	// 3. Unsupported scheme rejected
	cfgInvalid := config.NewFromData(&config.ConfigData{DatabaseURL: "mysql://user:pass@localhost/db"})
	if _, err := New(cfgInvalid); err == nil {
		t.Errorf("expected error for unsupported scheme, got nil")
	}

	// 4. Valid SQLite temp db succeeds (supports both file paths and sqlite:// prefix)
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	cfgValid := config.NewFromData(&config.ConfigData{DatabaseURL: "sqlite://" + tmpFile})
	database, err := New(cfgValid)
	if err != nil {
		t.Fatalf("expected valid sqlite initialization, got: %v", err)
	}
	defer database.Close()
}

func TestInitDB_PostgresAirgap(t *testing.T) {
	// 1. Prohibited by default in test environment
	cfgPg := config.NewFromData(&config.ConfigData{DatabaseURL: "postgres://aerial:aerial@localhost:5432/aerial"})
	_, err := New(cfgPg)
	if err == nil {
		t.Fatalf("expected error connecting to PostgreSQL in test environment, got nil")
	}
	if !strings.Contains(err.Error(), "prohibited in test environments") {
		t.Errorf("expected prohibited error message, got: %v", err)
	}

	// 2. Allowed when AERIAL_ALLOW_TEST_POSTGRES=1 is set (passes airgap, reaches pgx.ParseConfig immediately)
	t.Setenv("AERIAL_ALLOW_TEST_POSTGRES", "1")
	cfgBadSyntax := config.NewFromData(&config.ConfigData{DatabaseURL: "postgres://invalid user@localhost:5432/aerial"})
	_, errBadSyntax := New(cfgBadSyntax)
	if errBadSyntax == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if strings.Contains(errBadSyntax.Error(), "prohibited in test environments") {
		t.Errorf("expected airgap to be bypassed, but got airgap error: %v", errBadSyntax)
	}
	if !strings.Contains(errBadSyntax.Error(), "invalid postgres connection string") {
		t.Errorf("expected invalid postgres connection string error, got: %v", errBadSyntax)
	}
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared&_busy_timeout=5000", t.Name(), time.Now().UnixNano())
	database, err := New(config.NewFromData(&config.ConfigData{DatabaseURL: dsn}))
	if err != nil {
		t.Fatalf("Failed to initialize hermetic SQLite in-memory test database: %v", err)
		return nil
	}
	database.SetMaxOpenConns(1)
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

func TestGetFactsPaginatedImportanceDescending(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	emb := make([]float32, 384)

	// Insert facts with different importance and timestamps
	idLow, err := InsertFact(database, "general", "Low importance fact", 0.2, "t1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	idMax, err := InsertFact(database, "core", "Max importance fact", 1.0, "t1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	idMid, err := InsertFact(database, "infra", "Mid importance fact", 0.5, "t1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	idHigh1, err := InsertFact(database, "preference", "High importance fact 1", 0.8, "t1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	idHigh2, err := InsertFact(database, "preference", "High importance fact 2", 0.8, "t1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	idVeryHigh, err := InsertFact(database, "system", "Very high importance fact", 0.95, "t1", emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	// 1. Unlimited fetch: verify all 6 are returned in exact descending order of importance
	resAll, err := GetFactsPaginated(database, FactsFilter{Limit: 0})
	if err != nil {
		t.Fatalf("GetFactsPaginated unlimited failed: %v", err)
	}
	if resAll.Total != 6 || len(resAll.Facts) != 6 {
		t.Fatalf("Expected 6 total facts, got total=%d, len=%d", resAll.Total, len(resAll.Facts))
	}

	expectedOrder := []int64{idMax, idVeryHigh, idHigh2, idHigh1, idMid, idLow}
	for i, expectedID := range expectedOrder {
		if resAll.Facts[i].ID != expectedID {
			t.Errorf("Index %d: expected fact ID %d (imp=%f), got ID %d (imp=%f, text=%q)",
				i, expectedID, resAll.Facts[i].Importance, resAll.Facts[i].ID, resAll.Facts[i].Importance, resAll.Facts[i].FactText)
		}
	}

	// 2. Pagination fetch: page 1 (Limit: 3, Offset: 0)
	resPage1, err := GetFactsPaginated(database, FactsFilter{Limit: 3, Offset: 0})
	if err != nil {
		t.Fatalf("GetFactsPaginated page 1 failed: %v", err)
	}
	if len(resPage1.Facts) != 3 || resPage1.Total != 6 {
		t.Fatalf("Expected 3 facts on page 1, got %d (total=%d)", len(resPage1.Facts), resPage1.Total)
	}
	if resPage1.Facts[0].ID != idMax || resPage1.Facts[1].ID != idVeryHigh || resPage1.Facts[2].ID != idHigh2 {
		t.Errorf("Page 1 mismatch: got IDs [%d, %d, %d]", resPage1.Facts[0].ID, resPage1.Facts[1].ID, resPage1.Facts[2].ID)
	}

	// 3. Pagination fetch: page 2 (Limit: 3, Offset: 3)
	resPage2, err := GetFactsPaginated(database, FactsFilter{Limit: 3, Offset: 3})
	if err != nil {
		t.Fatalf("GetFactsPaginated page 2 failed: %v", err)
	}
	if len(resPage2.Facts) != 3 || resPage2.Total != 6 {
		t.Fatalf("Expected 3 facts on page 2, got %d (total=%d)", len(resPage2.Facts), resPage2.Total)
	}
	if resPage2.Facts[0].ID != idHigh1 || resPage2.Facts[1].ID != idMid || resPage2.Facts[2].ID != idLow {
		t.Errorf("Page 2 mismatch: got IDs [%d, %d, %d]", resPage2.Facts[0].ID, resPage2.Facts[1].ID, resPage2.Facts[2].ID)
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

func TestRecentThreadMessagesAndActiveIDs(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	_ = InsertMessage(database, Message{
		ID: "m-rec-1", ThreadID: "thread-active-rec", GuildID: "g1", AuthorID: "a1", AuthorName: "Alex",
		Content: "Message 1", Status: StatusCompleted, CreatedAt: now.Add(-10 * time.Minute), UpdatedAt: now.Add(-10 * time.Minute),
	})
	_ = InsertMessage(database, Message{
		ID: "m-rec-2", ThreadID: "thread-active-rec", GuildID: "g1", AuthorID: "a1", AuthorName: "Alex",
		Content: "Message 2", Status: StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})

	ids, err := GetActiveRecentThreadIDs(database, 24*time.Hour)
	if err != nil {
		t.Fatalf("GetActiveRecentThreadIDs failed: %v", err)
	}
	if len(ids) == 0 || ids[0] != "thread-active-rec" {
		t.Errorf("expected thread-active-rec in active IDs, got %v", ids)
	}

	msgs, err := GetRecentThreadMessages(database, "thread-active-rec", 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages failed: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
}

func TestSchedulesLifecycleAndMetrics(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// One-shot schedule lifecycle
	osID := "os-test-1"
	err := CreateOneShotSchedule(database, OneShotSchedule{
		ID:       osID,
		ThreadID: "thread-os",
		Prompt:   "One-shot prompt",
		RunAt:    now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	allOS, err := GetAllOneShotSchedules(database, "thread-os")
	if err != nil {
		t.Fatalf("GetAllOneShotSchedules failed: %v", err)
	}
	if len(allOS) == 0 {
		t.Fatalf("expected at least 1 one-shot schedule")
	}

	err = DeleteOneShotSchedule(database, osID)
	if err != nil {
		t.Fatalf("DeleteOneShotSchedule failed: %v", err)
	}

	// Cron schedule lifecycle
	cronID := "cron-test-1"
	err = CreateCronSchedule(database, CronSchedule{
		ID:          cronID,
		TargetID:    "thread-cron",
		TitlePrefix: "[CRON]",
		CronExpr:    "0 * * * *",
		Prompt:      "Cron prompt",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(time.Hour),
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}

	allCrons, err := GetAllCronSchedules(database, "thread-cron")
	if err != nil {
		t.Fatalf("GetAllCronSchedules failed: %v", err)
	}
	if len(allCrons) == 0 {
		t.Fatalf("expected at least 1 cron schedule")
	}

	err = UpdateCronNextRun(database, cronID, now.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("UpdateCronNextRun failed: %v", err)
	}

	// Schedule metrics
	metrics, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed: %v", err)
	}
	t.Logf("Schedule metrics total active: %d", metrics.TotalActive)

	err = DeleteCronSchedule(database, cronID)
	if err != nil {
		t.Fatalf("DeleteCronSchedule failed: %v", err)
	}
}

func TestReconcileAndPruneScheduleRuns(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	runID := "run-test-1"
	err := CreateScheduleRun(database, ScheduleRun{
		ID:           runID,
		ScheduleID:   "cron-test-1",
		ScheduleType: "cron",
		Prompt:       "Cron prompt",
		StartedAt:    now.Add(-2 * time.Hour),
		Status:       "enqueued",
	})
	if err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	reconciled, err := ReconcileOrphanedScheduleRuns(database)
	if err != nil {
		t.Fatalf("ReconcileOrphanedScheduleRuns failed: %v", err)
	}
	t.Logf("Reconciled orphaned runs: %d", reconciled)

	_ = UpdateScheduleRunStatus(database, UpdateRunParams{
		RunID:       runID,
		Status:      "completed",
		CompletedAt: now,
	})

	pruned, err := PruneScheduleRuns(database, 100, 30*24*time.Hour)
	if err != nil {
		t.Fatalf("PruneScheduleRuns failed: %v", err)
	}
	t.Logf("Pruned schedule runs: %d", pruned)
}

func TestTasksAndTriggers(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	clean := CleanTaskSummary("Task <@12345> details [url](http://foo) ```code``` \n\n extra space")
	if strings.Contains(clean, "<@") || strings.Contains(clean, "```") {
		t.Errorf("CleanTaskSummary did not sanitize markdown/mentions: %s", clean)
	}

	trig := InferTriggerType("thread-cron-123", "Every morning run health check")
	if trig != "cron" && trig != "discord" && trig != "reminder" {
		t.Errorf("unexpected trigger type: %s", trig)
	}

	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	t.Logf("Active tasks found: %d", len(tasks))
}

func TestSessionTurnMapping(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	// Conversation Mapping
	err := SaveConversationMapping(database, "ext-1", "int-1")
	if err != nil {
		t.Fatalf("SaveConversationMapping failed: %v", err)
	}

	intID, err := GetInternalConversationID(database, "ext-1")
	if err != nil || intID != "int-1" {
		t.Errorf("expected int-1, got %s (err: %v)", intID, err)
	}

	extID, err := GetExternalConversationID(database, "int-1")
	if err != nil || extID != "ext-1" {
		t.Errorf("expected ext-1, got %s (err: %v)", extID, err)
	}

	// Turn State
	err = RegisterTurn(database, "ext-1", "m-turn-1", "Turn prompt")
	if err != nil {
		t.Fatalf("RegisterTurn failed: %v", err)
	}

	err = SetTurnProcessing(database, "ext-1", true, "m-turn-1")
	if err != nil {
		t.Fatalf("SetTurnProcessing failed: %v", err)
	}

	turn, err := GetTurnState(database, "ext-1")
	if err != nil || turn == nil {
		t.Fatalf("GetTurnState failed: %v", err)
	}
	if !turn.IsProcessing {
		t.Errorf("expected isProcessing true, got %v", turn.IsProcessing)
	}

	interrupted, err := GetInterruptedTurns(database)
	if err != nil {
		t.Fatalf("GetInterruptedTurns failed: %v", err)
	}
	t.Logf("Interrupted turns: %d", len(interrupted))
}

func TestFactEmbeddingAndThreadLookup(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	vec := make([]float32, ExpectedEmbeddingDim)
	vec[0] = 0.8
	vec[1] = 0.2

	id, err := InsertFact(database, "category", "Fact with emb", 1.0, "thread-lookup-1", vec)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	err = UpdateFactEmbedding(database, id, vec)
	if err != nil {
		t.Fatalf("UpdateFactEmbedding failed: %v", err)
	}

	err = UpdateConversationFactExtractedAt(database, "thread-lookup-1")
	if err != nil {
		t.Fatalf("UpdateConversationFactExtractedAt failed: %v", err)
	}

	facts, err := GetFactsByThreadWithEmbeddings(database, "thread-lookup-1")
	if err != nil {
		t.Fatalf("GetFactsByThreadWithEmbeddings failed: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact for thread, got %d", len(facts))
	}
}

func TestCleanTaskSummary(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty input",
			input:    "",
			expected: "Agent Task",
		},
		{
			name:     "whitespace only",
			input:    "   \n\t  ",
			expected: "Agent Task",
		},
		{
			name:     "user request tag with content prefix",
			input:    "<USER_REQUEST>\n- id: 123\n- content: Deploy new feature\n</USER_REQUEST>",
			expected: "Deploy new feature",
		},
		{
			name:     "user request tag with multiline prompt",
			input:    "<USER_REQUEST>\nPrompt:\nGenerate quarterly report\n</USER_REQUEST>",
			expected: "Generate quarterly report",
		},
		{
			name:     "markdown formatting and mentions stripped",
			input:    "**Hello** <@123456789> `code block`",
			expected: "Hello code block",
		},
		{
			name:     "long text truncated to 140 chars",
			input:    "This is an extraordinarily long prompt designed specifically to verify that the CleanTaskSummary helper properly cuts off strings exceeding one hundred and forty runes with an ellipsis suffix accurately.",
			expected: "This is an extraordinarily long prompt designed specifically to verify that the CleanTaskSummary helper properly cuts off strings exceedi...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CleanTaskSummary(tt.input)
			if got != tt.expected {
				t.Errorf("CleanTaskSummary() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestInferTriggerType(t *testing.T) {
	if tt := InferTriggerType("http-client", ""); tt != "http" {
		t.Errorf("expected 'http', got %q", tt)
	}
	if tt := InferTriggerType("user-1", "cron-run-123"); tt != "cron" {
		t.Errorf("expected 'cron', got %q", tt)
	}
	if tt := InferTriggerType("user-1", "reminder-run-456"); tt != "reminder" {
		t.Errorf("expected 'reminder', got %q", tt)
	}
	if tt := InferTriggerType("user-1", ""); tt != "discord" {
		t.Errorf("expected 'discord', got %q", tt)
	}
}

func TestSQLiteExplicitSchemaAndMigrations(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "aerial_sqlite_explicit.db")
	database, err := InitDB(sqlitePath)
	if err != nil {
		t.Fatalf("InitDB failed for SQLite: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Test SQLite message insert and query
	msg := Message{
		ID:         "msg-sqlite-1",
		ThreadID:   "th-sqlite-1",
		GuildID:    "g-sqlite-1",
		AuthorID:   "author-1",
		AuthorName: "Alice",
		Content:    "Hello SQLite",
		Status:     StatusPending,
	}
	if err := InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed on SQLite: %v", err)
	}

	m, err := GetMessage(database, "msg-sqlite-1")
	if err != nil || m == nil {
		t.Fatalf("GetMessage failed on SQLite: %v", err)
	}

	// Test SQLite facts insert and vector search
	vec := make([]float32, ExpectedEmbeddingDim)
	vec[0] = 1.0
	factID, err := InsertFact(database, "pref", "User likes tea", 0.9, "th-sqlite-1", vec)
	if err != nil {
		t.Fatalf("InsertFact on SQLite failed: %v", err)
	}
	if factID <= 0 {
		t.Errorf("expected positive factID, got %d", factID)
	}

	simFacts, err := SearchSimilarFacts(database, vec, 5, 0.5, "th-sqlite-1")
	if err != nil {
		t.Fatalf("SearchSimilarFacts on SQLite failed: %v", err)
	}
	if len(simFacts) == 0 {
		t.Errorf("expected similar facts returned on SQLite")
	}

	// Test SQLite active tasks query
	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("GetActiveTasks on SQLite failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 active task, got %d", len(tasks))
	}
}

func TestVectorHelpersEdgeCases(t *testing.T) {
	// 1. Float32ToBytes and BytesToFloat32 empty
	b := Float32ToBytes(nil)
	if len(b) != 0 {
		t.Errorf("expected empty bytes for nil slice")
	}
	f := BytesToFloat32(nil)
	if len(f) != 0 {
		t.Errorf("expected empty float slice for nil bytes")
	}

	// 2. BytesToFloat32 invalid length
	invalidBytes := []byte{1, 2, 3}
	fInv := BytesToFloat32(invalidBytes)
	if len(fInv) != 0 {
		t.Errorf("expected empty slice for misaligned byte slice")
	}

	// 3. cosineSimilarity zero vectors
	sim := cosineSimilarity(nil, nil)
	if sim != 0.0 {
		t.Errorf("expected 0.0 for nil vectors, got %f", sim)
	}
	vZero := make([]float32, 10)
	vOne := make([]float32, 10)
	vOne[0] = 1.0
	sim = cosineSimilarity(vZero, vOne)
	if sim != 0.0 {
		t.Errorf("expected 0.0 when one vector is zero, got %f", sim)
	}
}

func TestNullVector_Scan(t *testing.T) {
	var nv NullVector

	// 1. nil
	if err := nv.Scan(nil); err != nil || nv.Valid || nv.Vector != nil {
		t.Errorf("expected nil result for nil src, got err: %v, valid: %t", err, nv.Valid)
	}

	// 2. valid string
	if err := nv.Scan("[0.1,0.2,0.3]"); err != nil || !nv.Valid || len(nv.Vector) != 3 {
		t.Errorf("expected valid vector from string, got err: %v, len: %d", err, len(nv.Vector))
	}

	// 3. valid byte slice with string format
	if err := nv.Scan([]byte("[0.4,0.5]")); err != nil || !nv.Valid || len(nv.Vector) != 2 {
		t.Errorf("expected valid vector from byte string, got err: %v, len: %d", err, len(nv.Vector))
	}

	// 4. raw float32 byte slice
	rawBytes := Float32ToBytes([]float32{1.0, 2.0, 3.0, 4.0})
	if err := nv.Scan(rawBytes); err != nil || !nv.Valid || len(nv.Vector) != 4 {
		t.Errorf("expected valid vector from raw float bytes, got err: %v, len: %d", err, len(nv.Vector))
	}

	// 5. invalid int type
	if err := nv.Scan(12345); err == nil {
		t.Error("expected error for invalid int type in NullVector.Scan")
	}
}

func TestDB_NilDatabaseHandling_All(t *testing.T) {
	// Messages
	_ = InsertMessage(nil, Message{})
	_ = UpdateMessageStatus(nil, "m1", StatusCompleted, "")
	_ = UpdateMessageCompleted(nil, "m1", "resp")
	_ = IncrementMessageRetry(nil, "m1", "err")
	_, _ = GetPendingOrProcessingMessages(nil)
	_, _ = GetMessage(nil, "m1")
	_, _ = MessageExists(nil, "m1")
	_, _ = ClaimPendingMessage(nil, "m1")
	_, _ = GetActiveRecentThreadIDs(nil, 1)
	_, _ = GetRecentThreadMessages(nil, "th1", 10)
	_, _ = GetMaxMessageRowID(nil, "th1")

	// Schedules
	_ = CreateOneShotSchedule(nil, OneShotSchedule{})
	_, _ = GetDueOneShotSchedules(nil)
	_ = DeleteOneShotSchedule(nil, "s1")
	_ = InsertMessageAndConsumeOneShot(nil, "s1", Message{})
	_, _ = GetAllOneShotSchedules(nil, "")
	_ = CreateCronSchedule(nil, CronSchedule{})
	_, _ = GetDueCronSchedules(nil)
	_, _ = GetAllCronSchedules(nil, "")
	_ = DeleteCronSchedule(nil, "c1")
	_ = UpdateCronNextRun(nil, "c1", time.Now())
	_ = CreateScheduleRun(nil, ScheduleRun{})
	_ = UpdateScheduleRunStatus(nil, UpdateRunParams{})
	_, _, _ = GetScheduleRunsPaginated(nil, 10, 0, "", "")
	_, _ = GetScheduleSummaryMetrics(nil)
	_, _ = ReconcileOrphanedScheduleRuns(nil)
	_, _ = PruneScheduleRuns(nil, 10, time.Hour)

	// Sessions
	_, _ = GetSessionID(nil, "th1")
	_ = SaveSessionID(nil, "th1", "s1")
	_ = DeleteSessionID(nil, "th1")
	_, _ = IncrementSessionTurnCount(nil, "th1")
	_ = RotateSessionID(nil, "th1", "s2")
	_, _ = GetSessionTurnCount(nil, "th1")
	_, _ = GetExternalConversationID(nil, "s1")
	_ = RegisterTurn(nil, "th1", "m1", "prompt")
	_ = SetTurnProcessing(nil, "th1", true, "m1")
	_, _ = GetTurnState(nil, "th1")
	_, _ = GetInterruptedTurns(nil)

	// Facts
	_, _ = InsertFact(nil, "cat", "fact", 1.0, "th1", nil)
	_ = UpdateFactEmbedding(nil, 1, nil)
	_, _ = GetAllFactsWithEmbeddings(nil)
	_, _ = GetFactsByThreadWithEmbeddings(nil, "th1")
	_, _ = SearchSimilarFacts(nil, nil, 10, 0.5, "")
	_, _ = GetActiveConversationsForExtraction(nil, 1)
	_ = UpdateConversationFactWatermark(nil, "th1", 1)
	_ = UpdateConversationFactExtractedAt(nil, "th1")
	_, _ = GetFactsPaginated(nil, FactsFilter{})

	// Tasks
	_, _ = GetActiveTasks(nil)
}

func TestDB_ClosedDatabaseHandling_All(t *testing.T) {
	closedDB, err := InitDB(filepath.Join(t.TempDir(), "closed_all.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = closedDB.Close()

	now := time.Now().UTC()
	vec := make([]float32, ExpectedEmbeddingDim)

	// Messages
	_ = InsertMessage(closedDB, Message{ID: "m1", ThreadID: "th1", Status: StatusPending, CreatedAt: now})
	_ = UpdateMessageStatus(closedDB, "m1", StatusCompleted, "")
	_ = UpdateMessageCompleted(closedDB, "m1", "resp")
	_ = IncrementMessageRetry(closedDB, "m1", "err")
	_, _ = GetPendingOrProcessingMessages(closedDB)
	_, _ = GetMessage(closedDB, "m1")
	_, _ = MessageExists(closedDB, "m1")
	_, _ = ClaimPendingMessage(closedDB, "m1")
	_, _ = GetActiveRecentThreadIDs(closedDB, 1)
	_, _ = GetRecentThreadMessages(closedDB, "th1", 10)
	_, _ = GetMaxMessageRowID(closedDB, "th1")

	// Schedules
	_ = CreateOneShotSchedule(closedDB, OneShotSchedule{ID: "s1", ThreadID: "th1", RunAt: now})
	_, _ = GetDueOneShotSchedules(closedDB)
	_ = DeleteOneShotSchedule(closedDB, "s1")
	_ = InsertMessageAndConsumeOneShot(closedDB, "s1", Message{ID: "m1", ThreadID: "th1"})
	_, _ = GetAllOneShotSchedules(closedDB, "th1")
	_ = CreateCronSchedule(closedDB, CronSchedule{ID: "c1", TargetID: "th1", CronExpr: "0 0 * * *"})
	_, _ = GetDueCronSchedules(closedDB)
	_, _ = GetAllCronSchedules(closedDB, "")
	_ = DeleteCronSchedule(closedDB, "c1")
	_ = UpdateCronNextRun(closedDB, "c1", now)
	_ = CreateScheduleRun(closedDB, ScheduleRun{ID: "r1", ScheduleID: "c1", StartedAt: now})
	_ = UpdateScheduleRunStatus(closedDB, UpdateRunParams{RunID: "r1", Status: "completed"})
	_, _, _ = GetScheduleRunsPaginated(closedDB, 10, 0, "c1", "completed")
	_, _ = GetScheduleSummaryMetrics(closedDB)
	_, _ = ReconcileOrphanedScheduleRuns(closedDB)
	_, _ = PruneScheduleRuns(closedDB, 10, time.Hour)

	// Sessions
	_, _ = GetSessionID(closedDB, "th1")
	_ = SaveSessionID(closedDB, "th1", "s1")
	_ = DeleteSessionID(closedDB, "th1")
	_, _ = IncrementSessionTurnCount(closedDB, "th1")
	_ = RotateSessionID(closedDB, "th1", "s2")
	_, _ = GetSessionTurnCount(closedDB, "th1")
	_, _ = GetExternalConversationID(closedDB, "s1")
	_ = RegisterTurn(closedDB, "th1", "m1", "prompt")
	_ = SetTurnProcessing(closedDB, "th1", true, "m1")
	_, _ = GetTurnState(closedDB, "th1")
	_, _ = GetInterruptedTurns(closedDB)

	// Facts
	_, _ = InsertFact(closedDB, "cat", "fact", 1.0, "th1", vec)
	_ = UpdateFactEmbedding(closedDB, 1, vec)
	_, _ = GetAllFactsWithEmbeddings(closedDB)
	_, _ = GetFactsByThreadWithEmbeddings(closedDB, "th1")
	_, _ = SearchSimilarFacts(closedDB, vec, 10, 0.5, "th1")
	_, _ = GetActiveConversationsForExtraction(closedDB, 1)
	_ = UpdateConversationFactWatermark(closedDB, "th1", 1)
	_ = UpdateConversationFactExtractedAt(closedDB, "th1")
	_, _ = GetFactsPaginated(closedDB, FactsFilter{Query: "fact", Category: "cat"})

	// Tasks
	_, _ = GetActiveTasks(closedDB)
}

func TestDB_InitDB_InvalidPath(t *testing.T) {
	_, err := InitDB("/proc/sys/fs/nonexistent_dir_12345/database.db")
	if err == nil {
		t.Error("expected error for invalid DB path")
	}
}

func TestDB_GetScheduleSummaryMetrics_Calculations(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	now := time.Now().UTC()
	// Insert enabled cron with next run
	_ = CreateCronSchedule(database, CronSchedule{
		ID:        "sched-cron-active",
		CronExpr:  "0 9 * * *",
		NextRunAt: now.Add(2 * time.Hour),
		Enabled:   true,
	})
	// Insert one-shot with next run
	_ = CreateOneShotSchedule(database, OneShotSchedule{
		ID:     "sched-one-active",
		RunAt:  now.Add(1 * time.Hour),
		Prompt: "active reminder",
	})

	// Insert completed run with duration
	completedTime := now.Add(5 * time.Second)
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-metric-comp",
		ScheduleID:   "sched-1",
		ScheduleType: "cron",
		MessageID:    "m-metric-1",
		TargetID:     "th-1",
		ThreadID:     "th-1",
		Title:        "Cron Run 1",
		Prompt:       "Run Prompt",
		Status:       "completed",
		StartedAt:    now.Add(-10 * time.Minute),
		CompletedAt:  &completedTime,
		DurationMs:   5000,
	})

	// Insert failed run
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-metric-fail",
		ScheduleID:   "sched-2",
		ScheduleType: "cron",
		MessageID:    "m-metric-2",
		TargetID:     "th-2",
		ThreadID:     "th-2",
		Title:        "Cron Run 2",
		Prompt:       "Run Prompt 2",
		Status:       "failed",
		StartedAt:    now.Add(-5 * time.Minute),
		Error:        "fatal timeout",
	})

	// Insert running run
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-metric-running",
		ScheduleID:   "sched-3",
		ScheduleType: "one_shot",
		MessageID:    "m-metric-3",
		TargetID:     "th-3",
		ThreadID:     "th-3",
		Title:        "One-shot 1",
		Prompt:       "Run Prompt 3",
		Status:       "running",
		StartedAt:    now.Add(-1 * time.Minute),
	})

	metrics, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed: %v", err)
	}

	if metrics.TotalRuns24h != 3 {
		t.Errorf("expected TotalRuns24h = 3, got %d", metrics.TotalRuns24h)
	}
	if metrics.SuccessRate24h != 33.3 {
		t.Errorf("expected SuccessRate24h = 33.3, got %f", metrics.SuccessRate24h)
	}
	if metrics.TotalActive != 2 {
		t.Errorf("expected TotalActive = 2, got %d", metrics.TotalActive)
	}
	if metrics.NextRunAt == nil {
		t.Error("expected non-nil NextRunAt")
	}
}

func TestDB_GetFactsPaginated_Filters(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	vec := make([]float32, ExpectedEmbeddingDim)
	vec[0] = 1.0

	_, _ = InsertFact(database, "user_pref", "User likes espresso", 1.0, "th-fact-1", vec)
	_, _ = InsertFact(database, "system_config", "Server running on port 8080", 0.9, "th-fact-2", vec)
	_, _ = InsertFact(database, "user_pref", "User likes tea", 0.8, "th-fact-1", vec)

	// 1. Filter by category
	resCat, err := GetFactsPaginated(database, FactsFilter{Category: "user_pref", Limit: 10, Offset: 0})
	if err != nil || resCat.Total != 2 || len(resCat.Facts) != 2 {
		t.Errorf("expected 2 user_pref facts, got total=%d, len=%d, err=%v", resCat.Total, len(resCat.Facts), err)
	}

	// 2. Filter by search query
	resQ, err := GetFactsPaginated(database, FactsFilter{Query: "espresso", Limit: 10, Offset: 0})
	if err != nil || resQ.Total != 1 || len(resQ.Facts) != 1 {
		t.Errorf("expected 1 espresso fact, got total=%d, len=%d, err=%v", resQ.Total, len(resQ.Facts), err)
	}

	// 3. Pagination limit and offset
	resP, err := GetFactsPaginated(database, FactsFilter{Limit: 1, Offset: -1})
	if err != nil || resP.Total != 3 || len(resP.Facts) != 1 {
		t.Errorf("expected total=3, len=1 for limit=1, got total=%d, len=%d, err=%v", resP.Total, len(resP.Facts), err)
	}
}

func TestDB_GetScheduleRunsPaginated_Filters(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	now := time.Now().UTC()
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-f-1",
		ScheduleID:   "sched-filter-1",
		ScheduleType: "cron",
		MessageID:    "m-1",
		TargetID:     "th-1",
		ThreadID:     "th-1",
		Title:        "Title 1",
		Prompt:       "Prompt 1",
		Status:       "completed",
		StartedAt:    now,
	})
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-f-2",
		ScheduleID:   "sched-filter-1",
		ScheduleType: "cron",
		MessageID:    "m-2",
		TargetID:     "th-1",
		ThreadID:     "th-1",
		Title:        "Title 2",
		Prompt:       "Prompt 2",
		Status:       "failed",
		StartedAt:    now,
	})
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-f-3",
		ScheduleID:   "sched-filter-2",
		ScheduleType: "cron",
		MessageID:    "m-3",
		TargetID:     "th-2",
		ThreadID:     "th-2",
		Title:        "Title 3",
		Prompt:       "Prompt 3",
		Status:       "completed",
		StartedAt:    now,
	})

	// 1. Filter by scheduleID
	runs1, total1, err := GetScheduleRunsPaginated(database, 10, 0, "sched-filter-1", "")
	if err != nil || total1 != 2 || len(runs1) != 2 {
		t.Errorf("expected 2 runs for sched-filter-1, got total=%d, len=%d, err=%v", total1, len(runs1), err)
	}

	// 2. Filter by status
	runs2, total2, err := GetScheduleRunsPaginated(database, 10, 0, "", "failed")
	if err != nil || total2 != 1 || len(runs2) != 1 {
		t.Errorf("expected 1 failed run, got total=%d, len=%d, err=%v", total2, len(runs2), err)
	}
}

func TestDB_SearchSimilarFacts_EdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. nil embedding / mismatched embedding dimension
	res, err := SearchSimilarFacts(database, []float32{1.0, 2.0}, 10, 0.5, "")
	if res != nil || err != nil {
		t.Errorf("expected nil, nil for mismatched dimensions, got %v, %v", res, err)
	}

	// 2. limit <= 0 and minScore <= 0 defaults
	vec1 := make([]float32, ExpectedEmbeddingDim)
	vec1[0] = 1.0
	_, _ = InsertFact(database, "cat1", "Fact text 1", 1.0, "th-1", vec1)

	vec2 := make([]float32, ExpectedEmbeddingDim)
	vec2[0] = 0.5
	vec2[1] = 0.5
	_, _ = InsertFact(database, "cat2", "Fact text 2", 0.8, "th-2", vec2)

	vec3 := make([]float32, ExpectedEmbeddingDim)
	vec3[0] = 0.1
	_, _ = InsertFact(database, "cat3", "Fact text 3", 0.1, "", vec3)

	res2, err := SearchSimilarFacts(database, vec1, -1, -1, "")
	if err != nil || len(res2) < 1 {
		t.Errorf("expected >=1 fact returned with default limit/score, got %d, err: %v", len(res2), err)
	}

	// 3. Filter by specific thread
	resThread, err := SearchSimilarFacts(database, vec1, 10, 0.2, "th-1")
	if err != nil {
		t.Fatalf("unexpected error searching facts by thread: %v", err)
	}
	for _, f := range resThread {
		if f.ThreadID != "" && f.ThreadID != "th-1" {
			t.Errorf("expected thread th-1 or empty, got %s", f.ThreadID)
		}
	}

	// 4. High minScore filter
	resHigh, err := SearchSimilarFacts(database, vec1, 10, 0.95, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resHigh) != 1 {
		t.Errorf("expected only 1 fact with score >= 0.95, got %d", len(resHigh))
	}
}

func TestDB_InitDB_PostgresConnectionFailure(t *testing.T) {
	origMax := postgresMaxAttempts
	origBase := postgresRetryBase
	postgresMaxAttempts = 2
	postgresRetryBase = 5 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origMax
		postgresRetryBase = origBase
	}()

	invalidDSN := "postgres://invalid_user:invalid_pass@127.0.0.1:59999/invalid_db?sslmode=disable"
	db, err := InitDB(invalidDSN)
	if err == nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("expected connection error for invalid postgres DSN, got nil")
	}
}

func TestDB_InitDB_SQLiteSuccessAndSchemaError(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test_init.db")
	db, err := InitDB(tempFile)
	if err != nil {
		t.Fatalf("failed to init SQLite DB: %v", err)
	}
	_ = db.Close()

	// Closed DB schema init error
	closedDB, err := sql.Open("sqlite", ":memory:")
	if err == nil && closedDB != nil {
		_ = closedDB.Close()
		ctx := context.Background()
		if err := initSchemaSQLite(closedDB); err == nil {
			t.Error("expected error calling initSchemaSQLite on closed db")
		}
		if err := initSchemaPostgres(ctx, closedDB); err == nil {
			t.Error("expected error calling initSchemaPostgres on closed db")
		}
	}
}

func TestDB_GetScheduleSummaryMetrics_NextRunVariants(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	now := time.Now().UTC()

	// Variant 1: OneShot is earlier than Cron
	_ = CreateCronSchedule(database, CronSchedule{ID: "s-cron-1", TargetID: "th-1", TitlePrefix: "Cron 1", Prompt: "P", CronExpr: "0 0 * * *", NextRunAt: now.Add(2 * time.Hour), Enabled: true})
	_ = CreateOneShotSchedule(database, OneShotSchedule{ID: "s-one-1", ThreadID: "th-1", Prompt: "P", RunAt: now.Add(1 * time.Hour)})

	m1, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m1.NextRunAt == nil {
		t.Errorf("expected NextRunAt to be non-nil, got nil")
	}

	// Clean up one shot, leaving only Cron
	_ = DeleteOneShotSchedule(database, "s-one-1")
	m2, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m2.NextRunAt == nil {
		t.Error("expected NextRunAt to be cron (2 hours from now), got nil")
	}

	// Clean up cron and insert only one-shot
	_ = DeleteCronSchedule(database, "s-cron-1")
	_ = CreateOneShotSchedule(database, OneShotSchedule{ID: "s-one-2", ThreadID: "th-1", Prompt: "P", RunAt: now.Add(3 * time.Hour)})
	m3, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m3.NextRunAt == nil {
		t.Error("expected NextRunAt to be one-shot (3 hours from now), got nil")
	}
}

func TestDB_SearchSimilarFacts_SQLite_InvalidEmbeddingDimensions(t *testing.T) {
	database, err := InitDB(filepath.Join(t.TempDir(), "sqlite_embed_dim.db"))
	if err != nil {
		t.Fatalf("failed to init SQLite DB: %v", err)
	}
	defer database.Close()

	vecValid := make([]float32, ExpectedEmbeddingDim)
	vecValid[0] = 0.9

	// Insert fact with valid embedding
	f1ID, err := InsertFact(database, "cat", "Valid fact", 0.9, "th-match", vecValid)
	if err != nil {
		t.Fatalf("failed to insert fact: %v", err)
	}

	// Update embedding bytes to valid binary float32 slice of mismatched dimension (10 != ExpectedEmbeddingDim)
	shortVec := Float32ToBytes(make([]float32, 10))
	_, err = database.Exec("UPDATE facts SET embedding = $1 WHERE id = $2", shortVec, f1ID)
	if err != nil {
		t.Fatalf("failed to update embedding: %v", err)
	}

	res, err := SearchSimilarFacts(database, vecValid, 10, 0.1, "th-match")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("expected 0 facts for mismatched embedding length, got %d", len(res))
	}
}

func TestDB_IsPostgres_And_MessageStatus_EdgeCases(t *testing.T) {
	if isPostgres(nil) {
		t.Error("expected false for nil db")
	}

	database := setupTestDB(t)
	defer database.Close()

	_ = isPostgres(database)

	// Insert test message
	msgID := "msg-status-test-1"
	_ = InsertMessage(database, Message{
		ID:       msgID,
		ThreadID: "chan-1",
		Content:  "test content",
		Status:   StatusPending,
	})

	if err := UpdateMessageStatus(database, msgID, StatusProcessing, "working"); err != nil {
		t.Fatalf("failed to update message status: %v", err)
	}

	if err := IncrementMessageRetry(database, msgID, "transient error occurred"); err != nil {
		t.Fatalf("failed to increment retry: %v", err)
	}

	if err := UpdateMessageCompleted(database, msgID, "final response text"); err != nil {
		t.Fatalf("failed to update message completed: %v", err)
	}

	msg, err := GetMessage(database, msgID)
	if err != nil || msg.Status != StatusCompleted || msg.ResponseText != "final response text" {
		t.Errorf("unexpected message state: %+v, err: %v", msg, err)
	}

	// Prune schedule runs
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-old-1",
		ScheduleID:   "s-1",
		ScheduleType: "one_shot",
		MessageID:    "m-1",
		TargetID:     "t-1",
		ThreadID:     "t-1",
		Title:        "Old",
		Prompt:       "P",
		Status:       "completed",
		StartedAt:    time.Now().Add(-48 * time.Hour),
	})

	deleted, err := PruneScheduleRuns(database, 10, 24*time.Hour)
	if err != nil {
		t.Fatalf("unexpected prune error: %v", err)
	}
	if deleted < 0 {
		t.Errorf("expected deleted >= 0, got %d", deleted)
	}

	// Active Tasks
	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("unexpected active tasks error: %v", err)
	}
	_ = tasks
}

func TestDB_NullVector_Scan_AllBranches(t *testing.T) {
	var nv NullVector

	// 1. nil
	if err := nv.Scan(nil); err != nil || nv.Valid || nv.Vector != nil {
		t.Errorf("expected Valid=false for nil src, got %+v", nv)
	}

	// 2. string pgvector format
	if err := nv.Scan("[1.0,2.0,3.0]"); err != nil || !nv.Valid || len(nv.Vector) != 3 {
		t.Errorf("expected 3 items for string pgvector, got %+v, err: %v", nv, err)
	}

	// 3. invalid string
	if err := nv.Scan("invalid-string"); err == nil {
		t.Error("expected error for invalid string pgvector")
	}

	// 4. []byte pgvector format
	if err := nv.Scan([]byte("[0.5,0.25]")); err != nil || !nv.Valid || len(nv.Vector) != 2 {
		t.Errorf("expected 2 items for byte pgvector, got %+v, err: %v", nv, err)
	}

	// 5. []byte binary float32
	binFloats := Float32ToBytes([]float32{1.5, 2.5})
	if err := nv.Scan(binFloats); err != nil || !nv.Valid || len(nv.Vector) != 2 {
		t.Errorf("expected 2 items for binary float bytes, got %+v, err: %v", nv, err)
	}

	// 6. []byte odd length (not divisible by 4)
	_ = nv.Scan([]byte{1, 2, 3})

	// 7. Unsupported type
	if err := nv.Scan(12345); err == nil {
		t.Error("expected error scanning int into NullVector")
	}
}

func TestDB_Fact_InsertAndEmbedding_EdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. Empty fact text error
	_, err := InsertFact(database, "cat", "", 1.0, "th-1", nil)
	if err == nil {
		t.Error("expected error for empty fact text")
	}

	// 2. Defaults for category and importance
	fID, err := InsertFact(database, "", "Default cat and imp", -1.0, "th-1", nil)
	if err != nil {
		t.Fatalf("failed to insert fact with defaults: %v", err)
	}
	if fID <= 0 {
		t.Errorf("expected fID > 0, got %d", fID)
	}

	// 3. UpdateFactEmbedding dimension error
	if err := UpdateFactEmbedding(database, fID, []float32{1.0, 2.0}); err == nil {
		t.Error("expected error updating fact embedding with invalid dim")
	}

	// 4. GetFactsByThreadWithEmbeddings empty thread
	facts, err := GetFactsByThreadWithEmbeddings(database, "")
	if err != nil || len(facts) == 0 {
		t.Errorf("expected non-empty facts for empty thread query, got %v, err: %v", facts, err)
	}
}

func TestDB_SessionsAndSchedules_MoreBranches(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. GetSessionTurnCount and GetExternalConversationID non-existent
	cnt, err := GetSessionTurnCount(database, "non-existent-thread")
	if err != nil || cnt != 0 {
		t.Errorf("expected 0 turn count for non-existent thread, got %d, err: %v", cnt, err)
	}

	extID, err := GetExternalConversationID(database, "non-existent-internal-id")
	if err != nil || extID != "" {
		t.Errorf("expected empty extID, got %s, err: %v", extID, err)
	}

	// 2. SetTurnProcessing with and without message ID
	if err := SetTurnProcessing(database, "th-1", true, ""); err != nil {
		t.Errorf("unexpected error with empty message ID: %v", err)
	}

	_ = InsertMessage(database, Message{
		ID:       "msg-turn-1",
		ThreadID: "th-1",
		Content:  "turn prompt",
		Status:   StatusPending,
	})
	if err := SetTurnProcessing(database, "th-1", false, "msg-turn-1"); err != nil {
		t.Errorf("unexpected error updating turn message: %v", err)
	}

	// 3. InsertMessageAndConsumeOneShot edge cases
	if err := InsertMessageAndConsumeOneShot(nil, "s-1", Message{ID: "m-1"}); err == nil {
		t.Error("expected error for nil database")
	}
	if err := InsertMessageAndConsumeOneShot(database, "s-1", Message{ID: ""}); err == nil {
		t.Error("expected error for empty message ID")
	}
	if err := InsertMessageAndConsumeOneShot(database, "non-existent-sched-id", Message{ID: "m-valid", ThreadID: "th-1", Content: "c"}); err == nil {
		t.Error("expected error consuming non-existent one-shot schedule")
	}

	// 4. GetAllOneShotSchedules and GetAllCronSchedules
	_ = CreateOneShotSchedule(database, OneShotSchedule{ID: "one-all-1", ThreadID: "th-1", Prompt: "P", RunAt: time.Now()})
	ones, err := GetAllOneShotSchedules(database, "")
	if err != nil || len(ones) == 0 {
		t.Errorf("expected >= 1 one-shot schedule, got %v, err: %v", ones, err)
	}
	onesTh, _ := GetAllOneShotSchedules(database, "th-1")
	if len(onesTh) == 0 {
		t.Errorf("expected >= 1 one-shot for th-1")
	}

	_ = CreateCronSchedule(database, CronSchedule{ID: "cron-all-1", TargetID: "th-1", TitlePrefix: "T", Prompt: "P", CronExpr: "0 0 * * *", NextRunAt: time.Now(), Enabled: true})
	crons, err := GetAllCronSchedules(database, "")
	if err != nil || len(crons) == 0 {
		t.Errorf("expected >= 1 cron schedule, got %v, err: %v", crons, err)
	}
	cronsTh, _ := GetAllCronSchedules(database, "th-1")
	if len(cronsTh) == 0 {
		t.Errorf("expected >= 1 cron for th-1")
	}

	// 5. UpdateScheduleRunStatus statuses
	_ = CreateScheduleRun(database, ScheduleRun{
		ID:           "run-status-1",
		ScheduleID:   "s-1",
		ScheduleType: "cron",
		MessageID:    "m-1",
		TargetID:     "th-1",
		ThreadID:     "th-1",
		Title:        "Title",
		Prompt:       "Prompt",
		Status:       "running",
		StartedAt:    time.Now(),
	})
	if err := UpdateScheduleRunStatus(database, UpdateRunParams{
		RunID:       "run-status-1",
		Status:      "failed",
		Error:       "something failed",
		CompletedAt: time.Now(),
		DurationMs:  500,
	}); err != nil {
		t.Errorf("failed to update run status: %v", err)
	}

	// 6. GetPendingOrProcessingMessages and GetRecentThreadMessages with full fields
	_ = InsertMessage(database, Message{
		ID:            "msg-full-1",
		ThreadID:      "th-full",
		Content:       "full message",
		Status:        StatusPending,
		ErrorMessage:  "some error",
		ResponseText:  "some resp",
		ScheduleRunID: "run-1",
	})
	pendingMsgs, err := GetPendingOrProcessingMessages(database)
	if err != nil || len(pendingMsgs) == 0 {
		t.Errorf("expected pending messages, got %v, err: %v", pendingMsgs, err)
	}

	// 7. MessageExists and ClaimPendingMessage nil / empty checks
	if _, err := MessageExists(nil, "msg-1"); err == nil {
		t.Error("expected error for nil db in MessageExists")
	}
	if exists, err := MessageExists(database, ""); err != nil || exists {
		t.Errorf("expected false, nil for empty ID in MessageExists, got %v, %v", exists, err)
	}
	if claimed, err := ClaimPendingMessage(nil, "msg-1"); err != nil || claimed {
		t.Errorf("expected false, nil for nil db in ClaimPendingMessage, got %v, %v", claimed, err)
	}
	// 8. CleanTaskSummary multiline Prompt: extraction
	multiPrompt := "<USER_REQUEST>\nPrompt:\n\nActual multi line prompt body\n</USER_REQUEST>"
	resPrompt := CleanTaskSummary(multiPrompt)
	if resPrompt != "Actual multi line prompt body" {
		t.Errorf("expected 'Actual multi line prompt body', got %q", resPrompt)
	}

	// 9. GetActiveConversationsForExtraction nil and defaults
	if convs, err := GetActiveConversationsForExtraction(nil, 10); convs != nil || err != nil {
		t.Errorf("expected nil, nil for nil db, got %v, %v", convs, err)
	}
	if _, err := GetActiveConversationsForExtraction(database, -1); err != nil {
		t.Errorf("unexpected error with default active hours: %v", err)
	}

	// 10. GetFactsPaginated nil db, negative offset, and offset-only pagination
	if _, err := GetFactsPaginated(nil, FactsFilter{}); err == nil {
		t.Error("expected error for nil db in GetFactsPaginated")
	}
	resOff, err := GetFactsPaginated(database, FactsFilter{Offset: 1, Limit: 0})
	if err != nil {
		t.Fatalf("unexpected error for offset-only pagination: %v", err)
	}
	_ = resOff

	resNeg, err := GetFactsPaginated(database, FactsFilter{Offset: -5, Limit: 5})
	if err != nil {
		t.Fatalf("unexpected error for negative offset: %v", err)
	}
	_ = resNeg
}









