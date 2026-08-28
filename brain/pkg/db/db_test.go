package db

import (
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

	oneShot := OneShotSchedule{
		ID:        "sched-1",
		ThreadID:  "thread-1",
		Prompt:    "test",
		RunAt:     time.Now().UTC().Add(-1 * time.Minute),
		CreatedAt: time.Now().UTC(),
	}
	if err := CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("Failed to create one-shot schedule: %v", err)
	}
	due, err := GetDueOneShotSchedules(database)
	if err != nil || len(due) != 1 {
		t.Fatalf("Expected 1 due schedule, got %d (err: %v)", len(due), err)
	}
	if err := DeleteOneShotSchedule(database, "sched-1"); err != nil {
		t.Fatalf("Failed to delete schedule: %v", err)
	}
}
