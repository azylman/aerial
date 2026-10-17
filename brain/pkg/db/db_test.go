package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
)


func TestNew_ConfigPointerInjection(t *testing.T) {
	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	defer func() { sqliteTestInitHook = oldHook }()

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

	// 4. SQLite scheme rejected by New() in production (nil hook)
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	cfgSQLite := config.NewFromData(&config.ConfigData{DatabaseURL: "sqlite://" + tmpFile})
	if _, err := New(cfgSQLite); err == nil {
		t.Errorf("expected error for sqlite scheme in New, got nil")
	}

	// 5. With hook restored, SQLite succeeds in test mode
	sqliteTestInitHook = oldHook
	if oldHook != nil {
		dbHooked, err := New(cfgSQLite)
		if err != nil {
			t.Errorf("expected New to succeed with hooked sqlite, got: %v", err)
		} else {
			dbHooked.Close()
		}
	}

	// 6. InitDB type validation for non-string / non-config
	if _, err := InitDB(12345); err == nil {
		t.Errorf("expected error for int type in InitDB, got nil")
	}

	// 7. New with nil config snapshot
	cfgEmptySnap := &config.Config{}
	if _, err := New(cfgEmptySnap); err == nil {
		t.Errorf("expected error for nil config snapshot in New, got nil")
	}

	// 8. InitDB DSN pragma variants
	for _, dsn := range []string{
		"file:mem_test_1?mode=memory&cache=shared",
		"file:mem_test_2?mode=memory",
		"file:mem_test_3?mode=memory&_pragma=busy_timeout(5000)",
		"sqlite://:memory:",
	} {
		dbPragma, err := InitDB(dsn)
		if err != nil {
			t.Fatalf("InitDB(%q) failed: %v", dsn, err)
		}
		_ = dbPragma.Close()
	}

	// 9. Subfolder creation via initTestSQLite
	subFile := filepath.Join(t.TempDir(), "subfolder", "test.db")
	dbSub, err := initTestSQLite(subFile)
	if err != nil {
		t.Fatalf("initTestSQLite with subfolder path failed: %v", err)
	}
	_ = dbSub.Close()
	if _, err := os.Stat(subFile); os.IsNotExist(err) {
		t.Errorf("expected sqlite db file to exist at %s", subFile)
	}
}

func TestInitDB_PostgresValidation(t *testing.T) {
	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	defer func() { sqliteTestInitHook = oldHook }()

	// 0. Rejects nil config, empty URL, and non-PostgreSQL schemes in New()
	if _, err := New(nil); err == nil {
		t.Errorf("expected error for nil config in New, got nil")
	}
	cfgEmptyURL := config.NewFromData(&config.ConfigData{DatabaseURL: ""})
	if _, err := New(cfgEmptyURL); err == nil {
		t.Errorf("expected error for empty DatabaseURL in New, got nil")
	}
	for _, invalidScheme := range []string{":memory:", "sqlite://aerial.db", "file:aerial.db?mode=memory", "/var/data/aerial.db"} {
		cfgInvalidScheme := config.NewFromData(&config.ConfigData{DatabaseURL: invalidScheme})
		if _, err := New(cfgInvalidScheme); err == nil {
			t.Errorf("expected error for non-postgres scheme %q in New, got nil", invalidScheme)
		} else if !strings.Contains(err.Error(), "only postgres:// or postgresql:// supported") {
			t.Errorf("unexpected error for %q: %v", invalidScheme, err)
		}
	}

	// 1. Rejects invalid PostgreSQL connection string syntax via pgx.ParseConfig
	cfgBadSyntax := config.NewFromData(&config.ConfigData{DatabaseURL: "postgres://invalid user@localhost:5432/aerial"})
	_, errBadSyntax := New(cfgBadSyntax)
	if errBadSyntax == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(errBadSyntax.Error(), "invalid postgres connection string") {
		t.Errorf("expected invalid postgres connection string error, got: %v", errBadSyntax)
	}

	// 2. Unreachable PostgreSQL host fails connection retry loop
	origAttempts := postgresMaxAttempts
	origBase := postgresRetryBase
	postgresMaxAttempts = 1
	postgresRetryBase = 1 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origAttempts
		postgresRetryBase = origBase
	}()

	cfgUnreachable := config.NewFromData(&config.ConfigData{DatabaseURL: "postgres://user:pass@127.0.0.1:54399/nonexistent?sslmode=disable&connect_timeout=1"})
	_, errUnreachable := New(cfgUnreachable)
	if errUnreachable == nil {
		t.Fatalf("expected error connecting to unreachable PostgreSQL host, got nil")
	}
	if !strings.Contains(errUnreachable.Error(), "could not connect to PostgreSQL after retries") {
		t.Errorf("expected connection failure error, got: %v", errUnreachable)
	}

	// 3. Invalid url percent-encoding fails pgx.ParseConfig
	if _, err := initDB("postgres://%zz"); err == nil {
		t.Errorf("expected error parsing invalid postgres url, got nil")
	}
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared&_busy_timeout=5000", t.Name(), time.Now().UnixNano())
	database, err := initTestSQLite(dsn)
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
	_, err := database.Exec("SELECT id, row_id, thread_id, guild_id, author_id, author_name, content, status, retry_count, restart_count, error_message, response_text, created_at, updated_at FROM messages LIMIT 0")
	if err != nil {
		t.Fatalf("Messages table schema error: %v", err)
	}

	// Verify foreign key pragma is active
	var fkEnabled int
	if err := database.QueryRow("PRAGMA foreign_keys;").Scan(&fkEnabled); err != nil {
		t.Fatalf("Failed to query foreign_keys pragma: %v", err)
	}
	if fkEnabled != 1 {
		t.Errorf("Expected foreign_keys = 1, got %d", fkEnabled)
	}

	// Test initSchemaSQLite on closed DB returns error
	tmpPath := filepath.Join(t.TempDir(), "closed-mig.db")
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	_ = closedDB.Close()
	if err := initSchemaSQLite(closedDB); err == nil {
		t.Error("expected error calling initSchemaSQLite on closed db")
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

func TestMessageCRUD(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	// 1. Insert message with empty status defaulting to PENDING
	msg := Message{
		ID:         "msg-crud-1",
		ThreadID:   "th-crud-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "Alice",
		Content:    "Hello Aerial",
		Status:     "", // Defaults to PENDING
	}
	if err := InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	fetched, err := GetMessage(database, "msg-crud-1")
	if err != nil || fetched == nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if fetched.Status != StatusPending {
		t.Errorf("expected default status PENDING, got %q", fetched.Status)
	}

	// 2. Update status to PROCESSING
	if err := UpdateMessageStatus(database, "msg-crud-1", StatusProcessing, "worker-1"); err != nil {
		t.Fatalf("UpdateMessageStatus failed: %v", err)
	}
	fetched, _ = GetMessage(database, "msg-crud-1")
	if fetched.Status != StatusProcessing {
		t.Errorf("expected status PROCESSING, got %q", fetched.Status)
	}

	// 3. Increment retry
	if err := IncrementMessageRetry(database, "msg-crud-1", "transient error"); err != nil {
		t.Fatalf("IncrementMessageRetry failed: %v", err)
	}
	fetched, _ = GetMessage(database, "msg-crud-1")
	if fetched.RetryCount != 1 || fetched.ErrorMessage != "transient error" {
		t.Errorf("expected retry_count=1, got %d, err=%q", fetched.RetryCount, fetched.ErrorMessage)
	}

	// 4. Reset to pending with restart (requires status=PROCESSING)
	if err := ResetMessageToPendingWithRestart(database, "msg-crud-1", "restarted"); err != nil {
		t.Fatalf("ResetMessageToPendingWithRestart failed: %v", err)
	}
	fetched, _ = GetMessage(database, "msg-crud-1")
	if fetched.Status != StatusPending || fetched.RestartCount != 1 {
		t.Errorf("expected status=PENDING, restart_count=1, got %q, %d", fetched.Status, fetched.RestartCount)
	}

	// 6. MessageExists and ClaimPendingMessage
	exists, err := MessageExists(database, "msg-crud-1")
	if err != nil || !exists {
		t.Fatalf("MessageExists failed: %v", err)
	}
	claimed, err := ClaimPendingMessage(database, "msg-crud-1")
	if err != nil || !claimed {
		t.Fatalf("ClaimPendingMessage failed: %v", err)
	}
	claimedAgain, err := ClaimPendingMessage(database, "msg-crud-1")
	if err != nil || claimedAgain {
		t.Errorf("expected second claim to return false, got %v, %v", claimedAgain, err)
	}

	// 7. Update completed with responseText
	if err := UpdateMessageCompleted(database, "msg-crud-1", "Processed response"); err != nil {
		t.Fatalf("UpdateMessageCompleted failed: %v", err)
	}
	fetched, _ = GetMessage(database, "msg-crud-1")
	if fetched.Status != StatusCompleted || fetched.ResponseText != "Processed response" {
		t.Errorf("expected completed with response text, got status=%q text=%q", fetched.Status, fetched.ResponseText)
	}

	// 8. Active recent threads, messages, and max rowID
	threadIDs, err := GetActiveRecentThreadIDs(database, time.Hour)
	if err != nil || len(threadIDs) == 0 {
		t.Fatalf("GetActiveRecentThreadIDs failed: %v, len=%d", err, len(threadIDs))
	}
	msgs, err := GetRecentThreadMessages(database, "th-crud-1", 10)
	if err != nil || len(msgs) == 0 {
		t.Fatalf("GetRecentThreadMessages failed: %v, len=%d", err, len(msgs))
	}
	zeroMsgs, err := GetRecentThreadMessages(database, "th-crud-1", 0)
	if err != nil {
		t.Errorf("GetRecentThreadMessages with 0 limit failed: %v", err)
	}
	_ = zeroMsgs

	maxRowID, err := GetMaxMessageRowID(database, "th-crud-1")
	if err != nil || maxRowID <= 0 {
		t.Errorf("GetMaxMessageRowID failed: %v, maxRowID=%d", err, maxRowID)
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

func TestSessionLifecycle_TimestampsAndInfo(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	key := "session-lifecycle-test"
	ctx := context.Background()

	// 1. Initial save creates session with created_at == updated_at
	if err := SaveSessionID(database, key, "sess-1"); err != nil {
		t.Fatalf("SaveSessionID 1 failed: %v", err)
	}

	gotID, err := GetSessionID(database, key)
	if err != nil || gotID != "sess-1" {
		t.Fatalf("GetSessionID 1 failed: got %q, err %v", gotID, err)
	}
	turns, err := GetSessionTurnCount(database, key)
	if err != nil || turns != 0 {
		t.Fatalf("GetSessionTurnCount 1 failed: got %d, err %v", turns, err)
	}
	prevID, err := GetPreviousSessionID(database, key)
	if err != nil || prevID != "" {
		t.Fatalf("GetPreviousSessionID 1 failed: got %q, err %v", prevID, err)
	}

	var firstCreatedAt, firstUpdatedAt time.Time
	var rawCreated, rawUpdated any
	if err := database.QueryRowContext(ctx, "SELECT created_at, updated_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated, &rawUpdated); err != nil {
		t.Fatalf("query timestamps failed: %v", err)
	}
	firstCreatedAt, _ = ParseDBTime(rawCreated)
	firstUpdatedAt, _ = ParseDBTime(rawUpdated)
	if firstCreatedAt.IsZero() || firstUpdatedAt.IsZero() {
		t.Errorf("Expected non-zero timestamps: created=%v, updated=%v", firstCreatedAt, firstUpdatedAt)
	}

	// 2. Mid-session turn increment updates updated_at but leaves created_at untouched
	time.Sleep(10 * time.Millisecond)
	if _, err := IncrementSessionTurnCount(database, key); err != nil {
		t.Fatalf("IncrementSessionTurnCount failed: %v", err)
	}

	turns, err = GetSessionTurnCount(database, key)
	if err != nil || turns != 1 {
		t.Fatalf("Expected turn_count=1, got %d (err: %v)", turns, err)
	}
	if err := database.QueryRowContext(ctx, "SELECT created_at, updated_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated, &rawUpdated); err != nil {
		t.Fatalf("query timestamps 2 failed: %v", err)
	}
	created2, _ := ParseDBTime(rawCreated)
	updated2, _ := ParseDBTime(rawUpdated)
	if !created2.Equal(firstCreatedAt) {
		t.Errorf("created_at was mutated on turn increment: before=%v, after=%v", firstCreatedAt, created2)
	}
	if !updated2.After(firstUpdatedAt) {
		t.Errorf("updated_at did not advance on turn increment: before=%v, after=%v", firstUpdatedAt, updated2)
	}

	// 3. Re-saving identical session ID does NOT change created_at
	time.Sleep(10 * time.Millisecond)
	if err := SaveSessionID(database, key, "sess-1"); err != nil {
		t.Fatalf("SaveSessionID idempotent failed: %v", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT created_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated); err != nil {
		t.Fatalf("query timestamps 3 failed: %v", err)
	}
	created3, _ := ParseDBTime(rawCreated)
	if !created3.Equal(firstCreatedAt) {
		t.Errorf("created_at mutated on idempotent SaveSessionID: before=%v, after=%v", firstCreatedAt, created3)
	}

	// 4. RotateSessionID updates created_at to now and resets turn_count to 0
	time.Sleep(10 * time.Millisecond)
	if err := RotateSessionID(database, key, ""); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}
	gotID, err = GetSessionID(database, key)
	if err != nil || gotID != "" {
		t.Errorf("Expected empty internal session ID, got %q (err: %v)", gotID, err)
	}
	turns, err = GetSessionTurnCount(database, key)
	if err != nil || turns != 0 {
		t.Errorf("Expected turn_count=0, got %d (err: %v)", turns, err)
	}
	prevAfterRot, err := GetPreviousSessionID(database, key)
	if err != nil || prevAfterRot != "sess-1" {
		t.Errorf("Expected GetPreviousSessionID 'sess-1', got %q (err: %v)", prevAfterRot, err)
	}

	// Redundant rotation does not wipe previous session ID
	if err := RotateSessionID(database, key, ""); err != nil {
		t.Fatalf("Redundant RotateSessionID failed: %v", err)
	}
	prevAfterRedundant, err := GetPreviousSessionID(database, key)
	if err != nil || prevAfterRedundant != "sess-1" {
		t.Errorf("Expected GetPreviousSessionID 'sess-1' after redundant rotation, got %q (err: %v)", prevAfterRedundant, err)
	}

	if err := database.QueryRowContext(ctx, "SELECT created_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated); err != nil {
		t.Fatalf("query timestamps 4 failed: %v", err)
	}
	created4, _ := ParseDBTime(rawCreated)
	if !created4.After(firstCreatedAt) {
		t.Errorf("RotateSessionID did not advance created_at: first=%v, rotated=%v", firstCreatedAt, created4)
	}

	// 5. Saving a new session ID after cold rotation preserves previous_session_id
	time.Sleep(10 * time.Millisecond)
	if err := SaveSessionID(database, key, "sess-fresh-2"); err != nil {
		t.Fatalf("SaveSessionID fresh failed: %v", err)
	}
	gotID, err = GetSessionID(database, key)
	if err != nil || gotID != "sess-fresh-2" {
		t.Errorf("Expected sess-fresh-2, got %s (err: %v)", gotID, err)
	}
	prevID, err = GetPreviousSessionID(database, key)
	if err != nil || prevID != "sess-1" {
		t.Errorf("Expected prevID 'sess-1', got %q (err: %v)", prevID, err)
	}
	if err := database.QueryRowContext(ctx, "SELECT created_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated); err != nil {
		t.Fatalf("query timestamps 5 failed: %v", err)
	}
	created5, _ := ParseDBTime(rawCreated)
	if !created5.After(firstCreatedAt) {
		t.Errorf("Fresh session created_at is not newer than initial: initial=%v, fresh=%v", firstCreatedAt, created5)
	}

	// 6. Rotating again advances previous_session_id to sess-fresh-2
	if err := RotateSessionID(database, key, "sess-fresh-3"); err != nil {
		t.Fatalf("RotateSessionID to sess-fresh-3 failed: %v", err)
	}
	prevAfterRot2, err := GetPreviousSessionID(database, key)
	if err != nil || prevAfterRot2 != "sess-fresh-2" {
		t.Errorf("Expected GetPreviousSessionID 'sess-fresh-2' after 2nd rotation, got %q (err: %v)", prevAfterRot2, err)
	}

	// 7. Verify FindUnrotatedSessions behavior for this thread
	for i := 0; i < 15; i++ {
		_, _ = IncrementSessionTurnCount(database, key)
	}
	unrotated, err := FindUnrotatedSessions(ctx, database, 10)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions failed: %v", err)
	}
	if len(unrotated) == 0 || unrotated[0].ThreadID != key || unrotated[0].TurnCount != 15 {
		t.Errorf("expected unrotated session for %s with 15 turns, got %+v", key, unrotated)
	}

	// Edge case validation: nil DB and empty thread
	if p, err := GetPreviousSessionID(nil, "key"); p != "" || err != nil {
		t.Errorf("Expected empty string and nil error for nil DB, got %q, %v", p, err)
	}
	if p, err := GetPreviousSessionID(database, ""); p != "" || err != nil {
		t.Errorf("Expected empty string and nil error for empty key, got %q, %v", p, err)
	}
}

func TestParseDBTime(t *testing.T) {
	// 1. nil
	if _, ok := ParseDBTime(nil); ok {
		t.Error("expected false for nil")
	}

	// 2. time.Time
	now := time.Now().UTC()
	if got, ok := ParseDBTime(now); !ok || !got.Equal(now) {
		t.Errorf("expected true and equal time, got %v, %v", got, ok)
	}

	// 3. *time.Time
	var nilTimePtr *time.Time
	if _, ok := ParseDBTime(nilTimePtr); ok {
		t.Error("expected false for nil *time.Time")
	}
	if got, ok := ParseDBTime(&now); !ok || !got.Equal(now) {
		t.Errorf("expected true for valid *time.Time, got %v, %v", got, ok)
	}

	// 4. string (empty and whitespace)
	if _, ok := ParseDBTime(""); ok {
		t.Error("expected false for empty string")
	}
	if _, ok := ParseDBTime("   "); ok {
		t.Error("expected false for whitespace string")
	}

	// 5. string formats
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, l := range layouts {
		str := now.Format(l)
		if _, ok := ParseDBTime(str); !ok {
			t.Errorf("failed to parse layout %q from string %q", l, str)
		}
	}

	// 6. invalid string
	if _, ok := ParseDBTime("not-a-timestamp"); ok {
		t.Error("expected false for invalid timestamp string")
	}

	// 7. []byte
	byteStr := []byte(now.Format(time.RFC3339))
	if _, ok := ParseDBTime(byteStr); !ok {
		t.Error("expected true for valid []byte timestamp")
	}

	// 8. unsupported type
	if _, ok := ParseDBTime(12345); ok {
		t.Error("expected false for integer")
	}
}


func TestFindUnrotatedSessions(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	ctx := context.Background()

	// 1. Nil database returns nil, nil
	nilRes, err := FindUnrotatedSessions(ctx, nil, 8)
	if err != nil || nilRes != nil {
		t.Fatalf("expected nil, nil for nil DB, got res=%v, err=%v", nilRes, err)
	}

	// 2. Insert sessions with varied turn counts
	_ = SaveSessionID(database, "th-low", "sess-low")
	_ = SaveSessionID(database, "th-high-9", "sess-9")
	_ = SaveSessionID(database, "th-high-12", "sess-12")

	// Increment turn counts
	for i := 0; i < 3; i++ {
		_, _ = IncrementSessionTurnCount(database, "th-low")
	}
	for i := 0; i < 9; i++ {
		_, _ = IncrementSessionTurnCount(database, "th-high-9")
	}
	for i := 0; i < 12; i++ {
		_, _ = IncrementSessionTurnCount(database, "th-high-12")
	}

	// 3. Query with minTurns <= 0 (defaults to session.DefaultMaxSessionTurns = 8)
	// Both th-high-12 (12) and th-high-9 (9) should be returned, ordered by turn_count DESC
	unrotated, err := FindUnrotatedSessions(ctx, database, 0)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions failed: %v", err)
	}
	if len(unrotated) != 2 {
		t.Fatalf("expected 2 unrotated sessions (>8), got %d", len(unrotated))
	}
	if unrotated[0].ThreadID != "th-high-12" || unrotated[0].TurnCount != 12 {
		t.Errorf("expected first to be th-high-12 with 12 turns, got %+v", unrotated[0])
	}
	if unrotated[1].ThreadID != "th-high-9" || unrotated[1].TurnCount != 9 {
		t.Errorf("expected second to be th-high-9 with 9 turns, got %+v", unrotated[1])
	}

	// 3b. Verify recency horizon excludes dormant sessions (>2h) by default,
	// but includes them when maxAge <= 0 or extended.
	dormantTime := time.Now().UTC().Add(-3 * time.Hour)
	_, err = database.Exec("INSERT INTO sessions (thread_id, internal_session_id, turn_count, created_at, updated_at) VALUES ('th-old', 'sess-old', 20, $1, $2)", dormantTime, dormantTime)
	if err != nil {
		t.Fatalf("failed to insert dormant session: %v", err)
	}

	// Default 2-hour horizon excludes th-old
	recentOnly, err := FindUnrotatedSessions(ctx, database, 8)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with default maxAge failed: %v", err)
	}
	if len(recentOnly) != 2 {
		t.Fatalf("expected 2 recent sessions, got %d", len(recentOnly))
	}

	// Extended 4-hour horizon includes th-old
	extended, err := FindUnrotatedSessions(ctx, database, 8, 4*time.Hour)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with 4h maxAge failed: %v", err)
	}
	if len(extended) != 3 || extended[0].ThreadID != "th-old" {
		t.Fatalf("expected 3 sessions with th-old first, got %+v", extended)
	}

	// maxAge <= 0 disables age filtering
	allSessions, err := FindUnrotatedSessions(ctx, database, 8, 0)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with disabled maxAge failed: %v", err)
	}
	if len(allSessions) != 3 {
		t.Fatalf("expected 3 sessions with disabled maxAge, got %d", len(allSessions))
	}

	// 4. SQLStore method
	store := NewSQLStore(database)
	storeRes, err := store.FindUnrotatedSessions(ctx, 10)
	if err != nil {
		t.Fatalf("store.FindUnrotatedSessions failed: %v", err)
	}
	// With default 2-hour horizon, th-old is excluded, so only th-high-12 remains
	if len(storeRes) != 1 || storeRes[0].ThreadID != "th-high-12" {
		t.Errorf("expected only th-high-12 for minTurns=10 with default recency, got %+v", storeRes)
	}

	// Nil store error
	var nilStore *SQLStore
	if _, err := nilStore.FindUnrotatedSessions(ctx, 8); err == nil {
		t.Error("expected error for nil SQLStore")
	}

	// Closed DB error
	closedDB, _ := InitDB(":memory:")
	_ = closedDB.Close()
	if _, err := FindUnrotatedSessions(ctx, closedDB, 8); err == nil {
		t.Error("expected error for closed DB")
	}

	// 5. FakeStore implementation
	fake := NewFakeStore()
	defer func() { _ = fake.Close() }()

	_ = fake.SaveSessionID(ctx, "fake-low", "s-low")
	_ = fake.SaveSessionID(ctx, "fake-9", "s-9")
	_ = fake.SaveSessionID(ctx, "fake-15", "s-15")

	for i := 0; i < 5; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "fake-low")
	}
	for i := 0; i < 9; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "fake-9")
	}
	for i := 0; i < 15; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "fake-15")
	}

	fakeUnrotated, err := fake.FindUnrotatedSessions(ctx, 0)
	if err != nil {
		t.Fatalf("fake.FindUnrotatedSessions failed: %v", err)
	}
	if len(fakeUnrotated) != 2 {
		t.Fatalf("expected 2 unrotated sessions in fake store, got %d", len(fakeUnrotated))
	}
	if fakeUnrotated[0].ThreadID != "fake-15" || fakeUnrotated[0].TurnCount != 15 {
		t.Errorf("expected first fake unrotated to be fake-15, got %+v", fakeUnrotated[0])
	}
	if fakeUnrotated[1].ThreadID != "fake-9" || fakeUnrotated[1].TurnCount != 9 {
		t.Errorf("expected second fake unrotated to be fake-9, got %+v", fakeUnrotated[1])
	}

	// Verify FakeStore recency horizon
	fake.SetSessionUpdatedAt("fake-15", time.Now().UTC().Add(-3*time.Hour))
	fakeRecent, err := fake.FindUnrotatedSessions(ctx, 0)
	if err != nil {
		t.Fatalf("fake.FindUnrotatedSessions failed: %v", err)
	}
	if len(fakeRecent) != 1 || fakeRecent[0].ThreadID != "fake-9" {
		t.Errorf("expected only fake-9 with default recency horizon, got %+v", fakeRecent)
	}

	fakeExtended, err := fake.FindUnrotatedSessions(ctx, 0, 4*time.Hour)
	if err != nil {
		t.Fatalf("fake.FindUnrotatedSessions with 4h failed: %v", err)
	}
	if len(fakeExtended) != 2 {
		t.Errorf("expected 2 unrotated sessions with 4h horizon, got %d", len(fakeExtended))
	}

	fakeAll, err := fake.FindUnrotatedSessions(ctx, 0, 0)
	if err != nil {
		t.Fatalf("fake.FindUnrotatedSessions with disabled maxAge failed: %v", err)
	}
	if len(fakeAll) != 2 {
		t.Errorf("expected 2 unrotated sessions with disabled maxAge, got %d", len(fakeAll))
	}

	// Closed fake store error
	closedFake := NewFakeStore()
	_ = closedFake.Close()
	if _, err := closedFake.FindUnrotatedSessions(ctx, 8); err == nil {
		t.Error("expected error for closed FakeStore")
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

func TestGetFactsPaginatedImportanceDescending(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	ctx := context.Background()

	// Insert facts with varying importance, categories, and casing
	_, _ = InsertFactWithContext(ctx, database, false, "work", "Important Work Fact", 2.0, nil)
	_, _ = InsertFactWithContext(ctx, database, false, "work", "Routine work fact", 1.0, nil)
	_, _ = InsertFactWithContext(ctx, database, false, "personal", "Personal hobby fact", 1.5, nil)

	// 1. Importance descending ordering
	res, err := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{Limit: 10})
	if err != nil || len(res.Facts) != 3 {
		t.Fatalf("GetFactsPaginated failed: %v, len=%d", err, len(res.Facts))
	}
	if res.Facts[0].Importance < res.Facts[1].Importance || res.Facts[1].Importance < res.Facts[2].Importance {
		t.Errorf("expected facts ordered by importance DESC, got [%f, %f, %f]", res.Facts[0].Importance, res.Facts[1].Importance, res.Facts[2].Importance)
	}

	// 2. Case-insensitive query filtering
	resQuery, err := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{Query: "WORK"})
	if err != nil || len(resQuery.Facts) != 2 {
		t.Fatalf("case-insensitive query filter failed: %v, len=%d", err, len(resQuery.Facts))
	}

	// 3. Category filtering
	resCat, err := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{Category: "personal"})
	if err != nil || len(resCat.Facts) != 1 || resCat.Facts[0].Category != "personal" {
		t.Fatalf("category filter failed: %v, len=%d", err, len(resCat.Facts))
	}

	// 4. Offset variants (Limit=0, Offset>0, Negative offset clamped to 0)
	resOffset, err := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{Limit: 0, Offset: 1})
	if err != nil || len(resOffset.Facts) != 2 {
		t.Errorf("offset with 0 limit failed: %v, len=%d", err, len(resOffset.Facts))
	}
	resNeg, err := GetFactsPaginatedWithContext(ctx, database, false, FactsFilter{Limit: 10, Offset: -5})
	if err != nil || len(resNeg.Facts) != 3 {
		t.Errorf("negative offset failed: %v, len=%d", err, len(resNeg.Facts))
	}

	// 5. GetFactsPaginatedWithContext isPg=true with Offset>0 (exercises paginationSQL = "OFFSET $1")
	_, _ = GetFactsPaginatedWithContext(ctx, database, true, FactsFilter{Offset: 2})
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

func TestNativeVectorSearchHNSW(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()

	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0

	embInvalid := []float32{0.1, 0.2, 0.3}

	// 1. Fact matching high score
	_, _ = store.InsertFact(ctx, "cat", "High score fact", 1.0, embValid)

	// 2. Fact matching low importance (filtered out by minScore=0.5)
	_, _ = store.InsertFact(ctx, "cat", "Low score fact", 0.001, embValid)

	// 3. Fact with orthogonal embedding (filtered out by minScore=0.5)
	embOrthogonal := make([]float32, ExpectedEmbeddingDim)
	embOrthogonal[1] = 1.0
	_, _ = store.InsertFact(ctx, "cat", "Orthogonal fact", 0.5, embOrthogonal)

	// 4. Fact with invalid embedding dimension
	_, _ = store.InsertFact(ctx, "cat", "Invalid dim fact", 1.0, embInvalid)

	// 5. Fact with nil embedding
	_, _ = store.InsertFact(ctx, "cat", "No embedding fact", 1.0, nil)

	// Vector search execution
	res, err := store.SearchSimilarFacts(ctx, embValid, "", 10, 0.5)
	if err != nil || len(res) != 1 {
		t.Fatalf("SearchSimilarFacts failed: %v, len=%d", err, len(res))
	}
	if res[0].FactText != "High score fact" {
		t.Errorf("expected 'High score fact', got %q", res[0].FactText)
	}

	// Filter branches: matching query
	resFilter, err := store.SearchSimilarFacts(ctx, embValid, "Matching", 10, 0.1)
	if err != nil {
		t.Fatalf("SearchSimilarFacts filter branches failed: %v", err)
	}
	_ = resFilter

	// Edge case: Nil database
	resNilDB, err := SearchSimilarFactsWithContext(ctx, nil, true, embValid, "test", 10, 0.2)
	if err != nil || resNilDB != nil {
		t.Errorf("expected nil, nil for nil DB in SearchSimilarFactsWithContext, got %v, %v", resNilDB, err)
	}

	// Edge case: Empty embedding and empty query
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()
	resEmpty, err := SearchSimilarFactsWithContext(ctx, database, false, nil, "", 10, 0.2)
	if err != nil || resEmpty != nil {
		t.Errorf("expected nil, nil for empty embedding and query, got %v, %v", resEmpty, err)
	}

	// Edge case: Clamped limit and clamped minScore with nil context
	resClamped, err := SearchSimilarFactsWithContext(nil, database, false, nil, "   ", -1, -0.5)
	if err != nil || resClamped != nil {
		t.Errorf("expected nil, nil for clamped query, got %v, %v", resClamped, err)
	}

	// Edge case: isPg=true on SQLite database triggers query error path
	_, errPgOnSqlite := SearchSimilarFactsWithContext(ctx, database, true, embValid, "", 10, 0.2)
	if errPgOnSqlite == nil {
		t.Errorf("expected error running pg vector query on sqlite db, got nil")
	}

	// Edge case: isPg=true with candidateLimit >= 30 using testMockDBTX
	mockCandidate := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return database.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
		},
	}
	candidateFacts, err := SearchSimilarFactsWithContext(ctx, mockCandidate, true, embValid, "", 20, 0.1)
	if err != nil {
		t.Fatalf("SearchSimilarFactsWithContext candidateLimit>=30 failed: %v", err)
	}
	_ = candidateFacts
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

func TestSchedulesAndRunsLifecycle(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	now := time.Now().Truncate(time.Second).UTC()

	// 1. One-shot schedule CRUD
	osSched := OneShotSchedule{
		ID:        "os-1",
		ThreadID:  "th-1",
		Prompt:    "Run once prompt",
		RunAt:     now.Add(-time.Minute),
		CreatedAt: now,
		Effort:    "low",
	}
	if err := CreateOneShotSchedule(database, osSched); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	dueOS, err := GetDueOneShotSchedules(database)
	if err != nil || len(dueOS) != 1 || dueOS[0].ID != "os-1" {
		t.Fatalf("GetDueOneShotSchedules failed: %v, len=%d", err, len(dueOS))
	}

	allOS, err := GetAllOneShotSchedules(database, "th-1")
	if err != nil || len(allOS) != 1 {
		t.Fatalf("GetAllOneShotSchedules failed: %v, len=%d", err, len(allOS))
	}

	// Consume one-shot schedule into message
	osMsg := Message{
		ID:       "msg-os-1",
		ThreadID: "th-1",
		Content:  "From one shot",
		Status:   StatusPending,
	}
	if err := InsertMessageAndConsumeOneShot(database, "os-1", osMsg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}
	if err := InsertMessageAndConsumeOneShot(database, "non-existent-oneshot", osMsg); err == nil {
		t.Errorf("expected error consuming non-existent one-shot, got nil")
	}


	// 2. Cron schedule CRUD
	cronSched := CronSchedule{
		ID:        "cron-1",
		TargetID:  "chan-1",
		CronExpr:  "*/5 * * * *",
		Prompt:    "Recurring prompt",
		NextRunAt: now.Add(-time.Minute),
		Enabled:   true,
		Effort:    "low",
		CreatedAt: now,
	}
	if err := CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}

	dueCron, err := GetDueCronSchedules(database)
	if err != nil || len(dueCron) != 1 || dueCron[0].ID != "cron-1" {
		t.Fatalf("GetDueCronSchedules failed: %v, len=%d", err, len(dueCron))
	}

	allCron, err := GetAllCronSchedules(database, "chan-1")
	if err != nil || len(allCron) != 1 {
		t.Fatalf("GetAllCronSchedules failed: %v, len=%d", err, len(allCron))
	}

	nextTime := now.Add(time.Hour)
	if err := UpdateCronNextRun(database, "cron-1", nextTime); err != nil {
		t.Fatalf("UpdateCronNextRun failed: %v", err)
	}


	// 3. Schedule runs CRUD
	run1 := ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		ThreadID:     "th-1",
		Title:        "Run Title",
		Prompt:       "Run Prompt",
		Status:       "running",
		StartedAt:    now.Add(-2 * time.Hour),
		Effort:       "unrecognized_fallback",
	}
	if err := CreateScheduleRun(database, run1); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	updateParams := UpdateRunParams{
		RunID:       "run-1",
		Status:      "success",
		Error:       "",
		CompletedAt: now,
		DurationMs:  500,
	}
	if err := UpdateScheduleRunStatus(database, updateParams); err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}

	if err := UpdateScheduleRunStatus(database, UpdateRunParams{RunID: "run-1"}); err != nil {
		t.Errorf("expected nil error on empty sets UpdateScheduleRunStatus, got %v", err)
	}

	runs, total, err := GetScheduleRunsPaginated(database, 10, 0, "cron-1", "success")
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("GetScheduleRunsPaginated failed: %v, total=%d, len=%d", err, total, len(runs))
	}
	runsClamped, totalClamped, err := GetScheduleRunsPaginated(database, -5, -10, "", "")
	if err != nil || totalClamped != 1 || len(runsClamped) != 1 {
		t.Errorf("GetScheduleRunsPaginated clamping failed: %v, total=%d", err, totalClamped)
	}

	// 4. Reconcile orphaned runs
	runOrphan := ScheduleRun{
		ID:           "run-orphan",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		Status:       "running",
		StartedAt:    now.Add(-3 * time.Hour),
	}
	_ = CreateScheduleRun(database, runOrphan)
	reconciled, err := ReconcileOrphanedScheduleRuns(database)
	if err != nil || reconciled == 0 {
		t.Fatalf("ReconcileOrphanedScheduleRuns failed: %v, reconciled=%d", err, reconciled)
	}

	// 5. Prune old schedule runs
	pruned, err := PruneScheduleRuns(database, 100, time.Hour)
	if err != nil {
		t.Fatalf("PruneScheduleRuns failed: %v", err)
	}
	_ = pruned

}

func TestGetScheduleSummaryMetrics(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	// 1. Empty database
	metrics, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics on empty db failed: %v", err)
	}
	if metrics.CronCount != 0 || metrics.OneShotCount != 0 || metrics.NextRunAt != nil || metrics.SuccessRate24h != 100.0 {
		t.Errorf("unexpected empty db metrics: %+v", metrics)
	}

	now := time.Now().Truncate(time.Second).UTC()

	// 2. Cron schedule earlier than one-shot schedule
	tCron := now.Add(5 * time.Minute)
	tOneShot := now.Add(10 * time.Minute)
	_, _ = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, enabled, next_run_at, created_at) VALUES ('cs-1', 't1', '*/5 * * * *', 'Prompt', 1, ?, ?)", tCron, now)
	_, _ = database.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ('os-1', 't1', 'Prompt', ?, ?)", tOneShot, now)

	mCronEarlier, err := GetScheduleSummaryMetrics(database)
	if err != nil || mCronEarlier.NextRunAt == nil || !mCronEarlier.NextRunAt.Equal(tCron) {
		t.Errorf("expected NextRunAt = tCron, got %+v (err: %v)", mCronEarlier, err)
	}

	// 3. One-shot schedule earlier than cron schedule
	tOneShotEarlier := now.Add(2 * time.Minute)
	_, _ = database.Exec("UPDATE one_shot_schedules SET run_at = ? WHERE id = 'os-1'", tOneShotEarlier)

	mOneShotEarlier, err := GetScheduleSummaryMetrics(database)
	if err != nil || mOneShotEarlier.NextRunAt == nil || !mOneShotEarlier.NextRunAt.Equal(tOneShotEarlier) {
		t.Errorf("expected NextRunAt = tOneShotEarlier, got %+v (err: %v)", mOneShotEarlier, err)
	}

	// 4. Single schedule type NextRunAt variants (only one-shot or only cron)
	_, _ = database.Exec("DELETE FROM cron_schedules")
	mOnlyOneShot, err := GetScheduleSummaryMetrics(database)
	if err != nil || mOnlyOneShot.NextRunAt == nil || !mOnlyOneShot.NextRunAt.Equal(tOneShotEarlier) {
		t.Errorf("expected NextRunAt for only one-shot, got %+v", mOnlyOneShot)
	}

	_, _ = database.Exec("DELETE FROM one_shot_schedules")
	_, _ = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, enabled, next_run_at, created_at) VALUES ('cs-only', 't1', '*/5 * * * *', 'Prompt', 1, ?, ?)", tCron, now)
	mOnlyCron, err := GetScheduleSummaryMetrics(database)
	if err != nil || mOnlyCron.NextRunAt == nil || !mOnlyCron.NextRunAt.Equal(tCron) {
		t.Errorf("expected NextRunAt for only cron, got %+v", mOnlyCron)
	}

	// 5. 24h success rate calculations (completed runs vs other runs)
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-s1', 'cs-1', 'cron', 't1', 't1', 'p', 'completed', ?)", now.Add(-time.Hour))
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-f1', 'cs-1', 'cron', 't1', 't1', 'p', 'failed', ?)", now.Add(-time.Hour))
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-t1', 'cs-1', 'cron', 't1', 't1', 'p', 'timeout', ?)", now.Add(-time.Hour))
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-c1', 'cs-1', 'cron', 't1', 't1', 'p', 'cancelled', ?)", now.Add(-time.Hour))

	mRates, err := GetScheduleSummaryMetrics(database)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics failed: %v", err)
	}
	if mRates.SuccessRate24h < 24.0 || mRates.SuccessRate24h > 26.0 {
		t.Errorf("expected ~25.0 success rate, got %f", mRates.SuccessRate24h)
	}
}

func TestSessionConversationMapping(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	err := SaveConversationMapping(database, "ext-1", "int-1")
	if err != nil {
		t.Fatalf("SaveConversationMapping failed: %v", err)
	}

	intID, err := GetSessionID(database, "ext-1")
	if err != nil || intID != "int-1" {
		t.Errorf("expected int-1, got %s (err: %v)", intID, err)
	}

	extID, err := GetExternalConversationID(database, "int-1")
	if err != nil || extID != "ext-1" {
		t.Errorf("expected ext-1, got %s (err: %v)", extID, err)
	}
}

func TestFactEmbeddingAndThreadLookup(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	vec := make([]float32, ExpectedEmbeddingDim)
	vec[0] = 0.8
	vec[1] = 0.2

	id, err := InsertFact(database, "category", "Fact with emb", 1.0, vec)
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
	// 1. Multiline Prompt: header
	input1 := "<USER_REQUEST>\nPrompt:\n   Actual prompt content here\n</USER_REQUEST>"
	res1 := CleanTaskSummary(input1)
	if res1 != "Actual prompt content here" {
		t.Errorf("expected 'Actual prompt content here', got %q", res1)
	}

	// 2. Same-line Prompt: header
	input2 := "<USER_REQUEST>\nPrompt: Clean inline prompt\n</USER_REQUEST>"
	res2 := CleanTaskSummary(input2)
	if res2 != "Clean inline prompt" {
		t.Errorf("expected 'Clean inline prompt', got %q", res2)
	}

	// 3. Bullet content format
	input3 := "<USER_REQUEST>\n- content: Bullet content format\n</USER_REQUEST>"
	res3 := CleanTaskSummary(input3)
	if res3 != "Bullet content format" {
		t.Errorf("expected 'Bullet content format', got %q", res3)
	}

	// 4. Tag mention fallback
	input4 := "<tag>@mention</tag>"
	res4 := CleanTaskSummary(input4)
	if res4 != "Agent Task" {
		t.Errorf("expected 'Agent Task', got %q", res4)
	}

	// 5. Random XML tag fallback
	input5 := "<random_tag></random_tag>"
	res5 := CleanTaskSummary(input5)
	if res5 != "Agent Task" {
		t.Errorf("expected 'Agent Task', got %q", res5)
	}

	// 6. Whitespace fallback
	input6 := "   "
	res6 := CleanTaskSummary(input6)
	if res6 != "Agent Task" {
		t.Errorf("expected 'Agent Task', got %q", res6)
	}

	// 7. Long prompt truncation
	input7 := "This is a very long prompt string " + strings.Repeat("with lots of extra words ", 10)
	res7 := CleanTaskSummary(input7)
	if !strings.HasSuffix(res7, "...") {
		t.Errorf("expected truncated string with ..., got %q", res7)
	}

	// 8. GetActiveTasks with empty message summary (auto-cleaned)
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	_, err := database.Exec("INSERT INTO messages (id, thread_id, content, summary, status, created_at, updated_at) VALUES ('m-empty-summary', 't-task', 'Prompt from task content', '', 'PENDING', datetime('now'), datetime('now'))")
	if err != nil {
		t.Fatalf("failed to insert message with empty summary: %v", err)
	}
	tasks, err := GetActiveTasks(database)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	var foundTask bool
	for _, task := range tasks {
		if task.ID == "m-empty-summary" {
			foundTask = true
			if task.Summary != "Prompt from task content" {
				t.Errorf("expected auto-cleaned summary 'Prompt from task content', got %q", task.Summary)
			}
		}
	}
	if !foundTask {
		t.Errorf("expected to find task m-empty-summary in active tasks")
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

func TestVectorHelpersEdgeCases(t *testing.T) {
	// cosineSimilarity zero vectors
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

	// Nil source
	if err := nv.Scan(nil); err != nil || nv.Valid || nv.Vector != nil {
		t.Errorf("expected nil Vector and Valid=false on nil Scan, got %v, valid=%v", err, nv.Valid)
	}

	// Invalid type
	if err := nv.Scan(12345); err == nil {
		t.Errorf("expected error scanning int into NullVector, got nil")
	}

	// Invalid JSON string
	if err := nv.Scan("[invalid-json"); err == nil {
		t.Errorf("expected error scanning invalid JSON string into NullVector, got nil")
	}

	// Valid string
	if err := nv.Scan("[0.1,0.2,0.3]"); err != nil || !nv.Valid || len(nv.Vector) != 3 {
		t.Errorf("expected valid vector scan, got err=%v, valid=%v, len=%d", err, nv.Valid, len(nv.Vector))
	}

	// Valid byte slice
	if err := nv.Scan([]byte("[0.1,0.2]")); err != nil || !nv.Valid || len(nv.Vector) != 2 {
		t.Errorf("expected valid vector scan from bytes, got err=%v, valid=%v, len=%d", err, nv.Valid, len(nv.Vector))
	}

	// FactTime Scan branches
	var ft FactTime
	if err := ft.Scan(nil); err != nil || ft.Valid {
		t.Errorf("expected Valid=false on nil FactTime scan")
	}
	now := time.Now()
	if err := ft.Scan(now); err != nil || !ft.Valid {
		t.Errorf("expected Valid=true on valid FactTime scan")
	}
	if err := ft.Scan("invalid-date-string"); err == nil {
		t.Errorf("expected error scanning invalid date string into FactTime, got nil")
	}
	var pTime *time.Time = &now
	if err := ft.Scan(pTime); err != nil || !ft.Valid {
		t.Errorf("expected Valid=true on *time.Time scan")
	}
	var nilPTime *time.Time = nil
	if err := ft.Scan(nilPTime); err != nil || ft.Valid {
		t.Errorf("expected Valid=false on nil *time.Time scan")
	}
	if err := ft.Scan(""); err != nil || ft.Valid {
		t.Errorf("expected Valid=false on empty string scan")
	}
	if err := ft.Scan([]byte("")); err != nil || ft.Valid {
		t.Errorf("expected Valid=false on empty bytes scan")
	}
	if err := ft.Scan([]byte(now.Format(time.RFC3339))); err != nil || !ft.Valid {
		t.Errorf("expected Valid=true on bytes scan")
	}
}

func TestDB_NilDatabaseHandling_All(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	// 1. Facts nil database
	if _, err := InsertFact(nil, "cat", "fact", 1.0, nil); err == nil {
		t.Error("expected error calling InsertFact on nil database")
	}
	if _, err := GetFactsPaginated(nil, FactsFilter{}); err == nil {
		t.Error("expected error calling GetFactsPaginated on nil database")
	}
	if res, err := SearchSimilarFacts(nil, nil, "query", 10, 0.5); err != nil || res != nil {
		t.Errorf("expected nil, nil calling SearchSimilarFacts on nil database, got %v, %v", res, err)
	}
	if res, err := GetFactsByThreadWithEmbeddings(nil, "th1"); err != nil || res != nil {
		t.Errorf("expected nil, nil calling GetFactsByThreadWithEmbeddings on nil database, got %v, %v", res, err)
	}
	if err := UpdateFactEmbedding(nil, 1, nil); err == nil {
		t.Error("expected error calling UpdateFactEmbedding on nil database")
	}
	if res, err := GetActiveConversationsForExtraction(nil, 24); err != nil || res != nil {
		t.Errorf("expected nil, nil calling GetActiveConversationsForExtraction on nil database, got %v, %v", res, err)
	}
	if err := UpdateConversationFactWatermark(nil, "th1", 10); err != nil {
		t.Errorf("expected nil error calling UpdateConversationFactWatermark on nil database, got %v", err)
	}
	if err := UpdateConversationFactExtractedAt(nil, "th1"); err != nil {
		t.Errorf("expected nil error calling UpdateConversationFactExtractedAt on nil database, got %v", err)
	}

	// 2. Schedules & Runs nil database
	if err := CreateOneShotSchedule(nil, OneShotSchedule{ID: "os1", ThreadID: "th1"}); err == nil {
		t.Error("expected error calling CreateOneShotSchedule on nil database")
	}
	if res, err := GetDueOneShotSchedules(nil); err != nil || res != nil {
		t.Errorf("expected nil, nil calling GetDueOneShotSchedules on nil database, got %v, %v", res, err)
	}
	if res, err := GetAllOneShotSchedules(nil, "th1"); err != nil || res != nil {
		t.Errorf("expected nil, nil calling GetAllOneShotSchedules on nil database, got %v, %v", res, err)
	}
	if err := CreateCronSchedule(nil, CronSchedule{ID: "cs1", TargetID: "th1"}); err == nil {
		t.Error("expected error calling CreateCronSchedule on nil database")
	}
	if res, err := GetDueCronSchedules(nil); err != nil || res != nil {
		t.Errorf("expected nil, nil calling GetDueCronSchedules on nil database, got %v, %v", res, err)
	}
	if res, err := GetAllCronSchedules(nil, "th1"); err != nil || res != nil {
		t.Errorf("expected nil, nil calling GetAllCronSchedules on nil database, got %v, %v", res, err)
	}
	if err := UpdateCronNextRun(nil, "cs1", time.Now()); err != nil {
		t.Errorf("expected nil error calling UpdateCronNextRun on nil database, got %v", err)
	}
	if err := CreateScheduleRun(nil, ScheduleRun{ID: "r1"}); err == nil {
		t.Error("expected error calling CreateScheduleRun on nil database")
	}
	if err := UpdateScheduleRunStatus(nil, UpdateRunParams{RunID: "r1"}); err == nil {
		t.Error("expected error calling UpdateScheduleRunStatus on nil database")
	}
	if _, _, err := GetScheduleRunsPaginated(nil, 10, 0, "", ""); err == nil {
		t.Error("expected error calling GetScheduleRunsPaginated on nil database")
	}
	if _, err := GetScheduleSummaryMetrics(nil); err == nil {
		t.Error("expected error calling GetScheduleSummaryMetrics on nil database")
	}
	if _, err := ReconcileOrphanedScheduleRuns(nil); err == nil {
		t.Error("expected error calling ReconcileOrphanedScheduleRuns on nil database")
	}
	if _, err := PruneScheduleRuns(nil, 10, time.Hour); err == nil {
		t.Error("expected error calling PruneScheduleRuns on nil database")
	}

	// 3. Messages nil database & empty-ID validation
	if err := InsertMessage(nil, Message{ID: "m1"}); err == nil {
		t.Error("expected error calling InsertMessage on nil database")
	}
	if err := InsertMessage(database, Message{ID: ""}); err == nil {
		t.Error("expected error calling InsertMessage with empty ID")
	}
	if err := UpdateMessageStatus(nil, "m1", StatusCompleted, ""); err == nil {
		t.Error("expected error calling UpdateMessageStatus on nil database")
	}
	if err := UpdateMessageStatus(database, "", StatusCompleted, ""); err == nil {
		t.Error("expected error calling UpdateMessageStatus with empty ID")
	}
	if err := UpdateMessageCompleted(nil, "m1", "resp"); err == nil {
		t.Error("expected error calling UpdateMessageCompleted on nil database")
	}
	if err := UpdateMessageCompleted(database, "", "resp"); err == nil {
		t.Error("expected error calling UpdateMessageCompleted with empty ID")
	}
	if err := IncrementMessageRetry(nil, "m1", "err"); err == nil {
		t.Error("expected error calling IncrementMessageRetry on nil database")
	}
	if err := IncrementMessageRetry(database, "", "err"); err == nil {
		t.Error("expected error calling IncrementMessageRetry with empty ID")
	}
	if err := ResetMessageToPendingWithRestart(nil, "m1", "reason"); err == nil {
		t.Error("expected error calling ResetMessageToPendingWithRestart on nil database")
	}
	if err := ResetMessageToPendingWithRestart(database, "", "reason"); err == nil {
		t.Error("expected error calling ResetMessageToPendingWithRestart with empty ID")
	}
	if m, err := GetMessage(nil, "m1"); err == nil && m != nil {
		t.Error("expected nil/error calling GetMessage on nil database")
	}
	if m, err := GetMessage(database, ""); err != nil || m != nil {
		t.Errorf("expected nil, nil for empty ID in GetMessage, got (%v, %v)", m, err)
	}
	if exists, err := MessageExists(nil, "m1"); err == nil && exists {
		t.Error("expected false/error calling MessageExists on nil database")
	}
	if exists, err := MessageExists(database, ""); err != nil || exists {
		t.Errorf("expected false, nil for empty ID in MessageExists, got (%v, %v)", exists, err)
	}
	if claimed, err := ClaimPendingMessage(nil, "m1"); err != nil || claimed {
		t.Errorf("expected false, nil on ClaimPendingMessage(nil, id), got (%v, %v)", claimed, err)
	}
	if claimed, err := ClaimPendingMessage(database, ""); err != nil || claimed {
		t.Errorf("expected false, nil on ClaimPendingMessage(db, empty), got (%v, %v)", claimed, err)
	}
	if maxID, err := GetMaxMessageRowID(nil, "t1"); err != nil || maxID != 0 {
		t.Errorf("expected 0, nil on GetMaxMessageRowID(nil, t1), got (%d, %v)", maxID, err)
	}
	if maxID, err := GetMaxMessageRowID(database, ""); err != nil || maxID != 0 {
		t.Errorf("expected 0, nil on GetMaxMessageRowID(db, empty), got (%d, %v)", maxID, err)
	}

	// 4. Sessions nil database & empty key validation
	if id, err := GetSessionID(nil, "th1"); err != nil || id != "" {
		t.Errorf("expected empty, nil calling GetSessionID on nil database, got (%q, %v)", id, err)
	}
	if err := SaveSessionID(nil, "th1", "s1"); err != nil {
		t.Errorf("expected nil error calling SaveSessionID on nil database, got %v", err)
	}
	if _, err := IncrementSessionTurnCount(nil, "th1"); err == nil {
		t.Error("expected error calling IncrementSessionTurnCount on nil database")
	}
	if _, err := IncrementSessionTurnCount(database, ""); err == nil {
		t.Error("expected error calling IncrementSessionTurnCount with empty key")
	}
	if cnt, err := GetSessionTurnCount(nil, "th1"); err != nil || cnt != 0 {
		t.Errorf("expected 0, nil calling GetSessionTurnCount on nil database, got (%d, %v)", cnt, err)
	}
	if err := RotateSessionID(nil, "th1", "s2"); err == nil {
		t.Error("expected error calling RotateSessionID on nil database")
	}
	if err := RotateSessionID(database, "", "s2"); err == nil {
		t.Error("expected error calling RotateSessionID with empty key")
	}
	if prev, err := GetPreviousSessionID(nil, "th1"); err != nil || prev != "" {
		t.Errorf("expected empty, nil calling GetPreviousSessionID on nil database, got (%q, %v)", prev, err)
	}

	// 5. Validation on schedule runs and nil context calls
	if err := CreateScheduleRun(database, ScheduleRun{ID: ""}); err == nil {
		t.Error("expected error on empty id in CreateScheduleRun")
	}
	if err := UpdateScheduleRunStatus(database, UpdateRunParams{RunID: ""}); err == nil {
		t.Error("expected error on empty RunID in UpdateScheduleRunStatus")
	}
	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0
	idNilCtx, err := InsertFactWithContext(nil, database, false, "cat", "fact text nil ctx", 1.0, embValid)
	if err != nil || idNilCtx <= 0 {
		t.Fatalf("InsertFactWithContext with nil ctx failed: %v, id=%d", err, idNilCtx)
	}
	resNilCtx, err := GetFactsPaginatedWithContext(nil, database, false, FactsFilter{Limit: 5})
	if err != nil || resNilCtx.Total == 0 {
		t.Fatalf("GetFactsPaginatedWithContext with nil ctx failed: %v", err)
	}
	if _, err := GetActiveTasks(nil); err == nil {
		t.Error("expected error on nil DB in GetActiveTasks")
	}
}

func TestDB_ClosedDatabaseHandling_All(t *testing.T) {
	tmpPath := filepath.Join(t.TempDir(), "closed-all.db")
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	_ = closedDB.Close()

	if _, err := InsertFact(closedDB, "cat", "fact", 1.0, nil); err == nil {
		t.Error("expected error on closed db in InsertFact")
	}
	if _, err := GetFactsPaginated(closedDB, FactsFilter{}); err == nil {
		t.Error("expected error on closed db in GetFactsPaginated")
	}
	if _, err := SearchSimilarFacts(closedDB, nil, "query", 10, 0.5); err == nil {
		t.Error("expected error on closed db in SearchSimilarFacts")
	}
	if _, err := GetFactsByThreadWithEmbeddings(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in GetFactsByThreadWithEmbeddings")
	}
	if err := UpdateFactEmbedding(closedDB, 1, nil); err == nil {
		t.Error("expected error on closed db in UpdateFactEmbedding")
	}
	if _, err := GetActiveConversationsForExtraction(closedDB, 24); err == nil {
		t.Error("expected error on closed db in GetActiveConversationsForExtraction")
	}
	if err := UpdateConversationFactWatermark(closedDB, "th1", 10); err == nil {
		t.Error("expected error on closed db in UpdateConversationFactWatermark")
	}
	if err := UpdateConversationFactExtractedAt(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in UpdateConversationFactExtractedAt")
	}

	if err := CreateOneShotSchedule(closedDB, OneShotSchedule{ID: "os1", ThreadID: "th1"}); err == nil {
		t.Error("expected error on closed db in CreateOneShotSchedule")
	}
	if _, err := GetDueOneShotSchedules(closedDB); err == nil {
		t.Error("expected error on closed db in GetDueOneShotSchedules")
	}
	if _, err := GetAllOneShotSchedules(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in GetAllOneShotSchedules")
	}
	if err := CreateCronSchedule(closedDB, CronSchedule{ID: "cs1", TargetID: "th1"}); err == nil {
		t.Error("expected error on closed db in CreateCronSchedule")
	}
	if _, err := GetDueCronSchedules(closedDB); err == nil {
		t.Error("expected error on closed db in GetDueCronSchedules")
	}
	if _, err := GetAllCronSchedules(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in GetAllCronSchedules")
	}
	if err := UpdateCronNextRun(closedDB, "cs1", time.Now()); err == nil {
		t.Error("expected error on closed db in UpdateCronNextRun")
	}
	if err := CreateScheduleRun(closedDB, ScheduleRun{ID: "r1"}); err == nil {
		t.Error("expected error on closed db in CreateScheduleRun")
	}
	if err := UpdateScheduleRunStatus(closedDB, UpdateRunParams{RunID: "r1", Status: "success"}); err == nil {
		t.Error("expected error on closed db in UpdateScheduleRunStatus")
	}
	if _, _, err := GetScheduleRunsPaginated(closedDB, 10, 0, "", ""); err == nil {
		t.Error("expected error on closed db in GetScheduleRunsPaginated")
	}
	if _, err := GetScheduleSummaryMetrics(closedDB); err == nil {
		t.Error("expected error on closed db in GetScheduleSummaryMetrics")
	}
	if _, err := ReconcileOrphanedScheduleRuns(closedDB); err == nil {
		t.Error("expected error on closed db in ReconcileOrphanedScheduleRuns")
	}
	if _, err := PruneScheduleRuns(closedDB, 10, time.Hour); err == nil {
		t.Error("expected error on closed db in PruneScheduleRuns")
	}

	if err := InsertMessage(closedDB, Message{ID: "m1"}); err == nil {
		t.Error("expected error on closed db in InsertMessage")
	}
	if err := UpdateMessageStatus(closedDB, "m1", StatusCompleted, ""); err == nil {
		t.Error("expected error on closed db in UpdateMessageStatus")
	}
	if err := UpdateMessageCompleted(closedDB, "m1", "resp"); err == nil {
		t.Error("expected error on closed db in UpdateMessageCompleted")
	}
	if err := IncrementMessageRetry(closedDB, "m1", "err"); err == nil {
		t.Error("expected error on closed db in IncrementMessageRetry")
	}
	if err := ResetMessageToPendingWithRestart(closedDB, "m1", "reason"); err == nil {
		t.Error("expected error on closed db in ResetMessageToPendingWithRestart")
	}
	if _, err := GetMessage(closedDB, "m1"); err == nil {
		t.Error("expected error on closed db in GetMessage")
	}
	if _, err := MessageExists(closedDB, "m1"); err == nil {
		t.Error("expected error on closed db in MessageExists")
	}
	if _, err := GetActiveRecentThreadIDs(closedDB, time.Hour); err == nil {
		t.Error("expected error on closed db in GetActiveRecentThreadIDs")
	}
	if _, err := GetRecentThreadMessages(closedDB, "t1", 10); err == nil {
		t.Error("expected error on closed db in GetRecentThreadMessages")
	}
	if _, err := GetMaxMessageRowID(closedDB, "t1"); err == nil {
		t.Error("expected error on closed db in GetMaxMessageRowID")
	}

	if _, err := GetSessionID(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in GetSessionID")
	}
	if err := SaveSessionID(closedDB, "th1", "s1"); err == nil {
		t.Error("expected error on closed db in SaveSessionID")
	}
	if _, err := IncrementSessionTurnCount(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in IncrementSessionTurnCount")
	}
	if _, err := GetSessionTurnCount(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in GetSessionTurnCount")
	}
	if err := RotateSessionID(closedDB, "th1", "s2"); err == nil {
		t.Error("expected error on closed db in RotateSessionID")
	}
	if _, err := GetPreviousSessionID(closedDB, "th1"); err == nil {
		t.Error("expected error on closed db in GetPreviousSessionID")
	}
	if _, _, err := GetThreadSummary(closedDB, "t1"); err == nil {
		t.Error("expected error on closed db in GetThreadSummary")
	}
}

func TestDB_Fact_InsertAndEmbedding_EdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. Empty fact text error
	_, err := InsertFact(database, "cat", "", 1.0, nil)
	if err == nil {
		t.Error("expected error for empty fact text")
	}

	// 2. Defaults for category and importance
	fID, err := InsertFact(database, "", "Default cat and imp", -1.0, nil)
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

func TestCronScheduleAndMessageEffortRouting(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	ctx := context.Background()
	store := NewSQLStore(database)

	// 1. Create CronSchedule with empty effort (should default to "low")
	cron1 := CronSchedule{
		ID:          "cron-low-1",
		TargetID:    "target-chan-1",
		TitlePrefix: "Weather",
		CronExpr:    "0 6 * * *",
		Prompt:      "Morning forecast",
		NextRunAt:   time.Now().UTC().Add(-1 * time.Minute),
		Enabled:     true,
		Effort:      "",
	}
	if err := store.CreateCronSchedule(ctx, cron1); err != nil {
		t.Fatalf("CreateCronSchedule low failed: %v", err)
	}

	// 2. Create CronSchedule with explicit "high" effort
	cron2 := CronSchedule{
		ID:          "cron-high-1",
		TargetID:    "target-chan-1",
		TitlePrefix: "News",
		CronExpr:    "0 8 * * *",
		Prompt:      "Morning news",
		NextRunAt:   time.Now().UTC().Add(-1 * time.Minute),
		Enabled:     true,
		Effort:      "high",
	}
	if err := store.CreateCronSchedule(ctx, cron2); err != nil {
		t.Fatalf("CreateCronSchedule high failed: %v", err)
	}

	// 3. Query due crons and verify effort fields
	dueCrons, err := store.GetDueCronSchedules(ctx)
	if err != nil {
		t.Fatalf("GetDueCronSchedules failed: %v", err)
	}
	if len(dueCrons) != 2 {
		t.Fatalf("expected 2 due crons, got %d", len(dueCrons))
	}
	var foundLow, foundHigh bool
	for _, c := range dueCrons {
		if c.ID == "cron-low-1" && c.Effort == "medium" {
			foundLow = true
		}
		if c.ID == "cron-high-1" && c.Effort == "high" {
			foundHigh = true
		}
	}
	if !foundLow || !foundHigh {
		t.Errorf("expected foundLow=true and foundHigh=true, got foundLow=%v foundHigh=%v", foundLow, foundHigh)
	}

	// 4. Test ScheduleRun with Effort and Model
	runID := "run-test-1"
	run := ScheduleRun{
		ID:           runID,
		ScheduleID:   "cron-low-1",
		ScheduleType: "cron",
		MessageID:    "msg-test-1",
		TargetID:     "target-chan-1",
		ThreadID:     "thread-chan-1",
		Title:        "Weather Run",
		Prompt:       "Morning forecast",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC(),
		Effort:       "low",
		Model:        "gemini-3.7-flash-low",
	}
	if err := store.CreateScheduleRun(ctx, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}
	runs, total, err := store.GetScheduleRunsPaginated(ctx, 10, 0, "cron-low-1", "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated failed: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d (total %d)", len(runs), total)
	}
	if runs[0].Effort != "low" || runs[0].Model != "gemini-3.7-flash-low" {
		t.Errorf("expected effort='low' and model='gemini-3.7-flash-low', got effort=%q model=%q", runs[0].Effort, runs[0].Model)
	}

	// 6. Test UpdateScheduleRunStatus with Model
	now := time.Now().UTC()
	if err := store.UpdateScheduleRunStatus(ctx, UpdateRunParams{
		RunID:       runID,
		Status:      "completed",
		CompletedAt: now,
		DurationMs:  1234,
		Model:       "gemini-3.8-flash-low",
	}); err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}
	runsAfter, _, err := store.GetScheduleRunsPaginated(ctx, 10, 0, "cron-low-1", "completed")
	if err != nil || len(runsAfter) == 0 {
		t.Fatalf("failed fetching completed runs: %v", err)
	}
	if runsAfter[0].Model != "gemini-3.8-flash-low" {
		t.Errorf("expected updated model='gemini-3.8-flash-low', got %q", runsAfter[0].Model)
	}

	// 7. Test Message with Effort
	msg := Message{
		ID:         "msg-test-effort-1",
		ThreadID:   "thread-chan-1",
		Content:    "Run weather",
		Status:     StatusPending,
		Effort:     "low",
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	fetchedMsg, err := store.GetMessage(ctx, "msg-test-effort-1")
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if fetchedMsg == nil || fetchedMsg.Effort != "low" {
		t.Errorf("expected message effort='low', got %v", fetchedMsg)
	}

	// 8. Test GetRecentThreadMessages with Effort
	recent, err := store.GetRecentThreadMessages(ctx, "thread-chan-1", 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages failed: %v", err)
	}
	if len(recent) != 1 || recent[0].Effort != "low" {
		t.Errorf("expected 1 recent message with effort='low', got %v", recent)
	}
}

type dummyTestCloser struct {
	err error
}

func (d *dummyTestCloser) Close() error {
	return d.err
}

func TestDB_CloseWarnAndRollbackWarn(t *testing.T) {
	// 1. closeWarn with nil closer (no-op)
	closeWarn(nil, "nil-closer")

	// 2. closeWarn with successful closer
	closeWarn(&dummyTestCloser{err: nil}, "ok-closer")

	// 3. closeWarn with error closer (logs warning)
	closeWarn(&dummyTestCloser{err: fmt.Errorf("simulated close error")}, "err-closer")

	// 4. rollbackWarn with nil tx (no-op)
	rollbackWarn(nil, "nil-tx")

	// 5. rollbackWarn with active transaction
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	tx1, err := database.Begin()
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	rollbackWarn(tx1, "active-tx")

	// 6. rollbackWarn with already committed transaction (sql.ErrTxDone suppressed)
	tx2, err := database.Begin()
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}
	rollbackWarn(tx2, "committed-tx")
}

func TestThreadSummaryDirectFunctions(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// 1. Nil / empty checks
	if err := SaveThreadSummary(nil, "th1", "sum", "m1"); err != nil {
		t.Errorf("expected nil for nil DB in SaveThreadSummary")
	}
	if err := SaveThreadSummary(database, "", "sum", "m1"); err != nil {
		t.Errorf("expected nil for empty threadID in SaveThreadSummary")
	}

	sum1, msg1, err1 := GetThreadSummary(nil, "th1")
	if err1 != nil || sum1 != "" || msg1 != "" {
		t.Errorf("expected empty strings, nil err for nil DB in GetThreadSummary, got %q, %q, %v", sum1, msg1, err1)
	}
	sum2, msg2, err2 := GetThreadSummary(database, "")
	if err2 != nil || sum2 != "" || msg2 != "" {
		t.Errorf("expected empty strings, nil err for empty threadID in GetThreadSummary, got %q, %q, %v", sum2, msg2, err2)
	}

	// 2. Non-existent thread ID returns empty strings and nil error
	sum3, msg3, err3 := GetThreadSummary(database, "non-existent")
	if err3 != nil || sum3 != "" || msg3 != "" {
		t.Errorf("expected empty, nil for non-existent thread in GetThreadSummary, got %q, %q, %v", sum3, msg3, err3)
	}

	// 3. Save and retrieve thread summary
	if err := SaveThreadSummary(database, "th-sum-1", "First summary", "m-sum-1"); err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}

	sum4, msg4, err4 := GetThreadSummary(database, "th-sum-1")
	if err4 != nil || sum4 != "First summary" || msg4 != "m-sum-1" {
		t.Fatalf("GetThreadSummary failed: sum=%q msg=%q err=%v", sum4, msg4, err4)
	}

	// 4. Update existing thread summary (upsert ON CONFLICT)
	if err := SaveThreadSummary(database, "th-sum-1", "Updated summary", "m-sum-2"); err != nil {
		t.Fatalf("SaveThreadSummary upsert failed: %v", err)
	}

	sum5, msg5, err5 := GetThreadSummary(database, "th-sum-1")
	if err5 != nil || sum5 != "Updated summary" || msg5 != "m-sum-2" {
		t.Fatalf("GetThreadSummary upsert check failed: sum=%q msg=%q err=%v", sum5, msg5, err5)
	}
}
