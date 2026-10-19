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

	store := NewSQLStore(database)
	ctx := context.Background()

	msg := Message{
		ID:         "msg-dup-1",
		ThreadID:   "thread-1",
		GuildID:    "guild-1",
		AuthorID:   "author-1",
		AuthorName: "Alice",
		Content:    "Original prompt",
		Status:     StatusProcessing,
	}

	if err := store.InsertMessage(ctx, msg); err != nil {
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
	if err := store.InsertMessage(ctx, dupMsg); err != nil {
		t.Fatalf("Failed to insert duplicate message: %v", err)
	}

	fetched, err := store.GetMessage(ctx, "msg-dup-1")
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

	store := NewSQLStore(database)
	ctx := context.Background()

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
	if err := store.InsertMessage(ctx, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	fetched, err := store.GetMessage(ctx, "msg-crud-1")
	if err != nil || fetched == nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if fetched.Status != StatusPending {
		t.Errorf("expected default status PENDING, got %q", fetched.Status)
	}

	// 2. Update status to PROCESSING
	if err := store.UpdateMessageStatus(ctx, "msg-crud-1", StatusProcessing, "worker-1"); err != nil {
		t.Fatalf("UpdateMessageStatus failed: %v", err)
	}
	fetched, _ = store.GetMessage(ctx, "msg-crud-1")
	if fetched.Status != StatusProcessing {
		t.Errorf("expected status PROCESSING, got %q", fetched.Status)
	}

	// 3. Increment retry
	if err := store.IncrementMessageRetry(ctx, "msg-crud-1", "transient error"); err != nil {
		t.Fatalf("IncrementMessageRetry failed: %v", err)
	}
	fetched, _ = store.GetMessage(ctx, "msg-crud-1")
	if fetched.RetryCount != 1 || fetched.ErrorMessage != "transient error" {
		t.Errorf("expected retry_count=1, got %d, err=%q", fetched.RetryCount, fetched.ErrorMessage)
	}

	// 4. Reset to pending with restart (requires status=PROCESSING)
	if err := store.ResetMessageToPendingWithRestart(ctx, "msg-crud-1", "restarted"); err != nil {
		t.Fatalf("ResetMessageToPendingWithRestart failed: %v", err)
	}
	fetched, _ = store.GetMessage(ctx, "msg-crud-1")
	if fetched.Status != StatusPending || fetched.RestartCount != 1 {
		t.Errorf("expected status=PENDING, restart_count=1, got %q, %d", fetched.Status, fetched.RestartCount)
	}

	// 6. MessageExists and ClaimPendingMessage
	exists, err := store.MessageExists(ctx, "msg-crud-1")
	if err != nil || !exists {
		t.Fatalf("MessageExists failed: %v", err)
	}
	claimed, err := store.ClaimPendingMessage(ctx, "msg-crud-1")
	if err != nil || !claimed {
		t.Fatalf("ClaimPendingMessage failed: %v", err)
	}
	claimedAgain, err := store.ClaimPendingMessage(ctx, "msg-crud-1")
	if err != nil || claimedAgain {
		t.Errorf("expected second claim to return false, got %v, %v", claimedAgain, err)
	}

	// 7. Update completed with responseText
	if err := store.UpdateMessageCompleted(ctx, "msg-crud-1", "Processed response"); err != nil {
		t.Fatalf("UpdateMessageCompleted failed: %v", err)
	}
	fetched, _ = store.GetMessage(ctx, "msg-crud-1")
	if fetched.Status != StatusCompleted || fetched.ResponseText != "Processed response" {
		t.Errorf("expected completed with response text, got status=%q text=%q", fetched.Status, fetched.ResponseText)
	}

	// 8. Active recent threads, messages, and max rowID
	threadIDs, err := store.GetActiveRecentThreadIDs(ctx, time.Hour)
	if err != nil || len(threadIDs) == 0 {
		t.Fatalf("GetActiveRecentThreadIDs failed: %v, len=%d", err, len(threadIDs))
	}
	msgs, err := store.GetRecentThreadMessages(ctx, "th-crud-1", 10)
	if err != nil || len(msgs) == 0 {
		t.Fatalf("GetRecentThreadMessages failed: %v, len=%d", err, len(msgs))
	}
	zeroMsgs, err := store.GetRecentThreadMessages(ctx, "th-crud-1", 0)
	if err != nil {
		t.Errorf("GetRecentThreadMessages with 0 limit failed: %v", err)
	}
	_ = zeroMsgs

	maxRowID, err := store.GetMaxMessageRowID(ctx, "th-crud-1")
	if err != nil || maxRowID <= 0 {
		t.Errorf("GetMaxMessageRowID failed: %v, maxRowID=%d", err, maxRowID)
	}
}

func TestIncrementSessionTurnCountAndRotation(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	store := NewSQLStore(database)
	ctx := context.Background()
	key := "chan-test-1"

	c1, err := store.IncrementSessionTurnCount(ctx, key)
	if err != nil {
		t.Fatalf("IncrementSessionTurnCount 1 failed: %v", err)
	}
	if c1 != 1 {
		t.Errorf("Expected turn_count=1, got %d", c1)
	}

	c2, err := store.IncrementSessionTurnCount(ctx, key)
	if err != nil {
		t.Fatalf("IncrementSessionTurnCount 2 failed: %v", err)
	}
	if c2 != 2 {
		t.Errorf("Expected turn_count=2, got %d", c2)
	}

	// Rotate session ID
	if err := store.RotateSessionID(ctx, key, "new-session-uuid-123"); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}

	cRotated, _ := store.GetSessionTurnCount(ctx, key)
	if cRotated != 0 {
		t.Errorf("Expected turn_count=0 after rotation, got %d", cRotated)
	}

	sessID, _ := store.GetSessionID(ctx, key)
	if sessID != "new-session-uuid-123" {
		t.Errorf("Expected new-session-uuid-123, got %s", sessID)
	}
}

func TestSessionLifecycle_TimestampsAndInfo(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	store := NewSQLStore(database)
	key := "session-lifecycle-test"
	ctx := context.Background()

	// 1. Initial save creates session with created_at == updated_at
	if err := store.SaveSessionID(ctx, key, "sess-1"); err != nil {
		t.Fatalf("SaveSessionID 1 failed: %v", err)
	}

	gotID, err := store.GetSessionID(ctx, key)
	if err != nil || gotID != "sess-1" {
		t.Fatalf("GetSessionID 1 failed: got %q, err %v", gotID, err)
	}
	turns, err := store.GetSessionTurnCount(ctx, key)
	if err != nil || turns != 0 {
		t.Fatalf("GetSessionTurnCount 1 failed: got %d, err %v", turns, err)
	}
	prevID, err := store.GetPreviousSessionID(ctx, key)
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
	// Backdate deterministically with 0ms sleep
	pastTime := time.Now().UTC().Add(-10 * time.Second)
	if _, err := database.ExecContext(ctx, "UPDATE sessions SET created_at = $1, updated_at = $1 WHERE thread_id = $2", pastTime, key); err != nil {
		t.Fatalf("backdate timestamps failed: %v", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT created_at, updated_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated, &rawUpdated); err != nil {
		t.Fatalf("query re-scanned timestamps failed: %v", err)
	}
	firstCreatedAt, _ = ParseDBTime(rawCreated)
	firstUpdatedAt, _ = ParseDBTime(rawUpdated)

	if _, err := store.IncrementSessionTurnCount(ctx, key); err != nil {
		t.Fatalf("IncrementSessionTurnCount failed: %v", err)
	}

	turns, err = store.GetSessionTurnCount(ctx, key)
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
	if err := store.SaveSessionID(ctx, key, "sess-1"); err != nil {
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
	if err := store.RotateSessionID(ctx, key, ""); err != nil {
		t.Fatalf("RotateSessionID failed: %v", err)
	}
	gotID, err = store.GetSessionID(ctx, key)
	if err != nil || gotID != "" {
		t.Errorf("Expected empty internal session ID, got %q (err: %v)", gotID, err)
	}
	turns, err = store.GetSessionTurnCount(ctx, key)
	if err != nil || turns != 0 {
		t.Errorf("Expected turn_count=0, got %d (err: %v)", turns, err)
	}
	prevAfterRot, err := store.GetPreviousSessionID(ctx, key)
	if err != nil || prevAfterRot != "sess-1" {
		t.Errorf("Expected GetPreviousSessionID sess-1, got %q (err: %v)", prevAfterRot, err)
	}

	// Redundant rotation does not wipe previous session ID
	if err := store.RotateSessionID(ctx, key, ""); err != nil {
		t.Fatalf("Redundant RotateSessionID failed: %v", err)
	}
	prevAfterRedundant, err := store.GetPreviousSessionID(ctx, key)
	if err != nil || prevAfterRedundant != "sess-1" {
		t.Errorf("Expected GetPreviousSessionID sess-1 after redundant rotation, got %q (err: %v)", prevAfterRedundant, err)
	}

	if err := database.QueryRowContext(ctx, "SELECT created_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated); err != nil {
		t.Fatalf("query timestamps 4 failed: %v", err)
	}
	created4, _ := ParseDBTime(rawCreated)
	if !created4.After(firstCreatedAt) {
		t.Errorf("RotateSessionID did not advance created_at: first=%v, rotated=%v", firstCreatedAt, created4)
	}

	// 5. Saving a new session ID after cold rotation preserves previous_session_id
	// Backdate deterministically
	_, _ = database.ExecContext(ctx, "UPDATE sessions SET created_at = $1, updated_at = $1 WHERE thread_id = $2", pastTime, key)
	_ = database.QueryRowContext(ctx, "SELECT created_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated)
	firstCreatedAt, _ = ParseDBTime(rawCreated)

	if err := store.SaveSessionID(ctx, key, "sess-fresh-2"); err != nil {
		t.Fatalf("SaveSessionID fresh failed: %v", err)
	}
	gotID, err = store.GetSessionID(ctx, key)
	if err != nil || gotID != "sess-fresh-2" {
		t.Errorf("Expected sess-fresh-2, got %s (err: %v)", gotID, err)
	}
	prevID, err = store.GetPreviousSessionID(ctx, key)
	if err != nil || prevID != "sess-1" {
		t.Errorf("Expected prevID sess-1, got %q (err: %v)", prevID, err)
	}
	if err := database.QueryRowContext(ctx, "SELECT created_at FROM sessions WHERE thread_id = $1", key).Scan(&rawCreated); err != nil {
		t.Fatalf("query timestamps 5 failed: %v", err)
	}
	created5, _ := ParseDBTime(rawCreated)
	if !created5.After(firstCreatedAt) {
		t.Errorf("Fresh session created_at is not newer than initial: initial=%v, fresh=%v", firstCreatedAt, created5)
	}

	// 6. Rotating again advances previous_session_id to sess-fresh-2
	if err := store.RotateSessionID(ctx, key, "sess-fresh-3"); err != nil {
		t.Fatalf("RotateSessionID to sess-fresh-3 failed: %v", err)
	}
	prevAfterRot2, err := store.GetPreviousSessionID(ctx, key)
	if err != nil || prevAfterRot2 != "sess-fresh-2" {
		t.Errorf("Expected GetPreviousSessionID sess-fresh-2 after 2nd rotation, got %q (err: %v)", prevAfterRot2, err)
	}

	// 7. Verify FindUnrotatedSessions behavior for this thread
	for i := 0; i < 15; i++ {
		_, _ = store.IncrementSessionTurnCount(ctx, key)
	}
	unrotated, err := store.FindUnrotatedSessions(ctx, 10)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions failed: %v", err)
	}
	if len(unrotated) == 0 || unrotated[0].ThreadID != key || unrotated[0].TurnCount != 15 {
		t.Errorf("expected unrotated session for %s with 15 turns, got %+v", key, unrotated)
	}

	// Edge case validation: non-existent and empty thread
	if p, err := store.GetPreviousSessionID(ctx, "key"); p != "" || err != nil {
		t.Errorf("Expected empty string and nil error for non-existent key, got %q, %v", p, err)
	}
	if p, err := store.GetPreviousSessionID(ctx, ""); p != "" || err != nil {
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

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Insert sessions with varied turn counts
	_ = store.SaveSessionID(ctx, "th-low", "sess-low")
	_ = store.SaveSessionID(ctx, "th-high-9", "sess-9")
	_ = store.SaveSessionID(ctx, "th-high-12", "sess-12")

	// Increment turn counts
	for i := 0; i < 3; i++ {
		_, _ = store.IncrementSessionTurnCount(ctx, "th-low")
	}
	for i := 0; i < 9; i++ {
		_, _ = store.IncrementSessionTurnCount(ctx, "th-high-9")
	}
	for i := 0; i < 12; i++ {
		_, _ = store.IncrementSessionTurnCount(ctx, "th-high-12")
	}

	// 2. Query with minTurns <= 0 (defaults to session.DefaultMaxSessionTurns = 8)
	unrotated, err := store.FindUnrotatedSessions(ctx, 0)
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

	// 3. Verify recency horizon excludes dormant sessions (>2h) by default,
	// but includes them when maxAge <= 0 or extended.
	dormantTime := time.Now().UTC().Add(-3 * time.Hour)
	_, err = database.Exec("INSERT INTO sessions (thread_id, internal_session_id, turn_count, created_at, updated_at) VALUES ('th-old', 'sess-old', 20, $1, $2)", dormantTime, dormantTime)
	if err != nil {
		t.Fatalf("failed to insert dormant session: %v", err)
	}

	// Default 2-hour horizon excludes th-old
	recentOnly, err := store.FindUnrotatedSessions(ctx, 8)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with default maxAge failed: %v", err)
	}
	if len(recentOnly) != 2 {
		t.Fatalf("expected 2 recent sessions, got %d", len(recentOnly))
	}

	// Extended 4-hour horizon includes th-old
	extended, err := store.FindUnrotatedSessions(ctx, 8, 4*time.Hour)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with 4h maxAge failed: %v", err)
	}
	if len(extended) != 3 {
		t.Fatalf("expected 3 sessions with 4h horizon, got %d", len(extended))
	}

	// Disabled age filter (maxAge <= 0) includes th-old
	allUnrotated, err := store.FindUnrotatedSessions(ctx, 8, -1)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with disabled maxAge failed: %v", err)
	}
	if len(allUnrotated) != 3 {
		t.Fatalf("expected 3 sessions with disabled age filter, got %d", len(allUnrotated))
	}

	// 4. Threshold above all active sessions returns empty slice
	none, err := store.FindUnrotatedSessions(ctx, 50)
	if err != nil {
		t.Fatalf("FindUnrotatedSessions with high threshold failed: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 sessions (>50), got %d", len(none))
	}
}

func TestAtomicInsertMessageAndConsumeOneShot(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	store := NewSQLStore(database)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	oneShot := OneShotSchedule{
		ID:        "oneshot-consume-1",
		ThreadID:  "thread-1",
		Prompt:    "Alert",
		RunAt:     now,
		CreatedAt: now,
	}
	if err := store.CreateOneShotSchedule(ctx, oneShot); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	msg := Message{
		ID:       "msg-consumed-1",
		ThreadID: "thread-1",
		Content:  "Alert",
		Status:   StatusPending,
	}

	if err := store.InsertMessageAndConsumeOneShot(ctx, "oneshot-consume-1", msg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}

	// Ensure one-shot was deleted
	due, _ := store.GetDueOneShotSchedules(ctx)
	if len(due) != 0 {
		t.Errorf("Expected 0 due one-shots after consume, got %d", len(due))
	}

	// Ensure message exists
	m, _ := store.GetMessage(ctx, "msg-consumed-1")
	if m == nil || m.Status != StatusPending {
		t.Errorf("Expected message to exist with status PENDING, got %+v", m)
	}
}

func TestGetFactsPaginatedImportanceDescending(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	ctx := context.Background()
	store := NewSQLStore(database)

	// Insert facts with varying importance, categories, and casing
	_, _ = store.InsertFact(ctx, "work", "Important Work Fact", 2.0, nil)
	_, _ = store.InsertFact(ctx, "work", "Routine work fact", 1.0, nil)
	_, _ = store.InsertFact(ctx, "personal", "Personal hobby fact", 1.5, nil)

	// 1. Importance descending ordering
	res, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil || len(res.Facts) != 3 {
		t.Fatalf("GetFactsPaginated failed: %v, len=%d", err, len(res.Facts))
	}
	if res.Facts[0].Importance < res.Facts[1].Importance || res.Facts[1].Importance < res.Facts[2].Importance {
		t.Errorf("expected facts ordered by importance DESC, got [%f, %f, %f]", res.Facts[0].Importance, res.Facts[1].Importance, res.Facts[2].Importance)
	}

	// 2. Case-insensitive query filtering
	resQuery, err := store.GetFactsPaginated(ctx, FactsFilter{Query: "WORK"})
	if err != nil || len(resQuery.Facts) != 2 {
		t.Fatalf("case-insensitive query filter failed: %v, len=%d", err, len(resQuery.Facts))
	}

	// 3. Category filtering
	resCat, err := store.GetFactsPaginated(ctx, FactsFilter{Category: "personal"})
	if err != nil || len(resCat.Facts) != 1 || resCat.Facts[0].Category != "personal" {
		t.Fatalf("category filter failed: %v, len=%d", err, len(resCat.Facts))
	}

	// 4. Offset variants (Limit=0, Offset>0, Negative offset clamped to 0)
	resOffset, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 0, Offset: 1})
	if err != nil || len(resOffset.Facts) != 2 {
		t.Errorf("offset with 0 limit failed: %v, len=%d", err, len(resOffset.Facts))
	}
	resNeg, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10, Offset: -5})
	if err != nil || len(resNeg.Facts) != 3 {
		t.Errorf("negative offset failed: %v, len=%d", err, len(resNeg.Facts))
	}

	// 5. GetFactsPaginated isPg=true with Offset>0 (exercises paginationSQL = "OFFSET $1")
	_, _ = (&SQLStore{db: database, isPostgres: true}).GetFactsPaginated(ctx, FactsFilter{Offset: 2})
}

func TestFactExtractionWatermarkAndFiltering(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	store := NewSQLStore(database)
	ctx := context.Background()

	msg1 := Message{
		ID:           "msg-wm-1",
		ThreadID:     "thread-watermark",
		Content:      "First turn",
		ResponseText: "First reply",
		Status:       StatusCompleted,
		Metadata:     MessageMetadata{IsAdmin: true},
		CreatedAt:    time.Now().UTC().Add(-2 * time.Hour),
	}
	if err := store.InsertMessage(ctx, msg1); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	// Non-admin turn and ambient silent turn should be excluded
	_ = store.InsertMessage(ctx, Message{
		ID:           "msg-wm-nonadmin",
		ThreadID:     "thread-nonadmin",
		Content:      "Cron turn",
		ResponseText: "Cron output",
		Status:       StatusCompleted,
		Metadata:     MessageMetadata{IsAdmin: false},
		CreatedAt:    time.Now().UTC().Add(-1 * time.Hour),
	})
	_ = store.InsertMessage(ctx, Message{
		ID:           "msg-wm-ambient",
		ThreadID:     "thread-watermark",
		Content:      "Ambient chatter",
		ResponseText: "[AMBIENT_SILENT]",
		Status:       StatusCompleted,
		Metadata:     MessageMetadata{IsAdmin: true},
		CreatedAt:    time.Now().UTC().Add(-1 * time.Hour),
	})

	maxRow1, err := store.GetMaxMessageRowID(ctx, "thread-watermark")
	if err != nil {
		t.Fatalf("GetMaxMessageRowID failed: %v", err)
	}
	if maxRow1 <= 0 {
		t.Fatalf("Expected positive row_id sequence, got %d", maxRow1)
	}

	// Active conversations for extraction (only thread-watermark should qualify)
	convs, err := store.GetActiveConversationsForExtraction(ctx, 24)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(convs) != 1 || convs[0] != "thread-watermark" {
		t.Fatalf("Expected thread-watermark in active extractions, got %+v", convs)
	}

	turns, err := store.GetUnextractedAdminTurns(ctx, "thread-watermark")
	if err != nil || len(turns) != 1 || turns[0].ID != "msg-wm-1" {
		t.Fatalf("Expected 1 unextracted admin turn (msg-wm-1), got %+v (err=%v)", turns, err)
	}
	if emptyTurns, err := store.GetUnextractedAdminTurns(ctx, ""); err != nil || len(emptyTurns) != 0 {
		t.Fatalf("Expected nil turns for empty threadID, got %+v (err=%v)", emptyTurns, err)
	}

	// Advance watermark
	if err := store.UpdateConversationFactWatermark(ctx, "thread-watermark", maxRow1); err != nil {
		t.Fatalf("UpdateConversationFactWatermark failed: %v", err)
	}

	// Re-query: should now be 0 since watermark matches latest message
	convsAfter, err := store.GetActiveConversationsForExtraction(ctx, 24)
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
	var nilStore *SQLStore
	if _, err := nilStore.SearchSimilarFacts(ctx, embValid, "test", 10, 0.2); err == nil {
		t.Errorf("expected error for nil store in SearchSimilarFacts")
	}

	// Edge case: Empty embedding and empty query
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()
	storeDB := NewSQLStore(database)
	resEmpty, err := storeDB.SearchSimilarFacts(ctx, nil, "", 10, 0.2)
	if err != nil || resEmpty != nil {
		t.Errorf("expected nil, nil for empty embedding and query, got %v, %v", resEmpty, err)
	}

	// Edge case: Clamped limit and clamped minScore with nil context
	resClamped, err := storeDB.SearchSimilarFacts(nil, nil, "   ", -1, -0.5)
	if err != nil || resClamped != nil {
		t.Errorf("expected nil, nil for clamped query, got %v, %v", resClamped, err)
	}

	// Edge case: isPg=true on SQLite database triggers query error path
	_, errPgOnSqlite := (&SQLStore{db: database, isPostgres: true}).SearchSimilarFacts(ctx, embValid, "", 10, 0.2)
	if errPgOnSqlite == nil {
		t.Errorf("expected error running pg vector query on sqlite db, got nil")
	}

	// Edge case: isPg=true with candidateLimit >= 30 using testMockDBTX
	mockCandidate := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return database.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
		},
	}
	candidateFacts, err := (&SQLStore{db: mockCandidate, isPostgres: true}).SearchSimilarFacts(ctx, embValid, "", 20, 0.1)
	if err != nil {
		t.Fatalf("SearchSimilarFacts candidateLimit>=30 failed: %v", err)
	}
	_ = candidateFacts
}

func TestRecentThreadMessagesAndActiveIDs(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	store := NewSQLStore(database)
	ctx := context.Background()

	now := time.Now().UTC()
	_ = store.InsertMessage(ctx, Message{
		ID: "m-rec-1", ThreadID: "thread-active-rec", GuildID: "g1", AuthorID: "a1", AuthorName: "Alex",
		Content: "Message 1", Status: StatusCompleted, CreatedAt: now.Add(-10 * time.Minute), UpdatedAt: now.Add(-10 * time.Minute),
	})
	_ = store.InsertMessage(ctx, Message{
		ID: "m-rec-2", ThreadID: "thread-active-rec", GuildID: "g1", AuthorID: "a1", AuthorName: "Alex",
		Content: "Message 2", Status: StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})

	ids, err := store.GetActiveRecentThreadIDs(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("GetActiveRecentThreadIDs failed: %v", err)
	}
	if len(ids) == 0 || ids[0] != "thread-active-rec" {
		t.Errorf("expected thread-active-rec in active IDs, got %v", ids)
	}

	msgs, err := store.GetRecentThreadMessages(ctx, "thread-active-rec", 10)
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

	store := NewSQLStore(database)
	ctx := context.Background()

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
	if err := store.CreateOneShotSchedule(ctx, osSched); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	dueOS, err := store.GetDueOneShotSchedules(ctx)
	if err != nil || len(dueOS) != 1 || dueOS[0].ID != "os-1" {
		t.Fatalf("GetDueOneShotSchedules failed: %v, len=%d", err, len(dueOS))
	}

	allOS, err := store.GetAllOneShotSchedules(ctx, "th-1")
	if err != nil || len(allOS) != 1 {
		t.Fatalf("GetAllOneShotSchedules failed: %v, len=%d", err, len(allOS))
	}

	allOneShotAll, err := store.GetAllOneShotSchedules(ctx, "")
	if err != nil || len(allOneShotAll) != 1 {
		t.Fatalf("GetAllOneShotSchedules with empty threadID failed: %v, len=%d", err, len(allOneShotAll))
	}

	// Consume one-shot schedule into message
	osMsg := Message{
		ID:       "msg-os-1",
		ThreadID: "th-1",
		Content:  "From one shot",
		Status:   StatusPending,
	}
	if err := store.InsertMessageAndConsumeOneShot(ctx, "os-1", osMsg); err != nil {
		t.Fatalf("InsertMessageAndConsumeOneShot failed: %v", err)
	}
	if err := store.InsertMessageAndConsumeOneShot(ctx, "non-existent-oneshot", osMsg); err == nil {
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
	_, err = database.ExecContext(ctx, `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		cronSched.ID, cronSched.TargetID, cronSched.TitlePrefix, cronSched.CronExpr, cronSched.Prompt, "America/Los_Angeles", cronSched.NextRunAt, cronSched.Enabled, cronSched.CreatedAt, cronSched.Effort)
	if err != nil {
		t.Fatalf("failed to insert cron schedule: %v", err)
	}

	dueCron, err := store.GetDueCronSchedules(ctx)
	if err != nil || len(dueCron) != 1 || dueCron[0].ID != "cron-1" {
		t.Fatalf("GetDueCronSchedules failed: %v, len=%d", err, len(dueCron))
	}

	allCron, err := store.GetAllCronSchedules(ctx, "chan-1")
	if err != nil || len(allCron) != 1 {
		t.Fatalf("GetAllCronSchedules failed: %v, len=%d", err, len(allCron))
	}

	allCronAll, err := store.GetAllCronSchedules(ctx, "")
	if err != nil || len(allCronAll) != 1 {
		t.Fatalf("GetAllCronSchedules with empty targetID failed: %v, len=%d", err, len(allCronAll))
	}

	nextTime := now.Add(time.Hour)
	if err := store.UpdateCronNextRun(ctx, "cron-1", nextTime); err != nil {
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
	if err := store.CreateScheduleRun(ctx, run1); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	updateParams := UpdateRunParams{
		RunID:       "run-1",
		Status:      "failed",
		Error:       "something broke",
		Model:       "gpt-4o",
		CompletedAt: now,
		DurationMs:  500,
	}
	if err := store.UpdateScheduleRunStatus(ctx, updateParams); err != nil {
		t.Fatalf("UpdateScheduleRunStatus failed: %v", err)
	}

	if err := store.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "run-1"}); err != nil {
		t.Errorf("expected nil error on empty sets UpdateScheduleRunStatus, got %v", err)
	}

	runs, total, err := store.GetScheduleRunsPaginated(ctx, 10, 0, "cron-1", "failed")
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("GetScheduleRunsPaginated failed: %v, total=%d, len=%d", err, total, len(runs))
	}
	runsClamped, totalClamped, err := store.GetScheduleRunsPaginated(ctx, -5, -10, "", "")
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
	_ = store.CreateScheduleRun(ctx, runOrphan)
	reconciled, err := store.ReconcileOrphanedScheduleRuns(ctx)
	if err != nil || reconciled == 0 {
		t.Fatalf("ReconcileOrphanedScheduleRuns failed: %v, reconciled=%d", err, reconciled)
	}

	// 5. Prune old schedule runs
	pruned, err := store.PruneScheduleRuns(ctx, 100, time.Hour)
	if err != nil {
		t.Fatalf("PruneScheduleRuns failed: %v", err)
	}
	_ = pruned
}

func TestGetScheduleSummaryMetrics(t *testing.T) {
	database := setupTestDB(t)
	defer func() { _ = database.Close() }()

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Empty database
	metrics, err := store.GetScheduleSummaryMetrics(ctx)
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

	mCronEarlier, err := store.GetScheduleSummaryMetrics(ctx)
	if err != nil || mCronEarlier.NextRunAt == nil || !mCronEarlier.NextRunAt.Equal(tCron) {
		t.Errorf("expected NextRunAt = tCron, got %+v (err: %v)", mCronEarlier, err)
	}

	// 3. One-shot schedule earlier than cron schedule
	tOneShotEarlier := now.Add(2 * time.Minute)
	_, _ = database.Exec("UPDATE one_shot_schedules SET run_at = ? WHERE id = 'os-1'", tOneShotEarlier)

	mOneShotEarlier, err := store.GetScheduleSummaryMetrics(ctx)
	if err != nil || mOneShotEarlier.NextRunAt == nil || !mOneShotEarlier.NextRunAt.Equal(tOneShotEarlier) {
		t.Errorf("expected NextRunAt = tOneShotEarlier, got %+v (err: %v)", mOneShotEarlier, err)
	}

	// 4. Single schedule type NextRunAt variants (only one-shot or only cron)
	_, _ = database.Exec("DELETE FROM cron_schedules")
	mOnlyOneShot, err := store.GetScheduleSummaryMetrics(ctx)
	if err != nil || mOnlyOneShot.NextRunAt == nil || !mOnlyOneShot.NextRunAt.Equal(tOneShotEarlier) {
		t.Errorf("expected NextRunAt for only one-shot, got %+v", mOnlyOneShot)
	}

	_, _ = database.Exec("DELETE FROM one_shot_schedules")
	_, _ = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, enabled, next_run_at, created_at) VALUES ('cs-only', 't1', '*/5 * * * *', 'Prompt', 1, ?, ?)", tCron, now)
	mOnlyCron, err := store.GetScheduleSummaryMetrics(ctx)
	if err != nil || mOnlyCron.NextRunAt == nil || !mOnlyCron.NextRunAt.Equal(tCron) {
		t.Errorf("expected NextRunAt for only cron, got %+v", mOnlyCron)
	}

	// 5. 24h success rate calculations (completed runs vs other runs)
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-s1', 'cs-1', 'cron', 't1', 't1', 'p', 'completed', ?)", now.Add(-time.Hour))
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-f1', 'cs-1', 'cron', 't1', 't1', 'p', 'failed', ?)", now.Add(-time.Hour))
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-t1', 'cs-1', 'cron', 't1', 't1', 'p', 'timeout', ?)", now.Add(-time.Hour))
	_, _ = database.Exec("INSERT INTO schedule_runs (id, schedule_id, schedule_type, target_id, thread_id, prompt, status, started_at) VALUES ('r-c1', 'cs-1', 'cron', 't1', 't1', 'p', 'cancelled', ?)", now.Add(-time.Hour))

	mRates, err := store.GetScheduleSummaryMetrics(ctx)
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

	store := NewSQLStore(database)
	ctx := context.Background()

	err := store.SaveSessionID(ctx, "ext-1", "int-1")
	if err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}

	intID, err := store.GetSessionID(ctx, "ext-1")
	if err != nil || intID != "int-1" {
		t.Errorf("expected int-1, got %s (err: %v)", intID, err)
	}

	extID, err := store.GetExternalConversationID(ctx, "int-1")
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

	store := NewSQLStore(database)
	ctx := context.Background()

	id, err := store.InsertFact(ctx, "category", "Fact with emb", 1.0, vec)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	err = store.UpdateFactEmbedding(ctx, id, vec)
	if err != nil {
		t.Fatalf("UpdateFactEmbedding failed: %v", err)
	}

	err = store.UpdateConversationFactExtractedAt(ctx, "thread-lookup-1")
	if err != nil {
		t.Fatalf("UpdateConversationFactExtractedAt failed: %v", err)
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
	store := NewSQLStore(database)
	tasks, err := store.GetActiveTasks(context.Background())
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

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Messages empty-ID validation on store
	if err := store.InsertMessage(ctx, Message{ID: ""}); err == nil {
		t.Error("expected error calling InsertMessage with empty ID")
	}
	if err := store.UpdateMessageStatus(ctx, "", StatusCompleted, ""); err == nil {
		t.Error("expected error calling UpdateMessageStatus with empty ID")
	}
	if err := store.UpdateMessageCompleted(ctx, "", "resp"); err == nil {
		t.Error("expected error calling UpdateMessageCompleted with empty ID")
	}
	if err := store.IncrementMessageRetry(ctx, "", "err"); err == nil {
		t.Error("expected error calling IncrementMessageRetry with empty ID")
	}
	if err := store.ResetMessageToPendingWithRestart(ctx, "", "reason"); err == nil {
		t.Error("expected error calling ResetMessageToPendingWithRestart with empty ID")
	}
	if m, err := store.GetMessage(ctx, ""); err != nil || m != nil {
		t.Errorf("expected nil, nil for empty ID in GetMessage, got (%v, %v)", m, err)
	}
	if exists, err := store.MessageExists(ctx, ""); err != nil || exists {
		t.Errorf("expected false, nil for empty ID in MessageExists, got (%v, %v)", exists, err)
	}
	if claimed, err := store.ClaimPendingMessage(ctx, ""); err != nil || claimed {
		t.Errorf("expected false, nil on ClaimPendingMessage(db, empty), got (%v, %v)", claimed, err)
	}
	if maxID, err := store.GetMaxMessageRowID(ctx, ""); err != nil || maxID != 0 {
		t.Errorf("expected 0, nil on GetMaxMessageRowID(db, empty), got (%d, %v)", maxID, err)
	}

	// 2. Sessions empty key validation on store
	if id, err := store.GetSessionID(ctx, ""); err != nil || id != "" {
		t.Errorf("expected empty, nil calling GetSessionID with empty key, got (%q, %v)", id, err)
	}
	if err := store.SaveSessionID(ctx, "", "s1"); err != nil {
		t.Errorf("expected nil error calling SaveSessionID with empty key, got %v", err)
	}
	if _, err := store.IncrementSessionTurnCount(ctx, ""); err == nil {
		t.Error("expected error calling IncrementSessionTurnCount with empty key")
	}
	if cnt, err := store.GetSessionTurnCount(ctx, ""); err != nil || cnt != 0 {
		t.Errorf("expected 0, nil calling GetSessionTurnCount with empty key, got (%d, %v)", cnt, err)
	}
	if err := store.RotateSessionID(ctx, "", "s2"); err == nil {
		t.Error("expected error calling RotateSessionID with empty key")
	}
	if prev, err := store.GetPreviousSessionID(ctx, ""); err != nil || prev != "" {
		t.Errorf("expected empty, nil calling GetPreviousSessionID with empty key, got (%q, %v)", prev, err)
	}
	if ext, err := store.GetExternalConversationID(ctx, ""); err != nil || ext != "" {
		t.Errorf("expected empty, nil calling GetExternalConversationID with empty key, got (%q, %v)", ext, err)
	}

	// 3. Schedules empty validation on store
	if err := store.CreateScheduleRun(ctx, ScheduleRun{ID: ""}); err == nil {
		t.Error("expected error on empty id in CreateScheduleRun")
	}
	if err := store.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: ""}); err == nil {
		t.Error("expected error on empty RunID in UpdateScheduleRunStatus")
	}
	if err := store.UpdateCronNextRun(ctx, "", time.Now()); err != nil {
		t.Errorf("expected nil error on empty id in UpdateCronNextRun, got %v", err)
	}
	if err := store.UpdateConversationFactWatermark(ctx, "", 1); err != nil {
		t.Errorf("expected nil error on empty threadID in UpdateConversationFactWatermark, got %v", err)
	}

	// 4. Nil context handling
	embValid := make([]float32, ExpectedEmbeddingDim)
	embValid[0] = 1.0
	idNilCtx, err := store.InsertFact(nil, "cat", "fact text nil ctx", 1.0, embValid)
	if err != nil || idNilCtx <= 0 {
		t.Fatalf("InsertFact with nil ctx failed: %v, id=%d", err, idNilCtx)
	}
	resNilCtx, err := store.GetFactsPaginated(nil, FactsFilter{Limit: 5})
	if err != nil || resNilCtx.Total == 0 {
		t.Fatalf("GetFactsPaginated with nil ctx failed: %v", err)
	}
}

func TestDB_ClosedDatabaseHandling_All(t *testing.T) {
	tmpPath := filepath.Join(t.TempDir(), "closed-all.db")
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	_ = closedDB.Close()

	closedStore := NewSQLStore(closedDB)
	ctx := context.Background()

	if _, err := closedStore.InsertFact(ctx, "cat", "fact", 1.0, nil); err == nil {
		t.Error("expected error on closed db in InsertFact")
	}
	if _, err := closedStore.GetFactsPaginated(ctx, FactsFilter{}); err == nil {
		t.Error("expected error on closed db in GetFactsPaginated")
	}
	if _, err := closedStore.SearchSimilarFacts(ctx, nil, "query", 10, 0.5); err == nil {
		t.Error("expected error on closed db in SearchSimilarFacts")
	}
	if err := closedStore.UpdateFactEmbedding(ctx, 1, nil); err == nil {
		t.Error("expected error on closed db in UpdateFactEmbedding")
	}
	if _, err := closedStore.GetActiveConversationsForExtraction(ctx, 24); err == nil {
		t.Error("expected error on closed db in GetActiveConversationsForExtraction")
	}
	if err := closedStore.UpdateConversationFactWatermark(ctx, "th1", 10); err == nil {
		t.Error("expected error on closed db in UpdateConversationFactWatermark")
	}
	if err := closedStore.UpdateConversationFactExtractedAt(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in UpdateConversationFactExtractedAt")
	}

	if err := closedStore.CreateOneShotSchedule(ctx, OneShotSchedule{ID: "os1", ThreadID: "th1"}); err == nil {
		t.Error("expected error on closed db in CreateOneShotSchedule")
	}
	if _, err := closedStore.GetDueOneShotSchedules(ctx); err == nil {
		t.Error("expected error on closed db in GetDueOneShotSchedules")
	}
	if _, err := closedStore.GetAllOneShotSchedules(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetAllOneShotSchedules")
	}
	if _, err := closedStore.GetDueCronSchedules(ctx); err == nil {
		t.Error("expected error on closed db in GetDueCronSchedules")
	}
	if _, err := closedStore.GetAllCronSchedules(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetAllCronSchedules")
	}
	if err := closedStore.UpdateCronNextRun(ctx, "cs1", time.Now()); err == nil {
		t.Error("expected error on closed db in UpdateCronNextRun")
	}
	if err := closedStore.CreateScheduleRun(ctx, ScheduleRun{ID: "r1"}); err == nil {
		t.Error("expected error on closed db in CreateScheduleRun")
	}
	if err := closedStore.UpdateScheduleRunStatus(ctx, UpdateRunParams{RunID: "r1", Status: "success"}); err == nil {
		t.Error("expected error on closed db in UpdateScheduleRunStatus")
	}
	if _, _, err := closedStore.GetScheduleRunsPaginated(ctx, 10, 0, "", ""); err == nil {
		t.Error("expected error on closed db in GetScheduleRunsPaginated")
	}
	if _, err := closedStore.GetScheduleSummaryMetrics(ctx); err == nil {
		t.Error("expected error on closed db in GetScheduleSummaryMetrics")
	}
	if _, err := closedStore.ReconcileOrphanedScheduleRuns(ctx); err == nil {
		t.Error("expected error on closed db in ReconcileOrphanedScheduleRuns")
	}
	if _, err := closedStore.PruneScheduleRuns(ctx, 10, time.Hour); err == nil {
		t.Error("expected error on closed db in PruneScheduleRuns")
	}

	if err := closedStore.InsertMessage(ctx, Message{ID: "m1"}); err == nil {
		t.Error("expected error on closed db in InsertMessage")
	}
	if err := closedStore.UpdateMessageStatus(ctx, "m1", StatusCompleted, ""); err == nil {
		t.Error("expected error on closed db in UpdateMessageStatus")
	}
	if err := closedStore.UpdateMessageCompleted(ctx, "m1", "resp"); err == nil {
		t.Error("expected error on closed db in UpdateMessageCompleted")
	}
	if err := closedStore.IncrementMessageRetry(ctx, "m1", "err"); err == nil {
		t.Error("expected error on closed db in IncrementMessageRetry")
	}
	if err := closedStore.ResetMessageToPendingWithRestart(ctx, "m1", "reason"); err == nil {
		t.Error("expected error on closed db in ResetMessageToPendingWithRestart")
	}
	if _, err := closedStore.GetMessage(ctx, "m1"); err == nil {
		t.Error("expected error on closed db in GetMessage")
	}
	if _, err := closedStore.MessageExists(ctx, "m1"); err == nil {
		t.Error("expected error on closed db in MessageExists")
	}
	if _, err := closedStore.GetActiveRecentThreadIDs(ctx, time.Hour); err == nil {
		t.Error("expected error on closed db in GetActiveRecentThreadIDs")
	}
	if _, err := closedStore.GetRecentThreadMessages(ctx, "t1", 10); err == nil {
		t.Error("expected error on closed db in GetRecentThreadMessages")
	}
	if _, err := closedStore.GetMaxMessageRowID(ctx, "t1"); err == nil {
		t.Error("expected error on closed db in GetMaxMessageRowID")
	}

	if _, err := closedStore.GetSessionID(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetSessionID")
	}
	if err := closedStore.SaveSessionID(ctx, "th1", "s1"); err == nil {
		t.Error("expected error on closed db in SaveSessionID")
	}
	if _, err := closedStore.IncrementSessionTurnCount(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in IncrementSessionTurnCount")
	}
	if _, err := closedStore.GetSessionTurnCount(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetSessionTurnCount")
	}
	if err := closedStore.RotateSessionID(ctx, "th1", "s2"); err == nil {
		t.Error("expected error on closed db in RotateSessionID")
	}
	if _, err := closedStore.GetPreviousSessionID(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetPreviousSessionID")
	}
	if err := closedStore.SaveThreadSummary(ctx, "th1", "sum", "m1"); err == nil {
		t.Error("expected error on closed db in SaveThreadSummary")
	}
	if _, _, err := closedStore.GetThreadSummary(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetThreadSummary")
	}
	if _, err := closedStore.GetSessionActivityStats(ctx, "th1"); err == nil {
		t.Error("expected error on closed db in GetSessionActivityStats")
	}
	if _, err := closedStore.GetExternalConversationID(ctx, "int1"); err == nil {
		t.Error("expected error on closed db in GetExternalConversationID")
	}
	if _, err := closedStore.FindUnrotatedSessions(ctx, 8); err == nil {
		t.Error("expected error on closed db in FindUnrotatedSessions")
	}
	if err := closedStore.InsertMessageAndConsumeOneShot(ctx, "os1", Message{ID: "m1"}); err == nil {
		t.Error("expected error on closed db in InsertMessageAndConsumeOneShot")
	}
	if _, err := closedStore.GetActiveTasks(ctx); err == nil {
		t.Error("expected error on closed db in GetActiveTasks")
	}
}

func TestDB_Fact_InsertAndEmbedding_EdgeCases(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	// 1. Empty fact text error
	_, err := store.InsertFact(ctx, "cat", "", 1.0, nil)
	if err == nil {
		t.Error("expected error for empty fact text")
	}

	// 2. Defaults for category and importance
	fID, err := store.InsertFact(ctx, "", "Default cat and imp", -1.0, nil)
	if err != nil {
		t.Fatalf("failed to insert fact with defaults: %v", err)
	}
	if fID <= 0 {
		t.Errorf("expected fID > 0, got %d", fID)
	}

	// 3. UpdateFactEmbedding dimension error
	if err := store.UpdateFactEmbedding(ctx, fID, []float32{1.0, 2.0}); err == nil {
		t.Error("expected error updating fact embedding with invalid dim")
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
	insertCron := func(c CronSchedule) {
		t.Helper()
		_, err := database.ExecContext(ctx, `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			c.ID, c.TargetID, c.TitlePrefix, c.CronExpr, c.Prompt, "America/Los_Angeles", c.NextRunAt, c.Enabled, time.Now().UTC(), c.Effort)
		if err != nil {
			t.Fatalf("failed to insert cron %s: %v", c.ID, err)
		}
	}
	insertCron(cron1)

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
	insertCron(cron2)

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
		t.Errorf("expected effort=low and model=gemini-3.7-flash-low, got effort=%q model=%q", runs[0].Effort, runs[0].Model)
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
		t.Errorf("expected updated model=gemini-3.8-flash-low, got %q", runsAfter[0].Model)
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
		t.Errorf("expected message effort=low, got %v", fetchedMsg)
	}

	// 8. Test GetRecentThreadMessages with Effort
	recent, err := store.GetRecentThreadMessages(ctx, "thread-chan-1", 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages failed: %v", err)
	}
	if len(recent) != 1 || recent[0].Effort != "low" {
		t.Errorf("expected 1 recent message with effort=low, got %v", recent)
	}

	// 9. Folded from schedules_test.go: Test normalization of untrimmed and table-driven effort values
	normCases := []struct {
		input    string
		expected string
	}{
		{"low", "low"},
		{"medium", "medium"},
		{"high", "high"},
		{"  HIGH  ", "high"},
		{"  medium  ", "medium"},
		{"untrimmed", "medium"},
		{"", "medium"},
		{"unknown-effort", "medium"},
	}
	for _, tc := range normCases {
		if got := normalizeScheduleEffort(tc.input); got != tc.expected {
			t.Errorf("normalizeScheduleEffort(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}

	// 10. Raw SQL insert into one_shot_schedules, cron_schedules, schedule_runs with empty effort COALESCE to medium
	_, err = database.ExecContext(ctx, `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6)`,
		"os-raw-empty", "thread-coalesce", "prompt 1", now.Add(-1*time.Minute), now, "")
	if err != nil {
		t.Fatalf("failed raw insert one_shot_schedules: %v", err)
	}
	dueOS, err := store.GetDueOneShotSchedules(ctx)
	if err != nil {
		t.Fatalf("GetDueOneShotSchedules failed: %v", err)
	}
	var foundOS bool
	for _, s := range dueOS {
		if s.ID == "os-raw-empty" {
			foundOS = true
			if s.Effort != "medium" {
				t.Fatalf("expected effort to COALESCE to medium, got %q", s.Effort)
			}
		}
	}
	if !foundOS {
		t.Fatalf("expected to find os-raw-empty in due one shots")
	}
	allOS, err := store.GetAllOneShotSchedules(ctx, "thread-coalesce")
	if err != nil || len(allOS) == 0 || allOS[0].Effort != "medium" {
		t.Fatalf("expected allOS effort to COALESCE to medium, got %v", allOS)
	}

	_, err = database.ExecContext(ctx, `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"cron-raw-empty", "target-coalesce", "Cron", "0 0 * * *", "daily prompt", "UTC", now.Add(-1*time.Minute), true, now, "")
	if err != nil {
		t.Fatalf("failed raw insert cron_schedules: %v", err)
	}
	dueCronsCoalesce, err := store.GetDueCronSchedules(ctx)
	if err != nil {
		t.Fatalf("GetDueCronSchedules failed: %v", err)
	}
	var foundCron bool
	for _, c := range dueCronsCoalesce {
		if c.ID == "cron-raw-empty" {
			foundCron = true
			if c.Effort != "medium" {
				t.Fatalf("expected cron effort to COALESCE to medium, got %q", c.Effort)
			}
		}
	}
	if !foundCron {
		t.Fatalf("expected to find cron-raw-empty in due crons")
	}
	allCron, err := store.GetAllCronSchedules(ctx, "target-coalesce")
	if err != nil || len(allCron) == 0 || allCron[0].Effort != "medium" {
		t.Fatalf("expected allCron effort to COALESCE to medium, got %v", allCron)
	}

	_, err = database.ExecContext(ctx, `INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, duration_ms, error, effort, model) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		"run-raw-empty", "cron-raw-empty", "cron", "m1", "target-coalesce", "thread-1", "Run", "prompt", "enqueued", now, 0, "", "", "")
	if err != nil {
		t.Fatalf("failed raw insert schedule_runs: %v", err)
	}
	runsCoalesce, totalCoalesce, err := store.GetScheduleRunsPaginated(ctx, 10, 0, "cron-raw-empty", "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated failed: %v", err)
	}
	if totalCoalesce != 1 || len(runsCoalesce) != 1 || runsCoalesce[0].Effort != "medium" {
		t.Fatalf("expected runs[0].Effort to COALESCE to medium, got %q (total=%d)", runsCoalesce[0].Effort, totalCoalesce)
	}

	// 11. Untrimmed "  HIGH  " normalization check
	oneShotUntrimmed := OneShotSchedule{
		ID:       "oneshot-untrimmed-high",
		ThreadID: "thread-3",
		Prompt:   "untrimmed reasoning",
		RunAt:    time.Now().UTC().Add(-1 * time.Minute),
		Effort:   "  HIGH  ",
	}
	if err := store.CreateOneShotSchedule(ctx, oneShotUntrimmed); err != nil {
		t.Fatalf("failed to create untrimmed high one shot: %v", err)
	}
	allUntrimmed, err := store.GetAllOneShotSchedules(ctx, "thread-3")
	if err != nil || len(allUntrimmed) == 0 || allUntrimmed[0].Effort != "high" {
		t.Fatalf("expected canonicalized effort high, got %v", allUntrimmed)
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
