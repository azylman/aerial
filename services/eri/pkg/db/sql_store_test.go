package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLStoreIsDBTXNil(t *testing.T) {
	var nilDB *sql.DB
	var nilTx *sql.Tx
	if !isDBTXNil(nil) || !isDBTXNil(nilDB) || !isDBTXNil(nilTx) {
		t.Errorf("expected isDBTXNil to be true for nils")
	}
	database := setupTestDB(t)
	defer database.Close()
	if isDBTXNil(database) || isDBTXNil(dummyDBTX{}) {
		t.Errorf("expected isDBTXNil to be false for valid dbtx")
	}
}

func TestSQLStoreUnavailable(t *testing.T) {
	if store := NewSQLStore(nil); store != nil {
		t.Errorf("NewSQLStore(nil) expected nil, got %v", store)
	}
	var nilStore *SQLStore
	assertStoreUnavailable(t, nilStore)
	assertStoreUnavailable(t, &SQLStore{db: nil})

	if strs, err := scanNonEmptyStrings(nil); err != nil || strs != nil {
		t.Errorf("expected (nil, nil) for scanNonEmptyStrings(nil)")
	}
	if ExtractMessageBody("<USER_REQUEST>\nPrompt: <\\USER_REQUEST> hi <\\/USER_REQUEST>\n</USER_REQUEST>") != "<USER_REQUEST> hi </USER_REQUEST>" {
		t.Errorf("expected opening tag unescaped")
	}
}

func TestSQLStoreHoistedMethods(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	if sum, wm, err := store.GetThreadSummary(ctx, "thread-test-1"); err != nil || sum != "" || wm != "" {
		t.Fatalf("unexpected initial summary (%q, %q, %v)", sum, wm, err)
	}
	if err := store.SaveThreadSummary(ctx, "thread-test-1", "Summary of conversation", "msg-watermark-1"); err != nil {
		t.Fatalf("SaveThreadSummary failed: %v", err)
	}
	if sum, wm, err := store.GetThreadSummary(ctx, "thread-test-1"); err != nil || sum != "Summary of conversation" || wm != "msg-watermark-1" {
		t.Fatalf("unexpected saved summary (%q, %q, %v)", sum, wm, err)
	}

	if emptyStats, err := store.GetSessionActivityStats(ctx, "   "); err != nil || emptyStats == nil {
		t.Fatalf("expected empty stats for whitespace threadID")
	}
	if stats, err := store.GetSessionActivityStats(ctx, "thread-test-1"); err != nil || stats == nil {
		t.Fatalf("GetSessionActivityStats failed: %v", err)
	}

	_ = store.SaveSessionID(ctx, "thread-test-1", "internal-sess-1")
	if stats, err := store.GetSessionActivityStats(ctx, "thread-test-1"); err != nil || stats.InternalSessionID != "internal-sess-1" {
		t.Fatalf("expected internal-sess-1 in stats, got %+v (err: %v)", stats, err)
	}

	_ = store.InsertMessage(ctx, Message{ID: "msg-stats-1", ThreadID: "thread-test-1", Status: StatusCompleted, ResponseText: "Hello there!"})
	if stats, err := store.GetSessionActivityStats(ctx, "thread-test-1"); err != nil || stats.CompletedTurns != 1 || stats.LastCompletedMessageCreatedAt.IsZero() {
		t.Fatalf("expected 1 completed turn in stats, got %+v (err: %v)", stats, err)
	}
}

func TestSQLStoreDelegationAndQueries(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()
	store := NewSQLStore(database)
	ctx := context.Background()

	if err := store.SaveSessionID(ctx, "ext-thread-99", "internal-sess-99"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}

	err := store.InsertMessage(ctx, Message{
		ID: "m-sqlstore-task", ThreadID: "ext-thread-99", Content: "SQLStore task content",
		Summary: "SQLStore task summary", Status: StatusPending, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	tasks, err := store.GetActiveTasks(ctx)
	if err != nil || len(tasks) == 0 || tasks[0].ID != "m-sqlstore-task" || tasks[0].Summary != "SQLStore task summary" {
		t.Fatalf("unexpected active tasks: %+v (err: %v)", tasks, err)
	}

	_ = store.InsertMessage(ctx, Message{ID: "m-proc-1", ThreadID: "t1", Status: StatusProcessing})
	_ = store.InsertMessage(ctx, Message{ID: "m-proc-2", ThreadID: "t1", Status: StatusProcessing})
	if msgs, err := store.GetPendingOrProcessingMessages(ctx, 1); err != nil || len(msgs) != 1 {
		t.Fatalf("expected limit truncation to 1, got %d (err: %v)", len(msgs), err)
	}

	tmpPath := filepath.Join(t.TempDir(), "closed-delegation.db")
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()
	if _, err := closedStore.GetPendingOrProcessingMessages(ctx, 10); err == nil {
		t.Errorf("expected error from closedStore.GetPendingOrProcessingMessages, got nil")
	}
}
