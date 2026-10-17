package db

import (
	"context"
	"errors"
	"testing"
)

func runPRRegistryTests(t *testing.T, store Store) {
	ctx := context.Background()

	// 1. Validation errors
	badRecords := []PRRecord{
		{Repo: "", PRNumber: 1, Branch: "feat", HeadSHA: "sha1", TargetID: "123"},
		{Repo: "aerial", PRNumber: 0, Branch: "feat", HeadSHA: "sha1", TargetID: "123"},
		{Repo: "aerial", PRNumber: -1, Branch: "feat", HeadSHA: "sha1", TargetID: "123"},
		{Repo: "aerial", PRNumber: 1, Branch: "", HeadSHA: "sha1", TargetID: "123"},
		{Repo: "aerial", PRNumber: 1, Branch: "feat", HeadSHA: "", TargetID: "123"},
		{Repo: "aerial", PRNumber: 1, Branch: "feat", HeadSHA: "sha1", TargetID: ""},
	}
	for i, bad := range badRecords {
		err := store.UpsertPR(ctx, bad)
		if err == nil {
			t.Errorf("[%d] Expected validation error for record %+v, got nil", i, bad)
		}
	}

	// 2. Initial Upsert
	rec := PRRecord{
		Repo:     "aerial",
		PRNumber: 101,
		Branch:   "feat/push-pipeline",
		HeadSHA:  "head1234",
		TargetID: "1555405874565091380",
		Title:    "feat: add push pipeline",
		Metadata: `{"author":"alex"}`,
	}
	if err := store.UpsertPR(ctx, rec); err != nil {
		t.Fatalf("UpsertPR failed: %v", err)
	}

	// 3. GetPRByNumber
	got, err := store.GetPRByNumber(ctx, "aerial", 101)
	if err != nil {
		t.Fatalf("GetPRByNumber failed: %v", err)
	}
	if got.Repo != "azylman/aerial" || got.PRNumber != 101 || got.Branch != "feat/push-pipeline" || got.HeadSHA != "head1234" || got.TargetID != "1555405874565091380" {
		t.Errorf("Unexpected record content: %+v", got)
	}
	if got.Status != "open" {
		t.Errorf("Expected status 'open', got %q", got.Status)
	}

	// Non-existent GetPRByNumber
	_, err = store.GetPRByNumber(ctx, "aerial", 9999)
	if !errors.Is(err, ErrPRNotFound) {
		t.Errorf("Expected ErrPRNotFound, got %v", err)
	}

	// 4. Idempotent Upsert (update head_sha & title)
	rec.HeadSHA = "head5678"
	rec.Title = "feat: add push pipeline updated"
	if err := store.UpsertPR(ctx, rec); err != nil {
		t.Fatalf("UpsertPR update failed: %v", err)
	}
	updated, err := store.GetPRByNumber(ctx, "aerial", 101)
	if err != nil {
		t.Fatalf("GetPRByNumber after update failed: %v", err)
	}
	if updated.HeadSHA != "head5678" || updated.Title != "feat: add push pipeline updated" {
		t.Errorf("Updated fields mismatch: %+v", updated)
	}

	// 5. Preserving deploying/merged/deployed status on UpsertPR
	recDeploying := PRRecord{
		Repo:     "aerial",
		PRNumber: 102,
		Branch:   "feat/deploying",
		HeadSHA:  "head102",
		TargetID: "1555405874565091380",
		Status:   "deploying",
	}
	if err := store.UpsertPR(ctx, recDeploying); err != nil {
		t.Fatalf("UpsertPR on deploying PR failed: %v", err)
	}
	recReupsert := recDeploying
	recReupsert.Status = "open"
	if err := store.UpsertPR(ctx, recReupsert); err != nil {
		t.Fatalf("UpsertPR reupsert failed: %v", err)
	}
	afterReupsert, err := store.GetPRByNumber(ctx, "aerial", 102)
	if err != nil || afterReupsert.Status != "deploying" {
		t.Fatalf("Expected UpsertPR to preserve 'deploying' status, got %+v (err: %v)", afterReupsert, err)
	}

	// 6. UpdatePRStatusMessageID
	if err := store.UpdatePRStatusMessageID(ctx, "aerial", 101, "  msg-widget-777  "); err != nil {
		t.Fatalf("UpdatePRStatusMessageID failed: %v", err)
	}
	withMsgID, err := store.GetPRByNumber(ctx, "aerial", 101)
	if err != nil || withMsgID.StatusMessageID != "msg-widget-777" {
		t.Fatalf("Expected status_message_id 'msg-widget-777', got %+v (err: %v)", withMsgID, err)
	}
	err = store.UpdatePRStatusMessageID(ctx, "aerial", 9999, "msg-widget-999")
	if !errors.Is(err, ErrPRNotFound) {
		t.Errorf("Expected ErrPRNotFound on UpdatePRStatusMessageID for non-existent PR, got %v", err)
	}
	if err := store.UpdatePRStatusMessageID(ctx, "", 101, "msg-widget-777"); err == nil {
		t.Errorf("Expected validation error on empty repo in UpdatePRStatusMessageID")
	}
}

func TestPRRegistry_SQLStore(t *testing.T) {
	store := NewTestStore(t)
	runPRRegistryTests(t, store)
}

func TestPRRegistry_FakeStore(t *testing.T) {
	store := NewFakeStore()
	runPRRegistryTests(t, store)
}

func TestPRRegistry_FakeStore_Failures(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()

	simErr := errors.New("simulated error")

	// UpsertPR fail
	store.FailNext("UpsertPR", simErr)
	err := store.UpsertPR(ctx, PRRecord{Repo: "a", PRNumber: 1, Branch: "b", HeadSHA: "c", TargetID: "d"})
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// GetPRByNumber fail
	store.FailNext("GetPRByNumber", simErr)
	_, err = store.GetPRByNumber(ctx, "a", 1)
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// UpdatePRStatusMessageID fail
	store.FailNext("UpdatePRStatusMessageID", simErr)
	err = store.UpdatePRStatusMessageID(ctx, "a", 1, "msg-1")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}
}

func TestPRRegistry_SQLStore_NilAndValidationBranches(t *testing.T) {
	ctx := context.Background()
	var nilStore *SQLStore
	storeNilDB := &SQLStore{db: nil}

	stores := []*SQLStore{nilStore, storeNilDB}
	for i, s := range stores {
		if err := s.UpsertPR(ctx, PRRecord{Repo: "aerial", PRNumber: 1, Branch: "b", HeadSHA: "h", TargetID: "t"}); err == nil {
			t.Errorf("[%d] expected error on UpsertPR with nil store/db", i)
		}
		if _, err := s.GetPRByNumber(ctx, "aerial", 1); err == nil {
			t.Errorf("[%d] expected error on GetPRByNumber with nil store/db", i)
		}
		if err := s.UpdatePRStatusMessageID(ctx, "aerial", 1, "msg-1"); err == nil {
			t.Errorf("[%d] expected error on UpdatePRStatusMessageID with nil store/db", i)
		}
	}

	store := NewTestStore(t)
	sqlStore, ok := store.(*SQLStore)
	if !ok {
		t.Fatalf("expected *SQLStore")
	}

	// Empty repo validation
	if _, err := sqlStore.GetPRByNumber(ctx, "", 1); err == nil {
		t.Errorf("expected error on empty repo GetPRByNumber")
	}
	if err := sqlStore.UpdatePRStatusMessageID(ctx, "", 1, "msg-1"); err == nil {
		t.Errorf("expected error on empty repo UpdatePRStatusMessageID")
	}

	// Upsert with explicit status and non-empty metadata
	recExplicit := PRRecord{
		Repo:     "azylman/aerial",
		PRNumber: 202,
		Branch:   "feat/custom",
		HeadSHA:  "headcustom",
		TargetID: "1555405874565091380",
		Status:   "custom_status",
		Metadata: `{"custom":"value"}`,
	}
	if err := sqlStore.UpsertPR(ctx, recExplicit); err != nil {
		t.Fatalf("UpsertPR explicit failed: %v", err)
	}
}

func TestPRRegistry_SQLStore_ClosedDBBranches(t *testing.T) {
	ctx := context.Background()
	tmpPath := t.TempDir() + "/closed_pr.db"
	closedDB, err := initTestSQLite(tmpPath)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()

	rec := PRRecord{
		Repo:     "aerial",
		PRNumber: 1,
		Branch:   "b",
		HeadSHA:  "h",
		TargetID: "t",
	}

	if err := closedStore.UpsertPR(ctx, rec); err == nil {
		t.Errorf("expected error on closed db UpsertPR")
	}
	if _, err := closedStore.GetPRByNumber(ctx, "aerial", 1); err == nil {
		t.Errorf("expected error on closed db GetPRByNumber")
	}
	if err := closedStore.UpdatePRStatusMessageID(ctx, "aerial", 1, "msg-1"); err == nil {
		t.Errorf("expected error on closed db UpdatePRStatusMessageID")
	}
}
