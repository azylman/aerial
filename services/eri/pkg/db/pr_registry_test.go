package db

import (
	"context"
	"errors"
	"testing"
)

func runPRRegistryLifecycle(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()

	for _, bad := range []PRRecord{
		{Repo: "", PRNumber: 1, Branch: "b", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: 0, Branch: "b", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: -1, Branch: "b", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: 1, Branch: "", HeadSHA: "s", TargetID: "t"},
		{Repo: "aerial", PRNumber: 1, Branch: "b", HeadSHA: "", TargetID: "t"},
		{Repo: "aerial", PRNumber: 1, Branch: "b", HeadSHA: "s", TargetID: ""},
	} {
		if err := store.UpsertPR(ctx, bad); err == nil {
			t.Errorf("expected validation error for %+v", bad)
		}
	}

	rec := PRRecord{Repo: "aerial", PRNumber: 101, Branch: "feat/push-pipeline", HeadSHA: "head1234", TargetID: "1555405874565091380", Title: "feat: add push pipeline", Metadata: `{"author":"alex"}`}
	if err := store.UpsertPR(ctx, rec); err != nil {
		t.Fatalf("UpsertPR failed: %v", err)
	}

	got, err := store.GetPRByNumber(ctx, "aerial", 101)
	if err != nil || got.Repo != "azylman/aerial" || got.PRNumber != 101 || got.Branch != "feat/push-pipeline" || got.HeadSHA != "head1234" || got.TargetID != "1555405874565091380" || got.Status != "open" {
		t.Fatalf("unexpected record content: %+v (err: %v)", got, err)
	}
	if _, err := store.GetPRByNumber(ctx, "aerial", 9999); !errors.Is(err, ErrPRNotFound) {
		t.Errorf("expected ErrPRNotFound, got %v", err)
	}

	rec.HeadSHA, rec.Title = "head5678", "feat: add push pipeline updated"
	if err := store.UpsertPR(ctx, rec); err != nil {
		t.Fatalf("UpsertPR update failed: %v", err)
	}
	if updated, err := store.GetPRByNumber(ctx, "aerial", 101); err != nil || updated.HeadSHA != "head5678" || updated.Title != "feat: add push pipeline updated" {
		t.Fatalf("updated record mismatch: %+v (err: %v)", updated, err)
	}

	recDeploying := PRRecord{Repo: "aerial", PRNumber: 102, Branch: "feat/deploying", HeadSHA: "head102", TargetID: "1555405874565091380", Status: "deploying"}
	if err := store.UpsertPR(ctx, recDeploying); err != nil {
		t.Fatalf("UpsertPR deploying failed: %v", err)
	}
	recDeploying.Status = "open"
	if err := store.UpsertPR(ctx, recDeploying); err != nil {
		t.Fatalf("UpsertPR reupsert failed: %v", err)
	}
	if afterReupsert, err := store.GetPRByNumber(ctx, "aerial", 102); err != nil || afterReupsert.Status != "deploying" {
		t.Fatalf("expected deploying status preserved, got %+v (err: %v)", afterReupsert, err)
	}

	if err := store.UpdatePRStatusMessageID(ctx, "aerial", 101, "  msg-widget-777  "); err != nil {
		t.Fatalf("UpdatePRStatusMessageID failed: %v", err)
	}
	if withMsgID, err := store.GetPRByNumber(ctx, "aerial", 101); err != nil || withMsgID.StatusMessageID != "msg-widget-777" {
		t.Fatalf("expected msg-widget-777, got %+v (err: %v)", withMsgID, err)
	}
	if err := store.UpdatePRStatusMessageID(ctx, "aerial", 9999, "msg-widget-999"); !errors.Is(err, ErrPRNotFound) {
		t.Errorf("expected ErrPRNotFound on non-existent PR, got %v", err)
	}
	if err := store.UpdatePRStatusMessageID(ctx, "", 101, "msg-widget-777"); err == nil {
		t.Errorf("expected validation error on empty repo")
	}
}

func TestPRRegistry_StoreParity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store Store
	}{
		{"SQLStore", NewTestStore(t)},
	} {
		t.Run(tc.name, func(t *testing.T) { runPRRegistryLifecycle(t, tc.store) })
	}
}

func TestPRRegistry_SQLStore_NilAndValidationBranches(t *testing.T) {
	ctx := context.Background()
	closedDB, err := initTestSQLite(t.TempDir() + "/closed_pr.db")
	if err != nil {
		t.Fatalf("initTestSQLite failed: %v", err)
	}
	closedStore := NewSQLStore(closedDB)
	_ = closedDB.Close()

	rec := PRRecord{Repo: "aerial", PRNumber: 1, Branch: "b", HeadSHA: "h", TargetID: "t"}
	for _, tc := range []struct {
		name  string
		store *SQLStore
	}{
		{"nil_store", nil},
		{"nil_db", &SQLStore{db: nil}},
		{"closed_db", closedStore.(*SQLStore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{
				tc.store.UpsertPR(ctx, rec),
				c1(tc.store.GetPRByNumber(ctx, "aerial", 1)),
				tc.store.UpdatePRStatusMessageID(ctx, "aerial", 1, "msg-1"),
			} {
				if err == nil {
					t.Errorf("[%s] expected error", tc.name)
				}
			}
		})
	}

	live := NewTestStore(t).(*SQLStore)
	if _, err := live.GetPRByNumber(ctx, "", 1); err == nil {
		t.Error("expected error on empty repo GetPRByNumber")
	}
	if err := live.UpdatePRStatusMessageID(ctx, "", 1, "msg-1"); err == nil {
		t.Error("expected error on empty repo UpdatePRStatusMessageID")
	}
	if err := live.UpsertPR(ctx, PRRecord{Repo: "azylman/aerial", PRNumber: 202, Branch: "feat/custom", HeadSHA: "headcustom", TargetID: "1555405874565091380", Status: "custom_status", Metadata: `{"custom":"value"}`}); err != nil {
		t.Fatalf("UpsertPR explicit failed: %v", err)
	}
	if err := live.SaveThreadSummary(ctx, "", "sum", "m1"); err != nil {
		t.Error("expected nil on empty threadID SaveThreadSummary")
	}
}
