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

	// 4. GetPRByHeadSHA
	byHead, err := store.GetPRByHeadSHA(ctx, "aerial", "head1234")
	if err != nil {
		t.Fatalf("GetPRByHeadSHA failed: %v", err)
	}
	if byHead.PRNumber != 101 {
		t.Errorf("Expected PR 101, got %d", byHead.PRNumber)
	}

	_, err = store.GetPRByHeadSHA(ctx, "aerial", "nonexistent-sha")
	if !errors.Is(err, ErrPRNotFound) {
		t.Errorf("Expected ErrPRNotFound, got %v", err)
	}

	// 5. Idempotent Upsert (update head_sha & title)
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

	// 6. UpdatePRMergeSHA
	if err := store.UpdatePRMergeSHA(ctx, "aerial", 101, "merge9999"); err != nil {
		t.Fatalf("UpdatePRMergeSHA failed: %v", err)
	}
	merged, err := store.GetPRByMergeSHA(ctx, "aerial", "merge9999")
	if err != nil {
		t.Fatalf("GetPRByMergeSHA failed: %v", err)
	}
	if merged.PRNumber != 101 || merged.Status != "merged" || merged.MergeSHA != "merge9999" {
		t.Errorf("Unexpected merged record: %+v", merged)
	}

	err = store.UpdatePRMergeSHA(ctx, "aerial", 9999, "merge0000")
	if !errors.Is(err, ErrPRNotFound) {
		t.Errorf("Expected ErrPRNotFound on UpdatePRMergeSHA for non-existent PR, got %v", err)
	}

	// 7. UpdatePRStatus
	if err := store.UpdatePRStatus(ctx, "aerial", 101, "deploying"); err != nil {
		t.Fatalf("UpdatePRStatus failed: %v", err)
	}
	deploying, err := store.GetPRByNumber(ctx, "aerial", 101)
	if err != nil || deploying.Status != "deploying" {
		t.Fatalf("Expected status 'deploying', got %v (err: %v)", deploying, err)
	}

	err = store.UpdatePRStatus(ctx, "aerial", 9999, "deploying")
	if !errors.Is(err, ErrPRNotFound) {
		t.Errorf("Expected ErrPRNotFound on UpdatePRStatus for non-existent PR, got %v", err)
	}

	// 8. AtomicTransitionPRStatus (CAS gate)
	// Transition from 'deploying' to 'deployed' when status != 'deployed' -> succeeds
	trans1, err := store.AtomicTransitionPRStatus(ctx, "aerial", 101, "deployed", "deployed")
	if err != nil {
		t.Fatalf("AtomicTransitionPRStatus failed: %v", err)
	}
	if trans1 == nil || trans1.Status != "deployed" {
		t.Fatalf("Expected transition to deployed, got %+v", trans1)
	}

	// Attempt duplicate transition when status is already 'deployed' -> returns nil (no-op)
	trans2, err := store.AtomicTransitionPRStatus(ctx, "aerial", 101, "deployed", "deployed")
	if err != nil {
		t.Fatalf("AtomicTransitionPRStatus duplicate failed: %v", err)
	}
	if trans2 != nil {
		t.Errorf("Expected nil on duplicate CAS transition, got %+v", trans2)
	}

	// 9. AtomicTransitionPRStatusByMergeSHA (CAS gate)
	trans3, err := store.AtomicTransitionPRStatusByMergeSHA(ctx, "aerial", "merge9999", "verified", "verified")
	if err != nil {
		t.Fatalf("AtomicTransitionPRStatusByMergeSHA failed: %v", err)
	}
	if trans3 == nil || trans3.Status != "verified" {
		t.Fatalf("Expected transition to verified, got %+v", trans3)
	}

	trans4, err := store.AtomicTransitionPRStatusByMergeSHA(ctx, "aerial", "merge9999", "verified", "verified")
	if err != nil {
		t.Fatalf("AtomicTransitionPRStatusByMergeSHA duplicate failed: %v", err)
	}
	if trans4 != nil {
		t.Errorf("Expected nil on duplicate CAS transition by merge_sha, got %+v", trans4)
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

	// GetPRByHeadSHA fail
	store.FailNext("GetPRByHeadSHA", simErr)
	_, err = store.GetPRByHeadSHA(ctx, "a", "c")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// GetPRByMergeSHA fail
	store.FailNext("GetPRByMergeSHA", simErr)
	_, err = store.GetPRByMergeSHA(ctx, "a", "m")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// UpdatePRMergeSHA fail
	store.FailNext("UpdatePRMergeSHA", simErr)
	err = store.UpdatePRMergeSHA(ctx, "a", 1, "m")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// UpdatePRStatus fail
	store.FailNext("UpdatePRStatus", simErr)
	err = store.UpdatePRStatus(ctx, "a", 1, "s")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// AtomicTransitionPRStatus fail
	store.FailNext("AtomicTransitionPRStatus", simErr)
	_, err = store.AtomicTransitionPRStatus(ctx, "a", 1, "s1", "s2")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}

	// AtomicTransitionPRStatusByMergeSHA fail
	store.FailNext("AtomicTransitionPRStatusByMergeSHA", simErr)
	_, err = store.AtomicTransitionPRStatusByMergeSHA(ctx, "a", "m", "s1", "s2")
	if !errors.Is(err, simErr) {
		t.Errorf("Expected %v, got %v", simErr, err)
	}
}
