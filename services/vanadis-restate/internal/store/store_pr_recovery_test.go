package store

// PR recovery store tests for vanadis-restate.
//
// Covers recoverFailedPR head acceptance rules, failure injection, baseline
// seeding, and head-update tracking against real Postgres rows in each
// test's private database.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// rcVerified carries the router's verified-head marker.
func rcVerified(verified bool) context.Context {
	return context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, verified)
}

// rcFailNoWrites makes any chunk or build write fail, so a skip that
// attempted a write surfaces as a non-skip error.
var rcFailNoWrites = failStatement("vanadis_chunks", "UPDATE") + failStatement("vanadis_builds", "UPDATE")

// rcOpen seeds build b1 in parent status with chunks and applies sabotage.
func rcOpen(t *testing.T, parent string, chunks []model.VanadisChunk, sabotage string) (*Store, *pgxpool.Pool) {
	t.Helper()
	st, pool := openStore(t)
	seed(t, pool, []model.VanadisBuild{{ID: "b1", ThreadID: "thread-1", Status: parent}}, chunks)
	mustExec(t, pool, sabotage)
	return st, pool
}

// rcOpenStore seeds build b1 in parent status with chunks and applies sabotage, returning only the Store.
func rcOpenStore(t *testing.T, parent string, chunks []model.VanadisChunk, sabotage string) *Store {
	t.Helper()
	st, pool := openStore(t)
	seed(t, pool, []model.VanadisBuild{{ID: "b1", ThreadID: "thread-1", Status: parent}}, chunks)
	mustExec(t, pool, sabotage)
	return st
}

// rcSetStatus moves a stored chunk to status between recovery steps.
func rcSetStatus(t *testing.T, pool *pgxpool.Pool, id, status string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `UPDATE vanadis_chunks SET status = $2 WHERE id = $1`, id, status); err != nil {
		t.Fatalf("set chunk %s status: %v", id, err)
	}
}

func TestPostgresVanadisStore_RecoverFailedPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, status, mode, head string
		pr                               int
		sibling                          string
		skip                             bool
		unverified                       bool
		mergeSHA                         string
		legacySeed                       bool
	}{
		{"single failure", "failed", "failed", "pr", "new", 12, "completed", false, false, "", false},
		{"running parent", "running", "failed", "pr", "new", 12, "completed", false, false, "", false},
		{"same head", "failed", "failed", "pr", "OLD", 12, "completed", true, false, "", true},
		{"action", "failed", "failed", "action", "new", 12, "completed", true, false, "", false},
		{"unattached", "failed", "failed", "pr", "new", 0, "completed", true, false, "", false},
		{"cancelled parent", "cancelled", "failed", "pr", "new", 12, "completed", true, false, "", false},
		{"completed parent", "completed", "failed", "pr", "new", 12, "completed", true, false, "", false},
		{"cancelled chunk", "failed", "cancelled", "pr", "new", 12, "completed", true, false, "", false},
		{"completed chunk", "failed", "completed", "pr", "new", 12, "completed", true, false, "", false},
		{"unverified new head", "failed", "failed", "pr", "new", 12, "completed", true, true, "", false},
		{"failed after merge", "failed", "failed", "pr", "new", 12, "completed", true, false, "merge", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Repo: "aerial", Status: tc.status, Mode: tc.mode, PRNumber: tc.pr, HeadSHA: "old", MergeSHA: tc.mergeSHA}
			st, pool := rcOpen(t, tc.parent, []model.VanadisChunk{chunk, {ID: "c2", BuildID: "b1", ChunkIndex: 1, Status: tc.sibling}}, "")
			err := st.recoverFailedPR(rcVerified(!tc.unverified), "c1", tc.head)
			if tc.skip && !tc.legacySeed {
				legacyChunk := chunk
				legacyChunk.HeadSHA = ""
				legacy := rcOpenStore(t, tc.parent, []model.VanadisChunk{legacyChunk}, rcFailNoWrites)
				if legacyErr := legacy.recoverFailedPR(rcVerified(!tc.unverified), "c1", tc.head); !errors.Is(legacyErr, ErrPRTransitionSkipped) {
					t.Fatalf("legacy error=%v", legacyErr)
				}
			}
			if errors.Is(err, ErrPRTransitionSkipped) != tc.skip || (err != nil && !tc.skip) {
				t.Fatalf("error=%v skip=%v", err, tc.skip)
			}
			if tc.skip {
				return
			}
			got := loadChunk(t, pool, "c1")
			if got.Status != "pr_opened" || got.HeadSHA != tc.head || loadBuild(t, pool, "b1").Status != "running" {
				t.Fatalf("after recovery chunk=%s head=%s", got.Status, got.HeadSHA)
			}
		})
	}
}

func TestPostgresVanadisStore_RecoverFailedPRFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		closed     bool
		sabotage   string
		baseline   bool
		wantErrSub string
	}{
		{name: "begin", closed: true, wantErrSub: "closed pool"},
		{name: "lookup", sabotage: `ALTER TABLE vanadis_chunks RENAME COLUMN build_id TO build_ref;`, wantErrSub: "build_id"},
		{name: "parent", sabotage: `ALTER TABLE vanadis_builds RENAME COLUMN quiet TO quiet_x;`, wantErrSub: "quiet"},
		{name: "chunks", sabotage: `ALTER TABLE vanadis_chunks RENAME COLUMN banner TO banner_x;`, wantErrSub: "banner"},
		{name: "chunk write", sabotage: failStatement("vanadis_chunks", "UPDATE"), wantErrSub: "sabotaged UPDATE on vanadis_chunks"},
		{name: "parent write", sabotage: failStatement("vanadis_builds", "UPDATE"), wantErrSub: "sabotaged UPDATE on vanadis_builds"},
		{name: "baseline write", baseline: true, sabotage: failStatement("vanadis_chunks", "UPDATE"), wantErrSub: "sabotaged UPDATE on vanadis_chunks"},
		{name: "baseline commit", baseline: true, sabotage: failCommit("vanadis_chunks", "UPDATE"), wantErrSub: "sabotaged UPDATE on vanadis_chunks"},
		{name: "commit", sabotage: failCommit("vanadis_builds", "UPDATE"), wantErrSub: "sabotaged UPDATE on vanadis_builds"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}
			if tc.baseline {
				chunk.HeadSHA = ""
			}
			st, pool := rcOpen(t, "failed", []model.VanadisChunk{chunk}, tc.sabotage)
			if tc.closed {
				pool.Close()
			}
			err := st.recoverFailedPR(rcVerified(true), "c1", "new")
			if err == nil || errors.Is(err, ErrPRTransitionSkipped) || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("err=%v want containing %q", err, tc.wantErrSub)
			}
		})
	}
}

// A failed sibling no longer blocks reopening the parent, so the first
// recovery writes the parent; that write's failure must roll back the chunk
// head update too.
func TestPostgresVanadisStore_RecoverFailedPRParentWriteRollsBack(t *testing.T) {
	t.Parallel()
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}, {ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "old"}}
	st, pool := rcOpen(t, "failed", chunks, failStatement("vanadis_builds", "UPDATE"))
	err := st.recoverFailedPR(rcVerified(true), "c1", "new")
	if err == nil || !strings.Contains(err.Error(), "sabotaged UPDATE on vanadis_builds") {
		t.Fatalf("recoverFailedPR err = %v, want parent write failure", err)
	}
	parent := loadBuild(t, pool, "b1").Status
	for _, id := range []string{"c1", "c2"} {
		if c := loadChunk(t, pool, id); parent != "failed" || c.Status != "failed" || c.HeadSHA != "old" {
			t.Fatalf("after rollback parent=%s %s=%s/%s, want failed and unchanged", parent, id, c.Status, c.HeadSHA)
		}
	}
}

func TestPostgresVanadisStore_PRRecoveryHeadUpdates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, nextStatus string
		skip             bool
	}{
		{"open while sibling failed", "pr_opened", false},
		{"ci failure while sibling failed", "ci_failed", false},
		{"conflict while sibling failed", "conflict", false},
		{"deployment failure remains post-merge", "deploy_failed", true},
		{"merged remains terminal", "merged", true},
		{"cancelled remains terminal", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed"}, {ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "h1"}}
			st, pool := rcOpen(t, "failed", chunks, "")
			rcAssertLegacyBaseline(t, st, pool)
			if err := st.recoverFailedPR(rcVerified(true), "c1", "h2"); err != nil {
				t.Fatal(err)
			}
			rcSetStatus(t, pool, "c1", tc.nextStatus)
			err := st.recoverFailedPR(rcVerified(true), "c1", "h3")
			if errors.Is(err, ErrPRTransitionSkipped) != tc.skip || (err != nil && !tc.skip) {
				t.Fatalf("head update: %v", err)
			}
			wantHead := "h3"
			if tc.skip {
				wantHead = "h2"
			}
			rcAssertRunningHead(t, pool, wantHead)
			if err := st.recoverFailedPR(rcVerified(true), "c2", "h2"); err != nil {
				t.Fatal(err)
			}
			rcAssertRunningHead(t, pool, wantHead)
		})
	}
}

// rcAssertLegacyBaseline recovers legacy chunk c1 (no stored head) twice: each
// call only seeds its baseline head, is skipped and keeps the failure state.
func rcAssertLegacyBaseline(t *testing.T, st *Store, pool *pgxpool.Pool) {
	t.Helper()
	for range 2 {
		err := st.recoverFailedPR(rcVerified(true), "c1", "h1")
		c1, c2, parent := loadChunk(t, pool, "c1"), loadChunk(t, pool, "c2"), loadBuild(t, pool, "b1").Status
		if !errors.Is(err, ErrPRTransitionSkipped) || c1.HeadSHA != "h1" || c1.Status != "failed" || parent != "failed" || c2.HeadSHA != "h1" {
			t.Fatalf("baseline error=%v parent=%s c1=%+v c2=%+v", err, parent, c1, c2)
		}
	}
}

// rcAssertRunningHead checks b1 is running and c1 stores wantHead.
func rcAssertRunningHead(t *testing.T, pool *pgxpool.Pool, wantHead string) {
	t.Helper()
	if parent, head := loadBuild(t, pool, "b1").Status, loadChunk(t, pool, "c1").HeadSHA; parent != "running" || head != wantHead {
		t.Fatalf("parent=%s head=%s, want running/%s", parent, head, wantHead)
	}
}
