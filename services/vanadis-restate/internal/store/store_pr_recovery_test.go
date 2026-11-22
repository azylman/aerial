package store

// PR recovery store tests for vanadis-restate.
//
// Covers recoverFailedPR head acceptance rules, failure injection, baseline
// seeding, and head-update tracking. Moved here from store_ci_fail_test.go
// alongside the production module; assertions unchanged.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

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
			st, _ := dbScript{row: valuesRow("b1"), buildRow: buildRow("b1", tc.parent), chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: tc.status, Mode: tc.mode, PRNumber: tc.pr, HeadSHA: "old", MergeSHA: tc.mergeSHA}, {ID: "c2", Status: tc.sibling}}}.open()
			err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, !tc.unverified), "c1", tc.head)
			if tc.skip && !tc.legacySeed {
				forbidden := errors.New("excluded legacy chunk attempted write or commit")
				legacy, _ := dbScript{execErr: forbidden, commitErr: forbidden, row: valuesRow("b1"), buildRow: buildRow("b1", tc.parent), chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: tc.status, Mode: tc.mode, PRNumber: tc.pr, MergeSHA: tc.mergeSHA}}}.open()
				if legacyErr := legacy.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, !tc.unverified), "c1", tc.head); !errors.Is(legacyErr, ErrPRTransitionSkipped) {
					t.Fatalf("legacy error=%v", legacyErr)
				}
			}
			if errors.Is(err, ErrPRTransitionSkipped) != tc.skip || (err != nil && !tc.skip) {
				t.Fatalf("error=%v skip=%v", err, tc.skip)
			}
		})
	}
}

func TestPostgresVanadisStore_RecoverFailedPRFailures(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected")
	tests := []struct {
		name     string
		db       dbScript
		baseline bool
		want     error
	}{
		{name: "begin", db: dbScript{beginErr: injected}, want: injected},
		{name: "lookup", db: dbScript{row: errRow(injected)}, want: injected},
		{name: "parent", db: dbScript{buildRow: errRow(injected)}, want: injected},
		{name: "chunks", db: dbScript{queryErr: injected}, want: injected},
		{name: "chunk write", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: injected}, want: injected},
		{name: "parent write", db: dbScript{failExec: "UPDATE vanadis_builds", execErr: injected}, want: injected},
		{name: "baseline write", baseline: true, db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: injected}, want: injected},
		{name: "baseline commit", baseline: true, db: dbScript{commitErr: injected}, want: injected},
		{name: "commit", db: dbScript{commitErr: injected}, want: injected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := tc.db
			if db.row == nil {
				db.row = valuesRow("b1")
			}
			if db.buildRow == nil {
				db.buildRow = buildRow("b1", "failed")
			}
			db.chunks = []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}}
			if tc.baseline {
				db.chunks[0].HeadSHA = ""
			}
			st, _ := db.open()
			ctx := context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true)
			if err := st.recoverFailedPR(ctx, "c1", "new"); !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
		})
	}
}

// recoveryFixtureStore models the external database's atomic commit and parent
// lock for recovery tests.
func recoveryFixtureStore(mu *sync.Mutex, parent *string, chunks *[]model.VanadisChunk, failParent bool) *Store {
	return New(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}, beginFn: func(context.Context) (pgx.Tx, error) {
		var local []model.VanadisChunk
		var state string
		locked, closed := false, false
		tx := &mockTx{
			queryRowFn: func(_ context.Context, q string, _ ...any) pgx.Row {
				if strings.Contains(q, "SELECT build_id") {
					return valuesRow("b1")
				}
				mu.Lock()
				locked = true
				state = *parent
				local = append([]model.VanadisChunk(nil), (*chunks)...)
				return buildRow("b1", state)
			}, queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{items: local}, nil },
			execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
				if !locked {
					return pgconn.CommandTag{}, errors.New("write without lock")
				}
				if strings.Contains(q, "UPDATE vanadis_chunks") {
					applyRecoveryFixtureHead(local, args, strings.Contains(q, "SET ci_checks = COALESCE"))
				} else {
					if failParent {
						return pgconn.CommandTag{}, errors.New("parent write failed")
					}
					state = "running"
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		}
		return &retryTransaction{mockTx: tx, finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if locked {
				defer mu.Unlock()
			}
			if commit {
				*chunks = local
				*parent = state
			}
			return nil
		}}, nil
	}}, "azylman")
}

// retryTransaction models the external database's atomic commit and parent lock.
type retryTransaction struct {
	*mockTx

	finish func(commit bool) error
}

func (tx *retryTransaction) Commit(context.Context) error   { return tx.finish(true) }
func (tx *retryTransaction) Rollback(context.Context) error { return tx.finish(false) }

func applyRecoveryFixtureHead(chunks []model.VanadisChunk, args []any, baseline bool) {
	for i := range chunks {
		if chunks[i].ID == args[0] {
			if !baseline {
				chunks[i].Status = "pr_opened"
			}
			chunks[i].HeadSHA, _ = args[1].(string)
		}
	}
}

func assertLegacyBaseline(t *testing.T, st *Store, parent *string, stored *[]model.VanadisChunk) {
	t.Helper()
	for _, head := range []string{"h1", "h1"} {
		err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true), "c1", head)
		chunks := *stored
		if !errors.Is(err, ErrPRTransitionSkipped) || chunks[0].HeadSHA != "h1" || chunks[0].Status != "failed" || *parent != "failed" || chunks[1].HeadSHA != "h1" {
			t.Fatalf("baseline error=%v parent=%s chunks=%+v", err, *parent, chunks)
		}
	}
}

func TestPostgresVanadisStore_PRRecoveryTransactions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		failParent bool
		wantParent string
		wantFailed int
	}{
		{"parent write rolls back last recovery", true, "failed", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			parent := "failed"
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}, {ID: "c2", BuildID: "b1", Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "old"}}
			st := recoveryFixtureStore(&mu, &parent, &chunks, tc.failParent)
			ctx := context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true)
			if err := st.recoverFailedPR(ctx, "c1", "new"); err != nil {
				t.Fatal(err)
			}
			if err := st.recoverFailedPR(ctx, "c2", "new"); err == nil {
				t.Fatal("expected parent write failure")
			}
			failed := 0
			for _, c := range chunks {
				if c.Status == "failed" {
					failed++
				}
			}
			if parent != tc.wantParent || failed != tc.wantFailed {
				t.Fatalf("parent=%s failed=%d", parent, failed)
			}
		})
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
			var mu sync.Mutex
			parent := "failed"
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "h1"}, {ID: "c2", BuildID: "b1", Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "h1"}}
			st := recoveryFixtureStore(&mu, &parent, &chunks, false)
			chunks[0].HeadSHA = ""
			assertLegacyBaseline(t, st, &parent, &chunks)
			if err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true), "c1", "h2"); err != nil {
				t.Fatal(err)
			}
			chunks[0].Status = tc.nextStatus
			err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true), "c1", "h3")
			if errors.Is(err, ErrPRTransitionSkipped) != tc.skip || (err != nil && !tc.skip) {
				t.Fatalf("head update: %v", err)
			}
			wantHead := "h3"
			if tc.skip {
				wantHead = "h2"
			}
			if parent != "failed" || chunks[0].HeadSHA != wantHead {
				t.Fatalf("parent=%s head=%s", parent, chunks[0].HeadSHA)
			}
			if err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true), "c2", "h2"); err != nil {
				t.Fatal(err)
			}
			wantParent := "running"
			if tc.nextStatus == "cancelled" {
				wantParent = "failed"
			}
			if parent != wantParent || chunks[0].HeadSHA != wantHead {
				t.Fatalf("after sibling recovery parent=%s head=%s", parent, chunks[0].HeadSHA)
			}
		})
	}
}

func TestPostgresVanadisStore_UpdatePRLifecycleStatus_RecoveryQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		chunkID    string
		status     string
		wantCTE    string
		wantWhere  string
		wantBanner string
	}{
		{
			name:       "merged lifecycle status contains CTE and recovery where clause",
			chunkID:    "c1",
			status:     "merged",
			wantCTE:    "WITH reopened_build AS (UPDATE vanadis_builds",
			wantWhere:  "AND status NOT IN ('completed', 'failed', 'cancelled')",
			wantBanner: "banner = CASE WHEN status = 'failed' THEN '' ELSE banner END",
		},
		{
			name:       "deploying lifecycle status contains CTE and recovery where clause",
			chunkID:    "c1",
			status:     "deploying",
			wantCTE:    "WITH reopened_build AS (UPDATE vanadis_builds",
			wantWhere:  "AND status NOT IN ('completed', 'failed', 'cancelled')",
			wantBanner: "banner = CASE WHEN status = 'failed' THEN '' ELSE banner END",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var capturedQuery string
			var capturedArgs []any
			st := New(mockDB{execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
				capturedQuery = q
				capturedArgs = args
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}, "azylman")
			err := st.UpdatePRLifecycleStatus(context.Background(), tc.chunkID, tc.status)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(capturedQuery, tc.wantCTE) {
				t.Fatalf("query missing CTE: %s", capturedQuery)
			}
			if !strings.Contains(capturedQuery, tc.wantWhere) {
				t.Fatalf("query missing where: %s", capturedQuery)
			}
			if !strings.Contains(capturedQuery, tc.wantBanner) {
				t.Fatalf("query missing banner: %s", capturedQuery)
			}
			if len(capturedArgs) != 2 || capturedArgs[0] != tc.status || capturedArgs[1] != tc.chunkID {
				t.Fatalf("unexpected args: %v", capturedArgs)
			}
		})
	}
}

func TestPostgresVanadisStore_CompletePRDeployment_RecoveryQuery(t *testing.T) {
	t.Parallel()
	var capturedQuery string
	var capturedArgs []any
	st := New(mockDB{execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
		capturedQuery = q
		capturedArgs = args
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}}, "azylman")
	err := st.CompletePRDeployment(context.Background(), "c1", &pubsub.PRRolloutState{ExpectedJobs: []string{"test"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(capturedQuery, "WITH reopened_build AS (UPDATE vanadis_builds") {
		t.Fatalf("query missing CTE: %s", capturedQuery)
	}
	if !strings.Contains(capturedQuery, "c.status != 'cancelled' AND (c.status != 'failed' OR COALESCE(c.pr_number, 0) > 0)") {
		t.Fatalf("query missing recovery condition: %s", capturedQuery)
	}
	if len(capturedArgs) != 2 || capturedArgs[1] != "c1" {
		t.Fatalf("unexpected args: %v", capturedArgs)
	}
}
