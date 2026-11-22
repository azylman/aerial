package store

// PR recovery store tests for vanadis-restate.
//
// Covers recoverFailedPR head acceptance rules, failure injection, baseline
// seeding, and head-update tracking. Moved here from store_ci_fail_test.go
// alongside the production module; assertions unchanged.

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	}}, "example-org")
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

// A failed sibling no longer blocks reopening the parent, so the first
// recovery writes the parent; that write's failure must roll back the chunk
// head update too (rollback is only observable on the stateful fixture).
func TestPostgresVanadisStore_RecoverFailedPRParentWriteRollsBack(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	parent := "failed"
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}, {ID: "c2", BuildID: "b1", Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "old"}}
	st := recoveryFixtureStore(&mu, &parent, &chunks, true)
	err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true), "c1", "new")
	if err == nil || !strings.Contains(err.Error(), "parent write failed") {
		t.Fatalf("recoverFailedPR err = %v, want parent write failure", err)
	}
	want := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}, {ID: "c2", BuildID: "b1", Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "old"}}
	if parent != "failed" || !reflect.DeepEqual(chunks, want) {
		t.Fatalf("after rollback parent=%s chunks=%+v, want failed and unchanged", parent, chunks)
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
			if parent != "running" || chunks[0].HeadSHA != wantHead {
				t.Fatalf("parent=%s head=%s", parent, chunks[0].HeadSHA)
			}
			if err := st.recoverFailedPR(context.WithValue(context.Background(), PrHeadVerifiedContextKey{}, true), "c2", "h2"); err != nil {
				t.Fatal(err)
			}
			if parent != "running" || chunks[0].HeadSHA != wantHead {
				t.Fatalf("after sibling recovery parent=%s head=%s", parent, chunks[0].HeadSHA)
			}
		})
	}
}
