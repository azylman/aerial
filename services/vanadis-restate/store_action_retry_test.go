package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresVanadisStore_RetryActionChunk(t *testing.T) {
	t.Parallel()
	injectedErr := errors.New("injected database failure")
	tests := []struct {
		name        string
		buildStatus string
		chunk       VanadisChunk
		siblings    []VanadisChunk
		expected    string
		db          dbScript
		wantErr     bool
		wantErrIs   error
		request     *RetryActionChunkRequest
	}{
		{name: "empty build", request: &RetryActionChunkRequest{ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "empty chunk", request: &RetryActionChunkRequest{BuildID: "b1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "blank reason", request: &RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: " "}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "negative generation", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: -1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed replay", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing prerequisite", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"absent"}}, expected: "vanadis-c1-turn-0", wantErr: true},

		{name: "generation overflow", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2147483647}, expected: "vanadis-c1-turn-2147483647", wantErr: true},
		{name: "transitive completed descendant", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c3", Status: "completed", DependsOn: []string{"c2"}}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil receiver", db: dbScript{bare: nilReceiver}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil database", db: dbScript{bare: nilDB}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "parent not found", db: dbScript{routes: []sqlRoute{{match: "FROM vanadis_builds", row: errRow(pgx.ErrNoRows)}}}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrBuildNotFound},
		{name: "parent query error", db: dbScript{routes: []sqlRoute{{match: "FROM vanadis_builds", row: errRow(injectedErr)}}}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "selected absent", buildStatus: "failed", chunk: VanadisChunk{ID: "other", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrChunkNotFound},
		{name: "failed action", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0"},
		{name: "committed reset replay pending", buildStatus: "running", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "pending", StallNudges: 1}, expected: "vanadis-c1-turn-0"},
		{name: "replay after independent sibling launch", buildStatus: "running", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, siblings: []VanadisChunk{{ID: "c2", Status: "running"}}, expected: "vanadis-c1-turn-0"},
		{name: "committed reset replay running", buildStatus: "running", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, expected: "vanadis-c1-turn-0"},
		{name: "terminal replay", buildStatus: "completed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing identity", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, wantErr: true},
		{name: "stale identity", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled parent", buildStatus: "cancelled", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "pr action", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", PRNumber: 12}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "pr mode", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled selected", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "cancelled"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed sibling", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "failed"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled sibling", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "cancelled"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "active sibling", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "deploying"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "unready dependency", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []VanadisChunk{{ID: "c0", Mode: "pr", Status: "merged"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "safe graph", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []VanadisChunk{{ID: "c0", Status: "completed"}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0"},
		{name: "begin failure", db: dbScript{beginErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "query failure", buildStatus: "failed", db: dbScript{queryErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "scan failure", buildStatus: "failed", chunk: VanadisChunk{ID: "c1"}, db: dbScript{scanErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "rows failure", buildStatus: "failed", db: dbScript{rowsErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "chunk write rollback", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("write")}, wantErr: true},
		{name: "commit failure", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", db: dbScript{commitErr: errors.New("commit")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.db.buildRow = buildRow("b1", tc.buildStatus)
			tc.db.chunks = append([]VanadisChunk{tc.chunk}, tc.siblings...)
			store, _ := tc.db.open()
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: tc.expected, Reason: "operator retry"}
			if tc.request != nil {
				req = *tc.request
			}
			got, err := store.RetryActionChunk(context.Background(), req)
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err=%v want=%v", err, tc.wantErrIs)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("result=%+v err=%v wantErr=%v", got, err, tc.wantErr)
			}
			if !tc.wantErr && (got == nil || *got != (RetryActionChunkResult{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1", Accepted: true})) {
				t.Fatalf("result=%+v", got)
			}
		})
	}
}

// retryTransaction models the external database's atomic commit and parent lock.
// State changes remain private until Commit; Rollback discards them.
type retryTransaction struct {
	*mockTx

	finish func(bool) error
}

func (tx *retryTransaction) Commit(context.Context) error   { return tx.finish(true) }
func (tx *retryTransaction) Rollback(context.Context) error { return tx.finish(false) }

func TestPostgresVanadisStore_RetryActionChunkTransactions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                          string
		concurrent                                    int
		failBuild                                     bool
		lostCommit                                    bool
		wantErr                                       bool
		wantStatus                                    string
		wantGeneration                                int
		expectedInstructions, replacementInstructions *string
		wantInstructions                              string
		replayChanged                                 bool
	}{
		{name: "correction rolls back", concurrent: 1, failBuild: true, wantErr: true, expectedInstructions: new("verify"), replacementInstructions: new("corrected"), wantInstructions: "verify", wantStatus: "failed"},
		{name: "corrected", concurrent: 8, expectedInstructions: new("verify"), replacementInstructions: new("corrected"), wantInstructions: "corrected", wantStatus: "pending", wantGeneration: 1},
		{name: "instruction mismatch", concurrent: 1, wantErr: true, expectedInstructions: new("other"), replacementInstructions: new("corrected"), wantInstructions: "verify", wantStatus: "failed", wantGeneration: 0},
		{name: "exact mismatch", concurrent: 1, wantErr: true, expectedInstructions: new("verify "), replacementInstructions: new("corrected"), wantInstructions: "verify", wantStatus: "failed", wantGeneration: 0},
		{name: "changed replay payload", concurrent: 1, replayChanged: true, expectedInstructions: new("verify"), replacementInstructions: new("corrected"), wantInstructions: "corrected", wantStatus: "pending", wantGeneration: 1},
		{name: "missing replacement", concurrent: 1, wantErr: true, expectedInstructions: new("verify"), replacementInstructions: nil, wantInstructions: "verify", wantStatus: "failed", wantGeneration: 0},
		{name: "missing expected", concurrent: 1, wantErr: true, expectedInstructions: nil, replacementInstructions: new("corrected"), wantInstructions: "verify", wantStatus: "failed", wantGeneration: 0},
		{name: "blank expected", concurrent: 1, wantErr: true, expectedInstructions: new(" "), replacementInstructions: new("corrected"), wantInstructions: "verify", wantStatus: "failed", wantGeneration: 0},
		{name: "blank replacement", concurrent: 1, wantErr: true, expectedInstructions: new("verify"), replacementInstructions: new(" "), wantInstructions: "verify", wantStatus: "failed", wantGeneration: 0},
		{name: "duplicate concurrent requests", concurrent: 8, wantStatus: "pending", wantGeneration: 1},
		{name: "build write rolls back reset", concurrent: 1, failBuild: true, wantErr: true, wantStatus: "failed"},
		{name: "committed before journal replay", concurrent: 1, lostCommit: true, wantErr: true, wantStatus: "pending", wantGeneration: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			buildStatus := "failed"
			original := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}, StallNudges: 0, Instructions: "verify"}
			current := original
			sibling := VanadisChunk{ID: "c0", BuildID: "b1", Status: "completed"}
			lost := tc.lostCommit
			store := retryFixtureStore(&mu, &current, &buildStatus, sibling, tc.failBuild, &lost)
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry", ExpectedInstructions: tc.expectedInstructions, ReplacementInstructions: tc.replacementInstructions}
			for err := range retryConcurrentRequests(store, req, tc.concurrent) {
				if (err != nil) != tc.wantErr {
					t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
				}
			}
			if tc.lostCommit || tc.replayChanged {
				req.ExpectedInstructions, req.ReplacementInstructions = new("different"), new("ignored")
				result, err := store.RetryActionChunk(context.Background(), req)
				if err != nil || !validRetryResult(result) {
					t.Fatalf("replay=%+v err=%v", result, err)
				}
			}
			if current.Status != tc.wantStatus || current.StallNudges != tc.wantGeneration {
				t.Fatalf("status=%s generation=%d", current.Status, current.StallNudges)
			}
			if current.Instructions != cmp.Or(tc.wantInstructions, "verify") {
				t.Fatalf("instructions=%q", current.Instructions)
			}
			wantParent := map[string]string{"pending": "running", "failed": "failed"}[tc.wantStatus]
			if buildStatus != wantParent {
				t.Fatalf("parent=%s", buildStatus)
			}
		})
	}
}

func retryFixtureStore(mu *sync.Mutex, current *VanadisChunk, buildStatus *string, sibling VanadisChunk, failBuild bool, lost *bool) *PostgresVanadisStore {
	return newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		var local VanadisChunk
		var parent string
		locked := false
		closed := false
		base := &mockTx{
			queryRowFn: func(_ context.Context, query string, _ ...any) pgx.Row {
				if !strings.Contains(query, "FROM vanadis_builds") || !strings.Contains(query, "FOR UPDATE") {
					return errRow(errors.New("missing parent lock"))
				}
				mu.Lock()
				locked = true
				local = *current
				parent = *buildStatus
				return buildRow("b1", parent)
			},
			queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if !locked || !strings.Contains(query, "ORDER BY chunk_index ASC FOR UPDATE") {
					return nil, errors.New("missing ordered chunk locks")
				}
				return &mockRows{items: []VanadisChunk{sibling, local}}, nil
			},
			execFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				return retryFixtureExec(query, &local, &parent, failBuild, args...)
			},
		}
		return &retryTransaction{mockTx: base, finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if !locked {
				return errors.New("transaction never locked parent")
			}
			defer mu.Unlock()
			if commit {
				*current = local
				*buildStatus = parent
				if *lost {
					*lost = false
					return errors.New("commit response lost")
				}
			}
			return nil
		}}, nil
	}})
}

func validRetryResult(result *RetryActionChunkResult) bool {
	return result != nil && result.DispatchID == "vanadis-c1-turn-1" && result.Accepted
}

func retryFixtureExec(query string, local *VanadisChunk, parent *string, failBuild bool, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(query, "UPDATE vanadis_chunks"):
		for _, fragment := range []string{"status = 'pending'", "stall_nudges = stall_nudges + 1", "started_at = NULL", "completed_at = NULL", "ci_fail_count = 0", "ci_checks = '{}'::jsonb", "rollout_state = '{}'::jsonb", "banner = ''", "merge_awakeable_id = ''", "deploy_awakeable_id = ''"} {
			if !strings.Contains(query, fragment) {
				return pgconn.CommandTag{}, fmt.Errorf("missing reset %s", fragment)
			}
		}
		if strings.Contains(query, "instructions = COALESCE($2, instructions)") && len(args) == 2 {
			if replacement, ok := args[1].(*string); ok && replacement != nil {
				local.Instructions = *replacement
			}
		}
		local.Status = "pending"
		local.StallNudges++
	case strings.Contains(query, "UPDATE vanadis_builds SET status = 'running'"):
		if failBuild {
			return pgconn.CommandTag{}, errors.New("build write failed")
		}
		*parent = "running"
	default:
		return pgconn.CommandTag{}, errors.New("unexpected mutation")
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func retryConcurrentRequests(store *PostgresVanadisStore, req RetryActionChunkRequest, concurrent int) <-chan error {
	results := make(chan error, concurrent)
	var wg sync.WaitGroup
	for range concurrent {
		wg.Go(func() {
			result, err := store.RetryActionChunk(context.Background(), req)
			if err == nil && (!validRetryResult(result)) {
				err = fmt.Errorf("unexpected result %+v", result)
			}
			results <- err
		})
	}
	wg.Wait()
	close(results)
	return results
}

func buildRow(id, status string) pgx.Row {
	return valuesRow(id, "thread", 3, "title", "objective", "", status, "", false, time.Time{}, time.Time{})
}
