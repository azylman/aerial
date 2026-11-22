package store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

func TestPostgresVanadisStore_RetryActionChunk(t *testing.T) {
	t.Parallel()
	injectedErr := errors.New("injected database failure")
	tests := []struct {
		name        string
		buildStatus string
		chunk       model.VanadisChunk
		siblings    []model.VanadisChunk
		expected    string
		db          dbScript
		wantErr     bool
		wantErrIs   error
		request     *RetryActionChunkRequest
	}{
		{name: "empty build", request: &RetryActionChunkRequest{ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "empty chunk", request: &RetryActionChunkRequest{BuildID: "b1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "blank reason", request: &RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: " "}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "negative generation", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: -1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed replay", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing prerequisite", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"absent"}}, expected: "vanadis-c1-turn-0", wantErr: true},

		{name: "generation overflow", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2147483647}, expected: "vanadis-c1-turn-2147483647", wantErr: true},
		{name: "transitive completed descendant", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c3", Status: "completed", DependsOn: []string{"c2"}}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil receiver", db: nilReceiver, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil database", db: nilDB, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "parent not found", db: dbScript{Routes: []sqlRoute{{Match: "FROM vanadis_builds", Row: sqltest.ErrRow(pgx.ErrNoRows)}}}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrBuildNotFound},
		{name: "parent query error", db: dbScript{Routes: []sqlRoute{{Match: "FROM vanadis_builds", Row: sqltest.ErrRow(injectedErr)}}}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "selected absent", buildStatus: "failed", chunk: model.VanadisChunk{ID: "other", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrChunkNotFound},
		{name: "failed action", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0"},
		{name: "committed reset replay pending", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "pending", StallNudges: 1}, expected: "vanadis-c1-turn-0"},
		{name: "replay after independent sibling launch", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, siblings: []model.VanadisChunk{{ID: "c2", Status: "running"}}, expected: "vanadis-c1-turn-0"},
		{name: "committed reset replay running", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, expected: "vanadis-c1-turn-0"},
		{name: "terminal replay", buildStatus: "completed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing identity", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, wantErr: true},
		{name: "stale identity", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled parent", buildStatus: "cancelled", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "pr action", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", PRNumber: 12}, expected: "vanadis-c1-turn-0"},
		{name: "pr mode", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled selected", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "cancelled"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed sibling accepted", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c2", Status: "failed"}}, expected: "vanadis-c1-turn-0"},
		{name: "cancelled sibling accepted", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c2", Status: "cancelled"}}, expected: "vanadis-c1-turn-0"},
		{name: "active sibling accepted", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c2", Status: "deploying"}}, expected: "vanadis-c1-turn-0"},
		{name: "running sibling in running build", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c2", Status: "running"}}, expected: "vanadis-c1-turn-0"},
		{name: "running descendant", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c2", Status: "running", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "unready dependency", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []model.VanadisChunk{{ID: "c0", Mode: "pr", Status: "merged"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []model.VanadisChunk{{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "safe graph", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []model.VanadisChunk{{ID: "c0", Status: "completed"}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0"},
		{name: "begin failure", db: dbScript{BeginErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "query failure", buildStatus: "failed", db: dbScript{QueryErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "scan failure", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1"}, db: dbScript{ScanErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "rows failure", buildStatus: "failed", db: dbScript{RowsErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "chunk write rollback", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", db: dbScript{FailExec: "UPDATE vanadis_chunks", ExecErr: errors.New("write")}, wantErr: true},
		{name: "commit failure", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", db: dbScript{CommitErr: errors.New("commit")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.db.BuildRow = sqltest.BuildRow("b1", tc.buildStatus)
			tc.db.Chunks = append([]model.VanadisChunk{tc.chunk}, tc.siblings...)
			st, _ := open(tc.db)
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: tc.expected, Reason: "operator retry"}
			if tc.request != nil {
				req = *tc.request
			}
			got, err := st.RetryActionChunk(context.Background(), req)
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
			original := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}, StallNudges: 0, Instructions: "verify"}
			current := original
			sibling := model.VanadisChunk{ID: "c0", BuildID: "b1", Status: "completed"}
			lost := tc.lostCommit
			st := retryFixtureStore(&mu, &current, &buildStatus, sibling, tc.failBuild, &lost)
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry", ExpectedInstructions: tc.expectedInstructions, ReplacementInstructions: tc.replacementInstructions}
			for err := range retryConcurrentRequests(st, req, tc.concurrent) {
				if (err != nil) != tc.wantErr {
					t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
				}
			}
			if tc.lostCommit || tc.replayChanged {
				req.ExpectedInstructions, req.ReplacementInstructions = new("different"), new("ignored")
				result, err := st.RetryActionChunk(context.Background(), req)
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

func validRetryResult(result *RetryActionChunkResult) bool {
	return result != nil && result.DispatchID == "vanadis-c1-turn-1" && result.Accepted
}

func retryConcurrentRequests(st *Store, req RetryActionChunkRequest, concurrent int) <-chan error {
	results := make(chan error, concurrent)
	var wg sync.WaitGroup
	for range concurrent {
		wg.Go(func() {
			result, err := st.RetryActionChunk(context.Background(), req)
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

func retryFixtureStore(mu *sync.Mutex, current *model.VanadisChunk, buildStatus *string, sibling model.VanadisChunk, failBuild bool, lost *bool) *Store {
	return New(sqltest.RetryFixtureDB(mu, current, buildStatus, sibling, failBuild, lost), "example-org")
}
