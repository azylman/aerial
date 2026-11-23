package store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// rtChunks assigns sequential chunk indexes in build b1 to chunk and its
// siblings; a chunk without an ID is left out.
func rtChunks(chunk model.VanadisChunk, siblings []model.VanadisChunk) []model.VanadisChunk {
	var out []model.VanadisChunk
	for _, c := range append([]model.VanadisChunk{chunk}, siblings...) {
		if c.ID == "" {
			continue
		}
		c.BuildID, c.ChunkIndex = "b1", len(out)
		out = append(out, c)
	}
	return out
}

func TestPostgresVanadisStore_RetryActionChunk(t *testing.T) {
	t.Parallel()
	failed := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}
	tests := []struct {
		name               string
		buildStatus        string
		chunk              model.VanadisChunk
		siblings           []model.VanadisChunk
		expected           string
		nilReceiver, nilDB bool
		closed             bool
		sabotage           string
		wantErr            bool
		wantErrIs          error
		request            *RetryActionChunkRequest
		wantStatus         string
		wantGeneration     int
	}{
		{name: "empty build", request: &RetryActionChunkRequest{ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "empty chunk", request: &RetryActionChunkRequest{BuildID: "b1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "blank reason", request: &RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: " "}, wantErr: true, wantErrIs: ErrInvalidBuildInput},
		{name: "negative generation", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: -1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed replay", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing prerequisite", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"absent"}}, expected: "vanadis-c1-turn-0", wantErr: true},

		{name: "generation overflow", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2147483647}, expected: "vanadis-c1-turn-2147483647", wantErr: true},
		{name: "transitive completed descendant", buildStatus: "failed", chunk: failed, siblings: []model.VanadisChunk{{ID: "c3", Status: "completed", DependsOn: []string{"c2"}}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil receiver", nilReceiver: true, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil database", nilDB: true, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "parent not found", expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrBuildNotFound},
		{name: "parent query error", buildStatus: "failed", chunk: failed, sabotage: `ALTER TABLE vanadis_builds RENAME COLUMN quiet TO quiet_x;`, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "selected absent", buildStatus: "failed", chunk: model.VanadisChunk{ID: "other", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrChunkNotFound},
		{name: "failed action", buildStatus: "failed", chunk: failed, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "committed reset replay pending", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "pending", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "replay after independent sibling launch", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, siblings: []model.VanadisChunk{{ID: "c2", Status: "running"}}, expected: "vanadis-c1-turn-0", wantStatus: "running", wantGeneration: 1},
		{name: "committed reset replay running", buildStatus: "running", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantStatus: "running", wantGeneration: 1},
		{name: "terminal replay", buildStatus: "completed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing identity", buildStatus: "failed", chunk: failed, wantErr: true},
		{name: "stale identity", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled parent", buildStatus: "cancelled", chunk: failed, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "pr action", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Repo: "aerial", Status: "failed", PRNumber: 12}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "pr mode", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled selected", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "cancelled"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed sibling accepted", buildStatus: "failed", chunk: failed, siblings: []model.VanadisChunk{{ID: "c2", Status: "failed"}}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "cancelled sibling accepted", buildStatus: "failed", chunk: failed, siblings: []model.VanadisChunk{{ID: "c2", Status: "cancelled"}}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "active sibling accepted", buildStatus: "failed", chunk: failed, siblings: []model.VanadisChunk{{ID: "c2", Status: "deploying"}}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "running sibling in running build", buildStatus: "running", chunk: failed, siblings: []model.VanadisChunk{{ID: "c2", Status: "running"}}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "running descendant", buildStatus: "running", chunk: failed, siblings: []model.VanadisChunk{{ID: "c2", Status: "running", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "unready dependency", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []model.VanadisChunk{{ID: "c0", Mode: "pr", Status: "merged"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunk: failed, siblings: []model.VanadisChunk{{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "safe graph", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []model.VanadisChunk{{ID: "c0", Status: "completed"}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantStatus: "pending", wantGeneration: 1},
		{name: "begin failure", closed: true, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "query failure", buildStatus: "failed", chunk: failed, sabotage: `ALTER TABLE vanadis_chunks RENAME COLUMN banner TO banner_x;`, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "scan failure", buildStatus: "failed", chunk: failed, sabotage: breakChunkScanSQL, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "rows failure", buildStatus: "failed", chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", Banner: "boom"}, sabotage: failChunkRowsSQL, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "chunk write rollback", buildStatus: "failed", chunk: failed, expected: "vanadis-c1-turn-0", sabotage: failStatement("vanadis_chunks", "UPDATE"), wantErr: true},
		{name: "commit failure", buildStatus: "failed", chunk: failed, expected: "vanadis-c1-turn-0", sabotage: failCommit("vanadis_chunks", "UPDATE"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st *Store
			switch {
			case tc.nilDB:
				st = &Store{}
			case !tc.nilReceiver:
				pool := rtOpen(t, tc.buildStatus, rtChunks(tc.chunk, tc.siblings), tc.sabotage, tc.closed)
				st = New(pool, testOwner)
				defer rtAssertStored(t, pool, tc.wantStatus, tc.wantGeneration)
			}
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

// rtOpen seeds build b1 (when buildStatus is set) with chunks into a private
// database, applies sabotage and optionally closes the pool.
func rtOpen(t *testing.T, buildStatus string, chunks []model.VanadisChunk, sabotage string, closed bool) *pgxpool.Pool {
	t.Helper()
	_, pool := openStore(t)
	if buildStatus != "" {
		seed(t, pool, []model.VanadisBuild{{ID: "b1", ThreadID: "thread-1", Status: buildStatus}}, chunks)
	}
	mustExec(t, pool, sabotage)
	if closed {
		pool.Close()
	}
	return pool
}

// rtAssertStored checks c1's stored status and generation for accepted
// retries; an empty wantStatus skips the check.
func rtAssertStored(t *testing.T, pool *pgxpool.Pool, wantStatus string, wantGeneration int) {
	t.Helper()
	if wantStatus == "" || t.Failed() {
		return
	}
	if c := loadChunk(t, pool, "c1"); c.Status != wantStatus || c.StallNudges != wantGeneration {
		t.Errorf("stored c1 = %s/%d, want %s/%d", c.Status, c.StallNudges, wantStatus, wantGeneration)
	}
}

func TestPostgresVanadisStore_RetryActionChunkTransactions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                          string
		concurrent                                    int
		failBuild                                     bool
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
		// A lost commit acknowledgement cannot be produced on a real server;
		// the durable half is the committed retry followed by a journal
		// replay carrying a different payload, which must be acknowledged
		// without a second reset.
		{name: "committed before journal replay", concurrent: 1, replayChanged: true, wantStatus: "pending", wantGeneration: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sabotage := ""
			if tc.failBuild {
				sabotage = failStatement("vanadis_builds", "UPDATE")
			}
			chunks := []model.VanadisChunk{
				{ID: "c0", BuildID: "b1", Status: "completed"},
				{ID: "c1", BuildID: "b1", ChunkIndex: 1, Mode: "action", Status: "failed", DependsOn: []string{"c0"}, Instructions: "verify"},
			}
			pool := rtOpen(t, "failed", chunks, sabotage, false)
			st := New(pool, testOwner)
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry", ExpectedInstructions: tc.expectedInstructions, ReplacementInstructions: tc.replacementInstructions}
			for err := range retryConcurrentRequests(st, req, tc.concurrent) {
				if (err != nil) != tc.wantErr {
					t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
				}
			}
			if tc.replayChanged {
				req.ExpectedInstructions, req.ReplacementInstructions = new("different"), new("ignored")
				result, err := st.RetryActionChunk(context.Background(), req)
				if err != nil || !validRetryResult(result) {
					t.Fatalf("replay=%+v err=%v", result, err)
				}
			}
			current := loadChunk(t, pool, "c1")
			if current.Status != tc.wantStatus || current.StallNudges != tc.wantGeneration {
				t.Fatalf("status=%s generation=%d", current.Status, current.StallNudges)
			}
			if current.Instructions != cmp.Or(tc.wantInstructions, "verify") {
				t.Fatalf("instructions=%q", current.Instructions)
			}
			wantParent := map[string]string{"pending": "running", "failed": "failed"}[tc.wantStatus]
			if buildStatus := loadBuild(t, pool, "b1").Status; buildStatus != wantParent {
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
