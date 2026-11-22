package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// cfChunk is the PR-attached chunk c1 of running build b1 in a given status.
func cfChunk(status string) model.VanadisChunk {
	return model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", PRNumber: 12, Status: status}
}

// cfOpen seeds running build b1 plus chunk into a private database and then
// applies sabotage. nilDB returns a Store without a database instead.
func cfOpen(t *testing.T, nilDB bool, chunk model.VanadisChunk, sabotage string) (*Store, *pgxpool.Pool) {
	t.Helper()
	if nilDB {
		return &Store{}, nil
	}
	st, pool := openStore(t)
	seed(t, pool, []model.VanadisBuild{{ID: "b1", ThreadID: "thread-1"}}, []model.VanadisChunk{chunk})
	mustExec(t, pool, sabotage)
	return st, pool
}

// cfFailChunkWrite makes every vanadis_chunks UPDATE fail.
var cfFailChunkWrite = failStatement("vanadis_chunks", "UPDATE")

func TestUpdatePRLifecycleStatus(t *testing.T) {
	t.Parallel()
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name, id, status, chunkStatus, sabotage string
		nilDB                                   bool
		initial, wantStamp                      time.Time
		wantStampNow                            bool
		wantErr, wantSkipped                    bool
	}{
		{name: "nil database", id: "c1", status: "merged", nilDB: true, wantErr: true},
		{name: "empty id", id: "", status: "merged", chunkStatus: "pr_opened", wantErr: true},
		{name: "eligible merge stamps write time", id: "c1", status: "merged", chunkStatus: "pr_opened", wantStampNow: true},
		{name: "merge keeps existing stamp", id: "c1", status: "merged", chunkStatus: "pr_opened", initial: mergeBase, wantStamp: mergeBase},
		{name: "deploying keeps merge stamp", id: "c1", status: "deploying", chunkStatus: "merged", initial: mergeBase, wantStamp: mergeBase},
		{name: "deploying never fabricates stamp", id: "c1", status: "deploying", chunkStatus: "merged"},
		{name: "other status clears stamp", id: "c1", status: "conflict", chunkStatus: "merged", initial: mergeBase},
		{name: "terminal chunk", id: "c1", status: "merged", chunkStatus: "completed", wantErr: true, wantSkipped: true},
		{name: "persistence failure", id: "c1", status: "merged", chunkStatus: "pr_opened", sabotage: cfFailChunkWrite, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := cfChunk(tc.chunkStatus)
			chunk.CompletedAt = tc.initial
			st, pool := cfOpen(t, tc.nilDB, chunk, tc.sabotage)
			before := cfDBNow(t, pool)
			err := st.UpdatePRLifecycleStatus(context.Background(), tc.id, tc.status)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if tc.wantErr {
				return
			}
			got := loadChunk(t, pool, "c1")
			assert.Equal(t, tc.status, got.Status)
			if tc.wantStampNow {
				// The stamp is the write transaction's database clock, so it
				// falls between database now() read before and after the call.
				after := cfDBNow(t, pool)
				assert.False(t, got.CompletedAt.Before(before) || got.CompletedAt.After(after), "completed_at = %v, want within [%v, %v]", got.CompletedAt, before, after)
				return
			}
			assert.True(t, got.CompletedAt.Equal(tc.wantStamp), "completed_at = %v, want %v", got.CompletedAt, tc.wantStamp)
		})
	}
}

func TestIncrementChunkCIFailCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID, chunkStatus, sabotage string
		headSHA                              []string
		nilDB                                bool
		initialCount, wantCount              int
		wantErr                              bool
		wantErrIs                            error
		wantRemediatedSHA                    string
	}{
		{name: "nil database", chunkID: "c1", nilDB: true, wantErr: true},
		{name: "empty id", chunkID: "", chunkStatus: "pr_opened", wantErr: true},
		{name: "successful increment without sha", chunkID: "c1", chunkStatus: "pr_opened", initialCount: 1, wantCount: 2},
		{name: "successful increment with sha", chunkID: "c1", chunkStatus: "ci_failed", headSHA: []string{"sha1234"}, initialCount: 2, wantCount: 3, wantRemediatedSHA: "sha1234"},
		{name: "skipped terminal transition", chunkID: "c1", chunkStatus: "merged", headSHA: []string{"sha1234"}, wantErr: true, wantErrIs: ErrPRTransitionSkipped},
		{name: "query error", chunkID: "c1", chunkStatus: "pr_opened", sabotage: cfFailChunkWrite, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := cfChunk(tc.chunkStatus)
			chunk.CIFailCount = tc.initialCount
			st, pool := cfOpen(t, tc.nilDB, chunk, tc.sabotage)
			count, err := st.IncrementChunkCIFailCount(context.Background(), tc.chunkID, "check failed", tc.headSHA...)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			if tc.wantErrIs != nil {
				assert.ErrorIs(t, err, tc.wantErrIs)
			}
			if tc.wantErr {
				return
			}
			assert.Equal(t, tc.wantCount, count)
			got := loadChunk(t, pool, "c1")
			assert.Equal(t, tc.wantCount, got.CIFailCount)
			assert.Equal(t, "ci_failed", got.Status)
			assert.Equal(t, "check failed", got.Banner)
			assert.Equal(t, tc.wantRemediatedSHA, got.RemediatedSHA)
		})
	}
}

// cfDeployCase is one row of the deploy-completion tables: the stored chunk
// status, the evidence supplied and the rollout jobs stored afterwards.
type cfDeployCase struct {
	name, chunkID, chunkStatus, sabotage string
	nilDB, evidence                      bool
	wantErr, wantSkipped                 bool
	wantJobs                             []string
	// wantStatus is the chunk status stored afterwards ("" skips the check).
	wantStatus string
}

func TestCompletePRDeployment(t *testing.T) {
	t.Parallel()
	tests := []cfDeployCase{
		{name: "nil database", chunkID: "c1", nilDB: true, wantErr: true},
		{name: "empty id", chunkID: "", chunkStatus: "deploying", wantErr: true, wantStatus: "deploying"},
		{name: "rejected update", chunkID: "c1", chunkStatus: "cancelled", wantErr: true, wantSkipped: true, wantStatus: "cancelled"},
		{name: "nil evidence", chunkID: "c1", chunkStatus: "deploying", wantJobs: []string{"seeded"}, wantStatus: "completed"},
		{name: "supplied evidence", chunkID: "c1", chunkStatus: "deploying", evidence: true, wantJobs: []string{"service"}, wantStatus: "completed"},
		{name: "persistence failure", chunkID: "c1", chunkStatus: "deploying", evidence: true, sabotage: cfFailChunkWrite, wantErr: true, wantStatus: "deploying"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := cfChunk(tc.chunkStatus)
			chunk.Banner = "deploying"
			chunk.RolloutState = &pubsub.PRRolloutState{ExpectedJobs: []string{"seeded"}}
			st, pool := cfOpen(t, tc.nilDB, chunk, tc.sabotage)
			var evidence *pubsub.PRRolloutState
			if tc.evidence {
				evidence = &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			}
			err := st.CompletePRDeployment(context.Background(), tc.chunkID, evidence)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if tc.wantStatus != "" {
				assert.Equal(t, tc.wantStatus, loadChunk(t, pool, "c1").Status)
			}
			if tc.wantErr {
				return
			}
			got := loadChunk(t, pool, "c1")
			assert.Empty(t, got.Banner)
			assert.False(t, got.CompletedAt.IsZero(), "completed_at not stamped")
			require.NotNil(t, got.RolloutState)
			assert.Equal(t, tc.wantJobs, got.RolloutState.ExpectedJobs)
		})
	}
}

func TestMarkActionRepairDeployed(t *testing.T) {
	t.Parallel()
	tests := []cfDeployCase{
		{name: "nil database", chunkID: "c1", nilDB: true, wantErr: true},
		{name: "empty id", chunkID: " ", chunkStatus: "deploying", wantErr: true},
		{name: "chunk outside repair stage skipped", chunkID: "c1", chunkStatus: "running", wantErr: true, wantSkipped: true},
		{name: "nil evidence", chunkID: "c1", chunkStatus: "deploying", wantJobs: []string{"seeded"}},
		{name: "supplied evidence", chunkID: " c1 ", chunkStatus: "pr_opened", evidence: true, wantJobs: []string{"service"}},
		{name: "persistence failure", chunkID: "c1", chunkStatus: "deploying", evidence: true, sabotage: cfFailChunkWrite, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Repo: "aerial", PRNumber: 7, Status: tc.chunkStatus, Banner: "repairing", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"seeded"}}}
			st, pool := cfOpen(t, tc.nilDB, chunk, tc.sabotage)
			var evidence *pubsub.PRRolloutState
			if tc.evidence {
				evidence = &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			}
			err := st.MarkActionRepairDeployed(context.Background(), tc.chunkID, evidence)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if tc.wantErr {
				return
			}
			got := loadChunk(t, pool, "c1")
			assert.Equal(t, "running", got.Status)
			assert.Equal(t, 7, got.PRNumber)
			assert.Equal(t, "aerial", got.Repo)
			assert.Empty(t, got.Banner)
			require.NotNil(t, got.RolloutState)
			assert.Equal(t, tc.wantJobs, got.RolloutState.ExpectedJobs)
		})
	}
}

func TestActionRepairInFlight(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		want  bool
	}{
		{"opened repair", model.VanadisChunk{Mode: "action", Status: "pr_opened", PRNumber: 7}, true},
		{"deploying repair", model.VanadisChunk{Mode: "action", Status: "deploying", PRNumber: 7}, true},
		{"failed ci repair", model.VanadisChunk{Mode: "action", Status: "ci_failed", PRNumber: 7}, true},
		{"deployed repair back to running", model.VanadisChunk{Mode: "action", Status: "running", PRNumber: 7}, false},
		{"plain running action", model.VanadisChunk{Mode: "action", Status: "running"}, false},
		{"pr chunk deploying", model.VanadisChunk{Mode: "pr", Status: "deploying", PRNumber: 7}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ActionRepairInFlight(tc.chunk))
		})
	}
}

func TestPostgresVanadisStore_RecordPRCheckStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                  string
		chunkID, headSHA      string
		checkName, checkState string
		chunkStatus, sabotage string
		nilDB                 bool
		wantErr, wantSkipped  bool
	}{
		{name: "nil database", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", nilDB: true, wantErr: true},
		{name: "empty chunk id", chunkID: "", headSHA: "sha-1", checkName: "test", checkState: "passed", chunkStatus: "pr_opened", wantErr: true},
		{name: "empty check name", chunkID: "c1", headSHA: "sha-1", checkName: "", checkState: "passed", chunkStatus: "pr_opened", wantErr: true},
		{name: "record passed status", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", chunkStatus: "pr_opened"},
		{name: "skipped transition rejected", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", chunkStatus: "deploying", wantErr: true, wantSkipped: true},
		{name: "persistence failure", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", chunkStatus: "pr_opened", sabotage: cfFailChunkWrite, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := cfOpen(t, tc.nilDB, cfChunk(tc.chunkStatus), tc.sabotage)
			err := st.RecordPRCheckStatus(context.Background(), tc.chunkID, tc.headSHA, tc.checkName, tc.checkState)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if tc.wantErr {
				return
			}
			got := loadChunk(t, pool, "c1")
			assert.Equal(t, map[string]string{tc.checkName: tc.checkState}, got.CIChecks)
			assert.Equal(t, tc.headSHA, got.HeadSHA)
		})
	}
}

func TestPostgresVanadisStore_ResetPRCheckState(t *testing.T) {
	t.Parallel()
	failedCI := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", PRNumber: 12, Status: "ci_failed", Banner: "CI failed", CIChecks: map[string]string{"lint": "failed"}}
	merged := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", PRNumber: 12, Status: "merged", Banner: "merged", HeadSHA: "sha-1", MergeSHA: "merge-sha-1", CIChecks: map[string]string{"build": "passed"}}
	tests := []struct {
		name                 string
		chunkID, headSHA     string
		mergeSHA, sabotage   string
		chunk                model.VanadisChunk
		nilDB                bool
		wantErr, wantSkipped bool
		wantStatus           string
		wantHead, wantMerge  string
		wantChecks           map[string]string
	}{
		{name: "nil database", chunkID: "c1", headSHA: "sha-1", nilDB: true, wantErr: true},
		{name: "empty chunk id", chunkID: "", headSHA: "sha-1", chunk: failedCI, wantErr: true},
		{name: "reset sync check state", chunkID: "c1", headSHA: "sha-2", chunk: failedCI, wantStatus: "pr_opened", wantHead: "sha-2"},
		{name: "reset merge check state", chunkID: "c1", headSHA: "sha-2", mergeSHA: "merge-sha-1", chunk: merged, wantStatus: "merged", wantHead: "sha-2", wantMerge: "merge-sha-1", wantChecks: map[string]string{"build": "passed"}},
		{name: "rejected sync check state", chunkID: "c1", headSHA: "sha-2", chunk: cfChunk("deploying"), wantErr: true, wantSkipped: true},
		{name: "rejected merge check state", chunkID: "c1", headSHA: "sha-2", mergeSHA: "merge-sha-1", chunk: cfChunk("completed"), wantErr: true, wantSkipped: true},
		{name: "sync check state failure", chunkID: "c1", headSHA: "sha-2", chunk: failedCI, sabotage: cfFailChunkWrite, wantErr: true},
		{name: "merge check state failure", chunkID: "c1", headSHA: "sha-2", mergeSHA: "merge-sha-1", chunk: merged, sabotage: cfFailChunkWrite, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := cfOpen(t, tc.nilDB, tc.chunk, tc.sabotage)
			err := st.ResetPRCheckState(context.Background(), tc.chunkID, tc.headSHA, tc.mergeSHA)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if tc.wantErr {
				return
			}
			got := loadChunk(t, pool, "c1")
			assert.Equal(t, tc.wantStatus, got.Status)
			assert.Equal(t, tc.wantHead, got.HeadSHA)
			assert.Equal(t, tc.wantMerge, got.MergeSHA)
			assert.Equal(t, tc.wantChecks, got.CIChecks)
			assert.Empty(t, got.Banner)
		})
	}
}

// cfDBNow reads the database clock (nil pool rows never get here).
func cfDBNow(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	if pool == nil {
		return time.Time{}
	}
	var now time.Time
	if err := pool.QueryRow(context.Background(), `SELECT now()`).Scan(&now); err != nil {
		t.Fatalf("read database clock: %v", err)
	}
	return now
}
