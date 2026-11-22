package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

// lifecycleStampFake simulates the completed_at CASE of the lifecycle status
// write: merged stamps an unknown merge time, deploying preserves the stamp
// without ever creating one, and every other status clears it.
type lifecycleStampFake struct {
	stamp     time.Time
	write     time.Time
	query     string
	tag       string
	failWrite bool
}

func (f *lifecycleStampFake) exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if f.failWrite {
		return pgconn.CommandTag{}, errors.New("database unavailable")
	}
	f.query = query
	if status, ok := args[0].(string); ok {
		switch status {
		case "merged":
			if f.stamp.IsZero() {
				f.stamp = f.write
			}
		case "deploying":
			// Preserve, never create: an unknown merge stamp stays unknown.
		default:
			f.stamp = time.Time{}
		}
	}
	return pgconn.NewCommandTag(f.tag), nil
}

func TestUpdatePRLifecycleStatus(t *testing.T) {
	t.Parallel()
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	writeStamp := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name, id, status, tag, wantSQL string
		nilDB, execFailure             bool
		initial, wantStamp             time.Time
		wantErr                        bool
	}{
		{name: "nil database", id: "c1", status: "merged", tag: "UPDATE 1", nilDB: true, wantErr: true},
		{name: "empty id", id: "", status: "merged", tag: "UPDATE 1", wantErr: true},
		{name: "eligible merge stamps write time", id: "c1", status: "merged", tag: "UPDATE 1", wantSQL: "WHEN $1 = 'merged' THEN COALESCE(completed_at", wantStamp: writeStamp},
		{name: "merge keeps existing stamp", id: "c1", status: "merged", tag: "UPDATE 1", wantSQL: "WHEN $1 = 'merged' THEN COALESCE(completed_at", initial: mergeBase, wantStamp: mergeBase},
		{name: "deploying keeps merge stamp", id: "c1", status: "deploying", tag: "UPDATE 1", wantSQL: "WHEN $1 = 'deploying' THEN completed_at", initial: mergeBase, wantStamp: mergeBase},
		{name: "deploying never fabricates stamp", id: "c1", status: "deploying", tag: "UPDATE 1", wantSQL: "WHEN $1 = 'deploying' THEN completed_at", wantStamp: time.Time{}},
		{name: "other status clears stamp", id: "c1", status: "conflict", tag: "UPDATE 1", wantSQL: "ELSE NULL END", initial: mergeBase, wantStamp: time.Time{}},
		{name: "terminal chunk", id: "c1", status: "merged", tag: "UPDATE 0", wantErr: true},
		{name: "persistence failure", id: "c1", status: "merged", tag: "", execFailure: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &lifecycleStampFake{stamp: tc.initial, write: writeStamp, tag: tc.tag, failWrite: tc.execFailure}
			st := New(sqltest.DB{ExecFn: fake.exec}, "example-org")
			if tc.nilDB {
				st = &Store{}
			}
			err := st.UpdatePRLifecycleStatus(context.Background(), tc.id, tc.status)
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.tag == "UPDATE 0" {
				assert.ErrorIs(t, err, ErrPRTransitionSkipped)
			}
			if tc.wantErr {
				return
			}
			assert.True(t, fake.stamp.Equal(tc.wantStamp), "completed_at = %v, want %v", fake.stamp, tc.wantStamp)
			if tc.wantSQL != "" {
				assert.Contains(t, fake.query, tc.wantSQL)
			}
		})
	}
}

func TestIncrementChunkCIFailCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID string
		headSHA       []string
		nilDB         bool
		row           pgx.Row
		wantCount     int
		wantErr       bool
		wantErrIs     error
		wantArgSHA    string
	}{
		{"nil database", "c1", nil, true, nil, 0, true, nil, ""},
		{"empty id", "", nil, false, nil, 0, true, nil, ""},
		{"successful increment without sha", "c1", nil, false, sqltest.ValuesRow(2), 2, false, nil, ""},
		{"successful increment with sha", "c1", []string{"sha1234"}, false, sqltest.ValuesRow(3), 3, false, nil, "sha1234"},
		{"skipped terminal transition", "c1", []string{"sha1234"}, false, sqltest.ErrRow(pgx.ErrNoRows), 0, true, ErrPRTransitionSkipped, ""},
		{"query error", "c1", nil, false, sqltest.ErrRow(errors.New("db error")), 0, true, nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var capturedArgs []any
			st := New(sqltest.DB{QueryRowFn: func(_ context.Context, _ string, args ...any) pgx.Row {
				capturedArgs = args
				if tc.row != nil {
					return tc.row
				}
				return sqltest.ValuesRow(1)
			}}, "example-org")
			if tc.nilDB {
				st = &Store{}
			}
			count, err := st.IncrementChunkCIFailCount(context.Background(), tc.chunkID, "check failed", tc.headSHA...)
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.wantErrIs != nil {
				assert.ErrorIs(t, err, tc.wantErrIs)
			}
			if !tc.wantErr {
				assert.Equal(t, tc.wantCount, count)
				assert.Equal(t, tc.wantArgSHA, capturedArgs[2])
			}
		})
	}
}

func TestCompletePRDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID                string
		nilDB, evidence, execFailure bool
		wantErr                      bool
		rejected                     bool
	}{
		{"nil database", "c1", true, false, false, true, false},
		{"empty id", "", false, false, false, true, false},
		{"rejected update", "c1", false, false, false, true, true},
		{"nil evidence", "c1", false, false, false, false, false},
		{"supplied evidence", "c1", false, true, false, false, false},
		{"persistence failure", "c1", false, true, true, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotArgs []any
			st := New(sqltest.DB{ExecFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				gotArgs = args
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				if tc.rejected {
					return pgconn.NewCommandTag("UPDATE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}, "example-org")
			if tc.nilDB {
				st = &Store{}
			}
			var evidence *pubsub.PRRolloutState
			if tc.evidence {
				evidence = &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			}
			err := st.CompletePRDeployment(context.Background(), tc.chunkID, evidence)
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.wantErr {
				return
			}

			if tc.evidence {
				data, ok := gotArgs[0].([]byte)
				assert.True(t, ok)
				assert.Contains(t, string(data), "service")
			} else {
				assert.Nil(t, gotArgs[0])
			}
		})
	}
}

func TestMarkActionRepairDeployed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID                string
		nilDB, evidence, execFailure bool
		rejected                     bool
		wantErr, wantSkipped         bool
	}{
		{"nil database", "c1", true, false, false, false, true, false},
		{"empty id", " ", false, false, false, false, true, false},
		{"chunk outside repair stage skipped", "c1", false, false, false, true, true, true},
		{"nil evidence", "c1", false, false, false, false, false, false},
		{"supplied evidence", " c1 ", false, true, false, false, false, false},
		{"persistence failure", "c1", false, true, true, false, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotArgs []any
			st := New(sqltest.DB{ExecFn: func(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
				gotArgs = args
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				if tc.rejected {
					return pgconn.NewCommandTag("UPDATE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}, "example-org")
			if tc.nilDB {
				st = &Store{}
			}
			var evidence *pubsub.PRRolloutState
			if tc.evidence {
				evidence = &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			}
			err := st.MarkActionRepairDeployed(context.Background(), tc.chunkID, evidence)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if tc.wantErr {
				return
			}
			assert.Equal(t, "c1", gotArgs[2])
			if tc.evidence {
				data, ok := gotArgs[0].([]byte)
				assert.True(t, ok)
				assert.Contains(t, string(data), "service")
			} else {
				assert.Nil(t, gotArgs[0])
			}
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
		nilDB, execFailure    bool
		rejected              bool
		wantErr, wantSkipped  bool
	}{
		{name: "nil database", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", nilDB: true, wantErr: true},
		{name: "empty chunk id", chunkID: "", headSHA: "sha-1", checkName: "test", checkState: "passed", wantErr: true},
		{name: "empty check name", chunkID: "c1", headSHA: "sha-1", checkName: "", checkState: "passed", wantErr: true},
		{name: "record passed status", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed"},
		{name: "skipped transition rejected", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", rejected: true, wantErr: true, wantSkipped: true},
		{name: "persistence failure", chunkID: "c1", headSHA: "sha-1", checkName: "test", checkState: "passed", execFailure: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotQuery string
			st := New(sqltest.DB{ExecFn: func(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
				gotQuery = query
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				if tc.rejected {
					return pgconn.NewCommandTag("UPDATE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}, "example-org")
			if tc.nilDB {
				st = &Store{}
			}
			err := st.RecordPRCheckStatus(context.Background(), tc.chunkID, tc.headSHA, tc.checkName, tc.checkState)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
			if !tc.wantErr {
				assert.Contains(t, gotQuery, "$2 != 'failed'")
				assert.Contains(t, gotQuery, "ci_checks->>'_merge_sha' = $4")
				assert.NotContains(t, gotQuery, "'{_merge_sha}'")
			}
		})
	}
}

func TestPostgresVanadisStore_ResetPRCheckState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		chunkID, headSHA     string
		mergeSHA             string
		nilDB, execFailure   bool
		rejected             bool
		wantErr, wantSkipped bool
	}{
		{name: "nil database", chunkID: "c1", headSHA: "sha-1", nilDB: true, wantErr: true},
		{name: "empty chunk id", chunkID: "", headSHA: "sha-1", wantErr: true},
		{name: "reset sync check state", chunkID: "c1", headSHA: "sha-2"},
		{name: "reset merge check state", chunkID: "c1", headSHA: "sha-2", mergeSHA: "merge-sha-1"},
		{name: "rejected sync check state", chunkID: "c1", headSHA: "sha-2", rejected: true, wantErr: true, wantSkipped: true},
		{name: "rejected merge check state", chunkID: "c1", headSHA: "sha-2", mergeSHA: "merge-sha-1", rejected: true, wantErr: true, wantSkipped: true},
		{name: "sync check state failure", chunkID: "c1", headSHA: "sha-2", execFailure: true, wantErr: true},
		{name: "merge check state failure", chunkID: "c1", headSHA: "sha-2", mergeSHA: "merge-sha-1", execFailure: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New(sqltest.DB{
				ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
					if tc.execFailure {
						return pgconn.CommandTag{}, errors.New("database unavailable")
					}
					if tc.rejected {
						return pgconn.NewCommandTag("UPDATE 0"), nil
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
				QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
					return sqltest.ErrRow(pgx.ErrNoRows)
				},
			}, "example-org")
			if tc.nilDB {
				st = &Store{}
			}
			err := st.ResetPRCheckState(context.Background(), tc.chunkID, tc.headSHA, tc.mergeSHA)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantSkipped, errors.Is(err, ErrPRTransitionSkipped))
		})
	}
}

func TestDeployFailedEarlierDeploymentSweepIDs(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	later := base.Add(time.Hour)
	pr := func(id string, idx int, status string, at time.Time) model.VanadisChunk {
		return sweepChunk(id, idx, "pr", "aerial", status, at)
	}
	dep := func(idx int, at time.Time) model.VanadisChunk { return pr("deploying", idx, "deploying", at) }
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		want   []string
	}{
		{
			name:   "earlier deploy_failed chunk with valid merge timestamp is swept",
			chunks: mutateChunks(pr("failed-1", 0, "deploy_failed", base), dep(1, later)),
			want:   []string{"failed-1"},
		},
		{
			name:   "earlier deploy_failed chunk without merge timestamp is skipped",
			chunks: mutateChunks(pr("failed-1", 0, "deploy_failed", time.Time{}), dep(1, later)),
			want:   nil,
		},
		{
			name:   "later deploy_failed chunk after deploying is untouched",
			chunks: mutateChunks(pr("failed-1", 0, "deploy_failed", later.Add(time.Hour)), dep(1, later)),
			want:   nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := EarlierDeploymentSweepIDs("example-org", tc.chunks, "deploying")
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDeployFailedCrossBuildDeploymentSweepIDs(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	later := base.Add(time.Hour)
	eri := []string{"eri"}
	deploying := xbuildChunk("deploying", "b1", 2, "pr", "aerial", "deploying", 7, later, eri, nil)
	cand := func(buildID, status string, at time.Time, jobs []string) model.VanadisChunk {
		return xbuildChunk("f1", buildID, 0, "pr", "aerial", status, 5, at, jobs, nil)
	}
	tests := []struct {
		name       string
		deploying  model.VanadisChunk
		candidates []model.VanadisChunk
		want       []SweptChunk
	}{
		{
			name:       "cross-build deploy_failed chunk with valid merge timestamp is swept",
			deploying:  deploying,
			candidates: mutateChunks(cand("b2", "deploy_failed", base, eri)),
			want:       []SweptChunk{{ChunkID: "f1", BuildID: "b2"}},
		},
		{
			name:       "cross-build deploy_failed chunk without merge timestamp is skipped",
			deploying:  deploying,
			candidates: mutateChunks(cand("b2", "deploy_failed", time.Time{}, eri)),
			want:       nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CrossBuildDeploymentSweepIDs("example-org", tc.deploying, tc.candidates)
			assert.Equal(t, tc.want, got)
		})
	}
}
