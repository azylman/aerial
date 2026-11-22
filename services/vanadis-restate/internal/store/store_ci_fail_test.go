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

	// Nil store
	sNil := &Store{}
	assert.Error(t, sNil.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, sNil.ResetPRCheckState(context.Background(), "c1", "sha-1", ""))

	// Empty arguments
	st, _ := open(dbScript{ExecTag: "UPDATE 1"})
	assert.Error(t, st.RecordPRCheckStatus(context.Background(), "", "sha-1", "test", "passed"))
	assert.Error(t, st.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "", "passed"))
	assert.Error(t, st.ResetPRCheckState(context.Background(), "", "sha-1", ""))

	// Success
	assert.NoError(t, st.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.NoError(t, st.ResetPRCheckState(context.Background(), "c1", "sha-2", ""))
	assert.NoError(t, st.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"))

	// Skipped transition (0 rows affected)
	storeZero, _ := open(dbScript{ExecTag: "UPDATE 0", Row: sqltest.ErrRow(pgx.ErrNoRows)})
	assert.ErrorIs(t, storeZero.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"), ErrPRTransitionSkipped)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "sha-2", ""), ErrPRTransitionSkipped)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"), ErrPRTransitionSkipped)

	// DB Exec error
	storeErr, _ := open(dbScript{ExecErr: errors.New("db error")})
	assert.Error(t, storeErr.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "sha-2", ""))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"))
}
