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
)

func buildRow(id, status string) pgx.Row {
	return valuesRow(id, "thread", 3, "title", "objective", "", status, "", false, time.Time{}, time.Time{})
}

func TestUpdatePRLifecycleStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, id, tag      string
		nilDB, execFailure bool
		wantErr            bool
	}{
		{"nil database", "c1", "UPDATE 1", true, false, true},
		{"empty id", "", "UPDATE 1", false, false, true},
		{"eligible merge", "c1", "UPDATE 1", false, false, false},
		{"terminal chunk", "c1", "UPDATE 0", false, false, true},
		{"persistence failure", "c1", "", false, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				return pgconn.NewCommandTag(tc.tag), nil
			}})
			if tc.nilDB {
				st = &Store{}
			}
			err := st.UpdatePRLifecycleStatus(context.Background(), tc.id, "merged")
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.tag == "UPDATE 0" {
				assert.ErrorIs(t, err, ErrPRTransitionSkipped)
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
		{"successful increment without sha", "c1", nil, false, valuesRow(2), 2, false, nil, ""},
		{"successful increment with sha", "c1", []string{"sha1234"}, false, valuesRow(3), 3, false, nil, "sha1234"},
		{"skipped terminal transition", "c1", []string{"sha1234"}, false, errRow(pgx.ErrNoRows), 0, true, ErrPRTransitionSkipped, ""},
		{"query error", "c1", nil, false, errRow(errors.New("db error")), 0, true, nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var capturedArgs []any
			st := New(mockDB{queryRowFn: func(_ context.Context, _ string, args ...any) pgx.Row {
				capturedArgs = args
				if tc.row != nil {
					return tc.row
				}
				return valuesRow(1)
			}})
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
			st := New(mockDB{execFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				gotArgs = args
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				if tc.rejected {
					return pgconn.NewCommandTag("UPDATE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}})
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

func TestPostgresVanadisStore_RecordPRCheckStatus(t *testing.T) {
	t.Parallel()

	// Nil store
	sNil := &Store{}
	assert.Error(t, sNil.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, sNil.ResetPRCheckState(context.Background(), "c1", "sha-1", ""))

	// Empty arguments
	st, _ := dbScript{execTag: "UPDATE 1"}.open()
	assert.Error(t, st.RecordPRCheckStatus(context.Background(), "", "sha-1", "test", "passed"))
	assert.Error(t, st.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "", "passed"))
	assert.Error(t, st.ResetPRCheckState(context.Background(), "", "sha-1", ""))

	// Success
	assert.NoError(t, st.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.NoError(t, st.ResetPRCheckState(context.Background(), "c1", "sha-2", ""))
	assert.NoError(t, st.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"))

	// Skipped transition (0 rows affected)
	storeZero, _ := dbScript{execTag: "UPDATE 0", row: errRow(pgx.ErrNoRows)}.open()
	assert.ErrorIs(t, storeZero.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"), ErrPRTransitionSkipped)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "sha-2", ""), ErrPRTransitionSkipped)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"), ErrPRTransitionSkipped)

	// DB Exec error
	storeErr, _ := dbScript{execErr: errors.New("db error")}.open()
	assert.Error(t, storeErr.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "sha-2", ""))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"))
}
