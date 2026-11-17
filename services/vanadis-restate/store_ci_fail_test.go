package main

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

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
			store := newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				return pgconn.NewCommandTag(tc.tag), nil
			}})
			if tc.nilDB {
				store = &PostgresVanadisStore{}
			}
			err := store.UpdatePRLifecycleStatus(context.Background(), tc.id, "merged")
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.tag == "UPDATE 0" {
				assert.ErrorIs(t, err, errPRTransitionSkipped)
			}
		})
	}
}

func TestIncrementChunkCIFailCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID string
		nilDB         bool
		row           pgx.Row
		wantCount     int
		wantErr       bool
		wantErrIs     error
	}{
		{"nil database", "c1", true, nil, 0, true, nil},
		{"empty id", "", false, nil, 0, true, nil},
		{"successful increment", "c1", false, valuesRow(2), 2, false, nil},
		{"skipped terminal transition", "c1", false, errRow(pgx.ErrNoRows), 0, true, errPRTransitionSkipped},
		{"query error", "c1", false, errRow(errors.New("db error")), 0, true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				if tc.row != nil {
					return tc.row
				}
				return valuesRow(1)
			}})
			if tc.nilDB {
				store = &PostgresVanadisStore{}
			}
			count, err := store.IncrementChunkCIFailCount(context.Background(), tc.chunkID, "check failed")
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.wantErrIs != nil {
				assert.ErrorIs(t, err, tc.wantErrIs)
			}
			if !tc.wantErr {
				assert.Equal(t, tc.wantCount, count)
			}
		})
	}
}
