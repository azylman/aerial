package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgresVanadisStore_GetChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		bare           *bareStore
		repo           string
		prNumber       int
		rowErr         error
		wantErr        bool
		wantLookup     VanadisChunkPRLookup
		wantCandidates []string
	}{
		{name: "nil receiver errors", bare: nilReceiver, repo: "aerial", prNumber: 1, wantErr: true},
		{name: "nil db errors", bare: nilDB, repo: "aerial", prNumber: 1, wantErr: true},
		{name: "invalid args return unmatched", repo: "", prNumber: 0, wantLookup: VanadisChunkPRLookup{}},
		{name: "no rows returns unmatched", repo: "aerial", prNumber: 101, rowErr: pgx.ErrNoRows, wantLookup: VanadisChunkPRLookup{}, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "chunk query error propagates", repo: "aerial", prNumber: 101, rowErr: errors.New("db error"), wantErr: true, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "successful match returns lookup", repo: "aerial", prNumber: 101, wantLookup: VanadisChunkPRLookup{Matched: true, ChunkID: "c1", BuildID: "b1", Effort: "low"}, wantCandidates: []string{"aerial", "azylman/aerial"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotCandidates []string
			s := newMockStore(mockDB{
				queryRowFn: func(_ context.Context, _ string, args ...any) pgx.Row {
					gotCandidates, _ = args[0].([]string)
					return &genericMockRow{scanFn: func(dest ...any) error {
						if tc.rowErr != nil {
							return tc.rowErr
						}
						return scanValues(dest, "c1", "b1", 0, "low", true)
					}}
				},
			})
			if tc.bare != nil {
				s = tc.bare.store
			}
			got, err := s.GetChunkByPR(context.Background(), tc.repo, tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetChunkByPR() err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && *got != tc.wantLookup {
				t.Errorf("GetChunkByPR() = %+v, want %+v", *got, tc.wantLookup)
			}
			if !slices.Equal(gotCandidates, tc.wantCandidates) {
				t.Errorf("GetChunkByPR candidates = %v, want %v", gotCandidates, tc.wantCandidates)
			}
		})
	}
}
