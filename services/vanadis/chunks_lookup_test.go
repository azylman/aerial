package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestNextChunkModeAfter(t *testing.T) {
	t.Parallel()
	sum := func(id string, idx int, mode, status string) VanadisChunkSummary {
		return VanadisChunkSummary{ID: id, ChunkIndex: idx, Mode: mode, Status: status}
	}
	tests := []struct {
		name        string
		chunks      []VanadisChunkSummary
		doneChunkID string
		doneIdx     int
		want        string
	}{
		{name: "next chunk is a pr", chunks: []VanadisChunkSummary{sum("c0", 0, "pr", "pr_opened"), sum("c1", 1, "pr", "pending")}, doneChunkID: "c0", doneIdx: 0, want: "pr"},
		{name: "next chunk is an action", chunks: []VanadisChunkSummary{sum("c0", 0, "pr", "pr_opened"), sum("c1", 1, "action", "pending")}, doneChunkID: "c0", doneIdx: 0, want: "action"},
		{name: "last chunk leaves nothing", chunks: []VanadisChunkSummary{sum("c0", 0, "pr", "merged"), sum("c1", 1, "pr", "pr_opened")}, doneChunkID: "c1", doneIdx: 1, want: ""},
		{name: "skips done chunks to the nearest undone later chunk", chunks: []VanadisChunkSummary{sum("c0", 0, "pr", "pr_opened"), sum("c1", 1, "pr", "merged"), sum("c2", 2, "action", "pending"), sum("c3", 3, "pr", "pending")}, doneChunkID: "c0", doneIdx: 0, want: "action"},
		{name: "falls back to earlier undone chunk", chunks: []VanadisChunkSummary{sum("c0", 0, "action", "failed"), sum("c1", 1, "pr", "pr_opened")}, doneChunkID: "c1", doneIdx: 1, want: "action"},
		{name: "matched chunk counts as done even when unmerged", chunks: []VanadisChunkSummary{sum("c0", 0, "pr", "pr_opened")}, doneChunkID: "c0", doneIdx: 0, want: ""},
		{name: "empty chunk list", chunks: nil, doneChunkID: "c0", doneIdx: 0, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := slices.Clone(tc.chunks)
			if got := nextChunkModeAfter(tc.chunks, tc.doneChunkID, tc.doneIdx); got != tc.want {
				t.Errorf("nextChunkModeAfter() = %q, want %q", got, tc.want)
			}
			if !slices.Equal(before, tc.chunks) {
				t.Errorf("nextChunkModeAfter mutated input: %+v", tc.chunks)
			}
		})
	}
}

func TestPostgresVanadisStore_GetChunkByPR(t *testing.T) {
	t.Parallel()
	actionChunk := func(id string, idx int, status string) VanadisChunk {
		c := mkChunk(id, "b1", idx, id, "", "", status)
		c.Mode = "action"
		return c
	}
	prChunk := func(id string, idx int, status string) VanadisChunk {
		return mkChunk(id, "b1", idx, id, "", "", status)
	}
	tests := []struct {
		name           string
		repo           string
		prNumber       int
		rowErr         error
		chunks         []VanadisChunk
		chunksQueryErr error
		chunksScanErr  error
		wantErr        bool
		wantLookup     VanadisChunkPRLookup
		wantCandidates []string
	}{
		{name: "invalid args return unmatched", repo: "", prNumber: 0, wantLookup: VanadisChunkPRLookup{}},
		{name: "no rows returns unmatched", repo: "aerial", prNumber: 101, rowErr: pgx.ErrNoRows, wantLookup: VanadisChunkPRLookup{}, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "chunk query error propagates", repo: "aerial", prNumber: 101, rowErr: errors.New("db error"), wantErr: true, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "build chunks query error propagates", repo: "aerial", prNumber: 101, chunksQueryErr: errors.New("chunks query error"), wantErr: true, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "build chunks scan error propagates", repo: "aerial", prNumber: 101, chunks: []VanadisChunk{prChunk("c1", 0, "pr_opened")}, chunksScanErr: errors.New("scan error"), wantErr: true, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "next chunk is an action", repo: "aerial", prNumber: 101, chunks: []VanadisChunk{prChunk("c1", 0, "pr_opened"), actionChunk("c2", 1, "pending")}, wantLookup: VanadisChunkPRLookup{Matched: true, ChunkID: "c1", BuildID: "b1", Effort: "low", HasRemainingChunks: true, NextChunkMode: "action"}, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "next chunk is a pr", repo: "azylman/aerial", prNumber: 101, chunks: []VanadisChunk{prChunk("c1", 0, "pr_opened"), prChunk("c2", 1, "pending")}, wantLookup: VanadisChunkPRLookup{Matched: true, ChunkID: "c1", BuildID: "b1", Effort: "low", HasRemainingChunks: true, NextChunkMode: "pr"}, wantCandidates: []string{"azylman/aerial", "aerial"}},
		{name: "last chunk reports no next mode", repo: "aerial", prNumber: 101, chunks: []VanadisChunk{prChunk("c1", 0, "pr_opened")}, wantLookup: VanadisChunkPRLookup{Matched: true, ChunkID: "c1", BuildID: "b1", Effort: "low", HasRemainingChunks: true}, wantCandidates: []string{"aerial", "azylman/aerial"}},
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
						*dest[0].(*string), *dest[1].(*string), *dest[2].(*int), *dest[3].(*string), *dest[4].(*bool) = "c1", "b1", 0, "low", true
						return nil
					}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksQueryErr != nil {
						return nil, tc.chunksQueryErr
					}
					return &mockRows{items: tc.chunks, scanErr: tc.chunksScanErr}, nil
				},
			})
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
