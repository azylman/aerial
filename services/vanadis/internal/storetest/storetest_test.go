package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/azylman/aerial/services/vanadis/internal/model"
)

var errStoreBoom = errors.New("boom")

// seededStore holds build b1 (chunk c1) and an orphan chunk c-orphan whose build is missing.
func seededStore(getErr, listErr error) *Store {
	s := New()
	s.Builds["b1"] = Build("b1", "100", "Build One", "obj", "running")
	s.Chunks["b1"] = []model.VanadisChunk{Chunk("c1", "b1", 0, "C1", "I1", "", "pending")}
	s.Chunks["b-gone"] = []model.VanadisChunk{Chunk("c-orphan", "b-gone", 0, "C", "I", "", "pending")}
	s.GetErr, s.ListErr = getErr, listErr
	return s
}

func TestStoreGetBuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		getErr     error
		id         string
		wantChunks []string
		wantErr    error
	}{
		{"found", nil, "b1", []string{"c1"}, nil},
		{"missing", nil, "nope", nil, model.ErrBuildNotFound},
		{"injected error", errStoreBoom, "b1", nil, errStoreBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, chunks, err := seededStore(tc.getErr, nil).GetBuild(context.Background(), tc.id)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				return
			}
			if b.ID != tc.id || !reflect.DeepEqual(chunkIDs(chunks), tc.wantChunks) {
				t.Fatalf("GetBuild(%q) = %q %v, want chunks %v", tc.id, b.ID, chunkIDs(chunks), tc.wantChunks)
			}
		})
	}
}

func TestStoreListBuilds(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		count   int
		limit   int
		listErr error
		wantLen int
		want    []string
		wantErr error
	}{
		{name: "newest first with ID tiebreak", count: 3, limit: 10, wantLen: 3, want: []string{"b2", "b1", "b0"}},
		{name: "limit caps the result", count: 3, limit: 2, wantLen: 2, want: []string{"b2", "b1"}},
		{name: "zero limit defaults to 50", count: 51, limit: 0, wantLen: 50},
		{name: "over 100 defaults to 50", count: 51, limit: 101, wantLen: 50},
		{name: "injected error", listErr: errStoreBoom, wantErr: errStoreBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			s.ListErr = tc.listErr
			for i := range tc.count {
				b := Build(fmt.Sprintf("b%d", i), "100", "T", "obj", "running")
				b.CreatedAt = base.Add(time.Duration(i/2) * time.Hour)
				s.Builds[b.ID] = b
			}
			entries, err := s.ListBuilds(context.Background(), tc.limit)
			if !errors.Is(err, tc.wantErr) || len(entries) != tc.wantLen {
				t.Fatalf("ListBuilds = %d entries, %v; want %d, %v", len(entries), err, tc.wantLen, tc.wantErr)
			}
			if tc.want == nil {
				return
			}
			got := make([]string, len(entries))
			for i, e := range entries {
				got[i] = e.ID
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ListBuilds = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStoreGetChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		getErr    error
		id        string
		wantBuild string
		wantErr   error
	}{
		{"found", nil, "c1", "b1", nil},
		{"chunk missing", nil, "nope", "", model.ErrChunkNotFound},
		{"build missing", nil, "c-orphan", "", model.ErrBuildNotFound},
		{"injected error", errStoreBoom, "c1", "", errStoreBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, b, err := seededStore(tc.getErr, nil).GetChunk(context.Background(), tc.id)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (c.ID != tc.id || b.ID != tc.wantBuild) {
				t.Fatalf("GetChunk(%q) = %q in %q, want build %q", tc.id, c.ID, b.ID, tc.wantBuild)
			}
		})
	}
}

func chunkIDs(chunks []model.VanadisChunk) []string {
	if chunks == nil {
		return nil
	}
	ids := make([]string, len(chunks))
	for i, c := range chunks {
		ids[i] = c.ID
	}
	return ids
}
