package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresStore_GetBuild_And_ListBuilds_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// GetBuild nil store and empty ID
	var nilStore *PostgresVanadisStore
	if _, _, err := nilStore.GetBuild(ctx, "b1"); err == nil {
		t.Fatal("expected error on nil store GetBuild")
	}
	s := newMockStore(mockDB{})
	if _, _, err := s.GetBuild(ctx, ""); err == nil {
		t.Fatal("expected error on empty build ID")
	}

	// GetBuild queryRow ErrNoRows
	sNoBuild := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{err: pgx.ErrNoRows}
		},
	})
	if _, _, err := sNoBuild.GetBuild(ctx, "b1"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}

	// GetBuild chunks query error
	sChunksErr := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build"}}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return nil, errors.New("chunks query error")
		},
	})
	if _, _, err := sChunksErr.GetBuild(ctx, "b1"); err == nil {
		t.Fatal("expected error on chunks query error")
	}

	// GetBuild success with scanChunks
	sSuccess := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build"}}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: []VanadisChunk{
				{ID: "c1", BuildID: "b1", Title: "Chunk 1", StartedAt: time.Now(), CompletedAt: time.Now()},
			}}, nil
		},
	})
	b, chs, err := sSuccess.GetBuild(ctx, "b1")
	if err != nil || b == nil || len(chs) != 1 {
		t.Fatalf("unexpected GetBuild result: b=%+v chs=%+v err=%v", b, chs, err)
	}
	if chs[0].CIChecks == nil || chs[0].RolloutState == nil {
		t.Fatalf("expected CIChecks and RolloutState to be unmarshaled, got: %+v", chs[0])
	}

	// ListBuilds nil store
	if _, err = nilStore.ListBuilds(ctx, 10); err == nil {
		t.Fatal("expected error on nil store ListBuilds")
	}

	// ListBuilds query builds error
	sListErr := newMockStore(mockDB{
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return nil, errors.New("query builds error")
		},
	})
	if _, err = sListErr.ListBuilds(ctx, 0); err == nil {
		t.Fatal("expected error on query builds error")
	}

	// ListBuilds empty (0 builds)
	sListEmpty := newMockStore(mockDB{
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{builds: []VanadisBuild{}}, nil
		},
	})
	entries, err := sListEmpty.ListBuilds(ctx, 50)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected empty entries, got %v err %v", entries, err)
	}

	// ListBuilds chunks query error
	sListChunksErr := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "FROM vanadis_builds") {
				return &mockRows{builds: []VanadisBuild{{ID: "b1", Status: "running"}}}, nil
			}
			return nil, errors.New("query chunks error")
		},
	})
	if _, err = sListChunksErr.ListBuilds(ctx, 50); err == nil {
		t.Fatal("expected error on chunks query error in ListBuilds")
	}

	// ListBuilds success
	sListSuccess := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "FROM vanadis_builds") {
				return &mockRows{builds: []VanadisBuild{
					{ID: "b1", Title: "Build 1", Status: "running"},
					{ID: "b2", Title: "Build 2", Status: "completed"},
				}}, nil
			}
			return &mockRows{items: []VanadisChunk{
				{ID: "c1", BuildID: "b1", Title: "Chunk 1", Status: "running"},
				{ID: "c2", BuildID: "b2", Title: "Chunk 2", Status: "completed"},
			}}, nil
		},
	})
	allEntries, err := sListSuccess.ListBuilds(ctx, 50)
	if err != nil || len(allEntries) != 2 {
		t.Fatalf("expected 2 entries, got %v err %v", allEntries, err)
	}
}
