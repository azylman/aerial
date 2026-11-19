package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestSearchSessions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		stack     string
		query     string
		wantCount int
	}{
		{"empty stack searches all stacks", "", "debug", 1},
		{"scoped stack searches own stack", "aerial", "debug", 1},
		{"unknown stack returns nothing", "nope", "debug", 0},
		{"empty query returns nothing", "aerial", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
			if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if _, err := store.db.ExecContext(ctx, `UPDATE session_summaries SET summary = 'debugged indexer' WHERE stack = 'aerial' AND session_id = 'sess-a'`); err != nil {
				t.Fatalf("seed summary text: %v", err)
			}
			got, err := store.SearchSessions(ctx, tc.stack, tc.query, 20)
			if err != nil {
				t.Fatalf("SearchSessions() err = %v", err)
			}
			if len(got) != tc.wantCount {
				t.Errorf("SearchSessions() = %d rows, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestSearchSteps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		stack     string
		query     string
		session   string
		tool      string
		wantCount int
	}{
		{"empty stack matches all stacks", "", "", "sess-a", "", 3},
		{"scoped stack matches own steps", "aerial", "", "sess-a", "", 3},
		{"wrong stack matches nothing", "other", "", "sess-a", "", 0},
		{"content query matches", "", "content", "", "", 3},
		{"all empty returns nothing", "", "", "", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
			if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			got, err := store.SearchSteps(ctx, tc.stack, tc.query, tc.session, tc.tool, 20)
			if err != nil {
				t.Fatalf("SearchSteps() err = %v", err)
			}
			if len(got) != tc.wantCount {
				t.Errorf("SearchSteps() = %d rows, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestGetSteps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		stack     string
		session   string
		start     int
		end       int
		wantCount int
	}{
		{"empty stack reads across stacks", "", "sess-a", 0, 10, 3},
		{"scoped stack reads own steps", "aerial", "sess-a", 0, 10, 3},
		{"range clips steps", "aerial", "sess-a", 1, 2, 2},
		{"empty session returns nothing", "aerial", "", 0, 10, 0},
		{"unknown session returns nothing", "aerial", "missing", 0, 10, 0},
		{"inverted range clamps to empty", "aerial", "sess-a", 5, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
			if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			got, err := store.GetSteps(ctx, tc.stack, tc.session, tc.start, tc.end)
			if err != nil {
				t.Fatalf("GetSteps() err = %v", err)
			}
			if len(got) != tc.wantCount {
				t.Errorf("GetSteps() = %d rows, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestGetStatsAndWatermarks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		filter         string
		wantStacks     int
		wantWatermarks int
	}{
		{"empty stack covers all stacks", "", 1, 1},
		{"scoped stack covers one stack", "aerial", 1, 1},
		{"unknown stack covers nothing", "nope", 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
			if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			stats, err := store.GetStats(ctx, tc.filter)
			if err != nil {
				t.Fatalf("GetStats() err = %v", err)
			}
			if len(stats) != tc.wantStacks {
				t.Errorf("GetStats() = %d stacks, want %d", len(stats), tc.wantStacks)
			}
			if tc.wantStacks > 0 && stats[0].Steps != 3 {
				t.Errorf("stats steps = %d, want 3", stats[0].Steps)
			}
			marks, err := store.GetWatermarks(ctx, tc.filter)
			if err != nil {
				t.Fatalf("GetWatermarks() err = %v", err)
			}
			if len(marks) != tc.wantWatermarks {
				t.Errorf("GetWatermarks() = %d rows, want %d", len(marks), tc.wantWatermarks)
			}
			if tc.wantWatermarks > 0 && marks[0].LastIndexedStep != 2 {
				t.Errorf("watermark step = %d, want 2", marks[0].LastIndexedStep)
			}
		})
	}
}

func TestRowScanErrors(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	if _, err := store.ApplyBatch(ctx, testBatch("sess-a", time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC), 0, 1, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	badRows := func(t *testing.T) *sql.Rows {
		t.Helper()
		rows, err := store.db.QueryContext(ctx, `SELECT 1`)
		if err != nil {
			t.Fatalf("bad rows query: %v", err)
		}
		return rows
	}
	if _, err := scanSessionResult(badRows(t)); err == nil {
		t.Errorf("expected scan session error")
	}
	if _, err := scanStepResult(badRows(t)); err == nil {
		t.Errorf("expected scan step error")
	}
	if _, err := scanStackStat(badRows(t)); err == nil {
		t.Errorf("expected scan stats error")
	}
	if _, err := scanSessionWatermark(badRows(t)); err == nil {
		t.Errorf("expected scan watermark error")
	}
	if _, err := collectReadRows(ctx, NewStore(nil, "aerial"), "op", "SELECT 1", scanStackStat); err == nil {
		t.Errorf("expected collect error on nil db")
	}
}

func TestSearchStepsToolFilter(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-a", epoch, 0, 2, false, false)
	batch.Steps[0].ToolName = "bash"
	if _, err := store.ApplyBatch(ctx, batch); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := store.SearchSteps(ctx, "", "", "", "bash", 20)
	if err != nil {
		t.Fatalf("SearchSteps() err = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("SearchSteps(tool=bash) = %d rows, want 1", len(got))
	}
	got, err = store.SearchSteps(ctx, "", "", "", "bash", 0)
	if err != nil {
		t.Fatalf("SearchSteps() err = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("SearchSteps(limit=0) = %d rows, want default-limit 1", len(got))
	}
}

func TestSearchSessionsDefaultLimit(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE session_summaries SET summary = 'debugged' WHERE session_id = 'sess-a'`); err != nil {
		t.Fatalf("seed summary: %v", err)
	}
	got, err := store.SearchSessions(ctx, "", "debug", 0)
	if err != nil {
		t.Fatalf("SearchSessions() err = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("SearchSessions(limit=0) = %d rows, want 1", len(got))
	}
}

func TestGetStepsNegativeStart(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := store.GetSteps(ctx, "aerial", "sess-a", -5, 2)
	if err != nil {
		t.Fatalf("GetSteps() err = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("GetSteps(negative start) = %d rows, want 3", len(got))
	}
}

func TestPostgresDriverClause(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgx", "postgres://127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open pgx: %v", err)
	}
	defer db.Close()
	if !isPostgresDriver(db.Driver()) {
		t.Errorf("expected pgx stdlib driver to match")
	}
	if got := forUpdateClause(db); got != " FOR UPDATE" {
		t.Errorf("forUpdateClause = %q, want FOR UPDATE", got)
	}
	if isPostgresDriver(nil) {
		t.Errorf("nil driver must not match")
	}
}

func failingRowScan(*sql.Rows) (StackStats, error) {
	return StackStats{}, errors.New("scan boom")
}

func TestCollectReadRowsScanError(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 0, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := collectReadRows(ctx, store, "op", `SELECT stack, session_id, last_indexed_step, last_mtime FROM session_summaries`, failingRowScan); err == nil {
		t.Errorf("expected scan error")
	}
}
