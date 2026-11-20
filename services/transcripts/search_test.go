package main

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/transcriptwire"
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
			if _, err := store.db.ExecContext(ctx, `UPDATE session_summaries SET summary = 'debugged indexer' WHERE agent = 'aerial' AND session_id = 'sess-a'`); err != nil {
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

func TestBuildStepsQuery(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		stack          string
		query          string
		session        string
		tool           string
		limit          int
		useFTS         bool
		since          time.Time
		wantContains   string
		wantNotContain string
		wantArgs       []any
	}{
		{"sqlite keeps like scan", "", "hello", "", "", 20, false, time.Time{}, "t.content LIKE '%' || $1 || '%'", "fts_tokens", []any{"hello", 20}},
		{"postgres uses fts index", "", "hello", "", "", 20, true, time.Time{}, "t.fts_tokens @@ websearch_to_tsquery('simple', $1)", "LIKE", []any{"hello", 20}},
		{"postgres keeps filter placeholders", "aerial", "hello", "sess-a", "bash", 10, true, time.Time{}, "t.agent = $2 AND t.session_id = $3", "LIKE", []any{"hello", "aerial", "sess-a", "bash", 10}},
		{"comma-separated sessions in IN clause", "", "hello", "sess-a,sess-b", "", 20, false, time.Time{}, "t.session_id IN ($2, $3)", "fts_tokens", []any{"hello", "sess-a", "sess-b", 20}},
		{"since clause added", "", "hello", "", "", 20, false, epoch, "t.created_at >= $2", "fts_tokens", []any{"hello", epoch, 20}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, args := buildStepsQuery(tc.stack, tc.query, tc.session, tc.tool, tc.limit, tc.useFTS, tc.since)
			if !strings.Contains(got, tc.wantContains) {
				t.Errorf("buildStepsQuery() = %q, want it to contain %q", got, tc.wantContains)
			}
			if tc.wantNotContain != "" && strings.Contains(got, tc.wantNotContain) {
				t.Errorf("buildStepsQuery() = %q, want it to omit %q", got, tc.wantNotContain)
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Errorf("buildStepsQuery() args = %v, want %v", args, tc.wantArgs)
			}
		})
	}
}

func TestSearchStepsNilDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		query   string
		session string
		tool    string
		wantErr bool
	}{
		{"nil db content query errors", "hello", "", "", true},
		{"nil db session filter errors", "", "sess-a", "", true},
		{"nil db tool filter errors", "", "", "bash", true},
		{"nil db empty filters returns nothing", "", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewStore(nil, "aerial")
			got, err := store.SearchSteps(context.Background(), "", tc.query, tc.session, tc.tool, 20)
			if tc.wantErr && err == nil {
				t.Errorf("SearchSteps() err = nil, want database nil error")
			}
			if !tc.wantErr && (err != nil || got != nil) {
				t.Errorf("SearchSteps() = %v, %v, want nil, nil", got, err)
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
	if _, err := collectReadRows(ctx, store, "op", `SELECT agent, session_id, last_indexed_step, last_mtime FROM session_summaries`, failingRowScan); err == nil {
		t.Errorf("expected scan error")
	}
}

func TestGetWatermark(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 5, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tests := []struct {
		name      string
		nilDB     bool
		stack     string
		sessionID string
		wantStep  int
		wantErr   bool
	}{
		{"existing session matching stack", false, "aerial", "sess-a", 5, false},
		{"existing session empty stack", false, "", "sess-a", 5, false},
		{"wrong stack returns not found", false, "other", "sess-a", -1, false},
		{"missing session returns not found", false, "aerial", "missing", -1, false},
		{"empty session returns not found", false, "aerial", "", -1, false},
		{"nil db returns error", true, "aerial", "sess-a", -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := store
			if tc.nilDB {
				target = NewStore(nil, "aerial")
			}
			got, err := target.GetWatermark(ctx, tc.stack, tc.sessionID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetWatermark() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.wantStep {
				t.Errorf("GetWatermark() = %d, want %d", got, tc.wantStep)
			}
		})
	}
}

func TestGetThreadTrajectory(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-traj", epoch, 0, 2, false, false)
	batch.ThreadID = "th-traj-1"
	batch.Steps = []transcriptwire.TranscriptStep{
		{
			StepIndex: 0,
			Type:      "USER_INPUT",
			Content:   "<retrieved_memory>\n- [id:42] User likes matcha\n</retrieved_memory>",
		},
		{
			StepIndex: 1,
			Type:      "PLANNER_RESPONSE",
			ToolName:  "read_file",
			Content:   `{"path": "/test/file.go"}`,
		},
		{
			StepIndex: 2,
			Type:      "TOOL",
			ToolName:  "read_file",
			Content:   "file contents",
		},
	}
	if _, err := store.ApplyBatch(ctx, batch); err != nil {
		t.Fatalf("seed ApplyBatch: %v", err)
	}

	tests := []struct {
		name        string
		stack       string
		threadID    string
		limit       int
		wantFactSub string
		wantToolSub string
		wantFactLen int
		wantToolLen int
	}{
		{"trajectory extracted for thread", "aerial", "th-traj-1", 10, "[id:42] User likes matcha", "read_file", 1, 2},
		{"trajectory limit zero defaults", "aerial", "th-traj-1", 0, "[id:42] User likes matcha", "read_file", 1, 2},
		{"empty thread returns empty", "aerial", "", 10, "", "", 0, 0},
		{"missing thread returns empty", "aerial", "missing", 10, "", "", 0, 0},
		{"wrong stack returns empty", "other", "th-traj-1", 10, "", "", 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			facts, tools, err := store.GetThreadTrajectory(ctx, tc.stack, tc.threadID, tc.limit)
			if err != nil {
				t.Fatalf("GetThreadTrajectory() err = %v", err)
			}
			if len(facts) != tc.wantFactLen {
				t.Errorf("facts len = %d, want %d", len(facts), tc.wantFactLen)
			}
			if tc.wantFactSub != "" && (len(facts) == 0 || !strings.Contains(facts[0], tc.wantFactSub)) {
				t.Errorf("facts = %v, want substring %q", facts, tc.wantFactSub)
			}
			if len(tools) != tc.wantToolLen {
				t.Errorf("tools len = %d, want %d", len(tools), tc.wantToolLen)
			}
			if tc.wantToolSub != "" && (len(tools) == 0 || !strings.Contains(tools[0], tc.wantToolSub)) {
				t.Errorf("tools = %v, want substring %q", tools, tc.wantToolSub)
			}
		})
	}
}

func TestTrajectoryFormatting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		stepType string
		toolName string
		content  string
		want     string
	}{
		{"empty all", "", "", "", ""},
		{"tool only without content", "", "bash", "", "bash"},
		{"step type only without content", "PLANNER_RESPONSE", "", "", "PLANNER_RESPONSE"},
		{"tool and step type with content", "PLANNER_RESPONSE", "bash", "echo hi", "bash/PLANNER_RESPONSE: echo hi"},
		{"tool and content", "", "bash", "echo hi", "bash: echo hi"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatTrajectoryToolCall(tc.stepType, tc.toolName, tc.content); got != tc.want {
				t.Errorf("formatTrajectoryToolCall(%q, %q, %q) = %q, want %q", tc.stepType, tc.toolName, tc.content, got, tc.want)
			}
		})
	}
}

func TestTruncateSingleLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		s      string
		maxLen int
		want   string
	}{
		{"maxLen negative", "hello world", -1, "hello world"},
		{"longer than maxLen", "hello world", 5, "hello"},
		{"shorter than maxLen", "hello", 10, "hello"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := truncateSingleLine(tc.s, tc.maxLen); got != tc.want {
				t.Errorf("truncateSingleLine(%q, %d) = %q, want %q", tc.s, tc.maxLen, got, tc.want)
			}
		})
	}
}
