package db

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// testEngineInfer is a deterministic stand-in for transcript.InferEngine.
func testEngineInfer(names []string) string {
	switch {
	case slices.Contains(names, "Bash"):
		return "claude"
	case slices.Contains(names, "run_command"):
		return "agy"
	default:
		return ""
	}
}

// testKindMapper is a deterministic stand-in for transcript.NormalizeToolKind.
func testKindMapper(name string) string {
	switch name {
	case "Bash", "run_command":
		return "shell"
	case "unmapped_tool":
		return ""
	default:
		return "other"
	}
}

func TestResolveTranscriptStack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		databaseURL     string
		configuredStack string
		want            string
		wantErr         string
	}{
		{name: "empty stack derives from database url path", databaseURL: "postgres://u:p@h:5432/alpha?sslmode=disable", want: "alpha"},
		{name: "empty stack derives from sqlite memory dsn", databaseURL: ":memory:", want: "memory"},
		{name: "empty stack derives from file path dsn", databaseURL: "/tmp/fixture/local.db", want: "local"},
		{name: "explicit stack overrides database url path", databaseURL: "postgres://u:p@h:5432/beta", configuredStack: " custom ", want: "custom"},
		{name: "explicit stack without database path", databaseURL: "postgres://u:p@h:5432", configuredStack: "solo", want: "solo"},
		{name: "empty url and empty stack errors", databaseURL: "  ", wantErr: "database URL cannot be empty"},
		{name: "unparseable url errors", databaseURL: "postgres://u:p@h:bad port/alpha", wantErr: "parse database URL"},
		{name: "no database name and empty stack errors", databaseURL: "postgres://u:p@h:5432", wantErr: "cannot derive stack"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveTranscriptStack(tc.databaseURL, tc.configuredStack)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}

// newSharedStackPair returns two SQL transcript stores for different stacks sharing one fixture DB.
func newSharedStackPair(t *testing.T) (shared *sql.DB, a, b TranscriptStore) {
	t.Helper()
	shared = initTestSQLiteDB(t)
	a = NewSQLTranscriptStore(shared, "stack_a")
	b = NewSQLTranscriptStore(shared, "stack_b")
	return shared, a, b
}

// seedSharedStacks writes the same session ID from both stacks plus a stack-local session each.
func seedSharedStacks(t *testing.T, a, b TranscriptStore) {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, w := range []struct {
		store   TranscriptStore
		session string
		engine  string
		tool    string
	}{
		{a, "dup-sess", "agy", "run_command"},
		{a, "a-only", "", "Bash"},
		{b, "dup-sess", "claude", "Bash"},
		{b, "b-only", "claude", "unmapped_tool"},
	} {
		if err := w.store.UpsertSessionSummary(ctx, SessionSummary{SessionID: w.session, Engine: w.engine, Summary: "deploy " + w.session, LastIndexedStep: 1, LastMtime: base}); err != nil {
			t.Fatalf("seed upsert %s: %v", w.session, err)
		}
		steps := []TranscriptStep{
			{SessionID: w.session, StepIndex: 0, StepType: "user_message", Content: "deploy please", CreatedAt: base},
			{SessionID: w.session, StepIndex: 1, StepType: "tool_call", ToolName: w.tool, ToolCallID: "call-" + w.session, Content: "kubectl apply", CreatedAt: base.Add(time.Minute)},
		}
		if err := w.store.BatchInsertTranscriptSteps(ctx, steps); err != nil {
			t.Fatalf("seed steps %s: %v", w.session, err)
		}
	}
}

func TestSharedTranscripts_SyncStatesScopedToStack(t *testing.T) {
	_, a, b := newSharedStackPair(t)
	seedSharedStacks(t, a, b)
	tests := []struct {
		name        string
		store       TranscriptStore
		wantEngines map[string]string
	}{
		{name: "stack a sees only its sessions", store: a, wantEngines: map[string]string{"dup-sess": "agy", "a-only": ""}},
		{name: "stack b sees only its sessions", store: b, wantEngines: map[string]string{"dup-sess": "claude", "b-only": "claude"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			states, err := tc.store.GetSessionSyncStates(context.Background())
			if err != nil {
				t.Fatalf("GetSessionSyncStates: %v", err)
			}
			got := make(map[string]string, len(states))
			for id, st := range states {
				got[id] = st.Engine
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.wantEngines) {
				t.Errorf("engines = %v, want %v", got, tc.wantEngines)
			}
		})
	}
}

func TestSharedTranscripts_SearchStepsSpanStacks(t *testing.T) {
	_, a, b := newSharedStackPair(t)
	seedSharedStacks(t, a, b)
	tests := []struct {
		name       string
		reader     TranscriptStore
		stack      string
		session    string
		wantStacks []string
	}{
		{name: "unfiltered read from a sees both stacks", reader: a, session: "dup-sess", wantStacks: []string{"stack_a", "stack_a", "stack_b", "stack_b"}},
		{name: "unfiltered read from b sees both stacks", reader: b, session: "dup-sess,a-only", wantStacks: []string{"stack_a", "stack_a", "stack_a", "stack_a", "stack_b", "stack_b"}},
		{name: "stack filter narrows to one stack", reader: a, stack: "stack_b", session: "dup-sess,b-only", wantStacks: []string{"stack_b", "stack_b", "stack_b", "stack_b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			steps, err := tc.reader.SearchTranscriptSteps(context.Background(), tc.stack, "", tc.session, "", 50)
			if err != nil {
				t.Fatalf("SearchTranscriptSteps: %v", err)
			}
			var gotStacks []string
			for _, st := range steps {
				gotStacks = append(gotStacks, st.Stack)
			}
			sort.Strings(gotStacks)
			if !slices.Equal(gotStacks, tc.wantStacks) {
				t.Errorf("stacks=%v, want %v", gotStacks, tc.wantStacks)
			}
		})
	}
}

func TestSharedTranscripts_ToolFilterMatchesKindOrName(t *testing.T) {
	_, a, b := newSharedStackPair(t)
	seedSharedStacks(t, a, b)
	for _, w := range []TranscriptStore{a, b} {
		if _, err := w.BackfillToolKinds(context.Background(), testKindMapper); err != nil {
			t.Fatalf("BackfillToolKinds: %v", err)
		}
	}
	tests := []struct {
		name     string
		stack    string
		tool     string
		wantRows []string
	}{
		{name: "kind spans engines and stacks", tool: "shell", wantRows: []string{"stack_a/a-only/Bash", "stack_a/dup-sess/run_command", "stack_b/dup-sess/Bash"}},
		{name: "raw name matches only that tool", tool: "Bash", wantRows: []string{"stack_a/a-only/Bash", "stack_b/dup-sess/Bash"}},
		{name: "kind with stack filter", stack: "stack_a", tool: "shell", wantRows: []string{"stack_a/a-only/Bash", "stack_a/dup-sess/run_command"}},
		{name: "unmapped raw name still matches", tool: "unmapped_tool", wantRows: []string{"stack_b/b-only/unmapped_tool"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			steps, err := b.SearchTranscriptSteps(context.Background(), tc.stack, "", "", tc.tool, 50)
			if err != nil {
				t.Fatalf("SearchTranscriptSteps: %v", err)
			}
			var gotRows []string
			for _, st := range steps {
				gotRows = append(gotRows, fmt.Sprintf("%s/%s/%s", st.Stack, st.SessionID, st.ToolName))
			}
			sort.Strings(gotRows)
			if !slices.Equal(gotRows, tc.wantRows) {
				t.Errorf("rows=%v, want %v", gotRows, tc.wantRows)
			}
		})
	}
}

func TestSharedTranscripts_StatsPerStack(t *testing.T) {
	_, a, b := newSharedStackPair(t)
	seedSharedStacks(t, a, b)
	tests := []struct {
		name      string
		stack     string
		wantStats []TranscriptStackStats
	}{
		{name: "empty stack counts every stack", wantStats: []TranscriptStackStats{{Stack: "stack_a", Sessions: 2, Steps: 4}, {Stack: "stack_b", Sessions: 2, Steps: 4}}},
		{name: "stack filter counts one stack", stack: "stack_b", wantStats: []TranscriptStackStats{{Stack: "stack_b", Sessions: 2, Steps: 4}}},
		{name: "unknown stack is empty", stack: "missing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := a.GetTranscriptStats(context.Background(), tc.stack)
			if err != nil {
				t.Fatalf("GetTranscriptStats: %v", err)
			}
			if !slices.Equal(got, tc.wantStats) {
				t.Errorf("stats=%+v, want %+v", got, tc.wantStats)
			}
		})
	}
}

func TestSharedTranscripts_StepRangeSpansStacks(t *testing.T) {
	_, a, b := newSharedStackPair(t)
	seedSharedStacks(t, a, b)
	tests := []struct {
		name        string
		stack       string
		session     string
		wantRows    []string
		wantEngines []string
	}{
		{name: "empty stack returns every stack's steps", session: "dup-sess", wantRows: []string{"stack_a/0", "stack_a/1", "stack_b/0", "stack_b/1"}, wantEngines: []string{"agy", "agy", "claude", "claude"}},
		{name: "stack filter returns one stack", stack: "stack_b", session: "dup-sess", wantRows: []string{"stack_b/0", "stack_b/1"}, wantEngines: []string{"claude", "claude"}},
		{name: "stack without the session is empty", stack: "stack_a", session: "b-only"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			steps, err := a.GetTranscriptStepRange(context.Background(), tc.stack, tc.session, 0, 5)
			if err != nil {
				t.Fatalf("GetTranscriptStepRange: %v", err)
			}
			var gotRows, gotEngines []string
			for _, st := range steps {
				gotRows = append(gotRows, fmt.Sprintf("%s/%d", st.Stack, st.StepIndex))
				gotEngines = append(gotEngines, st.Engine)
			}
			if !slices.Equal(gotRows, tc.wantRows) || !slices.Equal(gotEngines, tc.wantEngines) {
				t.Errorf("rows=%v engines=%v, want rows=%v engines=%v", gotRows, gotEngines, tc.wantRows, tc.wantEngines)
			}
		})
	}
}

func TestSharedTranscripts_BackfillToolKindsScopedToStack(t *testing.T) {
	tests := []struct {
		name         string
		writerStack  string
		batchSize    int
		extraSteps   int
		wantUpdated  int64
		wantKinds    map[string]string
		wantUnmapped map[string]string
	}{
		{name: "backfill from a leaves b untouched", writerStack: "stack_a", batchSize: 2000, wantUpdated: 2, wantKinds: map[string]string{"stack_a/dup-sess": "shell|call-dup-sess", "stack_a/a-only": "shell|call-a-only", "stack_b/dup-sess": "|call-dup-sess", "stack_b/b-only": "|call-b-only"}, wantUnmapped: map[string]string{"stack_b/unmapped": "2"}},
		{name: "backfill from b skips unmapped names", writerStack: "stack_b", batchSize: 2000, wantUpdated: 1, wantKinds: map[string]string{"stack_a/dup-sess": "|call-dup-sess", "stack_a/a-only": "|call-a-only", "stack_b/dup-sess": "shell|call-dup-sess", "stack_b/b-only": "|call-b-only"}, wantUnmapped: map[string]string{"stack_a/unmapped": "2", "stack_b/unmapped": "1"}},
		{name: "small batches drain every local row and leave b untouched", writerStack: "stack_a", batchSize: 2, extraSteps: 5, wantUpdated: 7, wantKinds: map[string]string{"stack_a/dup-sess": "shell|call-dup-sess", "stack_a/a-only": "shell|call-a-only", "stack_b/dup-sess": "|call-dup-sess", "stack_b/b-only": "|call-b-only"}, wantUnmapped: map[string]string{"stack_b/unmapped": "2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shared, a, b := newSharedStackPair(t)
			seedSharedStacks(t, a, b)
			base := time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC)
			extra := make([]TranscriptStep, 0, tc.extraSteps)
			for i := range tc.extraSteps {
				extra = append(extra, TranscriptStep{SessionID: "a-only", StepIndex: 2 + i, StepType: "tool_call", ToolName: "Bash", Content: "kubectl get pods", CreatedAt: base})
			}
			if err := a.BatchInsertTranscriptSteps(context.Background(), extra); err != nil {
				t.Fatalf("seed extra steps: %v", err)
			}
			updated, err := NewSQLTranscriptStore(shared, tc.writerStack).backfillToolKinds(context.Background(), testKindMapper, tc.batchSize, time.Minute)
			if err != nil || updated != tc.wantUpdated {
				t.Fatalf("updated=%d err=%v, want %d", updated, err, tc.wantUpdated)
			}
			got := readStackKeyed(t, shared, "SELECT agent, session_id, tool_kind || '|' || tool_call_id FROM transcript_steps WHERE step_index = 1")
			if fmt.Sprint(got) != fmt.Sprint(tc.wantKinds) {
				t.Errorf("kinds = %v, want %v", got, tc.wantKinds)
			}
			unmapped := readStackKeyed(t, shared, "SELECT agent, 'unmapped', COUNT(*) FROM transcript_steps WHERE tool_kind = '' AND tool_name <> '' GROUP BY agent")
			if fmt.Sprint(unmapped) != fmt.Sprint(tc.wantUnmapped) {
				t.Errorf("unmapped = %v, want %v", unmapped, tc.wantUnmapped)
			}
		})
	}
}

func TestSharedTranscripts_BackfillSessionEnginesScopedToStack(t *testing.T) {
	tests := []struct {
		name        string
		backfillB   bool
		wantUpdated int64
		wantEngines map[string]string
	}{
		{name: "backfill from a labels its unlabeled session only", wantUpdated: 1, wantEngines: map[string]string{"stack_a/a-only": "claude", "stack_a/dup-sess": "agy", "stack_b/dup-sess": "claude", "stack_b/b-only": "claude"}},
		{name: "backfill from b never touches a", backfillB: true, wantUpdated: 0, wantEngines: map[string]string{"stack_a/a-only": "", "stack_a/dup-sess": "agy", "stack_b/dup-sess": "claude", "stack_b/b-only": "claude"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shared, a, b := newSharedStackPair(t)
			seedSharedStacks(t, a, b)
			writer := a
			if tc.backfillB {
				writer = b
			}
			updated, err := writer.BackfillSessionEngines(context.Background(), testEngineInfer)
			if err != nil || updated != tc.wantUpdated {
				t.Fatalf("updated=%d err=%v, want %d", updated, err, tc.wantUpdated)
			}
			got := readStackKeyed(t, shared, "SELECT agent, session_id, engine FROM session_summaries")
			if fmt.Sprint(got) != fmt.Sprint(tc.wantEngines) {
				t.Errorf("engines = %v, want %v", got, tc.wantEngines)
			}
		})
	}
}

// readStackKeyed maps "stack/session_id" to the third selected column.
func readStackKeyed(t *testing.T, database *sql.DB, query string) map[string]string {
	t.Helper()
	rows, err := database.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var stack, session, value string
		if err := rows.Scan(&stack, &session, &value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[stack+"/"+session] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return got
}

// TestSQLTranscriptStore_ExactScanTx drives the PostgreSQL-only exact-scan transaction against the
// wire mock: BEGIN and SET LOCAL succeed over the simple protocol, so failures surface from the
// begin (cancelled context) or the query (client-side encode error) and the tx is rolled back.
func TestSQLTranscriptStore_ExactScanTx(t *testing.T) {
	addr := startMockPostgres(t)
	pg, err := Open(fmt.Sprintf("postgres://mock:mock@%s/stack_a?sslmode=disable", addr))
	if err != nil {
		t.Fatalf("Open mock postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })
	store := NewSQLTranscriptStore(pg, "stack_a")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		arg     any
		wantErr string
	}{
		{name: "begin failure", ctx: cancelled, arg: "x", wantErr: "begin exact-scan search tx"},
		{name: "query failure rolls back", ctx: context.Background(), arg: make(chan int), wantErr: "search session summaries query failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.querySessionSummaries(tc.ctx, true, "SELECT $1::text", tc.arg)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
