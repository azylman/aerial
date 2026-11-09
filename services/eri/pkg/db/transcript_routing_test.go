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

	"github.com/azylman/aerial/services/eri/pkg/config"
)

const (
	testDSNStackA = "postgres://aerial@pg.internal:5432/stack_a?sslmode=disable"
	testDSNStackB = "postgres://aerial@pg.internal:5432/stack_b?sslmode=disable"
)

// testOpener serves pre-opened fixture pools by DSN and counts opens.
type testOpener struct {
	pools map[string]*sql.DB
	opens int
}

func (o *testOpener) open(dsn string) (*sql.DB, error) {
	pool, ok := o.pools[dsn]
	if !ok {
		return nil, fmt.Errorf("unknown dsn %s", dsn)
	}
	o.opens++
	return pool, nil
}

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

func TestResolveTranscriptTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		databaseURL string
		tc          config.TranscriptsConfig
		want        TranscriptTarget
		wantErr     string
	}{
		{name: "no block keeps own db and stack", databaseURL: "postgres://u:p@h:5432/alpha?sslmode=disable", want: TranscriptTarget{DSN: "postgres://u:p@h:5432/alpha?sslmode=disable", Stack: "alpha"}},
		{name: "shared database swaps path and keeps own stack", databaseURL: "postgres://u:p@h:5432/beta?sslmode=disable", tc: config.TranscriptsConfig{Database: "alpha"}, want: TranscriptTarget{DSN: "postgres://u:p@h:5432/alpha?sslmode=disable", Stack: "beta"}},
		{name: "explicit stack overrides db name", databaseURL: "postgres://u:p@h:5432/beta", tc: config.TranscriptsConfig{Database: "alpha", Stack: " custom "}, want: TranscriptTarget{DSN: "postgres://u:p@h:5432/alpha", Stack: "custom"}},
		{name: "database equal to own name keeps dsn verbatim", databaseURL: "postgres://u:p@h:5432/alpha?sslmode=disable", tc: config.TranscriptsConfig{Database: "alpha"}, want: TranscriptTarget{DSN: "postgres://u:p@h:5432/alpha?sslmode=disable", Stack: "alpha"}},
		{name: "explicit stack without database path", databaseURL: "postgres://u:p@h:5432", tc: config.TranscriptsConfig{Stack: "solo"}, want: TranscriptTarget{DSN: "postgres://u:p@h:5432", Stack: "solo"}},
		{name: "empty url", databaseURL: "  ", wantErr: "database URL cannot be empty"},
		{name: "unparseable url", databaseURL: "postgres://u:p@h:bad port/alpha", wantErr: "parse database URL"},
		{name: "no database name and no stack", databaseURL: "postgres://u:p@h:5432", wantErr: "cannot derive stack"},
		{name: "schemeless dsn cannot swap", databaseURL: "/tmp/fixture/local.db", tc: config.TranscriptsConfig{Database: "alpha"}, wantErr: "must be a URL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveTranscriptTarget(tc.databaseURL, tc.tc)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %+v err=%v, want %+v", got, err, tc.want)
			}
		})
	}
}

// TestRoutedTranscriptStore_HotReload applies config snapshots in order to one routed store and
// asserts where each write lands, which stack labels it, and how many shared pools were opened.
func TestRoutedTranscriptStore_HotReload(t *testing.T) {
	primary, shared := initTestSQLiteDB(t), initTestSQLiteDB(t)
	opener := &testOpener{pools: map[string]*sql.DB{"postgres://aerial@pg.internal:5432/shared_tx?sslmode=disable": shared}}
	cfg := config.NewFromData(&config.ConfigData{DatabaseURL: testDSNStackA})
	store := NewRoutedTranscriptStore(cfg, primary, opener.open)
	dbs := map[string]*sql.DB{"primary": primary, "shared": shared}

	steps := []struct {
		name        string
		databaseURL string
		transcripts config.TranscriptsConfig
		wantDB      string
		wantStack   string
		wantOpens   int
		wantErr     string
	}{
		{name: "own db by default", databaseURL: testDSNStackA, wantDB: "primary", wantStack: "stack_a"},
		{name: "reload to shared db opens a pool", databaseURL: testDSNStackA, transcripts: config.TranscriptsConfig{Database: "shared_tx"}, wantDB: "shared", wantStack: "stack_a", wantOpens: 1},
		{name: "shared pool is cached", databaseURL: testDSNStackA, transcripts: config.TranscriptsConfig{Database: "shared_tx"}, wantDB: "shared", wantStack: "stack_a", wantOpens: 1},
		{name: "explicit stack label", databaseURL: testDSNStackA, transcripts: config.TranscriptsConfig{Database: "shared_tx", Stack: "custom"}, wantDB: "shared", wantStack: "custom", wantOpens: 1},
		{name: "naming own db reuses primary", databaseURL: testDSNStackA, transcripts: config.TranscriptsConfig{Database: "stack_a"}, wantDB: "primary", wantStack: "stack_a", wantOpens: 1},
		{name: "open failure surfaces", databaseURL: testDSNStackA, transcripts: config.TranscriptsConfig{Database: "missing"}, wantOpens: 1, wantErr: "open shared transcript database"},
		{name: "unresolvable target surfaces", databaseURL: "postgres://aerial@pg.internal:5432", wantOpens: 1, wantErr: "cannot derive stack"},
	}
	for i, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			cfg.Update(&config.ConfigData{DatabaseURL: st.databaseURL, Transcripts: st.transcripts})
			sessionID := fmt.Sprintf("sess-%d", i)
			err := store.UpsertSessionSummary(context.Background(), SessionSummary{SessionID: sessionID, Engine: "agy"})
			if opener.opens != st.wantOpens {
				t.Errorf("opens = %d, want %d", opener.opens, st.wantOpens)
			}
			if st.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), st.wantErr) {
					t.Fatalf("expected error containing %q, got %v", st.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("upsert failed: %v", err)
			}
			var gotStack string
			if err := dbs[st.wantDB].QueryRow("SELECT stack FROM session_summaries WHERE session_id = ?", sessionID).Scan(&gotStack); err != nil {
				t.Fatalf("row missing from %s db: %v", st.wantDB, err)
			}
			if gotStack != st.wantStack {
				t.Errorf("stack = %q, want %q", gotStack, st.wantStack)
			}
		})
	}

	closer, ok := store.(interface{ Close() error })
	if !ok {
		t.Fatal("routed store should implement Close")
	}
	if err := closer.Close(); err != nil {
		t.Errorf("Close returned %v", err)
	}
}

func TestRoutedTranscriptStore_Unavailable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr string
	}{
		{name: "nil config", wantErr: "config cannot be nil"},
		{name: "no opener for shared db", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: testDSNStackB, Transcripts: config.TranscriptsConfig{Database: "stack_a"}}), wantErr: "no opener configured"},
		{name: "nil primary pool", cfg: config.NewFromData(&config.ConfigData{DatabaseURL: testDSNStackB}), wantErr: ErrNilDatabase.Error()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRoutedTranscriptStore(tc.cfg, nil, nil)
			ctx := context.Background()
			errs := map[string]error{
				"GetSessionSyncStates":       c1(s.GetSessionSyncStates(ctx)),
				"UpsertSessionSummary":       s.UpsertSessionSummary(ctx, SessionSummary{SessionID: "s"}),
				"SetSessionEngine":           s.SetSessionEngine(ctx, "s", "agy"),
				"BatchInsertTranscriptSteps": s.BatchInsertTranscriptSteps(ctx, []TranscriptStep{{SessionID: "s"}}),
				"BackfillToolKinds":          c1(s.BackfillToolKinds(ctx, testKindMapper)),
				"BackfillSessionEngines":     c1(s.BackfillSessionEngines(ctx, testEngineInfer)),
				"PurgeMigrated":              c1(s.PurgeMigratedLocalTranscripts(ctx)),
				"SearchSessionSummaries":     c1(s.SearchSessionSummaries(ctx, "", nil, "q", 1, 0)),
				"SearchTranscriptSteps":      c1(s.SearchTranscriptSteps(ctx, "", "q", "", "", 1)),
				"GetTranscriptStepRange":     c1(s.GetTranscriptStepRange(ctx, "", "s", 0, 1)),
				"GetTranscriptStats":         c1(s.GetTranscriptStats(ctx, "")),
			}
			for op, err := range errs {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("%s: expected error containing %q, got %v", op, tc.wantErr, err)
				}
			}
		})
	}
}

// newSharedStackPair returns two routed stores for different stacks that share one fixture DB,
// mirroring two bots pointed at the same transcripts.database.
func newSharedStackPair(t *testing.T) (shared *sql.DB, a, b TranscriptStore) {
	t.Helper()
	shared = initTestSQLiteDB(t)
	opener := &testOpener{pools: map[string]*sql.DB{testDSNStackA: shared}}
	cfgA := config.NewFromData(&config.ConfigData{DatabaseURL: testDSNStackA})
	cfgB := config.NewFromData(&config.ConfigData{DatabaseURL: testDSNStackB, Transcripts: config.TranscriptsConfig{Database: "stack_a"}})
	a = NewRoutedTranscriptStore(cfgA, shared, opener.open)
	b = NewRoutedTranscriptStore(cfgB, initTestSQLiteDB(t), opener.open)
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
			got := readStackKeyed(t, shared, "SELECT stack, session_id, tool_kind || '|' || tool_call_id FROM transcript_steps WHERE step_index = 1")
			if fmt.Sprint(got) != fmt.Sprint(tc.wantKinds) {
				t.Errorf("kinds = %v, want %v", got, tc.wantKinds)
			}
			unmapped := readStackKeyed(t, shared, "SELECT stack, 'unmapped', COUNT(*) FROM transcript_steps WHERE tool_kind = '' AND tool_name <> '' GROUP BY stack")
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
			got := readStackKeyed(t, shared, "SELECT stack, session_id, engine FROM session_summaries")
			if fmt.Sprint(got) != fmt.Sprint(tc.wantEngines) {
				t.Errorf("engines = %v, want %v", got, tc.wantEngines)
			}
		})
	}
}

// TestSharedTranscripts_PurgeMigratedLocal seeds a primary DB holding stack_a rows written before the
// index moved to shared_tx, then purges: only stack_a sessions already in the shared index are deleted
// (with their steps), and the shared index itself is never modified.
func TestSharedTranscripts_PurgeMigratedLocal(t *testing.T) {
	const sharedDSN = "postgres://aerial@pg.internal:5432/shared_tx?sslmode=disable"
	allLocal := map[string]string{"stack_a/s1": "2", "stack_a/s2": "2", "stack_a/s3": "2", "stack_a/s4": "2", "stack_z/s1": "2"}
	survivors := map[string]string{"stack_a/s4": "2", "stack_z/s1": "2"}
	tests := []struct {
		name        string
		transcripts config.TranscriptsConfig
		batchSize   int
		budget      time.Duration
		breakLocal  string
		breakShared string
		wantDeleted int64
		wantErr     string
		wantLocal   map[string]string
	}{
		{name: "deletes only sessions present in the shared index", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, batchSize: 500, budget: time.Minute, wantDeleted: 3, wantLocal: survivors},
		{name: "small batches page through every local session", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, batchSize: 1, budget: time.Minute, wantDeleted: 3, wantLocal: survivors},
		{name: "index in primary db is a no-op", batchSize: 500, budget: time.Minute, wantLocal: allLocal},
		{name: "zero budget deletes nothing", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, batchSize: 500, wantLocal: allLocal},
		{name: "non-positive batch size rejected", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, budget: time.Minute, wantErr: "batch size must be positive"},
		{name: "unopenable shared index surfaces", transcripts: config.TranscriptsConfig{Database: "missing"}, batchSize: 500, budget: time.Minute, wantErr: "open shared transcript database"},
		{name: "failing delete returns wrapped error", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, batchSize: 500, budget: time.Minute, breakLocal: "DROP TABLE transcript_steps", wantErr: "purge migrated local transcripts: delete transcript steps"},
		{name: "failing local page query returns wrapped error", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, batchSize: 500, budget: time.Minute, breakLocal: "ALTER TABLE session_summaries RENAME TO gone", wantErr: "purge migrated local transcripts: query session ids"},
		{name: "failing shared lookup returns wrapped error", transcripts: config.TranscriptsConfig{Database: "shared_tx"}, batchSize: 500, budget: time.Minute, breakShared: "ALTER TABLE session_summaries RENAME TO gone", wantErr: "purge migrated local transcripts: query existing session ids"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			primary, shared := initTestSQLiteDB(t), initTestSQLiteDB(t)
			seedTranscriptSessions(t, primary, "stack_a", "s1", "s2", "s3", "s4")
			seedTranscriptSessions(t, primary, "stack_z", "s1")
			seedTranscriptSessions(t, shared, "stack_a", "s1", "s2", "s3")
			seedTranscriptSessions(t, shared, "stack_z", "s4")
			sharedBefore := readStackKeyed(t, shared, stepCountsQuery)
			for database, stmt := range map[*sql.DB]string{primary: tc.breakLocal, shared: tc.breakShared} {
				if stmt == "" {
					continue
				}
				if _, err := database.Exec(stmt); err != nil {
					t.Fatalf("break fixture with %q: %v", stmt, err)
				}
			}
			opener := &testOpener{pools: map[string]*sql.DB{sharedDSN: shared}}
			cfg := config.NewFromData(&config.ConfigData{DatabaseURL: testDSNStackA, Transcripts: tc.transcripts})
			store := NewRoutedTranscriptStore(cfg, primary, opener.open).(*routedTranscriptStore)

			deleted, err := store.purgeMigratedLocalTranscripts(context.Background(), tc.batchSize, tc.budget)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil || deleted != tc.wantDeleted {
				t.Fatalf("deleted=%d err=%v, want %d", deleted, err, tc.wantDeleted)
			}
			if got := readStackKeyed(t, primary, stepCountsQuery); fmt.Sprint(got) != fmt.Sprint(tc.wantLocal) {
				t.Errorf("local sessions = %v, want %v", got, tc.wantLocal)
			}
			if got := readStackKeyed(t, primary, "SELECT stack, session_id, COUNT(*) FROM transcript_steps GROUP BY stack, session_id"); fmt.Sprint(got) != fmt.Sprint(tc.wantLocal) {
				t.Errorf("local steps = %v, want %v", got, tc.wantLocal)
			}
			if got := readStackKeyed(t, shared, stepCountsQuery); fmt.Sprint(got) != fmt.Sprint(sharedBefore) {
				t.Errorf("shared index changed: %v, want %v", got, sharedBefore)
			}
		})
	}
}

// stepCountsQuery maps each session summary to its stored step count.
const stepCountsQuery = "SELECT s.stack, s.session_id, COUNT(t.step_index) FROM session_summaries s LEFT JOIN transcript_steps t ON t.stack = s.stack AND t.session_id = s.session_id GROUP BY s.stack, s.session_id"

// seedTranscriptSessions writes one summary and two steps per session ID under stack into database.
func seedTranscriptSessions(t *testing.T, database *sql.DB, stack string, sessionIDs ...string) {
	t.Helper()
	store := NewSQLTranscriptStore(database, stack)
	base := time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC)
	for _, id := range sessionIDs {
		if err := store.UpsertSessionSummary(context.Background(), SessionSummary{SessionID: id, Engine: "agy", LastMtime: base}); err != nil {
			t.Fatalf("seed summary %s/%s: %v", stack, id, err)
		}
		steps := []TranscriptStep{
			{SessionID: id, StepIndex: 0, StepType: "user", Content: "hello", CreatedAt: base},
			{SessionID: id, StepIndex: 1, StepType: "tool_call", ToolName: "Bash", Content: "ls", CreatedAt: base},
		}
		if err := store.BatchInsertTranscriptSteps(context.Background(), steps); err != nil {
			t.Fatalf("seed steps %s/%s: %v", stack, id, err)
		}
	}
}

func TestSQLTranscriptStore_SetSessionEngine(t *testing.T) {
	tests := []struct {
		name        string
		session     string
		engine      string
		wantErr     string
		wantEngines map[string]string
	}{
		{name: "stamps only the own-stack row", session: "a-only", engine: "claude", wantEngines: map[string]string{"stack_a/a-only": "claude", "stack_a/dup-sess": "agy", "stack_b/dup-sess": "claude", "stack_b/b-only": "claude"}},
		{name: "same id on another stack is untouched", session: "dup-sess", engine: "claude", wantEngines: map[string]string{"stack_a/a-only": "", "stack_a/dup-sess": "claude", "stack_b/dup-sess": "claude", "stack_b/b-only": "claude"}},
		{name: "empty engine rejected", session: "a-only", engine: " ", wantErr: "cannot be empty"},
		{name: "empty session rejected", engine: "agy", wantErr: "cannot be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shared, a, b := newSharedStackPair(t)
			seedSharedStacks(t, a, b)
			err := a.SetSessionEngine(context.Background(), tc.session, tc.engine)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("SetSessionEngine: %v", err)
			}
			got := readStackKeyed(t, shared, "SELECT stack, session_id, engine FROM session_summaries")
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
