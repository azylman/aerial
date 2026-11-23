package store

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/coven/internal/config"
)

// sabotageSessionJoin fails only the conversation_id join fallback of the
// session summary lookup; the direct sessions query keeps working.
const sabotageSessionJoin = `ALTER TABLE messages DROP COLUMN conversation_id`

// extractSession is one sessions row seeded for thread context lookups.
type extractSession struct {
	agent, id, summary string
}

type threadContextBeforeWatermarkTestCase struct {
	name        string
	nilDB       bool
	threadID    string
	priorLimit  int
	agent       string
	watermark   int64
	sessions    []extractSession
	turns       []testTurn
	sabotage    string
	errSubstr   string
	wantSummary string
	wantRowIDs  []int64
}

func TestSQLStore_GetThreadContextBeforeWatermark(t *testing.T) {
	t.Parallel()
	const beta = "agent-beta"
	betaTurn := func(rowID int64, content, response string) testTurn {
		return testTurn{agent: beta, thread: "t-1", rowID: rowID, content: content, response: response, status: "COMPLETED", admin: true}
	}

	tests := []threadContextBeforeWatermarkTestCase{
		{name: "NilDBError", nilDB: true, threadID: "t-1", errSubstr: "database connection is not initialized"},
		{name: "EmptyThreadID", threadID: "   "},
		{name: "WatermarkQueryError", threadID: "t-1", sabotage: sabotageWatermarks, errSubstr: "extraction_watermarks"},
		{name: "SessionQueryError", threadID: "t-1", sabotage: sabotageSessions, errSubstr: "failed to query session summary"},
		{name: "JoinedSessionQueryError", threadID: "t-1", sabotage: sabotageSessionJoin, errSubstr: "failed to query joined session summary"},
		{
			name: "TurnsQueryError", threadID: "t-1", watermark: 15,
			sessions: []extractSession{{config.DefaultStack, "t-1", "summary text"}},
			sabotage: sabotageTurnReads, errSubstr: "response_text",
		},
		{
			// No watermark yet: the summary is returned but no prior turns.
			name: "NoWatermarkReturnsSummaryOnly", threadID: "t-1", agent: "agent-alpha",
			sessions:    []extractSession{{"agent-alpha", "t-1", "summary text"}},
			turns:       []testTurn{{agent: "agent-alpha", thread: "t-1", rowID: 8, content: "p", response: "r", admin: true}},
			wantSummary: "summary text",
		},
		{
			// Direct session match; prior turns are this agent's completed
			// admin turns before the watermark, newest two, oldest first.
			name: "DirectSessionChronologicalPriorTurns", threadID: "t-1", agent: "  " + beta + "  ", priorLimit: -1, watermark: 20,
			sessions: []extractSession{{beta, "t-1", "Session summary here"}, {config.DefaultStack, "t-1", "other agent summary"}},
			turns: []testTurn{
				betaTurn(4, "oldest prompt", "oldest response"),
				betaTurn(8, "first prompt", "first response"),
				{agent: beta, thread: "t-1", rowID: 10, content: "not admin", response: "r", status: "COMPLETED"},
				betaTurn(12, "second prompt", "second response"),
				{agent: beta, thread: "t-1", rowID: 14, content: "failed", response: "r", status: "FAILED", admin: true},
				{agent: config.DefaultStack, thread: "t-1", rowID: 15, content: "other agent", response: "r", status: "COMPLETED", admin: true},
				betaTurn(25, "after watermark", "r"),
			},
			wantSummary: "Session summary here",
			wantRowIDs:  []int64{8, 12},
		},
		{
			// 'discord-' prefixed session; non-extractable turns (tool-only
			// "[" response, empty response) are skipped, not counted.
			name: "DiscordPrefixSessionSkipsNonExtractable", threadID: "t-3", priorLimit: 2, watermark: 20,
			sessions: []extractSession{{config.DefaultStack, "discord-t-3", "Discord summary"}},
			turns: []testTurn{
				adminTurn("t-3", 3, "p3", "r3"),
				adminTurn("t-3", 5, "p5", "r5"),
				adminTurn("t-3", 7, "p7", "r7"),
				adminTurn("t-3", 9, "p9", "[tool output]"),
				adminTurn("t-3", 11, "p11", ""),
			},
			wantSummary: "Discord summary",
			wantRowIDs:  []int64{5, 7},
		},
		{
			name: "JoinFallbackSummaryViaConversationID", threadID: "t-2", agent: "agent-alpha", priorLimit: -1, watermark: 20,
			sessions:    []extractSession{{"agent-alpha", "conv-9", "Fallback summary"}},
			turns:       []testTurn{{agent: "agent-alpha", thread: "t-2", rowID: 8, content: "first prompt", response: "first response", status: "COMPLETED", admin: true, convID: "conv-9"}},
			wantSummary: "Fallback summary",
			wantRowIDs:  []int64{8},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runGetThreadContextBeforeWatermarkCase(t, tc)
		})
	}
}

func runGetThreadContextBeforeWatermarkCase(t *testing.T, tc threadContextBeforeWatermarkTestCase) {
	t.Helper()
	ctx := context.Background()
	store := &SQLStore{}
	if !tc.nilDB {
		db := newTestDB(t)
		agent := strings.TrimSpace(tc.agent)
		if agent == "" {
			agent = config.DefaultStack
		}
		if tc.watermark > 0 {
			seedWatermark(t, db, agent, tc.threadID, tc.watermark, nil)
		}
		for _, s := range tc.sessions {
			seedSession(t, db, s.agent, s.id, s.summary)
		}
		seedTurns(t, db, tc.turns...)
		execSQL(t, db, tc.sabotage)
		store = NewSQLStore(db)
	}

	summary, turns, err := store.GetThreadContextBeforeWatermark(ctx, tc.threadID, tc.priorLimit, tc.agent)
	if tc.errSubstr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
			t.Fatalf("GetThreadContextBeforeWatermark() err = %v, want substring %q", err, tc.errSubstr)
		}
		return
	}
	if err != nil {
		t.Fatalf("GetThreadContextBeforeWatermark() err = %v", err)
	}
	if summary != tc.wantSummary {
		t.Errorf("summary = %q, want %q", summary, tc.wantSummary)
	}
	var rowIDs []int64
	for _, m := range turns {
		rowIDs = append(rowIDs, m.RowID)
	}
	if !slices.Equal(rowIDs, tc.wantRowIDs) {
		t.Errorf("prior turn row ids = %v, want %v", rowIDs, tc.wantRowIDs)
	}
}

func TestSQLStore_GetThreadTrajectoryContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nilDB      bool
		noIndexer  bool
		threadID   string
		agent      string
		statusCode int
		body       string
		wantErr    bool
		wantFacts  []string
		wantTools  []string
		wantAgent  string
	}{
		{name: "NilDBError", nilDB: true, threadID: "t-1", wantErr: true},
		{name: "EmptyThreadIDReturnsNil", threadID: "   "},
		{name: "NoIndexerClientReturnsNil", noIndexer: true, threadID: "t-1"},
		{
			name:       "Successconfig.DefaultStack",
			threadID:   "th-traj-1",
			statusCode: http.StatusOK,
			body:       `{"retrieved_facts": ["- [id:1] Matcha"], "tool_calls": ["search(tea)"]}`,
			wantFacts:  []string{"- [id:1] Matcha"},
			wantTools:  []string{"search(tea)"},
			wantAgent:  "aerial",
		},
		{
			name:       "SuccessAgentOverride",
			threadID:   "th-traj-1",
			agent:      "  agent-beta  ",
			statusCode: http.StatusOK,
			body:       `{"retrieved_facts": ["- [id:2] Dark mode"], "tool_calls": []}`,
			wantFacts:  []string{"- [id:2] Dark mode"},
			wantTools:  []string{},
			wantAgent:  "agent-beta",
		},
		{name: "IndexerServerError", threadID: "th-traj-2", statusCode: http.StatusInternalServerError, wantErr: true, wantAgent: "aerial"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotAgent string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAgent = r.URL.Query().Get("agent")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			store := &SQLStore{}
			if !tc.nilDB {
				store = newTestStore(t)
			}
			if !tc.noIndexer {
				store.SetIndexerClient(NewIndexerClient(srv.URL, "aerial", srv.Client()))
			}

			facts, tools, err := store.GetThreadTrajectoryContext(context.Background(), tc.threadID, tc.agent)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetThreadTrajectoryContext() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if gotAgent != tc.wantAgent {
				t.Errorf("indexer agent = %q, want %q", gotAgent, tc.wantAgent)
			}
			if !reflect.DeepEqual(facts, tc.wantFacts) {
				t.Errorf("facts = %#v, want %#v", facts, tc.wantFacts)
			}
			if !reflect.DeepEqual(tools, tc.wantTools) {
				t.Errorf("tools = %#v, want %#v", tools, tc.wantTools)
			}
		})
	}
}

// sabotageNullFactText lets a fact row carry a NULL fact_text so scanning it
// into a Go string fails (GetAllFactsWithEmbeddings scan error).
const sabotageNullFactText = `ALTER TABLE facts ALTER COLUMN fact_text DROP NOT NULL; UPDATE facts SET fact_text = NULL`

func queryStrings(t testing.TB, db DBTX, query string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %q: %v", query, err)
	}
	return out
}

// seedFacts inserts facts through the production InsertFact, fact i using embs[i].
func seedFacts(t testing.TB, store *SQLStore, texts []string, embs [][]float32) {
	t.Helper()
	for i, text := range texts {
		if _, err := store.InsertFact(context.Background(), text, embs[i]); err != nil {
			t.Fatalf("seed fact %q: %v", text, err)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSQLStore_GetAllFactsWithEmbeddings(t *testing.T) {
	t.Parallel()
	const fixTimes = `UPDATE facts SET created_at = '2026-01-01T00:00:00Z', last_reinforced_at = '2026-01-02T00:00:00Z' WHERE id = 1;
UPDATE facts SET created_at = '2026-01-03T00:00:00Z', last_reinforced_at = '2026-01-04T00:00:00Z', reinforce_count = 3 WHERE id = 2`

	tests := []struct {
		name    string
		nilDB   bool
		facts   []string
		sql     string
		wantErr bool
		want    []string
	}{
		{name: "NilDBError", nilDB: true, wantErr: true},
		{name: "QueryError", sql: sabotageFacts, wantErr: true},
		{name: "RowScanError", facts: []string{"Morning run"}, sql: sabotageNullFactText, wantErr: true},
		{name: "SuccessEmpty"},
		{
			name:  "RowsInIDOrderWithEmbeddings",
			facts: []string{"Morning run", "Evening swim"},
			sql:   fixTimes,
			want: []string{
				"1|Morning run|2026-01-01T00:00:00Z|2026-01-02T00:00:00Z|1|0.5,0|384",
				"2|Evening swim|2026-01-03T00:00:00Z|2026-01-04T00:00:00Z|3|0,0.5|384",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &SQLStore{}
			if !tc.nilDB {
				store = newTestStore(t)
				seedFacts(t, store, tc.facts, [][]float32{testEmbeddingAt(0, 0.5), testEmbeddingAt(1, 0.5)})
				execSQL(t, store.DB(), tc.sql)
			}
			facts, err := store.GetAllFactsWithEmbeddings(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetAllFactsWithEmbeddings() err = %v, wantErr = %v", err, tc.wantErr)
			}
			got := make([]string, 0, len(facts))
			for _, f := range facts {
				got = append(got, fmt.Sprintf("%d|%s|%s|%s|%d|%g,%g|%d", f.Fact.ID, f.Fact.FactText,
					f.Fact.CreatedAt.UTC().Format(time.RFC3339), f.Fact.LastReinforcedAt.UTC().Format(time.RFC3339),
					f.Fact.ReinforceCount, f.Embedding[0], f.Embedding[1], len(f.Embedding)))
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("GetAllFactsWithEmbeddings() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractLoadedSkillsFromCalls(t *testing.T) {
	tests := []struct {
		name      string
		toolCalls []string
		want      []string
	}{
		{
			name:      "Empty",
			toolCalls: nil,
			want:      nil,
		},
		{
			name: "SingleSkill",
			toolCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
			},
			want: []string{"ha-operations"},
		},
		{
			name: "DeduplicatedMultipleCalls",
			toolCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
				"[SKILL_LOADED: ha-operations]",
				"-> OUTPUT(read_file): done",
			},
			want: []string{"ha-operations"},
		},
		{
			name: "MultipleDifferentSkills",
			toolCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
				"[SKILL_LOADED: culinary-recipes]",
			},
			want: []string{"ha-operations", "culinary-recipes"},
		},
		{
			name: "NoLoadedSkills",
			toolCalls: []string{
				"run_command(echo hi)",
				"-> OUTPUT(run_command): hi",
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractLoadedSkillsFromCalls(tc.toolCalls)
			if len(got) != len(tc.want) {
				t.Fatalf("ExtractLoadedSkillsFromCalls() len = %d, want %d (%v)", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("ExtractLoadedSkillsFromCalls()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// proceduralRows renders every procedural_candidates row in id order.
func proceduralRows(t testing.TB, db DBTX) []string {
	t.Helper()
	return queryStrings(t, db, `SELECT id || '|' || agent || '|' || thread_id || '|' || domain || '|' || loaded_skill || '|' || trigger_pattern || '|' ||
	steps_json::text || '|' || avoid_json::text || '|' || status || '|' || COALESCE(pr_number::text, '') FROM procedural_candidates ORDER BY id`)
}

// seedProceduralCandidates inserts candidates through the production
// InsertProceduralCandidate, replacing a missing embedding with a one-hot
// vector at dimension 100+i (orthogonal to every fixture embedding; the real
// column rejects empty vectors and zero vectors break cosine distance).
func seedProceduralCandidates(t testing.TB, store *SQLStore, cands []ProceduralCandidate) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(cands))
	for i, cand := range cands {
		if len(cand.Embedding) == 0 {
			cand.Embedding = testEmbeddingAt(100+i, 1.0)
		}
		id, err := store.InsertProceduralCandidate(context.Background(), cand)
		if err != nil {
			t.Fatalf("seed procedural candidate %q: %v", cand.Trigger, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestSQLStore_InsertProceduralCandidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilDB    bool
		sabotage string
		cand     ProceduralCandidate
		wantErr  bool
		wantID   int64
		wantRows []string
	}{
		{name: "NilDB", nilDB: true, cand: ProceduralCandidate{Trigger: "valid trigger"}, wantErr: true},
		{name: "EmptyTrigger", cand: ProceduralCandidate{Trigger: "   ", Embedding: testEmbedding(0.5)}, wantErr: true},
		{name: "InsertError", sabotage: sabotageProceduralInsert, cand: ProceduralCandidate{Trigger: "valid trigger", Embedding: testEmbedding(0.5)}, wantErr: true},
		{
			name: "SuccessWithPR",
			cand: ProceduralCandidate{
				ThreadID:    "t-100",
				Domain:      "common",
				LoadedSkill: "ha-operations",
				Trigger:     "  turn on porch light when motion detected  ",
				Steps:       []string{"call ha_search", "call ha_call_write_tool"},
				Avoid:       []string{"avoid loop"},
				Embedding:   testEmbedding(0.5),
				Status:      "consolidated",
				PRNumber:    42,
				Agent:       "agent-beta",
			},
			wantID:   1,
			wantRows: []string{`1|agent-beta|t-100|common|ha-operations|turn on porch light when motion detected|["call ha_search", "call ha_call_write_tool"]|["avoid loop"]|consolidated|42`},
		},
		{
			name:     "SuccessDefaults",
			cand:     ProceduralCandidate{Trigger: "valid trigger pattern", Embedding: testEmbedding(0.1)},
			wantID:   1,
			wantRows: []string{`1|aerial||discord||valid trigger pattern|[]|[]|pending|`},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilDB {
				if _, err := (&SQLStore{}).InsertProceduralCandidate(context.Background(), tc.cand); err == nil {
					t.Fatal("InsertProceduralCandidate() on nil DB succeeded, want error")
				}
				return
			}
			store := newTestStore(t)
			execSQL(t, store.DB(), tc.sabotage)
			id, err := store.InsertProceduralCandidate(context.Background(), tc.cand)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InsertProceduralCandidate() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if id != tc.wantID {
				t.Errorf("InsertProceduralCandidate() id = %d, want %d", id, tc.wantID)
			}
			if got := proceduralRows(t, store.DB()); !equalStrings(got, tc.wantRows) {
				t.Errorf("rows = %q, want %q", got, tc.wantRows)
			}
		})
	}
}

func TestSQLStore_GetPendingProceduralCandidates(t *testing.T) {
	t.Parallel()
	fixture := []ProceduralCandidate{
		{ThreadID: "t-1", LoadedSkill: "ha-operations", Trigger: "motion detected", Steps: []string{"step 1"}, Avoid: []string{"avoid 1"}, Embedding: testEmbeddingAt(0, 0.5)},
		{ThreadID: "t-2", Trigger: "already skipped", Status: "skipped", Embedding: testEmbeddingAt(1, 0.5)},
		{ThreadID: "t-3", Trigger: "unloaded skill trigger", Embedding: testEmbeddingAt(2, 0.5), Agent: "agent-beta", PRNumber: 5},
		{ThreadID: "t-4", Domain: "common", Trigger: "malformed json", Embedding: testEmbeddingAt(3, 0.5)},
	}
	// Row 1 is newest, so created_at ASC, id ASC yields 3, 4, 1; row 4 carries
	// JSON objects (not arrays) and row 3 NULL avoid, both decoded as [].
	const ordering = `UPDATE procedural_candidates SET created_at = CASE id WHEN 1 THEN '2026-10-01T13:00:00Z'::timestamptz ELSE '2026-10-01T12:00:00Z'::timestamptz END;
UPDATE procedural_candidates SET steps_json = '{"a": 1}', avoid_json = '{"b": 2}' WHERE id = 4;
ALTER TABLE procedural_candidates ALTER COLUMN avoid_json DROP NOT NULL; UPDATE procedural_candidates SET avoid_json = NULL WHERE id = 3`
	const bulk = `INSERT INTO procedural_candidates (trigger_pattern, embedding) SELECT 'bulk ' || g, array_fill(1::real, ARRAY[384])::vector FROM generate_series(1, 101) g`
	const nullThread = `ALTER TABLE procedural_candidates ALTER COLUMN thread_id DROP NOT NULL; UPDATE procedural_candidates SET thread_id = NULL`

	tests := []struct {
		name      string
		nilDB     bool
		cands     []ProceduralCandidate
		sql       string
		wantErr   bool
		wantCount int
		want      []string
	}{
		{name: "NilDB", nilDB: true, wantErr: true},
		{name: "QueryError", sql: sabotageProcedural, wantErr: true},
		{name: "ScanError", cands: fixture[:1], sql: nullThread, wantErr: true},
		{name: "Empty"},
		{
			name:      "OnlyPendingOrderedWithParsedJSON",
			cands:     fixture,
			sql:       ordering,
			wantCount: 3,
			want: []string{
				`3|agent-beta|t-3|discord||unloaded skill trigger|[]|[]|0.5@2|pending|5|2026-10-01T12:00:00Z`,
				`4|aerial|t-4|common||malformed json|[]|[]|0.5@3|pending|0|2026-10-01T12:00:00Z`,
				`1|aerial|t-1|discord|ha-operations|motion detected|["step 1"]|["avoid 1"]|0.5@0|pending|0|2026-10-01T13:00:00Z`,
			},
		},
		{name: "LimitedTo100", sql: bulk, wantCount: 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &SQLStore{}
			if !tc.nilDB {
				store = newTestStore(t)
				seedProceduralCandidates(t, store, tc.cands)
				execSQL(t, store.DB(), tc.sql)
			}
			cands, err := store.GetPendingProceduralCandidates(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetPendingProceduralCandidates() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(cands) != tc.wantCount {
				t.Fatalf("GetPendingProceduralCandidates() count = %d, want %d", len(cands), tc.wantCount)
			}
			if tc.want == nil {
				return
			}
			var got []string
			for _, c := range cands {
				dim := slices.IndexFunc(c.Embedding, func(v float32) bool { return v != 0 })
				got = append(got, fmt.Sprintf("%d|%s|%s|%s|%s|%s|%q|%q|%g@%d|%s|%d|%s", c.ID, c.Agent, c.ThreadID, c.Domain, c.LoadedSkill, c.Trigger,
					c.Steps, c.Avoid, c.Embedding[dim], dim, c.Status, c.PRNumber, c.CreatedAt.UTC().Format(time.RFC3339)))
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("GetPendingProceduralCandidates() = %q, want %q", got, tc.want)
			}
		})
	}
}

// statusFixture is three pending candidates (ids 1, 2, 3).
var statusFixture = []ProceduralCandidate{
	{Trigger: "first trigger pattern"},
	{Trigger: "second trigger pattern"},
	{Trigger: "third trigger pattern"},
}

func procStatusRows(t testing.TB, db DBTX) []string {
	t.Helper()
	return queryStrings(t, db, `SELECT id || ':' || status || ':' || COALESCE(pr_number::text, '-') FROM procedural_candidates ORDER BY id`)
}

func TestSQLStore_MarkProceduralCandidatesConsolidated(t *testing.T) {
	t.Parallel()
	pending := []string{"1:pending:-", "2:pending:-", "3:pending:-"}
	tests := []struct {
		name     string
		nilDB    bool
		sabotage string
		ids      []int64
		prNum    int
		wantErr  bool
		want     []string
	}{
		{name: "NilDB", nilDB: true, ids: []int64{1}, prNum: 10, wantErr: true},
		{name: "EmptyIDsNoOp", prNum: 10, want: pending},
		{name: "ExecError", sabotage: sabotageProceduralUpdate, ids: []int64{1, 2}, prNum: 10, wantErr: true, want: pending},
		{name: "Success", ids: []int64{1, 2}, prNum: 10, want: []string{"1:consolidated:10", "2:consolidated:10", "3:pending:-"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilDB {
				if err := (&SQLStore{}).MarkProceduralCandidatesConsolidated(context.Background(), tc.ids, tc.prNum); err == nil {
					t.Fatal("MarkProceduralCandidatesConsolidated() on nil DB succeeded, want error")
				}
				return
			}
			store := newTestStore(t)
			seedProceduralCandidates(t, store, statusFixture)
			execSQL(t, store.DB(), tc.sabotage)
			err := store.MarkProceduralCandidatesConsolidated(context.Background(), tc.ids, tc.prNum)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MarkProceduralCandidatesConsolidated() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got := procStatusRows(t, store.DB()); !equalStrings(got, tc.want) {
				t.Errorf("rows = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSQLStore_MarkProceduralCandidatesSkipped(t *testing.T) {
	t.Parallel()
	pending := []string{"1:pending:-", "2:pending:-", "3:pending:-"}
	tests := []struct {
		name     string
		nilDB    bool
		sabotage string
		ids      []int64
		wantErr  bool
		want     []string
	}{
		{name: "NilDB", nilDB: true, ids: []int64{1}, wantErr: true},
		{name: "EmptyIDsNoOp", want: pending},
		{name: "ExecError", sabotage: sabotageProceduralUpdate, ids: []int64{3}, wantErr: true, want: pending},
		{name: "Success", ids: []int64{2, 3}, want: []string{"1:pending:-", "2:skipped:-", "3:skipped:-"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilDB {
				if err := (&SQLStore{}).MarkProceduralCandidatesSkipped(context.Background(), tc.ids); err == nil {
					t.Fatal("MarkProceduralCandidatesSkipped() on nil DB succeeded, want error")
				}
				return
			}
			store := newTestStore(t)
			seedProceduralCandidates(t, store, statusFixture)
			execSQL(t, store.DB(), tc.sabotage)
			err := store.MarkProceduralCandidatesSkipped(context.Background(), tc.ids)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MarkProceduralCandidatesSkipped() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got := procStatusRows(t, store.DB()); !equalStrings(got, tc.want) {
				t.Errorf("rows = %q, want %q", got, tc.want)
			}
		})
	}
}
