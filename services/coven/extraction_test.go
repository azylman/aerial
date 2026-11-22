package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/pgvector/pgvector-go"
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
			sessions: []extractSession{{DefaultStack, "t-1", "summary text"}},
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
			sessions: []extractSession{{beta, "t-1", "Session summary here"}, {DefaultStack, "t-1", "other agent summary"}},
			turns: []testTurn{
				betaTurn(4, "oldest prompt", "oldest response"),
				betaTurn(8, "first prompt", "first response"),
				{agent: beta, thread: "t-1", rowID: 10, content: "not admin", response: "r", status: "COMPLETED"},
				betaTurn(12, "second prompt", "second response"),
				{agent: beta, thread: "t-1", rowID: 14, content: "failed", response: "r", status: "FAILED", admin: true},
				{agent: DefaultStack, thread: "t-1", rowID: 15, content: "other agent", response: "r", status: "COMPLETED", admin: true},
				betaTurn(25, "after watermark", "r"),
			},
			wantSummary: "Session summary here",
			wantRowIDs:  []int64{8, 12},
		},
		{
			// 'discord-' prefixed session; non-extractable turns (tool-only
			// "[" response, empty response) are skipped, not counted.
			name: "DiscordPrefixSessionSkipsNonExtractable", threadID: "t-3", priorLimit: 2, watermark: 20,
			sessions: []extractSession{{DefaultStack, "discord-t-3", "Discord summary"}},
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
			agent = DefaultStack
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
			name:       "SuccessDefaultStack",
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

func TestFormatExtractableTurns(t *testing.T) {
	tests := []struct {
		name      string
		turns     []Message
		wantEmpty bool
		wantSub   string
	}{
		{
			name:      "EmptyTurns",
			turns:     nil,
			wantEmpty: true,
		},
		{
			name: "SkipEmptyContentAndResponse",
			turns: []Message{
				{Content: "   ", ResponseText: "   "},
			},
			wantEmpty: true,
		},
		{
			name: "NormalTurns",
			turns: []Message{
				{Content: "What is my favorite tea?", ResponseText: "You love matcha latte."},
			},
			wantSub: "User: What is my favorite tea?\nAssistant: You love matcha latte.",
		},
		{
			name: "TruncationOver20000Chars",
			turns: []Message{
				{Content: strings.Repeat("A", 15000), ResponseText: strings.Repeat("B", 10000)},
			},
			wantSub: "Assistant: ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatExtractableTurns(tc.turns)
			if tc.wantEmpty && got != "" {
				t.Fatalf("expected empty, got %q", got)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("expected output to contain %q, got length %d", tc.wantSub, len(got))
			}
		})
	}
}

func TestParseFactsJSON(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantErr   bool
		wantCount int
	}{
		{
			name:    "EmptyRaw",
			raw:     "   ",
			wantErr: true,
		},
		{
			name:    "NoJSONBrace",
			raw:     "Just some plain text without braces",
			wantErr: true,
		},
		{
			name:    "InvalidJSON",
			raw:     "Here is: {invalid_json}",
			wantErr: true,
		},
		{
			name:      "ValidJSON",
			raw:       `{"facts":[{"action":"ADD","category":"user_preference","fact_text":"User likes green tea","importance_score":0.8}]}`,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "ValidJSONWithPreamble",
			raw:       "Sure, here are the extracted facts:\n```json\n{\"facts\":[{\"action\":\"DELETE\",\"delete_id\":10,\"category\":\"user_preference\",\"fact_text\":\"old fact\",\"importance_score\":0.5}]}\n```",
			wantErr:   false,
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := parseFactsJSON(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseFactsJSON() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && len(payload.Facts) != tc.wantCount {
				t.Errorf("len(Facts) = %d, want %d", len(payload.Facts), tc.wantCount)
			}
		})
	}
}

func TestEvaluateCandidateAdmission(t *testing.T) {
	tests := []struct {
		name       string
		item       ExtractedFactItem
		wantAdmit  bool
		wantReason string
	}{
		{
			name:       "TooShort",
			item:       ExtractedFactItem{FactText: "short"},
			wantAdmit:  false,
			wantReason: "too_short",
		},
		{
			name:       "EphemeralTelemetryPR",
			item:       ExtractedFactItem{FactText: "Merged PR #1234 into main branch successfully"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralVanadisChunk",
			item:       ExtractedFactItem{FactText: "Executed vanadis_chunk task cleanly for build"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralCommitSha",
			item:       ExtractedFactItem{FactText: "Rolled back commit 0123456789abcdef due to panic"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralAutoMerge",
			item:       ExtractedFactItem{FactText: "Verified that auto-merge armed on the target pull request"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralAwaitingDelivery",
			item:       ExtractedFactItem{FactText: "Hardware sensor is awaiting delivery before installation"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralCurrentlyInProgress",
			item:       ExtractedFactItem{FactText: "Database migration task is currently in progress on the cluster"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "PlaintextPasswordRejected",
			item:       ExtractedFactItem{FactText: "The password for the staging service account is SuperSecret2026!."},
			wantAdmit:  false,
			wantReason: "plaintext_credential",
		},
		{
			name:       "PlaintextTokenPrefixRejected",
			item:       ExtractedFactItem{FactText: "Service authentication uses token ghp_0123456789abcdefghij0123456789ab"},
			wantAdmit:  false,
			wantReason: "plaintext_credential",
		},
		{
			name:       "PlaintextPINCodeRejected",
			item:       ExtractedFactItem{FactText: "Guest smart lock PIN code is 4829 for weekend access"},
			wantAdmit:  false,
			wantReason: "plaintext_credential",
		},
		{
			name:       "CredentialStorageLocationAdmitted",
			item:       ExtractedFactItem{FactText: "The database password is stored in the cluster secret manager"},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name:       "PlaintextCredentialAfterWhitelistedClauseRejected",
			item:       ExtractedFactItem{FactText: "The GitHub access token is configured as ghp_0123456789abcdefghij0123456789ab"},
			wantAdmit:  false,
			wantReason: "plaintext_credential",
		},
		{
			name:       "DurableDirectiveOverridesEphemeral",
			item:       ExtractedFactItem{FactText: "Always verify CI before pull request #500 merges"},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name:       "ValidDurableFact",
			item:       ExtractedFactItem{FactText: "User drinks oat milk latte in the morning"},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name:       "NoopActionRejected",
			item:       ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail"},
			wantAdmit:  false,
			wantReason: "noop_action",
		},
		{
			name:       "NoopActionCaseInsensitiveWithWhitespace",
			item:       ExtractedFactItem{Action: "  noop  ", FactText: "User mentioned something ephemeral yesterday"},
			wantAdmit:  false,
			wantReason: "noop_action",
		},
		{
			name:       "DeleteActionRejected",
			item:       ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact about favorite drinks"},
			wantAdmit:  false,
			wantReason: "delete_action",
		},
		{
			name:       "DeleteActionCaseInsensitiveWithWhitespace",
			item:       ExtractedFactItem{Action: "  delete  ", FactText: "Old preference that is no longer active"},
			wantAdmit:  false,
			wantReason: "delete_action",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admit, reason := EvaluateCandidateAdmission(tc.item)
			if admit != tc.wantAdmit {
				t.Errorf("admit = %v, want %v", admit, tc.wantAdmit)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestBuildCandidateReconcilePrompt(t *testing.T) {
	candidate := ExtractedFactItem{
		FactText: "User prefers dark mode across all interfaces",
	}
	neighbors := []ScoredFact{
		{
			Fact: Fact{
				ID:       42,
				FactText: "User prefers light mode on mobile displays",
			},
			Score: 0.76,
		},
	}
	prompt := buildCandidateReconcilePrompt(candidate, neighbors)
	for _, expected := range []string{
		"CANDIDATE FACT:",
		"Fact: User prefers dark mode across all interfaces",
		"EXISTING CANDIDATE NEIGHBORS",
		"ID 42 (similarity 0.76)",
		"User prefers light mode on mobile displays",
		"supersedes_id",
		"delete_id",
		"Never merge distinct topics",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("buildCandidateReconcilePrompt missing expected substring %q in:\n%s", expected, prompt)
		}
	}
}

// sabotageFactEmbedding fails FindNearestScoredFacts (and every other
// embedding read/write) while id, fact_text and reinforce_count stay readable.
const sabotageFactEmbedding = `ALTER TABLE facts DROP COLUMN embedding`

// reconcileNeighborID is the ID of the seeded neighbor fact whenever a
// reconcile tier case provides neighborEmb.
const reconcileNeighborID int64 = 10

type reconcileCandidateFactTierTestCase struct {
	name        string
	nilStore    bool
	nilClient   bool
	item        ExtractedFactItem
	ollamaErr   error
	sabotage    string
	neighborEmb []float32
	llmResp     string
	llmErr      error
	nilLLM      bool
	wantErr     bool
	wantFacts   []extractFactRow
}

func TestReconcileCandidateFact_Tiers(t *testing.T) {
	t.Parallel()
	// The candidate embeds at 0°: a 0° neighbor has similarity 1 (tier 2,
	// > 0.88) and a 41.41° neighbor cos(41.41°) = 0.75 (tier 3, 0.60..0.88).
	highSimEmb := angleEmbedding(0)
	tier3Emb := angleEmbedding(41.41)

	valid := ExtractedFactItem{FactText: "Valid candidate fact text"}
	update10 := `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here"}]}`
	delete10 := `{"facts":[{"action":"DELETE","delete_id":10}]}`
	neighbor := extractFactRow{reconcileNeighborID, "Existing neighbor text", 1}
	reinforced := extractFactRow{reconcileNeighborID, "Existing neighbor text", 2}
	inserted := extractFactRow{1, "Valid candidate fact text", 1}
	insertedBeside := []extractFactRow{inserted, neighbor}
	untouched := []extractFactRow{neighbor}

	tests := []reconcileCandidateFactTierTestCase{
		{name: "NilStoreSafe", nilStore: true, item: valid, wantErr: true},
		{name: "NilClientSafe", nilClient: true, item: valid, wantErr: true},
		{name: "UnadmittedTooShort", item: ExtractedFactItem{FactText: "short"}},
		{name: "UnadmittedNoopAction", item: ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail"}},
		{name: "UnadmittedDeleteAction", item: ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact text should not be inserted or reinforced"}},
		{name: "EmbeddingErrorLogged", item: valid, ollamaErr: errors.New("embed fail"), wantErr: true},
		{name: "FindNearestErrorLogged", item: valid, sabotage: sabotageFactEmbedding, wantErr: true},
		{name: "Tier1_NoNeighbors_Insert", item: valid, wantFacts: []extractFactRow{inserted}},
		{name: "Tier1_InsertErrorLogged", item: valid, sabotage: sabotageInsertFact, wantErr: true},
		{name: "Tier2_HighSimReinforce", item: valid, neighborEmb: highSimEmb, wantFacts: []extractFactRow{reinforced}},
		{name: "Tier2_ReinforceErrorLogged", item: valid, neighborEmb: highSimEmb, sabotage: sabotageReinforceFact, wantErr: true, wantFacts: untouched},
		{name: "Tier3_NilLLM_FallbackInsert", item: valid, neighborEmb: tier3Emb, nilLLM: true, wantFacts: insertedBeside},
		{name: "Tier3_LLMError_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmErr: errors.New("timeout"), wantFacts: insertedBeside},
		{name: "Tier3_ParseError_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: "invalid json", wantFacts: insertedBeside},
		{name: "Tier3_EmptyFacts_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[]}`, wantFacts: insertedBeside},
		{name: "Tier3_ValidUpdate", item: valid, neighborEmb: tier3Emb, llmResp: update10, wantFacts: []extractFactRow{{reconcileNeighborID, "Updated text here", 2}}},
		{name: "Tier3_UnwhitelistedUpdate_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"UPDATE","supersedes_id":999,"fact_text":"Updated text here"}]}`, wantFacts: insertedBeside},
		{name: "Tier3_UpdateStoreErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: update10, sabotage: sabotageUpdateFact, wantErr: true, wantFacts: untouched},
		{name: "Tier3_ValidDelete", item: valid, neighborEmb: tier3Emb, llmResp: delete10},
		{name: "Tier3_UnwhitelistedDelete_NOOP", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"DELETE","delete_id":999}]}`, wantFacts: untouched},
		{name: "Tier3_DeleteStoreErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: delete10, sabotage: sabotageDeleteFact, wantErr: true, wantFacts: untouched},
		{name: "Tier3_NOOP", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"NOOP"}]}`, wantFacts: []extractFactRow{reinforced}},
		{name: "Tier3_NOOP_ExplicitSupersedesID", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"NOOP","supersedes_id":10}]}`, wantFacts: []extractFactRow{reinforced}},
		{name: "Tier3_NOOP_ReinforceErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"NOOP"}]}`, sabotage: sabotageReinforceFact, wantErr: true, wantFacts: untouched},
		{name: "Tier3_ADD", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"ADD"}]}`, wantFacts: insertedBeside},
		{name: "Tier3_DefaultUnknownAction", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"OTHER"}]}`, wantFacts: insertedBeside},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runReconcileCandidateFactTierCase(t, tc)
		})
	}
}

func runReconcileCandidateFactTierCase(t *testing.T, tc reconcileCandidateFactTierTestCase) {
	t.Helper()
	ctx := context.Background()
	db := newTestDB(t)
	if len(tc.neighborEmb) > 0 {
		execSQL(t, db, `INSERT INTO facts (id, fact_text, embedding) VALUES ($1, 'Existing neighbor text', $2)`, reconcileNeighborID, pgvector.NewVector(tc.neighborEmb))
	}
	execSQL(t, db, tc.sabotage)

	var store Store
	if !tc.nilStore {
		store = NewSQLStore(db)
	}
	var client OllamaClient
	if !tc.nilClient {
		client = &MockOllamaClient{embedding: angleEmbedding(0), err: tc.ollamaErr}
	}
	var llm LLMClientFunc
	if !tc.nilLLM {
		llm = func(ctx context.Context, prompt string) (string, error) {
			if tc.llmErr != nil {
				return "", tc.llmErr
			}
			return tc.llmResp, nil
		}
	}

	err := reconcileCandidateFact(ctx, store, client, llm, tc.item, nil, "")
	if (err != nil) != tc.wantErr {
		t.Fatalf("reconcileCandidateFact() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if got := readExtractFacts(t, db); !slices.Equal(got, tc.wantFacts) {
		t.Errorf("facts = %+v, want %+v", got, tc.wantFacts)
	}
}

type extractThreadFactsTestCase struct {
	name      string
	threadID  string
	agent     string
	nilStore  bool
	nilLLM    bool
	wantErr   bool
	wantAgent string
}

func TestExtractThreadFacts(t *testing.T) {
	t.Parallel()

	tests := []extractThreadFactsTestCase{
		{name: "NilStoreFails", threadID: "th-1", nilStore: true, wantErr: true},
		{name: "NilLLMFails", threadID: "th-1", nilLLM: true, wantErr: true},
		{name: "EmptyThreadIDFails", threadID: "", wantErr: true},
		{name: "WhitespaceThreadIDFails", threadID: "   ", wantErr: true},
		{name: "ValidArgumentsSuccess", threadID: "th-valid", wantAgent: DefaultStack},
		{name: "ValidArgumentsWithAgentPropagation", threadID: "th-valid-agent", agent: "agent-alpha", wantAgent: "agent-alpha"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runExtractThreadFactsCase(t, tc)
		})
	}
}

func runExtractThreadFactsCase(t *testing.T, tc extractThreadFactsTestCase) {
	t.Helper()
	db := newTestDB(t)
	// The thread's only message is a non-admin turn at row 10, so a valid
	// extraction finds nothing to extract and advances the agent's watermark to 10.
	if tc.wantAgent != "" {
		seedTurns(t, db, testTurn{agent: tc.wantAgent, thread: tc.threadID, rowID: 10, content: "hello", response: "hi"})
	}
	var store Store
	if !tc.nilStore {
		store = NewSQLStore(db)
	}
	var llm LLMClientFunc
	if !tc.nilLLM {
		llm = func(c context.Context, prompt string) (string, error) {
			return `{"facts":[]}`, nil
		}
	}

	var err error
	if tc.agent != "" {
		err = ExtractThreadFacts(context.Background(), store, &MockOllamaClient{embedding: angleEmbedding(0)}, llm, tc.threadID, tc.agent)
	} else {
		err = ExtractThreadFacts(context.Background(), store, &MockOllamaClient{embedding: angleEmbedding(0)}, llm, tc.threadID)
	}
	if (err != nil) != tc.wantErr {
		t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantAgent != "" {
		if wm := readExtractWatermark(t, db, tc.wantAgent, tc.threadID); wm != 10 {
			t.Errorf("watermark for agent %q = %d, want 10", tc.wantAgent, wm)
		}
	}
}
