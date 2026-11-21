package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSQLStore_GetThreadContextBeforeWatermark(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []threadContextBeforeWatermarkTestCase{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "t-1",
			wantErr:  true,
		},
		{
			name:     "EmptyThreadID",
			threadID: "   ",
			wantErr:  false,
		},
		{
			name:     "DBWatermarkError",
			threadID: "t-1",
			queryErr: errors.New("db disconnect"),
			wantErr:  true,
		},
		{
			name:         "LastExtractedRowIDZero",
			threadID:     "t-1",
			agent:        "agent-alpha",
			watermarkSQL: "SELECT 0, NULL",
			summarySQL:   "SELECT 'summary text'",
			wantSummary:  "summary text",
		},
		{
			name:         "DBQueryErrorOnTurns",
			threadID:     "t-1",
			watermarkSQL: "SELECT 15, NULL",
			summarySQL:   "SELECT 'summary text'",
			queryErr:     errors.New("db disconnect"),
			wantErr:      true,
		},
		{
			name:          "SuccessChronologicalOrder",
			threadID:      "t-1",
			agent:         "agent-beta",
			watermarkSQL:  "SELECT 20, NULL",
			summarySQL:    "SELECT 'Session summary here'",
			rowsSQL:       "SELECT 'm2', 12, 't-1', 'second prompt', 'second response', 'COMPLETED', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00' UNION ALL SELECT 'm1', 8, 't-1', 'first prompt', 'first response', 'COMPLETED', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00'",
			priorLimit:    -1,
			wantSummary:   "Session summary here",
			wantTurnCount: 2,
		},
		{
			name:          "SuccessChronologicalOrder_JoinFallbackSummary",
			threadID:      "t-2",
			agent:         "agent-alpha",
			watermarkSQL:  "SELECT 20, NULL",
			summarySQL:    "SELECT ''",
			joinSQL:       "SELECT 'Fallback summary'",
			rowsSQL:       "SELECT 'm1', 8, 't-2', 'first prompt', 'first response', 'COMPLETED', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00'",
			priorLimit:    -1,
			wantSummary:   "Fallback summary",
			wantTurnCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGetThreadContextBeforeWatermarkCase(t, tc, dbMem)
		})
	}
}

func TestSQLStore_GetThreadTrajectoryContext(t *testing.T) {
	dbMem := setupTestDBMem(t)

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
			var gotAgent string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAgent = r.URL.Query().Get("agent")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			store := NewSQLStore(dbMem)
			if tc.nilDB {
				store = &SQLStore{}
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

func TestReconcileCandidateFact_Tiers(t *testing.T) {
	dim := ExpectedEmbeddingDim
	baseEmb := make([]float32, dim)
	baseEmb[0] = 1.0

	highSimEmb := make([]float32, dim)
	copy(highSimEmb, baseEmb)

	tier3Emb := make([]float32, dim)
	tier3Emb[0] = 0.75
	tier3Emb[1] = 0.6614378

	valid := ExtractedFactItem{FactText: "Valid candidate fact text"}
	update10 := `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here"}]}`
	delete10 := `{"facts":[{"action":"DELETE","delete_id":10}]}`

	tests := []reconcileCandidateFactTierTestCase{
		{name: "NilStoreSafe", nilStore: true, item: valid, wantErr: true},
		{name: "NilClientSafe", nilClient: true, item: valid, wantErr: true, want: reconcileUnchanged},
		{name: "UnadmittedTooShort", item: ExtractedFactItem{FactText: "short"}, want: reconcileUnchanged},
		{name: "UnadmittedNoopAction", item: ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail"}, want: reconcileUnchanged},
		{name: "UnadmittedDeleteAction", item: ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact text should not be inserted or reinforced"}, want: reconcileUnchanged},
		{name: "EmbeddingErrorLogged", item: valid, ollamaErr: errors.New("embed fail"), wantErr: true, want: reconcileUnchanged},
		{name: "FindNearestErrorLogged", item: valid, storeFailOp: "FindNearestScoredFacts", wantErr: true, want: reconcileUnchanged},
		{name: "Tier1_NoNeighbors_Insert", item: valid, want: reconcileInserted},
		{name: "Tier1_InsertErrorLogged", item: valid, storeFailOp: "InsertFact", wantErr: true, want: reconcileUnchanged},
		{name: "Tier2_HighSimReinforce", item: valid, neighborEmb: highSimEmb, want: reconcileReinforced},
		{name: "Tier2_ReinforceErrorLogged", item: valid, neighborEmb: highSimEmb, storeFailOp: "ReinforceFact", wantErr: true, want: reconcileUnchanged},
		{name: "Tier3_NilLLM_FallbackInsert", item: valid, neighborEmb: tier3Emb, nilLLM: true, want: reconcileInserted},
		{name: "Tier3_LLMError_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmErr: errors.New("timeout"), want: reconcileInserted},
		{name: "Tier3_ParseError_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: "invalid json", want: reconcileInserted},
		{name: "Tier3_EmptyFacts_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[]}`, want: reconcileInserted},
		{name: "Tier3_ValidUpdate", item: valid, neighborEmb: tier3Emb, llmResp: update10, want: reconcileUpdated},
		{name: "Tier3_UnwhitelistedUpdate_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"UPDATE","supersedes_id":999,"fact_text":"Updated text here"}]}`, want: reconcileInserted},
		{name: "Tier3_UpdateStoreErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: update10, storeFailOp: "UpdateFact", wantErr: true, want: reconcileUnchanged},
		{name: "Tier3_ValidDelete", item: valid, neighborEmb: tier3Emb, llmResp: delete10, want: reconcileDeleted},
		{name: "Tier3_UnwhitelistedDelete_NOOP", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"DELETE","delete_id":999}]}`, want: reconcileUnchanged},
		{name: "Tier3_DeleteStoreErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: delete10, storeFailOp: "DeleteFact", wantErr: true, want: reconcileUnchanged},
		{name: "Tier3_NOOP", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"NOOP"}]}`, want: reconcileReinforced},
		{name: "Tier3_NOOP_ExplicitSupersedesID", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"NOOP","supersedes_id":10}]}`, want: reconcileReinforced},
		{name: "Tier3_NOOP_ReinforceErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"NOOP"}]}`, storeFailOp: "ReinforceFact", wantErr: true, want: reconcileUnchanged},
		{name: "Tier3_ADD", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"ADD"}]}`, want: reconcileInserted},
		{name: "Tier3_DefaultUnknownAction", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"OTHER"}]}`, want: reconcileInserted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runReconcileCandidateFactTierCase(t, tc, baseEmb)
		})
	}
}

type threadContextBeforeWatermarkTestCase struct {
	name          string
	nilDB         bool
	threadID      string
	priorLimit    int
	agent         string
	queryErr      error
	watermarkSQL  string
	summarySQL    string
	joinSQL       string
	rowsSQL       string
	wantErr       bool
	wantSummary   string
	wantTurnCount int
}

func runGetThreadContextBeforeWatermarkCase(t *testing.T, tc threadContextBeforeWatermarkTestCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var store *SQLStore

	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:        dbMem,
			queryErr:     tc.queryErr,
			watermarkSQL: tc.watermarkSQL,
			summarySQL:   tc.summarySQL,
			joinSQL:      tc.joinSQL,
			rowsSQL:      tc.rowsSQL,
		})
	}

	var summary string
	var turns []Message
	var err error
	if tc.agent != "" {
		summary, turns, err = store.GetThreadContextBeforeWatermark(ctx, tc.threadID, tc.priorLimit, tc.agent)
	} else {
		summary, turns, err = store.GetThreadContextBeforeWatermark(ctx, tc.threadID, tc.priorLimit)
	}
	if (err != nil) != tc.wantErr {
		t.Fatalf("GetThreadContextBeforeWatermark() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	if summary != tc.wantSummary {
		t.Errorf("summary = %q, want %q", summary, tc.wantSummary)
	}
	if len(turns) != tc.wantTurnCount {
		t.Errorf("len(turns) = %d, want %d", len(turns), tc.wantTurnCount)
	}
	if len(turns) == 2 && turns[0].RowID > turns[1].RowID {
		t.Errorf("turns not chronological: row_id[0]=%d, row_id[1]=%d", turns[0].RowID, turns[1].RowID)
	}
}

// reconcileNeighborID is the ID of the seeded neighbor fact whenever a
// reconcile tier case provides neighborEmb.
const reconcileNeighborID int64 = 10

// reconcileOutcome is the single store effect a reconcile tier case expects.
// The zero value asserts nothing beyond the call not panicking.
type reconcileOutcome int

const (
	reconcileUnasserted reconcileOutcome = iota
	reconcileUnchanged
	reconcileInserted
	reconcileReinforced
	reconcileUpdated
	reconcileDeleted
)

type reconcileCandidateFactTierTestCase struct {
	name        string
	nilStore    bool
	nilClient   bool
	item        ExtractedFactItem
	ollamaErr   error
	storeFailOp string
	neighborEmb []float32
	llmResp     string
	llmErr      error
	nilLLM      bool
	wantErr     bool
	want        reconcileOutcome
}

func runReconcileCandidateFactTierCase(t *testing.T, tc reconcileCandidateFactTierTestCase, baseEmb []float32) {
	t.Helper()
	ctx := context.Background()
	var store Store
	var mem *TestMemStore
	if !tc.nilStore {
		mem = newTestMemStore()
		if len(tc.neighborEmb) > 0 {
			mem.facts[reconcileNeighborID] = &Fact{ID: reconcileNeighborID, FactText: "Existing neighbor text", ReinforceCount: 1}
			mem.embeddings[reconcileNeighborID] = tc.neighborEmb
		}
		if tc.storeFailOp != "" {
			mem.SetFail(tc.storeFailOp, errors.New("simulated store failure"))
		}
		store = mem
	}

	var client OllamaClient
	if !tc.nilClient {
		client = &MockOllamaClient{embedding: baseEmb, err: tc.ollamaErr}
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

	if mem == nil {
		return
	}
	assertReconcileOutcome(t, mem, len(tc.neighborEmb) > 0, tc.want)
}

func assertReconcileOutcome(t *testing.T, mem *TestMemStore, hasNeighbor bool, want reconcileOutcome) {
	t.Helper()
	seeded := 0
	if hasNeighbor {
		seeded = 1
	}
	neighbor := mem.facts[reconcileNeighborID]
	switch want {
	case reconcileUnchanged:
		if len(mem.facts) != seeded {
			t.Errorf("store has %d facts, want %d (unchanged)", len(mem.facts), seeded)
		}
		if hasNeighbor && (neighbor == nil || neighbor.FactText != "Existing neighbor text" || neighbor.ReinforceCount != 1) {
			t.Errorf("expected neighbor %d to be untouched, got %+v", reconcileNeighborID, neighbor)
		}
	case reconcileInserted:
		// A fallback insert adds a new fact alongside any seeded neighbor.
		if len(mem.facts) != seeded+1 {
			t.Errorf("store has %d facts, want %d after insert", len(mem.facts), seeded+1)
		}
	case reconcileReinforced:
		if neighbor.ReinforceCount < 2 {
			t.Errorf("expected neighbor %d to be reinforced, got count %d", reconcileNeighborID, neighbor.ReinforceCount)
		}
	case reconcileUpdated:
		if neighbor.FactText != "Updated text here" {
			t.Errorf("expected neighbor %d to be updated with new text, got %q", reconcileNeighborID, neighbor.FactText)
		}
	case reconcileDeleted:
		if neighbor != nil {
			t.Errorf("expected neighbor %d to be deleted from store, but it remains", reconcileNeighborID)
		}
	case reconcileUnasserted:
	}
}

type extractThreadFactsTestCase struct {
	name      string
	ctx       context.Context
	threadID  string
	agent     string
	nilStore  bool
	nilLLM    bool
	wantErr   bool
	wantAgent string
}

func TestExtractThreadFacts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []extractThreadFactsTestCase{
		{
			name:     "NilStoreFails",
			ctx:      ctx,
			threadID: "th-1",
			nilStore: true,
			wantErr:  true,
		},
		{
			name:     "NilLLMFails",
			ctx:      ctx,
			threadID: "th-1",
			nilLLM:   true,
			wantErr:  true,
		},
		{
			name:     "EmptyThreadIDFails",
			ctx:      ctx,
			threadID: "",
			wantErr:  true,
		},
		{
			name:     "WhitespaceThreadIDFails",
			ctx:      ctx,
			threadID: "   ",
			wantErr:  true,
		},
		{
			name:     "ValidArgumentsSuccess",
			ctx:      ctx,
			threadID: "th-valid",
			wantErr:  false,
		},
		{
			name:      "ValidArgumentsWithAgentPropagation",
			ctx:       ctx,
			threadID:  "th-valid-agent",
			agent:     "agent-alpha",
			wantAgent: "agent-alpha",
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runExtractThreadFactsCase(t, tc, validEmb)
		})
	}
}

func runExtractThreadFactsCase(t *testing.T, tc extractThreadFactsTestCase, validEmb []float32) {
	t.Helper()
	var store Store
	var mem *TestMemStore
	if !tc.nilStore {
		mem = newTestMemStore()
		if tc.threadID != "" {
			mem.maxRowIDs[tc.threadID] = 10
		}
		store = mem
	}

	client := &MockOllamaClient{embedding: validEmb}

	var llm LLMClientFunc
	if !tc.nilLLM {
		llm = func(c context.Context, prompt string) (string, error) {
			return `{"facts":[]}`, nil
		}
	}

	var err error
	if tc.agent != "" {
		err = ExtractThreadFacts(tc.ctx, store, client, llm, tc.threadID, tc.agent)
	} else {
		err = ExtractThreadFacts(tc.ctx, store, client, llm, tc.threadID)
	}
	if (err != nil) != tc.wantErr {
		t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if !tc.wantErr && tc.wantAgent != "" && mem != nil {
		if mem.lastUpdatedWatermarkAgent != tc.wantAgent {
			t.Errorf("watermark agent = %q, want %q", mem.lastUpdatedWatermarkAgent, tc.wantAgent)
		}
	}
}
