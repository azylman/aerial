package server

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/pgvector/pgvector-go"

	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

func TestFormatExtractableTurns(t *testing.T) {
	tests := []struct {
		name      string
		turns     []store.Message
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
			turns: []store.Message{
				{Content: "   ", ResponseText: "   "},
			},
			wantEmpty: true,
		},
		{
			name: "NormalTurns",
			turns: []store.Message{
				{Content: "What is my favorite tea?", ResponseText: "You love matcha latte."},
			},
			wantSub: "User: What is my favorite tea?\nAssistant: You love matcha latte.",
		},
		{
			name: "TruncationOver20000Chars",
			turns: []store.Message{
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
			item:       ExtractedFactItem{FactText: "Service authentication uses token " + "ghp_" + "0123456789abcdefghij0123456789ab"},
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
			item:       ExtractedFactItem{FactText: "The GitHub access token is configured as " + "ghp_" + "0123456789abcdefghij0123456789ab"},
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
	neighbors := []store.ScoredFact{
		{
			Fact: store.Fact{
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
		{name: "Tier3_Update_WithDeleteID", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"UPDATE","delete_id":10,"fact_text":"Updated via delete_id"}]}`, wantFacts: []extractFactRow{{reconcileNeighborID, "Updated via delete_id", 2}}},
		{name: "Tier3_Update_EmptyFactTextFallback", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":""}]}`, wantFacts: []extractFactRow{{reconcileNeighborID, "Valid candidate fact text", 2}}},
		{name: "Tier3_UnwhitelistedUpdate_FallbackInsert", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"UPDATE","supersedes_id":999,"fact_text":"Updated text here"}]}`, wantFacts: insertedBeside},
		{name: "Tier3_UpdateStoreErrorLogged", item: valid, neighborEmb: tier3Emb, llmResp: update10, sabotage: sabotageUpdateFact, wantErr: true, wantFacts: untouched},
		{name: "Tier3_ValidDelete", item: valid, neighborEmb: tier3Emb, llmResp: delete10},
		{name: "Tier3_Delete_WithSupersedesID", item: valid, neighborEmb: tier3Emb, llmResp: `{"facts":[{"action":"DELETE","supersedes_id":10}]}`},
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

	var s store.Store
	if !tc.nilStore {
		s = store.NewSQLStore(db)
	}
	var client store.OllamaClient
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

	err := reconcileCandidateFact(ctx, s, client, llm, tc.item, "")
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
		{name: "ValidArgumentsSuccess", threadID: "th-valid", wantAgent: config.DefaultStack},
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
	var s store.Store
	if !tc.nilStore {
		s = store.NewSQLStore(db)
	}
	var llm LLMClientFunc
	if !tc.nilLLM {
		llm = func(c context.Context, prompt string) (string, error) {
			return `{"facts":[]}`, nil
		}
	}

	var err error
	if tc.agent != "" {
		err = ExtractThreadFacts(context.Background(), s, &MockOllamaClient{embedding: angleEmbedding(0)}, llm, tc.threadID, tc.agent)
	} else {
		err = ExtractThreadFacts(context.Background(), s, &MockOllamaClient{embedding: angleEmbedding(0)}, llm, tc.threadID)
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
