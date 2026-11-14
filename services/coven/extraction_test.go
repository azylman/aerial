package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
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
			watermarkSQL: "SELECT 0, NULL",
			eriBody:      `{"status":"ok","tasks":[{"thread_summary":"summary text"}]}`,
			wantSummary:  "summary text",
		},
		{
			name:         "EriTasksError",
			threadID:     "t-1",
			watermarkSQL: "SELECT 15, NULL",
			eriStatus:    http.StatusInternalServerError,
			wantErr:      true,
		},
		{
			name:         "SuccessChronologicalOrder",
			threadID:     "t-1",
			watermarkSQL: "SELECT 20, NULL",
			eriBody: `{"status":"ok","tasks":[
				{"id":"m2","row_id":12,"thread_id":"t-1","prompt":"second prompt","response_text":"second response","status":"COMPLETED","is_admin":true,"thread_summary":"Session summary here"},
				{"id":"m1","row_id":8,"thread_id":"t-1","prompt":"first prompt","response_text":"first response","status":"COMPLETED","is_admin":true,"thread_summary":"Session summary here"}
			]}`,
			priorLimit:    -1,
			wantSummary:   "Session summary here",
			wantTurnCount: 2,
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

	tests := []threadTrajectoryContextTestCase{
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
			name:     "DBQueryError",
			threadID: "t-1",
			queryErr: errors.New("db disconnect"),
			wantErr:  true,
		},
		{
			name:         "EriTasksError",
			threadID:     "t-1",
			watermarkSQL: "SELECT 0, NULL",
			tasksStatus:  http.StatusInternalServerError,
			wantErr:      true,
		},
		{
			name:         "EmptySessions",
			threadID:     "t-1",
			watermarkSQL: "SELECT 0, NULL",
			tasksBody:    `{"status":"ok","tasks":[{"session_id":"","previous_session_id":""}]}`,
			wantErr:      false,
		},
		{
			name:         "TranscriptSearchError",
			threadID:     "t-1",
			watermarkSQL: "SELECT 0, NULL",
			tasksBody:    `{"status":"ok","tasks":[{"session_id":"s1","previous_session_id":"s0"}]}`,
			searchStatus: http.StatusInternalServerError,
			wantErr:      true,
		},
		{
			name:         "SuccessParsingAndDeduplication",
			threadID:     "t-1",
			watermarkSQL: "SELECT 0, '2026-01-01T00:00:00Z'",
			tasksBody:    `{"status":"ok","tasks":[{"session_id":"s1","previous_session_id":"s0"}]}`,
			searchBody: `{"steps":[
				{"step_type":"USER_INPUT","tool_name":"","content":"<retrieved_memory>\n- [id:42] User likes matcha\n- [id:42] User likes matcha\n- [id:43] Prefers dark mode\n</retrieved_memory>"},
				{"step_type":"PLANNER_RESPONSE","tool_name":"memory_search","content":"{\"query\": \"matcha\"}"},
				{"step_type":"USER_INPUT","tool_name":"","content":"<\\RETRIEVED_MEMORY>\n- [id:42] User likes matcha\n- [id:44] Morning coffee at 8am\n<\\/RETRIEVED_MEMORY>"},
				{"step_type":"OTHER","tool_name":"","content":"ignored"}
			]}`,
			wantErr: false,
			wantFacts: []string{
				"- [id:42] User likes matcha",
				"- [id:43] Prefers dark mode",
				"- [id:44] Morning coffee at 8am",
			},
			wantToolCalls: []string{
				`memory_search({"query": "matcha"})`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGetThreadTrajectoryContextCase(t, tc, dbMem)
		})
	}
}

func TestTruncateSingleLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "Empty",
			input:    "",
			maxLen:   10,
			expected: "",
		},
		{
			name:     "WhitespaceCollapse",
			input:    "   hello \n\t  world  \r\n test   ",
			maxLen:   50,
			expected: "hello world test",
		},
		{
			name:     "Truncated",
			input:    "this is a long sentence that exceeds the limit",
			maxLen:   10,
			expected: "this is a ",
		},
		{
			name:     "NoLimit",
			input:    "sentence with spaces",
			maxLen:   0,
			expected: "sentence with spaces",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := truncateSingleLine(tc.input, tc.maxLen)
			if res != tc.expected {
				t.Errorf("truncateSingleLine(%q, %d) = %q, want %q", tc.input, tc.maxLen, res, tc.expected)
			}
		})
	}
}

func TestExtractTrajectorySignals(t *testing.T) {
	tests := []struct {
		name          string
		stepType      string
		toolName      string
		content       string
		wantFacts     []string
		wantToolCalls string
	}{
		{
			name:     "UserInputStandardRetrievedMemory",
			stepType: "USER_INPUT",
			content:  "<retrieved_memory>\n- [id:1] User likes matcha\n- [id:2] Prefers dark mode\n</retrieved_memory>",
			wantFacts: []string{
				"- [id:1] User likes matcha",
				"- [id:2] Prefers dark mode",
			},
		},
		{
			name:     "UserInputSanitizedEscapedRetrievedMemory",
			stepType: "USER_INPUT",
			content:  "<\x5cRETRIEVED_MEMORY>\n- [id:10] Fact A\n- [id:10] Fact A\n<\x5c/RETRIEVED_MEMORY>",
			wantFacts: []string{
				"- [id:10] Fact A",
			},
		},
		{
			name:      "UserInputNoRetrievedMemory",
			stepType:  "USER_INPUT",
			content:   "Just regular user content",
			wantFacts: nil,
		},
		{
			name:          "PlannerResponseWithTool",
			stepType:      "PLANNER_RESPONSE",
			toolName:      "memory_search",
			content:       "{\"query\": \n \"something\"}",
			wantToolCalls: "memory_search({\"query\": \"something\"})",
		},
		{
			name:     "PlannerResponseEmptyTool",
			stepType: "PLANNER_RESPONSE",
			toolName: "",
			content:  "some thought",
		},
		{
			name:     "OtherStepType",
			stepType: "SYSTEM",
			content:  "<retrieved_memory>- [id:1] test</retrieved_memory>",
		},
		{
			name:          "PlannerResponseWithSkill",
			stepType:      "PLANNER_RESPONSE",
			toolName:      "read_file",
			content:       `{"Path": "/skills/ha-operations/SKILL.md"}`,
			wantToolCalls: `read_file({"Path": "/skills/ha-operations/SKILL.md"}) [SKILL_LOADED: ha-operations]`,
		},
		{
			name:          "GenericStepWithSkill",
			stepType:      "GENERIC",
			toolName:      "",
			content:       "---\nname: ha-operations\ndescription: test",
			wantToolCalls: `[SKILL_LOADED: ha-operations]`,
		},
		{
			name:          "ToolStepOutputNoSkill",
			stepType:      "TOOL",
			toolName:      "read_file",
			content:       "file content without skill",
			wantToolCalls: `-> OUTPUT(read_file): file content without skill`,
		},
		{
			name:          "RunCommandStepEmptyTool",
			stepType:      "RUN_COMMAND",
			toolName:      "",
			content:       "command line output",
			wantToolCalls: `-> OUTPUT(run_command): command line output`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			facts, tool := extractTrajectorySignals(tc.stepType, tc.toolName, tc.content)
			if len(facts) != len(tc.wantFacts) {
				t.Fatalf("len(facts) = %d, want %d (%v)", len(facts), len(tc.wantFacts), facts)
			}
			for i := range facts {
				if facts[i] != tc.wantFacts[i] {
					t.Errorf("facts[%d] = %q, want %q", i, facts[i], tc.wantFacts[i])
				}
			}
			if tool != tc.wantToolCalls {
				t.Errorf("tool = %q, want %q", tool, tc.wantToolCalls)
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

func TestExtractActiveConversationFacts(t *testing.T) {
	ctx := context.Background()
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []extractActiveConversationFactsTestCase{
		{
			name:      "OverlappingExecutionSkipped",
			ctx:       ctx,
			threads:   []string{"th-overlap"},
			lockMutex: true,
			wantErr:   false,
		},
		{
			name:     "NilStoreFails",
			ctx:      ctx,
			nilStore: true,
			wantErr:  true,
		},
		{
			name:      "NilClientFails",
			ctx:       ctx,
			nilClient: true,
			wantErr:   true,
		},
		{
			name:    "NilLLMFails",
			ctx:     ctx,
			nilLLM:  true,
			wantErr: true,
		},
		{
			name:    "NoActiveThreads",
			ctx:     ctx,
			threads: nil,
			wantErr: false,
		},
		{
			name:        "GetActiveConversationsError",
			ctx:         ctx,
			storeFailOp: "GetActiveConversationsForExtraction",
			wantErr:     true,
		},
		{
			name:        "GetMaxMessageRowIDErrorContinues",
			ctx:         ctx,
			threads:     []string{"th-1"},
			storeFailOp: "GetMaxMessageRowID",
			wantErr:     false,
		},
		{
			name:        "GetUnextractedAdminTurnsErrorContinues",
			ctx:         ctx,
			threads:     []string{"th-1"},
			storeFailOp: "GetUnextractedAdminTurns",
			wantErr:     false,
		},
		{
			name:    "EmptyTranscriptMarksWatermark",
			ctx:     ctx,
			threads: []string{"th-empty"},
			turns: []Message{
				{Content: "   ", ResponseText: "   ", RowID: 15},
			},
			wantErr:       false,
			wantWatermark: 15,
		},
		{
			name:          "EmptyTurnsSliceFallsBackToThreadMaxRowID",
			ctx:           ctx,
			threads:       []string{"th-fallback-max"},
			threadMaxID:   42,
			wantErr:       false,
			wantWatermark: 42,
		},
		{
			name:          "WatermarkCappedToExtractedTurnsMaxRowID",
			ctx:           ctx,
			threads:       []string{"th-capped"},
			turns:         []Message{{Content: "Turn 50", ResponseText: "Resp 50", RowID: 50}},
			threadMaxID:   100,
			llmResp:       `{"facts":[]}`,
			wantErr:       false,
			wantWatermark: 50,
		},
		{
			name:        "EmptyTranscriptWatermarkErrorContinues",
			ctx:         ctx,
			threads:     []string{"th-empty-err"},
			turns:       []Message{{Content: " ", ResponseText: " ", RowID: 16}},
			storeFailOp: "UpdateConversationFactWatermark",
			wantErr:     false,
		},
		{
			name:    "LLMCallFailsContinues",
			ctx:     ctx,
			threads: []string{"th-llm-fail"},
			turns: []Message{
				{Content: "turn 1", ResponseText: "resp 1", RowID: 20},
			},
			llmErr:  errors.New("llm down"),
			wantErr: false,
		},
		{
			name:    "ParseJSONFailsContinues",
			ctx:     ctx,
			threads: []string{"th-bad-json"},
			turns: []Message{
				{Content: "turn 1", ResponseText: "resp 1", RowID: 25},
			},
			llmResp: "invalid json string",
			wantErr: false,
		},
		{
			name:        "ExistingFactsIncludedInPrompt",
			ctx:         ctx,
			threads:     []string{"th-existing"},
			turns:       []Message{{Content: "User lives in Metropolis", ResponseText: "Recorded.", RowID: 28}},
			seedNearest: true,
			llmResp:     `{"facts":[]}`,
			wantErr:     false,
		},
		{
			name:    "FullReconciliationSuccess",
			ctx:     ctx,
			threads: []string{"th-full"},
			turns: []Message{
				{Content: "User's address is 123 Example St", ResponseText: "Got it.", RowID: 30},
			},
			llmResp: `{"facts":[
				{"action":"ADD","category":"user_preference","fact_text":"User loves espresso","importance_score":0.9},
				{"action":"UPDATE","supersedes_id":1,"category":"user_preference","fact_text":"User updated preference","importance_score":0.8},
				{"action":"DELETE","delete_id":2,"category":"general","fact_text":"Obsolete fact"},
				{"action":"NOOP"}
			]}`,
			wantErr:       false,
			wantWatermark: 30,
		},
		{
			name:    "ContextCanceledAborts",
			ctx:     canceledCtx,
			threads: []string{"th-cancel-1", "th-cancel-2"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runExtractActiveConversationFactsCase(t, tc, validEmb)
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

	tests := []reconcileCandidateFactTierTestCase{
		{
			name:     "NilStoreSafe",
			nilStore: true,
			item:     ExtractedFactItem{FactText: "Valid candidate fact text"},
		},
		{
			name:      "NilClientSafe",
			nilClient: true,
			item:      ExtractedFactItem{FactText: "Valid candidate fact text"},
		},
		{
			name: "UnadmittedTooShort",
			item: ExtractedFactItem{FactText: "short"},
		},
		{
			name: "UnadmittedNoopAction",
			item: ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail"},
		},
		{
			name: "UnadmittedDeleteAction",
			item: ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact text should not be inserted or reinforced"},
		},
		{
			name:      "EmbeddingErrorLogged",
			item:      ExtractedFactItem{FactText: "Valid candidate fact text"},
			ollamaErr: errors.New("embed fail"),
		},
		{
			name:        "FindNearestErrorLogged",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			storeFailOp: "FindNearestScoredFacts",
		},
		{
			name:         "Tier1_NoNeighbors_Insert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			wantInserted: true,
		},
		{
			name:         "Tier1_InsertErrorLogged",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			storeFailOp:  "InsertFact",
			wantInserted: false,
		},
		{
			name:           "Tier2_HighSimReinforce",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    highSimEmb,
			wantReinforced: true,
		},
		{
			name:           "Tier2_ReinforceErrorLogged",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    highSimEmb,
			storeFailOp:    "ReinforceFact",
			wantReinforced: false,
		},
		{
			name:         "Tier3_NilLLM_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			nilLLM:       true,
			wantInserted: true,
		},
		{
			name:         "Tier3_LLMError_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmErr:       errors.New("timeout"),
			wantInserted: true,
		},
		{
			name:         "Tier3_ParseError_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      "invalid json",
			wantInserted: true,
		},
		{
			name:         "Tier3_EmptyFacts_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[]}`,
			wantInserted: true,
		},
		{
			name:        "Tier3_ValidUpdate",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here"}]}`,
			wantUpdated: true,
		},
		{
			name:         "Tier3_UnwhitelistedUpdate_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[{"action":"UPDATE","supersedes_id":999,"fact_text":"Updated text here"}]}`,
			wantInserted: true,
		},
		{
			name:        "Tier3_UpdateStoreErrorLogged",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here"}]}`,
			storeFailOp: "UpdateFact",
		},
		{
			name:        "Tier3_ValidDelete",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"DELETE","delete_id":10}]}`,
			wantDeleted: true,
		},
		{
			name:        "Tier3_UnwhitelistedDelete_NOOP",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"DELETE","delete_id":999}]}`,
			wantDeleted: false,
		},
		{
			name:        "Tier3_DeleteStoreErrorLogged",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"DELETE","delete_id":10}]}`,
			storeFailOp: "DeleteFact",
		},
		{
			name:           "Tier3_NOOP",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    tier3Emb,
			llmResp:        `{"facts":[{"action":"NOOP"}]}`,
			wantReinforced: true,
		},
		{
			name:           "Tier3_NOOP_ExplicitSupersedesID",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    tier3Emb,
			llmResp:        `{"facts":[{"action":"NOOP","supersedes_id":10}]}`,
			wantReinforced: true,
		},
		{
			name:           "Tier3_NOOP_ReinforceErrorLogged",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    tier3Emb,
			llmResp:        `{"facts":[{"action":"NOOP"}]}`,
			storeFailOp:    "ReinforceFact",
			wantReinforced: false,
		},
		{
			name:         "Tier3_ADD",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[{"action":"ADD"}]}`,
			wantInserted: true,
		},
		{
			name:         "Tier3_DefaultUnknownAction",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[{"action":"OTHER"}]}`,
			wantInserted: true,
		},
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
	queryErr      error
	watermarkSQL  string
	eriStatus     int
	eriBody       string
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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tc.eriStatus != 0 {
				w.WriteHeader(tc.eriStatus)
			}
			if tc.eriBody != "" {
				_, _ = w.Write([]byte(tc.eriBody))
			}
		}))
		defer server.Close()

		store = NewSQLStoreWithBrainURL(queryMockDBTX{
			dbMem:    dbMem,
			queryErr: tc.queryErr,
			rowSQL:   tc.watermarkSQL,
		}, server.URL, server.Client())
	}

	summary, turns, err := store.GetThreadContextBeforeWatermark(ctx, tc.threadID, tc.priorLimit)
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

type threadTrajectoryContextTestCase struct {
	name          string
	nilDB         bool
	threadID      string
	queryErr      error
	watermarkSQL  string
	tasksStatus   int
	tasksBody     string
	searchStatus  int
	searchBody    string
	wantErr       bool
	wantFacts     []string
	wantToolCalls []string
}

func handleTrajectoryMock(w http.ResponseWriter, r *http.Request, tc threadTrajectoryContextTestCase) {
	if strings.HasPrefix(r.URL.Path, "/tasks") {
		handleTrajectoryTasks(w, tc.tasksStatus, tc.tasksBody)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/transcripts/search") {
		handleTrajectorySearch(w, tc.searchStatus, tc.searchBody)
		return
	}
	http.NotFound(w, r)
}

func handleTrajectoryTasks(w http.ResponseWriter, status int, body string) {
	if status != 0 {
		w.WriteHeader(status)
	}
	if body != "" {
		_, _ = w.Write([]byte(body))
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok","tasks":[]}`))
}

func handleTrajectorySearch(w http.ResponseWriter, status int, body string) {
	if status != 0 {
		w.WriteHeader(status)
	}
	if body != "" {
		_, _ = w.Write([]byte(body))
		return
	}
	_, _ = w.Write([]byte(`{"steps":[]}`))
}

func runGetThreadTrajectoryContextCase(t *testing.T, tc threadTrajectoryContextTestCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var store *SQLStore

	if tc.nilDB {
		store = &SQLStore{}
	} else {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleTrajectoryMock(w, r, tc)
		}))
		defer server.Close()

		store = NewSQLStoreWithBrainURL(queryMockDBTX{
			dbMem:    dbMem,
			queryErr: tc.queryErr,
			rowSQL:   tc.watermarkSQL,
		}, server.URL, server.Client())
	}

	facts, tools, err := store.GetThreadTrajectoryContext(ctx, tc.threadID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("GetThreadTrajectoryContext() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	assertTrajectoryFacts(t, facts, tc.wantFacts)
	assertTrajectoryTools(t, tools, tc.wantToolCalls)
}

func assertTrajectoryFacts(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(facts) = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("facts[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func assertTrajectoryTools(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(tools) = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("tools[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

type extractActiveConversationFactsTestCase struct {
	name          string
	ctx           context.Context
	nilStore      bool
	nilClient     bool
	nilLLM        bool
	threads       []string
	turns         []Message
	threadMaxID   int64
	storeFailOp   string
	llmResp       string
	llmErr        error
	lockMutex     bool
	seedNearest   bool
	wantErr       bool
	wantWatermark int64
}

func buildExtractionTestMemStore(tc extractActiveConversationFactsTestCase, validEmb []float32) *TestMemStore {
	memStore := newTestMemStore()
	memStore.activeThreads = tc.threads
	for _, tid := range tc.threads {
		memStore.unextractedTurns[tid] = tc.turns
		for _, trn := range tc.turns {
			if trn.RowID > memStore.maxRowIDs[tid] {
				memStore.maxRowIDs[tid] = trn.RowID
			}
		}
		if tc.threadMaxID > memStore.maxRowIDs[tid] {
			memStore.maxRowIDs[tid] = tc.threadMaxID
		}
	}
	memStore.facts[1] = &Fact{ID: 1, FactText: "Old preference"}
	memStore.facts[2] = &Fact{ID: 2, FactText: "Obsolete fact"}
	if tc.seedNearest {
		memStore.facts[99] = &Fact{ID: 99, FactText: "Existing nearest fact"}
		memStore.embeddings[99] = validEmb
	}
	if tc.storeFailOp != "" {
		memStore.SetFail(tc.storeFailOp, errors.New("simulated error"))
	}
	return memStore
}

func runExtractActiveConversationFactsCase(t *testing.T, tc extractActiveConversationFactsTestCase, validEmb []float32) {
	t.Helper()
	if tc.lockMutex {
		extractionMutex.Lock()
		defer extractionMutex.Unlock()
	}
	var store Store
	var memStore *TestMemStore
	if !tc.nilStore {
		memStore = buildExtractionTestMemStore(tc, validEmb)
		store = memStore
	}

	var client OllamaClient
	if !tc.nilClient {
		client = &MockOllamaClient{embedding: validEmb}
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

	err := ExtractActiveConversationFacts(tc.ctx, store, client, llm, 24)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ExtractActiveConversationFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantWatermark > 0 && memStore != nil {
		for _, tid := range tc.threads {
			if memStore.watermarks[tid] != tc.wantWatermark {
				t.Errorf("watermark for %s = %d, want %d", tid, memStore.watermarks[tid], tc.wantWatermark)
			}
		}
	}
}

type reconcileCandidateFactTierTestCase struct {
	name           string
	nilStore       bool
	nilClient      bool
	item           ExtractedFactItem
	ollamaErr      error
	storeFailOp    string
	neighborID     int64
	neighborEmb    []float32
	llmResp        string
	llmErr         error
	nilLLM         bool
	wantInserted   bool
	wantReinforced bool
	wantUpdated    bool
	wantDeleted    bool
}

func runReconcileCandidateFactTierCase(t *testing.T, tc reconcileCandidateFactTierTestCase, baseEmb []float32) {
	t.Helper()
	ctx := context.Background()
	var store Store
	var mem *TestMemStore
	if !tc.nilStore {
		mem = newTestMemStore()
		if tc.neighborID > 0 && len(tc.neighborEmb) > 0 {
			mem.facts[tc.neighborID] = &Fact{ID: tc.neighborID, FactText: "Existing neighbor text", ReinforceCount: 1}
			mem.embeddings[tc.neighborID] = tc.neighborEmb
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

	reconcileCandidateFact(ctx, store, client, llm, tc.item)

	if mem == nil {
		return
	}
	assertReconcileTierResults(t, tc, mem)
}

func assertReconcileTierResults(t *testing.T, tc reconcileCandidateFactTierTestCase, mem *TestMemStore) {
	t.Helper()
	if tc.wantInserted && len(mem.facts) == 0 && tc.neighborID == 0 {
		t.Errorf("expected fact to be inserted, but store has 0 facts")
	}
	if tc.wantReinforced && mem.facts[tc.neighborID].ReinforceCount < 2 {
		t.Errorf("expected neighbor %d to be reinforced, got count %d", tc.neighborID, mem.facts[tc.neighborID].ReinforceCount)
	}
	if tc.wantUpdated && mem.facts[tc.neighborID].FactText != "Updated text here" {
		t.Errorf("expected neighbor %d to be updated with new text, got %q", tc.neighborID, mem.facts[tc.neighborID].FactText)
	}
	if tc.wantDeleted && mem.facts[tc.neighborID] != nil {
		t.Errorf("expected neighbor %d to be deleted from store, but it remains", tc.neighborID)
	}
}
