package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestExtractLoadedSkillName(t *testing.T) {
	tests := []struct {
		name     string
		stepType string
		toolName string
		content  string
		want     string
	}{
		{
			name:     "EmptyContent",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  "",
			want:     "",
		},
		{
			name:     "PlannerResponseDirectPath",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `{"Path": "/skills/ha-operations/SKILL.md"}`,
			want:     "ha-operations",
		},
		{
			name:     "PlannerResponseDiscordSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/discord/ha-operations/SKILL.md`,
			want:     "ha-operations",
		},
		{
			name:     "PlannerResponseCommonSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/common/culinary-recipes/SKILL.md`,
			want:     "culinary-recipes",
		},
		{
			name:     "PlannerResponseVoiceSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/voice/voice-assistant/SKILL.md`,
			want:     "voice-assistant",
		},
		{
			name:     "PlannerResponseEphemeralSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/ephemeral/test-skill/SKILL.md`,
			want:     "test-skill",
		},
		{
			name:     "PlannerResponseWindowsBackslashes",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `C:\data\skills\discord\ha-operations\SKILL.md`,
			want:     "ha-operations",
		},
		{
			name:     "GenericStepYAML",
			stepType: "GENERIC",
			toolName: "",
			content:  "---\nname: ha-operations\ndescription: Manage HA",
			want:     "ha-operations",
		},
		{
			name:     "ToolStepYAMLNumbered",
			stepType: "TOOL",
			toolName: "read_file",
			content:  "1: ---\n2: name: culinary-recipes\n3: description: Cook",
			want:     "culinary-recipes",
		},
		{
			name:     "RunCommandStepYAML",
			stepType: "RUN_COMMAND",
			toolName: "bash",
			content:  "---\nname: my-script-skill\n",
			want:     "my-script-skill",
		},
		{
			name:     "NoSkillFound",
			stepType: "PLANNER_RESPONSE",
			toolName: "run_command",
			content:  "echo hello",
			want:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractLoadedSkillName(tc.stepType, tc.toolName, tc.content)
			if got != tc.want {
				t.Errorf("ExtractLoadedSkillName() = %q, want %q", got, tc.want)
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

func TestEvaluateProceduralCandidateAdmission(t *testing.T) {
	tests := []struct {
		name       string
		item       ExtractedProceduralItem
		wantAdmit  bool
		wantReason string
	}{
		{
			name: "AdmittedWithStepsAndAvoid",
			item: ExtractedProceduralItem{
				Trigger: "restart nomad service when stuck",
				Steps:   []string{"check status", "restart allocation"},
				Avoid:   []string{"do not kill without checking status"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "AdmittedWithStepsOnly",
			item: ExtractedProceduralItem{
				Trigger: "configure home assistant entity",
				Steps:   []string{"call ha_search", "apply entity change"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "AdmittedWithAvoidOnly",
			item: ExtractedProceduralItem{
				Trigger: "deploy docker container on host",
				Avoid:   []string{"do not use host networking mode"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "EmptyTrigger",
			item: ExtractedProceduralItem{
				Trigger: "   ",
				Steps:   []string{"step 1"},
			},
			wantAdmit:  false,
			wantReason: "empty_trigger",
		},
		{
			name: "TriggerTooShort",
			item: ExtractedProceduralItem{
				Trigger: "run",
				Steps:   []string{"step 1"},
			},
			wantAdmit:  false,
			wantReason: "trigger_too_short",
		},
		{
			name: "EphemeralTelemetryPR",
			item: ExtractedProceduralItem{
				Trigger: "verify changes for PR #1234 before deploying",
				Steps:   []string{"run tests"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "EphemeralTelemetryBuildID",
			item: ExtractedProceduralItem{
				Trigger: "execute build-1792688043-932febae in pipeline",
				Steps:   []string{"run tests"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "EphemeralTelemetryDurableOverride",
			item: ExtractedProceduralItem{
				Trigger: "always verify pull request #1234 invariant strictly before commit",
				Steps:   []string{"run verify.sh"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "EphemeralTelemetryInSteps",
			item: ExtractedProceduralItem{
				Trigger: "reconcile repository branch state",
				Steps:   []string{"check status of PR #1234 before merging"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "EphemeralTelemetryInAvoid",
			item: ExtractedProceduralItem{
				Trigger: "deploy service allocation on cluster",
				Steps:   []string{"run nomad job"},
				Avoid:   []string{"wait for build-1792688043-932febae to finish"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "MissingStepsAndAvoid",
			item: ExtractedProceduralItem{
				Trigger: "some valid trigger description",
				Steps:   nil,
				Avoid:   nil,
			},
			wantAdmit:  false,
			wantReason: "missing_steps_and_avoid",
		},
		{
			name: "WhitespaceOnlyStepsAndAvoid",
			item: ExtractedProceduralItem{
				Trigger: "some valid trigger description",
				Steps:   []string{"   ", ""},
				Avoid:   []string{"   "},
			},
			wantAdmit:  false,
			wantReason: "missing_steps_and_avoid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admit, reason := EvaluateProceduralCandidateAdmission(tc.item)
			if admit != tc.wantAdmit {
				t.Errorf("admit = %v, want %v (reason: %q)", admit, tc.wantAdmit, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

type sqlStoreProceduralCandidatesCase struct {
	name        string
	action      string
	nilDB       bool
	cand        ProceduralCandidate
	ids         []int64
	prNum       int
	queryErr    error
	execErr     error
	rowSQL      string
	rowsSQL     string
	wantErr     bool
	wantCount   int
	wantID      int64
	wantNoQuery bool
}

func runSQLStoreProceduralCandidatesCase(t *testing.T, tc sqlStoreProceduralCandidatesCase, dbMem *sql.DB) {
	t.Helper()
	var capturedQuery string
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			execErr:       tc.execErr,
			rowSQL:        tc.rowSQL,
			rowsSQL:       tc.rowsSQL,
			capturedQuery: &capturedQuery,
		})
	}

	ctx := context.Background()
	switch tc.action {
	case "insert":
		id, err := store.InsertProceduralCandidate(ctx, tc.cand)
		if (err != nil) != tc.wantErr {
			t.Fatalf("InsertProceduralCandidate() err = %v, wantErr = %v", err, tc.wantErr)
		}
		if tc.wantID > 0 && id != tc.wantID {
			t.Errorf("InsertProceduralCandidate() id = %d, want %d", id, tc.wantID)
		}
	case "get_pending":
		cands, err := store.GetPendingProceduralCandidates(ctx)
		if (err != nil) != tc.wantErr {
			t.Fatalf("GetPendingProceduralCandidates() err = %v, wantErr = %v", err, tc.wantErr)
		}
		if len(cands) != tc.wantCount {
			t.Errorf("GetPendingProceduralCandidates() count = %d, want %d", len(cands), tc.wantCount)
		}
	default:
		runSQLStoreProceduralStatusAction(t, store, tc, capturedQuery)
	}
}

func runSQLStoreProceduralStatusAction(t *testing.T, store *SQLStore, tc sqlStoreProceduralCandidatesCase, capturedQuery string) {
	t.Helper()
	ctx := context.Background()
	switch tc.action {
	case "mark_consolidated":
		err := store.MarkProceduralCandidatesConsolidated(ctx, tc.ids, tc.prNum)
		if (err != nil) != tc.wantErr {
			t.Fatalf("MarkProceduralCandidatesConsolidated() err = %v, wantErr = %v", err, tc.wantErr)
		}
	case "mark_skipped":
		err := store.MarkProceduralCandidatesSkipped(ctx, tc.ids)
		if (err != nil) != tc.wantErr {
			t.Fatalf("MarkProceduralCandidatesSkipped() err = %v, wantErr = %v", err, tc.wantErr)
		}
	}
	if tc.wantNoQuery && capturedQuery != "" {
		t.Errorf("expected no query for empty IDs, got: %s", capturedQuery)
	}
}

func TestSQLStore_ProceduralCandidates(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []sqlStoreProceduralCandidatesCase{
		{
			name:    "InsertNilDB",
			action:  "insert",
			nilDB:   true,
			cand:    ProceduralCandidate{Trigger: "valid trigger"},
			wantErr: true,
		},
		{
			name:    "InsertEmptyTrigger",
			action:  "insert",
			cand:    ProceduralCandidate{Trigger: "   "},
			wantErr: true,
		},
		{
			name:     "InsertQueryError",
			action:   "insert",
			cand:     ProceduralCandidate{Trigger: "valid trigger"},
			queryErr: errors.New("insert failed"),
			wantErr:  true,
		},
		{
			name:   "InsertSuccessWithPR",
			action: "insert",
			cand: ProceduralCandidate{
				ThreadID:    "t-100",
				Domain:      "discord",
				LoadedSkill: "ha-operations",
				Trigger:     "turn on porch light when motion detected",
				Steps:       []string{"call ha_search", "call ha_call_write_tool"},
				Avoid:       []string{"avoid loop"},
				Embedding:   testEmbedding(0.5),
				Status:      "pending",
				PRNumber:    42,
			},
			rowSQL:  "SELECT 101",
			wantID:  101,
			wantErr: false,
		},
		{
			name:   "InsertSuccessDefaults",
			action: "insert",
			cand: ProceduralCandidate{
				Trigger:   "valid trigger pattern",
				Embedding: testEmbedding(0.1),
			},
			rowSQL:  "SELECT 102",
			wantID:  102,
			wantErr: false,
		},
		{
			name:    "GetPendingNilDB",
			action:  "get_pending",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "GetPendingQueryError",
			action:   "get_pending",
			queryErr: errors.New("select failed"),
			wantErr:  true,
		},
		{
			name:    "GetPendingScanError",
			action:  "get_pending",
			rowsSQL: "SELECT 'bad_row'",
			wantErr: true,
		},
		{
			name:   "GetPendingSuccessParsing",
			action: "get_pending",
			rowsSQL: "SELECT 1, 't-1', 'discord', 'ha-operations', 'motion detected', '[\"step 1\"]', '[\"avoid 1\"]', '[0.1,0.2]', 'pending', 0, '2026-10-01 12:00:00' " +
				"UNION ALL SELECT 2, 't-2', 'discord', '', 'unloaded skill trigger', '', '', '[0.3,0.4]', 'pending', 5, '2026-10-01 13:00:00'",
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:    "MarkConsolidatedNilDB",
			action:  "mark_consolidated",
			nilDB:   true,
			ids:     []int64{1},
			prNum:   10,
			wantErr: true,
		},
		{
			name:        "MarkConsolidatedEmptyIDs",
			action:      "mark_consolidated",
			ids:         nil,
			prNum:       10,
			wantErr:     false,
			wantNoQuery: true,
		},
		{
			name:    "MarkConsolidatedExecError",
			action:  "mark_consolidated",
			ids:     []int64{1, 2},
			prNum:   10,
			execErr: errors.New("update failed"),
			wantErr: true,
		},
		{
			name:    "MarkConsolidatedSuccess",
			action:  "mark_consolidated",
			ids:     []int64{1, 2},
			prNum:   10,
			wantErr: false,
		},
		{
			name:    "MarkSkippedNilDB",
			action:  "mark_skipped",
			nilDB:   true,
			ids:     []int64{1},
			wantErr: true,
		},
		{
			name:        "MarkSkippedEmptyIDs",
			action:      "mark_skipped",
			ids:         nil,
			wantErr:     false,
			wantNoQuery: true,
		},
		{
			name:    "MarkSkippedExecError",
			action:  "mark_skipped",
			ids:     []int64{3},
			execErr: errors.New("update failed"),
			wantErr: true,
		},
		{
			name:    "MarkSkippedSuccess",
			action:  "mark_skipped",
			ids:     []int64{3, 4},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSQLStoreProceduralCandidatesCase(t, tc, dbMem)
		})
	}
}

func TestFormatProceduralCandidateEmbeddingText(t *testing.T) {
	mk := func(trig string, steps, avoid []string) ExtractedProceduralItem {
		return ExtractedProceduralItem{Trigger: trig, Steps: steps, Avoid: avoid}
	}
	tests := []struct {
		name string
		item ExtractedProceduralItem
		want string
	}{
		{"StepsAndAvoid", mk("find", []string{" include -maxdepth ", "", "target subdir"}, []string{" unconstrained find ", " "}), "Steps: include -maxdepth; target subdir\nAvoid: unconstrained find"},
		{"StepsOnly", mk("find", []string{"include -maxdepth"}, nil), "Steps: include -maxdepth"},
		{"AvoidOnly", mk("find", nil, []string{"unconstrained find"}), "Avoid: unconstrained find"},
		{"FallbackTrigger", mk("  when running find  ", nil, nil), "when running find"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatProceduralCandidateEmbeddingText(tc.item); got != tc.want {
				t.Errorf("FormatProceduralCandidateEmbeddingText() = %q, want %q", got, tc.want)
			}
		})
	}
}

type processThreadFactsProceduralCase struct {
	name                 string
	llmResp              string
	storeFailOp          string
	storeFailErr         error
	ollamaErr            error
	preexistingCandidate *ProceduralCandidate
	trajectoryCalls      []string
	wantCandidateCount   int
	wantPromptContains   string
	wantEmbeddedText     string
	wantWatermark        bool
	wantErr              bool
}

func processThreadFactsProceduralCases() []processThreadFactsProceduralCase {
	return []processThreadFactsProceduralCase{
		{
			name: "SuccessAdmitCandidate",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"domain": "discord",
						"loaded_skill": "ha-operations",
						"trigger": "turning on bedroom light when dark",
						"steps": ["check sensor", "call ha_write_tool"],
						"avoid": ["avoid loop"]
					}
				]
			}`,
			wantCandidateCount: 1,
			wantEmbeddedText:   "Steps: check sensor; call ha_write_tool\nAvoid: avoid loop",
			wantWatermark:      true,
		},
		{
			name: "RejectCandidateAdmissionEmptyTrigger",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"trigger": "   ",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 0,
			wantWatermark:      true,
		},
		{
			name: "RejectCandidateAdmissionEphemeralTelemetry",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"trigger": "deploying PR #999 to production",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 0,
			wantWatermark:      true,
		},
		{
			name: "SkipNearDuplicateSameLoadedSkill",
			preexistingCandidate: &ProceduralCandidate{
				LoadedSkill: "ha-operations",
				Trigger:     "turning on bedroom light when dark",
				Embedding:   testEmbedding(1.0),
				Status:      "pending",
			},
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"loaded_skill": "ha-operations",
						"trigger": "turning on bedroom light when dark",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 1,
			wantWatermark:      true,
		},
		{
			name: "DifferentLoadedSkillAdmitted",
			preexistingCandidate: &ProceduralCandidate{
				LoadedSkill: "other-skill",
				Trigger:     "turning on bedroom light when dark",
				Embedding:   testEmbedding(1.0),
				Status:      "pending",
			},
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"loaded_skill": "ha-operations",
						"trigger": "turning on bedroom light when dark",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 2,
			wantWatermark:      true,
		},
		{
			name: "CapAtThreeCandidates",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"loaded_skill": "skill-1", "trigger": "procedural task step number 1", "steps": ["s1"]},
					{"loaded_skill": "skill-2", "trigger": "procedural task step number 2", "steps": ["s2"]},
					{"loaded_skill": "skill-3", "trigger": "procedural task step number 3", "steps": ["s3"]},
					{"loaded_skill": "skill-4", "trigger": "procedural task step number 4", "steps": ["s4"]},
					{"loaded_skill": "skill-5", "trigger": "procedural task step number 5", "steps": ["s5"]}
				]
			}`,
			wantCandidateCount: 3,
			wantWatermark:      true,
		},
		{
			name:      "EmbeddingFailureContinues",
			ollamaErr: errors.New("ollama down"),
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"trigger": "valid procedural task step", "steps": ["s1"]}
				]
			}`,
			wantCandidateCount: 0,
			wantWatermark:      false,
		},
		{
			name:         "StoreGetPendingErrorContinues",
			storeFailOp:  "GetPendingProceduralCandidates",
			storeFailErr: errors.New("db read failure"),
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"trigger": "valid procedural task step", "steps": ["s1"]}
				]
			}`,
			wantCandidateCount: 1,
			wantWatermark:      false,
		},
		{
			name:         "StoreInsertErrorContinues",
			storeFailOp:  "InsertProceduralCandidate",
			storeFailErr: errors.New("db insert failure"),
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"trigger": "valid procedural task step", "steps": ["s1"]}
				]
			}`,
			wantCandidateCount: 0,
			wantWatermark:      false,
		},
		{
			name: "LoadedSkillsInPrompt",
			trajectoryCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
			},
			llmResp:            `{"facts": []}`,
			wantPromptContains: "LOADED SKILLS IN TURN:\n- ha-operations",
			wantCandidateCount: 0,
			wantWatermark:      true,
		},
	}
}

func setupProcessThreadFactsProceduralStore(tc processThreadFactsProceduralCase) *TestMemStore {
	mem := newTestMemStore()
	mem.unextractedTurns["th-proc"] = []Message{
		{ID: "m1", RowID: 1, Content: "user prompt", Status: "PENDING"},
	}
	if len(tc.trajectoryCalls) > 0 {
		mem.threadToolCalls["th-proc"] = tc.trajectoryCalls
	}
	if tc.preexistingCandidate != nil {
		_, _ = mem.InsertProceduralCandidate(context.Background(), *tc.preexistingCandidate)
	}
	if tc.storeFailOp != "" {
		mem.SetFail(tc.storeFailOp, tc.storeFailErr)
	}
	return mem
}

func assertProcessThreadFactsProceduralResults(t *testing.T, tc processThreadFactsProceduralCase, mem *TestMemStore, capturedPrompt, lastText string) {
	t.Helper()
	if tc.wantPromptContains != "" && !strings.Contains(capturedPrompt, tc.wantPromptContains) {
		t.Errorf("prompt missing %q:\n%s", tc.wantPromptContains, capturedPrompt)
	}
	if len(mem.proceduralCandidates) != tc.wantCandidateCount {
		t.Errorf("candidate count = %d, want %d", len(mem.proceduralCandidates), tc.wantCandidateCount)
	}
	if tc.wantEmbeddedText != "" && lastText != tc.wantEmbeddedText {
		t.Errorf("embedded text = %q, want %q", lastText, tc.wantEmbeddedText)
	}
	if tc.wantWatermark {
		if mem.watermarks["th-proc"] != 1 {
			t.Errorf("watermark = %d, want 1", mem.watermarks["th-proc"])
		}
	} else {
		if wm, ok := mem.watermarks["th-proc"]; ok && wm != 0 {
			t.Errorf("watermark unexpectedly advanced to %d, want not advanced", wm)
		}
	}
}

func runProcessThreadFactsProceduralCase(t *testing.T, tc processThreadFactsProceduralCase) {
	t.Helper()
	ctx := context.Background()
	mem := setupProcessThreadFactsProceduralStore(tc)
	client := &MockOllamaClient{
		embedding: testEmbedding(1.0),
		err:       tc.ollamaErr,
	}

	var capturedPrompt string
	llm := func(_ context.Context, prompt string) (string, error) {
		capturedPrompt = prompt
		return tc.llmResp, nil
	}

	err := processThreadFacts(ctx, mem, client, llm, "th-proc")
	if (err != nil) != tc.wantErr {
		t.Fatalf("processThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertProcessThreadFactsProceduralResults(t, tc, mem, capturedPrompt, client.lastText)
}

func TestProcessThreadFacts_ProceduralCandidates(t *testing.T) {
	tests := processThreadFactsProceduralCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runProcessThreadFactsProceduralCase(t, tc)
		})
	}
}

func testProcCand(id int64, skill, trigger string, avoid []string, emb []float32) ProceduralCandidate {
	return ProceduralCandidate{
		ID:          id,
		Domain:      "discord",
		LoadedSkill: skill,
		Trigger:     trigger,
		Steps:       []string{"step 1", "step 2", "step 3"},
		Avoid:       avoid,
		Embedding:   emb,
		Status:      "pending",
	}
}

func makeProceduralCandidateBatch(startID int64, count int) []ProceduralCandidate {
	cands := make([]ProceduralCandidate, count)
	for i := range count {
		cands[i] = ProceduralCandidate{
			ID:        startID + int64(i),
			Domain:    "discord",
			Trigger:   "psql missing",
			Steps:     []string{"use python"},
			Avoid:     []string{"avoid psql"},
			Embedding: testEmbeddingAt(2, 1.0),
		}
	}
	return cands
}

func testDiskSkill(name, domain, relPath, content string, emb []float32) DiskSkill {
	return DiskSkill{
		Name:        name,
		Domain:      domain,
		RelPath:     relPath,
		Description: "A test description for " + name,
		RawContent:  content,
		Embedding:   emb,
	}
}

func testEmbedding2D(x, y float32) []float32 {
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = x
	emb[1] = y
	return emb
}

func TestParseSkillFrontmatter(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantName    string
		wantDesc    string
		wantBody    string
		wantErr     bool
		errContains string
	}{
		{
			name: "ValidStandard",
			input: `---
name: my-skill
description: This is a standard valid description.
---
# Content Header
Body content text here.`,
			wantName: "my-skill",
			wantDesc: "This is a standard valid description.",
			wantBody: "# Content Header\nBody content text here.",
		},
		{
			name:     "ValidOuterCodeFencesMarkdown",
			input:    "```markdown\n---\nname: fenced-skill\ndescription: Fenced markdown description here.\n---\n# Fenced Content\n```",
			wantName: "fenced-skill",
			wantDesc: "Fenced markdown description here.",
			wantBody: "# Fenced Content",
		},
		{
			name:     "ValidOuterCodeFencesPlain",
			input:    "```\n---\nname: plain-fence\ndescription: Plain fenced description here.\n---\nBody text\n```",
			wantName: "plain-fence",
			wantDesc: "Plain fenced description here.",
			wantBody: "Body text",
		},
		{
			name:     "ValidCRLF",
			input:    "---\r\nname: crlf-skill\r\ndescription: CRLF description text here.\r\n---\r\nCRLF body text",
			wantName: "crlf-skill",
			wantDesc: "CRLF description text here.",
			wantBody: "CRLF body text",
		},
		{
			name:     "ValidEmptyBody",
			input:    "---\nname: no-body\ndescription: Description without any markdown body.\n---",
			wantName: "no-body",
			wantDesc: "Description without any markdown body.",
			wantBody: "",
		},
		{
			name:        "ErrMissingOpeningDelimiter",
			input:       "name: no-delims\ndescription: something",
			wantErr:     true,
			errContains: "missing frontmatter opening delimiter",
		},
		{
			name:        "ErrInvalidOpeningDelimiter",
			input:       "---invalid opening\nname: bad\n---\n",
			wantErr:     true,
			errContains: "invalid frontmatter opening delimiter",
		},
		{
			name:        "ErrMissingClosingDelimiter",
			input:       "---\nname: unclosed\ndescription: missing closing delimiter\n",
			wantErr:     true,
			errContains: "missing frontmatter closing delimiter",
		},
		{
			name:        "ErrInvalidYAML",
			input:       "---\n: bad : yaml :\n---\nbody",
			wantErr:     true,
			errContains: "invalid frontmatter yaml",
		},
		{
			name:        "ErrEmptyName",
			input:       "---\nname: \"   \"\ndescription: valid description text\n---\nbody",
			wantErr:     true,
			errContains: "frontmatter name and description must be non-empty",
		},
		{
			name:        "ErrEmptyDescription",
			input:       "---\nname: valid-name\ndescription: \"\"\n---\nbody",
			wantErr:     true,
			errContains: "frontmatter name and description must be non-empty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotName, gotDesc, gotBody, err := ParseSkillFrontmatter(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseSkillFrontmatter() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if gotName != tc.wantName {
				t.Errorf("name = %q, want %q", gotName, tc.wantName)
			}
			if gotDesc != tc.wantDesc {
				t.Errorf("desc = %q, want %q", gotDesc, tc.wantDesc)
			}
			if gotBody != tc.wantBody {
				t.Errorf("body = %q, want %q", gotBody, tc.wantBody)
			}
		})
	}
}

type scanCustomSkillsCase struct {
	name            string
	nonExistentRoot bool
	entries         []diskEntry
	mockClient      *MockOllamaClient
	wantSkillLen    int
	wantRelPaths    []string
	wantEmbsNil     bool
	wantErr         bool
	errContains     string
}

type diskEntry struct {
	relPath string
	content string
	isDir   bool
}

func setupScanCustomSkillsDir(t *testing.T, tc scanCustomSkillsCase) string {
	t.Helper()
	if tc.nonExistentRoot {
		return filepath.Join(t.TempDir(), "nonexistent")
	}
	rootDir := t.TempDir()
	for _, entry := range tc.entries {
		fullPath := filepath.Join(rootDir, entry.relPath)
		if entry.isDir {
			if err := os.MkdirAll(fullPath, 0o755); err != nil {
				t.Fatalf("failed to create dir %s: %v", fullPath, err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("failed to create parent dir for %s: %v", fullPath, err)
		}
		if err := os.WriteFile(fullPath, []byte(entry.content), 0o644); err != nil {
			t.Fatalf("failed to write test file %s: %v", fullPath, err)
		}
	}
	return rootDir
}

func assertScanCustomSkillsResults(t *testing.T, tc scanCustomSkillsCase, skills []DiskSkill) {
	t.Helper()
	if len(skills) != tc.wantSkillLen {
		t.Fatalf("skills len = %d, want %d", len(skills), tc.wantSkillLen)
	}
	for i, wantRel := range tc.wantRelPaths {
		if skills[i].RelPath != wantRel {
			t.Errorf("skill[%d].RelPath = %q, want %q", i, skills[i].RelPath, wantRel)
		}
	}
	if tc.wantEmbsNil {
		for i, s := range skills {
			if len(s.Embedding) != 0 {
				t.Errorf("skill[%d] embedding is not nil", i)
			}
		}
	}
}

func runScanCustomSkillsCase(t *testing.T, tc scanCustomSkillsCase) {
	t.Helper()
	rootDir := setupScanCustomSkillsDir(t, tc)

	var client OllamaClient
	if tc.mockClient != nil {
		client = tc.mockClient
	}

	skills, err := ScanCustomSkills(context.Background(), rootDir, client)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ScanCustomSkills() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
			t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
		}
		return
	}
	assertScanCustomSkillsResults(t, tc, skills)
}

func TestScanCustomSkills(t *testing.T) {
	validSkillMD := `---
name: sample-skill
description: This is a sample description for testing scanner.
---
# Sample Skill Instructions
`
	corruptSkillMD := `---
invalid yaml without name or closing
`
	commonMD := strings.Replace(validSkillMD, "sample-skill", "sample-common", 1)
	discordMD := strings.Replace(validSkillMD, "sample-skill", "sample-discord", 1)
	nilEmbMD := strings.Replace(validSkillMD, "sample-skill", "nil-emb", 1)
	errEmbMD := strings.Replace(validSkillMD, "sample-skill", "err-emb", 1)

	tests := []scanCustomSkillsCase{
		{
			name: "ValidSkillsScannedBothDomains",
			entries: []diskEntry{
				{relPath: "common/sample-common/SKILL.md", content: commonMD},
				{relPath: "discord/sample-discord/SKILL.md", content: discordMD},
			},
			mockClient:   &MockOllamaClient{embedding: testEmbedding(1.0)},
			wantSkillLen: 2,
			wantRelPaths: []string{
				"custom-skills/common/sample-common/SKILL.md",
				"custom-skills/discord/sample-discord/SKILL.md",
			},
		},
		{
			name:            "NonExistentDirTreatedAsEmptyWithoutError",
			nonExistentRoot: true,
			wantSkillLen:    0,
		},
		{
			name: "IgnoredNonDirectoryEntriesAndMissingSkillMD",
			entries: []diskEntry{
				{relPath: "common/README.md", content: "not a dir"},
				{relPath: "common/empty-dir", isDir: true},
			},
			wantSkillLen: 0,
		},
		{
			name: "NilOllamaClientLeavesEmbeddingNil",
			entries: []diskEntry{
				{relPath: "discord/nil-emb/SKILL.md", content: nilEmbMD},
			},
			wantSkillLen: 1,
			wantEmbsNil:  true,
		},
		{
			name: "CorruptSkillFileReturnsError",
			entries: []diskEntry{
				{relPath: "discord/bad-skill/SKILL.md", content: corruptSkillMD},
			},
			wantErr:     true,
			errContains: "failed to parse frontmatter",
		},
		{
			name: "OllamaEmbeddingErrorReturnsError",
			entries: []diskEntry{
				{relPath: "discord/err-emb/SKILL.md", content: errEmbMD},
			},
			mockClient:  &MockOllamaClient{err: errors.New("ollama failure")},
			wantErr:     true,
			errContains: "failed to generate embedding",
		},
		{
			name: "ReadDirDomainError",
			entries: []diskEntry{
				{relPath: "common", content: "not a dir"},
			},
			wantErr:     true,
			errContains: "failed to read custom skills domain",
		},
		{
			name: "ReadFileSkillError",
			entries: []diskEntry{
				{relPath: "discord/dir-skill/SKILL.md", isDir: true},
			},
			wantErr:     true,
			errContains: "failed to read skill file",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runScanCustomSkillsCase(t, tc)
		})
	}
}

type planSkillConsolidationCase struct {
	name              string
	pending           []ProceduralCandidate
	diskSkills        []DiskSkill
	wantBatches       int
	wantActions       []string
	wantDomains       []string
	wantCandCount     []int
	wantRequireReview []bool
	wantSkipped       []int64
}

func planSkillConsolidationCases(diskSkills []DiskSkill) []planSkillConsolidationCase {
	return []planSkillConsolidationCase{
		{
			name: "BlockedCoreSkillsRoutedWithRequireReview",
			pending: []ProceduralCandidate{
				testProcCand(1, "self-improvement", "trigger 1", nil, testEmbedding2D(0.80, 0.60)),
				testProcCand(2, "writing-plans", "trigger 2", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(3, "incident-triage", "trigger 3", nil, testEmbeddingAt(2, 1.0)),
			},
			diskSkills:        diskSkills,
			wantBatches:       2,
			wantActions:       []string{"EDIT", "CREATE"},
			wantDomains:       []string{"discord", "discord"},
			wantCandCount:     []int{1, 2},
			wantRequireReview: []bool{true, true},
			wantSkipped:       nil,
		},
		{
			name: "CaseA_LoadedSkillMatchTwoCandidates",
			pending: []ProceduralCandidate{
				testProcCand(10, "ha-ops", "trigger 1", nil, nil),
				testProcCand(11, "ha-ops", "trigger 2", nil, nil),
			},
			diskSkills:        diskSkills,
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{2},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseA_LoadedSkillMatchSingleCandidateWithAvoid",
			pending: []ProceduralCandidate{
				testProcCand(20, "ha-ops", "trigger with avoid", []string{"do not do this"}, nil),
			},
			diskSkills:        diskSkills,
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{1},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseA_LoadedSkillMatchSingleCandidateNoAvoidNotBatched",
			pending: []ProceduralCandidate{
				testProcCand(30, "ha-ops", "single candidate no avoid", nil, nil),
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
		},
		{
			name: "CaseB_UnloadedRoutingMissHighSimilarityToExistingSkill",
			pending: []ProceduralCandidate{
				testProcCand(40, "", "routing miss trigger", nil, testEmbedding2D(0.80, 0.60)),
			},
			diskSkills:        diskSkills,
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{1},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseB_UnmatchedLoadedSkillRoutingMiss",
			pending: []ProceduralCandidate{
				testProcCand(45, "unknown-skill", "routing miss trigger 2", nil, testEmbedding2D(0.60, 0.80)),
			},
			diskSkills:        diskSkills,
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"common"},
			wantCandCount:     []int{1},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseC_NovelWorkflowCliqueFormsCreateBatch",
			pending: []ProceduralCandidate{
				testProcCand(50, "", "novel workflow step 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(51, "", "novel workflow step 2", nil, testEmbeddingAt(2, 1.0)),
			},
			diskSkills:        diskSkills,
			wantBatches:       1,
			wantActions:       []string{"CREATE"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{2},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseC_ShortCandidatesUnderMinStepsDoNotFormClique",
			pending: []ProceduralCandidate{
				{ID: 55, Domain: "discord", Trigger: "find depth", Steps: []string{"use -maxdepth"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 56, Domain: "discord", Trigger: "find cmd", Steps: []string{"pass -maxdepth 2"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 57, Domain: "discord", Trigger: "find flag", Steps: []string{"pass -maxdepth 3"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 58, Domain: "discord", Trigger: "find limit", Steps: []string{"pass -maxdepth 4"}, Embedding: testEmbeddingAt(2, 1.0)},
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
		},
		{
			name:              "CaseD_ShortCandidatesFiveRepeatsEmitAlertBatch",
			pending:           makeProceduralCandidateBatch(71, 5),
			diskSkills:        diskSkills,
			wantBatches:       1,
			wantActions:       []string{"ALERT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{5},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseC_DissimilarCandidatesDoNotFormClique",
			pending: []ProceduralCandidate{
				testProcCand(60, "", "cand a", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(61, "", "cand b", nil, testEmbeddingAt(3, 1.0)),
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
		},
		{
			name: "MixedCasesComprehensive",
			pending: []ProceduralCandidate{
				testProcCand(1, "incident-triage", "blocked unmatched", nil, nil),
				testProcCand(2, "ha-ops", "case a 1", nil, nil),
				testProcCand(3, "ha-ops", "case a 2", nil, nil),
				testProcCand(4, "", "case b miss", nil, testEmbedding2D(0.0, 1.0)),
				testProcCand(5, "self-improvement", "case c 1", nil, testEmbeddingAt(4, 1.0)),
				testProcCand(6, "", "case c 2", nil, testEmbeddingAt(4, 1.0)),
			},
			diskSkills:        diskSkills,
			wantBatches:       3,
			wantActions:       []string{"EDIT", "EDIT", "CREATE"},
			wantDomains:       []string{"discord", "common", "discord"},
			wantCandCount:     []int{2, 1, 2},
			wantRequireReview: []bool{false, false, true},
			wantSkipped:       nil,
		},
	}
}

func assertPlanSkillBatches(t *testing.T, batches []SkillConsolidationBatch, tc planSkillConsolidationCase) {
	t.Helper()
	if len(batches) != tc.wantBatches {
		t.Fatalf("batches len = %d, want %d", len(batches), tc.wantBatches)
	}
	for i, wantAct := range tc.wantActions {
		if batches[i].Action != wantAct {
			t.Errorf("batch[%d].Action = %q, want %q", i, batches[i].Action, wantAct)
		}
		if batches[i].Domain != tc.wantDomains[i] {
			t.Errorf("batch[%d].Domain = %q, want %q", i, batches[i].Domain, tc.wantDomains[i])
		}
		if len(batches[i].Candidates) != tc.wantCandCount[i] {
			t.Errorf("batch[%d].Candidates len = %d, want %d", i, len(batches[i].Candidates), tc.wantCandCount[i])
		}
		if i < len(tc.wantRequireReview) && batches[i].RequireReview != tc.wantRequireReview[i] {
			t.Errorf("batch[%d].RequireReview = %v, want %v", i, batches[i].RequireReview, tc.wantRequireReview[i])
		}
	}
}

func assertPlanSkillSkipped(t *testing.T, skipped []int64, wantSkipped []int64) {
	t.Helper()
	if len(skipped) != len(wantSkipped) {
		t.Fatalf("skipped len = %d, want %d", len(skipped), len(wantSkipped))
	}
	for i, wantID := range wantSkipped {
		if skipped[i] != wantID {
			t.Errorf("skipped[%d] = %d, want %d", i, skipped[i], wantID)
		}
	}
}

func runPlanSkillConsolidationCase(t *testing.T, tc planSkillConsolidationCase) {
	t.Helper()
	batches, skipped := PlanSkillConsolidation(tc.pending, tc.diskSkills)
	assertPlanSkillBatches(t, batches, tc)
	assertPlanSkillSkipped(t, skipped, tc.wantSkipped)
}

func TestPlanSkillConsolidation(t *testing.T) {
	skillA := testDiskSkill("ha-ops", "discord", "custom-skills/discord/ha-ops/SKILL.md", "content", testEmbedding2D(1.0, 0.0))
	skillB := testDiskSkill("deploy-flow", "common", "custom-skills/common/deploy-flow/SKILL.md", "content", testEmbedding2D(0.0, 1.0))
	diskSkills := []DiskSkill{skillA, skillB}

	tests := planSkillConsolidationCases(diskSkills)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPlanSkillConsolidationCase(t, tc)
		})
	}
}
