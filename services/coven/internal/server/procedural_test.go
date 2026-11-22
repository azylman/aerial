package server

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/coven/internal/store"
)

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

// proceduralRows renders every procedural_candidates row in id order.
func proceduralRows(t testing.TB, db store.DBTX) []string {
	t.Helper()
	return queryStrings(t, db, `SELECT id || '|' || agent || '|' || thread_id || '|' || domain || '|' || loaded_skill || '|' || trigger_pattern || '|' ||
	steps_json::text || '|' || avoid_json::text || '|' || status || '|' || COALESCE(pr_number::text, '') FROM procedural_candidates ORDER BY id`)
}

// seedProceduralCandidates inserts candidates through the production
// InsertProceduralCandidate, replacing a missing embedding with a one-hot
// vector at dimension 100+i (orthogonal to every fixture embedding; the real
// column rejects empty vectors and zero vectors break cosine distance).
func seedProceduralCandidates(t testing.TB, s *store.SQLStore, cands []store.ProceduralCandidate) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(cands))
	for i, cand := range cands {
		if len(cand.Embedding) == 0 {
			cand.Embedding = testEmbeddingAt(100+i, 1.0)
		}
		id, err := s.InsertProceduralCandidate(context.Background(), cand)
		if err != nil {
			t.Fatalf("seed procedural candidate %q: %v", cand.Trigger, err)
		}
		ids = append(ids, id)
	}
	return ids
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
	sabotage             string
	ollamaErr            error
	preexistingCandidate *store.ProceduralCandidate
	trajectoryCalls      []string
	wantCandidates       []string
	wantPromptContains   string
	wantEmbeddedText     string
	wantWatermark        string
	wantErr              bool
}

// proceduralResp renders an extraction LLM response with no facts and the given
// procedural candidate JSON objects.
func proceduralResp(candidates ...string) string {
	return `{"facts": [], "procedural_candidates": [` + strings.Join(candidates, ", ") + `]}`
}

// pendingProcCand is a pending, already-embedded candidate for the bedroom-light trigger.
func pendingProcCand(loadedSkill string) *store.ProceduralCandidate {
	return &store.ProceduralCandidate{LoadedSkill: loadedSkill, Trigger: "turning on bedroom light when dark", Embedding: testEmbedding(1.0), Status: "pending"}
}

func processThreadFactsProceduralCases() []processThreadFactsProceduralCase {
	const bedroomLight = `{"loaded_skill": "ha-operations", "trigger": "turning on bedroom light when dark", "steps": ["step 1"]}`
	const validTask = `{"trigger": "valid procedural task step", "steps": ["s1"]}`
	return []processThreadFactsProceduralCase{
		{
			name: "SuccessAdmitCandidate",
			llmResp: proceduralResp(`{"domain": "discord", "loaded_skill": "ha-operations", "trigger": "turning on bedroom light when dark",
				"steps": ["check sensor", "call ha_write_tool"], "avoid": ["avoid loop"]}`),
			wantCandidates:   []string{`1|aerial|th-proc|discord|ha-operations|turning on bedroom light when dark|["check sensor", "call ha_write_tool"]|["avoid loop"]|pending|`},
			wantEmbeddedText: "Steps: check sensor; call ha_write_tool\nAvoid: avoid loop",
			wantWatermark:    "1",
		},
		{name: "RejectCandidateAdmissionEmptyTrigger", llmResp: proceduralResp(`{"trigger": "   ", "steps": ["step 1"]}`), wantWatermark: "1"},
		{name: "RejectCandidateAdmissionEphemeralTelemetry", llmResp: proceduralResp(`{"trigger": "deploying PR #999 to production", "steps": ["step 1"]}`), wantWatermark: "1"},
		{
			name:                 "SkipNearDuplicateSameLoadedSkill",
			preexistingCandidate: pendingProcCand("ha-operations"),
			llmResp:              proceduralResp(bedroomLight),
			wantCandidates:       []string{`1|aerial||discord|ha-operations|turning on bedroom light when dark|[]|[]|pending|`},
			wantWatermark:        "1",
		},
		{
			name:                 "DifferentLoadedSkillAdmitted",
			preexistingCandidate: pendingProcCand("other-skill"),
			llmResp:              proceduralResp(bedroomLight),
			wantCandidates: []string{
				`1|aerial||discord|other-skill|turning on bedroom light when dark|[]|[]|pending|`,
				`2|aerial|th-proc|discord|ha-operations|turning on bedroom light when dark|["step 1"]|[]|pending|`,
			},
			wantWatermark: "1",
		},
		{
			name: "CapAtThreeCandidates",
			llmResp: proceduralResp(
				`{"loaded_skill": "skill-1", "trigger": "procedural task step number 1", "steps": ["s1"]}`,
				`{"loaded_skill": "skill-2", "trigger": "procedural task step number 2", "steps": ["s2"]}`,
				`{"loaded_skill": "skill-3", "trigger": "procedural task step number 3", "steps": ["s3"]}`,
				`{"loaded_skill": "skill-4", "trigger": "procedural task step number 4", "steps": ["s4"]}`,
				`{"loaded_skill": "skill-5", "trigger": "procedural task step number 5", "steps": ["s5"]}`,
			),
			wantCandidates: []string{
				`1|aerial|th-proc|discord|skill-1|procedural task step number 1|["s1"]|[]|pending|`,
				`2|aerial|th-proc|discord|skill-2|procedural task step number 2|["s2"]|[]|pending|`,
				`3|aerial|th-proc|discord|skill-3|procedural task step number 3|["s3"]|[]|pending|`,
			},
			wantWatermark: "1",
		},
		{name: "EmbeddingFailureFails", ollamaErr: errors.New("ollama down"), llmResp: proceduralResp(validTask), wantWatermark: "0", wantErr: true},
		// The candidates table is gone, so its rows are not asserted.
		{name: "StoreGetPendingErrorFails", sabotage: sabotageProcedural, llmResp: proceduralResp(validTask), wantWatermark: "0", wantErr: true},
		{name: "StoreInsertErrorFails", sabotage: sabotageProceduralInsert, llmResp: proceduralResp(validTask), wantWatermark: "0", wantErr: true},
		{
			name:               "LoadedSkillsInPrompt",
			trajectoryCalls:    []string{"read_file(...) [SKILL_LOADED: ha-operations]"},
			llmResp:            `{"facts": []}`,
			wantPromptContains: "LOADED SKILLS IN TURN:\n- ha-operations\n",
			wantWatermark:      "1",
		},
	}
}

func runProcessThreadFactsProceduralCase(t *testing.T, tc processThreadFactsProceduralCase) {
	t.Helper()
	ctx := context.Background()
	s := newTestStore(t)
	db := s.DB()
	seedTurns(t, db, adminTurn("th-proc", 1, "user prompt", "ok"))
	s.SetIndexerClient(newIndexerServer(t, http.StatusOK, nil, tc.trajectoryCalls))
	if tc.preexistingCandidate != nil {
		seedProceduralCandidates(t, s, []store.ProceduralCandidate{*tc.preexistingCandidate})
	}
	execSQL(t, db, tc.sabotage)
	client := &MockOllamaClient{
		embedding: testEmbedding(1.0),
		err:       tc.ollamaErr,
	}

	var capturedPrompt string
	llm := func(_ context.Context, prompt string) (string, error) {
		capturedPrompt = prompt
		return tc.llmResp, nil
	}

	err := processThreadFacts(ctx, s, client, llm, "th-proc")
	if (err != nil) != tc.wantErr {
		t.Fatalf("processThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantPromptContains != "" && !strings.Contains(capturedPrompt, tc.wantPromptContains) {
		t.Errorf("prompt missing %q:\n%s", tc.wantPromptContains, capturedPrompt)
	}
	if tc.wantEmbeddedText != "" && client.lastText != tc.wantEmbeddedText {
		t.Errorf("embedded text = %q, want %q", client.lastText, tc.wantEmbeddedText)
	}
	if tc.sabotage != sabotageProcedural {
		if got := proceduralRows(t, db); !equalStrings(got, tc.wantCandidates) {
			t.Errorf("procedural candidates = %q, want %q", got, tc.wantCandidates)
		}
	}
	if got := watermarkRow(t, db, "th-proc"); got != tc.wantWatermark {
		t.Errorf("watermark = %s, want %s", got, tc.wantWatermark)
	}
}

func TestProcessThreadFacts_ProceduralCandidates(t *testing.T) {
	t.Parallel()
	for _, tc := range processThreadFactsProceduralCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessThreadFactsProceduralCase(t, tc)
		})
	}
}

func testProcCand(id int64, skill, trigger string, avoid []string, emb []float32) store.ProceduralCandidate {
	return store.ProceduralCandidate{
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

func makeProceduralCandidateBatch(startID int64, count int) []store.ProceduralCandidate {
	cands := make([]store.ProceduralCandidate, count)
	for i := range count {
		cands[i] = store.ProceduralCandidate{
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
	emb := make([]float32, store.ExpectedEmbeddingDim)
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

	var client store.OllamaClient
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
	pending           []store.ProceduralCandidate
	wantBatches       int
	wantActions       []string
	wantDomains       []string
	wantCandCount     []int
	wantRequireReview []bool
}

func planSkillConsolidationCases() []planSkillConsolidationCase {
	return []planSkillConsolidationCase{
		{
			name: "BlockedCoreSkillsRoutedWithRequireReview",
			pending: []store.ProceduralCandidate{
				testProcCand(1, "self-improvement", "trigger 1", nil, testEmbedding2D(0.80, 0.60)),
				testProcCand(2, "writing-plans", "trigger 2", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(3, "incident-triage", "trigger 3", nil, testEmbeddingAt(2, 1.0)),
			},
			wantBatches:       2,
			wantActions:       []string{"EDIT", "CREATE"},
			wantDomains:       []string{"discord", "discord"},
			wantCandCount:     []int{1, 2},
			wantRequireReview: []bool{true, true},
		},
		{
			name: "CaseA_LoadedSkillMatchTwoCandidates",
			pending: []store.ProceduralCandidate{
				testProcCand(10, "ha-ops", "trigger 1", nil, nil),
				testProcCand(11, "ha-ops", "trigger 2", nil, nil),
			},
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{2},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseA_LoadedSkillMatchSingleCandidateWithAvoid",
			pending: []store.ProceduralCandidate{
				testProcCand(20, "ha-ops", "trigger with avoid", []string{"do not do this"}, nil),
			},
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{1},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseA_LoadedSkillMatchSingleCandidateNoAvoidNotBatched",
			pending: []store.ProceduralCandidate{
				testProcCand(30, "ha-ops", "single candidate no avoid", nil, nil),
			},
			wantBatches: 0,
		},
		{
			name: "CaseB_UnloadedRoutingMissHighSimilarityToExistingSkill",
			pending: []store.ProceduralCandidate{
				testProcCand(40, "", "routing miss trigger", nil, testEmbedding2D(0.80, 0.60)),
			},
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{1},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseB_UnmatchedLoadedSkillRoutingMiss",
			pending: []store.ProceduralCandidate{
				testProcCand(45, "unknown-skill", "routing miss trigger 2", nil, testEmbedding2D(0.60, 0.80)),
			},
			wantBatches:       1,
			wantActions:       []string{"EDIT"},
			wantDomains:       []string{"common"},
			wantCandCount:     []int{1},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseC_NovelWorkflowCliqueFormsCreateBatch",
			pending: []store.ProceduralCandidate{
				testProcCand(50, "", "novel workflow step 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(51, "", "novel workflow step 2", nil, testEmbeddingAt(2, 1.0)),
			},
			wantBatches:       1,
			wantActions:       []string{"CREATE"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{2},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseC_ShortCandidatesUnderMinStepsDoNotFormClique",
			pending: []store.ProceduralCandidate{
				{ID: 55, Domain: "discord", Trigger: "find depth", Steps: []string{"use -maxdepth"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 56, Domain: "discord", Trigger: "find cmd", Steps: []string{"pass -maxdepth 2"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 57, Domain: "discord", Trigger: "find flag", Steps: []string{"pass -maxdepth 3"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 58, Domain: "discord", Trigger: "find limit", Steps: []string{"pass -maxdepth 4"}, Embedding: testEmbeddingAt(2, 1.0)},
			},
			wantBatches: 0,
		},
		{
			name:              "CaseD_ShortCandidatesFiveRepeatsEmitAlertBatch",
			pending:           makeProceduralCandidateBatch(71, 5),
			wantBatches:       1,
			wantActions:       []string{"ALERT"},
			wantDomains:       []string{"discord"},
			wantCandCount:     []int{5},
			wantRequireReview: []bool{false},
		},
		{
			name: "CaseC_DissimilarCandidatesDoNotFormClique",
			pending: []store.ProceduralCandidate{
				testProcCand(60, "", "cand a", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(61, "", "cand b", nil, testEmbeddingAt(3, 1.0)),
			},
			wantBatches: 0,
		},
		{
			name: "MixedCasesComprehensive",
			pending: []store.ProceduralCandidate{
				testProcCand(1, "incident-triage", "blocked unmatched", nil, nil),
				testProcCand(2, "ha-ops", "case a 1", nil, nil),
				testProcCand(3, "ha-ops", "case a 2", nil, nil),
				testProcCand(4, "", "case b miss", nil, testEmbedding2D(0.0, 1.0)),
				testProcCand(5, "self-improvement", "case c 1", nil, testEmbeddingAt(4, 1.0)),
				testProcCand(6, "", "case c 2", nil, testEmbeddingAt(4, 1.0)),
			},
			wantBatches:       3,
			wantActions:       []string{"EDIT", "EDIT", "CREATE"},
			wantDomains:       []string{"discord", "common", "discord"},
			wantCandCount:     []int{2, 1, 2},
			wantRequireReview: []bool{false, false, true},
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

func runPlanSkillConsolidationCase(t *testing.T, tc planSkillConsolidationCase, diskSkills []DiskSkill) {
	t.Helper()
	batches, skipped := PlanSkillConsolidation(tc.pending, diskSkills)
	assertPlanSkillBatches(t, batches, tc)
	if len(skipped) != 0 {
		t.Errorf("skipped = %v, want none", skipped)
	}
}

func TestPlanSkillConsolidation(t *testing.T) {
	skillA := testDiskSkill("ha-ops", "discord", "custom-skills/discord/ha-ops/SKILL.md", "content", testEmbedding2D(1.0, 0.0))
	skillB := testDiskSkill("deploy-flow", "common", "custom-skills/common/deploy-flow/SKILL.md", "content", testEmbedding2D(0.0, 1.0))
	diskSkills := []DiskSkill{skillA, skillB}

	tests := planSkillConsolidationCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPlanSkillConsolidationCase(t, tc, diskSkills)
		})
	}
}
