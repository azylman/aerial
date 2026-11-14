package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type validateCompiledSkillMarkdownCase struct {
	name           string
	action         string
	domain         string
	rawMarkdown    string
	target         *DiskSkill
	existingSkills []DiskSkill
	wantName       string
	wantRelPath    string
	wantErr        bool
	errContains    string
}

func validateCompiledSkillMarkdownCases(validMarkdown string, targetSkill *DiskSkill, existingSkills []DiskSkill) []validateCompiledSkillMarkdownCase {
	return []validateCompiledSkillMarkdownCase{
		{
			name:           "ValidCreateDiscordDomain",
			action:         "CREATE",
			domain:         "discord",
			rawMarkdown:    strings.Replace(validMarkdown, "custom-workflow", "new-workflow", 1),
			existingSkills: existingSkills,
			wantName:       "new-workflow",
			wantRelPath:    "custom-skills/discord/new-workflow/SKILL.md",
		},
		{
			name:           "ValidCreateCommonDomainNormalized",
			action:         "CREATE",
			domain:         "COMMON",
			rawMarkdown:    strings.Replace(validMarkdown, "custom-workflow", "new-common", 1),
			existingSkills: existingSkills,
			wantName:       "new-common",
			wantRelPath:    "custom-skills/common/new-common/SKILL.md",
		},
		{
			name:           "ValidCreateFallbackDomainNormalized",
			action:         "create",
			domain:         "unknown-domain",
			rawMarkdown:    strings.Replace(validMarkdown, "custom-workflow", "fallback-flow", 1),
			existingSkills: existingSkills,
			wantName:       "fallback-flow",
			wantRelPath:    "custom-skills/discord/fallback-flow/SKILL.md",
		},
		{
			name:           "ValidEditRetainsTargetRelPath",
			action:         "EDIT",
			domain:         "discord",
			rawMarkdown:    validMarkdown + "\nAdditional instructions.",
			target:         targetSkill,
			existingSkills: existingSkills,
			wantName:       "custom-workflow",
			wantRelPath:    "custom-skills/discord/custom-workflow/SKILL.md",
		},
		{
			name:        "ErrInvalidFrontmatter",
			action:      "CREATE",
			rawMarkdown: "not valid frontmatter",
			wantErr:     true,
			errContains: "invalid skill markdown frontmatter",
		},
		{
			name:   "ErrInvalidSkillName",
			action: "CREATE",
			rawMarkdown: `---
name: Invalid_Name!
description: Description that is at least twenty characters long.
---
# Title
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "invalid skill name",
		},
		{
			name:   "ErrDescriptionTooShort",
			action: "CREATE",
			rawMarkdown: `---
name: valid-name
description: short
---
# Title
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "out of bounds [20, 500]",
		},
		{
			name:   "ErrMarkdownTooShort",
			action: "CREATE",
			rawMarkdown: `---
name: valid-name
description: This description is long enough to pass validation.
---
short`,
			wantErr:     true,
			errContains: "out of bounds [100, 16384]",
		},
		{
			name:   "ErrBlockedCoreSkillName",
			action: "CREATE",
			rawMarkdown: `---
name: self-improvement
description: This description is long enough to pass validation.
---
# Blocked Skill Body
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "is a blocked core skill",
		},
		{
			name:        "ErrEditNilTarget",
			action:      "EDIT",
			rawMarkdown: validMarkdown,
			target:      nil,
			wantErr:     true,
			errContains: "target skill required for EDIT",
		},
		{
			name:   "ErrEditNameMismatch",
			action: "EDIT",
			target: targetSkill,
			rawMarkdown: `---
name: different-name
description: This description is long enough to pass validation.
---
# Different Name
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "does not match target",
		},
		{
			name:   "ErrEditShrunkMoreThan30Percent",
			action: "EDIT",
			target: &DiskSkill{
				Name:        "custom-workflow",
				RelPath:     "custom-skills/discord/custom-workflow/SKILL.md",
				RawContent:  strings.Repeat("a", 500),
				Description: "long content target",
			},
			rawMarkdown: `---
name: custom-workflow
description: This description is long enough to pass validation.
---
# Small Body
Body text that is at least 100 bytes long so length check passes.
`,
			wantErr:     true,
			errContains: "shrunk by >30%",
		},
		{
			name:           "ErrCreateNameCollision",
			action:         "CREATE",
			rawMarkdown:    validMarkdown,
			existingSkills: existingSkills, // contains custom-workflow
			wantErr:        true,
			errContains:    "collides with existing skill",
		},
		{
			name:        "ErrUnsupportedAction",
			action:      "DELETE",
			rawMarkdown: validMarkdown,
			wantErr:     true,
			errContains: "unsupported action",
		},
	}
}

func runValidateCompiledSkillMarkdownCase(t *testing.T, tc validateCompiledSkillMarkdownCase) {
	t.Helper()
	name, relPath, _, err := ValidateCompiledSkillMarkdown(tc.action, tc.domain, tc.rawMarkdown, tc.target, tc.existingSkills)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ValidateCompiledSkillMarkdown() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
			t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
		}
		return
	}
	if name != tc.wantName {
		t.Errorf("name = %q, want %q", name, tc.wantName)
	}
	if relPath != tc.wantRelPath {
		t.Errorf("relPath = %q, want %q", relPath, tc.wantRelPath)
	}
}

func TestValidateCompiledSkillMarkdown(t *testing.T) {
	validMarkdown := `---
name: custom-workflow
description: This is a robust description with at least twenty characters.
---
# Custom Workflow Title
Detailed procedure description goes here with step-by-step instructions.
`
	targetSkill := &DiskSkill{
		Name:        "custom-workflow",
		Domain:      "discord",
		RelPath:     "custom-skills/discord/custom-workflow/SKILL.md",
		RawContent:  validMarkdown,
		Description: "This is a robust description with at least twenty characters.",
	}
	existingSkills := []DiskSkill{*targetSkill}

	for _, tc := range validateCompiledSkillMarkdownCases(validMarkdown, targetSkill, existingSkills) {
		t.Run(tc.name, func(t *testing.T) {
			runValidateCompiledSkillMarkdownCase(t, tc)
		})
	}
}

func TestBuildSkillDesignerPrompt(t *testing.T) {
	target := &DiskSkill{
		Name:        "target-skill",
		Domain:      "discord",
		RelPath:     "custom-skills/discord/target-skill/SKILL.md",
		Description: "Target description",
		RawContent:  "# Target Content",
	}

	tests := []struct {
		name         string
		batch        SkillConsolidationBatch
		wantContains []string
	}{
		{
			name: "EditPromptWithTargetAndCandidates",
			batch: SkillConsolidationBatch{
				Action:      "EDIT",
				Domain:      "discord",
				TargetSkill: target,
				Candidates: []ProceduralCandidate{
					{
						ID:      101,
						Trigger: "run diagnostic command",
						Steps:   []string{"check logs", "verify service"},
						Avoid:   []string{"do not restart without checking logs"},
					},
				},
			},
			wantContains: []string{
				"Action: EDIT",
				"Target Skill Name: target-skill",
				"Existing SKILL.md Content:",
				"# Target Content",
				"Candidate #1 (ID: 101):",
				"Trigger: run diagnostic command",
				"- check logs",
				"- do not restart without checking logs",
			},
		},
		{
			name: "CreatePromptWithDomainAndCandidates",
			batch: SkillConsolidationBatch{
				Action: "CREATE",
				Domain: "common",
				Candidates: []ProceduralCandidate{
					{
						ID:      202,
						Trigger: "backup sqlite database",
						Steps:   []string{"run sqlite3 .backup"},
					},
				},
			},
			wantContains: []string{
				"Action: CREATE",
				"Target Domain: common",
				"Candidate #1 (ID: 202):",
				"Trigger: backup sqlite database",
				"- run sqlite3 .backup",
				"30+ days",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prompt := buildSkillDesignerPrompt(tc.batch)
			for _, want := range tc.wantContains {
				if !strings.Contains(prompt, want) {
					t.Errorf("prompt missing %q:\n%s", want, prompt)
				}
			}
		})
	}
}

func TestFindCandidateCliques(t *testing.T) {
	tests := []struct {
		name        string
		cands       []ProceduralCandidate
		minSim      float64
		minSize     int
		maxSize     int
		wantCliques int
		wantSizes   []int
	}{
		{
			name:        "EmptyCandidates",
			cands:       nil,
			wantCliques: 0,
		},
		{
			name: "FallbackDefaults",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(2, "", "t2", nil, testEmbeddingAt(0, 1.0)),
			},
			minSim:      -1.0,
			minSize:     -1,
			maxSize:     -1,
			wantCliques: 1,
			wantSizes:   []int{2},
		},
		{
			name: "CandidateWithoutEmbeddingSkipped",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, nil),
				testProcCand(2, "", "t2", nil, []float32{}),
			},
			wantCliques: 0,
		},
		{
			name: "CliqueOfSizeThree",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(2, "", "t2", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(3, "", "t3", nil, testEmbeddingAt(0, 1.0)),
			},
			minSim:      0.75,
			minSize:     2,
			maxSize:     5,
			wantCliques: 1,
			wantSizes:   []int{3},
		},
		{
			name: "CandidateMatchesOnlyFirstTwoNotThird",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbedding2D(0.8, 0.6)),
				testProcCand(2, "", "t2", nil, testEmbedding2D(0.8, 0.6)),
				testProcCand(3, "", "t3", nil, testEmbedding2D(1.0, 0.0)),
				testProcCand(4, "", "t4", nil, testEmbedding2D(0.6, 0.8)),
			},
			minSim:      0.75,
			minSize:     2,
			maxSize:     5,
			wantCliques: 1,
			wantSizes:   []int{3},
		},
		{
			name: "MaxSizeBreaksEarly",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(2, "", "t2", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(3, "", "t3", nil, testEmbeddingAt(0, 1.0)),
			},
			minSim:      0.75,
			minSize:     2,
			maxSize:     2,
			wantCliques: 1,
			wantSizes:   []int{2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cliques := findCandidateCliques(tc.cands, tc.minSim, tc.minSize, tc.maxSize)
			if len(cliques) != tc.wantCliques {
				t.Fatalf("cliques len = %d, want %d", len(cliques), tc.wantCliques)
			}
			for i, wantSize := range tc.wantSizes {
				if len(cliques[i]) != wantSize {
					t.Errorf("cliques[%d] len = %d, want %d", i, len(cliques[i]), wantSize)
				}
			}
		})
	}
}

type mockGitHubServerConfig struct {
	openPRRefs       []string
	failListPRs      bool
	failGetMainRef   bool
	emptyMainRefSHA  bool
	failGetCommit    bool
	emptyBaseTreeSHA bool
	failCreateTree   bool
	emptyTreeSHA     bool
	failCreateCommit bool
	emptyCommitSHA   bool
	failCreateRef    bool
	failCreatePR     bool
	failDeleteRef    bool
	failAutoMerge    bool
	failBrainReg     bool
	autoMergeCalls   *int
	brainRegCalls    *int
	discordMsgCalls  *int
}

func handleMockGitGet(w http.ResponseWriter, r *http.Request, cfg mockGitHubServerConfig) bool {
	if r.Method != http.MethodGet {
		return false
	}
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/pulls"):
		if cfg.failListPRs {
			http.Error(w, `{"message":"failed to list PRs"}`, http.StatusInternalServerError)
			return true
		}
		var prs []map[string]any
		for i, ref := range cfg.openPRRefs {
			prs = append(prs, map[string]any{
				"number": i + 1,
				"head": map[string]string{
					"ref": ref,
					"sha": "head-sha-123",
				},
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(prs)
		return true

	case strings.HasSuffix(path, "/git/ref/heads/main"):
		if cfg.failGetMainRef {
			http.Error(w, `{"message":"not found"}`, http.StatusInternalServerError)
			return true
		}
		sha := "main-sha-123"
		if cfg.emptyMainRefSHA {
			sha = ""
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": map[string]string{"sha": sha, "type": "commit"},
		})
		return true

	case strings.Contains(path, "/git/commits/"):
		if cfg.failGetCommit {
			http.Error(w, `{"message":"commit error"}`, http.StatusInternalServerError)
			return true
		}
		treeSHA := "base-tree-456"
		if cfg.emptyBaseTreeSHA {
			treeSHA = ""
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":  "main-sha-123",
			"tree": map[string]string{"sha": treeSHA},
		})
		return true
	}
	return false
}

func handleMockGitPost(w http.ResponseWriter, r *http.Request, cfg mockGitHubServerConfig) bool {
	path := r.URL.Path
	if r.Method == http.MethodDelete && strings.Contains(path, "/git/refs/heads/") {
		if cfg.failDeleteRef {
			http.Error(w, `{"message":"del error"}`, http.StatusInternalServerError)
			return true
		}
		w.WriteHeader(http.StatusNoContent)
		return true
	}

	if r.Method != http.MethodPost {
		return false
	}

	switch {
	case strings.HasSuffix(path, "/git/trees"):
		if cfg.failCreateTree {
			http.Error(w, `{"message":"tree error"}`, http.StatusInternalServerError)
			return true
		}
		treeSHA := "new-tree-789"
		if cfg.emptyTreeSHA {
			treeSHA = ""
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"sha": treeSHA})
		return true

	case strings.HasSuffix(path, "/git/commits"):
		if cfg.failCreateCommit {
			http.Error(w, `{"message":"commit error"}`, http.StatusInternalServerError)
			return true
		}
		commitSHA := "new-commit-abc"
		if cfg.emptyCommitSHA {
			commitSHA = ""
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"sha": commitSHA})
		return true

	case strings.HasSuffix(path, "/git/refs"):
		if cfg.failCreateRef {
			http.Error(w, `{"message":"ref error"}`, http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ref": "refs/heads/branch"})
		return true

	case strings.HasSuffix(path, "/pulls"):
		if cfg.failCreatePR {
			http.Error(w, `{"message":"pr error"}`, http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number":  42,
			"node_id": "PR_kwDO123",
		})
		return true
	}
	return false
}

func handleMockInternalPost(w http.ResponseWriter, r *http.Request, cfg mockGitHubServerConfig) bool {
	if r.Method != http.MethodPost {
		return false
	}
	switch r.URL.Path {
	case "/graphql":
		if cfg.autoMergeCalls != nil {
			*cfg.autoMergeCalls++
		}
		if cfg.failAutoMerge {
			http.Error(w, `{"errors":[{"message":"automerge error"}]}`, http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"enablePullRequestAutoMerge": map[string]any{},
			},
		})
		return true

	case "/internal/pr/register":
		if cfg.brainRegCalls != nil {
			*cfg.brainRegCalls++
		}
		if cfg.failBrainReg {
			http.Error(w, `{"error":"registration failed"}`, http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
		return true

	case "/discord/message":
		if cfg.discordMsgCalls != nil {
			*cfg.discordMsgCalls++
		}
		if cfg.failBrainReg {
			http.Error(w, `{"error":"discord message failed"}`, http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "sent"})
		return true
	}
	return false
}

func newMockGitHubServer(t *testing.T, cfg mockGitHubServerConfig) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleMockGitGet(w, r, cfg) {
			return
		}
		if handleMockGitPost(w, r, cfg) {
			return
		}
		if handleMockInternalPost(w, r, cfg) {
			return
		}
		http.NotFound(w, r)
	}))
}

type publishSkillChangesCase struct {
	name               string
	token              string
	repo               string
	channelID          string
	changes            []SkillFileChange
	serverCfg          mockGitHubServerConfig
	wantPR             int
	wantErr            bool
	errSubstr          string
	checkCalls         bool
	wantAutoMergeCalls int
	wantBrainRegCalls  int
	wantDiscordCalls   int
}

func publishSkillChangesCases() []publishSkillChangesCase {
	return []publishSkillChangesCase{
		{
			name:      "MissingToken",
			token:     "",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "missing GITHUB_PAT",
		},
		{
			name:      "ForbiddenAerialRepo",
			token:     "pat-123",
			repo:      "aerial",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "publishing directly to aerial repository is forbidden",
		},
		{
			name:    "EmptyChangesReturnsZero",
			token:   "pat-123",
			repo:    "aerial-config",
			changes: nil,
			wantPR:  0,
			wantErr: false,
		},
		{
			name:      "InvalidRelPathRoot",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "invalid skill relative path",
		},
		{
			name:      "InvalidRelPathBadDomain",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/voice/ha/SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "invalid skill relative path",
		},
		{
			name:      "OpenCovenPRAlreadyExists",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{openPRRefs: []string{"update/coven-skills-999"}},
			wantPR:    0,
			wantErr:   false,
		},
		{
			name:      "ListPullsError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failListPRs: true},
			wantErr:   true,
			errSubstr: "failed to list open pull requests",
		},
		{
			name:      "GetMainRefError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failGetMainRef: true},
			wantErr:   true,
			errSubstr: "failed to get heads/main",
		},
		{
			name:      "EmptyMainRefSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyMainRefSHA: true},
			wantErr:   true,
			errSubstr: "empty SHA for heads/main",
		},
		{
			name:      "GetCommitError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failGetCommit: true},
			wantErr:   true,
			errSubstr: "failed to get commit",
		},
		{
			name:      "EmptyBaseTreeSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyBaseTreeSHA: true},
			wantErr:   true,
			errSubstr: "empty tree SHA",
		},
		{
			name:      "CreateTreeError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreateTree: true},
			wantErr:   true,
			errSubstr: "failed to create tree",
		},
		{
			name:      "EmptyCreatedTreeSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyTreeSHA: true},
			wantErr:   true,
			errSubstr: "empty SHA for created tree",
		},
		{
			name:      "CreateCommitError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreateCommit: true},
			wantErr:   true,
			errSubstr: "failed to create commit",
		},
		{
			name:      "EmptyCreatedCommitSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyCommitSHA: true},
			wantErr:   true,
			errSubstr: "empty SHA for created commit",
		},
		{
			name:      "CreateRefError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreateRef: true},
			wantErr:   true,
			errSubstr: "failed to create ref",
		},
		{
			name:      "CreatePRErrorCleansBranch",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreatePR: true},
			wantErr:   true,
			errSubstr: "failed to create pull request",
		},
		{
			name:      "CreatePRErrorCleansBranchDeleteErrorHandled",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreatePR: true, failDeleteRef: true},
			wantErr:   true,
			errSubstr: "failed to create pull request",
		},
		{
			name:      "SuccessSingleChangeWithAutoMergeAndBrainRegister",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "1562837549255495693",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo", CommitSummary: "update ha", Action: "EDIT"}},
			wantPR:    42,
			wantErr:   false,
		},
		{
			name:      "SuccessMultipleChangesWithoutChannelID",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "",
			changes: []SkillFileChange{
				{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo", CommitSummary: "update ha", Action: "EDIT"},
				{RelPath: "custom-skills/common/deploy/SKILL.md", Content: "bar", CommitSummary: "add deploy", Action: "CREATE"},
			},
			wantPR:  42,
			wantErr: false,
		},
		{
			name:      "SuccessAutoMergeAndBrainRegisterFailuresHandledBestEffort",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "1562837549255495693",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo", CommitSummary: "update ha", Action: "EDIT"}},
			serverCfg: mockGitHubServerConfig{failAutoMerge: true, failBrainReg: true},
			wantPR:    42,
			wantErr:   false,
		},
		{
			name:      "SuccessRequireReviewSkipsAutoMergeAndNotifiesSystemChannel",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "1562837549255495693",
			changes: []SkillFileChange{{
				RelPath:       "custom-skills/discord/ha/SKILL.md",
				Content:       "foo",
				CommitSummary: "update ha",
				Action:        "EDIT",
				RequireReview: true,
				CandidateIDs:  []int64{101},
				Candidates: []ProceduralCandidate{{
					ID:          101,
					LoadedSkill: "self-improvement",
					Trigger:     "when running recursive grep",
					Steps:       []string{"pass --exclude-dir=.git"},
					Avoid:       []string{"do not run bare grep -r"},
				}},
			}},
			wantPR:             42,
			wantErr:            false,
			checkCalls:         true,
			wantAutoMergeCalls: 0,
			wantBrainRegCalls:  0,
			wantDiscordCalls:   1,
		},
		{
			name:      "SuccessRequireReviewNotifyFailureHandledBestEffort",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "1562837549255495693",
			changes: []SkillFileChange{{
				RelPath:       "custom-skills/discord/ha/SKILL.md",
				Content:       "foo",
				CommitSummary: "update ha",
				Action:        "EDIT",
				RequireReview: true,
			}},
			serverCfg:          mockGitHubServerConfig{failBrainReg: true},
			wantPR:             42,
			wantErr:            false,
			checkCalls:         true,
			wantAutoMergeCalls: 0,
			wantBrainRegCalls:  0,
			wantDiscordCalls:   1,
		},
	}
}

func assertPublishSkillChangesCalls(t *testing.T, autoMerge, brainReg, discordMsg int, tc publishSkillChangesCase) {
	t.Helper()
	if autoMerge != tc.wantAutoMergeCalls {
		t.Errorf("autoMergeCalls = %d, want %d", autoMerge, tc.wantAutoMergeCalls)
	}
	if brainReg != tc.wantBrainRegCalls {
		t.Errorf("brainRegCalls = %d, want %d", brainReg, tc.wantBrainRegCalls)
	}
	if discordMsg != tc.wantDiscordCalls {
		t.Errorf("discordMsgCalls = %d, want %d", discordMsg, tc.wantDiscordCalls)
	}
}

func runPublishSkillChangesCase(ctx context.Context, t *testing.T, tc publishSkillChangesCase) {
	t.Helper()
	var autoMergeCalls, brainRegCalls, discordMsgCalls int
	cfg := tc.serverCfg
	cfg.autoMergeCalls = &autoMergeCalls
	cfg.brainRegCalls = &brainRegCalls
	cfg.discordMsgCalls = &discordMsgCalls

	server := newMockGitHubServer(t, cfg)
	defer server.Close()

	fixedTime := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	publisher := NewGitHubSkillPublisher(GitHubSkillPublisherConfig{
		Token:              tc.token,
		Owner:              "azylman",
		Repo:               tc.repo,
		BrainURL:           server.URL,
		BrainPRRegisterURL: server.URL + "/internal/pr/register",
		SystemChannelID:    tc.channelID,
		BaseURL:            server.URL,
		HTTPClient:         server.Client(),
		Now:                func() time.Time { return fixedTime },
	})

	prNum, err := publisher.PublishSkillChanges(ctx, tc.changes)
	if (err != nil) != tc.wantErr {
		t.Fatalf("PublishSkillChanges() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
			t.Errorf("err = %q, want substring %q", err.Error(), tc.errSubstr)
		}
		return
	}
	if prNum != tc.wantPR {
		t.Errorf("prNum = %d, want %d", prNum, tc.wantPR)
	}
	if tc.checkCalls {
		assertPublishSkillChangesCalls(t, autoMergeCalls, brainRegCalls, discordMsgCalls, tc)
	}
}

func TestGitHubSkillPublisher_PublishSkillChanges(t *testing.T) {
	ctx := context.Background()

	for _, tc := range publishSkillChangesCases() {
		t.Run(tc.name, func(t *testing.T) {
			runPublishSkillChangesCase(ctx, t, tc)
		})
	}
}

func TestGitHubSkillPublisher_NotifyShortFriction(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		channelID  string
		content    string
		respStatus int
		clientErr  bool
		wantErr    bool
	}{
		{name: "EmptyChannelIDNoop", channelID: "", content: "alert", wantErr: false},
		{name: "EmptyContentNoop", channelID: "ch-1", content: "   ", wantErr: false},
		{name: "Success200", channelID: "ch-1", content: "alert", respStatus: http.StatusOK, wantErr: false},
		{name: "ServerError500", channelID: "ch-1", content: "alert", respStatus: http.StatusInternalServerError, wantErr: true},
		{name: "ClientError", channelID: "ch-1", content: "alert", clientErr: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.respStatus != 0 {
					w.WriteHeader(tc.respStatus)
				}
			}))
			defer server.Close()

			targetURL := server.URL
			if tc.clientErr {
				targetURL = "http://127.0.0.1:0/bad"
			}

			p := &GitHubSkillPublisher{
				systemChannelID: tc.channelID,
				brainURL:        targetURL,
				httpClient:      server.Client(),
			}

			err := p.NotifyShortFriction(ctx, tc.content)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NotifyShortFriction() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

type mockSkillPublisher struct {
	prNum    int
	err      error
	alertErr error
	changes  []SkillFileChange
	alerts   []string
}

func (m *mockSkillPublisher) PublishSkillChanges(ctx context.Context, changes []SkillFileChange) (int, error) {
	m.changes = changes
	if m.err != nil {
		return 0, m.err
	}
	return m.prNum, nil
}

func (m *mockSkillPublisher) NotifyShortFriction(ctx context.Context, content string) error {
	m.alerts = append(m.alerts, content)
	return m.alertErr
}

type consolidateProceduralSkillsCase struct {
	name                    string
	lockExtraction          bool
	nilStore                bool
	nilPublisher            bool
	nilLLM                  bool
	storeFailOp             string
	cands                   []ProceduralCandidate
	customSkillsDir         string
	llmResp                 string
	llmErr                  error
	pubPR                   int
	pubErr                  error
	alertErr                error
	wantPR                  int
	wantErr                 bool
	wantMarkConsolidate     bool
	wantMarkSkipped         bool
	wantChangesLen          int
	wantAlertsLen           int
	wantConsolidatedIndices []int
	wantPendingIndices      []int
}

func consolidateProceduralSkillsCases(validSkillMD, validNewSkillMD string) []consolidateProceduralSkillsCase {
	return []consolidateProceduralSkillsCase{
		{
			name:           "ExtractionMutexLockedReturnsZero",
			lockExtraction: true,
			wantPR:         0,
			wantErr:        false,
		},
		{
			name:     "NilStoreFails",
			nilStore: true,
			wantErr:  true,
		},
		{
			name:         "NilPublisherFails",
			nilPublisher: true,
			wantErr:      true,
		},
		{
			name:    "NilLLMFails",
			nilLLM:  true,
			wantErr: true,
		},
		{
			name:        "GetPendingCandidatesStoreError",
			storeFailOp: "GetPendingProceduralCandidates",
			wantErr:     true,
		},
		{
			name:    "NoPendingCandidatesReturnsZero",
			cands:   nil,
			wantPR:  0,
			wantErr: false,
		},
		{
			name:            "ScanCustomSkillsDirError",
			cands:           []ProceduralCandidate{testProcCand(1, "ha-ops", "trigger", []string{"avoid"}, nil)},
			customSkillsDir: "FILE_AS_DIR",
			wantErr:         true,
		},
		{
			name: "PlanYieldsNoBatchesUnmatchedBlockedCoreSkillsRemainPending",
			cands: []ProceduralCandidate{
				testProcCand(10, "self-improvement", "trigger 1", nil, nil),
				testProcCand(11, "writing-plans", "trigger 2", nil, nil),
			},
			wantPR:             0,
			wantErr:            false,
			wantMarkSkipped:    false,
			wantPendingIndices: []int{0, 1},
		},
		{
			name: "LLMInvocationErrorBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(20, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmErr:  errors.New("llm execution error"),
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "LLMMalformedJSONBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(21, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp: "not a json object",
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "DesignerReturnsNOOPCandidateMarkedSkipped",
			cands: []ProceduralCandidate{
				testProcCand(22, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp:         `{"action":"NOOP"}`,
			wantPR:          0,
			wantErr:         false,
			wantMarkSkipped: true,
		},
		{
			name: "DesignerReturnsUnknownActionBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(23, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp: `{"action":"DELETE"}`,
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "DesignerValidationFailsBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(24, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp: `{"action":"EDIT","name":"ha-ops","markdown":"short"}`,
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "PublisherFailsReturnsError",
			cands: []ProceduralCandidate{
				testProcCand(25, "", "trigger 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(26, "", "trigger 2", nil, testEmbeddingAt(2, 1.0)),
			},
			llmResp: fmt.Sprintf(`{"action":"CREATE","name":"new-flow","domain":"discord","commit_summary":"add new flow","markdown":%q}`, validNewSkillMD),
			pubErr:  errors.New("github publish error"),
			wantErr: true,
		},
		{
			name: "SuccessCREATEBatch",
			cands: []ProceduralCandidate{
				testProcCand(30, "", "trigger 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(31, "", "trigger 2", nil, testEmbeddingAt(2, 1.0)),
			},
			llmResp:             fmt.Sprintf(`{"action":"CREATE","name":"new-flow","domain":"discord","commit_summary":"add new flow","markdown":%q}`, validNewSkillMD),
			pubPR:               88,
			wantPR:              88,
			wantErr:             false,
			wantMarkConsolidate: true,
		},
		{
			name: "SuccessEDITBatch",
			cands: []ProceduralCandidate{
				testProcCand(40, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp:             fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","domain":"discord","commit_summary":"update ha-ops","markdown":%q}`, validSkillMD),
			pubPR:               89,
			wantPR:              89,
			wantErr:             false,
			wantMarkConsolidate: true,
		},
		{
			name: "PublisherReturnsZeroPRAlreadyOpenCandidatesNotConsolidated",
			cands: []ProceduralCandidate{
				testProcCand(50, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp:             fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","domain":"discord","commit_summary":"update ha-ops","markdown":%q}`, validSkillMD),
			pubPR:               0,
			wantPR:              0,
			wantErr:             false,
			wantMarkConsolidate: false,
		},
		{
			name: "CollidingBatchesInSamePassDeferred",
			cands: []ProceduralCandidate{
				testProcCand(60, "ha-ops", "trigger 1", []string{"instruction 1"}, nil),
				testProcCand(61, "", "trigger 2", nil, testEmbeddingAt(0, 1.0)),
			},
			llmResp:                 fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","domain":"discord","commit_summary":"update ha-ops","markdown":%q}`, validSkillMD),
			pubPR:                   95,
			wantPR:                  95,
			wantErr:                 false,
			wantChangesLen:          1,
			wantConsolidatedIndices: []int{0},
			wantPendingIndices:      []int{1},
		},
		{
			name:            "ShortFrictionAlertBatchNotifiedAndMarkedSkipped",
			cands:           makeProceduralCandidateBatch(81, 5),
			wantPR:          0,
			wantErr:         false,
			wantMarkSkipped: true,
			wantAlertsLen:   1,
		},
		{
			name:               "ShortFrictionAlertErrorRetainsPending",
			cands:              makeProceduralCandidateBatch(91, 5),
			alertErr:           errors.New("alert post failed"),
			wantPR:             0,
			wantErr:            false,
			wantAlertsLen:      1,
			wantPendingIndices: []int{0, 1, 2, 3, 4},
		},
	}
}

func setupConsolidateProceduralSkillsDir(t *testing.T, validSkillMD, customSkillsDir string) string {
	t.Helper()
	tmpDir := t.TempDir()
	skillsDir := filepath.Join(tmpDir, "custom-skills")
	discordDir := filepath.Join(skillsDir, "discord", "ha-ops")
	if err := os.MkdirAll(discordDir, 0o755); err != nil {
		t.Fatalf("failed to create test custom skills dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(discordDir, "SKILL.md"), []byte(validSkillMD), 0o644); err != nil {
		t.Fatalf("failed to write test SKILL.md: %v", err)
	}

	if customSkillsDir == "FILE_AS_DIR" {
		dummyFile := filepath.Join(tmpDir, "dummy-file")
		_ = os.WriteFile(dummyFile, []byte("not-a-dir"), 0o644)
		return dummyFile
	}
	if customSkillsDir != "" {
		return customSkillsDir
	}
	return skillsDir
}

func setupConsolidateProceduralStore(ctx context.Context, failOp string, cands []ProceduralCandidate) (*TestMemStore, []int64) {
	mem := newTestMemStore()
	if failOp != "" {
		mem.failOp = failOp
		mem.failErr = errors.New("simulated store failure")
	}
	insertedIDs := make([]int64, 0, len(cands))
	for _, cand := range cands {
		id, _ := mem.InsertProceduralCandidate(ctx, cand)
		insertedIDs = append(insertedIDs, id)
	}
	return mem, insertedIDs
}

func assertConsolidateProceduralCandidateStatus(t *testing.T, tc consolidateProceduralSkillsCase, mem *TestMemStore, insertedIDs []int64) {
	t.Helper()
	if tc.wantMarkConsolidate {
		for _, id := range insertedIDs {
			if mem.proceduralCandidates[id].Status != "consolidated" {
				t.Errorf("candidate %d status = %q, want consolidated", id, mem.proceduralCandidates[id].Status)
			}
			if mem.proceduralCandidates[id].PRNumber != tc.wantPR {
				t.Errorf("candidate %d pr_number = %d, want %d", id, mem.proceduralCandidates[id].PRNumber, tc.wantPR)
			}
		}
	}
	if tc.wantMarkSkipped {
		for _, id := range insertedIDs {
			if mem.proceduralCandidates[id].Status != "skipped" {
				t.Errorf("candidate %d status = %q, want skipped", id, mem.proceduralCandidates[id].Status)
			}
		}
	}
	for _, idx := range tc.wantConsolidatedIndices {
		if idx < len(insertedIDs) {
			id := insertedIDs[idx]
			if mem.proceduralCandidates[id].Status != "consolidated" {
				t.Errorf("candidate %d status = %q, want consolidated", id, mem.proceduralCandidates[id].Status)
			}
		}
	}
	for _, idx := range tc.wantPendingIndices {
		if idx < len(insertedIDs) {
			id := insertedIDs[idx]
			if mem.proceduralCandidates[id].Status != "pending" {
				t.Errorf("candidate %d status = %q, want pending", id, mem.proceduralCandidates[id].Status)
			}
		}
	}
}

func assertConsolidateProceduralPublisherOutput(t *testing.T, tc consolidateProceduralSkillsCase, pub *mockSkillPublisher) {
	t.Helper()
	if tc.wantChangesLen > 0 && len(pub.changes) != tc.wantChangesLen {
		t.Errorf("published changes len = %d, want %d", len(pub.changes), tc.wantChangesLen)
	}
	if tc.wantAlertsLen > 0 && len(pub.alerts) != tc.wantAlertsLen {
		t.Errorf("alerts len = %d, want %d", len(pub.alerts), tc.wantAlertsLen)
	}
}

func runConsolidateProceduralSkillsCase(ctx context.Context, t *testing.T, validSkillMD string, tc consolidateProceduralSkillsCase) {
	t.Helper()
	if tc.lockExtraction {
		extractionMutex.Lock()
		defer extractionMutex.Unlock()
	}

	targetSkillsDir := setupConsolidateProceduralSkillsDir(t, validSkillMD, tc.customSkillsDir)
	mem, insertedIDs := setupConsolidateProceduralStore(ctx, tc.storeFailOp, tc.cands)

	var store Store = mem
	if tc.nilStore {
		store = nil
	}

	pub := &mockSkillPublisher{
		prNum:    tc.pubPR,
		err:      tc.pubErr,
		alertErr: tc.alertErr,
	}
	var publisher SkillPublisher = pub
	if tc.nilPublisher {
		publisher = nil
	}

	var llm LLMClientFunc = func(c context.Context, prompt string) (string, error) {
		if tc.llmErr != nil {
			return "", tc.llmErr
		}
		return tc.llmResp, nil
	}
	if tc.nilLLM {
		llm = nil
	}

	ollama := &MockOllamaClient{embedding: testEmbeddingAt(0, 1.0)}

	prNum, err := ConsolidateProceduralSkills(ctx, store, ollama, llm, publisher, targetSkillsDir)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ConsolidateProceduralSkills() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	if prNum != tc.wantPR {
		t.Errorf("prNum = %d, want %d", prNum, tc.wantPR)
	}

	assertConsolidateProceduralCandidateStatus(t, tc, mem, insertedIDs)
	assertConsolidateProceduralPublisherOutput(t, tc, pub)
}

func TestConsolidateProceduralSkills(t *testing.T) {
	ctx := context.Background()

	validSkillMD := `---
name: ha-ops
description: Home assistant operations workflow and automation routines.
---
# Home Assistant Operations
Procedure steps for configuring automations and entities properly.
`
	validNewSkillMD := `---
name: new-flow
description: Novel procedure for orchestrating automated deployments.
---
# New Flow Procedure
1. First step in the procedural execution pipeline.
2. Second step in the procedural execution pipeline.
3. Third step in the procedural execution pipeline.
`

	for _, tc := range consolidateProceduralSkillsCases(validSkillMD, validNewSkillMD) {
		t.Run(tc.name, func(t *testing.T) {
			runConsolidateProceduralSkillsCase(ctx, t, validSkillMD, tc)
		})
	}
}

func TestWorker_RunDaily_ProceduralSkills(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	skillsDir := filepath.Join(tmpDir, "custom-skills", "discord", "ha-ops")
	_ = os.MkdirAll(skillsDir, 0o755)
	validMD := `---
name: ha-ops
description: Home assistant operations workflow and automation routines.
---
# Home Assistant Operations
Procedure steps.
`
	_ = os.WriteFile(filepath.Join(skillsDir, "SKILL.md"), []byte(validMD), 0o644)

	mem := newTestMemStore()
	_, _ = mem.InsertProceduralCandidate(ctx, testProcCand(1, "ha-ops", "trigger 1", []string{"avoid"}, nil))

	pub := &mockSkillPublisher{prNum: 55}
	llm := func(c context.Context, prompt string) (string, error) {
		return fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","markdown":%q}`, validMD), nil
	}

	w := NewWorker(WorkerConfig{
		Store:           mem,
		Ollama:          &MockOllamaClient{embedding: testEmbeddingAt(0, 1.0)},
		LLMClient:       llm,
		Publisher:       pub,
		CustomSkillsDir: filepath.Join(tmpDir, "custom-skills"),
	})

	// Tick 60 is daily decay + dreaming + procedural consolidation
	w.Tick(ctx, 60)

	if len(pub.changes) != 1 {
		t.Errorf("publisher changes len = %d, want 1", len(pub.changes))
	}
}

func TestToolHandler_Consolidate_ProceduralSkills(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	skillsDir := filepath.Join(tmpDir, "custom-skills", "discord", "ha-ops")
	_ = os.MkdirAll(skillsDir, 0o755)
	validMD := `---
name: ha-ops
description: Home assistant operations workflow and automation routines.
---
# Home Assistant Operations
Procedure steps.
`
	_ = os.WriteFile(filepath.Join(skillsDir, "SKILL.md"), []byte(validMD), 0o644)

	mem := newTestMemStore()
	_, _ = mem.InsertProceduralCandidate(ctx, testProcCand(1, "ha-ops", "trigger 1", []string{"avoid"}, nil))

	pub := &mockSkillPublisher{prNum: 77}
	cfg := &Config{
		CustomSkillsDir: filepath.Join(tmpDir, "custom-skills"),
	}

	h := NewToolHandler(cfg, mem, &MockOllamaClient{embedding: testEmbeddingAt(0, 1.0)}, pub)
	h.llmClient = func(c context.Context, prompt string) (string, error) {
		return fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","markdown":%q}`, validMD), nil
	}

	count, err := h.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate() err = %v", err)
	}
	if count != 0 {
		t.Errorf("fact consolidate count = %d, want 0", count)
	}
	if len(pub.changes) != 1 {
		t.Errorf("pub.changes len = %d, want 1", len(pub.changes))
	}
}
