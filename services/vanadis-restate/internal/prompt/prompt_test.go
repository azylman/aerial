package prompt

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestSymbolResolution(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "aerial")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}

	code := `package test

func DoAwesomeThing() {
	println("awesome")
}

type AwesomeStruct struct {
	Field int
}
`
	if err := os.WriteFile(filepath.Join(repoDir, "awesome.go"), []byte(code), 0o644); err != nil {
		t.Fatal(err)
	}

	spans, ok := ResolveSymbolSpans(tmpDir, "aerial", []string{"awesome.go"}, []string{"DoAwesomeThing", "AwesomeStruct"})
	if !ok || len(spans) == 0 {
		t.Fatalf("ResolveSymbolSpans failed, spans = %v, ok = %v", spans, ok)
	}
	if !strings.Contains(spans[0], "DoAwesomeThing") || !strings.Contains(spans[0], "AwesomeStruct") {
		t.Errorf("spans missing symbols: %v", spans)
	}
}

func TestFormatChunkPromptWithShareRoot(t *testing.T) {
	t.Parallel()
	b := model.VanadisBuild{
		ID:            "b-1",
		Title:         "Build Title",
		Objective:     "Build Objective",
		SharedContext: "Shared Context",
		Tier:          2,
	}
	chunks := []model.VanadisChunk{
		{
			ID:           "c-1",
			Title:        "Chunk 1",
			Mode:         "pr",
			Repo:         "aerial",
			Instructions: "Do chunk 1",
			TargetFiles:  []string{"services/vanadis-restate/main.go"},
		},
		{
			ID:           "c-2",
			Title:        "Action Chunk 2",
			Mode:         "action",
			Instructions: "Verify deployment",
		},
	}

	tests := []struct {
		name      string
		chunks    []model.VanadisChunk
		activeIdx int
		wantErr   bool
		wantPart  string
	}{
		{name: "empty chunks error", chunks: nil, activeIdx: 0, wantErr: true},
		{name: "negative index error", chunks: chunks, activeIdx: -1, wantErr: true},
		{name: "out of range index error", chunks: chunks, activeIdx: 5, wantErr: true},
		{name: "first chunk prompt", chunks: chunks, activeIdx: 0, wantPart: "=== Active Chunk (Chunk 1 of 2) ==="},
		{name: "action chunk prompt", chunks: chunks, activeIdx: 1, wantPart: "=== Active Chunk (Chunk 2 of 2) ==="},
		{name: "action chunk states verdict contract", chunks: chunks, activeIdx: 1, wantPart: `This chunk completes ONLY when you call complete_chunk(chunk_id="c-2", dispatch_id="vanadis-c-2-turn-0", summary="<PASS evidence>")`},
		{name: "action chunk states resume cap", chunks: chunks, activeIdx: 1, wantPart: "Vanadis resumes it automatically (up to 12 times) and then fails it as RESUMABLE."},
		{name: "action chunk states turn budget", chunks: chunks, activeIdx: 1, wantPart: "Turns stop at about 1h. Wait at most 45 min per turn"},
		{name: "action chunk states repair pr step", chunks: chunks, activeIdx: 1, wantPart: "is a step, not the verdict: after it deploys, re-verify live and then call complete_chunk."},
		{name: "action chunk states resumable failure", chunks: chunks, activeIdx: 1, wantPart: `call fail_chunk(chunk_id="c-2", dispatch_id="vanadis-c-2-turn-0", reason="RESUMABLE: <blocker>")`},
		{name: "action chunk contract finishes with verdict", chunks: chunks, activeIdx: 1, wantPart: "=== Mandatory Execution Contract ===\n1. Execute chunk instructions directly in this turn.\n2. Finish only with complete_chunk(chunk_id=\"c-2\", dispatch_id=\"vanadis-c-2-turn-0\", summary=\"<PASS evidence>\").\n"},
		{name: "pr chunk contract submits pr", chunks: chunks, activeIdx: 0, wantPart: "2. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk c-1`.\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := FormatChunkPromptWithShareRoot(b, tc.chunks, tc.activeIdx, "/tmp")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(got, tc.wantPart) {
				t.Errorf("prompt missing %q:\n%s", tc.wantPart, got)
			}
		})
	}
}

func TestIsBoundedIdent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		line  string
		start int
		end   int
		want  bool
	}{
		{name: "middle bounded", line: " foo ", start: 1, end: 4, want: true},
		{name: "prefixed", line: "afoo ", start: 1, end: 5, want: false},
		{name: "suffixed", line: " fooa", start: 1, end: 4, want: false},
		{name: "start of line", line: "foo ", start: 0, end: 3, want: true},
		{name: "end of line", line: " foo", start: 1, end: 4, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isBoundedIdent(tc.line, tc.start, tc.end)
			if got != tc.want {
				t.Errorf("isBoundedIdent(%q, %d, %d) = %v, want %v", tc.line, tc.start, tc.end, got, tc.want)
			}
		})
	}
}

func TestContainsIdent(t *testing.T) {
	t.Parallel()
	if containsIdent("any", "") {
		t.Error("expected false for empty ident")
	}
	if !containsIdent("xfoo foo", "foo") {
		t.Error("expected true when second occurrence matches")
	}
}

func TestRepoCheckoutDir_Cases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		repo     string
		wantPath string
		wantOK   bool
	}{
		{repo: "", wantOK: false},
		{repo: ".", wantOK: false},
		{repo: "..", wantOK: false},
		{repo: "aerial", wantPath: "/share/aerial", wantOK: true},
		{repo: "azylman/aerial", wantPath: "/share/aerial", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.repo, func(t *testing.T) {
			t.Parallel()
			path, ok := repoCheckoutDir("/share", tc.repo)
			if ok != tc.wantOK {
				t.Fatalf("repoCheckoutDir(%q) ok = %v, want %v", tc.repo, ok, tc.wantOK)
			}
			if ok && path != tc.wantPath {
				t.Errorf("repoCheckoutDir(%q) path = %q, want %q", tc.repo, path, tc.wantPath)
			}
		})
	}
}

func TestStripLineComment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		line string
		want string
	}{
		{line: "func Foo() { // comment", want: "func Foo() { "},
		{line: "func Foo() {}", want: "func Foo() {}"},
	}
	for _, tc := range tests {
		t.Run(tc.line, func(t *testing.T) {
			t.Parallel()
			got := stripLineComment(tc.line)
			if got != tc.want {
				t.Errorf("stripLineComment(%q) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}

func TestFindSymbolSpan_Cases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		content   string
		symbol    string
		wantStart int
		wantEnd   int
		wantFound bool
	}{
		{name: "empty symbol", content: "func Foo() {}", symbol: "", wantFound: false},
		{name: "empty content", content: "", symbol: "Foo", wantFound: false},
		{name: "not found", content: "func Foo() {}", symbol: "Bar", wantFound: false},
		{name: "decl prefix with closing brace", content: "func Foo() {\n\treturn\n}", symbol: "Foo", wantStart: 1, wantEnd: 3, wantFound: true},
		{name: "decl prefix without closing brace", content: "type Foo struct {\n\tX int", symbol: "Foo", wantStart: 1, wantEnd: 1, wantFound: true},
		{name: "fallback non-decl line", content: "\n\tFoo := 123", symbol: "Foo", wantStart: 2, wantEnd: 2, wantFound: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start, end, found := FindSymbolSpan(tc.content, tc.symbol)
			if found != tc.wantFound || start != tc.wantStart || end != tc.wantEnd {
				t.Errorf("FindSymbolSpan(%q, %q) = (%d, %d, %v), want (%d, %d, %v)", tc.content, tc.symbol, start, end, found, tc.wantStart, tc.wantEnd, tc.wantFound)
			}
		})
	}
}

func TestResolveSymbolSpans(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "aerial")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(repoDir, "test.go")
	if err := os.WriteFile(testFile, []byte("package main\n\nfunc MyTargetFunc() {\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		shareRoot     string
		repo          string
		files         []string
		symbols       []string
		wantResolved  bool
		wantSpanCount int
	}{
		{
			name:         "invalid repo path",
			shareRoot:    tmpDir,
			repo:         ".",
			files:        []string{"test.go"},
			symbols:      []string{"MyTargetFunc"},
			wantResolved: false,
		},
		{
			name:         "nonexistent file",
			shareRoot:    tmpDir,
			repo:         "aerial",
			files:        []string{"nonexistent.go"},
			symbols:      []string{"MyTargetFunc"},
			wantResolved: false,
		},
		{
			name:         "file exists symbol missing",
			shareRoot:    tmpDir,
			repo:         "aerial",
			files:        []string{"test.go"},
			symbols:      []string{"MissingSymbol"},
			wantResolved: false,
		},
		{
			name:          "success resolving symbol",
			shareRoot:     tmpDir,
			repo:          "aerial",
			files:         []string{"test.go"},
			symbols:       []string{"MyTargetFunc"},
			wantResolved:  true,
			wantSpanCount: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spans, ok := ResolveSymbolSpans(tc.shareRoot, tc.repo, tc.files, tc.symbols)
			if ok != tc.wantResolved {
				t.Fatalf("ResolveSymbolSpans ok = %v, want %v", ok, tc.wantResolved)
			}
			if len(spans) != tc.wantSpanCount {
				t.Errorf("spans count = %d, want %d", len(spans), tc.wantSpanCount)
			}
		})
	}
}

func TestNormalizeSymbolName(t *testing.T) {
	t.Parallel()
	if sym := normalizeSymbolName("pkg.FooBar()"); sym != "FooBar" {
		t.Errorf("expected FooBar, got %q", sym)
	}
	if sym := normalizeSymbolName("  MyFunc  "); sym != "MyFunc" {
		t.Errorf("expected MyFunc, got %q", sym)
	}
}

func TestFailurePromptDirectives(t *testing.T) {
	t.Parallel()
	ev := pubsub.PRLifecycleEvent{
		Repo:      "azylman/aerial",
		PRNumber:  123,
		CheckName: "test-unit",
		Branch:    "feat/test",
		HeadSHA:   "abcdef1",
	}
	got := FormatCIFailurePrompt(ev, "extracted failure details")
	if !strings.Contains(got, "PR #123") || !strings.Contains(got, "extracted failure details") {
		t.Errorf("FormatCIFailurePrompt missing details: %s", got)
	}
	for _, want := range []string{"[VANADIS CI REMEDIATION — MANDATORY]", "must end in exactly one machine-visible outcome: (1) a fix pushed", "(2) a final line in the form REMEDIATION BLOCKED", "Ending this turn with neither a commit nor that blocked final line is a failed remediation", "counts as no outcome", "smallest boundary-crossing fix", "REMEDIATION BLOCKED: <reason and the decision needed>"} {
		if !strings.Contains(got, want) {
			t.Errorf("FormatCIFailurePrompt missing %q: %s", want, got)
		}
	}

	conflictGot := FormatConflictPrompt(ev)
	if !strings.Contains(conflictGot, "Merge conflict") || !strings.Contains(conflictGot, "PR #123") {
		t.Errorf("FormatConflictPrompt missing details: %s", conflictGot)
	}

	roborevEv := pubsub.PRLifecycleEvent{
		Repo:      "aerial",
		PRNumber:  101,
		CheckName: "roborev",
		HeadSHA:   "abcdef1",
		Branch:    "feat/test",
		CheckURL:  "https://github.com/azylman/aerial/actions/runs/123/job/456789",
	}
	roborevGot := FormatCIFailurePrompt(roborevEv, "details")
	if !strings.Contains(roborevGot, "critically evaluate each RoboRev finding") {
		t.Errorf("expected RoboRev directive, got %q", roborevGot)
	}
	for _, want := range []string{"[VANADIS CI REMEDIATION — MANDATORY]", "must end in exactly one machine-visible outcome: (1) a verified fix pushed", "(2) a technical rebuttal recorded via", "(3) a final line in the form REMEDIATION BLOCKED", "Ending this turn with no commit, no override, and no blocked final line is a failed remediation", "REMEDIATION BLOCKED: <reason and the decision needed>"} {
		if !strings.Contains(roborevGot, want) {
			t.Errorf("RoboRev prompt missing %q: %s", want, roborevGot)
		}
	}
	roborevConflict := FormatConflictPrompt(roborevEv)
	if !strings.Contains(roborevConflict, "Merge conflict detected on PR #101") {
		t.Errorf("expected conflict prompt, got %q", roborevConflict)
	}
}

func TestFormatDeployFailureBanner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		event      pubsub.PRLifecycleEvent
		wantBanner string
	}{
		{
			name:       "sync failed without details",
			event:      pubsub.PRLifecycleEvent{Event: "sync_failed"},
			wantBanner: "Git sync failed",
		},
		{
			name:       "deploy failed with job name",
			event:      pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri"},
			wantBanner: "Deployment failed for eri",
		},
		{
			name:       "deploy failed with job name and details",
			event:      pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crash loop"},
			wantBanner: "Deployment failed for eri: crash loop",
		},
		{
			name:       "deploy failed without job name",
			event:      pubsub.PRLifecycleEvent{Event: "deploy_failed"},
			wantBanner: "Deployment failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatDeployFailureBanner(tc.event)
			assert.Equal(t, tc.wantBanner, got)
		})
	}
}

func TestFormatDeployFailurePrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		event        pubsub.PRLifecycleEvent
		wantContains []string
	}{
		{
			name:         "sync failed on pr",
			event:        pubsub.PRLifecycleEvent{Event: "sync_failed", Repo: "aerial", PRNumber: 101, MergeSHA: "msha1", Branch: "feat/foo", Details: "failed to sync"},
			wantContains: []string{"Git sync failed for PR #101 on aerial (commit: msha1)", "Error details:\n```\nfailed to sync\n```", "Branch: feat/foo", "Please investigate and fix the git sync failure."},
		},
		{
			name:         "deploy failed with job on pr",
			event:        pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse", Repo: "aerial", PRNumber: 202, MergeSHA: "msha2", Details: "pod failed"},
			wantContains: []string{"Deployment failed for job synapse on aerial (PR #202, commit: msha2)", "Error details:\n```\npod failed\n```", "Please investigate and fix the deployment failure."},
		},
		{
			name:         "deploy failed without job on pr",
			event:        pubsub.PRLifecycleEvent{Event: "deploy_failed", Repo: "aerial", PRNumber: 303, HeadSHA: "hsha3"},
			wantContains: []string{"Deployment failed for PR #303 on aerial (commit: hsha3)", "Please investigate and fix the deployment failure."},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatDeployFailurePrompt(tc.event)
			for _, exp := range tc.wantContains {
				assert.Contains(t, got, exp)
			}
		})
	}
}

func TestFormatNoPRResumePrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		chunk        model.VanadisChunk
		chunkPrompt  string
		wantContains []string
	}{
		{
			name:        "includes recovery steps and original prompt",
			chunk:       model.VanadisChunk{ID: "c1"},
			chunkPrompt: "ORIGINAL CHUNK PROMPT",
			wantContains: []string{
				"[VANADIS AUTOMATIC RESUME]\nYour previous turn for chunk c1 ended without opening a PR.",
				"check whether a PR already exists for this chunk's branch (branch name contains \"vanadis-c1\")",
				"--vanadis-chunk c1",
				"Call fail_chunk with a reason only if genuinely blocked.",
				"Never end the turn while a background command is still needed for the next step",
				"The original chunk instructions follow.\n\nORIGINAL CHUNK PROMPT",
			},
		},
		{
			name:         "trims chunk id",
			chunk:        model.VanadisChunk{ID: " c2 "},
			chunkPrompt:  "P",
			wantContains: []string{"for chunk c2 ended", "--vanadis-chunk c2'"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatNoPRResumePrompt(tc.chunk, tc.chunkPrompt)
			for _, want := range tc.wantContains {
				assert.Contains(t, got, want)
			}
		})
	}
}

func TestFormatActionResumePrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		chunk        model.VanadisChunk
		chunkPrompt  string
		k            int
		wantPrefix   string
		wantContains []string
	}{
		{
			name:        "first resume",
			chunk:       model.VanadisChunk{ID: "c1", StallNudges: 1},
			chunkPrompt: "ORIGINAL CHUNK PROMPT",
			k:           1,
			wantPrefix:  "[VANADIS AUTOMATIC RESUME 1/12]\nYour previous turn for action chunk c1 ended without calling complete_chunk. Dispatch ID for this turn: vanadis-c1-turn-1-resume-1 ",
			wantContains: []string{
				"Continue from your checkpoint (/tmp/vanadis-c1/checkpoint.md",
				`complete_chunk(chunk_id="c1", dispatch_id="vanadis-c1-turn-1-resume-1", summary="<PASS evidence>")`,
				"Wait at most 45 min per turn",
				"is a step, not the verdict",
				`fail_chunk(chunk_id="c1", dispatch_id="vanadis-c1-turn-1-resume-1", reason="RESUMABLE: <blocker>")`,
				"The original chunk instructions follow.\n\nORIGINAL CHUNK PROMPT",
			},
		},
		{
			name:         "trims chunk id on last resume",
			chunk:        model.VanadisChunk{ID: " c2 "},
			chunkPrompt:  "P",
			k:            12,
			wantPrefix:   "[VANADIS AUTOMATIC RESUME 12/12]\nYour previous turn for action chunk c2 ended",
			wantContains: []string{"Dispatch ID for this turn: vanadis-c2-turn-0-resume-12 ", "--vanadis-chunk c2)"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatActionResumePrompt(tc.chunk, tc.chunkPrompt, tc.k, 12)
			assert.True(t, strings.HasPrefix(got, tc.wantPrefix), got)
			for _, want := range tc.wantContains {
				assert.Contains(t, got, want)
			}
		})
	}
}
