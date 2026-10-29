package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFindSymbolSpan(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		content   string
		symbol    string
		wantStart int
		wantEnd   int
		wantOK    bool
	}{
		{
			name:      "multi-line func",
			content:   "package p\n\nfunc Foo() {\n\tx := 1\n\t_ = x\n}\n",
			symbol:    "Foo",
			wantStart: 3,
			wantEnd:   6,
			wantOK:    true,
		},
		{
			name:      "single-line func",
			content:   "package p\nfunc Bar() { return }\n",
			symbol:    "Bar",
			wantStart: 2,
			wantEnd:   2,
			wantOK:    true,
		},
		{
			name:      "multi-line parameter list and return type",
			content:   "package p\n\nfunc Complex(\n\ta int,\n\tb string,\n) (int, error) {\n\treturn a, nil\n}\n",
			symbol:    "Complex",
			wantStart: 3,
			wantEnd:   8,
			wantOK:    true,
		},
		{
			name:      "struct type declaration",
			content:   "package p\n\ntype MyStruct struct {\n\tFieldA string\n\tFieldB int\n}\n",
			symbol:    "MyStruct",
			wantStart: 3,
			wantEnd:   6,
			wantOK:    true,
		},
		{
			name:      "interface type declaration",
			content:   "package p\n\ntype MyInterface interface {\n\tDoWork() error\n}\n",
			symbol:    "MyInterface",
			wantStart: 3,
			wantEnd:   5,
			wantOK:    true,
		},
		{
			name:      "raw backtick string with column 0 brace",
			content:   "package p\n\nfunc WithRaw() string {\n\tconst s = `\n}\n`\n\treturn s\n}\n",
			symbol:    "WithRaw",
			wantStart: 3,
			wantEnd:   8,
			wantOK:    true,
		},
		{
			name:      "receiver qualified symbol",
			content:   "package p\n\nfunc (s *Server) HandleReq() error {\n\treturn nil\n}\n",
			symbol:    "*Server.HandleReq",
			wantStart: 3,
			wantEnd:   5,
			wantOK:    true,
		},
		{
			name:      "receiver symbol with trailing parens",
			content:   "package p\n\nfunc (s *Server) HandleReq() error {\n\treturn nil\n}\n",
			symbol:    "*Server.HandleReq()",
			wantStart: 3,
			wantEnd:   5,
			wantOK:    true,
		},
		{
			name:      "single-line type alias",
			content:   "package p\n\ntype UserID string\n",
			symbol:    "UserID",
			wantStart: 3,
			wantEnd:   3,
			wantOK:    true,
		},
		{
			name:      "fallback pass call site line",
			content:   "package p\n\nfunc Worker() {\n\tHelperFunc()\n}\n",
			symbol:    "HelperFunc",
			wantStart: 4,
			wantEnd:   4,
			wantOK:    true,
		},
		{
			name:      "prefix boundary non-match",
			content:   "package p\n\nfunc MyFoo() {}\n",
			symbol:    "Foo",
			wantStart: 0,
			wantEnd:   0,
			wantOK:    false,
		},
		{
			name:      "suffix boundary non-match",
			content:   "package p\n\nfunc FooBar() {}\n",
			symbol:    "Foo",
			wantStart: 0,
			wantEnd:   0,
			wantOK:    false,
		},
		{
			name:      "url with slashes inside double quotes",
			content:   "package p\n\nconst URL = \"http://example.com/Foo\"\n",
			symbol:    "URL",
			wantStart: 3,
			wantEnd:   3,
			wantOK:    true,
		},
		{
			name:      "escaped quote inside string before comment marker",
			content:   "package p\n\nconst Msg = \"escaped \\\" quote // ignored\"\n",
			symbol:    "Msg",
			wantStart: 3,
			wantEnd:   3,
			wantOK:    true,
		},
		{
			name:      "var block closed with paren",
			content:   "package p\n\nvar (\n\tAlpha = 1\n\tBeta = 2\n)\n",
			symbol:    "var",
			wantStart: 3,
			wantEnd:   6,
			wantOK:    true,
		},
		{
			name:      "unclosed block terminated by next declaration",
			content:   "package p\n\nfunc Unclosed() {\n\tx := 1\nfunc Next() {}\n",
			symbol:    "Unclosed",
			wantStart: 3,
			wantEnd:   4,
			wantOK:    true,
		},
		{
			name:      "unclosed block reaching eof",
			content:   "package p\n\nfunc UnclosedEOF() {\n\tx := 1\n",
			symbol:    "UnclosedEOF",
			wantStart: 3,
			wantEnd:   5,
			wantOK:    true,
		},
		{
			name:      "commented line ignored",
			content:   "package p\n\n// func Commented() {\n// }\nfunc Active() {\n\treturn\n}\n",
			symbol:    "Commented",
			wantStart: 0,
			wantEnd:   0,
			wantOK:    false,
		},
		{
			name:      "empty content",
			content:   "",
			symbol:    "Foo",
			wantStart: 0,
			wantEnd:   0,
			wantOK:    false,
		},
		{
			name:      "empty symbol",
			content:   "package p\n\nfunc Foo() {}\n",
			symbol:    "   ",
			wantStart: 0,
			wantEnd:   0,
			wantOK:    false,
		},
		{
			name:      "symbol not found",
			content:   "package p\n\nfunc Foo() {}\n",
			symbol:    "Bar",
			wantStart: 0,
			wantEnd:   0,
			wantOK:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotStart, gotEnd, gotOK := FindSymbolSpan(tc.content, tc.symbol)
			if gotStart != tc.wantStart || gotEnd != tc.wantEnd || gotOK != tc.wantOK {
				t.Fatalf("FindSymbolSpan(%q) = (%d, %d, %v), want (%d, %d, %v)", tc.symbol, gotStart, gotEnd, gotOK, tc.wantStart, tc.wantEnd, tc.wantOK)
			}
		})
	}
}

func TestResolveSymbolSpans(t *testing.T) {
	t.Parallel()
	shareRoot := t.TempDir()
	aerialDir := filepath.Join(shareRoot, "aerial")
	if err := os.MkdirAll(filepath.Join(aerialDir, "pkg"), 0o755); err != nil {
		t.Fatalf("failed to create test dir: %v", err)
	}

	fooContent := "package pkg\n\nfunc Foo() {\n\tSharedCall()\n}\n\nfunc Helper() {\n\treturn\n}\n\nfunc Common() {\n\treturn\n}\n"
	barContent := "package pkg\n\nfunc Bar() {\n\tHelper()\n\tSharedCall()\n}\n\nfunc Common() {\n\treturn\n}\n"
	bazContent := "package pkg\n\nfunc Baz() {\n\treturn\n}\n\nfunc ComputeBaz() {\n\treturn\n}\n"
	bazTestContent := "package pkg\n\nimport \"testing\"\n\nfunc Test(t *testing.T) {}\n\nfunc TestBaz_Subcase(t *testing.T) {\n\t// exercises Baz via route\n}\n\nfunc TestBazAndComputeBaz(t *testing.T) {\n\tBaz()\n\tComputeBaz()\n}\n\nfunc TestOtherBodyOnly(t *testing.T) {\n\tOnlyInBody()\n}\n\nconst testFixtureConst = 42\n"
	if err := os.WriteFile(filepath.Join(aerialDir, "pkg/foo.go"), []byte(fooContent), 0o600); err != nil {
		t.Fatalf("failed to write foo.go: %v", err)
	}
	if err := os.WriteFile(filepath.Join(aerialDir, "pkg/bar.go"), []byte(barContent), 0o600); err != nil {
		t.Fatalf("failed to write bar.go: %v", err)
	}
	if err := os.WriteFile(filepath.Join(aerialDir, "pkg/baz.go"), []byte(bazContent), 0o600); err != nil {
		t.Fatalf("failed to write baz.go: %v", err)
	}
	if err := os.WriteFile(filepath.Join(aerialDir, "pkg/baz_test.go"), []byte(bazTestContent), 0o600); err != nil {
		t.Fatalf("failed to write baz_test.go: %v", err)
	}

	tests := []struct {
		name            string
		repo            string
		targetFiles     []string
		targetSymbols   []string
		wantLines       []string
		wantAllResolved bool
	}{
		{
			name:            "all top-level symbols resolved",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/foo.go", "pkg/bar.go"},
			targetSymbols:   []string{"Foo", "Bar"},
			wantLines:       []string{"- pkg/foo.go: Foo (L3-L5)", "- pkg/bar.go: Bar (L3-L6)"},
			wantAllResolved: true,
		},
		{
			name:            "multi-file top-level symbol resolved in both files",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/foo.go", "pkg/bar.go"},
			targetSymbols:   []string{"Common"},
			wantLines:       []string{"- pkg/foo.go: Common (L11-L13)", "- pkg/bar.go: Common (L8-L10)"},
			wantAllResolved: true,
		},
		{
			name:            "multi-file fallback pass 2 symbol resolved in both files",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/foo.go", "pkg/bar.go"},
			targetSymbols:   []string{"SharedCall"},
			wantLines:       []string{"- pkg/foo.go: SharedCall (L4-L4)", "- pkg/bar.go: SharedCall (L5-L5)"},
			wantAllResolved: true,
		},
		{
			name:            "repo without slash and duplicate whitespace files",
			repo:            "aerial",
			targetFiles:     []string{"", "pkg/foo.go", "pkg/foo.go"},
			targetSymbols:   []string{"Foo"},
			wantLines:       []string{"- pkg/foo.go: Foo (L3-L5)"},
			wantAllResolved: true,
		},
		{
			name:            "cross-file pass 1 precedence over pass 2 call site",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/bar.go", "pkg/foo.go"},
			targetSymbols:   []string{"Helper"},
			wantLines:       []string{"- pkg/foo.go: Helper (L7-L9)"},
			wantAllResolved: true,
		},
		{
			name:            "fallback pass 2 resolves symbol across files",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/bar.go"},
			targetSymbols:   []string{"Helper"},
			wantLines:       []string{"- pkg/bar.go: Helper (L4-L4)"},
			wantAllResolved: true,
		},
		{
			name:            "partially resolved symbol",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/foo.go"},
			targetSymbols:   []string{"Foo", "NonExistent"},
			wantLines:       []string{"- pkg/foo.go: Foo (L3-L5)"},
			wantAllResolved: false,
		},
		{
			name:            "empty symbols emits full file line bounds",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/foo.go", "pkg/bar.go", "pkg/foo.go"},
			targetSymbols:   nil,
			wantLines:       []string{"- pkg/foo.go (L1-L14)", "- pkg/bar.go (L1-L11)"},
			wantAllResolved: true,
		},
		{
			name:            "companion test file auto-discovered with deduplicated test spans",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/baz.go"},
			targetSymbols:   []string{"Baz", "ComputeBaz", "TestBazAndComputeBaz"},
			wantLines:       []string{"- pkg/baz.go: Baz (L3-L5), ComputeBaz (L7-L9)", "- pkg/baz_test.go: TestBazAndComputeBaz (L11-L14), TestBaz_Subcase (L7-L9)"},
			wantAllResolved: true,
		},
		{
			name:            "explicit test file resolves body-referenced test block and fallback line",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/baz.go", "pkg/baz_test.go"},
			targetSymbols:   []string{"OnlyInBody", "testFixtureConst"},
			wantLines:       []string{"- pkg/baz_test.go: testFixtureConst (L20-L20), TestOtherBodyOnly (L16-L18)"},
			wantAllResolved: true,
		},
		{
			name:            "non existent file",
			repo:            "azylman/aerial",
			targetFiles:     []string{"pkg/missing.go"},
			targetSymbols:   []string{"Foo"},
			wantLines:       nil,
			wantAllResolved: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotLines, gotResolved := ResolveSymbolSpans(shareRoot, tc.repo, tc.targetFiles, tc.targetSymbols)
			if gotResolved != tc.wantAllResolved {
				t.Fatalf("ResolveSymbolSpans allResolved = %v, want %v", gotResolved, tc.wantAllResolved)
			}
			if len(gotLines) != len(tc.wantLines) {
				t.Fatalf("ResolveSymbolSpans lines len = %d (%v), want %d (%v)", len(gotLines), gotLines, len(tc.wantLines), tc.wantLines)
			}
			for i := range gotLines {
				if gotLines[i] != tc.wantLines[i] {
					t.Fatalf("line[%d] = %q, want %q", i, gotLines[i], tc.wantLines[i])
				}
			}
		})
	}
}

func TestFormatChunkPromptWithShareRoot(t *testing.T) {
	t.Parallel()
	shareRoot := t.TempDir()
	aerialDir := filepath.Join(shareRoot, "aerial")
	if err := os.MkdirAll(filepath.Join(aerialDir, "pkg"), 0o755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	content := "package pkg\n\nfunc TargetFunc() {\n\treturn\n}\n"
	if err := os.WriteFile(filepath.Join(aerialDir, "pkg/worker.go"), []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	build := mkBuild("build-test", "thread-1", "Test Build", "Test Objective", "")
	c1 := mkChunk("chunk-1", "azylman/aerial", 0, "Task 1", "Instructions", "normal", "pending")
	c1.TargetFiles = []string{"pkg/worker.go"}

	tests := []struct {
		name          string
		symbols       []string
		wantSpanText  string
		wantGrepCmd   bool
		checkContains bool
	}{
		{
			name:          "all symbols resolved suppresses run after init grep",
			symbols:       []string{"TargetFunc"},
			wantSpanText:  "Target Symbol Spans:\n- pkg/worker.go: TargetFunc (L3-L5)",
			wantGrepCmd:   false,
			checkContains: true,
		},
		{
			name:          "unresolved symbol falls back to run after init grep",
			symbols:       []string{"TargetFunc", "MissingFunc"},
			wantSpanText:  "Target Symbol Spans:\n- pkg/worker.go: TargetFunc (L3-L5)",
			wantGrepCmd:   true,
			checkContains: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := c1
			chunk.TargetSymbols = tc.symbols
			chunks := []VanadisChunk{chunk}

			prompt, err := formatChunkPromptWithShareRoot(build, chunks, 0, shareRoot)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(prompt, tc.wantSpanText) {
				t.Fatalf("prompt missing expected span text %q:\n%s", tc.wantSpanText, prompt)
			}
			hasGrep := strings.Contains(prompt, "Run after init:")
			if hasGrep != tc.wantGrepCmd {
				t.Fatalf("prompt contains Run after init: %v, want %v", hasGrep, tc.wantGrepCmd)
			}
		})
	}
}
