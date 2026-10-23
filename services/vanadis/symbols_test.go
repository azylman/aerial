package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFindSymbolSpan(t *testing.T) {
	t.Parallel()

	multiLineContent := `package p

func MultiLineFunc(a int) error {
	x := 1
	_ = x
	return nil
}
`

	complexSigContent := `package p

func ComplexSig(
	a int,
	b string,
) (int, error) {
	return a, nil
}
`

	structContent := `package p

type ServerConfig struct {
	Host string
	Port int
}
`

	rawBacktickContent := `package p

func RawBacktick() string {
	q := ` + "`" + `
}
` + "`" + `
	return q
}
`

	receiverContent := `package p

func (s *Server) ProcessTask() error {
	return nil
}
`

	fallbackContent := `package p

func Run() {
	localTarget := 10
	_ = localTarget
}
`

	commentedContent := `package p

func CommentedFunc() { // inline comment
	return
}

/* block comment */
func BlockCommented() { /* mid */ return }

func TrailingCommented() { /* unclosed comment
	return
}
`

	unclosedFuncContent := `package p

func UnclosedFunc() {
	x := 1

func NextFunc() {
	return
}
`

	eofFuncContent := `package p

func EOFFunc() {
	x := 1`

	varBlockContent := `package p

var (
	ConfigA = 1
)
`

	tests := []struct {
		name      string
		content   string
		symbol    string
		wantStart int
		wantEnd   int
		wantOK    bool
	}{
		{name: "empty symbol", content: multiLineContent, symbol: "", wantStart: 0, wantEnd: 0, wantOK: false},
		{name: "empty content", content: "", symbol: "MultiLineFunc", wantStart: 0, wantEnd: 0, wantOK: false},
		{name: "multi-line func", content: multiLineContent, symbol: "MultiLineFunc", wantStart: 3, wantEnd: 7, wantOK: true},
		{name: "single-line func", content: "package p\nfunc Bar() { return }\n", symbol: "Bar", wantStart: 2, wantEnd: 2, wantOK: true},
		{name: "multi-line parameter signature", content: complexSigContent, symbol: "ComplexSig", wantStart: 3, wantEnd: 8, wantOK: true},
		{name: "struct type definition", content: structContent, symbol: "ServerConfig", wantStart: 3, wantEnd: 6, wantOK: true},
		{name: "single-line struct", content: "package p\ntype Empty struct{}\n", symbol: "Empty", wantStart: 2, wantEnd: 2, wantOK: true},
		{name: "raw backtick with column-0 brace", content: rawBacktickContent, symbol: "RawBacktick", wantStart: 3, wantEnd: 8, wantOK: true},
		{name: "receiver pointer symbol", content: receiverContent, symbol: "*Server.ProcessTask", wantStart: 3, wantEnd: 5, wantOK: true},
		{name: "receiver paren pointer symbol", content: receiverContent, symbol: "(*Server).ProcessTask()", wantStart: 3, wantEnd: 5, wantOK: true},
		{name: "trailing line comment on declaration", content: commentedContent, symbol: "CommentedFunc", wantStart: 3, wantEnd: 5, wantOK: true},
		{name: "block comment on declaration", content: commentedContent, symbol: "BlockCommented", wantStart: 8, wantEnd: 8, wantOK: true},
		{name: "unclosed comment on declaration line", content: commentedContent, symbol: "TrailingCommented", wantStart: 10, wantEnd: 12, wantOK: true},
		{name: "unclosed func terminated by next func", content: unclosedFuncContent, symbol: "UnclosedFunc", wantStart: 3, wantEnd: 5, wantOK: true},
		{name: "unclosed func reaching EOF", content: eofFuncContent, symbol: "EOFFunc", wantStart: 3, wantEnd: 4, wantOK: true},
		{name: "var block closed by paren", content: varBlockContent, symbol: "var", wantStart: 3, wantEnd: 5, wantOK: true},
		{name: "fallback non-top-level line match", content: fallbackContent, symbol: "localTarget", wantStart: 4, wantEnd: 4, wantOK: true},
		{name: "symbol not present", content: multiLineContent, symbol: "NonExistent", wantStart: 0, wantEnd: 0, wantOK: false},
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

func setupMockRepoTree(t *testing.T) string {
	t.Helper()
	shareRoot := t.TempDir()
	aerialDir := filepath.Join(shareRoot, "aerial")
	if err := os.MkdirAll(filepath.Join(aerialDir, "pkg"), 0o755); err != nil {
		t.Fatalf("failed to create aerial mock dir: %v", err)
	}

	mainContent := `package main

func TopFunc() {
	localVar := 42
	_ = localVar
	println("top")
}

func SharedSymbol() {
	println("shared top")
}
`
	if err := os.WriteFile(filepath.Join(aerialDir, "main.go"), []byte(mainContent), 0o644); err != nil {
		t.Fatalf("failed to write main.go: %v", err)
	}

	testContent := `package main

import "testing"

func TestShared(t *testing.T) {
	SharedSymbol()
}
`
	if err := os.WriteFile(filepath.Join(aerialDir, "main_test.go"), []byte(testContent), 0o644); err != nil {
		t.Fatalf("failed to write main_test.go: %v", err)
	}

	pkgContent := `package pkg

type Widget struct {
	Name string
}
`
	if err := os.WriteFile(filepath.Join(aerialDir, "pkg", "widget.go"), []byte(pkgContent), 0o644); err != nil {
		t.Fatalf("failed to write widget.go: %v", err)
	}

	return shareRoot
}

func TestResolveSymbolSpans(t *testing.T) {
	t.Parallel()
	shareRoot := setupMockRepoTree(t)

	tests := []struct {
		name         string
		repo         string
		targetFiles  []string
		targetSyms   []string
		wantSpans    []string
		wantResolved bool
	}{
		{
			name:         "all symbols resolved cleanly",
			repo:         "azylman/aerial",
			targetFiles:  []string{"main.go", "pkg/widget.go"},
			targetSyms:   []string{"TopFunc", "Widget"},
			wantSpans:    []string{"- main.go: TopFunc (L3-L7)", "- pkg/widget.go: Widget (L3-L5)"},
			wantResolved: true,
		},
		{
			name:         "cross-file pass 1 precedence over pass 2",
			repo:         "aerial",
			targetFiles:  []string{"main_test.go", "main.go"},
			targetSyms:   []string{"SharedSymbol"},
			wantSpans:    []string{"- main.go: SharedSymbol (L9-L11)"},
			wantResolved: true,
		},
		{
			name:         "pass 2 fallback resolution across multiple files",
			repo:         "azylman/aerial",
			targetFiles:  []string{"pkg/widget.go", "./main.go", " "},
			targetSyms:   []string{"localVar", " "},
			wantSpans:    []string{"- main.go: localVar (L4-L4)"},
			wantResolved: true,
		},
		{
			name:         "partial resolution returns false for allResolved",
			repo:         "azylman/aerial",
			targetFiles:  []string{"main.go"},
			targetSyms:   []string{"TopFunc", "MissingSymbol"},
			wantSpans:    []string{"- main.go: TopFunc (L3-L7)"},
			wantResolved: false,
		},
		{
			name:         "empty target symbols returns resolved without spans",
			repo:         "azylman/aerial",
			targetFiles:  []string{"main.go"},
			targetSyms:   nil,
			wantSpans:    nil,
			wantResolved: true,
		},
		{
			name:         "empty target files returns resolved without spans",
			repo:         "azylman/aerial",
			targetFiles:  nil,
			targetSyms:   []string{"TopFunc"},
			wantSpans:    nil,
			wantResolved: true,
		},
		{
			name:         "empty repo returns unresolved",
			repo:         "",
			targetFiles:  []string{"main.go"},
			targetSyms:   []string{"TopFunc"},
			wantSpans:    nil,
			wantResolved: false,
		},
		{
			name:         "nonexistent repo returns unresolved",
			repo:         "unknown/repo",
			targetFiles:  []string{"main.go"},
			targetSyms:   []string{"TopFunc"},
			wantSpans:    nil,
			wantResolved: false,
		},
		{
			name:         "path traversal safely blocked",
			repo:         "azylman/aerial",
			targetFiles:  []string{"../../etc/passwd", "../aerial/main.go", "/abs/path.go"},
			targetSyms:   []string{"TopFunc"},
			wantSpans:    nil,
			wantResolved: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotSpans, gotResolved := ResolveSymbolSpans(shareRoot, tc.repo, tc.targetFiles, tc.targetSyms)
			if gotResolved != tc.wantResolved {
				t.Fatalf("ResolveSymbolSpans resolved = %v, want %v (spans: %v)", gotResolved, tc.wantResolved, gotSpans)
			}
			if len(gotSpans) != len(tc.wantSpans) {
				t.Fatalf("ResolveSymbolSpans spans count = %d, want %d: got %v, want %v", len(gotSpans), len(tc.wantSpans), gotSpans, tc.wantSpans)
			}
			for i := range gotSpans {
				if gotSpans[i] != tc.wantSpans[i] {
					t.Fatalf("ResolveSymbolSpans span[%d] = %q, want %q", i, gotSpans[i], tc.wantSpans[i])
				}
			}
		})
	}
}

func TestFormatChunkPromptWithShareRoot(t *testing.T) {
	t.Parallel()
	shareRoot := setupMockRepoTree(t)

	build := mkBuild("build-test", "12345", "Test Build", "Test Objective", "")
	c1 := mkChunk("chunk-1", "azylman/aerial", 0, "Task 1", "Instructions 1", "", "running")
	c1.TargetFiles = []string{"main.go"}
	c1.TargetSymbols = []string{"TopFunc"}

	c2 := mkChunk("chunk-2", "azylman/aerial", 1, "Task 2", "Instructions 2", "", "pending")
	c2.TargetFiles = []string{"main.go"}
	c2.TargetSymbols = []string{"MissingSymbol"}

	chunks := []VanadisChunk{c1, c2}

	p1, err := formatChunkPromptWithShareRoot(build, chunks, 0, shareRoot)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(p1, "Target Symbol Spans:\n- main.go: TopFunc (L3-L7)") {
		t.Fatalf("expected resolved symbol spans in p1, got:\n%s", p1)
	}
	if strings.Contains(p1, "Run after init:") {
		t.Fatalf("expected no 'Run after init:' when all symbols resolved in p1, got:\n%s", p1)
	}

	p2, err := formatChunkPromptWithShareRoot(build, chunks, 1, shareRoot)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(p2, "Run after init: `grep -n -E \"MissingSymbol\" \"$SCRATCH_DIR/main.go\"`") {
		t.Fatalf("expected 'Run after init:' when symbols unresolved in p2, got:\n%s", p2)
	}
}
