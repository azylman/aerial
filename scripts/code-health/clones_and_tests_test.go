package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type analyzeTestCase struct {
	name           string
	content        string
	rel            string
	missingFile    bool
	wantViolations []string
	wantErr        bool
}

func runAnalyzeTestFileCase(t *testing.T, tc analyzeTestCase) {
	t.Helper()
	dir := t.TempDir()
	rel := tc.rel
	if rel == "" {
		rel = "sample_test.go"
	}
	filePath := filepath.Join(dir, filepath.FromSlash(rel))
	if !tc.missingFile {
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filePath, []byte(tc.content), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
	} else {
		filePath = filepath.Join(dir, "does_not_exist_test.go")
		rel = "does_not_exist_test.go"
	}

	fm, violations, err := analyzeTestFile(filePath, rel, defaultRules())
	if (err != nil) != tc.wantErr {
		t.Fatalf("analyzeTestFile() error = %v, wantErr = %v", err, tc.wantErr)
	}
	if !tc.wantErr && fm.Lines < 0 {
		t.Errorf("expected non-negative lines, got %d", fm.Lines)
	}
	if len(tc.wantViolations) > 0 {
		joined := strings.Join(violations, "\n")
		for _, wv := range tc.wantViolations {
			if !strings.Contains(joined, wv) {
				t.Errorf("violations output does not contain expected snippet %q; got:\n%s", wv, joined)
			}
		}
	} else if len(violations) > 0 && !tc.wantErr {
		t.Errorf("expected 0 violations, got %d: %v", len(violations), violations)
	}
}

func TestAnalyzeTestFile(t *testing.T) {
	t.Parallel()

	for _, tc := range analyzeTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runAnalyzeTestFileCase(t, tc)
		})
	}
}

func analyzeTestCases() []analyzeTestCase {
	all := append(analyzeTestCasesBase(), analyzeTestCasesPorts()...)
	return append(all, analyzeTestCasesTables()...)
}

func analyzeTestCasesPorts() []analyzeTestCase {
	return []analyzeTestCase{
		{
			name: "reserved live localhost daemon ports in test HTTP calls detected",
			content: `package pkg

import (
	"context"
	"net/http"
)

func TestReservedPorts(ctx context.Context) {
	_, _ = http.Get("http://localhost:8080")
	_, _ = http.Post("http://127.0.0.1:4020/webhook", "application/json", nil)
	_, _ = http.NewRequest(http.MethodGet, "http://[::1]:8081", nil)
	_, _ = http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8082/builds", nil)
	_ = NewHTTPPromptDispatcher("http://localhost:8083")
	_ = haro.NewHTTPPromptDispatcher("http://localhost:8083")
	_, _ = newRequestWithContext(ctx, http.MethodGet, "http://localhost:8090/test", nil)
	_, _ = http.Get("http://localhost:8070/call")
	_, _ = http.Get("http://localhost:9070/status")
	_, _ = http.Get("http://localhost:8088/alt")
	_, _ = http.Get("localhost:8080")
	_, _ = http.Get(` + "`" + `localhost:8080` + "`" + `)
}
`,
			wantViolations: []string{
				"sample_test.go:9: call to http.Get targeting reserved live daemon port 8080 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:10: call to http.Post targeting reserved live daemon port 4020 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:11: call to http.NewRequest targeting reserved live daemon port 8081 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:12: call to http.NewRequestWithContext targeting reserved live daemon port 8082 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:13: call to NewHTTPPromptDispatcher targeting reserved live daemon port 8083 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:14: call to NewHTTPPromptDispatcher targeting reserved live daemon port 8083 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:15: call to newRequestWithContext targeting reserved live daemon port 8090 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:16: call to http.Get targeting reserved live daemon port 8070 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:17: call to http.Get targeting reserved live daemon port 9070 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:18: call to http.Get targeting reserved live daemon port 8088 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:19: call to http.Get targeting reserved live daemon port 8080 in tests is prohibited; use httptest.Server or mock transport instead",
				"sample_test.go:20: call to http.Get targeting reserved live daemon port 8080 in tests is prohibited; use httptest.Server or mock transport instead",
			},
		},
		{
			name: "safe non-reserved or dynamic HTTP calls produce no violations",
			content: `package pkg

import (
	"context"
	"fmt"
	"net/http"
)

func TestAllowedCalls(ctx context.Context, srvURL string) {
	_, _ = http.Get(srvURL)
	_, _ = http.Get("http://localhost:54321")
	_, _ = http.Get("http://example.com:8080")
	_, _ = http.NewRequest(http.MethodGet, "http://localhost:18080", nil)
	_ = NewHTTPPromptDispatcher(srvURL)
	fmt.Println("http://localhost:8080")
	_, _ = http.Get("http://[invalid-ipv6:8080")
}
`,
		},
	}
}

func analyzeTestCasesBase() []analyzeTestCase {
	return []analyzeTestCase{
		{
			name: "clean test file produces no violations",
			content: `package pkg

import (
	"net/http"
	"syscall"
	"time"
)

func TestValid(rt http.RoundTripper) {
	_ = time.Now()
	_ = syscall.Getpid()
	http.DefaultClient.CloseIdleConnections()
	_ = &http.Client{Timeout: time.Second, Transport: rt}
}
type Case struct {
	expected int
}
`,
		},
		{
			name: "banned time.Sleep, syscall.Kill, DefaultClient, and unairgapped http.Client detected",
			content: `package pkg

import (
	"net/http"
	"syscall"
	"time"
)

func TestBannedCalls(req *http.Request) {
	time.Sleep(time.Millisecond)
	_ = syscall.Kill(1, syscall.SIGHUP)
	_, _ = http.DefaultClient.Do(req)
	_ = &http.Client{}
	_ = &http.Client{Timeout: time.Second}
	_ = &http.Client{Transport: nil}
	_ = &http.Client{Transport: http.DefaultTransport}
	_ = &http.Client{Transport: &http.Transport{}}
	_ = &http.Client{Transport: http.Transport{}}
}
`,
			wantViolations: []string{
				"sample_test.go:10: time.Sleep in tests is prohibited; use event-driven channels, condition variables, or injected delay overrides",
				"sample_test.go:11: raw syscall.Kill in tests is prohibited; use dependency-injected signal or process-kill functions",
				"sample_test.go:12: unguarded http.DefaultClient in tests is prohibited; use httptest.Server.Client() or an http.Client with a mock Transport",
				"sample_test.go:13: http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()",
				"sample_test.go:14: http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()",
				"sample_test.go:15: http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()",
				"sample_test.go:16: http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()",
				"sample_test.go:17: http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()",
				"sample_test.go:18: http.Client literal without mock Transport in tests is prohibited; inject a mock RoundTripper or use httptest.Server.Client()",
			},
		},
		{
			name: "generated test file is skipped",
			content: "// Code generated by tool; DO NOT EDIT.\npackage pkg\n//nolint:errcheck\nfunc TestFoo_Extra() {}\n" +
				"type Case struct {\n\t" + "check" + " " + "func()\n}\n",
		},
		{
			name:        "nonexistent file returns error",
			missingFile: true,
			wantErr:     true,
		},
		{
			name:    "syntax error returns error",
			content: "package pkg\nfunc (",
			wantErr: true,
		},
		{
			name:    "catch-all extra test filename detected",
			rel:     "pkg/foo_extra_test.go",
			content: "package pkg\n",
			wantViolations: []string{
				"pkg/foo_extra_test.go: test filename uses banned catch-all suffix",
			},
		},
		{
			name:    "catch-all more test filename detected",
			rel:     "pkg/foo_more_test.go",
			content: "package pkg\n",
			wantViolations: []string{
				"pkg/foo_more_test.go: test filename uses banned catch-all suffix",
			},
		},
		{
			name:    "catch-all coverage gaps test filename detected",
			rel:     "pkg/foo_coverage_gaps_test.go",
			content: "package pkg\n",
			wantViolations: []string{
				"pkg/foo_coverage_gaps_test.go: test filename uses banned catch-all suffix",
			},
		},
		{
			name:    "catch-all additional test filename detected",
			rel:     "pkg/foo_additional_test.go",
			content: "package pkg\n",
			wantViolations: []string{
				"pkg/foo_additional_test.go: test filename uses banned catch-all suffix",
			},
		},
	}
}

func analyzeTestCasesTables() []analyzeTestCase {
	return []analyzeTestCase{
		{
			name: "split test names and closure fields detected",
			content: `package pkg

//nolint:gocyclo
func TestA_Extra() {}
func TestB_MoreCases() {}
func TestC_Additional() {}
func TestD_Uncovered() {}
func TestE_EdgeCases() {}
func TestF_CornerCases() {}
func TestG_Branches() {}
func TestH_AllBranches() {}
func TestI_Chunk1_Cases() {}
func TestJ_CoverageGaps() {}
` + buildClosureFieldsSource(),
			wantViolations: []string{
				"//nolint:gocyclo is prohibited",
				"test function TestA_Extra uses banned split/catch-all suffix",
				"test function TestB_MoreCases uses banned split/catch-all suffix",
				"test function TestC_Additional uses banned split/catch-all suffix",
				"test function TestD_Uncovered uses banned split/catch-all suffix",
				"test function TestE_EdgeCases uses banned split/catch-all suffix",
				"test function TestF_CornerCases uses banned split/catch-all suffix",
				"test function TestG_Branches uses banned split/catch-all suffix",
				"test function TestH_AllBranches uses banned split/catch-all suffix",
				"test function TestI_Chunk1_Cases uses banned split/catch-all suffix",
				"test function TestJ_CoverageGaps uses banned split/catch-all suffix",
				`test struct field "run" uses a banned closure func type`,
				`test struct field "check" uses a banned closure func type`,
				`test struct field "extraCheck" uses a banned closure func type`,
				`test struct field "setup" uses a banned closure func type`,
				`test struct field "validate" uses a banned closure func type`,
				`test struct field "validator" uses a banned closure func type`,
				`test struct field "verify" uses a banned closure func type`,
				`test struct field "lookup" uses a banned closure func type`,
			},
		},
		{
			name: "arbitrary func field in slice of struct test table is detected",
			content: `package pkg

import "testing"

func TestTableWithFunc(t *testing.T) {
	tests := []struct {
		name string
		hook func()
	}{
		{name: "a"},
	}
	_ = tests
}
`,
			wantViolations: []string{
				`test struct field "hook" uses a banned closure func type; use declarative expectation fields instead`,
			},
		},
	}
}

func TestIsGoBuiltin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sym  string
		want bool
	}{
		{name: "len", sym: "len", want: true},
		{name: "cap", sym: "cap", want: true},
		{name: "make", sym: "make", want: true},
		{name: "new", sym: "new", want: true},
		{name: "append", sym: "append", want: true},
		{name: "copy", sym: "copy", want: true},
		{name: "delete", sym: "delete", want: true},
		{name: "close", sym: "close", want: true},
		{name: "min", sym: "min", want: true},
		{name: "max", sym: "max", want: true},
		{name: "panic", sym: "panic", want: true},
		{name: "recover", sym: "recover", want: true},
		{name: "print", sym: "print", want: true},
		{name: "println", sym: "println", want: true},
		{name: "complex", sym: "complex", want: true},
		{name: "real", sym: "real", want: true},
		{name: "imag", sym: "imag", want: true},
		{name: "clear", sym: "clear", want: true},
		{name: "non-builtin custom", sym: "customFunc", want: false},
		{name: "empty name", sym: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isGoBuiltin(tc.sym); got != tc.want {
				t.Errorf("isGoBuiltin(%q) = %v, want %v", tc.sym, got, tc.want)
			}
		})
	}
}

func TestJaccardSimilarity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    map[string]struct{}
		b    map[string]struct{}
		want float64
	}{
		{name: "both empty", a: nil, b: nil, want: 0.0},
		{name: "left empty", a: nil, b: map[string]struct{}{"x": {}}, want: 0.0},
		{name: "right empty", a: map[string]struct{}{"x": {}}, b: nil, want: 0.0},
		{name: "identical sets", a: map[string]struct{}{"x": {}, "y": {}}, b: map[string]struct{}{"x": {}, "y": {}}, want: 1.0},
		{name: "disjoint sets", a: map[string]struct{}{"x": {}}, b: map[string]struct{}{"y": {}}, want: 0.0},
		{name: "partial overlap", a: map[string]struct{}{"x": {}, "y": {}}, b: map[string]struct{}{"y": {}, "z": {}}, want: 1.0 / 3.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := jaccardSimilarity(tc.a, tc.b)
			if diff := got - tc.want; diff > 1e-6 || diff < -1e-6 {
				t.Errorf("jaccardSimilarity() = %f, want %f", got, tc.want)
			}
		})
	}
}

func TestCollectTestTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		files    map[string]string
		explicit []string
		want     []string
		wantErr  bool
	}{
		{
			name: "collects candidates and skips generated and excluded",
			files: map[string]string{
				"pkg/a_test.go":          "package pkg\n",
				"pkg/b_gen.go":           "package pkg\n",
				"pkg/c.gen.go":           "package pkg\n",
				"pkg/d.pb.go":            "package pkg\n",
				"pkg/e_gen_test.go":      "package pkg\n",
				"pkg/f.gen_test.go":      "package pkg\n",
				"pkg/g.pb_test.go":       "package pkg\n",
				"vendor/v/v_test.go":     "package v\n",
				"testdata/t_test.go":     "package t\n",
				".hidden/h_test.go":      "package h\n",
				"node_modules/m_test.go": "package m\n",
			},
			want: []string{"pkg/a_test.go"},
		},
		{
			name: "explicit targets filters valid candidates",
			files: map[string]string{
				"pkg/a_test.go": "package pkg\n",
				"pkg/b_test.go": "package pkg\n",
			},
			explicit: []string{"pkg/b_test.go", "pkg/nonexistent_test.go", "pkg/regular.go"},
			want:     []string{"pkg/b_test.go"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			setupTestFiles(t, dir, tc.files)

			var explicitArgs []string
			for _, exp := range tc.explicit {
				explicitArgs = append(explicitArgs, filepath.Join(dir, filepath.FromSlash(exp)))
			}

			got, err := collectTestTargets(dir, explicitArgs)
			if (err != nil) != tc.wantErr {
				t.Fatalf("collectTestTargets() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("collectTestTargets() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildCloneTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		src        string
		wantTokens []string
	}{
		{
			name: "call expressions and type switch tokens",
			src:  buildCallAndTypeSwitchSource(),
			wantTokens: []string{
				"switch", "case", "call:.Process", "case", "call:fn",
			},
		},
		{
			name: "control flow constructs",
			src: `package sample
func Control(ch chan int) int {
	if true {
		defer close(ch)
	} else {
		for i := 0; i < 1; i++ {
			for range []int{1} {
				select {
				case <-ch:
					return 1
				default:
					return 0
				}
			}
		}
	}
	return 0
}
`,
			wantTokens: []string{
				"if", "else", "defer", "for", "range", "select", "comm", "return", "comm", "return", "return",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := buildCloneTokens(firstFuncBody(t, tc.src)); !slices.Equal(got, tc.wantTokens) {
				t.Errorf("buildCloneTokens() = %v, want %v", got, tc.wantTokens)
			}
		})
	}
}

func TestBuildScopedCloneTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		src        string
		wantTokens []string
	}{
		{name: "scoped block delimiters with calls and controls", src: "package sample\nfunc Scoped() {\n\tif true {\n\t\tprintln(\"ok\")\n\t}\n}\n", wantTokens: []string{"{", "if", "{", "}", "}"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := buildScopedCloneTokens(firstFuncBody(t, tc.src)); !slices.Equal(got, tc.wantTokens) {
				t.Errorf("buildScopedCloneTokens() = %v, want %v", got, tc.wantTokens)
			}
		})
	}
}

func TestBuildScopedCloneTokensNilBody(t *testing.T) {
	t.Parallel()
	if got := buildScopedCloneTokens(nil); got != nil {
		t.Fatalf("buildScopedCloneTokens(nil) = %v, want nil", got)
	}
}

// firstFuncBody parses src and returns the body of its first function declaration.
func firstFuncBody(t *testing.T, src string) *ast.BlockStmt {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "sample.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
			return fn.Body
		}
	}
	t.Fatal("no function body found")
	return nil
}

func TestFormatFieldType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr ast.Expr
		want string
	}{
		{name: "ident", expr: &ast.Ident{Name: "int"}, want: "int"},
		{name: "star expr", expr: &ast.StarExpr{X: &ast.Ident{Name: "string"}}, want: "*string"},
		{name: "selector expr", expr: &ast.SelectorExpr{X: &ast.Ident{Name: "pkg"}, Sel: &ast.Ident{Name: "Type"}}, want: "pkg.Type"},
		{name: "array type", expr: &ast.ArrayType{Elt: &ast.Ident{Name: "byte"}}, want: "[]byte"},
		{name: "map type", expr: &ast.MapType{Key: &ast.Ident{Name: "string"}, Value: &ast.Ident{Name: "int"}}, want: "map[string]int"},
		{name: "fallback", expr: &ast.ChanType{Value: &ast.Ident{Name: "int"}}, want: "other"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatFieldType(tc.expr); got != tc.want {
				t.Errorf("formatFieldType() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEditSimilarity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    []string
		b    []string
		want float64
	}{
		{name: "both empty", a: nil, b: nil, want: 0.0},
		{name: "left empty", a: nil, b: []string{"x"}, want: 0.0},
		{name: "right empty", a: []string{"x"}, b: nil, want: 0.0},
		{name: "identical slices", a: []string{"a", "b", "c"}, b: []string{"a", "b", "c"}, want: 1.0},
		{name: "single substitution", a: []string{"a", "b", "c"}, b: []string{"a", "x", "c"}, want: 2.0 / 3.0},
		{name: "single insertion", a: []string{"a", "b"}, b: []string{"a", "x", "b"}, want: 2.0 / 3.0},
		{name: "completely disjoint", a: []string{"a", "b"}, b: []string{"c", "d"}, want: 0.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := editSimilarity(tc.a, tc.b)
			if diff := got - tc.want; diff > 1e-6 || diff < -1e-6 {
				t.Errorf("editSimilarity() = %f, want %f", got, tc.want)
			}
		})
	}
}

func TestIsCleanupCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{name: "selector Cleanup", src: "package p\nfunc f(t T) { t.Cleanup(nil) }\ntype T struct{}\nfunc (T) Cleanup(any) {}", want: true},
		{name: "selector Other", src: "package p\nfunc f(t T) { t.Other(nil) }\ntype T struct{}\nfunc (T) Other(any) {}", want: false},
		{name: "ident call", src: "package p\nfunc f() { Cleanup() }\nfunc Cleanup() {}", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			call := findFirstCallExpr(t, tc.src)
			if got := isCleanupCall(call); got != tc.want {
				t.Errorf("isCleanupCall() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsNonExemptBlankCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{name: "non-exempt ident call", src: "package p\nfunc f() { _ = fn() }\nfunc fn() error { return nil }", want: true},
		{name: "non-exempt selector call", src: "package p\nfunc f(s S) { _ = s.Do() }\ntype S struct{}\nfunc (S) Do() error { return nil }", want: true},
		{name: "exempt close call", src: "package p\nfunc f(s S) { _ = s.Close() }\ntype S struct{}\nfunc (S) Close() error { return nil }", want: false},
		{name: "exempt encode call", src: "package p\nfunc f(s S) { _ = s.Encode(1) }\ntype S struct{}\nfunc (S) Encode(any) error { return nil }", want: false},
		{name: "exempt write call", src: "package p\nfunc f(s S) { _ = s.Write(nil) }\ntype S struct{}\nfunc (S) Write([]byte) (int, error) { return 0, nil }", want: false},
		{name: "exempt write header call", src: "package p\nfunc f(s S) { _ = s.WriteHeader(200) }\ntype S struct{}\nfunc (S) WriteHeader(int) error { return nil }", want: false},
		{name: "exempt write string call", src: "package p\nfunc f(s S) { _ = s.WriteString(\"a\") }\ntype S struct{}\nfunc (S) WriteString(string) (int, error) { return 0, nil }", want: false},
		{name: "exempt flush call", src: "package p\nfunc f(s S) { _ = s.Flush() }\ntype S struct{}\nfunc (S) Flush() error { return nil }", want: false},
		{name: "exempt remove call", src: "package p\nfunc f(s S) { _ = s.Remove(\"a\") }\ntype S struct{}\nfunc (S) Remove(string) error { return nil }", want: false},
		{name: "exempt remove all call", src: "package p\nfunc f(s S) { _ = s.RemoveAll(\"a\") }\ntype S struct{}\nfunc (S) RemoveAll(string) error { return nil }", want: false},
		{name: "exempt setenv call", src: "package p\nfunc f(s S) { _ = s.Setenv(\"a\", \"b\") }\ntype S struct{}\nfunc (S) Setenv(string, string) error { return nil }", want: false},
		{name: "exempt unsetenv call", src: "package p\nfunc f(s S) { _ = s.Unsetenv(\"a\") }\ntype S struct{}\nfunc (S) Unsetenv(string) error { return nil }", want: false},
		{name: "exempt chdir call", src: "package p\nfunc f(s S) { _ = s.Chdir(\"a\") }\ntype S struct{}\nfunc (S) Chdir(string) error { return nil }", want: false},
		{name: "exempt unsubscribe call", src: "package p\nfunc f(s S) { _ = s.Unsubscribe() }\ntype S struct{}\nfunc (S) Unsubscribe() error { return nil }", want: false},
		{name: "exempt stop call", src: "package p\nfunc f(s S) { _ = s.Stop() }\ntype S struct{}\nfunc (S) Stop() bool { return true }", want: false},
		{name: "exempt cancel call", src: "package p\nfunc f(s S) { _ = s.Cancel() }\ntype S struct{}\nfunc (S) Cancel() error { return nil }", want: false},
		{name: "lhs not blank ident", src: "package p\nfunc f() { x := fn() }\nfunc fn() error { return nil }", want: false},
		{name: "rhs not call expr", src: "package p\nfunc f() { var x int; _ = x }", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lhs, rhs := findFirstAssignExprs(t, tc.src)
			if got := isNonExemptBlankCall(lhs, rhs); got != tc.want {
				t.Errorf("isNonExemptBlankCall() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCountTestBlankCallAssigns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{name: "single blank call discard", src: "package p\nfunc f() { _ = fn() }\nfunc fn() error { return nil }", want: 1},
		{name: "multi return blank discard", src: "package p\nfunc f() { v, _ := fn(); _ = v }\nfunc fn() (int, error) { return 0, nil }", want: 1},
		{name: "multi assign blank discard", src: "package p\nfunc f() { var a int; a, _ = f1(), f2(); _ = a }\nfunc f1() int { return 0 }\nfunc f2() error { return nil }", want: 1},
		{name: "allowed value discard", src: "package p\nfunc f() { _, err := fn(); _ = err }\nfunc fn() (int, error) { return 0, nil }", want: 0},
		{name: "exempt cleanup and defer discards", src: "package p\ntype C struct{}\nfunc (C) Close() error { return nil }\nfunc (C) Cleanup(any) {}\nfunc fn() error { return nil }\nfunc f(c C) {\n\t_ = c.Close()\n\tdefer func() { _ = fn() }()\n\tc.Cleanup(func() { _ = fn() })\n}\n", want: 0},
		{name: "non cleanup selector call with closure", src: "package p\ntype C struct{}\nfunc (C) Custom(any) {}\nfunc fn() error { return nil }\nfunc f(c C) {\n\tc.Custom(func() { _ = fn() })\n}\n", want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "p_test.go", tc.src, 0)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := countTestBlankCallAssigns(f); got != tc.want {
				t.Errorf("countTestBlankCallAssigns() = %d, want %d", got, tc.want)
			}
		})
	}
}

func findFirstCallExpr(t *testing.T, src string) *ast.CallExpr {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "p.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var found *ast.CallExpr
	ast.Inspect(f, func(n ast.Node) bool {
		if found != nil {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok {
			found = call
			return false
		}
		return true
	})
	if found == nil {
		t.Fatal("no call expression found")
	}
	return found
}

func findFirstAssignExprs(t *testing.T, src string) (ast.Expr, ast.Expr) {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "p.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var lhs, rhs ast.Expr
	ast.Inspect(f, func(n ast.Node) bool {
		if lhs != nil {
			return false
		}
		if assign, ok := n.(*ast.AssignStmt); ok {
			if len(assign.Lhs) > 0 && len(assign.Rhs) == 1 {
				lhs = assign.Lhs[len(assign.Lhs)-1]
				rhs = assign.Rhs[0]
				return false
			}
			if len(assign.Lhs) > 0 && len(assign.Rhs) == len(assign.Lhs) {
				lhs = assign.Lhs[0]
				rhs = assign.Rhs[0]
				return false
			}
		}
		return true
	})
	if lhs == nil || rhs == nil {
		t.Fatal("no assign expressions found")
	}
	return lhs, rhs
}
