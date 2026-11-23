package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRun_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		secrets     map[string]string
		extra       string
		withExtra   bool
		noOut       bool
		statements  bool
		extraArgs   []string
		missingFile bool
		wantCode    int
		wantStdout  string
		wantGroups  int
		matches     []string
		nonMatches  []string
	}{
		{
			name:       "secrets dir and extra file",
			secrets:    map[string]string{"API_KEY": "dummy_api_value_one", "DB_PASS": "dummy \"db\" pass.*", ".hidden": "dummy_hidden_value"},
			extra:      "dummy-canary-value\n\n  \nabc\n",
			withExtra:  true,
			wantCode:   0,
			wantStdout: "redact-pattern: wrote pattern for 4 values\n",
			matches:    []string{"k=dummy_api_value_one;", `x dummy "db" pass.* y`, "span dummy-canary-value end", "ZHVtbXktY2FuYXJ5LXZhbHVl"},
			nonMatches: []string{"dummy_hidden_value", "abc", "dummy db pass"},
		},
		{
			name:       "statements and pattern from one run",
			secrets:    map[string]string{"API_KEY": "dummy_api_value_one", "DB_PASS": "dummy \"db\" pass.*", ".hidden": "dummy_hidden_value"},
			extra:      "dummy-canary-value\n\n  \nabc\n",
			withExtra:  true,
			statements: true,
			wantCode:   0,
			wantStdout: "redact-pattern: wrote pattern for 4 values\nredact-pattern: wrote statements for 4 values in 1 groups\n",
			wantGroups: 1,
			matches:    []string{"k=dummy_api_value_one;", `x dummy "db" pass.* y`, "span dummy-canary-value end", "ZHVtbXktY2FuYXJ5LXZhbHVl"},
			nonMatches: []string{"dummy_hidden_value", "abc", "dummy db pass"},
		},
		{
			name:       "statements only split into one group per value",
			secrets:    map[string]string{"API_KEY": "dummy_api_value_one", "DB_PASS": "dummy \"db\" pass.*"},
			extra:      "dummy-canary-value\nabc\n",
			withExtra:  true,
			noOut:      true,
			statements: true,
			extraArgs:  []string{"-max-pattern-bytes", "50"},
			wantCode:   0,
			wantStdout: "redact-pattern: wrote statements for 4 values in 3 groups\n",
			wantGroups: 3,
			matches:    []string{"k=dummy_api_value_one;", `x dummy "db" pass.* y`, "span dummy-canary-value end", "ZHVtbXktY2FuYXJ5LXZhbHVl"},
			nonMatches: []string{"abc", "dummy db pass"},
		},
		{
			name:       "empty secrets statements match nothing",
			secrets:    map[string]string{},
			noOut:      true,
			statements: true,
			wantCode:   0,
			wantStdout: "redact-pattern: wrote statements for 0 values in 0 groups\n",
			wantGroups: 1,
			nonMatches: []string{"", "anything"},
		},
		{
			name:       "invalid utf-8 value fails statements",
			secrets:    map[string]string{"API_KEY": "dummy\xffvalue"},
			noOut:      true,
			statements: true,
			wantCode:   1,
		},
		{
			name:      "unwritable statements path fails",
			secrets:   map[string]string{"API_KEY": "dummy_api_value_six"},
			noOut:     true,
			extraArgs: []string{"-statements-out", "/nonexistent-redact-dir/statements.yaml"},
			wantCode:  1,
		},
		{
			name:      "non-positive max pattern bytes fails",
			secrets:   map[string]string{"API_KEY": "dummy_api_value_seven"},
			extraArgs: []string{"-max-pattern-bytes", "0"},
			wantCode:  2,
		},
		{
			name:       "empty secrets dir without extra file matches nothing",
			secrets:    map[string]string{},
			wantCode:   0,
			wantStdout: "redact-pattern: wrote pattern for 0 values\n",
			nonMatches: []string{"", "anything"},
		},
		{
			name:        "missing extra file fails",
			secrets:     map[string]string{"API_KEY": "dummy_api_value_two"},
			withExtra:   true,
			missingFile: true,
			wantCode:    1,
		},
		{
			name:      "unknown flag fails",
			secrets:   map[string]string{"API_KEY": "dummy_api_value_four"},
			extraArgs: []string{"-bogus"},
			wantCode:  2,
		},
		{
			name:      "unwritable out path fails",
			secrets:   map[string]string{"API_KEY": "dummy_api_value_five"},
			extraArgs: []string{"-out", "/nonexistent-redact-dir/pattern"},
			wantCode:  1,
		},
		{
			name:     "missing out flag fails",
			secrets:  map[string]string{"API_KEY": "dummy_api_value_three"},
			noOut:    true,
			wantCode: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			args := buildArgs(t, root, tc.secrets, tc.extra, tc.withExtra, tc.missingFile, tc.noOut, tc.statements)
			args = append(args, tc.extraArgs...)

			var stdout, stderr bytes.Buffer
			code := run(args, &stdout, &stderr)
			if code != tc.wantCode {
				t.Fatalf("run() = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if tc.wantStdout != "" && stdout.String() != tc.wantStdout {
				t.Errorf("stdout = %q, want %q", stdout.String(), tc.wantStdout)
			}
			assertNoLeak(t, stdout.String()+stderr.String(), tc.secrets, tc.extra)
			if tc.wantCode == 0 && !tc.noOut {
				assertPattern(t, filepath.Join(root, "pattern"), tc.matches, tc.nonMatches)
			}
			if tc.wantCode == 0 && tc.statements {
				assertStatements(t, filepath.Join(root, "statements.yaml"), tc.wantGroups, tc.matches, tc.nonMatches)
			}
		})
	}
}

func buildArgs(t *testing.T, root string, secrets map[string]string, extra string, withExtra, missingFile, noOut, statements bool) []string {
	t.Helper()
	secretsDir := filepath.Join(root, "secrets")
	if err := os.Mkdir(secretsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for k, v := range secrets {
		if err := os.WriteFile(filepath.Join(secretsDir, k), []byte(v+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"-secrets-dir", secretsDir}
	if !noOut {
		args = append(args, "-out", filepath.Join(root, "pattern"))
	}
	if statements {
		args = append(args, "-statements-out", filepath.Join(root, "statements.yaml"))
	}
	if !withExtra {
		return args
	}
	extraPath := filepath.Join(root, "extra")
	if !missingFile {
		if err := os.WriteFile(extraPath, []byte(extra), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return append(args, "-extra-file", extraPath)
}

func assertNoLeak(t *testing.T, output string, secrets map[string]string, extra string) {
	t.Helper()
	values := make([]string, 0, len(secrets))
	for _, v := range secrets {
		values = append(values, v)
	}
	for line := range strings.SplitSeq(extra, "\n") {
		values = append(values, line)
	}
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" && strings.Contains(output, v) {
			t.Errorf("output leaked a configured value: %q", output)
		}
	}
}

func assertPattern(t *testing.T, outPath string, matches, nonMatches []string) {
	t.Helper()
	body, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	assertMatches(t, []*regexp.Regexp{compileBody(t, string(body))}, matches, nonMatches)
}

// compileBody compiles an OTTL-escaped pattern body.
func compileBody(t *testing.T, body string) *regexp.Regexp {
	t.Helper()
	re, err := regexp.Compile(strings.NewReplacer(`\\`, `\`, `\"`, `"`).Replace(body))
	if err != nil {
		t.Fatalf("pattern does not compile: %v", err)
	}
	return re
}

// assertMatches checks each match is redacted by applying every pattern in order and that
// each non-match survives untouched.
func assertMatches(t *testing.T, res []*regexp.Regexp, matches, nonMatches []string) {
	t.Helper()
	redact := func(s string) string {
		for _, re := range res {
			s = re.ReplaceAllString(s, "[REDACTED]")
		}
		return s
	}
	for _, s := range matches {
		if got := redact(s); !strings.Contains(got, "[REDACTED]") {
			t.Errorf("patterns should redact %q, got %q", s, got)
		}
	}
	for _, s := range nonMatches {
		if got := redact(s); got != s {
			t.Errorf("patterns should not touch %q, got %q", s, got)
		}
	}
}

// statementBody extracts the OTTL-escaped pattern literal from a redaction statement.
var statementBody = regexp.MustCompile(`"((?:[^"\\]|\\.)*)", "\[REDACTED\]"\)$`)

// assertStatements parses the statements YAML, checks its 4 contexts carry 8 statements (the live config's 8 shapes)
// per group with every field listing the same group patterns in the same order, then
// checks redaction through those patterns.
func assertStatements(t *testing.T, path string, wantGroups int, matches, nonMatches []string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got []statementContext
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatalf("statements YAML does not parse: %v", err)
	}
	contexts := make([]string, len(got))
	total := 0
	var groupBodies []string
	for i, c := range got {
		contexts[i] = c.Context
		total += len(c.Statements)
		for j, st := range c.Statements {
			m := statementBody.FindStringSubmatch(st)
			if m == nil {
				t.Fatalf("statement %q has no pattern literal", st)
			}
			if i == 0 {
				groupBodies = append(groupBodies, m[1])
			} else if m[1] != groupBodies[j%len(groupBodies)] {
				t.Errorf("context %s statement %d uses a different pattern than group %d", c.Context, j, j%len(groupBodies))
			}
		}
	}
	if want := []string{"resource", "scope", "span", "spanevent"}; !reflect.DeepEqual(contexts, want) {
		t.Errorf("contexts = %v, want %v", contexts, want)
	}
	if len(groupBodies) != wantGroups || total != 8*wantGroups {
		t.Fatalf("got %d groups and %d statements, want %d and %d", len(groupBodies), total, wantGroups, 8*wantGroups)
	}
	res := make([]*regexp.Regexp, len(groupBodies))
	for i, b := range groupBodies {
		res[i] = compileBody(t, b)
	}
	assertMatches(t, res, matches, nonMatches)
}

func TestGroupValues_TableDriven(t *testing.T) {
	t.Parallel()
	// OTTL-escaped pattern sizes (hand-checked against sanitizer.ValuePattern):
	// alpha-secret-0001 = 41, beta-secret-0002 = 39, gamma-secret-03 = 36,
	// alpha+beta = 81, alpha+beta+gamma = 118, beta+gamma = 76.
	const (
		alpha = "alpha-secret-0001"
		beta  = "beta-secret-0002"
		gamma = "gamma-secret-03"
	)
	tests := []struct {
		name     string
		values   []string
		maxBytes int
		want     [][]string
	}{
		{name: "empty input", values: nil, maxBytes: 8192, want: [][]string{}},
		{name: "all fit in one group longest first", values: []string{gamma, alpha, beta}, maxBytes: 8192, want: [][]string{{alpha, beta, gamma}}},
		{name: "packs up to the exact budget", values: []string{gamma, beta, alpha}, maxBytes: 81, want: [][]string{{alpha, beta}, {gamma}}},
		{name: "one byte under budget splits", values: []string{gamma, beta, alpha}, maxBytes: 80, want: [][]string{{alpha}, {beta, gamma}}},
		{name: "oversized single value gets its own group", values: []string{gamma, alpha}, maxBytes: 40, want: [][]string{{alpha}, {gamma}}},
		{name: "superstring lands before its substring", values: []string{"token-1234", "prefix-token-1234-suffix"}, maxBytes: 1, want: [][]string{{"prefix-token-1234-suffix"}, {"token-1234"}}},
		{name: "equal lengths order lexically", values: []string{"wxyz-2", "abcd-1"}, maxBytes: 1, want: [][]string{{"abcd-1"}, {"wxyz-2"}}},
		{name: "duplicates and unregistered short values drop", values: []string{"xyz", alpha, " " + alpha + " ", ""}, maxBytes: 8192, want: [][]string{{alpha}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := groupValues(tc.values, tc.maxBytes); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("groupValues() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRenderStatements_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		patterns []string
		want     []statementContext
		wantErr  bool
	}{
		{
			name:     "one escaped pattern",
			patterns: []string{`a\\.b\"c`},
			want: []statementContext{
				{Context: "resource", Statements: []string{`replace_all_patterns(resource.attributes, "value", "a\\.b\"c", "[REDACTED]")`}},
				{Context: "scope", Statements: []string{
					`replace_all_patterns(scope.attributes, "value", "a\\.b\"c", "[REDACTED]")`,
					`replace_pattern(scope.name, "a\\.b\"c", "[REDACTED]")`,
				}},
				{Context: "span", Statements: []string{
					`replace_all_patterns(span.attributes, "value", "a\\.b\"c", "[REDACTED]")`,
					`replace_pattern(span.name, "a\\.b\"c", "[REDACTED]")`,
					`replace_pattern(span.status.message, "a\\.b\"c", "[REDACTED]")`,
				}},
				{Context: "spanevent", Statements: []string{
					`replace_pattern(spanevent.name, "a\\.b\"c", "[REDACTED]")`,
					`replace_all_patterns(spanevent.attributes, "value", "a\\.b\"c", "[REDACTED]")`,
				}},
			},
		},
		{
			name:     "groups stay in order per field",
			patterns: []string{"p1", "it's\n%s"},
			want: []statementContext{
				{Context: "resource", Statements: []string{
					`replace_all_patterns(resource.attributes, "value", "p1", "[REDACTED]")`,
					"replace_all_patterns(resource.attributes, \"value\", \"it's\n%s\", \"[REDACTED]\")",
				}},
				{Context: "scope", Statements: []string{
					`replace_all_patterns(scope.attributes, "value", "p1", "[REDACTED]")`,
					"replace_all_patterns(scope.attributes, \"value\", \"it's\n%s\", \"[REDACTED]\")",
					`replace_pattern(scope.name, "p1", "[REDACTED]")`,
					"replace_pattern(scope.name, \"it's\n%s\", \"[REDACTED]\")",
				}},
				{Context: "span", Statements: []string{
					`replace_all_patterns(span.attributes, "value", "p1", "[REDACTED]")`,
					"replace_all_patterns(span.attributes, \"value\", \"it's\n%s\", \"[REDACTED]\")",
					`replace_pattern(span.name, "p1", "[REDACTED]")`,
					"replace_pattern(span.name, \"it's\n%s\", \"[REDACTED]\")",
					`replace_pattern(span.status.message, "p1", "[REDACTED]")`,
					"replace_pattern(span.status.message, \"it's\n%s\", \"[REDACTED]\")",
				}},
				{Context: "spanevent", Statements: []string{
					`replace_pattern(spanevent.name, "p1", "[REDACTED]")`,
					"replace_pattern(spanevent.name, \"it's\n%s\", \"[REDACTED]\")",
					`replace_all_patterns(spanevent.attributes, "value", "p1", "[REDACTED]")`,
					"replace_all_patterns(spanevent.attributes, \"value\", \"it's\n%s\", \"[REDACTED]\")",
				}},
			},
		},
		{name: "invalid utf-8 refused", patterns: []string{"ok", "bad\xff"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc, err := renderStatements(tc.patterns)
			if (err != nil) != tc.wantErr {
				t.Fatalf("renderStatements() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			var got []statementContext
			if err := yaml.Unmarshal(doc, &got); err != nil {
				t.Fatalf("rendered YAML does not parse: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("statements = %q, want %q", got, tc.want)
			}
		})
	}
}
