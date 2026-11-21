package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRun_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		secrets     map[string]string
		extra       string
		withExtra   bool
		noOut       bool
		extraArgs   []string
		missingFile bool
		wantCode    int
		wantStdout  string
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
			args := buildArgs(t, root, tc.secrets, tc.extra, tc.withExtra, tc.missingFile, tc.noOut)
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
			if tc.wantCode == 0 {
				assertPattern(t, filepath.Join(root, "pattern"), tc.matches, tc.nonMatches)
			}
		})
	}
}

func buildArgs(t *testing.T, root string, secrets map[string]string, extra string, withExtra, missingFile, noOut bool) []string {
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
	pattern := strings.NewReplacer(`\\`, `\`, `\"`, `"`).Replace(string(body))
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("pattern does not compile: %v", err)
	}
	for _, s := range matches {
		if !re.MatchString(s) {
			t.Errorf("pattern should match %q", s)
		}
	}
	for _, s := range nonMatches {
		if re.MatchString(s) {
			t.Errorf("pattern should not match %q", s)
		}
	}
}
