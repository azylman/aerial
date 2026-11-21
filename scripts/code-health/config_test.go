package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const unsortedRoutesSource = `package pkg

import "net/http"

type S struct {
	mux *http.ServeMux
}

func (s *S) setupRoutes() {
	s.mux.HandleFunc("/z", nil)
	s.mux.HandleFunc("/a", nil)
}
`

const unsortedHandlerFieldsSource = `package pkg

import "net/http"

type Config struct {
	VoiceHandler http.Handler
	AudioHandler http.Handler
}

type S struct {
	mux *http.ServeMux
}

func (s *S) setupRoutes() {
	s.mux.HandleFunc("/z", nil)
	s.mux.HandleFunc("/a", nil)
}
`

const unsortedCompositeHandlersSource = `package pkg

import "net/http"

type Config struct {
	AudioHandler http.Handler
	VoiceHandler http.Handler
}

func initServer() {
	_ = Config{
		VoiceHandler: nil,
		AudioHandler: nil,
	}
}
`

const nonMatchingOrderingSource = `package pkg

import "net/http"

type Mixed struct {
	int
	Name string
	AudioHandler http.Handler
}

type S struct {
	mux *http.ServeMux
}

func noop() {}

func (s *S) setupRoutes() {
	var x int
	_ = x
	noop()
	s.mux.HandleFunc("/a", nil)
	s.mux.Handle("/b", nil)
}

func init() {
	_ = []any{123, "text"}
	_ = map[string]string{"foo": "bar"}
}
`

const hygieneTestSource = "package pkg\n\nimport (\n\t\"net/http\"\n\t\"time\"\n)\n\n" +
	"func useClient() {\n\t_ = http.DefaultClient\n\t_ = &http.Client{}\n\ttime.Sleep(0)\n}\n"

const prefixedClosureTestSource = "package pkg\n\ntype Case struct {\n\tcheckBody func()\n\tchecker func()\n}\n"

func setupConfig(t *testing.T, dir, configJSON string) string {
	t.Helper()
	p := filepath.Join(dir, "code-health-config.json")
	if configJSON == "missing" {
		return p
	}
	if err := os.WriteFile(p, []byte(configJSON), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

func verifyFileContains(t *testing.T, dir string, want map[string]string) {
	t.Helper()
	for rel, snippet := range want {
		data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if !strings.Contains(string(data), snippet) {
			t.Errorf("%s does not contain %q:\n%s", rel, snippet, data)
		}
	}
}

func testRunConfigCases() []testRunCase {
	return []testRunCase{
		{name: "missing config file fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: "missing", wantCode: 1, wantErr: "code-health: load config:"},
		{name: "malformed config json fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: "{bad", wantCode: 1, wantErr: "load config"},
		{name: "unknown config key fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: `{"max_lines":10}`, wantCode: 1, wantErr: `unknown field "max_lines"`},
		{name: "unknown test rule fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: `{"disabled_test_rules":["no-such-rule"]}`, wantCode: 1, wantErr: `unknown test rule "no-such-rule"`},
		{name: "negative threshold fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: `{"max_depth":-1}`, wantCode: 1, wantErr: "thresholds must not be negative"},
		{name: "parent skipped dir fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: `{"skipped":["../x"]}`, wantCode: 1, wantErr: `invalid skipped directory "../x"`},
		{name: "root generated dir fails", files: map[string]string{"pkg/a.go": "package pkg\n"}, configJSON: `{"generated":["/"]}`, wantCode: 1, wantErr: `invalid skipped directory "/"`},
		{name: "empty config keeps aerial defaults", files: map[string]string{"pkg/big.go": buildLargeGoSource(1505, 155)}, configJSON: `{}`, wantCode: 1, wantErr: "file length 1505 exceeds limit 1500 (ceiling 1500)"},
		{name: "raised file and depth thresholds pass", files: map[string]string{"pkg/big.go": buildLargeGoSource(1505, 155), "pkg/complex.go": complexAndDeepSource}, configJSON: `{"max_file_lines":2000,"max_depth":20}`, wantCode: 0, wantOut: "Checked 2 Go file(s)"},
		{name: "lowered file threshold fails", files: map[string]string{"pkg/a.go": "package pkg\n\nfunc A() {}\n"}, configJSON: `{"max_file_lines":2}`, wantCode: 1, wantErr: "file length 3 exceeds limit 2 (ceiling 2)"},
		{name: "update mode honours configured threshold", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/mid.go": buildLargeGoSource(120, 0)}, configJSON: `{"max_file_lines":100}`, customArgs: []string{"-update"}, wantBaselineFiles: map[string]int{"pkg/mid.go": 120}, wantCode: 0, wantOut: "(1 files, 0 functions)"},
		{name: "empty banned suffix list allows catch-all filename", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/foo_extra_test.go": "package pkg\n"}, configJSON: `{"banned_test_filename_suffixes":[]}`, wantCode: 0, wantOut: "Checked 2 Go file(s)"},
		{name: "custom banned suffix list replaces defaults", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/foo_misc_test.go": "package pkg\n"}, configJSON: `{"banned_test_filename_suffixes":["_misc_test.go"]}`, wantCode: 1, wantErr: "pkg/foo_misc_test.go: test filename uses banned catch-all suffix"},
		{name: "default rules flag http client and sleep in tests", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/h_test.go": hygieneTestSource}, wantCode: 1, wantErr: "3 ratchet violation(s) detected"},
		{name: "disabled http rules keep sleep rule", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/h_test.go": hygieneTestSource}, configJSON: `{"disabled_test_rules":["http-default-client","http-client-literal"]}`, wantCode: 1, wantErr: "1 ratchet violation(s) detected:\n   • pkg/h_test.go:11: time.Sleep in tests is prohibited"},
		{name: "all hygiene rules disabled passes", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/h_test.go": hygieneTestSource}, configJSON: `{"disabled_test_rules":["http-default-client","http-client-literal","time-sleep","syscall-kill","reserved-port"]}`, wantCode: 0, wantOut: "Checked 2 Go file(s)"},
		{name: "prefixed closure fields allowed by default", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/c_test.go": prefixedClosureTestSource}, wantCode: 0, wantOut: "Checked 2 Go file(s)"},
		{name: "configured closure prefix bans camel case field only", files: map[string]string{"pkg/ok.go": "package pkg\n", "pkg/c_test.go": prefixedClosureTestSource}, configJSON: `{"banned_closure_field_prefixes":["check"]}`, wantCode: 1, wantErr: "1 ratchet violation(s) detected:\n   • pkg/c_test.go:4: test struct field \"checkBody\" uses a banned closure func type"},
		{name: "generated and skipped dirs both excluded", files: map[string]string{"pkg/ok.go": "package pkg\n", "gen/big.go": buildLargeGoSource(1600, 0), "web/out/big_test.go": "package out\n\nfunc TestA_Extra() {}\n", "web/outer.go": "package web\n"}, configJSON: `{"generated":["gen"],"skipped":["/web/out/"]}`, wantCode: 0, wantOut: "Checked 2 Go file(s)"},
		{name: "unsorted routes ignored without ordering check", files: map[string]string{"pkg/server.go": unsortedRoutesSource}, wantCode: 0, wantOut: "Checked 1 Go file(s)"},
		{name: "unsorted routes detected when ordering check enabled", files: map[string]string{"pkg/server.go": unsortedRoutesSource}, configJSON: `{"check_ordering":true}`, wantCode: 1, wantErr: "pkg/server.go:10: contiguous route statement run is unsorted"},
		{name: "fix sorts routes and handler fields", files: map[string]string{"pkg/server.go": unsortedHandlerFieldsSource}, customArgs: []string{"-fix"}, wantFileContains: map[string]string{"pkg/server.go": "AudioHandler http.Handler\n\tVoiceHandler http.Handler"}, wantCode: 0, wantOut: "Checked 1 Go file(s)"},
		{name: "fix sorts composite literal handlers", files: map[string]string{"pkg/server.go": unsortedCompositeHandlersSource}, customArgs: []string{"-fix"}, wantFileContains: map[string]string{"pkg/server.go": "AudioHandler: nil,\n\t\tVoiceHandler: nil,"}, wantCode: 0, wantOut: "Checked 1 Go file(s)"},
		{name: "ordering check ignores non-matching constructs", files: map[string]string{"pkg/server.go": nonMatchingOrderingSource, "pkg/gen.go": "// Code generated by tool; DO NOT EDIT.\npackage pkg\n"}, configJSON: `{"check_ordering":true}`, wantCode: 0, wantOut: "Checked 2 Go file(s)"},
		{name: "ordering check fails on syntax error", files: map[string]string{"pkg/broken.go": "package pkg\nfunc ("}, configJSON: `{"check_ordering":true}`, wantCode: 1, wantErr: "code-health: check:"},
	}
}
