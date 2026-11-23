package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

type parseCLIArgsCase struct {
	name             string
	args             []string
	envEriURL        string
	envTranscriptURL string
	wantCmd          string
	wantQuery        string
	wantMode         string
	wantTool         string
	wantSession      string
	wantLimit        int
	wantStep         int
	wantContext      int
	wantJSON         bool
	wantStack        string
	wantURL          string
	wantErrSub       string
	wantErrIs        error
}

func assertParseCLIArgsCase(t *testing.T, tc parseCLIArgsCase) {
	t.Helper()
	t.Setenv("TRANSCRIPTS_URL", tc.envTranscriptURL)
	t.Setenv("ERI_URL", tc.envEriURL)
	t.Setenv("AERIAL_ERI_URL", "")
	t.Setenv("AERIAL_BRAIN_URL", "")
	cfg, err := parseCLIArgs(tc.args)
	if tc.wantErrIs != nil {
		if !errors.Is(err, tc.wantErrIs) {
			t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
		}
		return
	}
	if tc.wantErrSub != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Command != tc.wantCmd || cfg.Query != tc.wantQuery || cfg.Mode != tc.wantMode ||
		cfg.Tool != tc.wantTool || cfg.Session != tc.wantSession || cfg.Limit != tc.wantLimit ||
		cfg.Step != tc.wantStep || cfg.Context != tc.wantContext || cfg.JSON != tc.wantJSON ||
		cfg.Stack != tc.wantStack || (tc.wantURL != "" && cfg.BaseURL != tc.wantURL) {
		t.Errorf("unexpected cfg: %+v, want cmd=%q query=%q mode=%q tool=%q session=%q limit=%d step=%d ctx=%d json=%v stack=%q url=%q",
			cfg, tc.wantCmd, tc.wantQuery, tc.wantMode, tc.wantTool, tc.wantSession, tc.wantLimit, tc.wantStep, tc.wantContext, tc.wantJSON, tc.wantStack, tc.wantURL)
	}
}

func parseCLIArgsTestCases() []parseCLIArgsCase {
	extra := parseCLIArgsExtraCases()
	cases := make([]parseCLIArgsCase, 0, 15+len(extra))
	cases = append(cases, []parseCLIArgsCase{
		{
			name:       "EmptyArgs",
			args:       []string{},
			wantErrSub: "no arguments provided",
		},
		{
			name:        "BareQuery",
			args:        []string{"docker compose"},
			wantCmd:     "search",
			wantQuery:   "docker compose",
			wantMode:    "auto",
			wantLimit:   10,
			wantStep:    -1,
			wantContext: 2,
		},
		{
			name:        "SubcommandStats",
			args:        []string{"stats", "--json", "--url", "http://test:8080"},
			wantCmd:     "stats",
			wantMode:    "auto",
			wantLimit:   10,
			wantStep:    -1,
			wantContext: 2,
			wantJSON:    true,
			wantURL:     "http://test:8080",
		},
		{
			name: "SubcommandSearchWithFlags",
			args: []string{
				"search",
				"--query", "memory leak",
				"--mode", "steps",
				"--tool", "shell",
				"--stack", "stack-b",
				"--session", "sess-123",
				"--limit", "25",
				"--json",
			},
			wantCmd:     "search",
			wantQuery:   "memory leak",
			wantMode:    "steps",
			wantTool:    "shell",
			wantStack:   "stack-b",
			wantSession: "sess-123",
			wantLimit:   25,
			wantStep:    -1,
			wantContext: 2,
			wantJSON:    true,
		},
		{
			name: "SubcommandGetWithFlags",
			args: []string{
				"get",
				"--session", "sess-123",
				"--step", "4",
				"--context", "5",
				"--stack=stack-a",
				"--json",
			},
			wantCmd:     "get",
			wantMode:    "auto",
			wantSession: "sess-123",
			wantStack:   "stack-a",
			wantLimit:   10,
			wantStep:    4,
			wantContext: 5,
			wantJSON:    true,
		},
		{
			name:        "SubcommandGetShorthandAndPositional",
			args:        []string{"get", "-s", "sess-abc", "-i", "2", "-C", "1"},
			wantCmd:     "get",
			wantMode:    "auto",
			wantSession: "sess-abc",
			wantLimit:   10,
			wantStep:    2,
			wantContext: 1,
		},
		{
			name:        "SubcommandGetPositionalSession",
			args:        []string{"get", "sess-xyz", "-i", "3"},
			wantCmd:     "get",
			wantMode:    "auto",
			wantSession: "sess-xyz",
			wantLimit:   10,
			wantStep:    3,
			wantContext: 2,
		},
		{
			name:       "SubcommandGetMissingSession",
			args:       []string{"get", "--step", "2"},
			wantErrSub: "session ID is required",
		},
		{
			name:       "SubcommandGetMissingStep",
			args:       []string{"get", "--session", "sess-1"},
			wantErrSub: "step index is required",
		},
		{
			name:       "SubcommandGetNegativeStep",
			args:       []string{"get", "--session", "sess-1", "--step", "-5"},
			wantErrSub: "step index is required and must be non-negative",
		},
	}...)
	return append(cases, extra...)
}

func parseCLIArgsExtraCases() []parseCLIArgsCase {
	return []parseCLIArgsCase{
		{
			name: "ShorthandFlagsAndPositional",
			args: []string{
				"search",
				"-m", "sessions",
				"-t", "view_file",
				"-s", "sess-abc",
				"-n", "5",
				"OOM error",
			},
			wantCmd:     "search",
			wantQuery:   "OOM error",
			wantMode:    "sessions",
			wantTool:    "view_file",
			wantSession: "sess-abc",
			wantLimit:   5,
			wantStep:    -1,
			wantContext: 2,
		},
		{
			name:        "FlagPrefixWithoutSubcommand",
			args:        []string{"-q", "quick test"},
			wantCmd:     "search",
			wantQuery:   "quick test",
			wantMode:    "auto",
			wantLimit:   10,
			wantStep:    -1,
			wantContext: 2,
		},
		{
			name:        "EriURLEnvOverride",
			args:        []string{"stats"},
			envEriURL:   "http://eri-host:9090/",
			wantCmd:     "stats",
			wantMode:    "auto",
			wantLimit:   10,
			wantStep:    -1,
			wantContext: 2,
			wantURL:     "http://eri-host:9090",
		},
		{
			name:             "TranscriptsURLPrecedenceOverEriURL",
			args:             []string{"stats"},
			envTranscriptURL: "http://transcripts-host:4060/",
			envEriURL:        "http://eri-host:9090/",
			wantCmd:          "stats",
			wantMode:         "auto",
			wantLimit:        10,
			wantStep:         -1,
			wantContext:      2,
			wantURL:          "http://transcripts-host:4060",
		},
		{
			name:       "InvalidFlag",
			args:       []string{"search", "--invalid-nonexistent-flag"},
			wantErrSub: "flag provided but not defined",
		},
		{
			name:      "HelpShortFlag",
			args:      []string{"-h"},
			wantErrIs: flag.ErrHelp,
		},
		{
			name:      "HelpLongFlag",
			args:      []string{"--help"},
			wantErrIs: flag.ErrHelp,
		},
		{
			name:        "BareHelpQuery",
			args:        []string{"help"},
			wantCmd:     "search",
			wantQuery:   "help",
			wantMode:    "auto",
			wantLimit:   10,
			wantStep:    -1,
			wantContext: 2,
		},
		{
			name:       "NegativeLimit",
			args:       []string{"search", "--limit", "-1", "query"},
			wantErrSub: "limit must be non-negative",
		},
		{
			name:       "NegativeContext",
			args:       []string{"search", "--context", "-1", "query"},
			wantErrSub: "context window size must be non-negative",
		},
	}
}

func TestParseCLIArgs(t *testing.T) {
	tests := parseCLIArgsTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertParseCLIArgsCase(t, tc)
		})
	}
}

func TestFormatSearchResults(t *testing.T) {
	longID := "11112222-3333-4444-5555-666677778888"
	tests := []struct {
		name        string
		res         transcript.SearchResult
		wantSubstrs []string
	}{
		{name: "empty matches", res: transcript.SearchResult{Query: "nothing", Mode: "auto"}, wantSubstrs: []string{"*(No matching records found)*"}},
		{
			name: "sessions and steps",
			res: transcript.SearchResult{
				Query: "quota limit",
				Mode:  "auto",
				Sessions: []db.SessionSummary{
					{Stack: "stack-a", Engine: "claude", SessionID: longID, ThreadID: "th-100", Summary: "Triaged quota exhaustion and tuned backoff."},
					{SessionID: "short", Summary: "Short ID test session"},
				},
				Steps: []db.TranscriptStep{
					{
						Stack:     "stack-b",
						Engine:    "agy",
						SessionID: longID,
						StepIndex: 5,
						ToolName:  "run_command",
						ToolKind:  "shell",
						Content:   "docker logs svc --tail 50\nline 2 of logs",
						CreatedAt: transcriptsFixedTime,
					},
					{SessionID: "short", StepIndex: 12, StepType: "PLANNER_RESPONSE", Content: strings.Repeat("a", 400), CreatedAt: transcriptsFixedTime},
				},
			},
			wantSubstrs: []string{
				"### 📄 Session Summaries (2 matches)",
				"[11112222...](conversation://11112222-3333-4444-5555-666677778888)",
				"### 💬 Transcript Execution Steps (2 matches)",
				"- [stack-a/claude] Session [11112222...]",
				"- [?/?] Session [short]",
				"- [stack-b/agy] **[run_command · shell]** Step #5",
				"- [?/?] **[PLANNER_RESPONSE]** Step #12",
				"... [truncated]",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := formatSearchResults(tc.res)
			for _, want := range tc.wantSubstrs {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q:\n%s", want, out)
				}
			}
		})
	}
}

func TestFormatStepWindow(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name        string
		res         transcript.StepWindowResult
		wantSubstrs []string
	}{
		{
			name: "empty steps",
			res: transcript.StepWindowResult{
				SessionID:  "sess-empty",
				CenterStep: 3,
				Context:    2,
				Steps:      nil,
			},
			wantSubstrs: []string{
				"Transcript Step Window for Session [sess-emp...](conversation://sess-empty)",
				"*(No matching steps found in range)*",
			},
		},
		{
			name: "single line and multiline steps with center marker",
			res: transcript.StepWindowResult{
				SessionID:  "12345678-abcd-ef01-2345-6789abcdef01",
				CenterStep: 5,
				Context:    2,
				Steps: []db.TranscriptStep{
					{
						SessionID: "12345678-abcd-ef01-2345-6789abcdef01",
						Stack:     "stack-a",
						Engine:    "agy",
						StepIndex: 4,
						ToolName:  "run_command",
						ToolKind:  "shell",
						Content:   "single line command",
						CreatedAt: now,
					},
					{
						SessionID: "12345678-abcd-ef01-2345-6789abcdef01",
						StepIndex: 5,
						StepType:  "tool_call",
						Content:   "line 1\nline 2",
						CreatedAt: now,
					},
				},
			},
			wantSubstrs: []string{
				"Transcript Step Window for Session [12345678...](conversation://12345678-abcd-ef01-2345-6789abcdef01)",
				"  [stack-a/agy] **[run_command · shell]** Step #4",
				"*Content:* single line command",
				"▶ [?/?] **[tool_call]** Step #5",
				"line 1\n  line 2",
			},
		},
		{
			name: "short session id and default step badge",
			res: transcript.StepWindowResult{
				SessionID:  "short",
				CenterStep: 1,
				Context:    1,
				Steps: []db.TranscriptStep{
					{
						SessionID: "short",
						StepIndex: 1,
						Content:   strings.Repeat("x", 700),
						CreatedAt: now,
					},
					{
						SessionID: "short",
						StepIndex: 2,
						Content:   strings.Repeat("y", 700),
						CreatedAt: now,
					},
				},
			},
			wantSubstrs: []string{
				"[short](conversation://short)",
				"▶ [?/?] **[STEP]** Step #1",
				strings.Repeat("x", 700),
				"... [truncated]",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := formatStepWindow(tc.res)
			for _, want := range tc.wantSubstrs {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q:\n%s", want, out)
				}
			}
		})
	}
}

var transcriptsFixedTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// transcriptsServers are the shared fakes for the HTTP tables: echo serves the three endpoints and echoes the
// forwarded query string back, the rest are fixed failure modes.
type transcriptsServers struct {
	echo, badJSON, status500, truncated, closed string
}

func newTranscriptsServers(t *testing.T) transcriptsServers {
	t.Helper()
	// Clients resolve their SA token from the env and /var/run/secrets; point them at an absent file instead.
	t.Setenv("AERIAL_SA_TOKEN_PATH", filepath.Join(t.TempDir(), "absent-token"))
	t.Setenv("TRANSCRIPTS_URL", "")
	t.Setenv("ERI_URL", "")
	serve := func(h http.HandlerFunc) string {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		return srv.URL
	}
	closedSrv := httptest.NewServer(http.NotFoundHandler())
	closedSrv.Close()
	return transcriptsServers{
		echo: serve(transcriptsEchoHandler),
		badJSON: serve(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("{invalid-json"))
		}),
		status500: serve(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "server error", http.StatusInternalServerError)
		}),
		truncated: serve(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "500")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("truncated"))
			if hijacker, ok := w.(http.Hijacker); ok {
				conn, _, _ := hijacker.Hijack()
				_ = conn.Close()
			}
		}),
		closed: closedSrv.URL,
	}
}

func transcriptsEchoHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var payload any
	switch r.URL.Path {
	case "/api/transcripts/search":
		payload = transcript.SearchResult{
			Query:    q.Get("q"),
			Mode:     q.Get("mode"),
			Sessions: []db.SessionSummary{{Stack: q.Get("stack"), Engine: "claude", SessionID: "sess-abc", Summary: r.URL.RawQuery}},
			Total:    1,
		}
	case "/api/transcripts/stats":
		stacks := []db.TranscriptStackStats{{Stack: "stack-a", Sessions: 5000, Steps: 90000}, {Stack: "stack-b", Sessions: 32, Steps: 400}}
		stats := statsResponse{Stacks: stacks, Status: "ok"}
		if q.Get("stack") == "stack-b" {
			stats.Stacks = stats.Stacks[1:]
		}
		for _, st := range stats.Stacks {
			stats.TotalSessions += st.Sessions
			stats.TotalSteps += st.Steps
		}
		payload = stats
	case "/api/transcripts/steps":
		step, _ := strconv.Atoi(q.Get("step"))
		window, _ := strconv.Atoi(q.Get("context"))
		payload = transcript.StepWindowResult{SessionID: q.Get("session"), CenterStep: step, Context: window, Steps: []db.TranscriptStep{
			{
				Stack:     q.Get("stack"),
				Engine:    "agy",
				SessionID: q.Get("session"),
				StepIndex: step,
				ToolName:  "run_command",
				ToolKind:  "shell",
				Content:   r.URL.RawQuery,
				CreatedAt: transcriptsFixedTime,
			},
		}}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func assertTranscriptsHTTPResult[T any](t *testing.T, got T, err error, want T, wantErr string) {
	t.Helper()
	if wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("error = %v, want containing %q", err, wantErr)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("result = %+v\nwant %+v", got, want)
	}
}

func TestExecuteSearchHTTP(t *testing.T) {
	srv := newTranscriptsServers(t)
	tests := []struct {
		name    string
		cfg     CLIConfig
		want    transcript.SearchResult
		wantErr string
	}{
		{
			name: "forwards every filter",
			cfg:  CLIConfig{BaseURL: srv.echo + "/", Query: "docker", Mode: "steps", Tool: "shell", Session: "s1", Stack: "stack-b", Limit: 5},
			want: transcript.SearchResult{Query: "docker", Mode: "steps", Total: 1, Sessions: []db.SessionSummary{
				{Stack: "stack-b", Engine: "claude", SessionID: "sess-abc", Summary: "limit=5&mode=steps&q=docker&session=s1&stack=stack-b&tool=shell"},
			}},
		},
		{
			name: "omits empty filters",
			cfg:  CLIConfig{BaseURL: srv.echo, Query: "docker", Mode: "auto", Limit: 10},
			want: transcript.SearchResult{Query: "docker", Mode: "auto", Total: 1, Sessions: []db.SessionSummary{
				{Engine: "claude", SessionID: "sess-abc", Summary: "limit=10&mode=auto&q=docker"},
			}},
		},
		{name: "invalid url", cfg: CLIConfig{BaseURL: "://bad-url", Query: "q"}, wantErr: "missing protocol scheme"},
		{name: "bad json", cfg: CLIConfig{BaseURL: srv.badJSON, Query: "q"}, wantErr: "failed unmarshaling response"},
		{name: "status 500", cfg: CLIConfig{BaseURL: srv.status500, Query: "q"}, wantErr: "HTTP 500: server error"},
		{name: "truncated body", cfg: CLIConfig{BaseURL: srv.truncated, Query: "q"}, wantErr: "unexpected EOF"},
		{name: "connection refused", cfg: CLIConfig{BaseURL: srv.closed, Query: "q"}, wantErr: "connection refused"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := executeSearchHTTP(context.Background(), tc.cfg)
			assertTranscriptsHTTPResult(t, got, err, tc.want, tc.wantErr)
		})
	}
}

func TestExecuteStatsHTTP(t *testing.T) {
	srv := newTranscriptsServers(t)
	stackA := db.TranscriptStackStats{Stack: "stack-a", Sessions: 5000, Steps: 90000}
	stackB := db.TranscriptStackStats{Stack: "stack-b", Sessions: 32, Steps: 400}
	tests := []struct {
		name    string
		cfg     CLIConfig
		want    statsResponse
		wantErr string
	}{
		{name: "all stacks", cfg: CLIConfig{BaseURL: srv.echo}, want: statsResponse{5032, 90400, []db.TranscriptStackStats{stackA, stackB}, "ok"}},
		{name: "stack filter", cfg: CLIConfig{BaseURL: srv.echo, Stack: "stack-b"}, want: statsResponse{32, 400, []db.TranscriptStackStats{stackB}, "ok"}},
		{name: "invalid url", cfg: CLIConfig{BaseURL: "://bad-url"}, wantErr: "missing protocol scheme"},
		{name: "bad json", cfg: CLIConfig{BaseURL: srv.badJSON}, wantErr: "failed unmarshaling response"},
		{name: "status 500", cfg: CLIConfig{BaseURL: srv.status500}, wantErr: "HTTP 500: server error"},
		{name: "truncated body", cfg: CLIConfig{BaseURL: srv.truncated}, wantErr: "unexpected EOF"},
		{name: "connection refused", cfg: CLIConfig{BaseURL: srv.closed}, wantErr: "connection refused"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := executeStatsHTTP(context.Background(), tc.cfg)
			assertTranscriptsHTTPResult(t, got, err, tc.want, tc.wantErr)
		})
	}
}

func TestExecuteGetHTTP(t *testing.T) {
	srv := newTranscriptsServers(t)
	tests := []struct {
		name    string
		cfg     CLIConfig
		want    transcript.StepWindowResult
		wantErr string
	}{
		{
			name: "forwards session step context and stack",
			cfg:  CLIConfig{BaseURL: srv.echo, Session: "sess-123", Step: 2, Context: 3, Stack: "stack-b"},
			want: transcript.StepWindowResult{SessionID: "sess-123", CenterStep: 2, Context: 3, Steps: []db.TranscriptStep{
				{
					Stack:     "stack-b",
					Engine:    "agy",
					SessionID: "sess-123",
					StepIndex: 2,
					ToolName:  "run_command",
					ToolKind:  "shell",
					Content:   "context=3&session=sess-123&stack=stack-b&step=2",
					CreatedAt: transcriptsFixedTime,
				},
			}},
		},
		{name: "invalid url", cfg: CLIConfig{BaseURL: "://bad-url", Session: "s"}, wantErr: "missing protocol scheme"},
		{name: "bad json", cfg: CLIConfig{BaseURL: srv.badJSON, Session: "s"}, wantErr: "failed unmarshaling response"},
		{name: "status 500", cfg: CLIConfig{BaseURL: srv.status500, Session: "s"}, wantErr: "HTTP 500: server error"},
		{name: "connection refused", cfg: CLIConfig{BaseURL: srv.closed, Session: "s"}, wantErr: "connection refused"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := executeGetHTTP(context.Background(), tc.cfg)
			assertTranscriptsHTTPResult(t, got, err, tc.want, tc.wantErr)
		})
	}
}

// transcriptsWriter is the stdout fake: it records writes, or fails every write with err when set.
type transcriptsWriter struct {
	err error
	buf bytes.Buffer
}

func (w *transcriptsWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.buf.Write(p)
}

func TestWriteCLIOutput(t *testing.T) {
	errDiskFull := errors.New("simulated disk full")
	errMarshal := errors.New("simulated marshal error")
	tests := []struct {
		name       string
		asJSON     bool
		marshalErr error
		writeErr   error
		want       string
		wantErr    string
	}{
		{name: "text", want: "formatted"},
		{name: "json", asJSON: true, want: "{\n  \"k\": \"v\"\n}\n"},
		{name: "json marshal error", asJSON: true, marshalErr: errMarshal, wantErr: "simulated marshal error"},
		{name: "text write error", writeErr: errDiskFull, wantErr: "simulated disk full"},
		{name: "json write error", asJSON: true, writeErr: errDiskFull, wantErr: "simulated disk full"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orig := jsonMarshalIndent
			t.Cleanup(func() { jsonMarshalIndent = orig })
			jsonMarshalIndent = func(v any, prefix, indent string) ([]byte, error) {
				if tc.marshalErr != nil {
					return nil, tc.marshalErr
				}
				return orig(v, prefix, indent)
			}
			out := &transcriptsWriter{err: tc.writeErr}
			err := writeCLIOutput(out, tc.asJSON, map[string]string{"k": "v"}, "formatted")
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := out.buf.String(); got != tc.want {
				t.Errorf("output = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunMain(t *testing.T) {
	srv := newTranscriptsServers(t)
	errDiskFull := errors.New("simulated disk full")
	statsText := "📊 Aerial Transcript Index Stats:\n- Total Indexed Sessions: 5032\n- Total Indexed Steps: 90400\n" +
		"  - [stack-a] 5000 sessions, 90000 steps\n  - [stack-b] 32 sessions, 400 steps\n- Status: ok\n"
	getArgs := []string{"get", "--session", "sess-123", "--step", "2"}
	tests := []struct {
		name       string
		args       []string
		url        string
		writeErr   error
		wantCode   int
		wantOut    []string
		wantStderr string
	}{
		{name: "parse error", args: []string{"--invalid-flag"}, wantCode: 1, wantStderr: "Error: flag provided but not defined: -invalid-flag\n"},
		{
			name:       "search empty query",
			args:       []string{"search"},
			url:        srv.echo,
			wantCode:   1,
			wantStderr: "Error: search query cannot be empty (usage: transcript-search search <query>)\n",
		},
		{
			name:    "search text",
			args:    []string{"search", "docker"},
			url:     srv.echo,
			wantOut: []string{"🔍 Search Results for: `docker` (mode: auto)", "- [?/claude] Session [sess-abc]"},
		},
		{
			name:    "search stack and tool filters",
			args:    []string{"search", "docker", "--stack", "stack-b", "-t", "run_command", "-s", "s1"},
			url:     srv.echo,
			wantOut: []string{"- [stack-b/claude] Session [sess-abc]", "limit=10&mode=auto&q=docker&session=s1&stack=stack-b&tool=run_command"},
		},
		{
			name:    "search json",
			args:    []string{"search", "docker", "--json"},
			url:     srv.echo,
			wantOut: []string{`"query": "docker"`, `"session_id": "sess-abc"`},
		},
		{
			name:       "search http error",
			args:       []string{"search", "docker"},
			url:        srv.status500,
			wantCode:   1,
			wantStderr: "Error: search failed: HTTP 500: server error\n\n",
		},
		{
			name:       "search write error",
			args:       []string{"search", "docker"},
			url:        srv.echo,
			writeErr:   errDiskFull,
			wantCode:   1,
			wantStderr: "Error: simulated disk full\n",
		},
		{
			name:    "get text",
			args:    getArgs,
			url:     srv.echo,
			wantOut: []string{"▶ [?/agy] **[run_command · shell]** Step #2 (2026-01-02 03:04:05)", "context=2&session=sess-123&step=2"},
		},
		{
			name:    "get stack filter",
			args:    []string{"get", "--session", "sess-123", "--step", "2", "--stack", "stack-b"},
			url:     srv.echo,
			wantOut: []string{"[stack-b/agy] **[run_command · shell]** Step #2"},
		},
		{
			name:    "get json",
			args:    []string{"get", "--session", "sess-123", "--step", "2", "--json"},
			url:     srv.echo,
			wantOut: []string{`"session_id": "sess-123"`, `"step": 2`},
		},
		{name: "get http error", args: getArgs, url: srv.status500, wantCode: 1, wantStderr: "Error: get failed: HTTP 500: server error\n\n"},
		{name: "get write error", args: getArgs, url: srv.echo, writeErr: errDiskFull, wantCode: 1, wantStderr: "Error: simulated disk full\n"},
		{name: "stats text", args: []string{"stats"}, url: srv.echo, wantOut: []string{statsText}},
		{
			name:    "stats stack filter",
			args:    []string{"stats", "--stack", "stack-b"},
			url:     srv.echo,
			wantOut: []string{"Total Indexed Sessions: 32\n- Total Indexed Steps: 400\n  - [stack-b] 32 sessions, 400 steps\n- Status: ok\n"},
		},
		{name: "stats json", args: []string{"stats", "--json"}, url: srv.echo, wantOut: []string{`"total_sessions": 5032`}},
		{
			name:       "stats http error",
			args:       []string{"stats"},
			url:        srv.status500,
			wantCode:   1,
			wantStderr: "Error: failed fetching stats: HTTP 500: server error\n\n",
		},
		{name: "stats write error", args: []string{"stats"}, url: srv.echo, writeErr: errDiskFull, wantCode: 1, wantStderr: "Error: simulated disk full\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.args
			if tc.url != "" {
				args = append(append([]string{}, tc.args...), "--url", tc.url)
			}
			out := &transcriptsWriter{err: tc.writeErr}
			var errOut bytes.Buffer
			if code := runMain(args, out, &errOut); code != tc.wantCode {
				t.Errorf("exit code = %d, want %d", code, tc.wantCode)
			}
			if got := errOut.String(); got != tc.wantStderr {
				t.Errorf("stderr = %q, want %q", got, tc.wantStderr)
			}
			for _, want := range tc.wantOut {
				if !strings.Contains(out.buf.String(), want) {
					t.Errorf("stdout missing %q:\n%s", want, out.buf.String())
				}
			}
		})
	}
}

func TestClient_GetJSON(t *testing.T) {
	t.Parallel()
	type testResponse struct {
		Message string `json:"message"`
	}

	tests := []struct {
		name       string
		token      string
		tokenPath  string
		status     int
		body       string
		wantAuth   string
		wantErrSub string
	}{
		{
			name:     "attaches bearer token from file",
			token:    "secret-agent-token",
			status:   http.StatusOK,
			body:     `{"message":"ok"}`,
			wantAuth: "Bearer secret-agent-token",
		},
		{name: "skips auth when token file is missing", tokenPath: "/nonexistent/path/to/token", status: http.StatusOK, body: `{"message":"ok"}`},
		{name: "skips auth when token file is empty", token: "   \n  \t  ", status: http.StatusOK, body: `{"message":"ok"}`},
		{
			name:       "returns error on http 403 status",
			token:      "invalid-token",
			status:     http.StatusForbidden,
			body:       "forbidden",
			wantErrSub: "HTTP 403: forbidden",
		},
		{name: "returns error on invalid json", status: http.StatusOK, body: "{invalid-json", wantErrSub: "failed unmarshaling response"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedAuth string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedAuth = r.Header.Get("Authorization")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			tokenPath := tc.tokenPath
			if tokenPath == "" {
				dir := t.TempDir()
				tokenPath = filepath.Join(dir, "sa-token")
				if tc.token != "" {
					if err := os.WriteFile(tokenPath, []byte(tc.token), 0o600); err != nil {
						t.Fatalf("failed writing token file: %v", err)
					}
				}
			}

			c := &client{
				BaseURL:    server.URL,
				TokenPath:  tokenPath,
				HTTPClient: server.Client(),
			}

			var resp testResponse
			err := c.getJSON(context.Background(), server.URL+"/test", &resp)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if receivedAuth != tc.wantAuth {
				t.Errorf("Authorization header = %q, want %q", receivedAuth, tc.wantAuth)
			}
			if resp.Message != "ok" {
				t.Errorf("expected response message 'ok', got %q", resp.Message)
			}
		})
	}
}

func writeTestToken(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte("tok"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestResolveSATokenPath(t *testing.T) {
	tests := []struct {
		name         string
		envTokenPath string
		hasDefault   bool
		hasHome      bool
		wantHome     bool
		wantExact    string
	}{
		{
			name:         "returns explicit AERIAL_SA_TOKEN_PATH when set",
			envTokenPath: "/custom/sa/token",
			wantExact:    "/custom/sa/token",
		},
		{
			name:       "prefers readable default path when both default and home token exist",
			hasDefault: true,
			hasHome:    true,
		},
		{
			name:     "falls back to HOME turn_env sa-token when default unreadable",
			hasHome:  true,
			wantHome: true,
		},
		{
			name: "falls back to defaultSATokenPath when neither readable",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origDefault := defaultSATokenPath
			defer func() { defaultSATokenPath = origDefault }()

			t.Setenv("AERIAL_SA_TOKEN_PATH", tc.envTokenPath)
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)

			defaultSATokenPath = filepath.Join(t.TempDir(), "default-token")
			if tc.hasDefault {
				writeTestToken(t, defaultSATokenPath)
			}

			homeTokPath := filepath.Join(homeDir, ".gemini", "turn_env", "sa-token")
			if tc.hasHome {
				writeTestToken(t, homeTokPath)
			}

			got := resolveSATokenPath()
			want := defaultSATokenPath
			switch {
			case tc.wantExact != "":
				want = tc.wantExact
			case tc.wantHome:
				want = homeTokPath
			}
			if got != want {
				t.Errorf("resolveSATokenPath() = %q, want %q", got, want)
			}
		})
	}
}
