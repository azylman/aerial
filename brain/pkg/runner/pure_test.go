package runner

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestBuildAgyArgs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input    AgyArgsInput
		contains []string
		omits    []string
	}{
		{AgyArgsInput{}, []string{"--dangerously-skip-permissions", "--output-format stream-json"}, []string{"--input-format", "--model", "--conversation"}},
		{AgyArgsInput{OutputFormat: "json", Model: "gemini-2.5-flash", SessionID: "11111111-2222-3333-4444-555555555555"}, []string{"--dangerously-skip-permissions", "--output-format json", "--model gemini-2.5-flash", "--conversation 11111111-2222-3333-4444-555555555555"}, []string{"--input-format"}},
		{AgyArgsInput{WorkerMode: true, OutputFormat: "stream-json", Model: "gemini-pro", SessionID: "22222222-3333-4444-5555-666666666666"}, []string{"--dangerously-skip-permissions", "--input-format stream-json", "--output-format stream-json", "--model gemini-pro", "--conversation 22222222-3333-4444-5555-666666666666"}, nil},
	} {
		args := BuildAgyArgs(tc.input)
		joined := strings.Join(args, " ")
		for _, c := range tc.contains {
			if !strings.Contains(joined, c) {
				t.Errorf("BuildAgyArgs(%+v) missing %q in %v", tc.input, c, args)
			}
		}
		for _, o := range tc.omits {
			if strings.Contains(joined, o) {
				t.Errorf("BuildAgyArgs(%+v) should omit %q in %v", tc.input, o, args)
			}
		}
	}
}

func TestBuildDaemonArgs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		cfg      DaemonConfig
		contains []string
		omits    []string
	}{
		{DaemonConfig{SessionID: "sess-abc", Model: "gemini-test"}, []string{"--dangerously-skip-permissions", "--input-format stream-json", "--output-format stream-json", "--conversation sess-abc", "--model gemini-test", "--print-timeout 60m"}, nil},
		{DaemonConfig{Timeout: 15 * time.Minute}, []string{"--print-timeout 15m"}, []string{"--conversation", "--model"}},
		{DaemonConfig{Timeout: 45 * time.Second}, []string{"--print-timeout 45s"}, nil},
	} {
		args := BuildDaemonArgs(tc.cfg)
		joined := strings.Join(args, " ")
		for _, c := range tc.contains {
			if !strings.Contains(joined, c) {
				t.Errorf("BuildDaemonArgs(%+v) missing %q in %v", tc.cfg, c, args)
			}
		}
		for _, o := range tc.omits {
			if strings.Contains(joined, o) {
				t.Errorf("BuildDaemonArgs(%+v) should omit %q in %v", tc.cfg, o, args)
			}
		}
	}
}

func TestBuildAgyEnv(t *testing.T) {
	t.Parallel()
	filt := FilterAgyBaseEnv([]string{"FOO=bar", "GEMINI_API_KEY=secret1", "gemini_api_key=secret2", "ANTIGRAVITY_API_KEY=s3", "GOOGLE_GENAI_API_KEY=s4", "MALFORMED_NO_EQUALS", "PATH=/usr/bin"})
	if fj := strings.Join(filt, "\n"); strings.Contains(fj, "secret") || !strings.Contains(fj, "FOO=bar") || !strings.Contains(fj, "MALFORMED_NO_EQUALS") {
		t.Errorf("FilterAgyBaseEnv leak or drop: %v", filt)
	}

	for _, tc := range []struct {
		input       AgyEnvInput
		contains    []string
		notContains []string
	}{
		{AgyEnvInput{}, []string{"GIT_TERMINAL_PROMPT=0", "AGY_LOG_LEVEL=debug", "ANTIGRAVITY_LOG_LEVEL=debug"}, nil},
		{
			AgyEnvInput{
				BaseEnv:  []string{"CUSTOM_BASE=1", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "PATH=/bin", "HOME=/root", "USERPROFILE=C:\\root", "GEMINI_CLI_HOME=/gemini"},
				HomeDir:  "/custom/home",
				APIKey:   "secret-token-123",
				TargetID: "target-agent",
				Timezone: "America/Los_Angeles",
				ExtraEnv: []string{"EXTRA_KEY=foo"},
			},
			[]string{"CUSTOM_BASE=1", "GIT_TERMINAL_PROMPT=0", "AGY_LOG_LEVEL=debug", "ANTIGRAVITY_LOG_LEVEL=debug", "HOME=/custom/home", "USERPROFILE=/custom/home", "GEMINI_CLI_HOME=/custom/home", "GEMINI_API_KEY=secret-token-123", "ANTIGRAVITY_API_KEY=secret-token-123", "GOOGLE_GENAI_API_KEY=secret-token-123", "AERIAL_TARGET_ID=target-agent", "TZ=America/Los_Angeles", "EXTRA_KEY=foo"},
			[]string{"AERIAL_VANADIS_CHUNK_ID=stale-chunk", "HOME=/root", "USERPROFILE=C:\\root", "GEMINI_CLI_HOME=/gemini"},
		},
		{
			AgyEnvInput{
				BaseEnv: []string{"CUSTOM_BASE=1", "GEMINI_API_KEY=ambient-key", "ANTIGRAVITY_API_KEY=ambient-key", "GOOGLE_GENAI_API_KEY=ambient-key", "gemini_api_key=ambient2", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "VANADIS_CHUNK_ID=stale-legacy-chunk", "MALFORMED_NO_EQUALS", "OTHER_ENV=kept"},
				APIKey:  "",
			},
			[]string{"CUSTOM_BASE=1", "OTHER_ENV=kept", "MALFORMED_NO_EQUALS"},
			[]string{"ambient-key", "ambient2", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "VANADIS_CHUNK_ID=stale-legacy-chunk"},
		},
	} {
		env := BuildAgyEnv(tc.input)
		joined := strings.Join(env, "\n")
		for _, c := range tc.contains {
			if !strings.Contains(joined, c) {
				t.Errorf("BuildAgyEnv missing %q in %v", c, env)
			}
		}
		for _, nc := range tc.notContains {
			if strings.Contains(joined, nc) {
				t.Errorf("BuildAgyEnv unexpected %q in %v", nc, env)
			}
		}
	}
}

func TestShouldRotateWorker(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		used, budget int
		rss, maxRSS  uint64
		dead, want   bool
		reason       string
	}{
		{3, 10, 100 << 20, 500 << 20, false, false, ""},
		{1, 10, 0, 500 << 20, true, true, "worker is dead"},
		{1, 10, 600 << 20, 500 << 20, false, true, "rss limit exceeded"},
		{10, 10, 100 << 20, 500 << 20, false, true, "turn budget reached"},
		{100, 0, 0, 0, false, false, ""},
	} {
		rot, reason := ShouldRotateWorker(tc.used, tc.budget, tc.rss, tc.maxRSS, tc.dead)
		if rot != tc.want || (tc.reason != "" && !strings.Contains(reason, tc.reason)) {
			t.Errorf("ShouldRotateWorker(%d,%d,%d,%d,%v) = (%v,%q), want (%v,%q)", tc.used, tc.budget, tc.rss, tc.maxRSS, tc.dead, rot, reason, tc.want, tc.reason)
		}
	}
}

func TestParseInitEvent(t *testing.T) {
	t.Parallel()
	vID := "11111111-2222-3333-4444-555555555555"
	for _, tc := range []struct {
		line, wantID string
		wantOk       bool
	}{
		{`{"event":"init","conversation_id":"` + vID + `"}`, vID, true},
		{`log prefix {"event":"init","conversation_id":"` + vID + `"}`, vID, true},
		{`{"event":"result","status":"SUCCESS"}`, "", false},
		{`{"event":"init","conversation_id":"not-a-valid-uuid"}`, "", false},
		{"", "", false},
	} {
		if id, ok := ParseInitEvent(tc.line); ok != tc.wantOk || id != tc.wantID {
			t.Errorf("ParseInitEvent(%q) = (%q, %v), want (%q, %v)", tc.line, id, ok, tc.wantID, tc.wantOk)
		}
	}
}

func TestIsResultEvent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		line string
		want bool
	}{
		{`{"event":"result","result":{"status":"SUCCESS"}}`, true},
		{`{"event":"result"}`, true},
		{`{"event":"init","conversation_id":"123"}`, false},
		{`2026/09/12 12:00:00 Starting server...`, false},
		{`[2026-09-22 13:00:00] {"event":"result","result":{"status":"SUCCESS"}}`, true},
		{`{"event":"step_update","tool_output":"task finished with result: OK"}`, false},
		{`{"event":"step_update","tool_output":"{\"event\":\"result\"}"}`, false},
		{`prefix log {"event":"result"} suffix`, true},
		{"prefix log {not valid json result", false},
		{"", false},
		{"   ", false},
	} {
		if got := IsResultEvent(tc.line); got != tc.want {
			t.Errorf("IsResultEvent(%q) = %v, want %v", tc.line, got, tc.want)
		}
	}
}

type errWriter struct{ err error }

func (e *errWriter) Write(p []byte) (int, error) { return 0, e.err }

func TestWriteWorkerTurn(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	for _, tc := range []struct {
		w       io.Writer
		prompt  string
		wantErr string
		wantSub []string
	}{
		{nil, "hello", "writer is nil", nil},
		{&buf, "hello world", "", []string{`"event":"user"`, "hello world", "\n"}},
		{&errWriter{err: errors.New("disk full")}, "hello", "disk full", nil},
	} {
		buf.Reset()
		err := WriteWorkerTurn(tc.w, tc.prompt)
		if (tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr))) || (tc.wantErr == "" && err != nil) {
			t.Errorf("WriteWorkerTurn(%v, %q) err = %v, wantErr %q", tc.w, tc.prompt, err, tc.wantErr)
		}
		for _, s := range tc.wantSub {
			if !strings.Contains(buf.String(), s) {
				t.Errorf("expected %q in %q", s, buf.String())
			}
		}
	}
}

func TestExtractSubagentID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{`{"conversationId": "sub-123"}`, "sub-123"},
		{`{"conversation_id": "sub-456"}`, "sub-456"},
		{`[{"conversationId": "sub-789"}]`, "sub-789"},
		{`[{"conversation_id": "sub-array-snake"}]`, "sub-array-snake"},
		{`Subagent conversation started with conversation ID: sub-777, {"conversationId": "sub-777"}`, "sub-777"},
		{`{"subagents": [{"conversationId": "sub-nested-1"}]}`, "sub-nested-1"},
		{`[2026-09-19 {worker-1}] {"conversationId": "sub-logger-id"}`, "sub-logger-id"},
		{"", ""},
		{"Command completed with status 0", ""},
		{`log prefix [{"conversation_id": "sub-alt-1"}]`, "sub-alt-1"},
		{`{"conversation_id": "sub-snake-1"}`, "sub-snake-1"},
		{`{"subagents": [{"conversation_id": "sub-snake-sub"}]}`, "sub-snake-sub"},
	} {
		if got := extractSubagentID(tc.in); got != tc.want {
			t.Errorf("extractSubagentID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestActivityWriter_SessionProbe(t *testing.T) {
	w := NewActivityWriter("")
	if n, err := w.Write(nil); n != 0 || err != nil {
		t.Fatalf("expected 0, nil from empty write")
	}
	jsonPayload := []byte(`some log prefix {"session_id": "123e4567-e89b-12d3-a456-426614174000"} some suffix`)
	if n, err := w.Write(jsonPayload); err != nil || n != len(jsonPayload) {
		t.Fatalf("write failed: %v", err)
	}
	if w.SessionID() != "123e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("expected session ID, got %q", w.SessionID())
	}
	w2 := NewActivityWriter("")
	_, _ = w2.Write([]byte("Starting conversation update stream for 223e4567-e89b-12d3-a456-426614174000\n"))
	if w2.SessionID() != "223e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("expected update stream session ID, got %q", w2.SessionID())
	}
}

func TestDefaultWatchdogOptions(t *testing.T) {
	opts := DefaultWatchdogOptions(5)
	if opts.InactivityTimeout == 0 || opts.MaxDuration == 0 {
		t.Errorf("expected non-zero watchdog options")
	}
}

func TestCleanUnescapedString(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`"github"`, "github"},
		{`"\"github\""`, "github"},
		{`"\"nested\""`, "nested"},
		{`\hello\`, "hello"},
		{`  "unifi"  `, "unifi"},
		{"plain", "plain"},
		{`""`, ""},
		{"", ""},
		{`"a"`, "a"},
		{`"`, `"`},
	} {
		if got := CleanUnescapedString(tc.in); got != tc.want {
			t.Errorf("CleanUnescapedString(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestExtractMCPToolInfo(t *testing.T) {
	for _, tc := range []struct {
		toolName   string
		params     map[string]any
		wantTool   string
		wantServer string
	}{
		{"", nil, "unknown", "native"},
		{"run_command", map[string]any{"CommandLine": "ls"}, "run_command", "native"},
		{"call_mcp_tool", map[string]any{"ServerName": "github", "ToolName": "list_pull_requests"}, "list_pull_requests", "github"},
		{"call_mcp_tool", map[string]any{"ServerName": `"\"unifi\""`, "ToolName": `"\"unifi_execute\""`}, "unifi_execute", "unifi"},
		{"call_mcp_tool", nil, "call_mcp_tool", "unknown"},
		{"call_mcp_tool", map[string]any{"ServerName": "docker"}, "call_mcp_tool", "docker"},
		{"call_mcp_tool", map[string]any{"ToolName": "list_containers"}, "list_containers", "unknown"},
		{"call_mcp_tool", map[string]any{"ServerName": 12345, "ToolName": []any{"bad"}}, "call_mcp_tool", "unknown"},
		{"mcp_github_create_pull_request", nil, "create_pull_request", "github"},
		{"mcp_docker", nil, "docker", "unknown"},
		{"mcp_singlepart", nil, "singlepart", "unknown"},
	} {
		gotTool, gotServer := ExtractMCPToolInfo(tc.toolName, tc.params)
		if gotTool != tc.wantTool || gotServer != tc.wantServer {
			t.Errorf("ExtractMCPToolInfo(%q, %v) = (%q, %q), want (%q, %q)", tc.toolName, tc.params, gotTool, gotServer, tc.wantTool, tc.wantServer)
		}
	}
}

func TestExtractSkillFromTarget(t *testing.T) {
	for _, tc := range []struct {
		target, want string
		wantOk       bool
	}{
		{"", "", false},
		{"/share/aerial/pkg/runner/pure.go", "", false},
		{"/share/aerial/.agents/skills/discord/self-improvement/SKILL.md", "self-improvement", true},
		{"/share/aerial-config/custom-skills/ha-operations/SKILL.md", "ha-operations", true},
		{"file:///data/runtimes/discord/.gemini/config/skills/culinary-recipes/SKILL.md", "culinary-recipes", true},
		{"cat /root/.gemini/config/skills/antigravity-guide/SKILL.md | grep title", "antigravity-guide", true},
		{"SKILL.md", "", false},
		{"/foo/skills/SKILL.md", "", false},
		{"no-skill-here", "", false},
		{"view_file /skills/foo/SKILL.md", "foo", true},
	} {
		if got, ok := ExtractSkillFromTarget(tc.target); got != tc.want || ok != tc.wantOk {
			t.Errorf("ExtractSkillFromTarget(%q) = (%q, %v), want (%q, %v)", tc.target, got, ok, tc.want, tc.wantOk)
		}
	}
}
