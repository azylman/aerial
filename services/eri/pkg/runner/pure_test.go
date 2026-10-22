package runner

import (
	"context"
	"strings"
	"testing"
	"time"
)

func assertContainsAndOmits(t *testing.T, target string, contains, omits []string, context any) {
	for _, c := range contains {
		if !strings.Contains(target, c) {
			t.Errorf("%+v missing %q in %s", context, c, target)
		}
	}
	for _, o := range omits {
		if strings.Contains(target, o) {
			t.Errorf("%+v should omit %q in %s", context, o, target)
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
		assertContainsAndOmits(t, strings.Join(BuildDaemonArgs(tc.cfg), " "), tc.contains, tc.omits, tc.cfg)
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
			AgyEnvInput{BaseEnv: []string{"CUSTOM_BASE=1", "GEMINI_API_KEY=ambient-key", "ANTIGRAVITY_API_KEY=ambient-key", "GOOGLE_GENAI_API_KEY=ambient-key", "gemini_api_key=ambient2", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "VANADIS_CHUNK_ID=stale-legacy-chunk", "MALFORMED_NO_EQUALS", "OTHER_ENV=kept"}},
			[]string{"CUSTOM_BASE=1", "OTHER_ENV=kept", "MALFORMED_NO_EQUALS"},
			[]string{"ambient-key", "ambient2", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "VANADIS_CHUNK_ID=stale-legacy-chunk"},
		},
	} {
		assertContainsAndOmits(t, strings.Join(BuildAgyEnv(tc.input), "\n"), tc.contains, tc.notContains, tc.input)
	}
}

func TestParseInitEvent(t *testing.T) {
	t.Parallel()
	vID := "11111111-2222-3333-4444-555555555555"
	for _, tc := range []struct {
		line, wantID string
		wantOk       bool
	}{
		{`{"event":"init","conversation_id":"` + vID + `"}`, vID, true}, {`{"event":"result","status":"SUCCESS"}`, "", false}, {`{"event":"init","conversation_id":"not-a-valid-uuid"}`, "", false}, {"", "", false},
	} {
		if id, ok := ParseInitEvent(tc.line); ok != tc.wantOk || id != tc.wantID {
			t.Errorf("ParseInitEvent(%q) = (%q, %v), want (%q, %v)", tc.line, id, ok, tc.wantID, tc.wantOk)
		}
	}
}

func TestActivityWriter_SessionProbe(t *testing.T) {
	w := NewActivityWriter("")
	if n, err := w.Write(nil); n != 0 || err != nil {
		t.Fatalf("expected 0, nil from empty write")
	}
	jsonPayload := []byte(`some log prefix {"session_id": "123e4567-e89b-12d3-a456-426614174000"} some suffix`)
	if n, err := w.Write(jsonPayload); err != nil || n != len(jsonPayload) || w.SessionID() != "123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("write or SessionID failed: %v", err)
	}
	w2 := NewActivityWriter("")
	_, _ = w2.Write([]byte("Starting conversation update stream for 223e4567-e89b-12d3-a456-426614174000\n"))
	if w2.SessionID() != "223e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("expected update stream session ID, got %q", w2.SessionID())
	}
}

func DefaultWatchdogOptions(timeoutMinutes int) WatchdogOptions {
	return WatchdogOptions{
		InactivityTimeout: 5 * time.Minute,
		MaxDuration:       time.Duration(timeoutMinutes) * time.Minute,
		OutputFormat:      "stream-json",
	}
}

func TestDefaultWatchdogOptions(t *testing.T) {
	if opts := DefaultWatchdogOptions(5); opts.InactivityTimeout == 0 || opts.MaxDuration == 0 {
		t.Errorf("expected non-zero watchdog options")
	}
}

func TestCleanUnescapedString(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`"github"`, "github"}, {`"\"github\""`, "github"}, {`"\"nested\""`, "nested"}, {`\hello\`, "hello"}, {`  "unifi"  `, "unifi"}, {"plain", "plain"}, {`""`, ""}, {"", ""}, {`"a"`, "a"}, {`"`, `"`},
	} {
		if got := CleanUnescapedString(tc.in); got != tc.want {
			t.Errorf("CleanUnescapedString(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestExtractMCPToolInfo(t *testing.T) {
	for _, tc := range []struct {
		toolName, wantTool, wantServer string
		params                         map[string]any
	}{
		{"", "unknown", "native", nil},
		{"run_command", "run_command", "native", map[string]any{"CommandLine": "ls"}},
		{"call_mcp_tool", "list_pull_requests", "github", map[string]any{"ServerName": "github", "ToolName": "list_pull_requests"}},
		{"call_mcp_tool", "unifi_execute", "unifi", map[string]any{"ServerName": `"\"unifi\""`, "ToolName": `"\"unifi_execute\""`}},
		{"call_mcp_tool", "call_mcp_tool", "unknown", nil},
		{"call_mcp_tool", "call_mcp_tool", "docker", map[string]any{"ServerName": "docker"}},
		{"call_mcp_tool", "list_containers", "unknown", map[string]any{"ToolName": "list_containers"}},
		{"call_mcp_tool", "call_mcp_tool", "unknown", map[string]any{"ServerName": 12345, "ToolName": []any{"bad"}}},
		{"mcp_github_create_pull_request", "create_pull_request", "github", nil},
		{"mcp_docker", "docker", "unknown", nil},
		{"mcp_singlepart", "singlepart", "unknown", nil},
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
		{"SKILL.md", "", false},
		{"/foo/skills/SKILL.md", "", false},
		{"no-skill-here", "", false},
		{"view_file /skills/foo/SKILL.md", "foo", true},
		{"/share/aerial/.agents/skills/discord/self-improvement/SKILL.md", "self-improvement", true},
		{"/share/aerial-config/custom-skills/ha-operations/SKILL.md", "ha-operations", true},
		{"file:///data/runtimes/discord/.gemini/config/skills/culinary-recipes/SKILL.md", "culinary-recipes", true},
		{"cat /root/.gemini/config/skills/antigravity-guide/SKILL.md | grep title", "antigravity-guide", true},
	} {
		if got, ok := ExtractSkillFromTarget(tc.target); got != tc.want || ok != tc.wantOk {
			t.Errorf("ExtractSkillFromTarget(%q) = (%q, %v), want (%q, %v)", tc.target, got, ok, tc.want, tc.wantOk)
		}
	}

	var gotMemQuery string
	injected := injectRetrievedContext(nil, "full prompt envelope", "raw query", func(context.Context) (string, error) {
		return "<ambient_context>amb</ambient_context>", nil
	}, time.Second, func(_ context.Context, q string) (string, error) {
		gotMemQuery = q
		return "<retrieved_memory>mem</retrieved_memory>", nil
	}, time.Second, "[Test]")
	if gotMemQuery != "raw query" || !strings.HasPrefix(injected, "<retrieved_memory>mem</retrieved_memory>\n\n<ambient_context>amb</ambient_context>\n\nfull prompt envelope") {
		t.Errorf("injectRetrievedContext query=%q, out=%q", gotMemQuery, injected)
	}
	_ = injectRetrievedContext(context.Background(), "fallback prompt", "", nil, time.Second, func(_ context.Context, q string) (string, error) {
		gotMemQuery = q
		return "", nil
	}, time.Second, "[Test]")
	if gotMemQuery != "fallback prompt" {
		t.Errorf("expected fallback prompt, got %q", gotMemQuery)
	}
}
