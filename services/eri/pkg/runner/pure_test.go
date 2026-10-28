package runner

import (
	"context"
	"errors"
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
		{
			AgyEnvInput{TargetID: "discord-1564872824148205579"},
			[]string{"AERIAL_TARGET_ID=1564872824148205579"},
			[]string{"AERIAL_TARGET_ID=discord-"},
		},
		{
			AgyEnvInput{TargetID: "stateful_medium:discord-1564872824148205579"},
			[]string{"AERIAL_TARGET_ID=1564872824148205579"},
			[]string{"stateful_medium"},
		},
		{
			AgyEnvInput{BaseEnv: []string{"AERIAL_TARGET_ID=stale-target"}, TargetID: "heartbeat-daily-check"},
			[]string{"GIT_TERMINAL_PROMPT=0"},
			[]string{"AERIAL_TARGET_ID="},
		},
		{
			AgyEnvInput{BaseEnv: []string{"AERIAL_TARGET_ID=stale-target"}, TargetID: "dispatch-abc-123"},
			[]string{"GIT_TERMINAL_PROMPT=0"},
			[]string{"AERIAL_TARGET_ID="},
		},
		{
			AgyEnvInput{BaseEnv: []string{"AERIAL_TARGET_ID=stale-target"}, TargetID: "discord:medium-worker-0"},
			[]string{"GIT_TERMINAL_PROMPT=0"},
			[]string{"AERIAL_TARGET_ID="},
		},
		{
			AgyEnvInput{BaseEnv: []string{"AERIAL_TARGET_ID=stale-target"}, TargetID: "vanadis-chunk-1"},
			[]string{"GIT_TERMINAL_PROMPT=0"},
			[]string{"AERIAL_TARGET_ID="},
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

	// Test error and pre-existing tag branches
	_ = injectRetrievedContext(context.Background(), "error prompt", "q", func(context.Context) (string, error) {
		return "", errors.New("amb error")
	}, time.Second, func(_ context.Context, _ string) (string, error) {
		return "", errors.New("mem error")
	}, time.Second, "[Test]")

	tagPrompt := "<ambient_context>skip</ambient_context> <retrieved_memory>skip</retrieved_memory>"
	if res := injectRetrievedContext(context.Background(), tagPrompt, "q", func(context.Context) (string, error) {
		return "should not be called", nil
	}, time.Second, func(_ context.Context, _ string) (string, error) {
		return "should not be called", nil
	}, time.Second, "[Test]"); res != tagPrompt {
		t.Errorf("injectRetrievedContext did not skip existing tags: %q", res)
	}
}

func TestCompileLeasePrompt(t *testing.T) {
	tests := []struct {
		name        string
		turn        *TurnContext
		isCold      bool
		prevSessID  string
		wantContain []string
	}{
		{
			name:        "NilTurn",
			turn:        nil,
			wantContain: nil,
		},
		{
			name:        "WarmTurn",
			turn:        &TurnContext{Prompt: "hello", CoordinationContext: "coord-info"},
			isCold:      false,
			wantContain: []string{"hello", "coord-info"},
		},
		{
			name: "ColdTurnWithScopeAndPrevSess",
			turn: &TurnContext{
				Prompt:            "hello",
				ScopeInstructions: "scope-rules",
				ThreadSummary:     "thread-sum",
			},
			isCold:      true,
			prevSessID:  "sess-prev-99",
			wantContain: []string{"hello", "<SCOPE_INSTRUCTIONS>", "scope-rules", "sess-prev-99", "thread-sum"},
		},
		{
			name: "ColdTurnWithChannelInstructions",
			turn: &TurnContext{
				Prompt:              "hello",
				ChannelInstructions: "chan-rules",
			},
			isCold:      true,
			wantContain: []string{"hello", "<SCOPE_INSTRUCTIONS>", "chan-rules"},
		},
		{
			name: "ColdTurnAlreadyHasScopeInstructions",
			turn: &TurnContext{
				Prompt:            "<SCOPE_INSTRUCTIONS>already present</SCOPE_INSTRUCTIONS>\nhello",
				ScopeInstructions: "ignored-rules",
			},
			isCold:      true,
			wantContain: []string{"already present", "hello"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := compileLeasePrompt(tc.turn, tc.isCold, tc.prevSessID)
			for _, w := range tc.wantContain {
				if !strings.Contains(got, w) {
					t.Errorf("compileLeasePrompt() = %q, missing %q", got, w)
				}
			}
		})
	}
}
