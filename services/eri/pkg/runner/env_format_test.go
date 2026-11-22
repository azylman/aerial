package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func assertContainsAndOmits(t *testing.T, target string, contains, omits []string, label any) {
	t.Helper()
	for _, c := range contains {
		if !strings.Contains(target, c) {
			t.Errorf("%+v missing %q in %s", label, c, target)
		}
	}
	for _, o := range omits {
		if strings.Contains(target, o) {
			t.Errorf("%+v should omit %q in %s", label, o, target)
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
		{DaemonConfig{SessionID: "sess-abc", Model: "gemini-test"}, []string{"--dangerously-skip-permissions", "--log-file /dev/stderr", "--input-format stream-json", "--output-format stream-json", "--conversation sess-abc", "--model gemini-test", "--print-timeout 60m"}, nil},
		{DaemonConfig{Timeout: 15 * time.Minute}, []string{"--log-file /dev/stderr", "--print-timeout 15m"}, []string{"--conversation", "--model"}},
		{DaemonConfig{Timeout: 45 * time.Second}, []string{"--log-file /dev/stderr", "--print-timeout 45s"}, nil},
		{
			DaemonConfig{
				Engine:        "claude",
				SessionID:     "sess-claude-new",
				ResumeSession: false,
				Model:         "claude-3-7-sonnet",
				GeminiHomeDir: "/home/user",
			},
			[]string{"-p", "--verbose", "--dangerously-skip-permissions", "--input-format stream-json", "--output-format stream-json", "--include-partial-messages", "--session-id sess-claude-new", "--model claude-3-7-sonnet", "--mcp-config /home/user/.claude/mcp_config.json"},
			[]string{"--resume", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine:        "claude",
				SessionID:     "sess-claude-resumed",
				ResumeSession: true,
			},
			[]string{"-p", "--verbose", "--dangerously-skip-permissions", "--resume sess-claude-resumed"},
			[]string{"--session-id", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine:    "claude",
				SessionID: "sess-claude-effort-high",
				Model:     "opus-high",
			},
			[]string{"-p", "--verbose", "--dangerously-skip-permissions", "--session-id sess-claude-effort-high", "--model opus", "--effort high"},
			[]string{"--resume", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine:    "claude",
				SessionID: "sess-claude-effort-medium",
				Model:     "opus-medium",
			},
			[]string{"-p", "--verbose", "--dangerously-skip-permissions", "--session-id sess-claude-effort-medium", "--model opus", "--effort medium"},
			[]string{"--resume", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine:    "claude",
				SessionID: "sess-claude-effort-low",
				Model:     "opus-low",
			},
			[]string{"-p", "--verbose", "--dangerously-skip-permissions", "--session-id sess-claude-effort-low", "--model opus", "--effort low"},
			[]string{"--resume", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine:    "claude",
				SessionID: "sess-claude-explicit-effort",
				Model:     "opus",
				Effort:    "high",
			},
			[]string{"-p", "--verbose", "--dangerously-skip-permissions", "--session-id sess-claude-explicit-effort", "--model opus", "--effort high"},
			[]string{"--resume", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine:    "codex",
				SessionID: "sess-codex-123",
				Model:     "gpt-5",
			},
			[]string{"app-server", "--listen", "stdio://"},
			[]string{"--dangerously-skip-permissions", "--conversation", "--model", "--print-timeout", "-p", "--verbose"},
		},
		{
			DaemonConfig{
				Engine:    "muse",
				SessionID: "sess-muse-123",
				Model:     "llama-3.3-70b",
			},
			[]string{"serve", "--provider", "meta", "--disable-sandbox", "--trust-workspace", "--disable-request-user-input", "-c runtime_capabilities.plugin:goal-reminder:reminder:goal-reminder.enabled=false", "--model llama-3.3-70b"},
			[]string{"--dangerously-skip-permissions", "--conversation", "--print-timeout", "--verbose", "app-server"},
		},
		{
			DaemonConfig{
				Engine:    "muse",
				SessionID: "sess-muse-effort",
				Model:     "muse-code-high",
			},
			[]string{"serve", "--provider", "meta", "--disable-sandbox", "--trust-workspace", "--disable-request-user-input", "-c runtime_capabilities.plugin:goal-reminder:reminder:goal-reminder.enabled=false", "--model muse-code"},
			[]string{"--dangerously-skip-permissions", "--conversation", "--print-timeout"},
		},
		{
			DaemonConfig{
				Engine: "muse",
			},
			[]string{"serve", "--provider", "meta", "--disable-sandbox", "--trust-workspace", "--disable-request-user-input", "-c runtime_capabilities.plugin:goal-reminder:reminder:goal-reminder.enabled=false"},
			[]string{"--model", "--dangerously-skip-permissions", "--conversation", "--print-timeout"},
		},
	} {
		assertContainsAndOmits(t, strings.Join(BuildDaemonArgs(tc.cfg), " "), tc.contains, tc.omits, tc.cfg)
	}
}

func TestParseModelAndEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		model      string
		effort     string
		allowXHigh bool
		wantModel  string
		wantEffort string
	}{
		{name: "xhigh allowed suffix", model: "codex-5-xhigh", effort: "", allowXHigh: true, wantModel: "codex-5", wantEffort: "xhigh"},
		{name: "xhigh disallowed keeps suffix", model: "codex-5-xhigh", effort: "", allowXHigh: false, wantModel: "codex-5-xhigh", wantEffort: ""},
		{name: "high suffix with xhigh allowed", model: "codex-5-high", effort: "", allowXHigh: true, wantModel: "codex-5", wantEffort: "high"},
		{name: "medium suffix", model: "model-medium", effort: "", allowXHigh: true, wantModel: "model", wantEffort: "medium"},
		{name: "low suffix", model: "model-low", effort: "", allowXHigh: true, wantModel: "model", wantEffort: "low"},
		{name: "explicit effort fallback", model: "model-base", effort: "high", allowXHigh: true, wantModel: "model-base", wantEffort: "high"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotModel, gotEffort := parseModelAndEffort(tc.model, tc.effort, tc.allowXHigh)
			if gotModel != tc.wantModel {
				t.Errorf("parseModelAndEffort(%q, %q, %v) gotModel = %q, want %q", tc.model, tc.effort, tc.allowXHigh, gotModel, tc.wantModel)
			}
			if gotEffort != tc.wantEffort {
				t.Errorf("parseModelAndEffort(%q, %q, %v) gotEffort = %q, want %q", tc.model, tc.effort, tc.allowXHigh, gotEffort, tc.wantEffort)
			}
		})
	}
}

func TestParseClaudeModelAndEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		model      string
		effort     string
		wantModel  string
		wantEffort string
	}{
		{name: "opus high suffix", model: "opus-high", effort: "", wantModel: "opus", wantEffort: "high"},
		{name: "opus medium suffix", model: "opus-medium", effort: "", wantModel: "opus", wantEffort: "medium"},
		{name: "opus low suffix", model: "opus-low", effort: "", wantModel: "opus", wantEffort: "low"},
		{name: "case insensitive suffix", model: "Claude-3-7-Sonnet-HIGH", effort: "", wantModel: "Claude-3-7-Sonnet", wantEffort: "high"},
		{name: "explicit effort takes effect without suffix", model: "opus", effort: "high", wantModel: "opus", wantEffort: "high"},
		{name: "explicit effort normalized lowercase", model: "opus", effort: "  MEDIUM  ", wantModel: "opus", wantEffort: "medium"},
		{name: "suffix overrides explicit effort", model: "opus-low", effort: "high", wantModel: "opus", wantEffort: "low"},
		{name: "bare suffix without prefix", model: "-high", effort: "medium", wantModel: "-high", wantEffort: "medium"},
		{name: "whitespace padded model", model: "  claude-3-5-sonnet-high  ", effort: "", wantModel: "claude-3-5-sonnet", wantEffort: "high"},
		{name: "no suffix and no effort", model: "claude-3-7-sonnet", effort: "", wantModel: "claude-3-7-sonnet", wantEffort: ""},
		{name: "empty model with effort", model: "", effort: "low", wantModel: "", wantEffort: "low"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotModel, gotEffort := parseClaudeModelAndEffort(tc.model, tc.effort)
			if gotModel != tc.wantModel {
				t.Errorf("parseClaudeModelAndEffort(%q, %q) gotModel = %q, want %q", tc.model, tc.effort, gotModel, tc.wantModel)
			}
			if gotEffort != tc.wantEffort {
				t.Errorf("parseClaudeModelAndEffort(%q, %q) gotEffort = %q, want %q", tc.model, tc.effort, gotEffort, tc.wantEffort)
			}
		})
	}
}

func TestFilterAgyBaseEnv(t *testing.T) {
	t.Parallel()
	filt := FilterAgyBaseEnv([]string{
		"FOO=bar",
		"CLAUDE_CODE_OAUTH_TOKEN=claude-secret-kept",
		"GEMINI_API_KEY=secret1",
		"gemini_api_key=secret2",
		"ANTIGRAVITY_API_KEY=s3",
		"GOOGLE_GENAI_API_KEY=s4",
		"gemini_harness_api_key=s5",
		"AGY_GEMINI_API_KEY=s6",
		"github_pat=s7",
		"DISCORD_TOKEN=s8",
		"discord_bot_token=s9",
		"EXAMPLE_AGENT_DISCORD_BOT_TOKEN=s24",
		"OTHER_AGENT_DISCORD_BOT_TOKEN=s32",
		"DATABASE_URL=s10",
		"POSTGRES_PASSWORD=s11",
		"ha_token=s12",
		"unifi_pass=s15",
		"UNIFI_PASSWORD=s16",
		"op_service_account_token=s17",
		"1PASSWORD_SERVICE_ACCOUNT_TOKEN=s18",
		"qnap_pass=s19",
		"infisical_client_secret=s21",
		"CLOUDFLARE_MCP_TOKEN=s22",
		"ubereats_mcp_token=s23",
		"OPENAI_API_KEY=s23a",
		"codex_api_key=s23b",
		"META_API_KEY=s23c",
		"model_api_key=s23d",
		"CUSTOM_APP_SECRET=s25",
		"MY_AUTH_TOKEN=s26",
		"REDIS_PASSWORD=s27",
		"HTTP_PROXY_PASS=s28",
		"DEPLOY_PAT=s29",
		"STRIPE_API_KEY=s30",
		"SSH_PRIVATE_KEY=s31",
		"NATS_URL=nats://secret-nats:4222",
		"RESTATE_INGRESS_URL=http://secret-restate:8080",
		"RESTATE_ADMIN_URL=http://secret-restate:9070",
		"MALFORMED_NO_EQUALS",
		"PATH=/usr/bin",
	})
	if fj := strings.Join(filt, "\n"); strings.Contains(fj, "secret1") || strings.Contains(fj, "=s") || strings.Contains(fj, "secret-nats") || strings.Contains(fj, "secret-restate") || !strings.Contains(fj, "FOO=bar") || !strings.Contains(fj, "CLAUDE_CODE_OAUTH_TOKEN=claude-secret-kept") || !strings.Contains(fj, "MALFORMED_NO_EQUALS") || !strings.Contains(fj, "PATH=/usr/bin") {
		t.Errorf("FilterAgyBaseEnv leak or drop: %v", filt)
	}
}

func TestBuildAgyEnv(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input       AgyEnvInput
		contains    []string
		notContains []string
	}{
		{
			AgyEnvInput{BaseEnv: []string{"OTHER_AGENT_DISCORD_BOT_TOKEN=other-agent-secret", "EXAMPLE_AGENT_DISCORD_BOT_TOKEN=example-agent-secret", "arbitrary_prefix_token=arbitrary-secret", "CLAUDE_CODE_OAUTH_TOKEN=oauth-kept", "OTHER_ENV=kept"}},
			[]string{"OTHER_ENV=kept", "CLAUDE_CODE_OAUTH_TOKEN=oauth-kept"},
			[]string{"OTHER_AGENT_DISCORD_BOT_TOKEN=", "EXAMPLE_AGENT_DISCORD_BOT_TOKEN=", "other-agent-secret", "example-agent-secret", "arbitrary_prefix_token=", "arbitrary-secret"},
		},
		{
			AgyEnvInput{BaseEnv: []string{"CUSTOM_BASE=1", "NATS_URL=nats://leak-nats:4222", "RESTATE_INGRESS_URL=http://leak-restate:8080", "RESTATE_ADMIN_URL=http://leak-restate:9070", "OTHER_ENV=kept"}},
			[]string{"CUSTOM_BASE=1", "OTHER_ENV=kept"},
			[]string{"NATS_URL=", "leak-nats", "RESTATE_INGRESS_URL=", "RESTATE_ADMIN_URL=", "leak-restate"},
		},
		{AgyEnvInput{}, []string{"GIT_TERMINAL_PROMPT=0", "AGY_LOG_LEVEL=debug", "ANTIGRAVITY_LOG_LEVEL=debug"}, nil},
		{
			AgyEnvInput{
				BaseEnv:  []string{"CUSTOM_BASE=1", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "PATH=/bin", "HOME=/root", "USERPROFILE=C:\\root", "GEMINI_CLI_HOME=/gemini", "CLAUDE_CONFIG_DIR=/stale/claude", "CODEX_HOME=/stale/codex", "XDG_CONFIG_HOME=/stale/config", "XDG_DATA_HOME=/stale/share", "MUSE_AUTH_PATH=/stale/auth.json"},
				HomeDir:  "/custom/home",
				APIKey:   "secret-token-123",
				TargetID: "target-agent",
				Timezone: "America/Los_Angeles",
				ExtraEnv: []string{"EXTRA_KEY=foo"},
			},
			[]string{"CUSTOM_BASE=1", "GIT_TERMINAL_PROMPT=0", "AGY_LOG_LEVEL=debug", "ANTIGRAVITY_LOG_LEVEL=debug", "HOME=/custom/home", "USERPROFILE=/custom/home", "GEMINI_CLI_HOME=/custom/home", "CLAUDE_CONFIG_DIR=/custom/home/.claude", "CODEX_HOME=/custom/home/.codex", "XDG_CONFIG_HOME=/custom/home/.config", "XDG_DATA_HOME=/custom/home/.local/share", "MUSE_AUTH_PATH=/custom/home/.config/muse/auth.json", "GEMINI_API_KEY=secret-token-123", "ANTIGRAVITY_API_KEY=secret-token-123", "GOOGLE_GENAI_API_KEY=secret-token-123", "AERIAL_TARGET_ID=target-agent", "TZ=America/Los_Angeles", "EXTRA_KEY=foo"},
			[]string{"AERIAL_VANADIS_CHUNK_ID=stale-chunk", "HOME=/root", "USERPROFILE=C:\\root", "GEMINI_CLI_HOME=/gemini", "CLAUDE_CONFIG_DIR=/stale/claude", "CODEX_HOME=/stale/codex", "XDG_CONFIG_HOME=/stale/config", "XDG_DATA_HOME=/stale/share", "MUSE_AUTH_PATH=/stale/auth.json"},
		},
		{
			AgyEnvInput{BaseEnv: []string{"CUSTOM_BASE=1", "GEMINI_API_KEY=ambient-key", "ANTIGRAVITY_API_KEY=ambient-key", "GOOGLE_GENAI_API_KEY=ambient-key", "gemini_api_key=ambient2", "OPENAI_API_KEY=ambient-openai", "CODEX_API_KEY=ambient-codex", "GITHUB_PAT=secret-pat", "DISCORD_TOKEN=secret-discord", "DATABASE_URL=postgres://leak", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "VANADIS_CHUNK_ID=stale-legacy-chunk", "MALFORMED_NO_EQUALS", "OTHER_ENV=kept"}},
			[]string{"CUSTOM_BASE=1", "OTHER_ENV=kept", "MALFORMED_NO_EQUALS"},
			[]string{"ambient-key", "ambient2", "ambient-openai", "ambient-codex", "secret-pat", "secret-discord", "postgres://leak", "AERIAL_VANADIS_CHUNK_ID=stale-chunk", "VANADIS_CHUNK_ID=stale-legacy-chunk"},
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
		{
			AgyEnvInput{VanadisChunkID: "build-1794202576-chunk-2", TurnEnvFile: "/data/.gemini/turn_env/default.sh", SATokenPath: "/data/.gemini/turn_env/sa-token"},
			[]string{"AERIAL_VANADIS_CHUNK_ID=build-1794202576-chunk-2", "BASH_ENV=/data/.gemini/turn_env/default.sh", "AERIAL_SA_TOKEN_PATH=/data/.gemini/turn_env/sa-token"},
			nil,
		},
		{
			AgyEnvInput{DaemonID: "p3/discord-1", MCPProxyPort: "8080"},
			[]string{"ERI_DAEMON_ID=p3/discord-1", "ERI_HOOK_ENDPOINT=http://127.0.0.1:8080/internal/mcp-proxy/announce"},
			nil,
		},
		{
			AgyEnvInput{DaemonID: "p3/discord-1"},
			[]string{"GIT_TERMINAL_PROMPT=0"},
			[]string{"ERI_DAEMON_ID=", "ERI_HOOK_ENDPOINT="},
		},
		{
			AgyEnvInput{MCPProxyPort: "8080"},
			[]string{"GIT_TERMINAL_PROMPT=0"},
			[]string{"ERI_DAEMON_ID=", "ERI_HOOK_ENDPOINT="},
		},
		{
			AgyEnvInput{
				BaseEnv:  []string{"TRACEPARENT=00-11111111111111111111111111111111-1111111111111111-01", "tracestate=stale=1", "Baggage=stale=1", "KEEP=1"},
				TraceEnv: map[string]string{"TRACEPARENT": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "BAGGAGE": "tenant=a"},
			},
			[]string{"KEEP=1", "TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "BAGGAGE=tenant=a"},
			[]string{"11111111111111111111111111111111", "stale=1"},
		},
		{
			AgyEnvInput{TraceEnv: map[string]string{"tracestate": "vendor=v1", "BAGGAGE": "", " ": "blank-key"}},
			[]string{"TRACESTATE=vendor=v1"},
			[]string{"tracestate=", "BAGGAGE=", "blank-key"},
		},
		{
			AgyEnvInput{BaseEnv: []string{"TRACEPARENT=00-11111111111111111111111111111111-1111111111111111-01", "KEEP=1"}},
			[]string{"KEEP=1"},
			[]string{"TRACEPARENT="},
		},
	} {
		assertContainsAndOmits(t, strings.Join(BuildAgyEnv(tc.input), "\n"), tc.contains, tc.notContains, tc.input)
	}
}

func TestBuildAgyEnv_ClaudeTelemetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input       AgyEnvInput
		contains    []string
		notContains []string
	}{
		{
			AgyEnvInput{
				Engine:       "claude",
				MCPProxyPort: "8080",
				DaemonID:     "d-claude-1",
			},
			[]string{
				"CLAUDE_CODE_ENABLE_TELEMETRY=1",
				"OTEL_LOGS_EXPORTER=otlp",
				"OTEL_METRICS_EXPORTER=none",
				"OTEL_TRACES_EXPORTER=none",
				"OTEL_EXPORTER_OTLP_LOGS_PROTOCOL=http/json",
				"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://127.0.0.1:8080/internal/otel/v1/logs",
				"OTEL_LOG_USER_PROMPTS=0",
				"OTEL_LOG_ASSISTANT_RESPONSES=0",
				"OTEL_LOG_TOOL_CONTENT=0",
				"OTEL_LOG_TOOL_DETAILS=0",
				"OTEL_LOG_RAW_API_BODIES=0",
				"OTEL_LOG_MANAGED_SETTINGS=0",
				"OTEL_EXPORTER_OTLP_LOGS_HEADERS=X-Eri-Daemon=d-claude-1",
			},
			nil,
		},
		{
			AgyEnvInput{
				Engine:       "claude",
				MCPProxyPort: "8080",
			},
			[]string{
				"CLAUDE_CODE_ENABLE_TELEMETRY=1",
				"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://127.0.0.1:8080/internal/otel/v1/logs",
			},
			[]string{
				"OTEL_EXPORTER_OTLP_LOGS_HEADERS=",
			},
		},
		{
			AgyEnvInput{
				Engine:       "agy",
				MCPProxyPort: "8080",
			},
			nil,
			[]string{
				"CLAUDE_CODE_ENABLE_TELEMETRY=",
				"OTEL_LOGS_EXPORTER=",
			},
		},
	} {
		assertContainsAndOmits(t, strings.Join(BuildAgyEnv(tc.input), "\n"), tc.contains, tc.notContains, tc.input)
	}
}

func TestSanitizeDaemonTargetKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want string
	}{
		{"", "default"},
		{"   ", "default"},
		{"discord-12345", "discord-12345"},
		{"stateful_medium:discord-12345", "stateful_medium_discord-12345"},
		{"worker:0.1-alpha", "worker_0.1-alpha"},
		{"foo@bar/baz#qux", "foo_bar_baz_qux"},
	}
	for _, tc := range tests {
		if got := SanitizeDaemonTargetKey(tc.in); got != tc.want {
			t.Errorf("SanitizeDaemonTargetKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTraceCarrierEnv(t *testing.T) {
	t.Parallel()
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ts, _ := trace.ParseTraceState("vendor=v1")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: ts, Remote: true})
	bag, _ := baggage.Parse("tenant=a")
	full := baggage.ContextWithBaggage(trace.ContextWithSpanContext(context.Background(), sc), bag)
	composite := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	tests := []struct {
		name string
		ctx  context.Context
		prop propagation.TextMapPropagator
		want map[string]string
	}{
		{name: "span and baggage become uppercase env keys", ctx: full, prop: composite, want: map[string]string{"TRACEPARENT": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "TRACESTATE": "vendor=v1", "BAGGAGE": "tenant=a"}},
		{name: "trace context only propagator omits baggage", ctx: full, prop: propagation.TraceContext{}, want: map[string]string{"TRACEPARENT": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "TRACESTATE": "vendor=v1"}},
		{name: "no span context injects nothing", ctx: context.Background(), prop: composite, want: map[string]string{}},
		{name: "nil propagator injects nothing", ctx: full, want: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TraceCarrierEnv(tc.ctx, tc.prop); fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("TraceCarrierEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFormatTurnEnvFileContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		targetID       string
		vanadisChunkID string
		wantContains   []string
	}{
		{
			name:           "populated target and chunk",
			targetID:       "discord-123456",
			vanadisChunkID: "build-1-chunk-2",
			wantContains: []string{
				"unset BASH_ENV\n",
				"export AERIAL_TARGET_ID='123456'\n",
				"export AERIAL_VANADIS_CHUNK_ID='build-1-chunk-2'\n",
			},
		},
		{
			name:           "empty target and chunk",
			targetID:       "",
			vanadisChunkID: "",
			wantContains: []string{
				"unset BASH_ENV\n",
				"unset AERIAL_TARGET_ID\n",
				"unset AERIAL_VANADIS_CHUNK_ID\n",
			},
		},
		{
			name:           "headless worker target normalized to unset",
			targetID:       "dispatch-999",
			vanadisChunkID: "",
			wantContains: []string{
				"unset BASH_ENV\n",
				"unset AERIAL_TARGET_ID\n",
				"unset AERIAL_VANADIS_CHUNK_ID\n",
			},
		},
		{
			name:           "escaping single quotes",
			targetID:       "foo'bar",
			vanadisChunkID: "chunk'escaped'test",
			wantContains: []string{
				"unset BASH_ENV\n",
				"export AERIAL_TARGET_ID='foo'\\''bar'\n",
				"export AERIAL_VANADIS_CHUNK_ID='chunk'\\''escaped'\\''test'\n",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatTurnEnvFileContent(tc.targetID, tc.vanadisChunkID)
			for _, exp := range tc.wantContains {
				if !strings.Contains(got, exp) {
					t.Errorf("FormatTurnEnvFileContent(%q, %q) missing %q; got:\n%s", tc.targetID, tc.vanadisChunkID, exp, got)
				}
			}
		})
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
		{"mcp__github__create_pull_request", "create_pull_request", "github", nil},
		{"mcp__docker__run", "run", "docker", nil},
		{"mcp__singlepart", "singlepart", "unknown", nil},
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
		{"/share/aerial/pkg/runner/env_format.go", "", false},
		{"SKILL.md", "", false},
		{"/foo/skills/SKILL.md", "", false},
		{"no-skill-here", "", false},
		{"view_file /skills/foo/SKILL.md", "foo", true},
		{"/share/aerial/.agents/skills/discord/self-improvement/SKILL.md", "self-improvement", true},
		{"/share/aerial-config/custom-skills/ha-operations/SKILL.md", "ha-operations", true},
		{"file:///data/runtimes/discord/.gemini/config/skills/culinary-recipes/SKILL.md", "culinary-recipes", true},
		{"cat /root/.gemini/config/skills/antigravity-guide/SKILL.md | grep title", "antigravity-guide", true},
		{"/data/runtimes/discord/.gemini/config/skills/create-payment-credential.tmp/SKILL.md", "create-payment-credential", true},
		{"/data/runtimes/discord/.gemini/antigravity-cli/builtin/skills/antigravity_guide/SKILL.md", "antigravity_guide", true},
		{`"/data/runtimes/discord/.gemini/config/skills/systematic-debugging/SKILL.md"`, "systematic-debugging", true},
		{`"\"/data/runtimes/discord/.gemini/config/skills/systematic-debugging/SKILL.md\""`, "systematic-debugging", true},
		{`find /share/aerial -maxdepth 5 -name "SKILL.md"`, "", false},
		{"find . -name SKILL.md", "", false},
		{`find /share -path "*/SKILL.md"`, "", false},
		{"/share/aerial/.agents/skills/discord/SKILL.md", "", false},
		{"/share/aerial/.agents/skills/common/SKILL.md", "", false},
		{"/share/aerial/.agents/skills/-bad-skill/SKILL.md", "", false},
		{"/share/aerial/.agents/skills/BadUpper/SKILL.md", "", false},
		{"/share/aerial/.agents/skills/foo/SKILL.md.bak", "", false},
		{"test -f SKILL.md && cat /share/aerial-config/custom-skills/ha-operations/SKILL.md", "ha-operations", true},
	} {
		if got, ok := ExtractSkillFromTarget(tc.target); got != tc.want || ok != tc.wantOk {
			t.Errorf("ExtractSkillFromTarget(%q) = (%q, %v), want (%q, %v)", tc.target, got, ok, tc.want, tc.wantOk)
		}
	}

	var gotMemQuery string
	injected := injectRetrievedContext(t.Context(), "full prompt envelope", "raw query", func(context.Context) (string, error) {
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

type parseStderrLineCase struct {
	name, engine, line, wantMsg, wantSource string
	wantLevel                               slog.Level
	wantKeep                                bool
}

func assertParseAndFilterStderrLine(t *testing.T, tc parseStderrLineCase) {
	t.Helper()
	gotLevel, gotMsg, gotSource, gotKeep := ParseAndFilterStderrLine(tc.engine, tc.line)
	if gotKeep != tc.wantKeep {
		t.Fatalf("ParseAndFilterStderrLine() keep = %v, want %v", gotKeep, tc.wantKeep)
	}
	if !tc.wantKeep {
		return
	}
	if gotLevel != tc.wantLevel {
		t.Errorf("level = %v, want %v", gotLevel, tc.wantLevel)
	}
	if gotMsg != tc.wantMsg {
		t.Errorf("msg = %q, want %q", gotMsg, tc.wantMsg)
	}
	if gotSource != tc.wantSource {
		t.Errorf("source = %q, want %q", gotSource, tc.wantSource)
	}
}

func TestParseAndFilterStderrLine(t *testing.T) {
	t.Parallel()
	tests := []parseStderrLineCase{
		{name: "glog info", engine: "agy", line: "I1118 09:24:54.123456 12345 client.go:42] Successfully connected to backend", wantLevel: slog.LevelInfo, wantMsg: "Successfully connected to backend", wantSource: "client.go:42", wantKeep: true},
		{name: "glog warn", engine: "agy", line: "W1118 09:24:54.123456 12345 auth.go:10] Token refreshing soon", wantLevel: slog.LevelWarn, wantMsg: "Token refreshing soon", wantSource: "auth.go:10", wantKeep: true},
		{name: "glog error", engine: "agy", line: "E1118 09:24:54.123456 12345 db.go:99] Connection failed to database", wantLevel: slog.LevelError, wantMsg: "Connection failed to database", wantSource: "db.go:99", wantKeep: true},
		{name: "glog fatal", engine: "agy", line: "F1118 09:24:54.123456 12345 main.go:1] Fatal runtime crash", wantLevel: slog.LevelError, wantMsg: "Fatal runtime crash", wantSource: "main.go:1", wantKeep: true},
		{name: "benign noise unauthenticated", engine: "agy", line: "I1118 09:24:54.123456 12345 client.go:42] You are not logged into Antigravity.", wantKeep: false},
		{name: "benign noise auth mode unspecified", engine: "agy", line: "I1118 09:24:54.123456 12345 client.go:42] Auth mode is unspecified, skipping fetchAvailableModels", wantKeep: false},
		{name: "benign noise non-glog", engine: "agy", line: "Warning: You are not logged into Antigravity. Please login.", wantKeep: false},
		{name: "non-glog warning on error keyword", engine: "claude", line: "An error occurred while reading config file", wantLevel: slog.LevelWarn, wantMsg: "An error occurred while reading config file", wantKeep: true},
		{name: "non-glog warning on fatal keyword", engine: "codex", line: "fatal: branch not found", wantLevel: slog.LevelWarn, wantMsg: "fatal: branch not found", wantKeep: true},
		{name: "non-glog warning on panic keyword", engine: "muse", line: "panic: runtime error dereference", wantLevel: slog.LevelWarn, wantMsg: "panic: runtime error dereference", wantKeep: true},
		{name: "non-glog warning on warn keyword", engine: "agy", line: "warning: deprecated flag used", wantLevel: slog.LevelWarn, wantMsg: "warning: deprecated flag used", wantKeep: true},
		{name: "non-glog regular info", engine: "agy", line: "Server listening on port 8080", wantLevel: slog.LevelInfo, wantMsg: "Server listening on port 8080", wantKeep: true},
		{name: "empty line", engine: "agy", line: "", wantKeep: false},
		{name: "whitespace line with newline", engine: "agy", line: "   \r\n", wantKeep: false},
		{name: "glog empty message", engine: "agy", line: "I1118 09:24:54.123456 12345 client.go:42]   \n", wantKeep: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertParseAndFilterStderrLine(t, tc)
		})
	}
}
