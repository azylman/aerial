package discordutil

import (
	"maps"
	"testing"
)

func TestTokenEnvKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		agent string
		want  string
	}{
		{name: "empty agent uses plain token", agent: "", want: "DISCORD_TOKEN"},
		{name: "default agent uses plain token", agent: "aerial", want: "DISCORD_TOKEN"},
		{name: "named agent upper-cased", agent: "alpha", want: "DISCORD_TOKEN_ALPHA"},
		{name: "hyphen maps to underscore", agent: "agent-alpha", want: "DISCORD_TOKEN_AGENT_ALPHA"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TokenEnvKey(tc.agent); got != tc.want {
				t.Errorf("TokenEnvKey(%q) = %q, want %q", tc.agent, got, tc.want)
			}
		})
	}
}

func TestAgentTokensFromEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		environ []string
		want    map[string]string
	}{
		{name: "empty environ", environ: nil, want: map[string]string{}},
		{name: "per-agent token trimmed and keyed", environ: []string{"DISCORD_TOKEN_ALPHA= tok-a "}, want: map[string]string{"alpha": "tok-a"}},
		{name: "underscore maps to hyphen", environ: []string{"DISCORD_TOKEN_AGENT_ALPHA=tok"}, want: map[string]string{"agent-alpha": "tok"}},
		{name: "plain tokens ignored", environ: []string{"DISCORD_TOKEN=plain", "DISCORD_BOT_TOKEN=bot"}, want: map[string]string{}},
		{name: "blank value skipped", environ: []string{"DISCORD_TOKEN_ALPHA=   "}, want: map[string]string{}},
		{name: "empty suffix skipped", environ: []string{"DISCORD_TOKEN_=tok"}, want: map[string]string{}},
		{name: "entry without equals skipped", environ: []string{"DISCORD_TOKEN_ALPHA"}, want: map[string]string{}},
		{name: "unrelated keys skipped", environ: []string{"PATH=/bin", "OTHER_DISCORD_TOKEN_X=tok"}, want: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AgentTokensFromEnv(tc.environ); !maps.Equal(got, tc.want) {
				t.Errorf("AgentTokensFromEnv = %v, want %v", got, tc.want)
			}
		})
	}
}
