package buildplan

import "testing"

func TestServiceNames(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, agent, wantBuild, wantRouter string }{
		{"empty", "", "Build", "BuildRouter"},
		{"default", "aerial", "Build", "BuildRouter"},
		{"scoped", "Agent-Alpha", "Build-agent-alpha", "BuildRouter-agent-alpha"},
		{"whitespace", "  Agent-Beta  ", "Build-agent-beta", "BuildRouter-agent-beta"},
		{"underscore", "agent_beta", "Build-agent_beta", "BuildRouter-agent_beta"},
		{"unsafe", "bad name!", "Build", "BuildRouter"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := BuildServiceName(tc.agent); got != tc.wantBuild {
				t.Errorf("BuildServiceName(%q) = %q, want %q", tc.agent, got, tc.wantBuild)
			}
			if got := BuildRouterServiceName(tc.agent); got != tc.wantRouter {
				t.Errorf("BuildRouterServiceName(%q) = %q, want %q", tc.agent, got, tc.wantRouter)
			}
		})
	}
}
