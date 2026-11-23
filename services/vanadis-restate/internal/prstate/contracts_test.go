package prstate

import (
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

// TestPRStateContracts freezes the key formats. Any rename here is a
// cross-chunk breaking change.
func TestPRStateContracts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "effect key string", got: EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 3, Kind: prwire.EffectDiscordAlert}.String(), want: "azylman/aerial#101@abc123:gen3:discord_alert"},
		{name: "effect key deploy carries job", got: EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 1, Kind: prwire.EffectDeployDispatch, JobName: "eri"}.String(), want: "azylman/aerial#101@abc123:gen1:deploy_dispatch:job=eri"},
		{name: "effect key ci carries check", got: EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 1, Kind: prwire.EffectGitHubRerun, CheckName: "Lint"}.String(), want: "azylman/aerial#101@abc123:gen1:github_rerun:check=lint"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}

func TestPRStateStaleGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		current string
		event   string
		want    bool
	}{
		{name: "matching sha applies", current: "abc123", event: "abc123", want: false},
		{name: "case insensitive match applies", current: "ABC123", event: "abc123", want: false},
		{name: "mismatched sha drops", current: "abc123", event: "def456", want: true},
		{name: "empty event sha never stale", current: "abc123", event: "", want: false},
		{name: "empty current sha never stale", current: "", event: "abc123", want: false},
		{name: "zero sha never stale", current: "abc123", event: "0000000", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsStaleEvent(tc.current, tc.event); got != tc.want {
				t.Fatalf("IsStaleEvent(%q, %q) = %v, want %v", tc.current, tc.event, got, tc.want)
			}
		})
	}
}
