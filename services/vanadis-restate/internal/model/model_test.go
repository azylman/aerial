package model

import "testing"

func TestNormalizeAgentName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, agent, want string
	}{
		{"empty stays empty", "", ""},
		{"trims whitespace", "  suletta  ", "suletta"},
		{"lowercases", "Suletta", "suletta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeAgentName(tc.agent); got != tc.want {
				t.Fatalf("NormalizeAgentName(%q) = %q, want %q", tc.agent, got, tc.want)
			}
		})
	}
}

func TestServiceNameDerivation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, agent, wantBuild, wantRouter string
	}{
		{"empty defaults to legacy", "", "Build", "BuildRouter"},
		{"aerial defaults to legacy", "aerial", "Build", "BuildRouter"},
		{"aerial case insensitive", "Aerial", "Build", "BuildRouter"},
		{"suletta scoped", "suletta", "Build-suletta", "BuildRouter-suletta"},
		{"miorine scoped", "miorine", "Build-miorine", "BuildRouter-miorine"},
		{"whitespace trimmed", "  suletta  ", "Build-suletta", "BuildRouter-suletta"},
		{"mixed case scoped normalized", "Suletta", "Build-suletta", "BuildRouter-suletta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := BuildServiceName(tc.agent); got != tc.wantBuild {
				t.Fatalf("BuildServiceName(%q) = %q, want %q", tc.agent, got, tc.wantBuild)
			}
			if got := BuildRouterServiceName(tc.agent); got != tc.wantRouter {
				t.Fatalf("BuildRouterServiceName(%q) = %q, want %q", tc.agent, got, tc.wantRouter)
			}
		})
	}
}
