package model

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
			assert.Equal(t, tc.want, NormalizeAgentName(tc.agent))
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
			assert.Equal(t, tc.wantBuild, BuildServiceName(tc.agent))
			assert.Equal(t, tc.wantRouter, BuildRouterServiceName(tc.agent))
		})
	}
}

func TestActionDispatchID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		id     string
		nudges int
		want   string
	}{
		{name: "initial generation", id: "c", nudges: 0, want: "vanadis-c-turn-0"},
		{name: "retry generation", id: "c", nudges: 2, want: "vanadis-c-turn-2"},
		{name: "hyphenated chunk", id: "chunk-1-router", nudges: 1, want: "vanadis-chunk-1-router-turn-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ActionDispatchID(VanadisChunk{ID: tc.id, StallNudges: tc.nudges}))
		})
	}
}

// TestCleanRepoNameAndNormalizeDiscordThreadID covers both string-to-string
// normalization helpers in one table; they share the exact same data contract.
func TestCleanRepoNameAndNormalizeDiscordThreadID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, wantRepo, thread, wantThread string
	}{
		{name: "bare values", repo: "aerial", wantRepo: "aerial", thread: "th-1", wantThread: "th-1"},
		{name: "prefixed values", repo: "azylman/aerial", wantRepo: "aerial", thread: "discord-abc", wantThread: "abc"},
		{name: "padded values", repo: "  azylman/aerial  ", wantRepo: "aerial", thread: "  discord-abc  ", wantThread: "abc"},
		{name: "namespaced thread", repo: "aerial", wantRepo: "aerial", thread: "guild:discord-xyz", wantThread: "xyz"},
		{name: "empty values", repo: "", wantRepo: "", thread: "", wantThread: ""},
		{name: "trailing slash repo", repo: "owner/", wantRepo: "", thread: "th-1", wantThread: "th-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantRepo, CleanRepoName(tc.repo))
			assert.Equal(t, tc.wantThread, NormalizeDiscordThreadID(tc.thread))
		})
	}
}

func TestExtractChunkIDFromBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, branch, want string
	}{
		{name: "empty repo", repo: "", branch: "feat/aerial-pr-vanadis-c1-012345", want: ""},
		{name: "empty branch", repo: "aerial", branch: "", want: ""},
		{name: "standalone branch without vanadis", repo: "aerial", branch: "feat/aerial-pr-20261116-123456", want: ""},
		{name: "valid with refs/heads", repo: "azylman/aerial", branch: "refs/heads/feat/aerial-pr-vanadis-chunk-1-router-a1b2c3", want: "chunk-1-router"},
		{name: "valid bare branch", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-012345", want: "c1"},
		{name: "valid uppercase hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-ABCDEF", want: "c1"},
		{name: "git suffix repo", repo: "aerial.git", branch: "feat/aerial-pr-vanadis-c1-012345", want: "c1"},
		{name: "repo containing delimiter and chunk containing delimiter", repo: "owner/demo-pr-vanadis-tools", branch: "feat/demo-pr-vanadis-tools-pr-vanadis-c1-pr-vanadis-2-a1b2c3", want: "c1-pr-vanadis-2"},
		{name: "invalid suffix too short", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-12345", want: ""},
		{name: "invalid suffix too long", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-1234567", want: ""},
		{name: "invalid suffix non-hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-12345g", want: ""},
		{name: "no separator after vanadis", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1", want: ""},
		{name: "empty chunk id", repo: "aerial", branch: "feat/aerial-pr-vanadis--123456", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ExtractChunkIDFromBranch(tc.repo, tc.branch))
		})
	}
}

func TestIsValidVanadisChunkID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		id    string
		valid bool
	}{
		{name: "simple", id: "abc", valid: true},
		{name: "complex", id: "Abc-123_XZ", valid: true},
		{name: "empty", id: "", valid: false},
		{name: "blank", id: "   ", valid: false},
		{name: "too long", id: strings.Repeat("a", 121), valid: false},
		{name: "max length", id: strings.Repeat("a", 120), valid: true},
		{name: "slash", id: "a/b", valid: false},
		{name: "space", id: "a b", valid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.valid, IsValidVanadisChunkID(tc.id))
		})
	}
}

func TestGenerateBuildID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, wantPrefix string
	}{
		{name: "build prefix", wantPrefix: "build-"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := GenerateBuildID()
			assert.True(t, strings.HasPrefix(id, tc.wantPrefix), "id %q missing prefix %q", id, tc.wantPrefix)
			parts := strings.Split(id, "-")
			assert.Len(t, parts, 3, "id %q must match build-<unix>-<8hex>", id)
			assert.Len(t, parts[2], 8, "id %q must carry an 8-char hex suffix", id)
			assert.NotEqual(t, id, GenerateBuildID(), "duplicate build id %q", id)
		})
	}
}

func TestNormalizeRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, want string
	}{
		{name: "bare repo", repo: "aerial", want: "aerial"},
		{name: "padded repo", repo: "  azylman/aerial  ", want: "azylman/aerial"},
		{name: "empty", repo: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, NormalizeRepo(tc.repo))
		})
	}
}

// TestNormalizeChunkModeAndEffort covers both validators in one table; they
// share the exact same data contract of raw string to normalized string.
func TestNormalizeChunkModeAndEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, rawMode, wantMode, rawEffort, wantEffort string
		wantModeErr, wantEffortErr                     bool
	}{
		{name: "defaults", rawMode: "", wantMode: "pr", rawEffort: "", wantEffort: "medium"},
		{name: "canonical values", rawMode: "pr", wantMode: "pr", rawEffort: "medium", wantEffort: "medium"},
		{name: "uppercase values", rawMode: "PR", wantMode: "pr", rawEffort: "LOW", wantEffort: "low"},
		{name: "action and low", rawMode: "action", wantMode: "action", rawEffort: "low", wantEffort: "low"},
		{name: "mixed case and high", rawMode: "Action", wantMode: "action", rawEffort: "high", wantEffort: "medium"},
		{name: "invalid values", rawMode: "bogus", wantModeErr: true, rawEffort: "bogus", wantEffortErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotMode, modeErr := NormalizeChunkMode(tc.rawMode)
			assert.Equal(t, tc.wantModeErr, modeErr != nil)
			assert.Equal(t, tc.wantMode, gotMode)
			gotEffort, effortErr := NormalizeChunkEffort(tc.rawEffort)
			assert.Equal(t, tc.wantEffortErr, effortErr != nil)
			assert.Equal(t, tc.wantEffort, gotEffort)
		})
	}
}

func TestValidateChunkModeAndRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		mode, repo     string
		files, symbols []string
		wantErr        bool
	}{
		{name: "pr valid", mode: "pr", repo: "aerial", wantErr: false},
		{name: "default mode with repo", mode: "", repo: "aerial", wantErr: false},
		{name: "pr missing repo", mode: "pr", repo: "   ", wantErr: true},
		{name: "invalid mode", mode: "bogus", repo: "aerial", wantErr: true},
		{name: "action clean", mode: "action", repo: "", wantErr: false},
		{name: "action with repo", mode: "action", repo: "aerial", wantErr: true},
		{name: "action with files", mode: "action", files: []string{"f.go"}, wantErr: true},
		{name: "action with symbols", mode: "action", symbols: []string{"Sym"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantErr, ValidateChunkModeAndRepo(0, tc.mode, tc.repo, tc.files, tc.symbols) != nil)
		})
	}
}
