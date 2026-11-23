package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		{name: "invalid chunk characters", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1!-012345", want: ""},
		{name: "valid with 17 digit target id", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-t12345678901234567-012345", want: "c1"},
		{name: "valid with 18 digit target id and refs heads", repo: "azylman/aerial", branch: "refs/heads/feat/aerial-pr-vanadis-chunk-1-router-t123456789012345678-a1b2c3", want: "chunk-1-router"},
		{name: "valid with 20 digit target id", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-t12345678901234567890-012345", want: "c1"},
		{name: "non snowflake target id segment kept as chunk id", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-t12345-012345", want: "c1-t12345"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ExtractChunkIDFromBranch(tc.repo, tc.branch))
		})
	}
}

func TestExtractTargetIDFromBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, branch, want string
	}{
		{name: "empty repo", repo: "", branch: "feat/aerial-pr-t123456789012345678-012345", want: ""},
		{name: "empty branch", repo: "aerial", branch: "", want: ""},
		{name: "standalone branch without target", repo: "aerial", branch: "feat/aerial-pr-20261116-123456", want: ""},
		{name: "standalone with 17 digit target", repo: "aerial", branch: "feat/aerial-pr-t12345678901234567-012345", want: "12345678901234567"},
		{name: "standalone with 18 digit target", repo: "aerial", branch: "feat/aerial-pr-t123456789012345678-012345", want: "123456789012345678"},
		{name: "standalone with 20 digit target", repo: "aerial", branch: "feat/aerial-pr-t12345678901234567890-012345", want: "12345678901234567890"},
		{name: "standalone with refs heads", repo: "azylman/aerial", branch: "refs/heads/feat/aerial-pr-t123456789012345678-012345", want: "123456789012345678"},
		{name: "vanadis branch with 18 digit target", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-t123456789012345678-012345", want: "123456789012345678"},
		{name: "vanadis branch without target", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-012345", want: ""},
		{name: "target too short 16 digits", repo: "aerial", branch: "feat/aerial-pr-t1234567890123456-012345", want: ""},
		{name: "target too long 21 digits", repo: "aerial", branch: "feat/aerial-pr-t123456789012345678901-012345", want: ""},
		{name: "non numeric target", repo: "aerial", branch: "feat/aerial-pr-t12345678901234567a-012345", want: ""},
		{name: "invalid suffix non-hex", repo: "aerial", branch: "feat/aerial-pr-t123456789012345678-12345g", want: ""},
		{name: "invalid suffix too short", repo: "aerial", branch: "feat/aerial-pr-t123456789012345678-12345", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ExtractTargetIDFromBranch(tc.repo, tc.branch))
		})
	}
}
