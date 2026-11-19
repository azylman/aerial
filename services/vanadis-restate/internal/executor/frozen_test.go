package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractChunkIDFromBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		repo   string
		branch string
		want   string
	}{
		{name: "empty repo", repo: "", branch: "feat/aerial-pr-vanadis-c1-012345", want: ""},
		{name: "empty branch", repo: "aerial", branch: "", want: ""},
		{name: "standalone branch without vanadis", repo: "aerial", branch: "feat/aerial-pr-20261116-123456", want: ""},
		{name: "valid with refs/heads", repo: "azylman/aerial", branch: "refs/heads/feat/aerial-pr-vanadis-chunk-1-router-a1b2c3", want: "chunk-1-router"},
		{name: "valid bare branch", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-012345", want: "c1"},
		{name: "valid uppercase hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-ABCDEF", want: "c1"},
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
			assert.Equal(t, tc.want, extractChunkIDFromBranch(tc.repo, tc.branch))
		})
	}
}
