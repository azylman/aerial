package prwire

import "testing"

func TestPRObjectKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		repo     string
		prNumber int
		want     string
	}{
		{name: "qualified repo", repo: "acme/aerial", prNumber: 42, want: "acme/aerial#42"},
		{name: "spaces trimmed", repo: "  acme/aerial  ", prNumber: 7, want: "acme/aerial#7"},
		{name: "zero pr", repo: "acme/aerial", prNumber: 0, want: "acme/aerial#0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := PRObjectKey(tc.repo, tc.prNumber); got != tc.want {
				t.Errorf("PRObjectKey(%q, %d) = %q, want %q", tc.repo, tc.prNumber, got, tc.want)
			}
		})
	}
}

func TestRepoObjectKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		repo string
		want string
	}{
		{name: "qualified repo", repo: "acme/aerial", want: "acme/aerial"},
		{name: "spaces trimmed", repo: " acme/aerial ", want: "acme/aerial"},
		{name: "empty", repo: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RepoObjectKey(tc.repo); got != tc.want {
				t.Errorf("RepoObjectKey(%q) = %q, want %q", tc.repo, got, tc.want)
			}
		})
	}
}
