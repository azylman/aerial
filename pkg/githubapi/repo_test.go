package githubapi

import "testing"

func TestQualifyRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		owner string
		repo  string
		want  string
	}{
		{name: "bare repo gets owner", owner: "azylman", repo: "aerial", want: "azylman/aerial"},
		{name: "qualified repo unchanged", owner: "azylman", repo: "x/y", want: "x/y"},
		{name: "empty owner keeps bare repo", owner: "", repo: "aerial", want: "aerial"},
		{name: "blank owner keeps bare repo", owner: " / ", repo: "aerial", want: "aerial"},
		{name: "empty repo", owner: "azylman", repo: "", want: ""},
		{name: "blank repo", owner: "azylman", repo: "  / ", want: ""},
		{name: "spaces and slashes trimmed", owner: " acme/ ", repo: " /aerial/ ", want: "acme/aerial"},
		{name: "qualified repo slashes trimmed", owner: "acme", repo: "/x/y/", want: "x/y"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := QualifyRepo(tc.owner, tc.repo); got != tc.want {
				t.Errorf("QualifyRepo(%q, %q) = %q, want %q", tc.owner, tc.repo, got, tc.want)
			}
		})
	}
}
