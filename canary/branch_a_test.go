package canary

import "testing"

func TestBranchA(t *testing.T) {
	t.Parallel()
	got := BranchA()
	if got != "branch-a" {
		t.Fatalf("BranchA() = %q, want %q", got, "branch-a")
	}
}
