package prwire

import "testing"

// TestCheckNameClassifiers pins IsRoboRevCheck, IsGateAggregatorCheck and
// CanonicalFailedCheckKey over one set of check names: all classify the same
// input.
func TestCheckNameClassifiers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		checkName   string
		wantRoboRev bool
		wantGate    bool
		wantKey     string
	}{
		{name: "roborev lowercase", checkName: "roborev", wantRoboRev: true, wantKey: "roborev"},
		{name: "roborev uppercase", checkName: "ROBOREV", wantRoboRev: true, wantKey: "roborev"},
		{name: "roborev long name", checkName: "RoboRev Code Review", wantRoboRev: true, wantKey: "roborev"},
		{name: "roborev long name any case", checkName: "roborev code review", wantRoboRev: true, wantKey: "roborev"},
		{name: "roborev surrounding spaces", checkName: "  roborev  ", wantRoboRev: true, wantKey: "roborev"},
		{name: "roborev substring is not exact", checkName: "roborev-lite", wantKey: "roborev-lite"},
		{name: "ci gate exact", checkName: "ci gate", wantGate: true, wantKey: "ci gate"},
		{name: "ci gate uppercase", checkName: "CI Gate", wantGate: true, wantKey: "ci gate"},
		{name: "pr gate suffix", checkName: "pr gate", wantGate: true, wantKey: "pr gate"},
		{name: "gate spaces trimmed", checkName: "  CI Gate  ", wantGate: true, wantKey: "ci gate"},
		{name: "gate prefix only", checkName: "gatekeeper", wantKey: "gatekeeper"},
		{name: "other check", checkName: "unit-tests", wantKey: "unit-tests"},
		{name: "empty", checkName: ""},
		{name: "blank", checkName: "  "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRoboRevCheck(tc.checkName); got != tc.wantRoboRev {
				t.Errorf("IsRoboRevCheck(%q) = %v, want %v", tc.checkName, got, tc.wantRoboRev)
			}
			if got := IsGateAggregatorCheck(tc.checkName); got != tc.wantGate {
				t.Errorf("IsGateAggregatorCheck(%q) = %v, want %v", tc.checkName, got, tc.wantGate)
			}
			if got := CanonicalFailedCheckKey(tc.checkName); got != tc.wantKey {
				t.Errorf("CanonicalFailedCheckKey(%q) = %q, want %q", tc.checkName, got, tc.wantKey)
			}
		})
	}
}

func TestClassifyCheckState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       string
	}{
		{name: "completed success", status: "completed", conclusion: "success", want: "passed"},
		{name: "completed neutral", status: "completed", conclusion: "neutral", want: "passed"},
		{name: "completed skipped", status: "completed", conclusion: "skipped", want: "passed"},
		{name: "completed failure", status: "completed", conclusion: "failure", want: "failed"},
		{name: "completed timed_out", status: "completed", conclusion: "timed_out", want: "failed"},
		{name: "completed cancelled", status: "completed", conclusion: "cancelled", want: "failed"},
		{name: "completed action_required", status: "completed", conclusion: "action_required", want: "failed"},
		{name: "case and spaces normalized", status: " Completed ", conclusion: " SUCCESS ", want: "passed"},
		{name: "completed unknown conclusion", status: "completed", conclusion: "stale", want: ""},
		{name: "queued status", status: "queued", want: "pending"},
		{name: "in_progress status", status: "in_progress", want: "pending"},
		{name: "waiting status", status: "waiting", want: "pending"},
		{name: "requested status", status: "requested", want: "pending"},
		{name: "pending status", status: "pending", want: "pending"},
		{name: "other non-empty status", status: "running", want: "pending"},
		{name: "empty status", status: "", conclusion: "success", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyCheckState(tc.status, tc.conclusion); got != tc.want {
				t.Errorf("ClassifyCheckState(%q, %q) = %q, want %q", tc.status, tc.conclusion, got, tc.want)
			}
		})
	}
}

func TestAutoMergeBlockReason(t *testing.T) {
	t.Parallel()
	no, yes := false, true
	tests := []struct {
		name      string
		state     string
		mergeable *bool
		want      string
	}{
		{name: "dirty state", state: "dirty", want: "merge_conflict"},
		{name: "mergeable false", state: "clean", mergeable: &no, want: "merge_conflict"},
		{name: "blocked state", state: "blocked", mergeable: &yes, want: "checks_or_reviews_blocked"},
		{name: "behind state", state: "Behind", mergeable: &yes, want: "branch_behind_base"},
		{name: "clean state", state: "clean", mergeable: &yes, want: ""},
		{name: "unstable state", state: "unstable", want: ""},
		{name: "has_hooks state", state: "has_hooks", want: ""},
		{name: "empty state", state: "", want: ""},
		{name: "other state passes through", state: " Draft ", mergeable: &yes, want: "draft"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AutoMergeBlockReason(tc.state, tc.mergeable); got != tc.want {
				t.Errorf("AutoMergeBlockReason(%q, %v) = %q, want %q", tc.state, tc.mergeable, got, tc.want)
			}
		})
	}
}

func TestIsActionableAutoMergeBlock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		reason string
		want   bool
	}{
		{name: "merge_conflict", reason: "merge_conflict", want: true},
		{name: "dirty", reason: " DIRTY ", want: true},
		{name: "contains conflict", reason: "has merge conflicts with main", want: true},
		{name: "branch_behind_base", reason: "branch_behind_base", want: true},
		{name: "checks_or_reviews_blocked", reason: "checks_or_reviews_blocked", want: false},
		{name: "empty", reason: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsActionableAutoMergeBlock(tc.reason); got != tc.want {
				t.Errorf("IsActionableAutoMergeBlock(%q) = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}
