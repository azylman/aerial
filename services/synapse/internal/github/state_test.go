package github

import (
	"testing"
)

func TestPRCheckKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		repo     string
		prNumber int
		want     string
	}{
		{name: "standard full repo", repo: "azylman/aerial", prNumber: 42, want: "azylman/aerial#42"},
		{name: "short repo name normalized", repo: "aerial", prNumber: 10, want: "azylman/aerial#10"},
		{name: "with surrounding whitespace", repo: "  mirrormere  ", prNumber: 1, want: "azylman/mirrormere#1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := PRCheckKey(tc.repo, tc.prNumber)
			if got != tc.want {
				t.Errorf("PRCheckKey(%q, %d) = %q, want %q", tc.repo, tc.prNumber, got, tc.want)
			}
		})
	}
}

func TestCanonicalFailedCheckKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		checkName string
		want      string
	}{
		{name: "roborev standard", checkName: "roborev", want: "roborev"},
		{name: "roborev long name", checkName: "RoboRev Code Review", want: "roborev"},
		{name: "other check uppercase", checkName: "Build-Docker-Image", want: "build-docker-image"},
		{name: "whitespace padded", checkName: "  test / unit  ", want: "test / unit"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CanonicalFailedCheckKey(tc.checkName)
			if got != tc.want {
				t.Errorf("CanonicalFailedCheckKey(%q) = %q, want %q", tc.checkName, got, tc.want)
			}
		})
	}
}

func TestStateTracker_NilSafety_Basic(t *testing.T) {
	t.Parallel()
	var tr *StateTracker

	if tr.RecordFailedCISHA("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.HasFailedCISHA("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if _, ok := tr.GetFailedCISHA("repo", 1); ok {
		t.Errorf("expected false on nil tracker")
	}
	tr.ClearFailedCISHA("repo", 1)
	tr.RecordNonRoboRevFailedSHA("repo", 1, "sha")
	tr.DeleteNonRoboRevFailedSHA("repo", 1)
	if tr.HasNonRoboRevFailedSHA("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if _, ok := tr.GetNonRoboRevFailedSHA("repo", 1); ok {
		t.Errorf("expected false on nil tracker")
	}
	tr.RecordCancelledCheckSHA("repo", 1, "sha")
	tr.DeleteCancelledCheckSHA("repo", 1)
	if tr.HasCancelledCheckSHA("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.IsCachedFailedCISHA("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
}

func TestStateTracker_NilSafety_CollaboratorAndMain(t *testing.T) {
	t.Parallel()
	var tr *StateTracker

	emit, active, prior := tr.RecordFailedCheckRunDetailed("repo", 1, "sha", "check")
	if emit || active || prior {
		t.Errorf("expected all false on nil tracker")
	}
	tr.RollbackFailedCheckRun("repo", 1, "sha", "check", false, false)
	tr.ClearActiveFailedCheckRun("repo", 1, "sha", "check")
	if tr.ClearFailedCheckRun("repo", 1, "sha", "check") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.HasActiveFailedCheckRuns("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.HasActiveNonRoboRevFailures("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.HasActiveRoboRevFailure("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.CanRecoverNonGateCheckSuccess("repo", 1, "check", "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.MarkCollaboratorAlerted("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	tr.ClearCollaboratorAlerted("repo", 1, "sha")
	if tr.MarkCollaboratorConflict("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	tr.ClearCollaboratorConflict("repo", 1, "sha")
	if tr.RecordMainPushSHA("repo", "sha") {
		t.Errorf("expected false on nil tracker")
	}
	if tr.GetMainPushSHA("repo") != "" {
		t.Errorf("expected empty on nil tracker")
	}
	tr.MarkUpdatedForMain("repo", 1)
	if tr.WasUpdatedForMain("repo", 1) {
		t.Errorf("expected false on nil tracker")
	}
	tr.ClearUpdatedForMain("repo", 1)
	tr.ClearUpdatedPRsForRepo("repo")
	tr.ClearUpdatedPRsForMainCommit("repo", "sha")
	tr.MarkConflictPublished("repo", 1, "sha")
	if tr.WasConflictPublished("repo", 1, "sha") {
		t.Errorf("expected false on nil tracker")
	}
	tr.ClearConflictPublished("repo", 1)
	tr.RecordCommitSHAPR("repo", "sha", 1, "main")
	if _, _, ok := tr.LookupCommitSHAPR("repo", "sha"); ok {
		t.Errorf("expected false on nil tracker")
	}
	tr.StoreRawCommitSHAPRForTest("key", "val")
}

func TestStateTracker_FailedCISHA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repo       string
		prNumber   int
		sha        string
		wantRecord bool
	}{
		{name: "empty repo", repo: "", prNumber: 1, sha: "sha1", wantRecord: false},
		{name: "zero pr number", repo: "azylman/aerial", prNumber: 0, sha: "sha1", wantRecord: false},
		{name: "negative pr number", repo: "azylman/aerial", prNumber: -1, sha: "sha1", wantRecord: false},
		{name: "empty sha", repo: "azylman/aerial", prNumber: 1, sha: "", wantRecord: false},
		{name: "zero sha", repo: "azylman/aerial", prNumber: 1, sha: "0000000000000000000000000000000000000000", wantRecord: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := &StateTracker{}
			got := tr.RecordFailedCISHA(tc.repo, tc.prNumber, tc.sha)
			if got != tc.wantRecord {
				t.Errorf("RecordFailedCISHA() = %v, want %v", got, tc.wantRecord)
			}
		})
	}

	tr := &StateTracker{}
	if !tr.RecordFailedCISHA("azylman/aerial", 42, "sha42a") {
		t.Errorf("expected initial record to be true")
	}
	if tr.RecordFailedCISHA("azylman/aerial", 42, "sha42a") {
		t.Errorf("expected duplicate record to be false")
	}
	if !tr.RecordFailedCISHA("azylman/aerial", 42, "sha42b") {
		t.Errorf("expected changed record to be true")
	}

	if !tr.HasFailedCISHA("azylman/aerial", 42, "sha42b") {
		t.Errorf("expected HasFailedCISHA to be true for recorded SHA")
	}
	if !tr.HasFailedCISHA("azylman/aerial", 42, "") {
		t.Errorf("expected HasFailedCISHA with empty SHA check to be true")
	}
	if tr.HasFailedCISHA("azylman/aerial", 42, "other") {
		t.Errorf("expected HasFailedCISHA to be false for different SHA")
	}
	if tr.HasFailedCISHA("azylman/aerial", -1, "sha42b") {
		t.Errorf("expected false for invalid pr number in HasFailedCISHA")
	}
	if tr.HasFailedCISHA("azylman/aerial", 999, "sha42b") {
		t.Errorf("expected false for untracked PR in HasFailedCISHA")
	}
	if sha, ok := tr.GetFailedCISHA("azylman/aerial", 42); !ok || sha != "sha42b" {
		t.Errorf("GetFailedCISHA() = (%q, %v), want (sha42b, true)", sha, ok)
	}
	if _, ok := tr.GetFailedCISHA("azylman/aerial", 0); ok {
		t.Errorf("expected false for invalid pr in GetFailedCISHA")
	}
	if _, ok := tr.GetFailedCISHA("azylman/aerial", 999); ok {
		t.Errorf("expected false for missing PR in GetFailedCISHA")
	}

	tr.ClearFailedCISHA("", 42)
	tr.ClearFailedCISHA("azylman/aerial", 0)
	tr.ClearFailedCISHA("azylman/aerial", 42)
	if tr.HasFailedCISHA("azylman/aerial", 42, "") {
		t.Errorf("expected HasFailedCISHA false after ClearFailedCISHA")
	}
}

func TestStateTracker_NonRoboRevFailedSHA(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.RecordNonRoboRevFailedSHA("", 1, "sha")
	tr.RecordNonRoboRevFailedSHA("azylman/aerial", 0, "sha")
	tr.RecordNonRoboRevFailedSHA("azylman/aerial", 1, "")
	tr.RecordNonRoboRevFailedSHA("azylman/aerial", 1, "0000000000000000000000000000000000000000")
	if tr.HasNonRoboRevFailedSHA("azylman/aerial", 1, "") {
		t.Errorf("expected false for invalid inputs")
	}
	if tr.HasNonRoboRevFailedSHA("azylman/aerial", -1, "") {
		t.Errorf("expected false for negative pr")
	}
	if _, ok := tr.GetNonRoboRevFailedSHA("azylman/aerial", 0); ok {
		t.Errorf("expected false for zero pr")
	}
	if _, ok := tr.GetNonRoboRevFailedSHA("azylman/aerial", 1); ok {
		t.Errorf("expected false for untracked pr")
	}

	tr.RecordNonRoboRevFailedSHA("azylman/aerial", 50, "head50")
	if !tr.HasNonRoboRevFailedSHA("azylman/aerial", 50, "head50") {
		t.Errorf("expected true for recorded non-roborev SHA")
	}
	if !tr.HasNonRoboRevFailedSHA("azylman/aerial", 50, "") {
		t.Errorf("expected true for empty check")
	}
	if tr.HasNonRoboRevFailedSHA("azylman/aerial", 50, "other") {
		t.Errorf("expected false for mismatched SHA")
	}
	if got, ok := tr.GetNonRoboRevFailedSHA("azylman/aerial", 50); !ok || got != "head50" {
		t.Errorf("GetNonRoboRevFailedSHA() = (%q, %v), want (head50, true)", got, ok)
	}

	tr.DeleteNonRoboRevFailedSHA("azylman/aerial", 0)
	tr.DeleteNonRoboRevFailedSHA("azylman/aerial", 50)
	if tr.HasNonRoboRevFailedSHA("azylman/aerial", 50, "") {
		t.Errorf("expected false after delete")
	}
}

func TestStateTracker_CancelledCheckSHA(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.RecordCancelledCheckSHA("", 1, "sha")
	tr.RecordCancelledCheckSHA("azylman/aerial", 0, "sha")
	tr.RecordCancelledCheckSHA("azylman/aerial", 1, "")
	if tr.HasCancelledCheckSHA("azylman/aerial", 1, "sha") {
		t.Errorf("expected false for invalid inputs")
	}
	if tr.HasCancelledCheckSHA("azylman/aerial", 0, "sha") {
		t.Errorf("expected false for zero pr")
	}
	if tr.HasCancelledCheckSHA("azylman/aerial", 1, "") {
		t.Errorf("expected false for empty sha")
	}

	tr.RecordCancelledCheckSHA("azylman/aerial", 60, "head60")
	if !tr.HasCancelledCheckSHA("azylman/aerial", 60, "head60") {
		t.Errorf("expected true for recorded cancelled check")
	}
	if tr.HasCancelledCheckSHA("azylman/aerial", 60, "mismatched") {
		t.Errorf("expected false for mismatched SHA")
	}

	tr.DeleteCancelledCheckSHA("azylman/aerial", 0)
	tr.DeleteCancelledCheckSHA("azylman/aerial", 60)
	if tr.HasCancelledCheckSHA("azylman/aerial", 60, "head60") {
		t.Errorf("expected false after delete")
	}
}

func TestStateTracker_IsCachedFailedCISHA(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	if tr.IsCachedFailedCISHA("", 1, "sha") || tr.IsCachedFailedCISHA("repo", 0, "sha") || tr.IsCachedFailedCISHA("repo", 1, "") {
		t.Errorf("expected false for invalid parameters")
	}

	tr.RecordFailedCISHA("azylman/aerial", 10, "sha-fail")
	tr.RecordNonRoboRevFailedSHA("azylman/aerial", 11, "sha-nonroborev")
	tr.RecordCancelledCheckSHA("azylman/aerial", 12, "sha-cancelled")

	tests := []struct {
		name     string
		repo     string
		prNumber int
		sha      string
		want     bool
	}{
		{name: "failedCheckSHAs hit", repo: "azylman/aerial", prNumber: 10, sha: "sha-fail", want: true},
		{name: "nonRoboRevFailedSHAs hit", repo: "azylman/aerial", prNumber: 11, sha: "sha-nonroborev", want: true},
		{name: "cancelledCheckSHAs hit", repo: "azylman/aerial", prNumber: 12, sha: "sha-cancelled", want: true},
		{name: "miss on sha", repo: "azylman/aerial", prNumber: 10, sha: "other", want: false},
		{name: "miss on pr", repo: "azylman/aerial", prNumber: 99, sha: "sha-fail", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tr.IsCachedFailedCISHA(tc.repo, tc.prNumber, tc.sha)
			if got != tc.want {
				t.Errorf("IsCachedFailedCISHA() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStateTracker_FailedCheckRunDetailed(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	// Invalid input guards
	guards := []struct {
		repo     string
		prNumber int
		sha      string
		check    string
	}{
		{"", 1, "sha", "check"},
		{"repo", 0, "sha", "check"},
		{"repo", 1, "", "check"},
		{"repo", 1, "sha", ""},
	}
	for _, g := range guards {
		emit, active, prior := tr.RecordFailedCheckRunDetailed(g.repo, g.prNumber, g.sha, g.check)
		if emit || active || prior {
			t.Errorf("expected all false for guard %+v", g)
		}
	}

	// First failure on unit tests
	emit, active, prior := tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "test / unit")
	if emit || active || prior {
		t.Errorf("first failure should have emit=false, active=false, prior=false; got (%v, %v, %v)", emit, active, prior)
	}

	// Second failure on lint check -> emitAdditional should be true
	emit, active, prior = tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "lint")
	if !emit || active || prior {
		t.Errorf("second failure should emit additional (emit=true, active=false, prior=false); got (%v, %v, %v)", emit, active, prior)
	}

	// Duplicate failure on lint check -> wasActive should be true
	emit, active, prior = tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "lint")
	if emit || !active || !prior {
		t.Errorf("duplicate failure should have active=true, prior=true; got (%v, %v, %v)", emit, active, prior)
	}
}

func TestStateTracker_FailedCheckRunQueries_Active(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "test / unit")

	if !tr.HasActiveFailedCheckRuns("azylman/aerial", 70, "sha70") {
		t.Errorf("expected active failures after recording check failure")
	}
	if tr.HasActiveFailedCheckRuns("azylman/aerial", 70, "other-sha") {
		t.Errorf("expected false for other sha in HasActiveFailedCheckRuns")
	}
	if tr.HasActiveFailedCheckRuns("azylman/aerial", 0, "sha70") {
		t.Errorf("expected false for zero pr in HasActiveFailedCheckRuns")
	}
	if tr.HasActiveFailedCheckRuns("azylman/aerial", 999, "sha70") {
		t.Errorf("expected false for missing pr in HasActiveFailedCheckRuns")
	}

	if !tr.HasActiveNonRoboRevFailures("azylman/aerial", 70, "sha70") {
		t.Errorf("expected active non-roborev failure")
	}
	if !tr.HasActiveCheckRun("azylman/aerial", 70, "sha70", "test / unit") {
		t.Errorf("expected HasActiveCheckRun true for test / unit")
	}
	if tr.HasActiveCheckRun("azylman/aerial", 70, "sha70", "lint") {
		t.Errorf("expected HasActiveCheckRun false for lint before recording")
	}
	if !tr.HasEverFailedCheckRun("azylman/aerial", 70, "sha70", "test / unit") {
		t.Errorf("expected HasEverFailedCheckRun true for test / unit")
	}
	if tr.HasActiveCheckRun("azylman/aerial", 0, "sha70", "test / unit") || tr.HasActiveCheckRun("azylman/aerial", 70, "", "test / unit") || tr.HasActiveCheckRun("azylman/aerial", 70, "sha70", "") {
		t.Errorf("expected false for invalid inputs in HasActiveCheckRun")
	}
	if tr.HasEverFailedCheckRun("azylman/aerial", 0, "sha70", "test / unit") || tr.HasEverFailedCheckRun("azylman/aerial", 70, "", "test / unit") || tr.HasEverFailedCheckRun("azylman/aerial", 70, "sha70", "") {
		t.Errorf("expected false for invalid inputs in HasEverFailedCheckRun")
	}
	if tr.HasActiveNonRoboRevFailures("azylman/aerial", 70, "other-sha") {
		t.Errorf("expected false for other sha in HasActiveNonRoboRevFailures")
	}
	if tr.HasActiveNonRoboRevFailures("azylman/aerial", 0, "sha70") {
		t.Errorf("expected false for zero pr in HasActiveNonRoboRevFailures")
	}
}

func TestStateTracker_FailedCheckRunQueries_RoboRev(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	if tr.HasActiveRoboRevFailure("azylman/aerial", 70, "sha70") {
		t.Errorf("expected no active roborev failure")
	}
	if tr.HasActiveRoboRevFailure("azylman/aerial", 70, "other-sha") {
		t.Errorf("expected false for other sha in HasActiveRoboRevFailure")
	}
	if tr.HasActiveRoboRevFailure("azylman/aerial", 0, "sha70") {
		t.Errorf("expected false for zero pr in HasActiveRoboRevFailure")
	}

	// RoboRev check run queries
	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "roborev")
	if !tr.HasActiveCheckRun("azylman/aerial", 70, "sha70", "roborev") {
		t.Errorf("expected HasActiveCheckRun true for roborev")
	}
	if !tr.HasActiveRoboRevFailure("azylman/aerial", 70, "sha70") {
		t.Errorf("expected HasActiveRoboRevFailure true for roborev")
	}
}

func TestStateTracker_FailedCheckRunQueries_Mismatched(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}
	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "test / unit")
	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "roborev")

	// Mismatched SHA queries
	if tr.HasActiveCheckRun("azylman/aerial", 70, "mismatched", "test / unit") {
		t.Errorf("expected false on mismatched SHA for HasActiveCheckRun")
	}
	if tr.HasEverFailedCheckRun("azylman/aerial", 70, "mismatched", "test / unit") {
		t.Errorf("expected false on mismatched SHA for HasEverFailedCheckRun")
	}
	if tr.HasActiveRoboRevFailure("azylman/aerial", 70, "mismatched") {
		t.Errorf("expected false on mismatched SHA for HasActiveRoboRevFailure")
	}
	if tr.HasActiveFailedCheckRuns("azylman/aerial", 70, "mismatched") {
		t.Errorf("expected false on mismatched SHA for HasActiveFailedCheckRuns")
	}
	if tr.HasActiveNonRoboRevFailures("azylman/aerial", 70, "mismatched") {
		t.Errorf("expected false on mismatched SHA for HasActiveNonRoboRevFailures")
	}
}

func TestStateTracker_Rollback(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "test / unit")
	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "lint")

	// Rollback guards & rollback lint failure
	tr.RollbackFailedCheckRun("azylman/aerial", 0, "sha70", "lint", false, false)
	tr.RollbackFailedCheckRun("azylman/aerial", 70, "sha70", "", false, false)
	tr.RollbackFailedCheckRun("azylman/aerial", 70, "sha70", "lint", true, false)
	tr.RollbackFailedCheckRun("azylman/aerial", 999, "sha70", "lint", false, false)
	tr.RollbackFailedCheckRun("azylman/aerial", 70, "other-sha", "lint", false, false)
	tr.RollbackFailedCheckRun("azylman/aerial", 70, "sha70", "lint", false, false)

	// RoboRev failure recording
	emit, active, prior := tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "roborev")
	if !emit || active || prior {
		t.Errorf("roborev failure when unit already failed should emit additional; got (%v, %v, %v)", emit, active, prior)
	}
	if !tr.HasActiveRoboRevFailure("azylman/aerial", 70, "sha70") {
		t.Errorf("expected active roborev failure")
	}

	// Rollback roborev check with prior failure preserved
	tr.RollbackFailedCheckRun("azylman/aerial", 70, "sha70", "roborev", false, true)
	// Re-record roborev
	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "roborev")

	// Clearing unit check: roborev is still active, so canRecover should be false
	canRecover := tr.CanRecoverNonGateCheckSuccess("azylman/aerial", 70, "test / unit", "sha70")
	if canRecover {
		t.Errorf("expected canRecover=false while roborev is still active")
	}
}

func TestStateTracker_RecoveryAndClear(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "test / unit")
	tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha70", "roborev")

	// ClearActiveFailedCheckRun guards and branches on separate tracker
	trActive := &StateTracker{}
	trActive.RecordFailedCheckRunDetailed("azylman/aerial", 72, "sha72", "roborev")
	trActive.RecordFailedCheckRunDetailed("azylman/aerial", 72, "sha72", "test / unit")
	trActive.ClearActiveFailedCheckRun("azylman/aerial", 72, "sha72", "")
	trActive.ClearActiveFailedCheckRun("azylman/aerial", 999, "sha72", "roborev")
	trActive.ClearActiveFailedCheckRun("azylman/aerial", 72, "sha72", "test / unit")
	trActive.ClearActiveFailedCheckRun("azylman/aerial", 72, "sha72", "roborev")
	if trActive.HasActiveCheckRun("azylman/aerial", 72, "sha72", "test / unit") || trActive.HasActiveRoboRevFailure("azylman/aerial", 72, "sha72") {
		t.Errorf("expected all active checks cleared in trActive")
	}

	// CanRecoverNonGateCheckSuccess when other active failures remain (returns false)
	trMulti := &StateTracker{}
	trMulti.RecordFailedCheckRunDetailed("azylman/aerial", 75, "sha75", "test / unit")
	trMulti.RecordFailedCheckRunDetailed("azylman/aerial", 75, "sha75", "lint")
	if trMulti.CanRecoverNonGateCheckSuccess("azylman/aerial", 75, "test / unit", "sha75") {
		t.Errorf("expected canRecover=false while lint is still active")
	}

	// ClearFailedCheckRun guards
	if tr.ClearFailedCheckRun("azylman/aerial", 70, "sha70", "") {
		t.Errorf("expected false for empty check in ClearFailedCheckRun")
	}
	if tr.ClearFailedCheckRun("azylman/aerial", 999, "sha70", "roborev") {
		t.Errorf("expected false for missing pr in ClearFailedCheckRun")
	}
	if tr.ClearFailedCheckRun("azylman/aerial", 70, "other-sha", "roborev") {
		t.Errorf("expected false for mismatched sha in ClearFailedCheckRun")
	}

	// Clear roborev failure
	if !tr.ClearFailedCheckRun("azylman/aerial", 70, "sha70", "roborev") {
		t.Errorf("expected ClearFailedCheckRun to return true for active roborev")
	}
	if tr.HasActiveRoboRevFailure("azylman/aerial", 70, "sha70") {
		t.Errorf("expected roborev active failure cleared")
	}

	// CanRecoverNonGateCheckSuccess guards
	if tr.CanRecoverNonGateCheckSuccess("azylman/aerial", 70, "", "sha70") {
		t.Errorf("expected false for empty checkName")
	}
	if tr.CanRecoverNonGateCheckSuccess("azylman/aerial", 999, "test / unit", "sha70") {
		t.Errorf("expected false for missing entry")
	}
	if tr.CanRecoverNonGateCheckSuccess("azylman/aerial", 70, "test / unit", "other-sha") {
		t.Errorf("expected false for mismatched sha")
	}
	if tr.CanRecoverNonGateCheckSuccess("azylman/aerial", 70, "never-failed-check", "sha70") {
		t.Errorf("expected false for never-failed check")
	}

	// New commit resets state
	emit, active, prior := tr.RecordFailedCheckRunDetailed("azylman/aerial", 70, "sha71", "test / unit")
	if emit || active || prior {
		t.Errorf("new commit should reset state; got (%v, %v, %v)", emit, active, prior)
	}
	// CanRecoverNonGateCheckSuccess on new commit clears failure and returns true
	canRecover := tr.CanRecoverNonGateCheckSuccess("azylman/aerial", 70, "test / unit", "sha71")
	if !canRecover {
		t.Errorf("expected canRecover=true when only failure is cleared")
	}
}

func TestStateTracker_CollaboratorAlerts(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	// Guards
	if tr.MarkCollaboratorAlerted("", 1, "sha") || tr.MarkCollaboratorAlerted("repo", 0, "sha") || tr.MarkCollaboratorAlerted("repo", 1, "") {
		t.Errorf("expected false for invalid inputs in MarkCollaboratorAlerted")
	}
	tr.ClearCollaboratorAlerted("repo", 0, "sha")

	// Collaborator CI alert
	if !tr.MarkCollaboratorAlerted("azylman/aerial", 80, "sha80a") {
		t.Errorf("first MarkCollaboratorAlerted should return true")
	}
	if tr.MarkCollaboratorAlerted("azylman/aerial", 80, "sha80a") {
		t.Errorf("second MarkCollaboratorAlerted with same sha should return false")
	}
	if !tr.MarkCollaboratorAlerted("azylman/aerial", 80, "sha80b") {
		t.Errorf("MarkCollaboratorAlerted with new sha should return true")
	}
	tr.ClearCollaboratorAlerted("azylman/aerial", 80, "")
	if !tr.MarkCollaboratorAlerted("azylman/aerial", 80, "sha80b") {
		t.Errorf("MarkCollaboratorAlerted after full clear should return true")
	}
	tr.ClearCollaboratorAlerted("azylman/aerial", 80, "sha80b")

	// Collaborator conflict alert
	if tr.MarkCollaboratorConflict("", 1, "sha") || tr.MarkCollaboratorConflict("repo", 0, "sha") || tr.MarkCollaboratorConflict("repo", 1, "") {
		t.Errorf("expected false for invalid inputs in MarkCollaboratorConflict")
	}
	tr.ClearCollaboratorConflict("repo", 0, "sha")

	if !tr.MarkCollaboratorConflict("azylman/aerial", 81, "sha81") {
		t.Errorf("first MarkCollaboratorConflict should return true")
	}
	if tr.MarkCollaboratorConflict("azylman/aerial", 81, "sha81") {
		t.Errorf("duplicate MarkCollaboratorConflict should return false")
	}
	tr.ClearCollaboratorConflict("azylman/aerial", 81, "sha81")
	if !tr.MarkCollaboratorConflict("azylman/aerial", 81, "sha81") {
		t.Errorf("MarkCollaboratorConflict after clear should return true")
	}
	tr.ClearCollaboratorConflict("azylman/aerial", 81, "")
}

func TestStateTracker_MainPushAndUpdatedForMain(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	if tr.RecordMainPushSHA("", "sha") || tr.RecordMainPushSHA("repo", "") || tr.RecordMainPushSHA("repo", "0000000000000000000000000000000000000000") {
		t.Errorf("expected false for invalid inputs in RecordMainPushSHA")
	}
	if tr.GetMainPushSHA("missing-repo") != "" {
		t.Errorf("expected empty string for missing repo in GetMainPushSHA")
	}

	if !tr.RecordMainPushSHA("azylman/aerial", "mainsha1") {
		t.Errorf("first RecordMainPushSHA should return true")
	}
	if tr.RecordMainPushSHA("azylman/aerial", "mainsha1") {
		t.Errorf("duplicate RecordMainPushSHA should return false")
	}
	if got := tr.GetMainPushSHA("azylman/aerial"); got != "mainsha1" {
		t.Errorf("GetMainPushSHA() = %q, want mainsha1", got)
	}

	tr.MarkUpdatedForMain("azylman/aerial", 0)
	tr.MarkUpdatedForMain("azylman/aerial", 90)
	tr.MarkUpdatedForMain("azylman/aerial", 91)
	if tr.WasUpdatedForMain("azylman/aerial", 0) {
		t.Errorf("expected false for zero pr in WasUpdatedForMain")
	}
	if tr.WasUpdatedForMain("azylman/aerial", 999) {
		t.Errorf("expected false for missing pr in WasUpdatedForMain")
	}
	if !tr.WasUpdatedForMain("azylman/aerial", 90) {
		t.Errorf("expected WasUpdatedForMain to be true")
	}

	tr.ClearUpdatedForMain("azylman/aerial", 0)
	tr.ClearUpdatedForMain("azylman/aerial", 90)
	if tr.WasUpdatedForMain("azylman/aerial", 90) {
		t.Errorf("expected WasUpdatedForMain to be false after clear")
	}
	if !tr.WasUpdatedForMain("azylman/aerial", 91) {
		t.Errorf("expected WasUpdatedForMain(91) to still be true")
	}

	// Repeated push does not clear
	tr.ClearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if !tr.WasUpdatedForMain("azylman/aerial", 91) {
		t.Errorf("expected WasUpdatedForMain(91) still true on duplicate main commit")
	}

	tr.ClearUpdatedPRsForMainCommit("azylman/aerial", "mainsha2")
	if tr.WasUpdatedForMain("azylman/aerial", 91) {
		t.Errorf("expected WasUpdatedForMain(91) to be cleared after new main commit")
	}
}

func TestStateTracker_ConflictPublished(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.MarkConflictPublished("", 1, "sha")
	tr.MarkConflictPublished("repo", 0, "sha")
	tr.MarkConflictPublished("repo", 1, "")
	if tr.WasConflictPublished("repo", 0, "sha") {
		t.Errorf("expected false for zero pr in WasConflictPublished")
	}
	if tr.WasConflictPublished("repo", 1, "") {
		t.Errorf("expected false for empty sha in WasConflictPublished")
	}

	tr.MarkConflictPublished("azylman/aerial", 100, "conf-sha")
	if !tr.WasConflictPublished("azylman/aerial", 100, "conf-sha") {
		t.Errorf("expected WasConflictPublished to be true")
	}
	if tr.WasConflictPublished("azylman/aerial", 100, "other") {
		t.Errorf("expected WasConflictPublished to be false for other sha")
	}
	tr.ClearConflictPublished("azylman/aerial", 0)
	tr.ClearConflictPublished("azylman/aerial", 100)
	if tr.WasConflictPublished("azylman/aerial", 100, "conf-sha") {
		t.Errorf("expected WasConflictPublished to be false after clear")
	}
}

func TestStateTracker_CommitSHAPR(t *testing.T) {
	t.Parallel()
	tr := &StateTracker{}

	tr.RecordCommitSHAPR("", "sha", 1, "main")
	tr.RecordCommitSHAPR("repo", "", 1, "main")
	tr.RecordCommitSHAPR("repo", "0000000000000000000000000000000000000000", 1, "main")
	tr.RecordCommitSHAPR("repo", "sha", 0, "main")
	if _, _, ok := tr.LookupCommitSHAPR("", "sha"); ok {
		t.Errorf("expected false for empty repo")
	}
	if _, _, ok := tr.LookupCommitSHAPR("repo", ""); ok {
		t.Errorf("expected false for empty sha")
	}
	if _, _, ok := tr.LookupCommitSHAPR("repo", "0000000000000000000000000000000000000000"); ok {
		t.Errorf("expected false for zero sha")
	}
	if _, _, ok := tr.LookupCommitSHAPR("repo", "missing"); ok {
		t.Errorf("expected false for missing entry")
	}

	tr.RecordCommitSHAPR("azylman/aerial", "commitha", 105, "feat/branch")
	prNum, branch, ok := tr.LookupCommitSHAPR("azylman/aerial", "commitha")
	if !ok || prNum != 105 || branch != "feat/branch" {
		t.Errorf("LookupCommitSHAPR() = (%d, %q, %v), want (105, feat/branch, true)", prNum, branch, ok)
	}

	// Corrupt entry handling
	tr.StoreRawCommitSHAPRForTest("azylman/aerial@corrupt", "not-a-struct")
	if _, _, ok := tr.LookupCommitSHAPR("azylman/aerial", "corrupt"); ok {
		t.Errorf("expected false for corrupt entry in commitSHAPRs")
	}
	tr.StoreRawCommitSHAPRForTest("azylman/aerial@zero-pr", GitHubCheckRunPR{Number: 0})
	if _, _, ok := tr.LookupCommitSHAPR("azylman/aerial", "zero-pr"); ok {
		t.Errorf("expected false for zero PR number in commitSHAPRs")
	}
}
