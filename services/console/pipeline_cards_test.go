package main

import (
	"testing"
	"time"
)

func mkMainRun(id int64, repo, mergedFrom, sha, status, conclusion string, createdOff, updatedOff time.Duration) GitHubRun {
	r := mkRun(id, repo, sha, status, conclusion, "", createdOff, updatedOff)
	r.Event = "push"
	r.HeadBranch = "main"
	r.MergedFromBranch = mergedFrom
	return r
}

func mergeClusterBatch4Cases() []mergeClusterTestCase {
	const (
		repo   = "azylman/aerial"
		branch = "feat/pr-cards"
		shaA   = "aaaa111111111111111111111111111111111111"
		shaB   = "bbbb222222222222222222222222222222222222"
		shaM   = "cccc333333333333333333333333333333333333"
		prID   = "pr-azylman-aerial-feat-pr-cards"
	)
	branchFailed := mkPRRun(1101, repo, branch, shaA, "completed", "failure", -20*time.Minute, -19*time.Minute)
	branchGreen := mkPRRun(1102, repo, branch, shaB, "completed", "success", -10*time.Minute, -9*time.Minute)
	mainMerge := mkMainRun(1103, repo, branch, shaM, "completed", "success", -3*time.Minute, -1*time.Minute)
	branchCancelled := mkPRRun(1111, repo, branch, shaA, "completed", "cancelled", -10*time.Minute, -9*time.Minute)
	branchBuilding := mkPRRun(1112, repo, branch, shaB, "in_progress", "", -5*time.Minute, -1*time.Minute)
	hiddenGreen := mkPRRun(1121, repo, branch, shaB, "completed", "success", -5*time.Minute, -4*time.Minute)

	return []mergeClusterTestCase{
		// 56. Superseded cancelled branch push disappears; newest run owns the PR card
		{name: "PRCard_CancelledOlderPushSuperseded", runs: mkRuns(branchCancelled, branchBuilding), wantList: []wantDep{{ID: prID, Commit: "bbbb222", Stage: "building"}}},

		// 57. Same runs newest-first render identically
		{name: "PRCard_CancelledOlderPushSuperseded_NewestFirst", runs: mkRuns(branchBuilding, branchCancelled), wantList: []wantDep{{ID: prID, Commit: "bbbb222", Stage: "building"}}},

		// 58. Failed branch run fixed by a green branch run leaves no card
		{name: "PRCard_GreenBranchRunHidesFailedCard", runs: mkRuns(branchFailed, branchGreen), wantCount: 0},

		// 59. Merge to main takes over the PR card
		{name: "PRCard_MergeToMainOwnsCard", runs: mkRuns(branchFailed, branchGreen, mainMerge), wantList: []wantDep{{ID: prID, Commit: "cccc333", Stage: "awaiting_pull", HTMLURL: "https://github.com/azylman/aerial/actions/runs/1103"}}},

		// 60. Same runs newest-first render identically
		{name: "PRCard_MergeToMainOwnsCard_NewestFirst", runs: mkRuns(mainMerge, branchGreen, branchFailed), wantList: []wantDep{{ID: prID, Commit: "cccc333", Stage: "awaiting_pull", HTMLURL: "https://github.com/azylman/aerial/actions/runs/1103"}}},

		// 61. Main push with no resolved PR stays SHA-keyed
		{name: "MainPushWithoutMergedBranch_ShaKeyed", runs: mkRuns(mkMainRun(1131, repo, "", "dddd444444444444444444444444444444444444", "in_progress", "", -2*time.Minute, -1*time.Minute)), wantList: []wantDep{{ID: "gh-run-1131", Commit: "dddd444", Stage: "building"}}},

		// 62. Same branch name in two repositories keeps separate cards
		{name: "PRCard_SameBranchTwoRepos", runs: mkRuns(mkPRRun(1141, repo, "feat/shared", shaA, "in_progress", "", -2*time.Minute, -1*time.Minute), mkPRRun(1142, "azylman/mirrormere", "feat/shared", shaB, "in_progress", "", -3*time.Minute, -1*time.Minute)), wantList: []wantDep{{ID: "pr-azylman-aerial-feat-shared", Repository: repo, Commit: "aaaa111"}, {ID: "pr-azylman-mirrormere-feat-shared", Repository: "azylman/mirrormere", Commit: "bbbb222"}}},

		// 63. Cancelled main run superseded by a newer main run disappears
		{name: "CancelledMainRunSupersededByNewerMainRun", runs: mkRuns(mkMainRun(1151, repo, "feat/one", shaA, "completed", "cancelled", -10*time.Minute, -9*time.Minute), mkMainRun(1152, repo, "feat/two", shaB, "in_progress", "", -5*time.Minute, -1*time.Minute)), wantList: []wantDep{{ID: "pr-azylman-aerial-feat-two", Commit: "bbbb222", Stage: "building"}}},

		// 64. Cancelled main run without a newer main run still shows failed
		{name: "LoneCancelledMainRunStillFailed", runs: mkRuns(mkRun(1161, repo, shaA, "completed", "cancelled", "", -10*time.Minute, -9*time.Minute)), wantList: []wantDep{{ID: "gh-run-1161", Stage: "failed"}}},

		// 65. Hidden PR card is not resurrected by a container group on its SHA
		{name: "HiddenPRCardIgnoresContainerGroup", runs: mkRuns(hiddenGreen), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", shaB, "running", "", -60*time.Second)}, wantCount: 0},

		// 66. Hidden PR card is not resurrected by Hangar reconciliation on its SHA
		{name: "HiddenPRCardIgnoresHangar", runs: mkRuns(hiddenGreen), gitSync: mkRecon(true, "pulling", shaB, "", nil, -10*time.Second), wantCount: 0},
	}
}

func TestPipelineKey_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  GitHubRun
		want string
	}{
		{name: "PullRequestRunKeyedByBranch", run: mkPRRun(1, "azylman/aerial", "feat/x", "ABCDEF1234567", "in_progress", "", 0, 0), want: "pr:azylman/aerial:feat/x"},
		{name: "MergedMainPushKeyedByPRBranch", run: mkMainRun(2, "azylman/aerial", "feat/x", "abcdef1234567", "completed", "success", 0, 0), want: "pr:azylman/aerial:feat/x"},
		{name: "MainPushWithoutPRKeyedBySHA", run: mkMainRun(3, "azylman/aerial", "", "ABCDEF1234567", "completed", "success", 0, 0), want: "abcdef1234567"},
		{name: "LegacyRunWithoutBranchKeyedBySHA", run: mkRun(4, "", " ABCDEF1234567 ", "queued", "", "", 0, 0), want: "abcdef1234567"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := pipelineKey(tc.run); got != tc.want {
				t.Errorf("pipelineKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPRPipelineCardID_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		repo   string
		branch string
		want   string
	}{
		{name: "SlashesBecomeDashes", repo: "azylman/aerial", branch: "feat/pr-cards", want: "pr-azylman-aerial-feat-pr-cards"},
		{name: "UppercaseAndSymbolsNormalized", repo: "Azylman/Aerial.Config", branch: "Fix_Thing#2", want: "pr-azylman-aerial-config-fix-thing-2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := prPipelineCardID(tc.repo, tc.branch); got != tc.want {
				t.Errorf("prPipelineCardID() = %q, want %q", got, tc.want)
			}
		})
	}
}
