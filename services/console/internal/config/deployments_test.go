package config

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	testRefTime    = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	testCommitTime = testRefTime.Add(-10 * time.Minute)
)

func mkRuns(runs ...GitHubRun) []GitHubRun {
	return runs
}

func mkRun(id int64, repo, sha, status, conclusion, msg string, createdOff, updatedOff time.Duration) GitHubRun {
	r := GitHubRun{
		ID:         id,
		Repository: repo,
		Name:       "Continuous Delivery",
		HeadSHA:    sha,
		Status:     status,
		Conclusion: conclusion,
		CreatedAt:  testRefTime.Add(createdOff),
		UpdatedAt:  testRefTime.Add(updatedOff),
	}
	if repo == "" {
		r.HTMLURL = fmt.Sprintf("https://github.com/azylman/aerial/actions/runs/%d", id)
	} else {
		r.HTMLURL = fmt.Sprintf("https://github.com/%s/actions/runs/%d", repo, id)
	}
	if msg != "" {
		r.HeadCommit = &HeadCommit{Message: msg, Timestamp: testRefTime.Add(-10 * time.Minute)}
	}
	return r
}

func mkPRRun(id int64, repo, branch, sha, status, conclusion string, createdOff, updatedOff time.Duration) GitHubRun {
	r := mkRun(id, repo, sha, status, conclusion, "", createdOff, updatedOff)
	r.Event = "pull_request"
	r.HeadBranch = branch
	return r
}

func mkJob(id, runID int64, name, status, conclusion string, startOff, endOff time.Duration) GitHubJob {
	j := GitHubJob{ID: id, RunID: runID, Name: name, Status: status, Conclusion: conclusion}
	if startOff != 0 {
		j.StartedAt = testRefTime.Add(startOff)
	}
	if endOff != 0 {
		j.CompletedAt = testRefTime.Add(endOff)
	}
	return j
}

// mkCont builds a container in the shape synthesizeContainersFromServices produces
// for a service without image metadata; the service defaults to the name minus "aerial-".
func mkCont(name, service, sha, state, health string, createdOff time.Duration) DockerContainerJSON {
	if service == "" {
		service = strings.TrimPrefix(name, "aerial-")
	}
	c := DockerContainerJSON{
		State:   state,
		Created: testRefTime.Add(createdOff).Unix(),
		Labels:  map[string]string{"aerial.service": service},
	}
	if sha != "" {
		c.Labels["aerial.commit_sha"] = sha
	}
	if health != "" {
		c.Health = &DockerContainerHealth{Status: health}
	}
	return c
}

func mkLabeledCont(state string, labels map[string]string) DockerContainerJSON {
	return DockerContainerJSON{State: state, Labels: labels}
}

type wantDep struct {
	ID               string
	Service          string
	Repository       string
	Commit           string
	CommitMsgSubstr  string
	Stage            string
	Progress         int
	HTMLURL          string
	StepStatuses     []string
	StepStatusMap    map[int]string
	MatrixJobCount   int
	HasContainerChip bool
	HasCIChip        bool
	CheckCommitTime  *time.Time
}

func TestMergeClusterDeployments_PureTableDriven(t *testing.T) {
	t.Parallel()
	tests := make([]mergeClusterTestCase, 0, 70)
	tests = append(tests, mergeClusterBatch1Cases()...)
	tests = append(tests, mergeClusterBatch2Cases()...)
	tests = append(tests, mergeClusterBatch3Cases()...)
	tests = append(tests, mergeClusterBatch4Cases()...)
	tests = append(tests, mergeClusterBatch5Cases()...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runMergeClusterDeploymentsCase(t, tt, testRefTime)
		})
	}
}

func mergeClusterBatch1Cases() []mergeClusterTestCase {
	var cardinalityRuns []GitHubRun
	for i := 1; i <= 8; i++ {
		cardinalityRuns = append(cardinalityRuns, mkRun(int64(600+i), "", fmt.Sprintf("%07d000000000000000000000000000000000", i), "in_progress", "", "", time.Duration(-i)*time.Minute, time.Duration(-i)*time.Minute))
	}

	sortRuns := mkRuns(
		mkRun(100, "", "1111111111111111111111111111111111111111", "completed", "success", "", -10*time.Minute, -2*time.Minute),
		mkRun(101, "", "3333333333333333333333333333333333333333", "in_progress", "", "", -4*time.Minute, -3*time.Minute),
		mkRun(102, "", "5555555555555555555555555555555555555555", "completed", "success", "", -10*time.Minute, -2*time.Minute),
		mkRun(103, "", "2222222222222222222222222222222222222222", "queued", "", "", -1*time.Minute, -1*time.Minute),
	)
	sortContainers := []DockerContainerJSON{
		mkCont("proxy", "proxy", "1111111111111111111111111111111111111111", "running", "", -40*time.Second),
		mkCont("brain", "brain", "6666666666666666666666666666666666666666", "running", "", -300*time.Second),
	}

	return []mergeClusterTestCase{
		// 2. AwaitingPullBridge
		{name: "AwaitingPullBridge", runs: mkRuns(mkRun(301, "", "cccccccccccccccccccccccccccccccccccccccc", "completed", "success", "chore: updated docs", -10*time.Minute, -5*time.Minute)), currentCommit: "oldcommit", wantList: []wantDep{{Commit: "ccccccc", Stage: "awaiting_pull", StepStatusMap: map[int]string{1: "completed", 2: "active"}}}},

		// 3. DeterministicSort
		{name: "DeterministicSort", runs: sortRuns, containers: sortContainers, currentCommit: "6666666", wantCount: 4, wantList: []wantDep{{Stage: "swapping"}, {Stage: "building"}, {Stage: "awaiting_pull"}, {Stage: "queued"}}, wantRepeats: 10},

		// 4. MultiCommitContainerGrouping
		{name: "MultiCommitContainerGrouping", runs: mkRuns(mkRun(1, "", "aaaaaaa123456789", "completed", "success", "", -10*time.Minute, -5*time.Minute), mkRun(2, "", "bbbbbbb123456789", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "aaaaaaa123456789", "running", "", -300*time.Second), mkCont("aerial-proxy", "proxy", "bbbbbbb123456789", "running", "", -30*time.Second)}, currentCommit: "aaaaaaa", wantList: []wantDep{{Commit: "bbbbbbb", Stage: "swapping"}, {Commit: "aaaaaaa", Stage: "live"}}},

		// 5. ChipScoping
		{name: "ChipScoping", runs: mkRuns(mkRun(401, "", "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "in_progress", "", "", -2*time.Minute, -1*time.Minute)), jobs: map[int64][]GitHubJob{401: {mkJob(1, 401, "build (brain)", "in_progress", "", -90*time.Second, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "running", "", -30*time.Second)}, currentCommit: "7777777", wantList: []wantDep{{HasCIChip: true, HasContainerChip: true}}},

		// 6. SafeNilHandling
		{name: "SafeNilHandling", runs: mkRuns(mkRun(501, "", "", "queued", "", "", -10*time.Second, 0), mkRun(502, "", "abc", "in_progress", "", "", -20*time.Second, 0), mkRun(503, "", "not-a-valid-hex-sha!@#$%^&*()", "completed", "failure", "", -30*time.Second, -25*time.Second)), wantCount: 3},

		// 7. CardinalityClamp
		{name: "CardinalityClamp", runs: cardinalityRuns, wantCount: 5},

		// 9. DegradedAndFailedBranches_DegradedContainer
		{name: "DegradedAndFailedBranches_DegradedContainer", runs: mkRuns(mkRun(801, "", "8888888888888888888888888888888888888888", "completed", "success", "", -5*time.Minute, -2*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-dashboard", "", "8888888888888888888888888888888888888888", "exited", "", -60*time.Second)}, wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{4: "failed"}}}},

		// 12. MultiRunSameCommitAndSortTies_SameCommit
		{name: "MultiRunSameCommitAndSortTies_SameCommit", runs: mkRuns(mkRun(111, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "completed", "success", "", -10*time.Minute, -5*time.Minute), mkRun(112, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "", -1*time.Minute, -30*time.Second)), wantList: []wantDep{{Stage: "building"}}},

		// 13. MultiRunSameCommitAndSortTies_SortTiesLexicographical
		{name: "MultiRunSameCommitAndSortTies_SortTiesLexicographical", runs: mkRuns(mkRun(201, "", "ffffffffffffffffffffffffffffffffffffffff", "in_progress", "", "", -2*time.Minute, -1*time.Minute), mkRun(202, "", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "in_progress", "", "", -2*time.Minute, -1*time.Minute)), wantList: []wantDep{{Commit: "eeeeeee"}, {Commit: "fffffff"}}},

		// 14. MultiRunSameCommitAndSortTies_FailedPrecedesDegraded
		{name: "MultiRunSameCommitAndSortTies_FailedPrecedesDegraded", runs: mkRuns(mkRun(301, "", "1111111111111111111111111111111111111111", "completed", "failure", "", -5*time.Minute, -5*time.Minute), mkRun(302, "", "2222222222222222222222222222222222222222", "completed", "success", "", -5*time.Minute, -5*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-proxy", "", "2222222222222222222222222222222222222222", "exited", "", -5*time.Minute)}, wantList: []wantDep{{Stage: "failed"}, {Stage: "degraded"}}},

		// 15. MultiRunSameCommitAndSortTies_ExpiredIgnored
		{name: "MultiRunSameCommitAndSortTies_ExpiredIgnored", runs: mkRuns(mkRun(401, "", "3333333333333333333333333333333333333333", "completed", "success", "", -40*time.Minute, -35*time.Minute)), wantCount: 0},

		// 16. UnparentedContainersDoNotSpawnCards
		{name: "UnparentedContainersDoNotSpawnCards", containers: []DockerContainerJSON{mkLabeledCont("running", map[string]string{"aerial.service": "postgres"}), mkLabeledCont("exited", map[string]string{"aerial.service": "vector"})}, currentCommit: "livecommit", wantCount: 0},

		// 17. ActiveCIRun
		{name: "ActiveCIRun", runs: mkRuns(mkRun(481, "", "7a9f1b234567", "in_progress", "", "feat(brain): live github actions tracking", -2*time.Minute, -10*time.Second)), jobs: map[int64][]GitHubJob{481: {mkJob(1, 481, "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)", "in_progress", "", -40*time.Second, 0), mkJob(2, 481, "Build & Push Images to GHCR (dashboard, ./dashboard, ./dashboard/Dockerfile, aerial-dashboard)", "completed", "success", -60*time.Second, -10*time.Second)}}, currentCommit: "7a9f1b2", wantList: []wantDep{{Stage: "building", Commit: "7a9f1b2", MatrixJobCount: 2, HTMLURL: "https://github.com/azylman/aerial/actions/runs/481"}}},

		// 18. FailedCIRun
		{name: "FailedCIRun", runs: mkRuns(mkRun(482, "", "abc1234567", "completed", "failure", "", -5*time.Minute, -4*time.Minute)), currentCommit: "abc1234", wantList: []wantDep{{Stage: "failed", StepStatusMap: map[int]string{1: "failed"}}}},

		// 19. AwaitingPullUntilRolloutObserved
		{name: "AwaitingPullUntilRolloutObserved", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, runs: mkRuns(mkRun(483, "", "def5678901", "completed", "success", "", -4*time.Minute, -45*time.Second)), jobs: map[int64][]GitHubJob{483: {mkJob(0, 483, "Build & Push Images to GHCR (brain)", "completed", "success", 0, 0)}}, currentCommit: "old1234", wantList: []wantDep{{Stage: "awaiting_pull", StepStatusMap: map[int]string{2: "active"}}}},
	}
}

func mergeClusterBatch2Cases() []mergeClusterTestCase {
	return []mergeClusterTestCase{
		// 21. NoContainerBuilds_ImmediateLive
		{name: "NoContainerBuilds_ImmediateLive", runs: mkRuns(mkRun(485, "", "purecode1234", "completed", "success", "", -10*time.Minute, -150*time.Second)), jobs: map[int64][]GitHubJob{485: {mkJob(0, 485, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(0, 485, "Lint Microservices & Frontend", "completed", "success", 0, 0), mkJob(0, 485, "Build & Push Images to GHCR (brain)", "completed", "skipped", 0, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -1*time.Hour)}, currentCommit: "old1234", wantList: []wantDep{{Stage: "live", Progress: 100, StepStatusMap: map[int]string{2: "completed"}}}},

		// 22. RollingSwap
		{name: "RollingSwap", runs: mkRuns(mkRun(101, "", "abc9999999", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "abc9999999", "running", "starting", -30*time.Second), mkCont("aerial-dashboard", "dashboard", "", "running", "", -10*time.Second)}, currentCommit: "abc9999", wantList: []wantDep{{Stage: "swapping", Commit: "abc9999", MatrixJobCount: 2}}},

		// 23. SyncedGrace
		{name: "SyncedGrace", runs: mkRuns(mkRun(102, "", "live123", "completed", "success", "", -5*time.Minute, -4*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "healthy", -3*time.Minute), mkCont("aerial-dashboard", "dashboard", "", "running", "healthy", -3*time.Minute)}, currentCommit: "live123", wantList: []wantDep{{Stage: "live", Progress: 100, StepStatuses: []string{"completed", "completed", "completed", "completed", "completed"}}}},

		// 24. DegradedState
		{name: "DegradedState", runs: mkRuns(mkRun(103, "", "deg1234", "completed", "success", "", -5*time.Minute, -3*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "unhealthy", -2*time.Minute), mkCont("aerial-dashboard", "dashboard", "", "running", "healthy", -2*time.Minute)}, currentCommit: "deg1234", wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{4: "failed"}}}},

		// 25. Idle
		{name: "Idle", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "healthy", -2*time.Hour)}, currentCommit: "idle123", wantCount: 0},

		// 26. AdversarialNilLabelsAndNilHealth
		{name: "AdversarialNilLabelsAndNilHealth", runs: mkRuns(mkRun(104, "", "test1234", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "", "", "running", "", -30*time.Second), mkCont("aerial-dashboard", "", "", "running", "", -30*time.Second)}, currentCommit: "test1234", wantList: []wantDep{{Stage: "swapping", MatrixJobCount: 2}}},

		// 27. AdversarialClockSkewFutureTimestamp
		{name: "AdversarialClockSkewFutureTimestamp", runs: mkRuns(mkRun(999, "", "future1234", "completed", "success", "", 10*time.Second, 15*time.Second)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 2*time.Hour)}, currentCommit: "old1234", wantList: []wantDep{{Stage: "awaiting_pull"}}},

		// 28. CommitTimeAcrossAllStages_Queued
		{name: "CommitTimeAcrossAllStages_Queued", runs: mkRuns(mkRun(501, "", "1111aaa", "queued", "", "feat: queued", -2*time.Minute, 0)), currentCommit: "1111aaa", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 29. CommitTimeAcrossAllStages_Building
		{name: "CommitTimeAcrossAllStages_Building", runs: mkRuns(mkRun(502, "", "2222bbb", "in_progress", "", "feat: building", -1*time.Minute, 0)), currentCommit: "2222bbb", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 30. CommitTimeAcrossAllStages_Failed
		{name: "CommitTimeAcrossAllStages_Failed", runs: mkRuns(mkRun(503, "", "3333ccc", "completed", "failure", "feat: failed", -5*time.Minute, -4*time.Minute)), currentCommit: "3333ccc", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 31. CommitTimeAcrossAllStages_Swapping
		{name: "CommitTimeAcrossAllStages_Swapping", runs: mkRuns(mkRun(504, "", "4444ddd", "completed", "success", "feat: success", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "starting", -30*time.Second)}, currentCommit: "4444ddd", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 32. EmptyAndFuture_Empty
		{name: "EmptyAndFuture_Empty", currentCommit: "commit123", wantCount: 0},

		// 33. EmptyAndFuture_FutureContainers
		{name: "EmptyAndFuture_FutureContainers", runs: mkRuns(mkRun(123, "", "sha1234567890", "completed", "success", "", -10*time.Minute, 0)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 2*time.Hour)}, wantCount: 1},

		// 37. UnhealthyContainerDegradesCard
		{name: "UnhealthyContainerDegradesCard", runs: mkRuns(mkRun(105, "", "commit123", "completed", "success", "", -15*time.Minute, -12*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "unhealthy", -10*time.Minute)}, currentCommit: "commit123", wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{3: "completed", 4: "failed"}}}},

		// 38. MultiRepoActiveFanout
		{name: "MultiRepoActiveFanout", runs: mkRuns(mkRun(101, "azylman/aerial", "1111aaa", "in_progress", "", "", -2*time.Minute, 0), mkRun(201, "azylman/aerial-sidecars", "2222bbb", "in_progress", "", "", -1*time.Minute, 0)), currentCommit: "0000000", wantList: []wantDep{{Repository: "azylman/aerial-sidecars", Service: "aerial-sidecars", Stage: "building"}, {Repository: "azylman/aerial", Service: "aerial-stack", Stage: "building"}}},
	}
}

func mergeClusterBatch3Cases() []mergeClusterTestCase {
	return []mergeClusterTestCase{
		// 39. NonContainerRepoNoTimeout
		{name: "NonContainerRepoNoTimeout", runs: mkRuns(mkRun(301, "azylman/aerial-config", "3333ccc", "completed", "success", "", -5*time.Minute, -150*time.Second)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, currentCommit: "oldcorecommit", wantList: []wantDep{{Service: "aerial-config", Stage: "awaiting_pull"}}},

		// 40. ActivePRRunShowsBuilding
		{name: "ActivePRRunShowsBuilding", runs: mkRuns(mkPRRun(9001, "azylman/aerial-sidecars", "feat/sidecars-pr", "aabbccdd1122", "in_progress", "", -1*time.Minute, -30*time.Second)), jobs: map[int64][]GitHubJob{9001: {mkJob(0, 9001, "Run Sidecar Tests & Lint", "completed", "success", 0, 0), mkJob(0, 9001, "Build & Push Sidecar Images to GHCR (sidecar-alpha)", "in_progress", "", 0, 0)}}, currentCommit: "0000000", wantList: []wantDep{{Repository: "azylman/aerial-sidecars", Stage: "building", MatrixJobCount: 2}}},

		// 41. CompletedPRRunDoesNotTimeout
		{name: "CompletedPRRunDoesNotTimeout", runs: mkRuns(mkPRRun(9002, "azylman/aerial-sidecars", "feat/sidecars-pr", "aabbccdd1122", "completed", "success", -300*time.Second, -200*time.Second)), jobs: map[int64][]GitHubJob{9002: {mkJob(0, 9002, "Build & Push Sidecar Images to GHCR (sidecar-alpha)", "completed", "success", 0, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, currentCommit: "oldcorecommit", wantCount: 0},

		// 54. Prefix40CharSHA_MatchedBy7CharPrefixContainer
		{name: "Prefix40CharSHA_MatchedBy7CharPrefixContainer", runs: mkRuns(mkRun(888, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "", -3*time.Minute, 0)), containers: []DockerContainerJSON{mkCont("aerial-proxy", "proxy", "bbbbbbb", "running", "", -20*time.Second)}, wantList: []wantDep{{Commit: "bbbbbbb"}}},

		// 55. SortTieBreaker_FailedPrecedesDegraded_BothPositions
		{name: "SortTieBreaker_FailedPrecedesDegraded_BothPositions", runs: mkRuns(mkRun(901, "", "1111111111111111111111111111111111111111", "completed", "failure", "", -5*time.Minute, -5*time.Minute), mkRun(902, "", "2222222222222222222222222222222222222222", "completed", "success", "", -5*time.Minute, -5*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "", "2222222222222222222222222222222222222222", "exited", "", -5*time.Minute)}, wantList: []wantDep{{Stage: "failed", Commit: "1111111"}, {Stage: "degraded", Commit: "2222222"}}},
	}
}

type mergeClusterTestCase struct {
	name          string
	runs          []GitHubRun
	jobs          map[int64][]GitHubJob
	containers    []DockerContainerJSON
	currentCommit string
	refTime       time.Time
	useZeroTime   bool
	wantCount     int
	wantList      []wantDep
	wantRepeats   int
}

func runMergeClusterDeploymentsCase(t *testing.T, tt mergeClusterTestCase, defaultRefTime time.Time) {
	t.Helper()
	ref := tt.refTime
	if ref.IsZero() && !tt.useZeroTime {
		ref = defaultRefTime
	}
	deps := MergeClusterDeployments(tt.runs, tt.jobs, tt.containers, tt.currentCommit, ref)

	expectedCount := tt.wantCount
	if expectedCount == 0 && len(tt.wantList) > 0 {
		expectedCount = len(tt.wantList)
	}
	if len(deps) != expectedCount {
		t.Fatalf("expected %d deployments, got %d: %+v", expectedCount, len(deps), deps)
	}
	for i, want := range tt.wantList {
		if i >= len(deps) {
			break
		}
		assertMergeClusterDeploymentItem(t, i, deps[i], want)
	}
	if tt.wantRepeats > 0 {
		assertMergeClusterDeploymentRepeats(t, tt, ref)
	}
}

func assertMergeClusterDeploymentItem(t *testing.T, i int, got DeploymentStatus, want wantDep) {
	t.Helper()
	if want.ID != "" && got.ID != want.ID {
		t.Errorf("dep[%d].ID = %q, want %q", i, got.ID, want.ID)
	}
	if want.Service != "" && got.Service != want.Service {
		t.Errorf("dep[%d].Service = %q, want %q", i, got.Service, want.Service)
	}
	if want.Repository != "" && got.Repository != want.Repository {
		t.Errorf("dep[%d].Repository = %q, want %q", i, got.Repository, want.Repository)
	}
	if want.Commit != "" && got.Commit != want.Commit {
		t.Errorf("dep[%d].Commit = %q, want %q", i, got.Commit, want.Commit)
	}
	if want.Stage != "" && got.Stage != want.Stage {
		t.Errorf("dep[%d].Stage = %q, want %q", i, got.Stage, want.Stage)
	}
	if want.Progress != 0 && got.Progress != want.Progress {
		t.Errorf("dep[%d].Progress = %d, want %d", i, got.Progress, want.Progress)
	}
	if want.CommitMsgSubstr != "" && !strings.Contains(got.CommitMsg, want.CommitMsgSubstr) {
		t.Errorf("dep[%d].CommitMsg = %q, want containing %q", i, got.CommitMsg, want.CommitMsgSubstr)
	}
	if want.HTMLURL != "" && got.HTMLURL != want.HTMLURL {
		t.Errorf("dep[%d].HTMLURL = %q, want %q", i, got.HTMLURL, want.HTMLURL)
	}
	assertMergeClusterDeploymentSteps(t, i, got, want)
	assertMergeClusterDeploymentChips(t, i, got, want)
	if want.CheckCommitTime != nil {
		if got.CommitTime == nil || !got.CommitTime.Equal(*want.CheckCommitTime) {
			t.Errorf("dep[%d].CommitTime = %v, want %v", i, got.CommitTime, *want.CheckCommitTime)
		}
	}
}

func assertMergeClusterDeploymentSteps(t *testing.T, i int, got DeploymentStatus, want wantDep) {
	t.Helper()
	if len(want.StepStatuses) > 0 {
		if len(got.Steps) != len(want.StepStatuses) {
			t.Errorf("dep[%d].Steps len = %d, want %d", i, len(got.Steps), len(want.StepStatuses))
		} else {
			for sIdx, expStatus := range want.StepStatuses {
				if got.Steps[sIdx].Status != expStatus {
					t.Errorf("dep[%d].Steps[%d].Status = %q, want %q", i, sIdx, got.Steps[sIdx].Status, expStatus)
				}
			}
		}
	}
	for sIdx, expStatus := range want.StepStatusMap {
		if sIdx < len(got.Steps) {
			if got.Steps[sIdx].Status != expStatus {
				t.Errorf("dep[%d].Steps[%d].Status = %q, want %q", i, sIdx, got.Steps[sIdx].Status, expStatus)
			}
		} else {
			t.Errorf("dep[%d].Steps has len %d, cannot check index %d", i, len(got.Steps), sIdx)
		}
	}
}

func assertMergeClusterDeploymentChips(t *testing.T, i int, got DeploymentStatus, want wantDep) {
	t.Helper()
	if want.MatrixJobCount > 0 && len(got.MatrixJobs) != want.MatrixJobCount {
		t.Errorf("dep[%d].MatrixJobs count = %d, want %d", i, len(got.MatrixJobs), want.MatrixJobCount)
	}
	if want.HasContainerChip {
		found := false
		for _, c := range got.MatrixJobs {
			if c.Type == "container" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("dep[%d] expected container chip, got %+v", i, got.MatrixJobs)
		}
	}
	if want.HasCIChip {
		found := false
		for _, c := range got.MatrixJobs {
			if c.Type == "ci" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("dep[%d] expected CI chip, got %+v", i, got.MatrixJobs)
		}
	}
}

func assertMergeClusterDeploymentRepeats(t *testing.T, tt mergeClusterTestCase, ref time.Time) {
	t.Helper()
	for iter := range tt.wantRepeats {
		iterDeps := MergeClusterDeployments(tt.runs, tt.jobs, tt.containers, tt.currentCommit, ref)
		if len(iterDeps) != len(tt.wantList) {
			t.Fatalf("iteration %d: expected %d deployments, got %d", iter, len(tt.wantList), len(iterDeps))
		}
		for idx, want := range tt.wantList {
			if iterDeps[idx].Stage != want.Stage {
				t.Fatalf("iteration %d at index %d: expected stage %q, got %q", iter, idx, want.Stage, iterDeps[idx].Stage)
			}
		}
	}
}

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

// mkK8sCont builds a container exactly as k8s mode synthesizes it from a Deployment.
func mkK8sCont(name, image, status string, uptimeSec int64) DockerContainerJSON {
	return synthesizeContainersFromServices([]ServiceStatus{{Name: name, Status: status, UptimeSeconds: uptimeSec, Commit: imageCommitSHA(image), Image: image}}, testRefTime)[0]
}

func TestImageCommitSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "full sha tag", image: "ghcr.io/azylman/aerial-synapse:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"},
		{name: "uppercase sha tag lowercased", image: "ghcr.io/azylman/aerial-eri:DE22EF4A238E56B6F9FB67CB7EB676F39D6596C3", want: "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"},
		{name: "registry with port", image: "host:5000/img:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"},
		{name: "registry with port and no tag", image: "host:5000/img", want: ""},
		{name: "digest", image: "ghcr.io/azylman/aerial-eri@sha256:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3de22ef4a238e56b6", want: ""},
		{name: "latest", image: "ghcr.io/azylman/aerial-eri:latest", want: ""},
		{name: "semver", image: "grafana/grafana:11.3.0", want: ""},
		{name: "date tag", image: "ghcr.io/azylman/aerial-eri:20261120", want: ""},
		{name: "short hex", image: "ghcr.io/azylman/aerial-eri:de22ef4", want: ""},
		{name: "41 hex chars", image: "ghcr.io/azylman/aerial-eri:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3a", want: ""},
		{name: "40 non-hex chars", image: "ghcr.io/azylman/aerial-eri:zz22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: ""},
		{name: "empty", image: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := imageCommitSHA(tc.image); got != tc.want {
				t.Errorf("imageCommitSHA(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}

func TestImageName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "ghcr with sha tag", image: "ghcr.io/azylman/aerial-synapse:de22ef4a238e56b6f9fb67cb7eb676f39d6596c3", want: "aerial-synapse"},
		{name: "registry with port", image: "host:5000/team/img:v1", want: "img"},
		{name: "digest", image: "ghcr.io/azylman/aerial-eri@sha256:abcd", want: "aerial-eri"},
		{name: "bare name", image: "nginx", want: "nginx"},
		{name: "bare name with tag", image: "nginx:1.27", want: "nginx"},
		{name: "empty", image: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := imageName(tc.image); got != tc.want {
				t.Errorf("imageName(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}

func TestBuiltImages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		jobs []GitHubJob
		want []string
	}{
		{name: "no jobs", jobs: nil, want: nil},
		{name: "successful builds sorted", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (synapse)", "completed", "success", 0, 0), mkJob(2, 1, "Build & Push Images to GHCR (eri-restate)", "completed", "success", 0, 0)}, want: []string{"aerial-eri-restate", "aerial-synapse"}},
		{name: "failed and skipped builds excluded", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (eri)", "completed", "failure", 0, 0), mkJob(2, 1, "Build & Push Images to GHCR (docs)", "completed", "skipped", 0, 0)}, want: nil},
		{name: "legacy tuple job name uses first field", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)", "completed", "success", 0, 0)}, want: []string{"aerial-brain"}},
		{name: "duplicate service deduped", jobs: []GitHubJob{mkJob(1, 1, "Build & Push Images to GHCR (eri)", "completed", "success", 0, 0), mkJob(2, 1, "Build & Push Images to GHCR (eri)", "completed", "success", 0, 0)}, want: []string{"aerial-eri"}},
		{name: "non build jobs and sidecar builds ignored", jobs: []GitHubJob{mkJob(1, 1, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(2, 1, "Build & Push Sidecar Images to GHCR (aura)", "completed", "success", 0, 0), mkJob(3, 1, "Build & Push Images to GHCR ()", "completed", "success", 0, 0)}, want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := builtImages(tc.jobs); !slices.Equal(got, tc.want) {
				t.Errorf("builtImages() = %v, want %v", got, tc.want)
			}
		})
	}
}

func mergeClusterBatch5Cases() []mergeClusterTestCase {
	const (
		shaNew   = "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"
		shaOld   = "1111222233334444555566667777888899990000"
		shaNext  = "abcdef0123456789abcdef0123456789abcdef01"
		synNew   = "ghcr.io/azylman/aerial-synapse:" + shaNew
		synOld   = "ghcr.io/azylman/aerial-synapse:" + shaOld
		synNext  = "ghcr.io/azylman/aerial-synapse:" + shaNext
		eriOld   = "ghcr.io/azylman/aerial-eri:" + shaOld
		buildSyn = "Build & Push Images to GHCR (synapse)"
		buildEri = "Build & Push Images to GHCR (eri)"
	)
	run := mkRun(2001, "azylman/aerial", shaNew, "completed", "success", "", -10*time.Minute, -5*time.Minute)
	nextRun := mkRun(2002, "azylman/aerial", shaNext, "completed", "success", "", -4*time.Minute, -2*time.Minute)
	synJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "success", 0, 0)}}
	eriSynJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, buildEri, "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "success", 0, 0)}}
	restateJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, "Build & Push Images to GHCR (eri-restate)", "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "success", 0, 0)}}
	docsJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(2, 2001, buildSyn, "completed", "skipped", 0, 0)}}
	bothJobs := map[int64][]GitHubJob{2001: {mkJob(1, 2001, buildSyn, "completed", "success", 0, 0)}, 2002: {mkJob(2, 2002, buildSyn, "completed", "success", 0, 0)}}
	console := mkK8sCont("console", "ghcr.io/azylman/aerial-console:"+shaOld, "healthy", 7200)
	grafana := mkK8sCont("grafana", "grafana/grafana:11.3.0", "unhealthy", 7200)
	liveSteps := []string{"completed", "completed", "completed", "completed", "completed"}

	return []mergeClusterTestCase{
		// 67. Synapse-only build fully rolled to both stacks long ago is live
		{name: "K8sRollout_SynapseBothStacksOnSHA_Live", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), mkK8sCont("synapse-agent-alpha", synNew, "healthy", 900), console, grafana}, currentCommit: "0000000", wantList: []wantDep{{Commit: "de22ef4", Stage: "live", Progress: 100, StepStatuses: liveSteps}}},

		// 68. Sibling stack still on the old SHA keeps the card swapping
		{name: "K8sRollout_SiblingOnOldSHA_Swapping", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), mkK8sCont("synapse-agent-alpha", synOld, "healthy", 7200), console}, currentCommit: "0000000", wantList: []wantDep{{Stage: "swapping", Progress: 72, StepStatuses: []string{"completed", "completed", "completed", "active", "active"}, HasContainerChip: true}}},

		// 69. eri and synapse built, only synapse rolled: not live
		{name: "K8sRollout_PartialServiceRollout_Swapping", runs: mkRuns(run), jobs: eriSynJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), mkK8sCont("eri", eriOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "swapping"}}},

		// 70. Built image with no Deployment (RestateDeployment) is ignored
		{name: "K8sRollout_BuiltImageWithoutDeploymentIgnored_Live", runs: mkRuns(run), jobs: restateJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 60)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "live", Progress: 100}}},

		// 71. Docs-only run is live on CI success
		{name: "K8sRollout_DocsOnlyRun_Live", runs: mkRuns(run), jobs: docsJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "live", Progress: 100}}},

		// 72. Jobs missing after a console restart: healthy group on the SHA is live regardless of uptime
		{name: "K8sRollout_JobsMissingHealthyGroup_Live", runs: mkRuns(run), containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "healthy", 900), console, grafana}, currentCommit: "0000000", wantList: []wantDep{{Stage: "live", Progress: 100}}},

		// 73. No container on the SHA after 5 min waits in awaiting_pull
		{name: "K8sRollout_NotRolledAfter5Min_AwaitingPull", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "awaiting_pull", StepStatusMap: map[int]string{2: "active"}}}},

		// 74. Jobs missing and nothing on the SHA also waits in awaiting_pull
		{name: "K8sRollout_JobsMissingNotRolled_AwaitingPull", runs: mkRuns(run), containers: []DockerContainerJSON{mkK8sCont("synapse", synOld, "healthy", 7200)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "awaiting_pull"}}},

		// 75. Deployment on the SHA still starting is swapping
		{name: "K8sRollout_StartingOnSHA_Swapping", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "starting", 20)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "swapping", Progress: 60}}},

		// 76. Unhealthy Deployment running a built image degrades the card
		{name: "K8sRollout_UnhealthyOnSHA_Degraded", runs: mkRuns(run), jobs: synJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNew, "unhealthy", 300), mkK8sCont("synapse-agent-alpha", synNew, "healthy", 300)}, currentCommit: "0000000", wantList: []wantDep{{Stage: "degraded", Progress: 85, StepStatusMap: map[int]string{3: "completed", 4: "failed"}}}},

		// 77. Containers already on a newer main run's SHA count as rolled for the older card
		{name: "K8sRollout_NewerRunSHACountsAsRolled_BothLive", runs: mkRuns(run, nextRun), jobs: bothJobs, containers: []DockerContainerJSON{mkK8sCont("synapse", synNext, "healthy", 60)}, currentCommit: "0000000", wantList: []wantDep{{Commit: "abcdef0", Stage: "live"}, {Commit: "de22ef4", Stage: "live"}}},
	}
}

func TestSynthesizeContainersFromServices(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	const sha = "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"
	tests := []struct {
		name        string
		svc         ServiceStatus
		wantState   string
		wantCreated int64
		wantLabels  map[string]string
	}{
		{name: "k8s service with sha image stamps commit and image", svc: ServiceStatus{Name: "synapse-agent-alpha", Status: "healthy", UptimeSeconds: 900, Commit: sha, Image: "ghcr.io/azylman/aerial-synapse:" + sha}, wantState: "running", wantCreated: now.Unix() - 900, wantLabels: map[string]string{"aerial.service": "synapse-agent-alpha", "aerial.commit_sha": sha, "aerial.image": "aerial-synapse"}},
		{name: "non-sha image stamps image only", svc: ServiceStatus{Name: "grafana", Status: "starting", UptimeSeconds: 5, Image: "grafana/grafana:11.3.0"}, wantState: "running", wantCreated: now.Unix() - 5, wantLabels: map[string]string{"aerial.service": "grafana", "aerial.image": "grafana"}},
		{name: "unhealthy service without image is exited and unstamped", svc: ServiceStatus{Name: "eri", Status: "unhealthy"}, wantState: "exited", wantCreated: now.Unix(), wantLabels: map[string]string{"aerial.service": "eri"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := synthesizeContainersFromServices([]ServiceStatus{tc.svc}, now)
			if len(got) != 1 {
				t.Fatalf("expected 1 container, got %d", len(got))
			}
			c := got[0]
			if c.State != tc.wantState || c.Created != tc.wantCreated {
				t.Errorf("state/created = %q/%d, want %q/%d", c.State, c.Created, tc.wantState, tc.wantCreated)
			}
			if !maps.Equal(c.Labels, tc.wantLabels) {
				t.Errorf("labels = %v, want %v", c.Labels, tc.wantLabels)
			}
		})
	}
}
