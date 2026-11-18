package main

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

const (
	tRepo   = "example/aerial"
	tPath   = ".github/workflows/docker-publish.yml"
	tHead   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tBase   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	tMerge  = "cccccccccccccccccccccccccccccccccccccccc"
	tTree   = "dddddddddddddddddddddddddddddddddddddddd"
	tOther  = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	tDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	tRunID  = 77
)

// tRun is the authoritative main-branch producer run at an attempt.
func tRun(attempt int, status, conclusion string) metadataWorkflowRun {
	r := metadataWorkflowRun{ID: tRunID, RunNumber: 8, RunAttempt: attempt, Path: tPath, Event: buildEventPush, HeadSHA: tHead, HeadBranch: "main", Status: status, Conclusion: conclusion}
	r.Repository.FullName = tRepo
	return r
}

// tPRRun is the authoritative PR producer run for tHead.
func tPRRun(attempt int, status, conclusion string) metadataWorkflowRun {
	r := tRun(attempt, status, conclusion)
	r.Event, r.HeadBranch = ghEventPullRequest, "feature"
	return r
}

func tSelected(services ...string) []BuildSelection {
	out := make([]BuildSelection, 0, len(services))
	for _, svc := range services {
		out = append(out, BuildSelection{Service: svc, ImageName: "aerial-" + svc})
	}
	return out
}

// tMeta is a manifest identity for the main-branch run selecting services.
func tMeta(attempt int, services ...string) BuildMetadata {
	return BuildMetadata{Version: 1, Kind: buildMetadataManifest, Repository: tRepo, WorkflowPath: tPath, RunID: tRunID, RunNumber: 8, RunAttempt: attempt, Event: buildEventPush, SourceSHA: tHead, ExecutionSHA: tHead, Selected: tSelected(services...)}
}

func tPRMeta(attempt int, services ...string) BuildMetadata {
	m := tMeta(attempt, services...)
	pr := 42
	m.Event, m.ExecutionSHA, m.PRNumber = ghEventPullRequest, tMerge, &pr
	return m
}

func tImageResult(svc, state string, pushed bool, sourceAttempt int) BuildResult {
	r := BuildResult{Service: svc, ImageName: "aerial-" + svc, State: state, Pushed: pushed, SourceAttempt: sourceAttempt}
	if pushed {
		r.ImageRefs, r.Digest = []string{"ghcr.io/example/aerial-" + svc + ":tree-" + tTree}, tDigest
	}
	return r
}

// tResultOf turns an identity into a per-service result transport record.
func tResultOf(m BuildMetadata, svc, state string, pushed bool) BuildMetadata {
	r := tImageResult(svc, state, pushed, m.RunAttempt)
	m.Kind, m.Service, m.ImageName, m.State, m.Pushed, m.SourceAttempt, m.ImageRefs, m.Digest = buildMetadataResult, r.Service, r.ImageName, r.State, r.Pushed, r.SourceAttempt, r.ImageRefs, r.Digest
	return m
}

func tFinalOf(m BuildMetadata, results ...BuildResult) BuildMetadata {
	m.Kind, m.Results = buildMetadataFinal, append([]BuildResult{}, results...)
	return m
}

// tCheck publishes a record exactly as the producer does.
func tCheck(m BuildMetadata) GitHubMetadataCheck {
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	c := GitHubMetadataCheck{ID: 100 + int64(m.RunAttempt), Name: "build-metadata", HeadSHA: m.SourceSHA, ExternalID: metadataExternalID(m), Status: ghStatusCompleted, Conclusion: "neutral"}
	if m.Kind == buildMetadataResult {
		c.Name = "build-metadata-result"
	}
	c.App.ID = buildMetadataActionsAppID
	c.CheckSuite.ID = 999
	c.Output.Text = string(raw)
	return c
}

func withApp(c GitHubMetadataCheck, app int64) GitHubMetadataCheck { c.App.ID = app; return c }

func withExternalID(c GitHubMetadataCheck, id string) GitHubMetadataCheck {
	c.ExternalID = id
	return c
}

func withHead(c GitHubMetadataCheck, sha string) GitHubMetadataCheck { c.HeadSHA = sha; return c }

func withText(c GitHubMetadataCheck, text string) GitHubMetadataCheck { c.Output.Text = text; return c }

func withName(c GitHubMetadataCheck, name string) GitHubMetadataCheck { c.Name = name; return c }

func withStatus(c GitHubMetadataCheck, status, conclusion string) GitHubMetadataCheck {
	c.Status, c.Conclusion = status, conclusion
	return c
}

func withSuite(c GitHubMetadataCheck, suite int64) GitHubMetadataCheck {
	c.CheckSuite.ID = suite
	return c
}

func withRunNumber(m BuildMetadata, n int64) BuildMetadata { m.RunNumber = n; return m }

func withRunID(m BuildMetadata, id int64) BuildMetadata { m.RunID = id; return m }

func withSelected(m BuildMetadata, sel []BuildSelection) BuildMetadata { m.Selected = sel; return m }

func withRefs(m BuildMetadata, refs ...string) BuildMetadata { m.ImageRefs = refs; return m }

func withDigest(m BuildMetadata, digest string) BuildMetadata { m.Digest = digest; return m }

func withPRNumber(m BuildMetadata, pr *int) BuildMetadata { m.PRNumber = pr; return m }

func withExecution(m BuildMetadata, sha string) BuildMetadata { m.ExecutionSHA = sha; return m }

func checks(cs ...GitHubMetadataCheck) []GitHubMetadataCheck { return cs }

func TestCurrentMetadataSnapshot(t *testing.T) {
	t.Parallel()
	m1, m2 := tMeta(1, "engine", "worker"), tMeta(2, "engine", "worker")
	ok1, ok2 := tResultOf(m1, "engine", conclusionSuccess, true), tResultOf(m1, "worker", conclusionSuccess, true)
	failWorker := tResultOf(m1, "worker", buildResultFailure, false)
	retryWorker := tResultOf(m2, "worker", conclusionSuccess, true)
	retryRunning := tResultOf(m2, "worker", buildResultRunning, false)
	final1 := tFinalOf(m1, tImageResult("engine", conclusionSuccess, true, 1), tImageResult("worker", conclusionSuccess, true, 1))
	final2 := tFinalOf(m2, tImageResult("engine", conclusionSuccess, true, 1), tImageResult("worker", conclusionSuccess, true, 2))
	empty := tFinalOf(tMeta(1))
	otherRun := withRunID(tMeta(1, "engine"), 76)
	sidecarSuite := withSuite(tCheck(final1), 1234)
	tests := []struct {
		name        string
		records     []GitHubMetadataCheck
		run         metadataWorkflowRun
		wantFound   bool
		wantKind    string
		wantResults map[string]string
		wantErr     bool
	}{
		{"no records", nil, tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, false},
		{"manifest only", checks(tCheck(m1)), tRun(1, "in_progress", ""), true, buildMetadataManifest, nil, false},
		{"concurrent matrix progress", checks(tCheck(m1), tCheck(failWorker), tCheck(ok1)), tRun(1, "in_progress", ""), true, buildMetadataProgress, map[string]string{"engine": conclusionSuccess, "worker": buildResultFailure}, false},
		{"result before manifest", checks(tCheck(ok1)), tRun(1, "in_progress", ""), true, buildMetadataProgress, map[string]string{"engine": conclusionSuccess}, false},
		{"final aggregate", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), tCheck(final1)), tRun(1, ghStatusCompleted, conclusionSuccess), true, buildMetadataFinal, map[string]string{"engine": conclusionSuccess, "worker": conclusionSuccess}, false},
		{"custom suite differs from actions suite", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), sidecarSuite), tRun(1, ghStatusCompleted, conclusionSuccess), true, buildMetadataFinal, map[string]string{"engine": conclusionSuccess, "worker": conclusionSuccess}, false},
		{"explicit empty selection", checks(tCheck(tMeta(1)), tCheck(empty)), tRun(1, ghStatusCompleted, conclusionSuccess), true, buildMetadataFinal, map[string]string{}, false},
		{"identical rerun replicas deduplicate", checks(tCheck(m1), tCheck(ok1), tCheck(ok1), tCheck(ok2), tCheck(final1)), tRun(1, ghStatusCompleted, conclusionSuccess), true, buildMetadataFinal, map[string]string{"engine": conclusionSuccess, "worker": conclusionSuccess}, false},
		{"rerun-failed retains prior success", checks(tCheck(m1), tCheck(ok1), tCheck(failWorker), tCheck(retryWorker), tCheck(final2)), tRun(2, ghStatusCompleted, conclusionSuccess), true, buildMetadataFinal, map[string]string{"engine": conclusionSuccess, "worker": conclusionSuccess}, false},
		{"rerun pending hides prior final", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), tCheck(final1)), tRun(2, "queued", ""), true, buildMetadataManifest, nil, false},
		{"rerun progress shows only current attempt", checks(tCheck(m1), tCheck(ok1), tCheck(failWorker), tCheck(retryRunning)), tRun(2, "in_progress", ""), true, buildMetadataProgress, map[string]string{"worker": buildResultRunning}, false},
		{"final disagrees with newer result", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), tCheck(retryRunning), tCheck(final2)), tRun(2, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"final claims missing transport", checks(tCheck(m1), tCheck(ok1), tCheck(final1)), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"other run on same commit ignored", checks(tCheck(otherRun), tCheck(m1)), tRun(1, "in_progress", ""), true, buildMetadataManifest, nil, false},
		{"foreign app ignored", checks(withApp(tCheck(m1), 9)), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, false},
		{"conflicting replica", checks(tCheck(ok1), withText(tCheck(ok1), strings.Replace(tCheck(ok1).Output.Text, tDigest, strings.ReplaceAll(tDigest, "1", "2"), 1))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"conflicting selection", checks(tCheck(m1), tCheck(tResultOf(withSelected(m1, tSelected("engine")), "engine", buildResultRunning, false))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"malformed record", checks(withText(tCheck(m1), "```json\n{}\n```")), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"unattributable external id", checks(withExternalID(tCheck(m1), "build-metadata:v2:example/aerial")), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"external id names another repository", checks(withExternalID(tCheck(m1), "build-metadata:v1:other/aerial:77:1:manifest")), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"external id disagrees with kind", checks(withExternalID(tCheck(m1), "build-metadata:v1:example/aerial:77:1:final")), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"record claims another run", checks(withExternalID(tCheck(otherRun), metadataExternalID(m1))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"wrong check name", checks(withName(tCheck(m1), "build-metadata-result")), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"in-progress custom check", checks(withStatus(tCheck(m1), "in_progress", "")), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"failed custom check", checks(withStatus(tCheck(m1), ghStatusCompleted, conclusionFailure)), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"check on wrong commit", checks(withHead(tCheck(m1), tOther)), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"wrong run number", checks(tCheck(withRunNumber(m1, 9))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"future attempt", checks(tCheck(m2)), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"branch build with PR number", checks(tCheck(withPRNumber(m1, new(int)))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"branch build with distinct execution", checks(tCheck(withExecution(m1, tMerge))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"PR record on branch run", checks(tCheck(tPRMeta(1))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"PR merge identity", checks(tCheck(tPRMeta(1, "engine"))), tPRRun(1, "in_progress", ""), true, buildMetadataManifest, nil, false},
		{"PR without merge revision", checks(tCheck(withExecution(tPRMeta(1), tHead))), tPRRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"service maps to foreign image", checks(tCheck(withSelected(m1, []BuildSelection{{Service: "engine", ImageName: "other-engine"}}))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"none sentinel never a service", checks(tCheck(withSelected(m1, []BuildSelection{{Service: "none", ImageName: "aerial-none"}}))), tRun(1, ghStatusCompleted, conclusionSuccess), false, "", nil, true},
		{"image ref wrong owner", checks(tCheck(withRefs(ok1, "ghcr.io/other/aerial-engine:"+tHead))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"image ref wrong image", checks(tCheck(withRefs(ok1, "ghcr.io/example/aerial-worker:"+tHead))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"image ref mutable tag", checks(tCheck(withRefs(ok1, "ghcr.io/example/aerial-engine:latest"))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"pushed without image ref", checks(tCheck(withRefs(ok1))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"invalid digest", checks(tCheck(withDigest(ok1, "sha256:123"))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"result outside selection", checks(tCheck(withSelected(ok1, tSelected("worker")))), tRun(1, "in_progress", ""), false, "", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			records, err := collectMetadataRecords(tc.records, tRepo, tc.run)
			var got BuildMetadata
			found := false
			if err == nil {
				got, found, err = currentMetadataSnapshot(records, tc.run.RunAttempt)
			}
			if (err != nil) != tc.wantErr || found != tc.wantFound {
				t.Fatalf("found=%v err=%v, want found=%v err=%v", found, err, tc.wantFound, tc.wantErr)
			}
			if !found {
				return
			}
			states := map[string]string{}
			for _, r := range got.Results {
				states[r.Service] = r.State
			}
			if got.Kind != tc.wantKind || got.RunAttempt != tc.run.RunAttempt || (tc.wantResults != nil && !reflect.DeepEqual(states, tc.wantResults)) || (tc.wantResults == nil && len(got.Results) > 0) {
				t.Fatalf("snapshot kind=%s attempt=%d results=%v, want %s/%d/%v", got.Kind, got.RunAttempt, states, tc.wantKind, tc.run.RunAttempt, tc.wantResults)
			}
			raw, _ := json.Marshal(got)
			if _, err := parseStoredBuildMetadata(raw); err != nil {
				t.Fatalf("snapshot is not storable: %v", err)
			}
		})
	}
}

func TestMetadataPublishable(t *testing.T) {
	t.Parallel()
	m := tMeta(2, "engine")
	tests := []struct {
		name     string
		snapshot BuildMetadata
		run      metadataWorkflowRun
		want     bool
	}{
		{"successful final", tFinalOf(m, tImageResult("engine", conclusionSuccess, true, 1)), tRun(2, ghStatusCompleted, conclusionSuccess), true},
		{"explicit empty final", tFinalOf(tMeta(2)), tRun(2, ghStatusCompleted, conclusionSuccess), true},
		{"prior attempt final", tFinalOf(tMeta(1, "engine"), tImageResult("engine", conclusionSuccess, true, 1)), tRun(2, ghStatusCompleted, conclusionSuccess), false},
		{"manifest", m, tRun(2, ghStatusCompleted, conclusionSuccess), false},
		{"failed separate promotion", tFinalOf(m, tImageResult("engine", conclusionSuccess, true, 1)), tRun(2, ghStatusCompleted, conclusionFailure), false},
		{"skipped separate promotion", tFinalOf(m, tImageResult("engine", conclusionSuccess, true, 1)), tRun(2, ghStatusCompleted, "skipped"), false},
		{"workflow still running", tFinalOf(m, tImageResult("engine", conclusionSuccess, true, 1)), tRun(2, "in_progress", ""), false},
		{"compiled without push", tFinalOf(m, tImageResult("engine", conclusionSuccess, false, 2)), tRun(2, ghStatusCompleted, conclusionSuccess), false},
		{"partial failure", tFinalOf(m, tImageResult("engine", buildResultFailure, false, 2)), tRun(2, ghStatusCompleted, conclusionSuccess), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := metadataPublishable(tc.snapshot, tc.run); got != tc.want {
				t.Fatalf("publishable=%v want %v", got, tc.want)
			}
		})
	}
}

func TestValidateMetadataRunIdentity(t *testing.T) {
	t.Parallel()
	dispatch := tRun(1, ghStatusCompleted, conclusionSuccess)
	dispatch.Event = buildEventDispatch
	branch := tRun(1, ghStatusCompleted, conclusionSuccess)
	branch.HeadBranch = "release"
	scheduled := tRun(1, ghStatusCompleted, conclusionSuccess)
	scheduled.Event = "schedule"
	wrongPath := tRun(1, ghStatusCompleted, conclusionSuccess)
	wrongPath.Path = ".github/workflows/other.yml"
	wrongRepo := tRun(1, ghStatusCompleted, conclusionSuccess)
	wrongRepo.Repository.FullName = "other/aerial"
	badSHA := tRun(1, ghStatusCompleted, conclusionSuccess)
	badSHA.HeadSHA = "sha123"
	tests := []struct {
		name    string
		run     metadataWorkflowRun
		wantErr bool
	}{
		{"push on main", tRun(1, ghStatusCompleted, conclusionSuccess), false},
		{"manual dispatch on main", dispatch, false},
		{"pull request", tPRRun(1, ghStatusCompleted, conclusionSuccess), false},
		{"push off main", branch, true},
		{"unsupported event", scheduled, true},
		{"wrong workflow", wrongPath, true},
		{"wrong repository", wrongRepo, true},
		{"invalid head", badSHA, true},
		{"zero attempt", tRun(0, ghStatusCompleted, conclusionSuccess), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := validateMetadataRunIdentity(tc.run, tRepo, tRunID); (err != nil) != tc.wantErr {
				t.Fatalf("err=%v want error=%v", err, tc.wantErr)
			}
		})
	}
}

func TestParseMetadataExternalRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, id string
		wantRepo string
		wantRun  int64
		wantOK   bool
	}{
		{"result", "build-metadata:v1:example/aerial:77:2:engine", "example/aerial", 77, true},
		{"final", "build-metadata:v1:example/aerial:77:1:final", "example/aerial", 77, true},
		{"wrong version", "build-metadata:v2:example/aerial:77:1:final", "", 0, false},
		{"missing suffix", "build-metadata:v1:example/aerial:77:1:", "", 0, false},
		{"bad run", "build-metadata:v1:example/aerial:x:1:final", "", 0, false},
		{"bad attempt", "build-metadata:v1:example/aerial:77:0:final", "", 0, false},
		{"extra part", "build-metadata:v1:example/aerial:77:1:final:x", "", 0, false},
		{"empty", "", "", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo, run, ok := parseMetadataExternalRun(tc.id)
			if repo != tc.wantRepo || run != tc.wantRun || ok != tc.wantOK {
				t.Fatalf("got %q/%d/%v", repo, run, ok)
			}
		})
	}
}

func TestPRBuildRevision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, head, merge, sha string
		wantPR, wantOK                 bool
	}{
		{"open head", prOpen, tHead, "", tHead, true, true},
		{"ci failed head", prCIFailed, tHead, "", tHead, true, true},
		{"conflict head", prConflict, tHead, "", tHead, true, true},
		{"stale PR revision", prOpen, tOther, "", tHead, true, false},
		{"merged follows merge commit", prMerged, tOther, tHead, tHead, false, true},
		{"synced follows merge commit", prSynced, tOther, tHead, tHead, false, true},
		{"merged ignores PR head", prMerged, tHead, tOther, tHead, false, false},
		{"deploying is terminal for builds", statusDeploying, tHead, tHead, tHead, false, false},
		{"unregistered", "", "", "", tHead, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pr, ok := prBuildRevision(PRBuildLifecycle{Status: tc.status, HeadSHA: tc.head, MergeSHA: tc.merge}, tc.sha)
			if pr != tc.wantPR || ok != tc.wantOK {
				t.Fatalf("got pr=%v ok=%v", pr, ok)
			}
		})
	}
}

func TestBuildPRRolloutStateStructuredCards(t *testing.T) {
	t.Parallel()
	m := tMeta(2, "engine", "worker", "web", "api", "cron")
	progress := m
	progress.Kind = buildMetadataProgress
	progress.Results = []BuildResult{tImageResult("engine", buildResultRunning, false, 2), tImageResult("worker", conclusionSuccess, true, 2), tImageResult("web", conclusionSuccess, false, 2), tImageResult("api", buildResultCancelled, false, 2)}
	tests := []struct {
		name         string
		meta         map[string]any
		wantExpected []string
		wantStates   map[string]string
	}{
		{"no snapshot", map[string]any{"jobs": []any{"git-sync"}}, []string{"git-sync"}, nil},
		{"manifest pending", map[string]any{"build_metadata": tMeta(1, "engine")}, []string{"engine"}, map[string]string{"engine": "image_pending"}},
		{"explicit empty", map[string]any{"build_metadata": tFinalOf(tMeta(1))}, nil, nil},
		{"progress states", map[string]any{"build_metadata": progress}, []string{"engine", "worker", "web", "api", "cron"}, map[string]string{"engine": "image_building", "worker": "image_built", "web": "image_unpublished", "api": "image_failed", "cron": "image_pending"}},
		{"deployment state wins", map[string]any{"jobs": []any{"engine"}, "started_jobs": []any{"engine"}, "job_states": map[string]any{"engine": "verifying_health"}, "build_metadata": tMeta(1, "engine")}, []string{"engine"}, map[string]string{"engine": "verifying_health"}},
		{"raw registry JSON revalidated", map[string]any{"build_metadata": map[string]any{"kind": "final"}}, nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := buildPRRolloutState(tc.meta)
			var expected []string
			var states map[string]string
			if got != nil {
				expected, states = got.ExpectedJobs, got.JobStates
			}
			if !reflect.DeepEqual(expected, tc.wantExpected) || (len(tc.wantStates) > 0 && !reflect.DeepEqual(states, tc.wantStates)) || (len(tc.wantStates) == 0 && len(states) > 0) {
				t.Fatalf("expected=%v states=%v, want %v %v", expected, states, tc.wantExpected, tc.wantStates)
			}
		})
	}
}

// TestReadinessFixtureContracts replays real producer output fetched from the
// GitHub API during the readiness gate, so the consumer stays bound to what the
// three producers actually publish (including a sibling run on the same commit).
func TestReadinessFixtureContracts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, fixture, repo string
		wantSelected        int
		wantPublishable     bool
		wantExecutionSHA    string
	}{
		{"aerial push with separate promotion", "aerial-push-44221983780", "azylman/aerial", 6, true, "21cf1e08dae2e2b641901bce84bdd8a0db382969"},
		{"aerial PR merge revision", "aerial-pr-44221326112", "azylman/aerial", 6, true, "ca65104a24f394d9de0aef6b14e306553d847a53"},
		{"aerial push with explicit empty selection", "aerial-push-44235493552", "azylman/aerial", 0, true, "b1fe25c11a45dfa6c721b0109f32e14a6a01fe46"},
		{"mirrormere manual dispatch with sibling run", "mirrormere-dispatch-44213767225", "azylman/mirrormere", 8, true, "62d0d57b408cdb273684d6440d77eada1d9d75d7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var run metadataWorkflowRun
			var resp GitHubCheckRunsResponse
			readJSONFixture(t, "testdata/readiness/"+tc.fixture+".run.json", &run)
			readJSONFixture(t, "testdata/readiness/"+tc.fixture+".checks.json", &resp)
			if err := validateMetadataRunIdentity(run, tc.repo, run.ID); err != nil {
				t.Fatal(err)
			}
			records, err := collectMetadataRecords(resp.CheckRuns, tc.repo, run)
			if err != nil {
				t.Fatal(err)
			}
			m, found, err := currentMetadataSnapshot(records, run.RunAttempt)
			if err != nil || !found {
				t.Fatalf("found=%v err=%v", found, err)
			}
			if len(m.Selected) != tc.wantSelected || metadataPublishable(m, run) != tc.wantPublishable || m.ExecutionSHA != tc.wantExecutionSHA {
				t.Fatalf("selected=%d publishable=%v execution=%s", len(m.Selected), metadataPublishable(m, run), m.ExecutionSHA)
			}
		})
	}
}

func readJSONFixture(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, v); err != nil {
		t.Fatal(err)
	}
}
