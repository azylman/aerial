package buildmeta

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/model"
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

func tRun(attempt int, status, conclusion string) MetadataWorkflowRun {
	r := MetadataWorkflowRun{ID: tRunID, RunNumber: 8, RunAttempt: attempt, Path: tPath, Event: model.BuildEventPush, HeadSHA: tHead, HeadBranch: "main", Status: status, Conclusion: conclusion}
	r.Repository.FullName = tRepo
	return r
}

func tPRRun(attempt int, status, conclusion string) MetadataWorkflowRun {
	r := tRun(attempt, status, conclusion)
	r.Event, r.HeadBranch = model.GHEventPullRequest, "feature"
	return r
}

func tSelected(services ...string) []model.BuildSelection {
	out := make([]model.BuildSelection, 0, len(services))
	for _, svc := range services {
		out = append(out, model.BuildSelection{Service: svc, ImageName: "aerial-" + svc})
	}
	return out
}

func tMeta(attempt int, services ...string) model.BuildMetadata {
	return model.BuildMetadata{Version: 1, Kind: model.BuildMetadataManifest, Repository: tRepo, WorkflowPath: tPath, RunID: tRunID, RunNumber: 8, RunAttempt: attempt, Event: model.BuildEventPush, SourceSHA: tHead, ExecutionSHA: tHead, Selected: tSelected(services...)}
}

func tPRMeta(attempt int, services ...string) model.BuildMetadata {
	m := tMeta(attempt, services...)
	pr := 42
	m.Event, m.ExecutionSHA, m.PRNumber = model.GHEventPullRequest, tMerge, &pr
	return m
}

func tImageResult(svc, state string, pushed bool, sourceAttempt int) model.BuildResult {
	r := model.BuildResult{Service: svc, ImageName: "aerial-" + svc, State: state, Pushed: pushed, SourceAttempt: sourceAttempt}
	if pushed {
		r.ImageRefs, r.Digest = []string{"ghcr.io/example/aerial-" + svc + ":tree-" + tTree}, tDigest
	}
	return r
}

func tResultOf(m model.BuildMetadata, svc, state string, pushed bool) model.BuildMetadata {
	r := tImageResult(svc, state, pushed, m.RunAttempt)
	m.Kind, m.Service, m.ImageName, m.State, m.Pushed, m.SourceAttempt, m.ImageRefs, m.Digest = model.BuildMetadataResult, r.Service, r.ImageName, r.State, r.Pushed, r.SourceAttempt, r.ImageRefs, r.Digest
	return m
}

func tFinalOf(m model.BuildMetadata, results ...model.BuildResult) model.BuildMetadata {
	m.Kind, m.Results = model.BuildMetadataFinal, append([]model.BuildResult{}, results...)
	return m
}

func tCheck(m model.BuildMetadata) model.GitHubMetadataCheck {
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	c := model.GitHubMetadataCheck{ID: 100 + int64(m.RunAttempt), Name: model.BuildMetadataCheckName, HeadSHA: m.SourceSHA, ExternalID: MetadataExternalID(m), Status: "completed", Conclusion: "neutral"}
	if m.Kind == model.BuildMetadataResult {
		c.Name = model.BuildMetadataResultCheckName
	}
	c.App.ID = BuildMetadataActionsAppID
	c.CheckSuite.ID = 999
	c.Output.Text = string(raw)
	return c
}

func withApp(c model.GitHubMetadataCheck, app int64) model.GitHubMetadataCheck {
	c.App.ID = app
	return c
}

func withExternalID(c model.GitHubMetadataCheck, id string) model.GitHubMetadataCheck {
	c.ExternalID = id
	return c
}

func withHead(c model.GitHubMetadataCheck, sha string) model.GitHubMetadataCheck {
	c.HeadSHA = sha
	return c
}

func withText(c model.GitHubMetadataCheck, text string) model.GitHubMetadataCheck {
	c.Output.Text = text
	return c
}

func withName(c model.GitHubMetadataCheck, name string) model.GitHubMetadataCheck {
	c.Name = name
	return c
}

func withStatus(c model.GitHubMetadataCheck, status, conclusion string) model.GitHubMetadataCheck {
	c.Status, c.Conclusion = status, conclusion
	return c
}

func withSuite(c model.GitHubMetadataCheck, suite int64) model.GitHubMetadataCheck {
	c.CheckSuite.ID = suite
	return c
}

func withRunNumber(m model.BuildMetadata, n int64) model.BuildMetadata {
	m.RunNumber = n
	return m
}

func withRunID(m model.BuildMetadata, id int64) model.BuildMetadata {
	m.RunID = id
	return m
}

func withSelected(m model.BuildMetadata, sel []model.BuildSelection) model.BuildMetadata {
	m.Selected = sel
	return m
}

func withRefs(m model.BuildMetadata, refs ...string) model.BuildMetadata {
	m.ImageRefs = refs
	return m
}

func withDigest(m model.BuildMetadata, digest string) model.BuildMetadata {
	m.Digest = digest
	return m
}

func withPRNumber(m model.BuildMetadata, pr *int) model.BuildMetadata {
	m.PRNumber = pr
	return m
}

func withExecution(m model.BuildMetadata, sha string) model.BuildMetadata {
	m.ExecutionSHA = sha
	return m
}

func checks(cs ...model.GitHubMetadataCheck) []model.GitHubMetadataCheck { return cs }

func TestCurrentMetadataSnapshot(t *testing.T) {
	t.Parallel()
	m1, m2 := tMeta(1, "engine", "worker"), tMeta(2, "engine", "worker")
	ok1, ok2 := tResultOf(m1, "engine", model.ConclusionSuccess, true), tResultOf(m1, "worker", model.ConclusionSuccess, true)
	failWorker := tResultOf(m1, "worker", model.BuildResultFailure, false)
	retryWorker := tResultOf(m2, "worker", model.ConclusionSuccess, true)
	retryRunning := tResultOf(m2, "worker", model.BuildResultRunning, false)
	final1 := tFinalOf(m1, tImageResult("engine", model.ConclusionSuccess, true, 1), tImageResult("worker", model.ConclusionSuccess, true, 1))
	final2 := tFinalOf(m2, tImageResult("engine", model.ConclusionSuccess, true, 1), tImageResult("worker", model.ConclusionSuccess, true, 2))
	empty := tFinalOf(tMeta(1))
	otherRun := withRunID(tMeta(1, "engine"), 76)
	sidecarSuite := withSuite(tCheck(final1), 1234)
	tests := []struct {
		name        string
		records     []model.GitHubMetadataCheck
		run         MetadataWorkflowRun
		wantFound   bool
		wantKind    string
		wantResults map[string]string
		wantErr     bool
	}{
		{"no records", nil, tRun(1, "completed", model.ConclusionSuccess), false, "", nil, false},
		{"manifest only", checks(tCheck(m1)), tRun(1, "in_progress", ""), true, model.BuildMetadataManifest, nil, false},
		{"concurrent matrix progress", checks(tCheck(m1), tCheck(failWorker), tCheck(ok1)), tRun(1, "in_progress", ""), true, model.BuildMetadataProgress, map[string]string{"engine": model.ConclusionSuccess, "worker": model.BuildResultFailure}, false},
		{"result before manifest", checks(tCheck(ok1)), tRun(1, "in_progress", ""), true, model.BuildMetadataProgress, map[string]string{"engine": model.ConclusionSuccess}, false},
		{"final aggregate", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), tCheck(final1)), tRun(1, "completed", model.ConclusionSuccess), true, model.BuildMetadataFinal, map[string]string{"engine": model.ConclusionSuccess, "worker": model.ConclusionSuccess}, false},
		{"custom suite differs from actions suite", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), sidecarSuite), tRun(1, "completed", model.ConclusionSuccess), true, model.BuildMetadataFinal, map[string]string{"engine": model.ConclusionSuccess, "worker": model.ConclusionSuccess}, false},
		{"explicit empty selection", checks(tCheck(tMeta(1)), tCheck(empty)), tRun(1, "completed", model.ConclusionSuccess), true, model.BuildMetadataFinal, map[string]string{}, false},
		{"identical rerun replicas deduplicate", checks(tCheck(m1), tCheck(ok1), tCheck(ok1), tCheck(ok2), tCheck(final1)), tRun(1, "completed", model.ConclusionSuccess), true, model.BuildMetadataFinal, map[string]string{"engine": model.ConclusionSuccess, "worker": model.ConclusionSuccess}, false},
		{"rerun-failed retains prior success", checks(tCheck(m1), tCheck(ok1), tCheck(failWorker), tCheck(retryWorker), tCheck(final2)), tRun(2, "completed", model.ConclusionSuccess), true, model.BuildMetadataFinal, map[string]string{"engine": model.ConclusionSuccess, "worker": model.ConclusionSuccess}, false},
		{"rerun pending hides prior final", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), tCheck(final1)), tRun(2, "queued", ""), true, model.BuildMetadataManifest, nil, false},
		{"rerun progress shows only current attempt", checks(tCheck(m1), tCheck(ok1), tCheck(failWorker), tCheck(retryRunning)), tRun(2, "in_progress", ""), true, model.BuildMetadataProgress, map[string]string{"worker": model.BuildResultRunning}, false},
		{"final disagrees with newer result", checks(tCheck(m1), tCheck(ok1), tCheck(ok2), tCheck(retryRunning), tCheck(final2)), tRun(2, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"final claims missing transport", checks(tCheck(m1), tCheck(ok1), tCheck(final1)), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"other run on same commit ignored", checks(tCheck(otherRun), tCheck(m1)), tRun(1, "in_progress", ""), true, model.BuildMetadataManifest, nil, false},
		{"foreign app ignored", checks(withApp(tCheck(m1), 9)), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, false},
		{"conflicting replica", checks(tCheck(ok1), withText(tCheck(ok1), strings.Replace(tCheck(ok1).Output.Text, tDigest, strings.ReplaceAll(tDigest, "1", "2"), 1))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"conflicting selection", checks(tCheck(m1), tCheck(tResultOf(withSelected(m1, tSelected("engine")), "engine", model.BuildResultRunning, false))), tRun(1, "in_progress", ""), false, "", nil, true},
		{"malformed record", checks(withText(tCheck(m1), "```json\n{}\n```")), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"unattributable external id", checks(withExternalID(tCheck(m1), "build-metadata:v2:example/aerial")), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"external id names another repository", checks(withExternalID(tCheck(m1), "build-metadata:v1:other/aerial:77:1:manifest")), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"external id disagrees with kind", checks(withExternalID(tCheck(m1), "build-metadata:v1:example/aerial:77:1:final")), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"record claims another run", checks(withExternalID(tCheck(otherRun), MetadataExternalID(m1))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"wrong check name", checks(withName(tCheck(m1), "build-metadata-result")), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"in-progress custom check", checks(withStatus(tCheck(m1), "in_progress", "")), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"failed custom check", checks(withStatus(tCheck(m1), "completed", model.BuildResultFailure)), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"check on wrong commit", checks(withHead(tCheck(m1), tOther)), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"wrong run number", checks(tCheck(withRunNumber(m1, 9))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"future attempt", checks(tCheck(m2)), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"branch build with PR number", checks(tCheck(withPRNumber(m1, new(int)))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"branch build with distinct execution", checks(tCheck(withExecution(m1, tMerge))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"PR record on branch run", checks(tCheck(tPRMeta(1))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"PR merge identity", checks(tCheck(tPRMeta(1, "engine"))), tPRRun(1, "in_progress", ""), true, model.BuildMetadataManifest, nil, false},
		{"PR without merge revision", checks(tCheck(withExecution(tPRMeta(1), tHead))), tPRRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"service maps to foreign image", checks(tCheck(withSelected(m1, []model.BuildSelection{{Service: "engine", ImageName: "other-engine"}}))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
		{"none sentinel never a service", checks(tCheck(withSelected(m1, []model.BuildSelection{{Service: "none", ImageName: "aerial-none"}}))), tRun(1, "completed", model.ConclusionSuccess), false, "", nil, true},
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
			records, err := CollectMetadataRecords(tc.records, tRepo, tc.run)
			var got model.BuildMetadata
			found := false
			if err == nil {
				got, found, err = CurrentMetadataSnapshot(records, tc.run.RunAttempt)
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
			if _, err := model.ParseStoredBuildMetadata(raw); err != nil {
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
		snapshot model.BuildMetadata
		run      MetadataWorkflowRun
		want     bool
	}{
		{"successful final", tFinalOf(m, tImageResult("engine", model.ConclusionSuccess, true, 1)), tRun(2, "completed", model.ConclusionSuccess), true},
		{"explicit empty final", tFinalOf(tMeta(2)), tRun(2, "completed", model.ConclusionSuccess), true},
		{"prior attempt final", tFinalOf(tMeta(1, "engine"), tImageResult("engine", model.ConclusionSuccess, true, 1)), tRun(2, "completed", model.ConclusionSuccess), false},
		{"manifest", m, tRun(2, "completed", model.ConclusionSuccess), false},
		{"failed separate promotion", tFinalOf(m, tImageResult("engine", model.ConclusionSuccess, true, 1)), tRun(2, "completed", model.BuildResultFailure), false},
		{"skipped separate promotion", tFinalOf(m, tImageResult("engine", model.ConclusionSuccess, true, 1)), tRun(2, "completed", "skipped"), false},
		{"workflow still running", tFinalOf(m, tImageResult("engine", model.ConclusionSuccess, true, 1)), tRun(2, "in_progress", ""), false},
		{"compiled without push", tFinalOf(m, tImageResult("engine", model.ConclusionSuccess, false, 2)), tRun(2, "completed", model.ConclusionSuccess), false},
		{"partial failure", tFinalOf(m, tImageResult("engine", model.BuildResultFailure, false, 2)), tRun(2, "completed", model.ConclusionSuccess), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MetadataPublishable(tc.snapshot, tc.run); got != tc.want {
				t.Fatalf("publishable=%v want %v", got, tc.want)
			}
		})
	}
}

func TestValidateMetadataRunIdentity(t *testing.T) {
	t.Parallel()
	dispatch := tRun(1, "completed", model.ConclusionSuccess)
	dispatch.Event = model.BuildEventDispatch
	branch := tRun(1, "completed", model.ConclusionSuccess)
	branch.HeadBranch = "release"
	scheduled := tRun(1, "completed", model.ConclusionSuccess)
	scheduled.Event = "schedule"
	wrongPath := tRun(1, "completed", model.ConclusionSuccess)
	wrongPath.Path = ".github/workflows/other.yml"
	wrongRepo := tRun(1, "completed", model.ConclusionSuccess)
	wrongRepo.Repository.FullName = "other/aerial"
	badSHA := tRun(1, "completed", model.ConclusionSuccess)
	badSHA.HeadSHA = "sha123"
	tests := []struct {
		name    string
		run     MetadataWorkflowRun
		wantErr bool
	}{
		{"push on main", tRun(1, "completed", model.ConclusionSuccess), false},
		{"manual dispatch on main", dispatch, false},
		{"pull request", tPRRun(1, "completed", model.ConclusionSuccess), false},
		{"push off main", branch, true},
		{"unsupported event", scheduled, true},
		{"wrong workflow", wrongPath, true},
		{"wrong repository", wrongRepo, true},
		{"invalid head", badSHA, true},
		{"zero attempt", tRun(0, "completed", model.ConclusionSuccess), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateMetadataRunIdentity(tc.run, tRepo, tRunID); (err != nil) != tc.wantErr {
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
			repo, run, ok := ParseMetadataExternalRun(tc.id)
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
		{"open head", "open", tHead, "", tHead, true, true},
		{"ci failed head", "ci_failed", tHead, "", tHead, true, true},
		{"conflict head", "conflict", tHead, "", tHead, true, true},
		{"stale PR revision", "open", tOther, "", tHead, true, false},
		{"merged follows merge commit", "merged", tOther, tHead, tHead, false, true},
		{"synced follows merge commit", "synced", tOther, tHead, tHead, false, true},
		{"merged ignores PR head", "merged", tHead, tOther, tHead, false, false},
		{"deploying is terminal for builds", "deploying", tHead, tHead, tHead, false, false},
		{"unregistered", "", "", "", tHead, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pr, ok := PRBuildRevision(model.PRBuildLifecycle{Status: tc.status, HeadSHA: tc.head, MergeSHA: tc.merge}, tc.sha)
			if pr != tc.wantPR || ok != tc.wantOK {
				t.Fatalf("got pr=%v ok=%v", pr, ok)
			}
		})
	}
}

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
			var run MetadataWorkflowRun
			var resp model.GitHubCheckRunsResponse
			readJSONFixture(t, "testdata/readiness/"+tc.fixture+".run.json", &run)
			readJSONFixture(t, "testdata/readiness/"+tc.fixture+".checks.json", &resp)
			if err := ValidateMetadataRunIdentity(run, tc.repo, run.ID); err != nil {
				t.Fatal(err)
			}
			records, err := CollectMetadataRecords(resp.CheckRuns, tc.repo, run)
			if err != nil {
				t.Fatal(err)
			}
			m, found, err := CurrentMetadataSnapshot(records, run.RunAttempt)
			if err != nil || !found {
				t.Fatalf("found=%v err=%v", found, err)
			}
			if len(m.Selected) != tc.wantSelected || MetadataPublishable(m, run) != tc.wantPublishable || m.ExecutionSHA != tc.wantExecutionSHA {
				t.Fatalf("selected=%d publishable=%v execution=%s", len(m.Selected), MetadataPublishable(m, run), m.ExecutionSHA)
			}
		})
	}
}

func TestCheckoutRevisionFromArchive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		logs    []string
		invalid bool
		want    string
		wantErr bool
	}{
		{"checkout revision", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, false, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"repeated checkout", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, false, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"conflicting jobs", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "time [command]/usr/bin/git log -1 --format=%H\ntime bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, false, "", true},
		{"conflicting checkouts", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\ntime [command]/usr/bin/git log -1 --format=%H\ntime bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, false, "", true},
		{"missing checkout", []string{"a random SHA aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, false, "", true},
		{"invalid archive", nil, true, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			writer := zip.NewWriter(&buf)
			if _, err := writer.Create("logs/"); err != nil {
				t.Fatal(err)
			}
			for i, log := range tc.logs {
				entry, err := writer.Create(string(rune('a'+i)) + ".txt")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = entry.Write([]byte(log)); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			raw := buf.Bytes()
			if tc.invalid {
				raw = []byte("invalid")
			}
			got, err := CheckoutRevisionFromArchive(raw)
			if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
				t.Fatalf("revision=%q error=%v", got, err)
			}
		})
	}
}

func readJSONFixture(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		raw, err = os.ReadFile(filepath.Join("..", "..", path))
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, v); err != nil {
		t.Fatal(err)
	}
}
