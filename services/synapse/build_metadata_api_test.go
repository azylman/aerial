package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// metadataAPI is a declarative GitHub API serving one repository's producer runs.
type metadataAPI struct {
	runs                                     []metadataWorkflowRun
	checks                                   []GitHubMetadataCheck
	paginate                                 bool
	parents                                  []string
	blob                                     string
	logs                                     []byte
	jobs                                     []GitHubWorkflowJobItem
	runStatus, checksStatus, blobStatus, cmt int
	listStatus, logsStatus                   int
}

func (f metadataAPI) serve(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var body any
	status := http.StatusOK
	switch {
	case len(parts) == 5 && parts[3] == "actions" && parts[4] == "runs":
		body, status = map[string]any{"workflow_runs": f.runs}, firstStatus(f.listStatus, status)
	case len(parts) == 6 && parts[3] == "actions" && parts[4] == "runs":
		body, status = f.runByID(parts[5])
	case len(parts) == 7 && parts[6] == "jobs":
		body = GitHubWorkflowJobsResponse{Jobs: f.jobs}
	case len(parts) == 9 && parts[8] == "logs":
		w.WriteHeader(firstStatus(f.logsStatus, http.StatusOK))
		_, _ = w.Write(f.logs)
		return
	case len(parts) == 6 && parts[3] == "commits" && parts[5] == "check-runs":
		body, status = GitHubCheckRunsResponse{CheckRuns: f.checkPage(r.URL.Query().Get("page"))}, firstStatus(f.checksStatus, status)
	case len(parts) == 5 && parts[3] == "commits":
		parents := []map[string]string{}
		for _, sha := range f.parents {
			parents = append(parents, map[string]string{"sha": sha})
		}
		body, status = map[string]any{"sha": parts[4], "parents": parents}, firstStatus(f.cmt, status)
	case len(parts) >= 5 && parts[3] == "contents":
		body, status = map[string]string{"sha": f.blob}, firstStatus(f.blobStatus, status)
	default:
		status = http.StatusNotFound
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f metadataAPI) runByID(id string) (any, int) {
	for _, run := range f.runs {
		if strconv.FormatInt(run.ID, 10) == id {
			return run, firstStatus(f.runStatus, http.StatusOK)
		}
	}
	return nil, http.StatusNotFound
}

// checkPage fills page 1 with native checks when paginating, so metadata only
// appears on page 2.
func (f metadataAPI) checkPage(page string) []GitHubMetadataCheck {
	if !f.paginate || page != "1" {
		return f.checks
	}
	natives := make([]GitHubMetadataCheck, metadataListPageSize)
	for i := range natives {
		natives[i] = GitHubMetadataCheck{ID: int64(i + 1), Name: fmt.Sprintf("native-%d", i)}
	}
	return natives
}

func firstStatus(override, status int) int {
	if override != 0 {
		return override
	}
	return status
}

// tRepoMeta is a main-branch identity for any producer repository.
func tRepoMeta(repo string, attempt int, svc, image string) BuildMetadata {
	m := tMeta(attempt)
	m.Repository, m.WorkflowPath = repo, metadataWorkflowPath(repo)
	if svc != "" {
		m.Selected = []BuildSelection{{Service: svc, ImageName: image}}
	}
	return m
}

func tRepoRun(repo string, attempt int, event, status, conclusion string) metadataWorkflowRun {
	r := tRun(attempt, status, conclusion)
	r.Repository.FullName, r.Path, r.Event = repo, metadataWorkflowPath(repo), event
	return r
}

// tPublished is a complete successful producer publication for one service.
func tPublished(m BuildMetadata) []GitHubMetadataCheck {
	out := []GitHubMetadataCheck{tCheck(withRunAttempt(m, 1))}
	results := make([]BuildResult, 0, len(m.Selected))
	for _, sel := range m.Selected {
		r := withRunAttempt(m, 1)
		r.Kind, r.Service, r.ImageName, r.State, r.Pushed, r.SourceAttempt = buildMetadataResult, sel.Service, sel.ImageName, conclusionSuccess, true, 1
		r.ImageRefs = []string{"ghcr.io/" + strings.Split(m.Repository, "/")[0] + "/" + sel.ImageName + ":" + tHead}
		out = append(out, tCheck(r))
		results = append(results, BuildResult{Service: sel.Service, ImageName: sel.ImageName, State: conclusionSuccess, Pushed: true, SourceAttempt: 1, ImageRefs: r.ImageRefs})
	}
	return append(out, tCheck(tFinalOf(m, results...)))
}

func withRunAttempt(m BuildMetadata, attempt int) BuildMetadata { m.RunAttempt = attempt; return m }

func withEvent(r metadataWorkflowRun, event string) metadataWorkflowRun { r.Event = event; return r }

func withRunPath(r metadataWorkflowRun, path string) metadataWorkflowRun { r.Path = path; return r }

func withMetaEvent(m BuildMetadata, event string) BuildMetadata { m.Event = event; return m }

func runs(rs ...metadataWorkflowRun) []metadataWorkflowRun { return rs }

func legacyJobs() []GitHubWorkflowJobItem {
	return []GitHubWorkflowJobItem{{Name: "Build & Push Images (engine)", Status: ghStatusCompleted, Conclusion: conclusionSuccess}}
}

func TestResolveStructuredWorkflowImages(t *testing.T) {
	t.Parallel()
	ok := tRun(1, ghStatusCompleted, conclusionSuccess)
	engine := tMeta(1, "engine")
	sidecars, display := "example/aerial-sidecars", "example/mirrormere"
	tests := []struct {
		name     string
		repo     string
		api      metadataAPI
		event    workflowRunEvent
		want     []string
		wantErr  error
		wantFail bool
		dbErr    error
	}{
		{"aerial service mapping", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"sidecar service mapping", sidecars, metadataAPI{runs: runs(tRepoRun(sidecars, 1, buildEventPush, ghStatusCompleted, conclusionSuccess)), checks: tPublished(tRepoMeta(sidecars, 1, "engine", "aerial-sidecar-engine"))}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"display service mapping", display, metadataAPI{runs: runs(tRepoRun(display, 1, buildEventPush, ghStatusCompleted, conclusionSuccess)), checks: tPublished(tRepoMeta(display, 1, "engine", "mirrormere-engine"))}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"manual dispatch", tRepo, metadataAPI{runs: runs(withEvent(ok, buildEventDispatch)), checks: tPublished(withMetaEvent(engine, buildEventDispatch))}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"renamed jobs and steps are irrelevant", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine), jobs: []GitHubWorkflowJobItem{{Name: "Ship it (other)", Status: ghStatusCompleted, Conclusion: conclusionSuccess}}}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"paginated transport", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine), paginate: true}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"explicit empty selection", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(tMeta(1))}, workflowRunEvent{tRunID, 1, tHead}, []string{}, nil, false, nil},
		{"missing metadata never means no deploy", tRepo, metadataAPI{runs: runs(ok), blob: tOther, jobs: legacyJobs()}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"inventoried historical run", tRepo, metadataAPI{runs: runs(ok), blob: legacyFixtureBlob, jobs: legacyJobs()}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"blob lookup failure blocks fallback", tRepo, metadataAPI{runs: runs(ok), blobStatus: http.StatusBadGateway, jobs: legacyJobs()}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"malformed record blocks inventoried fallback", tRepo, metadataAPI{runs: runs(ok), checks: checks(withText(tCheck(engine), "{")), blob: legacyFixtureBlob, jobs: legacyJobs()}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"manifest cannot promote", tRepo, metadataAPI{runs: runs(ok), checks: checks(tCheck(engine))}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"failed separate promotion", tRepo, metadataAPI{runs: runs(tRun(1, ghStatusCompleted, conclusionFailure)), checks: tPublished(engine)}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"rerun of a finished run waits for its own final", tRepo, metadataAPI{runs: runs(tRun(2, ghStatusCompleted, conclusionSuccess)), checks: tPublished(engine)}, workflowRunEvent{tRunID, 2, tHead}, nil, nil, true, nil},
		{"delayed prior-attempt webhook", tRepo, metadataAPI{runs: runs(tRun(2, ghStatusCompleted, conclusionSuccess)), checks: tPublished(engine)}, workflowRunEvent{tRunID, 1, tHead}, nil, errStaleWorkflowEvent, false, nil},
		{"API behind webhook retries", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, workflowRunEvent{tRunID, 2, tHead}, nil, nil, true, nil},
		{"webhook without attempt", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, workflowRunEvent{tRunID, 0, tHead}, nil, nil, true, nil},
		{"webhook head mismatch", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, workflowRunEvent{tRunID, 1, tOther}, nil, nil, true, nil},
		{"PR run never promotes", tRepo, metadataAPI{runs: runs(tPRRun(1, ghStatusCompleted, conclusionSuccess))}, workflowRunEvent{tRunID, 1, tHead}, nil, errNotImageWorkflow, false, nil},
		{"non-producer workflow", tRepo, metadataAPI{runs: runs(withRunPath(ok, "dynamic/dependabot/dependabot-updates"))}, workflowRunEvent{tRunID, 1, tHead}, nil, errNotImageWorkflow, false, nil},
		{"repository without producer", "example/aerial-config", metadataAPI{}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, false, nil},
		{"snapshot write failure never blocks promotion", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, workflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, errors.New("offline")},
		{"webhook without run", tRepo, metadataAPI{runs: runs(ok)}, workflowRunEvent{0, 1, tHead}, nil, nil, true, nil},
		{"final disagrees with transport", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)[2:]}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"run unavailable", tRepo, metadataAPI{runs: runs(ok), runStatus: http.StatusInternalServerError}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"checks unavailable", tRepo, metadataAPI{runs: runs(ok), checksStatus: http.StatusInternalServerError}, workflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(tc.api.serve))
			defer server.Close()
			cfg := legacyFixtureConfig(Config{GitHubAPIURL: server.URL})
			for i := range cfg.LegacyBuildWorkflows {
				cfg.LegacyBuildWorkflows[i].Repository = strings.Replace(cfg.LegacyBuildWorkflows[i].Repository, "azylman/", "example/", 1)
			}
			reg := &mockPRRegistry{recordBuildMetadataErr: tc.dbErr}
			srv := newTestRouter(cfg, server.Client(), reg, nil)
			got, err := srv.resolveWorkflowRunImages(context.Background(), tc.repo, tc.event, 42)
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) || tc.wantErr == nil && (err != nil) != tc.wantFail {
				t.Fatalf("err=%v want %v/fail=%v", err, tc.wantErr, tc.wantFail)
			}
			if err == nil && (len(got) != len(tc.want) || (len(got) > 0 && !reflect.DeepEqual(got, tc.want))) {
				t.Fatalf("images=%v want %v", got, tc.want)
			}
		})
	}
}

// tLegacyLogs is an archived attempt log whose checkout resolved revision.
func tLegacyLogs(revision string) []byte {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("build/1_checkout.txt")
	if err == nil {
		_, err = f.Write([]byte("t [command]/usr/bin/git log -1 --format=%H\nt " + revision + "\n"))
	}
	if err != nil || w.Close() != nil {
		panic("zip fixture")
	}
	return buf.Bytes()
}

func TestRefreshPRBuildMetadata(t *testing.T) {
	t.Parallel()
	pr := tPRMeta(1, "engine")
	prFinal := tFinalOf(pr, tImageResult("engine", conclusionSuccess, true, 1))
	prChecks := checks(tCheck(pr), tCheck(tResultOf(pr, "engine", conclusionSuccess, true)), tCheck(prFinal))
	prRun := tPRRun(1, ghStatusCompleted, conclusionSuccess)
	newer := tPRRun(1, "queued", "")
	newer.ID, newer.RunNumber = 78, 9
	newerFailed := newer
	newerFailed.Status, newerFailed.Conclusion = ghStatusCompleted, conclusionFailure
	open := PRBuildLifecycle{Status: prOpen, HeadSHA: tHead}
	merged := PRBuildLifecycle{Status: prMerged, HeadSHA: tOther, MergeSHA: tHead}
	bound := []string{tBase, tHead}
	pr43 := 43
	tests := []struct {
		name        string
		lifecycle   PRBuildLifecycle
		lifeErr     error
		dbErr       error
		api         metadataAPI
		wantKind    string
		wantUpdated bool
		wantErr     bool
	}{
		{"PR head final", open, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, buildMetadataFinal, true, false},
		{"Dependabot-free conflict status still tracked", PRBuildLifecycle{Status: prConflict, HeadSHA: tHead}, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, buildMetadataFinal, true, false},
		{"unbound execution revision", open, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: []string{tBase, tOther}}, "", false, true},
		{"execution commit unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, cmt: http.StatusNotFound}, "", false, true},
		{"stale PR revision", PRBuildLifecycle{Status: prOpen, HeadSHA: tOther}, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, "", false, false},
		{"merged PR follows main run", merged, nil, nil, metadataAPI{runs: runs(priorPRRun(), tRun(1, ghStatusCompleted, conclusionSuccess)), checks: tPublished(tMeta(1, "engine"))}, buildMetadataFinal, true, false},
		{"newer pending run hides older success", open, nil, nil, metadataAPI{runs: runs(prRun, newer), checks: prChecks, parents: bound}, "", false, false},
		{"newer failed run without metadata", open, nil, nil, metadataAPI{runs: runs(prRun, newerFailed), checks: prChecks, parents: bound, blob: tOther, logs: tLegacyLogs(tMerge)}, "", false, true},
		{"no eligible run yet", open, nil, nil, metadataAPI{}, "", false, false},
		{"wrong PR number", open, nil, nil, metadataAPI{runs: runs(prRun), checks: checks(tCheck(withPRNumber(pr, &pr43))), parents: bound}, "", false, true},
		{"inventoried historical PR revision", open, nil, nil, metadataAPI{runs: runs(prRun), parents: bound, blob: legacyFixtureBlob, logs: tLegacyLogs(tMerge)}, "", false, false},
		{"historical checkout unbound to PR head", open, nil, nil, metadataAPI{runs: runs(prRun), parents: []string{tBase, tOther}, blob: legacyFixtureBlob, logs: tLegacyLogs(tMerge)}, "", false, true},
		{"historical logs unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), parents: bound, blob: legacyFixtureBlob, logsStatus: http.StatusGone}, "", false, true},
		{"historical logs not an archive", open, nil, nil, metadataAPI{runs: runs(prRun), parents: bound, blob: legacyFixtureBlob, logs: []byte("x")}, "", false, true},
		{"historical checkout is the source head", open, nil, nil, metadataAPI{runs: runs(prRun), parents: bound, blob: legacyFixtureBlob, logs: tLegacyLogs(tHead)}, "", false, true},
		{"invalid workflow blob", open, nil, nil, metadataAPI{runs: runs(prRun), parents: bound, logs: tLegacyLogs(tMerge)}, "", false, true},
		{"run listing unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), listStatus: http.StatusBadGateway}, "", false, true},
		{"authoritative run unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), runStatus: http.StatusBadGateway}, "", false, true},
		{"authoritative run identity invalid", open, nil, nil, metadataAPI{runs: runs(tPRRun(0, "queued", ""))}, "", false, true},
		{"lifecycle unavailable", open, errors.New("offline"), nil, metadataAPI{}, "", false, true},
		{"snapshot write fails", open, nil, errors.New("offline"), metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, buildMetadataFinal, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(tc.api.serve))
			defer server.Close()
			cfg := Config{GitHubAPIURL: server.URL, LegacyBuildWorkflows: []LegacyBuildWorkflow{{Repository: tRepo, WorkflowPath: tPath, BlobSHA: legacyFixtureBlob}}}
			reg := &mockPRRegistry{buildLifecycle: tc.lifecycle, buildLifecycleErr: tc.lifeErr, recordBuildMetadataErr: tc.dbErr}
			srv := newTestRouter(cfg, server.Client(), reg, nil)
			updated, err := srv.refreshPRBuildMetadata(context.Background(), tRepo, 42, tHead)
			if (err != nil) != tc.wantErr || updated != tc.wantUpdated {
				t.Fatalf("updated=%v err=%v, want %v/%v", updated, err, tc.wantUpdated, tc.wantErr)
			}
			kind := ""
			if len(reg.recordedBuildMetadata) > 0 {
				kind = reg.recordedBuildMetadata[len(reg.recordedBuildMetadata)-1].Kind
			}
			if kind != tc.wantKind {
				t.Fatalf("recorded kind=%q want %q", kind, tc.wantKind)
			}
		})
	}
}

// priorPRRun is the PR's own run on the same revision, which a merged PR's cards no longer follow.
func priorPRRun() metadataWorkflowRun {
	r := tPRRun(1, ghStatusCompleted, conclusionSuccess)
	r.ID, r.RunNumber = 70, 7
	return r
}

func TestSupersedeStaleBuildAttempt(t *testing.T) {
	t.Parallel()
	stored := tFinalOf(tMeta(1, "engine"), tImageResult("engine", conclusionSuccess, true, 1))
	tests := []struct {
		name      string
		found     bool
		event     workflowRunEvent
		wantWrite bool
	}{
		{"newer attempt demotes prior success", true, workflowRunEvent{tRunID, 2, tHead}, true},
		{"same attempt keeps snapshot", true, workflowRunEvent{tRunID, 1, tHead}, false},
		{"different run untouched", true, workflowRunEvent{78, 2, tHead}, false},
		{"different revision untouched", true, workflowRunEvent{tRunID, 2, tOther}, false},
		{"no snapshot", false, workflowRunEvent{tRunID, 2, tHead}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{buildMetadata: stored, buildMetadataFound: tc.found}
			srv := newTestRouter(Config{}, nil, reg, nil)
			srv.supersedeStaleBuildAttempt(context.Background(), tRepo, 42, tc.event)
			if (len(reg.recordedBuildMetadata) > 0) != tc.wantWrite {
				t.Fatalf("writes=%v", reg.recordedBuildMetadata)
			}
			if tc.wantWrite {
				got := reg.recordedBuildMetadata[0]
				if got.Kind != buildMetadataManifest || got.RunAttempt != 2 || len(got.Results) != 0 || !reflect.DeepEqual(got.Selected, stored.Selected) {
					t.Fatalf("demoted snapshot=%+v", got)
				}
			}
		})
	}
}

func TestBuildMetadataEventIngestion(t *testing.T) {
	t.Parallel()
	pr := tPRMeta(1, "engine")
	prChecks := checks(tCheck(pr), tCheck(tResultOf(pr, "engine", buildResultRunning, false)))
	api := metadataAPI{runs: runs(tPRRun(1, "in_progress", "")), checks: prChecks, parents: []string{tBase, tHead}}
	checkRun := `{"action":"completed","repository":{"full_name":"example/aerial"},"check_run":{"name":"build-metadata-result","head_sha":"` + tHead + `","status":"completed","conclusion":"neutral","pull_requests":[{"number":42,"head":{"sha":"` + tHead + `","ref":"BRANCH"}}]}}`
	workflowRun := `{"action":"in_progress","repository":{"full_name":"example/aerial"},"workflow_run":{"id":77,"run_attempt":1,"path":".github/workflows/docker-publish.yml","event":"pull_request","name":"CD","head_sha":"` + tHead + `","head_branch":"BRANCH","status":"in_progress","pull_requests":[{"number":42,"head":{"sha":"` + tHead + `","ref":"BRANCH"}}]}}`
	tests := []struct {
		name, event, payload, branch string
		owner                        bool
		checksStatus                 int
		wantWrites                   int
		wantPublished                int
		wantErr                      bool
	}{
		{"Dependabot transport check ingests", ghEventCheckRun, checkRun, "dependabot/go_modules/x", true, 0, 1, 1, false},
		{"feature transport check ingests", ghEventCheckRun, checkRun, "feature", true, 0, 1, 1, false},
		{"foreign stack owner skips", ghEventCheckRun, checkRun, "", false, 0, 0, 0, false},
		{"transport API failure retries", ghEventCheckRun, checkRun, "feature", true, http.StatusBadGateway, 0, 0, true},
		{"workflow run refreshes progress", ghEventWorkflowRun, workflowRun, "feature", true, 0, 1, 0, false},
		{"workflow run refresh failure never blocks CI handling", ghEventWorkflowRun, workflowRun, "feature", true, http.StatusBadGateway, 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := api
			fixture.checksStatus = tc.checksStatus
			server := httptest.NewServer(http.HandlerFunc(fixture.serve))
			defer server.Close()
			reg := &mockPRRegistry{resolveBranch: tc.branch, resolveHeadSHA: tHead, buildLifecycle: PRBuildLifecycle{Status: prOpen, HeadSHA: tHead}}
			if tc.branch != "" {
				reg.resolveTargetID = "100000000000000001"
			}
			bus := &mockEventBus{}
			srv := newTestRouter(Config{GitHubAPIURL: server.URL, DefaultOwner: tc.owner, WebhookForwardURLs: forwardURLsUnlessOwner(tc.owner)}, server.Client(), reg, nil, bus)
			_, err := srv.ProcessGitHubEvent(context.Background(), tc.event, "delivery", []byte(strings.ReplaceAll(tc.payload, "BRANCH", tc.branch)))
			if (err != nil) != tc.wantErr || len(reg.recordedBuildMetadata) != tc.wantWrites {
				t.Fatalf("err=%v writes=%d, want err=%v writes=%d", err, len(reg.recordedBuildMetadata), tc.wantErr, tc.wantWrites)
			}
			published := 0
			for _, ev := range bus.publishedPREvents {
				if ev.Event == "check_updated" && ev.PRNumber == 42 && ev.CheckName == "" {
					published++
				}
			}
			if published != tc.wantPublished {
				t.Fatalf("published=%d want %d", published, tc.wantPublished)
			}
		})
	}
}

func forwardURLsUnlessOwner(owner bool) map[string]string {
	if owner {
		return nil
	}
	return map[string]string{"other": "http://127.0.0.1:1/forward"}
}
