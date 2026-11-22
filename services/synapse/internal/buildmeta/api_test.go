package buildmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type metadataAPI struct {
	runs                                     []MetadataWorkflowRun
	checks                                   []model.GitHubMetadataCheck
	paginate                                 bool
	parents                                  []string
	runStatus, checksStatus, cmt, listStatus int
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
	case len(parts) == 6 && parts[3] == "commits" && parts[5] == "check-runs":
		body, status = model.GitHubCheckRunsResponse{CheckRuns: f.checkPage(r.URL.Query().Get("page"))}, firstStatus(f.checksStatus, status)
	case len(parts) == 5 && parts[3] == "commits":
		parents := make([]map[string]string, 0, len(f.parents))
		for _, sha := range f.parents {
			parents = append(parents, map[string]string{"sha": sha})
		}
		body, status = map[string]any{"sha": parts[4], "parents": parents}, firstStatus(f.cmt, status)
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

func (f metadataAPI) checkPage(page string) []model.GitHubMetadataCheck {
	if !f.paginate || page != "1" {
		return f.checks
	}
	natives := make([]model.GitHubMetadataCheck, MetadataListPageSize)
	for i := range natives {
		natives[i] = model.GitHubMetadataCheck{ID: int64(i + 1), Name: fmt.Sprintf("native-%d", i)}
	}
	return natives
}

func firstStatus(override, status int) int {
	if override != 0 {
		return override
	}
	return status
}

type testGitHubGetter struct {
	client  *http.Client
	baseURL string
}

func (g *testGitHubGetter) GetGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.baseURL+path, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxBytes)).Decode(out)
}

type testPRRegistry struct {
	lifecycle model.PRBuildLifecycle
	lifeErr   error
	recordErr error
	metadata  prwire.BuildMetadata
	hasMeta   bool
	recorded  []prwire.BuildMetadata
}

func (r *testPRRegistry) LoadPRBuildLifecycle(ctx context.Context, repo string, prNumber int) (model.PRBuildLifecycle, error) {
	return r.lifecycle, r.lifeErr
}

func (r *testPRRegistry) RecordPRBuildMetadata(ctx context.Context, repo string, prNumber int, metadata prwire.BuildMetadata) (bool, error) {
	r.recorded = append(r.recorded, metadata)
	if r.recordErr != nil {
		return false, r.recordErr
	}
	r.metadata = metadata
	r.hasMeta = true
	return true, nil
}

func (r *testPRRegistry) LoadPRBuildMetadata(ctx context.Context, repo string, prNumber int) (prwire.BuildMetadata, bool, error) {
	return r.metadata, r.hasMeta, r.lifeErr
}

func tRepoMeta(repo string, attempt int, svc, image string) prwire.BuildMetadata {
	m := tMeta(attempt)
	m.Repository, m.WorkflowPath = repo, MetadataWorkflowPath(repo)
	if svc != "" {
		m.Selected = []prwire.BuildSelection{{Service: svc, ImageName: image}}
	}
	return m
}

func tRepoRun(repo string, attempt int, event, status, conclusion string) MetadataWorkflowRun {
	r := tRun(attempt, status, conclusion)
	r.Repository.FullName, r.Path, r.Event = repo, MetadataWorkflowPath(repo), event
	return r
}

func tPublished(m prwire.BuildMetadata) []model.GitHubMetadataCheck {
	out := []model.GitHubMetadataCheck{tCheck(withRunAttempt(m, 1))}
	results := make([]prwire.BuildResult, 0, len(m.Selected))
	for _, sel := range m.Selected {
		r := withRunAttempt(m, 1)
		r.Kind, r.Service, r.ImageName, r.State, r.Pushed, r.SourceAttempt = prwire.BuildMetadataResult, sel.Service, sel.ImageName, model.ConclusionSuccess, true, 1
		r.ImageRefs = []string{"ghcr.io/" + strings.Split(m.Repository, "/")[0] + "/" + sel.ImageName + ":" + tHead}
		out = append(out, tCheck(r))
		results = append(results, prwire.BuildResult{Service: sel.Service, ImageName: sel.ImageName, State: model.ConclusionSuccess, Pushed: true, SourceAttempt: 1, ImageRefs: r.ImageRefs})
	}
	return append(out, tCheck(tFinalOf(m, results...)))
}

func withRunAttempt(m prwire.BuildMetadata, attempt int) prwire.BuildMetadata {
	m.RunAttempt = attempt
	return m
}

func withEvent(r MetadataWorkflowRun, event string) MetadataWorkflowRun {
	r.Event = event
	return r
}

func withRunPath(r MetadataWorkflowRun, path string) MetadataWorkflowRun {
	r.Path = path
	return r
}

func withMetaEvent(m prwire.BuildMetadata, event string) prwire.BuildMetadata {
	m.Event = event
	return m
}

func runs(rs ...MetadataWorkflowRun) []MetadataWorkflowRun { return rs }

func priorPRRun() MetadataWorkflowRun {
	r := tPRRun(1, "completed", model.ConclusionSuccess)
	r.ID, r.RunNumber = 70, 7
	return r
}

func TestResolveStructuredWorkflowImages(t *testing.T) {
	t.Parallel()
	ok := tRun(1, "completed", model.ConclusionSuccess)
	engine := tMeta(1, "engine")
	sidecars, display := "example/aerial-sidecars", "example/mirrormere"
	tests := []struct {
		name     string
		repo     string
		api      metadataAPI
		event    WorkflowRunEvent
		want     []string
		wantErr  error
		wantFail bool
		dbErr    error
	}{
		{"aerial service mapping", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"sidecar service mapping", sidecars, metadataAPI{runs: runs(tRepoRun(sidecars, 1, model.BuildEventPush, "completed", model.ConclusionSuccess)), checks: tPublished(tRepoMeta(sidecars, 1, "engine", "aerial-sidecar-engine"))}, WorkflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"display service mapping", display, metadataAPI{runs: runs(tRepoRun(display, 1, model.BuildEventPush, "completed", model.ConclusionSuccess)), checks: tPublished(tRepoMeta(display, 1, "engine", "mirrormere-engine"))}, WorkflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"manual dispatch", tRepo, metadataAPI{runs: runs(withEvent(ok, model.BuildEventDispatch)), checks: tPublished(withMetaEvent(engine, model.BuildEventDispatch))}, WorkflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"paginated transport", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine), paginate: true}, WorkflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, nil},
		{"explicit empty selection", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(tMeta(1))}, WorkflowRunEvent{tRunID, 1, tHead}, []string{}, nil, false, nil},
		{"missing metadata never means no deploy", tRepo, metadataAPI{runs: runs(ok)}, WorkflowRunEvent{tRunID, 1, tHead}, nil, ErrBuildMetadataMissing, false, nil},
		{"malformed record blocks promotion", tRepo, metadataAPI{runs: runs(ok), checks: checks(withText(tCheck(engine), "{"))}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"manifest cannot promote", tRepo, metadataAPI{runs: runs(ok), checks: checks(tCheck(engine))}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"failed separate promotion", tRepo, metadataAPI{runs: runs(tRun(1, "completed", model.BuildResultFailure)), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"rerun of a finished run waits for its own final", tRepo, metadataAPI{runs: runs(tRun(2, "completed", model.ConclusionSuccess)), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 2, tHead}, nil, nil, true, nil},
		{"delayed prior-attempt webhook", tRepo, metadataAPI{runs: runs(tRun(2, "completed", model.ConclusionSuccess)), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 1, tHead}, nil, ErrStaleWorkflowEvent, false, nil},
		{"API behind webhook retries", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 2, tHead}, nil, nil, true, nil},
		{"webhook without attempt", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 0, tHead}, nil, nil, true, nil},
		{"webhook head mismatch", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 1, tOther}, nil, nil, true, nil},
		{"PR run never promotes", tRepo, metadataAPI{runs: runs(tPRRun(1, "completed", model.ConclusionSuccess))}, WorkflowRunEvent{tRunID, 1, tHead}, nil, ErrNotImageWorkflow, false, nil},
		{"non-producer workflow", tRepo, metadataAPI{runs: runs(withRunPath(ok, "dynamic/dependabot/dependabot-updates"))}, WorkflowRunEvent{tRunID, 1, tHead}, nil, ErrNotImageWorkflow, false, nil},
		{"repository without producer", "example/aerial-config", metadataAPI{}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, false, nil},
		{"snapshot write failure never blocks promotion", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)}, WorkflowRunEvent{tRunID, 1, tHead}, []string{"ghcr.io/example/engine:latest"}, nil, false, errors.New("offline")},
		{"webhook without run", tRepo, metadataAPI{runs: runs(ok)}, WorkflowRunEvent{0, 1, tHead}, nil, nil, true, nil},
		{"final disagrees with transport", tRepo, metadataAPI{runs: runs(ok), checks: tPublished(engine)[2:]}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"run unavailable", tRepo, metadataAPI{runs: runs(ok), runStatus: http.StatusInternalServerError}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
		{"checks unavailable", tRepo, metadataAPI{runs: runs(ok), checksStatus: http.StatusInternalServerError}, WorkflowRunEvent{tRunID, 1, tHead}, nil, nil, true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(tc.api.serve))
			defer server.Close()
			reg := &testPRRegistry{recordErr: tc.dbErr}
			gh := &testGitHubGetter{client: server.Client(), baseURL: server.URL}
			c := NewConsumer(gh, reg)

			got, err := c.ResolveWorkflowRunImages(context.Background(), tc.repo, tc.event, 42)
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) || tc.wantErr == nil && (err != nil) != tc.wantFail {
				t.Fatalf("err=%v want %v/fail=%v", err, tc.wantErr, tc.wantFail)
			}
			if err == nil && (len(got) != len(tc.want) || (len(got) > 0 && !reflect.DeepEqual(got, tc.want))) {
				t.Fatalf("images=%v want %v", got, tc.want)
			}
		})
	}
}

func TestRefreshPRBuildMetadata(t *testing.T) {
	t.Parallel()
	pr := tPRMeta(1, "engine")
	prFinal := tFinalOf(pr, tImageResult("engine", model.ConclusionSuccess, true, 1))
	prChecks := checks(tCheck(pr), tCheck(tResultOf(pr, "engine", model.ConclusionSuccess, true)), tCheck(prFinal))
	prRun := tPRRun(1, "completed", model.ConclusionSuccess)
	newer := tPRRun(1, "queued", "")
	newer.ID, newer.RunNumber = 78, 9
	newerFailed := newer
	newerFailed.Status, newerFailed.Conclusion = "completed", model.BuildResultFailure
	open := model.PRBuildLifecycle{Status: "open", HeadSHA: tHead}
	merged := model.PRBuildLifecycle{Status: "merged", HeadSHA: tOther, MergeSHA: tHead}
	bound := []string{tBase, tHead}
	pr43 := 43
	tests := []struct {
		name        string
		lifecycle   model.PRBuildLifecycle
		lifeErr     error
		dbErr       error
		api         metadataAPI
		wantKind    string
		wantUpdated bool
		wantErr     bool
	}{
		{"PR head final", open, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, prwire.BuildMetadataFinal, true, false},
		{"Dependabot-free conflict status still tracked", model.PRBuildLifecycle{Status: "conflict", HeadSHA: tHead}, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, prwire.BuildMetadataFinal, true, false},
		{"unbound execution revision", open, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: []string{tBase, tOther}}, "", false, true},
		{"execution commit unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, cmt: http.StatusNotFound}, "", false, true},
		{"stale PR revision", model.PRBuildLifecycle{Status: "open", HeadSHA: tOther}, nil, nil, metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, "", false, false},
		{"merged PR follows main run", merged, nil, nil, metadataAPI{runs: runs(priorPRRun(), tRun(1, "completed", model.ConclusionSuccess)), checks: tPublished(tMeta(1, "engine"))}, prwire.BuildMetadataFinal, true, false},
		{"newer pending run hides older success", open, nil, nil, metadataAPI{runs: runs(prRun, newer), checks: prChecks, parents: bound}, "", false, false},
		{"newer failed run without metadata", open, nil, nil, metadataAPI{runs: runs(prRun, newerFailed), checks: prChecks, parents: bound}, "", false, true},
		{"no eligible run yet", open, nil, nil, metadataAPI{}, "", false, false},
		{"wrong PR number", open, nil, nil, metadataAPI{runs: runs(prRun), checks: checks(tCheck(withPRNumber(pr, &pr43))), parents: bound}, "", false, true},
		{"completed PR run without metadata", open, nil, nil, metadataAPI{runs: runs(prRun), parents: bound}, "", false, true},
		{"run listing unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), listStatus: http.StatusBadGateway}, "", false, true},
		{"authoritative run unavailable", open, nil, nil, metadataAPI{runs: runs(prRun), runStatus: http.StatusBadGateway}, "", false, true},
		{"authoritative run identity invalid", open, nil, nil, metadataAPI{runs: runs(tPRRun(0, "queued", ""))}, "", false, true},
		{"lifecycle unavailable", open, errors.New("offline"), nil, metadataAPI{}, "", false, true},
		{"snapshot write fails", open, nil, errors.New("offline"), metadataAPI{runs: runs(prRun), checks: prChecks, parents: bound}, prwire.BuildMetadataFinal, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(tc.api.serve))
			defer server.Close()
			reg := &testPRRegistry{lifecycle: tc.lifecycle, lifeErr: tc.lifeErr, recordErr: tc.dbErr}
			gh := &testGitHubGetter{client: server.Client(), baseURL: server.URL}
			c := NewConsumer(gh, reg)

			updated, err := c.RefreshPRBuildMetadata(context.Background(), tRepo, 42, tHead)
			if (err != nil) != tc.wantErr || updated != tc.wantUpdated {
				t.Fatalf("updated=%v err=%v, want %v/%v", updated, err, tc.wantUpdated, tc.wantErr)
			}
			kind := ""
			if len(reg.recorded) > 0 {
				kind = reg.recorded[len(reg.recorded)-1].Kind
			}
			if kind != tc.wantKind {
				t.Fatalf("recorded kind=%q want %q", kind, tc.wantKind)
			}
		})
	}
}

func TestSupersedeStaleBuildAttempt(t *testing.T) {
	t.Parallel()
	stored := tFinalOf(tMeta(1, "engine"), tImageResult("engine", model.ConclusionSuccess, true, 1))
	tests := []struct {
		name      string
		hasMeta   bool
		event     WorkflowRunEvent
		wantWrite bool
	}{
		{"newer attempt demotes prior success", true, WorkflowRunEvent{tRunID, 2, tHead}, true},
		{"same attempt keeps snapshot", true, WorkflowRunEvent{tRunID, 1, tHead}, false},
		{"different run untouched", true, WorkflowRunEvent{78, 2, tHead}, false},
		{"different revision untouched", true, WorkflowRunEvent{tRunID, 2, tOther}, false},
		{"no snapshot", false, WorkflowRunEvent{tRunID, 2, tHead}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &testPRRegistry{metadata: stored, hasMeta: tc.hasMeta}
			c := NewConsumer(nil, reg)
			c.SupersedeStaleBuildAttempt(context.Background(), tRepo, 42, tc.event)
			if (len(reg.recorded) > 0) != tc.wantWrite {
				t.Fatalf("writes=%v", reg.recorded)
			}
			if tc.wantWrite {
				got := reg.recorded[0]
				if got.Kind != prwire.BuildMetadataManifest || got.RunAttempt != 2 || len(got.Results) != 0 || !reflect.DeepEqual(got.Selected, stored.Selected) {
					t.Fatalf("demoted snapshot=%+v", got)
				}
			}
		})
	}
}

func TestConsumer_DirectEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Consumer with nil dependencies
	c := NewConsumer(nil, nil)
	c.recordPRBuildSnapshot(ctx, "repo", 0, prwire.BuildMetadata{})
	c.recordPRBuildSnapshot(ctx, "repo", 1, prwire.BuildMetadata{})

	if _, err := c.fetchMetadataRun(ctx, "example/aerial", -1); err == nil {
		t.Fatal("expected error for id <= 0")
	}
	if _, err := c.fetchMetadataRun(ctx, "example/aerial", 1); err == nil {
		t.Fatal("expected error for nil gh")
	}
	if _, err := c.listCommitCheckRuns(ctx, "example/aerial", "sha"); err == nil {
		t.Fatal("expected error for nil gh")
	}
	if _, _, err := c.newestMetadataRun(ctx, "example/aerial", "sha", true); err == nil {
		t.Fatal("expected error for nil gh")
	}

	nonPRRun := MetadataWorkflowRun{Event: model.BuildEventPush, HeadSHA: tHead, Path: MetadataWorkflowPath("example/aerial")}

	// ValidateMetadataExecution branch build matching / mismatching head
	if execErr := c.ValidateMetadataExecution(ctx, "example/aerial", nonPRRun, tHead); execErr != nil {
		t.Fatalf("expected nil for branch execution match, got %v", execErr)
	}
	if execErr := c.ValidateMetadataExecution(ctx, "example/aerial", nonPRRun, tOther); execErr == nil {
		t.Fatal("expected error for branch execution mismatch")
	}

	// SupersedeStaleBuildAttempt edge cases
	c.SupersedeStaleBuildAttempt(ctx, "example/aerial", 0, WorkflowRunEvent{ID: 1})
	c.SupersedeStaleBuildAttempt(ctx, "example/aerial", 42, WorkflowRunEvent{ID: 0})
}
