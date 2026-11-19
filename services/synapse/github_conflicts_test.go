package main

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// --- 6. Workflow Run Image Resolution & Dispatch ---

type resolveWorkflowRunCase struct {
	name    string
	repo    string
	runID   int64
	want    []string
	wantErr bool
}

func newWorkflowRunJobsMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 8,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 8, Name: "Build & Push Images to GHCR (${{ matrix.service }})", Status: "completed", Conclusion: "success", Steps: []GitHubWorkflowJobStep{{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 1}}},
			{ID: 1, Name: "Detect Changed Microservices", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Run Service Unit Tests", Status: "completed", Conclusion: "success"},
			{ID: 3, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 4, Name: "Build & Push Images to GHCR (synapse)", Status: "completed", Conclusion: "success"},
			{ID: 5, Name: "Build & Push Images to GHCR (custom-microservice)", Status: "completed", Conclusion: "success"},
			{ID: 6, Name: "Build & Push Images to GHCR (failed-service)", Status: "completed", Conclusion: "failure"},
			{ID: 7, Name: "Build & Push Images to GHCR (running-service)", Status: "in_progress", Conclusion: ""},
		},
	}
	mockSidecarJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 10, Name: "Build & Push Sidecar Images to GHCR (banana)", Status: "completed", Conclusion: "success"},
			{ID: 11, Name: "Build & Push Sidecar Images to GHCR (voice-cuda)", Status: "completed", Conclusion: "success"},
		},
	}
	mockMirrormereJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 20, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success"},
			{ID: 21, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success"},
		},
	}
	mockSkippedStepJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{
				ID: 101, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "skipped", Number: 2},
				},
			},
			{
				ID: 102, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 2},
				},
			},
		},
	}
	mockSkippedPromotionJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 201, Name: "Build & Push Images to GHCR (eri)", Status: "completed", Conclusion: "success"},
			{ID: 202, Name: "Promote Images to :latest", Status: "completed", Conclusion: "skipped"},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/actions/runs/12345/jobs"):
			_ = json.NewEncoder(w).Encode(mockJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/54321/jobs"):
			_ = json.NewEncoder(w).Encode(mockSidecarJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/67890/jobs"):
			_ = json.NewEncoder(w).Encode(mockMirrormereJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/77777/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedStepJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/88888/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedPromotionJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/403/jobs"):
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
		case strings.Contains(r.URL.Path, "/actions/runs/500/jobs"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/actions/runs/999/jobs"):
			_, _ = w.Write([]byte("malformed json"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func resolveWorkflowRunTestCases() []resolveWorkflowRunCase {
	return []resolveWorkflowRunCase{
		{
			name:  "aerial monorepo images",
			repo:  "azylman/aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/brain:latest",
				"ghcr.io/azylman/synapse:latest",
				"ghcr.io/azylman/custom-microservice:latest",
			},
		},
		{
			name:  "unprefixed repo normalization",
			repo:  "aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/brain:latest",
				"ghcr.io/azylman/synapse:latest",
				"ghcr.io/azylman/custom-microservice:latest",
			},
		},
		{
			name:  "aerial-sidecars images",
			repo:  "azylman/aerial-sidecars",
			runID: 54321,
			want: []string{
				"ghcr.io/azylman/banana:latest",
				"ghcr.io/azylman/voice-cuda:latest",
			},
		},
		{
			name:  "mirrormere images",
			repo:  "azylman/mirrormere",
			runID: 67890,
			want: []string{
				"ghcr.io/azylman/mirrormere:latest",
				"ghcr.io/azylman/mirrormere-cast-watcher:latest",
			},
		},
		{
			name:  "skipped build step filtering",
			repo:  "azylman/mirrormere",
			runID: 77777,
			want:  []string{"ghcr.io/azylman/mirrormere-cast-watcher:latest"},
		},
		{
			name:  "skipped image promotion returns nil",
			repo:  "azylman/aerial",
			runID: 88888,
			want:  nil,
		},
		{name: "empty repo", repo: "", runID: 12345, wantErr: true},
		{name: "zero runID", repo: "aerial", runID: 0, wantErr: true},
		{name: "403 rate limit", repo: "aerial", runID: 403, wantErr: true},
		{name: "500 server error", repo: "aerial", runID: 500, wantErr: true},
		{name: "malformed json", repo: "aerial", runID: 999, wantErr: true},
	}
}

func runResolveWorkflowRunImagesCase(ctx context.Context, t *testing.T, srv *RouterServer, tc resolveWorkflowRunCase) {
	t.Helper()
	t.Parallel()
	images, err := srv.resolveLegacyWorkflowRunImages(ctx, tc.repo, tc.runID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("resolveWorkflowRunImages() err = %v, wantErr %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	if len(images) != len(tc.want) {
		t.Fatalf("expected %v, got %v", tc.want, images)
	}
	for i := range tc.want {
		if images[i] != tc.want[i] {
			t.Errorf("image[%d] = %q, want %q", i, images[i], tc.want[i])
		}
	}
}

func TestResolveWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := newWorkflowRunJobsMockServer(t)
	srv := NewRouterServer(fixedConfig(Config{GitHubAPIURL: ts.URL}), ts.Client())

	for _, tc := range resolveWorkflowRunTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runResolveWorkflowRunImagesCase(ctx, t, srv, tc)
		})
	}
}

func TestDispatchWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	core := fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"}
	marida := fluxKustomizationRef{Namespace: "flux-system", Name: "marida-core"}
	owners := map[string][]fluxKustomizationRef{
		"aerial-brain": {core},
		"aerial-haro":  {core, marida},
	}

	for _, tc := range dispatchWorkflowRunTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runDispatchWorkflowRunCase(ctx, t, tc, owners)
		})
	}
}

type dispatchWorkflowRunTestCase struct {
	name             string
	isGitPush        bool
	ref              string
	commit           string
	headBranch       string
	conclusion       string
	jobsStatus       int
	jobsBody         string
	noDisp           bool
	gitPushErr       error
	promoteErr       error
	discoverErr      error
	reconcileErrs    map[string]error
	deployingErr     error
	recordErr        error
	hasExpectedErr   error
	wantPromoteCalls int
	wantDeployJobs   []string

	wantReconciles []fluxKustomizationRef
	wantRecorded   []string
	wantErr        bool
}

func assertDispatchWorkflowRunDeployAndWidget(t *testing.T, tc dispatchWorkflowRunTestCase, disp *mockOutboundDispatcher, reg *mockPRRegistry) {
	t.Helper()
	if got := len(disp.PromoteCalls()); got != tc.wantPromoteCalls {
		t.Errorf("promote calls = %d, want %d", got, tc.wantPromoteCalls)
	}
	var gotDeployJobs []string
	if len(reg.deployingCalls) > 0 {
		gotDeployJobs = reg.deployingCalls[0].jobs
	}
	if len(reg.deployingCalls) > 1 || !slices.Equal(gotDeployJobs, tc.wantDeployJobs) {
		t.Errorf("deploying calls = %+v, want one call with jobs %v", reg.deployingCalls, tc.wantDeployJobs)
	}
}

func assertDispatchWorkflowRunReconciles(t *testing.T, tc dispatchWorkflowRunTestCase, disp *mockOutboundDispatcher, reg *mockPRRegistry) {
	t.Helper()
	if got := disp.ReconcileCalls(); !slices.Equal(got, tc.wantReconciles) {
		t.Errorf("reconcile calls = %v, want %v", got, tc.wantReconciles)
	}
	if tc.wantRecorded == nil {
		if len(reg.kustomizationRequestCalls) != 0 {
			t.Errorf("recorded requests = %v, want none", reg.kustomizationRequestCalls)
		}
		return
	}
	if len(reg.kustomizationRequestCalls) != 1 {
		t.Fatalf("recorded request calls = %d, want 1", len(reg.kustomizationRequestCalls))
	}
	recorded := reg.kustomizationRequestCalls[0]
	if got := slices.Sorted(maps.Keys(recorded)); !slices.Equal(got, tc.wantRecorded) {
		t.Errorf("recorded kustomizations = %v, want %v", got, tc.wantRecorded)
	}
	for i, ref := range disp.ReconcileCalls() {
		if req := recorded[ref.Name]; req.Namespace != "flux-system" || disp.reconcileRequestedAts[i] != req.RequestedAt {
			t.Errorf("reconcile %s requestedAt = %q, want recorded %+v", ref.Name, disp.reconcileRequestedAts[i], req)
		}
	}
}

func runDispatchWorkflowRunCase(ctx context.Context, t *testing.T, tc dispatchWorkflowRunTestCase, owners map[string][]fluxKustomizationRef) {
	t.Helper()
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := tc.jobsStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		body := tc.jobsBody
		if body == "" {
			body = `{"jobs":[]}`
		}
		_, _ = w.Write([]byte(body))
	}))
	defer ts.Close()

	var disp *mockOutboundDispatcher
	if !tc.noDisp {
		disp = &mockOutboundDispatcher{
			gitPushErr:            tc.gitPushErr,
			promoteErr:            tc.promoteErr,
			discoverErr:           tc.discoverErr,
			reconcileErrs:         tc.reconcileErrs,
			kustomizationsByImage: owners,
		}
	}
	reg := &mockPRRegistry{
		deployingErr:            tc.deployingErr,
		kustomizationRequestErr: tc.recordErr,
		hasExpectedErr:          tc.hasExpectedErr,

		resolveTargetID: "100000000000000001",
		resolveBranch:   "feat/test",
	}
	srv := NewRouterServer(fixedConfig(legacyFixtureConfig(Config{GitHubAPIURL: ts.URL})), legacyFixtureClient(ts.Client()))
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	srv.SetRegistry(reg)

	var err error
	if tc.isGitPush {
		err = srv.dispatchGitPush(ctx, "azylman/aerial", tc.ref, tc.commit, "100000000000000001", 1)
	} else {
		err = srv.dispatchWorkflowRunImages(ctx, "azylman/aerial", workflowRunEvent{ID: 12345, Attempt: 1, HeadSHA: legacyFixtureHead}, tc.headBranch, tc.conclusion, "100000000000000001", 42)
	}

	if (err != nil) != tc.wantErr {
		t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
	}
	if disp == nil {
		return
	}
	assertDispatchWorkflowRunDeployAndWidget(t, tc, disp, reg)
	assertDispatchWorkflowRunReconciles(t, tc, disp, reg)
}

func dispatchWorkflowRunTestCases() []dispatchWorkflowRunTestCase {
	p1 := dispatchWorkflowRunCasesPart1()
	p2 := dispatchWorkflowRunCasesPart2()
	all := make([]dispatchWorkflowRunTestCase, 0, len(p1)+len(p2))
	all = append(all, p1...)
	all = append(all, p2...)
	return all
}

func dispatchWorkflowRunCasesPart1() []dispatchWorkflowRunTestCase {
	brainJob := `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`
	twoJobs := `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"},{"name":"Build & Push Images to GHCR (haro)","status":"completed","conclusion":"success"}]}`
	core := fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"}
	marida := fluxKustomizationRef{Namespace: "flux-system", Name: "marida-core"}

	return []dispatchWorkflowRunTestCase{
		{
			name:             "main success transitions deploying then reconciles owning kustomization",
			headBranch:       "main",
			conclusion:       "success",
			jobsStatus:       http.StatusOK,
			jobsBody:         brainJob,
			wantPromoteCalls: 1,
			wantDeployJobs:   []string{"brain"},
			wantReconciles:   []fluxKustomizationRef{core},
			wantRecorded:     []string{"aerial-core"},
		},
		{
			name:             "two images reconcile each distinct kustomization once",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         twoJobs,
			wantPromoteCalls: 2,
			wantDeployJobs:   []string{"brain", "haro"},
			wantReconciles:   []fluxKustomizationRef{core, marida},
			wantRecorded:     []string{"aerial-core", "marida-core"},
		},
		{
			name:             "partial reconcile failure keeps every recorded request for watcher repair and errors",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         twoJobs,
			reconcileErrs:    map[string]error{"aerial-core": errors.New("patch denied")},
			wantPromoteCalls: 2,
			wantDeployJobs:   []string{"brain", "haro"},
			wantReconciles:   []fluxKustomizationRef{core, marida},
			wantRecorded:     []string{"aerial-core", "marida-core"},
			wantErr:          true,
		},
		{
			name:             "reconcile failure after record keeps the request and errors",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         brainJob,
			reconcileErrs:    map[string]error{"aerial-core": errors.New("patch denied")},
			wantPromoteCalls: 1,
			wantDeployJobs:   []string{"brain"},
			wantReconciles:   []fluxKustomizationRef{core},
			wantRecorded:     []string{"aerial-core"},
			wantErr:          true,
		},
		{
			name:             "record error returns before any reconcile patch",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         brainJob,
			recordErr:        errors.New("db fail"),
			wantPromoteCalls: 1,
			wantDeployJobs:   []string{"brain"},
			wantErr:          true,
		},
		{
			name:       "jobs API 500 error",
			headBranch: "main",
			conclusion: "success",
			jobsStatus: http.StatusInternalServerError,
			jobsBody:   `api error`,
			wantErr:    true,
		},
		{
			name:       "0 resolved images returns nil",
			headBranch: "main",
			conclusion: "success",
			jobsBody:   `{"jobs":[]}`,
		},
		{
			name:           "0 resolved images with hasExpectedErr propagates error",
			headBranch:     "main",
			conclusion:     "success",
			jobsBody:       `{"jobs":[]}`,
			hasExpectedErr: errors.New("expected err"),
			wantErr:        true,
		},
		{
			name:             "image without owning kustomization promotes then completes no-deploy",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         `{"jobs":[{"name":"Build & Push Images to GHCR (unmanaged)","status":"completed","conclusion":"success"}]}`,
			wantPromoteCalls: 1,
		},
	}
}

func dispatchWorkflowRunCasesPart2() []dispatchWorkflowRunTestCase {
	brainJob := `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`

	return []dispatchWorkflowRunTestCase{
		{
			name:        "discovery error returns before any side effect",
			headBranch:  "main",
			conclusion:  "success",
			jobsBody:    brainJob,
			discoverErr: errors.New("list forbidden"),
			wantErr:     true,
		},
		{
			name:             "promote failure skips reconcile and returns error",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         brainJob,
			promoteErr:       errors.New("hangar image failed"),
			wantPromoteCalls: 1,
			wantDeployJobs:   []string{"brain"},
			wantErr:          true,
		},
		{
			name:         "deployingErr registry error returns before promotion",
			headBranch:   "main",
			conclusion:   "success",
			jobsBody:     brainJob,
			deployingErr: errors.New("db fail"),
			wantErr:      true,
		},
		{
			name:       "non-main branch is no-op",
			headBranch: "feat/my-feature",
			conclusion: "success",
		},
		{
			name:       "non-success conclusion is no-op",
			headBranch: "main",
			conclusion: "failure",
		},
		{
			name:      "git push nil dispatcher is no-op",
			isGitPush: true,
			noDisp:    true,
			ref:       "refs/heads/main",
			commit:    "sha123",
		},
		{
			name:      "git push non-main ref is no-op",
			isGitPush: true,
			ref:       "refs/heads/feat",
			commit:    "sha123",
		},
		{
			name:      "git push zero sha is no-op",
			isGitPush: true,
			ref:       "refs/heads/main",
			commit:    "0000000000000000000000000000000000000000",
		},
		{
			name:       "git push dispatcher error returns error",
			isGitPush:  true,
			ref:        "refs/heads/main",
			commit:     "sha123",
			gitPushErr: errors.New("hangar down"),
			wantErr:    true,
		},
		{
			name:      "git push successful dispatch",
			isGitPush: true,
			ref:       "refs/heads/main",
			commit:    "sha123",
		},
	}
}

func TestMergeConflictAndDeployHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("resolveDeployEventTargetID handles empty target and resolution", func(t *testing.T) {
		t.Parallel()
		reg := &mockPRRegistry{resolveTargetID: "100000000000000001"}
		srv := &RouterServer{registry: reg}

		evt := &DeployEvent{Repo: "azylman/aerial", PRNumber: 10}
		srv.resolveDeployEventTargetID(ctx, evt)
		if evt.TargetID != "100000000000000001" {
			t.Errorf("evt.TargetID = %q, want %q", evt.TargetID, "100000000000000001")
		}

		// Resolution error leaves targetID unchanged
		regErr := &mockPRRegistry{resolveErr: errors.New("db error")}
		srvErr := &RouterServer{registry: regErr}
		evt2 := &DeployEvent{Repo: "azylman/aerial", PRNumber: 11}
		srvErr.resolveDeployEventTargetID(ctx, evt2)
		if evt2.TargetID != "" {
			t.Errorf("evt2.TargetID = %q, want empty", evt2.TargetID)
		}
	})
}

func TestHealDependabotPRAndBranchHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("populateMissingBranch branches", func(t *testing.T) {
		t.Parallel()
		var nilSrv *RouterServer
		nilSrv.populateMissingBranch(ctx, nil)

		srvNoReg := &RouterServer{}
		srvNoReg.populateMissingBranch(ctx, &ResolvedGitHubEvent{PRNumber: 10})

		srv := &RouterServer{
			registry: &mockPRRegistry{resolveBranch: "feat/discovered"},
		}
		// PRNumber <= 0
		srv.populateMissingBranch(ctx, &ResolvedGitHubEvent{PRNumber: 0})
		// Branch already set
		resBranch := &ResolvedGitHubEvent{PRNumber: 10, Branch: "feat/existing"}
		srv.populateMissingBranch(ctx, resBranch)
		if resBranch.Branch != "feat/existing" {
			t.Errorf("expected branch unchanged, got %q", resBranch.Branch)
		}
		// ResolvePRByNumber success
		resEmpty := &ResolvedGitHubEvent{PRNumber: 10, Branch: ""}
		srv.populateMissingBranch(ctx, resEmpty)
		if resEmpty.Branch != "feat/discovered" {
			t.Errorf("expected branch to be populated, got %q", resEmpty.Branch)
		}
		// ResolvePRByNumber error
		srvErr := &RouterServer{
			registry: &mockPRRegistry{resolveErr: errors.New("not found")},
		}
		resNotFound := &ResolvedGitHubEvent{PRNumber: 11, Branch: ""}
		srvErr.populateMissingBranch(ctx, resNotFound)
		if resNotFound.Branch != "" {
			t.Errorf("expected branch to remain empty, got %q", resNotFound.Branch)
		}
	})
}

func TestTransitionRegisteredConflict_DedupAndRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		initialConflictInDB bool
		firstCallUpdated    bool
		firstSHA            string
		secondSHA           string
		callClearFailedCI   bool
		wantFirstHandled    bool
		wantSecondHandled   bool
		wantPublishedCount  int
	}{
		{
			name:                "first transition publishes event and second call with same sha dedupes",
			initialConflictInDB: false,
			firstCallUpdated:    true,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-1",
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  1,
		},
		{
			name:                "already in conflict status publishes event once and dedupes on retry",
			initialConflictInDB: true,
			firstCallUpdated:    false,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-1",
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  1,
		},
		{
			name:                "different head sha re-publishes conflict event",
			initialConflictInDB: true,
			firstCallUpdated:    false,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-2",
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  2,
		},
		{
			name:                "clearing failed CI SHA resets dedup map allowing re-publish",
			initialConflictInDB: true,
			firstCallUpdated:    false,
			firstSHA:            "sha-conflict-1",
			secondSHA:           "sha-conflict-1",
			callClearFailedCI:   true,
			wantFirstHandled:    true,
			wantSecondHandled:   true,
			wantPublishedCount:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{
				conflictTargetID: "100000000000000001",
				conflictUpdated:  tc.firstCallUpdated,
			}
			if tc.initialConflictInDB {
				reg.resolveTargetID = "100000000000000001"
			}
			srv := newTestRouter(Config{}, nil, reg, nil, bus)

			handled1, err1 := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", tc.firstSHA)
			if err1 != nil {
				t.Fatalf("first transitionRegisteredConflict error = %v", err1)
			}
			if handled1 != tc.wantFirstHandled {
				t.Errorf("first handled = %v, want %v", handled1, tc.wantFirstHandled)
			}

			if tc.callClearFailedCI {
				srv.clearFailedCISHA("azylman/aerial", 42)
			}

			reg.conflictUpdated = false
			reg.resolveTargetID = "100000000000000001"

			handled2, err2 := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", tc.secondSHA)
			if err2 != nil {
				t.Fatalf("second transitionRegisteredConflict error = %v", err2)
			}
			if handled2 != tc.wantSecondHandled {
				t.Errorf("second handled = %v, want %v", handled2, tc.wantSecondHandled)
			}

			if len(bus.publishedPREvents) != tc.wantPublishedCount {
				t.Errorf("published events count = %d, want %d", len(bus.publishedPREvents), tc.wantPublishedCount)
			}
		})
	}
}

func TestCIFailureTransition_RollbackAndRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "publish error rolls back check state and retry succeeds when already ci_failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{err: errors.New("nats publish failed")}
			reg := &mockPRRegistry{
				ciFailedTargetID: "100000000000000001",
				ciFailedUpdated:  false,
				resolveHeadSHA:   "sha-ci-fail",
				buildLifecycle:   PRBuildLifecycle{Status: prCIFailed},
			}
			srv := newTestRouter(Config{}, nil, reg, nil, bus)

			res := &ResolvedGitHubEvent{
				Event:      ghEventCheckRun,
				Repo:       "azylman/aerial",
				PRNumber:   42,
				HeadSHA:    "sha-ci-fail",
				CheckName:  "unit-tests",
				Conclusion: conclusionFailure,
			}

			err1 := srv.handleCIFailureTransition(context.Background(), res, "test failure", "https://ci/1")
			if err1 == nil {
				t.Fatalf("first handleCIFailureTransition err = nil, want error")
			}

			entry := srv.loadMatchingFailedCheckState(res.Repo, res.PRNumber, res.HeadSHA)
			if entry == nil {
				t.Fatalf("expected non-nil failedCheckState entry")
			}
			if entry.activeFailed["unit-tests"] {
				t.Fatalf("expected active failure for unit-tests to be rolled back, got %v", entry.activeFailed)
			}

			bus.err = nil
			reg.ciFailedUpdated = false

			resRetry := &ResolvedGitHubEvent{
				Event:      ghEventCheckRun,
				Repo:       "azylman/aerial",
				PRNumber:   42,
				HeadSHA:    "sha-ci-fail",
				CheckName:  "unit-tests",
				Conclusion: conclusionFailure,
			}
			err2 := srv.handleCIFailureTransition(context.Background(), resRetry, "test failure", "https://ci/1")
			if err2 != nil {
				t.Fatalf("retry handleCIFailureTransition err = %v, want nil", err2)
			}
			if len(bus.publishedPREvents) != 1 {
				t.Errorf("published events = %d, want 1", len(bus.publishedPREvents))
			}
		})
	}
}
