package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	images, err := srv.resolveWorkflowRunImages(ctx, tc.repo, tc.runID)
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
	suppressVanadis  bool
	widgetErr        error
	wantPromoteCalls int
	wantDeployJobs   []string
	wantWidgetCalls  int
	wantReconciles   []fluxKustomizationRef
	wantRecorded     []string
	wantErr          bool
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
	if tc.wantWidgetCalls > 0 && len(disp.WidgetCalls()) != tc.wantWidgetCalls {
		t.Errorf("widgetCalls = %d, want %d", len(disp.WidgetCalls()), tc.wantWidgetCalls)
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
			widgetErr:             tc.widgetErr,
			kustomizationsByImage: owners,
		}
	}
	reg := &mockPRRegistry{
		deployingErr:            tc.deployingErr,
		kustomizationRequestErr: tc.recordErr,
		hasExpectedErr:          tc.hasExpectedErr,
		suppressVanadisRes:      tc.suppressVanadis,
		resolveTargetID:         "1555405874565091380",
		resolveBranch:           "feat/test",
	}
	srv := NewRouterServer(fixedConfig(Config{GitHubAPIURL: ts.URL}), ts.Client())
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	srv.SetRegistry(reg)

	var err error
	if tc.isGitPush {
		err = srv.dispatchGitPush(ctx, "azylman/aerial", tc.ref, tc.commit, "1555405874565091380", 1)
	} else {
		err = srv.dispatchWorkflowRunImages(ctx, "azylman/aerial", 12345, tc.headBranch, tc.conclusion, "sha123", "1555405874565091380", 42)
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
			wantWidgetCalls:  1,
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
			name:             "main success with widget sync warning still succeeds",
			headBranch:       "main",
			conclusion:       "success",
			jobsBody:         brainJob,
			widgetErr:        errors.New("transient widget error"),
			wantPromoteCalls: 1,
			wantDeployJobs:   []string{"brain"},
			wantWidgetCalls:  1,
			wantReconciles:   []fluxKustomizationRef{core},
			wantRecorded:     []string{"aerial-core"},
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
			name:            "0 resolved images with hasExpectedErr and Vanadis widget sync warning",
			headBranch:      "main",
			conclusion:      "success",
			jobsBody:        `{"jobs":[]}`,
			hasExpectedErr:  errors.New("expected err"),
			suppressVanadis: true,
			widgetErr:       errors.New("widget sync err"),
			wantErr:         true,
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

func makeTestOpenPR(number int, branch, sha, author string) GitHubPullRequest {
	return GitHubPullRequest{Number: number, Head: GitHubPRRef{Ref: branch, SHA: sha}, User: struct {
		Login string `json:"login"`
	}{Login: author}}
}

type checkOpenPRConflictsTestCase struct {
	name                  string
	autoUpdateAuthors     []string
	listPRsStatus         int
	useNilRegistry        bool
	useRiver              bool
	riverErr              error
	conflictErr           error
	openPRs               []GitHubPullRequest
	prResponses           []string
	prStatusCodes         []int
	closedErr             error
	wantClosedPRs         []int
	wantErr               bool
	wantErrSubstr         string
	wantConflict          bool
	wantConflictPR        int
	wantConflictQueued    int
	wantPrompts           int
	wantDispatches        int
	wantPromptSubs        []string
	wantDMs               int
	wantUpdatedPRs        []string
	checkRunsBySHA        map[string]string
	presetFailedSHAs      map[int]string
	presetUpdatedPRs      []int
	wantCachedFailedSHAs  map[int]string
	wantUncachedFailedPRs []int
}

func newConflictCheckMockServer(tc checkOpenPRConflictsTestCase, putPaths *[]string, mu *sync.Mutex) *httptest.Server {
	var reqCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pulls") {
			w.Header().Set("Content-Type", "application/json")
			if tc.listPRsStatus != 0 {
				w.WriteHeader(tc.listPRsStatus)
				_, _ = w.Write([]byte(`{"message":"server error"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(tc.openPRs)
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/check-runs") {
			w.Header().Set("Content-Type", "application/json")
			trimmed := strings.TrimSuffix(r.URL.Path, "/check-runs")
			sha := trimmed[strings.LastIndex(trimmed, "/")+1:]
			body := `{"check_runs":[]}`
			if tc.checkRunsBySHA != nil {
				if resp, ok := tc.checkRunsBySHA[sha]; ok {
					body = resp
				}
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
			return
		}
		if r.Method == http.MethodPut {
			mu.Lock()
			*putPaths = append(*putPaths, r.URL.Path)
			mu.Unlock()
		}
		idx := int(reqCount.Add(1)) - 1
		code := http.StatusOK
		if idx < len(tc.prStatusCodes) && tc.prStatusCodes[idx] != 0 {
			code = tc.prStatusCodes[idx]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		body := `{"number":0}`
		if idx < len(tc.prResponses) {
			body = tc.prResponses[idx]
		} else if len(tc.prResponses) > 0 {
			body = tc.prResponses[len(tc.prResponses)-1]
		}
		_, _ = w.Write([]byte(body))
	}))
	return server
}

func assertConflictCheckRegistryCalls(t *testing.T, tc checkOpenPRConflictsTestCase, mockReg PRRegistryUpdater) {
	t.Helper()
	mReg, ok := mockReg.(*mockPRRegistry)
	if !ok || mReg == nil {
		return
	}
	if tc.wantConflict {
		if len(mReg.conflictCalls) != 1 || mReg.conflictCalls[0].prNumber != tc.wantConflictPR {
			t.Errorf("conflictCalls = %+v, want PR %d", mReg.conflictCalls, tc.wantConflictPR)
		}
	} else if len(mReg.conflictCalls) != 0 {
		t.Errorf("unexpected conflictCalls: %+v", mReg.conflictCalls)
	}
	if len(mReg.closedUnmergedCalls) != len(tc.wantClosedPRs) {
		t.Errorf("closedUnmergedCalls count = %d, want %d", len(mReg.closedUnmergedCalls), len(tc.wantClosedPRs))
		return
	}
	for i, wantPR := range tc.wantClosedPRs {
		if mReg.closedUnmergedCalls[i].prNumber != wantPR {
			t.Errorf("closedUnmergedCalls[%d] = %d, want %d", i, mReg.closedUnmergedCalls[i].prNumber, wantPR)
		}
	}
}

func assertConflictCheckDispatcherCalls(t *testing.T, tc checkOpenPRConflictsTestCase, mockDisp *mockOutboundDispatcher) {
	t.Helper()
	if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
		t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
	}
	if len(mockDisp.PromptCalls()) != tc.wantPrompts {
		t.Errorf("got %d prompts, want %d", len(mockDisp.PromptCalls()), tc.wantPrompts)
	}
	if len(mockDisp.DispatchCalls()) != tc.wantDispatches {
		t.Errorf("got %d dispatches, want %d", len(mockDisp.DispatchCalls()), tc.wantDispatches)
	}
	for _, sub := range tc.wantPromptSubs {
		if tc.wantDispatches > 0 {
			if len(mockDisp.DispatchCalls()) == 0 || !strings.Contains(mockDisp.DispatchCalls()[0].Prompt, sub) {
				t.Errorf("expected dispatch prompt containing %q", sub)
			}
		} else if len(mockDisp.PromptCalls()) == 0 || !strings.Contains(mockDisp.PromptCalls()[0].Prompt, sub) {
			t.Errorf("expected prompt containing %q", sub)
		}
	}
}

func assertConflictCheckCacheAndPuts(t *testing.T, tc checkOpenPRConflictsTestCase, srv *RouterServer, putPaths []string) {
	t.Helper()
	for prNum, wantSHA := range tc.wantCachedFailedSHAs {
		v, ok := srv.failedCheckSHAs.Load(prCheckKey("azylman/aerial", prNum))
		if !ok || fmt.Sprint(v) != wantSHA {
			t.Errorf("cached failedCheckSHAs[%d] = %v (ok=%v), want %q", prNum, v, ok, wantSHA)
		}
	}
	for _, prNum := range tc.wantUncachedFailedPRs {
		if v, ok := srv.failedCheckSHAs.Load(prCheckKey("azylman/aerial", prNum)); ok {
			t.Errorf("cached failedCheckSHAs[%d] = %v, want uncached", prNum, v)
		}
	}
	if len(putPaths) != len(tc.wantUpdatedPRs) {
		t.Fatalf("got updated PR paths %v, want %v", putPaths, tc.wantUpdatedPRs)
	}
	for i := range tc.wantUpdatedPRs {
		if putPaths[i] != tc.wantUpdatedPRs[i] {
			t.Errorf("updated PR path[%d] = %q, want %q", i, putPaths[i], tc.wantUpdatedPRs[i])
		}
	}
}

func runCheckOpenPRConflictsCase(ctx context.Context, t *testing.T, tc checkOpenPRConflictsTestCase) {
	t.Helper()
	t.Parallel()
	var mu sync.Mutex
	var putPaths []string
	apiServer := newConflictCheckMockServer(tc, &putPaths, &mu)
	defer apiServer.Close()

	var mockReg PRRegistryUpdater
	if !tc.useNilRegistry {
		mockReg = &mockPRRegistry{
			conflictErr:      tc.conflictErr,
			conflictTargetID: "1555405874565091380",
			conflictUpdated:  true,
			closedErr:        tc.closedErr,
		}
	}
	mockDisp := &mockOutboundDispatcher{}
	var inserters []RiverInserter
	var mockRiver *mockRiverInserter
	if tc.useRiver {
		mockRiver = &mockRiverInserter{err: tc.riverErr}
		inserters = append(inserters, mockRiver)
	}
	srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL, GitHubToken: "test-token", AutoUpdatePRAuthors: tc.autoUpdateAuthors, DefaultOwner: true}, nil, mockReg, mockDisp, inserters...)
	for prNum, sha := range tc.presetFailedSHAs {
		srv.recordFailedCISHA("azylman/aerial", prNum, sha)
	}
	for _, prNum := range tc.presetUpdatedPRs {
		srv.markUpdatedForCurrentMain("azylman/aerial", prNum)
	}

	err := srv.checkOpenPRConflicts(ctx, "azylman/aerial")
	if (err != nil) != tc.wantErr {
		t.Fatalf("checkOpenPRConflicts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
		t.Errorf("err = %v, want substr %q", err, tc.wantErrSubstr)
	}

	mu.Lock()
	gotUpdated := append([]string(nil), putPaths...)
	mu.Unlock()

	assertConflictCheckCacheAndPuts(t, tc, srv, gotUpdated)
	assertConflictCheckRegistryCalls(t, tc, mockReg)
	if mockRiver != nil && len(mockRiver.insertedConflictJobs) != tc.wantConflictQueued {
		t.Errorf("insertedConflictJobs = %d, want %d", len(mockRiver.insertedConflictJobs), tc.wantConflictQueued)
	}
	assertConflictCheckDispatcherCalls(t, tc, mockDisp)
}

func TestCheckOpenPRConflicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range checkOpenPRConflictsTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runCheckOpenPRConflictsCase(ctx, t, tc)
		})
	}
}

func checkOpenPRConflictsTestCases() []checkOpenPRConflictsTestCase {
	p1 := checkOpenPRConflictsCasesPart1()
	p2 := checkOpenPRConflictsCasesPart2()
	p3 := checkOpenPRConflictsCasesPart3()
	all := make([]checkOpenPRConflictsTestCase, 0, len(p1)+len(p2)+len(p3))
	all = append(all, p1...)
	all = append(all, p2...)
	all = append(all, p3...)
	return all
}

func checkOpenPRConflictsCasesPart1() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:              "empty autoUpdateAuthors is clean no-op",
			autoUpdateAuthors: nil,
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
			},
			wantErr: false,
		},
		{
			name:              "422 no new commits on base branch marks PR updated and stops FIFO loop",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/behind-2", "head551", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"There are no new commits on the base branch."}`,
			},
			prStatusCodes:  []int{http.StatusUnprocessableEntity},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "nil registry still updates whitelisted PRs",
			useNilRegistry:    true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/clean", "head550", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "nil registry on conflict does not crash",
			useNilRegistry:    true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"merge conflict between base and head"}`},
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "nil registry on stale head ref does not crash",
			useNilRegistry:    true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes: []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
		{
			name:              "skips non-whitelisted PR authors and updates whitelisted PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(540, "feat/untrusted", "head540", "untrusted-user"),
				makeTestOpenPR(541, "feat/trusted", "head541", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/541/update-branch"},
		},
		{
			name:              "list open PRs error returns error",
			autoUpdateAuthors: []string{"trusted-author"},
			listPRsStatus:     http.StatusInternalServerError,
			wantErr:           true,
			wantErrSubstr:     "list open prs",
		},
		{
			name:              "422 merge conflict transitions and dispatches via background /dispatch without DM",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"merge conflict between base and head"}`},
			wantConflict:      true,
			wantConflictPR:    543,
			wantDMs:           0,
			wantDispatches:    1,
			wantPromptSubs:    []string{"Merge conflict detected on PR #543"},
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
	}
}

func checkOpenPRConflictsCasesPart2() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:               "409 conflict with river client enqueues MergeConflictArgs on merge_conflict queue",
			useRiver:           true,
			autoUpdateAuthors:  []string{"trusted-author"},
			openPRs:            []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:      []int{http.StatusConflict},
			prResponses:        []string{`{"message":"Merge conflict"}`},
			wantConflict:       true,
			wantConflictPR:     543,
			wantConflictQueued: 1,
			wantDMs:            0,
			wantDispatches:     0,
			wantUpdatedPRs:     []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "river insert error on conflict logs warning without failing check",
			useRiver:          true,
			riverErr:          errors.New("river insert failed"),
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:     []int{http.StatusConflict},
			prResponses:       []string{`{"message":"Merge conflict"}`},
			wantConflict:      true,
			wantConflictPR:    543,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "open PRs update only oldest clean PR in FIFO order",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/behind-2", "head551", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "conflicted oldest PR transitions to conflict and advances to update next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(545, "feat/conflicted-1", "head545", "trusted-author"),
				makeTestOpenPR(546, "feat/clean-2", "head546", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"merge conflict between base and head"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes:  []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantConflict:   true,
			wantConflictPR: 545,
			wantDMs:        0,
			wantDispatches: 1,
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/545/update-branch",
				"/repos/azylman/aerial/pulls/546/update-branch",
			},
		},
		{
			name:              "non-conflict 422 logs warning and skips without marking conflict",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(552, "feat/behind-err", "head552", "trusted-author")},
			prResponses:       []string{`{"message":"Validation Failed"}`},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/552/update-branch"},
		},
		{
			name:              "403 rate-limit error returns error",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(546, "feat/rate-limit", "head546", "trusted-author")},
			prStatusCodes:     []int{http.StatusForbidden},
			prResponses:       []string{`{"message":"API rate limit exceeded"}`},
			wantErr:           true,
			wantErrSubstr:     "rate limit",
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/546/update-branch"},
		},
		{
			name:              "non-403 error skips PR and returns nil",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(547, "feat/not-found", "head547", "trusted-author")},
			prStatusCodes:     []int{http.StatusNotFound},
			prResponses:       []string{`{"message":"Not Found"}`},
			wantErr:           false,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/547/update-branch"},
		},
		{
			name:              "conflict transition error logged cleanly without returning error",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(549, "feat/trans-err", "head549", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"merge conflict between base and head"}`},
			conflictErr:       errors.New("conflict transition err"),
			wantErr:           false,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/549/update-branch"},
		},
	}
}

func checkOpenPRConflictsCasesPart3() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:              "422 head ref does not exist closes stale PR and advances to next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes: []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantClosedPRs: []int{512},
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
		{
			name:              "422 head ref does not exist logs close error cleanly without failing check",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"head ref does not exist"}`},
			closedErr:         errors.New("close db err"),
			wantClosedPRs:     nil,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/512/update-branch"},
		},
		{
			name:              "skips draft PRs and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				{Number: 550, Head: GitHubPRRef{Ref: "feat/draft", SHA: "head550"}, Draft: true, User: struct {
					Login string `json:"login"`
				}{Login: "trusted-author"}},
				makeTestOpenPR(551, "feat/ready", "head551", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/551/update-branch"},
		},
		{
			name:              "skips PR with in-memory failedCheckSHAs and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			presetFailedSHAs:  map[int]string{550: "head550"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/failing", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/551/update-branch"},
		},
		{
			name:              "skips PR with failed check-run in API, caches failure, and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/failing-check", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:          []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:        []int{http.StatusAccepted},
			wantUpdatedPRs:       []string{"/repos/azylman/aerial/pulls/551/update-branch"},
			wantCachedFailedSHAs: map[int]string{550: "head550"},
		},
		{
			name:              "breaks without updating when leading clean PR already updated for current main",
			autoUpdateAuthors: []string{"trusted-author"},
			presetUpdatedPRs:  []int{550},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/in-flight", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/waiting", "head551", "trusted-author"),
			},
			wantUpdatedPRs: nil,
		},
		{
			name:              "skips PR with pending check-run without caching failure and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"roborev","status":"in_progress","conclusion":""}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/pending-check", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:           []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:         []int{http.StatusAccepted},
			wantUpdatedPRs:        []string{"/repos/azylman/aerial/pulls/551/update-branch"},
			wantUncachedFailedPRs: []int{550},
		},
		{
			name:              "breaks without updating when leading clean PR already updated for current main has pending checks",
			autoUpdateAuthors: []string{"trusted-author"},
			presetUpdatedPRs:  []int{550},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"roborev","status":"in_progress","conclusion":""}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/in-flight", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/waiting", "head551", "trusted-author"),
			},
			wantUpdatedPRs:        nil,
			wantUncachedFailedPRs: []int{550},
		},
		{
			name:              "advances to update next clean PR when leading PR updated for current main has failed checks",
			autoUpdateAuthors: []string{"trusted-author"},
			presetUpdatedPRs:  []int{550},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"failure"}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/failed-in-flight", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:          []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:        []int{http.StatusAccepted},
			wantUpdatedPRs:       []string{"/repos/azylman/aerial/pulls/551/update-branch"},
			wantCachedFailedSHAs: map[int]string{550: "head550"},
		},
	}
}

type resolvePRFromCommitSHATestCase struct {
	name          string
	repo          string
	sha           string
	apiURL        string
	statusCode    int
	responseBody  string
	openPRsBody   string
	preCacheRepo  string
	preCacheSHA   string
	preCachePR    int
	preCacheBr    string
	wantErr       bool
	wantErrSubstr string
	wantPRNum     int
	wantBranch    string
}

func newResolvePRMockServer(tc resolvePRFromCommitSHATestCase) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/pulls") && strings.Contains(r.URL.RawQuery, "state=open") {
			if tc.openPRsBody != "" {
				_, _ = w.Write([]byte(tc.openPRsBody))
				return
			}
			_, _ = w.Write([]byte(`[]`))
			return
		}
		code := tc.statusCode
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(tc.responseBody))
	}))
}

func runResolvePRFromCommitSHACase(ctx context.Context, t *testing.T, tc resolvePRFromCommitSHATestCase) {
	t.Helper()
	t.Parallel()
	ts := newResolvePRMockServer(tc)
	defer ts.Close()

	apiURL := ts.URL
	if tc.apiURL != "" {
		apiURL = tc.apiURL
	}
	srv := NewRouterServer(fixedConfig(Config{GitHubAPIURL: apiURL, GitHubToken: "secret"}), ts.Client())
	if tc.preCacheRepo != "" && tc.preCacheSHA != "" {
		srv.recordCommitSHAPR(tc.preCacheRepo, tc.preCacheSHA, tc.preCachePR, tc.preCacheBr)
	}
	prNum, branch, err := srv.resolvePRFromCommitSHA(ctx, tc.repo, tc.sha)
	if (err != nil) != tc.wantErr {
		t.Fatalf("resolvePRFromCommitSHA() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
		t.Errorf("err = %v, want substr %q", err, tc.wantErrSubstr)
	}
	if prNum != tc.wantPRNum {
		t.Errorf("got prNum = %d, want %d", prNum, tc.wantPRNum)
	}
	if branch != tc.wantBranch {
		t.Errorf("got branch = %q, want %q", branch, tc.wantBranch)
	}
}

func TestResolvePRFromCommitSHA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range resolvePRFromCommitSHATestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runResolvePRFromCommitSHACase(ctx, t, tc)
		})
	}
}

func resolvePRFromCommitSHATestCases() []resolvePRFromCommitSHATestCase {
	return []resolvePRFromCommitSHATestCase{
		{
			name:          "empty repo",
			repo:          "",
			sha:           "sha123",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:    "invalid api url",
			repo:    "azylman/aerial",
			sha:     "sha123",
			apiURL:  "::bad-url::",
			wantErr: true,
		},
		{
			name:          "empty sha",
			repo:          "azylman/aerial",
			sha:           "",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "zero SHA",
			repo:          "azylman/aerial",
			sha:           "0000000000000000000000000000000000000000",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "403 rate limit",
			repo:          "azylman/aerial",
			sha:           "sha403",
			statusCode:    http.StatusForbidden,
			responseBody:  `{"message":"API rate limit exceeded"}`,
			wantErr:       true,
			wantErrSubstr: "rate limit",
		},
		{
			name:          "404 not found",
			repo:          "azylman/aerial",
			sha:           "sha404",
			statusCode:    http.StatusNotFound,
			responseBody:  `{"message":"Not Found"}`,
			wantErr:       true,
			wantErrSubstr: "404",
		},
		{
			name:         "malformed JSON",
			repo:         "azylman/aerial",
			sha:          "shamalformed",
			statusCode:   http.StatusOK,
			responseBody: `{not-an-array`,
			wantErr:      true,
		},
		{
			name:          "empty PR list",
			repo:          "azylman/aerial",
			sha:           "shaempty",
			statusCode:    http.StatusOK,
			responseBody:  `[]`,
			wantErr:       true,
			wantErrSubstr: "no pull requests associated",
		},
		{
			name:         "closed-PR over open-PR priority",
			repo:         "azylman/aerial",
			sha:          "shaclosed",
			statusCode:   http.StatusOK,
			responseBody: `[{"number":10,"state":"open","head":{"ref":"feat/open"}},{"number":20,"state":"closed","head":{"ref":"feat/closed"}}]`,
			wantPRNum:    20,
			wantBranch:   "feat/closed",
		},
		{
			name:         "higher-number selection priority when states match",
			repo:         "azylman/aerial",
			sha:          "shahigher",
			statusCode:   http.StatusOK,
			responseBody: `[{"number":10,"state":"closed","head":{"ref":"feat/ten"}},{"number":30,"state":"closed","head":{"ref":"feat/thirty"}},{"number":20,"state":"closed","head":{"ref":"feat/twenty"}}]`,
			wantPRNum:    30,
			wantBranch:   "feat/thirty",
		},
		{
			name:         "fork PR fallback scans open pulls when commits pulls returns empty",
			repo:         "azylman/aerial",
			sha:          "forksha123",
			statusCode:   http.StatusOK,
			responseBody: `[]`,
			openPRsBody:  `[{"number":456,"state":"open","head":{"ref":"fork-feature","sha":"forksha123"}},{"number":455,"state":"open","head":{"ref":"other","sha":"diffsha"}}]`,
			wantPRNum:    456,
			wantBranch:   "fork-feature",
		},
		{
			name:         "in-memory commitSHAPRs cache hit avoids hitting GitHub API",
			repo:         "azylman/aerial",
			sha:          "cachedsha999",
			apiURL:       "::bad-url::",
			preCacheRepo: "azylman/aerial",
			preCacheSHA:  "cachedsha999",
			preCachePR:   999,
			preCacheBr:   "feat/cached-branch",
			wantPRNum:    999,
			wantBranch:   "feat/cached-branch",
		},
		{
			name:          "invalid commit sha characters return error",
			repo:          "azylman/aerial",
			sha:           "0000",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "lookupCommitSHAPR empty or zero sha returns false",
			repo:          "",
			sha:           "",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "open PR fallback fails if both commits/pulls and pulls return empty",
			repo:          "azylman/aerial",
			sha:           "notfoundsha",
			statusCode:    http.StatusOK,
			responseBody:  `[]`,
			openPRsBody:   `[{"number":123,"state":"open","head":{"ref":"foo","sha":"other"}}]`,
			wantErr:       true,
			wantErrSubstr: "no pull requests associated",
		},
	}
}
