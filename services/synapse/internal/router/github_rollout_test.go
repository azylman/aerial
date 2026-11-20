package router

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

type checkRecoveryTestCase struct {
	name                   string
	nilServer              bool
	repo                   string
	prNumber               int
	headSHA                string
	presetRegistryCIFailed bool
	presetRegistryHeadSHA  string
	commentsBody           string
	evalCheck              string
	wantRecover            bool
}

func runCheckRecoveryCase(t *testing.T, tc checkRecoveryTestCase) {
	t.Helper()
	t.Parallel()
	var srv *RouterServer
	if !tc.nilServer {
		var client *http.Client
		var cfg Config
		if tc.commentsBody != "" {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.commentsBody))
			}))
			defer ts.Close()
			cfg.GitHubAPIURL = ts.URL
			client = ts.Client()
		}
		var reg PRRegistryUpdater
		if tc.presetRegistryCIFailed || tc.presetRegistryHeadSHA != "" {
			status := prOpen
			if tc.presetRegistryCIFailed {
				status = prCIFailed
			}
			reg = &mockPRRegistry{
				buildLifecycle: PRBuildLifecycle{
					Status:  status,
					HeadSHA: tc.presetRegistryHeadSHA,
				},
			}
		}
		srv = newTestRouter(cfg, client, reg, nil)
	}
	got := srv.canRecoverCheckSuccess(context.Background(), tc.repo, tc.prNumber, tc.evalCheck, tc.headSHA)
	if got != tc.wantRecover {
		t.Fatalf("canRecover(%q) = %v, want %v", tc.evalCheck, got, tc.wantRecover)
	}
}

func checkRecoveryCasesPart1() []checkRecoveryTestCase {
	return []checkRecoveryTestCase{
		{
			name:        "nil server returns false",
			nilServer:   true,
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "invalid PR number returns false",
			repo:        "azylman/aerial",
			prNumber:    0,
			headSHA:     "sha10",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "empty head sha returns false",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "zero head sha without registry failure returns false",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "0000000000000000000000000000000000000000",
			evalCheck:   "Unit Tests",
			wantRecover: false,
		},
		{
			name:        "empty check name returns false",
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "",
			wantRecover: false,
		},
		{
			name:        "nil server returns false for roborev",
			nilServer:   true,
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "roborev",
			wantRecover: false,
		},
		{
			name:        "nil server returns false for CI Gate",
			nilServer:   true,
			repo:        "azylman/aerial",
			prNumber:    10,
			headSHA:     "sha10",
			evalCheck:   "CI Gate",
			wantRecover: false,
		},
		{
			name:                   "non-gate success recovers on registry ci_failed",
			repo:                   "azylman/aerial",
			prNumber:               11,
			headSHA:                "sha11",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha11",
			evalCheck:              "Unit Tests",
			wantRecover:            true,
		},
		{
			name:                   "non-gate success rejected when registry head sha differs",
			repo:                   "azylman/aerial",
			prNumber:               12,
			headSHA:                "sha12-new",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha12-stale",
			evalCheck:              "Unit Tests",
			wantRecover:            false,
		},
		{
			name:        "non-gate success without registry failure returns false",
			repo:        "azylman/aerial",
			prNumber:    13,
			headSHA:     "sha13",
			evalCheck:   "Lint",
			wantRecover: false,
		},
	}
}

func checkRecoveryCasesPart2() []checkRecoveryTestCase {
	return []checkRecoveryTestCase{
		{
			name:                   "ci gate success recovers on registry ci failed",
			repo:                   "azylman/aerial",
			prNumber:               22,
			headSHA:                "sha22",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha22",
			evalCheck:              "CI Gate",
			wantRecover:            true,
		},
		{
			name:                   "ci gate recovery rejected when registry head sha differs",
			repo:                   "azylman/aerial",
			prNumber:               24,
			headSHA:                "sha24-new",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha24-stale",
			evalCheck:              "CI Gate",
			wantRecover:            false,
		},
		{
			name:        "ci gate success without registry failure returns false",
			repo:        "azylman/aerial",
			prNumber:    27,
			headSHA:     "sha27",
			evalCheck:   "CI Gate",
			wantRecover: false,
		},
		{
			name:                   "roborev recovery via preset registry ci failed",
			repo:                   "azylman/aerial",
			prNumber:               23,
			headSHA:                "sha23",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha23",
			evalCheck:              "roborev",
			wantRecover:            true,
		},
		{
			name:                   "roborev recovery rejected when registry head sha differs",
			repo:                   "azylman/aerial",
			prNumber:               25,
			headSHA:                "sha25-new",
			presetRegistryCIFailed: true,
			presetRegistryHeadSHA:  "sha25-stale",
			evalCheck:              "roborev",
			wantRecover:            false,
		},
		{
			name:         "roborev recovery via roborev-override comment matching",
			repo:         "azylman/aerial",
			prNumber:     26,
			headSHA:      "sha26",
			commentsBody: `[{"body":"## roborev-override: manual override approved for commit ` + "`sha26`" + `"}]`,
			evalCheck:    "roborev",
			wantRecover:  true,
		},
		{
			name:         "roborev without matching comment returns false",
			repo:         "azylman/aerial",
			prNumber:     28,
			headSHA:      "sha28",
			commentsBody: `[{"body":"unrelated chatter"}]`,
			evalCheck:    "roborev",
			wantRecover:  false,
		},
	}
}

func TestCheckRecoveryHelpers(t *testing.T) {
	t.Parallel()

	tests := append(checkRecoveryCasesPart1(), checkRecoveryCasesPart2()...)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCheckRecoveryCase(t, tc)
		})
	}
}

func TestParseAerialPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body, wantTarget, wantBotID string
		wantOK                            bool
	}{
		{name: "no trailer", body: "Standard PR body\n<!-- regular comment -->"},
		{name: "unclosed trailer", body: "Body\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"}"},
		{name: "invalid json", body: "Body\n<!-- aerial-pr: {bad-json} -->"},
		{name: "invalid target and empty", body: "Body\n<!-- aerial-pr: {\"target_id\":\"bad\"} -->"},
		{name: "valid target", body: "<!-- x -->\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "discord prefixed target and pool prefix", body: "<!-- aerial-pr: {\"target_id\":\"stateful_medium:discord-100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "valid bot id only", body: "<!-- aerial-pr: {\"bot_id\":\"eri-sub\"} -->", wantBotID: "eri-sub", wantOK: true},
		{name: "valid target and bot id", body: "<!-- aerial-pr: {\"target_id\":\"100000000000000002\",\"bot_id\":\"eri-sub\"} -->", wantTarget: "100000000000000002", wantBotID: "eri-sub", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseAerialPRMetadata(tc.body)
			if got.OK() != tc.wantOK || got.TargetID != tc.wantTarget || got.BotID != tc.wantBotID {
				t.Fatalf("ParseAerialPRMetadata() = %+v (ok=%v), want target=%q bot=%q ok=%v", got, got.OK(), tc.wantTarget, tc.wantBotID, tc.wantOK)
			}
		})
	}
}

func TestClassifyCheckState_Unit(t *testing.T) {
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
		{name: "completed unknown conclusion", status: "completed", conclusion: "stale", want: ""},
		{name: "queued", status: "queued", want: "pending"},
		{name: "in_progress", status: "in_progress", want: "pending"},
		{name: "waiting", status: "waiting", want: "pending"},
		{name: "requested", status: "requested", want: "pending"},
		{name: "pending", status: "pending", want: "pending"},
		{name: "other non-completed", status: "custom_status", want: "pending"},
		{name: "empty status", status: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyCheckState(tc.status, tc.conclusion)
			if got != tc.want {
				t.Fatalf("classifyCheckState(%q, %q) = %q, want %q", tc.status, tc.conclusion, got, tc.want)
			}
		})
	}
}

func TestExtractStringMapFromMetadata_Unit(t *testing.T) {
	t.Parallel()

	if res := extractStringMapFromMetadata(nil, "key"); res != nil {
		t.Errorf("expected nil for nil meta, got %v", res)
	}
	if res := extractStringMapFromMetadata(map[string]any{}, "key"); res != nil {
		t.Errorf("expected nil for missing key, got %v", res)
	}

	paddedKey := " ERI "
	metaObj := map[string]any{
		"job_states": map[string]any{
			paddedKey: "verifying_health",
			"":        "invalid",
			"HARO":    "starting",
		},
	}
	gotObj := extractStringMapFromMetadata(metaObj, "job_states")
	if gotObj["eri"] != "verifying_health" || gotObj["haro"] != "starting" {
		t.Errorf("unexpected map from interface: %+v", gotObj)
	}

	metaStr := map[string]any{
		"job_states": map[string]string{
			" CADVISOR ": " healthy ",
		},
	}
	gotStr := extractStringMapFromMetadata(metaStr, "job_states")
	if gotStr["cadvisor"] != "healthy" {
		t.Errorf("unexpected map from string map: %+v", gotStr)
	}
}

func TestPublishDeployLifecycleEvent_Unit(t *testing.T) {
	t.Parallel()
	bus := &mockEventBus{}
	srv := &RouterServer{bus: bus}
	evt := DeployEvent{
		Repo:     "azylman/aerial",
		PRNumber: 42,
	}
	if err := srv.publishDeployLifecycleEvent(context.Background(), evt, "deploy_success", "deployed"); err != nil {
		t.Fatalf("publishDeployLifecycleEvent() err = %v", err)
	}
	if len(bus.publishedPREvents) != 1 || bus.publishedPREvents[0].JobName != "git-sync" {
		t.Errorf("expected jobName git-sync, got %v", bus.publishedPREvents)
	}
}

type kubeRolloutTestCase struct {
	name           string
	routes         map[string]kubeRoute
	wantReconciled []string
	wantRecorded   []string
	wantErr        bool
}

func assertKubeRolloutRequests(t *testing.T, fixture *kubeFixture, tc kubeRolloutTestCase, reg *mockPRRegistry, configMapPath, kustPrefix string) {
	t.Helper()
	lastTagWrite, firstReconcile := -1, -1
	var reconciled []string
	for i, req := range fixture.Requests() {
		switch {
		case req == configMapPath:
			lastTagWrite = i
		case strings.HasPrefix(req, kustPrefix):
			if firstReconcile < 0 {
				firstReconcile = i
			}
			reconciled = append(reconciled, strings.TrimPrefix(req, kustPrefix))
		}
	}
	if lastTagWrite < 0 || firstReconcile <= lastTagWrite {
		t.Errorf("requests %v: want every configmap write before the first reconcile", fixture.Requests())
	}
	slices.Sort(reconciled)
	if !slices.Equal(reconciled, tc.wantReconciled) {
		t.Errorf("reconciled = %v, want %v", reconciled, tc.wantReconciled)
	}
	if len(reg.deployingCalls) != 1 || !slices.Equal(reg.deployingCalls[0].jobs, []string{"synapse", "eri"}) {
		t.Errorf("deploying calls = %+v, want one call with jobs [synapse eri]", reg.deployingCalls)
	}
	assertKubeRolloutRecorded(t, fixture, tc, reg, kustPrefix)
}

func assertKubeRolloutRecorded(t *testing.T, fixture *kubeFixture, tc kubeRolloutTestCase, reg *mockPRRegistry, kustPrefix string) {
	t.Helper()
	if len(reg.kustomizationRequestCalls) != 1 {
		t.Fatalf("recorded request calls = %d, want 1", len(reg.kustomizationRequestCalls))
	}
	recorded := reg.kustomizationRequestCalls[0]
	if got := slices.Sorted(maps.Keys(recorded)); !slices.Equal(got, tc.wantRecorded) {
		t.Errorf("recorded kustomizations = %v, want %v", got, tc.wantRecorded)
	}
	for name, req := range recorded {
		if req.Namespace != "flux-system" || !strings.Contains(fixture.Body(kustPrefix+name), `"`+req.RequestedAt+`"`) {
			t.Errorf("recorded %s = %+v, want namespace flux-system and the patched requestedAt (body %s)", name, req, fixture.Body(kustPrefix+name))
		}
	}
}

func runKubeRolloutCase(t *testing.T, tc kubeRolloutTestCase, configMapPath, kustPrefix string) {
	t.Helper()
	t.Parallel()
	fixture, ts := newKubeFixture(t, tc.routes)
	disp := NewDefaultOutboundDispatcher("", ts.Client())
	disp.SetKubeConfig(ts.URL, "test-token")
	reg := &mockPRRegistry{resolveTargetID: "100000000000000001"}
	srv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: ts.URL, DefaultOwner: true}), legacyFixtureClient(ts.Client()), reg, disp)

	err := srv.dispatchWorkflowRunImages(context.Background(), "azylman/aerial", workflowRunEvent{ID: 7, Attempt: 1, HeadSHA: legacyFixtureHead}, "main", "success", "100000000000000001", 42)
	if (err != nil) != tc.wantErr {
		t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertKubeRolloutRequests(t, fixture, tc, reg, configMapPath, kustPrefix)
}

func TestDispatchWorkflowRunImages_KubeRollout(t *testing.T) {
	t.Parallel()
	const (
		jobsPath      = "GET /repos/azylman/aerial/actions/runs/7/jobs"
		configMapPath = "PATCH /api/v1/namespaces/flux-system/configmaps/aerial-image-tags"
		kustPrefix    = "PATCH /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/"
	)
	twoImages := `{"jobs":[{"name":"Build & Push Images to GHCR (synapse)","status":"completed","conclusion":"success"},{"name":"Build & Push Images to GHCR (eri)","status":"completed","conclusion":"success"}]}`
	ownedDeployments := `{"items":[
		{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"aerial-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:old"},{"image":"ghcr.io/azylman/aerial-eri:old"}]}}}},
		{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"marida-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:old"}]}}}}
	]}`
	fallbackKustomizations := `{"items":[{"metadata":{"name":"aerial-config","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"aerial-image-tags"}]}}}]}`

	tests := []kubeRolloutTestCase{
		{
			name: "every owning kustomization reconciles once after all tags are written",
			routes: kubeWorkloadRoutes(map[string]kubeRoute{
				jobsPath:                   {body: twoImages},
				kubeDeploymentsPath:        {body: ownedDeployments},
				configMapPath:              {body: `{}`},
				kustPrefix + "aerial-core": {body: `{}`},
				kustPrefix + "marida-core": {body: `{}`},
			}),
			wantReconciled: []string{"aerial-core", "marida-core"},
			wantRecorded:   []string{"aerial-core", "marida-core"},
		},
		{
			name: "no labeled owner falls back to image tag substituting kustomizations",
			routes: kubeWorkloadRoutes(map[string]kubeRoute{
				jobsPath:                     {body: twoImages},
				kubeKustomizations:           {body: fallbackKustomizations},
				configMapPath:                {body: `{}`},
				kustPrefix + "aerial-config": {body: `{}`},
			}),
			wantReconciled: []string{"aerial-config"},
			wantRecorded:   []string{"aerial-config"},
		},
		{
			name: "partial reconcile failure still records every request before patching",
			routes: kubeWorkloadRoutes(map[string]kubeRoute{
				jobsPath:                   {body: twoImages},
				kubeDeploymentsPath:        {body: ownedDeployments},
				configMapPath:              {body: `{}`},
				kustPrefix + "aerial-core": {status: http.StatusForbidden, body: `forbidden`},
				kustPrefix + "marida-core": {body: `{}`},
			}),
			wantReconciled: []string{"aerial-core", "marida-core"},
			wantRecorded:   []string{"aerial-core", "marida-core"},
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runKubeRolloutCase(t, tc, configMapPath, kustPrefix)
		})
	}
}

func TestHasFailedCIForSHA_ResponseHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		body       string
		repo       string
		prNumber   int
		headSHA    string
		wantResult bool
	}{
		{
			name:       "404 not found treats as not failed",
			status:     http.StatusNotFound,
			body:       `{"message":"Not Found"}`,
			wantResult: false,
		},
		{
			name:       "500 internal server error fails closed",
			status:     http.StatusInternalServerError,
			body:       `{"message":"Internal Server Error"}`,
			wantResult: true,
		},
		{
			name:       "403 rate limited fails closed",
			status:     http.StatusForbidden,
			body:       `{"message":"API rate limit exceeded"}`,
			wantResult: true,
		},
		{
			name:       "200 success with failed check returns true",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			wantResult: true,
		},
		{
			name:       "200 success with pending check returns true",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"Unit Tests","status":"in_progress"}]}`,
			wantResult: true,
		},
		{
			name:       "200 success with all passed returns false",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"Unit Tests","status":"completed","conclusion":"success"}]}`,
			wantResult: false,
		},
		{
			name:       "multiple check runs ascending: older failed, newer succeeded returns false",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"id":100,"name":"Unit Tests","status":"completed","conclusion":"failure"},{"id":200,"name":"Unit Tests","status":"completed","conclusion":"success"}]}`,
			wantResult: false,
		},
		{
			name:       "multiple check runs descending: newer succeeded first, older failed second returns false",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"id":200,"name":"Unit Tests","status":"completed","conclusion":"success"},{"id":100,"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			wantResult: false,
		},
		{
			name:       "multiple check runs: older succeeded, newer failed returns true",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"id":100,"name":"Unit Tests","status":"completed","conclusion":"success"},{"id":200,"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			wantResult: true,
		},
		{
			name:       "empty repo returns false",
			repo:       "none",
			wantResult: false,
		},
		{
			name:       "invalid pr number <= 0 returns false",
			prNumber:   -1,
			wantResult: false,
		},
		{
			name:       "empty head sha returns false",
			headSHA:    "none",
			wantResult: false,
		},
		{
			name:       "zero head sha returns false",
			headSHA:    "0000000000000000000000000000000000000000",
			wantResult: false,
		},
		{
			name:       "dedupe filters out empty check name and metadata checks",
			status:     http.StatusOK,
			body:       `{"check_runs":[{"name":"","status":"completed","conclusion":"failure"},{"name":"build-metadata","status":"completed","conclusion":"failure"},{"app":{"slug":"dependabot"},"name":"Dependabot Check","status":"completed","conclusion":"failure"},{"id":1,"name":"Unit Tests","status":"completed","conclusion":"success"}]}`,
			wantResult: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			repo := "azylman/aerial"
			if tc.repo == "none" {
				repo = ""
			}
			prNumber := 42
			if tc.prNumber != 0 {
				prNumber = tc.prNumber
			}
			headSHA := "head123"
			if tc.headSHA == "none" {
				headSHA = ""
			} else if tc.headSHA != "" {
				headSHA = tc.headSHA
			}

			srv := newTestRouter(Config{GitHubAPIURL: ts.URL}, ts.Client(), nil, nil)
			got := srv.hasFailedCIForSHA(context.Background(), repo, prNumber, headSHA)
			if got != tc.wantResult {
				t.Errorf("hasFailedCIForSHA() = %v, want %v", got, tc.wantResult)
			}
		})
	}
}

func TestRouterServer_NilGuards(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if emit, active, prior := nilSrv.recordFailedCheckRunDetailed("repo", 1, "sha", "check"); emit || active || prior {
		t.Errorf("expected all false for nil server")
	}
	if emit, active, prior := nilSrv.recordNonGateCheckFailureDetailed(nil); emit || active || prior {
		t.Errorf("expected all false for nil event")
	}
	if nilSrv.isAutoUpdateAuthorAllowed("author") {
		t.Errorf("expected false")
	}
	if _, _, err := nilSrv.resolvePRFromCommitSHA(context.Background(), "repo", "sha"); err == nil {
		t.Errorf("expected error for nil client")
	}
	if err := nilSrv.completeCIFailureTransition(context.Background(), nil, "", ""); err != nil {
		t.Errorf("expected nil err, got %v", err)
	}
	if nilSrv.isMergeSHAFailure(context.Background(), nil) {
		t.Errorf("expected false for nil event")
	}
	if nilSrv.isPRRegistryCIFailed(context.Background(), "repo", 1) {
		t.Errorf("expected false for nil server")
	}
	srv := &RouterServer{}
	srv.handleStaleOpenPR(context.Background(), "repo", 1)
	if srv.isPRRegistryCIFailed(context.Background(), "repo", 1) {
		t.Errorf("expected false for nil registry")
	}
	if nilSrv.fetchLatestRoboRevComment(context.Background(), "repo", 1, "sha") != "" {
		t.Errorf("expected empty string")
	}
	if err := nilSrv.notifyCollaboratorConflict(context.Background(), "repo", GitHubPullRequest{}); err != nil {
		t.Errorf("expected nil err, got %v", err)
	}
	if err := nilSrv.notifyCollaboratorCIFailure(context.Background(), nil, ""); err != nil {
		t.Errorf("expected nil err, got %v", err)
	}
}
