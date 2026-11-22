package router

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	cfgPRKey   = "/PRObject/azylman%2Faerial-config%2342/"
	cfgRepoKey = "/RepoObject/azylman%2Faerial-config/"
	ownedReqAt = "2026-11-20T00:00:00Z"
)

// deployViewBody renders a PRObject deploy view for azylman/<repo>#42.
func deployViewBody(repo, status, extra string) string {
	return fmt.Sprintf(`{"found":true,"repo":"azylman/%s","pr_number":42,"status":%q,"branch":"feat","head_sha":%q,"merge_sha":%q,"target_id":%q%s}`,
		repo, status, ownedHead, ownedMerge, ownedThread, extra)
}

// trackedView is a deploying view recording one core request.
func trackedView(repo string) string {
	return deployViewBody(repo, statusDeploying, `,"kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"`+ownedReqAt+`"}}`)
}

// assertOwnedDeploy checks the exact Restate sends and the stateless system-alert effects a PR-less alert performs.
func assertOwnedDeploy(t *testing.T, h *ownedHarness, wantPaths, wantBodies []string, wantEffects int) {
	t.Helper()
	paths, bodies := h.ingress.sends()
	if strings.Join(paths, ",") != strings.Join(wantPaths, ",") {
		t.Fatalf("sends = %v, want %v", paths, wantPaths)
	}
	for i, sub := range wantBodies {
		if !strings.Contains(bodies[i], sub) {
			t.Errorf("send %d body = %s, want substring %s", i, bodies[i], sub)
		}
	}
	if got := h.effectCount(); got != wantEffects {
		t.Errorf("synapse effects = %d, want %d", got, wantEffects)
	}
}

// processDeployEventOwnedCase is one deploy alert and the exact Restate sends
// and synapse effects it must produce.
type processDeployEventOwnedCase struct {
	name        string
	evt         DeployEvent
	bodies      map[string]string
	callCode    int
	repoRefs    map[string][]fluxKustomizationRef
	ghBody      string
	wantPaths   []string
	wantBodies  []string
	wantEffects int
	wantErr     bool
	// wantBusEvent is the exact "source:event_type" trigger list fired on
	// the event bus ("" when no trigger fires).
	wantBusEvent string
}

func processDeployEventOwnedCases() []processDeployEventOwnedCase {
	cfgRepo := "azylman/aerial-config"
	cfgRefs := map[string][]fluxKustomizationRef{cfgRepo: {{Namespace: "flux-system", Name: "aerial-config"}}}
	return []processDeployEventOwnedCase{
		{
			name: "untracked flux success starts the watcher rollout", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: kindKustomization, JobName: "aerial-config", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")}, repoRefs: cfgRefs,
			wantPaths:  []string{cfgPRKey + "HandleKustomizationRequested/send"},
			wantBodies: []string{`"jobs":["aerial-config"],"kustomization_requests":{"aerial-config":{"namespace":"flux-system"`},
		},
		{
			name: "tracked flux alert defers silently", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: kindKustomization, JobName: "core", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": trackedView("aerial-config")},
		},
		{
			name: "flux discovery failure suppresses the alert", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: kindKustomization, JobName: "core", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")}, repoRefs: map[string][]fluxKustomizationRef{"discover-error": nil},
		},
		{
			name: "unowned success goes to the merged deploy verdict", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeploySuccess/send"},
			wantBodies: []string{`"facts":[{"event":"deploy_success","repo":"azylman/aerial-config","pr_number":42,"merge_sha":"` + ownedMerge + `","target_id":"` + ownedThread + `","status":"deployed","job_name":"eri"`},
		},
		{
			name: "pr failure carries the deploy_failed fact", evt: DeployEvent{Event: eventDeployFailed, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge, Details: "crashloop"},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial-config", statusDeploying, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeployFailed/send"},
			wantBodies: []string{`"event":"deploy_failed"`},
		},
		{
			name: "started job transitions through the deploy observation", evt: DeployEvent{Event: eventDeployStarted, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge, ExpectedJobs: []string{"haro"}},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeployObserved/send"},
			wantBodies: []string{`"job_name":"eri"`},
		},
		{
			name: "sync with expected jobs records them and publishes sync_success", wantBusEvent: "flux:sync_success", evt: DeployEvent{Event: eventSyncSuccess, Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge, ExpectedJobs: []string{"eri"}},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeployObserved/send", cfgPRKey + "HandleDeployObserved/send"},
			wantBodies: []string{`"expected_jobs":["eri"]`, `"event":"sync_success"`},
		},
		{
			name: "sync without expected jobs defers to the watcher", wantBusEvent: "flux:sync_success", evt: DeployEvent{Event: eventSyncSuccess, Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")}, repoRefs: cfgRefs,
			wantPaths: []string{cfgPRKey + "HandleKustomizationRequested/send"},
		},
		{
			name: "sync of a repo without flux completes with git-sync", wantBusEvent: "flux:sync_success", evt: DeployEvent{Event: eventSyncSuccess, Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeploySuccess/send"},
			wantBodies: []string{`"job_name":"git-sync"`},
		},
		{
			name: "sync failure publishes through the deploy observation", wantBusEvent: "flux:sync_failed", evt: DeployEvent{Event: eventSyncFailed, Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeployObserved/send"},
			wantBodies: []string{`"event":"sync_failed"`},
		},
		{
			name: "commit resolves through the restate sha index", evt: DeployEvent{Event: eventDeployFailed, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, CommitSHA: ownedMerge},
			bodies:    map[string]string{"ResolvePRBySHA": `{"pr_number":42,"branch":"feat","found":true}`, "GetDeployView": deployViewBody("aerial-config", statusDeploying, "")},
			wantPaths: []string{cfgPRKey + "HandleDeployFailed/send"},
		},
		{
			name: "pr-less failure keeps the stateless system alert", evt: DeployEvent{Event: eventDeployFailed, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, CommitSHA: ownedMerge},
			bodies: map[string]string{"ResolvePRBySHA": `{"found":false}`}, ghBody: "[]", wantEffects: 3,
		},
		{
			name: "deploy view failure retries without a send", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			callCode: http.StatusInternalServerError, wantErr: true,
		},
		{
			name: "rollback carries the deploy_failed fact", evt: DeployEvent{Event: eventDeployRollback, SourceKind: "HelmRelease", JobName: "eri", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies:    map[string]string{"GetDeployView": deployViewBody("aerial-config", statusDeploying, "")},
			wantPaths: []string{cfgPRKey + "HandleDeployFailed/send"},
		},
		{
			name: "pr without an alert target publishes nothing", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": `{"found":true,"repo":"azylman/aerial-config","pr_number":42,"status":"merged"}`},
		},
		{
			name: "success while the image build is pending publishes sync_success", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: kindKustomization, JobName: "aerial-core", Repo: "azylman/aerial", PRNumber: 42, CommitSHA: ownedMerge},
			bodies:     map[string]string{"GetDeployView": deployViewBody("aerial", prMerged, ""), "GetBuildLifecycle": `{"found":true,"status":"merged"}`},
			repoRefs:   map[string][]fluxKustomizationRef{"azylman/aerial": {{Namespace: "flux-system", Name: "aerial-core"}}},
			wantPaths:  []string{ownedPRKey + "HandleDeployObserved/send"},
			wantBodies: []string{`"event":"sync_success"`},
		},
		{
			name: "started while the image build is pending is skipped", evt: DeployEvent{Event: eventDeployStarted, SourceKind: kindKustomization, JobName: "aerial-core", Repo: "azylman/aerial", PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": deployViewBody("aerial", prMerged, ""), "GetBuildLifecycle": `{"found":true,"status":"merged"}`},
		},
		{
			name: "sha index failure falls back to the github lookup", evt: DeployEvent{Event: eventDeployFailed, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, CommitSHA: ownedMerge},
			bodies: map[string]string{"ResolvePRBySHA": `nope`}, ghBody: "[]", wantEffects: 3,
		},
		{
			name: "untracked pr patches its kustomizations once without tracking", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: kindKustomization, JobName: "aerial-config", Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": `{"found":false}`}, repoRefs: cfgRefs, wantEffects: 1,
		},
		{
			name: "pr-less sync failure keeps the stateless system alert", wantBusEvent: "flux:sync_failed", evt: DeployEvent{Event: eventSyncFailed, Repo: cfgRepo, Details: "fetch failed"},
			wantEffects: 3,
		},
		{
			name: "pr-less deploy started publishes its fact", evt: DeployEvent{Event: eventDeployStarted, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, TargetID: ownedThread},
			wantEffects: 1,
		},
		{
			name: "pr-less deploy success publishes and prompts the system channel", evt: DeployEvent{Event: eventDeploySuccess, SourceKind: "Deployment", JobName: "eri", Repo: cfgRepo, TargetID: ownedThread, DeploymentID: "rs-1"},
			wantEffects: 2,
		},
		{
			name: "pr-less rollback keeps the stateless system alert", evt: DeployEvent{Event: eventDeployRollback, SourceKind: "HelmRelease", JobName: "eri", Repo: cfgRepo},
			wantEffects: 3,
		},
		{
			name: "pr-less sync success only fires the flux trigger", wantBusEvent: "flux:sync_success", evt: DeployEvent{Event: eventSyncSuccess, Repo: cfgRepo},
		},
		{
			name: "sync deferral discovery failure retries", wantBusEvent: "flux:sync_success", evt: DeployEvent{Event: eventSyncSuccess, Repo: cfgRepo, PRNumber: 42, CommitSHA: ownedMerge},
			bodies: map[string]string{"GetDeployView": deployViewBody("aerial-config", prMerged, "")}, repoRefs: map[string][]fluxKustomizationRef{"discover-error": nil}, wantErr: true,
		},
	}
}

// TestProcessDeployEventRestateOwned proves restate-owned deploy alerts are
// a Postgres-free edge: PR state comes from PRObject.GetDeployView, every PR
// outcome is a PRObject send carrying its fact, and only PR-less alerts keep
// the stateless system-alert effects.
func TestProcessDeployEventRestateOwned(t *testing.T) {
	t.Parallel()
	tests := processDeployEventOwnedCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.callCode
			if code == 0 {
				code = http.StatusOK
			}
			ghBody := tc.ghBody
			if ghBody == "" {
				ghBody = "{}"
			}
			h := newOwnedHarness(t, Config{SystemChannelID: ownedSystem}, &ownedIngress{sendCode: http.StatusAccepted, callCode: code, callBody: "{}", handlerBodies: tc.bodies}, ghBody)
			h.disp.repoKustomizations = tc.repoRefs
			if _, broken := tc.repoRefs["discover-error"]; broken {
				h.disp.repoDiscoverErr = fmt.Errorf("kube down")
			}
			_, err := h.srv.ProcessDeployEvent(ownedCtx(), tc.evt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessDeployEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, tc.wantEffects)
			if got := h.triggerEvents(); got != tc.wantBusEvent {
				t.Errorf("trigger events = %q, want %q", got, tc.wantBusEvent)
			}
		})
	}
}

// TestOwnedWaitingForImageBuild pins the Restate-backed image-build wait:
// lifecycle from GetBuildLifecycle, tracked requests from the deploy view.
func TestOwnedWaitingForImageBuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		repo      string
		noURL     bool
		callCode  int
		lifecycle string
		view      prwire.DeployView
		want      bool
	}{
		{name: "repo without build metadata never waits", repo: "azylman/aerial-config"},
		{name: "open pr without a final build waits", repo: "azylman/aerial", lifecycle: `{"found":true,"status":"open"}`, want: true},
		{name: "started build stops waiting", repo: "azylman/aerial", lifecycle: `{"found":true,"status":"merged","started":true}`},
		{name: "tracked requests stop waiting", repo: "azylman/aerial", lifecycle: `{"found":true,"status":"merged"}`, view: prwire.DeployView{KustomizationRequests: map[string]prwire.KustomizationRequest{"core": {}}}},
		{name: "final build with nothing selected stops waiting", repo: "azylman/aerial", lifecycle: `{"found":true,"status":"merged","has_build_metadata":true,"build_metadata":{"version":1,"kind":"final","selected":[]}}`},
		{name: "deploying pr never waits", repo: "azylman/aerial", lifecycle: `{"found":true,"status":"deploying"}`},
		{name: "lifecycle read failure waits", repo: "azylman/aerial", callCode: http.StatusInternalServerError, want: true},
		{name: "no restate url waits", repo: "azylman/aerial", noURL: true, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.callCode
			if code == 0 {
				code = http.StatusOK
			}
			ingress := &ownedIngress{callCode: code, callBody: tc.lifecycle}
			cfg := Config{}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			if got := srv.ownedWaitingForImageBuild(ownedCtx(), tc.repo, 42, tc.view); got != tc.want {
				t.Fatalf("ownedWaitingForImageBuild() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplyKustomizationStatusRestateOwned proves the restate-owned watcher
// reads its rollouts from the Restate index and reports each gate outcome
// to PRObject with zero registry calls and zero synapse effects.
func TestApplyKustomizationStatusRestateOwned(t *testing.T) {
	t.Parallel()
	ready := []model.FluxCondition{{Type: "Ready", Status: "True"}}
	core := fluxKustomizationRef{Namespace: "flux-system", Name: "core"}
	list := `{"rollouts":[{"repo":"azylman/aerial-config","pr_number":42}]}`
	tests := []struct {
		name         string
		obs          kustomizationObservation
		bodies       map[string]string
		callCode     int
		sourceRepo   string
		workloadsErr bool
		ghBody       string
		wantPaths    []string
		wantBodies   []string
		wantErr      bool
	}{
		{
			name: "live change completes the kustomization", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt, Conditions: ready},
			bodies:     map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")},
			wantPaths:  []string{cfgPRKey + "HandleRolloutProgress/send", cfgPRKey + "HandleDeploySuccess/send"},
			wantBodies: []string{`"deployed_jobs":["core"],"deployed_kustomizations":["core"]`, `"facts":[{"event":"deploy_success"`},
		},
		{
			name: "suspended kustomization fails the rollout", obs: kustomizationObservation{Ref: core, Suspended: true},
			bodies:     map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")},
			wantPaths:  []string{cfgPRKey + "HandleDeployFailed/send"},
			wantBodies: []string{`"details":"Kustomization suspended"`},
		},
		{
			name: "lost request re-patches through PRObject", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: "2026-11-19T00:00:00Z", RequestedAt: "2026-11-19T00:00:00Z"},
			bodies:     map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")},
			wantPaths:  []string{cfgPRKey + "HandleReconcileLost/send"},
			wantBodies: []string{`"job_name":"core"`},
		},
		{
			name: "unready kustomization waits", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")},
		},
		{
			name: "view no longer deploying is skipped", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt, Conditions: ready},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": deployViewBody("aerial-config", "deployed", "")},
		},
		{
			name: "own-repo source waits until the merge commit is applied", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt, Conditions: ready},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")}, sourceRepo: "azylman/aerial-config",
		},
		{
			name: "own-repo source that applied the merge commit completes", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt, LastAppliedRevision: "main@sha1:" + ownedMerge, Conditions: ready},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")}, sourceRepo: "azylman/aerial-config",
			wantPaths: []string{cfgPRKey + "HandleRolloutProgress/send", cfgPRKey + "HandleDeploySuccess/send"},
		},
		{
			name: "workload list failure surfaces for the watcher retry", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt, Conditions: ready},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")}, workloadsErr: true, wantErr: true,
		},
		{
			name: "merge time failure surfaces for the watcher retry", obs: kustomizationObservation{Ref: core, LastHandledReconcileAt: ownedReqAt, Conditions: ready},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")}, ghBody: `{}`, wantErr: true,
		},
		{
			name: "rollout index failure surfaces for the watcher retry", obs: kustomizationObservation{Ref: core},
			callCode: http.StatusInternalServerError, wantErr: true,
		},
		{
			name: "deploy view failure surfaces for the watcher retry", obs: kustomizationObservation{Ref: core},
			bodies: map[string]string{"ListRollouts": list, "GetDeployView": `nope`}, wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.callCode
			if code == 0 {
				code = http.StatusOK
			}
			ghBody := tc.ghBody
			if ghBody == "" {
				ghBody = `{"committer":{"date":"2026-11-20T00:00:00Z"}}`
			}
			h := newOwnedHarness(t, Config{}, &ownedIngress{sendCode: http.StatusAccepted, callCode: code, handlerBodies: tc.bodies}, ghBody)
			h.disp.sourceRepo = tc.sourceRepo
			if tc.workloadsErr {
				h.disp.workloadsErr = errors.New("kube down")
			}
			err := h.srv.ApplyKustomizationStatus(ownedCtx(), tc.obs)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ApplyKustomizationStatus() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}

// TestApplyKustomizationListRestateOwned proves a full LIST applies every
// observation and fails a rollout whose pending Kustomization is gone from
// its namespace.
func TestApplyKustomizationListRestateOwned(t *testing.T) {
	t.Parallel()
	list := `{"rollouts":[{"repo":"azylman/aerial-config","pr_number":42}]}`
	core := fluxKustomizationRef{Namespace: "flux-system", Name: "core"}
	tests := []struct {
		name         string
		namespace    string
		observations []kustomizationObservation
		callCode     int
		wantPaths    []string
		wantBodies   []string
		wantErr      bool
	}{
		{
			name: "pending kustomization missing from the list fails the rollout", namespace: "flux-system",
			wantPaths: []string{cfgPRKey + "HandleDeployFailed/send"}, wantBodies: []string{`"details":"Kustomization deleted"`},
		},
		{name: "listed unready kustomization waits", namespace: "flux-system", observations: []kustomizationObservation{{Ref: core, LastHandledReconcileAt: ownedReqAt}}},
		{name: "another namespace's list leaves the rollout alone", namespace: "apps"},
		{name: "rollout index failure surfaces for the watcher retry", namespace: "flux-system", callCode: http.StatusInternalServerError, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.callCode
			if code == 0 {
				code = http.StatusOK
			}
			h := newOwnedHarness(t, Config{}, &ownedIngress{sendCode: http.StatusAccepted, callCode: code, handlerBodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")}}, "{}")
			err := h.srv.ApplyKustomizationList(ownedCtx(), tc.namespace, tc.observations)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ApplyKustomizationList() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}

// TestApplyKustomizationDeletedRestateOwned proves a deleted Kustomization
// fails every rollout still waiting on it.
func TestApplyKustomizationDeletedRestateOwned(t *testing.T) {
	t.Parallel()
	list := `{"rollouts":[{"repo":"azylman/aerial-config","pr_number":42}]}`
	tests := []struct {
		name       string
		ref        fluxKustomizationRef
		callCode   int
		wantPaths  []string
		wantBodies []string
		wantErr    bool
	}{
		{
			name: "deleted pending kustomization fails the rollout", ref: fluxKustomizationRef{Namespace: "flux-system", Name: "core"},
			wantPaths: []string{cfgPRKey + "HandleDeployFailed/send"}, wantBodies: []string{`"details":"Kustomization deleted"`},
		},
		{name: "deleted unrelated kustomization changes nothing", ref: fluxKustomizationRef{Namespace: "flux-system", Name: "other"}},
		{name: "rollout index failure surfaces for the watcher retry", ref: fluxKustomizationRef{Namespace: "flux-system", Name: "core"}, callCode: http.StatusInternalServerError, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.callCode
			if code == 0 {
				code = http.StatusOK
			}
			h := newOwnedHarness(t, Config{}, &ownedIngress{sendCode: http.StatusAccepted, callCode: code, handlerBodies: map[string]string{"ListRollouts": list, "GetDeployView": trackedView("aerial-config")}}, "{}")
			err := h.srv.ApplyKustomizationDeleted(ownedCtx(), tc.ref)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ApplyKustomizationDeleted() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}

// TestSendOwnedRolloutProgress pins the gate outcome sends: progress with
// the rollout_updated fact, then HandleDeploySuccess with the deploy_success
// fact for a completed Kustomization; an empty outcome sends nothing.
func TestSendOwnedRolloutProgress(t *testing.T) {
	t.Parallel()
	pr := DeployingPR{Repo: "azylman/aerial-config", PRNumber: 42, Branch: "dependabot/go/x", HeadSHA: ownedHead, MergeSHA: ownedMerge, TargetID: ownedThread}
	tests := []struct {
		name       string
		ready      []string
		done       bool
		wantPaths  []string
		wantBodies []string
	}{
		{name: "ready jobs ride with the progress fact", ready: []string{"eri"}, wantPaths: []string{cfgPRKey + "HandleRolloutProgress/send"}, wantBodies: []string{`"deployed_jobs":["eri"],"facts":[{"event":"rollout_updated"`}},
		{name: "completion follows with the deploy_success verdict", ready: []string{"core"}, done: true, wantPaths: []string{cfgPRKey + "HandleRolloutProgress/send", cfgPRKey + "HandleDeploySuccess/send"}, wantBodies: []string{`"deployed_kustomizations":["core"]`, `"facts":[{"event":"deploy_success","repo":"azylman/aerial-config","pr_number":42,"head_sha":"` + ownedHead + `","merge_sha":"` + ownedMerge + `","target_id":"` + ownedThread + `","status":"deployed","job_name":"core","quiet":true`}},
		{name: "nothing ready sends nothing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newOwnedHarness(t, Config{}, &ownedIngress{sendCode: http.StatusAccepted}, "{}")
			if err := h.srv.sendOwnedRolloutProgress(ownedCtx(), pr, "core", tc.ready, tc.done); err != nil {
				t.Fatalf("sendOwnedRolloutProgress() err = %v", err)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}

// TestHasPendingRolloutsRestateOwned pins the recheck gate on the Restate
// rollout index.
func TestHasPendingRolloutsRestateOwned(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		bodies  map[string]string
		noURL   bool
		want    bool
		wantErr bool
	}{
		{name: "pending request keeps rechecking", bodies: map[string]string{"ListRollouts": `{"rollouts":[{"repo":"azylman/aerial-config","pr_number":42}]}`, "GetDeployView": trackedView("aerial-config")}, want: true},
		{name: "empty index stops rechecking", bodies: map[string]string{"ListRollouts": `{"rollouts":[]}`}},
		{name: "no restate url fails", noURL: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ownedIngress{callCode: http.StatusOK, handlerBodies: tc.bodies}
			cfg := Config{}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			got, err := srv.HasPendingRollouts(ownedCtx())
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("HasPendingRollouts() = %v, %v; want %v, err %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

// mainRunPayload renders a main-branch workflow_run on azylman/aerial-config.
func mainRunPayload(action, conclusion string, attempt int) []byte {
	return fmt.Appendf(nil, `{"action":%q,"workflow_run":{"id":7,"run_attempt":%d,"name":"CI","head_sha":%q,"head_branch":"main","status":"completed","conclusion":%q,"html_url":"https://ci/wf","pull_requests":[]},"repository":{"full_name":"azylman/aerial-config"}}`,
		action, attempt, ownedMerge, conclusion)
}

// TestProcessOwnedMainWorkflowRun proves main-branch workflow runs on the
// restate-owned path rerun, fail or promote through Restate with zero
// registry calls and zero synapse effects.
func TestProcessOwnedMainWorkflowRun(t *testing.T) {
	t.Parallel()
	resolved := `{"pr_number":42,"branch":"feat","found":true}`
	tests := []struct {
		name       string
		notOwner   bool
		sendCode   int
		body       []byte
		bodies     map[string]string
		repoRefs   map[string][]fluxKustomizationRef
		wantPaths  []string
		wantBodies []string
		wantErr    bool
	}{
		{
			name: "attempt-1 failure reruns failed jobs in RepoObject", body: mainRunPayload("completed", "failure", 1), bodies: map[string]string{"ResolvePRBySHA": resolved},
			wantPaths:  []string{cfgPRKey + "HandleCheckRerunRequested/send", cfgRepoKey + "HandleWorkflowRerun/send"},
			wantBodies: []string{`"check_name":"CI"`, `"run_id":7`},
		},
		{
			name: "later failure fails the merged pr's deploy", body: mainRunPayload("completed", "failure", 2), bodies: map[string]string{"ResolvePRBySHA": resolved},
			wantPaths:  []string{cfgPRKey + "HandleDeployFailed/send"},
			wantBodies: []string{`"details":"Workflow \"CI\" concluded with failure"`},
		},
		{
			name: "unresolved later failure rides the repo sha route", body: mainRunPayload("completed", "timed_out", 2), bodies: map[string]string{"ResolvePRBySHA": `{"found":false}`},
			wantPaths:  []string{cfgRepoKey + "HandleWorkflowRunBySHA/send"},
			wantBodies: []string{`"handler":"HandleDeployFailed"`},
		},
		{
			name: "success without images completes with git-merge", body: mainRunPayload("completed", "success", 1),
			bodies:     map[string]string{"ResolvePRBySHA": resolved, "GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			wantPaths:  []string{cfgPRKey + "HandleDeploySuccess/send"},
			wantBodies: []string{`"job_name":"git-merge"`},
		},
		{
			name: "success with expected deploy jobs waits for them", body: mainRunPayload("completed", "success", 1),
			bodies: map[string]string{"ResolvePRBySHA": resolved, "GetDeployView": deployViewBody("aerial-config", prMerged, `,"has_expected_deploy_jobs":true`)},
		},
		{name: "in-progress run does nothing", body: mainRunPayload("in_progress", "", 1), bodies: map[string]string{"ResolvePRBySHA": resolved}},
		{name: "non-default owner leaves main runs alone", notOwner: true, body: mainRunPayload("completed", "failure", 1), bodies: map[string]string{"ResolvePRBySHA": resolved}},
		{
			name: "success with a watched repo defers to the watcher", body: mainRunPayload("completed", "success", 1),
			bodies:   map[string]string{"ResolvePRBySHA": resolved, "GetDeployView": deployViewBody("aerial-config", prMerged, "")},
			repoRefs: map[string][]fluxKustomizationRef{"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}}}, wantPaths: []string{cfgPRKey + "HandleKustomizationRequested/send"},
		},
		{
			name: "failed rerun send retries the job", sendCode: http.StatusServiceUnavailable, body: mainRunPayload("completed", "failure", 1), bodies: map[string]string{"ResolvePRBySHA": resolved},
			wantPaths: []string{cfgPRKey + "HandleCheckRerunRequested/send"}, wantErr: true,
		},
		{
			name: "deploy view failure on completion retries the job", body: mainRunPayload("completed", "success", 1),
			bodies: map[string]string{"ResolvePRBySHA": resolved, "GetDeployView": `nope`}, wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.sendCode
			if code == 0 {
				code = http.StatusAccepted
			}
			h := newOwnedHarness(t, Config{DefaultOwner: !tc.notOwner}, &ownedIngress{sendCode: code, callCode: http.StatusOK, callBody: "{}", handlerBodies: tc.bodies}, "[]")
			h.disp.repoKustomizations = tc.repoRefs
			if _, err := h.srv.ProcessGitHubEvent(ownedCtx(), ghEventWorkflowRun, "d1", tc.body); (err != nil) != tc.wantErr {
				t.Fatalf("ProcessGitHubEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}

// TestPromoteOwnedImages pins the image promotion hand-off: RepoObject gets
// the images, deploying workloads, reconcile requests and both fact
// templates; images no Kustomization owns also complete the PR.
func TestPromoteOwnedImages(t *testing.T) {
	t.Parallel()
	images := []string{"ghcr.io/azylman/synapse:latest"}
	refs := map[string][]fluxKustomizationRef{"aerial-synapse": {{Namespace: "flux-system", Name: "aerial-core"}}}
	tests := []struct {
		name       string
		images     []string
		byImage    map[string][]fluxKustomizationRef
		discErr    bool
		wantPaths  []string
		wantBodies []string
		wantErr    bool
	}{
		{
			name: "owned images start the pr rollout", images: images, byImage: refs,
			wantPaths:  []string{"/RepoObject/azylman%2Faerial/HandleImagePromotion/send"},
			wantBodies: []string{`"pr_number":42,"head_sha":"` + ownedMerge + `","branch":"feat","images":["ghcr.io/azylman/synapse:latest"],"jobs":["synapse"],"kustomization_requests":{"aerial-core":`},
		},
		{
			name: "unowned images promote then complete with git-merge", images: images,
			wantPaths:  []string{"/RepoObject/azylman%2Faerial/HandleImagePromotion/send", ownedPRKey + "HandleDeploySuccess/send"},
			wantBodies: []string{`"deploy_failed_facts":[{"event":"deploy_failed"`, `"job_name":"git-merge"`},
		},
		{name: "no deployable workload completes with git-merge", images: []string{"ghcr.io/azylman/none:latest"}, wantPaths: []string{ownedPRKey + "HandleDeploySuccess/send"}},
		{name: "kustomization discovery failure retries", images: images, discErr: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newOwnedHarness(t, Config{}, &ownedIngress{sendCode: http.StatusAccepted, callCode: http.StatusOK, handlerBodies: map[string]string{"GetDeployView": deployViewBody("aerial", prMerged, "")}}, "{}")
			h.disp.kustomizationsByImage = tc.byImage
			if tc.discErr {
				h.disp.discoverErr = fmt.Errorf("kube down")
			}
			err := h.srv.promoteOwnedImages(ownedCtx(), "azylman/aerial", 42, ownedMerge, "feat", tc.images)
			if (err != nil) != tc.wantErr {
				t.Fatalf("promoteOwnedImages() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}

// TestRolloutFromView pins which deploy views the watcher tracks.
func TestRolloutFromView(t *testing.T) {
	t.Parallel()
	reqs := map[string]prwire.KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: ownedReqAt}}
	tests := []struct {
		name   string
		view   prwire.DeployView
		wantOK bool
		want   trackedRollout
	}{
		{
			name: "deploying view with requests is tracked", wantOK: true,
			view: prwire.DeployView{Found: true, Repo: "azylman/aerial", PRNumber: 42, Status: statusDeploying, Branch: "feat", HeadSHA: ownedHead, MergeSHA: ownedMerge, TargetID: ownedThread, KustomizationRequests: reqs, DeployedKustomizations: []string{"web"}},
			want: trackedRollout{PR: DeployingPR{Repo: "azylman/aerial", PRNumber: 42, Branch: "feat", HeadSHA: ownedHead, MergeSHA: ownedMerge, TargetID: ownedThread}, Requests: reqs, Deployed: []string{"web"}},
		},
		{name: "unknown pr is skipped", view: prwire.DeployView{}},
		{name: "deployed view is skipped", view: prwire.DeployView{Found: true, PRNumber: 42, Status: "deployed", KustomizationRequests: reqs}},
		{name: "deploying view without requests is skipped", view: prwire.DeployView{Found: true, PRNumber: 42, Status: statusDeploying}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := rolloutFromView(tc.view)
			if ok != tc.wantOK || fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("rolloutFromView() = %+v, %v; want %+v, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// TestDispatchOwnedWorkflowRunImages pins the image-resolution outcomes of a
// restate-owned main-branch build: a non-image workflow is skipped, a
// rate-limited resolution retries without arming deploy_failed, and a
// failed resolution reports the deploy attempt to PRObject and retries.
func TestDispatchOwnedWorkflowRunImages(t *testing.T) {
	t.Parallel()
	rateLimitServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"rate limited"}`))
	}))
	t.Cleanup(rateLimitServer.Close)

	tests := []struct {
		name       string
		ghBody     string
		ghURL      string
		pr         int
		wantPaths  []string
		wantBodies []string
		wantErr    bool
	}{
		{name: "non-image workflow is skipped", ghBody: `{"id":7,"path":".github/workflows/ci.yml"}`, pr: 42},
		{
			name: "failed resolution reports the attempt and retries", ghBody: `nope`, pr: 42, wantErr: true,
			wantPaths: []string{ownedPRKey + "HandleDeployAttemptFailed/send"}, wantBodies: []string{`"event":"deploy_failed"`},
		},
		{name: "pr-less failed resolution only retries", ghBody: `nope`, wantErr: true},
		{name: "rate-limited resolution retries without reporting deploy attempt failure", ghURL: rateLimitServer.URL, pr: 42, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{GitHubAPIURL: tc.ghURL}
			h := newOwnedHarness(t, cfg, &ownedIngress{sendCode: http.StatusAccepted, callCode: http.StatusOK, callBody: "{}"}, tc.ghBody)
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: tc.pr, HeadSHA: ownedMerge}
			err := h.srv.dispatchOwnedWorkflowRunImages(ownedCtx(), workflowRunEvent{ID: 7, Attempt: 1, HeadSHA: ownedMerge}, res, "feat")
			if (err != nil) != tc.wantErr {
				t.Fatalf("dispatchOwnedWorkflowRunImages() err = %v, wantErr %v", err, tc.wantErr)
			}
			assertOwnedDeploy(t, h, tc.wantPaths, tc.wantBodies, 0)
		})
	}
}
