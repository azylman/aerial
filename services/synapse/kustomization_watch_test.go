package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const sha42Image = "ghcr.io/azylman/aerial-synapse:sha-42"

const (
	reqT0 = "2026-11-08T09:00:00Z"
	reqT1 = "2026-11-08T10:00:00.5Z"
	reqT2 = "2026-11-08T11:00:00Z"
)

func readyCond(status, reason, message string) []fluxCondition {
	return []fluxCondition{{Type: "Reconciling", Status: "False"}, {Type: "Ready", Status: status, Reason: reason, Message: message}}
}

// kobs builds a flux-system Kustomization observation.
func kobs(name, annotation, lastHandled string, suspended bool, conditions []fluxCondition) kustomizationObservation {
	return kustomizationObservation{Ref: fluxKustomizationRef{Namespace: "flux-system", Name: name}, Suspended: suspended, RequestedAt: annotation, LastHandledReconcileAt: lastHandled, Conditions: conditions}
}

// trackedPR builds a deploying PR with kustomization_requests (name -> requested_at, flux-system) and deployed names.
func trackedPR(prNumber int, requests map[string]string, deployed ...string) DeployingPR {
	reqs := map[string]any{}
	for name, at := range requests {
		reqs[name] = map[string]any{"namespace": "flux-system", "requested_at": at}
	}
	done := make([]any, 0, len(deployed))
	for _, name := range deployed {
		done = append(done, name)
	}
	return DeployingPR{
		Repo: "azylman/aerial", PRNumber: prNumber, MergeSHA: fmt.Sprintf("sha-%d", prNumber), TargetID: "1555405874565091380",
		Metadata: map[string]any{kustomizationRequestsKey: reqs, deployedKustomizationsKey: done},
	}
}

func TestKustomizationRequestHandled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		requestedAt string
		lastHandled string
		want        bool
	}{
		{name: "equal timestamps", requestedAt: reqT1, lastHandled: reqT1, want: true},
		{name: "equal instant in different precision", requestedAt: "2026-11-08T10:00:00.500000000Z", lastHandled: reqT1, want: true},
		{name: "later handled request supersedes", requestedAt: reqT1, lastHandled: reqT2, want: true},
		{name: "earlier handled request does not", requestedAt: reqT1, lastHandled: reqT0},
		{name: "empty last handled", requestedAt: reqT1, lastHandled: ""},
		{name: "unparseable equal strings", requestedAt: "token-a", lastHandled: "token-a", want: true},
		{name: "unparseable unequal strings", requestedAt: "token-a", lastHandled: "token-b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := kustomizationRequestHandled(tc.requestedAt, tc.lastHandled); got != tc.want {
				t.Errorf("kustomizationRequestHandled(%q, %q) = %v, want %v", tc.requestedAt, tc.lastHandled, got, tc.want)
			}
		})
	}
}

func TestKustomizationReady(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		conditions []fluxCondition
		wantReady  bool
		wantFailed bool
		wantReason string
	}{
		{name: "Ready True", conditions: readyCond("True", "ReconciliationSucceeded", ""), wantReady: true, wantReason: "ReconciliationSucceeded"},
		{name: "Ready False terminal reason", conditions: readyCond("False", "BuildFailed", "kustomize build failed"), wantFailed: true, wantReason: "BuildFailed: kustomize build failed"},
		{name: "Ready False without reason", conditions: readyCond("False", "", "boom"), wantFailed: true, wantReason: "Ready=False: boom"},
		{name: "Ready Unknown", conditions: readyCond("Unknown", "Progressing", "reconciling"), wantReason: "Progressing: reconciling"},
		{name: "Ready False Progressing", conditions: readyCond("False", "Progressing", ""), wantReason: "Progressing"},
		{name: "Ready False DependencyNotReady", conditions: readyCond("False", "DependencyNotReady", ""), wantReason: "DependencyNotReady"},
		{name: "no Ready condition", conditions: []fluxCondition{{Type: "Reconciling", Status: "True"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ready, failed, reason := kustomizationReady(tc.conditions)
			if ready != tc.wantReady || failed != tc.wantFailed || reason != tc.wantReason {
				t.Errorf("kustomizationReady() = (%v, %v, %q), want (%v, %v, %q)", ready, failed, reason, tc.wantReady, tc.wantFailed, tc.wantReason)
			}
		})
	}
}

func TestKustomizationRequestAction(t *testing.T) {
	t.Parallel()
	recorded := kustomizationRequest{Namespace: "flux-system", RequestedAt: reqT1}
	tests := []struct {
		name string
		obs  kustomizationObservation
		want kustomizationAction
	}{
		{name: "handled and ready marks deployed", obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationMarkDeployed}},
		{name: "handled by a later request and ready marks deployed", obs: kobs("aerial-core", reqT2, reqT2, false, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationMarkDeployed}},
		{name: "handled and failed fails", obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("False", "HealthCheckFailed", "")), want: kustomizationAction{Kind: kustomizationFail, Reason: "HealthCheckFailed"}},
		{name: "handled and progressing waits", obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("Unknown", "Progressing", "")), want: kustomizationAction{Kind: kustomizationWait}},
		{name: "unhandled with earlier annotation repatches", obs: kobs("aerial-core", reqT0, reqT0, false, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationRepatch}},
		{name: "unhandled with missing annotation repatches", obs: kobs("aerial-core", "", "", false, nil), want: kustomizationAction{Kind: kustomizationRepatch}},
		{name: "unhandled with unparseable annotation repatches", obs: kobs("aerial-core", "manual", reqT0, false, nil), want: kustomizationAction{Kind: kustomizationRepatch}},
		{name: "unhandled with equal annotation waits", obs: kobs("aerial-core", reqT1, reqT0, false, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationWait}},
		{name: "unhandled with later annotation waits", obs: kobs("aerial-core", reqT2, reqT0, false, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationWait}},
		{name: "suspended and unhandled fails", obs: kobs("aerial-core", reqT1, reqT0, true, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationFail, Reason: "Kustomization suspended"}},
		{name: "suspended but handled and ready marks deployed", obs: kobs("aerial-core", reqT1, reqT1, true, readyCond("True", "ReconciliationSucceeded", "")), want: kustomizationAction{Kind: kustomizationMarkDeployed}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := kustomizationRequestAction(recorded, tc.obs); got != tc.want {
				t.Errorf("kustomizationRequestAction() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestGitRepoSlug(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "https with .git", url: "https://github.com/acme/widgets.git", want: "acme/widgets"},
		{name: "https trailing slash", url: "https://github.com/acme/widgets/", want: "acme/widgets"},
		{name: "ssh scheme", url: "ssh://git@github.com/acme/widgets", want: "acme/widgets"},
		{name: "scp-like", url: "git@github.com:acme/widgets.git", want: "acme/widgets"},
		{name: "host only", url: "https://github.com"},
		{name: "single path segment", url: "https://github.com/widgets"},
		{name: "empty", url: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := gitRepoSlug(tc.url); got != tc.want {
				t.Errorf("gitRepoSlug(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}

func TestRecordedKustomizationRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		meta string
		want map[string]kustomizationRequest
	}{
		{name: "decodes requests", meta: `{"kustomization_requests":{"marida-core":{"namespace":"apps","requested_at":"t1"}}}`, want: map[string]kustomizationRequest{"marida-core": {Namespace: "apps", RequestedAt: "t1"}}},
		{name: "legacy started_jobs only", meta: `{"started_jobs":["aerial-synapse"]}`},
		{name: "malformed requests list", meta: `{"kustomization_requests":["aerial-core"]}`},
		{name: "malformed request entry", meta: `{"kustomization_requests":{"aerial-core":"t1"}}`},
		{name: "nil metadata"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var meta map[string]any
			if tc.meta != "" {
				if err := json.Unmarshal([]byte(tc.meta), &meta); err != nil {
					t.Fatalf("unmarshal meta: %v", err)
				}
			}
			got := recordedKustomizationRequests(meta)
			if len(got) != len(tc.want) {
				t.Fatalf("recordedKustomizationRequests() = %v, want %v", got, tc.want)
			}
			for name, req := range tc.want {
				if got[name] != req {
					t.Errorf("request %s = %+v, want %+v", name, got[name], req)
				}
			}
		})
	}
}

func (m *mockOutboundDispatcher) ReconcileKustomization(ctx context.Context, ref fluxKustomizationRef, requestedAt string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileCalls = append(m.reconcileCalls, ref)
	m.reconcileRequestedAts = append(m.reconcileRequestedAts, requestedAt)
	return m.reconcileErrs[ref.Name]
}

func (m *mockOutboundDispatcher) ListKustomizationWorkloads(ctx context.Context, ref fluxKustomizationRef) ([]workloadRollout, error) {
	return m.workloads, m.workloadsErr
}

// rolloutOutcome is the observable effect of a watcher apply call on registry and dispatcher fakes.
type rolloutOutcome struct {
	marks       []string
	failed      int
	prompts     int
	promptSub   string
	dispatches  int
	dispatchSub string
	repatchAts  []string
}

func observeRollout(t *testing.T, reg *mockPRRegistry, disp *mockOutboundDispatcher, want rolloutOutcome) {
	t.Helper()
	var marks []string
	for _, c := range reg.markKustomizationsCalls {
		marks = append(marks, c.jobs...)
	}
	if !slices.Equal(marks, want.marks) {
		t.Errorf("marked deployed = %v, want %v", marks, want.marks)
	}
	if len(reg.deployFailedCalls) != want.failed {
		t.Errorf("deploy_failed transitions = %d, want %d", len(reg.deployFailedCalls), want.failed)
	}
	prompts := disp.PromptCalls()
	if len(prompts) != want.prompts {
		t.Errorf("prompts = %d, want %d", len(prompts), want.prompts)
	}
	if want.promptSub != "" && (len(prompts) == 0 || !strings.Contains(prompts[len(prompts)-1].Prompt, want.promptSub)) {
		t.Errorf("prompts = %+v, want substring %q", prompts, want.promptSub)
	}
	dispatches := disp.DispatchCalls()
	if len(dispatches) != want.dispatches {
		t.Errorf("background dispatches = %d, want %d", len(dispatches), want.dispatches)
	}
	if want.dispatchSub != "" && (len(dispatches) == 0 || !strings.Contains(dispatches[len(dispatches)-1].Prompt, want.dispatchSub)) {
		t.Errorf("background dispatches = %+v, want substring %q", dispatches, want.dispatchSub)
	}
	if !slices.Equal(disp.reconcileRequestedAts, want.repatchAts) {
		t.Errorf("re-patched requestedAt = %v, want %v", disp.reconcileRequestedAts, want.repatchAts)
	}
}

func TestApplyKustomizationStatus(t *testing.T) {
	t.Parallel()
	ready := readyCond("True", "ReconciliationSucceeded", "")
	twoTargets := map[string]string{"aerial-core": reqT1, "marida-core": reqT1}

	tests := []struct {
		name         string
		prs          []DeployingPR
		listErr      error
		markRes      bool
		markErr      error
		dispatchErr  error
		patchErr     error
		workloads    []workloadRollout
		workloadsErr error
		obs          kustomizationObservation
		want         rolloutOutcome
		wantErr      bool
	}{
		{name: "two-target PR stays deploying after the first kustomization", prs: []DeployingPR{trackedPR(42, twoTargets)}, obs: kobs("aerial-core", reqT1, reqT1, false, ready), want: rolloutOutcome{marks: []string{"aerial-core"}}},
		{name: "two-target PR dispatches exactly one wake on the second kustomization", prs: []DeployingPR{trackedPR(42, twoTargets, "aerial-core")}, markRes: true, obs: kobs("marida-core", reqT1, reqT1, false, ready), want: rolloutOutcome{marks: []string{"marida-core"}, prompts: 1, promptSub: "deployment completed for job marida-core on azylman/aerial (PR #42, commit: sha-42)"}},
		{name: "already deployed kustomization is not re-marked", prs: []DeployingPR{trackedPR(42, twoTargets, "aerial-core")}, obs: kobs("aerial-core", reqT1, reqT1, false, ready)},
		{name: "superseded request completes on the later handled request", prs: []DeployingPR{trackedPR(41, map[string]string{"aerial-core": reqT1})}, markRes: true, obs: kobs("aerial-core", reqT2, reqT2, false, ready), want: rolloutOutcome{marks: []string{"aerial-core"}, prompts: 1}},
		{name: "Ready=False after handling fails the PR", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("False", "ReconciliationFailed", "apply failed")), want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "ReconciliationFailed: apply failed"}},
		{name: "progressing kustomization waits", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("Unknown", "Progressing", ""))},
		{name: "lost requests issue one PATCH carrying the latest recorded value", prs: []DeployingPR{trackedPR(41, map[string]string{"aerial-core": reqT1}), trackedPR(42, map[string]string{"aerial-core": reqT2}), trackedPR(43, map[string]string{"aerial-core": reqT1})}, obs: kobs("aerial-core", reqT0, reqT0, false, ready), want: rolloutOutcome{repatchAts: []string{reqT2}}},
		{name: "pending request waits without re-patching", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, obs: kobs("aerial-core", reqT1, reqT0, false, ready)},
		{name: "suspended kustomization fails the PR", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, obs: kobs("aerial-core", reqT1, reqT0, true, ready), want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Kustomization suspended"}},
		{name: "legacy PR without requests is untouched", prs: []DeployingPR{{Repo: "azylman/aerial", PRNumber: 40, Metadata: map[string]any{"jobs": []any{"aerial-core"}}}}, obs: kobs("aerial-core", reqT0, reqT0, false, ready)},
		{name: "request in another namespace is ignored", prs: []DeployingPR{{Repo: "azylman/aerial", PRNumber: 44, Metadata: map[string]any{kustomizationRequestsKey: map[string]any{"aerial-core": map[string]any{"namespace": "apps", "requested_at": reqT1}}}}}, obs: kobs("aerial-core", reqT0, reqT0, false, ready)},
		{name: "deploying list error is returned", listErr: errors.New("db down"), obs: kobs("aerial-core", reqT1, reqT1, false, ready), wantErr: true},
		{name: "registry mark failure is returned so the watcher re-lists", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, markErr: errors.New("db down"), obs: kobs("aerial-core", reqT1, reqT1, false, ready), want: rolloutOutcome{marks: []string{"aerial-core"}}, wantErr: true},
		{name: "failure alert dispatch error is returned so the watcher re-lists", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, dispatchErr: errors.New("brain down"), obs: kobs("aerial-core", reqT1, reqT0, true, ready), want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Kustomization suspended"}, wantErr: true},
		{name: "ready kustomization waits while a merge-SHA deployment rolls out", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, workloads: []workloadRollout{deployRollout("synapse", sha42Image, 3, 3, 1, 1, 0, 2, false)}, obs: kobs("aerial-core", reqT1, reqT1, false, ready)},
		{name: "ready kustomization deploys once its merge-SHA deployment rolled out", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, markRes: true, workloads: []workloadRollout{deployRollout("synapse", sha42Image, 3, 3, 1, 1, 1, 1, false)}, obs: kobs("aerial-core", reqT1, reqT1, false, ready), want: rolloutOutcome{marks: []string{"aerial-core"}, prompts: 1}},
		{name: "merge-SHA deployment past its progress deadline fails the PR", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, workloads: []workloadRollout{deployRollout("synapse", sha42Image, 3, 3, 1, 1, 0, 2, true)}, obs: kobs("aerial-core", reqT1, reqT1, false, ready), want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Deployment default/synapse: progress deadline exceeded"}},
		{name: "unhealthy workload on another image does not block the deploy", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, markRes: true, workloads: []workloadRollout{deployRollout("haro", "ghcr.io/azylman/aerial-haro:sha-7", 3, 3, 1, 0, 0, 2, true)}, obs: kobs("aerial-core", reqT1, reqT1, false, ready), want: rolloutOutcome{marks: []string{"aerial-core"}, prompts: 1}},
		{name: "each PR is gated on its own merge-SHA workloads", prs: []DeployingPR{trackedPR(41, map[string]string{"aerial-core": reqT1}), trackedPR(42, map[string]string{"aerial-core": reqT1})}, markRes: true, workloads: []workloadRollout{deployRollout("synapse", sha42Image, 3, 3, 1, 1, 0, 2, false)}, obs: kobs("aerial-core", reqT1, reqT1, false, ready), want: rolloutOutcome{marks: []string{"aerial-core"}, prompts: 1, promptSub: "PR #41"}},
		{name: "workload list failure is returned and leaves every PR deploying", prs: []DeployingPR{trackedPR(41, map[string]string{"aerial-core": reqT1}), trackedPR(42, map[string]string{"aerial-core": reqT1})}, workloadsErr: errors.New("forbidden"), obs: kobs("aerial-core", reqT1, reqT1, false, ready), wantErr: true},
		{name: "re-patch failure is returned so the watcher re-lists", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, patchErr: errors.New("forbidden"), obs: kobs("aerial-core", reqT0, reqT0, false, ready), want: rolloutOutcome{repatchAts: []string{reqT1}}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{listDeployingPRs: tc.prs, listDeployingErr: tc.listErr, markKustomizationsRes: tc.markRes, markKustomizationsErr: tc.markErr, deployFailedUpdated: true}
			disp := &mockOutboundDispatcher{dispatchErr: tc.dispatchErr, reconcileErrs: map[string]error{"aerial-core": tc.patchErr}, workloads: tc.workloads, workloadsErr: tc.workloadsErr}
			srv := newTestRouter(Config{}, nil, reg, disp)
			if err := srv.applyKustomizationStatus(context.Background(), tc.obs); (err != nil) != tc.wantErr {
				t.Fatalf("applyKustomizationStatus() err = %v, wantErr %v", err, tc.wantErr)
			}
			observeRollout(t, reg, disp, tc.want)
		})
	}
}

func TestApplyKustomizationList(t *testing.T) {
	t.Parallel()
	ready := readyCond("True", "ReconciliationSucceeded", "")
	twoTargets := map[string]string{"aerial-core": reqT1, "marida-core": reqT1}

	tests := []struct {
		name         string
		prs          []DeployingPR
		listErr      error
		dispatchErr  error
		observations []kustomizationObservation
		want         rolloutOutcome
		wantErr      bool
	}{
		{name: "recorded kustomization absent from the list fails the PR", prs: []DeployingPR{trackedPR(42, twoTargets)}, observations: []kustomizationObservation{kobs("aerial-core", reqT1, reqT0, false, ready)}, want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Kustomization deleted"}},
		{name: "every listed item is applied", prs: []DeployingPR{trackedPR(42, twoTargets)}, observations: []kustomizationObservation{kobs("aerial-core", reqT1, reqT1, false, ready), kobs("marida-core", reqT0, reqT0, false, ready)}, want: rolloutOutcome{marks: []string{"aerial-core"}, repatchAts: []string{reqT1}}},
		{name: "absent but already deployed kustomization is ignored", prs: []DeployingPR{trackedPR(42, twoTargets, "marida-core")}, observations: []kustomizationObservation{kobs("aerial-core", reqT1, reqT0, false, ready)}},
		{name: "absent kustomization of another namespace is ignored", prs: []DeployingPR{{Repo: "azylman/aerial", PRNumber: 44, Metadata: map[string]any{kustomizationRequestsKey: map[string]any{"stack": map[string]any{"namespace": "apps", "requested_at": reqT1}}}}}},
		{name: "deploying list error is returned", listErr: errors.New("db down"), wantErr: true},
		{name: "absent kustomization failure dispatch error is returned", prs: []DeployingPR{trackedPR(42, map[string]string{"marida-core": reqT1})}, dispatchErr: errors.New("brain down"), want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Kustomization deleted"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{listDeployingPRs: tc.prs, listDeployingErr: tc.listErr, deployFailedUpdated: true}
			disp := &mockOutboundDispatcher{dispatchErr: tc.dispatchErr}
			srv := newTestRouter(Config{}, nil, reg, disp)
			if err := srv.applyKustomizationList(context.Background(), "flux-system", tc.observations); (err != nil) != tc.wantErr {
				t.Fatalf("applyKustomizationList() err = %v, wantErr %v", err, tc.wantErr)
			}
			observeRollout(t, reg, disp, tc.want)
		})
	}
}

func TestApplyKustomizationDeleted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		prs         []DeployingPR
		listErr     error
		dispatchErr error
		want        rolloutOutcome
		wantErr     bool
	}{
		{name: "pending request fails the PR", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Kustomization deleted"}},
		{name: "already deployed kustomization is ignored", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1, "marida-core": reqT1}, "aerial-core")}},
		{name: "PR waiting on other kustomizations is ignored", prs: []DeployingPR{trackedPR(42, map[string]string{"marida-core": reqT1})}},
		{name: "deploying list error is returned", listErr: errors.New("db down"), wantErr: true},
		{name: "failure alert dispatch error is returned so the watcher re-lists", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1})}, dispatchErr: errors.New("brain down"), want: rolloutOutcome{failed: 1, dispatches: 1, dispatchSub: "Kustomization deleted"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{listDeployingPRs: tc.prs, listDeployingErr: tc.listErr, deployFailedUpdated: true}
			disp := &mockOutboundDispatcher{dispatchErr: tc.dispatchErr}
			srv := newTestRouter(Config{}, nil, reg, disp)
			if err := srv.applyKustomizationDeleted(context.Background(), fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"}); (err != nil) != tc.wantErr {
				t.Fatalf("applyKustomizationDeleted() err = %v, wantErr %v", err, tc.wantErr)
			}
			observeRollout(t, reg, disp, tc.want)
		})
	}
}

// watchFixture is a kube API serving queued LIST and WATCH responses in order. Once a queue is
// exhausted it cancels the watcher context, ending the test run.
type watchFixture struct {
	mu       sync.Mutex
	lists    []kubeRoute
	watches  []kubeRoute
	requests []string
	cancel   context.CancelFunc
}

func (f *watchFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	query := r.URL.Query()
	queue := &f.lists
	label := "LIST " + path.Base(r.URL.Path)
	if sel := query.Get("labelSelector"); sel != "" {
		label += " sel=" + sel
	}
	if query.Get("watch") == "1" {
		queue, label = &f.watches, strings.Replace(label, "LIST", "WATCH", 1)+" rv="+query.Get("resourceVersion")
	}
	f.requests = append(f.requests, label)
	if len(*queue) == 0 {
		f.mu.Unlock()
		f.cancel()
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	route := (*queue)[0]
	*queue = (*queue)[1:]
	f.mu.Unlock()
	w.WriteHeader(max(route.status, http.StatusOK))
	for _, line := range strings.SplitAfter(route.body, "\n") {
		_, _ = w.Write([]byte(line))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

// recordingSink records every watcher callback.
type recordingSink struct {
	mu         sync.Mutex
	events     []string
	failStatus bool
	pending    bool
}

func (r *recordingSink) record(evt string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
}

func (r *recordingSink) applyKustomizationList(_ context.Context, namespace string, observations []kustomizationObservation) error {
	names := make([]string, 0, len(observations))
	for _, obs := range observations {
		names = append(names, obs.Ref.Name)
	}
	r.record(fmt.Sprintf("list %s %v", namespace, names))
	return nil
}

func (r *recordingSink) applyKustomizationStatus(_ context.Context, obs kustomizationObservation) error {
	r.record(fmt.Sprintf("status %s/%s requested=%s handled=%s", obs.Ref.Namespace, obs.Ref.Name, obs.RequestedAt, obs.LastHandledReconcileAt))
	if r.failStatus {
		return errors.New("registry down")
	}
	return nil
}

func (r *recordingSink) applyKustomizationDeleted(_ context.Context, ref fluxKustomizationRef) error {
	r.record("deleted " + ref.Namespace + "/" + ref.Name)
	return nil
}

func (r *recordingSink) hasPendingRollouts(context.Context) (bool, error) {
	r.record(fmt.Sprintf("pending? %v", r.pending))
	return r.pending, nil
}

func TestKustomizationWatcherRun(t *testing.T) {
	t.Parallel()
	list := func(rv string, names ...string) kubeRoute {
		items := make([]string, 0, len(names))
		for _, n := range names {
			items = append(items, `{"metadata":{"name":"`+n+`","namespace":"flux-system"}}`)
		}
		return kubeRoute{body: `{"metadata":{"resourceVersion":"` + rv + `"},"items":[` + strings.Join(items, ",") + `]}`}
	}
	event := func(typ, name, annotation, handled string) string {
		return `{"type":"` + typ + `","object":{"metadata":{"name":"` + name + `","annotations":{"reconcile.fluxcd.io/requestedAt":"` + annotation + `"}},"status":{"lastHandledReconcileAt":"` + handled + `"}}}` + "\n"
	}
	workloads := func(rv string) kubeRoute {
		return kubeRoute{body: `{"metadata":{"resourceVersion":"` + rv + `"},"items":[{"metadata":{"name":"synapse","namespace":"default"}}]}`}
	}
	workloadEvent := func(typ string) string {
		return `{"type":"` + typ + `","object":{"metadata":{"name":"synapse","namespace":"default"}}}` + "\n"
	}
	const (
		deploymentsList  = "LIST deployments sel=kustomize.toolkit.fluxcd.io/name"
		deploymentsWatch = "WATCH deployments sel=kustomize.toolkit.fluxcd.io/name rv="
	)
	deployments := workloadSource{path: "/apis/apps/v1/deployments"}
	gone := `{"type":"ERROR","object":{"kind":"Status","code":410,"message":"too old resource version"}}` + "\n"

	tests := []struct {
		name         string
		lists        []kubeRoute
		watches      []kubeRoute
		source       kubeWatchSource
		failStatus   bool
		pending      bool
		wantRequests []string
		wantEvents   []string
	}{
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "list then chunked watch stream applies events and re-lists when the stream ends",
			lists:        []kubeRoute{list("10", "aerial-core"), list("20")},
			watches:      []kubeRoute{{body: event("ADDED", "marida-core", reqT1, "") + `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"15"}}}` + "\n" + event("MODIFIED", "aerial-core", reqT1, reqT1) + event("DELETED", "marida-core", reqT1, "")}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=20"},
			wantEvents: []string{
				"list flux-system [aerial-core]",
				"status flux-system/marida-core requested=" + reqT1 + " handled=",
				"status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT1,
				"deleted flux-system/marida-core",
				"list flux-system []",
			},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "410 Gone error event re-lists",
			lists:        []kubeRoute{list("10", "aerial-core"), list("30", "aerial-core")},
			watches:      []kubeRoute{{body: event("MODIFIED", "aerial-core", reqT1, reqT0) + gone}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=30"},
			wantEvents:   []string{"list flux-system [aerial-core]", "status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT0, "list flux-system [aerial-core]"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "410 Gone watch response re-lists",
			lists:        []kubeRoute{list("10"), list("40")},
			watches:      []kubeRoute{{status: http.StatusGone, body: `{"kind":"Status","code":410}`}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=40"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "list failure backs off and retries",
			lists:        []kubeRoute{{status: http.StatusInternalServerError, body: `boom`}, list("50")},
			wantRequests: []string{"LIST kustomizations", "LIST kustomizations", "WATCH kustomizations rv=50"},
			wantEvents:   []string{"list flux-system []"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "watch rejection backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("60")},
			watches:      []kubeRoute{{status: http.StatusForbidden, body: `forbidden`}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=60"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "malformed watch event backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("70")},
			watches:      []kubeRoute{{body: "{bad\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=70"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "non-410 error event backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("80")},
			watches:      []kubeRoute{{body: `{"type":"ERROR","object":{"kind":"Status","code":500}}` + "\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=80"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "malformed watched object backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("85")},
			watches:      []kubeRoute{{body: `{"type":"MODIFIED","object":[]}` + "\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=85"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       kustomizationSource{namespace: "flux-system"},
			name:         "sink failure on an event re-lists to re-derive state",
			lists:        []kubeRoute{list("10"), list("90")},
			watches:      []kubeRoute{{body: event("MODIFIED", "aerial-core", reqT1, reqT1)}},
			failStatus:   true,
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=90"},
			wantEvents:   []string{"list flux-system []", "status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT1, "list flux-system []"},
		},
		{
			name:         "workload list and changes re-list kustomizations while a rollout is pending",
			source:       deployments,
			pending:      true,
			lists:        []kubeRoute{workloads("5"), list("6", "aerial-core"), list("7", "aerial-core"), list("8", "aerial-core"), workloads("9"), list("10")},
			watches:      []kubeRoute{{body: workloadEvent("ADDED") + workloadEvent("MODIFIED") + workloadEvent("DELETED") + `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"6"}}}` + "\n"}},
			wantRequests: []string{deploymentsList, "LIST kustomizations", deploymentsWatch + "5", "LIST kustomizations", "LIST kustomizations", deploymentsList, "LIST kustomizations", deploymentsWatch + "9"},
			wantEvents:   []string{"pending? true", "list flux-system [aerial-core]", "pending? true", "list flux-system [aerial-core]", "pending? true", "list flux-system [aerial-core]", "pending? true", "list flux-system []"},
		},
		{
			name:         "workload changes skip the kustomization re-list without pending rollouts",
			source:       deployments,
			lists:        []kubeRoute{workloads("5"), workloads("9")},
			watches:      []kubeRoute{{body: workloadEvent("MODIFIED")}},
			wantRequests: []string{deploymentsList, deploymentsWatch + "5", deploymentsList, deploymentsWatch + "9"},
			wantEvents:   []string{"pending? false", "pending? false", "pending? false"},
		},
		{
			name:         "kustomization re-list failure backs off and re-lists workloads",
			source:       deployments,
			pending:      true,
			lists:        []kubeRoute{workloads("5"), {status: http.StatusInternalServerError, body: `boom`}, workloads("9"), list("10")},
			wantRequests: []string{deploymentsList, "LIST kustomizations", deploymentsList, "LIST kustomizations", deploymentsWatch + "9"},
			wantEvents:   []string{"pending? true", "pending? true", "list flux-system []"},
		},
		{
			name:         "workload list failure backs off and retries",
			source:       deployments,
			lists:        []kubeRoute{{status: http.StatusForbidden, body: `forbidden`}, {body: `{"metadata":`}, workloads("9")},
			wantRequests: []string{deploymentsList, deploymentsList, deploymentsList, deploymentsWatch + "9"},
			wantEvents:   []string{"pending? false"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture := &watchFixture{lists: tc.lists, watches: tc.watches, cancel: cancel}
			ts := httptest.NewServer(fixture)
			defer ts.Close()
			disp := NewDefaultOutboundDispatcher("", ts.Client())
			disp.SetKubeConfig(ts.URL, "test-token")
			sink := &recordingSink{failStatus: tc.failStatus, pending: tc.pending}

			newKubeWatcher(disp, sink, tc.source, time.Millisecond).Run(ctx)

			if got := fixture.requests; !slices.Equal(got, tc.wantRequests) {
				t.Errorf("requests = %v, want %v", got, tc.wantRequests)
			}
			if !slices.Equal(sink.events, tc.wantEvents) {
				t.Errorf("events = %q, want %q", sink.events, tc.wantEvents)
			}
		})
	}
}

func TestStartKustomizationWatcher(t *testing.T) {
	t.Parallel()
	allSources := []string{"/apis/apps/v1/daemonsets", "/apis/apps/v1/deployments", "/apis/apps/v1/statefulsets", "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations"}
	tests := []struct {
		name         string
		withRegistry bool
		withKube     bool
		wantListed   []string
	}{
		{name: "no registry does not watch", withKube: true},
		{name: "no kube API does not watch", withRegistry: true},
		{name: "registry and kube API watch kustomizations and every rollout workload kind until stopped", withRegistry: true, withKube: true, wantListed: allSources},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			listed := make(chan string, 16)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("watch") == "1" {
					<-r.Context().Done()
					return
				}
				listed <- r.URL.Path
				_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"1"},"items":[]}`))
			}))
			defer ts.Close()
			disp := NewDefaultOutboundDispatcher("", ts.Client())
			if tc.withKube {
				disp.SetKubeConfig(ts.URL, "test-token")
			}
			var reg PRRegistryUpdater
			if tc.withRegistry {
				reg = &mockPRRegistry{}
			}
			srv := newTestRouter(Config{}, nil, reg, disp)
			stop := srv.startKustomizationWatcher(context.Background(), disp)
			got := make([]string, 0, len(tc.wantListed))
			for range tc.wantListed {
				got = append(got, <-listed)
			}
			stop()
			slices.Sort(got)
			if !slices.Equal(got, tc.wantListed) {
				t.Errorf("listed = %v, want %v", got, tc.wantListed)
			}
			if n := len(listed); n != 0 {
				t.Errorf("unexpected extra lists: %d", n)
			}
		})
	}
}

func TestHasPendingRollouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		prs     []DeployingPR
		listErr error
		want    bool
		wantErr bool
	}{
		{name: "PR with an undeployed request is pending", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1, "marida-core": reqT1}, "aerial-core")}, want: true},
		{name: "PR with every request deployed is not pending", prs: []DeployingPR{trackedPR(42, map[string]string{"aerial-core": reqT1}, "aerial-core")}},
		{name: "legacy PR without requests is not pending", prs: []DeployingPR{{Repo: "azylman/aerial", PRNumber: 40, Metadata: map[string]any{"jobs": []any{"aerial-core"}}}}},
		{name: "no deploying PRs", want: false},
		{name: "deploying list error is returned", listErr: errors.New("db down"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{listDeployingPRs: tc.prs, listDeployingErr: tc.listErr}
			srv := newTestRouter(Config{}, nil, reg, &mockOutboundDispatcher{})
			got, err := srv.hasPendingRollouts(context.Background())
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("hasPendingRollouts() = (%v, %v), want (%v, wantErr %v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_ListKustomizationWorkloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deploymentList := `{"items":[{"metadata":{"name":"synapse","namespace":"default","generation":2},"spec":{"replicas":1,"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}},"status":{"observedGeneration":2,"replicas":1,"updatedReplicas":1,"availableReplicas":1}}]}`
	daemonSetList := `{"items":[{"metadata":{"name":"agent","namespace":"kube-system","generation":1},"spec":{"template":{"spec":{"containers":[{"image":"agent:abc"}]}}},"status":{"observedGeneration":1,"desiredNumberScheduled":2,"updatedNumberScheduled":1,"numberAvailable":2}}]}`

	tests := []struct {
		name         string
		noKube       bool
		routes       map[string]kubeRoute
		want         []string
		wantErr      string
		wantRequests int
	}{
		{
			name:         "lists deployments statefulsets and daemonsets of the kustomization",
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeDaemonSetsPath: {body: daemonSetList}}),
			want:         []string{"Deployment default/synapse", "DaemonSet kube-system/agent"},
			wantRequests: 3,
		},
		{
			name:         "list failure returns error",
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeStatefulSetsPath: {status: http.StatusForbidden, body: `forbidden`}}),
			wantErr:      "list aerial-core workloads",
			wantRequests: 2,
		},
		{
			name:         "malformed list returns error",
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: `{"items":`}}),
			wantErr:      "decode /apis/apps/v1/deployments",
			wantRequests: 1,
		},
		{name: "unconfigured kube API returns error", noKube: true, wantErr: "kube api server not configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, ts := newKubeFixture(t, tc.routes)
			disp := NewDefaultOutboundDispatcher("", ts.Client())
			if !tc.noKube {
				disp.SetKubeConfig(ts.URL, "test-token")
			}
			workloads, err := disp.ListKustomizationWorkloads(ctx, fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			got := make([]string, 0, len(workloads))
			for _, w := range workloads {
				got = append(got, w.String())
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("workloads = %v, want %v", got, tc.want)
			}
			if n := len(fixture.Requests()); n != tc.wantRequests {
				t.Errorf("kube requests = %d (%v), want %d", n, fixture.Requests(), tc.wantRequests)
			}
			if tc.wantRequests > 0 {
				if q := fixture.Query(kubeDeploymentsPath); q != "labelSelector=kustomize.toolkit.fluxcd.io%2Fname%3Daerial-core%2Ckustomize.toolkit.fluxcd.io%2Fnamespace%3Dflux-system" {
					t.Errorf("deployments query = %q", q)
				}
			}
		})
	}
}

func TestHandleFluxWebhookSourceRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		payload    string
		noDisp     bool
		sourceRepo string
		sourceErr  error
		wantRepo   string
	}{
		{name: "alert without repo resolves from its flux source", payload: `{"involvedObject":{"kind":"Kustomization","name":"stack-core","namespace":"flux-system"},"reason":"Progressing","metadata":{"revision":"main@sha1:abc"}}`, sourceRepo: "acme/stack", wantRepo: "acme/stack"},
		{name: "payload repo wins over source lookup", payload: `{"involvedObject":{"kind":"Kustomization","name":"stack-core"},"reason":"Progressing","metadata":{"repo":"acme/payload","revision":"main@sha1:abc"}}`, sourceRepo: "acme/stack", wantRepo: "acme/payload"},
		{name: "source lookup failure falls back to the default repo", payload: `{"involvedObject":{"kind":"GitRepository","name":"stack"},"reason":"NewArtifact","metadata":{"revision":"main@sha1:abc"}}`, sourceErr: errors.New("forbidden"), wantRepo: "azylman/aerial"},
		{name: "no dispatcher falls back to the default repo", payload: `{"involvedObject":{"kind":"GitRepository","name":"stack"},"reason":"NewArtifact","metadata":{"revision":"main@sha1:abc"}}`, noDisp: true, wantRepo: "azylman/aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var disp OutboundDispatcher
			if !tc.noDisp {
				disp = &mockOutboundDispatcher{sourceRepo: tc.sourceRepo, sourceRepoErr: tc.sourceErr}
			}
			srv := newTestRouter(Config{}, nil, nil, disp)
			river := &mockRiverInserter{}
			srv.SetRiverClient(river)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/flux", strings.NewReader(tc.payload))
			w := httptest.NewRecorder()
			srv.Routes().ServeHTTP(w, req)
			if w.Code != http.StatusAccepted || len(river.insertedDeployJobs) != 1 {
				t.Fatalf("status = %d, inserted = %d; want 202 and one deploy job (body %s)", w.Code, len(river.insertedDeployJobs), w.Body.String())
			}
			if got := river.insertedDeployJobs[0].Event.Repo; got != tc.wantRepo {
				t.Errorf("repo = %q, want %q", got, tc.wantRepo)
			}
		})
	}
}
