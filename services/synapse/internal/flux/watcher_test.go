package flux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	reqT0 = "2026-11-08T09:00:00Z"
	reqT1 = "2026-11-08T10:00:00.5Z"
	reqT2 = "2026-11-08T11:00:00Z"
)

func readyCond(status, reason, message string) []model.FluxCondition {
	return []model.FluxCondition{{Type: "Reconciling", Status: "False"}, {Type: "Ready", Status: status, Reason: reason, Message: message}}
}

func kobs(name, annotation, lastHandled string, suspended bool, conditions []model.FluxCondition) model.KustomizationObservation {
	return model.KustomizationObservation{Ref: model.FluxKustomizationRef{Namespace: "flux-system", Name: name}, Suspended: suspended, RequestedAt: annotation, LastHandledReconcileAt: lastHandled, Conditions: conditions}
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
			if got := KustomizationRequestHandled(tc.requestedAt, tc.lastHandled); got != tc.want {
				t.Errorf("KustomizationRequestHandled(%q, %q) = %v, want %v", tc.requestedAt, tc.lastHandled, got, tc.want)
			}
		})
	}
}

func TestKustomizationReady(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		conditions []model.FluxCondition
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
		{name: "no Ready condition", conditions: []model.FluxCondition{{Type: "Reconciling", Status: "True"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ready, failed, reason := KustomizationReady(tc.conditions)
			if ready != tc.wantReady || failed != tc.wantFailed || reason != tc.wantReason {
				t.Errorf("KustomizationReady() = (%v, %v, %q), want (%v, %v, %q)", ready, failed, reason, tc.wantReady, tc.wantFailed, tc.wantReason)
			}
		})
	}
}

func TestKustomizationRequestAction(t *testing.T) {
	t.Parallel()
	recorded := prwire.KustomizationRequest{Namespace: "flux-system", RequestedAt: reqT1}
	tests := []struct {
		name string
		obs  model.KustomizationObservation
		want KustomizationAction
	}{
		{name: "handled and ready marks deployed", obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationMarkDeployed}},
		{name: "handled by a later request and ready marks deployed", obs: kobs("aerial-core", reqT2, reqT2, false, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationMarkDeployed}},
		{name: "handled and failed fails", obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("False", "HealthCheckFailed", "")), want: KustomizationAction{Kind: KustomizationFail, Reason: "HealthCheckFailed"}},
		{name: "handled and progressing waits", obs: kobs("aerial-core", reqT1, reqT1, false, readyCond("Unknown", "Progressing", "")), want: KustomizationAction{Kind: KustomizationWait}},
		{name: "unhandled with earlier annotation repatches", obs: kobs("aerial-core", reqT0, reqT0, false, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationRepatch}},
		{name: "unhandled with missing annotation repatches", obs: kobs("aerial-core", "", "", false, nil), want: KustomizationAction{Kind: KustomizationRepatch}},
		{name: "unhandled with unparseable annotation repatches", obs: kobs("aerial-core", "manual", reqT0, false, nil), want: KustomizationAction{Kind: KustomizationRepatch}},
		{name: "unhandled with equal annotation waits", obs: kobs("aerial-core", reqT1, reqT0, false, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationWait}},
		{name: "unhandled with later annotation waits", obs: kobs("aerial-core", reqT2, reqT0, false, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationWait}},
		{name: "suspended and unhandled fails", obs: kobs("aerial-core", reqT1, reqT0, true, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationFail, Reason: "Kustomization suspended"}},
		{name: "suspended but handled and ready marks deployed", obs: kobs("aerial-core", reqT1, reqT1, true, readyCond("True", "ReconciliationSucceeded", "")), want: KustomizationAction{Kind: KustomizationMarkDeployed}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := KustomizationRequestAction(recorded, tc.obs); got != tc.want {
				t.Errorf("KustomizationRequestAction() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestIsDeploySuccessPending(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		meta map[string]any
		want bool
	}{
		{name: "nil metadata", meta: nil, want: false},
		{name: "missing key", meta: map[string]any{"other": 1}, want: false},
		{name: "bool true", meta: map[string]any{"deploy_success_pending": true}, want: true},
		{name: "bool false", meta: map[string]any{"deploy_success_pending": false}, want: false},
		{name: "string true", meta: map[string]any{"deploy_success_pending": "true"}, want: true},
		{name: "string false", meta: map[string]any{"deploy_success_pending": "false"}, want: false},
		{name: "other type", meta: map[string]any{"deploy_success_pending": 123}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsDeploySuccessPending(tc.meta); got != tc.want {
				t.Errorf("IsDeploySuccessPending() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewKustomizationRequests(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	refs := []model.FluxKustomizationRef{
		{Namespace: "flux-system", Name: "aerial-core"},
		{Namespace: "flux-system", Name: "agent-gamma-core"},
	}
	reqs := NewKustomizationRequests(refs, now)
	if len(reqs) != 2 {
		t.Fatalf("NewKustomizationRequests() returned %d items, want 2", len(reqs))
	}
	wantAt := now.Format(time.RFC3339Nano)
	if reqs["aerial-core"].RequestedAt != wantAt || reqs["aerial-core"].Namespace != "flux-system" {
		t.Errorf("aerial-core request = %+v, want requestedAt %s", reqs["aerial-core"], wantAt)
	}
}

func TestTrackedRollout(t *testing.T) {
	t.Parallel()
	ref := model.FluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"}
	recorded := map[string]prwire.KustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: reqT1}}
	pr := model.DeployingPR{Repo: "example/aerial", PRNumber: 42}
	successPending := model.DeployingPR{Repo: "example/aerial", PRNumber: 42, Metadata: map[string]any{"deploy_success_pending": true}}
	tests := []struct {
		name           string
		tr             TrackedRollout
		ref            model.FluxKustomizationRef
		wantRequest    string
		wantHasPending bool
	}{
		{name: "recorded request pending", tr: TrackedRollout{PR: pr, Requests: recorded}, ref: ref, wantRequest: reqT1, wantHasPending: true},
		{name: "deployed request settled", tr: TrackedRollout{PR: pr, Requests: recorded, Deployed: []string{"aerial-core"}}, ref: ref},
		{name: "deploy success pending keeps deployed request pending", tr: TrackedRollout{PR: successPending, Requests: recorded, Deployed: []string{"aerial-core"}}, ref: ref, wantRequest: reqT1, wantHasPending: true},
		{name: "wrong namespace", tr: TrackedRollout{PR: pr, Requests: recorded}, ref: model.FluxKustomizationRef{Namespace: "other", Name: "aerial-core"}, wantHasPending: true},
		{name: "empty requestedAt", tr: TrackedRollout{Requests: map[string]prwire.KustomizationRequest{"aerial-core": {Namespace: "flux-system"}}}, ref: ref},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, ok := tc.tr.PendingRequest(tc.ref)
			if req.RequestedAt != tc.wantRequest || ok != (tc.wantRequest != "") {
				t.Errorf("PendingRequest() = (%+v, %v), want requestedAt %q", req, ok, tc.wantRequest)
			}
			if got := tc.tr.HasPending(); got != tc.wantHasPending {
				t.Errorf("HasPending() = %v, want %v", got, tc.wantHasPending)
			}
		})
	}
}

func TestGitHistoryCache(t *testing.T) {
	t.Parallel()
	cache := &GitHistoryCache{}

	if _, ok := cache.CommitTime("c1"); ok {
		t.Errorf("expected miss on CommitTime")
	}
	now := time.Now().UTC()
	cache.StoreCommitTime("c1", now)
	if at, ok := cache.CommitTime("c1"); !ok || !at.Equal(now) {
		t.Errorf("expected hit on CommitTime = (%v, %v)", at, ok)
	}

	if _, ok := cache.IncludesVerdict("v1"); ok {
		t.Errorf("expected miss on IncludesVerdict")
	}
	cache.StoreIncludesVerdict("v1", true)
	if v, ok := cache.IncludesVerdict("v1"); !ok || !v {
		t.Errorf("expected hit on IncludesVerdict = (%v, %v)", v, ok)
	}
}

type kubeRoute struct {
	status int
	body   string
}

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

type recordingSink struct {
	mu         sync.Mutex
	events     []string
	failList   bool
	failStatus bool
	pending    bool
	pendingErr error
}

func (r *recordingSink) record(evt string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
}

func (r *recordingSink) ApplyKustomizationList(_ context.Context, namespace string, observations []model.KustomizationObservation) error {
	if r.failList {
		return errors.New("apply list failed")
	}
	names := make([]string, 0, len(observations))
	for _, obs := range observations {
		names = append(names, obs.Ref.Name)
	}
	r.record(fmt.Sprintf("list %s %v", namespace, names))
	return nil
}

func (r *recordingSink) ApplyKustomizationStatus(_ context.Context, obs model.KustomizationObservation) error {
	r.record(fmt.Sprintf("status %s/%s requested=%s handled=%s", obs.Ref.Namespace, obs.Ref.Name, obs.RequestedAt, obs.LastHandledReconcileAt))
	if r.failStatus {
		return errors.New("registry down")
	}
	return nil
}

func (r *recordingSink) ApplyKustomizationDeleted(_ context.Context, ref model.FluxKustomizationRef) error {
	r.record("deleted " + ref.Namespace + "/" + ref.Name)
	return nil
}

func (r *recordingSink) HasPendingRollouts(context.Context) (bool, error) {
	r.record(fmt.Sprintf("pending? %v", r.pending))
	return r.pending, r.pendingErr
}

type kubeWatcherTestCase struct {
	name         string
	source       KubeWatchSource
	lists        []kubeRoute
	watches      []kubeRoute
	failList     bool
	failStatus   bool
	pending      bool
	pendingErr   error
	wantRequests []string
	wantEvents   []string
}

func kubeWatcherKustomizationCases(list func(rv string, names ...string) kubeRoute, event func(typ, name, annotation, handled string) string, gone string) []kubeWatcherTestCase {
	return []kubeWatcherTestCase{
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "list then chunked watch stream applies events and re-lists when stream ends",
			lists:        []kubeRoute{list("10", "aerial-core"), list("20")},
			watches:      []kubeRoute{{body: event("ADDED", "agent-gamma-core", reqT1, "") + `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"15"}}}` + "\n" + event("MODIFIED", "aerial-core", reqT1, reqT1) + event("DELETED", "agent-gamma-core", reqT1, "")}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=20"},
			wantEvents: []string{
				"list flux-system [aerial-core]",
				"status flux-system/agent-gamma-core requested=" + reqT1 + " handled=",
				"status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT1,
				"deleted flux-system/agent-gamma-core",
				"list flux-system []",
			},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "410 Gone error event re-lists",
			lists:        []kubeRoute{list("10", "aerial-core"), list("30", "aerial-core")},
			watches:      []kubeRoute{{body: event("MODIFIED", "aerial-core", reqT1, reqT0) + gone}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=30"},
			wantEvents:   []string{"list flux-system [aerial-core]", "status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT0, "list flux-system [aerial-core]"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "410 Gone watch response re-lists",
			lists:        []kubeRoute{list("10"), list("40")},
			watches:      []kubeRoute{{status: http.StatusGone, body: `{"kind":"Status","code":410}`}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=40"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "list failure backs off and retries",
			lists:        []kubeRoute{{status: http.StatusInternalServerError, body: `boom`}, list("50")},
			wantRequests: []string{"LIST kustomizations", "LIST kustomizations", "WATCH kustomizations rv=50"},
			wantEvents:   []string{"list flux-system []"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "watch rejection backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("60")},
			watches:      []kubeRoute{{status: http.StatusForbidden, body: `forbidden`}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=60"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "malformed watch event backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("70")},
			watches:      []kubeRoute{{body: "{bad\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=70"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "non-410 error event backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("80")},
			watches:      []kubeRoute{{body: `{"type":"ERROR","object":{"kind":"Status","code":500}}` + "\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=80"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "malformed watched object backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("85")},
			watches:      []kubeRoute{{body: `{"type":"MODIFIED","object":[]}` + "\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=85"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "sink failure applying the list backs off and re-lists",
			lists:        []kubeRoute{list("10", "aerial-core")},
			failList:     true,
			wantRequests: []string{"LIST kustomizations", "LIST kustomizations"},
		},
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "sink failure on an event re-lists to re-derive state",
			lists:        []kubeRoute{list("10"), list("90")},
			watches:      []kubeRoute{{body: event("MODIFIED", "aerial-core", reqT1, reqT1)}},
			failStatus:   true,
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=90"},
			wantEvents:   []string{"list flux-system []", "status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT1, "list flux-system []"},
		},
	}
}

func kubeWatcherWorkloadCases(list func(rv string, names ...string) kubeRoute, workloads func(rv string) kubeRoute, workloadEvent func(typ string) string, deployments WorkloadSource) []kubeWatcherTestCase {
	const (
		deploymentsList  = "LIST deployments sel=kustomize.toolkit.fluxcd.io/name"
		deploymentsWatch = "WATCH deployments sel=kustomize.toolkit.fluxcd.io/name rv="
	)
	return []kubeWatcherTestCase{
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
			name:         "workload changes skip kustomization re-list without pending rollouts",
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
			name:         "pending rollout lookup failure backs off and re-lists workloads",
			source:       deployments,
			pendingErr:   errors.New("db error"),
			lists:        []kubeRoute{workloads("5")},
			wantRequests: []string{deploymentsList, deploymentsList},
			wantEvents:   []string{"pending? false"},
		},
		{
			name:         "workload list failure backs off and retries",
			source:       deployments,
			lists:        []kubeRoute{{status: http.StatusForbidden, body: `forbidden`}, {body: `{"metadata":`}, workloads("9")},
			wantRequests: []string{deploymentsList, deploymentsList, deploymentsList, deploymentsWatch + "9"},
			wantEvents:   []string{"pending? false"},
		},
	}
}

func TestKubeWatcherRun(t *testing.T) {
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
	deployments := WorkloadSource{Path: "/apis/apps/v1/deployments"}
	gone := `{"type":"ERROR","object":{"kind":"Status","code":410,"message":"too old resource version"}}` + "\n"

	tests := append(
		kubeWatcherKustomizationCases(list, event, gone),
		kubeWatcherWorkloadCases(list, workloads, workloadEvent, deployments)...,
	)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture := &watchFixture{lists: tc.lists, watches: tc.watches, cancel: cancel}
			ts := httptest.NewServer(fixture)
			defer ts.Close()

			client := kube.NewClient(ts.Client())
			client.SetKubeConfig(ts.URL, "test-token")
			client.SetRetryConfig(time.Millisecond, 6)
			sink := &recordingSink{failList: tc.failList, failStatus: tc.failStatus, pending: tc.pending, pendingErr: tc.pendingErr}

			NewKubeWatcher(client, sink, tc.source, time.Millisecond).Run(ctx)

			if !slices.Equal(fixture.requests, tc.wantRequests) {
				t.Errorf("requests = %v, want %v", fixture.requests, tc.wantRequests)
			}
			if !slices.Equal(sink.events, tc.wantEvents) {
				t.Errorf("events = %q, want %q", sink.events, tc.wantEvents)
			}
		})
	}
}

func TestKubeWatchSourceString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source KubeWatchSource
		want   string
	}{
		{"kustomizations", KustomizationSource{Namespace: "flux-system"}, "flux-system kustomizations"},
		{"workloads", WorkloadSource{Path: "/apis/apps/v1/deployments"}, "/apis/apps/v1/deployments"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.source.String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunRolloutRechecks(t *testing.T) {
	t.Parallel()
	list := kubeRoute{body: `{"metadata":{"resourceVersion":"3"},"items":[{"metadata":{"name":"aerial-core","namespace":"flux-system"}}]}`}
	tests := []struct {
		name         string
		pending      bool
		ticks        int
		lists        []kubeRoute
		wantRequests []string
		wantEvents   []string
	}{
		{name: "every tick re-lists kustomizations while a rollout is pending", pending: true, ticks: 2, lists: []kubeRoute{list, list}, wantRequests: []string{"LIST kustomizations", "LIST kustomizations"}, wantEvents: []string{"pending? true", "list flux-system [aerial-core]", "pending? true", "list flux-system [aerial-core]"}},
		{name: "ticks without pending rollouts list nothing", ticks: 2, wantEvents: []string{"pending? false", "pending? false"}},
		{name: "a failed re-list is logged and the next tick retries", pending: true, ticks: 2, lists: []kubeRoute{{status: http.StatusInternalServerError, body: `boom`}, list}, wantRequests: []string{"LIST kustomizations", "LIST kustomizations"}, wantEvents: []string{"pending? true", "pending? true", "list flux-system [aerial-core]"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := &watchFixture{lists: tc.lists, cancel: func() {}}
			ts := httptest.NewServer(fixture)
			defer ts.Close()
			client := kube.NewClient(ts.Client())
			client.SetKubeConfig(ts.URL, "test-token")
			sink := &recordingSink{pending: tc.pending}
			ticks := make(chan time.Time, tc.ticks)
			for range tc.ticks {
				ticks <- time.Time{}
			}
			close(ticks)

			RunRolloutRechecks(t.Context(), client, sink, ticks)

			if got := fixture.requests; !slices.Equal(got, tc.wantRequests) {
				t.Errorf("requests = %v, want %v", got, tc.wantRequests)
			}
			if !slices.Equal(sink.events, tc.wantEvents) {
				t.Errorf("events = %q, want %q", sink.events, tc.wantEvents)
			}
		})
	}
}

func TestRunRolloutRechecksRateLimit(t *testing.T) {
	t.Parallel()
	client := kube.NewClient(nil)
	sink := &recordingSink{
		pending:    true,
		pendingErr: &githubapi.StatusError{StatusCode: 429, RateLimited: true, RetryAfter: time.Hour},
	}
	ticks := make(chan time.Time, 3)
	ticks <- time.Now()
	ticks <- time.Now()
	ticks <- time.Now()
	close(ticks)

	RunRolloutRechecks(context.Background(), client, sink, ticks)

	// Since tick 1 encountered a 1-hour rate limit, ticks 2 and 3 must be skipped.
	// Only 1 pending check should have been attempted.
	if len(sink.events) != 1 {
		t.Fatalf("expected exactly 1 event (the initial check), got %d: %v", len(sink.events), sink.events)
	}
}

type rateLimitSource struct {
	mu     sync.Mutex
	times  []time.Time
	cancel context.CancelFunc
	err    error
}

func (*rateLimitSource) String() string { return "rateLimitSource" }

func (s *rateLimitSource) List(context.Context, KubeClient, RolloutSink) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.times = append(s.times, time.Now())
	if len(s.times) >= 2 {
		s.cancel()
	}
	return "", s.err
}

func (*rateLimitSource) Watch(context.Context, KubeClient, string) (io.ReadCloser, error) {
	return nil, nil
}

func (*rateLimitSource) Apply(context.Context, KubeClient, RolloutSink, string, json.RawMessage) error {
	return nil
}

func TestKubeWatcherRunRateLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const rlDelay = 50 * time.Millisecond
	src := &rateLimitSource{
		cancel: cancel,
		err:    &githubapi.StatusError{StatusCode: 429, RateLimited: true, RetryAfter: rlDelay},
	}
	watcher := NewKubeWatcher(nil, nil, src, time.Millisecond)
	watcher.Run(ctx)

	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.times) < 2 {
		t.Fatalf("expected at least 2 list calls, got %d", len(src.times))
	}
	gap := src.times[1].Sub(src.times[0])
	if gap < 35*time.Millisecond {
		t.Fatalf("gap between calls = %v, want at least %v", gap, 35*time.Millisecond)
	}
}

func TestStartWatcher(t *testing.T) {
	t.Parallel()
	// Nil client returns no-op
	stop := StartWatcher(context.Background(), nil, &recordingSink{})
	stop()

	// Client without APIServer returns no-op
	clientUnconf := kube.NewClient(nil)
	stopUnconf := StartWatcher(context.Background(), clientUnconf, &recordingSink{})
	stopUnconf()

	// Configured client starts watchers and can be stopped
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

	client := kube.NewClient(ts.Client())
	client.SetKubeConfig(ts.URL, "token")
	sink := &recordingSink{}

	stopRunning := StartWatcher(context.Background(), client, sink)
	// Collect first 6 lists
	for range 6 {
		select {
		case <-listed:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for watcher list")
		}
	}
	stopRunning()
}
