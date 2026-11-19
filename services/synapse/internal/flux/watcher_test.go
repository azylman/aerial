package flux

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
	recorded := model.KustomizationRequest{Namespace: "flux-system", RequestedAt: reqT1}
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

func TestRecordedKustomizationRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		meta string
		want map[string]model.KustomizationRequest
	}{
		{name: "decodes requests", meta: `{"kustomization_requests":{"marida-core":{"namespace":"apps","requested_at":"t1"}}}`, want: map[string]model.KustomizationRequest{"marida-core": {Namespace: "apps", RequestedAt: "t1"}}},
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
			got := RecordedKustomizationRequests(meta)
			if len(got) != len(tc.want) {
				t.Fatalf("RecordedKustomizationRequests() = %v, want %v", got, tc.want)
			}
			for name, req := range tc.want {
				if got[name] != req {
					t.Errorf("request %s = %+v, want %+v", name, got[name], req)
				}
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
		{Namespace: "flux-system", Name: "marida-core"},
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
	tr := TrackedRollout{
		PR: model.DeployingPR{
			Repo:     "azylman/aerial",
			PRNumber: 42,
		},
		Requests: map[string]model.KustomizationRequest{
			"aerial-core": {Namespace: "flux-system", RequestedAt: reqT1},
		},
	}

	req, ok := tr.PendingRequest(ref)
	if !ok || req.RequestedAt != reqT1 {
		t.Errorf("PendingRequest() = (%+v, %v), want ok=true", req, ok)
	}
	if !tr.HasPending() {
		t.Errorf("HasPending() = false, want true")
	}

	// Deployed
	tr.Deployed = []string{"aerial-core"}
	_, ok = tr.PendingRequest(ref)
	if ok {
		t.Errorf("expected PendingRequest = false when already deployed")
	}
	if tr.HasPending() {
		t.Errorf("expected HasPending = false when deployed")
	}

	// Deploy success pending flag keeps it pending
	tr.PR.Metadata = map[string]any{"deploy_success_pending": true}
	_, ok = tr.PendingRequest(ref)
	if !ok {
		t.Errorf("expected PendingRequest = true when deploy_success_pending is set")
	}

	// Wrong namespace
	wrongRef := model.FluxKustomizationRef{Namespace: "other", Name: "aerial-core"}
	if _, pending := tr.PendingRequest(wrongRef); pending {
		t.Errorf("expected PendingRequest = false for wrong namespace")
	}

	// Empty requestedAt
	emptyReq := TrackedRollout{
		Requests: map[string]model.KustomizationRequest{
			"aerial-core": {Namespace: "flux-system", RequestedAt: ""},
		},
	}
	if _, pending := emptyReq.PendingRequest(ref); pending {
		t.Errorf("expected PendingRequest = false for empty requestedAt")
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
	failStatus   bool
	pending      bool
	wantRequests []string
	wantEvents   []string
}

func kubeWatcherKustomizationCases(list func(rv string, names ...string) kubeRoute, event func(typ, name, annotation, handled string) string, gone string) []kubeWatcherTestCase {
	return []kubeWatcherTestCase{
		{
			source:       KustomizationSource{Namespace: "flux-system"},
			name:         "list then chunked watch stream applies events and re-lists when stream ends",
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
			sink := &recordingSink{failStatus: tc.failStatus, pending: tc.pending}

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

func TestKubeWatchSourcesEdgeCases(t *testing.T) {
	t.Parallel()
	ks := KustomizationSource{Namespace: "flux-system"}
	if got := ks.String(); got != "flux-system kustomizations" {
		t.Errorf("KustomizationSource.String() = %q", got)
	}

	ws := WorkloadSource{Path: "/test"}
	if got := ws.String(); got != "/test" {
		t.Errorf("WorkloadSource.String() = %q", got)
	}

	// Apply deleted on WorkloadSource is no-op
	if err := ws.Apply(context.Background(), nil, &recordingSink{}, "DELETED", nil); err != nil {
		t.Errorf("unexpected error on WorkloadSource DELETED apply: %v", err)
	}

	// Apply malformed JSON on KustomizationSource
	if err := ks.Apply(context.Background(), nil, &recordingSink{}, "ADDED", []byte("invalid")); err == nil {
		t.Errorf("expected error decoding malformed JSON in KustomizationSource.Apply")
	}

	// List error in KustomizationSource when sink fails
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"1"},"items":[]}`))
	}))
	defer ts.Close()
	client := kube.NewClient(ts.Client())
	client.SetKubeConfig(ts.URL, "token")
	sinkFail := &recordingSink{failList: true}
	if _, err := ks.List(context.Background(), client, sinkFail); err == nil {
		t.Errorf("expected error when ApplyKustomizationList fails")
	}

	// RecheckPendingRollouts error from sink
	sinkErr := &recordingSink{pendingErr: errors.New("db error")}
	if err := RecheckPendingRollouts(context.Background(), client, sinkErr); err == nil {
		t.Errorf("expected error when HasPendingRollouts returns error")
	}
}

func TestRunRolloutRechecks(t *testing.T) {
	t.Parallel()
	list := kubeRoute{body: `{"metadata":{"resourceVersion":"3"},"items":[{"metadata":{"name":"aerial-core","namespace":"flux-system"}}]}`}
	fixture := &watchFixture{lists: []kubeRoute{list, list}, cancel: func() {}}
	ts := httptest.NewServer(fixture)
	defer ts.Close()

	client := kube.NewClient(ts.Client())
	client.SetKubeConfig(ts.URL, "test-token")
	sink := &recordingSink{pending: true}
	ticks := make(chan time.Time, 2)
	ticks <- time.Time{}
	ticks <- time.Time{}
	close(ticks)

	RunRolloutRechecks(context.Background(), client, sink, ticks)

	if len(sink.events) == 0 {
		t.Errorf("expected recheck events to be recorded")
	}
}

func TestRolloutWatchSources(t *testing.T) {
	t.Parallel()
	sources := RolloutWatchSources()
	if len(sources) != 6 {
		t.Errorf("RolloutWatchSources() returned %d sources, want 6", len(sources))
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
