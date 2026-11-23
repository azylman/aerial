package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/flux"
)

const (
	reqT0 = "2026-11-08T09:00:00Z"
	reqT1 = "2026-11-08T10:00:00.5Z"
)

func readyCond(status, reason, message string) []fluxCondition {
	return []fluxCondition{{Type: "Reconciling", Status: "False"}, {Type: "Ready", Status: status, Reason: reason, Message: message}}
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

func (m *mockOutboundDispatcher) ListKustomizationConfigMaps(ctx context.Context, ref fluxKustomizationRef) ([]configMapChange, error) {
	return m.configMaps, m.configMapsErr
}

const (
	sha42Commit  = "GET /repos/azylman/aerial/git/commits/sha-42"
	sha42Compare = "GET /repos/azylman/aerial/compare/sha-42...sha-50"
)

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

func (r *recordingSink) ApplyKustomizationList(_ context.Context, namespace string, observations []kustomizationObservation) error {
	names := make([]string, 0, len(observations))
	for _, obs := range observations {
		names = append(names, obs.Ref.Name)
	}
	r.record(fmt.Sprintf("list %s %v", namespace, names))
	return nil
}

func (r *recordingSink) ApplyKustomizationStatus(_ context.Context, obs kustomizationObservation) error {
	r.record(fmt.Sprintf("status %s/%s requested=%s handled=%s", obs.Ref.Namespace, obs.Ref.Name, obs.RequestedAt, obs.LastHandledReconcileAt))
	if r.failStatus {
		return errors.New("registry down")
	}
	return nil
}

func (r *recordingSink) ApplyKustomizationDeleted(_ context.Context, ref fluxKustomizationRef) error {
	r.record("deleted " + ref.Namespace + "/" + ref.Name)
	return nil
}

func (r *recordingSink) HasPendingRollouts(context.Context) (bool, error) {
	r.record(fmt.Sprintf("pending? %v", r.pending))
	return r.pending, nil
}

type kustomizationWatcherRunTestCase struct {
	name         string
	lists        []kubeRoute
	watches      []kubeRoute
	source       flux.KubeWatchSource
	failStatus   bool
	pending      bool
	wantRequests []string
	wantEvents   []string
}

func runKustomizationWatcherCase(t *testing.T, tc kustomizationWatcherRunTestCase) {
	t.Helper()
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture := &watchFixture{lists: tc.lists, watches: tc.watches, cancel: cancel}
	ts := httptest.NewServer(fixture)
	defer ts.Close()
	disp := NewDefaultOutboundDispatcher("", ts.Client())
	disp.SetKubeConfig(ts.URL, "test-token")
	sink := &recordingSink{failStatus: tc.failStatus, pending: tc.pending}

	flux.NewKubeWatcher(disp, sink, tc.source, time.Millisecond).Run(ctx)

	if got := fixture.requests; !slices.Equal(got, tc.wantRequests) {
		t.Errorf("requests = %v, want %v", got, tc.wantRequests)
	}
	if !slices.Equal(sink.events, tc.wantEvents) {
		t.Errorf("events = %q, want %q", sink.events, tc.wantEvents)
	}
}

func TestKustomizationWatcherRun(t *testing.T) {
	t.Parallel()

	for _, tc := range kustomizationWatcherRunTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runKustomizationWatcherCase(t, tc)
		})
	}
}

func kustomizationWatcherRunTestCases() []kustomizationWatcherRunTestCase {
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
	deployments := flux.WorkloadSource{Path: "/apis/apps/v1/deployments"}
	gone := `{"type":"ERROR","object":{"kind":"Status","code":410,"message":"too old resource version"}}` + "\n"

	return []kustomizationWatcherRunTestCase{
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "list then chunked watch stream applies events and re-lists when the stream ends",
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
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "410 Gone error event re-lists",
			lists:        []kubeRoute{list("10", "aerial-core"), list("30", "aerial-core")},
			watches:      []kubeRoute{{body: event("MODIFIED", "aerial-core", reqT1, reqT0) + gone}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=30"},
			wantEvents:   []string{"list flux-system [aerial-core]", "status flux-system/aerial-core requested=" + reqT1 + " handled=" + reqT0, "list flux-system [aerial-core]"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "410 Gone watch response re-lists",
			lists:        []kubeRoute{list("10"), list("40")},
			watches:      []kubeRoute{{status: http.StatusGone, body: `{"kind":"Status","code":410}`}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=40"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "list failure backs off and retries",
			lists:        []kubeRoute{{status: http.StatusInternalServerError, body: `boom`}, list("50")},
			wantRequests: []string{"LIST kustomizations", "LIST kustomizations", "WATCH kustomizations rv=50"},
			wantEvents:   []string{"list flux-system []"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "watch rejection backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("60")},
			watches:      []kubeRoute{{status: http.StatusForbidden, body: `forbidden`}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=60"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "malformed watch event backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("70")},
			watches:      []kubeRoute{{body: "{bad\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=70"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "non-410 error event backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("80")},
			watches:      []kubeRoute{{body: `{"type":"ERROR","object":{"kind":"Status","code":500}}` + "\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=80"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
			name:         "malformed watched object backs off and re-lists",
			lists:        []kubeRoute{list("10"), list("85")},
			watches:      []kubeRoute{{body: `{"type":"MODIFIED","object":[]}` + "\n"}},
			wantRequests: []string{"LIST kustomizations", "WATCH kustomizations rv=10", "LIST kustomizations", "WATCH kustomizations rv=85"},
			wantEvents:   []string{"list flux-system []", "list flux-system []"},
		},
		{
			source:       flux.KustomizationSource{Namespace: "flux-system"},
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
}

func TestStartKustomizationWatcher(t *testing.T) {
	t.Parallel()
	allSources := []string{
		"/apis/apps/v1/daemonsets",
		"/apis/apps/v1/deployments",
		"/apis/apps/v1/statefulsets",
		"/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations",
		"/apis/restate.dev/v1/restateclusters",
		"/apis/restate.dev/v1beta1/restatedeployments",
	}
	tests := []struct {
		name       string
		withKube   bool
		wantListed []string
	}{
		{name: "no kube API does not watch"},
		{name: "kube API watches kustomizations and every rollout workload kind until stopped", withKube: true, wantListed: allSources},
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
			srv := newTestRouter(Config{}, nil, disp)
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

func TestDefaultOutboundDispatcher_ListKustomizationWorkloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deploymentList := `{"items":[{"metadata":{"name":"synapse","namespace":"default","generation":2},"spec":{"replicas":1,"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}},"status":{"observedGeneration":2,"replicas":1,"updatedReplicas":1,"availableReplicas":1}}]}`
	daemonSetList := `{"items":[{"metadata":{"name":"agent","namespace":"kube-system","generation":1},"spec":{"template":{"spec":{"containers":[{"image":"agent:abc"}]}}},"status":{"observedGeneration":1,"desiredNumberScheduled":2,"updatedNumberScheduled":1,"numberAvailable":2}}]}`
	restateDeploymentList := `{"items":[{"metadata":{"name":"restate-worker","namespace":"default","generation":1},"spec":{"replicas":1,"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-restate-worker:abc"}]}}},"status":{"observedGeneration":1,"replicas":1,"readyReplicas":1,"availableReplicas":1,"deploymentId":"dp_123","conditions":[{"type":"Ready","status":"True"}]}}]}`
	restateClusterList := `{"items":[{"metadata":{"name":"restate","namespace":"default","generation":1},"spec":{"compute":{"image":"docker.io/restatedev/restate:1.6"}},"status":{"observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}]}}]}`

	tests := []struct {
		name         string
		noKube       bool
		routes       map[string]kubeRoute
		want         []string
		wantErr      string
		wantRequests int
	}{
		{
			name:         "lists deployments statefulsets daemonsets and restate resources of the kustomization",
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeDaemonSetsPath: {body: daemonSetList}, kubeRestateDeploymentsPath: {body: restateDeploymentList}, kubeRestateClustersPath: {body: restateClusterList}}),
			want:         []string{"Deployment default/synapse", "DaemonSet kube-system/agent", "RestateDeployment default/restate-worker", "RestateCluster default/restate"},
			wantRequests: 5,
		},
		{
			name:         "missing crd 404 is treated as empty workload list during rollout listing",
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeRestateDeploymentsPath: {status: http.StatusNotFound, body: `{"reason":"NotFound"}`}, kubeRestateClustersPath: {status: http.StatusNotFound, body: `{"reason":"NotFound"}`}}),
			want:         []string{"Deployment default/synapse"},
			wantRequests: 5,
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
				got = append(got, flux.WorkloadString(w))
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
		{name: "bare payload repo is qualified with the owner", payload: `{"involvedObject":{"kind":"Kustomization","name":"stack-core"},"reason":"Progressing","metadata":{"repo":"mirrormere","revision":"main@sha1:abc"}}`, wantRepo: "azylman/mirrormere"},
		{name: "no dispatcher falls back to the default repo", payload: `{"involvedObject":{"kind":"GitRepository","name":"stack"},"reason":"NewArtifact","metadata":{"revision":"main@sha1:abc"}}`, noDisp: true, wantRepo: "azylman/aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var disp OutboundDispatcher
			if !tc.noDisp {
				disp = &mockOutboundDispatcher{sourceRepo: tc.sourceRepo, sourceRepoErr: tc.sourceErr}
			}
			srv := newTestRouter(Config{GitHubOwner: "azylman"}, nil, disp)
			bus := &mockEventBus{}
			srv.SetEventBus(bus)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/flux", strings.NewReader(tc.payload))
			w := httptest.NewRecorder()
			srv.Routes().ServeHTTP(w, req)
			if w.Code != http.StatusAccepted || len(bus.insertedDeployJobs) != 1 {
				t.Fatalf("status = %d, inserted = %d; want 202 and one deploy job (body %s)", w.Code, len(bus.insertedDeployJobs), w.Body.String())
			}
			if got := bus.insertedDeployJobs[0].Event.Repo; got != tc.wantRepo {
				t.Errorf("repo = %q, want %q", got, tc.wantRepo)
			}
		})
	}
}

func TestRevisionIncludes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		revision     string
		routes       map[string]kubeRoute
		want         bool
		wantErr      bool
		wantRequests int
	}{
		{name: "empty revision includes nothing", wantRequests: 0},
		{name: "the commit itself", revision: "sha-42", want: true},
		{name: "ahead verdict is cached", revision: "sha-50", routes: map[string]kubeRoute{sha42Compare: {body: `{"status":"ahead"}`}}, want: true, wantRequests: 1},
		{name: "behind verdict is cached", revision: "sha-50", routes: map[string]kubeRoute{sha42Compare: {body: `{"status":"behind"}`}}, wantRequests: 1},
		{name: "compare failure is not cached", revision: "sha-50", routes: map[string]kubeRoute{sha42Compare: {status: http.StatusNotFound, body: `{}`}}, wantErr: true, wantRequests: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, srv := newRolloutRouter(t, tc.routes)
			for range 2 {
				got, err := srv.revisionIncludes(context.Background(), "azylman/aerial", tc.revision, "sha-42")
				if got != tc.want || (err != nil) != tc.wantErr {
					t.Fatalf("revisionIncludes() = (%v, %v), want (%v, wantErr %v)", got, err, tc.want, tc.wantErr)
				}
			}
			if n := len(fixture.Requests()); n != tc.wantRequests {
				t.Errorf("github requests = %d (%v), want %d", n, fixture.Requests(), tc.wantRequests)
			}
			if tc.wantRequests > 0 {
				if q := fixture.Query(sha42Compare); q != "per_page=1" {
					t.Errorf("compare query = %q, want per_page=1", q)
				}
			}
		})
	}
}

func TestCommitTime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		routes       map[string]kubeRoute
		want         time.Time
		wantErr      bool
		wantRequests int
	}{
		{name: "committer date is cached", want: rolloutClock(10, 0, 0), wantRequests: 1},
		{name: "lookup failure is not cached", routes: map[string]kubeRoute{sha42Commit: {status: http.StatusInternalServerError, body: `{}`}}, wantErr: true, wantRequests: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, srv := newRolloutRouter(t, tc.routes)
			for range 2 {
				got, err := srv.commitTime(context.Background(), "azylman/aerial", "sha-42")
				if !got.Equal(tc.want) || (err != nil) != tc.wantErr {
					t.Fatalf("commitTime() = (%v, %v), want (%v, wantErr %v)", got, err, tc.want, tc.wantErr)
				}
			}
			if n := len(fixture.Requests()); n != tc.wantRequests {
				t.Errorf("github requests = %d (%v), want %d", n, fixture.Requests(), tc.wantRequests)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_ListKustomizationConfigMaps(t *testing.T) {
	t.Parallel()
	const configMapsPath = "GET /api/v1/configmaps"
	tests := []struct {
		name    string
		noKube  bool
		routes  map[string]kubeRoute
		want    []configMapChange
		wantErr string
	}{
		{
			name:   "decodes the last non-subresource write of each configmap",
			routes: map[string]kubeRoute{configMapsPath: {body: `{"items":[{"metadata":{"name":"eri-config","namespace":"default","managedFields":[{"manager":"kustomize-controller","operation":"Apply","time":"2026-11-08T10:01:00Z"},{"manager":"other","operation":"Update","subresource":"status","time":"2026-11-08T10:05:00Z"}]}},{"metadata":{"name":"bare","namespace":"apps"}}]}`}},
			want:   []configMapChange{{Namespace: "default", Name: "eri-config", ChangedAt: rolloutClock(10, 1, 0)}, {Namespace: "apps", Name: "bare"}},
		},
		{name: "list failure returns error", routes: map[string]kubeRoute{configMapsPath: {status: http.StatusForbidden, body: `forbidden`}}, wantErr: "list aerial-core configmaps"},
		{name: "malformed list returns error", routes: map[string]kubeRoute{configMapsPath: {body: `{"items":`}}, wantErr: "decode aerial-core configmaps"},
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
			got, err := disp.ListKustomizationConfigMaps(context.Background(), fluxKustomizationRef{Namespace: "flux-system", Name: "aerial-core"})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("configmaps = %+v, want %+v", got, tc.want)
			}
			if !tc.noKube {
				if q := fixture.Query(configMapsPath); q != "labelSelector=kustomize.toolkit.fluxcd.io%2Fname%3Daerial-core%2Ckustomize.toolkit.fluxcd.io%2Fnamespace%3Dflux-system" {
					t.Errorf("configmaps query = %q", q)
				}
			}
		})
	}
}

// newRolloutRouter serves the merge-commit lookups the rollout gate makes from
// a fake GitHub API (plus any per-test overrides).
func newRolloutRouter(t *testing.T, overrides map[string]kubeRoute) (*kubeFixture, *RouterServer) {
	t.Helper()
	routes := map[string]kubeRoute{
		"GET /repos/azylman/aerial/git/commits/sha-41": {body: `{"sha":"sha-41","committer":{"date":"2026-11-08T10:00:00Z"}}`},
		sha42Commit: {body: `{"sha":"sha-42","committer":{"date":"2026-11-08T10:00:00Z"}}`},
	}
	maps.Copy(routes, overrides)
	fixture, ts := newKubeFixture(t, routes)
	return fixture, newTestRouter(Config{GitHubAPIURL: ts.URL}, ts.Client(), nil)
}

// rolloutClock returns 2026-11-08 at hh:mm:ss UTC, the day of the rollout fixtures.
func rolloutClock(hh, mm, ss int) time.Time {
	return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
}

type handleFluxWebhookCase struct {
	name           string
	endpoint       string
	payload        string
	withBus        bool
	busFail        bool
	downstreamFail bool
	wantStatusCode int
	wantStatus     string
	wantEvent      string
	wantJobName    string
	wantPRNumber   int
	wantBusJobs    int
}

func assertFluxResponse(t *testing.T, body []byte, wantStatus, wantEvent, wantJob string, wantPR int) {
	t.Helper()
	if wantStatus == "" {
		return
	}
	var res map[string]any
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if gotStatus, _ := res["status"].(string); gotStatus != wantStatus {
		t.Errorf("status = %q, want %q", gotStatus, wantStatus)
	}
	if wantEvent != "" {
		if gotEvent, _ := res["event"].(string); gotEvent != wantEvent {
			t.Errorf("event = %q, want %q", gotEvent, wantEvent)
		}
	}
	if wantJob != "" {
		if gotJob, _ := res["job_name"].(string); gotJob != wantJob {
			t.Errorf("job_name = %q, want %q", gotJob, wantJob)
		}
	}
	if wantPR != 0 {
		if gotPR, _ := res["pr_number"].(float64); int(gotPR) != wantPR {
			t.Errorf("pr_number = %v, want %d", res["pr_number"], wantPR)
		}
	}
}

func runHandleFluxWebhookCase(t *testing.T, tc handleFluxWebhookCase) {
	t.Helper()
	srv := newTestRouter(Config{SystemChannelID: "123456789012345678"}, nil, nil)
	if tc.downstreamFail {
		srv.SetDispatcher(&mockOutboundDispatcher{directMessageErr: errors.New("dispatch failed")})
	}
	var mockBus *mockEventBus
	if tc.withBus {
		mockBus = &mockEventBus{}
		if tc.busFail {
			mockBus.err = errors.New("bus insert failed")
		}
		srv.SetEventBus(mockBus)
	}

	req := httptest.NewRequest(http.MethodPost, tc.endpoint, strings.NewReader(tc.payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != tc.wantStatusCode {
		t.Fatalf("status code = %d, want %d (body: %s)", w.Code, tc.wantStatusCode, w.Body.String())
	}

	assertFluxResponse(t, w.Body.Bytes(), tc.wantStatus, tc.wantEvent, tc.wantJobName, tc.wantPRNumber)

	if tc.withBus && mockBus != nil {
		if len(mockBus.insertedDeployJobs) != tc.wantBusJobs {
			t.Errorf("inserted bus jobs = %d, want %d", len(mockBus.insertedDeployJobs), tc.wantBusJobs)
		}
	}
}

func TestHandleFluxWebhook(t *testing.T) {
	t.Parallel()

	tests := []handleFluxWebhookCase{
		{
			name:           "POST /webhooks/flux valid kustomization without bus",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"synapse","namespace":"flux-system"},"severity":"info","reason":"ReconciliationSucceeded","message":"reconciliation ok"}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "deploy_success",
			wantJobName:    "synapse",
		},
		{
			name:           "POST /api/webhooks/flux valid gitrepository without bus",
			endpoint:       "/api/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"GitRepository","name":"aerial-config","namespace":"flux-system"},"severity":"info","reason":"NewArtifact","message":"sync ok","metadata":{"revision":"main@sha1:abcdef1234"}}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "sync_success",
			wantJobName:    "git-sync",
		},
		{
			name:           "POST /webhooks/flux unhandled alert returns 200 ignored",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Bucket","name":"test","namespace":"flux-system"},"severity":"info","reason":"Progressing"}`,
			wantStatusCode: http.StatusOK,
			wantStatus:     "ignored",
		},
		{
			name:           "POST /webhooks/flux invalid json returns 400",
			endpoint:       "/webhooks/flux",
			payload:        `{invalid json`,
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "POST /webhooks/flux with bus inserts job",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"brain","namespace":"flux-system"},"severity":"info","reason":"Progressing","message":"progressing","metadata":{"revision":"sha1:12345"}}`,
			withBus:        true,
			wantStatusCode: http.StatusAccepted,
			wantStatus:     "accepted",
			wantEvent:      "deploy_started",
			wantJobName:    "brain",
			wantBusJobs:    1,
		},
		{
			name:           "POST /webhooks/flux with bus failure returns 500",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"brain","namespace":"flux-system"},"severity":"info","reason":"Progressing"}`,
			withBus:        true,
			busFail:        true,
			wantStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "POST /webhooks/flux without bus error processing event returns 200 with result",
			endpoint:       "/webhooks/flux",
			payload:        `{"involvedObject":{"kind":"Kustomization","name":"webhooks-router","namespace":"flux-system"},"severity":"error","reason":"ReconciliationFailed","message":"broken deploy"}`,
			downstreamFail: true,
			wantStatusCode: http.StatusOK,
			wantStatus:     "accepted",
			wantEvent:      "deploy_failed",
			wantJobName:    "webhooks-router",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleFluxWebhookCase(t, tc)
		})
	}
}
