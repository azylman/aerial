package main

import (
	"cmp"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type kubeRoute struct {
	status int
	body   string
}

// kubeFixture is a declarative kube API mock that records every request in arrival order.
type kubeFixture struct {
	mu       sync.Mutex
	routes   map[string]kubeRoute
	requests []string
	bodies   map[string]string
	queries  map[string]string
}

func newKubeFixture(t *testing.T, routes map[string]kubeRoute) (*kubeFixture, *httptest.Server) {
	t.Helper()
	f := &kubeFixture{routes: routes, bodies: map[string]string{}, queries: map[string]string{}}
	ts := httptest.NewServer(f)
	t.Cleanup(ts.Close)
	return f, ts
}

func (f *kubeFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, key)
	f.bodies[key] = string(body)
	f.queries[key] = r.URL.RawQuery
	route, ok := f.routes[key]
	f.mu.Unlock()
	if !ok {
		route = kubeRoute{status: http.StatusNotFound, body: `{"kind":"Status","reason":"NotFound"}`}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(cmp.Or(route.status, http.StatusOK))
	_, _ = w.Write([]byte(route.body))
}

func (f *kubeFixture) Requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func (f *kubeFixture) Query(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries[key]
}

func (f *kubeFixture) Body(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bodies[key]
}

const (
	kubeDeploymentsPath  = "GET /apis/apps/v1/deployments"
	kubeStatefulSetsPath = "GET /apis/apps/v1/statefulsets"
	kubeDaemonSetsPath   = "GET /apis/apps/v1/daemonsets"
	kubeJobsPath         = "GET /apis/batch/v1/jobs"
	kubeCronJobsPath     = "GET /apis/batch/v1/cronjobs"
	kubeKustomizations   = "GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations"
	emptyKubeList        = `{"items":[]}`
)

// kubeWorkloadRoutes returns every workload list route empty, overlaid with overrides.
func kubeWorkloadRoutes(overrides map[string]kubeRoute) map[string]kubeRoute {
	routes := map[string]kubeRoute{
		kubeDeploymentsPath:  {body: emptyKubeList},
		kubeStatefulSetsPath: {body: emptyKubeList},
		kubeDaemonSetsPath:   {body: emptyKubeList},
		kubeJobsPath:         {body: emptyKubeList},
		kubeCronJobsPath:     {body: emptyKubeList},
	}
	maps.Copy(routes, overrides)
	return routes
}
