package kube

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

func TestClientConfig(t *testing.T) {
	t.Parallel()
	c1 := NewClient(nil)
	if c1.httpClient == nil {
		t.Errorf("expected default httpClient")
	}
	c1.SetRetryConfig(100*time.Millisecond, 3)
	if c1.RetryDelay() != 100*time.Millisecond {
		t.Errorf("RetryDelay() = %v, want 100ms", c1.RetryDelay())
	}
	c1.SetKubeConfig("http://localhost:8080/", "test-token")
	if c1.APIServer() != "http://localhost:8080" {
		t.Errorf("APIServer() = %q, want http://localhost:8080", c1.APIServer())
	}
}

func TestClient_getKubeToken(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("  mounted-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	tests := []struct {
		name          string
		explicitToken string
		tokenPath     string
		want          string
	}{
		{name: "explicit token wins over mounted token", explicitToken: " explicit-token ", tokenPath: tokenFile, want: "explicit-token"},
		{name: "empty explicit token reads mounted token", tokenPath: tokenFile, want: "mounted-token"},
		{name: "missing mounted token yields empty", tokenPath: filepath.Join(dir, "missing"), want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClient(nil)
			c.tokenSource = k8sauth.NewTokenSource(tc.tokenPath, nil)
			c.SetKubeConfig("http://kube.test", tc.explicitToken)
			if got := c.getKubeToken(); got != tc.want {
				t.Errorf("getKubeToken() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClient_SetKubeConfigAPIClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		apiServer       string
		wantBaseClient  bool
		wantAPIServer   string
		wantDedicatedTr bool
	}{
		{name: "http server reuses traced base client", apiServer: "http://kube.test/", wantBaseClient: true, wantAPIServer: "http://kube.test"},
		{name: "https server gets dedicated TLS client", apiServer: "https://kube.test:6443", wantAPIServer: "https://kube.test:6443", wantDedicatedTr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.NotFoundHandler())
			defer ts.Close()
			base := ts.Client()
			base.Timeout = 9 * time.Second
			c := NewClient(base)
			c.SetKubeConfig(tc.apiServer, "tok")
			if c.APIServer() != tc.wantAPIServer {
				t.Errorf("APIServer() = %q, want %q", c.APIServer(), tc.wantAPIServer)
			}
			if (c.apiClient == c.httpClient) != tc.wantBaseClient {
				t.Errorf("apiClient is base = %v, want %v", c.apiClient == c.httpClient, tc.wantBaseClient)
			}
			if c.apiClient.Timeout != 9*time.Second {
				t.Errorf("apiClient.Timeout = %v, want 9s", c.apiClient.Timeout)
			}
			if _, ok := c.apiClient.Transport.(*http.Transport); ok != tc.wantDedicatedTr {
				t.Errorf("dedicated transport = %v, want %v", ok, tc.wantDedicatedTr)
			}
		})
	}
}

// newKubeRouteClient returns a client for a mock kube API serving routes. Nil routes leave the kube API unconfigured;
// down closes the server before the client is used so every request fails at the transport.
func newKubeRouteClient(t *testing.T, routes map[string]kubeRoute, down bool) *Client {
	t.Helper()
	if routes == nil {
		return NewClient(nil)
	}
	ts := newMockKubeServer(routes)
	if down {
		ts.Close()
	} else {
		t.Cleanup(ts.Close)
	}
	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "tok")
	return c
}

// errText returns err's message, or "" for nil, so rows compare errors by literal substring.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func checkKubeErr(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wantErr != "" && !strings.Contains(errText(err), wantErr) {
		t.Fatalf("error = %v, want containing %q", err, wantErr)
	}
}

const (
	kubeEmptyList  = `{"items":[]}`
	kubeBadJSON    = `{invalid`
	kubeBadJSONErr = "invalid character 'i' looking for beginning of object key string"
	kubeKustPath   = "GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations"
	kubeGitRepo    = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial"
)

var (
	kubeAll500  = map[string]kubeRoute{"*": {status: http.StatusInternalServerError, body: "internal server error"}}
	kubeAllBad  = map[string]kubeRoute{"*": {body: kubeBadJSON}}
	kubeAll404  = map[string]kubeRoute{}
	kubeLongErr = strings.Repeat("x", 600)
)

func TestClient_KubeJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		down    bool
		want    string
		wantErr string
	}{
		{name: "ok", routes: map[string]kubeRoute{"GET /test": {body: `{"ok":true}`}}, want: `{"ok":true}`},
		{name: "unconfigured", wantErr: "kube api server not configured"},
		{name: "transport failure", routes: kubeAll404, down: true, wantErr: "kube GET /test request failed"},
		{name: "non-2xx truncates body", routes: map[string]kubeRoute{"*": {status: 500, body: kubeLongErr}}, wantErr: "returned status 500: " + strings.Repeat("x", 512) + "..."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, tc.down).KubeJSON(context.Background(), http.MethodGet, "/test", "", nil)
			checkKubeErr(t, err, tc.wantErr)
			if string(got) != tc.want {
				t.Errorf("KubeJSON() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTruncateKubeBody(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"short body kept", "short", "short"},
		{"exactly 512 kept", strings.Repeat("a", 512), strings.Repeat("a", 512)},
		{"over 512 truncated", strings.Repeat("a", 600), strings.Repeat("a", 512) + "..."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := truncateKubeBody([]byte(tc.in)); got != tc.want {
				t.Errorf("truncateKubeBody() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsKubeNotFound(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"404", errors.New("returned status 404: not found"), true},
		{"500", errors.New("returned status 500: error"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isKubeNotFound(tc.err); got != tc.want {
				t.Errorf("isKubeNotFound() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompareKustomizationRefs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b model.FluxKustomizationRef
		want int
	}{
		{"namespace orders first", model.FluxKustomizationRef{Namespace: "a", Name: "2"}, model.FluxKustomizationRef{Namespace: "b", Name: "1"}, -1},
		{"name breaks namespace tie", model.FluxKustomizationRef{Namespace: "a", Name: "1"}, model.FluxKustomizationRef{Namespace: "a", Name: "2"}, -1},
		{"equal", model.FluxKustomizationRef{Namespace: "a", Name: "1"}, model.FluxKustomizationRef{Namespace: "a", Name: "1"}, 0},
		{"greater", model.FluxKustomizationRef{Namespace: "b", Name: "1"}, model.FluxKustomizationRef{Namespace: "a", Name: "1"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := compareKustomizationRefs(tc.a, tc.b); got != tc.want {
				t.Errorf("compareKustomizationRefs() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCleanRevisionSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, rev, want string
	}{
		{"branch at sha1", "main@sha1:1234567890abcdef", "1234567890abcdef"},
		{"branch at bare sha", "main@abc123", "abc123"},
		{"tag", "tag:v1.0.0", "v1.0.0"},
		{"bare trimmed", "  abc123  ", "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CleanRevisionSHA(tc.rev); got != tc.want {
				t.Errorf("CleanRevisionSHA(%q) = %q, want %q", tc.rev, got, tc.want)
			}
		})
	}
}

func TestKustomizationsForImages(t *testing.T) {
	t.Parallel()
	coreLabels := map[string]string{"kustomize.toolkit.fluxcd.io/name": "core", "kustomize.toolkit.fluxcd.io/namespace": "flux-system"}
	tests := []struct {
		name      string
		workloads []model.WorkloadImageRef
		images    []string
		want      []model.FluxKustomizationRef
	}{
		{name: "no workloads", images: []string{"aerial-synapse"}},
		{name: "matching workload", workloads: []model.WorkloadImageRef{{Labels: coreLabels, Images: []string{"ghcr.io/example/aerial-synapse:latest"}}}, images: []string{"aerial-synapse"}, want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "core"}}},
		{name: "terminating workload excluded", workloads: []model.WorkloadImageRef{{Labels: coreLabels, Images: []string{"ghcr.io/example/aerial-synapse:latest"}, DeletionTimestamp: "2026-11-20T18:00:00Z"}}, images: []string{"aerial-synapse"}},
		{name: "duplicate workloads deduped", workloads: []model.WorkloadImageRef{{Labels: coreLabels, Images: []string{"aerial-synapse@sha256:abc"}}, {Labels: coreLabels, Images: []string{"aerial-synapse"}}}, images: []string{" aerial-synapse ", ""}, want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "core"}}},
		{name: "unlabelled workload skipped", workloads: []model.WorkloadImageRef{{Images: []string{"aerial-synapse"}}}, images: []string{"aerial-synapse"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := KustomizationsForImages(tc.workloads, tc.images); !slices.Equal(got, tc.want) {
				t.Errorf("KustomizationsForImages() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecodeWorkloadRollouts(t *testing.T) {
	t.Parallel()
	deployment := `{"items":[{"metadata":{"name":"test-deploy","namespace":"default","uid":"u1","generation":2,"deletionTimestamp":"2026-11-20T18:00:00Z","finalizers":["deployments.restate.dev"],"managedFields":[{"time":"2026-11-18T00:00:00Z"}]},` +
		`"spec":{"template":{"spec":{"containers":[{"image":"img1"}],"initContainers":[{"image":"init1"}]}},"compute":{"image":"comp1"}},"status":{"observedGeneration":2,"replicas":1,"conditions":[{"type":"Ready","status":"True"}]}}]}`
	deployed := model.WorkloadRollout{
		Kind: "Deployment", UID: "u1", Namespace: "default", Name: "test-deploy", Images: []string{"img1", "init1", "comp1"}, ChangedAt: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC),
		Generation: 2, ObservedGeneration: 2, Replicas: 1, StatusReplicas: 1, ReadyConditionTrue: true, DeletionTimestamp: "2026-11-20T18:00:00Z", Finalizers: []string{"deployments.restate.dev"},
	}
	computeReplicas := `{"items":[{"metadata":{"name":"restate-comp","namespace":"default","generation":1},"spec":{"compute":{"replicas":3}},"status":{"observedGeneration":1}}]}`
	tests := []struct {
		name, kind, body string
		want             []model.WorkloadRollout
		wantErr          string
	}{
		{name: "deployment images, deletion and finalizers", kind: "Deployment", body: deployment, want: []model.WorkloadRollout{deployed}},
		{name: "compute replicas used when spec replicas absent", kind: "RestateDeployment", body: computeReplicas, want: []model.WorkloadRollout{{Kind: "RestateDeployment", Namespace: "default", Name: "restate-comp", Generation: 1, ObservedGeneration: 1, Replicas: 3}}},
		{name: "malformed", kind: "Deployment", body: kubeBadJSON, wantErr: kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := DecodeWorkloadRollouts(tc.kind, []byte(tc.body))
			checkKubeErr(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("DecodeWorkloadRollouts() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDecodeWorkloadImageRefs(t *testing.T) {
	t.Parallel()
	full := `{"items":[{"metadata":{"labels":{"app":"test"},"deletionTimestamp":" 2026-11-20T18:00:00Z "},"spec":{"template":{"spec":{"containers":[{"image":"pod1"}],"initContainers":[{"image":"podinit1"}]}},` +
		`"jobTemplate":{"spec":{"template":{"spec":{"containers":[{"image":"job1"}],"initContainers":[{"image":"jobinit1"}]}}}},"compute":{"image":"comp2"}}}]}`
	fullWant := []model.WorkloadImageRef{{Labels: map[string]string{"app": "test"}, Images: []string{"pod1", "podinit1", "job1", "jobinit1", "comp2"}, DeletionTimestamp: "2026-11-20T18:00:00Z"}}
	tests := []struct {
		name, body string
		want       []model.WorkloadImageRef
		wantErr    string
	}{
		{name: "pod and job templates with compute", body: full, want: fullWant},
		{name: "job init containers only", body: `{"items":[{"spec":{"jobTemplate":{"spec":{"template":{"spec":{"initContainers":[{"image":"job-init-img"}]}}}}}}]}`, want: []model.WorkloadImageRef{{Images: []string{"job-init-img"}}}},
		{name: "malformed", body: kubeBadJSON, wantErr: kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := DecodeWorkloadImageRefs([]byte(tc.body))
			checkKubeErr(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("DecodeWorkloadImageRefs() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestClient_ListKustomizationConfigMaps(t *testing.T) {
	t.Parallel()
	ref := model.FluxKustomizationRef{Namespace: "flux-system", Name: "core"}
	changed := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	routes := map[string]kubeRoute{"GET /api/v1/configmaps": {body: `{"items":[{"metadata":{"name":"cfg1","namespace":"flux-system","managedFields":[` +
		`{"time":"2026-11-18T11:00:00Z"},{"time":"2026-11-18T12:00:00Z"},{"time":"2026-11-18T13:00:00Z","subresource":"status"}]}}]}`}}
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		want    []model.ConfigMapChange
		wantErr string
	}{
		{name: "latest managed field wins", routes: routes, want: []model.ConfigMapChange{{Namespace: "flux-system", Name: "cfg1", ChangedAt: changed}}},
		{name: "unconfigured", wantErr: "list core configmaps: kube api server not configured"},
		{name: "server error", routes: kubeAll500, wantErr: "list core configmaps: kube GET /api/v1/configmaps?"},
		{name: "malformed", routes: kubeAllBad, wantErr: "decode core configmaps: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).ListKustomizationConfigMaps(context.Background(), ref)
			checkKubeErr(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ListKustomizationConfigMaps() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestClient_ListKustomizationWorkloads(t *testing.T) {
	t.Parallel()
	ref := model.FluxKustomizationRef{Namespace: "flux-system", Name: "core"}
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		wantLen int
		wantErr string
	}{
		{name: "missing kinds skipped", routes: map[string]kubeRoute{"GET /apis/apps/v1/deployments": {body: `{"items":[{"metadata":{"name":"a"}}]}`}}, wantLen: 1},
		{name: "unconfigured", wantErr: "list core workloads: kube api server not configured"},
		{name: "server error", routes: kubeAll500, wantErr: "list core workloads: kube GET /apis/apps/v1/deployments?"},
		{name: "malformed", routes: kubeAllBad, wantErr: "decode /apis/apps/v1/deployments: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).ListKustomizationWorkloads(context.Background(), ref)
			checkKubeErr(t, err, tc.wantErr)
			if len(got) != tc.wantLen {
				t.Errorf("ListKustomizationWorkloads() = %+v, want %d workloads", got, tc.wantLen)
			}
		})
	}
}

func TestClient_ListFluxWorkloadsVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		want    string
		wantErr string
	}{
		{name: "resource version", routes: map[string]kubeRoute{"GET /apis/apps/v1/deployments": {body: `{"metadata":{"resourceVersion":"456"},"items":[]}`}}, want: "456"},
		{name: "unconfigured", wantErr: "list flux workloads: kube api server not configured"},
		{name: "server error", routes: kubeAll500, wantErr: "list flux workloads: kube GET /apis/apps/v1/deployments?"},
		{name: "malformed", routes: kubeAllBad, wantErr: "decode /apis/apps/v1/deployments: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).ListFluxWorkloadsVersion(context.Background(), "/apis/apps/v1/deployments")
			checkKubeErr(t, err, tc.wantErr)
			if got != tc.want {
				t.Errorf("ListFluxWorkloadsVersion() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClient_ListWorkloadImages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		want    []model.WorkloadImageRef
		wantErr string
	}{
		{name: "missing kinds skipped", routes: map[string]kubeRoute{"GET /apis/batch/v1/jobs": {body: `{"items":[{"spec":{"template":{"spec":{"containers":[{"image":"job1"}]}}}}]}`}}, want: []model.WorkloadImageRef{{Images: []string{"job1"}}}},
		{name: "unconfigured", wantErr: "list workloads: kube api server not configured"},
		{name: "server error", routes: kubeAll500, wantErr: "list workloads: kube GET /apis/apps/v1/deployments returned status 500"},
		{name: "malformed", routes: kubeAllBad, wantErr: "decode /apis/apps/v1/deployments: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).ListWorkloadImages(context.Background())
			checkKubeErr(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ListWorkloadImages() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestClient_DiscoverImageKustomizations(t *testing.T) {
	t.Parallel()
	deployments := map[string]kubeRoute{"GET /apis/apps/v1/deployments": {body: `{"items":[{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"aerial-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/example/aerial-synapse:abc"}]}}}}]}`}}
	fallback := map[string]kubeRoute{kubeKustPath: {body: `{"items":[{"metadata":{"name":"alpha","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"Secret","name":"x"},{"kind":"ConfigMap","name":"aerial-image-tags"}]}}}]}`}}
	tests := []struct {
		name    string
		images  []string
		routes  map[string]kubeRoute
		want    []model.FluxKustomizationRef
		wantErr string
	}{
		{name: "empty image names skips kube", routes: kubeAll500},
		{name: "no kube configured skips kube", images: []string{"aerial-synapse"}},
		{name: "workload label discovered", images: []string{"aerial-synapse"}, routes: deployments, want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}}},
		{name: "fallback to configmap substitute", images: []string{"aerial-haro"}, routes: fallback, want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "alpha"}}},
		{name: "workload list error", images: []string{"aerial-synapse"}, routes: kubeAll500, wantErr: "list workloads: kube GET /apis/apps/v1/deployments returned status 500"},
		{name: "fallback list error", images: []string{"aerial-haro"}, routes: map[string]kubeRoute{kubeKustPath: {status: 500}}, wantErr: "list kustomizations: kube GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations returned status 500"},
		{name: "fallback malformed", images: []string{"aerial-haro"}, routes: map[string]kubeRoute{kubeKustPath: {body: kubeBadJSON}}, wantErr: "decode kustomizations: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).DiscoverImageKustomizations(context.Background(), tc.images)
			checkKubeErr(t, err, tc.wantErr)
			if !slices.Equal(got, tc.want) {
				t.Errorf("DiscoverImageKustomizations() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClient_ReconcileKustomization(t *testing.T) {
	t.Parallel()
	ref := model.FluxKustomizationRef{Namespace: "flux-system", Name: "core"}
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		wantErr string
	}{
		{name: "patched", routes: map[string]kubeRoute{"PATCH /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/core": {body: `{}`}}},
		{name: "unconfigured", wantErr: "reconcile kustomization flux-system/core: kube api server not configured"},
		{name: "server error", routes: kubeAll500, wantErr: "reconcile kustomization flux-system/core: kube PATCH /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/core returned status 500"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := newKubeRouteClient(t, tc.routes, false).ReconcileKustomization(context.Background(), ref, "2026-11-18T00:00:00Z")
			checkKubeErr(t, err, tc.wantErr)
		})
	}
}

func TestClient_ListKustomizations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		routes  map[string]kubeRoute
		wantRV  string
		wantLen int
		wantErr string
	}{
		{name: "resource version", routes: map[string]kubeRoute{kubeKustPath: {body: `{"metadata":{"resourceVersion":"123"},"items":[{"metadata":{"name":"core"}}]}`}}, wantRV: "123", wantLen: 1},
		{name: "unconfigured", wantErr: "list kustomizations: kube api server not configured"},
		{name: "server error", routes: kubeAll500, wantErr: "list kustomizations: kube GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations returned status 500"},
		{name: "malformed", routes: kubeAllBad, wantErr: "decode kustomization list: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, rv, err := newKubeRouteClient(t, tc.routes, false).ListKustomizations(context.Background(), "flux-system")
			checkKubeErr(t, err, tc.wantErr)
			if rv != tc.wantRV || len(got) != tc.wantLen {
				t.Errorf("ListKustomizations() = (%+v, %q), want %d items at %q", got, rv, tc.wantLen, tc.wantRV)
			}
		})
	}
}

func TestClient_WatchKustomizations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		routes   map[string]kubeRoute
		down     bool
		wantBody string
		wantErr  string
	}{
		{name: "stream opened", routes: map[string]kubeRoute{kubeKustPath: {body: `{"type":"ADDED"}`}}, wantBody: `{"type":"ADDED"}`},
		{name: "gone expires watch", routes: map[string]kubeRoute{"*": {status: http.StatusGone}}, wantErr: "kube watch resourceVersion expired"},
		{name: "server error truncates body", routes: map[string]kubeRoute{"*": {status: 500, body: kubeLongErr}}, wantErr: "kustomizations returned status 500: " + strings.Repeat("x", 512) + "..."},
		{name: "unconfigured", wantErr: "kube api server not configured"},
		{name: "transport failure", routes: kubeAll404, down: true, wantErr: "kube watch /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations request failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stream, err := newKubeRouteClient(t, tc.routes, tc.down).WatchKustomizations(context.Background(), "flux-system", "123")
			checkKubeErr(t, err, tc.wantErr)
			if err != nil {
				return
			}
			defer stream.Close()
			if body, _ := io.ReadAll(stream); string(body) != tc.wantBody {
				t.Errorf("stream = %q, want %q", body, tc.wantBody)
			}
		})
	}
}

func TestClient_WatchFluxWorkloads(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		routes   map[string]kubeRoute
		wantBody string
		wantErr  string
	}{
		{name: "stream opened", routes: map[string]kubeRoute{"GET /apis/apps/v1/deployments": {body: `{"type":"MODIFIED"}`}}, wantBody: `{"type":"MODIFIED"}`},
		{name: "unconfigured", wantErr: "kube api server not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stream, err := newKubeRouteClient(t, tc.routes, false).WatchFluxWorkloads(context.Background(), "/apis/apps/v1/deployments", "456")
			checkKubeErr(t, err, tc.wantErr)
			if err != nil {
				return
			}
			defer stream.Close()
			if body, _ := io.ReadAll(stream); string(body) != tc.wantBody {
				t.Errorf("stream = %q, want %q", body, tc.wantBody)
			}
		})
	}
}

func TestClient_ResolveFluxSourceRepo(t *testing.T) {
	t.Parallel()
	routes := map[string]kubeRoute{
		kubeGitRepo:                   {body: `{"spec":{"url":"https://github.com/example/aerial.git"}}`},
		kubeKustPath + "/aerial-core": {body: `{"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}}`},
		"GET /apis/source.toolkit.fluxcd.io/v1/namespaces/apps/gitrepositories/stack": {body: `{"spec":{"url":"git@github.com:acme/stack.git"}}`},
		kubeKustPath + "/stack":  {body: `{"spec":{"sourceRef":{"kind":"GitRepository","name":"stack","namespace":"apps"}}}`},
		kubeKustPath + "/bucket": {body: `{"spec":{"sourceRef":{"kind":"Bucket","name":"b"}}}`},
	}
	tests := []struct {
		name    string
		obj     model.FluxInvolvedObject
		routes  map[string]kubeRoute
		want    string
		wantErr string
	}{
		{name: "git repository", obj: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"}, routes: routes, want: "example/aerial"},
		{name: "kustomization defaults namespace", obj: model.FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: routes, want: "example/aerial"},
		{name: "kustomization cross namespace source", obj: model.FluxInvolvedObject{Kind: "Kustomization", Name: "stack", Namespace: "flux-system"}, routes: routes, want: "acme/stack"},
		{name: "bucket source", obj: model.FluxInvolvedObject{Kind: "Kustomization", Name: "bucket", Namespace: "flux-system"}, routes: routes},
		{name: "helm release", obj: model.FluxInvolvedObject{Kind: "HelmRelease", Name: "hr", Namespace: "flux-system"}, routes: routes},
		{name: "git repository without name", obj: model.FluxInvolvedObject{Kind: "GitRepository", Name: "  "}, routes: kubeAll500},
		{name: "kustomization get error", obj: model.FluxInvolvedObject{Kind: "Kustomization", Name: "app"}, routes: kubeAll500, wantErr: "get kustomization flux-system/app: kube GET"},
		{name: "kustomization malformed", obj: model.FluxInvolvedObject{Kind: "Kustomization", Name: "app"}, routes: kubeAllBad, wantErr: "decode kustomization flux-system/app: " + kubeBadJSONErr},
		{name: "git repository get error", obj: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"}, routes: kubeAll500, wantErr: "get gitrepository flux-system/aerial: kube GET"},
		{name: "git repository malformed", obj: model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"}, routes: kubeAllBad, wantErr: "decode gitrepository flux-system/aerial: " + kubeBadJSONErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).ResolveFluxSourceRepo(context.Background(), tc.obj)
			checkKubeErr(t, err, tc.wantErr)
			if got != tc.want {
				t.Errorf("ResolveFluxSourceRepo() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClient_DiscoverRepoKustomizations(t *testing.T) {
	t.Parallel()
	kusts := kubeRoute{body: `{"items":[{"metadata":{"name":"core"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}},{"metadata":{"name":"also"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}},{"metadata":{"name":"suspended"},"spec":{"suspend":true,"sourceRef":{"kind":"GitRepository","name":"aerial"}}}]}`}
	routes := map[string]kubeRoute{kubeKustPath: kusts, kubeGitRepo: {body: `{"spec":{"url":"ssh://git@github.com/example/aerial"}}`}}
	tests := []struct {
		name    string
		repo    string
		routes  map[string]kubeRoute
		want    []model.FluxKustomizationRef
		wantErr string
	}{
		{name: "matching sources sorted, suspended skipped", repo: " EXAMPLE/aerial ", routes: routes, want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "also"}, {Namespace: "flux-system", Name: "core"}}},
		{name: "empty repo", repo: " ", routes: routes},
		{name: "unconfigured", repo: "example/aerial"},
		{name: "list error", repo: "example/aerial", routes: kubeAll500, wantErr: "list kustomizations: kube GET"},
		{name: "source lookup error", repo: "example/aerial", routes: map[string]kubeRoute{kubeKustPath: kusts, kubeGitRepo: {status: 500}}, wantErr: "get gitrepository flux-system/aerial: kube GET"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newKubeRouteClient(t, tc.routes, false).DiscoverRepoKustomizations(context.Background(), tc.repo)
			checkKubeErr(t, err, tc.wantErr)
			if !slices.Equal(got, tc.want) {
				t.Errorf("DiscoverRepoKustomizations() = %v, want %v", got, tc.want)
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
		{"https with .git", "https://github.com/acme/widgets.git", "acme/widgets"},
		{"https trailing slash", "https://github.com/acme/widgets/", "acme/widgets"},
		{"ssh scheme", "ssh://git@github.com/acme/widgets", "acme/widgets"},
		{"scp-like", "git@github.com:acme/widgets.git", "acme/widgets"},
		{"host only", "https://github.com", ""},
		{"scp-like without path", "git@github.com:", ""},
		{"single path segment", "https://github.com/widgets", ""},
		{"not a url", "invalid", ""},
		{"empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := GitRepoSlug(tc.url); got != tc.want {
				t.Errorf("GitRepoSlug(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}
