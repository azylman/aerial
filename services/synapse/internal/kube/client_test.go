package kube

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

func TestClient_DiscoverImageKustomizations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	deploymentList := `{"items":[{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"aerial-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}}}]}`
	kustomizationList := `{"items":[{"metadata":{"name":"alpha","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"aerial-image-tags"}]}}}]}`
	emptyList := `{"items":[]}`

	tests := []struct {
		name       string
		imageNames []string
		routes     map[string]string
		noKube     bool
		want       []model.FluxKustomizationRef
		wantErr    bool
	}{
		{
			name:       "empty image names skips kube",
			imageNames: nil,
			want:       nil,
		},
		{
			name:       "no kube configured skips kube",
			imageNames: []string{"aerial-synapse"},
			noKube:     true,
			want:       nil,
		},
		{
			name:       "workload label discovered",
			imageNames: []string{"aerial-synapse"},
			routes: map[string]string{
				"GET /apis/apps/v1/deployments":                    deploymentList,
				"GET /apis/apps/v1/statefulsets":                   emptyList,
				"GET /apis/apps/v1/daemonsets":                     emptyList,
				"GET /apis/batch/v1/jobs":                          emptyList,
				"GET /apis/batch/v1/cronjobs":                      emptyList,
				"GET /apis/restate.dev/v1beta1/restatedeployments": emptyList,
				"GET /apis/restate.dev/v1/restateclusters":         emptyList,
			},
			want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}},
		},
		{
			name:       "fallback to configmap substitute",
			imageNames: []string{"aerial-haro"},
			routes: map[string]string{
				"GET /apis/apps/v1/deployments":                                                  emptyList,
				"GET /apis/apps/v1/statefulsets":                                                 emptyList,
				"GET /apis/apps/v1/daemonsets":                                                   emptyList,
				"GET /apis/batch/v1/jobs":                                                        emptyList,
				"GET /apis/batch/v1/cronjobs":                                                    emptyList,
				"GET /apis/restate.dev/v1beta1/restatedeployments":                               emptyList,
				"GET /apis/restate.dev/v1/restateclusters":                                       emptyList,
				"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations": kustomizationList,
			},
			want: []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "alpha"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := r.Method + " " + r.URL.Path
				if body, ok := tc.routes[key]; ok {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(body))
					return
				}
				http.NotFound(w, r)
			}))
			defer ts.Close()

			c := NewClient(ts.Client())
			if !tc.noKube {
				c.SetKubeConfig(ts.URL, "token")
			}

			refs, err := c.DiscoverImageKustomizations(ctx, tc.imageNames)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DiscoverImageKustomizations err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !slices.Equal(refs, tc.want) {
				t.Errorf("refs = %v, want %v", refs, tc.want)
			}
		})
	}
}

func TestClient_ReconcileAndWatchKustomization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Query().Get("watch") == "1" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"type":"ADDED"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"123"},"items":[]}`))
	}))
	defer ts.Close()

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "token")

	ref := model.FluxKustomizationRef{Namespace: "flux-system", Name: "core"}
	if err := c.ReconcileKustomization(ctx, ref, "2026-11-18T00:00:00Z"); err != nil {
		t.Fatalf("ReconcileKustomization err = %v", err)
	}

	obs, rv, err := c.ListKustomizations(ctx, "flux-system")
	if err != nil || rv != "123" || len(obs) != 0 {
		t.Fatalf("ListKustomizations = (%v, %q, %v)", obs, rv, err)
	}

	stream, err := c.WatchKustomizations(ctx, "flux-system", "123")
	if err != nil {
		t.Fatalf("WatchKustomizations err = %v", err)
	}
	_ = stream.Close()
}

func TestClient_WatchKube_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 410 Gone returns ErrWatchExpired
	tsGone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer tsGone.Close()

	cGone := NewClient(tsGone.Client())
	cGone.SetKubeConfig(tsGone.URL, "tok")
	_, err := cGone.WatchKustomizations(ctx, "flux-system", "old")
	if !errors.Is(err, ErrWatchExpired) {
		t.Errorf("expected ErrWatchExpired, got %v", err)
	}

	// 500 error returns formatted error with long body to test truncateKubeBody
	longBody := strings.Repeat("x", 600)
	tsErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(longBody))
	}))
	defer tsErr.Close()

	cErr := NewClient(tsErr.Client())
	cErr.SetKubeConfig(tsErr.URL, "tok")
	_, err = cErr.WatchKustomizations(ctx, "flux-system", "bad")
	if err == nil {
		t.Errorf("expected error on 500 watch")
	}

	// unconfigured kube returns error
	cUnconf := NewClient(nil)
	_, err = cUnconf.WatchKustomizations(ctx, "flux-system", "1")
	if err == nil {
		t.Errorf("expected error on unconfigured kube")
	}
	if err = cUnconf.ReconcileKustomization(ctx, model.FluxKustomizationRef{Name: "c"}, "t"); err == nil {
		t.Errorf("expected error on unconfigured kube for ReconcileKustomization")
	}
}

func TestClient_ListAndWatchFluxWorkloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "1" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"type":"MODIFIED"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"456"},"items":[]}`))
	}))
	defer ts.Close()

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "token")

	rv, err := c.ListFluxWorkloadsVersion(ctx, "/apis/apps/v1/deployments")
	if err != nil || rv != "456" {
		t.Fatalf("ListFluxWorkloadsVersion = (%q, %v)", rv, err)
	}

	stream, err := c.WatchFluxWorkloads(ctx, "/apis/apps/v1/deployments", "456")
	if err != nil {
		t.Fatalf("WatchFluxWorkloads err = %v", err)
	}
	_ = stream.Close()
}

func TestClient_ListKustomizationConfigMaps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	nowStr := now.Format(time.RFC3339)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"cfg1","namespace":"flux-system","managedFields":[{"time":"` + nowStr + `"}]}}]}`))
	}))
	defer ts.Close()

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "token")

	changes, err := c.ListKustomizationConfigMaps(ctx, model.FluxKustomizationRef{Namespace: "flux-system", Name: "core"})
	if err != nil {
		t.Fatalf("ListKustomizationConfigMaps err = %v", err)
	}
	if len(changes) != 1 || changes[0].Name != "cfg1" {
		t.Fatalf("changes = %v, want 1 change for cfg1", changes)
	}
}

func TestClient_ResolveFluxSourceRepo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	routes := map[string]string{
		"GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial":        `{"spec":{"url":"https://github.com/azylman/aerial.git"}}`,
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/aerial-core": `{"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}}`,
		"GET /apis/source.toolkit.fluxcd.io/v1/namespaces/apps/gitrepositories/stack":                `{"spec":{"url":"git@github.com:acme/stack.git"}}`,
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/stack":       `{"spec":{"sourceRef":{"kind":"GitRepository","name":"stack","namespace":"apps"}}}`,
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/bucket":      `{"spec":{"sourceRef":{"kind":"Bucket","name":"b"}}}`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		if b, ok := routes[key]; ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(b))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "token")

	repo, err := c.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"})
	if err != nil || repo != "azylman/aerial" {
		t.Fatalf("ResolveFluxSourceRepo(GitRepository) = (%q, %v), want azylman/aerial", repo, err)
	}

	repoK, err := c.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core", Namespace: "flux-system"})
	if err != nil || repoK != "azylman/aerial" {
		t.Fatalf("ResolveFluxSourceRepo(Kustomization) = (%q, %v), want azylman/aerial", repoK, err)
	}

	repoStack, err := c.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "Kustomization", Name: "stack", Namespace: "flux-system"})
	if err != nil || repoStack != "acme/stack" {
		t.Fatalf("ResolveFluxSourceRepo(CrossNamespace) = (%q, %v), want acme/stack", repoStack, err)
	}

	bucketRepo, err := c.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "Kustomization", Name: "bucket", Namespace: "flux-system"})
	if err != nil || bucketRepo != "" {
		t.Fatalf("ResolveFluxSourceRepo(Bucket) = (%q, %v), want empty", bucketRepo, err)
	}

	otherRepo, err := c.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "HelmRelease", Name: "hr", Namespace: "flux-system"})
	if err != nil || otherRepo != "" {
		t.Fatalf("ResolveFluxSourceRepo(HelmRelease) = (%q, %v), want empty", otherRepo, err)
	}
}

func TestClient_DiscoverRepoKustomizations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	routes := map[string]string{
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations":        `{"items":[{"metadata":{"name":"core"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}},{"metadata":{"name":"suspended"},"spec":{"suspend":true,"sourceRef":{"kind":"GitRepository","name":"aerial"}}}]}`,
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/agent-gamma-system/kustomizations": `{"items":[]}`,
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/agent-alpha-system/kustomizations": `{"items":[]}`,
		"GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial":   `{"spec":{"url":"ssh://git@github.com/azylman/aerial"}}`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		if b, ok := routes[key]; ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(b))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "token")

	refs, err := c.DiscoverRepoKustomizations(ctx, "azylman/aerial")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []model.FluxKustomizationRef{{Namespace: "flux-system", Name: "core"}}
	if !slices.Equal(refs, want) {
		t.Errorf("refs = %v, want %v", refs, want)
	}

	// empty repo or unconfigured kube returns nil, nil
	if r, err := c.DiscoverRepoKustomizations(ctx, ""); err != nil || r != nil {
		t.Errorf("expected nil for empty repo")
	}
	cUnconf := NewClient(nil)
	if r, err := cUnconf.DiscoverRepoKustomizations(ctx, "aerial"); err != nil || r != nil {
		t.Errorf("expected nil for unconfigured kube")
	}
}

func TestPureDecoders(t *testing.T) {
	t.Parallel()

	// CleanRevisionSHA
	shaTests := []struct {
		rev  string
		want string
	}{
		{rev: "main@sha1:1234567890abcdef", want: "1234567890abcdef"},
		{rev: "tag:v1.0.0", want: "v1.0.0"},
		{rev: "  abc123  ", want: "abc123"},
	}
	for _, tc := range shaTests {
		if got := CleanRevisionSHA(tc.rev); got != tc.want {
			t.Errorf("CleanRevisionSHA(%q) = %q, want %q", tc.rev, got, tc.want)
		}
	}

	// KustomizationsForImages
	workloads := []model.WorkloadImageRef{
		{
			Labels: map[string]string{
				"kustomize.toolkit.fluxcd.io/name":      "core",
				"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
			},
			Images: []string{"ghcr.io/azylman/aerial-synapse:latest"},
		},
		{
			Labels: map[string]string{
				"kustomize.toolkit.fluxcd.io/name":      "terminating-app",
				"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
			},
			Images:            []string{"ghcr.io/azylman/aerial-synapse:latest"},
			DeletionTimestamp: "2026-11-20T18:00:00Z",
		},
	}
	refs := KustomizationsForImages(workloads, []string{"aerial-synapse"})
	if len(refs) != 1 || refs[0].Name != "core" {
		t.Errorf("KustomizationsForImages = %v, want 1 core ref (excluding terminating)", refs)
	}

	// CompareKustomizationRefs
	ref1 := model.FluxKustomizationRef{Namespace: "a", Name: "1"}
	ref2 := model.FluxKustomizationRef{Namespace: "b", Name: "1"}
	ref3 := model.FluxKustomizationRef{Namespace: "a", Name: "2"}
	if compareKustomizationRefs(ref1, ref2) >= 0 {
		t.Errorf("expected ref1 < ref2")
	}
	if compareKustomizationRefs(ref1, ref3) >= 0 {
		t.Errorf("expected ref1 < ref3")
	}

	// isKubeNotFound
	if !isKubeNotFound(errors.New("returned status 404: not found")) {
		t.Errorf("expected isKubeNotFound to report true")
	}
	if isKubeNotFound(errors.New("returned status 500: error")) {
		t.Errorf("expected isKubeNotFound to report false")
	}
}

func TestDecodeWorkloadRolloutsAndImages(t *testing.T) {
	t.Parallel()

	workloadListJSON := []byte(`{"items":[{
		"metadata":{
			"name":"test-deploy",
			"namespace":"default",
			"uid":"u1",
			"generation":2,
			"deletionTimestamp":"2026-11-20T18:00:00Z",
			"finalizers":["deployments.restate.dev"],
			"managedFields":[{"time":"2026-11-18T00:00:00Z"}]
		},
		"spec":{
			"template":{"spec":{"containers":[{"image":"img1"}],"initContainers":[{"image":"init1"}]}},
			"compute":{"image":"comp1"}
		},
		"status":{
			"observedGeneration":2,
			"replicas":1,
			"conditions":[{"type":"Ready","status":"True"}]
		}
	}]}`)

	workloads, err := DecodeWorkloadRollouts("Deployment", workloadListJSON)
	if err != nil {
		t.Fatalf("DecodeWorkloadRollouts err = %v", err)
	}
	if len(workloads) != 1 || !workloads[0].ReadyConditionTrue || len(workloads[0].Images) != 3 || workloads[0].DeletionTimestamp != "2026-11-20T18:00:00Z" || len(workloads[0].Finalizers) != 1 || workloads[0].Finalizers[0] != "deployments.restate.dev" {
		t.Fatalf("unexpected decoded workload: %+v", workloads)
	}

	jobListJSON := []byte(`{"items":[{
		"metadata":{
			"labels":{"app":"test"},
			"deletionTimestamp":" 2026-11-20T18:00:00Z "
		},
		"spec":{
			"jobTemplate":{"spec":{"template":{"spec":{"containers":[{"image":"job1"}],"initContainers":[{"image":"jobinit1"}]}}}},
			"compute":{"image":"comp2"}
		}
	}]}`)
	imageRefs, err := DecodeWorkloadImageRefs(jobListJSON)
	if err != nil {
		t.Fatalf("DecodeWorkloadImageRefs err = %v", err)
	}
	if len(imageRefs) != 1 || len(imageRefs[0].Images) != 3 || imageRefs[0].DeletionTimestamp != "2026-11-20T18:00:00Z" {
		t.Fatalf("unexpected image refs: %+v", imageRefs)
	}
}

func TestDecoderErrors(t *testing.T) {
	t.Parallel()
	malformed := []byte(`{invalid`)

	if _, err := DecodeWorkloadRollouts("Deployment", malformed); err == nil {
		t.Errorf("expected error decoding malformed workload rollouts")
	}
	if _, err := DecodeWorkloadImageRefs(malformed); err == nil {
		t.Errorf("expected error decoding malformed workload image refs")
	}
	if _, err := DecodeConfigMapChanges(malformed); err == nil {
		t.Errorf("expected error decoding malformed config map changes")
	}
	if _, _, err := DecodeKustomizationList(malformed, "ns"); err == nil {
		t.Errorf("expected error decoding malformed kustomization list")
	}
	if _, err := DecodeImageTagKustomizations(malformed); err == nil {
		t.Errorf("expected error decoding malformed image tag kustomizations")
	}
}

func TestKubeClient_ErrorBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Unconfigured API server returns error
	cUnconf := NewClient(nil)
	if _, err := cUnconf.KubeJSON(ctx, http.MethodGet, "/path", "", nil); err == nil {
		t.Errorf("expected error from unconfigured KubeJSON")
	}
	if _, err := cUnconf.ListKustomizationConfigMaps(ctx, model.FluxKustomizationRef{Name: "c"}); err == nil {
		t.Errorf("expected error from unconfigured ListKustomizationConfigMaps")
	}
	if _, err := cUnconf.ListKustomizationWorkloads(ctx, model.FluxKustomizationRef{Name: "c"}); err == nil {
		t.Errorf("expected error from unconfigured ListKustomizationWorkloads")
	}
	if _, err := cUnconf.ListFluxWorkloadsVersion(ctx, "/path"); err == nil {
		t.Errorf("expected error from unconfigured ListFluxWorkloadsVersion")
	}
	if _, _, err := cUnconf.ListKustomizations(ctx, "ns"); err == nil {
		t.Errorf("expected error from unconfigured ListKustomizations")
	}
	if _, err := cUnconf.listWorkloadImages(ctx); err == nil {
		t.Errorf("expected error from unconfigured listWorkloadImages")
	}

	// 2. Server 500 errors
	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`internal server error`))
	}))
	defer ts500.Close()

	c500 := NewClient(ts500.Client())
	c500.SetKubeConfig(ts500.URL, "tok")

	if _, err := c500.ListKustomizationConfigMaps(ctx, model.FluxKustomizationRef{Name: "c"}); err == nil {
		t.Errorf("expected error from 500 ListKustomizationConfigMaps")
	}
	if _, err := c500.ListKustomizationWorkloads(ctx, model.FluxKustomizationRef{Name: "c"}); err == nil {
		t.Errorf("expected error from 500 ListKustomizationWorkloads")
	}
	if _, err := c500.ListFluxWorkloadsVersion(ctx, "/path"); err == nil {
		t.Errorf("expected error from 500 ListFluxWorkloadsVersion")
	}
	if _, _, err := c500.ListKustomizations(ctx, "ns"); err == nil {
		t.Errorf("expected error from 500 ListKustomizations")
	}
	if _, err := c500.listWorkloadImages(ctx); err == nil {
		t.Errorf("expected error from 500 listWorkloadImages")
	}

	// 3. Server returning malformed JSON
	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid-json`))
	}))
	defer tsBadJSON.Close()

	cBad := NewClient(tsBadJSON.Client())
	cBad.SetKubeConfig(tsBadJSON.URL, "tok")
	if _, err := cBad.ListKustomizationConfigMaps(ctx, model.FluxKustomizationRef{Name: "c"}); err == nil {
		t.Errorf("expected decode error from ListKustomizationConfigMaps")
	}
	if _, err := cBad.ListKustomizationWorkloads(ctx, model.FluxKustomizationRef{Name: "c"}); err == nil {
		t.Errorf("expected decode error from ListKustomizationWorkloads")
	}
	if _, err := cBad.ListFluxWorkloadsVersion(ctx, "/path"); err == nil {
		t.Errorf("expected decode error from ListFluxWorkloadsVersion")
	}
	if _, err := cBad.listWorkloadImages(ctx); err == nil {
		t.Errorf("expected decode error from listWorkloadImages")
	}
}

func TestKubeClient_UnhealthyEndpointErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. gitRepositorySlug with empty name or non-GitRepository
	c := NewClient(nil)
	slug, err := c.gitRepositorySlug(ctx, model.FluxSourceRef{Kind: "Bucket", Name: "b"})
	if err != nil || slug != "" {
		t.Errorf("expected empty slug for non-GitRepository")
	}
	slug, err = c.gitRepositorySlug(ctx, model.FluxSourceRef{Kind: "GitRepository", Name: ""})
	if err != nil || slug != "" {
		t.Errorf("expected empty slug for empty name")
	}

	// 2. DiscoverImageKustomizations error on listWorkloadImages
	tsErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer tsErr.Close()

	cErr := NewClient(tsErr.Client())
	cErr.SetKubeConfig(tsErr.URL, "tok")
	if _, discErr := cErr.DiscoverImageKustomizations(ctx, []string{"aerial-synapse"}); discErr == nil {
		t.Errorf("expected error from failed listWorkloadImages")
	}

	// 3. ReconcileKustomization patch error
	if recErr := cErr.ReconcileKustomization(ctx, model.FluxKustomizationRef{Name: "c"}, "t"); recErr == nil {
		t.Errorf("expected error from failed ReconcileKustomization")
	}

	// 4. ResolveFluxSourceRepo with GET failure and malformed JSON
	tsBadKust := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "gitrepositories") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid-kust`))
	}))
	defer tsBadKust.Close()

	cBadKust := NewClient(tsBadKust.Client())
	cBadKust.SetKubeConfig(tsBadKust.URL, "tok")
	if _, resErr := cBadKust.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "Kustomization", Name: "c"}); resErr == nil {
		t.Errorf("expected decode error from ResolveFluxSourceRepo")
	}
	if _, resErr := cBadKust.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "GitRepository", Name: "g"}); resErr == nil {
		t.Errorf("expected get error from ResolveFluxSourceRepo")
	}
}

func TestKubeClient_TransportFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	ts.Close() // closed immediately

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "tok")
	if _, err := c.KubeJSON(ctx, http.MethodGet, "/test", "", nil); err == nil {
		t.Errorf("expected transport error from KubeJSON")
	}
}

func TestKubeClient_FinalPushes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. gitRepositorySlug malformed JSON
	tsBadGit := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid-git`))
	}))
	defer tsBadGit.Close()

	cBadGit := NewClient(tsBadGit.Client())
	cBadGit.SetKubeConfig(tsBadGit.URL, "tok")
	if _, err := cBadGit.gitRepositorySlug(ctx, model.FluxSourceRef{Kind: "GitRepository", Name: "g"}); err == nil {
		t.Errorf("expected decode error from gitRepositorySlug")
	}

	// 2. DiscoverImageKustomizations with malformed fallback kustomization list
	emptyList := `{"items":[]}`
	tsBadFallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "kustomizations") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{invalid-kust`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(emptyList))
	}))
	defer tsBadFallback.Close()

	cBadFallback := NewClient(tsBadFallback.Client())
	cBadFallback.SetKubeConfig(tsBadFallback.URL, "tok")
	if _, err := cBadFallback.DiscoverImageKustomizations(ctx, []string{"unmatched-image"}); err == nil {
		t.Errorf("expected decode error from fallback DiscoverImageKustomizations")
	}
}

func TestKubeClient_PushOver95_DecodingAndEdgeCases(t *testing.T) {
	t.Parallel()

	// isKubeNotFound nil
	if isKubeNotFound(nil) {
		t.Errorf("expected isKubeNotFound(nil) to be false")
	}

	// truncateKubeBody > 512 bytes
	longBody := make([]byte, 600)
	for i := range longBody {
		longBody[i] = 'a'
	}
	truncated := truncateKubeBody(longBody)
	if !strings.HasSuffix(truncated, "...") || len(truncated) != 515 {
		t.Errorf("expected truncated string with length 515, got %d", len(truncated))
	}

	// DecodeWorkloadImageRefs with JobTemplate initContainers
	jobInitJSON := []byte(`{"items":[{
		"spec":{
			"jobTemplate":{"spec":{"template":{"spec":{"initContainers":[{"image":"job-init-img"}]}}}}
		}
	}]}`)
	jobRefs, err := DecodeWorkloadImageRefs(jobInitJSON)
	if err != nil || len(jobRefs) != 1 || len(jobRefs[0].Images) != 1 || jobRefs[0].Images[0] != "job-init-img" {
		t.Errorf("expected job init container decoded, got %+v (err: %v)", jobRefs, err)
	}

	// DecodeWorkloadRollouts with Compute.Replicas
	computeRepJSON := []byte(`{"items":[{
		"metadata":{"name":"restate-comp","namespace":"default","generation":1},
		"spec":{"compute":{"replicas":3}},
		"status":{"observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}]}
	}]}`)
	compRollouts, err := DecodeWorkloadRollouts("RestateDeployment", computeRepJSON)
	if err != nil || len(compRollouts) != 1 || compRollouts[0].Replicas != 3 {
		t.Errorf("expected compute replicas 3, got %+v (err: %v)", compRollouts, err)
	}
}

func TestKubeClient_PushOver95_ServerErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// ListKustomizationWorkloads decode error
	tsBadRollout := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid-rollout`))
	}))
	defer tsBadRollout.Close()

	cBad := NewClient(tsBadRollout.Client())
	cBad.SetKubeConfig(tsBadRollout.URL, "tok")
	if _, wlErr := cBad.ListKustomizationWorkloads(ctx, model.FluxKustomizationRef{Name: "c"}); wlErr == nil {
		t.Errorf("expected decode error from ListKustomizationWorkloads")
	}

	// ListKustomizationConfigMaps decode error
	if _, cmErr := cBad.ListKustomizationConfigMaps(ctx, model.FluxKustomizationRef{Name: "c"}); cmErr == nil {
		t.Errorf("expected decode error from ListKustomizationConfigMaps")
	}

	// ReconcileKustomization server error
	tsErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`internal server error`))
	}))
	defer tsErr.Close()

	cErr := NewClient(tsErr.Client())
	cErr.SetKubeConfig(tsErr.URL, "tok")
	if recErr := cErr.ReconcileKustomization(ctx, model.FluxKustomizationRef{Namespace: "flux-system", Name: "app"}, "now"); recErr == nil {
		t.Errorf("expected error from ReconcileKustomization on 500")
	}

	// DiscoverRepoKustomizations error on ListKustomizations
	if _, discErr := cErr.DiscoverRepoKustomizations(ctx, "azylman/aerial"); discErr == nil {
		t.Errorf("expected error from DiscoverRepoKustomizations on 500")
	}

	// ResolveFluxSourceRepo error on Kustomization 500
	if _, srcErr := cErr.ResolveFluxSourceRepo(ctx, model.FluxInvolvedObject{Kind: "Kustomization", Name: "app"}); srcErr == nil {
		t.Errorf("expected error from ResolveFluxSourceRepo on 500")
	}

	// watchKube network failure on closed server
	tsClosed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	tsClosed.Close()
	cClosed := NewClient(tsClosed.Client())
	cClosed.SetKubeConfig(tsClosed.URL, "tok")
	if _, watchErr := cClosed.watchKube(ctx, "/path", url.Values{}, "1"); watchErr == nil {
		t.Errorf("expected network failure from watchKube")
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
