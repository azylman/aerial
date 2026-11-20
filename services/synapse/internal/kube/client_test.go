package kube

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

	c1.SetFluxReceiverURL("http://notification-controller.flux-system.svc.cluster.local/")
	if c1.fluxReceiverURL != "" {
		t.Errorf("expected notification controller root URL to be rejected, got %q", c1.fluxReceiverURL)
	}
	c1.SetFluxReceiverURL("http://flux.lan/receiver")
	if c1.fluxReceiverURL != "http://flux.lan/receiver" {
		t.Errorf("fluxReceiverURL = %q, want http://flux.lan/receiver", c1.fluxReceiverURL)
	}
}

func TestIsFluxEventServerRootURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		url  string
		want bool
	}{
		{url: "invalid-url", want: false},
		{url: "http://notification-controller/", want: true},
		{url: "http://notification-controller.flux-system.svc.cluster.local/", want: true},
		{url: "http://notification-controller:8080/", want: true},
		{url: "http://notification-controller/path", want: false},
		{url: "http://other-service/", want: false},
	}
	for _, tc := range tests {
		if got := isFluxEventServerRootURL(tc.url); got != tc.want {
			t.Errorf("isFluxEventServerRootURL(%q) = %v, want %v", tc.url, got, tc.want)
		}
	}
}

func TestClient_DispatchGitPush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		setURL     bool
		statusCode int
		wantErr    bool
	}{
		{name: "no receiver url does nothing", setURL: false, wantErr: false},
		{name: "receiver url 200 ok", setURL: true, statusCode: http.StatusOK, wantErr: false},
		{name: "receiver url 500 error", setURL: true, statusCode: http.StatusInternalServerError, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			}))
			defer ts.Close()

			c := NewClient(ts.Client())
			if tc.setURL {
				c.SetFluxReceiverURL(ts.URL)
			}
			err := c.DispatchGitPush(ctx, model.GitPushEventRequest{Ref: "main", Commit: "c1"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("DispatchGitPush err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestClient_ForwardFluxReceiver_MarshalError(t *testing.T) {
	t.Parallel()
	c := NewClient(nil)
	c.SetFluxReceiverURL("http://localhost:8080")
	if err := c.forwardFluxReceiver(context.Background(), make(chan int)); err == nil {
		t.Errorf("expected marshal error")
	}
}

func TestClient_PromoteImageTag(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name         string
		req          model.ImageReadyEventRequest
		hasKube      bool
		hasFlux      bool
		kubeStatus   int
		fluxStatus   int
		wantWorkload string
		wantErr      bool
	}{
		{
			name: "workload none skipped",
			req: model.ImageReadyEventRequest{
				Repo:      "azylman/aerial",
				Image:     "ghcr.io/azylman/none:latest",
				CommitSHA: "abc",
			},
			hasKube:      true,
			wantWorkload: "",
		},
		{
			name: "promote via kube and flux success",
			req: model.ImageReadyEventRequest{
				Repo:      "azylman/aerial",
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
			},
			hasKube:      true,
			hasFlux:      true,
			kubeStatus:   http.StatusOK,
			fluxStatus:   http.StatusOK,
			wantWorkload: "aerial-synapse",
		},
		{
			name: "zero sha skips kube and promotes via flux",
			req: model.ImageReadyEventRequest{
				Repo:      "azylman/aerial",
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "0000000000000000000000000000000000000000",
			},
			hasKube:      true,
			hasFlux:      true,
			fluxStatus:   http.StatusOK,
			wantWorkload: "aerial-synapse",
		},
		{
			name: "kube error only",
			req: model.ImageReadyEventRequest{
				Repo:      "azylman/aerial",
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
			},
			hasKube:    true,
			kubeStatus: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name: "flux error only",
			req: model.ImageReadyEventRequest{
				Repo:      "azylman/aerial",
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
			},
			hasFlux:    true,
			fluxStatus: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name: "both kube and flux error",
			req: model.ImageReadyEventRequest{
				Repo:      "azylman/aerial",
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
			},
			hasKube:    true,
			hasFlux:    true,
			kubeStatus: http.StatusInternalServerError,
			fluxStatus: http.StatusInternalServerError,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch {
					w.WriteHeader(tc.kubeStatus)
				} else {
					w.WriteHeader(tc.fluxStatus)
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			defer ts.Close()

			c := NewClient(ts.Client())
			if tc.hasKube {
				c.SetKubeConfig(ts.URL, "token")
			}
			if tc.hasFlux {
				c.SetFluxReceiverURL(ts.URL)
			}

			workload, err := c.PromoteImageTag(ctx, tc.req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("PromoteImageTag err = %v, wantErr = %v", err, tc.wantErr)
			}
			if workload != tc.wantWorkload {
				t.Errorf("workload = %q, want %q", workload, tc.wantWorkload)
			}
		})
	}
}

func TestClient_PromoteImageTag_ConfigMap404Retry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if r.Method == http.MethodPatch && att == 1 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "tok")
	workload, err := c.PromoteImageTag(ctx, model.ImageReadyEventRequest{
		Repo:      "azylman/aerial",
		Image:     "ghcr.io/azylman/aerial-synapse:latest",
		CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if workload != "aerial-synapse" {
		t.Errorf("workload = %q, want aerial-synapse", workload)
	}
}

func TestClient_PromoteImageTag_ConfigMap404Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 404 patch, POST fails with 400
	tsPostErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`bad post`))
	}))
	defer tsPostErr.Close()

	c := NewClient(tsPostErr.Client())
	c.SetKubeConfig(tsPostErr.URL, "tok")
	_, err := c.PromoteImageTag(ctx, model.ImageReadyEventRequest{
		Repo:      "azylman/aerial",
		Image:     "ghcr.io/azylman/aerial-synapse:latest",
		CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
	})
	if err == nil {
		t.Fatalf("expected error from failed post")
	}

	// 404 patch, POST 409 conflict, retry patch fails with 400
	var att atomic.Int32
	tsConflictErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := att.Add(1)
		if r.Method == http.MethodPatch && a == 1 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`bad retry patch`))
	}))
	defer tsConflictErr.Close()

	cConflict := NewClient(tsConflictErr.Client())
	cConflict.SetKubeConfig(tsConflictErr.URL, "tok")
	_, err = cConflict.PromoteImageTag(ctx, model.ImageReadyEventRequest{
		Repo:      "azylman/aerial",
		Image:     "ghcr.io/azylman/aerial-synapse:latest",
		CommitSHA: "abcdef1234567890abcdef1234567890abcdef12",
	})
	if err == nil {
		t.Fatalf("expected error from failed retry patch")
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
		"GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/marida-system/kustomizations":      `{"items":[]}`,
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

	refs, err := c.DiscoverRepoKustomizations(ctx, "aerial")
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

func TestResolveImageTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		repo          string
		imageRef      string
		wantTagKey    string
		wantImageName string
		wantWorkload  string
	}{
		{
			name:          "empty image",
			repo:          "azylman/aerial",
			imageRef:      "",
			wantTagKey:    "",
			wantImageName: "",
			wantWorkload:  "",
		},
		{
			name:          "none image",
			repo:          "azylman/aerial",
			imageRef:      "ghcr.io/azylman/none:latest",
			wantTagKey:    "",
			wantImageName: "",
			wantWorkload:  "",
		},
		{
			name:          "aerial repo with aerial- prefix",
			repo:          "azylman/aerial",
			imageRef:      "ghcr.io/azylman/aerial-synapse:sha123",
			wantTagKey:    "TAG_AERIAL_SYNAPSE",
			wantImageName: "aerial-synapse",
			wantWorkload:  "aerial-synapse",
		},
		{
			name:          "aerial-sidecars repo with aerial-sidecar- prefix",
			repo:          "azylman/aerial-sidecars",
			imageRef:      "ghcr.io/azylman/aerial-sidecar-prospera:sha123",
			wantTagKey:    "TAG_AERIAL_SIDECAR_PROSPERA",
			wantImageName: "aerial-sidecar-prospera",
			wantWorkload:  "aerial-sidecar-prospera",
		},
		{
			name:          "mirrormere repo with mirrormere- prefix",
			repo:          "azylman/mirrormere",
			imageRef:      "ghcr.io/azylman/mirrormere-kiosk:sha123",
			wantTagKey:    "TAG_MIRRORMERE_KIOSK",
			wantImageName: "mirrormere-kiosk",
			wantWorkload:  "mirrormere-kiosk",
		},
		{
			name:          "mirrormere repo without prefix",
			repo:          "mirrormere",
			imageRef:      "ghcr.io/azylman/kiosk:sha123",
			wantTagKey:    "TAG_MIRRORMERE_KIOSK",
			wantImageName: "mirrormere-kiosk",
			wantWorkload:  "kiosk",
		},
		{
			name:          "other repo with aerial- prefix",
			repo:          "other-org/other-repo",
			imageRef:      "ghcr.io/other/aerial-extra:sha123",
			wantTagKey:    "TAG_AERIAL_EXTRA",
			wantImageName: "aerial-extra",
			wantWorkload:  "aerial-extra",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tagKey, imageName, workload := ResolveImageTag(tc.repo, tc.imageRef)
			if tagKey != tc.wantTagKey || imageName != tc.wantImageName || workload != tc.wantWorkload {
				t.Errorf("ResolveImageTag(%q, %q) = (%q, %q, %q), want (%q, %q, %q)", tc.repo, tc.imageRef, tagKey, imageName, workload, tc.wantTagKey, tc.wantImageName, tc.wantWorkload)
			}
		})
	}
}

func TestPureDecoders(t *testing.T) {
	t.Parallel()

	// GitRepoSlug
	slugTests := []struct {
		url  string
		want string
	}{
		{url: "https://github.com/azylman/aerial.git", want: "azylman/aerial"},
		{url: "ssh://git@github.com/azylman/aerial", want: "azylman/aerial"},
		{url: "git@github.com:azylman/aerial.git", want: "azylman/aerial"},
		{url: "invalid", want: ""},
	}
	for _, tc := range slugTests {
		if got := GitRepoSlug(tc.url); got != tc.want {
			t.Errorf("GitRepoSlug(%q) = %q, want %q", tc.url, got, tc.want)
		}
	}

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
	}
	refs := KustomizationsForImages(workloads, []string{"aerial-synapse"})
	if len(refs) != 1 || refs[0].Name != "core" {
		t.Errorf("KustomizationsForImages = %v, want 1 core ref", refs)
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
	if len(workloads) != 1 || !workloads[0].ReadyConditionTrue || len(workloads[0].Images) != 3 {
		t.Fatalf("unexpected decoded workload: %+v", workloads)
	}

	jobListJSON := []byte(`{"items":[{
		"metadata":{"labels":{"app":"test"}},
		"spec":{
			"jobTemplate":{"spec":{"template":{"spec":{"containers":[{"image":"job1"}],"initContainers":[{"image":"jobinit1"}]}}}},
			"compute":{"image":"comp2"}
		}
	}]}`)
	imageRefs, err := DecodeWorkloadImageRefs(jobListJSON)
	if err != nil {
		t.Fatalf("DecodeWorkloadImageRefs err = %v", err)
	}
	if len(imageRefs) != 1 || len(imageRefs[0].Images) != 3 {
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
	c500.SetFluxReceiverURL(ts500.URL)

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
	if err := c500.upsertImageTagConfigMap(ctx, "TAG_A", "sha123"); err == nil {
		t.Errorf("expected error from 500 upsertImageTagConfigMap")
	}
	if err := c500.forwardFluxReceiver(ctx, map[string]string{"foo": "bar"}); err == nil {
		t.Errorf("expected error from 500 forwardFluxReceiver")
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

	// 5. createOrRetryImageTagConfigMap directly returns 201
	tsCreated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer tsCreated.Close()

	cCreated := NewClient(tsCreated.Client())
	cCreated.SetKubeConfig(tsCreated.URL, "tok")
	err = cCreated.createOrRetryImageTagConfigMap(ctx, tsCreated.Client(), "tok", "TAG_X", "sha", nil)
	if err != nil {
		t.Errorf("expected nil error on 201 Created")
	}
}

func TestKubeClient_TransportFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	ts.Close() // closed immediately

	c := NewClient(ts.Client())
	c.SetKubeConfig(ts.URL, "tok")
	c.SetFluxReceiverURL(ts.URL)

	if err := c.forwardFluxReceiver(ctx, map[string]string{"k": "v"}); err == nil {
		t.Errorf("expected transport error from forwardFluxReceiver")
	}
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

	// 3. createOrRetryImageTagConfigMap conflict retry returns error
	tsConflictErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer tsConflictErr.Close()

	cConflict := NewClient(tsConflictErr.Client())
	cConflict.SetKubeConfig(tsConflictErr.URL, "tok")
	err := cConflict.createOrRetryImageTagConfigMap(ctx, tsConflictErr.Client(), "tok", "TAG_Y", "sha", func() (int, []byte, error) {
		return 0, nil, errors.New("retry patch error")
	})
	if err == nil || !strings.Contains(err.Error(), "retry patch error") {
		t.Errorf("expected retry patch error, got %v", err)
	}

	// 4. upsertImageTagConfigMap network failure
	tsClosed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	tsClosed.Close()

	cClosed := NewClient(tsClosed.Client())
	cClosed.SetKubeConfig(tsClosed.URL, "tok")
	if err := cClosed.upsertImageTagConfigMap(ctx, "TAG_Z", "sha"); err == nil {
		t.Errorf("expected network failure from upsertImageTagConfigMap")
	}
}

func TestKubeClient_Over95Percent(t *testing.T) {
	t.Parallel()

	// 1. GitRepoSlug edge cases
	if s := GitRepoSlug("https://github.com"); s != "" {
		t.Errorf("expected empty slug for https://github.com, got %q", s)
	}
	if s := GitRepoSlug("git@github.com:"); s != "" {
		t.Errorf("expected empty slug for git@github.com:, got %q", s)
	}

	// 2. createOrRetryImageTagConfigMap conflict patch success
	tsConflict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer tsConflict.Close()

	c := NewClient(tsConflict.Client())
	c.SetKubeConfig(tsConflict.URL, "tok")
	err := c.createOrRetryImageTagConfigMap(context.Background(), tsConflict.Client(), "tok", "TAG_OK", "sha", func() (int, []byte, error) {
		return http.StatusOK, []byte(`{}`), nil
	})
	if err != nil {
		t.Errorf("expected nil error on conflict retry patch 200, got %v", err)
	}
}

func TestKubeClient_PushOver95_DecodingAndEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

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

	// PromoteImageTag with neither kube nor flux configured
	cEmpty := NewClient(nil)
	wName, err := cEmpty.PromoteImageTag(ctx, model.ImageReadyEventRequest{
		Repo:  "azylman/aerial",
		Image: "ghcr.io/azylman/aerial-synapse:v1",
	})
	if err != nil || wName != "" {
		t.Errorf("expected empty workload and nil err for unconfigured client, got (%q, %v)", wName, err)
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

	// LoadK8sCertPool with non-PEM content
	tmpCert, err := os.CreateTemp(t.TempDir(), "bad-pem-*.crt")
	if err == nil {
		defer os.Remove(tmpCert.Name())
		_, _ = tmpCert.WriteString("this is not valid pem data")
		_ = tmpCert.Close()
		if pool := LoadK8sCertPool(tmpCert.Name()); pool != nil {
			t.Errorf("expected nil cert pool for invalid PEM")
		}
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
