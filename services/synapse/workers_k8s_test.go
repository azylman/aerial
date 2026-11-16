package main

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type mockConsumerStatsQuerier struct {
	stats map[string][2]int64
	err   error
}

func (m *mockConsumerStatsQuerier) ConsumerStats(ctx context.Context, stream, consumer string) (int64, int64, error) {
	if m.err != nil {
		return 0, 0, m.err
	}
	if v, ok := m.stats[stream+"/"+consumer]; ok {
		return v[0], v[1], nil
	}
	return 0, 0, nil
}

func TestCollectJetStreamQueueMetrics(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		nilQuerier     bool
		querierErr     error
		stats          map[string][2]int64
		wantErr        bool
		wantSubstr     string
		wantDepthQueue string
		wantDepthState string
		wantDepthVal   float64
	}{
		{
			name:       "nil querier returns nil without error",
			nilQuerier: true,
			wantErr:    false,
		},
		{
			name:       "querier error returns wrapped error",
			querierErr: errors.New("nats down"),
			wantErr:    true,
			wantSubstr: "consumer stats",
		},
		{
			name: "aggregates pending and in_flight across consumers",
			stats: map[string][2]int64{
				"AERIAL_JOBS/synapse-github":              {3, 1},
				"AERIAL_JOBS/synapse-deploy":              {4, 2},
				"AERIAL_JOBS/synapse-merge-conflict":      {1, 0},
				"AERIAL_JOBS/synapse-discord-ingress":     {5, 3},
				"AERIAL_JOBS/synapse-discord-interaction": {2, 1},
				"AERIAL_COALESCED/synapse-widget-sync":    {6, 2},
			},
			wantDepthQueue: QueueDefault,
			wantDepthState: "pending",
			wantDepthVal:   7,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var q consumerStatsQuerier
			if !tc.nilQuerier {
				q = &mockConsumerStatsQuerier{stats: tc.stats, err: tc.querierErr}
			}
			err := collectJetStreamQueueMetrics(ctx, q)
			if (err != nil) != tc.wantErr {
				t.Fatalf("collectJetStreamQueueMetrics() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if tc.wantDepthQueue != "" {
				gotDepth := testutil.ToFloat64(QueueDepth.WithLabelValues(tc.wantDepthQueue, tc.wantDepthState))
				if gotDepth != tc.wantDepthVal {
					t.Errorf("QueueDepth[%s, %s] = %v, want %v", tc.wantDepthQueue, tc.wantDepthState, gotDepth, tc.wantDepthVal)
				}
			}
		})
	}
}

// kubeRoute is a canned kube API response keyed by "METHOD path" in a kubeFixture.
func TestDefaultOutboundDispatcher_DiscoverImageKustomizations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deploymentList := `{"items":[
		{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"aerial-core","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}}},
		{"metadata":{"labels":{"app":"unmanaged"}},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/azylman/aerial-synapse:abc"}]}}}}
	]}`
	cronJobList := `{"items":[{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"marida-core"}},"spec":{"jobTemplate":{"spec":{"template":{"spec":{"initContainers":[{"image":"ghcr.io/azylman/aerial-synapse@sha256:beef"}],"containers":[{"image":"busybox"}]}}}}}}]}`
	restateClusterList := `{"items":[{"metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"restate-system","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"compute":{"image":"docker.io/restatedev/restate:1.6"}}}]}`
	kustomizationList := `{"items":[
		{"metadata":{"name":"zeta","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"aerial-image-tags"}]}}},
		{"metadata":{"name":"flux-system","namespace":"flux-system"},"spec":{}},
		{"metadata":{"name":"secrets-only","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"Secret","name":"aerial-image-tags"}]}}},
		{"metadata":{"name":"alpha","namespace":"flux-system"},"spec":{"postBuild":{"substituteFrom":[{"kind":"ConfigMap","name":"other"},{"kind":"ConfigMap","name":"aerial-image-tags"}]}}}
	]}`

	tests := []struct {
		name         string
		noKube       bool
		imageNames   []string
		routes       map[string]kubeRoute
		want         []fluxKustomizationRef
		wantErr      string
		wantRequests int
	}{
		{
			name:         "labeled deployment and cronjob initContainer owners are discovered",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeCronJobsPath: {body: cronJobList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}, {Namespace: "flux-system", Name: "marida-core"}},
			wantRequests: 7,
		},
		{
			name:         "restate cluster compute image owner is discovered",
			imageNames:   []string{"restate"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeRestateClustersPath: {body: restateClusterList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "restate-system"}},
			wantRequests: 7,
		},
		{
			name:         "missing crd 404 is treated as empty workload list during discovery",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeRestateDeploymentsPath: {status: http.StatusNotFound, body: `{"reason":"NotFound"}`}, kubeRestateClustersPath: {status: http.StatusNotFound, body: `{"reason":"NotFound"}`}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}},
			wantRequests: 7,
		},
		{
			name:         "no owner falls back to kustomizations substituting the image tag configmap",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeKustomizations: {body: kustomizationList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "alpha"}, {Namespace: "flux-system", Name: "zeta"}},
			wantRequests: 8,
		},
		{
			name:         "workload list failure returns error",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeStatefulSetsPath: {status: http.StatusForbidden, body: `{"reason":"Forbidden"}`}}),
			wantErr:      "returned status 403",
			wantRequests: 2,
		},
		{
			name:         "malformed workload list returns error",
			imageNames:   []string{"aerial-synapse"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: `{"items":`}}),
			wantErr:      "decode /apis/apps/v1/deployments",
			wantRequests: 1,
		},
		{
			name:         "fallback list failure returns error",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(nil),
			wantErr:      "list kustomizations",
			wantRequests: 8,
		},
		{
			name:         "malformed fallback list returns error",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeKustomizations: {body: `[`}}),
			wantErr:      "decode kustomizations",
			wantRequests: 8,
		},
		{
			name:       "no image names skips the kube API",
			imageNames: nil,
		},
		{
			name:       "unconfigured kube API skips discovery",
			noKube:     true,
			imageNames: []string{"aerial-synapse"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, fixture := newConfiguredDispatcher(t, tc.routes, !tc.noKube)
			got, err := disp.DiscoverImageKustomizations(ctx, tc.imageNames)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("refs = %v, want %v", got, tc.want)
			}
			if n := len(fixture.Requests()); n != tc.wantRequests {
				t.Errorf("kube requests = %d (%v), want %d", n, fixture.Requests(), tc.wantRequests)
			}
		})
	}
}

func newConfiguredDispatcher(t *testing.T, routes map[string]kubeRoute, configureKube bool) (*DefaultOutboundDispatcher, *kubeFixture) {
	t.Helper()
	fixture, ts := newKubeFixture(t, routes)
	disp := NewDefaultOutboundDispatcher("", ts.Client())
	if configureKube {
		disp.SetKubeConfig(ts.URL, "test-token")
	}
	return disp, fixture
}

func TestDefaultOutboundDispatcher_ReconcileKustomization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const marida = "PATCH /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/apps/kustomizations/marida-core"
	const requestedAt = "2026-11-08T10:00:00.123456789Z"

	tests := []struct {
		name    string
		noKube  bool
		routes  map[string]kubeRoute
		wantErr string
	}{
		{name: "patches the given requestedAt annotation in the ref namespace", routes: map[string]kubeRoute{marida: {body: `{}`}}},
		{name: "patch rejection returns error", routes: map[string]kubeRoute{marida: {status: http.StatusForbidden, body: `forbidden`}}, wantErr: "reconcile kustomization apps/marida-core"},
		{name: "unconfigured kube API returns error", noKube: true, wantErr: "kube api server not configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, fixture := newConfiguredDispatcher(t, tc.routes, !tc.noKube)
			err := disp.ReconcileKustomization(ctx, fluxKustomizationRef{Namespace: "apps", Name: "marida-core"}, requestedAt)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			wantBody := `{"metadata":{"annotations":{"reconcile.fluxcd.io/requestedAt":"2026-11-08T10:00:00.123456789Z"}}}`
			if got := fixture.Body(marida); got != wantBody {
				t.Errorf("patch body = %s, want %s", got, wantBody)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_DiscoverRepoKustomizations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		configGitGet = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial-config"
		aerialGitGet = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial"
		appsGitGet   = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/apps/gitrepositories/stack"
		kustList     = `{"items":[` +
			`{"metadata":{"name":"zeta-config"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial-config"}}},` +
			`{"metadata":{"name":"aerial-core"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}},` +
			`{"metadata":{"name":"alpha-config"},"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial-config"}}},` +
			`{"metadata":{"name":"paused-config"},"spec":{"suspend":true,"sourceRef":{"kind":"GitRepository","name":"aerial-config"}}},` +
			`{"metadata":{"name":"stack"},"spec":{"sourceRef":{"kind":"GitRepository","name":"stack","namespace":"apps"}}},` +
			`{"metadata":{"name":"charts"},"spec":{"sourceRef":{"kind":"OCIRepository","name":"charts"}}}]}`
	)
	sourceRoutes := map[string]kubeRoute{
		kubeKustomizations: {body: kustList},
		configGitGet:       {body: `{"spec":{"url":"https://github.com/Azylman/Aerial-Config.git"}}`},
		aerialGitGet:       {body: `{"spec":{"url":"ssh://git@github.com/azylman/aerial"}}`},
		appsGitGet:         {body: `{"spec":{"url":"git@github.com:acme/stack.git"}}`},
	}

	tests := []struct {
		name      string
		apiServer bool
		repo      string
		routes    map[string]kubeRoute
		want      []fluxKustomizationRef
		wantErr   string
	}{
		{name: "matches every unsuspended kustomization of the repo sorted", apiServer: true, repo: "azylman/aerial-config", routes: sourceRoutes, want: []fluxKustomizationRef{{Namespace: "flux-system", Name: "alpha-config"}, {Namespace: "flux-system", Name: "zeta-config"}}},
		{name: "bare repo name normalizes to the default owner", apiServer: true, repo: "aerial", routes: sourceRoutes, want: []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}}},
		{name: "cross-namespace source resolves in its own namespace", apiServer: true, repo: "acme/stack", routes: sourceRoutes, want: []fluxKustomizationRef{{Namespace: "flux-system", Name: "stack"}}},
		{name: "repo with no flux source finds nothing", apiServer: true, repo: "azylman/mirrormere", routes: sourceRoutes},
		{name: "kube api not configured finds nothing", repo: "azylman/aerial-config"},
		{name: "empty repo finds nothing", apiServer: true, repo: " ", routes: sourceRoutes},
		{name: "kustomization list failure", apiServer: true, repo: "azylman/aerial-config", wantErr: "list kustomizations"},
		{name: "malformed kustomization list", apiServer: true, repo: "azylman/aerial-config", routes: map[string]kubeRoute{kubeKustomizations: {body: `{`}}, wantErr: "decode kustomization list"},
		{name: "gitrepository get failure", apiServer: true, repo: "azylman/aerial-config", routes: map[string]kubeRoute{kubeKustomizations: {body: kustList}}, wantErr: "get gitrepository flux-system/aerial-config"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, _ := newConfiguredDispatcher(t, tc.routes, tc.apiServer)
			got, err := disp.DiscoverRepoKustomizations(ctx, tc.repo)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("refs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_ResolveFluxSourceRepo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		coreGet    = "GET /apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations/aerial-core"
		repoGet    = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/flux-system/gitrepositories/aerial"
		appsGitGet = "GET /apis/source.toolkit.fluxcd.io/v1/namespaces/apps/gitrepositories/stack"
	)

	tests := []struct {
		name    string
		obj     FluxInvolvedObject
		routes  map[string]kubeRoute
		want    string
		wantErr string
	}{
		{name: "gitrepository resolves its own url", obj: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial", Namespace: "flux-system"}, routes: map[string]kubeRoute{repoGet: {body: `{"spec":{"url":"https://github.com/acme/widgets.git"}}`}}, want: "acme/widgets"},
		{name: "kustomization follows sourceRef in its namespace", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: map[string]kubeRoute{coreGet: {body: `{"spec":{"sourceRef":{"kind":"GitRepository","name":"aerial"}}}`}, repoGet: {body: `{"spec":{"url":"ssh://git@github.com/acme/core"}}`}}, want: "acme/core"},
		{name: "kustomization follows cross-namespace sourceRef", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core", Namespace: "flux-system"}, routes: map[string]kubeRoute{coreGet: {body: `{"spec":{"sourceRef":{"kind":"GitRepository","name":"stack","namespace":"apps"}}}`}, appsGitGet: {body: `{"spec":{"url":"git@github.com:acme/stack.git"}}`}}, want: "acme/stack"},
		{name: "kustomization with bucket source resolves empty", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: map[string]kubeRoute{coreGet: {body: `{"spec":{"sourceRef":{"kind":"Bucket","name":"b"}}}`}}},
		{name: "unsupported kind resolves empty", obj: FluxInvolvedObject{Kind: "HelmRelease", Name: "x"}},
		{name: "kustomization get failure", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, wantErr: "get kustomization flux-system/aerial-core"},
		{name: "malformed kustomization", obj: FluxInvolvedObject{Kind: "Kustomization", Name: "aerial-core"}, routes: map[string]kubeRoute{coreGet: {body: `{`}}, wantErr: "decode kustomization flux-system/aerial-core"},
		{name: "gitrepository get failure", obj: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"}, wantErr: "get gitrepository flux-system/aerial"},
		{name: "malformed gitrepository", obj: FluxInvolvedObject{Kind: "GitRepository", Name: "aerial"}, routes: map[string]kubeRoute{repoGet: {body: `[`}}, wantErr: "decode gitrepository flux-system/aerial"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, _ := newConfiguredDispatcher(t, tc.routes, true)
			got, err := disp.ResolveFluxSourceRepo(ctx, tc.obj)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("repo = %q, want %q", got, tc.want)
			}
		})
	}
}
