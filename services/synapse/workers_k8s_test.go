package main

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type mockRowData struct {
	queue     string
	state     string
	count     int64
	oldestAge float64
}

type mockPgxRows struct {
	rows    []mockRowData
	idx     int
	scanErr error
	rowsErr error
	closed  bool
}

func (m *mockPgxRows) Close() {
	m.closed = true
}

func (m *mockPgxRows) Err() error {
	return m.rowsErr
}

func (m *mockPgxRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (m *mockPgxRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (m *mockPgxRows) Next() bool {
	if m.scanErr != nil && m.idx == 0 {
		return true
	}
	if m.idx < len(m.rows) {
		m.idx++
		return true
	}
	return false
}

func (m *mockPgxRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if m.idx <= 0 || m.idx > len(m.rows) {
		return errors.New("out of range scan")
	}
	row := m.rows[m.idx-1]
	if len(dest) >= 4 {
		if p, ok := dest[0].(*string); ok {
			*p = row.queue
		}
		if p, ok := dest[1].(*string); ok {
			*p = row.state
		}
		if p, ok := dest[2].(*int64); ok {
			*p = row.count
		}
		if p, ok := dest[3].(*float64); ok {
			*p = row.oldestAge
		}
	}
	return nil
}

func (m *mockPgxRows) Values() ([]any, error) {
	return nil, nil
}

func (m *mockPgxRows) RawValues() [][]byte {
	return nil
}

func (m *mockPgxRows) Conn() *pgx.Conn {
	return nil
}

func (m *mockPgxRows) TypeMap() *pgtype.Map {
	return nil
}

type mockQueueStatsQuerier struct {
	queryErr error
	rows     *mockPgxRows
}

func (m *mockQueueStatsQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	if m.rows != nil {
		return m.rows, nil
	}
	return &mockPgxRows{}, nil
}

func TestCollectRiverQueueMetrics(t *testing.T) {
	if !strings.Contains(riverQueueStatsQuery, "FROM river_job") {
		t.Errorf("riverQueueStatsQuery should query FROM river_job, got: %s", riverQueueStatsQuery)
	}
	if strings.Contains(riverQueueStatsQuery, "sidecars.river_job") {
		t.Errorf("riverQueueStatsQuery should not query sidecars.river_job, got: %s", riverQueueStatsQuery)
	}

	ctx := context.Background()

	tests := []struct {
		name               string
		nilQuerier         bool
		queryErr           error
		scanErr            error
		rowsErr            error
		rows               []mockRowData
		wantErr            bool
		wantSubstr         string
		wantDepthQueue     string
		wantDepthState     string
		wantDepthVal       float64
		wantOldestAgeQueue string
		wantOldestAgeVal   float64
	}{
		{
			name:       "nil querier returns nil without error",
			nilQuerier: true,
			wantErr:    false,
		},
		{
			name:       "query error returns wrapped error",
			queryErr:   errors.New("connection closed"),
			wantErr:    true,
			wantSubstr: "query river queue stats",
		},
		{
			name:       "scan error returns wrapped error",
			scanErr:    errors.New("column count mismatch"),
			wantErr:    true,
			wantSubstr: "scan river queue stats",
		},
		{
			name:       "rows iteration error returns wrapped error",
			rowsErr:    errors.New("network disconnect during iteration"),
			wantErr:    true,
			wantSubstr: "iterate river queue stats",
		},
		{
			name: "multi-queue state and age aggregation populates gauges correctly",
			rows: []mockRowData{
				{queue: "default", state: "available", count: 7, oldestAge: 45.5},
				{queue: "default", state: "running", count: 3, oldestAge: 120.0},
				{queue: "default", state: "retryable", count: 2, oldestAge: 90.2},
				{queue: "discord_ingress", state: "available", count: 12, oldestAge: 250.0},
				{queue: "widget_sync", state: "scheduled", count: 4, oldestAge: 18.0},
			},
			wantDepthQueue:     "default",
			wantDepthState:     "available",
			wantDepthVal:       7,
			wantOldestAgeQueue: "default",
			wantOldestAgeVal:   90.2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var q queueStatsQuerier
			if !tc.nilQuerier {
				q = &mockQueueStatsQuerier{
					queryErr: tc.queryErr,
					rows: &mockPgxRows{
						rows:    tc.rows,
						scanErr: tc.scanErr,
						rowsErr: tc.rowsErr,
					},
				}
			}

			err := collectRiverQueueMetrics(ctx, q)
			if (err != nil) != tc.wantErr {
				t.Fatalf("collectRiverQueueMetrics() err = %v, wantErr = %v", err, tc.wantErr)
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
			if tc.wantOldestAgeQueue != "" {
				gotAge := testutil.ToFloat64(QueueOldestAgeSeconds.WithLabelValues(tc.wantOldestAgeQueue))
				if gotAge != tc.wantOldestAgeVal {
					t.Errorf("QueueOldestAgeSeconds[%s] = %v, want %v", tc.wantOldestAgeQueue, gotAge, tc.wantOldestAgeVal)
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
			wantRequests: 5,
		},
		{
			name:         "no owner falls back to kustomizations substituting the image tag configmap",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeDeploymentsPath: {body: deploymentList}, kubeKustomizations: {body: kustomizationList}}),
			want:         []fluxKustomizationRef{{Namespace: "flux-system", Name: "alpha"}, {Namespace: "flux-system", Name: "zeta"}},
			wantRequests: 6,
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
			wantRequests: 6,
		},
		{
			name:         "malformed fallback list returns error",
			imageNames:   []string{"aerial-haro"},
			routes:       kubeWorkloadRoutes(map[string]kubeRoute{kubeKustomizations: {body: `[`}}),
			wantErr:      "decode kustomizations",
			wantRequests: 6,
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
