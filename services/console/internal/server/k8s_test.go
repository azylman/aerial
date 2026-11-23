package server

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type parseK8sClusterStateTestCase struct {
	name            string
	deploymentsJSON string
	podsJSON        string
	now             time.Time
	wantErr         string
	wantCount       int
	wantStatuses    map[string]string
	wantUptimes     map[string]int64
	wantCommits     map[string]string
	wantImages      map[string]string
}

// dep renders one Deployment whose status.replicas equals its desired replicas.
func dep(name string, replicas, ready, updated int) string {
	return fmt.Sprintf(`{"metadata":{"name":%q},"spec":{"replicas":%d},"status":{"replicas":%d,"readyReplicas":%d,"updatedReplicas":%d}}`, name, replicas, replicas, ready, updated)
}

// pod renders one Pod. label is "key=value" ("" for none); a non-empty waitingReason adds a
// waiting container status and a non-zero startedAt sets status.startTime.
func pod(name, label, phase, waitingReason string, startedAt time.Time) string {
	meta := fmt.Sprintf(`"name":%q`, name)
	if key, value, ok := strings.Cut(label, "="); ok {
		meta += fmt.Sprintf(`,"labels":{%q:%q}`, key, value)
	}
	status := fmt.Sprintf(`"phase":%q`, phase)
	if !startedAt.IsZero() {
		status += `,"startTime":"` + startedAt.Format(time.RFC3339) + `"`
	}
	if waitingReason != "" {
		status += fmt.Sprintf(`,"containerStatuses":[{"name":"main","ready":false,"state":{"waiting":{"reason":%q}}}]`, waitingReason)
	}
	return `{"metadata":{` + meta + `},"status":{` + status + `}}`
}

func deps(items ...string) string { return `{"items":[` + strings.Join(items, ",") + `]}` }

func pods(items ...string) string { return `{"items":[` + strings.Join(items, ",") + `]}` }

func TestParseK8sClusterState(t *testing.T) {
	refTime := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	tenSecAgo := refTime.Add(-10 * time.Second)
	twentySecAgo := refTime.Add(-20 * time.Second)
	const (
		sha    = "de22ef4a238e56b6f9fb67cb7eb676f39d6596c3"
		synImg = "ghcr.io/example/aerial-synapse:" + sha
		eriImg = "ghcr.io/example/aerial-eri:" + sha
	)
	var noTime time.Time

	tests := []parseK8sClusterStateTestCase{
		{name: "empty deployments data error", now: refTime, wantErr: "empty deployments data"},
		{name: "invalid deployments json error", deploymentsJSON: "{invalid-json", podsJSON: pods(), now: refTime, wantErr: "failed to parse k8s deployments"},
		{name: "invalid pods json error", deploymentsJSON: deps(dep("eri", 1, 1, 1)), podsJSON: "{invalid-pods-json", now: refTime, wantErr: "failed to parse k8s pods"},
		{
			name:            "healthy deployment uptime from pod start time over creation timestamp",
			deploymentsJSON: `{"items":[{"metadata":{"name":"eri","creationTimestamp":"` + twentySecAgo.Format(time.RFC3339) + `"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1}}]}`,
			podsJSON:        pods(pod("eri-7bfd-1234", "app.kubernetes.io/name=eri", "Running", "", tenSecAgo)),
			now:             refTime, wantCount: 1, wantStatuses: map[string]string{"eri": "healthy"}, wantUptimes: map[string]int64{"eri": 10},
		},
		{
			name:            "prefix matching disambiguation with overlapping hyphenated names",
			deploymentsJSON: deps(dep("victoriametrics", 1, 1, 1), dep("victoriametrics-mcp", 1, 1, 1)),
			podsJSON:        pods(pod("victoriametrics-mcp-7bfd-1234", "", "Running", "", tenSecAgo)),
			now:             refTime, wantCount: 2,
			wantStatuses: map[string]string{"victoriametrics": "healthy", "victoriametrics-mcp": "healthy"},
			wantUptimes:  map[string]int64{"victoriametrics": 0, "victoriametrics-mcp": 10},
		},
		{
			name: "starting deployment with pending pod", deploymentsJSON: deps(dep("haro", 1, 0, 0)), podsJSON: pods(pod("haro-xyz", "app=haro", "Pending", "", noTime)),
			now: refTime, wantCount: 1, wantStatuses: map[string]string{"haro": "starting"},
		},
		{
			name: "starting deployment readyReplicas less than replicas without crashloop", deploymentsJSON: deps(dep("coven", 2, 1, 1)),
			podsJSON: pods(pod("coven-abc", "", "Running", "", tenSecAgo)), now: refTime, wantCount: 1, wantStatuses: map[string]string{"coven": "starting"},
		},
		{
			name: "unhealthy container crashloop backoff", deploymentsJSON: deps(dep("vanadis", 1, 0, 0)),
			podsJSON: pods(pod("vanadis-crash", "aerial.service=vanadis", "Running", "CrashLoopBackOff", noTime)), now: refTime, wantCount: 1, wantStatuses: map[string]string{"vanadis": "unhealthy"},
		},
		{
			name: "unhealthy container image pull backoff", deploymentsJSON: deps(dep("synapse", 1, 0, 0)),
			podsJSON: pods(pod("synapse-pull", "app.kubernetes.io/name=synapse", "Waiting", "ImagePullBackOff", noTime)), now: refTime, wantCount: 1, wantStatuses: map[string]string{"synapse": "unhealthy"},
		},
		{
			name: "unhealthy container err image pull matched by pod name", deploymentsJSON: deps(dep("dashboard", 1, 0, 0)),
			podsJSON: pods(pod("dashboard", "", "Waiting", "ErrImagePull", noTime)), now: refTime, wantCount: 1, wantStatuses: map[string]string{"dashboard": "unhealthy"},
		},
		{
			name: "unhealthy initContainer crashloop backoff", deploymentsJSON: deps(dep("hangar", 1, 0, 0)),
			podsJSON: `{"items":[{"metadata":{"name":"hangar-init","labels":{"app.kubernetes.io/name":"hangar"}},"status":{"phase":"Pending","initContainerStatuses":[{"name":"git-sync","ready":false,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}]}`,
			now:      refTime, wantCount: 1, wantStatuses: map[string]string{"hangar": "unhealthy"},
		},
		{
			name: "unhealthy pod phase failed", deploymentsJSON: deps(dep("coredns", 1, 0, 0)),
			podsJSON: pods(pod("coredns-fail", "app.kubernetes.io/name=coredns", "Failed", "", noTime)), now: refTime, wantCount: 1, wantStatuses: map[string]string{"coredns": "unhealthy"},
		},
		{name: "unhealthy deployment zero replicas", deploymentsJSON: deps(dep("stopped-svc", 0, 0, 0)), podsJSON: pods(), now: refTime, wantCount: 1, wantStatuses: map[string]string{"stopped-svc": "unhealthy"}},
		{
			name: "multiple deployments sorted alphabetically with uptime fallback",
			deploymentsJSON: deps(
				`{"metadata":{"name":"synapse","creationTimestamp":"`+tenSecAgo.Format(time.RFC3339)+`"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1}}`,
				`{"metadata":{"name":"coven","creationTimestamp":"`+twentySecAgo.Format(time.RFC3339)+`"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1}}`,
				dep("", 1, 1, 1), dep("eri", 1, 1, 1),
			),
			now:       noTime, // exercises the now.IsZero() fallback
			wantCount: 3, wantStatuses: map[string]string{"coven": "healthy", "eri": "healthy", "synapse": "healthy"},
		},
		{
			name:            "fills commit and image from container named like deployment",
			deploymentsJSON: `{"items":[{"metadata":{"name":"synapse","generation":3},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"otel","image":"ghcr.io/example/aerial-eri:1111222233334444555566667777888899990000"},{"name":"synapse","image":"` + synImg + `"}]}}},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1,"observedGeneration":3}}]}`,
			now:             refTime, wantCount: 1,
			wantStatuses: map[string]string{"synapse": "healthy"}, wantCommits: map[string]string{"synapse": sha}, wantImages: map[string]string{"synapse": synImg},
		},
		{
			name:            "sibling deployment picks first sha-tagged container",
			deploymentsJSON: `{"items":[{"metadata":{"name":"eri-agent-beta"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"git-sync","image":"registry.k8s.io/git-sync/git-sync:v4.4.0"},{"name":"eri","image":"` + eriImg + `"}]}}},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1}}]}`,
			now:             refTime, wantCount: 1, wantCommits: map[string]string{"eri-agent-beta": sha}, wantImages: map[string]string{"eri-agent-beta": eriImg},
		},
		{
			name:            "non-sha tag keeps image without commit",
			deploymentsJSON: `{"items":[{"metadata":{"name":"grafana"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"grafana","image":"grafana/grafana:11.3.0"}]}}},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1}}]}`,
			now:             refTime, wantCount: 1, wantCommits: map[string]string{"grafana": ""}, wantImages: map[string]string{"grafana": "grafana/grafana:11.3.0"},
		},
		{
			name:            "scaled to zero keeps status and drops commit",
			deploymentsJSON: `{"items":[{"metadata":{"name":"synapse"},"spec":{"replicas":0,"template":{"spec":{"containers":[{"name":"synapse","image":"` + synImg + `"}]}}},"status":{}}]}`,
			now:             refTime, wantCount: 1,
			wantStatuses: map[string]string{"synapse": "unhealthy"}, wantCommits: map[string]string{"synapse": ""}, wantImages: map[string]string{"synapse": synImg},
		},
		{name: "updated replicas below desired is starting", deploymentsJSON: deps(dep("synapse", 2, 2, 1)), now: refTime, wantCount: 1, wantStatuses: map[string]string{"synapse": "starting"}},
		{
			name:            "old replicaset pods still present is starting",
			deploymentsJSON: `{"items":[{"metadata":{"name":"synapse"},"spec":{"replicas":1},"status":{"replicas":2,"readyReplicas":1,"updatedReplicas":1}}]}`,
			now:             refTime, wantCount: 1, wantStatuses: map[string]string{"synapse": "starting"},
		},
		{
			name:            "unobserved generation is starting",
			deploymentsJSON: `{"items":[{"metadata":{"name":"synapse","generation":5},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1,"updatedReplicas":1,"observedGeneration":4}}]}`,
			now:             refTime, wantCount: 1, wantStatuses: map[string]string{"synapse": "starting"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			services, err := ParseK8sClusterState([]byte(tc.deploymentsJSON), []byte(tc.podsJSON), tc.now)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertParsedK8sServices(t, tc, services)
		})
	}
}

// assertParsedK8sServices checks count, name ordering and every per-service expectation map.
func assertParsedK8sServices(t *testing.T, tc parseK8sClusterStateTestCase, services []ServiceStatus) {
	t.Helper()
	if len(services) != tc.wantCount {
		t.Fatalf("expected %d services, got %d", tc.wantCount, len(services))
	}
	for i := 1; i < len(services); i++ {
		if services[i].Name < services[i-1].Name {
			t.Errorf("services not sorted alphabetically: index %d (%q) < index %d (%q)", i, services[i].Name, i-1, services[i-1].Name)
		}
	}
	for _, svc := range services {
		if want, ok := tc.wantStatuses[svc.Name]; ok && svc.Status != want {
			t.Errorf("service %q status = %q, want %q", svc.Name, svc.Status, want)
		}
		if want, ok := tc.wantCommits[svc.Name]; ok && svc.Commit != want {
			t.Errorf("service %q commit = %q, want %q", svc.Name, svc.Commit, want)
		}
		if want, ok := tc.wantImages[svc.Name]; ok && svc.Image != want {
			t.Errorf("service %q image = %q, want %q", svc.Name, svc.Image, want)
		}
		if want, ok := tc.wantUptimes[svc.Name]; ok && svc.UptimeSeconds != want {
			t.Errorf("service %q uptime = %d, want %d", svc.Name, svc.UptimeSeconds, want)
		}
	}
}

type fetchK8sClusterStateTestCase struct {
	name          string
	tokenPath     string
	caPath        string
	subpath       string
	overrideAddr  string
	wantAuth      string
	wantErr       bool
	wantErrSubstr string
	wantCount     int
	wantName      string
	wantStatus    string
}

func TestFetchK8sClusterState(t *testing.T) {
	origClient := k8sHTTPClient
	origTokenPath := kubeTokenPath
	origCAPath := kubeCAPath
	t.Cleanup(func() {
		k8sHTTPClient = origClient
		kubeTokenPath = origTokenPath
		kubeCAPath = origCAPath
		if k8sHTTPClient != nil {
			k8sHTTPClient.CloseIdleConnections()
		}
	})

	k8sAPI := newFakeK8sAPI(t)

	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/default/deployments":
			_, _ = w.Write([]byte(`{"items": [{"metadata": {"name": "eri-tls"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1, "updatedReplicas": 1}}]}`))
		case "/api/v1/namespaces/default/pods":
			_, _ = w.Write([]byte(`{"items": []}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockTLSServer.Close()

	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("mock-k8s-bearer-token"), 0o600); err != nil {
		t.Fatalf("failed to write token file: %v", err)
	}

	caFile := writeTestCA(t, mockTLSServer)

	tests := []fetchK8sClusterStateTestCase{
		{
			name:       "success with bearer token",
			tokenPath:  tokenFile,
			subpath:    "/ok",
			wantAuth:   "Bearer mock-k8s-bearer-token",
			wantCount:  1,
			wantName:   "k8s-brain",
			wantStatus: "healthy",
		},
		{
			name:       "success without token file",
			tokenPath:  filepath.Join(tempDir, "nonexistent"),
			subpath:    "/ok",
			wantCount:  1,
			wantName:   "k8s-brain",
			wantStatus: "healthy",
		},
		{
			name:         "success with in-cluster ca certificate",
			tokenPath:    tokenFile,
			caPath:       caFile,
			overrideAddr: mockTLSServer.URL,
			wantCount:    1,
			wantName:     "eri-tls",
			wantStatus:   "healthy",
		},
		{name: "tls server without ca fails verification", tokenPath: tokenFile, overrideAddr: mockTLSServer.URL, wantErr: true, wantErrSubstr: "certificate"},
		{
			name:          "empty kube api server",
			overrideAddr:  "   ",
			wantErr:       true,
			wantErrSubstr: "empty kubernetes API server address",
		},
		{
			name:         "invalid url",
			overrideAddr: "http:// ",
			wantErr:      true,
		},
		{
			name:          "deployments http 500 error",
			subpath:       "/dep500",
			wantErr:       true,
			wantErrSubstr: "k8s deployments returned HTTP 500",
		},
		{
			name:          "pods http 500 error",
			subpath:       "/pod500",
			wantErr:       true,
			wantErrSubstr: "k8s pods returned HTTP 500",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runFetchK8sClusterStateCase(t, tc, tempDir, k8sAPI)
		})
	}
}

// fakeK8sAPI serves Deployment and Pod lists under per-scenario path prefixes:
// /ok (one healthy k8s-brain), /degraded (k8s-brain crash-looping), /dep500
// (deployments list fails) and /pod500 (pods list fails). It records the last
// non-empty Authorization header.
type fakeK8sAPI struct {
	*httptest.Server

	lastAuth atomic.Value
}

const (
	k8sDeploymentsPath = "/apis/apps/v1/namespaces/default/deployments"
	k8sPodsPath        = "/api/v1/namespaces/default/pods"
)

var fakeK8sRoutes = map[string]string{
	"/ok" + k8sDeploymentsPath:       deps(dep("k8s-brain", 1, 1, 1)),
	"/ok" + k8sPodsPath:              pods(pod("k8s-brain-pod", "app.kubernetes.io/name=k8s-brain", "Running", "", time.Time{})),
	"/degraded" + k8sDeploymentsPath: deps(dep("k8s-brain", 1, 0, 1)),
	"/degraded" + k8sPodsPath:        pods(pod("k8s-brain-pod", "app.kubernetes.io/name=k8s-brain", "Running", "CrashLoopBackOff", time.Time{})),
	"/pod500" + k8sDeploymentsPath:   deps(dep("k8s-brain", 1, 1, 1)),
}

func newFakeK8sAPI(t *testing.T) *fakeK8sAPI {
	t.Helper()
	f := &fakeK8sAPI{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			f.lastAuth.Store(auth)
		}
		body, ok := fakeK8sRoutes[r.URL.Path]
		if !ok {
			http.Error(w, "k8s server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(f.Close)
	return f
}

// writeTestCA writes srv's self-signed certificate as a PEM CA bundle and returns its path.
func writeTestCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.crt")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return path
}

func runFetchK8sClusterStateCase(t *testing.T, tc fetchK8sClusterStateTestCase, tempDir string, k8sAPI *fakeK8sAPI) {
	t.Helper()
	k8sAPI.lastAuth.Store("")
	if tc.tokenPath != "" {
		kubeTokenPath = tc.tokenPath
	} else {
		kubeTokenPath = filepath.Join(tempDir, "nonexistent")
	}

	if tc.caPath != "" {
		kubeCAPath = tc.caPath
	} else {
		kubeCAPath = filepath.Join(tempDir, "nonexistent-ca")
	}
	k8sHTTPClient = buildK8sHTTPClient()

	addr := tc.overrideAddr
	if addr == "" {
		addr = k8sAPI.URL + tc.subpath
	}

	services, err := fetchK8sClusterState(context.Background(), addr)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
			t.Errorf("expected error containing %q, got: %v", tc.wantErrSubstr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(services) != tc.wantCount {
		t.Fatalf("expected %d services, got %d", tc.wantCount, len(services))
	}
	if len(services) > 0 {
		if services[0].Name != tc.wantName || services[0].Status != tc.wantStatus {
			t.Errorf("unexpected service: %+v", services[0])
		}
	}
	if tc.wantAuth != "" {
		val, _ := k8sAPI.lastAuth.Load().(string)
		if val != tc.wantAuth {
			t.Errorf("expected Authorization header %q, got %q", tc.wantAuth, val)
		}
	}
}
