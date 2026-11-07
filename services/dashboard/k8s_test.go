package main

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseK8sClusterState(t *testing.T) {
	refTime := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	tenSecAgo := refTime.Add(-10 * time.Second)
	twentySecAgo := refTime.Add(-20 * time.Second)

	tests := []struct {
		name            string
		deploymentsJSON string
		podsJSON        string
		now             time.Time
		wantErr         bool
		wantErrSubstr   string
		wantCount       int
		wantStatuses    map[string]string
		wantUptimes     map[string]int64
	}{
		{
			name:            "empty deployments data error",
			deploymentsJSON: "",
			podsJSON:        "",
			now:             refTime,
			wantErr:         true,
			wantErrSubstr:   "empty deployments data",
		},
		{
			name:            "invalid deployments json error",
			deploymentsJSON: "{invalid-json",
			podsJSON:        `{"items":[]}`,
			now:             refTime,
			wantErr:         true,
			wantErrSubstr:   "failed to parse k8s deployments",
		},
		{
			name:            "invalid pods json error",
			deploymentsJSON: `{"items":[{"metadata":{"name":"eri"},"spec":{"replicas":1},"status":{"readyReplicas":1}}]}`,
			podsJSON:        `{invalid-pods-json`,
			now:             refTime,
			wantErr:         true,
			wantErrSubstr:   "failed to parse k8s pods",
		},
		{
			name:            "healthy deployment with running pod",
			deploymentsJSON: `{"items":[{"metadata":{"name":"eri","creationTimestamp":"` + twentySecAgo.Format(time.RFC3339) + `"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "eri-7bfd-1234", "labels": {"app.kubernetes.io/name": "eri"}},
						"status": {
							"phase": "Running",
							"startTime": "` + tenSecAgo.Format(time.RFC3339) + `",
							"containerStatuses": [{"name": "eri", "ready": true, "state": {"running": {"startedAt": "` + tenSecAgo.Format(time.RFC3339) + `"}}}]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"eri": "healthy",
			},
			wantUptimes: map[string]int64{
				"eri": 10,
			},
		},
		{
			name: "prefix matching disambiguation with overlapping hyphenated names",
			deploymentsJSON: `{
				"items": [
					{"metadata": {"name": "victoriametrics"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}},
					{"metadata": {"name": "victoriametrics-mcp"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}}
				]
			}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "victoriametrics-mcp-7bfd-1234"},
						"status": {
							"phase": "Running",
							"startTime": "` + tenSecAgo.Format(time.RFC3339) + `",
							"containerStatuses": [{"name": "victoriametrics-mcp", "ready": true, "state": {"running": {"startedAt": "` + tenSecAgo.Format(time.RFC3339) + `"}}}]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 2,
			wantStatuses: map[string]string{
				"victoriametrics":     "healthy",
				"victoriametrics-mcp": "healthy",
			},
			wantUptimes: map[string]int64{
				"victoriametrics-mcp": 10,
			},
		},
		{
			name:            "starting deployment with pending pod",
			deploymentsJSON: `{"items":[{"metadata":{"name":"haro"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":0}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "haro-xyz", "labels": {"app": "haro"}},
						"status": {
							"phase": "Pending",
							"containerStatuses": [{"name": "haro", "ready": false, "state": {}}]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"haro": "starting",
			},
		},
		{
			name:            "starting deployment readyReplicas less than replicas without crashloop",
			deploymentsJSON: `{"items":[{"metadata":{"name":"coven"},"spec":{"replicas":2},"status":{"replicas":2,"readyReplicas":1}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "coven-abc"},
						"status": {
							"phase": "Running",
							"startTime": "` + tenSecAgo.Format(time.RFC3339) + `",
							"containerStatuses": [{"name": "coven", "ready": true, "state": {"running": {"startedAt": "` + tenSecAgo.Format(time.RFC3339) + `"}}}]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"coven": "starting",
			},
		},
		{
			name:            "unhealthy container crashloop backoff",
			deploymentsJSON: `{"items":[{"metadata":{"name":"vanadis"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":0}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "vanadis-crash", "labels": {"aerial.service": "vanadis"}},
						"status": {
							"phase": "Running",
							"containerStatuses": [
								{
									"name": "vanadis",
									"ready": false,
									"state": {"waiting": {"reason": "CrashLoopBackOff", "message": "back-off restarting failed container"}}
								}
							]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"vanadis": "unhealthy",
			},
		},
		{
			name:            "unhealthy container image pull backoff",
			deploymentsJSON: `{"items":[{"metadata":{"name":"synapse"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":0}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "synapse-pull", "labels": {"app.kubernetes.io/name": "synapse"}},
						"status": {
							"phase": "Waiting",
							"containerStatuses": [
								{
									"name": "synapse",
									"ready": false,
									"state": {"waiting": {"reason": "ImagePullBackOff"}}
								}
							]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"synapse": "unhealthy",
			},
		},
		{
			name:            "unhealthy container err image pull",
			deploymentsJSON: `{"items":[{"metadata":{"name":"dashboard"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":0}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "dashboard", "labels": {"app": "dashboard"}},
						"status": {
							"phase": "Waiting",
							"containerStatuses": [
								{
									"name": "dashboard",
									"ready": false,
									"state": {"waiting": {"reason": "ErrImagePull"}}
								}
							]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"dashboard": "unhealthy",
			},
		},
		{
			name:            "unhealthy initContainer crashloop backoff",
			deploymentsJSON: `{"items":[{"metadata":{"name":"hangar"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":0}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "hangar-init", "labels": {"app.kubernetes.io/name": "hangar"}},
						"status": {
							"phase": "Pending",
							"initContainerStatuses": [
								{
									"name": "git-sync",
									"ready": false,
									"state": {"waiting": {"reason": "CrashLoopBackOff"}}
								}
							]
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"hangar": "unhealthy",
			},
		},
		{
			name:            "unhealthy pod phase failed",
			deploymentsJSON: `{"items":[{"metadata":{"name":"coredns"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":0}}]}`,
			podsJSON: `{
				"items": [
					{
						"metadata": {"name": "coredns-fail", "labels": {"app.kubernetes.io/name": "coredns"}},
						"status": {
							"phase": "Failed"
						}
					}
				]
			}`,
			now:       refTime,
			wantCount: 1,
			wantStatuses: map[string]string{
				"coredns": "unhealthy",
			},
		},
		{
			name:            "unhealthy deployment zero replicas",
			deploymentsJSON: `{"items":[{"metadata":{"name":"stopped-svc"},"spec":{"replicas":0},"status":{"replicas":0,"readyReplicas":0}}]}`,
			podsJSON:        `{"items":[]}`,
			now:             refTime,
			wantCount:       1,
			wantStatuses: map[string]string{
				"stopped-svc": "unhealthy",
			},
		},
		{
			name: "multiple deployments sorted alphabetically with uptime fallback",
			deploymentsJSON: `{
				"items": [
					{"metadata": {"name": "synapse", "creationTimestamp": "` + tenSecAgo.Format(time.RFC3339) + `"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}},
					{"metadata": {"name": "coven", "creationTimestamp": "` + twentySecAgo.Format(time.RFC3339) + `"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}},
					{"metadata": {"name": ""}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}},
					{"metadata": {"name": "eri"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}}
				]
			}`,
			podsJSON:  "",
			now:       time.Time{}, // exercises now.IsZero() fallback
			wantErr:   false,
			wantCount: 3,
			wantStatuses: map[string]string{
				"coven":   "healthy",
				"eri":     "healthy",
				"synapse": "healthy",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			services, err := ParseK8sClusterState([]byte(tc.deploymentsJSON), []byte(tc.podsJSON), tc.now)
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
			// Verify sorted by Name
			for i := 1; i < len(services); i++ {
				if services[i].Name < services[i-1].Name {
					t.Errorf("services not sorted alphabetically: index %d (%q) < index %d (%q)", i, services[i].Name, i-1, services[i-1].Name)
				}
			}
			for _, svc := range services {
				if expectedStatus, ok := tc.wantStatuses[svc.Name]; ok {
					if svc.Status != expectedStatus {
						t.Errorf("service %q expected status %q, got %q", svc.Name, expectedStatus, svc.Status)
					}
				}
				if expectedUptime, ok := tc.wantUptimes[svc.Name]; ok {
					if svc.UptimeSeconds != expectedUptime {
						t.Errorf("service %q expected uptime %d, got %d", svc.Name, expectedUptime, svc.UptimeSeconds)
					}
				}
			}
		})
	}
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

	var authHeaderReceived atomic.Value

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		auth := r.Header.Get("Authorization")
		if auth != "" {
			authHeaderReceived.Store(auth)
		}

		switch r.URL.Path {
		case "/ok/apis/apps/v1/namespaces/default/deployments":
			_, _ = w.Write([]byte(`{
				"items": [
					{"metadata": {"name": "eri"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}}
				]
			}`))
		case "/ok/api/v1/namespaces/default/pods":
			_, _ = w.Write([]byte(`{
				"items": [
					{
						"metadata": {"name": "eri-pod", "labels": {"app.kubernetes.io/name": "eri"}},
						"status": {"phase": "Running", "containerStatuses": [{"name": "eri", "ready": true, "state": {}}]}
					}
				]
			}`))
		case "/dep500/apis/apps/v1/namespaces/default/deployments":
			http.Error(w, "server error", http.StatusInternalServerError)
		case "/pod500/apis/apps/v1/namespaces/default/deployments":
			_, _ = w.Write([]byte(`{"items": [{"metadata": {"name": "eri"}}]}`))
		case "/pod500/api/v1/namespaces/default/pods":
			http.Error(w, "pod error", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockServer.Close()

	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/default/deployments":
			_, _ = w.Write([]byte(`{"items": [{"metadata": {"name": "eri-tls"}, "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1}}]}`))
		case "/api/v1/namespaces/default/pods":
			_, _ = w.Write([]byte(`{"items": []}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockTLSServer.Close()

	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	_ = os.WriteFile(tokenFile, []byte("mock-k8s-bearer-token"), 0o600)

	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: mockTLSServer.Certificate().Raw,
	})
	caFile := filepath.Join(tempDir, "ca.crt")
	_ = os.WriteFile(caFile, caCertPEM, 0o644)

	tests := []struct {
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
	}{
		{
			name:       "success with bearer token",
			tokenPath:  tokenFile,
			subpath:    "/ok",
			wantAuth:   "Bearer mock-k8s-bearer-token",
			wantCount:  1,
			wantName:   "eri",
			wantStatus: "healthy",
		},
		{
			name:       "success without token file",
			tokenPath:  filepath.Join(tempDir, "nonexistent"),
			subpath:    "/ok",
			wantCount:  1,
			wantName:   "eri",
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
			authHeaderReceived.Store("")
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
				addr = mockServer.URL + tc.subpath
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
				val, _ := authHeaderReceived.Load().(string)
				if val != tc.wantAuth {
					t.Errorf("expected Authorization header %q, got %q", tc.wantAuth, val)
				}
			}
		})
	}
}

func TestLoadK8sCertPool_TableDriven(t *testing.T) {
	tempDir := t.TempDir()
	emptyFile := filepath.Join(tempDir, "empty.crt")
	_ = os.WriteFile(emptyFile, []byte(""), 0o644)
	invalidFile := filepath.Join(tempDir, "invalid.crt")
	_ = os.WriteFile(invalidFile, []byte("not pem"), 0o644)

	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer mockTLSServer.Close()
	validPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: mockTLSServer.Certificate().Raw,
	})
	validFile := filepath.Join(tempDir, "valid.crt")
	_ = os.WriteFile(validFile, validPEM, 0o644)

	tests := []struct {
		name       string
		path       string
		wantLoaded bool
	}{
		{
			name:       "empty path returns nil",
			path:       "",
			wantLoaded: false,
		},
		{
			name:       "nonexistent file returns nil",
			path:       filepath.Join(tempDir, "nonexistent.crt"),
			wantLoaded: false,
		},
		{
			name:       "empty file returns nil",
			path:       emptyFile,
			wantLoaded: false,
		},
		{
			name:       "invalid pem returns nil",
			path:       invalidFile,
			wantLoaded: false,
		},
		{
			name:       "valid pem returns cert pool",
			path:       validFile,
			wantLoaded: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := loadK8sCertPool(tc.path)
			if tc.wantLoaded && pool == nil {
				t.Errorf("expected non-nil cert pool for %s", tc.path)
			}
			if !tc.wantLoaded && pool != nil {
				t.Errorf("expected nil cert pool for %s", tc.path)
			}
		})
	}
}

func TestBuildK8sHTTPClient_TableDriven(t *testing.T) {
	tempDir := t.TempDir()
	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer mockTLSServer.Close()
	validPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: mockTLSServer.Certificate().Raw,
	})
	validFile := filepath.Join(tempDir, "valid.crt")
	if err := os.WriteFile(validFile, validPEM, 0o644); err != nil {
		t.Fatalf("failed to write valid cert: %v", err)
	}

	tests := []struct {
		name       string
		caPath     string
		wantRootCA bool
	}{
		{
			name:       "attaches root ca when ca file valid",
			caPath:     validFile,
			wantRootCA: true,
		},
		{
			name:       "leaves root ca nil when ca file missing",
			caPath:     filepath.Join(tempDir, "missing.crt"),
			wantRootCA: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origCA := kubeCAPath
			defer func() { kubeCAPath = origCA }()
			kubeCAPath = tc.caPath

			client := buildK8sHTTPClient()
			tr, ok := client.Transport.(*http.Transport)
			if !ok || tr == nil {
				t.Fatalf("expected *http.Transport, got %T", client.Transport)
			}

			hasRootCA := tr.TLSClientConfig != nil && tr.TLSClientConfig.RootCAs != nil
			if hasRootCA != tc.wantRootCA {
				t.Errorf("hasRootCA = %v, want %v", hasRootCA, tc.wantRootCA)
			}
		})
	}
}
