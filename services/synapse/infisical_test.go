package main

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInfisicalWebhookPayload_Getters(t *testing.T) {
	t.Parallel()
	type dataStruct = struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	}
	tests := []struct {
		name     string
		payload  InfisicalWebhookPayload
		fallback string
		wantWS   string
		wantEnv  string
	}{
		{name: "workspace id from payload field", payload: InfisicalWebhookPayload{WorkspaceID: "ws-1"}, fallback: "fb", wantWS: "ws-1", wantEnv: "fb"},
		{name: "project id from payload field", payload: InfisicalWebhookPayload{ProjectID: "proj-1"}, fallback: "fb", wantWS: "proj-1", wantEnv: "fb"},
		{name: "workspace id from nested data", payload: InfisicalWebhookPayload{Data: &dataStruct{WorkspaceID: "ws-data"}}, fallback: "fb", wantWS: "ws-data", wantEnv: "fb"},
		{name: "project id from nested data", payload: InfisicalWebhookPayload{Data: &dataStruct{ProjectID: "proj-data"}}, fallback: "fb", wantWS: "proj-data", wantEnv: "fb"},
		{name: "environment from payload field", payload: InfisicalWebhookPayload{Environment: "prod"}, fallback: "fb", wantWS: "fb", wantEnv: "prod"},
		{name: "environment from nested data", payload: InfisicalWebhookPayload{Data: &dataStruct{Environment: "staging"}}, fallback: "fb", wantWS: "fb", wantEnv: "staging"},
		{name: "fallback when fields empty", payload: InfisicalWebhookPayload{}, fallback: "fb", wantWS: "fb", wantEnv: "fb"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if gotWS := tc.payload.GetWorkspaceID(tc.fallback); gotWS != tc.wantWS {
				t.Errorf("GetWorkspaceID() = %q, want %q", gotWS, tc.wantWS)
			}
			if gotEnv := tc.payload.GetEnvironment(tc.fallback); gotEnv != tc.wantEnv {
				t.Errorf("GetEnvironment() = %q, want %q", gotEnv, tc.wantEnv)
			}
		})
	}
}

func TestHandleInfisicalAndGenericWebhooks(t *testing.T) {
	t.Parallel()
	routes := NewRouterServer(Config{}, nil).Routes()

	cases := []struct {
		name, path, wantTargetSec, wantTargetVar, wantEnv, wantMsg string
		body                                                       io.Reader
		wantStatus                                                 int
	}{
		{name: "infisical valid payload", path: "/webhooks/infisical", body: strings.NewReader(`{"event":"secrets.modified"}`), wantStatus: http.StatusAccepted, wantTargetSec: TargetK8sSecret, wantTargetVar: TargetK8sSecret},
		{name: "infisical malformed json", path: "/webhooks/infisical", body: strings.NewReader(`{not valid json}`), wantStatus: http.StatusBadRequest},
		{name: "infisical body read error", path: "/webhooks/infisical", body: errReader{}, wantStatus: http.StatusBadRequest},
		{name: "generic standard sync", path: "/webhooks/generic", body: strings.NewReader(`{"environment":"prod"}`), wantStatus: http.StatusAccepted, wantTargetSec: TargetK8sSecret, wantTargetVar: TargetK8sSecret},
		{name: "generic empty body allowed", path: "/webhooks/generic", body: strings.NewReader(""), wantStatus: http.StatusAccepted},
		{name: "generic direct secrets map", path: "/webhooks/generic", body: strings.NewReader(`{"secrets":{"KEY1":"VAL1"}}`), wantStatus: http.StatusAccepted, wantTargetSec: TargetK8sSecret, wantTargetVar: TargetK8sSecret, wantMsg: "Direct secret sync scheduled"},
		{name: "generic explicit workspace and env", path: "/webhooks/generic", body: strings.NewReader(`{"workspaceId":"custom-ws","environment":"dev"}`), wantStatus: http.StatusAccepted, wantTargetSec: TargetK8sSecret, wantTargetVar: TargetK8sSecret, wantEnv: "dev"},
		{name: "generic bad json", path: "/webhooks/generic", body: strings.NewReader(`{invalid`), wantStatus: http.StatusBadRequest},
		{name: "generic body read error", path: "/webhooks/generic", body: errReader{}, wantStatus: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, nil)
			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
			if tc.wantTargetSec != "" || tc.wantTargetVar != "" || tc.wantEnv != "" || tc.wantMsg != "" {
				var resp map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse response: %v", err)
				}
				if tc.wantTargetSec != "" && resp["targetSecret"] != tc.wantTargetSec {
					t.Errorf("expected targetSecret %q, got %v", tc.wantTargetSec, resp["targetSecret"])
				}
				if tc.wantTargetVar != "" && resp["targetVariable"] != tc.wantTargetVar {
					t.Errorf("expected targetVariable %q, got %v", tc.wantTargetVar, resp["targetVariable"])
				}
				if tc.wantEnv != "" && resp["environment"] != tc.wantEnv {
					t.Errorf("expected environment %q, got %v", tc.wantEnv, resp["environment"])
				}
				if tc.wantMsg != "" && resp["message"] != tc.wantMsg {
					t.Errorf("expected message %q, got %v", tc.wantMsg, resp["message"])
				}
			}
		})
	}
}

func TestSyncSecrets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		cfgOverride   *Config
		loginStatus   int
		loginBody     string
		secretsStatus int
		secretsBody   string
		k8sPutStatus  int
		k8sPostStatus int
		unreachable   bool
		wantErr       bool
		wantK8sPuts   int32
		wantK8sPosts  int32
	}{
		{name: "successful full sync flow", loginStatus: http.StatusOK, loginBody: `{"accessToken":"mock-jwt-token"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"API_KEY","secretValue":"secret123"}]}`, k8sPutStatus: http.StatusOK, wantK8sPuts: 1},
		{name: "missing infisical credentials", cfgOverride: &Config{}, wantErr: true},
		{name: "login 500 server error", loginStatus: http.StatusInternalServerError, loginBody: "server error", wantErr: true},
		{name: "login invalid json response", loginStatus: http.StatusOK, loginBody: "{invalid-json", wantErr: true},
		{name: "login empty token string", loginStatus: http.StatusOK, loginBody: `{"accessToken":""}`, wantErr: true},
		{name: "login fallback token field", loginStatus: http.StatusOK, loginBody: `{"token":"fallback-token"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, k8sPutStatus: http.StatusOK, wantK8sPuts: 1},
		{name: "secrets fetch 502 bad gateway", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusBadGateway, wantErr: true},
		{name: "secrets fetch invalid json", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: "{invalid-secrets", wantErr: true},
		{name: "k8s put 201 created", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, k8sPutStatus: http.StatusCreated, wantK8sPuts: 1},
		{name: "k8s put 404 falls back to post 201 created", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, k8sPutStatus: http.StatusNotFound, k8sPostStatus: http.StatusCreated, wantK8sPuts: 1, wantK8sPosts: 1},
		{name: "k8s put 404 post fails 500", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, k8sPutStatus: http.StatusNotFound, k8sPostStatus: http.StatusInternalServerError, wantErr: true, wantK8sPuts: 1, wantK8sPosts: 1},
		{name: "missing kube api server", cfgOverride: &Config{InfisicalClientID: "cid", InfisicalClientSecret: "csec", InfisicalURL: "http://mock"}, loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, wantErr: true},
		{name: "k8s put 500 internal server error", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, k8sPutStatus: http.StatusInternalServerError, wantErr: true, wantK8sPuts: 1},
		{name: "unreachable client connections", unreachable: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.unreachable {
				badClientServer := NewRouterServer(Config{
					InfisicalURL:          "http://127.0.0.1:1",
					InfisicalClientID:     "cid",
					InfisicalClientSecret: "csec",
					KubeAPIServer:         "http://127.0.0.1:1",
				}, &http.Client{Transport: &zeroCallTransport{}, Timeout: 50 * time.Millisecond})

				if err := badClientServer.SyncSecrets(ctx, "ws", "env"); err == nil {
					t.Errorf("SyncSecrets() expected error on unreachable, got nil")
				}
				if _, err := badClientServer.fetchInfisicalSecrets(ctx, "tok", "ws", "env"); err == nil {
					t.Errorf("fetchInfisicalSecrets() expected error on unreachable, got nil")
				}
				if err := badClientServer.upsertK8sSecret(ctx, map[string]string{"A": "B"}); err == nil {
					t.Errorf("upsertK8sSecret() expected error on unreachable, got nil")
				}
				return
			}

			var k8sPutCount atomic.Int32
			var k8sPostCount atomic.Int32
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/universal-auth/login":
					w.WriteHeader(tc.loginStatus)
					_, _ = w.Write([]byte(tc.loginBody))
				case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw":
					w.WriteHeader(tc.secretsStatus)
					_, _ = w.Write([]byte(tc.secretsBody))
				case r.Method == http.MethodPut && r.URL.Path == "/api/v1/namespaces/default/secrets/shared-secrets":
					k8sPutCount.Add(1)
					w.WriteHeader(tc.k8sPutStatus)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/default/secrets":
					k8sPostCount.Add(1)
					w.WriteHeader(tc.k8sPostStatus)
				default:
					http.NotFound(w, r)
				}
			}))
			defer mockServer.Close()

			cfg := Config{
				InfisicalURL:          mockServer.URL,
				InfisicalClientID:     "client-id",
				InfisicalClientSecret: "client-secret",
				InfisicalProjectID:    "proj-123",
				InfisicalEnvironment:  "prod",
				KubeAPIServer:         mockServer.URL,
				KubeToken:             "test-kube-token",
			}
			if tc.cfgOverride != nil {
				cfg = *tc.cfgOverride
			}

			srv := NewRouterServer(cfg, mockServer.Client())
			err := srv.SyncSecrets(ctx, "proj-123", "prod")
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncSecrets() err = %v, wantErr %v", err, tc.wantErr)
			}
			if gotPuts := k8sPutCount.Load(); gotPuts != tc.wantK8sPuts {
				t.Errorf("k8s PUT count = %d, want %d", gotPuts, tc.wantK8sPuts)
			}
			if gotPosts := k8sPostCount.Load(); gotPosts != tc.wantK8sPosts {
				t.Errorf("k8s POST count = %d, want %d", gotPosts, tc.wantK8sPosts)
			}
		})
	}
}

func TestLoadK8sCertPool(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	emptyFile := filepath.Join(tempDir, "empty.crt")
	_ = os.WriteFile(emptyFile, []byte(""), 0o644)
	invalidFile := filepath.Join(tempDir, "invalid.crt")
	_ = os.WriteFile(invalidFile, []byte("not pem"), 0o644)

	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(mockTLSServer.Close)
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
		{name: "empty path returns nil", path: "", wantLoaded: false},
		{name: "nonexistent file returns nil", path: filepath.Join(tempDir, "nonexistent.crt"), wantLoaded: false},
		{name: "empty file returns nil", path: emptyFile, wantLoaded: false},
		{name: "invalid pem returns nil", path: invalidFile, wantLoaded: false},
		{name: "valid pem returns cert pool", path: validFile, wantLoaded: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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

func TestGetKubeToken(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "sa-token")
	_ = os.WriteFile(tokenFile, []byte("  projected-sa-token  \n"), 0o600)

	origTokenPath := kubeTokenPath
	t.Cleanup(func() { kubeTokenPath = origTokenPath })

	tests := []struct {
		name      string
		cfgToken  string
		tokenPath string
		wantToken string
	}{
		{name: "returns config token when set", cfgToken: "explicit-token", tokenPath: tokenFile, wantToken: "explicit-token"},
		{name: "reads from token file when config token empty", cfgToken: "", tokenPath: tokenFile, wantToken: "projected-sa-token"},
		{name: "returns empty string when token file missing", cfgToken: "", tokenPath: filepath.Join(tempDir, "missing-token"), wantToken: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeTokenPath = tc.tokenPath
			srv := &RouterServer{cfg: Config{KubeToken: tc.cfgToken}}
			got := srv.getKubeToken()
			if got != tc.wantToken {
				t.Errorf("getKubeToken() = %q, want %q", got, tc.wantToken)
			}
		})
	}
}

func TestGetK8sHTTPClient(t *testing.T) {
	tempDir := t.TempDir()
	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(mockTLSServer.Close)
	validPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: mockTLSServer.Certificate().Raw,
	})
	validCAFile := filepath.Join(tempDir, "ca.crt")
	_ = os.WriteFile(validCAFile, validPEM, 0o644)

	origCAPath := kubeCAPath
	t.Cleanup(func() { kubeCAPath = origCAPath })

	tests := []struct {
		name          string
		apiServer     string
		caPath        string
		hasHTTPClient bool
		wantCustom    bool
		wantRootCA    bool
	}{
		{
			name:          "custom client reused for http endpoint",
			apiServer:     "http://localhost:8080",
			caPath:        validCAFile,
			hasHTTPClient: true,
			wantCustom:    true,
		},
		{
			name:          "https endpoint builds client with root CA",
			apiServer:     "https://kubernetes.default.svc",
			caPath:        validCAFile,
			hasHTTPClient: true,
			wantRootCA:    true,
		},
		{
			name:          "https endpoint without root CA file builds transport without root CA",
			apiServer:     "https://kubernetes.default.svc",
			caPath:        filepath.Join(tempDir, "missing.crt"),
			hasHTTPClient: false,
			wantRootCA:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeCAPath = tc.caPath
			var customClient *http.Client
			if tc.hasHTTPClient {
				customClient = &http.Client{Transport: &zeroCallTransport{}, Timeout: 42 * time.Millisecond}
			}
			srv := &RouterServer{
				cfg:        Config{KubeAPIServer: tc.apiServer},
				httpClient: customClient,
			}
			client := srv.getK8sHTTPClient()
			if tc.wantCustom {
				if client != customClient {
					t.Fatalf("expected custom client to be returned, got %v", client)
				}
				return
			}
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
