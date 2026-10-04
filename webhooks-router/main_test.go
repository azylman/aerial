package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

type mockRiverInserter struct {
	mu                 sync.Mutex
	insertedJobs       []GitHubWebhookArgs
	insertedHangarJobs []HangarWebhookArgs
	insertedNomadJobs  []NomadEventArgs
	insertedOpts       []*river.InsertOpts
	err                error
}

func (m *mockRiverInserter) Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.insertedOpts = append(m.insertedOpts, opts)
	if ghArgs, ok := args.(GitHubWebhookArgs); ok {
		m.insertedJobs = append(m.insertedJobs, ghArgs)
	}
	if hArgs, ok := args.(HangarWebhookArgs); ok {
		m.insertedHangarJobs = append(m.insertedHangarJobs, hArgs)
	}
	if nArgs, ok := args.(NomadEventArgs); ok {
		m.insertedNomadJobs = append(m.insertedNomadJobs, nArgs)
	}
	return &rivertype.JobInsertResult{
		Job: &rivertype.JobRow{ID: 1},
	}, nil
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func TestPayloadHelpers_TableDriven(t *testing.T) {
	// 1. GetWorkspaceID
	p1 := InfisicalWebhookPayload{WorkspaceID: "ws-1"}
	if got := p1.GetWorkspaceID("fb"); got != "ws-1" {
		t.Errorf("expected ws-1, got %s", got)
	}

	p2 := InfisicalWebhookPayload{ProjectID: "proj-1"}
	if got := p2.GetWorkspaceID("fb"); got != "proj-1" {
		t.Errorf("expected proj-1, got %s", got)
	}

	p3 := InfisicalWebhookPayload{Data: &struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	}{WorkspaceID: "ws-data"}}
	if got := p3.GetWorkspaceID("fb"); got != "ws-data" {
		t.Errorf("expected ws-data, got %s", got)
	}

	p4 := InfisicalWebhookPayload{Data: &struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	}{ProjectID: "proj-data"}}
	if got := p4.GetWorkspaceID("fb"); got != "proj-data" {
		t.Errorf("expected proj-data, got %s", got)
	}

	p5 := InfisicalWebhookPayload{}
	if got := p5.GetWorkspaceID("fb"); got != "fb" {
		t.Errorf("expected fb, got %s", got)
	}

	// 2. GetEnvironment
	pEnv1 := InfisicalWebhookPayload{Environment: "prod"}
	if got := pEnv1.GetEnvironment("fb"); got != "prod" {
		t.Errorf("expected prod, got %s", got)
	}

	pEnv2 := InfisicalWebhookPayload{Data: &struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	}{Environment: "staging"}}
	if got := pEnv2.GetEnvironment("fb"); got != "staging" {
		t.Errorf("expected staging, got %s", got)
	}

	pEnv3 := InfisicalWebhookPayload{}
	if got := pEnv3.GetEnvironment("fb"); got != "fb" {
		t.Errorf("expected fb, got %s", got)
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	os.Setenv("PORT", "9999")
	os.Setenv("INFISICAL_URL", "http://infisical:8085/")
	os.Setenv("INFISICAL_CLIENT_ID", "cid")
	os.Setenv("INFISICAL_CLIENT_SECRET", "csec")
	os.Setenv("INFISICAL_PROJECT_ID", "pid")
	os.Setenv("INFISICAL_ENVIRONMENT", "staging")
	os.Setenv("NOMAD_ADDR", "http://nomad:4646/")
	os.Setenv("NOMAD_TOKEN", "ntok")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("INFISICAL_URL")
		os.Unsetenv("INFISICAL_CLIENT_ID")
		os.Unsetenv("INFISICAL_CLIENT_SECRET")
		os.Unsetenv("INFISICAL_PROJECT_ID")
		os.Unsetenv("INFISICAL_ENVIRONMENT")
		os.Unsetenv("NOMAD_ADDR")
		os.Unsetenv("NOMAD_TOKEN")
	}()

	cfg := LoadConfigFromEnv()
	if cfg.Port != "9999" || cfg.InfisicalURL != "http://infisical:8085" || cfg.NomadAddr != "http://nomad:4646" {
		t.Errorf("unexpected config loaded: %+v", cfg)
	}
	if cfg.InfisicalClientID != "cid" || cfg.InfisicalClientSecret != "csec" || cfg.InfisicalProjectID != "pid" {
		t.Errorf("unexpected infisical credentials: %+v", cfg)
	}
	if cfg.InfisicalEnvironment != "staging" {
		t.Errorf("unexpected env: %+v", cfg)
	}

	// Test alternate environment variable keys
	os.Unsetenv("INFISICAL_URL")
	os.Setenv("INFISICAL_HOST_URL", "http://alternate:8085")
	os.Unsetenv("INFISICAL_CLIENT_ID")
	os.Setenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID", "u-cid")
	os.Unsetenv("INFISICAL_CLIENT_SECRET")
	os.Setenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET", "u-csec")
	os.Unsetenv("INFISICAL_PROJECT_ID")
	os.Setenv("INFISICAL_WORKSPACE_ID", "w-id")

	cfgAlt := LoadConfigFromEnv()
	if cfgAlt.InfisicalURL != "http://alternate:8085" || cfgAlt.InfisicalClientID != "u-cid" ||
		cfgAlt.InfisicalClientSecret != "u-csec" || cfgAlt.InfisicalProjectID != "w-id" {
		t.Errorf("unexpected alternate config: %+v", cfgAlt)
	}
	os.Unsetenv("INFISICAL_HOST_URL")
	os.Unsetenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID")
	os.Unsetenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET")
	os.Unsetenv("INFISICAL_WORKSPACE_ID")

	// Test default fallback loading
	os.Unsetenv("PORT")
	os.Unsetenv("NOMAD_ADDR")
	os.Unsetenv("INFISICAL_ENVIRONMENT")
	cfgDef := LoadConfigFromEnv()
	if cfgDef.Port != "4020" || cfgDef.InfisicalURL != "http://127.0.0.1:8085" || cfgDef.NomadAddr != "http://127.0.0.1:4646" {
		t.Errorf("unexpected default config: %+v", cfgDef)
	}
}

func TestHandleHealthz(t *testing.T) {
	server := NewRouterServer(Config{Port: "4020"}, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode json body: %v", err)
	}
	if resp["status"] != "ok" || resp["service"] != "webhooks-router" {
		t.Errorf("unexpected healthz body: %v", resp)
	}
}

func TestHandleInfisicalWebhook(t *testing.T) {
	server := NewRouterServer(Config{}, nil)

	body := []byte(`{"event":"secrets.modified"}`)

	// 1. Valid payload -> 202
	req := httptest.NewRequest(http.MethodPost, "/webhooks/infisical", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d on valid payload, got %d", http.StatusAccepted, rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp["targetVariable"] != TargetNomadVariable {
		t.Errorf("expected targetVariable %q, got %v", TargetNomadVariable, resp["targetVariable"])
	}

	// 2. Malformed JSON -> 400
	badJSON := []byte(`{not valid json}`)
	reqBad := httptest.NewRequest(http.MethodPost, "/webhooks/infisical", strings.NewReader(string(badJSON)))
	recBad := httptest.NewRecorder()
	server.Routes().ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on malformed json, got %d", recBad.Code)
	}

	// 3. Body Read Error -> 400
	reqErr := httptest.NewRequest(http.MethodPost, "/webhooks/infisical", errReader{})
	recErr := httptest.NewRecorder()
	server.Routes().ServeHTTP(recErr, reqErr)
	if recErr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on read error, got %d", recErr.Code)
	}
}

func TestHandleGenericWebhook(t *testing.T) {
	server := NewRouterServer(Config{}, nil)

	// 1. Standard sync -> 202
	body := []byte(`{"environment":"prod"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode json body: %v", err)
	}
	if resp["targetVariable"] != TargetNomadVariable {
		t.Errorf("expected targetVariable %q, got %v", TargetNomadVariable, resp["targetVariable"])
	}

	// 2. Direct secrets map payload -> 202
	directBody := []byte(`{"secrets":{"KEY1":"VAL1"}}`)
	reqDirect := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(directBody)))
	recDirect := httptest.NewRecorder()
	server.Routes().ServeHTTP(recDirect, reqDirect)
	if recDirect.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", recDirect.Code)
	}
	var respDirect map[string]interface{}
	_ = json.Unmarshal(recDirect.Body.Bytes(), &respDirect)
	if respDirect["targetVariable"] != TargetNomadVariable || respDirect["message"] != "Direct secret sync scheduled" {
		t.Errorf("unexpected direct payload handling: %+v", respDirect)
	}

	// 3. Payload with explicit workspaceId and environment
	explicitBody := []byte(`{"workspaceId":"custom-ws","environment":"dev"}`)
	reqExp := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(explicitBody)))
	recExp := httptest.NewRecorder()
	server.Routes().ServeHTTP(recExp, reqExp)
	if recExp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", recExp.Code)
	}
	var respExp map[string]interface{}
	_ = json.Unmarshal(recExp.Body.Bytes(), &respExp)
	if respExp["targetVariable"] != TargetNomadVariable || respExp["environment"] != "dev" {
		t.Errorf("unexpected explicit payload handling: %+v", respExp)
	}

	// 4. Bad JSON -> 400
	badJSON := []byte(`{invalid`)
	reqB := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(badJSON)))
	recB := httptest.NewRecorder()
	server.Routes().ServeHTTP(recB, reqB)
	if recB.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on bad json, got %d", recB.Code)
	}

	// 5. Body Read Error -> 400
	reqErr := httptest.NewRequest(http.MethodPost, "/webhooks/generic", errReader{})
	recErr := httptest.NewRecorder()
	server.Routes().ServeHTTP(recErr, reqErr)
	if recErr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on read error, got %d", recErr.Code)
	}
}

func TestSyncSecrets_Flow(t *testing.T) {
	var nomadPutCount atomic.Int32
	var infisicalLoginCount atomic.Int32
	var infisicalFetchCount atomic.Int32

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/universal-auth/login":
			infisicalLoginCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{
				AccessToken: "mock-jwt-token",
			})

		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw":
			infisicalFetchCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(InfisicalSecretsResponse{
				Secrets: []InfisicalSecretItem{
					{SecretKey: "API_KEY", SecretValue: "secret123"},
				},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v1/var/"+TargetNomadVariable:
			nomadPutCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

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
		NomadAddr:             mockServer.URL,
		NomadToken:            "test-nomad-token",
	}
	server := NewRouterServer(cfg, mockServer.Client())

	ctx := context.Background()

	// 1. Initial sync writes directly to Nomad
	err := server.SyncSecrets(ctx, "proj-123", "prod")
	if err != nil {
		t.Fatalf("SyncSecrets failed: %v", err)
	}
	if nomadPutCount.Load() != 1 {
		t.Errorf("expected 1 Nomad PUT, got %d", nomadPutCount.Load())
	}

	// 2. Error branch: Missing Infisical Credentials
	serverBad := NewRouterServer(Config{}, mockServer.Client())
	if err := serverBad.SyncSecrets(ctx, "", ""); err == nil {
		t.Errorf("expected error with unconfigured infisical credentials")
	}
}

func TestSyncSecrets_ErrorBranches(t *testing.T) {
	// 1. Infisical Login returns 500 Internal Server Error
	mockServer500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer mockServer500.Close()

	cfg500 := Config{
		InfisicalURL:          mockServer500.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
		NomadAddr:             mockServer500.URL,
	}
	s500 := NewRouterServer(cfg500, mockServer500.Client())
	if err := s500.SyncSecrets(context.Background(), "", ""); err == nil {
		t.Errorf("expected error on infisical login 500")
	}

	// 2. Infisical Login returns invalid JSON
	mockServerLoginJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer mockServerLoginJSON.Close()
	sLoginJSON := NewRouterServer(Config{
		InfisicalURL:          mockServerLoginJSON.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
	}, mockServerLoginJSON.Client())
	if _, err := sLoginJSON.getInfisicalToken(context.Background()); err == nil {
		t.Errorf("expected error on invalid login json")
	}

	// 3. Infisical Login returns empty token
	mockServerEmptyTok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accessToken":""}`))
	}))
	defer mockServerEmptyTok.Close()
	sEmptyTok := NewRouterServer(Config{
		InfisicalURL:          mockServerEmptyTok.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
	}, mockServerEmptyTok.Client())
	if _, err := sEmptyTok.getInfisicalToken(context.Background()); err == nil {
		t.Errorf("expected error on empty login token")
	}

	// 4. Infisical Login fallback token field
	mockServerTokFallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"token":"fallback-token"}`))
	}))
	defer mockServerTokFallback.Close()
	sTokFallback := NewRouterServer(Config{
		InfisicalURL:          mockServerTokFallback.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
	}, mockServerTokFallback.Client())
	tok, err := sTokFallback.getInfisicalToken(context.Background())
	if err != nil || tok != "fallback-token" {
		t.Errorf("expected fallback token, got %q, err: %v", tok, err)
	}

	// 5. Infisical Fetch returns 502
	mockServerFetchErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/universal-auth/login" {
			_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "tok"})
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer mockServerFetchErr.Close()

	cfgFetch := Config{
		InfisicalURL:          mockServerFetchErr.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
		NomadAddr:             mockServerFetchErr.URL,
	}
	sFetch := NewRouterServer(cfgFetch, mockServerFetchErr.Client())
	if err := sFetch.SyncSecrets(context.Background(), "", ""); err == nil {
		t.Errorf("expected error on infisical fetch 502")
	}

	// 6. Infisical Fetch returns invalid JSON
	mockServerFetchJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-secrets"))
	}))
	defer mockServerFetchJSON.Close()
	sFetchJSON := NewRouterServer(Config{InfisicalURL: mockServerFetchJSON.URL}, mockServerFetchJSON.Client())
	if _, err := sFetchJSON.fetchInfisicalSecrets(context.Background(), "tok", "ws", "env"); err == nil {
		t.Errorf("expected error on invalid secrets json")
	}

	// 7. Nomad PUT returns 201 Created and 204 No Content
	mockServerNomad201 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer mockServerNomad201.Close()
	sNomad201 := NewRouterServer(Config{NomadAddr: mockServerNomad201.URL}, mockServerNomad201.Client())
	if err := sNomad201.putNomadVariable(context.Background(), map[string]string{"A": "B"}); err != nil {
		t.Errorf("expected success on 201 Created: %v", err)
	}

	mockServerNomad204 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer mockServerNomad204.Close()
	sNomad204 := NewRouterServer(Config{NomadAddr: mockServerNomad204.URL}, mockServerNomad204.Client())
	if err := sNomad204.putNomadVariable(context.Background(), map[string]string{"A": "B"}); err != nil {
		t.Errorf("expected success on 204 No Content: %v", err)
	}

	// 8. Nomad PUT returns 500
	mockServerPutErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/universal-auth/login":
			_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "tok"})
		case "/api/v3/secrets/raw":
			_ = json.NewEncoder(w).Encode(InfisicalSecretsResponse{
				Secrets: []InfisicalSecretItem{{SecretKey: "K", SecretValue: "V"}},
			})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer mockServerPutErr.Close()

	cfgPut := Config{
		InfisicalURL:          mockServerPutErr.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
		NomadAddr:             mockServerPutErr.URL,
	}
	sPut := NewRouterServer(cfgPut, mockServerPutErr.Client())
	if err := sPut.SyncSecrets(context.Background(), "", ""); err == nil {
		t.Errorf("expected error on nomad put 500")
	}

	// 9. HTTP Client connection errors (invalid / unreachable URL)
	badClientServer := NewRouterServer(Config{
		InfisicalURL:          "http://127.0.0.1:1",
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
		NomadAddr:             "http://127.0.0.1:1",
	}, &http.Client{Timeout: 50 * time.Millisecond})

	if _, err := badClientServer.getInfisicalToken(context.Background()); err == nil {
		t.Errorf("expected error on bad login client URL")
	}
	if _, err := badClientServer.fetchInfisicalSecrets(context.Background(), "tok", "ws", "env"); err == nil {
		t.Errorf("expected error on bad fetch client URL")
	}
	if err := badClientServer.putNomadVariable(context.Background(), map[string]string{"A": "B"}); err == nil {
		t.Errorf("expected error on bad nomad put client URL")
	}
}

func TestRunServer_LifecycleAndErrors(t *testing.T) {
	// 1. Clean lifecycle
	cfg := Config{
		Port: "0", // Dynamic available port
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)

	done := make(chan error, 1)
	go func() {
		done <- runServer(ctx, cfg, func(addr string) {
			ready <- addr
		})
	}()

	select {
	case addr := <-ready:
		if addr == "" {
			t.Errorf("expected non-empty listening address")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server to start")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected clean shutdown, got error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server shutdown")
	}

	// 2. Listen error (invalid port)
	cfgErr := Config{Port: "-1"}
	if err := runServer(context.Background(), cfgErr, nil); err == nil {
		t.Errorf("expected error on invalid port -1")
	}

	// 3. Test runApp helper with canceled context
	ctxC, cancelC := context.WithCancel(context.Background())
	cancelC()
	if err := runApp(ctxC, Config{Port: "0"}); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("unexpected error from runApp: %v", err)
	}

	// 4. Invalid Postgres URL error
	cfgErrPg := Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}
	if err := runServer(context.Background(), cfgErrPg, nil); err == nil {
		t.Errorf("expected error on invalid postgres url")
	}
}

func TestIsZeroSHA(t *testing.T) {
	tests := []struct {
		sha  string
		want bool
	}{
		{"", true},
		{"0000000000000000000000000000000000000000", true},
		{"0", true},
		{"000", true},
		{"0000000000000000000000000000000000000001", false},
		{"ed197571a381920f540e7a3c55b68008a45912a0", false},
	}
	for _, tc := range tests {
		if got := isZeroSHA(tc.sha); got != tc.want {
			t.Errorf("isZeroSHA(%q) = %v, want %v", tc.sha, got, tc.want)
		}
	}
}

func TestLoadConfigFromEnv_GitHub(t *testing.T) {
	os.Setenv("GITHUB_PAT", "pat123")
	os.Setenv("GITHUB_API_URL", "https://api.github.test/")
	os.Setenv("POSTGRES_URL", "postgres://custom:pass@db:5432/customdb")
	defer func() {
		os.Unsetenv("GITHUB_PAT")
		os.Unsetenv("GITHUB_API_URL")
		os.Unsetenv("POSTGRES_URL")
	}()

	cfg := LoadConfigFromEnv()
	if cfg.GitHubToken != "pat123" {
		t.Errorf("expected GitHubToken pat123, got %s", cfg.GitHubToken)
	}
	if cfg.GitHubAPIURL != "https://api.github.test" {
		t.Errorf("expected GitHubAPIURL https://api.github.test, got %s", cfg.GitHubAPIURL)
	}
	if cfg.PostgresURL != "postgres://custom:pass@db:5432/customdb" {
		t.Errorf("expected PostgresURL from env, got %s", cfg.PostgresURL)
	}

	// Test fallback to GITHUB_TOKEN and DATABASE_URL
	os.Unsetenv("GITHUB_PAT")
	os.Unsetenv("POSTGRES_URL")
	os.Setenv("GITHUB_TOKEN", "tok456")
	os.Setenv("DATABASE_URL", "postgres://dbuser:pass@db:5432/db")
	defer func() {
		os.Unsetenv("GITHUB_TOKEN")
		os.Unsetenv("DATABASE_URL")
	}()
	cfg2 := LoadConfigFromEnv()
	if cfg2.GitHubToken != "tok456" {
		t.Errorf("expected GitHubToken tok456, got %s", cfg2.GitHubToken)
	}
	if cfg2.PostgresURL != "postgres://dbuser:pass@db:5432/db" {
		t.Errorf("expected PostgresURL from DATABASE_URL, got %s", cfg2.PostgresURL)
	}
}

func TestHandleGitHubWebhook_EndpointsAndValidation(t *testing.T) {
	mockRiver := &mockRiverInserter{}
	srv := NewRouterServer(Config{}, nil, mockRiver)
	routes := srv.Routes()

	// 1. Missing X-GitHub-Event header -> 400 Bad Request
	reqMissingEvent := httptest.NewRequest(http.MethodPost, "/api/webhooks/github", strings.NewReader(`{}`))
	recMissingEvent := httptest.NewRecorder()
	routes.ServeHTTP(recMissingEvent, reqMissingEvent)
	if recMissingEvent.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing event header, got %d", recMissingEvent.Code)
	}

	// 2. Read error in body -> 400 Bad Request
	reqErrBody := httptest.NewRequest(http.MethodPost, "/api/webhooks/github", errReader{})
	reqErrBody.Header.Set("X-GitHub-Event", "ping")
	recErrBody := httptest.NewRecorder()
	routes.ServeHTTP(recErrBody, reqErrBody)
	if recErrBody.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request on read error, got %d", recErrBody.Code)
	}

	// 3. River client not initialized (nil) -> 500 Internal Server Error
	srvNilRiver := NewRouterServer(Config{}, nil)
	reqNilRiver := httptest.NewRequest(http.MethodPost, "/api/webhooks/github", strings.NewReader(`{"zen":"Keep it simple"}`))
	reqNilRiver.Header.Set("X-GitHub-Event", "ping")
	recNilRiver := httptest.NewRecorder()
	srvNilRiver.Routes().ServeHTTP(recNilRiver, reqNilRiver)
	if recNilRiver.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 when river client is nil, got %d", recNilRiver.Code)
	}

	// 4. River client failure (e.g. database error) -> 500 Internal Server Error (never return 200 OK without durable persistence)
	mockRiverErr := &mockRiverInserter{err: errors.New("simulated database failure")}
	srvErrRiver := NewRouterServer(Config{}, nil, mockRiverErr)
	reqErrRiver := httptest.NewRequest(http.MethodPost, "/api/webhooks/github", strings.NewReader(`{"zen":"Keep it simple"}`))
	reqErrRiver.Header.Set("X-GitHub-Event", "ping")
	recErrRiver := httptest.NewRecorder()
	srvErrRiver.Routes().ServeHTTP(recErrRiver, reqErrRiver)
	if recErrRiver.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 when river persistence fails, got %d", recErrRiver.Code)
	}

	// 5. Successful durable persistence -> 200 OK on valid request (testing both routes)
	for _, path := range []string{"/api/webhooks/github", "/webhooks/github"} {
		mockRiver.insertedJobs = nil
		reqValid := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"zen":"Keep it simple"}`))
		reqValid.Header.Set("X-GitHub-Event", "ping")
		reqValid.Header.Set("X-GitHub-Delivery", "del-1234")
		recValid := httptest.NewRecorder()
		routes.ServeHTTP(recValid, reqValid)
		if recValid.Code != http.StatusOK {
			t.Errorf("expected 200 OK on %s, got %d", path, recValid.Code)
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(recValid.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse json response: %v", err)
		}
		if resp["status"] != "accepted" || resp["event"] != "ping" || resp["delivery"] != "del-1234" {
			t.Errorf("unexpected response body: %+v", resp)
		}
		if len(mockRiver.insertedJobs) != 1 {
			t.Fatalf("expected 1 job inserted into river, got %d", len(mockRiver.insertedJobs))
		}
		if mockRiver.insertedJobs[0].Event != "ping" || mockRiver.insertedJobs[0].Delivery != "del-1234" {
			t.Errorf("unexpected inserted job: %+v", mockRiver.insertedJobs[0])
		}
	}
}

func TestGitHubWebhookWorker_Work(t *testing.T) {
	srv := NewRouterServer(Config{}, nil)
	worker := &GitHubWebhookWorker{server: srv}

	// 1. Success on valid ping event
	jobValid := &river.Job[GitHubWebhookArgs]{
		JobRow: &rivertype.JobRow{ID: 101},
		Args: GitHubWebhookArgs{
			Event:    "ping",
			Delivery: "del-worker-1",
			Body:     []byte(`{"zen":"Responsive is better than fast.","hook_id":123,"repository":{"full_name":"azylman/aerial"}}`),
		},
	}
	if err := worker.Work(context.Background(), jobValid); err != nil {
		t.Fatalf("worker.Work failed on valid job: %v", err)
	}

	// 2. Error when server is nil
	nilWorker := &GitHubWebhookWorker{server: nil}
	if err := nilWorker.Work(context.Background(), jobValid); err == nil {
		t.Errorf("expected error when worker server is nil")
	}

	// 3. Error when ProcessGitHubEvent fails (e.g. malformed JSON)
	jobMalformed := &river.Job[GitHubWebhookArgs]{
		JobRow: &rivertype.JobRow{ID: 102},
		Args: GitHubWebhookArgs{
			Event:    "pull_request",
			Delivery: "del-worker-2",
			Body:     []byte(`{malformed`),
		},
	}
	if err := worker.Work(context.Background(), jobMalformed); err == nil {
		t.Errorf("expected error when event processing fails")
	}
}

func TestHangarWebhookWorker_Work(t *testing.T) {
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{}, nil)
	srv.SetDispatcher(mockDisp)
	worker := &HangarWebhookWorker{server: srv}

	// 1. Success on valid deploy_success event
	jobValid := &river.Job[HangarWebhookArgs]{
		JobRow: &rivertype.JobRow{ID: 201},
		Args: HangarWebhookArgs{
			Event: HangarDeployEvent{
				Event:    "deploy_success",
				JobName:  "brain",
				Repo:     "azylman/aerial",
				PRNumber: 558,
				TargetID: "1555405874565091380",
				Status:   "success",
			},
		},
	}

	if err := worker.Work(context.Background(), jobValid); err != nil {
		t.Fatalf("unexpected worker error: %v", err)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call dispatched synchronously, got %d", len(mockDisp.PromptCalls()))
	}

	// 2. Error when worker server is nil
	nilWorker := &HangarWebhookWorker{server: nil}
	if err := nilWorker.Work(context.Background(), jobValid); err == nil {
		t.Errorf("expected error when worker server is nil")
	}

	// 3. Error bubbling on downstream failure
	mockDispErr := &mockOutboundDispatcher{promptErr: errors.New("brain prompt endpoint 503")}
	srvErr := NewRouterServer(Config{}, nil)
	srvErr.SetDispatcher(mockDispErr)
	workerErr := &HangarWebhookWorker{server: srvErr}

	if err := workerErr.Work(context.Background(), jobValid); err == nil {
		t.Errorf("expected error when downstream dispatch fails, got nil")
	}
}

func TestNomadEventWorker_Work(t *testing.T) {
	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-1","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    552,
		deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
	}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)
	worker := &NomadEventWorker{server: srv}

	// 1. Success on valid Deployment event
	depPayload := []byte(`{"Deployment":{"ID":"dep-101","JobID":"brain","Status":"successful"}}`)
	jobDep := &river.Job[NomadEventArgs]{
		JobRow: &rivertype.JobRow{ID: 301},
		Args: NomadEventArgs{
			Topic:   "Deployment",
			Type:    "DeploymentStatusUpdate",
			Payload: depPayload,
		},
	}

	if err := worker.Work(context.Background(), jobDep); err != nil {
		t.Fatalf("unexpected worker error on deployment: %v", err)
	}
	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "brain" {
		t.Errorf("expected deployedJobCalls [brain], got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call dispatched, got %d", len(mockDisp.PromptCalls()))
	}

	// 2. Success on valid Allocation event
	mockReg.deployedJobCalls = nil
	mockDisp.promptCalls = nil
	allocPayload := []byte(`{"Allocation":{"ID":"alloc-202","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}`)
	jobAlloc := &river.Job[NomadEventArgs]{
		JobRow: &rivertype.JobRow{ID: 302},
		Args: NomadEventArgs{
			Topic:   "Allocation",
			Type:    "AllocationUpdated",
			Payload: allocPayload,
		},
	}

	if err := worker.Work(context.Background(), jobAlloc); err != nil {
		t.Fatalf("unexpected worker error on allocation: %v", err)
	}
	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "scheduler-mcp" {
		t.Errorf("expected deployedJobCalls [scheduler-mcp], got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call dispatched, got %d", len(mockDisp.PromptCalls()))
	}

	// 3. Error when worker server is nil
	nilWorker := &NomadEventWorker{server: nil}
	if err := nilWorker.Work(context.Background(), jobDep); err == nil {
		t.Errorf("expected error when worker server is nil")
	}

	// 4. Error bubbling on downstream failure (e.g. prompt dispatch failure)
	mockReg.deployedJobCalls = nil
	mockDispErr := &mockOutboundDispatcher{promptErr: errors.New("brain prompt 503")}
	srvErr := NewRouterServer(Config{}, nil)
	srvErr.SetRegistry(mockReg)
	srvErr.SetDispatcher(mockDispErr)
	workerErr := &NomadEventWorker{server: srvErr}

	if err := workerErr.Work(context.Background(), jobDep); err == nil {
		t.Errorf("expected error when downstream prompt dispatch fails, got nil")
	}
}

func TestProcessGitHubEvent_Ping(t *testing.T) {
	srv := NewRouterServer(Config{}, nil)
	ctx := context.Background()

	payload := `{
		"zen": "Responsive is better than fast.",
		"hook_id": 98765,
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "ping", "del-ping-1", []byte(payload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent ping failed: %v", err)
	}
	if res.Event != "ping" || res.Repo != "azylman/aerial" {
		t.Errorf("unexpected ping result: %+v", res)
	}

	// Malformed JSON
	_, err = srv.ProcessGitHubEvent(ctx, "ping", "del-ping-2", []byte(`{invalid`))
	if err == nil {
		t.Errorf("expected error on malformed ping json")
	}
}

func TestProcessGitHubEvent_PullRequest(t *testing.T) {
	srv := NewRouterServer(Config{}, nil)
	ctx := context.Background()

	payload := `{
		"action": "closed",
		"number": 512,
		"pull_request": {
			"number": 512,
			"title": "feat: test pr",
			"head": {
				"ref": "feat/test",
				"sha": "headsha123"
			},
			"base": {
				"ref": "main",
				"sha": "basesha456"
			},
			"merged": true,
			"merge_commit_sha": "mergesha789"
		},
		"repository": {
			"full_name": "azylman/aerial"
		},
		"sender": {
			"login": "arcane103"
		}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-1", []byte(payload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent pull_request failed: %v", err)
	}
	if res.Repo != "azylman/aerial" || res.PRNumber != 512 || res.Action != "closed" ||
		res.HeadSHA != "headsha123" || res.MergeSHA != "mergesha789" || res.Branch != "feat/test" ||
		res.ResolutionSource != "pull_request_payload" || res.Sender != "arcane103" {
		t.Errorf("unexpected pull_request result: %+v", res)
	}

	// Malformed JSON
	_, err = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-2", []byte(`{invalid`))
	if err == nil {
		t.Errorf("expected error on malformed pull_request json")
	}
}

func TestProcessGitHubEvent_CheckRun(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/commit-fallback/pulls") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"number": 777, "state": "open", "head": {"ref": "feat/fallback", "sha": "commit-fallback"}}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockServer.Close()

	srv := NewRouterServer(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client())
	ctx := context.Background()

	// 1. CheckRun with pull_requests array
	payloadWithPR := `{
		"action": "completed",
		"check_run": {
			"id": 111,
			"name": "Unit Tests",
			"head_sha": "headsha111",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": [
				{
					"number": 515,
					"head": {"ref": "feat/awesome", "sha": "headsha111"}
				}
			]
		},
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-1", []byte(payloadWithPR))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent check_run failed: %v", err)
	}
	if res.PRNumber != 515 || res.Branch != "feat/awesome" || res.ResolutionSource != "check_run_payload" || res.Conclusion != "success" {
		t.Errorf("unexpected check_run result: %+v", res)
	}

	// 2. CheckRun without pull_requests array (e.g. fork PR) -> resolves via commit SHA lookup
	payloadFallback := `{
		"action": "completed",
		"check_run": {
			"id": 222,
			"name": "Integration Tests",
			"head_sha": "commit-fallback",
			"status": "completed",
			"conclusion": "failure",
			"pull_requests": []
		},
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	resFallback, err := srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-2", []byte(payloadFallback))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent check_run fallback failed: %v", err)
	}
	if resFallback.PRNumber != 777 || resFallback.Branch != "feat/fallback" || resFallback.ResolutionSource != "commit_sha_lookup" || resFallback.Conclusion != "failure" {
		t.Errorf("unexpected check_run fallback result: %+v", resFallback)
	}

	// Malformed JSON
	_, err = srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-3", []byte(`{invalid`))
	if err == nil {
		t.Errorf("expected error on malformed check_run json")
	}
}

func TestProcessGitHubEvent_WorkflowRun(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/wf-fallback/pulls") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"number": 888, "state": "open", "head": {"ref": "feat/wf-branch", "sha": "wf-fallback"}}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockServer.Close()

	srv := NewRouterServer(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client())
	ctx := context.Background()

	// 1. WorkflowRun with PR array
	payloadWithPR := `{
		"action": "completed",
		"workflow_run": {
			"id": 333,
			"name": "Continuous Delivery",
			"head_sha": "headsha333",
			"head_branch": "feat/cd",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": [
				{
					"number": 516,
					"head": {"ref": "feat/cd", "sha": "headsha333"}
				}
			]
		},
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-1", []byte(payloadWithPR))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent workflow_run failed: %v", err)
	}
	if res.PRNumber != 516 || res.Branch != "feat/cd" || res.ResolutionSource != "workflow_run_payload" {
		t.Errorf("unexpected workflow_run result: %+v", res)
	}

	// 2. WorkflowRun fallback to commit lookup
	payloadFallback := `{
		"action": "completed",
		"workflow_run": {
			"id": 444,
			"name": "Continuous Delivery",
			"head_sha": "wf-fallback",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": []
		},
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	resFallback, err := srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-2", []byte(payloadFallback))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent workflow_run fallback failed: %v", err)
	}
	if resFallback.PRNumber != 888 || resFallback.Branch != "feat/wf-branch" || resFallback.ResolutionSource != "commit_sha_lookup" {
		t.Errorf("unexpected workflow_run fallback result: %+v", resFallback)
	}

	// Malformed JSON
	_, err = srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-3", []byte(`{invalid`))
	if err == nil {
		t.Errorf("expected error on malformed workflow_run json")
	}
}

func TestProcessGitHubEvent_Push(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/squash123/pulls") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"number": 999, "state": "closed", "head": {"ref": "feat/squash", "sha": "squash123"}}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockServer.Close()

	srv := NewRouterServer(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client())
	ctx := context.Background()

	// 1. Standard push to main with commit resolution
	payloadMain := `{
		"ref": "refs/heads/main",
		"before": "prev111",
		"after": "squash123",
		"deleted": false,
		"head_commit": {
			"id": "squash123",
			"message": "feat: merged feature (#999)"
		},
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-1", []byte(payloadMain))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push failed: %v", err)
	}
	if res.PRNumber != 999 || res.MergeSHA != "squash123" || res.ResolutionSource != "commit_sha_lookup" {
		t.Errorf("unexpected push result: %+v", res)
	}

	// 2. Branch deletion push (deleted: true or zero SHA)
	payloadDeleted := `{
		"ref": "refs/heads/feat/temp",
		"before": "temp111",
		"after": "0000000000000000000000000000000000000000",
		"deleted": true,
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	resDel, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-2", []byte(payloadDeleted))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push deleted failed: %v", err)
	}
	if resDel.ResolutionSource != "branch_deleted" || resDel.PRNumber != 0 {
		t.Errorf("unexpected push deletion result: %+v", resDel)
	}

	// 3. Tag push (non-branch ref)
	payloadTag := `{
		"ref": "refs/tags/v1.0.0",
		"after": "tagsha111",
		"repository": {
			"full_name": "azylman/aerial"
		}
	}`

	resTag, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-3", []byte(payloadTag))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push tag failed: %v", err)
	}
	if resTag.ResolutionSource != "non_branch_ref" || resTag.PRNumber != 0 {
		t.Errorf("unexpected push tag result: %+v", resTag)
	}

	// Malformed JSON
	_, err = srv.ProcessGitHubEvent(ctx, "push", "del-push-4", []byte(`{invalid`))
	if err == nil {
		t.Errorf("expected error on malformed push json")
	}
}

func TestProcessGitHubEvent_Unhandled(t *testing.T) {
	srv := NewRouterServer(Config{}, nil)
	ctx := context.Background()

	res, err := srv.ProcessGitHubEvent(ctx, "star", "del-star-1", []byte(`{"action":"created"}`))
	if err != nil {
		t.Fatalf("unexpected error on unhandled event: %v", err)
	}
	if res.ResolutionSource != "unhandled_event" {
		t.Errorf("expected unhandled_event, got %s", res.ResolutionSource)
	}
}

func TestResolvePRFromCommitSHA_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	// 1. Validation errors
	srv := NewRouterServer(Config{}, nil)
	if _, _, err := srv.resolvePRFromCommitSHA(ctx, "", "sha"); err == nil {
		t.Errorf("expected error on empty repo")
	}
	if _, _, err := srv.resolvePRFromCommitSHA(ctx, "repo", ""); err == nil {
		t.Errorf("expected error on empty sha")
	}
	if _, _, err := srv.resolvePRFromCommitSHA(ctx, "repo", "0000000000000000000000000000000000000000"); err == nil {
		t.Errorf("expected error on zero sha")
	}

	// 2. HTTP 403 Rate Limited
	rateLimitServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer rateLimitServer.Close()

	srvRateLimit := NewRouterServer(Config{GitHubAPIURL: rateLimitServer.URL, GitHubToken: "secret-token"}, rateLimitServer.Client())
	if _, _, err := srvRateLimit.resolvePRFromCommitSHA(ctx, "aerial", "sha123"); err == nil {
		t.Errorf("expected error on 403 rate limit")
	}

	// 3. HTTP 404 Not Found
	notFoundServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer notFoundServer.Close()

	srvNotFound := NewRouterServer(Config{GitHubAPIURL: notFoundServer.URL}, notFoundServer.Client())
	if _, _, err := srvNotFound.resolvePRFromCommitSHA(ctx, "aerial", "sha123"); err == nil {
		t.Errorf("expected error on 404 status")
	}

	// 4. Malformed JSON response
	malformedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{not-an-array`))
	}))
	defer malformedServer.Close()

	srvMalformed := NewRouterServer(Config{GitHubAPIURL: malformedServer.URL}, malformedServer.Client())
	if _, _, err := srvMalformed.resolvePRFromCommitSHA(ctx, "aerial", "sha123"); err == nil {
		t.Errorf("expected error on malformed json")
	}

	// 5. Empty PR list returned
	emptyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer emptyServer.Close()

	srvEmpty := NewRouterServer(Config{GitHubAPIURL: emptyServer.URL}, emptyServer.Client())
	if _, _, err := srvEmpty.resolvePRFromCommitSHA(ctx, "aerial", "sha123"); err == nil {
		t.Errorf("expected error on empty pr list")
	}
}

func getTestPostgresURL() string {
	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = os.Getenv("DATABASE_URL")
	}
	if pgURL != "" {
		return pgURL
	}
	if _, err := net.LookupHost("aerial-postgres"); err == nil {
		return "postgres://aerial:aerial_secure_pass@aerial-postgres:5432/aerial?sslmode=disable"
	}
	if _, err := net.LookupHost("postgres"); err == nil {
		return "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
	}
	if conn, err := net.DialTimeout("tcp", "127.0.0.1:5432", 150*time.Millisecond); err == nil {
		conn.Close()
		return "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
	}
	if conn, err := net.DialTimeout("tcp", "192.168.1.14:5432", 150*time.Millisecond); err == nil {
		conn.Close()
		return "postgres://aerial:aerial_secure_pass@192.168.1.14:5432/aerial?sslmode=disable"
	}
	return "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
}

func TestRiverLiveIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live River integration test in short mode")
	}

	pgURL := getTestPostgresURL()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		t.Skipf("cannot parse postgres url: %v", err)
	}
	poolConfig.MaxConns = 5
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, ok := poolConfig.ConnConfig.RuntimeParams["search_path"]; !ok {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = "sidecars, public"
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Skipf("skipping live test, could not create pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping live test, postgres ping failed: %v", err)
	}

	server := NewRouterServer(Config{PostgresURL: pgURL}, nil)
	processedCh := make(chan string, 1)
	server.onProcessed = func(event, delivery string) {
		if delivery == "live-integration-delivery" {
			processedCh <- event
		}
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &GitHubWebhookWorker{server: server})

	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 5},
		},
		Workers: workers,
	})
	if err != nil {
		t.Fatalf("failed to create river client: %v", err)
	}
	server.SetRiverClient(riverClient)

	if err := riverClient.Start(ctx); err != nil {
		t.Fatalf("failed to start river client: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		_ = riverClient.Stop(stopCtx)
	}()

	// Perform HTTP request to server route
	reqBody := `{"zen":"Responsive is better than fast.","hook_id":9999,"repository":{"full_name":"azylman/aerial"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/webhooks/github", strings.NewReader(reqBody))
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set("X-GitHub-Delivery", "live-integration-delivery")
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from handleGitHubWebhook, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify asynchronous worker (either in-test worker or live cluster worker) picked up and processed job
	select {
	case event := <-processedCh:
		if event != "ping" {
			t.Errorf("expected processed event ping, got %s", event)
		}
	case <-time.After(2 * time.Second):
		// Check if the live background cluster worker picked up and completed the job
		var state string
		queryErr := pool.QueryRow(ctx, "SELECT state FROM river_job WHERE args->>'delivery' = 'live-integration-delivery' ORDER BY id DESC LIMIT 1").Scan(&state)
		if queryErr == nil && (state == "completed" || state == "running") {
			t.Logf("Job was successfully processed by live cluster worker (state=%s)", state)
			return
		}
		t.Fatalf("timeout waiting for River worker to process live job from postgres (db state: %s, queryErr: %v)", state, queryErr)
	}
}

type mockPRRegistry struct {
	mergedCalls         []struct{ repo string; prNumber int; mergeSHA string }
	syncCalls           []struct{ repo string; prNumber int; headSHA string }
	closedUnmergedCalls []struct{ repo string; prNumber int }
	ciFailedCalls       []struct{ repo string; prNumber int }
	ciFailedBySHACalls  []struct{ repo string; headSHA string }
	backfillCalls       []struct{ repo string; prNumber int; mergeSHA string }
	resolveBySHACalls   []struct{ repo string; sha string }

	mergedTargetID      string
	syncTargetID        string
	closedTargetID      string
	backfillTargetID    string

	resolvePRNum        int
	resolveBranch       string
	resolveTargetID     string
	resolveErr          error

	ciFailedTargetID     string
	ciFailedUpdated      bool
	ciFailedErr          error
	ciFailedBySHAPRNum   int
	ciFailedBySHATarget  string
	ciFailedBySHAUpdated bool
	ciFailedBySHAErr     error

	listOpenPRsCalls []string
	openPRs          []RegisteredPR
	listOpenPRsErr   error

	conflictCalls    []struct{ repo string; prNumber int }
	conflictTargetID string
	conflictUpdated  bool
	conflictErr      error

	deployingCalls []struct {
		repo     string
		prNumber int
		jobs     []string
	}
	deployingTargetID string
	deployingErr      error

	deployedJobCalls []string
	deployedTargetID string
	deployedPRNum    int
	deployedMergeSHA string
	deployedRepo     string
	deployedUpdated  bool
	deployedErr      error

	deployFailedCalls    []struct{ repo string; prNumber int }
	deployFailedTargetID string
	deployFailedUpdated  bool
	deployFailedErr      error

	deployFailedJobCalls    []string
	deployFailedJobTargetID string
	deployFailedJobPRNum    int
	deployFailedJobMergeSHA string
	deployFailedJobRepo     string
	deployFailedJobUpdated  bool
	deployFailedJobErr      error

	listDeployingPRs []DeployingPR
	listDeployingErr error

	channelContextMap map[string][]ChannelMessageContext
	channelContextErr error

	err error
}

func (m *mockPRRegistry) TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (string, error) {
	if m.deployingErr != nil {
		return "", m.deployingErr
	}
	if m.err != nil {
		return "", m.err
	}
	m.deployingCalls = append(m.deployingCalls, struct {
		repo     string
		prNumber int
		jobs     []string
	}{repo, prNumber, jobs})
	return m.deployingTargetID, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	if m.deployedErr != nil {
		return "", 0, "", "", false, m.deployedErr
	}
	if m.err != nil {
		return "", 0, "", "", false, m.err
	}
	m.deployedJobCalls = append(m.deployedJobCalls, jobName)
	return m.deployedTargetID, m.deployedPRNum, m.deployedMergeSHA, m.deployedRepo, m.deployedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if m.deployFailedErr != nil {
		return "", false, m.deployFailedErr
	}
	if m.err != nil {
		return "", false, m.err
	}
	m.deployFailedCalls = append(m.deployFailedCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.deployFailedTargetID, m.deployFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	if m.deployFailedJobErr != nil {
		return "", 0, "", "", false, m.deployFailedJobErr
	}
	if m.err != nil {
		return "", 0, "", "", false, m.err
	}
	m.deployFailedJobCalls = append(m.deployFailedJobCalls, jobName)
	return m.deployFailedJobTargetID, m.deployFailedJobPRNum, m.deployFailedJobMergeSHA, m.deployFailedJobRepo, m.deployFailedJobUpdated, nil
}

func (m *mockPRRegistry) ListDeployingPRs(ctx context.Context) ([]DeployingPR, error) {
	if m.listDeployingErr != nil {
		return nil, m.listDeployingErr
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.listDeployingPRs, nil
}

func (m *mockPRRegistry) ListOpenPRs(ctx context.Context, repo string) ([]RegisteredPR, error) {
	if m.listOpenPRsErr != nil {
		return nil, m.listOpenPRsErr
	}
	if m.err != nil {
		return nil, m.err
	}
	m.listOpenPRsCalls = append(m.listOpenPRsCalls, repo)
	return m.openPRs, nil
}

func (m *mockPRRegistry) GetRecentChannelContext(ctx context.Context, targetID string, limit int) ([]ChannelMessageContext, error) {
	if m.channelContextErr != nil {
		return nil, m.channelContextErr
	}
	if m.channelContextMap != nil {
		return m.channelContextMap[targetID], nil
	}
	return nil, nil
}

func (m *mockPRRegistry) AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if m.conflictErr != nil {
		return "", false, m.conflictErr
	}
	if m.err != nil {
		return "", false, m.err
	}
	m.conflictCalls = append(m.conflictCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.conflictTargetID, m.conflictUpdated, nil
}

func (m *mockPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.mergedCalls = append(m.mergedCalls, struct{ repo string; prNumber int; mergeSHA string }{repo, prNumber, mergeSHA})
	return m.mergedTargetID, nil
}

func (m *mockPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.syncCalls = append(m.syncCalls, struct{ repo string; prNumber int; headSHA string }{repo, prNumber, headSHA})
	return m.syncTargetID, nil
}

func (m *mockPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.closedUnmergedCalls = append(m.closedUnmergedCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.closedTargetID, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if m.ciFailedErr != nil {
		return "", false, m.ciFailedErr
	}
	if m.err != nil {
		return "", false, m.err
	}
	m.ciFailedCalls = append(m.ciFailedCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.ciFailedTargetID, m.ciFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	if m.ciFailedBySHAErr != nil {
		return "", 0, false, m.ciFailedBySHAErr
	}
	if m.err != nil {
		return "", 0, false, m.err
	}
	m.ciFailedBySHACalls = append(m.ciFailedBySHACalls, struct{ repo string; headSHA string }{repo, headSHA})
	return m.ciFailedBySHATarget, m.ciFailedBySHAPRNum, m.ciFailedBySHAUpdated, nil
}

func (m *mockPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.backfillCalls = append(m.backfillCalls, struct{ repo string; prNumber int; mergeSHA string }{repo, prNumber, mergeSHA})
	return m.backfillTargetID, nil
}

func (m *mockPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if m.resolveErr != nil {
		return 0, "", "", m.resolveErr
	}
	if m.err != nil {
		return 0, "", "", m.err
	}
	m.resolveBySHACalls = append(m.resolveBySHACalls, struct{ repo string; sha string }{repo, sha})
	return m.resolvePRNum, m.resolveBranch, m.resolveTargetID, nil
}

func (m *mockPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	if m.resolveErr != nil {
		return "", "", "", "", m.resolveErr
	}
	if m.err != nil {
		return "", "", "", "", m.err
	}
	return m.resolveTargetID, m.resolveBranch, "", "", nil
}

func TestNormalizeRepo(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"   ", ""},
		{"aerial", "azylman/aerial"},
		{"azylman/aerial", "azylman/aerial"},
		{"  mirrormere  ", "azylman/mirrormere"},
		{"otherorg/custom", "otherorg/custom"},
	}

	for _, tt := range tests {
		got := normalizeRepo(tt.input)
		if got != tt.expected {
			t.Errorf("normalizeRepo(%q) = %q; expected %q", tt.input, got, tt.expected)
		}
	}
}

func TestProcessGitHubEvent_RegistryTransitions(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{
		mergedTargetID:       "target-merged-1",
		closedTargetID:       "target-closed-1",
		syncTargetID:         "target-sync-1",
		ciFailedTargetID:     "target-111",
		ciFailedUpdated:      true,
		ciFailedBySHAPRNum:   520,
		ciFailedBySHATarget:  "target-222",
		ciFailedBySHAUpdated: true,
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)

	// 1. PullRequest closed and merged
	payloadPRMerged := `{
		"action": "closed",
		"number": 515,
		"pull_request": {
			"number": 515,
			"head": {"ref": "feat/decouple", "sha": "head111"},
			"merged": true,
			"merge_commit_sha": "merge111"
		},
		"repository": {"full_name": "azylman/aerial"},
		"sender": {"login": "arcane103"}
	}`
	resPRMerged, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged", []byte(payloadPRMerged))
	if err != nil {
		t.Fatalf("pull_request merged failed: %v", err)
	}
	if len(mockReg.mergedCalls) != 1 || mockReg.mergedCalls[0].prNumber != 515 || mockReg.mergedCalls[0].mergeSHA != "merge111" {
		t.Errorf("unexpected mergedCalls: %+v", mockReg.mergedCalls)
	}
	if resPRMerged.TargetID != "target-merged-1" {
		t.Errorf("expected TargetID %q, got %q", "target-merged-1", resPRMerged.TargetID)
	}

	// 2. PullRequest closed unmerged
	payloadPRClosed := `{
		"action": "closed",
		"number": 516,
		"pull_request": {
			"number": 516,
			"head": {"ref": "feat/discard", "sha": "head222"},
			"merged": false
		},
		"repository": {"full_name": "azylman/aerial"},
		"sender": {"login": "arcane103"}
	}`
	resPRClosed, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-closed", []byte(payloadPRClosed))
	if err != nil {
		t.Fatalf("pull_request closed unmerged failed: %v", err)
	}
	if len(mockReg.closedUnmergedCalls) != 1 || mockReg.closedUnmergedCalls[0].prNumber != 516 {
		t.Errorf("unexpected closedUnmergedCalls: %+v", mockReg.closedUnmergedCalls)
	}
	if resPRClosed.TargetID != "target-closed-1" {
		t.Errorf("expected TargetID %q, got %q", "target-closed-1", resPRClosed.TargetID)
	}

	// 3. PullRequest synchronize
	payloadPRSync := `{
		"action": "synchronize",
		"number": 517,
		"pull_request": {
			"number": 517,
			"head": {"ref": "feat/update", "sha": "newhead333"}
		},
		"repository": {"full_name": "azylman/aerial"},
		"sender": {"login": "arcane103"}
	}`
	resPRSync, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-sync", []byte(payloadPRSync))
	if err != nil {
		t.Fatalf("pull_request synchronize failed: %v", err)
	}
	if len(mockReg.syncCalls) != 1 || mockReg.syncCalls[0].prNumber != 517 || mockReg.syncCalls[0].headSHA != "newhead333" {
		t.Errorf("unexpected syncCalls: %+v", mockReg.syncCalls)
	}
	if resPRSync.TargetID != "target-sync-1" {
		t.Errorf("expected TargetID %q, got %q", "target-sync-1", resPRSync.TargetID)
	}

	// 4. CheckRun failure with PR in payload
	payloadCheckRunFail := `{
		"action": "completed",
		"check_run": {
			"name": "Unit Tests",
			"head_sha": "checkhead444",
			"status": "completed",
			"conclusion": "failure",
			"pull_requests": [{"number": 518, "head": {"ref": "feat/test", "sha": "checkhead444"}}]
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-fail", []byte(payloadCheckRunFail))
	if err != nil {
		t.Fatalf("check_run failure failed: %v", err)
	}
	if len(mockReg.ciFailedCalls) != 1 || mockReg.ciFailedCalls[0].prNumber != 518 {
		t.Errorf("unexpected ciFailedCalls: %+v", mockReg.ciFailedCalls)
	}

	// 5. CheckRun timed_out with PR in payload
	payloadCheckRunTimeout := `{
		"action": "completed",
		"check_run": {
			"name": "Integration Tests",
			"head_sha": "checkhead555",
			"status": "completed",
			"conclusion": "timed_out",
			"pull_requests": [{"number": 519, "head": {"ref": "feat/timeout", "sha": "checkhead555"}}]
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-timeout", []byte(payloadCheckRunTimeout))
	if err != nil {
		t.Fatalf("check_run timeout failed: %v", err)
	}
	if len(mockReg.ciFailedCalls) != 2 || mockReg.ciFailedCalls[1].prNumber != 519 {
		t.Errorf("unexpected ciFailedCalls after timeout: %+v", mockReg.ciFailedCalls)
	}

	// 6. CheckRun failure without PR but with headSHA (falls back to headSHA transition)
	payloadCheckRunFallback := `{
		"action": "completed",
		"check_run": {
			"name": "Lint",
			"head_sha": "orphansha666",
			"status": "completed",
			"conclusion": "failure",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-fallback", []byte(payloadCheckRunFallback))
	if err != nil {
		t.Fatalf("check_run failure fallback failed: %v", err)
	}
	if len(mockReg.ciFailedBySHACalls) != 1 || mockReg.ciFailedBySHACalls[0].headSHA != "orphansha666" {
		t.Errorf("unexpected ciFailedBySHACalls: %+v", mockReg.ciFailedBySHACalls)
	}

	// 7. WorkflowRun failure with PR
	payloadWFFail := `{
		"action": "completed",
		"workflow_run": {
			"name": "Continuous Integration",
			"head_sha": "wfhead777",
			"status": "completed",
			"conclusion": "failure",
			"pull_requests": [{"number": 521, "head": {"ref": "feat/wf", "sha": "wfhead777"}}]
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-fail", []byte(payloadWFFail))
	if err != nil {
		t.Fatalf("workflow_run failure failed: %v", err)
	}
	if len(mockReg.ciFailedCalls) != 3 || mockReg.ciFailedCalls[2].prNumber != 521 {
		t.Errorf("unexpected ciFailedCalls after workflow failure: %+v", mockReg.ciFailedCalls)
	}

	// 8. WorkflowRun timed_out without PR
	payloadWFTimeout := `{
		"action": "completed",
		"workflow_run": {
			"name": "Build Docker",
			"head_sha": "wfhead888",
			"status": "completed",
			"conclusion": "timed_out",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-timeout", []byte(payloadWFTimeout))
	if err != nil {
		t.Fatalf("workflow_run timeout failed: %v", err)
	}
	if len(mockReg.ciFailedBySHACalls) != 2 || mockReg.ciFailedBySHACalls[1].headSHA != "wfhead888" {
		t.Errorf("unexpected ciFailedBySHACalls after workflow timeout: %+v", mockReg.ciFailedBySHACalls)
	}

	// 9. Push to main with resolved PR
	// Mock GitHub API server for commit SHA resolution
	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{
			{Number: 522, State: "closed"},
		})
	}))
	defer ghMock.Close()

	srvWithGH := NewRouterServer(Config{GitHubAPIURL: ghMock.URL}, ghMock.Client())
	srvWithGH.SetRegistry(mockReg)

	payloadPush := `{
		"ref": "refs/heads/main",
		"after": "pushsha999",
		"repository": {"full_name": "azylman/aerial"},
		"head_commit": {"id": "pushsha999", "message": "feat: merge pr"}
	}`
	_, err = srvWithGH.ProcessGitHubEvent(ctx, "push", "del-push-main", []byte(payloadPush))
	if err != nil {
		t.Fatalf("push to main failed: %v", err)
	}
	if len(mockReg.backfillCalls) != 1 || mockReg.backfillCalls[0].prNumber != 522 || mockReg.backfillCalls[0].mergeSHA != "pushsha999" {
		t.Errorf("unexpected backfillCalls: %+v", mockReg.backfillCalls)
	}

	// 10. Registry error propagation: error bubbles up to River
	mockRegErr := &mockPRRegistry{err: errors.New("simulated database failure")}
	srvErr := NewRouterServer(Config{}, nil)
	srvErr.SetRegistry(mockRegErr)

	_, err = srvErr.ProcessGitHubEvent(ctx, "pull_request", "del-err", []byte(payloadPRMerged))
	if err == nil {
		t.Errorf("expected error when registry returns failure, got nil")
	}

	// 11. Local PR resolution priority: ResolvePRBySHA succeeds, avoiding GitHub API
	var ghHit bool
	ghSpy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ghHit = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{
			{Number: 999, State: "closed"},
		})
	}))
	defer ghSpy.Close()

	mockLocalReg := &mockPRRegistry{
		resolvePRNum:    530,
		resolveBranch:   "feat/local-resolved",
		resolveTargetID: "thread-local-530",
	}
	srvLocal := NewRouterServer(Config{GitHubAPIURL: ghSpy.URL}, ghSpy.Client())
	srvLocal.SetRegistry(mockLocalReg)

	// a. check_run without pull_requests resolves locally
	crPayload := `{
		"action": "completed",
		"check_run": {
			"name": "Integration Tests",
			"head_sha": "localsha123",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	resCR, err := srvLocal.ProcessGitHubEvent(ctx, "check_run", "del-cr-local", []byte(crPayload))
	if err != nil {
		t.Fatalf("local check_run resolution failed: %v", err)
	}
	if ghHit {
		t.Errorf("GitHub API was hit despite local resolution succeeding")
	}
	if resCR.ResolutionSource != "local_pr_registry" || resCR.PRNumber != 530 || resCR.Branch != "feat/local-resolved" || resCR.TargetID != "thread-local-530" {
		t.Errorf("unexpected resCR: %+v", resCR)
	}

	// b. workflow_run without pull_requests resolves locally
	ghHit = false
	wfPayload := `{
		"action": "completed",
		"workflow_run": {
			"name": "Build Pipeline",
			"head_sha": "localsha123",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	resWF, err := srvLocal.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-local", []byte(wfPayload))
	if err != nil {
		t.Fatalf("local workflow_run resolution failed: %v", err)
	}
	if ghHit {
		t.Errorf("GitHub API was hit despite local resolution succeeding")
	}
	if resWF.ResolutionSource != "local_pr_registry" || resWF.PRNumber != 530 || resWF.Branch != "feat/local-resolved" || resWF.TargetID != "thread-local-530" {
		t.Errorf("unexpected resWF: %+v", resWF)
	}

	// c. push resolves locally and backfills
	ghHit = false
	pushPayloadLocal := `{
		"ref": "refs/heads/main",
		"after": "localsha123",
		"repository": {"full_name": "azylman/aerial"},
		"head_commit": {"id": "localsha123", "message": "feat: merged commit"}
	}`
	resPush, err := srvLocal.ProcessGitHubEvent(ctx, "push", "del-push-local", []byte(pushPayloadLocal))
	if err != nil {
		t.Fatalf("local push resolution failed: %v", err)
	}
	if ghHit {
		t.Errorf("GitHub API was hit despite local resolution succeeding")
	}
	if resPush.ResolutionSource != "local_pr_registry" || resPush.PRNumber != 530 || resPush.MergeSHA != "localsha123" || resPush.TargetID != "thread-local-530" {
		t.Errorf("unexpected resPush: %+v", resPush)
	}
	if len(mockLocalReg.backfillCalls) != 1 || mockLocalReg.backfillCalls[0].prNumber != 530 || mockLocalReg.backfillCalls[0].mergeSHA != "localsha123" {
		t.Errorf("unexpected backfillCalls on local push: %+v", mockLocalReg.backfillCalls)
	}
}

func TestPostgresPRRegistry_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live PostgresPRRegistry integration test in short mode")
	}

	pgURL := getTestPostgresURL()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		t.Skipf("cannot parse postgres url: %v", err)
	}
	poolConfig.MaxConns = 3
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, ok := poolConfig.ConnConfig.RuntimeParams["search_path"]; !ok {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = "sidecars, public"
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Skipf("skipping live test, could not create pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping live test, postgres ping failed: %v", err)
	}

	repo := "azylman/aerial"
	testPRNum := 999991
	targetID := "live-test-target-xyz"

	// Cleanup any previous test row
	_, _ = pool.Exec(ctx, "DELETE FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum)
	}()

	// Insert initial test record
	insertSQL := `
		INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, status, title, created_at, updated_at)
		VALUES ($1, $2, 'feat/live-test', 'initialsha111', $3, 'open', 'Live Test PR', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
	`
	if _, err := pool.Exec(ctx, insertSQL, repo, testPRNum, targetID); err != nil {
		t.Fatalf("failed to insert initial test row: %v", err)
	}

	reg := NewPostgresPRRegistry(pool)

	// 1. Test UpdatePRSync: updates head_sha and sets status = 'open'
	if target, err := reg.UpdatePRSync(ctx, repo, testPRNum, "syncsha222"); err != nil || target != targetID {
		t.Fatalf("UpdatePRSync failed (target=%s): %v", target, err)
	}
	var currentHead, currentStatus string
	err = pool.QueryRow(ctx, "SELECT head_sha, status FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&currentHead, &currentStatus)
	if err != nil {
		t.Fatalf("query row failed: %v", err)
	}
	if currentHead != "syncsha222" || currentStatus != "open" {
		t.Errorf("after UpdatePRSync: got head=%s, status=%s; expected syncsha222, open", currentHead, currentStatus)
	}

	// 2. Test AtomicTransitionCIFailed: transitions 'open' -> 'ci_failed'
	gotTarget, updated, err := reg.AtomicTransitionCIFailed(ctx, repo, testPRNum)
	if err != nil {
		t.Fatalf("AtomicTransitionCIFailed failed: %v", err)
	}
	if !updated || gotTarget != targetID {
		t.Errorf("AtomicTransitionCIFailed: got updated=%v, target=%s; expected true, %s", updated, gotTarget, targetID)
	}
	err = pool.QueryRow(ctx, "SELECT status FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&currentStatus)
	if err != nil || currentStatus != "ci_failed" {
		t.Errorf("status should be ci_failed, got %s (err: %v)", currentStatus, err)
	}

	// 3. Test CAS gate: second AtomicTransitionCIFailed should be no-op (already ci_failed)
	_, updated, err = reg.AtomicTransitionCIFailed(ctx, repo, testPRNum)
	if err != nil {
		t.Fatalf("second AtomicTransitionCIFailed failed: %v", err)
	}
	if updated {
		t.Errorf("second AtomicTransitionCIFailed should have returned updated=false (CAS gate)")
	}

	// 4. Test UpdatePRSync resets 'ci_failed' back to 'open'
	if target, err := reg.UpdatePRSync(ctx, repo, testPRNum, "newhead333"); err != nil || target != targetID {
		t.Fatalf("UpdatePRSync reset failed (target=%s): %v", target, err)
	}
	err = pool.QueryRow(ctx, "SELECT head_sha, status FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&currentHead, &currentStatus)
	if err != nil || currentHead != "newhead333" || currentStatus != "open" {
		t.Errorf("after second UpdatePRSync: got head=%s, status=%s; expected newhead333, open", currentHead, currentStatus)
	}

	// 5. Test AtomicTransitionCIFailedByHeadSHA
	gotTarget, gotPR, updated, err := reg.AtomicTransitionCIFailedByHeadSHA(ctx, repo, "newhead333")
	if err != nil {
		t.Fatalf("AtomicTransitionCIFailedByHeadSHA failed: %v", err)
	}
	if !updated || gotPR != testPRNum || gotTarget != targetID {
		t.Errorf("AtomicTransitionCIFailedByHeadSHA: got updated=%v, pr=%d, target=%s; expected true, %d, %s", updated, gotPR, gotTarget, testPRNum, targetID)
	}

	// 6. Test UpdatePRMerged: transitions to 'merged'
	if target, err := reg.UpdatePRMerged(ctx, repo, testPRNum, "mergesha444"); err != nil || target != targetID {
		t.Fatalf("UpdatePRMerged failed (target=%s): %v", target, err)
	}
	var currentMerge string
	err = pool.QueryRow(ctx, "SELECT merge_sha, status FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&currentMerge, &currentStatus)
	if err != nil || currentMerge != "mergesha444" || currentStatus != "merged" {
		t.Errorf("after UpdatePRMerged: got merge=%s, status=%s; expected mergesha444, merged", currentMerge, currentStatus)
	}

	// 7. Merged PR cannot transition to ci_failed
	_, updated, err = reg.AtomicTransitionCIFailed(ctx, repo, testPRNum)
	if err != nil {
		t.Fatalf("AtomicTransitionCIFailed on merged failed: %v", err)
	}
	if updated {
		t.Errorf("AtomicTransitionCIFailed on merged PR must return updated=false")
	}

	// 8. Test BackfillPushMergeSHA
	if target, err := reg.BackfillPushMergeSHA(ctx, repo, testPRNum, "mergesha555"); err != nil || target != targetID {
		t.Fatalf("BackfillPushMergeSHA failed (target=%s): %v", target, err)
	}
	err = pool.QueryRow(ctx, "SELECT merge_sha FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&currentMerge)
	if err != nil || currentMerge != "mergesha555" {
		t.Errorf("after BackfillPushMergeSHA: got merge=%s; expected mergesha555", currentMerge)
	}

	// 9. Untracked PR succeeds silently
	if target, err := reg.UpdatePRMerged(ctx, repo, 999999, "sha"); err != nil || target != "" {
		t.Errorf("expected nil error and empty target on untracked PR, got %v, %q", err, target)
	}
	if target, err := reg.UpdatePRClosedUnmerged(ctx, repo, 999999); err != nil || target != "" {
		t.Errorf("expected nil error and empty target on untracked PR closed, got %v, %q", err, target)
	}
	if target, err := reg.UpdatePRSync(ctx, repo, 999999, "sha"); err != nil || target != "" {
		t.Errorf("expected nil error and empty target on untracked PR sync, got %v, %q", err, target)
	}
	_, updated, err = reg.AtomicTransitionCIFailed(ctx, repo, 999999)
	if err != nil || updated {
		t.Errorf("untracked AtomicTransitionCIFailed: got updated=%v, err=%v; expected false, nil", updated, err)
	}
	_, _, updated, err = reg.AtomicTransitionCIFailedByHeadSHA(ctx, repo, "nonexistent-sha")
	if err != nil || updated {
		t.Errorf("untracked AtomicTransitionCIFailedByHeadSHA: got updated=%v, err=%v; expected false, nil", updated, err)
	}

	// 10. Empty / Zero SHA guard
	_, _, updated, err = reg.AtomicTransitionCIFailedByHeadSHA(ctx, repo, "")
	if err != nil || updated {
		t.Errorf("empty sha should return false, nil; got %v, %v", updated, err)
	}
	_, _, updated, err = reg.AtomicTransitionCIFailedByHeadSHA(ctx, repo, "0000000000000000000000000000000000000000")
	if err != nil || updated {
		t.Errorf("zero sha should return false, nil; got %v, %v", updated, err)
	}

	// 11. Test ResolvePRBySHA
	// a. Resolve by head_sha
	resPR, resBranch, resTarget, err := reg.ResolvePRBySHA(ctx, repo, "newhead333")
	if err != nil {
		t.Fatalf("ResolvePRBySHA by head_sha failed: %v", err)
	}
	if resPR != testPRNum || resBranch != "feat/live-test" || resTarget != targetID {
		t.Errorf("ResolvePRBySHA by head_sha: got (%d, %s, %s); expected (%d, feat/live-test, %s)", resPR, resBranch, resTarget, testPRNum, targetID)
	}

	// b. Resolve by merge_sha
	resPR, resBranch, resTarget, err = reg.ResolvePRBySHA(ctx, repo, "mergesha555")
	if err != nil {
		t.Fatalf("ResolvePRBySHA by merge_sha failed: %v", err)
	}
	if resPR != testPRNum || resBranch != "feat/live-test" || resTarget != targetID {
		t.Errorf("ResolvePRBySHA by merge_sha: got (%d, %s, %s); expected (%d, feat/live-test, %s)", resPR, resBranch, resTarget, testPRNum, targetID)
	}

	// c. Untracked SHA returns 0, "", "", nil
	resPR, resBranch, resTarget, err = reg.ResolvePRBySHA(ctx, repo, "untracked-sha-999")
	if err != nil {
		t.Fatalf("ResolvePRBySHA on untracked SHA failed: %v", err)
	}
	if resPR != 0 || resBranch != "" || resTarget != "" {
		t.Errorf("ResolvePRBySHA on untracked SHA: got (%d, %s, %s); expected (0, \"\", \"\")", resPR, resBranch, resTarget)
	}

	// d. Empty / Zero SHA returns 0, "", "", nil
	resPR, _, _, err = reg.ResolvePRBySHA(ctx, repo, "")
	if err != nil || resPR != 0 {
		t.Errorf("ResolvePRBySHA empty sha: got (%d, %v); expected (0, nil)", resPR, err)
	}
	resPR, _, _, err = reg.ResolvePRBySHA(ctx, repo, "0000000000000000000000000000000000000000")
	if err != nil || resPR != 0 {
		t.Errorf("ResolvePRBySHA zero sha: got (%d, %v); expected (0, nil)", resPR, err)
	}
}

func TestHandleHangarWebhook_EndpointsAndValidation(t *testing.T) {
	srv := NewRouterServer(Config{}, nil)
	routes := srv.Routes()

	// 1. Malformed JSON -> 400 Bad Request
	for _, path := range []string{"/api/webhooks/hangar", "/webhooks/hangar"} {
		reqMalformed := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{invalid json`))
		recMalformed := httptest.NewRecorder()
		routes.ServeHTTP(recMalformed, reqMalformed)
		if recMalformed.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for malformed json on %s, got %d", path, recMalformed.Code)
		}
		var errResp map[string]string
		if err := json.Unmarshal(recMalformed.Body.Bytes(), &errResp); err != nil || errResp["error"] != "invalid json payload" {
			t.Errorf("unexpected error response on %s: %+v", path, errResp)
		}
	}

	// 2. Read error in body -> 400 Bad Request
	reqErrBody := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", errReader{})
	recErrBody := httptest.NewRecorder()
	routes.ServeHTTP(recErrBody, reqErrBody)
	if recErrBody.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on read error, got %d", recErrBody.Code)
	}

	// 3. Missing event -> 400 Bad Request
	reqMissingEvent := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(`{"job_name":"brain"}`))
	recMissingEvent := httptest.NewRecorder()
	routes.ServeHTTP(recMissingEvent, reqMissingEvent)
	if recMissingEvent.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing event, got %d", recMissingEvent.Code)
	}

	// 4. Missing job_name -> 400 Bad Request
	reqMissingJob := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(`{"event":"deploy_started"}`))
	recMissingJob := httptest.NewRecorder()
	routes.ServeHTTP(recMissingJob, reqMissingJob)
	if recMissingJob.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing job_name, got %d", recMissingJob.Code)
	}

	// 5. Missing both -> 400 Bad Request
	reqMissingBoth := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(`{}`))
	recMissingBoth := httptest.NewRecorder()
	routes.ServeHTTP(recMissingBoth, reqMissingBoth)
	if recMissingBoth.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing both required fields, got %d", recMissingBoth.Code)
	}
}

func TestHandleHangarWebhook_ValidPayloads(t *testing.T) {
	events := []struct {
		event   string
		status  string
		details string
	}{
		{"deploy_started", "started", "Nomad job evaluation placed"},
		{"deploy_success", "success", "Healthy deployment completed"},
		{"deploy_failed", "failed", "Allocation failed healthcheck"},
		{"deploy_rollback", "rollback", "Reverted to previous healthy release"},
	}

	for _, tc := range events {
		t.Run(tc.event, func(t *testing.T) {
			srv := NewRouterServer(Config{}, nil)
			routes := srv.Routes()

			for _, path := range []string{"/api/webhooks/hangar", "/webhooks/hangar"} {
				body := fmt.Sprintf(`{
					"event": %q,
					"job_name": "brain",
					"repo": "azylman/aerial",
					"pr_number": 420,
					"target_id": "thread-999",
					"image": "ghcr.io/azylman/aerial:latest",
					"digest": "sha256:abc1234",
					"status": %q,
					"deployment_id": "dep-uuid-1",
					"details": %q,
					"timestamp": "2026-10-02T20:00:00Z"
				}`, tc.event, tc.status, tc.details)

				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				rec := httptest.NewRecorder()
				routes.ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					t.Fatalf("expected 200 OK for %s on %s, got %d: %s", tc.event, path, rec.Code, rec.Body.String())
				}

				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse response json: %v", err)
				}
				if resp["status"] != "accepted" {
					t.Errorf("expected status accepted, got %v", resp["status"])
				}
				if resp["event"] != tc.event {
					t.Errorf("expected event %s, got %v", tc.event, resp["event"])
				}
				if resp["job_name"] != "brain" {
					t.Errorf("expected job_name brain, got %v", resp["job_name"])
				}
				if int(resp["pr_number"].(float64)) != 420 {
					t.Errorf("expected pr_number 420, got %v", resp["pr_number"])
				}
				if resp["target_id"] != "thread-999" {
					t.Errorf("expected target_id thread-999, got %v", resp["target_id"])
				}
			}
		})
	}
}

func TestHandleHangarWebhook_ResolvePRBySHA(t *testing.T) {
	mockReg := &mockPRRegistry{
		resolvePRNum:    789,
		resolveTargetID: "thread-resolved-789",
	}

	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	routes := srv.Routes()

	// 1. CommitSHA provided without PRNumber: resolves PR and TargetID
	body := `{
		"event": "deploy_success",
		"job_name": "webhooks-router",
		"repo": "azylman/aerial",
		"commit_sha": "deploycommit123",
		"status": "success",
		"details": "Deployed successfully"
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body))
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(mockReg.resolveBySHACalls) != 1 {
		t.Fatalf("expected 1 call to ResolvePRBySHA, got %d", len(mockReg.resolveBySHACalls))
	}
	if mockReg.resolveBySHACalls[0].sha != "deploycommit123" || mockReg.resolveBySHACalls[0].repo != "azylman/aerial" {
		t.Errorf("unexpected resolve call: %+v", mockReg.resolveBySHACalls[0])
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if int(resp["pr_number"].(float64)) != 789 {
		t.Errorf("expected resolved pr_number 789, got %v", resp["pr_number"])
	}
	if resp["target_id"] != "thread-resolved-789" {
		t.Errorf("expected resolved target_id thread-resolved-789, got %v", resp["target_id"])
	}

	// 2. CommitSHA provided with existing TargetID: preserves existing TargetID
	mockReg.resolveBySHACalls = nil
	bodyPreserveTarget := `{
		"event": "deploy_rollback",
		"job_name": "webhooks-router",
		"repo": "azylman/aerial",
		"commit_sha": "deploycommit123",
		"target_id": "existing-target-snowflake",
		"status": "rollback"
	}`
	reqPreserve := httptest.NewRequest(http.MethodPost, "/webhooks/hangar", strings.NewReader(bodyPreserveTarget))
	recPreserve := httptest.NewRecorder()
	routes.ServeHTTP(recPreserve, reqPreserve)

	if recPreserve.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", recPreserve.Code, recPreserve.Body.String())
	}
	var respPreserve map[string]interface{}
	_ = json.Unmarshal(recPreserve.Body.Bytes(), &respPreserve)
	if int(respPreserve["pr_number"].(float64)) != 789 {
		t.Errorf("expected pr_number 789, got %v", respPreserve["pr_number"])
	}
	if respPreserve["target_id"] != "existing-target-snowflake" {
		t.Errorf("expected existing target_id preserved, got %v", respPreserve["target_id"])
	}

	// 3. PRNumber already provided: does not call ResolvePRBySHA
	mockReg.resolveBySHACalls = nil
	bodyWithPR := `{
		"event": "deploy_started",
		"job_name": "brain",
		"repo": "azylman/aerial",
		"commit_sha": "deploycommit123",
		"pr_number": 123,
		"status": "started"
	}`
	reqWithPR := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(bodyWithPR))
	recWithPR := httptest.NewRecorder()
	routes.ServeHTTP(recWithPR, reqWithPR)

	if recWithPR.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", recWithPR.Code, recWithPR.Body.String())
	}
	if len(mockReg.resolveBySHACalls) != 0 {
		t.Errorf("expected 0 calls to ResolvePRBySHA when PRNumber already given, got %d", len(mockReg.resolveBySHACalls))
	}

	// 4. Registry error: logs warning and still accepts event
	mockRegErr := &mockPRRegistry{resolveErr: errors.New("db error")}
	srvErr := NewRouterServer(Config{}, nil)
	srvErr.SetRegistry(mockRegErr)

	reqErr := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body))
	recErr := httptest.NewRecorder()
	srvErr.Routes().ServeHTTP(recErr, reqErr)

	if recErr.Code != http.StatusOK {
		t.Errorf("expected 200 OK even when registry resolve errors, got %d", recErr.Code)
	}

	// 5. Registry nil: handles gracefully without error
	srvNilReg := NewRouterServer(Config{}, nil)
	reqNil := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body))
	recNil := httptest.NewRecorder()
	srvNilReg.Routes().ServeHTTP(recNil, reqNil)

	if recNil.Code != http.StatusOK {
		t.Errorf("expected 200 OK when registry is nil, got %d", recNil.Code)
	}
}

func TestHandleHangarWebhook_WithRiver(t *testing.T) {
	mockRiver := &mockRiverInserter{}
	srv := NewRouterServer(Config{}, nil, mockRiver)
	routes := srv.Routes()

	body := `{
		"event": "deploy_success",
		"job_name": "brain",
		"repo": "azylman/aerial",
		"pr_number": 558,
		"target_id": "1555405874565091380",
		"status": "success",
		"details": "Deployment healthy"
	}`

	// 1. Success -> 202 Accepted
	for _, path := range []string{"/api/webhooks/hangar", "/webhooks/hangar"} {
		mockRiver.insertedHangarJobs = nil
		mockRiver.insertedOpts = nil

		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted on %s, got %d: %s", path, rec.Code, rec.Body.String())
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp["status"] != "accepted" || resp["event"] != "deploy_success" || resp["job_name"] != "brain" {
			t.Errorf("unexpected response body: %+v", resp)
		}

		if len(mockRiver.insertedHangarJobs) != 1 {
			t.Fatalf("expected 1 hangar job inserted into river, got %d", len(mockRiver.insertedHangarJobs))
		}
		if mockRiver.insertedHangarJobs[0].Event.Event != "deploy_success" || mockRiver.insertedHangarJobs[0].Event.JobName != "brain" {
			t.Errorf("unexpected inserted job: %+v", mockRiver.insertedHangarJobs[0])
		}
		if len(mockRiver.insertedOpts) != 1 || mockRiver.insertedOpts[0] == nil || mockRiver.insertedOpts[0].MaxAttempts != 5 {
			t.Errorf("expected MaxAttempts: 5, got %+v", mockRiver.insertedOpts)
		}
	}

	// 2. River insertion failure -> 500 Internal Server Error
	mockRiverErr := &mockRiverInserter{err: errors.New("simulated river insert error")}
	srvErr := NewRouterServer(Config{}, nil, mockRiverErr)
	reqErr := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body))
	recErr := httptest.NewRecorder()
	srvErr.Routes().ServeHTTP(recErr, reqErr)

	if recErr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error when river persistence fails, got %d", recErr.Code)
	}
}

func TestProcessHangarEvent_Direct(t *testing.T) {
	mockReg := &mockPRRegistry{
		resolvePRNum:    99,
		resolveTargetID: "target-99",
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)

	ctx := context.Background()

	// 1. deploy_started
	evtStarted := HangarDeployEvent{
		Event:     "deploy_started",
		JobName:   "webhooks-router",
		Repo:      "azylman/aerial",
		CommitSHA: "sha111",
		Status:    "started",
	}
	resStarted, err := srv.ProcessHangarEvent(ctx, evtStarted)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resStarted.PRNumber != 99 || resStarted.TargetID != "target-99" {
		t.Errorf("unexpected resStarted: %+v", resStarted)
	}
	if len(mockReg.deployingCalls) != 1 || mockReg.deployingCalls[0].prNumber != 99 || len(mockReg.deployingCalls[0].jobs) != 1 || mockReg.deployingCalls[0].jobs[0] != "webhooks-router" {
		t.Errorf("unexpected deployingCalls: %+v", mockReg.deployingCalls)
	}
	mockReg.deployingErr = errors.New("db error")
	_, err = srv.ProcessHangarEvent(ctx, evtStarted)
	if err != nil {
		t.Errorf("ProcessHangarEvent should not fail when TransitionDeploying errors: %v", err)
	}
	mockReg.deployingErr = nil

	// 2. deploy_success with pr_number > 0 logs registry success
	evtSuccess := HangarDeployEvent{
		Event:     "deploy_success",
		JobName:   "webhooks-router",
		Repo:      "azylman/aerial",
		PRNumber:  99,
		Status:    "success",
		TargetID:  "target-99",
		Details:   "deployment completed",
	}
	resSuccess, err := srv.ProcessHangarEvent(ctx, evtSuccess)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resSuccess.Event != "deploy_success" {
		t.Errorf("unexpected event: %s", resSuccess.Event)
	}

	// 3. deploy_rollback with pr_number > 0 logs registry rollback
	evtRollback := HangarDeployEvent{
		Event:     "deploy_rollback",
		JobName:   "webhooks-router",
		Repo:      "azylman/aerial",
		PRNumber:  99,
		Status:    "rollback",
		Details:   "rolled back",
	}
	resRollback, err := srv.ProcessHangarEvent(ctx, evtRollback)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resRollback.Event != "deploy_rollback" {
		t.Errorf("unexpected event: %s", resRollback.Event)
	}

	// 4. deploy_failed
	evtFailed := HangarDeployEvent{
		Event:    "deploy_failed",
		JobName:  "webhooks-router",
		Repo:     "azylman/aerial",
		PRNumber: 99,
		Status:   "failed",
		Details:  "deploy unhealthy",
	}
	resFailed, err := srv.ProcessHangarEvent(ctx, evtFailed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resFailed.Status != "failed" {
		t.Errorf("unexpected status: %s", resFailed.Status)
	}
}

type zeroCallTransport struct {
	calls atomic.Int32
}

func (z *zeroCallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	z.calls.Add(1)
	return nil, errors.New("unexpected outbound HTTP call")
}

func TestHangarWebhook_ZeroDownstreamCallsToBrain(t *testing.T) {
	// STRICT INVARIANT: ZERO downstream dispatch to Brain or Discord!
	transport := &zeroCallTransport{}
	client := &http.Client{Transport: transport}

	srv := NewRouterServer(Config{}, client)
	mockReg := &mockPRRegistry{resolvePRNum: 101, resolveTargetID: "target-101"}
	srv.SetRegistry(mockReg)

	routes := srv.Routes()

	events := []string{"deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"}
	for _, evt := range events {
		body := fmt.Sprintf(`{
			"event": %q,
			"job_name": "brain",
			"repo": "azylman/aerial",
			"commit_sha": "sha-test",
			"status": "started"
		}`, evt)

		req := httptest.NewRequest(http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body))
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for %s, got %d", evt, rec.Code)
		}
	}

	if calls := transport.calls.Load(); calls != 0 {
		t.Fatalf("STRICT INVARIANT VIOLATION: expected 0 outbound calls to Brain or Discord, but %d were made", calls)
	}
}


type mockOutboundDispatcher struct {
	mu                 sync.Mutex
	gitPushCalls       []GitPushEventRequest
	gitPushCh          chan GitPushEventRequest
	imageReadyCalls    []ImageReadyEventRequest
	imageReadyCh       chan ImageReadyEventRequest
	promptCalls        []PromptRequest
	promptCh           chan PromptRequest
	directMessageCalls []DirectMessageRequest
	directMessageCh    chan DirectMessageRequest
	gitPushErr         error
	imageReadyErr      error
	promptErr          error
	directMessageErr   error
	matchedJobs        map[string][]string // image -> matched jobs
}

func (m *mockOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	m.mu.Lock()
	m.gitPushCalls = append(m.gitPushCalls, req)
	ch := m.gitPushCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return m.gitPushErr
}

func (m *mockOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	m.mu.Lock()
	m.imageReadyCalls = append(m.imageReadyCalls, req)
	ch := m.imageReadyCh
	var jobs []string
	if m.matchedJobs != nil {
		jobs = m.matchedJobs[req.Image]
	}
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return jobs, m.imageReadyErr
}

func (m *mockOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	m.mu.Lock()
	m.promptCalls = append(m.promptCalls, req)
	ch := m.promptCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return m.promptErr
}

func (m *mockOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	m.mu.Lock()
	m.directMessageCalls = append(m.directMessageCalls, req)
	ch := m.directMessageCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return m.directMessageErr
}

func (m *mockOutboundDispatcher) GitPushCalls() []GitPushEventRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]GitPushEventRequest, len(m.gitPushCalls))
	copy(copied, m.gitPushCalls)
	return copied
}

func (m *mockOutboundDispatcher) ImageReadyCalls() []ImageReadyEventRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]ImageReadyEventRequest, len(m.imageReadyCalls))
	copy(copied, m.imageReadyCalls)
	return copied
}

func (m *mockOutboundDispatcher) PromptCalls() []PromptRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]PromptRequest, len(m.promptCalls))
	copy(copied, m.promptCalls)
	return copied
}

func (m *mockOutboundDispatcher) DirectMessageCalls() []DirectMessageRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]DirectMessageRequest, len(m.directMessageCalls))
	copy(copied, m.directMessageCalls)
	return copied
}

func TestDefaultOutboundDispatcher_AllMethods(t *testing.T) {
	// 1. Test GitPush dispatch
	var receivedGitPush GitPushEventRequest
	gitPushServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/git_push" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedGitPush); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer gitPushServer.Close()

	// 2. Test ImageReady dispatch
	var receivedImageReady ImageReadyEventRequest
	imageReadyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events/image_ready" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedImageReady); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer imageReadyServer.Close()

	// 3. Test Prompt and DirectMessage dispatch with retry
	var promptAttempts int
	var receivedPrompt PromptRequest
	var directAttempts int
	var receivedDirect DirectMessageRequest
	brainServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/prompt" {
			promptAttempts++
			if promptAttempts == 1 {
				// First attempt fails with 500 to test retry
				http.Error(w, "transient error", http.StatusInternalServerError)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&receivedPrompt); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"queued"}`))
			return
		}

		if r.URL.Path == "/discord/message" {
			directAttempts++
			if directAttempts == 1 {
				// First attempt fails with 500 to test retry
				http.Error(w, "transient error", http.StatusInternalServerError)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&receivedDirect); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"sent"}`))
			return
		}

		http.NotFound(w, r)
	}))
	defer brainServer.Close()

	dispatcher := NewDefaultOutboundDispatcher(gitPushServer.URL, brainServer.URL, nil)
	dispatcher.retryDelay = 10 * time.Millisecond // fast retry for unit test

	ctx := context.Background()

	// Test DispatchGitPush
	err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{
		Repo:   "azylman/aerial",
		Ref:    "refs/heads/main",
		Commit: "testcommit123",
	})
	if err != nil {
		t.Fatalf("DispatchGitPush failed: %v", err)
	}
	if receivedGitPush.Repo != "azylman/aerial" || receivedGitPush.Commit != "testcommit123" {
		t.Errorf("unexpected git push payload: %+v", receivedGitPush)
	}

	// Test DispatchImageReady
	dispatcher.hangarURL = imageReadyServer.URL
	_, err = dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{
		Image:  "ghcr.io/azylman/aerial-webhooks-router:latest",
		Digest: "sha256:abcd",
	})
	if err != nil {
		t.Fatalf("DispatchImageReady failed: %v", err)
	}
	if receivedImageReady.Image != "ghcr.io/azylman/aerial-webhooks-router:latest" || receivedImageReady.Digest != "sha256:abcd" {
		t.Errorf("unexpected image ready payload: %+v", receivedImageReady)
	}

	// Test DispatchPrompt invalid snowflake
	err = dispatcher.DispatchPrompt(ctx, PromptRequest{
		ChannelID: "not-a-snowflake",
		Prompt:    "hello",
	})
	if err == nil {
		t.Fatalf("expected error on invalid snowflake, got nil")
	}

	// Test DispatchPrompt valid snowflake with retry
	validSnowflake := "1555405874565091380"
	err = dispatcher.DispatchPrompt(ctx, PromptRequest{
		ChannelID: validSnowflake,
		Prompt:    "test prompt message",
	})
	if err != nil {
		t.Fatalf("DispatchPrompt failed: %v", err)
	}
	if promptAttempts != 2 {
		t.Errorf("expected 2 attempts for prompt dispatch, got %d", promptAttempts)
	}
	if receivedPrompt.ChannelID != validSnowflake || receivedPrompt.Prompt != "test prompt message" {
		t.Errorf("unexpected prompt payload: %+v", receivedPrompt)
	}

	// Test DispatchDirectMessage invalid snowflake
	err = dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
		ChannelID: "not-a-snowflake",
		Content:   "deployment completed",
	})
	if err == nil {
		t.Fatalf("expected error on invalid snowflake for direct message, got nil")
	}

	// Test DispatchDirectMessage valid snowflake with retry
	err = dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
		ChannelID: validSnowflake,
		Content:   "deployment completed",
	})
	if err != nil {
		t.Fatalf("DispatchDirectMessage failed: %v", err)
	}
	if directAttempts != 2 {
		t.Errorf("expected 2 attempts for direct message dispatch, got %d", directAttempts)
	}
	if receivedDirect.ChannelID != validSnowflake || receivedDirect.Content != "deployment completed" {
		t.Errorf("unexpected direct message payload: %+v", receivedDirect)
	}

	// Test DispatchDirectMessage client error 400 does not retry
	clientErrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer clientErrServer.Close()
	clientErrDisp := NewDefaultOutboundDispatcher("", clientErrServer.URL, nil)
	clientErrDisp.retryDelay = 5 * time.Millisecond
	if err := clientErrDisp.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: validSnowflake, Content: "hi"}); err == nil {
		t.Errorf("expected error on 400 bad request, got nil")
	}
}

func TestProcessGitHubEvent_OutboundDispatch(t *testing.T) {
	mockDisp := &mockOutboundDispatcher{}
	mockReg := &mockPRRegistry{
		ciFailedUpdated: true,
	}

	srv := NewRouterServer(Config{}, nil)
	srv.SetDispatcher(mockDisp)
	srv.SetRegistry(mockReg)

	ctx := context.Background()

	// 1. Push to main triggers DispatchGitPush
	pushPayloadMain := []byte(`{
		"ref": "refs/heads/main",
		"after": "maincommit123",
		"repository": {"full_name": "azylman/aerial"}
	}`)
	_, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-main", pushPayloadMain)
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // wait for goroutine
	calls := mockDisp.GitPushCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 git push dispatch, got %d", len(calls))
	}
	if calls[0].Repo != "azylman/aerial" || calls[0].Commit != "maincommit123" || calls[0].Ref != "refs/heads/main" {
		t.Errorf("unexpected git push call: %+v", calls[0])
	}

	// 2. Push to feature branch does NOT trigger DispatchGitPush
	pushPayloadFeat := []byte(`{
		"ref": "refs/heads/feat/test-branch",
		"after": "featcommit456",
		"repository": {"full_name": "azylman/aerial"}
	}`)
	_, err = srv.ProcessGitHubEvent(ctx, "push", "del-push-feat", pushPayloadFeat)
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push feat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if len(mockDisp.GitPushCalls()) != 1 {
		t.Fatalf("expected still 1 git push dispatch, got %d", len(mockDisp.GitPushCalls()))
	}

	// 3. Check_run failure with valid snowflake triggers DispatchPrompt
	mockReg.resolvePRNum = 530
	mockReg.resolveTargetID = "1555405874565091380"
	mockReg.ciFailedTargetID = "1555405874565091380"
	mockReg.ciFailedUpdated = true

	crFailurePayload := []byte(`{
		"action": "completed",
		"repository": {"full_name": "azylman/aerial"},
		"check_run": {
			"name": "Service Unit Tests",
			"head_sha": "headsha789",
			"status": "completed",
			"conclusion": "failure",
			"html_url": "https://github.com/azylman/aerial/actions/runs/123",
			"output": {
				"title": "Build Failed",
				"summary": "Compilation error in brain/main.go:42"
			},
			"pull_requests": [{"number": 530, "head": {"ref": "feat/test"}}]
		}
	}`)
	_, err = srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-fail", crFailurePayload)
	if err != nil {
		t.Fatalf("ProcessGitHubEvent check_run failure failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt dispatch, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" || !strings.Contains(pCalls[0].Prompt, "Service Unit Tests") {
		t.Errorf("unexpected prompt call: %+v", pCalls[0])
	}
	if !strings.Contains(pCalls[0].Prompt, "Compilation error in brain/main.go:42") {
		t.Errorf("expected error details in prompt: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "https://github.com/azylman/aerial/actions/runs/123") {
		t.Errorf("expected check URL in prompt: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Please investigate and fix") {
		t.Errorf("expected investigate and fix directive in prompt: %s", pCalls[0].Prompt)
	}

	// 4. Duplicate check_run failure (ciFailedUpdated = false) does NOT trigger DispatchPrompt
	mockReg.ciFailedUpdated = false
	_, err = srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-dup", crFailurePayload)
	if err != nil {
		t.Fatalf("ProcessGitHubEvent duplicate check_run failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected still 1 prompt dispatch for duplicate, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestProcessHangarEvent_OutboundDispatch(t *testing.T) {
	mockDisp := &mockOutboundDispatcher{}
	mockReg := &mockPRRegistry{
		deployFailedUpdated: true,
		channelContextMap: map[string][]ChannelMessageContext{
			"1555405874565091380": {
				{AuthorName: "arcane103", Content: "Deploy the new router", ResponseText: "Working on it!"},
			},
		},
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetDispatcher(mockDisp)
	srv.SetRegistry(mockReg)

	ctx := context.Background()

	// 0. deploy_started with valid snowflake targetID triggers direct message (NOT prompt!)
	startedEvt := HangarDeployEvent{
		Event:     "deploy_started",
		JobName:   "brain",
		Repo:      "azylman/aerial",
		PRNumber:  527,
		CommitSHA: "sha1234",
		TargetID:  "1555405874565091380",
		Status:    "started",
	}
	srv.ProcessHangarEvent(ctx, startedEvt)

	time.Sleep(50 * time.Millisecond)
	dmCalls := mockDisp.DirectMessageCalls()
	if len(dmCalls) != 1 {
		t.Fatalf("expected 1 direct message dispatch after deploy_started, got %d", len(dmCalls))
	}
	if dmCalls[0].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[0].Content, "🚀 **(PR: #527, repo: aerial)** Starting deploy for brain") {
		t.Errorf("unexpected deploy_started direct message: %+v", dmCalls[0])
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls on deploy_started, got %d", len(mockDisp.PromptCalls()))
	}

	// 1. deploy_success with valid snowflake targetID triggers prompt with channel context and directive (NOT direct message!)
	successEvt := HangarDeployEvent{
		Event:     "deploy_success",
		JobName:   "brain",
		Repo:      "azylman/aerial",
		PRNumber:  528,
		CommitSHA: "sha3d2a",
		TargetID:  "1555405874565091380",
		Status:    "success",
	}
	srv.ProcessHangarEvent(ctx, successEvt)

	time.Sleep(50 * time.Millisecond)
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt dispatch on deploy_success, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" || !strings.Contains(pCalls[0].Prompt, "Continuous Delivery deployment completed for job brain") {
		t.Errorf("unexpected deploy_success prompt: %+v", pCalls[0])
	}
	if !strings.Contains(pCalls[0].Prompt, "[arcane103]: Deploy the new router") || !strings.Contains(pCalls[0].Prompt, "[Aerial]: Working on it!") {
		t.Errorf("expected channel context in prompt: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Directive: Based on the recent conversation context above") {
		t.Errorf("expected directive in prompt: %s", pCalls[0].Prompt)
	}
	if len(mockDisp.DirectMessageCalls()) != 1 {
		t.Fatalf("expected 1 direct message call on deploy_success, got %d", len(mockDisp.DirectMessageCalls()))
	}

	// 2. deploy_rollback with valid snowflake targetID triggers direct message first, then prompt
	longDetails := strings.Repeat("error detail line\n", 50)
	rollbackEvt := HangarDeployEvent{
		Event:     "deploy_rollback",
		JobName:   "webhooks-router",
		Repo:      "azylman/aerial",
		PRNumber:  529,
		CommitSHA: "shafail",
		TargetID:  "1555405874565091380",
		Status:    "rollback",
		Details:   longDetails,
	}
	srv.ProcessHangarEvent(ctx, rollbackEvt)

	time.Sleep(50 * time.Millisecond)
	pCalls = mockDisp.PromptCalls()
	if len(pCalls) != 2 {
		t.Fatalf("expected 2 prompt dispatches after rollback, got %d", len(pCalls))
	}
	if pCalls[1].ChannelID != "1555405874565091380" || !strings.Contains(pCalls[1].Prompt, "Deployment failed and rolled back") {
		t.Errorf("unexpected deploy_rollback prompt: %+v", pCalls[1])
	}
	if !strings.Contains(pCalls[1].Prompt, "Please investigate and fix") {
		t.Errorf("expected investigate and fix directive in prompt: %s", pCalls[1].Prompt)
	}
	if !strings.Contains(pCalls[1].Prompt, "```") {
		t.Errorf("expected codeblock in prompt: %s", pCalls[1].Prompt)
	}
	// direct message count should now be 2 (deploy_started + deploy_rollback)
	dmCalls = mockDisp.DirectMessageCalls()
	if len(dmCalls) != 2 {
		t.Fatalf("expected 2 direct message dispatches after rollback, got %d", len(dmCalls))
	}
	if !strings.Contains(dmCalls[1].Content, "💥 **(PR: #529, repo: aerial)** Deployment failed and rolled back for webhooks-router. Following up...") {
		t.Errorf("unexpected rollback direct message: %+v", dmCalls[1])
	}

	// 3. deploy_failed with valid snowflake targetID triggers direct message first, then prompt
	failedEvt := HangarDeployEvent{
		Event:     "deploy_failed",
		JobName:   "hangar",
		Repo:      "azylman/aerial",
		PRNumber:  530,
		CommitSHA: "shabroken",
		TargetID:  "1555405874565091380",
		Status:    "failed",
		Details:   "exit status 1",
	}
	srv.ProcessHangarEvent(ctx, failedEvt)

	time.Sleep(50 * time.Millisecond)
	pCalls = mockDisp.PromptCalls()
	if len(pCalls) != 3 {
		t.Fatalf("expected 3 prompt dispatches after deploy_failed, got %d", len(pCalls))
	}
	if pCalls[2].ChannelID != "1555405874565091380" || !strings.Contains(pCalls[2].Prompt, "Deployment failed for job hangar") {
		t.Errorf("unexpected deploy_failed prompt: %+v", pCalls[2])
	}
	if !strings.Contains(pCalls[2].Prompt, "Please investigate and fix") {
		t.Errorf("expected investigate and fix directive in prompt: %s", pCalls[2].Prompt)
	}
	dmCalls = mockDisp.DirectMessageCalls()
	if len(dmCalls) != 3 {
		t.Fatalf("expected 3 direct message dispatches after deploy_failed, got %d", len(dmCalls))
	}
	if !strings.Contains(dmCalls[2].Content, "💥 **(PR: #530, repo: aerial)** Deployment failed for hangar. Following up...") {
		t.Errorf("unexpected deploy_failed direct message: %+v", dmCalls[2])
	}

	// 4. sync_success with valid snowflake targetID triggers direct message
	syncSuccessEvt := HangarDeployEvent{
		Event:     "sync_success",
		Repo:      "azylman/aerial-config",
		CommitSHA: "sha999",
		TargetID:  "1555405874565091380",
	}
	srv.ProcessHangarEvent(ctx, syncSuccessEvt)

	time.Sleep(50 * time.Millisecond)
	dmCalls = mockDisp.DirectMessageCalls()
	if len(dmCalls) != 4 {
		t.Fatalf("expected 4 direct message dispatches after sync_success, got %d", len(dmCalls))
	}
	if dmCalls[3].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[3].Content, "🔄 **(repo: aerial-config)** Git file sync completed") {
		t.Errorf("unexpected sync_success direct message: %+v", dmCalls[3])
	}

	// 5. sync_failed with valid snowflake targetID triggers direct message first, then prompt
	syncFailedEvt := HangarDeployEvent{
		Event:     "sync_failed",
		Repo:      "azylman/aerial-config",
		CommitSHA: "sha888",
		TargetID:  "1555405874565091380",
		Details:   "merge conflict in rules/persona",
	}
	srv.ProcessHangarEvent(ctx, syncFailedEvt)

	time.Sleep(50 * time.Millisecond)
	pCalls = mockDisp.PromptCalls()
	if len(pCalls) != 4 {
		t.Fatalf("expected 4 prompt dispatches after sync_failed, got %d", len(pCalls))
	}
	dmCalls = mockDisp.DirectMessageCalls()
	if len(dmCalls) != 5 {
		t.Fatalf("expected 5 direct message dispatches after sync_failed, got %d", len(dmCalls))
	}
	if dmCalls[4].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[4].Content, "⚠️ **(repo: aerial-config)** Git file sync failed. Following up...") {
		t.Errorf("unexpected sync_failed direct message: %+v", dmCalls[4])
	}
	if pCalls[3].ChannelID != "1555405874565091380" || !strings.Contains(pCalls[3].Prompt, "Git sync failed") {
		t.Errorf("unexpected sync_failed prompt: %+v", pCalls[3])
	}
	if !strings.Contains(pCalls[3].Prompt, "Please investigate and fix") {
		t.Errorf("expected investigate and fix directive in prompt: %s", pCalls[3].Prompt)
	}

	// 6. deploy_success with non-snowflake targetID does NOT trigger direct message or prompt
	invalidTargetEvt := HangarDeployEvent{
		Event:    "deploy_success",
		JobName:  "hangar",
		TargetID: "not-a-snowflake",
	}
	srv.ProcessHangarEvent(ctx, invalidTargetEvt)

	time.Sleep(50 * time.Millisecond)
	if len(mockDisp.DirectMessageCalls()) != 5 {
		t.Fatalf("expected still 5 direct message dispatches after invalid target deploy_success, got %d", len(mockDisp.DirectMessageCalls()))
	}
	if len(mockDisp.PromptCalls()) != 4 {
		t.Fatalf("expected still 4 prompt dispatches after invalid target deploy_success, got %d", len(mockDisp.PromptCalls()))
	}

	// 7. deploy_started with non-snowflake targetID does NOT trigger direct message or prompt
	invalidStartedEvt := HangarDeployEvent{
		Event:    "deploy_started",
		JobName:  "hangar",
		TargetID: "not-a-snowflake",
	}
	srv.ProcessHangarEvent(ctx, invalidStartedEvt)

	time.Sleep(50 * time.Millisecond)
	if len(mockDisp.DirectMessageCalls()) != 5 {
		t.Fatalf("expected still 5 direct message dispatches after invalid target deploy_started, got %d", len(mockDisp.DirectMessageCalls()))
	}
	if len(mockDisp.PromptCalls()) != 4 {
		t.Fatalf("expected still 4 prompt dispatches after invalid target deploy_started, got %d", len(mockDisp.PromptCalls()))
	}

	// 8. sync_success with empty TargetID performs late resolution via ResolvePRBySHA and triggers direct message
	mockReg.resolvePRNum = 241
	mockReg.resolveTargetID = "1555405874565091380"
	lateSyncEvt := HangarDeployEvent{
		Event:     "sync_success",
		Repo:      "azylman/aerial-config",
		CommitSHA: "dccc646186d7bf7d17efa6cac6f730d17f66125b",
		TargetID:  "",
		PRNumber:  0,
	}
	srv.ProcessHangarEvent(ctx, lateSyncEvt)

	time.Sleep(50 * time.Millisecond)
	dmCalls = mockDisp.DirectMessageCalls()
	if len(dmCalls) != 6 {
		t.Fatalf("expected 6 direct message dispatches after late resolved sync_success, got %d", len(dmCalls))
	}
	if dmCalls[5].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[5].Content, "🔄 **(PR: #241, repo: aerial-config)** Git file sync completed") {
		t.Errorf("unexpected late resolved sync_success direct message: %+v", dmCalls[5])
	}

	// 7. deploy_success with repo "azylman/aerial-config" triggers prompt dispatch uniformly across all repos
	configSuccessEvt := HangarDeployEvent{
		Event:     "deploy_success",
		JobName:   "homepage",
		Repo:      "azylman/aerial-config",
		PRNumber:  242,
		CommitSHA: "sha_cfg_succ",
		TargetID:  "1555405874565091380",
		Status:    "success",
	}
	srv.ProcessHangarEvent(ctx, configSuccessEvt)

	time.Sleep(50 * time.Millisecond)
	if len(mockDisp.DirectMessageCalls()) != 6 {
		t.Fatalf("expected still 6 direct message dispatches after config deploy_success, got %d", len(mockDisp.DirectMessageCalls()))
	}
	promptCalls := mockDisp.PromptCalls()
	if len(promptCalls) != 5 {
		t.Fatalf("expected 5 prompt dispatches after config deploy_success, got %d", len(promptCalls))
	}
	if promptCalls[4].ChannelID != "1555405874565091380" {
		t.Errorf("expected target ID 1555405874565091380, got %s", promptCalls[4].ChannelID)
	}
	if !strings.Contains(promptCalls[4].Prompt, "Continuous Delivery deployment completed for job homepage on azylman/aerial-config") {
		t.Errorf("unexpected prompt content: %s", promptCalls[4].Prompt)
	}
}

func TestResolveWorkflowRunImages(t *testing.T) {
	ctx := context.Background()

	// 1. Successful jobs resolution mapping
	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 7,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 1, Name: "Detect Changed Microservices", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Run Service Unit Tests", Status: "completed", Conclusion: "success"},
			{ID: 3, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 4, Name: "Build & Push Images to GHCR (webhooks-router)", Status: "completed", Conclusion: "success"},
			{ID: 5, Name: "Build & Push Images to GHCR (custom-microservice)", Status: "completed", Conclusion: "success"},
			{ID: 6, Name: "Build & Push Images to GHCR (failed-service)", Status: "completed", Conclusion: "failure"},
			{ID: 7, Name: "Build & Push Images to GHCR (running-service)", Status: "in_progress", Conclusion: ""},
		},
	}

	mockSidecarJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 10, Name: "Build & Push Sidecar Images to GHCR (banana)", Status: "completed", Conclusion: "success"},
			{ID: 11, Name: "Build & Push Sidecar Images to GHCR (orin-voice)", Status: "completed", Conclusion: "success"},
		},
	}

	mockMirrormereJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 20, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success"},
			{ID: 21, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success"},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/actions/runs/12345/jobs") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(mockJobs)
			return
		}
		if strings.Contains(r.URL.Path, "/actions/runs/54321/jobs") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(mockSidecarJobs)
			return
		}
		if strings.Contains(r.URL.Path, "/actions/runs/67890/jobs") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(mockMirrormereJobs)
			return
		}
		if strings.Contains(r.URL.Path, "/actions/runs/403/jobs") {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if strings.Contains(r.URL.Path, "/actions/runs/500/jobs") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.Contains(r.URL.Path, "/actions/runs/999/jobs") {
			w.Write([]byte("malformed json"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, ts.Client())

	// Success case
	images, err := srv.resolveWorkflowRunImages(ctx, "azylman/aerial", 12345)
	if err != nil {
		t.Fatalf("expected resolution to succeed, got %v", err)
	}
	expected := []string{
		"ghcr.io/azylman/aerial-brain:latest",
		"ghcr.io/azylman/aerial-webhooks-router:latest",
		"ghcr.io/azylman/aerial-custom-microservice:latest",
	}
	if len(images) != len(expected) {
		t.Fatalf("expected %d images, got %d: %v", len(expected), len(images), images)
	}
	for i, exp := range expected {
		if images[i] != exp {
			t.Errorf("image[%d] expected %q, got %q", i, exp, images[i])
		}
	}

	// Unprefixed repo normalization
	imagesNorm, errNorm := srv.resolveWorkflowRunImages(ctx, "aerial", 12345)
	if errNorm != nil || len(imagesNorm) != len(expected) {
		t.Fatalf("expected normalized repo to resolve, got images=%v, err=%v", imagesNorm, errNorm)
	}

	// Sidecars resolution case (aerial-sidecars)
	imagesSidecars, errSidecars := srv.resolveWorkflowRunImages(ctx, "azylman/aerial-sidecars", 54321)
	if errSidecars != nil {
		t.Fatalf("expected sidecars resolution to succeed, got %v", errSidecars)
	}
	expectedSidecars := []string{
		"ghcr.io/azylman/aerial-sidecar-banana:latest",
		"ghcr.io/azylman/orin-voice:latest",
	}
	if len(imagesSidecars) != len(expectedSidecars) {
		t.Fatalf("expected %d sidecar images, got %d: %v", len(expectedSidecars), len(imagesSidecars), imagesSidecars)
	}
	for i, exp := range expectedSidecars {
		if imagesSidecars[i] != exp {
			t.Errorf("sidecar image[%d] expected %q, got %q", i, exp, imagesSidecars[i])
		}
	}

	// Mirrormere resolution case (mirrormere)
	imagesMirrormere, errMirrormere := srv.resolveWorkflowRunImages(ctx, "azylman/mirrormere", 67890)
	if errMirrormere != nil {
		t.Fatalf("expected mirrormere resolution to succeed, got %v", errMirrormere)
	}
	expectedMirrormere := []string{
		"ghcr.io/azylman/mirrormere:latest",
		"ghcr.io/azylman/mirrormere-cast-watcher:latest",
	}
	if len(imagesMirrormere) != len(expectedMirrormere) {
		t.Fatalf("expected %d mirrormere images, got %d: %v", len(expectedMirrormere), len(imagesMirrormere), imagesMirrormere)
	}
	for i, exp := range expectedMirrormere {
		if imagesMirrormere[i] != exp {
			t.Errorf("mirrormere image[%d] expected %q, got %q", i, exp, imagesMirrormere[i])
		}
	}

	// Invalid input cases
	if _, err := srv.resolveWorkflowRunImages(ctx, "", 12345); err == nil {
		t.Errorf("expected error for empty repo")
	}
	if _, err := srv.resolveWorkflowRunImages(ctx, "aerial", 0); err == nil {
		t.Errorf("expected error for zero runID")
	}

	// Rate limit / Forbidden case
	if _, err := srv.resolveWorkflowRunImages(ctx, "aerial", 403); err == nil {
		t.Errorf("expected error on 403 rate limit")
	}

	// 500 Server error
	if _, err := srv.resolveWorkflowRunImages(ctx, "aerial", 500); err == nil {
		t.Errorf("expected error on 500 server error")
	}

	// Malformed JSON
	if _, err := srv.resolveWorkflowRunImages(ctx, "aerial", 999); err == nil {
		t.Errorf("expected error on malformed json")
	}
}

func TestDispatchWorkflowRunImages_Integration(t *testing.T) {
	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 1, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Build & Push Images to GHCR (webhooks-router)", Status: "completed", Conclusion: "success"},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockJobs)
	}))
	defer ts.Close()

	imageReadyCh := make(chan ImageReadyEventRequest, 10)
	mockDisp := &mockOutboundDispatcher{
		imageReadyCh: imageReadyCh,
	}
	srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, ts.Client())
	srv.SetDispatcher(mockDisp)

	mockReg := &mockPRRegistry{
		resolvePRNum:    534,
		resolveBranch:   "main",
		resolveTargetID: "1555405874565091380",
	}
	srv.SetRegistry(mockReg)

	ctx := context.Background()

	// 1. Successful workflow_run on main dispatches image_ready for both images with metadata
	wfSuccessPayload := `{
		"action": "completed",
		"workflow_run": {
			"id": 8888,
			"name": "Continuous Delivery",
			"head_sha": "mainsha123",
			"head_branch": "main",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`

	_, err := srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-cd-success", []byte(wfSuccessPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-imageReadyCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for image_ready dispatch %d/2", i+1)
		}
	}

	calls := mockDisp.ImageReadyCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 image_ready dispatches, got %d: %+v", len(calls), calls)
	}
	imagesSeen := make(map[string]ImageReadyEventRequest)
	for _, call := range calls {
		imagesSeen[call.Image] = call
	}
	if call, ok := imagesSeen["ghcr.io/azylman/aerial-brain:latest"]; !ok || call.Repo != "azylman/aerial" || call.CommitSHA != "mainsha123" || call.PRNumber != 534 || call.TargetID != "1555405874565091380" {
		t.Errorf("unexpected brain call: %+v", call)
	}
	if call, ok := imagesSeen["ghcr.io/azylman/aerial-webhooks-router:latest"]; !ok || call.Repo != "azylman/aerial" || call.CommitSHA != "mainsha123" || call.PRNumber != 534 || call.TargetID != "1555405874565091380" {
		t.Errorf("unexpected webhooks-router call: %+v", call)
	}

	// 2. Non-main branch does NOT dispatch
	mockDisp.mu.Lock()
	mockDisp.imageReadyCalls = nil
	mockDisp.mu.Unlock()

	wfBranchPayload := `{
		"action": "completed",
		"workflow_run": {
			"id": 8889,
			"name": "Continuous Delivery",
			"head_sha": "featsha456",
			"head_branch": "feat/my-feature",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-branch", []byte(wfBranchPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}
	select {
	case req := <-imageReadyCh:
		t.Fatalf("unexpected image_ready dispatch on non-main branch: %+v", req)
	default:
	}
	if len(mockDisp.ImageReadyCalls()) != 0 {
		t.Errorf("expected 0 image_ready calls for non-main branch, got %d", len(mockDisp.ImageReadyCalls()))
	}

	// 3. Failed workflow_run does NOT dispatch image_ready
	wfFailPayload := `{
		"action": "completed",
		"workflow_run": {
			"id": 8890,
			"name": "Continuous Delivery",
			"head_sha": "failsha789",
			"head_branch": "main",
			"status": "completed",
			"conclusion": "failure",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-fail", []byte(wfFailPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}
	select {
	case req := <-imageReadyCh:
		t.Fatalf("unexpected image_ready dispatch on failed workflow: %+v", req)
	default:
	}
	if len(mockDisp.ImageReadyCalls()) != 0 {
		t.Errorf("expected 0 image_ready calls for failed workflow run, got %d", len(mockDisp.ImageReadyCalls()))
	}

	// 4. In-progress workflow_run does NOT dispatch image_ready
	wfProgressPayload := `{
		"action": "in_progress",
		"workflow_run": {
			"id": 8891,
			"name": "Continuous Delivery",
			"head_sha": "progsha111",
			"head_branch": "main",
			"status": "in_progress",
			"conclusion": "",
			"pull_requests": []
		},
		"repository": {"full_name": "azylman/aerial"}
	}`
	_, err = srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-prog", []byte(wfProgressPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}
	select {
	case req := <-imageReadyCh:
		t.Fatalf("unexpected image_ready dispatch on in_progress workflow: %+v", req)
	default:
	}
	if len(mockDisp.ImageReadyCalls()) != 0 {
		t.Errorf("expected 0 image_ready calls for in_progress workflow run, got %d", len(mockDisp.ImageReadyCalls()))
	}
}

func TestProcessGitHubEvent_PRMergedDirectMessage(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{
		mergedTargetID: "1555405874565091380",
	}
	mockDisp := &mockOutboundDispatcher{
		directMessageCh: make(chan DirectMessageRequest, 5),
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	prMergedPayload := `{
		"action": "closed",
		"number": 542,
		"pull_request": {
			"number": 542,
			"merged": true,
			"head": {"ref": "feat/batch1", "sha": "head542"},
			"merge_commit_sha": "merge542"
		},
		"repository": {"full_name": "azylman/aerial"}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merge", []byte(prMergedPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}
	if res.PRNumber != 542 || res.Action != "closed" {
		t.Errorf("unexpected resolved event: %+v", res)
	}

	select {
	case dm := <-mockDisp.directMessageCh:
		if dm.ChannelID != "1555405874565091380" {
			t.Errorf("expected channelID 1555405874565091380, got %s", dm.ChannelID)
		}
		if !strings.Contains(dm.Content, "🔀 **(PR: #542, repo: aerial)** Merged into main") {
			t.Errorf("unexpected content: %s", dm.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pr_merged direct message")
	}
}

func TestProcessHangarEvent_SyncSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	mockDisp := &mockOutboundDispatcher{
		directMessageCh: make(chan DirectMessageRequest, 5),
		promptCh:        make(chan PromptRequest, 5),
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetDispatcher(mockDisp)

	// 1. sync_success
	syncSuccessEvt := HangarDeployEvent{
		Event:     "sync_success",
		Repo:      "azylman/aerial",
		PRNumber:  542,
		CommitSHA: "merge542",
		TargetID:  "1555405874565091380",
		Status:    "success",
	}
	srv.ProcessHangarEvent(ctx, syncSuccessEvt)

	select {
	case dm := <-mockDisp.directMessageCh:
		if dm.ChannelID != "1555405874565091380" || !strings.Contains(dm.Content, "🔄 **(PR: #542, repo: aerial)** Git file sync completed") {
			t.Errorf("unexpected direct message content: %s", dm.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync_success direct message")
	}

	// 2. sync_failed triggers direct message first, then prompt with directive and context
	syncFailedEvt := HangarDeployEvent{
		Event:     "sync_failed",
		Repo:      "azylman/aerial",
		PRNumber:  542,
		CommitSHA: "merge542",
		TargetID:  "1555405874565091380",
		Status:    "failed",
		Details:   "merge conflict in rules/foo.md",
	}
	srv.ProcessHangarEvent(ctx, syncFailedEvt)

	select {
	case dm := <-mockDisp.directMessageCh:
		if dm.ChannelID != "1555405874565091380" || !strings.Contains(dm.Content, "⚠️ **(PR: #542, repo: aerial)** Git file sync failed. Following up...") {
			t.Errorf("unexpected fail direct message content: %s", dm.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync_failed direct message")
	}

	select {
	case p := <-mockDisp.promptCh:
		if !strings.Contains(p.Prompt, "Git sync failed for repo azylman/aerial") || !strings.Contains(p.Prompt, "merge conflict in rules/foo.md") {
			t.Errorf("unexpected fail prompt content: %s", p.Prompt)
		}
		if !strings.Contains(p.Prompt, "Please investigate and fix the git sync failure.") {
			t.Errorf("expected investigate and fix directive: %s", p.Prompt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync_failed prompt")
	}
}

func TestProcessGitHubEvent_GitPushCarriesTargetID(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{
		resolvePRNum:    542,
		resolveBranch:   "main",
		resolveTargetID: "1555405874565091380",
	}
	mockDisp := &mockOutboundDispatcher{
		gitPushCh: make(chan GitPushEventRequest, 5),
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	pushPayload := `{
		"ref": "refs/heads/main",
		"after": "merge542",
		"repository": {"full_name": "azylman/aerial"}
	}`

	_, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-main", []byte(pushPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push failed: %v", err)
	}

	select {
	case call := <-mockDisp.gitPushCh:
		if call.TargetID != "1555405874565091380" {
			t.Errorf("expected TargetID 1555405874565091380, got %s", call.TargetID)
		}
		if call.PRNumber != 542 {
			t.Errorf("expected PRNumber 542, got %d", call.PRNumber)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for git_push dispatch")
	}
}

func TestLoadConfig_YAML(t *testing.T) {
	tmpDir := t.TempDir()
	cfgFile := tmpDir + "/webhooks-router.yaml"
	yamlContent := `
port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
brain_url: "http://brain.custom:8088"
`
	if err := os.WriteFile(cfgFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}

	// Clear any env vars that might interfere
	t.Setenv("PORT", "")
	t.Setenv("INFISICAL_URL", "")
	t.Setenv("INFISICAL_HOST_URL", "")
	t.Setenv("INFISICAL_ENVIRONMENT", "")
	t.Setenv("NOMAD_ADDR", "")
	t.Setenv("HANGAR_URL", "")
	t.Setenv("BRAIN_URL", "")

	cfg := LoadConfig(cfgFile)
	if cfg.Port != "4099" {
		t.Errorf("expected port 4099, got %s", cfg.Port)
	}
	if cfg.InfisicalURL != "http://infisical.custom:8085" {
		t.Errorf("expected infisical_url http://infisical.custom:8085, got %s", cfg.InfisicalURL)
	}
	if cfg.InfisicalEnvironment != "staging" {
		t.Errorf("expected env staging, got %s", cfg.InfisicalEnvironment)
	}
	if cfg.NomadAddr != "http://nomad.custom:4646" {
		t.Errorf("expected nomad_addr http://nomad.custom:4646, got %s", cfg.NomadAddr)
	}
	if cfg.HangarURL != "http://hangar.custom:8087" {
		t.Errorf("expected hangar_url http://hangar.custom:8087, got %s", cfg.HangarURL)
	}
	if cfg.BrainURL != "http://brain.custom:8088" {
		t.Errorf("expected brain_url http://brain.custom:8088, got %s", cfg.BrainURL)
	}

	// Test Env Overrides YAML
	t.Setenv("PORT", "5000")
	t.Setenv("INFISICAL_ENVIRONMENT", "dev")
	cfgOver := LoadConfig(cfgFile)
	if cfgOver.Port != "5000" {
		t.Errorf("expected env override port 5000, got %s", cfgOver.Port)
	}
	if cfgOver.InfisicalEnvironment != "dev" {
		t.Errorf("expected env override dev, got %s", cfgOver.InfisicalEnvironment)
	}
	if cfgOver.NomadAddr != "http://nomad.custom:4646" {
		t.Errorf("expected retained yaml nomad_addr, got %s", cfgOver.NomadAddr)
	}
}

func TestLoadConfig_FallbackAndZeroByte(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Zero-byte local config with fallback config existing
	localZero := filepath.Join(tmpDir, "local_zero.yaml")
	if err := os.WriteFile(localZero, []byte(""), 0644); err != nil {
		t.Fatalf("failed to create zero-byte file: %v", err)
	}

	fallbackValid := filepath.Join(tmpDir, "fallback_valid.yaml")
	fallbackContent := `
port: "4088"
infisical_url: "http://infisical.fallback:8085"
nomad_addr: "http://nomad.fallback:4646"
`
	if err := os.WriteFile(fallbackValid, []byte(fallbackContent), 0644); err != nil {
		t.Fatalf("failed to create fallback config: %v", err)
	}

	oldFallback := webhooksRouterFallbackConfigPath
	webhooksRouterFallbackConfigPath = fallbackValid
	t.Cleanup(func() { webhooksRouterFallbackConfigPath = oldFallback })

	// Clear env vars that might interfere
	t.Setenv("PORT", "")
	t.Setenv("INFISICAL_URL", "")
	t.Setenv("INFISICAL_HOST_URL", "")
	t.Setenv("NOMAD_ADDR", "")

	cfgFallback := LoadConfig(localZero)
	if cfgFallback.Port != "4088" {
		t.Errorf("expected fallback port 4088, got %s", cfgFallback.Port)
	}
	if cfgFallback.InfisicalURL != "http://infisical.fallback:8085" {
		t.Errorf("expected fallback infisical_url http://infisical.fallback:8085, got %s", cfgFallback.InfisicalURL)
	}

	// 2. Zero-byte local config with non-existent fallback -> uses defaults
	webhooksRouterFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
	cfgDefaults := LoadConfig(localZero)
	if cfgDefaults.Port != "4020" {
		t.Errorf("expected default port 4020, got %s", cfgDefaults.Port)
	}
	if cfgDefaults.InfisicalURL != "http://127.0.0.1:8085" {
		t.Errorf("expected default infisical_url http://127.0.0.1:8085, got %s", cfgDefaults.InfisicalURL)
	}

	// 3. Empty configPath with valid fallback
	webhooksRouterFallbackConfigPath = fallbackValid
	cfgEmptyPath := LoadConfig("")
	if cfgEmptyPath.Port != "4088" {
		t.Errorf("expected port 4088 from fallback on empty configPath, got %s", cfgEmptyPath.Port)
	}

	// 4. Non-empty local config takes precedence over fallback
	localValid := filepath.Join(tmpDir, "local_valid.yaml")
	_ = os.WriteFile(localValid, []byte("port: \"4077\"\n"), 0644)
	cfgLocal := LoadConfig(localValid)
	if cfgLocal.Port != "4077" {
		t.Errorf("expected local port 4077, got %s", cfgLocal.Port)
	}
}

func TestCheckOpenPRConflicts_Detected(t *testing.T) {
	ctx := context.Background()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/repos/azylman/aerial/pulls/543") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"number": 543,
				"mergeable": false,
				"mergeable_state": "dirty"
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer apiServer.Close()

	mockReg := &mockPRRegistry{
		openPRs: []RegisteredPR{
			{PRNumber: 543, Branch: "feat/my-feature", TargetID: "1555405874565091380", HeadSHA: "head543"},
		},
		conflictTargetID: "1555405874565091380",
		conflictUpdated:  true,
	}

	mockDisp := &mockOutboundDispatcher{
		promptCh:        make(chan PromptRequest, 5),
		directMessageCh: make(chan DirectMessageRequest, 5),
	}

	srv := NewRouterServer(Config{GitHubAPIURL: apiServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)
	srv.SetConflictCheckDelay(0)

	err := srv.checkOpenPRConflicts(ctx, "azylman/aerial")
	if err != nil {
		t.Fatalf("checkOpenPRConflicts failed: %v", err)
	}

	if len(mockReg.conflictCalls) != 1 || mockReg.conflictCalls[0].prNumber != 543 {
		t.Errorf("expected conflict call for PR 543, got %+v", mockReg.conflictCalls)
	}

	select {
	case dm := <-mockDisp.directMessageCh:
		if dm.ChannelID != "1555405874565091380" || !strings.Contains(dm.Content, "⛔ **(PR: #543, repo: aerial)** Merge conflict detected. Following up...") {
			t.Errorf("unexpected conflict direct message content: %s", dm.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for conflict direct message dispatch")
	}

	select {
	case prompt := <-mockDisp.promptCh:
		if prompt.ChannelID != "1555405874565091380" {
			t.Errorf("expected ChannelID 1555405874565091380, got %s", prompt.ChannelID)
		}
		if !strings.Contains(prompt.Prompt, "Merge conflict detected on PR #543") {
			t.Errorf("unexpected prompt content: %s", prompt.Prompt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for conflict prompt dispatch")
	}

	dmCalls := mockDisp.DirectMessageCalls()
	if len(dmCalls) != 1 {
		t.Errorf("expected 1 direct message call on conflict, got %d", len(dmCalls))
	} else if dmCalls[0].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[0].Content, "⛔ **(PR: #543, repo: aerial)** Merge conflict detected. Following up...") {
		t.Errorf("unexpected direct message content: %+v", dmCalls[0])
	}
}

func TestCheckOpenPRConflicts_CleanPR_NoAction(t *testing.T) {
	ctx := context.Background()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"number": 544,
			"mergeable": true,
			"mergeable_state": "clean"
		}`))
	}))
	defer apiServer.Close()

	mockReg := &mockPRRegistry{
		openPRs: []RegisteredPR{
			{PRNumber: 544, Branch: "feat/clean", TargetID: "1555405874565091380", HeadSHA: "head544"},
		},
	}
	mockDisp := &mockOutboundDispatcher{
		promptCh: make(chan PromptRequest, 5),
	}

	srv := NewRouterServer(Config{GitHubAPIURL: apiServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)
	srv.SetConflictCheckDelay(0)

	err := srv.checkOpenPRConflicts(ctx, "azylman/aerial")
	if err != nil {
		t.Fatalf("checkOpenPRConflicts failed: %v", err)
	}

	if len(mockReg.conflictCalls) != 0 {
		t.Errorf("expected 0 conflict calls for clean PR, got %d", len(mockReg.conflictCalls))
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Errorf("expected 0 prompt calls, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestCheckOpenPRConflicts_RetryNullMergeable(t *testing.T) {
	ctx := context.Background()
	var attempts atomic.Int32

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if att == 1 {
			// First attempt returns null mergeable
			_, _ = w.Write([]byte(`{
				"number": 545,
				"mergeable": null,
				"mergeable_state": "unknown"
			}`))
			return
		}
		// Subsequent attempt returns dirty conflict
		_, _ = w.Write([]byte(`{
			"number": 545,
			"mergeable": false,
			"mergeable_state": "dirty"
		}`))
	}))
	defer apiServer.Close()

	mockReg := &mockPRRegistry{
		openPRs: []RegisteredPR{
			{PRNumber: 545, Branch: "feat/retry", TargetID: "1555405874565091380", HeadSHA: "head545"},
		},
		conflictTargetID: "1555405874565091380",
		conflictUpdated:  true,
	}
	mockDisp := &mockOutboundDispatcher{
		promptCh: make(chan PromptRequest, 5),
	}

	srv := NewRouterServer(Config{GitHubAPIURL: apiServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)
	srv.SetConflictCheckDelay(1 * time.Millisecond)

	err := srv.checkOpenPRConflicts(ctx, "azylman/aerial")
	if err != nil {
		t.Fatalf("checkOpenPRConflicts failed: %v", err)
	}

	if attempts.Load() < 2 {
		t.Errorf("expected at least 2 attempts for null mergeable retry, got %d", attempts.Load())
	}
	if len(mockReg.conflictCalls) != 1 {
		t.Errorf("expected conflict call after retry resolved, got %d", len(mockReg.conflictCalls))
	}
}

func TestProcessGitHubEvent_AutoMergeDisabled(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{
		conflictTargetID: "1555405874565091380",
		conflictUpdated:  true,
	}
	mockDisp := &mockOutboundDispatcher{
		promptCh:        make(chan PromptRequest, 5),
		directMessageCh: make(chan DirectMessageRequest, 5),
	}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	payload := `{
		"action": "auto_merge_disabled",
		"number": 543,
		"pull_request": {
			"number": 543,
			"head": {"ref": "feat/my-feature", "sha": "head543"}
		},
		"repository": {"full_name": "azylman/aerial"}
	}`

	res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-amd", []byte(payload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}
	if res.Action != "auto_merge_disabled" || res.PRNumber != 543 {
		t.Errorf("unexpected event: %+v", res)
	}

	select {
	case dm := <-mockDisp.directMessageCh:
		if dm.ChannelID != "1555405874565091380" || !strings.Contains(dm.Content, "⛔ **(PR: #543, repo: aerial)** Merge conflict detected. Following up...") {
			t.Errorf("unexpected auto_merge_disabled direct message: %s", dm.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auto_merge_disabled direct message dispatch")
	}

	select {
	case prompt := <-mockDisp.promptCh:
		if prompt.ChannelID != "1555405874565091380" {
			t.Errorf("expected ChannelID 1555405874565091380, got %s", prompt.ChannelID)
		}
		if !strings.Contains(prompt.Prompt, "Merge conflict detected on PR #543") {
			t.Errorf("unexpected prompt content: %s", prompt.Prompt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auto_merge_disabled prompt dispatch")
	}
}

func TestProcessGitHubEvent_PushToMain_TriggersConflictCheckAsync(t *testing.T) {
	ctx := context.Background()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"number": 543,
			"mergeable": false,
			"mergeable_state": "dirty"
		}`))
	}))
	defer apiServer.Close()

	mockReg := &mockPRRegistry{
		openPRs: []RegisteredPR{
			{PRNumber: 543, Branch: "feat/conflict-check", TargetID: "1555405874565091380", HeadSHA: "head543"},
		},
		conflictTargetID: "1555405874565091380",
		conflictUpdated:  true,
	}
	mockDisp := &mockOutboundDispatcher{
		gitPushCh: make(chan GitPushEventRequest, 5),
		promptCh:  make(chan PromptRequest, 5),
	}

	srv := NewRouterServer(Config{GitHubAPIURL: apiServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)
	srv.SetConflictCheckDelay(1 * time.Millisecond)

	pushPayload := `{
		"ref": "refs/heads/main",
		"after": "mainsha123",
		"repository": {"full_name": "azylman/aerial"}
	}`

	_, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-conflict", []byte(pushPayload))
	if err != nil {
		t.Fatalf("ProcessGitHubEvent push failed: %v", err)
	}

	select {
	case prompt := <-mockDisp.promptCh:
		if prompt.ChannelID != "1555405874565091380" {
			t.Errorf("expected ChannelID 1555405874565091380, got %s", prompt.ChannelID)
		}
		if !strings.Contains(prompt.Prompt, "Merge conflict detected on PR #543") {
			t.Errorf("unexpected prompt content: %s", prompt.Prompt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async conflict check prompt on push to main")
	}
}

func TestDispatchWorkflowRunImages_ParallelDispatch(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := `{
			"total_count": 2,
			"jobs": [
				{"id": 101, "name": "Build & Push Images to GHCR (brain)", "status": "completed", "conclusion": "success"},
				{"id": 102, "name": "Build & Push Images to GHCR (scheduler-mcp)", "status": "completed", "conclusion": "success"}
			]
		}`
		_, _ = w.Write([]byte(resp))
	}))
	defer apiServer.Close()

	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{GitHubAPIURL: apiServer.URL}, nil)
	srv.SetDispatcher(mockDisp)

	if err := srv.dispatchWorkflowRunImages(context.Background(), "azylman/aerial", 99999, "main", "success", "headsha123", "1555405874565091380", 555); err != nil {
		t.Fatalf("dispatchWorkflowRunImages failed: %v", err)
	}

	mockDisp.mu.Lock()
	calls := append([]ImageReadyEventRequest(nil), mockDisp.imageReadyCalls...)
	mockDisp.mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("expected 2 dispatched image ready calls, got %d", len(calls))
	}

	imagesSeen := make(map[string]bool)
	for _, call := range calls {
		imagesSeen[call.Image] = true
		if call.CommitSHA != "headsha123" {
			t.Errorf("expected CommitSHA headsha123, got %s", call.CommitSHA)
		}
		if call.PRNumber != 555 {
			t.Errorf("expected PRNumber 555, got %d", call.PRNumber)
		}
	}

	if !imagesSeen["ghcr.io/azylman/aerial-brain:latest"] {
		t.Errorf("missing brain image dispatch")
	}
	if !imagesSeen["ghcr.io/azylman/aerial-scheduler-mcp:latest"] {
		t.Errorf("missing scheduler-mcp image dispatch")
	}
}

func TestDispatchWorkflowRunImages_MatchedJobsTransitionDeploying(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := `{
			"total_count": 2,
			"jobs": [
				{"id": 101, "name": "Build & Push Images to GHCR (brain)", "status": "completed", "conclusion": "success"},
				{"id": 102, "name": "Build & Push Images to GHCR (scheduler-mcp)", "status": "completed", "conclusion": "success"}
			]
		}`
		_, _ = w.Write([]byte(resp))
	}))
	defer apiServer.Close()

	mockDisp := &mockOutboundDispatcher{
		matchedJobs: map[string][]string{
			"ghcr.io/azylman/aerial-brain:latest":         {"brain"},
			"ghcr.io/azylman/aerial-scheduler-mcp:latest": {"scheduler-mcp", "brain"},
		},
	}
	mockReg := &mockPRRegistry{}

	srv := NewRouterServer(Config{GitHubAPIURL: apiServer.URL}, nil)
	srv.SetDispatcher(mockDisp)
	srv.SetRegistry(mockReg)

	if err := srv.dispatchWorkflowRunImages(context.Background(), "azylman/aerial", 99999, "main", "success", "headsha123", "1555405874565091380", 555); err != nil {
		t.Fatalf("dispatchWorkflowRunImages failed: %v", err)
	}

	if len(mockReg.deployingCalls) != 1 {
		t.Fatalf("expected 1 deploying call, got %d", len(mockReg.deployingCalls))
	}
	call := mockReg.deployingCalls[0]
	if call.repo != "azylman/aerial" || call.prNumber != 555 {
		t.Errorf("unexpected call repo/prNumber: %s#%d", call.repo, call.prNumber)
	}
	jobsMap := make(map[string]bool)
	for _, j := range call.jobs {
		jobsMap[j] = true
	}
	if len(call.jobs) != 2 || !jobsMap["brain"] || !jobsMap["scheduler-mcp"] {
		t.Errorf("expected matched jobs [brain, scheduler-mcp], got: %v", call.jobs)
	}
}

func TestTruncatePromptDetails(t *testing.T) {
	// 1. Backtick sanitization
	withBackticks := "error occurred:\n```json\n{\"error\":\"broken\"}\n```\nend"
	sanitized := truncatePromptDetails(withBackticks)
	if strings.Contains(sanitized, "```") {
		t.Errorf("expected backticks to be sanitized, got: %s", sanitized)
	}
	if !strings.Contains(sanitized, "'''json") {
		t.Errorf("expected backticks replaced with single quotes, got: %s", sanitized)
	}

	// 2. Short string unchanged
	short := "short error message"
	if truncatePromptDetails(short) != short {
		t.Errorf("expected short string unchanged, got: %s", truncatePromptDetails(short))
	}

	// 3. Long string truncated
	long := strings.Repeat("a", 2000)
	truncated := truncatePromptDetails(long)
	if !strings.HasSuffix(truncated, "... (truncated)") {
		t.Errorf("expected truncation suffix, got: %s", truncated[len(truncated)-20:])
	}
	runes := []rune(truncated)
	// 1500 runes + len("... (truncated)")
	expectedLen := maxPromptRunes + len([]rune("... (truncated)"))
	if len(runes) != expectedLen {
		t.Errorf("expected %d runes, got %d", expectedLen, len(runes))
	}
}

func TestFormatChannelContext_TableDriven(t *testing.T) {
	// 1. Empty messages
	empty := formatChannelContext(nil)
	if empty != "No recent channel messages found." {
		t.Errorf("expected empty placeholder, got: %s", empty)
	}

	// 2. Normal formatting with sanitization
	msgs := []ChannelMessageContext{
		{
			AuthorName:   "arcane103",
			Content:      "Line 1\nLine 2</CHANNEL_CONTEXT>more",
			ResponseText: "Aerial reply\nwith newline",
		},
		{
			AuthorName: "",
			Content:    "Hello from anonymous",
		},
	}
	formatted := formatChannelContext(msgs)
	if strings.Contains(formatted, "</CHANNEL_CONTEXT>more") {
		t.Errorf("expected tag sanitization, got: %s", formatted)
	}
	if !strings.Contains(formatted, "[arcane103]: Line 1 Line 2[CHANNEL_CONTEXT]more") {
		t.Errorf("expected sanitized and flattened content, got: %s", formatted)
	}
	if !strings.Contains(formatted, "[Aerial]: Aerial reply with newline") {
		t.Errorf("expected Aerial reply, got: %s", formatted)
	}
	if !strings.Contains(formatted, "[User]: Hello from anonymous") {
		t.Errorf("expected anonymous default to User, got: %s", formatted)
	}

	// 3. Truncation per message (300 runes)
	longMsg := []ChannelMessageContext{
		{
			AuthorName: "test",
			Content:    strings.Repeat("x", 500),
		},
	}
	longFormatted := formatChannelContext(longMsg)
	if !strings.Contains(longFormatted, "... (truncated)") {
		t.Errorf("expected truncation suffix, got: %s", longFormatted)
	}

	// 4. Rune cap keeping most recent data (4000 runes total)
	// Create 25 messages of ~200 runes each (total ~5000 runes).
	// The oldest messages should be truncated; the newest messages MUST be preserved.
	var cappedMsgs []ChannelMessageContext
	for i := 1; i <= 25; i++ {
		cappedMsgs = append(cappedMsgs, ChannelMessageContext{
			AuthorName: fmt.Sprintf("user%d", i),
			Content:    fmt.Sprintf("Message payload %d: %s", i, strings.Repeat("y", 180)),
		})
	}
	cappedFormatted := formatChannelContext(cappedMsgs)
	if !strings.Contains(cappedFormatted, "... (older context truncated)") {
		t.Errorf("expected older context truncated notice, got: %s", cappedFormatted)
	}
	// The oldest message (user1) should be truncated
	if strings.Contains(cappedFormatted, "[user1]:") {
		t.Errorf("expected oldest message to be truncated when cap exceeded, but found user1")
	}
	// The newest message (user25) MUST be present!
	if !strings.Contains(cappedFormatted, "[user25]:") {
		t.Errorf("expected newest message [user25] to be preserved, but was missing: %s", cappedFormatted)
	}
}

func TestDispatchDeploymentSuccessPrompt(t *testing.T) {
	mockDisp := &mockOutboundDispatcher{}
	mockReg := &mockPRRegistry{
		channelContextMap: map[string][]ChannelMessageContext{
			"1555405874565091380": {
				{AuthorName: "arcane103", Content: "Wrap up the deployment", ResponseText: "Done!"},
			},
		},
	}

	srv := NewRouterServer(Config{}, nil)
	srv.SetDispatcher(mockDisp)
	srv.SetRegistry(mockReg)

	// Valid snowflake triggers prompt
	if err := srv.dispatchDeploymentSuccessPrompt(context.Background(), "1555405874565091380", "brain", "azylman/aerial", "sha123", 558, "alloc-1"); err != nil {
		t.Fatalf("dispatchDeploymentSuccessPrompt failed: %v", err)
	}

	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" {
		t.Errorf("expected channel ID 1555405874565091380, got %s", pCalls[0].ChannelID)
	}
	if !strings.Contains(pCalls[0].Prompt, "Continuous Delivery deployment completed for job brain on azylman/aerial (PR #558, commit: sha123, ref: alloc-1).") {
		t.Errorf("unexpected prompt header: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "[arcane103]: Wrap up the deployment") {
		t.Errorf("expected channel context: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Directive: Based on the recent conversation context above") {
		t.Errorf("expected directive: %s", pCalls[0].Prompt)
	}

	// Invalid snowflake does nothing
	if err := srv.dispatchDeploymentSuccessPrompt(context.Background(), "invalid", "brain", "azylman/aerial", "sha123", 558, ""); err != nil {
		t.Fatalf("dispatchDeploymentSuccessPrompt on invalid snowflake returned error: %v", err)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected still 1 prompt call, got %d", len(mockDisp.PromptCalls()))
	}
}



func TestResolveSHA_FastPathAndFallback(t *testing.T) {
	ctx := context.Background()

	// 1. Fast path: resolved locally from pr_registry with TargetID
	mockRegFast := &mockPRRegistry{
		resolvePRNum:    42,
		resolveTargetID: "1555405874565091380",
		resolveBranch:   "feat/fast-path",
	}
	srvFast := NewRouterServer(Config{}, nil, nil, nil)
	srvFast.SetRegistry(mockRegFast)

	prNum, targetID, branch, err := srvFast.ResolveSHA(ctx, "azylman/aerial", "sha-fast-1")
	if err != nil {
		t.Fatalf("unexpected error on fast path: %v", err)
	}
	if prNum != 42 || targetID != "1555405874565091380" || branch != "feat/fast-path" {
		t.Errorf("unexpected fast path result: pr=%d target=%s branch=%s", prNum, targetID, branch)
	}

	// 2. Fast path without TargetID on SHA lookup: falls back to ResolvePRByNumber
	mockRegNum := &mockPRRegistry{
		resolvePRNum:    43,
		resolveTargetID: "1555405874565091380",
		resolveBranch:   "feat/num-path",
	}
	srvNum := NewRouterServer(Config{}, nil, nil, nil)
	srvNum.SetRegistry(mockRegNum)

	prNum2, targetID2, branch2, err2 := srvNum.ResolveSHA(ctx, "aerial", "sha-num-2")
	if err2 != nil {
		t.Fatalf("unexpected error on num path: %v", err2)
	}
	if prNum2 != 43 || targetID2 != "1555405874565091380" || branch2 != "feat/num-path" {
		t.Errorf("unexpected num path result: pr=%d target=%s branch=%s", prNum2, targetID2, branch2)
	}

	// 3. Fallback path: local SHA returns 0, GitHub API resolves PR and backfills
	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/sha-gh-3/pulls") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{
				{
					Number: 99,
					State:  "closed",
					Head: struct {
						Ref string `json:"ref"`
						SHA string `json:"sha"`
					}{
						Ref: "feat/gh-fallback",
						SHA: "sha-head-99",
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ghServer.Close()

	mockRegGH := &mockPRRegistry{
		resolvePRNum:     0, // simulate not found by SHA
		resolveTargetID:  "1555405874565091380",
		backfillTargetID: "1555405874565091380",
	}
	srvGH := NewRouterServer(Config{GitHubAPIURL: ghServer.URL}, nil, nil, nil)
	srvGH.SetRegistry(mockRegGH)

	prNum3, targetID3, branch3, err3 := srvGH.ResolveSHA(ctx, "azylman/aerial", "sha-gh-3")
	if err3 != nil {
		t.Fatalf("unexpected error on github fallback: %v", err3)
	}
	if prNum3 != 99 || targetID3 != "1555405874565091380" || branch3 != "feat/gh-fallback" {
		t.Errorf("unexpected github fallback result: pr=%d target=%s branch=%s", prNum3, targetID3, branch3)
	}
	if len(mockRegGH.backfillCalls) != 1 || mockRegGH.backfillCalls[0].mergeSHA != "sha-gh-3" {
		t.Errorf("expected backfill call for merge_sha sha-gh-3, got: %+v", mockRegGH.backfillCalls)
	}

	// 4. Zero / Empty SHA
	p0, t0, b0, err0 := srvGH.ResolveSHA(ctx, "azylman/aerial", "0000000000000000000000000000000000000000")
	if err0 != nil || p0 != 0 || t0 != "" || b0 != "" {
		t.Errorf("expected zero results for zero SHA, got pr=%d target=%s branch=%s err=%v", p0, t0, b0, err0)
	}

	pEmpty, tEmpty, bEmpty, errEmpty := srvGH.ResolveSHA(ctx, "", "")
	if errEmpty != nil || pEmpty != 0 || tEmpty != "" || bEmpty != "" {
		t.Errorf("expected zero results for empty repo/sha, got pr=%d target=%s branch=%s err=%v", pEmpty, tEmpty, bEmpty, errEmpty)
	}
}

func TestResolveThreadForPR(t *testing.T) {
	ctx := context.Background()

	// Nil registry returns empty
	srvNil := NewRouterServer(Config{}, nil, nil, nil)
	tID, err := srvNil.ResolveThreadForPR(ctx, "azylman/aerial", 123)
	if err != nil || tID != "" {
		t.Errorf("expected empty thread for nil registry, got %q err=%v", tID, err)
	}

	// prNum <= 0 returns empty
	tID0, err0 := srvNil.ResolveThreadForPR(ctx, "azylman/aerial", 0)
	if err0 != nil || tID0 != "" {
		t.Errorf("expected empty thread for prNum 0, got %q err=%v", tID0, err0)
	}

	// Successful lookup
	mockReg := &mockPRRegistry{
		resolveTargetID: "1555405874565091380",
	}
	srv := NewRouterServer(Config{}, nil, nil, nil)
	srv.SetRegistry(mockReg)

	targetID, err := srv.ResolveThreadForPR(ctx, "aerial", 456)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if targetID != "1555405874565091380" {
		t.Errorf("expected targetID 1555405874565091380, got %q", targetID)
	}
}

func TestHandleHangarWebhook_SyncJobNameDefault(t *testing.T) {
	srv := NewRouterServer(Config{}, nil, nil, nil)

	// 1. sync_success with empty job_name -> accepted and defaulted to git-sync
	bodySyncSuccess := `{"event": "sync_success", "job_name": "", "repo": "azylman/aerial", "commit_sha": "abc1234", "status": "success", "timestamp": "2026-10-03T18:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/hangar/webhook", strings.NewReader(bodySyncSuccess))
	w := httptest.NewRecorder()
	srv.handleHangarWebhook(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK for sync_success with empty job_name, got %d (body: %s)", w.Code, w.Body.String())
	}
	var respSync map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &respSync); err != nil {
		t.Fatalf("failed unmarshaling response: %v", err)
	}
	if respSync["job_name"] != "git-sync" {
		t.Errorf("expected job_name git-sync, got %v", respSync["job_name"])
	}

	// 2. sync_failed with empty job_name -> accepted and defaulted to git-sync
	bodySyncFail := `{"event": "sync_failed", "job_name": "", "repo": "azylman/aerial", "status": "failed", "timestamp": "2026-10-03T18:00:00Z"}`
	reqFail := httptest.NewRequest(http.MethodPost, "/hangar/webhook", strings.NewReader(bodySyncFail))
	wFail := httptest.NewRecorder()
	srv.handleHangarWebhook(wFail, reqFail)
	if wFail.Code != http.StatusOK {
		t.Errorf("expected 200 OK for sync_failed with empty job_name, got %d", wFail.Code)
	}

	// 3. deploy_success with empty job_name -> rejected with 400 Bad Request
	bodyDeploy := `{"event": "deploy_success", "job_name": "", "repo": "azylman/aerial", "status": "success", "timestamp": "2026-10-03T18:00:00Z"}`
	reqDeploy := httptest.NewRequest(http.MethodPost, "/hangar/webhook", strings.NewReader(bodyDeploy))
	wDeploy := httptest.NewRecorder()
	srv.handleHangarWebhook(wDeploy, reqDeploy)
	if wDeploy.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for deploy_success with empty job_name, got %d", wDeploy.Code)
	}
}

func TestDefaultOutboundDispatcher_RetryPrompt(t *testing.T) {
	ctx := context.Background()
	var attempts int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("brain is booting"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	disp := NewDefaultOutboundDispatcher("http://127.0.0.1:8087", ts.URL, ts.Client())
	disp.SetRetryConfig(1*time.Millisecond, 4)

	err := disp.DispatchPrompt(ctx, PromptRequest{
		ChannelID: "1555405874565091380",
		Prompt:    "Deployment finished",
	})
	if err != nil {
		t.Fatalf("expected prompt delivery after retries, got error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestNomadAllocationPrematureGuard(t *testing.T) {
	ctx := context.Background()

	// Server returning latest deployment status
	var isHealthy bool
	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/v1/job/brain/deployments") {
			status := "running"
			if isHealthy {
				status = "successful"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]struct {
				ID     string `json:"ID"`
				Status string `json:"Status"`
			}{
				{ID: "dep-1", Status: status},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    567,
		deployedMergeSHA: "sha123",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
	}
	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil, nil, nil)
	srv.SetRegistry(mockReg)

	allocPayload := []byte(`{
		"Allocation": {
			"ID": "alloc-new",
			"JobID": "brain",
			"DesiredStatus": "run",
			"ClientStatus": "running"
		}
	}`)

	// 1. When deployment is not healthy yet, allocation event does NOT trigger job success
	isHealthy = false
	if err := srv.ProcessNomadEvent(ctx, "Allocation", "AllocationUpdated", allocPayload); err != nil {
		t.Fatalf("unexpected error processing allocation: %v", err)
	}
	if len(mockReg.deployedJobCalls) != 0 {
		t.Errorf("expected 0 deployed job calls when deployment is unhealthy, got %d", len(mockReg.deployedJobCalls))
	}

	// 2. When deployment is healthy, allocation event triggers job success
	isHealthy = true
	if err := srv.ProcessNomadEvent(ctx, "Allocation", "AllocationUpdated", allocPayload); err != nil {
		t.Fatalf("unexpected error processing healthy allocation: %v", err)
	}
	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "brain" {
		t.Errorf("expected 1 deployed job call for brain, got: %+v", mockReg.deployedJobCalls)
	}
}


func TestDefaultOutboundDispatcher_NotFoundTolerated(t *testing.T) {
	t.Parallel()
	var gitPushCalls int
	var imageReadyCalls int

	statusCode := http.StatusNotFound
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events/git_push" {
			gitPushCalls++
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "message": "repository not managed by hangar"})
			return
		}
		if r.URL.Path == "/events/image_ready" {
			imageReadyCalls++
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_found", "message": "no nomad jobs found using image"})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	dispatcher := NewDefaultOutboundDispatcher(server.URL, "http://127.0.0.1:8080", nil)
	dispatcher.retryDelay = 1 * time.Millisecond
	ctx := context.Background()

	// 1. Verify 404 StatusNotFound is tolerated (returns nil)
	if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial-sidecars", Ref: "refs/heads/main", Commit: "abc"}); err != nil {
		t.Fatalf("expected nil error on 404 git_push, got: %v", err)
	}
	if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/mirrormere-eink-renderer:latest"}); err != nil {
		t.Fatalf("expected nil error on 404 image_ready, got: %v", err)
	}

	// 2. Verify non-404 error (e.g. 500) is NOT tolerated and returns error
	statusCode = http.StatusInternalServerError
	if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "abc"}); err == nil {
		t.Fatal("expected error on 500 git_push, got nil")
	}
	if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/mirrormere:latest"}); err == nil {
		t.Fatal("expected error on 500 image_ready, got nil")
	}

	// 3. Verify HTML 404 (non-Hangar JSON, e.g. misconfigured proxy) is NOT tolerated and returns error
	html404Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("<html><title>404 Not Found</title><body>Proxy Error</body></html>"))
	}))
	defer html404Server.Close()
	dispatcher.hangarURL = html404Server.URL
	if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "abc"}); err == nil {
		t.Fatal("expected error on HTML 404 git_push, got nil")
	}
	if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/mirrormere:latest"}); err == nil {
		t.Fatal("expected error on HTML 404 image_ready, got nil")
	}
}

func TestResolveWorkflowRunImages_SkippedStepFiltering(t *testing.T) {
	t.Parallel()
	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{
				ID:         101,
				Name:       "Build & Publish Container Image (mirrormere)",
				Status:     "completed",
				Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Path Filter Check", Status: "completed", Conclusion: "success", Number: 2},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "skipped", Number: 3},
				},
			},
			{
				ID:         102,
				Name:       "Build & Publish Container Image (mirrormere-cast-watcher)",
				Status:     "completed",
				Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Path Filter Check", Status: "completed", Conclusion: "success", Number: 2},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 3},
				},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/actions/runs/999/jobs") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(mockJobs)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, nil, nil, nil)
	images, err := srv.resolveWorkflowRunImages(context.Background(), "azylman/mirrormere", 999)
	if err != nil {
		t.Fatalf("unexpected error resolving workflow run images: %v", err)
	}

	if len(images) != 1 {
		t.Fatalf("expected 1 image (cast-watcher), got %d: %v", len(images), images)
	}
	if images[0] != "ghcr.io/azylman/mirrormere-cast-watcher:latest" {
		t.Errorf("expected cast-watcher image, got %s", images[0])
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	srv := NewRouterServer(Config{}, nil, nil, nil)
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/coverage/flush")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got: %d", resp.StatusCode)
	}
}
