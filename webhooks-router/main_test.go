package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

type mockRiverInserter struct {
	insertedJobs []GitHubWebhookArgs
	err          error
}

func (m *mockRiverInserter) Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	if ghArgs, ok := args.(GitHubWebhookArgs); ok {
		m.insertedJobs = append(m.insertedJobs, ghArgs)
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

func TestRiverLiveIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live River integration test in short mode")
	}

	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = os.Getenv("DATABASE_URL")
	}
	if pgURL == "" {
		if _, err := net.LookupHost("aerial-postgres"); err == nil {
			pgURL = "postgres://aerial:aerial_secure_pass@aerial-postgres:5432/aerial?sslmode=disable"
		} else if _, err := net.LookupHost("postgres"); err == nil {
			pgURL = "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
		} else {
			pgURL = "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
		}
	}

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
	ciFailedTargetID    string
	ciFailedUpdated     bool
	ciFailedErr         error
	ciFailedBySHAPRNum  int
	ciFailedBySHATarget string
	ciFailedBySHAUpdated bool
	ciFailedBySHAErr    error
	err                 error
}

func (m *mockPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) error {
	if m.err != nil {
		return m.err
	}
	m.mergedCalls = append(m.mergedCalls, struct{ repo string; prNumber int; mergeSHA string }{repo, prNumber, mergeSHA})
	return nil
}

func (m *mockPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) error {
	if m.err != nil {
		return m.err
	}
	m.syncCalls = append(m.syncCalls, struct{ repo string; prNumber int; headSHA string }{repo, prNumber, headSHA})
	return nil
}

func (m *mockPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) error {
	if m.err != nil {
		return m.err
	}
	m.closedUnmergedCalls = append(m.closedUnmergedCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return nil
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

func (m *mockPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) error {
	if m.err != nil {
		return m.err
	}
	m.backfillCalls = append(m.backfillCalls, struct{ repo string; prNumber int; mergeSHA string }{repo, prNumber, mergeSHA})
	return nil
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
		ciFailedTargetID:    "target-111",
		ciFailedUpdated:     true,
		ciFailedBySHAPRNum:  520,
		ciFailedBySHATarget: "target-222",
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
	_, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged", []byte(payloadPRMerged))
	if err != nil {
		t.Fatalf("pull_request merged failed: %v", err)
	}
	if len(mockReg.mergedCalls) != 1 || mockReg.mergedCalls[0].prNumber != 515 || mockReg.mergedCalls[0].mergeSHA != "merge111" {
		t.Errorf("unexpected mergedCalls: %+v", mockReg.mergedCalls)
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
	_, err = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-closed", []byte(payloadPRClosed))
	if err != nil {
		t.Fatalf("pull_request closed unmerged failed: %v", err)
	}
	if len(mockReg.closedUnmergedCalls) != 1 || mockReg.closedUnmergedCalls[0].prNumber != 516 {
		t.Errorf("unexpected closedUnmergedCalls: %+v", mockReg.closedUnmergedCalls)
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
	_, err = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-sync", []byte(payloadPRSync))
	if err != nil {
		t.Fatalf("pull_request synchronize failed: %v", err)
	}
	if len(mockReg.syncCalls) != 1 || mockReg.syncCalls[0].prNumber != 517 || mockReg.syncCalls[0].headSHA != "newhead333" {
		t.Errorf("unexpected syncCalls: %+v", mockReg.syncCalls)
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
}

func TestPostgresPRRegistry_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live PostgresPRRegistry integration test in short mode")
	}

	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = os.Getenv("DATABASE_URL")
	}
	if pgURL == "" {
		if _, err := net.LookupHost("aerial-postgres"); err == nil {
			pgURL = "postgres://aerial:aerial_secure_pass@aerial-postgres:5432/aerial?sslmode=disable"
		} else if _, err := net.LookupHost("postgres"); err == nil {
			pgURL = "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
		} else {
			pgURL = "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
		}
	}

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
	if err := reg.UpdatePRSync(ctx, repo, testPRNum, "syncsha222"); err != nil {
		t.Fatalf("UpdatePRSync failed: %v", err)
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
	if err := reg.UpdatePRSync(ctx, repo, testPRNum, "newhead333"); err != nil {
		t.Fatalf("UpdatePRSync reset failed: %v", err)
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
	if err := reg.UpdatePRMerged(ctx, repo, testPRNum, "mergesha444"); err != nil {
		t.Fatalf("UpdatePRMerged failed: %v", err)
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
	if err := reg.BackfillPushMergeSHA(ctx, repo, testPRNum, "mergesha555"); err != nil {
		t.Fatalf("BackfillPushMergeSHA failed: %v", err)
	}
	err = pool.QueryRow(ctx, "SELECT merge_sha FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&currentMerge)
	if err != nil || currentMerge != "mergesha555" {
		t.Errorf("after BackfillPushMergeSHA: got merge=%s; expected mergesha555", currentMerge)
	}

	// 9. Untracked PR succeeds silently
	if err := reg.UpdatePRMerged(ctx, repo, 999999, "sha"); err != nil {
		t.Errorf("expected nil error on untracked PR, got %v", err)
	}
	if err := reg.UpdatePRClosedUnmerged(ctx, repo, 999999); err != nil {
		t.Errorf("expected nil error on untracked PR closed, got %v", err)
	}
	if err := reg.UpdatePRSync(ctx, repo, 999999, "sha"); err != nil {
		t.Errorf("expected nil error on untracked PR sync, got %v", err)
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
}
