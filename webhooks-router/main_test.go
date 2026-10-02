package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func TestResolveTargetJob_TableDriven(t *testing.T) {
	fallback := "default-job"

	tests := []struct {
		name       string
		secretPath string
		fallback   string
		expected   string
	}{
		{name: "root path", secretPath: "/", fallback: fallback, expected: fallback},
		{name: "empty path", secretPath: "", fallback: fallback, expected: fallback},
		{name: "whitespace path", secretPath: "   ", fallback: fallback, expected: "   "},
		{name: "clean service path", secretPath: "/mirrormere-core", fallback: fallback, expected: "mirrormere-core"},
		{name: "nested service path", secretPath: "/mirrormere-core/production", fallback: fallback, expected: "mirrormere-core"},
		{name: "no leading slash", secretPath: "mirrormere-voice", fallback: fallback, expected: "mirrormere-voice"},
		{name: "multiple slashes", secretPath: "///photos-api/dev///", fallback: fallback, expected: "photos-api"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveTargetJob(tt.secretPath, tt.fallback)
			if got != tt.expected {
				t.Errorf("ResolveTargetJob(%q, %q) = %q, want %q", tt.secretPath, tt.fallback, got, tt.expected)
			}
		})
	}
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
		SecretPath  string `json:"secretPath"`
	}{WorkspaceID: "ws-data"}}
	if got := p3.GetWorkspaceID("fb"); got != "ws-data" {
		t.Errorf("expected ws-data, got %s", got)
	}

	p4 := InfisicalWebhookPayload{Data: &struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
		SecretPath  string `json:"secretPath"`
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
		SecretPath  string `json:"secretPath"`
	}{Environment: "staging"}}
	if got := pEnv2.GetEnvironment("fb"); got != "staging" {
		t.Errorf("expected staging, got %s", got)
	}

	pEnv3 := InfisicalWebhookPayload{}
	if got := pEnv3.GetEnvironment("fb"); got != "fb" {
		t.Errorf("expected fb, got %s", got)
	}

	// 3. GetSecretPath
	pPath1 := InfisicalWebhookPayload{SecretPath: "/service-a"}
	if got := pPath1.GetSecretPath(); got != "/service-a" {
		t.Errorf("expected /service-a, got %s", got)
	}

	pPath2 := InfisicalWebhookPayload{Data: &struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
		SecretPath  string `json:"secretPath"`
	}{SecretPath: "/service-b"}}
	if got := pPath2.GetSecretPath(); got != "/service-b" {
		t.Errorf("expected /service-b, got %s", got)
	}

	pPath3 := InfisicalWebhookPayload{}
	if got := pPath3.GetSecretPath(); got != "/" {
		t.Errorf("expected /, got %s", got)
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
	os.Setenv("DEFAULT_TARGET_JOB", "fallback-job")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("INFISICAL_URL")
		os.Unsetenv("INFISICAL_CLIENT_ID")
		os.Unsetenv("INFISICAL_CLIENT_SECRET")
		os.Unsetenv("INFISICAL_PROJECT_ID")
		os.Unsetenv("INFISICAL_ENVIRONMENT")
		os.Unsetenv("NOMAD_ADDR")
		os.Unsetenv("NOMAD_TOKEN")
		os.Unsetenv("DEFAULT_TARGET_JOB")
	}()

	cfg := LoadConfigFromEnv()
	if cfg.Port != "9999" || cfg.InfisicalURL != "http://infisical:8085" || cfg.NomadAddr != "http://nomad:4646" {
		t.Errorf("unexpected config loaded: %+v", cfg)
	}
	if cfg.InfisicalClientID != "cid" || cfg.InfisicalClientSecret != "csec" || cfg.InfisicalProjectID != "pid" {
		t.Errorf("unexpected infisical credentials: %+v", cfg)
	}
	if cfg.InfisicalEnvironment != "staging" || cfg.DefaultTargetJob != "fallback-job" {
		t.Errorf("unexpected env/job: %+v", cfg)
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
	os.Unsetenv("DEFAULT_TARGET_JOB")
	os.Unsetenv("INFISICAL_ENVIRONMENT")
	cfgDef := LoadConfigFromEnv()
	if cfgDef.Port != "4020" || cfgDef.InfisicalURL != "http://127.0.0.1:8085" || cfgDef.NomadAddr != "http://127.0.0.1:4646" || cfgDef.DefaultTargetJob != "mirrormere-core" {
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

	body := []byte(`{"event":"secrets.modified","secretPath":"/test-service"}`)

	// 1. Valid payload without query param -> 202 (hits default ResolveTargetJob)
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
	if resp["targetJob"] != "test-service" {
		t.Errorf("expected targetJob 'test-service', got %v", resp["targetJob"])
	}

	// 2. Valid payload with explicit ?job= query param
	reqJob := httptest.NewRequest(http.MethodPost, "/webhooks/infisical?job=override-job", strings.NewReader(string(body)))
	recJob := httptest.NewRecorder()
	server.Routes().ServeHTTP(recJob, reqJob)
	if recJob.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, recJob.Code)
	}

	// 3. Malformed JSON -> 400
	badJSON := []byte(`{not valid json}`)
	reqBad := httptest.NewRequest(http.MethodPost, "/webhooks/infisical", strings.NewReader(string(badJSON)))
	recBad := httptest.NewRecorder()
	server.Routes().ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on malformed json, got %d", recBad.Code)
	}

	// 4. Body Read Error -> 400
	reqErr := httptest.NewRequest(http.MethodPost, "/webhooks/infisical", errReader{})
	recErr := httptest.NewRecorder()
	server.Routes().ServeHTTP(recErr, reqErr)
	if recErr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on read error, got %d", recErr.Code)
	}
}

func TestHandleGenericWebhook(t *testing.T) {
	server := NewRouterServer(Config{DefaultTargetJob: "default-app"}, nil)

	// 1. Path-based sync -> 202
	body := []byte(`{"job":"custom-job","path":"/custom-path"}`)
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
	if resp["targetJob"] != "custom-job" {
		t.Errorf("expected targetJob 'custom-job', got %v", resp["targetJob"])
	}

	// 2. Direct secrets map payload -> 202
	directBody := []byte(`{"targetJob":"direct-job","secrets":{"KEY1":"VAL1"}}`)
	reqDirect := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(directBody)))
	recDirect := httptest.NewRecorder()
	server.Routes().ServeHTTP(recDirect, reqDirect)
	if recDirect.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", recDirect.Code)
	}
	var respDirect map[string]interface{}
	_ = json.Unmarshal(recDirect.Body.Bytes(), &respDirect)
	if respDirect["targetJob"] != "direct-job" || respDirect["message"] != "Direct secret sync scheduled" {
		t.Errorf("unexpected direct payload handling: %+v", respDirect)
	}

	// 3. Query Param overrides & Empty Payload
	reqQuery := httptest.NewRequest(http.MethodPost, "/webhooks/generic?job=q-job&path=/q-path", strings.NewReader(`{}`))
	recQuery := httptest.NewRecorder()
	server.Routes().ServeHTTP(recQuery, reqQuery)
	if recQuery.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", recQuery.Code)
	}
	var respQ map[string]interface{}
	_ = json.Unmarshal(recQuery.Body.Bytes(), &respQ)
	if respQ["targetJob"] != "q-job" || respQ["secretPath"] != "/q-path" {
		t.Errorf("unexpected query param handling: %+v", respQ)
	}

	// 4. Payload with explicit workspaceId and environment
	explicitBody := []byte(`{"workspaceId":"custom-ws","environment":"dev","targetJob":"custom-app"}`)
	reqExp := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(explicitBody)))
	recExp := httptest.NewRecorder()
	server.Routes().ServeHTTP(recExp, reqExp)
	if recExp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", recExp.Code)
	}
	var respExp map[string]interface{}
	_ = json.Unmarshal(recExp.Body.Bytes(), &respExp)
	if respExp["targetJob"] != "custom-app" || respExp["environment"] != "dev" {
		t.Errorf("unexpected explicit payload handling: %+v", respExp)
	}

	// 5. Bad JSON -> 400
	badJSON := []byte(`{invalid`)
	reqB := httptest.NewRequest(http.MethodPost, "/webhooks/generic", strings.NewReader(string(badJSON)))
	recB := httptest.NewRecorder()
	server.Routes().ServeHTTP(recB, reqB)
	if recB.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on bad json, got %d", recB.Code)
	}

	// 6. Body Read Error -> 400
	reqErr := httptest.NewRequest(http.MethodPost, "/webhooks/generic", errReader{})
	recErr := httptest.NewRecorder()
	server.Routes().ServeHTTP(recErr, reqErr)
	if recErr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 on read error, got %d", recErr.Code)
	}
}

func TestSyncSecrets_IdempotencyAndFlow(t *testing.T) {
	var nomadPutCount atomic.Int32
	var nomadGetCount atomic.Int32
	var infisicalLoginCount atomic.Int32
	var infisicalFetchCount atomic.Int32

	existingSecrets := map[string]string{
		"API_KEY": "secret123",
	}

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

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/var/"):
			nomadGetCount.Add(1)
			if r.URL.Path == "/v1/var/nomad/jobs/existing-job" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(NomadVariable{
					Path:  "nomad/jobs/existing-job",
					Items: existingSecrets,
				})
			} else {
				w.WriteHeader(http.StatusNotFound)
			}

		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/var/"):
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
		DefaultTargetJob:      "test-job",
	}
	server := NewRouterServer(cfg, mockServer.Client())

	ctx := context.Background()

	// 1. Initial sync on new job -> Issues PUT
	err := server.SyncSecrets(ctx, "proj-123", "prod", "/new-job", "new-job")
	if err != nil {
		t.Fatalf("SyncSecrets failed: %v", err)
	}
	if nomadPutCount.Load() != 1 {
		t.Errorf("expected 1 Nomad PUT, got %d", nomadPutCount.Load())
	}

	// 2. Idempotent sync on existing job with same secrets -> Skips PUT
	nomadPutCount.Store(0)
	err = server.SyncSecrets(ctx, "proj-123", "prod", "/existing-job", "existing-job")
	if err != nil {
		t.Fatalf("SyncSecrets failed: %v", err)
	}
	if nomadPutCount.Load() != 0 {
		t.Errorf("expected 0 Nomad PUTs due to semantic idempotency check, got %d", nomadPutCount.Load())
	}

	// 3. Error branch: Missing Infisical Credentials
	serverBad := NewRouterServer(Config{}, mockServer.Client())
	if err := serverBad.SyncSecrets(ctx, "", "", "", ""); err == nil {
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
	if err := s500.SyncSecrets(context.Background(), "", "", "", ""); err == nil {
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
	if err := sFetch.SyncSecrets(context.Background(), "", "", "", ""); err == nil {
		t.Errorf("expected error on infisical fetch 502")
	}

	// 6. Infisical Fetch returns invalid JSON
	mockServerFetchJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-secrets"))
	}))
	defer mockServerFetchJSON.Close()
	sFetchJSON := NewRouterServer(Config{InfisicalURL: mockServerFetchJSON.URL}, mockServerFetchJSON.Client())
	if _, err := sFetchJSON.fetchInfisicalSecrets(context.Background(), "tok", "ws", "env", "no-slash"); err == nil {
		t.Errorf("expected error on invalid secrets json")
	}

	// 7. Nomad GET returns 500
	mockServerNomadErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/universal-auth/login":
			_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "tok"})
		case "/api/v3/secrets/raw":
			_ = json.NewEncoder(w).Encode(InfisicalSecretsResponse{})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer mockServerNomadErr.Close()

	cfgNomad := Config{
		InfisicalURL:          mockServerNomadErr.URL,
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
		NomadAddr:             mockServerNomadErr.URL,
	}
	sNomad := NewRouterServer(cfgNomad, mockServerNomadErr.Client())
	if err := sNomad.SyncSecrets(context.Background(), "", "", "", ""); err == nil {
		t.Errorf("expected error on nomad get 500")
	}

	// 8. Nomad GET returns invalid JSON
	mockServerNomadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-nomad-var"))
	}))
	defer mockServerNomadJSON.Close()
	sNomadJSON := NewRouterServer(Config{NomadAddr: mockServerNomadJSON.URL}, mockServerNomadJSON.Client())
	if _, _, err := sNomadJSON.getNomadVariable(context.Background(), "job"); err == nil {
		t.Errorf("expected error on invalid nomad var json")
	}

	// 9. Nomad GET returns nil items -> initializes empty map
	mockServerNomadNilItems := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Path":"nomad/jobs/nil-job"}`))
	}))
	defer mockServerNomadNilItems.Close()
	sNomadNil := NewRouterServer(Config{NomadAddr: mockServerNomadNilItems.URL}, mockServerNomadNilItems.Client())
	items, exists, err := sNomadNil.getNomadVariable(context.Background(), "nil-job")
	if err != nil || !exists || items == nil {
		t.Errorf("expected initialized map on nil items, got %+v, exists=%v, err=%v", items, exists, err)
	}

	// 10. Nomad PUT returns 201 Created and 204 No Content
	mockServerNomad201 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer mockServerNomad201.Close()
	sNomad201 := NewRouterServer(Config{NomadAddr: mockServerNomad201.URL}, mockServerNomad201.Client())
	if err := sNomad201.putNomadVariable(context.Background(), "job", map[string]string{"A": "B"}); err != nil {
		t.Errorf("expected success on 201 Created: %v", err)
	}

	mockServerNomad204 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer mockServerNomad204.Close()
	sNomad204 := NewRouterServer(Config{NomadAddr: mockServerNomad204.URL}, mockServerNomad204.Client())
	if err := sNomad204.putNomadVariable(context.Background(), "job", map[string]string{"A": "B"}); err != nil {
		t.Errorf("expected success on 204 No Content: %v", err)
	}

	// 11. Nomad PUT returns 500
	mockServerPutErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/universal-auth/login":
			_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "tok"})
		case "/api/v3/secrets/raw":
			_ = json.NewEncoder(w).Encode(InfisicalSecretsResponse{
				Secrets: []InfisicalSecretItem{{SecretKey: "K", SecretValue: "V"}},
			})
		default:
			if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
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
	if err := sPut.SyncSecrets(context.Background(), "", "", "", ""); err == nil {
		t.Errorf("expected error on nomad put 500")
	}

	// 12. HTTP Client connection errors (invalid / unreachable URL)
	badClientServer := NewRouterServer(Config{
		InfisicalURL:          "http://127.0.0.1:1",
		InfisicalClientID:     "cid",
		InfisicalClientSecret: "csec",
		NomadAddr:             "http://127.0.0.1:1",
	}, &http.Client{Timeout: 50 * time.Millisecond})

	if _, err := badClientServer.getInfisicalToken(context.Background()); err == nil {
		t.Errorf("expected error on bad login client URL")
	}
	if _, err := badClientServer.fetchInfisicalSecrets(context.Background(), "tok", "ws", "env", "/path"); err == nil {
		t.Errorf("expected error on bad fetch client URL")
	}
	if _, _, err := badClientServer.getNomadVariable(context.Background(), "job"); err == nil {
		t.Errorf("expected error on bad nomad get client URL")
	}
	if err := badClientServer.putNomadVariable(context.Background(), "job", map[string]string{"A": "B"}); err == nil {
		t.Errorf("expected error on bad nomad put client URL")
	}
}

func TestRunServer_LifecycleAndErrors(t *testing.T) {
	// 1. Clean lifecycle
	cfg := Config{
		Port:             "0", // Dynamic available port
		DefaultTargetJob: "test",
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
}
