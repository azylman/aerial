package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
		name, path, wantTargetVar, wantEnv, wantMsg string
		body                                        io.Reader
		wantStatus                                  int
	}{
		{name: "infisical valid payload", path: "/webhooks/infisical", body: strings.NewReader(`{"event":"secrets.modified"}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable},
		{name: "infisical malformed json", path: "/webhooks/infisical", body: strings.NewReader(`{not valid json}`), wantStatus: http.StatusBadRequest},
		{name: "infisical body read error", path: "/webhooks/infisical", body: errReader{}, wantStatus: http.StatusBadRequest},
		{name: "generic standard sync", path: "/webhooks/generic", body: strings.NewReader(`{"environment":"prod"}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable},
		{name: "generic empty body allowed", path: "/webhooks/generic", body: strings.NewReader(""), wantStatus: http.StatusAccepted},
		{name: "generic direct secrets map", path: "/webhooks/generic", body: strings.NewReader(`{"secrets":{"KEY1":"VAL1"}}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable, wantMsg: "Direct secret sync scheduled"},
		{name: "generic explicit workspace and env", path: "/webhooks/generic", body: strings.NewReader(`{"workspaceId":"custom-ws","environment":"dev"}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable, wantEnv: "dev"},
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
			if tc.wantTargetVar != "" || tc.wantEnv != "" || tc.wantMsg != "" {
				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse response: %v", err)
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
		nomadStatus   int
		unreachable   bool
		wantErr       bool
		wantNomadPuts int32
	}{
		{name: "successful full sync flow", loginStatus: http.StatusOK, loginBody: `{"accessToken":"mock-jwt-token"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"API_KEY","secretValue":"secret123"}]}`, nomadStatus: http.StatusOK, wantNomadPuts: 1},
		{name: "missing infisical credentials", cfgOverride: &Config{}, wantErr: true},
		{name: "login 500 server error", loginStatus: http.StatusInternalServerError, loginBody: "server error", wantErr: true},
		{name: "login invalid json response", loginStatus: http.StatusOK, loginBody: "{invalid-json", wantErr: true},
		{name: "login empty token string", loginStatus: http.StatusOK, loginBody: `{"accessToken":""}`, wantErr: true},
		{name: "login fallback token field", loginStatus: http.StatusOK, loginBody: `{"token":"fallback-token"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, nomadStatus: http.StatusOK, wantNomadPuts: 1},
		{name: "secrets fetch 502 bad gateway", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusBadGateway, wantErr: true},
		{name: "secrets fetch invalid json", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: "{invalid-secrets", wantErr: true},
		{name: "nomad put 201 created", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, nomadStatus: http.StatusCreated, wantNomadPuts: 1},
		{name: "nomad put 204 no content", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, nomadStatus: http.StatusNoContent, wantNomadPuts: 1},
		{name: "nomad put 500 internal server error", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, nomadStatus: http.StatusInternalServerError, wantErr: true, wantNomadPuts: 1},
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
					NomadAddr:             "http://127.0.0.1:1",
				}, &http.Client{Timeout: 50 * time.Millisecond})

				if err := badClientServer.SyncSecrets(ctx, "ws", "env"); err == nil {
					t.Errorf("SyncSecrets() expected error on unreachable, got nil")
				}
				if _, err := badClientServer.fetchInfisicalSecrets(ctx, "tok", "ws", "env"); err == nil {
					t.Errorf("fetchInfisicalSecrets() expected error on unreachable, got nil")
				}
				if err := badClientServer.putNomadVariable(ctx, map[string]string{"A": "B"}); err == nil {
					t.Errorf("putNomadVariable() expected error on unreachable, got nil")
				}
				return
			}

			var nomadPutCount atomic.Int32
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/universal-auth/login":
					w.WriteHeader(tc.loginStatus)
					_, _ = w.Write([]byte(tc.loginBody))
				case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw":
					w.WriteHeader(tc.secretsStatus)
					_, _ = w.Write([]byte(tc.secretsBody))
				case r.Method == http.MethodPut && r.URL.Path == "/v1/var/"+TargetNomadVariable:
					nomadPutCount.Add(1)
					w.WriteHeader(tc.nomadStatus)
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
			if tc.cfgOverride != nil {
				cfg = *tc.cfgOverride
			}

			srv := NewRouterServer(cfg, mockServer.Client())
			err := srv.SyncSecrets(ctx, "proj-123", "prod")
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncSecrets() err = %v, wantErr %v", err, tc.wantErr)
			}
			if gotPuts := nomadPutCount.Load(); gotPuts != tc.wantNomadPuts {
				t.Errorf("nomad PUT count = %d, want %d", gotPuts, tc.wantNomadPuts)
			}
		})
	}
}
