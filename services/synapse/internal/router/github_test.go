package router

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

const testGitHubWebhookSecret = "test-secret"

func signTestGitHubWebhook(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(strings.TrimSpace(secret)))
	mac.Write([]byte(payload))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

type ghWebhookTestCase struct {
	name, path, wantMetricSubstr string
	body                         io.Reader
	headers                      map[string]string
	bus                          *mockEventBus
	hasBus                       bool
	webhookSecret                string
	noWebhookSecret              bool
	wantStatus, wantInserted     int
}

func assertGitHubWebhookResponse(t *testing.T, tc ghWebhookTestCase, rec *httptest.ResponseRecorder, routes http.Handler) {
	t.Helper()
	if rec.Code != tc.wantStatus {
		t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
	}
	if tc.wantInserted > 0 && tc.bus != nil {
		if len(tc.bus.insertedJobs) != tc.wantInserted {
			t.Fatalf("expected %d inserted jobs, got %d", tc.wantInserted, len(tc.bus.insertedJobs))
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse json response: %v", err)
		}
		if resp["status"] != "accepted" || resp["event"] != tc.headers["X-GitHub-Event"] || resp["delivery"] != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected response body: %+v", resp)
		}
		if tc.bus.insertedJobs[0].Event != tc.headers["X-GitHub-Event"] || tc.bus.insertedJobs[0].Delivery != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected inserted jobs: %+v", tc.bus.insertedJobs)
		}
	}
	if tc.wantMetricSubstr != "" {
		recM := doHTTPRequest(routes, http.MethodGet, "/metrics", nil, nil)
		if !strings.Contains(recM.Body.String(), tc.wantMetricSubstr) {
			t.Errorf("expected /metrics to include %q, got:\n%s", tc.wantMetricSubstr, recM.Body.String())
		}
	}
}

func runGitHubWebhookCase(t *testing.T, tc ghWebhookTestCase) {
	t.Helper()
	t.Parallel()
	secret := tc.webhookSecret
	if secret == "" && !tc.noWebhookSecret {
		secret = testGitHubWebhookSecret
	}
	srv := NewRouterServer(fixedConfig(Config{GitHubWebhookSecret: secret}), nil)
	if tc.bus != nil {
		srv.SetEventBus(tc.bus)
	}
	busCh := make(chan string, 1)
	if tc.hasBus {
		memBus := pubsub.NewMemoryBus()
		t.Cleanup(func() { _ = memBus.Close() })
		srv.SetEventBus(memBus)
		srv.onProcessed = func(_, delivery string) {
			select {
			case busCh <- delivery:
			default:
			}
		}
		if err := srv.StartJobConsumers(context.Background()); err != nil {
			t.Fatalf("StartJobConsumers() error: %v", err)
		}
	}
	routes := srv.Routes()
	rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, tc.headers)
	assertGitHubWebhookResponse(t, tc, rec, routes)
	if tc.hasBus && tc.wantStatus == http.StatusOK {
		select {
		case gotDel := <-busCh:
			if gotDel != tc.headers["X-GitHub-Delivery"] {
				t.Errorf("bus consumer delivery = %q, want %q", gotDel, tc.headers["X-GitHub-Delivery"])
			}
		case <-time.After(2 * time.Second):
			t.Errorf("timed out waiting for synapse-github bus consumer")
		}
	}
}

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	pingBody := `{"zen":"Keep it simple"}`
	validPingSig := signTestGitHubWebhook(testGitHubWebhookSecret, pingBody)

	tests := []ghWebhookTestCase{
		{name: "missing X-GitHub-Event header", path: "/api/webhooks/github", body: strings.NewReader(`{}`), wantStatus: http.StatusBadRequest},
		{name: "read error in body", path: "/api/webhooks/github", body: errReader{}, headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusBadRequest},
		{name: "bus nil", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, wantStatus: http.StatusInternalServerError},
		{name: "bus publish error", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{err: errors.New("simulated database failure")}, wantStatus: http.StatusInternalServerError},
		{name: "api path success", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "bus only success", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-bus-1", "X-Hub-Signature-256": validPingSig}, hasBus: true, wantStatus: http.StatusOK},
		{name: "legacy path success", path: "/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "empty secret fails closed without signature", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping"}, noWebhookSecret: true, wantStatus: http.StatusUnauthorized},
		{name: "empty secret fails closed even with signature header", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, noWebhookSecret: true, wantStatus: http.StatusUnauthorized},
		{name: "secret configured missing signature header", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured malformed signature prefix", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha1=1234"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured malformed signature hex", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha256=invalidhex!"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured mismatched hmac digest", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha256=0000000000000000000000000000000000000000000000000000000000000000"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured valid hmac digest", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-sig-1", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusOK, wantInserted: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGitHubWebhookCase(t, tc)
		})
	}
}

func TestIsForeignBotEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilCfg      bool
		cfgBots     map[string]BotConfig
		cfgBotID    string
		meta        AerialPRMetadata
		wantForeign bool
	}{
		{
			name:        "nil cfgSrc returns false",
			nilCfg:      true,
			meta:        AerialPRMetadata{Agent: "aerial"},
			wantForeign: false,
		},
		{
			name:        "empty agent is not foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}},
			meta:        AerialPRMetadata{},
			wantForeign: false,
		},
		{
			name:        "configured bot is not foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}, "agent-alpha": {}},
			meta:        AerialPRMetadata{Agent: "agent-alpha"},
			wantForeign: false,
		},
		{
			name:        "case-insensitive configured bot is not foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}, "agent-alpha": {}},
			meta:        AerialPRMetadata{Agent: "AGENT-ALPHA"},
			wantForeign: false,
		},
		{
			name:        "unconfigured bot is foreign",
			cfgBots:     map[string]BotConfig{"aerial": {}},
			meta:        AerialPRMetadata{Agent: "agent-beta"},
			wantForeign: true,
		},
		{
			name:        "legacy single bot config matching is not foreign",
			cfgBotID:    "aerial",
			meta:        AerialPRMetadata{BotID: "aerial"},
			wantForeign: false,
		},
		{
			name:        "legacy single bot config foreign",
			cfgBotID:    "aerial",
			meta:        AerialPRMetadata{BotID: "agent-beta"},
			wantForeign: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if tc.nilCfg {
				srv = &RouterServer{}
			} else {
				srv = newTestRouter(Config{Bots: tc.cfgBots, BotID: tc.cfgBotID}, nil, nil)
			}
			got := srv.isForeignBotEvent(tc.meta)
			if got != tc.wantForeign {
				t.Errorf("isForeignBotEvent() = %v, want %v", got, tc.wantForeign)
			}
		})
	}
}
