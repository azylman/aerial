package router

import (
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauthtest"
)

type handlePRRegisterK8sAuthCase struct {
	name       string
	token      string
	auth       *k8sauthtest.RoundTripper
	cfgBots    map[string]BotConfig
	body       string
	wantStatus int
	wantAgent  string
}

func handlePRRegisterK8sAuthCases() []handlePRRegisterK8sAuthCase {
	return []handlePRRegisterK8sAuthCase{
		{
			name:       "missing bearer token returns 401",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid token returns 401",
			token:      "invalid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReviewDenied()},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "mismatched bot_id without prs/all returns 403",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-aerial"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "aerial"): true}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial","bot_id":"agent-alpha"}`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "mismatched bot_id with prs/all succeeds",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-aerial"), SARRules: map[string]bool{k8sauthtest.SARKey("prs/all", "create", ""): true, k8sauthtest.SARKey("prs", "create", "aerial"): true}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial","bot_id":"agent-alpha"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "aerial",
		},
		{
			name:       "caller forbidden to create prs returns 403",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "agent-alpha"): true}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-beta"}`,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "caller allowed to create prs for target succeeds",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "agent-alpha"): true}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-alpha"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
		{
			name:       "caller with empty agent defaults to caller agent and succeeds",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "agent-alpha"): true}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
		{
			name:       "service caller with valid target agent and prs/all succeeds",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:ci-runner"), SARRules: map[string]bool{k8sauthtest.SARKey("prs/all", "create", "agent-alpha"): true}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"agent-alpha"}`,
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
		{
			name:       "service caller with missing target agent returns 400",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:ci-runner")},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "caller with unknown target agent when bots configured returns 400",
			token:      "valid-token",
			auth:       &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-aerial")},
			cfgBots:    map[string]BotConfig{"aerial": {}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"unknown-agent"}`,
			wantStatus: http.StatusBadRequest,
		},
	}
}

func TestHandlePRRegister_K8sAuth(t *testing.T) {
	t.Parallel()

	for _, tc := range handlePRRegisterK8sAuthCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := tc.auth
			if auth == nil {
				auth = &k8sauthtest.RoundTripper{}
			}
			authCfg := k8sauthtest.Config(t, auth)
			ingress := &ownedIngress{callCode: http.StatusOK, callBody: `{"target_id":"100000000000000001"}`}
			srv := newTestRouter(Config{
				RestateURL:      ingress.serve(t),
				SystemChannelID: "100000000000000001",
				Bots:            tc.cfgBots,
				K8sAuthEnabled:  true,
				K8sAuthConfig:   authCfg,
			}, nil, &mockOutboundDispatcher{})

			headers := make(map[string]string)
			if tc.token != "" {
				headers["Authorization"] = "Bearer " + tc.token
			}
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), headers)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantAgent != "" {
				if !strings.Contains(rec.Body.String(), `"agent":"`+tc.wantAgent+`"`) {
					t.Errorf("body = %s, want agent %q", rec.Body.String(), tc.wantAgent)
				}
			}
		})
	}
}

func TestIsKnownBot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilCfg   bool
		cfgBotID string
		cfgBots  map[string]BotConfig
		botID    string
		want     bool
	}{
		{name: "nil cfgSrc returns false", nilCfg: true, botID: "aerial", want: false},
		{name: "empty botID returns false", botID: "", want: false},
		{name: "no bots configured returns true for any botID", botID: "anyone", want: true},
		{name: "matching cfg.BotID returns true", cfgBotID: "aerial", botID: "aerial", want: true},
		{name: "mismatched cfg.BotID returns false", cfgBotID: "aerial", botID: "agent-beta", want: false},
		{name: "matching cfg.Bots returns true", cfgBots: map[string]BotConfig{"agent-beta": {}}, botID: "agent-beta", want: true},
		{name: "case-insensitive matching cfg.Bots returns true", cfgBots: map[string]BotConfig{"agent-beta": {}}, botID: "AGENT-BETA", want: true},
		{name: "unknown bot in cfg.Bots returns false", cfgBots: map[string]BotConfig{"agent-beta": {}}, botID: "other", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if tc.nilCfg {
				srv = &RouterServer{}
			} else {
				srv = newTestRouter(Config{BotID: tc.cfgBotID, Bots: tc.cfgBots}, nil, nil)
			}
			got := srv.isKnownBot(tc.botID)
			if got != tc.want {
				t.Errorf("isKnownBot(%q) = %v, want %v", tc.botID, got, tc.want)
			}
		})
	}
}
