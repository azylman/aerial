package router

import (
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauthtest"
)

// prRegisterCaller fakes the k8s API for one service-account caller that
// holds the given SARKey grants.
func prRegisterCaller(serviceAccount string, grants ...string) *k8sauthtest.RoundTripper {
	rules := make(map[string]bool, len(grants))
	for _, g := range grants {
		rules[g] = true
	}
	return &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:" + serviceAccount), SARRules: rules}
}

// prRegisterGrant is the SARKey for creating PRs on resource for agent.
func prRegisterGrant(resource, agent string) string {
	return k8sauthtest.SARKey(resource, "create", agent)
}

// prRegisterBody is a register request for acme/aerial#42 with extra JSON
// fields appended.
func prRegisterBody(extra string) string {
	return `{"repo":"acme/aerial","pr_number":42,"branch":"feat","head_sha":"sha1","target_id":"100000000000000001"` + extra + `}`
}

// TestHandlePRRegister pins the register route: local validation, the full
// k8s caller/agent auth matrix, PRObject.Register's returned target and its
// failures mapped to HTTP statuses.
func TestHandlePRRegister(t *testing.T) {
	t.Parallel()
	const (
		okBody    = `{"target_id":"100000000000000001","agent":"aerial","status":"open"}`
		regPath   = "/PRObject/acme%2Faerial%2342/Register"
		sysTarget = "100000000000000009"
	)
	alpha := prRegisterGrant("prs", "agent-alpha")
	tests := []struct {
		name        string
		auth        *k8sauthtest.RoundTripper // non-nil enables k8s auth
		token       string
		noURL       bool
		owner       string
		bots        map[string]BotConfig
		body        string
		callCode    int
		callBody    string
		wantStatus  int
		wantRespSub string
		wantCallSub string
		wantCalls   int
	}{
		{
			name: "bare repo is qualified with the configured owner", owner: "acme", body: `{"repo":"aerial","pr_number":42,"branch":"feat","head_sha":"sha1","target_id":"100000000000000001"}`,
			callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantCallSub: `"repo":"acme/aerial"`, wantCalls: 1,
		},
		{name: "bare repo without an owner never reaches restate", body: `{"repo":"aerial","pr_number":42,"branch":"feat","head_sha":"sha1","target_id":"100000000000000001"}`, wantStatus: http.StatusInternalServerError},
		{
			name: "valid register returns the restate target", body: prRegisterBody(""), callCode: 200, callBody: okBody,
			wantStatus: http.StatusOK, wantRespSub: `"target_id":"100000000000000001"`, wantCallSub: `"fallback_target_id":"` + sysTarget + `"`, wantCalls: 1,
		},
		{
			name: "headless target travels empty for restate to resolve", body: `{"repo":"acme/aerial","pr_number":42,"branch":"feat","head_sha":"sha1","target_id":"dispatch-uuid-1"}`,
			callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantCallSub: `"target_id":""`, wantCalls: 1,
		},
		{
			name: "agent metadata is forwarded", body: prRegisterBody(`,"metadata":"{\"widget_state\":\"w1\"}"`), callCode: 200, callBody: okBody,
			wantStatus: http.StatusOK, wantCallSub: `"widget_state":"w1"`, wantCalls: 1,
		},
		{name: "explicit bad target is rejected locally", body: `{"repo":"acme/aerial","pr_number":42,"branch":"feat","head_sha":"sha1","target_id":"bad"}`, wantStatus: http.StatusBadRequest},
		{name: "empty repo is rejected locally", body: `{"repo":"","pr_number":42,"branch":"feat","head_sha":"sha1"}`, wantStatus: http.StatusBadRequest},
		{name: "zero pr is rejected locally", body: `{"repo":"acme/aerial","pr_number":0,"branch":"feat","head_sha":"sha1"}`, wantStatus: http.StatusBadRequest},
		{name: "empty branch is rejected locally", body: `{"repo":"acme/aerial","pr_number":42,"branch":"","head_sha":"sha1"}`, wantStatus: http.StatusBadRequest},
		{name: "empty head is rejected locally", body: `{"repo":"acme/aerial","pr_number":42,"branch":"feat","head_sha":""}`, wantStatus: http.StatusBadRequest},
		{name: "malformed json is rejected", body: `{`, wantStatus: http.StatusBadRequest},
		{name: "unknown bot is a conflict", bots: map[string]BotConfig{"aerial": {}}, body: prRegisterBody(`,"agent":"nobody"`), wantStatus: http.StatusConflict},
		{name: "restate owner mismatch is a conflict", body: prRegisterBody(""), callCode: 409, callBody: "owner", wantStatus: http.StatusConflict, wantCalls: 1},
		{name: "restate bad target is a bad request", body: prRegisterBody(""), callCode: 400, callBody: "target", wantStatus: http.StatusBadRequest, wantCalls: 1},
		{name: "restate outage is a server error", body: prRegisterBody(""), callCode: 500, callBody: "boom", wantStatus: http.StatusInternalServerError, wantCalls: 1},
		{name: "no restate url is a server error", noURL: true, body: prRegisterBody(""), wantStatus: http.StatusInternalServerError},
		{name: "k8s caller without a token is unauthorized", auth: &k8sauthtest.RoundTripper{}, body: prRegisterBody(""), wantStatus: http.StatusUnauthorized},
		{
			name: "k8s denied token is unauthorized", auth: &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReviewDenied()}, token: "invalid-token", body: prRegisterBody(""),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "k8s caller fences the owner inside restate", auth: prRegisterCaller("eri-aerial", prRegisterGrant("prs", "aerial")), token: "valid-token", body: prRegisterBody(""),
			callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantRespSub: `"agent":"aerial"`, wantCallSub: `"enforce_owner":true,"expected_owner":"aerial"`, wantCalls: 1,
		},
		{
			name: "k8s mismatched bot_id without prs/all is forbidden", auth: prRegisterCaller("eri-aerial", prRegisterGrant("prs", "aerial")), token: "valid-token",
			body: prRegisterBody(`,"agent":"aerial","bot_id":"agent-alpha"`), wantStatus: http.StatusForbidden,
		},
		{
			name: "k8s mismatched bot_id with prs/all succeeds", auth: prRegisterCaller("eri-aerial", prRegisterGrant("prs/all", ""), prRegisterGrant("prs", "aerial")), token: "valid-token",
			body: prRegisterBody(`,"agent":"aerial","bot_id":"agent-alpha"`), callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantRespSub: `"agent":"aerial"`, wantCalls: 1,
		},
		{
			name: "k8s caller forbidden for another agent", auth: prRegisterCaller("eri-agent-alpha", alpha), token: "valid-token", body: prRegisterBody(`,"agent":"agent-beta"`),
			wantStatus: http.StatusForbidden,
		},
		{
			name: "k8s caller allowed for its own agent", auth: prRegisterCaller("eri-agent-alpha", alpha), token: "valid-token", body: prRegisterBody(`,"agent":"agent-alpha"`),
			callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantRespSub: `"agent":"agent-alpha"`, wantCallSub: `"expected_owner":"agent-alpha"`, wantCalls: 1,
		},
		{
			name: "k8s caller with empty agent defaults to itself", auth: prRegisterCaller("eri-agent-alpha", alpha), token: "valid-token", body: prRegisterBody(""),
			callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantRespSub: `"agent":"agent-alpha"`, wantCalls: 1,
		},
		{
			name: "k8s service caller with prs/all for the target succeeds", auth: prRegisterCaller("ci-runner", prRegisterGrant("prs/all", "agent-alpha")), token: "valid-token",
			body: prRegisterBody(`,"agent":"agent-alpha"`), callCode: 200, callBody: okBody, wantStatus: http.StatusOK, wantRespSub: `"agent":"agent-alpha"`, wantCalls: 1,
		},
		{name: "k8s service caller without a target agent is a bad request", auth: prRegisterCaller("ci-runner"), token: "valid-token", body: prRegisterBody(""), wantStatus: http.StatusBadRequest},
		{
			name: "k8s caller with an unknown target agent is a bad request", auth: prRegisterCaller("eri-aerial"), token: "valid-token", bots: map[string]BotConfig{"aerial": {}},
			body: prRegisterBody(`,"agent":"unknown-agent"`), wantStatus: http.StatusBadRequest,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ownedIngress{sendCode: http.StatusAccepted, callCode: tc.callCode, callBody: tc.callBody}
			cfg := Config{SystemChannelID: sysTarget, GitHubOwner: tc.owner, Bots: tc.bots}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			if tc.auth != nil {
				cfg.K8sAuthEnabled, cfg.K8sAuthConfig = true, k8sauthtest.Config(t, tc.auth)
			}
			srv := newTestRouter(cfg, nil, &mockOutboundDispatcher{})
			headers := map[string]string{}
			if tc.token != "" {
				headers["Authorization"] = "Bearer " + tc.token
			}
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), headers)
			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantRespSub) {
				t.Fatalf("response = %d %s, want %d containing %s", rec.Code, rec.Body.String(), tc.wantStatus, tc.wantRespSub)
			}
			calls := ingress.calls()
			if len(calls) != tc.wantCalls {
				t.Fatalf("register calls = %v, want %d", calls, tc.wantCalls)
			}
			if tc.wantCalls > 0 && (calls[0] != regPath || !strings.Contains(ingress.callBodies()[0], tc.wantCallSub)) {
				t.Errorf("register call = %s %s, want %s containing %s", calls[0], ingress.callBodies()[0], regPath, tc.wantCallSub)
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
