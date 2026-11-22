package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauthtest"
)

// readinessBody is a retry-readiness request for acme/aerial#42 with extra
// JSON fields appended.
func readinessBody(extra string) string {
	return `{"repo":"acme/aerial","pr_number":42,"expected_merge_sha":"m1"` + extra + `}`
}

// TestHandleRetryPRReadiness pins the readiness retry route: local
// validation, the agent hint, PRObject.RetryReadiness status mapping and the
// k8s auth wiring (the full auth matrix lives in TestHandlePRRegister).
func TestHandleRetryPRReadiness(t *testing.T) {
	t.Parallel()
	const callPath = "/PRObject/acme%2Faerial%2342/RetryReadiness"
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
			name: "accepted gate is pending", body: readinessBody(""), callCode: 200, callBody: `{"status":202,"kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"t1"}}}`,
			wantStatus: http.StatusAccepted, wantRespSub: `"readiness_pending"`, wantCallSub: `{"merge_sha":"m1"}`, wantCalls: 1,
		},
		{
			name: "bare repo is qualified with the configured owner", owner: "acme", body: `{"repo":"aerial","pr_number":42,"expected_merge_sha":"m1"}`, callCode: 200, callBody: `{"status":202}`,
			wantStatus: http.StatusAccepted, wantCallSub: `{"merge_sha":"m1"}`, wantCalls: 1,
		},
		{name: "bare repo without an owner never reaches restate", body: `{"repo":"aerial","pr_number":42,"expected_merge_sha":"m1"}`, wantStatus: http.StatusServiceUnavailable, wantRespSub: "github_owner is unset"},
		{name: "no matching gate is a conflict", body: readinessBody(""), callCode: 200, callBody: `{"status":409}`, wantStatus: http.StatusConflict, wantRespSub: "no matching merged rollout gate", wantCalls: 1},
		{name: "restate owner fence is a conflict", body: readinessBody(""), callCode: 409, callBody: "owner", wantStatus: http.StatusConflict, wantCalls: 1},
		{name: "restate outage is unavailable", body: readinessBody(""), callCode: 500, callBody: "boom", wantStatus: http.StatusServiceUnavailable, wantCalls: 1},
		{name: "no restate url is unavailable", noURL: true, body: readinessBody(""), wantStatus: http.StatusServiceUnavailable, wantRespSub: "readiness retry unavailable"},
		{name: "missing merge sha is rejected locally", body: `{"repo":"acme/aerial","pr_number":42}`, wantStatus: http.StatusBadRequest},
		{name: "invalid pr number is rejected locally", body: `{"repo":"acme/aerial","pr_number":0,"expected_merge_sha":"m1"}`, wantStatus: http.StatusBadRequest},
		{name: "malformed json is rejected locally", body: `{`, wantStatus: http.StatusBadRequest},
		{name: "unknown agent is a conflict", bots: map[string]BotConfig{"aerial": {}}, body: readinessBody(`,"agent":"unknown-bot"`), wantStatus: http.StatusConflict},
		{
			name: "known agent hint is forwarded", bots: map[string]BotConfig{"aerial": {}}, body: readinessBody(`,"agent":"aerial"`), callCode: 200, callBody: `{"status":202}`,
			wantStatus: http.StatusAccepted, wantCallSub: `{"merge_sha":"m1","agent":"aerial"}`, wantCalls: 1,
		},
		{name: "k8s caller without a token is unauthorized", auth: &k8sauthtest.RoundTripper{}, body: readinessBody(""), wantStatus: http.StatusUnauthorized},
		{
			name: "k8s caller fences the owner inside restate", auth: prRegisterCaller("eri-aerial", prRegisterGrant("prs", "aerial")), token: "valid-token", body: readinessBody(""),
			callCode: 200, callBody: `{"status":202}`, wantStatus: http.StatusAccepted, wantCallSub: `"agent":"aerial","enforce_owner":true,"expected_owner":"aerial"`, wantCalls: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ownedIngress{sendCode: http.StatusAccepted, callCode: tc.callCode, callBody: tc.callBody}
			cfg := Config{Bots: tc.bots, GitHubOwner: tc.owner}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			if tc.auth != nil {
				cfg.K8sAuthEnabled, cfg.K8sAuthConfig = true, k8sauthtest.Config(t, tc.auth)
			}
			srv := newTestRouter(cfg, nil, &mockOutboundDispatcher{})
			req := httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantRespSub) {
				t.Fatalf("response = %d %s, want %d containing %s", rec.Code, rec.Body.String(), tc.wantStatus, tc.wantRespSub)
			}
			calls := ingress.calls()
			if len(calls) != tc.wantCalls {
				t.Fatalf("readiness calls = %v, want %d", calls, tc.wantCalls)
			}
			if tc.wantCalls > 0 && (calls[0] != callPath || !strings.Contains(ingress.callBodies()[0], tc.wantCallSub)) {
				t.Errorf("readiness call = %s %s, want %s containing %s", calls[0], ingress.callBodies()[0], callPath, tc.wantCallSub)
			}
		})
	}
}
