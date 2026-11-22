package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleRetryPRReadinessRestateOwned(t *testing.T) {
	t.Parallel()
	const (
		validReq = `{"repo":"azylman/aerial","pr_number":42,"expected_merge_sha":"m1"}`
		authOK   = `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`
		callPath = "/PRObject/azylman%2Faerial%2342/RetryReadiness"
	)
	tests := []struct {
		name        string
		k8s         bool
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
			name: "accepted gate is pending", body: validReq, callCode: 200, callBody: `{"status":202,"kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"t1"}}}`,
			wantStatus: http.StatusAccepted, wantRespSub: `"readiness_pending"`, wantCallSub: `{"merge_sha":"m1"}`, wantCalls: 1,
		},
		{name: "bare repo is qualified with the configured owner", owner: "azylman", body: `{"repo":"aerial","pr_number":42,"expected_merge_sha":"m1"}`, callCode: 200, callBody: `{"status":202}`, wantStatus: http.StatusAccepted, wantCallSub: `{"merge_sha":"m1"}`, wantCalls: 1},
		{name: "bare repo without an owner never reaches restate", body: `{"repo":"aerial","pr_number":42,"expected_merge_sha":"m1"}`, wantStatus: http.StatusServiceUnavailable, wantRespSub: "github_owner is unset"},
		{name: "no matching gate is a conflict", body: validReq, callCode: 200, callBody: `{"status":409}`, wantStatus: http.StatusConflict, wantRespSub: "no matching merged rollout gate", wantCalls: 1},
		{name: "restate owner fence is a conflict", body: validReq, callCode: 409, callBody: "owner", wantStatus: http.StatusConflict, wantCalls: 1},
		{name: "restate outage is unavailable", body: validReq, callCode: 500, callBody: "boom", wantStatus: http.StatusServiceUnavailable, wantCalls: 1},
		{name: "no restate url is unavailable", noURL: true, body: validReq, wantStatus: http.StatusServiceUnavailable, wantRespSub: "readiness retry unavailable"},
		{name: "missing merge sha is rejected locally", body: `{"repo":"azylman/aerial","pr_number":42}`, wantStatus: http.StatusBadRequest},
		{name: "invalid pr number is rejected locally", body: `{"repo":"azylman/aerial","pr_number":0,"expected_merge_sha":"m1"}`, wantStatus: http.StatusBadRequest},
		{name: "malformed json is rejected locally", body: `{`, wantStatus: http.StatusBadRequest},
		{name: "unknown agent is a conflict", bots: map[string]BotConfig{"aerial": {}}, body: `{"repo":"azylman/aerial","pr_number":42,"expected_merge_sha":"m1","agent":"unknown-bot"}`, wantStatus: http.StatusConflict},
		{
			name: "known agent hint is forwarded", bots: map[string]BotConfig{"aerial": {}}, body: `{"repo":"azylman/aerial","pr_number":42,"expected_merge_sha":"m1","agent":"aerial"}`, callCode: 200, callBody: `{"status":202}`,
			wantStatus: http.StatusAccepted, wantCallSub: `{"merge_sha":"m1","agent":"aerial"}`, wantCalls: 1,
		},
		{
			name: "k8s caller fences the owner inside restate", k8s: true, token: "valid-token", body: validReq, callCode: 200, callBody: `{"status":202}`,
			wantStatus: http.StatusAccepted, wantCallSub: `"agent":"aerial","enforce_owner":true,"expected_owner":"aerial"`, wantCalls: 1,
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
			if tc.k8s {
				cfg.K8sAuthEnabled = true
				cfg.K8sAuthConfig = newTestK8sAuthConfig(t, &staticAuthRoundTripper{authStatus: http.StatusOK, authBody: authOK, sarRules: []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}}})
			}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			srv.SetDispatcher(&mockOutboundDispatcher{})
			req := httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			srv.handleRetryPRReadiness(rec, req)
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
