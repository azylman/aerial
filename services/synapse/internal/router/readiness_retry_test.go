package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauthtest"
)

func TestRetryPRReadiness_K8sAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		auth  *k8sauthtest.RoundTripper
		body  string
		want  int
	}{
		{
			name: "missing bearer token returns 401",
			body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`,
			want: http.StatusUnauthorized,
		},
		{
			name:  "invalid token returns 401",
			token: "invalid-token",
			auth:  &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReviewDenied()},
			body:  `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`,
			want:  http.StatusUnauthorized,
		},
		{
			name:  "caller forbidden returns 403",
			token: "valid-token",
			auth:  &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "agent-alpha"): true}},
			body:  `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"agent-beta"}`,
			want:  http.StatusForbidden,
		},
		{
			name:  "mismatched bot_id without prs/all returns 403",
			token: "valid-token",
			auth:  &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-aerial"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "aerial"): true}},
			body:  `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial","bot_id":"agent-alpha"}`,
			want:  http.StatusForbidden,
		},
		{
			name:  "mismatched bot_id with prs/all succeeds",
			token: "valid-token",
			auth:  &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-aerial"), SARRules: map[string]bool{k8sauthtest.SARKey("prs/all", "create", ""): true, k8sauthtest.SARKey("prs", "create", "aerial"): true}},
			body:  `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial","bot_id":"agent-alpha"}`,
			want:  http.StatusAccepted,
		},
		{
			name:  "authorized retry succeeds",
			token: "valid-token",
			auth:  &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "agent-alpha"): true}},
			body:  `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"agent-alpha"}`,
			want:  http.StatusAccepted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := tc.auth
			if auth == nil {
				auth = &k8sauthtest.RoundTripper{}
			}
			authCfg := k8sauthtest.Config(t, auth)
			ingress := &ownedIngress{callCode: http.StatusOK, callBody: `{"status":202}`}
			disp := &mockOutboundDispatcher{}
			srv := newTestRouter(Config{
				RestateURL:     ingress.serve(t),
				K8sAuthEnabled: true,
				K8sAuthConfig:  authCfg,
			}, nil, disp)

			req := httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			srv.Routes().ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status %d body %s, want %d", w.Code, w.Body.String(), tc.want)
			}
		})
	}
}
