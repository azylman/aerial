package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRetryPRReadiness_K8sAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		authStatus int
		authBody   string
		sarStatus  int
		sarRules   []sarRule
		sarBody    string
		body       string
		want       int
	}{
		{
			name: "missing bearer token returns 401",
			body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`,
			want: http.StatusUnauthorized,
		},
		{
			name:       "invalid token returns 401",
			token:      "invalid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`,
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`,
			want:       http.StatusUnauthorized,
		},
		{
			name:       "caller forbidden returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "agent-alpha"}},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"agent-beta"}`,
			want:       http.StatusForbidden,
		},
		{
			name:       "mismatched bot_id without prs/all returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial","bot_id":"agent-alpha"}`,
			want:       http.StatusForbidden,
		},
		{
			name:       "mismatched bot_id with prs/all succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs/all", Verb: "create", Name: ""}, {Resource: "prs", Verb: "create", Name: "aerial"}},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial","bot_id":"agent-alpha"}`,
			want:       http.StatusAccepted,
		},
		{
			name:       "authorized retry succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "agent-alpha"}},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"agent-alpha"}`,
			want:       http.StatusAccepted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := &staticAuthRoundTripper{
				authStatus: tc.authStatus,
				authBody:   tc.authBody,
				sarStatus:  tc.sarStatus,
				sarRules:   tc.sarRules,
				sarBody:    tc.sarBody,
			}
			authCfg := newTestK8sAuthConfig(t, tr)
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
