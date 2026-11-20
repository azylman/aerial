package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type readinessRetryRegistry struct {
	*mockPRRegistry

	accepted bool
	err      error
}

func (r *readinessRetryRegistry) RetryPRReadiness(_ context.Context, _, _ string, _ int) (map[string]kustomizationRequest, error) {
	if r.accepted {
		return map[string]kustomizationRequest{"config": {Namespace: "flux-system", RequestedAt: "2026-01-02T03:04:05Z"}}, r.err
	}
	return nil, r.err
}

func TestRetryPRReadiness(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body        string
		accepted          bool
		dbErr, errorPatch error
		authoritative     bool
		want              int
	}{
		{name: "original request retried", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, accepted: true, want: 202},
		{name: "authoritative journal failure surfaces 503", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, accepted: true, authoritative: true, want: 503},
		{name: "stale identity rejected", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"old"}`, want: 409},
		{name: "missing identity", body: `{"repo":"owner/config","pr_number":42}`, want: 400},
		{name: "invalid number", body: `{"repo":"owner/config","pr_number":0,"expected_merge_sha":"abc"}`, want: 400},
		{name: "malformed", body: `{`, want: 400},
		{name: "no recorded gate", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, want: 409},
		{name: "database failure", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, dbErr: errors.New("offline"), want: 503},
		{name: "patch failure retains durable request", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, accepted: true, errorPatch: errors.New("offline"), want: 503},
		{name: "unknown agent rejected with 409", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"unknown-bot"}`, accepted: false, want: 409},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &readinessRetryRegistry{mockPRRegistry: &mockPRRegistry{}, accepted: tc.accepted, err: tc.dbErr}
			disp := &mockOutboundDispatcher{reconcileErrs: map[string]error{"config": tc.errorPatch}}
			bus := &mockEventBus{}
			srv := &RouterServer{
				registry:   reg,
				dispatcher: disp,
				bus:        bus,
				cfgSrc:     fixedConfig(Config{BotID: "aerial", Bots: map[string]BotConfig{"aerial": {}}, RestateAuthoritative: tc.authoritative}),
			}
			w := httptest.NewRecorder()
			srv.Routes().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body)))
			if w.Code != tc.want {
				t.Fatalf("status %d body %s, want %d", w.Code, w.Body.String(), tc.want)
			}
			if len(bus.publishedPREvents) != 0 {
				t.Fatal("retry emitted lifecycle event before readiness observation")
			}
			if tc.accepted && (len(disp.reconcileRequestedAts) != 1 || disp.reconcileRequestedAts[0] != "2026-01-02T03:04:05Z") {
				t.Fatal("original request identity changed")
			}
		})
	}
}

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
		rawMeta    map[string]any
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
			name:       "existing PR with different agent update forbidden returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}},
			rawMeta:    map[string]any{"agent": "agent-beta"},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial"}`,
			want:       http.StatusForbidden,
		},
		{
			name:       "existing PR with bot_id update forbidden returns 403",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}},
			rawMeta:    map[string]any{"bot_id": "agent-beta"},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial"}`,
			want:       http.StatusForbidden,
		},
		{
			name:       "existing PR with bot_id update allowed succeeds",
			token:      "valid-token",
			authStatus: http.StatusOK,
			authBody:   `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarRules:   []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}, {Resource: "prs", Verb: "update", Name: "aerial"}},
			rawMeta:    map[string]any{"bot_id": "aerial"},
			body:       `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial"}`,
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
			reg := &readinessRetryRegistry{
				mockPRRegistry: &mockPRRegistry{rawMetadata: tc.rawMeta},
				accepted:       true,
			}
			disp := &mockOutboundDispatcher{}
			srv := newTestRouter(Config{
				K8sAuthEnabled: true,
				K8sAuthConfig:  authCfg,
			}, nil, reg, disp)

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
