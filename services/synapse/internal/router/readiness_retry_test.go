package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauth"
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
		name               string
		body               string
		accepted           bool
		dbErr              error
		errorPatch         error
		authoritative      bool
		authEnabled        bool
		authHeader         string
		tokenReviewResp    string
		sarResp            string
		sarPrsAllDeny      bool
		sarUpdateDeny      bool
		sarUpdateDenyAgent string
		existingAgent      string
		want               int
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
		{
			name:        "k8sauth missing bearer token returns 401",
			body:        `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`,
			authEnabled: true,
			want:        401,
		},
		{
			name:            "k8sauth invalid bearer token returns 401",
			body:            `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`,
			authEnabled:     true,
			authHeader:      "Bearer invalid-tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`,
			want:            401,
		},
		{
			name:            "k8sauth forbidden target agent returns 403",
			body:            `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"miorine"}`,
			authEnabled:     true,
			authHeader:      "Bearer valid-tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false}}`,
			want:            403,
		},
		{
			name:            "k8sauth mismatched bot_id denied on prs/all returns 403",
			body:            `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial","bot_id":"suletta"}`,
			authEnabled:     true,
			authHeader:      "Bearer valid-tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			sarPrsAllDeny:   true,
			want:            403,
		},
		{
			name:            "k8sauth existing agent mismatch denied on prs/update returns 403",
			body:            `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial"}`,
			authEnabled:     true,
			authHeader:      "Bearer valid-tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			sarUpdateDeny:   true,
			existingAgent:   "suletta",
			want:            403,
		},
		{
			name:            "k8sauth authorized target agent succeeds",
			body:            `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"aerial"}`,
			accepted:        true,
			authEnabled:     true,
			authHeader:      "Bearer valid-tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			want:            202,
		},
		{
			name:               "k8sauth non-default agent retry on unregistered PR succeeds without prs:update on default agent",
			body:               `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc","agent":"miorine"}`,
			accepted:           true,
			authEnabled:        true,
			authHeader:         "Bearer valid-tok",
			tokenReviewResp:    `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-miorine"}}}`,
			sarResp:            `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			sarUpdateDeny:      true,
			sarUpdateDenyAgent: "aerial",
			want:               202,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{}
			if tc.existingAgent != "" {
				mockReg.rawMetadata = map[string]any{"agent": tc.existingAgent}
			}
			reg := &readinessRetryRegistry{mockPRRegistry: mockReg, accepted: tc.accepted, err: tc.dbErr}
			disp := &mockOutboundDispatcher{reconcileErrs: map[string]error{"config": tc.errorPatch}}
			bus := &mockEventBus{}
			srv := &RouterServer{registry: reg, dispatcher: disp, bus: bus}
			if tc.authoritative {
				srv.cfgSrc = fixedConfig(Config{RestateAuthoritative: true})
			}
			if tc.authEnabled {
				tokenFile := filepath.Join(t.TempDir(), "token")
				_ = os.WriteFile(tokenFile, []byte("sa-token"), 0o600)
				rt := &cannedAuthRoundTripper{
					tokenReviewResp:    tc.tokenReviewResp,
					sarResp:            tc.sarResp,
					sarPrsAllDeny:      tc.sarPrsAllDeny,
					sarUpdateDeny:      tc.sarUpdateDeny,
					sarUpdateDenyAgent: tc.sarUpdateDenyAgent,
				}
				auth := k8sauth.NewAuthenticator(k8sauth.Config{
					Enabled:      true,
					APIServerURL: "https://k8s.local",
					HTTPClient:   &http.Client{Transport: rt},
					TokenPath:    tokenFile,
				})
				srv.SetAuthenticator(auth)
			}
			req := httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body))
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			w := httptest.NewRecorder()
			srv.Routes().ServeHTTP(w, req)
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
