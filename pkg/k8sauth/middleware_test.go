package k8sauth_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/k8sauthtest"
)

func TestMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		enabled     bool
		exemptPaths []string
		reqPath     string
		authHeader  string
		rt          *k8sauthtest.RoundTripper
		wantStatus  int
		wantAgent   string
	}{
		{name: "disabled middleware passes through unauthenticated", reqPath: "/api/secure", wantStatus: http.StatusOK},
		{name: "exempt path passes through unauthenticated", enabled: true, exemptPaths: []string{"/healthz"}, reqPath: "/healthz", wantStatus: http.StatusOK},
		{name: "exempt path with trailing slash passes through", enabled: true, exemptPaths: []string{"/healthz"}, reqPath: "/healthz/", wantStatus: http.StatusOK},
		{name: "missing authorization header returns 401", enabled: true, reqPath: "/api/secure", wantStatus: http.StatusUnauthorized},
		{name: "malformed authorization header returns 401", enabled: true, reqPath: "/api/secure", authHeader: "Basic invalid-token", wantStatus: http.StatusUnauthorized},
		{
			name:       "authenticate failure returns 401",
			enabled:    true,
			reqPath:    "/api/secure",
			authHeader: "Bearer bad-token",
			rt:         &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReviewDenied()},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "authenticate infrastructure failure returns 500",
			enabled:    true,
			reqPath:    "/api/secure",
			authHeader: "Bearer valid-token",
			rt:         &k8sauthtest.RoundTripper{Err: errors.New("simulated network error")},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "authenticated request attaches caller to context",
			enabled:    true,
			reqPath:    "/api/secure",
			authHeader: "Bearer valid-token",
			rt:         &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha")},
			wantStatus: http.StatusOK,
			wantAgent:  "agent-alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt := tc.rt
			if rt == nil {
				rt = &k8sauthtest.RoundTripper{}
			}
			cfg := k8sauthtest.Config(t, rt)
			cfg.Enabled = tc.enabled
			auth := k8sauth.NewAuthenticator(cfg)

			var capturedAgent string
			handler := auth.Middleware(tc.exemptPaths...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if id, ok := k8sauth.CallerFromContext(r.Context()); ok {
					capturedAgent = id.CallerAgent
				}
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, tc.reqPath, http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("response status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if capturedAgent != tc.wantAgent {
				t.Fatalf("captured caller agent = %q, want %q", capturedAgent, tc.wantAgent)
			}
		})
	}
}
