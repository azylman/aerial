package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/k8sauthtest"
	"github.com/azylman/aerial/services/vanadis/internal/storetest"
)

// authFake answers TokenReview as the eri service account (or unauthenticated
// when authOK is false) and SubjectAccessReview with sarOK.
func authFake(authOK, sarOK bool) *k8sauthtest.RoundTripper {
	rt := &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReviewDenied(), SARBody: k8sauthtest.SAR(sarOK, "")}
	if authOK {
		rt.AuthBody = k8sauthtest.TokenReview("system:serviceaccount:default:eri-aerial")
	}
	return rt
}

func TestWriteStoreErr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantCode   int
		wantSubstr string
	}{
		{name: "auth validation error", err: &k8sauth.ValidationError{Message: "bad slug"}, wantCode: http.StatusBadRequest, wantSubstr: "bad slug"},
		{name: "auth unauthorized error", err: &k8sauth.UnauthorizedError{Message: "no token"}, wantCode: http.StatusUnauthorized, wantSubstr: "no token"},
		{name: "auth forbidden error", err: &k8sauth.ForbiddenError{Message: "denied"}, wantCode: http.StatusForbidden, wantSubstr: "denied"},
		{name: "auth generic error", err: errors.New("boom"), wantCode: http.StatusInternalServerError, wantSubstr: "boom"},
		{name: "store build not found", err: ErrBuildNotFound, wantCode: http.StatusNotFound, wantSubstr: "build not found"},
		{name: "store build conflict", err: ErrBuildConflict, wantCode: http.StatusConflict, wantSubstr: "vanadis build state conflict"},
		{name: "store invalid build input", err: ErrInvalidBuildInput, wantCode: http.StatusBadRequest, wantSubstr: "invalid vanadis build input"},
		{name: "restate timeout", err: ErrRestateTimeout, wantCode: http.StatusGatewayTimeout, wantSubstr: "upstream response timed out"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			writeStoreErr(context.Background(), rec, tc.err)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d", rec.Code, tc.wantCode)
			}
			if !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_AuthEnabledConfig(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("tok"), 0o600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}
	srv := NewVanadisServer(Config{Auth: k8sauth.Config{Enabled: true, TokenPath: tokenFile}}, RestateConfig{}, nil, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/builds", http.NoBody)
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestServer_MissingCallerInContext(t *testing.T) {
	t.Parallel()
	auth := k8sauthtest.Authenticator(t, authFake(true, true))
	mem := newMemoryVanadisStore()
	b1 := storetest.Build("b1", "1234567890", "Build 1", "Obj 1", "running")
	c1 := storetest.Chunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", "running")
	_ = mem.CreateBuild(context.Background(), b1, []VanadisChunk{c1})

	srv := NewVanadisServer(Config{}, RestateConfig{}, mem, nil, auth)

	err := srv.authorizeBuildUpdate(context.Background(), "aerial")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if _, ok := errors.AsType[*k8sauth.UnauthorizedError](err); !ok {
		t.Fatalf("expected UnauthorizedError, got %T: %v", err, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /builds/b1", func(w http.ResponseWriter, r *http.Request) { srv.handleBuildRoot(w, r, "b1") })
	mux.HandleFunc("POST /builds", srv.handleCreateBuild)
	mux.HandleFunc("GET /builds", srv.handleListBuilds)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantCode   int
		wantSubstr string
	}{
		{
			name:       "get build missing caller",
			method:     http.MethodGet,
			path:       "/builds/b1",
			wantCode:   http.StatusUnauthorized,
			wantSubstr: errMissingCallerIdentity,
		},
		{
			name:       "create build missing caller",
			method:     http.MethodPost,
			path:       "/builds",
			body:       `{"thread_id":"1234567890","objective":"o","chunks":[{"mode":"action","title":"c","instructions":"i","depends_on":[]}]}`,
			wantCode:   http.StatusUnauthorized,
			wantSubstr: errMissingCallerIdentity,
		},
		{
			name:       "list builds missing caller",
			method:     http.MethodGet,
			path:       "/builds",
			wantCode:   http.StatusUnauthorized,
			wantSubstr: errMissingCallerIdentity,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var body io.Reader = http.NoBody
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}
