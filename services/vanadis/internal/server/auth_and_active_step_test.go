package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/buildplan"
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

func TestServer_AuthMiddleware_ExemptAndProtected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		path       string
		method     string
		token      string
		authOk     bool
		sarOk      bool
		wantCode   int
		wantSubstr string
	}{
		{name: "exempt health without token", path: "/health", method: http.MethodGet, wantCode: http.StatusOK, wantSubstr: "ok"},
		{name: "exempt healthz without token", path: "/healthz", method: http.MethodGet, wantCode: http.StatusOK, wantSubstr: "ok"},
		{name: "exempt metrics without token", path: "/metrics", method: http.MethodGet, wantCode: http.StatusOK},
		{name: "protected builds missing auth header", path: "/builds", method: http.MethodGet, wantCode: http.StatusUnauthorized, wantSubstr: "missing Authorization header"},
		{name: "protected builds invalid token format", path: "/builds", method: http.MethodGet, token: "Basic abc", wantCode: http.StatusUnauthorized, wantSubstr: "invalid bearer token"},
		{name: "protected builds unauthenticated token", path: "/builds", method: http.MethodGet, token: "Bearer bad-token", authOk: false, wantCode: http.StatusUnauthorized, wantSubstr: "token not authenticated"},
		{name: "protected builds authenticated but forbidden", path: "/builds", method: http.MethodGet, token: "Bearer good-token", authOk: true, sarOk: false, wantCode: http.StatusForbidden, wantSubstr: "access denied"},
		{name: "protected builds authenticated and allowed", path: "/builds", method: http.MethodGet, token: "Bearer good-token", authOk: true, sarOk: true, wantCode: http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := k8sauthtest.Authenticator(t, authFake(tc.authOk, tc.sarOk))
			mem := newMemoryVanadisStore()
			srv := NewVanadisServer(Config{}, RestateConfig{}, mem, nil, auth)
			mux := srv.Routes()

			req := httptest.NewRequest(tc.method, tc.path, http.NoBody)
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_RBAC_Endpoints(t *testing.T) {
	restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var routed struct {
			Agent string `json:"agent"`
		}
		_ = json.NewDecoder(r.Body).Decode(&routed)
		if strings.HasSuffix(r.URL.Path, "/Create") && !strings.HasPrefix(r.URL.Path, "/"+buildplan.BuildServiceName("")+"/") {
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte(`{"message":"misrouted build object"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		if strings.Contains(r.URL.Path, "RouteAttachPR") {
			_, _ = w.Write([]byte(`{"status":"attached","build_id":"b-new","chunk_id":"c-new","thread_id":"thread-` + routed.Agent + `"}`))
			return
		}
		if strings.Contains(r.URL.Path, "AppendChunks") {
			_, _ = w.Write([]byte(`[{"id":"c2","status":"pending"}]`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"b1","status":"running"}`))
	}))
	defer restateSrv.Close()

	now := time.Now().UTC()
	b1 := storetest.Build("b1", "1234567890", "Build 1", "Obj 1", "running")
	b1.Agent = "aerial"
	b1.CreatedAt = now
	c1 := storetest.Chunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", "running")

	bEmpty := storetest.Build("b-empty", "1234567890", "Build Empty", "Obj", "running")
	bEmpty.CreatedAt = now
	cEmpty := storetest.Chunk("c-empty", "b-empty", 0, "Chunk Empty", "Inst", "medium", "running")

	tests := []struct {
		name       string
		path       string
		method     string
		body       string
		sarOk      bool
		wantCode   int
		wantSubstr string
	}{
		{name: "create build allowed", path: "/builds", method: http.MethodPost, body: `{"thread_id":"1234567890","objective":"obj","chunks":[{"mode":"action","title":"c","instructions":"i","depends_on":[]}]}`, sarOk: true, wantCode: http.StatusAccepted},
		{name: "create build forbidden", path: "/builds", method: http.MethodPost, body: `{"thread_id":"1234567890","objective":"obj","chunks":[{"mode":"action","title":"c","instructions":"i","depends_on":[]}]}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "create build invalid agent slug", path: "/builds", method: http.MethodPost, body: `{"agent":"BAD_SLUG!","thread_id":"1234567890","objective":"obj","chunks":[{"mode":"action","title":"c","instructions":"i","depends_on":[]}]}`, sarOk: true, wantCode: http.StatusBadRequest, wantSubstr: "invalid target agent"},
		{name: "create build for another agent targets its runner", path: "/builds", method: http.MethodPost, body: `{"agent":"agent-alpha","thread_id":"1234567890","objective":"obj","chunks":[{"mode":"action","title":"c","instructions":"i","depends_on":[]}]}`, sarOk: true, wantCode: http.StatusAccepted},
		{name: "attach pr pins agent caller to its own agent", path: "/chunks/attach-pr", method: http.MethodPost, body: `{"repo":"aerial","pr_number":7,"agent":"agent-alpha"}`, sarOk: true, wantCode: http.StatusOK, wantSubstr: `"thread_id":"thread-aerial"`},
		{name: "get build allowed", path: "/builds/b1", method: http.MethodGet, sarOk: true, wantCode: http.StatusOK, wantSubstr: "b1"},
		{name: "get build forbidden", path: "/builds/b1", method: http.MethodGet, sarOk: false, wantCode: http.StatusForbidden},
		{name: "cancel build allowed", path: "/builds/b1/cancel", method: http.MethodPost, body: `{"reason":"cancel"}`, sarOk: true, wantCode: http.StatusOK},
		{name: "cancel build forbidden", path: "/builds/b1/cancel", method: http.MethodPost, body: `{"reason":"cancel"}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "cancel build not found", path: "/builds/b-missing/cancel", method: http.MethodPost, body: `{"reason":"cancel"}`, sarOk: true, wantCode: http.StatusNotFound},
		{name: "cancel build empty agent fallback", path: "/builds/b-empty/cancel", method: http.MethodPost, body: `{"reason":"cancel"}`, sarOk: true, wantCode: http.StatusOK},
		{name: "get chunk by id allowed", path: "/chunks/c1", method: http.MethodGet, sarOk: true, wantCode: http.StatusOK, wantSubstr: "c1"},
		{name: "get chunk by id forbidden", path: "/chunks/c1", method: http.MethodGet, sarOk: false, wantCode: http.StatusForbidden},
		{name: "get chunk by id not found", path: "/chunks/c-missing", method: http.MethodGet, sarOk: true, wantCode: http.StatusNotFound},
		{name: "get build chunk allowed", path: "/builds/b1/chunks/c1", method: http.MethodGet, sarOk: true, wantCode: http.StatusOK, wantSubstr: "c1"},
		{name: "get build chunk forbidden", path: "/builds/b1/chunks/c1", method: http.MethodGet, sarOk: false, wantCode: http.StatusForbidden},
		{name: "get build chunk not found", path: "/builds/b1/chunks/c-missing", method: http.MethodGet, sarOk: true, wantCode: http.StatusNotFound},
		{name: "update build allowed", path: "/builds/b1", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: true, wantCode: http.StatusOK},
		{name: "update build forbidden", path: "/builds/b1", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "update build bad json", path: "/builds/b1", method: http.MethodPatch, body: `{bad json}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "update build not found", path: "/builds/b-missing", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: true, wantCode: http.StatusNotFound},
		{name: "update build empty agent fallback", path: "/builds/b-empty", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: true, wantCode: http.StatusOK},
		{name: "update chunk allowed", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: true, wantCode: http.StatusOK},
		{name: "update chunk forbidden", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "update chunk bad json", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{bad json}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "update chunk empty agent fallback", path: "/builds/b-empty/chunks/c-empty", method: http.MethodPatch, body: `{"title":"new"}`, sarOk: true, wantCode: http.StatusOK},
		{name: "append chunks allowed", path: "/builds/b1/chunks", method: http.MethodPost, body: `{"chunks":[{"mode":"action","title":"c2","instructions":"i2"}]}`, sarOk: true, wantCode: http.StatusOK},
		{name: "append chunks forbidden", path: "/builds/b1/chunks", method: http.MethodPost, body: `{"chunks":[{"mode":"action","title":"c2","instructions":"i2"}]}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "append chunks bad json", path: "/builds/b1/chunks", method: http.MethodPost, body: `{bad json}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "append chunks empty agent fallback", path: "/builds/b-empty/chunks", method: http.MethodPost, body: `{"chunks":[{"mode":"action","title":"c2","instructions":"i2"}]}`, sarOk: true, wantCode: http.StatusOK},
		{name: "remove chunk allowed", path: "/builds/b1/chunks/c1", method: http.MethodDelete, sarOk: true, wantCode: http.StatusOK},
		{name: "remove chunk forbidden", path: "/builds/b1/chunks/c1", method: http.MethodDelete, sarOk: false, wantCode: http.StatusForbidden},
		{name: "remove chunk not found", path: "/builds/b-missing/chunks/c1", method: http.MethodDelete, sarOk: true, wantCode: http.StatusNotFound},
		{name: "remove chunk empty agent fallback", path: "/builds/b-empty/chunks/c-empty", method: http.MethodDelete, sarOk: true, wantCode: http.StatusOK},
		{name: "build by id empty id", path: "/builds/", method: http.MethodGet, sarOk: true, wantCode: http.StatusBadRequest, wantSubstr: "build id is required"},
		{name: "build chunk routes method not allowed", path: "/builds/b1/chunks/c1", method: http.MethodPost, sarOk: true, wantCode: http.StatusMethodNotAllowed},
		{name: "build root method not allowed", path: "/builds/b1", method: http.MethodPut, sarOk: true, wantCode: http.StatusMethodNotAllowed},
		{name: "build cancel method not allowed", path: "/builds/b1/cancel", method: http.MethodGet, sarOk: true, wantCode: http.StatusMethodNotAllowed},
		{name: "build chunk empty chunkRef trailing slash", path: "/builds/b1/chunks/", method: http.MethodGet, sarOk: true, wantCode: http.StatusBadRequest, wantSubstr: "chunk_ref is required"},
		{name: "build chunk unknown subpath", path: "/builds/b1/unknown", method: http.MethodGet, sarOk: true, wantCode: http.StatusNotFound},
		{name: "build chunk deep unknown subpath", path: "/builds/b1/chunks/c1/deep", method: http.MethodGet, sarOk: true, wantCode: http.StatusNotFound},
		{name: "build 3 parts not chunks", path: "/builds/b1/notchunks/c1", method: http.MethodGet, sarOk: true, wantCode: http.StatusNotFound},
		{name: "list builds forbidden", path: "/builds", method: http.MethodGet, sarOk: false, wantCode: http.StatusForbidden},
		{name: "chunk by id method not allowed", path: "/chunks/c1", method: http.MethodPost, sarOk: true, wantCode: http.StatusMethodNotAllowed},
		{name: "update build empty fields", path: "/builds/b1", method: http.MethodPatch, body: `{}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "append chunks empty chunks", path: "/builds/b1/chunks", method: http.MethodPost, body: `{"chunks":[]}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "update chunk invalid mode", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{"mode":"invalid"}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "update chunk invalid effort", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{"effort":"superheavy"}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "update chunk empty mode", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{"mode":""}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "update chunk empty body fields", path: "/builds/b1/chunks/c1", method: http.MethodPatch, body: `{}`, sarOk: true, wantCode: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			auth := k8sauthtest.Authenticator(t, authFake(true, tc.sarOk))
			mem := newMemoryVanadisStore()
			_ = mem.CreateBuild(context.Background(), b1, []VanadisChunk{c1})
			_ = mem.CreateBuild(context.Background(), bEmpty, []VanadisChunk{cEmpty})

			srv := NewVanadisServer(Config{}, RestateConfig{}, mem, nil, auth)
			srv.restateCfg = RestateConfig{RestateIngressURL: restateSrv.URL}
			mux := srv.Routes()

			var body io.Reader = http.NoBody
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_ActiveStepEndpoints(t *testing.T) {
	restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "error-build") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`internal restate error`))
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "UpdateActiveStep/send") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer restateSrv.Close()

	now := time.Now().UTC()
	b1 := storetest.Build("b1", "1234567890", "Build 1", "Obj 1", "running")
	b1.Agent = "aerial"
	b1.CreatedAt = now
	c1 := storetest.Chunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", "running")

	bErr := storetest.Build("error-build", "1234567890", "Error Build", "Obj", "running")
	bErr.Agent = "aerial"
	bErr.CreatedAt = now
	cErr := storetest.Chunk("c-err", "error-build", 0, "Chunk Err", "Inst", "medium", "running")

	bEmptyActive := storetest.Build("b-empty-active", "1234567890", "Build Empty", "Obj", "running")
	bEmptyActive.CreatedAt = now
	cEmptyActive := storetest.Chunk("c-empty-active", "b-empty-active", 0, "Chunk", "Inst", "medium", "running")

	tests := []struct {
		name       string
		path       string
		method     string
		body       string
		sarOk      bool
		wantCode   int
		wantSubstr string
	}{
		{name: "chunks active step invalid json", path: "/chunks/active-step", method: http.MethodPost, body: `{bad json`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "chunks active step missing chunk_id", path: "/chunks/active-step", method: http.MethodPost, body: `{"active_step":"step 1"}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "chunks active step chunk not found", path: "/chunks/active-step", method: http.MethodPost, body: `{"chunk_id":"c-missing","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusNotFound},
		{name: "chunks active step forbidden", path: "/chunks/active-step", method: http.MethodPost, body: `{"chunk_id":"c1","active_step":"step 1"}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "chunks active step success", path: "/chunks/active-step", method: http.MethodPost, body: `{"chunk_id":"c1","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusOK, wantSubstr: "ok"},
		{name: "chunks active step empty agent fallback", path: "/chunks/active-step", method: http.MethodPost, body: `{"chunk_id":"c-empty-active","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusOK, wantSubstr: "ok"},
		{name: "chunks active step restate error 500", path: "/chunks/active-step", method: http.MethodPost, body: `{"chunk_id":"c-err","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusInternalServerError},
		{name: "builds active step method not allowed", path: "/builds/b1/active-step", method: http.MethodGet, sarOk: true, wantCode: http.StatusMethodNotAllowed},
		{name: "builds active step invalid json", path: "/builds/b1/active-step", method: http.MethodPost, body: `{bad json`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "builds active step missing chunk_id", path: "/builds/b1/active-step", method: http.MethodPost, body: `{"active_step":"step 1"}`, sarOk: true, wantCode: http.StatusBadRequest},
		{name: "builds active step build not found", path: "/builds/b-missing/active-step", method: http.MethodPost, body: `{"chunk_id":"c1","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusNotFound},
		{name: "builds active step forbidden", path: "/builds/b1/active-step", method: http.MethodPost, body: `{"chunk_id":"c1","active_step":"step 1"}`, sarOk: false, wantCode: http.StatusForbidden},
		{name: "builds active step success", path: "/builds/b1/active-step", method: http.MethodPost, body: `{"chunk_id":"c1","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusOK, wantSubstr: "ok"},
		{name: "builds active step empty agent fallback", path: "/builds/b-empty-active/active-step", method: http.MethodPost, body: `{"chunk_id":"c-empty-active","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusOK, wantSubstr: "ok"},
		{name: "builds active step restate error 500", path: "/builds/error-build/active-step", method: http.MethodPost, body: `{"chunk_id":"c-err","active_step":"step 1"}`, sarOk: true, wantCode: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			auth := k8sauthtest.Authenticator(t, authFake(true, tc.sarOk))
			mem := newMemoryVanadisStore()
			_ = mem.CreateBuild(context.Background(), b1, []VanadisChunk{c1})
			_ = mem.CreateBuild(context.Background(), bErr, []VanadisChunk{cErr})
			_ = mem.CreateBuild(context.Background(), bEmptyActive, []VanadisChunk{cEmptyActive})

			srv := NewVanadisServer(Config{}, RestateConfig{}, mem, nil, auth)
			srv.restateCfg = RestateConfig{
				RestateIngressURL: restateSrv.URL,
			}
			mux := srv.Routes()

			var body io.Reader = http.NoBody
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_NilStore_Endpoints(t *testing.T) {
	t.Parallel()
	srv := NewVanadisServer(Config{}, RestateConfig{}, nil, nil)
	mux := srv.Routes()

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantCode int
	}{
		{name: "get build chunk nil store", method: http.MethodGet, path: "/builds/b1/chunks/c1", wantCode: http.StatusInternalServerError},
		{name: "remove chunk nil store", method: http.MethodDelete, path: "/builds/b1/chunks/c1", wantCode: http.StatusInternalServerError},
		{name: "update build nil store", method: http.MethodPatch, path: "/builds/b1", body: `{"title":"new"}`, wantCode: http.StatusInternalServerError},
		{name: "chunks active step nil store", method: http.MethodPost, path: "/chunks/active-step", body: `{"chunk_id":"c1"}`, wantCode: http.StatusInternalServerError},
		{name: "builds active step nil store", method: http.MethodPost, path: "/builds/b1/active-step", body: `{"chunk_id":"c1"}`, wantCode: http.StatusInternalServerError},
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
				t.Fatalf("code = %d, want %d", rec.Code, tc.wantCode)
			}
		})
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
