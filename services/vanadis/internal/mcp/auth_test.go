package mcp

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
	"github.com/azylman/aerial/services/vanadis/internal/config"
	"github.com/azylman/aerial/services/vanadis/internal/model"
	"github.com/azylman/aerial/services/vanadis/internal/restate"
	"github.com/azylman/aerial/services/vanadis/internal/store"
)

type staticAuthRoundTripper struct {
	authStatus int
	authBody   string
	sarStatus  int
	sarBody    string
	err        error
}

func (s *staticAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}
	if strings.Contains(req.URL.Path, "tokenreviews") {
		return &http.Response{
			StatusCode: s.authStatus,
			Body:       io.NopCloser(strings.NewReader(s.authBody)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
	if strings.Contains(req.URL.Path, "subjectaccessreviews") {
		return &http.Response{
			StatusCode: s.sarStatus,
			Body:       io.NopCloser(strings.NewReader(s.sarBody)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func newTestAuthenticator(t *testing.T, tr *staticAuthRoundTripper) *k8sauth.Authenticator {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("host-token"), 0o600); err != nil {
		t.Fatalf("failed to write host token: %v", err)
	}
	return k8sauth.NewAuthenticator(k8sauth.Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: "https://k8s.local",
		HTTPClient:   &http.Client{Transport: tr},
	})
}

func authRoundTripper(authOk, sarOk bool) *staticAuthRoundTripper {
	authBody := `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`
	if authOk {
		authBody = `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`
	}
	sarBody := `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false}}`
	if sarOk {
		sarBody = `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`
	}
	return &staticAuthRoundTripper{
		authStatus: http.StatusOK,
		authBody:   authBody,
		sarStatus:  http.StatusOK,
		sarBody:    sarBody,
	}
}

func TestMCP_StoreErrorStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "validation error", err: &k8sauth.ValidationError{Message: "bad slug"}, wantStatus: http.StatusBadRequest},
		{name: "unauthorized error", err: &k8sauth.UnauthorizedError{Message: "no token"}, wantStatus: http.StatusUnauthorized},
		{name: "forbidden error", err: &k8sauth.ForbiddenError{Message: "denied"}, wantStatus: http.StatusForbidden},
		{name: "invalid build input", err: model.ErrInvalidBuildInput, wantStatus: http.StatusBadRequest},
		{name: "build not found", err: model.ErrBuildNotFound, wantStatus: http.StatusNotFound},
		{name: "build conflict", err: model.ErrBuildConflict, wantStatus: http.StatusConflict},
		{name: "generic internal error", err: errors.New("boom"), wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := storeErrorStatus(tc.err)
			if got != tc.wantStatus {
				t.Fatalf("storeErrorStatus(%v) = %d, want %d", tc.err, got, tc.wantStatus)
			}
		})
	}
}

func TestMCP_AuthRBAC_GetBuild(t *testing.T) {
	t.Parallel()
	b1 := mkBuild("b1", "1234567890", "Build 1", "Obj 1", statusRunning)
	b1.Agent = "aerial"
	c1 := mkChunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", statusRunning)

	caller := k8sauth.CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial", IsService: true}

	tests := []struct {
		name       string
		withCaller bool
		sarOk      bool
		wantSubstr string
		wantErr    bool
	}{
		{name: "missing caller", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "forbidden caller", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "allowed caller", withCaller: true, sarOk: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := newTestAuthenticator(t, authRoundTripper(true, tc.sarOk))
			memStore := newMemoryStore()
			memStore.builds["b1"] = b1
			memStore.chunks["b1"] = []model.VanadisChunk{c1}

			handler := NewHandler(memStore, nil, false, auth)
			ctx := context.Background()
			if tc.withCaller {
				ctx = k8sauth.WithCallerIdentity(ctx, caller)
			}
			out, err := handler.handleMCPGetBuild(ctx, MCPGetBuildArgs{BuildID: "b1"})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err %q missing %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.ID != "b1" {
				t.Fatalf("out.ID = %q, want b1", out.ID)
			}
		})
	}
}

func TestMCP_AuthRBAC_GetChunk(t *testing.T) {
	t.Parallel()
	b1 := mkBuild("b1", "1234567890", "Build 1", "Obj 1", statusRunning)
	b1.Agent = "aerial"
	c1 := mkChunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", statusRunning)

	caller := k8sauth.CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial", IsService: true}

	tests := []struct {
		name       string
		withCaller bool
		sarOk      bool
		wantSubstr string
		wantErr    bool
	}{
		{name: "missing caller", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "forbidden caller", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "allowed caller", withCaller: true, sarOk: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := newTestAuthenticator(t, authRoundTripper(true, tc.sarOk))
			memStore := newMemoryStore()
			memStore.builds["b1"] = b1
			memStore.chunks["b1"] = []model.VanadisChunk{c1}

			handler := NewHandler(memStore, nil, false, auth)
			ctx := context.Background()
			if tc.withCaller {
				ctx = k8sauth.WithCallerIdentity(ctx, caller)
			}
			out, err := handler.handleMCPGetChunk(ctx, MCPGetChunkArgs{ChunkID: "c1"})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err %q missing %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.Chunk.ID != "c1" {
				t.Fatalf("out.Chunk.ID = %q, want c1", out.Chunk.ID)
			}
		})
	}
}

func TestMCP_AuthRBAC_Mutations(t *testing.T) {
	restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if strings.Contains(r.URL.Path, "AppendChunks") {
			_, _ = w.Write([]byte(`[{"id":"c2","status":"pending"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "RouteFailChunk") {
			_, _ = w.Write([]byte(`{"failed":true,"build_id":"b1","chunk_id":"c1"}`))
			return
		}
		if strings.Contains(r.URL.Path, "UpdateChunk") {
			_, _ = w.Write([]byte(`{"id":"c1","status":"updated"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"b1","status":"running"}`))
	}))
	defer restateSrv.Close()

	b1 := mkBuild("b1", "1234567890", "Build 1", "Obj 1", statusRunning)
	b1.Agent = "aerial"
	c1 := mkChunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", statusRunning)

	caller := k8sauth.CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial", IsService: true}

	tests := []struct {
		name       string
		op         string
		buildID    string
		chunkID    string
		withCaller bool
		sarOk      bool
		wantSubstr string
		wantErr    bool
	}{
		{name: "update build missing caller", op: "update_build", buildID: "b1", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "update build not found", op: "update_build", buildID: "b-missing", withCaller: true, sarOk: true, wantErr: true, wantSubstr: "not found"},
		{name: "update build forbidden", op: "update_build", buildID: "b1", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "update build allowed", op: "update_build", buildID: "b1", withCaller: true, sarOk: true, wantErr: false},

		{name: "update chunk missing caller", op: "update_chunk", buildID: "b1", chunkID: "c1", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "update chunk not found", op: "update_chunk", buildID: "b-missing", chunkID: "c1", withCaller: true, sarOk: true, wantErr: true, wantSubstr: "not found"},
		{name: "update chunk forbidden", op: "update_chunk", buildID: "b1", chunkID: "c1", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "update chunk allowed", op: "update_chunk", buildID: "b1", chunkID: "c1", withCaller: true, sarOk: true, wantErr: false},

		{name: "append chunk missing caller", op: "append_chunk", buildID: "b1", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "append chunk not found", op: "append_chunk", buildID: "b-missing", withCaller: true, sarOk: true, wantErr: true, wantSubstr: "not found"},
		{name: "append chunk forbidden", op: "append_chunk", buildID: "b1", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "append chunk allowed", op: "append_chunk", buildID: "b1", withCaller: true, sarOk: true, wantErr: false},

		{name: "remove chunk missing caller", op: "remove_chunk", buildID: "b1", chunkID: "c1", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "remove chunk not found", op: "remove_chunk", buildID: "b-missing", chunkID: "c1", withCaller: true, sarOk: true, wantErr: true, wantSubstr: "not found"},
		{name: "remove chunk forbidden", op: "remove_chunk", buildID: "b1", chunkID: "c1", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "remove chunk allowed", op: "remove_chunk", buildID: "b1", chunkID: "c1", withCaller: true, sarOk: true, wantErr: false},

		{name: "fail chunk missing caller", op: "fail_chunk", chunkID: "c1", withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "fail chunk not found", op: "fail_chunk", chunkID: "c-missing", withCaller: true, sarOk: true, wantErr: true, wantSubstr: "not found"},
		{name: "fail chunk forbidden", op: "fail_chunk", chunkID: "c1", withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
		{name: "fail chunk allowed", op: "fail_chunk", chunkID: "c1", withCaller: true, sarOk: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			auth := newTestAuthenticator(t, authRoundTripper(true, tc.sarOk))
			memStore := newMemoryStore()
			memStore.builds["b1"] = b1
			memStore.chunks["b1"] = []model.VanadisChunk{c1}

			rc := restate.NewClient(config.RestateConfig{RestateIngressURL: restateSrv.URL}, nil)
			handler := NewHandler(memStore, rc, true, auth)
			ctx := context.Background()
			if tc.withCaller {
				ctx = k8sauth.WithCallerIdentity(ctx, caller)
			}

			var err error
			switch tc.op {
			case "update_build":
				_, err = handler.handleMCPUpdateBuild(ctx, MCPUpdateBuildArgs{BuildID: tc.buildID, Title: "new"})
			case "update_chunk":
				_, err = handler.handleMCPUpdateChunk(ctx, MCPUpdateChunkArgs{BuildID: tc.buildID, ChunkRef: tc.chunkID, Title: "new"})
			case "append_chunk":
				_, err = handler.handleMCPAppendChunk(ctx, MCPAppendChunkArgs{BuildID: tc.buildID, Chunks: []MCPCreateChunkItem{{Title: "c2", Instructions: "i2"}}})
			case "remove_chunk":
				_, err = handler.handleMCPRemoveChunk(ctx, MCPRemoveChunkArgs{BuildID: tc.buildID, ChunkRef: tc.chunkID})
			case "fail_chunk":
				_, err = handler.handleMCPFailChunk(ctx, MCPFailChunkArgs{ChunkID: tc.chunkID, Reason: "failure"})
			}

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err %q missing %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMCP_RetryActionChunk_Auth(t *testing.T) {
	b1 := mkBuild("b1", "1234567890", "Build 1", "Obj 1", statusFailed)
	b1.Agent = "aerial"
	c1 := mkChunk("c1", "b1", 0, "Chunk 1", "Inst 1", "medium", statusFailed)
	c1.Mode = chunkModeAction

	caller := k8sauth.CallerIdentity{Username: "system:serviceaccount:default:eri-aerial", CallerAgent: "aerial", IsService: true}

	tests := []struct {
		name       string
		buildID    string
		withStore  bool
		withCaller bool
		sarOk      bool
		wantSubstr string
		wantErr    bool
	}{
		{name: "missing caller", buildID: "b1", withStore: true, withCaller: false, sarOk: true, wantErr: true, wantSubstr: "unauthorized"},
		{name: "nil store", buildID: "b1", withStore: false, withCaller: true, sarOk: true, wantErr: true, wantSubstr: "store not configured"},
		{name: "build not found", buildID: "b-missing", withStore: true, withCaller: true, sarOk: true, wantErr: true, wantSubstr: "not found"},
		{name: "forbidden caller", buildID: "b1", withStore: true, withCaller: true, sarOk: false, wantErr: true, wantSubstr: "access denied"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			auth := newTestAuthenticator(t, authRoundTripper(true, tc.sarOk))
			var st store.VanadisStore
			if tc.withStore {
				mem := newMemoryStore()
				mem.builds["b1"] = b1
				mem.chunks["b1"] = []model.VanadisChunk{c1}
				st = mem
			}

			handler := NewHandler(st, nil, false, auth)
			ctx := context.Background()
			if tc.withCaller {
				ctx = k8sauth.WithCallerIdentity(ctx, caller)
			}
			_, err := handler.RetryActionChunk(ctx, RetryActionChunkRequest{
				BuildID:            tc.buildID,
				ChunkID:            "c1",
				ExpectedDispatchID: "d1",
				Reason:             "retry",
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("err %q missing %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
