package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	if err := mem.CreateBuild(context.Background(), b1, []VanadisChunk{c1}); err != nil {
		t.Fatalf("CreateBuild failed: %v", err)
	}

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

type coalescingTestCase struct {
	name              string
	initialSteps      map[string]string
	initialInFlight   bool
	lastForwardOffset time.Duration
	minInterval       time.Duration
	uninitializedMaps bool
	req               UpdateActiveStepRequest
	wantRestateCall   bool
	wantCode          int
	wantSteps         map[string]string
}

func setupCoalescingTestServer(mockURL string, client *http.Client, now time.Time, tc coalescingTestCase) *VanadisServer {
	if tc.uninitializedMaps {
		return &VanadisServer{
			restateCfg: RestateConfig{RestateIngressURL: mockURL},
			httpClient: client,
			nowFn:      func() time.Time { return now },
		}
	}
	srv := NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: mockURL}, nil, client)
	srv.nowFn = func() time.Time { return now }
	if tc.minInterval > 0 {
		srv.minActiveStepInterval = tc.minInterval
	}
	if tc.initialSteps != nil {
		srv.stepsByBuild["b1"] = maps.Clone(tc.initialSteps)
	}
	if tc.initialInFlight {
		srv.inFlightBuilds["b1"] = true
	}
	if tc.lastForwardOffset != 0 {
		srv.lastForwardedAt["b1"] = now.Add(tc.lastForwardOffset)
	}
	return srv
}

func assertCoalescingState(t *testing.T, srv *VanadisServer, tc coalescingTestCase) {
	t.Helper()
	srv.activeStepMu.Lock()
	defer srv.activeStepMu.Unlock()
	if tc.wantSteps == nil {
		if srv.stepsByBuild["b1"] != nil {
			t.Fatalf("expected nil steps map for b1, got %+v", srv.stepsByBuild["b1"])
		}
	} else if !reflect.DeepEqual(srv.stepsByBuild["b1"], tc.wantSteps) {
		t.Fatalf("stepsByBuild[b1] = %+v, want %+v", srv.stepsByBuild["b1"], tc.wantSteps)
	}
	if !tc.initialInFlight && srv.inFlightBuilds["b1"] {
		t.Fatal("expected inFlightBuilds[b1] to be false after completion")
	}
}

func TestProcessActiveStepUpdate_Coalescing(t *testing.T) {
	t.Parallel()

	tests := []coalescingTestCase{
		{
			name:            "initial step forwards to restate",
			req:             UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 1"},
			wantRestateCall: true,
			wantCode:        http.StatusOK,
			wantSteps:       map[string]string{"c1": "step 1"},
		},
		{
			name:              "uninitialized maps and zero minInterval forward safely",
			uninitializedMaps: true,
			req:               UpdateActiveStepRequest{ChunkID: "c1", Step: "fallback step"},
			wantRestateCall:   true,
			wantCode:          http.StatusOK,
			wantSteps:         map[string]string{"c1": "fallback step"},
		},
		{
			name:              "duplicate non-empty step returns ok without calling restate",
			initialSteps:      map[string]string{"c1": "step 1"},
			lastForwardOffset: -5 * time.Second,
			req:               UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 1"},
			wantRestateCall:   false,
			wantCode:          http.StatusOK,
			wantSteps:         map[string]string{"c1": "step 1"},
		},
		{
			name:            "update while in flight returns ok without calling restate but records new step",
			initialSteps:    map[string]string{"c1": "step 1"},
			initialInFlight: true,
			req:             UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 2"},
			wantRestateCall: false,
			wantCode:        http.StatusOK,
			wantSteps:       map[string]string{"c1": "step 2"},
		},
		{
			name:              "throttled update within minInterval returns ok without calling restate",
			initialSteps:      map[string]string{"c1": "step 1"},
			lastForwardOffset: -500 * time.Millisecond,
			minInterval:       2 * time.Second,
			req:               UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 2"},
			wantRestateCall:   false,
			wantCode:          http.StatusOK,
			wantSteps:         map[string]string{"c1": "step 2"},
		},
		{
			name:              "update after minInterval forwards with all active steps",
			initialSteps:      map[string]string{"c1": "step 1", "c2": "other step"},
			lastForwardOffset: -3 * time.Second,
			minInterval:       2 * time.Second,
			req:               UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 2"},
			wantRestateCall:   true,
			wantCode:          http.StatusOK,
			wantSteps:         map[string]string{"c1": "step 2", "c2": "other step"},
		},
		{
			name:              "clear removes chunk and forwards when not in flight",
			initialSteps:      map[string]string{"c1": "step 1", "c2": "step 2"},
			lastForwardOffset: -500 * time.Millisecond,
			minInterval:       2 * time.Second,
			req:               UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: ""},
			wantRestateCall:   true,
			wantCode:          http.StatusOK,
			wantSteps:         map[string]string{"c2": "step 2"},
		},
		{
			name:              "clear last chunk deletes build entry from map",
			initialSteps:      map[string]string{"c1": "step 1"},
			lastForwardOffset: -3 * time.Second,
			req:               UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: ""},
			wantRestateCall:   true,
			wantCode:          http.StatusOK,
			wantSteps:         nil,
		},
		{
			name:            "clear while in flight updates map and returns ok without calling restate",
			initialSteps:    map[string]string{"c1": "step 1"},
			initialInFlight: true,
			req:             UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: ""},
			wantRestateCall: false,
			wantCode:        http.StatusOK,
			wantSteps:       nil,
		},
		{
			name:            "forwarding error returns 500 status",
			req:             UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "trigger-err"},
			wantRestateCall: true,
			wantCode:        http.StatusInternalServerError,
			wantSteps:       map[string]string{"c1": "trigger-err"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			called := false
			var lastFwdReq UpdateActiveStepRequest
			var lastFwdMu sync.Mutex
			mockRestate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if tc.req.ActiveStep == "trigger-err" {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				var fwdReq UpdateActiveStepRequest
				if err := json.NewDecoder(r.Body).Decode(&fwdReq); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(err.Error()))
					return
				}
				lastFwdMu.Lock()
				lastFwdReq = fwdReq
				lastFwdMu.Unlock()
				if fwdReq.ChunkID == "" && fwdReq.ActiveSteps == nil && !fwdReq.ClearAll {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":"chunk_id cannot be empty without active_steps"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			}))
			defer mockRestate.Close()

			now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
			srv := setupCoalescingTestServer(mockRestate.URL, mockRestate.Client(), now, tc)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/builds/b1/active-step", http.NoBody)
			srv.processActiveStepUpdate(rec, req, &VanadisBuild{ID: "b1", Agent: "aerial"}, tc.req)

			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d", rec.Code, tc.wantCode)
			}
			if called != tc.wantRestateCall {
				t.Fatalf("restate called = %v, want %v", called, tc.wantRestateCall)
			}
			if tc.wantSteps == nil && tc.wantRestateCall {
				lastFwdMu.Lock()
				fwd := lastFwdReq
				lastFwdMu.Unlock()
				if !fwd.ClearAll {
					t.Fatal("expected ClearAll to be true on final clear forward")
				}
			}
			assertCoalescingState(t, srv, tc)
		})
	}
}

func TestProcessActiveStepUpdate_TrailingFlushAndPrune(t *testing.T) {
	t.Parallel()
	flushed := make(chan struct{}, 5)
	var lastPayload UpdateActiveStepRequest
	var payloadMu sync.Mutex
	mockRestate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req UpdateActiveStepRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payloadMu.Lock()
		lastPayload = req
		payloadMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		flushed <- struct{}{}
	}))
	defer mockRestate.Close()

	srv := NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: mockRestate.URL}, nil, mockRestate.Client())
	baseTime := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	currentTime := baseTime
	srv.nowFn = func() time.Time { return currentTime }
	srv.minActiveStepInterval = 20 * time.Millisecond
	b := &VanadisBuild{ID: "b-trail", Agent: "aerial"}

	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/builds/b-trail/active-step", http.NoBody)
	srv.processActiveStepUpdate(rec1, req1, b, UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 1"})
	if rec1.Code != http.StatusOK {
		t.Fatalf("step 1 code = %d", rec1.Code)
	}

	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial flush")
	}

	currentTime = baseTime.Add(5 * time.Millisecond) // Inside 20ms debounce window
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/builds/b-trail/active-step", http.NoBody)
	srv.processActiveStepUpdate(rec2, req2, b, UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 2"})
	if rec2.Code != http.StatusOK {
		t.Fatalf("step 2 code = %d", rec2.Code)
	}

	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trailing flush")
	}

	payloadMu.Lock()
	gotStep := lastPayload.ActiveSteps["c1"]
	payloadMu.Unlock()
	if gotStep != "step 2" {
		t.Fatalf("expected trailing flush to send 'step 2', got %q", gotStep)
	}

	// Test prune stale builds
	srv.activeStepMu.Lock()
	now := time.Now()
	for i := range 300 {
		id := fmt.Sprintf("old-b-%d", i)
		srv.lastForwardedAt[id] = now.Add(-2 * time.Hour)
		srv.stepsByBuild[id] = map[string]string{"c": "s"}
	}
	srv.pruneStaleBuildsLocked(now)
	tracked := len(srv.lastForwardedAt)
	srv.activeStepMu.Unlock()
	if tracked > maxTrackedActiveStepBuilds {
		t.Fatalf("expected pruned entries <= %d, got %d", maxTrackedActiveStepBuilds, tracked)
	}
}

func TestProcessActiveStepUpdate_DirtyLoopAndFlush(t *testing.T) {
	t.Parallel()
	var forwardCalls atomic.Int32
	var srv *VanadisServer
	mockRestate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var fwdReq UpdateActiveStepRequest
		if err := json.NewDecoder(r.Body).Decode(&fwdReq); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fwdReq.ChunkID == "" && fwdReq.ActiveSteps == nil && !fwdReq.ClearAll {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n := forwardCalls.Add(1)
		if n == 1 && srv != nil {
			srv.activeStepMu.Lock()
			srv.dirtyBuilds["b-loop"] = true
			srv.stepsByBuild["b-loop"]["c2"] = "step 2"
			srv.activeStepMu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer mockRestate.Close()

	srv = NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: mockRestate.URL}, nil, mockRestate.Client())
	b := &VanadisBuild{ID: "b-loop", Agent: "aerial"}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/builds/b-loop/active-step", http.NoBody)
	srv.processActiveStepUpdate(rec, req, b, UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step 1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	if forwardCalls.Load() != 2 {
		t.Fatalf("expected 2 forward calls in loop, got %d", forwardCalls.Load())
	}

	// Test flushTrailingActiveStep when in flight
	srv.activeStepMu.Lock()
	srv.inFlightBuilds["b-loop"] = true
	srv.activeStepMu.Unlock()
	srv.flushTrailingActiveStep(context.Background(), b)
	srv.activeStepMu.Lock()
	if !srv.dirtyBuilds["b-loop"] {
		t.Fatal("expected dirtyBuilds to be true when flushing during in flight")
	}
	delete(srv.inFlightBuilds, "b-loop")
	srv.activeStepMu.Unlock()

	// Test flushTrailingActiveStep when empty steps
	var emptyCalls atomic.Int32
	emptyMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		emptyCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer emptyMock.Close()
	emptySrv := NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: emptyMock.URL}, nil, emptyMock.Client())
	emptyBuild := &VanadisBuild{ID: "b-empty", Agent: "aerial"}
	emptySrv.flushTrailingActiveStep(context.Background(), emptyBuild)
	if emptyCalls.Load() != 0 {
		t.Fatalf("expected no restate calls for empty steps build, got %d", emptyCalls.Load())
	}
	emptySrv.activeStepMu.Lock()
	if emptySrv.inFlightBuilds["b-empty"] {
		t.Fatal("expected inFlightBuilds to be false for empty steps build")
	}
	emptySrv.activeStepMu.Unlock()

	// Test flushTrailingActiveStep error logging cleans up in flight flag
	var errCalls atomic.Int32
	errMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		errCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer errMock.Close()
	errSrv := NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: errMock.URL}, nil, errMock.Client())
	errBuild := &VanadisBuild{ID: "b-err", Agent: "aerial"}
	errSrv.activeStepMu.Lock()
	errSrv.stepsByBuild["b-err"] = map[string]string{"c1": "s1"}
	errSrv.activeStepMu.Unlock()
	errSrv.flushTrailingActiveStep(context.Background(), errBuild)
	if errCalls.Load() != 1 {
		t.Fatalf("expected 1 restate call on error build, got %d", errCalls.Load())
	}
	errSrv.activeStepMu.Lock()
	if errSrv.inFlightBuilds["b-err"] {
		t.Fatal("expected inFlightBuilds to be cleaned up after forward error")
	}
	errSrv.activeStepMu.Unlock()
}
