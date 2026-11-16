package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/httpkit"
)

type fakeObjectRuntime struct {
	key            string
	launched       map[string]bool
	state          *BuildObjectState
	getStateErr    error
	getLaunchedErr error
	runStepErr     error
	stepsRun       []string
	cleared        bool
}

func newFakeObjectRuntime(key string) *fakeObjectRuntime {
	return &fakeObjectRuntime{
		key: key,
	}
}

func (f *fakeObjectRuntime) Key() string {
	return f.key
}

func (f *fakeObjectRuntime) GetLaunched() (map[string]bool, error) {
	if f.getLaunchedErr != nil {
		return nil, f.getLaunchedErr
	}
	if f.launched == nil {
		return nil, nil
	}
	cp := make(map[string]bool, len(f.launched))
	maps.Copy(cp, f.launched)
	return cp, nil
}

func (f *fakeObjectRuntime) SetLaunched(launched map[string]bool) {
	f.launched = make(map[string]bool, len(launched))
	maps.Copy(f.launched, launched)
}

func (f *fakeObjectRuntime) GetState() (*BuildObjectState, error) {
	if f.getStateErr != nil {
		return nil, f.getStateErr
	}
	if f.state == nil {
		return nil, nil
	}
	cp := *f.state
	if f.state.Launched != nil {
		cp.Launched = make(map[string]bool, len(f.state.Launched))
		maps.Copy(cp.Launched, f.state.Launched)
	}
	if f.state.Chunks != nil {
		cp.Chunks = make([]VanadisChunk, len(f.state.Chunks))
		copy(cp.Chunks, f.state.Chunks)
	}
	return &cp, nil
}

func (f *fakeObjectRuntime) SetState(state BuildObjectState) {
	cp := state
	if state.Launched != nil {
		cp.Launched = make(map[string]bool, len(state.Launched))
		maps.Copy(cp.Launched, state.Launched)
	}
	if state.Chunks != nil {
		cp.Chunks = make([]VanadisChunk, len(state.Chunks))
		copy(cp.Chunks, state.Chunks)
	}
	f.state = &cp
}

func (f *fakeObjectRuntime) ClearState() {
	f.cleared = true
	f.launched = make(map[string]bool)
	f.state = nil
}

func (f *fakeObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	f.stepsRun = append(f.stepsRun, name)
	if f.runStepErr != nil {
		return reconcileStepOutput{}, f.runStepErr
	}
	return fn(context.Background())
}

func TestConfig_LoadConfigFromEnv(t *testing.T) {
	t.Setenv("RESTATE_PORT", "9999")
	t.Setenv("HEALTH_PORT", "4444")
	t.Setenv("VANADIS_URL", "http://test-vanadis:4025/")
	cfg := LoadConfigFromEnv()
	assert.Equal(t, "9999", cfg.RestatePort)
	assert.Equal(t, "4444", cfg.HealthPort)
	assert.Equal(t, "http://test-vanadis:4025", cfg.VanadisURL)

	t.Setenv("RESTATE_PORT", "")
	t.Setenv("PORT", "9191")
	t.Setenv("HEALTH_PORT", "")
	t.Setenv("VANADIS_URL", "")
	cfgDefault := LoadConfigFromEnv()
	assert.Equal(t, "9191", cfgDefault.RestatePort)
	assert.Equal(t, "4026", cfgDefault.HealthPort)
	assert.Equal(t, "http://vanadis.aerial:4025", cfgDefault.VanadisURL)

	t.Setenv("PORT", "")
	cfgFallback := LoadConfigFromEnv()
	assert.Equal(t, "9080", cfgFallback.RestatePort)
}

func TestBuildObject_ReflectionAndInterface(t *testing.T) {
	t.Parallel()
	bo := NewBuildObject("http://vanadis:4025", nil)
	assert.Equal(t, "Build", bo.ServiceName())

	require.NotPanics(t, func() {
		def := restate.Reflect(bo)
		require.NotNil(t, def)
	})
}

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return nil }

func runMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(mockRunContext{Context: context.Background()})
	if err != nil {
		return restate.TerminalErrorf("%v", err)
	}
	if outPtr, ok := output.(*reconcileStepOutput); ok && res != nil {
		if stepRes, sOk := res.(reconcileStepOutput); sOk {
			*outPtr = stepRes
		}
	}
	return nil
}

func TestBuildObject_WithRestateMockContext_Completed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reconcileStepOutput{
			BuildStatus:    statusCompleted,
			LaunchedChunks: []string{"c1"},
		})
	}))
	defer ts.Close()

	bo := NewBuildObject(ts.URL, ts.Client())

	mc := mocks.NewMockContext(t)
	mc.EXPECT().Key().Return("b-restate-mock")
	mc.EXPECT().Get("state", mock.Anything).Return(false, nil)
	mc.EXPECT().Get("launched", mock.Anything).Return(false, nil)
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext)
	mc.EXPECT().ClearAll().Return()

	ctx := restate.WithMockContext(mc)
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{})
	require.NoError(t, err)
	assert.Equal(t, statusCompleted, res.Status)
}

func TestBuildObject_WithRestateMockContext_Running(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reconcileStepOutput{
			BuildStatus:    "running",
			LaunchedChunks: []string{"c1"},
			LaunchedMap:    map[string]bool{"c1": true},
			State: &BuildObjectState{
				Build: VanadisBuild{ID: "b-restate-mock", Status: "running"},
			},
		})
	}))
	defer ts.Close()

	bo := NewBuildObject(ts.URL, ts.Client())

	mc := mocks.NewMockContext(t)
	mc.EXPECT().Key().Return("b-restate-mock")
	mc.EXPECT().Get("state", mock.Anything).Return(false, nil)
	mc.EXPECT().Get("launched", mock.Anything).Return(false, nil)
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext)
	mc.EXPECT().Set("state", mock.Anything, mock.Anything).Return()
	mc.EXPECT().Set("launched", mock.Anything, mock.Anything).Return()

	ctx := restate.WithMockContext(mc)
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{})
	require.NoError(t, err)
	assert.Equal(t, "running", res.Status)
}

func TestBuildObject_ValidationFailures(t *testing.T) {
	t.Parallel()

	// 1. nil object or empty vanadisURL
	var nilBO *BuildObject
	_, errNil := nilBO.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errNil)
	_, errNilMut := nilBO.Mutate(nil, BuildMutationRequest{})
	assert.Error(t, errNilMut)

	boEmptyURL := NewBuildObject("", nil)
	_, errEmptyURL := boEmptyURL.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errEmptyURL)
	_, errEmptyURLMut := boEmptyURL.Mutate(nil, BuildMutationRequest{})
	assert.Error(t, errEmptyURLMut)

	// 2. Empty buildID / Key
	boEmptyKey := NewBuildObject("http://localhost:4025", nil, func(ctx restate.ObjectContext) objectRuntime {
		return newFakeObjectRuntime("")
	})
	_, errEmptyKey := boEmptyKey.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errEmptyKey)
	_, errEmptyKeyMut := boEmptyKey.Mutate(nil, BuildMutationRequest{})
	assert.Error(t, errEmptyKeyMut)

	// 3. GetState error
	boGetStateErr := NewBuildObject("http://localhost:4025", nil, func(ctx restate.ObjectContext) objectRuntime {
		rt := newFakeObjectRuntime("b-state-err")
		rt.getStateErr = errors.New("cannot read state")
		return rt
	})
	_, errGetState := boGetStateErr.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errGetState)

	// 4. GetLaunched error
	boGetLaunchedErr := NewBuildObject("http://localhost:4025", nil, func(ctx restate.ObjectContext) objectRuntime {
		rt := newFakeObjectRuntime("b-launch-err")
		rt.getLaunchedErr = errors.New("cannot read launched")
		return rt
	})
	_, errGetLaunched := boGetLaunchedErr.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errGetLaunched)

	// 5. RunStep error
	boRunStepErr := NewBuildObject("http://localhost:4025", nil, func(ctx restate.ObjectContext) objectRuntime {
		rt := newFakeObjectRuntime("b-step-err")
		rt.runStepErr = errors.New("step execution error")
		return rt
	})
	_, errRunStep := boRunStepErr.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errRunStep)
}

type buildObjectTransitionTestCase struct {
	name           string
	mutation       *BuildMutationRequest
	priorState     *BuildObjectState
	priorLaunch    map[string]bool
	wantReqLaunch  map[string]bool
	serverStatus   int
	serverResponse any
	serverErr      bool
	wantStatus     string
	wantChunks     []string
	wantErr        bool
	wantCleared    bool
}

func runBuildObjectTransitionTestCase(t *testing.T, tc buildObjectTransitionTestCase) {
	t.Helper()
	var ts *httptest.Server
	if tc.serverErr {
		ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				_ = conn.Close()
				return
			}
			http.Error(w, "server error", http.StatusInternalServerError)
		}))
	} else {
		ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/internal/restate/builds/b-trans/transition", r.URL.Path)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			if tc.wantReqLaunch != nil {
				var reqBody RestateTransitionRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&reqBody))
				assert.Equal(t, tc.wantReqLaunch, reqBody.PriorLaunched)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(tc.serverStatus)
			switch v := tc.serverResponse.(type) {
			case string:
				_, _ = w.Write([]byte(v))
			default:
				_ = json.NewEncoder(w).Encode(v)
			}
		}))
	}
	t.Cleanup(ts.Close)

	rt := newFakeObjectRuntime("b-trans")
	rt.state = tc.priorState
	rt.launched = tc.priorLaunch
	bo := NewBuildObject(ts.URL, ts.Client(), func(ctx restate.ObjectContext) objectRuntime {
		return rt
	})

	var res ReconcileBuildResult
	var err error
	if tc.mutation != nil {
		res, err = bo.Mutate(nil, *tc.mutation)
	} else {
		res, err = bo.Reconcile(nil, ReconcileBuildRequest{})
	}

	if tc.wantErr {
		assert.Error(t, err)
	} else {
		require.NoError(t, err)
		assert.Equal(t, tc.wantStatus, res.Status)
		assert.Equal(t, tc.wantChunks, res.LaunchedChunks)
		assert.Equal(t, tc.wantCleared, rt.cleared)
	}
}

func TestBuildObject_Transitions_Table(t *testing.T) {
	t.Parallel()

	tests := []buildObjectTransitionTestCase{
		{
			name:         "successful_reconcile_running",
			serverStatus: http.StatusOK,
			serverResponse: reconcileStepOutput{
				BuildStatus:    "running",
				LaunchedChunks: []string{"c1"},
				LaunchedMap:    map[string]bool{"c1": true},
				State: &BuildObjectState{
					Build: VanadisBuild{ID: "b1", Status: "running"},
				},
			},
			wantStatus:  "running",
			wantChunks:  []string{"c1"},
			wantErr:     false,
			wantCleared: false,
		},
		{
			name: "successful_reconcile_with_nil_launched_reconstructed_from_state",
			priorState: &BuildObjectState{
				Build:    VanadisBuild{ID: "b1", Status: "running"},
				Launched: map[string]bool{"c0": true},
			},
			priorLaunch:   nil,
			wantReqLaunch: map[string]bool{"c0": true},
			serverStatus:  http.StatusOK,
			serverResponse: reconcileStepOutput{
				BuildStatus:    "running",
				LaunchedChunks: []string{"c1"},
				LaunchedMap:    map[string]bool{"c0": true, "c1": true},
				State: &BuildObjectState{
					Build: VanadisBuild{ID: "b1", Status: "running"},
				},
			},
			wantStatus:  "running",
			wantChunks:  []string{"c1"},
			wantErr:     false,
			wantCleared: false,
		},
		{
			name: "successful_mutate_completed_clears_state",
			mutation: &BuildMutationRequest{
				Action:  "chunk_merged",
				ChunkID: "c1",
			},
			priorState: &BuildObjectState{
				Build:    VanadisBuild{ID: "b1", Status: "running"},
				Launched: map[string]bool{"c0": true},
			},
			priorLaunch:  map[string]bool{"c0": true},
			serverStatus: http.StatusOK,
			serverResponse: reconcileStepOutput{
				BuildStatus:    statusCompleted,
				LaunchedChunks: nil,
			},
			wantStatus:  statusCompleted,
			wantErr:     false,
			wantCleared: true,
		},
		{
			name:         "server_returns_400_bad_request",
			serverStatus: http.StatusBadRequest,
			serverResponse: map[string]string{
				"error": "invalid build input",
			},
			wantErr: true,
		},
		{
			name:         "server_returns_500_internal_error",
			serverStatus: http.StatusInternalServerError,
			serverResponse: map[string]string{
				"error": "database connection lost",
			},
			wantErr: true,
		},
		{
			name:           "server_returns_invalid_json",
			serverStatus:   http.StatusOK,
			serverResponse: "invalid-json-body",
			wantErr:        true,
		},
		{
			name:      "transport_error",
			serverErr: true,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBuildObjectTransitionTestCase(t, tc)
		})
	}
}

type errBodyReader struct{}

func (errBodyReader) Read([]byte) (int, error) { return 0, errors.New("read body error") }
func (errBodyReader) Close() error             { return nil }

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestCallVanadisTransition_TransportFailures(t *testing.T) {
	t.Parallel()

	// 1. Invalid URL creates request error
	boInvalidURL := NewBuildObject("http://[::1]:namedport", nil, func(ctx restate.ObjectContext) objectRuntime {
		return newFakeObjectRuntime("b-invalid-url")
	})
	_, errReq := boInvalidURL.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errReq)

	// 2. Response body read error
	clientReadErr := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       errBodyReader{},
			}, nil
		}),
	}
	boReadErr := NewBuildObject("http://vanadis.aerial:4025", clientReadErr, func(ctx restate.ObjectContext) objectRuntime {
		return newFakeObjectRuntime("b-read-err")
	})
	_, errRead := boReadErr.Reconcile(nil, ReconcileBuildRequest{})
	assert.Error(t, errRead)
}

func TestHealthEndpoints(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }

	mux := SetupHealthMux()

	for _, path := range []string{"/health", "/healthz", "/metrics", "/debug/coverage/flush"} {
		req := httptest.NewRequest(http.MethodGet, path, http.NoBody)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code, "endpoint %s should return 200 OK", path)
	}
}

func TestRunApp_Shutdown(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }

	// Find two free ports for health and restate
	lHealth, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthPort := strings.TrimPrefix(lHealth.Addr().String(), "127.0.0.1:")
	_ = lHealth.Close()

	lRestate, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	restatePort := strings.TrimPrefix(lRestate.Addr().String(), "127.0.0.1:")
	_ = lRestate.Close()

	cfg := Config{
		HealthPort:  healthPort,
		RestatePort: restatePort,
		VanadisURL:  "http://127.0.0.1:4025",
	}

	ctx, cancel := context.WithCancel(context.Background())
	appDone := make(chan error, 1)
	go func() {
		appDone <- RunApp(ctx, cfg)
	}()

	// Wait for health endpoint to become ready
	healthURL := "http://127.0.0.1:" + healthPort + "/healthz"
	require.Eventually(t, func() bool {
		resp, hErr := http.Get(healthURL)
		if hErr == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return true
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)

	// Cancel context and verify shutdown
	cancel()
	select {
	case err := <-appDone:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunApp failed to stop within 5s")
	}
}

func TestRunApp_ErrorCases(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }

	// 1. Invalid health port causes listen error
	errHealth := RunApp(context.Background(), Config{
		HealthPort:  "invalid-port",
		RestatePort: "9080",
		VanadisURL:  "http://vanadis.aerial:4025",
	})
	assert.Error(t, errHealth)

	// 2. Invalid Restate port causes server error
	lHealth, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthPort := strings.TrimPrefix(lHealth.Addr().String(), "127.0.0.1:")
	_ = lHealth.Close()

	errRestate := RunApp(context.Background(), Config{
		HealthPort:  healthPort,
		RestatePort: "invalid-port",
		VanadisURL:  "http://vanadis.aerial:4025",
	})
	assert.Error(t, errRestate)
}

func TestMain_Execution(t *testing.T) {
	origLoad, origRun, origExit := loadConfigFn, runAppFn, exitFn
	t.Cleanup(func() {
		loadConfigFn, runAppFn, exitFn = origLoad, origRun, origExit
	})

	loadConfigFn = func() Config {
		return Config{HealthPort: "4026", RestatePort: "9080", VanadisURL: "http://vanadis.aerial:4025"}
	}

	// 1. Success case
	runAppFn = func(ctx context.Context, cfg Config) error {
		return nil
	}
	exited := false
	exitFn = func(format string, v ...any) {
		exited = true
	}
	main()
	assert.False(t, exited)

	// 2. Error case
	runAppFn = func(ctx context.Context, cfg Config) error {
		return errors.New("startup failed")
	}
	main()
	assert.True(t, exited)
}
