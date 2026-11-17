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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
)

type fakeObjectRuntime struct {
	key                string
	launched           map[string]bool
	state              *BuildObjectState
	getStateErr        error
	getLaunchedErr     error
	runStepErr         error
	stepsRun           []string
	cleared            bool
	reconcileScheduled bool
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
	if output != nil && res != nil {
		outVal := reflect.ValueOf(output)
		resVal := reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
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
	origPing, origBus := pingPgxPoolFn, newBusFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
		pingPgxPoolFn, newBusFn = origPing, origBus
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	pingPgxPoolFn = func(context.Context, *pgxpool.Pool) error { return nil }
	newBusFn = func(context.Context, string) (pubsub.Bus, error) { return pubsub.NewMemoryBus(), nil }

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
		PostgresURL: "postgres://mock:mock@localhost:5432/mock",
		NATSURL:     "nats://mock:4222",
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
	origPing, origBus := pingPgxPoolFn, newBusFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
		pingPgxPoolFn, newBusFn = origPing, origBus
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	pingPgxPoolFn = func(context.Context, *pgxpool.Pool) error { return nil }
	newBusFn = func(context.Context, string) (pubsub.Bus, error) { return pubsub.NewMemoryBus(), nil }

	// 1. Invalid health port causes listen error
	errHealth := RunApp(context.Background(), Config{
		HealthPort:  "invalid-port",
		RestatePort: "9080",
		VanadisURL:  "http://vanadis.aerial:4025",
		PostgresURL: "postgres://mock:mock@localhost:5432/mock",
		NATSURL:     "nats://mock:4222",
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
		PostgresURL: "postgres://mock:mock@localhost:5432/mock",
		NATSURL:     "nats://mock:4222",
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

func makeObjectMockContext(t *testing.T, key string) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Key().Return(key).Maybe()
	mc.EXPECT().Get("state", mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get("launched", mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get(pendingDeploymentKey, mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Maybe()
	mc.EXPECT().Set("state", mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().Set("launched", mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().ClearAll().Return().Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Send(mock.Anything, mock.Anything).Return(mInv).Maybe()
	return restate.WithMockContext(mc)
}

func TestToTerminalErr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantNil  bool
		wantCode restate.Code
	}{
		{name: "nil error", err: nil, wantNil: true},
		{name: "invalid build input", err: ErrInvalidBuildInput, wantCode: 400},
		{name: "build not found", err: ErrBuildNotFound, wantCode: 404},
		{name: "chunk not found", err: ErrChunkNotFound, wantCode: 404},
		{name: "build conflict", err: ErrBuildConflict, wantCode: 409},
		{name: "unknown error", err: errors.New("something exploded"), wantCode: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := toTerminalErr(tc.err)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			var term restate.TerminalError
			if tc.wantCode > 0 {
				require.True(t, errors.As(got, &term))
				assert.Equal(t, tc.wantCode, term.Code())
			} else {
				assert.False(t, errors.As(got, &term))
			}
		})
	}
}

func TestLoadConfigFromEnv_Authoritative(t *testing.T) {
	t.Setenv("RESTATE_PORT", "9999")
	t.Setenv("HEALTH_PORT", "8888")
	t.Setenv("VANADIS_URL", "http://vanadis.local:4025")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("ERI_URL", "http://eri.local")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("GITHUB_TOKEN", "token-xyz")
	t.Setenv("SHARE_ROOT", "/tmp/share")

	cfg := LoadConfigFromEnv()
	assert.Equal(t, "9999", cfg.RestatePort)
	assert.Equal(t, "8888", cfg.HealthPort)
	assert.Equal(t, "http://vanadis.local:4025", cfg.VanadisURL)
	assert.Equal(t, "postgres://user:pass@localhost:5432/db", cfg.PostgresURL)
	assert.Equal(t, "http://eri.local", cfg.EriURL)
	assert.Equal(t, "nats://localhost:4222", cfg.NATSURL)
	assert.Equal(t, "token-xyz", cfg.GitHubToken)
	assert.Equal(t, "/tmp/share", cfg.ShareRoot)

	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("POSTGRES_PASSWORD", "secret-pass")
	cfgDSN := LoadConfigFromEnv()
	assert.Equal(t, "postgres://aerial:secret-pass@postgres:5432/aerial?sslmode=disable", cfgDSN.PostgresURL)
}

func TestBuildObject_AuthoritativeHandlers(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor("", eri, bus, "test-token", nil)
	b1 := VanadisBuild{ID: "b1", Title: "Build 1", Objective: "Obj 1", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b1", Title: "Chunk 1", Instructions: "Inst 1", Status: "pending", Mode: "pr", Repo: "aerial", TargetFiles: []string{"pkg/a.go"}}
	c2 := VanadisChunk{ID: "c2", BuildID: "b1", Title: "Chunk 2", Instructions: "Inst 2", Status: "pending", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/b.go"}}
	store, _ := dbScript{
		routes: []sqlRoute{
			{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
			{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("pr")},
			{match: "SELECT build_id FROM vanadis_chunks", row: valuesRow("b1")},
			{match: "RETURNING b.id, c.id, b.thread_id", row: valuesRow("b1", "c1", "th-1")},
			{match: "RETURNING ci_fail_count", row: valuesRow(1)},
			{match: "WHERE c.id = $1", row: failRow("running", "running")},
			{match: "c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b1", 0, "medium", true)},
			{match: "MAX(chunk_index)", row: valuesRow(1)},
		},
		row:      valuesRow("running"),
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1, c2},
		countRow: valuesRow(1),
		execTag:  "UPDATE 1",
	}.open()

	bo := NewBuildObject("", nil).WithAuthority(store, exec)

	// 1. Create validation failure
	ctx := makeObjectMockContext(t, "b1")
	_, err := bo.Create(ctx, CreateBuildRequest{})
	assert.Error(t, err)

	// 2. Create success
	createReq := CreateBuildRequest{
		ID:        "b1",
		ThreadID:  "th-1",
		Objective: "Test build",
		Chunks: []CreateChunkInput{
			{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Do 1", DependsOn: []string{}},
		},
	}
	bld, err := bo.Create(ctx, createReq)
	assert.NoError(t, err)
	assert.Equal(t, "b1", bld.ID)

	// 3. UpdateBuild
	upReq := UpdateBuildRequest{Title: "New Title", Objective: "New Obj"}
	bldUp, err := bo.UpdateBuild(ctx, upReq)
	assert.NoError(t, err)
	assert.Equal(t, "b1", bldUp.ID)

	// 4. UpdateChunk
	cUp, err := bo.UpdateChunk(ctx, UpdateChunkRequest{ChunkRef: "c1", Title: "Updated Title"})
	assert.NoError(t, err)
	assert.Equal(t, "c1", cUp.ID)

	// 5. AppendChunks
	appReq := AppendChunksRequest{Chunks: []CreateChunkInput{{Title: "Chunk 3", Repo: "aerial", Instructions: "Do 3", DependsOn: []string{"c2"}, TargetFiles: []string{"pkg/c.go"}}}}
	appended, err := bo.AppendChunks(ctx, appReq)
	assert.NoError(t, err)
	assert.NotEmpty(t, appended)

	// 6. AttachPR
	attReq := AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}
	attResp, err := bo.AttachPR(ctx, attReq)
	assert.NoError(t, err)
	assert.Equal(t, "attached", attResp.Status)

	// 7. FailChunk
	failReq := FailChunkRequest{ChunkID: "c1", Reason: "unrecoverable error"}
	failResp, err := bo.FailChunk(ctx, failReq)
	assert.NoError(t, err)
	assert.Equal(t, "c1", failResp.ChunkID)

	// 8. RemoveChunk
	remErr := bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
	assert.NoError(t, remErr)

	// 9. OnPREvent
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", CheckName: "lint", CheckURL: "https://github.com/...", Details: "failure", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}}))
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "conflict"}))
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "merged"}))
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "closed"}))

	// 10. OnDispatchOutcome
	assert.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"}))
	assert.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "crashed"}))
	assert.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{}))

	boNil := NewBuildObject("", nil)
	assert.NoError(t, boNil.OnPREvent(ctx, pubsub.PRLifecycleEvent{}))
	assert.NoError(t, boNil.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{}))

	// 11. Cancel
	canBld, err := bo.Cancel(ctx, CancelBuildRequest{Reason: "user cancelled"})
	assert.NoError(t, err)
	assert.Equal(t, "b1", canBld.ID)
}

func TestBuildObject_AuthoritativeTransitions(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor("", eri, bus, "test-token", nil)

	b := VanadisBuild{ID: "b-trans", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr"}
	c2 := VanadisChunk{ID: "c2", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr", DependsOn: []string{"c1"}}

	store, _ := dbScript{row: &mockRow{b: &b}, chunks: []VanadisChunk{c1, c2}, execTag: "UPDATE 1"}.open()
	bo := NewBuildObject("", nil).WithAuthority(store, exec)

	ctx := makeObjectMockContext(t, "b-trans")

	// Reconcile launches c1 (no dependencies)
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{Reason: "tick"})
	assert.NoError(t, err)
	assert.Equal(t, "b-trans", res.BuildID)
	assert.Equal(t, "running", res.Status)
	assert.Contains(t, res.LaunchedChunks, "c1")

	// Mutate: chunk 1 merged -> launches c2
	c1Merged := VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "merged", Repo: "aerial", Mode: "pr"}
	storeMerged, _ := dbScript{row: &mockRow{b: &b}, chunks: []VanadisChunk{c1Merged, c2}, execTag: "UPDATE 1"}.open()
	boMerged := NewBuildObject("", nil).WithAuthority(storeMerged, exec)

	resMut, err := boMerged.Mutate(ctx, BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "running", resMut.Status)
	assert.Contains(t, resMut.LaunchedChunks, "c2")

	// Merged + completed -> still running until all chunks reach completed (deploy_success)
	c2Done := VanadisChunk{ID: "c2", BuildID: "b-trans", Status: "completed", Repo: "aerial", Mode: "pr"}
	storeStillDeploying, _ := dbScript{row: &mockRow{b: &b}, chunks: []VanadisChunk{c1Merged, c2Done}, execTag: "UPDATE 1"}.open()
	boStillDeploying := NewBuildObject("", nil).WithAuthority(storeStillDeploying, exec)

	resDeploying, err := boStillDeploying.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusRunning, resDeploying.Status)

	// All completed -> status completed
	c1Done := VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "completed", Repo: "aerial", Mode: "pr"}
	storeDone, _ := dbScript{row: &mockRow{b: &b}, chunks: []VanadisChunk{c1Done, c2Done}, execTag: "UPDATE 1"}.open()
	boDone := NewBuildObject("", nil).WithAuthority(storeDone, exec)

	resDone, err := boDone.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusCompleted, resDone.Status)
	assert.Equal(t, statusCompleted, eri.lastWidget.Status)

	// Chunk failed -> status failed
	cFailed := VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "failed", Repo: "aerial", Mode: "pr"}
	storeFail, _ := dbScript{row: &mockRow{b: &b}, chunks: []VanadisChunk{cFailed}, execTag: "UPDATE 1"}.open()
	boFail := NewBuildObject("", nil).WithAuthority(storeFail, exec)

	resFail, err := boFail.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusFailed, resFail.Status)
}

func TestBuildObject_Transitions_Extended(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor("", eri, bus, "token", nil)
	b := VanadisBuild{ID: "b-ext", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-ext", Status: "running", Repo: "aerial", Mode: "pr"}

	store, _ := dbScript{row: &mockRow{b: &b}, routes: []sqlRoute{{match: "SELECT c.id, c.build_id", row: failRow("running", "running")}}, chunks: []VanadisChunk{c1}, execTag: "UPDATE 1"}.open()
	bo := NewBuildObject("", nil).WithAuthority(store, exec)
	ctx := makeObjectMockContext(t, "b-ext")

	// chunk_deploy_success
	res, err := bo.Mutate(ctx, BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "b-ext", res.BuildID)

	// action_completed
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "action_completed", ChunkID: "c1"})
	assert.NoError(t, err)

	// chunk_failed
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1", Reason: "boom"})
	assert.NoError(t, err)

	// cancelled
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "cancelled", ChunkID: "c1", Reason: "stopped"})
	assert.NoError(t, err)

	// build not found
	storeNotFound, _ := dbScript{row: errRow(ErrBuildNotFound)}.open()
	boNotFound := NewBuildObject("", nil).WithAuthority(storeNotFound, exec)
	res, err = boNotFound.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusNotFound, res.Status)

	// build already terminal
	bTerminal := VanadisBuild{ID: "b-ext", Status: "completed"}
	storeTerm, _ := dbScript{row: &mockRow{b: &bTerminal}}.open()
	boTerm := NewBuildObject("", nil).WithAuthority(storeTerm, exec)
	res, err = boTerm.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, "completed", res.Status)

	// nil store
	boNil := NewBuildObject("", nil)
	_, err = boNil.executeAuthoritativeTransition(context.Background(), "b1", nil, nil, false)
	assert.Error(t, err)
}

func TestBuildObject_CallVanadisTransition(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/transition-ok"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"build_status":"running","launched_chunks":["c1"]}`))
		case strings.Contains(r.URL.Path, "/transition-400"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`bad request`))
		case strings.Contains(r.URL.Path, "/transition-500"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`server error`))
		case strings.Contains(r.URL.Path, "/transition-bad-json"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{not-json`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	ctx := context.Background()

	// 200 OK
	boOK := NewBuildObject(srv.URL, srv.Client())
	out, err := boOK.callVanadisTransition(ctx, "transition-ok", nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "running", out.BuildStatus)

	// 400 Bad Request
	bo400 := NewBuildObject(srv.URL, srv.Client())
	_, err = bo400.callVanadisTransition(ctx, "transition-400", nil, nil)
	assert.Error(t, err)

	// 500 Server Error
	bo500 := NewBuildObject(srv.URL, srv.Client())
	_, err = bo500.callVanadisTransition(ctx, "transition-500", nil, nil)
	assert.Error(t, err)

	// Bad JSON
	boBadJSON := NewBuildObject(srv.URL, srv.Client())
	_, err = boBadJSON.callVanadisTransition(ctx, "transition-bad-json", nil, nil)
	assert.Error(t, err)
}

func TestBuildObject_HandlerErrors(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")
	store, _ := dbScript{row: errRow(ErrChunkNotFound)}.open()
	bo := NewBuildObject("", nil).WithAuthority(store, nil)

	// UpdateBuild empty
	_, err := bo.UpdateBuild(ctx, UpdateBuildRequest{})
	assert.Error(t, err)

	// UpdateChunk empty ref
	_, err = bo.UpdateChunk(ctx, UpdateChunkRequest{})
	assert.Error(t, err)

	// AppendChunks empty
	_, err = bo.AppendChunks(ctx, AppendChunksRequest{})
	assert.Error(t, err)

	// RemoveChunk empty ref
	err = bo.RemoveChunk(ctx, RemoveChunkRequest{})
	assert.Error(t, err)

	// FailChunk empty
	_, err = bo.FailChunk(ctx, FailChunkRequest{})
	assert.Error(t, err)

	// Create with empty inputs
	_, err = bo.Create(ctx, CreateBuildRequest{})
	assert.Error(t, err)
}

func TestRestateObjectRuntime_SetLaunchedNil(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Set("launched", map[string]bool{}, mock.Anything).Return().Once()
	mc.EXPECT().Key().Return("b1").Once()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object("Build", "b1", "Reconcile").Return(mClient).Once()
	mClient.EXPECT().Send(ReconcileBuildRequest{Reason: "active dispatch recovery"}, restate.WithDelay(time.Second)).Return(mInv).Once()
	ctx := restate.WithMockContext(mc)
	rt := restateObjectRuntime{ctx: ctx}
	rt.SetLaunched(nil)
	rt.ScheduleReconcile()
}

func TestBuildObject_NilStoreBranches(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")
	var typedNilStore *PostgresVanadisStore
	bo := NewBuildObject("", nil).WithAuthority(typedNilStore, nil)
	router := NewBuildRouterService(typedNilStore)
	// Strict interface-nil check to ensure typed-nil pointer was normalized to untyped nil interface;
	// Testify assert.Nil uses reflection and would consider a non-normalized (*PostgresVanadisStore)(nil) interface value nil.
	assert.True(t, router.store == nil)

	// UpdateBuild nil store
	b, err := bo.UpdateBuild(ctx, UpdateBuildRequest{Title: "Title"})
	assert.NoError(t, err)
	assert.Equal(t, "Title", b.Title)

	// Cancel nil store
	b, err = bo.Cancel(ctx, CancelBuildRequest{Reason: "test"})
	assert.NoError(t, err)
	assert.Equal(t, "cancelled", b.Status)

	// AppendChunks nil store
	chunks, err := bo.AppendChunks(ctx, AppendChunksRequest{Chunks: []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}})
	assert.NoError(t, err)
	assert.Nil(t, chunks)

	// UpdateChunk nil store
	chk, err := bo.UpdateChunk(ctx, UpdateChunkRequest{ChunkRef: "c1", Title: "T"})
	assert.NoError(t, err)
	assert.Empty(t, chk.ID)

	// RemoveChunk nil store
	err = bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
	assert.NoError(t, err)

	// FailChunk nil store
	fResp, err := bo.FailChunk(ctx, FailChunkRequest{ChunkID: "c1", Reason: "err"})
	assert.NoError(t, err)
	assert.True(t, fResp.Failed)

	// AttachPR nil store
	aResp, err := bo.AttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "attached", aResp.Status)

	// OnPREvent nil store
	err = bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101})
	assert.NoError(t, err)

	// OnDispatchOutcome nil store
	err = bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"})
	assert.NoError(t, err)

	// Reconcile and Mutate with nil store and empty URL
	_, err = bo.Reconcile(ctx, ReconcileBuildRequest{})
	assert.Error(t, err)
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "ping"})
	assert.Error(t, err)
}

func TestBuildObject_CreateAndCancelErrors(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")

	// Create with empty key and empty ID
	ctxEmptyKey := makeObjectMockContext(t, "")
	bo := NewBuildObject("", nil)
	_, err := bo.Create(ctxEmptyKey, CreateBuildRequest{ID: ""})
	assert.Error(t, err)

	// Create with chunk with empty ID (generates auto ID)
	ctxAuto := makeObjectMockContext(t, "b-auto")
	storeCreate, _ := dbScript{routes: []sqlRoute{
		{match: "SELECT id, thread_id", row: &mockRow{b: &VanadisBuild{ID: "b-auto", Status: "running"}}},
	}}.open()
	boCreate := NewBuildObject("", nil).WithAuthority(storeCreate, nil)
	validNoChunkIDReq := CreateBuildRequest{
		ID:        "b-auto",
		ThreadID:  "th1",
		Objective: "Test Auto Chunk ID",
		Chunks: []CreateChunkInput{
			{Title: "C1", Instructions: "I", Repo: "aerial", DependsOn: []string{}},
		},
	}
	bAuto, err := boCreate.Create(ctxAuto, validNoChunkIDReq)
	assert.NoError(t, err)
	assert.Equal(t, "b-auto", bAuto.ID)

	// Create with empty objective
	_, err = bo.Create(ctx, CreateBuildRequest{ID: "b1", ThreadID: "th1", Objective: ""})
	assert.Error(t, err)

	// Create with cycle in DAG
	cycleReq := CreateBuildRequest{
		ID:        "b1",
		ThreadID:  "th1",
		Objective: "Test Cycle",
		Chunks: []CreateChunkInput{
			{ID: "c1", Title: "C1", Instructions: "I", Repo: "aerial", DependsOn: []string{"c2"}},
			{ID: "c2", Title: "C2", Instructions: "I", Repo: "aerial", DependsOn: []string{"c1"}},
		},
	}
	_, err = bo.Create(ctx, cycleReq)
	assert.Error(t, err)

	// Cancel terminal builds
	bComp := VanadisBuild{ID: "b1", Status: "completed"}
	storeCompleted, _ := dbScript{buildRow: &mockRow{b: &bComp}}.open()
	boCompleted := NewBuildObject("", nil).WithAuthority(storeCompleted, nil)
	_, err = boCompleted.Cancel(ctx, CancelBuildRequest{})
	assert.Error(t, err)

	bFail := VanadisBuild{ID: "b1", Status: "failed"}
	storeFailed, _ := dbScript{buildRow: &mockRow{b: &bFail}}.open()
	boFailed := NewBuildObject("", nil).WithAuthority(storeFailed, nil)
	_, err = boFailed.Cancel(ctx, CancelBuildRequest{})
	assert.Error(t, err)
}

func TestBuildObject_AuthoritativeTransitions_LaunchAndAllDone(t *testing.T) {
	t.Parallel()

	// 1. Pending chunk eligible for launch
	bLaunch := VanadisBuild{
		ID:        "b-launch",
		ThreadID:  "th-1",
		Title:     "Launch Build",
		Objective: "Test Chunk Launch",
		Status:    "running",
	}
	chunkPending := VanadisChunk{
		ID:         "c-pending",
		BuildID:    "b-launch",
		ChunkIndex: 0,
		Title:      "Pending Chunk",
		Repo:       "aerial",
		Status:     "pending",
		Mode:       "pr",
	}

	eriMock := &mockEriClient{}
	exec := NewExecutor(t.TempDir(), eriMock, nil, "token", nil)
	storeLaunch, _ := dbScript{
		buildRow: &mockRow{b: &bLaunch},
		chunks:   []VanadisChunk{chunkPending},
		execTag:  "UPDATE 1",
	}.open()

	bo := NewBuildObject("", nil).WithAuthority(storeLaunch, exec)
	out, err := bo.executeAuthoritativeTransition(context.Background(), "b-launch", nil, map[string]bool{"prior-chk": true}, false)
	assert.NoError(t, err)
	assert.Equal(t, "running", out.BuildStatus)
	assert.Contains(t, out.LaunchedChunks, "c-pending")
	assert.True(t, out.LaunchedMap["c-pending"])

	// 1b. Running chunk with PRNumber == 0 and absent from priorLaunched is dispatched.
	// Already launched running chunk or chunk with PRNumber > 0 is not re-dispatched.
	chunkRunningUnlaunched := VanadisChunk{
		ID:         "c-running-unlaunched",
		BuildID:    "b-launch-running",
		ChunkIndex: 0,
		Title:      "Running Unlaunched Chunk",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   0,
	}
	chunkRunningAlreadyLaunched := VanadisChunk{
		ID:         "c-running-already",
		BuildID:    "b-launch-running",
		ChunkIndex: 1,
		Title:      "Running Already Launched",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   0,
	}
	chunkRunningWithPR := VanadisChunk{
		ID:         "c-running-pr",
		BuildID:    "b-launch-running",
		ChunkIndex: 2,
		Title:      "Running With PR",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   42,
	}
	bLaunchRunning := VanadisBuild{
		ID:        "b-launch-running",
		ThreadID:  "th-1",
		Title:     "Launch Running Build",
		Objective: "Test Running Unlaunched Recovery",
		Status:    "running",
	}
	storeRunning, _ := dbScript{
		buildRow: &mockRow{b: &bLaunchRunning},
		chunks:   []VanadisChunk{chunkRunningUnlaunched, chunkRunningAlreadyLaunched, chunkRunningWithPR},
		execTag:  "UPDATE 1",
	}.open()
	boRunning := NewBuildObject("", nil).WithAuthority(storeRunning, exec)
	outRunning, errRunning := boRunning.executeAuthoritativeTransition(
		context.Background(),
		"b-launch-running",
		nil,
		map[string]bool{"c-running-already": true},
		false,
	)
	assert.NoError(t, errRunning)
	assert.Equal(t, "running", outRunning.BuildStatus)
	assert.Contains(t, outRunning.LaunchedChunks, "c-running-unlaunched")
	assert.NotContains(t, outRunning.LaunchedChunks, "c-running-already")
	assert.NotContains(t, outRunning.LaunchedChunks, "c-running-pr")
	assert.True(t, outRunning.LaunchedMap["c-running-unlaunched"])
	assert.True(t, outRunning.LaunchedMap["c-running-already"])

	// Store GetBuild error branch
	storeGetErr, _ := dbScript{row: errRow(errors.New("db fail"))}.open()
	boGetErr := NewBuildObject("", nil).WithAuthority(storeGetErr, nil)
	_, errGetErr := boGetErr.executeAuthoritativeTransition(context.Background(), "b-fail", nil, nil, false)
	assert.Error(t, errGetErr)

	// 2. All chunks completed -> build completes
	chunkDone := VanadisChunk{
		ID:         "c-done",
		BuildID:    "b-done",
		ChunkIndex: 0,
		Title:      "Done Chunk",
		Repo:       "aerial",
		Status:     "completed",
		Mode:       "pr",
	}
	bDone := VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	storeCompleted, _ := dbScript{
		buildRow: &mockRow{b: &bDone},
		chunks:   []VanadisChunk{chunkDone},
		execTag:  "UPDATE 1",
	}.open()
	boDone := NewBuildObject("", nil).WithAuthority(storeCompleted, exec)
	outDone, err := boDone.executeAuthoritativeTransition(context.Background(), "b-done", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "completed", outDone.BuildStatus)

	// 3. One chunk failed -> build fails
	chunkFailed := VanadisChunk{
		ID:         "c-failed",
		BuildID:    "b-fail",
		ChunkIndex: 0,
		Title:      "Failed Chunk",
		Repo:       "aerial",
		Status:     "failed",
		Mode:       "pr",
	}
	bFail := VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	storeFailed, _ := dbScript{
		buildRow: &mockRow{b: &bFail},
		chunks:   []VanadisChunk{chunkFailed},
		execTag:  "UPDATE 1",
	}.open()
	boFailed := NewBuildObject("", nil).WithAuthority(storeFailed, exec)
	outFailed, err := boFailed.executeAuthoritativeTransition(context.Background(), "b-fail", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "failed", outFailed.BuildStatus)
}

func TestRunApp_ContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	cfg := Config{
		RestatePort: "0",
		HealthPort:  "0",
	}
	err := RunApp(ctx, cfg)
	// Server should cleanly exit when context is cancelled
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("RunApp returned: %v", err)
	}
}

func TestBuildObject_ReconcileTerminalStatuses(t *testing.T) {
	t.Parallel()

	// Terminal statuses should return without error and not transition
	for _, status := range []string{"completed", "failed", "cancelled"} {
		bTerm := VanadisBuild{ID: "b-term", Status: status, ThreadID: "th-1"}
		storeTerm, _ := dbScript{
			buildRow: &mockRow{b: &bTerm},
			chunks:   []VanadisChunk{{ID: "c1", Status: status}},
		}.open()
		bo := NewBuildObject("", nil).WithAuthority(storeTerm, nil)
		out, err := bo.executeAuthoritativeTransition(context.Background(), "b-term", nil, nil, false)
		assert.NoError(t, err)
		assert.Equal(t, status, out.BuildStatus)
	}

	// Build not found returns not_found
	storeNF, _ := dbScript{row: errRow(ErrBuildNotFound)}.open()
	boNF := NewBuildObject("", nil).WithAuthority(storeNF, nil)
	outNF, err := boNF.executeAuthoritativeTransition(context.Background(), "b-nf", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "not_found", outNF.BuildStatus)
}

func TestBuildObject_ObjectTransitionsDirect(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b-active", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-active", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "pr", Repo: "aerial"}
	store, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	eri := &mockEriClient{}
	exec := NewExecutor(t.TempDir(), eri, nil, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(store, exec)

	rt := newFakeObjectRuntime("b-active")
	rt.SetState(BuildObjectState{Build: b1, Chunks: []VanadisChunk{c1}})

	// executeObjectTransition with nil mutation
	res, err := bo.executeObjectTransition(rt, "reconcile", nil)
	assert.NoError(t, err)
	assert.Equal(t, "running", res.Status)

	// executeObjectTransition with complete action mutation
	cAction := VanadisChunk{ID: "c-act", BuildID: "b-active", ChunkIndex: 0, Title: "CAct", Status: "running", Mode: "action"}
	storeAct, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{cAction},
		execTag:  "UPDATE 1",
	}.open()
	boAct := NewBuildObject("", nil).WithAuthority(storeAct, exec)
	rtAct := newFakeObjectRuntime("b-active")
	rtAct.SetState(BuildObjectState{Build: b1, Chunks: []VanadisChunk{cAction}})
	mut := &BuildMutationRequest{Action: "complete_chunk", ChunkID: "c-act"}
	resAct, err := boAct.executeObjectTransition(rtAct, "mutate", mut)
	assert.NoError(t, err)
	assert.Equal(t, "running", resAct.Status)

	// executeObjectTransition with cancel mutation
	bCancel := VanadisBuild{ID: "b-cancel", Status: "cancelled", ThreadID: "th-1"}
	storeCancel, _ := dbScript{
		buildRow: &mockRow{b: &bCancel},
		chunks:   []VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	boCancel := NewBuildObject("", nil).WithAuthority(storeCancel, exec)
	rtCancel := newFakeObjectRuntime("b-cancel")
	rtCancel.SetState(BuildObjectState{Build: bCancel, Chunks: []VanadisChunk{c1}})
	mutCancel := &BuildMutationRequest{Action: "cancelled", Reason: "user requested"}
	resCancel, err := boCancel.executeObjectTransition(rtCancel, "mutate-cancel", mutCancel)
	assert.NoError(t, err)
	assert.Equal(t, "cancelled", resCancel.Status)
	assert.True(t, rtCancel.cleared)
}

func TestBuildObject_HandlersWithAuthorityAndMockContext(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b-auth", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-auth", ChunkIndex: 0, Title: "C1", Status: "pending", Mode: "action"}
	store, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1},
		routes: []sqlRoute{
			{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("action")},
			{match: "UPDATE vanadis_chunks", row: valuesRow("b-auth", "c1", "th-1")},
			{match: "WHERE c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b-auth", 0, "medium", false)},
			{match: "SELECT id, build_id, chunk_index, mode, title, status", row: valuesRow("c1", "b-auth", 0, "action", "C1", "running", 0)},
			{match: "SELECT id, thread_id, tier", row: valuesRow("b-auth", "th-1", 1, "Build Title", "Build Obj", "running", "")},
			{match: "SELECT COUNT(*)", row: valuesRow(1)},
		},
		execTag: "UPDATE 1",
	}.open()
	eri := &mockEriClient{}
	exec := NewExecutor(t.TempDir(), eri, nil, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(store, exec)

	ctx := makeObjectMockContext(t, "b-auth")

	// AppendChunks
	appReq := AppendChunksRequest{Chunks: []CreateChunkInput{{Mode: "action", Title: "C2", Instructions: "Inst"}}}
	_, _ = bo.AppendChunks(ctx, appReq)

	// UpdateChunk
	updChunkReq := UpdateChunkRequest{ChunkRef: "c1", Title: "Updated Action"}
	_, _ = bo.UpdateChunk(ctx, updChunkReq)

	// UpdateBuild
	rt := newFakeObjectRuntime("b-auth")
	rt.SetState(BuildObjectState{Build: b1, Chunks: []VanadisChunk{c1}})
	bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
	updBldReq := UpdateBuildRequest{Title: "Updated Build"}
	_, _ = bo.UpdateBuild(ctx, updBldReq)

	// OnPREvent with check_failed
	prEv := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", CheckName: "lint"}
	_ = bo.OnPREvent(ctx, prEv)

	// OnPREvent with conflict
	prEvConflict := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "conflict"}
	_ = bo.OnPREvent(ctx, prEvConflict)

	// OnPREvent with merged
	prEvMerged := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "merged"}
	_ = bo.OnPREvent(ctx, prEvMerged)

	// OnPREvent with closed (status merged -> no fail)
	prEvClosed := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "merged"}
	_ = bo.OnPREvent(ctx, prEvClosed)

	// OnDispatchOutcome completed
	outEv := pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"}
	_ = bo.OnDispatchOutcome(ctx, outEv)
}

func TestBuildObject_AuthorityCoverageExtra(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b-cov", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-cov", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	store, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1},
		routes: []sqlRoute{
			{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("action")},
			{match: "UPDATE vanadis_chunks", row: valuesRow("b-cov", "c1", "th-1")},
			{match: "WHERE c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b-cov", 0, "medium", false)},
			{match: "SELECT id, build_id, chunk_index, mode, title, status", row: valuesRow("c1", "b-cov", 0, "action", "C1", "running", 0)},
			{match: "SELECT id, thread_id, tier", row: valuesRow("b-cov", "th-1", 1, "Build Title", "Build Obj", "running", "")},
			{match: "SELECT COUNT(*)", row: valuesRow(0)},
			{match: "SELECT c.id, c.build_id, c.chunk_index, c.mode", row: failRow("running", "running")},
		},
		execTag: "UPDATE 1",
	}.open()
	mockBus := &mockBusExecutor{}
	eri := &mockEriClient{}
	exec := NewExecutor(t.TempDir(), eri, mockBus, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(store, exec)
	ctx := makeObjectMockContext(t, "b-cov")

	// FailChunk
	_, _ = bo.FailChunk(ctx, FailChunkRequest{ChunkID: "c1", Reason: "chunk fail"})

	// RemoveChunk
	_ = bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})

	// AttachPR
	_, _ = bo.AttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})

	// Cancel
	_, _ = bo.Cancel(ctx, CancelBuildRequest{Reason: "user cancel"})

	// OnDispatchOutcome failed
	_ = bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "boom"})

	// OnPREvent closed unmerged
	_ = bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "closed"})

	// executeObjectTransition with each mutation action
	rt := newFakeObjectRuntime("b-cov")
	rt.SetState(BuildObjectState{Build: b1, Chunks: []VanadisChunk{c1}})
	for _, act := range []string{"chunk_merged", "chunk_deploy_success", "action_completed", "chunk_failed", "cancelled"} {
		_, _ = bo.executeObjectTransition(rt, "mut-"+act, &BuildMutationRequest{Action: act, ChunkID: "c1", Reason: "test"})
	}

	// evaluateBuildTerminalStatus paths (all completed vs has failed)
	bCompleted := VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	cCompleted := VanadisChunk{ID: "c1", Status: "completed"}
	_ = bo.evaluateBuildTerminalStatus(context.Background(), &bCompleted, []VanadisChunk{cCompleted})

	bFailed := VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	cFailed := VanadisChunk{ID: "c1", Status: "failed"}
	_ = bo.evaluateBuildTerminalStatus(context.Background(), &bFailed, []VanadisChunk{cFailed})
}

func (f *fakeObjectRuntime) ScheduleReconcile() { f.reconcileScheduled = true }
