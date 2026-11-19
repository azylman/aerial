package build

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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
			State: &model.BuildObjectState{
				Build: model.VanadisBuild{ID: "b-restate-mock", Status: "running"},
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
	priorState     *model.BuildObjectState
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
				State: &model.BuildObjectState{
					Build: model.VanadisBuild{ID: "b1", Status: "running"},
				},
			},
			wantStatus:  "running",
			wantChunks:  []string{"c1"},
			wantErr:     false,
			wantCleared: false,
		},
		{
			name: "successful_reconcile_with_nil_launched_reconstructed_from_state",
			priorState: &model.BuildObjectState{
				Build:    model.VanadisBuild{ID: "b1", Status: "running"},
				Launched: map[string]bool{"c0": true},
			},
			priorLaunch:   nil,
			wantReqLaunch: map[string]bool{"c0": true},
			serverStatus:  http.StatusOK,
			serverResponse: reconcileStepOutput{
				BuildStatus:    "running",
				LaunchedChunks: []string{"c1"},
				LaunchedMap:    map[string]bool{"c0": true, "c1": true},
				State: &model.BuildObjectState{
					Build: model.VanadisBuild{ID: "b1", Status: "running"},
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
			priorState: &model.BuildObjectState{
				Build:    model.VanadisBuild{ID: "b1", Status: "running"},
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

func TestBuildObject_AuthoritativeTransitions(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := executor.NewExecutor("", eri, bus, "test-token", nil)

	b := model.VanadisBuild{ID: "b-trans", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr"}
	c2 := model.VanadisChunk{ID: "c2", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr", DependsOn: []string{"c1"}}

	st, _ := dbScript{row: &mockRow{b: &b}, chunks: []model.VanadisChunk{c1, c2}, execTag: "UPDATE 1"}.open()
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	ctx := makeObjectMockContext(t, "b-trans")

	// Reconcile launches c1 (no dependencies)
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{Reason: "tick"})
	assert.NoError(t, err)
	assert.Equal(t, "b-trans", res.BuildID)
	assert.Equal(t, "running", res.Status)
	assert.Contains(t, res.LaunchedChunks, "c1")

	// Mutate: chunk 1 merged -> launches c2
	c1Merged := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "merged", Repo: "aerial", Mode: "pr"}
	storeMerged, _ := dbScript{row: &mockRow{b: &b}, chunks: []model.VanadisChunk{c1Merged, c2}, execTag: "UPDATE 1"}.open()
	boMerged := NewBuildObject("", nil).WithAuthority(storeMerged, exec)

	resMut, err := boMerged.Mutate(ctx, BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "running", resMut.Status)
	assert.Contains(t, resMut.LaunchedChunks, "c2")

	// Merged + completed -> still running until all chunks reach completed (deploy_success)
	c2Done := model.VanadisChunk{ID: "c2", BuildID: "b-trans", Status: "completed", Repo: "aerial", Mode: "pr"}
	storeStillDeploying, _ := dbScript{row: &mockRow{b: &b}, chunks: []model.VanadisChunk{c1Merged, c2Done}, execTag: "UPDATE 1"}.open()
	boStillDeploying := NewBuildObject("", nil).WithAuthority(storeStillDeploying, exec)

	resDeploying, err := boStillDeploying.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusRunning, resDeploying.Status)

	// All completed -> status completed
	c1Done := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "completed", Repo: "aerial", Mode: "pr"}
	storeDone, _ := dbScript{row: &mockRow{b: &b}, chunks: []model.VanadisChunk{c1Done, c2Done}, execTag: "UPDATE 1"}.open()
	boDone := NewBuildObject("", nil).WithAuthority(storeDone, exec)

	resDone, err := boDone.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusCompleted, resDone.Status)
	assert.Equal(t, statusCompleted, eri.lastWidget.Status)

	// Chunk failed -> status failed
	cFailed := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "failed", Repo: "aerial", Mode: "pr"}
	storeFail, _ := dbScript{row: &mockRow{b: &b}, chunks: []model.VanadisChunk{cFailed}, execTag: "UPDATE 1"}.open()
	boFail := NewBuildObject("", nil).WithAuthority(storeFail, exec)

	resFail, err := boFail.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusFailed, resFail.Status)
}

func TestBuildObject_MutationActionsAndTerminalGuards(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := executor.NewExecutor("", eri, bus, "token", nil)
	b := model.VanadisBuild{ID: "b-ext", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-ext", Status: "running", Repo: "aerial", Mode: "pr"}

	st, _ := dbScript{row: &mockRow{b: &b}, routes: []sqlRoute{{match: "SELECT c.id, c.build_id", row: failRow("running", "running")}}, chunks: []model.VanadisChunk{c1}, execTag: "UPDATE 1"}.open()
	bo := NewBuildObject("", nil).WithAuthority(st, exec)
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
	storeNotFound, _ := dbScript{row: errRow(store.ErrBuildNotFound)}.open()
	boNotFound := NewBuildObject("", nil).WithAuthority(storeNotFound, exec)
	res, err = boNotFound.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusNotFound, res.Status)

	// build already terminal
	bTerminal := model.VanadisBuild{ID: "b-ext", Status: "completed"}
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
	var termErr restate.TerminalError
	assert.True(t, errors.As(err, &termErr))
	assert.Equal(t, restate.Code(400), termErr.Code())

	// 500 Server Error
	bo500 := NewBuildObject(srv.URL, srv.Client())
	_, err = bo500.callVanadisTransition(ctx, "transition-500", nil, nil)
	assert.Error(t, err)

	// Bad JSON
	boBadJSON := NewBuildObject(srv.URL, srv.Client())
	_, err = boBadJSON.callVanadisTransition(ctx, "transition-bad-json", nil, nil)
	assert.Error(t, err)
}

func TestBuildObject_AuthoritativeTransitions_LaunchAndAllDone(t *testing.T) {
	t.Parallel()

	// 1. Pending chunk eligible for launch
	bLaunch := model.VanadisBuild{
		ID:        "b-launch",
		ThreadID:  "th-1",
		Title:     "Launch Build",
		Objective: "Test Chunk Launch",
		Status:    "running",
	}
	chunkPending := model.VanadisChunk{
		ID:         "c-pending",
		BuildID:    "b-launch",
		ChunkIndex: 0,
		Title:      "Pending Chunk",
		Repo:       "aerial",
		Status:     "pending",
		Mode:       "pr",
	}

	eriMock := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eriMock, nil, "token", nil)
	storeLaunch, _ := dbScript{
		buildRow: &mockRow{b: &bLaunch},
		chunks:   []model.VanadisChunk{chunkPending},
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
	chunkRunningUnlaunched := model.VanadisChunk{
		ID:         "c-running-unlaunched",
		BuildID:    "b-launch-running",
		ChunkIndex: 0,
		Title:      "Running Unlaunched Chunk",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   0,
	}
	chunkRunningAlreadyLaunched := model.VanadisChunk{
		ID:         "c-running-already",
		BuildID:    "b-launch-running",
		ChunkIndex: 1,
		Title:      "Running Already Launched",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   0,
	}
	chunkRunningWithPR := model.VanadisChunk{
		ID:         "c-running-pr",
		BuildID:    "b-launch-running",
		ChunkIndex: 2,
		Title:      "Running With PR",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   42,
	}
	bLaunchRunning := model.VanadisBuild{
		ID:        "b-launch-running",
		ThreadID:  "th-1",
		Title:     "Launch Running Build",
		Objective: "Test Running Unlaunched Recovery",
		Status:    "running",
	}
	storeRunning, _ := dbScript{
		buildRow: &mockRow{b: &bLaunchRunning},
		chunks:   []model.VanadisChunk{chunkRunningUnlaunched, chunkRunningAlreadyLaunched, chunkRunningWithPR},
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
	chunkDone := model.VanadisChunk{
		ID:         "c-done",
		BuildID:    "b-done",
		ChunkIndex: 0,
		Title:      "Done Chunk",
		Repo:       "aerial",
		Status:     "completed",
		Mode:       "pr",
	}
	bDone := model.VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	storeCompleted, _ := dbScript{
		buildRow: &mockRow{b: &bDone},
		chunks:   []model.VanadisChunk{chunkDone},
		execTag:  "UPDATE 1",
	}.open()
	boDone := NewBuildObject("", nil).WithAuthority(storeCompleted, exec)
	outDone, err := boDone.executeAuthoritativeTransition(context.Background(), "b-done", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "completed", outDone.BuildStatus)

	// 3. One chunk failed -> build fails
	chunkFailed := model.VanadisChunk{
		ID:         "c-failed",
		BuildID:    "b-fail",
		ChunkIndex: 0,
		Title:      "Failed Chunk",
		Repo:       "aerial",
		Status:     "failed",
		Mode:       "pr",
	}
	bFail := model.VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	storeFailed, _ := dbScript{
		buildRow: &mockRow{b: &bFail},
		chunks:   []model.VanadisChunk{chunkFailed},
		execTag:  "UPDATE 1",
	}.open()
	boFailed := NewBuildObject("", nil).WithAuthority(storeFailed, exec)
	outFailed, err := boFailed.executeAuthoritativeTransition(context.Background(), "b-fail", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "failed", outFailed.BuildStatus)
}

func TestBuildObject_ReconcileTerminalStatuses(t *testing.T) {
	t.Parallel()

	// Terminal statuses should return without error and not transition
	for _, status := range []string{"completed", "failed", "cancelled"} {
		bTerm := model.VanadisBuild{ID: "b-term", Status: status, ThreadID: "th-1"}
		storeTerm, _ := dbScript{
			buildRow: &mockRow{b: &bTerm},
			chunks:   []model.VanadisChunk{{ID: "c1", Status: status}},
		}.open()
		bo := NewBuildObject("", nil).WithAuthority(storeTerm, nil)
		out, err := bo.executeAuthoritativeTransition(context.Background(), "b-term", nil, nil, false)
		assert.NoError(t, err)
		assert.Equal(t, status, out.BuildStatus)
	}

	// Build not found returns not_found
	storeNF, _ := dbScript{row: errRow(store.ErrBuildNotFound)}.open()
	boNF := NewBuildObject("", nil).WithAuthority(storeNF, nil)
	outNF, err := boNF.executeAuthoritativeTransition(context.Background(), "b-nf", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "not_found", outNF.BuildStatus)
}

func TestBuildObject_ObjectTransitionsDirect(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-active", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-active", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "pr", Repo: "aerial"}
	st, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	eri := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	rt := newFakeObjectRuntime("b-active")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})

	// executeObjectTransition with nil mutation
	res, err := bo.executeObjectTransition(rt, "reconcile", nil)
	assert.NoError(t, err)
	assert.Equal(t, "running", res.Status)

	// executeObjectTransition with complete action mutation
	cAction := model.VanadisChunk{ID: "c-act", BuildID: "b-active", ChunkIndex: 0, Title: "CAct", Status: "running", Mode: "action"}
	storeAct, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{cAction},
		execTag:  "UPDATE 1",
	}.open()
	boAct := NewBuildObject("", nil).WithAuthority(storeAct, exec)
	rtAct := newFakeObjectRuntime("b-active")
	rtAct.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{cAction}})
	mut := &BuildMutationRequest{Action: "complete_chunk", ChunkID: "c-act"}
	resAct, err := boAct.executeObjectTransition(rtAct, "mutate", mut)
	assert.NoError(t, err)
	assert.Equal(t, "running", resAct.Status)

	// executeObjectTransition with cancel mutation
	bCancel := model.VanadisBuild{ID: "b-cancel", Status: "cancelled", ThreadID: "th-1"}
	storeCancel, _ := dbScript{
		buildRow: &mockRow{b: &bCancel},
		chunks:   []model.VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	boCancel := NewBuildObject("", nil).WithAuthority(storeCancel, exec)
	rtCancel := newFakeObjectRuntime("b-cancel")
	rtCancel.SetState(model.BuildObjectState{Build: bCancel, Chunks: []model.VanadisChunk{c1}})
	mutCancel := &BuildMutationRequest{Action: "cancelled", Reason: "user requested"}
	resCancel, err := boCancel.executeObjectTransition(rtCancel, "mutate-cancel", mutCancel)
	assert.NoError(t, err)
	assert.Equal(t, "cancelled", resCancel.Status)
	assert.True(t, rtCancel.cleared)
}

func openAuthorityHandlerHarness(t *testing.T) (*BuildObject, *fakeObjectRuntime) {
	t.Helper()
	b1 := model.VanadisBuild{ID: "b-cov", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-cov", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	st, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{c1},
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
	exec := executor.NewExecutor(t.TempDir(), &mockEriClient{}, &mockBusExecutor{}, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)
	rt := newFakeObjectRuntime("b-cov")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})
	return bo, rt
}

func TestBuildObject_AuthorityHandlerOperations(t *testing.T) {
	t.Parallel()
	bo, _ := openAuthorityHandlerHarness(t)
	ctx := makeObjectMockContext(t, "b-cov")

	// Smoke-cover each authority handler once; routes are reusable stubs.
	_, _ = bo.FailChunk(ctx, model.FailChunkRequest{ChunkID: "c1", Reason: "chunk fail"})
	_ = bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
	_, _ = bo.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	_, _ = bo.Cancel(ctx, CancelBuildRequest{Reason: "user cancel"})
	_ = bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "boom"})
	_ = bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "closed"})

	mutations := []struct {
		name    string
		action  string
		chunkID string
		reason  string
	}{
		{name: "chunk merged", action: "chunk_merged", chunkID: "c1", reason: "test"},
		{name: "chunk deploy success", action: "chunk_deploy_success", chunkID: "c1", reason: "test"},
		{name: "action completed", action: "action_completed", chunkID: "c1", reason: "test"},
		{name: "chunk failed", action: "chunk_failed", chunkID: "c1", reason: "test"},
		{name: "cancelled", action: "cancelled", chunkID: "c1", reason: "test"},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			boMut, rtMut := openAuthorityHandlerHarness(t)
			_, _ = boMut.executeObjectTransition(rtMut, "mut-"+tc.action, &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, Reason: tc.reason})
		})
	}

	terminals := []struct {
		name        string
		buildID     string
		chunkStatus string
		wantStatus  string
	}{
		{name: "all completed completes build", buildID: "b-done", chunkStatus: "completed", wantStatus: statusCompleted},
		{name: "chunk failed fails build", buildID: "b-fail", chunkStatus: "failed", wantStatus: statusFailed},
	}
	for _, tc := range terminals {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			boTerm, _ := openAuthorityHandlerHarness(t)
			bTerm := model.VanadisBuild{ID: tc.buildID, Status: "running", ThreadID: "th-1"}
			termStatus, err := boTerm.evaluateBuildTerminalStatus(context.Background(), &bTerm, []model.VanadisChunk{{ID: "c1", Status: tc.chunkStatus}})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantStatus, termStatus)
		})
	}
}

type skippedCompletionStore struct {
	store.VanadisStore

	getBuildCalls int
}

func (s *skippedCompletionStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	s.getBuildCalls++
	if s.getBuildCalls > 1 {
		return nil, nil, errors.New("get build fail")
	}
	return &model.VanadisBuild{ID: buildID, Status: "running", ThreadID: "th-1"},
		[]model.VanadisChunk{{ID: "c1", BuildID: buildID}}, nil
}

func (*skippedCompletionStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
	return &store.VanadisAdvancementResult{}, nil
}

type replayFailStore struct {
	store.VanadisStore
}

func (*replayFailStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: buildID, Status: statusCompleted, ThreadID: "th-1"},
		[]model.VanadisChunk{{ID: "c1", BuildID: buildID, Mode: "action"}}, nil
}

func (*replayFailStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
	return &store.VanadisAdvancementResult{CompletionAccepted: true}, nil
}

type errDeploymentAttemptsRuntime struct {
	*fakeObjectRuntime
}

func (errDeploymentAttemptsRuntime) GetDeploymentAttempts() (map[string]deploymentAttempt, error) {
	return nil, errors.New("attempts read fail")
}

func (errDeploymentAttemptsRuntime) SetDeploymentAttempts(map[string]deploymentAttempt) {}

func (errDeploymentAttemptsRuntime) DeploymentNow() (time.Time, error) {
	return time.Now().UTC(), nil
}

func (errDeploymentAttemptsRuntime) GetPendingDeploymentStarts() (map[string]pendingDeploymentStart, error) {
	return map[string]pendingDeploymentStart{}, nil
}

func (errDeploymentAttemptsRuntime) SetPendingDeploymentStarts(map[string]pendingDeploymentStart) {}

func (errDeploymentAttemptsRuntime) ScheduleDeploymentDeadline(DeploymentDeadlineRequest, time.Duration) {
}

func TestBuildObject_ReconcileErrorBranches(t *testing.T) {
	t.Parallel()

	replays := []struct {
		name         string
		buildStatus  string
		action       string
		chunkID      string
		chunkMode    string
		chunkPR      int
		reason       string
		failNotify   bool
		wantErr      bool
		wantNotified bool
	}{
		{name: "completed action completion notifies", buildStatus: statusCompleted, action: "action_completed", chunkID: "c1", chunkMode: "action", reason: "test", wantNotified: true},
		{name: "completed action completion notify failure", buildStatus: statusCompleted, action: "action_completed", chunkID: "c1", chunkMode: "action", reason: "test", failNotify: true, wantErr: true},
		{name: "failed chunk failure notifies", buildStatus: statusFailed, action: "chunk_failed", chunkID: "c1", chunkMode: "pr", reason: "boom", wantNotified: true},
		{name: "failed chunk failure notify failure", buildStatus: statusFailed, action: "chunk_failed", chunkID: "c1", chunkMode: "pr", reason: "boom", failNotify: true, wantErr: true},
	}
	for _, tc := range replays {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bo, _ := openAuthorityHandlerHarness(t)
			eri := &mockEriClient{}
			if tc.failNotify {
				eri.promptFn = func(context.Context, string, string) error { return errors.New("prompt fail") }
				eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
					return "", errors.New("widget fail")
				}
			}
			bo.executor = executor.NewExecutor(t.TempDir(), eri, &mockBusExecutor{}, "token", nil)
			build := &model.VanadisBuild{ID: "b-replay", Status: tc.buildStatus, ThreadID: "th-1"}
			chunks := []model.VanadisChunk{{ID: tc.chunkID, BuildID: "b-replay", Mode: tc.chunkMode, PRNumber: tc.chunkPR}}
			mutation := &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, Reason: tc.reason}
			notified, err := bo.replayTerminalMutationEffects(context.Background(), build, chunks, mutation, false)
			if tc.wantErr {
				assert.Error(t, err)
				assert.False(t, notified)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNotified, notified)
		})
	}

	bMerge := model.VanadisBuild{ID: "b-merge", Status: "running", ThreadID: "th-1"}
	cMerge := model.VanadisChunk{ID: "c1", BuildID: "b-merge", Status: "open", Mode: "pr", Repo: "aerial"}
	stMerge, _ := dbScript{buildRow: &mockRow{b: &bMerge}, chunks: []model.VanadisChunk{cMerge}, execTag: "UPDATE 1"}.open()
	busErr := &mockBusError{err: errors.New("pub fail")}
	boMerge := NewBuildObject("", nil).WithAuthority(stMerge, executor.NewExecutor(t.TempDir(), &mockEriClient{}, busErr, "token", nil))
	assert.NoError(t, boMerge.applyMutation(context.Background(), "b-merge", &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}))

	bBegin := model.VanadisBuild{ID: "b-begin", Status: "running", ThreadID: "th-1"}
	cBegin := model.VanadisChunk{ID: "c1", BuildID: "b-begin", Status: "running", Mode: "action"}
	stBegin, _ := dbScript{buildRow: &mockRow{b: &bBegin}, chunks: []model.VanadisChunk{cBegin}, beginErr: errors.New("begin fail")}.open()
	boBegin := NewBuildObject("", nil).WithAuthority(stBegin, nil)
	assert.Error(t, boBegin.applyMutation(context.Background(), "b-begin", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"}))

	bTerm := model.VanadisBuild{ID: "b-term", Status: "completed", ThreadID: "th-1"}
	stTerm, _ := dbScript{buildRow: &mockRow{b: &bTerm}}.open()
	boTerm := NewBuildObject("", nil).WithAuthority(stTerm, nil)
	outTerm, err := boTerm.executeAuthoritativeTransition(context.Background(), "b-term", nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, statusCompleted, outTerm.BuildStatus)

	boSkip := NewBuildObject("", nil).WithAuthority(&skippedCompletionStore{}, nil)
	_, err = boSkip.executeAuthoritativeTransition(context.Background(), "b-skip", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"}, nil, false)
	assert.Error(t, err)

	eriReplayFail := &mockEriClient{}
	eriReplayFail.promptFn = func(context.Context, string, string) error { return errors.New("prompt fail") }
	eriReplayFail.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
		return "", errors.New("widget fail")
	}
	boReplayFail := NewBuildObject("", nil).WithAuthority(&replayFailStore{},
		executor.NewExecutor(t.TempDir(), eriReplayFail, &mockBusExecutor{}, "token", nil))
	rtReplayFail := newFakeObjectRuntime("b-replay-fail")
	_, err = boReplayFail.executeObjectTransition(rtReplayFail, labelMutateBuild,
		&BuildMutationRequest{Action: "action_completed", ChunkID: "c1"})
	assert.Error(t, err)

	bDeadline := model.VanadisBuild{ID: "b-deadline", Status: "running", ThreadID: "th-1"}
	cDeadline := model.VanadisChunk{ID: "c1", BuildID: "b-deadline", Status: statusDeploying, Mode: "pr", Repo: "aerial", PRNumber: 1, MergeSHA: "sha"}
	stDeadline, _ := dbScript{buildRow: &mockRow{b: &bDeadline}, chunks: []model.VanadisChunk{cDeadline}, execTag: "UPDATE 1"}.open()
	eriDeadlineWidgetFail := &mockEriClient{}
	eriDeadlineWidgetFail.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
		return "", errors.New("widget fail")
	}
	boDeadline := NewBuildObject("", nil).WithAuthority(stDeadline,
		executor.NewExecutor(t.TempDir(), eriDeadlineWidgetFail, &mockBusExecutor{}, "token", nil))
	rtDeadline := &errDeploymentAttemptsRuntime{fakeObjectRuntime: newFakeObjectRuntime("b-deadline")}
	rtDeadline.SetState(model.BuildObjectState{Build: bDeadline, Chunks: []model.VanadisChunk{cDeadline}})
	_, err = boDeadline.executeObjectTransition(rtDeadline, "reconcile", nil)
	assert.Error(t, err)
}
