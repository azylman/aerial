package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestBuildObject_Reflection(t *testing.T) {
	t.Parallel()

	srv := &VanadisServer{}
	bo := NewBuildObject(srv)
	assert.Equal(t, "Build", bo.ServiceName())

	// Exercise runtime adapter methods on concrete restateObjectRuntime type
	var or objectRuntime = restateObjectRuntime{}
	assert.NotNil(t, or)

	// restate.Reflect panics if handler signatures are invalid
	require.NotPanics(t, func() {
		def := restate.Reflect(bo)
		require.NotNil(t, def)
	})

	// Direct reflection call on nil or empty build_id
	nilBO := NewBuildObject(nil)
	resNil, errNil := nilBO.Reconcile(nil, ReconcileBuildRequest{Reason: "test"})
	require.Error(t, errNil)
	assert.Empty(t, resNil.BuildID)

	resMutNil, errMutNil := nilBO.Mutate(nil, BuildMutationRequest{Action: "chunk_merged"})
	require.Error(t, errMutNil)
	assert.Empty(t, resMutNil.BuildID)
}

func TestBuildObject_Reconcile_EmptyBuildID(t *testing.T) {
	t.Parallel()

	bo := NewBuildObject(nil)
	res, err := bo.Reconcile(nil, ReconcileBuildRequest{Reason: "test"})
	require.Error(t, err)
	assert.Empty(t, res.BuildID)
}

func TestRegisterRestateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		adminHandler http.HandlerFunc
		adminURL     string
		advertiseURL string
		wantErr      bool
		errContains  string
	}{
		{
			name:         "empty urls skip registration",
			adminURL:     "",
			advertiseURL: "",
			wantErr:      false,
		},
		{
			name: "successful registration",
			adminHandler: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/deployments", r.URL.Path)
				assert.Equal(t, http.MethodPost, r.Method)
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				var payload map[string]any
				assert.NoError(t, json.Unmarshal(body, &payload))
				assert.Equal(t, "http://vanadis:9080", payload["uri"])
				assert.Equal(t, true, payload["force"])
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":"dep-123"}`))
			},
			advertiseURL: "http://vanadis:9080",
			wantErr:      false,
		},
		{
			name: "admin error response returns error",
			adminHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`invalid deployment uri`))
			},
			advertiseURL: "http://vanadis:9080",
			wantErr:      true,
			errContains:  "restate admin returned status 400",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adminURL := tc.adminURL
			if tc.adminHandler != nil {
				ts := httptest.NewServer(tc.adminHandler)
				defer ts.Close()
				adminURL = ts.URL
			}

			err := registerRestateDeployment(context.Background(), adminURL, tc.advertiseURL)
			if tc.wantErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAutoRegisterRestateDeployment(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if att < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"registered"}`))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	autoRegisterRestateDeployment(ctx, ts.URL, "http://vanadis:9080")
	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestStartRestateServer_Disabled(t *testing.T) {
	t.Parallel()

	srv := &VanadisServer{
		restateCfg: RestateConfig{RestatePort: "disabled"},
	}
	rServer, err := startRestateServer(context.Background(), srv)
	require.ErrorIs(t, err, ErrRestateDisabled)
	assert.Nil(t, rServer)

	srvMinusOne := &VanadisServer{
		restateCfg: RestateConfig{RestatePort: "-1"},
	}
	rServer2, err2 := startRestateServer(context.Background(), srvMinusOne)
	require.ErrorIs(t, err2, ErrRestateDisabled)
	assert.Nil(t, rServer2)

	srvNil := (*VanadisServer)(nil)
	_, errNil := startRestateServer(context.Background(), srvNil)
	require.Error(t, errNil)
}

func TestComputeEligibleChunksToLaunch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		chunkOrder  []string
		states      map[string]*workflowChunkRuntimeState
		maxParallel int
		wantIDs     []string
	}{
		{
			name:        "all active chunks capacity exhausted",
			chunkOrder:  []string{"c1", "c2"},
			maxParallel: 1,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {launched: true, codeReady: false},
				"c2": {launched: false, codeReady: false},
			},
			wantIDs: nil,
		},
		{
			name:        "single independent chunk launches",
			chunkOrder:  []string{"c1"},
			maxParallel: 4,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {chunk: VanadisChunk{ID: "c1", Mode: "pr"}},
			},
			wantIDs: []string{"c1"},
		},
		{
			name:        "pr chunk unblocked by upstream merged pr chunk",
			chunkOrder:  []string{"c1", "c2"},
			maxParallel: 4,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {chunk: VanadisChunk{ID: "c1", Mode: "pr"}, launched: true, codeReady: true, runtimeReady: false},
				"c2": {chunk: VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}}},
			},
			wantIDs: []string{"c2"},
		},
		{
			name:        "action chunk blocked when upstream pr chunk is not runtimeReady",
			chunkOrder:  []string{"c1", "c2"},
			maxParallel: 4,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {chunk: VanadisChunk{ID: "c1", Mode: "pr"}, launched: true, codeReady: true, runtimeReady: false},
				"c2": {chunk: VanadisChunk{ID: "c2", Mode: "action", DependsOn: []string{"c1"}}},
			},
			wantIDs: nil,
		},
		{
			name:        "action chunk unblocked when upstream pr chunk is runtimeReady",
			chunkOrder:  []string{"c1", "c2"},
			maxParallel: 4,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {chunk: VanadisChunk{ID: "c1", Mode: "pr"}, launched: true, codeReady: true, runtimeReady: true},
				"c2": {chunk: VanadisChunk{ID: "c2", Mode: "action", DependsOn: []string{"c1"}}},
			},
			wantIDs: []string{"c2"},
		},
		{
			name:        "action chunk unblocked when upstream action chunk is runtimeReady",
			chunkOrder:  []string{"c1", "c2"},
			maxParallel: 4,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {chunk: VanadisChunk{ID: "c1", Mode: "action"}, launched: true, codeReady: true, runtimeReady: true},
				"c2": {chunk: VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}}},
			},
			wantIDs: []string{"c2"},
		},
		{
			name:        "unknown dependency prevents launching",
			chunkOrder:  []string{"c1"},
			maxParallel: 4,
			states: map[string]*workflowChunkRuntimeState{
				"c1": {chunk: VanadisChunk{ID: "c1", Mode: "pr", DependsOn: []string{"c999"}}},
			},
			wantIDs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := computeEligibleChunksToLaunch(tc.chunkOrder, tc.states, tc.maxParallel)
			assert.Equal(t, tc.wantIDs, got)
		})
	}
}

func TestWorkflowExecutionHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// nil server / nil store
	assert.Error(t, executeCompleteWorkflowBuild(ctx, nil, "b1"))
	launched, err := executeLaunchChunk(ctx, nil, "b1", VanadisChunk{})
	assert.False(t, launched)
	assert.Error(t, err)

	store := newMemoryVanadisStore()
	b := VanadisBuild{ID: "b-wf", ThreadID: "th-1", Objective: "test wf"}
	chunks := []VanadisChunk{
		{ID: "c1", BuildID: "b-wf", ChunkIndex: 0, Mode: "pr", Title: "c1", Instructions: "do c1"},
		{ID: "c2", BuildID: "b-wf", ChunkIndex: 1, Mode: "action", Title: "c2", Instructions: "do c2"},
	}
	require.NoError(t, store.CreateBuild(ctx, b, chunks))

	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: store, bus: memBus}

	// executeLaunchChunk enqueues execute chunk and reserves in running status
	launched, err = executeLaunchChunk(ctx, srv, "b-wf", chunks[0])
	require.NoError(t, err)
	assert.True(t, launched)
	gotChunk, _, err := store.GetChunk(ctx, "c1")
	require.NoError(t, err)
	assert.Equal(t, "running", gotChunk.Status, "enqueued chunk must be reserved in running status")

	// executeLaunchChunk on already opened PR does not enqueue execute chunk
	srv.bus = nil
	chunks[0].Status = "pr_opened"
	launched, err = executeLaunchChunk(ctx, srv, "b-wf", chunks[0])
	require.NoError(t, err)
	assert.False(t, launched)
}

func TestRestateIngressClientHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var paths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"Accepted"}`))
	}))
	defer ts.Close()

	store := newMemoryVanadisStore()
	b := VanadisBuild{ID: "b-restate", ThreadID: "123456789012345678", Objective: "test ingress", Status: "running"}
	chunks := []VanadisChunk{
		{ID: "c1", BuildID: "b-restate", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "c1", Instructions: "do c1", Status: "pr_opened", PRNumber: 42},
		{ID: "c2", BuildID: "b-restate", ChunkIndex: 1, Mode: "action", Title: "c2", Instructions: "do c2", Status: "completed"},
		{ID: "c3", BuildID: "b-restate", ChunkIndex: 2, Mode: "action", Title: "c3", Instructions: "do c3", Status: "failed"},
	}
	require.NoError(t, store.CreateBuild(ctx, b, chunks))

	srv := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts.URL},
		store:      store,
		httpClient: ts.Client(),
	}

	require.NoError(t, srv.launchCreatedBuild(ctx, "b-restate"))
	require.NoError(t, srv.launchBuildOnRestate(ctx, "b-restate"))

	// Test that an action chunk completing a build under Restate dispatches completed wake prompt
	var wakePromptSent string
	tsWake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		wakePromptSent = req["prompt"]
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer tsWake.Close()

	srvWake := &VanadisServer{
		cfg:        Config{BrainURL: tsWake.URL},
		restateCfg: RestateConfig{RestateIngressURL: ts.URL},
		store:      store,
		httpClient: ts.Client(),
	}
	bAct := VanadisBuild{ID: "b-act-single", ThreadID: "123456789012345678", Objective: "test act wake", Status: "running"}
	cAct := []VanadisChunk{
		{ID: "c-act-single", BuildID: "b-act-single", ChunkIndex: 0, Mode: "action", Title: "act 1", Status: "running"},
	}
	require.NoError(t, store.CreateBuild(ctx, bAct, cAct))
	require.NoError(t, srvWake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-single",
		DispatchID:     "d-act-1",
	}))
	assert.NotEmpty(t, wakePromptSent)

	// Test action chunk completes with Restate failure: error is returned and redelivery retries reconciliation
	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("restate error"))
	}))
	defer ts500.Close()

	bRetry := VanadisBuild{ID: "b-act-retry", ThreadID: "123456789012345678", Objective: "test act retry", Status: "running"}
	cRetry := []VanadisChunk{
		{ID: "c-act-retry-1", BuildID: "b-act-retry", ChunkIndex: 0, Mode: "action", Title: "act 1", Status: "running"},
	}
	require.NoError(t, store.CreateBuild(ctx, bRetry, cRetry))
	srv500Wake := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts500.URL},
		store:      store,
		httpClient: ts500.Client(),
	}
	require.Error(t, srv500Wake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-retry-1",
		DispatchID:     "d-act-retry-1",
	}))
	cLoaded, _, err := store.GetChunk(ctx, "c-act-retry-1")
	require.NoError(t, err)
	assert.Equal(t, "completed", cLoaded.Status)
	// On redelivery with healthy Restate, reconciliation retries and succeeds for already-completed chunk
	require.NoError(t, srvWake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-retry-1",
		DispatchID:     "d-act-retry-1",
	}))

	require.NoError(t, srv.mutateBuildOnRestate(ctx, "b-restate", BuildMutationRequest{
		Action:  "chunk_merged",
		ChunkID: "c1",
	}))

	assert.Equal(t, []string{
		"/Build/b-restate/Reconcile/send",
		"/Build/b-restate/Reconcile/send",
		"/Build/b-act-single/Reconcile/send",
		"/Build/b-act-retry/Reconcile/send",
		"/Build/b-act-retry/Reconcile/send",
		"/Build/b-restate/Mutate/send",
	}, paths)

	// Error and no-op edge cases for ingress helpers
	var nilSrv *VanadisServer
	assert.Error(t, nilSrv.postRestateIngressJSON(ctx, "/test", nil))
	assert.Error(t, srv.reconcileBuildOnRestate(ctx, "", "created"))
	assert.Error(t, srv.mutateBuildOnRestate(ctx, "", BuildMutationRequest{Action: "chunk_merged"}))

	srv500 := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts500.URL},
		store:      store,
		httpClient: ts500.Client(),
	}
	assert.Error(t, srv500.reconcileBuildOnRestate(ctx, "b-restate", "created"))
	assert.Error(t, srv500.mutateBuildOnRestate(ctx, "b-restate", BuildMutationRequest{Action: "chunk_merged"}))
}

func TestRestateLaunchCreatedBuild_LocalAndActionPR(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryVanadisStore()

	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	bLocalLaunch := VanadisBuild{ID: "b-local-launch", ThreadID: "123456789012345678", Objective: "test local launch", Status: "pending"}
	cLocalLaunch := []VanadisChunk{
		{ID: "c-ll-0", BuildID: "b-local-launch", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR 0", Instructions: "do", Status: "pending"},
	}
	require.NoError(t, store.CreateBuild(ctx, bLocalLaunch, cLocalLaunch))
	published := make(chan ExecuteChunkArgs, 1)
	_, errSub := memBus.Subscribe(ctx, "aerial.jobs.vanadis.chunk", "local-launch-sub", func(_ context.Context, msg pubsub.Message) error {
		var args ExecuteChunkArgs
		_ = json.Unmarshal(msg.Data(), &args)
		published <- args
		return nil
	})
	require.NoError(t, errSub)
	srvNoRestate := &VanadisServer{store: store, bus: memBus}
	require.NoError(t, srvNoRestate.launchCreatedBuild(ctx, "b-local-launch"))
	select {
	case publishedArgs := <-published:
		assert.Equal(t, "b-local-launch", publishedArgs.BuildID)
		assert.Equal(t, "c-ll-0", publishedArgs.ChunkID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published chunk job")
	}
	cReserved, _, err := store.GetChunk(ctx, "c-ll-0")
	require.NoError(t, err)
	assert.Equal(t, "running", cReserved.Status)
}

func TestLocalLaunchedCache(t *testing.T) {
	t.Parallel()
	var nilS *VanadisServer
	assert.Nil(t, nilS.getLocalLaunched("b1"))
	nilS.setLocalLaunched("b1", "running", map[string]bool{"c1": true})

	s := &VanadisServer{}
	assert.Nil(t, s.getLocalLaunched("b1"))
	s.setLocalLaunched("b1", "running", map[string]bool{"c1": true, "c2": true})
	got := s.getLocalLaunched("b1")
	assert.Equal(t, map[string]bool{"c1": true, "c2": true}, got)
	got["c3"] = true
	assert.False(t, s.getLocalLaunched("b1")["c3"])

	s.setLocalLaunched("b1", "completed", got)
	assert.Nil(t, s.getLocalLaunched("b1"))
}

func TestReconstructLaunchedChunks(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	chunks := []VanadisChunk{
		{ID: "c0", ChunkIndex: 0, Status: "running", CreatedAt: now, UpdatedAt: now.Add(time.Second)},
		{ID: "c1", ChunkIndex: 1, Status: "running", CreatedAt: now, UpdatedAt: now},
		{ID: "c2", ChunkIndex: 2, Status: "pr_opened", PRNumber: 42},
		{ID: "c3", ChunkIndex: 3, Status: "pending"},
	}
	launched := reconstructLaunchedChunks(chunks)
	assert.True(t, launched["c0"])
	assert.True(t, launched["c1"])
	assert.True(t, launched["c2"])
	assert.False(t, launched["c3"])

	chunks2 := []VanadisChunk{
		{ID: "c0", ChunkIndex: 0, Status: "running", CreatedAt: now, UpdatedAt: now},
		{ID: "c1", ChunkIndex: 1, Status: "pending", PRNumber: 10},
	}
	launched2 := reconstructLaunchedChunks(chunks2)
	assert.True(t, launched2["c0"])
	assert.True(t, launched2["c1"])
}

func TestLoadRestateConfig(t *testing.T) {
	t.Parallel()
	tmpYAML := t.TempDir() + "/restate.yaml"
	require.NoError(t, os.WriteFile(tmpYAML, []byte("restate_port: \"9099\"\nrestate_ingress_url: \"http://restate:8080/\"\nrestate_admin_url: \"http://restate:9070/\"\nrestate_advertise_url: \"http://vanadis:9099/\"\n"), 0o600))
	rcfg := LoadRestateConfig(tmpYAML)
	assert.Equal(t, "9099", rcfg.RestatePort)
	assert.Equal(t, "http://restate:8080", rcfg.RestateIngressURL)
	assert.Equal(t, "http://restate:9070", rcfg.RestateAdminURL)
	assert.Equal(t, "http://vanadis:9099", rcfg.RestateAdvertiseURL)

	rcfgDefault := LoadRestateConfig("")
	assert.Equal(t, "9080", rcfgDefault.RestatePort)

	rcfgMissing := LoadRestateConfig(t.TempDir() + "/nonexistent.yaml")
	assert.Equal(t, "9080", rcfgMissing.RestatePort)

	tmpYAMLDisabled := t.TempDir() + "/disabled.yaml"
	require.NoError(t, os.WriteFile(tmpYAMLDisabled, []byte("restate_port: \"disabled\"\n"), 0o600))
	rcfgDis := LoadRestateConfig(tmpYAMLDisabled)
	assert.Equal(t, "disabled", rcfgDis.RestatePort)

	tmpYAMLBad := t.TempDir() + "/bad.yaml"
	require.NoError(t, os.WriteFile(tmpYAMLBad, []byte(":::invalid"), 0o600))
	rcfgBad := LoadRestateConfig(tmpYAMLBad)
	assert.Equal(t, "9080", rcfgBad.RestatePort)

	assert.Equal(t, "", cleanRestatePortEnv("http://localhost:9080"))
}

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

func TestBuildObject_DeriveReconcileChunkStates(t *testing.T) {
	t.Parallel()

	chunks := []VanadisChunk{
		{ID: "c-comp", Status: "completed"},
		{ID: "c-m-pr", Mode: "pr", Status: "merged"},
		{ID: "c-m-act", Mode: "action", Status: "merged"},
		{ID: "c-dep-pr", Mode: "pr", Status: "deploying"},
		{ID: "c-dep-act", Mode: "action", Status: "deploying"},
		{ID: "c-prop", Status: "pr_opened"},
		{ID: "c-cifail", Status: "ci_failed"},
		{ID: "c-conf", Status: "conflict"},
		{ID: "c-depfail", Mode: "pr", Status: "deploy_failed"},
		{ID: "c-code", Status: "coding"},
		{ID: "c-run-created-new", Status: "running"},
		{ID: "c-run-created-prior", Status: "running"},
		{ID: "c-run-created-pr", Status: "running", PRNumber: 123},
		{ID: "c-pend-new", Status: "pending"},
		{ID: "c-pend-prior", Status: "pending"},
	}

	priorLaunched := map[string]bool{
		"c-run-created-prior": true,
		"c-pend-prior":        true,
	}

	order, states := deriveReconcileChunkStates(chunks, priorLaunched)
	assert.Len(t, order, len(chunks))

	assert.True(t, states["c-comp"].launched)
	assert.True(t, states["c-comp"].codeReady)
	assert.True(t, states["c-comp"].runtimeReady)

	assert.True(t, states["c-m-pr"].launched)
	assert.True(t, states["c-m-pr"].codeReady)
	assert.False(t, states["c-m-pr"].runtimeReady)

	assert.True(t, states["c-m-act"].launched)
	assert.False(t, states["c-m-act"].codeReady)
	assert.False(t, states["c-m-act"].runtimeReady)

	assert.True(t, states["c-dep-pr"].codeReady)
	assert.False(t, states["c-dep-act"].codeReady)

	assert.True(t, states["c-prop"].launched)
	assert.False(t, states["c-prop"].codeReady)

	assert.True(t, states["c-cifail"].launched)
	assert.True(t, states["c-conf"].launched)
	assert.True(t, states["c-depfail"].launched)
	assert.True(t, states["c-depfail"].codeReady)
	assert.False(t, states["c-depfail"].runtimeReady)
	assert.True(t, states["c-code"].launched)

	// on "created", unlaunched running chunk without PRNumber is NOT launched yet
	assert.False(t, states["c-run-created-new"].launched)
	assert.True(t, states["c-run-created-prior"].launched)
	assert.True(t, states["c-run-created-pr"].launched)

	// pending chunks
	assert.False(t, states["c-pend-new"].launched)
	assert.True(t, states["c-pend-prior"].launched)
}

func TestBuildObject_ReconcileAndExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}
	rec := NewBuildObject(srv)

	// GetLaunched error
	rtErrGet := newFakeObjectRuntime("b1")
	rtErrGet.getLaunchedErr = errors.New("storage get error")
	resErrGet, err := rec.reconcileWithRuntime(rtErrGet)
	require.Error(t, err)
	assert.Equal(t, "failed", resErrGet.Status)

	// Step error
	rtErrStep := newFakeObjectRuntime("b1")
	rtErrStep.runStepErr = errors.New("step execution error")
	resErrStep, err := rec.reconcileWithRuntime(rtErrStep)
	require.Error(t, err)
	assert.Equal(t, "failed", resErrStep.Status)

	// Build not found returns terminal not_found and clears state
	rtNotFound := newFakeObjectRuntime("b-missing")
	resNotFound, err := rec.reconcileWithRuntime(rtNotFound)
	require.NoError(t, err)
	assert.Equal(t, "not_found", resNotFound.Status)
	assert.True(t, rtNotFound.cleared)

	// 2. Initial creation launch on "created"
	bCreate := VanadisBuild{ID: "b-create", ThreadID: "th-1", Objective: "test create", Status: "pending"}
	cCreate := []VanadisChunk{
		{ID: "c0", BuildID: "b-create", ChunkIndex: 0, Mode: "pr", Status: "running"},
		{ID: "c1", BuildID: "b-create", ChunkIndex: 1, Mode: "pr", Status: "pending", DependsOn: []string{"c0"}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bCreate, cCreate))

	rtCreate := newFakeObjectRuntime("b-create")
	resCreate, err := rec.reconcileWithRuntime(rtCreate)
	require.NoError(t, err)
	assert.Equal(t, "running", resCreate.Status)
	assert.Equal(t, []string{"c0"}, resCreate.LaunchedChunks)
	assert.True(t, rtCreate.launched["c0"])
	assert.False(t, rtCreate.cleared)

	// Verify build status was moved to "running"
	bLoaded, _, err := memStore.GetBuild(ctx, "b-create")
	require.NoError(t, err)
	assert.Equal(t, "running", bLoaded.Status)

	// 3. In-flight handover: running chunk already in Postgres does not double-launch on subsequent reconcile
	resHandover, err := rec.reconcileWithRuntime(rtCreate)
	require.NoError(t, err)
	assert.Equal(t, "running", resHandover.Status)
	assert.Empty(t, resHandover.LaunchedChunks)

	// 4. PR chunk merged unblocks dependent PR chunk, but build stays running until deploy_success completes it
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "c0", "merged"))
	resMerged, err := rec.reconcileWithRuntime(rtCreate)
	require.NoError(t, err)
	assert.Equal(t, "running", resMerged.Status)
	assert.Equal(t, []string{"c1"}, resMerged.LaunchedChunks)
	assert.True(t, rtCreate.launched["c1"])
	assert.False(t, rtCreate.cleared)

	// Complete both chunks -> completes build and clears state
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "c0", "completed"))
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "c1", "completed"))
	resComp, err := rec.reconcileWithRuntime(rtCreate)
	require.NoError(t, err)
	assert.Equal(t, "completed", resComp.Status)
	assert.True(t, rtCreate.cleared)

	// Reconcile already terminal build returns terminal status immediately
	resAlreadyTerminal, err := rec.reconcileWithRuntime(rtCreate)
	require.NoError(t, err)
	assert.Equal(t, "completed", resAlreadyTerminal.Status)
}

func TestBuildObject_EdgeCasesAndFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}
	rec := NewBuildObject(srv)

	// 5. Action chunk dependency: dependent chunk only unblocks on action chunk completed
	bAction := VanadisBuild{ID: "b-action", ThreadID: "th-act", Objective: "test action", Status: "running"}
	cAction := []VanadisChunk{
		{ID: "ca0", BuildID: "b-action", ChunkIndex: 0, Mode: "action", Status: "merged"},
		{ID: "ca1", BuildID: "b-action", ChunkIndex: 1, Mode: "pr", Status: "pending", DependsOn: []string{"ca0"}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bAction, cAction))
	rtAction := newFakeObjectRuntime("b-action")
	resActMerged, err := rec.reconcileWithRuntime(rtAction)
	require.NoError(t, err)
	// ca0 is action chunk in "merged", so codeReady=false; ca1 cannot launch
	assert.Empty(t, resActMerged.LaunchedChunks)

	// When ca0 completes, ca1 launches
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "ca0", "completed"))
	resActComp, err := rec.reconcileWithRuntime(rtAction)
	require.NoError(t, err)
	assert.Equal(t, []string{"ca1"}, resActComp.LaunchedChunks)

	// 6. Chunk failure terminates build with failed status, dispatches fail wake, and clears state
	bFail := VanadisBuild{ID: "b-fail-rec", ThreadID: "th-fail", Objective: "test fail", Status: "running"}
	cFail := []VanadisChunk{
		{ID: "cf0", BuildID: "b-fail-rec", ChunkIndex: 0, Mode: "pr", Status: "failed"},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bFail, cFail))
	rtFail := newFakeObjectRuntime("b-fail-rec")
	resFail, err := rec.reconcileWithRuntime(rtFail)
	require.NoError(t, err)
	assert.Equal(t, "failed", resFail.Status)
	assert.Contains(t, resFail.Error, "chunk cf0 failed")
	assert.True(t, rtFail.cleared)

	bFailLoaded, _, err := memStore.GetBuild(ctx, "b-fail-rec")
	require.NoError(t, err)
	assert.Equal(t, "failed", bFailLoaded.Status)

	// 7. Store failure branch in executeReconcileBuild
	memStore.getErr = errors.New("db disconnect")
	_, errStore := executeReconcileBuild(ctx, srv, "b-create", nil)
	assert.Error(t, errStore)
	memStore.getErr = nil

	// 7b. Pending build status update failure
	bPendingErr := VanadisBuild{ID: "b-pending-err", Status: "pending"}
	cPendingErr := []VanadisChunk{{ID: "c-pe0", BuildID: "b-pending-err", ChunkIndex: 0, Mode: "pr", Status: "pending"}}
	require.NoError(t, memStore.CreateBuild(ctx, bPendingErr, cPendingErr))
	memStore.updateBuildErr = errors.New("cannot update build to running")
	_, errPendingFail := executeReconcileBuild(ctx, srv, "b-pending-err", nil)
	assert.Error(t, errPendingFail)
	memStore.updateBuildErr = nil

	// 7c. Complete build update failure
	bCompErr := VanadisBuild{ID: "b-comp-err", Status: "running"}
	cCompErr := []VanadisChunk{{ID: "cce0", BuildID: "b-comp-err", ChunkIndex: 0, Mode: "action", Status: "completed"}}
	require.NoError(t, memStore.CreateBuild(ctx, bCompErr, cCompErr))
	memStore.updateBuildErr = errors.New("cannot complete build")
	_, errCompFail := executeReconcileBuild(ctx, srv, "b-comp-err", nil)
	assert.Error(t, errCompFail)
	memStore.updateBuildErr = nil

	// 8. Launch chunk error branch in launchReconcileEligibleChunks
	bLaunchErr := VanadisBuild{ID: "b-launch-err", ThreadID: "th-launch", Objective: "launch err", Status: "running"}
	cLaunchErr := []VanadisChunk{
		{ID: "cle0", BuildID: "b-launch-err", ChunkIndex: 0, Mode: "pr", Status: "pending"},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bLaunchErr, cLaunchErr))
	srvNoBus := &VanadisServer{store: memStore, bus: nil}
	_, errLaunchFail := executeReconcileBuild(ctx, srvNoBus, "b-launch-err", nil)
	assert.Error(t, errLaunchFail)

	// 9. Fail build dispatch error and update status error branches
	bDispatchErr := VanadisBuild{ID: "b-disp-err", ThreadID: "th-disp-err", Objective: "obj", Status: "running"}
	cDispatchErr := []VanadisChunk{{ID: "c-de0", BuildID: "b-disp-err", ChunkIndex: 0, Mode: "pr", Status: "failed"}}
	require.NoError(t, memStore.CreateBuild(ctx, bDispatchErr, cDispatchErr))
	srvErrBrain := &VanadisServer{
		cfg:   Config{BrainURL: "http://127.0.0.1:99999"}, // invalid brain port to trigger dErr != nil
		store: memStore,
		httpClient: &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("brain unavailable")
		})},
	}
	resFailBrain, errFailBrain := executeReconcileBuild(ctx, srvErrBrain, "b-disp-err", nil)
	require.NoError(t, errFailBrain)
	assert.Equal(t, "failed", resFailBrain.BuildStatus)

	// 10. Chunk failure in executeReconcileBuild with executeFailBuild error
	bFailReconcileErr := VanadisBuild{ID: "b-fail-rec-err", Status: "running"}
	cFailReconcileErr := []VanadisChunk{{ID: "c-fre0", BuildID: "b-fail-rec-err", Mode: "pr", Status: "failed"}}
	require.NoError(t, memStore.CreateBuild(ctx, bFailReconcileErr, cFailReconcileErr))
	memStore.updateBuildErr = errors.New("cannot update build to failed")
	_, errRecFailBuild := executeReconcileBuild(ctx, srv, "b-fail-rec-err", nil)
	assert.Error(t, errRecFailBuild)
	memStore.updateBuildErr = nil
}

func TestBuildObject_ReconcileAndMutate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}
	bo := NewBuildObject(srv)

	testBuildObjectRuntimeErrors(t, bo)

	// 1. Initial Reconcile without prior state: hydrations from Postgres and SetState persistence
	b1 := VanadisBuild{ID: "b-bo-1", ThreadID: "th-bo-1", Objective: "test bo", Status: "pending"}
	c1 := []VanadisChunk{
		{ID: "c-bo-0", BuildID: "b-bo-1", ChunkIndex: 0, Mode: "pr", Status: "running"},
		{ID: "c-bo-1", BuildID: "b-bo-1", ChunkIndex: 1, Mode: "pr", Status: "pending", DependsOn: []string{"c-bo-0"}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, b1, c1))

	rt1 := newFakeObjectRuntime("b-bo-1")
	res1, err1 := bo.reconcileWithRuntime(rt1)
	require.NoError(t, err1)
	assert.Equal(t, "running", res1.Status)
	assert.Equal(t, []string{"c-bo-0"}, res1.LaunchedChunks)
	assert.False(t, rt1.cleared)
	require.NotNil(t, rt1.state)
	assert.Equal(t, "running", rt1.state.Build.Status)
	assert.True(t, rt1.state.Launched["c-bo-0"])
	assert.True(t, rt1.launched["c-bo-0"])

	// 2. Mutate: chunk_merged
	resMerged, errMerged := bo.mutateWithRuntime(rt1, BuildMutationRequest{
		Action:  "chunk_merged",
		ChunkID: "c-bo-0",
	})
	require.NoError(t, errMerged)
	assert.Equal(t, "running", resMerged.Status)
	assert.Equal(t, []string{"c-bo-1"}, resMerged.LaunchedChunks)
	assert.True(t, rt1.launched["c-bo-1"])
	assert.True(t, rt1.state.Launched["c-bo-1"])
	c0Loaded, _, err := memStore.GetChunk(ctx, "c-bo-0")
	require.NoError(t, err)
	assert.Equal(t, "merged", c0Loaded.Status)

	// 3. Mutate: chunk_deploy_success
	resDep, errDep := bo.mutateWithRuntime(rt1, BuildMutationRequest{
		Action:  "chunk_deploy_success",
		ChunkID: "c-bo-0",
	})
	require.NoError(t, errDep)
	assert.Equal(t, "running", resDep.Status)
	c0AfterDep, _, err := memStore.GetChunk(ctx, "c-bo-0")
	require.NoError(t, err)
	assert.Equal(t, "completed", c0AfterDep.Status)

	// 4. Mutate: action_completed on action chunk
	bAct := VanadisBuild{ID: "b-bo-act", Status: "running"}
	cAct := []VanadisChunk{
		{ID: "c-act-0", BuildID: "b-bo-act", ChunkIndex: 0, Mode: "action", Status: "running"},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bAct, cAct))
	rtAct := newFakeObjectRuntime("b-bo-act")
	resAct, errAct := bo.mutateWithRuntime(rtAct, BuildMutationRequest{
		Action:  "action_completed",
		ChunkID: "c-act-0",
	})
	require.NoError(t, errAct)
	assert.Equal(t, "completed", resAct.Status)
	assert.True(t, rtAct.cleared)

	// 5. Mutate: chunk_failed
	bFail := VanadisBuild{ID: "b-bo-fail", Status: "running"}
	cFail := []VanadisChunk{
		{ID: "c-fail-0", BuildID: "b-bo-fail", ChunkIndex: 0, Mode: "pr", Status: "running"},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bFail, cFail))
	rtFail := newFakeObjectRuntime("b-bo-fail")
	resFail, errFail := bo.mutateWithRuntime(rtFail, BuildMutationRequest{
		Action:  "chunk_failed",
		ChunkID: "c-fail-0",
		Reason:  "compilation error",
	})
	require.NoError(t, errFail)
	assert.Equal(t, "failed", resFail.Status)
	assert.Contains(t, resFail.Error, "c-fail-0")
	assert.True(t, rtFail.cleared)

	// 6. Mutate: cancelled
	bCancel := VanadisBuild{ID: "b-bo-cancel", Status: "running"}
	cCancel := []VanadisChunk{
		{ID: "c-cancel-0", BuildID: "b-bo-cancel", ChunkIndex: 0, Mode: "pr", Status: "running"},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bCancel, cCancel))
	rtCancel := newFakeObjectRuntime("b-bo-cancel")
	resCancel, errCancel := bo.mutateWithRuntime(rtCancel, BuildMutationRequest{
		Action: "cancelled",
	})
	require.NoError(t, errCancel)
	assert.Equal(t, "cancelled", resCancel.Status)
	assert.True(t, rtCancel.cleared)
	bCancelLoaded, _, err := memStore.GetBuild(ctx, "b-bo-cancel")
	require.NoError(t, err)
	assert.Equal(t, "cancelled", bCancelLoaded.Status)

	testBuildMutationErrorBranches(t, srv, memStore)
}

func testBuildObjectRuntimeErrors(t *testing.T, bo *BuildObject) {
	t.Helper()
	// Runtime error cases on empty build ID
	rtEmpty := newFakeObjectRuntime("  ")
	_, errEmptyRec := bo.reconcileWithRuntime(rtEmpty)
	require.Error(t, errEmptyRec)
	_, errEmptyMut := bo.mutateWithRuntime(rtEmpty, BuildMutationRequest{Action: "chunk_merged"})
	require.Error(t, errEmptyMut)

	// Runtime error cases on GetState error
	rtErrState := newFakeObjectRuntime("b-err")
	rtErrState.getStateErr = errors.New("get state failed")
	resErrState, errErrState := bo.reconcileWithRuntime(rtErrState)
	require.Error(t, errErrState)
	assert.Equal(t, "failed", resErrState.Status)

	resErrMutState, errErrMutState := bo.mutateWithRuntime(rtErrState, BuildMutationRequest{Action: "chunk_merged"})
	require.Error(t, errErrMutState)
	assert.Equal(t, "failed", resErrMutState.Status)

	// Runtime error cases on GetLaunched error
	rtErrLaunched := newFakeObjectRuntime("b-err")
	rtErrLaunched.getLaunchedErr = errors.New("get launched failed")
	resErrLaunched, errErrLaunched := bo.reconcileWithRuntime(rtErrLaunched)
	require.Error(t, errErrLaunched)
	assert.Equal(t, "failed", resErrLaunched.Status)

	resErrMutLaunched, errErrMutLaunched := bo.mutateWithRuntime(rtErrLaunched, BuildMutationRequest{Action: "chunk_merged"})
	require.Error(t, errErrMutLaunched)
	assert.Equal(t, "failed", resErrMutLaunched.Status)

	// Step error cases
	rtErrStep := newFakeObjectRuntime("b-err")
	rtErrStep.runStepErr = errors.New("step run failed")
	resErrStep, errErrStep := bo.reconcileWithRuntime(rtErrStep)
	require.Error(t, errErrStep)
	assert.Equal(t, "failed", resErrStep.Status)

	resErrMutStep, errErrMutStep := bo.mutateWithRuntime(rtErrStep, BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"})
	require.Error(t, errErrMutStep)
	assert.Equal(t, "failed", resErrMutStep.Status)
}

func testBuildMutationErrorBranches(t *testing.T, srv *VanadisServer, memStore *memoryVanadisStore) {
	t.Helper()
	ctx := context.Background()

	// Cancel on already terminal build
	errTerminalCancel := executeBuildMutation(ctx, srv, "b-bo-cancel", BuildMutationRequest{Action: "cancelled"})
	require.NoError(t, errTerminalCancel)

	// Cancel on nonexistent build returns nil
	errMissingCancel := executeBuildMutation(ctx, srv, "b-nonexistent", BuildMutationRequest{Action: "cancelled"})
	require.NoError(t, errMissingCancel)

	tests := []struct {
		name        string
		req         BuildMutationRequest
		buildID     string
		wantErr     bool
		errContains string
	}{
		{
			name:        "unsupported action",
			req:         BuildMutationRequest{Action: "unknown_action"},
			buildID:     "b1",
			wantErr:     true,
			errContains: "unsupported mutation action",
		},
		{
			name:        "chunk_merged missing chunk_id",
			req:         BuildMutationRequest{Action: "chunk_merged", ChunkID: ""},
			buildID:     "b1",
			wantErr:     true,
			errContains: "chunk_id required",
		},
		{
			name:        "chunk_deploy_success missing chunk_id",
			req:         BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: ""},
			buildID:     "b1",
			wantErr:     true,
			errContains: "chunk_id required",
		},
		{
			name:        "action_completed missing chunk_id",
			req:         BuildMutationRequest{Action: "action_completed", ChunkID: ""},
			buildID:     "b1",
			wantErr:     true,
			errContains: "chunk_id required",
		},
		{
			name:        "chunk_failed missing chunk_id",
			req:         BuildMutationRequest{Action: "chunk_failed", ChunkID: ""},
			buildID:     "b1",
			wantErr:     true,
			errContains: "chunk_id required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := executeBuildMutation(ctx, srv, tc.buildID, tc.req)
			if tc.wantErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}

	// executeBuildMutation store errors
	var nilSrv *VanadisServer
	assert.Error(t, executeBuildMutation(ctx, nilSrv, "b1", BuildMutationRequest{Action: "cancelled"}))

	memStore.updateChunkErr = errors.New("update chunk db err")
	assert.Error(t, executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{Action: "chunk_merged", ChunkID: "c-bo-1"}))
	memStore.updateChunkErr = nil

	assert.Error(t, executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c-nonexistent"}))

	assert.Error(t, executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{Action: "action_completed", ChunkID: "c-nonexistent"}))

	assert.Error(t, executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{Action: "chunk_failed", ChunkID: "c-nonexistent"}))

	memStore.getErr = errors.New("get build db err")
	assert.Error(t, executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{Action: "cancelled"}))
	memStore.getErr = nil

	memStore.updateBuildErr = errors.New("update build db err")
	assert.Error(t, executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{Action: "cancelled"}))
	memStore.updateBuildErr = nil
}

func TestReconcileBuild(t *testing.T) {
	ctx := context.Background()
	var paths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"Accepted"}`))
	}))
	defer ts.Close()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{
		store:      memStore,
		bus:        memBus,
		restateCfg: RestateConfig{RestateIngressURL: ts.URL},
		httpClient: ts.Client(),
	}

	require.NoError(t, srv.reconcileBuild(ctx, "b-rec", "created"))

	assert.Equal(t, []string{
		"/Build/b-rec/Reconcile/send",
	}, paths)

	// Local reconciliation when Restate ingress is unconfigured
	srvLocal := &VanadisServer{
		store:      memStore,
		bus:        memBus,
		restateCfg: RestateConfig{RestateIngressURL: ""},
	}
	bLocal := VanadisBuild{ID: "b-local", Status: "running"}
	cLocal := []VanadisChunk{{ID: "cl0", BuildID: "b-local", ChunkIndex: 0, Mode: "action", Status: "completed"}}
	require.NoError(t, memStore.CreateBuild(ctx, bLocal, cLocal))
	require.NoError(t, srvLocal.reconcileBuild(ctx, "b-local", "deploy_success"))
	bFinished, _, err := memStore.GetBuild(ctx, "b-local")
	require.NoError(t, err)
	assert.Equal(t, "completed", bFinished.Status)

	// Error branches
	assert.Error(t, srvLocal.reconcileBuild(ctx, "", "deploy_success"))
	var nilSrv *VanadisServer
	assert.Error(t, nilSrv.reconcileBuild(ctx, "b-local", "deploy_success"))
}
