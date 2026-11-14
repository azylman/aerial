package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildWorkflow_Reflection(t *testing.T) {
	t.Parallel()

	srv := &VanadisServer{}
	wf := NewBuildWorkflow(srv)
	require.NotNil(t, wf)
	assert.Equal(t, "BuildWorkflow", wf.ServiceName())

	// restate.Reflect panics if handler signatures are invalid
	require.NotPanics(t, func() {
		def := restate.Reflect(wf)
		require.NotNil(t, def)
	})
}

func TestBuildWorkflow_Run_EmptyBuildID(t *testing.T) {
	t.Parallel()

	wf := NewBuildWorkflow(nil)
	res, err := wf.Run(nil, RunBuildWorkflowRequest{BuildID: ""})
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
		cfg: Config{RestatePort: "disabled"},
	}
	rServer, err := startRestateServer(context.Background(), srv)
	require.ErrorIs(t, err, ErrRestateDisabled)
	assert.Nil(t, rServer)

	srvMinusOne := &VanadisServer{
		cfg: Config{RestatePort: "-1"},
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

func TestApplyAwakeableResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      *workflowChunkRuntimeState
		eventType  string
		val        string
		inErr      error
		wantStatus string
		wantErr    bool
	}{
		{
			name:       "nil state returns error",
			state:      nil,
			eventType:  "merge",
			wantErr:    true,
			wantStatus: "",
		},
		{
			name:       "awakeable error resolves to failed",
			state:      &workflowChunkRuntimeState{},
			eventType:  "merge",
			inErr:      errors.New("timeout"),
			wantStatus: "failed",
			wantErr:    true,
		},
		{
			name:       "failed value resolves to failed",
			state:      &workflowChunkRuntimeState{},
			eventType:  "merge",
			val:        "failed",
			wantStatus: "failed",
			wantErr:    false,
		},
		{
			name:       "merge success updates codeReady",
			state:      &workflowChunkRuntimeState{},
			eventType:  "merge",
			val:        "merged",
			wantStatus: "merged",
			wantErr:    false,
		},
		{
			name:       "deploy success updates codeReady and runtimeReady",
			state:      &workflowChunkRuntimeState{},
			eventType:  "deploy",
			val:        "deploy_success",
			wantStatus: "completed",
			wantErr:    false,
		},
		{
			name:       "action success updates codeReady and runtimeReady",
			state:      &workflowChunkRuntimeState{},
			eventType:  "action",
			val:        "completed",
			wantStatus: "completed",
			wantErr:    false,
		},
		{
			name:       "unknown event type returns error",
			state:      &workflowChunkRuntimeState{},
			eventType:  "unknown",
			val:        "ok",
			wantStatus: "",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status, err := applyAwakeableResolution(tc.state, tc.eventType, tc.val, tc.inErr)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantStatus, status)

			if tc.state != nil && tc.wantStatus == "merged" {
				assert.True(t, tc.state.codeReady)
				assert.False(t, tc.state.runtimeReady)
			}
			if tc.state != nil && tc.wantStatus == "completed" {
				assert.True(t, tc.state.codeReady)
				assert.True(t, tc.state.runtimeReady)
			}
		})
	}
}

func TestWorkflowStateHelpers(t *testing.T) {
	t.Parallel()

	now := time.Now()
	chunks := []VanadisChunk{
		{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "Chunk 1", Status: "completed", StartedAt: now, CompletedAt: now},
		{ID: "c2", ChunkIndex: 1, Mode: "action", Repo: "", Title: "Chunk 2", Status: "running", StartedAt: now},
	}

	summaries := chunksToSummaries(chunks)
	require.Len(t, summaries, 2)
	assert.Equal(t, "c1", summaries[0].ID)
	assert.Equal(t, "completed", summaries[0].Status)
	assert.Equal(t, "c2", summaries[1].ID)
	assert.Equal(t, "running", summaries[1].Status)

	states := map[string]*workflowChunkRuntimeState{
		"c1": {runtimeReady: true},
		"c2": {runtimeReady: false},
		"c3": {runtimeReady: true},
	}
	assert.Equal(t, 2, countCompletedChunksFromStates(states))
	assert.False(t, isWorkflowBuildComplete([]string{"c1", "c2", "c3"}, states))

	states["c2"].runtimeReady = true
	assert.True(t, isWorkflowBuildComplete([]string{"c1", "c2", "c3"}, states))

	// Active running count
	states["c1"].launched = true
	states["c1"].codeReady = false
	states["c2"].launched = true
	states["c2"].codeReady = true
	assert.Equal(t, 1, countActiveRunningChunks([]string{"c1", "c2", "c3"}, states))
}

func TestInitializeWorkflowStates_And_TerminalResult(t *testing.T) {
	t.Parallel()

	chunks := []VanadisChunk{
		{ID: "c1", Mode: "pr", Status: "completed"},
		{ID: "c2", Mode: "pr", Status: "merged"},
		{ID: "c3", Mode: "action", Status: "pending"},
	}

	states, order := initializeWorkflowStates(chunks)
	require.Equal(t, []string{"c1", "c2", "c3"}, order)
	assert.True(t, states["c1"].codeReady)
	assert.True(t, states["c1"].runtimeReady)
	assert.True(t, states["c2"].codeReady)
	assert.False(t, states["c2"].runtimeReady)
	assert.False(t, states["c3"].launched)

	snap := &workflowBuildSnapshot{
		Build:  &VanadisBuild{ID: "b1", Status: "completed"},
		Chunks: chunks,
	}
	res := terminalWorkflowResult("b1", snap)
	assert.Equal(t, "b1", res.BuildID)
	assert.Equal(t, "completed", res.Status)
	assert.Equal(t, 3, res.TotalChunks)
	assert.Equal(t, 2, res.CompletedChunks)
}

func TestBuildWorkflowWaitFutures(t *testing.T) {
	t.Parallel()

	states := map[string]*workflowChunkRuntimeState{
		"c1": {chunk: VanadisChunk{Mode: "pr"}, launched: false},
		"c2": {chunk: VanadisChunk{Mode: "pr"}, launched: true, codeReady: true, runtimeReady: true},
		"c3": {chunk: VanadisChunk{Mode: "pr"}, launched: true, codeReady: false},
		"c4": {chunk: VanadisChunk{Mode: "pr"}, launched: true, codeReady: true, runtimeReady: false},
		"c5": {chunk: VanadisChunk{Mode: "action"}, launched: true, runtimeReady: false},
		"c6": {chunk: VanadisChunk{Mode: "action"}, launched: true, runtimeReady: true},
	}
	futs := buildWorkflowWaitFutures(nil, nil, []string{"c1", "c2", "c3", "c4", "c5", "c6"}, states)
	assert.Len(t, futs, 2)
}

func TestProcessChunkAwakeable_Unmatched(t *testing.T) {
	t.Parallel()

	states := map[string]*workflowChunkRuntimeState{
		"c1": {launched: false},
		"c2": {launched: true},
	}
	handled, err := processChunkAwakeable(nil, nil, "b1", nil, []string{"c1", "c2"}, states)
	assert.False(t, handled)
	assert.NoError(t, err)
}

func TestWorkflowExecutionHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// nil server / nil store
	assert.Error(t, executeCompleteWorkflowBuild(ctx, nil, "b1"))
	assert.Error(t, executeLaunchChunk(ctx, nil, "b1", VanadisChunk{}, "", ""))
	assert.Error(t, executeCancelWorkflowBuild(ctx, nil, "b1", "reason"))
	assert.Error(t, executeAwakeableStatusUpdate(ctx, nil, "b1", "c1", "Title", "merge", "merged"))
	_, err := fetchWorkflowSnapshot(ctx, nil, "b1")
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

	// fetchWorkflowSnapshot
	snap, err := fetchWorkflowSnapshot(ctx, srv, "b-wf")
	require.NoError(t, err)
	assert.Equal(t, "b-wf", snap.Build.ID)
	assert.Len(t, snap.Chunks, 2)

	// executeLaunchChunk
	err = executeLaunchChunk(ctx, srv, "b-wf", chunks[0], "m1", "d1")
	require.NoError(t, err)

	// executeAwakeableStatusUpdate success
	err = executeAwakeableStatusUpdate(ctx, srv, "b-wf", "c1", "c1", "merge", "merged")
	require.NoError(t, err)

	// executeAwakeableStatusUpdate failure
	err = executeAwakeableStatusUpdate(ctx, srv, "b-wf", "c1", "c1", "deploy", "failed")
	require.NoError(t, err)

	// executeCancelWorkflowBuild
	err = executeCancelWorkflowBuild(ctx, srv, "b-wf", "user cancelled")
	require.NoError(t, err)

	// executeCompleteWorkflowBuild
	err = executeCompleteWorkflowBuild(ctx, srv, "b-wf")
	require.NoError(t, err)
}

func TestBuildWorkflow_DirectExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: store, bus: memBus}
	wf := NewBuildWorkflow(srv)

	// 1. Cancel and MutateDAG with nil ctx
	assert.NoError(t, wf.Cancel(nil, CancelBuildWorkflowRequest{Reason: "user cancelled"}))
	assert.NoError(t, wf.Cancel(nil, CancelBuildWorkflowRequest{}))
	assert.NoError(t, wf.MutateDAG(nil, DAGMutationSignalRequest{Action: "append"}))
	assert.NoError(t, wf.MutateDAG(nil, DAGMutationSignalRequest{}))

	// 2. Run with build not found
	res, err := wf.Run(nil, RunBuildWorkflowRequest{BuildID: "nonexistent"})
	assert.Error(t, err)
	assert.Equal(t, "failed", res.Status)

	// 3. Run with already terminal build
	bTerm := VanadisBuild{ID: "b-term", Status: "completed"}
	cTerm := []VanadisChunk{{ID: "c1", BuildID: "b-term", Status: "completed"}}
	require.NoError(t, store.CreateBuild(ctx, bTerm, cTerm))
	resTerm, err := wf.Run(nil, RunBuildWorkflowRequest{BuildID: "b-term"})
	require.NoError(t, err)
	assert.Equal(t, "completed", resTerm.Status)

	// 4. Run with pending build where all chunks are completed
	bComp := VanadisBuild{ID: "b-comp", Status: "pending"}
	cComp := []VanadisChunk{
		{ID: "c1", BuildID: "b-comp", ChunkIndex: 0, Mode: "pr", Status: "completed"},
		{ID: "c2", BuildID: "b-comp", ChunkIndex: 1, Mode: "action", Status: "completed"},
	}
	require.NoError(t, store.CreateBuild(ctx, bComp, cComp))
	resComp, err := wf.Run(nil, RunBuildWorkflowRequest{BuildID: "b-comp"})
	require.NoError(t, err)
	assert.Equal(t, "completed", resComp.Status)
	assert.Equal(t, 2, resComp.CompletedChunks)

	// 5. Run with pending build that has eligible chunks to launch
	bRun := VanadisBuild{ID: "b-run", Status: "pending"}
	cRun := []VanadisChunk{
		{ID: "c-pr", BuildID: "b-run", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR Chunk", Status: "pending"},
		{ID: "c-act", BuildID: "b-run", ChunkIndex: 1, Mode: "action", Title: "Action Chunk", Status: "pending"},
	}
	require.NoError(t, store.CreateBuild(ctx, bRun, cRun))
	resRun, err := wf.Run(nil, RunBuildWorkflowRequest{BuildID: "b-run"})
	require.NoError(t, err)
	assert.Equal(t, "running", resRun.Status)

	// launchActionChunk directly
	csAction := &workflowChunkRuntimeState{chunk: cRun[1]}
	launchActionChunk(nil, srv, "b-run", csAction)
	assert.True(t, csAction.launched)

	// 6. cancelWorkflowBuild with nil ctx
	order := make([]string, 0, 3)
	order = append(order, "c-pr", "c-act")
	states := map[string]*workflowChunkRuntimeState{
		"c-pr":  {chunk: cRun[0], runtimeReady: true},
		"c-act": {chunk: cRun[1], runtimeReady: false},
	}
	resCancel, err := cancelWorkflowBuild(nil, srv, "b-run", nil, order, states)
	require.NoError(t, err)
	assert.Equal(t, "cancelled", resCancel.Status)
	assert.Equal(t, 1, resCancel.CompletedChunks)

	// 7. reloadWorkflowDAG with nil ctx
	order = append(order, "c-del")
	states["c-del"] = &workflowChunkRuntimeState{chunk: VanadisChunk{ID: "c-del"}}
	prom := reloadWorkflowDAG(nil, srv, "b-run", &order, states, 1)
	assert.Nil(t, prom)
	assert.NotContains(t, states, "c-del")
	assert.NotContains(t, order, "c-del")

	// 8. handleAwakeableOutcome with nil ctx
	cs := states["c-act"]
	st, err := handleAwakeableOutcome(nil, srv, "b-run", cs, "action", "completed", nil)
	require.NoError(t, err)
	assert.Equal(t, "completed", st)

	stFail, errFail := handleAwakeableOutcome(nil, srv, "b-run", cs, "action", "failed", errors.New("boom"))
	assert.Error(t, errFail)
	assert.Equal(t, "failed", stFail)

	// 9. launchPRChunk with nil ctx
	csPR := &workflowChunkRuntimeState{chunk: cRun[0]}
	launchPRChunk(nil, srv, "b-run", csPR)
	assert.True(t, csPR.launched)

	// 10. processChunkAwakeable with nil firstDone
	done, err := processChunkAwakeable(nil, srv, "b-run", nil, order, states)
	assert.False(t, done)
	assert.NoError(t, err)

	ensureBuildRunning(nil, srv, "b-run", "pending")
	ensureBuildRunning(nil, srv, "b-run", "running")

	snapLoaded, lErr := loadWorkflowSnapshot(nil, srv, "b-run")
	assert.NoError(t, lErr)
	assert.NotNil(t, snapLoaded)

	stFailVal, errFailVal := handleAwakeableOutcome(nil, srv, "b-run", cs, "action", "failed", nil)
	assert.NoError(t, errFailVal)
	assert.Equal(t, "failed", stFailVal)

	// 11. buildWorkflowWaitFutures with various states
	wfFuts := buildWorkflowWaitFutures(nil, nil, order, states)
	assert.Equal(t, 2, len(wfFuts)) // only nil cancel & dag promises
}

func TestBuildWorkflow_ApplyAwakeableResolution(t *testing.T) {
	t.Parallel()

	// nil chunk state
	_, err := applyAwakeableResolution(nil, "merge", "ok", nil)
	assert.Error(t, err)

	// error or failed / cancelled
	cs := &workflowChunkRuntimeState{}
	st, err := applyAwakeableResolution(cs, "merge", "ok", errors.New("fail"))
	assert.Equal(t, "failed", st)
	assert.Error(t, err)

	st, err = applyAwakeableResolution(cs, "merge", "cancelled", nil)
	assert.Equal(t, "failed", st)
	assert.NoError(t, err)

	st, err = applyAwakeableResolution(cs, "merge", "failed", nil)
	assert.Equal(t, "failed", st)
	assert.NoError(t, err)

	// merge event
	csMerge := &workflowChunkRuntimeState{}
	st, err = applyAwakeableResolution(csMerge, "merge", "ok", nil)
	assert.NoError(t, err)
	assert.Equal(t, "merged", st)
	assert.True(t, csMerge.codeReady)
	assert.False(t, csMerge.runtimeReady)

	// deploy event
	csDeploy := &workflowChunkRuntimeState{}
	st, err = applyAwakeableResolution(csDeploy, "deploy", "ok", nil)
	assert.NoError(t, err)
	assert.Equal(t, "completed", st)
	assert.True(t, csDeploy.codeReady)
	assert.True(t, csDeploy.runtimeReady)

	// action event
	csAction := &workflowChunkRuntimeState{}
	st, err = applyAwakeableResolution(csAction, "action", "ok", nil)
	assert.NoError(t, err)
	assert.Equal(t, "completed", st)
	assert.True(t, csAction.codeReady)
	assert.True(t, csAction.runtimeReady)

	// unknown event
	csUnknown := &workflowChunkRuntimeState{}
	_, err = applyAwakeableResolution(csUnknown, "bogus", "ok", nil)
	assert.Error(t, err)
}

func TestBuildWorkflow_ComputeEligibleChunksToLaunch(t *testing.T) {
	t.Parallel()

	c1 := VanadisChunk{ID: "c1", Mode: "pr"}
	c2 := VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}}
	c3 := VanadisChunk{ID: "c3", Mode: "action", DependsOn: []string{"c1"}}
	c4 := VanadisChunk{ID: "c4", Mode: "pr", DependsOn: []string{"missing"}}

	states := map[string]*workflowChunkRuntimeState{
		"c1": {chunk: c1, launched: true, codeReady: true, runtimeReady: false},
		"c2": {chunk: c2, launched: false},
		"c3": {chunk: c3, launched: false},
		"c4": {chunk: c4, launched: false},
	}
	order := []string{"c1", "c2", "c3", "c4"}

	// active count is 0 because c1 is launched AND codeReady!
	// c2 is eligible because c1 is codeReady and c2 mode is "pr"
	// c3 is NOT eligible because c1 is NOT runtimeReady and c3 mode is "action"
	// c4 is NOT eligible because "missing" is not in states
	eligible := computeEligibleChunksToLaunch(order, states, 1)
	assert.Equal(t, []string{"c2"}, eligible)

	// maxParallel 0 returns nil
	assert.Nil(t, computeEligibleChunksToLaunch(order, states, 0))

	// if c1 is launched and not codeReady, active count is 1
	states["c1"].codeReady = false
	assert.Nil(t, computeEligibleChunksToLaunch(order, states, 1)) // 1 - 1 = 0
}

func TestBuildWorkflow_ExecuteHelpers_NilStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	assert.Error(t, executeAwakeableStatusUpdate(ctx, nil, "b1", "c1", "title", "merge", "merged"))
	assert.Error(t, executeCancelWorkflowBuild(ctx, nil, "b1", "reason"))

	// Test store update error logging branches
	memStoreErr := newMemoryVanadisStore()
	memStoreErr.updateChunkErr = errors.New("chunk update err")
	memStoreErr.updateBuildErr = errors.New("build update err")
	srvStoreErr := &VanadisServer{store: memStoreErr}

	// Status update "failed" with store errors
	assert.NoError(t, executeAwakeableStatusUpdate(ctx, srvStoreErr, "b1", "c1", "title", "merge", "failed"))

	// Status update "merged" with store error
	assert.NoError(t, executeAwakeableStatusUpdate(ctx, srvStoreErr, "b1", "c1", "title", "merge", "merged"))

	// Cancel workflow with store error
	assert.NoError(t, executeCancelWorkflowBuild(ctx, srvStoreErr, "b1", "reason"))

	_, err := startRestateServer(ctx, nil)
	assert.Error(t, err)
}

func TestBuildWorkflow_StoreErrorHandling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memStore.updateBuildErr = errors.New("simulated update build err")
	memStore.updateChunkErr = errors.New("simulated update chunk err")
	srv := &VanadisServer{store: memStore}

	// 1. ensureBuildRunning error branch
	ensureBuildRunning(nil, srv, "b1", "pending")
	ensureBuildRunning(nil, srv, "b1", "running")

	// 2. completeWorkflowBuild error branch
	resComp, errComp := completeWorkflowBuild(nil, srv, "b1", nil, nil)
	assert.Error(t, errComp)
	assert.Equal(t, "failed", resComp.Status)

	// 3. executeLaunchChunk error branches
	c := VanadisChunk{ID: "c1", ChunkIndex: 0}
	errLaunch := executeLaunchChunk(ctx, srv, "b1", c, "m1", "d1")
	assert.Error(t, errLaunch)

	// 4. launchPRChunk & launchActionChunk with executeLaunchChunk failure
	csPR := &workflowChunkRuntimeState{chunk: c}
	launchPRChunk(nil, srv, "b1", csPR)
	assert.True(t, csPR.launched)

	csAct := &workflowChunkRuntimeState{chunk: c}
	launchActionChunk(nil, srv, "b1", csAct)
	assert.True(t, csAct.launched)
}
