package main

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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestBuildWorkflow_Reflection(t *testing.T) {
	t.Parallel()

	srv := &VanadisServer{}
	wf := NewBuildWorkflow(srv)
	require.NotNil(t, wf)
	assert.Equal(t, "BuildWorkflow", wf.ServiceName())

	rec := NewBuildReconciler(srv)
	assert.Equal(t, "BuildReconciler", rec.ServiceName())

	// Exercise runtime adapter methods on concrete restateSharedRuntime, restateWorkflowRuntime, and restateObjectRuntime types
	var sr workflowSharedRuntime = restateSharedRuntime{}
	assert.NotNil(t, sr)
	var wr workflowRuntime = restateWorkflowRuntime{}
	assert.NotNil(t, wr)

	// restate.Reflect panics if handler signatures are invalid
	require.NotPanics(t, func() {
		def := restate.Reflect(wf)
		require.NotNil(t, def)
		_ = restate.Reflect(rec)
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
			name:       "cancelled value resolves to failed",
			state:      &workflowChunkRuntimeState{},
			eventType:  "merge",
			val:        "cancelled",
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

func TestSelectChunkWaitSlot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state *workflowChunkRuntimeState
		want  string
	}{
		{
			name:  "nil state returns empty",
			state: nil,
			want:  "",
		},
		{
			name:  "unlaunched pr chunk returns empty",
			state: &workflowChunkRuntimeState{chunk: VanadisChunk{Mode: "pr"}, launched: false},
			want:  "",
		},
		{
			name:  "completed pr chunk returns empty",
			state: &workflowChunkRuntimeState{chunk: VanadisChunk{Mode: "pr"}, launched: true, codeReady: true, runtimeReady: true},
			want:  "",
		},
		{
			name:  "launched pr chunk waiting on merge returns merge",
			state: &workflowChunkRuntimeState{chunk: VanadisChunk{Mode: "pr"}, launched: true, codeReady: false},
			want:  "merge",
		},
		{
			name:  "merged pr chunk waiting on deploy returns deploy",
			state: &workflowChunkRuntimeState{chunk: VanadisChunk{Mode: "pr"}, launched: true, codeReady: true, runtimeReady: false},
			want:  "deploy",
		},
		{
			name:  "launched action chunk waiting on completion returns action",
			state: &workflowChunkRuntimeState{chunk: VanadisChunk{Mode: "action"}, launched: true, runtimeReady: false},
			want:  "action",
		},
		{
			name:  "completed action chunk returns empty",
			state: &workflowChunkRuntimeState{chunk: VanadisChunk{Mode: "action"}, launched: true, runtimeReady: true},
			want:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, selectChunkWaitSlot(tc.state))
		})
	}
}

func TestReconcileWorkflowDAGSnapshot(t *testing.T) {
	t.Parallel()

	order := []string{"c1", "c-del"}
	states := map[string]*workflowChunkRuntimeState{
		"c1":    {chunk: VanadisChunk{ID: "c1", Title: "old"}, launched: true, codeReady: true},
		"c-del": {chunk: VanadisChunk{ID: "c-del", Title: "removed"}},
	}
	reconcileWorkflowDAGSnapshot(nil, &order, states)
	assert.Equal(t, []string{"c1", "c-del"}, order)

	snap := &workflowBuildSnapshot{
		Chunks: []VanadisChunk{
			{ID: "c1", Title: "updated"},
			{ID: "c2", Title: "added"},
		},
	}
	reconcileWorkflowDAGSnapshot(snap, &order, states)
	assert.Equal(t, []string{"c1", "c2"}, order)
	assert.NotContains(t, states, "c-del")
	assert.Equal(t, "updated", states["c1"].chunk.Title)
	assert.True(t, states["c1"].codeReady)
	assert.Equal(t, "added", states["c2"].chunk.Title)
}

func TestProcessChunkAwakeable_Unmatched(t *testing.T) {
	t.Parallel()

	states := map[string]*workflowChunkRuntimeState{
		"c1": {launched: false},
		"c2": {
			launched:        true,
			mergeAwakeable:  &fakeWorkflowAwakeable{id: "awk-m"},
			deployAwakeable: &fakeWorkflowAwakeable{id: "awk-d"},
		},
		"c3": {
			launched:        true,
			actionAwakeable: &fakeWorkflowAwakeable{id: "awk-a"},
		},
	}
	handled, err := processChunkAwakeable(nil, nil, "b1", nil, []string{"c1", "c2", "c3"}, states)
	assert.False(t, handled)
	assert.NoError(t, err)

	unmatched := &fakeWorkflowPromise{name: "other"}
	handled, err = processChunkAwakeable(nil, nil, "b1", unmatched, []string{"c1", "c2", "c3"}, states)
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

	// executeLaunchChunk persists awakeable IDs and enqueues execute chunk
	err = executeLaunchChunk(ctx, srv, "b-wf", chunks[0], "m1", "d1")
	require.NoError(t, err)
	gotChunk, _, err := store.GetChunk(ctx, "c1")
	require.NoError(t, err)
	assert.Equal(t, "m1", gotChunk.MergeAwakeableID)
	assert.Equal(t, "d1", gotChunk.DeployAwakeableID)
	assert.Equal(t, "running", gotChunk.Status, "enqueued chunk must be reserved in running status")

	// executeLaunchChunk on already opened PR does not enqueue execute chunk
	srv.bus = nil
	chunks[0].Status = "pr_opened"
	require.NoError(t, executeLaunchChunk(ctx, srv, "b-wf", chunks[0], "m2", "d2"))
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
		{ID: "c1", BuildID: "b-restate", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "c1", Instructions: "do c1", Status: "pr_opened", PRNumber: 42, MergeAwakeableID: "awk-m1", DeployAwakeableID: "awk-d1"},
		{ID: "c2", BuildID: "b-restate", ChunkIndex: 1, Mode: "action", Title: "c2", Instructions: "do c2", Status: "completed", DeployAwakeableID: "awk-a2"},
		{ID: "c3", BuildID: "b-restate", ChunkIndex: 2, Mode: "action", Title: "c3", Instructions: "do c3", Status: "failed", DeployAwakeableID: "awk-a3"},
	}
	require.NoError(t, store.CreateBuild(ctx, b, chunks))

	srv := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts.URL},
		store:      store,
		httpClient: ts.Client(),
	}

	require.NoError(t, srv.launchCreatedBuild(ctx, "b-restate"))
	require.NoError(t, srv.launchBuildOnRestate(ctx, "b-restate"))
	require.NoError(t, srv.cancelRestateBuildWorkflow(ctx, "b-restate", "test cancel"))
	require.NoError(t, srv.notifyRestateDAGMutation(ctx, "b-restate", "append_chunks"))

	assert.NoError(t, srv.notifyRestateActionChunkCompleted(ctx, "c2"))
	assert.NoError(t, srv.notifyRestateActionChunkCompleted(ctx, "c3"), "failed action chunk must not be notified as completed")
	srv.notifyRestateChunkFailed(ctx, "c1")

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
		{ID: "c-act-single", BuildID: "b-act-single", ChunkIndex: 0, Mode: "action", Title: "act 1", Status: "running", DeployAwakeableID: "awk-act-single"},
	}
	require.NoError(t, store.CreateBuild(ctx, bAct, cAct))
	require.NoError(t, srvWake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-single",
		DispatchID:     "d-act-1",
	}))
	assert.NotEmpty(t, wakePromptSent)

	// Test action chunk completes with Restate failure: error is returned and redelivery retries reconciliation
	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("restate error"))
	}))
	defer ts500.Close()

	bRetry := VanadisBuild{ID: "b-act-retry", ThreadID: "123456789012345678", Objective: "test act retry", Status: "running"}
	cRetry := []VanadisChunk{
		{ID: "c-act-retry-1", BuildID: "b-act-retry", ChunkIndex: 0, Mode: "action", Title: "act 1", Status: "running", DeployAwakeableID: "awk-act-retry"},
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

	assert.Equal(t, []string{
		"/BuildWorkflow/b-restate/Run/send",
		"/BuildWorkflow/b-restate/Run/send",
		"/BuildWorkflow/b-restate/Cancel/send",
		"/BuildWorkflow/b-restate/MutateDAG/send",
		"/restate/awakeables/awk-a2/resolve",
		"/restate/awakeables/awk-m1/resolve",
		"/restate/awakeables/awk-d1/resolve",
		"/restate/awakeables/awk-act-single/resolve",
		"/restate/awakeables/awk-act-retry/resolve",
	}, paths)

	// Error and no-op edge cases for ingress helpers
	var nilSrv *VanadisServer
	assert.Error(t, nilSrv.postRestateIngressJSON(ctx, "/test", nil))
	assert.Error(t, srv.startRestateBuildWorkflow(ctx, ""))
	assert.Error(t, srv.reconcileBuildOnRestate(ctx, "", "created"))
	assert.NoError(t, srv.cancelRestateBuildWorkflow(ctx, "", "r"))
	assert.NoError(t, srv.notifyRestateDAGMutation(ctx, "", "a"))
	assert.NoError(t, srv.resolveRestateAwakeable(ctx, "", "v"))
	assert.NoError(t, srv.notifyRestateActionChunkCompleted(ctx, ""))
	srv.notifyRestateChunkFailed(ctx, "")
	srv.notifyRestateChunkFailed(ctx, "nonexistent")

	srv500 := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts500.URL},
		store:      store,
		httpClient: ts500.Client(),
	}
	assert.Error(t, srv500.startRestateBuildWorkflow(ctx, "b-restate"))
	assert.Error(t, srv500.notifyRestateActionChunkCompleted(ctx, "c2"))
	srv500.notifyRestateChunkFailed(ctx, "c1")
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

	bActPR := VanadisBuild{ID: "b-act-pr", Objective: "act pr", Status: "running"}
	cActPR := []VanadisChunk{
		{ID: "c-act-pr", BuildID: "b-act-pr", ChunkIndex: 0, Mode: "action", Status: "completed", PRNumber: 50},
	}
	require.NoError(t, store.CreateBuild(ctx, bActPR, cActPR))
	assert.NoError(t, srvNoRestate.notifyRestateActionChunkCompleted(ctx, "c-act-pr"))
	assert.NoError(t, srvNoRestate.notifyRestateActionChunkCompleted(ctx, "nonexistent-chunk"))
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

type fakeWorkflowPromise struct {
	name       string
	val        string
	err        restate.TerminalError
	resolveErr restate.TerminalError
}

func (p *fakeWorkflowPromise) Result() (string, restate.TerminalError) { return p.val, p.err }
func (p *fakeWorkflowPromise) Resolve(v string) restate.TerminalError {
	if p.resolveErr != nil {
		return p.resolveErr
	}
	p.val = v
	return nil
}

type fakeWorkflowAwakeable struct {
	id  string
	val string
	err restate.TerminalError
}

func (a *fakeWorkflowAwakeable) Result() (string, restate.TerminalError) { return a.val, a.err }

type fakeWorkflowRuntime struct {
	promises   map[string]*fakeWorkflowPromise
	awakeables []*fakeWorkflowAwakeable
	waitStep   func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error)
	stepCount  int
}

func newFakeWorkflowRuntime(waitStep func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error)) *fakeWorkflowRuntime {
	return &fakeWorkflowRuntime{
		promises: make(map[string]*fakeWorkflowPromise),
		waitStep: waitStep,
	}
}

func (r *fakeWorkflowRuntime) Promise(name string) workflowPromise {
	if p, ok := r.promises[name]; ok {
		return p
	}
	p := &fakeWorkflowPromise{name: name}
	r.promises[name] = p
	return p
}

func (r *fakeWorkflowRuntime) Awakeable() (string, workflowAwakeable) {
	awk := &fakeWorkflowAwakeable{id: fmt.Sprintf("awk-%d", len(r.awakeables)+1)}
	r.awakeables = append(r.awakeables, awk)
	return awk.id, awk
}

func (r *fakeWorkflowRuntime) Run(fn func(context.Context) error) restate.TerminalError {
	if err := fn(context.Background()); err != nil {
		return restate.TerminalErrorf("%v", err)
	}
	return nil
}

func (r *fakeWorkflowRuntime) RunSnapshot(srv *VanadisServer, buildID string) (*workflowBuildSnapshot, restate.TerminalError) {
	snap, err := fetchWorkflowSnapshot(context.Background(), srv, buildID)
	if err != nil {
		return nil, restate.TerminalErrorf("%v", err)
	}
	return snap, nil
}

func (r *fakeWorkflowRuntime) WaitFirst(futures ...any) (any, error) {
	r.stepCount++
	return r.waitStep(r.stepCount, r, futures)
}

func TestBuildWorkflow_StateMachineWithRuntime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: store, bus: memBus}
	wf := NewBuildWorkflow(srv)

	// 1. Shared signal handlers (cancelWithRuntime & mutateDAGWithRuntime)
	rtSignals := newFakeWorkflowRuntime(nil)
	require.NoError(t, wf.cancelWithRuntime(rtSignals, CancelBuildWorkflowRequest{}))
	assert.Equal(t, "cancelled", rtSignals.promises["cancel"].val)

	dagPromise, ok := rtSignals.Promise("dag_mutation").(*fakeWorkflowPromise)
	require.Truef(t, ok, "dag_mutation promise is %T, want *fakeWorkflowPromise", rtSignals.Promise("dag_mutation"))
	dagPromise.resolveErr = restate.TerminalErrorf("already resolved")
	require.NoError(t, wf.mutateDAGWithRuntime(rtSignals, DAGMutationSignalRequest{}))
	assert.Equal(t, "mutated", rtSignals.promises["dag_mutation_1"].val)

	// 2. Full DAG workflow: PR chunk (merge -> deploy) + DAG mutation reload + dependent Action chunk -> completed
	b := VanadisBuild{ID: "b-dag", ThreadID: "123456789012345678", Objective: "dag run", Status: "pending"}
	chunks := []VanadisChunk{
		{ID: "c1", BuildID: "b-dag", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR 1", Instructions: "do c1", Status: "pending"},
		{ID: "c2", BuildID: "b-dag", ChunkIndex: 1, Mode: "action", Title: "Action 2", Instructions: "do c2", Status: "pending", DependsOn: []string{"c1"}},
	}
	require.NoError(t, store.CreateBuild(ctx, b, chunks))

	rtDAG := newFakeWorkflowRuntime(func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error) {
		switch step {
		case 1:
			// Trigger DAG mutation reload first
			return rt.promises["dag_mutation"], nil
		case 2:
			// Resolve PR chunk merge awakeable
			rt.awakeables[0].val = "merged"
			return rt.awakeables[0], nil
		case 3:
			// Resolve PR chunk deploy awakeable
			rt.awakeables[1].val = "deploy_success"
			return rt.awakeables[1], nil
		case 4:
			// Resolve dependent Action chunk awakeable
			rt.awakeables[2].val = "completed"
			return rt.awakeables[2], nil
		default:
			return nil, errors.New("unexpected step")
		}
	})

	res, err := wf.runWithRuntime(rtDAG, RunBuildWorkflowRequest{BuildID: "b-dag"})
	require.NoError(t, err)
	assert.Equal(t, "completed", res.Status)
	assert.Equal(t, 2, res.CompletedChunks)

	// 3. Cancellation signal mid-flight
	bCancel := VanadisBuild{ID: "b-canc", ThreadID: "123456789012345678", Objective: "cancel run", Status: "running"}
	cCancel := []VanadisChunk{
		{ID: "cc1", BuildID: "b-canc", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR", Instructions: "do", Status: "pending"},
	}
	require.NoError(t, store.CreateBuild(ctx, bCancel, cCancel))

	rtCancel := newFakeWorkflowRuntime(func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error) {
		rt.promises["cancel"].val = "aborted by user"
		return rt.promises["cancel"], nil
	})
	resCancel, err := wf.runWithRuntime(rtCancel, RunBuildWorkflowRequest{BuildID: "b-canc"})
	require.NoError(t, err)
	assert.Equal(t, "cancelled", resCancel.Status)

	// 4. Chunk awakeable failure terminates workflow with failed status
	bFail := VanadisBuild{ID: "b-fail-wf", ThreadID: "123456789012345678", Objective: "fail run", Status: "running"}
	cFail := []VanadisChunk{
		{ID: "cf1", BuildID: "b-fail-wf", ChunkIndex: 0, Mode: "action", Title: "Act", Instructions: "do", Status: "pending"},
	}
	require.NoError(t, store.CreateBuild(ctx, bFail, cFail))

	rtFail := newFakeWorkflowRuntime(func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error) {
		rt.awakeables[0].val = "failed"
		return rt.awakeables[0], nil
	})
	resFail, err := wf.runWithRuntime(rtFail, RunBuildWorkflowRequest{BuildID: "b-fail-wf"})
	require.Error(t, err)
	assert.Equal(t, "failed", resFail.Status)

	// 5. PR -> PR chain driven through PR lifecycle events and awakeable resolver
	var rtPRChain *fakeWorkflowRuntime
	tsIngress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if after, ok := strings.CutPrefix(r.URL.Path, "/restate/awakeables/"); ok {
			awkID := strings.TrimSuffix(after, "/resolve")
			var resolvedVal string
			_ = json.NewDecoder(r.Body).Decode(&resolvedVal)
			for _, awk := range rtPRChain.awakeables {
				if awk.id == awkID {
					awk.val = resolvedVal
				}
			}
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer tsIngress.Close()

	srv.restateCfg.RestateIngressURL = tsIngress.URL
	srv.httpClient = tsIngress.Client()

	bPRChain := VanadisBuild{ID: "b-pr-chain", ThreadID: "123456789012345678", Objective: "pr->pr chain", Status: "pending"}
	cPRChain := []VanadisChunk{
		{ID: "c-pr1", BuildID: "b-pr-chain", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR 1", Instructions: "do pr1", Status: "pending", PRNumber: 501, TargetFiles: []string{"a.go"}},
		{ID: "c-pr2", BuildID: "b-pr-chain", ChunkIndex: 1, Mode: "pr", Repo: "azylman/aerial", Title: "PR 2", Instructions: "do pr2", Status: "pending", PRNumber: 502, DependsOn: []string{"c-pr1"}, TargetFiles: []string{"b.go"}},
	}
	require.NoError(t, store.CreateBuild(ctx, bPRChain, cPRChain))

	postAdvancePR := func(prNum int, event string) error {
		return srv.ProcessPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:    event,
			Repo:     "azylman/aerial",
			PRNumber: prNum,
		})
	}

	rtPRChain = newFakeWorkflowRuntime(func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error) {
		switch step {
		case 1:
			errAdv1 := postAdvancePR(501, "merged")
			assert.NoError(t, errAdv1)
		case 2:
			errAdv2 := postAdvancePR(501, "deploy_success")
			assert.NoError(t, errAdv2)
		case 3:
			errAdv3 := postAdvancePR(502, "merged")
			assert.NoError(t, errAdv3)
		case 4:
			errAdv4 := postAdvancePR(502, "deploy_success")
			assert.NoError(t, errAdv4)
		}
		for _, f := range futures {
			if awk, ok := f.(*fakeWorkflowAwakeable); ok && awk.val != "" {
				return awk, nil
			}
		}
		return nil, fmt.Errorf("no resolved awakeable at step %d", step)
	})

	resPRChain, err := wf.runWithRuntime(rtPRChain, RunBuildWorkflowRequest{BuildID: "b-pr-chain"})
	require.NoError(t, err)
	assert.Equal(t, "completed", resPRChain.Status)
	assert.Equal(t, 2, resPRChain.CompletedChunks)
}

type fakeObjectRuntime struct {
	key            string
	launched       map[string]bool
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

func (f *fakeObjectRuntime) ClearState() {
	f.cleared = true
	f.launched = make(map[string]bool)
}

func (f *fakeObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	f.stepsRun = append(f.stepsRun, name)
	if f.runStepErr != nil {
		return reconcileStepOutput{}, f.runStepErr
	}
	return fn(context.Background())
}

func TestBuildReconciler_DeriveReconcileChunkStates(t *testing.T) {
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

func TestBuildReconciler_ReconcileAndExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}
	rec := NewBuildReconciler(srv)

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

func TestBuildReconciler_EdgeCasesAndFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}
	rec := NewBuildReconciler(srv)

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
		"/BuildReconciler/b-rec/Reconcile/send",
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
