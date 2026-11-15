package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

	// executeLaunchChunk persists awakeable IDs and transitions chunk to running
	err = executeLaunchChunk(ctx, srv, "b-wf", chunks[0], "m1", "d1")
	require.NoError(t, err)
	gotChunk, _, err := store.GetChunk(ctx, "c1")
	require.NoError(t, err)
	assert.Equal(t, "m1", gotChunk.MergeAwakeableID)
	assert.Equal(t, "d1", gotChunk.DeployAwakeableID)
	assert.Equal(t, "running", gotChunk.Status)
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
		{ID: "c2", BuildID: "b-restate", ChunkIndex: 1, Mode: "action", Title: "c2", Instructions: "do c2", Status: "running", DeployAwakeableID: "awk-a2"},
	}
	require.NoError(t, store.CreateBuild(ctx, b, chunks))

	srv := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts.URL},
		store:      store,
		httpClient: ts.Client(),
	}

	require.NoError(t, srv.launchCreatedBuild(ctx, "b-restate", true))
	require.NoError(t, srv.cancelRestateBuildWorkflow(ctx, "b-restate", "test cancel"))
	require.NoError(t, srv.notifyRestateDAGMutation(ctx, "b-restate", "append_chunks"))

	resAdv := &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         true,
		BuildID:          "b-restate",
		MergedChunkIndex: 0,
		NextChunkIndex:   1,
	}
	assert.True(t, srv.notifyRestateChunkPRAdvanced(ctx, resAdv))
	assert.True(t, srv.notifyRestateActionChunkCompleted(ctx, "c2"))
	srv.notifyRestateChunkFailed(ctx, "c1")

	assert.Equal(t, []string{
		"/BuildWorkflow/b-restate/Run/send",
		"/BuildWorkflow/b-restate/Cancel/send",
		"/BuildWorkflow/b-restate/MutateDAG/send",
		"/restate/awakeables/awk-m1/resolve",
		"/restate/awakeables/awk-d1/resolve",
		"/restate/awakeables/awk-a2/resolve",
		"/restate/awakeables/awk-m1/resolve",
		"/restate/awakeables/awk-d1/resolve",
	}, paths)

	// Error and no-op edge cases for ingress helpers
	var nilSrv *VanadisServer
	assert.Error(t, nilSrv.postRestateIngressJSON(ctx, "/test", nil))
	assert.Error(t, srv.startRestateBuildWorkflow(ctx, ""))
	assert.NoError(t, srv.cancelRestateBuildWorkflow(ctx, "", "r"))
	assert.NoError(t, srv.notifyRestateDAGMutation(ctx, "", "a"))
	assert.NoError(t, srv.resolveRestateAwakeable(ctx, "", "v"))
	assert.False(t, srv.notifyRestateChunkPRAdvanced(ctx, nil))
	assert.False(t, srv.notifyRestateActionChunkCompleted(ctx, ""))
	srv.notifyRestateChunkFailed(ctx, "")
	srv.notifyRestateChunkFailed(ctx, "nonexistent")

	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("restate error"))
	}))
	defer ts500.Close()
	srv500 := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts500.URL},
		store:      store,
		httpClient: ts500.Client(),
	}
	assert.Error(t, srv500.startRestateBuildWorkflow(ctx, "b-restate"))
	assert.False(t, srv500.notifyRestateChunkPRAdvanced(ctx, resAdv))
	assert.False(t, srv500.notifyRestateActionChunkCompleted(ctx, "c2"))
	srv500.notifyRestateChunkFailed(ctx, "c1")

	tmpYAML := t.TempDir() + "/restate.yaml"
	require.NoError(t, os.WriteFile(tmpYAML, []byte("restate_port: \"9099\"\nrestate_ingress_url: \"http://restate:8080/\"\nrestate_admin_url: \"http://restate:9070/\"\nrestate_advertise_url: \"http://vanadis:9099/\"\n"), 0o600))
	rcfg := LoadRestateConfig(tmpYAML)
	assert.Equal(t, "9099", rcfg.RestatePort)
	assert.Equal(t, "http://restate:8080", rcfg.RestateIngressURL)
	assert.Equal(t, "http://restate:9070", rcfg.RestateAdminURL)
	assert.Equal(t, "http://vanadis:9099", rcfg.RestateAdvertiseURL)
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

	rtSignals.Promise("dag_mutation").(*fakeWorkflowPromise).resolveErr = restate.TerminalErrorf("already resolved")
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

	// 5. PR -> PR chain driven through real HTTP advance-pr and awakeable resolver
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

	postAdvancePR := func(prNum int) VanadisAdvancementResult {
		body := fmt.Sprintf(`{"repo":"azylman/aerial","pr_number":%d}`, prNum)
		req := httptest.NewRequest(http.MethodPost, "/chunks/advance-pr", strings.NewReader(body))
		rec := httptest.NewRecorder()
		srv.Routes().ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		var out VanadisAdvancementResult
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		return out
	}

	rtPRChain = newFakeWorkflowRuntime(func(step int, rt *fakeWorkflowRuntime, futures []any) (any, error) {
		switch step {
		case 1:
			adv1 := postAdvancePR(501)
			assert.True(t, adv1.Advanced)
			assert.False(t, adv1.BuildCompleted)
		case 3:
			adv2 := postAdvancePR(502)
			assert.True(t, adv2.Advanced)
			assert.True(t, adv2.BuildCompleted, "advance-pr should preserve BuildCompleted for event trigger evaluation")
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
