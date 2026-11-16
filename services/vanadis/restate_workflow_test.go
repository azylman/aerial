package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

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
		{ID: "c1", BuildID: "b-wf", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "c1", Instructions: "do c1", DependsOn: []string{}},
		{ID: "c2", BuildID: "b-wf", ChunkIndex: 1, Mode: "action", Title: "c2", Instructions: "do c2", DependsOn: []string{"c1"}},
	}
	require.NoError(t, store.CreateBuild(ctx, b, chunks))

	testChunks := []VanadisChunk{
		{ID: "tc1", BuildID: "b-wf", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "tc1", Instructions: "do c1", PRNumber: 42, UpdatedAt: time.Now().UTC()},
		{ID: "tc2", BuildID: "b-wf", ChunkIndex: 1, Mode: "action", Title: "tc2", Instructions: "do c2", UpdatedAt: time.Now().UTC()},
	}
	summaries := chunksToSummaries(testChunks)
	require.Len(t, summaries, 2)
	assert.Equal(t, 42, summaries[0].PRNumber)
	assert.False(t, summaries[0].UpdatedAt.IsZero())

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

	// executeCompleteWorkflowBuild completes build and dispatches completed build wake with completed chunk count
	var wakePromptSent string
	tsWake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		wakePromptSent = req["prompt"]
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer tsWake.Close()

	srv.cfg = Config{BrainURL: tsWake.URL}
	srv.httpClient = tsWake.Client()
	_, _, _, errAttach := store.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 42, Repo: "azylman/aerial"})
	require.NoError(t, errAttach)
	require.NoError(t, executeCompleteWorkflowBuild(ctx, srv, "b-wf"))
	assert.Contains(t, wakePromptSent, "2 chunks completed")
	assert.Contains(t, wakePromptSent, "PR #42")
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
		{ID: "c1", BuildID: "b-restate", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "c1", Instructions: "do c1", Status: "pr_opened", PRNumber: 42, DependsOn: []string{}},
		{ID: "c2", BuildID: "b-restate", ChunkIndex: 1, Mode: "action", Title: "c2", Instructions: "do c2", Status: "completed", DependsOn: []string{"c1"}},
		{ID: "c3", BuildID: "b-restate", ChunkIndex: 2, Mode: "action", Title: "c3", Instructions: "do c3", Status: "failed", DependsOn: []string{"c2"}},
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
		{ID: "c-act-single", BuildID: "b-act-single", ChunkIndex: 0, Mode: "action", Title: "act 1", Status: "running", DependsOn: []string{}},
	}
	require.NoError(t, store.CreateBuild(ctx, bAct, cAct))
	require.NoError(t, srvWake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-single",
		DispatchID:     "vanadis-c-act-single-turn-0",
	}))
	assert.NotEmpty(t, wakePromptSent)
	assert.Contains(t, wakePromptSent, "Completed Chunks Ledger:")
	assert.Contains(t, wakePromptSent, "- Chunk 1/1: act 1 (action, completed)")

	// Test action chunk completes with Restate failure: error is returned and redelivery retries reconciliation
	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("restate error"))
	}))
	defer ts500.Close()

	bRetry := VanadisBuild{ID: "b-act-retry", ThreadID: "123456789012345678", Objective: "test act retry", Status: "running"}
	cRetry := []VanadisChunk{
		{ID: "c-act-retry-1", BuildID: "b-act-retry", ChunkIndex: 0, Mode: "action", Title: "act 1", Status: "running", DependsOn: []string{}},
	}
	require.NoError(t, store.CreateBuild(ctx, bRetry, cRetry))
	srv500Wake := &VanadisServer{
		restateCfg: RestateConfig{RestateIngressURL: ts500.URL},
		store:      store,
		httpClient: ts500.Client(),
	}
	require.Error(t, srv500Wake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-retry-1",
		DispatchID:     "vanadis-c-act-retry-1-turn-0",
	}))
	cLoaded, _, err := store.GetChunk(ctx, "c-act-retry-1")
	require.NoError(t, err)
	assert.Equal(t, "completed", cLoaded.Status)
	// On redelivery with healthy Restate, reconciliation retries and succeeds for already-completed chunk
	require.NoError(t, srvWake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-retry-1",
		DispatchID:     "vanadis-c-act-retry-1-turn-0",
	}))

	bMulti := VanadisBuild{ID: "b-act-multi", ThreadID: "123456789012345678", Objective: "test act multi wake", Status: "running"}
	cMulti := []VanadisChunk{
		{ID: "c-act-m0", BuildID: "b-act-multi", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "m0 pr", Status: "completed", PRNumber: 99, DependsOn: []string{}},
		{ID: "c-act-m1", BuildID: "b-act-multi", ChunkIndex: 1, Mode: "action", Title: "m1 act", Status: "running", DependsOn: []string{"c-act-m0"}},
	}
	require.NoError(t, store.CreateBuild(ctx, bMulti, cMulti))
	require.NoError(t, srvWake.handleCompletedDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c-act-m1",
		DispatchID:     "vanadis-c-act-m1-turn-0",
	}))
	assert.Contains(t, wakePromptSent, "2 chunks completed")
	assert.Contains(t, wakePromptSent, "PR #99")
	assert.Contains(t, wakePromptSent, "(action, completed)")

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
		"/Build/b-act-multi/Reconcile/send",
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
		{ID: "c-ll-0", BuildID: "b-local-launch", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR 0", Instructions: "do", Status: "pending", DependsOn: []string{}},
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
	for _, index := range []int{0, 1} {
		c := VanadisChunk{ID: "retry", Mode: "action", Status: "running", StallNudges: 1, ChunkIndex: index}
		assert.Empty(t, reconstructLaunchedChunks([]VanadisChunk{c}))
		_, states := deriveReconcileChunkStates([]VanadisChunk{c}, map[string]bool{"retry": true})
		assert.False(t, states["retry"].launched)
		_, states = deriveReconcileChunkStates([]VanadisChunk{c}, map[string]bool{"dispatch:vanadis-retry-turn-1": true})
		assert.True(t, states["retry"].launched)
	}

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
	require.NoError(t, os.WriteFile(tmpYAML, []byte("enabled: true\nrestate_ingress_url: \"http://restate:8080/\"\n"), 0o600))
	rcfg := LoadRestateConfig(tmpYAML)
	assert.True(t, rcfg.Enabled)
	assert.Equal(t, "http://restate:8080", rcfg.RestateIngressURL)

	rcfgDefault := LoadRestateConfig("")
	assert.Empty(t, rcfgDefault.RestateIngressURL)

	rcfgMissing := LoadRestateConfig(t.TempDir() + "/nonexistent.yaml")
	assert.Empty(t, rcfgMissing.RestateIngressURL)

	tmpYAMLDisabled := t.TempDir() + "/disabled.yaml"
	require.NoError(t, os.WriteFile(tmpYAMLDisabled, []byte("enabled: false\nrestate_ingress_url: \"\"\n"), 0o600))
	rcfgDis := LoadRestateConfig(tmpYAMLDisabled)
	assert.False(t, rcfgDis.Enabled)

	tmpYAMLBad := t.TempDir() + "/bad.yaml"
	require.NoError(t, os.WriteFile(tmpYAMLBad, []byte(":::invalid"), 0o600))
	rcfgBad := LoadRestateConfig(tmpYAMLBad)
	assert.Empty(t, rcfgBad.RestateIngressURL)
}

func TestDeriveReconcileChunkStates(t *testing.T) {
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
		{ID: "c-m-eri", Mode: "pr", Status: "merged", TargetFiles: []string{"services/eri/handlers.go"}},
		{ID: "c-dep-eri", Mode: "pr", Status: "deploying", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}},
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
	assert.False(t, states["c-depfail"].codeReady)
	assert.False(t, states["c-depfail"].runtimeReady)

	assert.True(t, states["c-m-eri"].launched)
	assert.False(t, states["c-m-eri"].codeReady, "merged chunk restarting eri must not be code ready")
	assert.False(t, states["c-m-eri"].runtimeReady)

	assert.True(t, states["c-dep-eri"].launched)
	assert.False(t, states["c-dep-eri"].codeReady, "deploying chunk restarting eri must not be code ready")
	assert.False(t, states["c-dep-eri"].runtimeReady)

	assert.True(t, states["c-code"].launched)

	// on "created", unlaunched running chunk without PRNumber is NOT launched yet
	assert.False(t, states["c-run-created-new"].launched)
	assert.True(t, states["c-run-created-prior"].launched)
	assert.True(t, states["c-run-created-pr"].launched)

	// pending chunks
	assert.False(t, states["c-pend-new"].launched)
	assert.True(t, states["c-pend-prior"].launched)
}

func TestExecuteReconcileBuild_Execution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}

	// 1. Build not found returns terminal not_found
	resNotFound, err := executeReconcileBuild(ctx, srv, "b-missing", nil)
	require.NoError(t, err)
	assert.Equal(t, "not_found", resNotFound.BuildStatus)

	// 2. Initial creation launch on "created"
	bCreate := VanadisBuild{ID: "b-create", ThreadID: "th-1", Objective: "test create", Status: "pending"}
	cCreate := []VanadisChunk{
		{ID: "c0", BuildID: "b-create", ChunkIndex: 0, Mode: "pr", Status: "running", DependsOn: []string{}},
		{ID: "c1", BuildID: "b-create", ChunkIndex: 1, Mode: "pr", Status: "pending", DependsOn: []string{"c0"}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bCreate, cCreate))

	resCreate, err := executeReconcileBuild(ctx, srv, "b-create", nil)
	require.NoError(t, err)
	assert.Equal(t, "running", resCreate.BuildStatus)
	assert.Equal(t, []string{"c0"}, resCreate.LaunchedChunks)
	assert.True(t, resCreate.LaunchedMap["c0"])

	// Verify build status was moved to "running"
	bLoaded, _, err := memStore.GetBuild(ctx, "b-create")
	require.NoError(t, err)
	assert.Equal(t, "running", bLoaded.Status)

	// 3. In-flight handover: running chunk already in Postgres does not double-launch on subsequent reconcile
	resHandover, err := executeReconcileBuild(ctx, srv, "b-create", resCreate.LaunchedMap)
	require.NoError(t, err)
	assert.Equal(t, "running", resHandover.BuildStatus)
	assert.Empty(t, resHandover.LaunchedChunks)

	// 4. PR chunk merged unblocks dependent PR chunk, but build stays running until deploy_success completes it
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "c0", "merged"))
	resMerged, err := executeReconcileBuild(ctx, srv, "b-create", resCreate.LaunchedMap)
	require.NoError(t, err)
	assert.Equal(t, "running", resMerged.BuildStatus)
	assert.Equal(t, []string{"c1"}, resMerged.LaunchedChunks)
	assert.True(t, resMerged.LaunchedMap["c1"])

	// Complete both chunks -> completes build
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "c0", "completed"))
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "c1", "completed"))
	resComp, err := executeReconcileBuild(ctx, srv, "b-create", resMerged.LaunchedMap)
	require.NoError(t, err)
	assert.Equal(t, "completed", resComp.BuildStatus)

	// Reconcile already terminal build returns terminal status immediately
	resAlreadyTerminal, err := executeReconcileBuild(ctx, srv, "b-create", resMerged.LaunchedMap)
	require.NoError(t, err)
	assert.Equal(t, "completed", resAlreadyTerminal.BuildStatus)
}

func TestExecuteReconcileBuild_EdgeCasesAndFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}

	// 5. Action chunk dependency: dependent chunk only unblocks on action chunk completed
	bAction := VanadisBuild{ID: "b-action", ThreadID: "th-act", Objective: "test action", Status: "running"}
	cAction := []VanadisChunk{
		{ID: "ca0", BuildID: "b-action", ChunkIndex: 0, Mode: "action", Status: "merged", DependsOn: []string{}},
		{ID: "ca1", BuildID: "b-action", ChunkIndex: 1, Mode: "pr", Status: "pending", DependsOn: []string{"ca0"}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bAction, cAction))
	resActMerged, err := executeReconcileBuild(ctx, srv, "b-action", nil)
	require.NoError(t, err)
	// ca0 is action chunk in "merged", so codeReady=false; ca1 cannot launch
	assert.Empty(t, resActMerged.LaunchedChunks)

	// When ca0 completes, ca1 launches
	require.NoError(t, memStore.UpdateChunkStatus(ctx, "ca0", "completed"))
	resActComp, err := executeReconcileBuild(ctx, srv, "b-action", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"ca1"}, resActComp.LaunchedChunks)

	// 6. Chunk failure terminates build with failed status, dispatches fail wake
	bFail := VanadisBuild{ID: "b-fail-rec", ThreadID: "th-fail", Objective: "test fail", Status: "running"}
	cFail := []VanadisChunk{
		{ID: "cf0", BuildID: "b-fail-rec", ChunkIndex: 0, Mode: "pr", Status: "failed", DependsOn: []string{}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bFail, cFail))
	resFail, err := executeReconcileBuild(ctx, srv, "b-fail-rec", nil)
	require.NoError(t, err)
	assert.Equal(t, "failed", resFail.BuildStatus)
	assert.Contains(t, resFail.Error, "chunk cf0 failed")

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
	cPendingErr := []VanadisChunk{{ID: "c-pe0", BuildID: "b-pending-err", ChunkIndex: 0, Mode: "pr", Status: "pending", DependsOn: []string{}}}
	require.NoError(t, memStore.CreateBuild(ctx, bPendingErr, cPendingErr))
	memStore.updateBuildErr = errors.New("cannot update build to running")
	_, errPendingFail := executeReconcileBuild(ctx, srv, "b-pending-err", nil)
	assert.Error(t, errPendingFail)
	memStore.updateBuildErr = nil

	// 7c. Complete build update failure
	bCompErr := VanadisBuild{ID: "b-comp-err", Status: "running"}
	cCompErr := []VanadisChunk{{ID: "cce0", BuildID: "b-comp-err", ChunkIndex: 0, Mode: "action", Status: "completed", DependsOn: []string{}}}
	require.NoError(t, memStore.CreateBuild(ctx, bCompErr, cCompErr))
	memStore.updateBuildErr = errors.New("cannot complete build")
	_, errCompFail := executeReconcileBuild(ctx, srv, "b-comp-err", nil)
	assert.Error(t, errCompFail)
	memStore.updateBuildErr = nil

	// 8. Launch chunk error branch in launchReconcileEligibleChunks
	bLaunchErr := VanadisBuild{ID: "b-launch-err", ThreadID: "th-launch", Objective: "launch err", Status: "running"}
	cLaunchErr := []VanadisChunk{
		{ID: "cle0", BuildID: "b-launch-err", ChunkIndex: 0, Mode: "pr", Status: "pending", DependsOn: []string{}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bLaunchErr, cLaunchErr))
	srvNoBus := &VanadisServer{store: memStore, bus: nil}
	_, errLaunchFail := executeReconcileBuild(ctx, srvNoBus, "b-launch-err", nil)
	assert.Error(t, errLaunchFail)

	// 9. Fail build dispatch error and update status error branches
	bDispatchErr := VanadisBuild{ID: "b-disp-err", ThreadID: "th-disp-err", Objective: "obj", Status: "running"}
	cDispatchErr := []VanadisChunk{{ID: "c-de0", BuildID: "b-disp-err", ChunkIndex: 0, Mode: "pr", Status: "failed", DependsOn: []string{}}}
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
	cFailReconcileErr := []VanadisChunk{{ID: "c-fre0", BuildID: "b-fail-rec-err", Mode: "pr", Status: "failed", DependsOn: []string{}}}
	require.NoError(t, memStore.CreateBuild(ctx, bFailReconcileErr, cFailReconcileErr))
	memStore.updateBuildErr = errors.New("cannot update build to failed")
	_, errRecFailBuild := executeReconcileBuild(ctx, srv, "b-fail-rec-err", nil)
	assert.Error(t, errRecFailBuild)
	memStore.updateBuildErr = nil
}

func TestExecuteBuildMutationAndReconcile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStore := newMemoryVanadisStore()
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	srv := &VanadisServer{store: memStore, bus: memBus}

	// 1. Initial Reconcile without prior state
	b1 := VanadisBuild{ID: "b-bo-1", ThreadID: "th-bo-1", Objective: "test bo", Status: "pending"}
	c1 := []VanadisChunk{
		{ID: "c-bo-0", BuildID: "b-bo-1", ChunkIndex: 0, Mode: "pr", Status: "running", DependsOn: []string{}},
		{ID: "c-bo-1", BuildID: "b-bo-1", ChunkIndex: 1, Mode: "pr", Status: "pending", DependsOn: []string{"c-bo-0"}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, b1, c1))

	res1, err1 := executeReconcileBuild(ctx, srv, "b-bo-1", nil)
	require.NoError(t, err1)
	assert.Equal(t, "running", res1.BuildStatus)
	assert.Equal(t, []string{"c-bo-0"}, res1.LaunchedChunks)
	assert.True(t, res1.LaunchedMap["c-bo-0"])

	// 2. Mutate: chunk_merged
	errMerged := executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{
		Action:  "chunk_merged",
		ChunkID: "c-bo-0",
	})
	require.NoError(t, errMerged)
	resMerged, errRecMerged := executeReconcileBuild(ctx, srv, "b-bo-1", res1.LaunchedMap)
	require.NoError(t, errRecMerged)
	assert.Equal(t, "running", resMerged.BuildStatus)
	assert.Equal(t, []string{"c-bo-1"}, resMerged.LaunchedChunks)
	assert.True(t, resMerged.LaunchedMap["c-bo-1"])
	c0Loaded, _, err := memStore.GetChunk(ctx, "c-bo-0")
	require.NoError(t, err)
	assert.Equal(t, "merged", c0Loaded.Status)

	// 3. Mutate: chunk_deploy_success
	errDep := executeBuildMutation(ctx, srv, "b-bo-1", BuildMutationRequest{
		Action:  "chunk_deploy_success",
		ChunkID: "c-bo-0",
	})
	require.NoError(t, errDep)
	c0AfterDep, _, err := memStore.GetChunk(ctx, "c-bo-0")
	require.NoError(t, err)
	assert.Equal(t, "completed", c0AfterDep.Status)

	// 4. Mutate: chunk_failed
	bFail := VanadisBuild{ID: "b-bo-fail", Status: "running"}
	cFail := []VanadisChunk{
		{ID: "c-fail-0", BuildID: "b-bo-fail", ChunkIndex: 0, Mode: "pr", Status: "running", DependsOn: []string{}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bFail, cFail))
	errFail := executeBuildMutation(ctx, srv, "b-bo-fail", BuildMutationRequest{
		Action:  "chunk_failed",
		ChunkID: "c-fail-0",
		Reason:  "compilation error",
	})
	require.NoError(t, errFail)
	resFail, errRecFail := executeReconcileBuild(ctx, srv, "b-bo-fail", nil)
	require.NoError(t, errRecFail)
	assert.Equal(t, "failed", resFail.BuildStatus)
	assert.Contains(t, resFail.Error, "c-fail-0")

	// 5. Mutate: cancelled
	bCancel := VanadisBuild{ID: "b-bo-cancel", Status: "running"}
	cCancel := []VanadisChunk{
		{ID: "c-cancel-0", BuildID: "b-bo-cancel", ChunkIndex: 0, Mode: "pr", Status: "running", DependsOn: []string{}},
	}
	require.NoError(t, memStore.CreateBuild(ctx, bCancel, cCancel))
	errCancel := executeBuildMutation(ctx, srv, "b-bo-cancel", BuildMutationRequest{
		Action: "cancelled",
	})
	require.NoError(t, errCancel)
	resCancel, errRecCancel := executeReconcileBuild(ctx, srv, "b-bo-cancel", nil)
	require.NoError(t, errRecCancel)
	assert.Equal(t, "cancelled", resCancel.BuildStatus)
	bCancelLoaded, _, err := memStore.GetBuild(ctx, "b-bo-cancel")
	require.NoError(t, err)
	assert.Equal(t, "cancelled", bCancelLoaded.Status)

	testBuildMutationErrorBranches(t, srv, memStore)
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
	cLocal := []VanadisChunk{{ID: "cl0", BuildID: "b-local", ChunkIndex: 0, Mode: "action", Status: "completed", DependsOn: []string{}}}
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

type restateBuildTransitionTestCase struct {
	name       string
	path       string
	setupStore func(store *memoryVanadisStore)
	srv        *VanadisServer
	body       string
	useErrBody bool
	wantCode   int
	wantStatus string
}

func runRestateBuildTransitionTestCase(t *testing.T, tc restateBuildTransitionTestCase) {
	t.Helper()
	store := newMemoryVanadisStore()
	if tc.setupStore != nil {
		tc.setupStore(store)
	}
	bus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })
	srv := tc.srv
	if srv == nil {
		srv = &VanadisServer{store: store, bus: bus}
	}

	mux := SetupMux(srv)
	var r *http.Request
	if tc.useErrBody {
		r = httptest.NewRequest(http.MethodPost, tc.path, errorReader{})
	} else {
		r = httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	assert.Equal(t, tc.wantCode, w.Code)
	if tc.wantStatus != "" {
		var out reconcileStepOutput
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)
		assert.Equal(t, tc.wantStatus, out.BuildStatus)
	}
}

func TestRestateBuildTransitionHandler(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []restateBuildTransitionTestCase{
		{
			name: "valid_reconcile",
			path: "/internal/restate/builds/b-trans-1/transition",
			setupStore: func(s *memoryVanadisStore) {
				b := VanadisBuild{ID: "b-trans-1", Status: "running"}
				chunks := []VanadisChunk{
					{ID: "c-t1", BuildID: "b-trans-1", ChunkIndex: 0, Mode: "action", Status: "completed", DependsOn: []string{}},
				}
				_ = s.CreateBuild(ctx, b, chunks)
			},
			body:       `{"prior_launched":{"c-t1":true}}`,
			wantCode:   http.StatusOK,
			wantStatus: "completed",
		},
		{
			name: "valid_mutation",
			path: "/internal/restate/builds/b-trans-2/transition",
			setupStore: func(s *memoryVanadisStore) {
				b := VanadisBuild{ID: "b-trans-2", Status: "running"}
				chunks := []VanadisChunk{
					{ID: "c-t2", BuildID: "b-trans-2", ChunkIndex: 0, Mode: "action", Status: "running", DependsOn: []string{}},
				}
				_ = s.CreateBuild(ctx, b, chunks)
			},
			body:       `{"mutation":{"action":"action_completed","chunk_id":"c-t2"}}`,
			wantCode:   http.StatusOK,
			wantStatus: "completed",
		},
		{
			name:       "invalid_json",
			path:       "/internal/restate/builds/b-trans-3/transition",
			body:       `{not-valid-json`,
			wantCode:   http.StatusBadRequest,
			wantStatus: "",
		},
		{
			name:       "invalid_mutation_action",
			path:       "/internal/restate/builds/b-trans-4/transition",
			body:       `{"mutation":{"action":"unknown_action"}}`,
			wantCode:   http.StatusBadRequest,
			wantStatus: "",
		},
		{
			name:       "invalid_mutation_missing_chunk_id",
			path:       "/internal/restate/builds/b-trans-5/transition",
			body:       `{"mutation":{"action":"chunk_merged"}}`,
			wantCode:   http.StatusBadRequest,
			wantStatus: "",
		},
		{
			name:       "nil_store",
			path:       "/internal/restate/builds/b-trans-6/transition",
			srv:        &VanadisServer{store: nil},
			body:       `{}`,
			wantCode:   http.StatusInternalServerError,
			wantStatus: "",
		},
		{
			name: "store_reconcile_error",
			path: "/internal/restate/builds/b-trans-7/transition",
			setupStore: func(s *memoryVanadisStore) {
				s.getErr = errors.New("db query error")
			},
			body:       `{}`,
			wantCode:   http.StatusInternalServerError,
			wantStatus: "",
		},
		{
			name: "store_mutation_internal_error",
			path: "/internal/restate/builds/b-trans-8/transition",
			setupStore: func(s *memoryVanadisStore) {
				b := VanadisBuild{ID: "b-trans-8", Status: "running"}
				chunks := []VanadisChunk{{ID: "c-t8", BuildID: "b-trans-8", ChunkIndex: 0, Mode: "pr", Status: "running", DependsOn: []string{}}}
				_ = s.CreateBuild(ctx, b, chunks)
				s.updateChunkErr = errors.New("db write failed")
			},
			body:       `{"mutation":{"action":"chunk_merged","chunk_id":"c-t8"}}`,
			wantCode:   http.StatusInternalServerError,
			wantStatus: "",
		},
		{
			name:       "read_body_error",
			path:       "/internal/restate/builds/b-trans-9/transition",
			useErrBody: true,
			wantCode:   http.StatusBadRequest,
			wantStatus: "",
		},
		{
			name: "missing_chunk_mutation_returns_404",
			path: "/internal/restate/builds/b-trans-10/transition",
			setupStore: func(s *memoryVanadisStore) {
				b := VanadisBuild{ID: "b-trans-10", Status: "running"}
				_ = s.CreateBuild(ctx, b, nil)
			},
			body:     `{"mutation":{"action":"chunk_merged","chunk_id":"missing-chunk"}}`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRestateBuildTransitionTestCase(t, tc)
		})
	}

	// Direct test for empty buildID in handler
	t.Run("empty_build_id_direct", func(t *testing.T) {
		t.Parallel()
		srv := &VanadisServer{store: newMemoryVanadisStore()}
		r := httptest.NewRequest(http.MethodPost, "/transition", strings.NewReader(`{}`))
		w := httptest.NewRecorder()
		srv.handleRestateBuildTransition(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestActionCompletionMutationIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, action, buildID, dispatchID, chunkStatus, buildStatus, wantStatus string
		generation                                                              int
		wantErr                                                                 bool
		quiet, wantWake                                                         bool
	}{
		{"first run compatibility", "action_completed", "b1", "", "running", "running", "completed", 0, false, false, true},
		{"first run supplied identity", "action_completed", "b1", "vanadis-c1-turn-0", "running", "running", "completed", 0, false, false, true},
		{"first run stale identity", "action_completed", "b1", "stale", "running", "running", "running", 0, false, false, false},
		{"matching retry", "action_completed", "b1", "vanadis-c1-turn-1", "running", "running", "completed", 1, false, false, true},
		{"missing retry", "action_completed", "b1", "", "running", "running", "running", 1, false, false, false},
		{"stale retry", "action_completed", "b1", "vanadis-c1-turn-0", "running", "running", "running", 1, false, false, false},
		{"wrong build", "action_completed", "other", "", "running", "running", "running", 0, true, false, false},
		{"duplicate", "action_completed", "b1", "vanadis-c1-turn-1", "completed", "completed", "completed", 1, false, false, false},
		{"cancelled", "action_completed", "b1", "vanadis-c1-turn-1", "cancelled", "cancelled", "cancelled", 1, false, false, false},
		{"first run compatibility", "chunk_failed", "b1", "", "running", "running", "failed", 0, false, false, true},
		{"first run supplied identity", "chunk_failed", "b1", "vanadis-c1-turn-0", "running", "running", "failed", 0, false, false, true},
		{"first run stale identity", "chunk_failed", "b1", "stale", "running", "running", "running", 0, false, false, false},
		{"matching retry", "chunk_failed", "b1", "vanadis-c1-turn-1", "running", "running", "failed", 1, false, false, true},
		{"missing retry", "chunk_failed", "b1", "", "running", "running", "running", 1, false, false, false},
		{"stale retry", "chunk_failed", "b1", "vanadis-c1-turn-0", "running", "running", "running", 1, false, false, false},
		{"failure duplicate", "chunk_failed", "b1", "vanadis-c1-turn-1", "failed", "failed", "failed", 1, false, false, false},
		{"cancelled", "chunk_failed", "b1", "vanadis-c1-turn-1", "cancelled", "cancelled", "cancelled", 1, false, false, false},
		{"deployment missing identity", "chunk_deploy_success", "b1", "", "running", "running", "running", 1, false, false, false},
		{"deployment stale identity", "chunk_deploy_success", "b1", "vanadis-c1-turn-0", "running", "running", "running", 1, false, false, false},
		{"deployment matching identity", "chunk_deploy_success", "b1", "vanadis-c1-turn-1", "running", "running", "completed", 1, false, false, true},
		{"deployment wrong build", "chunk_deploy_success", "other", "vanadis-c1-turn-1", "running", "running", "running", 1, true, false, false},
		{"quiet deployment", "chunk_deploy_success", "b1", "vanadis-c1-turn-1", "running", "running", "completed", 1, false, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newMemoryVanadisStore()
			require.NoError(t, store.CreateBuild(ctx, VanadisBuild{ID: "b1", ThreadID: "thread1", Status: tc.buildStatus, Quiet: tc.quiet}, []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: tc.chunkStatus, StallNudges: tc.generation, DependsOn: []string{}}}))
			brain := newDispatchBrainServer(t)
			srv := &VanadisServer{store: store, cfg: Config{BrainURL: brain.URL}, httpClient: brain.Client()}
			err := executeBuildMutation(ctx, srv, tc.buildID, BuildMutationRequest{Action: tc.action, ChunkID: "c1", DispatchID: tc.dispatchID})
			assert.Equal(t, tc.wantErr, err != nil)
			chunk, _, err := store.GetChunk(ctx, "c1")
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, chunk.Status)
			_, wakes := brain.calls()
			assert.Equal(t, tc.wantWake, wakes > 0)
			// Redelivery must not emit another terminal wake.
			_ = executeBuildMutation(ctx, srv, tc.buildID, BuildMutationRequest{Action: tc.action, ChunkID: "c1", DispatchID: tc.dispatchID})
			_, redeliveredWakes := brain.calls()
			assert.Equal(t, wakes, redeliveredWakes)
		})
	}
}

func TestExecuteLaunchChunk_Errors(t *testing.T) {
	t.Parallel()
	retry := VanadisChunk{ID: "c", BuildID: "b", Mode: "action", Status: "running", StallNudges: 1, ChunkIndex: 1, DependsOn: []string{}}
	accepted := ExecuteChunkArgs{BuildID: "b", ChunkID: "c", ChunkIndex: 1, DispatchID: "vanadis-c-turn-1"}
	tests := []struct {
		name       string
		chunks     []VanadisChunk
		prior      map[string]bool
		publishErr bool
		reserveErr bool
		wantErr    bool
		want       []ExecuteChunkArgs
	}{
		{name: "reserved retry recovers old map", chunks: []VanadisChunk{retry}, prior: map[string]bool{"c": true}, want: []ExecuteChunkArgs{accepted}},
		{name: "running retry occupies one slot before pending siblings", chunks: []VanadisChunk{
			{ID: "sibling", BuildID: "b", Mode: "action", Status: "pending", DependsOn: []string{}},
			{ID: "a", BuildID: "b", Mode: "pr", Repo: "aerial", TargetFiles: []string{"a.go"}, Status: "running", PRNumber: 1, DependsOn: []string{}},
			{ID: "d", BuildID: "b", Mode: "pr", Repo: "mirrormere", TargetFiles: []string{"b.go"}, Status: "running", PRNumber: 2, DependsOn: []string{}},
			retry,
		}, want: []ExecuteChunkArgs{accepted, {BuildID: "b", ChunkID: "sibling"}}},
		{name: "accepted generation not republished", chunks: []VanadisChunk{retry}, prior: map[string]bool{"dispatch:vanadis-c-turn-1": true}},
		{name: "publish failure retains reservation", chunks: []VanadisChunk{retry}, publishErr: true, wantErr: true},
		{name: "reserve failure prevents publication", chunks: []VanadisChunk{retry}, reserveErr: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newMemoryVanadisStore()
			require.NoError(t, store.CreateBuild(ctx, VanadisBuild{ID: "b", Status: "running"}, tc.chunks))
			bus := &mockChunkBus{}
			if tc.publishErr {
				bus.err = fmt.Errorf("publish unavailable")
			}
			if tc.reserveErr {
				store.updateChunkErr = fmt.Errorf("reservation unavailable")
			}
			srv := &VanadisServer{store: store, bus: bus}
			out, err := executeReconcileBuild(ctx, srv, "b", tc.prior)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.want, bus.published)
			if len(tc.want) > 0 {
				require.NotEmpty(t, bus.publishedIDs)
				assert.Equal(t, "vanadis:chunk:b:c:vanadis-c-turn-1", bus.publishedIDs[0])
			}
			if !tc.wantErr {
				again, err := executeReconcileBuild(ctx, srv, "b", out.LaunchedMap)
				require.NoError(t, err)
				assert.Empty(t, again.LaunchedChunks)
			}
		})
	}
}

func TestReconcileRetryAcknowledgementCollision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		prior       map[string]bool
		wantSibling []ExecuteChunkArgs
	}{
		{name: "retry acknowledgement does not suppress dependent sibling", wantSibling: []ExecuteChunkArgs{{BuildID: "b", ChunkID: "vanadis-c-turn-1", ChunkIndex: 1}}},
		{name: "sibling acknowledgement does not suppress retry", prior: map[string]bool{"vanadis-c-turn-1": true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newMemoryVanadisStore()
			chunks := []VanadisChunk{
				{ID: "c", BuildID: "b", Mode: "action", Status: "running", StallNudges: 1, DependsOn: []string{}},
				{ID: "vanadis-c-turn-1", BuildID: "b", ChunkIndex: 1, Mode: "action", Status: "pending", DependsOn: []string{"c"}},
			}
			require.True(t, isValidVanadisChunkID("vanadis-c-turn-1"))
			require.NoError(t, store.CreateBuild(ctx, VanadisBuild{ID: "b", Status: "running"}, chunks))
			bus := &mockChunkBus{}
			srv := &VanadisServer{store: store, bus: bus}
			out, err := executeReconcileBuild(ctx, srv, "b", tc.prior)
			require.NoError(t, err)
			assert.Equal(t, []ExecuteChunkArgs{{BuildID: "b", ChunkID: "c", DispatchID: "vanadis-c-turn-1"}}, bus.published)

			require.NoError(t, store.UpdateChunkStatus(ctx, "c", "completed"))
			bus.published = nil
			_, err = executeReconcileBuild(ctx, srv, "b", out.LaunchedMap)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSibling, bus.published)
		})
	}
}
