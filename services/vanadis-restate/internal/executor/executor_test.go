package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/githubapitest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

var errDelegation = errors.New("eri unavailable")

type mockStoreForLaunch struct {
	store.VanadisStore

	reserveErr error
	reserved   []string
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(_ context.Context, chunkID string) error {
	m.reserved = append(m.reserved, chunkID)
	return m.reserveErr
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockBusExecutor) Publish(_ context.Context, subject, _ string, _ any) error {
	m.lastSubj = subject
	return nil
}

// casWidgetStore is a CAS-correct in-memory status message id store for one
// build: stored is the current status_message_id, writes counts swaps.
type casWidgetStore struct {
	store.VanadisStore

	mu     sync.Mutex
	stored string
	casErr error
	writes int
}

func (m *casWidgetStore) CompareAndSwapBuildStatusMessageID(_ context.Context, _, oldID, newID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.casErr != nil {
		return false, m.casErr
	}
	if m.stored != oldID {
		return false, nil
	}
	m.stored = newID
	m.writes++
	return true, nil
}

// mintingEriClient is a concurrency-safe fake whose Widget sinks when the
// request carries staleID (posting a new card) and edits fresh cards in place.
type mintingEriClient struct {
	eritest.Client

	mu      sync.Mutex
	staleID string
	minted  int
	deleted []string
}

func (m *mintingEriClient) Widget(_ context.Context, req StatusWidgetRequest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.MessageID != "" && req.MessageID != m.staleID {
		return req.MessageID, nil
	}
	m.minted++
	return fmt.Sprintf("new-%d", m.minted), nil
}

func (m *mintingEriClient) DeleteWidget(_ context.Context, _, _, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, messageID)
	return nil
}

func TestNewExecutor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, shareRoot string
		deployUIURL     []string
		wantShare       string
		wantDeploy      string
	}{
		{"defaults", "", nil, "/share", ""},
		{"explicit share root and deploy ui", "/custom-share", []string{"https://flux.example.com"}, "/custom-share", "https://flux.example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			exec := NewExecutor(tc.shareRoot, nil, nil, "tok", "example-owner", nil, tc.deployUIURL...)
			assert.Equal(t, tc.wantShare, exec.shareRoot)
			assert.Equal(t, tc.wantDeploy, exec.deployUIURL)
			assert.Equal(t, "example-owner", exec.GitHubOwner())
			var nilExec *Executor
			assert.Empty(t, nilExec.GitHubOwner())
		})
	}
}

// TestExecutorEriDelegation drives every thin eri delegation method with the
// same arguments (remediation first, so the silent wake is the recorded last
// dispatch); a nil eri row exercises the no-client guards.
func TestExecutorEriDelegation(t *testing.T) {
	t.Parallel()
	silent := &eriwire.BrainDispatchRequest{
		ID: "vanadis-deploy-fail-c1-94f73a41fd9c6e67", Source: "vanadis-restate", SessionID: "vanadis-c1", TargetID: "th1",
		VanadisChunkID: "c1", Prompt: "silent wake", Effort: "medium", SkipIfActive: true,
	}
	recorded := eritest.Client{LastCancel: "c1", LastThreadID: "th1", LastPrompt: "wake", LastDispatch: silent, LastMsgChan: "ch1", LastMsgCont: "msg"}
	failing := eritest.Client{CancelErr: errDelegation, PromptErr: errDelegation, DispatchErr: errDelegation, MessageErr: errDelegation}
	failingRecorded := failing
	failingRecorded.LastCancel, failingRecorded.LastThreadID, failingRecorded.LastPrompt = "c1", "th1", "wake"
	failingRecorded.LastDispatch, failingRecorded.LastMsgChan, failingRecorded.LastMsgCont = silent, "ch1", "msg"
	tests := []struct {
		name     string
		eri      *eritest.Client
		wantErrs []string
		want     eritest.Client
	}{
		{"nil eri skips notifications and rejects remediation", nil, []string{"executor or eri client is nil", "", "", "", ""}, eritest.Client{}},
		{"fake records every call", &eritest.Client{}, []string{"", "", "", "", ""}, recorded},
		{"eri errors propagate", &failing, []string{"eri unavailable", "eri unavailable", "eri unavailable", "eri unavailable", "eri unavailable"}, failingRecorded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var client EriClient
			var eri eritest.Client
			if tc.eri != nil {
				eri = *tc.eri
				client = &eri
			}
			exec := NewExecutor("", client, nil, "", "example-owner", nil)
			ctx := context.Background()
			remediation := eriwire.BrainDispatchRequest{ID: "d1", Source: "test"}
			remediationErr := exec.DispatchChunkRemediation(ctx, remediation)
			if tc.eri != nil {
				assert.Equal(t, &remediation, eri.LastDispatch)
			}
			errs := []error{
				remediationErr,
				exec.CancelChunkDispatch(ctx, "c1"),
				exec.DispatchWakePrompt(ctx, "th1", "wake"),
				exec.DispatchSilentWakePrompt(ctx, "th1", "c1", "silent wake", ""),
				exec.DispatchDirectMessage(ctx, "ch1", "msg"),
			}
			got := make([]string, len(errs))
			for i, err := range errs {
				if err != nil {
					got[i] = err.Error()
				}
			}
			assert.Equal(t, tc.wantErrs, got)
			assert.Equal(t, tc.want, eri)
		})
	}
}

func TestExecutorFormatChunkPrompt(t *testing.T) {
	t.Parallel()
	build := model.VanadisBuild{ID: "b1", Title: "Build 1", Objective: "Make everything awesome", ThreadID: "th-1"}
	chunks := []model.VanadisChunk{
		{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Do step 1", Effort: "high"},
		{ID: "c2", Title: "Chunk 2", Repo: "aerial", Instructions: "Do step 2", Mode: chunkModeAction},
		{ID: "c3", Title: "Chunk 3", Mode: "pr", Repo: "aerial", TargetFiles: []string{"main.go"}, TargetSymbols: []string{"MySpecialSym"}},
	}
	tests := []struct {
		name     string
		idx      int
		want     string
		wantErr  string
		wantNots []string
	}{
		{name: "pr chunk", idx: 0, want: "Chunk 1", wantNots: []string{"Target Symbol Spans:"}},
		{name: "action chunk", idx: 1, want: "Chunk 2"},
		{name: "target symbols render spans from share root", idx: 2, want: "Target Symbol Spans:"},
		{name: "out of bounds index", idx: 9, wantErr: "active chunk index 9 out of range [0, 3)"},
	}
	shareRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(shareRoot, "aerial"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shareRoot, "aerial", "main.go"), []byte("package main\n\nfunc MySpecialSym() {\n}\n"), 0o644))
	exec := NewExecutor(shareRoot, &eritest.Client{}, nil, "", "example-owner", nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := exec.FormatChunkPrompt(build, chunks, tc.idx)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Contains(t, got, tc.want)
			for _, not := range tc.wantNots {
				assert.NotContains(t, got, not)
			}
		})
	}
}

func TestExecutorPublishExternalEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		bus      pubsub.Bus
		wantErr  string
		wantSubj string
	}{
		{"nil bus publishes nothing", nil, "", ""},
		{"publishes to source event subject", &mockBusExecutor{}, "", "aerial.events.source.type"},
		{"publish error propagates", &pubsubtest.FaultBus{PublishErr: errDelegation}, "eri unavailable", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := NewExecutor("", nil, tc.bus, "", "example-owner", nil).PublishExternalEvent(context.Background(), "source", "type", []string{"t1"}, "ctx")
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			if rec, ok := tc.bus.(*mockBusExecutor); ok {
				assert.Equal(t, tc.wantSubj, rec.lastSubj)
			}
		})
	}
}

// TestExecutorResolveCIFailureDetailsChecked covers the coordinator's
// delegation to cilog; cilog's own tables own the log and RoboRev parsing.
func TestExecutorResolveCIFailureDetailsChecked(t *testing.T) {
	t.Parallel()
	const jobURL = "https://github.com/o/r/actions/runs/1/job/42"
	routes := map[string][]githubapitest.Response{"GET /repos/example-owner/aerial/actions/jobs/42/logs": {{Body: "--- FAIL: TestDelegated (0.01s)\n"}}}
	tests := []struct {
		name         string
		nilExec      bool
		noHTTPClient bool
		want         string
		wantPaths    []string
	}{
		{name: "nil executor returns details", nilExec: true, want: "failure", wantPaths: []string{}},
		{name: "missing http client returns details", noHTTPClient: true, want: "failure", wantPaths: []string{}},
		{name: "job log fetched through coordinator", want: "FAIL: TestDelegated", wantPaths: []string{"/repos/example-owner/aerial/actions/jobs/42/logs"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := githubapitest.New(t, routes)
			exec := NewExecutor("", nil, nil, "token", "example-owner", api.Client())
			exec.GitHubAPIURL = api.URL()
			if tc.noHTTPClient {
				exec.HTTPClient = nil
			}
			if tc.nilExec {
				exec = nil
			}
			got, err := exec.ResolveCIFailureDetailsChecked(context.Background(), "aerial", 0, "unit", "abc", jobURL, "failure")
			require.NoError(t, err)
			assert.Contains(t, got, tc.want)
			requests := api.Requests()
			paths := make([]string, 0, len(requests))
			for _, req := range requests {
				paths = append(paths, req.Path)
			}
			assert.Equal(t, tc.wantPaths, paths)
		})
	}
}

func TestExecutor_SyncBuildWidget(t *testing.T) {
	t.Parallel()
	errCAS := errors.New("cas unavailable")
	tests := []struct {
		name        string
		nilExec     bool
		noStore     bool
		eri         eritest.Client
		known       string
		stored      string
		casErr      error
		wantID      string
		wantErr     string
		wantStored  string
		wantWrites  int
		wantDeleted []string
	}{
		{name: "nil executor is a no-op", nilExec: true, known: "old", stored: "old", wantStored: "old"},
		{name: "edit in place stores nothing", eri: eritest.Client{WidgetID: "old"}, known: "old", stored: "old", wantID: "old", wantStored: "old"},
		{name: "first post stores the new card", eri: eritest.Client{WidgetID: "new"}, wantID: "new", wantStored: "new", wantWrites: 1},
		{name: "sink win stores the new card", eri: eritest.Client{WidgetID: "new"}, known: "old", stored: "old", wantID: "new", wantStored: "new", wantWrites: 1},
		{name: "sink loss deletes the new card", eri: eritest.Client{WidgetID: "new"}, known: "old", stored: "other", wantStored: "other", wantDeleted: []string{"new"}},
		{name: "sink loss tolerates delete failure", eri: eritest.Client{WidgetID: "new", DeleteWidgetErr: errors.New("discord down")}, known: "old", stored: "other", wantStored: "other", wantDeleted: []string{"new"}},
		{name: "cas error keeps the new card", eri: eritest.Client{WidgetID: "new"}, known: "old", stored: "old", casErr: errCAS, wantID: "new", wantStored: "old"},
		{name: "no store returns the new card", noStore: true, eri: eritest.Client{WidgetID: "new"}, known: "old", wantID: "new"},
		{name: "widget error writes and deletes nothing", eri: eritest.Client{WidgetErr: errors.New("eri widget failure")}, known: "old", stored: "old", wantErr: "sync widget to eri: eri widget failure", wantStored: "old"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := tc.eri
			exec := NewExecutor("", &eri, nil, "", "example-owner", nil)
			if tc.nilExec {
				exec = nil
			}
			st := &casWidgetStore{stored: tc.stored, casErr: tc.casErr}
			var vs store.VanadisStore = st
			if tc.noStore {
				vs = nil
			}
			b := model.VanadisBuild{ID: "b1", Title: "Build 1", Status: "running", ThreadID: "th-1", StatusMessageID: tc.known}
			chunks := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Status: "running"}}
			got, err := exec.SyncBuildWidget(context.Background(), vs, b, chunks, "Notice!")
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
			}
			assert.Equal(t, tc.wantID, got)
			assert.Equal(t, tc.wantStored, st.stored)
			assert.Equal(t, tc.wantWrites, st.writes)
			assert.Equal(t, tc.wantDeleted, eri.DeletedWidgets)
			if !tc.nilExec {
				require.NotNil(t, eri.LastWidget)
				assert.Equal(t, "Notice!", eri.LastWidget.Banner)
			}
		})
	}
}

// TestExecutor_SyncBuildWidgetConcurrentSinks races N syncs from the same stale
// ID: the first racer sinks and stores the new card, while concurrent racers
// have their ID forwarded to the new card and edit in place without deleting or
// re-posting duplicate cards.
func TestExecutor_SyncBuildWidgetConcurrentSinks(t *testing.T) {
	t.Parallel()
	const racers = 8
	eri := &mintingEriClient{staleID: "stale"}
	exec := NewExecutor("", eri, nil, "", "example-owner", nil)
	st := &casWidgetStore{stored: "stale"}
	b := model.VanadisBuild{ID: "b1", Title: "Build 1", Status: "running", ThreadID: "th-1", StatusMessageID: "stale"}
	chunks := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Status: "running"}}

	var wg sync.WaitGroup
	for range racers {
		wg.Go(func() {
			id, err := exec.SyncBuildWidget(context.Background(), st, b, chunks, "")
			assert.NoError(t, err)
			assert.Equal(t, "new-1", id)
		})
	}
	wg.Wait()

	assert.Equal(t, 1, st.writes)
	assert.Equal(t, "new-1", st.stored)
	assert.Empty(t, eri.deleted)
	assert.Equal(t, 1, eri.minted)
}

func TestDispatchSilentWakePrompt_OccurrenceDistinguishesFailures(t *testing.T) {
	t.Parallel()
	eri := &eritest.Client{}
	exec := NewExecutor(t.TempDir(), eri, nil, "test-token", "example-owner", nil)
	ctx := context.Background()
	dispatch := func(occurrence string) string {
		t.Helper()
		if err := exec.DispatchSilentWakePrompt(ctx, "th1", "c1", "Deployment failed", occurrence); err != nil {
			t.Fatal(err)
		}
		return eri.LastDispatch.ID
	}
	first := dispatch("2026-11-19T00:00:00Z")
	if redelivered := dispatch("2026-11-19T00:00:00Z"); redelivered != first {
		t.Errorf("redelivery ID = %q, want stable %q", redelivered, first)
	}
	if second := dispatch("2026-11-19T00:00:01Z"); second == first {
		t.Errorf("distinct occurrence reused dispatch ID %q", second)
	}
}

func TestExecuteLaunchChunk_Errors(t *testing.T) {
	t.Parallel()
	chunk := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Mode: "pr", Repo: "aerial"}}
	tests := []struct {
		name         string
		nilExec      bool
		store        *mockStoreForLaunch
		chunks       []model.VanadisChunk
		idx          int
		wantErr      string
		wantDispatch bool
	}{
		{name: "nil executor", nilExec: true, wantErr: "executor or eri client is nil"},
		{name: "no chunks", wantErr: "chunk index 0 out of bounds"},
		{name: "negative index", chunks: chunk, idx: -1, wantErr: "chunk index -1 out of bounds"},
		{name: "index past end", chunks: chunk, idx: 2, wantErr: "chunk index 2 out of bounds"},
		{name: "reserve error only warns", store: &mockStoreForLaunch{reserveErr: errors.New("database locked")}, chunks: chunk, wantDispatch: true},
		{name: "nil store dispatches without reservation", chunks: chunk, wantDispatch: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{}
			exec := NewExecutor("", eri, nil, "", "example-owner", nil)
			if tc.nilExec {
				exec = nil
			}
			var st store.VanadisStore
			if tc.store != nil {
				st = tc.store
			}
			err := exec.ExecuteLaunchChunk(context.Background(), st, model.VanadisBuild{ID: "b1"}, tc.chunks, tc.idx, "")
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantDispatch, eri.LastDispatch != nil)
		})
	}
}

func TestExecuteLaunchChunk_RetryReservation(t *testing.T) {
	t.Parallel()
	unavailable := errors.New("unavailable")
	tests := []struct {
		name         string
		nudges       int
		dispatchErr  error
		reserveErr   error
		wantErr      error
		wantReserved []string
	}{
		{name: "accepted then reserved", nudges: 1, wantReserved: []string{"c"}},
		{name: "retry reservation unavailable after acceptance", nudges: 1, reserveErr: unavailable, wantErr: unavailable, wantReserved: []string{"c"}},
		{name: "retry reservation missing chunk after acceptance", nudges: 1, reserveErr: store.ErrChunkNotFound, wantErr: store.ErrChunkNotFound, wantReserved: []string{"c"}},
		{name: "initial reservation failure only warns", nudges: 0, reserveErr: unavailable, wantReserved: []string{"c"}},
		{name: "deferred launch keeps pre-launch status", nudges: 0, dispatchErr: eriwire.ErrDispatchActive, wantErr: eriwire.ErrDispatchActive},
		{name: "rejected launch keeps pre-launch status", nudges: 1, dispatchErr: unavailable, wantErr: unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{DispatchErr: tc.dispatchErr}
			st := &mockStoreForLaunch{reserveErr: tc.reserveErr}
			executor := NewExecutor(t.TempDir(), eri, nil, "", "example-owner", nil)
			err := executor.ExecuteLaunchChunk(context.Background(), st, model.VanadisBuild{ID: "b"}, []model.VanadisChunk{{ID: "c", Mode: "action", Status: "pending", StallNudges: tc.nudges}}, 0, "")
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, "c", eri.LastDispatch.VanadisChunkID)
			assert.Equal(t, "b", eri.LastDispatch.VanadisBuildID)
			assert.Equal(t, tc.wantReserved, st.reserved)
		})
	}
}

func TestExecutor_ActiveSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nilExec       bool
		buildID       string
		initialSteps  map[string]string
		setSteps      map[string]string
		clearBuildID  string
		inputChunks   []model.VanadisChunk
		expectOverlay map[string]string
	}{
		{
			name:         "nil executor noop",
			nilExec:      true,
			buildID:      "b1",
			setSteps:     map[string]string{"c1": "step 1"},
			clearBuildID: "b1",
			inputChunks:  []model.VanadisChunk{{ID: "c1"}},
		},
		{
			name:        "empty build id noop",
			buildID:     "",
			setSteps:    map[string]string{"c1": "step 1"},
			inputChunks: []model.VanadisChunk{{ID: "c1"}},
		},
		{
			name:        "empty input chunks noop",
			buildID:     "b1",
			setSteps:    map[string]string{"c1": "step 1"},
			inputChunks: nil,
		},
		{
			name:     "all empty steps clears map",
			buildID:  "b1",
			setSteps: map[string]string{"c1": "   ", "c2": ""},
			inputChunks: []model.VanadisChunk{
				{ID: "c1", ActiveStep: ""},
			},
		},
		{
			name:    "set and overlay active steps",
			buildID: "b1",
			setSteps: map[string]string{
				"c1": "⚡ Compiling",
				"c2": "⚡ Testing",
			},
			inputChunks: []model.VanadisChunk{
				{ID: "c1", ActiveStep: ""},
				{ID: "c2", ActiveStep: "existing step"},
				{ID: "c3", ActiveStep: ""},
			},
			expectOverlay: map[string]string{
				"c1": "⚡ Compiling",
				"c2": "existing step",
				"c3": "",
			},
		},
		{
			name:         "clear active steps removes overlay",
			buildID:      "b1",
			initialSteps: map[string]string{"c1": "step 1"},
			clearBuildID: "b1",
			inputChunks:  []model.VanadisChunk{{ID: "c1"}},
			expectOverlay: map[string]string{
				"c1": "",
			},
		},
		{
			name:         "clear active steps with empty buildID noop",
			buildID:      "b1",
			initialSteps: map[string]string{"c1": "step 1"},
			clearBuildID: "",
			inputChunks:  []model.VanadisChunk{{ID: "c1"}},
			expectOverlay: map[string]string{
				"c1": "step 1",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var exec *Executor
			if !tc.nilExec {
				exec = NewExecutor("", &eritest.Client{}, nil, "", "example-owner", nil)
				if tc.initialSteps != nil {
					exec.SetActiveSteps(tc.buildID, tc.initialSteps)
				}
				if tc.setSteps != nil {
					exec.SetActiveSteps(tc.buildID, tc.setSteps)
				}
				if tc.clearBuildID != "" || tc.name == "clear active steps with empty buildID noop" {
					exec.ClearActiveSteps(tc.clearBuildID)
				}
			}

			chunks := make([]model.VanadisChunk, len(tc.inputChunks))
			copy(chunks, tc.inputChunks)

			exec.OverlayActiveSteps(tc.buildID, chunks)

			for _, ch := range chunks {
				if tc.expectOverlay != nil {
					assert.Equal(t, tc.expectOverlay[ch.ID], ch.ActiveStep)
				}
			}
		})
	}
}
