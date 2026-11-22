package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

type mockWidgetStore struct {
	store.VanadisStore

	updateErr error
}

func (m *mockWidgetStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return m.updateErr
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
	eri := &eritest.Client{WidgetID: "widget-msg-456"}
	exec := NewExecutor("", eri, nil, "", "example-owner", nil)
	st := &mockWidgetStore{}

	b := model.VanadisBuild{ID: "b1", Title: "Build 1", Status: "running", ThreadID: "th-1"}
	chunks := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Status: "running"}}

	msgID, err := exec.SyncBuildWidget(context.Background(), st, b, chunks, "Notice!")
	if err != nil || msgID != "widget-msg-456" {
		t.Fatalf("SyncBuildWidget msgID = %q err = %v", msgID, err)
	}
	if eri.LastWidget == nil || eri.LastWidget.Banner != "Notice!" {
		t.Errorf("expected banner 'Notice!', got %v", eri.LastWidget)
	}

	// nil executor or eri client
	if id, err := (*Executor)(nil).SyncBuildWidget(context.Background(), nil, b, chunks, ""); id != "" || err != nil {
		t.Error("expected empty string and nil error")
	}

	// eri client error
	execErr := NewExecutor("", &eritest.Client{WidgetErr: errors.New("eri widget failure")}, nil, "", "example-owner", nil)
	if _, err := execErr.SyncBuildWidget(context.Background(), st, b, chunks, ""); err == nil {
		t.Error("expected error on eri failure")
	}

	// update message id error branch
	storeUpdateErr := &mockWidgetStore{updateErr: errors.New("update err")}
	_, _ = exec.SyncBuildWidget(context.Background(), storeUpdateErr, b, chunks, "")
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
