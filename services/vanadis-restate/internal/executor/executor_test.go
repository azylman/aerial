package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type mockEriClient struct {
	dispatchFn func(ctx context.Context, req eriwire.BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error

	lastDispatch *eriwire.BrainDispatchRequest
	lastCancel   string
	lastThreadID string
	lastPrompt   string
	lastWidget   *StatusWidgetRequest
	lastMsgChan  string
	lastMsgCont  string
}

func (m *mockEriClient) DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	return m.Dispatch(ctx, req)
}

type mockStoreForLaunch struct {
	store.VanadisStore

	reserveErr error
	reserved   []string
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockEriClient) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	m.lastDispatch = &req
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return nil
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	m.lastCancel = chunkID
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return nil
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	m.lastThreadID = threadID
	m.lastPrompt = prompt
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return nil
}

func (m *mockEriClient) Widget(ctx context.Context, req StatusWidgetRequest) (string, error) {
	m.lastWidget = &req
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return "msg-123", nil
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	m.lastMsgChan = channelID
	m.lastMsgCont = content
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return nil
}

type mockWidgetStore struct {
	store.VanadisStore

	updateErr error
}

func (m *mockWidgetStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return m.updateErr
}

func TestExecutor_PromptAndDispatch(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor(t.TempDir(), eri, bus, "test-token", "azylman", nil)

	b := model.VanadisBuild{ID: "b1", Title: "Build 1", Objective: "Make everything awesome", ThreadID: "th-1"}
	chunks := []model.VanadisChunk{
		{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Do step 1", Effort: "high"},
		{ID: "c2", Title: "Chunk 2", Repo: "aerial", Instructions: "Do step 2", Mode: chunkModeAction},
	}

	prompt1, err := exec.FormatChunkPrompt(b, chunks, 0)
	if err != nil || !strings.Contains(prompt1, "Chunk 1") {
		t.Fatalf("FormatChunkPrompt 0 failed: %v, prompt: %s", err, prompt1)
	}

	prompt2, err := exec.FormatChunkPrompt(b, chunks, 1)
	if err != nil || !strings.Contains(prompt2, "Chunk 2") {
		t.Fatalf("FormatChunkPrompt 1 failed: %v, prompt: %s", err, prompt2)
	}

	// Test chunk out of bounds
	if _, err := exec.FormatChunkPrompt(b, chunks, 9); err == nil {
		t.Error("expected error on out of bounds chunk index, got nil")
	}

	st := &mockStoreForLaunch{}
	if err := exec.ExecuteLaunchChunk(context.Background(), st, b, chunks, 0, "[NOTICE]\n"); err != nil {
		t.Fatalf("ExecuteLaunchChunk failed: %v", err)
	}
	if eri.lastDispatch == nil || eri.lastDispatch.VanadisChunkID != "c1" || eri.lastDispatch.VanadisBuildID != "b1" {
		t.Errorf("lastDispatch chunkID / buildID = %+v, want chunkID c1, buildID b1", eri.lastDispatch)
	}

	if err := exec.CancelChunkDispatch(context.Background(), "c1"); err != nil || eri.lastCancel != "c1" {
		t.Errorf("CancelChunkDispatch failed: %v", err)
	}
	if err := exec.DispatchWakePrompt(context.Background(), "th-1", "wake"); err != nil || eri.lastPrompt != "wake" {
		t.Errorf("DispatchWakePrompt failed: %v", err)
	}
	if err := exec.DispatchDirectMessage(context.Background(), "ch-1", "msg"); err != nil || eri.lastMsgCont != "msg" {
		t.Errorf("DispatchDirectMessage failed: %v", err)
	}
}

func TestExecutor_SyncBuildWidget(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{
		widgetFn: func(ctx context.Context, req StatusWidgetRequest) (string, error) {
			return "widget-msg-456", nil
		},
	}
	exec := NewExecutor("", eri, nil, "", "azylman", nil)
	st := &mockWidgetStore{}

	b := model.VanadisBuild{ID: "b1", Title: "Build 1", Status: "running", ThreadID: "th-1"}
	chunks := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Status: "running"}}

	msgID, err := exec.SyncBuildWidget(context.Background(), st, b, chunks, "Notice!")
	if err != nil || msgID != "widget-msg-456" {
		t.Fatalf("SyncBuildWidget msgID = %q err = %v", msgID, err)
	}
	if eri.lastWidget == nil || eri.lastWidget.Banner != "Notice!" {
		t.Errorf("expected banner 'Notice!', got %v", eri.lastWidget)
	}

	// nil executor or eri client
	if id, err := (*Executor)(nil).SyncBuildWidget(context.Background(), nil, b, chunks, ""); id != "" || err != nil {
		t.Error("expected empty string and nil error")
	}

	// eri client error
	eriErr := &mockEriClient{
		widgetFn: func(ctx context.Context, req StatusWidgetRequest) (string, error) {
			return "", errors.New("eri widget failure")
		},
	}
	execErr := NewExecutor("", eriErr, nil, "", "azylman", nil)
	if _, err := execErr.SyncBuildWidget(context.Background(), st, b, chunks, ""); err == nil {
		t.Error("expected error on eri failure")
	}

	// update message id error branch
	storeUpdateErr := &mockWidgetStore{updateErr: errors.New("update err")}
	_, _ = exec.SyncBuildWidget(context.Background(), storeUpdateErr, b, chunks, "")
}

func TestExecutor_PublishExternalEvent(t *testing.T) {
	t.Parallel()
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor("", nil, bus, "", "azylman", nil)

	ctx := context.Background()
	if err := exec.PublishExternalEvent(ctx, "vanadis_build", "completed", []string{"b1"}, "all green"); err != nil {
		t.Fatalf("PublishExternalEvent failed: %v", err)
	}
}

func TestExecutor_NilEriAndDirectHelpers(t *testing.T) {
	t.Parallel()
	execNil := NewExecutor("", nil, nil, "", "azylman", nil)
	ctx := context.Background()
	if err := execNil.CancelChunkDispatch(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := execNil.DispatchWakePrompt(ctx, "th1", "prompt"); err != nil {
		t.Fatal(err)
	}
	if err := execNil.DispatchSilentWakePrompt(ctx, "th1", "c1", "prompt", ""); err != nil {
		t.Fatal(err)
	}
	if err := execNil.DispatchDirectMessage(ctx, "ch1", "msg"); err != nil {
		t.Fatal(err)
	}
	if err := execNil.DispatchChunkRemediation(ctx, eriwire.BrainDispatchRequest{}); err == nil {
		t.Error("expected error from nil eri client on DispatchChunkRemediation")
	}

	eri := &mockEriClient{}
	exec := NewExecutor("", eri, nil, "", "azylman", nil)
	if err := exec.CancelChunkDispatch(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if eri.lastCancel != "c1" {
		t.Errorf("lastCancel = %q, want c1", eri.lastCancel)
	}
	if err := exec.DispatchWakePrompt(ctx, "th1", "prompt"); err != nil {
		t.Fatal(err)
	}
	if eri.lastPrompt != "prompt" {
		t.Errorf("lastPrompt = %q, want prompt", eri.lastPrompt)
	}
	if err := exec.DispatchSilentWakePrompt(ctx, "th1", "c1", "silent wake", ""); err != nil {
		t.Fatal(err)
	}
	if eri.lastDispatch == nil || eri.lastDispatch.Prompt != "silent wake" || eri.lastDispatch.TargetID != "th1" || eri.lastDispatch.SessionID != "vanadis-c1" || eri.lastDispatch.VanadisChunkID != "c1" || !eri.lastDispatch.SkipIfActive || eri.lastDispatch.ID == "" {
		t.Errorf("silent dispatch = %+v, want silent dispatch to th1 for c1", eri.lastDispatch)
	}
	if err := exec.DispatchDirectMessage(ctx, "ch1", "msg"); err != nil {
		t.Fatal(err)
	}
	if eri.lastMsgCont != "msg" {
		t.Errorf("lastMsgCont = %q, want msg", eri.lastMsgCont)
	}
}

func TestDispatchSilentWakePrompt_OccurrenceDistinguishesFailures(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	exec := NewExecutor(t.TempDir(), eri, nil, "test-token", "azylman", nil)
	ctx := context.Background()
	dispatch := func(occurrence string) string {
		t.Helper()
		if err := exec.DispatchSilentWakePrompt(ctx, "th1", "c1", "Deployment failed", occurrence); err != nil {
			t.Fatal(err)
		}
		return eri.lastDispatch.ID
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
	ctx := context.Background()

	// nil executor or eri client
	if err := (*Executor)(nil).ExecuteLaunchChunk(ctx, nil, model.VanadisBuild{}, nil, 0, ""); err == nil {
		t.Error("expected error for nil executor")
	}

	exec := NewExecutor("", &mockEriClient{}, nil, "", "azylman", nil)

	// out of bounds
	if err := exec.ExecuteLaunchChunk(ctx, nil, model.VanadisBuild{}, nil, 0, ""); err == nil {
		t.Error("expected error for out of bounds chunk")
	}

	// activeIdx out of bounds
	chunks := []model.VanadisChunk{{ID: "c1"}}
	if err := exec.ExecuteLaunchChunk(ctx, nil, model.VanadisBuild{ID: "b1"}, chunks, -1, ""); err == nil {
		t.Error("expected error for negative activeIdx")
	}
	if err := exec.ExecuteLaunchChunk(ctx, nil, model.VanadisBuild{ID: "b1"}, chunks, 2, ""); err == nil {
		t.Error("expected error for out of range activeIdx")
	}

	// store reserve error branch
	mockStore := &mockStoreForLaunch{reserveErr: errors.New("database locked")}
	execWithStore := NewExecutor("", &mockEriClient{}, nil, "", "azylman", nil)
	chunksForLaunch := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Mode: "pr", Repo: "aerial"}}
	if err := execWithStore.ExecuteLaunchChunk(ctx, mockStore, model.VanadisBuild{ID: "b1"}, chunksForLaunch, 0, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// TargetSymbols span rendering in FormatChunkPrompt
	tmpShare := t.TempDir()
	aerialDir := filepath.Join(tmpShare, "aerial")
	_ = os.MkdirAll(aerialDir, 0o755)
	_ = os.WriteFile(filepath.Join(aerialDir, "main.go"), []byte("package main\n\nfunc MySpecialSym() {\n}\n"), 0o644)
	execShare := NewExecutor(tmpShare, &mockEriClient{}, nil, "", "azylman", nil)
	promptWithSpans, err := execShare.FormatChunkPrompt(model.VanadisBuild{ID: "b1"}, []model.VanadisChunk{{
		ID:            "c1",
		Title:         "Chunk 1",
		Mode:          "pr",
		Repo:          "aerial",
		TargetFiles:   []string{"main.go"},
		TargetSymbols: []string{"MySpecialSym"},
	}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(promptWithSpans, "Target Symbol Spans:") {
		t.Errorf("expected Target Symbol Spans in prompt, got:\n%s", promptWithSpans)
	}

	// PublishExternalEvent
	mockB := &mockBusExecutor{}
	execBus := NewExecutor("", nil, mockB, "", "azylman", nil)
	if err := execBus.PublishExternalEvent(ctx, "source", "type", []string{"t1"}, "ctx"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockB.lastSubj != "aerial.events.source.type" {
		t.Errorf("lastSubj = %q, want aerial.events.source.type", mockB.lastSubj)
	}
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	m.reserved = append(m.reserved, chunkID)
	return m.reserveErr
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}

func TestExecutorCoordinatorBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Nil-executor CI delegation falls back to details.
	if got, err := (*Executor)(nil).ResolveCIFailureDetailsChecked(ctx, "aerial", 1, "unit", "sha", "", "details"); got != "details" || err != nil {
		t.Errorf("nil ResolveCIFailureDetailsChecked = (%q, %v), want (details, nil)", got, err)
	}

	// Coordinator with deploy UI URL and explicit share root.
	eri := &mockEriClient{}
	exec := NewExecutor("/custom-share", eri, nil, "tok", "azylman", nil, "https://flux.example.com")
	if exec.shareRoot != "/custom-share" || exec.deployUIURL != "https://flux.example.com" {
		t.Errorf("unexpected executor config: %+v", exec)
	}

	// Successful remediation dispatch records the request.
	req := eriwire.BrainDispatchRequest{ID: "d1", Source: "test"}
	if err := exec.DispatchChunkRemediation(ctx, req); err != nil {
		t.Fatalf("DispatchChunkRemediation failed: %v", err)
	}
	if eri.lastDispatch == nil || eri.lastDispatch.ID != "d1" {
		t.Errorf("lastDispatch = %+v, want d1", eri.lastDispatch)
	}

	// Nil store skips reservation and dispatches directly.
	chunks := []model.VanadisChunk{{ID: "c1", Title: "Chunk 1", Mode: "pr", Repo: "aerial"}}
	if err := exec.ExecuteLaunchChunk(ctx, nil, model.VanadisBuild{ID: "b1"}, chunks, 0, ""); err != nil {
		t.Fatalf("ExecuteLaunchChunk with nil store failed: %v", err)
	}

	// Nil bus publishes nothing and reports no error.
	execNoBus := NewExecutor("", nil, nil, "", "azylman", nil)
	if err := execNoBus.PublishExternalEvent(ctx, "source", "type", []string{"t1"}, "ctx"); err != nil {
		t.Fatalf("PublishExternalEvent with nil bus failed: %v", err)
	}

	// Hand-constructed executor without an HTTP client resolves to details, not a fetch.
	bare := &Executor{gitHubToken: "t"}
	if got, err := bare.ResolveCIFailureDetailsChecked(ctx, "aerial", 0, "unit", "sha", "", "details"); got != "details" || err != nil {
		t.Errorf("bare executor details = (%q, %v), want (details, nil)", got, err)
	}
}

func TestExecutorCILogDelegation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Accept") != "application/vnd.github+json":
			w.WriteHeader(http.StatusUnauthorized)
		case strings.Contains(r.URL.Path, "/comments"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("--- FAIL: TestDelegated (0.01s)\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	exec := NewExecutor("", nil, nil, "token", "azylman", srv.Client())
	exec.GitHubAPIURL = srv.URL

	// Log fetch success delegates through the coordinator.
	got, err := exec.ResolveCIFailureDetailsChecked(ctx, "aerial", 0, "unit", "abc", "https://github.com/o/r/actions/runs/1/job/42", "failure")
	if err != nil || !strings.Contains(got, "FAIL: TestDelegated") {
		t.Errorf("delegated checked = (%q, %v), want FAIL log", got, err)
	}
	// RoboRev errors propagate through the checked path.
	if _, err := exec.ResolveCIFailureDetailsChecked(ctx, "aerial", 10, "roborev", "abc123", "", "failure"); err == nil {
		t.Error("expected delegated error for 500 comments, got nil")
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
			dispatchErr := tc.dispatchErr
			eri := &mockEriClient{dispatchFn: func(context.Context, eriwire.BrainDispatchRequest) error { return dispatchErr }}
			st := &mockStoreForLaunch{reserveErr: tc.reserveErr}
			executor := NewExecutor(t.TempDir(), eri, nil, "", "azylman", nil)
			err := executor.ExecuteLaunchChunk(context.Background(), st, model.VanadisBuild{ID: "b"}, []model.VanadisChunk{{ID: "c", Mode: "action", Status: "pending", StallNudges: tc.nudges}}, 0, "")
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			require.NotNil(t, eri.lastDispatch)
			assert.Equal(t, tc.wantReserved, st.reserved)
		})
	}
}
