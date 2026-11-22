package build

// Shared BuildObject test harness: mockEriClient/launch helpers and the
// unattached-PR spy. Store-backed tests use the Postgres fixture in
// store_fixture_test.go.

import (
	"context"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func attachRequest(chunkID, repo string) store.AttachChunkPRRequest {
	return store.AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

// mockEriClient layers build's behavioural closures (attempt counters,
// prompt/banner capture) over the shared eritest.Client, which records every
// call; error-only cases should prefer the embedded Client's error fields.
type mockEriClient struct {
	eritest.Client

	dispatchFn func(ctx context.Context, req eriwire.BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req executor.StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error
}

func (m *mockEriClient) DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	return m.Dispatch(ctx, req)
}

func (m *mockEriClient) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	err := m.Client.Dispatch(ctx, req)
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return err
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	err := m.Client.CancelDispatch(ctx, chunkID)
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return err
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	err := m.Client.Prompt(ctx, threadID, prompt)
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return err
}

func (m *mockEriClient) Widget(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
	id, err := m.Client.Widget(ctx, req)
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return id, err
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	err := m.Client.Message(ctx, channelID, content)
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return err
}

type mockStoreForLaunch struct {
	store.VanadisStore

	reserveErr error
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	return m.reserveErr
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}

type unattachedPREventSpyStore struct {
	store.VanadisStore

	chunks          []model.VanadisChunk
	getBuildErr     error
	attachErr       error
	attachedChunkID string
	attachedPR      int
	attachedRepo    string
	prStatusUpdated string
	cStatus         string
	banner          string
	failedChunkID   string
	syncWidgetCalls int
}

func (*unattachedPREventSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	return &store.VanadisChunkPRLookup{Matched: false}, nil
}

func (s *unattachedPREventSpyStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	return &model.VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, s.chunks, nil
}

func (s *unattachedPREventSpyStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	if s.attachErr != nil {
		return "", "", "", s.attachErr
	}
	s.attachedChunkID = req.ChunkID
	s.attachedPR = req.PRNumber
	s.attachedRepo = req.Repo
	return "b1", req.ChunkID, "th-1", nil
}

func (s *unattachedPREventSpyStore) UpdatePRLifecycleStatus(_ context.Context, chunkID, status string) error {
	s.prStatusUpdated = chunkID + ":" + status
	return nil
}

func (*unattachedPREventSpyStore) RecordPRCheckStatus(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ResetPRCheckState(_ context.Context, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) RecordPRRolloutState(_ context.Context, _, _ string, _ *pubsub.PRRolloutState) error {
	return nil
}

func (s *unattachedPREventSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.cStatus = chunkID + ":" + status
	s.banner = banner
	return nil
}

func (s *unattachedPREventSpyStore) CompareAndSwapBuildStatusMessageID(context.Context, string, string, string) (bool, error) {
	s.syncWidgetCalls++
	return true, nil
}

func (*unattachedPREventSpyStore) UpdateBuildStatus(_ context.Context, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ReserveChunkForLaunch(_ context.Context, _ string) error {
	return nil
}

func (s *unattachedPREventSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string, _ ...string) (int, error) {
	s.cStatus = chunkID + ":ci_failed"
	s.banner = banner
	return 1, nil
}

func (s *unattachedPREventSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failedChunkID = chunkID + ":" + reason
	return &model.VanadisBuild{ID: "b1", Status: statusRunning, ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, Status: statusFailed}, true, nil
}
