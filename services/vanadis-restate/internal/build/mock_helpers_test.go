package build

// Shared BuildObject test harness: mockEriClient and launch helpers. Store
// fakes live in fakes_test.go; store-backed tests use the Postgres fixture in
// store_fixture_test.go.

import (
	"context"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
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

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}
