package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

// observeJournalCallback inspects the callback error before the SDK mock adapter
// terminates execution. It does not simulate Restate scheduling or retries.
func observeJournalCallback(t *testing.T, mc *mocks.MockContext, want error, code restate.Code) {
	t.Helper()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if code != 0 {
			var terminal restate.TerminalError
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, code, terminal.Code())
		} else {
			assert.False(t, restate.IsTerminalError(err))
		}
		switch {
		case want != nil && code != 0:
			assert.EqualError(t, err, want.Error())
		case want != nil:
			assert.ErrorIs(t, err, want)
		default:
			assert.NoError(t, err)
		}
		// Only adapt after observing the raw callback error above.
		return runMockRunContext(func(restate.RunContext) (any, error) { return res, err }, output, opts...)
	})
}

func TestRunJournaledCallback(t *testing.T) {
	t.Parallel()
	transient := errors.New("storage unavailable")
	terminal := restate.ToTerminalError(errors.New("already terminal"), restate.WithErrorCode(422))
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"wrapped invalid", fmt.Errorf("validation: %w", ErrInvalidBuildInput), 400},
		{"missing build", ErrBuildNotFound, 404},
		{"missing chunk", ErrChunkNotFound, 404},
		{"conflict", ErrBuildConflict, 409},
		{"already terminal", terminal, 422},
		{"transient", transient, 0},
		{"success", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			observeJournalCallback(t, mc, tc.err, tc.code)
			got, err := runJournaled(restate.WithMockContext(mc), labelUpdateBuild, "update-build-postgres", func(restate.RunContext) (string, error) {
				return "payload", tc.err
			})
			if tc.err == nil {
				require.NoError(t, err)
				assert.Equal(t, "payload", got)
			}
		})
	}
}

type metadataMutationStore struct {
	VanadisStore

	err error
}

func (s *metadataMutationStore) UpdateBuild(_ context.Context, id string, req UpdateBuildRequest) (*VanadisBuild, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &VanadisBuild{ID: id, Title: req.Title}, nil
}

//nolint:tparallel // Requests must run in order against the same object after a conflict.
func TestUpdateBuildConflictThenValid(t *testing.T) {
	t.Parallel()
	store := &metadataMutationStore{err: ErrBuildConflict}
	bo := NewBuildObject("", nil).WithAuthority(store, nil)
	rt := newFakeObjectRuntime("build")
	bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"conflict", ErrBuildConflict, 409},
		{"valid", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store.err = tc.err
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("build")
			observeJournalCallback(t, mc, tc.err, tc.code)
			got, err := bo.UpdateBuild(restate.WithMockContext(mc), UpdateBuildRequest{Title: "Updated"})
			if tc.err == nil {
				require.NoError(t, err)
				assert.Equal(t, VanadisBuild{ID: "build", Title: "Updated"}, got)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func (s *metadataMutationStore) LookupChunkBuildID(context.Context, string) (string, error) {
	return "", s.err
}

func (s *metadataMutationStore) LookupPRChunk(context.Context, string, int) (string, string, error) {
	return "", "", s.err
}

func TestRouterJournalLookupErrors(t *testing.T) {
	t.Parallel()
	transient := errors.New("storage unavailable")
	tests := []struct {
		name        string
		storageErr  error
		callbackErr error
	}{
		{"missing build", ErrBuildNotFound, nil},
		{"missing chunk", ErrChunkNotFound, nil},
		{"storage retry", transient, transient},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			router := &BuildRouterService{store: &metadataMutationStore{err: tc.storageErr}}
			prContext := mocks.NewMockContext(t)
			observeJournalCallback(t, prContext, tc.callbackErr, 0)
			prErr := router.RoutePREvent(restate.WithMockContext(prContext), pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1})
			dispatchContext := mocks.NewMockContext(t)
			observeJournalCallback(t, dispatchContext, tc.callbackErr, 0)
			dispatchErr := router.RouteDispatchOutcome(restate.WithMockContext(dispatchContext), pubsub.DispatchOutcomeEvent{VanadisChunkID: "chunk"})
			if tc.callbackErr == nil {
				assert.NoError(t, prErr)
				assert.NoError(t, dispatchErr)
			} else {
				assert.Error(t, prErr)
				assert.Error(t, dispatchErr)
			}
		})
	}
}
