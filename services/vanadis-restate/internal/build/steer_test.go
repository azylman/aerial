package build

import (
	"context"
	"errors"
	"strings"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/restatetest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type steerMockStore struct {
	store.VanadisStore

	steerRes *store.SteerChunkResult
	steerErr error

	build  *model.VanadisBuild
	chunks []model.VanadisChunk
	getErr error
}

func (s *steerMockStore) SteerChunk(_ context.Context, _, _ string, _ store.SteerChunkRequest) (*store.SteerChunkResult, error) {
	return s.steerRes, s.steerErr
}

func (s *steerMockStore) GetBuild(_ context.Context, _ string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return s.build, s.chunks, s.getErr
}

func makeSteerMockContext(t *testing.T, key string) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Key().Return(key).Maybe()
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-test"}).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(restatetest.RunMockRunContext).Maybe()
	return restate.WithMockContext(mc)
}

func TestBuildObject_SteerChunk(t *testing.T) {
	t.Parallel()
	transient := errors.New("db temporarily unavailable")

	tests := []struct {
		name          string
		buildKey      string
		req           store.SteerChunkRequest
		nilStore      bool
		storeSteerErr error
		steerStatus   string
		nilExec       bool
		getErr        error
		wantAccepted  bool
		wantDispatch  bool
		wantErr       bool
		terminalCode  restate.Code
	}{
		{
			name:         "empty chunk ref validation failure",
			buildKey:     "b1",
			req:          store.SteerChunkRequest{Prompt: "test prompt"},
			wantErr:      true,
			terminalCode: 400,
		},
		{
			name:         "empty prompt validation failure",
			buildKey:     "b1",
			req:          store.SteerChunkRequest{ChunkRef: "c1", Prompt: "  "},
			wantErr:      true,
			terminalCode: 400,
		},
		{
			name:         "nil store returns accepted fallback",
			buildKey:     "b1",
			req:          store.SteerChunkRequest{ChunkRef: "c1", Prompt: "test prompt"},
			nilStore:     true,
			wantAccepted: true,
		},
		{
			name:          "store conflict error maps to 412",
			buildKey:      "b1",
			req:           store.SteerChunkRequest{ChunkRef: "c1", Prompt: "test prompt"},
			storeSteerErr: store.ErrBuildConflict,
			wantErr:       true,
			terminalCode:  412,
		},
		{
			name:          "store not found error maps to 404",
			buildKey:      "b1",
			req:           store.SteerChunkRequest{ChunkRef: "c1", Prompt: "test prompt"},
			storeSteerErr: store.ErrChunkNotFound,
			wantErr:       true,
			terminalCode:  404,
		},
		{
			name:         "pending chunk steered without eri dispatch",
			buildKey:     "b1",
			req:          store.SteerChunkRequest{ChunkRef: "c1", Prompt: "operator guidance"},
			steerStatus:  "pending",
			wantAccepted: true,
			wantDispatch: false,
		},
		{
			name:         "running chunk steered with eri dispatch",
			buildKey:     "b1",
			req:          store.SteerChunkRequest{ChunkRef: "c1", Prompt: "operator guidance"},
			steerStatus:  "running",
			wantAccepted: true,
			wantDispatch: true,
		},
		{
			name:         "running chunk with nil executor succeeds without dispatch",
			buildKey:     "b1",
			req:          store.SteerChunkRequest{ChunkRef: "c1", Prompt: "operator guidance"},
			steerStatus:  "running",
			nilExec:      true,
			wantAccepted: true,
			wantDispatch: false,
		},
		{
			name:        "running chunk get build error propagates",
			buildKey:    "b1",
			req:         store.SteerChunkRequest{ChunkRef: "c1", Prompt: "operator guidance"},
			steerStatus: "running",
			getErr:      transient,
			wantErr:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := makeSteerMockContext(t, tc.buildKey)

			var st store.VanadisStore
			if !tc.nilStore {
				st = &steerMockStore{
					steerRes: &store.SteerChunkResult{
						Accepted:     true,
						BuildID:      tc.buildKey,
						ChunkID:      tc.req.ChunkRef,
						ChunkStatus:  tc.steerStatus,
						Instructions: "existing\n\n### [Operator Steering]\n" + tc.req.Prompt,
					},
					steerErr: tc.storeSteerErr,
					build:    &model.VanadisBuild{ID: tc.buildKey, Agent: "aerial", ThreadID: "th-1"},
					chunks:   []model.VanadisChunk{{ID: tc.req.ChunkRef, BuildID: tc.buildKey, Status: tc.steerStatus, Effort: "high"}},
					getErr:   tc.getErr,
				}
			}

			eri := &eritest.Client{}
			var exec *executor.Executor
			if !tc.nilExec {
				exec = executor.NewExecutor("", eri, nil, "tok", "owner", nil)
			}
			bo := NewBuildObject("", nil).WithAuthority(st, exec)

			res, err := bo.SteerChunk(ctx, tc.req)
			if tc.wantErr {
				require.Error(t, err)
				if tc.terminalCode != 0 {
					var term restate.TerminalError
					require.ErrorAs(t, err, &term)
					assert.Equal(t, tc.terminalCode, term.Code())
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantAccepted, res.Accepted)
			if tc.wantDispatch {
				assert.NotEmpty(t, res.DispatchID)
				assert.True(t, strings.HasPrefix(res.DispatchID, "vanadis-"+tc.req.ChunkRef+"-steer-"))
				require.NotNil(t, eri.LastDispatch)
				assert.Equal(t, res.DispatchID, eri.LastDispatch.ID)
			} else {
				assert.Empty(t, res.DispatchID)
			}
		})
	}
}

func TestOnDispatchOutcome_IgnoresSteerDispatch(t *testing.T) {
	t.Parallel()
	ctx := makeSteerMockContext(t, "b1")
	st := &steerMockStore{}
	bo := NewBuildObject("", nil).WithAuthority(st, nil)

	ev := pubsub.DispatchOutcomeEvent{
		VanadisChunkID: "c1",
		DispatchID:     "vanadis-c1-steer-abcdef123456",
		Status:         "completed",
	}
	err := bo.OnDispatchOutcome(ctx, ev)
	assert.NoError(t, err)
}

func TestBuildObject_SteerChunk_TransientErrorStaysRetryable(t *testing.T) {
	t.Parallel()
	transient := errors.New("db temporarily unavailable")
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Key().Return("b1").Maybe()
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-test"}).Maybe()
	restatetest.ObserveJournalCallback(t, mc, transient, 0)

	st := &steerMockStore{
		steerErr: transient,
		build:    &model.VanadisBuild{ID: "b1", Agent: "aerial", ThreadID: "th-1"},
	}
	bo := NewBuildObject("", nil).WithAuthority(st, nil)
	_, err := bo.SteerChunk(restate.WithMockContext(mc), store.SteerChunkRequest{ChunkRef: "c1", Prompt: "test prompt"})
	require.Error(t, err)
}
