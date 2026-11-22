package build

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

type mockActiveStepStore struct {
	store.VanadisStore

	build  *model.VanadisBuild
	chunks []model.VanadisChunk
	getErr error
}

func (m *mockActiveStepStore) GetBuild(_ context.Context, _ string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	return m.build, m.chunks, nil
}

func (*mockActiveStepStore) CompareAndSwapBuildStatusMessageID(context.Context, string, string, string) (bool, error) {
	return true, nil
}

var (
	testActiveBuild = &model.VanadisBuild{
		ID:        "b-active",
		ThreadID:  "th-1",
		Title:     "Active Build",
		Status:    "running",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	testQuietBuild = &model.VanadisBuild{
		ID:        "b-quiet",
		ThreadID:  "th-1",
		Title:     "Quiet Build",
		Status:    "running",
		Quiet:     true,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
)

func TestUpdateActiveStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		buildID      string
		req          UpdateActiveStepRequest
		nilStore     bool
		storeBuild   *model.VanadisBuild
		storeChunks  []model.VanadisChunk
		storeGetErr  error
		widgetErr    error
		expectCode   restate.Code
		expectErrSub string
		expectStep   string
		expectWidget bool
	}{
		{
			name:         "empty chunk_id returns 400 terminal error",
			buildID:      "b-active",
			req:          UpdateActiveStepRequest{ChunkID: "", ActiveStep: "⚡ Compiling..."},
			expectCode:   400,
			expectErrSub: "chunk_id cannot be empty",
		},
		{
			name:       "nil store and executor succeeds and updates live registry",
			buildID:    "b-active",
			req:        UpdateActiveStepRequest{ChunkID: "chunk-nil", ActiveStep: "⚡ Running `verify.sh`..."},
			nilStore:   true,
			expectStep: "⚡ Running `verify.sh`...",
		},
		{
			name:       "step fallback field updates live registry",
			buildID:    "b-active",
			req:        UpdateActiveStepRequest{ChunkID: "chunk-step", Step: "⚡ Running `go test`..."},
			nilStore:   true,
			expectStep: "⚡ Running `go test`...",
		},
		{
			name:        "store build not found ignores widget sync and does not update registry",
			buildID:     "b-active",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-notfound", ActiveStep: "⚡ Compiling..."},
			storeGetErr: store.ErrBuildNotFound,
			expectStep:  "",
		},
		{
			name:        "empty step clears active step in registry",
			buildID:     "b-active",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-clear", ActiveStep: ""},
			storeBuild:  testActiveBuild,
			storeChunks: []model.VanadisChunk{{ID: "chunk-clear", BuildID: "b-active", Title: "Chunk Clear", Status: "running"}},
			expectStep:  "",
		},
		{
			name:        "store get error logs warning, returns ok, and preserves active step in registry",
			buildID:     "b-active",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-err", ActiveStep: "⚡ Compiling..."},
			storeGetErr: errors.New("db connection failure"),
			expectStep:  "⚡ Compiling...",
		},
		{
			name:        "quiet build skips widget delivery",
			buildID:     "b-quiet",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-quiet", ActiveStep: "⚡ Compiling..."},
			storeBuild:  testQuietBuild,
			storeChunks: []model.VanadisChunk{{ID: "chunk-quiet", BuildID: "b-quiet", Title: "Quiet Chunk", Status: "running"}},
			expectStep:  "⚡ Compiling...",
		},
		{
			name:         "active build refreshes widget",
			buildID:      "b-active",
			req:          UpdateActiveStepRequest{ChunkID: "chunk-active", ActiveStep: "⚡ Executing `git status`..."},
			storeBuild:   testActiveBuild,
			storeChunks:  []model.VanadisChunk{{ID: "chunk-active", BuildID: "b-active", Title: "Active Chunk", Status: "running"}},
			expectStep:   "⚡ Executing `git status`...",
			expectWidget: true,
		},
		{
			name:         "widget delivery error is logged as warning and does not abort",
			buildID:      "b-active",
			req:          UpdateActiveStepRequest{ChunkID: "chunk-warn", ActiveStep: "⚡ Running tests..."},
			storeBuild:   testActiveBuild,
			storeChunks:  []model.VanadisChunk{{ID: "chunk-warn", BuildID: "b-active", Title: "Warn Chunk", Status: "running"}},
			widgetErr:    errors.New("discord rate limited"),
			expectStep:   "⚡ Running tests...",
			expectWidget: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var widgetTimedOut bool
			var remainingTimeout time.Duration
			eri := &mockEriClient{
				widgetFn: func(ctx context.Context, _ executor.StatusWidgetRequest) (string, error) {
					if deadline, ok := ctx.Deadline(); ok {
						widgetTimedOut = true
						remainingTimeout = time.Until(deadline)
					}
					return "msg-123", tc.widgetErr
				},
			}
			exec := executor.NewExecutor("", eri, nil, "", "azylman", nil)

			var bo *BuildObject
			if tc.nilStore {
				bo = NewBuildObject("", nil)
			} else {
				st := &mockActiveStepStore{
					build:  tc.storeBuild,
					chunks: tc.storeChunks,
					getErr: tc.storeGetErr,
				}
				bo = NewBuildObject("", nil).WithAuthority(st, exec)
			}

			mc := mocks.NewMockContext(t)
			if tc.req.ChunkID != "" {
				mc.EXPECT().Key().Return(tc.buildID)
				observeJournalCallback(t, mc, nil, tc.expectCode)
			}

			resp, err := bo.UpdateActiveStep(restate.WithMockContext(mc), tc.req)
			if tc.expectErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErrSub)
				if tc.req.ChunkID != "" {
					assert.Empty(t, widget.GetChunkActiveStep(tc.req.ChunkID))
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, AckResponse{Status: "ok"}, resp)
			if tc.expectStep != "" {
				assert.Equal(t, tc.expectStep, widget.GetChunkActiveStep(tc.req.ChunkID))
			}
			if tc.expectWidget {
				assertSyncedWidgetStep(t, eri, tc.req.ChunkID, tc.expectStep)
				assert.True(t, widgetTimedOut, "expected bounded context timeout on widget sync")
				assert.LessOrEqual(t, remainingTimeout, 3*time.Second)
				assert.Positive(t, remainingTimeout)
			}
		})
	}
}

func TestUpdateActiveStep_ConcurrentCoalesce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		buildID     string
		chunk1      string
		step1       string
		chunk2      string
		step2       string
		expectCalls int
	}{
		{
			name:        "concurrent active step updates coalesce with trailing sync",
			buildID:     "b-coalesce",
			chunk1:      "chunk-c1",
			step1:       "⚡ Compiling...",
			chunk2:      "chunk-c2",
			step2:       "⚡ Running tests...",
			expectCalls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bld := &model.VanadisBuild{
				ID:        tc.buildID,
				ThreadID:  "th-1",
				Title:     "Coalesce Build",
				Status:    "running",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}
			chunks := []model.VanadisChunk{
				{ID: tc.chunk1, BuildID: tc.buildID, Title: "Chunk 1", Status: "running"},
				{ID: tc.chunk2, BuildID: tc.buildID, Title: "Chunk 2", Status: "running"},
			}
			st := &mockActiveStepStore{
				build:  bld,
				chunks: chunks,
			}

			var (
				mu           sync.Mutex
				callCount    int
				lastWidget   *executor.StatusWidgetRequest
				firstCall    = make(chan struct{})
				releaseFirst = make(chan struct{})
			)

			eri := &mockEriClient{
				widgetFn: func(_ context.Context, req executor.StatusWidgetRequest) (string, error) {
					mu.Lock()
					callCount++
					currentCall := callCount
					reqCopy := req
					lastWidget = &reqCopy
					mu.Unlock()

					if currentCall == 1 {
						close(firstCall)
						<-releaseFirst
					}
					return "msg-123", nil
				},
			}
			exec := executor.NewExecutor("", eri, nil, "", "azylman", nil)
			bo := NewBuildObject("", nil).WithAuthority(st, exec)

			mc1 := mocks.NewMockContext(t)
			mc1.EXPECT().Key().Return(tc.buildID)
			observeJournalCallback(t, mc1, nil, 0)

			mc2 := mocks.NewMockContext(t)
			mc2.EXPECT().Key().Return(tc.buildID)
			observeJournalCallback(t, mc2, nil, 0)

			var wg sync.WaitGroup
			wg.Go(func() {
				resp1, err1 := bo.UpdateActiveStep(restate.WithMockContext(mc1), UpdateActiveStepRequest{
					ChunkID:    tc.chunk1,
					ActiveStep: tc.step1,
				})
				assert.NoError(t, err1)
				assert.Equal(t, AckResponse{Status: "ok"}, resp1)
			})

			<-firstCall
			resp2, err2 := bo.UpdateActiveStep(restate.WithMockContext(mc2), UpdateActiveStepRequest{
				ChunkID:    tc.chunk2,
				ActiveStep: tc.step2,
			})
			assert.NoError(t, err2)
			assert.Equal(t, AckResponse{Status: "ok"}, resp2)

			close(releaseFirst)
			wg.Wait()

			mu.Lock()
			finalCalls := callCount
			finalWidget := lastWidget
			mu.Unlock()

			assert.Equal(t, tc.expectCalls, finalCalls)
			require.NotNil(t, finalWidget)
			assertSyncedWidgetStep(t, &mockEriClient{LastWidget: finalWidget}, tc.chunk1, tc.step1)
			assertSyncedWidgetStep(t, &mockEriClient{LastWidget: finalWidget}, tc.chunk2, tc.step2)
		})
	}
}

func assertSyncedWidgetStep(t *testing.T, eri *mockEriClient, chunkID, expectStep string) {
	t.Helper()
	require.NotNil(t, eri.LastWidget)
	if expectStep == "" {
		return
	}
	for _, ch := range eri.LastWidget.Chunks {
		if ch.ID == chunkID {
			assert.Equal(t, expectStep, ch.ActiveStep)
			return
		}
	}
	t.Fatalf("expected chunk %s in synced widget chunks", chunkID)
}
