package build

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/restatetest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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

// observedActiveSteps maps chunk ID to the active step the update published:
// the synced widget's chunks when a widget was sent, else the executor's live
// overlay on the stored chunks.
func observedActiveSteps(eri *mockEriClient, exec *executor.Executor, buildID string, stored []model.VanadisChunk) map[string]string {
	out := map[string]string{}
	if eri.LastWidget != nil {
		for _, ch := range eri.LastWidget.Chunks {
			out[ch.ID] = ch.ActiveStep
		}
		return out
	}
	chunks := slices.Clone(stored)
	for i := range chunks {
		chunks[i].ActiveStep = ""
	}
	exec.OverlayActiveSteps(buildID, chunks)
	for _, ch := range chunks {
		out[ch.ID] = ch.ActiveStep
	}
	return out
}

type updateActiveStepTestCase struct {
	name            string
	buildID         string
	req             UpdateActiveStepRequest
	nilStore        bool
	storeBuild      *model.VanadisBuild
	storeChunks     []model.VanadisChunk
	storeGetErr     error
	widgetErr       error
	expectCode      restate.Code
	expectErrSub    string
	expectStep      string
	expectOtherStep string
	expectCleared   bool
	expectWidget    bool
}

func executeUpdateActiveStepTestCase(t *testing.T, tc updateActiveStepTestCase) {
	t.Helper()
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
		st := &fakeStore{chunks: slices.Clone(tc.storeChunks), errs: map[string]error{"GetBuild": tc.storeGetErr}}
		if tc.storeBuild != nil {
			st.builds = []model.VanadisBuild{*tc.storeBuild}
		}
		bo = NewBuildObject("", nil).WithAuthority(st, exec)
	}

	mc := mocks.NewMockContext(t)
	if tc.req.ChunkID != "" || len(tc.req.ActiveSteps) > 0 || tc.req.ActiveSteps != nil || tc.req.ClearAll {
		mc.EXPECT().Key().Return(tc.buildID)
		restatetest.ObserveJournalCallback(t, mc, nil, tc.expectCode)
	}

	resp, err := bo.UpdateActiveStep(restate.WithMockContext(mc), tc.req)
	if tc.expectErrSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.expectErrSub)
		return
	}
	require.NoError(t, err)
	assert.Equal(t, AckResponse{Status: "ok"}, resp)
	observed := observedActiveSteps(eri, exec, tc.buildID, tc.storeChunks)
	if tc.expectCleared {
		for i := range tc.storeChunks {
			assert.Empty(t, observed[tc.storeChunks[i].ID])
		}
	} else {
		if tc.expectStep != "" && len(tc.storeChunks) > 0 {
			assert.Equal(t, tc.expectStep, observed[tc.storeChunks[0].ID])
		}
		if tc.expectOtherStep != "" && len(tc.storeChunks) > 1 {
			assert.Equal(t, tc.expectOtherStep, observed[tc.storeChunks[1].ID])
		}
	}
	if tc.expectWidget {
		assertSyncedWidgetStep(t, eri, tc.req.ChunkID, tc.expectStep, tc.expectCleared)
		assert.True(t, widgetTimedOut, "expected bounded context timeout on widget sync")
		assert.LessOrEqual(t, remainingTimeout, 3*time.Second)
		assert.Positive(t, remainingTimeout)
	}
}

func TestUpdateActiveStep(t *testing.T) {
	t.Parallel()

	tests := []updateActiveStepTestCase{
		{
			name:         "empty chunk_id and empty active_steps returns 400 terminal error",
			buildID:      "b-active",
			req:          UpdateActiveStepRequest{ChunkID: ""},
			expectCode:   400,
			expectErrSub: "chunk_id cannot be empty without active_steps",
		},
		{
			name:    "empty chunk_id with non-empty active_steps map succeeds",
			buildID: "b-active",
			req: UpdateActiveStepRequest{
				ActiveSteps: map[string]string{
					"chunk-step": "⚡ Compiling...",
				},
			},
			storeBuild:  testActiveBuild,
			storeChunks: []model.VanadisChunk{{ID: "chunk-step", BuildID: "b-active", Title: "Step Chunk", Status: "running"}},
			expectStep:  "⚡ Compiling...",
		},
		{
			name:     "nil store and executor succeeds without panicking",
			buildID:  "b-active",
			req:      UpdateActiveStepRequest{ChunkID: "chunk-nil", ActiveStep: "⚡ Running `verify.sh`..."},
			nilStore: true,
		},
		{
			name:         "step fallback field updates chunk active step",
			buildID:      "b-active",
			req:          UpdateActiveStepRequest{ChunkID: "chunk-step", Step: "⚡ Running `go test`..."},
			storeBuild:   testActiveBuild,
			storeChunks:  []model.VanadisChunk{{ID: "chunk-step", BuildID: "b-active", Title: "Step Chunk", Status: "running"}},
			expectStep:   "⚡ Running `go test`...",
			expectWidget: true,
		},
		{
			name:        "store build not found ignores widget sync",
			buildID:     "b-active",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-notfound", ActiveStep: "⚡ Compiling..."},
			storeGetErr: store.ErrBuildNotFound,
		},
		{
			name:        "empty step clears active step on chunk",
			buildID:     "b-active",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-clear", ActiveStep: ""},
			storeBuild:  testActiveBuild,
			storeChunks: []model.VanadisChunk{{ID: "chunk-clear", BuildID: "b-active", Title: "Chunk Clear", Status: "running", ActiveStep: "old"}},
			expectStep:  "",
		},
		{
			name:        "store get error logs warning and returns ok",
			buildID:     "b-active",
			req:         UpdateActiveStepRequest{ChunkID: "chunk-err", ActiveStep: "⚡ Compiling..."},
			storeGetErr: errors.New("db connection failure"),
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
		{
			name:    "active steps map updates multiple chunks",
			buildID: "b-active",
			req: UpdateActiveStepRequest{
				ChunkID:    "chunk-1",
				ActiveStep: "⚡ Compiling binary...",
				ActiveSteps: map[string]string{
					"chunk-1": "⚡ Compiling binary...",
					"chunk-2": "⚡ Linting code...",
				},
			},
			storeBuild: testActiveBuild,
			storeChunks: []model.VanadisChunk{
				{ID: "chunk-1", BuildID: "b-active", Title: "C1", Status: "running"},
				{ID: "chunk-2", BuildID: "b-active", Title: "C2", Status: "running"},
			},
			expectStep:   "⚡ Compiling binary...",
			expectWidget: true,
		},
		{
			name:       "empty chunk_id with clear_all=true clears active steps",
			buildID:    "b-active",
			req:        UpdateActiveStepRequest{ClearAll: true},
			storeBuild: testActiveBuild,
			storeChunks: []model.VanadisChunk{
				{ID: "chunk-1", BuildID: "b-active", Title: "Chunk 1", Status: "running", ActiveStep: "old-1"},
				{ID: "chunk-2", BuildID: "b-active", Title: "Chunk 2", Status: "running", ActiveStep: "old-2"},
			},
			expectStep:    "",
			expectCleared: true,
			expectWidget:  true,
		},
		{
			name:       "empty chunk_id with empty active_steps map clears active steps",
			buildID:    "b-active",
			req:        UpdateActiveStepRequest{ActiveSteps: map[string]string{}},
			storeBuild: testActiveBuild,
			storeChunks: []model.VanadisChunk{
				{ID: "chunk-1", BuildID: "b-active", Title: "Chunk 1", Status: "running", ActiveStep: "old-1"},
			},
			expectStep:    "",
			expectCleared: true,
			expectWidget:  true,
		},
		{
			name:       "single-chunk fallback update preserves other chunk active steps",
			buildID:    "b-active",
			req:        UpdateActiveStepRequest{ChunkID: "chunk-1", ActiveStep: "new step"},
			storeBuild: testActiveBuild,
			storeChunks: []model.VanadisChunk{
				{ID: "chunk-1", BuildID: "b-active", Title: "Chunk 1", Status: "running", ActiveStep: "old-1"},
				{ID: "chunk-2", BuildID: "b-active", Title: "Chunk 2", Status: "running", ActiveStep: "preserve-2"},
			},
			expectStep:      "new step",
			expectOtherStep: "preserve-2",
			expectWidget:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executeUpdateActiveStepTestCase(t, tc)
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
			st := &fakeStore{builds: []model.VanadisBuild{*bld}, chunks: chunks}

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
			restatetest.ObserveJournalCallback(t, mc1, nil, 0)

			mc2 := mocks.NewMockContext(t)
			mc2.EXPECT().Key().Return(tc.buildID)
			restatetest.ObserveJournalCallback(t, mc2, nil, 0)

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

func assertSyncedWidgetStep(t *testing.T, eri *mockEriClient, chunkID, expectStep string, expectCleared ...bool) {
	t.Helper()
	require.NotNil(t, eri.LastWidget)
	assert.True(t, eri.LastWidget.SkipSink, "active step widget sync must set SkipSink = true")
	if len(expectCleared) > 0 && expectCleared[0] {
		for _, ch := range eri.LastWidget.Chunks {
			assert.Empty(t, ch.ActiveStep, "expected chunk %s active step to be cleared", ch.ID)
		}
		return
	}
	if chunkID == "" || expectStep == "" {
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

func TestSyncBuildWidget_PreservesActiveStep(t *testing.T) {
	t.Parallel()

	var lastWidget executor.StatusWidgetRequest
	eri := &mockEriClient{
		widgetFn: func(_ context.Context, req executor.StatusWidgetRequest) (string, error) {
			lastWidget = req
			return "msg-123", nil
		},
	}
	exec := executor.NewExecutor("", eri, nil, "", "azylman", nil)
	exec.SetActiveSteps("b-test", map[string]string{
		"c-running": "⚡ Compiling packages...",
	})

	bld := model.VanadisBuild{ID: "b-test", Title: "Build", Status: "running"}
	// Non-active-step widget sync: caller passes chunks with empty ActiveStep (e.g. loaded fresh from DB)
	chunks := []model.VanadisChunk{
		{ID: "c-running", BuildID: "b-test", Title: "Chunk 1", Status: "running", ActiveStep: ""},
		{ID: "c-other", BuildID: "b-test", Title: "Chunk 2", Status: "pending", ActiveStep: ""},
	}

	_, err := exec.SyncBuildWidget(context.Background(), nil, bld, chunks, "Status changed")
	require.NoError(t, err)
	require.Len(t, lastWidget.Chunks, 2)
	assert.Equal(t, "⚡ Compiling packages...", lastWidget.Chunks[0].ActiveStep, "non-active-step widget sync must preserve active step from executor overlay")
	assert.Empty(t, lastWidget.Chunks[1].ActiveStep)
}
