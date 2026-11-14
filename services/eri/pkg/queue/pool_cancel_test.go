package queue

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type cancelDispatchFixture struct {
	queuedDispatches []*db.Dispatch
	queuedMessages   []*db.Message
	activeDispatch   *db.Dispatch
	persistedStatus  string
}

type cancelDispatchTestCase struct {
	name             string
	nilPool          bool
	dispatchID       string
	buildID          string
	reason           string
	fixture          cancelDispatchFixture
	wantCancelledIDs []string
	wantRemainingIDs []string
	wantInterrupted  bool
	wantCtxCancelled bool
	wantReason       string
	wantSpanStatus   string
	wantSpanCode     codes.Code
}

func assertActiveTurnExecution(t *testing.T, tc cancelDispatchTestCase, activeTE *turnExecution, rec *tracetest.SpanRecorder) {
	t.Helper()
	if activeTE == nil {
		return
	}
	if activeTE.isInterrupted() != tc.wantInterrupted {
		t.Fatalf("activeTE.isInterrupted() = %v, want %v", activeTE.isInterrupted(), tc.wantInterrupted)
	}
	isCancelled := activeTE.turnCtx.Err() != nil
	if isCancelled != tc.wantCtxCancelled {
		t.Fatalf("activeTE context cancelled = %v, want %v", isCancelled, tc.wantCtxCancelled)
	}
	if tc.wantReason != "" && activeTE.getInterruptReason() != tc.wantReason {
		t.Fatalf("activeTE interruptReason = %q, want %q", activeTE.getInterruptReason(), tc.wantReason)
	}

	if activeTE.isInterrupted() {
		activeTE.finalizeInterruptedTurn("claude-opus-5-5")
	}
	activeTE.endTurnSpan()
	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	gotStatus := attrMap(ended[0].Attributes())[attrTurnStatus].AsString()
	if gotStatus != tc.wantSpanStatus {
		t.Fatalf("span %s = %q, want %q", attrTurnStatus, gotStatus, tc.wantSpanStatus)
	}
	if ended[0].Status().Code != tc.wantSpanCode {
		t.Fatalf("span status code = %v, want %v", ended[0].Status().Code, tc.wantSpanCode)
	}
}

func prepareActiveDispatchStore(t *testing.T, tc cancelDispatchTestCase, store db.Store) {
	t.Helper()
	if tc.fixture.activeDispatch != nil && tc.fixture.persistedStatus != "" {
		persisted := *tc.fixture.activeDispatch
		persisted.Status = tc.fixture.persistedStatus
		if _, err := store.InsertDispatch(context.Background(), persisted); err != nil {
			t.Fatalf("InsertDispatch: %v", err)
		}
	}
}

func assertCancelledAndRemainingIDs(t *testing.T, tc cancelDispatchTestCase, gotCancelled []string, ch chan queueItem) {
	t.Helper()
	if len(gotCancelled) != len(tc.wantCancelledIDs) {
		t.Fatalf("cancelled IDs count = %d (%v), want %d (%v)",
			len(gotCancelled), gotCancelled, len(tc.wantCancelledIDs), tc.wantCancelledIDs)
	}
	for _, wantID := range tc.wantCancelledIDs {
		if !slices.Contains(gotCancelled, wantID) {
			t.Fatalf("expected cancelled IDs to contain %q, got %v", wantID, gotCancelled)
		}
	}

	var remainingIDs []string
drainRemaining:
	for {
		select {
		case item := <-ch:
			if item.dispatch != nil {
				remainingIDs = append(remainingIDs, item.dispatch.ID)
			} else if item.msg != nil {
				remainingIDs = append(remainingIDs, item.msg.ID)
			}
		default:
			break drainRemaining
		}
	}
	if len(remainingIDs) != len(tc.wantRemainingIDs) {
		t.Fatalf("remaining IDs count = %d (%v), want %d (%v)",
			len(remainingIDs), remainingIDs, len(tc.wantRemainingIDs), tc.wantRemainingIDs)
	}
	for i, wantID := range tc.wantRemainingIDs {
		if remainingIDs[i] != wantID {
			t.Fatalf("remaining ID at %d = %q, want %q", i, remainingIDs[i], wantID)
		}
	}
}

func runCancelDispatchCase(t *testing.T, tc cancelDispatchTestCase) {
	t.Helper()
	if tc.nilPool {
		var nilP *WorkerPool
		got := nilP.CancelDispatches(tc.dispatchID, tc.buildID, tc.reason)
		if len(got) != 0 {
			t.Fatalf("expected empty cancelled list from nil pool, got %v", got)
		}
		return
	}

	store := setupTestStore(t)
	p := &WorkerPool{
		cfg:         WorkerPoolConfig{Store: store},
		threadChs:   make(map[string]*threadWorkerState),
		activeTurns: make(map[string]*turnExecution),
	}

	ch := make(chan queueItem, 20)
	p.threadChs["test-thread"] = &threadWorkerState{ch: ch}

	for _, d := range tc.fixture.queuedDispatches {
		ch <- queueItem{dispatch: d}
	}
	for _, m := range tc.fixture.queuedMessages {
		ch <- queueItem{msg: m}
	}

	prepareActiveDispatchStore(t, tc, store)

	rec := tracetest.NewSpanRecorder()
	var activeTE *turnExecution
	if tc.fixture.activeDispatch != nil {
		_, turnSpan := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test").Start(context.Background(), runner.OperationInvokeAgent)
		turnCtx, cancelTurn := context.WithCancel(context.Background())
		defer cancelTurn()
		activeTE = &turnExecution{
			pool:          p,
			threadID:      "test-thread",
			dispatch:      tc.fixture.activeDispatch,
			turnCtx:       turnCtx,
			turnCancel:    cancelTurn,
			turnSpan:      turnSpan,
			turnAttempted: true,
			execStart:     time.Now(),
		}
		p.activeTurns["test-thread"] = activeTE
	}

	gotCancelled := p.CancelDispatches(tc.dispatchID, tc.buildID, tc.reason)
	assertCancelledAndRemainingIDs(t, tc, gotCancelled, ch)
	assertActiveTurnExecution(t, tc, activeTE, rec)
}

func TestCancelDispatches(t *testing.T) {
	t.Parallel()

	tests := []cancelDispatchTestCase{
		{
			name:             "nil worker pool returns nil",
			nilPool:          true,
			dispatchID:       "disp-1",
			wantCancelledIDs: nil,
		},
		{
			name:             "empty dispatch and build IDs returns nil",
			dispatchID:       "",
			buildID:          "",
			wantCancelledIDs: nil,
		},
		{
			name:       "drain queued dispatch by dispatch_id",
			dispatchID: "disp-target",
			fixture: cancelDispatchFixture{
				queuedDispatches: []*db.Dispatch{
					{ID: "disp-other", Prompt: "unrelated prompt"},
					{ID: "disp-target", Prompt: "target dispatch"},
				},
				queuedMessages: []*db.Message{
					{ID: "msg-1", Content: "chat message"},
				},
			},
			wantCancelledIDs: []string{"disp-target"},
			wantRemainingIDs: []string{"disp-other", "msg-1"},
		},
		{
			name:    "drain queued dispatch by build_id in prompt",
			buildID: "b-build-42",
			fixture: cancelDispatchFixture{
				queuedDispatches: []*db.Dispatch{
					{ID: "disp-b42-1", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: b-build-42\nChunk 1"},
					{ID: "disp-other", Prompt: "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: b-build-99\nChunk 1"},
				},
			},
			wantCancelledIDs: []string{"disp-b42-1"},
			wantRemainingIDs: []string{"disp-other"},
		},
		{
			name:    "interrupt active dispatch turn by build_id session prefix",
			buildID: "b-build-42",
			fixture: cancelDispatchFixture{
				activeDispatch: &db.Dispatch{
					ID:        "disp-active",
					SessionID: "vanadis-b-build-42-chunk-1",
					Prompt:    "chunk 1 execution",
				},
			},
			wantCancelledIDs: []string{"disp-active"},
			wantInterrupted:  true,
			wantCtxCancelled: true,
			wantReason:       "dispatch cancelled by operator",
			wantSpanStatus:   "cancelled",
			wantSpanCode:     codes.Error,
		},
		{
			name:       "interrupt active dispatch already failed by cancel API records cancelled span",
			dispatchID: "disp-active-failed",
			reason:     "cancelled by vanadis",
			fixture: cancelDispatchFixture{
				activeDispatch: &db.Dispatch{
					ID:     "disp-active-failed",
					Prompt: "webhook dispatch prompt",
				},
				persistedStatus: db.StatusFailed,
			},
			wantCancelledIDs: []string{"disp-active-failed"},
			wantInterrupted:  true,
			wantCtxCancelled: true,
			wantReason:       "cancelled by vanadis",
			wantSpanStatus:   "cancelled",
			wantSpanCode:     codes.Error,
		},
		{
			name:       "interrupt active dispatch turn by dispatch_id with custom reason",
			dispatchID: "disp-active-direct",
			reason:     "superseded by newer task",
			fixture: cancelDispatchFixture{
				activeDispatch: &db.Dispatch{
					ID:     "disp-active-direct",
					Prompt: "active dispatch prompt",
				},
			},
			wantCancelledIDs: []string{"disp-active-direct"},
			wantInterrupted:  true,
			wantCtxCancelled: true,
			wantReason:       "superseded by newer task",
			wantSpanStatus:   "cancelled",
			wantSpanCode:     codes.Error,
		},
		{
			name:       "non matching target preserves active dispatch",
			dispatchID: "disp-unrelated",
			fixture: cancelDispatchFixture{
				activeDispatch: &db.Dispatch{
					ID:     "disp-active",
					Prompt: "active dispatch",
				},
			},
			wantCancelledIDs: nil,
			wantInterrupted:  false,
			wantCtxCancelled: false,
			wantSpanStatus:   "unknown",
			wantSpanCode:     codes.Error,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCancelDispatchCase(t, tc)
		})
	}
}
