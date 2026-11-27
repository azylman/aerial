package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

const dcKey = "vanadis-chunk-1"

var (
	dcSvc  = DispatchSessionServiceName("aerial")
	dcTurn = turnIDFor(dcKey, tcInv)
)

func runDispatch(id string) sentCall {
	return sentCall{Service: tcRunnerSvc, Method: HandlerRunDispatch, Input: DispatchTurnRequest{SessionKey: dcKey, TurnID: dcTurn, DispatchID: id}}
}

func startedDispatch(id string) *ActiveTurn {
	return &ActiveTurn{TurnID: dcTurn, ItemIDs: []string{id}, RunnerInvocationID: harnessRunnerInvocation}
}

func dispatchSend(method string, input any) sentCall {
	return sentCall{Service: dcSvc, Key: dcKey, Method: method, Input: input}
}

func TestDispatchCoordinator_Enqueue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		key             string
		state           coordinatorState
		rows            []db.Dispatch
		req             eriwire.BrainDispatchRequest
		cancelStep      string
		sabotageSQL     string
		wantResp        eriwire.DispatchEnqueueResponse
		wantSends       []sentCall
		wantRows        map[string]rowWant
		wantTraceparent string
		wantCode        int
	}{
		{
			name:      "EmptyKeyResolvesSessionFromID",
			key:       "",
			req:       eriwire.BrainDispatchRequest{ID: "d9", Prompt: "go"},
			wantResp:  eriwire.DispatchEnqueueResponse{Status: eriwire.StatusAccepted, DispatchID: "d9", SessionID: "dispatch-d9"},
			wantSends: []sentCall{{Service: dcSvc, Key: "dispatch-d9", Method: HandlerAppend, Input: AppendRequest{IDs: []string{"d9"}}}},
			wantRows:  map[string]rowWant{"d9": {status: db.StatusPending}},
		},
		{
			name:            "Accepted",
			key:             dcKey,
			req:             eriwire.BrainDispatchRequest{ID: "d1", Source: "Vanadis", Prompt: "go", Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
			wantResp:        eriwire.DispatchEnqueueResponse{Status: eriwire.StatusAccepted, DispatchID: "d1", SessionID: dcKey},
			wantSends:       []sentCall{dispatchSend(HandlerAppend, AppendRequest{IDs: []string{"d1"}})},
			wantRows:        map[string]rowWant{"d1": {status: db.StatusPending}},
			wantTraceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
		{
			name:      "ExistingIDStillAppendedWithoutSkip",
			key:       dcKey,
			rows:      []db.Dispatch{dispRow("d2", dcKey, db.StatusPending)},
			req:       eriwire.BrainDispatchRequest{ID: "d2", Prompt: "go", SkipIfActive: true},
			wantResp:  eriwire.DispatchEnqueueResponse{Status: eriwire.StatusAlreadyExists, DispatchID: "d2", SessionID: dcKey},
			wantSends: []sentCall{dispatchSend(HandlerAppend, AppendRequest{IDs: []string{"d2"}})},
			wantRows:  map[string]rowWant{"d2": {status: db.StatusPending}},
		},
		{
			name:     "SkipIfActiveBusy",
			key:      dcKey,
			state:    st([]string{"d0"}, nil),
			req:      eriwire.BrainDispatchRequest{ID: "d3", Prompt: "go", SkipIfActive: true},
			wantResp: eriwire.DispatchEnqueueResponse{Status: eriwire.StatusSkippedActive, DispatchID: "d3", SessionID: dcKey},
		},
		{
			name:       "RecordStepCancelSurfacesAsConflict",
			key:        dcKey,
			req:        eriwire.BrainDispatchRequest{ID: "d4", Prompt: "go"},
			cancelStep: StepRecordQueued,
			wantCode:   CodeQueueConflict,
		},
		{
			name:        "RecordStepStoreFailureIsTerminal",
			key:         dcKey,
			req:         eriwire.BrainDispatchRequest{ID: "d6", Prompt: "go"},
			sabotageSQL: failTriggerSQL("dispatches", "INSERT", "23505"),
			wantCode:    500,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedStore(t, nil, tc.rows, tc.sabotageSQL)
			h := newCoordHarness(t, tc.key, tcInv, tc.state)
			h.cancelStep = tc.cancelStep
			resp, err := NewDispatchCoordinator(store, "aerial").Enqueue(h.sharedCtx(), tc.req)
			if assertCoordinatorErr(t, err, tc.wantCode) {
				return
			}
			assert.Equal(t, tc.wantResp, resp)
			assert.Equal(t, tc.wantSends, h.sends)
			assertDispatchRows(t, store, tc.wantRows)
			if tc.wantResp.Status != eriwire.StatusSkippedActive {
				d, getErr := store.GetDispatch(context.Background(), tc.wantResp.DispatchID)
				require.NoError(t, getErr)
				require.NotNil(t, d)
				assert.Equal(t, tc.wantTraceparent, d.Traceparent)
			}
		})
	}
}

func TestDispatchCoordinator_Append(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		state     coordinatorState
		rows      []db.Dispatch
		req       AppendRequest
		wantState coordinatorState
		wantSends []sentCall
		wantRows  map[string]rowWant
	}{
		{
			name:      "OneDispatchPerTurn",
			req:       AppendRequest{IDs: []string{"d1", "d2"}},
			wantState: st([]string{"d2"}, startedDispatch("d1")),
			wantSends: []sentCall{runDispatch("d1")},
		},
		{
			name:      "SkipIfActiveBusyRecordsSkipped",
			state:     st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"d0"}}),
			rows:      []db.Dispatch{dispRow("d5", dcKey, db.StatusPending)},
			req:       AppendRequest{IDs: []string{"d5"}, SkipIfActive: true},
			wantState: st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"d0"}}),
			wantRows:  map[string]rowWant{"d5": {status: db.StatusCompleted, errText: skippedActiveTurnReason}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store coordinatorStore
			var sqlStore *db.SQLStore
			if len(tc.rows) > 0 || len(tc.wantRows) > 0 {
				sqlStore = seedStore(t, nil, tc.rows, "")
				store = sqlStore
			}
			h := newCoordHarness(t, dcKey, tcInv, tc.state)
			require.NoError(t, NewDispatchCoordinator(store, "aerial").Append(h.objectCtx(), tc.req))
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
			assertDispatchRows(t, sqlStore, tc.wantRows)
		})
	}
}

func TestDispatchCoordinator_OnTurnDone(t *testing.T) {
	t.Parallel()
	running := &ActiveTurn{TurnID: "t0", ItemIDs: []string{"d1"}, RunnerInvocationID: "inv-0"}
	tests := []struct {
		name      string
		state     coordinatorState
		rows      []db.Dispatch
		req       TurnDoneRequest
		wantState coordinatorState
		wantSends []sentCall
	}{
		{
			name:      "CompletedRequeuesPendingActiveItem",
			state:     st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"d1", "d2"}, RunnerInvocationID: "inv-0"}),
			req:       TurnDoneRequest{TurnID: "t0", Outcome: OutcomeCompleted, RequeueIDs: []string{"d2"}},
			wantState: st(nil, startedDispatch("d2")),
			wantSends: []sentCall{runDispatch("d2")},
		},
		{name: "StaleTurnFenced", state: st([]string{"d2"}, running), req: TurnDoneRequest{TurnID: "t9", Outcome: OutcomeCompleted}, wantState: st([]string{"d2"}, running)},
		{name: "CompletedStartsNext", state: st([]string{"d2"}, running), req: TurnDoneRequest{TurnID: "t0", Outcome: OutcomeCompleted}, wantState: st(nil, startedDispatch("d2")), wantSends: []sentCall{runDispatch("d2")}},
		{name: "RescheduleRestartsAtFront", state: st([]string{"d2"}, running), req: TurnDoneRequest{TurnID: "t0", Outcome: OutcomeReschedule, RescheduleDelayMs: 1000}, wantState: st([]string{"d2"}, startedDispatch("d1")), wantSends: []sentCall{runDispatch("d1")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store coordinatorStore
			if len(tc.rows) > 0 {
				store = seedStore(t, nil, tc.rows, "")
			}
			h := newCoordHarness(t, dcKey, tcInv, tc.state)
			require.NoError(t, NewDispatchCoordinator(store, "aerial").OnTurnDone(h.objectCtx(), tc.req))
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
		})
	}
}

func TestDispatchCoordinator_Cancel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		state     coordinatorState
		rows      []db.Dispatch
		req       CancelDispatchRequest
		wantResp  CancelDispatchResponse
		wantSends []sentCall
		wantRows  map[string]rowWant
	}{
		{
			name:      "CancelsEverythingOnKey",
			state:     st([]string{"d2"}, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"d1"}, RunnerInvocationID: "inv-r1"}),
			rows:      []db.Dispatch{dispRow("d1", dcKey, db.StatusProcessing), dispRow("d2", dcKey, db.StatusPending)},
			req:       CancelDispatchRequest{BuildID: "b1"},
			wantResp:  CancelDispatchResponse{Status: "ok", CancelledCount: 2, CancelledIDs: []string{"d2", "d1"}},
			wantSends: []sentCall{dispatchSend(HandlerClearPending, ClearPendingRequest{IDs: []string{"d2"}, CancelActive: true, TurnID: "t1"})},
			wantRows:  map[string]rowWant{"d1": {status: db.StatusProcessing}, "d2": {status: db.StatusFailed, errText: cancelledDispatchReason}},
		},
		{
			name:     "IdleNoop",
			wantResp: CancelDispatchResponse{Status: "ok"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store coordinatorStore
			var sqlStore *db.SQLStore
			if len(tc.rows) > 0 || len(tc.wantRows) > 0 {
				sqlStore = seedStore(t, nil, tc.rows, "")
				store = sqlStore
			}
			h := newCoordHarness(t, dcKey, tcInv, tc.state)
			resp, err := NewDispatchCoordinator(store, "aerial").Cancel(h.sharedCtx(), tc.req)
			require.NoError(t, err)
			assert.Equal(t, tc.wantResp, resp)
			assert.Equal(t, tc.wantSends, h.sends)
			assert.Empty(t, h.cancels)
			assertDispatchRows(t, sqlStore, tc.wantRows)
		})
	}
}

func TestDispatchCoordinator_ClearPending(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		state       coordinatorState
		req         ClearPendingRequest
		wantState   coordinatorState
		wantSends   []sentCall
		wantCancels []string
	}{
		{
			name:        "CancelsActiveAndStartsRemaining",
			state:       st([]string{"d2", "d3"}, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"d1"}, RunnerInvocationID: "inv-r1"}),
			req:         ClearPendingRequest{IDs: []string{"d2"}, CancelActive: true, TurnID: "t1"},
			wantState:   st(nil, startedDispatch("d3")),
			wantSends:   []sentCall{runDispatch("d3")},
			wantCancels: []string{"inv-r1"},
		},
		{
			name:      "DropsPendingOnly",
			state:     st([]string{"d2"}, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"d1"}, RunnerInvocationID: "inv-r1"}),
			req:       ClearPendingRequest{IDs: []string{"d2"}},
			wantState: st(nil, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"d1"}, RunnerInvocationID: "inv-r1"}),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, dcKey, tcInv, tc.state)
			require.NoError(t, NewDispatchCoordinator(nil, "aerial").ClearPending(h.objectCtx(), tc.req))
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
			assert.Equal(t, tc.wantCancels, h.cancels)
		})
	}
}

func TestDispatchCoordinator_Status(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		state coordinatorState
		want  CoordinatorStatus
	}{
		{name: "Idle", want: CoordinatorStatus{}},
		{name: "Busy", state: st([]string{"d2"}, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"d1"}}), want: CoordinatorStatus{Pending: []string{"d2"}, Active: &ActiveTurn{TurnID: "t1", ItemIDs: []string{"d1"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, dcKey, tcInv, tc.state)
			got, err := NewDispatchCoordinator(nil, "aerial").Status(h.sharedCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Compile-time proof that *WorkerPool satisfies threadResolver and the SQL store satisfies
// the coordinator store surfaces.
var (
	_ threadResolver           = (*WorkerPool)(nil)
	_ coordinatorStore         = db.Store(nil)
	_ dispatchSessionKeyLister = db.Store(nil)
)
