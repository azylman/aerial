package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

func TestDispatchCancelRouter_CancelDispatch(t *testing.T) {
	t.Parallel()
	vanadis := func(id, session, chunk string) db.Dispatch {
		d := dispRow(id, session, db.StatusProcessing)
		d.VanadisChunkID = chunk
		return d
	}
	tests := []struct {
		name        string
		rows        []db.Dispatch
		req         CancelDispatchControlRequest
		replies     map[string]CancelDispatchResponse
		cancelStep  string
		sabotageSQL string
		wantResp    CancelDispatchResponse
		wantSends   []sentCall
		wantCode    int
	}{
		{
			name: "FansOutInSortedKeyOrder",
			rows: []db.Dispatch{vanadis("d2", "vanadis-b1-chunk-z", "b1-chunk-z"), vanadis("d1", "vanadis-b1-chunk-a", "b1-chunk-a")},
			req:  CancelDispatchControlRequest{BuildID: "b1", SessionKey: "sess-m", Reason: "stop"},
			replies: map[string]CancelDispatchResponse{
				"vanadis-b1-chunk-a": {Status: "ok", CancelledCount: 1, CancelledIDs: []string{"d1"}},
				"vanadis-b1-chunk-z": {Status: "ok", CancelledCount: 1, CancelledIDs: []string{"d2"}},
				"sess-m":             {Status: "ok", CancelledCount: 1, CancelledIDs: []string{"d1"}},
			},
			wantResp: CancelDispatchResponse{Status: "ok", CancelledCount: 2, CancelledIDs: []string{"d1", "d2"}},
			wantSends: []sentCall{
				{Service: dcSvc, Key: "sess-m", Method: HandlerCancel, Input: CancelDispatchRequest{BuildID: "b1", Reason: "stop"}},
				{Service: dcSvc, Key: "vanadis-b1-chunk-a", Method: HandlerCancel, Input: CancelDispatchRequest{BuildID: "b1", Reason: "stop"}},
				{Service: dcSvc, Key: "vanadis-b1-chunk-z", Method: HandlerCancel, Input: CancelDispatchRequest{BuildID: "b1", Reason: "stop"}},
			},
		},
		{
			name:      "ResolvesByDispatchID",
			rows:      []db.Dispatch{dispRow("d3", "dispatch-d3", db.StatusPending)},
			req:       CancelDispatchControlRequest{DispatchID: "d3"},
			replies:   map[string]CancelDispatchResponse{"dispatch-d3": {Status: "ok", CancelledCount: 1, CancelledIDs: []string{"d3"}}},
			wantResp:  CancelDispatchResponse{Status: "ok", CancelledCount: 1, CancelledIDs: []string{"d3"}},
			wantSends: []sentCall{{Service: dcSvc, Key: "dispatch-d3", Method: HandlerCancel, Input: CancelDispatchRequest{DispatchID: "d3"}}},
		},
		{
			name:     "NothingToCancel",
			req:      CancelDispatchControlRequest{BuildID: "none"},
			wantResp: CancelDispatchResponse{Status: "ok"},
		},
		{
			name:       "ResolveStepCancelSurfacesAsConflict",
			req:        CancelDispatchControlRequest{BuildID: "b1"},
			cancelStep: stepResolveDispatchSessionKeys,
			wantCode:   CodeQueueConflict,
		},
		{
			name:        "ResolveStepStoreFailureIsTerminal",
			req:         CancelDispatchControlRequest{BuildID: "b1"},
			sabotageSQL: "DROP TABLE dispatches",
			wantCode:    500,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedStore(t, nil, tc.rows, tc.sabotageSQL)
			h := newCoordHarness(t, "", tcInv, coordinatorState{})
			h.cancelStep = tc.cancelStep
			h.replies = tc.replies
			resp, err := NewDispatchCancelRouter(store, "aerial").CancelDispatch(h.plainCtx(), tc.req)
			if assertCoordinatorErr(t, err, tc.wantCode) {
				return
			}
			assert.Equal(t, tc.wantResp, resp)
			assert.Equal(t, tc.wantSends, h.sends)
		})
	}
}
