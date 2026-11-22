package queue

import (
	"encoding/json"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
)

func TestLegacyRollbackAdapters(t *testing.T) {
	t.Parallel()

	const legacyInv = "inv-legacy"
	leftover := st([]string{"m2"}, startedTurn("m1"))
	threadObj := NewThreadSessionObject(nil, "aerial")
	dispatchObj := NewDispatchSessionObject(nil, "miorine")
	allKeys := []string{"active_invocation_id", StateKeyActive, StateKeyPending}

	tests := []struct {
		name       string
		obj        any
		key        string
		handler    string
		input      string
		activeInv  string
		wantSends  []sentCall
		wantKeys   []string
		wantStatus *CoordinatorStatus
	}{
		{
			name: "thread Append wakes RunTurn", obj: threadObj, key: "t1", handler: HandlerAppend, input: `{"ids":["m3"]}`, activeInv: legacyInv,
			wantSends: []sentCall{threadSend("t1", HandlerRunTurn, ThreadTurnRequest{}, 0)}, wantKeys: allKeys,
		},
		{
			name: "thread OnTurnDone clears coordinator keys", obj: threadObj, key: "t1", handler: HandlerOnTurnDone, input: `{"turn_id":"x","outcome":"completed"}`, activeInv: legacyInv,
			wantSends: []sentCall{threadSend("t1", HandlerRunTurn, ThreadTurnRequest{}, 0)}, wantKeys: []string{"active_invocation_id"},
		},
		{
			name: "thread Kick clears coordinator keys", obj: threadObj, key: "t1", handler: HandlerKick, input: `{"turn_id":"x"}`,
			wantSends: []sentCall{threadSend("t1", HandlerRunTurn, ThreadTurnRequest{}, 0)}, wantKeys: []string{},
		},
		{
			name: "thread ClearPending clears coordinator keys", obj: threadObj, key: "t1", handler: HandlerClearPending, input: `{"ids":["m2"],"cancel_active":true,"turn_id":"x"}`,
			wantSends: []sentCall{threadSend("t1", HandlerRunTurn, ThreadTurnRequest{}, 0)}, wantKeys: []string{},
		},
		{
			name: "thread AdoptPending wakes RunTurn", obj: threadObj, key: "t1", handler: HandlerAdoptPending, input: `{}`, activeInv: legacyInv,
			wantSends: []sentCall{threadSend("t1", HandlerRunTurn, ThreadTurnRequest{}, 0)}, wantKeys: allKeys,
		},
		{
			name: "thread Status reports legacy invocation", obj: threadObj, key: "t1", handler: HandlerStatus, activeInv: legacyInv,
			wantKeys: allKeys, wantStatus: &CoordinatorStatus{Active: &ActiveTurn{TurnID: legacyInv}},
		},
		{
			name: "thread Status idle", obj: threadObj, key: "t1", handler: HandlerStatus,
			wantKeys: []string{StateKeyActive, StateKeyPending}, wantStatus: &CoordinatorStatus{},
		},
		{
			name: "dispatch Append wakes RunTurn", obj: dispatchObj, key: "vanadis-b1", handler: HandlerAppend, input: `{"ids":["d1"],"skip_if_active":true}`,
			wantSends: []sentCall{{Service: "DispatchSession-miorine", Key: "vanadis-b1", Method: HandlerRunTurn, Input: eriwire.BrainDispatchRequest{}}}, wantKeys: []string{StateKeyActive, StateKeyPending},
		},
		{
			name: "dispatch OnTurnDone clears coordinator keys", obj: dispatchObj, key: "vanadis-b1", handler: HandlerOnTurnDone, input: `{"turn_id":"x","outcome":"reschedule","reschedule_delay_ms":5}`, activeInv: legacyInv,
			wantSends: []sentCall{{Service: "DispatchSession-miorine", Key: "vanadis-b1", Method: HandlerRunTurn, Input: eriwire.BrainDispatchRequest{}}}, wantKeys: []string{"active_invocation_id"},
		},
		{
			name: "dispatch ClearPending clears coordinator keys", obj: dispatchObj, key: "vanadis-b1", handler: HandlerClearPending, input: `{}`,
			wantSends: []sentCall{{Service: "DispatchSession-miorine", Key: "vanadis-b1", Method: HandlerRunTurn, Input: eriwire.BrainDispatchRequest{}}}, wantKeys: []string{},
		},
		{
			name: "dispatch AdoptPending wakes RunTurn", obj: dispatchObj, key: "vanadis-b1", handler: HandlerAdoptPending, input: `{}`,
			wantSends: []sentCall{{Service: "DispatchSession-miorine", Key: "vanadis-b1", Method: HandlerRunTurn, Input: eriwire.BrainDispatchRequest{}}}, wantKeys: []string{StateKeyActive, StateKeyPending},
		},
		{
			name: "dispatch Status reports legacy invocation", obj: dispatchObj, key: "vanadis-b1", handler: HandlerStatus, activeInv: legacyInv,
			wantKeys: allKeys, wantStatus: &CoordinatorStatus{Active: &ActiveTurn{TurnID: legacyInv}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, tc.key, "inv-adapter", leftover)
			if tc.activeInv != "" {
				h.state["active_invocation_id"] = mustJSON(t, tc.activeInv)
			}
			handler, ok := restate.Reflect(tc.obj).Handlers()[tc.handler]
			require.True(t, ok, "handler %s not bound", tc.handler)

			var in []byte
			if tc.input != "" {
				in = []byte(tc.input)
			}
			out, err := handler.Call(h.mc, in)
			require.NoError(t, err)

			assert.Equal(t, tc.wantSends, h.sends)
			assert.Empty(t, h.cancels)
			keys := make([]string, 0, len(h.state))
			for k := range h.state {
				keys = append(keys, k)
			}
			assert.ElementsMatch(t, tc.wantKeys, keys)
			if tc.wantStatus != nil {
				var got CoordinatorStatus
				require.NoError(t, json.Unmarshal(out, &got))
				assert.Equal(t, *tc.wantStatus, got)
			}
		})
	}
}
