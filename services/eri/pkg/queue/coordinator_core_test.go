package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

// sentCall is one outbound Restate call recorded by coordHarness.
type sentCall struct {
	Service string
	Key     string
	Method  string
	Input   any
	Delay   time.Duration
}

// coordHarness backs a mocks.MockContext with in-memory object state and records sends,
// requests and invocation cancels.
type coordHarness struct {
	mc        *mocks.MockContext
	state     map[string][]byte
	sends     []sentCall
	cancels   []string
	target    sentCall
	replies   map[string]CancelDispatchResponse
	clearAlls int
}

const harnessRunnerInvocation = "inv-runner"

func newCoordHarness(t *testing.T, key, requestID string, st coordinatorState) *coordHarness {
	t.Helper()
	h := &coordHarness{mc: mocks.NewMockContext(t), state: map[string][]byte{}, replies: map[string]CancelDispatchResponse{}}
	if len(st.pending) > 0 {
		h.state[StateKeyPending] = mustJSON(t, st.pending)
	}
	if st.active != nil {
		h.state[StateKeyActive] = mustJSON(t, st.active)
	}
	mc := h.mc
	mc.On("Key").Return(key).Maybe()
	mc.On("Request").Return(&restate.Request{ID: requestID}).Maybe()
	mc.EXPECT().Get(mock.Anything, mock.Anything).RunAndReturn(func(k string, out any, _ ...restate.GetOption) (bool, restate.TerminalError) {
		b, ok := h.state[k]
		if !ok {
			return false, nil
		}
		require.NoError(t, json.Unmarshal(b, out))
		return true, nil
	}).Maybe()
	mc.On("Set", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		h.state[args.String(0)] = mustJSON(t, args.Get(1))
	}).Return().Maybe()
	mc.On("Clear", mock.Anything).Run(func(args mock.Arguments) { delete(h.state, args.String(0)) }).Return().Maybe()
	mc.On("ClearAll").Run(func(mock.Arguments) {
		h.state = map[string][]byte{}
		h.clearAlls++
	}).Return().Maybe()
	mc.On("CancelInvocation", mock.Anything).Run(func(args mock.Arguments) {
		h.cancels = append(h.cancels, args.String(0))
	}).Return().Maybe()

	runHandler := func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if err != nil {
			return restate.ToTerminalError(err)
		}
		if output != nil {
			require.NoError(t, json.Unmarshal(mustJSON(t, res), output))
		}
		return nil
	}
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(runHandler).Maybe()

	inv := mocks.NewMockInvocation(t)
	inv.On("GetInvocationId").Return(harnessRunnerInvocation).Maybe()
	client := mocks.NewMockClient(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		h.target = sentCall{Service: args.String(0), Key: args.String(1), Method: args.String(2)}
	}).Return(client).Maybe()
	mc.On("Service", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		h.target = sentCall{Service: args.String(0), Method: args.String(1)}
	}).Return(client).Maybe()
	client.On("Send", mock.Anything).Run(func(args mock.Arguments) {
		h.record(args.Get(0), 0)
	}).Return(inv).Maybe()
	client.On("Send", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		h.record(args.Get(0), sendDelay(args.Get(1)))
	}).Return(inv).Maybe()
	client.On("Request", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		h.record(args.Get(0), 0)
		reflect.ValueOf(args.Get(1)).Elem().Set(reflect.ValueOf(h.replies[h.target.Key]))
	}).Return(nil).Maybe()
	return h
}

func (h *coordHarness) record(input any, delay time.Duration) {
	call := h.target
	call.Input = input
	call.Delay = delay
	h.sends = append(h.sends, call)
}

// sendDelay reads the delay carried by a restate.WithDelay send option.
func sendDelay(opt any) time.Duration {
	v := reflect.ValueOf(opt)
	if v.Kind() != reflect.Struct {
		return 0
	}
	f := v.FieldByName("delay")
	if !f.IsValid() {
		return 0
	}
	return time.Duration(f.Int())
}

func (h *coordHarness) objectCtx() restate.ObjectContext { return restate.WithMockContext(h.mc) }

func (h *coordHarness) sharedCtx() restate.ObjectSharedContext {
	return restate.WithMockContext(h.mc)
}

func (h *coordHarness) plainCtx() restate.Context { return restate.WithMockContext(h.mc) }

// snapshot decodes the harness state.
func (h *coordHarness) snapshot(t *testing.T) coordinatorState {
	t.Helper()
	var st coordinatorState
	if b, ok := h.state[StateKeyPending]; ok {
		require.NoError(t, json.Unmarshal(b, &st.pending))
	}
	if b, ok := h.state[StateKeyActive]; ok {
		require.NoError(t, json.Unmarshal(b, &st.active))
	}
	return st
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// turnIDFor is the contract turn ID: uuid.NewSHA1(URL, "aerial:turn:<key>:<invocation>").
func turnIDFor(key, invocationID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("aerial:turn:"+key+":"+invocationID)).String()
}

func msgRow(id, threadID, status string) db.Message {
	return db.Message{ID: id, ThreadID: threadID, Status: status, CreatedAt: time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)}
}

func dispRow(id, sessionID, status string) db.Dispatch {
	return db.Dispatch{ID: id, SessionID: sessionID, Status: status, CreatedAt: time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)}
}

func seedStore(t *testing.T, msgs []db.Message, disps []db.Dispatch) *dbtest.FakeStore {
	t.Helper()
	store := dbtest.NewFakeStore()
	for _, m := range msgs {
		require.NoError(t, store.InsertMessage(context.Background(), m))
	}
	for _, d := range disps {
		_, err := store.InsertDispatch(context.Background(), d)
		require.NoError(t, err)
	}
	return store
}

func assertMessageRows(t *testing.T, store *dbtest.FakeStore, want map[string]rowWant) {
	t.Helper()
	for id, w := range want {
		m, err := store.GetMessage(context.Background(), id)
		require.NoError(t, err)
		require.NotNil(t, m, "message %s", id)
		assert.Equal(t, w.status, m.Status, "message %s status", id)
		assert.Equal(t, w.errText, m.ErrorMessage, "message %s error", id)
	}
}

func assertDispatchRows(t *testing.T, store *dbtest.FakeStore, want map[string]rowWant) {
	t.Helper()
	for id, w := range want {
		d, err := store.GetDispatch(context.Background(), id)
		require.NoError(t, err)
		require.NotNil(t, d, "dispatch %s", id)
		assert.Equal(t, w.status, d.Status, "dispatch %s status", id)
		assert.Equal(t, w.errText, d.ErrorMessage, "dispatch %s error", id)
	}
}

// assertCoordinatorErr checks a handler error against the wanted terminal code (0 = no error).
func assertCoordinatorErr(t *testing.T, err error, wantCode int) bool {
	t.Helper()
	if wantCode == 0 {
		require.NoError(t, err)
		return false
	}
	require.Error(t, err)
	require.True(t, restate.IsTerminalError(err))
	assert.Equal(t, restate.Code(wantCode), restate.AsTerminalError(err).Code())
	assert.NotEqual(t, restate.Code(CodeRestateCancelled), restate.AsTerminalError(err).Code())
	return true
}

var errRestateCancel = restate.ToTerminalError(errors.New("cancelled"), restate.WithErrorCode(CodeRestateCancelled))

func TestClearPendingCancelsActive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		active *ActiveTurn
		req    ClearPendingRequest
		want   bool
	}{
		{name: "NoActive", active: nil, req: ClearPendingRequest{CancelActive: true, TurnID: "t1"}, want: false},
		{name: "NotAsked", active: &ActiveTurn{TurnID: "t1", ItemIDs: []string{"a"}}, req: ClearPendingRequest{TurnID: "t1", IDs: []string{"a"}}, want: false},
		{name: "TurnMatches", active: &ActiveTurn{TurnID: "t1", ItemIDs: []string{"a"}}, req: ClearPendingRequest{CancelActive: true, TurnID: "t1"}, want: true},
		{name: "AllItemsCovered", active: &ActiveTurn{TurnID: "t2", ItemIDs: []string{"b", "c"}}, req: ClearPendingRequest{CancelActive: true, TurnID: "t1", IDs: []string{"b", "c", "d"}}, want: true},
		{name: "PostSnapshotItem", active: &ActiveTurn{TurnID: "t2", ItemIDs: []string{"b", "x"}}, req: ClearPendingRequest{CancelActive: true, TurnID: "t1", IDs: []string{"b"}}, want: false},
		{name: "EmptyTurnIDNoItems", active: &ActiveTurn{TurnID: ""}, req: ClearPendingRequest{CancelActive: true}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, clearPendingCancelsActive(tc.active, tc.req))
		})
	}
}

func TestSplitTurnBatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		pending   []string
		max       int
		wantBatch []string
		wantRest  []string
	}{
		{name: "UnderCap", pending: []string{"a", "b"}, max: 5, wantBatch: []string{"a", "b"}, wantRest: nil},
		{name: "OverCap", pending: []string{"a", "b", "c", "d", "e", "f", "g"}, max: 5, wantBatch: []string{"a", "b", "c", "d", "e"}, wantRest: []string{"f", "g"}},
		{name: "DispatchCap", pending: []string{"a", "b"}, max: 1, wantBatch: []string{"a"}, wantRest: []string{"b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			batch, rest := splitTurnBatch(tc.pending, tc.max)
			assert.Equal(t, tc.wantBatch, batch)
			assert.Equal(t, tc.wantRest, rest)
		})
	}
}

func TestCoordinatorError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantNil  bool
	}{
		{name: "Nil", err: nil, wantNil: true},
		{name: "RestateCancelBecomesConflict", err: errRestateCancel, wantCode: CodeQueueConflict},
		{name: "TerminalKeepsCode", err: restate.ToTerminalError(errors.New("bad"), restate.WithErrorCode(500)), wantCode: 500},
		{name: "BadRequestKept", err: restate.ToTerminalError(errors.New("bad"), restate.WithErrorCode(400)), wantCode: 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := coordinatorError(tc.err)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			assert.Equal(t, restate.Code(tc.wantCode), restate.AsTerminalError(got).Code())
		})
	}
}

func TestCoordinatorRetryableErrorStaysRetryable(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("connection reset")
	assert.Equal(t, err, coordinatorError(err))
}

func TestCoordinatorHandlerKinds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		service any
		want    map[string]string
	}{
		{
			name:    "ThreadCoordinator",
			service: NewThreadCoordinator(nil, nil, "aerial"),
			want: map[string]string{
				HandlerEnqueue: "SHARED", HandlerCancelTurn: "SHARED", HandlerStatus: "SHARED", HandlerAdoptPending: "SHARED",
				HandlerAppend: "EXCLUSIVE", HandlerOnTurnDone: "EXCLUSIVE", HandlerKick: "EXCLUSIVE", HandlerClearPending: "EXCLUSIVE", HandlerRunTurn: "EXCLUSIVE",
			},
		},
		{
			name:    "DispatchCoordinator",
			service: NewDispatchCoordinator(nil, "aerial"),
			want: map[string]string{
				HandlerEnqueue: "SHARED", HandlerCancel: "SHARED", HandlerStatus: "SHARED", HandlerAdoptPending: "SHARED",
				HandlerAppend: "EXCLUSIVE", HandlerOnTurnDone: "EXCLUSIVE", HandlerClearPending: "EXCLUSIVE", HandlerRunTurn: "EXCLUSIVE",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			def := restate.Reflect(tc.service)
			got := map[string]string{}
			for name, h := range def.Handlers() {
				got[name] = fmt.Sprint(*h.HandlerType())
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCoordinatorServiceNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		service interface{ ServiceName() string }
		want    string
	}{
		{name: "ThreadDefault", service: NewThreadCoordinator(nil, nil, ""), want: "ThreadSession"},
		{name: "ThreadMiorine", service: NewThreadCoordinator(nil, nil, "miorine"), want: "ThreadSession-miorine"},
		{name: "DispatchMiorine", service: NewDispatchCoordinator(nil, "miorine"), want: "DispatchSession-miorine"},
		{name: "RouterMiorine", service: NewDispatchCancelRouter(nil, "miorine"), want: "SessionManager-miorine"},
		{name: "RouterDefault", service: NewDispatchCancelRouter(nil, " "), want: "SessionManager"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.service.ServiceName())
		})
	}
}
