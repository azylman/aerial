package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

const (
	tcKey = "th-1"
	tcInv = "inv-1"
)

var (
	tcThreadSvc = ThreadSessionServiceName("aerial")
	tcRunnerSvc = TurnRunnerServiceName("aerial")
	tcTurn      = turnIDFor(tcKey, tcInv)
	// tcDerivedMsgID is uuid.NewSHA1(URL, "aerial:thread:msg:th-1:inv-1").
	tcDerivedMsgID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("aerial:thread:msg:"+tcKey+":"+tcInv)).String()
)

// fakeThreadResolver redirects to redirect (when set) and resolves to final (when set).
type fakeThreadResolver struct {
	redirect string
	final    string
}

func (f fakeThreadResolver) RedirectBuildPrompt(channelID, _ string) string {
	if f.redirect != "" {
		return f.redirect
	}
	return channelID
}

func (f fakeThreadResolver) ResolveTargetThread(channelID, _, _ string, _ time.Time) string {
	if f.final != "" {
		return f.final
	}
	return channelID
}

type fakeIngressWakeResolver struct {
	fakeThreadResolver

	shouldEnqueue   bool
	completedReason string
}

func (f fakeIngressWakeResolver) EvaluateIngressWake(_ context.Context, _ string, _ db.Message) (bool, string) {
	return f.shouldEnqueue, f.completedReason
}

func st(pending []string, active *ActiveTurn) coordinatorState {
	return coordinatorState{pending: pending, active: active}
}

func runBurst(ids ...string) sentCall {
	return sentCall{Service: tcRunnerSvc, Method: HandlerRunBurst, Input: BurstTurnRequest{ThreadID: tcKey, TurnID: tcTurn, MessageIDs: ids}}
}

func runGatedBurst(ids ...string) sentCall {
	return sentCall{Service: tcRunnerSvc, Method: HandlerRunBurst, Input: BurstTurnRequest{ThreadID: tcKey, TurnID: tcTurn, MessageIDs: ids, Gated: true}}
}

func startedTurn(ids ...string) *ActiveTurn {
	return &ActiveTurn{TurnID: tcTurn, ItemIDs: ids, RunnerInvocationID: harnessRunnerInvocation}
}

func threadSend(key, method string, input any, delay time.Duration) sentCall {
	return sentCall{Service: tcThreadSvc, Key: key, Method: method, Input: input, Delay: delay}
}

func TestThreadCoordinator_Enqueue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		key             string
		state           coordinatorState
		rows            []db.Message
		resolver        threadResolver
		req             ThreadEnqueueRequest
		cancelStep      string
		sabotageSQL     string
		wantResp        ThreadEnqueueResponse
		wantSends       []sentCall
		wantRows        map[string]rowWant
		wantNoRow       string
		wantTraceparent string
		wantCode        int
	}{
		{
			name:      "PromptAccepted",
			key:       tcKey,
			req:       ThreadEnqueueRequest{Prompt: "hello", MessageID: "m1"},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: "m1"},
			wantSends: []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m1"}}, 0)},
			wantRows:  map[string]rowWant{"m1": {status: db.StatusPending}},
		},
		{
			name:            "PromptWithTraceparent",
			key:             tcKey,
			req:             ThreadEnqueueRequest{Prompt: "traced prompt", MessageID: "m-tp", Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
			wantResp:        ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: "m-tp"},
			wantSends:       []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m-tp"}}, 0)},
			wantRows:        map[string]rowWant{"m-tp": {status: db.StatusPending}},
			wantTraceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
		{
			name:      "DiscordMessageAccepted",
			key:       tcKey,
			req:       ThreadEnqueueRequest{Message: &db.Message{ID: "m2", ThreadID: tcKey, Content: "hi"}},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: "m2"},
			wantSends: []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m2"}}, 0)},
			wantRows:  map[string]rowWant{"m2": {status: db.StatusPending}},
		},
		{
			name:            "DiscordMessageInheritsEnvelopeTraceparent",
			key:             tcKey,
			req:             ThreadEnqueueRequest{Message: &db.Message{ID: "m-env-tp", ThreadID: tcKey, Content: "hi"}, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02"},
			wantResp:        ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: "m-env-tp"},
			wantSends:       []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m-env-tp"}}, 0)},
			wantRows:        map[string]rowWant{"m-env-tp": {status: db.StatusPending}},
			wantTraceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02",
		},
		{
			name:            "DiscordMessagePreservesExistingTraceparent",
			key:             tcKey,
			req:             ThreadEnqueueRequest{Message: &db.Message{ID: "m-keep-tp", ThreadID: tcKey, Content: "hi", Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03"}, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-04"},
			wantResp:        ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: "m-keep-tp"},
			wantSends:       []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m-keep-tp"}}, 0)},
			wantRows:        map[string]rowWant{"m-keep-tp": {status: db.StatusPending}},
			wantTraceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03",
		},
		{
			name:      "MessageWithoutIDsDerivesIDOnKey",
			key:       tcKey,
			req:       ThreadEnqueueRequest{Message: &db.Message{Content: "bare"}},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: tcDerivedMsgID},
			wantSends: []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{tcDerivedMsgID}}, 0)},
			wantRows:  map[string]rowWant{tcDerivedMsgID: {status: db.StatusPending}},
		},
		{
			name:      "RedirectAppendsToFinalThread",
			key:       tcKey,
			resolver:  fakeThreadResolver{final: "th-final"},
			req:       ThreadEnqueueRequest{Prompt: "build done", MessageID: "m3"},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: "th-final", MessageID: "m3"},
			wantSends: []sentCall{threadSend("th-final", HandlerAppend, AppendRequest{IDs: []string{"m3"}}, 0)},
			wantRows:  map[string]rowWant{"m3": {status: db.StatusPending}},
		},
		{
			name:      "SkipIfActiveBusySameKey",
			key:       tcKey,
			state:     st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"x"}}),
			req:       ThreadEnqueueRequest{Prompt: "ping", MessageID: "m4", SkipIfActive: true},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusSkippedActive, ThreadID: tcKey},
			wantNoRow: "m4",
		},
		{
			name:      "SkipIfActiveIdleAccepted",
			key:       tcKey,
			req:       ThreadEnqueueRequest{Prompt: "ping", MessageID: "m5", SkipIfActive: true},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: tcKey, MessageID: "m5"},
			wantSends: []sentCall{threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m5"}, SkipIfActive: true}, 0)},
			wantRows:  map[string]rowWant{"m5": {status: db.StatusPending}},
		},
		{
			name:      "SkipIfActiveRedirectedDefersToAppend",
			key:       tcKey,
			state:     st([]string{"x"}, nil),
			resolver:  fakeThreadResolver{redirect: "th-sys"},
			req:       ThreadEnqueueRequest{Prompt: "deploy done", MessageID: "m6", SkipIfActive: true},
			wantResp:  ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: "th-sys", MessageID: "m6"},
			wantSends: []sentCall{threadSend("th-sys", HandlerAppend, AppendRequest{IDs: []string{"m6"}, SkipIfActive: true}, 0)},
			wantRows:  map[string]rowWant{"m6": {status: db.StatusPending}},
		},
		{
			name: "PromptAlreadyRecordedReturnsExistingThread", key: "ch-1", resolver: fakeThreadResolver{final: "th-new"},
			rows: []db.Message{msgRow("m-dup", "th-old", db.StatusCompleted)}, req: ThreadEnqueueRequest{Prompt: "hello", MessageID: "m-dup"},
			wantResp: ThreadEnqueueResponse{Status: eriwire.StatusAlreadyExists, ThreadID: "th-old", MessageID: "m-dup"},
		},
		{name: "MissingThreadRejected", key: "", req: ThreadEnqueueRequest{Prompt: "orphan"}, wantCode: 400},
		{
			name:       "RecordStepCancelSurfacesAsConflict",
			key:        tcKey,
			req:        ThreadEnqueueRequest{Prompt: "hello", MessageID: "m7"},
			cancelStep: StepRecordQueued,
			wantCode:   CodeQueueConflict,
		},
		{
			name:        "RecordStepStoreFailureIsTerminal",
			key:         tcKey,
			req:         ThreadEnqueueRequest{Prompt: "hello", MessageID: "m8"},
			sabotageSQL: failTriggerSQL("messages", "INSERT", "23505"),
			wantCode:    500,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedStore(t, tc.rows, nil, tc.sabotageSQL)
			h := newCoordHarness(t, tc.key, tcInv, tc.state)
			h.cancelStep = tc.cancelStep
			resp, err := NewThreadCoordinator(store, tc.resolver, "aerial").Enqueue(h.sharedCtx(), tc.req)
			if assertCoordinatorErr(t, err, tc.wantCode) {
				return
			}
			assert.Equal(t, tc.wantResp, resp)
			assert.Equal(t, tc.wantSends, h.sends)
			assertMessageRows(t, store, tc.wantRows)
			if tc.wantNoRow != "" {
				m, getErr := store.GetMessage(context.Background(), tc.wantNoRow)
				require.NoError(t, getErr)
				assert.Nil(t, m)
			}
			if tc.wantResp.Status != eriwire.StatusSkippedActive {
				m, getErr := store.GetMessage(context.Background(), tc.wantResp.MessageID)
				require.NoError(t, getErr)
				require.NotNil(t, m)
				assert.Equal(t, tc.wantTraceparent, m.Traceparent)
			}
		})
	}
}

func TestThreadCoordinator_Enqueue_IngressWakeGating(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		resolver  fakeIngressWakeResolver
		req       ThreadEnqueueRequest
		wantSends []sentCall
		wantRows  map[string]rowWant
	}{
		{
			name:     "GatedIgnoredMessageCompletesWithoutAppend",
			resolver: fakeIngressWakeResolver{shouldEnqueue: false, completedReason: "[IGNORED]"},
			req:      ThreadEnqueueRequest{Message: &db.Message{ID: "m-ign", ThreadID: tcKey, Content: "ambient chat"}},
			wantSends: []sentCall{
				threadSend(tcKey, HandlerAppend, AppendRequest{ReserveID: "m-ign"}, 0),
				threadSend(tcKey, HandlerAppend, AppendRequest{DropID: "m-ign"}, 0),
			},
			wantRows: map[string]rowWant{"m-ign": {status: db.StatusCompleted, errText: "[IGNORED]"}},
		},
		{
			name:     "GatedWakeMessageEnqueuesAndAppends",
			resolver: fakeIngressWakeResolver{shouldEnqueue: true},
			req:      ThreadEnqueueRequest{Message: &db.Message{ID: "m-wake", ThreadID: tcKey, Content: "wake question"}},
			wantSends: []sentCall{
				threadSend(tcKey, HandlerAppend, AppendRequest{ReserveID: "m-wake"}, 0),
				threadSend(tcKey, HandlerAppend, AppendRequest{IDs: []string{"m-wake"}, Gated: true, FromIngress: true}, 0),
			},
			wantRows: map[string]rowWant{"m-wake": {status: db.StatusPending}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedStore(t, nil, nil, "")
			h := newCoordHarness(t, tcKey, tcInv, coordinatorState{})
			resp, err := NewThreadCoordinator(store, tc.resolver, "aerial").Enqueue(h.sharedCtx(), tc.req)
			require.NoError(t, err)
			assert.Equal(t, eriwire.StatusAccepted, resp.Status)
			assert.Equal(t, tc.wantSends, h.sends)
			assertMessageRows(t, store, tc.wantRows)
		})
	}
}

type channelIngressWakeResolver struct {
	fakeThreadResolver

	holdA   chan struct{}
	entered chan string
	decide  func(msg db.Message) (bool, string)
}

func (c *channelIngressWakeResolver) EvaluateIngressWake(_ context.Context, _ string, msg db.Message) (bool, string) {
	if c.entered != nil {
		c.entered <- msg.ID
	}
	if msg.ID == "m-slow" && c.holdA != nil {
		<-c.holdA
	}
	if c.decide != nil {
		return c.decide(msg)
	}
	return true, ""
}

func TestThreadCoordinator_Enqueue_ArrivalOrder_SecondFinishesBeforeFirstInsert(t *testing.T) {
	t.Parallel()

	store := seedStore(t, nil, nil, "")
	h := newCoordHarness(t, tcKey, tcInv, coordinatorState{})
	resolver := &channelIngressWakeResolver{}
	coord := NewThreadCoordinator(store, resolver, "aerial")

	var appendMu sync.Mutex
	bDecisionAppended := make(chan struct{})
	h.onSend = func(call sentCall) {
		req, ok := call.Input.(AppendRequest)
		if call.Method != HandlerAppend || !ok {
			return
		}
		appendMu.Lock()
		require.NoError(t, coord.Append(h.objectCtx(), req))
		appendMu.Unlock()
		if len(req.IDs) == 1 && req.IDs[0] == "m-fast" {
			close(bDecisionAppended)
		}
	}

	var stepMu sync.Mutex
	recordCalls := 0
	aPausedBeforeInsert := make(chan struct{})
	releaseAInsert := make(chan struct{})
	h.onRunStep = func(stepName string) {
		if stepName != StepRecordQueued {
			return
		}
		stepMu.Lock()
		recordCalls++
		callNum := recordCalls
		stepMu.Unlock()
		if callNum == 1 {
			close(aPausedBeforeInsert)
			<-releaseAInsert
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-slow", ThreadID: tcKey, Content: "slow message"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	<-aPausedBeforeInsert

	go func() {
		defer wg.Done()
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-fast", ThreadID: tcKey, Content: "fast message"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	<-bDecisionAppended

	snap := h.snapshot(t)
	assert.Empty(t, snap.pending, "m-fast must not overtake m-slow before m-slow finishes InsertMessage")
	assert.Nil(t, snap.active, "no turn may start while leading reservation m-slow is undecided")
	assert.Equal(t, []ingressSlot{
		{ID: "m-slow"},
		{ID: "m-fast", Decided: true, Accepted: true, Gated: true},
	}, snap.ingress)

	close(releaseAInsert)
	wg.Wait()

	finalSnap := h.snapshot(t)
	assert.Empty(t, finalSnap.ingress)
	assert.Equal(t, startedTurn("m-slow", "m-fast"), finalSnap.active)
	assert.Contains(t, h.sends, runGatedBurst("m-slow", "m-fast"))
}

func TestThreadCoordinator_Enqueue_ArrivalOrder_DecisionAppendReordered(t *testing.T) {
	t.Parallel()

	store := seedStore(t, nil, nil, "")
	h := newCoordHarness(t, tcKey, tcInv, coordinatorState{})
	aGateEntered := make(chan string, 2)
	resolver := &channelIngressWakeResolver{entered: aGateEntered}
	coord := NewThreadCoordinator(store, resolver, "aerial")

	var appendMu sync.Mutex
	aReserved := make(chan struct{})
	aDecisionHeld := make(chan struct{})
	bDecisionApplied := make(chan struct{})
	h.onSend = func(call sentCall) {
		req, ok := call.Input.(AppendRequest)
		if call.Method != HandlerAppend || !ok {
			return
		}
		if req.ReserveID == "m-slow" {
			appendMu.Lock()
			require.NoError(t, coord.Append(h.objectCtx(), req))
			appendMu.Unlock()
			close(aReserved)
			return
		}
		if len(req.IDs) == 1 && req.IDs[0] == "m-slow" {
			close(aDecisionHeld)
			<-bDecisionApplied
		}
		appendMu.Lock()
		require.NoError(t, coord.Append(h.objectCtx(), req))
		appendMu.Unlock()
		if len(req.IDs) == 1 && req.IDs[0] == "m-fast" {
			close(bDecisionApplied)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-slow", ThreadID: tcKey, Content: "first message"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	<-aReserved
	<-aDecisionHeld

	go func() {
		defer wg.Done()
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-fast", ThreadID: tcKey, Content: "second message"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	wg.Wait()

	finalSnap := h.snapshot(t)
	assert.Empty(t, finalSnap.ingress)
	assert.Equal(t, startedTurn("m-slow", "m-fast"), finalSnap.active)
	assert.Contains(t, h.sends, runGatedBurst("m-slow", "m-fast"))
}

func TestThreadCoordinator_Enqueue_ArrivalOrder_IgnoredReservationReleasesWake(t *testing.T) {
	t.Parallel()

	holdA := make(chan struct{})
	store := seedStore(t, nil, nil, "")
	h := newCoordHarness(t, tcKey, tcInv, coordinatorState{})
	resolver := &channelIngressWakeResolver{
		holdA: holdA,
		decide: func(msg db.Message) (bool, string) {
			if msg.ID == "m-slow" {
				return false, "[IGNORED]"
			}
			return true, ""
		},
	}
	coord := NewThreadCoordinator(store, resolver, "aerial")

	var appendMu sync.Mutex
	aReserved := make(chan struct{})
	bDecisionApplied := make(chan struct{})
	h.onSend = func(call sentCall) {
		req, ok := call.Input.(AppendRequest)
		if call.Method != HandlerAppend || !ok {
			return
		}
		appendMu.Lock()
		require.NoError(t, coord.Append(h.objectCtx(), req))
		appendMu.Unlock()
		if req.ReserveID == "m-slow" {
			close(aReserved)
		}
		if len(req.IDs) == 1 && req.IDs[0] == "m-fast" {
			close(bDecisionApplied)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-slow", ThreadID: tcKey, Content: "ambient chatter"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	<-aReserved

	go func() {
		defer wg.Done()
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-fast", ThreadID: tcKey, Content: "hey @Aerial"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	<-bDecisionApplied
	assert.Nil(t, h.snapshot(t).active, "m-fast must wait while leading m-slow reservation is undecided")

	close(holdA)
	wg.Wait()

	finalSnap := h.snapshot(t)
	assert.Empty(t, finalSnap.ingress)
	assert.Equal(t, startedTurn("m-fast"), finalSnap.active)
	assert.Contains(t, h.sends, runGatedBurst("m-fast"))
}

func TestThreadCoordinator_Enqueue_DoesNotDelayCancelOrDrain(t *testing.T) {
	t.Parallel()

	holdA := make(chan struct{})
	defer close(holdA)

	store := seedStore(t, []db.Message{msgRow("m-active", tcKey, db.StatusProcessing)}, nil, "")
	activeState := st([]string{"m-pending"}, &ActiveTurn{TurnID: "turn-slow", ItemIDs: []string{"m-active"}, RunnerInvocationID: "inv-slow"})
	h := newCoordHarness(t, tcKey, tcInv, activeState)

	resolver := &channelIngressWakeResolver{holdA: holdA}
	coord := NewThreadCoordinator(store, resolver, "aerial")

	go func() {
		resp, err := coord.Enqueue(h.sharedCtx(), ThreadEnqueueRequest{
			Message: &db.Message{ID: "m-slow", ThreadID: tcKey, Content: "new slow message"},
		})
		assert.NoError(t, err)
		assert.Equal(t, eriwire.StatusAccepted, resp.Status)
	}()

	// Wait until m-slow is inserted and blocked in evaluator
	require.Eventually(t, func() bool {
		m, err := store.GetMessage(context.Background(), "m-slow")
		return err == nil && m != nil
	}, 2*time.Second, 10*time.Millisecond)

	// CancelTurn must complete immediately without waiting for the slow evaluator
	cancelDone := make(chan struct{})
	go func() {
		cancelResp, err := coord.CancelTurn(h.sharedCtx(), CancelTurnRequest{Reason: "operator cancel"})
		assert.NoError(t, err)
		assert.Equal(t, "ok", cancelResp.Status)
		assert.True(t, cancelResp.CancelledActiveTurn)
		close(cancelDone)
	}()

	select {
	case <-cancelDone:
	case <-time.After(1 * time.Second):
		t.Fatal("CancelTurn was delayed by slow ingress wake evaluator")
	}

	// DrainPending must complete immediately without waiting for the slow evaluator
	drainDone := make(chan struct{})
	go func() {
		drainResp, err := coord.DrainPending(h.objectCtx(), DrainPendingRequest{TurnID: "turn-slow"})
		assert.NoError(t, err)
		assert.Equal(t, []string{"m-pending"}, drainResp.DrainedIDs)
		close(drainDone)
	}()

	select {
	case <-drainDone:
	case <-time.After(1 * time.Second):
		t.Fatal("DrainPending was delayed by slow ingress wake evaluator")
	}
}

func TestThreadCoordinator_Append(t *testing.T) {
	t.Parallel()
	waiting := &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}, RunnerInvocationID: "inv-0", Waiting: true}
	tests := []struct {
		name        string
		state       coordinatorState
		rows        []db.Message
		req         AppendRequest
		wantState   coordinatorState
		wantSends   []sentCall
		wantRows    map[string]rowWant
		wantCleared bool
		cancelStep  string
		sabotageSQL string
		wantCode    int
	}{
		{
			name:       "SkipStepCancelSurfacesAsConflict",
			state:      st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			rows:       []db.Message{msgRow("s3", tcKey, db.StatusPending)},
			req:        AppendRequest{IDs: []string{"s3"}, SkipIfActive: true},
			cancelStep: StepRecordSkipped,
			wantCode:   CodeQueueConflict,
		},
		{
			name:        "SkipStepStoreFailureIsTerminal",
			state:       st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			rows:        []db.Message{msgRow("s4", tcKey, db.StatusPending)},
			req:         AppendRequest{IDs: []string{"s4"}, SkipIfActive: true},
			sabotageSQL: failTriggerSQL("messages", "UPDATE", "23505"),
			wantCode:    500,
		},
		{
			name:      "IdleStartsBurst",
			req:       AppendRequest{IDs: []string{"a", "b"}},
			wantState: st(nil, startedTurn("a", "b")),
			wantSends: []sentCall{runBurst("a", "b")},
		},
		{
			name:      "BurstCapFive",
			req:       AppendRequest{IDs: []string{"a", "b", "c", "d", "e", "f", "g"}},
			wantState: st([]string{"f", "g"}, startedTurn("a", "b", "c", "d", "e")),
			wantSends: []sentCall{runBurst("a", "b", "c", "d", "e")},
		},
		{
			name:      "ActiveOnlyAppends",
			state:     st([]string{"b"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			req:       AppendRequest{IDs: []string{"c"}},
			wantState: st([]string{"b", "c"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
		},
		{
			name:      "WaitingOnlyAppends",
			state:     st([]string{"a"}, waiting),
			req:       AppendRequest{IDs: []string{"c"}},
			wantState: st([]string{"a", "c"}, waiting),
		},
		{
			name:      "DedupsPendingAndActive",
			state:     st([]string{"b"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			req:       AppendRequest{IDs: []string{"a", "b", "b"}},
			wantState: st([]string{"b"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
		},
		{
			name:      "SkipIfActiveBusyRecordsSkipped",
			state:     st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			rows:      []db.Message{msgRow("s1", tcKey, db.StatusPending)},
			req:       AppendRequest{IDs: []string{"s1"}, SkipIfActive: true},
			wantState: st(nil, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			wantRows:  map[string]rowWant{"s1": {status: db.StatusCompleted, errText: skippedActiveTurnReason}},
		},
		{
			name:      "SkipIfActiveNeverSkipsHeldID",
			state:     st([]string{"s1"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			rows:      []db.Message{msgRow("s1", tcKey, db.StatusPending)},
			req:       AppendRequest{IDs: []string{"s1"}, SkipIfActive: true},
			wantState: st([]string{"s1"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}),
			wantRows:  map[string]rowWant{"s1": {status: db.StatusPending}},
		},
		{
			name:      "SkipIfActiveIdleStarts",
			rows:      []db.Message{msgRow("s2", tcKey, db.StatusPending)},
			req:       AppendRequest{IDs: []string{"s2"}, SkipIfActive: true},
			wantState: st(nil, startedTurn("s2")),
			wantSends: []sentCall{runBurst("s2")},
			wantRows:  map[string]rowWant{"s2": {status: db.StatusPending}},
		},
		{
			name:        "NothingNewWhenIdleClearsAll",
			req:         AppendRequest{IDs: []string{" "}},
			wantCleared: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store coordinatorStore
			var sqlStore *db.SQLStore
			if len(tc.rows) > 0 || len(tc.wantRows) > 0 {
				sqlStore = seedStore(t, tc.rows, nil, tc.sabotageSQL)
				store = sqlStore
			}
			h := newCoordHarness(t, tcKey, tcInv, tc.state)
			h.cancelStep = tc.cancelStep
			if assertCoordinatorErr(t, NewThreadCoordinator(store, nil, "aerial").Append(h.objectCtx(), tc.req), tc.wantCode) {
				return
			}
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
			assert.Equal(t, tc.wantCleared, h.clearAlls > 0)
			assertMessageRows(t, sqlStore, tc.wantRows)
		})
	}
}

func TestThreadCoordinator_OnTurnDone(t *testing.T) {
	t.Parallel()
	running := &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a", "b"}, RunnerInvocationID: "inv-0"}
	tests := []struct {
		name        string
		state       coordinatorState
		req         TurnDoneRequest
		wantState   coordinatorState
		wantSends   []sentCall
		wantCleared bool
	}{
		{
			name:      "StaleTurnFenced",
			state:     st([]string{"c"}, running),
			req:       TurnDoneRequest{TurnID: "t-old", Outcome: OutcomeCompleted},
			wantState: st([]string{"c"}, running),
		},
		{
			name:      "NoActiveFenced",
			state:     st([]string{"c"}, nil),
			req:       TurnDoneRequest{TurnID: "t0", Outcome: OutcomeCompleted},
			wantState: st([]string{"c"}, nil),
		},
		{
			name:      "CompletedStartsNext",
			state:     st([]string{"c"}, running),
			req:       TurnDoneRequest{TurnID: "t0", Outcome: OutcomeCompleted},
			wantState: st(nil, startedTurn("c")),
			wantSends: []sentCall{runBurst("c")},
		},
		{
			name:      "TimeoutClearsActiveAndStartsNext",
			state:     st([]string{"c", "d"}, running),
			req:       TurnDoneRequest{TurnID: "t0", Outcome: OutcomeTimeout},
			wantState: st(nil, startedTurn("c", "d")),
			wantSends: []sentCall{runBurst("c", "d")},
		},
		{
			name:        "FailedWithNothingPendingClearsAll",
			state:       st(nil, running),
			req:         TurnDoneRequest{TurnID: "t0", Outcome: OutcomeFailed},
			wantCleared: true,
		},
		{
			name:  "RescheduleWaitsAndKicksLater",
			state: st([]string{"c"}, running),
			req:   TurnDoneRequest{TurnID: "t0", Outcome: OutcomeReschedule, RescheduleDelayMs: 1500},
			wantState: st([]string{"a", "b", "c"},
				&ActiveTurn{TurnID: "t0", ItemIDs: []string{"a", "b"}, RunnerInvocationID: "inv-0", Waiting: true}),
			wantSends: []sentCall{threadSend(tcKey, HandlerKick, KickRequest{TurnID: "t0"}, 1500*time.Millisecond)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, tcKey, tcInv, tc.state)
			require.NoError(t, NewThreadCoordinator(nil, nil, "aerial").OnTurnDone(h.objectCtx(), tc.req))
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
			assert.Equal(t, tc.wantCleared, h.clearAlls > 0)
		})
	}
}

func TestThreadCoordinator_Kick(t *testing.T) {
	t.Parallel()
	waiting := &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}, RunnerInvocationID: "inv-0", Waiting: true}
	running := &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}, RunnerInvocationID: "inv-0"}
	tests := []struct {
		name      string
		state     coordinatorState
		req       KickRequest
		wantState coordinatorState
		wantSends []sentCall
	}{
		{name: "MismatchNoop", state: st([]string{"a"}, waiting), req: KickRequest{TurnID: "t-other"}, wantState: st([]string{"a"}, waiting)},
		{name: "NotWaitingNoop", state: st([]string{"b"}, running), req: KickRequest{TurnID: "t0"}, wantState: st([]string{"b"}, running)},
		{name: "WaitingRestarts", state: st([]string{"a", "b"}, waiting), req: KickRequest{TurnID: "t0"}, wantState: st(nil, startedTurn("a", "b")), wantSends: []sentCall{runBurst("a", "b")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, tcKey, tcInv, tc.state)
			require.NoError(t, NewThreadCoordinator(nil, nil, "aerial").Kick(h.objectCtx(), tc.req))
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
		})
	}
}

func TestThreadCoordinator_CancelTurn(t *testing.T) {
	t.Parallel()
	t1 := &ActiveTurn{TurnID: "t1", ItemIDs: []string{"a"}, RunnerInvocationID: "inv-r1"}
	tests := []struct {
		name        string
		state       coordinatorState
		rows        []db.Message
		req         CancelTurnRequest
		cancelStep  string
		sabotageSQL string
		wantResp    CancelTurnResponse
		wantSends   []sentCall
		wantRows    map[string]rowWant
		wantCode    int
	}{
		{
			name:      "UnstartedRunnerFailsPendingActiveIDs",
			state:     st([]string{"b"}, t1),
			rows:      []db.Message{msgRow("a", tcKey, db.StatusPending), msgRow("b", tcKey, db.StatusPending)},
			wantResp:  CancelTurnResponse{Status: "ok", CancelledCount: 2, CancelledActiveTurn: true},
			wantSends: []sentCall{threadSend(tcKey, HandlerClearPending, ClearPendingRequest{IDs: []string{"b"}, CancelActive: true, TurnID: "t1"}, 0)},
			wantRows:  map[string]rowWant{"a": {status: db.StatusFailed, errText: cancelledByOperatorReason}, "b": {status: db.StatusFailed, errText: cancelledByOperatorReason}},
		},
		{
			name:      "SnapshotRaceLeavesClaimedRows",
			state:     st([]string{"b"}, t1),
			rows:      []db.Message{msgRow("a", tcKey, db.StatusCompleted), msgRow("b", tcKey, db.StatusProcessing)},
			req:       CancelTurnRequest{Reason: "stop"},
			wantResp:  CancelTurnResponse{Status: "ok", CancelledCount: 0, CancelledActiveTurn: true},
			wantSends: []sentCall{threadSend(tcKey, HandlerClearPending, ClearPendingRequest{IDs: []string{"b"}, CancelActive: true, TurnID: "t1"}, 0)},
			wantRows:  map[string]rowWant{"a": {status: db.StatusCompleted}, "b": {status: db.StatusProcessing}},
		},
		{
			name:     "IdleNoop",
			wantResp: CancelTurnResponse{Status: "ok"},
		},
		{
			name:       "StepCancelSurfacesAsConflict",
			state:      st([]string{"b"}, nil),
			rows:       []db.Message{msgRow("b", tcKey, db.StatusPending)},
			cancelStep: StepRecordCancelled,
			wantCode:   CodeQueueConflict,
		},
		{
			name:        "StepStoreFailureIsTerminal",
			state:       st([]string{"b"}, nil),
			rows:        []db.Message{msgRow("b", tcKey, db.StatusPending)},
			sabotageSQL: failTriggerSQL("messages", "UPDATE", "23505"),
			wantCode:    500,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store coordinatorStore
			var sqlStore *db.SQLStore
			if len(tc.rows) > 0 || len(tc.wantRows) > 0 {
				sqlStore = seedStore(t, tc.rows, nil, tc.sabotageSQL)
				store = sqlStore
			}
			h := newCoordHarness(t, tcKey, tcInv, tc.state)
			h.cancelStep = tc.cancelStep
			resp, err := NewThreadCoordinator(store, nil, "aerial").CancelTurn(h.sharedCtx(), tc.req)
			if assertCoordinatorErr(t, err, tc.wantCode) {
				return
			}
			assert.Equal(t, tc.wantResp, resp)
			assert.Equal(t, tc.wantSends, h.sends)
			assert.Empty(t, h.cancels, "the shared handler never cancels invocations")
			assertMessageRows(t, sqlStore, tc.wantRows)
		})
	}
}

func TestThreadCoordinator_ClearPending(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		state       coordinatorState
		req         ClearPendingRequest
		wantState   coordinatorState
		wantSends   []sentCall
		wantCancels []string
		wantCleared bool
	}{
		{
			name:        "CancelsRunnerThatNeverReported",
			state:       st(nil, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"a"}, RunnerInvocationID: "inv-r1"}),
			req:         ClearPendingRequest{CancelActive: true, TurnID: "t1"},
			wantCancels: []string{"inv-r1"},
			wantCleared: true,
		},
		{
			name:        "SnapshotRaceCancelsT2FromBacklog",
			state:       st([]string{"c"}, &ActiveTurn{TurnID: "t2", ItemIDs: []string{"b"}, RunnerInvocationID: "inv-r2"}),
			req:         ClearPendingRequest{IDs: []string{"b"}, CancelActive: true, TurnID: "t1"},
			wantState:   st(nil, startedTurn("c")),
			wantSends:   []sentCall{runBurst("c")},
			wantCancels: []string{"inv-r2"},
		},
		{
			name:      "T2WithPostSnapshotIDKeepsRunning",
			state:     st([]string{"b"}, &ActiveTurn{TurnID: "t2", ItemIDs: []string{"b", "new"}, RunnerInvocationID: "inv-r2"}),
			req:       ClearPendingRequest{IDs: []string{"b"}, CancelActive: true, TurnID: "t1"},
			wantState: st(nil, &ActiveTurn{TurnID: "t2", ItemIDs: []string{"b", "new"}, RunnerInvocationID: "inv-r2"}),
		},
		{
			name:        "WaitingTurnClearedWithoutCancel",
			state:       st([]string{"a", "z"}, &ActiveTurn{TurnID: "t1", ItemIDs: []string{"a"}, RunnerInvocationID: "inv-r1", Waiting: true}),
			req:         ClearPendingRequest{IDs: []string{"a"}, CancelActive: true, TurnID: "t1"},
			wantState:   st(nil, startedTurn("z")),
			wantSends:   []sentCall{runBurst("z")},
			wantCancels: nil,
		},
		{
			name:      "EmptyBodyStartsIdleBacklog",
			state:     st([]string{"a"}, nil),
			req:       ClearPendingRequest{},
			wantState: st(nil, startedTurn("a")),
			wantSends: []sentCall{runBurst("a")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, tcKey, tcInv, tc.state)
			require.NoError(t, NewThreadCoordinator(nil, nil, "aerial").ClearPending(h.objectCtx(), tc.req))
			assert.Equal(t, tc.wantState, h.snapshot(t))
			assert.Equal(t, tc.wantSends, h.sends)
			assert.Equal(t, tc.wantCancels, h.cancels)
			assert.Equal(t, tc.wantCleared, h.clearAlls > 0)
		})
	}
}

func TestThreadCoordinator_Status(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		state coordinatorState
		want  CoordinatorStatus
	}{
		{name: "Idle", want: CoordinatorStatus{}},
		{name: "Busy", state: st([]string{"b"}, &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}), want: CoordinatorStatus{Pending: []string{"b"}, Active: &ActiveTurn{TurnID: "t0", ItemIDs: []string{"a"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCoordHarness(t, tcKey, tcInv, tc.state)
			got, err := NewThreadCoordinator(nil, nil, "aerial").Status(h.sharedCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
