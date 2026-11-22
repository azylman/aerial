package pubsub

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// dispatchObservation records what a dispatched handler saw.
type dispatchObservation struct {
	invoked      bool
	subject      string
	data         string
	numDelivered uint64
	traceID      string
}

// traceparentHeader returns NATS headers carrying the given W3C traceparent and msg ID.
func traceparentHeader(traceparent, msgID string) nats.Header {
	h := nats.Header{HeaderMsgID: []string{msgID}}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(http.Header(h)))
	return h
}

// fakeMsg is a jetstream.Msg double that records the settlement tokens
// (+ACK, -NAK, +WPI, +TERM) the bus sends, in order, and fails every
// settlement call with settleErr.
type fakeMsg struct {
	subject   string
	data      []byte
	header    nats.Header
	meta      *jetstream.MsgMetadata
	settleErr error

	mu     sync.Mutex
	tokens []string
}

func (f *fakeMsg) record(token string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = append(f.tokens, token)
	return f.settleErr
}

func (f *fakeMsg) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.tokens)
}

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if f.meta == nil {
		return nil, jetstream.ErrNotJSMessage
	}
	return f.meta, nil
}
func (f *fakeMsg) Data() []byte                     { return f.data }
func (f *fakeMsg) Headers() nats.Header             { return f.header }
func (f *fakeMsg) Subject() string                  { return f.subject }
func (*fakeMsg) Reply() string                      { return "" }
func (f *fakeMsg) Ack() error                       { return f.record("+ACK") }
func (f *fakeMsg) DoubleAck(context.Context) error  { return f.record("+ACK") }
func (f *fakeMsg) Nak() error                       { return f.record("-NAK") }
func (f *fakeMsg) NakWithDelay(time.Duration) error { return f.record("-NAK") }
func (f *fakeMsg) InProgress() error                { return f.record("+WPI") }
func (f *fakeMsg) Term() error                      { return f.record("+TERM") }
func (f *fakeMsg) TermWithReason(string) error      { return f.record("+TERM") }

func TestJetStreamBus_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		subject       string
		meta          *jetstream.MsgMetadata
		data          string
		group         string
		header        nats.Header
		handlerErr    error
		termInHandler bool
		want          dispatchObservation
		wantAcks      []string
		wantSpan      spanShape
	}{
		{
			name:    "message metadata exposes delivery count",
			subject: "aerial.events.meta", meta: &jetstream.MsgMetadata{NumDelivered: 4}, data: "with-meta", group: "durable",
			want:     dispatchObservation{invoked: true, subject: "aerial.events.meta", data: "with-meta", numDelivered: 4},
			wantAcks: []string{"+ACK"},
			wantSpan: procSpan("proc", "", "aerial.events.meta", "durable", "", 4),
		},
		{
			name:    "traceparent header parents the process span seen by the handler",
			subject: "aerial.events.head", data: "h", group: "workers",
			header:   traceparentHeader(testTraceparent, "evt-head"),
			want:     dispatchObservation{invoked: true, subject: "aerial.events.head", data: "h", numDelivered: 1, traceID: testTraceID},
			wantAcks: []string{"+ACK"},
			wantSpan: procSpan("proc", parentRemote, "aerial.events.head", "workers", "evt-head", 1),
		},
		{
			name:    "handler error is auto-naked and fails the process span",
			subject: "aerial.events.err", data: "err",
			handlerErr: errors.New("handler failed"),
			want:       dispatchObservation{invoked: true, subject: "aerial.events.err", data: "err", numDelivered: 1},
			wantAcks:   []string{"-NAK"},
			wantSpan:   procSpan("proc", "", "aerial.events.err", "", "", 1).failed(),
		},
		{
			name:    "handler settling via InProgress and Term skips auto-settlement",
			subject: "aerial.events.term", data: "term",
			termInHandler: true,
			want:          dispatchObservation{invoked: true, subject: "aerial.events.term", data: "term", numDelivered: 1},
			wantAcks:      []string{"+WPI", "+TERM"},
			wantSpan:      procSpan("proc", "", "aerial.events.term", "", "", 1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := newJetStreamBusWithContext(nil, &mockJetStream{existingStreams: make(map[string]*nats.StreamInfo)})
			// Keep the background heartbeat out of the recorded ack stream.
			bus.heartbeatInterval = time.Hour

			var got dispatchObservation
			var handlerSpan trace.SpanID
			msg := &fakeMsg{subject: tc.subject, meta: tc.meta, data: []byte(tc.data), header: tc.header}
			bus.dispatch(t.Context(), msg, tc.group, func(ctx context.Context, msg Message) error {
				got = dispatchObservation{invoked: true, subject: msg.Subject(), data: string(msg.Data()), numDelivered: msg.NumDelivered()}
				spanCtx := trace.SpanContextFromContext(ctx)
				handlerSpan = spanCtx.SpanID()
				if tc.header != nil {
					got.traceID = spanCtx.TraceID().String()
				}
				if tc.termInHandler {
					_ = msg.InProgress()
					return msg.Term()
				}
				return tc.handlerErr
			})
			if got != tc.want {
				t.Errorf("handler observed %+v, want %+v", got, tc.want)
			}
			if acks := msg.recorded(); !slices.Equal(acks, tc.wantAcks) {
				t.Errorf("ack frames = %v, want %v", acks, tc.wantAcks)
			}
			spans := assertSpanShapes(t, testSpans.await(tc.subject, 1), []spanShape{tc.wantSpan})
			if proc := spans["proc"]; proc != nil && proc.SpanContext().SpanID() != handlerSpan {
				t.Errorf("handler span = %s, want process span %s", handlerSpan, proc.SpanContext().SpanID())
			}
		})
	}
}

func TestJetStreamBus_PublishWithHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		subject     string
		msgID       string
		traceparent string
		data        any
		publishErr  error
		wantErr     bool
		wantHeader  string
		wantData    string
		wantSpan    spanShape
	}{
		{
			name:    "traced publish injects the producer span and keeps custom headers",
			subject: "aerial.jobs.pubspan.ok", msgID: "job-7", traceparent: testTraceparent, data: "payload",
			wantHeader: "Custom-Val", wantData: "payload",
			wantSpan: pubSpan("pub", parentRemote, "aerial.jobs.pubspan.ok", "job-7"),
		},
		{
			name:    "structured payload is published as JSON",
			subject: "aerial.jobs.pubspan.json", msgID: "job-9", data: map[string]string{"foo": "bar"},
			wantHeader: "Custom-Val", wantData: `{"foo":"bar"}`,
			wantSpan: pubSpan("pub", "", "aerial.jobs.pubspan.json", "job-9"),
		},
		{
			name:    "server publish failure fails the producer span",
			subject: "aerial.jobs.pubspan.err", data: "payload", publishErr: errors.New("pub failed"), wantErr: true,
			wantSpan: pubSpan("pub", "", "aerial.jobs.pubspan.err", "").failed(),
		},
		{
			name:    "unmarshalable payload fails the producer span",
			subject: "aerial.jobs.pubspan.marshal", msgID: "job-8", data: make(chan int), wantErr: true,
			wantSpan: pubSpan("pub", "", "aerial.jobs.pubspan.marshal", "job-8").failed(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{publishErr: tc.publishErr}
			bus := newJetStreamBusWithContext(nil, mjs)
			ctx := context.Background()
			if tc.traceparent != "" {
				ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": tc.traceparent})
			}
			err := bus.PublishWithHeader(ctx, tc.subject, tc.msgID, tc.data, nats.Header{"Custom-Key": []string{"Custom-Val"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("PublishWithHeader error = %v, wantErr %v", err, tc.wantErr)
			}
			spans := assertSpanShapes(t, testSpans.await(tc.subject, 1), []spanShape{tc.wantSpan})
			if tc.wantErr {
				return
			}
			if mjs.publishedCount() != 1 {
				t.Fatalf("published %d msgs, want 1", mjs.publishedCount())
			}
			h := mjs.publishedMsgs[0].Header
			if got := h.Get("Custom-Key"); got != tc.wantHeader {
				t.Errorf("Custom-Key = %q, want %q", got, tc.wantHeader)
			}
			if got := string(mjs.publishedMsgs[0].Data); got != tc.wantData {
				t.Errorf("data = %q, want %q", got, tc.wantData)
			}
			injected := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(http.Header(h))))
			if pub := spans["pub"]; pub != nil && injected.SpanID() != pub.SpanContext().SpanID() {
				t.Errorf("injected traceparent span = %s, want producer span %s", injected.SpanID(), pub.SpanContext().SpanID())
			}
		})
	}
}

// plainBus hides JetStreamBus's HeaderPublisher implementation so Snooze takes
// its plain Publish fallback.
type plainBus struct {
	Bus
}

func countInProgressTokens(tokens []string) int {
	var count int
	for _, tok := range tokens {
		if tok == "+WPI" {
			count++
		}
	}
	return count
}

// snoozeContinuation returns the msg id (generated ids as "msg-N") and snooze sequence headers
// of the one continuation mjs received, or empty strings when none was published.
func snoozeContinuation(t *testing.T, mjs *mockJetStream) (msgID, seq string) {
	t.Helper()
	mjs.mu.Lock()
	defer mjs.mu.Unlock()
	switch len(mjs.publishedMsgs) {
	case 0:
		return "", ""
	case 1:
		h := mjs.publishedMsgs[0].Header
		return memGeneratedMsgID.ReplaceAllString(h.Get(HeaderMsgID), "msg-N"), h.Get(HeaderSnoozeSeq)
	}
	t.Errorf("published %d continuations, want at most 1", len(mjs.publishedMsgs))
	return "", ""
}

func TestJSMessage_Snooze(t *testing.T) {
	t.Parallel()

	errContinuation := errors.New("continuation pub failed")
	tests := []struct {
		name              string
		delay             time.Duration
		hbInterval        time.Duration
		header            nats.Header
		publishErr        error
		plainBus          bool
		settleErr         error
		countInProgress   bool
		cancelCtx         bool
		wantErr           error
		wantSettled       bool
		wantMinInProgress int
		wantMaxInProgress int
		wantMsgID         string
		wantSeq           string
	}{
		{
			name:      "message without msg id publishes a generated id then fails to ack",
			settleErr: jetstream.ErrMsgNotBound, wantErr: jetstream.ErrMsgNotBound, wantSettled: true, wantMsgID: "msg-N:snooze:1", wantSeq: "1",
		},
		{name: "continuation publish failure is returned", publishErr: errContinuation, wantErr: errContinuation},
		{
			name:   "existing snooze suffix on a non-header publisher bus publishes without headers",
			header: nats.Header{HeaderMsgID: []string{"task-1:snooze:2"}}, plainBus: true,
			settleErr: jetstream.ErrMsgNotBound, wantErr: jetstream.ErrMsgNotBound, wantSettled: true, wantMsgID: "task-1:snooze:1",
		},
		{name: "delayed continuation publish failure does not settle", delay: 5 * time.Millisecond, publishErr: errContinuation, wantErr: errContinuation},
		{
			name:   "delay rotates an existing snooze sequence after waiting",
			header: nats.Header{HeaderMsgID: []string{"msg-1"}, HeaderSnoozeSeq: []string{"4"}}, delay: 10 * time.Millisecond,
			wantSettled: true, wantMsgID: "msg-1:snooze:5", wantSeq: "5",
		},
		{
			name:  "message with delay exceeding heartbeat interval triggers in progress and settles",
			delay: 15 * time.Millisecond, hbInterval: 3 * time.Millisecond, countInProgress: true,
			wantSettled: true, wantMinInProgress: 1, wantMaxInProgress: 100, wantMsgID: "msg-N:snooze:1", wantSeq: "1",
		},
		{
			name:  "message with delay below heartbeat interval settles without in progress",
			delay: 5 * time.Millisecond, hbInterval: 50 * time.Millisecond, countInProgress: true,
			wantSettled: true, wantMsgID: "msg-N:snooze:1", wantSeq: "1",
		},
		{name: "canceled context aborts snooze delay and leaves message unsettled", delay: time.Hour, cancelCtx: true, wantErr: context.Canceled},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{publishErr: tc.publishErr}
			jsBus := newJetStreamBusWithContext(nil, mjs)
			if tc.hbInterval > 0 {
				jsBus.heartbeatInterval = tc.hbInterval
			}
			var bus Bus = jsBus
			if tc.plainBus {
				bus = plainBus{bus}
			}

			raw := &fakeMsg{subject: "aerial.jobs.snooze", data: []byte("s"), header: tc.header, settleErr: tc.settleErr}

			var doneCh <-chan struct{}
			if tc.cancelCtx {
				c, cancel := context.WithCancel(context.Background())
				cancel()
				doneCh = c.Done()
			}
			msg := &jsMessage{raw: raw, bus: bus, done: doneCh}
			err := msg.Snooze(tc.delay)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Snooze(%v) = %v, want %v", tc.delay, err, tc.wantErr)
			}
			msg.mu.Lock()
			settled := msg.settled
			msg.mu.Unlock()
			if settled != tc.wantSettled {
				t.Errorf("settled = %v, want %v", settled, tc.wantSettled)
			}
			if tc.countInProgress {
				gotIP := countInProgressTokens(raw.recorded())
				if gotIP < tc.wantMinInProgress || gotIP > tc.wantMaxInProgress {
					t.Errorf("inProgress tokens = %d, want between %d and %d", gotIP, tc.wantMinInProgress, tc.wantMaxInProgress)
				}
			}
			if gotMsgID, gotSeq := snoozeContinuation(t, mjs); gotMsgID != tc.wantMsgID || gotSeq != tc.wantSeq {
				t.Errorf("continuation msg id, seq = %q, %q, want %q, %q", gotMsgID, gotSeq, tc.wantMsgID, tc.wantSeq)
			}
		})
	}
}

func TestJSMessage_NumDelivered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  *jsMessage
		want uint64
	}{
		{name: "cached metadata", msg: &jsMessage{raw: &fakeMsg{}, meta: &jetstream.MsgMetadata{NumDelivered: 42}}, want: 42},
		{name: "no metadata defaults to first delivery", msg: &jsMessage{raw: &fakeMsg{}}, want: 1},
		{name: "read from raw metadata", msg: &jsMessage{raw: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 3}}}, want: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.msg.NumDelivered(); got != tc.want {
				t.Errorf("NumDelivered() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestJSMessage_Header(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		raw       nats.Header
		wantValue string
	}{
		{name: "existing header is returned as-is", raw: nats.Header{"X-Test": []string{"1"}}, wantValue: "1"},
		{name: "missing header is replaced by a stable empty header", raw: nil, wantValue: "set"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &jsMessage{raw: &fakeMsg{header: tc.raw}}
			if msg.Header().Get("X-Test") == "" {
				msg.Header().Set("X-Test", "set")
			}
			if got := msg.Header().Get("X-Test"); got != tc.wantValue {
				t.Errorf("Header().Get(X-Test) = %q, want %q", got, tc.wantValue)
			}
		})
	}
}

func TestJetStreamBus_Close(t *testing.T) {
	t.Parallel()
	if err := (&JetStreamBus{}).Close(); err != nil {
		t.Errorf("Close with nil nc = %v, want nil", err)
	}
}
