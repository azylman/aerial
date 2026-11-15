package pubsub

import (
	"context"
	"errors"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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

// traceparentHeader returns NATS headers carrying the given W3C traceparent.
func traceparentHeader(traceparent string) nats.Header {
	h := nats.Header{}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(http.Header(h)))
	return h
}

// recordingNatsDialer is a fake NATS server that records every byte the
// client writes, so tests can observe ack frames published on reply subjects.
type recordingNatsDialer struct {
	mu   sync.Mutex
	wire strings.Builder
}

func (d *recordingNatsDialer) Dial(string, string) (net.Conn, error) {
	c1, c2 := net.Pipe()
	go func() {
		defer c2.Close()
		_, _ = c2.Write([]byte("INFO {\"server_id\":\"TEST\",\"version\":\"2.10.0\",\"proto\":1,\"jetstream\":true,\"max_payload\":1048576}\r\n"))
		buf := make([]byte, 1024)
		for {
			n, err := c2.Read(buf)
			if err != nil {
				return
			}
			d.mu.Lock()
			d.wire.Write(buf[:n])
			d.mu.Unlock()
			if strings.Contains(string(buf[:n]), "PING") {
				_, _ = c2.Write([]byte("PONG\r\n"))
			}
		}
	}()
	return c1, nil
}

// ackTokens returns the leading token (+ACK, -NAK, +WPI, +TERM, ...) of every
// payload the client published to reply, in order. Call it after nc.Flush so
// the fake server has read every prior frame.
func (d *recordingNatsDialer) ackTokens(reply string) []string {
	d.mu.Lock()
	wire := d.wire.String()
	d.mu.Unlock()
	var tokens []string
	prefix := "PUB " + reply + " "
	for {
		i := strings.Index(wire, prefix)
		if i < 0 {
			return tokens
		}
		size, rest, _ := strings.Cut(wire[i+len(prefix):], "\r\n")
		n, _ := strconv.Atoi(size)
		tokens = append(tokens, strings.Fields(rest[:n])[0])
		wire = rest[n:]
	}
}

// boundTestSub connects a NATS client to a recording fake server and returns
// a subscription the dispatched message can be bound to for acking.
func boundTestSub(t *testing.T) (*nats.Conn, *nats.Subscription, *recordingNatsDialer) {
	t.Helper()
	dialer := &recordingNatsDialer{}
	nc, err := nats.Connect("nats://127.0.0.1:4222", nats.SetCustomDialer(dialer), nats.Timeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatalf("nats.Connect failed: %v", err)
	}
	t.Cleanup(nc.Close)
	sub, err := nc.SubscribeSync("aerial.events.>")
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}
	return nc, sub, dialer
}

func TestJetStreamBus_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		subject       string
		reply         string
		data          string
		header        nats.Header
		handlerErr    error
		termInHandler bool
		want          dispatchObservation
		wantAcks      []string
	}{
		{
			// $JS.ACK.<stream>.<consumer>.<numDelivered>.<streamSeq>.<consumerSeq>.<timestamp>.<numPending>
			name:    "ack reply metadata exposes delivery count",
			subject: "aerial.events.meta", reply: "$JS.ACK.AERIAL_EVENTS.durable.4.1.1.1600000000.0", data: "with-meta",
			want:     dispatchObservation{invoked: true, subject: "aerial.events.meta", data: "with-meta", numDelivered: 4},
			wantAcks: []string{"+ACK"},
		},
		{
			name:    "traceparent header is extracted into the handler context",
			subject: "aerial.events.head", reply: "_INBOX.head", data: "h",
			header:   traceparentHeader("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
			want:     dispatchObservation{invoked: true, subject: "aerial.events.head", data: "h", numDelivered: 1, traceID: "4bf92f3577b34da6a3ce929d0e0e4736"},
			wantAcks: []string{"+ACK"},
		},
		{
			name:    "handler error is auto-naked",
			subject: "aerial.events.err", reply: "_INBOX.err", data: "err",
			handlerErr: errors.New("handler failed"),
			want:       dispatchObservation{invoked: true, subject: "aerial.events.err", data: "err", numDelivered: 1},
			wantAcks:   []string{"-NAK"},
		},
		{
			name:    "handler settling via InProgress and Term skips auto-settlement",
			subject: "aerial.events.term", reply: "_INBOX.term", data: "term",
			termInHandler: true,
			want:          dispatchObservation{invoked: true, subject: "aerial.events.term", data: "term", numDelivered: 1},
			wantAcks:      []string{"+WPI", "+TERM"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nc, sub, dialer := boundTestSub(t)
			bus := newJetStreamBusWithContext(nil, &mockJetStream{existingStreams: make(map[string]*nats.StreamInfo)})
			// Keep the background heartbeat out of the recorded ack stream.
			bus.heartbeatInterval = time.Hour

			var got dispatchObservation
			msg := &nats.Msg{Subject: tc.subject, Reply: tc.reply, Sub: sub, Data: []byte(tc.data), Header: tc.header}
			bus.dispatch(t.Context(), msg, func(ctx context.Context, msg Message) error {
				got = dispatchObservation{invoked: true, subject: msg.Subject(), data: string(msg.Data()), numDelivered: msg.NumDelivered()}
				if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
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
			if err := nc.Flush(); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}
			if acks := dialer.ackTokens(tc.reply); !slices.Equal(acks, tc.wantAcks) {
				t.Errorf("ack frames = %v, want %v", acks, tc.wantAcks)
			}
		})
	}
}

func TestJSMessage_SettledSettlementIsNoop(t *testing.T) {
	t.Parallel()

	msg := &jsMessage{
		raw:     &nats.Msg{Subject: "aerial.jobs.jsmsg", Data: []byte("raw"), Header: nats.Header{}},
		bus:     newJetStreamBusWithContext(nil, &mockJetStream{}),
		settled: true,
	}
	tests := []struct {
		name string
		err  error
	}{
		{name: "Ack", err: msg.Ack()},
		{name: "NakWithDelay", err: msg.NakWithDelay(0)},
		{name: "Term", err: msg.Term()},
		{name: "Snooze", err: msg.Snooze(0)},
		{name: "InProgress", err: msg.InProgress()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.err != nil {
				t.Errorf("%s on settled jsMessage = %v, want nil", tc.name, tc.err)
			}
		})
	}
}

// plainBus hides JetStreamBus's HeaderPublisher implementation so Snooze takes
// its plain Publish fallback.
type plainBus struct {
	Bus
}

func TestJSMessage_Snooze(t *testing.T) {
	t.Parallel()

	errContinuation := errors.New("continuation pub failed")
	tests := []struct {
		name       string
		header     nats.Header
		publishErr error
		plainBus   bool
		wantErr    error
	}{
		{
			name:    "unbound message without msg id publishes then fails to ack",
			wantErr: nats.ErrMsgNotBound,
		},
		{
			name:       "continuation publish failure is returned",
			publishErr: errContinuation,
			wantErr:    errContinuation,
		},
		{
			name:     "existing snooze sequence on a non-header publisher bus",
			header:   nats.Header{HeaderMsgID: []string{"task-1:snooze:2"}},
			plainBus: true,
			wantErr:  nats.ErrMsgNotBound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var bus Bus = newJetStreamBusWithContext(nil, &mockJetStream{publishErr: tc.publishErr})
			if tc.plainBus {
				bus = plainBus{bus}
			}
			msg := &jsMessage{raw: &nats.Msg{Subject: "aerial.jobs.snooze", Data: []byte("s"), Header: tc.header}, bus: bus}
			if err := msg.Snooze(0); !errors.Is(err, tc.wantErr) {
				t.Errorf("Snooze(0) = %v, want %v", err, tc.wantErr)
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
		{name: "cached metadata", msg: &jsMessage{raw: &nats.Msg{}, meta: &nats.MsgMetadata{NumDelivered: 42}}, want: 42},
		{name: "no metadata defaults to first delivery", msg: &jsMessage{raw: &nats.Msg{}}, want: 1},
		{
			name: "parsed from raw ack reply",
			msg:  &jsMessage{raw: &nats.Msg{Reply: "$JS.ACK.AERIAL_JOBS.test-cons.3.100.200.1600000000.5", Sub: &nats.Subscription{}}},
			want: 3,
		},
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
		raw       *nats.Msg
		wantValue string
	}{
		{name: "existing header is returned as-is", raw: &nats.Msg{Header: nats.Header{"X-Test": []string{"1"}}}, wantValue: "1"},
		{name: "missing header is initialized on the raw message", raw: &nats.Msg{}, wantValue: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &jsMessage{raw: tc.raw}
			h := msg.Header()
			if h == nil || tc.raw.Header == nil {
				t.Fatalf("Header() = %v, raw.Header = %v, want both non-nil", h, tc.raw.Header)
			}
			if got := h.Get("X-Test"); got != tc.wantValue {
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
