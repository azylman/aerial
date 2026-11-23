package pubsub

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	testTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	testRemoteSpan  = "00f067aa0ba902b7"
	testTraceparent = "00-" + testTraceID + "-" + testRemoteSpan + "-01"
	// parentRemote marks a span whose parent is the remote testTraceparent span.
	parentRemote = "remote"
)

// testSpans records every span ended in this package's tests; tests keep their spans apart by
// publishing to subjects no other test uses.
var testSpans = newSpanSink()

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(testSpans)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

// spanSink is a span processor that lets tests block until spans for a subject have ended.
type spanSink struct {
	mu    sync.Mutex
	cond  *sync.Cond
	ended []sdktrace.ReadOnlySpan
	// expired is set when an await deadline passes so waiters stop blocking.
	expired bool
}

func newSpanSink() *spanSink {
	s := &spanSink{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (*spanSink) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (*spanSink) Shutdown(context.Context) error                  { return nil }
func (*spanSink) ForceFlush(context.Context) error                { return nil }

func (s *spanSink) OnEnd(span sdktrace.ReadOnlySpan) {
	s.mu.Lock()
	s.ended = append(s.ended, span)
	s.mu.Unlock()
	s.cond.Broadcast()
}

// await blocks until at least n spans whose messaging.destination.name is subject have ended.
func (s *spanSink) await(subject string, n int) []sdktrace.ReadOnlySpan {
	deadline := time.AfterFunc(10*time.Second, func() {
		s.mu.Lock()
		s.expired = true
		s.mu.Unlock()
		s.cond.Broadcast()
	})
	defer deadline.Stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		var got []sdktrace.ReadOnlySpan
		for _, span := range s.ended {
			for _, kv := range span.Attributes() {
				if kv.Key == "messaging.destination.name" && kv.Value.AsString() == subject {
					got = append(got, span)
				}
			}
		}
		if len(got) >= n || s.expired {
			return got
		}
		s.cond.Wait()
	}
}

// spanShape is the expected shape of one recorded span. parent is the key of another
// spanShape, parentRemote, or empty for a root span.
type spanShape struct {
	key    string
	name   string
	kind   trace.SpanKind
	parent string
	status codes.Code
	attrs  map[string]string
}

// assertSpanShapes matches got against want by name and exact attribute set, checks kinds,
// statuses and parent links, that every span shares one trace, and returns the matches by key.
func assertSpanShapes(t *testing.T, got []sdktrace.ReadOnlySpan, want []spanShape) map[string]sdktrace.ReadOnlySpan {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("recorded %d spans, want %d", len(got), len(want))
	}
	matched := map[string]sdktrace.ReadOnlySpan{}
	used := map[int]bool{}
	for _, w := range want {
		for i, span := range got {
			if used[i] || span.Name() != w.name || !maps.Equal(spanAttrs(span), w.attrs) {
				continue
			}
			used[i] = true
			matched[w.key] = span
			break
		}
		if matched[w.key] == nil {
			t.Errorf("no recorded span matches %q (%s %v)", w.key, w.name, w.attrs)
		}
	}
	for _, w := range want {
		span := matched[w.key]
		if span == nil {
			continue
		}
		if span.SpanKind() != w.kind {
			t.Errorf("span %q kind = %v, want %v", w.key, span.SpanKind(), w.kind)
		}
		if span.Status().Code != w.status {
			t.Errorf("span %q status = %v, want %v", w.key, span.Status().Code, w.status)
		}
		assertSpanParent(t, w, span.Parent(), matched)
		if first := matched[want[0].key]; first != nil && span.SpanContext().TraceID() != first.SpanContext().TraceID() {
			t.Errorf("span %q trace = %s, want %s", w.key, span.SpanContext().TraceID(), first.SpanContext().TraceID())
		}
	}
	return matched
}

// assertSpanParent checks that parent is the span w.parent names.
func assertSpanParent(t *testing.T, w spanShape, parent trace.SpanContext, matched map[string]sdktrace.ReadOnlySpan) {
	t.Helper()
	switch {
	case w.parent == "":
		if parent.IsValid() {
			t.Errorf("span %q has parent %s, want a root span", w.key, parent.SpanID())
		}
	case w.parent == parentRemote:
		if parent.TraceID().String() != testTraceID || parent.SpanID().String() != testRemoteSpan {
			t.Errorf("span %q parent = %s/%s, want remote %s/%s", w.key, parent.TraceID(), parent.SpanID(), testTraceID, testRemoteSpan)
		}
	case matched[w.parent] != nil:
		if parent.SpanID() != matched[w.parent].SpanContext().SpanID() {
			t.Errorf("span %q parent = %s, want span %q (%s)", w.key, parent.SpanID(), w.parent, matched[w.parent].SpanContext().SpanID())
		}
	}
}

// spanAttrs renders span attributes as strings so expectations can be hand-written literals.
func spanAttrs(span sdktrace.ReadOnlySpan) map[string]string {
	attrs := map[string]string{}
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	return attrs
}

// pubSpan is the expected producer span for one publish of subject; an empty msgID omits the
// message id attribute.
func pubSpan(key, parent, subject, msgID string) spanShape {
	attrs := map[string]string{
		"messaging.system": "nats", "messaging.operation.type": "send", "messaging.operation.name": "publish", "messaging.destination.name": subject,
	}
	if msgID != "" {
		attrs["messaging.message.id"] = msgID
	}
	return spanShape{key: key, name: subject + " publish", kind: trace.SpanKindProducer, parent: parent, attrs: attrs}
}

// procSpan is the expected consumer span for one delivery of subject; an empty group or msgID
// omits that attribute.
func procSpan(key, parent, subject, group, msgID string, delivered int) spanShape {
	attrs := map[string]string{
		"messaging.system": "nats", "messaging.operation.type": "process", "messaging.destination.name": subject,
		"messaging.nats.message.delivery_count": strconv.Itoa(delivered),
	}
	if group != "" {
		attrs["messaging.consumer.group.name"] = group
	}
	if msgID != "" {
		attrs["messaging.message.id"] = msgID
	}
	return spanShape{key: key, name: subject + " process", kind: trace.SpanKindConsumer, parent: parent, attrs: attrs}
}

// failed marks the expected span as ended with an error status.
func (s spanShape) failed() spanShape {
	s.status = codes.Error
	return s
}

// memBusStep publishes msgID (with an optional initial snooze sequence header) to the row's
// subject, or, with unsub set, unsubscribes the consumer at index consumer.
type memBusStep struct {
	msgID    string
	seq      string
	unsub    bool
	consumer int
}

// memDelivery is what consumer (its index in subscription order) saw on one delivery. Generated
// "msg-<UnixNano>" ids are recorded as "msg-N".
type memDelivery struct {
	consumer  int
	msgID     string
	seq       string
	delivered uint64
}

// memSettlement scripts how handlers settle the bus-wide n-th delivery: the first fail
// deliveries return an error, the next nak call NakWithDelay(0), the next snooze call
// Snooze(snoozeDelay); later deliveries Term when term is set and otherwise return nil so the
// bus auto-acks. Rows with a script publish one message.
type memSettlement struct {
	fail, nak, snooze int
	snoozeDelay       time.Duration
	term              bool
}

var memGeneratedMsgID = regexp.MustCompile(`^msg-\d{10,}`)

// memDeliveryRecorder hands out per-consumer handlers that record every delivery and settle
// it per script.
type memDeliveryRecorder struct {
	script memSettlement
	calls  atomic.Int64
	ch     chan memDelivery
}

func newMemDeliveryRecorder(script memSettlement) *memDeliveryRecorder {
	return &memDeliveryRecorder{script: script, ch: make(chan memDelivery, 64)}
}

func (r *memDeliveryRecorder) handler(consumer int) Handler {
	return func(_ context.Context, msg Message) error {
		n := int(r.calls.Add(1))
		msgID := memGeneratedMsgID.ReplaceAllString(msg.Header().Get(HeaderMsgID), "msg-N")
		r.ch <- memDelivery{consumer: consumer, msgID: msgID, seq: msg.Header().Get(HeaderSnoozeSeq), delivered: msg.NumDelivered()}
		s := r.script
		switch {
		case n <= s.fail:
			return errors.New("transient failure")
		case n <= s.fail+s.nak:
			return msg.NakWithDelay(0)
		case n <= s.fail+s.nak+s.snooze:
			return msg.Snooze(s.snoozeDelay)
		case s.term:
			return msg.Term()
		}
		return nil
	}
}

// run applies steps to bus, waits for len(want) deliveries and compares them, sorted by
// consumer, msg id and delivery count, with want. A delivery already waiting beyond want fails.
func (r *memDeliveryRecorder) run(t *testing.T, bus *MemoryBus, subject string, subs []Subscription, steps []memBusStep, want []memDelivery) {
	t.Helper()
	for _, step := range steps {
		if step.unsub {
			if err := subs[step.consumer].Unsubscribe(); err != nil {
				t.Fatalf("Unsubscribe(%d): %v", step.consumer, err)
			}
			continue
		}
		header := nats.Header{}
		if step.seq != "" {
			header.Set(HeaderSnoozeSeq, step.seq)
		}
		if err := bus.PublishWithHeader(context.Background(), subject, step.msgID, []byte("payload"), header); err != nil {
			t.Fatalf("PublishWithHeader(%q): %v", step.msgID, err)
		}
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	got := []memDelivery{}
	for len(got) < len(want) {
		select {
		case d := <-r.ch:
			got = append(got, d)
		case <-deadline.C:
			t.Fatalf("received %+v, want %+v", got, want)
		}
	}
	select {
	case d := <-r.ch:
		t.Errorf("unexpected extra delivery %+v", d)
	default:
	}
	slices.SortFunc(got, func(a, b memDelivery) int {
		return cmp.Or(cmp.Compare(a.consumer, b.consumer), cmp.Compare(a.msgID, b.msgID), cmp.Compare(a.delivered, b.delivered))
	})
	if !slices.Equal(got, want) {
		t.Errorf("deliveries = %+v, want %+v", got, want)
	}
}

func TestMemoryBus_Subscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		subject  string
		durables []string
		steps    []memBusStep
		want     []memDelivery
	}{
		{
			name: "fan-out delivers once to each distinct durable", pattern: "aerial.events.>", subject: "aerial.events.device.created",
			durables: []string{"haro-events", "coven-events"}, steps: []memBusStep{{msgID: "evt-1"}},
			want: []memDelivery{{0, "evt-1", "", 1}, {1, "evt-1", "", 1}},
		},
		{
			name: "non-matching subject is filtered out", pattern: "aerial.turn.>", subject: "aerial.dispatch.completed",
			durables: []string{"coven-turns"}, steps: []memBusStep{{msgID: "evt-2"}}, want: []memDelivery{},
		},
		{
			name: "empty durable subscribes directly", pattern: "aerial.events.nodur", subject: "aerial.events.nodur",
			durables: []string{""}, steps: []memBusStep{{msgID: "m-nodur"}}, want: []memDelivery{{0, "m-nodur", "", 1}},
		},
		{
			name: "duplicate msg id inside the window is dropped", pattern: "aerial.events.dedup", subject: "aerial.events.dedup",
			durables: []string{"dedup-sub"}, steps: []memBusStep{{msgID: "dup-1"}, {msgID: "dup-1"}}, want: []memDelivery{{0, "dup-1", "", 1}},
		},
		{
			name: "unsubscribed durable gets nothing more", pattern: "aerial.events.unsub", subject: "aerial.events.unsub",
			durables: []string{"unsub-durable"}, steps: []memBusStep{{msgID: "msg-1"}, {unsub: true}, {msgID: "msg-2"}},
			want: []memDelivery{{0, "msg-1", "", 1}},
		},
		{
			name: "subscribers sharing a durable take turns", pattern: "aerial.events.shared", subject: "aerial.events.shared",
			durables: []string{"shared-durable", "shared-durable"}, steps: []memBusStep{{msgID: "m1"}, {msgID: "m2"}},
			want: []memDelivery{{0, "m1", "", 1}, {1, "m2", "", 1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := NewMemoryBus()
			defer bus.Close()

			rec := newMemDeliveryRecorder(memSettlement{})
			subs := make([]Subscription, len(tc.durables))
			for i, d := range tc.durables {
				sub, err := bus.Subscribe(context.Background(), tc.pattern, d, rec.handler(i))
				if err != nil {
					t.Fatalf("Subscribe failed: %v", err)
				}
				subs[i] = sub
			}
			rec.run(t, bus, tc.subject, subs, tc.steps, tc.want)
		})
	}
}

func TestMemoryBus_ConsumeQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		consumers int
		script    memSettlement
		steps     []memBusStep
		want      []memDelivery
	}{
		{
			name: "competing consumers take turns without duplicates", consumers: 3,
			steps: []memBusStep{{msgID: "job-1"}, {msgID: "job-2"}, {msgID: "job-3"}, {msgID: "job-4"}, {msgID: "job-5"}, {msgID: "job-6"}},
			want:  []memDelivery{{0, "job-1", "", 1}, {0, "job-4", "", 1}, {1, "job-2", "", 1}, {1, "job-5", "", 1}, {2, "job-3", "", 1}, {2, "job-6", "", 1}},
		},
		{
			name: "unsubscribing one consumer leaves the others serving", consumers: 2,
			steps: []memBusStep{{msgID: "m1"}, {msgID: "m2"}, {unsub: true, consumer: 0}, {msgID: "m3"}},
			want:  []memDelivery{{0, "m1", "", 1}, {1, "m2", "", 1}, {1, "m3", "", 1}},
		},
		{
			name: "handler error redelivers with a higher delivery count", consumers: 1, script: memSettlement{fail: 1},
			steps: []memBusStep{{msgID: "fail-1"}}, want: []memDelivery{{0, "fail-1", "", 1}, {0, "fail-1", "", 2}},
		},
		{
			name: "nak redelivers and term ends the message", consumers: 1, script: memSettlement{nak: 1, term: true},
			steps: []memBusStep{{msgID: "job-term-1"}}, want: []memDelivery{{0, "job-term-1", "", 1}, {0, "job-term-1", "", 2}},
		},
		{
			name: "snooze rotates the msg id and resets the delivery count", consumers: 1, script: memSettlement{snooze: 1},
			steps: []memBusStep{{msgID: "job-42"}}, want: []memDelivery{{0, "job-42", "", 1}, {0, "job-42:snooze:1", "1", 1}},
		},
		{
			name: "consecutive snoozes advance the suffix", consumers: 1, script: memSettlement{snooze: 3}, steps: []memBusStep{{msgID: "job-99"}},
			want: []memDelivery{{0, "job-99", "", 1}, {0, "job-99:snooze:1", "1", 1}, {0, "job-99:snooze:2", "2", 1}, {0, "job-99:snooze:3", "3", 1}},
		},
		{
			name: "re-snooze increments an existing snooze suffix", consumers: 1, script: memSettlement{snooze: 1},
			steps: []memBusStep{{msgID: "job-abc:snooze:5", seq: "5"}}, want: []memDelivery{{0, "job-abc:snooze:5", "5", 1}, {0, "job-abc:snooze:6", "6", 1}},
		},
		{
			name: "delayed snooze republishes after the delay", consumers: 1, script: memSettlement{snooze: 1, snoozeDelay: time.Millisecond},
			steps: []memBusStep{{msgID: "job-delay-1"}}, want: []memDelivery{{0, "job-delay-1", "", 1}, {0, "job-delay-1:snooze:1", "1", 1}},
		},
		{
			name: "snooze without a msg id generates one", consumers: 1, script: memSettlement{snooze: 1},
			steps: []memBusStep{{}}, want: []memDelivery{{0, "", "", 1}, {0, "msg-N:snooze:1", "1", 1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := NewMemoryBus()
			defer bus.Close()

			rec := newMemDeliveryRecorder(tc.script)
			subs := make([]Subscription, tc.consumers)
			for i := range subs {
				sub, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.>", "workers", rec.handler(i))
				if err != nil {
					t.Fatalf("ConsumeQueue failed: %v", err)
				}
				subs[i] = sub
			}
			rec.run(t, bus, "aerial.jobs.work", subs, tc.steps, tc.want)
		})
	}
}

func TestMessage_SettledSettlementIsNoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		msg           Message
		wantSubject   string
		wantData      string
		wantDelivered uint64
	}{
		{
			name:          "memory message",
			msg:           &memMessage{bus: NewMemoryBus(), subject: "aerial.jobs.settled", data: []byte("d"), delivered: 1, settled: true},
			wantSubject:   "aerial.jobs.settled",
			wantData:      "d",
			wantDelivered: 1,
		},
		{
			name: "jetstream message",
			msg: &jsMessage{
				raw:     &fakeMsg{subject: "aerial.jobs.jsmsg", data: []byte("raw"), header: nats.Header{}},
				bus:     newJetStreamBusWithContext(nil, &mockJetStream{}),
				settled: true,
			},
			wantSubject:   "aerial.jobs.jsmsg",
			wantData:      "raw",
			wantDelivered: 1,
		},
		{
			name: "core message with header",
			msg: &coreMessage{
				raw: &nats.Msg{Subject: "voice.stream.settled-1", Data: []byte("vdata-1"), Header: nats.Header{"X-V": []string{"1"}}},
			},
			wantSubject:   "voice.stream.settled-1",
			wantData:      "vdata-1",
			wantDelivered: 1,
		},
		{
			name: "core message nil header",
			msg: &coreMessage{
				raw: &nats.Msg{Subject: "voice.stream.settled-2", Data: []byte("vdata-2")},
			},
			wantSubject:   "voice.stream.settled-2",
			wantData:      "vdata-2",
			wantDelivered: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.msg.Ack(); err != nil {
				t.Errorf("Ack = %v, want nil", err)
			}
			if err := tc.msg.NakWithDelay(0); err != nil {
				t.Errorf("NakWithDelay = %v, want nil", err)
			}
			if err := tc.msg.Term(); err != nil {
				t.Errorf("Term = %v, want nil", err)
			}
			if err := tc.msg.Snooze(0); err != nil {
				t.Errorf("Snooze = %v, want nil", err)
			}
			if err := tc.msg.InProgress(); err != nil {
				t.Errorf("InProgress = %v, want nil", err)
			}
			if tc.msg.Header() == nil {
				t.Error("Header() = nil, want a non-nil header")
			}
			if gotSub := tc.msg.Subject(); gotSub != tc.wantSubject {
				t.Errorf("Subject() = %q, want %q", gotSub, tc.wantSubject)
			}
			if gotData := string(tc.msg.Data()); gotData != tc.wantData {
				t.Errorf("Data() = %q, want %q", gotData, tc.wantData)
			}
			if gotDeliv := tc.msg.NumDelivered(); gotDeliv != tc.wantDelivered {
				t.Errorf("NumDelivered() = %d, want %d", gotDeliv, tc.wantDelivered)
			}
		})
	}
}

func TestTracePropagation_InjectAndExtract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		subject          string
		durable          string
		queue            string
		msgID            string
		traceparent      string
		failDeliveries   int
		snoozeDeliveries int
		want             []spanShape
	}{
		{
			name:    "durable subscriber process span is a child of the publish span",
			subject: "aerial.events.tracing.durable", durable: "tracer", msgID: "trace-msg-1",
			traceparent: testTraceparent,
			want: []spanShape{
				pubSpan("pub", parentRemote, "aerial.events.tracing.durable", "trace-msg-1"),
				procSpan("proc", "pub", "aerial.events.tracing.durable", "tracer", "trace-msg-1", 1),
			},
		},
		{
			name:    "queue redelivery after a handler error is a second process span",
			subject: "aerial.jobs.tracing.retry", queue: "retry-workers", msgID: "trace-msg-2",
			traceparent: testTraceparent, failDeliveries: 1,
			want: []spanShape{
				pubSpan("pub", parentRemote, "aerial.jobs.tracing.retry", "trace-msg-2"),
				procSpan("proc1", "pub", "aerial.jobs.tracing.retry", "retry-workers", "trace-msg-2", 1).failed(),
				procSpan("proc2", "pub", "aerial.jobs.tracing.retry", "retry-workers", "trace-msg-2", 2),
			},
		},
		{
			name:    "snooze republishes under the process span in the same trace",
			subject: "aerial.jobs.tracing.snooze", queue: "snooze-workers", msgID: "trace-msg-3",
			traceparent: testTraceparent, snoozeDeliveries: 1,
			want: []spanShape{
				pubSpan("pub", parentRemote, "aerial.jobs.tracing.snooze", "trace-msg-3"),
				procSpan("proc1", "pub", "aerial.jobs.tracing.snooze", "snooze-workers", "trace-msg-3", 1),
				pubSpan("pub2", "proc1", "aerial.jobs.tracing.snooze", "trace-msg-3:snooze:1"),
				procSpan("proc2", "pub2", "aerial.jobs.tracing.snooze", "snooze-workers", "trace-msg-3:snooze:1", 1),
			},
		},
		{
			name:    "untraced publish without msg id to an ephemeral subscriber starts a root",
			subject: "aerial.events.tracing.root",
			want: []spanShape{
				pubSpan("pub", "", "aerial.events.tracing.root", ""),
				procSpan("proc", "pub", "aerial.events.tracing.root", "", "", 1),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := NewMemoryBus()
			defer bus.Close()

			var mu sync.Mutex
			calls := 0
			var handlerSpans []trace.SpanID
			handler := func(ctx context.Context, msg Message) error {
				mu.Lock()
				calls++
				call := calls
				handlerSpans = append(handlerSpans, trace.SpanContextFromContext(ctx).SpanID())
				mu.Unlock()
				switch {
				case call <= tc.failDeliveries:
					return errors.New("transient failure")
				case call <= tc.failDeliveries+tc.snoozeDeliveries:
					return msg.Snooze(0)
				}
				return nil
			}
			var err error
			if tc.queue != "" {
				_, err = bus.ConsumeQueue(context.Background(), tc.subject, tc.queue, handler)
			} else {
				_, err = bus.Subscribe(context.Background(), tc.subject, tc.durable, handler)
			}
			if err != nil {
				t.Fatalf("subscribe failed: %v", err)
			}

			ctx := context.Background()
			if tc.traceparent != "" {
				ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": tc.traceparent})
			}
			if err := bus.Publish(ctx, tc.subject, tc.msgID, []byte("traced")); err != nil {
				t.Fatalf("Publish failed: %v", err)
			}

			got := assertSpanShapes(t, testSpans.await(tc.subject, len(tc.want)), tc.want)
			mu.Lock()
			defer mu.Unlock()
			for _, w := range tc.want {
				if w.kind == trace.SpanKindConsumer && got[w.key] != nil && !slices.Contains(handlerSpans, got[w.key].SpanContext().SpanID()) {
					t.Errorf("handler never ran under process span %q; handler spans %v", w.key, handlerSpans)
				}
			}
		})
	}
}

func TestCoalescedStream_SupersededSequence(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var firstMsg Message
	msg1Received := make(chan struct{})
	allowMsg1Finish := make(chan struct{})
	doneMsg1 := make(chan struct{})
	handledMsg2 := make(chan struct{})
	var calls atomic.Int32

	_, err := bus.ConsumeQueue(context.Background(), "aerial.coalesced.device.1", "device-workers", func(ctx context.Context, msg Message) error {
		if calls.Add(1) == 2 {
			defer close(handledMsg2)
		}
		if firstMsg == nil {
			firstMsg = msg
			close(msg1Received)
			<-allowMsg1Finish
			defer close(doneMsg1)
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ConsumeQueue failed: %v", err)
	}

	// Publish message 1
	if err = bus.Publish(context.Background(), "aerial.coalesced.device.1", "state-v1", []byte("v1")); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	<-msg1Received

	// Publish message 2 to the exact same subject to supersede message 1
	if err = bus.Publish(context.Background(), "aerial.coalesced.device.1", "state-v2", []byte("v2")); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}
	select {
	case <-handledMsg2:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for superseding message to be handled")
	}

	// Now message 1 has been superseded
	err = firstMsg.InProgress()
	if !errors.Is(err, nats.ErrMsgNotFound) {
		t.Errorf("expected ErrMsgNotFound on superseded message InProgress, got: %v", err)
	}

	// Snoozing superseded message should be a benign no-op (exercises coalesced supersede check)
	if err := firstMsg.Snooze(0); err != nil {
		t.Errorf("expected nil error on superseded message Snooze, got: %v", err)
	}

	// Acking superseded message should be a benign no-op
	if err := firstMsg.Ack(); err != nil {
		t.Errorf("expected nil error on superseded message Ack, got: %v", err)
	}

	close(allowMsg1Finish)
	<-doneMsg1
}

func TestMatchSubject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		subject string
		want    bool
	}{
		{pattern: ">", subject: "aerial.dispatch.completed", want: true},
		{pattern: "aerial.dispatch.>", subject: "aerial.dispatch.completed", want: true},
		{pattern: "aerial.dispatch.>", subject: "aerial.dispatch.vanadis.chunk.1", want: true},
		{pattern: "aerial.events.*", subject: "aerial.events.github", want: true},
		{pattern: "aerial.events.*", subject: "aerial.events.github.push", want: false},
		{pattern: "aerial.turn.completed", subject: "aerial.turn.completed", want: true},
		{pattern: "aerial.turn.completed", subject: "aerial.turn.failed", want: false},
		{pattern: "aerial.jobs.>", subject: "aerial.events.foo", want: false},
		{pattern: "aerial.jobs.foo.bar", subject: "aerial.jobs.foo", want: false},
		{pattern: "aerial.*.events", subject: "aerial.custom.events", want: true},
		{pattern: "aerial.*.events", subject: "aerial.custom.other", want: false},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s_vs_%s", tc.pattern, tc.subject), func(t *testing.T) {
			t.Parallel()
			got := matchSubject(tc.pattern, tc.subject)
			if got != tc.want {
				t.Errorf("matchSubject(%q, %q) = %v, want %v", tc.pattern, tc.subject, got, tc.want)
			}
		})
	}
}

func TestMemoryBus_ClosedAndErrorPaths(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	if err := bus.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "sub", "id", "data"); err == nil {
		t.Error("expected error publishing on closed bus, got nil")
	}

	if _, err := bus.Subscribe(context.Background(), "sub", "d", func(ctx context.Context, msg Message) error { return nil }); err == nil {
		t.Error("expected error subscribing on closed bus, got nil")
	}

	if _, err := bus.ConsumeQueue(context.Background(), "sub", "q", func(ctx context.Context, msg Message) error { return nil }); err == nil {
		t.Error("expected error consume queue on closed bus, got nil")
	}

	// Snooze on closed bus
	memMsg := &memMessage{
		bus:     bus,
		subject: "aerial.jobs.closed",
		data:    []byte("closed"),
	}
	if err := memMsg.Snooze(0); err == nil {
		t.Error("expected error snoozing message on closed bus, got nil")
	}

	// A nil header clones to an empty, non-nil header.
	if h := cloneHeader(nil); h == nil {
		t.Error("expected non-nil header from cloneHeader(nil)")
	}

	// Serialization error on open bus
	openBus := NewMemoryBus()
	defer openBus.Close()
	unmarshalable := make(chan int)
	if err := openBus.Publish(context.Background(), "sub", "id", unmarshalable); err == nil {
		t.Error("expected error serializing chan, got nil")
	}
}

func TestIsMessageNotFoundError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		want bool
	}{
		{err: nil, want: false},
		{err: nats.ErrMsgNotFound, want: true},
		{err: errors.New("nats: message not found"), want: true},
		{err: errors.New("10039: message not found"), want: true},
		{err: errors.New("some unexpected connection failure"), want: false},
	}

	for _, tc := range tests {
		got := isMessageNotFoundError(tc.err)
		if got != tc.want {
			t.Errorf("isMessageNotFoundError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestNewJetStreamBus_Validation(t *testing.T) {
	t.Parallel()

	// A nil connection is rejected.
	if _, err := NewJetStreamBusFromConn(nil); err == nil {
		t.Error("expected error for nil conn, got nil")
	}

	// NewJetStreamBus with invalid URL and canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewJetStreamBus(ctx, "://"); err == nil {
		t.Error("expected error for invalid nats url, got nil")
	}

	// Successful connect and JetStream initialization via mock dialer
	dialer := &mockNatsDialer{}
	bus, err := NewJetStreamBus(context.Background(), "nats://127.0.0.1:4222",
		nats.SetCustomDialer(dialer),
		nats.Timeout(1*time.Second),
		nats.NoReconnect(),
		nats.MaxReconnects(0),
	)
	if err != nil {
		t.Fatalf("NewJetStreamBus with mock dialer failed: %v", err)
	}
	if err = bus.Close(); err != nil {
		t.Errorf("bus.Close failed: %v", err)
	}

	// NewJetStreamBusFromConn with valid connection
	nc, err := nats.Connect("nats://127.0.0.1:4222",
		nats.SetCustomDialer(dialer),
		nats.Timeout(1*time.Second),
		nats.NoReconnect(),
		nats.MaxReconnects(0),
	)
	if err != nil {
		t.Fatalf("Connect with mock dialer failed: %v", err)
	}
	busConn, err := NewJetStreamBusFromConn(nc)
	if err != nil {
		t.Fatalf("NewJetStreamBusFromConn failed: %v", err)
	}
	if err := busConn.Close(); err != nil {
		t.Errorf("busConn.Close failed: %v", err)
	}
}

type mockNatsDialer struct{}

func (*mockNatsDialer) Dial(network, address string) (net.Conn, error) {
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
			msg := string(buf[:n])
			if strings.Contains(msg, "PING") {
				_, _ = c2.Write([]byte("PONG\r\n"))
			}
		}
	}()
	return c1, nil
}

func (m *mockJetStream) publishedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.publishedMsgs)
}

func TestJetStreamBus_ConsumerStats(t *testing.T) {
	t.Parallel()

	var nilJSBus *JetStreamBus
	if _, _, err := nilJSBus.ConsumerStats(context.Background(), "stream", "consumer"); err == nil {
		t.Error("expected error from nil bus ConsumerStats")
	}

	memBusStats := NewMemoryBus()
	defer memBusStats.Close()
	if p, inf, err := memBusStats.ConsumerStats(context.Background(), "stream", "consumer"); err != nil || p != 0 || inf != 0 {
		t.Errorf("MemoryBus.ConsumerStats = (%d, %d, %v), want (0, 0, nil)", p, inf, err)
	}

	mjs := &mockJetStream{
		existingStreams: make(map[string]*nats.StreamInfo),
		consumers: map[string]*nats.ConsumerInfo{
			"stream/consumer": {Stream: "stream", Name: "consumer", NumPending: 7, NumAckPending: 3},
		},
	}
	bus := newJetStreamBusWithContext(nil, mjs)

	if p, inf, err := bus.ConsumerStats(context.Background(), "stream", "consumer"); err != nil || p != 7 || inf != 3 {
		t.Errorf("ConsumerStats with ctx = (%d, %d, %v), want (7, 3, nil)", p, inf, err)
	}
	if p, inf, err := bus.ConsumerStats(nil, "stream", "consumer"); err != nil || p != 7 || inf != 3 {
		t.Errorf("ConsumerStats with nil ctx = (%d, %d, %v), want (7, 3, nil)", p, inf, err)
	}
	mjs.consumerInfoErr = errors.New("simulated consumer info err")
	if _, _, err := bus.ConsumerStats(context.Background(), "stream", "consumer"); err == nil {
		t.Error("expected error from bus.ConsumerStats with consumerInfoErr")
	}
}

// newJetStreamBusWithContext builds a bus around js with a 1ms heartbeat. A
// non-nil nc also gets a jetstream handle so durable consumers work.
func newJetStreamBusWithContext(nc *nats.Conn, js nats.JetStreamContext) *JetStreamBus {
	bus := &JetStreamBus{nc: nc, js: js, heartbeatInterval: 1 * time.Millisecond, pullHeartbeat: defaultPullHeartbeat}
	if nc != nil {
		jet, err := jetstream.New(nc)
		if err != nil {
			panic(err)
		}
		bus.jet = jet
	}
	return bus
}

func TestJetStreamBusOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		extra                []nats.Option
		wantMaxReconnect     int
		wantRetryOnFailed    bool
		wantIgnoreAuthAbort  bool
		wantClosedHandlerSet bool
	}{
		{"defaults", nil, -1, true, true, true},
		{"caller overrides retry on failed connect", []nats.Option{nats.RetryOnFailedConnect(false)}, -1, false, true, true},
		{"caller overrides max reconnects", []nats.Option{nats.MaxReconnects(5)}, 5, true, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := nats.GetDefaultOptions()
			for _, opt := range jetStreamBusOptions(tc.extra...) {
				if err := opt(&o); err != nil {
					t.Fatalf("apply option: %v", err)
				}
			}
			if o.MaxReconnect != tc.wantMaxReconnect {
				t.Errorf("MaxReconnect = %d, want %d", o.MaxReconnect, tc.wantMaxReconnect)
			}
			if o.RetryOnFailedConnect != tc.wantRetryOnFailed {
				t.Errorf("RetryOnFailedConnect = %v, want %v", o.RetryOnFailedConnect, tc.wantRetryOnFailed)
			}
			if o.IgnoreAuthErrorAbort != tc.wantIgnoreAuthAbort {
				t.Errorf("IgnoreAuthErrorAbort = %v, want %v", o.IgnoreAuthErrorAbort, tc.wantIgnoreAuthAbort)
			}
			if (o.ClosedCB != nil) != tc.wantClosedHandlerSet {
				t.Errorf("ClosedCB set = %v, want %v", o.ClosedCB != nil, tc.wantClosedHandlerSet)
			}
		})
	}
}

func newMockDialedBus(t *testing.T) *JetStreamBus {
	t.Helper()
	bus, err := NewJetStreamBus(context.Background(), "nats://127.0.0.1:4222",
		nats.SetCustomDialer(&mockNatsDialer{}), nats.Timeout(1*time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatalf("NewJetStreamBus with mock dialer: %v", err)
	}
	return bus
}

func TestJetStreamBus_Healthy(t *testing.T) {
	t.Parallel()

	openBus := newMockDialedBus(t)
	t.Cleanup(func() { _ = openBus.Close() })
	closedBus := newMockDialedBus(t)
	_ = closedBus.Close()

	tests := []struct {
		name       string
		bus        *JetStreamBus
		wantErr    bool
		wantClosed bool
	}{
		{"nil bus", nil, true, false},
		{"bus without connection", newJetStreamBusWithContext(nil, &mockJetStream{}), true, false},
		{"open connection", openBus, false, false},
		{"permanently closed connection", closedBus, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.bus.Healthy()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Healthy err = %v, wantErr %v", err, tc.wantErr)
			}
			if errors.Is(err, ErrConnectionClosed) != tc.wantClosed {
				t.Errorf("errors.Is(err, ErrConnectionClosed) = %v, want %v (err %v)", errors.Is(err, ErrConnectionClosed), tc.wantClosed, err)
			}
		})
	}
}

func TestBuildConnectOptions_UserCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		user         string
		pass         string
		url          string
		wantUser     string
		wantPassword string
	}{
		{name: "credentials injected without userinfo", user: "u1", pass: "p1", url: "nats://127.0.0.1:4222", wantUser: "u1", wantPassword: "p1"},
		{name: "url userinfo kept without nats.UserInfo", user: "u1", pass: "p1", url: "nats://existing:pass@127.0.0.1:4222"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := buildConnectOptions(tc.url, tc.user, tc.pass)
			var natsOpts nats.Options
			for _, opt := range opts {
				if err := opt(&natsOpts); err != nil {
					t.Fatalf("unexpected option error: %v", err)
				}
			}
			if natsOpts.User != tc.wantUser {
				t.Errorf("natsOpts.User = %q, want %q", natsOpts.User, tc.wantUser)
			}
			if natsOpts.Password != tc.wantPassword {
				t.Errorf("natsOpts.Password = %q, want %q", natsOpts.Password, tc.wantPassword)
			}
		})
	}
}

func TestIsCoreSubject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		subject string
		want    bool
	}{
		{subject: "voice.stream.turn-123", want: true},
		{subject: "voice.stream.abc.def", want: true},
		{subject: "  voice.stream.padded  ", want: true},
		{subject: "voice.stream.", want: true},
		{subject: "voice.stream", want: false},
		{subject: "voice.events.turn", want: false},
		{subject: "aerial.events.voice.stream", want: false},
		{subject: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.subject, func(t *testing.T) {
			t.Parallel()
			if got := isCoreSubject(tc.subject); got != tc.want {
				t.Errorf("isCoreSubject(%q) = %v, want %v", tc.subject, got, tc.want)
			}
		})
	}
}

func TestJetStreamBus_CoreVoice_PublishValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		closedConn bool
		subject    string
		wantErrSub string
	}{
		{
			name:       "publish on core subject with nil nats connection fails",
			subject:    "voice.stream.turn-nil-pub",
			wantErrSub: "nats connection is nil",
		},
		{
			name:       "publish on core subject with closed nats connection fails",
			closedConn: true,
			subject:    "voice.stream.turn-closed-pub",
			wantErrSub: "publish to voice.stream.turn-closed-pub",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &JetStreamBus{}
			if tc.closedConn {
				nc := startJetStreamServer(t)
				b, err := NewJetStreamBusFromConn(nc)
				if err != nil {
					t.Fatalf("NewJetStreamBusFromConn failed: %v", err)
				}
				if closeErr := b.Close(); closeErr != nil {
					t.Fatalf("bus.Close failed: %v", closeErr)
				}
				bus = b
			}

			err := bus.PublishWithHeader(context.Background(), tc.subject, "msg-1", []byte("payload"), nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("PublishWithHeader error = %v, want substring %q", err, tc.wantErrSub)
			}
		})
	}
}

func TestJetStreamBus_CoreVoice_SubscribeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		closedConn bool
		subject    string
		durable    string
		wantErrSub string
		wantErrIs  error
	}{
		{
			name:       "subscribe on core subject with nil nats connection fails",
			subject:    "voice.stream.turn-nil-sub",
			durable:    "",
			wantErrSub: "nats connection is nil",
		},
		{
			name:      "subscribe on non-core subject with empty durable returns errDurableRequired",
			subject:   "aerial.events.requires.durable",
			durable:   "",
			wantErrIs: errDurableRequired,
		},
		{
			name:       "subscribe on core subject with closed nats connection fails",
			closedConn: true,
			subject:    "voice.stream.turn-closed-sub",
			durable:    "",
			wantErrSub: "subscribe to voice.stream.turn-closed-sub",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &JetStreamBus{}
			if tc.closedConn {
				nc := startJetStreamServer(t)
				b, err := NewJetStreamBusFromConn(nc)
				if err != nil {
					t.Fatalf("NewJetStreamBusFromConn failed: %v", err)
				}
				if closeErr := b.Close(); closeErr != nil {
					t.Fatalf("bus.Close failed: %v", closeErr)
				}
				bus = b
			}

			_, err := bus.Subscribe(context.Background(), tc.subject, tc.durable, func(ctx context.Context, msg Message) error {
				return nil
			})
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("Subscribe error = %v, want errors.Is %v", err, tc.wantErrIs)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("Subscribe error = %v, want substring %q", err, tc.wantErrSub)
			}
		})
	}
}

type coreVoiceObservation struct {
	subject   string
	data      string
	customVal string
	msgID     string
	delivered uint64
	traceID   string
}

func assertCoreVoiceDelivery(t *testing.T, got coreVoiceObservation, wantSub, wantData, wantCustom, wantMsgID, wantTraceID string) {
	t.Helper()
	if got.subject != wantSub {
		t.Errorf("gotSubject = %q, want %q", got.subject, wantSub)
	}
	if got.data != wantData {
		t.Errorf("gotData = %q, want %q", got.data, wantData)
	}
	if wantCustom != "" && got.customVal != wantCustom {
		t.Errorf("gotCustomVal = %q, want %q", got.customVal, wantCustom)
	}
	if got.msgID != wantMsgID {
		t.Errorf("gotMsgID = %q, want %q", got.msgID, wantMsgID)
	}
	if got.delivered != 1 {
		t.Errorf("gotDelivered = %d, want 1", got.delivered)
	}
	if wantTraceID != "" && got.traceID != wantTraceID {
		t.Errorf("gotTraceID = %q, want %q", got.traceID, wantTraceID)
	}
}

type coreVoiceTestCase struct {
	name        string
	subject     string
	msgID       string
	traceparent string
	customKey   string
	customVal   string
	payload     string
	handlerErr  error
	wantSpan    spanShape
}

func runCoreVoiceTest(t *testing.T, tc coreVoiceTestCase) {
	t.Helper()
	nc := startJetStreamServer(t)
	mjs := &mockJetStream{}
	bus := newJetStreamBusWithContext(nc, mjs)
	t.Cleanup(func() {
		if closeErr := bus.Close(); closeErr != nil {
			t.Errorf("bus.Close failed: %v", closeErr)
		}
	})

	deliveredCh := make(chan struct{})
	var obs coreVoiceObservation

	sub, err := bus.Subscribe(context.Background(), tc.subject, "", func(ctx context.Context, msg Message) error {
		defer close(deliveredCh)
		obs = coreVoiceObservation{
			subject:   msg.Subject(),
			data:      string(msg.Data()),
			customVal: msg.Header().Get(tc.customKey),
			msgID:     msg.Header().Get(HeaderMsgID),
			delivered: msg.NumDelivered(),
			traceID:   trace.SpanContextFromContext(ctx).TraceID().String(),
		}
		return tc.handlerErr
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	t.Cleanup(func() {
		if unsubErr := sub.Unsubscribe(); unsubErr != nil {
			t.Errorf("sub.Unsubscribe failed: %v", unsubErr)
		}
	})

	ctx := context.Background()
	if tc.traceparent != "" {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": tc.traceparent})
	}
	customHdr := nats.Header{}
	if tc.customKey != "" {
		customHdr.Set(tc.customKey, tc.customVal)
	}

	if pubErr := bus.PublishWithHeader(ctx, tc.subject, tc.msgID, []byte(tc.payload), customHdr); pubErr != nil {
		t.Fatalf("PublishWithHeader failed: %v", pubErr)
	}

	select {
	case <-deliveredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message delivery on core subject")
	}

	if mjs.publishedCount() != 0 {
		t.Errorf("JetStream PublishMsg called %d times for core subject, want 0", mjs.publishedCount())
	}

	wantTrace := ""
	if tc.traceparent != "" {
		wantTrace = testTraceID
	}
	assertCoreVoiceDelivery(t, obs, tc.subject, tc.payload, tc.customVal, tc.msgID, wantTrace)

	spans := assertSpanShapes(t, testSpans.await(tc.subject, 2), []spanShape{
		pubSpan("pub", parentRemote, tc.subject, tc.msgID),
		tc.wantSpan,
	})
	if spans["proc"] == nil {
		t.Errorf("expected process span for %s", tc.subject)
	}
}

func TestJetStreamBus_CoreVoice_PublishSubscribeDispatch(t *testing.T) {
	t.Parallel()

	tests := []coreVoiceTestCase{
		{
			name:        "successful core dispatch propagates traceparent and attributes",
			subject:     "voice.stream.turn-trace-ok",
			msgID:       "vmsg-1",
			traceparent: testTraceparent,
			customKey:   "X-Custom-Voice",
			customVal:   "voice-meta-ok",
			payload:     "sentence chunk audio",
			wantSpan:    procSpan("proc", "pub", "voice.stream.turn-trace-ok", "", "vmsg-1", 1),
		},
		{
			name:        "handler error fails consumer process span",
			subject:     "voice.stream.turn-trace-err",
			msgID:       "vmsg-2",
			traceparent: testTraceparent,
			payload:     "error chunk",
			handlerErr:  errors.New("downstream speech error"),
			wantSpan:    procSpan("proc", "pub", "voice.stream.turn-trace-err", "", "vmsg-2", 1).failed(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCoreVoiceTest(t, tc)
		})
	}
}

func TestJetStreamBus_DispatchCore_Direct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subject  string
		msg      *nats.Msg
		wantSpan spanShape
	}{
		{
			name:     "dispatch with nil header starts root process span",
			subject:  "voice.stream.direct.root",
			msg:      &nats.Msg{Subject: "voice.stream.direct.root", Data: []byte("d")},
			wantSpan: procSpan("proc", "", "voice.stream.direct.root", "", "", 1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &JetStreamBus{}
			bus.dispatchCore(context.Background(), tc.msg, func(ctx context.Context, msg Message) error {
				return nil
			})
			assertSpanShapes(t, testSpans.await(tc.subject, 1), []spanShape{tc.wantSpan})
		})
	}
}
