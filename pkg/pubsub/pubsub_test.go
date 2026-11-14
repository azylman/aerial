package pubsub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestMemoryBus_PublishSubscribeAndWorkQueue(t *testing.T) {
	t.Parallel()

	type publishStep struct {
		subject string
		msgID   string
		payload string
	}

	tests := []struct {
		name             string
		isQueue          bool
		pattern          string
		durables         []string
		queueName        string
		numConsumers     int
		steps            []publishStep
		wantTotalHandled int
	}{
		{
			name:             "pubsub fan-out delivers to all distinct durable subscribers",
			isQueue:          false,
			pattern:          "aerial.events.>",
			durables:         []string{"haro-events", "coven-events"},
			steps:            []publishStep{{subject: "aerial.events.device.created", msgID: "evt-1", payload: "data1"}},
			wantTotalHandled: 2,
		},
		{
			name:             "pubsub filters out non-matching subjects",
			isQueue:          false,
			pattern:          "aerial.turn.>",
			durables:         []string{"coven-turns"},
			steps:            []publishStep{{subject: "aerial.dispatch.completed", msgID: "evt-2", payload: "data2"}},
			wantTotalHandled: 0,
		},
		{
			name:         "competing consumer queue balances messages across workers without duplicates",
			isQueue:      true,
			pattern:      "aerial.jobs.>",
			queueName:    "control-workers",
			numConsumers: 3,
			steps: []publishStep{
				{subject: "aerial.jobs.sync", msgID: "job-1", payload: "p1"},
				{subject: "aerial.jobs.sync", msgID: "job-2", payload: "p2"},
				{subject: "aerial.jobs.sync", msgID: "job-3", payload: "p3"},
				{subject: "aerial.jobs.sync", msgID: "job-4", payload: "p4"},
				{subject: "aerial.jobs.sync", msgID: "job-5", payload: "p5"},
				{subject: "aerial.jobs.sync", msgID: "job-6", payload: "p6"},
			},
			wantTotalHandled: 6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := NewMemoryBus()
			defer bus.Close()

			var totalHandled atomic.Int64
			var wg sync.WaitGroup
			wg.Add(tc.wantTotalHandled)

			if tc.isQueue {
				for range tc.numConsumers {
					_, err := bus.ConsumeQueue(context.Background(), tc.pattern, tc.queueName, func(ctx context.Context, msg Message) error {
						totalHandled.Add(1)
						wg.Done()
						return nil
					})
					if err != nil {
						t.Fatalf("ConsumeQueue failed: %v", err)
					}
				}
			} else {
				for _, d := range tc.durables {
					_, err := bus.Subscribe(context.Background(), tc.pattern, d, func(ctx context.Context, msg Message) error {
						totalHandled.Add(1)
						wg.Done()
						return nil
					})
					if err != nil {
						t.Fatalf("Subscribe failed: %v", err)
					}
				}
			}

			for _, step := range tc.steps {
				if err := bus.Publish(context.Background(), step.subject, step.msgID, step.payload); err != nil {
					t.Fatalf("Publish failed: %v", err)
				}
			}

			if tc.wantTotalHandled > 0 {
				wg.Wait()
			}

			got := int(totalHandled.Load())
			if got != tc.wantTotalHandled {
				t.Errorf("handled %d messages, want %d", got, tc.wantTotalHandled)
			}
		})
	}
}

func TestSnooze_RotatesMsgIDAndResetsAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialMsgID   string
		initialSeq     string
		snoozeRounds   int
		wantFinalMsgID string
		wantFinalSeq   string
		wantDelivered  uint64
		wantAcks       int
	}{
		{
			name:           "single snooze from clean base id rotates suffix and keeps delivered 1",
			initialMsgID:   "job-42",
			initialSeq:     "",
			snoozeRounds:   1,
			wantFinalMsgID: "job-42:snooze:1",
			wantFinalSeq:   "1",
			wantDelivered:  1,
			wantAcks:       1,
		},
		{
			name:           "multiple consecutive snoozes monotonically rotate generation suffix",
			initialMsgID:   "job-99",
			initialSeq:     "",
			snoozeRounds:   3,
			wantFinalMsgID: "job-99:snooze:3",
			wantFinalSeq:   "3",
			wantDelivered:  1,
			wantAcks:       3,
		},
		{
			name:           "re-snooze of message that already had snooze sequence increments it",
			initialMsgID:   "job-abc:snooze:5",
			initialSeq:     "5",
			snoozeRounds:   1,
			wantFinalMsgID: "job-abc:snooze:6",
			wantFinalSeq:   "6",
			wantDelivered:  1,
			wantAcks:       1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := NewMemoryBus()
			defer bus.Close()

			var wg sync.WaitGroup
			wg.Add(tc.snoozeRounds + 1)

			var mu sync.Mutex
			currentRound := 0
			ackCount := 0
			var finalMsgID string
			var finalSeq string
			var finalDelivered uint64

			_, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.snooze", "snooze-workers", func(ctx context.Context, msg Message) error {
				mu.Lock()
				currentRound++
				round := currentRound
				mu.Unlock()

				wg.Done()

				if round <= tc.snoozeRounds {
					mu.Lock()
					ackCount++
					mu.Unlock()
					return msg.Snooze(0)
				}

				mu.Lock()
				finalMsgID = msg.Header().Get(HeaderMsgID)
				finalSeq = msg.Header().Get(HeaderSnoozeSeq)
				finalDelivered = msg.NumDelivered()
				ackCount++
				mu.Unlock()
				return msg.Ack()
			})
			if err != nil {
				t.Fatalf("ConsumeQueue failed: %v", err)
			}

			initialHeader := nats.Header{}
			if tc.initialSeq != "" {
				initialHeader.Set(HeaderSnoozeSeq, tc.initialSeq)
			}

			err = bus.PublishWithHeader(context.Background(), "aerial.jobs.snooze", tc.initialMsgID, []byte("payload"), initialHeader)
			if err != nil {
				t.Fatalf("PublishWithHeader failed: %v", err)
			}

			wg.Wait()

			mu.Lock()
			defer mu.Unlock()

			if finalMsgID != tc.wantFinalMsgID {
				t.Errorf("final msg ID = %q, want %q", finalMsgID, tc.wantFinalMsgID)
			}
			if finalSeq != tc.wantFinalSeq {
				t.Errorf("final snooze seq = %q, want %q", finalSeq, tc.wantFinalSeq)
			}
			if finalDelivered != tc.wantDelivered {
				t.Errorf("final delivered = %d, want %d", finalDelivered, tc.wantDelivered)
			}
			if ackCount != tc.wantAcks+1 {
				t.Errorf("ack count = %d, want %d", ackCount, tc.wantAcks+1)
			}
		})
	}
}

func TestTracePropagation_InjectAndExtract(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	traceIDHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	spanIDHex := "00f067aa0ba902b7"
	traceparentVal := fmt.Sprintf("00-%s-%s-01", traceIDHex, spanIDHex)

	carrier := propagation.MapCarrier{"traceparent": traceparentVal}
	ctxWithTrace := propagation.TraceContext{}.Extract(context.Background(), carrier)

	var extractedTraceID string
	var extractedSpanID string
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := bus.Subscribe(context.Background(), "aerial.events.tracing", "tracer", func(ctx context.Context, msg Message) error {
		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.IsValid() {
			extractedTraceID = spanCtx.TraceID().String()
			extractedSpanID = spanCtx.SpanID().String()
		}
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := bus.Publish(ctxWithTrace, "aerial.events.tracing", "trace-msg-1", []byte("traced")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	wg.Wait()

	if extractedTraceID != traceIDHex {
		t.Errorf("extracted trace ID = %q, want %q", extractedTraceID, traceIDHex)
	}
	if extractedSpanID != spanIDHex {
		t.Errorf("extracted span ID = %q, want %q", extractedSpanID, spanIDHex)
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

	_, err := bus.ConsumeQueue(context.Background(), "aerial.coalesced.device.1", "device-workers", func(ctx context.Context, msg Message) error {
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
	if err := bus.Publish(context.Background(), "aerial.coalesced.device.1", "state-v1", []byte("v1")); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	<-msg1Received

	// Publish message 2 to the exact same subject to supersede message 1
	if err := bus.Publish(context.Background(), "aerial.coalesced.device.1", "state-v2", []byte("v2")); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
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

func TestMemoryBus_DeduplicationWindow(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var deliveredCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := bus.Subscribe(context.Background(), "aerial.events.dedup", "dedup-sub", func(ctx context.Context, msg Message) error {
		deliveredCount.Add(1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// First publish
	if err := bus.Publish(context.Background(), "aerial.events.dedup", "dup-msg-id-1", []byte("payload")); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	wg.Wait()

	// Second publish with same MsgID within 30m window
	if err := bus.Publish(context.Background(), "aerial.events.dedup", "dup-msg-id-1", []byte("payload")); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	if count := int(deliveredCount.Load()); count != 1 {
		t.Errorf("delivered count = %d, want 1 (should be deduplicated)", count)
	}
}

func TestEnsureCoreStreams(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	if err := EnsureCoreStreams(context.Background(), bus); err != nil {
		t.Fatalf("EnsureCoreStreams failed: %v", err)
	}

	if err := EnsureCoreStreams(context.Background(), nil); err == nil {
		t.Error("EnsureCoreStreams with nil bus expected error, got nil")
	}
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

func TestMemoryBus_SubscribeEmptyDurable(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	_, err := bus.Subscribe(context.Background(), "aerial.events.nodur", "", func(ctx context.Context, msg Message) error {
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "aerial.events.nodur", "m-nodur", []byte("ok")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	wg.Wait()
}

func TestSubscription_Unsubscribe(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var deliveredCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)

	sub, err := bus.Subscribe(context.Background(), "aerial.events.unsub", "unsub-durable", func(ctx context.Context, msg Message) error {
		deliveredCount.Add(1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "aerial.events.unsub", "msg-1", []byte("a")); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	wg.Wait()

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "aerial.events.unsub", "msg-2", []byte("b")); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	if count := int(deliveredCount.Load()); count != 1 {
		t.Errorf("delivered count = %d, want 1 (second publish after unsub should be skipped)", count)
	}
}

func TestMessage_TermAndNak(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var deliveredCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)

	_, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.term", "term-queue", func(ctx context.Context, msg Message) error {
		count := deliveredCount.Add(1)
		wg.Done()
		if count == 1 {
			return msg.NakWithDelay(0)
		}
		return msg.Term()
	})
	if err != nil {
		t.Fatalf("ConsumeQueue failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "aerial.jobs.term", "job-term-1", []byte("payload")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	wg.Wait()

	if count := int(deliveredCount.Load()); count != 2 {
		t.Errorf("delivered count = %d, want 2", count)
	}
}

func TestMemoryMessage_SnoozeWithDelay(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	var secondMsgMsgID, secondMsgSnoozeSeq string
	var mu sync.Mutex

	_, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.delay", "delay-workers", func(ctx context.Context, msg Message) error {
		defer wg.Done()
		if msg.Header().Get(HeaderSnoozeSeq) == "" {
			return msg.Snooze(10 * time.Millisecond)
		}
		mu.Lock()
		secondMsgMsgID = msg.Header().Get(HeaderMsgID)
		secondMsgSnoozeSeq = msg.Header().Get(HeaderSnoozeSeq)
		mu.Unlock()
		return msg.Ack()
	})
	if err != nil {
		t.Fatalf("ConsumeQueue failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "aerial.jobs.delay", "job-delay-1", []byte("data")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if secondMsgMsgID != "job-delay-1:snooze:1" {
		t.Errorf("second message msgID = %q, want %q", secondMsgMsgID, "job-delay-1:snooze:1")
	}
	if secondMsgSnoozeSeq != "1" {
		t.Errorf("second message snoozeSeq = %q, want %q", secondMsgSnoozeSeq, "1")
	}
}

func TestMemoryBus_HandlerReturnsError(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var attempts atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)

	_, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.fail", "fail-queue", func(ctx context.Context, msg Message) error {
		wg.Done()
		if attempts.Add(1) == 1 {
			return errors.New("transient failure")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ConsumeQueue failed: %v", err)
	}

	if err := bus.Publish(context.Background(), "aerial.jobs.fail", "fail-1", []byte("data")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	wg.Wait()
}

func TestJetStreamBus_SnoozeWithDelay(t *testing.T) {
	t.Parallel()

	dialer := &mockNatsDialer{}
	nc, err := nats.Connect("nats://127.0.0.1:4222",
		nats.SetCustomDialer(dialer),
		nats.Timeout(1*time.Second),
		nats.NoReconnect(),
		nats.MaxReconnects(0),
	)
	if err != nil {
		t.Fatalf("nats.Connect failed: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("aerial.jobs.delaytest")
	if err != nil {
		t.Fatalf("nc.SubscribeSync failed: %v", err)
	}

	published := make(chan *nats.Msg, 1)
	mjs := &mockJetStream{
		onPublish: func(msg *nats.Msg) {
			select {
			case published <- msg:
			default:
			}
		},
	}
	bus := newJetStreamBusWithContext(nc, mjs)
	msg := &jsMessage{
		raw: &nats.Msg{
			Subject: "aerial.jobs.delaytest",
			Reply:   "test.reply.inbox",
			Sub:     sub,
			Header:  nats.Header{HeaderMsgID: []string{"msg-1"}, HeaderSnoozeSeq: []string{"4"}},
		},
		bus: bus,
	}
	if err := msg.Snooze(10 * time.Millisecond); err != nil {
		t.Fatalf("Snooze with delay failed: %v", err)
	}

	select {
	case continuation := <-published:
		if got := continuation.Header.Get(HeaderMsgID); got != "msg-1:snooze:5" {
			t.Errorf("expected rotated MsgID msg-1:snooze:5, got: %s", got)
		}
		if got := continuation.Header.Get(HeaderSnoozeSeq); got != "5" {
			t.Errorf("expected SnoozeSeq 5, got: %s", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for background snooze continuation publish")
	}
}

func TestMemoryBus_UnsubscribeRetainsOthersAndQueueRoundRobin(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var count1, count2 int64
	var wg sync.WaitGroup
	wg.Add(2)
	var wgRemaining sync.WaitGroup

	sub1, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.rr", "rr-queue", func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&count1, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("ConsumeQueue 1 failed: %v", err)
	}

	_, err = bus.ConsumeQueue(context.Background(), "aerial.jobs.rr", "rr-queue", func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&count2, 1)
		if atomic.LoadInt64(&count2) <= 1 {
			wg.Done()
		} else {
			wgRemaining.Done()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ConsumeQueue 2 failed: %v", err)
	}

	// Publish two messages to trigger round robin on queue
	if err := bus.Publish(context.Background(), "aerial.jobs.rr", "m1", []byte("1")); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	if err := bus.Publish(context.Background(), "aerial.jobs.rr", "m2", []byte("2")); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}
	wg.Wait()

	if c1 := atomic.LoadInt64(&count1); c1 != 1 {
		t.Errorf("count1 = %d, want 1", c1)
	}
	if c2 := atomic.LoadInt64(&count2); c2 != 1 {
		t.Errorf("count2 = %d, want 1", c2)
	}

	// Unsubscribe one to exercise removeSubscription with retained subscribers
	if err := sub1.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	wgRemaining.Add(1)
	if err := bus.Publish(context.Background(), "aerial.jobs.rr", "m3", []byte("3")); err != nil {
		t.Fatalf("Publish 3 failed: %v", err)
	}
	wgRemaining.Wait()

	if c2 := atomic.LoadInt64(&count2); c2 != 2 {
		t.Errorf("count2 after sub1 unsub = %d, want 2", c2)
	}

	// MemMessage snooze with empty initial msgID to cover msgID == "" branch
	memMsgNoID := &memMessage{
		bus:     bus,
		subject: "aerial.jobs.noid",
		data:    []byte("noid"),
	}
	var wgSnooze sync.WaitGroup
	wgSnooze.Add(1)
	_, _ = bus.Subscribe(context.Background(), "aerial.jobs.noid", "snooze-durable", func(ctx context.Context, msg Message) error {
		wgSnooze.Done()
		return nil
	})
	if err := memMsgNoID.Snooze(0); err != nil {
		t.Errorf("Snooze with empty msgID failed: %v", err)
	}
	wgSnooze.Wait()
}

func TestMemoryBus_SameDurableRoundRobin(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	var handled1, handled2 int64
	var wg sync.WaitGroup
	wg.Add(2)

	_, _ = bus.Subscribe(context.Background(), "aerial.events.shared", "shared-durable", func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&handled1, 1)
		wg.Done()
		return nil
	})
	_, _ = bus.Subscribe(context.Background(), "aerial.events.shared", "shared-durable", func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&handled2, 1)
		wg.Done()
		return nil
	})

	if err := bus.Publish(context.Background(), "aerial.events.shared", "m1", []byte("1")); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	if err := bus.Publish(context.Background(), "aerial.events.shared", "m2", []byte("2")); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}
	wg.Wait()

	if total := atomic.LoadInt64(&handled1) + atomic.LoadInt64(&handled2); total != 2 {
		t.Errorf("total handled = %d, want 2", total)
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

	// cloneHeader(nil)
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

func TestMessage_AlreadySettled(t *testing.T) {
	t.Parallel()

	bus := NewMemoryBus()
	defer bus.Close()

	msg := &memMessage{
		bus:     bus,
		subject: "aerial.jobs.settled",
		data:    []byte("d"),
		settled: true,
	}

	if err := msg.Ack(); err != nil {
		t.Errorf("Ack on settled should return nil, got: %v", err)
	}
	if err := msg.NakWithDelay(0); err != nil {
		t.Errorf("NakWithDelay on settled should return nil, got: %v", err)
	}
	if err := msg.Term(); err != nil {
		t.Errorf("Term on settled should return nil, got: %v", err)
	}
	if err := msg.Snooze(0); err != nil {
		t.Errorf("Snooze on settled should return nil, got: %v", err)
	}
	if err := msg.InProgress(); err != nil {
		t.Errorf("InProgress on settled should return nil, got: %v", err)
	}
	if h := msg.Header(); h == nil {
		t.Error("Header() should return non-nil even if uninitialized")
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

	// NewJetStreamBusFromConn(nil)
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
	if err := bus.Close(); err != nil {
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

func (d *mockNatsDialer) Dial(network, address string) (net.Conn, error) {
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

type mockJetStream struct {
	nats.JetStreamContext

	mu              sync.Mutex
	publishErr      error
	subscribeErr    error
	streamInfoErr   error
	addStreamErr    error
	updateStreamErr error
	existingStreams map[string]*nats.StreamInfo
	publishedMsgs   []*nats.Msg
	onPublish       func(msg *nats.Msg)
}

func (m *mockJetStream) PublishMsg(msg *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	m.publishedMsgs = append(m.publishedMsgs, msg)
	if m.onPublish != nil {
		m.onPublish(msg)
	}
	return &nats.PubAck{Stream: "TEST", Sequence: uint64(len(m.publishedMsgs))}, nil
}

func (m *mockJetStream) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	if cb != nil {
		cb(&nats.Msg{Subject: subj, Data: []byte("test")})
	}
	return &nats.Subscription{}, nil
}

func (m *mockJetStream) QueueSubscribe(subj, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	if cb != nil {
		cb(&nats.Msg{Subject: subj, Data: []byte("test")})
	}
	return &nats.Subscription{}, nil
}

func (m *mockJetStream) StreamInfo(stream string, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.streamInfoErr != nil {
		return nil, m.streamInfoErr
	}
	if info, ok := m.existingStreams[stream]; ok {
		return info, nil
	}
	return nil, nats.ErrStreamNotFound
}

func (m *mockJetStream) AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addStreamErr != nil {
		return nil, m.addStreamErr
	}
	if m.existingStreams == nil {
		m.existingStreams = make(map[string]*nats.StreamInfo)
	}
	info := &nats.StreamInfo{Config: *cfg}
	m.existingStreams[cfg.Name] = info
	return info, nil
}

func (m *mockJetStream) UpdateStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateStreamErr != nil {
		return nil, m.updateStreamErr
	}
	if m.existingStreams == nil {
		m.existingStreams = make(map[string]*nats.StreamInfo)
	}
	info := &nats.StreamInfo{Config: *cfg}
	m.existingStreams[cfg.Name] = info
	return info, nil
}

func (m *mockJetStream) setPublishErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishErr = err
}

func (m *mockJetStream) setSubscribeErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeErr = err
}

func (m *mockJetStream) setAddStreamErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addStreamErr = err
}

func (m *mockJetStream) setStreamInfoErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamInfoErr = err
}

func (m *mockJetStream) publishedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.publishedMsgs)
}

func (m *mockJetStream) streamCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.existingStreams)
}

func (m *mockJetStream) resetStreams() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.existingStreams = make(map[string]*nats.StreamInfo)
}

func TestJetStreamBus_StreamsAndPublishSubscribe(t *testing.T) {
	t.Parallel()

	mjs := &mockJetStream{
		existingStreams: make(map[string]*nats.StreamInfo),
	}
	bus := newJetStreamBusWithContext(nil, mjs)

	// 1. EnsureCoreStreams - AddStream path
	if err := bus.EnsureCoreStreams(context.Background()); err != nil {
		t.Fatalf("EnsureCoreStreams (add) failed: %v", err)
	}
	if mjs.streamCount() != 3 {
		t.Errorf("expected 3 streams created, got %d", mjs.streamCount())
	}

	// 2. EnsureCoreStreams - UpdateStream path
	if err := bus.EnsureCoreStreams(context.Background()); err != nil {
		t.Fatalf("EnsureCoreStreams (update) failed: %v", err)
	}

	// 3. Publish and PublishWithHeader
	if err := bus.Publish(context.Background(), "aerial.jobs.test", "job-1", "string-payload"); err != nil {
		t.Fatalf("Publish string failed: %v", err)
	}
	if err := bus.Publish(context.Background(), "aerial.jobs.test", "job-2", []byte("byte-payload")); err != nil {
		t.Fatalf("Publish bytes failed: %v", err)
	}
	h := nats.Header{}
	h.Set("Custom-Key", "Custom-Val")
	if err := bus.PublishWithHeader(context.Background(), "aerial.jobs.test", "job-3", map[string]string{"foo": "bar"}, h); err != nil {
		t.Fatalf("PublishWithHeader failed: %v", err)
	}
	if mjs.publishedCount() != 3 {
		t.Errorf("expected 3 published msgs, got %d", mjs.publishedCount())
	}

	// 4. Subscribe and QueueSubscribe
	sub, err := bus.Subscribe(context.Background(), "aerial.events.>", "sub-durable", func(ctx context.Context, msg Message) error { return nil })
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	_ = sub.Unsubscribe()

	qsub, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.>", "q1", func(ctx context.Context, msg Message) error { return nil })
	if err != nil {
		t.Fatalf("ConsumeQueue failed: %v", err)
	}
	_ = qsub.Unsubscribe()

	// Subscribe and ConsumeQueue with empty durable/queue
	if _, err := bus.Subscribe(context.Background(), "aerial.events.>", "", func(ctx context.Context, msg Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe with empty durable failed: %v", err)
	}
	if _, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.>", "", func(ctx context.Context, msg Message) error { return nil }); err != nil {
		t.Fatalf("ConsumeQueue with empty queue failed: %v", err)
	}
	nilSub := &jsSubscription{}
	if err := nilSub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe on nil sub should return nil, got: %v", err)
	}

	// 5. Error paths on JetStreamBus
	mjs.setPublishErr(errors.New("pub failed"))
	if err := bus.Publish(context.Background(), "aerial.jobs.err", "id", "data"); err == nil {
		t.Error("expected publish error, got nil")
	}
	if err := bus.Publish(context.Background(), "aerial.jobs.err", "id", make(chan int)); err == nil {
		t.Error("expected payload marshal error, got nil")
	}
	mjs.setPublishErr(nil)

	mjs.setSubscribeErr(errors.New("sub failed"))
	if _, err := bus.Subscribe(context.Background(), "aerial.events.>", "dur", func(ctx context.Context, msg Message) error { return nil }); err == nil {
		t.Error("expected subscribe error, got nil")
	}
	if _, err := bus.ConsumeQueue(context.Background(), "aerial.jobs.>", "q", func(ctx context.Context, msg Message) error { return nil }); err == nil {
		t.Error("expected queue subscribe error, got nil")
	}
	mjs.setSubscribeErr(nil)

	mjs.setAddStreamErr(errors.New("add stream failed"))
	mjs.resetStreams() // force AddStream path
	if err := bus.EnsureCoreStreams(context.Background()); err == nil {
		t.Error("expected add stream error, got nil")
	}
	mjs.setAddStreamErr(nil)

	mjs.setStreamInfoErr(errors.New("stream info failed"))
	if err := bus.EnsureCoreStreams(context.Background()); err == nil {
		t.Error("expected stream info error, got nil")
	}
	mjs.setStreamInfoErr(nil)
}

func TestJetStreamBus_DispatchAndMessageSettlement(t *testing.T) {
	t.Parallel()

	mjs := &mockJetStream{
		existingStreams: make(map[string]*nats.StreamInfo),
	}
	bus := newJetStreamBusWithContext(nil, mjs)
	bus.heartbeatInterval = 5 * time.Millisecond

	// 1. Dispatch execution paths
	// Metadata extraction from JetStream ACK reply format:
	// $JS.ACK.<stream>.<consumer>.<numDelivered>.<streamSeq>.<consumerSeq>.<timestamp>.<numPending>
	metaReply := "$JS.ACK.AERIAL_EVENTS.durable.4.1.1.1600000000.0"
	dummySub := &nats.Subscription{}
	bus.dispatch(&nats.Msg{
		Subject: "aerial.events.meta",
		Reply:   metaReply,
		Sub:     dummySub,
		Data:    []byte("with-meta"),
	}, func(ctx context.Context, msg Message) error {
		if msg.NumDelivered() != 4 {
			t.Errorf("expected NumDelivered 4, got %d", msg.NumDelivered())
		}
		return nil
	})

	// Traceparent header extraction and background heartbeat ticker
	var extractedTraceID string
	hTrace := nats.Header{}
	injectedCtx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	})
	propagation.TraceContext{}.Inject(injectedCtx, propagation.HeaderCarrier(http.Header(hTrace)))
	bus.dispatch(&nats.Msg{
		Subject: "aerial.events.head",
		Data:    []byte("h"),
		Header:  hTrace,
	}, func(ctx context.Context, msg Message) error {
		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.IsValid() {
			extractedTraceID = spanCtx.TraceID().String()
		}
		return nil
	})
	if extractedTraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("expected extracted traceparent traceID 4bf92f3577b34da6a3ce929d0e0e4736, got %s", extractedTraceID)
	}

	// Normal success auto-ack
	handledSuccess := false
	bus.dispatch(&nats.Msg{Subject: "aerial.events.ok", Data: []byte("ok")}, func(ctx context.Context, msg Message) error {
		handledSuccess = true
		if msg.Subject() != "aerial.events.ok" {
			t.Errorf("unexpected subject %q", msg.Subject())
		}
		if string(msg.Data()) != "ok" {
			t.Errorf("unexpected data %q", string(msg.Data()))
		}
		return nil
	})
	if !handledSuccess {
		t.Error("expected handledSuccess to be true")
	}

	// Error path auto-nak
	handledError := false
	bus.dispatch(&nats.Msg{Subject: "aerial.events.err", Data: []byte("err")}, func(ctx context.Context, msg Message) error {
		handledError = true
		return errors.New("handler failed")
	})
	if !handledError {
		t.Error("expected handledError to be true")
	}

	// Handler exercising InProgress and Term settling
	handledTerm := false
	bus.dispatch(&nats.Msg{Subject: "aerial.events.term", Data: []byte("term")}, func(ctx context.Context, msg Message) error {
		handledTerm = true
		_ = msg.InProgress()
		return msg.Term()
	})
	if !handledTerm {
		t.Error("expected handledTerm to be true")
	}

	// 2. jsMessage methods and error/settled branches
	jsMsg := &jsMessage{
		raw:     &nats.Msg{Subject: "aerial.jobs.jsmsg", Data: []byte("raw"), Header: nats.Header{}},
		bus:     bus,
		settled: true,
	}
	if err := jsMsg.Ack(); err != nil {
		t.Errorf("Ack on settled jsMsg should return nil, got: %v", err)
	}
	if err := jsMsg.NakWithDelay(0); err != nil {
		t.Errorf("NakWithDelay on settled jsMsg should return nil, got: %v", err)
	}
	if err := jsMsg.Term(); err != nil {
		t.Errorf("Term on settled jsMsg should return nil, got: %v", err)
	}
	if err := jsMsg.Snooze(0); err != nil {
		t.Errorf("Snooze on settled jsMsg should return nil, got: %v", err)
	}
	if err := jsMsg.InProgress(); err != nil {
		t.Errorf("InProgress on settled jsMsg should return nil, got: %v", err)
	}
	if jsMsg.Header() == nil {
		t.Error("expected non-nil header")
	}

	// 3. Unsettled jsMsg snooze without explicit msgID
	unsettledJsMsg4 := &jsMessage{
		raw: &nats.Msg{Subject: "aerial.jobs.u4", Data: []byte("u4")},
		bus: bus,
	}
	if err := unsettledJsMsg4.Snooze(0); !errors.Is(err, nats.ErrMsgNotBound) {
		t.Errorf("expected ErrMsgNotBound on Snooze, got: %v", err)
	}

	// 4. NumDelivered resolution
	jsMsgWithMeta := &jsMessage{
		raw:  &nats.Msg{},
		meta: &nats.MsgMetadata{NumDelivered: 42},
	}
	if d := jsMsgWithMeta.NumDelivered(); d != 42 {
		t.Errorf("expected NumDelivered 42, got %d", d)
	}
	jsMsgNoMeta := &jsMessage{raw: &nats.Msg{}}
	if d := jsMsgNoMeta.NumDelivered(); d != 1 {
		t.Errorf("expected NumDelivered 1, got %d", d)
	}
	jsMsgNoHeader := &jsMessage{raw: &nats.Msg{}}
	if h := jsMsgNoHeader.Header(); h == nil {
		t.Error("expected non-nil header from raw msg without header")
	}

	// NumDelivered parsed directly from raw reply
	replyMetaMsg := &nats.Msg{
		Reply: "$JS.ACK.AERIAL_JOBS.test-cons.3.100.200.1600000000.5",
		Sub:   dummySub,
	}
	jsMsgReplyMeta := &jsMessage{raw: replyMetaMsg}
	if d := jsMsgReplyMeta.NumDelivered(); d != 3 {
		t.Errorf("expected NumDelivered 3 from reply metadata, got %d", d)
	}

	// 5. Unsettled jsMsg snooze failure when continuation fails
	mjs.setPublishErr(errors.New("continuation pub failed"))
	unsettledJsMsg5 := &jsMessage{
		raw: &nats.Msg{Subject: "aerial.jobs.u5", Data: []byte("u5")},
		bus: bus,
	}
	if err := unsettledJsMsg5.Snooze(0); err == nil {
		t.Error("expected snooze error when continuation fails, got nil")
	}
	mjs.setPublishErr(nil)

	// 6. Snooze with existing snooze sequence and fallback non-HeaderPublisher bus
	type fallbackBus struct {
		Bus
	}
	fb := fallbackBus{bus}
	unsettledExisting := &jsMessage{
		raw: &nats.Msg{
			Subject: "aerial.jobs.existing",
			Data:    []byte("ex"),
			Header:  nats.Header{HeaderMsgID: []string{"task-1:snooze:2"}},
		},
		bus: fb,
	}
	if err := unsettledExisting.Snooze(0); !errors.Is(err, nats.ErrMsgNotBound) {
		t.Errorf("expected ErrMsgNotBound on unbound raw msg snooze, got: %v", err)
	}

	// 7. Bus Close
	busNilNC := &JetStreamBus{}
	if err := busNilNC.Close(); err != nil {
		t.Errorf("Close with nil nc failed: %v", err)
	}
}
