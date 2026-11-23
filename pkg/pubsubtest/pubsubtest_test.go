package pubsubtest_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
)

var errBoom = errors.New("boom")

type settle struct {
	op    string
	delay time.Duration
}

func TestMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		msg            *pubsubtest.Message
		ops            []settle
		wantCalls      []string
		wantErrs       []string
		wantAcked      bool
		wantTermed     bool
		wantNak        time.Duration
		wantNakOK      bool
		wantSnooze     time.Duration
		wantSnoozeOK   bool
		wantInProgress int
	}{
		{name: "no calls", msg: &pubsubtest.Message{}},
		{name: "ack", msg: &pubsubtest.Message{}, ops: []settle{{op: "ack"}}, wantCalls: []string{"ack"}, wantErrs: []string{""}, wantAcked: true},
		{
			name: "progress then last nak wins", msg: &pubsubtest.Message{}, ops: []settle{{op: "in_progress"}, {op: "nak", delay: time.Second}, {op: "in_progress"}, {op: "nak", delay: 5 * time.Second}},
			wantCalls: []string{"in_progress", "nak 1s", "in_progress", "nak 5s"}, wantErrs: []string{"", "", "", ""}, wantNak: 5 * time.Second, wantNakOK: true, wantInProgress: 2,
		},
		{name: "term and snooze", msg: &pubsubtest.Message{}, ops: []settle{{op: "snooze", delay: time.Minute}, {op: "term"}}, wantCalls: []string{"snooze 1m0s", "term"}, wantErrs: []string{"", ""}, wantTermed: true, wantSnooze: time.Minute, wantSnoozeOK: true},
		{
			name: "errors returned after recording", msg: &pubsubtest.Message{AckErr: errBoom, NakErr: errBoom, TermErr: errBoom, SnoozeErr: errBoom, InProgressErr: errBoom},
			ops: []settle{{op: "ack"}, {op: "nak"}, {op: "term"}, {op: "snooze"}, {op: "in_progress"}}, wantCalls: []string{"ack", "nak 0s", "term", "snooze 0s", "in_progress"},
			wantErrs: []string{"boom", "boom", "boom", "boom", "boom"}, wantAcked: true, wantTermed: true, wantNakOK: true, wantSnoozeOK: true, wantInProgress: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var errs []string
			for _, op := range tc.ops {
				errs = append(errs, errText(applySettle(tc.msg, op)))
			}
			if !reflect.DeepEqual(errs, tc.wantErrs) {
				t.Errorf("errors = %q, want %q", errs, tc.wantErrs)
			}
			if got := tc.msg.Calls(); !reflect.DeepEqual(got, tc.wantCalls) {
				t.Errorf("Calls() = %q, want %q", got, tc.wantCalls)
			}
			nak, nakOK := tc.msg.NakDelay()
			snooze, snoozeOK := tc.msg.SnoozeDelay()
			got := []any{tc.msg.Acked(), tc.msg.Termed(), nak, nakOK, snooze, snoozeOK, tc.msg.InProgressCount()}
			want := []any{tc.wantAcked, tc.wantTermed, tc.wantNak, tc.wantNakOK, tc.wantSnooze, tc.wantSnoozeOK, tc.wantInProgress}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("accessors (acked, termed, nak, nakOK, snooze, snoozeOK, inProgress) = %v, want %v", got, want)
			}
		})
	}
}

func applySettle(m *pubsubtest.Message, s settle) error {
	switch s.op {
	case "ack":
		return m.Ack()
	case "nak":
		return m.NakWithDelay(s.delay)
	case "term":
		return m.Term()
	case "snooze":
		return m.Snooze(s.delay)
	default:
		return m.InProgress()
	}
}

func TestMessageFields(t *testing.T) {
	t.Parallel()
	hdr := nats.Header{"K": []string{"v"}}
	var msg pubsub.Message = &pubsubtest.Message{Subj: "aerial.events.x", Payload: []byte("p"), Hdr: hdr, Delivered: 3}
	got := []any{msg.Subject(), string(msg.Data()), msg.Header().Get("K"), msg.NumDelivered()}
	want := []any{"aerial.events.x", "p", "v", uint64(3)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}
}

func TestFaultBus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		bus           pubsubtest.FaultBus
		handlerErr    error
		wantEnsure    string
		wantSubscribe string
		wantConsume   string
		wantPublish   string
		wantClose     string
		wantHandled   []string
	}{
		{name: "memory bus delegates", wantHandled: []string{"", ""}},
		{name: "handler errors reported", handlerErr: errBoom, wantHandled: []string{"boom", "boom"}},
		{
			name: "every fault returned", bus: pubsubtest.FaultBus{EnsureErr: errBoom, SubscribeErr: errBoom, ConsumeErr: errBoom, PublishErr: errBoom, CloseErr: errBoom},
			wantEnsure: "boom", wantSubscribe: "boom", wantConsume: "boom", wantPublish: "boom", wantClose: "boom",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mem := pubsub.NewMemoryBus()
			t.Cleanup(func() { _ = mem.Close() })
			bus := tc.bus
			bus.Bus = mem
			bus.Handled = make(chan error, 2)
			ctx := context.Background()
			handler := func(context.Context, pubsub.Message) error { return tc.handlerErr }
			_, subErr := bus.Subscribe(ctx, "aerial.events.a", "dur", handler)
			_, conErr := bus.ConsumeQueue(ctx, "aerial.events.b", "q", handler)
			ensureErr, pubErr := bus.EnsureCoreStreams(ctx), bus.Publish(ctx, "aerial.events.a", "m1", []byte("x"))
			if err := mem.Publish(ctx, "aerial.events.b", "m2", []byte("y")); err != nil {
				t.Fatalf("memory publish: %v", err)
			}
			got := []string{errText(ensureErr), errText(subErr), errText(conErr), errText(pubErr), errText(bus.Close())}
			want := []string{tc.wantEnsure, tc.wantSubscribe, tc.wantConsume, tc.wantPublish, tc.wantClose}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("errors (ensure, subscribe, consume, publish, close) = %q, want %q", got, want)
			}
			var handled []string
			for range tc.wantHandled {
				handled = append(handled, errText(<-bus.Handled))
			}
			if !reflect.DeepEqual(handled, tc.wantHandled) {
				t.Errorf("handled = %q, want %q", handled, tc.wantHandled)
			}
		})
	}
}

func TestFaultBusNilHandled(t *testing.T) {
	t.Parallel()
	got := make(chan string, 1)
	bus := &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus()}
	t.Cleanup(func() { _ = bus.Close() })
	if _, err := bus.Subscribe(context.Background(), "aerial.events.c", "dur", func(_ context.Context, m pubsub.Message) error {
		got <- string(m.Data())
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), "aerial.events.c", "m1", []byte("z")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if data := <-got; data != "z" {
		t.Errorf("handler data = %q, want z", data)
	}
}

func TestConsumerStats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		stats        pubsubtest.ConsumerStats
		consumer     string
		wantPending  int64
		wantInflight int64
		wantErr      string
	}{
		{name: "known consumer", stats: pubsubtest.ConsumerStats{Pending: map[string]int64{"c": 7}, Inflight: map[string]int64{"c": 3}}, consumer: "c", wantPending: 7, wantInflight: 3},
		{name: "unknown consumer is zero", stats: pubsubtest.ConsumerStats{Pending: map[string]int64{"c": 7}}, consumer: "other"},
		{name: "error wins", stats: pubsubtest.ConsumerStats{Pending: map[string]int64{"c": 7}, Err: errBoom}, consumer: "c", wantErr: "boom"},
		{name: "per-consumer error", stats: pubsubtest.ConsumerStats{Pending: map[string]int64{"c": 7}, Errs: map[string]error{"c": errBoom}}, consumer: "c", wantErr: "boom"},
		{name: "other consumer error ignored", stats: pubsubtest.ConsumerStats{Pending: map[string]int64{"c": 7}, Errs: map[string]error{"other": errBoom}}, consumer: "c", wantPending: 7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p pubsub.ConsumerStatsProvider = tc.stats
			pending, inflight, err := p.ConsumerStats(context.Background(), "STREAM", tc.consumer)
			if pending != tc.wantPending || inflight != tc.wantInflight || errText(err) != tc.wantErr {
				t.Errorf("ConsumerStats = (%d, %d, %q), want (%d, %d, %q)", pending, inflight, errText(err), tc.wantPending, tc.wantInflight, tc.wantErr)
			}
		})
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
