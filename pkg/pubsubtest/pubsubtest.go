// Package pubsubtest holds declarative fakes for pkg/pubsub: a recording
// Message, a fault-injecting Bus wrapper and a ConsumerStatsProvider.
package pubsubtest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// Message is a pubsub.Message that records every settlement call. Delivered
// is returned verbatim by NumDelivered; set it to 1 for a first delivery.
// A non-nil *Err field is returned by the matching method after the call is
// recorded.
type Message struct {
	Subj      string
	Payload   []byte
	Hdr       nats.Header
	Delivered uint64

	AckErr, NakErr, TermErr, SnoozeErr, InProgressErr error

	mu    sync.Mutex
	calls []string
}

var _ pubsub.Message = (*Message)(nil)

// Data returns Payload.
func (m *Message) Data() []byte { return m.Payload }

// Subject returns Subj.
func (m *Message) Subject() string { return m.Subj }

// Header returns Hdr.
func (m *Message) Header() nats.Header { return m.Hdr }

// NumDelivered returns Delivered.
func (m *Message) NumDelivered() uint64 { return m.Delivered }

// Ack records "ack".
func (m *Message) Ack() error { return m.record("ack", m.AckErr) }

// NakWithDelay records "nak <delay>".
func (m *Message) NakWithDelay(delay time.Duration) error {
	return m.record(fmt.Sprintf("nak %s", delay), m.NakErr)
}

// Term records "term".
func (m *Message) Term() error { return m.record("term", m.TermErr) }

// Snooze records "snooze <delay>".
func (m *Message) Snooze(delay time.Duration) error {
	return m.record(fmt.Sprintf("snooze %s", delay), m.SnoozeErr)
}

// InProgress records "in_progress".
func (m *Message) InProgress() error { return m.record("in_progress", m.InProgressErr) }

func (m *Message) record(call string, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, call)
	return err
}

// Calls returns a copy of the recorded settlement calls in order, e.g.
// ["in_progress", "nak 5s"].
func (m *Message) Calls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

// Acked reports whether Ack was called.
func (m *Message) Acked() bool { return m.has("ack") }

// Termed reports whether Term was called.
func (m *Message) Termed() bool { return m.has("term") }

// NakDelay returns the delay of the last NakWithDelay call, and whether one happened.
func (m *Message) NakDelay() (time.Duration, bool) { return m.lastDelay("nak ") }

// SnoozeDelay returns the delay of the last Snooze call, and whether one happened.
func (m *Message) SnoozeDelay() (time.Duration, bool) { return m.lastDelay("snooze ") }

// InProgressCount returns how many times InProgress was called.
func (m *Message) InProgressCount() int {
	n := 0
	for _, c := range m.Calls() {
		if c == "in_progress" {
			n++
		}
	}
	return n
}

func (m *Message) has(call string) bool { return slices.Contains(m.Calls(), call) }

func (m *Message) lastDelay(prefix string) (time.Duration, bool) {
	for _, c := range slices.Backward(m.Calls()) {
		if rest, ok := strings.CutPrefix(c, prefix); ok {
			d, err := time.ParseDuration(rest)
			return d, err == nil
		}
	}
	return 0, false
}

// FaultBus wraps a pubsub.Bus (normally pubsub.NewMemoryBus()). A non-nil
// error field is returned by its method without calling the embedded Bus;
// otherwise the embedded Bus handles the call. When Handled is non-nil, each
// subscribed handler's return value is sent to it after the handler returns.
type FaultBus struct {
	pubsub.Bus

	EnsureErr, SubscribeErr, ConsumeErr, PublishErr, CloseErr error
	Handled                                                   chan error
}

// Publish returns PublishErr or delegates.
func (f *FaultBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	if f.PublishErr != nil {
		return f.PublishErr
	}
	return f.Bus.Publish(ctx, subject, msgID, data)
}

// Subscribe returns SubscribeErr or delegates with a reporting handler.
func (f *FaultBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if f.SubscribeErr != nil {
		return nil, f.SubscribeErr
	}
	return f.Bus.Subscribe(ctx, subject, durable, f.wrap(handler))
}

// ConsumeQueue returns ConsumeErr or delegates with a reporting handler.
func (f *FaultBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if f.ConsumeErr != nil {
		return nil, f.ConsumeErr
	}
	return f.Bus.ConsumeQueue(ctx, subject, queue, f.wrap(handler))
}

// EnsureCoreStreams returns EnsureErr or delegates.
func (f *FaultBus) EnsureCoreStreams(ctx context.Context) error {
	if f.EnsureErr != nil {
		return f.EnsureErr
	}
	return f.Bus.EnsureCoreStreams(ctx)
}

// Close returns CloseErr or delegates.
func (f *FaultBus) Close() error {
	if f.CloseErr != nil {
		return f.CloseErr
	}
	return f.Bus.Close()
}

func (f *FaultBus) wrap(handler pubsub.Handler) pubsub.Handler {
	return func(ctx context.Context, msg pubsub.Message) error {
		err := handler(ctx, msg)
		if f.Handled != nil {
			f.Handled <- err
		}
		return err
	}
}

// ConsumerStats is a pubsub.ConsumerStatsProvider keyed by consumer name:
// it returns Err when set, otherwise Pending[consumer] and Inflight[consumer]
// (0 when absent).
type ConsumerStats struct {
	Pending, Inflight map[string]int64
	Err               error
}

var _ pubsub.ConsumerStatsProvider = ConsumerStats{}

// ConsumerStats implements pubsub.ConsumerStatsProvider.
func (s ConsumerStats) ConsumerStats(_ context.Context, _, consumer string) (int64, int64, error) {
	if s.Err != nil {
		return 0, 0, s.Err
	}
	return s.Pending[consumer], s.Inflight[consumer], nil
}
