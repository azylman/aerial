package pubsub

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type mockJetStream struct {
	nats.JetStreamContext

	mu              sync.Mutex
	publishErr      error
	streamInfoErr   error
	addStreamErr    error
	updateStreamErr error
	consumerInfoErr error
	existingStreams map[string]*nats.StreamInfo
	// consumers holds server-side consumer state keyed by stream/name.
	consumers     map[string]*nats.ConsumerInfo
	publishedMsgs []*nats.Msg
	onPublish     func(msg *nats.Msg)
}

func consumerKey(stream, name string) string { return stream + "/" + name }

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

func (m *mockJetStream) ConsumerInfo(stream, consumer string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumerInfoErr != nil {
		return nil, m.consumerInfoErr
	}
	if info, ok := m.consumers[consumerKey(stream, consumer)]; ok {
		return info, nil
	}
	return nil, nats.ErrConsumerNotFound
}

// brokerDeadline bounds every wait on broker state in these tests. Recovery
// waits include up to two backoff steps (1s + 2s).
const brokerDeadline = 30 * time.Second

// startJetStreamServer starts an in-process JetStream server with its store in
// t.TempDir() and returns a connection to it; both close on cleanup.
func startJetStreamServer(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(brokerDeadline) {
		t.Fatal("nats server not ready")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// newServerBus returns a bus on a fresh embedded server with the core streams.
func newServerBus(t *testing.T) *JetStreamBus {
	t.Helper()
	bus, err := NewJetStreamBusFromConn(startJetStreamServer(t))
	if err != nil {
		t.Fatalf("NewJetStreamBusFromConn: %v", err)
	}
	// Detect a consumer that vanished under a busy handler within 1s.
	bus.pullHeartbeat = 500 * time.Millisecond
	if err := bus.EnsureCoreStreams(t.Context()); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}
	return bus
}

// awaitCond polls cond on a ticker until it holds or brokerDeadline passes.
func awaitCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(brokerDeadline)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		case <-ticker.C:
		}
	}
}

// awaitConsumer polls ConsumerInfo until cond holds and returns that info.
func awaitConsumer(t *testing.T, bus *JetStreamBus, stream, durable string, cond func(*nats.ConsumerInfo) bool) *nats.ConsumerInfo {
	t.Helper()
	var last *nats.ConsumerInfo
	awaitCond(t, "consumer "+stream+"/"+durable, func() bool {
		info, err := bus.js.ConsumerInfo(stream, durable)
		if err != nil {
			return false
		}
		last = info
		return cond(info)
	})
	return last
}

// handledMsg is what a test handler saw for one delivery.
type handledMsg struct {
	data      string
	delivered uint64
}

// recordingHandler sends every delivery to the returned channel. A delivery
// whose data equals blockOn waits for release first.
func recordingHandler(blockOn string, release <-chan struct{}) (Handler, <-chan handledMsg) {
	ch := make(chan handledMsg, 32)
	return func(ctx context.Context, msg Message) error {
		if string(msg.Data()) == blockOn {
			<-release
		}
		ch <- handledMsg{string(msg.Data()), msg.NumDelivered()}
		return nil
	}, ch
}

// receive returns the next n deliveries or fails after brokerDeadline.
func receive(t *testing.T, ch <-chan handledMsg, n int) []handledMsg {
	t.Helper()
	deadline := time.NewTimer(brokerDeadline)
	defer deadline.Stop()
	got := make([]handledMsg, 0, n)
	for len(got) < n {
		select {
		case m := <-ch:
			got = append(got, m)
		case <-deadline.C:
			t.Fatalf("received %v, want %d deliveries", got, n)
		}
	}
	return got
}

// assertNoMore fails if another delivery is already waiting.
func assertNoMore(t *testing.T, ch <-chan handledMsg) {
	t.Helper()
	select {
	case m := <-ch:
		t.Errorf("unexpected extra delivery %+v", m)
	default:
	}
}

func publishAll(t *testing.T, bus *JetStreamBus, subject string, data ...string) {
	t.Helper()
	for _, d := range data {
		if err := bus.Publish(t.Context(), subject, subject+"/"+d, d); err != nil {
			t.Fatalf("publish %s: %v", d, err)
		}
	}
}

func subscribe(t *testing.T, bus *JetStreamBus, subject, durable string, handler Handler) {
	t.Helper()
	sub, err := bus.Subscribe(t.Context(), subject, durable, handler)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func TestJetStreamBus_NoClientBuffering(t *testing.T) {
	t.Parallel()
	bus := newServerBus(t)
	release := make(chan struct{})
	handler, handled := recordingHandler("m1", release)
	sub, err := bus.ConsumeQueue(t.Context(), "aerial.jobs.buffer", "buffer-workers", handler)
	if err != nil {
		t.Fatalf("ConsumeQueue: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	publishAll(t, bus, "aerial.jobs.buffer", "m1", "m2", "m3")

	info := awaitConsumer(t, bus, StreamAerialJobs, "buffer-workers", func(ci *nats.ConsumerInfo) bool { return ci.NumAckPending >= 1 })
	if info.NumAckPending != 1 || info.NumPending != 2 || info.NumWaiting != 0 {
		t.Errorf("while busy: ack pending %d, pending %d, waiting %d; want 1, 2, 0", info.NumAckPending, info.NumPending, info.NumWaiting)
	}
	close(release)
	got := receive(t, handled, 3)
	awaitConsumer(t, bus, StreamAerialJobs, "buffer-workers", func(ci *nats.ConsumerInfo) bool { return ci.NumAckPending == 0 && ci.NumPending == 0 })
	assertNoMore(t, handled)
	want := []handledMsg{{"m1", 1}, {"m2", 1}, {"m3", 1}}
	if !equalHandled(got, want) {
		t.Errorf("handled %v, want %v", got, want)
	}
}

func equalHandled(got, want []handledMsg) bool {
	count := func(ms []handledMsg) map[handledMsg]int {
		c := map[handledMsg]int{}
		for _, m := range ms {
			c[m]++
		}
		return c
	}
	return maps.Equal(count(got), count(want))
}

// assertPullConsumer checks that durable on stream is a pull consumer filtering subject with
// explicit 30s acks delivering all.
func assertPullConsumer(t *testing.T, bus *JetStreamBus, stream, durable, subject string) {
	t.Helper()
	info, err := bus.js.ConsumerInfo(stream, durable)
	if err != nil {
		t.Fatalf("ConsumerInfo: %v", err)
	}
	cfg := info.Config
	if cfg.DeliverSubject != "" || cfg.DeliverGroup != "" || cfg.FilterSubject != subject ||
		cfg.AckPolicy != nats.AckExplicitPolicy || cfg.AckWait != 30*time.Second || cfg.DeliverPolicy != nats.DeliverAllPolicy {
		t.Errorf("consumer config = %+v, want pull filtering %q with explicit 30s acks delivering all", cfg, subject)
	}
}

// bindErrString returns err's text, or "" for nil.
func bindErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestJetStreamBus_SubscribeBindsDurable(t *testing.T) {
	t.Parallel()

	serverBus := newServerBus(t)
	bareBus := newJetStreamBusWithContext(nil, &mockJetStream{})
	tests := []struct {
		name       string
		bus        *JetStreamBus
		subject    string
		durable    string
		wantErr    string
		wantStream string
	}{
		{"durable subscribe creates pull consumer", serverBus, "aerial.events.haro", "haro-events", "", "AERIAL_EVENTS"},
		{"empty durable rejected", serverBus, "aerial.events.>", "", "pubsub: durable name required", ""},
		{"unresolvable stream fails", serverBus, "unknown.subject", "dur", "resolve stream for unknown.subject", ""},
		{"bus without a connection fails", bareBus, "aerial.events.haro", "dur", "jetstream handle is nil", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sub, err := tc.bus.Subscribe(t.Context(), tc.subject, tc.durable, func(context.Context, Message) error { return nil })
			if sub != nil {
				t.Cleanup(func() { _ = sub.Unsubscribe() })
			}
			if got := bindErrString(err); !strings.Contains(got, tc.wantErr) || (got == "") != (tc.wantErr == "") {
				t.Fatalf("err = %q, want containing %q", got, tc.wantErr)
			}
			if tc.wantErr == "" {
				assertPullConsumer(t, tc.bus, tc.wantStream, tc.durable, tc.subject)
			}
		})
	}
}

func TestJetStreamBus_ConsumeQueueBindsDurable(t *testing.T) {
	t.Parallel()

	bus := newServerBus(t)
	tests := []struct {
		name       string
		subject    string
		queue      string
		wantErr    string
		wantStream string
	}{
		{"consume queue creates pull consumer", "aerial.jobs.vanadis.chunk", "vanadis-chunk", "", "AERIAL_JOBS"},
		{"empty queue rejected", "aerial.jobs.>", "", "queue name is required", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sub, err := bus.ConsumeQueue(t.Context(), tc.subject, tc.queue, func(context.Context, Message) error { return nil })
			if sub != nil {
				t.Cleanup(func() { _ = sub.Unsubscribe() })
			}
			if got := bindErrString(err); !strings.Contains(got, tc.wantErr) || (got == "") != (tc.wantErr == "") {
				t.Fatalf("err = %q, want containing %q", got, tc.wantErr)
			}
			if tc.wantErr == "" {
				assertPullConsumer(t, bus, tc.wantStream, tc.queue, tc.subject)
			}
		})
	}
}

func TestJSSubscription_Unsubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sub        *jsSubscription
		wantClosed bool
	}{
		{name: "never-started subscription is a no-op", sub: &jsSubscription{}},
		{name: "repeated unsubscribe closes stop once", sub: &jsSubscription{stop: make(chan struct{})}, wantClosed: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				if err := tc.sub.Unsubscribe(); err != nil {
					t.Fatalf("Unsubscribe = %v, want nil", err)
				}
			}
			closed := false
			if tc.sub.stop != nil {
				select {
				case <-tc.sub.stop:
					closed = true
				default:
				}
			}
			if closed != tc.wantClosed {
				t.Errorf("stop closed = %v, want %v", closed, tc.wantClosed)
			}
		})
	}
}

// faultyJetStream hands out faultyStreams so tests can stage what other pods
// do between the bus's own consumer API calls on a real server.
type faultyJetStream struct {
	jetstream.JetStream

	racer     *jetstream.ConsumerConfig
	deleteErr error
}

func (f faultyJetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	s, err := f.JetStream.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return &faultyStream{Stream: s, racer: f.racer, deleteErr: f.deleteErr}, nil
}

// faultyStream creates racer just before the first CreateConsumer, as a pod
// winning the creation race would, and fails DeleteConsumer with deleteErr.
type faultyStream struct {
	jetstream.Stream

	racer     *jetstream.ConsumerConfig
	deleteErr error
}

func (f *faultyStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if f.racer != nil {
		racer := *f.racer
		f.racer = nil
		if _, err := f.Stream.CreateConsumer(ctx, racer); err != nil {
			return nil, err
		}
	}
	return f.Stream.CreateConsumer(ctx, cfg)
}

func (f *faultyStream) DeleteConsumer(ctx context.Context, name string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.Stream.DeleteConsumer(ctx, name)
}

func TestJetStreamBus_EnsureDurableConsumer(t *testing.T) {
	t.Parallel()

	const subj = "aerial.events.haro"
	tests := []struct {
		name       string
		subject    string
		durable    string
		existing   *nats.ConsumerConfig
		racer      *jetstream.ConsumerConfig
		deleteErr  error
		wantErr    string
		wantFilter string
		wantPolicy nats.DeliverPolicy
		wantStart  uint64
	}{
		{name: "missing consumer created delivering all", subject: subj, durable: "haro-events", wantFilter: subj, wantPolicy: nats.DeliverAllPolicy},
		{
			name: "compatible pull consumer bound unchanged", subject: subj, durable: "haro-events",
			existing:   &nats.ConsumerConfig{Durable: "haro-events", FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, DeliverPolicy: nats.DeliverByStartSequencePolicy, OptStartSeq: 3},
			wantFilter: subj, wantPolicy: nats.DeliverByStartSequencePolicy, wantStart: 3,
		},
		{
			name: "push consumer with deliver group recreated as pull after ack floor", subject: subj, durable: "haro-events",
			existing:   &nats.ConsumerConfig{Durable: "haro-events", FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, DeliverSubject: "_DELIVER.AERIAL_EVENTS.haro-events", DeliverGroup: "haro-events", DeliverPolicy: nats.DeliverByStartSequencePolicy, OptStartSeq: 4},
			wantFilter: subj, wantPolicy: nats.DeliverByStartSequencePolicy, wantStart: 4,
		},
		{
			name: "legacy push consumer without group recreated as pull after ack floor", subject: subj, durable: "haro-events",
			existing:   &nats.ConsumerConfig{Durable: "haro-events", FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, DeliverSubject: "_INBOX.legacy", DeliverPolicy: nats.DeliverByStartSequencePolicy, OptStartSeq: 2},
			wantFilter: subj, wantPolicy: nats.DeliverByStartSequencePolicy, wantStart: 2,
		},
		{
			name: "pull consumer with another filter recreated after ack floor", subject: subj, durable: "haro-events",
			existing:   &nats.ConsumerConfig{Durable: "haro-events", FilterSubject: "aerial.events.other", AckPolicy: nats.AckExplicitPolicy, DeliverPolicy: nats.DeliverByStartSequencePolicy, OptStartSeq: 5},
			wantFilter: subj, wantPolicy: nats.DeliverByStartSequencePolicy, wantStart: 5,
		},
		{
			name: "create race with compatible winner binds the winner", subject: subj, durable: "haro-events",
			racer:      &jetstream.ConsumerConfig{Durable: "haro-events", FilterSubject: subj, AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: 2},
			wantFilter: subj, wantPolicy: nats.DeliverByStartSequencePolicy, wantStart: 2,
		},
		{
			name: "create race with incompatible winner fails and leaves it", subject: subj, durable: "haro-events",
			racer:      &jetstream.ConsumerConfig{Durable: "haro-events", FilterSubject: "aerial.events.other", AckPolicy: jetstream.AckExplicitPolicy},
			wantErr:    "concurrently created with an incompatible config",
			wantFilter: "aerial.events.other", wantPolicy: nats.DeliverAllPolicy,
		},
		{
			name: "failed delete of an incompatible consumer leaves it", subject: subj, durable: "haro-events",
			existing:   &nats.ConsumerConfig{Durable: "haro-events", FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, DeliverSubject: "_INBOX.legacy"},
			deleteErr:  errors.New("delete refused"),
			wantErr:    "delete incompatible consumer AERIAL_EVENTS/haro-events: delete refused",
			wantFilter: subj, wantPolicy: nats.DeliverAllPolicy,
		},
		{name: "stream lookup failure", subject: "unknown.subject", durable: "haro-events", wantErr: "resolve stream for unknown.subject"},
		{name: "invalid durable name fails the consumer read", subject: subj, durable: "bad.name", wantErr: "consumer info AERIAL_EVENTS/bad.name"},
		{
			name: "work queue overlap fails creation", subject: "aerial.jobs.overlap", durable: "overlap-workers",
			existing: &nats.ConsumerConfig{Durable: "other-workers", FilterSubject: "aerial.jobs.>", AckPolicy: nats.AckExplicitPolicy},
			wantErr:  "add consumer AERIAL_JOBS/overlap-workers",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := newServerBus(t)
			publishAll(t, bus, subj, "a", "b", "c", "d", "e")
			seedConsumer(t, bus, tc.existing)
			bus.jet = faultyJetStream{JetStream: bus.jet, racer: tc.racer, deleteErr: tc.deleteErr}

			stream, cons, err := bus.ensureDurableConsumer(t.Context(), tc.subject, tc.durable)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				if stream != nil || cons != nil {
					t.Errorf("stream, consumer = %v, %v on error, want nil", stream, cons)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			} else if got := cons.CachedInfo().Stream; got != StreamAerialEvents {
				t.Errorf("consumer stream = %q, want %q", got, StreamAerialEvents)
			}
			if tc.wantFilter == "" {
				return
			}
			wantDeliver := ""
			if tc.existing != nil && tc.deleteErr != nil {
				wantDeliver = tc.existing.DeliverSubject
			}
			assertConsumerConfig(t, bus, tc.durable, nats.ConsumerConfig{
				DeliverSubject: wantDeliver, FilterSubject: tc.wantFilter, DeliverPolicy: tc.wantPolicy, OptStartSeq: tc.wantStart,
			})
		})
	}
}

// seedConsumer creates cfg on the stream its filter belongs to; nil is a no-op.
func seedConsumer(t *testing.T, bus *JetStreamBus, cfg *nats.ConsumerConfig) {
	t.Helper()
	if cfg == nil {
		return
	}
	stream, err := bus.js.StreamNameBySubject(cfg.FilterSubject)
	if err != nil {
		t.Fatalf("seed stream: %v", err)
	}
	if _, err := bus.js.AddConsumer(stream, cfg); err != nil {
		t.Fatalf("seed consumer: %v", err)
	}
}

// assertConsumerConfig checks the deliver subject, filter, deliver policy and
// start sequence of durable on AERIAL_EVENTS against want.
func assertConsumerConfig(t *testing.T, bus *JetStreamBus, durable string, want nats.ConsumerConfig) {
	t.Helper()
	info, err := bus.js.ConsumerInfo(StreamAerialEvents, durable)
	if err != nil {
		t.Fatalf("ConsumerInfo: %v", err)
	}
	got := info.Config
	if got.DeliverSubject != want.DeliverSubject || got.FilterSubject != want.FilterSubject || got.DeliverPolicy != want.DeliverPolicy || got.OptStartSeq != want.OptStartSeq {
		t.Errorf("consumer = deliver %q filter %q policy %v start %d; want deliver %q filter %q policy %v start %d",
			got.DeliverSubject, got.FilterSubject, got.DeliverPolicy, got.OptStartSeq, want.DeliverSubject, want.FilterSubject, want.DeliverPolicy, want.OptStartSeq)
	}
}

func TestJetStreamBus_PushToPullMigration(t *testing.T) {
	t.Parallel()
	const subj, durable = "aerial.events.migrate", "migrate"
	bus := newServerBus(t)
	publishAll(t, bus, subj, "m1", "m2", "m3", "m4", "m5")

	// An earlier release's push consumer acked m1 and m2 and left m3 unacked.
	if _, err := bus.js.AddConsumer(StreamAerialEvents, &nats.ConsumerConfig{
		Durable: durable, FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, AckWait: 30 * time.Second,
		DeliverPolicy: nats.DeliverAllPolicy, DeliverSubject: "_DELIVER.AERIAL_EVENTS.migrate", DeliverGroup: durable, MaxAckPending: 3,
	}); err != nil {
		t.Fatalf("add push consumer: %v", err)
	}
	legacy, err := bus.nc.QueueSubscribeSync("_DELIVER.AERIAL_EVENTS.migrate", durable)
	if err != nil {
		t.Fatalf("legacy subscribe: %v", err)
	}
	for i := range 3 {
		m, err := legacy.NextMsg(brokerDeadline)
		if err != nil {
			t.Fatalf("legacy delivery %d: %v", i, err)
		}
		if i < 2 {
			if err := m.AckSync(); err != nil {
				t.Fatalf("legacy ack: %v", err)
			}
		}
	}
	if err := legacy.Unsubscribe(); err != nil {
		t.Fatalf("legacy unsubscribe: %v", err)
	}
	awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.AckFloor.Stream == 2 })

	handler, handled := recordingHandler("", nil)
	subscribe(t, bus, subj, durable, handler)
	got := receive(t, handled, 3)
	info := awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool {
		return ci.NumAckPending == 0 && ci.NumPending == 0 && ci.AckFloor.Stream == 5
	})
	assertNoMore(t, handled)
	if want := []handledMsg{{"m3", 1}, {"m4", 1}, {"m5", 1}}; !equalHandled(got, want) {
		t.Errorf("handled %v, want %v", got, want)
	}
	if info.Config.DeliverSubject != "" || info.Config.OptStartSeq != 3 {
		t.Errorf("migrated consumer deliver subject %q start %d, want pull starting at 3", info.Config.DeliverSubject, info.Config.OptStartSeq)
	}
}

func TestJetStreamBus_RecoverConsumerDeletedWhilePullWaits(t *testing.T) {
	t.Parallel()
	const subj, durable = "aerial.events.recover.wait", "recover-wait"
	bus := newServerBus(t)
	handler, handled := recordingHandler("", nil)
	subscribe(t, bus, subj, durable, handler)
	publishAll(t, bus, subj, "m1")
	if got := receive(t, handled, 1); got[0] != (handledMsg{"m1", 1}) {
		t.Fatalf("first delivery %+v, want m1", got[0])
	}
	awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.AckFloor.Stream == 1 && ci.NumWaiting == 1 })

	if err := bus.js.DeleteConsumer(StreamAerialEvents, durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	publishAll(t, bus, subj, "m2")
	if got := receive(t, handled, 1); got[0] != (handledMsg{"m2", 1}) {
		t.Errorf("after recovery got %+v, want m2 delivered once", got[0])
	}
	info := awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.AckFloor.Stream == 2 })
	assertNoMore(t, handled)
	if info.Config.DeliverSubject != "" || info.Config.OptStartSeq != 2 {
		t.Errorf("recreated consumer deliver subject %q start %d, want pull starting at 2", info.Config.DeliverSubject, info.Config.OptStartSeq)
	}
	publishAll(t, bus, subj, "m3")
	if got := receive(t, handled, 1); got[0] != (handledMsg{"m3", 1}) {
		t.Errorf("after resume got %+v, want m3", got[0])
	}
}

func TestJetStreamBus_RecoverConsumerDeletedWhileHandlerBusy(t *testing.T) {
	t.Parallel()
	const subj, durable = "aerial.events.recover.busy", "recover-busy"
	bus := newServerBus(t)
	release := make(chan struct{})
	handler, handled := recordingHandler("m1", release)
	subscribe(t, bus, subj, durable, handler)
	publishAll(t, bus, subj, "m1")
	awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.NumAckPending == 1 })

	if err := bus.js.DeleteConsumer(StreamAerialEvents, durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	publishAll(t, bus, subj, "m2")
	close(release)
	got := receive(t, handled, 2)
	if want := []handledMsg{{"m1", 1}, {"m2", 1}}; !equalHandled(got, want) {
		t.Errorf("handled %v, want %v", got, want)
	}
	info := awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.AckFloor.Stream == 2 })
	assertNoMore(t, handled)
	if info.Config.DeliverSubject != "" || info.Config.OptStartSeq != 2 {
		t.Errorf("recreated consumer deliver subject %q start %d, want pull starting at 2", info.Config.DeliverSubject, info.Config.OptStartSeq)
	}
}

func TestJetStreamBus_RecoverLeavesPushConsumerAlone(t *testing.T) {
	t.Parallel()
	const subj, durable = "aerial.events.recover.push", "recover-push"
	bus := newServerBus(t)
	handler, handled := recordingHandler("", nil)
	subscribe(t, bus, subj, durable, handler)
	publishAll(t, bus, subj, "m1")
	receive(t, handled, 1)
	awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.AckFloor.Stream == 1 && ci.NumWaiting == 1 })

	// A reverted pod replaces the durable with its push consumer.
	if err := bus.js.DeleteConsumer(StreamAerialEvents, durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	push, err := bus.js.AddConsumer(StreamAerialEvents, &nats.ConsumerConfig{
		Durable: durable, FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy,
		DeliverSubject: "_DELIVER.AERIAL_EVENTS.recover-push", DeliverGroup: durable,
	})
	if err != nil {
		t.Fatalf("add push consumer: %v", err)
	}
	awaitCond(t, "Healthy() to report the blocked durable", func() bool { return bus.Healthy() != nil })
	if healthErr := bus.Healthy(); !strings.Contains(healthErr.Error(), durable) {
		t.Errorf("Healthy() = %v, want it to name %s", healthErr, durable)
	}
	info, err := bus.js.ConsumerInfo(StreamAerialEvents, durable)
	if err != nil || info.Config.DeliverSubject == "" || !info.Created.Equal(push.Created) {
		t.Fatalf("push consumer after recovery probe = %+v, %v; want the untouched push consumer", info, err)
	}

	// Once the push consumer goes away the subscription converges back.
	if err := bus.js.DeleteConsumer(StreamAerialEvents, durable); err != nil {
		t.Fatalf("delete push consumer: %v", err)
	}
	publishAll(t, bus, subj, "m2")
	if got := receive(t, handled, 1); got[0] != (handledMsg{"m2", 1}) {
		t.Errorf("after convergence got %+v, want m2", got[0])
	}
	awaitCond(t, "Healthy() to recover", func() bool { return bus.Healthy() == nil })
	assertNoMore(t, handled)
}

func TestJetStreamBus_SettlementAgainstServer(t *testing.T) {
	t.Parallel()
	const subj = "aerial.jobs.settle"
	bus := newServerBus(t)
	type delivery struct {
		data      string
		delivered uint64
		snoozeSeq string
	}
	handled := make(chan delivery, 32)
	handler := func(ctx context.Context, msg Message) error {
		data, snoozeSeq := string(msg.Data()), msg.Header().Get(HeaderSnoozeSeq)
		handled <- delivery{data, msg.NumDelivered(), snoozeSeq}
		switch {
		case data == "ack":
			return msg.Ack()
		case data == "nak" && msg.NumDelivered() == 1:
			return msg.NakWithDelay(10 * time.Millisecond)
		case data == "term":
			return msg.Term()
		case data == "progress":
			if err := msg.InProgress(); err != nil {
				return err
			}
			return msg.Ack()
		case data == "snooze" && snoozeSeq == "":
			return msg.Snooze(0)
		}
		return nil
	}
	sub, err := bus.ConsumeQueue(t.Context(), subj, "settle-workers", handler)
	if err != nil {
		t.Fatalf("ConsumeQueue: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	publishAll(t, bus, subj, "ack", "nak", "term", "progress", "snooze")

	deadline := time.NewTimer(brokerDeadline)
	defer deadline.Stop()
	got := map[delivery]int{}
	for range 7 {
		select {
		case d := <-handled:
			got[d]++
		case <-deadline.C:
			t.Fatalf("handled %v, want 7 deliveries", got)
		}
	}
	awaitConsumer(t, bus, StreamAerialJobs, "settle-workers", func(ci *nats.ConsumerInfo) bool { return ci.NumAckPending == 0 && ci.NumPending == 0 })
	awaitCond(t, "work queue to drain", func() bool {
		si, err := bus.js.StreamInfo(StreamAerialJobs)
		return err == nil && si.State.Msgs == 0
	})
	select {
	case d := <-handled:
		t.Errorf("unexpected extra delivery %+v", d)
	default:
	}
	want := map[delivery]int{
		{"ack", 1, ""}: 1, {"nak", 1, ""}: 1, {"nak", 2, ""}: 1, {"term", 1, ""}: 1,
		{"progress", 1, ""}: 1, {"snooze", 1, ""}: 1, {"snooze", 1, "1"}: 1,
	}
	if !maps.Equal(got, want) {
		t.Errorf("deliveries %v, want %v", got, want)
	}
}

func TestResumeCursor(t *testing.T) {
	t.Parallel()

	type settlement struct {
		seq   uint64
		naked bool
	}
	tests := []struct {
		name      string
		initial   uint64
		settles   []settlement
		wantStart uint64
	}{
		{"nothing settled resumes at ack floor + 1", 7, nil, 7},
		{"acks advance past the highest sequence", 1, []settlement{{1, false}, {2, false}, {3, false}}, 4},
		{"out of order acks keep the highest", 1, []settlement{{3, false}, {2, false}}, 4},
		{"a nak pins the start at its sequence", 1, []settlement{{1, false}, {2, true}, {3, false}}, 2},
		{"a naked message settled later releases the pin", 1, []settlement{{2, true}, {3, false}, {2, false}}, 4},
		{"the lowest of several naks wins", 1, []settlement{{5, true}, {3, true}, {6, false}}, 3},
		{"settlements below the initial cursor do not rewind it", 10, []settlement{{4, false}}, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newResumeCursor(tc.initial)
			for _, st := range tc.settles {
				c.settle(st.seq, st.naked)
			}
			if got := c.start(); got != tc.wantStart {
				t.Errorf("start() = %d, want %d", got, tc.wantStart)
			}
		})
	}
}

func TestIsConsumerExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"consumer exists", jetstream.ErrConsumerExists, true},
		{"name already in use", jetstream.ErrConsumerNameAlreadyInUse, true},
		{"already exists api code", &jetstream.APIError{ErrorCode: jetstream.JSErrCodeConsumerAlreadyExists}, true},
		{"name exists api code", &jetstream.APIError{ErrorCode: jetstream.JSErrCodeConsumerNameExists}, true},
		{"not found", jetstream.ErrConsumerNotFound, false},
		{"other error", errors.New("boom"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isConsumerExists(tc.err); got != tc.want {
				t.Errorf("isConsumerExists(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestJetStreamBus_UnsubscribeStopsBlockedRecovery(t *testing.T) {
	t.Parallel()
	const subj, durable = "aerial.events.recover.stop", "recover-stop"
	bus := newServerBus(t)
	sub, subErr := bus.Subscribe(t.Context(), subj, durable, func(context.Context, Message) error { return nil })
	if subErr != nil {
		t.Fatalf("Subscribe: %v", subErr)
	}
	awaitConsumer(t, bus, StreamAerialEvents, durable, func(ci *nats.ConsumerInfo) bool { return ci.NumWaiting == 1 })
	if err := bus.js.DeleteConsumer(StreamAerialEvents, durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	if _, err := bus.js.AddConsumer(StreamAerialEvents, &nats.ConsumerConfig{
		Durable: durable, FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, DeliverSubject: "_DELIVER.AERIAL_EVENTS.recover-stop",
	}); err != nil {
		t.Fatalf("add push consumer: %v", err)
	}
	awaitCond(t, "Healthy() to report the blocked durable", func() bool { return bus.Healthy() != nil })

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("second Unsubscribe: %v", err)
	}
	js, ok := sub.(*jsSubscription)
	if !ok {
		t.Fatalf("subscription type %T, want *jsSubscription", sub)
	}
	done := js.done
	awaitCond(t, "the recovery loop to exit", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	if err := bus.Healthy(); err != nil {
		t.Errorf("Healthy() after Unsubscribe = %v, want nil", err)
	}
	info, err := bus.js.ConsumerInfo(StreamAerialEvents, durable)
	if err != nil || info.Config.DeliverSubject == "" {
		t.Errorf("push consumer after Unsubscribe = %+v, %v; want it untouched", info, err)
	}
}

// WorkQueue streams reject any deliver policy but "all"; they hold only
// unacked messages, so both migration and recovery recreate with DeliverAll.
func TestJetStreamBus_WorkQueueMigrationAndRecovery(t *testing.T) {
	t.Parallel()
	const subj, durable = "aerial.jobs.wqmigrate", "wq-migrate"
	bus := newServerBus(t)
	publishAll(t, bus, subj, "m1", "m2", "m3")

	if _, err := bus.js.AddConsumer(StreamAerialJobs, &nats.ConsumerConfig{
		Durable: durable, FilterSubject: subj, AckPolicy: nats.AckExplicitPolicy, AckWait: 30 * time.Second,
		DeliverPolicy: nats.DeliverAllPolicy, DeliverSubject: "_DELIVER.AERIAL_JOBS.wq-migrate", DeliverGroup: durable, MaxAckPending: 1,
	}); err != nil {
		t.Fatalf("add push consumer: %v", err)
	}
	legacy, err := bus.nc.QueueSubscribeSync("_DELIVER.AERIAL_JOBS.wq-migrate", durable)
	if err != nil {
		t.Fatalf("legacy subscribe: %v", err)
	}
	m, err := legacy.NextMsg(brokerDeadline)
	if err != nil {
		t.Fatalf("legacy delivery: %v", err)
	}
	if err := m.AckSync(); err != nil {
		t.Fatalf("legacy ack: %v", err)
	}
	if err := legacy.Unsubscribe(); err != nil {
		t.Fatalf("legacy unsubscribe: %v", err)
	}
	awaitConsumer(t, bus, StreamAerialJobs, durable, func(ci *nats.ConsumerInfo) bool { return ci.AckFloor.Stream == 1 })

	handler, handled := recordingHandler("", nil)
	subscribe(t, bus, subj, durable, handler)
	if got, want := receive(t, handled, 2), []handledMsg{{"m2", 1}, {"m3", 1}}; !equalHandled(got, want) {
		t.Errorf("after migration handled %v, want %v", got, want)
	}
	info := awaitConsumer(t, bus, StreamAerialJobs, durable, func(ci *nats.ConsumerInfo) bool {
		return ci.AckFloor.Stream == 3 && ci.NumWaiting == 1
	})
	if info.Config.DeliverSubject != "" || info.Config.DeliverPolicy != nats.DeliverAllPolicy {
		t.Errorf("migrated consumer deliver subject %q policy %v, want pull delivering all", info.Config.DeliverSubject, info.Config.DeliverPolicy)
	}

	if err := bus.js.DeleteConsumer(StreamAerialJobs, durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	publishAll(t, bus, subj, "m4")
	if got := receive(t, handled, 1); got[0] != (handledMsg{"m4", 1}) {
		t.Errorf("after recovery got %+v, want m4 delivered once", got[0])
	}
	info = awaitConsumer(t, bus, StreamAerialJobs, durable, func(ci *nats.ConsumerInfo) bool { return ci.NumAckPending == 0 && ci.NumPending == 0 })
	assertNoMore(t, handled)
	if info.Config.DeliverPolicy != nats.DeliverAllPolicy {
		t.Errorf("recreated consumer policy %v, want deliver all", info.Config.DeliverPolicy)
	}
}
