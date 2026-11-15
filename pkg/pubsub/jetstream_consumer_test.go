package pubsub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go"
)

type mockJetStream struct {
	nats.JetStreamContext

	mu              sync.Mutex
	publishErr      error
	subscribeErr    error
	streamInfoErr   error
	addStreamErr    error
	updateStreamErr error
	consumerInfoErr error
	addConsumerErr  error
	existingStreams map[string]*nats.StreamInfo
	// consumers holds server-side consumer state keyed by stream/name.
	consumers map[string]*nats.ConsumerInfo
	// racingConsumer is installed by AddConsumer just before it reports a lost
	// creation race, modelling another pod creating the consumer first.
	racingConsumer   *nats.ConsumerInfo
	addedConsumers   []nats.ConsumerConfig
	deletedConsumers []string
	subscriptions    []recordedSub
	publishedMsgs    []*nats.Msg
	onPublish        func(msg *nats.Msg)
}

// recordedSub captures how the bus asked the library to subscribe.
type recordedSub struct {
	queue     string
	bound     bool
	stream    string
	consumer  string
	durable   string
	manualAck bool
}

// decodeSubOpts applies nats.SubOpt values to a scratch nats.subOpts via
// reflection so tests can assert the bind target without a live server. It is
// coupled to nats.go internals, so any layout change surfaces as a descriptive
// error from the mock instead of a reflection panic.
func decodeSubOpts(queue string, opts []nats.SubOpt) (recordedSub, error) {
	got := recordedSub{queue: queue}
	var so reflect.Value
	cfg := &nats.ConsumerConfig{}
	field := func(name string, kind reflect.Kind) (reflect.Value, error) {
		f := so.FieldByName(name)
		if !f.IsValid() || f.Kind() != kind {
			return reflect.Value{}, fmt.Errorf("decodeSubOpts: nats.go subOpts.%s missing or not %s; update the test decoder for this nats.go version", name, kind)
		}
		return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem(), nil
	}
	for _, opt := range opts {
		fn := reflect.ValueOf(opt)
		if fn.Kind() != reflect.Func {
			continue // value options such as AckWait carry no bind target
		}
		ft := fn.Type()
		if ft.NumIn() != 1 || ft.In(0).Kind() != reflect.Pointer || ft.In(0).Elem().Kind() != reflect.Struct {
			return got, fmt.Errorf("decodeSubOpts: unexpected nats.SubOpt func type %s", ft)
		}
		if !so.IsValid() {
			so = reflect.New(ft.In(0).Elem()).Elem()
			cfgField, err := field("cfg", reflect.Pointer)
			if err != nil {
				return got, err
			}
			cfgField.Set(reflect.ValueOf(cfg))
		}
		fn.Call([]reflect.Value{so.Addr()})
	}
	if !so.IsValid() {
		return got, nil
	}
	fields := []struct {
		name string
		kind reflect.Kind
	}{{"bound", reflect.Bool}, {"stream", reflect.String}, {"consumer", reflect.String}, {"mack", reflect.Bool}}
	vals := make(map[string]reflect.Value, len(fields))
	for _, f := range fields {
		v, err := field(f.name, f.kind)
		if err != nil {
			return got, err
		}
		vals[f.name] = v
	}
	got.bound = vals["bound"].Bool()
	got.stream = vals["stream"].String()
	got.consumer = vals["consumer"].String()
	got.durable = cfg.Durable
	got.manualAck = vals["mack"].Bool()
	return got, nil
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

func (m *mockJetStream) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return m.QueueSubscribe(subj, "", cb, opts...)
}

func (m *mockJetStream) QueueSubscribe(subj, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	rec, err := decodeSubOpts(queue, opts)
	if err != nil {
		return nil, err
	}
	m.subscriptions = append(m.subscriptions, rec)
	if cb != nil {
		cb(&nats.Msg{Subject: subj, Data: []byte("test")})
	}
	return &nats.Subscription{}, nil
}

func (m *mockJetStream) StreamNameBySubject(subj string, opts ...nats.JSOpt) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, info := range m.existingStreams {
		for _, pattern := range info.Config.Subjects {
			if matchSubject(pattern, subj) {
				return name, nil
			}
		}
	}
	return "", nats.ErrNoMatchingStream
}

func (m *mockJetStream) AddConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumers == nil {
		m.consumers = make(map[string]*nats.ConsumerInfo)
	}
	if m.racingConsumer != nil {
		m.consumers[consumerKey(stream, m.racingConsumer.Name)] = m.racingConsumer
		m.racingConsumer = nil
		return nil, nats.ErrConsumerNameAlreadyInUse
	}
	if m.addConsumerErr != nil {
		return nil, m.addConsumerErr
	}
	m.addedConsumers = append(m.addedConsumers, *cfg)
	info := &nats.ConsumerInfo{Stream: stream, Name: cfg.Durable, Config: *cfg}
	m.consumers[consumerKey(stream, cfg.Durable)] = info
	return info, nil
}

func (m *mockJetStream) DeleteConsumer(stream, consumer string, opts ...nats.JSOpt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedConsumers = append(m.deletedConsumers, consumerKey(stream, consumer))
	delete(m.consumers, consumerKey(stream, consumer))
	return nil
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

type busSubscribeFunc func(b *JetStreamBus, ctx context.Context, subject, name string, handler Handler) (Subscription, error)

func TestJetStreamBus_SubscribeAndConsumeQueueBindDurables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		subscribe    busSubscribeFunc
		subject      string
		durable      string
		subscribeErr error
		wantErr      bool
		wantSubs     []recordedSub
		wantAdded    int
	}{
		{
			"durable subscribe binds deliver group", (*JetStreamBus).Subscribe, "aerial.events.haro", "haro-events", nil, false,
			[]recordedSub{{queue: "haro-events", bound: true, stream: "AERIAL_EVENTS", consumer: "haro-events", manualAck: true}},
			1,
		},
		{
			"consume queue binds deliver group", (*JetStreamBus).ConsumeQueue, "aerial.jobs.vanadis.chunk", "vanadis-chunk", nil, false,
			[]recordedSub{{queue: "vanadis-chunk", bound: true, stream: "AERIAL_JOBS", consumer: "vanadis-chunk", manualAck: true}},
			1,
		},
		{
			"empty durable subscribe stays ephemeral", (*JetStreamBus).Subscribe, "aerial.events.>", "", nil, false,
			[]recordedSub{{manualAck: true}},
			0,
		},
		{"empty queue rejected", (*JetStreamBus).ConsumeQueue, "aerial.jobs.>", "", nil, true, nil, 0},
		{"unresolvable stream fails before subscribing", (*JetStreamBus).Subscribe, "unknown.subject", "dur", nil, true, nil, 0},
		{"durable subscribe error", (*JetStreamBus).Subscribe, "aerial.events.>", "dur", errors.New("sub failed"), true, nil, 1},
		{"queue subscribe error", (*JetStreamBus).ConsumeQueue, "aerial.jobs.>", "q", errors.New("sub failed"), true, nil, 1},
		{"ephemeral subscribe error", (*JetStreamBus).Subscribe, "aerial.events.>", "", errors.New("sub failed"), true, nil, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{existingStreams: make(map[string]*nats.StreamInfo)}
			bus := newJetStreamBusWithContext(nil, mjs)
			if err := bus.EnsureCoreStreams(context.Background()); err != nil {
				t.Fatalf("EnsureCoreStreams: %v", err)
			}
			mjs.subscribeErr = tc.subscribeErr

			sub, err := tc.subscribe(bus, context.Background(), tc.subject, tc.durable, func(ctx context.Context, msg Message) error { return nil })
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if sub != nil {
				_ = sub.Unsubscribe()
			}
			if !reflect.DeepEqual(mjs.subscriptions, tc.wantSubs) {
				t.Errorf("subscriptions = %+v, want %+v", mjs.subscriptions, tc.wantSubs)
			}
			if len(mjs.addedConsumers) != tc.wantAdded {
				t.Errorf("added consumers = %d, want %d", len(mjs.addedConsumers), tc.wantAdded)
			}
			if len(mjs.deletedConsumers) != 0 {
				t.Errorf("deleted consumers = %v, want none", mjs.deletedConsumers)
			}
		})
	}
}

func TestJetStreamBus_EnsureDurableConsumer(t *testing.T) {
	t.Parallel()

	const subj = "aerial.events.haro"
	pushInfo := func(filter, group string, ackFloor uint64) *nats.ConsumerInfo {
		return &nats.ConsumerInfo{
			Stream:   "AERIAL_EVENTS",
			Name:     "haro-events",
			Config:   nats.ConsumerConfig{Durable: "haro-events", FilterSubject: filter, DeliverGroup: group, DeliverSubject: "_INBOX.legacy"},
			AckFloor: nats.SequenceInfo{Stream: ackFloor},
		}
	}
	fresh := nats.ConsumerConfig{
		Durable:        "haro-events",
		FilterSubject:  "aerial.events.haro",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  nats.DeliverAllPolicy,
		DeliverGroup:   "haro-events",
		DeliverSubject: "_DELIVER.AERIAL_EVENTS.haro-events",
	}
	resumeAt := func(seq uint64) nats.ConsumerConfig {
		cfg := fresh
		cfg.DeliverPolicy = nats.DeliverByStartSequencePolicy
		cfg.OptStartSeq = seq
		return cfg
	}

	tests := []struct {
		name            string
		subject         string
		existing        *nats.ConsumerInfo
		racing          *nats.ConsumerInfo
		consumerInfoErr error
		addConsumerErr  error
		wantStream      string
		wantErr         bool
		wantAdded       []nats.ConsumerConfig
		wantDeleted     []string
	}{
		{
			"missing consumer created with deterministic deliver subject", subj, nil, nil, nil, nil, "AERIAL_EVENTS", false,
			[]nats.ConsumerConfig{fresh},
			nil,
		},
		{
			"compatible consumer bound unchanged", subj, pushInfo(subj, "haro-events", 41), nil, nil, nil, "AERIAL_EVENTS", false,
			nil, nil,
		},
		{
			"legacy consumer without group recreated after ack floor", subj, pushInfo(subj, "", 41), nil, nil, nil, "AERIAL_EVENTS", false,
			[]nats.ConsumerConfig{resumeAt(42)},
			[]string{"AERIAL_EVENTS/haro-events"},
		},
		{
			"different group recreated", subj, pushInfo(subj, "other", 0), nil, nil, nil, "AERIAL_EVENTS", false,
			[]nats.ConsumerConfig{resumeAt(1)},
			[]string{"AERIAL_EVENTS/haro-events"},
		},
		{
			"filter mismatch recreated", subj, pushInfo("aerial.events.other", "haro-events", 9), nil, nil, nil, "AERIAL_EVENTS", false,
			[]nats.ConsumerConfig{resumeAt(10)},
			[]string{"AERIAL_EVENTS/haro-events"},
		},
		{
			"create race with compatible winner succeeds", subj, nil, pushInfo(subj, "haro-events", 0), nil, nil, "AERIAL_EVENTS", false,
			nil, nil,
		},
		{
			"create race with incompatible winner fails", subj, nil, pushInfo(subj, "", 0), nil, nil, "", true,
			nil, nil,
		},
		{
			"stream lookup failure", "unknown.subject", nil, nil, nil, nil, "", true,
			nil, nil,
		},
		{
			"consumer info failure", subj, nil, nil, errors.New("timeout"), nil, "", true,
			nil, nil,
		},
		{
			"add consumer failure", subj, nil, nil, nil, errors.New("insufficient resources"), "", true,
			nil, nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{
				existingStreams: map[string]*nats.StreamInfo{
					"AERIAL_EVENTS": {Config: nats.StreamConfig{Name: "AERIAL_EVENTS", Subjects: []string{"aerial.events.>"}}},
				},
				consumers:       map[string]*nats.ConsumerInfo{},
				racingConsumer:  tc.racing,
				consumerInfoErr: tc.consumerInfoErr,
				addConsumerErr:  tc.addConsumerErr,
			}
			if tc.existing != nil {
				mjs.consumers["AERIAL_EVENTS/haro-events"] = tc.existing
			}
			bus := newJetStreamBusWithContext(nil, mjs)

			stream, err := bus.ensureDurableConsumer(context.Background(), tc.subject, "haro-events")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if stream != tc.wantStream {
				t.Errorf("stream = %q, want %q", stream, tc.wantStream)
			}
			if !reflect.DeepEqual(mjs.addedConsumers, tc.wantAdded) {
				t.Errorf("added = %+v, want %+v", mjs.addedConsumers, tc.wantAdded)
			}
			if !reflect.DeepEqual(mjs.deletedConsumers, tc.wantDeleted) {
				t.Errorf("deleted = %v, want %v", mjs.deletedConsumers, tc.wantDeleted)
			}
		})
	}
}
