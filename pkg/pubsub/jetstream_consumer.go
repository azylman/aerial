package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

// errDurableRequired rejects ephemeral subscriptions: every consumer the bus
// creates is a named durable pull consumer.
var errDurableRequired = errors.New("pubsub: durable name required")

// Subscribe establishes a pub/sub fan-out subscription bound to the explicitly
// managed durable pull consumer named durable (see ensureDurableConsumer).
// Overlapping pods share it during rolling updates and no pod ever deletes it
// on Unsubscribe. An empty durable is rejected with errDurableRequired, unless
// subject is an ephemeral Core NATS subject (e.g. voice.stream.*).
func (b *JetStreamBus) Subscribe(ctx context.Context, subject string, durable string, handler Handler) (Subscription, error) {
	if durable == "" && isCoreSubject(subject) {
		return b.subscribeCore(ctx, subject, handler)
	}
	if durable == "" {
		return nil, fmt.Errorf("subscribe to %s: %w", subject, errDurableRequired)
	}
	return b.bindDurable(ctx, subject, durable, handler)
}

func (b *JetStreamBus) subscribeCore(subCtx context.Context, subject string, handler Handler) (Subscription, error) {
	if b == nil || b.nc == nil {
		return nil, fmt.Errorf("subscribe to %s: nats connection is nil", subject)
	}
	sub, err := b.nc.Subscribe(subject, func(m *nats.Msg) { b.dispatchCore(subCtx, m, handler) })
	if err != nil {
		return nil, fmt.Errorf("subscribe to %s: %w", subject, err)
	}
	return sub, nil
}

func (*JetStreamBus) dispatchCore(subCtx context.Context, m *nats.Msg, handler Handler) {
	ctx := context.WithoutCancel(subCtx)
	msg := &coreMessage{raw: m}
	if h := msg.Header(); len(h) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(http.Header(h)))
	}
	ctx, span := tracing.Tracer().Start(ctx, SpanSubject(msg.Subject())+" process", processSpanOptions(msg.Subject(), "", 1, msg.Header().Get(HeaderMsgID))...)
	var err error
	defer func() {
		failSpan(span, err)
		span.End()
	}()
	err = handler(ctx, msg)
}

type coreMessage struct {
	raw *nats.Msg
}

func (m *coreMessage) Data() []byte    { return m.raw.Data }
func (m *coreMessage) Subject() string { return m.raw.Subject }

func (m *coreMessage) Header() nats.Header {
	if m.raw.Header != nil {
		return m.raw.Header
	}
	return nats.Header{}
}

func (*coreMessage) NumDelivered() uint64             { return 1 }
func (*coreMessage) Ack() error                       { return nil }
func (*coreMessage) NakWithDelay(time.Duration) error { return nil }
func (*coreMessage) Term() error                      { return nil }
func (*coreMessage) Snooze(time.Duration) error       { return nil }
func (*coreMessage) InProgress() error                { return nil }

// ConsumeQueue establishes a competing-consumer work queue subscription bound
// to the explicitly managed durable consumer named queue.
func (b *JetStreamBus) ConsumeQueue(ctx context.Context, subject string, queue string, handler Handler) (Subscription, error) {
	if queue == "" {
		return nil, fmt.Errorf("queue subscribe to %s: queue name is required", subject)
	}
	return b.bindDurable(ctx, subject, queue, handler)
}

// pullBatch is the most messages one subscription asks the server for at a
// time. Handlers run serially, so any prefetch above 1 parks messages
// client-side with their AckWait already running: they get redelivered while
// they wait and replay after they already succeeded, which is the bug pull
// consumers exist to remove. There is deliberately no per-consumer size option.
const pullBatch = 1

// defaultPullHeartbeat is the idle heartbeat the server sends while a pull
// request waits. A pull sent to a consumer that vanished while its handler was
// busy gets no reply at all, so two missed heartbeats are the only signal that
// recovery is needed. The nats.go default (half the 30s pull expiry) would
// leave such a subscription silent for 30s.
const defaultPullHeartbeat = 5 * time.Second

// pullOptions returns the options for every message iterator the bus opens.
func (b *JetStreamBus) pullOptions() []jetstream.PullMessagesOpt {
	return []jetstream.PullMessagesOpt{jetstream.PullMaxMessages(pullBatch), jetstream.PullHeartbeat(b.pullHeartbeat)}
}

// bindDurable ensures the durable pull consumer exists, opens a message
// iterator on it and starts the subscription's consume loop.
func (b *JetStreamBus) bindDurable(ctx context.Context, subject, durable string, handler Handler) (Subscription, error) {
	if b.jet == nil {
		return nil, fmt.Errorf("subscribe to %s: jetstream handle is nil", subject)
	}
	stream, cons, err := b.ensureDurableConsumer(ctx, subject, durable)
	if err != nil {
		return nil, err
	}
	iter, err := cons.Messages(b.pullOptions()...)
	if err != nil {
		return nil, fmt.Errorf("consume %s/%s: %w", stream.CachedInfo().Config.Name, durable, err)
	}
	sub := &jsSubscription{
		bus:     b,
		stream:  stream,
		subject: subject,
		durable: durable,
		handler: handler,
		iter:    iter,
		cursor:  newResumeCursor(cons.CachedInfo().AckFloor.Stream + 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go sub.run(ctx)
	return sub, nil
}

// durableAckWait is the redelivery timeout for all consumers created by the bus.
const durableAckWait = 30 * time.Second

// durableConsumerConfig returns the desired pull consumer config for durable.
func durableConsumerConfig(subject, durable string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       durableAckWait,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
}

// durableCompatible reports whether an existing consumer can be consumed
// as-is: a pull consumer (no deliver subject) filtering subject.
func durableCompatible(cfg jetstream.ConsumerConfig, subject string) bool {
	return cfg.DeliverSubject == "" && cfg.FilterSubject == subject
}

// isConsumerExists reports whether CreateConsumer lost a creation race.
func isConsumerExists(err error) bool {
	if errors.Is(err, jetstream.ErrConsumerExists) || errors.Is(err, jetstream.ErrConsumerNameAlreadyInUse) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == jetstream.JSErrCodeConsumerAlreadyExists ||
			apiErr.ErrorCode == jetstream.JSErrCodeConsumerNameExists ||
			apiErr.ErrorCode == jetstream.JSErrCodeConsumerExists)
}

// readConsumer returns the info of durable on stream whatever its type, plus
// a handle to it when it is a pull consumer.
func readConsumer(ctx context.Context, stream jetstream.Stream, durable string) (*jetstream.ConsumerInfo, jetstream.Consumer, error) {
	cons, err := stream.Consumer(ctx, durable)
	if err == nil {
		return cons.CachedInfo(), cons, nil
	}
	if !errors.Is(err, jetstream.ErrNotPullConsumer) {
		return nil, nil, err
	}
	push, err := stream.PushConsumer(ctx, durable)
	if err != nil {
		return nil, nil, err
	}
	return push.CachedInfo(), nil, nil
}

// ensureDurableConsumer makes sure a durable pull consumer filtering subject
// exists and returns its stream and a handle to it. Compatible consumers are
// left untouched. Incompatible ones (push consumers from earlier releases, or
// a different filter) are deleted and recreated as pull consumers starting
// right after their ack floor: pull and push consumers cannot be updated into
// each other in place. Nothing at or below the floor is redelivered and no
// unacked message is skipped. On WorkQueue streams acked messages are already
// gone; on limits streams messages acked out of order above the floor may be
// redelivered, so delivery stays at-least-once and handlers must be idempotent.
//
// This startup migration is the only place the bus ever deletes a push
// consumer; subscription recovery never does (see jsSubscription.recover).
//
// Concurrent migrations are tolerated but not serialized: JetStream has no
// conditional delete, so two pods that both read the legacy consumer may each
// delete and recreate it. On a lost creation race the winner is re-read and
// bound only if compatible; the cost is redelivery of messages acked in that
// window, which the at-least-once contract above already requires handlers to
// absorb.
func (b *JetStreamBus) ensureDurableConsumer(ctx context.Context, subject, durable string) (jetstream.Stream, jetstream.Consumer, error) {
	name, err := b.jet.StreamNameBySubject(ctx, subject)
	var stream jetstream.Stream
	if err == nil {
		stream, err = b.jet.Stream(ctx, name)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("resolve stream for %s: %w", subject, err)
	}
	desired := durableConsumerConfig(subject, durable)

	info, cons, err := readConsumer(ctx, stream, durable)
	switch {
	case err == nil && durableCompatible(info.Config, subject):
		return stream, cons, nil
	case err == nil:
		resumeAt(&desired, stream, info.AckFloor.Stream+1)
		log.Printf("[pubsub] migrating consumer %s/%s (filter %q, deliver subject %q) to pull, resuming at stream seq %d",
			name, durable, info.Config.FilterSubject, info.Config.DeliverSubject, desired.OptStartSeq)
		if deleteErr := stream.DeleteConsumer(ctx, durable); deleteErr != nil && !errors.Is(deleteErr, jetstream.ErrConsumerNotFound) {
			return nil, nil, fmt.Errorf("delete incompatible consumer %s/%s: %w", name, durable, deleteErr)
		}
	case !errors.Is(err, jetstream.ErrConsumerNotFound):
		return nil, nil, fmt.Errorf("consumer info %s/%s: %w", name, durable, err)
	}

	cons, err = stream.CreateConsumer(ctx, desired)
	if err == nil {
		return stream, cons, nil
	}
	if !isConsumerExists(err) {
		return nil, nil, fmt.Errorf("add consumer %s/%s: %w", name, durable, err)
	}
	// Another pod created it first; bind only if it is compatible.
	info, cons, err = readConsumer(ctx, stream, durable)
	if err != nil {
		return nil, nil, fmt.Errorf("consumer info %s/%s after create race: %w", name, durable, err)
	}
	if !durableCompatible(info.Config, subject) {
		return nil, nil, fmt.Errorf("consumer %s/%s was concurrently created with an incompatible config", name, durable)
	}
	return stream, cons, nil
}

// resumeAt points cfg at stream sequence seq. WorkQueue streams reject any
// deliver policy but "all", and need none: they hold only unacked messages, so
// DeliverAll already resumes exactly where the old consumer left off.
func resumeAt(cfg *jetstream.ConsumerConfig, stream jetstream.Stream, seq uint64) {
	if info := stream.CachedInfo(); info != nil && info.Config.Retention == jetstream.WorkQueuePolicy {
		return
	}
	cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
	cfg.OptStartSeq = seq
}

// resumeCursor tracks the stream sequence a recreated consumer must start at
// so that no handled message replays and no unhandled one is skipped.
type resumeCursor struct {
	next  uint64
	naked map[uint64]struct{}
}

func newResumeCursor(next uint64) *resumeCursor {
	return &resumeCursor{next: next, naked: map[uint64]struct{}{}}
}

// settle records that the message at stream sequence seq settled. A naked
// message still needs a redelivery, so it pins the start sequence until it
// settles any other way.
func (c *resumeCursor) settle(seq uint64, naked bool) {
	if naked {
		c.naked[seq] = struct{}{}
	} else {
		delete(c.naked, seq)
	}
	c.next = max(c.next, seq+1)
}

// start returns the stream sequence a recreated consumer starts at.
func (c *resumeCursor) start() uint64 {
	start := c.next
	for seq := range c.naked {
		start = min(start, seq)
	}
	return start
}

// jsSubscription is one durable pull subscription. A single goroutine (run)
// asks for one message, dispatches it, and only then asks for the next, so no
// message ever waits client-side with its AckWait running while the handler
// is busy.
//
// Known leftover: after a NATS reconnect, nats.go issues a fresh pull request
// even though the one parked on the server before the disconnect may still be
// live, so at most one extra message can wait client-side briefly. A
// redelivery needs yet another pull, so a duplicate handler run stays
// unlikely.
type jsSubscription struct {
	bus     *JetStreamBus
	stream  jetstream.Stream
	subject string
	durable string
	handler Handler
	cursor  *resumeCursor // owned by run

	mu       sync.Mutex
	iter     jetstream.MessagesContext
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// Unsubscribe stops fetching and stops the recovery loop. No new message is
// fetched after it returns; a handler already running still finishes and
// settles its message. The durable consumer is never deleted.
func (s *jsSubscription) Unsubscribe() error {
	if s.stop == nil {
		return nil
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		close(s.stop)
		if s.iter != nil {
			s.iter.Stop()
		}
		s.mu.Unlock()
	})
	return nil
}

func (s *jsSubscription) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

func (s *jsSubscription) currentIter() jetstream.MessagesContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.iter
}

// run is the subscription's consume loop: fetch one message, dispatch it,
// record its settlement, repeat. Next errors trigger recover. ctx is the
// subscriber's context; deliveries keep its values but not its cancellation.
func (s *jsSubscription) run(ctx context.Context) {
	defer close(s.done)
	// A stopped subscription no longer counts against Healthy().
	defer s.bus.blocked.set(s.durable, false)
	for {
		iter := s.currentIter()
		msg, err := iter.Next()
		if err != nil {
			iter.Stop()
			if s.stopped() || s.bus.nc.IsClosed() {
				return
			}
			if !s.recover(ctx, err) {
				return
			}
			continue
		}
		wrapped := s.bus.dispatch(ctx, msg, s.durable, s.handler)
		if wrapped.meta != nil {
			s.cursor.settle(wrapped.meta.Sequence.Stream, wrapped.wasNaked())
		}
	}
}

// recoveryBackoffCap bounds the wait between recovery probes.
const recoveryBackoffCap = 30 * time.Second

// recover restores consumption after the iterator failed with cause. It
// probes the durable on a capped backoff until it can consume again:
//   - a pull consumer with our filter: open a new iterator;
//   - not found twice in a row (one backoff step apart): recreate it as a pull
//     consumer at the resume cursor, never DeliverAll;
//   - a push consumer or a different filter (an old or reverted pod owns it):
//     never delete it; keep backing off and report Healthy() errors until this
//     subscription consumes again, so a revert converges instead of fighting.
//
// It returns false when the subscription was stopped.
func (s *jsSubscription) recover(subCtx context.Context, cause error) bool {
	ctx := context.WithoutCancel(subCtx)
	name := s.stream.CachedInfo().Config.Name
	log.Printf("[pubsub] consumer %s/%s stopped delivering (%v); recovering", name, s.durable, cause)
	missing := false
	for attempt := uint64(1); ; attempt++ {
		info, cons, err := readConsumer(ctx, s.stream, s.durable)
		switch {
		case err == nil && durableCompatible(info.Config, s.subject):
			if s.restart(cons) {
				s.bus.blocked.set(s.durable, false)
				log.Printf("[pubsub] consumer %s/%s consuming again", name, s.durable)
				return true
			}
		case err == nil:
			s.bus.blocked.set(s.durable, true)
			log.Printf("[pubsub] ERROR: consumer %s/%s is held by an incompatible consumer (filter %q, deliver subject %q); not deleting it, retrying",
				name, s.durable, info.Config.FilterSubject, info.Config.DeliverSubject)
			missing = false
		case errors.Is(err, jetstream.ErrConsumerNotFound) && missing:
			s.recreate(ctx, name)
			missing = false
			continue
		case errors.Is(err, jetstream.ErrConsumerNotFound):
			missing = true
		default:
			log.Printf("[pubsub] warning: probing consumer %s/%s: %v", name, s.durable, err)
			missing = false
		}
		if !s.wait(NakBackoff(attempt, recoveryBackoffCap)) {
			return false
		}
	}
}

// recreate creates the missing durable as a pull consumer at the resume
// cursor. A creation race is resolved by the next probe.
func (s *jsSubscription) recreate(ctx context.Context, name string) {
	cfg := durableConsumerConfig(s.subject, s.durable)
	resumeAt(&cfg, s.stream, s.cursor.start())
	log.Printf("[pubsub] recreating missing consumer %s/%s as pull, resuming at stream seq %d", name, s.durable, cfg.OptStartSeq)
	if _, err := s.stream.CreateConsumer(ctx, cfg); err != nil && !isConsumerExists(err) {
		log.Printf("[pubsub] warning: recreating consumer %s/%s: %v", name, s.durable, err)
	}
}

// restart installs a fresh iterator on cons unless the subscription stopped.
func (s *jsSubscription) restart(cons jetstream.Consumer) bool {
	iter, err := cons.Messages(s.bus.pullOptions()...)
	if err != nil {
		log.Printf("[pubsub] warning: reopening consumer %s: %v", s.durable, err)
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped() {
		iter.Stop()
		return false
	}
	s.iter = iter
	return true
}

// wait blocks for d or until the subscription stops; it reports whether the
// subscription is still running.
func (s *jsSubscription) wait(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-s.stop:
		return false
	case <-timer.C:
		return true
	}
}

// dispatch runs handler for m and returns the settled message. Deliveries keep the
// subscriber's context values but not its cancellation (a delivery is settled by ack/nak,
// not by the subscribe call's lifetime); the publisher's trace arrives through m's headers
// and parents a messaging process span that covers the handler and the settlement decision.
// group is the durable or queue name.
func (b *JetStreamBus) dispatch(subCtx context.Context, m jetstream.Msg, group string, handler Handler) *jsMessage {
	ctx := context.WithoutCancel(subCtx)
	if h := m.Headers(); h != nil {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(http.Header(h)))
	}

	meta, err := m.Metadata()
	wrapped := &jsMessage{
		raw:  m,
		bus:  b,
		done: subCtx.Done(),
	}
	if err == nil {
		wrapped.meta = meta
	}

	ctx, span := tracing.Tracer().Start(ctx, SpanSubject(m.Subject())+" process", processSpanOptions(m.Subject(), group, wrapped.NumDelivered(), m.Headers().Get(HeaderMsgID))...)
	wrapped.spanCtx = span.SpanContext()
	var handlerErr error
	defer func() {
		failSpan(span, handlerErr)
		span.End()
	}()

	stopTicker := make(chan struct{})
	doneTicker := make(chan struct{})
	interval := b.heartbeatInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		defer close(doneTicker)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicker:
				return
			case <-ticker.C:
				if progErr := wrapped.InProgress(); progErr != nil {
					log.Printf("[pubsub] warning: background heartbeat failed: %v", progErr)
				}
			}
		}
	}()

	err = handler(ctx, wrapped)
	handlerErr = err
	close(stopTicker)
	<-doneTicker

	wrapped.mu.Lock()
	settled := wrapped.settled
	wrapped.mu.Unlock()

	if settled {
		return wrapped
	}
	if err == nil {
		if ackErr := wrapped.Ack(); ackErr != nil {
			log.Printf("[pubsub] warning: automatic ack failed: %v", ackErr)
		}
	} else {
		if nakErr := wrapped.NakWithDelay(1 * time.Second); nakErr != nil {
			log.Printf("[pubsub] warning: automatic nak failed: %v", nakErr)
		}
	}
	return wrapped
}

type jsMessage struct {
	raw     jetstream.Msg
	bus     Bus
	done    <-chan struct{}
	spanCtx trace.SpanContext // process span; Snooze republishes as its child
	mu      sync.Mutex
	settled bool
	naked   bool
	meta    *jetstream.MsgMetadata
	header  nats.Header // stands in for a missing raw header
}

func (m *jsMessage) Data() []byte {
	return m.raw.Data()
}

func (m *jsMessage) Subject() string {
	return m.raw.Subject()
}

func (m *jsMessage) Header() nats.Header {
	if h := m.raw.Headers(); h != nil {
		return h
	}
	if m.header == nil {
		m.header = nats.Header{}
	}
	return m.header
}

func (m *jsMessage) NumDelivered() uint64 {
	if m.meta != nil {
		return m.meta.NumDelivered
	}
	meta, err := m.raw.Metadata()
	if err == nil && meta != nil {
		m.meta = meta
		return meta.NumDelivered
	}
	return 1
}

// wasNaked reports whether the message settled with a nak.
func (m *jsMessage) wasNaked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.naked
}

func (m *jsMessage) InProgress() error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	err := m.raw.InProgress()
	if isMessageNotFoundError(err) {
		return nil
	}
	return err
}

func (m *jsMessage) Ack() error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
	m.mu.Unlock()

	err := m.raw.Ack()
	if isMessageNotFoundError(err) {
		return nil
	}
	return err
}

func (m *jsMessage) NakWithDelay(delay time.Duration) error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
	m.naked = true
	m.mu.Unlock()

	err := m.raw.NakWithDelay(delay)
	if isMessageNotFoundError(err) {
		return nil
	}
	return err
}

func (m *jsMessage) Term() error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
	m.mu.Unlock()

	err := m.raw.Term()
	if isMessageNotFoundError(err) {
		return nil
	}
	return err
}

// waitSnoozeDelay synchronously holds the message delivery for delay, heartbeating via
// InProgress on each hbInterval tick so JetStream AckWait does not expire. Synchronous
// waiting ensures the original message is acknowledged only after publishContinuation
// succeeds, maintaining at-least-once delivery without premature acknowledgment.
// The wait aborts immediately when m.done receives, returning context.Canceled so the
// caller leaves the delivery unacknowledged for clean redelivery on shutdown.
func (m *jsMessage) waitSnoozeDelay(delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	hbInterval := 10 * time.Second
	if jsb, ok := m.bus.(*JetStreamBus); ok && jsb != nil && jsb.heartbeatInterval > 0 {
		hbInterval = jsb.heartbeatInterval
	}
	if hbInterval > durableAckWait/2 {
		hbInterval = durableAckWait / 2
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	ticker := time.NewTicker(hbInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return context.Canceled
		case <-timer.C:
			return nil
		case <-ticker.C:
			if inProgErr := m.InProgress(); inProgErr != nil {
				log.Printf("[pubsub] warning: snooze heartbeat failed: %v", inProgErr)
			}
		}
	}
}

func (m *jsMessage) Snooze(delay time.Duration) error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	seq := 0
	if seqStr := m.Header().Get(HeaderSnoozeSeq); seqStr != "" {
		if s, err := strconv.Atoi(seqStr); err == nil {
			seq = s
		}
	}
	nextSeq := seq + 1

	msgID := m.Header().Get(HeaderMsgID)
	if idx := strings.Index(msgID, ":snooze:"); idx != -1 {
		msgID = msgID[:idx]
	}
	if msgID == "" {
		msgID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	rotatedMsgID := fmt.Sprintf("%s:snooze:%d", msgID, nextSeq)

	newHeader := nats.Header{}
	for k, vv := range m.Header() {
		for _, v := range vv {
			newHeader.Add(k, v)
		}
	}
	newHeader.Set(HeaderMsgID, rotatedMsgID)
	newHeader.Set(HeaderSnoozeSeq, strconv.Itoa(nextSeq))

	publishContinuation := func() error {
		ctx := trace.ContextWithSpanContext(context.Background(), m.spanCtx)
		if hp, ok := m.bus.(HeaderPublisher); ok {
			return hp.PublishWithHeader(ctx, m.Subject(), rotatedMsgID, m.Data(), newHeader)
		}
		return m.bus.Publish(ctx, m.Subject(), rotatedMsgID, m.Data())
	}

	if err := m.waitSnoozeDelay(delay); err != nil {
		return fmt.Errorf("snooze delay: %w", err)
	}

	if err := publishContinuation(); err != nil {
		return fmt.Errorf("snooze publish continuation: %w", err)
	}

	m.mu.Lock()
	m.settled = true
	m.mu.Unlock()

	err := m.raw.Ack()
	if isMessageNotFoundError(err) {
		return nil
	}
	return err
}
