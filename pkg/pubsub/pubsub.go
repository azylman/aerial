package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

// Stream names across the cluster.
const (
	StreamAerialEvents    = "AERIAL_EVENTS"
	StreamAerialJobs      = "AERIAL_JOBS"
	StreamAerialCoalesced = "AERIAL_COALESCED"
	StreamVisionJobs      = "VISION_JOBS"
)

// Subject prefixes and well-known subjects.
const (
	SubjectAerialDispatchPrefix  = "aerial.dispatch."
	SubjectAerialEventsPrefix    = "aerial.events."
	SubjectAerialTurnPrefix      = "aerial.turn."
	SubjectAerialJobsPrefix      = "aerial.jobs."
	SubjectAerialCoalescedPrefix = "aerial.coalesced."
	SubjectAerialTurnCompleted   = "aerial.turn.completed"
)

// Well-known headers.
const (
	HeaderMsgID       = "Nats-Msg-Id"
	HeaderSnoozeSeq   = "Nats-Snooze-Seq"
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// Bus defines the core event bus and work-queue contract across Aerial.
type Bus interface {
	Publish(ctx context.Context, subject string, msgID string, data any) error
	Subscribe(ctx context.Context, subject string, durable string, handler Handler) (Subscription, error)
	ConsumeQueue(ctx context.Context, subject string, queue string, handler Handler) (Subscription, error)
	EnsureCoreStreams(ctx context.Context) error
	Close() error
}

// HeaderPublisher is an optional interface implemented by buses supporting custom header forwarding.
type HeaderPublisher interface {
	PublishWithHeader(ctx context.Context, subject string, msgID string, data any, header nats.Header) error
}

// Subscription represents an active subscription that can be terminated.
type Subscription interface {
	Unsubscribe() error
}

// Message wraps a delivered event or job message.
type Message interface {
	Data() []byte
	Subject() string
	Header() nats.Header
	NumDelivered() uint64
	Ack() error
	NakWithDelay(delay time.Duration) error
	Term() error
	Snooze(delay time.Duration) error
	InProgress() error
}

// Handler is invoked when a message is received on a subscription or queue.
type Handler func(ctx context.Context, msg Message) error

// DispatchOutcomeEvent represents an execution completion event emitted on aerial.dispatch.<status>.
type DispatchOutcomeEvent struct {
	DispatchID     string    `json:"dispatch_id"`
	Source         string    `json:"source"`
	SessionID      string    `json:"session_id,omitempty"`
	TargetID       string    `json:"target_id,omitempty"`
	VanadisChunkID string    `json:"vanadis_chunk_id,omitempty"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	DurationMs     int64     `json:"duration_ms"`
}

// ExternalEvent represents an external incoming trigger emitted on aerial.events.<source>.<event_type>.
type ExternalEvent struct {
	EventID       string    `json:"event_id,omitempty"`
	Source        string    `json:"source"`
	EventType     string    `json:"event_type"`
	Targets       []string  `json:"targets"`
	ContextHeader string    `json:"context_header,omitempty"`
	OccurredAt    time.Time `json:"occurred_at"`
}

// TurnCompletedEvent represents an agent conversation turn completion on aerial.turn.completed.
type TurnCompletedEvent struct {
	SessionID      string    `json:"session_id"`
	ConversationID string    `json:"conversation_id,omitempty"`
	Source         string    `json:"source"`
	CompletedAt    time.Time `json:"completed_at"`
}

// EnsureCoreStreams ensures the three core streams exist with their specified topologies.
func EnsureCoreStreams(ctx context.Context, bus Bus) error {
	if bus == nil {
		return errors.New("pubsub: bus is nil")
	}
	return bus.EnsureCoreStreams(ctx)
}

// JetStreamBus is the production NATS JetStream implementation of Bus.
type JetStreamBus struct {
	nc                *nats.Conn
	js                nats.JetStreamContext
	heartbeatInterval time.Duration
}

// NewJetStreamBus connects to NATS with resilient reconnect options and initializes JetStream.
func NewJetStreamBus(ctx context.Context, natsURL string, extraOpts ...nats.Option) (*JetStreamBus, error) {
	opts := make([]nats.Option, 0, 3+len(extraOpts))
	opts = append(opts,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1*time.Second),
		nats.RetryOnFailedConnect(true),
	)
	opts = append(opts, extraOpts...)
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &JetStreamBus{nc: nc, js: js, heartbeatInterval: 10 * time.Second}, nil
}

// NewJetStreamBusFromConn creates a JetStreamBus from an existing active NATS connection.
func NewJetStreamBusFromConn(nc *nats.Conn) (*JetStreamBus, error) {
	if nc == nil {
		return nil, errors.New("pubsub: nats connection is nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &JetStreamBus{nc: nc, js: js, heartbeatInterval: 10 * time.Second}, nil
}

func newJetStreamBusWithContext(nc *nats.Conn, js nats.JetStreamContext) *JetStreamBus {
	return &JetStreamBus{nc: nc, js: js, heartbeatInterval: 1 * time.Millisecond}
}

// Publish publishes a message with W3C trace injection and deduplication MsgID.
func (b *JetStreamBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
	return b.PublishWithHeader(ctx, subject, msgID, data, nil)
}

// PublishWithHeader publishes a message with additional custom headers and trace context.
func (b *JetStreamBus) PublishWithHeader(ctx context.Context, subject string, msgID string, data any, header nats.Header) error {
	payload, err := serializePayload(data)
	if err != nil {
		return err
	}

	mergedHeader := nats.Header{}
	for k, vv := range header {
		for _, v := range vv {
			mergedHeader.Add(k, v)
		}
	}
	if msgID != "" {
		mergedHeader.Set(HeaderMsgID, msgID)
	}

	carrier := propagation.HeaderCarrier(http.Header(mergedHeader))
	propagation.TraceContext{}.Inject(ctx, carrier)

	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  mergedHeader,
	}

	var pubOpts []nats.PubOpt
	if msgID != "" {
		pubOpts = append(pubOpts, nats.MsgId(msgID))
	}

	_, err = b.js.PublishMsg(msg, pubOpts...)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", subject, err)
	}
	return nil
}

// Subscribe establishes a durable pub/sub fan-out subscription.
func (b *JetStreamBus) Subscribe(ctx context.Context, subject string, durable string, handler Handler) (Subscription, error) {
	opts := []nats.SubOpt{
		nats.ManualAck(),
		nats.AckWait(30 * time.Second),
	}
	if durable != "" {
		opts = append(opts, nats.Durable(durable))
	}

	sub, err := b.js.Subscribe(subject, func(m *nats.Msg) {
		b.dispatch(m, handler)
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("subscribe to %s: %w", subject, err)
	}
	return &jsSubscription{sub: sub}, nil
}

// ConsumeQueue establishes a competing-consumer work queue subscription.
func (b *JetStreamBus) ConsumeQueue(ctx context.Context, subject string, queue string, handler Handler) (Subscription, error) {
	opts := []nats.SubOpt{
		nats.ManualAck(),
		nats.AckWait(30 * time.Second),
	}
	if queue != "" {
		opts = append(opts, nats.Durable(queue))
	}

	sub, err := b.js.QueueSubscribe(subject, queue, func(m *nats.Msg) {
		b.dispatch(m, handler)
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("queue subscribe to %s: %w", subject, err)
	}
	return &jsSubscription{sub: sub}, nil
}

// EnsureCoreStreams registers and reconciles the 3 primary JetStream streams.
func (b *JetStreamBus) EnsureCoreStreams(ctx context.Context) error {
	configs := []*nats.StreamConfig{
		{
			Name:        StreamAerialEvents,
			Description: "Multi-consumer pub/sub fan-out stream",
			Subjects:    []string{"aerial.dispatch.>", "aerial.events.>", "aerial.turn.>"},
			Retention:   nats.LimitsPolicy,
			Storage:     nats.FileStorage,
			MaxAge:      72 * time.Hour,
			MaxBytes:    256 * 1024 * 1024,
			Duplicates:  10 * time.Minute,
		},
		{
			Name:        StreamAerialJobs,
			Description: "Competing-consumer durable job queue for control-plane workers",
			Subjects:    []string{"aerial.jobs.>"},
			Retention:   nats.WorkQueuePolicy,
			Storage:     nats.FileStorage,
			MaxAge:      72 * time.Hour,
			MaxBytes:    256 * 1024 * 1024,
			Duplicates:  30 * time.Minute,
		},
		{
			Name:              StreamAerialCoalesced,
			Description:       "Per-key state-coalescing work queue",
			Subjects:          []string{"aerial.coalesced.>"},
			Retention:         nats.WorkQueuePolicy,
			MaxMsgsPerSubject: 1,
			Discard:           nats.DiscardOld,
			Storage:           nats.FileStorage,
			MaxAge:            24 * time.Hour,
			MaxBytes:          64 * 1024 * 1024,
		},
	}

	for _, cfg := range configs {
		if err := b.ensureStream(cfg); err != nil {
			return fmt.Errorf("ensure stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

func (b *JetStreamBus) ensureStream(cfg *nats.StreamConfig) error {
	info, err := b.js.StreamInfo(cfg.Name)
	if err == nil && info != nil {
		_, err = b.js.UpdateStream(cfg)
		return err
	}
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = b.js.AddStream(cfg)
		return err
	}
	return err
}

// Close closes the underlying NATS connection.
func (b *JetStreamBus) Close() error {
	if b.nc != nil {
		b.nc.Close()
	}
	return nil
}

func (b *JetStreamBus) dispatch(m *nats.Msg, handler Handler) {
	ctx := context.Background()
	if m.Header != nil {
		carrier := propagation.HeaderCarrier(http.Header(m.Header))
		ctx = propagation.TraceContext{}.Extract(ctx, carrier)
	}

	meta, err := m.Metadata()
	wrapped := &jsMessage{
		raw: m,
		bus: b,
	}
	if err == nil {
		wrapped.meta = meta
	}

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
	close(stopTicker)
	<-doneTicker

	wrapped.mu.Lock()
	settled := wrapped.settled
	wrapped.mu.Unlock()

	if settled {
		return
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
}

type jsSubscription struct {
	sub *nats.Subscription
}

func (s *jsSubscription) Unsubscribe() error {
	if s.sub != nil {
		return s.sub.Unsubscribe()
	}
	return nil
}

type jsMessage struct {
	raw     *nats.Msg
	bus     Bus
	mu      sync.Mutex
	settled bool
	meta    *nats.MsgMetadata
}

func (m *jsMessage) Data() []byte {
	return m.raw.Data
}

func (m *jsMessage) Subject() string {
	return m.raw.Subject
}

func (m *jsMessage) Header() nats.Header {
	if m.raw.Header == nil {
		m.raw.Header = nats.Header{}
	}
	return m.raw.Header
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

func (m *jsMessage) Snooze(delay time.Duration) error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
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
		ctx := context.Background()
		if hp, ok := m.bus.(HeaderPublisher); ok {
			return hp.PublishWithHeader(ctx, m.Subject(), rotatedMsgID, m.Data(), newHeader)
		}
		return m.bus.Publish(ctx, m.Subject(), rotatedMsgID, m.Data())
	}

	if delay > 0 {
		time.AfterFunc(delay, func() {
			if pubErr := publishContinuation(); pubErr != nil {
				log.Printf("[pubsub] warning: background snooze publish failed: %v", pubErr)
			}
		})
	} else {
		if err := publishContinuation(); err != nil {
			return fmt.Errorf("snooze publish continuation: %w", err)
		}
	}

	err := m.raw.Ack()
	if isMessageNotFoundError(err) {
		return nil
	}
	return err
}

func serializePayload(data any) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		return b, nil
	}
}

func isMessageNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "message not found") ||
		strings.Contains(s, "10039") ||
		errors.Is(err, nats.ErrMsgNotFound)
}
