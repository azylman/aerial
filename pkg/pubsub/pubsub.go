package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

// JetStreamBus is the production NATS JetStream implementation of Bus.
// Publishing and stream management use the legacy JetStreamContext js;
// durable consumers are pull consumers created and consumed through jet.
type JetStreamBus struct {
	nc                *nats.Conn
	js                nats.JetStreamContext
	jet               jetstream.JetStream
	heartbeatInterval time.Duration
	pullHeartbeat     time.Duration
	blocked           blockedDurables
}

// blockedDurables tracks subscriptions that cannot consume because a push
// consumer (owned by an old or reverted pod) holds their durable name.
type blockedDurables struct {
	mu    sync.Mutex
	names map[string]struct{}
}

func (d *blockedDurables) set(name string, blocked bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !blocked {
		delete(d.names, name)
		return
	}
	if d.names == nil {
		d.names = make(map[string]struct{})
	}
	d.names[name] = struct{}{}
}

func (d *blockedDurables) sorted() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Sorted(maps.Keys(d.names))
}

// jetStreamBusOptions returns the resilient default connect options followed by
// extraOpts, so callers can override any default.
//
// IgnoreAuthErrorAbort keeps the client reconnecting through repeated
// authorization errors (e.g. a client that races a broker password rotation)
// instead of closing the connection permanently after the second auth failure.
func jetStreamBusOptions(extraOpts ...nats.Option) []nats.Option {
	opts := make([]nats.Option, 0, 5+len(extraOpts))
	opts = append(opts,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.IgnoreAuthErrorAbort(),
		nats.ClosedHandler(logConnClosed),
	)
	return append(opts, extraOpts...)
}

func logConnClosed(nc *nats.Conn) {
	log.Printf("[pubsub] ERROR: NATS connection permanently closed (last error: %v); bus is unhealthy until restart", nc.LastError())
}

// buildConnectOptions constructs the NATS connect options including user credentials and defaults.
func buildConnectOptions(natsURL, user, pass string, extraOpts ...nats.Option) []nats.Option {
	opts := make([]nats.Option, 0, len(extraOpts)+1)
	opts = append(opts, extraOpts...)
	if user != "" && pass != "" && !strings.Contains(natsURL, "@") {
		opts = append(opts, nats.UserInfo(user, pass))
	}
	return jetStreamBusOptions(opts...)
}

// NewJetStreamBus connects to NATS with resilient reconnect options and initializes JetStream.
func NewJetStreamBus(ctx context.Context, natsURL string, extraOpts ...nats.Option) (*JetStreamBus, error) {
	user := os.Getenv("NATS_USER")
	pass := os.Getenv("NATS_PASSWORD")
	nc, err := nats.Connect(natsURL, buildConnectOptions(natsURL, user, pass, extraOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	bus, err := NewJetStreamBusFromConn(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return bus, nil
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
	jet, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return &JetStreamBus{nc: nc, js: js, jet: jet, heartbeatInterval: 10 * time.Second, pullHeartbeat: defaultPullHeartbeat}, nil
}

// Publish publishes a message with W3C trace injection and deduplication MsgID.
func (b *JetStreamBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
	return b.PublishWithHeader(ctx, subject, msgID, data, nil)
}

// PublishWithHeader publishes a message with additional custom headers and trace context.
// The send is wrapped in a messaging producer span whose context travels in the headers.
func (b *JetStreamBus) PublishWithHeader(ctx context.Context, subject string, msgID string, data any, header nats.Header) (err error) {
	ctx, span := tracing.Tracer().Start(ctx, subject+" publish", publishSpanOptions(subject, msgID)...)
	defer func() {
		failSpan(span, err)
		span.End()
	}()

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

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(mergedHeader)))

	msg := &nats.Msg{Subject: subject, Data: payload, Header: mergedHeader}

	if isCoreSubject(subject) {
		if b == nil || b.nc == nil {
			return fmt.Errorf("publish to %s: nats connection is nil", subject)
		}
		if err = b.nc.PublishMsg(msg); err != nil {
			return fmt.Errorf("publish to %s: %w", subject, err)
		}
		return nil
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

// EnsureCoreStreams registers and reconciles the 4 primary JetStream streams.
func (b *JetStreamBus) EnsureCoreStreams(ctx context.Context) error {
	configs := []*nats.StreamConfig{
		{
			Name:        StreamAerialEvents,
			Description: "Multi-consumer pub/sub fan-out stream",
			Subjects:    []string{"aerial.dispatch.>", "aerial.events.>", "aerial.turn.>", "aerial.pr.>"},
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
			MaxAge:            72 * time.Hour,
			MaxBytes:          64 * 1024 * 1024,
		},
		{
			Name:        StreamAerialTranscripts,
			Description: "Per-turn transcript step batches",
			Subjects:    []string{"aerial.transcripts.>"},
			Retention:   nats.WorkQueuePolicy,
			Storage:     nats.FileStorage,
			MaxAge:      72 * time.Hour,
			MaxBytes:    128 * 1024 * 1024,
			Discard:     nats.DiscardNew,
			Duplicates:  30 * time.Minute,
		},
	}

	for _, cfg := range configs {
		if err := b.EnsureStream(cfg); err != nil {
			return fmt.Errorf("ensure stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

// mergeStreamLimits returns the desired config with retention floors raised to
// the broker's existing values. Old pods still UpdateStream at startup, so a
// rollout must never shrink a stream below limits an earlier release set:
// MaxAge and MaxBytes only ever grow here. A zero MaxAge and a -1 MaxBytes
// each mean unlimited and always win, so an unlimited stream is never capped.
func mergeStreamLimits(desired, existing *nats.StreamConfig) *nats.StreamConfig {
	merged := *desired
	merged.MaxAge = maxStreamAge(desired.MaxAge, existing.MaxAge)
	merged.MaxBytes = maxStreamBytes(desired.MaxBytes, existing.MaxBytes)
	return &merged
}

// maxStreamAge returns the larger age limit, where zero means unlimited.
func maxStreamAge(desired, existing time.Duration) time.Duration {
	if desired == 0 || existing == 0 {
		return 0
	}
	if existing > desired {
		return existing
	}
	return desired
}

// maxStreamBytes returns the larger byte limit, where -1 means unlimited.
func maxStreamBytes(desired, existing int64) int64 {
	if desired == -1 || existing == -1 {
		return -1
	}
	if existing > desired {
		return existing
	}
	return desired
}

// EnsureStream creates the stream when it does not exist, or updates it with
// mergeStreamLimits so a rollout never shrinks retention limits an earlier
// release set.
func (b *JetStreamBus) EnsureStream(cfg *nats.StreamConfig) error {
	info, err := b.js.StreamInfo(cfg.Name)
	if err == nil && info != nil {
		_, err = b.js.UpdateStream(mergeStreamLimits(cfg, &info.Config))
		return err
	}
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = b.js.AddStream(cfg)
		return err
	}
	return err
}

// Healthy reports an error when the bus has no connection or the connection is
// permanently closed. A connection that is reconnecting is still healthy: it
// buffers and recovers on its own.
func (b *JetStreamBus) Healthy() error {
	if b == nil || b.nc == nil {
		return errors.New("pubsub: nats connection not initialized")
	}
	if b.nc.IsClosed() {
		if lastErr := b.nc.LastError(); lastErr != nil {
			return fmt.Errorf("%w: %w", ErrConnectionClosed, lastErr)
		}
		return ErrConnectionClosed
	}
	if blocked := b.blocked.sorted(); len(blocked) > 0 {
		return fmt.Errorf("pubsub: durables %s are held by push consumers and cannot consume", strings.Join(blocked, ", "))
	}
	return nil
}

// Close closes the underlying NATS connection.
func (b *JetStreamBus) Close() error {
	if b.nc != nil {
		b.nc.Close()
	}
	return nil
}

// ConsumerStats returns the number of pending and in-flight (ack pending) messages for a consumer.
func (b *JetStreamBus) ConsumerStats(ctx context.Context, stream, consumer string) (int64, int64, error) {
	if b == nil || b.js == nil {
		return 0, 0, errors.New("pubsub: jetstream context is nil")
	}
	info, err := b.js.ConsumerInfo(stream, consumer, jsContextOpts(ctx)...)
	if err != nil {
		return 0, 0, fmt.Errorf("consumer info %s/%s: %w", stream, consumer, err)
	}
	return int64(info.NumPending), int64(info.NumAckPending), nil
}

// jsContextOpts scopes JetStream API requests to ctx when one is provided.
func jsContextOpts(ctx context.Context) []nats.JSOpt {
	if ctx == nil {
		return nil
	}
	return []nats.JSOpt{nats.Context(ctx)}
}

// Messaging semantic-convention attribute keys and values for NATS spans.
const (
	attrMessagingSystem        = attribute.Key("messaging.system")
	attrMessagingOpType        = attribute.Key("messaging.operation.type")
	attrMessagingOpName        = attribute.Key("messaging.operation.name")
	attrMessagingDestination   = attribute.Key("messaging.destination.name")
	attrMessagingMessageID     = attribute.Key("messaging.message.id")
	attrMessagingConsumerGroup = attribute.Key("messaging.consumer.group.name")
	attrNatsDeliveryCount      = attribute.Key("messaging.nats.message.delivery_count")
	messagingSystemNats        = "nats"
)

// publishSpanOptions describes the producer span for one publish of subject.
func publishSpanOptions(subject, msgID string) []trace.SpanStartOption {
	attrs := []attribute.KeyValue{
		attrMessagingSystem.String(messagingSystemNats),
		attrMessagingOpType.String("send"),
		attrMessagingOpName.String("publish"),
		attrMessagingDestination.String(subject),
	}
	if msgID != "" {
		attrs = append(attrs, attrMessagingMessageID.String(msgID))
	}
	return []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...)}
}

// processSpanOptions describes the consumer span for one delivery of subject; it is started
// as a child of the context extracted from the message headers.
func processSpanOptions(subject, group string, delivered uint64, msgID string) []trace.SpanStartOption {
	attrs := []attribute.KeyValue{
		attrMessagingSystem.String(messagingSystemNats),
		attrMessagingOpType.String("process"),
		attrMessagingDestination.String(subject),
		attrNatsDeliveryCount.Int64(int64(min(delivered, uint64(math.MaxInt64)))),
	}
	if group != "" {
		attrs = append(attrs, attrMessagingConsumerGroup.String(group))
	}
	if msgID != "" {
		attrs = append(attrs, attrMessagingMessageID.String(msgID))
	}
	return []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...)}
}

// failSpan records err on span and sets status Error when err is non-nil.
func failSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
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
	return strings.Contains(s, "message not found") || strings.Contains(s, "10039") || errors.Is(err, nats.ErrMsgNotFound)
}
