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
	SubjectAerialDispatchPrefix       = "aerial.dispatch."
	SubjectAerialEventsPrefix         = "aerial.events."
	SubjectAerialTurnPrefix           = "aerial.turn."
	SubjectAerialPRPrefix             = "aerial.pr."
	SubjectAerialJobsPrefix           = "aerial.jobs."
	SubjectAerialCoalescedPrefix      = "aerial.coalesced."
	SubjectAerialTurnCompleted        = "aerial.turn.completed"
	SubjectAerialTurnTranscriptSynced = "aerial.turn.transcript_synced"
	SubjectCovenExtractPrefix         = "aerial.coalesced.coven_extract."
	SubjectBuildWidgetSyncPrefix      = "aerial.coalesced.widget_sync.build."
	SubjectBuildWidgetSyncWildcard    = SubjectBuildWidgetSyncPrefix + "*"
	SubjectProsperaReportJob          = "aerial.jobs.prospera.report"
	SubjectProsperaVoicemailJob       = "aerial.jobs.prospera.voicemail"
	SubjectProsperaSMSJob             = "aerial.jobs.prospera.sms"
	SubjectEriPromptJob               = "aerial.jobs.eri.prompt"
	SubjectEriDispatchJob             = "aerial.jobs.eri.dispatch"
	SubjectEriDiscordMessageJob       = "aerial.jobs.eri.discord_message"
	SubjectHaroOneShotJob             = "aerial.jobs.haro.oneshot"
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

// ConsumerStatsProvider is an optional interface implemented by buses that support querying consumer queue depth.
type ConsumerStatsProvider interface {
	ConsumerStats(ctx context.Context, stream, consumer string) (pending int64, inFlight int64, err error)
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

// TranscriptSyncedEvent represents transcript indexing completion on aerial.turn.transcript_synced.
type TranscriptSyncedEvent struct {
	SessionID      string    `json:"session_id"`
	ConversationID string    `json:"conversation_id"`
	StepsInserted  int       `json:"steps_inserted"`
	SyncedAt       time.Time `json:"synced_at"`
}

// TurnCompletedEvent represents an agent conversation turn completion on aerial.turn.completed.
type TurnCompletedEvent struct {
	SessionID      string    `json:"session_id"`
	ConversationID string    `json:"conversation_id,omitempty"`
	Source         string    `json:"source"`
	CompletedAt    time.Time `json:"completed_at"`
	Status         string    `json:"status,omitempty"`
	DurationMs     int64     `json:"duration_ms,omitempty"`
	InputTokens    int       `json:"input_tokens,omitempty"`
	OutputTokens   int       `json:"output_tokens,omitempty"`
	TotalTokens    int       `json:"total_tokens,omitempty"`
}

// CovenExtractJobArgs contains arguments for coalesced Coven thread fact extraction.
type CovenExtractJobArgs struct {
	ThreadID  string `json:"thread_id"`
	SessionID string `json:"session_id,omitempty"`
}

// WidgetSyncJobArgs contains arguments for coalesced widget sync jobs.
type WidgetSyncJobArgs struct {
	BuildID  string `json:"build_id,omitempty"`
	Banner   string `json:"banner,omitempty"`
	Repo     string `json:"repo,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
	Status   string `json:"status,omitempty"`
}

// ProsperaReportJobArgs contains arguments for post-call report generation jobs.
type ProsperaReportJobArgs struct {
	CallID string `json:"call_id"`
}

// ProsperaVoicemailJobArgs contains arguments for voicemail transcription and notification jobs.
type ProsperaVoicemailJobArgs struct {
	CallID       string `json:"call_id"`
	RecordingSID string `json:"recording_sid,omitempty"`
	RecordingURL string `json:"recording_url"`
	DurationSec  int    `json:"duration_sec,omitempty"`
}

// ProsperaSMSJobArgs contains arguments for inbound SMS processing and routing jobs.
type ProsperaSMSJobArgs struct {
	MessageSID    string   `json:"message_sid"`
	FromNumber    string   `json:"from_number"`
	ToNumber      string   `json:"to_number"`
	Body          string   `json:"body"`
	MediaURLs     []string `json:"media_urls,omitempty"`
	RelatedCallID string   `json:"related_call_id,omitempty"`
}

// EriPromptJobArgs contains arguments for asynchronous Eri prompt execution jobs.
type EriPromptJobArgs struct {
	ChannelID      string `json:"channel_id"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	Silent         bool   `json:"silent,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	DispatchID     string `json:"dispatch_id,omitempty"`
}

// EriDispatchJobArgs contains arguments for asynchronous Eri dispatch execution jobs.
type EriDispatchJobArgs struct {
	ID             string `json:"id,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
}

// EriDiscordMessageJobArgs contains arguments for asynchronous Discord message delivery jobs.
type EriDiscordMessageJobArgs struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

// HaroOneShotJobArgs contains arguments for durable one-shot scheduled execution jobs.
type HaroOneShotJobArgs struct {
	ID       string    `json:"id,omitempty"`
	ThreadID string    `json:"thread_id"`
	Prompt   string    `json:"prompt"`
	Effort   string    `json:"effort,omitempty"`
	RunAt    time.Time `json:"run_at"`
}

func cleanSubjectToken(token string) string {
	t := strings.TrimSpace(token)
	if t == "" {
		return "unknown"
	}
	t = strings.ReplaceAll(t, "*", "-")
	t = strings.ReplaceAll(t, ">", "-")
	t = strings.ReplaceAll(t, " ", "-")
	for strings.Contains(t, "--") {
		t = strings.ReplaceAll(t, "--", "-")
	}
	t = strings.Trim(t, ".-")
	if t == "" {
		return "unknown"
	}
	return t
}

// SubjectCovenExtractCoalesced returns the coalesced subject for a thread's fact extraction.
func SubjectCovenExtractCoalesced(threadID string) string {
	t := cleanSubjectToken(threadID)
	t = strings.ReplaceAll(t, ".", "-")
	if t == "" {
		t = "unknown"
	}
	return SubjectCovenExtractPrefix + t
}

// SubjectBuildWidgetSyncCoalesced returns the coalesced subject for build widget sync.
func SubjectBuildWidgetSyncCoalesced(buildID string) string {
	t := cleanSubjectToken(buildID)
	t = strings.ReplaceAll(t, ".", "-")
	if t == "" {
		t = "unknown"
	}
	return SubjectBuildWidgetSyncPrefix + t
}

// SubjectPRWidgetSyncCoalesced returns the coalesced subject for PR widget sync.
func SubjectPRWidgetSyncCoalesced(repo string, prNumber int) string {
	r := strings.ReplaceAll(strings.TrimSpace(repo), "/", ".")
	r = cleanSubjectToken(r)
	return fmt.Sprintf("aerial.coalesced.widget_sync.pr.%s.%d", r, prNumber)
}

// PRRolloutState tracks Kubernetes / GitOps rollout progress across jobs and Kustomizations.
type PRRolloutState struct {
	ExpectedJobs          []string          `json:"expected_jobs,omitempty"`
	StartedJobs           []string          `json:"started_jobs,omitempty"`
	DeployedJobs          []string          `json:"deployed_jobs,omitempty"`
	JobStates             map[string]string `json:"job_states,omitempty"`
	KustomizationStatuses map[string]string `json:"kustomization_statuses,omitempty"`
}

// PRLifecycleEvent represents pull request and deployment lifecycle transitions on aerial.pr.<event>.
type PRLifecycleEvent struct {
	Event        string          `json:"event"`
	Repo         string          `json:"repo"`
	PRNumber     int             `json:"pr_number"`
	Branch       string          `json:"branch,omitempty"`
	HeadSHA      string          `json:"head_sha,omitempty"`
	MergeSHA     string          `json:"merge_sha,omitempty"`
	Title        string          `json:"title,omitempty"`
	TargetID     string          `json:"target_id,omitempty"`
	Status       string          `json:"status,omitempty"`
	CheckName    string          `json:"check_name,omitempty"`
	CheckState   string          `json:"check_state,omitempty"`
	CheckURL     string          `json:"check_url,omitempty"`
	JobName      string          `json:"job_name,omitempty"`
	Details      string          `json:"details,omitempty"`
	RolloutState *PRRolloutState `json:"rollout_state,omitempty"`
	Quiet        bool            `json:"quiet,omitempty"`
	OccurredAt   time.Time       `json:"occurred_at"`
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

// Subscribe establishes a pub/sub fan-out subscription. A non-empty durable
// binds to an explicitly managed durable consumer (see ensureDurableConsumer)
// whose deliver group equals the durable name, so overlapping pods share it
// during rolling updates and no pod ever deletes it on Unsubscribe. An empty
// durable creates a library-owned ephemeral consumer that is removed on
// Unsubscribe.
func (b *JetStreamBus) Subscribe(ctx context.Context, subject string, durable string, handler Handler) (Subscription, error) {
	if durable != "" {
		return b.bindDurable(ctx, subject, durable, handler)
	}
	sub, err := b.js.Subscribe(subject, func(m *nats.Msg) {
		b.dispatch(ctx, m, handler)
	}, nats.ManualAck(), nats.AckWait(durableAckWait))
	if err != nil {
		return nil, fmt.Errorf("subscribe to %s: %w", subject, err)
	}
	return &jsSubscription{sub: sub}, nil
}

// ConsumeQueue establishes a competing-consumer work queue subscription bound
// to the explicitly managed durable consumer named queue.
func (b *JetStreamBus) ConsumeQueue(ctx context.Context, subject string, queue string, handler Handler) (Subscription, error) {
	if queue == "" {
		return nil, fmt.Errorf("queue subscribe to %s: queue name is required", subject)
	}
	return b.bindDurable(ctx, subject, queue, handler)
}

// bindDurable ensures the durable consumer exists and attaches a queue
// subscription to it with nats.Bind, which never deletes the consumer.
func (b *JetStreamBus) bindDurable(ctx context.Context, subject, durable string, handler Handler) (Subscription, error) {
	stream, err := b.ensureDurableConsumer(ctx, subject, durable)
	if err != nil {
		return nil, err
	}
	sub, err := b.js.QueueSubscribe(subject, durable, func(m *nats.Msg) {
		b.dispatch(ctx, m, handler)
	}, nats.Bind(stream, durable), nats.ManualAck())
	if err != nil {
		return nil, fmt.Errorf("queue subscribe to %s: %w", subject, err)
	}
	return &jsSubscription{sub: sub}, nil
}

// durableAckWait is the redelivery timeout for all consumers created by the bus.
const durableAckWait = 30 * time.Second

// durableDeliverSubject is deterministic so concurrent creators on different
// pods submit identical configs and subscriptions keep receiving even if the
// consumer is recreated underneath them.
func durableDeliverSubject(stream, durable string) string {
	return "_DELIVER." + stream + "." + durable
}

// durableConsumerConfig returns the desired push consumer config for durable.
func durableConsumerConfig(stream, subject, durable string) *nats.ConsumerConfig {
	return &nats.ConsumerConfig{
		Durable:        durable,
		FilterSubject:  subject,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        durableAckWait,
		DeliverPolicy:  nats.DeliverAllPolicy,
		DeliverGroup:   durable,
		DeliverSubject: durableDeliverSubject(stream, durable),
	}
}

// durableCompatible reports whether an existing consumer can be bound as-is:
// a push consumer filtering subject with a deliver group equal to durable.
func durableCompatible(cfg *nats.ConsumerConfig, subject, durable string) bool {
	return cfg.DeliverSubject != "" && cfg.FilterSubject == subject && cfg.DeliverGroup == durable
}

// isConsumerExists reports whether AddConsumer lost a creation race.
func isConsumerExists(err error) bool {
	if errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		return true
	}
	var apiErr *nats.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == nats.JSErrCodeConsumerAlreadyExists || apiErr.ErrorCode == nats.JSErrCodeConsumerNameExists)
}

// ensureDurableConsumer makes sure a durable push consumer compatible with a
// bound queue subscription exists for subject and returns its stream.
// Compatible consumers are left untouched. Incompatible ones (legacy consumers
// without a deliver group, a different group or filter, or pull consumers) are
// recreated starting right after their ack floor: nothing at or below the floor
// is redelivered and no unacked message is skipped, but messages acked out of
// order above the floor may be redelivered, so delivery stays at-least-once and
// handlers must be idempotent.
//
// Concurrent migrations are tolerated but not serialized: JetStream has no
// conditional delete, so two pods that both read the legacy consumer may each
// delete and recreate it. The deterministic deliver subject and group keep
// every pod's bound subscription receiving from whichever consumer survives;
// the cost is redelivery of messages acked in that window, which the
// at-least-once contract above already requires handlers to absorb.
func (b *JetStreamBus) ensureDurableConsumer(ctx context.Context, subject, durable string) (string, error) {
	opts := jsContextOpts(ctx)
	stream, err := b.js.StreamNameBySubject(subject, opts...)
	if err != nil {
		return "", fmt.Errorf("resolve stream for %s: %w", subject, err)
	}
	desired := durableConsumerConfig(stream, subject, durable)

	info, err := b.js.ConsumerInfo(stream, durable, opts...)
	switch {
	case err == nil && durableCompatible(&info.Config, subject, durable):
		return stream, nil
	case err == nil:
		desired.DeliverPolicy = nats.DeliverByStartSequencePolicy
		desired.OptStartSeq = info.AckFloor.Stream + 1
		log.Printf("[pubsub] migrating consumer %s/%s (filter %q, group %q) to bound deliver group, resuming at stream seq %d",
			stream, durable, info.Config.FilterSubject, info.Config.DeliverGroup, desired.OptStartSeq)
		if err := b.js.DeleteConsumer(stream, durable, opts...); err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
			return "", fmt.Errorf("delete incompatible consumer %s/%s: %w", stream, durable, err)
		}
	case !errors.Is(err, nats.ErrConsumerNotFound):
		return "", fmt.Errorf("consumer info %s/%s: %w", stream, durable, err)
	}

	_, err = b.js.AddConsumer(stream, desired, opts...)
	if err == nil {
		return stream, nil
	}
	if !isConsumerExists(err) {
		return "", fmt.Errorf("add consumer %s/%s: %w", stream, durable, err)
	}
	// Another pod created it first; bind only if it is compatible.
	info, err = b.js.ConsumerInfo(stream, durable, opts...)
	if err != nil {
		return "", fmt.Errorf("consumer info %s/%s after create race: %w", stream, durable, err)
	}
	if !durableCompatible(&info.Config, subject, durable) {
		return "", fmt.Errorf("consumer %s/%s was concurrently created with an incompatible config", stream, durable)
	}
	return stream, nil
}

// EnsureCoreStreams registers and reconciles the 3 primary JetStream streams.
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

// dispatch runs handler for m. Deliveries keep the subscriber's context values but not its
// cancellation (a delivery is settled by ack/nak, not by the subscribe call's lifetime); the
// publisher's trace arrives through m's headers.
func (b *JetStreamBus) dispatch(subCtx context.Context, m *nats.Msg, handler Handler) {
	ctx := context.WithoutCancel(subCtx)
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
		m.mu.Lock()
		m.settled = true
		m.mu.Unlock()

		time.AfterFunc(delay, func() {
			if pubErr := publishContinuation(); pubErr != nil {
				log.Printf("[pubsub] warning: background snooze publish failed: %v", pubErr)
			}
		})
	} else {
		if err := publishContinuation(); err != nil {
			return fmt.Errorf("snooze publish continuation: %w", err)
		}
		m.mu.Lock()
		m.settled = true
		m.mu.Unlock()
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
