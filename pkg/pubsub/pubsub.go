package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/pkg/tracing"
)

const (
	unknownSubjectToken = "unknown"
	defaultAgent        = "aerial"
)

// Stream names across the cluster.
const (
	StreamAerialEvents      = "AERIAL_EVENTS"
	StreamAerialJobs        = "AERIAL_JOBS"
	StreamAerialCoalesced   = "AERIAL_COALESCED"
	StreamAerialTranscripts = "AERIAL_TRANSCRIPTS"
	StreamVisionJobs        = "VISION_JOBS"
)

// Subject prefixes and well-known subjects.
const (
	SubjectAerialDispatchPrefix    = "aerial.dispatch."
	SubjectAerialEventsPrefix      = "aerial.events."
	SubjectAerialTurnPrefix        = "aerial.turn."
	SubjectAerialPRPrefix          = "aerial.pr."
	SubjectAerialJobsPrefix        = "aerial.jobs."
	SubjectAerialCoalescedPrefix   = "aerial.coalesced."
	SubjectAerialTranscriptsPrefix = "aerial.transcripts."
	SubjectTranscriptsStepsPrefix  = "aerial.transcripts.steps."
	SubjectTranscriptsIndexed      = "aerial.turn.transcripts_indexed"
	SubjectAerialTurnCompleted     = "aerial.turn.completed"
	SubjectCovenExtractPrefix      = "aerial.coalesced.coven_extract."
	SubjectBuildWidgetSyncPrefix   = "aerial.coalesced.widget_sync.build."
	SubjectBuildWidgetSyncWildcard = SubjectBuildWidgetSyncPrefix + "*"
	SubjectProsperaReportJob       = "aerial.jobs.prospera.report"
	SubjectProsperaVoicemailJob    = "aerial.jobs.prospera.voicemail"
	SubjectProsperaSMSJob          = "aerial.jobs.prospera.sms"
	SubjectHaroOneShotJob          = "aerial.jobs.haro.oneshot"
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

// TranscriptsIndexedEvent represents transcript step-batch indexing completion
// on aerial.turn.transcripts_indexed. The indexer publishes one event per ingested
// batch; a duplicate Final batch re-publish is expected and safe.
type TranscriptsIndexedEvent struct {
	Agent        string    `json:"agent,omitempty"`
	Stack        string    `json:"stack"`
	SessionID    string    `json:"session_id"`
	ThreadID     string    `json:"thread_id,omitempty"`
	FileMtime    time.Time `json:"file_mtime"`
	FirstStep    int       `json:"first_step"`
	LastStep     int       `json:"last_step"`
	StepsIndexed int       `json:"steps_indexed"`
	Final        bool      `json:"final"`
	IndexedAt    time.Time `json:"indexed_at"`
}

// EffectiveAgent returns the active agent identifier, falling back to Stack or "aerial".
func (e TranscriptsIndexedEvent) EffectiveAgent() string {
	return strutil.FirstNonEmpty(e.Agent, e.Stack, defaultAgent)
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
	Agent     string `json:"agent,omitempty"`
	Stack     string `json:"stack,omitempty"`
	ThreadID  string `json:"thread_id"`
	SessionID string `json:"session_id,omitempty"`
}

// EffectiveAgent returns the active agent identifier, falling back to Stack or "aerial".
func (a CovenExtractJobArgs) EffectiveAgent() string {
	return strutil.FirstNonEmpty(a.Agent, a.Stack, defaultAgent)
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
		return unknownSubjectToken
	}
	t = strings.ReplaceAll(t, "*", "-")
	t = strings.ReplaceAll(t, ">", "-")
	t = strings.ReplaceAll(t, " ", "-")
	for strings.Contains(t, "--") {
		t = strings.ReplaceAll(t, "--", "-")
	}
	t = strings.Trim(t, ".-")
	if t == "" {
		return unknownSubjectToken
	}
	return t
}

func sanitizeToken(token string) string {
	t := cleanSubjectToken(token)
	t = strings.ReplaceAll(t, ".", "-")
	var b strings.Builder
	b.Grow(len(t))
	for i := range len(t) {
		c := t[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			b.WriteByte(c)
		} else {
			b.WriteByte('-')
		}
	}
	res := b.String()
	for strings.Contains(res, "--") {
		res = strings.ReplaceAll(res, "--", "-")
	}
	res = strings.Trim(res, "-_")
	if res == "" {
		return unknownSubjectToken
	}
	return res
}

// SubjectTranscriptsSteps returns the step-batch subject for a session's transcript.
func SubjectTranscriptsSteps(sessionID string) string {
	return SubjectTranscriptsStepsPrefix + sanitizeToken(sessionID)
}

// SubjectTranscriptsStepsForAgent returns the step-batch subject for a specific agent and session.
func SubjectTranscriptsStepsForAgent(agent, sessionID string) string {
	cleanAgent := sanitizeToken(agent)
	if cleanAgent == unknownSubjectToken {
		cleanAgent = defaultAgent
	}
	cleanSessionID := sanitizeToken(sessionID)
	return SubjectTranscriptsStepsPrefix + cleanAgent + "." + cleanSessionID
}

// ParseTranscriptsStepsSubject extracts the agent and session ID from an aerial transcripts steps subject.
// It handles 5-token "aerial.transcripts.steps.<agent>.<session_id>" (returning agent, sessionID, true)
// and legacy 4-token "aerial.transcripts.steps.<session_id>" (returning "", sessionID, true).
func ParseTranscriptsStepsSubject(subject string) (agent, sessionID string, ok bool) {
	tokens := strings.Split(subject, ".")
	if len(tokens) == 5 && tokens[0] == "aerial" && tokens[1] == "transcripts" && tokens[2] == "steps" {
		if tokens[3] == "" || tokens[4] == "" {
			return "", "", false
		}
		return tokens[3], tokens[4], true
	}
	if len(tokens) == 4 && tokens[0] == "aerial" && tokens[1] == "transcripts" && tokens[2] == "steps" {
		if tokens[3] == "" {
			return "", "", false
		}
		return "", tokens[3], true
	}
	return "", "", false
}

// SubjectCovenExtractCoalesced returns the coalesced subject for a thread's fact extraction.
func SubjectCovenExtractCoalesced(threadID string) string {
	t := cleanSubjectToken(threadID)
	t = strings.ReplaceAll(t, ".", "-")
	if t == "" {
		t = unknownSubjectToken
	}
	return SubjectCovenExtractPrefix + t
}

// SubjectCovenExtractCoalescedForAgent returns the coalesced extraction subject scoped by agent.
func SubjectCovenExtractCoalescedForAgent(agent, threadID string) string {
	a := sanitizeToken(agent)
	if a == unknownSubjectToken {
		a = defaultAgent
	}
	t := strings.TrimSpace(threadID)
	if t == ">" || t == "*" {
		return fmt.Sprintf("aerial.coalesced.coven_extract.%s.%s", a, t)
	}
	return fmt.Sprintf("aerial.coalesced.coven_extract.%s.%s", a, sanitizeToken(t))
}

// SubjectBuildWidgetSyncCoalesced returns the coalesced subject for build widget sync.
func SubjectBuildWidgetSyncCoalesced(buildID string) string {
	t := cleanSubjectToken(buildID)
	t = strings.ReplaceAll(t, ".", "-")
	if t == "" {
		t = unknownSubjectToken
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

// EnsureCoreStreams ensures the four core streams exist with their specified topologies.
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

// HealthChecker is an optional interface implemented by buses that can report
// whether their underlying connection is still usable.
type HealthChecker interface {
	Healthy() error
}

// CheckHealth reports the bus's connection health. Buses that do not implement
// HealthChecker are treated as healthy; a nil bus is unhealthy.
func CheckHealth(bus Bus) error {
	if bus == nil {
		return errors.New("pubsub: bus is nil")
	}
	if hc, ok := bus.(HealthChecker); ok {
		return hc.Healthy()
	}
	return nil
}

// ErrConnectionClosed is returned by Healthy once the NATS connection has
// closed for good and will never reconnect.
var ErrConnectionClosed = errors.New("pubsub: nats connection permanently closed")

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
		b.dispatch(ctx, m, "", handler)
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
		b.dispatch(ctx, m, durable, handler)
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
		if deleteConsumerErr := b.js.DeleteConsumer(stream, durable, opts...); deleteConsumerErr != nil && !errors.Is(deleteConsumerErr, nats.ErrConsumerNotFound) {
			return "", fmt.Errorf("delete incompatible consumer %s/%s: %w", stream, durable, deleteConsumerErr)
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

// dispatch runs handler for m. Deliveries keep the subscriber's context values but not its
// cancellation (a delivery is settled by ack/nak, not by the subscribe call's lifetime); the
// publisher's trace arrives through m's headers and parents a messaging process span that
// covers the handler and the settlement decision. group is the durable or queue name, empty
// for ephemeral subscriptions.
func (b *JetStreamBus) dispatch(subCtx context.Context, m *nats.Msg, group string, handler Handler) {
	ctx := context.WithoutCancel(subCtx)
	if m.Header != nil {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(http.Header(m.Header)))
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

	ctx, span := tracing.Tracer().Start(ctx, m.Subject+" process", processSpanOptions(m.Subject, group, wrapped.NumDelivered(), m.Header.Get(HeaderMsgID))...)
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
	done    <-chan struct{}
	spanCtx trace.SpanContext // process span; Snooze republishes as its child
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
	return strings.Contains(s, "message not found") ||
		strings.Contains(s, "10039") ||
		errors.Is(err, nats.ErrMsgNotFound)
}
