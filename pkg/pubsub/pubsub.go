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
	"strconv"
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
	SubjectAerialTurnPrefix        = "aerial.turn."
	SubjectAerialPRPrefix          = "aerial.pr."
	SubjectAerialCoalescedPrefix   = "aerial.coalesced."
	SubjectAerialTranscriptsPrefix = "aerial.transcripts."
	SubjectTranscriptsStepsPrefix  = "aerial.transcripts.steps."
	SubjectAerialTurnCompleted     = "aerial.turn.completed"
	SubjectCovenExtractPrefix      = "aerial.coalesced.coven_extract."
	SubjectBuildWidgetSyncPrefix   = "aerial.coalesced.widget_sync.build."
	SubjectProsperaReportJob       = "aerial.jobs.prospera.report"
	SubjectProsperaVoicemailJob    = "aerial.jobs.prospera.voicemail"
	SubjectProsperaSMSJob          = "aerial.jobs.prospera.sms"
	SubjectHaroOneShotJob          = "aerial.jobs.haro.oneshot"
)

// Well-known headers.
const (
	HeaderMsgID     = "Nats-Msg-Id"
	HeaderSnoozeSeq = "Nats-Snooze-Seq"
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

// errDurableRequired rejects ephemeral subscriptions: every consumer the bus
// creates is a named durable pull consumer.
var errDurableRequired = errors.New("pubsub: durable name required")

// Subscribe establishes a pub/sub fan-out subscription bound to the explicitly
// managed durable pull consumer named durable (see ensureDurableConsumer).
// Overlapping pods share it during rolling updates and no pod ever deletes it
// on Unsubscribe. An empty durable is rejected with errDurableRequired.
func (b *JetStreamBus) Subscribe(ctx context.Context, subject string, durable string, handler Handler) (Subscription, error) {
	if durable == "" {
		return nil, fmt.Errorf("subscribe to %s: %w", subject, errDurableRequired)
	}
	return b.bindDurable(ctx, subject, durable, handler)
}

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

	ctx, span := tracing.Tracer().Start(ctx, m.Subject()+" process", processSpanOptions(m.Subject(), group, wrapped.NumDelivered(), m.Headers().Get(HeaderMsgID))...)
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
