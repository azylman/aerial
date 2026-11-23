package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/strutil"
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
	SubjectVoiceStreamPrefix       = "voice.stream."
	SubjectAerialTurnCompleted     = "aerial.turn.completed"
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

// HealthChecker is an optional interface implemented by buses that can report
// whether their underlying connection is still usable.
type HealthChecker interface {
	Healthy() error
}

// EnsureCoreStreams ensures the four core streams exist with their specified topologies.
func EnsureCoreStreams(ctx context.Context, bus Bus) error {
	if bus == nil {
		return errors.New("pubsub: bus is nil")
	}
	return bus.EnsureCoreStreams(ctx)
}

// ErrConnectionClosed is returned by Healthy once the NATS connection has
// closed for good and will never reconnect.
var ErrConnectionClosed = errors.New("pubsub: nats connection permanently closed")

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

func isCoreSubject(subject string) bool {
	return strings.HasPrefix(strings.TrimSpace(subject), SubjectVoiceStreamPrefix)
}

// SanitizeSubjectToken makes an untrusted value safe as one NATS subject
// token: it trims surrounding space and maps every rune outside
// [A-Za-z0-9_-] to '_'. A result that is empty or only underscores becomes
// "unknown".
func SanitizeSubjectToken(token string) string {
	clean := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, strings.TrimSpace(token))
	if strings.Trim(clean, "_") == "" {
		return "unknown"
	}
	return clean
}

func cleanSubjectToken(token string) string {
	t := strings.TrimSpace(token)
	if t == "" {
		return unknownSubjectToken
	}
	for _, old := range []string{"*", ">", " "} {
		t = strings.ReplaceAll(t, old, "-")
	}
	for strings.Contains(t, "--") {
		t = strings.ReplaceAll(t, "--", "-")
	}
	if t = strings.Trim(t, ".-"); t == "" {
		return unknownSubjectToken
	}
	return t
}

func sanitizeToken(token string) string {
	t := strings.ReplaceAll(cleanSubjectToken(token), ".", "-")
	var b strings.Builder
	b.Grow(len(t))
	for i := range len(t) {
		if c := t[i]; (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			b.WriteByte(c)
		} else {
			b.WriteByte('-')
		}
	}
	res := b.String()
	for strings.Contains(res, "--") {
		res = strings.ReplaceAll(res, "--", "-")
	}
	if res = strings.Trim(res, "-_"); res == "" {
		return unknownSubjectToken
	}
	return res
}

// SubjectTranscriptsStepsForAgent returns the step-batch subject for a specific agent and session.
func SubjectTranscriptsStepsForAgent(agent, sessionID string) string {
	cleanAgent := sanitizeToken(agent)
	if cleanAgent == unknownSubjectToken {
		cleanAgent = defaultAgent
	}
	return SubjectTranscriptsStepsPrefix + cleanAgent + "." + sanitizeToken(sessionID)
}

// ParseTranscriptsStepsSubject extracts the agent and session ID from an aerial transcripts steps subject.
// It handles 5-token "aerial.transcripts.steps.<agent>.<session_id>" (returning agent, sessionID, true)
// and legacy 4-token "aerial.transcripts.steps.<session_id>" (returning "", sessionID, true).
func ParseTranscriptsStepsSubject(subject string) (agent, sessionID string, ok bool) {
	tokens := strings.Split(subject, ".")
	if len(tokens) < 4 || len(tokens) > 5 || tokens[0] != "aerial" || tokens[1] != "transcripts" || tokens[2] != "steps" {
		return "", "", false
	}
	if len(tokens) == 5 {
		if tokens[3] == "" || tokens[4] == "" {
			return "", "", false
		}
		return tokens[3], tokens[4], true
	}
	if tokens[3] == "" {
		return "", "", false
	}
	return "", tokens[3], true
}

// SubjectCovenExtractCoalescedForAgent returns the coalesced extraction subject scoped by agent.
func SubjectCovenExtractCoalescedForAgent(agent, threadID string) string {
	a := sanitizeToken(agent)
	if a == unknownSubjectToken {
		a = defaultAgent
	}
	t := strings.TrimSpace(threadID)
	if t != ">" && t != "*" {
		t = sanitizeToken(t)
	}
	return fmt.Sprintf("aerial.coalesced.coven_extract.%s.%s", a, t)
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

// NakBackoff returns the redelivery delay for a message that failed on its
// numDelivered-th delivery: 1s for the first delivery, then doubling
// (1s<<(numDelivered-1)), clamped to maxDelay.
func NakBackoff(numDelivered uint64, maxDelay time.Duration) time.Duration {
	delay := time.Second
	if numDelivered > 1 {
		delay = time.Second << min(numDelivered-1, 30)
	}
	return min(delay, maxDelay)
}
