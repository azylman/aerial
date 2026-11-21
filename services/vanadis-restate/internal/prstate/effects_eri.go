package prstate

// Eri-bound effects executed by PRObject: the owning agent's prompt turn,
// headless background dispatch, and plain direct message (synapse's
// DispatchPrompt, DispatchBackground, and DispatchDirectMessage).
//
// Synapse renders each effect from stateless inputs and attaches it to the
// PREvent it already routes (PREvent.EriEffects). PRObject applies the state
// transition first, then for every rendered effect whose generation-scoped
// EffectKey has not fired:
//
//   - effects_owner=restate: one journaled restate.Run attempts delivery to
//     the stored owner through the Restate session path (EriEffector). It
//     fails fast with 1 attempt to avoid holding the exclusive PR object lock
//     across backoff sleeps. Success commits the effect key; failure
//     schedules a delayed RetryEriEffect self-send instead of failing the
//     invocation, so an eri outage never blocks later transitions for the PR.
//   - effects_owner=legacy: synapse already performed it. PRObject commits
//     the effect key, appends an IntendedEffect to the bounded per-PR ring,
//     bumps prstate_intended_effects_total{kind}, and logs [prstate-shadow]
//     with the generation-free match key synapse logs as [synapse-effect].
//
// Idempotency: prompts and background dispatches carry a deterministic
// Idempotency-Key / dispatch ID derived from the EffectKey, so Restate
// ingress deduplicates a resend. Discord direct messages accept no external
// key: the journaled Run never re-sends after a recorded success, and only a
// crash between the Discord send and the journal ack can duplicate one DM
// (accepted at-least-once tolerance).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

// MaxIntendedEffects bounds the per-PR intended-effect ring.
const MaxIntendedEffects = 32

// PRHandlerRetryEriEffect is the delayed self-send that retries one
// restate-owned eri effect whose delivery exhausted its journaled retries.
const PRHandlerRetryEriEffect = "RetryEriEffect"

// Eri effect retry policy: fail fast with 1 attempt inside the exclusive
// PRObject handler, delayed re-deliveries after that, and the delay between them.
const (
	eriEffectRunAttempts       = 1
	maxEriEffectRetries        = 6
	DefaultEriEffectRetryDelay = 2 * time.Minute
)

// eriEffectSource is the dispatch source eri records for prstate effects.
const eriEffectSource = "prstate"

// metricLabelResult is the outcome label on prstate counters.
const metricLabelResult = "result"

// Effect execution outcomes for prstate_effects_total.
const (
	effectResultOK           = "ok"
	effectResultError        = "error"
	effectResultUnconfigured = "unconfigured"
	effectResultRetry        = "retry_scheduled"
	effectResultDropped      = "dropped"
)

var (
	intendedEffectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "prstate_intended_effects_total",
		Help: "Effects PRObject recorded but skipped because synapse owned the event (effects_owner=legacy), by kind.",
	}, []string{"kind"})
	effectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "prstate_effects_total",
		Help: "Restate-owned effects PRObject executed, by kind and result.",
	}, []string{"kind", metricLabelResult})
)

func init() {
	registerCollector(intendedEffectsTotal)
	registerCollector(effectsTotal)
}

// EriEffector delivers one eri-bound effect to agent through its Restate
// session path. key is the generation-scoped EffectKey the effect fires
// under; implementations derive their idempotency key from it.
type EriEffector interface {
	DeliverEri(ctx context.Context, agent string, key EffectKey, eff EriEffect) error
}

// IntendedEffect is one effect PRObject recorded for a legacy-owned event.
type IntendedEffect struct {
	Kind      string
	Agent     string
	EffectKey string
	MatchKey  string
	Digest    string
}

// PlannedEriEffect is one normalized effect PRObject will fire or record.
type PlannedEriEffect struct {
	Effect EriEffect
	Key    EffectKey
	Agent  string
}

// isEriEffectKind reports whether kind is an eri-bound effect kind.
func isEriEffectKind(kind string) bool {
	switch kind {
	case EffectEriPrompt, EffectEriBackground, EffectEriDirectMessage:
		return true
	default:
		return false
	}
}

// normalizeEriEffect trims eff, fills an empty channel from the PR target,
// and rejects unknown kinds, empty content, and prompts or direct messages
// with no channel to land in. Background dispatches may run headless.
func normalizeEriEffect(st *PRState, eff EriEffect) (EriEffect, bool) {
	out := EriEffect{
		Kind:         strings.TrimSpace(eff.Kind),
		ChannelID:    strings.TrimSpace(eff.ChannelID),
		Content:      strings.TrimSpace(eff.Content),
		Effort:       strings.TrimSpace(eff.Effort),
		SkipIfActive: eff.SkipIfActive,
	}
	if !isEriEffectKind(out.Kind) || out.Content == "" {
		return EriEffect{}, false
	}
	if out.ChannelID == "" && st != nil {
		out.ChannelID = strings.TrimSpace(st.TargetID)
	}
	if out.ChannelID == "" && out.Kind != EffectEriBackground {
		return EriEffect{}, false
	}
	return out, true
}

// PlanEriEffects returns the effects ev carries that have not fired in the
// current gate generation, keyed on the event head SHA (falling back to the
// stored head), job, and check name, and addressed to the stored owner. Two
// effects with the same key in one event collapse to the first.
func PlanEriEffects(st *PRState, ev PREvent) []PlannedEriEffect {
	if st == nil || len(ev.EriEffects) == 0 {
		return nil
	}
	head := usableHeadSHA(ev.HeadSHA)
	if head == "" {
		head = usableHeadSHA(st.HeadSHA)
	}
	var out []PlannedEriEffect
	seen := make(map[string]bool, len(ev.EriEffects))
	for _, raw := range ev.EriEffects {
		eff, ok := normalizeEriEffect(st, raw)
		if !ok {
			continue
		}
		key := st.EffectKeyFor(eff.Kind, head, ev.JobName, ev.CheckName)
		ks := key.String()
		if seen[ks] || st.WasEffectDispatched(key) {
			continue
		}
		seen[ks] = true
		out = append(out, PlannedEriEffect{Effect: eff, Key: key, Agent: st.OwnerAgent()})
	}
	return out
}

// EriEffectDigest fingerprints the rendered payload so gates can tell a
// matching key with a different body apart.
func EriEffectDigest(eff EriEffect) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		eff.Kind, eff.ChannelID, eff.Content, eff.Effort, strconv.FormatBool(eff.SkipIfActive),
	}, "\x00")))
	return hex.EncodeToString(sum[:8])
}

// EffectIdempotencyKey is the deterministic Restate Idempotency-Key and
// dispatch ID for one effect: a bounded hash of the generation-scoped key.
func EffectIdempotencyKey(key EffectKey) string {
	sum := sha256.Sum256([]byte(key.String()))
	return "prstate-" + hex.EncodeToString(sum[:16])
}

// RecordIntendedEffect appends one intended effect, keeping the newest
// MaxIntendedEffects entries.
func (s *PRState) RecordIntendedEffect(e IntendedEffect) {
	if s == nil {
		return
	}
	s.IntendedEffects = append(s.IntendedEffects, e)
	if over := len(s.IntendedEffects) - MaxIntendedEffects; over > 0 {
		s.IntendedEffects = append([]IntendedEffect(nil), s.IntendedEffects[over:]...)
	}
}

func intendedEffectFor(p PlannedEriEffect) IntendedEffect {
	return IntendedEffect{
		Kind:      p.Effect.Kind,
		Agent:     p.Agent,
		EffectKey: p.Key.String(),
		MatchKey:  p.Key.MatchKey(),
		Digest:    EriEffectDigest(p.Effect),
	}
}

// WithEriEffector injects the eri effect boundary and returns s. Without it
// restate-owned eri effects count as unconfigured and schedule retries.
func (s *PRObjectService) WithEriEffector(e EriEffector) *PRObjectService {
	s.eri = e
	return s
}

// runEriEffects fires or records every planned eri effect for one applied
// event and persists the committed effect keys.
func (s *PRObjectService) runEriEffects(ctx restate.ObjectContext, st *PRState, ev PREvent) {
	planned := PlanEriEffects(st, ev)
	if len(planned) == 0 {
		return
	}
	owner := ParseEffectsOwner(string(ev.EffectsOwner))
	for _, p := range planned {
		if owner == EffectsOwnerRestate {
			if !s.deliverEriEffect(ctx, p) {
				s.scheduleEriRetry(ctx, EriEffectRetry{Effect: p.Effect, HeadSHA: p.Key.HeadSHA, JobName: p.Key.JobName, CheckName: p.Key.CheckName, Generation: p.Key.GateGeneration})
				continue
			}
		} else {
			recordIntended(ctx, p)
			st.RecordIntendedEffect(intendedEffectFor(p))
		}
		st.MarkEffectDispatched(p.Key)
	}
	restate.Set(ctx, s.stateKey, st)
}

// recordIntended journals the intended-effect metric and log once, so a
// handler replay never double counts what the shadow gate compares.
func recordIntended(ctx restate.ObjectContext, p PlannedEriEffect) {
	pr := PRObjectKey(p.Key.Repo, p.Key.PRNumber)
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		intendedEffectsTotal.WithLabelValues(p.Effect.Kind).Inc()
		slog.InfoContext(rc, "[prstate-shadow] intended effect recorded (synapse owns effects)",
			"pr", pr, "kind", p.Effect.Kind, "agent", p.Agent, "effect_key", p.Key.String(),
			"match_key", p.Key.MatchKey(), "digest", EriEffectDigest(p.Effect))
		return restate.Void{}, nil
	}, restate.WithName("record-intended-"+p.Effect.Kind))
}

// deliverEriEffect runs one journaled delivery and reports success.
func (s *PRObjectService) deliverEriEffect(ctx restate.ObjectContext, p PlannedEriEffect) bool {
	pr := PRObjectKey(p.Key.Repo, p.Key.PRNumber)
	eri := s.eri
	_, err := restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		if eri == nil {
			effectsTotal.WithLabelValues(p.Effect.Kind, effectResultUnconfigured).Inc()
			return restate.Void{}, restate.ToTerminalError(errEriEffectorUnconfigured)
		}
		if err := eri.DeliverEri(rc, p.Agent, p.Key, p.Effect); err != nil {
			effectsTotal.WithLabelValues(p.Effect.Kind, effectResultError).Inc()
			return restate.Void{}, err
		}
		effectsTotal.WithLabelValues(p.Effect.Kind, effectResultOK).Inc()
		slog.InfoContext(rc, "[prstate] eri effect delivered", "pr", pr, "kind", p.Effect.Kind,
			"agent", p.Agent, "effect_key", p.Key.String(), "match_key", p.Key.MatchKey())
		return restate.Void{}, nil
	}, restate.WithName("eri-effect-"+p.Effect.Kind), restate.WithMaxRetryAttempts(eriEffectRunAttempts))
	if err != nil {
		slog.ErrorContext(ctx, "[prstate] eri effect delivery failed", "pr", pr, "kind", p.Effect.Kind,
			"agent", p.Agent, "effect_key", p.Key.String(), "error", err)
		return false
	}
	return true
}

var errEriEffectorUnconfigured = errors.New("prstate: no eri effector configured")

// EriEffectRetry is the delayed RetryEriEffect input: the rendered effect,
// the key inputs it fired under, and the retry ordinal.
type EriEffectRetry struct {
	Effect     EriEffect `json:"effect"`
	HeadSHA    string    `json:"head_sha"`
	JobName    string    `json:"job_name,omitempty"`
	CheckName  string    `json:"check_name,omitempty"`
	Generation uint64    `json:"generation"`
	Attempt    int       `json:"attempt"`
}

// scheduleEriRetry sends a delayed RetryEriEffect for req, or drops the
// effect once maxEriEffectRetries delayed re-deliveries failed.
func (s *PRObjectService) scheduleEriRetry(ctx restate.ObjectContext, req EriEffectRetry) {
	kind := req.Effect.Kind
	pr := restate.Key(ctx)
	if req.Attempt >= maxEriEffectRetries {
		_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
			effectsTotal.WithLabelValues(kind, effectResultDropped).Inc()
			slog.ErrorContext(rc, "[prstate] eri effect dropped after retries", "pr", pr, "kind", kind, "attempts", req.Attempt)
			return restate.Void{}, nil
		}, restate.WithName("drop-eri-effect-"+kind))
		return
	}
	_, _ = restate.Run(ctx, func(restate.RunContext) (restate.Void, error) {
		effectsTotal.WithLabelValues(kind, effectResultRetry).Inc()
		return restate.Void{}, nil
	}, restate.WithName("retry-eri-effect-"+kind))
	next := req
	next.Attempt++
	restate.ObjectSend(ctx, PRObjectServiceName, pr, PRHandlerRetryEriEffect).Send(next, restate.WithDelay(s.eriRetryDelay))
}

// RetryEriEffect re-delivers one restate-owned eri effect to the current
// owner. It drops the retry when the PR is gone, the gate generation moved
// on (a new generation re-fires its own effects), or the key already fired.
func (s *PRObjectService) RetryEriEffect(ctx restate.ObjectContext, req EriEffectRetry) error {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return err
	}
	if st == nil || st.GateGeneration != req.Generation {
		return nil
	}
	eff, ok := normalizeEriEffect(st, req.Effect)
	if !ok {
		return nil
	}
	key := st.EffectKeyFor(eff.Kind, req.HeadSHA, req.JobName, req.CheckName)
	if st.WasEffectDispatched(key) {
		return nil
	}
	if !s.deliverEriEffect(ctx, PlannedEriEffect{Effect: eff, Key: key, Agent: st.OwnerAgent()}) {
		s.scheduleEriRetry(ctx, req)
		return nil
	}
	st.MarkEffectDispatched(key)
	restate.Set(ctx, s.stateKey, st)
	return nil
}

// EriSessionClient is the Restate session surface SessionEriEffector drives:
// agent-scoped ThreadSession prompts, DispatchSession background turns, and
// the agent-routed Discord direct message. None of them fall back to eri
// HTTP.
type EriSessionClient interface {
	PromptAgent(ctx context.Context, agent, threadID, prompt, idempotencyKey string) error
	DispatchRestate(ctx context.Context, req ericlient.BrainDispatchRequest) error
	MessageAgent(ctx context.Context, agent, channelID, content string) error
}

// SessionEriEffector is the production EriEffector over ericlient.
type SessionEriEffector struct {
	client EriSessionClient
}

// NewSessionEriEffector builds the production EriEffector.
func NewSessionEriEffector(client EriSessionClient) *SessionEriEffector {
	return &SessionEriEffector{client: client}
}

// DeliverEri routes one effect to agent's session path. A skip-if-active
// background dispatch that finds the session busy counts as delivered,
// matching eri's own skip semantics.
func (e *SessionEriEffector) DeliverEri(ctx context.Context, agent string, key EffectKey, eff EriEffect) error {
	if e == nil || e.client == nil {
		return errEriEffectorUnconfigured
	}
	switch eff.Kind {
	case EffectEriPrompt:
		return e.client.PromptAgent(ctx, agent, eff.ChannelID, eff.Content, EffectIdempotencyKey(key))
	case EffectEriBackground:
		err := e.client.DispatchRestate(ctx, ericlient.BrainDispatchRequest{
			ID:           EffectIdempotencyKey(key),
			Agent:        agent,
			Source:       eriEffectSource,
			TargetID:     eff.ChannelID,
			Prompt:       eff.Content,
			Effort:       eff.Effort,
			SkipIfActive: eff.SkipIfActive,
		})
		if errors.Is(err, ericlient.ErrDispatchActive) {
			return nil
		}
		return err
	case EffectEriDirectMessage:
		return e.client.MessageAgent(ctx, agent, eff.ChannelID, eff.Content)
	default:
		return restate.ToTerminalError(fmt.Errorf("prstate: unknown eri effect kind %q", eff.Kind))
	}
}
