package prstate

// aerial.pr.> lifecycle facts executed by PRObject.
//
// Synapse renders each fact from stateless inputs (target, rollout-state
// snapshot, quiet flag, details) and attaches it to the PREvent it already
// routes (PREvent.Facts). PRObject decides whether it fires: verdict
// handlers (HandleConflict, HandleDeploySuccess, HandleCIRecovered) commit
// their decision first, then run the facts; the deploy-failure deadline
// commits deploy_failed, then runs the fact captured with the attempt.
//
// For every fact whose generation-scoped EffectKey has not fired:
//
//   - effects_owner=restate: one journaled restate.Run publishes the fact on
//     aerial.pr.<event> with a deterministic Nats-Msg-Id derived from the
//     EffectKey, so JetStream deduplicates a resend. The BuildRouter route
//     rides the same publish: vanadis-nats forwards aerial.pr.> to
//     BuildRouter.RoutePREvent keyed by that Nats-Msg-Id, so the route is
//     deduplicated end to end and a second direct route would double-deliver.
//     The Run fails fast (one attempt) so the exclusive PR lock is never held
//     across backoff; a failure schedules a delayed RetryFactEffect.
//   - effects_owner=legacy: synapse published it. PRObject commits the key,
//     records an IntendedEffect, bumps prstate_intended_effects_total{kind},
//     and logs [prstate-shadow] with the match key synapse logs as
//     [synapse-effect].

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// FactEffectPrefix prefixes every lifecycle-fact effect kind:
// pr_fact_<event>, for example pr_fact_conflict.
const FactEffectPrefix = "pr_fact_"

// PRHandlerRetryFactEffect is the delayed self-send that retries one
// restate-owned fact publish.
const PRHandlerRetryFactEffect = "RetryFactEffect"

// DefaultFactRetryDelay spaces delayed fact re-publishes.
const DefaultFactRetryDelay = time.Minute

// maxFactRetries bounds delayed fact re-publishes before the fact drops.
const maxFactRetries = 10

// FactEffectKind is the effect kind for a lifecycle fact event.
func FactEffectKind(event string) string {
	return FactEffectPrefix + strings.ToLower(strings.TrimSpace(event))
}

// FactPublisher publishes one lifecycle fact on aerial.pr.<event> with the
// given JetStream message ID.
type FactPublisher interface {
	PublishFact(ctx context.Context, msgID string, fact pubsub.PRLifecycleEvent) error
}

// BusFactPublisher is the production FactPublisher over the JetStream bus.
type BusFactPublisher struct {
	bus pubsub.Bus
}

// NewBusFactPublisher builds the production FactPublisher.
func NewBusFactPublisher(bus pubsub.Bus) *BusFactPublisher {
	return &BusFactPublisher{bus: bus}
}

var errFactPublisherUnconfigured = errors.New("prstate: no fact publisher configured")

// PublishFact publishes fact on aerial.pr.<event>.
func (p *BusFactPublisher) PublishFact(ctx context.Context, msgID string, fact pubsub.PRLifecycleEvent) error {
	if p == nil || p.bus == nil {
		return errFactPublisherUnconfigured
	}
	return p.bus.Publish(ctx, pubsub.SubjectAerialPRPrefix+strings.TrimSpace(fact.Event), msgID, fact)
}

// WithFactPublisher injects the fact boundary and returns s. Without it
// restate-owned facts count as unconfigured and schedule retries.
func (s *PRObjectService) WithFactPublisher(p FactPublisher) *PRObjectService {
	s.facts = p
	return s
}

// PlannedFact is one normalized fact PRObject will publish or record.
type PlannedFact struct {
	Fact  pubsub.PRLifecycleEvent
	Key   EffectKey
	Agent string
}

// normalizeFact pins the fact to the PR it is addressed to and rejects facts
// with no event name.
func normalizeFact(st *PRState, f pubsub.PRLifecycleEvent) (pubsub.PRLifecycleEvent, bool) {
	f.Event = strings.ToLower(strings.TrimSpace(f.Event))
	if f.Event == "" {
		return pubsub.PRLifecycleEvent{}, false
	}
	f.Repo = st.Repo
	f.PRNumber = st.PRNumber
	return f, true
}

// FactEffectKey keys one fact on its head SHA, falling back to its merge SHA
// and then the stored head, plus its job and check names. The synapse
// restateclient FactMatchKey renders the same generation-free form.
func FactEffectKey(st *PRState, f pubsub.PRLifecycleEvent) EffectKey {
	head := usableHeadSHA(f.HeadSHA)
	if head == "" {
		head = usableHeadSHA(f.MergeSHA)
	}
	if head == "" && st != nil {
		head = usableHeadSHA(st.HeadSHA)
	}
	return st.EffectKeyFor(FactEffectKind(f.Event), head, f.JobName, f.CheckName)
}

// PlanFacts returns the facts that have not fired in the current gate
// generation, addressed to the stored owner. Duplicate keys collapse to the
// first fact.
func PlanFacts(st *PRState, facts []pubsub.PRLifecycleEvent) []PlannedFact {
	if st == nil || len(facts) == 0 {
		return nil
	}
	var out []PlannedFact
	seen := make(map[string]bool, len(facts))
	for _, raw := range facts {
		f, ok := normalizeFact(st, raw)
		if !ok {
			continue
		}
		key := FactEffectKey(st, f)
		ks := key.String()
		if seen[ks] || st.WasEffectDispatched(key) {
			continue
		}
		seen[ks] = true
		out = append(out, PlannedFact{Fact: f, Key: key, Agent: st.OwnerAgent()})
	}
	return out
}

// FactDigest fingerprints the rendered fact so gates can tell a matching
// key with a different body apart. OccurredAt is excluded: synapse stamps it
// per delivery.
func FactDigest(f pubsub.PRLifecycleEvent) string {
	return digestStrings(f.Event, f.Branch, f.HeadSHA, f.MergeSHA, f.TargetID, f.Status, f.CheckName, f.JobName, f.Details)
}

// runFacts publishes or records every planned fact and persists the
// committed effect keys. owner is the event's effects owner.
func (s *PRObjectService) runFacts(ctx restate.ObjectContext, st *PRState, owner EffectsOwner, facts []pubsub.PRLifecycleEvent) {
	planned := PlanFacts(st, facts)
	if len(planned) == 0 {
		return
	}
	for _, p := range planned {
		if ParseEffectsOwner(string(owner)) == EffectsOwnerRestate {
			if !s.publishFact(ctx, p) {
				s.scheduleFactRetry(ctx, FactEffectRetry{Fact: p.Fact, Generation: p.Key.GateGeneration})
				continue
			}
		} else {
			intended := IntendedEffect{Kind: p.Key.Kind, Agent: p.Agent, EffectKey: p.Key.String(), MatchKey: p.Key.MatchKey(), Digest: FactDigest(p.Fact)}
			recordIntendedEffect(ctx, intended)
			st.RecordIntendedEffect(intended)
		}
		st.MarkEffectDispatched(p.Key)
	}
	restate.Set(ctx, s.stateKey, st)
}

// recordIntendedEffect journals the intended-effect metric and log once.
func recordIntendedEffect(ctx restate.ObjectContext, e IntendedEffect) {
	pr := restate.Key(ctx)
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		intendedEffectsTotal.WithLabelValues(e.Kind).Inc()
		slog.InfoContext(rc, "[prstate-shadow] intended effect recorded (synapse owns effects)",
			"pr", pr, "kind", e.Kind, "agent", e.Agent, "effect_key", e.EffectKey,
			"match_key", e.MatchKey, "digest", e.Digest)
		return restate.Void{}, nil
	}, restate.WithName("record-intended-"+e.Kind))
}

// factPlanned binds one fact to the configured publisher. The JetStream
// message ID is the EffectKey-derived idempotency key.
func (s *PRObjectService) factPlanned(p PlannedFact) plannedEffect {
	out := plannedEffect{key: p.Key, agent: p.Agent, name: "fact-" + p.Key.Kind}
	if pub := s.facts; pub != nil {
		out.do = func(ctx context.Context) error { return pub.PublishFact(ctx, EffectIdempotencyKey(p.Key), p.Fact) }
	}
	return out
}

// publishFact runs one journaled publish and reports success.
func (s *PRObjectService) publishFact(ctx restate.ObjectContext, p PlannedFact) bool {
	return executeEffect(ctx, s.factPlanned(p))
}

// FactEffectRetry is the delayed RetryFactEffect input.
type FactEffectRetry struct {
	Fact       pubsub.PRLifecycleEvent `json:"fact"`
	Generation uint64                  `json:"generation"`
	Attempt    int                     `json:"attempt"`
}

// scheduleFactRetry sends a delayed RetryFactEffect, or drops the fact once
// maxFactRetries delayed re-publishes failed.
func (s *PRObjectService) scheduleFactRetry(ctx restate.ObjectContext, req FactEffectRetry) {
	kind := FactEffectKind(req.Fact.Event)
	pr := restate.Key(ctx)
	if req.Attempt >= maxFactRetries {
		_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
			effectsTotal.WithLabelValues(kind, effectResultDropped).Inc()
			slog.ErrorContext(rc, "[prstate] lifecycle fact dropped after retries", "pr", pr, "kind", kind, "attempts", req.Attempt)
			return restate.Void{}, nil
		}, restate.WithName("drop-fact-"+kind))
		return
	}
	_, _ = restate.Run(ctx, func(restate.RunContext) (restate.Void, error) {
		effectsTotal.WithLabelValues(kind, effectResultRetry).Inc()
		return restate.Void{}, nil
	}, restate.WithName("retry-fact-"+kind))
	next := req
	next.Attempt++
	restate.ObjectSend(ctx, PRObjectServiceName, pr, PRHandlerRetryFactEffect).Send(next, restate.WithDelay(s.factRetryDelay))
}

// RetryFactEffect re-publishes one restate-owned fact. It drops the retry
// when the PR is gone, the gate generation moved on, or the key fired.
func (s *PRObjectService) RetryFactEffect(ctx restate.ObjectContext, req FactEffectRetry) error {
	st, err := s.loadRetryState(ctx, req.Generation)
	if err != nil || st == nil {
		return err
	}
	f, ok := normalizeFact(st, req.Fact)
	p := s.factPlanned(PlannedFact{Fact: f, Key: FactEffectKey(st, f), Agent: st.OwnerAgent()})
	return s.retryPlanned(ctx, st, p, ok, func() { s.scheduleFactRetry(ctx, req) })
}
