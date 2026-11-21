package prstate

// Shared execution core for restate-owned effects (eri wakes and DMs,
// lifecycle facts): one journaled restate.Run per effect, and the delayed
// retry that re-executes it against current state.

import (
	"context"
	"errors"
	"log/slog"

	restate "github.com/restatedev/sdk-go"
)

// plannedEffect is one bound effect: its generation-scoped key, the owner
// it fires for, the journal step name, and the call that performs it (nil
// when the effect's boundary is unconfigured).
type plannedEffect struct {
	key   EffectKey
	agent string
	name  string
	do    func(context.Context) error
}

var errEffectUnconfigured = errors.New("prstate: effect boundary not configured")

// executeEffect runs p as one journaled restate.Run and reports success. It
// fails fast with one attempt so the exclusive PR lock is never held across
// backoff; callers schedule a delayed retry on failure.
func executeEffect(ctx restate.ObjectContext, p plannedEffect) bool {
	pr := PRObjectKey(p.key.Repo, p.key.PRNumber)
	_, err := restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		if p.do == nil {
			effectsTotal.WithLabelValues(p.key.Kind, effectResultUnconfigured).Inc()
			return restate.Void{}, restate.ToTerminalError(errEffectUnconfigured)
		}
		if err := p.do(rc); err != nil {
			effectsTotal.WithLabelValues(p.key.Kind, effectResultError).Inc()
			return restate.Void{}, err
		}
		effectsTotal.WithLabelValues(p.key.Kind, effectResultOK).Inc()
		slog.InfoContext(rc, "[prstate] effect executed", "pr", pr, "kind", p.key.Kind,
			"agent", p.agent, "effect_key", p.key.String(), "match_key", p.key.MatchKey())
		return restate.Void{}, nil
	}, restate.WithName(p.name), restate.WithMaxRetryAttempts(eriEffectRunAttempts))
	if err != nil {
		slog.ErrorContext(ctx, "[prstate] effect execution failed", "pr", pr, "kind", p.key.Kind,
			"agent", p.agent, "effect_key", p.key.String(), "error", err)
		return false
	}
	return true
}

// loadRetryState loads the PR for a delayed retry, returning nil when the
// PR is gone or its gate generation moved on (a new generation re-fires its
// own effects).
func (s *PRObjectService) loadRetryState(ctx restate.ObjectContext, generation uint64) (*PRState, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil || st == nil || st.GateGeneration != generation {
		return nil, err
	}
	return st, nil
}

// retryPlanned re-executes one usable, unfired effect: success commits its
// key, failure calls reschedule.
func (s *PRObjectService) retryPlanned(ctx restate.ObjectContext, st *PRState, p plannedEffect, usable bool, reschedule func()) error {
	if !usable || st.WasEffectDispatched(p.key) {
		return nil
	}
	if !executeEffect(ctx, p) {
		reschedule()
		return nil
	}
	st.MarkEffectDispatched(p.key)
	restate.Set(ctx, s.stateKey, st)
	return nil
}
