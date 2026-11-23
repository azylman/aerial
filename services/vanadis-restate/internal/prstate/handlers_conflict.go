package prstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
)

// Dark per-PR conflict check as a journaled GitHub effect.
//
// The conflict check reads and writes one PR's state, so it is PR-scoped:
// it runs against a PRStateStore (PRObject K/V in production), never from a
// RepoObject handler. GitHub check APIs expose no external idempotency key,
// so the at-least-once tolerance is: the effect key dedups re-fire within a
// generation; a new generation may re-fire the check once, which is safe
// because the check is read-only and the collaborator-conflict mark is
// overwritten per head SHA, never appended. Delayed rechecks go through
// ConflictRescheduler, which binds to a Restate delayed send. No wall-clock
// sleeps anywhere.

// ConflictRecheckDelay is the delay requested for a follow-up conflict check
// after a conflicted or failed check. It is passed to the Restate delayed
// send binding, never slept on.
const ConflictRecheckDelay = 5 * time.Minute

// PRStateStore is the per-PR state authority the conflict check runs on.
type PRStateStore interface {
	LoadPR(key string) (*PRState, bool)
	SavePR(state *PRState)
}

// ConflictChecker is the journaled GitHub conflict-check effect: it reports
// whether the PR head conflicts with current main. Production binds it to a
// journaled step keyed by the generation-scoped effect key, so replay
// returns the recorded result instead of re-firing.
type ConflictChecker interface {
	CheckConflict(ctx context.Context, repo string, prNumber int, headSHA string) (bool, error)
}

// ConflictRescheduler requests a delayed follow-up conflict check. The
// production binding is a Restate delayed send; the delay is data, not sleep.
type ConflictRescheduler interface {
	ScheduleConflictRecheck(ctx context.Context, delay time.Duration, ev PREvent) error
}

// ConflictHandlers runs the dark per-PR conflict check.
type ConflictHandlers struct {
	store      PRStateStore
	conflicts  ConflictChecker
	reschedule ConflictRescheduler
}

// errNilDependency is the cause a handler constructor returns when a
// required dependency is nil.
var errNilDependency = errors.New("prstate: required dependency is nil")

// NewConflictHandlers builds conflict handlers. Every dependency is
// required: a nil one returns errNilDependency.
func NewConflictHandlers(store PRStateStore, conflicts ConflictChecker, reschedule ConflictRescheduler) (*ConflictHandlers, error) {
	switch {
	case store == nil:
		return nil, fmt.Errorf("%w: conflict handlers PR store", errNilDependency)
	case conflicts == nil:
		return nil, fmt.Errorf("%w: conflict handlers conflict checker", errNilDependency)
	case reschedule == nil:
		return nil, fmt.Errorf("%w: conflict handlers rescheduler", errNilDependency)
	}
	return &ConflictHandlers{store: store, conflicts: conflicts, reschedule: reschedule}, nil
}

// ConflictOutcome is the pure routing result of one conflict check.
type ConflictOutcome struct {
	Disposition       EventDisposition
	Conflicted        bool
	AlreadyDispatched bool
	RecheckScheduled  bool
}

// HandleConflictCheck runs the journaled GitHub conflict-check effect for one
// PR. Stale head SHAs drop, terminal states absorb within their reopen epoch,
// and already-dispatched effect keys never re-fire. A conflicted head
// schedules the delayed recheck BEFORE persisting any state, so a scheduling
// failure leaves nothing behind and the retry re-fires the check instead of
// hitting AlreadyDispatched with the recheck permanently lost. A clean head
// clears the slot. A failed check schedules a delayed recheck and returns
// the error without marking the effect dispatched, so the retry re-fires
// exactly once per attempt.
func (h *ConflictHandlers) HandleConflictCheck(ctx context.Context, prKey string, ev PREvent) (ConflictOutcome, error) {
	if h == nil || h.store == nil {
		return ConflictOutcome{}, fmt.Errorf("conflict handlers not configured")
	}
	pr, ok := h.store.LoadPR(prKey)
	if !ok || pr == nil {
		return ConflictOutcome{}, fmt.Errorf("unknown pr key %q", strings.TrimSpace(prKey))
	}
	switch pr.ApplyEvent(ev) {
	case DispositionDropStale, DispositionRecoverStale:
		// ApplyEvent never recovers; only a synchronize does.
		return ConflictOutcome{Disposition: DispositionDropStale}, nil
	case DispositionAbsorbTerminal:
		return ConflictOutcome{Disposition: DispositionAbsorbTerminal}, nil
	case DispositionApply:
		// Proceed to the journaled effect below.
	}
	headSHA := strings.TrimSpace(ev.HeadSHA)
	if headSHA == "" || prwire.IsZeroSHA(headSHA) {
		return ConflictOutcome{Disposition: DispositionDropStale}, nil
	}
	key := pr.EffectKeyFor(prwire.EffectGitHubConflictCheck, headSHA, "", "")
	if pr.WasEffectDispatched(key) {
		return ConflictOutcome{Disposition: DispositionApply, AlreadyDispatched: true}, nil
	}
	conflicted, err := h.conflicts.CheckConflict(ctx, pr.Repo, pr.PRNumber, headSHA)
	if err != nil {
		delay := ConflictRecheckDelay
		if gerr, ok := errors.AsType[*GitHubError](err); ok && gerr.RateLimited && gerr.RetryAfter > delay {
			delay = gerr.RetryAfter
		}
		if schedErr := h.reschedule.ScheduleConflictRecheck(ctx, delay, ev); schedErr != nil {
			return ConflictOutcome{Disposition: DispositionApply}, fmt.Errorf("conflict check and recheck schedule failed: %w", errors.Join(err, schedErr))
		}
		return ConflictOutcome{Disposition: DispositionApply, RecheckScheduled: true}, err
	}
	if !conflicted {
		pr.MarkEffectDispatched(key)
		h.store.SavePR(pr)
		return ConflictOutcome{Disposition: DispositionApply}, nil
	}
	if err := h.reschedule.ScheduleConflictRecheck(ctx, ConflictRecheckDelay, ev); err != nil {
		return ConflictOutcome{Disposition: DispositionApply, Conflicted: true}, err
	}
	pr.MarkEffectDispatched(key)
	h.store.SavePR(pr)
	return ConflictOutcome{Disposition: DispositionApply, Conflicted: true, RecheckScheduled: true}, nil
}
