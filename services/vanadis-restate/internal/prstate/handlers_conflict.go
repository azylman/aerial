package prstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
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

type noopConflictChecker struct{}

func (noopConflictChecker) CheckConflict(context.Context, string, int, string) (bool, error) {
	return false, nil
}

type noopRescheduler struct{}

func (noopRescheduler) ScheduleConflictRecheck(context.Context, time.Duration, PREvent) error {
	return nil
}

// MemPRStore is an in-memory PRStateStore for hermetic tests. It stores
// JSON snapshots, so every load returns a fresh copy exactly like Restate
// K/V and an unsaved mutation never leaks into the store.
type MemPRStore struct {
	mu  sync.Mutex
	prs map[string][]byte
}

// NewMemPRStore returns an empty in-memory PR store.
func NewMemPRStore() *MemPRStore {
	return &MemPRStore{prs: make(map[string][]byte)}
}

// LoadPR returns a copy of the PR state for a repo#prNumber key.
func (s *MemPRStore) LoadPR(key string) (*PRState, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.prs[strings.TrimSpace(key)]
	if !ok {
		return nil, false
	}
	var st PRState
	//nolint:musttag // PRState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, false
	}
	return &st, true
}

// SavePR commits a snapshot of the PR state under its repo#prNumber key.
func (s *MemPRStore) SavePR(state *PRState) {
	if s == nil || state == nil {
		return
	}
	//nolint:musttag // PRState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prs[PRObjectKey(state.Repo, state.PRNumber)] = raw
}

// ConflictHandlers runs the dark per-PR conflict check.
type ConflictHandlers struct {
	store      PRStateStore
	conflicts  ConflictChecker
	reschedule ConflictRescheduler
}

// NewConflictHandlers builds dark conflict handlers; nil dependencies fall
// back to in-memory/no-op defaults so zero-value wiring never nil-panics.
func NewConflictHandlers(store PRStateStore, conflicts ConflictChecker, reschedule ConflictRescheduler) *ConflictHandlers {
	if store == nil {
		store = NewMemPRStore()
	}
	if conflicts == nil {
		conflicts = noopConflictChecker{}
	}
	if reschedule == nil {
		reschedule = noopRescheduler{}
	}
	return &ConflictHandlers{store: store, conflicts: conflicts, reschedule: reschedule}
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
	case DispositionDropStale:
		return ConflictOutcome{Disposition: DispositionDropStale}, nil
	case DispositionAbsorbTerminal:
		return ConflictOutcome{Disposition: DispositionAbsorbTerminal}, nil
	case DispositionApply:
		// Proceed to the journaled effect below.
	}
	headSHA := strings.TrimSpace(ev.HeadSHA)
	if headSHA == "" || IsZeroSHA(headSHA) {
		return ConflictOutcome{Disposition: DispositionDropStale}, nil
	}
	key := pr.EffectKeyFor(EffectGitHubUpdateBranch, headSHA, "", "")
	if pr.WasEffectDispatched(key) {
		return ConflictOutcome{Disposition: DispositionApply, AlreadyDispatched: true}, nil
	}
	conflicted, err := h.conflicts.CheckConflict(ctx, pr.Repo, pr.PRNumber, headSHA)
	if err != nil {
		if schedErr := h.reschedule.ScheduleConflictRecheck(ctx, ConflictRecheckDelay, ev); schedErr != nil {
			return ConflictOutcome{Disposition: DispositionApply}, fmt.Errorf("conflict check and recheck schedule failed: %w", errors.Join(err, schedErr))
		}
		return ConflictOutcome{Disposition: DispositionApply, RecheckScheduled: true}, err
	}
	if !conflicted {
		pr.MarkEffectDispatched(key)
		pr.ClearCollaboratorConflict(headSHA)
		h.store.SavePR(pr)
		return ConflictOutcome{Disposition: DispositionApply}, nil
	}
	if err := h.reschedule.ScheduleConflictRecheck(ctx, ConflictRecheckDelay, ev); err != nil {
		return ConflictOutcome{Disposition: DispositionApply, Conflicted: true}, err
	}
	pr.MarkCollaboratorConflict(headSHA)
	pr.MarkEffectDispatched(key)
	h.store.SavePR(pr)
	return ConflictOutcome{Disposition: DispositionApply, Conflicted: true, RecheckScheduled: true}, nil
}
