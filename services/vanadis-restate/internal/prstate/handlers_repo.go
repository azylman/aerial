package prstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Dark RepoObject handlers: main-push SHA recording, updated-for-main
// tracking, and conflict checks as journaled GitHub effects.
//
// Authority boundaries (frozen, see contracts.go):
//   - Restate K/V in RepoObject is the only writer; pr_registry is a
//     read-only projection synced by a one-way journaled send AFTER the state
//     commit. Projection failure never discards or gates the transition.
//   - Every external effect is keyed generation-scoped
//     (repo+PR+headSHA+gateGeneration+effectKind). Keys bump/clear only when a
//     new gate generation starts; HandleMainPush and readiness paths never
//     mint generations or clear effect keys.
//   - Events carrying stale head SHAs drop; terminal states absorb within
//     their reopen epoch, so a reopen always starts a new epoch.
//   - Unmanaged collaborator PRs stay inside PRObject and never reach
//     BuildRouter; these handlers hold no router dependency at all.
//   - Delayed conflict rechecks go through ConflictRescheduler, which chunk 07
//     binds to a Restate delayed send. No wall-clock sleeps anywhere.
//
// GitHub check APIs expose no external idempotency key, so the at-least-once
// tolerance is: the effect key dedups re-fire within a generation; a new
// generation may re-fire the check once, which is safe because the check is
// read-only and the collaborator-conflict mark is overwritten per head SHA,
// never appended.
//
// This chunk registers and tests new components only; no existing caller
// entrypoint is wired to them until the cutover chunk.

// ConflictRecheckDelay is the delay requested for a follow-up conflict check
// after a conflicted or failed check. It is passed to the Restate delayed
// send binding, never slept on.
const ConflictRecheckDelay = 5 * time.Minute

// RepoStateStore is the RepoObject state authority: the bounded main-push
// index, dispatched-SHA tombstones, the touched-PR rebuild index, per-PR
// states for updated-for-main fanout, and the durable SHA lookup rows that
// replace commitSHAPRs with ORDER BY updated_at DESC LIMIT 1 semantics.
type RepoStateStore interface {
	LoadRepo(repo string) (*RepoState, bool)
	SaveRepo(state *RepoState)
	LoadPR(key string) (*PRState, bool)
	SavePR(state *PRState)
	OpenPRs(repo string) []*PRState
	ListSHAEntries(repo string) []SHAEntry
	AppendSHAEntry(entry SHAEntry)
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

// RepoProjector syncs Restate-owned repo metadata keys to the pr_registry
// read-only projection. It runs AFTER the state commit as a fire-and-forget
// one-way send: implementations must not propagate projection failures
// because the committed state is authoritative and replays the projection.
type RepoProjector interface {
	SyncMainPush(ctx context.Context, repo, mainSHA string)
}

// PRFanoutSender delivers one async cross-object main-push send per touched
// PR key. Each payload carries the main SHA; receiving PRObjects drop stale
// events against their own head SHA.
type PRFanoutSender interface {
	SendMainPush(ctx context.Context, prKey, mainSHA string) error
}

// No-op defaults so a zero dependency never nil-panics in dark wiring.

type noopConflictChecker struct{}

func (noopConflictChecker) CheckConflict(context.Context, string, int, string) (bool, error) {
	return false, nil
}

type noopRescheduler struct{}

func (noopRescheduler) ScheduleConflictRecheck(context.Context, time.Duration, PREvent) error {
	return nil
}

type noopProjector struct{}

func (noopProjector) SyncMainPush(context.Context, string, string) {}

type noopFanoutSender struct{}

func (noopFanoutSender) SendMainPush(context.Context, string, string) error { return nil }

// MemRepoStore is an in-memory RepoStateStore for hermetic tests and dark
// wiring. Production chunk 07 replaces it with Restate K/V state.
type MemRepoStore struct {
	mu      sync.Mutex
	repos   map[string]*RepoState
	prs     map[string]*PRState
	entries []SHAEntry
}

// NewMemRepoStore returns an empty in-memory repo store.
func NewMemRepoStore() *MemRepoStore {
	return &MemRepoStore{
		repos: make(map[string]*RepoState),
		prs:   make(map[string]*PRState),
	}
}

// LoadRepo returns the repo state, or false when never touched.
func (s *MemRepoStore) LoadRepo(repo string) (*RepoState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil {
		return nil, false
	}
	st, ok := s.repos[NormalizeRepo(repo)]
	return st, ok
}

// SaveRepo commits the repo state.
func (s *MemRepoStore) SaveRepo(state *RepoState) {
	if s == nil || state == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repos[NormalizeRepo(state.Repo)] = state
}

// LoadPR returns the PR state for a repo#prNumber key.
func (s *MemRepoStore) LoadPR(key string) (*PRState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil {
		return nil, false
	}
	st, ok := s.prs[strings.TrimSpace(key)]
	return st, ok
}

// SavePR commits the PR state under its repo#prNumber key.
func (s *MemRepoStore) SavePR(state *PRState) {
	if s == nil || state == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prs[PRObjectKey(state.Repo, state.PRNumber)] = state
}

// OpenPRs returns every stored PR state for the repo.
func (s *MemRepoStore) OpenPRs(repo string) []*PRState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil {
		return nil
	}
	norm := NormalizeRepo(repo)
	var out []*PRState
	for _, st := range s.prs {
		if st != nil && st.Repo == norm {
			out = append(out, st)
		}
	}
	return out
}

// ListSHAEntries returns the durable SHA lookup rows for the repo.
func (s *MemRepoStore) ListSHAEntries(repo string) []SHAEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil {
		return nil
	}
	norm := strings.ToLower(NormalizeRepo(repo))
	var out []SHAEntry
	for _, e := range s.entries {
		if strings.ToLower(NormalizeRepo(e.Repo)) == norm {
			out = append(out, e)
		}
	}
	return out
}

// AppendSHAEntry records one durable SHA lookup row.
func (s *MemRepoStore) AppendSHAEntry(entry SHAEntry) {
	if s == nil || entry.PRNumber <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
}

// RepoHandlers implements the frozen RepoObject surface as dark components.
// No live caller routes here until the cutover chunk.
type RepoHandlers struct {
	store      RepoStateStore
	conflicts  ConflictChecker
	reschedule ConflictRescheduler
	project    RepoProjector
	fanout     PRFanoutSender
}

// Compile-time freeze: the dark handlers satisfy the full RepoObject surface.
var _ RepoObject = (*RepoHandlers)(nil)

// NewRepoHandlers builds dark RepoObject handlers; nil dependencies fall
// back to in-memory/no-op defaults so zero-value wiring never nil-panics.
func NewRepoHandlers(store RepoStateStore, conflicts ConflictChecker, reschedule ConflictRescheduler, project RepoProjector, fanout PRFanoutSender) *RepoHandlers {
	if store == nil {
		store = NewMemRepoStore()
	}
	if conflicts == nil {
		conflicts = noopConflictChecker{}
	}
	if reschedule == nil {
		reschedule = noopRescheduler{}
	}
	if project == nil {
		project = noopProjector{}
	}
	if fanout == nil {
		fanout = noopFanoutSender{}
	}
	return &RepoHandlers{
		store:      store,
		conflicts:  conflicts,
		reschedule: reschedule,
		project:    project,
		fanout:     fanout,
	}
}

// HandleMainPush records a main push SHA into the bounded 50-entry index and
// clears updated-for-main flags repo-wide only for newly seen SHAs, mirroring
// StateTracker.ClearUpdatedPRsForMainCommit. Duplicate SHAs are idempotent:
// no flag clearing, no generation mint, no effect-key clearing. The registry
// projection syncs after the commit; its failure never fails the push.
func (h *RepoHandlers) HandleMainPush(ctx context.Context, ev RepoEvent) error {
	if h == nil || h.store == nil {
		return fmt.Errorf("repo handlers not configured")
	}
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return fmt.Errorf("repo is required for main push")
	}
	mainSHA := strings.TrimSpace(ev.MainSHA)
	if mainSHA == "" || IsZeroSHA(mainSHA) {
		return fmt.Errorf("main SHA is required for main push")
	}
	st, ok := h.store.LoadRepo(repo)
	if !ok || st == nil {
		st = NewRepoState(repo)
	}
	recorded := st.RecordMainPushSHA(mainSHA)
	if ev.PRNumber > 0 {
		st.TouchPRKey(PRObjectKey(repo, ev.PRNumber))
	}
	h.store.SaveRepo(st)
	if recorded {
		for _, pr := range h.store.OpenPRs(repo) {
			pr.ClearUpdatedForMain()
			h.store.SavePR(pr)
		}
	}
	// One-way projection send after the commit; never gates the transition.
	h.project.SyncMainPush(ctx, repo, st.LatestMainSHA)
	return nil
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
// and already-dispatched effect keys never re-fire. A conflicted head marks
// the collaborator-conflict slot and schedules a delayed recheck; a clean
// head clears the slot. A failed check schedules a delayed recheck and
// returns the error without marking the effect dispatched, so the retry
// re-fires exactly once per attempt.
func (h *RepoHandlers) HandleConflictCheck(ctx context.Context, prKey string, ev PREvent) (ConflictOutcome, error) {
	if h == nil || h.store == nil {
		return ConflictOutcome{}, fmt.Errorf("repo handlers not configured")
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
	pr.MarkEffectDispatched(key)
	if !conflicted {
		pr.ClearCollaboratorConflict(headSHA)
		h.store.SavePR(pr)
		return ConflictOutcome{Disposition: DispositionApply}, nil
	}
	pr.MarkCollaboratorConflict(headSHA)
	h.store.SavePR(pr)
	if err := h.reschedule.ScheduleConflictRecheck(ctx, ConflictRecheckDelay, ev); err != nil {
		return ConflictOutcome{Disposition: DispositionApply, Conflicted: true}, err
	}
	return ConflictOutcome{Disposition: DispositionApply, Conflicted: true, RecheckScheduled: true}, nil
}

// HandleFanout delivers one async main-push send per touched PR key from the
// journaled rebuild index. It records no per-PR state; receiving PRObjects
// drop stale payloads against their own head SHA.
func (h *RepoHandlers) HandleFanout(ctx context.Context, ev RepoEvent) error {
	if h == nil || h.store == nil {
		return fmt.Errorf("repo handlers not configured")
	}
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return fmt.Errorf("repo is required for fanout")
	}
	st, ok := h.store.LoadRepo(repo)
	if !ok || st == nil || strings.TrimSpace(st.LatestMainSHA) == "" {
		return nil
	}
	for _, key := range st.PRKeys() {
		if err := h.fanout.SendMainPush(ctx, key, st.LatestMainSHA); err != nil {
			return err
		}
	}
	return nil
}

// HandleKustomizationStatus records a dispatched-SHA tombstone scoped to its
// gate generation and evicts tombstones below that generation. Eviction is by
// generation, never by count alone; this handler never mints generations.
func (h *RepoHandlers) HandleKustomizationStatus(ctx context.Context, ev RepoEvent) error {
	_ = ctx
	if h == nil || h.store == nil {
		return fmt.Errorf("repo handlers not configured")
	}
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return fmt.Errorf("repo is required for kustomization status")
	}
	sha := strings.TrimSpace(ev.HeadSHA)
	if sha == "" || IsZeroSHA(sha) {
		return nil
	}
	st, ok := h.store.LoadRepo(repo)
	if !ok || st == nil {
		st = NewRepoState(repo)
	}
	st.RecordTombstone(sha, ev.Generation)
	st.EvictTombstonesBelowGeneration(ev.Generation)
	h.store.SaveRepo(st)
	return nil
}

// ResolvePRBySHA resolves a commit SHA to its PR through the durable lookup,
// preserving ORDER BY updated_at DESC LIMIT 1 ambiguity ordering. It is a
// read-only routing helper; authority never consults it for transitions.
func (h *RepoHandlers) ResolvePRBySHA(ctx context.Context, repo, sha string) (int, string, bool) {
	_ = ctx
	if h == nil || h.store == nil {
		return 0, "", false
	}
	return ResolvePRBySHA(h.store.ListSHAEntries(repo), repo, sha)
}
