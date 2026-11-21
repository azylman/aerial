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

// RepoObject logic: main-push SHA recording, the PRObject-fed SHA routing
// index, SHA-only event routing with held replays, kustomization status
// fan-out through the deploying set, and durable SHA resolution.
//
// Authority boundaries (frozen, see contracts.go):
//   - RepoObject owns only repo-level K/V (RepoState). It never loads or
//     saves PR state: Restate gives an exclusive handler only its own key.
//     Every PR effect of a repo event (clearing updated-for-main, forwarding
//     a resolved check run, a kustomization status for a deploying PR) is a
//     one-way send to PRObject, never request-response, so two exclusive
//     handlers can never wait on each other.
//   - PRObject feeds the index with one-way HandlePRIndex sends after its
//     own state commit; RepoObject applies them in arrival order and ignores
//     updates from an older reopen epoch.
//   - Index misses are held (bounded count, expiring after
//     HeldSHAEventMaxAge via a delayed self-send) and replayed by a delayed
//     self-send once PRObject indexes the SHA.
//   - pr_registry is a read-only projection synced AFTER the state commit;
//     projection failure never discards or gates the transition.
//
// The Restate binding in service.go builds these handlers per invocation
// over Restate K/V and Restate sends; tests use MemRepoStore and fakes.

// ErrInvalidRepoEvent marks malformed RepoObject input. No retry can fix it,
// so the Restate binding converts it to a terminal error.
var ErrInvalidRepoEvent = errors.New("prstate: invalid repo event")

// RepoStateStore is the RepoObject state authority: exactly one RepoState
// per repo. It has no PR accessors by design.
type RepoStateStore interface {
	LoadRepo(repo string) (*RepoState, error)
	SaveRepo(state *RepoState)
}

// RepoProjector syncs Restate-owned repo metadata keys to the pr_registry
// read-only projection. It runs AFTER the state commit as a fire-and-forget
// one-way send: implementations must not propagate projection failures
// because the committed state is authoritative and replays the projection.
type RepoProjector interface {
	SyncMainPush(ctx context.Context, repo, mainSHA string)
}

// PRSender delivers one one-way send to a PRObject handler keyed by the
// event's repo#prNumber. Production binds it to restate.ObjectSend.
type PRSender interface {
	SendPR(ctx context.Context, handler string, ev PREvent) error
}

// RepoSelfScheduler schedules delayed RepoObject self-sends for held event
// replay and expiry. Production binds it to delayed restate.ObjectSend.
type RepoSelfScheduler interface {
	ScheduleHeldReplay(ctx context.Context, repo, sha string, delay time.Duration) error
	ScheduleHeldExpiry(ctx context.Context, repo string, id uint64, delay time.Duration) error
}

// No-op defaults so a zero dependency never nil-panics.

type noopProjector struct{}

func (noopProjector) SyncMainPush(context.Context, string, string) {}

type noopPRSender struct{}

func (noopPRSender) SendPR(context.Context, string, PREvent) error { return nil }

type noopSelfScheduler struct{}

func (noopSelfScheduler) ScheduleHeldReplay(context.Context, string, string, time.Duration) error {
	return nil
}

func (noopSelfScheduler) ScheduleHeldExpiry(context.Context, string, uint64, time.Duration) error {
	return nil
}

// MemRepoStore is an in-memory RepoStateStore for hermetic tests. It stores
// JSON snapshots, so every load returns a fresh copy exactly like Restate
// K/V (including dropped unexported indexes) and an unsaved mutation never
// leaks into the store.
type MemRepoStore struct {
	mu    sync.Mutex
	repos map[string][]byte
	err   error
}

// NewMemRepoStore returns an empty in-memory repo store.
func NewMemRepoStore() *MemRepoStore {
	return &MemRepoStore{repos: make(map[string][]byte)}
}

// FailLoads makes every later LoadRepo return err, standing in for a Restate
// K/V failure.
func (s *MemRepoStore) FailLoads(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

// LoadRepo returns a copy of the repo state, or nil when never saved.
func (s *MemRepoStore) LoadRepo(repo string) (*RepoState, error) {
	if s == nil {
		return nil, nil //nolint:nilnil // a never-saved repo has no state, exactly like restate.Get on an empty slot.
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	raw, ok := s.repos[NormalizeRepo(repo)]
	if !ok {
		return nil, nil //nolint:nilnil // a never-saved repo has no state, exactly like restate.Get on an empty slot.
	}
	var st RepoState
	//nolint:musttag // RepoState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// SaveRepo commits a snapshot of the repo state.
func (s *MemRepoStore) SaveRepo(state *RepoState) {
	if s == nil || state == nil {
		return
	}
	//nolint:musttag // RepoState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repos[NormalizeRepo(state.Repo)] = raw
}

// RepoHandlers implements the RepoObject logic surface.
type RepoHandlers struct {
	store   RepoStateStore
	project RepoProjector
	prs     PRSender
	self    RepoSelfScheduler
}

// Compile-time freeze: the handlers satisfy the full RepoObject surface.
var _ RepoObject = (*RepoHandlers)(nil)

// NewRepoHandlers builds RepoObject handlers; nil dependencies fall back to
// in-memory/no-op defaults so zero-value wiring never nil-panics.
func NewRepoHandlers(store RepoStateStore, project RepoProjector, prs PRSender, self RepoSelfScheduler) *RepoHandlers {
	if store == nil {
		store = NewMemRepoStore()
	}
	if project == nil {
		project = noopProjector{}
	}
	if prs == nil {
		prs = noopPRSender{}
	}
	if self == nil {
		self = noopSelfScheduler{}
	}
	return &RepoHandlers{store: store, project: project, prs: prs, self: self}
}

func (h *RepoHandlers) loadOrNew(repo string) (*RepoState, error) {
	st, err := h.store.LoadRepo(repo)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return NewRepoState(repo), nil
	}
	return st, nil
}

// HandleMainPush records a main push SHA into the bounded 50-entry index.
// A newly seen SHA fans out one one-way HandleMainAdvanced send per open PR,
// which clears updated-for-main inside each PRObject, mirroring
// StateTracker.ClearUpdatedPRsForMainCommit. Duplicate SHAs are idempotent:
// no fan-out, no projection, and nothing persisted unless the push touches
// a new PR key. The registry projection syncs after the commit; its failure
// never fails the push.
func (h *RepoHandlers) HandleMainPush(ctx context.Context, ev RepoEvent) error {
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return fmt.Errorf("%w: repo is required for main push", ErrInvalidRepoEvent)
	}
	mainSHA := strings.TrimSpace(ev.MainSHA)
	if mainSHA == "" || IsZeroSHA(mainSHA) {
		return fmt.Errorf("%w: main SHA is required for main push", ErrInvalidRepoEvent)
	}
	st, err := h.loadOrNew(repo)
	if err != nil {
		return err
	}
	recorded := st.RecordMainPushSHA(mainSHA)
	touched := ev.PRNumber > 0 && st.TouchPRKey(PRObjectKey(repo, ev.PRNumber))
	if !recorded && !touched {
		return nil
	}
	var errs []error
	if recorded {
		errs = append(errs, h.sendMainAdvanced(ctx, st, ev.EffectsOwner))
	}
	h.store.SaveRepo(st)
	if recorded {
		// One-way projection send after the commit; never gates the transition.
		h.project.SyncMainPush(ctx, repo, st.LatestMainSHA)
	}
	return errors.Join(errs...)
}

// sendMainAdvanced sends one HandleMainAdvanced per open PR. Every PR is
// attempted: one failing send never starves the later PRs.
func (h *RepoHandlers) sendMainAdvanced(ctx context.Context, st *RepoState, owner EffectsOwner) error {
	var errs []error
	for _, pr := range st.OpenPRNumbers() {
		ev := PREvent{Event: "main_advanced", Repo: st.Repo, PRNumber: pr, EffectsOwner: ParseEffectsOwner(string(owner))}
		if err := h.prs.SendPR(ctx, PRHandlerMainAdvanced, ev); err != nil {
			errs = append(errs, fmt.Errorf("main advanced %s: %w", PRObjectKey(st.Repo, pr), err))
		}
	}
	return errors.Join(errs...)
}

// HandleFanout re-delivers the latest main push to every open PR as one-way
// HandleMainAdvanced sends. It records no state; receiving PRObjects treat
// the clear as idempotent.
func (h *RepoHandlers) HandleFanout(ctx context.Context, ev RepoEvent) error {
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return fmt.Errorf("%w: repo is required for fanout", ErrInvalidRepoEvent)
	}
	st, err := h.store.LoadRepo(repo)
	if err != nil {
		return err
	}
	if st == nil || strings.TrimSpace(st.LatestMainSHA) == "" {
		return nil
	}
	return h.sendMainAdvanced(ctx, st, ev.EffectsOwner)
}

// HandleKustomizationStatus records a dispatched-SHA tombstone scoped to its
// gate generation (evicting tombstones below that generation; never by count
// alone, never minting generations) and forwards the observation to every
// deploying PR as a one-way HandleRolloutProgress send. The deploying set
// replaces the registry ListDeployingPRs scan. A status without a SHA still
// fans out but persists nothing.
func (h *RepoHandlers) HandleKustomizationStatus(ctx context.Context, ev RepoEvent) error {
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return fmt.Errorf("%w: repo is required for kustomization status", ErrInvalidRepoEvent)
	}
	st, err := h.store.LoadRepo(repo)
	if err != nil {
		return err
	}
	changed := false
	if sha := strings.TrimSpace(ev.HeadSHA); sha != "" && !IsZeroSHA(sha) {
		if st == nil {
			st = NewRepoState(repo)
		}
		st.RecordTombstone(sha, ev.Generation)
		st.EvictTombstonesBelowGeneration(ev.Generation)
		changed = true
	}
	var errs []error
	for _, pr := range st.DeployingPRNumbers() {
		status, _ := st.PRStatus(pr)
		fwd := PREvent{
			Event:          "kustomization_status",
			Repo:           repo,
			PRNumber:       pr,
			Branch:         status.Branch,
			GateGeneration: status.DeployGeneration,
			EffectsOwner:   ParseEffectsOwner(string(ev.EffectsOwner)),
			Kustomization:  strings.TrimSpace(ev.Kustomization),
		}
		if err := h.prs.SendPR(ctx, PRHandlerRolloutProgress, fwd); err != nil {
			errs = append(errs, fmt.Errorf("kustomization status %s: %w", PRObjectKey(repo, pr), err))
		}
	}
	if changed {
		h.store.SaveRepo(st)
	}
	return errors.Join(errs...)
}

// HandlePRIndex applies one PRObject index update. Newly indexed SHAs with
// held events schedule a delayed replay self-send BEFORE the state persists,
// so a scheduling failure leaves nothing behind and the retry re-applies.
func (h *RepoHandlers) HandlePRIndex(ctx context.Context, ev PRIndexEvent) error {
	repo := NormalizeRepo(ev.Repo)
	if repo == "" || ev.PRNumber <= 0 {
		return fmt.Errorf("%w: repo and pr number are required for pr index", ErrInvalidRepoEvent)
	}
	st, err := h.loadOrNew(repo)
	if err != nil {
		return err
	}
	changed, replay := st.ApplyPRIndex(ev)
	for _, sha := range replay {
		if err := h.self.ScheduleHeldReplay(ctx, repo, sha, HeldReplayDelay); err != nil {
			return fmt.Errorf("schedule held replay %s@%s: %w", repo, sha, err)
		}
	}
	if changed {
		h.store.SaveRepo(st)
	}
	return nil
}

// RouteBySHA resolves one SHA-only event through the index. A hit forwards
// the event to its PRObject handler as a one-way send and persists nothing.
// A miss holds the event, scheduling its expiry before persisting; beyond
// MaxHeldSHAEvents the oldest held event is evicted and reported.
func (h *RepoHandlers) RouteBySHA(ctx context.Context, kind string, ev SHAEvent) (SHARouteOutcome, error) {
	repo := NormalizeRepo(ev.Repo)
	sha := normalizeIndexSHA(ev.SHA)
	if repo == "" || sha == "" {
		return SHARouteOutcome{}, fmt.Errorf("%w: repo and sha are required for %s", ErrInvalidRepoEvent, kind)
	}
	if !IsSHARouteAllowed(kind, ev.Handler) {
		return SHARouteOutcome{}, fmt.Errorf("%w: handler %q is not routable for %s", ErrInvalidRepoEvent, ev.Handler, kind)
	}
	ev.Repo = repo
	ev.SHA = sha
	st, err := h.loadOrNew(repo)
	if err != nil {
		return SHARouteOutcome{}, err
	}
	if entry, ok := st.ResolveSHA(sha); ok {
		if err := h.prs.SendPR(ctx, ev.Handler, forwardSHAEvent(kind, repo, entry, ev)); err != nil {
			return SHARouteOutcome{}, err
		}
		return SHARouteOutcome{Forwarded: true, PRNumber: entry.PRNumber}, nil
	}
	held, evicted := st.HoldSHAEvent(kind, ev)
	if err := h.self.ScheduleHeldExpiry(ctx, repo, held.ID, HeldSHAEventMaxAge); err != nil {
		return SHARouteOutcome{}, fmt.Errorf("schedule held expiry %s@%s: %w", repo, sha, err)
	}
	h.store.SaveRepo(st)
	return SHARouteOutcome{Held: true, HeldID: held.ID, Evicted: evicted}, nil
}

// forwardSHAEvent fills the repo object key and resolved PR into the carried
// PRObject event. The
// branch fills only when the event has none, and an event with neither head
// nor merge SHA takes the routed SHA (merge SHA for pushes, head otherwise).
func forwardSHAEvent(kind, repo string, entry SHAIndexEntry, ev SHAEvent) PREvent {
	out := ev.Event
	out.Repo = repo
	out.PRNumber = entry.PRNumber
	if strings.TrimSpace(out.Branch) == "" {
		out.Branch = entry.Branch
	}
	if strings.TrimSpace(out.HeadSHA) == "" && strings.TrimSpace(out.MergeSHA) == "" {
		if kind == SHAKindPush {
			out.MergeSHA = ev.SHA
		} else {
			out.HeadSHA = ev.SHA
		}
	}
	out.EffectsOwner = ParseEffectsOwner(string(out.EffectsOwner))
	return out
}

// ReplayHeld forwards every event held for sha once the SHA resolves. An
// unresolved SHA keeps its events held for the expiry self-send.
func (h *RepoHandlers) ReplayHeld(ctx context.Context, repo, sha string) error {
	repo = NormalizeRepo(repo)
	if repo == "" || normalizeIndexSHA(sha) == "" {
		return fmt.Errorf("%w: repo and sha are required for held replay", ErrInvalidRepoEvent)
	}
	st, err := h.store.LoadRepo(repo)
	if err != nil || st == nil {
		return err
	}
	entry, ok := st.ResolveSHA(sha)
	if !ok {
		return nil
	}
	held := st.TakeHeld(sha)
	if len(held) == 0 {
		return nil
	}
	for _, ev := range held {
		if err := h.prs.SendPR(ctx, ev.Event.Handler, forwardSHAEvent(ev.Kind, repo, entry, ev.Event)); err != nil {
			return fmt.Errorf("replay held %s@%s: %w", repo, ev.Event.SHA, err)
		}
	}
	h.store.SaveRepo(st)
	return nil
}

// ExpireHeld drops one held event if it is still waiting. Returns true when
// an event was dropped; replayed or evicted IDs report false.
func (h *RepoHandlers) ExpireHeld(ctx context.Context, repo string, id uint64) (bool, error) {
	_ = ctx
	repo = NormalizeRepo(repo)
	if repo == "" {
		return false, fmt.Errorf("%w: repo is required for held expiry", ErrInvalidRepoEvent)
	}
	st, err := h.store.LoadRepo(repo)
	if err != nil || st == nil {
		return false, err
	}
	if _, ok := st.ExpireHeld(id); !ok {
		return false, nil
	}
	h.store.SaveRepo(st)
	return true, nil
}

// ResolvePRBySHA resolves a commit SHA to its PR through the durable index,
// newest claim first. It is a read-only routing helper; authority never
// consults it for transitions. A store failure reports not-found.
func (h *RepoHandlers) ResolvePRBySHA(ctx context.Context, repo, sha string) (int, string, bool) {
	_ = ctx
	st, err := h.store.LoadRepo(repo)
	if err != nil {
		return 0, "", false
	}
	entry, ok := st.ResolveSHA(sha)
	if !ok {
		return 0, "", false
	}
	return entry.PRNumber, entry.Branch, true
}
