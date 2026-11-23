package prstate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
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

// RepoHandlers implements the RepoObject logic surface.
type RepoHandlers struct {
	store  RepoStateStore
	prs    PRSender
	self   RepoSelfScheduler
	github GitHubEffector
	kube   KubeEffector
}

// NewRepoHandlers builds RepoObject handlers. Every dependency is
// required: a nil one returns errNilDependency. The GitHub and Kubernetes
// boundaries attach through WithGitHub and WithKube; an unset one makes its
// effects schedule retries.
func NewRepoHandlers(store RepoStateStore, prs PRSender, self RepoSelfScheduler) (*RepoHandlers, error) {
	switch {
	case store == nil:
		return nil, fmt.Errorf("%w: repo handlers store", errNilDependency)
	case prs == nil:
		return nil, fmt.Errorf("%w: repo handlers PR sender", errNilDependency)
	case self == nil:
		return nil, fmt.Errorf("%w: repo handlers self scheduler", errNilDependency)
	}
	return &RepoHandlers{store: store, prs: prs, self: self}, nil
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
// no fan-out and nothing persisted unless the push touches a new PR key.
func (h *RepoHandlers) HandleMainPush(ctx context.Context, ev RepoEvent) error {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" {
		return fmt.Errorf("%w: repo is required for main push", ErrInvalidRepoEvent)
	}
	mainSHA := strings.TrimSpace(ev.MainSHA)
	if mainSHA == "" || prwire.IsZeroSHA(mainSHA) {
		return fmt.Errorf("%w: main SHA is required for main push", ErrInvalidRepoEvent)
	}
	st, err := h.loadOrNew(repo)
	if err != nil {
		return err
	}
	recorded := st.RecordMainPushSHA(mainSHA)
	touched := ev.PRNumber > 0 && st.TouchPRKey(prwire.PRObjectKey(repo, ev.PRNumber))
	needsFanout := st.LatestMainSHA != "" && st.FannedOutMainSHA != st.LatestMainSHA
	if !recorded && !touched && !needsFanout {
		return nil
	}
	var errs []error
	if recorded || needsFanout {
		errs = append(errs, h.sendMainAdvanced(ctx, st))
		st.FannedOutMainSHA = st.LatestMainSHA
	}
	h.store.SaveRepo(st)
	return errors.Join(errs...)
}

// sendMainAdvanced sends one HandleMainAdvanced per open PR. Every PR is
// attempted: one failing send never starves the later PRs.
func (h *RepoHandlers) sendMainAdvanced(ctx context.Context, st *RepoState) error {
	var errs []error
	for _, pr := range st.OpenPRNumbers() {
		ev := PREvent{Event: "main_advanced", Repo: st.Repo, PRNumber: pr}
		if err := h.prs.SendPR(ctx, prwire.PRHandlerMainAdvanced, ev); err != nil {
			errs = append(errs, fmt.Errorf("main advanced %s: %w", prwire.PRObjectKey(st.Repo, pr), err))
		}
	}
	return errors.Join(errs...)
}

// HandleFanout delivers the latest main push to every open PR as one-way
// HandleMainAdvanced sends, as a journaled effect keyed by the main SHA: a
// main SHA already fanned out (by HandleMainPush or an earlier fan-out)
// sends nothing, so a redelivered fan-out never re-fires.
func (h *RepoHandlers) HandleFanout(ctx context.Context, ev RepoEvent) error {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" {
		return fmt.Errorf("%w: repo is required for fanout", ErrInvalidRepoEvent)
	}
	st, err := h.store.LoadRepo(repo)
	if err != nil {
		return err
	}
	if st == nil || strings.TrimSpace(st.LatestMainSHA) == "" || st.FannedOutMainSHA == st.LatestMainSHA {
		return nil
	}
	err = h.sendMainAdvanced(ctx, st)
	st.FannedOutMainSHA = st.LatestMainSHA
	h.store.SaveRepo(st)
	return err
}

// HandleKustomizationStatus records a dispatched-SHA tombstone scoped to its
// gate generation (evicting tombstones below that generation; never by count
// alone, never minting generations) and forwards the observation to every
// deploying PR as a one-way HandleRolloutProgress send. The deploying set
// replaces the registry ListDeployingPRs scan. A status without a SHA still
// fans out but persists nothing.
func (h *RepoHandlers) HandleKustomizationStatus(ctx context.Context, ev RepoEvent) error {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" {
		return fmt.Errorf("%w: repo is required for kustomization status", ErrInvalidRepoEvent)
	}
	st, err := h.store.LoadRepo(repo)
	if err != nil {
		return err
	}
	changed := false
	if sha := strings.TrimSpace(ev.HeadSHA); sha != "" && !prwire.IsZeroSHA(sha) {
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
			Kustomization:  strings.TrimSpace(ev.Kustomization),
		}
		if err := h.prs.SendPR(ctx, prwire.PRHandlerRolloutProgress, fwd); err != nil {
			errs = append(errs, fmt.Errorf("kustomization status %s: %w", prwire.PRObjectKey(repo, pr), err))
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
	repo := strings.TrimSpace(ev.Repo)
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
	repo := strings.TrimSpace(ev.Repo)
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
		if ev.Handler == prwire.PRHandlerCheckFailed {
			st.ClearPRUpdatedForMain(entry.PRNumber)
			h.store.SaveRepo(st)
		}
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
	return out
}

// ReplayHeld forwards every event held for sha once the SHA resolves. An
// unresolved SHA keeps its events held for the expiry self-send.
func (h *RepoHandlers) ReplayHeld(ctx context.Context, repo, sha string) error {
	repo = strings.TrimSpace(repo)
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
		if ev.Event.Handler == prwire.PRHandlerCheckFailed {
			st.ClearPRUpdatedForMain(entry.PRNumber)
		}
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
	repo = strings.TrimSpace(repo)
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

// GitHubEffector is the GitHub boundary for restate-owned GitHub effects.
// *GitHubClient satisfies it directly; the Restate binding wraps every call
// in one journaled restate.Run so a replay returns the recorded result.
type GitHubEffector interface {
	ListOpenPRs(ctx context.Context, repo string) ([]GitHubPR, *GitHubError)
	HeadChecksBlocked(ctx context.Context, repo, sha string) bool
	UpdateBranch(ctx context.Context, repo string, prNumber int) (UpdateBranchOutcome, *GitHubError)
	FetchPR(ctx context.Context, repo string, prNumber int) (GitHubPR, *GitHubError)
	FetchPRMergeability(ctx context.Context, repo string, prNumber int) (GitHubPR, *GitHubError)
	RerunFailedJobs(ctx context.Context, repo string, runID int64) *GitHubError
}

var _ GitHubEffector = (*GitHubClient)(nil)

// errGitHubUnconfigured is the retryable failure a restate-owned GitHub
// effect reports when no client is wired.
var errGitHubUnconfigured = &GitHubError{Message: "prstate: no github client configured"}

// WithGitHub injects the GitHub boundary and returns h.
func (h *RepoHandlers) WithGitHub(gh GitHubEffector) *RepoHandlers {
	h.github = gh
	return h
}

// BranchUpdate is one update-branch call the cascade made.
type BranchUpdate struct {
	PRNumber int
	HeadSHA  string
	Outcome  UpdateBranchOutcome
}

// RepoEffectOutcome reports one repo-level GitHub effect: deduped, skipped,
// executed, or failed. Retry means the
// failure is retryable and the effect key was NOT committed, so the binding
// schedules a delayed retry.
type RepoEffectOutcome struct {
	Key      EffectKey
	Deduped  bool
	Skipped  bool
	Executed bool
	Retry    bool
	Err      *GitHubError
	// Kube is the failure of a Flux or image effect.
	Kube     *KubeError
	Updates  []BranchUpdate
	Forwards int
	// Promoted are the workloads an image promotion promoted.
	Promoted []string
}

// errText renders the outcome's failure, or "".
func (o RepoEffectOutcome) errText() string {
	switch {
	case o.Err != nil:
		return o.Err.Error()
	case o.Kube != nil:
		return o.Kube.Error()
	default:
		return ""
	}
}

// repoEffectTrigger returns the SHA a repo effect is keyed on: the head SHA
// of the triggering PR event, falling back to the main push SHA.
func repoEffectTrigger(ev RepoEvent) string {
	if sha := usableHeadSHA(ev.HeadSHA); sha != "" {
		return sha
	}
	return usableHeadSHA(ev.MainSHA)
}

// RepoEffectKey keys one repo effect: repo + triggering PR (0 for a main
// push) + trigger SHA + kind + trigger check. Repo effects have no gate
// generation. Synapse's [synapse-effect] line renders the same match key.
func RepoEffectKey(kind string, ev RepoEvent) EffectKey {
	return EffectKey{Repo: strings.TrimSpace(ev.Repo), PRNumber: ev.PRNumber, HeadSHA: repoEffectTrigger(ev), Kind: kind, CheckName: ev.CheckName}
}

// beginRepoEffect loads the repo state and resolves dedup for one repo
// effect. done reports that the outcome is final (deduped); otherwise the
// caller executes the effect.
func (h *RepoHandlers) beginRepoEffect(kind string, ev RepoEvent) (*RepoState, RepoEffectOutcome, bool, error) {
	repo := strings.TrimSpace(ev.Repo)
	if repo == "" || repoEffectTrigger(ev) == "" {
		return nil, RepoEffectOutcome{}, true, fmt.Errorf("%w: repo and trigger sha are required for %s", ErrInvalidRepoEvent, kind)
	}
	st, err := h.loadOrNew(repo)
	if err != nil {
		return nil, RepoEffectOutcome{}, true, err
	}
	out := RepoEffectOutcome{Key: RepoEffectKey(kind, ev)}
	if st.WasRepoEffect(out.Key) {
		out.Deduped = true
		return st, out, true, nil
	}
	return st, out, false, nil
}

// MergeQueueCheckName is the synthetic check name used to publish and track merge queue positions.
const MergeQueueCheckName = "_merge_queue"

// HandleAutoUpdate runs the open-PR auto-update cascade synapse's
// checkOpenPRConflicts runs, once per trigger key. It lists open PRs
// targeting main and, for each eligible PR (not draft, allowlisted author,
// and indexed here unless the policy is the default owner) whose head checks
// are neither pending nor failed, update the branch: a conflict forwards
// HandleBranchConflict (carrying the collaborator DM when a rule matches), a
// missing head ref closes the PR via HandleHeadRefMissing when GitHub shows
// it closed unmerged, and the first successful update ends the cascade. A
// rate limit or retryable list failure stops without committing the key so
// the binding retries; other per-PR failures are skipped.
func (h *RepoHandlers) HandleAutoUpdate(ctx context.Context, ev RepoEvent) (RepoEffectOutcome, error) {
	st, out, done, err := h.beginRepoEffect(prwire.EffectGitHubAutoUpdate, ev)
	if done || err != nil {
		return out, err
	}
	if ev.AutoUpdate == nil || len(ev.AutoUpdate.Authors) == 0 {
		out.Skipped = true
		st.MarkRepoEffect(out.Key)
		h.store.SaveRepo(st)
		return out, nil
	}
	if h.github == nil {
		out.Retry, out.Err = true, errGitHubUnconfigured
		return out, nil
	}
	if strings.TrimSpace(ev.MainSHA) != "" {
		st.RecordMainPushSHA(ev.MainSHA)
	}
	if strings.TrimSpace(ev.CheckName) == "" {
		if ev.PRNumber > 0 {
			st.ClearPRUpdatedForMain(ev.PRNumber)
		} else if strings.TrimSpace(ev.HeadSHA) != "" {
			if entry, ok := st.ResolveSHA(ev.HeadSHA); ok {
				st.ClearPRUpdatedForMain(entry.PRNumber)
			}
		}
	}
	sendErr := h.runAutoUpdate(ctx, st, ev.AutoUpdate, &out)
	if !out.Retry {
		out.Executed = true
		st.MarkRepoEffect(out.Key)
		h.store.SaveRepo(st)
	}
	return out, sendErr
}

// runAutoUpdate walks the open PRs for one restate-owned cascade.
func (h *RepoHandlers) runAutoUpdate(ctx context.Context, st *RepoState, policy *AutoUpdatePolicy, out *RepoEffectOutcome) error {
	repo := st.Repo
	prs, gerr := h.github.ListOpenPRs(ctx, repo)
	if gerr != nil {
		out.Err, out.Retry = gerr, gerr.Retryable()
		return nil
	}
	var errs []error
	if !hasActiveUpdatedPR(st, policy, prs) {
		errs = append(errs, h.updateFirstEligibleBranch(ctx, st, policy, prs, out)...)
	}
	if !out.Retry {
		if berr := h.broadcastMergeQueue(ctx, st, policy, prs, out); berr != nil {
			errs = append(errs, berr)
		}
	}
	return errors.Join(errs...)
}

func hasActiveUpdatedPR(st *RepoState, policy *AutoUpdatePolicy, prs []GitHubPR) bool {
	for _, pr := range prs {
		if !autoUpdateEligible(st, policy, pr) {
			continue
		}
		if st.WasPRUpdatedForMain(pr.Number) {
			if strings.EqualFold(pr.MergeableState, "dirty") || strings.EqualFold(pr.MergeableState, "conflicting") {
				st.ClearPRUpdatedForMain(pr.Number)
				continue
			}
			return true
		}
	}
	return false
}

func (h *RepoHandlers) updateFirstEligibleBranch(ctx context.Context, st *RepoState, policy *AutoUpdatePolicy, prs []GitHubPR, out *RepoEffectOutcome) []error {
	repo := st.Repo
	var errs []error
	for _, pr := range prs {
		if !autoUpdateEligible(st, policy, pr) || h.github.HeadChecksBlocked(ctx, repo, pr.Head.SHA) {
			continue
		}
		outcome, gerr := h.github.UpdateBranch(ctx, repo, pr.Number)
		if gerr != nil {
			out.Err = gerr
			if gerr.RateLimited {
				out.Retry = true
				break
			}
			continue
		}
		out.Updates = append(out.Updates, BranchUpdate{PRNumber: pr.Number, HeadSHA: strings.TrimSpace(pr.Head.SHA), Outcome: outcome})
		switch outcome {
		case UpdateBranchConflict:
			st.ClearPRUpdatedForMain(pr.Number)
			errs = append(errs, h.forward(ctx, prwire.PRHandlerBranchConflict, branchConflictEvent(policy, repo, pr), out))
			continue
		case UpdateBranchHeadMissing:
			st.ClearPRUpdatedForMain(pr.Number)
			if ev, ok := h.staleCloseEvent(ctx, repo, pr.Number); ok {
				errs = append(errs, h.forward(ctx, prwire.PRHandlerHeadRefMissing, ev, out))
			}
			continue
		case UpdateBranchUpdated, UpdateBranchUpToDate:
			st.MarkPRUpdatedForMain(pr.Number)
		}
		break
	}
	return errs
}

func (h *RepoHandlers) broadcastMergeQueue(ctx context.Context, st *RepoState, policy *AutoUpdatePolicy, prs []GitHubPR, out *RepoEffectOutcome) error {
	var candidates []GitHubPR
	for _, pr := range prs {
		if !autoUpdateEligible(st, policy, pr) {
			continue
		}
		if strings.EqualFold(pr.MergeableState, "dirty") || strings.EqualFold(pr.MergeableState, "conflicting") {
			continue
		}
		if failedInUpdates(out.Updates, pr.Number) {
			continue
		}
		candidates = append(candidates, pr)
	}

	var queue []GitHubPR
	var active *GitHubPR
	for i := range candidates {
		if st.WasPRUpdatedForMain(candidates[i].Number) {
			active = &candidates[i]
			break
		}
	}
	if active != nil {
		queue = append(queue, *active)
		for _, c := range candidates {
			if c.Number != active.Number {
				queue = append(queue, c)
			}
		}
	} else {
		queue = candidates
	}

	total := len(queue)
	if total == 1 {
		pr := queue[0]
		if st.HasPRQueuePosition(pr.Number) {
			st.ClearPRQueuePosition(pr.Number)
			return h.forward(ctx, prwire.PRHandlerCheckStatus, queueCheckStatusEvent(st, pr, "1/1"), out)
		}
		return nil
	}
	if total >= 2 {
		var errs []error
		for i, pr := range queue {
			pos := i + 1
			posStr := fmt.Sprintf("%d/%d", pos, total)
			if st.RecordPRQueuePosition(pr.Number, pr.Head.SHA, posStr) {
				errs = append(errs, h.forward(ctx, prwire.PRHandlerCheckStatus, queueCheckStatusEvent(st, pr, posStr), out))
			}
		}
		return errors.Join(errs...)
	}
	return nil
}

func failedInUpdates(updates []BranchUpdate, prNumber int) bool {
	for _, u := range updates {
		if u.PRNumber == prNumber && (u.Outcome == UpdateBranchConflict || u.Outcome == UpdateBranchHeadMissing) {
			return true
		}
	}
	return false
}

func queueCheckStatusEvent(st *RepoState, pr GitHubPR, posStr string) PREvent {
	return PREvent{
		Event:      "check_updated",
		Repo:       st.Repo,
		PRNumber:   pr.Number,
		HeadSHA:    strings.TrimSpace(pr.Head.SHA),
		Branch:     strings.TrimSpace(pr.Head.Ref),
		CheckName:  MergeQueueCheckName,
		CheckState: posStr,
		Facts: []pubsub.PRLifecycleEvent{{
			Event:      "check_updated",
			Repo:       st.Repo,
			PRNumber:   pr.Number,
			Branch:     strings.TrimSpace(pr.Head.Ref),
			HeadSHA:    strings.TrimSpace(pr.Head.SHA),
			JobName:    fmt.Sprintf("%s#%d", posStr, st.nextSeq()),
			CheckName:  MergeQueueCheckName,
			CheckState: posStr,
			Status:     RegistryStatusOpen,
		}},
	}
}

// forward sends one restate-owned PRObject event and counts it.
func (h *RepoHandlers) forward(ctx context.Context, handler string, ev PREvent, out *RepoEffectOutcome) error {
	if err := h.prs.SendPR(ctx, handler, ev); err != nil {
		return fmt.Errorf("%s %s: %w", handler, prwire.PRObjectKey(ev.Repo, ev.PRNumber), err)
	}
	out.Forwards++
	return nil
}

// autoUpdateEligible mirrors synapse isEligibleAutoUpdatePR: not a draft,
// an allowlisted author, and owned here (default owner, or a PR this repo
// object has indexed).
func autoUpdateEligible(st *RepoState, policy *AutoUpdatePolicy, pr GitHubPR) bool {
	if pr.Draft || pr.Number <= 0 || !authorAllowed(policy, pr.User.Login) {
		return false
	}
	if policy.DefaultOwner {
		return true
	}
	_, indexed := st.PRStatus(pr.Number)
	return indexed
}

// branchConflictEvent is the restate-owned conflict an update-branch call
// observed, carrying the collaborator DM when the policy routes one.
func branchConflictEvent(policy *AutoUpdatePolicy, repo string, pr GitHubPR) PREvent {
	ev := PREvent{
		Event: "branch_conflict", Repo: repo, PRNumber: pr.Number, HeadSHA: strings.TrimSpace(pr.Head.SHA),
		Branch: strings.TrimSpace(pr.Head.Ref), Title: strings.TrimSpace(pr.Title),
	}
	if dm, ok := collaboratorConflictDM(policy, repo, pr); ok {
		ev.EriEffects = []EriEffect{dm}
	}
	return ev
}

// staleCloseEvent mirrors synapse handleStaleOpenPR: a PR whose head ref is
// gone closes only when GitHub shows it closed and unmerged (a 404 counts as
// closed); a read failure, a merge, or a still-open PR closes nothing.
func (h *RepoHandlers) staleCloseEvent(ctx context.Context, repo string, prNumber int) (PREvent, bool) {
	fetched, gerr := h.github.FetchPR(ctx, repo, prNumber)
	if (gerr != nil && !gerr.NotFound()) || fetched.Merged || strings.EqualFold(strings.TrimSpace(fetched.State), "open") {
		return PREvent{}, false
	}
	return PREvent{
		Event: "head_ref_missing", Repo: repo, PRNumber: prNumber, HeadSHA: strings.TrimSpace(fetched.Head.SHA),
		Branch: strings.TrimSpace(fetched.Head.Ref),
	}, true
}

// HandleWorkflowRerun reruns the failed jobs of one workflow run once per
// trigger key (repo, PR, head SHA, workflow name). A retryable failure leaves the key
// uncommitted for the binding's delayed retry; a terminal 4xx commits it and
// reports the error, since no retry can succeed.
func (h *RepoHandlers) HandleWorkflowRerun(ctx context.Context, ev RepoEvent) (RepoEffectOutcome, error) {
	if ev.RunID <= 0 {
		return RepoEffectOutcome{}, fmt.Errorf("%w: run id is required for workflow rerun", ErrInvalidRepoEvent)
	}
	st, out, done, err := h.beginRepoEffect(prwire.EffectGitHubRerun, ev)
	if done || err != nil {
		return out, err
	}
	if h.github == nil {
		out.Retry, out.Err = true, errGitHubUnconfigured
		return out, nil
	}
	if gerr := h.github.RerunFailedJobs(ctx, st.Repo, ev.RunID); gerr != nil {
		out.Err = gerr
		if gerr.Retryable() {
			out.Retry = true
			return out, nil
		}
	} else {
		out.Executed = true
	}
	st.MarkRepoEffect(out.Key)
	h.store.SaveRepo(st)
	return out, nil
}

// errKubeUnconfigured is the retryable failure a restate-owned Flux or image
// effect reports when no Kubernetes client is wired.
var errKubeUnconfigured = &KubeError{Message: "prstate: no kube client configured"}

// WithKube injects the Kubernetes/Flux boundary and returns h.
func (h *RepoHandlers) WithKube(k KubeEffector) *RepoHandlers {
	h.kube = k
	return h
}

// finishKubeEffect settles one single-call Flux effect: a retryable failure
// leaves the key uncommitted for the binding's delayed retry; success or a
// terminal failure commits it.
func (h *RepoHandlers) finishKubeEffect(st *RepoState, out RepoEffectOutcome, kerr *KubeError) RepoEffectOutcome {
	if kerr != nil {
		out.Kube = kerr
		if kerr.Retryable() {
			out.Retry = true
			return out
		}
	} else {
		out.Executed = true
	}
	st.MarkRepoEffect(out.Key)
	h.store.SaveRepo(st)
	return out
}

// HandleGitPush forwards one main push to the Flux receiver once per
// trigger key (repo, PR or 0, pushed SHA), mirroring synapse
// dispatchGitPush. It is repo-level, so a direct commit to main with no PR
// still dispatches.
func (h *RepoHandlers) HandleGitPush(ctx context.Context, ev RepoEvent) (RepoEffectOutcome, error) {
	ref := strings.TrimSpace(ev.Ref)
	if !strings.HasPrefix(ref, "refs/heads/main") {
		return RepoEffectOutcome{}, fmt.Errorf("%w: git push dispatch requires a main ref, got %q", ErrInvalidRepoEvent, ref)
	}
	st, out, done, err := h.beginRepoEffect(prwire.EffectFluxGitPush, ev)
	if done || err != nil {
		return out, err
	}
	if h.kube == nil {
		out.Retry, out.Kube = true, errKubeUnconfigured
		return out, nil
	}
	kerr := h.kube.DispatchGitPush(ctx, GitPushRequest{
		Repo: st.Repo, Ref: ref, Commit: repoEffectTrigger(ev), TargetID: strings.TrimSpace(ev.TargetID), PRNumber: ev.PRNumber,
	})
	return h.finishKubeEffect(st, out, kerr), nil
}

// sortedRequestNames returns the non-empty request names in order.
func sortedRequestNames(requests map[string]KustomizationRequest) []string {
	names := make([]string, 0, len(requests))
	for name := range requests {
		if strings.TrimSpace(name) != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// HandleImagePromotion promotes the event's images to its commit SHA
// (ConfigMap/aerial-image-tags plus the Flux receiver), mirroring synapse
// dispatchWorkflowRunImages phases c-e. Promotion is repo-level, so a direct
// commit to main promotes with no PR. Once every image is promoted the
// rollout starts in the same invocation, after the tags are written so one
// reconcile renders all of them: a PR rollout forwards
// HandleKustomizationRequested to PRObject (which records the requests
// before patching them), and an untracked rollout (no PR) reconciles here
// under its own key. A retryable failure leaves the failed key uncommitted for the
// binding's delayed retry; a failed restate-owned PR promotion also reports
// HandleDeployAttemptFailed so the PR's deploy-failure deadline arms.
func (h *RepoHandlers) HandleImagePromotion(ctx context.Context, ev RepoEvent) (RepoEffectOutcome, error) {
	repo := strings.TrimSpace(ev.Repo)
	commit := usableHeadSHA(ev.HeadSHA)
	images := cleanStrings(ev.Images)
	if repo == "" || commit == "" || len(images) == 0 {
		return RepoEffectOutcome{}, fmt.Errorf("%w: repo, head sha and images are required for image promotion", ErrInvalidRepoEvent)
	}
	st, err := h.loadOrNew(repo)
	if err != nil {
		return RepoEffectOutcome{}, err
	}
	ev = resolvePromotionPR(st, ev)
	out := RepoEffectOutcome{Key: RepoEffectKey(prwire.EffectImagePromote, ev)}
	var reconcileKey EffectKey
	if ev.PRNumber <= 0 && len(sortedRequestNames(ev.KustomizationRequests)) > 0 {
		reconcileKey = RepoEffectKey(prwire.EffectFluxReconcile, ev)
	}
	promoteDone := st.WasRepoEffect(out.Key)
	reconcileDone := reconcileKey.Kind == "" || st.WasRepoEffect(reconcileKey)
	if promoteDone && reconcileDone {
		out.Deduped = true
		return out, nil
	}
	if h.kube == nil {
		out.Retry, out.Kube = true, errKubeUnconfigured
		ferr := h.reportPromotionFailure(ctx, ev, out.Kube, &out)
		return out, ferr
	}
	if !promoteDone {
		if failed, ferr := h.promoteImages(ctx, st, ev, images, reconcileKey, &out); failed {
			return out, ferr
		}
	}
	if reconcileKey.Kind != "" && !reconcileDone {
		kerr := reconcileRequests(ctx, h.kube, ev.KustomizationRequests)
		if kerr != nil {
			out.Kube = kerr
			if kerr.Retryable() {
				out.Retry = true
				return out, nil
			}
		}
		st.MarkRepoEffect(reconcileKey)
		h.store.SaveRepo(st)
	}
	return out, nil
}

// resolvePromotionPR fills a SHA-only restate-owned promotion's PR from the durable SHA
// index (synapse sends the commit when no PR resolved statelessly), so the
// rollout it starts is tracked on the owning PR. The carried facts take the
// resolved PR number. An unresolved commit stays an untracked rollout.
func resolvePromotionPR(st *RepoState, ev RepoEvent) RepoEvent {
	if ev.PRNumber > 0 {
		return ev
	}
	entry, ok := st.ResolveSHA(ev.HeadSHA)
	if !ok {
		return ev
	}
	ev.PRNumber = entry.PRNumber
	if strings.TrimSpace(ev.Branch) == "" {
		ev.Branch = entry.Branch
	}
	ev.Facts = withFactPR(ev.Facts, entry.PRNumber)
	ev.DeployFailedFacts = withFactPR(ev.DeployFailedFacts, entry.PRNumber)
	return ev
}

// withFactPR returns facts with every missing PR number set to pr.
func withFactPR(facts []pubsub.PRLifecycleEvent, pr int) []pubsub.PRLifecycleEvent {
	if len(facts) == 0 {
		return facts
	}
	out := make([]pubsub.PRLifecycleEvent, len(facts))
	for i, f := range facts {
		if f.PRNumber <= 0 {
			f.PRNumber = pr
		}
		out[i] = f
	}
	return out
}

// promoteImages promotes every image (one failure never skips the rest,
// like synapse's concurrent promotion) and reports whether the promotion
// failed. Success commits the promotion key and starts a PR rollout; a
// terminal failure commits the promotion and any reconcile key, since no
// retry can succeed and synapse never reconciles after a failed promotion.
func (h *RepoHandlers) promoteImages(ctx context.Context, st *RepoState, ev RepoEvent, images []string, reconcileKey EffectKey, out *RepoEffectOutcome) (bool, error) {
	var first *KubeError
	retry := false
	for _, img := range images {
		workload, kerr := h.kube.PromoteImageTag(ctx, ImagePromotionRequest{
			Image: img, Repo: st.Repo, CommitSHA: usableHeadSHA(ev.HeadSHA), PRNumber: ev.PRNumber, TargetID: strings.TrimSpace(ev.TargetID),
		})
		if kerr != nil {
			if first == nil {
				first = kerr
			}
			retry = retry || kerr.Retryable()
			continue
		}
		if workload != "" {
			out.Promoted = append(out.Promoted, workload)
		}
	}
	if first != nil {
		out.Kube = first
		out.Retry = retry
		if !retry {
			st.MarkRepoEffect(out.Key)
			if reconcileKey.Kind != "" {
				st.MarkRepoEffect(reconcileKey)
			}
			h.store.SaveRepo(st)
		}
		return true, h.reportPromotionFailure(ctx, ev, first, out)
	}
	out.Executed = true
	st.MarkRepoEffect(out.Key)
	h.store.SaveRepo(st)
	if ev.PRNumber > 0 && len(sortedRequestNames(ev.KustomizationRequests)) > 0 {
		return false, h.forward(ctx, prwire.PRHandlerKustomizationRequested, promotedRolloutEvent(st.Repo, ev), out)
	}
	return false, nil
}

// promotedRolloutEvent is the restate-owned PR rollout a promotion starts:
// the deploying jobs, the exact reconcile requests, and the deploy_started
// fact synapse rendered.
func promotedRolloutEvent(repo string, ev RepoEvent) PREvent {
	return PREvent{
		Event: "kustomization_requested", Repo: repo, PRNumber: ev.PRNumber, HeadSHA: usableHeadSHA(ev.HeadSHA),
		Branch: strings.TrimSpace(ev.Branch), TargetID: strings.TrimSpace(ev.TargetID), Jobs: cleanStrings(ev.Jobs),
		KustomizationRequests: ev.KustomizationRequests, Facts: ev.Facts,
	}
}

// reportPromotionFailure reports one failed restate-owned PR promotion to
// PRObject, whose bounded deploy-failure deadline decides deploy_failed
// (synapse handleDeployAttemptFailure). Promotions without a PR report
// nothing.
func (h *RepoHandlers) reportPromotionFailure(ctx context.Context, ev RepoEvent, kerr *KubeError, out *RepoEffectOutcome) error {
	if ev.PRNumber <= 0 || kerr == nil {
		return nil
	}
	msg := kerr.Error()
	failed := make([]pubsub.PRLifecycleEvent, 0, len(ev.DeployFailedFacts))
	for _, f := range ev.DeployFailedFacts {
		f.Details = msg
		failed = append(failed, f)
	}
	head := usableHeadSHA(ev.HeadSHA)
	return h.forward(ctx, prwire.PRHandlerDeployAttemptFailed, PREvent{
		Event: "deploy_attempt_failed", Repo: strings.TrimSpace(ev.Repo), PRNumber: ev.PRNumber, HeadSHA: head, MergeSHA: head,
		Branch: strings.TrimSpace(ev.Branch), Banner: msg, Facts: failed,
	}, out)
}

// reconcileRequests patches every request in name order; one failure never
// skips the rest. It returns the first failure, made retryable when any
// failure was.
func reconcileRequests(ctx context.Context, kube KubeEffector, requests map[string]KustomizationRequest) *KubeError {
	var first *KubeError
	retry := false
	for _, name := range sortedRequestNames(requests) {
		req := requests[name]
		if kerr := kube.ReconcileKustomization(ctx, req.Namespace, name, req.RequestedAt); kerr != nil {
			if first == nil {
				first = &KubeError{Status: kerr.Status, Message: kerr.Message}
			}
			retry = retry || kerr.Retryable()
		}
	}
	if first != nil && retry && !first.Retryable() {
		first.Status = 0
	}
	return first
}
