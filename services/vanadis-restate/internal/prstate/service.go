package prstate

// Restate Virtual Object assembly for synapse PR lifecycle state.
//
// PRObject (key repo#prNumber) is the single ordered authority for per-PR
// state; RepoObject (key repo) holds only repo-level state: the bounded
// main-push index, dispatched-SHA tombstones, the SHA->PR routing index with
// open and deploying sets, and SHA-only events held until indexed. Postgres
// pr_registry (row, metadata, typed columns and child tables) stays a
// read-only projection synced by one journaled transactional send after
// the state commit and is never consulted to decide a transition.
//
// The services bind beside BuildRouter so the Restate deployment discovers
// PRObject and RepoObject; synapse sends every lifecycle event here and
// keeps no registry. Each PRObject handler loads Restate K/V
// state, applies the pure disposition guard (stale-drop, terminal-absorbing),
// and persists on apply. After the commit it runs the effects the event
// carries through journaled, generation-scoped EffectKeys: eri wakes and DMs
// (effects_eri.go) and aerial.pr.> lifecycle facts, whose publish also
// carries the BuildRouter route via vanadis-nats (effects_facts.go). Verdict
// handlers (HandleConflict, HandleDeploySuccess, HandleCIRecovered) decide,
// commit, then execute in one exclusive invocation. GitHub effects
// (auto-update cascade, update-branch, conflict checks, workflow reruns)
// run in service_github.go; Flux and image effects (git-push dispatch,
// image promotion, reconcile requests, readiness retry) in service_flux.go.
//
// The only cross-object traffic is one-way sends: PRObject sends RepoObject
// a HandlePRIndex update after each commit that changes a routed field, and
// RepoObject forwards resolved SHA-only events, main-advanced clears, and
// kustomization statuses to PRObject. No exclusive handler ever waits on
// another object, so the two can never deadlock.
//
// Handler signatures use restate.ObjectContext (exclusive mode), so these
// services intentionally do not satisfy the context.Context-based PRObject
// and RepoObject interfaces: those freeze the logic surface, this file binds
// the Restate surface. ResolvePRBySHA takes a single request struct because
// Restate handlers accept at most one input parameter.

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/pubsub"
)

// Frozen Restate service names. They match the synapse ingress invoker
// defaults (DefaultPRService/DefaultRepoService) exactly; any rename is a
// cross-module breaking change caught by TestRegisterPRStateServices. Every
// agent's PRs live in these single objects: the owning agent is PRState
// data, never part of the service name.
const (
	PRObjectServiceName   = "PRObject"
	RepoObjectServiceName = "RepoObject"
)

// Restate K/V state keys. One slot per object key; the object key itself
// (repo#prNumber, repo) is the slot the ingress invocation addresses.
const (
	prObjectStateKey   = "state"
	repoObjectStateKey = "state"
)

// PRObjectService is the dark PRObject Virtual Object. Handlers persist
// first-touch and applied state to Restate K/V and fire nothing.
type PRObjectService struct {
	stateKey   string
	projection ProjectionStore
	retryDelay time.Duration

	eri           EriEffector
	eriRetryDelay time.Duration

	facts                 FactPublisher
	factRetryDelay        time.Duration
	deployFailureDeadline time.Duration

	github GitHubEffector

	kube                KubeEffector
	reconcileRetryDelay time.Duration
}

// NewPRObjectService builds the single PRObject service. Without
// WithProjection it writes no pr_registry rows.
func NewPRObjectService() *PRObjectService {
	return &PRObjectService{
		stateKey: prObjectStateKey, retryDelay: DefaultProjectionRetry,
		eriRetryDelay: DefaultEriEffectRetryDelay, factRetryDelay: DefaultFactRetryDelay, deployFailureDeadline: DefaultDeployFailureDeadline,
		reconcileRetryDelay: DefaultRepoEffectRetryDelay,
	}
}

// ServiceName returns the single PRObject service name for every agent.
// Nil-safe so the registrar never panics on a zero service.
func (*PRObjectService) ServiceName() string {
	return PRObjectServiceName
}

func (s *PRObjectService) loadOrCreatePRState(ctx restate.ObjectSharedContext, ev PREvent) (*PRState, error) {
	st, _, err := s.loadPRState(ctx, ev)
	return st, err
}

// loadPRState is loadOrCreatePRState that also reports whether the state
// was stored (false when it was just created for this event).
func (s *PRObjectService) loadPRState(ctx restate.ObjectSharedContext, ev PREvent) (*PRState, bool, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return nil, false, err
	}
	stored := st != nil
	if st == nil {
		st = NewPRState(ev.Repo, ev.PRNumber)
	}
	// Lifecycle events carry the agent only as a hint: it fills an empty
	// owner slot but never overwrites a stored owner. Register and PR-body
	// metadata on opened claim ownership via ClaimOwner instead.
	st.ApplyAgentHint(ev.Agent)
	return st, stored, nil
}

// untrackedEventsTotal counts rollout events dropped because the PR is
// untracked (PRState.Untracked), by handler.
var untrackedEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "prstate_untracked_events_total",
	Help: "Rollout events PRObject dropped because the PR never saw register, opened or merge (legacy keeps no row for it), by handler.",
}, []string{"handler"})

func init() {
	registerCollector(untrackedEventsTotal)
}

// dropUntrackedRollout drops one rollout event for an untracked PR, the way
// the legacy registry's deploy transitions return no target and record
// nothing when no row exists: it counts and logs the drop, persists nothing
// for a state this event created, and sends no index update, fact or
// reconcile. A stored untracked state with rollout residue (created before
// this guard) is cleaned instead (cleanUntrackedResidue). Returns true when
// the event must be dropped.
func (s *PRObjectService) dropUntrackedRollout(ctx restate.ObjectContext, st *PRState, stored bool, handler string) bool {
	if !st.Untracked() {
		return false
	}
	pr := PRObjectKey(st.Repo, st.PRNumber)
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		untrackedEventsTotal.WithLabelValues(handler).Inc()
		slog.InfoContext(rc, "[prstate] dropping rollout event for untracked PR (no register, opened or merge seen)",
			"pr", pr, "handler", handler, "stored", stored)
		return restate.Void{}, nil
	}, restate.WithName("untracked-"+handler))
	if stored {
		s.cleanUntrackedResidue(ctx, st)
	}
	return true
}

// dropUntrackedEvent loads the PR state for a handler that loads it itself
// later and applies dropUntrackedRollout. A malformed event is not dropped
// here; the handler's own validation decides.
func (s *PRObjectService) dropUntrackedEvent(ctx restate.ObjectContext, ev PREvent, handler string) (bool, error) {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return false, nil
	}
	st, stored, err := s.loadPRState(ctx, ev)
	if err != nil {
		return false, err
	}
	return s.dropUntrackedRollout(ctx, st, stored, handler), nil
}

// cleanUntrackedResidue clears the rollout fields of a stored untracked
// state and closes its index entries: the rollout index gets an inactive
// update (syncRolloutIndex) and RepoObject a deployed update for the
// indexed generation, so it leaves the deploying set. Idempotent: a state
// with no residue changes nothing and sends nothing.
func (s *PRObjectService) cleanUntrackedResidue(ctx restate.ObjectContext, st *PRState) bool {
	if !st.Untracked() || !st.HasDeployResidue() {
		return false
	}
	st.ClearDeployResidue()
	restate.Set(ctx, s.stateKey, st)
	s.syncRolloutIndex(ctx, st)
	if st.GateGeneration > 0 {
		restate.ObjectSend(ctx, RepoObjectServiceName, RepoObjectKey(st.Repo), RepoHandlerPRIndex).Send(PRIndexEvent{
			Repo: st.Repo, PRNumber: st.PRNumber, ReopenEpoch: st.ReopenEpoch, Generation: st.GateGeneration, Lifecycle: IndexDeployed,
		})
	}
	return true
}

// DropUntrackedRequest is the empty DropUntracked input; the object key
// names the PR.
type DropUntrackedRequest struct{}

// DropUntrackedResponse reports whether DropUntracked cleaned anything.
type DropUntrackedResponse struct {
	Untracked bool `json:"untracked"`
	Cleaned   bool `json:"cleaned"`
}

// DropUntracked is the idempotent operator cleanup for an untracked PR left
// in rollout before the untracked-event guard: it clears the rollout fields
// and closes the PR's rollout-index and RepoObject deploying entries. A
// tracked or unknown PR is untouched.
func (s *PRObjectService) DropUntracked(ctx restate.ObjectContext, _ DropUntrackedRequest) (DropUntrackedResponse, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil || st == nil {
		return DropUntrackedResponse{}, err
	}
	if !st.Untracked() {
		return DropUntrackedResponse{}, nil
	}
	return DropUntrackedResponse{Untracked: true, Cleaned: s.cleanUntrackedResidue(ctx, st)}, nil
}

// eventPhase selects the disposition guard for one handler: PR-lifecycle
// events are absorbed by any same-epoch terminal state, while deploy-phase
// events still apply to merged PRs (deploys happen after merge).
type eventPhase int

const (
	phaseLifecycle eventPhase = iota
	phaseDeploy
	// phaseRollout is a deploy-phase event that can start or advance a
	// rollout (TransitionDeploying, reconcile requests, rollout progress,
	// deploy failure). It is dropped for an untracked PR.
	phaseRollout
	// phaseMerge is a merge event (MergeDisposition): a live merge SHA on
	// a non-terminal PR applies whatever head it carries.
	phaseMerge
)

func (s *PRObjectService) mutatePREvent(ctx restate.ObjectContext, ev PREvent, mutate func(*PRState)) error {
	return s.mutatePhase(ctx, ev, phaseLifecycle, mutate)
}

func (s *PRObjectService) mutateDeployEvent(ctx restate.ObjectContext, ev PREvent, mutate func(*PRState)) error {
	return s.mutatePhase(ctx, ev, phaseDeploy, mutate)
}

func (s *PRObjectService) mutateRolloutEvent(ctx restate.ObjectContext, ev PREvent, handler string, mutate func(*PRState)) error {
	return s.mutateRolloutPhaseEvent(ctx, ev, handler, func(st *PRState, _ *PREvent) {
		if mutate != nil {
			mutate(st)
		}
	})
}

func (s *PRObjectService) mutatePhase(ctx restate.ObjectContext, ev PREvent, phase eventPhase, mutate func(*PRState)) error {
	return s.mutatePhaseEvent(ctx, ev, phase, func(st *PRState, _ *PREvent) {
		if mutate != nil {
			mutate(st)
		}
	})
}

// mutatePhaseEvent is mutatePhase for handlers whose transition decides
// which of the carried facts still apply: mutate may trim ev.Facts before
// they run.
func (s *PRObjectService) mutatePhaseEvent(ctx restate.ObjectContext, ev PREvent, phase eventPhase, mutate func(*PRState, *PREvent)) error {
	return s.mutateEvent(ctx, ev, phase, "", mutate)
}

// mutateRolloutPhaseEvent is mutatePhaseEvent for rollout events: an
// untracked PR drops the event (dropUntrackedRollout) before any mutation.
func (s *PRObjectService) mutateRolloutPhaseEvent(ctx restate.ObjectContext, ev PREvent, handler string, mutate func(*PRState, *PREvent)) error {
	return s.mutateEvent(ctx, ev, phaseRollout, handler, mutate)
}

func (s *PRObjectService) mutateEvent(ctx restate.ObjectContext, ev PREvent, phase eventPhase, handler string, mutate func(*PRState, *PREvent)) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, stored, err := s.loadPRState(ctx, ev)
	if err != nil {
		return err
	}
	if phase == phaseRollout && s.dropUntrackedRollout(ctx, st, stored, handler) {
		return nil
	}
	if dispositionFor(st, ev, phase) != DispositionApply {
		return nil
	}
	before := snapshotIndexView(st)
	applyEventRow(st, ev)
	if phase != phaseLifecycle && phase != phaseMerge {
		recoverMergedHead(ctx, st, ev)
	}
	mutate(st, &ev)
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, ev, lifecycle)
	}
	s.runEriEffects(ctx, st, ev)
	s.runFacts(ctx, st, ev.Facts)
	s.afterCommit(ctx, st)
	return nil
}

func dispositionFor(st *PRState, ev PREvent, phase eventPhase) EventDisposition {
	switch phase {
	case phaseDeploy, phaseRollout:
		return st.ApplyDeployEvent(ev)
	case phaseMerge:
		return st.MergeDisposition(ev)
	case phaseLifecycle:
	}
	return st.ApplyEvent(ev)
}

// applyEventRow applies the registry-row fields every applied event may
// carry: the first-seen head SHA (never moved once merged, so a deploy
// event carrying the merge commit cannot rewrite the PR head; later head
// changes arrive through HandleSynchronized or Register), branch/title
// identity, and a missing target ID.
func applyEventRow(st *PRState, ev PREvent) {
	if head := strings.TrimSpace(ev.HeadSHA); head != "" && !IsZeroSHA(head) && st.Terminal != TerminalMerged &&
		(st.HeadSHA == "" || !IsMergeOnlyRef(head, ev.MergeSHA)) {
		st.SetHead(head)
	}
	st.ApplyIdentity(prBranch(ev.Branch), ev.Title)
	if tid := strings.TrimSpace(ev.TargetID); st.TargetID == "" && IsValidDiscordSnowflake(tid) {
		st.TargetID = tid
	}
}

// prBranch drops a main-branch ref: deploy-phase events and squash-merge
// pushes carry the branch the workflow or push ran on, which is never the
// PR head branch the registry row keeps.
func prBranch(branch string) string {
	switch strings.TrimSpace(branch) {
	case "main", "refs/heads/main":
		return ""
	default:
		return branch
	}
}

// prIndexView is the slice of PRState RepoObject routes on.
type prIndexView struct {
	head     string
	terminal TerminalState
	epoch    uint64
	gen      uint64
}

func snapshotIndexView(st *PRState) prIndexView {
	return prIndexView{head: st.HeadSHA, terminal: st.Terminal, epoch: st.ReopenEpoch, gen: st.GateGeneration}
}

// indexLifecycle derives the index op for one committed transition, or ""
// when nothing RepoObject routes on changed.
func indexLifecycle(before, after prIndexView) string {
	switch {
	case after.terminal != before.terminal && after.terminal == TerminalClosed:
		return IndexClosed
	case after.terminal != before.terminal && after.terminal == TerminalMerged:
		return IndexMerged
	case after.gen > before.gen:
		return IndexDeploying
	case after.head != before.head || after.epoch != before.epoch:
		return IndexActive
	default:
		return ""
	}
}

// sendPRIndex sends RepoObject one one-way index update after the state
// commit. Only merge and deploy ops carry the event merge SHA, so a merged
// PR's squash commit resolves to it.
func sendPRIndex(ctx restate.ObjectContext, st *PRState, ev PREvent, lifecycle string) {
	idx := PRIndexEvent{
		Repo:        st.Repo,
		PRNumber:    st.PRNumber,
		Branch:      strings.TrimSpace(ev.Branch),
		HeadSHA:     st.HeadSHA,
		ReopenEpoch: st.ReopenEpoch,
		Generation:  st.GateGeneration,
		Lifecycle:   lifecycle,
	}
	if lifecycle == IndexMerged || lifecycle == IndexDeploying || lifecycle == IndexDeployed {
		merge := strings.TrimSpace(ev.MergeSHA)
		if merge == "" || IsZeroSHA(merge) {
			merge = strings.TrimSpace(st.MergeSHA)
		}
		if merge != "" && !IsZeroSHA(merge) {
			idx.MergeSHA = merge
		}
	}
	if idx.Branch == "" {
		idx.Branch = st.Branch
	}
	restate.ObjectSend(ctx, RepoObjectServiceName, RepoObjectKey(st.Repo), RepoHandlerPRIndex).Send(idx)
}

// applyPREvent loads the PR state, drops stale and same-epoch terminal
// events, and persists applied state. Effect and routing sends land in a
// later chunk; this assembly step only proves the K/V round-trip.
func (s *PRObjectService) applyPREvent(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, nil)
}

// HandleCheckFailed applies one check-failure event. Eri wakes the event
// carries (EriEffects) fire to the stored owner after the commit.
func (s *PRObjectService) HandleCheckFailed(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		if strings.TrimSpace(ev.CheckName) != "" {
			st.RecordFailedCheckRunDetailed(ev.HeadSHA, ev.CheckName)
			if !IsRoboRevCheck(ev.CheckName) {
				st.RecordNonRoboRevFailedSHA(ev.HeadSHA)
			}
		}
		st.RecordFailedCISHA(ev.HeadSHA)
		st.ApplyRegistryCIFailed()
	})
}

// HandleCheckPassed applies one check-pass event dark.
func (s *PRObjectService) HandleCheckPassed(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		if strings.TrimSpace(ev.CheckName) != "" {
			st.ClearFailedCheckRun(ev.HeadSHA, ev.CheckName)
		}
		if !st.HasActiveNonRoboRevFailures(ev.HeadSHA) {
			st.DeleteNonRoboRevFailedSHA()
		}
		if !st.HasActiveFailedCheckRuns(ev.HeadSHA) {
			st.ClearFailedCISHA()
		}
		if !st.IsCIFailedForSHA(st.HeadSHA) {
			st.ApplyRegistryRecovered(RegistryStatusCIFailed)
		}
	})
}

// HandleCheckRerunRequested applies one check-rerun request dark.
func (s *PRObjectService) HandleCheckRerunRequested(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleCollaboratorAlert applies one collaborator alert. The collaborator
// DM synapse rendered (an eri_direct_message EriEffect) fires once per
// generation-scoped EffectKey through the owner's Discord sender when
// prstate owns the event's effects.
func (s *PRObjectService) HandleCollaboratorAlert(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.MarkCollaboratorAlerted(ev.HeadSHA)
	})
}

// HandleConflict is the merged conflict verdict: one exclusive invocation
// resolves the republish decision, commits the published conflict SHA and
// the conflict row status, then publishes the conflict facts the event
// carries.
func (s *PRObjectService) HandleConflict(ctx restate.ObjectContext, ev PREvent) (ConflictDispatchDecision, error) {
	return runVerdict(s, ctx, ev, ResolveConflictDispatchDecision,
		func(d ConflictDispatchDecision) bool { return d.ShouldPublish },
		func(st *PRState) error {
			_, err := s.commitConflictPublished(ctx, st, ev)
			return err
		})
}

// runVerdict is the decide-commit-execute core of the merged verdict
// handlers: it loads the PR, resolves the verdict, and when fires reports
// the verdict applies, commits it, then runs the carried facts and syncs the
// projection. Malformed events resolve the zero verdict.
func runVerdict[D any](s *PRObjectService, ctx restate.ObjectContext, ev PREvent, resolve func(*PRState, PREvent) (D, error), fires func(D) bool, commit func(*PRState) error) (D, error) {
	var zero D
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return zero, nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return zero, err
	}
	dec, err := resolve(st, ev)
	if err != nil || !fires(dec) {
		return dec, err
	}
	if err := commit(st); err != nil {
		return dec, err
	}
	s.runFacts(ctx, st, ev.Facts)
	s.afterCommit(ctx, st)
	return dec, nil
}

// commitConflictPublished records the published conflict SHA and the
// conflict row status, and persists them on apply.
func (s *PRObjectService) commitConflictPublished(ctx restate.ObjectContext, st *PRState, ev PREvent) (ConflictPublishedOutcome, error) {
	out, err := RecordConflictPublished(st, ev)
	if err != nil || out.Disposition != DispositionApply {
		return out, err
	}
	st.ApplyRegistryConflict()
	restate.Set(ctx, s.stateKey, st)
	return out, nil
}

// HandleConflictDetected is the old shared verdict read, a thin delegate of
// the HandleConflict decision that records nothing. synapse's legacy registry
// pipeline still names it until r2 deletes that pipeline, so it stays
// registered until r2b; like every handler it rejects a legacy stamp.
func (s *PRObjectService) HandleConflictDetected(ctx restate.ObjectSharedContext, ev PREvent) (ConflictDispatchDecision, error) {
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return ConflictDispatchDecision{}, err
	}
	return ResolveConflictDispatchDecision(st, ev)
}

// HandleConflictPublished is the old publish ack, an idempotent delegate of
// the HandleConflict commit. synapse's legacy registry
// pipeline still names it until r2 deletes that pipeline, so it stays
// registered until r2b; like every handler it rejects a legacy stamp.
func (s *PRObjectService) HandleConflictPublished(ctx restate.ObjectContext, ev PREvent) (ConflictPublishedOutcome, error) {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return ConflictPublishedOutcome{}, nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return ConflictPublishedOutcome{}, err
	}
	out, err := s.commitConflictPublished(ctx, st, ev)
	if err != nil || out.Disposition != DispositionApply {
		return out, err
	}
	s.afterCommit(ctx, st)
	return out, nil
}

// HandleCIRecovered is the merged CI-recovery verdict: when the Restate
// failure slots say the PR is CI-failed for the event head, it clears the
// recovered check (and the failed-CI SHA once no failed check remains),
// reopens the ci_failed row, commits, then publishes the recovery facts the event carries. A PR that is not CI-failed
// commits and publishes nothing.
func (s *PRObjectService) HandleCIRecovered(ctx restate.ObjectContext, ev PREvent) (CIRecoveryDecision, error) {
	return runVerdict(s, ctx, ev, ResolveCIRecovery,
		func(d CIRecoveryDecision) bool { return d.CIFailed },
		func(st *PRState) error {
			applyCIRecovered(st, ev)
			restate.Set(ctx, s.stateKey, st)
			forwardRecoveredAutoUpdate(ctx, st, ev)
			return nil
		})
}

// forwardRecoveredAutoUpdate re-triggers the open-PR auto-update cascade for
// an applied CI recovery that carries an auto-update policy (restate-owned
// check_run recoveries only). Only PRObject knows the PR was CI-failed, so
// it replaces legacy synapse's recovered-check trigger; RepoObject keys the
// effect on the PR head and check, exactly as the legacy trigger did.
func forwardRecoveredAutoUpdate(ctx restate.ObjectContext, st *PRState, ev PREvent) {
	if ev.AutoUpdate == nil {
		return
	}
	restate.ObjectSend(ctx, RepoObjectServiceName, RepoObjectKey(st.Repo), RepoHandlerAutoUpdate).Send(RepoEvent{
		Repo: st.Repo, PRNumber: st.PRNumber, HeadSHA: ev.HeadSHA, CheckName: ev.CheckName,
		AutoUpdate: ev.AutoUpdate,
	})
}

// applyCIRecovered clears the recovered check and, once no failed check
// remains on the head, the failed-CI SHA, and reopens the ci_failed row.
func applyCIRecovered(st *PRState, ev PREvent) {
	if strings.TrimSpace(ev.CheckName) != "" {
		st.ClearFailedCheckRun(ev.HeadSHA, ev.CheckName)
	}
	if !st.HasActiveFailedCheckRuns(ev.HeadSHA) {
		st.ClearFailedCISHA()
	}
	st.ApplyRegistryRecovered(RegistryStatusCIFailed)
}

// HandleCIRecovery is the old shared CI-failed read, a thin delegate of the
// HandleCIRecovered decision that records nothing. synapse's legacy registry
// pipeline still names it until r2 deletes that pipeline, so it stays
// registered until r2b; like every handler it rejects a legacy stamp.
func (s *PRObjectService) HandleCIRecovery(ctx restate.ObjectSharedContext, ev PREvent) (CIRecoveryDecision, error) {
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return CIRecoveryDecision{}, err
	}
	return ResolveCIRecovery(st, ev)
}

// HandleConflictResolved applies one conflict-resolved event dark.
func (s *PRObjectService) HandleConflictResolved(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		resolveConflict(st, ev.HeadSHA)
	})
}

// resolveConflict clears the published conflict and the collaborator
// conflict alert for headSHA and reopens a conflict row.
func resolveConflict(st *PRState, headSHA string) {
	st.ClearConflictPublished()
	st.ClearCollaboratorConflict(headSHA)
	st.ApplyRegistryRecovered(RegistryStatusConflict)
}

// HandleMerged applies one merge event. A squash-merge push that arrives
// before pull_request.closed merges the PR on its merge SHA; the later
// closed event for the same merge SHA no longer changes state but still runs
// its carried facts, because legacy synapse publishes a merged fact for both
// (each keyed on its own head SHA, so true redeliveries still dedupe). That
// closed event carries the real PR head, so it also repairs a head PRObject
// left stale by a missed synchronize (RecoverMergedHead). The first merge
// of an open PR applies on its merge SHA even when PRObject's head is stale
// (MergeDisposition) and adopts the carried head unless the PR already
// moved past it (MergeEventHead); synapse also sends it when a deploy
// commit lookup proves a merge whose webhooks were lost.
func (s *PRObjectService) HandleMerged(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	if st.Terminal == TerminalMerged && ev.ReopenEpoch <= st.ReopenEpoch &&
		st.MergeSHA != "" && strings.EqualFold(st.MergeSHA, strings.TrimSpace(ev.MergeSHA)) {
		s.handleSameMerge(ctx, st, ev)
		return nil
	}
	ev, stale := st.MergeEventHead(ev)
	prev := st.HeadSHA
	return s.mutateEvent(ctx, ev, phaseMerge, "", func(st *PRState, _ *PREvent) {
		st.ClearFailedCISHA()
		st.MarkTerminal(TerminalMerged)
		st.ApplyRegistryMerged(ev.MergeSHA)
		if stale {
			noteMergedHeadRecovered(ctx, st, prev)
		}
	})
}

// handleSameMerge handles a merge event for the merge SHA the PR already
// merged on: it repairs a stale head the event corrects (persisted and
// indexed for RepoObject SHA routing) and runs the facts it carries.
func (s *PRObjectService) handleSameMerge(ctx restate.ObjectContext, st *PRState, ev PREvent) {
	before := snapshotIndexView(st)
	recovered := recoverMergedHead(ctx, st, ev)
	if recovered {
		restate.Set(ctx, s.stateKey, st)
		sendPRIndex(ctx, st, ev, indexLifecycle(before, snapshotIndexView(st)))
	}
	if len(ev.Facts) > 0 {
		s.runFacts(ctx, st, ev.Facts)
	}
	if recovered || len(ev.Facts) > 0 {
		s.afterCommit(ctx, st)
	}
}

// mergeHeadRecoveredTotal counts stale heads of merged PRs repaired from a
// merge or deploy-phase event (RecoverMergedHead).
var mergeHeadRecoveredTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "prstate_merge_head_recovered_total",
	Help: "Merged PRs whose stale stored head PRObject replaced with the real head carried by a merge event or an applied deploy-phase event.",
})

func init() {
	registerCollector(mergeHeadRecoveredTotal)
}

// recoverMergedHead applies RecoverMergedHead to st and, when it moved the
// head, counts and logs the repair (noteMergedHeadRecovered). The caller persists st and sends the
// head change to RepoObject.
func recoverMergedHead(ctx context.Context, st *PRState, ev PREvent) bool {
	prev, ok := st.RecoverMergedHead(ev)
	if ok {
		noteMergedHeadRecovered(ctx, st, prev)
	}
	return ok
}

// noteMergedHeadRecovered counts and logs one merged stale head repair.
func noteMergedHeadRecovered(ctx context.Context, st *PRState, prev string) {
	mergeHeadRecoveredTotal.Inc()
	slog.WarnContext(ctx, "[prstate] merged stale head recovered", "pr", PRObjectKey(st.Repo, st.PRNumber),
		"stored", prev, "head", st.HeadSHA, "merge", st.MergeSHA)
}

// HandleClosed applies one close event dark.
func (s *PRObjectService) HandleClosed(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.ClearFailedCISHA()
		st.DisarmDeployFailure()
		st.MarkTerminal(TerminalClosed)
		st.ApplyRegistryClosed()
	})
}

// HandleReopened applies one reopen event dark.
func (s *PRObjectService) HandleReopened(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.Reopen()
	})
}

// HandleOpened applies one pull_request opened event. The agent it carries
// comes only from the PR-body bot metadata, one of the three authoritative
// ownership sources, so it claims the owner instead of acting as a hint.
// The opened fact it carries publishes after the commit.
func (s *PRObjectService) HandleOpened(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		if agent := strings.TrimSpace(ev.Agent); agent != "" {
			st.ClaimOwner(agent)
			st.BotID = agent
		}
	})
}

// HandleSelfRegistered applies synapse's pull_request self-registration
// (the EnsurePRRegistered upsert, see PRState.ApplySelfRegistration). Synapse
// journals it before the legacy write in shadow mode and sends it in the
// restate-owned path, carrying the target legacy resolves, the title and the
// PR-body owner. It is neither opened (no owner claim on a tracked PR, no
// fact) nor a synchronize (the head only fills when empty, so a later
// HandleSynchronized still sees the head change and clears head-scoped
// state). The agent is the insert's owner seed, not a lifecycle hint, so it
// skips loadPRState's hint.
func (s *PRObjectService) HandleSelfRegistered(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return err
	}
	if st == nil {
		st = NewPRState(ev.Repo, ev.PRNumber)
	}
	before := snapshotIndexView(st)
	if !st.ApplySelfRegistration(ev.Branch, ev.HeadSHA, ev.TargetID, ev.Title, ev.Agent) {
		return nil
	}
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, ev, lifecycle)
	}
	s.afterCommit(ctx, st)
	return nil
}

// HandleRolloutProgress applies one rollout-progress event. It is a
// deploy-phase event, so merged PRs apply it: ready jobs union into
// deployed_jobs, completed Kustomizations into deployed_kustomizations
// (flipping the PR to deployed once every request completed), and a carried
// job state records into job_states. RepoObject kustomization-status
// forwards carry no ready lists and only refresh the row.
//
// A carried rollout_updated fact publishes only while the rollout is still
// deploying: once the last Kustomization flips the PR to deployed, the
// deploy_success verdict publishes instead, exactly like the legacy watcher.
func (s *PRObjectService) HandleRolloutProgress(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateRolloutPhaseEvent(ctx, ev, PRHandlerRolloutProgress, func(st *PRState, ev *PREvent) {
		st.MarkJobsDeployed(ev.DeployedJobs)
		st.MarkKustomizationsDeployed(ev.DeployedKustomizations)
		st.RecordJobState(ev.JobName, ev.JobState)
		if st.Status == RegistryStatusDeployed {
			ev.Facts = withoutFactEvent(ev.Facts, FactRolloutUpdated)
		}
	})
}

// FactRolloutUpdated is the watcher's per-Kustomization progress fact.
const FactRolloutUpdated = "rollout_updated"

// withoutFactEvent drops every fact named event.
func withoutFactEvent(facts []pubsub.PRLifecycleEvent, event string) []pubsub.PRLifecycleEvent {
	var out []pubsub.PRLifecycleEvent
	for _, f := range facts {
		if !strings.EqualFold(strings.TrimSpace(f.Event), event) {
			out = append(out, f)
		}
	}
	return out
}

// synchronizeRecoveredTotal counts synchronizes that recovered a stale
// stored head (DispositionRecoverStale).
var synchronizeRecoveredTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "prstate_synchronize_recovered_total",
	Help: "Synchronizes PRObject applied although its stored head matched neither the event head nor its before SHA (a missed synchronize), moving the head last-write-wins.",
})

func init() {
	registerCollector(synchronizeRecoveredTotal)
}

// HandleSynchronized advances the PR head on a synchronize under the
// BeforeSHA compare-and-set guard (recovering a stale stored head, see
// SynchronizeDisposition) and applies the UpdatePRSync row transition. The
// new head is indexed for RepoObject SHA routing.
func (s *PRObjectService) HandleSynchronized(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	disp := st.SynchronizeDisposition(ev)
	if disp != DispositionApply && disp != DispositionRecoverStale {
		return nil
	}
	before := snapshotIndexView(st)
	prevHead := st.HeadSHA
	wasConflict := st.Status == RegistryStatusConflict
	if disp == DispositionRecoverStale {
		synchronizeRecoveredTotal.Inc()
		slog.WarnContext(ctx, "[prstate] synchronize recovered stale head", "pr", PRObjectKey(st.Repo, st.PRNumber),
			"stored", prevHead, "before", strings.TrimSpace(ev.BeforeSHA), "head", strings.TrimSpace(ev.HeadSHA))
	}
	st.SetHead(ev.HeadSHA)
	st.ApplyRegistrySync(prevHead, st.HeadSHA)
	if wasConflict {
		// A synchronize resolves a stored conflict (legacy's follow-up
		// HandleConflictResolved): only PRObject knows the row was in
		// conflict, so a restate-owned edge never sends that follow-up.
		resolveConflict(st, ev.HeadSHA)
	}
	st.DisarmDeployFailure()
	st.ApplyIdentity(ev.Branch, ev.Title)
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, ev, lifecycle)
	}
	s.runFacts(ctx, st, ev.Facts)
	s.afterCommit(ctx, st)
	return nil
}

// HandleDeployFailed applies one deploy failure (AtomicTransitionDeployFailed)
// or, when Acknowledged, the confirmed deploy_failed publish
// (AcknowledgeDeployFailedPublished). Merged PRs apply it.
func (s *PRObjectService) HandleDeployFailed(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateRolloutEvent(ctx, ev, PRHandlerDeployFailed, func(st *PRState) {
		st.DisarmDeployFailure()
		if ev.Acknowledged {
			st.AckDeployFailedPublished()
			return
		}
		st.ApplyRegistryDeployFailed(ev.Banner)
	})
}

// HandleBuildMetadata records one build-metadata snapshot under the
// RecordPRBuildMetadata revision and ordering fence. Merged PRs apply it,
// since post-merge builds fence on the merge SHA.
func (s *PRObjectService) HandleBuildMetadata(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateDeployEvent(ctx, ev, func(st *PRState) {
		st.RecordBuildMetadata(ev.BuildMetadata)
	})
}

// HandleCheckStatus records one ci_checks value (RecordPRCheckStatus): the
// check state on an open PR's head SHA or a merged/synced PR's merge SHA.
// Synapse sends it exactly where the legacy registry records a check
// status (pending and passed runs, never gate aggregators). Merged PRs
// apply it; closed PRs absorb it.
func (s *PRObjectService) HandleCheckStatus(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateDeployEvent(ctx, ev, func(st *PRState) {
		st.RecordCheckState(ev.HeadSHA, ev.CheckName, ev.CheckState)
	})
}

// HandleDeploySuccess is the merged deploy_success verdict: one exclusive
// invocation resolves the publish decision from the deploy_dispatch key and
// both dedup slots, commits them plus the deployed row status, then
// publishes the deploy_success facts the event carries.
func (s *PRObjectService) HandleDeploySuccess(ctx restate.ObjectContext, ev PREvent) (DeployDispatchDecision, error) {
	if dropped, err := s.dropUntrackedEvent(ctx, ev, PRHandlerDeploySuccess); dropped || err != nil {
		return DeployDispatchDecision{}, err
	}
	return runVerdict(s, ctx, ev, ResolveDeployDispatchDecision,
		func(d DeployDispatchDecision) bool { return d.ShouldPublish },
		func(st *PRState) error {
			_, err := s.commitDeploySuccess(ctx, st, ev)
			return err
		})
}

// commitDeploySuccess records the deploy_dispatch key, both dedup slots and
// the deployed row status, disarms any pending deploy-failure deadline, and
// persists them on apply. A first record indexes the PR as deployed.
func (s *PRObjectService) commitDeploySuccess(ctx restate.ObjectContext, st *PRState, ev PREvent) (DeployPublishedOutcome, error) {
	before := snapshotIndexView(st)
	out, err := RecordDeployPublished(st, ev)
	if err != nil || out.Disposition != DispositionApply {
		return out, err
	}
	headRecovered := st.Terminal == TerminalMerged && st.HeadSHA != before.head
	if headRecovered {
		noteMergedHeadRecovered(ctx, st, before.head)
	}
	// Only a watcher-tracked rollout flips the row: legacy records the
	// completed Kustomization and clears deploy_success_pending, while direct
	// alert and sync deploys never touch the registry status.
	if len(st.KustomizationRequests) > 0 {
		st.MarkKustomizationsDeployed([]string{ev.JobName})
		if st.Status == RegistryStatusDeployed {
			st.AckDeploySuccess()
		}
	}
	st.DisarmDeployFailure()
	restate.Set(ctx, s.stateKey, st)
	switch {
	case !out.Deduped:
		sendPRIndex(ctx, st, ev, IndexDeployed)
	case headRecovered:
		sendPRIndex(ctx, st, ev, IndexActive)
	}
	return out, nil
}

// HandleDeployDispatched is the old shared verdict read, a thin delegate of
// the HandleDeploySuccess decision that records nothing. synapse's legacy registry
// pipeline still names it until r2 deletes that pipeline, so it stays
// registered until r2b; like every handler it rejects a legacy stamp.
func (s *PRObjectService) HandleDeployDispatched(ctx restate.ObjectSharedContext, ev PREvent) (DeployDispatchDecision, error) {
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return DeployDispatchDecision{}, err
	}
	return ResolveDeployDispatchDecision(st, ev)
}

// HandleDeploySuccessPublished is the old publish ack, an idempotent
// delegate of the HandleDeploySuccess commit. synapse's legacy registry
// pipeline still names it until r2 deletes that pipeline, so it stays
// registered until r2b; like every handler it rejects a legacy stamp.
func (s *PRObjectService) HandleDeploySuccessPublished(ctx restate.ObjectContext, ev PREvent) (DeployPublishedOutcome, error) {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return DeployPublishedOutcome{}, nil
	}
	st, stored, err := s.loadPRState(ctx, ev)
	if err != nil {
		return DeployPublishedOutcome{}, err
	}
	if s.dropUntrackedRollout(ctx, st, stored, "HandleDeploySuccessPublished") {
		return DeployPublishedOutcome{}, nil
	}
	out, err := s.commitDeploySuccess(ctx, st, ev)
	if err != nil || out.Disposition != DispositionApply {
		return out, err
	}
	s.afterCommit(ctx, st)
	return out, nil
}

// Register is the request-response PR register: it fences the existing
// owner (authorizeExistingPROwner), resolves the headless target (request
// snowflake, stored target, system-channel fallback), applies the
// RegisterPR upsert to state, and returns the resolved target ID. Invalid
// requests and bad targets fail terminally with 400, an owner mismatch
// with 409, so no retry repeats them.
func (s *PRObjectService) Register(ctx restate.ObjectContext, req RegisterRequest) (RegisterResponse, error) {
	repo := NormalizeRepo(req.Repo)
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return RegisterResponse{}, err
	}
	if st == nil {
		st = NewPRState(repo, req.PRNumber)
	}
	verdict, targetID := st.ResolveRegister(req)
	switch verdict {
	case RegisterOK:
	case RegisterOwnerMismatch:
		return RegisterResponse{}, restate.ToTerminalError(errRegisterOwnerMismatch, restate.WithErrorCode(409))
	case RegisterBadTarget:
		return RegisterResponse{}, restate.ToTerminalError(errRegisterBadTarget, restate.WithErrorCode(400))
	default:
		return RegisterResponse{}, restate.ToTerminalError(errRegisterInvalid, restate.WithErrorCode(400))
	}
	before := snapshotIndexView(st)
	st.ApplyRegister(req, targetID)
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, PREvent{Repo: repo, PRNumber: req.PRNumber, Branch: st.Branch}, lifecycle)
	}
	s.afterCommit(ctx, st)
	return RegisterResponse{TargetID: targetID, Agent: st.OwnerAgent(), Status: st.RegistryStatus()}, nil
}

// Register error causes, wrapped as terminal errors.
var (
	errRegisterInvalid       = errors.New("prstate: register requires repo, pr_number, branch and head_sha")
	errRegisterOwnerMismatch = errors.New("prstate: register caller is not authorized for the existing owner")
	errRegisterBadTarget     = errors.New("prstate: target_id must be a valid Discord snowflake (17-20 digits)")
)

// BuildLifecycleRequest is the empty GetBuildLifecycle input; the object
// key already names the PR.
type BuildLifecycleRequest struct{}

// GetBuildLifecycle is the shared, read-only build-lifecycle query matching
// LoadPRBuildLifecycle plus LoadPRBuildMetadata. A PR with no state yields
// the zero lifecycle (Found false). It never takes the exclusive lock.
func (s *PRObjectService) GetBuildLifecycle(ctx restate.ObjectSharedContext, _ BuildLifecycleRequest) (BuildLifecycle, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return BuildLifecycle{}, err
	}
	if st == nil {
		return BuildLifecycle{}, nil
	}
	return st.BuildLifecycleView(), nil
}

// HandleMainAdvanced clears updated-for-main after RepoObject recorded a new
// main push SHA, mirroring StateTracker.ClearUpdatedPRsForMainCommit. It is
// the one-way target of RepoObject main-push fan-out.
func (s *PRObjectService) HandleMainAdvanced(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.ClearUpdatedForMain()
	})
}

// RepoObjectService is the RepoObject Virtual Object. Each handler builds
// RepoHandlers over this invocation's Restate K/V and Restate one-way sends,
// so the logic in handlers_repo.go is the single implementation. RepoObject
// touches only its own repo-keyed state and reaches PRObject only through
// one-way sends. Malformed input fails terminally instead of retrying.
type RepoObjectService struct {
	stateKey   string
	github     GitHubEffector
	kube       KubeEffector
	retryDelay time.Duration
}

// NewRepoObjectService builds the single RepoObject service.
func NewRepoObjectService() *RepoObjectService {
	return &RepoObjectService{stateKey: repoObjectStateKey, retryDelay: DefaultRepoEffectRetryDelay}
}

// ServiceName returns the single RepoObject service name for every agent.
// Nil-safe so the registrar never panics on a zero service.
func (*RepoObjectService) ServiceName() string {
	return RepoObjectServiceName
}

// handlers wires RepoHandlers to this object's K/V, one-way sends and
// delayed self-sends. Every argument is a non-nil value, so
// NewRepoHandlers cannot fail here; a failure is a wiring bug and panics.
func (s *RepoObjectService) handlers(ctx restate.ObjectContext) *RepoHandlers {
	h, err := NewRepoHandlers(restateRepoStore{ctx: ctx, key: s.stateKey}, restatePRSender{ctx: ctx}, restateSelfScheduler{ctx: ctx})
	if err != nil {
		panic(err)
	}
	return h.WithGitHub(journaled(ctx, s.github)).WithKube(journaledKubeFor(ctx, s.kube))
}

// terminalOnInvalid converts malformed-input errors to terminal errors so
// Restate does not retry what no retry can fix.
func terminalOnInvalid(err error) error {
	if errors.Is(err, ErrInvalidRepoEvent) {
		return restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	return err
}

// HandleMainPush records one main push SHA and fans a main-advanced send out
// to every open PR. Duplicate pushes persist nothing.
func (s *RepoObjectService) HandleMainPush(ctx restate.ObjectContext, ev RepoEvent) error {
	return terminalOnInvalid(s.handlers(ctx).HandleMainPush(ctx, ev))
}

// HandleFanout re-delivers the latest main push to every open PR.
func (s *RepoObjectService) HandleFanout(ctx restate.ObjectContext, ev RepoEvent) error {
	return terminalOnInvalid(s.handlers(ctx).HandleFanout(ctx, ev))
}

// HandleKustomizationStatus records one dispatched-SHA tombstone scoped to
// its gate generation and forwards the status to every deploying PR.
func (s *RepoObjectService) HandleKustomizationStatus(ctx restate.ObjectContext, ev RepoEvent) error {
	return terminalOnInvalid(s.handlers(ctx).HandleKustomizationStatus(ctx, ev))
}

// HandlePRIndex applies one PRObject index update.
func (s *RepoObjectService) HandlePRIndex(ctx restate.ObjectContext, ev PRIndexEvent) error {
	return terminalOnInvalid(s.handlers(ctx).HandlePRIndex(ctx, ev))
}

// HandleCheckRunBySHA routes one check_run event by head SHA.
func (s *RepoObjectService) HandleCheckRunBySHA(ctx restate.ObjectContext, ev SHAEvent) error {
	return s.routeBySHA(ctx, SHAKindCheckRun, ev)
}

// HandleWorkflowRunBySHA routes one workflow_run event by head SHA.
func (s *RepoObjectService) HandleWorkflowRunBySHA(ctx restate.ObjectContext, ev SHAEvent) error {
	return s.routeBySHA(ctx, SHAKindWorkflowRun, ev)
}

// HandlePushBySHA routes one push event by commit SHA (for example a
// squash-merge push that carries no PR number).
func (s *RepoObjectService) HandlePushBySHA(ctx restate.ObjectContext, ev SHAEvent) error {
	return s.routeBySHA(ctx, SHAKindPush, ev)
}

// HandleImageReadyBySHA routes one image-ready event by commit SHA.
func (s *RepoObjectService) HandleImageReadyBySHA(ctx restate.ObjectContext, ev SHAEvent) error {
	return s.routeBySHA(ctx, SHAKindImageReady, ev)
}

func (s *RepoObjectService) routeBySHA(ctx restate.ObjectContext, kind string, ev SHAEvent) error {
	out, err := s.handlers(ctx).RouteBySHA(ctx, kind, ev)
	if err != nil {
		return terminalOnInvalid(err)
	}
	if out.Held {
		slog.InfoContext(ctx, "[prstate] RepoObject held SHA event until indexed", "repo", NormalizeRepo(ev.Repo), "sha", ev.SHA, "kind", kind, "handler", ev.Handler, "held_id", out.HeldID)
	}
	for _, dropped := range out.Evicted {
		slog.WarnContext(ctx, "[prstate] RepoObject evicted held SHA event at capacity", "repo", NormalizeRepo(ev.Repo), "sha", dropped.Event.SHA, "kind", dropped.Kind, "handler", dropped.Event.Handler, "held_id", dropped.ID)
	}
	return nil
}

// HandleHeldReplay is the delayed self-send that replays held events once
// their SHA is indexed.
func (s *RepoObjectService) HandleHeldReplay(ctx restate.ObjectContext, req HeldReplayRequest) error {
	return terminalOnInvalid(s.handlers(ctx).ReplayHeld(ctx, restate.Key(ctx), req.SHA))
}

// HandleHeldExpiry is the delayed self-send that drops one held event that
// never resolved.
func (s *RepoObjectService) HandleHeldExpiry(ctx restate.ObjectContext, req HeldExpiryRequest) error {
	repo := restate.Key(ctx)
	dropped, err := s.handlers(ctx).ExpireHeld(ctx, repo, req.ID)
	if err != nil {
		return terminalOnInvalid(err)
	}
	if dropped {
		slog.WarnContext(ctx, "[prstate] RepoObject expired unresolved held SHA event", "repo", repo, "held_id", req.ID)
	}
	return nil
}

// ResolveSHARequest is the single-input SHA lookup request. Restate handlers
// accept at most one input parameter, so repo and SHA ride one struct.
type ResolveSHARequest struct {
	Repo string `json:"repo"`
	SHA  string `json:"sha"`
}

// ResolveSHAResponse is the SHA lookup result.
type ResolveSHAResponse struct {
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	Found    bool   `json:"found"`
}

// ResolvePRBySHA resolves a commit SHA through the durable RepoObject index
// (newest claim wins). It is a shared, read-only handler: it never takes the
// exclusive lock and never writes.
func (s *RepoObjectService) ResolvePRBySHA(ctx restate.ObjectSharedContext, req ResolveSHARequest) (ResolveSHAResponse, error) {
	st, err := restate.Get[*RepoState](ctx, s.stateKey)
	if err != nil {
		return ResolveSHAResponse{}, err
	}
	entry, ok := st.ResolveSHA(req.SHA)
	if !ok {
		return ResolveSHAResponse{}, nil
	}
	return ResolveSHAResponse{PRNumber: entry.PRNumber, Branch: entry.Branch, Found: true}, nil
}

// restateRepoStore binds RepoStateStore to this invocation's Restate K/V.
// The object key already scopes the slot, so the repo argument is unused.
type restateRepoStore struct {
	ctx restate.ObjectContext
	key string
}

func (s restateRepoStore) LoadRepo(string) (*RepoState, error) {
	st, err := restate.Get[*RepoState](s.ctx, s.key)
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (s restateRepoStore) SaveRepo(st *RepoState) {
	restate.Set(s.ctx, s.key, st)
}

// restatePRSender binds PRSender to a journaled one-way ObjectSend.
type restatePRSender struct {
	ctx restate.ObjectContext
}

func (s restatePRSender) SendPR(_ context.Context, handler string, ev PREvent) error {
	restate.ObjectSend(s.ctx, PRObjectServiceName, PRObjectKey(ev.Repo, ev.PRNumber), handler).Send(ev)
	return nil
}

// restateSelfScheduler binds RepoSelfScheduler to delayed one-way
// ObjectSends back to this RepoObject.
type restateSelfScheduler struct {
	ctx restate.ObjectContext
}

func (s restateSelfScheduler) ScheduleHeldReplay(_ context.Context, repo, sha string, delay time.Duration) error {
	restate.ObjectSend(s.ctx, RepoObjectServiceName, RepoObjectKey(repo), RepoHandlerHeldReplay).Send(HeldReplayRequest{SHA: sha}, restate.WithDelay(delay))
	return nil
}

func (s restateSelfScheduler) ScheduleHeldExpiry(_ context.Context, repo string, id uint64, delay time.Duration) error {
	restate.ObjectSend(s.ctx, RepoObjectServiceName, RepoObjectKey(repo), RepoHandlerHeldExpiry).Send(HeldExpiryRequest{ID: id}, restate.WithDelay(delay))
	return nil
}

// RegisterPRStateServices binds the dark PRObject and RepoObject services
// beside Build and BuildRouter on srv. Nil services get dark defaults; a nil
// server skips binding and returns the effective services for hermetic use.
// It panics on duplicate service names exactly like every other Bind, so a
// name collision with Build or BuildRouter fails fast at startup.
func RegisterPRStateServices(srv *server.Restate, pr *PRObjectService, repo *RepoObjectService) (*PRObjectService, *RepoObjectService) {
	if pr == nil {
		pr = NewPRObjectService()
	}
	if repo == nil {
		repo = NewRepoObjectService()
	}
	if srv != nil {
		srv.Bind(restate.Reflect(pr)).Bind(restate.Reflect(repo))
	}
	return pr, repo
}
