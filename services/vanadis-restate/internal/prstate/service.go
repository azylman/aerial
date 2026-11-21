package prstate

// Restate Virtual Object assembly for synapse PR lifecycle state.
//
// PRObject (key repo#prNumber) is the single ordered authority for per-PR
// state; RepoObject (key repo) holds only repo-level state: the bounded
// main-push index, dispatched-SHA tombstones, the SHA->PR routing index with
// open and deploying sets, and SHA-only events held until indexed. Postgres
// pr_registry stays a read-only projection synced by a journaled send after
// the state commit and is never consulted to decide a transition.
//
// The services bind beside BuildRouter so the Restate deployment discovers
// PRObject and RepoObject, with all synapse consumers still on the legacy
// pipeline until the cutover chunks. Each PRObject handler loads Restate K/V
// state, applies the pure disposition guard (stale-drop, terminal-absorbing),
// and persists on apply. No external effects fire here (Discord alerts,
// GitHub update-branch/rerun, image promotion, Flux forward), and unmanaged
// collaborator PRs never signal the BuildRouter: effect and routing wiring
// lands in later chunks through the journaled, generation-scoped EffectKey
// boundaries frozen in contracts.go.
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

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
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
	stateKey string
}

// NewPRObjectService builds the single dark PRObject service.
func NewPRObjectService() *PRObjectService {
	return &PRObjectService{stateKey: prObjectStateKey}
}

// ServiceName returns the single PRObject service name for every agent.
// Nil-safe so the registrar never panics on a zero service.
func (*PRObjectService) ServiceName() string {
	return PRObjectServiceName
}

func (s *PRObjectService) loadOrCreatePRState(ctx restate.ObjectSharedContext, ev PREvent) (*PRState, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return nil, err
	}
	if st == nil {
		st = NewPRState(ev.Repo, ev.PRNumber)
	}
	// Lifecycle events carry the agent only as a hint: it fills an empty
	// owner slot but never overwrites a stored owner. Register, Hydrate, and
	// PR-body metadata on opened claim ownership via ClaimOwner instead.
	st.ApplyAgentHint(ev.Agent)
	return st, nil
}

// eventPhase selects the disposition guard for one handler: PR-lifecycle
// events are absorbed by any same-epoch terminal state, while deploy-phase
// events still apply to merged PRs (deploys happen after merge).
type eventPhase int

const (
	phaseLifecycle eventPhase = iota
	phaseDeploy
)

func (s *PRObjectService) mutatePREvent(ctx restate.ObjectContext, ev PREvent, mutate func(*PRState)) error {
	return s.mutatePhase(ctx, ev, phaseLifecycle, mutate)
}

func (s *PRObjectService) mutateDeployEvent(ctx restate.ObjectContext, ev PREvent, mutate func(*PRState)) error {
	return s.mutatePhase(ctx, ev, phaseDeploy, mutate)
}

func (s *PRObjectService) mutatePhase(ctx restate.ObjectContext, ev PREvent, phase eventPhase, mutate func(*PRState)) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	if dispositionFor(st, ev, phase) != DispositionApply {
		return nil
	}
	before := snapshotIndexView(st)
	applyEventRow(st, ev)
	if mutate != nil {
		mutate(st)
	}
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, ev, lifecycle)
	}
	return nil
}

func dispositionFor(st *PRState, ev PREvent, phase eventPhase) EventDisposition {
	if phase == phaseDeploy {
		return st.ApplyDeployEvent(ev)
	}
	return st.ApplyEvent(ev)
}

// applyEventRow applies the registry-row fields every applied event may
// carry: the first-seen head SHA (never moved once merged, so a deploy
// event carrying the merge commit cannot rewrite the PR head; later head
// changes arrive through HandleSynchronized or Register), branch/title
// identity, and a missing target ID.
func applyEventRow(st *PRState, ev PREvent) {
	if head := strings.TrimSpace(ev.HeadSHA); head != "" && !IsZeroSHA(head) && st.Terminal != TerminalMerged {
		st.HeadSHA = head
	}
	st.ApplyIdentity(ev.Branch, ev.Title)
	if tid := strings.TrimSpace(ev.TargetID); st.TargetID == "" && IsValidDiscordSnowflake(tid) {
		st.TargetID = tid
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

// HandleCheckFailed applies one check-failure event dark.
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

// HandleCollaboratorAlert applies one collaborator alert dark.
func (s *PRObjectService) HandleCollaboratorAlert(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.MarkCollaboratorAlerted(ev.HeadSHA)
	})
}

// HandleConflictDetected resolves the conflict-republish verdict without
// recording anything, so a publish failure leaves the next invocation
// returning ShouldPublish again. The verdict is the sole authority for the
// synapse republish; only HandleConflictPublished completes the dispatch.
func (s *PRObjectService) HandleConflictDetected(ctx restate.ObjectSharedContext, ev PREvent) (ConflictDispatchDecision, error) {
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return ConflictDispatchDecision{}, err
	}
	return ResolveConflictDispatchDecision(st, ev)
}

// HandleConflictPublished records one confirmed conflict publish for the
// event head SHA. The ack is idempotent and the commit point for the
// dispatch; an ack failure surfaces so redelivery re-resolves and
// republishes instead of losing the signal.
func (s *PRObjectService) HandleConflictPublished(ctx restate.ObjectContext, ev PREvent) (ConflictPublishedOutcome, error) {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return ConflictPublishedOutcome{}, nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return ConflictPublishedOutcome{}, err
	}
	out, recErr := RecordConflictPublished(st, ev)
	if recErr != nil {
		return out, recErr
	}
	if out.Disposition == DispositionApply {
		st.ApplyRegistryConflict()
		restate.Set(ctx, s.stateKey, st)
	}
	return out, nil
}

// HandleCIRecovery resolves whether the PR is CI-failed for the event head
// SHA from the Restate-owned failure slots. The resolve is a pure read and
// records nothing, so it never disturbs the failure state a later success
// event clears.
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
		st.ClearConflictPublished()
		st.ClearCollaboratorConflict(ev.HeadSHA)
		st.ApplyRegistryRecovered(RegistryStatusConflict)
	})
}

// HandleMerged applies one merge event dark.
func (s *PRObjectService) HandleMerged(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.ClearFailedCISHA()
		st.MarkTerminal(TerminalMerged)
		st.ApplyRegistryMerged(ev.MergeSHA)
	})
}

// HandleClosed applies one close event dark.
func (s *PRObjectService) HandleClosed(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.ClearFailedCISHA()
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

// HandleReadinessRetry applies one readiness retry dark. Retry never mints
// a generation and never clears effect keys, per the frozen contract.
func (s *PRObjectService) HandleReadinessRetry(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutatePREvent(ctx, ev, func(st *PRState) {
		st.RetryPRReadiness()
	})
}

// HandleRolloutProgress applies one rollout-progress event. It is a
// deploy-phase event, so merged PRs apply it: ready jobs union into
// deployed_jobs, completed Kustomizations into deployed_kustomizations
// (flipping the PR to deployed once every request completed), and a carried
// job state records into job_states. RepoObject kustomization-status
// forwards carry no ready lists and only refresh the row.
func (s *PRObjectService) HandleRolloutProgress(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateDeployEvent(ctx, ev, func(st *PRState) {
		st.MarkJobsDeployed(ev.DeployedJobs)
		st.MarkKustomizationsDeployed(ev.DeployedKustomizations)
		st.RecordJobState(ev.JobName, ev.JobState)
	})
}

// HandleSynchronized advances the PR head on a synchronize under the
// BeforeSHA compare-and-set guard and applies the UpdatePRSync row
// transition. The new head is indexed for RepoObject SHA routing.
func (s *PRObjectService) HandleSynchronized(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	if st.SynchronizeDisposition(ev) != DispositionApply {
		return nil
	}
	before := snapshotIndexView(st)
	prevHead := st.HeadSHA
	st.HeadSHA = strings.TrimSpace(ev.HeadSHA)
	st.ApplyRegistrySync(prevHead, st.HeadSHA)
	st.ApplyIdentity(ev.Branch, ev.Title)
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, ev, lifecycle)
	}
	return nil
}

// HandleDeployFailed applies one deploy failure (AtomicTransitionDeployFailed)
// or, when Acknowledged, the confirmed deploy_failed publish
// (AcknowledgeDeployFailedPublished). Merged PRs apply it.
func (s *PRObjectService) HandleDeployFailed(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateDeployEvent(ctx, ev, func(st *PRState) {
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

// HandleDeployDispatched resolves the deploy_success publish verdict without
// recording anything, so a publish failure leaves the next invocation
// returning ShouldPublish again. The verdict is the sole authority for the
// synapse publish; only HandleDeploySuccessPublished completes the dispatch.
func (s *PRObjectService) HandleDeployDispatched(ctx restate.ObjectSharedContext, ev PREvent) (DeployDispatchDecision, error) {
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return DeployDispatchDecision{}, err
	}
	return ResolveDeployDispatchDecision(st, ev)
}

// HandleDeploySuccessPublished records one confirmed deploy_success publish:
// the dispatch key plus both dedup slots. The ack is idempotent and the
// commit point for the dispatch; an ack failure surfaces so redelivery
// re-resolves and republishes instead of losing the signal.
func (s *PRObjectService) HandleDeploySuccessPublished(ctx restate.ObjectContext, ev PREvent) (DeployPublishedOutcome, error) {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return DeployPublishedOutcome{}, nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return DeployPublishedOutcome{}, err
	}
	out, recErr := RecordDeployPublished(st, ev)
	if recErr != nil {
		return out, recErr
	}
	if out.Disposition == DispositionApply {
		st.AckDeploySuccess()
		restate.Set(ctx, s.stateKey, st)
		if !out.Deduped {
			sendPRIndex(ctx, st, ev, IndexDeployed)
		}
	}
	return out, nil
}

// HandleKustomizationRequested starts a new gate generation for one
// kustomization request. It is a deploy-phase event, so merged PRs apply
// it: carried jobs move the PR to deploying (TransitionDeploying) and the
// carried requests replace the recorded set (RecordKustomizationRequests),
// in the registry's order. The generation bump sends RepoObject a deploying
// index update so kustomization status fans out to this PR.
func (s *PRObjectService) HandleKustomizationRequested(ctx restate.ObjectContext, ev PREvent) error {
	return s.mutateDeployEvent(ctx, ev, func(st *PRState) {
		st.TransitionDeploying(ev.Jobs)
		st.RecordKustomizationRequestSet(ev.KustomizationRequests)
		st.RecordKustomizationRequests()
	})
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
	stateKey string
}

// NewRepoObjectService builds the single RepoObject service.
func NewRepoObjectService() *RepoObjectService {
	return &RepoObjectService{stateKey: repoObjectStateKey}
}

// ServiceName returns the single RepoObject service name for every agent.
// Nil-safe so the registrar never panics on a zero service.
func (*RepoObjectService) ServiceName() string {
	return RepoObjectServiceName
}

func (s *RepoObjectService) handlers(ctx restate.ObjectContext) *RepoHandlers {
	return NewRepoHandlers(restateRepoStore{ctx: ctx, key: s.stateKey}, nil, restatePRSender{ctx: ctx}, restateSelfScheduler{ctx: ctx})
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
