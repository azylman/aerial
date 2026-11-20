package prstate

// Dark Restate Virtual Object assembly for synapse PR lifecycle state.
//
// PRObject (key repo#prNumber) is the single ordered authority for per-PR
// state; RepoObject (key repo) holds only the bounded main-push index,
// dispatched-SHA tombstones, and fan-out sends. Postgres pr_registry stays a
// read-only projection synced by a journaled send after the state commit and
// is never consulted to decide a transition.
//
// Chunk 07 is registration ONLY: the services below bind the frozen handler
// surfaces beside BuildRouter so the Restate deployment discovers PRObject
// and RepoObject, with all synapse consumers still on legacy trackers
// (fully dark, zero caller cutover). Each handler loads Restate K/V state,
// applies the pure disposition guard (stale-drop, terminal-absorbing), and
// persists on apply. No external effects fire here (Discord alerts, GitHub
// update-branch/rerun, image promotion, Flux forward), no cross-object sends
// fire, and unmanaged collaborator PRs never signal the BuildRouter: effect
// and routing wiring lands in later chunks through the journaled,
// generation-scoped EffectKey boundaries frozen in contracts.go.
//
// Handler signatures use restate.ObjectContext (exclusive mode), so these
// services intentionally do not satisfy the context.Context-based PRObject
// and RepoObject interfaces: those freeze the logic surface, this file binds
// the Restate surface. ResolvePRBySHA takes a single request struct because
// Restate handlers accept at most one input parameter; the durable SHA index
// lookup wires to the projection in a later chunk and reports not-found dark.

import (
	"strings"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
)

// Frozen Restate service names. They match the synapse ingress invoker
// defaults (DefaultPRService/DefaultRepoService) exactly; any rename is a
// cross-module breaking change caught by TestRegisterPRStateServices.
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

// NewPRObjectService builds the dark PRObject service.
func NewPRObjectService() *PRObjectService {
	return &PRObjectService{stateKey: prObjectStateKey}
}

// ServiceName returns the frozen PRObject service name. Nil-safe so the
// registrar never panics on a zero service.
func (s *PRObjectService) ServiceName() string {
	if s == nil {
		return PRObjectServiceName
	}
	return PRObjectServiceName
}

// applyPREvent loads the PR state, drops stale and same-epoch terminal
// events, and persists applied state. Effect and routing sends land in a
// later chunk; this assembly step only proves the K/V round-trip.
func (s *PRObjectService) applyPREvent(ctx restate.ObjectContext, ev PREvent) error {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return err
	}
	if st == nil {
		st = NewPRState(ev.Repo, ev.PRNumber)
	}
	if st.ApplyEvent(ev) != DispositionApply {
		return nil
	}
	restate.Set(ctx, s.stateKey, st)
	return nil
}

// HandleCheckFailed applies one check-failure event dark.
func (s *PRObjectService) HandleCheckFailed(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleCheckPassed applies one check-pass event dark.
func (s *PRObjectService) HandleCheckPassed(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleCheckRerunRequested applies one check-rerun request dark.
func (s *PRObjectService) HandleCheckRerunRequested(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleCollaboratorAlert applies one collaborator alert dark.
func (s *PRObjectService) HandleCollaboratorAlert(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleConflictDetected applies one conflict-detected event dark.
func (s *PRObjectService) HandleConflictDetected(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleConflictResolved applies one conflict-resolved event dark.
func (s *PRObjectService) HandleConflictResolved(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleMerged applies one merge event dark.
func (s *PRObjectService) HandleMerged(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleClosed applies one close event dark.
func (s *PRObjectService) HandleClosed(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleReopened applies one reopen event dark.
func (s *PRObjectService) HandleReopened(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleReadinessRetry applies one readiness retry dark. Retry never mints
// a generation and never clears effect keys, per the frozen contract.
func (s *PRObjectService) HandleReadinessRetry(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleRolloutProgress applies one rollout-progress event dark.
func (s *PRObjectService) HandleRolloutProgress(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// HandleDeployDispatched resolves the deploy_success publish verdict without
// recording anything, so a publish failure leaves the next invocation
// returning ShouldPublish again. The verdict is the sole authority for the
// synapse publish; only HandleDeploySuccessPublished completes the dispatch.
func (s *PRObjectService) HandleDeployDispatched(ctx restate.ObjectContext, ev PREvent) (DeployDispatchDecision, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return DeployDispatchDecision{}, err
	}
	if st == nil {
		st = NewPRState(ev.Repo, ev.PRNumber)
	}
	return ResolveDeployDispatchDecision(st, ev)
}

// HandleDeploySuccessPublished records one confirmed deploy_success publish:
// the dispatch key plus both dedup slots. The ack is idempotent and the
// commit point for the dispatch; an ack failure surfaces so redelivery
// re-resolves and republishes instead of losing the signal.
func (s *PRObjectService) HandleDeploySuccessPublished(ctx restate.ObjectContext, ev PREvent) (DeployPublishedOutcome, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return DeployPublishedOutcome{}, err
	}
	if st == nil {
		st = NewPRState(ev.Repo, ev.PRNumber)
	}
	out, recErr := RecordDeployPublished(st, ev)
	if recErr != nil {
		return out, recErr
	}
	if out.Disposition == DispositionApply {
		restate.Set(ctx, s.stateKey, st)
	}
	return out, nil
}

// HandleKustomizationRequested applies one kustomization request dark.
func (s *PRObjectService) HandleKustomizationRequested(ctx restate.ObjectContext, ev PREvent) error {
	return s.applyPREvent(ctx, ev)
}

// RepoObjectService is the dark RepoObject Virtual Object. Handlers persist
// main-push and tombstone state to Restate K/V and fire nothing.
type RepoObjectService struct {
	stateKey string
}

// NewRepoObjectService builds the dark RepoObject service.
func NewRepoObjectService() *RepoObjectService {
	return &RepoObjectService{stateKey: repoObjectStateKey}
}

// ServiceName returns the frozen RepoObject service name. Nil-safe so the
// registrar never panics on a zero service.
func (s *RepoObjectService) ServiceName() string {
	if s == nil {
		return RepoObjectServiceName
	}
	return RepoObjectServiceName
}

// HandleMainPush records one main push SHA into the bounded index dark.
// Pushes without a usable repo or SHA, and duplicate pushes, persist
// nothing: invalid input never creates a state slot.
func (s *RepoObjectService) HandleMainPush(ctx restate.ObjectContext, ev RepoEvent) error {
	if NormalizeRepo(ev.Repo) == "" {
		return nil
	}
	sha := strings.TrimSpace(ev.MainSHA)
	if sha == "" || IsZeroSHA(sha) {
		return nil
	}
	st, err := restate.Get[*RepoState](ctx, s.stateKey)
	if err != nil {
		return err
	}
	fresh := st == nil
	if fresh {
		st = NewRepoState(ev.Repo)
	}
	if !st.RecordMainPushSHA(ev.MainSHA) && !fresh {
		return nil
	}
	restate.Set(ctx, s.stateKey, st)
	return nil
}

// HandleFanout is the dark main-push fan-out entrypoint. Per-PR fan-out
// sends land in a later chunk; this assembly step registers the surface and
// touches no state.
func (s *RepoObjectService) HandleFanout(ctx restate.ObjectContext, ev RepoEvent) error { //nolint:revive // dark surface stub: receiver required by restate.Reflect; fanout sends wire in a later chunk.
	return nil
}

// HandleKustomizationStatus records one dispatched-SHA tombstone scoped to
// its gate generation dark, evicting tombstones below that generation.
// Statuses without a usable repo or SHA persist nothing: invalid input never
// creates a state slot.
func (s *RepoObjectService) HandleKustomizationStatus(ctx restate.ObjectContext, ev RepoEvent) error {
	repo := NormalizeRepo(ev.Repo)
	if repo == "" {
		return nil
	}
	st, err := restate.Get[*RepoState](ctx, s.stateKey)
	if err != nil {
		return err
	}
	if st == nil {
		st = NewRepoState(repo)
	}
	sha := strings.TrimSpace(ev.HeadSHA)
	if sha == "" || IsZeroSHA(sha) {
		return nil
	}
	st.RecordTombstone(sha, ev.Generation)
	st.EvictTombstonesBelowGeneration(ev.Generation)
	restate.Set(ctx, s.stateKey, st)
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

// ResolvePRBySHA resolves a commit SHA dark. The durable SHA index wires to
// the read-only projection in a later chunk, so every lookup reports
// not-found and touches no Restate K/V.
func (s *RepoObjectService) ResolvePRBySHA(ctx restate.ObjectContext, req ResolveSHARequest) (ResolveSHAResponse, error) { //nolint:revive // dark surface stub: receiver required by restate.Reflect; durable SHA index wires in a later chunk.
	return ResolveSHAResponse{}, nil
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
