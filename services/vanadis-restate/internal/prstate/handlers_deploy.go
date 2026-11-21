package prstate

// Rollout/deploy-dedup handlers. PRObject is the single ordered authority
// for per-PR deploy state; Postgres pr_registry stays a read-only
// projection fed by a journaled send after the state commit.
//
// Both handlers preserve the BOTH-key deploy dedup from
// services/synapse/internal/flux/dedup.go: a deploy counts as fully
// dispatched only when a PR-keyed or job-plus-SHA slot hit coincides with
// every applicable recorded effect key. Every external effect (Flux
// forward, image promotion) runs once per generation-scoped EffectKey;
// replay returns the recorded result and never re-fires. Terminal states
// absorb same-epoch events; a newer reopen epoch applies. Readiness retry
// never mints a generation and never clears effect keys, via the frozen
// PRState.RetryPRReadiness no-op covered in state_test.go.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// FluxForwarder is the journaled Flux-forward boundary. Implementations
// must forward exactly once per EffectKey; handlers guarantee they are
// called at most once per key per process by recording the key before
// returning success.
type FluxForwarder interface {
	ForwardFlux(ctx context.Context, key EffectKey, ev PREvent) error
}

// ImagePromoter is the journaled image-promotion boundary. Promotion is
// naturally idempotent on the merge SHA: the effect key carries the
// merge SHA, so a retry for the same merge replays the recorded result
// instead of promoting twice. Where the GitHub API accepts an external
// idempotency key, callers pass key.String(); otherwise the handler
// accepts at-most-once-per-key firing with at-least-once delivery on
// transport failure before the key is recorded.
type ImagePromoter interface {
	PromoteImages(ctx context.Context, key EffectKey, ev PREvent) error
}

// RolloutOutcome is the result of one rollout-progress event.
type RolloutOutcome struct {
	Disposition EventDisposition
	EffectKey   string
	Forwarded   bool
	Deduped     bool
}

// DeployOutcome is the result of one deploy event.
type DeployOutcome struct {
	Disposition      EventDisposition
	FluxEffectKey    string
	PromoteEffectKey string
	Dispatched       bool
	Deduped          bool
}

// DeployDispatchDecision is the synapse deploy_success publish verdict. The
// PRObject computes it from the deploy_dispatch effect key plus both
// WasDispatched slots; it never records anything, so a publish failure
// leaves the next invocation returning ShouldPublish again (at-least-once)
// and only the publish-recorded ack completes the dispatch.
type DeployDispatchDecision struct {
	Disposition   EventDisposition `json:"disposition"`
	ShouldPublish bool             `json:"should_publish"`
	Deduped       bool             `json:"deduped"`
	EffectKey     string           `json:"effect_key"`
}

// DeployPublishedOutcome is the result of one publish-recorded ack. The ack
// marks the dispatch key and both dedup slots, so it is idempotent: a
// redelivered ack replays as already recorded.
type DeployPublishedOutcome struct {
	Disposition EventDisposition `json:"disposition"`
	EffectKey   string           `json:"effect_key"`
	Recorded    bool             `json:"recorded"`
	Deduped     bool             `json:"deduped"`
}

// ResolveDeployDispatchDecision computes the deploy_success publish verdict
// without recording anything. Stale SHAs drop and same-epoch terminal states
// absorb with no publish. A dispatch is fully deduped only when the
// deploy_dispatch effect key is recorded and a WasDispatched slot hits;
// every other applied dispatch must publish, so an unrecorded publish is
// always retried instead of lost.
func ResolveDeployDispatchDecision(s *PRState, ev PREvent) (DeployDispatchDecision, error) {
	if s == nil {
		return DeployDispatchDecision{Disposition: DispositionDropStale}, errNilPRState
	}
	if disp := s.ApplyDeployEvent(ev); disp != DispositionApply {
		return DeployDispatchDecision{Disposition: disp}, nil
	}
	key := s.EffectKeyFor(EffectDeployDispatch, ev.HeadSHA, ev.JobName, "")
	out := DeployDispatchDecision{Disposition: DispositionApply, EffectKey: key.String()}
	if s.WasEffectDispatched(key) && WasDispatched(s, ev.Repo, ev.JobName, ev.HeadSHA, ev.PRNumber) {
		out.Deduped = true
		return out, nil
	}
	out.ShouldPublish = true
	return out, nil
}

// RecordDeployPublished marks one confirmed deploy_success publish: the
// deploy_dispatch effect key plus the PR-keyed and job-plus-SHA dedup slots,
// and the observed head SHA for later staleness checks. Stale SHAs drop and
// same-epoch terminal states absorb with nothing recorded.
func RecordDeployPublished(s *PRState, ev PREvent) (DeployPublishedOutcome, error) {
	if s == nil {
		return DeployPublishedOutcome{Disposition: DispositionDropStale}, errNilPRState
	}
	if disp := s.ApplyDeployEvent(ev); disp != DispositionApply {
		return DeployPublishedOutcome{Disposition: disp}, nil
	}
	key := s.EffectKeyFor(EffectDeployDispatch, ev.HeadSHA, ev.JobName, "")
	out := DeployPublishedOutcome{Disposition: DispositionApply, EffectKey: key.String()}
	out.Deduped = s.WasEffectDispatched(key)
	s.MarkEffectDispatched(key)
	s.RecordDeployedPR()
	if cleanJob, cleanHead := strings.TrimSpace(ev.JobName), strings.TrimSpace(ev.HeadSHA); cleanJob != "" && cleanHead != "" && !IsZeroSHA(cleanHead) {
		s.RecordDeployedSHAJob(cleanJob, cleanHead)
	}
	syncHeadSHA(s, ev.HeadSHA)
	out.Recorded = true
	return out, nil
}

// errNilPRState reports a nil state, which can never apply an event.
var errNilPRState = errors.New("prstate: nil PRState")

// WasDispatched reports whether a deployment already fired, preserving
// BOTH WasDispatched keys from DeployDedupTracker: the PR-keyed slot
// (pr:repo:prNumber) OR the job-plus-SHA slot (sha:repo:job:sha).
// A hit alone does not suppress re-dispatch: HandleDeployEvent treats the
// deploy as fully deduped only when every applicable effect key is also
// recorded, so a promotion that arrived after an empty-merge dispatch is
// retried instead of lost. A bare PR key that never expires is forbidden,
// so the SHA-scoped slot bounds dedup per commit.
func WasDispatched(s *PRState, repo, jobName, sha string, prNumber int) bool {
	if s == nil {
		return false
	}
	if prNumber > 0 && prNumber == s.PRNumber && NormalizeRepo(repo) == s.Repo && s.WasDeployedPR() {
		return true
	}
	cleanJob := strings.TrimSpace(jobName)
	cleanSHA := strings.TrimSpace(sha)
	if cleanJob == "" || cleanSHA == "" || IsZeroSHA(cleanSHA) {
		return false
	}
	if s.DeploySHAJob == nil {
		return false
	}
	return s.DeploySHAJob[DeploySHAJobKey(repo, cleanJob, cleanSHA)]
}

// fireFluxEffect fires the Flux-forward effect once per key: a recorded
// key replays without calling the runner, otherwise the runner fires and
// the key is recorded on success.
func fireFluxEffect(ctx context.Context, s *PRState, ev PREvent, key EffectKey, fwd FluxForwarder) error {
	if s.WasEffectDispatched(key) {
		return nil
	}
	if fwd == nil {
		return errNilPRState
	}
	if err := fwd.ForwardFlux(ctx, key, ev); err != nil {
		return err
	}
	s.MarkEffectDispatched(key)
	return nil
}

// firePromoteEffect fires image promotion once per merge-SHA key. An
// empty key (no merge SHA) is a no-op so promotion is skipped while the
// Flux-forward and deploy-dedup effects still apply.
func firePromoteEffect(ctx context.Context, s *PRState, ev PREvent, key EffectKey, prom ImagePromoter) error {
	if key.Kind == "" || s.WasEffectDispatched(key) {
		return nil
	}
	if prom == nil {
		return errNilPRState
	}
	if err := prom.PromoteImages(ctx, key, ev); err != nil {
		return err
	}
	s.MarkEffectDispatched(key)
	return nil
}

// syncHeadSHA records the observed head SHA after a successful apply so
// later events judge staleness against it. A merged PR keeps its head:
// deploy events may carry the merge commit, which must not rewrite it.
func syncHeadSHA(s *PRState, headSHA string) {
	if s.Terminal == TerminalMerged {
		return
	}
	if clean := strings.TrimSpace(headSHA); clean != "" && !IsZeroSHA(clean) {
		s.HeadSHA = clean
	}
}

// HandleRolloutEvent applies one rollout-progress event: stale SHAs drop,
// same-epoch terminal states absorb, and the Flux-forward effect fires
// exactly once per generation-scoped key (repo+PR+headSHA+generation+
// flux_forward+jobName). Replay of a recorded key reports Forwarded
// without calling the runner again.
func HandleRolloutEvent(ctx context.Context, s *PRState, ev PREvent, fwd FluxForwarder) (RolloutOutcome, error) {
	if s == nil {
		return RolloutOutcome{Disposition: DispositionDropStale}, errNilPRState
	}
	if disp := s.ApplyDeployEvent(ev); disp != DispositionApply {
		return RolloutOutcome{Disposition: disp}, nil
	}
	key := s.EffectKeyFor(EffectFluxForward, ev.HeadSHA, ev.JobName, "")
	out := RolloutOutcome{Disposition: DispositionApply, EffectKey: key.String()}
	already := s.WasEffectDispatched(key)
	if err := fireFluxEffect(ctx, s, ev, key, fwd); err != nil {
		return out, err
	}
	syncHeadSHA(s, ev.HeadSHA)
	out.Forwarded = true
	out.Deduped = already
	return out, nil
}

// HandleDeployEvent applies one deploy event, preserving BOTH dedup keys.
// Stale SHAs drop and same-epoch terminal states absorb with no effects.
// A deploy is fully deduped (Dispatched+Deduped, no re-fire) only when a
// dedup slot hit coincides with every applicable effect key recorded;
// otherwise the unrecorded effects fire, so a merge SHA that arrived after
// an empty-merge dispatch still gets its promotion. The Flux-forward
// effect fires once per its generation-scoped key and image promotion
// fires once per merge SHA; each recorded key replays without re-firing,
// so a retry after a partial failure replays Flux and retries only the
// unrecorded promotion. Deploy dedup slots are recorded only once no
// promotion remains pending; any runner error propagates with the keys
// recorded so far.
func HandleDeployEvent(ctx context.Context, s *PRState, ev PREvent, fwd FluxForwarder, prom ImagePromoter) (DeployOutcome, error) {
	if s == nil {
		return DeployOutcome{Disposition: DispositionDropStale}, errNilPRState
	}
	if disp := s.ApplyDeployEvent(ev); disp != DispositionApply {
		return DeployOutcome{Disposition: disp}, nil
	}
	fluxKey := s.EffectKeyFor(EffectFluxForward, ev.HeadSHA, ev.JobName, "")
	mergeSHA := strings.TrimSpace(ev.MergeSHA)
	var promoteKey EffectKey
	if mergeSHA != "" && !IsZeroSHA(mergeSHA) {
		promoteKey = s.EffectKeyFor(EffectImagePromote, mergeSHA, "", "")
	}
	out := DeployOutcome{Disposition: DispositionApply, FluxEffectKey: fluxKey.String(), PromoteEffectKey: promoteKey.String()}
	promotionDone := promoteKey.Kind == "" || s.WasEffectDispatched(promoteKey)
	if WasDispatched(s, ev.Repo, ev.JobName, ev.HeadSHA, ev.PRNumber) && s.WasEffectDispatched(fluxKey) && promotionDone {
		out.Dispatched = true
		out.Deduped = true
		return out, nil
	}
	if err := fireFluxEffect(ctx, s, ev, fluxKey, fwd); err != nil {
		return out, err
	}
	if err := firePromoteEffect(ctx, s, ev, promoteKey, prom); err != nil {
		return out, err
	}
	if promoteKey.Kind == "" || s.WasEffectDispatched(promoteKey) {
		s.RecordDeployedPR()
		if cleanJob, cleanHead := strings.TrimSpace(ev.JobName), strings.TrimSpace(ev.HeadSHA); cleanJob != "" && cleanHead != "" && !IsZeroSHA(cleanHead) {
			s.RecordDeployedSHAJob(cleanJob, cleanHead)
		}
	}
	syncHeadSHA(s, ev.HeadSHA)
	out.Dispatched = true
	return out, nil
}

// DefaultDeployFailureDeadline bounds how long a restate-owned deploy
// attempt may keep failing before PRObject declares deploy_failed. It
// replaces synapse's NumDelivered>=5 final-attempt rule for restate-owned
// events, whose JetStream retries run on the longer restate backoff.
const DefaultDeployFailureDeadline = 15 * time.Minute

// PendingDeployFailure is one armed deploy-failure deadline: the attempt's
// SHAs and gate generation, the newest failure banner, and the rendered
// deploy_failed facts the deadline publishes.
type PendingDeployFailure struct {
	Seq        uint64
	HeadSHA    string
	MergeSHA   string
	Generation uint64
	Banner     string
	Facts      []pubsub.PRLifecycleEvent
}

// DeployFailureDeadlineRequest is the delayed HandleDeployFailureDeadline
// input naming the deadline it fires.
type DeployFailureDeadlineRequest struct {
	Seq uint64 `json:"seq"`
}

// ArmDeployFailure arms a deploy-failure deadline for ev and returns its
// sequence number. An already-armed deadline keeps its timer and only
// refreshes the banner and facts to the newest failure (armed false).
func (s *PRState) ArmDeployFailure(ev PREvent) (uint64, bool) {
	if s == nil {
		return 0, false
	}
	if p := s.PendingDeployFailure; p != nil {
		if b := strings.TrimSpace(ev.Banner); b != "" {
			p.Banner = b
		}
		if len(ev.Facts) > 0 {
			p.Facts = append([]pubsub.PRLifecycleEvent(nil), ev.Facts...)
		}
		return p.Seq, false
	}
	s.DeployFailureSeq++
	s.PendingDeployFailure = &PendingDeployFailure{
		Seq:        s.DeployFailureSeq,
		HeadSHA:    strings.TrimSpace(ev.HeadSHA),
		MergeSHA:   strings.TrimSpace(ev.MergeSHA),
		Generation: s.GateGeneration,
		Banner:     strings.TrimSpace(ev.Banner),
		Facts:      append([]pubsub.PRLifecycleEvent(nil), ev.Facts...),
	}
	return s.DeployFailureSeq, true
}

// DisarmDeployFailure cancels any armed deadline: deploy progress, a
// recorded deploy outcome, a head move, or a close supersedes the attempt.
func (s *PRState) DisarmDeployFailure() {
	if s != nil {
		s.PendingDeployFailure = nil
	}
}

// TakeDeployFailure disarms and returns the armed deadline when seq names it
// and its gate generation is still current. A stale seq leaves a newer
// deadline armed; a moved generation disarms it without firing.
func (s *PRState) TakeDeployFailure(seq uint64) (*PendingDeployFailure, bool) {
	if s == nil || s.PendingDeployFailure == nil || s.PendingDeployFailure.Seq != seq {
		return nil, false
	}
	p := s.PendingDeployFailure
	s.PendingDeployFailure = nil
	return p, p.Generation == s.GateGeneration
}

// HandleDeployAttemptFailed records one failed restate-owned deploy attempt
// (image resolve, promotion, or reconcile-request record) and arms a bounded
// deploy-failure deadline as a delayed self-send; the deadline, not a
// delivery count, decides deploy_failed. Legacy-owned attempts are ignored:
// synapse's final-attempt rule still owns them and journals the outcome
// through HandleDeployFailed.
func (s *PRObjectService) HandleDeployAttemptFailed(ctx restate.ObjectContext, ev PREvent) error {
	if ParseEffectsOwner(string(ev.EffectsOwner)) != EffectsOwnerRestate || NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	if st.ApplyDeployEvent(ev) != DispositionApply {
		return nil
	}
	seq, armed := st.ArmDeployFailure(ev)
	restate.Set(ctx, s.stateKey, st)
	if armed {
		restate.ObjectSend(ctx, PRObjectServiceName, PRObjectKey(st.Repo, st.PRNumber), PRHandlerDeployFailureDeadline).
			Send(DeployFailureDeadlineRequest{Seq: seq}, restate.WithDelay(s.deployFailureDeadline))
	}
	return nil
}

// HandleDeployFailureDeadline fires one armed deadline: when the attempt is
// still the current one, it applies deploy_failed with the newest banner,
// commits, publishes the captured deploy_failed facts, and acknowledges the
// publish. A disarmed, superseded, or stale deadline does nothing.
func (s *PRObjectService) HandleDeployFailureDeadline(ctx restate.ObjectContext, req DeployFailureDeadlineRequest) error {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil || st == nil {
		return err
	}
	p, current := st.TakeDeployFailure(req.Seq)
	if p == nil {
		return nil
	}
	if !current || !st.ApplyRegistryDeployFailed(p.Banner) {
		restate.Set(ctx, s.stateKey, st)
		return nil
	}
	restate.Set(ctx, s.stateKey, st)
	s.runFacts(ctx, st, EffectsOwnerRestate, p.Facts)
	st.AckDeployFailedPublished()
	restate.Set(ctx, s.stateKey, st)
	s.afterCommit(ctx, st, EffectsOwnerRestate)
	return nil
}

// RetryReadinessGate mirrors registry RetryPRReadiness: a merged rollout
// whose merge SHA matches, whose status is deployed, deploy_failed, or
// deploying, and that recorded reconcile requests returns to deploying with
// its original requests. A PR that was not already deploying drops its
// deployed kustomizations and jobs; a deploying PR keeps partial progress.
// It never mints a generation and never clears effect keys; it numbers the
// retry so the readiness reconcile re-fires under its own key. ok is false
// (nothing changed) when no matching merged rollout gate exists.
func (s *PRState) RetryReadinessGate(mergeSHA string) (map[string]KustomizationRequest, bool) {
	merge := strings.TrimSpace(mergeSHA)
	if s == nil || merge == "" || s.MergeSHA != merge || len(s.KustomizationRequests) == 0 ||
		!statusIn(s.Status, RegistryStatusDeployed, RegistryStatusDeployFailed, RegistryStatusDeploying) {
		return nil, false
	}
	if s.Status != RegistryStatusDeploying {
		s.DeployedKustomizations = nil
		s.DeployedJobs = nil
	}
	s.Status = RegistryStatusDeploying
	s.ReadinessRetries++
	out := make(map[string]KustomizationRequest, len(s.KustomizationRequests))
	maps.Copy(out, s.KustomizationRequests)
	return out, true
}

// ReadinessJobName scopes the readiness reconcile effect in match keys;
// the journaled key appends the retry ordinal.
const ReadinessJobName = "readiness"

// ReadinessReconcileKey keys the reconcile one accepted readiness retry
// requests: the merge SHA plus the retry ordinal, in the current
// generation. The returned match key drops the ordinal, matching the
// [synapse-effect] line synapse logs for its legacy readiness reconcile.
func ReadinessReconcileKey(s *PRState, mergeSHA string) (EffectKey, string) {
	if s == nil {
		return EffectKey{}, ""
	}
	key := s.EffectKeyFor(EffectFluxReconcile, mergeSHA, fmt.Sprintf("%s-%d", ReadinessJobName, s.ReadinessRetries), "")
	match := s.EffectKeyFor(EffectFluxReconcile, mergeSHA, ReadinessJobName, "").MatchKey()
	return key, match
}

// RolloutReconcileKey keys the reconcile of a recorded rollout request set:
// the head SHA the request event carried, in the generation the record
// minted.
func RolloutReconcileKey(s *PRState, headSHA string) EffectKey {
	return s.EffectKeyFor(EffectFluxReconcile, usableHeadSHA(headSHA), "", "")
}
