package prstate

// Deploy verdicts, dedup slots and the deploy-failure deadline. PRObject is
// the single ordered authority for per-PR deploy state; Postgres pr_registry
// is a projection fed by a journaled send after the state commit.
//
// The deploy_success verdict preserves the BOTH-key deploy dedup legacy
// synapse used: a deploy counts as fully dispatched only when a PR-keyed or
// job-plus-SHA slot hit coincides with every applicable recorded effect
// key. Readiness retry never mints a generation and never clears effect
// keys.

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

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
// absorb with no publish. A watcher-tracked rollout publishes only when
// DeploySuccessDue (once per PR, on its last Kustomization). A dispatch is
// fully deduped only when the
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
	key := s.EffectKeyFor(prwire.EffectDeployDispatch, ev.HeadSHA, ev.JobName, "")
	out := DeployDispatchDecision{Disposition: DispositionApply, EffectKey: key.String()}
	if !s.DeploySuccessDue(ev.JobName) {
		return out, nil
	}
	if s.WasEffectDispatched(key) && WasDispatched(s, ev.Repo, ev.JobName, ev.HeadSHA, ev.PRNumber) {
		out.Deduped = true
		return out, nil
	}
	out.ShouldPublish = true
	return out, nil
}

// DeploySuccessDue reports whether a deploy_success for jobName may publish.
// A PR without recorded Kustomization requests (direct alert, sync) always
// may. A watcher-tracked rollout publishes once per PR, like the legacy
// MarkKustomizationsDeployed transition: only while deploy_success_pending
// is set (the last Kustomization already flipped the row) or when jobName is
// the last outstanding request of a deploying rollout.
func (s *PRState) DeploySuccessDue(jobName string) bool {
	if s == nil || len(s.KustomizationRequests) == 0 || s.DeploySuccessPending {
		return true
	}
	if !statusIn(s.Status, RegistryStatusDeploying, RegistryStatusDeployFailed) {
		return false
	}
	done := unionStrings(s.DeployedKustomizations, jobName)
	for name := range s.KustomizationRequests {
		if !slices.Contains(done, name) {
			return false
		}
	}
	return true
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
	key := s.EffectKeyFor(prwire.EffectDeployDispatch, ev.HeadSHA, ev.JobName, "")
	out := DeployPublishedOutcome{Disposition: DispositionApply, EffectKey: key.String()}
	out.Deduped = s.WasEffectDispatched(key)
	s.MarkEffectDispatched(key)
	s.RecordDeployedPR()
	if cleanJob, cleanHead := strings.TrimSpace(ev.JobName), strings.TrimSpace(ev.HeadSHA); cleanJob != "" && cleanHead != "" && !prwire.IsZeroSHA(cleanHead) {
		s.RecordDeployedSHAJob(cleanJob, cleanHead)
	}
	syncHeadSHA(s, ev)
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
	if prNumber > 0 && prNumber == s.PRNumber && strings.TrimSpace(repo) == s.Repo && s.WasDeployedPR() {
		return true
	}
	cleanJob := strings.TrimSpace(jobName)
	cleanSHA := strings.TrimSpace(sha)
	if cleanJob == "" || cleanSHA == "" || prwire.IsZeroSHA(cleanSHA) {
		return false
	}
	if s.DeploySHAJob == nil {
		return false
	}
	return s.DeploySHAJob[DeploySHAJobKey(repo, cleanJob, cleanSHA)]
}

// syncHeadSHA records the observed head SHA after a successful apply so
// later events judge staleness against it. A merged PR keeps its head
// unless the event repairs a stale one (RecoverMergedHead): deploy events
// may carry the merge commit, which must not rewrite it.
func syncHeadSHA(s *PRState, ev PREvent) {
	if s.Terminal == TerminalMerged {
		s.RecoverMergedHead(ev)
		return
	}
	if clean := strings.TrimSpace(ev.HeadSHA); clean != "" && !prwire.IsZeroSHA(clean) {
		s.SetHead(clean)
	}
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

// HandleDeployAttemptFailed records one failed deploy attempt (image
// resolve, promotion, or reconcile-request record) and arms a bounded
// deploy-failure deadline as a delayed self-send; the deadline, not a
// delivery count, decides deploy_failed.
func (s *PRObjectService) HandleDeployAttemptFailed(ctx restate.ObjectContext, ev PREvent) error {
	if strings.TrimSpace(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, stored, err := s.loadPRState(ctx, ev)
	if err != nil {
		return err
	}
	if s.dropUntrackedRollout(ctx, st, stored, prwire.PRHandlerDeployAttemptFailed) || st.ApplyDeployEvent(ev) != DispositionApply {
		return nil
	}
	seq, armed := st.ArmDeployFailure(ev)
	restate.Set(ctx, s.stateKey, st)
	if armed {
		restate.ObjectSend(ctx, prwire.PRObjectServiceName, prwire.PRObjectKey(st.Repo, st.PRNumber), prwire.PRHandlerDeployFailureDeadline).
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
	// Like legacy AtomicTransitionDeployFailed, exhaustion writes no banner;
	// the failure detail rides the deploy_failed fact.
	if !current || !st.ApplyRegistryDeployFailed("") {
		restate.Set(ctx, s.stateKey, st)
		return nil
	}
	restate.Set(ctx, s.stateKey, st)
	s.runFacts(ctx, st, p.Facts)
	st.AckDeployFailedPublished()
	restate.Set(ctx, s.stateKey, st)
	s.afterCommit(ctx, st)
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

// ReadinessReconcileKey keys the reconcile one accepted readiness retry
// requests: the merge SHA plus the retry ordinal, in the current
// generation. The returned match key drops the ordinal, matching the
// [synapse-effect] line synapse logs for its legacy readiness reconcile.
func ReadinessReconcileKey(s *PRState, mergeSHA string) (EffectKey, string) {
	if s == nil {
		return EffectKey{}, ""
	}
	key := s.EffectKeyFor(prwire.EffectFluxReconcile, mergeSHA, fmt.Sprintf("%s-%d", prwire.ReadinessJobName, s.ReadinessRetries), "")
	match := s.EffectKeyFor(prwire.EffectFluxReconcile, mergeSHA, prwire.ReadinessJobName, "").MatchKey()
	return key, match
}

// RolloutReconcileKey keys the reconcile of a recorded rollout request set:
// the head SHA the request event carried, in the generation the record
// minted.
func RolloutReconcileKey(s *PRState, headSHA string) EffectKey {
	return s.EffectKeyFor(prwire.EffectFluxReconcile, usableHeadSHA(headSHA), "", "")
}
