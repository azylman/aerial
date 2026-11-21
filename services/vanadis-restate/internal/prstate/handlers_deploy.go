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
	"strings"
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
