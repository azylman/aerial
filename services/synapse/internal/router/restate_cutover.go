package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// Consumer cutover: Restate PRObject/RepoObject are the sole ordered authority
// for every github/deploy entrypoint. The legacy in-memory trackers were
// deleted in chunk 12 after a clean soak; pr_registry is a read-only
// projection and SHA lookup index, never a decision authority.
//
// What flips: with restate_authoritative: true in YAML, each entrypoint
// routes its frozen PRObject/RepoObject event to Restate first via the
// route* helpers below. Lifecycle events are one-way ingress sends keyed by
// a deterministic Idempotency-Key; only verdicts stay request-response. A
// Restate send failure returns an error (job retry, watcher tick retry,
// readiness 503) so no event is silently lost: the ingress idempotency key
// and journaled effect keys deduplicate the redelivery. Restate-owned
// events retry on a long backoff; terminal 4xx rejections drop the job. Shadow (fail-closed) sends only run when the flag is
// off; authoritative and shadow sends never double-fire.
//
// What stays live: the registry-backed pipeline (registry atomic
// transitions, publishes, dispatches) runs unchanged. Every effect decision
// resolves from the Restate PRObject when authoritative: deploy_success via
// HandleDeployDispatched, conflict republish via HandleConflictDetected with
// the HandleConflictPublished ack, CI recovery via HandleCIRecovery. The
// Postgres projection is never consulted on the authoritative path. The
// projection stays only on the legacy fallback path (non-authoritative
// conflict/deployed/ci_failed suppression) and for pure routing and
// read-model renders, each marked projection-safe at its call site.
// Collaborator DMs are at-least-once: redelivery may resend because the
// per-SHA DM marks died with the trackers.
//
// Rollback: consumer rebind via YAML config (restate_authoritative: false),
// hot-reloaded without restart or code revert. Forward-fix only: the journal
// owns effects that deleted in-memory state cannot replay.
//
// Precondition: chunk-05 first-touch hydration must show zero divergence
// (RestateShadowDivergenceTotal stays zero) before the flag flips;
// otherwise the journal starts from a projection it disagrees with.
//
// At-least-once tolerance: GitHub webhook redelivery plus journaled,
// generation-scoped effect keys on the Restate side. Journaling precedes
// every external effect, and any failure after journaling returns an
// error (webhook 5xx) so redelivery re-journals idempotently and retries
// the effect.
//
// Projection guards suppress only legacy effects, never the journal: every
// suppress check still journals when authoritative; Restate absorbs the
// repeat via stale-drop and the journaled effect key.

// Effect ownership per event: every event is stamped legacy or restate
// exactly once when synapse first accepts it (into the JetStream job at
// publish time, or once per watcher call / HTTP request) and the stamp
// rides ctx through every branch. Branches read the stamp, never the live
// config, so a hot reload mid-job cannot split one event's effects between
// synapse and prstate.

type effectsOwnerKey struct{}

// newEventEffectsOwner is the single snapshot point for a newly accepted
// event's effect owner. It is always legacy until the authority flip wires
// it to config; prstate then only records intended effects.
func newEventEffectsOwner() string {
	return restateclient.EffectsOwnerLegacy
}

// withEffectsOwner binds a normalized effect-owner stamp into ctx.
func withEffectsOwner(ctx context.Context, owner string) context.Context {
	return context.WithValue(ctx, effectsOwnerKey{}, restateclient.NormalizeEffectsOwner(owner))
}

// stampEffectsOwner snapshots the effect owner into ctx unless an earlier
// accept point already stamped it, so nested entrypoints never re-decide.
func stampEffectsOwner(ctx context.Context) context.Context {
	if _, ok := ctx.Value(effectsOwnerKey{}).(string); ok {
		return ctx
	}
	return withEffectsOwner(ctx, newEventEffectsOwner())
}

// effectsOwnerFromContext returns the stamp bound into ctx; unstamped
// contexts are legacy.
func effectsOwnerFromContext(ctx context.Context) string {
	owner, ok := ctx.Value(effectsOwnerKey{}).(string)
	if !ok {
		return restateclient.EffectsOwnerLegacy
	}
	return restateclient.NormalizeEffectsOwner(owner)
}

// restateOwnedSendError is a Restate send failure on an effects_owner=restate
// event. Nothing else performs the event's effects, so the job retries it
// with the long restate backoff; a terminal failure drops the job instead.
type restateOwnedSendError struct {
	handler  string
	terminal bool
	err      error
}

func (e *restateOwnedSendError) Error() string {
	return fmt.Sprintf("restate-owned %s send: %v", e.handler, e.err)
}

func (e *restateOwnedSendError) Unwrap() error { return e.err }

// restateOwnedFailure classifies one restate-owned send failure, logging
// terminal rejections at error level because the job is dropped.
func restateOwnedFailure(ctx context.Context, handler string, err error) error {
	if restateclient.IsTerminal(err) {
		metrics.RecordRestateShadowSend(handler, "authoritative_terminal")
		slog.ErrorContext(ctx, "[synapse] [restate-cutover] restate-owned send rejected terminally; dropping",
			"handler", handler, "error", err)
		return &restateOwnedSendError{handler: handler, terminal: true, err: err}
	}
	metrics.RecordRestateShadowSend(handler, "authoritative_error")
	slog.WarnContext(ctx, "[synapse] [restate-cutover] restate-owned send failed; retrying",
		"handler", handler, "error", err)
	return &restateOwnedSendError{handler: handler, err: err}
}

// restateOwnedSendFailure reports whether err carries a restate-owned send
// failure, and whether it is terminal.
func restateOwnedSendFailure(err error) (failed, terminal bool) {
	var rerr *restateOwnedSendError
	if !errors.As(err, &rerr) {
		return false, false
	}
	return true, rerr.terminal
}

// restateAuthoritative reports whether the Restate PRObject/RepoObject
// journal owns github/deploy consumer decisions. Nil-safe: a server without
// config stays on the legacy path.
func (s *RouterServer) restateAuthoritative() bool {
	if s == nil {
		return false
	}
	return s.currentConfig().RestateAuthoritative
}

// routeEvent is the single journal-or-mirror core behind routePREvent and
// routeRepoEvent: authoritative sends journal through the enabled reporter
// with one shared metric/error mapping, while shadow mode runs the given
// fail-closed mirror. One core keeps the two frozen event shapes from
// drifting into near-duplicate bodies. Failures on effects_owner=legacy
// events keep today's mapping; failures on effects_owner=restate events are
// classified as retryable or terminal for the job retry policy.
func (s *RouterServer) routeEvent(ctx context.Context, handler string, journal func(r *shadowRestateReporter, ctx context.Context) error, shadow func(ctx context.Context)) error {
	if !s.restateAuthoritative() {
		shadow(ctx)
		return nil
	}
	restateOwned := effectsOwnerFromContext(ctx) == restateclient.EffectsOwnerRestate
	r := s.restateShadow()
	if !r.Enabled() {
		err := errors.New("restate authoritative with no restate_url configured")
		if restateOwned {
			return restateOwnedFailure(ctx, handler, err)
		}
		return err
	}
	if err := journal(r, ctx); err != nil {
		r.sendErrors.Add(1)
		if restateOwned {
			return restateOwnedFailure(ctx, handler, err)
		}
		metrics.RecordRestateShadowSend(handler, "authoritative_error")
		slog.WarnContext(ctx, "[synapse] [restate-cutover] authoritative send failed",
			"handler", handler, "error", err)
		return err
	}
	r.sends.Add(1)
	metrics.RecordRestateShadowSend(handler, "authoritative_sent")
	return nil
}

// routePREvent journals one PRObject event when authoritative (error
// surfaces for retry) and mirrors it fail-closed when shadowing.
func (s *RouterServer) routePREvent(ctx context.Context, handler string, ev restateclient.PREvent) error {
	ev.EffectsOwner = effectsOwnerFromContext(ctx)
	if ev.Agent == "" {
		if s.registry != nil && ev.Repo != "" && ev.PRNumber > 0 {
			ev.Agent = s.resolveOwningAgent(ctx, ev.Repo, ev.PRNumber, "")
		}
		if ev.Agent == "" && s.cfgSrc != nil && s.cfgSrc.Current() != nil {
			ev.Agent = s.cfgSrc.Current().DefaultOwnerAgent()
		}
	}
	return s.routeEvent(ctx, handler,
		func(r *shadowRestateReporter, ctx context.Context) error {
			return r.client.SendPREvent(ctx, handler, ev)
		},
		func(ctx context.Context) {
			s.shadowPREvent(ctx, handler, ev)
		})
}

// routeRepoEvent journals one RepoObject event when authoritative (error
// surfaces for retry) and mirrors it fail-closed when shadowing.
func (s *RouterServer) routeRepoEvent(ctx context.Context, handler string, ev restateclient.RepoEvent) error {
	ev.EffectsOwner = effectsOwnerFromContext(ctx)
	if ev.Agent == "" && s.cfgSrc != nil && s.cfgSrc.Current() != nil {
		ev.Agent = s.cfgSrc.Current().DefaultOwnerAgent()
	}
	return s.routeEvent(ctx, handler,
		func(r *shadowRestateReporter, ctx context.Context) error {
			return r.client.SendRepoEvent(ctx, handler, ev)
		},
		func(ctx context.Context) {
			s.shadowRepoEvent(ctx, handler, ev)
		})
}

// routeCheckOutcome journals one check_run or workflow_run conclusion.
// It mirrors shadowCheckOutcome's mapping exactly: unmanaged collaborator
// runs stay inside PRObject and never signal BuildRouter. Anything with
// no journalable PRObject event (pending/neutral conclusions, PR-less or
// SHA-less resolutions) journals nothing in either mode: legacy already
// handled it, and erroring would redelivery-loop on a payload no retry
// can fix. Only transport failures surface.
func (s *RouterServer) routeCheckOutcome(ctx context.Context, res *ResolvedGitHubEvent) error {
	if !s.restateAuthoritative() {
		s.shadowCheckOutcome(ctx, res)
		return nil
	}
	handler := checkOutcomeHandler(res)
	if handler == "" || res == nil || res.PRNumber <= 0 || strings.TrimSpace(res.HeadSHA) == "" {
		return nil
	}
	return s.routePREvent(ctx, handler, restateclient.PREvent{
		Repo:      res.Repo,
		PRNumber:  res.PRNumber,
		HeadSHA:   res.HeadSHA,
		MergeSHA:  res.MergeSHA,
		CheckName: res.CheckName,
		Branch:    res.Branch,
	})
}

// routeConflictResolved journals an observed conflict-to-sync recovery.
// It mirrors shadowConflictResolved exactly.
func (s *RouterServer) routeConflictResolved(ctx context.Context, res *ResolvedGitHubEvent) error {
	if !s.restateAuthoritative() {
		s.shadowConflictResolved(ctx, res)
		return nil
	}
	if res == nil {
		return nil
	}
	return s.routePREvent(ctx, "HandleConflictResolved", restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
	})
}

// routeDeployDispatched journals one deploy_success dispatch. It mirrors
// shadowDeployDispatched exactly: the deployed commit rides alongside
// both SHAs with the job name scoping the journaled Restate effect key.
// Non-PR dispatches journal nothing: no PRObject key exists for them,
// legacy already emitted the prompt, and erroring would redelivery-loop.
func (s *RouterServer) routeDeployDispatched(ctx context.Context, repo, jobName, headSHA, mergeSHA string, prNumber int) error {
	if !s.restateAuthoritative() {
		s.shadowDeployDispatched(ctx, repo, jobName, headSHA, mergeSHA, prNumber)
		return nil
	}
	if prNumber <= 0 || (strings.TrimSpace(headSHA) == "" && strings.TrimSpace(mergeSHA) == "") {
		return nil
	}
	return s.routePREvent(ctx, "HandleDeployDispatched", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	})
}

// resolvePRVerdict is the single authoritative-verdict core behind the
// deploy, conflict, and CI-recovery resolves: authoritative sends invoke the
// frozen PRObject handler and decode the verdict, while shadow mode returns
// the zero verdict without emitting a request. A transport or decode failure
// surfaces so the caller retries instead of losing the signal. It rides on
// routeEvent so verdicts share the one metric/error mapping; callers guard
// PR-less payloads (no PRObject key exists for them).
func resolvePRVerdict[T any](ctx context.Context, s *RouterServer, handler string, ev restateclient.PREvent) (T, error) {
	var zero T
	ev.EffectsOwner = effectsOwnerFromContext(ctx)
	err := s.routeEvent(ctx, handler,
		func(r *shadowRestateReporter, ctx context.Context) error {
			return r.client.InvokePREvent(ctx, handler, ev, &zero)
		},
		func(context.Context) {},
	)
	if err != nil {
		var empty T
		return empty, err
	}
	return zero, nil
}

// resolveDeployDispatch asks the Restate PRObject whether synapse must
// publish deploy_success. The PRObject verdict is the sole authority when
// restate_authoritative is on: the Postgres deployed projection is never
// consulted here. The verdict records nothing, so a publish failure leaves
// the next invocation returning ShouldPublish again (at-least-once). A
// transport or decode failure surfaces so the caller retries. Off the
// authoritative path it returns the zero verdict without emitting a request.
func (s *RouterServer) resolveDeployDispatch(ctx context.Context, repo, jobName, headSHA, mergeSHA string, prNumber int) (restateclient.DeployDispatchDecision, error) {
	if prNumber <= 0 || (strings.TrimSpace(headSHA) == "" && strings.TrimSpace(mergeSHA) == "") {
		return restateclient.DeployDispatchDecision{}, nil
	}
	return resolvePRVerdict[restateclient.DeployDispatchDecision](ctx, s, "HandleDeployDispatched", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	})
}

// recordDeployPublished acks one confirmed deploy_success publish to the
// Restate PRObject, completing the dispatch. The ack surfaces failures so a
// lost ack redelivers: the next resolve re-resolves and republishes rather
// than losing the signal. Off the authoritative path it is a no-op.
func (s *RouterServer) recordDeployPublished(ctx context.Context, repo, jobName, headSHA, mergeSHA string, prNumber int) error {
	if !s.restateAuthoritative() {
		return nil
	}
	if prNumber <= 0 || (strings.TrimSpace(headSHA) == "" && strings.TrimSpace(mergeSHA) == "") {
		return nil
	}
	return s.routePREvent(ctx, "HandleDeploySuccessPublished", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	})
}

// publishUnownedDeploySuccessAuthoritative publishes one unowned
// deploy_success decided by the Restate PRObject. The deployed projection is
// never consulted here; a deduped or stale verdict suppresses the publish,
// and an ack failure surfaces so redelivery re-resolves and republishes.
func (s *RouterServer) publishUnownedDeploySuccessAuthoritative(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	decision, err := s.resolveDeployDispatch(ctx, evt.Repo, evt.JobName, "", evt.CommitSHA, evt.PRNumber)
	if err != nil {
		return &evt, err
	}
	if !decision.ShouldPublish {
		return &evt, nil
	}
	if err := s.publishDeployLifecycleEvent(ctx, evt, "deploy_success", statusDeployed); err != nil {
		return &evt, err
	}
	if err := s.recordDeployPublished(ctx, evt.Repo, evt.JobName, "", evt.CommitSHA, evt.PRNumber); err != nil {
		return &evt, err
	}
	return &evt, nil
}

// resolveConflictDispatch asks the Restate PRObject whether synapse must
// publish a conflict republish. The PRObject verdict is the sole authority
// when restate_authoritative is on: the Postgres conflict projection is
// never consulted here. The verdict records nothing, so a publish failure
// leaves the next invocation returning ShouldPublish again (at-least-once).
// A transport or decode failure surfaces so the caller retries. Off the
// authoritative path it returns the zero verdict without emitting a request.
func (s *RouterServer) resolveConflictDispatch(ctx context.Context, repo, branch, headSHA string, prNumber int) (restateclient.ConflictDispatchDecision, error) {
	if prNumber <= 0 {
		return restateclient.ConflictDispatchDecision{}, nil
	}
	return resolvePRVerdict[restateclient.ConflictDispatchDecision](ctx, s, "HandleConflictDetected", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, Branch: branch,
	})
}

// recordConflictPublished acks one confirmed conflict republish to the
// Restate PRObject, completing the dispatch. The ack surfaces failures so a
// lost ack redelivers: the next resolve re-resolves and republishes rather
// than losing the signal. Off the authoritative path it is a no-op.
func (s *RouterServer) recordConflictPublished(ctx context.Context, repo, branch, headSHA string, prNumber int) error {
	if !s.restateAuthoritative() {
		return nil
	}
	if prNumber <= 0 {
		return nil
	}
	return s.routePREvent(ctx, "HandleConflictPublished", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, Branch: branch,
	})
}

// resolveCIRecovery asks the Restate PRObject whether the PR is CI-failed
// for the given head SHA from the Restate-owned failure slots. The verdict
// is the sole authority when restate_authoritative is on: the Postgres
// ci_failed projection is never consulted here. The resolve is a pure read
// and records nothing. A transport or decode failure surfaces so the caller
// can fall back to the projection instead of losing the signal. Off the
// authoritative path it returns the zero verdict without emitting a request.
func (s *RouterServer) resolveCIRecovery(ctx context.Context, repo string, prNumber int, headSHA string) (restateclient.CIRecoveryDecision, error) {
	if prNumber <= 0 {
		return restateclient.CIRecoveryDecision{}, nil
	}
	return resolvePRVerdict[restateclient.CIRecoveryDecision](ctx, s, "HandleCIRecovery", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA,
	})
}

// publishConflictAuthoritative publishes one conflict lifecycle event
// decided by the Restate PRObject. The projection is never consulted here;
// a deduped or stale verdict skips the publish, and an ack failure surfaces
// so redelivery re-resolves and republishes.
func (s *RouterServer) publishConflictAuthoritative(ctx context.Context, repo string, prNumber int, branch, headSHA, targetID string) (bool, error) {
	decision, err := s.resolveConflictDispatch(ctx, repo, branch, headSHA, prNumber)
	if err != nil {
		return false, err
	}
	if !decision.ShouldPublish {
		return true, nil
	}
	if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        prConflict,
		Repo:         repo,
		PRNumber:     prNumber,
		Branch:       branch,
		HeadSHA:      headSHA,
		TargetID:     targetID,
		Status:       prConflict,
		RolloutState: s.loadPRRolloutState(ctx, repo, prNumber),
		Quiet:        isDependabotBranch(branch),
		OccurredAt:   time.Now().UTC(),
	}); err != nil {
		return false, err
	}
	if err := s.recordConflictPublished(ctx, repo, branch, headSHA, prNumber); err != nil {
		return false, err
	}
	return true, nil
}

// routeRolloutProgress journals one kustomization progress observation.
// Like shadowRolloutProgress, deploy failures have no frozen PRObject
// handler, so only progress and dispatch observations route.
func (s *RouterServer) routeRolloutProgress(ctx context.Context, repo string, prNumber int, headSHA, mergeSHA, jobName string) error {
	if !s.restateAuthoritative() {
		s.shadowRolloutProgress(ctx, repo, prNumber, headSHA, mergeSHA, jobName)
		return nil
	}
	// PR-less progress has no PRObject key; journal nothing rather than
	// redelivery-loop on an unfixable payload.
	if prNumber <= 0 {
		return nil
	}
	return s.routePREvent(ctx, "HandleRolloutProgress", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	})
}
