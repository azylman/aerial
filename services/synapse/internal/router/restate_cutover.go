package router

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

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
// route* helpers below. A Restate send failure returns an error (webhook
// 5xx for GitHub redelivery, watcher tick retry, readiness 503) so no
// event is silently lost: Restate journaled effect keys deduplicate the
// redelivery. Shadow (fail-closed) sends only run when the flag is
// off; authoritative and shadow sends never double-fire.
//
// What stays live: the registry-backed pipeline (registry transitions,
// publishes, dispatches) runs unchanged. Per-SHA conflict republish
// suppression reads the projection lifecycle head SHA; deploy_success
// suppression reads the deployed projection; recovery reads the ci_failed
// projection. Collaborator DMs are at-least-once: redelivery may resend
// because the per-SHA DM marks died with the trackers.
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
// drifting into near-duplicate bodies.
func (s *RouterServer) routeEvent(ctx context.Context, handler string, journal func(r *shadowRestateReporter, ctx context.Context) error, shadow func(ctx context.Context)) error {
	if !s.restateAuthoritative() {
		shadow(ctx)
		return nil
	}
	r := s.restateShadow()
	if !r.Enabled() {
		return fmt.Errorf("restate authoritative with no restate_url configured")
	}
	if err := journal(r, ctx); err != nil {
		r.sendErrors.Add(1)
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

// resolveDeployDispatch asks the Restate PRObject whether synapse must
// publish deploy_success. The PRObject verdict is the sole authority when
// restate_authoritative is on: the Postgres deployed projection is never
// consulted here. The verdict records nothing, so a publish failure leaves
// the next invocation returning ShouldPublish again (at-least-once). A
// transport or decode failure surfaces so the caller retries. Off the
// authoritative path it returns the zero verdict without emitting a request.
func (s *RouterServer) resolveDeployDispatch(ctx context.Context, repo, jobName, headSHA, mergeSHA string, prNumber int) (restateclient.DeployDispatchDecision, error) {
	if !s.restateAuthoritative() {
		return restateclient.DeployDispatchDecision{}, nil
	}
	if prNumber <= 0 || (strings.TrimSpace(headSHA) == "" && strings.TrimSpace(mergeSHA) == "") {
		return restateclient.DeployDispatchDecision{}, nil
	}
	r := s.restateShadow()
	if !r.Enabled() {
		return restateclient.DeployDispatchDecision{}, fmt.Errorf("restate authoritative with no restate_url configured")
	}
	var decision restateclient.DeployDispatchDecision
	if err := r.client.InvokePREvent(ctx, "HandleDeployDispatched", restateclient.PREvent{
		Repo: repo, PRNumber: prNumber, HeadSHA: headSHA, MergeSHA: mergeSHA, JobName: jobName,
	}, &decision); err != nil {
		r.sendErrors.Add(1)
		metrics.RecordRestateShadowSend("HandleDeployDispatched", "authoritative_error")
		slog.WarnContext(ctx, "[synapse] [restate-cutover] authoritative deploy decision failed",
			"repo", repo, "pr", prNumber, "job", jobName, "error", err)
		return restateclient.DeployDispatchDecision{}, err
	}
	r.sends.Add(1)
	metrics.RecordRestateShadowSend("HandleDeployDispatched", "authoritative_sent")
	return decision, nil
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
