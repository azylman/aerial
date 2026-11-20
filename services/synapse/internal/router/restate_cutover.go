package router

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// Consumer cutover: Restate PRObject/RepoObject are the ordered authority for
// every github/deploy entrypoint while StateTracker and DeployDedupTracker
// stay compiled in as inert fallback for one deploy soak.
//
// What flips: with restate_authoritative: true in YAML, each entrypoint
// routes its frozen PRObject/RepoObject event to Restate first via the
// route* helpers below. A Restate send failure returns an error (webhook
// 5xx for GitHub redelivery, watcher tick retry, readiness 503) so no
// event is silently lost: Restate journaled effect keys deduplicate the
// redelivery, and the legacy in-memory guards below make the legacy
// re-run idempotent. Shadow (fail-closed) sends only run when the flag is
// off; authoritative and shadow sends never double-fire.
//
// What stays live: the whole legacy pipeline (StateTracker and
// DeployDedupTracker reads, writes, registry transitions, publishes,
// dispatches) runs unchanged. The trackers are "inert" only in the
// authority sense: Restate's journal is the ordered record every later
// chunk builds on, while the trackers execute byte-identical behavior as
// the soak fallback. Reads and writes stay because the legacy guards are
// load-bearing for redelivery idempotency: active-failure state
// suppresses repeat completion on webhook redelivery, collaborator and
// conflict marks dedup DMs, and deploy dedup suppresses repeat prompts.
// Gating those writes would trade a clean journal for duplicate
// user-visible effects, which the soak gate forbids. In-flight builds and
// PRs therefore complete exactly as before; post-cutover facts accumulate
// in both the journal and the legacy trackers until chunk 12 deletes the
// trackers after a clean soak.
//
// Rollback: consumer rebind via YAML config (restate_authoritative: false),
// hot-reloaded without restart or code revert. Time-boxed window: one
// deploy soak (about 48h) after the flip. The rebind is valid only before
// the first Restate-committed external effect; after that point of no
// return, rollback is forward-fix only because the journal owns effects
// legacy state cannot replay. No Restate handlers fire external effects
// in this build (state and projection only; effect wiring lands in later
// chunks), so the rebind window stays open for the whole soak.
//
// Precondition: chunk-05 first-touch hydration must show zero divergence
// (RestateShadowDivergenceTotal stays zero) before the flag flips;
// otherwise the journal starts from a projection it disagrees with.
//
// At-least-once tolerance: GitHub webhook redelivery plus journaled,
// generation-scoped effect keys on the Restate side. Journaling precedes
// every external effect, and any failure after journaling returns an
// error (webhook 5xx) so redelivery re-journals idempotently and retries
// the effect; no ack or dedup mark lands until the effect succeeds.
//
// Dedup guards suppress only legacy effects, never the journal: a mark
// proves legacy already published/dispatched the event, not that Restate
// journaled it — pre-flip marks predate any journal because the flag
// hot-reloads without restarting the process. Every suppress hit still
// journals when authoritative; Restate absorbs the repeat via stale-drop,
// the per-SHA mark, and the journaled effect key. Skipping the journal on
// a hit would lose pre-flip events forever, while re-journaling is free.

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
