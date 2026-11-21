package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

type readinessRetryStore = model.ReadinessRetryStore

func (s *RouterServer) handleRetryPRReadiness(w http.ResponseWriter, r *http.Request) {
	receivedAt := time.Now()
	// The effects owner is snapshotted once per request, so a hot reload
	// mid-request cannot split it between the registry and Restate.
	ctx := stampEffectsOwner(r.Context())
	var req struct {
		Repo   string `json:"repo"`
		Number int    `json:"pr_number"`
		Merge  string `json:"expected_merge_sha"`
		Agent  string `json:"agent,omitempty"`
		BotID  string `json:"bot_id,omitempty"`
	}
	if !readWebhookJSON(w, r, 1<<20, &req) {
		return
	}
	req.Repo = strings.TrimSpace(req.Repo)
	req.Merge = strings.TrimSpace(req.Merge)
	req.Agent = strings.TrimSpace(req.Agent)
	req.BotID = strings.TrimSpace(req.BotID)

	if req.Repo == "" || req.Number <= 0 || req.Merge == "" {
		httpkit.WriteJSON(w, 400, map[string]string{fieldError: "repo, pr_number and expected_merge_sha required"})
		return
	}

	targetAgent := req.Agent
	if targetAgent == "" {
		targetAgent = req.BotID
	}

	switch {
	case s.k8sAuthEnabled():
		resolved, err := s.authenticatePRRequest(ctx, r, req.Agent, req.BotID, req.Repo, req.Number)
		if err != nil {
			writeK8sAuthError(w, err)
			return
		}
		targetAgent = resolved
	case targetAgent != "":
		if !s.isKnownBot(targetAgent) {
			httpkit.WriteJSON(w, http.StatusConflict, map[string]string{fieldError: "bot_id mismatch for local pr_registry"})
			return
		}
	case restateOwnsEffects(ctx):
		// PRObject knows the owner; an empty agent is only a hint.
	default:
		targetAgent = s.resolveOwningAgent(ctx, req.Repo, req.Number, "")
	}
	if restateOwnsEffects(ctx) {
		s.retryReadinessRestate(ctx, w, req.Repo, req.Number, req.Merge, targetAgent)
		return
	}

	store, ok := s.registry.(readinessRetryStore)
	if !ok || s.dispatcher == nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: "readiness retry unavailable"})
		return
	}
	requests, err := store.RetryPRReadiness(ctx, req.Repo, req.Merge, req.Number)
	if err != nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: err.Error()})
		return
	}
	if len(requests) == 0 {
		httpkit.WriteJSON(w, 409, map[string]string{fieldError: "no matching merged rollout gate"})
		return
	}
	refs := make([]fluxKustomizationRef, 0, len(requests))
	for name, request := range requests {
		refs = append(refs, fluxKustomizationRef{Namespace: request.Namespace, Name: name})
	}
	if err = s.reconcileImageKustomizations(ctx, refs, requests); err != nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: err.Error()})
		return
	}
	recordLegacyEffect(ctx, restateclient.EffectFluxReconcile,
		restateclient.EffectMatchKey(req.Repo, req.Number, req.Merge, restateclient.EffectFluxReconcile, restateclient.ReadinessJobName, ""), normalizeRepo(req.Repo), req.Number)
	// Readiness retry never mints a new generation and never clears effect
	// keys: the route carries the expected merge SHA so Restate can
	// drop it as stale when the gate already advanced. A route failure
	// reports 503; the legacy reconcile above already ran, and the
	// operator retry safely re-triggers it without minting a generation.
	// Each operator retry is its own logical request: the identity carries
	// the request's arrival time so a deliberate re-trigger is not deduped,
	// and the effect owner is snapshotted once for the request.
	routeCtx := restateclient.WithEventIdentity(ctx,
		fmt.Sprintf("readiness:%s#%d:%s:%d", req.Repo, req.Number, req.Merge, receivedAt.UnixNano()))
	if err := s.routePREvent(routeCtx, "HandleReadinessRetry", restateclient.PREvent{
		Repo: req.Repo, PRNumber: req.Number, MergeSHA: req.Merge, Agent: targetAgent,
	}); err != nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: err.Error()})
		return
	}
	httpkit.WriteJSON(w, 202, map[string]string{"status": "readiness_pending"})
}

// retryReadinessRestate is the restate-owned readiness retry: PRObject
// (request-response) makes the 409/202 decision, re-arms the gate, and
// re-requests the original reconciles; pr_registry is never read or
// written. Under k8s auth the existing owner is fenced inside PRObject,
// exactly like the restate-owned register.
func (s *RouterServer) retryReadinessRestate(ctx context.Context, w http.ResponseWriter, repo string, prNumber int, merge, agent string) {
	rs := s.restateShadow()
	if !rs.Enabled() {
		metrics.RecordRestateShadowSend("RetryReadiness", "authoritative_error")
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: "readiness retry unavailable"})
		return
	}
	req := restateclient.RetryReadinessRequest{MergeSHA: merge, Agent: agent, EffectsOwner: restateclient.EffectsOwnerRestate}
	if s.k8sAuthEnabled() {
		req.EnforceOwner, req.ExpectedOwner = true, agent
	}
	resp, err := rs.client.RetryReadiness(ctx, repo, prNumber, req)
	if err != nil {
		rs.sendErrors.Add(1)
		metrics.RecordRestateShadowSend("RetryReadiness", "authoritative_error")
		status := 503
		if serr, ok := errors.AsType[*restateclient.StatusError](err); ok && serr.Code == http.StatusConflict {
			status = http.StatusConflict
		}
		slog.WarnContext(ctx, "[synapse] [restate-cutover] restate-owned readiness retry failed", "repo", repo, "pr", prNumber, "status", status, "error", err)
		httpkit.WriteJSON(w, status, map[string]string{fieldError: err.Error()})
		return
	}
	rs.sends.Add(1)
	metrics.RecordRestateShadowSend("RetryReadiness", "authoritative_sent")
	if resp.Status != http.StatusAccepted {
		httpkit.WriteJSON(w, 409, map[string]string{fieldError: "no matching merged rollout gate"})
		return
	}
	httpkit.WriteJSON(w, 202, map[string]string{"status": "readiness_pending"})
}
