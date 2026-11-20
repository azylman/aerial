package router

import (
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

type readinessRetryStore = model.ReadinessRetryStore

func (s *RouterServer) handleRetryPRReadiness(w http.ResponseWriter, r *http.Request) {
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
		resolved, err := s.authenticatePRRequest(r.Context(), r, req.Agent, req.BotID, req.Repo, req.Number)
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
	default:
		targetAgent = s.resolveOwningAgent(r.Context(), req.Repo, req.Number, "")
	}

	store, ok := s.registry.(readinessRetryStore)
	if !ok || s.dispatcher == nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: "readiness retry unavailable"})
		return
	}
	requests, err := store.RetryPRReadiness(r.Context(), req.Repo, req.Merge, req.Number)
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
	if err = s.reconcileImageKustomizations(r.Context(), refs, requests); err != nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: err.Error()})
		return
	}
	// Readiness retry never mints a new generation and never clears effect
	// keys: the route carries the expected merge SHA so Restate can
	// drop it as stale when the gate already advanced. A route failure
	// reports 503; the legacy reconcile above already ran, and the
	// operator retry safely re-triggers it without minting a generation.
	if err := s.routePREvent(r.Context(), "HandleReadinessRetry", restateclient.PREvent{
		Repo: req.Repo, PRNumber: req.Number, MergeSHA: req.Merge, Agent: targetAgent,
	}); err != nil {
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: err.Error()})
		return
	}
	httpkit.WriteJSON(w, 202, map[string]string{"status": "readiness_pending"})
}
