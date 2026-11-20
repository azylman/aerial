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
	}
	if !readWebhookJSON(w, r, 1<<20, &req) {
		return
	}
	if strings.TrimSpace(req.Repo) == "" || req.Number <= 0 || strings.TrimSpace(req.Merge) == "" {
		httpkit.WriteJSON(w, 400, map[string]string{fieldError: "repo, pr_number and expected_merge_sha required"})
		return
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
	// keys: the shadow mirror carries the expected merge SHA so Restate can
	// drop it as stale when the gate already advanced.
	s.shadowPREvent(r.Context(), "HandleReadinessRetry", restateclient.PREvent{
		Repo: req.Repo, PRNumber: req.Number, MergeSHA: req.Merge,
	})
	httpkit.WriteJSON(w, 202, map[string]string{"status": "readiness_pending"})
}
