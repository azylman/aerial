package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
)

type readinessRetryStore interface {
	RetryPRReadiness(ctx context.Context, repo, merge string, number int) (map[string]kustomizationRequest, error)
}

// RetryPRReadiness retries the original gate, never creates a new deployment generation.
// Repeated requests while deploying retain partial progress and original request identities.
func (r *PostgresPRRegistry) RetryPRReadiness(ctx context.Context, repo, merge string, number int) (map[string]kustomizationRequest, error) {
	if r == nil || r.pool == nil || normalizeRepo(repo) == "" || strings.TrimSpace(merge) == "" || number <= 0 {
		return map[string]kustomizationRequest{}, nil
	}
	const query = `UPDATE pr_registry SET status = 'deploying',
 metadata = CASE WHEN status = 'deploying' THEN metadata ELSE metadata - 'deployed_kustomizations' END,
 updated_at = CURRENT_TIMESTAMP
 WHERE repo = $1 AND pr_number = $2 AND merge_sha = $3
 AND status IN ('deployed', 'deploy_failed', 'deploying')
 AND jsonb_typeof(metadata->'kustomization_requests') = 'object'
 AND metadata->'kustomization_requests' <> '{}'::jsonb
 RETURNING metadata->'kustomization_requests';`
	var body []byte
	found, err := r.queryRowScan(ctx, "retry readiness", query, []any{&body}, normalizeRepo(repo), number, strings.TrimSpace(merge))
	if err != nil || !found {
		return nil, err
	}
	var requests map[string]kustomizationRequest
	err = json.Unmarshal(body, &requests)
	return requests, err
}

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
	httpkit.WriteJSON(w, 202, map[string]string{"status": "readiness_pending"})
}
