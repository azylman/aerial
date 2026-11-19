package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	KustomizationRequestsKey  = "kustomization_requests"
	DeployedKustomizationsKey = "deployed_kustomizations"
)

// RecordKustomizationRequests replaces the PR's kustomization_requests wholesale and clears
// deployed_kustomizations so completion is tracked only against the latest reconcile requests.
// A new request set starts a new gate generation, so it also clears any
// deploy_success_pending/claimed claim flags from the previous generation.
func (r *PostgresPRRegistry) RecordKustomizationRequests(ctx context.Context, repo string, prNumber int, requests map[string]model.KustomizationRequest) error {
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || len(requests) == 0 {
		return nil
	}
	reqBytes, err := json.Marshal(requests)
	if err != nil {
		return fmt.Errorf("marshal kustomization requests: %w", err)
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(((COALESCE(metadata, '{}'::jsonb) - '` + DeployedKustomizationsKey + `') - 'deploy_success_pending') - 'deploy_success_claimed', '{` + KustomizationRequestsKey + `}', $3::jsonb),
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2;`
	if _, err := r.pool.Exec(qCtx, query, repo, prNumber, reqBytes); err != nil {
		return fmt.Errorf("record kustomization requests: %w", err)
	}
	return nil
}

// MarkKustomizationsDeployed unions names (those recorded in kustomization_requests) into
// deployed_kustomizations of a deploying or tracked failed PR and flips it to deployed once every recorded Kustomization
// is present. The row lock taken by FOR UPDATE serializes concurrent callers on the latest row version,
// so exactly one caller observes the transition and gets true.
func (r *PostgresPRRegistry) MarkKustomizationsDeployed(ctx context.Context, repo string, prNumber int, names []string) (bool, error) {
	repo = model.NormalizeRepo(repo)
	names = model.DedupeTrimmedStrings(names)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || len(names) == 0 {
		return false, nil
	}
	query := `
		WITH cur AS (
			SELECT id, metadata AS meta
			FROM pr_registry
			WHERE repo = $1 AND pr_number = $2 AND status IN ('deploying', 'deploy_failed')
			  AND metadata->'` + KustomizationRequestsKey + `' <> '{}'::jsonb
			  AND jsonb_typeof(metadata->'` + KustomizationRequestsKey + `') = 'object'
			FOR UPDATE
		), next AS (
			SELECT id, meta, (
				SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
				FROM jsonb_array_elements(
					CASE WHEN jsonb_typeof(meta->'` + DeployedKustomizationsKey + `') = 'array' THEN meta->'` + DeployedKustomizationsKey + `' ELSE '[]'::jsonb END
					|| (SELECT COALESCE(jsonb_agg(n), '[]'::jsonb) FROM unnest($3::text[]) AS n WHERE meta->'` + KustomizationRequestsKey + `' ? n)
				) AS elem
			) AS deployed
			FROM cur
		), completed AS (
			SELECT id, meta, deployed,
			    NOT EXISTS (SELECT 1 FROM jsonb_object_keys(meta->'` + KustomizationRequestsKey + `') AS k WHERE NOT deployed ? k) AS done
			FROM next
		)
		UPDATE pr_registry p
		SET metadata = jsonb_set(
				CASE
					WHEN n.done THEN jsonb_set(n.meta - 'banner' - 'deploy_failed_published', '{deploy_success_pending}', 'true'::jsonb, true)
					ELSE n.meta
				END,
				'{` + DeployedKustomizationsKey + `}',
				n.deployed,
				true
			),
		    status = CASE
		        WHEN n.done THEN 'deployed'
		        ELSE p.status
		    END,
		    updated_at = CURRENT_TIMESTAMP
		FROM completed n
		WHERE p.id = n.id
		RETURNING p.status = 'deployed';`
	var transitioned bool
	if _, err := r.queryRowScan(ctx, "mark kustomizations deployed", query, []any{&transitioned}, repo, prNumber, names); err != nil {
		return false, err
	}
	return transitioned, nil
}

// HasKustomizationRequests reports whether the PR's completion is tracked by the Kustomization watcher.
func (r *PostgresPRRegistry) HasKustomizationRequests(ctx context.Context, repo string, prNumber int) (bool, error) {
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return false, nil
	}
	query := `SELECT COALESCE(jsonb_typeof(metadata->'` + KustomizationRequestsKey + `') = 'object', false) FROM pr_registry WHERE repo = $1 AND pr_number = $2;`
	var tracked bool
	if _, err := r.queryRowScan(ctx, "has kustomization requests", query, []any{&tracked}, repo, prNumber); err != nil {
		return false, err
	}
	return tracked, nil
}

// RetryPRReadiness retries the original gate and starts a fresh publish
// generation: it clears partial progress markers and any
// deploy_success_pending/claimed claim flags so a retried gate cannot inherit
// a previous generation's claim. Repeated requests while deploying retain the
// original request identities.
func (r *PostgresPRRegistry) RetryPRReadiness(ctx context.Context, repo, merge string, number int) (map[string]model.KustomizationRequest, error) {
	repo = model.NormalizeRepo(repo)
	merge = strings.TrimSpace(merge)
	if r == nil || r.pool == nil || repo == "" || merge == "" || number <= 0 {
		return map[string]model.KustomizationRequest{}, nil
	}
	const query = `UPDATE pr_registry SET status = 'deploying',
 metadata = ((CASE WHEN status = 'deploying' THEN metadata ELSE metadata - '` + DeployedKustomizationsKey + `' END) - 'deploy_success_pending') - 'deploy_success_claimed',
 updated_at = CURRENT_TIMESTAMP
 WHERE repo = $1 AND pr_number = $2 AND merge_sha = $3
 AND status IN ('deployed', 'deploy_failed', 'deploying')
 AND jsonb_typeof(metadata->'` + KustomizationRequestsKey + `') = 'object'
 AND metadata->'` + KustomizationRequestsKey + `' <> '{}'::jsonb
 RETURNING metadata->'` + KustomizationRequestsKey + `';`
	var body []byte
	found, err := r.queryRowScan(ctx, "retry readiness", query, []any{&body}, repo, number, merge)
	if err != nil || !found {
		return nil, err
	}
	var requests map[string]model.KustomizationRequest
	err = json.Unmarshal(body, &requests)
	return requests, err
}
