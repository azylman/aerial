package router

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

func (s *RouterServer) handleRetryPRReadiness(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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
		resolved, err := s.authenticatePRRequest(ctx, r, req.Agent, req.BotID)
		if err != nil {
			k8sauth.WriteError(w, err)
			return
		}
		targetAgent = resolved
	case targetAgent != "":
		if !s.isKnownBot(targetAgent) {
			httpkit.WriteJSON(w, http.StatusConflict, map[string]string{fieldError: errBotMismatch.Error()})
			return
		}
	}
	// PRObject knows the owner; an empty agent is only a hint.
	s.retryReadinessRestate(ctx, w, req.Repo, req.Number, req.Merge, targetAgent)
}

// retryReadinessRestate is the readiness retry: PRObject
// (request-response) makes the 409/202 decision, re-arms the gate, and
// re-requests the original reconciles. Under k8s auth the existing owner
// is fenced inside PRObject, exactly like the register.
func (s *RouterServer) retryReadinessRestate(ctx context.Context, w http.ResponseWriter, repo string, prNumber int, merge, agent string) {
	rs, err := s.restateClient()
	if err != nil {
		metrics.RecordRestateSend("RetryReadiness", "error")
		httpkit.WriteJSON(w, 503, map[string]string{fieldError: "readiness retry unavailable"})
		return
	}
	req := prwire.RetryReadinessRequest{MergeSHA: merge, Agent: agent}
	if s.k8sAuthEnabled() {
		req.EnforceOwner, req.ExpectedOwner = true, agent
	}
	resp, err := rs.RetryReadiness(ctx, repo, prNumber, req)
	if err != nil {
		metrics.RecordRestateSend("RetryReadiness", "error")
		status := 503
		if serr, ok := errors.AsType[*restateingress.HTTPError](err); ok && serr.Status == http.StatusConflict {
			status = http.StatusConflict
		}
		slog.WarnContext(ctx, "[synapse] [restate] readiness retry failed", "repo", repo, "pr", prNumber, "status", status, "error", err)
		httpkit.WriteJSON(w, status, map[string]string{fieldError: err.Error()})
		return
	}
	metrics.RecordRestateSend("RetryReadiness", "sent")
	if resp.Status != http.StatusAccepted {
		httpkit.WriteJSON(w, 409, map[string]string{fieldError: "no matching merged rollout gate"})
		return
	}
	httpkit.WriteJSON(w, 202, map[string]string{"status": "readiness_pending"})
}
