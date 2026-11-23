package router

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// handlePRRegisterRestate serves /internal/pr/register: the caller is
// authenticated and authorized locally, then
// PRObject.Register (request-response) fences the existing owner, resolves
// the target (request snowflake, stored target, system-channel fallback),
// records the row and writes the projection. The response carries the
// target_id PRObject returned.
func (s *RouterServer) handlePRRegisterRestate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, rawTargetID, ok := readPRRegisterRequest(w, r)
	if !ok {
		return
	}

	if err := s.resolvePRAgent(ctx, r, &req); err != nil {
		if errors.Is(err, errBotMismatch) {
			httpkit.WriteJSON(w, http.StatusConflict, map[string]string{fieldError: errBotMismatch.Error()})
			return
		}
		k8sauth.WriteError(w, err)
		return
	}
	if valErr := validateOwnedPRRegisterRequest(&req, rawTargetID); valErr != "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: valErr})
		return
	}
	req.Metadata = injectAgentPRMetadata(req.Metadata, req.Agent)

	rr := prwire.RegisterRequest{
		Repo: req.Repo, PRNumber: req.PRNumber, Branch: req.Branch, HeadSHA: req.HeadSHA,
		TargetID: req.TargetID, Title: req.Title, Agent: req.Agent, BotID: req.BotID,
		Metadata:         registerMetadataStrings(req.Metadata),
		FallbackTargetID: model.NormalizeDiscordTargetID(s.currentConfig().SystemChannelID),
	}
	if s.k8sAuthEnabled() {
		rr.EnforceOwner = true
		rr.ExpectedOwner = req.Agent
	}
	resp, err := s.registerWithRestate(ctx, rr)
	if err != nil {
		status, msg := registerFailureResponse(err)
		slog.WarnContext(ctx, "[synapse] [restate] register failed", "repo", req.Repo, "pr", req.PRNumber, "status", status, "error", err)
		httpkit.WriteJSON(w, status, map[string]string{fieldError: msg})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:   "registered",
		"repo":        req.Repo,
		fieldPRNumber: req.PRNumber,
		fieldTargetID: resp.TargetID,
		"agent":       req.Agent,
	})
}

// registerWithRestate invokes PRObject.Register through the configured
// client, counting the outcome like every other Restate send.
func (s *RouterServer) registerWithRestate(ctx context.Context, rr prwire.RegisterRequest) (prwire.RegisterResponse, error) {
	c, err := s.restateClient()
	if err != nil {
		metrics.RecordRestateSend("Register", "error")
		return prwire.RegisterResponse{}, err
	}
	resp, err := c.Register(ctx, rr)
	if err != nil {
		metrics.RecordRestateSend("Register", "error")
		return prwire.RegisterResponse{}, err
	}
	metrics.RecordRestateSend("Register", "sent")
	return resp, nil
}

// validateOwnedPRRegisterRequest checks one register request: a missing or
// headless target travels empty and PRObject
// resolves it from the stored target or the system-channel fallback, while
// an explicit non-headless target must already be a snowflake.
func validateOwnedPRRegisterRequest(req *PRRegisterRequest, rawTargetID string) string {
	switch {
	case req.Repo == "":
		return "repo cannot be empty"
	case req.PRNumber <= 0:
		return "pr_number must be greater than 0"
	case req.Branch == "":
		return "branch cannot be empty"
	case req.HeadSHA == "":
		return "head_sha cannot be empty"
	}
	if rawTargetID == "" || (!model.IsValidDiscordSnowflake(req.TargetID) && model.IsHeadlessSessionKey(rawTargetID)) {
		req.TargetID = ""
		return ""
	}
	if !model.IsValidDiscordSnowflake(req.TargetID) {
		return errMsgInvalidTargetID
	}
	return ""
}

// registerFailureResponse maps a PRObject.Register failure to the HTTP
// status for its cause: owner mismatch
// 409, a bad or unresolvable target 400, anything else 500.
func registerFailureResponse(err error) (int, string) {
	if serr, ok := errors.AsType[*restateingress.HTTPError](err); ok {
		switch serr.Status {
		case http.StatusConflict:
			return http.StatusConflict, "pr is owned by another agent"
		case http.StatusBadRequest:
			return http.StatusBadRequest, errMsgInvalidTargetID
		}
	}
	return http.StatusInternalServerError, "Failed to register PR"
}

// registerMetadataStrings forwards the string-valued keys of the raw
// register metadata JSON opaquely; malformed JSON, blank and non-string
// values are dropped. Nil when nothing remains.
func registerMetadataStrings(raw string) map[string]string {
	var meta map[string]any
	if json.Unmarshal([]byte(raw), &meta) != nil {
		return nil
	}
	var out map[string]string
	for key, value := range meta {
		v, ok := value.(string)
		if !ok || strings.TrimSpace(v) == "" {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[key] = strings.TrimSpace(v)
	}
	return out
}

var errBotMismatch = errors.New("bot_id mismatch: unknown agent")

// errMsgInvalidTargetID is the register response for a target that is not
// (and cannot resolve to) a Discord snowflake.
const errMsgInvalidTargetID = "target_id must be a valid Discord snowflake (17-20 digits)"

// fieldTargetID is the register response key carrying the resolved target.
const fieldTargetID = "target_id"

const defaultOwnerAgent = "aerial"

func (s *RouterServer) resolveTargetAgentForAuth(caller k8sauth.CallerIdentity, requestedAgent string) (targetAgent, sarResource string, err error) {
	if caller.IsService {
		if requestedAgent == "" || !s.isKnownBot(requestedAgent) {
			return "", "", &k8sauth.ValidationError{
				Message: "missing or unknown target agent for service principal",
			}
		}
		return requestedAgent, "prs/all", nil
	}
	if requestedAgent == "" {
		targetAgent = caller.CallerAgent
	} else {
		if requestedAgent != "*" && !s.isKnownBot(requestedAgent) {
			return "", "", &k8sauth.ValidationError{
				Message: "unknown target agent",
			}
		}
		targetAgent = requestedAgent
	}
	if targetAgent == caller.CallerAgent {
		return targetAgent, "prs", nil
	}
	return targetAgent, "prs/all", nil
}

func (s *RouterServer) authenticatePRRequest(ctx context.Context, r *http.Request, agent, botID string) (string, error) {
	auth := s.getAuthenticator()
	token, err := k8sauth.ExtractBearerToken(r)
	if err != nil {
		return "", err
	}
	caller, err := auth.Authenticate(ctx, token)
	if err != nil {
		return "", err
	}
	requestedAgent := agent
	if requestedAgent == "" {
		requestedAgent = botID
	}
	if agent != "" && botID != "" && !strings.EqualFold(agent, botID) {
		if authErr := auth.Authorize(ctx, caller, "prs/all", "create", ""); authErr != nil {
			return "", &k8sauth.ForbiddenError{Message: "mismatched bot_id requires prs/all authorization"}
		}
	}
	targetAgent, sarResource, err := s.resolveTargetAgentForAuth(caller, requestedAgent)
	if err != nil {
		return "", err
	}
	if err := auth.Authorize(ctx, caller, sarResource, "create", targetAgent); err != nil {
		return "", err
	}
	return targetAgent, nil
}

func (s *RouterServer) resolvePRAgent(ctx context.Context, r *http.Request, req *PRRegisterRequest) error {
	if s.k8sAuthEnabled() {
		targetAgent, err := s.authenticatePRRequest(ctx, r, req.Agent, req.BotID)
		if err != nil {
			return err
		}
		req.Agent = targetAgent
		req.BotID = targetAgent
		return nil
	}
	requestedAgent := req.Agent
	if requestedAgent == "" {
		requestedAgent = req.BotID
	}
	if requestedAgent != "" {
		if !s.isKnownBot(requestedAgent) {
			return errBotMismatch
		}
		req.Agent = requestedAgent
		req.BotID = requestedAgent
		return nil
	}
	defaultOwner := defaultOwnerAgent
	if s.cfgSrc != nil && s.cfgSrc.Current() != nil {
		defaultOwner = s.cfgSrc.Current().DefaultOwnerAgent()
	}
	req.Agent = defaultOwner
	req.BotID = defaultOwner
	return nil
}

func injectAgentPRMetadata(raw, agent string) string {
	var metaMap map[string]any
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &metaMap); err != nil {
			metaMap = make(map[string]any)
		}
	}
	if metaMap == nil {
		metaMap = make(map[string]any)
	}
	if agent != "" {
		metaMap["agent"] = agent
		metaMap["bot_id"] = agent
	}
	if b, err := json.Marshal(metaMap); err == nil {
		return string(b)
	}
	return raw
}

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
