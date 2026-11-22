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
