package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/k8sauth"
)

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
