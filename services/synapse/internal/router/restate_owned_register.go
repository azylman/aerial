package router

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// handlePRRegisterRestate is the restate-owned /internal/pr/register: the
// caller is authenticated and authorized exactly as on the legacy path, then
// PRObject.Register (request-response) fences the existing owner, resolves
// the target (request snowflake, stored target, system-channel fallback),
// records the row and writes the projection. The response carries the
// target_id PRObject returned. pr_registry is never read or written.
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
		writeK8sAuthError(w, err)
		return
	}
	if valErr := validateOwnedPRRegisterRequest(&req, rawTargetID); valErr != "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: valErr})
		return
	}
	req.Metadata = injectAgentPRMetadata(req.Metadata, req.Agent)

	rr := restateclient.RegisterRequest{
		Repo: req.Repo, PRNumber: req.PRNumber, Branch: req.Branch, HeadSHA: req.HeadSHA,
		TargetID: req.TargetID, Title: req.Title, Agent: req.Agent, BotID: req.BotID,
		Metadata:         registerMetadataStrings(req.Metadata),
		FallbackTargetID: model.NormalizeDiscordTargetID(s.currentConfig().SystemChannelID),
		EffectsOwner:     restateclient.EffectsOwnerRestate,
	}
	if s.k8sAuthEnabled() {
		rr.EnforceOwner = true
		rr.ExpectedOwner = req.Agent
	}
	resp, err := s.registerWithRestate(ctx, rr)
	if err != nil {
		status, msg := registerFailureResponse(err)
		slog.WarnContext(ctx, "[synapse] [restate-cutover] restate-owned register failed", "repo", req.Repo, "pr", req.PRNumber, "status", status, "error", err)
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
// client, counting the outcome like every other restate-owned send.
func (s *RouterServer) registerWithRestate(ctx context.Context, rr restateclient.RegisterRequest) (restateclient.RegisterResponse, error) {
	r := s.restateShadow()
	if !r.Enabled() {
		metrics.RecordRestateShadowSend("Register", "authoritative_error")
		return restateclient.RegisterResponse{}, errors.New("restate-owned register with no restate_url configured")
	}
	resp, err := r.client.Register(ctx, rr)
	if err != nil {
		r.sendErrors.Add(1)
		metrics.RecordRestateShadowSend("Register", "authoritative_error")
		return restateclient.RegisterResponse{}, err
	}
	r.sends.Add(1)
	metrics.RecordRestateShadowSend("Register", "authoritative_sent")
	return resp, nil
}

// validateOwnedPRRegisterRequest mirrors validatePRRegisterRequest without
// the registry: a missing or headless target travels empty and PRObject
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
	if rawTargetID == "" || (!isValidDiscordSnowflake(req.TargetID) && isHeadlessSessionKey(rawTargetID)) {
		req.TargetID = ""
		return ""
	}
	if !isValidDiscordSnowflake(req.TargetID) {
		return errMsgInvalidTargetID
	}
	return ""
}

// registerFailureResponse maps a PRObject.Register failure to the HTTP
// status the legacy register returns for the same cause: owner mismatch
// 409, a bad or unresolvable target 400, anything else 500.
func registerFailureResponse(err error) (int, string) {
	if serr, ok := errors.AsType[*restateclient.StatusError](err); ok {
		switch serr.Code {
		case http.StatusConflict:
			return http.StatusConflict, "pr is owned by another agent"
		case http.StatusBadRequest:
			return http.StatusBadRequest, errMsgInvalidTargetID
		}
	}
	return http.StatusInternalServerError, "Failed to register PR"
}

// Shadow lookup comparison: on legacy-owned events with Restate enabled,
// the SHA->PR and owner lookups synapse answers from pr_registry are
// compared read-only against RepoObject.ResolvePRBySHA and the PRObject
// owner. Mismatches count synapse_restate_lookup_divergence_total{kind} and
// log [restate-shadow]; the legacy answer is always the one used.
const (
	lookupKindSHAPR        = "sha_pr"
	lookupKindSHAPRMissing = "sha_pr_missing"
	lookupKindOwner        = "owner"
	lookupKindOwnerMissing = "owner_missing"
)

// shadowLookupEnabled reports whether a legacy-owned lookup should be
// compared against Restate.
func (s *RouterServer) shadowLookupEnabled(ctx context.Context) bool {
	return s != nil && !restateOwnsEffects(ctx) && s.restateShadow().Enabled()
}

// shaLookupDivergence classifies one SHA->PR comparison; empty means the
// two agree (or Restate failed and nothing can be said).
func shaLookupDivergence(legacyPR int, restate restateclient.ResolveSHAResponse) string {
	switch {
	case legacyPR <= 0:
		return ""
	case !restate.Found:
		return lookupKindSHAPRMissing
	case restate.PRNumber != legacyPR:
		return lookupKindSHAPR
	default:
		return ""
	}
}

// ownerLookupDivergence classifies one owner comparison.
func ownerLookupDivergence(legacyOwner string, restate restateclient.BuildLifecycle) string {
	legacyOwner = strings.TrimSpace(legacyOwner)
	restateOwner := strings.TrimSpace(restate.Agent)
	switch {
	case legacyOwner == "":
		return ""
	case restateOwner == "":
		return lookupKindOwnerMissing
	case !strings.EqualFold(restateOwner, legacyOwner):
		return lookupKindOwner
	default:
		return ""
	}
}

// recordLookupDivergence counts and logs one shadow lookup mismatch.
func recordLookupDivergence(ctx context.Context, kind, repo string, prNumber int, legacy, restate string) {
	if kind == "" {
		return
	}
	metrics.RecordRestateLookupDivergence(kind)
	slog.WarnContext(ctx, "[synapse] [restate-shadow] lookup diverged from pr_registry",
		"kind", kind, "repo", normalizeRepo(repo), "pr", prNumber, "legacy", legacy, "restate", restate)
}

// shadowCompareSHALookup compares one legacy SHA->PR resolution.
func (s *RouterServer) shadowCompareSHALookup(ctx context.Context, repo, sha string, legacyPR int) {
	if legacyPR <= 0 || !s.shadowLookupEnabled(ctx) {
		return
	}
	got, err := s.restateShadow().client.ResolvePRBySHA(ctx, repo, sha)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-shadow] sha lookup failed closed", "repo", repo, "sha", sha, "error", err)
		return
	}
	recordLookupDivergence(ctx, shaLookupDivergence(legacyPR, got), repo, legacyPR, sha, strconv.Itoa(got.PRNumber))
}

// shadowCompareOwnerLookup compares one legacy owner resolution.
func (s *RouterServer) shadowCompareOwnerLookup(ctx context.Context, repo string, prNumber int, legacyOwner string) {
	if prNumber <= 0 || strings.TrimSpace(legacyOwner) == "" || !s.shadowLookupEnabled(ctx) {
		return
	}
	got, err := s.restateShadow().client.GetBuildLifecycle(ctx, repo, prNumber)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-shadow] owner lookup failed closed", "repo", repo, "pr", prNumber, "error", err)
		return
	}
	recordLookupDivergence(ctx, ownerLookupDivergence(legacyOwner, got), repo, prNumber, legacyOwner, got.Agent)
}
