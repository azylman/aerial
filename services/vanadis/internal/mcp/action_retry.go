package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/vanadis/internal/model"
)

const chunkModeAction = "action"

// RetryActionChunkRequest carries the identity and optional correction for a
// durable action-chunk retry.
type RetryActionChunkRequest struct {
	BuildID                 string  `json:"build_id"`
	ChunkID                 string  `json:"chunk_id"`
	ExpectedDispatchID      string  `json:"expected_dispatch_id"`
	Reason                  string  `json:"reason"`
	ExpectedInstructions    *string `json:"expected_instructions,omitempty"    jsonschema:"Exact stored instructions required for correction; supply with replacement_instructions"`
	ReplacementInstructions *string `json:"replacement_instructions,omitempty" jsonschema:"Corrected nonblank worker instructions atomically saved before retry; supply with expected_instructions"`
}

// RetryActionChunkResult reports the accepted durable retry dispatch.
type RetryActionChunkResult struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Accepted   bool   `json:"accepted"`
}

// ErrRetryIngressUnavailable reports that durable Restate ingress is required
// for action retry.
var ErrRetryIngressUnavailable = errors.New("durable Restate ingress is required for action retry")

func validateRetryActionRequest(req RetryActionChunkRequest) error {
	if strings.TrimSpace(req.BuildID) == "" || strings.TrimSpace(req.ChunkID) == "" || strings.TrimSpace(req.ExpectedDispatchID) == "" || strings.TrimSpace(req.Reason) == "" {
		return fmt.Errorf("%w: build_id, chunk_id, expected_dispatch_id and reason are required", model.ErrInvalidBuildInput)
	}
	if (req.ExpectedInstructions == nil) != (req.ReplacementInstructions == nil) || (req.ExpectedInstructions != nil && (strings.TrimSpace(*req.ExpectedInstructions) == "" || strings.TrimSpace(*req.ReplacementInstructions) == "")) {
		return fmt.Errorf("%w: expected_instructions and replacement_instructions must be paired and nonblank", model.ErrInvalidBuildInput)
	}
	return nil
}

func retryActionIdempotencyKey(req RetryActionChunkRequest) string {
	// Quoted framing prevents ambiguity between adjacent identity components. Reason
	// is deliberately excluded: changing an explanation cannot create a new retry.
	identity := fmt.Sprintf("%q|%q|%q", req.BuildID, req.ChunkID, req.ExpectedDispatchID)
	return fmt.Sprintf("retry-action-%x", sha256.Sum256([]byte(identity)))
}

// RetryActionChunk durably retries one failed action chunk after validating
// eligibility, via the Restate Build Virtual Object RetryActionChunk handler.
func (h *Handler) RetryActionChunk(ctx context.Context, req RetryActionChunkRequest) (*RetryActionChunkResult, error) {
	if err := validateRetryActionRequest(req); err != nil {
		return nil, err
	}
	buildService, err := h.authorizeBuildMutation(ctx, req.BuildID)
	if err != nil {
		return nil, err
	}
	if h == nil || !h.RestateEnabled || h.Restate == nil || strings.TrimSpace(h.Restate.IngressURL()) == "" {
		return nil, ErrRetryIngressUnavailable
	}
	if err = h.ValidateRetryEligibility(ctx, req); err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var result RetryActionChunkResult
	if err := h.Restate.CallRetryAction(ctx, buildService, req.BuildID, body, retryActionIdempotencyKey(req), &result); err != nil {
		return nil, err
	}
	if !result.Accepted || result.BuildID != req.BuildID || result.ChunkID != req.ChunkID || result.DispatchID == "" {
		return nil, errors.New("invalid durable retry acknowledgement")
	}
	return &result, nil
}

func decodeRetryActionRequest(r *http.Request) (RetryActionChunkRequest, error) {
	var req RetryActionChunkRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		return req, fmt.Errorf("%w: invalid retry payload", model.ErrInvalidBuildInput)
	}
	if req.BuildID != r.PathValue("id") {
		return req, fmt.Errorf("%w: build_id must match path", model.ErrInvalidBuildInput)
	}
	return req, validateRetryActionRequest(req)
}

// HandleRetryActionChunk serves POST /builds/{id}/retry-action.
func (h *Handler) HandleRetryActionChunk(w http.ResponseWriter, r *http.Request) {
	req, err := decodeRetryActionRequest(r)
	if err != nil {
		writeStoreError(r.Context(), w, err)
		return
	}
	result, err := h.RetryActionChunk(r.Context(), req)
	if errors.Is(err, ErrRetryIngressUnavailable) {
		httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		writeStoreError(r.Context(), w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, result)
}

func (h *Handler) registerRetryActionTool(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{Name: "retry_action_chunk", Description: "Durably retry one failed action after reading its build and current dispatch identity. Requires build_id, chunk_id, expected_dispatch_id and reason; preserves other chunks. Optionally pair expected_instructions (exact stored text) and replacement_instructions (nonblank corrected worker instructions) to atomically correct before dispatch. Repeat the same identity to recover an uncertain response."}, func(ctx context.Context, _ *mcp.CallToolRequest, args RetryActionChunkRequest) (*mcp.CallToolResult, *RetryActionChunkResult, error) {
		result, err := h.RetryActionChunk(ctx, args)
		return nil, result, err
	})
}

// ValidateRetryEligibility rejects premature requests before consuming the
// generation's durable identity.
// The authoritative transaction rechecks these conditions under locks.
func (h *Handler) ValidateRetryEligibility(ctx context.Context, req RetryActionChunkRequest) error {
	if h.Store == nil {
		return errors.New("vanadis store not configured")
	}
	b, chunks, err := h.Store.GetBuild(ctx, req.BuildID)
	if err != nil {
		return err
	}
	if b == nil {
		return model.ErrBuildNotFound
	}
	var selected *model.VanadisChunk
	for i := range chunks {
		if chunks[i].ID == req.ChunkID {
			selected = &chunks[i]
			break
		}
	}
	if selected == nil {
		return model.ErrChunkNotFound
	}
	c := *selected
	if c.BuildID != req.BuildID || c.Mode != chunkModeAction || c.StallNudges < 0 {
		return model.ErrBuildConflict
	}
	// Older canonical identities only recover their retained durable result. If
	// retention has expired, the authoritative generation fence still prevents
	// another reset. Current-generation requests still require
	// eligibility below so a premature request cannot consume its retry key.
	generation, parseErr := strconv.Atoi(strings.TrimPrefix(req.ExpectedDispatchID, "vanadis-"+c.ID+"-turn-"))
	if parseErr == nil && generation >= 0 && generation < c.StallNudges && req.ExpectedDispatchID == fmt.Sprintf("vanadis-%s-turn-%d", c.ID, generation) {
		return nil
	}
	if b.Status != statusFailed || c.Status != statusFailed || c.StallNudges >= 2147483647 || req.ExpectedDispatchID != fmt.Sprintf("vanadis-%s-turn-%d", c.ID, c.StallNudges) {
		return model.ErrBuildConflict
	}
	if !retryInstructionsMatch(req, c) {
		return model.ErrBuildConflict
	}
	return validateRetrySiblings(c, chunks)
}

func validateRetrySiblings(c model.VanadisChunk, chunks []model.VanadisChunk) error {
	descendants := retryDescendants(c.ID, chunks)
	for _, other := range chunks {
		if other.ID != c.ID && descendants[other.ID] && other.Status != statusPending {
			return model.ErrBuildConflict
		}
	}
	for _, other := range chunks {
		if other.ID != c.ID && other.Status != statusCompleted && other.Status != statusPending {
			return model.ErrBuildConflict
		}
	}
	for _, dep := range c.DependsOn {
		ready := false
		for _, other := range chunks {
			if other.ID == dep && other.Status == statusCompleted {
				ready = true
			}
		}
		if !ready {
			return model.ErrBuildConflict
		}
	}
	return nil
}

func retryDescendants(id string, chunks []model.VanadisChunk) map[string]bool {
	descendants := map[string]bool{id: true}
	for changed := true; changed; {
		changed = false
		for _, other := range chunks {
			if descendants[other.ID] {
				continue
			}
			for _, dep := range other.DependsOn {
				if descendants[dep] {
					descendants[other.ID] = true
					changed = true
					break
				}
			}
		}
	}
	return descendants
}

func retryInstructionsMatch(req RetryActionChunkRequest, c model.VanadisChunk) bool {
	return req.ExpectedInstructions == nil || c.Instructions == *req.ExpectedInstructions
}
