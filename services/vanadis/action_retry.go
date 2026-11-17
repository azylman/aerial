package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
)

type RetryActionChunkRequest struct {
	BuildID            string `json:"build_id"`
	ChunkID            string `json:"chunk_id"`
	ExpectedDispatchID string `json:"expected_dispatch_id"`
	Reason             string `json:"reason"`
}
type RetryActionChunkResult struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Accepted   bool   `json:"accepted"`
}

var errRetryIngressUnavailable = errors.New("durable Restate ingress is required for action retry")

func validateRetryActionRequest(req RetryActionChunkRequest) error {
	if strings.TrimSpace(req.BuildID) == "" || strings.TrimSpace(req.ChunkID) == "" || strings.TrimSpace(req.ExpectedDispatchID) == "" || strings.TrimSpace(req.Reason) == "" {
		return fmt.Errorf("%w: build_id, chunk_id, expected_dispatch_id and reason are required", ErrInvalidBuildInput)
	}
	return nil
}

func retryActionIdempotencyKey(req RetryActionChunkRequest) string {
	// Quoted framing prevents ambiguity between adjacent identity components. Reason
	// is deliberately excluded: changing an explanation cannot create a new retry.
	identity := fmt.Sprintf("%q|%q|%q", req.BuildID, req.ChunkID, req.ExpectedDispatchID)
	return fmt.Sprintf("retry-action-%x", sha256.Sum256([]byte(identity)))
}

func (s *VanadisServer) retryActionChunk(ctx context.Context, req RetryActionChunkRequest) (*RetryActionChunkResult, error) {
	if err := validateRetryActionRequest(req); err != nil {
		return nil, err
	}
	if s == nil || !s.restateCfg.Enabled || strings.TrimSpace(s.restateCfg.RestateIngressURL) == "" {
		return nil, errRetryIngressUnavailable
	}
	if err := s.validateRetryEligibility(ctx, req); err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	target := strings.TrimRight(s.restateCfg.RestateIngressURL, "/") + "/Build/" + url.PathEscape(req.BuildID) + "/RetryActionChunk"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", retryActionIdempotencyKey(req))
	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("durable retry request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		switch response.StatusCode {
		case http.StatusBadRequest:
			return nil, fmt.Errorf("%w: retry rejected", ErrInvalidBuildInput)
		case http.StatusNotFound:
			return nil, ErrBuildNotFound
		case http.StatusConflict:
			return nil, ErrBuildConflict
		}
		return nil, fmt.Errorf("durable retry returned HTTP %d", response.StatusCode)
	}
	var result RetryActionChunkResult
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode durable retry: %w", err)
	}
	if !result.Accepted || result.BuildID != req.BuildID || result.ChunkID != req.ChunkID || result.DispatchID == "" {
		return nil, errors.New("invalid durable retry acknowledgement")
	}
	return &result, nil
}

func decodeRetryActionRequest(r *http.Request) (RetryActionChunkRequest, error) {
	var req RetryActionChunkRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		return req, fmt.Errorf("%w: invalid retry payload", ErrInvalidBuildInput)
	}
	if req.BuildID != r.PathValue("id") {
		return req, fmt.Errorf("%w: build_id must match path", ErrInvalidBuildInput)
	}
	return req, validateRetryActionRequest(req)
}

func (s *VanadisServer) handleRetryActionChunk(w http.ResponseWriter, r *http.Request) {
	req, err := decodeRetryActionRequest(r)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	result, err := s.retryActionChunk(r.Context(), req)
	if errors.Is(err, errRetryIngressUnavailable) {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, result)
}

func (s *VanadisServer) registerRetryActionTool(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{Name: "retry_action_chunk", Description: "Durably retry one failed action after reading its build and current dispatch identity. Requires build_id, chunk_id, expected_dispatch_id and reason; preserves other chunks. Repeat the same identity to recover an uncertain response."}, func(ctx context.Context, _ *mcp.CallToolRequest, args RetryActionChunkRequest) (*mcp.CallToolResult, *RetryActionChunkResult, error) {
		result, err := s.retryActionChunk(ctx, args)
		return nil, result, err
	})
}

// Reject premature requests before consuming the generation's durable identity.
// The authoritative transaction rechecks these conditions under locks.
func (s *VanadisServer) validateRetryEligibility(ctx context.Context, req RetryActionChunkRequest) error {
	if s.store == nil {
		return errors.New("vanadis store not configured")
	}
	b, chunks, err := s.store.GetBuild(ctx, req.BuildID)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrBuildNotFound
	}
	var selected *VanadisChunk
	for i := range chunks {
		if chunks[i].ID == req.ChunkID {
			selected = &chunks[i]
			break
		}
	}
	if selected == nil {
		return ErrChunkNotFound
	}
	c := *selected
	if c.BuildID != req.BuildID || c.Mode != "action" || c.PRNumber != 0 || c.StallNudges < 0 {
		return ErrBuildConflict
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
		return ErrBuildConflict
	}
	return validateRetrySiblings(c, chunks)
}

func validateRetrySiblings(c VanadisChunk, chunks []VanadisChunk) error {
	descendants := retryDescendants(c.ID, chunks)
	for _, other := range chunks {
		if other.ID != c.ID && descendants[other.ID] && other.Status != statusPending {
			return ErrBuildConflict
		}
	}
	for _, other := range chunks {
		if other.ID != c.ID && other.Status != statusCompleted && other.Status != statusPending {
			return ErrBuildConflict
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
			return ErrBuildConflict
		}
	}
	return nil
}

func retryDescendants(id string, chunks []VanadisChunk) map[string]bool {
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
