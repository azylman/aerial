// Package mcp holds the Vanadis MCP tool surface: argument/output shapes,
// chunk-brief projection, the Streamable HTTP handler, and the durable
// action-retry flow. It depends only on internal/model, internal/store and
// internal/restate (L2), plus shared pkg helpers.
package mcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/discordutil"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis/internal/model"
	"github.com/azylman/aerial/services/vanadis/internal/restate"
	"github.com/azylman/aerial/services/vanadis/internal/store"
)

const (
	serviceName = "vanadis"

	statusUpdated   = "updated"
	statusFailed    = "failed"
	statusRunning   = "running"
	statusPending   = "pending"
	statusCompleted = "completed"

	defaultAgent = "aerial"
)

// ServerInstructions is sent on initialize so clients that defer tool schemas
// still learn build/chunk lifecycle rules and ID provenance.
const ServerInstructions = `Read state first: get_build (by build_id, or by thread_id which resolves to that thread's latest running build, else its most recent build) returns status and chunk IDs; get_chunk returns full instructions by chunk_id or build_id + chunk_ref (chunk ID or 0-based index).
update_chunk and remove_chunk only work on chunks still pending; running, completed or failed chunks are rejected with a conflict. append_chunk adds pending chunks to the end of a build; update_build replaces the build title, objective or shared_context.
An action chunk passes only by calling complete_chunk with its dispatch_id and a summary of the PASS evidence; a turn that ends without it is resumed automatically, then failed (blocking only its dependents).
fail_chunk fails the chunk and blocks only its dependents; other chunks keep running and retry_action_chunk repairs it in place. Use it for blockers, not for fixable test, lint or CI failures; fix forward instead.`

// MCPChunkBrief captures the concise chunk fields exposed in MCP build listings.
type MCPChunkBrief struct {
	ID         string   `json:"id"`
	ChunkIndex int      `json:"chunk_index"`
	Title      string   `json:"title"`
	Mode       string   `json:"mode"`
	Status     string   `json:"status"`
	Repo       string   `json:"repo"`
	PRNumber   int      `json:"pr_number"`
	DependsOn  []string `json:"depends_on"`
}

// MCPCreateBuildArgs defines arguments for create_build tool.
type MCPCreateBuildArgs struct {
	ID            string               `json:"id,omitempty"             jsonschema:"Optional unique build identifier (e.g. 'build-1793405777-309e5517')."`
	ThreadID      string               `json:"thread_id"                jsonschema:"Discord thread snowflake or normalized identifier."`
	Tier          int                  `json:"tier,omitempty"           jsonschema:"Build execution tier (default 2)."`
	Title         string               `json:"title,omitempty"          jsonschema:"High-level build title."`
	Objective     string               `json:"objective"                jsonschema:"Primary objective of the build."`
	SharedContext string               `json:"shared_context,omitempty" jsonschema:"Shared context across all chunks."`
	Agent         string               `json:"agent,omitempty"          jsonschema:"Target execution agent override."`
	Chunks        []MCPCreateChunkItem `json:"chunks"                   jsonschema:"List of initial chunks forming the build DAG."`
}

// MCPCreateBuildOutput defines output for create_build tool.
type MCPCreateBuildOutput struct {
	Status       string `json:"status"`
	BuildID      string `json:"build_id"`
	ThreadID     string `json:"thread_id"`
	Tier         int    `json:"tier"`
	ChunkCount   int    `json:"chunk_count"`
	FirstChunkID string `json:"first_chunk_id"`
}

// MCPCancelBuildArgs defines arguments for cancel_build tool.
type MCPCancelBuildArgs struct {
	BuildID string `json:"build_id" jsonschema:"Vanadis build identifier to cancel."`
}

// MCPCancelBuildOutput defines output for cancel_build tool.
type MCPCancelBuildOutput struct {
	Status  string `json:"status"`
	BuildID string `json:"build_id"`
}

// MCPGetBuildArgs defines arguments for get_build tool.
type MCPGetBuildArgs struct {
	BuildID  string `json:"build_id,omitempty"  jsonschema:"Vanadis build identifier (e.g. 'build-1793405777-309e5517')."`
	ThreadID string `json:"thread_id,omitempty" jsonschema:"Discord thread identifier or snowflake to inspect."`
}

// MCPGetBuildOutput defines output for get_build tool.
type MCPGetBuildOutput struct {
	ID              string          `json:"id"`
	ThreadID        string          `json:"thread_id"`
	Tier            int             `json:"tier"`
	Title           string          `json:"title"`
	Objective       string          `json:"objective"`
	SharedContext   string          `json:"shared_context"`
	Status          string          `json:"status"`
	QueueState      string          `json:"queue_state"`
	TotalChunks     int             `json:"total_chunks"`
	CompletedChunks int             `json:"completed_chunks"`
	RunningChunks   int             `json:"running_chunks"`
	PendingChunks   int             `json:"pending_chunks"`
	FailedChunks    int             `json:"failed_chunks"`
	Chunks          []MCPChunkBrief `json:"chunks"`
}

// MCPGetChunkArgs defines arguments for get_chunk tool.
type MCPGetChunkArgs struct {
	ChunkID  string `json:"chunk_id,omitempty"  jsonschema:"Unique chunk identifier to inspect directly."`
	BuildID  string `json:"build_id,omitempty"  jsonschema:"Parent Vanadis build identifier when resolving via chunk_ref."`
	ChunkRef string `json:"chunk_ref,omitempty" jsonschema:"Chunk identifier or 0-based chunk_index within the parent build."`
}

// MCPGetChunkOutput defines output for get_chunk tool.
type MCPGetChunkOutput struct {
	BuildID     string             `json:"build_id"`
	BuildStatus string             `json:"build_status"`
	Chunk       model.VanadisChunk `json:"chunk"`
}

// MCPUpdateBuildArgs defines arguments for update_build tool.
type MCPUpdateBuildArgs struct {
	BuildID       string `json:"build_id"                 jsonschema:"Vanadis build identifier to mutate."`
	Title         string `json:"title,omitempty"          jsonschema:"New build title replacing existing title."`
	Objective     string `json:"objective,omitempty"      jsonschema:"New build objective replacing existing objective."`
	SharedContext string `json:"shared_context,omitempty" jsonschema:"New shared build context replacing existing context."`
}

// MCPUpdateBuildOutput defines output for update_build tool.
type MCPUpdateBuildOutput struct {
	Status string              `json:"status"`
	Build  *model.VanadisBuild `json:"build"`
}

// MCPUpdateChunkArgs defines arguments for update_chunk tool.
type MCPUpdateChunkArgs struct {
	BuildID       string    `json:"build_id"                 jsonschema:"Parent Vanadis build identifier containing the chunk."`
	ChunkRef      string    `json:"chunk_ref"                jsonschema:"Chunk identifier or 0-based chunk_index of the chunk to mutate."`
	Mode          *string   `json:"mode,omitempty"           jsonschema:"Optional new mode for the chunk ('pr' or 'action')."`
	Repo          string    `json:"repo,omitempty"           jsonschema:"Target repository for the chunk."`
	Title         string    `json:"title,omitempty"          jsonschema:"New title for the chunk."`
	Instructions  string    `json:"instructions,omitempty"   jsonschema:"New instructions for the chunk."`
	TargetFiles   []string  `json:"target_files,omitempty"   jsonschema:"Target files to inspect or modify."`
	TargetSymbols []string  `json:"target_symbols,omitempty" jsonschema:"Target symbols to inspect or modify."`
	Effort        string    `json:"effort,omitempty"         jsonschema:"Effort level ('low', 'medium', 'high')."`
	DependsOn     *[]string `json:"depends_on,omitempty"     jsonschema:"Optional replacement dependency chunk IDs or indices."`
}

// MCPUpdateChunkOutput defines output for update_chunk tool.
type MCPUpdateChunkOutput struct {
	Status string              `json:"status"`
	Chunk  *model.VanadisChunk `json:"chunk"`
}

// MCPCreateChunkItem defines a single chunk input for MCP chunk operations.
type MCPCreateChunkItem struct {
	ID                 string   `json:"id,omitempty"                  jsonschema:"Optional unique chunk identifier (1..120 chars)."`
	Mode               string   `json:"mode,omitempty"                jsonschema:"Chunk execution mode ('pr' or 'action')."`
	Repo               string   `json:"repo,omitempty"                jsonschema:"Optional target repository override."`
	Title              string   `json:"title"                         jsonschema:"Chunk title."`
	Instructions       string   `json:"instructions"                  jsonschema:"Chunk instructions."`
	TargetFiles        []string `json:"target_files,omitempty"        jsonschema:"Target files for this chunk."`
	TargetSymbols      []string `json:"target_symbols,omitempty"      jsonschema:"Target symbols for this chunk."`
	Effort             string   `json:"effort,omitempty"              jsonschema:"Effort level ('low', 'medium', 'high')."`
	AcceptanceCriteria []string `json:"acceptance_criteria,omitempty" jsonschema:"Acceptance criteria checklist."`
	DependsOn          []string `json:"depends_on"                    jsonschema:"Explicit list of chunk IDs or 0-based indices this chunk depends on ([] for root chunks)."`
}

// MCPAppendChunkArgs defines arguments for append_chunk tool.
type MCPAppendChunkArgs struct {
	BuildID string               `json:"build_id" jsonschema:"Vanadis build identifier to append chunks to."`
	Chunks  []MCPCreateChunkItem `json:"chunks"   jsonschema:"List of new pending chunks to append."`
}

// MCPAppendChunkOutput defines output for append_chunk tool.
type MCPAppendChunkOutput struct {
	Status  string               `json:"status"`
	BuildID string               `json:"build_id"`
	Chunks  []model.VanadisChunk `json:"chunks"`
}

// MCPRemoveChunkArgs defines arguments for remove_chunk tool.
type MCPRemoveChunkArgs struct {
	BuildID  string `json:"build_id"  jsonschema:"Parent Vanadis build identifier containing the chunk."`
	ChunkRef string `json:"chunk_ref" jsonschema:"Chunk identifier or 0-based chunk_index to remove."`
}

// MCPRemoveChunkOutput defines output for remove_chunk tool.
type MCPRemoveChunkOutput struct {
	Status         string          `json:"status"`
	BuildID        string          `json:"build_id"`
	RemovedChunkID string          `json:"removed_chunk_id"`
	Chunks         []MCPChunkBrief `json:"chunks"`
}

// MCPFailChunkArgs defines arguments for fail_chunk tool.
type MCPFailChunkArgs struct {
	DispatchID string `json:"dispatch_id,omitempty" jsonschema:"Current action dispatch identity"`
	ChunkID    string `json:"chunk_id"              jsonschema:"Chunk ID to mark as failed"`
	Reason     string `json:"reason"                jsonschema:"Detailed explanation of the unrecoverable blocker"`
}

// MCPFailChunkOutput defines output for fail_chunk tool.
type MCPFailChunkOutput struct {
	Status  string `json:"status"`
	BuildID string `json:"build_id"`
	ChunkID string `json:"chunk_id"`
	Failed  bool   `json:"failed"`
}

// MCPCompleteChunkArgs defines arguments for complete_chunk tool.
type MCPCompleteChunkArgs struct {
	ChunkID    string `json:"chunk_id"    jsonschema:"Action chunk ID to complete"`
	DispatchID string `json:"dispatch_id" jsonschema:"Current action dispatch identity"`
	Summary    string `json:"summary"     jsonschema:"Summary of the PASS evidence"`
}

// MCPCompleteChunkOutput defines output for complete_chunk tool.
type MCPCompleteChunkOutput struct {
	Status    string `json:"status"`
	BuildID   string `json:"build_id"`
	ChunkID   string `json:"chunk_id"`
	Completed bool   `json:"completed"`
}

// completeChunkPayload mirrors the Restate CompleteChunk ingress body.
type completeChunkPayload = buildplan.CompleteChunkRequest

// completeChunkResponse mirrors the Restate CompleteChunk ingress response.
type completeChunkResponse = buildplan.CompleteChunkResponse

// updateBuildPayload mirrors the Restate UpdateBuild ingress body.
type updateBuildPayload struct {
	Title         string `json:"title,omitempty"`
	Objective     string `json:"objective,omitempty"`
	SharedContext string `json:"shared_context,omitempty"`
}

// updateChunkPayload mirrors the Restate UpdateChunk ingress body.
type updateChunkPayload = buildplan.UpdateChunkRequest

// appendChunksPayload mirrors the Restate AppendChunks ingress body.
type appendChunksPayload struct {
	Chunks []model.CreateChunkInput `json:"chunks"`
}

// removeChunkPayload mirrors the Restate RemoveChunk ingress body.
type removeChunkPayload struct {
	ChunkRef string `json:"chunk_ref"`
}

// failChunkPayload mirrors the Restate RouteFailChunk ingress body.
type failChunkPayload = buildplan.FailChunkRequest

// failChunkResponse mirrors the Restate RouteFailChunk ingress response.
type failChunkResponse = buildplan.FailChunkResponse

// ToMCPChunkBriefs projects full chunks into concise MCP listing briefs.
func ToMCPChunkBriefs(chunks []model.VanadisChunk) []MCPChunkBrief {
	if chunks == nil {
		return []MCPChunkBrief{}
	}
	briefs := make([]MCPChunkBrief, len(chunks))
	for i, c := range chunks {
		briefs[i] = MCPChunkBrief{
			ID:         c.ID,
			ChunkIndex: c.ChunkIndex,
			Title:      c.Title,
			Mode:       c.Mode,
			Status:     c.Status,
			Repo:       c.Repo,
			PRNumber:   c.PRNumber,
			DependsOn:  buildplan.NonNilSlice(c.DependsOn),
		}
	}
	return briefs
}

// Handler serves the Vanadis MCP tool surface over Streamable HTTP and the
// retry-action HTTP route. Construct with NewHandler.
type Handler struct {
	Store   store.VanadisStore
	Restate *restate.Client
	// RestateEnabled gates durable Restate mutations; when false every
	// Restate-backed call reports ErrRetryIngressUnavailable-style failure
	// through the same paths as a missing ingress URL.
	RestateEnabled bool
	Auth           *k8sauth.Authenticator
}

// NewHandler constructs a Handler backed by the given store and Restate client.
func NewHandler(st store.VanadisStore, rc *restate.Client, restateEnabled bool, auth ...*k8sauth.Authenticator) *Handler {
	var a *k8sauth.Authenticator
	if len(auth) > 0 {
		a = auth[0]
	}
	return &Handler{Store: st, Restate: rc, RestateEnabled: restateEnabled, Auth: a}
}

func (h *Handler) authEnabled() bool {
	return h != nil && h.Auth != nil && h.Auth.Enabled()
}

// NewHTTPHandler constructs and returns the Streamable HTTP MCP server handler.
func (h *Handler) NewHTTPHandler() http.HandlerFunc {
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    serviceName,
		Version: "1.0.0",
	}, &mcp.ServerOptions{Instructions: ServerInstructions})

	h.RegisterTools(mcpServer)

	opts := &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	}

	streamHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, opts)

	return func(w http.ResponseWriter, r *http.Request) {
		httpkit.ServeMCPStream(w, r, streamHandler, "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n", "vanadis_", "")
	}
}

func (h *Handler) resolveTargetBuildID(ctx context.Context, buildID, threadID string) (string, error) {
	if buildID != "" {
		return buildID, nil
	}
	builds, err := h.Store.ListBuilds(ctx, 50)
	if err != nil {
		return "", err
	}
	var latestRunning *model.VanadisBuildEntry
	var latestAny *model.VanadisBuildEntry
	for i := range builds {
		b := &builds[i]
		if b.ThreadID == threadID {
			if latestAny == nil {
				latestAny = b
			}
			if b.Status == statusRunning {
				latestRunning = b
				break
			}
		}
	}
	chosen := latestRunning
	if chosen == nil {
		chosen = latestAny
	}
	if chosen == nil {
		return "", fmt.Errorf("%w: no build found for thread %s", model.ErrBuildNotFound, threadID)
	}
	return chosen.ID, nil
}

func (h *Handler) handleMCPGetBuild(ctx context.Context, args MCPGetBuildArgs) (MCPGetBuildOutput, error) {
	if h.Store == nil {
		return MCPGetBuildOutput{}, errors.New("vanadis store not configured")
	}
	if h.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(ctx)
		if !ok {
			return MCPGetBuildOutput{}, errors.New("unauthorized: missing caller identity")
		}
		if err := h.Auth.Authorize(ctx, caller, "builds", "get", "*"); err != nil {
			return MCPGetBuildOutput{}, err
		}
	}
	buildID := strings.TrimSpace(args.BuildID)
	threadID := discordutil.NormalizeThreadID(args.ThreadID)
	if buildID == "" && threadID == "" {
		return MCPGetBuildOutput{}, fmt.Errorf("%w: build_id or thread_id is required", model.ErrInvalidBuildInput)
	}

	targetBuildID, err := h.resolveTargetBuildID(ctx, buildID, threadID)
	if err != nil {
		return MCPGetBuildOutput{}, err
	}

	build, chunks, err := h.Store.GetBuild(ctx, targetBuildID)
	if err != nil {
		return MCPGetBuildOutput{}, err
	}

	entry := model.SummarizeBuildEntry(*build, chunks)
	return MCPGetBuildOutput{
		ID:              entry.ID,
		ThreadID:        entry.ThreadID,
		Tier:            entry.Tier,
		Title:           entry.Title,
		Objective:       entry.Objective,
		SharedContext:   entry.SharedContext,
		Status:          entry.Status,
		QueueState:      entry.QueueState,
		TotalChunks:     entry.TotalChunks,
		CompletedChunks: entry.CompletedChunks,
		RunningChunks:   entry.RunningChunks,
		PendingChunks:   entry.PendingChunks,
		FailedChunks:    entry.FailedChunks,
		Chunks:          ToMCPChunkBriefs(chunks),
	}, nil
}

func (h *Handler) handleMCPGetChunk(ctx context.Context, args MCPGetChunkArgs) (MCPGetChunkOutput, error) {
	if h.Store == nil {
		return MCPGetChunkOutput{}, errors.New("vanadis store not configured")
	}
	if h.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(ctx)
		if !ok {
			return MCPGetChunkOutput{}, errors.New("unauthorized: missing caller identity")
		}
		if err := h.Auth.Authorize(ctx, caller, "builds", "get", "*"); err != nil {
			return MCPGetChunkOutput{}, err
		}
	}
	chunkID := strings.TrimSpace(args.ChunkID)
	if chunkID != "" {
		chunk, build, err := h.Store.GetChunk(ctx, chunkID)
		if err != nil {
			return MCPGetChunkOutput{}, err
		}
		buildStatus := ""
		if build != nil {
			buildStatus = build.Status
		}
		return MCPGetChunkOutput{
			BuildID:     chunk.BuildID,
			BuildStatus: buildStatus,
			Chunk:       *chunk,
		}, nil
	}

	buildID := strings.TrimSpace(args.BuildID)
	chunkRef := strings.TrimSpace(args.ChunkRef)
	if buildID == "" || chunkRef == "" {
		return MCPGetChunkOutput{}, fmt.Errorf("%w: chunk_id or both build_id and chunk_ref are required", model.ErrInvalidBuildInput)
	}

	build, chunks, err := h.Store.GetBuild(ctx, buildID)
	if err != nil {
		return MCPGetChunkOutput{}, err
	}
	idx := buildplan.ResolveChunkRef(chunks, chunkRef)
	if idx < 0 {
		return MCPGetChunkOutput{}, fmt.Errorf("%w: chunk ref %s not found in build %s", model.ErrChunkNotFound, chunkRef, buildID)
	}

	return MCPGetChunkOutput{
		BuildID:     build.ID,
		BuildStatus: build.Status,
		Chunk:       chunks[idx],
	}, nil
}

func (h *Handler) requireStoreAndBuildID(buildID string) (string, error) {
	if h.Store == nil {
		return "", errors.New("vanadis store not configured")
	}
	trimmed := strings.TrimSpace(buildID)
	if trimmed == "" {
		return "", fmt.Errorf("%w: build_id is required", model.ErrInvalidBuildInput)
	}
	return trimmed, nil
}

// authorizeBuildMutation loads the build, authorizes the caller to update it,
// and returns the Restate Build service name.
func (h *Handler) authorizeBuildMutation(ctx context.Context, buildID string) (string, error) {
	var caller k8sauth.CallerIdentity
	if h.authEnabled() {
		var ok bool
		if caller, ok = k8sauth.CallerFromContext(ctx); !ok {
			return "", errors.New("unauthorized: missing caller identity")
		}
	}
	if h == nil || h.Store == nil {
		return "", errors.New("vanadis store not configured")
	}
	targetBuild, _, err := h.Store.GetBuild(ctx, buildID)
	if err != nil {
		return "", err
	}
	if targetBuild == nil {
		return "", model.ErrBuildNotFound
	}
	if h.authEnabled() {
		agent := targetBuild.Agent
		if agent == "" {
			agent = defaultAgent
		}
		if err := h.Auth.Authorize(ctx, caller, "builds", "update", agent); err != nil {
			return "", err
		}
	}
	return buildplan.BuildServiceName(""), nil
}

func (h *Handler) handleMCPUpdateBuild(ctx context.Context, args MCPUpdateBuildArgs) (MCPUpdateBuildOutput, error) {
	buildID, err := h.requireStoreAndBuildID(args.BuildID)
	if err != nil {
		return MCPUpdateBuildOutput{}, err
	}
	buildService, err := h.authorizeBuildMutation(ctx, buildID)
	if err != nil {
		return MCPUpdateBuildOutput{}, err
	}

	var updated model.VanadisBuild
	if err := h.Restate.CallIngress(ctx, h.Restate.ObjectPath(buildService, buildID, "UpdateBuild"), updateBuildPayload{
		Title:         args.Title,
		Objective:     args.Objective,
		SharedContext: args.SharedContext,
	}, &updated); err != nil {
		return MCPUpdateBuildOutput{}, err
	}

	return MCPUpdateBuildOutput{
		Status: statusUpdated,
		Build:  &updated,
	}, nil
}

func (h *Handler) requireStoreAndChunkRef(buildID, chunkRef string) (string, string, error) {
	if h.Store == nil {
		return "", "", errors.New("vanadis store not configured")
	}
	trimmedBuildID := strings.TrimSpace(buildID)
	trimmedChunkRef := strings.TrimSpace(chunkRef)
	if trimmedBuildID == "" || trimmedChunkRef == "" {
		return "", "", fmt.Errorf("%w: build_id and chunk_ref are required", model.ErrInvalidBuildInput)
	}
	return trimmedBuildID, trimmedChunkRef, nil
}

func (h *Handler) handleMCPUpdateChunk(ctx context.Context, args MCPUpdateChunkArgs) (MCPUpdateChunkOutput, error) {
	buildID, chunkRef, err := h.requireStoreAndChunkRef(args.BuildID, args.ChunkRef)
	if err != nil {
		return MCPUpdateChunkOutput{}, err
	}
	buildService, err := h.authorizeBuildMutation(ctx, buildID)
	if err != nil {
		return MCPUpdateChunkOutput{}, err
	}

	var modePtr *string
	if args.Mode != nil && strings.TrimSpace(*args.Mode) != "" {
		trimmed := strings.TrimSpace(*args.Mode)
		modePtr = &trimmed
	}

	req := updateChunkPayload{
		ChunkRef:      chunkRef,
		Mode:          modePtr,
		Repo:          args.Repo,
		Title:         args.Title,
		Instructions:  args.Instructions,
		TargetFiles:   args.TargetFiles,
		TargetSymbols: args.TargetSymbols,
		Effort:        args.Effort,
		DependsOn:     args.DependsOn,
	}

	var updated model.VanadisChunk
	if err := h.Restate.CallIngress(ctx, h.Restate.ObjectPath(buildService, buildID, "UpdateChunk"), req, &updated); err != nil {
		return MCPUpdateChunkOutput{}, err
	}

	return MCPUpdateChunkOutput{
		Status: statusUpdated,
		Chunk:  &updated,
	}, nil
}

func (h *Handler) handleMCPAppendChunk(ctx context.Context, args MCPAppendChunkArgs) (MCPAppendChunkOutput, error) {
	buildID, err := h.requireStoreAndBuildID(args.BuildID)
	if err != nil {
		return MCPAppendChunkOutput{}, err
	}
	buildService, err := h.authorizeBuildMutation(ctx, buildID)
	if err != nil {
		return MCPAppendChunkOutput{}, err
	}
	if len(args.Chunks) == 0 {
		return MCPAppendChunkOutput{}, fmt.Errorf("%w: at least one chunk is required", model.ErrInvalidBuildInput)
	}

	inputs := make([]model.CreateChunkInput, len(args.Chunks))
	for i, c := range args.Chunks {
		inputs[i] = model.CreateChunkInput{
			ID:                 c.ID,
			Mode:               c.Mode,
			Repo:               c.Repo,
			Title:              c.Title,
			Instructions:       c.Instructions,
			TargetFiles:        c.TargetFiles,
			TargetSymbols:      c.TargetSymbols,
			Effort:             c.Effort,
			AcceptanceCriteria: c.AcceptanceCriteria,
			DependsOn:          c.DependsOn,
		}
	}

	var appended []model.VanadisChunk
	if err := h.Restate.CallIngress(ctx, h.Restate.ObjectPath(buildService, buildID, "AppendChunks"), appendChunksPayload{Chunks: inputs}, &appended); err != nil {
		return MCPAppendChunkOutput{}, err
	}

	return MCPAppendChunkOutput{
		Status:  "appended",
		BuildID: buildID,
		Chunks:  appended,
	}, nil
}

func (h *Handler) handleMCPRemoveChunk(ctx context.Context, args MCPRemoveChunkArgs) (MCPRemoveChunkOutput, error) {
	buildID, chunkRef, err := h.requireStoreAndChunkRef(args.BuildID, args.ChunkRef)
	if err != nil {
		return MCPRemoveChunkOutput{}, err
	}
	buildService, err := h.authorizeBuildMutation(ctx, buildID)
	if err != nil {
		return MCPRemoveChunkOutput{}, err
	}

	removedID := chunkRef
	if _, chs, err := h.Store.GetBuild(ctx, buildID); err == nil {
		if idx := buildplan.ResolveChunkRef(chs, chunkRef); idx >= 0 {
			removedID = chs[idx].ID
		}
	}

	req := removeChunkPayload{ChunkRef: chunkRef}
	if err := h.Restate.CallIngress(ctx, h.Restate.ObjectPath(buildService, buildID, "RemoveChunk"), req, nil); err != nil {
		return MCPRemoveChunkOutput{}, err
	}

	var remaining []model.VanadisChunk
	if h.Store != nil {
		_, chs, err := h.Store.GetBuild(ctx, buildID)
		if err == nil {
			remaining = chs
		}
	}

	return MCPRemoveChunkOutput{
		Status:         "removed",
		BuildID:        buildID,
		RemovedChunkID: removedID,
		Chunks:         ToMCPChunkBriefs(remaining),
	}, nil
}

func (h *Handler) handleMCPFailChunk(ctx context.Context, args MCPFailChunkArgs) (MCPFailChunkOutput, error) {
	if h.Store == nil {
		return MCPFailChunkOutput{}, errors.New("vanadis store not configured")
	}
	chunkID := strings.TrimSpace(args.ChunkID)
	reason := strings.TrimSpace(args.Reason)
	if chunkID == "" || reason == "" {
		return MCPFailChunkOutput{}, fmt.Errorf("%w: chunk_id and reason are required", model.ErrInvalidBuildInput)
	}
	if h.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(ctx)
		if !ok {
			return MCPFailChunkOutput{}, errors.New("unauthorized: missing caller identity")
		}
		_, targetBuild, err := h.Store.GetChunk(ctx, chunkID)
		if err != nil {
			return MCPFailChunkOutput{}, err
		}
		agent := defaultAgent
		if targetBuild != nil && targetBuild.Agent != "" {
			agent = targetBuild.Agent
		}
		if err := h.Auth.Authorize(ctx, caller, "builds", "update", agent); err != nil {
			return MCPFailChunkOutput{}, err
		}
	}
	req := failChunkPayload{
		DispatchID: args.DispatchID,
		ChunkID:    chunkID,
		Reason:     reason,
	}
	var resp failChunkResponse
	if err := h.Restate.CallIngress(ctx, h.Restate.RouterPath("RouteFailChunk"), req, &resp); err != nil {
		return MCPFailChunkOutput{}, err
	}

	return MCPFailChunkOutput{
		Status:  statusFailed,
		BuildID: resp.BuildID,
		ChunkID: resp.ChunkID,
		Failed:  resp.Failed,
	}, nil
}

func (h *Handler) handleMCPCompleteChunk(ctx context.Context, args MCPCompleteChunkArgs) (MCPCompleteChunkOutput, error) {
	if h.Store == nil {
		return MCPCompleteChunkOutput{}, errors.New("vanadis store not configured")
	}
	chunkID := strings.TrimSpace(args.ChunkID)
	dispatchID := strings.TrimSpace(args.DispatchID)
	summary := strings.TrimSpace(args.Summary)
	if chunkID == "" || dispatchID == "" || summary == "" {
		return MCPCompleteChunkOutput{}, fmt.Errorf("%w: chunk_id, dispatch_id and summary are required", model.ErrInvalidBuildInput)
	}
	var caller k8sauth.CallerIdentity
	if h.authEnabled() {
		var ok bool
		if caller, ok = k8sauth.CallerFromContext(ctx); !ok {
			return MCPCompleteChunkOutput{}, errors.New("unauthorized: missing caller identity")
		}
	}
	_, targetBuild, err := h.Store.GetChunk(ctx, chunkID)
	if err != nil {
		return MCPCompleteChunkOutput{}, err
	}
	if targetBuild == nil {
		return MCPCompleteChunkOutput{}, model.ErrBuildNotFound
	}
	if h.authEnabled() {
		agent := defaultAgent
		if targetBuild.Agent != "" {
			agent = targetBuild.Agent
		}
		if err := h.Auth.Authorize(ctx, caller, "builds", "update", agent); err != nil {
			return MCPCompleteChunkOutput{}, err
		}
	}
	req := completeChunkPayload{BuildID: targetBuild.ID, ChunkID: chunkID, DispatchID: dispatchID, Summary: summary}
	path := h.Restate.ObjectPath(buildplan.BuildServiceName(""), targetBuild.ID, "CompleteChunk")
	var resp completeChunkResponse
	if err := h.Restate.CallIngressKeyed(ctx, path, "complete:"+dispatchID, req, &resp); err != nil {
		return MCPCompleteChunkOutput{}, err
	}
	return MCPCompleteChunkOutput(resp), nil
}

func (h *Handler) handleMCPCreateBuild(ctx context.Context, args MCPCreateBuildArgs) (MCPCreateBuildOutput, error) {
	if h.Store == nil {
		return MCPCreateBuildOutput{}, errors.New("vanadis store not configured")
	}
	threadID := discordutil.NormalizeThreadID(args.ThreadID)
	objective := strings.TrimSpace(args.Objective)
	if !discordutil.IsSnowflake(threadID) || objective == "" || len(args.Chunks) == 0 {
		return MCPCreateBuildOutput{}, fmt.Errorf("%w: valid thread_id, objective, and chunks are required", model.ErrInvalidBuildInput)
	}

	targetAgent := defaultAgent
	if h.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(ctx)
		if !ok {
			return MCPCreateBuildOutput{}, errors.New("unauthorized: missing caller identity")
		}
		resolved, err := h.Auth.AuthorizeTarget(ctx, caller, "builds", "create", args.Agent)
		if err != nil {
			return MCPCreateBuildOutput{}, err
		}
		targetAgent = resolved
	} else if a := strings.TrimSpace(args.Agent); a != "" {
		targetAgent = a
	}

	inputs := make([]model.CreateChunkInput, len(args.Chunks))
	for i, c := range args.Chunks {
		inputs[i] = model.CreateChunkInput{
			ID:                 c.ID,
			Mode:               c.Mode,
			Repo:               c.Repo,
			Title:              c.Title,
			Instructions:       c.Instructions,
			TargetFiles:        c.TargetFiles,
			TargetSymbols:      c.TargetSymbols,
			Effort:             c.Effort,
			AcceptanceCriteria: c.AcceptanceCriteria,
			DependsOn:          c.DependsOn,
		}
	}

	buildID := strutil.FirstNonEmpty(strings.TrimSpace(args.ID), buildplan.GenerateBuildID())
	tier := args.Tier
	if tier <= 0 {
		tier = 2
	}
	title := strutil.FirstNonEmpty(strings.TrimSpace(args.Title), objective)

	chunks, err := model.ParseCreateBuildChunks(buildID, time.Now().UTC(), inputs)
	if err != nil {
		return MCPCreateBuildOutput{}, fmt.Errorf("%w: %w", model.ErrInvalidBuildInput, err)
	}

	req := model.CreateBuildRequest{
		ID:            buildID,
		Agent:         targetAgent,
		ThreadID:      threadID,
		Tier:          tier,
		Title:         title,
		Objective:     objective,
		SharedContext: args.SharedContext,
		Chunks:        inputs,
	}

	var created model.VanadisBuild
	path := h.Restate.ObjectPath(buildplan.BuildServiceName(""), buildID, "Create")
	if err := h.Restate.CallIngress(ctx, path, req, &created); err != nil {
		return MCPCreateBuildOutput{}, err
	}

	outTier := tier
	if created.Tier > 0 {
		outTier = created.Tier
	}

	return MCPCreateBuildOutput{
		Status:       "accepted",
		BuildID:      strutil.FirstNonEmpty(created.ID, buildID),
		ThreadID:     strutil.FirstNonEmpty(created.ThreadID, threadID),
		Tier:         outTier,
		ChunkCount:   len(chunks),
		FirstChunkID: chunks[0].ID,
	}, nil
}

func (h *Handler) handleMCPCancelBuild(ctx context.Context, args MCPCancelBuildArgs) (MCPCancelBuildOutput, error) {
	buildID, err := h.requireStoreAndBuildID(args.BuildID)
	if err != nil {
		return MCPCancelBuildOutput{}, err
	}
	buildService, err := h.authorizeBuildMutation(ctx, buildID)
	if err != nil {
		return MCPCancelBuildOutput{}, err
	}

	var cancelled model.VanadisBuild
	if err := h.Restate.CallIngress(ctx, h.Restate.ObjectPath(buildService, buildID, "Cancel"), struct{}{}, &cancelled); err != nil {
		return MCPCancelBuildOutput{}, err
	}

	return MCPCancelBuildOutput{
		Status:  "cancelled",
		BuildID: buildID,
	}, nil
}

// RegisterTools registers every Vanadis MCP tool on the given server.
func (h *Handler) RegisterTools(mcpServer *mcp.Server) {
	h.registerRetryActionTool(mcpServer)
	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "create_build",
		Description: "Create and enqueue a new multi-chunk Vanadis build DAG.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPCreateBuildArgs) (*mcp.CallToolResult, MCPCreateBuildOutput, error) {
		out, err := h.handleMCPCreateBuild(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "cancel_build",
		Description: "Cancel an active or running Vanadis build.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPCancelBuildArgs) (*mcp.CallToolResult, MCPCancelBuildOutput, error) {
		out, err := h.handleMCPCancelBuild(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "get_build",
		Description: "Get concise build metadata, queue status, and chunk summaries by build ID or Discord thread ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPGetBuildArgs) (*mcp.CallToolResult, MCPGetBuildOutput, error) {
		out, err := h.handleMCPGetBuild(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "get_chunk",
		Description: "Get detailed chunk instructions and target references by chunk ID or build ID + chunk ref.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPGetChunkArgs) (*mcp.CallToolResult, MCPGetChunkOutput, error) {
		out, err := h.handleMCPGetChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "update_build",
		Description: "Update top-level metadata (title, objective, shared_context) of a running Vanadis build.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPUpdateBuildArgs) (*mcp.CallToolResult, MCPUpdateBuildOutput, error) {
		out, err := h.handleMCPUpdateBuild(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "update_chunk",
		Description: "Mutate definition fields of a pending Vanadis chunk before it executes.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPUpdateChunkArgs) (*mcp.CallToolResult, MCPUpdateChunkOutput, error) {
		out, err := h.handleMCPUpdateChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "append_chunk",
		Description: "Append new pending chunks to the end of a running Vanadis build.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPAppendChunkArgs) (*mcp.CallToolResult, MCPAppendChunkOutput, error) {
		out, err := h.handleMCPAppendChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "remove_chunk",
		Description: "Remove a pending chunk from a running Vanadis build and re-index remaining chunks.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPRemoveChunkArgs) (*mcp.CallToolResult, MCPRemoveChunkOutput, error) {
		out, err := h.handleMCPRemoveChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "fail_chunk",
		Description: "Fail an action chunk. Only its dependents are blocked; other chunks keep running, and retry_action_chunk repairs it in place. Use for blockers, not for fixable test, lint or CI failures.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPFailChunkArgs) (*mcp.CallToolResult, MCPFailChunkOutput, error) {
		out, err := h.handleMCPFailChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "complete_chunk",
		Description: "Complete an action chunk with an explicit verdict. An action chunk passes only by calling complete_chunk with its dispatch_id and a summary of the PASS evidence; a turn that ends without it is resumed automatically, then failed (blocking only its dependents).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPCompleteChunkArgs) (*mcp.CallToolResult, MCPCompleteChunkOutput, error) {
		out, err := h.handleMCPCompleteChunk(ctx, args)
		return nil, out, err
	})
}
