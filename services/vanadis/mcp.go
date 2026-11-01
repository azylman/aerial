package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCPChunkBrief captures the concise chunk fields exposed in MCP build listings.
type MCPChunkBrief struct {
	ID         string `json:"id"`
	ChunkIndex int    `json:"chunk_index"`
	Title      string `json:"title"`
	Status     string `json:"status"`
	Repo       string `json:"repo"`
	PRNumber   int    `json:"pr_number"`
}

// MCPGetBuildArgs defines arguments for get_build tool.
type MCPGetBuildArgs struct {
	BuildID  string `json:"build_id,omitempty" jsonschema:"Vanadis build identifier (e.g. 'build-1793405777-309e5517')."`
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
	ChunkID  string `json:"chunk_id,omitempty" jsonschema:"Unique chunk identifier to inspect directly."`
	BuildID  string `json:"build_id,omitempty" jsonschema:"Parent Vanadis build identifier when resolving via chunk_ref."`
	ChunkRef string `json:"chunk_ref,omitempty" jsonschema:"Chunk identifier or 0-based chunk_index within the parent build."`
}

// MCPGetChunkOutput defines output for get_chunk tool.
type MCPGetChunkOutput struct {
	BuildID     string       `json:"build_id"`
	BuildStatus string       `json:"build_status"`
	Chunk       VanadisChunk `json:"chunk"`
}

// MCPUpdateBuildArgs defines arguments for update_build tool.
type MCPUpdateBuildArgs struct {
	BuildID       string `json:"build_id" jsonschema:"Vanadis build identifier to mutate."`
	Title         string `json:"title,omitempty" jsonschema:"New build title replacing existing title."`
	Objective     string `json:"objective,omitempty" jsonschema:"New build objective replacing existing objective."`
	SharedContext string `json:"shared_context,omitempty" jsonschema:"New shared build context replacing existing context."`
}

// MCPUpdateBuildOutput defines output for update_build tool.
type MCPUpdateBuildOutput struct {
	Status string        `json:"status"`
	Build  *VanadisBuild `json:"build"`
}

// MCPUpdateChunkArgs defines arguments for update_chunk tool.
type MCPUpdateChunkArgs struct {
	BuildID       string   `json:"build_id" jsonschema:"Parent Vanadis build identifier containing the chunk."`
	ChunkRef      string   `json:"chunk_ref" jsonschema:"Chunk identifier or 0-based chunk_index of the chunk to mutate."`
	Repo          string   `json:"repo,omitempty" jsonschema:"Target repository for the chunk."`
	Title         string   `json:"title,omitempty" jsonschema:"New title for the chunk."`
	Instructions  string   `json:"instructions,omitempty" jsonschema:"New instructions for the chunk."`
	TargetFiles   []string `json:"target_files,omitempty" jsonschema:"Target files to inspect or modify."`
	TargetSymbols []string `json:"target_symbols,omitempty" jsonschema:"Target symbols to inspect or modify."`
	Effort        string   `json:"effort,omitempty" jsonschema:"Effort level ('low', 'medium', 'high')."`
}

// MCPUpdateChunkOutput defines output for update_chunk tool.
type MCPUpdateChunkOutput struct {
	Status string        `json:"status"`
	Chunk  *VanadisChunk `json:"chunk"`
}

// MCPCreateChunkItem defines a single chunk input for MCP chunk operations.
type MCPCreateChunkItem struct {
	ID                 string   `json:"id,omitempty" jsonschema:"Optional unique chunk identifier (1..120 chars)."`
	Repo               string   `json:"repo,omitempty" jsonschema:"Optional target repository override."`
	Title              string   `json:"title" jsonschema:"Chunk title."`
	Instructions       string   `json:"instructions" jsonschema:"Chunk instructions."`
	TargetFiles        []string `json:"target_files,omitempty" jsonschema:"Target files for this chunk."`
	TargetSymbols      []string `json:"target_symbols,omitempty" jsonschema:"Target symbols for this chunk."`
	Effort             string   `json:"effort,omitempty" jsonschema:"Effort level ('low', 'medium', 'high')."`
	AcceptanceCriteria []string `json:"acceptance_criteria,omitempty" jsonschema:"Acceptance criteria checklist."`
}

// MCPAppendChunkArgs defines arguments for append_chunk tool.
type MCPAppendChunkArgs struct {
	BuildID string               `json:"build_id" jsonschema:"Vanadis build identifier to append chunks to."`
	Chunks  []MCPCreateChunkItem `json:"chunks" jsonschema:"List of new pending chunks to append."`
}

// MCPAppendChunkOutput defines output for append_chunk tool.
type MCPAppendChunkOutput struct {
	Status  string         `json:"status"`
	BuildID string         `json:"build_id"`
	Chunks  []VanadisChunk `json:"chunks"`
}

// MCPRemoveChunkArgs defines arguments for remove_chunk tool.
type MCPRemoveChunkArgs struct {
	BuildID  string `json:"build_id" jsonschema:"Parent Vanadis build identifier containing the chunk."`
	ChunkRef string `json:"chunk_ref" jsonschema:"Chunk identifier or 0-based chunk_index to remove."`
}

// MCPRemoveChunkOutput defines output for remove_chunk tool.
type MCPRemoveChunkOutput struct {
	Status         string          `json:"status"`
	BuildID        string          `json:"build_id"`
	RemovedChunkID string          `json:"removed_chunk_id"`
	Chunks         []MCPChunkBrief `json:"chunks"`
}

func toMCPChunkBriefs(chunks []VanadisChunk) []MCPChunkBrief {
	if chunks == nil {
		return []MCPChunkBrief{}
	}
	briefs := make([]MCPChunkBrief, len(chunks))
	for i, c := range chunks {
		briefs[i] = MCPChunkBrief{
			ID:         c.ID,
			ChunkIndex: c.ChunkIndex,
			Title:      c.Title,
			Status:     c.Status,
			Repo:       c.Repo,
			PRNumber:   c.PRNumber,
		}
	}
	return briefs
}

// newMCPHandler constructs and returns the Streamable HTTP MCP server handler.
func (s *VanadisServer) newMCPHandler() http.HandlerFunc {
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "vanadis",
		Version: "1.0.0",
	}, nil)

	s.registerMCPTools(mcpServer)

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

func (s *VanadisServer) resolveTargetBuildID(ctx context.Context, buildID, threadID string) (string, error) {
	if buildID != "" {
		return buildID, nil
	}
	builds, err := s.store.ListBuilds(ctx, 50)
	if err != nil {
		return "", err
	}
	var latestRunning *VanadisBuildEntry
	var latestAny *VanadisBuildEntry
	for i := range builds {
		b := &builds[i]
		if b.ThreadID == threadID {
			if latestAny == nil {
				latestAny = b
			}
			if b.Status == "running" {
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
		return "", fmt.Errorf("%w: no build found for thread %s", ErrBuildNotFound, threadID)
	}
	return chosen.ID, nil
}

func (s *VanadisServer) handleMCPGetBuild(ctx context.Context, args MCPGetBuildArgs) (MCPGetBuildOutput, error) {
	if s.store == nil {
		return MCPGetBuildOutput{}, errors.New("vanadis store not configured")
	}
	buildID := strings.TrimSpace(args.BuildID)
	threadID := normalizeDiscordThreadID(args.ThreadID)
	if buildID == "" && threadID == "" {
		return MCPGetBuildOutput{}, fmt.Errorf("%w: build_id or thread_id is required", ErrInvalidBuildInput)
	}

	targetBuildID, err := s.resolveTargetBuildID(ctx, buildID, threadID)
	if err != nil {
		return MCPGetBuildOutput{}, err
	}

	build, chunks, err := s.store.GetBuild(ctx, targetBuildID)
	if err != nil {
		return MCPGetBuildOutput{}, err
	}

	entry := SummarizeBuildEntry(*build, chunks)
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
		Chunks:          toMCPChunkBriefs(chunks),
	}, nil
}

func (s *VanadisServer) handleMCPGetChunk(ctx context.Context, args MCPGetChunkArgs) (MCPGetChunkOutput, error) {
	if s.store == nil {
		return MCPGetChunkOutput{}, errors.New("vanadis store not configured")
	}
	chunkID := strings.TrimSpace(args.ChunkID)
	if chunkID != "" {
		chunk, build, err := s.store.GetChunk(ctx, chunkID)
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
		return MCPGetChunkOutput{}, fmt.Errorf("%w: chunk_id or both build_id and chunk_ref are required", ErrInvalidBuildInput)
	}

	build, chunks, err := s.store.GetBuild(ctx, buildID)
	if err != nil {
		return MCPGetChunkOutput{}, err
	}
	idx := resolveChunkRef(chunks, chunkRef)
	if idx < 0 {
		return MCPGetChunkOutput{}, fmt.Errorf("%w: chunk ref %s not found in build %s", ErrChunkNotFound, chunkRef, buildID)
	}

	return MCPGetChunkOutput{
		BuildID:     build.ID,
		BuildStatus: build.Status,
		Chunk:       chunks[idx],
	}, nil
}

func (s *VanadisServer) handleMCPUpdateBuild(ctx context.Context, args MCPUpdateBuildArgs) (MCPUpdateBuildOutput, error) {
	if s.store == nil {
		return MCPUpdateBuildOutput{}, errors.New("vanadis store not configured")
	}
	buildID := strings.TrimSpace(args.BuildID)
	if buildID == "" {
		return MCPUpdateBuildOutput{}, fmt.Errorf("%w: build_id is required", ErrInvalidBuildInput)
	}

	req := UpdateBuildRequest{
		Title:         args.Title,
		Objective:     args.Objective,
		SharedContext: args.SharedContext,
	}

	updated, err := s.store.UpdateBuild(ctx, buildID, req)
	if err != nil {
		return MCPUpdateBuildOutput{}, err
	}

	if strings.TrimSpace(args.Title) != "" {
		s.syncBuildWidget(ctx, buildID, "")
	}

	return MCPUpdateBuildOutput{
		Status: "updated",
		Build:  updated,
	}, nil
}

func (s *VanadisServer) requireStoreAndChunkRef(buildID, chunkRef string) (string, string, error) {
	if s.store == nil {
		return "", "", errors.New("vanadis store not configured")
	}
	trimmedBuildID := strings.TrimSpace(buildID)
	trimmedChunkRef := strings.TrimSpace(chunkRef)
	if trimmedBuildID == "" || trimmedChunkRef == "" {
		return "", "", fmt.Errorf("%w: build_id and chunk_ref are required", ErrInvalidBuildInput)
	}
	return trimmedBuildID, trimmedChunkRef, nil
}

func (s *VanadisServer) handleMCPUpdateChunk(ctx context.Context, args MCPUpdateChunkArgs) (MCPUpdateChunkOutput, error) {
	buildID, chunkRef, err := s.requireStoreAndChunkRef(args.BuildID, args.ChunkRef)
	if err != nil {
		return MCPUpdateChunkOutput{}, err
	}

	req := UpdateChunkRequest{
		Repo:          args.Repo,
		Title:         args.Title,
		Instructions:  args.Instructions,
		TargetFiles:   args.TargetFiles,
		TargetSymbols: args.TargetSymbols,
		Effort:        args.Effort,
	}

	updated, err := s.store.UpdateChunk(ctx, buildID, chunkRef, req)
	if err != nil {
		return MCPUpdateChunkOutput{}, err
	}

	s.syncBuildWidget(ctx, buildID, "")

	return MCPUpdateChunkOutput{
		Status: "updated",
		Chunk:  updated,
	}, nil
}

func (s *VanadisServer) handleMCPAppendChunk(ctx context.Context, args MCPAppendChunkArgs) (MCPAppendChunkOutput, error) {
	if s.store == nil {
		return MCPAppendChunkOutput{}, errors.New("vanadis store not configured")
	}
	buildID := strings.TrimSpace(args.BuildID)
	if buildID == "" {
		return MCPAppendChunkOutput{}, fmt.Errorf("%w: build_id is required", ErrInvalidBuildInput)
	}

	inputs := make([]CreateChunkInput, len(args.Chunks))
	for i, c := range args.Chunks {
		inputs[i] = CreateChunkInput(c)
	}

	appended, err := s.store.AppendChunks(ctx, buildID, inputs)
	if err != nil {
		return MCPAppendChunkOutput{}, err
	}

	s.syncBuildWidget(ctx, buildID, "")

	return MCPAppendChunkOutput{
		Status:  "appended",
		BuildID: buildID,
		Chunks:  appended,
	}, nil
}

func (s *VanadisServer) handleMCPRemoveChunk(ctx context.Context, args MCPRemoveChunkArgs) (MCPRemoveChunkOutput, error) {
	buildID, chunkRef, err := s.requireStoreAndChunkRef(args.BuildID, args.ChunkRef)
	if err != nil {
		return MCPRemoveChunkOutput{}, err
	}

	removed, remaining, err := s.store.RemoveChunk(ctx, buildID, chunkRef)
	if err != nil {
		return MCPRemoveChunkOutput{}, err
	}

	s.syncBuildWidget(ctx, buildID, "")

	return MCPRemoveChunkOutput{
		Status:         "removed",
		BuildID:        buildID,
		RemovedChunkID: removed.ID,
		Chunks:         toMCPChunkBriefs(remaining),
	}, nil
}

func (s *VanadisServer) registerMCPTools(mcpServer *mcp.Server) {
	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "get_build",
		Description: "Get concise build metadata, queue status, and chunk summaries by build ID or Discord thread ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPGetBuildArgs) (*mcp.CallToolResult, MCPGetBuildOutput, error) {
		out, err := s.handleMCPGetBuild(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "get_chunk",
		Description: "Get detailed chunk instructions and target references by chunk ID or build ID + chunk ref.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPGetChunkArgs) (*mcp.CallToolResult, MCPGetChunkOutput, error) {
		out, err := s.handleMCPGetChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "update_build",
		Description: "Update top-level metadata (title, objective, shared_context) of a running Vanadis build.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPUpdateBuildArgs) (*mcp.CallToolResult, MCPUpdateBuildOutput, error) {
		out, err := s.handleMCPUpdateBuild(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "update_chunk",
		Description: "Mutate definition fields of a pending Vanadis chunk before it executes.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPUpdateChunkArgs) (*mcp.CallToolResult, MCPUpdateChunkOutput, error) {
		out, err := s.handleMCPUpdateChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "append_chunk",
		Description: "Append new pending chunks to the end of a running Vanadis build.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPAppendChunkArgs) (*mcp.CallToolResult, MCPAppendChunkOutput, error) {
		out, err := s.handleMCPAppendChunk(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "remove_chunk",
		Description: "Remove a pending chunk from a running Vanadis build and re-index remaining chunks.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MCPRemoveChunkArgs) (*mcp.CallToolResult, MCPRemoveChunkOutput, error) {
		out, err := s.handleMCPRemoveChunk(ctx, args)
		return nil, out, err
	})
}
