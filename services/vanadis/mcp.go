package main

import vanadismcp "github.com/azylman/aerial/services/vanadis/internal/mcp"

// Transitional aliases onto the canonical L2 MCP surface in internal/mcp.
// The handler logic lives in internal/mcp; this file declares zero-statement
// type, constant, and variable aliases only.
type (
	// MCPChunkBrief captures the concise chunk fields exposed in MCP build listings.
	MCPChunkBrief = vanadismcp.MCPChunkBrief
	// MCPGetBuildArgs defines arguments for get_build tool.
	MCPGetBuildArgs = vanadismcp.MCPGetBuildArgs
	// MCPGetBuildOutput defines output for get_build tool.
	MCPGetBuildOutput = vanadismcp.MCPGetBuildOutput
	// MCPGetChunkArgs defines arguments for get_chunk tool.
	MCPGetChunkArgs = vanadismcp.MCPGetChunkArgs
	// MCPGetChunkOutput defines output for get_chunk tool.
	MCPGetChunkOutput = vanadismcp.MCPGetChunkOutput
	// MCPUpdateBuildArgs defines arguments for update_build tool.
	MCPUpdateBuildArgs = vanadismcp.MCPUpdateBuildArgs
	// MCPUpdateBuildOutput defines output for update_build tool.
	MCPUpdateBuildOutput = vanadismcp.MCPUpdateBuildOutput
	// MCPUpdateChunkArgs defines arguments for update_chunk tool.
	MCPUpdateChunkArgs = vanadismcp.MCPUpdateChunkArgs
	// MCPUpdateChunkOutput defines output for update_chunk tool.
	MCPUpdateChunkOutput = vanadismcp.MCPUpdateChunkOutput
	// MCPCreateChunkItem defines a single chunk input for MCP chunk operations.
	MCPCreateChunkItem = vanadismcp.MCPCreateChunkItem
	// MCPAppendChunkArgs defines arguments for append_chunk tool.
	MCPAppendChunkArgs = vanadismcp.MCPAppendChunkArgs
	// MCPAppendChunkOutput defines output for append_chunk tool.
	MCPAppendChunkOutput = vanadismcp.MCPAppendChunkOutput
	// MCPRemoveChunkArgs defines arguments for remove_chunk tool.
	MCPRemoveChunkArgs = vanadismcp.MCPRemoveChunkArgs
	// MCPRemoveChunkOutput defines output for remove_chunk tool.
	MCPRemoveChunkOutput = vanadismcp.MCPRemoveChunkOutput
	// MCPFailChunkArgs defines arguments for fail_chunk tool.
	MCPFailChunkArgs = vanadismcp.MCPFailChunkArgs
	// MCPFailChunkOutput defines output for fail_chunk tool.
	MCPFailChunkOutput = vanadismcp.MCPFailChunkOutput
	// Handler serves the Vanadis MCP tool surface.
	Handler = vanadismcp.Handler
)

const (
	// ServerInstructions is sent on initialize so clients learn lifecycle rules.
	ServerInstructions = vanadismcp.ServerInstructions
)

// ToMCPChunkBriefs projects full chunks into concise MCP listing briefs.
var ToMCPChunkBriefs = vanadismcp.ToMCPChunkBriefs
