package mcp

import (
	"net/http"

	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// ServerConfig defines the connection parameters for an outbound MCP server.
type ServerConfig = runner.MCPServerConfig

// Tool represents a tool declared by an MCP server.
type Tool = runner.MCPTool

// Dispatcher defines the interface for managing and executing MCP tools.
type Dispatcher = runner.MCPDispatcherInterface

// DefaultDispatcher discovers and dispatches tools across configured HTTP/SSE MCP servers.
type DefaultDispatcher = runner.DefaultDispatcher

// NewDispatcher constructs a DefaultDispatcher with optional allowed tools filter.
func NewDispatcher(servers []ServerConfig, client *http.Client, allowedTools ...[]string) *DefaultDispatcher {
	return runner.NewDispatcher(servers, client, allowedTools...)
}
