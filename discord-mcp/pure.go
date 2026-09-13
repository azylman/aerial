package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// DefaultBlockedToolList defines the canonical, alphabetically sorted list of blocked Discord tools.
var DefaultBlockedToolList = []string{
	"discord_create_thread",
	"discord_send",
	"send_message",
}

// BlockedToolNames defines the default set of tools that should be hidden and disallowed.
var BlockedToolNames = map[string]bool{
	"discord_send":          true,
	"send_message":          true,
	"discord_create_thread": true,
}

// JSONRPCRequest represents a minimal JSON-RPC 2.0 request structure.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// ToolCallParams represents parameters for tools/call.
type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// JSONRPCResponse represents a minimal JSON-RPC 2.0 response structure.
// Note: ID does NOT use omitempty so that nil IDs serialize as "id": null per JSON-RPC 2.0.
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC 2.0 error object.
type JSONRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// ToolsListResult represents the result payload of a tools/list request.
type ToolsListResult struct {
	Tools []ToolItem `json:"tools"`
}

// ToolItem represents an individual MCP tool in tools/list.
type ToolItem struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
}

// HealthResponse represents the payload of the /health endpoint.
type HealthResponse struct {
	Status       string   `json:"status"`
	Wrapper      string   `json:"wrapper"`
	BlockedTools []string `json:"blocked_tools"`
}

// isToolNameBlocked checks if a given tool name matches any blocked tool name case-insensitively.
func isToolNameBlocked(name string, blocked map[string]bool) bool {
	clean := strings.ToLower(strings.TrimSpace(name))
	if clean == "" {
		return false
	}
	if blocked != nil {
		if blocked[clean] {
			return true
		}
		for k, v := range blocked {
			if v && strings.ToLower(strings.TrimSpace(k)) == clean {
				return true
			}
		}
		return false
	}
	return BlockedToolNames[clean]
}

// IsBlockedToolCall inspects a JSON-RPC request (single or batch) to see if it calls a blocked tool.
// It detects top-level batch requests ([...]) and normalizes tool names to lowercase to prevent injection.
func IsBlockedToolCall(rawReq []byte, blocked map[string]bool) (bool, interface{}, string, error) {
	trimmed := bytes.TrimSpace(rawReq)
	if len(trimmed) == 0 {
		return false, nil, "", io.ErrUnexpectedEOF
	}

	// 1. Check for JSON-RPC batch request: [...]
	if trimmed[0] == '[' {
		var batch []JSONRPCRequest
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			return false, nil, "", err
		}
		for _, req := range batch {
			if strings.TrimSpace(req.Method) == "tools/call" {
				var params ToolCallParams
				if len(req.Params) > 0 {
					if err := json.Unmarshal(req.Params, &params); err != nil {
						return false, req.ID, "", err
					}
				}
				if isToolNameBlocked(params.Name, blocked) {
					return true, req.ID, params.Name, nil
				}
			}
		}
		var firstID interface{}
		if len(batch) > 0 {
			firstID = batch[0].ID
		}
		return false, firstID, "", nil
	}

	// 2. Single JSON-RPC request: {...}
	var req JSONRPCRequest
	if err := json.Unmarshal(trimmed, &req); err != nil {
		return false, nil, "", err
	}

	if strings.TrimSpace(req.Method) != "tools/call" {
		return false, req.ID, "", nil
	}

	var params ToolCallParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return false, req.ID, "", err
		}
	}

	if isToolNameBlocked(params.Name, blocked) {
		return true, req.ID, params.Name, nil
	}

	return false, req.ID, params.Name, nil
}

// FilterToolsResponse filters out blocked tools from a tools/list JSON-RPC response.
// It preserves additional MCP fields such as nextCursor and _meta, and preserves null results without shape mutation.
func FilterToolsResponse(rawResp []byte, blocked map[string]bool) ([]byte, error) {
	var resp JSONRPCResponse
	if err := json.Unmarshal(rawResp, &resp); err != nil {
		return rawResp, err
	}

	if len(resp.Result) == 0 {
		return rawResp, nil
	}

	trimmedResult := bytes.TrimSpace(resp.Result)
	// If result is null, do not mutate shape
	if len(trimmedResult) == 0 || bytes.Equal(trimmedResult, []byte("null")) {
		return rawResp, nil
	}

	// Unmarshal into generic map to preserve pagination (nextCursor) and metadata
	var rawResultMap map[string]json.RawMessage
	if err := json.Unmarshal(trimmedResult, &rawResultMap); err != nil {
		return rawResp, err
	}

	toolsRaw, ok := rawResultMap["tools"]
	if !ok || len(toolsRaw) == 0 {
		return rawResp, nil
	}

	var toolsList []ToolItem
	if err := json.Unmarshal(toolsRaw, &toolsList); err != nil {
		return rawResp, err
	}

	filteredTools := make([]ToolItem, 0, len(toolsList))
	for _, tool := range toolsList {
		if !isToolNameBlocked(tool.Name, blocked) {
			filteredTools = append(filteredTools, tool)
		}
	}

	filteredToolsBytes, err := json.Marshal(filteredTools)
	if err != nil {
		return rawResp, err
	}

	rawResultMap["tools"] = filteredToolsBytes
	resultBytes, err := json.Marshal(rawResultMap)
	if err != nil {
		return rawResp, err
	}

	resp.Result = resultBytes
	return json.Marshal(resp)
}

// IsToolsListRequest checks if a raw JSON-RPC request byte slice is a tools/list call.
func IsToolsListRequest(rawReq []byte) bool {
	trimmed := bytes.TrimSpace(rawReq)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var req JSONRPCRequest
	if err := json.Unmarshal(trimmed, &req); err != nil {
		return false
	}
	return strings.TrimSpace(req.Method) == "tools/list"
}

// BuildJSONRPCErrorResponse constructs a compact JSON-RPC 2.0 error response byte slice.
// If id is nil, it serializes "id": null.
func BuildJSONRPCErrorResponse(id interface{}, code int, message string) []byte {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &JSONRPCError{
			Code:    code,
			Message: message,
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

// BuildHealthResponse formats the JSON payload for the /health endpoint with sorted blocked tools.
func BuildHealthResponse(wrapper string, blockedTools []string) []byte {
	w := strings.TrimSpace(wrapper)
	if w == "" {
		w = "aerial-discord-mcp"
	}
	tools := make([]string, 0)
	if len(blockedTools) > 0 {
		tools = append(tools, blockedTools...)
		sort.Strings(tools)
	}
	resp := HealthResponse{
		Status:       "ok",
		Wrapper:      w,
		BlockedTools: tools,
	}
	b, _ := json.Marshal(resp)
	return b
}

// ValidateConfig validates and normalizes Config fields with non-empty checks.
func ValidateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("discord-mcp: config cannot be nil")
	}
	cfg.Port = strings.TrimSpace(cfg.Port)
	if cfg.Port == "" {
		return fmt.Errorf("discord-mcp: port cannot be empty")
	}
	cfg.UpstreamPort = strings.TrimSpace(cfg.UpstreamPort)
	if cfg.UpstreamPort == "" {
		return fmt.Errorf("discord-mcp: upstreamPort cannot be empty")
	}
	cfg.NodeBin = strings.TrimSpace(cfg.NodeBin)
	if cfg.NodeBin == "" {
		return fmt.Errorf("discord-mcp: nodeBin cannot be empty")
	}
	cfg.AppPath = strings.TrimSpace(cfg.AppPath)
	if cfg.AppPath == "" {
		return fmt.Errorf("discord-mcp: appPath cannot be empty")
	}
	return nil
}

// BuildNodeEnv builds the child process environment, case-insensitively filtering out any existing PORT variable
// and appending PORT=<upstreamPort>.
func BuildNodeEnv(environ []string, upstreamPort string) []string {
	upPort := strings.TrimSpace(upstreamPort)
	nodeEnv := make([]string, 0, len(environ)+1)
	for _, e := range environ {
		upper := strings.ToUpper(e)
		if !strings.HasPrefix(upper, "PORT=") {
			nodeEnv = append(nodeEnv, e)
		}
	}
	nodeEnv = append(nodeEnv, "PORT="+upPort)
	return nodeEnv
}

// FormatUpstreamURL returns the loopback URL for the upstream MCP server.
func FormatUpstreamURL(upstreamPort string) string {
	return "http://127.0.0.1:" + strings.TrimSpace(upstreamPort)
}
