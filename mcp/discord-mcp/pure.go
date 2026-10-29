package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
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

var marshalFn = json.Marshal

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
	b, err := marshalFn(resp)
	if err != nil {
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error marshalling response"}}`)
	}
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
	b, err := marshalFn(resp)
	if err != nil {
		return []byte(`{"status":"error","wrapper":` + strconv.Quote(w) + `,"blocked_tools":[]}`)
	}
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

// ProjectedDiscordMessage contains only essential fields from a Discord search message.
type ProjectedDiscordMessage struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	ChannelID string `json:"channel_id"`
	Author    string `json:"author"`
	AuthorID  string `json:"author_id,omitempty"`
	Content   string `json:"content"`
}

// ShapedSearchResponse is the compact payload returned for discord_search_messages.
type ShapedSearchResponse struct {
	TotalResults  int                       `json:"total_results"`
	ReturnedCount int                       `json:"returned_count"`
	Offset        int                       `json:"offset"`
	HasMore       bool                      `json:"has_more"`
	Messages      []ProjectedDiscordMessage `json:"messages"`
}

type rawDiscordSearchPayload struct {
	TotalResults int               `json:"total_results"`
	Messages     []json.RawMessage `json:"messages"`
}

type rawDiscordMessage struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
	Hit       *bool  `json:"hit,omitempty"`
	Author    struct {
		ID         string `json:"id"`
		Username   string `json:"username"`
		GlobalName string `json:"global_name"`
	} `json:"author"`
}

// ExtractSearchOffset extracts a non-negative offset parameter from a tools/call JSON-RPC request.
func ExtractSearchOffset(rawReq []byte) int {
	var req JSONRPCRequest
	if err := json.Unmarshal(bytes.TrimSpace(rawReq), &req); err != nil || len(req.Params) == 0 {
		return 0
	}
	var params ToolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params.Arguments) == 0 {
		return 0
	}
	var args struct {
		Offset int `json:"offset"`
	}
	if err := json.Unmarshal(params.Arguments, &args); err != nil || args.Offset < 0 {
		return 0
	}
	return args.Offset
}

func projectDiscordMessage(m rawDiscordMessage) ProjectedDiscordMessage {
	author := strings.TrimSpace(m.Author.Username)
	if g := strings.TrimSpace(m.Author.GlobalName); g != "" && !strings.EqualFold(g, author) {
		if author != "" {
			author = g + " (" + author + ")"
		} else {
			author = g
		}
	}
	return ProjectedDiscordMessage{
		ID:        m.ID,
		Timestamp: m.Timestamp,
		ChannelID: m.ChannelID,
		Author:    author,
		AuthorID:  m.Author.ID,
		Content:   m.Content,
	}
}

func parseSearchMessageEntry(raw json.RawMessage) []ProjectedDiscordMessage {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	if trimmed[0] == '[' {
		var group []rawDiscordMessage
		if err := json.Unmarshal(trimmed, &group); err != nil {
			return nil
		}
		var hits []ProjectedDiscordMessage
		for _, m := range group {
			if m.Hit != nil && *m.Hit {
				hits = append(hits, projectDiscordMessage(m))
			}
		}
		if len(hits) > 0 {
			return hits
		}
		out := make([]ProjectedDiscordMessage, 0, len(group))
		for _, m := range group {
			out = append(out, projectDiscordMessage(m))
		}
		return out
	}
	var single rawDiscordMessage
	if err := json.Unmarshal(trimmed, &single); err != nil {
		return nil
	}
	return []ProjectedDiscordMessage{projectDiscordMessage(single)}
}

// ShapeSearchMessagesResponse projects a discord_search_messages JSON-RPC response down to essential fields and explicit pagination metadata.
func ShapeSearchMessagesResponse(rawResp []byte, offset int) ([]byte, error) {
	var resp JSONRPCResponse
	if err := json.Unmarshal(rawResp, &resp); err != nil || len(resp.Result) == 0 {
		return rawResp, err
	}
	var resMap map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(resp.Result), &resMap); err != nil {
		return rawResp, err
	}
	contentRaw, ok := resMap["content"]
	if !ok || len(contentRaw) == 0 {
		return rawResp, nil
	}
	var contentList []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(contentRaw, &contentList); err != nil || len(contentList) == 0 || contentList[0].Text == "" {
		return rawResp, err
	}
	var payload rawDiscordSearchPayload
	if err := json.Unmarshal([]byte(contentList[0].Text), &payload); err != nil || payload.Messages == nil {
		return rawResp, err
	}

	projected := make([]ProjectedDiscordMessage, 0, len(payload.Messages))
	for _, entry := range payload.Messages {
		projected = append(projected, parseSearchMessageEntry(entry)...)
	}
	if offset < 0 {
		offset = 0
	}
	total := payload.TotalResults
	if total < len(projected) {
		total = len(projected)
	}
	shaped := ShapedSearchResponse{
		TotalResults:  total,
		ReturnedCount: len(projected),
		Offset:        offset,
		HasMore:       total > offset+len(projected),
		Messages:      projected,
	}
	shapedBytes, err := marshalFn(shaped)
	if err != nil {
		return rawResp, err
	}
	contentList[0].Text = string(shapedBytes)
	newContentBytes, err := marshalFn(contentList)
	if err != nil {
		return rawResp, err
	}
	resMap["content"] = newContentBytes
	newResBytes, err := marshalFn(resMap)
	if err != nil {
		return rawResp, err
	}
	resp.Result = newResBytes
	return marshalFn(resp)
}
