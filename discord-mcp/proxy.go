package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
)

// BlockedToolNames defines the set of tools that should be hidden and disallowed.
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
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
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

// IsBlockedToolCall inspects a JSON-RPC request to see if it calls a blocked tool.
func IsBlockedToolCall(rawReq []byte, blocked map[string]bool) (bool, interface{}, string, error) {
	var req JSONRPCRequest
	if err := json.Unmarshal(rawReq, &req); err != nil {
		return false, nil, "", err
	}

	if req.Method != "tools/call" {
		return false, req.ID, "", nil
	}

	var params ToolCallParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return false, req.ID, "", err
		}
	}

	if blocked[strings.TrimSpace(params.Name)] {
		return true, req.ID, params.Name, nil
	}

	return false, req.ID, params.Name, nil
}

// FilterToolsResponse filters out blocked tools from a tools/list JSON-RPC response.
func FilterToolsResponse(rawResp []byte, blocked map[string]bool) ([]byte, error) {
	var resp JSONRPCResponse
	if err := json.Unmarshal(rawResp, &resp); err != nil {
		return rawResp, err
	}

	if len(resp.Result) == 0 {
		return rawResp, nil
	}

	var listResult ToolsListResult
	if err := json.Unmarshal(resp.Result, &listResult); err != nil {
		return rawResp, err
	}

	filteredTools := make([]ToolItem, 0, len(listResult.Tools))
	for _, tool := range listResult.Tools {
		if !blocked[strings.TrimSpace(tool.Name)] {
			filteredTools = append(filteredTools, tool)
		}
	}

	filteredResult := ToolsListResult{
		Tools: filteredTools,
	}

	resultBytes, err := json.Marshal(filteredResult)
	if err != nil {
		return rawResp, err
	}

	resp.Result = resultBytes
	return json.Marshal(resp)
}

// ProxyHandler proxies MCP HTTP requests to the upstream server while filtering blocked tools.
type ProxyHandler struct {
	upstreamURL  *url.URL
	reverseProxy *httputil.ReverseProxy
	httpClient   *http.Client
	blockedTools map[string]bool
	mu           sync.RWMutex
}

// NewProxyHandler creates a new ProxyHandler targeting the given upstream URL.
func NewProxyHandler(target string, blocked map[string]bool) (*ProxyHandler, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream URL %q: %w", target, err)
	}

	rp := httputil.NewSingleHostReverseProxy(u)

	return &ProxyHandler{
		upstreamURL:  u,
		reverseProxy: rp,
		httpClient:   &http.Client{},
		blockedTools: blocked,
	}, nil
}

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// If not POST or not an MCP JSON-RPC endpoint, let the reverse proxy handle it (e.g. SSE streaming or GET).
	if r.Method != http.MethodPost {
		p.reverseProxy.ServeHTTP(w, r)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	// 1. Check if the request is attempting to call a blocked tool
	isBlocked, reqID, toolName, err := IsBlockedToolCall(bodyBytes, p.blockedTools)
	if err == nil && isBlocked {
		log.Printf("[Proxy] Blocked tool call attempted for %q. Returning JSON-RPC error.", toolName)
		errResp := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      reqID,
			Error: &JSONRPCError{
				Code:    -32601,
				Message: fmt.Sprintf("Tool %q is disabled. Outbound replies to the user are automatically delivered by Aerial Brain at the end of the turn.", toolName),
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(errResp)
		return
	}

	// 2. Determine if this request is a tools/list request
	var jsonReq JSONRPCRequest
	isToolsList := false
	if err := json.Unmarshal(bodyBytes, &jsonReq); err == nil && jsonReq.Method == "tools/list" {
		isToolsList = true
	}

	// Forward request to upstream
	targetURL := p.upstreamURL.ResolveReference(r.URL)
	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to construct upstream request: %v", err), http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, v := range values {
			upstreamReq.Header.Add(key, v)
		}
	}
	upstreamReq.Header.Set("Host", p.upstreamURL.Host)

	resp, err := p.httpClient.Do(upstreamReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read upstream response: %v", err), http.StatusBadGateway)
		return
	}

	// If this was tools/list, filter the response
	if isToolsList && resp.StatusCode == http.StatusOK {
		filtered, err := FilterToolsResponse(respBytes, p.blockedTools)
		if err == nil {
			respBytes = filtered
		} else {
			log.Printf("[Proxy] Warning: failed to filter tools/list response: %v", err)
		}
	}

	// Copy response headers
	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(respBytes)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBytes)
}
