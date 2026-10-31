package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// ServerConfig defines the connection parameters for an outbound MCP server.
type ServerConfig struct {
	Name      string            `json:"name" yaml:"name"`
	ServerURL string            `json:"serverUrl" yaml:"server_url"`
	Headers   map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// Tool represents a tool declared by an MCP server.
type Tool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"inputSchema,omitempty"`
}

// Dispatcher defines the interface for managing and executing MCP tools.
type Dispatcher interface {
	ListTools(ctx context.Context) ([]Tool, error)
	Execute(ctx context.Context, name string, args map[string]interface{}) (string, error)
	Prime(ctx context.Context) error
	EnsureSession(ctx context.Context, serverName string) (string, error)
}

// DefaultDispatcher discovers and dispatches tools across configured HTTP/SSE MCP servers.
type DefaultDispatcher struct {
	servers      []ServerConfig
	httpClient   *http.Client
	toolRoutes   map[string]ServerConfig
	sessionIDs   map[string]string
	serverMu     map[string]*sync.Mutex
	allowedTools map[string]struct{}
	mu           sync.RWMutex
}

var _ Dispatcher = (*DefaultDispatcher)(nil)

// NewDispatcher constructs a DefaultDispatcher with optional allowed tools filter.
func NewDispatcher(servers []ServerConfig, client *http.Client, allowedTools ...[]string) *DefaultDispatcher {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	var allowedMap map[string]struct{}
	if len(allowedTools) > 0 && allowedTools[0] != nil {
		allowedMap = make(map[string]struct{}, len(allowedTools[0]))
		for _, t := range allowedTools[0] {
			trimmed := strings.TrimSpace(t)
			if trimmed != "" {
				allowedMap[trimmed] = struct{}{}
			}
		}
	}
	return &DefaultDispatcher{
		servers:      servers,
		httpClient:   client,
		toolRoutes:   make(map[string]ServerConfig),
		sessionIDs:   make(map[string]string),
		serverMu:     make(map[string]*sync.Mutex),
		allowedTools: allowedMap,
	}
}

func (d *DefaultDispatcher) getServerMutex(serverName string) *sync.Mutex {
	d.mu.RLock()
	mu, ok := d.serverMu[serverName]
	d.mu.RUnlock()
	if ok {
		return mu
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if mu, ok = d.serverMu[serverName]; ok {
		return mu
	}
	mu = &sync.Mutex{}
	d.serverMu[serverName] = mu
	return mu
}

func (d *DefaultDispatcher) getServerConfig(serverName string) (ServerConfig, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, s := range d.servers {
		if s.Name == serverName {
			return s, true
		}
	}
	return ServerConfig{}, false
}

func extractMCPSessionID(h http.Header) string {
	sessID := h.Get("mcp-session-id")
	if sessID == "" {
		sessID = h.Get("Mcp-Session-Id")
	}
	if sessID == "" {
		for k, v := range h {
			if strings.EqualFold(k, "mcp-session-id") && len(v) > 0 {
				sessID = v[0]
				break
			}
		}
	}
	return sessID
}

func (d *DefaultDispatcher) sendInitializedNotification(ctx context.Context, srv ServerConfig, sessID string) {
	notifyReqBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	})
	if err != nil {
		return
	}
	notifyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(notifyReqBody))
	if err != nil {
		return
	}
	notifyReq.Header.Set("Content-Type", "application/json")
	notifyReq.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range srv.Headers {
		notifyReq.Header.Set(k, v)
	}
	if sessID != "" {
		notifyReq.Header.Set("mcp-session-id", sessID)
	}
	notifyResp, nErr := d.httpClient.Do(notifyReq)
	if nErr == nil && notifyResp != nil {
		if _, drainErr := io.Copy(io.Discard, io.LimitReader(notifyResp.Body, 64*1024)); drainErr != nil {
			log.Printf("[MCP] Warning reading notification body: %v", drainErr)
		}
		_ = notifyResp.Body.Close()
	}
}

// EnsureSession performs the standard MCP initialize handshake and caches the session ID.
func (d *DefaultDispatcher) EnsureSession(ctx context.Context, serverName string) (string, error) {
	if d == nil {
		return "", errors.New("mcp dispatcher is nil")
	}

	srv, ok := d.getServerConfig(serverName)
	if !ok {
		return "", fmt.Errorf("mcp server %q not found in configuration", serverName)
	}

	d.mu.RLock()
	sessID, ok := d.sessionIDs[srv.Name]
	d.mu.RUnlock()
	if ok && sessID != "" {
		return sessID, nil
	}

	sMu := d.getServerMutex(srv.Name)
	sMu.Lock()
	defer sMu.Unlock()

	d.mu.RLock()
	sessID, ok = d.sessionIDs[srv.Name]
	d.mu.RUnlock()
	if ok && sessID != "" {
		return sessID, nil
	}

	initReqBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]interface{}{},
			"clientInfo": map[string]interface{}{
				"name":    "aerial",
				"version": "1.0.0",
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal initialize request: %w", err)
	}

	initReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(initReqBody))
	if err != nil {
		return "", fmt.Errorf("failed to create initialize request for %s: %w", srv.Name, err)
	}
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range srv.Headers {
		initReq.Header.Set(k, v)
	}

	initResp, err := d.httpClient.Do(initReq)
	if err != nil {
		return "", fmt.Errorf("failed to send initialize to %s: %w", srv.Name, err)
	}
	defer initResp.Body.Close()

	initRespBytes, rErr := io.ReadAll(io.LimitReader(initResp.Body, 512*1024))
	if rErr != nil || initResp.StatusCode < 200 || initResp.StatusCode >= 300 {
		return "", fmt.Errorf("mcp server %s initialize returned status %d: %s", srv.Name, initResp.StatusCode, string(initRespBytes))
	}

	sessID = extractMCPSessionID(initResp.Header)
	if sessID != "" {
		d.mu.Lock()
		d.sessionIDs[srv.Name] = sessID
		d.mu.Unlock()
	}

	d.sendInitializedNotification(ctx, srv, sessID)

	return sessID, nil
}

func (d *DefaultDispatcher) doJSONRPCWithRetry(ctx context.Context, srv ServerConfig, sessID string, reqBody []byte, maxBytes int64) (*jsonRPCResponse, error) {
	currentSessID := sessID
	if maxBytes <= 0 {
		maxBytes = 10 << 20
	}

	for attempt := 0; attempt < 2; attempt++ {
		statusCode, respBytes, err := d.executeRPCRequest(ctx, srv, currentSessID, reqBody, maxBytes)
		if err != nil {
			if attempt == 0 {
				return nil, fmt.Errorf("failed to call %s: %w", srv.Name, err)
			}
			return nil, fmt.Errorf("failed to call %s on retry: %w", srv.Name, err)
		}

		rpcResp, parseErr := parseJSONRPCBody(respBytes)
		if attempt == 0 && isMissingSessionIDErr(statusCode, respBytes, rpcResp) {
			newSessID, sErr := d.recoverSession(ctx, srv.Name, currentSessID)
			if sErr != nil {
				return nil, fmt.Errorf("failed to re-establish session for %s: %w", srv.Name, sErr)
			}
			currentSessID = newSessID
			continue
		}

		return validateRPCResponse(srv.Name, statusCode, respBytes, rpcResp, parseErr)
	}

	return nil, fmt.Errorf("request to %s failed after retry", srv.Name)
}

func (d *DefaultDispatcher) executeRPCRequest(ctx context.Context, srv ServerConfig, sessID string, reqBody []byte, maxBytes int64) (int, []byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(reqBody))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create request for %s: %w", srv.Name, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range srv.Headers {
		httpReq.Header.Set(k, v)
	}
	if sessID != "" {
		httpReq.Header.Set("mcp-session-id", sessID)
	}

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBytes, rErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if rErr != nil {
		return resp.StatusCode, nil, fmt.Errorf("read error from %s: %w", srv.Name, rErr)
	}
	return resp.StatusCode, respBytes, nil
}

func (d *DefaultDispatcher) recoverSession(ctx context.Context, srvName, oldSessID string) (string, error) {
	d.mu.Lock()
	if d.sessionIDs[srvName] == oldSessID {
		delete(d.sessionIDs, srvName)
	}
	d.mu.Unlock()

	return d.EnsureSession(ctx, srvName)
}

func validateRPCResponse(srvName string, statusCode int, respBytes []byte, rpcResp *jsonRPCResponse, parseErr error) (*jsonRPCResponse, error) {
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("mcp server %s returned status %d: %s", srvName, statusCode, string(respBytes))
	}
	if parseErr != nil || rpcResp == nil {
		return nil, fmt.Errorf("empty or invalid JSON-RPC response from %s: %w", srvName, parseErr)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("mcp server %s returned error: %s (code %d)", srvName, rpcResp.Error.Message, rpcResp.Error.Code)
	}
	return rpcResp, nil
}

func (d *DefaultDispatcher) fetchServerTools(ctx context.Context, srv ServerConfig) ([]Tool, error) {
	d.mu.RLock()
	sessID := d.sessionIDs[srv.Name]
	d.mu.RUnlock()

	reqBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
		"params":  map[string]interface{}{},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tools/list payload: %w", err)
	}

	rpcResp, err := d.doJSONRPCWithRetry(ctx, srv, sessID, reqBody, 10<<20)
	if err != nil {
		return nil, err
	}

	var listResult struct {
		Tools []Tool `json:"tools"`
	}
	if err := json.Unmarshal(rpcResp.Result, &listResult); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tools from %s: %w", srv.Name, err)
	}

	var filteredTools []Tool
	d.mu.Lock()
	for _, tool := range listResult.Tools {
		if tool.Name == "" {
			continue
		}
		if d.allowedTools != nil {
			if _, allowed := d.allowedTools[tool.Name]; !allowed {
				continue
			}
		}
		d.toolRoutes[tool.Name] = srv
		filteredTools = append(filteredTools, tool)
	}
	d.mu.Unlock()

	return filteredTools, nil
}

// ListTools queries all configured MCP servers via tools/list and returns discovered tools.
func (d *DefaultDispatcher) ListTools(ctx context.Context) ([]Tool, error) {
	if d == nil || len(d.servers) == 0 {
		return nil, nil
	}

	var allTools []Tool
	for _, srv := range d.servers {
		if strings.TrimSpace(srv.ServerURL) == "" {
			continue
		}
		tools, err := d.fetchServerTools(ctx, srv)
		if err != nil {
			log.Printf("[MCP] Failed to fetch tools from %s: %v", srv.Name, err)
			continue
		}
		allTools = append(allTools, tools...)
	}
	return allTools, nil
}

func (d *DefaultDispatcher) resolveToolServer(ctx context.Context, name string) (ServerConfig, error) {
	d.mu.RLock()
	if d.allowedTools != nil {
		if _, allowed := d.allowedTools[name]; !allowed {
			d.mu.RUnlock()
			return ServerConfig{}, fmt.Errorf("tool %q is not in the allowed tools list", name)
		}
	}
	srv, ok := d.toolRoutes[name]
	d.mu.RUnlock()

	if !ok {
		if _, listErr := d.ListTools(ctx); listErr != nil {
			log.Printf("[MCP] JIT ListTools warning: %v", listErr)
		}
		d.mu.RLock()
		srv, ok = d.toolRoutes[name]
		d.mu.RUnlock()
		if !ok {
			return ServerConfig{}, fmt.Errorf("tool %q not found on any configured MCP server", name)
		}
	}
	return srv, nil
}

func prepareToolCallArgs(args map[string]interface{}) map[string]interface{} {
	if args == nil {
		return make(map[string]interface{})
	}
	copiedArgs := make(map[string]interface{}, len(args))
	for k, v := range args {
		copiedArgs[k] = v
	}
	if val, ok := copiedArgs["user_google_email"].(string); ok {
		trimmed := strings.ToLower(strings.TrimSpace(val))
		if trimmed == "primary" || trimmed == "me" || trimmed == "default" || trimmed == "" {
			delete(copiedArgs, "user_google_email")
		}
	}
	return copiedArgs
}

func decodeToolCallResult(name string, raw json.RawMessage) (string, error) {
	var callResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text,omitempty"`
		} `json:"content,omitempty"`
		IsError bool `json:"isError,omitempty"`
	}

	if err := json.Unmarshal(raw, &callResult); err == nil && len(callResult.Content) > 0 {
		var sb strings.Builder
		for _, c := range callResult.Content {
			if c.Text != "" {
				if sb.Len() > 0 {
					sb.WriteString("\n")
				}
				sb.WriteString(c.Text)
			}
		}
		resText := sb.String()
		if callResult.IsError {
			return resText, fmt.Errorf("tool %q returned error: %s", name, resText)
		}
		return resText, nil
	}

	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return str, nil
	}

	return string(raw), nil
}

// Execute calls a named tool on the corresponding MCP server with the provided arguments.
func (d *DefaultDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	if d == nil {
		return "", errors.New("mcp dispatcher is nil")
	}

	srv, err := d.resolveToolServer(ctx, name)
	if err != nil {
		return "", err
	}

	d.mu.RLock()
	sessID := d.sessionIDs[srv.Name]
	d.mu.RUnlock()

	if sessID == "" {
		var sErr error
		sessID, sErr = d.EnsureSession(ctx, srv.Name)
		if sErr != nil {
			log.Printf("[MCP] Failed to ensure session for %s before execute: %v", srv.Name, sErr)
		}
	}

	callArgs := prepareToolCallArgs(args)

	reqBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      name,
			"arguments": callArgs,
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal tools/call request: %w", err)
	}

	rpcResp, err := d.doJSONRPCWithRetry(ctx, srv, sessID, reqBody, 50<<20)
	if err != nil {
		return "", err
	}

	return decodeToolCallResult(name, rpcResp.Result)
}

// Prime warms up sessions and discovers tools across all configured MCP servers concurrently.
func (d *DefaultDispatcher) Prime(ctx context.Context) error {
	if d == nil || len(d.servers) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, srv := range d.servers {
		s := srv
		if strings.TrimSpace(s.ServerURL) == "" {
			continue
		}
		g.Go(func() error {
			if _, err := d.EnsureSession(gCtx, s.Name); err != nil {
				log.Printf("[MCP] Prime: ensureSession failed for %s: %v", s.Name, err)
			}
			if _, err := d.fetchServerTools(gCtx, s); err != nil {
				log.Printf("[MCP] Prime: fetchServerTools failed for %s: %v", s.Name, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func parseSSEJSONRPCResponse(trimmed []byte) (*jsonRPCResponse, bool) {
	if !bytes.HasPrefix(trimmed, []byte("event:")) && !bytes.HasPrefix(trimmed, []byte("data:")) && !bytes.Contains(trimmed, []byte("\ndata:")) {
		return nil, false
	}
	lines := bytes.Split(trimmed, []byte("\n"))
	for _, rawLine := range lines {
		line := bytes.TrimSpace(rawLine)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		dataPayload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if len(dataPayload) == 0 || dataPayload[0] != '{' {
			continue
		}
		var resp jsonRPCResponse
		if err := json.Unmarshal(dataPayload, &resp); err == nil {
			return &resp, true
		}
	}
	return nil, false
}

func parseJSONRPCBody(body []byte) (*jsonRPCResponse, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, errors.New("empty response body")
	}

	if resp, ok := parseSSEJSONRPCResponse(trimmed); ok {
		return resp, nil
	}

	var resp jsonRPCResponse
	if err := json.Unmarshal(trimmed, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON-RPC response: %w", err)
	}
	return &resp, nil
}

func isMissingSessionIDErr(statusCode int, bodyBytes []byte, rpcResp *jsonRPCResponse) bool {
	if statusCode == http.StatusBadRequest {
		return true
	}
	if rpcResp != nil && rpcResp.Error != nil {
		if rpcResp.Error.Code == -32600 || strings.Contains(strings.ToLower(rpcResp.Error.Message), "missing session id") {
			return true
		}
	}
	if bytes.Contains(bytes.ToLower(bodyBytes), []byte("missing session id")) {
		return true
	}
	return false
}
