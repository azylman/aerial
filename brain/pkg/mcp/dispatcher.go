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

	sessID = initResp.Header.Get("mcp-session-id")
	if sessID == "" {
		sessID = initResp.Header.Get("Mcp-Session-Id")
	}
	if sessID == "" {
		for k, v := range initResp.Header {
			if strings.EqualFold(k, "mcp-session-id") && len(v) > 0 {
				sessID = v[0]
				break
			}
		}
	}

	if sessID != "" {
		d.mu.Lock()
		d.sessionIDs[srv.Name] = sessID
		d.mu.Unlock()
	}

	// Send notifications/initialized notification with mcp-session-id header
	notifyReqBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	})
	if err == nil {
		notifyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(notifyReqBody))
		if err == nil {
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
	}

	return sessID, nil
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request for %s: %w", srv.Name, err)
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
		return nil, fmt.Errorf("failed to query tools/list on %s: %w", srv.Name, err)
	}
	respBytes, rErr := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	_ = resp.Body.Close()

	rpcResp, parseErr := parseJSONRPCBody(respBytes)
	if isMissingSessionIDErr(resp.StatusCode, respBytes, rpcResp) {
		d.mu.Lock()
		if d.sessionIDs[srv.Name] == sessID {
			delete(d.sessionIDs, srv.Name)
		}
		d.mu.Unlock()

		newSessID, sErr := d.EnsureSession(ctx, srv.Name)
		if sErr != nil {
			return nil, fmt.Errorf("failed to re-establish session for %s: %w", srv.Name, sErr)
		}

		retryReq, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(reqBody))
		if reqErr != nil {
			return nil, fmt.Errorf("failed to create retry request for %s: %w", srv.Name, reqErr)
		}
		retryReq.Header.Set("Content-Type", "application/json")
		retryReq.Header.Set("Accept", "application/json, text/event-stream")
		for k, v := range srv.Headers {
			retryReq.Header.Set(k, v)
		}
		if newSessID != "" {
			retryReq.Header.Set("mcp-session-id", newSessID)
		}

		retryResp, doErr := d.httpClient.Do(retryReq)
		if doErr != nil {
			return nil, fmt.Errorf("failed to query tools/list on retry for %s: %w", srv.Name, doErr)
		}
		resp = retryResp
		respBytes, rErr = io.ReadAll(io.LimitReader(retryResp.Body, 512*1024))
		_ = retryResp.Body.Close()
		rpcResp, parseErr = parseJSONRPCBody(respBytes)
	}

	if rErr != nil {
		return nil, fmt.Errorf("read error from %s: %w", srv.Name, rErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("mcp server %s returned status %d: %s", srv.Name, resp.StatusCode, string(respBytes))
	}

	if parseErr != nil || rpcResp == nil {
		return nil, fmt.Errorf("empty or invalid JSON-RPC response from %s: %w", srv.Name, parseErr)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("mcp server %s returned error: %s (code %d)", srv.Name, rpcResp.Error.Message, rpcResp.Error.Code)
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

// RegisterToolRoute manually associates a tool name with a server config.
func (d *DefaultDispatcher) RegisterToolRoute(toolName string, srv ServerConfig) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.toolRoutes[toolName] = srv
}

// Execute calls a named tool on the corresponding MCP server with the provided arguments.
func (d *DefaultDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	if d == nil {
		return "", errors.New("mcp dispatcher is nil")
	}

	d.mu.RLock()
	if d.allowedTools != nil {
		if _, allowed := d.allowedTools[name]; !allowed {
			d.mu.RUnlock()
			return "", fmt.Errorf("tool %q is not in the allowed tools list", name)
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
			return "", fmt.Errorf("tool %q not found on any configured MCP server", name)
		}
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

	if args == nil {
		args = make(map[string]interface{})
	} else {
		copiedArgs := make(map[string]interface{}, len(args))
		for k, v := range args {
			copiedArgs[k] = v
		}
		args = copiedArgs
	}

	if val, ok := args["user_google_email"].(string); ok {
		trimmed := strings.ToLower(strings.TrimSpace(val))
		if trimmed == "primary" || trimmed == "me" || trimmed == "default" || trimmed == "" {
			delete(args, "user_google_email")
		}
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      name,
			"arguments": args,
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal tools/call request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("failed to create http request for tool %q: %w", name, err)
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
		return "", fmt.Errorf("failed to call tool %q on %s: %w", name, srv.Name, err)
	}

	respBytes, rErr := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	_ = resp.Body.Close()
	if rErr != nil {
		return "", fmt.Errorf("failed to read response for tool %q: %w", name, rErr)
	}

	rpcResp, parseErr := parseJSONRPCBody(respBytes)
	if isMissingSessionIDErr(resp.StatusCode, respBytes, rpcResp) {
		d.mu.Lock()
		if d.sessionIDs[srv.Name] == sessID {
			delete(d.sessionIDs, srv.Name)
		}
		d.mu.Unlock()

		newSessID, sErr := d.EnsureSession(ctx, srv.Name)
		if sErr != nil {
			return "", fmt.Errorf("failed to establish session with %s for tool %q: %w", srv.Name, name, sErr)
		}

		retryReq, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, srv.ServerURL, bytes.NewReader(reqBody))
		if reqErr != nil {
			return "", fmt.Errorf("failed to create retry request for tool %q: %w", name, reqErr)
		}
		retryReq.Header.Set("Content-Type", "application/json")
		retryReq.Header.Set("Accept", "application/json, text/event-stream")
		for k, v := range srv.Headers {
			retryReq.Header.Set(k, v)
		}
		if newSessID != "" {
			retryReq.Header.Set("mcp-session-id", newSessID)
		}

		retryResp, doErr := d.httpClient.Do(retryReq)
		if doErr != nil {
			return "", fmt.Errorf("failed to retry tool %q on %s: %w", name, srv.Name, doErr)
		}
		resp = retryResp
		respBytes, rErr = io.ReadAll(io.LimitReader(retryResp.Body, 1024*1024))
		_ = retryResp.Body.Close()
		if rErr != nil {
			return "", fmt.Errorf("failed to read retry response for tool %q: %w", name, rErr)
		}
		rpcResp, parseErr = parseJSONRPCBody(respBytes)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("mcp server %s returned status %d: %s", srv.Name, resp.StatusCode, string(respBytes))
	}

	if parseErr != nil || rpcResp == nil {
		return "", fmt.Errorf("empty or invalid JSON-RPC response for tool %q: %w", name, parseErr)
	}
	if rpcResp.Error != nil {
		return "", fmt.Errorf("mcp tool %q error: %s (code %d)", name, rpcResp.Error.Message, rpcResp.Error.Code)
	}

	var callResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text,omitempty"`
		} `json:"content,omitempty"`
		IsError bool `json:"isError,omitempty"`
	}

	if err := json.Unmarshal(rpcResp.Result, &callResult); err == nil && len(callResult.Content) > 0 {
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
	if err := json.Unmarshal(rpcResp.Result, &str); err == nil {
		return str, nil
	}

	return string(rpcResp.Result), nil
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

func parseJSONRPCBody(body []byte) (*jsonRPCResponse, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, errors.New("empty response body")
	}

	if bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("data:")) || bytes.Contains(trimmed, []byte("\ndata:")) {
		lines := bytes.Split(trimmed, []byte("\n"))
		for _, rawLine := range lines {
			line := bytes.TrimSpace(rawLine)
			if bytes.HasPrefix(line, []byte("data:")) {
				dataPayload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
				if len(dataPayload) > 0 && dataPayload[0] == '{' {
					var resp jsonRPCResponse
					if err := json.Unmarshal(dataPayload, &resp); err == nil {
						return &resp, nil
					}
				}
			}
		}
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
