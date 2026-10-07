package runner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/mcp"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/google/uuid"
)

var apiKeyQueryRegex = regexp.MustCompile(`(?i)(key=)[^& \t\r\n"']+`)

// MCPServerConfig represents an endpoint configuration for an MCP microservice.
type MCPServerConfig = mcp.ServerConfig

// MCPTool represents a tool definition returned by an MCP server tools/list call.
type MCPTool = mcp.Tool

// MCPDispatcher abstracts tool discovery and tool execution across MCP servers.
type MCPDispatcher interface {
	ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error)
	Execute(ctx context.Context, name string, args map[string]interface{}) (string, error)
}

// jsonRPCResponse models a generic JSON-RPC 2.0 response.
type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

// jsonRPCError models a JSON-RPC 2.0 error block.
type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// parseJSONRPCBody parses JSON-RPC bodies, transparently handling both SSE streams and standard JSON.
func parseJSONRPCBody(body []byte) (*jsonRPCResponse, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, errors.New("empty response body")
	}

	// Check if this is an SSE stream (e.g. ha-mcp)
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

// cleanParameters sanitizes inputSchema for Gemini function calling.
func cleanParameters(schema map[string]interface{}) map[string]interface{} {
	if schema == nil {
		return map[string]interface{}{
			"type":       "OBJECT",
			"properties": map[string]interface{}{},
		}
	}
	res := cleanSchemaNode(schema)
	if res == nil {
		return map[string]interface{}{
			"type":       "OBJECT",
			"properties": map[string]interface{}{},
		}
	}
	if t, ok := res["type"].(string); !ok || t == "" {
		res["type"] = "OBJECT"
	}
	if res["type"] == "OBJECT" {
		if _, ok := res["properties"]; !ok {
			res["properties"] = map[string]interface{}{}
		}
	}
	return res
}

func normalizeSchemaType(t string) string {
	upper := strings.ToUpper(t)
	switch upper {
	case "INT":
		return "INTEGER"
	case "BOOL":
		return "BOOLEAN"
	case "FLOAT", "DOUBLE":
		return "NUMBER"
	default:
		return upper
	}
}

func cleanSchemaNode(schema map[string]interface{}) map[string]interface{} {
	if schema == nil {
		return map[string]interface{}{
			"type": "OBJECT",
		}
	}

	cp := make(map[string]interface{}, len(schema))
	for k, v := range schema {
		switch strings.ToLower(k) {
		case "additionalproperties", "$schema", "title", "default", "$ref", "$id", "definitions":
			continue
		}
		cp[k] = v
	}

	// Polymorphic type arrays & scalar type normalization
	if types, ok := cp["type"].([]interface{}); ok {
		isNullable := false
		var primaryType string
		for _, item := range types {
			if s, ok := item.(string); ok {
				if strings.EqualFold(s, "null") {
					isNullable = true
				} else if primaryType == "" {
					primaryType = normalizeSchemaType(s)
				}
			}
		}
		if primaryType != "" {
			cp["type"] = primaryType
		} else {
			cp["type"] = "OBJECT"
		}
		if isNullable {
			cp["nullable"] = true
		}
	} else if types, ok := cp["type"].([]string); ok {
		isNullable := false
		var primaryType string
		for _, s := range types {
			if strings.EqualFold(s, "null") {
				isNullable = true
			} else if primaryType == "" {
				primaryType = normalizeSchemaType(s)
			}
		}
		if primaryType != "" {
			cp["type"] = primaryType
		} else {
			cp["type"] = "OBJECT"
		}
		if isNullable {
			cp["nullable"] = true
		}
	} else if t, ok := cp["type"].(string); ok {
		if strings.EqualFold(t, "null") {
			cp["type"] = "OBJECT"
			cp["nullable"] = true
		} else {
			cp["type"] = normalizeSchemaType(t)
		}
	}

	// Flatten simple nullable unions in anyOf / oneOf
	flattenNullableUnion := func(unionKey string) bool {
		val, ok := cp[unionKey]
		if !ok {
			return false
		}
		var items []map[string]interface{}
		switch v := val.(type) {
		case []interface{}:
			for _, elem := range v {
				if m, ok := elem.(map[string]interface{}); ok {
					items = append(items, m)
				}
			}
			if len(items) != len(v) {
				return false
			}
		case []map[string]interface{}:
			items = v
		default:
			return false
		}
		if len(items) != 2 {
			return false
		}
		isNull := func(m map[string]interface{}) bool {
			if t, ok := m["type"].(string); ok && strings.EqualFold(t, "null") {
				return true
			}
			return false
		}
		var nonNull map[string]interface{}
		if isNull(items[0]) && !isNull(items[1]) {
			nonNull = items[1]
		} else if isNull(items[1]) && !isNull(items[0]) {
			nonNull = items[0]
		} else {
			return false
		}

		delete(cp, unionKey)
		cleanedNonNull := cleanSchemaNode(nonNull)
		for k, v := range cleanedNonNull {
			if _, exists := cp[k]; !exists {
				cp[k] = v
			} else if k == "type" {
				cp[k] = v
			}
		}
		if t, ok := cleanedNonNull["type"]; ok {
			cp["type"] = t
		}
		cp["nullable"] = true
		return true
	}

	for _, unionKey := range []string{"anyOf", "oneOf"} {
		flattenNullableUnion(unionKey)
	}

	// Recurse through properties
	if propsVal, ok := cp["properties"]; ok {
		if propsMap, ok := propsVal.(map[string]interface{}); ok {
			cleanedProps := make(map[string]interface{}, len(propsMap))
			for propName, propSchema := range propsMap {
				if propMap, ok := propSchema.(map[string]interface{}); ok {
					cleanedProps[propName] = cleanSchemaNode(propMap)
				} else {
					cleanedProps[propName] = propSchema
				}
			}
			cp["properties"] = cleanedProps
		}
	}

	// Recurse through items
	if itemsVal, ok := cp["items"]; ok {
		if itemsMap, ok := itemsVal.(map[string]interface{}); ok {
			cp["items"] = cleanSchemaNode(itemsMap)
		} else if itemsSlice, ok := itemsVal.([]interface{}); ok {
			cleanedItems := make([]interface{}, 0, len(itemsSlice))
			for _, elem := range itemsSlice {
				if elemMap, ok := elem.(map[string]interface{}); ok {
					cleanedItems = append(cleanedItems, cleanSchemaNode(elemMap))
				} else {
					cleanedItems = append(cleanedItems, elem)
				}
			}
			cp["items"] = cleanedItems
		}
	}

	// Recurse through remaining unflattened anyOf, oneOf, allOf
	for _, unionKey := range []string{"anyOf", "oneOf", "allOf"} {
		if val, ok := cp[unionKey]; ok {
			if slice, ok := val.([]interface{}); ok {
				cleanedSlice := make([]interface{}, 0, len(slice))
				for _, elem := range slice {
					if elemMap, ok := elem.(map[string]interface{}); ok {
						cleanedSlice = append(cleanedSlice, cleanSchemaNode(elemMap))
					} else {
						cleanedSlice = append(cleanedSlice, elem)
					}
				}
				cp[unionKey] = cleanedSlice
			} else if slice, ok := val.([]map[string]interface{}); ok {
				cleanedSlice := make([]interface{}, 0, len(slice))
				for _, elemMap := range slice {
					cleanedSlice = append(cleanedSlice, cleanSchemaNode(elemMap))
				}
				cp[unionKey] = cleanedSlice
			}
		}
	}

	// Default empty schemas or missing types to OBJECT
	if t, ok := cp["type"].(string); !ok || t == "" {
		if _, hasAnyOf := cp["anyOf"]; !hasAnyOf {
			if _, hasOneOf := cp["oneOf"]; !hasOneOf {
				if _, hasAllOf := cp["allOf"]; !hasAllOf {
					cp["type"] = "OBJECT"
				}
			}
		}
	}
	if cp["type"] == "OBJECT" {
		if _, ok := cp["properties"]; !ok {
			cp["properties"] = map[string]interface{}{}
		}
	}

	return cp
}

// isMissingSessionIDErr determines if a response indicates a missing or expired MCP session ID.
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

// DefaultMCPDispatcher discovers and dispatches tools across configured HTTP/SSE MCP servers.
type DefaultMCPDispatcher struct {
	d          *mcp.DefaultDispatcher
	toolRoutes map[string]MCPServerConfig
}

var _ MCPDispatcher = (*DefaultMCPDispatcher)(nil)

func NewDefaultMCPDispatcher(servers []MCPServerConfig, client *http.Client, allowedTools ...[]string) *DefaultMCPDispatcher {
	return &DefaultMCPDispatcher{
		d:          mcp.NewDispatcher(servers, client, allowedTools...),
		toolRoutes: make(map[string]MCPServerConfig),
	}
}

func (d *DefaultMCPDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	if d == nil || d.d == nil {
		return nil, nil
	}
	tools, err := d.d.ListTools(ctx)
	if err != nil {
		return nil, err
	}
	var decls []geminiFunctionDeclaration
	for _, t := range tools {
		decls = append(decls, geminiFunctionDeclaration{
			Name:        t.Name,
			Description: t.Description,
			Parameters:  cleanParameters(t.InputSchema),
		})
	}
	return decls, nil
}

func (d *DefaultMCPDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	if d == nil || d.d == nil {
		return "", errors.New("mcp dispatcher is nil")
	}
	if d.toolRoutes != nil {
		if srv, ok := d.toolRoutes[name]; ok {
			d.d.RegisterToolRoute(name, srv)
		}
	}
	res, err := d.d.Execute(ctx, name, args)
	if err != nil {
		if strings.Contains(err.Error(), "is not in the allowed tools list") {
			return "", fmt.Errorf("tool %q is not in the allowed voice tools list", name)
		}
		return res, err
	}
	return res, nil
}

func (d *DefaultMCPDispatcher) Prime(ctx context.Context) error {
	if d == nil || d.d == nil {
		return nil
	}
	return d.d.Prime(ctx)
}

// GeminiAPIPoolConfig holds configuration for the GeminiAPIPool.
type GeminiAPIPoolConfig struct {
	APIKey           string
	Model            string
	BaseURL          string // defaults to "https://generativelanguage.googleapis.com"
	HTTPClient       *http.Client
	PrewarmedTargets []string
	SystemPrompt     string
	MaxHistoryTurns  int // defaults to 10 (20 messages)
	MCPServers       []MCPServerConfig
	MCPDispatcher    MCPDispatcher
	AllowedTools     []string
	DataDir                 string
	MemoryRetriever         MemoryRetriever
	AmbientContextRetriever AmbientContextRetriever
	OnSessionRotated        func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error
	GetSessionRecord        func(ctx context.Context, targetKey string) (SessionRecord, error)
	MaxSessionTurns         int
	TargetLockTimeout       time.Duration
}

// geminiFunctionCall represents a function call requested by the model.
type geminiFunctionCall struct {
	ID   string                 `json:"id,omitempty"`
	Name string                 `json:"name"`
	Args map[string]interface{} `json:"args,omitempty"`
}

// geminiFunctionResp represents the execution outcome of a function call.
type geminiFunctionResp struct {
	Name     string                 `json:"name"`
	Response map[string]interface{} `json:"response"`
}

// geminiPart represents a content part in the Gemini REST API.
type geminiPart struct {
	Text             string              `json:"text,omitempty"`
	FunctionCall     *geminiFunctionCall `json:"functionCall,omitempty"`
	FunctionResponse *geminiFunctionResp `json:"functionResponse,omitempty"`
	ThoughtSignature string              `json:"thoughtSignature,omitempty"`
}

// geminiContent represents a message content block in the Gemini REST API.
type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

// geminiFunctionDeclaration models a callable tool schema for the model.
type geminiFunctionDeclaration struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
}

// geminiTool wraps declarations in the Gemini REST API tools array.
type geminiTool struct {
	FunctionDeclarations []geminiFunctionDeclaration `json:"functionDeclarations,omitempty"`
}

// geminiThinkingConfig controls thinking budget in generationConfig.
type geminiThinkingConfig struct {
	ThinkingBudget int `json:"thinkingBudget"`
}

// geminiGenerationConfig models the generationConfig in streamGenerateContent requests.
type geminiGenerationConfig struct {
	ThinkingConfig *geminiThinkingConfig `json:"thinkingConfig,omitempty"`
}

// geminiStreamRequest is the JSON payload sent to streamGenerateContent.
type geminiStreamRequest struct {
	Contents          []geminiContent         `json:"contents"`
	Tools             []geminiTool            `json:"tools,omitempty"`
	SystemInstruction *geminiContent          `json:"systemInstruction,omitempty"`
	GenerationConfig  *geminiGenerationConfig `json:"generationConfig,omitempty"`
}

// geminiCandidate represents a single generation candidate in the stream chunk.
type geminiCandidate struct {
	Content      *geminiContent `json:"content,omitempty"`
	FinishReason string         `json:"finishReason,omitempty"`
}

// geminiUsageMetadata models the token usage statistics returned in SSE chunks.
type geminiUsageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
	TotalTokenCount      int `json:"totalTokenCount"`
}

// geminiStreamChunk represents an SSE chunk payload from the Gemini API.
type geminiStreamChunk struct {
	Candidates    []geminiCandidate    `json:"candidates,omitempty"`
	UsageMetadata *geminiUsageMetadata `json:"usageMetadata,omitempty"`
}

// GeminiAPIPool manages direct REST-based sessions for target devices.
type GeminiAPIPool struct {
	cfg         GeminiAPIPoolConfig
	sessions    map[string]*GeminiAPISession
	mu          sync.RWMutex
	targetLocks map[string]*targetLock
	targetMu    sync.Mutex
	closed      bool
}

var (
	_ AgentPool       = (*GeminiAPIPool)(nil)
	_ SessionRotator  = (*GeminiAPIPool)(nil)
	_ LeasedAgentPool = (*GeminiAPIPool)(nil)
)

var (
	_ AgentPool      = (*GeminiAPIPool)(nil)
	_ SessionRotator = (*GeminiAPIPool)(nil)
)

// NewGeminiAPIPool instantiates a GeminiAPIPool with sensible defaults.
func NewGeminiAPIPool(cfg GeminiAPIPoolConfig) *GeminiAPIPool {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://generativelanguage.googleapis.com"
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	if cfg.MaxHistoryTurns <= 0 {
		cfg.MaxHistoryTurns = 10
	}
	if cfg.MCPDispatcher == nil && len(cfg.MCPServers) > 0 {
		cfg.MCPDispatcher = NewDefaultMCPDispatcher(cfg.MCPServers, cfg.HTTPClient, cfg.AllowedTools)
	}
	if disp, ok := cfg.MCPDispatcher.(*DefaultMCPDispatcher); ok && disp != nil {
		go func() {
			primeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := disp.Prime(primeCtx); err != nil {
				log.Printf("[MCPDispatcher] Prime warning: %v", err)
			}
		}()
	}
	if strings.TrimSpace(cfg.SystemPrompt) == "" && cfg.DataDir != "" {
		rulesDir := filepath.Join(cfg.DataDir, "runtimes", "voice", ".gemini", "config", "rules")
		if entries, err := os.ReadDir(rulesDir); err == nil && len(entries) > 0 {
			var sb strings.Builder
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
					content, rErr := os.ReadFile(filepath.Join(rulesDir, entry.Name()))
					if rErr == nil && len(content) > 0 {
						if sb.Len() > 0 {
							sb.WriteString("\n\n")
						}
						sb.Write(content)
					}
				}
			}
			cfg.SystemPrompt = sb.String()
		}
	}
	return &GeminiAPIPool{
		cfg:         cfg,
		sessions:    make(map[string]*GeminiAPISession),
		targetLocks: make(map[string]*targetLock),
	}
}

// Model returns the configured model for Gemini API generation.
func (p *GeminiAPIPool) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

// APIKey returns the configured API key.
func (p *GeminiAPIPool) APIKey() string {
	if p == nil {
		return ""
	}
	return p.cfg.APIKey
}

// sanitizeError replaces API keys and secret query parameters with [REDACTED].
func (p *GeminiAPIPool) sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if p != nil && p.cfg.APIKey != "" {
		msg = strings.ReplaceAll(msg, p.cfg.APIKey, "[REDACTED]")
	}
	msg = apiKeyQueryRegex.ReplaceAllString(msg, "$1[REDACTED]")
	return errors.New(msg)
}

// GetOrCreateSession retrieves an existing session or initializes a new one for targetKey.
func (p *GeminiAPIPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errors.New("gemini api pool is closed")
	}

	if sess, ok := p.sessions[targetKey]; ok {
		return sess, nil
	}

	sessID := targetKey
	if trimmed := strings.TrimSpace(sessionID); trimmed != "" {
		sessID = trimmed
	}

	sess := &GeminiAPISession{
		targetKey: targetKey,
		sessionID: sessID,
		pool:      p,
		history:   make([]geminiContent, 0),
		dataDir:   p.cfg.DataDir,
	}
	p.sessions[targetKey] = sess
	return sess, nil
}

// ShouldRotateSession reports whether the session needs rotation.
func (p *GeminiAPIPool) ShouldRotateSession(sess AgentSession) (bool, string) {
	return false, ""
}

// RotateSession retrieves or initializes the session for targetKey.
func (p *GeminiAPIPool) RotateSession(ctx context.Context, targetKey string) (AgentSession, error) {
	return p.GetOrCreateSession(ctx, targetKey, "")
}

// Initialize pre-warms configured target sessions.
func (p *GeminiAPIPool) Initialize(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("gemini api pool is closed")
	}

	for _, target := range p.cfg.PrewarmedTargets {
		if _, ok := p.sessions[target]; !ok {
			p.sessions[target] = &GeminiAPISession{
				targetKey: target,
				sessionID: target,
				pool:      p,
				history:   make([]geminiContent, 0),
				dataDir:   p.cfg.DataDir,
			}
		}
	}
	return nil
}

// PrewarmedTargets returns a defensive copy of configured prewarmed targets.
func (p *GeminiAPIPool) PrewarmedTargets() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.cfg.PrewarmedTargets == nil {
		return nil
	}
	targets := make([]string, len(p.cfg.PrewarmedTargets))
	copy(targets, p.cfg.PrewarmedTargets)
	return targets
}

// UpdatePrewarmedTargets updates the prewarmed targets list in configuration.
func (p *GeminiAPIPool) UpdatePrewarmedTargets(targets []string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.cfg.PrewarmedTargets = targets
}

// MarkDirty resets active sessions so subsequent requests create fresh sessions.
func (p *GeminiAPIPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.sessions = make(map[string]*GeminiAPISession)
}

// Close terminates the pool and marks it as closed.
func (p *GeminiAPIPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	return nil
}

// GeminiAPISession represents an active conversation session with sliding window history and transcript logging.
type GeminiAPISession struct {
	targetKey string
	sessionID string
	pool      *GeminiAPIPool
	mu        sync.Mutex
	history   []geminiContent
	dataDir   string
}

var _ AgentSession = (*GeminiAPISession)(nil)

// SessionID returns the identifier for this session.
func (s *GeminiAPISession) SessionID() string {
	if s.sessionID != "" {
		return s.sessionID
	}
	return s.targetKey
}

// TargetKey returns the target device key for this session.
func (s *GeminiAPISession) TargetKey() string {
	return s.targetKey
}

// History returns a copy of the session's conversation history.
func (s *GeminiAPISession) History() []geminiContent {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]geminiContent, len(s.history))
	copy(cp, s.history)
	return cp
}

func (s *GeminiAPISession) transcriptPath() string {
	if s.dataDir == "" {
		return ""
	}
	return filepath.Join(s.dataDir, "brain", s.SessionID(), ".system_generated", "logs", "transcript.jsonl")
}

// hydrateHistory populates in-memory history from persistent transcript if available.
func (s *GeminiAPISession) hydrateHistory() {
	p := s.transcriptPath()
	if p == "" {
		return
	}
	data, err := os.ReadFile(p)
	if err != nil || len(data) == 0 {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var loaded []geminiContent
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed == "" {
			continue
		}
		var entry struct {
			Type    string `json:"type"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal([]byte(trimmed), &entry); err == nil {
			if entry.Type == "USER_INPUT" && entry.Content != "" {
				loaded = append(loaded, geminiContent{
					Role:  "user",
					Parts: []geminiPart{{Text: entry.Content}},
				})
			} else if entry.Type == "PLANNER_RESPONSE" && entry.Content != "" {
				loaded = append(loaded, geminiContent{
					Role:  "model",
					Parts: []geminiPart{{Text: entry.Content}},
				})
			}
		}
	}
	if len(loaded) > 0 {
		maxMessages := 20
		if s.pool != nil && s.pool.cfg.MaxHistoryTurns > 0 {
			maxMessages = s.pool.cfg.MaxHistoryTurns * 2
		}
		if len(loaded) > maxMessages {
			loaded = loaded[len(loaded)-maxMessages:]
		}
		s.history = loaded
	}
}

// appendTranscript logs turns to transcript.jsonl for state persistence and recovery.
func (s *GeminiAPISession) appendTranscript(userPrompt, modelResponse string) {
	p := s.transcriptPath()
	if p == "" {
		return
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[GeminiAPISession] Warning creating transcript directory %s: %v", dir, err)
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)
	userEntry, err := json.Marshal(map[string]interface{}{
		"source":     "USER_EXPLICIT",
		"type":       "USER_INPUT",
		"content":    userPrompt,
		"created_at": now,
	})
	if err != nil {
		return
	}
	modelEntry, err := json.Marshal(map[string]interface{}{
		"source":     "MODEL",
		"type":       "PLANNER_RESPONSE",
		"content":    modelResponse,
		"created_at": now,
	})
	if err != nil {
		return
	}

	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("[GeminiAPISession] Warning opening transcript file %s: %v", p, err)
		return
	}
	defer func() {
		if cErr := f.Close(); cErr != nil {
			log.Printf("[GeminiAPISession] Warning closing transcript file: %v", cErr)
		}
	}()

	if _, wErr := f.Write(append(userEntry, '\n')); wErr != nil {
		log.Printf("[GeminiAPISession] Warning writing user transcript: %v", wErr)
	}
	if _, wErr := f.Write(append(modelEntry, '\n')); wErr != nil {
		log.Printf("[GeminiAPISession] Warning writing model transcript: %v", wErr)
	}
}

// Send submits a prompt to Gemini via streamGenerateContent SSE, handling function calling and streaming deltas.
func (s *GeminiAPISession) Send(prompt string, turn *TurnContext) error {
	if s.pool != nil {
		s.pool.mu.RLock()
		closed := s.pool.closed
		s.pool.mu.RUnlock()
		if closed {
			err := errors.New("gemini api pool is closed")
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(err)
			}
			return err
		}
	}

	if strings.TrimSpace(prompt) == "" {
		err := errors.New("empty prompt")
		if turn != nil && turn.Sink != nil {
			turn.Sink.OnError(err)
		}
		return err
	}

	ctx := context.Background()
	if turn != nil && turn.Ctx != nil {
		ctx = turn.Ctx
	}
	if err := ctx.Err(); err != nil {
		sanitizedErr := s.pool.sanitizeError(err)
		if turn != nil && turn.Sink != nil {
			turn.Sink.OnError(sanitizedErr)
		}
		return sanitizedErr
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Hydrate history from persistent transcript if in-memory history is empty
	if len(s.history) == 0 {
		s.hydrateHistory()
	}

	// Retrieve function declarations from MCPDispatcher if available
	var tools []geminiTool
	var dispatcher MCPDispatcher
	if s.pool != nil && s.pool.cfg.MCPDispatcher != nil {
		dispatcher = s.pool.cfg.MCPDispatcher
		decls, dErr := dispatcher.ListDeclarations(ctx)
		if dErr != nil {
			log.Printf("[GeminiAPISession] Warning: failed to list MCP declarations: %v", dErr)
		} else if len(decls) > 0 {
			tools = []geminiTool{{FunctionDeclarations: decls}}
		}
	}

	// Prepare conversation contents with user prompt
	payloadPrompt := prompt
	if s.pool != nil && s.pool.cfg.AmbientContextRetriever != nil && !strings.Contains(prompt, "<ambient_context>") {
		ambCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		ambBlock, aErr := s.pool.cfg.AmbientContextRetriever(ambCtx)
		cancel()
		if aErr != nil {
			log.Printf("[GeminiAPISession] Warning: AmbientContextRetriever failed: %v", aErr)
		} else if strings.TrimSpace(ambBlock) != "" {
			payloadPrompt = strings.TrimSpace(ambBlock) + "\n\n" + payloadPrompt
		}
	}
	if s.pool != nil && s.pool.cfg.MemoryRetriever != nil && !strings.Contains(prompt, "<retrieved_memory>") {
		memCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		memBlock, mErr := s.pool.cfg.MemoryRetriever(memCtx, prompt)
		cancel()
		if mErr != nil {
			log.Printf("[GeminiAPISession] Warning: MemoryRetriever failed: %v", mErr)
		} else if strings.TrimSpace(memBlock) != "" {
			payloadPrompt = strings.TrimSpace(memBlock) + "\n\n" + payloadPrompt
		}
	}

	workingContents := make([]geminiContent, len(s.history), len(s.history)+1)
	copy(workingContents, s.history)
	workingContents = append(workingContents, geminiContent{
		Role:  "user",
		Parts: []geminiPart{{Text: payloadPrompt}},
	})

	var systemInstruction *geminiContent
	if s.pool != nil && s.pool.cfg.SystemPrompt != "" {
		systemInstruction = &geminiContent{
			Parts: []geminiPart{
				{Text: s.pool.cfg.SystemPrompt},
			},
		}
	}

	baseURL := "https://generativelanguage.googleapis.com"
	apiKey := ""
	model := ""
	if s.pool != nil {
		if s.pool.cfg.BaseURL != "" {
			baseURL = strings.TrimRight(s.pool.cfg.BaseURL, "/")
		}
		apiKey = s.pool.cfg.APIKey
		model = s.pool.cfg.Model
	}

	reqURL := fmt.Sprintf("%s/v1beta/models/%s:streamGenerateContent?key=%s&alt=sse", baseURL, model, url.QueryEscape(apiKey))
	client := http.DefaultClient
	if s.pool != nil && s.pool.cfg.HTTPClient != nil {
		client = s.pool.cfg.HTTPClient
	}

	const maxToolIterations = 5
	var fullText strings.Builder
	var totalUsage AgyUsage
	started := false

	for iteration := 0; iteration < maxToolIterations; iteration++ {
		reqPayload := geminiStreamRequest{
			Contents:          workingContents,
			Tools:             tools,
			SystemInstruction: systemInstruction,
			GenerationConfig: &geminiGenerationConfig{
				ThinkingConfig: &geminiThinkingConfig{
					ThinkingBudget: 0,
				},
			},
		}

		bodyBytes, err := json.Marshal(reqPayload)
		if err != nil {
			sanitizedErr := s.pool.sanitizeError(fmt.Errorf("failed to marshal request: %w", err))
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(sanitizedErr)
			}
			return sanitizedErr
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
		if err != nil {
			sanitizedErr := s.pool.sanitizeError(fmt.Errorf("failed to create http request: %w", err))
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(sanitizedErr)
			}
			return sanitizedErr
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		if err != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			sanitizedErr := s.pool.sanitizeError(err)
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(sanitizedErr)
			}
			return sanitizedErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			body, rErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			targetModel := s.pool.cfg.Model
			if targetModel == "" {
				targetModel = "default"
			}
			var bodyStr string
			if rErr == nil {
				bodyStr = string(body)
			}
			if IsQuotaPause(bodyStr, "") {
				metrics.RecordRunnerError("quota_paused", targetModel)
			} else {
				metrics.RecordRunnerError("capacity_throttle", targetModel)
			}
			rateLimitErr := errors.New("rate limit exceeded: please try again shortly")
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(rateLimitErr)
			}
			return rateLimitErr
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, rErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			var bodyStr string
			if rErr != nil {
				bodyStr = fmt.Sprintf("<error reading response body: %v>", rErr)
			} else {
				bodyStr = string(body)
			}
			rawErr := fmt.Errorf("gemini api returned status %d: %s", resp.StatusCode, bodyStr)
			sanitizedErr := s.pool.sanitizeError(rawErr)
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(sanitizedErr)
			}
			return sanitizedErr
		}

		reader := bufio.NewReader(resp.Body)
		var pendingParts []geminiPart
		var currentTurnText strings.Builder

		for {
			line, rErr := reader.ReadBytes('\n')
			if len(line) > 0 {
				trimmed := strings.TrimRight(string(line), "\r\n")
				if strings.HasPrefix(trimmed, "data:") {
					data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
					if data != "" && data != "[DONE]" {
						var chunk geminiStreamChunk
						if uErr := json.Unmarshal([]byte(data), &chunk); uErr != nil {
							_ = resp.Body.Close()
							sanitizedErr := s.pool.sanitizeError(fmt.Errorf("failed to unmarshal SSE chunk: %w", uErr))
							if turn != nil && turn.Sink != nil {
								turn.Sink.OnError(sanitizedErr)
							}
							return sanitizedErr
						}

						for _, cand := range chunk.Candidates {
							if cand.Content != nil {
								for _, part := range cand.Content.Parts {
									if part.FunctionCall != nil {
										pendingParts = append(pendingParts, part)
									}
									if part.Text != "" {
										if !started {
											started = true
											if turn != nil && turn.Sink != nil {
												turn.Sink.OnTurnStarted()
											}
										}
										if turn != nil && turn.Sink != nil {
											turn.Sink.OnTextDelta(part.Text)
										}
										currentTurnText.WriteString(part.Text)
										fullText.WriteString(part.Text)
									}
								}
							}
						}

						if chunk.UsageMetadata != nil {
							totalUsage.InputTokens += chunk.UsageMetadata.PromptTokenCount
							totalUsage.OutputTokens += chunk.UsageMetadata.CandidatesTokenCount
							totalUsage.TotalTokens += chunk.UsageMetadata.TotalTokenCount
							if totalUsage.TotalTokens == 0 && (totalUsage.InputTokens > 0 || totalUsage.OutputTokens > 0) {
								totalUsage.TotalTokens = totalUsage.InputTokens + totalUsage.OutputTokens
							}
						}
					}
				}
			}

			if rErr != nil {
				_ = resp.Body.Close()
				if errors.Is(rErr, io.EOF) {
					break
				}
				readErr := rErr
				if ctx.Err() != nil {
					readErr = ctx.Err()
				}
				sanitizedErr := s.pool.sanitizeError(readErr)
				if turn != nil && turn.Sink != nil {
					turn.Sink.OnError(sanitizedErr)
				}
				return sanitizedErr
			}
		}

		if ctx.Err() != nil {
			sanitizedErr := s.pool.sanitizeError(ctx.Err())
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(sanitizedErr)
			}
			return sanitizedErr
		}

		// If no function calls were requested, the model turn is complete
		if len(pendingParts) == 0 {
			break
		}

		// Propagate turn-level thoughtSignature to any function call parts lacking one.
		// Gemini SSE streams emit thoughtSignature on the first functionCall part, but
		// subsequent request validation requires all functionCall parts in the turn to carry it.
		var turnThoughtSignature string
		for _, p := range pendingParts {
			if p.ThoughtSignature != "" {
				turnThoughtSignature = p.ThoughtSignature
				break
			}
		}
		if turnThoughtSignature != "" {
			for i := range pendingParts {
				if pendingParts[i].ThoughtSignature == "" {
					pendingParts[i].ThoughtSignature = turnThoughtSignature
				}
			}
		}

		// Dispatch function calls via MCPDispatcher
		modelParts := make([]geminiPart, 0, len(pendingParts))
		funcParts := make([]geminiPart, 0, len(pendingParts))

		for _, p := range pendingParts {
			fc := p.FunctionCall
			modelParts = append(modelParts, p)

			argsDesc := ""
			if len(fc.Args) > 0 {
				if b, bErr := json.Marshal(fc.Args); bErr == nil {
					argsDesc = string(b)
				}
			}
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnToolCall(fc.Name, argsDesc)
			}

			start := time.Now()
			var toolOutput string
			var execErr error
			if dispatcher != nil {
				toolOutput, execErr = dispatcher.Execute(ctx, fc.Name, fc.Args)
			} else {
				execErr = errors.New("no mcp dispatcher configured")
			}
			duration := time.Since(start)
			status := "ok"
			if execErr != nil {
				status = "error"
			}
			canonicalTool, mcpServer := ExtractMCPToolInfo(fc.Name, fc.Args)
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnToolCompleted(canonicalTool, mcpServer, duration, status)
			}

			respMap := map[string]interface{}{"output": toolOutput}
			if execErr != nil {
				respMap = map[string]interface{}{"error": execErr.Error()}
			}

			funcParts = append(funcParts, geminiPart{
				FunctionResponse: &geminiFunctionResp{
					Name:     fc.Name,
					Response: respMap,
				},
			})
		}

		workingContents = append(workingContents,
			geminiContent{
				Role:  "model",
				Parts: modelParts,
			},
			geminiContent{
				Role:  "user",
				Parts: funcParts,
			},
		)
	}

	// If the loop finished executing tool iterations but no text was produced,
	// make one final completion pass without tools to prompt the model to speak/summarize.
	if strings.TrimSpace(fullText.String()) == "" && len(workingContents) > 1 {
		finalPayload := geminiStreamRequest{
			Contents:          workingContents,
			SystemInstruction: systemInstruction,
			GenerationConfig: &geminiGenerationConfig{
				ThinkingConfig: &geminiThinkingConfig{
					ThinkingBudget: 0,
				},
			},
		}

		if finalBytes, err := json.Marshal(finalPayload); err == nil {
			if finalHttpReq, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(finalBytes)); reqErr == nil {
				finalHttpReq.Header.Set("Content-Type", "application/json")
				if finalResp, respErr := client.Do(finalHttpReq); respErr == nil {
					if finalResp.StatusCode >= 200 && finalResp.StatusCode < 300 {
						reader := bufio.NewReader(finalResp.Body)
						for {
							line, rErr := reader.ReadBytes('\n')
							if len(line) > 0 {
								trimmed := strings.TrimRight(string(line), "\r\n")
								if strings.HasPrefix(trimmed, "data:") {
									data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
									if data != "" && data != "[DONE]" {
										var chunk geminiStreamChunk
										if uErr := json.Unmarshal([]byte(data), &chunk); uErr == nil {
											for _, cand := range chunk.Candidates {
												if cand.Content != nil {
													for _, part := range cand.Content.Parts {
														if part.Text != "" {
															if !started {
																started = true
																if turn != nil && turn.Sink != nil {
																	turn.Sink.OnTurnStarted()
																}
															}
															if turn != nil && turn.Sink != nil {
																turn.Sink.OnTextDelta(part.Text)
															}
															fullText.WriteString(part.Text)
														}
													}
												}
											}
											if chunk.UsageMetadata != nil {
												totalUsage.InputTokens += chunk.UsageMetadata.PromptTokenCount
												totalUsage.OutputTokens += chunk.UsageMetadata.CandidatesTokenCount
												totalUsage.TotalTokens += chunk.UsageMetadata.TotalTokenCount
											}
										}
									}
								}
							}
							if rErr != nil {
								_ = finalResp.Body.Close()
								break
							}
						}
					} else {
						_ = finalResp.Body.Close()
					}
				}
			}
		}
	}

	resText := fullText.String()

	// Append user prompt and model response to history only if non-empty,
	// preventing empty part serialization ({}) which corrupts future Gemini requests.
	if strings.TrimSpace(resText) != "" {
		s.history = append(s.history,
			geminiContent{
				Role:  "user",
				Parts: []geminiPart{{Text: prompt}},
			},
			geminiContent{
				Role:  "model",
				Parts: []geminiPart{{Text: resText}},
			},
		)
	}

	// Trim history to MaxHistoryTurns*2 messages
	maxTurns := 10
	if s.pool != nil && s.pool.cfg.MaxHistoryTurns > 0 {
		maxTurns = s.pool.cfg.MaxHistoryTurns
	}
	maxMessages := maxTurns * 2
	if len(s.history) > maxMessages {
		s.history = s.history[len(s.history)-maxMessages:]
	}

	// Persist turn transcript to disk
	s.appendTranscript(prompt, resText)

	if turn != nil && turn.Sink != nil {
		turn.Sink.OnResult(&TurnResult{
			Response: resText,
			Usage:    totalUsage,
		})
	}

	return nil
}

func (p *GeminiAPIPool) acquireTargetLock(ctx context.Context, targetKey string) (*targetLock, error) {
	p.targetMu.Lock()
	if p.targetLocks == nil {
		p.targetLocks = make(map[string]*targetLock)
	}
	tl, ok := p.targetLocks[targetKey]
	if !ok {
		tl = &targetLock{
			sem: make(chan struct{}, 1),
		}
		p.targetLocks[targetKey] = tl
	}
	tl.refs++
	p.targetMu.Unlock()

	timeout := p.cfg.TargetLockTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	lockCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case tl.sem <- struct{}{}:
		return tl, nil
	case <-lockCtx.Done():
		p.releaseTargetLock(targetKey, tl, false)
		return nil, fmt.Errorf("timeout waiting for target lock for %s: %w", targetKey, lockCtx.Err())
	}
}

func (p *GeminiAPIPool) releaseTargetLock(targetKey string, tl *targetLock, held bool) {
	if held {
		select {
		case <-tl.sem:
		default:
		}
	}
	p.targetMu.Lock()
	tl.refs--
	if tl.refs <= 0 {
		delete(p.targetLocks, targetKey)
	}
	p.targetMu.Unlock()
}

// AcquireLease blocks until the target lock is acquired, ensures a session is initialized,
// and returns an exclusive SessionLease.
func (p *GeminiAPIPool) AcquireLease(ctx context.Context, targetKey string) (SessionLease, error) {
	if p == nil {
		return nil, errors.New("gemini api pool is uninitialized")
	}
	trimmedKey := strings.TrimSpace(targetKey)
	if trimmedKey == "" {
		return nil, errors.New("targetKey cannot be empty")
	}

	tl, err := p.acquireTargetLock(ctx, trimmedKey)
	if err != nil {
		return nil, err
	}

	var rec SessionRecord
	if p.cfg.GetSessionRecord != nil {
		if r, rErr := p.cfg.GetSessionRecord(ctx, trimmedKey); rErr == nil {
			rec = r
		}
	}

	sess, sErr := p.GetOrCreateSession(ctx, trimmedKey, rec.ActiveSessionID)
	if sErr != nil {
		p.releaseTargetLock(trimmedKey, tl, true)
		return nil, fmt.Errorf("failed to get or create session for %s: %w", trimmedKey, sErr)
	}

	gSess, ok := sess.(*GeminiAPISession)
	if !ok {
		p.releaseTargetLock(trimmedKey, tl, true)
		return nil, fmt.Errorf("unexpected session type for %s", trimmedKey)
	}

	isCold := false
	turnCount := 0
	if rec.TurnCount <= 1 || rec.ActiveSessionID == "" {
		isCold = true
	} else {
		turnCount = rec.TurnCount
	}

	sessID := gSess.SessionID()
	if sessID == "" {
		sessID = rec.ActiveSessionID
	}

	lease := &geminiSessionLease{
		pool:       p,
		session:    gSess,
		targetKey:  trimmedKey,
		sessionID:  sessID,
		prevSessID: rec.PreviousSessionID,
		isCold:     isCold,
		turnCount:  turnCount,
		lock:       tl,
	}
	return lease, nil
}

type geminiSessionLease struct {
	pool        *GeminiAPIPool
	session     *GeminiAPISession
	targetKey   string
	sessionID   string
	prevSessID  string
	isCold      bool
	turnCount      int
	cancelRotation bool
	lock           *targetLock
	releaseOnce    sync.Once
}

var _ SessionLease = (*geminiSessionLease)(nil)

func (l *geminiSessionLease) SessionID() string {
	return l.sessionID
}

func (l *geminiSessionLease) PreviousSessionID() string {
	return l.prevSessID
}

func (l *geminiSessionLease) IsCold() bool {
	return l.isCold
}

func (l *geminiSessionLease) TurnCount() int {
	return l.turnCount
}

func (l *geminiSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	if turn == nil {
		return nil, errors.New("turn context cannot be nil")
	}
	if turn.Ctx == nil {
		turn.Ctx = ctx
	}

	compiledPrompt := turn.Prompt
	if l.isCold {
		scopeInst := strings.TrimSpace(turn.ScopeInstructions)
		if scopeInst == "" {
			scopeInst = strings.TrimSpace(turn.ChannelInstructions)
		}
		if scopeInst != "" && !strings.Contains(compiledPrompt, "<SCOPE_INSTRUCTIONS>") && !strings.Contains(compiledPrompt, "<CHANNEL_INSTRUCTIONS>") {
			scopeBlock := fmt.Sprintf("<SCOPE_INSTRUCTIONS>\nScope-specific guidelines for this conversation:\n\n%s\n</SCOPE_INSTRUCTIONS>", scopeInst)
			compiledPrompt = scopeBlock + "\n\n" + compiledPrompt
		}
		if l.prevSessID != "" && !strings.Contains(compiledPrompt, "<PREVIOUS_SESSION>") {
			prevBlock := fmt.Sprintf("<PREVIOUS_SESSION>\nPrevious Session ID: %s\n</PREVIOUS_SESSION>", l.prevSessID)
			compiledPrompt = prevBlock + "\n\n" + compiledPrompt
		}
		if strings.TrimSpace(turn.ThreadSummary) != "" && !strings.Contains(compiledPrompt, "<THREAD_SUMMARY>") {
			sumBlock := fmt.Sprintf("<THREAD_SUMMARY>\n%s\n</THREAD_SUMMARY>", strings.TrimSpace(turn.ThreadSummary))
			compiledPrompt = sumBlock + "\n\n" + compiledPrompt
		}
	}
	if strings.TrimSpace(turn.CoordinationContext) != "" && !strings.Contains(compiledPrompt, "<COORDINATION_CONTEXT>") {
		coordBlock := fmt.Sprintf("<COORDINATION_CONTEXT>\n%s\n</COORDINATION_CONTEXT>", strings.TrimSpace(turn.CoordinationContext))
		compiledPrompt = coordBlock + "\n\n" + compiledPrompt
	}

	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	combinedSink := &leaseTurnSinkWrapper{
		inner: turn.Sink,
		buf:   bufSink,
	}
	origSink := turn.Sink
	turn.Sink = combinedSink
	defer func() {
		turn.Sink = origSink
	}()

	if err := l.session.Send(compiledPrompt, turn); err != nil {
		if IsQuotaPause(err.Error(), "") || IsCapacityBlip(err.Error(), "") {
			l.cancelRotation = true
		}
		return nil, err
	}

	res, waitErr := bufSink.Wait(ctx)
	if waitErr != nil {
		if IsQuotaPause(waitErr.Error(), "") || IsCapacityBlip(waitErr.Error(), "") {
			l.cancelRotation = true
		}
		return nil, waitErr
	}
	if res != nil && (IsQuotaPause(res.Stderr, "") || IsCapacityBlip(res.Stderr, "") || IsQuotaPause(res.Response, "") || IsCapacityBlip(res.Response, "")) {
		l.cancelRotation = true
	}
	l.turnCount++
	l.isCold = false
	return res, nil
}

func (l *geminiSessionLease) Release() error {
	var releaseErr error
	l.releaseOnce.Do(func() {
		defer l.pool.releaseTargetLock(l.targetKey, l.lock, true)

		if l.cancelRotation {
			return
		}

		shouldRotate, reason := l.pool.ShouldRotateSession(l.session)
		if !shouldRotate && l.pool.cfg.MaxSessionTurns > 0 && l.turnCount >= l.pool.cfg.MaxSessionTurns {
			shouldRotate = true
			reason = fmt.Sprintf("reached maximum turn threshold (%d)", l.pool.cfg.MaxSessionTurns)
		}

		if shouldRotate {
			log.Printf("[GeminiAPIPool] Session rotation triggered for %s (reason: %s)", l.targetKey, reason)
			newSessionID := uuid.New().String()
			oldSessionID := l.sessionID

			if l.pool.cfg.OnSessionRotated != nil {
				if rotErr := l.pool.cfg.OnSessionRotated(context.Background(), l.targetKey, oldSessionID, newSessionID); rotErr != nil {
					log.Printf("[GeminiAPIPool] Persistence callback failed during rotation for %s: %v. Resetting session to maintain DB truth.", l.targetKey, rotErr)
					l.pool.mu.Lock()
					delete(l.pool.sessions, l.targetKey)
					l.pool.mu.Unlock()
					releaseErr = rotErr
					return
				}
			}

			if _, rotErr := l.pool.RotateSession(context.Background(), l.targetKey); rotErr != nil {
				log.Printf("[GeminiAPIPool] Warning rotating session for %s: %v", l.targetKey, rotErr)
				l.pool.mu.Lock()
				delete(l.pool.sessions, l.targetKey)
				l.pool.mu.Unlock()
			}
		}
	})
	return releaseErr
}
