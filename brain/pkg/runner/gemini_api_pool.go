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

var apiKeyQueryRegex = regexp.MustCompile(`([?&]key=)[^&]+`)

type (
	MCPServerConfig = mcp.ServerConfig
	MCPTool         = mcp.Tool
	MCPDispatcher   interface {
		ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error)
		Execute(ctx context.Context, name string, args map[string]interface{}) (string, error)
	}
)

func cleanParameters(schema map[string]interface{}) map[string]interface{} {
	res := cleanSchemaNode(schema)
	if res == nil {
		res = map[string]interface{}{}
	}
	if t, ok := res["type"].(string); !ok || t == "" {
		res["type"] = "OBJECT"
	}
	if res["type"] == "OBJECT" && res["properties"] == nil {
		res["properties"] = map[string]interface{}{}
	}
	return res
}

func normalizeSchemaType(t string) string {
	switch upper := strings.ToUpper(t); upper {
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

func toStringSlice(v interface{}) ([]string, bool) {
	switch s := v.(type) {
	case []string:
		return s, true
	case []interface{}:
		res := make([]string, len(s))
		for i, elem := range s {
			str, ok := elem.(string)
			if !ok {
				return nil, false
			}
			res[i] = str
		}
		return res, true
	default:
		return nil, false
	}
}

func cleanSubSchemaList(val interface{}) ([]interface{}, []map[string]interface{}, bool) {
	switch v := val.(type) {
	case []map[string]interface{}:
		res := make([]interface{}, len(v))
		for i, m := range v {
			res[i] = cleanSchemaNode(m)
		}
		return res, v, true
	case []interface{}:
		res := make([]interface{}, len(v))
		maps := make([]map[string]interface{}, 0, len(v))
		for i, elem := range v {
			if m, ok := elem.(map[string]interface{}); ok {
				res[i] = cleanSchemaNode(m)
				maps = append(maps, m)
			} else {
				res[i] = elem
			}
		}
		if len(maps) != len(v) {
			maps = nil
		}
		return res, maps, true
	default:
		return nil, nil, false
	}
}

func cleanSchemaNode(schema map[string]interface{}) map[string]interface{} {
	if schema == nil {
		return map[string]interface{}{"type": "OBJECT"}
	}
	cp := make(map[string]interface{}, len(schema))
	for k, v := range schema {
		switch strings.ToLower(k) {
		case "additionalproperties", "$schema", "title", "default", "$ref", "$id", "definitions":
			continue
		}
		cp[k] = v
	}
	if types, ok := toStringSlice(cp["type"]); ok {
		var isNullable bool
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
			cp["type"], cp["nullable"] = "OBJECT", true
		} else {
			cp["type"] = normalizeSchemaType(t)
		}
	}
	for _, unionKey := range []string{"anyOf", "oneOf"} {
		if val, ok := cp[unionKey]; ok {
			if _, items, ok := cleanSubSchemaList(val); ok && len(items) == 2 {
				isNull := func(m map[string]interface{}) bool {
					t, ok := m["type"].(string)
					return ok && strings.EqualFold(t, "null")
				}
				var nonNull map[string]interface{}
				if isNull(items[0]) && !isNull(items[1]) {
					nonNull = items[1]
				} else if isNull(items[1]) && !isNull(items[0]) {
					nonNull = items[0]
				}
				if nonNull != nil {
					delete(cp, unionKey)
					cleanedNonNull := cleanSchemaNode(nonNull)
					for k, v := range cleanedNonNull {
						if _, exists := cp[k]; !exists || k == "type" {
							cp[k] = v
						}
					}
					if t, ok := cleanedNonNull["type"]; ok {
						cp["type"] = t
					}
					cp["nullable"] = true
				}
			}
		}
	}
	if propsMap, ok := cp["properties"].(map[string]interface{}); ok {
		cleanedProps := make(map[string]interface{}, len(propsMap))
		for k, v := range propsMap {
			if m, ok := v.(map[string]interface{}); ok {
				cleanedProps[k] = cleanSchemaNode(m)
			} else {
				cleanedProps[k] = v
			}
		}
		cp["properties"] = cleanedProps
	}
	if itemsMap, ok := cp["items"].(map[string]interface{}); ok {
		cp["items"] = cleanSchemaNode(itemsMap)
	} else if cleaned, _, ok := cleanSubSchemaList(cp["items"]); ok {
		cp["items"] = cleaned
	}
	for _, unionKey := range []string{"anyOf", "oneOf", "allOf"} {
		if val, ok := cp[unionKey]; ok {
			if cleaned, _, ok := cleanSubSchemaList(val); ok {
				cp[unionKey] = cleaned
			}
		}
	}
	if t, ok := cp["type"].(string); (!ok || t == "") && cp["anyOf"] == nil && cp["oneOf"] == nil && cp["allOf"] == nil {
		cp["type"] = "OBJECT"
	}
	if cp["type"] == "OBJECT" && cp["properties"] == nil {
		cp["properties"] = map[string]interface{}{}
	}
	return cp
}

type DefaultMCPDispatcher struct {
	d *mcp.DefaultDispatcher
}

var _ MCPDispatcher = (*DefaultMCPDispatcher)(nil)

func NewDefaultMCPDispatcher(servers []MCPServerConfig, client *http.Client, allowedTools ...[]string) *DefaultMCPDispatcher {
	return &DefaultMCPDispatcher{d: mcp.NewDispatcher(servers, client, allowedTools...)}
}

func (d *DefaultMCPDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	if d == nil || d.d == nil {
		return nil, nil
	}
	tools, err := d.d.ListTools(ctx)
	if err != nil || len(tools) == 0 {
		return nil, err
	}
	decls := make([]geminiFunctionDeclaration, len(tools))
	for i, t := range tools {
		decls[i] = geminiFunctionDeclaration{Name: t.Name, Description: t.Description, Parameters: cleanParameters(t.InputSchema)}
	}
	return decls, nil
}

func (d *DefaultMCPDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	if d == nil || d.d == nil {
		return "", errors.New("mcp dispatcher is nil")
	}
	res, err := d.d.Execute(ctx, name, args)
	if err != nil && strings.Contains(err.Error(), "is not in the allowed tools list") {
		return "", fmt.Errorf("tool %q is not in the allowed voice tools list", name)
	}
	return res, err
}

func (d *DefaultMCPDispatcher) Prime(ctx context.Context) error {
	if d == nil || d.d == nil {
		return nil
	}
	return d.d.Prime(ctx)
}

type GeminiAPIPoolConfig struct {
	APIKey, Model, BaseURL, SystemPrompt, DataDir string
	HTTPClient                                    *http.Client
	PrewarmedTargets, AllowedTools                []string
	MaxHistoryTurns, MaxSessionTurns              int
	MCPServers                                    []MCPServerConfig
	MCPDispatcher                                 MCPDispatcher
	MemoryRetriever                               MemoryRetriever
	AmbientContextRetriever                       AmbientContextRetriever
	OnSessionRotated                              func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error
	GetSessionRecord                              func(ctx context.Context, targetKey string) (SessionRecord, error)
	TargetLockTimeout                             time.Duration
}

type (
	geminiFunctionCall struct {
		ID   string                 `json:"id,omitempty"`
		Name string                 `json:"name"`
		Args map[string]interface{} `json:"args,omitempty"`
	}
	geminiFunctionResp struct {
		Name     string                 `json:"name"`
		Response map[string]interface{} `json:"response"`
	}
	geminiPart struct {
		Text             string              `json:"text,omitempty"`
		FunctionCall     *geminiFunctionCall `json:"functionCall,omitempty"`
		FunctionResponse *geminiFunctionResp `json:"functionResponse,omitempty"`
		ThoughtSignature string              `json:"thoughtSignature,omitempty"`
	}
	geminiContent struct {
		Role  string       `json:"role,omitempty"`
		Parts []geminiPart `json:"parts"`
	}
	geminiFunctionDeclaration struct {
		Name        string                 `json:"name"`
		Description string                 `json:"description,omitempty"`
		Parameters  map[string]interface{} `json:"parameters,omitempty"`
	}
	geminiTool struct {
		FunctionDeclarations []geminiFunctionDeclaration `json:"functionDeclarations,omitempty"`
	}
	geminiThinkingConfig struct {
		ThinkingBudget int `json:"thinkingBudget"`
	}
	geminiGenerationConfig struct {
		ThinkingConfig *geminiThinkingConfig `json:"thinkingConfig,omitempty"`
	}
	geminiStreamRequest struct {
		Contents          []geminiContent         `json:"contents"`
		Tools             []geminiTool            `json:"tools,omitempty"`
		SystemInstruction *geminiContent          `json:"systemInstruction,omitempty"`
		GenerationConfig  *geminiGenerationConfig `json:"generationConfig,omitempty"`
	}
	geminiCandidate struct {
		Content      *geminiContent `json:"content,omitempty"`
		FinishReason string         `json:"finishReason,omitempty"`
	}
	geminiUsageMetadata struct {
		PromptTokenCount     int `json:"promptTokenCount"`
		CandidatesTokenCount int `json:"candidatesTokenCount"`
		TotalTokenCount      int `json:"totalTokenCount"`
	}
	geminiStreamChunk struct {
		Candidates    []geminiCandidate    `json:"candidates,omitempty"`
		UsageMetadata *geminiUsageMetadata `json:"usageMetadata,omitempty"`
	}
)

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
			var parts []string
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
					if c, rErr := os.ReadFile(filepath.Join(rulesDir, e.Name())); rErr == nil && len(c) > 0 {
						parts = append(parts, string(c))
					}
				}
			}
			cfg.SystemPrompt = strings.Join(parts, "\n\n")
		}
	}
	return &GeminiAPIPool{
		cfg:         cfg,
		sessions:    make(map[string]*GeminiAPISession),
		targetLocks: make(map[string]*targetLock),
	}
}

func (p *GeminiAPIPool) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

func (p *GeminiAPIPool) APIKey() string {
	if p == nil {
		return ""
	}
	return p.cfg.APIKey
}

func (p *GeminiAPIPool) sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if p != nil && p.cfg.APIKey != "" {
		msg = strings.ReplaceAll(msg, p.cfg.APIKey, "[REDACTED]")
	}
	return errors.New(apiKeyQueryRegex.ReplaceAllString(msg, "$1[REDACTED]"))
}

func (p *GeminiAPIPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (AgentSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("gemini api pool is closed")
	}
	trimmedID := strings.TrimSpace(sessionID)
	if sess, exists := p.sessions[targetKey]; exists && (trimmedID == "" || sess.SessionID() == trimmedID) {
		return sess, nil
	}
	if trimmedID == "" {
		trimmedID = targetKey
	}
	sess := &GeminiAPISession{pool: p, targetKey: targetKey, sessionID: trimmedID, dataDir: p.cfg.DataDir}
	sess.hydrateHistory()
	p.sessions[targetKey] = sess
	return sess, nil
}

func (p *GeminiAPIPool) ShouldRotateSession(sess AgentSession) (bool, string) { return false, "" }
func (p *GeminiAPIPool) RotateSession(ctx context.Context, targetKey string) (AgentSession, error) {
	return p.GetOrCreateSession(ctx, targetKey, uuid.New().String())
}

func (p *GeminiAPIPool) Initialize(ctx context.Context) error {
	for _, target := range p.PrewarmedTargets() {
		if _, err := p.GetOrCreateSession(ctx, target, ""); err != nil {
			return err
		}
	}
	return nil
}

func (p *GeminiAPIPool) PrewarmedTargets() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]string(nil), p.cfg.PrewarmedTargets...)
}

func (p *GeminiAPIPool) UpdatePrewarmedTargets(targets []string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.PrewarmedTargets = append([]string(nil), targets...)
}

func (p *GeminiAPIPool) MarkDirty() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.sessions = make(map[string]*GeminiAPISession)
	p.mu.Unlock()
}

func (p *GeminiAPIPool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed, p.sessions = true, make(map[string]*GeminiAPISession)
	return nil
}

type GeminiAPISession struct {
	pool      *GeminiAPIPool
	targetKey string
	sessionID string
	dataDir   string
	history   []geminiContent
	mu        sync.Mutex
}

var _ AgentSession = (*GeminiAPISession)(nil)

func (s *GeminiAPISession) SessionID() string {
	if s.sessionID != "" {
		return s.sessionID
	}
	return s.targetKey
}

func (s *GeminiAPISession) TargetKey() string { return s.targetKey }

func (s *GeminiAPISession) History() []geminiContent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]geminiContent(nil), s.history...)
}

func (s *GeminiAPISession) transcriptPath() string {
	if s.dataDir == "" {
		return ""
	}
	return filepath.Join(s.dataDir, "brain", s.SessionID(), ".system_generated", "logs", "transcript.jsonl")
}

func (s *GeminiAPISession) hydrateHistory() {
	p := s.transcriptPath()
	if p == "" {
		return
	}
	data, err := os.ReadFile(p)
	if err != nil || len(data) == 0 {
		return
	}
	var loaded []geminiContent
	for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if trimmed := strings.TrimSpace(l); trimmed != "" {
			var entry struct{ Type, Content string }
			if json.Unmarshal([]byte(trimmed), &entry) == nil && entry.Content != "" {
				role := ""
				if entry.Type == "USER_INPUT" {
					role = "user"
				} else if entry.Type == "PLANNER_RESPONSE" {
					role = "model"
				}
				if role != "" {
					loaded = append(loaded, geminiContent{Role: role, Parts: []geminiPart{{Text: entry.Content}}})
				}
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

func (s *GeminiAPISession) appendTranscript(userPrompt, modelResponse string) {
	p := s.transcriptPath()
	if p == "" {
		return
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[GeminiAPISession] Warning creating transcript directory %s: %v", dir, err)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	u, err1 := json.Marshal(map[string]interface{}{"source": "USER_EXPLICIT", "type": "USER_INPUT", "content": userPrompt, "created_at": now})
	m, err2 := json.Marshal(map[string]interface{}{"source": "MODEL", "type": "PLANNER_RESPONSE", "content": modelResponse, "created_at": now})
	if err1 != nil || err2 != nil {
		return
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("[GeminiAPISession] Warning opening transcript file %s: %v", p, err)
		return
	}
	defer func() {
		if cErr := f.Close(); cErr != nil {
			log.Printf("[GeminiAPISession] Warning closing transcript file %s: %v", p, cErr)
		}
	}()
	if _, wErr := f.Write(append(append(u, '\n'), append(m, '\n')...)); wErr != nil {
		log.Printf("[GeminiAPISession] Warning writing transcript file %s: %v", p, wErr)
	}
}

func (s *GeminiAPISession) failTurn(turn *TurnContext, err error) error {
	sanitized := err
	if s.pool != nil {
		sanitized = s.pool.sanitizeError(err)
	}
	if turn != nil && turn.Sink != nil {
		turn.Sink.OnError(sanitized)
	}
	return sanitized
}

func (s *GeminiAPISession) consumeSSEStream(body io.ReadCloser, turn *TurnContext, started *bool, fullText *strings.Builder, totalUsage *AgyUsage) ([]geminiPart, error) {
	defer body.Close()
	reader := bufio.NewReader(body)
	var pendingParts []geminiPart
	for {
		line, rErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := strings.TrimRight(string(line), "\r\n")
			if strings.HasPrefix(trimmed, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
				if data != "" && data != "[DONE]" {
					var chunk geminiStreamChunk
					if uErr := json.Unmarshal([]byte(data), &chunk); uErr != nil {
						return nil, fmt.Errorf("failed to unmarshal SSE chunk: %w", uErr)
					}
					for _, cand := range chunk.Candidates {
						if cand.Content != nil {
							for _, part := range cand.Content.Parts {
								if part.FunctionCall != nil {
									pendingParts = append(pendingParts, part)
								}
								if part.Text != "" {
									if started != nil && !*started {
										*started = true
										if turn != nil && turn.Sink != nil {
											turn.Sink.OnTurnStarted()
										}
									}
									if turn != nil && turn.Sink != nil {
										turn.Sink.OnTextDelta(part.Text)
									}
									if fullText != nil {
										fullText.WriteString(part.Text)
									}
								}
							}
						}
					}
					if chunk.UsageMetadata != nil && totalUsage != nil {
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
			if errors.Is(rErr, io.EOF) {
				break
			}
			return nil, rErr
		}
	}
	return pendingParts, nil
}

func (s *GeminiAPISession) Send(prompt string, turn *TurnContext) error {
	if s.pool != nil {
		s.pool.mu.RLock()
		closed := s.pool.closed
		s.pool.mu.RUnlock()
		if closed {
			return s.failTurn(turn, errors.New("gemini api pool is closed"))
		}
	}
	if strings.TrimSpace(prompt) == "" {
		return s.failTurn(turn, errors.New("empty prompt"))
	}
	ctx := context.Background()
	if turn != nil && turn.Ctx != nil {
		ctx = turn.Ctx
	}
	if err := ctx.Err(); err != nil {
		return s.failTurn(turn, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.history) == 0 {
		s.hydrateHistory()
	}
	var tools []geminiTool
	var dispatcher MCPDispatcher
	if s.pool != nil && s.pool.cfg.MCPDispatcher != nil {
		dispatcher = s.pool.cfg.MCPDispatcher
		if decls, dErr := dispatcher.ListDeclarations(ctx); dErr != nil {
			log.Printf("[GeminiAPISession] Warning: failed to list MCP declarations: %v", dErr)
		} else if len(decls) > 0 {
			tools = []geminiTool{{FunctionDeclarations: decls}}
		}
	}
	var ambRetriever AmbientContextRetriever
	var memRetriever MemoryRetriever
	if s.pool != nil {
		ambRetriever, memRetriever = s.pool.cfg.AmbientContextRetriever, s.pool.cfg.MemoryRetriever
	}
	payloadPrompt := injectRetrievedContext(ctx, prompt, ambRetriever, 300*time.Millisecond, memRetriever, 1500*time.Millisecond, "[GeminiAPISession]")

	workingContents := append(append([]geminiContent(nil), s.history...), geminiContent{
		Role:  "user",
		Parts: []geminiPart{{Text: payloadPrompt}},
	})

	var systemInstruction *geminiContent
	if s.pool != nil && s.pool.cfg.SystemPrompt != "" {
		systemInstruction = &geminiContent{Parts: []geminiPart{{Text: s.pool.cfg.SystemPrompt}}}
	}
	baseURL, apiKey, model := "https://generativelanguage.googleapis.com", "", ""
	if s.pool != nil {
		if s.pool.cfg.BaseURL != "" {
			baseURL = strings.TrimRight(s.pool.cfg.BaseURL, "/")
		}
		apiKey, model = s.pool.cfg.APIKey, s.pool.cfg.Model
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
			GenerationConfig:  &geminiGenerationConfig{ThinkingConfig: &geminiThinkingConfig{ThinkingBudget: 0}},
		}
		bodyBytes, err := json.Marshal(reqPayload)
		if err != nil {
			return s.failTurn(turn, fmt.Errorf("failed to marshal request: %w", err))
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return s.failTurn(turn, fmt.Errorf("failed to create http request: %w", err))
		}
		httpReq.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(httpReq)
		if err != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return s.failTurn(turn, err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			body, rErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			targetModel := "default"
			if s.pool != nil && s.pool.cfg.Model != "" {
				targetModel = s.pool.cfg.Model
			}
			errType := "capacity_throttle"
			if rErr == nil && IsQuotaPause(string(body), "") {
				errType = "quota_paused"
			}
			metrics.RecordRunnerError(errType, targetModel)
			return s.failTurn(turn, errors.New("rate limit exceeded: please try again shortly"))
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, rErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			bodyStr := fmt.Sprintf("<error reading response body: %v>", rErr)
			if rErr == nil {
				bodyStr = string(body)
			}
			return s.failTurn(turn, fmt.Errorf("gemini api returned status %d: %s", resp.StatusCode, bodyStr))
		}
		pendingParts, streamErr := s.consumeSSEStream(resp.Body, turn, &started, &fullText, &totalUsage)
		if streamErr != nil {
			readErr := streamErr
			if ctx.Err() != nil {
				readErr = ctx.Err()
			}
			return s.failTurn(turn, readErr)
		}
		if ctx.Err() != nil {
			return s.failTurn(turn, ctx.Err())
		}
		if len(pendingParts) == 0 {
			break
		}
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
			var (
				toolOutput string
				execErr    error
			)
			if dispatcher != nil {
				toolOutput, execErr = dispatcher.Execute(ctx, fc.Name, fc.Args)
			} else {
				execErr = errors.New("no mcp dispatcher configured")
			}
			status := "ok"
			if execErr != nil {
				status = "error"
			}
			canonicalTool, mcpServer := ExtractMCPToolInfo(fc.Name, fc.Args)
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnToolCompleted(canonicalTool, mcpServer, time.Since(start), status)
			}
			respMap := map[string]interface{}{"output": toolOutput}
			if execErr != nil {
				respMap = map[string]interface{}{"error": execErr.Error()}
			}
			funcParts = append(funcParts, geminiPart{FunctionResponse: &geminiFunctionResp{Name: fc.Name, Response: respMap}})
		}
		workingContents = append(workingContents,
			geminiContent{Role: "model", Parts: modelParts},
			geminiContent{Role: "user", Parts: funcParts},
		)
	}

	if strings.TrimSpace(fullText.String()) == "" && len(workingContents) > 1 {
		finalPayload := geminiStreamRequest{
			Contents:          workingContents,
			SystemInstruction: systemInstruction,
			GenerationConfig:  &geminiGenerationConfig{ThinkingConfig: &geminiThinkingConfig{ThinkingBudget: 0}},
		}
		if finalBytes, err := json.Marshal(finalPayload); err == nil {
			if finalHttpReq, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(finalBytes)); reqErr == nil {
				finalHttpReq.Header.Set("Content-Type", "application/json")
				if finalResp, respErr := client.Do(finalHttpReq); respErr == nil {
					if finalResp.StatusCode >= 200 && finalResp.StatusCode < 300 {
						if _, sErr := s.consumeSSEStream(finalResp.Body, turn, &started, &fullText, &totalUsage); sErr != nil {
							log.Printf("[GeminiAPISession] Warning in fallback stream: %v", sErr)
						}
					} else {
						_ = finalResp.Body.Close()
					}
				}
			}
		}
	}

	resText := fullText.String()
	if strings.TrimSpace(resText) != "" {
		s.history = append(s.history,
			geminiContent{Role: "user", Parts: []geminiPart{{Text: prompt}}},
			geminiContent{Role: "model", Parts: []geminiPart{{Text: resText}}},
		)
	}
	maxTurns := 10
	if s.pool != nil && s.pool.cfg.MaxHistoryTurns > 0 {
		maxTurns = s.pool.cfg.MaxHistoryTurns
	}
	maxMessages := maxTurns * 2
	if len(s.history) > maxMessages {
		s.history = s.history[len(s.history)-maxMessages:]
	}
	s.appendTranscript(prompt, resText)
	if turn != nil && turn.Sink != nil {
		turn.Sink.OnResult(&TurnResult{Response: resText, Usage: totalUsage})
	}
	return nil
}

func (p *GeminiAPIPool) acquireTargetLock(ctx context.Context, targetKey string) (*targetLock, error) {
	return acquirePoolTargetLock(ctx, &p.targetMu, &p.targetLocks, targetKey, p.cfg.TargetLockTimeout)
}

func (p *GeminiAPIPool) releaseTargetLock(targetKey string, tl *targetLock, held bool) {
	releasePoolTargetLock(&p.targetMu, p.targetLocks, targetKey, tl, held)
}

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
	isCold, turnCount := false, rec.TurnCount
	if rec.TurnCount <= 1 || rec.ActiveSessionID == "" {
		isCold, turnCount = true, 0
	}
	sessID := gSess.SessionID()
	if sessID == "" {
		sessID = rec.ActiveSessionID
	}
	return &geminiSessionLease{
		pool: p, session: gSess, targetKey: trimmedKey, sessionID: sessID,
		prevSessID: rec.PreviousSessionID, isCold: isCold, turnCount: turnCount, lock: tl,
	}, nil
}

type geminiSessionLease struct {
	pool                             *GeminiAPIPool
	session                          *GeminiAPISession
	targetKey, sessionID, prevSessID string
	isCold, cancelRotation           bool
	turnCount                        int
	lock                             *targetLock
	releaseOnce                      sync.Once
}

var _ SessionLease = (*geminiSessionLease)(nil)

func (l *geminiSessionLease) SessionID() string         { return l.sessionID }
func (l *geminiSessionLease) PreviousSessionID() string { return l.prevSessID }
func (l *geminiSessionLease) IsCold() bool              { return l.isCold }
func (l *geminiSessionLease) IsEphemeral() bool         { return false }
func (l *geminiSessionLease) TurnCount() int            { return l.turnCount }

func (l *geminiSessionLease) MarkDirty() {
	if l != nil && l.pool != nil {
		l.pool.mu.Lock()
		delete(l.pool.sessions, l.targetKey)
		l.pool.mu.Unlock()
	}
	if l != nil {
		l.session = nil
	}
}

func (l *geminiSessionLease) Execute(ctx context.Context, turn *TurnContext) (*TurnResult, error) {
	if turn == nil {
		return nil, errors.New("turn context cannot be nil")
	}
	l.cancelRotation = false
	if turn.Ctx == nil {
		turn.Ctx = ctx
	}
	compiledPrompt := compileLeasePrompt(turn, l.isCold, l.prevSessID)
	bufSink := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	origSink := turn.Sink
	turn.Sink = &leaseTurnSinkWrapper{inner: origSink, buf: bufSink}
	defer func() { turn.Sink = origSink }()

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
	if res != nil && !IsSubstantiveResponse(res.Response) && (IsQuotaPause(res.Stderr, "") || IsCapacityBlip(res.Stderr, "")) {
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
		maxTurns := DefaultMaxSessionTurns
		if l.pool != nil && l.pool.cfg.MaxSessionTurns > 0 {
			maxTurns = l.pool.cfg.MaxSessionTurns
		}
		if !shouldRotate && l.turnCount >= maxTurns {
			shouldRotate, reason = true, fmt.Sprintf("reached maximum turn threshold (%d)", maxTurns)
		}
		if shouldRotate {
			log.Printf("[GeminiAPIPool] Session rotation triggered for %s (reason: %s)", l.targetKey, reason)
			newSessionID, oldSessionID := uuid.New().String(), l.sessionID
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
			if _, rotErr := l.pool.GetOrCreateSession(context.Background(), l.targetKey, newSessionID); rotErr != nil {
				log.Printf("[GeminiAPIPool] Warning rotating session for %s: %v", l.targetKey, rotErr)
				l.pool.mu.Lock()
				delete(l.pool.sessions, l.targetKey)
				l.pool.mu.Unlock()
			}
		}
	})
	return releaseErr
}
