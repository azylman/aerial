package runner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
)

var apiKeyQueryRegex = regexp.MustCompile(`(?i)(key=)[^& \t\r\n"']+`)

// GeminiVoicePoolConfig holds configuration for the GeminiVoicePool.
type GeminiVoicePoolConfig struct {
	APIKey           string
	Model            string
	BaseURL          string // defaults to "https://generativelanguage.googleapis.com"
	HTTPClient       *http.Client
	PrewarmedTargets []string
	SystemPrompt     string
	MaxHistoryTurns  int // defaults to 10 (20 messages)
}

// geminiPart represents a content part in the Gemini REST API.
type geminiPart struct {
	Text string `json:"text,omitempty"`
}

// geminiContent represents a message content block in the Gemini REST API.
type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
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

// GeminiVoicePool manages direct REST-based voice sessions for target devices.
type GeminiVoicePool struct {
	cfg      GeminiVoicePoolConfig
	sessions map[string]*GeminiVoiceSession
	mu       sync.RWMutex
	closed   bool
}

var _ VoiceProcessPool = (*GeminiVoicePool)(nil)

// NewGeminiVoicePool instantiates a GeminiVoicePool with sensible defaults.
func NewGeminiVoicePool(cfg GeminiVoicePoolConfig) *GeminiVoicePool {
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
	return &GeminiVoicePool{
		cfg:      cfg,
		sessions: make(map[string]*GeminiVoiceSession),
	}
}

// Model returns the configured model for Gemini API voice generation.
func (p *GeminiVoicePool) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

// APIKey returns the configured API key.
func (p *GeminiVoicePool) APIKey() string {
	if p == nil {
		return ""
	}
	return p.cfg.APIKey
}

// sanitizeError replaces API keys and secret query parameters with [REDACTED].
func (p *GeminiVoicePool) sanitizeError(err error) error {
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
func (p *GeminiVoicePool) GetOrCreateSession(ctx context.Context, targetKey string) (VoiceSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errors.New("gemini voice pool is closed")
	}

	if sess, ok := p.sessions[targetKey]; ok {
		return sess, nil
	}

	sess := &GeminiVoiceSession{
		targetKey: targetKey,
		sessionID: targetKey,
		pool:      p,
		history:   make([]geminiContent, 0),
	}
	p.sessions[targetKey] = sess
	return sess, nil
}

// Initialize pre-warms configured target sessions.
func (p *GeminiVoicePool) Initialize(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("gemini voice pool is closed")
	}

	for _, target := range p.cfg.PrewarmedTargets {
		if _, ok := p.sessions[target]; !ok {
			p.sessions[target] = &GeminiVoiceSession{
				targetKey: target,
				sessionID: target,
				pool:      p,
				history:   make([]geminiContent, 0),
			}
		}
	}
	return nil
}

// Close terminates the pool and marks it as closed.
func (p *GeminiVoicePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	return nil
}

// GeminiVoiceSession represents an active conversation session with sliding window history.
type GeminiVoiceSession struct {
	targetKey string
	sessionID string
	pool      *GeminiVoicePool
	mu        sync.Mutex
	history   []geminiContent
}

var _ VoiceSession = (*GeminiVoiceSession)(nil)

// SessionID returns the identifier for this voice session.
func (s *GeminiVoiceSession) SessionID() string {
	if s.sessionID != "" {
		return s.sessionID
	}
	return s.targetKey
}

// TargetKey returns the target device key for this session.
func (s *GeminiVoiceSession) TargetKey() string {
	return s.targetKey
}

// History returns a copy of the session's conversation history.
func (s *GeminiVoiceSession) History() []geminiContent {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]geminiContent, len(s.history))
	copy(cp, s.history)
	return cp
}

// Send submits a prompt to Gemini via streamGenerateContent SSE and notifies the sink.
func (s *GeminiVoiceSession) Send(prompt string, turn *TurnContext) error {
	if s.pool != nil {
		s.pool.mu.RLock()
		closed := s.pool.closed
		s.pool.mu.RUnlock()
		if closed {
			err := errors.New("gemini voice pool is closed")
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

	// Formulate request contents: previous history + current user prompt
	contents := make([]geminiContent, len(s.history), len(s.history)+1)
	copy(contents, s.history)
	contents = append(contents, geminiContent{
		Role:  "user",
		Parts: []geminiPart{{Text: prompt}},
	})

	var systemInstruction *geminiContent
	if s.pool != nil && s.pool.cfg.SystemPrompt != "" {
		systemInstruction = &geminiContent{
			Parts: []geminiPart{
				{Text: s.pool.cfg.SystemPrompt},
			},
		}
	}

	reqPayload := geminiStreamRequest{
		Contents:          contents,
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		sanitizedErr := s.pool.sanitizeError(fmt.Errorf("failed to create http request: %w", err))
		if turn != nil && turn.Sink != nil {
			turn.Sink.OnError(sanitizedErr)
		}
		return sanitizedErr
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient
	if s.pool != nil && s.pool.cfg.HTTPClient != nil {
		client = s.pool.cfg.HTTPClient
	}

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
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		rateLimitErr := errors.New("rate limit exceeded: please try again shortly")
		if turn != nil && turn.Sink != nil {
			turn.Sink.OnError(rateLimitErr)
		}
		return rateLimitErr
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, rErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
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
	var fullText strings.Builder
	var usage AgyUsage
	started := false

	for {
		line, rErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := strings.TrimRight(string(line), "\r\n")
			if strings.HasPrefix(trimmed, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
				if data != "" && data != "[DONE]" {
					var chunk geminiStreamChunk
					if uErr := json.Unmarshal([]byte(data), &chunk); uErr != nil {
						sanitizedErr := s.pool.sanitizeError(fmt.Errorf("failed to unmarshal SSE chunk: %w", uErr))
						if turn != nil && turn.Sink != nil {
							turn.Sink.OnError(sanitizedErr)
						}
						return sanitizedErr
					}

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
						usage.InputTokens = chunk.UsageMetadata.PromptTokenCount
						usage.OutputTokens = chunk.UsageMetadata.CandidatesTokenCount
						usage.TotalTokens = chunk.UsageMetadata.TotalTokenCount
						if usage.TotalTokens == 0 && (usage.InputTokens > 0 || usage.OutputTokens > 0) {
							usage.TotalTokens = usage.InputTokens + usage.OutputTokens
						}
					}
				}
			}
		}

		if rErr != nil {
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

	resText := fullText.String()

	// Append user prompt and model response to history
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

	// Trim history to MaxHistoryTurns*2 messages
	maxTurns := 10
	if s.pool != nil && s.pool.cfg.MaxHistoryTurns > 0 {
		maxTurns = s.pool.cfg.MaxHistoryTurns
	}
	maxMessages := maxTurns * 2
	if len(s.history) > maxMessages {
		s.history = s.history[len(s.history)-maxMessages:]
	}

	if turn != nil && turn.Sink != nil {
		turn.Sink.OnResult(&TurnResult{
			Response: resText,
			Usage:    usage,
		})
	}

	return nil
}
