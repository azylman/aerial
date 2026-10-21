package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OllamaClient provides embedding generation capabilities.
type OllamaClient interface {
	GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error)
}

// Client is the concrete implementation of OllamaClient communicating with Ollama over HTTP.
type Client struct {
	baseURL     string
	model       string
	queryPrefix string
	numCtx      int
	httpClient  *http.Client
	retryDelay  time.Duration
}

// NewOllamaClient creates a new Client configured from OllamaConfig.
func NewOllamaClient(cfg *OllamaConfig) *Client {
	if cfg == nil {
		cfg = &OllamaConfig{
			BaseURL: DefaultOllamaURL,
			Model:   DefaultOllamaModel,
			NumCtx:  DefaultOllamaNumCtx,
		}
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultOllamaURL
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = DefaultOllamaModel
	}
	numCtx := cfg.NumCtx
	if numCtx <= 0 {
		numCtx = DefaultOllamaNumCtx
	}
	return &Client{
		baseURL:     baseURL,
		model:       model,
		queryPrefix: cfg.QueryPrefix,
		numCtx:      numCtx,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		retryDelay: 50 * time.Millisecond,
	}
}

type embeddingRequest struct {
	Model   string         `json:"model"`
	Prompt  string         `json:"prompt"`
	Options map[string]any `json:"options,omitempty"`
}

type embeddingResponse struct {
	Embedding []float32 `json:"embedding"`
	Error     string    `json:"error,omitempty"`
}

// GenerateEmbedding generates an embedding vector for the provided text.
// If isQuery is true and queryPrefix is configured, prepends the query prefix to the prompt.
func (c *Client) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("text cannot be empty")
	}

	prompt := text
	if isQuery && c.queryPrefix != "" {
		prompt = c.queryPrefix + text
	}

	reqBody, err := json.Marshal(embeddingRequest{
		Model:  c.model,
		Prompt: prompt,
		Options: map[string]any{
			"num_ctx": c.numCtx,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	if maxRetries < 0 {
		maxRetries = 0
	}
	totalAttempts := maxRetries + 1

	var lastErr error
	for attempt := 0; attempt < totalAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
		emb, err := c.doRequest(attemptCtx, reqBody)
		cancel()

		if err == nil && len(emb) > 0 {
			return emb, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if attempt < totalAttempts-1 && c.retryDelay > 0 {
			timer := time.NewTimer(c.retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", totalAttempts, lastErr)
}

func (c *Client) doRequest(ctx context.Context, body []byte) ([]float32, error) {
	url := c.baseURL + "/api/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request error: %w", err)
	}
	defer closeWarn(resp.Body, "ollama response body")

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama HTTP %d: %s", resp.StatusCode, string(respBytes))
	}

	var parsed embeddingResponse
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	if parsed.Error != "" {
		return nil, fmt.Errorf("ollama error: %s", parsed.Error)
	}
	if len(parsed.Embedding) == 0 {
		return nil, fmt.Errorf("empty embedding returned")
	}

	return parsed.Embedding, nil
}
