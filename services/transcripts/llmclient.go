package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
)

// llmEffortLow pins summarizer completions to eri's cheapest effort tier.
const llmEffortLow = "low"

type llmKeyCtx struct{}

// withLLMKey attaches an Idempotency-Key identity to ctx for the next Complete call.
func withLLMKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, llmKeyCtx{}, key)
}

func llmKeyFrom(ctx context.Context) string {
	key, ok := ctx.Value(llmKeyCtx{}).(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(key)
}

// LLMClient generates a summary completion for a prompt.
type LLMClient interface {
	Complete(ctx context.Context, prompt string) (string, error)
}

// httpLLMClient posts prompts to eri's /internal/llm proxy with low effort.
type httpLLMClient struct {
	url        string
	httpClient *http.Client
}

// NewLLMClient builds an LLMClient over eri's internal LLM proxy URL.
// A nil httpClient gets a bounded default; an empty URL surfaces on Complete.
func NewLLMClient(eriLLMURL string, httpClient *http.Client) LLMClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 120 * time.Second}
	}
	return &httpLLMClient{url: strings.TrimSpace(eriLLMURL), httpClient: tracing.Client(httpClient)}
}

type internalLLMRequest struct {
	Prompt string `json:"prompt"`
	Effort string `json:"effort,omitempty"`
}

type internalLLMResponse struct {
	Response string `json:"response"`
}

// Complete sends prompt to eri /internal/llm at low effort and returns the reply.
func (c *httpLLMClient) Complete(ctx context.Context, prompt string) (string, error) {
	if c == nil || c.httpClient == nil {
		return "", errors.New("transcripts llm client is not initialized")
	}
	trimmed := strings.TrimSpace(prompt)
	if trimmed == "" {
		return "", errors.New("transcripts llm prompt cannot be empty")
	}
	if c.url == "" {
		return "", errors.New("transcripts eri llm url is not configured")
	}
	reqBody, err := json.Marshal(internalLLMRequest{Prompt: trimmed, Effort: llmEffortLow})
	if err != nil {
		return "", fmt.Errorf("marshal transcripts llm request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("create transcripts llm request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key := llmKeyFrom(ctx); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("transcripts llm request failed: %w", err)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read transcripts llm response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("transcripts llm returned status %d: %s", resp.StatusCode, string(respBytes))
	}
	var parsed internalLLMResponse
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return "", fmt.Errorf("decode transcripts llm response: %w", err)
	}
	return parsed.Response, nil
}
