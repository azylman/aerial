package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/ollama"
)

// OllamaClient provides embedding generation capabilities.
type OllamaClient interface {
	GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error)
}

// Client is the concrete implementation of OllamaClient communicating with Ollama over HTTP.
type Client struct {
	*ollama.Client
	baseURL     string
	model       string
	queryPrefix string
	numCtx      int
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
	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}
	retryDelay := 50 * time.Millisecond
	c := &Client{
		baseURL:     baseURL,
		model:       model,
		queryPrefix: cfg.QueryPrefix,
		numCtx:      numCtx,
	}
	c.Client = ollama.NewDynamicClient(func() ollama.Config {
		return ollama.Config{
			BaseURL:     c.baseURL,
			Model:       c.model,
			QueryPrefix: c.queryPrefix,
			NumCtx:      c.numCtx,
		}
	})
	c.Client.SetHTTPClient(httpClient)
	c.Client.SetRetryDelay(retryDelay)
	return c
}

type embeddingRequest struct {
	Model   string         `json:"model"`
	Prompt  string         `json:"prompt"`
	Options map[string]any `json:"options,omitempty"`
}

// GenerateEmbedding generates an embedding vector for the provided text.
// If isQuery is true and queryPrefix is configured, prepends the query prefix to the prompt.
func (c *Client) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	return c.Client.GenerateEmbedding(ctx, text, isQuery, maxRetries)
}

// BaseURL returns the configured base URL.
func (c *Client) BaseURL() string {
	if c.baseURL != "" {
		return c.baseURL
	}
	return c.Client.BaseURL()
}
