package transcript

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

const (
	DefaultEmbeddingModel = "all-minilm"
	DefaultOllamaURL      = "http://ollama:11434"
)

// Client manages HTTP embedding requests to Ollama for transcript indexing and search.
type Client struct {
	cfg        *config.Config
	httpClient *http.Client
	roots      []string
	retryDelay time.Duration
}

// NewEmbedder creates a new Ollama embedding client with *config.Config and optional transcript search roots.
func NewEmbedder(cfg *config.Config, roots ...string) *Client {
	if cfg == nil {
		cfg = config.NewFromData(&config.ConfigData{})
	}
	var cleanRoots []string
	for _, r := range roots {
		if tr := strings.TrimSpace(r); tr != "" {
			cleanRoots = append(cleanRoots, tr)
		}
	}
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		roots:      cleanRoots,
		retryDelay: 50 * time.Millisecond,
	}
}

// Roots returns a defensive copy of configured transcript search roots.
func (c *Client) Roots() []string {
	if c == nil || len(c.roots) == 0 {
		return nil
	}
	return append([]string(nil), c.roots...)
}

func (c *Client) getOllamaConfig() config.OllamaConfig {
	if c == nil || c.cfg == nil {
		return config.OllamaConfig{
			BaseURL: DefaultOllamaURL,
			Model:   DefaultEmbeddingModel,
			NumCtx:  512,
		}
	}
	cur := c.cfg.Current()
	if cur == nil {
		return config.OllamaConfig{
			BaseURL: DefaultOllamaURL,
			Model:   DefaultEmbeddingModel,
			NumCtx:  512,
		}
	}
	cfg := cur.Ollama
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultOllamaURL
	}
	if cfg.Model == "" {
		cfg.Model = DefaultEmbeddingModel
	}
	if cfg.NumCtx <= 0 {
		cfg.NumCtx = 512
	}
	return cfg
}

// BaseURL returns the normalized Ollama server URL without trailing slash.
func (c *Client) BaseURL() string {
	return strings.TrimSuffix(c.getOllamaConfig().BaseURL, "/")
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

// GenerateEmbedding generates an embedding for a text string.
func (c *Client) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) (result []float32, retErr error) {
	if text == "" {
		return nil, fmt.Errorf("text cannot be empty")
	}

	ollamaCfg := c.getOllamaConfig()

	prompt := text
	if isQuery {
		if prefix := ollamaCfg.QueryPrefix; prefix != "" {
			prompt = prefix + text
		}
	}

	model := ollamaCfg.Model
	if model == "" {
		model = DefaultEmbeddingModel
	}

	embedType := "document"
	if isQuery {
		embedType = "query"
	}

	start := time.Now()
	defer func() {
		status := "success"
		if retErr != nil || len(result) == 0 {
			status = "error"
		}
		metrics.RecordEmbedding(model, embedType, status, time.Since(start))
	}()

	reqBody, err := json.Marshal(embeddingRequest{
		Model:  model,
		Prompt: prompt,
		Options: map[string]any{
			"num_ctx": ollamaCfg.NumCtx,
		},
	})
	if err != nil {
		retErr = err
		return nil, retErr
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
			result = emb
			return result, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			retErr = ctx.Err()
			return nil, retErr
		}
		if attempt < totalAttempts-1 {
			delay := c.retryDelay
			if delay == 0 {
				delay = 50 * time.Millisecond
			}
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					retErr = ctx.Err()
					return nil, retErr
				case <-timer.C:
				}
			}
		}
	}
	retErr = fmt.Errorf("failed after %d attempts: %w", totalAttempts, lastErr)
	return nil, retErr
}

func (c *Client) doRequest(ctx context.Context, body []byte) ([]float32, error) {
	url := c.BaseURL() + "/api/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 3 * time.Second}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("[Transcript] Warning closing ollama response body: %v", closeErr)
		}
	}()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama HTTP %d: %s", resp.StatusCode, string(respBytes))
	}

	var res embeddingResponse
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return nil, err
	}
	if res.Error != "" {
		return nil, fmt.Errorf("ollama error: %s", res.Error)
	}
	if len(res.Embedding) == 0 {
		return nil, fmt.Errorf("empty embedding returned")
	}

	return res.Embedding, nil
}
