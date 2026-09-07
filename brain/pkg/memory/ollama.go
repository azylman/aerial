package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/metrics"
)

const (
	DefaultEmbeddingModel = "all-minilm"
	DefaultOllamaURL      = "http://ollama:11434"
	BGEQueryPrefix        = "Represent this sentence for searching relevant passages: "
)

type Client struct {
	cfg        *config.Config
	httpClient *http.Client
}

// New creates a new memory Client with pure *config.Config dependency injection.
func New(cfg *config.Config) *Client {
	if cfg == nil {
		cfg = config.NewFromData(&config.ConfigData{})
	}
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

// NewClient is a compatibility constructor.
func NewClient(baseURLOrCfg any) *Client {
	switch v := baseURLOrCfg.(type) {
	case *config.Config:
		return New(v)
	case string:
		return New(config.NewFromData(&config.ConfigData{
			Ollama: config.OllamaConfig{
				BaseURL: v,
			},
		}))
	default:
		return New(nil)
	}
}

func (c *Client) getOllamaConfig() config.OllamaConfig {
	if c == nil || c.cfg == nil {
		return config.OllamaConfig{
			BaseURL: DefaultOllamaURL,
			Model:   DefaultEmbeddingModel,
		}
	}
	cur := c.cfg.Current()
	if cur == nil {
		return config.OllamaConfig{
			BaseURL: DefaultOllamaURL,
			Model:   DefaultEmbeddingModel,
		}
	}
	cfg := cur.Ollama
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultOllamaURL
	}
	if cfg.Model == "" {
		cfg.Model = DefaultEmbeddingModel
	}
	return cfg
}

func (c *Client) BaseURL() string {
	return strings.TrimSuffix(c.getOllamaConfig().BaseURL, "/")
}

type EmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type EmbeddingResponse struct {
	Embedding []float32 `json:"embedding"`
	Error     string    `json:"error,omitempty"`
}

// GenerateEmbedding generates an embedding for a text string.
// If isQuery is true and Ollama.QueryPrefix is configured, prepends the query instruction prefix.
// Enforces a 1.0s timeout per attempt with up to maxRetries attempts (default 1 retry = 2 total attempts).
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

	reqBody, err := json.Marshal(EmbeddingRequest{
		Model:  model,
		Prompt: prompt,
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
		time.Sleep(50 * time.Millisecond)
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
	defer func() { _ = resp.Body.Close() }()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama HTTP %d: %s", resp.StatusCode, string(respBytes))
	}

	var res EmbeddingResponse
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
