package ollama

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

const (
	DefaultBaseURL = "http://ollama:11434"
	DefaultModel   = "all-minilm"
	DefaultNumCtx  = 512
)

type Config struct {
	BaseURL     string `yaml:"base_url"`
	Model       string `yaml:"model"`
	QueryPrefix string `yaml:"query_prefix"`
	NumCtx      int    `yaml:"num_ctx"`
}

type Client struct {
	cfgFn      func() Config
	httpClient *http.Client
	retryDelay time.Duration
}

func NewDynamicClient(cfgFn func() Config) *Client {
	if cfgFn == nil {
		cfgFn = func() Config {
			return Config{}
		}
	}
	return &Client{
		cfgFn: cfgFn,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		retryDelay: 50 * time.Millisecond,
	}
}

func (c *Client) SetHTTPClient(hc *http.Client) {
	c.httpClient = hc
}

func (c *Client) SetRetryDelay(d time.Duration) {
	c.retryDelay = d
}

func (c *Client) config() Config {
	var cfg Config
	if c.cfgFn != nil {
		cfg = c.cfgFn()
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = DefaultModel
	}
	numCtx := cfg.NumCtx
	if numCtx <= 0 {
		numCtx = DefaultNumCtx
	}
	return Config{
		BaseURL:     baseURL,
		Model:       model,
		QueryPrefix: cfg.QueryPrefix,
		NumCtx:      numCtx,
	}
}

func (c *Client) BaseURL() string {
	return c.config().BaseURL
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

func (c *Client) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("text cannot be empty")
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}

	cfg := c.config()
	prompt := text
	if isQuery && cfg.QueryPrefix != "" {
		prompt = cfg.QueryPrefix + text
	}

	reqBody, err := json.Marshal(embeddingRequest{
		Model:  cfg.Model,
		Prompt: prompt,
		Options: map[string]any{
			"num_ctx": cfg.NumCtx,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	totalAttempts := maxRetries + 1
	var lastErr error

	for attempt := range totalAttempts {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		emb, fatalErr, retryErr := c.doRequest(ctx, cfg.BaseURL, reqBody)
		if fatalErr != nil {
			return nil, fatalErr
		}
		if retryErr == nil && len(emb) > 0 {
			return emb, nil
		}
		lastErr = retryErr

		if attempt < totalAttempts-1 {
			if c.retryDelay > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(c.retryDelay):
				}
			}
		}
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", totalAttempts, lastErr)
}

func (c *Client) doRequest(ctx context.Context, baseURL string, body []byte) ([]float32, error, error) {
	url := baseURL + "/api/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err), nil
	}
	req.Header.Set("Content-Type", "application/json")

	hc := c.httpClient
	if hc == nil {
		hc = http.DefaultClient
	}

	resp, err := hc.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, fmt.Errorf("ollama HTTP %d: %s", resp.StatusCode, string(respBytes)), nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("ollama HTTP %d: %s", resp.StatusCode, string(respBytes))
	}

	var parsed embeddingResponse
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	if parsed.Error != "" {
		return nil, nil, fmt.Errorf("ollama error: %s", parsed.Error)
	}
	if len(parsed.Embedding) == 0 {
		return nil, nil, fmt.Errorf("empty embedding returned")
	}

	return parsed.Embedding, nil, nil
}
