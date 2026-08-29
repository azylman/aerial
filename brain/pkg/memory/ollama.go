package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	DefaultEmbeddingModel = "bge-small-en"
	DefaultOllamaURL      = "http://ollama:11434"
	BGEQueryPrefix        = "Represent this sentence for searching relevant passages: "
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = os.Getenv("OLLAMA_URL")
		if baseURL == "" {
			baseURL = DefaultOllamaURL
		}
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	return &Client{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
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
// If isQuery is true, prepends the BGE query instruction prefix.
// Enforces a 1.0s timeout per attempt with up to maxRetries attempts (default 1 retry = 2 total attempts).
func (c *Client) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	if text == "" {
		return nil, fmt.Errorf("text cannot be empty")
	}

	prompt := text
	if isQuery {
		prompt = BGEQueryPrefix + text
	}

	reqBody, err := json.Marshal(EmbeddingRequest{
		Model:  DefaultEmbeddingModel,
		Prompt: prompt,
	})
	if err != nil {
		return nil, err
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
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", totalAttempts, lastErr)
}

func (c *Client) doRequest(ctx context.Context, body []byte) ([]float32, error) {
	url := c.BaseURL + "/api/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
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
