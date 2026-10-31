package transcript

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/ollama"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

const (
	DefaultEmbeddingModel = ollama.DefaultModel
	DefaultOllamaURL      = ollama.DefaultBaseURL
)

// Client manages HTTP embedding requests to Ollama for transcript indexing and search.
type Client struct {
	*ollama.Client

	cfg   *config.Config
	roots []string
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
	c := &Client{
		cfg:   cfg,
		roots: cleanRoots,
	}
	c.Client = ollama.NewDynamicClient(func() ollama.Config {
		oc := c.getOllamaConfig()
		return ollama.Config{
			BaseURL:     oc.BaseURL,
			Model:       oc.Model,
			QueryPrefix: oc.QueryPrefix,
			NumCtx:      oc.NumCtx,
		}
	})
	return c
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

// GenerateEmbedding generates an embedding for a text string.
func (c *Client) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) (result []float32, retErr error) {
	if text == "" {
		return nil, fmt.Errorf("text cannot be empty")
	}

	ollamaCfg := c.getOllamaConfig()
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

	if c == nil || c.Client == nil {
		retErr = fmt.Errorf("ollama client not initialized")
		return nil, retErr
	}

	result, retErr = c.Client.GenerateEmbedding(ctx, text, isQuery, maxRetries)
	return result, retErr
}
