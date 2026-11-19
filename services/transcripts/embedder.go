package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/ollama"
	"github.com/azylman/aerial/pkg/tracing"
)

// DefaultSensesModel is the fallback embedding model served by senses.
const DefaultSensesModel = "bge-small-en-v1.5"

// Embedder generates a vector embedding for summary text via senses.
type Embedder interface {
	GenerateEmbedding(ctx context.Context, text string) ([]float32, error)
}

// httpEmbedder is an Embedder backed by the senses Ollama-compatible API.
type httpEmbedder struct {
	client *ollama.Client
}

// NewEmbedder builds an Embedder over the senses embeddings endpoint.
// A nil httpClient gets a bounded default; an empty baseURL surfaces on call.
func NewEmbedder(baseURL, model string, httpClient *http.Client) Embedder {
	trimmedBase, trimmedModel := strings.TrimRight(strings.TrimSpace(baseURL), "/"), strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = DefaultSensesModel
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 3 * time.Second}
	}
	httpClient = tracing.Client(httpClient)
	c := &httpEmbedder{}
	c.client = ollama.NewDynamicClient(func() ollama.Config {
		return ollama.Config{BaseURL: trimmedBase, Model: trimmedModel, NumCtx: ollama.DefaultNumCtx}
	})
	c.client.SetHTTPClient(httpClient)
	return c
}

// GenerateEmbedding embeds text for summary similarity search.
func (e *httpEmbedder) GenerateEmbedding(ctx context.Context, text string) ([]float32, error) {
	if e == nil || e.client == nil {
		return nil, errors.New("transcripts embedder is not initialized")
	}
	if strings.TrimSpace(text) == "" {
		return nil, errors.New("transcripts embed text cannot be empty")
	}
	return e.client.GenerateEmbedding(ctx, text, false, 1)
}
