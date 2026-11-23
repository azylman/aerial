package server

import (
	"context"
	"net/http"

	"github.com/azylman/aerial/pkg/eriwire"
)

// withLLMKey attaches an Idempotency-Key identity to ctx for the next Complete call.
func withLLMKey(ctx context.Context, key string) context.Context {
	return eriwire.WithLLMKey(ctx, key)
}

// LLMClient generates a summary completion for a prompt.
type LLMClient interface {
	Complete(ctx context.Context, prompt string) (string, error)
}

// NewLLMClient builds an LLMClient over eri's internal LLM proxy URL.
// A nil httpClient gets a bounded default; an empty URL surfaces on Complete.
func NewLLMClient(eriLLMURL string, httpClient *http.Client) LLMClient {
	return eriwire.NewInternalLLMClient(eriLLMURL, httpClient, eriwire.EffortLow)
}
