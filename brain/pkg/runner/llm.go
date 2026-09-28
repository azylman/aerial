package runner

import "context"

// LLMFunc represents a stateless LLM invocation that generates text
// given a context, model name, and prompt string.
type LLMFunc func(ctx context.Context, model, prompt string) (string, error)
