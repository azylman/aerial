package runner

import "context"

// LLMFunc represents a stateless LLM invocation that generates text
// given a context, model name, and prompt string.
type LLMFunc func(ctx context.Context, model, prompt string) (string, error)

type llmCallerKeyCtx struct{}

// WithLLMCallerKey returns ctx carrying the caller key that names the logical
// LLM operation (e.g. "classify:<channel>:<message>"). Keyed LLMFuncs forward
// it so a retried call attaches to the recorded result; others ignore it.
func WithLLMCallerKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, llmCallerKeyCtx{}, key)
}

// LLMCallerKey returns the caller key set by WithLLMCallerKey, or "".
func LLMCallerKey(ctx context.Context) string {
	if key, ok := ctx.Value(llmCallerKeyCtx{}).(string); ok {
		return key
	}
	return ""
}
