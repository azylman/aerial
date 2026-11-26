// Package ericlient hosts the outbound Eri client and payloads for vanadis-restate.
package ericlient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
)

const statusOK = "ok"

type agentContextKey struct{}

type widgetTimeoutContextKey struct{}

type widgetTimeoutConfig struct {
	parentCtx context.Context //nolint:containedctx // preserves un-timed base context for post-lock timeout derivation
	timeout   time.Duration
}

// WithWidgetTimeout returns a copy of parent context carrying the requested widget timeout duration
// and storing the un-timed parent context so post-lock timers can derive a fresh deadline.
// It returns ctx unchanged if ctx == nil or timeout <= 0.
func WithWidgetTimeout(ctx context.Context, timeout time.Duration) context.Context {
	if ctx == nil || timeout <= 0 {
		return ctx
	}
	parentCtx := ctx
	if cfg, ok := ctx.Value(widgetTimeoutContextKey{}).(widgetTimeoutConfig); ok && cfg.parentCtx != nil {
		parentCtx = cfg.parentCtx
	}
	return context.WithValue(ctx, widgetTimeoutContextKey{}, widgetTimeoutConfig{
		parentCtx: parentCtx,
		timeout:   timeout,
	})
}

// WidgetTimeoutFromContext extracts the stored un-timed parent context and timeout duration > 0
// when present, else returns (ctx, 0).
func WidgetTimeoutFromContext(ctx context.Context) (context.Context, time.Duration) {
	if ctx == nil {
		return nil, 0
	}
	if cfg, ok := ctx.Value(widgetTimeoutContextKey{}).(widgetTimeoutConfig); ok && cfg.timeout > 0 {
		if cfg.parentCtx != nil {
			return cfg.parentCtx, cfg.timeout
		}
		return ctx, cfg.timeout
	}
	return ctx, 0
}

// WithAgent returns a copy of parent context carrying the given agent identifier.
func WithAgent(ctx context.Context, agent string) context.Context {
	clean := strings.TrimSpace(agent)
	if clean == "" {
		return ctx
	}
	return context.WithValue(ctx, agentContextKey{}, clean)
}

// AgentFromContext extracts the agent identifier from the context, or returns "".
func AgentFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(agentContextKey{}).(string); ok {
		return strings.TrimSpace(val)
	}
	return ""
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task for widget rendering.
type StatusWidgetChunk struct {
	ID            string    `json:"id,omitempty"`
	Index         int       `json:"index"`
	Mode          string    `json:"mode,omitempty"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	ActiveStep    string    `json:"active_step,omitempty"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	Agent     string              `json:"agent,omitempty"`
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	SkipSink  bool                `json:"skip_sink,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
	StartedAt time.Time           `json:"started_at,omitempty"`
}

// SyncChunkPRWidgetRequest carries optional streaming state for a single PR chunk.
type SyncChunkPRWidgetRequest struct {
	PRNumber   int    `json:"pr_number"`
	PRStatus   string `json:"pr_status"`
	PRBanner   string `json:"pr_banner"`
	ActiveStep string `json:"active_step,omitempty"`
	PRFound    bool   `json:"pr_found"`
}

// DispatchRequest aliases the brain dispatch request wire type.
type DispatchRequest = eriwire.BrainDispatchRequest

// EriClient describes the outbound communication interface to Eri.
type EriClient interface {
	Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error
	DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error
	CancelDispatch(ctx context.Context, chunkID string) error
	Prompt(ctx context.Context, threadID, prompt string) error
	Widget(ctx context.Context, req StatusWidgetRequest) (string, error)
	// DeleteWidget removes a status widget message posted by agent. A message
	// that is already gone is a success.
	DeleteWidget(ctx context.Context, agent, channelID, messageID string) error
	Message(ctx context.Context, channelID, content string) error
}

func deterministicPromptMessageID(threadID, prompt string) string {
	sum := sha256.Sum256([]byte(threadID + "\x00" + prompt))
	return "vanadis-prompt-" + hex.EncodeToString(sum[:16])
}
