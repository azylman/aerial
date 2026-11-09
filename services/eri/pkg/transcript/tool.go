package transcript

import (
	"context"
	"fmt"
	"strings"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

func normalizeSearchMode(mode string) string {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	if normalized != "sessions" && normalized != "steps" {
		return "auto"
	}
	return normalized
}

func searchSessionSummariesHelper(
	ctx context.Context,
	store db.TranscriptStore,
	embedder EmbedderFunc,
	stack, query string,
	limit int,
) ([]db.SessionSummary, error) {
	var embedding []float32
	if embedder != nil {
		if emb, err := embedder(ctx, query); err == nil && len(emb) == db.ExpectedEmbeddingDim {
			embedding = emb
		}
	}
	return store.SearchSessionSummaries(ctx, stack, embedding, query, limit, 0.0)
}

// ExecuteTranscriptSearch queries past session summaries and execution steps across the shared index.
// Supported modes:
//   - "sessions": Hybrid RRF via store.SearchSessionSummaries
//   - "steps": FTS via store.SearchTranscriptSteps
//   - "auto" (default): Searches both sessions and steps
//
// An empty opts.Stack searches every stack. opts.ToolFilter matches either a normalized tool kind
// (shell, read, mcp:<server>, ...) or an engine-native tool name (Bash, run_command, ...).
func ExecuteTranscriptSearch(
	ctx context.Context,
	store db.TranscriptStore,
	embedder EmbedderFunc,
	opts SearchOptions,
) (SearchResult, error) {
	if store == nil {
		return SearchResult{}, fmt.Errorf("transcript store cannot be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return SearchResult{}, ctx.Err()
	}

	trimmedQuery := strings.TrimSpace(opts.Query)
	sessID := strings.TrimSpace(opts.SessionFilter)
	if sessID == "" {
		sessID = strings.TrimSpace(opts.SessionID)
	}
	limit := max(1, opts.Limit)
	if opts.Limit <= 0 {
		limit = 10
	}

	normalizedMode := normalizeSearchMode(opts.Mode)

	if trimmedQuery == "" && (normalizedMode != "steps" || sessID == "") {
		return SearchResult{
			Query: trimmedQuery,
			Mode:  normalizedMode,
		}, nil
	}

	result := SearchResult{
		Query: trimmedQuery,
		Mode:  normalizedMode,
	}

	// 1. Session Summaries Search
	if (normalizedMode == "sessions" || normalizedMode == "auto") && trimmedQuery != "" {
		sessions, err := searchSessionSummariesHelper(ctx, store, embedder, opts.Stack, trimmedQuery, limit)
		if err != nil {
			return SearchResult{}, fmt.Errorf("search session summaries: %w", err)
		}
		for i := range sessions {
			sessions[i].Summary = CleanSessionSummary(sessions[i].Summary, DefaultSummaryMaxRunes)
		}
		result.Sessions = sessions
	}

	// 2. Transcript Steps Search
	if normalizedMode == "steps" || normalizedMode == "auto" {
		steps, err := store.SearchTranscriptSteps(ctx, opts.Stack, trimmedQuery, sessID, opts.ToolFilter, limit, opts.Since)
		if err != nil {
			return SearchResult{}, fmt.Errorf("search transcript steps: %w", err)
		}
		result.Steps = steps
	}

	result.Total = len(result.Sessions) + len(result.Steps)
	return result, nil
}

// GetTranscriptStepWindow fetches a step and its surrounding context window from the transcript store.
// An empty stack matches the session on any stack.
func GetTranscriptStepWindow(
	ctx context.Context,
	store db.TranscriptStore,
	stack, sessionID string,
	stepIndex, contextWindow int,
) (StepWindowResult, error) {
	if store == nil {
		return StepWindowResult{}, fmt.Errorf("transcript store cannot be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return StepWindowResult{}, ctx.Err()
	}

	if contextWindow < 0 {
		contextWindow = 2
	}
	if contextWindow > 20 {
		contextWindow = 20
	}

	startStep := max(0, stepIndex-contextWindow)
	endStep := stepIndex + contextWindow

	steps, err := store.GetTranscriptStepRange(ctx, strings.TrimSpace(stack), strings.TrimSpace(sessionID), startStep, endStep)
	if err != nil {
		return StepWindowResult{}, fmt.Errorf("get transcript step range: %w", err)
	}
	if steps == nil {
		steps = []db.TranscriptStep{}
	}

	return StepWindowResult{
		SessionID:  strings.TrimSpace(sessionID),
		CenterStep: stepIndex,
		Context:    contextWindow,
		Steps:      steps,
	}, nil
}
