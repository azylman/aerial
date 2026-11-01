package transcript

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

// SearchTranscripts queries past session summaries and execution steps.
// Supported modes:
//   - "sessions": Hybrid RRF via store.SearchSessionSummaries
//   - "steps": FTS via store.SearchTranscriptSteps
//   - "auto" (default): Searches both sessions and steps
func SearchTranscripts(
	ctx context.Context,
	store db.TranscriptStore,
	embedder EmbedderFunc,
	query, mode, sessionFilter, toolFilter string,
	limit int,
	since ...time.Time,
) (SearchResult, error) {
	var sinceTime time.Time
	if len(since) > 0 {
		sinceTime = since[0]
	}
	return ExecuteTranscriptSearch(ctx, store, embedder, SearchOptions{
		Query:         query,
		Mode:          mode,
		SessionFilter: sessionFilter,
		SessionID:     sessionFilter,
		ToolFilter:    toolFilter,
		Limit:         limit,
		Since:         sinceTime,
	})
}

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
	query string,
	limit int,
) ([]db.SessionSummary, error) {
	var embedding []float32
	if embedder != nil {
		if emb, err := embedder(ctx, query); err == nil && len(emb) == db.ExpectedEmbeddingDim {
			embedding = emb
		}
	}
	return store.SearchSessionSummaries(ctx, embedding, query, limit, 0.0)
}

// ExecuteTranscriptSearch executes a transcript search with full SearchOptions.
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
		sessions, err := searchSessionSummariesHelper(ctx, store, embedder, trimmedQuery, limit)
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
		steps, err := store.SearchTranscriptSteps(ctx, trimmedQuery, sessID, opts.ToolFilter, limit, opts.Since)
		if err != nil {
			return SearchResult{}, fmt.Errorf("search transcript steps: %w", err)
		}
		result.Steps = steps
	}

	result.Total = len(result.Sessions) + len(result.Steps)
	return result, nil
}

// GetTranscriptStepWindow fetches a step and its surrounding context window from the transcript store.
func GetTranscriptStepWindow(
	ctx context.Context,
	store db.TranscriptStore,
	sessionID string,
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

	steps, err := store.GetTranscriptStepRange(ctx, strings.TrimSpace(sessionID), startStep, endStep)
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
