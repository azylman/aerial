package transcript

import (
	"context"
	"fmt"
	"strings"

	"github.com/azylman/aerial/brain/pkg/db"
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

	trimmedQuery := strings.TrimSpace(query)
	if limit <= 0 {
		limit = 10
	}

	normalizedMode := strings.ToLower(strings.TrimSpace(mode))
	if normalizedMode == "" || (normalizedMode != "sessions" && normalizedMode != "steps" && normalizedMode != "auto") {
		normalizedMode = "auto"
	}

	result := SearchResult{
		Query: trimmedQuery,
		Mode:  normalizedMode,
	}

	// 1. Session Summaries Search
	if normalizedMode == "sessions" || normalizedMode == "auto" {
		var embedding []float32
		if embedder != nil && trimmedQuery != "" {
			if emb, err := embedder(ctx, trimmedQuery); err == nil && len(emb) == db.ExpectedEmbeddingDim {
				embedding = emb
			}
		}
		sessions, err := store.SearchSessionSummaries(ctx, embedding, trimmedQuery, limit, 0.0)
		if err != nil {
			return SearchResult{}, fmt.Errorf("search session summaries: %w", err)
		}
		result.Sessions = sessions
	}

	// 2. Transcript Steps Search
	if normalizedMode == "steps" || normalizedMode == "auto" {
		steps, err := store.SearchTranscriptSteps(ctx, trimmedQuery, sessionFilter, toolFilter, limit)
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
