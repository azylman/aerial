package runner

import (
	"context"
	"strings"
	"time"
)

// TurnOutcome encapsulates the resolved status and metadata of a turn execution.
type TurnOutcome struct {
	Response            string
	IsSuccess           bool
	IsTransient         bool
	IsSessionCorruption bool
	ErrorDetail         string
	Usage               TokenUsage
	Duration            time.Duration
	SessionID           string
	RawError            error
}

// TurnResolver coordinates outcome determination and failure classification
// across persistent daemon turns and legacy single-shot executions.
type TurnResolver struct{}

// NewTurnResolver creates a TurnResolver.
func NewTurnResolver() *TurnResolver {
	return &TurnResolver{}
}

// Resolve evaluates a turn result and error, determining nominal success or classifying failures.
func (r *TurnResolver) Resolve(ctx context.Context, res *TurnResult, err error, sessionID string) TurnOutcome {
	outcome := TurnOutcome{
		SessionID: sessionID,
		RawError:  err,
	}

	// 1. Nominal Result Evaluation
	if res != nil {
		outcome.Usage = res.Usage
		outcome.Duration = res.Duration
		if res.ConversationID != "" && outcome.SessionID == "" {
			outcome.SessionID = res.ConversationID
		}
		if IsSubstantiveResponse(res.Response) {
			outcome.IsSuccess = true
			outcome.Response = strings.TrimSpace(res.Response)
			return outcome
		}
	}

	// 2. Genuine Failure Classification
	outcome.IsSuccess = false
	var errDetail string
	if err != nil {
		errDetail = err.Error()
	} else if res != nil && strings.TrimSpace(res.Response) == "" {
		errDetail = "daemon turn completed with empty response"
	} else {
		errDetail = "unknown execution failure"
	}
	outcome.ErrorDetail = errDetail

	lowerErr := strings.ToLower(errDetail)

	// Check context window / token limit cold start failure
	isContextWindow := strings.Contains(lowerErr, "context window") ||
		strings.Contains(lowerErr, "context length") ||
		strings.Contains(lowerErr, "maximum context length") ||
		strings.Contains(lowerErr, "token limit exceeded") ||
		strings.Contains(lowerErr, "prompt is too long") ||
		strings.Contains(lowerErr, "request too large")

	if isContextWindow {
		outcome.IsTransient = false
		outcome.IsSessionCorruption = false
		outcome.ErrorDetail = "prompt length exceeds maximum model context window (hard failure)"
		return outcome
	}

	// Check session corruption
	corruptionKeywords := []string{
		"session corrupt",
		"corrupted session",
		"invalid session",
		"session not found",
		"failed to load conversation",
		"corrupted transcript",
		"failed to parse session",
		"stream was interrupted",
		"the stream was interrupted",
	}
	for _, kw := range corruptionKeywords {
		if strings.Contains(lowerErr, kw) {
			outcome.IsSessionCorruption = true
			outcome.IsTransient = false
			return outcome
		}
	}

	// Check quota pauses and rate limits
	if IsQuotaPause(errDetail, "") || strings.Contains(lowerErr, "resource_exhausted") || strings.Contains(lowerErr, "429") || strings.Contains(lowerErr, "rate limit") || strings.Contains(lowerErr, "out of memory") || strings.Contains(lowerErr, "transient") {
		outcome.IsTransient = true
		return outcome
	}

	if isNonTransientError(errDetail) || strings.Contains(lowerErr, "unrecoverable") {
		outcome.IsTransient = false
		return outcome
	}

	// Default to transient for daemon errors (network blips, EOF, timeouts)
	outcome.IsTransient = true
	return outcome
}
