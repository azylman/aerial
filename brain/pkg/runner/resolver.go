package runner

import (
	"context"
	"log"
	"strings"
	"time"
)

// TranscriptExtractor provides an interface to query conversation transcripts on disk.
type TranscriptExtractor interface {
	SessionExistsOnDisk(convID string) bool
	ExtractResponseAndError(convID string) (response string, errDetail string)
}

// TurnOutcome encapsulates the resolved status and metadata of a turn execution.
type TurnOutcome struct {
	Response                string
	IsSuccess               bool
	IsTransient             bool
	IsSessionCorruption     bool
	ErrorDetail             string
	RecoveredFromTranscript bool
	Usage                   TokenUsage
	Duration                time.Duration
	SessionID               string
	RawError                error
}

// TurnResolver coordinates outcome determination and fallback transcript recovery
// across persistent daemon turns and legacy single-shot executions.
type TurnResolver struct {
	Extractor TranscriptExtractor
}

// NewTurnResolver creates a TurnResolver backed by the provided TranscriptExtractor.
func NewTurnResolver(extractor TranscriptExtractor) *TurnResolver {
	return &TurnResolver{
		Extractor: extractor,
	}
}

// Resolve evaluates a turn result and error, applying substantive recovery and transcript rescue.
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

	// 2. Transcript Rescue
	// If error occurred or result response was non-substantive, check transcript on disk.
	// Never perform transcript rescue on service unavailable (503) or quota pauses (429),
	// which are transient service errors that must be retried.
	isServiceUnavailable := err != nil && (strings.Contains(strings.ToLower(err.Error()), "503") ||
		strings.Contains(strings.ToLower(err.Error()), "service unavailable") ||
		IsQuotaPause(err.Error(), ""))
	if !isServiceUnavailable && r.Extractor != nil && outcome.SessionID != "" && r.Extractor.SessionExistsOnDisk(outcome.SessionID) {
		if transcriptResp, _ := r.Extractor.ExtractResponseAndError(outcome.SessionID); IsSubstantiveResponse(transcriptResp) && !strings.HasPrefix(transcriptResp, "[Tool Call Requested]:") {
			log.Printf("[TurnResolver] Recovered substantive response directly from session %s transcript after runner error/empty result: %v", outcome.SessionID, err)
			outcome.IsSuccess = true
			outcome.RecoveredFromTranscript = true
			outcome.Response = strings.TrimSpace(transcriptResp)
			return outcome
		}
	}

	// 3. Genuine Failure Classification
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
