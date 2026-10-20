package runner

import (
	"context"
	"strings"
	"time"
)

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

type TurnResolver struct{}

func NewTurnResolver() *TurnResolver { return &TurnResolver{} }

func (r *TurnResolver) Resolve(ctx context.Context, res *TurnResult, err error, sessionID string) TurnOutcome {
	outcome := TurnOutcome{SessionID: sessionID, RawError: err}
	if res != nil {
		outcome.Usage, outcome.Duration = res.Usage, res.Duration
		if res.ConversationID != "" && outcome.SessionID == "" {
			outcome.SessionID = res.ConversationID
		}
		if IsSubstantiveResponse(res.Response) {
			outcome.IsSuccess, outcome.Response = true, strings.TrimSpace(res.Response)
			return outcome
		}
	}

	errDetail := "unknown execution failure"
	if err != nil {
		errDetail = err.Error()
	} else if res != nil && strings.TrimSpace(res.Response) == "" {
		errDetail = "daemon turn completed with empty response"
	}
	outcome.ErrorDetail = errDetail

	lowerErr := strings.ToLower(errDetail)
	if hasContextWindowKeyword(lowerErr) {
		outcome.ErrorDetail = "prompt length exceeds maximum model context window (hard failure)"
	} else if hasCorruptionKeyword(lowerErr) {
		outcome.IsSessionCorruption = true
	} else if IsQuotaPause(errDetail, "") || strings.Contains(lowerErr, "resource_exhausted") || strings.Contains(lowerErr, "429") || strings.Contains(lowerErr, "rate limit") || strings.Contains(lowerErr, "out of memory") || strings.Contains(lowerErr, "transient") || (!isNonTransientError(errDetail) && !strings.Contains(lowerErr, "unrecoverable")) {
		outcome.IsTransient = true
	}
	return outcome
}
