package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTurnResolver_NominalSuccess(t *testing.T) {
	resolver := NewTurnResolver()

	res := &TurnResult{
		ConversationID: "conv-nominal",
		Response:       "Nominal response from daemon.",
		Usage: TokenUsage{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
		Duration: 150 * time.Millisecond,
	}

	outcome := resolver.Resolve(context.Background(), res, nil, "conv-nominal")
	if !outcome.IsSuccess {
		t.Fatalf("expected success outcome, got failure: %+v", outcome)
	}
	if outcome.Response != "Nominal response from daemon." {
		t.Errorf("expected response %q, got %q", "Nominal response from daemon.", outcome.Response)
	}
	if outcome.Usage.TotalTokens != 30 {
		t.Errorf("expected total tokens 30, got %d", outcome.Usage.TotalTokens)
	}
	if outcome.SessionID != "conv-nominal" {
		t.Errorf("expected sessionID 'conv-nominal', got %q", outcome.SessionID)
	}
}

func TestTurnResolver_ContextWindowColdStartClassification(t *testing.T) {
	resolver := NewTurnResolver()

	err := errors.New("maximum context length exceeded: prompt too large")
	outcome := resolver.Resolve(context.Background(), nil, err, "conv-ctx-limit")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if outcome.IsTransient {
		t.Errorf("expected context window cold start to be non-transient (fail-fast)")
	}
	if outcome.IsSessionCorruption {
		t.Errorf("expected context window cold start to not be marked as session corruption")
	}
	if outcome.ErrorDetail != "prompt length exceeds maximum model context window (hard failure)" {
		t.Errorf("unexpected error detail: %q", outcome.ErrorDetail)
	}
}

func TestTurnResolver_SessionCorruptionClassification(t *testing.T) {
	resolver := NewTurnResolver()

	err := errors.New("corrupted session state: failed to parse session json")
	outcome := resolver.Resolve(context.Background(), nil, err, "conv-corrupt")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if !outcome.IsSessionCorruption {
		t.Errorf("expected session corruption flag to be true")
	}
	if outcome.IsTransient {
		t.Errorf("expected session corruption to be non-transient")
	}
}

func TestTurnResolver_QuotaPauseClassification(t *testing.T) {
	resolver := NewTurnResolver()

	err := errors.New("API error: 429 RESOURCE_EXHAUSTED rate limit exceeded")
	outcome := resolver.Resolve(context.Background(), nil, err, "conv-quota")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if !outcome.IsTransient {
		t.Errorf("expected quota pause to be transient")
	}
}

func TestTurnResolver_FatalStderrClassification(t *testing.T) {
	resolver := NewTurnResolver()

	err := errors.New("fatal: unrecoverable crash in runtime")
	outcome := resolver.Resolve(context.Background(), nil, err, "conv-fatal")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if outcome.IsTransient {
		t.Errorf("expected fatal error to not be transient")
	}
}

func TestTurnResolver_EmptyResultClassification(t *testing.T) {
	resolver := NewTurnResolver()

	res := &TurnResult{Response: ""}
	outcome := resolver.Resolve(context.Background(), res, nil, "conv-empty")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if outcome.ErrorDetail != "daemon turn completed with empty response" {
		t.Errorf("unexpected error detail: %q", outcome.ErrorDetail)
	}
	if !outcome.IsTransient {
		t.Errorf("expected empty response to be transient")
	}
}

func TestTurnResolver_WhitespaceOnlyResponse(t *testing.T) {
	resolver := NewTurnResolver()

	res := &TurnResult{Response: "   \n\t  "}
	outcome := resolver.Resolve(context.Background(), res, nil, "conv-ws")

	if outcome.IsSuccess {
		t.Fatalf("expected failure for whitespace-only response, got success")
	}
	if outcome.ErrorDetail != "daemon turn completed with empty response" {
		t.Errorf("unexpected error detail: %q", outcome.ErrorDetail)
	}
}

func TestTurnResolver_DaemonErrorClassification(t *testing.T) {
	resolver := NewTurnResolver()

	err := errors.New("daemon connection dropped unexpectedly")
	outcome := resolver.Resolve(context.Background(), nil, err, "conv-crash")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if outcome.ErrorDetail != "daemon connection dropped unexpectedly" {
		t.Errorf("expected error detail 'daemon connection dropped unexpectedly', got %q", outcome.ErrorDetail)
	}
	if !outcome.IsTransient {
		t.Errorf("expected generic daemon connection error to be transient")
	}
}
