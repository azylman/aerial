package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockTranscriptExtractor struct {
	existsOnDisk bool
	resp         string
	errDetail    string
}

func (m *mockTranscriptExtractor) SessionExistsOnDisk(convID string) bool {
	return m.existsOnDisk
}

func (m *mockTranscriptExtractor) ExtractResponseAndError(convID string) (string, string) {
	return m.resp, m.errDetail
}

func TestTurnResolver_NominalSuccess(t *testing.T) {
	extractor := &mockTranscriptExtractor{existsOnDisk: false}
	resolver := NewTurnResolver(extractor)

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
	if outcome.RecoveredFromTranscript {
		t.Errorf("expected RecoveredFromTranscript=false for nominal turn")
	}
	if outcome.Usage.TotalTokens != 30 {
		t.Errorf("expected total tokens 30, got %d", outcome.Usage.TotalTokens)
	}
	if outcome.SessionID != "conv-nominal" {
		t.Errorf("expected sessionID 'conv-nominal', got %q", outcome.SessionID)
	}
}

func TestTurnResolver_TranscriptRescueOnError(t *testing.T) {
	extractor := &mockTranscriptExtractor{
		existsOnDisk: true,
		resp:         "Recovered transcript response after daemon crash.",
	}
	resolver := NewTurnResolver(extractor)

	daemonErr := errors.New("daemon connection dropped unexpectedly")
	outcome := resolver.Resolve(context.Background(), nil, daemonErr, "conv-crash")

	if !outcome.IsSuccess {
		t.Fatalf("expected transcript rescue to succeed, got: %+v", outcome)
	}
	if !outcome.RecoveredFromTranscript {
		t.Errorf("expected RecoveredFromTranscript=true")
	}
	if outcome.Response != "Recovered transcript response after daemon crash." {
		t.Errorf("expected recovered transcript, got %q", outcome.Response)
	}
}

func TestTurnResolver_TranscriptRescueOnEmptyResult(t *testing.T) {
	extractor := &mockTranscriptExtractor{
		existsOnDisk: true,
		resp:         "Recovered response after empty result event.",
	}
	resolver := NewTurnResolver(extractor)

	res := &TurnResult{
		Response: "",
	}
	outcome := resolver.Resolve(context.Background(), res, nil, "conv-empty")

	if !outcome.IsSuccess {
		t.Fatalf("expected transcript rescue to succeed, got: %+v", outcome)
	}
	if !outcome.RecoveredFromTranscript {
		t.Errorf("expected RecoveredFromTranscript=true")
	}
	if outcome.Response != "Recovered response after empty result event." {
		t.Errorf("expected recovered response, got %q", outcome.Response)
	}
}

func TestTurnResolver_ToolCallOnlyInTranscriptNotRescued(t *testing.T) {
	extractor := &mockTranscriptExtractor{
		existsOnDisk: true,
		resp:         "[Tool Call Requested]: run_command",
	}
	resolver := NewTurnResolver(extractor)

	daemonErr := errors.New("daemon process terminated")
	outcome := resolver.Resolve(context.Background(), nil, daemonErr, "conv-tool")

	if outcome.IsSuccess {
		t.Fatalf("expected failure when transcript only has tool call request, got success: %+v", outcome)
	}
	if outcome.ErrorDetail != "daemon process terminated" {
		t.Errorf("expected error detail 'daemon process terminated', got %q", outcome.ErrorDetail)
	}
}

func TestTurnResolver_ContextWindowColdStartClassification(t *testing.T) {
	extractor := &mockTranscriptExtractor{existsOnDisk: false}
	resolver := NewTurnResolver(extractor)

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
	extractor := &mockTranscriptExtractor{existsOnDisk: false}
	resolver := NewTurnResolver(extractor)

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
	extractor := &mockTranscriptExtractor{existsOnDisk: false}
	resolver := NewTurnResolver(extractor)

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
	extractor := &mockTranscriptExtractor{existsOnDisk: false}
	resolver := NewTurnResolver(extractor)

	err := errors.New("fatal: unrecoverable crash in runtime")
	outcome := resolver.Resolve(context.Background(), nil, err, "conv-fatal")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if outcome.IsTransient {
		t.Errorf("expected fatal error to not be transient")
	}
}

func TestTurnResolver_EmptyResultWithoutTranscript(t *testing.T) {
	extractor := &mockTranscriptExtractor{existsOnDisk: false}
	resolver := NewTurnResolver(extractor)

	res := &TurnResult{Response: ""}
	outcome := resolver.Resolve(context.Background(), res, nil, "conv-no-trans")

	if outcome.IsSuccess {
		t.Fatalf("expected failure, got success")
	}
	if outcome.ErrorDetail != "daemon turn completed with empty response" {
		t.Errorf("unexpected error detail: %q", outcome.ErrorDetail)
	}
}
