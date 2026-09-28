package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestThrowawayTurnSink_AllMethodsAndSuccess(t *testing.T) {
	sink := NewThrowawayTurnSink()

	// Verify no-ops do not panic
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("bash", "echo hi")
	sink.OnTextDelta("some text")

	// Test successful result delivery
	sink.OnResult(&TurnResult{Response: "test response"})

	// sync.Once test: duplicate calls should be ignored
	sink.OnResult(&TurnResult{Response: "ignored second result"})
	sink.OnError(errors.New("ignored error after result"))

	res, err := sink.Result()
	if err != nil {
		t.Fatalf("unexpected error from Result: %v", err)
	}
	if res != "test response" {
		t.Fatalf("expected 'test response', got %q", res)
	}
}

func TestThrowawayTurnSink_NilResult(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnResult(nil)

	res, err := sink.ResultContext(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "" {
		t.Fatalf("expected empty string for nil TurnResult, got %q", res)
	}
}

func TestThrowawayTurnSink_ErrorDelivery(t *testing.T) {
	sink := NewThrowawayTurnSink()
	expectedErr := errors.New("turn execution crashed")
	sink.OnError(expectedErr)

	// Idempotent sync.Once check
	sink.OnError(errors.New("second error"))
	sink.OnResult(&TurnResult{Response: "ignored"})

	_, err := sink.ResultContext(context.Background())
	if err == nil || !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestThrowawayTurnSink_ContextCancelled(t *testing.T) {
	t.Run("PreCancelled", func(t *testing.T) {
		sink := NewThrowawayTurnSink()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := sink.ResultContext(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("CancelledWhileWaiting", func(t *testing.T) {
		sink := NewThrowawayTurnSink()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err := sink.ResultContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}
