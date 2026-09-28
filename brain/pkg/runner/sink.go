package runner

import (
	"context"
	"sync"
)

// ThrowawayTurnSink implements TurnSink for fire-and-forget or transient turns
// such as ambient classification and title summarization.
type ThrowawayTurnSink struct {
	resCh chan string
	errCh chan error
	once  sync.Once
}

var _ TurnSink = (*ThrowawayTurnSink)(nil)

// NewThrowawayTurnSink creates a new buffered ThrowawayTurnSink.
func NewThrowawayTurnSink() *ThrowawayTurnSink {
	return &ThrowawayTurnSink{
		resCh: make(chan string, 1),
		errCh: make(chan error, 1),
	}
}

// OnTurnStarted is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnTurnStarted() {}

// OnThinking is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnThinking() {}

// OnToolCall is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnToolCall(toolName, commandName string) {}

// OnTextDelta is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnTextDelta(delta string) {}

// OnResult delivers the final response string to resCh guarded by sync.Once.
func (s *ThrowawayTurnSink) OnResult(res *TurnResult) {
	s.once.Do(func() {
		var response string
		if res != nil {
			response = res.Response
		}
		s.resCh <- response
	})
}

// OnError delivers the error to errCh guarded by sync.Once.
func (s *ThrowawayTurnSink) OnError(err error) {
	s.once.Do(func() {
		s.errCh <- err
	})
}

// ResultContext blocks waiting for turn result, error, or context cancellation.
func (s *ThrowawayTurnSink) ResultContext(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-s.resCh:
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return res, nil
	case err := <-s.errCh:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", err
	}
}

// Result blocks waiting for turn result or error without a deadline.
func (s *ThrowawayTurnSink) Result() (string, error) {
	return s.ResultContext(context.Background())
}
