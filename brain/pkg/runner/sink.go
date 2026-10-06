package runner

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

// IsSubstantiveResponse returns true if the response contains substantive text,
// rejecting empty strings, whitespace, intermediate tool call indicators, or incomplete tags.
func IsSubstantiveResponse(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, "[Tool Call Requested]:") {
		return false
	}
	if strings.HasPrefix(trimmed, "<turn>") && !strings.Contains(trimmed, "</turn>") {
		return false
	}
	return true
}

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

// OnToolCompleted is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {}

// OnSkillActivated is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnSkillActivated(skillName, source string) {}

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

// BufferingTurnSinkConfig configures the hooks and behavior of a BufferingTurnSink.
type BufferingTurnSinkConfig struct {
	OnTurnStarted      func()
	OnThinking         func()
	OnToolCall         func(toolName, commandName string)
	OnToolCompleted    func(toolName, mcpServer string, duration time.Duration, status string)
	OnSkillActivated   func(skillName, source string)
	OnTextDelta        func(delta string)
	OnComplete         func(res *TurnResult)
	OnError            func(err error)
	DisableDeltaRescue bool
}

// BufferingTurnSink implements TurnSink with thread-safe text delta accumulation,
// pluggable hooks, substantive delta error recovery, and result synchronization.
type BufferingTurnSink struct {
	cfg           BufferingTurnSinkConfig
	mu            sync.Mutex
	builder       strings.Builder
	emittedAny    bool
	purgedPreTool bool
	resCh         chan *TurnResult
	errCh         chan error
	once          sync.Once
}

var _ TurnSink = (*BufferingTurnSink)(nil)

// NewBufferingTurnSink constructs an initialized BufferingTurnSink.
func NewBufferingTurnSink(cfg BufferingTurnSinkConfig) *BufferingTurnSink {
	return &BufferingTurnSink{
		cfg:   cfg,
		resCh: make(chan *TurnResult, 1),
		errCh: make(chan error, 1),
	}
}

// OnTurnStarted invokes the OnTurnStarted hook if configured.
func (s *BufferingTurnSink) OnTurnStarted() {
	if s.cfg.OnTurnStarted != nil {
		s.cfg.OnTurnStarted()
	}
}

// OnThinking invokes the OnThinking hook if configured.
func (s *BufferingTurnSink) OnThinking() {
	if s.cfg.OnThinking != nil {
		s.cfg.OnThinking()
	}
}

// OnToolCall resets the delta buffer to purge pre-tool monologue/prompt echoes,
// and invokes the OnToolCall hook if configured.
func (s *BufferingTurnSink) OnToolCall(toolName, commandName string) {
	s.mu.Lock()
	if strings.TrimSpace(s.builder.String()) != "" || s.emittedAny {
		s.purgedPreTool = true
	}
	s.builder.Reset()
	s.emittedAny = false
	s.mu.Unlock()

	if s.cfg.OnToolCall != nil {
		s.cfg.OnToolCall(toolName, commandName)
	}
}

// OnToolCompleted resets the delta buffer to purge stray stream chunks leaked during
// tool execution, and invokes the OnToolCompleted hook if configured.
func (s *BufferingTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	s.mu.Lock()
	if strings.TrimSpace(s.builder.String()) != "" || s.emittedAny {
		s.purgedPreTool = true
	}
	s.builder.Reset()
	s.emittedAny = false
	s.mu.Unlock()

	if s.cfg.OnToolCompleted != nil {
		s.cfg.OnToolCompleted(toolName, mcpServer, duration, status)
	}
}

// OnSkillActivated invokes the OnSkillActivated hook if configured.
func (s *BufferingTurnSink) OnSkillActivated(skillName, source string) {
	if s.cfg.OnSkillActivated != nil {
		s.cfg.OnSkillActivated(skillName, source)
	}
}

// OnTextDelta accumulates the delta under lock and invokes the OnTextDelta hook.
func (s *BufferingTurnSink) OnTextDelta(delta string) {
	s.mu.Lock()
	s.builder.WriteString(delta)
	s.mu.Unlock()

	if s.cfg.OnTextDelta != nil {
		s.cfg.OnTextDelta(delta)
	}
}

// OnResult finalizes the turn, preferring accumulated deltas if non-empty,
// falling back to res.Response if the buffer is empty,
// invoking the OnComplete hook, and delivering to resCh.
func (s *BufferingTurnSink) OnResult(res *TurnResult) {
	s.once.Do(func() {
		s.mu.Lock()
		accumulated := s.builder.String()
		purged := s.purgedPreTool
		s.mu.Unlock()

		if res == nil {
			res = &TurnResult{Response: accumulated}
		} else if strings.TrimSpace(accumulated) != "" {
			res.Response = accumulated
		} else if purged {
			// If pre-tool text deltas were streamed and purged upon tool execution,
			// but no post-tool text deltas were emitted, any text in res.Response
			// was intermediate monologue or prompt echo. Suppress it so intermediate
			// chatter does not resurrect.
			res.Response = ""
		}

		if s.cfg.OnComplete != nil {
			s.cfg.OnComplete(res)
		}

		s.resCh <- res
	})
}

// OnError handles turn failure, salvaging substantive deltas if available
// or delivering the error to errCh.
func (s *BufferingTurnSink) OnError(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		accumulated := s.builder.String()
		emitted := s.emittedAny
		s.mu.Unlock()

		if !s.cfg.DisableDeltaRescue && (IsSubstantiveResponse(accumulated) || emitted) {
			log.Printf("[BufferingTurnSink] Notice: recovering substantive response from streamed deltas despite error: %v", err)
			res := &TurnResult{
				Response: accumulated,
			}
			if s.cfg.OnComplete != nil {
				s.cfg.OnComplete(res)
			}
			s.resCh <- res
			return
		}

		if s.cfg.OnError != nil {
			s.cfg.OnError(err)
		}

		s.errCh <- err
	})
}

// Wait blocks waiting for turn result, error, or context cancellation.
func (s *BufferingTurnSink) Wait(ctx context.Context) (*TurnResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-s.resCh:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return res, nil
	case err := <-s.errCh:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
}

// AccumulatedText returns the accumulated response string under mutex protection.
func (s *BufferingTurnSink) AccumulatedText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builder.String()
}

// EmittedAny reports whether any streamed tokens were emitted.
func (s *BufferingTurnSink) EmittedAny() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emittedAny
}

// MarkEmitted marks that streamed tokens have been emitted to consumers.
func (s *BufferingTurnSink) MarkEmitted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emittedAny = true
}

// SetEmitted sets the emitted state.
func (s *BufferingTurnSink) SetEmitted(emitted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emittedAny = emitted
}
