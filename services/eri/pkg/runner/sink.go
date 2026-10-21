package runner

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

// IsSubstantiveResponse returns true if the response contains substantive text.
func IsSubstantiveResponse(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || strings.HasPrefix(trimmed, "[Tool Call Requested]:") {
		return false
	}
	return !strings.HasPrefix(trimmed, "<turn>") || strings.Contains(trimmed, "</turn>")
}

func runHook0(fn func()) {
	if fn != nil {
		fn()
	}
}

func runHook[T any](fn func(T), arg T) {
	if fn != nil {
		fn(arg)
	}
}

func runHook2[A, B any](fn func(A, B), a A, b B) {
	if fn != nil {
		fn(a, b)
	}
}

func selectContext[T any](ctx context.Context, resCh <-chan T, errCh <-chan error, empty T) (T, error) {
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	select {
	case <-ctx.Done():
		return empty, ctx.Err()
	case res := <-resCh:
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		return res, nil
	case err := <-errCh:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return empty, ctxErr
		}
		return empty, err
	}
}

// ThrowawayTurnSink implements TurnSink for fire-and-forget or transient turns.
type ThrowawayTurnSink struct {
	resCh chan string
	errCh chan error
	once  sync.Once
}

var _ TurnSink = (*ThrowawayTurnSink)(nil)

func NewThrowawayTurnSink() *ThrowawayTurnSink {
	return &ThrowawayTurnSink{resCh: make(chan string, 1), errCh: make(chan error, 1)}
}

func (s *ThrowawayTurnSink) OnTurnStarted()                                         {}
func (s *ThrowawayTurnSink) OnThinking()                                            {}
func (s *ThrowawayTurnSink) OnToolCall(_, _ string)                                 {}
func (s *ThrowawayTurnSink) OnToolCompleted(_, _ string, _ time.Duration, _ string) {}
func (s *ThrowawayTurnSink) OnSkillActivated(_, _ string)                           {}
func (s *ThrowawayTurnSink) OnTextDelta(_ string)                                   {}

func (s *ThrowawayTurnSink) OnResult(res *TurnResult) {
	s.once.Do(func() {
		var response string
		if res != nil {
			response = res.Response
		}
		s.resCh <- response
	})
}

func (s *ThrowawayTurnSink) OnError(err error) {
	s.once.Do(func() { s.errCh <- err })
}

func (s *ThrowawayTurnSink) ResultContext(ctx context.Context) (string, error) {
	return selectContext(ctx, s.resCh, s.errCh, "")
}

func (s *ThrowawayTurnSink) Result() (string, error) {
	return s.ResultContext(context.Background())
}

// BufferingTurnSinkConfig configures the hooks and behavior of a BufferingTurnSink.
type BufferingTurnSinkConfig struct {
	OnTurnStarted      func()
	OnThinking         func()
	OnStepStarted      func(stepIndex int)
	OnToolCall         func(toolName, commandName string)
	OnToolCompleted    func(toolName, mcpServer string, duration time.Duration, status string)
	OnSkillActivated   func(skillName, source string)
	OnTextDelta        func(delta string)
	OnComplete         func(res *TurnResult)
	OnError            func(err error)
	DisableDeltaRescue bool
}

// BufferingTurnSink implements TurnSink with text delta accumulation, hooks, and result sync.
type BufferingTurnSink struct {
	cfg                BufferingTurnSinkConfig
	mu                 sync.Mutex
	builder            strings.Builder
	emittedAny         bool
	purgedIntermediate bool
	resCh              chan *TurnResult
	errCh              chan error
	once               sync.Once
}

var (
	_ TurnSink      = (*BufferingTurnSink)(nil)
	_ StepAwareSink = (*BufferingTurnSink)(nil)
)

func NewBufferingTurnSink(cfg BufferingTurnSinkConfig) *BufferingTurnSink {
	return &BufferingTurnSink{cfg: cfg, resCh: make(chan *TurnResult, 1), errCh: make(chan error, 1)}
}

func (s *BufferingTurnSink) resetIntermediateBuffer() {
	s.mu.Lock()
	if strings.TrimSpace(s.builder.String()) != "" || s.emittedAny {
		s.purgedIntermediate = true
	}
	s.builder.Reset()
	s.emittedAny = false
	s.mu.Unlock()
}

func (s *BufferingTurnSink) OnTurnStarted() { runHook0(s.cfg.OnTurnStarted) }
func (s *BufferingTurnSink) OnThinking()    { runHook0(s.cfg.OnThinking) }

func (s *BufferingTurnSink) OnStepStarted(stepIndex int) {
	s.resetIntermediateBuffer()
	runHook(s.cfg.OnStepStarted, stepIndex)
}

func (s *BufferingTurnSink) OnToolCall(toolName, commandName string) {
	s.resetIntermediateBuffer()
	runHook2(s.cfg.OnToolCall, toolName, commandName)
}

func (s *BufferingTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	s.resetIntermediateBuffer()
	if s.cfg.OnToolCompleted != nil {
		s.cfg.OnToolCompleted(toolName, mcpServer, duration, status)
	}
}

func (s *BufferingTurnSink) OnSkillActivated(skillName, source string) {
	runHook2(s.cfg.OnSkillActivated, skillName, source)
}

func (s *BufferingTurnSink) OnTextDelta(delta string) {
	s.mu.Lock()
	s.builder.WriteString(delta)
	s.mu.Unlock()
	runHook(s.cfg.OnTextDelta, delta)
}

func (s *BufferingTurnSink) OnResult(res *TurnResult) {
	s.once.Do(func() {
		s.mu.Lock()
		accumulated, purged := s.builder.String(), s.purgedIntermediate
		s.mu.Unlock()

		if res == nil {
			res = &TurnResult{Response: accumulated}
		} else if strings.TrimSpace(accumulated) != "" {
			res.Response = accumulated
		} else if purged {
			res.Response = ""
		}
		runHook(s.cfg.OnComplete, res)
		s.resCh <- res
	})
}

func (s *BufferingTurnSink) OnError(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		accumulated, emitted := s.builder.String(), s.emittedAny
		s.mu.Unlock()

		if !s.cfg.DisableDeltaRescue && (IsSubstantiveResponse(accumulated) || emitted) {
			log.Printf("[BufferingTurnSink] Notice: recovering substantive response from streamed deltas despite error: %v", err)
			res := &TurnResult{Response: accumulated}
			runHook(s.cfg.OnComplete, res)
			s.resCh <- res
			return
		}
		runHook(s.cfg.OnError, err)
		s.errCh <- err
	})
}

func (s *BufferingTurnSink) Wait(ctx context.Context) (*TurnResult, error) {
	return selectContext(ctx, s.resCh, s.errCh, nil)
}

func (s *BufferingTurnSink) EmittedAny() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emittedAny
}

func (s *BufferingTurnSink) MarkEmitted() { s.SetEmitted(true) }

func (s *BufferingTurnSink) SetEmitted(emitted bool) {
	s.mu.Lock()
	s.emittedAny = emitted
	s.mu.Unlock()
}
