package runner

import "time"

type leaseTurnSinkWrapper struct {
	inner TurnSink
	buf   *BufferingTurnSink
}

func (s *leaseTurnSinkWrapper) OnTurnStarted() {
	if s.inner != nil {
		s.inner.OnTurnStarted()
	}
	s.buf.OnTurnStarted()
}

func (s *leaseTurnSinkWrapper) OnThinking() {
	if s.inner != nil {
		s.inner.OnThinking()
	}
	s.buf.OnThinking()
}

func (s *leaseTurnSinkWrapper) OnStepStarted(idx int) {
	if s.inner != nil {
		if sa, ok := s.inner.(StepAwareSink); ok {
			sa.OnStepStarted(idx)
		}
	}
	if s.buf != nil {
		s.buf.OnStepStarted(idx)
	}
}

func (s *leaseTurnSinkWrapper) OnToolCall(call ToolCall) {
	if s.inner != nil {
		s.inner.OnToolCall(call)
	}
	s.buf.OnToolCall(call)
}

func (s *leaseTurnSinkWrapper) OnToolCompleted(call ToolCall, mcp string, d time.Duration, status string) {
	if s.inner != nil {
		s.inner.OnToolCompleted(call, mcp, d, status)
	}
	s.buf.OnToolCompleted(call, mcp, d, status)
}

func (s *leaseTurnSinkWrapper) OnSkillActivated(skill, src string) {
	if s.inner != nil {
		s.inner.OnSkillActivated(skill, src)
	}
	s.buf.OnSkillActivated(skill, src)
}

func (s *leaseTurnSinkWrapper) OnTextDelta(delta string) {
	if s.inner != nil {
		s.inner.OnTextDelta(delta)
	}
	s.buf.OnTextDelta(delta)
}

func (s *leaseTurnSinkWrapper) OnResult(res *TurnResult) {
	if s.inner != nil {
		s.inner.OnResult(res)
	}
	s.buf.OnResult(res)
}

func (s *leaseTurnSinkWrapper) OnError(err error) {
	if s.inner != nil {
		s.inner.OnError(err)
	}
	s.buf.OnError(err)
}

func (s *leaseTurnSinkWrapper) CompleteAsyncSubagent(summary SubagentTraceSummary) {
	if ts, ok := s.inner.(*TracingTurnSink); ok && ts != nil {
		ts.CompleteAsyncSubagent(summary)
	}
}
