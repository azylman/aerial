package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

func init() {
	testPoolHook = func(cfg WorkerPoolConfig) runner.AgentPool {
		if cfg.RunnerFunc != nil || cfg.RunnerWithOptionsFunc != nil {
			return newTestRunnerAgentPool(cfg, cfg.ProcessPool)
		}
		return nil
	}
}

type testRunnerAgentPool struct {
	cfg         WorkerPoolConfig
	trackerPool runner.AgentPool
	mu          sync.Mutex
	sessions    map[string]*testRunnerAgentSession
	locks       map[string]*sync.Mutex
}

func newTestRunnerAgentPool(cfg WorkerPoolConfig, trackerPool runner.AgentPool) *testRunnerAgentPool {
	return &testRunnerAgentPool{
		cfg:         cfg,
		trackerPool: trackerPool,
		sessions:    make(map[string]*testRunnerAgentSession),
		locks:       make(map[string]*sync.Mutex),
	}
}

func (p *testRunnerAgentPool) getTargetLock(targetKey string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.locks == nil {
		p.locks = make(map[string]*sync.Mutex)
	}
	l, ok := p.locks[targetKey]
	if !ok {
		l = &sync.Mutex{}
		p.locks[targetKey] = l
	}
	return l
}

func (p *testRunnerAgentPool) Get(threadID string) (*runner.StreamingDaemon, bool) {
	if p.trackerPool != nil {
		if tp, ok := p.trackerPool.(interface {
			Get(string) (*runner.StreamingDaemon, bool)
		}); ok {
			return tp.Get(threadID)
		}
	}
	return nil, false
}

func (p *testRunnerAgentPool) GetOrCreate(ctx context.Context, threadID string, sessionID string) (*runner.StreamingDaemon, error) {
	if p.trackerPool != nil {
		if tp, ok := p.trackerPool.(interface {
			GetOrCreate(context.Context, string, string) (*runner.StreamingDaemon, error)
		}); ok {
			return tp.GetOrCreate(ctx, threadID, sessionID)
		}
	}
	return nil, fmt.Errorf("no underlying process pool")
}

func (p *testRunnerAgentPool) Underlying() *runner.UnifiedProcessPool {
	if p.trackerPool != nil {
		if up, ok := p.trackerPool.(*runner.UnifiedProcessPool); ok {
			return up
		}
	}
	return nil
}

func (p *testRunnerAgentPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (runner.AgentSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	trimmed := strings.TrimSpace(sessionID)
	if s, ok := p.sessions[targetKey]; ok {
		s.mu.Lock()
		if trimmed != "" {
			s.sessionID = trimmed
		}
		s.mu.Unlock()
		return s, nil
	}
	s := &testRunnerAgentSession{
		pool:      p,
		targetKey: targetKey,
		sessionID: trimmed,
	}
	p.sessions[targetKey] = s
	return s, nil
}

func (p *testRunnerAgentPool) AcquireLease(ctx context.Context, targetKey string) (runner.SessionLease, error) {
	if p == nil {
		return nil, fmt.Errorf("test runner pool is uninitialized")
	}
	trimmedKey := strings.TrimSpace(targetKey)
	if trimmedKey == "" {
		return nil, fmt.Errorf("targetKey cannot be empty")
	}

	tl := p.getTargetLock(trimmedKey)
	tl.Lock()

	sess, err := p.GetOrCreateSession(ctx, trimmedKey, "")
	if err != nil {
		tl.Unlock()
		return nil, err
	}
	s, ok := sess.(*testRunnerAgentSession)
	if !ok {
		tl.Unlock()
		return nil, fmt.Errorf("session is not *testRunnerAgentSession")
	}

	var rec runner.SessionRecord
	if p.cfg.Store != nil {
		if id, err := p.cfg.Store.GetSessionID(ctx, trimmedKey); err == nil {
			rec.ActiveSessionID = id
		}
		if tc, err := p.cfg.Store.GetSessionTurnCount(ctx, trimmedKey); err == nil {
			rec.TurnCount = tc
		}
		if prev, err := p.cfg.Store.GetPreviousSessionID(ctx, trimmedKey); err == nil {
			rec.PreviousSessionID = prev
		}
	} else {
		s.mu.Lock()
		rec.ActiveSessionID = s.sessionID
		rec.TurnCount = s.turnCount
		rec.PreviousSessionID = s.prevSessionID
		s.mu.Unlock()
	}

	isCold := false
	turnCount := 0
	if rec.TurnCount <= 1 || rec.ActiveSessionID == "" {
		isCold = true
	} else {
		turnCount = rec.TurnCount
	}

	sessID := rec.ActiveSessionID
	s.mu.Lock()
	s.sessionID = sessID
	s.mu.Unlock()

	lease := &testSessionLease{
		pool:       p,
		session:    s,
		targetKey:  trimmedKey,
		sessionID:  sessID,
		prevSessID: rec.PreviousSessionID,
		isCold:     isCold,
		turnCount:  turnCount,
		lock:       tl,
	}
	return lease, nil
}

func (p *testRunnerAgentPool) RotateSession(ctx context.Context, targetKey string) (runner.AgentSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	trimmedKey := strings.TrimSpace(targetKey)
	if trimmedKey == "" {
		return nil, fmt.Errorf("targetKey cannot be empty")
	}

	oldSessID := ""
	if existing, ok := p.sessions[trimmedKey]; ok {
		existing.mu.Lock()
		oldSessID = existing.sessionID
		existing.mu.Unlock()
	}

	newSessID := ""
	if p.cfg.Store != nil {
		storedID, err := p.cfg.Store.GetSessionID(ctx, trimmedKey)
		if err == nil && storedID != "" && storedID != oldSessID {
			newSessID = storedID
		}
	}

	s := &testRunnerAgentSession{
		pool:          p,
		targetKey:     trimmedKey,
		sessionID:     newSessID,
		prevSessionID: oldSessID,
		turnCount:     0,
		isCold:        true,
	}
	p.sessions[trimmedKey] = s
	return s, nil
}

func (p *testRunnerAgentPool) ShouldRotateSession(sess runner.AgentSession) (bool, string) {
	if s, ok := sess.(*testRunnerAgentSession); ok && s != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.turnCount > runner.DefaultMaxSessionTurns {
			return true, "turn_count_exceeded"
		}
	}
	return false, ""
}

func (p *testRunnerAgentPool) Initialize(ctx context.Context) error {
	return nil
}

func (p *testRunnerAgentPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions = make(map[string]*testRunnerAgentSession)
	return nil
}

var _ runner.AgentPool = (*testRunnerAgentPool)(nil)
var _ runner.LeasedAgentPool = (*testRunnerAgentPool)(nil)
var _ runner.SessionRotator = (*testRunnerAgentPool)(nil)

type testSessionLease struct {
	pool        *testRunnerAgentPool
	session     *testRunnerAgentSession
	targetKey   string
	sessionID   string
	prevSessID  string
	isCold      bool
	turnCount   int
	lock        *sync.Mutex
	releaseOnce sync.Once
}

var _ runner.SessionLease = (*testSessionLease)(nil)

func (l *testSessionLease) SessionID() string {
	if l.session != nil {
		if sID := l.session.SessionID(); sID != "" {
			return sID
		}
	}
	return l.sessionID
}

func (l *testSessionLease) PreviousSessionID() string {
	return l.prevSessID
}

func (l *testSessionLease) IsCold() bool {
	return l.isCold
}

func (l *testSessionLease) TurnCount() int {
	return l.turnCount
}

func (l *testSessionLease) Execute(ctx context.Context, turn *runner.TurnContext) (*runner.TurnResult, error) {
	if turn == nil {
		return nil, fmt.Errorf("turn context cannot be nil")
	}
	if turn.Ctx == nil {
		turn.Ctx = ctx
	}

	bufSink := runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{})
	origSink := turn.Sink
	if origSink != nil {
		turn.Sink = &testTurnSinkWrapper{inner: origSink, buf: bufSink}
	} else {
		turn.Sink = bufSink
	}
	defer func() {
		turn.Sink = origSink
	}()

	if turn.SessionID == "" {
		l.sessionID = ""
		if l.session != nil {
			l.session.mu.Lock()
			l.session.sessionID = ""
			l.session.mu.Unlock()
		}
	}

	sendErr := l.session.Send(turn.Prompt, turn)
	res, waitErr := bufSink.Wait(ctx)

	if l.session != nil && l.session.SessionID() != "" {
		l.sessionID = l.session.SessionID()
	}
	if res != nil && res.ConversationID != "" {
		l.sessionID = res.ConversationID
	}

	if sendErr != nil {
		return res, sendErr
	}
	if waitErr != nil {
		return res, waitErr
	}

	l.turnCount++
	l.isCold = false
	if l.session != nil {
		l.session.mu.Lock()
		l.session.turnCount = l.turnCount
		l.session.sessionID = l.sessionID
		l.session.isCold = false
		l.session.mu.Unlock()
	}
	return res, nil
}

func (l *testSessionLease) Release() error {
	var releaseErr error
	l.releaseOnce.Do(func() {
		if l.lock != nil {
			l.lock.Unlock()
		}
	})
	return releaseErr
}

type testTurnSinkWrapper struct {
	inner runner.TurnSink
	buf   *runner.BufferingTurnSink
}

func (s *testTurnSinkWrapper) OnTurnStarted() {
	if s.inner != nil {
		s.inner.OnTurnStarted()
	}
	s.buf.OnTurnStarted()
}

func (s *testTurnSinkWrapper) OnThinking() {
	if s.inner != nil {
		s.inner.OnThinking()
	}
	s.buf.OnThinking()
}

func (s *testTurnSinkWrapper) OnStepStarted(stepIndex int) {
	if s.inner != nil {
		if sa, ok := s.inner.(runner.StepAwareSink); ok {
			sa.OnStepStarted(stepIndex)
		}
	}
	s.buf.OnStepStarted(stepIndex)
}

func (s *testTurnSinkWrapper) OnToolCall(toolName, commandName string) {
	if s.inner != nil {
		s.inner.OnToolCall(toolName, commandName)
	}
	s.buf.OnToolCall(toolName, commandName)
}

func (s *testTurnSinkWrapper) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {
	if s.inner != nil {
		s.inner.OnToolCompleted(toolName, mcpServer, duration, status)
	}
	s.buf.OnToolCompleted(toolName, mcpServer, duration, status)
}

func (s *testTurnSinkWrapper) OnSkillActivated(skillName, source string) {
	if s.inner != nil {
		s.inner.OnSkillActivated(skillName, source)
	}
	s.buf.OnSkillActivated(skillName, source)
}

func (s *testTurnSinkWrapper) OnTextDelta(delta string) {
	if s.inner != nil {
		s.inner.OnTextDelta(delta)
	}
	s.buf.OnTextDelta(delta)
}

func (s *testTurnSinkWrapper) OnResult(res *runner.TurnResult) {
	if s.inner != nil {
		s.inner.OnResult(res)
	}
	s.buf.OnResult(res)
}

func (s *testTurnSinkWrapper) OnError(err error) {
	if s.inner != nil {
		s.inner.OnError(err)
	}
	s.buf.OnError(err)
}

type testRunnerAgentSession struct {
	pool          *testRunnerAgentPool
	targetKey     string
	sessionID     string
	prevSessionID string
	turnCount     int
	isCold        bool
	mu            sync.Mutex
}

func (s *testRunnerAgentSession) SessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionID
}

func (s *testRunnerAgentSession) Send(prompt string, turn *runner.TurnContext) error {
	if turn != nil && turn.Sink != nil {
		turn.Sink.OnTurnStarted()
		turn.Sink.OnToolCall("processing", "")
	}
	timeout := s.pool.cfg.TimeoutMinutes
	if timeout <= 0 {
		timeout = DefaultTimeoutMinutes
	}
	watchdogOpts := runner.DefaultWatchdogOptions(timeout)
	if s.pool.cfg.SessionManager != nil {
		watchdogOpts.TranscriptDirs = s.pool.cfg.SessionManager.Roots()
		watchdogOpts.HomeDir = s.pool.cfg.SessionManager.HomeDir()
	}
	if strings.TrimSpace(s.targetKey) != "" {
		watchdogOpts.TargetID = strings.TrimSpace(s.targetKey)
	}
	ctx := context.Background()
	if turn != nil && turn.Ctx != nil {
		ctx = turn.Ctx
	}
	if turn != nil && turn.Sink != nil {
		watchdogOpts.StepUpdateHandler = func(event *runner.StepUpdateEvent) {
			if event != nil && event.Type == "tool_call" {
				cmd := ""
				if event.ToolInfo != nil {
					cmd = event.ToolInfo.Parameters.CommandLine
				}
				turn.Sink.OnToolCall(event.ToolName, cmd)
			}
		}
	}
	model := s.pool.cfg.Model
	if turn != nil && turn.Model != "" {
		model = turn.Model
	} else if s.pool.cfg.LowEffortModel != "" && strings.HasPrefix(s.targetKey, "voice-") {
		model = s.pool.cfg.LowEffortModel
	}
	var sessID string
	if turn != nil {
		sessID = turn.SessionID
		if turn.SessionID == "" {
			s.mu.Lock()
			s.sessionID = ""
			s.mu.Unlock()
		}
	} else {
		sessID = s.SessionID()
	}
	stdout, stderr, exitCode, err := s.pool.cfg.RunnerWithOptionsFunc(
		ctx,
		s.pool.cfg.AgyBin,
		prompt,
		sessID,
		s.pool.cfg.APIKey,
		model,
		watchdogOpts,
	)
	if extSess := runner.ExtractSessionID(stdout+"\n"+stderr, time.Time{}); extSess != "" {
		s.mu.Lock()
		s.sessionID = extSess
		s.mu.Unlock()
	}
	rescueTranscript := func() string {
		if s.pool.cfg.SessionManager == nil {
			return ""
		}
		resSess := s.SessionID()
		if resSess == "" && turn != nil {
			resSess = turn.SessionID
		}
		if resSess == "" {
			return ""
		}
		var since time.Time
		if turn != nil {
			since = turn.CreatedAt
		}
		if rescued := s.pool.cfg.SessionManager.ExtractResponseSince(resSess, since); runner.IsSubstantiveResponse(rescued) && !strings.HasPrefix(rescued, "[Tool Call Requested]:") {
			return strings.TrimSpace(rescued)
		}
		return ""
	}

	if err != nil {
		combinedErr := err
		if stderr != "" && !strings.Contains(err.Error(), stderr) {
			combinedErr = fmt.Errorf("%w: %s", err, stderr)
		}
		if turn != nil && turn.Sink != nil {
			turn.Sink.OnError(combinedErr)
		}
		return combinedErr
	}
	if exitCode != 0 {
		runErr := fmt.Errorf("runner failed with exit code %d: %s", exitCode, stderr)
		if turn != nil && turn.Sink != nil {
			turn.Sink.OnError(runErr)
		}
		return runErr
	}
	respText := stdout
	var parsedUsage runner.AgyUsage
	if parsed, pErr := runner.ParseAgyOutput(stdout); pErr == nil {
		respText = parsed.Response
		if parsed.ConversationID != "" {
			s.mu.Lock()
			s.sessionID = parsed.ConversationID
			s.mu.Unlock()
		}
		parsedUsage = parsed.Usage
		if parsed.Error != "" {
			if rescued := rescueTranscript(); rescued != "" {
				respText = rescued
			} else {
				if strings.Contains(strings.ToLower(parsed.Error), "stream was interrupted") || strings.Contains(strings.ToLower(parsed.Error), "session corrupt") || strings.Contains(strings.ToLower(parsed.Error), "corrupted session") {
					s.mu.Lock()
					s.sessionID = ""
					s.mu.Unlock()
				}
				runErr := fmt.Errorf("%s", parsed.Error)
				if turn != nil && turn.Sink != nil {
					turn.Sink.OnError(runErr)
				}
				return runErr
			}
		} else if parsed.Status != "" && strings.ToUpper(parsed.Status) != "SUCCESS" {
			if rescued := rescueTranscript(); rescued != "" {
				respText = rescued
			} else {
				runErr := fmt.Errorf("runner status: %s", parsed.Status)
				if turn != nil && turn.Sink != nil {
					turn.Sink.OnError(runErr)
				}
				return runErr
			}
		}
	}
	if strings.TrimSpace(respText) == "" {
		if rescued := rescueTranscript(); rescued != "" {
			respText = rescued
		}
	}
	if turn != nil && turn.Sink != nil {
		resSess := s.SessionID()
		if resSess == "" && turn.SessionID != "" {
			resSess = turn.SessionID
		}
		turn.Sink.OnResult(&runner.TurnResult{
			ConversationID: resSess,
			Response:       strings.TrimSpace(respText),
			Stderr:         stderr,
			Usage:          parsedUsage,
		})
	}
	return nil
}

var _ runner.AgentSession = (*testRunnerAgentSession)(nil)

type dummyNonRotatorPool struct {
	runner.AgentPool
}
