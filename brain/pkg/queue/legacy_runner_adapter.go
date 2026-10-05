package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

type legacyRunnerAgentPool struct {
	cfg         WorkerPoolConfig
	trackerPool runner.AgentPool
	mu          sync.Mutex
	sessions    map[string]*legacyRunnerAgentSession
}

func newLegacyRunnerAgentPool(cfg WorkerPoolConfig, trackerPool runner.AgentPool) *legacyRunnerAgentPool {
	return &legacyRunnerAgentPool{
		cfg:         cfg,
		trackerPool: trackerPool,
		sessions:    make(map[string]*legacyRunnerAgentSession),
	}
}

func (p *legacyRunnerAgentPool) Get(threadID string) (*runner.StreamingDaemon, bool) {
	if p.trackerPool != nil {
		if tp, ok := p.trackerPool.(interface {
			Get(string) (*runner.StreamingDaemon, bool)
		}); ok {
			return tp.Get(threadID)
		}
	}
	return nil, false
}

func (p *legacyRunnerAgentPool) GetOrCreate(ctx context.Context, threadID string, sessionID string) (*runner.StreamingDaemon, error) {
	if p.trackerPool != nil {
		if tp, ok := p.trackerPool.(interface {
			GetOrCreate(context.Context, string, string) (*runner.StreamingDaemon, error)
		}); ok {
			return tp.GetOrCreate(ctx, threadID, sessionID)
		}
	}
	return nil, fmt.Errorf("no underlying process pool")
}

func (p *legacyRunnerAgentPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (runner.AgentSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	trimmed := strings.TrimSpace(sessionID)
	if s, ok := p.sessions[targetKey]; ok {
		s.mu.Lock()
		s.sessionID = trimmed
		s.mu.Unlock()
		return s, nil
	}
	s := &legacyRunnerAgentSession{
		pool:      p,
		targetKey: targetKey,
		sessionID: trimmed,
	}
	p.sessions[targetKey] = s
	return s, nil
}

func (p *legacyRunnerAgentPool) EvictSession(targetKey string) error {
	p.mu.Lock()
	delete(p.sessions, targetKey)
	p.mu.Unlock()
	if p.trackerPool != nil {
		return p.trackerPool.EvictSession(targetKey)
	}
	return nil
}

func (p *legacyRunnerAgentPool) Initialize(ctx context.Context) error {
	return nil
}

func (p *legacyRunnerAgentPool) Close() error {
	return nil
}

var _ runner.AgentPool = (*legacyRunnerAgentPool)(nil)

type legacyRunnerAgentSession struct {
	pool      *legacyRunnerAgentPool
	targetKey string
	sessionID string
	mu        sync.Mutex
}

func (s *legacyRunnerAgentSession) SessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionID
}

func (s *legacyRunnerAgentSession) Send(prompt string, turn *runner.TurnContext) error {
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

var _ runner.AgentSession = (*legacyRunnerAgentSession)(nil)
