package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

type mockTrackerPool struct {
	daemon *runner.StreamingDaemon
}

func (m *mockTrackerPool) Get(threadID string) (*runner.StreamingDaemon, bool) {
	if m.daemon != nil {
		return m.daemon, true
	}
	return nil, false
}

func (m *mockTrackerPool) GetOrCreate(ctx context.Context, threadID string, sessionID string) (*runner.StreamingDaemon, error) {
	if m.daemon != nil {
		return m.daemon, nil
	}
	return nil, nil
}

func (m *mockTrackerPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (runner.AgentSession, error) {
	return nil, nil
}

func (m *mockTrackerPool) Initialize(ctx context.Context) error { return nil }
func (m *mockTrackerPool) Close() error                         { return nil }

type mockStepAwareSink struct {
	stepIndex int
	thinking  bool
	skills    []string
	deltas    []string
	completed bool
	errored   bool
}

func (m *mockStepAwareSink) OnTurnStarted()                                                      {}
func (m *mockStepAwareSink) OnThinking()                                                         { m.thinking = true }
func (m *mockStepAwareSink) OnStepStarted(idx int)                                               { m.stepIndex = idx }
func (m *mockStepAwareSink) OnToolCall(toolName, commandName string)                             {}
func (m *mockStepAwareSink) OnToolCompleted(toolName, mcp string, d time.Duration, stat string) { m.completed = true }
func (m *mockStepAwareSink) OnSkillActivated(skillName, src string)                              { m.skills = append(m.skills, skillName) }
func (m *mockStepAwareSink) OnTextDelta(delta string)                                            { m.deltas = append(m.deltas, delta) }
func (m *mockStepAwareSink) OnResult(res *runner.TurnResult)                                     {}
func (m *mockStepAwareSink) OnError(err error)                                                   { m.errored = true }

func TestLegacyRunnerAgentPool_Coverage(t *testing.T) {
	cfg := WorkerPoolConfig{
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			return `{"status":"SUCCESS","response":"ok"}`, "", 0, nil
		},
	}

	// 1. Without tracker pool
	pool := newLegacyRunnerAgentPool(cfg, nil)
	if d, ok := pool.Get("t1"); ok || d != nil {
		t.Errorf("expected nil daemon from Get")
	}
	if _, err := pool.GetOrCreate(context.Background(), "t1", ""); err == nil {
		t.Errorf("expected error from GetOrCreate without tracker pool")
	}
	if err := pool.Initialize(context.Background()); err != nil {
		t.Errorf("unexpected error from Initialize: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Errorf("unexpected error from Close: %v", err)
	}

	sess, err := pool.GetOrCreateSession(context.Background(), "target1", "")
	if err != nil || sess == nil {
		t.Fatalf("failed to GetOrCreateSession: %v", err)
	}
	// Calling GetOrCreateSession again returns same session
	sess2, err := pool.GetOrCreateSession(context.Background(), "target1", "")
	if err != nil || sess2 != sess {
		t.Fatalf("expected cached session, got: %v", sess2)
	}

	// Call Send with nil turn
	if err := sess.Send("hello", nil); err != nil {
		t.Errorf("unexpected error from Send: %v", err)
	}
	if sess.SessionID() == "" && sess.(*legacyRunnerAgentSession).sessionID != "" {
		t.Errorf("mismatch sessionID")
	}

	// 2. With tracker pool
	mockTP := &mockTrackerPool{}
	poolWithTracker := newLegacyRunnerAgentPool(cfg, mockTP)
	if d, ok := poolWithTracker.Get("t1"); ok || d != nil {
		t.Errorf("expected nil daemon")
	}
	if d, err := poolWithTracker.GetOrCreate(context.Background(), "t1", ""); err != nil || d != nil {
		t.Errorf("expected nil daemon, nil error")
	}

	// 3. WorkerPool.AgentPool()
	wp := &WorkerPool{processPool: pool}
	if wp.AgentPool() != pool {
		t.Errorf("expected agentPool to match")
	}

	// 4. VoiceTurnSink no-op callbacks
	vs := NewVoiceTurnSink(nil, "dev-1")
	if vs.DeviceID() != "dev-1" {
		t.Errorf("expected device ID dev-1, got %s", vs.DeviceID())
	}
	vs.OnTurnStarted()
	vs.OnThinking()
	vs.OnToolCall("tool", "cmd")
	vs.OnToolCompleted("tool", "mcp", time.Second, "ok")
	vs.OnSkillActivated("skill", "src")
	vs.OnTextDelta("delta")
	vs.OnResult(&runner.TurnResult{Response: "ok"})
	vs.OnError(errors.New("err"))

	ds := NewDiscordTurnSink(nil, "ch-1", "msg-1", nil, nil)
	ds.OnTurnStarted()
	ds.OnThinking()
	ds.OnToolCompleted("tool", "mcp", time.Second, "ok")
	ds.OnSkillActivated("skill", "src")

	// 5. RotateSession and ShouldRotateSession
	rotatedSess, err := pool.RotateSession(context.Background(), "target1")
	if err != nil || rotatedSess == nil {
		t.Fatalf("failed to RotateSession on existing session: %v", err)
	}
	if !rotatedSess.(*legacyRunnerAgentSession).isCold {
		t.Errorf("expected isCold to be true after RotateSession")
	}

	// Rotate on non-existing session
	newRotated, err := pool.RotateSession(context.Background(), "target-fresh")
	if err != nil || newRotated == nil {
		t.Fatalf("failed to RotateSession on new session: %v", err)
	}

	// ShouldRotateSession
	should, _ := pool.ShouldRotateSession(newRotated)
	if should {
		t.Errorf("new session should not rotate")
	}
	newRotated.(*legacyRunnerAgentSession).turnCount = runner.DefaultMaxSessionTurns + 1
	should, reason := pool.ShouldRotateSession(newRotated)
	if !should || reason == "" {
		t.Errorf("expected should rotate true, got %v (%s)", should, reason)
	}
	if should, _ := pool.ShouldRotateSession(nil); should {
		t.Errorf("nil session should not rotate")
	}

	// 6. leaseTurnSinkWrapper with both nil inner and mock step-aware inner
	buf1 := runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{})
	wrapperNil := &leaseTurnSinkWrapper{inner: nil, buf: buf1}
	wrapperNil.OnTurnStarted()
	wrapperNil.OnThinking()
	wrapperNil.OnStepStarted(0)
	wrapperNil.OnToolCall("tool", "cmd")
	wrapperNil.OnToolCompleted("tool", "mcp", time.Second, "ok")
	wrapperNil.OnSkillActivated("sk", "src")
	wrapperNil.OnTextDelta("txt")
	wrapperNil.OnResult(&runner.TurnResult{Response: "ok"})
	wrapperNil.OnError(errors.New("err"))

	buf2 := runner.NewBufferingTurnSink(runner.BufferingTurnSinkConfig{})
	stepSink := &mockStepAwareSink{}
	wrapperStep := &leaseTurnSinkWrapper{inner: stepSink, buf: buf2}
	wrapperStep.OnTurnStarted()
	wrapperStep.OnThinking()
	wrapperStep.OnStepStarted(42)
	wrapperStep.OnToolCall("tool", "cmd")
	wrapperStep.OnToolCompleted("tool", "mcp", time.Second, "ok")
	wrapperStep.OnSkillActivated("sk", "src")
	wrapperStep.OnTextDelta("txt")
	wrapperStep.OnResult(&runner.TurnResult{Response: "ok"})
	wrapperStep.OnError(errors.New("err"))

	if !stepSink.thinking || stepSink.stepIndex != 42 || !stepSink.completed || len(stepSink.skills) != 1 || len(stepSink.deltas) != 1 || !stepSink.errored {
		t.Errorf("step aware sink did not receive expected callbacks: %+v", stepSink)
	}

	// 7. AcquireLease and SessionLease methods
	lease, err := pool.AcquireLease(context.Background(), "target1")
	if err != nil || lease == nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if lease.SessionID() == "" {
		t.Errorf("lease sessionID empty")
	}
	_ = lease.IsCold()
	_ = lease.PreviousSessionID()
	_ = lease.TurnCount()
	lease.Release()
}
