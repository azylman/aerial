package queue

import (
	"context"
	"testing"

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

func (m *mockTrackerPool) EvictSession(targetKey string) error { return nil }
func (m *mockTrackerPool) Initialize(ctx context.Context) error { return nil }
func (m *mockTrackerPool) Close() error                         { return nil }

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
}
