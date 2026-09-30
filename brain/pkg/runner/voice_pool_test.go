package runner

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// mockVoiceSession implements VoiceSession for testing.
type mockVoiceSession struct {
	sessionID string
	sendFn    func(prompt string, turn *TurnContext) error
}

func (m *mockVoiceSession) SessionID() string {
	return m.sessionID
}

func (m *mockVoiceSession) Send(prompt string, turn *TurnContext) error {
	if m.sendFn != nil {
		return m.sendFn(prompt, turn)
	}
	return nil
}

var _ VoiceSession = (*mockVoiceSession)(nil)

// mockVoiceProcessPool implements VoiceProcessPool for testing.
type mockVoiceProcessPool struct {
	sessions   map[string]*mockVoiceSession
	initCalled bool
	closed     bool
	getErr     error
	initErr    error
	closeErr   error
}

func newMockVoiceProcessPool() *mockVoiceProcessPool {
	return &mockVoiceProcessPool{
		sessions: make(map[string]*mockVoiceSession),
	}
}

func (m *mockVoiceProcessPool) GetOrCreateSession(ctx context.Context, targetKey string) (VoiceSession, error) {
	if m.closed {
		return nil, errors.New("mock pool is closed")
	}
	if m.getErr != nil {
		return nil, m.getErr
	}
	if s, ok := m.sessions[targetKey]; ok {
		return s, nil
	}
	sess := &mockVoiceSession{sessionID: "sess-" + targetKey}
	m.sessions[targetKey] = sess
	return sess, nil
}

func (m *mockVoiceProcessPool) Initialize(ctx context.Context) error {
	m.initCalled = true
	return m.initErr
}

func (m *mockVoiceProcessPool) Close() error {
	m.closed = true
	return m.closeErr
}

var _ VoiceProcessPool = (*mockVoiceProcessPool)(nil)

func TestVoiceInterfaces_Satisfaction(t *testing.T) {
	t.Parallel()

	var _ AgentPool = (*UnifiedProcessPool)(nil)
	var _ AgentSession = (*StreamingDaemon)(nil)
	var _ AgentPool = (*mockVoiceProcessPool)(nil)
	var _ AgentSession = (*mockVoiceSession)(nil)
	var _ VoiceProcessPool = (*UnifiedProcessPool)(nil)
	var _ VoiceSession = (*StreamingDaemon)(nil)
	var _ VoiceProcessPool = (*mockVoiceProcessPool)(nil)
	var _ VoiceSession = (*mockVoiceSession)(nil)
}

func TestUnifiedProcessPool_GetOrCreateSession(t *testing.T) {
	t.Parallel()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000099"}` + "\n"))
				_, _ = io.ReadAll(inR)
				_ = outW.Close()
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 101}, nil
		},
	}

	cfg := PoolConfig{
		DefaultModel: "gemini-2.5-pro",
		MaxIdle:      24 * time.Hour,
	}

	pool := NewUnifiedProcessPool(cfg, mock)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sess, err := pool.GetOrCreateSession(ctx, "kiosk-voice")
	if err != nil {
		t.Fatalf("unexpected error from GetOrCreateSession: %v", err)
	}
	if sess == nil {
		t.Fatalf("expected non-nil VoiceSession")
	}

	if sess.SessionID() != "00000000-0000-0000-0000-000000000099" {
		t.Errorf("expected session ID 00000000-0000-0000-0000-000000000099, got %q", sess.SessionID())
	}

	turnCtx := &TurnContext{
		TurnID:    "turn-1",
		Prompt:    "hello",
		CreatedAt: time.Now(),
	}
	if err := sess.Send("hello", turnCtx); err != nil {
		t.Fatalf("failed sending prompt via VoiceSession: %v", err)
	}

	// Verify GetOrCreateSession on closed pool
	if err := pool.Close(); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}

	_, closedErr := pool.GetOrCreateSession(ctx, "kiosk-voice")
	if closedErr == nil {
		t.Fatalf("expected error from GetOrCreateSession on closed pool, got nil")
	}
}

func TestMockVoiceProcessPool_Behavior(t *testing.T) {
	t.Parallel()

	pool := newMockVoiceProcessPool()
	ctx := context.Background()

	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	if !pool.initCalled {
		t.Fatalf("expected initCalled to be true")
	}

	sess1, err := pool.GetOrCreateSession(ctx, "device-a")
	if err != nil {
		t.Fatalf("unexpected error getting session: %v", err)
	}
	if sess1.SessionID() != "sess-device-a" {
		t.Errorf("expected session ID sess-device-a, got %q", sess1.SessionID())
	}

	// Calling again should return the same session
	sess2, err := pool.GetOrCreateSession(ctx, "device-a")
	if err != nil {
		t.Fatalf("unexpected error getting existing session: %v", err)
	}
	if sess1 != sess2 {
		t.Errorf("expected same session instance returned")
	}

	// Test Send
	var receivedPrompt string
	mockSess := sess1.(*mockVoiceSession)
	mockSess.sendFn = func(prompt string, turn *TurnContext) error {
		receivedPrompt = prompt
		return nil
	}
	if err := sess1.Send("test prompt", nil); err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}
	if receivedPrompt != "test prompt" {
		t.Errorf("expected test prompt, got %q", receivedPrompt)
	}

	// Test Close and closed behavior
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if _, err := pool.GetOrCreateSession(ctx, "device-b"); err == nil {
		t.Fatalf("expected error from GetOrCreateSession on closed mock pool")
	}
}

func TestNewGeminiAPIPool(t *testing.T) {
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "fake-key",
	})
	if pool == nil {
		t.Fatal("expected non-nil GeminiAPIPool")
	}
	var _ AgentPool = pool
}
