package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAgentPool_InterfacesAndMockBehavior(t *testing.T) {
	t.Parallel()

	var _ AgentPool = (*UnifiedProcessPool)(nil)
	var _ AgentSession = (*StreamingDaemon)(nil)
	var _ AgentPool = (*mockVoicePool)(nil)
	var _ AgentSession = (*mockVoiceSession)(nil)
	var _ AgentPool = NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "fake-key"})

	pool := newMockVoicePool("test-model")
	ctx := context.Background()

	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	if pool.initCalled != 1 {
		t.Fatalf("expected initCalled == 1, got %d", pool.initCalled)
	}

	sess1, err := pool.GetOrCreateSession(ctx, "device-a", "")
	if err != nil || sess1.SessionID() != "device-a" {
		t.Fatalf("unexpected session 1: (%v, %v)", sess1, err)
	}
	sess2, err := pool.GetOrCreateSession(ctx, "device-a", "")
	if err != nil || sess1 != sess2 {
		t.Fatalf("expected cached session instance, got (%v, %v)", sess2, err)
	}

	var receivedPrompt string
	sess1.(*mockVoiceSession).sendFn = func(prompt string, turn *TurnContext) error {
		receivedPrompt = prompt
		return nil
	}
	if err := sess1.Send("test prompt", nil); err != nil || receivedPrompt != "test prompt" {
		t.Fatalf("unexpected Send: %v, got %q", err, receivedPrompt)
	}

	pool.getErr = errors.New("mock pool is closed")
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if _, err := pool.GetOrCreateSession(ctx, "device-b", ""); err == nil {
		t.Fatal("expected error on closed mock pool")
	}
}

func TestUnifiedProcessPool_GetOrCreateSession(t *testing.T) {
	t.Parallel()

	pool := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "gemini-2.5-pro",
		MaxIdle:      24 * time.Hour,
	}, createTestSpawner())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sess, err := pool.GetOrCreateSession(ctx, "kiosk-voice", "")
	if err != nil || sess == nil || sess.SessionID() == "" {
		t.Fatalf("unexpected session: (%v, %v)", sess, err)
	}

	turnCtx := &TurnContext{TurnID: "turn-1", Prompt: "hello", CreatedAt: time.Now()}
	if err := sess.Send("hello", turnCtx); err != nil {
		t.Fatalf("failed sending prompt: %v", err)
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
	if _, err := pool.GetOrCreateSession(ctx, "kiosk-voice", ""); err == nil {
		t.Fatal("expected error from GetOrCreateSession on closed pool")
	}
}
