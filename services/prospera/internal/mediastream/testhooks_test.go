package mediastream

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
)

// Test-only seams; production code never calls these.

// WithPacerInterval overrides the outbound audio pacing interval.
func WithPacerInterval(d time.Duration) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.pacerInterval = d
	}
}

// WithGeminiDialer overrides the Gemini Live dialer.
func WithGeminiDialer(dialer func(ctx context.Context, modelName string) (*websocket.Conn, error)) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.geminiDialer = dialer
	}
}

// WaitForSession blocks until the session for callID registers or ctx ends.
func (m *CallSessionManager) WaitForSession(ctx context.Context, callID string) (Session, error) {
	m.mu.Lock()
	if sess, ok := m.sessions[callID]; ok {
		m.mu.Unlock()
		return sess, nil
	}
	ch := make(chan Session, 1)
	m.sessionWaiters[callID] = append(m.sessionWaiters[callID], ch)
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		m.mu.Lock()
		waiters := m.sessionWaiters[callID]
		for i, w := range waiters {
			if w == ch {
				m.sessionWaiters[callID] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(m.sessionWaiters[callID]) == 0 {
			delete(m.sessionWaiters, callID)
		}
		m.mu.Unlock()
		return nil, ctx.Err()
	case sess := <-ch:
		return sess, nil
	}
}
