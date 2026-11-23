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
