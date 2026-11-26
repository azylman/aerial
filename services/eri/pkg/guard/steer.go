package guard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// SteerDrainURL is the loopback URL on eri (:4028) used to drain pending steering messages.
const (
	SteerDrainURL       = "http://127.0.0.1:4028/internal/turn/drain-pending"
	defaultSteerTimeout = 300 * time.Millisecond
)

// TurnSteerer drains pending mid-turn user steering prompts for an active session turn.
type TurnSteerer interface {
	DrainPending(ctx context.Context, payload Payload) []string
}

// HTTPTurnSteerer drains pending turn prompts by querying eri's loopback HTTP endpoint.
type HTTPTurnSteerer struct {
	Endpoint string
	DaemonID string
	Client   *http.Client
}

type drainPendingRequest struct {
	DaemonID       string `json:"daemon_id"`
	ConversationID string `json:"conversation_id"`
	SessionID      string `json:"session_id"`
}

type drainPendingResponse struct {
	Prompts []string `json:"prompts"`
}

// DrainPending posts to the steer drain endpoint and returns any pending prompts.
// It fails open (returns nil) on transport errors, timeouts, non-200 HTTP responses, or empty prompts.
func (s *HTTPTurnSteerer) DrainPending(ctx context.Context, payload Payload) []string {
	if s == nil || strings.TrimSpace(s.Endpoint) == "" || (strings.TrimSpace(s.DaemonID) == "" && strings.TrimSpace(payload.ConversationID) == "" && strings.TrimSpace(payload.SessionID) == "") {
		return nil
	}

	reqData, err := json.Marshal(drainPendingRequest{
		DaemonID:       s.DaemonID,
		ConversationID: payload.ConversationID,
		SessionID:      payload.SessionID,
	})
	if err != nil {
		return nil
	}

	drainCtx, cancel := context.WithTimeout(ctx, defaultSteerTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(drainCtx, http.MethodPost, s.Endpoint, bytes.NewReader(reqData))
	if err != nil {
		return nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var drainResp drainPendingResponse
	if err := json.NewDecoder(resp.Body).Decode(&drainResp); err != nil {
		return nil
	}

	var prompts []string
	for _, p := range drainResp.Prompts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			prompts = append(prompts, trimmed)
		}
	}
	return prompts
}
