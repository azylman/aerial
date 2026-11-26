package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// SteerDrainURL is the default loopback URL where the drain-pending handler is mounted.
const SteerDrainURL = "http://127.0.0.1:4028/internal/turn/drain-pending"

// SteerDrainPath is the route path where the drain-pending handler is mounted.
const SteerDrainPath = "/internal/turn/drain-pending"

// SteerDrainRequest is the payload sent to POST /internal/turn/drain-pending.
type SteerDrainRequest struct {
	DaemonID       string `json:"daemon_id"`
	ConversationID string `json:"conversation_id"`
	SessionID      string `json:"session_id"`
}

// SteerDrainResponse is the response returned by POST /internal/turn/drain-pending.
type SteerDrainResponse struct {
	Prompts []string `json:"prompts"`
}

func writeDrainError(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

func (p *WorkerPool) drainHTTPClient() *http.Client {
	if p != nil && p.cfg.DrainHTTPClient != nil {
		return p.cfg.DrainHTTPClient
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// RegisterSteerHandler mounts POST /internal/turn/drain-pending wrapped in httpkit.LoopbackOnly.
func RegisterSteerHandler(mux *http.ServeMux, pool *WorkerPool) {
	if mux == nil || pool == nil {
		return
	}
	mux.Handle("POST "+SteerDrainPath, httpkit.LoopbackOnly(http.HandlerFunc(pool.HandleDrainPending)))
}

// HandleDrainPending drains pending messages or dispatches for the turn currently running under
// the daemon or conversation identified by req.
func (p *WorkerPool) HandleDrainPending(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDrainError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if p == nil || p.turns == nil {
		writeDrainError(w, http.StatusNotFound, "turn registry not available")
		return
	}
	var req SteerDrainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeDrainError(w, http.StatusBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	reqDaemonID := strings.TrimSpace(req.DaemonID)
	reqConvID := strings.TrimSpace(req.ConversationID)
	reqSessID := strings.TrimSpace(req.SessionID)
	if reqDaemonID == "" && reqConvID == "" && reqSessID == "" {
		writeDrainError(w, http.StatusBadRequest, "at least one of daemon_id, conversation_id, or session_id is required")
		return
	}

	te := p.turns.FindExecution(func(candidate *turnExecution) bool {
		return matchTurnExecution(candidate, reqDaemonID, reqConvID, reqSessID)
	})
	if te == nil {
		writeDrainError(w, http.StatusNotFound, "no active turn found")
		return
	}

	te.steerMu.Lock()
	defer te.steerMu.Unlock()

	if te.steeringClosed || te.finalized.Load() || te.isInterrupted() || r.Context().Err() != nil {
		writeDrainError(w, http.StatusConflict, "turn steering closed or interrupted")
		return
	}

	coordResp, cErr := p.callRestateDrain(r.Context(), te)
	if cErr != nil {
		writeDrainError(w, http.StatusBadGateway, cErr.Error())
		return
	}

	if len(coordResp.DrainedIDs) == 0 {
		httpkit.WriteJSON(w, http.StatusOK, SteerDrainResponse{Prompts: []string{}})
		return
	}

	s := p.Store()
	if s == nil {
		writeDrainError(w, http.StatusInternalServerError, "store not available")
		return
	}

	if te.isDispatch() {
		prompts := p.drainDispatchTurn(r.Context(), te, s, coordResp.DrainedIDs)
		httpkit.WriteJSON(w, http.StatusOK, SteerDrainResponse{Prompts: prompts})
		return
	}

	prompts, bErr := p.drainBurstTurn(r.Context(), te, s, coordResp.DrainedIDs)
	if bErr != nil {
		writeDrainError(w, http.StatusInternalServerError, bErr.Error())
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, SteerDrainResponse{Prompts: prompts})
}

func (p *WorkerPool) callRestateDrain(ctx context.Context, te *turnExecution) (DrainPendingResponse, error) {
	ingressURL := p.resolveRestateIngressURL()
	if ingressURL == "" {
		ingressURL = "http://restate.aerial:8080"
	}
	agentName := p.cfg.AgentName
	var serviceName string
	if te.isDispatch() {
		serviceName = DispatchSessionServiceName(agentName)
	} else {
		serviceName = ThreadSessionServiceName(agentName)
	}
	targetURL := fmt.Sprintf("%s/%s/%s/DrainPending", ingressURL, serviceName, url.PathEscape(te.threadID))

	bodyBytes, err := json.Marshal(DrainPendingRequest{TurnID: te.turnID})
	if err != nil {
		return DrainPendingResponse{}, err
	}

	drainReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return DrainPendingResponse{}, err
	}
	drainReq.Header.Set("Content-Type", "application/json")
	drainReq.Header.Set("Accept", "application/json")

	drainResp, err := p.drainHTTPClient().Do(drainReq)
	if err != nil {
		return DrainPendingResponse{}, fmt.Errorf("restate drain call failed: %w", err)
	}
	defer drainResp.Body.Close()

	if drainResp.StatusCode < 200 || drainResp.StatusCode >= 300 {
		respBytes, readErr := io.ReadAll(io.LimitReader(drainResp.Body, 512))
		if readErr != nil {
			respBytes = []byte(fmt.Sprintf("<error reading response: %v>", readErr))
		}
		return DrainPendingResponse{}, fmt.Errorf("restate returned status %d: %s", drainResp.StatusCode, string(respBytes))
	}

	var coordResp DrainPendingResponse
	if dErr := json.NewDecoder(drainResp.Body).Decode(&coordResp); dErr != nil {
		return DrainPendingResponse{}, fmt.Errorf("decode restate response: %w", dErr)
	}
	return coordResp, nil
}

func (*WorkerPool) drainDispatchTurn(ctx context.Context, te *turnExecution, s db.Store, drainedIDs []string) []string {
	var prompts []string
	for _, id := range drainedIDs {
		claimed, cErr := s.ClaimPendingDispatch(ctx, id)
		if cErr != nil {
			log.Printf("[SteerHandler] Error claiming pending dispatch %s: %v", id, cErr)
			continue
		}
		if !claimed {
			continue
		}
		d, gErr := s.GetDispatch(ctx, id)
		if gErr != nil || d == nil {
			log.Printf("[SteerHandler] Error getting dispatch %s: %v", id, gErr)
			continue
		}
		te.steeredDispatches = append(te.steeredDispatches, *d)
		te.drainedIDs = append(te.drainedIDs, d.ID)
		if pText := strings.TrimSpace(d.Prompt); pText != "" {
			prompts = append(prompts, pText)
		}
	}
	if prompts == nil {
		prompts = []string{}
	}
	return prompts
}

func (p *WorkerPool) drainBurstTurn(ctx context.Context, te *turnExecution, s db.Store, drainedIDs []string) ([]string, error) {
	var claimedIDs []string
	for _, id := range drainedIDs {
		claimed, cErr := s.ClaimPendingMessage(ctx, id)
		if cErr != nil {
			log.Printf("[SteerHandler] Error claiming pending message %s: %v", id, cErr)
			continue
		}
		if claimed {
			claimedIDs = append(claimedIDs, id)
		}
	}
	if len(claimedIDs) == 0 {
		return []string{}, nil
	}
	rows, err := messagesByIDOrdered(ctx, s, claimedIDs)
	if err != nil {
		return nil, err
	}
	te.burst = append(te.burst, rows...)
	for _, row := range rows {
		te.drainedIDs = append(te.drainedIDs, row.ID)
	}
	var tz string
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil {
			tz = cur.Timezone
		}
	}
	coalesced := CoalesceBurstPrompt(rows, tz)
	var prompts []string
	if strings.TrimSpace(coalesced) != "" {
		prompts = append(prompts, coalesced)
	} else {
		prompts = []string{}
	}
	return prompts, nil
}

func matchTurnExecution(te *turnExecution, daemonID, conversationID, sessionID string) bool {
	if te == nil {
		return false
	}
	var owner runner.ActiveTurnOwner
	if te.lease != nil {
		if o, ok := te.lease.(runner.ActiveTurnOwner); ok {
			owner = o
		}
	}

	// Primary match: DaemonID
	if daemonID != "" {
		if te.activeTurnDaemon == daemonID {
			return true
		}
		if owner != nil && owner.ActiveTurn().DaemonID == daemonID {
			return true
		}
	}

	// Fallback match: ConversationID or SessionID
	te.activeSessionMu.Lock()
	currSess := te.currentSessionID
	leaseSess := te.leaseSessionID
	te.activeSessionMu.Unlock()

	var ownerConv string
	if owner != nil {
		ownerConv = owner.ActiveTurn().ConversationID
	}

	matchesTarget := func(target string) bool {
		if target == "" {
			return false
		}
		return target == currSess || target == leaseSess || (ownerConv != "" && target == ownerConv)
	}

	if conversationID != "" && matchesTarget(conversationID) {
		return true
	}
	if sessionID != "" && matchesTarget(sessionID) {
		return true
	}
	return false
}
