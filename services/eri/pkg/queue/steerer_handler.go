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
	"slices"
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
	if p.cfg.DrainHTTPClient != nil {
		return p.cfg.DrainHTTPClient
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// RegisterSteerHandler mounts POST /internal/turn/drain-pending wrapped in httpkit.LoopbackOnly.
func RegisterSteerHandler(mux *http.ServeMux, pool *WorkerPool) {
	mux.Handle("POST "+SteerDrainPath, httpkit.LoopbackOnly(http.HandlerFunc(pool.HandleDrainPending)))
}

// HandleDrainPending drains pending messages or dispatches for the turn currently running under
// the daemon or conversation identified by req.
func (p *WorkerPool) HandleDrainPending(w http.ResponseWriter, r *http.Request) {
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

	snap := te.matchSnapshot()
	coordResp, cErr := p.callRestateDrain(r.Context(), snap, nil)
	if cErr != nil {
		writeDrainError(w, http.StatusBadGateway, cErr.Error())
		return
	}

	candidates := removeIDs(normalizeStepIDs(append(slices.Clone(te.pendingDrainIDs), coordResp.DrainedIDs...)), te.drainedIDs)
	te.pendingDrainIDs = candidates
	if len(candidates) == 0 {
		if len(coordResp.DrainedIDs) > 0 {
			p.ackRestateDrain(r.Context(), snap, coordResp.DrainedIDs)
		}
		httpkit.WriteJSON(w, http.StatusOK, SteerDrainResponse{Prompts: []string{}})
		return
	}

	s := p.Store()
	if s == nil {
		writeDrainError(w, http.StatusInternalServerError, "store not available")
		return
	}

	var (
		prompts []string
		ackIDs  []string
		dErr    error
	)
	if snap.isDispatch {
		prompts, ackIDs, dErr = p.drainDispatchTurn(r.Context(), te, s, snap.turnID, candidates)
	} else {
		prompts, ackIDs, dErr = p.drainBurstTurn(r.Context(), te, s, snap.turnID, candidates)
	}
	if len(ackIDs) > 0 {
		te.pendingDrainIDs = removeIDs(te.pendingDrainIDs, ackIDs)
		p.ackRestateDrain(r.Context(), snap, ackIDs)
	}
	if dErr != nil {
		writeDrainError(w, http.StatusInternalServerError, dErr.Error())
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, SteerDrainResponse{Prompts: prompts})
}

func (p *WorkerPool) ackRestateDrain(ctx context.Context, snap turnMatchSnapshot, ackIDs []string) {
	if _, err := p.callRestateDrain(ctx, snap, ackIDs); err != nil {
		log.Printf("[SteerHandler] Warning acknowledging drained IDs: %v", err)
	}
}

func (p *WorkerPool) callRestateDrain(ctx context.Context, snap turnMatchSnapshot, ackIDs []string) (DrainPendingResponse, error) {
	ingressURL := p.resolveRestateIngressURL()
	if ingressURL == "" {
		ingressURL = "http://restate.aerial:8080"
	}
	agentName := p.cfg.AgentName
	var serviceName string
	if snap.isDispatch {
		serviceName = DispatchSessionServiceName(agentName)
	} else {
		serviceName = ThreadSessionServiceName(agentName)
	}
	targetURL := fmt.Sprintf("%s/%s/%s/DrainPending", ingressURL, serviceName, url.PathEscape(snap.threadID))

	bodyBytes, err := json.Marshal(DrainPendingRequest{TurnID: snap.turnID, AckIDs: normalizeStepIDs(ackIDs)})
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

func (*WorkerPool) drainDispatchTurn(ctx context.Context, te *turnExecution, s db.Store, turnID string, drainedIDs []string) ([]string, []string, error) {
	var prompts, ackIDs []string
	for _, id := range drainedIDs {
		if _, cErr := s.ClaimPendingDispatch(ctx, id); cErr != nil {
			log.Printf("[SteerHandler] Error claiming pending dispatch %s: %v", id, cErr)
			return nil, ackIDs, fmt.Errorf("claim pending dispatch %s: %w", id, cErr)
		}
		if rErr := recordTurnOwnedIDs(ctx, s, turnID, []string{id}); rErr != nil {
			return nil, ackIDs, rErr
		}
		d, gErr := s.GetDispatch(ctx, id)
		if gErr != nil {
			log.Printf("[SteerHandler] Error getting dispatch %s: %v", id, gErr)
			return nil, ackIDs, fmt.Errorf("get dispatch %s: %w", id, gErr)
		}
		if d == nil || d.Status != db.StatusProcessing {
			ackIDs = normalizeStepIDs(append(ackIDs, id))
			continue
		}
		te.steeredDispatches = append(te.steeredDispatches, *d)
		te.drainedIDs = normalizeStepIDs(append(te.drainedIDs, d.ID))
		ackIDs = normalizeStepIDs(append(ackIDs, d.ID))
		if pText := strings.TrimSpace(d.Prompt); pText != "" {
			prompts = append(prompts, pText)
			te.drainedPrompts = append(te.drainedPrompts, pText)
		}
	}
	if prompts == nil {
		prompts = []string{}
	}
	return prompts, ackIDs, nil
}

func (p *WorkerPool) drainBurstTurn(ctx context.Context, te *turnExecution, s db.Store, turnID string, drainedIDs []string) ([]string, []string, error) {
	for _, id := range drainedIDs {
		if _, cErr := s.ClaimPendingMessage(ctx, id); cErr != nil {
			log.Printf("[SteerHandler] Error claiming pending message %s: %v", id, cErr)
			return nil, nil, fmt.Errorf("claim pending message %s: %w", id, cErr)
		}
	}
	if rErr := recordTurnOwnedIDs(ctx, s, turnID, drainedIDs); rErr != nil {
		return nil, nil, rErr
	}
	rows, err := messagesByIDOrdered(ctx, s, drainedIDs)
	if err != nil {
		return nil, nil, err
	}
	var activeRows []db.Message
	var activeIDs []string
	for _, row := range rows {
		if row.Status == db.StatusProcessing {
			activeRows = append(activeRows, row)
			activeIDs = append(activeIDs, row.ID)
		}
	}
	ackIDs := normalizeStepIDs(drainedIDs)
	if len(activeRows) == 0 {
		return []string{}, ackIDs, nil
	}
	te.steeredMessages = append(te.steeredMessages, activeRows...)
	te.drainedIDs = normalizeStepIDs(append(te.drainedIDs, activeIDs...))
	coalesced := CoalesceBurstPrompt(activeRows, p.currentTimezone())
	var prompts []string
	if strings.TrimSpace(coalesced) != "" {
		prompts = append(prompts, coalesced)
		te.drainedPrompts = append(te.drainedPrompts, coalesced)
	} else {
		prompts = []string{}
	}
	return prompts, ackIDs, nil
}

func matchTurnExecution(te *turnExecution, daemonID, conversationID, sessionID string) bool {
	snap := te.matchSnapshot()
	var owner runner.ActiveTurnOwner
	if snap.lease != nil {
		if o, ok := snap.lease.(runner.ActiveTurnOwner); ok {
			owner = o
		}
	}

	// Primary match: DaemonID
	if daemonID != "" {
		if snap.activeTurnDaemon == daemonID {
			return true
		}
		if owner != nil && owner.ActiveTurn().DaemonID == daemonID {
			return true
		}
	}

	// Fallback match: ConversationID or SessionID
	var ownerConv string
	if owner != nil {
		ownerConv = owner.ActiveTurn().ConversationID
	}

	matchesTarget := func(target string) bool {
		return target == snap.currentSessionID || target == snap.leaseSessionID || (ownerConv != "" && target == ownerConv)
	}

	if conversationID != "" && matchesTarget(conversationID) {
		return true
	}
	if sessionID != "" && matchesTarget(sessionID) {
		return true
	}
	return false
}
