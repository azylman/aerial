package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

const (
	statusCompleted = "completed"
)

// ThreadSessionServiceName returns the Restate service name for ThreadSession.
func ThreadSessionServiceName(name string) string {
	return agent.ScopedServiceName("ThreadSession", name)
}

// DispatchSessionServiceName returns the Restate service name for DispatchSession.
func DispatchSessionServiceName(name string) string {
	return agent.ScopedServiceName("DispatchSession", name)
}

// VoiceSessionServiceName returns the Restate service name for VoiceSession.
func VoiceSessionServiceName(name string) string {
	return agent.ScopedServiceName("VoiceSession", name)
}

// SessionManagerServiceName returns the Restate service name for SessionManager.
func SessionManagerServiceName(name string) string {
	return agent.ScopedServiceName("SessionManager", name)
}

// CancelDispatchControlRequest defines the request to cancel dispatches from the unkeyed session manager.
type CancelDispatchControlRequest struct {
	SessionKey string `json:"session_key,omitempty"`
	DispatchID string `json:"dispatch_id,omitempty"`
	ChunkID    string `json:"chunk_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

var (
	activeInvocationsMu sync.RWMutex
	activeInvocations   = make(map[string]string)
)

func registerActiveInvocation(key, invID string) {
	if key == "" || invID == "" {
		return
	}
	activeInvocationsMu.Lock()
	defer activeInvocationsMu.Unlock()
	activeInvocations[key] = invID
}

func unregisterActiveInvocation(key, invID string) {
	if key == "" {
		return
	}
	activeInvocationsMu.Lock()
	defer activeInvocationsMu.Unlock()
	if invID == "" || activeInvocations[key] == invID {
		delete(activeInvocations, key)
	}
}

func getActiveInvocation(key string) string {
	if key == "" {
		return ""
	}
	activeInvocationsMu.RLock()
	defer activeInvocationsMu.RUnlock()
	return activeInvocations[key]
}

// ThreadEnqueueRequest defines the payload for enqueuing a thread message or prompt.
type ThreadEnqueueRequest struct {
	Message      *db.Message `json:"message,omitempty"`
	Prompt       string      `json:"prompt,omitempty"`
	ChannelID    string      `json:"channel_id,omitempty"`
	Title        string      `json:"title,omitempty"`
	MessageID    string      `json:"message_id,omitempty"`
	Model        string      `json:"model,omitempty"`
	Effort       string      `json:"effort,omitempty"`
	SkipIfActive bool        `json:"skip_if_active,omitempty"`
}

// ThreadEnqueueResponse defines the response after enqueuing a thread message.
type ThreadEnqueueResponse struct {
	Status    string `json:"status"` // "accepted", "skipped_active", "already_exists"
	ThreadID  string `json:"thread_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
}

// ThreadTurnRequest defines the payload for executing a thread's pending turn burst.
type ThreadTurnRequest struct {
	Trigger string `json:"trigger,omitempty"`
}

// ThreadTurnResult defines the execution outcome of a burst-drain run.
type ThreadTurnResult struct {
	Status          string `json:"status"`
	ProcessedTurns  int    `json:"processed_turns"`
	MessagesDrained int    `json:"messages_drained"`
}

// CancelTurnRequest defines the cancellation request for a thread.
type CancelTurnRequest struct {
	Reason string `json:"reason,omitempty"`
}

// CancelTurnResponse defines the cancellation response for a thread.
type CancelTurnResponse struct {
	Status              string `json:"status"`
	CancelledCount      int    `json:"cancelled_count"`
	CancelledActiveTurn bool   `json:"cancelled_active_turn"`
}

// ThreadSessionObject implements the Restate Virtual Object keyed by thread_id.
type ThreadSessionObject struct {
	pool      *WorkerPool
	agentName string
}

// NewThreadSessionObject constructs a ThreadSessionObject.
func NewThreadSessionObject(pool *WorkerPool, agentName string) *ThreadSessionObject {
	return &ThreadSessionObject{
		pool:      pool,
		agentName: strings.TrimSpace(agentName),
	}
}

// ServiceName returns the dynamic Restate service name for this Virtual Object.
func (t *ThreadSessionObject) ServiceName() string {
	if t == nil {
		return ThreadSessionServiceName("")
	}
	return ThreadSessionServiceName(t.agentName)
}

// ResolvePromptEffort returns effort when set, else the effort implied by the
// model name (medium models run at medium effort, flash models at low).
func ResolvePromptEffort(model, effort string) string {
	if effort != "" {
		return effort
	}
	if strings.Contains(strings.ToLower(model), "medium") {
		return config.EffortMedium
	}
	if strings.Contains(strings.ToLower(model), "flash") {
		return config.EffortLow
	}
	return ""
}

func (t *ThreadSessionObject) shouldSkipActive(ctx restate.ObjectSharedContext, targetThreadID string, req ThreadEnqueueRequest) (bool, error) {
	if (req.Message != nil || !IsBuildCompletionPrompt(req.Prompt)) && restate.Key(ctx) == targetThreadID {
		activeInvID, _ := restate.Get[string](ctx, "active_invocation_id")
		if activeInvID != "" {
			return true, nil
		}
	}
	if t.pool == nil || t.pool.cfg.Store == nil {
		return false, nil
	}
	type activeTurnResult struct {
		Active bool `json:"active"`
	}
	res, err := restate.Run(ctx, func(rc restate.RunContext) (activeTurnResult, error) {
		checkID := targetThreadID
		if req.Message == nil && t.pool != nil {
			checkID = t.pool.RedirectBuildPrompt(checkID, req.Prompt)
		}
		active, hErr := t.pool.cfg.Store.HasActiveThreadTurn(rc, checkID)
		if hErr != nil {
			return activeTurnResult{}, hErr
		}
		return activeTurnResult{Active: active}, nil
	}, restate.WithName("check-active-thread-turn"))
	if err != nil {
		return false, err
	}
	return res.Active, nil
}

func deriveDeterministicMessageID(ctx restate.ObjectSharedContext, targetThreadID string) string {
	if ctx != nil && ctx.Request() != nil && strings.TrimSpace(ctx.Request().ID) != "" {
		return uuid.NewSHA1(uuid.NameSpaceURL, []byte("aerial:thread:msg:"+targetThreadID+":"+strings.TrimSpace(ctx.Request().ID))).String()
	}
	return uuid.New().String()
}

func deriveDeterministicDispatchID(ctx restate.ObjectSharedContext, sessionKey string) string {
	if ctx != nil && ctx.Request() != nil && strings.TrimSpace(ctx.Request().ID) != "" {
		return uuid.NewSHA1(uuid.NameSpaceURL, []byte("aerial:dispatch:"+sessionKey+":"+strings.TrimSpace(ctx.Request().ID))).String()
	}
	return uuid.New().String()
}

// Enqueue queues a message or prompt into the thread's persistent backlog.
func (t *ThreadSessionObject) Enqueue(ctx restate.ObjectSharedContext, req ThreadEnqueueRequest) (ThreadEnqueueResponse, error) {
	threadID := restate.Key(ctx)
	if threadID == "" {
		threadID = strings.TrimSpace(req.ChannelID)
	}
	if req.Message != nil && strings.TrimSpace(req.Message.ThreadID) != "" {
		threadID = strings.TrimSpace(req.Message.ThreadID)
	}

	targetThreadID := strings.TrimSpace(threadID)
	if targetThreadID == "" {
		return ThreadEnqueueResponse{}, restate.ToTerminalError(errors.New("thread_id or channel_id is required"), restate.WithErrorCode(400))
	}

	// 1. Check SkipIfActive synchronously without inserting a DB row.
	if req.SkipIfActive {
		skip, sErr := t.shouldSkipActive(ctx, targetThreadID, req)
		if sErr != nil {
			return ThreadEnqueueResponse{}, sErr
		}
		if skip {
			return ThreadEnqueueResponse{
				Status:   eriwire.StatusSkippedActive,
				ThreadID: targetThreadID,
			}, nil
		}
	}

	// 2. Insert PENDING db.Message row in Postgres inside restate.Run.
	type messageInsertResult struct {
		ThreadID  string `json:"thread_id"`
		MessageID string `json:"message_id"`
	}

	insertRes, rErr := restate.Run(ctx, func(rc restate.RunContext) (messageInsertResult, error) {
		effThreadID := targetThreadID
		var msg db.Message
		if req.Message != nil {
			msg = *req.Message
			if strings.TrimSpace(msg.ThreadID) != "" {
				effThreadID = strings.TrimSpace(msg.ThreadID)
			}
		} else {
			channelID := effThreadID
			if t.pool != nil {
				channelID = t.pool.RedirectBuildPrompt(channelID, req.Prompt)
				channelID = t.pool.ResolveTargetThread(channelID, strings.TrimSpace(req.Title), req.Prompt, time.Now().UTC())
			}
			effThreadID = channelID

			msgID := strings.TrimSpace(req.MessageID)
			effort := ResolvePromptEffort(strings.TrimSpace(req.Model), strings.TrimSpace(req.Effort))
			msg = db.Message{
				ID:         msgID,
				ThreadID:   effThreadID,
				AuthorID:   httpClientAuthorID,
				AuthorName: "HTTP Client",
				IsAdmin:    true,
				Content:    req.Prompt,
				Summary:    db.CleanTaskSummary(req.Prompt),
				Status:     db.StatusPending,
				Effort:     effort,
				ChannelID:  effThreadID,
				Model:      strings.TrimSpace(req.Model),
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
		}

		if msg.ID == "" {
			msg.ID = deriveDeterministicMessageID(ctx, effThreadID)
		}
		if msg.ThreadID == "" {
			msg.ThreadID = effThreadID
		}
		msg.Status = db.StatusPending

		if t.pool != nil && t.pool.cfg.Store != nil {
			if err := t.pool.cfg.Store.InsertMessage(rc, msg); err != nil {
				log.Printf("[ThreadSession] Warning inserting message %s: %v", msg.ID, err)
				return messageInsertResult{}, err
			}
		}
		return messageInsertResult{ThreadID: effThreadID, MessageID: msg.ID}, nil
	}, restate.WithName("insert-pending-message"))
	if rErr != nil {
		return ThreadEnqueueResponse{}, rErr
	}

	// 3. Fire one-way RunTurn notification to serialize burst execution.
	svcName := t.ServiceName()
	_ = restate.ObjectSend(ctx, svcName, insertRes.ThreadID, "RunTurn").Send(ThreadTurnRequest{})

	return ThreadEnqueueResponse{
		Status:    eriwire.StatusAccepted,
		ThreadID:  insertRes.ThreadID,
		MessageID: insertRes.MessageID,
	}, nil
}

const maxCoalescedBurstMessages = 5

type burstStepResult struct {
	ClaimedCount int `json:"claimed_count"`
}

func (t *ThreadSessionObject) claimPendingBurst(ctx context.Context, threadID string) ([]db.Message, error) {
	msgs, err := t.pool.cfg.Store.GetPendingOrProcessingMessages(ctx, 0)
	if err != nil {
		return nil, err
	}
	var burst []db.Message
	for _, m := range msgs {
		if m.ThreadID != threadID {
			continue
		}
		if m.Status == db.StatusProcessing {
			if rErr := t.pool.cfg.Store.ResetMessageToPendingWithRestart(ctx, m.ID, "recovering in-flight message on restate step retry"); rErr != nil {
				continue
			}
			claimed, cErr := t.pool.cfg.Store.ClaimPendingMessage(ctx, m.ID)
			if cErr != nil || !claimed {
				continue
			}
			m.Status = db.StatusProcessing
			m.RestartCount++
			burst = append(burst, m)
			if len(burst) >= maxCoalescedBurstMessages {
				break
			}
			continue
		}
		if m.Status != db.StatusPending {
			continue
		}
		claimed, cErr := t.pool.cfg.Store.ClaimPendingMessage(ctx, m.ID)
		if cErr != nil || !claimed {
			continue
		}
		m.Status = db.StatusProcessing
		burst = append(burst, m)
		if len(burst) >= maxCoalescedBurstMessages {
			break
		}
	}
	return burst, nil
}

// RunTurn executes an exclusive burst-drain loop for thread_id.
func (t *ThreadSessionObject) RunTurn(ctx restate.ObjectContext, req ThreadTurnRequest) (ThreadTurnResult, error) {
	threadID := restate.Key(ctx)
	invID := ctx.Request().ID
	restate.Set(ctx, "active_invocation_id", invID)
	registerActiveInvocation(threadID, invID)
	defer func() {
		unregisterActiveInvocation(threadID, invID)
		restate.Clear(ctx, "active_invocation_id")
	}()

	var result ThreadTurnResult
	for {
		stepOut, err := restate.Run(ctx, func(rc restate.RunContext) (burstStepResult, error) {
			if t.pool == nil || t.pool.cfg.Store == nil {
				return burstStepResult{ClaimedCount: 0}, nil
			}
			burst, cErr := t.claimPendingBurst(rc, threadID)
			if cErr != nil {
				return burstStepResult{}, cErr
			}
			if len(burst) == 0 {
				return burstStepResult{ClaimedCount: 0}, nil
			}

			if execErr := t.pool.executeClaimedBurst(burst, threadID, rc); execErr != nil {
				log.Printf("[ThreadSession] Error executing claimed burst for %s: %v", threadID, execErr)
				return burstStepResult{}, execErr
			}
			if rc.Err() != nil {
				return burstStepResult{}, rc.Err()
			}
			return burstStepResult{ClaimedCount: len(burst)}, nil
		}, restate.WithName("claim-and-execute-burst"))
		if err != nil {
			return result, err
		}
		if stepOut.ClaimedCount == 0 {
			break
		}
		result.ProcessedTurns++
		result.MessagesDrained += stepOut.ClaimedCount
	}

	result.Status = statusCompleted
	return result, nil
}

func cancelPendingMessagesInStore(ctx context.Context, pool *WorkerPool, threadID, reason string) (int, error) {
	if pool == nil || pool.cfg.Store == nil {
		return 0, nil
	}
	msgs, err := pool.cfg.Store.GetPendingOrProcessingMessages(ctx, 0)
	if err != nil {
		return 0, err
	}
	cancelledCount := 0
	for _, m := range msgs {
		if m.ThreadID == threadID && m.Status == db.StatusPending {
			if uErr := pool.cfg.Store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, reason); uErr != nil {
				return cancelledCount, uErr
			}
			cancelledCount++
		}
	}
	return cancelledCount, nil
}

// CancelTurn cancels pending messages, records active invocation cancellation, and terminates daemon processes.
func (t *ThreadSessionObject) CancelTurn(ctx restate.ObjectSharedContext, req CancelTurnRequest) (CancelTurnResponse, error) {
	threadID := restate.Key(ctx)
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "cancelled by operator"
	}

	type cancelMessagesResult struct {
		CancelledCount int `json:"cancelled_count"`
	}
	res, rErr := restate.Run(ctx, func(rc restate.RunContext) (cancelMessagesResult, error) {
		count, err := cancelPendingMessagesInStore(rc, t.pool, threadID, reason)
		if err != nil {
			return cancelMessagesResult{}, err
		}
		return cancelMessagesResult{CancelledCount: count}, nil
	}, restate.WithName("cancel-pending-thread-messages"))
	if rErr != nil {
		return CancelTurnResponse{}, rErr
	}

	activeInvID, _ := restate.Get[string](ctx, "active_invocation_id")
	if activeInvID == "" {
		activeInvID = getActiveInvocation(threadID)
	}
	cancelledActive := false
	if activeInvID != "" {
		restate.CancelInvocation(ctx, activeInvID)
		cancelledActive = true
	}

	if t.pool != nil {
		t.pool.CloseThreadProcesses(threadID, reason)
	}

	return CancelTurnResponse{
		Status:              "ok",
		CancelledCount:      res.CancelledCount,
		CancelledActiveTurn: cancelledActive,
	}, nil
}

// DispatchTurnResult defines the execution outcome of dispatch draining.
type DispatchTurnResult struct {
	Status             string `json:"status"`
	DispatchesExecuted int    `json:"dispatches_executed"`
}

// CancelDispatchRequest defines the cancellation payload for a dispatch or build.
type CancelDispatchRequest struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	ChunkID    string `json:"chunk_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// CancelDispatchResponse defines the response after cancelling dispatches.
type CancelDispatchResponse struct {
	Status         string   `json:"status"`
	CancelledCount int      `json:"cancelled_count"`
	CancelledIDs   []string `json:"cancelled_ids,omitempty"`
}

// DispatchSessionObject implements the Restate Virtual Object keyed by dispatch session key.
type DispatchSessionObject struct {
	pool      *WorkerPool
	agentName string
}

// NewDispatchSessionObject constructs a DispatchSessionObject.
func NewDispatchSessionObject(pool *WorkerPool, agentName string) *DispatchSessionObject {
	return &DispatchSessionObject{
		pool:      pool,
		agentName: strings.TrimSpace(agentName),
	}
}

// ServiceName returns the dynamic Restate service name for this Virtual Object.
func (d *DispatchSessionObject) ServiceName() string {
	if d == nil {
		return DispatchSessionServiceName("")
	}
	return DispatchSessionServiceName(d.agentName)
}

func (d *DispatchSessionObject) shouldSkipActive(ctx restate.ObjectSharedContext, sessionKey, dispID string) (bool, error) {
	// The marker read stays journaled for replay compatibility, but only the store decides:
	// a marker left by a cancelled RunTurn must not skip the next Enqueue.
	_, _ = restate.Get[string](ctx, "active_invocation_id")
	if d.pool == nil || d.pool.cfg.Store == nil {
		return false, nil
	}
	type activeTurnResult struct {
		Active bool `json:"active"`
	}
	res, err := restate.Run(ctx, func(rc restate.RunContext) (activeTurnResult, error) {
		active, hErr := d.pool.cfg.Store.HasActiveDispatchTurn(rc, sessionKey, dispID)
		if hErr != nil {
			return activeTurnResult{}, hErr
		}
		return activeTurnResult{Active: active}, nil
	}, restate.WithName("check-active-dispatch-turn"))
	if err != nil {
		return false, err
	}
	return res.Active, nil
}

// Enqueue inserts a pending dispatch and triggers exclusive RunTurn processing.
func (d *DispatchSessionObject) Enqueue(ctx restate.ObjectSharedContext, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	sessionKey := restate.Key(ctx)
	if sessionKey == "" {
		keyReq := req
		if strings.TrimSpace(keyReq.ID) == "" {
			keyReq.ID = uuid.New().String()
		}
		sessionKey = eriwire.ResolveDispatchSessionKey(keyReq)
	}
	callerSuppliedID := strings.TrimSpace(req.ID) != ""
	dispID := strings.TrimSpace(req.ID)
	if dispID == "" {
		dispID = deriveDeterministicDispatchID(ctx, sessionKey)
		req.ID = dispID
	}

	// 1. Synchronous SkipIfActive check.
	if req.SkipIfActive {
		skip, sErr := d.shouldSkipActive(ctx, sessionKey, dispID)
		if sErr != nil {
			return eriwire.DispatchEnqueueResponse{}, sErr
		}
		if skip {
			return eriwire.DispatchEnqueueResponse{
				Status:     eriwire.StatusSkippedActive,
				DispatchID: dispID,
				SessionID:  sessionKey,
			}, nil
		}
	}

	// 2. Insert PENDING dispatch row in Postgres inside restate.Run.
	type dispatchInsertResult struct {
		Inserted bool `json:"inserted"`
	}

	insertRes, rErr := restate.Run(ctx, func(rc restate.RunContext) (dispatchInsertResult, error) {
		dispRecord := db.Dispatch{
			ID:             dispID,
			Source:         strings.ToLower(strings.TrimSpace(req.Source)),
			SessionID:      sessionKey,
			TargetID:       session.NormalizeEnvTargetID(req.TargetID),
			VanadisChunkID: strings.TrimSpace(req.VanadisChunkID),
			Prompt:         req.Prompt,
			Summary:        db.CleanTaskSummary(req.Prompt),
			Effort:         strings.ToLower(strings.TrimSpace(req.Effort)),
			Model:          strings.TrimSpace(req.Model),
			Status:         db.StatusPending,
			CreatedAt:      time.Now().UTC(),
			UpdatedAt:      time.Now().UTC(),
		}
		if d.pool != nil && d.pool.cfg.Store != nil {
			inserted, err := d.pool.cfg.Store.InsertDispatch(rc, dispRecord)
			if err != nil {
				log.Printf("[DispatchSession] Warning inserting dispatch %s: %v", dispID, err)
				return dispatchInsertResult{}, err
			}
			return dispatchInsertResult{Inserted: inserted}, nil
		}
		return dispatchInsertResult{Inserted: true}, nil
	}, restate.WithName("insert-pending-dispatch"))
	if rErr != nil {
		return eriwire.DispatchEnqueueResponse{}, rErr
	}

	// 3. Fire restate.ObjectSend(ctx, svcName, sessionKey, 'RunTurn').Send(req) ALWAYS,
	// even when the row already exists, so retrying an unjournaled enqueue wakes RunTurn.
	svcName := d.ServiceName()
	_ = restate.ObjectSend(ctx, svcName, sessionKey, "RunTurn").Send(req)

	if !insertRes.Inserted && callerSuppliedID {
		return eriwire.DispatchEnqueueResponse{
			Status:     eriwire.StatusAlreadyExists,
			DispatchID: dispID,
			SessionID:  sessionKey,
		}, nil
	}

	return eriwire.DispatchEnqueueResponse{
		Status:     eriwire.StatusAccepted,
		DispatchID: dispID,
		SessionID:  sessionKey,
	}, nil
}

type dispatchDrainStepResult struct {
	ClaimedCount int `json:"claimed_count"`
}

func matchesDispatchSession(item db.Dispatch, sessionKey, reqID string) bool {
	return item.SessionID == sessionKey ||
		(item.VanadisChunkID != "" && "vanadis-"+item.VanadisChunkID == sessionKey) ||
		"dispatch-"+item.ID == sessionKey ||
		item.ID == reqID
}

func (d *DispatchSessionObject) claimNextDispatch(ctx context.Context, sessionKey, reqID string) (db.Dispatch, bool, error) {
	disps, err := d.pool.cfg.Store.GetPendingOrProcessingDispatches(ctx, 0)
	if err != nil {
		return db.Dispatch{}, false, err
	}
	for _, item := range disps {
		if !matchesDispatchSession(item, sessionKey, reqID) {
			continue
		}
		if item.Status == db.StatusProcessing {
			if rErr := d.pool.cfg.Store.ResetDispatchToPendingWithRestart(ctx, item.ID, "recovering in-flight dispatch on restate step retry"); rErr != nil {
				continue
			}
			claimed, cErr := d.pool.cfg.Store.ClaimPendingDispatch(ctx, item.ID)
			if cErr != nil || !claimed {
				continue
			}
			item.Status = db.StatusProcessing
			item.RestartCount++
			return item, true, nil
		}
		if item.Status != db.StatusPending {
			continue
		}
		claimed, cErr := d.pool.cfg.Store.ClaimPendingDispatch(ctx, item.ID)
		if cErr != nil || !claimed {
			continue
		}
		item.Status = db.StatusProcessing
		return item, true, nil
	}
	return db.Dispatch{}, false, nil
}

// RunTurn drains pending dispatches for sessionKey via turnExecution inside restate.Run.
func (d *DispatchSessionObject) RunTurn(ctx restate.ObjectContext, req eriwire.BrainDispatchRequest) (DispatchTurnResult, error) {
	sessionKey := restate.Key(ctx)
	invID := ctx.Request().ID
	restate.Set(ctx, "active_invocation_id", invID)
	registerActiveInvocation(sessionKey, invID)
	defer unregisterActiveInvocation(sessionKey, invID)

	if d.pool == nil || d.pool.cfg.Store == nil {
		restate.ClearAll(ctx)
		return DispatchTurnResult{Status: statusCompleted}, nil
	}

	executedCount := 0
	for {
		step, runErr := restate.Run(ctx, func(rc restate.RunContext) (dispatchDrainStepResult, error) {
			claimed, found, cErr := d.claimNextDispatch(rc, sessionKey, req.ID)
			if cErr != nil {
				return dispatchDrainStepResult{}, cErr
			}
			if !found {
				return dispatchDrainStepResult{ClaimedCount: 0}, nil
			}
			if execErr := d.pool.executeClaimedDispatch(claimed, sessionKey, rc); execErr != nil {
				log.Printf("[DispatchSession] Error executing claimed dispatch for %s: %v", sessionKey, execErr)
				return dispatchDrainStepResult{}, execErr
			}
			if rc.Err() != nil {
				return dispatchDrainStepResult{}, rc.Err()
			}
			return dispatchDrainStepResult{ClaimedCount: 1}, nil
		}, restate.WithName("execute-dispatch"))

		if runErr != nil {
			restate.Clear(ctx, "active_invocation_id")
			return DispatchTurnResult{Status: "error", DispatchesExecuted: executedCount}, runErr
		}
		if step.ClaimedCount == 0 {
			break
		}
		executedCount += step.ClaimedCount
	}

	restate.ClearAll(ctx)
	return DispatchTurnResult{
		Status:             statusCompleted,
		DispatchesExecuted: executedCount,
	}, nil
}

type cancelDispatchesResult struct {
	CancelledIDs []string `json:"cancelled_ids"`
}

func cancelDispatchSession(ctx restate.Context, pool *WorkerPool, sessionKey, dispatchID, buildID, reason, activeInvID, caller string) (CancelDispatchResponse, error) {
	if reason == "" {
		reason = "cancelled dispatch"
	}
	if dispatchID == "" && strings.HasPrefix(sessionKey, "dispatch-") {
		dispatchID = strings.TrimPrefix(sessionKey, "dispatch-")
	}
	if buildID == "" && strings.HasPrefix(sessionKey, "vanadis-") {
		buildID = strings.TrimPrefix(sessionKey, "vanadis-")
	}

	var cancelledIDs []string
	if pool != nil && pool.cfg.Store != nil {
		res, rErr := restate.Run(ctx, func(rc restate.RunContext) (cancelDispatchesResult, error) {
			cIDs, err := pool.cfg.Store.CancelPendingDispatches(rc, dispatchID, buildID, reason)
			if err != nil {
				log.Printf("[%s] Error cancelling dispatches in store: %v", caller, err)
				return cancelDispatchesResult{}, err
			}
			return cancelDispatchesResult{CancelledIDs: cIDs}, nil
		}, restate.WithName("cancel-pending-dispatches"))
		if rErr != nil {
			return CancelDispatchResponse{}, rErr
		}
		cancelledIDs = res.CancelledIDs
	}

	if pool != nil {
		for _, pid := range pool.CancelDispatches(dispatchID, buildID, reason) {
			if !slices.Contains(cancelledIDs, pid) {
				cancelledIDs = append(cancelledIDs, pid)
			}
		}
	}

	if activeInvID == "" {
		activeInvID = getActiveInvocation(sessionKey)
	}
	if activeInvID == "" && dispatchID != "" {
		activeInvID = getActiveInvocation("dispatch-" + dispatchID)
	}
	if activeInvID == "" && buildID != "" {
		activeInvID = getActiveInvocation("vanadis-" + buildID)
	}
	if activeInvID != "" {
		restate.CancelInvocation(ctx, activeInvID)
	}

	return CancelDispatchResponse{
		Status:         "ok",
		CancelledCount: len(cancelledIDs),
		CancelledIDs:   cancelledIDs,
	}, nil
}

// Cancel marks dispatches cancelled, terminates execution, and cancels active Restate invocations.
func (d *DispatchSessionObject) Cancel(ctx restate.ObjectSharedContext, req CancelDispatchRequest) (CancelDispatchResponse, error) {
	sessionKey := restate.Key(ctx)
	activeInvID, _ := restate.Get[string](ctx, "active_invocation_id")
	buildID := strings.TrimSpace(req.BuildID)
	if buildID == "" {
		buildID = strings.TrimSpace(req.ChunkID)
	}
	return cancelDispatchSession(ctx, d.pool, sessionKey, strings.TrimSpace(req.DispatchID), buildID, strings.TrimSpace(req.Reason), activeInvID, "DispatchSession")
}

// VoiceSessionRequest represents a request to execute a voice turn.
type VoiceSessionRequest struct {
	TurnID         string `json:"turn_id"`
	Prompt         string `json:"prompt"`
	DeviceKey      string `json:"device_key,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	Effort         string `json:"effort,omitempty"`
}

// VoiceSessionResult represents the outcome of a voice turn.
type VoiceSessionResult struct {
	Reply          string `json:"reply"`
	ConversationID string `json:"conversation_id"`
	SessionID      string `json:"session_id"`
	DeviceKey      string `json:"device_key"`
}

// VoiceStreamEvent represents an event streamed to Core NATS during a voice turn.
type VoiceStreamEvent struct {
	Event          string `json:"event"`
	Text           string `json:"text,omitempty"`
	Status         string `json:"status,omitempty"`
	Reply          string `json:"reply,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	Error          string `json:"error,omitempty"`
}

// VoiceSessionObject implements the Restate Virtual Object keyed by device_key.
type VoiceSessionObject struct {
	pool      *WorkerPool
	agentName string
}

// NewVoiceSessionObject constructs a VoiceSessionObject.
func NewVoiceSessionObject(pool *WorkerPool, agentName string) *VoiceSessionObject {
	return &VoiceSessionObject{
		pool:      pool,
		agentName: strings.TrimSpace(agentName),
	}
}

// ServiceName returns the dynamic Restate service name for this Virtual Object.
func (v *VoiceSessionObject) ServiceName() string {
	if v == nil {
		return VoiceSessionServiceName("")
	}
	return VoiceSessionServiceName(v.agentName)
}

// Ask serializes per-device voice turns across pods, streaming events over Core NATS.
func (v *VoiceSessionObject) Ask(ctx restate.ObjectContext, req VoiceSessionRequest) (VoiceSessionResult, error) {
	deviceKey := restate.Key(ctx)
	if deviceKey == "" {
		deviceKey = strings.TrimSpace(req.DeviceKey)
	}
	if deviceKey == "" {
		deviceKey = strings.TrimSpace(req.SessionID)
	}
	if deviceKey == "" {
		deviceKey = strings.TrimSpace(req.ConversationID)
	}
	if deviceKey == "" {
		err := errors.New("device_key is required")
		return VoiceSessionResult{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}
	if strings.TrimSpace(req.Prompt) == "" {
		err := errors.New("prompt is required")
		return VoiceSessionResult{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	turnID := strings.TrimSpace(req.TurnID)
	if turnID == "" {
		turnID = uuid.New().String()
	}

	res, err := restate.Run(ctx, func(rc restate.RunContext) (VoiceSessionResult, error) {
		if v.pool == nil {
			return VoiceSessionResult{}, restate.ToTerminalError(errors.New("worker pool is nil"), restate.WithErrorCode(500))
		}

		subject := "voice.stream." + turnID
		publishEvent := func(ev VoiceStreamEvent) {
			if bus := v.pool.EventBus(); bus != nil {
				msgID := fmt.Sprintf("%s:%s:%d", turnID, ev.Event, time.Now().UnixNano())
				if pErr := bus.Publish(rc, subject, msgID, ev); pErr != nil {
					log.Printf("[VoiceSession] Error publishing voice stream event: %v", pErr)
				}
			}
		}

		onStatus := func(status string) {
			publishEvent(VoiceStreamEvent{
				Event:  "status",
				Status: status,
			})
		}

		onSentence := func(sentence string) {
			publishEvent(VoiceStreamEvent{
				Event: "sentence",
				Text:  sentence,
			})
		}

		reply, convID, turnErr := v.pool.ExecuteVoiceTurn(rc, req.Prompt, deviceKey, onStatus, onSentence)
		if turnErr != nil {
			publishEvent(VoiceStreamEvent{
				Event: "error",
				Error: turnErr.Error(),
			})
			return VoiceSessionResult{}, turnErr
		}

		publishEvent(VoiceStreamEvent{
			Event:          "reply",
			Reply:          reply,
			ConversationID: convID,
		})

		if v.pool.Store() != nil && strings.TrimSpace(convID) != "" {
			threadID := "voice-" + strings.TrimSpace(deviceKey)
			if rErr := v.pool.Store().RotateSessionID(rc, threadID, convID); rErr != nil {
				log.Printf("[VoiceSession] Warning rotating voice session in DB for %s: %v", threadID, rErr)
			}
		}

		v.pool.EnqueueTranscriptPublish(TranscriptPublishJob{
			SessionID: convID,
			ThreadID:  "voice-" + deviceKey,
		})

		return VoiceSessionResult{
			Reply:          reply,
			ConversationID: convID,
			SessionID:      convID,
			DeviceKey:      deviceKey,
		}, nil
	}, restate.WithName("execute-voice"))
	if err != nil {
		return VoiceSessionResult{}, err
	}

	return res, nil
}

//nolint:contextcheck
func (te *turnExecution) isRowStillProcessing(ctx context.Context) bool {
	if te == nil || te.pool == nil || te.pool.cfg.Store == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queryCtx := context.WithoutCancel(ctx)
	if te.isDispatch() {
		if te.dispatch == nil {
			return false
		}
		d, err := te.pool.cfg.Store.GetDispatch(queryCtx, te.dispatch.ID)
		return err != nil || (d != nil && d.Status == db.StatusProcessing)
	}
	for _, m := range te.burst {
		msg, err := te.pool.cfg.Store.GetMessage(queryCtx, m.ID)
		if err != nil || (msg != nil && msg.Status == db.StatusProcessing) {
			return true
		}
	}
	return false
}

func (p *WorkerPool) acquireScopeLock(scopeKey string) func() {
	lockVal, _ := p.scopeLocks.LoadOrStore(scopeKey, &sync.Mutex{})
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(scopeKey, scopeLock)
	}
	scopeLock.Lock()
	return scopeLock.Unlock
}

func (p *WorkerPool) checkTurnCompletionError(ctx context.Context, te *turnExecution, abortErr error) error {
	if te.finalized.Load() && !te.isRowStillProcessing(ctx) {
		return nil
	}
	if (!te.finalized.Load() || te.isRowStillProcessing(ctx)) && !te.isInterrupted() {
		return abortErr
	}
	if ctx != nil && ctx.Err() != nil && !te.isInterrupted() {
		return ctx.Err()
	}
	if p.ctx.Err() != nil && !te.isInterrupted() {
		return p.ctx.Err()
	}
	return nil
}

func (te *turnExecution) shouldRunBurstTurn() bool {
	if !te.resolveTurnPolicy() {
		return false
	}
	if te.evaluateAmbientWake() {
		return false
	}
	if te.turnCount > session.DefaultMaxSessionTurns {
		metrics.RecordUnrotatedSessionTurn()
	}
	return true
}

// executeClaimedBurst runs a single coalesced turnExecution for an atomically claimed burst.
func (p *WorkerPool) executeClaimedBurst(burst []db.Message, threadID string, execCtx ...context.Context) error {
	if len(burst) == 0 {
		return nil
	}
	var callerCtx context.Context
	if len(execCtx) > 0 && execCtx[0] != nil {
		callerCtx = execCtx[0]
	}
	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	scopeKey := threadID
	unlockScope := p.acquireScopeLock(scopeKey)
	defer unlockScope()

	execStart := time.Now().UTC()
	triggerType := resolveBurstTriggerType(burst[0])
	spanCtx, turnSpan := p.beginTurnTrace(burstTraceItems(burst, p.startedAt), threadID, triggerType, execStart)
	turnCtx, turnCancel := context.WithCancel(spanCtx)
	defer turnCancel()

	te := &turnExecution{
		pool:        p,
		burst:       burst,
		threadID:    threadID,
		triggerType: triggerType,
		execStart:   execStart,
		wakeIdx:     -1,
		isEphemeral: false,
		stopTyping:  func() {},
		turnCtx:     turnCtx,
		turnCancel:  turnCancel,
		turnSpan:    turnSpan,
	}

	p.registerActiveTurn(scopeKey, te)
	defer p.unregisterActiveTurn(scopeKey, te)
	defer te.endTurnSpan()
	te.beginSetupSpan()
	defer te.endSetupSpan()

	defer te.runPostTurnHook()
	defer te.stopIndicators()

	defer func() {
		if r := recover(); r != nil {
			te.handleBurstPanic(r)
		}
	}()

	if !te.shouldRunBurstTurn() {
		return nil
	}

	abortErr := fmt.Errorf("burst execution aborted unfinalized for thread %s", threadID)
	if !te.acquirePoolAndLease() {
		return p.checkTurnCompletionError(callerCtx, te, abortErr)
	}
	defer func() {
		if te.lease != nil {
			if relErr := te.lease.Release(); relErr != nil {
				log.Printf("[WorkerPool] Warning releasing lease for %s: %v", te.threadID, relErr)
			}
		}
	}()
	markTurnInactive := te.markTurnActive()
	defer markTurnInactive()

	if !te.runPreTurnHook() {
		return nil
	}

	te.turnAttempted = true
	promptCtx, promptSpan := te.startSetupChildSpan(te.turnCtxValue(), spanPrompt)
	te.buildTurnPrompt(promptCtx)
	promptSpan.End()
	te.executeWithRetries()
	te.refreshThreadSummaryAtTurnEnd(te.turnCtxValue())

	return p.checkTurnCompletionError(callerCtx, te, abortErr)
}

// executeClaimedDispatch runs a single turnExecution for an atomically claimed dispatch.
func (p *WorkerPool) executeClaimedDispatch(d db.Dispatch, scopeKey string, execCtx ...context.Context) error {
	var callerCtx context.Context
	if len(execCtx) > 0 && execCtx[0] != nil {
		callerCtx = execCtx[0]
	}
	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	unlockScope := p.acquireScopeLock(scopeKey)
	defer unlockScope()

	isEphem := strings.TrimSpace(d.SessionID) == ""
	leaseKey := d.SessionID
	if isEphem {
		leaseKey = "dispatch-" + d.ID
	}

	execStart := time.Now().UTC()
	triggerType := "dispatch:" + d.Source
	spanCtx, turnSpan := p.beginTurnTrace([]queuedTurnItem{dispatchTraceItem(d, p.startedAt)}, leaseKey, triggerType, execStart)
	turnCtx, turnCancel := context.WithCancel(spanCtx)
	defer turnCancel()

	te := &turnExecution{
		pool:        p,
		dispatch:    &d,
		threadID:    leaseKey,
		triggerType: triggerType,
		execStart:   execStart,
		wakeIdx:     -1,
		isEphemeral: isEphem,
		stopTyping:  func() {},
		turnCtx:     turnCtx,
		turnCancel:  turnCancel,
		turnSpan:    turnSpan,
	}

	p.registerActiveTurn(scopeKey, te)
	defer p.unregisterActiveTurn(scopeKey, te)
	defer te.endTurnSpan()
	te.beginSetupSpan()
	defer te.endSetupSpan()

	cleanupDispatch := p.setupDispatchTurn(d, te)
	defer cleanupDispatch()

	defer func() {
		if r := recover(); r != nil {
			te.handleBurstPanic(r)
		}
	}()

	abortErr := fmt.Errorf("dispatch execution aborted unfinalized for %s", scopeKey)
	if !te.acquirePoolAndLease() {
		return p.checkTurnCompletionError(callerCtx, te, abortErr)
	}
	defer te.releaseActivePoolLease()
	markTurnInactive := te.markTurnActive()
	defer markTurnInactive()

	te.turnAttempted = true
	promptCtx, promptSpan := te.startSetupChildSpan(te.turnCtxValue(), spanPrompt)
	te.turnPrompt = te.buildDispatchPrompt(promptCtx)
	promptSpan.End()
	te.executeWithRetries()

	return p.checkTurnCompletionError(callerCtx, te, abortErr)
}

// SessionManagerService provides unkeyed control operations that run concurrently without blocking on Virtual Object key locks.
type SessionManagerService struct {
	pool      *WorkerPool
	agentName string
}

// NewSessionManagerService constructs a SessionManagerService.
func NewSessionManagerService(pool *WorkerPool, agentName string) *SessionManagerService {
	return &SessionManagerService{
		pool:      pool,
		agentName: strings.TrimSpace(agentName),
	}
}

// ServiceName returns the dynamic Restate service name for this Service.
func (s *SessionManagerService) ServiceName() string {
	if s == nil {
		return SessionManagerServiceName("")
	}
	return SessionManagerServiceName(s.agentName)
}

// CancelDispatch preempts in-flight dispatches and cancels pending DB records without waiting on Virtual Object key locks.
func (s *SessionManagerService) CancelDispatch(ctx restate.Context, req CancelDispatchControlRequest) (CancelDispatchResponse, error) {
	buildID := strings.TrimSpace(req.BuildID)
	if buildID == "" {
		buildID = strings.TrimSpace(req.ChunkID)
	}
	return cancelDispatchSession(ctx, s.pool, strings.TrimSpace(req.SessionKey), strings.TrimSpace(req.DispatchID), buildID, strings.TrimSpace(req.Reason), "", "SessionManager")
}
