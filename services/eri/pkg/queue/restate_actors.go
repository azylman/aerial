package queue

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
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

const maxCoalescedBurstMessages = 5

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
