package queue

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

// DiscordMessageEditor provides message modification and deletion capabilities in Discord channels.
type DiscordMessageEditor interface {
	ChannelMessageEdit(channelID, messageID, content string) error
	ChannelMessageDelete(channelID, messageID string) error
}

// ThrowawayTurnSink is aliased to runner.ThrowawayTurnSink for backwards compatibility.
type ThrowawayTurnSink = runner.ThrowawayTurnSink

// NewThrowawayTurnSink creates a new buffered ThrowawayTurnSink.
var NewThrowawayTurnSink = runner.NewThrowawayTurnSink

// DiscordTurnSink implements runner.TurnSink for interactive Discord turns.
// It manages tool status badge updates, text streaming accumulation,
// typing heartbeat cancellation, and completion/error callbacks.
type DiscordTurnSink struct {
	editor         DiscordMessageEditor
	channelID      string
	badgeMessageID string
	stopTyping     func()
	onComplete     func(res *runner.TurnResult)
	onError        func(err error)

	mu       sync.Mutex
	response strings.Builder
}

var _ runner.TurnSink = (*DiscordTurnSink)(nil)

// NewDiscordTurnSink creates a new DiscordTurnSink.
func NewDiscordTurnSink(
	editor DiscordMessageEditor,
	channelID, badgeMessageID string,
	onComplete func(res *runner.TurnResult),
	onError func(err error),
) *DiscordTurnSink {
	return &DiscordTurnSink{
		editor:         editor,
		channelID:      channelID,
		badgeMessageID: badgeMessageID,
		onComplete:     onComplete,
		onError:        onError,
	}
}

// SetStopTyping registers the function to stop the Discord typing heartbeat.
func (d *DiscordTurnSink) SetStopTyping(fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopTyping = fn
}

func (d *DiscordTurnSink) stopTypingHeartbeat() {
	d.mu.Lock()
	stop := d.stopTyping
	d.stopTyping = nil
	d.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (d *DiscordTurnSink) deleteBadgeMessage() {
	if d.editor != nil && d.badgeMessageID != "" {
		if err := d.editor.ChannelMessageDelete(d.channelID, d.badgeMessageID); err != nil {
			log.Printf("[DiscordTurnSink] Failed to delete status badge %s in channel %s: %v", d.badgeMessageID, d.channelID, err)
		}
	}
}

// OnTurnStarted is called when turn execution begins.
func (d *DiscordTurnSink) OnTurnStarted() {}

// OnThinking is called when the runner enters thinking mode.
func (d *DiscordTurnSink) OnThinking() {}

// OnToolCall updates the status badge message with the executing tool name.
func (d *DiscordTurnSink) OnToolCall(toolName, commandName string) {
	if d.editor != nil && d.badgeMessageID != "" {
		content := "⚙️ Executing " + toolName + "..."
		if err := d.editor.ChannelMessageEdit(d.channelID, d.badgeMessageID, content); err != nil {
			log.Printf("[DiscordTurnSink] Failed to edit status message %s in channel %s: %v", d.badgeMessageID, d.channelID, err)
		}
	}
}

// OnToolCompleted is a no-op for DiscordTurnSink.
func (d *DiscordTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {}

// OnSkillActivated is a no-op for DiscordTurnSink.
func (d *DiscordTurnSink) OnSkillActivated(skillName, source string) {}

// OnTextDelta accumulates streaming text deltas under lock.
func (d *DiscordTurnSink) OnTextDelta(delta string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.response.WriteString(delta)
}

// OnResult stops typing, deletes status badge, and triggers onComplete.
func (d *DiscordTurnSink) OnResult(res *runner.TurnResult) {
	d.stopTypingHeartbeat()
	d.deleteBadgeMessage()
	if d.onComplete != nil {
		d.onComplete(res)
	}
}

// OnError stops typing, deletes status badge, and triggers onError.
func (d *DiscordTurnSink) OnError(err error) {
	d.stopTypingHeartbeat()
	d.deleteBadgeMessage()
	if d.onError != nil {
		d.onError(err)
	}
}

// ResponseText returns the accumulated response text.
func (d *DiscordTurnSink) ResponseText() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.response.String()
}

// TokenChunk represents a sequenced text token delta sent to voice clients.
type TokenChunk struct {
	Seq   uint64 `json:"seq"`
	Delta string `json:"delta"`
}

// WebSocketWriter abstracts WebSocket JSON message writing and lifecycle.
type WebSocketWriter interface {
	WriteJSON(v any) error
	Close() error
}

// VoiceTurnSink implements runner.TurnSink for interactive real-time voice turns.
// It manages sequential speech tokens with monotonic sequence IDs, maintains an
// in-memory sliding replay ring buffer to tolerate Wi-Fi / network blips, supports
// connection re-binding, and logs all communication failures with structured context.
type VoiceTurnSink struct {
	deviceID string
	conn     WebSocketWriter
	connMu   sync.Mutex
	seq      atomic.Uint64
	ringMu   sync.RWMutex
	ringBuf  []TokenChunk
	ringCap  int
}

var _ runner.TurnSink = (*VoiceTurnSink)(nil)

// NewVoiceTurnSink creates a new VoiceTurnSink initialized with a 1024-element replay buffer.
func NewVoiceTurnSink(conn WebSocketWriter, deviceID string) *VoiceTurnSink {
	return &VoiceTurnSink{
		deviceID: deviceID,
		conn:     conn,
		ringCap:  1024,
		ringBuf:  make([]TokenChunk, 0, 1024),
	}
}

// DeviceID returns the target device identifier associated with this sink.
func (v *VoiceTurnSink) DeviceID() string {
	return v.deviceID
}

// OnTurnStarted is called when turn execution begins.
func (v *VoiceTurnSink) OnTurnStarted() {}

// OnThinking is called when the runner enters thinking mode.
func (v *VoiceTurnSink) OnThinking() {}

// OnToolCall is called when a tool invocation begins.
func (v *VoiceTurnSink) OnToolCall(toolName, commandName string) {}

// OnToolCompleted is a no-op for VoiceTurnSink.
func (v *VoiceTurnSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {}

// OnSkillActivated is a no-op for VoiceTurnSink.
func (v *VoiceTurnSink) OnSkillActivated(skillName, source string) {}

// OnTextDelta increments the sequence number, appends the token chunk to the
// sliding replay ring buffer, and streams it to the active WebSocket connection.
func (v *VoiceTurnSink) OnTextDelta(delta string) {
	seq := v.seq.Add(1)
	chunk := TokenChunk{
		Seq:   seq,
		Delta: delta,
	}

	v.ringMu.Lock()
	capLimit := v.ringCap
	if capLimit <= 0 {
		capLimit = 1024
	}
	if len(v.ringBuf) >= capLimit {
		copy(v.ringBuf, v.ringBuf[1:])
		v.ringBuf = v.ringBuf[:len(v.ringBuf)-1]
	}
	v.ringBuf = append(v.ringBuf, chunk)
	v.ringMu.Unlock()

	v.connMu.Lock()
	defer v.connMu.Unlock()
	if v.conn != nil {
		if err := v.conn.WriteJSON(chunk); err != nil {
			log.Printf("[VoiceTurnSink] Error writing chunk seq=%d device=%s: %v", seq, v.deviceID, err)
		}
	}
}

// Rebind attaches a new WebSocket connection and immediately replays any
// unacknowledged token chunks whose sequence number is greater than lastAckSeq.
func (v *VoiceTurnSink) Rebind(newConn WebSocketWriter, lastAckSeq uint64) error {
	v.connMu.Lock()
	defer v.connMu.Unlock()

	v.conn = newConn
	if v.conn == nil {
		return nil
	}

	v.ringMu.RLock()
	defer v.ringMu.RUnlock()

	for _, chunk := range v.ringBuf {
		if chunk.Seq > lastAckSeq {
			if err := v.conn.WriteJSON(chunk); err != nil {
				return fmt.Errorf("failed to replay chunk seq=%d device=%s: %w", chunk.Seq, v.deviceID, err)
			}
		}
	}
	return nil
}

// OnResult delivers an end_of_turn event to the active connection and resets the replay buffer.
func (v *VoiceTurnSink) OnResult(res *runner.TurnResult) {
	v.connMu.Lock()
	if v.conn != nil {
		msg := map[string]string{"type": "end_of_turn"}
		if err := v.conn.WriteJSON(msg); err != nil {
			log.Printf("[VoiceTurnSink] Error writing end_of_turn device=%s: %v", v.deviceID, err)
		}
	}
	v.connMu.Unlock()

	v.ringMu.Lock()
	v.ringBuf = v.ringBuf[:0]
	v.ringMu.Unlock()
}

// OnError delivers an error event to the active connection and resets the replay buffer.
func (v *VoiceTurnSink) OnError(err error) {
	v.connMu.Lock()
	if v.conn != nil {
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		msg := map[string]string{"type": "error", "error": errMsg}
		if writeErr := v.conn.WriteJSON(msg); writeErr != nil {
			log.Printf("[VoiceTurnSink] Error writing error message device=%s: %v", v.deviceID, writeErr)
		}
	}
	v.connMu.Unlock()

	v.ringMu.Lock()
	v.ringBuf = v.ringBuf[:0]
	v.ringMu.Unlock()
}

