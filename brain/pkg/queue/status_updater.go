package queue

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/bwmarrin/discordgo"
)

const (
	DefaultStatusTickerInterval    = 1500 * time.Millisecond
	DefaultStatusDebounceDelay      = 1500 * time.Millisecond
	DefaultStatusToolLatchDuration = 2000 * time.Millisecond
	DefaultStatusMinEditInterval   = 1000 * time.Millisecond
	MaxStatusTextLength             = 100
)

// FormatToolStatus dynamically formats an active tool status badge.
func FormatToolStatus(toolName, commandName string, elapsed time.Duration) string {
	clean := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_"), "call_mcp_tool_")
	if clean == "" { clean = "tool" }
	sec := max(elapsed.Seconds(), 0.1)

	var text string
	if clean == "run_command" || clean == "execute_command" || clean == "bash" || clean == "sh" || commandName != "" {
		cmd := strings.TrimSpace(commandName)
		if cmd == "" { cmd = "command" }
		if len([]rune(cmd)) > 24 { cmd = string([]rune(cmd)[:24]) }
		text = fmt.Sprintf("⚡ Executing `%s`... (%.1fs)", cmd, sec)
	} else {
		text = fmt.Sprintf("⚡ Running `%s`... (%.1fs)", clean, sec)
	}

	if len([]rune(text)) > MaxStatusTextLength {
		text = string([]rune(text)[:MaxStatusTextLength])
	}
	return text
}

// StatusUpdater manages real-time intermediate progress updates in Discord thread mode.
type StatusUpdater struct {
	mu                   sync.Mutex
	s                    *discordgo.Session
	threadID             string
	statusMessageID      string
	activeTool           string
	activeCommand        string
	lastCompletedTool    string
	lastCompletedCommand string
	lastCompletedAt      time.Time
	lastCompletedElapsed time.Duration
	phase                string // "thinking", "tool", "responding"
	toolStart            time.Time
	turnStart            time.Time
	lastEditAt           time.Time
	inFlightREST         bool
	dirty                bool
	disabled             bool
	enabled              bool
	tickerInterval       time.Duration
	debounceDelay        time.Duration
	toolLatchDuration    time.Duration
	minEditInterval      time.Duration
	lastDelivered        string

	done        chan struct{}
	flushNotify chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
	wg          sync.WaitGroup

	sendFunc   func(channelID, text string) (string, error)
	editFunc   func(channelID, messageID, text string) error
	deleteFunc func(channelID, messageID string) error
}

// StatusUpdaterOption configures a StatusUpdater instance.
type StatusUpdaterOption func(*StatusUpdater)

// WithStatusInterval sets a custom ticker flush interval.
func WithStatusInterval(interval time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) {
		if interval > 0 { u.tickerInterval = interval }
	}
}

// WithStatusDebounce sets a custom initial debounce holdoff duration.
func WithStatusDebounce(debounce time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) { u.debounceDelay = debounce }
}

// NewStatusUpdater constructs a thread status updater.
func NewStatusUpdater(s *discordgo.Session, threadID string, enabled bool, opts ...StatusUpdaterOption) *StatusUpdater {
	u := &StatusUpdater{
		s: s, threadID: threadID, enabled: enabled && threadID != "",
		tickerInterval: DefaultStatusTickerInterval, debounceDelay: DefaultStatusDebounceDelay,
		toolLatchDuration: DefaultStatusToolLatchDuration, minEditInterval: DefaultStatusMinEditInterval,
		turnStart: time.Now(), done: make(chan struct{}), flushNotify: make(chan struct{}, 1),
		sendFunc: func(channelID, text string) (string, error) {
			if s == nil { return "", fmt.Errorf("discord session is nil") }
			msg, err := s.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{Content: text, Flags: discordgo.MessageFlagsSuppressEmbeds})
			if err != nil { return "", err }
			return msg.ID, nil
		},
		editFunc:   func(channelID, messageID, text string) error { return delivery.EditMessage(s, channelID, messageID, text) },
		deleteFunc: func(channelID, messageID string) error { return delivery.DeleteMessage(s, channelID, messageID) },
	}
	for _, opt := range opts {
		if opt != nil { opt(u) }
	}
	if u.enabled { u.Start() }
	return u
}

func (u *StatusUpdater) triggerFlush() {
	if !u.enabled { return }
	select {
	case u.flushNotify <- struct{}{}:
	default:
	}
}

// HandleStep processes incoming intermediate events from runner.activityTap.
func (u *StatusUpdater) HandleStep(ev *runner.StepUpdateEvent) {
	if u == nil || !u.enabled { return }
	u.mu.Lock()
	if u.disabled {
		u.mu.Unlock()
		return
	}

	typ := ev.ResolvedType()
	var shouldTriggerFlush bool
	switch {
	case typ == "thinking":
		u.phase, u.activeCommand, u.dirty = "thinking", "", true
	case typ == "tool_call" || typ == "tool":
		toolName := ev.ResolvedToolName()
		if ev.State == "DONE" || ev.State == "ERROR" {
			if u.activeTool == toolName || toolName == "" {
				if u.activeTool != "" {
					u.lastCompletedTool, u.lastCompletedCommand, u.lastCompletedAt, u.lastCompletedElapsed = u.activeTool, u.activeCommand, time.Now(), time.Since(u.toolStart)
				}
				u.activeTool, u.activeCommand, u.phase, u.dirty = "", "", "thinking", true
			}
		} else {
			u.activeTool, u.activeCommand, u.toolStart, u.lastCompletedTool, u.lastCompletedCommand, u.phase, u.dirty, shouldTriggerFlush = toolName, ev.ResolvedCommandName(), time.Now(), "", "", "tool", true, true
		}
	case typ == "agent_response" || typ == "text_delta":
		u.activeCommand = ""
		if u.phase != "responding" {
			u.phase, u.dirty, shouldTriggerFlush = "responding", true, true
		}
	}
	u.mu.Unlock()
	if shouldTriggerFlush { u.triggerFlush() }
}

// MarkTurnStarted sets the turn start timestamp to when the runner actually begins execution.
func (u *StatusUpdater) MarkTurnStarted() {
	if u == nil { return }
	u.mu.Lock()
	u.turnStart = time.Now()
	u.mu.Unlock()
}

// Start launches the background ticker goroutine with idempotency protection.
func (u *StatusUpdater) Start() {
	if !u.enabled { return }
	u.startOnce.Do(func() {
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			ticker := time.NewTicker(u.tickerInterval)
			defer ticker.Stop()
			for {
				select {
				case <-u.done:
					return
				case <-ticker.C:
					u.flush()
				case <-u.flushNotify:
					u.mu.Lock()
					canFlush := u.statusMessageID == "" || u.minEditInterval <= 0 || time.Since(u.lastEditAt) >= u.minEditInterval
					u.mu.Unlock()
					if canFlush { u.flush() }
				}
			}
		}()
	})
}

func (u *StatusUpdater) currentStatusText() string {
	switch u.phase {
	case "tool":
		if u.activeTool != "" {
			return FormatToolStatus(u.activeTool, u.activeCommand, time.Since(u.toolStart))
		}
		fallthrough
	case "responding":
		sec := max(time.Since(u.turnStart).Seconds(), 0.1)
		return fmt.Sprintf("✍️ Generating response... (%.1fs)", sec)
	case "thinking":
		if u.activeTool == "" && u.lastCompletedTool != "" && time.Since(u.lastCompletedAt) < u.toolLatchDuration {
			return FormatToolStatus(u.lastCompletedTool, u.lastCompletedCommand, u.lastCompletedElapsed)
		}
		sec := max(time.Since(u.turnStart).Seconds(), 0.1)
		return fmt.Sprintf("💭 Thinking... (%.1fs)", sec)
	default:
		if u.lastCompletedTool != "" && time.Since(u.lastCompletedAt) < u.toolLatchDuration {
			return FormatToolStatus(u.lastCompletedTool, u.lastCompletedCommand, u.lastCompletedElapsed)
		}
		sec := max(time.Since(u.turnStart).Seconds(), 0.1)
		return fmt.Sprintf("⚡ Aerial is cooking... (%.1fs)", sec)
	}
}

func (u *StatusUpdater) flush() {
	u.mu.Lock()
	if u.disabled || u.inFlightREST {
		u.mu.Unlock()
		return
	}
	hasToolActivity := u.activeTool != "" || (u.lastCompletedTool != "" && time.Since(u.lastCompletedAt) < u.toolLatchDuration)
	hasActivePhase := hasToolActivity || u.phase == "thinking" || u.phase == "responding"
	if !u.dirty && (u.statusMessageID == "" || !hasActivePhase) {
		u.mu.Unlock()
		return
	}
	if u.statusMessageID == "" && !hasToolActivity && time.Since(u.turnStart) < u.debounceDelay {
		u.mu.Unlock()
		return
	}
	text := u.currentStatusText()
	if u.statusMessageID != "" && text == u.lastDelivered {
		u.dirty = false
		u.mu.Unlock()
		return
	}
	msgID := u.statusMessageID
	u.inFlightREST, u.dirty = true, false
	u.mu.Unlock()

	if msgID == "" {
		newID, err := u.sendFunc(u.threadID, text)
		u.mu.Lock()
		u.inFlightREST = false
		if err != nil {
			if delivery.IsThreadArchivedOrLockedError(err) || delivery.IsPermissionError(err) {
				u.disabled = true
			} else {
				u.dirty = true
			}
			u.mu.Unlock()
			return
		}
		if u.disabled {
			u.mu.Unlock()
			if err := u.deleteFunc(u.threadID, newID); err != nil && !delivery.IsMessageNotFoundError(err) {
				log.Printf("[StatusUpdater] Warning deleting cancelled status message: %v", err)
			}
			return
		}
		u.statusMessageID, u.lastDelivered, u.lastEditAt = newID, text, time.Now()
		u.mu.Unlock()
	} else {
		err := u.editFunc(u.threadID, msgID, text)
		u.mu.Lock()
		u.inFlightREST = false
		if err != nil {
			if delivery.IsMessageNotFoundError(err) {
				u.statusMessageID, u.disabled = "", true
			} else if delivery.IsThreadArchivedOrLockedError(err) || delivery.IsPermissionError(err) {
				u.disabled = true
			} else {
				u.dirty = true
			}
		} else {
			u.lastDelivered, u.lastEditAt = text, time.Now()
		}
		u.mu.Unlock()
	}
}

// Stop terminates the ticker loop and blocks until any active in-flight edit returns.
func (u *StatusUpdater) Stop() {
	if u == nil { return }
	u.stopOnce.Do(func() { close(u.done) })
	u.wg.Wait()
}

func (u *StatusUpdater) resetState(deleteRemote, disable bool, warnSuffix string) {
	if u == nil { return }
	u.mu.Lock()
	msgID := u.statusMessageID
	u.statusMessageID, u.activeTool, u.activeCommand, u.lastCompletedTool, u.lastCompletedCommand = "", "", "", "", ""
	if disable {
		u.disabled = true
	} else {
		u.phase, u.dirty, u.lastDelivered, u.turnStart = "", false, "", time.Now()
	}
	u.mu.Unlock()

	if deleteRemote && msgID != "" {
		go func(threadID, messageID string) {
			if err := u.deleteFunc(threadID, messageID); err != nil && !delivery.IsMessageNotFoundError(err) {
				log.Printf("[StatusUpdater] Warning deleting status message%s: %v", warnSuffix, err)
			}
		}(u.threadID, msgID)
	}
}

// Reset resets updater state for retry attempts and deletes any active message.
func (u *StatusUpdater) Reset() { u.resetState(true, false, "") }

// DeleteStatusMessage cleans up the status message from Discord with zero tombstone.
func (u *StatusUpdater) DeleteStatusMessage() { u.resetState(true, true, " on shutdown") }
