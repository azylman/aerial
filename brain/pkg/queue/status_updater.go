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

// FormatToolStatus dynamically formats an active tool status badge without hardcoded dictionaries.
// For command execution tools (e.g. run_command), it avoids repetitive phrasing and displays
// strictly the executable name without arguments or sensitive parameters.
func FormatToolStatus(toolName, commandName string, elapsed time.Duration) string {
	clean := strings.TrimSpace(toolName)
	clean = strings.TrimPrefix(clean, "mcp_")
	clean = strings.TrimPrefix(clean, "call_mcp_tool_")
	if clean == "" {
		clean = "tool"
	}
	sec := elapsed.Seconds()
	if sec < 0.1 {
		sec = 0.1
	}

	var text string
	if clean == "run_command" || clean == "execute_command" || clean == "bash" || clean == "sh" || commandName != "" {
		cmd := strings.TrimSpace(commandName)
		if cmd == "" {
			cmd = "command"
		}
		if len([]rune(cmd)) > 24 {
			cmd = string([]rune(cmd)[:24])
		}
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

	// Dependency injection hooks for hermetic unit testing
	sendFunc   func(channelID, text string) (string, error)
	editFunc   func(channelID, messageID, text string) error
	deleteFunc func(channelID, messageID string) error
}

// StatusUpdaterOption configures a StatusUpdater instance.
type StatusUpdaterOption func(*StatusUpdater)

// WithStatusInterval sets a custom ticker flush interval.
func WithStatusInterval(interval time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) {
		if interval > 0 {
			u.tickerInterval = interval
		}
	}
}

// WithStatusDebounce sets a custom initial debounce holdoff duration.
func WithStatusDebounce(debounce time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) {
		u.debounceDelay = debounce
	}
}

// WithStatusToolLatch sets a custom duration to latch completed tool status before reverting to thinking.
func WithStatusToolLatch(duration time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) {
		if duration > 0 {
			u.toolLatchDuration = duration
		}
	}
}

// WithStatusMockFuncs injects mock REST handlers for hermetic unit tests.
func WithStatusMockFuncs(
	send func(channelID, text string) (string, error),
	edit func(channelID, messageID, text string) error,
	del func(channelID, messageID string) error,
) StatusUpdaterOption {
	return func(u *StatusUpdater) {
		if send != nil {
			u.sendFunc = send
		}
		if edit != nil {
			u.editFunc = edit
		}
		if del != nil {
			u.deleteFunc = del
		}
	}
}

// NewStatusUpdater constructs a thread status updater.
func NewStatusUpdater(s *discordgo.Session, threadID string, enabled bool, opts ...StatusUpdaterOption) *StatusUpdater {
	u := &StatusUpdater{
		s:                 s,
		threadID:          threadID,
		enabled:           enabled && threadID != "",
		tickerInterval:    DefaultStatusTickerInterval,
		debounceDelay:     DefaultStatusDebounceDelay,
		toolLatchDuration: DefaultStatusToolLatchDuration,
		minEditInterval:   DefaultStatusMinEditInterval,
		turnStart:         time.Now(),
		done:              make(chan struct{}),
		flushNotify:       make(chan struct{}, 1),
	}

	// Default REST implementations via delivery / discordgo
	u.sendFunc = func(channelID, text string) (string, error) {
		if u.s == nil {
			return "", fmt.Errorf("discord session is nil")
		}
		msg, err := u.s.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{
			Content: text,
			Flags:   discordgo.MessageFlagsSuppressEmbeds,
		})
		if err != nil {
			return "", err
		}
		return msg.ID, nil
	}
	u.editFunc = func(channelID, messageID, text string) error {
		return delivery.EditMessage(u.s, channelID, messageID, text)
	}
	u.deleteFunc = func(channelID, messageID string) error {
		return delivery.DeleteMessage(u.s, channelID, messageID)
	}

	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}

	if u.enabled {
		u.Start()
	}

	return u
}

// triggerFlush signals the background runner to perform an event-driven flush.
func (u *StatusUpdater) triggerFlush() {
	if !u.enabled {
		return
	}
	select {
	case u.flushNotify <- struct{}{}:
	default:
	}
}

// HandleStep processes incoming intermediate events from runner.activityTap.
func (u *StatusUpdater) HandleStep(ev *runner.StepUpdateEvent) {
	if u == nil || !u.enabled {
		return
	}

	u.mu.Lock()
	if u.disabled {
		u.mu.Unlock()
		return
	}

	typ := ev.ResolvedType()
	var shouldTriggerFlush bool

	switch {
	case typ == "thinking":
		u.phase = "thinking"
		u.activeCommand = ""
		u.dirty = true
	case typ == "tool_call" || typ == "tool":
		toolName := ev.ResolvedToolName()
		if ev.State == "DONE" || ev.State == "ERROR" {
			if u.activeTool == toolName || toolName == "" {
				if u.activeTool != "" {
					u.lastCompletedTool = u.activeTool
					u.lastCompletedCommand = u.activeCommand
					u.lastCompletedAt = time.Now()
					u.lastCompletedElapsed = time.Since(u.toolStart)
				}
				u.activeTool = ""
				u.activeCommand = ""
				u.phase = "thinking"
				u.dirty = true
			}
		} else {
			u.activeTool = toolName
			u.activeCommand = ev.ResolvedCommandName()
			u.toolStart = time.Now()
			u.lastCompletedTool = ""
			u.lastCompletedCommand = ""
			u.phase = "tool"
			u.dirty = true
			shouldTriggerFlush = true
		}
	case typ == "agent_response" || typ == "text_delta":
		u.activeCommand = ""
		if u.phase != "responding" {
			u.phase = "responding"
			u.dirty = true
			shouldTriggerFlush = true
		}
	}
	u.mu.Unlock()

	if shouldTriggerFlush {
		u.triggerFlush()
	}
}

// MarkTurnStarted sets the turn start timestamp to when the runner actually begins execution.
func (u *StatusUpdater) MarkTurnStarted() {
	if u == nil {
		return
	}
	u.mu.Lock()
	u.turnStart = time.Now()
	u.mu.Unlock()
}

// Start launches the background ticker goroutine with idempotency protection.
func (u *StatusUpdater) Start() {
	if !u.enabled {
		return
	}
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
					if canFlush {
						u.flush()
					}
				}
			}
		}()
	})
}

// currentStatusText formats the current status string under mutex lock.
func (u *StatusUpdater) currentStatusText() string {
	switch u.phase {
	case "tool":
		if u.activeTool != "" {
			elapsed := time.Since(u.toolStart)
			return FormatToolStatus(u.activeTool, u.activeCommand, elapsed)
		}
		fallthrough
	case "responding":
		sec := time.Since(u.turnStart).Seconds()
		if sec < 0.1 {
			sec = 0.1
		}
		return fmt.Sprintf("✍️ Generating response... (%.1fs)", sec)
	case "thinking":
		if u.activeTool == "" && u.lastCompletedTool != "" && time.Since(u.lastCompletedAt) < u.toolLatchDuration {
			return FormatToolStatus(u.lastCompletedTool, u.lastCompletedCommand, u.lastCompletedElapsed)
		}
		sec := time.Since(u.turnStart).Seconds()
		if sec < 0.1 {
			sec = 0.1
		}
		return fmt.Sprintf("💭 Thinking... (%.1fs)", sec)
	default:
		if u.lastCompletedTool != "" && time.Since(u.lastCompletedAt) < u.toolLatchDuration {
			return FormatToolStatus(u.lastCompletedTool, u.lastCompletedCommand, u.lastCompletedElapsed)
		}
		sec := time.Since(u.turnStart).Seconds()
		if sec < 0.1 {
			sec = 0.1
		}
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

	// Debounce check: on initial message creation, hold off until debounceDelay has elapsed
	// unless a tool is actively executing or was recently completed.
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
	u.inFlightREST = true
	u.dirty = false
	u.mu.Unlock()

	if msgID == "" {
		newID, err := u.sendFunc(u.threadID, text)
		u.mu.Lock()
		u.inFlightREST = false
		if err != nil {
			if delivery.IsThreadArchivedOrLockedError(err) || delivery.IsPermissionError(err) {
				u.disabled = true
			} else {
				// Transient error: re-mark dirty so next tick can self-heal
				u.dirty = true
			}
			u.mu.Unlock()
			return
		}
		if u.disabled {
			// Turn completed, cancelled, or deleted while sendFunc was in flight.
			// Delete immediately to prevent permanent zombie message leak.
			u.mu.Unlock()
			if err := u.deleteFunc(u.threadID, newID); err != nil && !delivery.IsMessageNotFoundError(err) {
				log.Printf("[StatusUpdater] Warning deleting cancelled status message: %v", err)
			}
			return
		}
		u.statusMessageID = newID
		u.lastDelivered = text
		u.lastEditAt = time.Now()
		u.mu.Unlock()
	} else {
		err := u.editFunc(u.threadID, msgID, text)
		u.mu.Lock()
		u.inFlightREST = false
		if err != nil {
			if delivery.IsMessageNotFoundError(err) {
				u.statusMessageID = ""
				u.disabled = true
			} else if delivery.IsThreadArchivedOrLockedError(err) || delivery.IsPermissionError(err) {
				u.disabled = true
			} else {
				// Transient error: re-mark dirty so next tick can self-heal
				u.dirty = true
			}
			u.mu.Unlock()
		} else {
			u.lastDelivered = text
			u.lastEditAt = time.Now()
			u.mu.Unlock()
		}
	}
}

// Stop terminates the ticker loop and blocks until any active in-flight edit returns.
func (u *StatusUpdater) Stop() {
	if u == nil {
		return
	}
	u.stopOnce.Do(func() {
		close(u.done)
	})
	u.wg.Wait()
}

// Reset resets updater state for retry attempts and deletes any active message.
func (u *StatusUpdater) Reset() {
	if u == nil {
		return
	}
	u.mu.Lock()
	msgID := u.statusMessageID
	u.statusMessageID = ""
	u.activeTool = ""
	u.activeCommand = ""
	u.lastCompletedTool = ""
	u.lastCompletedCommand = ""
	u.phase = ""
	u.dirty = false
	u.lastDelivered = ""
	u.turnStart = time.Now()
	u.mu.Unlock()

	if msgID != "" {
		go func(threadID, messageID string) {
			if err := u.deleteFunc(threadID, messageID); err != nil && !delivery.IsMessageNotFoundError(err) {
				log.Printf("[StatusUpdater] Warning deleting status message: %v", err)
			}
		}(u.threadID, msgID)
	}
}

// DeleteStatusMessage cleans up the status message from Discord with zero tombstone.
// Deletion is executed non-blockingly so the worker thread is never stalled on Discord REST latency.
func (u *StatusUpdater) DeleteStatusMessage() {
	if u == nil {
		return
	}
	u.mu.Lock()
	msgID := u.statusMessageID
	u.statusMessageID = ""
	u.activeTool = ""
	u.activeCommand = ""
	u.lastCompletedTool = ""
	u.lastCompletedCommand = ""
	u.disabled = true
	u.mu.Unlock()

	if msgID != "" {
		go func(threadID, messageID string) {
			if err := u.deleteFunc(threadID, messageID); err != nil && !delivery.IsMessageNotFoundError(err) {
				log.Printf("[StatusUpdater] Warning deleting status message on shutdown: %v", err)
			}
		}(u.threadID, msgID)
	}
}
