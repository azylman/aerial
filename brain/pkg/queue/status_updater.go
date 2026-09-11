package queue

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/bwmarrin/discordgo"
)

const (
	DefaultStatusTickerInterval = 1500 * time.Millisecond
	DefaultStatusDebounceDelay   = 1500 * time.Millisecond
	MaxStatusTextLength          = 100
)

// FormatToolStatus dynamically formats an active tool status badge without hardcoded dictionaries.
func FormatToolStatus(toolName string, elapsed time.Duration) string {
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
	text := fmt.Sprintf("⚡ Running %s... (%.1fs)", clean, sec)
	if len([]rune(text)) > MaxStatusTextLength {
		text = string([]rune(text)[:MaxStatusTextLength])
	}
	return text
}

// StatusUpdater manages real-time intermediate progress updates in Discord thread mode.
type StatusUpdater struct {
	mu              sync.Mutex
	s               *discordgo.Session
	threadID        string
	statusMessageID string
	activeTool      string
	phase           string // "thinking", "tool", "responding"
	toolStart       time.Time
	turnStart       time.Time
	dirty           bool
	disabled        bool
	enabled         bool
	tickerInterval  time.Duration
	debounceDelay   time.Duration

	ticker   *time.Ticker
	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

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
		s:              s,
		threadID:       threadID,
		enabled:        enabled && threadID != "",
		tickerInterval: DefaultStatusTickerInterval,
		debounceDelay:  DefaultStatusDebounceDelay,
		turnStart:      time.Now(),
		done:           make(chan struct{}),
	}

	// Default REST implementations via delivery / discordgo
	u.sendFunc = func(channelID, text string) (string, error) {
		if u.s == nil {
			return "", fmt.Errorf("discord session is nil")
		}
		msg, err := u.s.ChannelMessageSend(channelID, text)
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

// HandleStep processes incoming intermediate events from runner.activityTap.
func (u *StatusUpdater) HandleStep(ev *runner.StepUpdateEvent) {
	if u == nil || !u.enabled {
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.disabled {
		return
	}

	typ := ev.ResolvedType()
	switch {
	case typ == "thinking":
		u.phase = "thinking"
		u.dirty = true
	case typ == "tool_call" || typ == "tool":
		toolName := ev.ResolvedToolName()
		if ev.State == "DONE" || ev.State == "ERROR" {
			if u.activeTool == toolName {
				u.activeTool = ""
				u.dirty = true
			}
		} else {
			u.activeTool = toolName
			u.toolStart = time.Now()
			u.phase = "tool"
			u.dirty = true
		}
	case typ == "agent_response" || typ == "text_delta":
		if u.phase != "responding" {
			u.phase = "responding"
			u.dirty = true
		}
	}
}

// Start launches the background ticker goroutine.
func (u *StatusUpdater) Start() {
	if !u.enabled {
		return
	}
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
			}
		}
	}()
}

// currentStatusText formats the current status string under mutex lock.
func (u *StatusUpdater) currentStatusText() string {
	switch u.phase {
	case "tool":
		if u.activeTool != "" {
			elapsed := time.Since(u.toolStart)
			return FormatToolStatus(u.activeTool, elapsed)
		}
		fallthrough
	case "responding":
		sec := time.Since(u.turnStart).Seconds()
		if sec < 0.1 {
			sec = 0.1
		}
		return fmt.Sprintf("✍️ Generating response... (%.1fs)", sec)
	case "thinking":
		sec := time.Since(u.turnStart).Seconds()
		if sec < 0.1 {
			sec = 0.1
		}
		return fmt.Sprintf("💭 Thinking... (%.1fs)", sec)
	default:
		sec := time.Since(u.turnStart).Seconds()
		if sec < 0.1 {
			sec = 0.1
		}
		return fmt.Sprintf("⚡ Aerial is cooking... (%.1fs)", sec)
	}
}

func (u *StatusUpdater) flush() {
	u.mu.Lock()
	if !u.dirty || u.disabled {
		u.mu.Unlock()
		return
	}

	// Debounce check: on initial message creation, hold off until debounceDelay has elapsed
	// unless a tool is actively executing.
	if u.statusMessageID == "" && u.activeTool == "" && time.Since(u.turnStart) < u.debounceDelay {
		u.mu.Unlock()
		return
	}

	text := u.currentStatusText()
	msgID := u.statusMessageID
	u.dirty = false
	u.mu.Unlock()

	if msgID == "" {
		newID, err := u.sendFunc(u.threadID, text)
		if err != nil {
			u.mu.Lock()
			if delivery.IsThreadArchivedOrLockedError(err) {
				u.disabled = true
			}
			u.mu.Unlock()
			return
		}
		u.mu.Lock()
		u.statusMessageID = newID
		u.mu.Unlock()
	} else {
		err := u.editFunc(u.threadID, msgID, text)
		if err != nil {
			u.mu.Lock()
			if delivery.IsMessageNotFoundError(err) {
				u.statusMessageID = ""
				u.disabled = true
			} else if delivery.IsThreadArchivedOrLockedError(err) {
				u.disabled = true
			}
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

// DeleteStatusMessage cleans up the status message from Discord with zero tombstone.
func (u *StatusUpdater) DeleteStatusMessage() {
	if u == nil {
		return
	}
	u.mu.Lock()
	msgID := u.statusMessageID
	u.statusMessageID = ""
	u.disabled = true
	u.mu.Unlock()

	if msgID != "" {
		_ = u.deleteFunc(u.threadID, msgID)
	}
}
