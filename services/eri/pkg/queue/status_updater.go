package queue

import (
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/bwmarrin/discordgo"
)

const (
	DefaultStatusTickerInterval    = 1500 * time.Millisecond
	DefaultStatusDebounceDelay     = 1500 * time.Millisecond
	DefaultStatusToolLatchDuration = 2000 * time.Millisecond
	DefaultStatusMinEditInterval   = 1000 * time.Millisecond
	MaxStatusTextLength            = 100
)

// resolveToolVerb returns the natural language action verb for a tool category.
func resolveToolVerb(toolName string) string {
	lower := strings.ToLower(strings.TrimSpace(toolName))
	cleanLower := strings.TrimPrefix(strings.TrimPrefix(lower, "mcp_"), "call_mcp_tool_")
	switch {
	case cleanLower == "edit_file" || cleanLower == "replace_file_content":
		return "Editing"
	case cleanLower == "write_file":
		return "Writing"
	case cleanLower == "read_file" || cleanLower == "view_file":
		return "Reading"
	case cleanLower == "run_command" || cleanLower == "execute_command" || cleanLower == "bash" || cleanLower == "sh":
		return "Executing"
	case strings.HasPrefix(lower, "call_mcp_tool") || cleanLower == "call_mcp_tool":
		return "Calling"
	case cleanLower == "grep_search" || cleanLower == "find_by_name" || cleanLower == "search_web" || cleanLower == "web_search":
		return "Searching"
	case cleanLower == "invoke_subagent" || cleanLower == "define_subagent":
		return "Spawning"
	default:
		return "Executing"
	}
}

// isFileTool returns true if the tool operates on a file path.
func isFileTool(cleanLower string) bool {
	switch cleanLower {
	case "edit_file", "write_file", "read_file", "view_file", "replace_file_content":
		return true
	default:
		return false
	}
}

// FormatToolStatus dynamically formats an active tool status badge.
func FormatToolStatus(toolName, commandName string, elapsed time.Duration) string {
	rawLower := strings.ToLower(strings.TrimSpace(toolName))
	verb := resolveToolVerb(toolName)
	clean := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_"), "call_mcp_tool_")
	cleanLower := strings.ToLower(clean)
	if cleanLower == "" {
		cleanLower = "tool"
		clean = "tool"
	}
	sec := max(elapsed.Seconds(), 0.1)

	cmd := strings.TrimSpace(commandName)
	if isFileTool(cleanLower) && cmd != "" {
		cmd = filepath.Base(cmd)
	}
	if strings.HasPrefix(rawLower, "call_mcp_tool_") && cmd == "" && cleanLower != "call_mcp_tool" {
		cmd = clean
	}
	if len([]rune(cmd)) > 28 {
		cmd = string([]rune(cmd)[:28])
	}

	var text string
	if verb == "Executing" && (cleanLower == "run_command" || cleanLower == "execute_command" || cleanLower == "bash" || cleanLower == "sh") {
		if cmd == "" {
			cmd = "command"
		}
		text = fmt.Sprintf("⚡ Executing `%s`... (%.1fs)", cmd, sec)
	} else if cmd != "" {
		text = fmt.Sprintf("⚡ %s `%s`... (%.1fs)", verb, cmd, sec)
	} else {
		text = fmt.Sprintf("⚡ Running `%s`... (%.1fs)", clean, sec)
	}

	if len([]rune(text)) > MaxStatusTextLength {
		text = string([]rune(text)[:MaxStatusTextLength])
	}
	return text
}

// FormatCapacityBackoffStatus formats a capacity/rate-limit backoff countdown status string.
func FormatCapacityBackoffStatus(remaining time.Duration) string {
	secs := max(int(math.Ceil(remaining.Seconds())), 1)
	return fmt.Sprintf("Waiting on model capacity (retrying in %ds)...", secs)
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
	phase                string // "thinking", "tool", "responding", "backoff"
	toolStart            time.Time
	turnStart            time.Time
	backoffUntil         time.Time
	lastEditAt           time.Time
	inFlightREST         bool
	dirty                bool
	disabled             bool
	enabled              bool
	syncDelete           bool
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

// WithSyncDelete configures StatusUpdater to delete status messages synchronously.
func WithSyncDelete() StatusUpdaterOption {
	return func(u *StatusUpdater) {
		u.syncDelete = true
	}
}

// NewChunkStepStatusUpdater creates a StatusUpdater multiplexed onto a Vanadis build chunk active step.
func NewChunkStepStatusUpdater(chunkID string, flushFn func(step string) error, opts ...StatusUpdaterOption) *StatusUpdater {
	trimmedID := strings.TrimSpace(chunkID)
	enabled := trimmedID != "" && flushFn != nil
	allOpts := make([]StatusUpdaterOption, 0, len(opts)+2)
	allOpts = append(allOpts, WithSyncDelete(), func(u *StatusUpdater) {
		u.sendFunc = func(channelID, text string) (string, error) {
			if flushFn == nil {
				return trimmedID, nil
			}
			return trimmedID, flushFn(text)
		}
		u.editFunc = func(channelID, messageID, text string) error {
			if flushFn == nil {
				return nil
			}
			return flushFn(text)
		}
		u.deleteFunc = func(channelID, messageID string) error {
			if flushFn == nil {
				return nil
			}
			return flushFn("")
		}
	})
	allOpts = append(allOpts, opts...)
	return NewStatusUpdater(nil, trimmedID, enabled, allOpts...)
}

// NewStatusUpdater constructs a thread status updater.
func NewStatusUpdater(s *discordgo.Session, threadID string, enabled bool, opts ...StatusUpdaterOption) *StatusUpdater {
	u := &StatusUpdater{
		s: s, threadID: threadID, enabled: enabled && threadID != "",
		tickerInterval: DefaultStatusTickerInterval, debounceDelay: DefaultStatusDebounceDelay,
		toolLatchDuration: DefaultStatusToolLatchDuration, minEditInterval: DefaultStatusMinEditInterval,
		turnStart: time.Now(), done: make(chan struct{}), flushNotify: make(chan struct{}, 1),
		sendFunc: func(channelID, text string) (string, error) {
			if s == nil {
				return "", fmt.Errorf("discord session is nil")
			}
			msg, err := s.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{Content: text, Flags: discordgo.MessageFlagsSuppressEmbeds})
			if err != nil {
				return "", err
			}
			return msg.ID, nil
		},
		editFunc: func(channelID, messageID, text string) error {
			return delivery.EditMessage(s, channelID, messageID, text)
		},
		deleteFunc: func(channelID, messageID string) error { return delivery.DeleteMessage(s, channelID, messageID) },
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
		u.phase, u.activeCommand, u.dirty = "thinking", "", true
	case typ == "tool_call" || typ == "tool":
		toolName := ev.ResolvedToolName()
		if ev.State == "DONE" || ev.State == "ERROR" {
			if u.activeTool == toolName || toolName == "" || u.activeCommand == toolName || u.activeTool == "call_mcp_tool" || toolName == "call_mcp_tool" {
				if u.activeTool != "" {
					u.lastCompletedTool, u.lastCompletedCommand, u.lastCompletedAt, u.lastCompletedElapsed = u.activeTool, u.activeCommand, time.Now(), time.Since(u.toolStart)
				}
				u.activeTool, u.activeCommand, u.phase, u.dirty = "", "", "thinking", true
			}
		} else {
			u.activeTool, u.activeCommand, u.toolStart, u.lastCompletedTool, u.lastCompletedCommand, u.phase, u.dirty, shouldTriggerFlush = toolName, ev.ResolvedToolDetail(), time.Now(), "", "", "tool", true, true
		}
	case typ == "agent_response" || typ == "text_delta":
		u.activeCommand = ""
		if u.phase != "responding" {
			u.phase, u.dirty, shouldTriggerFlush = "responding", true, true
		}
	}
	u.mu.Unlock()
	if shouldTriggerFlush {
		u.triggerFlush()
	}
}

// SetCapacityBackoff switches the updater to a capacity backoff countdown and triggers a flush.
func (u *StatusUpdater) SetCapacityBackoff(delay time.Duration) {
	if u == nil || !u.enabled {
		return
	}
	u.mu.Lock()
	if u.disabled {
		u.mu.Unlock()
		return
	}
	u.phase, u.activeTool, u.activeCommand, u.lastCompletedTool, u.lastCompletedCommand, u.backoffUntil, u.dirty = "backoff", "", "", "", "", time.Now().Add(delay), true
	u.mu.Unlock()
	u.triggerFlush()
}

// MarkTurnStarted sets the turn start timestamp to when the runner actually begins execution.
func (u *StatusUpdater) MarkTurnStarted() {
	if u == nil {
		return
	}
	u.mu.Lock()
	u.turnStart = time.Now()
	if u.phase == "backoff" {
		u.phase, u.backoffUntil, u.dirty = "", time.Time{}, true
	}
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

func (u *StatusUpdater) currentStatusText() string {
	switch u.phase {
	case "backoff":
		return FormatCapacityBackoffStatus(time.Until(u.backoffUntil))
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

func (u *StatusUpdater) hasToolActivityLocked() bool {
	return u.activeTool != "" || (u.lastCompletedTool != "" && time.Since(u.lastCompletedAt) < u.toolLatchDuration)
}

func (u *StatusUpdater) prepareFlushPayloadLocked() (string, string, bool) {
	if u.disabled || u.inFlightREST {
		return "", "", false
	}
	hasToolActivity := u.hasToolActivityLocked()
	isBackoff := u.phase == "backoff"
	hasActivePhase := hasToolActivity || isBackoff || u.phase == "thinking" || u.phase == "responding"
	if !u.dirty && (u.statusMessageID == "" || !hasActivePhase) {
		return "", "", false
	}
	if u.statusMessageID == "" && !hasToolActivity && !isBackoff && time.Since(u.turnStart) < u.debounceDelay {
		return "", "", false
	}
	text := u.currentStatusText()
	if u.statusMessageID != "" && text == u.lastDelivered {
		u.dirty = false
		return "", "", false
	}
	msgID := u.statusMessageID
	u.inFlightREST, u.dirty = true, false
	return msgID, text, true
}

func (u *StatusUpdater) sendInitialStatusMessage(text string) {
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
}

func (u *StatusUpdater) editExistingStatusMessage(msgID, text string) {
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

func (u *StatusUpdater) flush() {
	u.mu.Lock()
	msgID, text, ok := u.prepareFlushPayloadLocked()
	u.mu.Unlock()
	if !ok {
		return
	}

	if msgID == "" {
		u.sendInitialStatusMessage(text)
		return
	}
	u.editExistingStatusMessage(msgID, text)
}

// Stop terminates the ticker loop and blocks until any active in-flight edit returns.
func (u *StatusUpdater) Stop() {
	if u == nil {
		return
	}
	u.stopOnce.Do(func() { close(u.done) })
	u.wg.Wait()
}

func (u *StatusUpdater) performDeleteMessage(threadID, msgID, warnSuffix string) {
	if err := u.deleteFunc(threadID, msgID); err != nil && !delivery.IsMessageNotFoundError(err) {
		log.Printf("[StatusUpdater] Warning deleting status message%s: %v", warnSuffix, err)
	}
}

func (u *StatusUpdater) dispatchDeleteMessage(threadID, msgID, warnSuffix string) {
	if u.syncDelete {
		u.performDeleteMessage(threadID, msgID, warnSuffix)
		return
	}
	go u.performDeleteMessage(threadID, msgID, warnSuffix)
}

func (u *StatusUpdater) resetState(deleteRemote, disable bool, warnSuffix string) {
	if u == nil {
		return
	}
	u.mu.Lock()
	msgID := u.statusMessageID
	u.statusMessageID, u.activeTool, u.activeCommand, u.lastCompletedTool, u.lastCompletedCommand = "", "", "", "", ""
	if disable {
		u.disabled = true
	} else {
		u.phase, u.dirty, u.lastDelivered, u.turnStart, u.backoffUntil = "", false, "", time.Now(), time.Time{}
	}
	u.mu.Unlock()

	if deleteRemote && msgID != "" {
		u.dispatchDeleteMessage(u.threadID, msgID, warnSuffix)
	}
}

// Reset resets updater state for retry attempts and deletes any active message.
func (u *StatusUpdater) Reset() { u.resetState(true, false, "") }

// DeleteStatusMessage cleans up the status message from Discord with zero tombstone.
func (u *StatusUpdater) DeleteStatusMessage() { u.resetState(true, true, " on shutdown") }
