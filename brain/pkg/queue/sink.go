package queue

import (
	"log"
	"strings"
	"sync"

	"github.com/azylman/aerial/brain/pkg/runner"
)

// DiscordMessageEditor provides message modification and deletion capabilities in Discord channels.
type DiscordMessageEditor interface {
	ChannelMessageEdit(channelID, messageID, content string) error
	ChannelMessageDelete(channelID, messageID string) error
}

// ThrowawayTurnSink implements runner.TurnSink for fire-and-forget or transient turns
// such as ambient classification and title summarization.
type ThrowawayTurnSink struct {
	resCh chan string
	errCh chan error
	once  sync.Once
}

var _ runner.TurnSink = (*ThrowawayTurnSink)(nil)

// NewThrowawayTurnSink creates a new buffered ThrowawayTurnSink.
func NewThrowawayTurnSink() *ThrowawayTurnSink {
	return &ThrowawayTurnSink{
		resCh: make(chan string, 1),
		errCh: make(chan error, 1),
	}
}

// OnTurnStarted is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnTurnStarted() {}

// OnThinking is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnThinking() {}

// OnToolCall is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnToolCall(toolName, commandName string) {}

// OnTextDelta is a no-op for throwaway turns.
func (s *ThrowawayTurnSink) OnTextDelta(delta string) {}

// OnResult delivers the final response string to resCh guarded by sync.Once.
func (s *ThrowawayTurnSink) OnResult(res *runner.TurnResult) {
	s.once.Do(func() {
		var response string
		if res != nil {
			response = res.Response
		}
		s.resCh <- response
	})
}

// OnError delivers the error to errCh guarded by sync.Once.
func (s *ThrowawayTurnSink) OnError(err error) {
	s.once.Do(func() {
		s.errCh <- err
	})
}

// Result blocks waiting for turn result or error.
func (s *ThrowawayTurnSink) Result() (string, error) {
	select {
	case res := <-s.resCh:
		return res, nil
	case err := <-s.errCh:
		return "", err
	}
}

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
