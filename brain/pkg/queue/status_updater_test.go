package queue

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestFormatToolStatus(t *testing.T) {
	tests := []struct {
		input    string
		elapsed  time.Duration
		expected string
	}{
		{"view_file", 1200 * time.Millisecond, "⚡ Running view_file... (1.2s)"},
		{"mcp_docker_list_containers", 2500 * time.Millisecond, "⚡ Running docker_list_containers... (2.5s)"},
		{"call_mcp_tool_github_create_pr", 3100 * time.Millisecond, "⚡ Running github_create_pr... (3.1s)"},
		{"", 500 * time.Millisecond, "⚡ Running tool... (0.5s)"},
	}

	for _, tt := range tests {
		got := FormatToolStatus(tt.input, tt.elapsed)
		if got != tt.expected {
			t.Errorf("FormatToolStatus(%q, %v) = %q, want %q", tt.input, tt.elapsed, got, tt.expected)
		}
	}
}

func TestStatusUpdater_DebounceAndFastTurn(t *testing.T) {
	var sends atomic.Int32
	var edits atomic.Int32
	var deletes atomic.Int32

	updater := NewStatusUpdater(nil, "thread-123", true,
		WithStatusInterval(10*time.Millisecond),
		WithStatusDebounce(50*time.Millisecond),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				sends.Add(1)
				return "msg-1", nil
			},
			func(channelID, messageID, text string) error {
				edits.Add(1)
				return nil
			},
			func(channelID, messageID string) error {
				deletes.Add(1)
				return nil
			},
		),
	)

	// Turn finishes fast (< 50ms) without running tools
	updater.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	time.Sleep(20 * time.Millisecond)
	updater.Stop()
	updater.DeleteStatusMessage()

	// No message should have been sent or deleted
	if sends.Load() != 0 {
		t.Errorf("expected 0 sends for fast turn, got %d", sends.Load())
	}
	if deletes.Load() != 0 {
		t.Errorf("expected 0 deletes when no status message was created, got %d", deletes.Load())
	}
}

func TestStatusUpdater_ActiveToolBypassesDebounceAndFlushes(t *testing.T) {
	var sends atomic.Int32
	var edits atomic.Int32
	var deletes atomic.Int32
	var lastText atomic.Pointer[string]

	updater := NewStatusUpdater(nil, "thread-456", true,
		WithStatusInterval(10*time.Millisecond),
		WithStatusDebounce(100*time.Millisecond),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				sends.Add(1)
				lastText.Store(&text)
				return "msg-456", nil
			},
			func(channelID, messageID, text string) error {
				edits.Add(1)
				lastText.Store(&text)
				return nil
			},
			func(channelID, messageID string) error {
				deletes.Add(1)
				return nil
			},
		),
	)

	// Active tool call arrives
	updater.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool",
		ToolName: "view_file",
		State:    "ACTIVE",
	})

	// Wait for ticker to fire
	time.Sleep(30 * time.Millisecond)

	if sends.Load() != 1 {
		t.Fatalf("expected 1 send immediately on active tool, got %d", sends.Load())
	}
	if last := lastText.Load(); last == nil || !strings.Contains(*last, "view_file") {
		t.Errorf("expected text to mention view_file, got %v", last)
	}

	// Tool completes, response starts
	updater.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool",
		ToolName: "view_file",
		State:    "DONE",
	})
	updater.HandleStep(&runner.StepUpdateEvent{
		StepType: "agent_response",
	})

	time.Sleep(30 * time.Millisecond)

	if edits.Load() == 0 {
		t.Errorf("expected at least 1 edit after phase change, got %d", edits.Load())
	}

	updater.Stop()
	updater.DeleteStatusMessage()

	if deletes.Load() != 1 {
		t.Errorf("expected 1 delete at turn cleanup, got %d", deletes.Load())
	}
}

func TestStatusUpdater_CircuitBreakerOn404(t *testing.T) {
	var edits atomic.Int32

	updater := NewStatusUpdater(nil, "thread-789", true,
		WithStatusInterval(10*time.Millisecond),
		WithStatusDebounce(0),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				return "msg-789", nil
			},
			func(channelID, messageID, text string) error {
				edits.Add(1)
				return errors.New("HTTP 404 Not Found, 10008 Unknown Message")
			},
			func(channelID, messageID string) error {
				return nil
			},
		),
	)

	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
	time.Sleep(20 * time.Millisecond)

	// Step updates continue arriving
	for i := 0; i < 5; i++ {
		updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
		time.Sleep(15 * time.Millisecond)
	}

	updater.Stop()

	// Edits should be capped at 1 because 404 disabled the updater
	if edits.Load() > 1 {
		t.Errorf("expected circuit breaker to halt edits after 404, got %d edits", edits.Load())
	}
}
