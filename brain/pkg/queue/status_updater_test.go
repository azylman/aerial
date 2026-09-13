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
	t.Parallel()
	tests := []struct {
		toolName    string
		commandName string
		elapsed     time.Duration
		expected    string
	}{
		{"view_file", "", 1200 * time.Millisecond, "⚡ Running `view_file`... (1.2s)"},
		{"mcp_docker_list_containers", "", 2500 * time.Millisecond, "⚡ Running `docker_list_containers`... (2.5s)"},
		{"call_mcp_tool_github_create_pr", "", 3100 * time.Millisecond, "⚡ Running `github_create_pr`... (3.1s)"},
		{"", "", 500 * time.Millisecond, "⚡ Running `tool`... (0.5s)"},
		// run_command with command name
		{"run_command", "git", 1200 * time.Millisecond, "⚡ Executing `git`... (1.2s)"},
		{"run_command", "docker-compose", 2500 * time.Millisecond, "⚡ Executing `docker-compose`... (2.5s)"},
		{"run_command", "verify.sh", 800 * time.Millisecond, "⚡ Executing `verify.sh`... (0.8s)"},
		// run_command with empty command fallback
		{"run_command", "", 500 * time.Millisecond, "⚡ Executing `command`... (0.5s)"},
		{"mcp_run_command", "go", 1500 * time.Millisecond, "⚡ Executing `go`... (1.5s)"},
	}

	for _, tt := range tests {
		got := FormatToolStatus(tt.toolName, tt.commandName, tt.elapsed)
		if got != tt.expected {
			t.Errorf("FormatToolStatus(%q, %q, %v) = %q, want %q", tt.toolName, tt.commandName, tt.elapsed, got, tt.expected)
		}
	}
}

func TestStatusUpdater_DebounceAndFastTurn(t *testing.T) {
	t.Parallel()
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
	time.Sleep(15 * time.Millisecond) // Exercises live ticker select branch once
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
	t.Parallel()
	var sends atomic.Int32
	var edits atomic.Int32
	var deletes atomic.Int32
	var lastText atomic.Pointer[string]
	deleteDone := make(chan struct{}, 1)

	updater := NewStatusUpdater(nil, "thread-456", true,
		WithStatusInterval(1*time.Hour),
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
				select {
				case deleteDone <- struct{}{}:
				default:
				}
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

	// Synchronously flush active tool
	updater.flush()

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

	updater.flush()

	if edits.Load() == 0 {
		t.Errorf("expected at least 1 edit after phase change, got %d", edits.Load())
	}

	updater.Stop()
	updater.DeleteStatusMessage()

	select {
	case <-deleteDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for async deleteFunc")
	}

	if deletes.Load() != 1 {
		t.Errorf("expected 1 delete at turn cleanup, got %d", deletes.Load())
	}
}

func TestStatusUpdater_CircuitBreakerOn404(t *testing.T) {
	t.Parallel()
	var edits atomic.Int32

	updater := NewStatusUpdater(nil, "thread-789", true,
		WithStatusInterval(1*time.Hour),
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
	updater.flush()

	// Step updates continue arriving
	for i := 0; i < 5; i++ {
		updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
		updater.flush()
	}

	updater.Stop()

	// Edits should be capped at 1 because 404 disabled the updater
	if edits.Load() > 1 {
		t.Errorf("expected circuit breaker to halt edits after 404, got %d edits", edits.Load())
	}
}

func TestStatusUpdater_CircuitBreakerOn403(t *testing.T) {
	t.Parallel()
	var sends atomic.Int32

	updater := NewStatusUpdater(nil, "thread-403", true,
		WithStatusInterval(1*time.Hour),
		WithStatusDebounce(0),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				sends.Add(1)
				return "", errors.New("HTTP 403 Forbidden, 50013 Missing Permissions")
			},
			func(channelID, messageID, text string) error {
				return nil
			},
			func(channelID, messageID string) error {
				return nil
			},
		),
	)

	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
	updater.flush()

	// Subsequent flush calls should not attempt sendFunc again
	updater.flush()
	updater.Stop()

	if sends.Load() != 1 {
		t.Errorf("expected circuit breaker to stop sends after 403, got %d sends", sends.Load())
	}
}

func TestStatusUpdater_ZombieLeakPrevention(t *testing.T) {
	t.Parallel()
	var sends atomic.Int32
	var deletes atomic.Int32
	sendStarted := make(chan struct{})
	sendRelease := make(chan struct{})
	deleteDone := make(chan struct{}, 1)

	updater := NewStatusUpdater(nil, "thread-zombie", true,
		WithStatusInterval(1*time.Hour),
		WithStatusDebounce(0),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				sends.Add(1)
				close(sendStarted)
				<-sendRelease
				return "msg-slow-send", nil
			},
			func(channelID, messageID, text string) error {
				return nil
			},
			func(channelID, messageID string) error {
				if messageID == "msg-slow-send" {
					deletes.Add(1)
					select {
					case deleteDone <- struct{}{}:
					default:
					}
				}
				return nil
			},
		),
	)

	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "slow_tool", State: "ACTIVE"})
	go updater.flush()

	<-sendStarted // Wait until sendFunc is actively in flight

	// Turn finishes or errors out concurrently while sendFunc is executing
	updater.DeleteStatusMessage()

	// Unblock sendFunc
	close(sendRelease)

	select {
	case <-deleteDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for zombie deleteFunc")
	}

	updater.Stop()

	// The message created by sendFunc must have been deleted to avoid zombie leak
	if deletes.Load() != 1 {
		t.Errorf("expected zombie message to be cleaned up immediately, got %d deletes", deletes.Load())
	}
}

func TestStatusUpdater_LiveTimerRefreshDuringLongTool(t *testing.T) {
	t.Parallel()
	var edits atomic.Int32

	updater := NewStatusUpdater(nil, "thread-timer", true,
		WithStatusInterval(1*time.Hour),
		WithStatusDebounce(0),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				return "msg-timer", nil
			},
			func(channelID, messageID, text string) error {
				edits.Add(1)
				return nil
			},
			func(channelID, messageID string) error {
				return nil
			},
		),
	)

	// Tool starts - only 1 event is emitted
	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "long_tool", State: "ACTIVE"})
	updater.flush() // First flush sends "msg-timer"

	// Simulate passage of time by backdating toolStart under mutex
	updater.mu.Lock()
	updater.toolStart = time.Now().Add(-250 * time.Millisecond)
	updater.mu.Unlock()
	updater.flush() // Formatted string advances to (0.2s or 0.3s) -> edits=1

	updater.mu.Lock()
	updater.toolStart = time.Now().Add(-500 * time.Millisecond)
	updater.mu.Unlock()
	updater.flush() // Formatted string advances to (0.5s) -> edits=2

	updater.Stop()

	// Live timer should have updated multiple times as elapsed seconds increased
	if edits.Load() < 2 {
		t.Errorf("expected timer to refresh across ticks during long tool, got %d edits", edits.Load())
	}
}

func TestStatusUpdater_ResetOnRetry(t *testing.T) {
	t.Parallel()
	var deletes atomic.Int32
	deleteDone := make(chan struct{}, 1)

	updater := NewStatusUpdater(nil, "thread-reset", true,
		WithStatusInterval(1*time.Hour),
		WithStatusDebounce(0),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				return "msg-retry-1", nil
			},
			func(channelID, messageID, text string) error {
				return nil
			},
			func(channelID, messageID string) error {
				deletes.Add(1)
				select {
				case deleteDone <- struct{}{}:
				default:
				}
				return nil
			},
		),
	)

	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "flaky_tool", State: "ACTIVE"})
	updater.flush()

	// Attempt fails; Reset() is called before retry backoff
	updater.Reset()

	select {
	case <-deleteDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for reset deleteFunc")
	}

	if deletes.Load() != 1 {
		t.Errorf("expected status message from failed attempt to be deleted on Reset, got %d deletes", deletes.Load())
	}

	updater.Stop()
}

func TestStatusUpdater_RunCommandDisplaysCommandName(t *testing.T) {
	t.Parallel()
	var sends atomic.Int32
	var lastText atomic.Pointer[string]

	updater := NewStatusUpdater(nil, "thread-cmd", true,
		WithStatusInterval(1*time.Hour),
		WithStatusDebounce(0),
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) {
				sends.Add(1)
				lastText.Store(&text)
				return "msg-cmd", nil
			},
			func(channelID, messageID, text string) error {
				lastText.Store(&text)
				return nil
			},
			func(channelID, messageID string) error {
				return nil
			},
		),
	)

	updater.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool",
		ToolName: "run_command",
		State:    "ACTIVE",
		ToolInfo: &runner.StepToolInfo{
			Name: "run_command",
			Parameters: runner.StepToolParameters{
				CommandLine: "git status",
			},
		},
	})

	updater.flush()

	if sends.Load() != 1 {
		t.Fatalf("expected 1 send, got %d", sends.Load())
	}
	last := lastText.Load()
	if last == nil || !strings.Contains(*last, "Executing `git`") {
		t.Errorf("expected text to contain 'Executing `git`', got %v", last)
	}

	// Tool completes
	updater.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool",
		ToolName: "run_command",
		State:    "DONE",
	})
	updater.flush()

	updater.Stop()
	updater.DeleteStatusMessage()
}


