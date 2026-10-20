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
		{"run_command", "git", 1200 * time.Millisecond, "⚡ Executing `git`... (1.2s)"},
		{"run_command", "docker-compose", 2500 * time.Millisecond, "⚡ Executing `docker-compose`... (2.5s)"},
		{"run_command", "verify.sh", 800 * time.Millisecond, "⚡ Executing `verify.sh`... (0.8s)"},
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

type statusUpdaterHarness struct {
	sends      atomic.Int32
	edits      atomic.Int32
	deletes    atomic.Int32
	lastText   atomic.Pointer[string]
	sendDone   chan string
	deleteDone chan string
	sendFn     func(channelID, text string) (string, error)
	editFn     func(channelID, messageID, text string) error
	deleteFn   func(channelID, messageID string) error
}

func newStatusUpdaterHarness() *statusUpdaterHarness {
	h := &statusUpdaterHarness{
		sendDone:   make(chan string, 8),
		deleteDone: make(chan string, 8),
	}
	h.sendFn = func(channelID, text string) (string, error) {
		h.sends.Add(1)
		h.lastText.Store(&text)
		select {
		case h.sendDone <- text:
		default:
		}
		return "msg-harness", nil
	}
	h.editFn = func(channelID, messageID, text string) error {
		h.edits.Add(1)
		h.lastText.Store(&text)
		return nil
	}
	h.deleteFn = func(channelID, messageID string) error {
		h.deletes.Add(1)
		select {
		case h.deleteDone <- messageID:
		default:
		}
		return nil
	}
	return h
}

func (h *statusUpdaterHarness) options(extra ...StatusUpdaterOption) []StatusUpdaterOption {
	opts := []StatusUpdaterOption{
		WithStatusMockFuncs(
			func(channelID, text string) (string, error) { return h.sendFn(channelID, text) },
			func(channelID, messageID, text string) error { return h.editFn(channelID, messageID, text) },
			func(channelID, messageID string) error { return h.deleteFn(channelID, messageID) },
		),
	}
	return append(opts, extra...)
}

func TestStatusUpdater_DebounceAndFastTurn(t *testing.T) {
	t.Parallel()
	h := newStatusUpdaterHarness()

	updater := NewStatusUpdater(nil, "thread-ticker", true,
		h.options(
			WithStatusDebounce(0),
			WithStatusInterval(2*time.Millisecond),
		)...,
	)
	updater.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})

	select {
	case <-h.sendDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for deterministic ticker send")
	}

	updater.Stop()
	updater.DeleteStatusMessage()

	select {
	case <-h.deleteDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for delete after ticker send")
	}

	if h.sends.Load() == 0 || h.deletes.Load() != 1 {
		t.Errorf("expected sends > 0 and 1 delete, got sends=%d deletes=%d", h.sends.Load(), h.deletes.Load())
	}

	hDebounce := newStatusUpdaterHarness()
	updaterDebounce := NewStatusUpdater(nil, "thread-debounce", true,
		hDebounce.options(
			WithStatusInterval(1*time.Hour),
			WithStatusDebounce(1*time.Hour),
		)...,
	)
	updaterDebounce.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	updaterDebounce.flush()
	updaterDebounce.Stop()
	updaterDebounce.DeleteStatusMessage()

	if hDebounce.sends.Load() != 0 || hDebounce.deletes.Load() != 0 {
		t.Errorf("expected 0 sends and 0 deletes during debounce, got sends=%d deletes=%d", hDebounce.sends.Load(), hDebounce.deletes.Load())
	}
}

func TestStatusUpdater_ToolLifecycleAndFlushes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		interval      time.Duration
		debounce      time.Duration
		latch         time.Duration
		steps         []*runner.StepUpdateEvent
		wantSendSub   string
		checkPostDone bool
		wantLatched   string
		simulateLatch bool
		wantUnlatched string
	}{
		{
			name:     "ActiveToolBypassesDebounceAndFlushes",
			interval: 1 * time.Hour,
			debounce: 100 * time.Millisecond,
			steps: []*runner.StepUpdateEvent{
				{StepType: "tool", ToolName: "view_file", State: "ACTIVE"},
			},
			wantSendSub:   "view_file",
			checkPostDone: true,
		},
		{
			name:     "RunCommandDisplaysCommandName",
			interval: 1 * time.Hour,
			debounce: 0,
			steps: []*runner.StepUpdateEvent{
				{
					StepType: "tool",
					ToolName: "run_command",
					State:    "ACTIVE",
					ToolInfo: &runner.StepToolInfo{
						Name:       "run_command",
						Parameters: runner.StepToolParameters{CommandLine: "git status"},
					},
				},
			},
			wantSendSub: "Executing `git`",
		},
		{
			name:     "FastToolLatchAndRecovery",
			interval: 1 * time.Hour,
			debounce: 0,
			latch:    500 * time.Millisecond,
			steps: []*runner.StepUpdateEvent{
				{StepType: "tool", ToolName: "view_file", State: "ACTIVE"},
				{StepType: "tool", ToolName: "view_file", State: "DONE"},
			},
			wantSendSub:   "view_file",
			wantLatched:   "view_file",
			simulateLatch: true,
			wantUnlatched: "Thinking",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newStatusUpdaterHarness()
			var opts []StatusUpdaterOption
			if tt.interval > 0 {
				opts = append(opts, WithStatusInterval(tt.interval))
			}
			opts = append(opts, WithStatusDebounce(tt.debounce))
			if tt.latch > 0 {
				opts = append(opts, WithStatusToolLatch(tt.latch))
			}

			updater := NewStatusUpdater(nil, "thread-"+tt.name, true, h.options(opts...)...)
			for _, step := range tt.steps {
				updater.HandleStep(step)
				updater.flush()
			}

			if h.sends.Load() != 1 {
				t.Fatalf("expected 1 send, got %d", h.sends.Load())
			}
			last := h.lastText.Load()
			if last == nil || !strings.Contains(*last, tt.wantSendSub) {
				t.Errorf("expected text to mention %q, got %v", tt.wantSendSub, last)
			}

			if tt.checkPostDone {
				updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "view_file", State: "DONE"})
				updater.HandleStep(&runner.StepUpdateEvent{StepType: "agent_response"})
				updater.flush()
				if h.edits.Load() == 0 {
					t.Errorf("expected at least 1 edit after phase change, got %d", h.edits.Load())
				}
			}

			if tt.wantLatched != "" {
				updater.mu.Lock()
				textLatched := updater.currentStatusText()
				updater.mu.Unlock()
				if !strings.Contains(textLatched, tt.wantLatched) {
					t.Errorf("expected latched text to contain %q, got: %s", tt.wantLatched, textLatched)
				}
			}

			if tt.simulateLatch {
				updater.mu.Lock()
				updater.lastCompletedAt = time.Now().Add(-600 * time.Millisecond)
				textExpired := updater.currentStatusText()
				updater.mu.Unlock()
				if !strings.Contains(textExpired, tt.wantUnlatched) {
					t.Errorf("expected expired text to contain %q, got: %s", tt.wantUnlatched, textExpired)
				}
			}

			updater.Stop()
			updater.DeleteStatusMessage()

			select {
			case <-h.deleteDone:
			case <-time.After(1 * time.Second):
				t.Fatal("timed out waiting for async delete")
			}
			if h.deletes.Load() != 1 {
				t.Errorf("expected 1 delete, got %d", h.deletes.Load())
			}
		})
	}
}

func TestStatusUpdater_PermanentCircuitBreaker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		errText string
	}{
		{name: "CircuitBreakerOn404", errText: "HTTP 404 Not Found, 10008 Unknown Message"},
		{name: "CircuitBreakerOn403", errText: "HTTP 403 Forbidden, 50013 Missing Permissions"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newStatusUpdaterHarness()
			h.editFn = func(channelID, messageID, text string) error {
				h.edits.Add(1)
				return errors.New(tc.errText)
			}

			updater := NewStatusUpdater(nil, "thread-"+tc.name, true, h.options(WithStatusInterval(1*time.Hour), WithStatusDebounce(0))...)
			updater.statusMessageID = "msg-exist"
			updater.phase = "thinking"
			updater.dirty = true
			updater.flush()

			for i := 0; i < 5; i++ {
				updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
				updater.flush()
			}
			updater.Stop()

			if h.edits.Load() != 1 {
				t.Errorf("expected circuit breaker to halt edits after 1 error, got %d edits", h.edits.Load())
			}
			if !updater.disabled {
				t.Errorf("expected updater.disabled=true")
			}
		})
	}
}

func TestStatusUpdater_SendPermissionCircuitBreaker(t *testing.T) {
	t.Parallel()
	h := newStatusUpdaterHarness()
	h.sendFn = func(channelID, text string) (string, error) {
		h.sends.Add(1)
		return "", errors.New("HTTP 403 Forbidden, 50013 Missing Permissions")
	}

	updater := NewStatusUpdater(nil, "thread-403", true, h.options(WithStatusInterval(1*time.Hour), WithStatusDebounce(0))...)
	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
	updater.flush()
	updater.flush()
	updater.Stop()

	if h.sends.Load() != 1 || !updater.disabled {
		t.Errorf("expected 1 send and disabled=true, got %d sends, disabled=%v", h.sends.Load(), updater.disabled)
	}
}

func TestStatusUpdater_TransientSendErrorSelfHeals(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	h := newStatusUpdaterHarness()
	h.sendFn = func(channelID, text string) (string, error) {
		if attempts.Add(1) == 1 {
			return "", errors.New("connection reset by peer")
		}
		return "msg-healed", nil
	}

	updater := NewStatusUpdater(nil, "thread-heals", true, h.options(WithStatusInterval(1*time.Hour), WithStatusDebounce(0))...)
	updater.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "heal_tool", State: "ACTIVE"})
	updater.flush()

	updater.mu.Lock()
	isDirty := updater.dirty
	msgID := updater.statusMessageID
	updater.mu.Unlock()

	if !isDirty || msgID != "" {
		t.Errorf("expected dirty=true and msgID='' after transient failure, got dirty=%v msgID=%s", isDirty, msgID)
	}

	updater.flush()

	updater.mu.Lock()
	healedID := updater.statusMessageID
	updater.mu.Unlock()

	if healedID != "msg-healed" {
		t.Errorf("expected statusMessageID to be msg-healed, got %s", healedID)
	}

	updater.Stop()
	updater.DeleteStatusMessage()
	select {
	case <-h.deleteDone:
	case <-time.After(1 * time.Second):
	}
}

func TestStatusUpdater_LifecycleAndConcurrency(t *testing.T) {
	t.Parallel()
	h := newStatusUpdaterHarness()

	// 1. Reset cleans up active message
	u := NewStatusUpdater(nil, "thread-reset", true, h.options(WithStatusInterval(1*time.Hour), WithStatusDebounce(0))...)
	u.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "flaky_tool", State: "ACTIVE"})
	u.flush()
	u.Reset()

	select {
	case <-h.deleteDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for reset deleteFunc")
	}
	u.Stop()

	// 2. Immediate flush on tool start
	hImm := newStatusUpdaterHarness()
	uImm := NewStatusUpdater(nil, "thread-imm", true, hImm.options(WithStatusInterval(1*time.Hour), WithStatusDebounce(1*time.Hour))...)
	uImm.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "grep_search", State: "RUNNING"})

	select {
	case text := <-hImm.sendDone:
		if !strings.Contains(text, "grep_search") {
			t.Errorf("expected grep_search, got %s", text)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for immediate flush")
	}
	uImm.Stop()
	uImm.DeleteStatusMessage()

	// 3. Zombie leak prevention
	hZombie := newStatusUpdaterHarness()
	sendStarted := make(chan struct{})
	sendRelease := make(chan struct{})
	hZombie.sendFn = func(ch, txt string) (string, error) {
		hZombie.sends.Add(1)
		close(sendStarted)
		<-sendRelease
		return "msg-slow", nil
	}

	uZombie := NewStatusUpdater(nil, "thread-zombie", true, hZombie.options(WithStatusInterval(1*time.Hour), WithStatusDebounce(0))...)
	uZombie.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "slow", State: "ACTIVE"})
	go uZombie.flush()

	<-sendStarted
	uZombie.DeleteStatusMessage()
	close(sendRelease)

	select {
	case msgID := <-hZombie.deleteDone:
		if msgID != "msg-slow" {
			t.Errorf("expected delete of msg-slow, got %s", msgID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for zombie delete")
	}
	uZombie.Stop()
}
