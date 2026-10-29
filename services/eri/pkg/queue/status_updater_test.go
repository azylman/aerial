package queue

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/runner"
)

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
	return func(u *StatusUpdater) { u.debounceDelay = debounce }
}

func TestFormatToolStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		toolName, commandName string
		elapsed               time.Duration
		expected              string
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
		if got := FormatToolStatus(tt.toolName, tt.commandName, tt.elapsed); got != tt.expected {
			t.Errorf("FormatToolStatus(%q, %q, %v) = %q, want %q", tt.toolName, tt.commandName, tt.elapsed, got, tt.expected)
		}
	}
}

type statusUpdaterHarness struct {
	sends, edits, deletes atomic.Int32
	lastText              atomic.Pointer[string]
	sendDone, deleteDone  chan string
	sendFn                func(channelID, text string) (string, error)
	editFn                func(channelID, messageID, text string) error
	deleteFn              func(channelID, messageID string) error
}

func newStatusUpdaterHarness() *statusUpdaterHarness {
	h := &statusUpdaterHarness{sendDone: make(chan string, 8), deleteDone: make(chan string, 8)}
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
	return append([]StatusUpdaterOption{func(u *StatusUpdater) {
		u.sendFunc = func(c, t string) (string, error) { return h.sendFn(c, t) }
		u.editFunc = func(c, m, t string) error { return h.editFn(c, m, t) }
		u.deleteFunc = func(c, m string) error { return h.deleteFn(c, m) }
	}}, extra...)
}

func TestStatusUpdater_DebounceAndFastTurn(t *testing.T) {
	t.Parallel()
	h := newStatusUpdaterHarness()
	u := NewStatusUpdater(nil, "thread-ticker", true, h.options(WithStatusDebounce(0), WithStatusInterval(2*time.Millisecond))...)
	u.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})

	select {
	case <-h.sendDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ticker send")
	}
	u.Stop()
	u.DeleteStatusMessage()

	select {
	case <-h.deleteDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete")
	}
	if h.sends.Load() == 0 || h.deletes.Load() != 1 {
		t.Errorf("expected sends > 0 and 1 delete, got sends=%d deletes=%d", h.sends.Load(), h.deletes.Load())
	}

	hDebounce := newStatusUpdaterHarness()
	uDebounce := NewStatusUpdater(nil, "thread-debounce", true, hDebounce.options(WithStatusInterval(time.Hour), WithStatusDebounce(time.Hour))...)
	uDebounce.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	uDebounce.flush()
	uDebounce.Stop()
	uDebounce.DeleteStatusMessage()
	if hDebounce.sends.Load() != 0 || hDebounce.deletes.Load() != 0 {
		t.Errorf("expected 0 sends/deletes, got sends=%d deletes=%d", hDebounce.sends.Load(), hDebounce.deletes.Load())
	}

	hSync := newStatusUpdaterHarness()
	uSync := NewStatusUpdater(nil, "thread-sync", true, hSync.options(WithSyncDelete(), WithStatusDebounce(0), WithStatusInterval(2*time.Millisecond))...)
	uSync.HandleStep(&runner.StepUpdateEvent{StepType: "thinking"})
	select {
	case <-hSync.sendDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sync ticker send")
	}
	uSync.Stop()
	uSync.DeleteStatusMessage()
	if hSync.deletes.Load() != 1 {
		t.Fatalf("expected synchronous delete count 1, got %d", hSync.deletes.Load())
	}

	chunkFlushed := make(chan string, 4)
	chunkDeleted := make(chan struct{}, 1)
	flushFn := func(step string) error {
		if step == "" {
			select {
			case chunkDeleted <- struct{}{}:
			default:
			}
		} else {
			select {
			case chunkFlushed <- step:
			default:
			}
		}
		return nil
	}
	uChunk := NewChunkStepStatusUpdater("chunk-test-1", flushFn, WithStatusDebounce(0), WithStatusInterval(2*time.Millisecond))
	uChunk.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool", ToolName: "run_command", State: "ACTIVE",
		ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "go test ./..."}},
	})
	select {
	case step := <-chunkFlushed:
		if !strings.Contains(step, "Executing `go`") {
			t.Fatalf("expected active step to contain 'Executing `go`', got %q", step)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for chunk step flush")
	}
	uChunk.Stop()
	uChunk.DeleteStatusMessage()
	select {
	case <-chunkDeleted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for chunk step delete")
	}

	uNil := NewChunkStepStatusUpdater("chunk-nil-fn", nil, WithStatusDebounce(0), WithStatusInterval(time.Hour))
	uNil.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool", ToolName: "read_file", State: "ACTIVE",
		ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "read"}},
	})
	uNil.flush()
	uNil.Stop()
	uNil.DeleteStatusMessage()

	errFlush := errors.New("flush failed")
	uErr := NewChunkStepStatusUpdater("chunk-err-fn", func(string) error { return errFlush }, WithStatusDebounce(0), WithStatusInterval(time.Hour))
	uErr.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool", ToolName: "read_file", State: "ACTIVE",
		ToolInfo: &runner.StepToolInfo{Parameters: runner.StepToolParameters{CommandLine: "read"}},
	})
	uErr.flush()
	uErr.Stop()
	uErr.DeleteStatusMessage()
}

func TestStatusUpdater_ToolLifecycleAndFlushes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                       string
		interval, debounce, latch  time.Duration
		steps                      []*runner.StepUpdateEvent
		wantSendSub                string
		checkPostDone              bool
		wantLatched, wantUnlatched string
		simulateLatch              bool
	}{
		{
			name: "ActiveToolBypassesDebounceAndFlushes", interval: time.Hour, debounce: 100 * time.Millisecond,
			steps:       []*runner.StepUpdateEvent{{StepType: "tool", ToolName: "view_file", State: "ACTIVE"}},
			wantSendSub: "view_file", checkPostDone: true,
		},
		{
			name: "RunCommandDisplaysCommandName", interval: time.Hour,
			steps:       []*runner.StepUpdateEvent{{StepType: "tool", ToolName: "run_command", State: "ACTIVE", ToolInfo: &runner.StepToolInfo{Name: "run_command", Parameters: runner.StepToolParameters{CommandLine: "git status"}}}},
			wantSendSub: "Executing `git`",
		},
		{
			name: "FastToolLatchAndRecovery", interval: time.Hour, latch: 500 * time.Millisecond,
			steps:       []*runner.StepUpdateEvent{{StepType: "tool", ToolName: "view_file", State: "ACTIVE"}, {StepType: "tool", ToolName: "view_file", State: "DONE"}},
			wantSendSub: "view_file", wantLatched: "view_file", simulateLatch: true, wantUnlatched: "Thinking",
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
				opts = append(opts, func(u *StatusUpdater) { u.toolLatchDuration = tt.latch })
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
					t.Errorf("expected at least 1 edit, got %d", h.edits.Load())
				}
			}
			if tt.wantLatched != "" {
				updater.mu.Lock()
				latched := updater.currentStatusText()
				updater.mu.Unlock()
				if !strings.Contains(latched, tt.wantLatched) {
					t.Errorf("expected latched %q, got: %s", tt.wantLatched, latched)
				}
			}
			if tt.simulateLatch {
				updater.mu.Lock()
				updater.lastCompletedAt = time.Now().Add(-600 * time.Millisecond)
				expired := updater.currentStatusText()
				updater.mu.Unlock()
				if !strings.Contains(expired, tt.wantUnlatched) {
					t.Errorf("expected expired %q, got: %s", tt.wantUnlatched, expired)
				}
			}
			updater.Stop()
			updater.DeleteStatusMessage()
			select {
			case <-h.deleteDone:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for delete")
			}
			if h.deletes.Load() != 1 {
				t.Errorf("expected 1 delete, got %d", h.deletes.Load())
			}
		})
	}
}

func TestStatusUpdater_CircuitBreakers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		isSend    bool
		errText   string
		wantCalls int32
	}{
		{"Edit404", false, "HTTP 404 Not Found, 10008 Unknown Message", 1},
		{"Edit403", false, "HTTP 403 Forbidden, 50013 Missing Permissions", 1},
		{"Send403", true, "HTTP 403 Forbidden, 50013 Missing Permissions", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newStatusUpdaterHarness()
			if tc.isSend {
				h.sendFn = func(_, _ string) (string, error) {
					h.sends.Add(1)
					return "", errors.New(tc.errText)
				}
			} else {
				h.editFn = func(_, _, _ string) error {
					h.edits.Add(1)
					return errors.New(tc.errText)
				}
			}
			u := NewStatusUpdater(nil, "thread-"+tc.name, true, h.options(WithStatusInterval(time.Hour), WithStatusDebounce(0))...)
			if !tc.isSend {
				u.statusMessageID, u.phase, u.dirty = "msg-exist", "thinking", true
				u.flush()
			}
			for i := 0; i < 3; i++ {
				u.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "test_tool", State: "ACTIVE"})
				u.flush()
			}
			u.Stop()
			gotCalls := h.edits.Load()
			if tc.isSend {
				gotCalls = h.sends.Load()
			}
			if gotCalls != tc.wantCalls || !u.disabled {
				t.Errorf("got calls=%d (want %d), disabled=%v (want true)", gotCalls, tc.wantCalls, u.disabled)
			}
		})
	}
}

func TestStatusUpdater_TransientSendErrorSelfHeals(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	h := newStatusUpdaterHarness()
	h.sendFn = func(_, _ string) (string, error) {
		if attempts.Add(1) == 1 {
			return "", errors.New("connection reset")
		}
		return "msg-healed", nil
	}
	u := NewStatusUpdater(nil, "thread-heals", true, h.options(WithStatusInterval(time.Hour), WithStatusDebounce(0))...)
	u.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "heal_tool", State: "ACTIVE"})
	u.flush()

	u.mu.Lock()
	dirty, mid := u.dirty, u.statusMessageID
	u.mu.Unlock()
	if !dirty || mid != "" {
		t.Errorf("expected dirty=true and empty msgID, got dirty=%v msgID=%s", dirty, mid)
	}

	u.flush()
	u.mu.Lock()
	healedID := u.statusMessageID
	u.mu.Unlock()
	if healedID != "msg-healed" {
		t.Errorf("expected msg-healed, got %s", healedID)
	}

	u.Stop()
	u.DeleteStatusMessage()
	select {
	case <-h.deleteDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete")
	}
}

func TestStatusUpdater_LifecycleAndConcurrency(t *testing.T) {
	t.Parallel()
	// 1. Reset cleans up active message
	hReset := newStatusUpdaterHarness()
	u := NewStatusUpdater(nil, "thread-reset", true, hReset.options(WithStatusInterval(time.Hour), WithStatusDebounce(0))...)
	u.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "flaky_tool", State: "ACTIVE"})
	u.flush()
	u.Reset()
	select {
	case <-hReset.deleteDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reset delete")
	}
	u.Stop()

	// 2. Immediate flush on tool start
	hImm := newStatusUpdaterHarness()
	uImm := NewStatusUpdater(nil, "thread-imm", true, hImm.options(WithStatusInterval(time.Hour), WithStatusDebounce(time.Hour))...)
	uImm.HandleStep(&runner.StepUpdateEvent{StepType: "tool_call", ToolName: "grep_search", State: "RUNNING"})
	select {
	case text := <-hImm.sendDone:
		if !strings.Contains(text, "grep_search") {
			t.Errorf("expected grep_search, got %s", text)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for immediate flush")
	}
	uImm.Stop()
	uImm.DeleteStatusMessage()

	// 3. Zombie leak prevention
	hZombie, start, rel := newStatusUpdaterHarness(), make(chan struct{}), make(chan struct{})
	hZombie.sendFn = func(ch, txt string) (string, error) {
		hZombie.sends.Add(1)
		close(start)
		<-rel
		return "msg-slow", nil
	}
	uZombie := NewStatusUpdater(nil, "thread-zombie", true, hZombie.options(WithStatusInterval(time.Hour), WithStatusDebounce(0))...)
	uZombie.HandleStep(&runner.StepUpdateEvent{StepType: "tool", ToolName: "slow", State: "ACTIVE"})
	go uZombie.flush()
	<-start
	uZombie.DeleteStatusMessage()
	close(rel)
	select {
	case msgID := <-hZombie.deleteDone:
		if msgID != "msg-slow" {
			t.Errorf("expected delete of msg-slow, got %s", msgID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for zombie delete")
	}
	uZombie.Stop()
}
