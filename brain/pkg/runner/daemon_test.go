package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDaemon_LifecycleAndTurnExecution(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	script := `#!/bin/sh
echo '{"event":"init","init":{"tools":["run_command"]}}'
while IFS= read -r line; do
  case "$line" in
    *notask*)
      echo '{"event":"step_update","step_update":{"step_index":1,"state":"DONE","step_type":"agent_response"}}'
      echo '{"event":"result","content":"Turn execution complete","usage":{"input_tokens":50,"output_tokens":25,"total_tokens":75}}'
      ;;
    *)
      echo '{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"sleep 20"}}}}'
      echo '{"event":"step_update","step_update":{"step_index":2,"state":"DONE","step_type":"tool","tool_name":"run_command","tool_output":"Tool is running as a background task with task id: test-task-42"}}'
      echo '{"event":"step_update","step_update":{"step_index":3,"state":"DONE","step_type":"agent_response"}}'
      echo '{"event":"result","content":"Turn execution complete","usage":{"input_tokens":100,"output_tokens":50,"total_tokens":150}}'
      ;;
  esac
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock agy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := DaemonConfig{
		SessionID: "sess-1234",
		ThreadID:  "thread-5678",
		AgyBin:    mockBin,
		Cwd:       tempDir,
		Timeout:   5 * time.Second,
	}

	daemon, err := StartDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer daemon.Close()

	if daemon.State() != StateReady {
		t.Errorf("expected state READY, got %s", daemon.State())
	}

	// Execute Turn 1
	res, err := daemon.ExecuteTurn(ctx, "Hello world turn 1")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if res.Content != "Turn execution complete" {
		t.Errorf("expected content 'Turn execution complete', got %q", res.Content)
	}
	if res.Usage.TotalTokens != 150 {
		t.Errorf("expected 150 total tokens, got %d", res.Usage.TotalTokens)
	}
	if !daemon.TaskTracker().Has("test-task-42") {
		t.Errorf("expected TaskTracker to track test-task-42 from step_update")
	}
	if !res.IsYieldTrap {
		t.Errorf("expected IsYieldTrap=true because test-task-42 was active")
	}

	// Execute Turn 2 (verifying persistent stream is still alive and per-turn buffer was reset)
	res2, err := daemon.ExecuteTurn(ctx, "Hello world turn 2")
	if err != nil {
		t.Fatalf("ExecuteTurn 2 failed: %v", err)
	}
	if res2.Content != "Turn execution complete" {
		t.Errorf("expected content 'Turn execution complete', got %q", res2.Content)
	}

	// Verify state while yield-waiting
	if daemon.State() != StateYieldWaiting {
		t.Errorf("expected state YIELD_WAITING while task is active, got %s", daemon.State())
	}

	// Remove active task and verify next turn transitions back to READY
	daemon.TaskTracker().Remove("test-task-42")
	res3, err := daemon.ExecuteTurn(ctx, "Hello world turn 3 notask")
	if err != nil {
		t.Fatalf("ExecuteTurn 3 failed: %v", err)
	}
	if daemon.State() != StateReady {
		t.Errorf("expected state READY when no tasks active, got %s", daemon.State())
	}
	_ = res3

	// Dirty flag tests
	if daemon.IsDirty() {
		t.Errorf("expected daemon initially not dirty")
	}
	daemon.SetDirty(true)
	if !daemon.IsDirty() {
		t.Errorf("expected daemon dirty after SetDirty(true)")
	}
	_, err = daemon.ExecuteTurn(ctx, "Hello world turn 4 dirty")
	if err != nil {
		t.Fatalf("ExecuteTurn 4 failed: %v", err)
	}
	if daemon.State() != StateClosed {
		t.Errorf("expected daemon to transition to StateClosed after turn on dirty daemon, got %s", daemon.State())
	}

	// Verify SessionID and LastUsed
	if daemon.SessionID() != "sess-1234" {
		t.Errorf("expected sessionID 'sess-1234', got %q", daemon.SessionID())
	}
	if daemon.LastUsed().IsZero() {
		t.Errorf("expected LastUsed to be non-zero")
	}

	// Close cleanly
	if err := daemon.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if daemon.State() != StateClosed {
		t.Errorf("expected state CLOSED, got %s", daemon.State())
	}
	// Close idempotency
	if err := daemon.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}

	// Execute on closed daemon should return error
	_, err = daemon.ExecuteTurn(ctx, "After closed")
	if err == nil {
		t.Errorf("expected error executing turn on closed daemon")
	}
}

func TestDaemon_StartAndStreamEdgeCases(t *testing.T) {
	// Start failure with nonexistent binary
	ctx := context.Background()
	_, err := StartDaemon(ctx, DaemonConfig{
		AgyBin: "/nonexistent/binary/path/agy",
	})
	if err == nil {
		t.Errorf("expected error starting daemon with nonexistent binary")
	}

	// Start with empty AgyBin (defaults to "agy")
	_, _ = StartDaemon(ctx, DaemonConfig{
		AgyBin: "",
	})

	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_stream_edge.sh")
	script := `#!/bin/sh
# Wait for input line first
read -r line
# Output blank lines, non-json lines, subagent start, task finished, and usage tokens
echo ""
echo "not valid json"
echo '{"event":"step_update","step_update":{"tool_name":"invoke_subagent","tool_output":"Subagent conversation started with conversation ID: subagent-777, {\"conversationId\": \"subagent-777\"}"}}'
echo '{"event":"step_update","step_update":{"tool_output":"Task id \"subagent-777\" finished with result:"}}'
echo '{"event":"result","content":"Done edge","usage":{"input_tokens":10,"output_tokens":20,"thinking_tokens":5,"cache_read_tokens":15,"total_tokens":50}}'
exit 0
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:        mockBin,
		GeminiHomeDir: tempDir,
		Model:         "gemini-2.5-flash",
		SessionID:     "edge-sess",
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	res, err := d.ExecuteTurn(ctx, "Run edge test")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if res.Content != "Done edge" {
		t.Errorf("expected content 'Done edge', got %q", res.Content)
	}
	if res.Usage.ThinkingTokens != 5 || res.Usage.CacheReadTokens != 15 {
		t.Errorf("expected thinking=5 and cache_read=15, got %d and %d", res.Usage.ThinkingTokens, res.Usage.CacheReadTokens)
	}

	// Now that mock script has exited (exit 0), subsequent turn should hit EOF
	_, err = d.ExecuteTurn(ctx, "Next turn should hit EOF")
	if err == nil {
		t.Errorf("expected EOF error on exited process")
	}

	// Test close with nil cmd
	nilCmdDaemon := &Daemon{state: StateReady}
	if err := nilCmdDaemon.Close(); err != nil {
		t.Errorf("expected nil error closing daemon with nil cmd, got %v", err)
	}
}

func TestDaemon_TaskSenderAndCancellation(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_sender.sh")
	script := `#!/bin/sh
read -r line
echo '{"event":"step_update","step_update":{"tool_output":"[Message] sender=task-sender-42 priority=HIGH content=done"}}'
echo '{"event":"step_update","step_update":{"tool_output":"[Message] sender=task-sender-suffix priority=HIGH content=done"}}'
echo '{"event":"step_update","step_update":{"tool_output":"Task id \"task-fin-99\" finished with result: ok"}}'
echo '{"event":"step_update","step_update":{"tool_output":"Task id \"prefix/task-fin-suffix\" finished with result: ok"}}'
echo '{"event":"result","content":"Task sender done","usage":{"total_tokens":20}}'
exit 0
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock sender: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin: mockBin,
		Cwd:    tempDir,
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	d.TaskTracker().Add(TaskMetadata{
		TaskID:    "task-sender-42",
		StartedAt: time.Now(),
	})
	d.TaskTracker().Add(TaskMetadata{
		TaskID:    "prefix/task-sender-suffix",
		StartedAt: time.Now(),
	})
	d.TaskTracker().Add(TaskMetadata{
		TaskID:    "task-fin-99",
		StartedAt: time.Now(),
	})
	d.TaskTracker().Add(TaskMetadata{
		TaskID:    "task-fin-suffix",
		StartedAt: time.Now(),
	})

	res, err := d.ExecuteTurn(ctx, "Run sender test")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if res.Content != "Task sender done" {
		t.Errorf("expected 'Task sender done', got %q", res.Content)
	}
	if res.Duration < 0 {
		t.Errorf("expected non-negative duration, got %v", res.Duration)
	}
	if d.TaskTracker().Has("task-sender-42") {
		t.Errorf("expected task-sender-42 to be removed via reTaskMessageSender")
	}
	if d.TaskTracker().Has("prefix/task-sender-suffix") {
		t.Errorf("expected prefix/task-sender-suffix to be removed via reTaskMessageSender suffix match")
	}
	if d.TaskTracker().Has("task-fin-99") {
		t.Errorf("expected task-fin-99 to be removed via reTaskFinishedContent")
	}
	if d.TaskTracker().Has("task-fin-suffix") {
		t.Errorf("expected task-fin-suffix to be removed via reTaskFinishedContent suffix match")
	}
}

func TestDaemon_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_sleep.sh")
	script := `#!/bin/sh
read -r line
sleep 10
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin: mockBin,
		Cwd:    tempDir,
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err = d.ExecuteTurn(ctx, "Will be cancelled")
	if err == nil {
		t.Errorf("expected error executing turn with cancelled context")
	}
}

func TestDaemon_SIGKILLFallback(t *testing.T) {
	origGrace := closeGracePeriod
	closeGracePeriod = 50 * time.Millisecond
	defer func() { closeGracePeriod = origGrace }()

	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_trap.sh")
	script := `#!/bin/sh
trap '' TERM
while true; do
  sleep 1
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin: mockBin,
		Cwd:    tempDir,
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}

	// Close should trigger SIGTERM, time out after 50ms, and send SIGKILL
	if err := d.Close(); err != nil {
		t.Errorf("expected clean exit on SIGKILL fallback, got: %v", err)
	}
	if d.State() != StateClosed {
		t.Errorf("expected state CLOSED, got %s", d.State())
	}
}

func TestDaemon_ExecuteClosedAndDoubleClose(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_simple.sh")
	script := `#!/bin/sh
read -r line
echo '{"event":"result","content":"ok","usage":{}}'
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin: mockBin,
		Cwd:    tempDir,
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	// Double close should succeed immediately
	if err := d.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	// Executing on closed daemon returns error
	_, err = d.ExecuteTurn(ctx, "hello")
	if err == nil {
		t.Errorf("expected error executing turn on closed daemon")
	}

	// Nil cmd RSSBytes returns 0
	nilDaemon := &Daemon{}
	if rss := nilDaemon.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 RSS for nil cmd, got %d", rss)
	}
}

func TestDaemon_ExecuteTurnWithHandler(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_stream.sh")
	script := `#!/bin/sh
read -r line
echo '{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"tool","tool_name":"test_tool"}}'
echo '{"event":"result","content":"all done","usage":{"total_tokens":42}}'
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:   mockBin,
		Cwd:      tempDir,
		ThreadID: "test-thread-123",
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	// Verify RSSBytes works on live process
	_ = d.RSSBytes()

	var receivedStep *StepUpdateEvent
	handler := func(ev *StepUpdateEvent) {
		receivedStep = ev
	}

	res, err := d.ExecuteTurnWithHandler(ctx, "hello with handler", handler)
	if err != nil {
		t.Fatalf("ExecuteTurnWithHandler failed: %v", err)
	}
	if res.Content != "all done" {
		t.Errorf("expected 'all done', got %q", res.Content)
	}
	if receivedStep == nil {
		t.Fatalf("expected step update event to be received by handler")
	}
	if receivedStep.ResolvedToolName() != "test_tool" {
		t.Errorf("expected tool_name 'test_tool', got %q", receivedStep.ResolvedToolName())
	}
}

func TestDaemon_StartFailures(t *testing.T) {
	ctx := context.Background()
	_, err := StartDaemon(ctx, DaemonConfig{
		AgyBin: "/path/to/nonexistent/binary/404",
	})
	if err == nil {
		t.Errorf("expected StartDaemon to fail for nonexistent binary")
	}
}

func TestDaemon_CoverageBoost(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_env.sh")
	script := `#!/bin/sh
read -r line
echo '{"event":"result","content":"env ok","usage":{}}'
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:        mockBin,
		Cwd:           tempDir,
		Model:         "gemini-test",
		GeminiHomeDir: "/tmp/gemini-home",
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	// Test stdin write error by closing stdin manually
	_ = d.stdin.Close()
	_, err = d.ExecuteTurn(ctx, "hello")
	if err == nil {
		t.Errorf("expected error executing turn on closed stdin")
	}

	// Close on nil stdin/cmd
	emptyD := &Daemon{}
	if err := emptyD.Close(); err != nil {
		t.Errorf("expected clean Close on empty daemon, got: %v", err)
	}
}

func TestDaemon_InvokeSubagentAndEOF(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_subagent.sh")
	script := `#!/bin/sh
read -r line
echo ''
echo '{"event":"step_update","step_update":{"tool_name":"invoke_subagent","tool_output":"Launched subagent {\"conversationId\": \"sub-999\"}"}}'
echo '{"event":"result","content":"subagent launched","usage":{}}'
# Now terminate to trigger EOF on next read
exit 0
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:   mockBin,
		Cwd:      tempDir,
		ThreadID: "thread-sub",
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	res, err := d.ExecuteTurn(ctx, "run subagent")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if !d.TaskTracker().Has("sub-999") {
		t.Errorf("expected sub-999 to be tracked")
	}
	if !res.IsYieldTrap {
		t.Errorf("expected IsYieldTrap=true")
	}

	// Next turn encounters EOF because mock script exited
	_, err = d.ExecuteTurn(ctx, "turn after exit")
	if err == nil {
		t.Errorf("expected EOF error on closed stream")
	}
}
