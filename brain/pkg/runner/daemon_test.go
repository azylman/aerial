package runner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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
      echo '{"event":"result","result":{"status":"SUCCESS","response":"Turn execution complete","usage":{"input_tokens":50,"output_tokens":25,"total_tokens":75}}}'
      ;;
    *)
      echo '{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"parameters":{"CommandLine":"sleep 20"}}}}'
      echo '{"event":"step_update","step_update":{"step_index":2,"state":"DONE","step_type":"tool","tool_name":"run_command","tool_output":"Tool is running as a background task with task id: test-task-42"}}'
      echo '{"event":"step_update","step_update":{"step_index":3,"state":"DONE","step_type":"agent_response"}}'
      echo '{"event":"result","result":{"status":"SUCCESS","response":"Turn execution complete","usage":{"input_tokens":100,"output_tokens":50,"total_tokens":150}}}'
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
	if res.Response != "Turn execution complete" {
		t.Errorf("expected response 'Turn execution complete', got %q", res.Response)
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
	if res2.Response != "Turn execution complete" {
		t.Errorf("expected response 'Turn execution complete', got %q", res2.Response)
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
echo '{"event":"result","result":{"status":"SUCCESS","response":"Done edge","usage":{"input_tokens":10,"output_tokens":20,"thinking_tokens":5,"cache_read_tokens":15,"total_tokens":50}}}'
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
	if res.Response != "Done edge" {
		t.Errorf("expected response 'Done edge', got %q", res.Response)
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
echo '{"event":"result","result":{"status":"SUCCESS","response":"Task sender done","usage":{"total_tokens":20}}}'
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
	if res.Response != "Task sender done" {
		t.Errorf("expected 'Task sender done', got %q", res.Response)
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
echo '{"event":"result","result":{"status":"SUCCESS","response":"ok","usage":{}}}'
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
echo '{"event":"result","result":{"status":"SUCCESS","response":"all done","usage":{"total_tokens":42}}}'
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
	if res.Response != "all done" {
		t.Errorf("expected 'all done', got %q", res.Response)
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
echo '{"event":"result","result":{"status":"SUCCESS","response":"env ok","usage":{}}}'
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
echo '{"event":"result","result":{"status":"SUCCESS","response":"subagent launched","usage":{}}}'
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

func TestDaemon_RSSBytesDetailed(t *testing.T) {
	t.Parallel()
	dNil := &Daemon{}
	if rss := dNil.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 for nil daemon, got %d", rss)
	}

	dNeg := &Daemon{
		cmd: &exec.Cmd{Process: &os.Process{Pid: -1}},
	}
	if rss := dNeg.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 for negative pid, got %d", rss)
	}

	dNon := &Daemon{
		cmd: &exec.Cmd{Process: &os.Process{Pid: 999999999}},
	}
	if rss := dNon.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 for nonexistent pid, got %d", rss)
	}

	if runtime.GOOS != "windows" && filepath.Separator != '\\' {
		dLive := &Daemon{
			cmd: &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
		}
		if rss := dLive.RSSBytes(); rss == 0 {
			t.Errorf("expected non-zero RSS for live process on Linux, got %d", rss)
		}
	}
}

func TestDaemon_ParseRawUsageDetailed(t *testing.T) {
	t.Parallel()
	rawAll := map[string]interface{}{
		"input_tokens":      float64(10),
		"output_tokens":     float64(20),
		"thinking_tokens":   float64(5),
		"cache_read_tokens": float64(2),
		"total_tokens":      float64(37),
	}
	uAll := parseRawUsage(rawAll)
	if uAll.InputTokens != 10 || uAll.OutputTokens != 20 || uAll.ThinkingTokens != 5 || uAll.CacheReadTokens != 2 || uAll.TotalTokens != 37 {
		t.Errorf("unexpected usage: %+v", uAll)
	}

	rawNoTotal := map[string]interface{}{
		"input_tokens":  float64(10),
		"output_tokens": float64(20),
	}
	uNoTotal := parseRawUsage(rawNoTotal)
	if uNoTotal.TotalTokens != 30 {
		t.Errorf("expected total tokens 30, got %d", uNoTotal.TotalTokens)
	}
}

func TestDaemon_ExtractSubagentIDDetailed(t *testing.T) {
	t.Parallel()
	if id := extractSubagentID(""); id != "" {
		t.Errorf("expected empty string for empty input, got %q", id)
	}

	// Payload with subagents list and snake_case conversation_id
	payloadNested := `{"subagents":[{"conversation_id":"sub-agent-123"}]}`
	if id := extractSubagentID(payloadNested); id != "sub-agent-123" {
		t.Errorf("expected 'sub-agent-123', got %q", id)
	}

	// Embedded list with prefix
	embeddedList := `Prefix chatter before json: [{"conversationId":"sub-embedded-456"}]`
	if id := extractSubagentID(embeddedList); id != "sub-embedded-456" {
		t.Errorf("expected 'sub-embedded-456', got %q", id)
	}
}

func TestParseAgyOutputDetailed(t *testing.T) {
	t.Parallel()
	// Flat result event
	flat := `{"event":"init","session_id":"11111111-2222-3333-4444-555555555555"}
{"event":"result","status":"SUCCESS","response":"flat model response"}
`
	res, err := ParseAgyOutput(flat)
	if err != nil || res.Response != "flat model response" || res.ConversationID != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("unexpected parse result for flat output: (%+v, %v)", res, err)
	}

	// Missing result event but valid legacy JSON fallback
	legacyFallback := `{"status":"SUCCESS","response":"legacy response","conversation_id":"sess-legacy"}`
	res, err = ParseAgyOutput(legacyFallback)
	if err != nil || res.Response != "legacy response" {
		t.Errorf("unexpected parse result for legacy fallback: (%+v, %v)", res, err)
	}
}

func TestDaemon_TurnStreamEdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_edge.sh")
	script := `#!/bin/sh
read -r line
# 1. Empty line
echo ''
# 2. Non-event json line
echo '{"unrecognized":"field"}'
# 3. Flat step_update (without step_update wrapper)
echo '{"event":"step_update","type":"tool","tool_name":"flat_tool","tool_output":"Tool is running as a background task with task id: bg-1"}'
# 4. Message from sender task (with ThreadID set on daemon)
echo '{"event":"step_update","step_update":{"tool_name":"task_sender","tool_output":"sender=bg-1"}}'
# 5. Add another task and finish it
echo '{"event":"step_update","step_update":{"tool_name":"run_command","tool_output":"Tool is running as a background task with task id: bg-2"}}'
echo '{"event":"step_update","step_update":{"tool_name":"finisher","tool_output":"Task id \"bg-2\" finished with result: done"}}'

# 6. Final result
echo '{"event":"result","result":{"status":"SUCCESS","response":"finished all edge cases","usage":{"total_tokens":10}}}'
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()
	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:        mockBin,
		Cwd:           tempDir,
		ThreadID:      "thread-edge-cases",
		SessionID:     "11111111-2222-3333-4444-555555555555",
		Model:         "gemini-2.5-flash",
		GeminiHomeDir: tempDir,
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	var flatReceived bool
	res, err := d.ExecuteTurnWithHandler(ctx, "run edge cases", func(ev *StepUpdateEvent) {
		if ev.ResolvedToolName() == "flat_tool" {
			flatReceived = true
		}
	})
	if err != nil {
		t.Fatalf("ExecuteTurnWithHandler failed: %v", err)
	}
	if !flatReceived {
		t.Errorf("expected flat_tool step update to be received by handler")
	}
	if res.Response != "finished all edge cases" {
		t.Errorf("unexpected response: %q", res.Response)
	}
	if d.TaskTracker().ActiveCount() != 0 {
		t.Errorf("expected 0 active tasks after cleanup, got %d", d.TaskTracker().ActiveCount())
	}
}

func TestDaemon_ColdStartSessionIDLatching_InitEvent(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	validUUID := "22222222-3333-4444-5555-666666666666"
	script := fmt.Sprintf(`#!/bin/sh
echo '{"event":"init","conversation_id":%q}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"Cold start turn done","usage":{"total_tokens":42}}}'
done
`, validUUID)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:   mockBin,
		Cwd:      tempDir,
		ThreadID: "thread-cold-start",
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	if d.SessionID() != "" {
		t.Errorf("expected initially empty SessionID before turn, got %q", d.SessionID())
	}

	res, err := d.ExecuteTurn(ctx, "cold start test prompt")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if res.ConversationID != validUUID {
		t.Errorf("expected res.ConversationID=%q, got %q", validUUID, res.ConversationID)
	}
	if d.SessionID() != validUUID {
		t.Errorf("expected d.SessionID()=%q, got %q", validUUID, d.SessionID())
	}
}

func TestDaemon_ColdStartSessionIDLatching_ResultEvent(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	validUUID := "33333333-4444-5555-6666-777777777777"
	script := fmt.Sprintf(`#!/bin/sh
while IFS= read -r line; do
  echo '{"event":"result","result":{"conversation_id":%q,"status":"SUCCESS","response":"Result session latched","usage":{"total_tokens":10}}}'
done
`, validUUID)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d, err := StartDaemon(ctx, DaemonConfig{
		AgyBin:   mockBin,
		Cwd:      tempDir,
		ThreadID: "thread-cold-result",
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer d.Close()

	res, err := d.ExecuteTurn(ctx, "test prompt")
	if err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}
	if res.ConversationID != validUUID {
		t.Errorf("expected res.ConversationID=%q, got %q", validUUID, res.ConversationID)
	}
	if d.SessionID() != validUUID {
		t.Errorf("expected d.SessionID()=%q, got %q", validUUID, d.SessionID())
	}
}

func TestDaemon_SessionID_FallbackStderrAndSetter(t *testing.T) {
	d := &Daemon{
		stderrBuf: NewActivityWriter(""),
	}
	if d.SessionID() != "" {
		t.Errorf("expected empty SessionID, got %q", d.SessionID())
	}

	validUUID := "44444444-5555-6666-7777-888888888888"
	d.stderrBuf.SetSessionID(validUUID)
	if d.SessionID() != validUUID {
		t.Errorf("expected SessionID from stderrBuf=%q, got %q", validUUID, d.SessionID())
	}

	manualUUID := "55555555-6666-7777-8888-999999999999"
	d.SetSessionID(manualUUID)
	if d.SessionID() != manualUUID {
		t.Errorf("expected SessionID=%q after SetSessionID, got %q", manualUUID, d.SessionID())
	}
}

func TestDaemon_ResultEventVariations(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_variations.sh")
	script := `#!/bin/sh
# Turn 1: flat response
read -r line
echo '{"event":"result","response":"flat text","status":"SUCCESS","usage":{"total_tokens":55}}'

# Turn 2: raw string in result field
read -r line
echo '{"event":"result","result":"raw string text","usage":{"input_tokens":10,"output_tokens":20}}'

# Turn 3: invalid structure in result field triggering error
read -r line
echo '{"event":"result","result":12345}'
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
	defer d.Close()

	// Turn 1
	res1, err := d.ExecuteTurn(ctx, "turn 1")
	if err != nil {
		t.Fatalf("Turn 1 failed: %v", err)
	}
	if res1.Response != "flat text" {
		t.Errorf("expected 'flat text', got %q", res1.Response)
	}
	if res1.Usage.TotalTokens != 55 {
		t.Errorf("expected 55 total tokens, got %d", res1.Usage.TotalTokens)
	}

	// Turn 2
	res2, err := d.ExecuteTurn(ctx, "turn 2")
	if err != nil {
		t.Fatalf("Turn 2 failed: %v", err)
	}
	if res2.Response != "raw string text" {
		t.Errorf("expected 'raw string text', got %q", res2.Response)
	}
	if res2.Usage.TotalTokens != 30 {
		t.Errorf("expected 30 total tokens (10+20), got %d", res2.Usage.TotalTokens)
	}

	// Turn 3
	_, err = d.ExecuteTurn(ctx, "turn 3")
	if err == nil {
		t.Errorf("expected error for numeric result payload")
	}
}

func TestStartDaemon_TargetIDEnvironment(t *testing.T) {
	tempDir := t.TempDir()
	envFile := filepath.Join(tempDir, "env_out.txt")
	mockBin := filepath.Join(tempDir, "mock_env_agy.sh")
	script := fmt.Sprintf(`#!/bin/sh
echo "AERIAL_TARGET_ID=$AERIAL_TARGET_ID" > "%s"
echo "DISCORD_THREAD_ID=$DISCORD_THREAD_ID" >> "%s"
echo '{"event":"init","init":{"tools":[]}}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"ok","usage":{}}}'
done
`, envFile, envFile)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock agy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := DaemonConfig{
		ThreadID: "target-thread-9999",
		AgyBin:   mockBin,
		Cwd:      tempDir,
	}

	daemon, err := StartDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer daemon.Close()

	if _, err := daemon.ExecuteTurn(ctx, "check env"); err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}

	outBytes, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("failed to read envFile: %v", err)
	}
	outStr := string(outBytes)
	if !strings.Contains(outStr, "AERIAL_TARGET_ID=target-thread-9999") {
		t.Errorf("expected AERIAL_TARGET_ID=target-thread-9999 in daemon env, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "DISCORD_THREAD_ID=target-thread-9999") {
		t.Errorf("expected DISCORD_THREAD_ID=target-thread-9999 in daemon env, got:\n%s", outStr)
	}

	// Subtest: Empty ThreadID should leave vars empty
	envFileEmpty := filepath.Join(tempDir, "env_empty_out.txt")
	mockBinEmpty := filepath.Join(tempDir, "mock_empty_agy.sh")
	scriptEmpty := fmt.Sprintf(`#!/bin/sh
echo "AERIAL_TARGET_ID=$AERIAL_TARGET_ID" > "%s"
echo "DISCORD_THREAD_ID=$DISCORD_THREAD_ID" >> "%s"
echo '{"event":"init","init":{"tools":[]}}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"ok","usage":{}}}'
done
`, envFileEmpty, envFileEmpty)
	if err := os.WriteFile(mockBinEmpty, []byte(scriptEmpty), 0755); err != nil {
		t.Fatalf("failed to write mock agy: %v", err)
	}

	daemonEmpty, err := StartDaemon(ctx, DaemonConfig{
		ThreadID: "   ",
		AgyBin:   mockBinEmpty,
		Cwd:      tempDir,
	})
	if err != nil {
		t.Fatalf("StartDaemon failed: %v", err)
	}
	defer daemonEmpty.Close()

	if _, err := daemonEmpty.ExecuteTurn(ctx, "check empty env"); err != nil {
		t.Fatalf("ExecuteTurn failed: %v", err)
	}

	outEmptyBytes, err := os.ReadFile(envFileEmpty)
	if err != nil {
		t.Fatalf("failed to read envFileEmpty: %v", err)
	}
	outEmptyStr := string(outEmptyBytes)
	if strings.Contains(outEmptyStr, "AERIAL_TARGET_ID= ") {
		t.Errorf("expected AERIAL_TARGET_ID to not contain spaces, got:\n%s", outEmptyStr)
	}
}

func TestBuildDaemonArgs(t *testing.T) {
	tests := []struct {
		name     string
		cfg      DaemonConfig
		contains []string
		omits    []string
	}{
		{
			name: "Default timeout 60m",
			cfg: DaemonConfig{
				SessionID: "sess-abc",
				Model:     "gemini-test",
			},
			contains: []string{
				"--dangerously-skip-permissions",
				"--input-format", "stream-json",
				"--output-format", "stream-json",
				"--conversation", "sess-abc",
				"--model", "gemini-test",
				"--print-timeout", "60m",
			},
		},
		{
			name: "Custom minute timeout",
			cfg: DaemonConfig{
				Timeout: 15 * time.Minute,
			},
			contains: []string{
				"--print-timeout", "15m",
			},
			omits: []string{"--conversation", "--model"},
		},
		{
			name: "Custom second timeout",
			cfg: DaemonConfig{
				Timeout: 45 * time.Second,
			},
			contains: []string{
				"--print-timeout", "45s",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := BuildDaemonArgs(tc.cfg)
			for i := 0; i < len(tc.contains); i++ {
				expected := tc.contains[i]
				found := false
				for j, arg := range args {
					if arg == expected {
						if i+1 < len(tc.contains) && !strings.HasPrefix(tc.contains[i+1], "--") {
							if j+1 < len(args) && args[j+1] == tc.contains[i+1] {
								found = true
								i++
								break
							}
						} else {
							found = true
							break
						}
					}
				}
				if !found {
					t.Errorf("expected args to contain %q, but got %v", expected, args)
				}
			}

			for _, omitted := range tc.omits {
				for _, arg := range args {
					if arg == omitted {
						t.Errorf("expected args to omit %q, but found it in %v", omitted, args)
					}
				}
			}
		})
	}
}

func TestDaemon_EmptyResponseAndPrintTimeoutSafeguard(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_safeguard.sh")
	script := `#!/bin/sh
echo '{"event":"init","init":{"tools":["run_command"]}}'
while IFS= read -r line; do
  case "$line" in
    *print_timeout*)
      echo "W0919 22:52:39.500551 1 poll.go:204] Print mode: print timeout after 5m0s with turn in progress" >&2
      echo '{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"tool","tool_name":"run_command"}}'
      echo '{"event":"result","result":{"status":"SUCCESS","response":""}}'
      ;;
    *empty_with_tools*)
      echo '{"event":"step_update","step_update":{"step_index":1,"state":"ACTIVE","step_type":"tool","tool_name":"run_command"}}'
      echo '{"event":"result","result":{"status":"SUCCESS","response":""}}'
      ;;
    *)
      echo '{"event":"result","result":{"status":"SUCCESS","response":"ok response"}}'
      ;;
  esac
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	ctx := context.Background()

	t.Run("Print Timeout Triggers Error", func(t *testing.T) {
		d, err := StartDaemon(ctx, DaemonConfig{
			AgyBin: mockBin,
			Cwd:    tempDir,
		})
		if err != nil {
			t.Fatalf("StartDaemon failed: %v", err)
		}
		defer d.Close()

		_, turnErr := d.ExecuteTurn(ctx, "trigger print_timeout")
		if turnErr == nil {
			t.Fatalf("expected error for print timeout turn, got nil")
		}
		if !strings.Contains(turnErr.Error(), "print timeout") {
			t.Errorf("expected error to mention 'print timeout', got: %v", turnErr)
		}
		if !d.IsDirty() {
			t.Errorf("expected daemon to be marked dirty after print timeout")
		}
		if d.State() != StateClosed {
			t.Errorf("expected daemon state to be CLOSED, got: %s", d.State())
		}
	})

	t.Run("Empty Response After Tools Triggers Error", func(t *testing.T) {
		d, err := StartDaemon(ctx, DaemonConfig{
			AgyBin: mockBin,
			Cwd:    tempDir,
		})
		if err != nil {
			t.Fatalf("StartDaemon failed: %v", err)
		}
		defer d.Close()

		_, turnErr := d.ExecuteTurn(ctx, "trigger empty_with_tools")
		if turnErr == nil {
			t.Fatalf("expected error for empty response after tools, got nil")
		}
		if !strings.Contains(turnErr.Error(), "empty response after") {
			t.Errorf("expected error to mention 'empty response after', got: %v", turnErr)
		}
		if !d.IsDirty() {
			t.Errorf("expected daemon to be marked dirty after empty response")
		}
		if d.State() != StateClosed {
			t.Errorf("expected daemon state to be CLOSED, got: %s", d.State())
		}
	})
}

type failWriteCloser struct {
	writeErr error
	closeErr error
}

func (f failWriteCloser) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return len(p), nil
}

func (f failWriteCloser) Close() error {
	return f.closeErr
}

type failReader struct {
	err error
}

func (f failReader) Read(p []byte) (int, error) {
	return 0, f.err
}

func TestDaemon_ExecuteTurn_StdinWriteError(t *testing.T) {
	t.Parallel()

	d := &Daemon{
		state:       StateReady,
		stdin:       failWriteCloser{writeErr: errors.New("broken pipe")},
		taskTracker: NewTaskTracker(),
	}

	_, err := d.ExecuteTurn(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "failed to write prompt") {
		t.Errorf("expected failed to write prompt error, got %v", err)
	}
	if d.State() != StateClosed {
		t.Errorf("expected state to be StateClosed, got %v", d.State())
	}
}

func TestDaemon_ExecuteTurn_YieldWaitingState(t *testing.T) {
	t.Parallel()

	tracker := NewTaskTracker()
	tracker.Add(TaskMetadata{TaskID: "task-123"})

	stdoutBuf := bufio.NewReader(strings.NewReader("{\"event\":\"result\",\"status\":\"SUCCESS\",\"response\":\"done\"}\n"))

	d := &Daemon{
		state:       StateReady,
		stdin:       failWriteCloser{},
		stdout:      stdoutBuf,
		taskTracker: tracker,
		cfg:         DaemonConfig{SessionID: "sess-1"},
	}

	res, err := d.ExecuteTurn(context.Background(), "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Response != "done" {
		t.Errorf("expected response 'done', got %q", res.Response)
	}
	if d.State() != StateYieldWaiting {
		t.Errorf("expected state StateYieldWaiting when active tasks exist, got %v", d.State())
	}
}

func TestDaemon_ExecuteTurn_NonEOFReadError(t *testing.T) {
	t.Parallel()

	d := &Daemon{
		state:       StateReady,
		stdin:       failWriteCloser{},
		stdout:      bufio.NewReader(failReader{err: errors.New("read error occurred")}),
		taskTracker: NewTaskTracker(),
		cfg:         DaemonConfig{SessionID: "sess-1"},
	}

	_, err := d.ExecuteTurn(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "error reading daemon stream") {
		t.Errorf("expected error reading daemon stream, got %v", err)
	}
}

func TestDaemon_Close_StdinCloseError(t *testing.T) {
	t.Parallel()

	d := &Daemon{
		state: StateReady,
		stdin: failWriteCloser{closeErr: errors.New("failed to close stdin")},
	}

	if err := d.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

