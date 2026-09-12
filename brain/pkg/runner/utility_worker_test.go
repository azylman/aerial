package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func createMockStreamJsonHelper(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "mock_agy.sh")

	script := `#!/usr/bin/env python3
import json, sys, time, uuid, os

conv_id = os.environ.get("MOCK_CONV_ID", str(uuid.uuid4()))
print(json.dumps({"event": "init", "conversation_id": conv_id}), flush=True)
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    if "CRASH_ALWAYS" in line or "CRASH_NOW" in line:
        sys.exit(1)
    if "CRASH_ONCE" in line:
        marker = "/tmp/mock_crash_once.marker"
        if not os.path.exists(marker):
            with open(marker, "w") as f: f.write("1")
            sys.exit(1)
    if "SLEEP" in line:
        time.sleep(5)
    try:
        data = json.loads(line)
        content = data.get("message", {}).get("content", "")
    except Exception:
        content = line
    res = {
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": "ECHO:" + content,
            "conversation_id": conv_id
        }
    }
    print(json.dumps(res), flush=True)
`
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}
	return scriptPath
}

func TestWorkerInstance_LifecycleAndExecution(t *testing.T) {
	script := createMockStreamJsonHelper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := WorkerOptions{
		AgyBin:  script,
		Model:   "test-model",
		HomeDir: t.TempDir(),
	}

	w, err := NewWorkerInstance(ctx, opts)
	if err != nil {
		t.Fatalf("failed to spawn worker instance: %v", err)
	}
	defer w.Close()

	if w.ConversationID() == "" {
		t.Errorf("unexpected empty conversation ID")
	}
	if w.TurnsUsed() != 0 {
		t.Errorf("expected 0 turns initially, got %d", w.TurnsUsed())
	}

	// Turn 1
	resp, err := w.Execute(ctx, "hello world")
	if err != nil {
		t.Fatalf("execute turn 1 failed: %v", err)
	}
	if !strings.Contains(resp.Response, "hello world") {
		t.Errorf("expected response to contain prompt, got: %q", resp.Response)
	}
	if w.TurnsUsed() != 1 {
		t.Errorf("expected 1 turn used, got %d", w.TurnsUsed())
	}

	// Turn 2
	resp2, err := w.Execute(ctx, "second prompt")
	if err != nil {
		t.Fatalf("execute turn 2 failed: %v", err)
	}
	if !strings.Contains(resp2.Response, "second prompt") {
		t.Errorf("expected response to contain second prompt, got: %q", resp2.Response)
	}
	if w.TurnsUsed() != 2 {
		t.Errorf("expected 2 turns used, got %d", w.TurnsUsed())
	}
}

func TestWorkerInstance_BrokenPipeDetection(t *testing.T) {
	script := createMockStreamJsonHelper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := WorkerOptions{
		AgyBin:  script,
		Model:   "test-model",
		HomeDir: t.TempDir(),
	}

	w, err := NewWorkerInstance(ctx, opts)
	if err != nil {
		t.Fatalf("failed to spawn worker instance: %v", err)
	}
	defer w.Close()

	// Trigger crash
	_, err = w.Execute(ctx, "CRASH_NOW")
	if err == nil {
		t.Fatal("expected error on crash, got nil")
	}

	if !w.IsDead() {
		t.Error("expected worker to be marked dead after crash")
	}

	// Subsequent execute must fail immediately with ErrWorkerDead
	_, err = w.Execute(ctx, "after crash")
	if err != ErrWorkerDead {
		t.Errorf("expected ErrWorkerDead, got %v", err)
	}
}

func TestWorkerInstance_ContextCancellation(t *testing.T) {
	script := createMockStreamJsonHelper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := WorkerOptions{
		AgyBin:  script,
		Model:   "test-model",
		HomeDir: t.TempDir(),
	}

	w, err := NewWorkerInstance(ctx, opts)
	if err != nil {
		t.Fatalf("failed to spawn worker instance: %v", err)
	}
	defer w.Close()

	// Execute with tight timeout on a slow command
	callCtx, callCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer callCancel()

	_, err = w.Execute(callCtx, "SLEEP_COMMAND")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("expected deadline exceeded, got: %v", err)
	}
	if !w.IsDead() {
		t.Error("expected timed out worker to be marked dead")
	}
}

func TestWorkerInstance_RSSBytesEdgeCases(t *testing.T) {
	// 1. nil cmd
	w1 := &WorkerInstance{}
	if rss := w1.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 RSS for nil cmd, got %d", rss)
	}

	// 2. process with pid <= 0 or nonexistent pid
	w2 := &WorkerInstance{
		cmd: &exec.Cmd{Process: &os.Process{Pid: -1}},
	}
	if rss := w2.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 RSS for negative pid, got %d", rss)
	}

	w3 := &WorkerInstance{
		cmd: &exec.Cmd{Process: &os.Process{Pid: 999999999}},
	}
	if rss := w3.RSSBytes(); rss != 0 {
		t.Errorf("expected 0 RSS for nonexistent pid, got %d", rss)
	}
}

func TestWorkerInstance_SpawnFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Invalid binary path
	_, err := NewWorkerInstance(ctx, WorkerOptions{
		AgyBin: "/nonexistent/path/to/binary",
	})
	if err == nil {
		t.Error("expected error for nonexistent binary")
	}

	// 2. Handshake timeout
	hangingScript := filepath.Join(t.TempDir(), "hang.sh")
	if err := os.WriteFile(hangingScript, []byte("#!/bin/sh\nsleep 10\n"), 0755); err != nil {
		t.Fatalf("failed to write hang script: %v", err)
	}
	_, err = NewWorkerInstance(ctx, WorkerOptions{
		AgyBin:  hangingScript,
		Timeout: 20 * time.Millisecond,
	})
	if err != ErrHandshakeTimeout {
		t.Errorf("expected ErrHandshakeTimeout, got %v", err)
	}

	// 3. Stdout closed before init event
	exitScript := filepath.Join(t.TempDir(), "exit.sh")
	if err := os.WriteFile(exitScript, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("failed to write exit script: %v", err)
	}
	_, err = NewWorkerInstance(ctx, WorkerOptions{
		AgyBin: exitScript,
	})
	if err == nil || !strings.Contains(err.Error(), ErrInvalidHandshake.Error()) {
		t.Errorf("expected ErrInvalidHandshake, got %v", err)
	}
}

func TestWorkerInstance_DiskPurgeOnClose(t *testing.T) {
	tmpDir := t.TempDir()
	script := createMockStreamJsonHelper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w, err := NewWorkerInstance(ctx, WorkerOptions{
		AgyBin:       script,
		SessionRoots: []string{tmpDir},
	})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	convID := w.ConversationID()
	sessDir := filepath.Join(tmpDir, convID)
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	w.Close()
	w.Close() // Idempotent close

	if _, err := os.Stat(sessDir); !os.IsNotExist(err) {
		t.Errorf("expected session directory %s to be purged, but it still exists", sessDir)
	}
}

func TestWorkerInstance_RSSBytesLive(t *testing.T) {
	w := &WorkerInstance{
		cmd: &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
	}
	if rss := w.RSSBytes(); rss == 0 {
		t.Error("expected non-zero RSS for live process")
	}
}

func TestWorkerInstance_SpawnWithAPIKeyAndHandshakeCancel(t *testing.T) {
	script := createMockStreamJsonHelper(t)
	opts := WorkerOptions{
		AgyBin:  script,
		APIKey:  "test-api-key",
		HomeDir: t.TempDir(),
	}
	w, err := NewWorkerInstance(context.Background(), opts)
	if err != nil {
		t.Fatalf("failed to spawn worker with API key: %v", err)
	}
	w.Close()

	// Handshake cancellation
	hangingScript := filepath.Join(t.TempDir(), "hang2.sh")
	if err := os.WriteFile(hangingScript, []byte("#!/bin/sh\nsleep 10\n"), 0755); err != nil {
		t.Fatalf("failed to write hang script: %v", err)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = NewWorkerInstance(canceledCtx, WorkerOptions{
		AgyBin: hangingScript,
	})
	if err == nil {
		t.Error("expected error for canceled context during handshake")
	}
}

func TestWorkerInstance_ExecuteWriteError(t *testing.T) {
	script := createMockStreamJsonHelper(t)
	w, err := NewWorkerInstance(context.Background(), WorkerOptions{
		AgyBin: script,
	})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
	defer w.Close()

	_ = w.stdin.Close()

	_, err = w.Execute(context.Background(), "test write error")
	if err == nil {
		t.Fatal("expected write error on closed stdin")
	}
}

func TestWorkerInstance_ParseErrorOnResult(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "bad_result.sh")
	script := `#!/bin/sh
echo '{"event":"init","conversation_id":"bad-1"}'
while read line; do
  echo '{"event":"result", "result": invalid json}'
done
`
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write script: %v", err)
	}

	w, err := NewWorkerInstance(context.Background(), WorkerOptions{
		AgyBin: scriptPath,
	})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
	defer w.Close()

	_, err = w.Execute(context.Background(), "test bad json")
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
}


