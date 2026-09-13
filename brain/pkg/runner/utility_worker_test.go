package runner

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkerInstance_LifecycleAndExecution(t *testing.T) {
	w, harness := NewMockStreamWorker(t, MockStreamWorkerConfig{})
	defer harness.Close()
	defer w.Close()

	if w.ConversationID() == "" {
		t.Errorf("unexpected empty conversation ID")
	}
	if w.TurnsUsed() != 0 {
		t.Errorf("expected 0 turns initially, got %d", w.TurnsUsed())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	w, harness := NewMockStreamWorker(t, MockStreamWorkerConfig{})
	defer harness.Close()
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Trigger crash
	_, err := w.Execute(ctx, "CRASH_NOW")
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
	w, harness := NewMockStreamWorker(t, MockStreamWorkerConfig{
		SleepDelay: 5 * time.Second,
	})
	defer harness.Close()
	defer w.Close()

	callCtx, callCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer callCancel()

	_, err := w.Execute(callCtx, "SLEEP_COMMAND")
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

	// 3. custom rssFunc
	w4 := &WorkerInstance{
		rssFunc: func() uint64 { return 12345 },
	}
	if rss := w4.RSSBytes(); rss != 12345 {
		t.Errorf("expected 12345 RSS, got %d", rss)
	}
}

func TestWorkerInstance_SpawnFailures(t *testing.T) {
	bin := getHelperProcessBin(t)

	// 1. Invalid binary path
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	_, err := NewWorkerInstance(ctx1, WorkerOptions{
		AgyBin: filepath.Join(t.TempDir(), "nonexistent_binary"),
	})
	if err == nil {
		t.Error("expected error for nonexistent binary")
	}

	// 2. Handshake timeout via NewWorkerInstance
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_, err = NewWorkerInstance(ctx2, WorkerOptions{
		AgyBin:   bin,
		Timeout:  20 * time.Millisecond,
		ExtraEnv: []string{"GO_WANT_HELPER_PROCESS=1", "MOCK_MODE=hang"},
	})
	if err != ErrHandshakeTimeout {
		t.Errorf("expected ErrHandshakeTimeout, got %v", err)
	}

	// 3. Process exits before handshake via NewWorkerInstance
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	_, err = NewWorkerInstance(ctx3, WorkerOptions{
		AgyBin:   bin,
		ExtraEnv: []string{"GO_WANT_HELPER_PROCESS=1", "MOCK_MODE=exit"},
	})
	if err == nil || !strings.Contains(err.Error(), ErrInvalidHandshake.Error()) {
		t.Errorf("expected ErrInvalidHandshake, got %v", err)
	}
}

func TestStreamIO_NilSafety(t *testing.T) {
	if err := WriteWorkerTurn(nil, "foo"); err == nil {
		t.Error("expected error writing to nil writer")
	}
	if _, err := ReadWorkerTurn(nil); err == nil {
		t.Error("expected error reading from nil reader")
	}
}

func TestWorkerInstance_DiskPurgeOnClose(t *testing.T) {
	tmpDir := t.TempDir()
	w, harness := NewMockStreamWorker(t, MockStreamWorkerConfig{
		Roots: []string{tmpDir},
	})
	defer harness.Close()

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
	if os.Getenv("GOOS") == "windows" || filepath.Separator == '\\' {
		t.Skip("RSS /proc/statm inspection is only available on Linux")
	}
	w := &WorkerInstance{
		cmd: &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
	}
	if rss := w.RSSBytes(); rss == 0 {
		t.Error("expected non-zero RSS for live process")
	}
}

func TestWorkerInstance_SpawnWithAPIKeyAndHandshakeCancel(t *testing.T) {
	bin := getHelperProcessBin(t)
	opts := WorkerOptions{
		AgyBin:   bin,
		APIKey:   "test-api-key",
		HomeDir:  t.TempDir(),
		ExtraEnv: []string{"GO_WANT_HELPER_PROCESS=1", "MOCK_MODE=stream-json"},
	}
	w, err := NewWorkerInstance(context.Background(), opts)
	if err != nil {
		t.Fatalf("failed to spawn worker with API key: %v", err)
	}
	w.Close()

	// Handshake cancellation
	userInR, userInW := io.Pipe()
	userOutR, userOutW := io.Pipe()
	defer userInR.Close()
	defer userInW.Close()
	defer userOutR.Close()
	defer userOutW.Close()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = NewWorkerInstanceFromStreams(canceledCtx, userInW, userOutR, cancel, nil, 2*time.Second, nil)
	if err == nil {
		t.Error("expected error for canceled context during handshake")
	}
}

func TestWorkerInstance_ExecuteWriteError(t *testing.T) {
	w, harness := NewMockStreamWorker(t, MockStreamWorkerConfig{})
	defer harness.Close()
	defer w.Close()

	_ = w.stdin.Close()

	_, err := w.Execute(context.Background(), "test write error")
	if err == nil {
		t.Fatal("expected write error on closed stdin")
	}
}

func TestWorkerInstance_ParseErrorOnResult(t *testing.T) {
	w, harness := NewMockStreamWorker(t, MockStreamWorkerConfig{
		BadResult: true,
	})
	defer harness.Close()
	defer w.Close()

	_, err := w.Execute(context.Background(), "test bad json")
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
}
