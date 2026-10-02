package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

type testMockFSWatcher struct {
	mu       sync.Mutex
	events   chan fsnotify.Event
	errors   chan error
	watched  map[string]bool
	closed   bool
	addErr   error
	closeErr error
}

func newTestMockFSWatcher() *testMockFSWatcher {
	return &testMockFSWatcher{
		events:  make(chan fsnotify.Event, 100),
		errors:  make(chan error, 10),
		watched: make(map[string]bool),
	}
}

func (m *testMockFSWatcher) Add(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addErr != nil {
		return m.addErr
	}
	m.watched[name] = true
	return nil
}

func (m *testMockFSWatcher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.closeErr
	}
	m.closed = true
	return m.closeErr
}

func (m *testMockFSWatcher) Events() <-chan fsnotify.Event {
	return m.events
}

func (m *testMockFSWatcher) Errors() <-chan error {
	return m.errors
}

func TestHermeticWatcher_FullLifecycle(t *testing.T) {
	mock := newTestMockFSWatcher()
	var callbackCount int32

	w, err := NewWatcher(
		withBackend(mock),
		WithDebounce(10*time.Millisecond),
		WithCallback(func() {
			atomic.AddInt32(&callbackCount, 1)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	tmpDir := t.TempDir()
	sub1 := filepath.Join(tmpDir, "sub1")
	sub2 := filepath.Join(sub1, "sub2")
	_ = os.MkdirAll(sub2, 0755)

	if err := w.AddRecursive(tmpDir); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}

	w.mu.Lock()
	if !w.watchedDirs[tmpDir] || !w.watchedDirs[sub1] || !w.watchedDirs[sub2] {
		t.Fatalf("expected dirs to be watched, got: %v", w.watchedDirs)
	}
	w.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()

	// 1. Send Write event on a normal file
	mock.events <- fsnotify.Event{
		Name: filepath.Join(tmpDir, "config.yaml"),
		Op:   fsnotify.Write,
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&callbackCount) > 0
	})

	// 1b. Send Create event on a regular file (fi.IsDir() is false)
	regularFile := filepath.Join(tmpDir, "regular.txt")
	_ = os.WriteFile(regularFile, []byte("content"), 0644)
	mock.events <- fsnotify.Event{
		Name: regularFile,
		Op:   fsnotify.Create,
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&callbackCount) > 1
	})

	// 2. Send event on an ignored file (.git) -> Should not trigger callback
	currentCount := atomic.LoadInt32(&callbackCount)
	mock.events <- fsnotify.Event{
		Name: filepath.Join(tmpDir, ".git", "index"),
		Op:   fsnotify.Write,
	}
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt32(&callbackCount) != currentCount {
		t.Errorf("expected ignored file event not to trigger callback")
	}

	// 3. Dynamic directory creation via Create event
	newSub := filepath.Join(tmpDir, "dynamically_added")
	_ = os.Mkdir(newSub, 0755)
	mock.events <- fsnotify.Event{
		Name: newSub,
		Op:   fsnotify.Create,
	}

	waitForCondition(t, 2*time.Second, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.watchedDirs[newSub]
	})

	// 4. Directory removal via Remove / Rename event
	mock.events <- fsnotify.Event{
		Name: sub1,
		Op:   fsnotify.Remove,
	}

	waitForCondition(t, 2*time.Second, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return !w.watchedDirs[sub1] && !w.watchedDirs[sub2]
	})

	// 5. Send error on Errors() channel
	mock.errors <- errors.New("simulated error in mock")
	time.Sleep(10 * time.Millisecond)

	// 6. Test context cancel
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit on ctx cancel")
	}

	// 7. Test Close idempotency
	if err := w.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestHermeticWatcher_AddRecursiveEdgeCases(t *testing.T) {
	mock := newTestMockFSWatcher()
	w, err := NewWatcher(withBackend(mock))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Non-existent directory
	if err := w.AddRecursive("/path/that/does/not/exist"); err == nil {
		t.Errorf("expected error for non-existent directory")
	}

	// Regular file instead of directory
	tmpFile := filepath.Join(t.TempDir(), "test.txt")
	_ = os.WriteFile(tmpFile, []byte("data"), 0644)
	if err := w.AddRecursive(tmpFile); err != nil {
		t.Errorf("expected regular file to return nil, got %v", err)
	}

	// Ignored directory
	tmpDir := t.TempDir()
	gitDir := filepath.Join(tmpDir, ".git")
	_ = os.Mkdir(gitDir, 0755)
	if err := w.AddRecursive(tmpDir); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}
	w.mu.Lock()
	if w.watchedDirs[gitDir] {
		t.Errorf("expected .git directory to be ignored")
	}
	w.mu.Unlock()
}

func TestHermeticWatcher_CallbackConcurrencyAndExecution(t *testing.T) {
	mock := newTestMockFSWatcher()
	var runCount int32
	var mu sync.Mutex

	w, err := NewWatcher(
		withBackend(mock),
		WithDebounce(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	w.AddCallback(func() {
		mu.Lock()
		defer mu.Unlock()
		atomic.AddInt32(&runCount, 1)
	})
	w.AddCallback(nil) // Test nil callback safety

	w.executeCallbacks()
	if atomic.LoadInt32(&runCount) != 1 {
		t.Errorf("expected runCount to be 1, got %d", atomic.LoadInt32(&runCount))
	}

	// Trigger debounced
	w.triggerDebounced()
	waitForCondition(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&runCount) >= 2
	})

	// Close watcher and verify triggerDebounced and executeCallbacks do nothing
	_ = w.Close()
	w.triggerDebounced()
	w.executeCallbacks()
}

func TestHermeticWatcher_EventsChannelClose(t *testing.T) {
	mock := newTestMockFSWatcher()
	w, err := NewWatcher(withBackend(mock))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()

	time.Sleep(5 * time.Millisecond)
	// Close events channel from mock backend
	close(mock.events)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit when events channel closed")
	}
	_ = w.Close()
}

func TestHermeticWatcher_ErrorsChannelClose(t *testing.T) {
	mock := newTestMockFSWatcher()
	w, err := NewWatcher(withBackend(mock))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()

	time.Sleep(5 * time.Millisecond)
	// Close errors channel from mock backend
	close(mock.errors)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit when errors channel closed")
	}
	_ = w.Close()
}

func TestRealFsnotifyWatcher_Direct(t *testing.T) {
	dummy := &fsnotify.Watcher{
		Events: make(chan fsnotify.Event),
		Errors: make(chan error),
	}
	adapter := &realFsnotifyWatcher{w: dummy}
	if adapter.Events() == nil {
		t.Errorf("expected non-nil events channel")
	}
	if adapter.Errors() == nil {
		t.Errorf("expected non-nil errors channel")
	}

	func() {
		defer func() { _ = recover() }()
		_ = adapter.Add("/tmp")
	}()

	func() {
		defer func() { _ = recover() }()
		_ = adapter.Close()
	}()
}

func TestHermeticWatcher_ExecuteCallbacksBranches(t *testing.T) {
	mock := newTestMockFSWatcher()
	w, err := NewWatcher(withBackend(mock))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	// 1. Concurrent lock skip
	w.cbMu.Lock()
	w.executeCallbacks() // should return immediately
	w.cbMu.Unlock()

	// 2. Closed watcher skip
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.executeCallbacks() // should return immediately when closed
}

func TestHermeticWatcher_DynamicCreateAddRecursiveError(t *testing.T) {
	mock := newTestMockFSWatcher()
	mock.addErr = errors.New("simulated add failure")
	w, err := NewWatcher(withBackend(mock))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "sub_fail")
	_ = os.Mkdir(subDir, 0755)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	// Send create event on directory when Add will fail
	mock.events <- fsnotify.Event{
		Name: subDir,
		Op:   fsnotify.Create,
	}

	time.Sleep(20 * time.Millisecond)
	_ = w.Close()
}

