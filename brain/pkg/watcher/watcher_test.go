package watcher

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestShouldIgnore(t *testing.T) {
	ignoredPaths := []string{
		".git",
		"/path/to/.git",
		"/path/to/.git/HEAD",
		".gemini",
		"/home/user/.gemini/settings.json",
		"system_instructions.md",
		"/share/aerial/.agents/rules/system_instructions.md",
		"/app/.agents/rules/system_instructions.md",
		"/home/user/.gemini/rules/system_instructions.md",
		"user_persona.md",
		"/share/aerial/.gemini/rules/user_persona.md",
		"/share/aerial/.agents/rules",
		"/app/.agents/rules",
		"/path/to/rules/custom.md",
		"aerial.db",
		"/data/aerial.db-wal",
		"/data/aerial.db-shm",
		"file.swp",
		"file.swx",
		"file.tmp",
		".file.tmp",
		".system_instructions.md.tmp.12345",
		"foo.tmp.999",
		"#file#",
		"file~",
	}

	for _, p := range ignoredPaths {
		if !ShouldIgnore(p) {
			t.Errorf("Expected path %q to be ignored", p)
		}
	}

	allowedPaths := []string{
		"AGENTS.md",
		"/share/aerial-config/AGENTS.md",
		"GEMINI.md",
		"/share/aerial/GEMINI.md",
		"/share/aerial-config/GEMINI.md",
		"SKILL.md",
		"/share/aerial-config/custom-skills/smart-home/SKILL.md",
		"config.json",
	}

	for _, p := range allowedPaths {
		if ShouldIgnore(p) {
			t.Errorf("Expected path %q NOT to be ignored", p)
		}
	}
}

func TestWatcherRecursiveAndIgnore(t *testing.T) {
	tmpDir := t.TempDir()

	sub1 := filepath.Join(tmpDir, "sub1")
	sub2 := filepath.Join(sub1, "sub2")
	gitDir := filepath.Join(tmpDir, ".git")
	geminiDir := filepath.Join(tmpDir, ".gemini")
	rulesDir := filepath.Join(tmpDir, ".agents", "rules")

	_ = os.MkdirAll(sub2, 0755)
	_ = os.MkdirAll(gitDir, 0755)
	_ = os.MkdirAll(geminiDir, 0755)
	_ = os.MkdirAll(rulesDir, 0755)

	w, err := NewWatcher()
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.AddRecursive(tmpDir); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}

	w.mu.Lock()
	if !w.watchedDirs[tmpDir] || !w.watchedDirs[sub1] || !w.watchedDirs[sub2] {
		t.Errorf("Expected non-ignored directories to be watched, got map: %v", w.watchedDirs)
	}
	if w.watchedDirs[gitDir] || w.watchedDirs[geminiDir] || w.watchedDirs[rulesDir] {
		t.Errorf("Expected ignored directories (.git, .gemini, .agents/rules) NOT to be watched, got map: %v", w.watchedDirs)
	}
	w.mu.Unlock()
}

func TestWatcherDebounceAndDynamicDir(t *testing.T) {
	tmpDir := t.TempDir()

	var callbackCount int32
	w, err := NewWatcher(
		WithDebounce(50*time.Millisecond),
		WithFallbackInterval(0), // Disable fallback ticker for this test
		WithCallback(func() {
			atomic.AddInt32(&callbackCount, 1)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.AddRecursive(tmpDir); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	// Rapidly write multiple files to verify debounce triggers fewer times than writes
	for i := 0; i < 5; i++ {
		testFile := filepath.Join(tmpDir, "test.md")
		_ = os.WriteFile(testFile, []byte("hello"), 0644)
		time.Sleep(5 * time.Millisecond)
	}

	// Wait for debounce period to settle
	time.Sleep(150 * time.Millisecond)

	count := atomic.LoadInt32(&callbackCount)
	if count == 0 {
		t.Errorf("Expected reload callback to be executed, but got %d", count)
	}

	// Dynamically create a new subdirectory and write inside it
	newSubDir := filepath.Join(tmpDir, "new_sub")
	_ = os.Mkdir(newSubDir, 0755)
	time.Sleep(50 * time.Millisecond)

	newSubFile := filepath.Join(newSubDir, "inner.md")
	_ = os.WriteFile(newSubFile, []byte("inner content"), 0644)

	time.Sleep(150 * time.Millisecond)

	newCount := atomic.LoadInt32(&callbackCount)
	if newCount <= count {
		t.Errorf("Expected reload callback to trigger for newly created subdirectory, got %d (prev: %d)", newCount, count)
	}
}

func TestWatcherFallbackTicker(t *testing.T) {
	var fallbackCount int32
	w, err := NewWatcher(
		WithDebounce(10*time.Millisecond),
		WithFallbackInterval(30*time.Millisecond),
		WithCallback(func() {
			atomic.AddInt32(&fallbackCount, 1)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	// Wait for fallback ticker to fire at least once
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&fallbackCount) == 0 {
		t.Errorf("Expected fallback ticker to execute callback, got 0")
	}
}

func TestWatcher_RecreatedDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	subDir := filepath.Join(tmpDir, "sub")
	_ = os.Mkdir(subDir, 0755)

	var callbackCount int32
	w, err := NewWatcher(
		WithDebounce(50*time.Millisecond),
		WithFallbackInterval(0),
		WithCallback(func() {
			atomic.AddInt32(&callbackCount, 1)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.AddRecursive(tmpDir); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Start(ctx)

	w.mu.Lock()
	if !w.watchedDirs[subDir] {
		t.Fatalf("Expected subDir %s to be watched", subDir)
	}
	w.mu.Unlock()

	// Remove directory and wait for event processing
	if err := os.RemoveAll(subDir); err != nil {
		t.Fatalf("Failed to remove subDir: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	w.mu.Lock()
	if w.watchedDirs[subDir] {
		t.Errorf("Expected subDir %s to be removed from watchedDirs after removal", subDir)
	}
	w.mu.Unlock()

	// Recreate directory
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("Failed to recreate subDir: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	w.mu.Lock()
	isWatched := w.watchedDirs[subDir]
	w.mu.Unlock()
	if !isWatched {
		t.Errorf("Expected recreated subDir %s to be re-added to watchedDirs", subDir)
	}

	// Write file into recreated directory to ensure events fire
	prevCount := atomic.LoadInt32(&callbackCount)
	testFile := filepath.Join(subDir, "file.txt")
	_ = os.WriteFile(testFile, []byte("content"), 0644)
	time.Sleep(150 * time.Millisecond)

	newCount := atomic.LoadInt32(&callbackCount)
	if newCount <= prevCount {
		t.Errorf("Expected event trigger in recreated directory, got count %d <= %d", newCount, prevCount)
	}
}

func TestWatcher_ConcurrentCallbacks(t *testing.T) {
	var running int32
	var maxConcurrent int32

	w, err := NewWatcher(
		WithDebounce(10*time.Millisecond),
		WithFallbackInterval(0),
		WithCallback(func() {
			current := atomic.AddInt32(&running, 1)
			defer atomic.AddInt32(&running, -1)

			// Record max concurrency observed
			for {
				oldMax := atomic.LoadInt32(&maxConcurrent)
				if current <= oldMax || atomic.CompareAndSwapInt32(&maxConcurrent, oldMax, current) {
					break
				}
			}

			// Simulate slow reload operation
			time.Sleep(50 * time.Millisecond)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Trigger callbacks concurrently from multiple goroutines
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			w.executeCallbacks()
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if maxVal := atomic.LoadInt32(&maxConcurrent); maxVal > 1 {
		t.Errorf("Expected max concurrent executions to be 1, got %d", maxVal)
	}
}

func TestAddCallbackAndClose(t *testing.T) {
	w, err := NewWatcher()
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	var called bool
	w.AddCallback(func() {
		called = true
	})

	w.executeCallbacks()
	if !called {
		t.Errorf("expected callback added with AddCallback to be called")
	}

	// Close twice to test idempotency
	if err := w.Close(); err != nil {
		t.Errorf("expected Close to succeed, got %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("expected second Close to succeed, got %v", err)
	}

	// triggerDebounced and executeCallbacks after close should be no-op
	w.triggerDebounced()
	w.executeCallbacks()
}

func TestWatcher_NonExistentDir(t *testing.T) {
	w, err := NewWatcher()
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	err = w.AddRecursive("/path/to/definitely/nonexistent/directory")
	if err == nil {
		t.Errorf("expected error when adding non-existent directory")
	}

	// Test adding a regular file (should return nil early)
	tmpFile := filepath.Join(t.TempDir(), "file.txt")
	_ = os.WriteFile(tmpFile, []byte("hello"), 0644)
	if err := w.AddRecursive(tmpFile); err != nil {
		t.Errorf("expected AddRecursive on regular file to return nil, got %v", err)
	}
}

func TestWatcher_StartContextCancel(t *testing.T) {
	w, err := NewWatcher(WithFallbackInterval(0))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit on context cancellation")
	}
}

func TestShouldIgnore_AdditionalPatterns(t *testing.T) {
	cases := []struct {
		path   string
		ignore bool
	}{
		{"/a/b/user_persona.md", true},
		{"/a/b/file.swp", true},
		{"/a/b/file.swx", true},
		{"/a/b/.#lockfile", true},
		{"/a/b/#autosave#", true},
		{"/a/b/file~", true},
		{"/a/b/normal_file.yaml", false},
	}
	for _, tc := range cases {
		if res := ShouldIgnore(tc.path); res != tc.ignore {
			t.Errorf("ShouldIgnore(%q) = %v; want %v", tc.path, res, tc.ignore)
		}
	}
}

func TestWatcher_StartClose(t *testing.T) {
	w, err := NewWatcher(WithFallbackInterval(0))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	done := make(chan struct{})
	go func() {
		w.Start(context.Background())
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	_ = w.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit on Close")
	}
}

func TestWatcher_AddRecursive_SkipDir(t *testing.T) {
	tmpDir := t.TempDir()
	gitDir := filepath.Join(tmpDir, ".git")
	_ = os.Mkdir(gitDir, 0755)
	_ = os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main"), 0644)

	w, err := NewWatcher(WithFallbackInterval(0))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.AddRecursive(tmpDir); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}

	w.mu.Lock()
	watchedGit := w.watchedDirs[gitDir]
	w.mu.Unlock()
	if watchedGit {
		t.Errorf("expected .git directory to be skipped by AddRecursive")
	}

	// Test WithCallback with nil callback
	w2, err := NewWatcher(WithCallback(nil))
	if err != nil {
		t.Fatalf("NewWatcher failed: %v", err)
	}
	_ = w2.Close()

	if !ShouldIgnore("cache.tmp") {
		t.Errorf("expected cache.tmp to be ignored")
	}
}

func TestWatcher_NestedDirectoryRemoval(t *testing.T) {
	tmpDir := t.TempDir()
	parent := filepath.Join(tmpDir, "parent")
	child := filepath.Join(parent, "child")
	_ = os.MkdirAll(child, 0755)

	w, err := NewWatcher(WithFallbackInterval(0))
	if err != nil {
		t.Fatalf("NewWatcher failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.AddRecursive(parent); err != nil {
		t.Fatalf("AddRecursive failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Start(ctx)

	// Verify both are watched
	w.mu.Lock()
	pWatched := w.watchedDirs[parent]
	cWatched := w.watchedDirs[child]
	w.mu.Unlock()
	if !pWatched || !cWatched {
		t.Fatalf("expected parent and child to be watched")
	}

	// Manually inject a subpath entry in watchedDirs to guarantee line 226 coverage
	w.mu.Lock()
	w.watchedDirs[filepath.Join(parent, "virtual_sub")] = true
	w.mu.Unlock()

	// Write an ignored file to trigger the ShouldIgnore branch in Start
	ignoredFile := filepath.Join(parent, "test.tmp")
	_ = os.WriteFile(ignoredFile, []byte("ignored"), 0644)
	time.Sleep(100 * time.Millisecond)

	// Remove parent
	_ = os.RemoveAll(parent)
	time.Sleep(150 * time.Millisecond)

	w.mu.Lock()
	cStillWatched := w.watchedDirs[child]
	vStillWatched := w.watchedDirs[filepath.Join(parent, "virtual_sub")]
	w.mu.Unlock()
	if cStillWatched || vStillWatched {
		t.Errorf("expected subdirs to be removed from watchedDirs when parent removed")
	}
}

