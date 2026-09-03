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
