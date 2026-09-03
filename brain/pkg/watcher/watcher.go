package watcher

import (
	"context"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

type ReloadCallback func()

type Watcher struct {
	fsw         *fsnotify.Watcher
	debounce    time.Duration
	fallback    time.Duration
	callbacks   []ReloadCallback
	mu          sync.Mutex
	cbMu        sync.Mutex
	timer       *time.Timer
	watchedDirs map[string]bool
	closed      bool
}

type Option func(*Watcher)

func WithDebounce(d time.Duration) Option {
	return func(w *Watcher) {
		w.debounce = d
	}
}

func WithFallbackInterval(d time.Duration) Option {
	return func(w *Watcher) {
		w.fallback = d
	}
}

func WithCallback(cb ReloadCallback) Option {
	return func(w *Watcher) {
		w.callbacks = append(w.callbacks, cb)
	}
}

// NewWatcher initializes a new fsnotify file watcher with default 500ms debounce and 30s fallback ticker.
func NewWatcher(opts ...Option) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	w := &Watcher{
		fsw:         fsw,
		debounce:    500 * time.Millisecond,
		fallback:    30 * time.Second,
		callbacks:   make([]ReloadCallback, 0),
		watchedDirs: make(map[string]bool),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// ShouldIgnore returns true for .git, .gemini, *.db*, system_instructions.md, rules directories, and editor/process temporary files.
func ShouldIgnore(path string) bool {
	base := filepath.Base(path)
	if base == ".git" || base == ".gemini" || base == "system_instructions.md" || base == "user_persona.md" {
		return true
	}

	normalized := filepath.ToSlash(path)
	parts := strings.Split(normalized, "/")
	for _, p := range parts {
		if p == ".git" || p == ".gemini" {
			return true
		}
	}

	// Ignore .agents/rules, /rules/, or trailing /rules
	if strings.Contains(normalized, "/.agents/rules") ||
		strings.Contains(normalized, ".agents/rules") ||
		strings.Contains(normalized, "/rules/") ||
		strings.HasSuffix(normalized, "/rules") ||
		base == "rules" {
		return true
	}

	// SQLite database files
	if strings.Contains(base, ".db") {
		return true
	}

	// Editor and process temporary files: *.tmp.*, .tmp., *.tmp, *.swp, *.swx, ~*, #*#
	if strings.Contains(base, ".tmp.") || strings.HasSuffix(base, ".tmp") || strings.Contains(base, ".tmp") {
		return true
	}
	if strings.HasSuffix(base, ".swp") || strings.HasSuffix(base, ".swx") {
		return true
	}
	if strings.HasPrefix(base, ".#") || strings.HasPrefix(base, "#") || strings.HasSuffix(base, "~") {
		return true
	}

	return false
}

// AddRecursive walks directory and adds fsnotify watches to all non-ignored subdirectories.
func (w *Watcher) AddRecursive(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return nil
	}

	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			if ShouldIgnore(path) {
				return filepath.SkipDir
			}

			w.mu.Lock()
			if !w.closed {
				if !w.watchedDirs[path] {
					if addErr := w.fsw.Add(path); addErr == nil {
						w.watchedDirs[path] = true
						log.Printf("[Watcher] Watching directory: %s", path)
					}
				}
			}
			w.mu.Unlock()
		}
		return nil
	})
}

// AddCallback registers a reload callback.
func (w *Watcher) AddCallback(cb ReloadCallback) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callbacks = append(w.callbacks, cb)
}

// triggerDebounced starts or resets the debounce timer to execute callbacks.
func (w *Watcher) triggerDebounced() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	if w.timer != nil {
		w.timer.Stop()
	}
	w.timer = time.AfterFunc(w.debounce, func() {
		w.executeCallbacks()
	})
}

// executeCallbacks executes all registered reload callbacks.
func (w *Watcher) executeCallbacks() {
	if !w.cbMu.TryLock() {
		// Skip if callbacks are already executing concurrently
		return
	}
	defer w.cbMu.Unlock()

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	cbs := make([]ReloadCallback, len(w.callbacks))
	copy(cbs, w.callbacks)
	w.mu.Unlock()

	for _, cb := range cbs {
		if cb != nil {
			cb()
		}
	}
}

// Start runs the background event listening loop until ctx is cancelled or watcher is closed.
func (w *Watcher) Start(ctx context.Context) {
	var fallbackTicker *time.Ticker
	var fallbackChan <-chan time.Time

	if w.fallback > 0 {
		fallbackTicker = time.NewTicker(w.fallback)
		defer fallbackTicker.Stop()
		fallbackChan = fallbackTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			_ = w.Close()
			return

		case event, ok := <-w.fsw.Events:
			if !ok {
				return
			}

			// Clean up watchedDirs on Remove or Rename of watched directories
			if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
				w.mu.Lock()
				delete(w.watchedDirs, event.Name)
				prefix := event.Name + string(filepath.Separator)
				for dir := range w.watchedDirs {
					if strings.HasPrefix(dir, prefix) {
						delete(w.watchedDirs, dir)
					}
				}
				w.mu.Unlock()
			}

			if ShouldIgnore(event.Name) {
				continue
			}

			// Dynamically add watch on newly created directory
			if event.Op&fsnotify.Create != 0 {
				if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
					_ = w.AddRecursive(event.Name)
				}
			}

			// Debounce write, create, rename, remove events
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) != 0 {
				w.triggerDebounced()
			}

		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			log.Printf("[Watcher] fsnotify error: %v", err)

		case <-fallbackChan:
			// Fallback ticker to guarantee sync even on non-inotify mounts
			w.executeCallbacks()
		}
	}
}

// Close closes the fsnotify watcher and stops active timers.
func (w *Watcher) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	if w.timer != nil {
		w.timer.Stop()
	}
	w.mu.Unlock()

	return w.fsw.Close()
}
