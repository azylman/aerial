package queue

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

// MemoryChecker returns the fraction of available system memory (0.0 to 1.0).
type MemoryChecker func() (availablePercent float64, err error)

// parseMeminfo parses MemTotal and MemAvailable from a meminfo-formatted reader.
func parseMeminfo(r io.Reader) (float64, error) {
	var total, avail float64
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if parsedTotal, parseErr := strconv.ParseFloat(fields[1], 64); parseErr == nil {
					total = parsedTotal
				}
			}
		} else if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if parsedAvail, parseErr := strconv.ParseFloat(fields[1], 64); parseErr == nil {
					avail = parsedAvail
				}
			}
		}
	}
	if total > 0 {
		return avail / total, nil
	}
	return 1.0, nil
}

var meminfoPath = "/proc/meminfo"

// DefaultLinuxMemoryChecker inspects /proc/meminfo to calculate available memory headroom.
func DefaultLinuxMemoryChecker() (float64, error) {
	file, err := os.Open(meminfoPath)
	if err != nil {
		return 1.0, err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("[DaemonPool] Warning: failed to close /proc/meminfo: %v", closeErr)
		}
	}()
	return parseMeminfo(file)
}

// DaemonPool manages a pool of persistent streaming agy daemons mapped by thread ID.
type DaemonPool struct {
	cfg        *config.Config
	agyBin     string
	env        []string
	cwd        string
	daemons    map[string]*runner.Daemon
	mu         sync.RWMutex
	memChecker MemoryChecker
}

// NewDaemonPool constructs an initialized DaemonPool.
func NewDaemonPool(cfg *config.Config, agyBin string, env []string, cwd string) *DaemonPool {
	return &DaemonPool{
		cfg:        cfg,
		agyBin:     agyBin,
		env:        env,
		cwd:        cwd,
		daemons:    make(map[string]*runner.Daemon),
		memChecker: DefaultLinuxMemoryChecker,
	}
}

// SetMemoryChecker overrides the memory checker function (primarily for testing).
func (p *DaemonPool) SetMemoryChecker(checker MemoryChecker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.memChecker = checker
}

// ActiveCount returns the number of persistent daemons currently tracked in the pool.
func (p *DaemonPool) ActiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.daemons)
}

// HasDaemon reports whether a persistent daemon currently exists for threadID.
func (p *DaemonPool) HasDaemon(threadID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.daemons[threadID]
	return ok
}

// GetOrCreateDaemon returns an existing persistent daemon for threadID, or starts a fresh one.
func (p *DaemonPool) GetOrCreateDaemon(ctx context.Context, threadID string, sessionID string, model string) (*runner.Daemon, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check existing
	if d, ok := p.daemons[threadID]; ok {
		if !d.IsDirty() && d.State() != runner.StateClosed && (sessionID == "" || d.SessionID() == "" || d.SessionID() == sessionID) {
			return d, nil
		}
		// Close dirty, dead, or rotated daemon
		if closeErr := d.Close(); closeErr != nil {
			log.Printf("[DaemonPool] Warning: failed to close rotated daemon for thread %s: %v", threadID, closeErr)
		}
		delete(p.daemons, threadID)
	}

	// Enforce concurrency ceiling via LRU eviction
	ceiling := 40
	if p.cfg != nil {
		data := p.cfg.Get()
		if data.MaxConcurrentDaemons > 0 {
			ceiling = data.MaxConcurrentDaemons
		}
	}

	if len(p.daemons) >= ceiling {
		if !p.evictOldestIdleLocked() {
			log.Printf("[DaemonPool] Warning: pool at capacity (%d) with all daemons active", len(p.daemons))
		}
	}

	geminiHome := ""
	if p.cfg != nil {
		geminiHome = p.cfg.GeminiHomeDir()
	}
	dCfg := runner.DaemonConfig{
		SessionID:     sessionID,
		ThreadID:      threadID,
		Model:         model,
		AgyBin:        p.agyBin,
		Cwd:           p.cwd,
		Env:           p.env,
		GeminiHomeDir: geminiHome,
	}

	d, err := runner.StartDaemon(ctx, dCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start persistent daemon: %w", err)
	}

	p.daemons[threadID] = d
	metrics.DaemonsActive.Set(float64(len(p.daemons)))
	metrics.DaemonSpawnsTotal.WithLabelValues("cold_start").Inc()

	return d, nil
}

// HasActiveTasks reports whether the daemon associated with threadID currently has active background tasks.
func (p *DaemonPool) HasActiveTasks(threadID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if d, ok := p.daemons[threadID]; ok {
		return d.TaskTracker().ActiveCount() > 0
	}
	return false
}

// ActiveTasks returns active tasks for the daemon associated with threadID.
func (p *DaemonPool) ActiveTasks(threadID string) []runner.TaskMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if d, ok := p.daemons[threadID]; ok && d != nil {
		return d.TaskTracker().ActiveTasks()
	}
	return nil
}

// RemoveTask removes a tracked task from the daemon associated with threadID.
func (p *DaemonPool) RemoveTask(threadID, taskID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if d, ok := p.daemons[threadID]; ok && d != nil {
		res := d.TaskTracker().Remove(taskID)
		metrics.ActiveTasksGauge.WithLabelValues(threadID).Set(float64(d.TaskTracker().ActiveCount()))
		return res
	}
	return false
}

// UpdateMemoryMetrics calculates aggregate RSS across active daemons and sets metrics.DaemonMemoryBytes.
func (p *DaemonPool) UpdateMemoryMetrics() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var totalRSS uint64
	for _, d := range p.daemons {
		if d != nil {
			totalRSS += d.RSSBytes()
		}
	}
	metrics.DaemonMemoryBytes.Set(float64(totalRSS))
}

// ReleaseDaemon handles cleanup of dirty daemons once a turn completes.
func (p *DaemonPool) ReleaseDaemon(threadID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d, ok := p.daemons[threadID]; ok {
		if d.IsDirty() && d.TaskTracker().ActiveCount() == 0 {
			if closeErr := d.Close(); closeErr != nil {
				log.Printf("[DaemonPool] Warning: failed to close dirty daemon for thread %s: %v", threadID, closeErr)
			}
			delete(p.daemons, threadID)
			metrics.DaemonsActive.Set(float64(len(p.daemons)))
			metrics.DaemonPrunesTotal.WithLabelValues("dirty_release").Inc()
		}
	}
}

func (p *DaemonPool) evictOldestIdleLocked() bool {
	var oldestThread string
	var oldestTime time.Time

	for thID, d := range p.daemons {
		if d.TaskTracker().ActiveCount() == 0 && d.State() == runner.StateReady {
			if oldestThread == "" || d.LastUsed().Before(oldestTime) {
				oldestThread = thID
				oldestTime = d.LastUsed()
			}
		}
	}

	if oldestThread != "" {
		d := p.daemons[oldestThread]
		if closeErr := d.Close(); closeErr != nil {
			log.Printf("[DaemonPool] Warning: failed to close LRU daemon for thread %s: %v", oldestThread, closeErr)
		}
		delete(p.daemons, oldestThread)
		metrics.DaemonsActive.Set(float64(len(p.daemons)))
		metrics.DaemonPrunesTotal.WithLabelValues("lru_ceiling").Inc()
		return true
	}
	return false
}

// EvictLRU evicts the oldest idle daemon without active tasks.
func (p *DaemonPool) EvictLRU() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.evictOldestIdleLocked()
}

// CheckMemoryPressureAndEvict inspects available memory and evicts idle daemons if available memory < 15%.
func (p *DaemonPool) CheckMemoryPressureAndEvict() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.memChecker == nil {
		return false
	}
	avail, err := p.memChecker()
	if err != nil || avail >= 0.15 {
		return false
	}

	log.Printf("[DaemonPool] Host memory pressure detected (available: %.1f%% < 15.0%%). Running LRU eviction.", avail*100)
	evicted := p.evictOldestIdleLocked()
	if evicted {
		metrics.DaemonPrunesTotal.WithLabelValues("lru_memory").Inc()
	}
	return evicted
}

// PruneIdle closes daemons that have been idle longer than maxIdle with 0 active background tasks.
func (p *DaemonPool) PruneIdle(maxIdle time.Duration) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	pruned := 0
	for thID, d := range p.daemons {
		if d.TaskTracker().ActiveCount() == 0 && d.State() == runner.StateReady {
			if now.Sub(d.LastUsed()) > maxIdle {
				if closeErr := d.Close(); closeErr != nil {
					log.Printf("[DaemonPool] Warning: failed to close idle daemon for thread %s: %v", thID, closeErr)
				}
				delete(p.daemons, thID)
				pruned++
				metrics.DaemonPrunesTotal.WithLabelValues("idle_timeout").Inc()
			}
		}
	}
	if pruned > 0 {
		metrics.DaemonsActive.Set(float64(len(p.daemons)))
	}
	return pruned
}

// MonitorZombieTasks prunes tasks exceeding maxDuration from active task trackers.
func (p *DaemonPool) MonitorZombieTasks(maxDuration time.Duration) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	terminated := 0
	for _, d := range p.daemons {
		expired := d.TaskTracker().PruneExpired(maxDuration)
		for _, taskID := range expired {
			terminated++
			metrics.TaskTimeoutsTotal.Inc()
			log.Printf("[DaemonPool] Terminated zombie background task %s exceeding %v duration ceiling", taskID, maxDuration)
		}
	}
	return terminated
}

// MarkDirty marks all active daemons dirty (or closes idle daemons immediately) on configuration reload.
func (p *DaemonPool) MarkDirty() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for thID, d := range p.daemons {
		if d.State() == runner.StateReady && d.TaskTracker().ActiveCount() == 0 {
			if closeErr := d.Close(); closeErr != nil {
				log.Printf("[DaemonPool] Warning: failed to close reload daemon for thread %s: %v", thID, closeErr)
			}
			delete(p.daemons, thID)
			metrics.DaemonPrunesTotal.WithLabelValues("hot_reload").Inc()
		} else {
			d.SetDirty(true)
		}
	}
	metrics.DaemonsActive.Set(float64(len(p.daemons)))
}

// Close gracefully closes all persistent daemons and empties the pool.
func (p *DaemonPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for thID, d := range p.daemons {
		if closeErr := d.Close(); closeErr != nil {
			log.Printf("[DaemonPool] Warning: failed to close daemon for thread %s: %v", thID, closeErr)
		}
		delete(p.daemons, thID)
	}
	metrics.DaemonsActive.Set(0)
	return nil
}
