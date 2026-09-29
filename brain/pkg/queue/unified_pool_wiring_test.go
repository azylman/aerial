package queue

import (
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWorkerPool_UnifiedProcessPoolWiring(t *testing.T) {
	t.Run("NilWorkerPool_ReturnsNil", func(t *testing.T) {
		var p *WorkerPool
		if got := p.ProcessPool(); got != nil {
			t.Fatalf("expected nil ProcessPool from nil WorkerPool, got %v", got)
		}
	})

	t.Run("DefaultConfig_ProcessPoolIsNil", func(t *testing.T) {
		p := New(nil, WorkerPoolConfig{})
		if got := p.ProcessPool(); got != nil {
			t.Fatalf("expected nil ProcessPool by default, got %v", got)
		}
	})

	t.Run("ConfiguredProcessPool_PopulatesAndAccesses", func(t *testing.T) {
		mockSpawner := runner.NewMockDaemonSpawner()
		unifiedPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			PrewarmedTargets: []string{"kiosk", "ephemeral:classifier", "ephemeral:summarizer"},
			DefaultModel:     "gemini-2.5-flash",
		}, mockSpawner)
		defer func() {
			if err := unifiedPool.Close(); err != nil {
				t.Logf("cleanup unified pool: %v", err)
			}
		}()

		p := New(nil, WorkerPoolConfig{
			ProcessPool: unifiedPool,
		})

		if got := p.ProcessPool(); got != unifiedPool {
			t.Fatalf("expected ProcessPool to return %p, got %p", unifiedPool, got)
		}
	})

	t.Run("MaintenanceLoopRunsWithoutMemoryPressureEviction", func(t *testing.T) {
		tempDir := t.TempDir()
		cfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.MaxConcurrentDaemons = 5
			d.DaemonIdleTimeout = 1 * time.Hour
			d.DataDir = tempDir
		})

		mockSpawner := runner.NewMockDaemonSpawner()
		unifiedPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			PrewarmedTargets: []string{"kiosk"},
			DefaultModel:     "gemini-2.5-flash",
		}, mockSpawner)
		defer func() {
			if err := unifiedPool.Close(); err != nil {
				t.Logf("cleanup unified pool: %v", err)
			}
		}()

		p := New(cfg, WorkerPoolConfig{
			ProcessPool:         unifiedPool,
			MaintenanceInterval: 10 * time.Millisecond,
		})

		p.Start()

		// Allow maintenance loop to tick multiple times
		time.Sleep(50 * time.Millisecond)

		p.Stop()
	})

	t.Run("AutoPopulatesLLMFunc_FromProcessPool", func(t *testing.T) {
		mockSpawner := runner.NewMockDaemonSpawner()
		unifiedPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			DefaultModel: "gemini-2.5-flash",
		}, mockSpawner)
		defer unifiedPool.Close()

		p := New(nil, WorkerPoolConfig{
			ProcessPool: unifiedPool,
		})

		if p.cfg.LLMFunc == nil {
			t.Fatalf("expected cfg.LLMFunc to be auto-populated from ProcessPool")
		}
	})

	t.Run("VoiceProcessPool_WiringAndAccessors", func(t *testing.T) {
		var nilP *WorkerPool
		if got := nilP.VoiceProcessPool(); got != nil {
			t.Fatalf("expected nil from nil WorkerPool.VoiceProcessPool(), got %v", got)
		}
		nilP.MarkDirty() // Should not panic

		pDefault := New(nil, WorkerPoolConfig{})
		if got := pDefault.VoiceProcessPool(); got != nil {
			t.Fatalf("expected nil VoiceProcessPool by default, got %v", got)
		}

		mockSpawner := runner.NewMockDaemonSpawner()
		voicePool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			DefaultModel: "gemini-2.5-flash",
		}, mockSpawner)
		defer voicePool.Close()

		procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			DefaultModel: "gemini-2.5-flash",
		}, mockSpawner)
		defer procPool.Close()

		p := New(nil, WorkerPoolConfig{
			ProcessPool:      procPool,
			VoiceProcessPool: voicePool,
		})

		if got := p.VoiceProcessPool(); got != voicePool {
			t.Fatalf("expected VoiceProcessPool to return %p, got %p", voicePool, got)
		}

		p.MarkDirty()
		p.Stop()
	})
}

func TestWorkerPool_DualPoolTurnRouting(t *testing.T) {
	primarySpawner := runner.NewMockSpawner()
	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowEffortSpawner := runner.NewMockSpawner()
	lowEffortPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowEffortSpawner)
	defer lowEffortPool.Close()

	wp := NewWorkerPool(WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowEffortPool,
	})
	defer wp.Stop()

	if wp.LowEffortProcessPool() != lowEffortPool {
		t.Fatalf("expected LowEffortProcessPool to match injected pool")
	}

	t.Run("NilWorkerPool_ReturnsNil", func(t *testing.T) {
		var p *WorkerPool
		if got := p.LowEffortProcessPool(); got != nil {
			t.Fatalf("expected nil from nil WorkerPool.LowEffortProcessPool(), got %v", got)
		}
	})

	t.Run("DefaultConfig_LowEffortProcessPoolIsNil", func(t *testing.T) {
		p := New(nil, WorkerPoolConfig{})
		if got := p.LowEffortProcessPool(); got != nil {
			t.Fatalf("expected nil LowEffortProcessPool by default, got %v", got)
		}
	})

	t.Run("MarkDirtyAndStop_CleansAllThreePools", func(t *testing.T) {
		pSpawner := runner.NewMockSpawner()
		pPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, pSpawner)
		defer pPool.Close()

		lSpawner := runner.NewMockSpawner()
		lPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lSpawner)
		defer lPool.Close()

		vSpawner := runner.NewMockSpawner()
		vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash"}, vSpawner)
		defer vPool.Close()

		pool := NewWorkerPool(WorkerPoolConfig{
			ProcessPool:          pPool,
			LowEffortProcessPool: lPool,
			VoiceProcessPool:     vPool,
		})

		pool.MarkDirty()
		pool.Stop()
	})

	t.Run("WorkerPool_MarkDirty_ReconcilesVoicePrewarmedTargets", func(t *testing.T) {
		vSpawner := runner.NewMockDaemonSpawner()
		vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			Model:            "gemini-2.5-flash",
			PrewarmedTargets: []string{"kiosk"},
		}, vSpawner)
		defer vPool.Close()

		appCfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.Voice.PrewarmedTargets = []string{"touch-kiosk-kitchen"}
		})

		pool := New(appCfg, WorkerPoolConfig{
			VoiceProcessPool: vPool,
		})
		defer pool.Stop()

		// Trigger MarkDirty on WorkerPool
		pool.MarkDirty()

		// Deterministically wait for background eviction and prewarming without arbitrary sleeps
		vPool.WaitBackground()

		// Verify voice process pool has updated prewarmed targets
		targets := vPool.PrewarmedTargets()
		if len(targets) != 1 || targets[0] != "touch-kiosk-kitchen" {
			t.Fatalf("expected voice pool targets to be updated to [touch-kiosk-kitchen], got %v", targets)
		}

		if !vPool.HasDaemon("touch-kiosk-kitchen") {
			t.Fatalf("expected touch-kiosk-kitchen to be active in process pool")
		}
	})
}

