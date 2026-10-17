package queue

import (
	"testing"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWorkerPool_UnifiedProcessPoolWiring(t *testing.T) {
	tests := []struct {
		name                 string
		nilPool              bool
		defaultConfig        bool
		withProcessPool      bool
		withVoicePool        bool
		checkAutoLLM         bool
		prewarmedTargets     []string
		verifyVoicePrewarmed bool
	}{
		{
			name:    "nil worker pool returns nil process pool and safe mark dirty",
			nilPool: true,
		},
		{
			name:          "default config has nil process pool and voice pool",
			defaultConfig: true,
		},
		{
			name:             "configured process pool populates and accesses",
			withProcessPool:  true,
			prewarmedTargets: []string{"kiosk", "ephemeral:classifier", "ephemeral:summarizer"},
		},
		{
			name:            "auto populates LLMFunc from process pool",
			withProcessPool: true,
			checkAutoLLM:    true,
		},
		{
			name:            "voice process pool wiring and accessors",
			withProcessPool: true,
			withVoicePool:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilPool {
				var p *WorkerPool
				if got := p.ProcessPool(); got != nil {
					t.Fatalf("expected nil ProcessPool from nil WorkerPool, got %v", got)
				}
				if got := p.VoiceProcessPool(); got != nil {
					t.Fatalf("expected nil VoiceProcessPool from nil WorkerPool, got %v", got)
				}
				p.MarkDirty() // Should not panic
				return
			}

			if tc.defaultConfig {
				p := New(nil, WorkerPoolConfig{})
				if got := p.ProcessPool(); got != nil {
					t.Fatalf("expected nil ProcessPool by default, got %v", got)
				}
				if got := p.VoiceProcessPool(); got != nil {
					t.Fatalf("expected nil VoiceProcessPool by default, got %v", got)
				}
				return
			}

			mockSpawner := runner.NewMockDaemonSpawner()
			var unifiedPool runner.AgentPool
			if tc.withProcessPool {
				up := runner.NewUnifiedProcessPool(runner.PoolConfig{
					PrewarmedTargets: tc.prewarmedTargets,
					DefaultModel:     "gemini-2.5-flash",
				}, mockSpawner)
				defer func() { _ = up.Close() }()
				unifiedPool = up
			}

			var voicePool runner.AgentPool
			if tc.withVoicePool {
				vp := runner.NewUnifiedProcessPool(runner.PoolConfig{
					DefaultModel: "gemini-2.5-flash",
				}, mockSpawner)
				defer func() { _ = vp.Close() }()
				voicePool = vp
			}

			p := New(nil, WorkerPoolConfig{
				ProcessPool:      unifiedPool,
				VoiceProcessPool: voicePool,
			})
			defer p.Stop()

			if tc.withProcessPool && p.ProcessPool() != unifiedPool {
				t.Fatalf("expected ProcessPool to return %p, got %p", unifiedPool, p.ProcessPool())
			}

			if tc.checkAutoLLM && p.cfg.LLMFunc == nil {
				t.Fatalf("expected cfg.LLMFunc to be auto-populated from ProcessPool")
			}

			if tc.withVoicePool {
				if got := p.VoiceProcessPool(); got != voicePool {
					t.Fatalf("expected VoiceProcessPool to return %p, got %p", voicePool, got)
				}
				p.MarkDirty()
			}
		})
	}
}

func TestWorkerPool_DualPoolTurnRouting(t *testing.T) {
	tests := []struct {
		name                 string
		nilPool              bool
		defaultConfig        bool
		interchangeable      bool
		cleanAllThree        bool
		reconcilePrewarmed   bool
		initialVoiceTargets  []string
		expectedVoiceTargets []string
	}{
		{
			name:    "nil worker pool returns nil low effort pool",
			nilPool: true,
		},
		{
			name:          "default config low effort pool is nil",
			defaultConfig: true,
		},
		{
			name:            "interchangeable pool returns underlying pool",
			interchangeable: true,
		},
		{
			name:          "mark dirty and stop cleans all three pools",
			cleanAllThree: true,
		},
		{
			name:                 "mark dirty reconciles voice prewarmed targets without arbitrary sleeps",
			reconcilePrewarmed:   true,
			initialVoiceTargets:  []string{"kiosk"},
			expectedVoiceTargets: []string{"touch-kiosk-kitchen"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilPool {
				var p *WorkerPool
				if got := p.LowEffortProcessPool(); got != nil {
					t.Fatalf("expected nil from nil WorkerPool.LowEffortProcessPool(), got %v", got)
				}
				if got := p.MediumEffortProcessPool(); got != nil {
					t.Fatalf("expected nil from nil WorkerPool.MediumEffortProcessPool(), got %v", got)
				}
				return
			}

			if tc.defaultConfig {
				p := New(nil, WorkerPoolConfig{})
				if got := p.LowEffortProcessPool(); got != nil {
					t.Fatalf("expected nil LowEffortProcessPool by default, got %v", got)
				}
				if got := p.MediumEffortProcessPool(); got != nil {
					t.Fatalf("expected nil MediumEffortProcessPool by default, got %v", got)
				}
				return
			}

			if tc.interchangeable {
				mockSpawner := runner.NewMockDaemonSpawner()
				underlying := runner.NewUnifiedProcessPool(runner.PoolConfig{
					Model: "gemini-3.8-flash-low",
				}, mockSpawner)
				defer func() { _ = underlying.Close() }()

				interPool := runner.NewInterchangeablePool(underlying, runner.InterchangeablePoolConfig{
					WorkerCount: 2,
				})
				defer func() { _ = interPool.Close() }()

				medUnderlying := runner.NewUnifiedProcessPool(runner.PoolConfig{
					Model: "gemini-3.8-flash-medium",
				}, mockSpawner)
				defer func() { _ = medUnderlying.Close() }()
				medInterPool := runner.NewInterchangeablePool(medUnderlying, runner.InterchangeablePoolConfig{
					WorkerCount: 2,
				})
				defer func() { _ = medInterPool.Close() }()

				p := New(nil, WorkerPoolConfig{
					LowEffortProcessPool:    interPool,
					MediumEffortProcessPool: medInterPool,
				})
				defer p.Stop()

				if got := p.LowEffortProcessPool(); got != underlying {
					t.Fatalf("expected LowEffortProcessPool to return underlying pool %p, got %p", underlying, got)
				}
				if got := p.MediumEffortProcessPool(); got != medUnderlying {
					t.Fatalf("expected MediumEffortProcessPool to return underlying pool %p, got %p", medUnderlying, got)
				}
				return
			}

			if tc.cleanAllThree {
				pSpawner := runner.NewMockSpawner()
				pPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, pSpawner)
				defer func() { _ = pPool.Close() }()

				lSpawner := runner.NewMockSpawner()
				lPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lSpawner)
				defer func() { _ = lPool.Close() }()

				vSpawner := runner.NewMockSpawner()
				vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash"}, vSpawner)
				defer func() { _ = vPool.Close() }()

				pool := NewWorkerPool(WorkerPoolConfig{
					ProcessPool:          pPool,
					LowEffortProcessPool: lPool,
					VoiceProcessPool:     vPool,
				})
				pool.MarkDirty()
				pool.Stop()
				return
			}

			if tc.reconcilePrewarmed {
				vSpawner := runner.NewMockDaemonSpawner()
				vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
					Model:            "gemini-2.5-flash",
					PrewarmedTargets: tc.initialVoiceTargets,
				}, vSpawner)
				defer func() { _ = vPool.Close() }()

				appCfg := config.NewTestConfig(func(d *config.ConfigData) {
					d.Voice.PrewarmedTargets = tc.expectedVoiceTargets
				})

				pool := New(appCfg, WorkerPoolConfig{
					VoiceProcessPool: vPool,
				})
				defer pool.Stop()

				pool.MarkDirty()
				vPool.WaitBackground()

				targets := vPool.PrewarmedTargets()
				if len(targets) != len(tc.expectedVoiceTargets) || targets[0] != tc.expectedVoiceTargets[0] {
					t.Fatalf("expected voice pool targets to be %v, got %v", tc.expectedVoiceTargets, targets)
				}
				if !vPool.HasDaemon(tc.expectedVoiceTargets[0]) {
					t.Fatalf("expected %s to be active in process pool", tc.expectedVoiceTargets[0])
				}
			}
		})
	}
}
