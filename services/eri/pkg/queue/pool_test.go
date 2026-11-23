package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func TestLogInterruptWarning(t *testing.T) {
	t.Parallel()
	logInterruptWarning("test-op-nil", nil)
	logInterruptWarning("test-op-err", errors.New("simulated interrupt warning"))
}

type mockModelUpdatingPool struct {
	runner.AgentPool

	updatedModel string
	prewarmed    []string
}

func (m *mockModelUpdatingPool) UpdateModel(model string) {
	m.updatedModel = model
}

func (m *mockModelUpdatingPool) UpdatePrewarmedTargets(targets []string) {
	m.prewarmed = append([]string(nil), targets...)
}

func TestWorkerPool_PoolModelForEffortAndReconcile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		effort            string
		cfgModel          string
		lowModel          string
		medModel          string
		voiceModel        string
		hasExplicitAppCfg bool
		voiceTargets      []string
		wantEffortModel   string
	}{
		{
			name:              "low effort model",
			effort:            config.EffortLow,
			cfgModel:          "base-model",
			lowModel:          "low-model",
			medModel:          "med-model",
			voiceModel:        "voice-model",
			hasExplicitAppCfg: true,
			voiceTargets:      []string{"kiosk-1"},
			wantEffortModel:   "low-model",
		},
		{
			name:              "medium effort model",
			effort:            config.EffortMedium,
			cfgModel:          "base-model",
			lowModel:          "low-model",
			medModel:          "med-model",
			voiceModel:        "voice-model",
			hasExplicitAppCfg: true,
			voiceTargets:      []string{"kiosk-2"},
			wantEffortModel:   "med-model",
		},
		{
			name:              "default effort model",
			effort:            "high",
			cfgModel:          "base-model",
			lowModel:          "low-model",
			medModel:          "med-model",
			voiceModel:        "voice-model",
			hasExplicitAppCfg: false,
			voiceTargets:      nil,
			wantEffortModel:   "base-model",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := &WorkerPool{
				cfg: WorkerPoolConfig{
					Model:             tc.cfgModel,
					LowEffortModel:    tc.lowModel,
					MediumEffortModel: tc.medModel,
				},
			}
			gotEffort := pool.poolModelForEffort(tc.effort)
			if gotEffort != tc.wantEffortModel {
				t.Fatalf("poolModelForEffort(%q) = %q, want %q", tc.effort, gotEffort, tc.wantEffortModel)
			}

			mockPoolMap := make(map[PoolKey]runner.AgentPool)
			for _, k := range AllValidPoolKeys() {
				mockPoolMap[k] = &mockModelUpdatingPool{}
			}
			mockVoice := &mockModelUpdatingPool{}

			pool.reconcilePoolModelsAndVoiceTargets(mockPoolMap, mockVoice, tc.hasExplicitAppCfg, tc.voiceTargets)
			if tc.hasExplicitAppCfg && len(tc.voiceTargets) > 0 {
				if len(mockVoice.prewarmed) != len(tc.voiceTargets) {
					t.Fatalf("voice targets len = %d, want %d", len(mockVoice.prewarmed), len(tc.voiceTargets))
				}
			}
		})
	}
}

func TestWorkerPool_ResolveChannelPolicy(t *testing.T) {
	t.Parallel()
	customPolicy := config.ChannelPolicy{WakeMode: config.WakeModeAll}
	poolWithHook := &WorkerPool{
		cfg: WorkerPoolConfig{
			ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
				return customPolicy
			},
		},
	}
	if got := poolWithHook.resolveChannelPolicy("ch-1", "chan"); got.GetWakeMode() != config.WakeModeAll {
		t.Fatalf("got wake mode %q, want %q", got.GetWakeMode(), config.WakeModeAll)
	}

	poolDefault := &WorkerPool{}
	if got := poolDefault.resolveChannelPolicy("ch-default", "default"); got.Mode == "" {
		t.Fatal("expected non-empty channel policy mode")
	}
}

func TestWorkerPool_CloseTurnExecution(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		nilTurn        bool
		dispatch       bool
		noThread       bool
		daemonKey      string
		wantDaemonLive bool
	}{
		{name: "nil turn is a no-op", nilTurn: true, daemonKey: "discord-1234567890", wantDaemonLive: true},
		{name: "burst turn closes its thread daemon", daemonKey: "discord-1234567890"},
		{name: "dispatch turn closes the daemon under its session key", dispatch: true, daemonKey: "1234567890"},
		{name: "daemon under another key survives", daemonKey: "discord-9876543210", wantDaemonLive: true},
		{name: "turn without a thread closes no daemon", noThread: true, daemonKey: "discord-1234567890", wantDaemonLive: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, newMockDaemonSpawner())
			t.Cleanup(func() { _ = procPool.Close() })
			if _, err := procPool.GetOrCreate(context.Background(), tc.daemonKey, ""); err != nil {
				t.Fatalf("spawn daemon: %v", err)
			}
			pool := &WorkerPool{pools: map[PoolKey]runner.AgentPool{{Ephemeral: false, Effort: config.EffortHigh}: procPool}}
			turnCtx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var te *turnExecution
			if !tc.nilTurn {
				te = &turnExecution{threadID: "1234567890", turnCtx: turnCtx, turnCancel: cancel}
				if tc.dispatch {
					te.dispatch = &db.Dispatch{ID: "d-close"}
				}
				if tc.noThread {
					te.threadID = ""
				}
			}
			pool.closeTurnExecution(te, "operator stop")
			if _, live := procPool.Get(tc.daemonKey); live != tc.wantDaemonLive {
				t.Errorf("daemon %q live = %v, want %v", tc.daemonKey, live, tc.wantDaemonLive)
			}
			if gotCancelled := turnCtx.Err() != nil; gotCancelled != !tc.nilTurn {
				t.Errorf("turn context cancelled = %v, want %v", gotCancelled, !tc.nilTurn)
			}
		})
	}
}

func TestResolveDefaultAppConfig(t *testing.T) {
	t.Parallel()
	given := config.NewFromData(&config.ConfigData{Model: "given-model"})
	tests := []struct {
		name             string
		appCfg           *config.Config
		cfg              WorkerPoolConfig
		wantExplicit     bool
		wantModel        string
		wantLowModel     string
		wantMediumModel  string
		wantSystemPrompt string
		wantNATSURL      string
	}{
		{name: "explicit config wins", appCfg: given, cfg: WorkerPoolConfig{Model: "ignored"}, wantExplicit: true, wantModel: "given-model"},
		{name: "pool overrides fill defaults", cfg: WorkerPoolConfig{Model: "m-high", LowEffortModel: "m-low", MediumEffortModel: "m-med", SystemPrompt: "be brief", NATSURL: "nats://bus:4222"}, wantModel: "m-high", wantLowModel: "m-low", wantMediumModel: "m-med", wantSystemPrompt: "be brief", wantNATSURL: "nats://bus:4222"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, explicit := resolveDefaultAppConfig(tc.appCfg, tc.cfg)
			if explicit != tc.wantExplicit {
				t.Errorf("explicit = %v, want %v", explicit, tc.wantExplicit)
			}
			cur := got.Current()
			if cur.Model != tc.wantModel {
				t.Errorf("Model = %q, want %q", cur.Model, tc.wantModel)
			}
			if tc.wantExplicit {
				return
			}
			if cur.LowEffortModel != tc.wantLowModel || cur.MediumEffortModel != tc.wantMediumModel || cur.SystemPrompt != tc.wantSystemPrompt || cur.NATSURL != tc.wantNATSURL {
				t.Errorf("overrides = %q/%q/%q/%q, want %q/%q/%q/%q", cur.LowEffortModel, cur.MediumEffortModel, cur.SystemPrompt, cur.NATSURL, tc.wantLowModel, tc.wantMediumModel, tc.wantSystemPrompt, tc.wantNATSURL)
			}
		})
	}
}

func TestApplyWorkerPoolTimingDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		in          WorkerPoolConfig
		wantMax     int
		wantBackoff time.Duration
		wantFlush   time.Duration
	}{
		{name: "zero values get defaults", wantMax: 3, wantBackoff: 3 * time.Second, wantFlush: 5 * time.Second},
		{name: "explicit values kept", in: WorkerPoolConfig{MaxAttempts: 5, BackoffBase: time.Second, ChunkStepFlushInterval: time.Minute}, wantMax: 5, wantBackoff: time.Second, wantFlush: time.Minute},
		{name: "negative flush interval disables flushing", in: WorkerPoolConfig{ChunkStepFlushInterval: -time.Second}, wantMax: 3, wantBackoff: 3 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.in
			applyWorkerPoolTimingDefaults(&cfg)
			if cfg.MaxAttempts != tc.wantMax || cfg.BackoffBase != tc.wantBackoff || cfg.ChunkStepFlushInterval != tc.wantFlush {
				t.Errorf("got max=%d backoff=%v flush=%v, want %d/%v/%v", cfg.MaxAttempts, cfg.BackoffBase, cfg.ChunkStepFlushInterval, tc.wantMax, tc.wantBackoff, tc.wantFlush)
			}
		})
	}
}

func TestWorkerPool_NilReceiverAccessors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		pool              *WorkerPool
		wantEventBusNil   bool
		wantStatefulPools int
		wantRuntimeModel  string
	}{
		{name: "nil pool", wantEventBusNil: true},
		{name: "override model wins", pool: &WorkerPool{overrideModel: "override-m", cfg: WorkerPoolConfig{Model: "cfg-m"}}, wantEventBusNil: true, wantRuntimeModel: "override-m"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.pool.SetEventBus(nil)
			tc.pool.MarkDirty()
			if got := tc.pool.EventBus() == nil; got != tc.wantEventBusNil {
				t.Errorf("EventBus() nil = %v, want %v", got, tc.wantEventBusNil)
			}
			if got := len(tc.pool.StatefulProcessPools()); got != tc.wantStatefulPools {
				t.Errorf("StatefulProcessPools() = %d, want %d", got, tc.wantStatefulPools)
			}
			if tc.pool != nil && tc.pool.GetRuntimeConfig() != tc.wantRuntimeModel {
				t.Errorf("GetRuntimeConfig() = %q, want %q", tc.pool.GetRuntimeConfig(), tc.wantRuntimeModel)
			}
		})
	}
}
