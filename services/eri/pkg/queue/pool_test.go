package queue

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func TestWorkerPool_InterruptThread_FinalizedTurnLatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		finalized       bool
		seedStatus      string
		wantLatchedIDs  []string
		wantStatus      string
		wantInterrupted bool
	}{
		{name: "finalized turn refreshing its summary keeps completed status", finalized: true, seedStatus: db.StatusCompleted, wantStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := setupTestStore(t)
			threadID := "th-latch"
			msg := db.Message{ID: "msg-latch", ThreadID: threadID, Content: "hi", Status: db.StatusPending}
			if err := store.InsertMessage(ctx, msg); err != nil {
				t.Fatalf("insert: %v", err)
			}
			if err := store.UpdateMessageStatus(ctx, msg.ID, tc.seedStatus, ""); err != nil {
				t.Fatalf("seed status: %v", err)
			}
			turnCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			te := &turnExecution{threadID: threadID, burst: []db.Message{msg}, turnCtx: turnCtx, turnCancel: cancel}
			te.finalized.Store(tc.finalized)
			pool := &WorkerPool{ctx: ctx, cfg: WorkerPoolConfig{Store: store}, activeTurns: map[string]*turnExecution{db.DiscordSessionID(threadID): te}}

			pool.mu.Lock()
			active, latchedIDs := pool.latchActiveTurnsLocked(threadID)
			pool.mu.Unlock()
			if len(active) != 1 || active[0] != te {
				t.Errorf("latched active turns = %d, want the registered turn", len(active))
			}
			if !slices.Equal(latchedIDs, tc.wantLatchedIDs) {
				t.Errorf("latched burst IDs = %v, want %v", latchedIDs, tc.wantLatchedIDs)
			}
			if got := te.isInterrupted(); got != tc.wantInterrupted {
				t.Errorf("interrupted after latch = %t, want %t", got, tc.wantInterrupted)
			}

			pool.InterruptThread(ctx, threadID, false)
			if turnCtx.Err() == nil {
				t.Errorf("turn context not cancelled by interrupt")
			}
			got, err := store.GetMessage(ctx, msg.ID)
			if err != nil {
				t.Fatalf("get message: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("message status = %q, want %q", got.Status, tc.wantStatus)
			}
		})
	}
}

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
	_ = poolDefault.resolveChannelPolicy("ch-default", "default")
}
