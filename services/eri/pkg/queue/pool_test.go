package queue

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
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

type mockFullVoicePool struct {
	runner.AgentPool

	updatedModel  string
	prewarmed     []string
	updatedEngine string
	rescuerSet    bool
	managerSet    bool
	callbacksSet  bool
	getFn         func(context.Context, string) (runner.SessionRecord, error)
	saveFn        func(context.Context, string, string, string) error
}

func (m *mockFullVoicePool) UpdateModel(model string) {
	m.updatedModel = model
}

func (m *mockFullVoicePool) UpdatePrewarmedTargets(targets []string) {
	m.prewarmed = append([]string(nil), targets...)
}

func (m *mockFullVoicePool) UpdateEngine(engine, claudeBin string, extraBins ...string) {
	m.updatedEngine = engine
}

func (m *mockFullVoicePool) SetTranscriptRescuer(fn func(context.Context, string, time.Time) string) {
	m.rescuerSet = fn != nil
}

func (m *mockFullVoicePool) SetSessionManager(mgr *session.Manager) {
	m.managerSet = mgr != nil
}

func (m *mockFullVoicePool) SetSessionCallbacks(getFn func(context.Context, string) (runner.SessionRecord, error), saveFn func(context.Context, string, string, string) error) {
	m.callbacksSet = getFn != nil && saveFn != nil
	m.getFn = getFn
	m.saveFn = saveFn
}

func TestVoiceSessionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		targetKey string
		want      string
	}{
		{name: "empty key", targetKey: "", want: ""},
		{name: "whitespace key", targetKey: "   ", want: ""},
		{name: "bare target key", targetKey: "kiosk-1", want: "voice-kiosk-1"},
		{name: "already prefixed key", targetKey: "voice-kiosk-1", want: "voice-kiosk-1"},
		{name: "trimmed target key", targetKey: "  device-watch  ", want: "voice-device-watch"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := voiceSessionKey(tc.targetKey)
			if got != tc.want {
				t.Fatalf("voiceSessionKey(%q) = %q, want %q", tc.targetKey, got, tc.want)
			}
		})
	}
}

func TestWireVoicePoolSessionDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		inDiscordPools   bool
		withSessionMgr   bool
		withStore        bool
		wantRescuerSet   bool
		wantManagerSet   bool
		wantCallbacksSet bool
	}{
		{
			name:             "standalone voice pool receives rescuer, manager, and callbacks",
			withSessionMgr:   true,
			withStore:        true,
			wantRescuerSet:   true,
			wantManagerSet:   true,
			wantCallbacksSet: true,
		},
		{
			name:             "voice pool in discord pools receives manager and rescuer but skips voice callbacks",
			inDiscordPools:   true,
			withSessionMgr:   true,
			withStore:        true,
			wantRescuerSet:   true,
			wantManagerSet:   true,
			wantCallbacksSet: false,
		},
		{
			name:             "voice pool without store skips callbacks",
			withSessionMgr:   true,
			withStore:        false,
			wantRescuerSet:   true,
			wantManagerSet:   true,
			wantCallbacksSet: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			voiceMock := &mockFullVoicePool{}
			discordPools := make(map[PoolKey]runner.AgentPool)
			if tc.inDiscordPools {
				discordPools[PoolKey{Ephemeral: false, Effort: config.EffortHigh}] = voiceMock
			}

			var mgr *session.Manager
			if tc.withSessionMgr {
				mgr = &session.Manager{}
			}

			var testStore db.Store
			if tc.withStore {
				sqlStore, testDB := newTestStore(t)
				defer testDB.Close()
				testStore = sqlStore
			}

			wireVoicePoolSessionDependencies(voiceMock, discordPools, mgr, testStore)

			if voiceMock.rescuerSet != tc.wantRescuerSet {
				t.Errorf("rescuerSet = %v, want %v", voiceMock.rescuerSet, tc.wantRescuerSet)
			}
			if voiceMock.managerSet != tc.wantManagerSet {
				t.Errorf("managerSet = %v, want %v", voiceMock.managerSet, tc.wantManagerSet)
			}
			if voiceMock.callbacksSet != tc.wantCallbacksSet {
				t.Errorf("callbacksSet = %v, want %v", voiceMock.callbacksSet, tc.wantCallbacksSet)
			}
			if tc.wantCallbacksSet {
				assertWiredVoiceSessionCallbacks(t, voiceMock, testStore)
			}
		})
	}
}

func assertWiredVoiceSessionCallbacks(t *testing.T, voiceMock *mockFullVoicePool, testStore db.Store) {
	t.Helper()
	if voiceMock.getFn == nil || voiceMock.saveFn == nil {
		t.Fatal("expected non-nil callbacks on mock")
	}
	ctx := context.Background()
	if err := testStore.SaveSessionID(ctx, "voice-kiosk-1", "sess-init"); err != nil {
		t.Fatalf("seed session err: %v", err)
	}
	if err := testStore.RotateSessionID(ctx, "voice-kiosk-1", "sess-active"); err != nil {
		t.Fatalf("seed rotate err: %v", err)
	}
	if _, err := testStore.IncrementSessionTurnCount(ctx, "voice-kiosk-1"); err != nil {
		t.Fatalf("seed increment err: %v", err)
	}
	rec, err := voiceMock.getFn(ctx, "kiosk-1")
	if err != nil {
		t.Fatalf("getFn err: %v", err)
	}
	if rec.ActiveSessionID != "sess-active" || rec.PreviousSessionID != "sess-init" || rec.TurnCount != 1 {
		t.Fatalf("getFn returned unexpected record: %+v", rec)
	}
	if saveErr := voiceMock.saveFn(ctx, "kiosk-1", "sess-active", "sess-rotated"); saveErr != nil {
		t.Fatalf("saveFn err: %v", saveErr)
	}
	recAfter, err := voiceMock.getFn(ctx, "kiosk-1")
	if err != nil {
		t.Fatalf("getFn after rot err: %v", err)
	}
	if recAfter.ActiveSessionID != "sess-rotated" || recAfter.PreviousSessionID != "sess-active" || recAfter.TurnCount != 0 {
		t.Fatalf("getFn after rot returned unexpected record: %+v", recAfter)
	}

	// Attempting to rotate from stale oldSessionID must fail and preserve active session
	staleErr := voiceMock.saveFn(ctx, "kiosk-1", "sess-active", "sess-stale-attempt")
	if staleErr == nil {
		t.Fatal("expected saveFn to fail with stale oldSessionID")
	}
	recStillRotated, err := voiceMock.getFn(ctx, "kiosk-1")
	if err != nil {
		t.Fatalf("getFn after stale rot err: %v", err)
	}
	if recStillRotated.ActiveSessionID != "sess-rotated" {
		t.Fatalf("expected active session to stay sess-rotated, got %s", recStillRotated.ActiveSessionID)
	}
}

func TestMakeSessionCallbacks_GuardsSupersededRotation(t *testing.T) {
	t.Parallel()
	sqlStore, testDB := newTestStore(t)
	defer testDB.Close()
	getRec, onRot := makeSessionCallbacks(sqlStore)
	ctx := context.Background()

	if err := sqlStore.SaveSessionID(ctx, "thread-1", "sess-1"); err != nil {
		t.Fatalf("SaveSessionID failed: %v", err)
	}

	rec, err := getRec(ctx, "thread-1")
	if err != nil || rec.ActiveSessionID != "sess-1" {
		t.Fatalf("getRec = %+v, err = %v", rec, err)
	}

	// First rotation from sess-1 to sess-2 succeeds
	if rotErr := onRot(ctx, "thread-1", "sess-1", "sess-2"); rotErr != nil {
		t.Fatalf("onRot failed: %v", rotErr)
	}

	// Second rotation from stale sess-1 must fail and leave sess-2 intact
	staleErr := onRot(ctx, "thread-1", "sess-1", "sess-3")
	if staleErr == nil {
		t.Fatal("expected onRot to fail for superseded oldSessionID")
	}

	recAfter, errAfter := getRec(ctx, "thread-1")
	if errAfter != nil || recAfter.ActiveSessionID != "sess-2" {
		t.Fatalf("expected sess-2 intact, got %+v, err = %v", recAfter, errAfter)
	}
}

func TestReconcilePoolModelsAndVoiceTargets_Engine(t *testing.T) {
	t.Parallel()

	appCfg := config.NewFromData(&config.ConfigData{
		Model: "base-model",
		Voice: config.VoiceConfig{
			Engine: "gemini_api",
		},
	})

	pool := &WorkerPool{
		appCfg:            appCfg,
		hasExplicitAppCfg: true,
	}

	voiceMock := &mockFullVoicePool{}
	poolByKey := make(map[PoolKey]runner.AgentPool)
	for _, k := range AllValidPoolKeys() {
		poolByKey[k] = &mockFullVoicePool{}
	}

	pool.reconcilePoolModelsAndVoiceTargets(poolByKey, voiceMock, true, []string{"kiosk-1"})

	if voiceMock.updatedEngine != "gemini_api" {
		t.Fatalf("expected voiceMock.updatedEngine = %q, got %q", "gemini_api", voiceMock.updatedEngine)
	}

	appCfgClaude := config.NewFromData(&config.ConfigData{
		Engine: "claude",
		Model:  "base-model",
	})
	poolClaude := &WorkerPool{
		appCfg:            appCfgClaude,
		hasExplicitAppCfg: true,
	}
	voiceMockClaude := &mockFullVoicePool{}
	poolClaude.reconcilePoolModelsAndVoiceTargets(poolByKey, voiceMockClaude, true, []string{"kiosk-1"})
	if voiceMockClaude.updatedEngine != "claude" {
		t.Fatalf("expected voiceMockClaude.updatedEngine = %q, got %q", "claude", voiceMockClaude.updatedEngine)
	}
}

func assertPersistedVoiceTurn(t *testing.T, pool *WorkerPool, store db.Store, deviceKey, convID, key string) {
	t.Helper()
	gotSess, getErr := store.GetSessionID(context.Background(), key)
	if getErr != nil {
		t.Fatalf("unexpected GetSessionID error: %v", getErr)
	}
	turns, turnErr := store.GetSessionTurnCount(context.Background(), key)
	if turnErr != nil {
		t.Fatalf("unexpected GetSessionTurnCount error: %v", turnErr)
	}
	if gotSess != convID {
		t.Fatalf("session = %q, want %q", gotSess, convID)
	}
	if turns != 1 {
		t.Fatalf("turn count = %d, want 1", turns)
	}
	pool.persistVoiceTurnSession(context.Background(), deviceKey, convID)
	turns2, turnErr2 := store.GetSessionTurnCount(context.Background(), key)
	if turnErr2 != nil {
		t.Fatalf("unexpected GetSessionTurnCount error: %v", turnErr2)
	}
	if turns2 != 2 {
		t.Fatalf("turn count after 2nd turn = %d, want 2", turns2)
	}
}

func assertUnpersistedVoiceTurn(t *testing.T, store db.Store, key string) {
	t.Helper()
	gotSess, getErr := store.GetSessionID(context.Background(), key)
	if getErr != nil {
		t.Fatalf("unexpected GetSessionID error: %v", getErr)
	}
	if gotSess != "" {
		t.Fatalf("expected empty session, got %q", gotSess)
	}
	turns, turnErr := store.GetSessionTurnCount(context.Background(), key)
	if turnErr != nil {
		t.Fatalf("unexpected GetSessionTurnCount error: %v", turnErr)
	}
	if turns != 0 {
		t.Fatalf("turn count = %d, want 0", turns)
	}
}

func seedVoiceSessionTurns(t *testing.T, store db.Store, key, sessID string, turns int) {
	t.Helper()
	ctx := context.Background()
	if err := store.SaveSessionID(ctx, key, sessID); err != nil {
		t.Fatalf("seed session err: %v", err)
	}
	for range turns {
		if _, err := store.IncrementSessionTurnCount(ctx, key); err != nil {
			t.Fatalf("seed turns err: %v", err)
		}
	}
}

func assertVoiceSessionRotated(t *testing.T, store db.Store, key, wantConvID, wantPrevSession string) {
	t.Helper()
	ctx := context.Background()
	gotSess, err := store.GetSessionID(ctx, key)
	if err != nil {
		t.Fatalf("GetSessionID err: %v", err)
	}
	if gotSess != wantConvID {
		t.Fatalf("session = %q, want %q", gotSess, wantConvID)
	}
	gotPrev, err := store.GetPreviousSessionID(ctx, key)
	if err != nil {
		t.Fatalf("GetPreviousSessionID err: %v", err)
	}
	if gotPrev != wantPrevSession {
		t.Fatalf("prev session = %q, want %q", gotPrev, wantPrevSession)
	}
	gotTurns, err := store.GetSessionTurnCount(ctx, key)
	if err != nil {
		t.Fatalf("GetSessionTurnCount err: %v", err)
	}
	if gotTurns != 1 {
		t.Fatalf("turn count after rotation = %d, want 1", gotTurns)
	}
}

func assertVoiceSessionUnchanged(t *testing.T, store db.Store, key, wantSessID string, wantTurns int) {
	t.Helper()
	ctx := context.Background()
	gotSess, err := store.GetSessionID(ctx, key)
	if err != nil {
		t.Fatalf("GetSessionID err: %v", err)
	}
	if gotSess != wantSessID {
		t.Fatalf("session = %q, want %q", gotSess, wantSessID)
	}
	gotTurns, err := store.GetSessionTurnCount(ctx, key)
	if err != nil {
		t.Fatalf("GetSessionTurnCount err: %v", err)
	}
	if gotTurns != wantTurns {
		t.Fatalf("turns = %d, want %d", gotTurns, wantTurns)
	}
}

func TestWorkerPool_PersistVoiceTurnSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		deviceKey     string
		convID        string
		seedSession   string
		seedTurns     int
		wantPersisted bool
		wantRotated   bool
	}{
		{
			name:          "empty conv id is no-op",
			deviceKey:     "kiosk-living",
			convID:        "",
			wantPersisted: false,
		},
		{
			name:          "empty conv id with seeded session is complete no-op",
			deviceKey:     "kiosk-living-seeded",
			convID:        "",
			seedSession:   "sess-existing",
			seedTurns:     4,
			wantPersisted: false,
		},
		{
			name:          "saves session and increments turn count for bare device key",
			deviceKey:     "kiosk-kitchen",
			convID:        "conv-kitchen-1",
			wantPersisted: true,
		},
		{
			name:          "saves session and increments turn count for already-prefixed device key",
			deviceKey:     "voice-kiosk-bedroom",
			convID:        "conv-bedroom-1",
			wantPersisted: true,
		},
		{
			name:          "replacing conversation resets turn count and preserves previous session",
			deviceKey:     "kiosk-rotated",
			convID:        "conv-new",
			seedSession:   "conv-old",
			seedTurns:     8,
			wantPersisted: true,
			wantRotated:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sqlStore, testDB := newTestStore(t)
			defer testDB.Close()
			pool := &WorkerPool{
				cfg: WorkerPoolConfig{
					Store: sqlStore,
				},
			}
			key := voiceSessionKey(tc.deviceKey)
			if tc.seedSession != "" {
				seedVoiceSessionTurns(t, sqlStore, key, tc.seedSession, tc.seedTurns)
			}

			pool.persistVoiceTurnSession(context.Background(), tc.deviceKey, tc.convID)

			switch {
			case tc.wantRotated:
				assertVoiceSessionRotated(t, sqlStore, key, tc.convID, tc.seedSession)
			case tc.wantPersisted:
				assertPersistedVoiceTurn(t, pool, sqlStore, tc.deviceKey, tc.convID, key)
			case tc.seedSession != "":
				assertVoiceSessionUnchanged(t, sqlStore, key, tc.seedSession, tc.seedTurns)
			default:
				assertUnpersistedVoiceTurn(t, sqlStore, key)
			}
		})
	}
}

func TestWorkerPool_TwoPools_SupersededLeaseDoesNotChangeRecord(t *testing.T) {
	t.Parallel()
	sqlStore, testDB := newTestStore(t)
	defer testDB.Close()

	ctx := context.Background()
	const targetKey = "target-two-pools-cas"
	const initSessionID = "550e8400-e29b-41d4-a716-446655440001"

	if err := sqlStore.SaveSessionID(ctx, targetKey, initSessionID); err != nil {
		t.Fatalf("seed session failed: %v", err)
	}
	seedSessionTurns(t, sqlStore, targetKey, 2)

	var spawnCount atomic.Int32
	spawnStarted := make(chan struct{})
	proceedSpawn := make(chan struct{})

	spawner1 := &mockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			n := spawnCount.Add(1)
			if n == 2 {
				close(spawnStarted)
				select {
				case <-proceedSpawn:
				case <-ctx.Done():
					return nil, nil, nil, nil, ctx.Err()
				}
			}
			return spawnDefaultQueueMockPipes(cfg, false)
		},
	}

	acq1, get1, rot1 := makeSessionCallbacksCAS(sqlStore)
	pool1 := runner.NewUnifiedProcessPool(runner.PoolConfig{
		MaxSessionTurns:      2,
		AcquireSessionRecord: acq1,
		GetSessionRecord:     get1,
		OnSessionRotatedCAS:  rot1,
	}, spawner1)
	defer pool1.Close()

	spawner2 := newMockDaemonSpawner()
	acq2, get2, rot2 := makeSessionCallbacksCAS(sqlStore)
	pool2 := runner.NewUnifiedProcessPool(runner.PoolConfig{
		MaxSessionTurns:      2,
		AcquireSessionRecord: acq2,
		GetSessionRecord:     get2,
		OnSessionRotatedCAS:  rot2,
	}, spawner2)
	defer pool2.Close()

	// Pool 1 acquires lease at generation 1 and releases it, triggering background rotation.
	lease1, err := pool1.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("pool1 AcquireLease failed: %v", err)
	}
	if relErr := lease1.Release(); relErr != nil {
		t.Fatalf("pool1 Release failed: %v", relErr)
	}

	// Wait until Pool 1 is blocked spawning the replacement daemon for rotation.
	<-spawnStarted

	// Pool 2 (simulating a newer pod) acquires the same session key.
	// This increments lease_generation in the store, without changing conversation_id.
	lease2, err := pool2.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("pool2 AcquireLease failed: %v", err)
	}
	defer lease2.Release()

	// Unblock Pool 1's spawn. Pool 1 must detect that the store record's lease generation
	// was superseded, and must NOT overwrite the session record.
	close(proceedSpawn)

	// Close pool1 to wait for its background rotation goroutines to complete.
	if closeErr := pool1.Close(); closeErr != nil {
		t.Fatalf("pool1 Close failed: %v", closeErr)
	}

	// Verify the stored session was not changed by Pool 1's aborted rotation.
	info, err := sqlStore.GetSessionInfo(ctx, targetKey)
	if err != nil {
		t.Fatalf("GetSessionInfo failed: %v", err)
	}
	if info.InternalSessionID != initSessionID {
		t.Fatalf("session ID changed by superseded pool: got %q, want %q", info.InternalSessionID, initSessionID)
	}
	if info.TurnCount != 2 {
		t.Fatalf("turn count overwritten by superseded pool: got %d, want 2", info.TurnCount)
	}
}
