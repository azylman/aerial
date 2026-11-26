package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

type poolDrainCase struct {
	name               string
	seedIdle           bool
	holdLease          bool
	wantAcquireErr     bool
	wantEphemeralErr   bool
	wantIdleEvicted    bool
	wantReleaseEvicts  bool
	wantRotatedSkipped bool
	wantGetOrCreateOK  bool
}

func seedDrainIdleDaemon(ctx context.Context, t *testing.T, pool *UnifiedProcessPool, target, sessID string) {
	t.Helper()
	if _, err := pool.GetOrCreate(ctx, target, sessID, 5); err != nil {
		t.Fatalf("seed GetOrCreate failed: %v", err)
	}
	if !poolHasDaemon(pool, target) {
		t.Fatalf("expected seeded idle daemon to exist")
	}
}

func checkDrainRefusals(ctx context.Context, t *testing.T, pool *UnifiedProcessPool, sessID string, tc poolDrainCase) {
	t.Helper()
	if tc.wantAcquireErr {
		if _, err := pool.AcquireLease(ctx, "drain-new-target"); !errors.Is(err, ErrPoolDraining) {
			t.Errorf("expected ErrPoolDraining from AcquireLease during drain, got: %v", err)
		}
	}
	if tc.wantEphemeralErr {
		if _, err := pool.ExecuteEphemeral(ctx, "drain-ephemeral-target", "hi"); !errors.Is(err, ErrPoolDraining) {
			t.Errorf("expected ErrPoolDraining from ExecuteEphemeral during drain, got: %v", err)
		}
	}
	if tc.wantGetOrCreateOK {
		if _, err := pool.GetOrCreate(ctx, "drain-held-target", sessID, 5); err != nil {
			t.Errorf("expected GetOrCreate to still succeed during drain, got: %v", err)
		}
	}
}

func checkDrainReleaseEvicts(t *testing.T, pool *UnifiedProcessPool, lease SessionLease, target string, rotated bool, tc poolDrainCase) {
	t.Helper()
	if !tc.wantReleaseEvicts {
		return
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release during drain failed: %v", err)
	}
	waitPoolBackground(pool)
	if poolHasDaemon(pool, target) {
		t.Errorf("expected held daemon to be evicted on Release during drain")
	}
	if tc.wantRotatedSkipped && rotated {
		t.Errorf("expected OnSessionRotated NOT to be called on Release during drain")
	}
}

func runPoolDrainCase(t *testing.T, tc poolDrainCase) {
	t.Helper()
	ctx := context.Background()
	pinnedSessID := uuid.New().String()
	var rotated bool
	pool := NewUnifiedProcessPool(PoolConfig{
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			return SessionRecord{ActiveSessionID: pinnedSessID, TurnCount: 5}, nil
		},
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotated = true
			return nil
		},
	}, newPipeMockSpawner(pipeMockSpawnerOpts{pid: 351, initSessID: pinnedSessID, echoRes: "ok"}))
	defer pool.Close()

	const idleTarget = "drain-idle-target"
	const heldTarget = "drain-held-target"
	if tc.seedIdle {
		seedDrainIdleDaemon(ctx, t, pool, idleTarget, pinnedSessID)
	}

	var lease SessionLease
	if tc.holdLease {
		var err error
		lease, err = pool.AcquireLease(ctx, heldTarget)
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		defer lease.Release()
	}

	pool.BeginDrain()
	pool.BeginDrain()
	if !pool.Draining() {
		t.Fatalf("expected pool to report draining after BeginDrain")
	}
	waitPoolBackground(pool)

	if tc.wantIdleEvicted && poolHasDaemon(pool, idleTarget) {
		t.Errorf("expected idle daemon %q to be evicted by BeginDrain", idleTarget)
	}
	checkDrainRefusals(ctx, t, pool, pinnedSessID, tc)
	checkDrainReleaseEvicts(t, pool, lease, heldTarget, rotated, tc)
}

func TestUnifiedProcessPool_BeginDrain_Suite(t *testing.T) {
	tests := []poolDrainCase{
		{
			name:     "drain evicts idle and refuses new leases and spawns",
			seedIdle: true, wantAcquireErr: true, wantEphemeralErr: true, wantIdleEvicted: true,
		},
		{
			name:      "drain release evicts without rotation callback",
			holdLease: true, wantReleaseEvicts: true, wantRotatedSkipped: true,
		},
		{
			name:      "drain keeps getorcreate for in-flight turns",
			holdLease: true, wantAcquireErr: true, wantGetOrCreateOK: true,
			wantReleaseEvicts: true, wantRotatedSkipped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPoolDrainCase(t, tc)
		})
	}
}

type drainColdCase struct {
	name       string
	turnCount  int
	recordSess string
	daemonSess string
	wantCold   bool
}

func runDrainColdCase(t *testing.T, tc drainColdCase) {
	t.Helper()
	if got := isLeaseCold(tc.turnCount, tc.recordSess, tc.daemonSess); got != tc.wantCold {
		t.Errorf("isLeaseCold(turns=%d record=%q daemon=%q) = %v, want %v",
			tc.turnCount, tc.recordSess, tc.daemonSess, got, tc.wantCold)
	}
}

func TestUnifiedProcessPool_AcquireLease_DrainColdCheck_Suite(t *testing.T) {
	tests := []drainColdCase{
		{name: "matching session stays warm", turnCount: 5, recordSess: "sess-record-1", daemonSess: "sess-record-1", wantCold: false},
		{name: "empty daemon session falls back to record", turnCount: 5, recordSess: "sess-record-1", daemonSess: "", wantCold: false},
		{name: "mismatched daemon session is cold", turnCount: 5, recordSess: "sess-record-1", daemonSess: "sess-daemon-2", wantCold: true},
		{name: "empty record session is cold", turnCount: 5, recordSess: "", daemonSess: "sess-daemon-2", wantCold: true},
		{name: "first turn is cold", turnCount: 1, recordSess: "sess-record-1", daemonSess: "sess-record-1", wantCold: true},
		{name: "zero turns is cold", turnCount: 0, recordSess: "sess-record-1", daemonSess: "sess-record-1", wantCold: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDrainColdCase(t, tc)
		})
	}
}

func TestUnifiedProcessPool_DrainDuringBackgroundRotation(t *testing.T) {
	ctx := context.Background()
	spawnStarted := make(chan struct{})
	proceedSpawn := make(chan struct{})
	var spawnCount atomic.Int32
	var onRotatedCalled atomic.Bool

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:        352,
		initSessID: uuid.New().String(),
		echoRes:    "ok",
		onSpawn: func(cfg DaemonConfig) {
			if spawnCount.Add(1) == 2 {
				close(spawnStarted)
				<-proceedSpawn
			}
		},
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		MaxSessionTurns: 1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			onRotatedCalled.Store(true)
			return nil
		},
	}, mock)
	defer pool.Close()

	const targetKey = "drain-bg-target"
	lease, err := pool.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}

	if _, err := lease.Execute(ctx, &TurnContext{TurnID: "t1", Prompt: "p1", Ctx: ctx}); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Release triggers background rotation (spawnCount reaches 2)
	if err := lease.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	<-spawnStarted
	// While spawner is blocked in RotateDaemon during background rotation, begin drain:
	pool.BeginDrain()
	if !pool.Draining() {
		t.Fatalf("expected pool to be draining")
	}

	// Unblock spawn and wait for background upkeep to settle
	close(proceedSpawn)
	waitPoolBackground(pool)

	if poolHasDaemon(pool, targetKey) {
		t.Errorf("expected daemon to be evicted when pool drained during background rotation")
	}
	if onRotatedCalled.Load() {
		t.Errorf("expected OnSessionRotated NOT to be called when pool drains during background rotation")
	}
}
