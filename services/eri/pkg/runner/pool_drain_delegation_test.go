package runner

import (
	"context"
	"errors"
	"testing"
)

type interchangeableDrainCase struct {
	name                string
	nilUnderlying       bool
	wantDraining        bool
	wantAcquireDrainErr bool
}

func runInterchangeableDrainCase(t *testing.T, tc interchangeableDrainCase) {
	t.Helper()
	var underlying *UnifiedProcessPool
	if !tc.nilUnderlying {
		underlying = NewUnifiedProcessPool(PoolConfig{}, nil)
		defer underlying.Close()
	}
	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{WorkerCount: 1})
	defer pool.Close()

	pool.BeginDrain()
	if pool.Draining() != tc.wantDraining {
		t.Errorf("Draining() = %v, want %v", pool.Draining(), tc.wantDraining)
	}
	if tc.wantAcquireDrainErr {
		if _, err := pool.AcquireLease(context.Background(), "drain-worker-target"); !errors.Is(err, ErrPoolDraining) {
			t.Errorf("expected ErrPoolDraining from AcquireLease during drain, got: %v", err)
		}
	}
}

func TestInterchangeablePool_BeginDrain_Suite(t *testing.T) {
	tests := []interchangeableDrainCase{
		{name: "drain delegates to underlying and refuses leases", wantDraining: true, wantAcquireDrainErr: true},
		{name: "nil underlying is safe", nilUnderlying: true, wantDraining: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runInterchangeableDrainCase(t, tc)
		})
	}
}

type dynamicDrainCase struct {
	name              string
	changeKey         bool
	wantActiveDrained bool
	wantNewDrained    bool
}

func runDynamicDrainCase(t *testing.T, tc dynamicDrainCase) {
	t.Helper()
	fresh := func() *UnifiedProcessPool {
		return NewUnifiedProcessPool(PoolConfig{}, nil)
	}
	initial := fresh()
	factory := func() (AgentPool, string, error) {
		if tc.changeKey {
			return fresh(), "voice-next", nil
		}
		return fresh(), "voice-static", nil
	}
	pool := NewDynamicVoicePoolWithInitial(initial, "voice-static", factory)
	defer pool.Close()

	pool.BeginDrain()
	if !pool.Draining() {
		t.Fatalf("expected wrapper to report draining after BeginDrain")
	}
	if initial.Draining() != tc.wantActiveDrained {
		t.Errorf("active pool Draining() = %v, want %v", initial.Draining(), tc.wantActiveDrained)
	}

	pool.MarkDirty()
	if tc.wantNewDrained {
		active, err := pool.activePool()
		if err != nil {
			t.Fatalf("activePool failed: %v", err)
		}
		d, ok := active.(interface{ Draining() bool })
		if !ok || !d.Draining() {
			t.Errorf("expected adopted candidate pool to be draining")
		}
	}
}

func TestDynamicVoicePool_BeginDrain_Suite(t *testing.T) {
	tests := []dynamicDrainCase{
		{name: "drain delegates to active pool", wantActiveDrained: true},
		{name: "drain propagates to reloaded candidate", changeKey: true, wantActiveDrained: true, wantNewDrained: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDynamicDrainCase(t, tc)
		})
	}
}
