package runner

import (
	"context"
	"testing"
	"time"
)

func TestUnifiedProcessPool_EvictAndRescuerCoverage(t *testing.T) {
	t.Parallel()

	var nilUnified *UnifiedProcessPool
	if err := nilUnified.EvictSession("target-1"); err != nil {
		t.Errorf("expected nil error on nil UnifiedProcessPool.EvictSession")
	}

	mock := createTestSpawner()
	uPool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPool.Close()

	if err := uPool.EvictSession("nonexistent"); err != nil {
		t.Errorf("expected nil error on nonexistent target eviction: %v", err)
	}

	sess, err := uPool.GetOrCreate(context.Background(), "target-evict-live", "")
	if err != nil || sess == nil {
		t.Fatalf("unexpected error spawning daemon: %v", err)
	}

	if err := uPool.EvictSession("target-evict-live"); err != nil || uPool.HasDaemon("target-evict-live") {
		t.Errorf("unexpected error evicting active daemon: %v", err)
	}

	var nilPool *UnifiedProcessPool
	nilPool.SetTranscriptRescuer(func(convID string, since time.Time) string { return "" })

	uPoolRescuer := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPoolRescuer.Close()
	_, _ = uPoolRescuer.GetOrCreate(context.Background(), "target-rescuer", "")

	rescuerCalled := false
	uPoolRescuer.SetTranscriptRescuer(func(convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})
	if uPoolRescuer.cfg.TranscriptRescuer == nil || !rescuerCalled && uPoolRescuer.cfg.TranscriptRescuer("conv-1", time.Now()) != "rescued" || !rescuerCalled {
		t.Errorf("expected rescuer callback to be set and invoked")
	}
}

func TestIsDaemonMatch_Comprehensive(t *testing.T) {
	t.Parallel()
	dClosed, dDirty := &StreamingDaemon{state: StateClosed}, &StreamingDaemon{dirty: true, state: StateReady}
	dReady, dEmpty := &StreamingDaemon{state: StateReady, sessionID: "sess-abc"}, &StreamingDaemon{state: StateReady, sessionID: ""}

	for _, tc := range []struct {
		d    *StreamingDaemon
		sess string
		want bool
	}{
		{nil, "", false},
		{dClosed, "", false},
		{dDirty, "", false},
		{dReady, "", true},
		{dEmpty, "sess-target", true},
		{dReady, "sess-abc", true},
		{dReady, "sess-xyz", false},
	} {
		if got := isDaemonMatch(tc.d, tc.sess); got != tc.want {
			t.Errorf("isDaemonMatch(%v, %q) = %v, want %v", tc.d, tc.sess, got, tc.want)
		}
	}
}
