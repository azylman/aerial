package queue

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
)

func TestTurnRegistry(t *testing.T) {
	t.Parallel()
	// ops: begin (fresh context), beginCancelled (context already done), beginWait (Begin in the
	// background, collected by awaitWait), release (latest unreleased registration), stop, and
	// stopFlood (tombstones maxTurnTombstones other turns).
	tests := []struct {
		name            string
		ops             []string
		wantOK          []bool
		wantStop        []bool
		wantInterrupted bool
		wantClosed      int32
	}{
		{name: "FreshBegin", ops: []string{"begin"}, wantOK: []bool{true}},
		{name: "StopBeforeBegin", ops: []string{"stop", "begin"}, wantOK: []bool{false}, wantStop: []bool{false}},
		{name: "BeginAfterStop", ops: []string{"begin", "release", "stop", "begin"}, wantOK: []bool{true, false}, wantStop: []bool{false}},
		{name: "StopLiveInterrupts", ops: []string{"begin", "stop", "begin"}, wantOK: []bool{true, false}, wantStop: []bool{true}, wantInterrupted: true, wantClosed: 1},
		{name: "SecondBeginWaitsForRelease", ops: []string{"begin", "beginWait", "release", "awaitWait"}, wantOK: []bool{true, true}},
		{name: "WaitingBeginRefusedByStop", ops: []string{"begin", "beginWait", "stop", "release", "awaitWait"}, wantOK: []bool{true, false}, wantStop: []bool{true}, wantInterrupted: true, wantClosed: 1},
		{name: "WaitingBeginGivesUpOnContext", ops: []string{"begin", "beginCancelled"}, wantOK: []bool{true, false}},
		{name: "OldestTombstoneForgottenPastCap", ops: []string{"stop", "stopFlood", "begin"}, wantOK: []bool{true}, wantStop: []bool{false}},
		{name: "ReleaseIsIdempotent", ops: []string{"begin", "release", "release", "begin"}, wantOK: []bool{true, true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRegistryHarness(t)
			for _, op := range tc.ops {
				h.apply(op)
			}
			gotOK, gotStop, execs := h.gotOK, h.gotStop, h.execs
			closed := &h.closed
			if !slices.Equal(gotOK, tc.wantOK) || !slices.Equal(gotStop, tc.wantStop) {
				t.Errorf("Begin results = %v, Stop results = %v; want %v, %v", gotOK, gotStop, tc.wantOK, tc.wantStop)
			}
			if got := closed.Load(); got != tc.wantClosed {
				t.Errorf("closeExec calls = %d, want %d", got, tc.wantClosed)
			}
			if len(execs) == 0 {
				return
			}
			first := execs[0]
			if first.isInterrupted() != tc.wantInterrupted || (first.turnCtx.Err() != nil) != tc.wantInterrupted {
				t.Errorf("first execution interrupted=%v ctxErr=%v, want interrupted=%v", first.isInterrupted(), first.turnCtx.Err(), tc.wantInterrupted)
			}
			if tc.wantInterrupted && first.getInterruptReason() != "operator stop" {
				t.Errorf("interrupt reason = %q, want %q", first.getInterruptReason(), "operator stop")
			}
		})
	}
}

// registryHarness applies TestTurnRegistry ops to one registry and records their results.
type registryHarness struct {
	t        *testing.T
	reg      *TurnRegistry
	closed   atomic.Int32
	execs    []*turnExecution
	releases []func()
	gotOK    []bool
	gotStop  []bool
	waitCh   chan bool
}

func newRegistryHarness(t *testing.T) *registryHarness {
	t.Helper()
	h := &registryHarness{t: t, waitCh: make(chan bool, 1)}
	h.reg = NewTurnRegistry(func(te *turnExecution, _ string) {
		h.closed.Add(1)
		te.cancelTurn()
	})
	return h
}

func (h *registryHarness) newExec() *turnExecution {
	ctx, cancel := context.WithCancel(context.Background())
	h.t.Cleanup(cancel)
	te := &turnExecution{turnCtx: ctx, turnCancel: cancel}
	h.execs = append(h.execs, te)
	return te
}

func (h *registryHarness) record(release func(), ok bool) {
	h.gotOK = append(h.gotOK, ok)
	if ok {
		h.releases = append(h.releases, release)
	}
}

func (h *registryHarness) apply(op string) {
	switch op {
	case "begin":
		h.record(h.reg.Begin(context.Background(), "turn-1", h.newExec()))
	case "beginCancelled":
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		h.record(h.reg.Begin(ctx, "turn-1", h.newExec()))
	case "beginWait":
		te := h.newExec()
		go func() {
			release, ok := h.reg.Begin(context.Background(), "turn-1", te)
			if ok {
				release()
			}
			h.waitCh <- ok
		}()
	case "awaitWait":
		h.gotOK = append(h.gotOK, <-h.waitCh)
	case "release":
		h.releases[len(h.releases)-1]()
	case "stop":
		h.gotStop = append(h.gotStop, h.reg.Stop("turn-1", "operator stop"))
	case "stopFlood":
		for i := range maxTurnTombstones {
			h.reg.Stop(fmt.Sprintf("other-%d", i), "flood")
		}
	default:
		h.t.Fatalf("unknown op %q", op)
	}
}
