//go:build restateintegration

package restatetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// The cancel spike proves the TurnRunner cancel design against a real Restate
// server: a coordinator Virtual Object cancels a runner invocation whose agent
// work runs inside restate.RunAsync("run-1"); the runner observes the
// cancellation as a terminal error at the await, runs stop-turn (tombstone and
// kill the work) and reports OnDone exactly once, including when the endpoint
// crashes while stop-turn is in flight.

const (
	spikeCoordinator = "SpikeCoordinator"
	spikeRunner      = "SpikeRunner"
	spikeWait        = 45 * time.Second
	codeCancelled    = 409
)

var (
	errTombstoned = errors.New("turn tombstoned")
	// errKilled is what a run closure interrupted by stop-turn returns: a
	// terminal 412 (never 409, which is Restate's own cancel). The handler is
	// already past the await, so the late run-1 proposal is just recorded. A
	// retryable error here instead aborts the current attempt, and when that
	// proposal lands before stop-turn's completion is journaled, stop-turn
	// runs twice (observed in this spike).
	errKilled = restate.ToTerminalError(errors.New("work killed by stop-turn"), restate.WithErrorCode(412))
)

// workRegistry is the in-test stand-in for TurnRegistry: work handles keyed by
// turn ID, a tombstone that refuses new registrations once stop-turn ran, and
// a wait for any orphaned closure of the same turn before registering anew.
type workRegistry struct {
	mu         sync.Mutex
	handles    map[string]map[*workHandle]struct{}
	tombstoned map[string]bool
	changed    chan struct{}
}

type workHandle struct {
	killed chan struct{}
	once   sync.Once
}

func newWorkRegistry() *workRegistry {
	return &workRegistry{
		handles:    map[string]map[*workHandle]struct{}{},
		tombstoned: map[string]bool{},
		changed:    make(chan struct{}),
	}
}

func (r *workRegistry) broadcastLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

// Begin registers a work handle for turnID, waiting out any live handle of the
// same turn first; it is refused once the turn is tombstoned.
func (r *workRegistry) Begin(ctx context.Context, turnID string) (*workHandle, error) {
	for {
		r.mu.Lock()
		if r.tombstoned[turnID] {
			r.mu.Unlock()
			return nil, errTombstoned
		}
		if len(r.handles[turnID]) == 0 {
			h := &workHandle{killed: make(chan struct{})}
			r.handles[turnID] = map[*workHandle]struct{}{h: {}}
			r.broadcastLocked()
			r.mu.Unlock()
			return h, nil
		}
		wait := r.changed
		r.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Release drops a handle whose work ended on its own (stream drop, abort).
func (r *workRegistry) Release(turnID string, h *workHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h.once.Do(func() { close(h.killed) })
	delete(r.handles[turnID], h)
	r.broadcastLocked()
}

// Stop tombstones turnID and kills every live handle; returns how many it killed.
func (r *workRegistry) Stop(turnID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tombstoned[turnID] = true
	n := 0
	for h := range r.handles[turnID] {
		h.once.Do(func() { close(h.killed) })
		n++
	}
	delete(r.handles, turnID)
	r.broadcastLocked()
	return n
}

func (r *workRegistry) Alive(turnID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.handles[turnID]) > 0
}

// spikeEnv is shared by every endpoint instance of one case, like process
// state that survives in-pod (the registry) plus counters the test observes.
type spikeEnv struct {
	reg             *workRegistry
	stopTurnCrashes int32

	attempts       atomic.Int32
	runEntries     atomic.Int32
	stopEntries    atomic.Int32
	stopCompleted  atomic.Int32
	stopKilled     atomic.Int32
	stopEnteredSig chan struct{}
	stopSigOnce    sync.Once

	mu    sync.Mutex
	notes []string
}

func newSpikeEnv(stopTurnCrashes int) *spikeEnv {
	return &spikeEnv{reg: newWorkRegistry(), stopTurnCrashes: int32(stopTurnCrashes), stopEnteredSig: make(chan struct{})}
}

func (e *spikeEnv) notef(format string, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.notes = append(e.notes, fmt.Sprintf(format, args...))
}

func (e *spikeEnv) observations() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.notes...)
}

type spikeRunRequest struct {
	Key    string `json:"key"`
	TurnID string `json:"turn_id"`
}

type spikeDone struct {
	TurnID  string `json:"turn_id"`
	Outcome string `json:"outcome"`
}

type spikeStatus struct {
	RunnerInvocationID string      `json:"runner_invocation_id"`
	Done               []spikeDone `json:"done"`
}

// SpikeCoordinator mirrors the coordinator: it starts the runner, stores its
// invocation ID, cancels it via restate.CancelInvocation and records OnDone.
type SpikeCoordinator struct{}

func (SpikeCoordinator) Start(ctx restate.ObjectContext, turnID string) (string, error) {
	inv := restate.ServiceSend(ctx, spikeRunner, "Run").Send(spikeRunRequest{Key: restate.Key(ctx), TurnID: turnID})
	id := inv.GetInvocationId()
	restate.Set(ctx, "runner", id)
	return id, nil
}

func (SpikeCoordinator) Cancel(ctx restate.ObjectContext) error {
	id, err := restate.Get[string](ctx, "runner")
	if err != nil {
		return err
	}
	if id == "" {
		return restate.ToTerminalError(errors.New("no runner"), restate.WithErrorCode(412))
	}
	restate.CancelInvocation(ctx, id)
	return nil
}

func (SpikeCoordinator) OnDone(ctx restate.ObjectContext, done spikeDone) error {
	got, err := restate.Get[[]spikeDone](ctx, "done")
	if err != nil {
		return err
	}
	restate.Set(ctx, "done", append(got, done))
	return nil
}

func (SpikeCoordinator) Status(ctx restate.ObjectSharedContext) (spikeStatus, error) {
	id, err := restate.Get[string](ctx, "runner")
	if err != nil {
		return spikeStatus{}, err
	}
	done, err := restate.Get[[]spikeDone](ctx, "done")
	if err != nil {
		return spikeStatus{}, err
	}
	return spikeStatus{RunnerInvocationID: id, Done: done}, nil
}

// SpikeRunner has the TurnRunner shape: one RunAsync attempt awaited, a
// stop-turn step on a terminal (cancel) error, then OnDone on every path.
type SpikeRunner struct{ env *spikeEnv }

func (s SpikeRunner) Run(ctx restate.Context, req spikeRunRequest) (string, error) {
	env := s.env
	env.notef("runner attempt %d started", env.attempts.Add(1))
	fut := restate.RunAsync(ctx, func(rc restate.RunContext) (string, error) {
		entry := env.runEntries.Add(1)
		h, err := env.reg.Begin(rc, req.TurnID)
		if err != nil {
			env.notef("run-1 entry %d: registration refused: %v", entry, err)
			return "", err
		}
		env.notef("run-1 entry %d: work registered", entry)
		select {
		case <-h.killed:
			env.notef("run-1 entry %d: work killed by stop-turn (run context cancelled=%t)", entry, rc.Err() != nil)
			return "", errKilled
		case <-rc.Done():
			env.reg.Release(req.TurnID, h)
			env.notef("run-1 entry %d: run context cancelled (%v); work released", entry, rc.Err())
			return "", rc.Err()
		}
	}, restate.WithName("run-1"))
	out, err := fut.Result()
	if err == nil {
		restate.ObjectSend(ctx, spikeCoordinator, req.Key, "OnDone").Send(spikeDone{TurnID: req.TurnID, Outcome: "completed"})
		return out, nil
	}
	outcome := "failed"
	if err.Code() == codeCancelled {
		outcome = "cancelled"
	}
	env.notef("run-1 await returned terminal error code %d (%v)", err.Code(), err)

	killed, serr := restate.Run(ctx, func(rc restate.RunContext) (int, error) {
		entry := env.stopEntries.Add(1)
		env.stopSigOnce.Do(func() { close(env.stopEnteredSig) })
		if entry <= env.stopTurnCrashes {
			// Crash window: the test restarts the endpoint while stop-turn is in
			// flight; the dropped stream cancels this run context.
			<-rc.Done()
			env.notef("stop-turn entry %d: crashed mid-step (%v)", entry, rc.Err())
			return 0, rc.Err()
		}
		n := env.reg.Stop(req.TurnID)
		env.stopKilled.Add(int32(n))
		env.stopCompleted.Add(1)
		env.notef("stop-turn entry %d: tombstoned turn, killed %d work handle(s)", entry, n)
		return n, nil
	}, restate.WithName("stop-turn"))
	if serr != nil {
		return "", serr
	}
	env.notef("stop-turn result replayed/journaled: killed=%d", killed)
	restate.ObjectSend(ctx, spikeCoordinator, req.Key, "OnDone").Send(spikeDone{TurnID: req.TurnID, Outcome: outcome})
	return "", err
}

func spikeDefs(env *spikeEnv) []restate.ServiceDefinition {
	return []restate.ServiceDefinition{
		restate.Reflect(SpikeCoordinator{}),
		restate.Reflect(SpikeRunner{env: env}),
	}
}

type cancelSpikeCase struct {
	name             string
	stopTurnCrashes  int
	wantAttachStatus int
	wantStopTurnRuns int32
	wantOnDone       []spikeDone
}

func TestCancelSpike(t *testing.T) {
	t.Parallel()
	tests := []cancelSpikeCase{
		{
			name:             "cancel during run",
			stopTurnCrashes:  0,
			wantAttachStatus: codeCancelled,
			wantStopTurnRuns: 1,
			wantOnDone:       []spikeDone{{TurnID: "turn-a", Outcome: "cancelled"}},
		},
		{
			name:             "crash right after cancel",
			stopTurnCrashes:  1,
			wantAttachStatus: codeCancelled,
			wantStopTurnRuns: 1,
			wantOnDone:       []spikeDone{{TurnID: "turn-b", Outcome: "cancelled"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCancelSpikeCase(t, tc)
		})
	}
}

func runCancelSpikeCase(t *testing.T, tc cancelSpikeCase) {
	t.Helper()
	env := newSpikeEnv(tc.stopTurnCrashes)
	h := Start(t, spikeDefs(env)...)
	key := strings.ReplaceAll(tc.name, " ", "-")
	turnID := tc.wantOnDone[0].TurnID

	var runnerID string
	if st := h.Call(spikeCoordinator, key, "Start", turnID, &runnerID); st != 200 {
		t.Fatalf("Start status = %d", st)
	}
	h.Eventually("run-1 work registered", spikeWait, func() bool { return env.reg.Alive(turnID) })

	if st := h.Call(spikeCoordinator, key, "Cancel", nil, nil); st != 200 {
		t.Fatalf("Cancel status = %d", st)
	}
	if tc.stopTurnCrashes > 0 {
		select {
		case <-env.stopEnteredSig:
		case <-time.After(spikeWait):
			t.Fatal("stop-turn never entered")
		}
		h.RestartEndpoint(spikeDefs(env)...)
	}

	res := h.Attach(runnerID)
	if res.Status != tc.wantAttachStatus {
		t.Errorf("runner attach status = %d (%s), want %d", res.Status, res.Body, tc.wantAttachStatus)
	}
	if env.reg.Alive(turnID) {
		t.Errorf("work handle still alive after stop-turn completed")
	}
	var st spikeStatus
	h.Eventually("an OnDone delivery", spikeWait, func() bool {
		st = spikeStatus{}
		return h.Call(spikeCoordinator, key, "Status", nil, &st) == 200 && len(st.Done) > 0
	})
	h.Eventually("every invocation completed", spikeWait, func() bool {
		rows := h.Query("SELECT count(*) AS n FROM sys_invocation WHERE status != 'completed'")
		return len(rows) == 1 && fmt.Sprint(rows[0]["n"]) == "0"
	})
	st = spikeStatus{}
	if code := h.Call(spikeCoordinator, key, "Status", nil, &st); code != 200 {
		t.Fatalf("Status status = %d", code)
	}
	t.Logf("runner invocation: %v", h.Query("SELECT target, status, completion_result, completion_failure FROM sys_invocation WHERE id = '"+runnerID+"'"))
	if got := env.stopCompleted.Load(); got != tc.wantStopTurnRuns {
		t.Errorf("stop-turn completions = %d, want %d", got, tc.wantStopTurnRuns)
	}
	if fmt.Sprint(st.Done) != fmt.Sprint(tc.wantOnDone) {
		t.Errorf("OnDone deliveries = %v, want %v", st.Done, tc.wantOnDone)
	}
	if env.reg.Alive(turnID) {
		t.Errorf("work handle alive at end of case")
	}
	t.Logf("observations: runner attempts=%d, run-1 closure entries=%d (re-entered on replay=%t), stop-turn entries=%d, stop-turn killed=%d",
		env.attempts.Load(), env.runEntries.Load(), env.runEntries.Load() > 1, env.stopEntries.Load(), env.stopKilled.Load())
	for _, n := range env.observations() {
		t.Logf("  %s", n)
	}
}
