package queue

import (
	"context"
	"sync"
)

// maxTurnTombstones bounds the stopped-turn memory; the oldest tombstones are forgotten first.
// A tombstone only has to outlive the replay of the turn it fences, which is minutes.
const maxTurnTombstones = 4096

// TurnRegistry tracks the TurnRunner attempts executing on this pod by turn ID. Begin serializes
// closures of the same turn (a replayed run step waits out an orphaned closure of the same turn)
// and Stop fences a turn for good: it interrupts the live execution and tombstones the turn ID so
// no later Begin of it can start work. Begin and Stop share one mutex, so a closure can never
// register after Stop returned.
type TurnRegistry struct {
	mu         sync.Mutex
	live       map[string]*turnRegistration
	tombstones map[string]string
	tombOrder  []string
	closeExec  func(te *turnExecution, reason string)
}

type turnRegistration struct {
	te   *turnExecution
	done chan struct{}
}

// NewTurnRegistry builds a registry; closeExec cancels a stopped execution's turn context and
// terminates its daemon; it must not be nil.
func NewTurnRegistry(closeExec func(te *turnExecution, reason string)) *TurnRegistry {
	return &TurnRegistry{
		live:       make(map[string]*turnRegistration),
		tombstones: make(map[string]string),
		closeExec:  closeExec,
	}
}

// Begin registers te as the live execution of turnID. It waits for any live registration of the
// same turn on this pod to be released first, and refuses (ok=false) once turnID is tombstoned or
// ctx ends while waiting. release is idempotent.
func (r *TurnRegistry) Begin(ctx context.Context, turnID string, te *turnExecution) (release func(), ok bool) {
	for {
		r.mu.Lock()
		if _, stopped := r.tombstones[turnID]; stopped {
			r.mu.Unlock()
			return nil, false
		}
		prev := r.live[turnID]
		if prev == nil {
			reg := &turnRegistration{te: te, done: make(chan struct{})}
			r.live[turnID] = reg
			r.mu.Unlock()
			var once sync.Once
			return func() { once.Do(func() { r.release(turnID, reg) }) }, true
		}
		r.mu.Unlock()
		select {
		case <-prev.done:
		case <-ctx.Done():
			return nil, false
		}
	}
}

func (r *TurnRegistry) release(turnID string, reg *turnRegistration) {
	r.mu.Lock()
	if r.live[turnID] == reg {
		delete(r.live, turnID)
	}
	r.mu.Unlock()
	close(reg.done)
}

// Stop tombstones turnID and interrupts its live execution, if any: the execution is marked
// interrupted with reason, its turn context is cancelled and its daemon closed. It reports whether
// a live execution was interrupted.
func (r *TurnRegistry) Stop(turnID, reason string) bool {
	r.mu.Lock()
	if _, stopped := r.tombstones[turnID]; !stopped {
		r.tombstones[turnID] = reason
		r.tombOrder = append(r.tombOrder, turnID)
		if len(r.tombOrder) > maxTurnTombstones {
			delete(r.tombstones, r.tombOrder[0])
			r.tombOrder = r.tombOrder[1:]
		}
	}
	var te *turnExecution
	if reg := r.live[turnID]; reg != nil {
		te = reg.te
		te.interrupted.Store(true)
		te.setInterruptReason(reason)
	}
	r.mu.Unlock()
	if te == nil {
		return false
	}
	r.closeExec(te, reason)
	return true
}

// Tombstoned reports whether turnID was stopped on this pod.
func (r *TurnRegistry) Tombstoned(turnID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, stopped := r.tombstones[turnID]
	return stopped
}

// LiveExecution returns the live turnExecution registered for turnID, if any.
func (r *TurnRegistry) LiveExecution(turnID string) *turnExecution {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if reg := r.live[turnID]; reg != nil {
		return reg.te
	}
	return nil
}

// FindExecution returns the first live turnExecution satisfying predicate, or nil.
func (r *TurnRegistry) FindExecution(predicate func(te *turnExecution) bool) *turnExecution {
	if r == nil || predicate == nil {
		return nil
	}
	r.mu.Lock()
	candidates := make([]*turnExecution, 0, len(r.live))
	for _, reg := range r.live {
		if reg != nil && reg.te != nil {
			candidates = append(candidates, reg.te)
		}
	}
	r.mu.Unlock()
	for _, te := range candidates {
		if predicate(te) {
			return te
		}
	}
	return nil
}

// Turns returns the pool's TurnRegistry.
func (p *WorkerPool) Turns() *TurnRegistry {
	return p.turns
}
