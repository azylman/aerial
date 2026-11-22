package prstate

// Test-only in-memory stores and no-op boundaries. No production path
// constructs these: live wiring binds every dependency to Restate K/V,
// sends and journaled effects.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// MemPRStore is an in-memory PRStateStore for hermetic tests. It stores
// JSON snapshots, so every load returns a fresh copy exactly like Restate
// K/V and an unsaved mutation never leaks into the store.
type MemPRStore struct {
	mu  sync.Mutex
	prs map[string][]byte
}

// NewMemPRStore returns an empty in-memory PR store.
func NewMemPRStore() *MemPRStore {
	return &MemPRStore{prs: make(map[string][]byte)}
}

// LoadPR returns a copy of the PR state for a repo#prNumber key.
func (s *MemPRStore) LoadPR(key string) (*PRState, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.prs[strings.TrimSpace(key)]
	if !ok {
		return nil, false
	}
	var st PRState
	//nolint:musttag // PRState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, false
	}
	return &st, true
}

// SavePR commits a snapshot of the PR state under its repo#prNumber key.
func (s *MemPRStore) SavePR(state *PRState) {
	if s == nil || state == nil {
		return
	}
	//nolint:musttag // PRState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prs[PRObjectKey(state.Repo, state.PRNumber)] = raw
}

// MemRepoStore is an in-memory RepoStateStore for hermetic tests. It stores
// JSON snapshots, so every load returns a fresh copy exactly like Restate
// K/V (including dropped unexported indexes) and an unsaved mutation never
// leaks into the store.
type MemRepoStore struct {
	mu    sync.Mutex
	repos map[string][]byte
	err   error
}

// NewMemRepoStore returns an empty in-memory repo store.
func NewMemRepoStore() *MemRepoStore {
	return &MemRepoStore{repos: make(map[string][]byte)}
}

// FailLoads makes every later LoadRepo return err, standing in for a Restate
// K/V failure.
func (s *MemRepoStore) FailLoads(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

// LoadRepo returns a copy of the repo state, or nil when never saved.
func (s *MemRepoStore) LoadRepo(repo string) (*RepoState, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	raw, ok := s.repos[NormalizeRepo(repo)]
	if !ok {
		return nil, nil
	}
	var st RepoState
	//nolint:musttag // RepoState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// SaveRepo commits a snapshot of the repo state.
func (s *MemRepoStore) SaveRepo(state *RepoState) {
	if s == nil || state == nil {
		return
	}
	//nolint:musttag // RepoState is Restate-owned state with intentionally untagged fields; the snapshot mirrors Restate K/V, not a wire format.
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repos[NormalizeRepo(state.Repo)] = raw
}

type noopConflictChecker struct{}

func (noopConflictChecker) CheckConflict(context.Context, string, int, string) (bool, error) {
	return false, nil
}

type noopRescheduler struct{}

func (noopRescheduler) ScheduleConflictRecheck(context.Context, time.Duration, PREvent) error {
	return nil
}

// No-op dependencies for tests that do not observe a boundary.

type noopPRSender struct{}

func (noopPRSender) SendPR(context.Context, string, PREvent) error { return nil }

type noopSelfScheduler struct{}

func (noopSelfScheduler) ScheduleHeldReplay(context.Context, string, string, time.Duration) error {
	return nil
}

func (noopSelfScheduler) ScheduleHeldExpiry(context.Context, string, uint64, time.Duration) error {
	return nil
}

// testRepoHandlers builds RepoHandlers for a test, standing in a no-op for
// each boundary the test passes as nil.
func testRepoHandlers(t *testing.T, store RepoStateStore, prs PRSender, self RepoSelfScheduler) *RepoHandlers {
	t.Helper()
	if prs == nil {
		prs = noopPRSender{}
	}
	if self == nil {
		self = noopSelfScheduler{}
	}
	h, err := NewRepoHandlers(store, prs, self)
	if err != nil {
		t.Fatalf("NewRepoHandlers: %v", err)
	}
	return h
}

// testConflictHandlers builds ConflictHandlers for a test, standing in a
// no-op for each boundary the test passes as nil.
func testConflictHandlers(t *testing.T, store PRStateStore, conflicts ConflictChecker, reschedule ConflictRescheduler) *ConflictHandlers {
	t.Helper()
	if conflicts == nil {
		conflicts = noopConflictChecker{}
	}
	if reschedule == nil {
		reschedule = noopRescheduler{}
	}
	h, err := NewConflictHandlers(store, conflicts, reschedule)
	if err != nil {
		t.Fatalf("NewConflictHandlers: %v", err)
	}
	return h
}
