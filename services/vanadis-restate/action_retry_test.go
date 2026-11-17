package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
)

type retryJournalRuntime struct {
	*fakeObjectRuntime

	journal         map[string]reconcileStepOutput
	loseReset       bool
	loseReconcile   bool
	reconcileErr    error
	terminalJournal map[string]error
}

func (r *retryJournalRuntime) RunReconcileStep(name string, fn func(context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	if err := r.terminalJournal[name]; err != nil {
		return reconcileStepOutput{}, err
	}
	if out, ok := r.journal[name]; ok {
		return out, nil
	}
	if name == "retry-action-reconcile" && r.reconcileErr != nil {
		err := r.reconcileErr
		r.reconcileErr = nil
		return reconcileStepOutput{}, err
	}
	out, err := fn(context.Background())
	if err != nil {
		if restate.IsTerminalError(err) {
			if r.terminalJournal == nil {
				r.terminalJournal = map[string]error{}
			}
			r.terminalJournal[name] = err
		}
		return out, err
	}
	if name == "retry-action-reset" && r.loseReset {
		r.loseReset = false
		return reconcileStepOutput{}, errors.New("journal acknowledgement lost")
	}
	if name == "retry-action-reconcile" && r.loseReconcile {
		r.loseReconcile = false
		return reconcileStepOutput{}, errors.New("reconcile journal lost")
	}
	r.journal[name] = out
	return out, nil
}

type retryAuthorityFixture struct {
	VanadisStore

	current     *VanadisChunk
	buildStatus *string
}

func (f *retryAuthorityFixture) GetBuild(context.Context, string) (*VanadisBuild, []VanadisChunk, error) {
	return &VanadisBuild{ID: "b1", Status: *f.buildStatus}, []VanadisChunk{*f.current}, nil
}

func TestBuildRetryAction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		lostCommit, lostJournal bool
		reconcileErr            error
		reqBuild, identity      string
		wantErr                 bool
	}{
		{name: "accepted"}, {name: "commit response lost", lostCommit: true}, {name: "reset journal lost", lostJournal: true}, {name: "reconcile crash", reconcileErr: errors.New("worker interrupted")}, {name: "wrong build", reqBuild: "other", wantErr: true}, {name: "stale", identity: "vanadis-c1-turn-8", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			current := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}
			status := "failed"
			lost := tc.lostCommit
			store := retryFixtureStore(&mu, &current, &status, VanadisChunk{ID: "c0", Status: "completed"}, false, &lost)
			authority := &retryAuthorityFixture{VanadisStore: store, current: &current, buildStatus: &status}
			rt := &retryJournalRuntime{fakeObjectRuntime: newFakeObjectRuntime("b1"), journal: map[string]reconcileStepOutput{}, loseReset: tc.lostJournal, reconcileErr: tc.reconcileErr}
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(authority, nil)
			req := RetryActionChunkRequest{BuildID: cmp.Or(tc.reqBuild, "b1"), ChunkID: "c1", ExpectedDispatchID: cmp.Or(tc.identity, "vanadis-c1-turn-0"), Reason: "retry"}
			got, err := bo.RetryActionChunk(nil, req)
			if tc.lostCommit || tc.lostJournal || tc.reconcileErr != nil {
				if err == nil || restate.IsTerminalError(err) {
					t.Fatalf("first error must be retriable: %v", err)
				}
				got, err = bo.RetryActionChunk(nil, req)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			if tc.wantErr {
				if current.Status != "failed" || current.StallNudges != 0 {
					t.Fatalf("rejected request mutated %+v", current)
				}
				return
			}
			if !got.Accepted || got.DispatchID != "vanadis-c1-turn-1" || current.StallNudges != 1 || status != "running" {
				t.Fatalf("result=%+v chunk=%+v status=%s", got, current, status)
			}
			again, err := bo.RetryActionChunk(nil, req)
			if err != nil || again != got || current.StallNudges != 1 {
				t.Fatalf("journal replay=%+v err=%v", again, err)
			}
		})
	}
}

func (f *retryAuthorityFixture) ReserveChunkForLaunch(context.Context, string) error {
	f.current.Status = "running"
	return nil
}

func (*retryAuthorityFixture) UpdateBuildStatusMessageID(context.Context, string, string) error {
	return nil
}

type retryDispatchFixture struct {
	mu       sync.Mutex
	accepted map[string]BrainDispatchRequest
}

func (f *retryDispatchFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/dispatch" {
		_, _ = w.Write([]byte(`{}`))
		return
	}
	var req BrainDispatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	status := "accepted"
	if _, ok := f.accepted[req.ID]; ok {
		status = "already_exists"
	}
	f.accepted[req.ID] = req
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "dispatch_id": req.ID})
}

func TestRetryActionDispatchRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		loseJournal bool
	}{{"accepted", false}, {"dispatch accepted before journal lost", true}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			current := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", Instructions: "verify"}
			status := "failed"
			lost := false
			store := retryFixtureStore(&mu, &current, &status, VanadisChunk{ID: "c0", Status: "completed"}, false, &lost)
			authority := &retryAuthorityFixture{VanadisStore: store, current: &current, buildStatus: &status}
			f := &retryDispatchFixture{accepted: map[string]BrainDispatchRequest{}}
			ts := httptest.NewServer(f)
			defer ts.Close()
			rt := &retryJournalRuntime{fakeObjectRuntime: newFakeObjectRuntime("b1"), journal: map[string]reconcileStepOutput{}, loseReconcile: tc.loseJournal}
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(authority, NewExecutor(t.TempDir(), NewHTTPEriClient(ts.URL, ts.Client()), nil, "", nil))
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}
			got, err := bo.RetryActionChunk(nil, req)
			if tc.loseJournal {
				if err == nil {
					t.Fatal("expected interrupted journal")
				}
				got, err = bo.RetryActionChunk(nil, req)
			}
			if err != nil || !got.Accepted || current.Status != "running" || current.StallNudges != 1 {
				t.Fatalf("result=%+v err=%v chunk=%+v", got, err, current)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.accepted) != 1 {
				t.Fatalf("accepted executions=%+v", f.accepted)
			}
			dispatch, ok := f.accepted["vanadis-c1-turn-1"]
			if !ok || dispatch.SessionID != "vanadis-c1" || dispatch.VanadisChunkID != "c1" {
				t.Fatalf("dispatch=%+v", dispatch)
			}
			if !rt.launched["dispatch:vanadis-c1-turn-1"] {
				t.Fatal("accepted generation not persisted")
			}
		})
	}
}
