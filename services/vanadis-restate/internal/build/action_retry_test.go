package build

import (
	"cmp"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// retryStore seeds build b1 (failed) with a completed c0 and the failed
// action chunk c1 carrying instructions, applying sabotage afterwards.
func retryStore(t *testing.T, instructions, sabotage string) *store.Store {
	t.Helper()
	st, pool := seededStore(t,
		[]model.VanadisBuild{{ID: "b1", ThreadID: "th-1", Status: "failed"}},
		[]model.VanadisChunk{
			{ID: "c0", BuildID: "b1", Status: "completed"},
			{ID: "c1", BuildID: "b1", ChunkIndex: 1, Mode: "action", Status: "failed", Instructions: instructions},
		})
	mustExec(t, pool, sabotage)
	return st
}

// retryStored is the persisted retry-relevant view of c1 and its build.
type retryStored struct {
	Status, Instructions, BuildStatus string
	Generation                        int
}

func loadRetryStored(t *testing.T, st *store.Store) retryStored {
	t.Helper()
	b, chunks := loadBuild(t, st, "b1")
	for _, c := range chunks {
		if c.ID == "c1" {
			return retryStored{Status: c.Status, Instructions: c.Instructions, BuildStatus: b.Status, Generation: c.StallNudges}
		}
	}
	t.Fatal("c1 not stored")
	return retryStored{}
}

func TestBuildRetryAction(t *testing.T) {
	t.Parallel()
	const invalid = "invalid vanadis build input"
	tests := []struct {
		name                                          string
		lostCommit, lostJournal                       bool
		failBuild, noAuthority                        bool
		reconcileErr                                  error
		reqBuild, identity                            string
		wantErr                                       string
		expectedInstructions, replacementInstructions *string
	}{
		{name: "missing expected", replacementInstructions: new("corrected"), wantErr: invalid},
		{name: "missing replacement", expectedInstructions: new("verify"), wantErr: invalid},
		{name: "blank expected", expectedInstructions: new(" "), replacementInstructions: new("corrected"), wantErr: invalid},
		{name: "blank replacement", expectedInstructions: new("verify"), replacementInstructions: new(" "), wantErr: invalid},
		{name: "accepted"},
		{name: "commit response lost", lostCommit: true},
		{name: "reset journal lost", lostJournal: true},
		{name: "reconcile crash", reconcileErr: errors.New("worker interrupted")},
		{name: "wrong build", reqBuild: "other", wantErr: invalid},
		{name: "stale", identity: "vanadis-c1-turn-8", wantErr: "action retry preconditions changed"},
		{name: "build write fails", failBuild: true, wantErr: "sabotaged UPDATE on vanadis_builds"},
		{name: "no authority", noAuthority: true, wantErr: "retry authority not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sabotage := ""
			if tc.failBuild {
				sabotage = failStatement("vanadis_builds", "UPDATE")
			}
			st := retryStore(t, "", sabotage)
			authority := &faultStore{VanadisStore: st}
			if tc.lostCommit {
				authority.lostAcks = map[string]error{"RetryActionChunk": errors.New("commit response lost")}
			}
			rt := &fakeObjectRuntime{key: "b1", journal: map[string]reconcileStepOutput{}, failOnce: map[string]error{"retry-action-reconcile": tc.reconcileErr}}
			if tc.lostJournal {
				rt.lostAcks = map[string]error{"retry-action-reset": errors.New("journal acknowledgement lost")}
			}
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(authority, nil)
			if tc.noAuthority {
				bo = bo.WithAuthority(nil, nil)
			}
			req := store.RetryActionChunkRequest{BuildID: cmp.Or(tc.reqBuild, "b1"), ChunkID: "c1", ExpectedDispatchID: cmp.Or(tc.identity, "vanadis-c1-turn-0"), Reason: "retry", ExpectedInstructions: tc.expectedInstructions, ReplacementInstructions: tc.replacementInstructions}
			got, err := bo.RetryActionChunk(nil, req)
			if tc.lostCommit || tc.lostJournal || tc.reconcileErr != nil {
				if err == nil || restate.IsTerminalError(err) {
					t.Fatalf("first error must be retriable: %v", err)
				}
				got, err = bo.RetryActionChunk(nil, req)
			}
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Equal(t, retryStored{Status: "failed", BuildStatus: "failed"}, loadRetryStored(t, st), "rejected request mutated the chunk")
				return
			}
			require.NoError(t, err)
			require.Equal(t, store.RetryActionChunkResult{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1", Accepted: true}, got)
			require.Equal(t, retryStored{Status: "pending", BuildStatus: "running", Generation: 1}, loadRetryStored(t, st))
			again, err := bo.RetryActionChunk(nil, req)
			require.NoError(t, err, "journal replay")
			require.Equal(t, got, again, "journal replay")
			require.Equal(t, retryStored{Status: "pending", BuildStatus: "running", Generation: 1}, loadRetryStored(t, st), "journal replay reset again")
		})
	}
}

// Duplicate retries from independent invocations race on the same database:
// the parent row lock serializes them, so exactly one generation reset
// commits and every caller acknowledges the same dispatch identity.
func TestBuildRetryActionConcurrent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		concurrent int
	}{
		{"two invocations", 2},
		{"eight invocations", 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := retryStore(t, "", "")
			req := store.RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}
			results := make(chan store.RetryActionChunkResult, tc.concurrent)
			errs := make(chan error, tc.concurrent)
			var wg sync.WaitGroup
			for range tc.concurrent {
				wg.Go(func() {
					rt := &fakeObjectRuntime{key: "b1", journal: map[string]reconcileStepOutput{}}
					bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(st, nil)
					got, err := bo.RetryActionChunk(nil, req)
					results <- got
					errs <- err
				})
			}
			wg.Wait()
			close(results)
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			for got := range results {
				require.Equal(t, store.RetryActionChunkResult{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1", Accepted: true}, got)
			}
			require.Equal(t, retryStored{Status: "pending", BuildStatus: "running", Generation: 1}, loadRetryStored(t, st))
		})
	}
}

type retryDispatchFixture struct {
	mu       sync.Mutex
	accepted map[string]eriwire.BrainDispatchRequest
}

func (f *retryDispatchFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/Enqueue") {
		_, _ = w.Write([]byte(`{}`))
		return
	}
	var req eriwire.BrainDispatchRequest
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
		name                  string
		loseJournal           bool
		expected, replacement *string
		wantInstructions      string
	}{{"accepted", false, nil, nil, "verify"}, {"dispatch accepted before journal lost", true, nil, nil, "verify"}, {"corrected dispatch", false, new("verify"), new("corrected verification"), "corrected verification"}, {"corrected dispatch recovery", true, new("verify"), new("corrected verification"), "corrected verification"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := retryStore(t, "verify", "")
			f := &retryDispatchFixture{accepted: map[string]eriwire.BrainDispatchRequest{}}
			ts := httptest.NewServer(f)
			defer ts.Close()
			rt := &fakeObjectRuntime{key: "b1", journal: map[string]reconcileStepOutput{}}
			if tc.loseJournal {
				rt.lostAcks = map[string]error{"retry-action-reconcile": errors.New("reconcile journal lost")}
			}
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(st, executor.NewExecutor(t.TempDir(), executor.NewRestateEriClient(ts.URL, "", "aerial", ts.Client()), nil, "", "example-owner", nil))
			req := store.RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry", ExpectedInstructions: tc.expected, ReplacementInstructions: tc.replacement}
			got, err := bo.RetryActionChunk(nil, req)
			if tc.loseJournal {
				if err == nil {
					t.Fatal("expected interrupted journal")
				}
				req.ExpectedInstructions, req.ReplacementInstructions = new("changed"), new("must not apply")
				got, err = bo.RetryActionChunk(nil, req)
			}
			current := loadRetryStored(t, st)
			if err != nil || !got.Accepted || current.Status != "running" || current.Generation != 1 || current.BuildStatus != "running" {
				t.Fatalf("result=%+v err=%v stored=%+v", got, err, current)
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
			if current.Instructions != tc.wantInstructions || !strings.Contains(dispatch.Prompt, "Instructions:\n"+tc.wantInstructions+"\n") {
				t.Fatalf("instructions=%q prompt=%s", current.Instructions, dispatch.Prompt)
			}
			if !rt.launched["dispatch:vanadis-c1-turn-1"] {
				t.Fatal("accepted generation not persisted")
			}
		})
	}
}
