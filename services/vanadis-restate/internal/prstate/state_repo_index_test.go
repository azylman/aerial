package prstate

import (
	"fmt"
	"slices"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

// Positional fixture constructors for RepoObject routing index rows.

func idxActive(pr int, head, branch string, epoch uint64) PRIndexEvent {
	return PRIndexEvent{Repo: "azylman/aerial", PRNumber: pr, Branch: branch, HeadSHA: head, ReopenEpoch: epoch, Lifecycle: IndexActive}
}

func idxMerged(pr int, head, merge string) PRIndexEvent {
	return PRIndexEvent{Repo: "azylman/aerial", PRNumber: pr, HeadSHA: head, MergeSHA: merge, Lifecycle: IndexMerged}
}

func idxClosed(pr int, head string) PRIndexEvent {
	return PRIndexEvent{Repo: "azylman/aerial", PRNumber: pr, HeadSHA: head, Lifecycle: IndexClosed}
}

func idxDeploying(pr int, merge string, gen uint64) PRIndexEvent {
	return PRIndexEvent{Repo: "azylman/aerial", PRNumber: pr, MergeSHA: merge, Generation: gen, Lifecycle: IndexDeploying}
}

func idxDeployed(pr int, gen uint64) PRIndexEvent {
	return PRIndexEvent{Repo: "azylman/aerial", PRNumber: pr, Generation: gen, Lifecycle: IndexDeployed}
}

// headUpdates returns n active updates for one PR with heads h01..hNN.
func headUpdates(pr, n int) []PRIndexEvent {
	out := make([]PRIndexEvent, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, idxActive(pr, fmt.Sprintf("h%02d", i), "feat", 0))
	}
	return out
}

// closedPRs opens and closes PRs from..to with heads cNNN.
func closedPRs(from, to int) []PRIndexEvent {
	var out []PRIndexEvent
	for pr := from; pr <= to; pr++ {
		head := fmt.Sprintf("c%03d", pr)
		out = append(out, idxActive(pr, head, "", 0), idxClosed(pr, head))
	}
	return out
}

func seededRepoState(index []PRIndexEvent, heldSHAs []string) *RepoState {
	st := NewRepoState("azylman/aerial")
	for _, sha := range heldSHAs {
		st.HoldSHAEvent(SHAKindCheckRun, SHAEvent{Repo: "azylman/aerial", SHA: sha, Handler: prwire.PRHandlerCheckFailed})
	}
	for _, ev := range index {
		st.ApplyPRIndex(ev)
	}
	return st
}

func TestRepoState_ApplyPRIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		seed          []PRIndexEvent
		heldSHAs      []string
		ev            PRIndexEvent
		probeSHA      string
		wantChanged   bool
		wantReplay    []string
		wantOpen      []int
		wantDeploying []int
		wantPR        int
		wantBranch    string
	}{
		{name: "first update indexes head and opens pr", ev: idxActive(101, "h1", "feat-a", 0), probeSHA: "h1", wantChanged: true, wantOpen: []int{101}, wantPR: 101, wantBranch: "feat-a"},
		{name: "identical update changes nothing", seed: []PRIndexEvent{idxActive(101, "h1", "feat-a", 0)}, ev: idxActive(101, "h1", "feat-a", 0), probeSHA: "h1", wantOpen: []int{101}, wantPR: 101, wantBranch: "feat-a"},
		{name: "branch change re-labels head", seed: []PRIndexEvent{idxActive(101, "h1", "feat-a", 0)}, ev: idxActive(101, "h1", "feat-b", 0), probeSHA: "h1", wantChanged: true, wantOpen: []int{101}, wantPR: 101, wantBranch: "feat-b"},
		{name: "merge indexes merge sha and leaves open set", seed: []PRIndexEvent{idxActive(101, "h1", "feat-a", 0)}, ev: idxMerged(101, "h1", "m1"), probeSHA: "M1", wantChanged: true, wantPR: 101, wantBranch: "feat-a"},
		{name: "closed absorbs same epoch update but still indexes", seed: []PRIndexEvent{idxActive(101, "h1", "", 0), idxClosed(101, "h1")}, ev: idxActive(101, "h2", "", 0), probeSHA: "h2", wantChanged: true, wantPR: 101},
		{name: "newer epoch reopens", seed: []PRIndexEvent{idxActive(101, "h1", "", 0), idxClosed(101, "h1")}, ev: idxActive(101, "h1", "", 1), probeSHA: "h1", wantChanged: true, wantOpen: []int{101}, wantPR: 101},
		{name: "older epoch ignored", seed: []PRIndexEvent{idxActive(101, "h1", "", 2)}, ev: idxActive(101, "h9", "", 1), probeSHA: "h9", wantOpen: []int{101}},
		{name: "deploying adds merged pr", seed: []PRIndexEvent{idxActive(101, "h1", "", 0), idxMerged(101, "h1", "m1")}, ev: idxDeploying(101, "m1", 1), probeSHA: "m1", wantChanged: true, wantDeploying: []int{101}, wantPR: 101},
		{name: "deploying falls back to head sha", ev: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Generation: 1, Lifecycle: IndexDeploying}, probeSHA: "h1", wantChanged: true, wantOpen: []int{101}, wantDeploying: []int{101}, wantPR: 101},
		{name: "duplicate deploying changes nothing", seed: []PRIndexEvent{idxDeploying(101, "m1", 1)}, ev: idxDeploying(101, "m1", 1), probeSHA: "m1", wantOpen: []int{101}, wantDeploying: []int{101}, wantPR: 101},
		{name: "deployed clears deploying", seed: []PRIndexEvent{idxDeploying(101, "m1", 1)}, ev: idxDeployed(101, 1), probeSHA: "m1", wantChanged: true, wantOpen: []int{101}, wantPR: 101},
		{name: "older generation deployed ignored", seed: []PRIndexEvent{idxDeploying(101, "m1", 2)}, ev: idxDeployed(101, 1), probeSHA: "m1", wantOpen: []int{101}, wantDeploying: []int{101}, wantPR: 101},
		{name: "deployed without deploying changes nothing", seed: []PRIndexEvent{idxActive(101, "h1", "", 0)}, ev: idxDeployed(101, 1), probeSHA: "h1", wantOpen: []int{101}, wantPR: 101},
		{name: "close drops deploying", seed: []PRIndexEvent{idxDeploying(101, "m1", 1)}, ev: idxClosed(101, ""), probeSHA: "m1", wantChanged: true, wantPR: 101},
		{name: "repeat close keeps first close", seed: []PRIndexEvent{idxActive(101, "h1", "", 0), idxClosed(101, "h1")}, ev: idxClosed(101, "h1"), probeSHA: "h1", wantPR: 101},
		{name: "newest claim wins shared sha", seed: []PRIndexEvent{idxActive(101, "s1", "old", 0)}, ev: idxActive(102, "s1", "new", 0), probeSHA: "s1", wantChanged: true, wantOpen: []int{101, 102}, wantPR: 102, wantBranch: "new"},
		{name: "held sha is returned for replay once", heldSHAs: []string{"m1", "zz"}, seed: []PRIndexEvent{idxActive(101, "h1", "", 0)}, ev: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1", MergeSHA: "M1", Lifecycle: IndexMerged}, probeSHA: "m1", wantChanged: true, wantReplay: []string{"m1"}, wantPR: 101},
		{name: "zero sha never indexes", ev: idxActive(101, "0000000", "", 0), probeSHA: "0000000", wantChanged: true, wantOpen: []int{101}},
		{name: "missing pr number ignored", ev: idxActive(0, "h1", "", 0), probeSHA: "h1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(tc.seed, tc.heldSHAs)
			changed, replay := st.ApplyPRIndex(tc.ev)
			if changed != tc.wantChanged {
				t.Fatalf("changed = %v, want %v", changed, tc.wantChanged)
			}
			if !slices.Equal(replay, tc.wantReplay) {
				t.Fatalf("replay = %v, want %v", replay, tc.wantReplay)
			}
			if got := st.OpenPRNumbers(); !slices.Equal(got, tc.wantOpen) {
				t.Fatalf("open = %v, want %v", got, tc.wantOpen)
			}
			if got := st.DeployingPRNumbers(); !slices.Equal(got, tc.wantDeploying) {
				t.Fatalf("deploying = %v, want %v", got, tc.wantDeploying)
			}
			entry, _ := st.ResolveSHA(tc.probeSHA)
			if entry.PRNumber != tc.wantPR || entry.Branch != tc.wantBranch {
				t.Fatalf("ResolveSHA(%q) = (%d,%q), want (%d,%q)", tc.probeSHA, entry.PRNumber, entry.Branch, tc.wantPR, tc.wantBranch)
			}
		})
	}
}

func TestRepoState_IndexRetention(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		seed     []PRIndexEvent
		probeSHA string
		wantPR   int
	}{
		{name: "open pr keeps its newest twenty shas", seed: headUpdates(101, 21), probeSHA: "h02", wantPR: 101},
		{name: "open pr drops its oldest sha past twenty", seed: headUpdates(101, 21), probeSHA: "h01", wantPR: 0},
		{name: "oldest close evicted past one hundred closed prs", seed: closedPRs(1, 101), probeSHA: "c001", wantPR: 0},
		{name: "second oldest close retained", seed: closedPRs(1, 101), probeSHA: "c002", wantPR: 2},
		{name: "newest close retained", seed: closedPRs(1, 101), probeSHA: "c101", wantPR: 101},
		{name: "deploying close never evicted", seed: append([]PRIndexEvent{idxActive(1, "c001", "", 0), idxMerged(1, "c001", "m001"), idxDeploying(1, "m001", 1)}, closedPRs(2, 102)...), probeSHA: "m001", wantPR: 1},
		{name: "oldest non deploying close evicted instead", seed: append([]PRIndexEvent{idxActive(1, "c001", "", 0), idxMerged(1, "c001", "m001"), idxDeploying(1, "m001", 1)}, closedPRs(2, 102)...), probeSHA: "c002", wantPR: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(tc.seed, nil)
			entry, _ := st.ResolveSHA(tc.probeSHA)
			if entry.PRNumber != tc.wantPR {
				t.Fatalf("ResolveSHA(%q) = %d, want %d", tc.probeSHA, entry.PRNumber, tc.wantPR)
			}
		})
	}
}

func TestRepoState_ResolveSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nilState   bool
		seed       []PRIndexEvent
		sha        string
		wantPR     int
		wantBranch string
		wantOK     bool
	}{
		{name: "exact sha resolves", seed: []PRIndexEvent{idxActive(101, "abc123", "feat/x", 0)}, sha: "abc123", wantPR: 101, wantBranch: "feat/x", wantOK: true},
		{name: "case and padding insensitive", seed: []PRIndexEvent{idxActive(101, "abc123", "feat/x", 0)}, sha: " ABC123 ", wantPR: 101, wantBranch: "feat/x", wantOK: true},
		{name: "unknown sha misses", seed: []PRIndexEvent{idxActive(101, "abc123", "feat/x", 0)}, sha: "zzz999"},
		{name: "zero sha misses", seed: []PRIndexEvent{idxActive(101, "abc123", "feat/x", 0)}, sha: "0000000"},
		{name: "empty index misses", sha: "abc123"},
		{name: "nil state misses", nilState: true, sha: "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(tc.seed, nil)
			if tc.nilState {
				st = nil
			}
			entry, ok := st.ResolveSHA(tc.sha)
			if entry.PRNumber != tc.wantPR || entry.Branch != tc.wantBranch || ok != tc.wantOK {
				t.Fatalf("ResolveSHA(%q) = (%d,%q,%v), want (%d,%q,%v)", tc.sha, entry.PRNumber, entry.Branch, ok, tc.wantPR, tc.wantBranch, tc.wantOK)
			}
		})
	}
}

func TestRepoState_PRStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		seed     []PRIndexEvent
		pr       int
		want     RepoPRStatus
		wantOK   bool
	}{
		{name: "deploying status reported", seed: []PRIndexEvent{idxActive(101, "h1", "feat", 0), idxMerged(101, "h1", "m1"), idxDeploying(101, "M1", 3)}, pr: 101, want: RepoPRStatus{Branch: "feat", Closed: true, ClosedSeq: 2, Deploying: true, DeploySHA: "m1", DeployGeneration: 3}, wantOK: true},
		{name: "unknown pr misses", seed: []PRIndexEvent{idxActive(101, "h1", "feat", 0)}, pr: 102},
		{name: "nil state misses", nilState: true, pr: 101},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(tc.seed, nil)
			if tc.nilState {
				st = nil
			}
			got, ok := st.PRStatus(tc.pr)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("PRStatus(%d) = (%+v,%v), want (%+v,%v)", tc.pr, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestRepoState_RoutingSetsNilSafe(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		nilState      bool
		wantOpen      []int
		wantDeploying []int
	}{
		{name: "nil state reports empty sets", nilState: true},
		{name: "fresh state reports empty sets"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := NewRepoState("azylman/aerial")
			if tc.nilState {
				st = nil
			}
			if got := st.OpenPRNumbers(); !slices.Equal(got, tc.wantOpen) {
				t.Fatalf("open = %v, want %v", got, tc.wantOpen)
			}
			if got := st.DeployingPRNumbers(); !slices.Equal(got, tc.wantDeploying) {
				t.Fatalf("deploying = %v, want %v", got, tc.wantDeploying)
			}
			if changed, _ := st.ApplyPRIndex(idxActive(101, "h1", "", 0)); changed != !tc.nilState {
				t.Fatalf("ApplyPRIndex changed = %v, want %v", changed, !tc.nilState)
			}
		})
	}
}

// heldSHAs lists the held event SHAs in order.
func heldSHAs(st *RepoState) []string {
	if st == nil {
		return nil
	}
	var out []string
	for _, h := range st.Held {
		out = append(out, h.Event.SHA)
	}
	return out
}

func numberedSHAs(n int) []string {
	out := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, fmt.Sprintf("q%02d", i))
	}
	return out
}

func TestRepoState_HoldSHAEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		nilState       bool
		seedHeld       []string
		sha            string
		wantID         uint64
		wantEvictedIDs []uint64
		wantHeldLen    int
		wantLast       string
	}{
		{name: "first hold gets id one and normalized sha", sha: " ABC123 ", wantID: 1, wantHeldLen: 1, wantLast: "abc123"},
		{name: "hold below capacity evicts nothing", seedHeld: numberedSHAs(63), sha: "abc123", wantID: 64, wantHeldLen: 64, wantLast: "abc123"},
		{name: "hold at capacity evicts oldest", seedHeld: numberedSHAs(64), sha: "abc123", wantID: 65, wantEvictedIDs: []uint64{1}, wantHeldLen: 64, wantLast: "abc123"},
		{name: "nil state holds nothing", nilState: true, sha: "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(nil, tc.seedHeld)
			if tc.nilState {
				st = nil
			}
			held, evicted := st.HoldSHAEvent(SHAKindPush, SHAEvent{Repo: "azylman/aerial", SHA: tc.sha, Handler: prwire.PRHandlerMerged})
			if held.ID != tc.wantID {
				t.Fatalf("held id = %d, want %d", held.ID, tc.wantID)
			}
			var gotEvicted []uint64
			for _, e := range evicted {
				gotEvicted = append(gotEvicted, e.ID)
			}
			if !slices.Equal(gotEvicted, tc.wantEvictedIDs) {
				t.Fatalf("evicted = %v, want %v", gotEvicted, tc.wantEvictedIDs)
			}
			if tc.nilState {
				return
			}
			got := heldSHAs(st)
			if len(got) != tc.wantHeldLen || got[len(got)-1] != tc.wantLast {
				t.Fatalf("held = %d ending %q, want %d ending %q", len(got), got[len(got)-1], tc.wantHeldLen, tc.wantLast)
			}
		})
	}
}

func TestRepoState_TakeHeld(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		seedHeld []string
		sha      string
		wantIDs  []uint64
		wantLeft []string
	}{
		{name: "takes every event for the sha oldest first", seedHeld: []string{"a1", "b2", "a1"}, sha: "A1", wantIDs: []uint64{1, 3}, wantLeft: []string{"b2"}},
		{name: "unknown sha takes nothing", seedHeld: []string{"a1"}, sha: "zz", wantLeft: []string{"a1"}},
		{name: "zero sha takes nothing", seedHeld: []string{"a1"}, sha: "0000000", wantLeft: []string{"a1"}},
		{name: "nil state takes nothing", nilState: true, sha: "a1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(nil, tc.seedHeld)
			if tc.nilState {
				st = nil
			}
			var gotIDs []uint64
			for _, h := range st.TakeHeld(tc.sha) {
				gotIDs = append(gotIDs, h.ID)
			}
			if !slices.Equal(gotIDs, tc.wantIDs) {
				t.Fatalf("taken ids = %v, want %v", gotIDs, tc.wantIDs)
			}
			if tc.nilState {
				return
			}
			if got := heldSHAs(st); !slices.Equal(got, tc.wantLeft) {
				t.Fatalf("left = %v, want %v", got, tc.wantLeft)
			}
		})
	}
}

func TestRepoState_ExpireHeld(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		seedHeld []string
		id       uint64
		wantSHA  string
		wantOK   bool
		wantLeft []string
	}{
		{name: "expires the middle event", seedHeld: []string{"a1", "b2", "c3"}, id: 2, wantSHA: "b2", wantOK: true, wantLeft: []string{"a1", "c3"}},
		{name: "unknown id is a no-op", seedHeld: []string{"a1"}, id: 9, wantLeft: []string{"a1"}},
		{name: "nil state reports false", nilState: true, id: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := seededRepoState(nil, tc.seedHeld)
			if tc.nilState {
				st = nil
			}
			got, ok := st.ExpireHeld(tc.id)
			if got.Event.SHA != tc.wantSHA || ok != tc.wantOK {
				t.Fatalf("ExpireHeld(%d) = (%q,%v), want (%q,%v)", tc.id, got.Event.SHA, ok, tc.wantSHA, tc.wantOK)
			}
			if tc.nilState {
				return
			}
			if left := heldSHAs(st); !slices.Equal(left, tc.wantLeft) {
				t.Fatalf("left = %v, want %v", left, tc.wantLeft)
			}
		})
	}
}
