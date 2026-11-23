package prstate

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/prwire"
)

func TestPRStateRecordFailedCISHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		repo    string
		pr      int
		first   string
		sha     string
		want    bool
		wantGet string
	}{
		{name: "records fresh sha", repo: "azylman/aerial", pr: 101, sha: "abc123", want: true, wantGet: "abc123"},
		{name: "rejects empty sha", repo: "azylman/aerial", pr: 101, sha: "", want: false},
		{name: "rejects zero sha", repo: "azylman/aerial", pr: 101, sha: "0000000", want: false},
		{name: "rejects zero pr", repo: "azylman/aerial", pr: 0, sha: "abc123", want: false},
		{name: "rejects empty repo", repo: "", pr: 101, sha: "abc123", want: false},
		{name: "same sha not new", repo: "azylman/aerial", pr: 101, first: "abc123", sha: "abc123", want: false, wantGet: "abc123"},
		{name: "same sha different case not new", repo: "azylman/aerial", pr: 101, first: "ABC123", sha: "abc123", want: false, wantGet: "ABC123"},
		{name: "changed sha is new", repo: "azylman/aerial", pr: 101, first: "abc123", sha: "def456", want: true, wantGet: "def456"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState(tc.repo, tc.pr)
			s.RecordFailedCISHA(tc.first)
			if got := s.RecordFailedCISHA(tc.sha); got != tc.want {
				t.Fatalf("RecordFailedCISHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
			if s.FailedCISHA != tc.wantGet {
				t.Fatalf("FailedCISHA = %q, want %q", s.FailedCISHA, tc.wantGet)
			}
		})
	}
}

func TestPRStateHasFailedCISHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		stored string
		query  string
		want   bool
	}{
		{name: "empty query matches any stored", stored: "abc123", query: "", want: true},
		{name: "matching sha", stored: "abc123", query: "abc123", want: true},
		{name: "case insensitive match", stored: "ABC123", query: "abc123", want: true},
		{name: "mismatch", stored: "abc123", query: "def456", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCISHA(tc.stored)
			if got := s.HasFailedCISHA(tc.query); got != tc.want {
				t.Fatalf("HasFailedCISHA(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

// checkRunRecord is one RecordFailedCheckRunDetailed result:
// emitAdditional, wasActive, hadPriorEverFailed.
type checkRunRecord [3]bool

func TestPRStateFailedCheckRunDetailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		first     string
		secondSHA string
		second    string
		want      checkRunRecord
		want2     checkRunRecord
		wantHead  string
	}{
		{name: "first failure no emit not active no prior", first: "lint", wantHead: "abc123"},
		{name: "repeat failure emits was active", first: "lint", secondSHA: "abc123", second: "lint", want2: checkRunRecord{false, true, true}, wantHead: "abc123"},
		{name: "second distinct check emits", first: "lint", secondSHA: "abc123", second: "build", want2: checkRunRecord{true, false, false}, wantHead: "abc123"},
		{name: "roborev first no emit", first: "roborev", wantHead: "abc123"},
		{name: "roborev repeat was active", first: "roborev", secondSHA: "abc123", second: "roborev", want2: checkRunRecord{false, true, true}, wantHead: "abc123"},
		{name: "non roborev after roborev emits", first: "roborev", secondSHA: "abc123", second: "lint", want2: checkRunRecord{true, false, false}, wantHead: "abc123"},
		{name: "new sha resets ever failed for lint", first: "lint", secondSHA: "def456", second: "lint", wantHead: "def456"},
		{name: "new sha resets roborev ever failed", first: "roborev", secondSHA: "def456", second: "roborev", wantHead: "def456"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			emit, wasActive, hadPrior := s.RecordFailedCheckRunDetailed("abc123", tc.first)
			if got := (checkRunRecord{emit, wasActive, hadPrior}); got != tc.want {
				t.Fatalf("first record = %v, want %v", got, tc.want)
			}
			if tc.second != "" {
				emit, wasActive, hadPrior = s.RecordFailedCheckRunDetailed(tc.secondSHA, tc.second)
				if got := (checkRunRecord{emit, wasActive, hadPrior}); got != tc.want2 {
					t.Fatalf("second record = %v, want %v", got, tc.want2)
				}
			}
			if s.Checks.HeadSHA != tc.wantHead {
				t.Fatalf("HeadSHA = %q, want %q", s.Checks.HeadSHA, tc.wantHead)
			}
		})
	}
}

func TestPRStateRollbackFailedCheckRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		check       string
		recordSHA   string
		rollbackSHA string
		wasActive   bool
		hadPrior    bool
		wantActive  bool
		wantEver    bool
	}{
		{name: "rollback fresh lint clears active and ever", check: "lint", recordSHA: "abc123", rollbackSHA: "abc123", wasActive: false, hadPrior: false, wantActive: false, wantEver: false},
		{name: "rollback keeps prior ever failed", check: "lint", recordSHA: "abc123", rollbackSHA: "abc123", wasActive: false, hadPrior: true, wantActive: false, wantEver: true},
		{name: "rollback skips when was active", check: "lint", recordSHA: "abc123", rollbackSHA: "abc123", wasActive: true, hadPrior: false, wantActive: true, wantEver: true},
		{name: "rollback fresh roborev clears", check: "roborev", recordSHA: "abc123", rollbackSHA: "abc123", wasActive: false, hadPrior: false, wantActive: false, wantEver: false},
		{name: "rollback roborev keeps prior ever", check: "roborev", recordSHA: "abc123", rollbackSHA: "abc123", wasActive: false, hadPrior: true, wantActive: false, wantEver: true},
		{name: "stale sha rollback noop", check: "lint", recordSHA: "abc123", rollbackSHA: "def456", wasActive: false, hadPrior: false, wantActive: true, wantEver: true},
		{name: "empty check rollback noop", check: "", recordSHA: "abc123", rollbackSHA: "abc123", wasActive: false, hadPrior: false, wantActive: false, wantEver: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCheckRunDetailed(tc.recordSHA, tc.check)
			s.RollbackFailedCheckRun(tc.rollbackSHA, tc.check, tc.wasActive, tc.hadPrior)
			if got := s.HasActiveCheckRun(tc.recordSHA, tc.check); got != tc.wantActive {
				t.Fatalf("HasActiveCheckRun = %v, want %v", got, tc.wantActive)
			}
			if got := s.HasEverFailedCheckRun(tc.recordSHA, tc.check); got != tc.wantEver {
				t.Fatalf("HasEverFailedCheckRun = %v, want %v", got, tc.wantEver)
			}
		})
	}
}

// TestPRStateClearFailedCISHA pins the ClearFailedCISHA fan-out: the failed
// CI SHA, every per-check failure and the conflict-published SHA clear.
func TestPRStateClearFailedCISHA(t *testing.T) {
	t.Parallel()
	s := NewPRState("azylman/aerial", 101)
	s.RecordFailedCISHA("abc123")
	s.RecordFailedCheckRunDetailed("abc123", "lint")
	s.MarkConflictPublished("abc123")
	s.ClearFailedCISHA()
	tests := []struct {
		name string
		got  bool
		want bool
	}{
		{name: "failed ci sha clears", got: s.HasFailedCISHA(""), want: false},
		{name: "active check runs clear", got: s.HasActiveFailedCheckRuns("abc123"), want: false},
		{name: "ever failed checks clear", got: s.HasEverFailedCheckRun("abc123", "lint"), want: false},
		{name: "conflict published clears", got: s.WasConflictPublished("abc123"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Fatalf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestPRStateClearFailedCheckRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		record string
		clear  string
		sha    string
		want   bool
	}{
		{name: "clears active lint", record: "lint", clear: "lint", sha: "abc123", want: true},
		{name: "unknown check not cleared", record: "lint", clear: "build", sha: "abc123", want: false},
		{name: "stale sha not cleared", record: "lint", clear: "lint", sha: "def456", want: false},
		{name: "empty check not cleared", record: "lint", clear: "   ", sha: "abc123", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCheckRunDetailed("abc123", tc.record)
			if got := s.ClearFailedCheckRun(tc.sha, tc.clear); got != tc.want {
				t.Fatalf("ClearFailedCheckRun = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPRStateActiveQueries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		checks      []string
		sha         string
		wantAny     bool
		wantRoboRev bool
	}{
		{name: "no checks nothing active", checks: nil, sha: "abc123", wantAny: false, wantRoboRev: false},
		{name: "lint active non roborev", checks: []string{"lint"}, sha: "abc123", wantAny: true, wantRoboRev: false},
		{name: "roborev only", checks: []string{"roborev"}, sha: "abc123", wantAny: true, wantRoboRev: true},
		{name: "stale sha nothing active", checks: []string{"lint"}, sha: "def456", wantAny: false, wantRoboRev: false},
		{name: "empty sha nothing active", checks: []string{"lint"}, sha: "", wantAny: false, wantRoboRev: false},
		{name: "zero sha nothing active", checks: []string{"lint", "roborev"}, sha: "0000000", wantAny: false, wantRoboRev: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			for _, c := range tc.checks {
				s.RecordFailedCheckRunDetailed("abc123", c)
			}
			if got := s.HasActiveFailedCheckRuns(tc.sha); got != tc.wantAny {
				t.Fatalf("HasActiveFailedCheckRuns = %v, want %v", got, tc.wantAny)
			}
			if got := s.HasActiveRoboRevFailure(tc.sha); got != tc.wantRoboRev {
				t.Fatalf("HasActiveRoboRevFailure = %v, want %v", got, tc.wantRoboRev)
			}
		})
	}
}

// TestPRStateGenerationGating pins generation fencing: each kustomization
// request mints a generation that re-keys the generation-scoped effects,
// while a readiness retry never mints one and keeps its dispatched keys.
func TestPRStateGenerationGating(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		kind       string
		retries    int
		mints      int
		wantGen    uint64
		wantOldKey bool
	}{
		{name: "discord alert keyed to generation", kind: prwire.EffectDiscordAlert, mints: 1, wantGen: 2, wantOldKey: false},
		{name: "deploy dispatch keyed to generation", kind: prwire.EffectDeployDispatch, mints: 1, wantGen: 2, wantOldKey: false},
		{name: "retry preserves generation and keys", kind: prwire.EffectDiscordAlert, retries: 1, wantGen: 1, wantOldKey: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			if gen := s.RecordKustomizationRequests(); gen != 1 {
				t.Fatalf("first generation = %d, want 1", gen)
			}
			k1 := s.EffectKeyFor(tc.kind, "abc123", "eri", "lint")
			s.MarkEffectDispatched(k1)
			if !s.WasEffectDispatched(k1) {
				t.Fatalf("effect key should be dispatched in generation 1")
			}
			for range tc.retries {
				s.RetryPRReadiness()
			}
			for range tc.mints {
				s.RecordKustomizationRequests()
			}
			if s.GateGeneration != tc.wantGen {
				t.Fatalf("GateGeneration = %d, want %d", s.GateGeneration, tc.wantGen)
			}
			if got := s.WasEffectDispatched(k1); got != tc.wantOldKey {
				t.Fatalf("generation-1 key dispatched = %v, want %v", got, tc.wantOldKey)
			}
			if got := s.WasEffectDispatched(s.EffectKeyFor(tc.kind, "abc123", "eri", "lint")); got != tc.wantOldKey {
				t.Fatalf("current-generation key dispatched = %v, want %v", got, tc.wantOldKey)
			}
		})
	}
}

// TestPRStateApplyEvent pins the event disposition: stale heads and merge
// SHAs drop, terminal states absorb within their reopen epoch, and
// reopenAfter rows prove a Reopen re-admits the next epoch's events.
func TestPRStateApplyEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		current     string
		stored      string
		terminal    TerminalState
		epoch       uint64
		event       string
		merge       string
		want        EventDisposition
		reopenAfter bool
	}{
		{name: "current sha applies", current: "abc123", event: "abc123", want: DispositionApply},
		{name: "stale sha drops", current: "abc123", event: "def456", want: DispositionDropStale},
		{name: "no current head applies", current: "", event: "abc123", want: DispositionApply},
		{name: "merge sha match applies", current: "abc123", merge: "abc123", want: DispositionApply},
		{name: "merge-only sha before the merge is known applies", current: "abc123", merge: "zzz999", want: DispositionApply},
		{name: "merge-only sha matching the stored merge applies", current: "abc123", stored: "zzz999", merge: "zzz999", want: DispositionApply},
		{name: "merge-only sha against another stored merge drops", current: "abc123", stored: "mmm111", merge: "zzz999", want: DispositionDropStale},
		{name: "squash push carrying the merge sha as head applies", current: "abc123", event: "zzz999", merge: "zzz999", want: DispositionApply},
		{name: "squash push against another stored merge drops", current: "abc123", stored: "mmm111", event: "zzz999", merge: "zzz999", want: DispositionDropStale},
		{name: "empty event applies", current: "abc123", want: DispositionApply},
		{name: "merged absorbs same epoch", current: "abc123", terminal: TerminalMerged, event: "abc123", want: DispositionAbsorbTerminal, reopenAfter: true},
		{name: "closed absorbs same epoch", current: "abc123", terminal: TerminalClosed, event: "abc123", want: DispositionAbsorbTerminal, reopenAfter: true},
		{name: "newer epoch applies through terminal", current: "abc123", terminal: TerminalMerged, epoch: 1, event: "abc123", want: DispositionApply},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCISHA(tc.current)
			s.MergeSHA = tc.stored
			s.MarkTerminal(tc.terminal)
			ev := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: tc.event, MergeSHA: tc.merge, ReopenEpoch: tc.epoch}
			if got := s.ApplyEvent(ev); got != tc.want {
				t.Fatalf("ApplyEvent = %q, want %q", got, tc.want)
			}
			if !tc.reopenAfter {
				return
			}
			s.Reopen()
			if s.Terminal != TerminalNone || s.ReopenEpoch != 1 {
				t.Fatalf("after reopen Terminal/ReopenEpoch = %q/%d, want none/1", s.Terminal, s.ReopenEpoch)
			}
			s.RecordFailedCISHA("def456")
			ev2 := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "def456", ReopenEpoch: 1}
			if got := s.ApplyEvent(ev2); got != DispositionApply {
				t.Fatalf("ApplyEvent after reopen = %q, want %q", got, DispositionApply)
			}
		})
	}
}

// TestPRStateConflictPublished pins the conflict-published slot: a record
// matches its own SHA, an empty query never matches, and a clear drops it.
func TestPRStateConflictPublished(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mark          string
		query         string
		wantPublished bool
	}{
		{name: "mark round trip", mark: "abc123", query: "abc123", wantPublished: true},
		{name: "case insensitive match", mark: "ABC123", query: "abc123", wantPublished: true},
		{name: "other sha misses", mark: "abc123", query: "other-sha"},
		{name: "empty query never matches", mark: "abc123", query: ""},
		{name: "empty mark records nothing", mark: "", query: "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.MarkConflictPublished(tc.mark)
			if got := s.WasConflictPublished(tc.query); got != tc.wantPublished {
				t.Fatalf("WasConflictPublished(%q) = %v, want %v", tc.query, got, tc.wantPublished)
			}
			s.ClearConflictPublished()
			if s.WasConflictPublished(tc.query) {
				t.Fatalf("WasConflictPublished(%q) after clear = true, want false", tc.query)
			}
		})
	}
}

// mainPushSHAs returns n distinct main push SHAs ("sha" + letter + digit + "x1").
func mainPushSHAs(n int) []string {
	shas := make([]string, n)
	for i := range n {
		shas[i] = "sha" + string(rune('a'+i%26)) + string(rune('0'+i/26)) + "x1"
	}
	return shas
}

func TestRepoStateMainPushIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		shas       []string
		want       bool
		wantN      int
		wantLatest string
	}{
		{name: "records fresh sha", shas: []string{"AAA111"}, want: true, wantN: 1, wantLatest: "aaa111"},
		{name: "duplicate sha not new", shas: []string{"aaa111", "aaa111"}, want: false, wantN: 1, wantLatest: "aaa111"},
		{name: "zero sha rejected", shas: []string{"0000000"}, want: false, wantN: 0},
		{name: "empty sha rejected", shas: []string{""}, want: false, wantN: 0},
		{name: "index bounded at fifty", shas: mainPushSHAs(60), want: true, wantN: 50, wantLatest: "shah2x1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRepoState("azylman/aerial")
			var got bool
			for _, sha := range tc.shas {
				got = s.RecordMainPushSHA(sha)
			}
			if got != tc.want {
				t.Fatalf("RecordMainPushSHA = %v, want %v", got, tc.want)
			}
			if n := len(s.MainPushIndex); n != tc.wantN {
				t.Fatalf("index length = %d, want %d", n, tc.wantN)
			}
			if s.LatestMainSHA != tc.wantLatest {
				t.Fatalf("LatestMainSHA = %q, want %q", s.LatestMainSHA, tc.wantLatest)
			}
		})
	}
}

func TestRepoStateTouchPRKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		keys      []string
		roundTrip bool
		wantN     int
	}{
		{name: "touches two distinct keys", keys: []string{"azylman/aerial#101", "azylman/aerial#102"}, roundTrip: false, wantN: 2},
		{name: "duplicate touch idempotent", keys: []string{"azylman/aerial#101", "azylman/aerial#101"}, roundTrip: false, wantN: 1},
		{name: "empty key ignored", keys: []string{""}, roundTrip: false, wantN: 0},
		{name: "restore rebuilds index without duplicates", keys: []string{"azylman/aerial#101"}, roundTrip: true, wantN: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRepoState("azylman/aerial")
			for _, k := range tc.keys {
				s.TouchPRKey(k)
			}
			if tc.roundTrip {
				//nolint:musttag // RepoState is Restate-owned state with intentionally untagged fields; this pins codec survival, not wire names.
				raw, err := json.Marshal(s)
				if err != nil {
					t.Fatalf("marshal RepoState: %v", err)
				}
				restored := NewRepoState("azylman/aerial")
				//nolint:musttag // RepoState is Restate-owned state with intentionally untagged fields; this pins codec survival, not wire names.
				if err := json.Unmarshal(raw, restored); err != nil {
					t.Fatalf("unmarshal RepoState: %v", err)
				}
				restored.touchedSet = nil
				for _, k := range tc.keys {
					restored.TouchPRKey(k)
				}
				s = restored
			}
			if got := s.PRKeys(); len(got) != tc.wantN {
				t.Fatalf("PRKeys length = %d, want %d", len(got), tc.wantN)
			}
		})
	}
}

func TestRepoStateTombstones(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		sha    string
		gen    uint64
		minGen uint64
		query  string
		want   bool
	}{
		{name: "tombstone survives at generation", sha: "abc123", gen: 2, minGen: 2, query: "abc123", want: true},
		{name: "tombstone evicted below generation", sha: "abc123", gen: 1, minGen: 2, query: "abc123", want: false},
		{name: "tombstone absent for another sha", sha: "abc123", gen: 2, minGen: 2, query: "zzz999", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRepoState("azylman/aerial")
			s.RecordTombstone(tc.sha, tc.gen)
			s.EvictTombstonesBelowGeneration(tc.minGen)
			if got := s.HasTombstone(tc.query); got != tc.want {
				t.Fatalf("HasTombstone(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

// results packs a multi-value call into one comparable row value.
func results(vals ...any) []any { return vals }

// TestPRStateNilReceiver pins the degenerate receivers: every PRState method
// is a safe no-op on a nil state, maps initialize lazily on a zero-value
// state, and a zero PR number never records. Mutators run first (they must
// not panic); the rows read the result eagerly.
func TestPRStateNilReceiver(t *testing.T) {
	t.Parallel()
	alert := prwire.EffectDiscordAlert
	var nilState *PRState
	nilState.RollbackFailedCheckRun("abc123", "lint", false, false)
	nilState.MarkConflictPublished("abc123")
	nilState.ClearFailedCISHA()
	nilState.ClearConflictPublished()
	nilState.MarkTerminal(TerminalMerged)
	nilState.Reopen()
	nilState.RetryPRReadiness()
	nilState.MarkEffectDispatched(nilState.EffectKeyFor(alert, "abc123", "", ""))
	zero := &PRState{}
	zero.MarkEffectDispatched(zero.EffectKeyFor(alert, "abc123", "", ""))
	zeroPR := NewPRState("azylman/aerial", 0)
	zeroPR.MarkConflictPublished("abc123")
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "nil HasFailedCISHA", got: nilState.HasFailedCISHA(""), want: false},
		{name: "nil RecordFailedCISHA", got: nilState.RecordFailedCISHA("abc123"), want: false},
		{name: "nil RecordFailedCheckRunDetailed", got: results(nilState.RecordFailedCheckRunDetailed("abc123", "lint")), want: results(false, false, false)},
		{name: "nil ClearFailedCheckRun", got: nilState.ClearFailedCheckRun("abc123", "lint"), want: false},
		{name: "nil HasActiveFailedCheckRuns", got: nilState.HasActiveFailedCheckRuns("abc123"), want: false},
		{name: "nil HasActiveRoboRevFailure", got: nilState.HasActiveRoboRevFailure("abc123"), want: false},
		{name: "nil HasActiveCheckRun", got: nilState.HasActiveCheckRun("abc123", "lint"), want: false},
		{name: "nil HasEverFailedCheckRun", got: nilState.HasEverFailedCheckRun("abc123", "lint"), want: false},
		{name: "nil WasConflictPublished", got: nilState.WasConflictPublished("abc123"), want: false},
		{name: "nil RecordKustomizationRequests", got: nilState.RecordKustomizationRequests(), want: uint64(0)},
		{name: "nil EffectKeyFor", got: nilState.EffectKeyFor(alert, "abc123", "", ""), want: EffectKey{}},
		{name: "nil EffectKeyFor still renders", got: nilState.EffectKeyFor(alert, "abc123", "", "").String() != "", want: true},
		{name: "nil WasEffectDispatched", got: nilState.WasEffectDispatched(nilState.EffectKeyFor(alert, "abc123", "", "")), want: false},
		{name: "nil ApplyEvent drops stale", got: nilState.ApplyEvent(PREvent{}), want: DispositionDropStale},
		{name: "zero value effect dispatches", got: zero.WasEffectDispatched(zero.EffectKeyFor(alert, "abc123", "", "")), want: true},
		{name: "zero value other effect undispatched", got: zero.WasEffectDispatched(zero.EffectKeyFor(prwire.EffectFluxForward, "abc123", "", "")), want: false},
		{name: "zero pr RecordFailedCISHA", got: zeroPR.RecordFailedCISHA("abc123"), want: false},
		{name: "zero pr WasConflictPublished", got: zeroPR.WasConflictPublished("abc123"), want: false},
		{name: "zero pr RecordFailedCheckRunDetailed", got: results(zeroPR.RecordFailedCheckRunDetailed("abc123", "lint")), want: results(false, false, false)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

// TestRepoStateNilReceiver pins the degenerate RepoState receivers: a nil
// state reads empty and ignores writes, a zero-value state initializes its
// index and tombstones lazily, and an empty repo never records a main push.
func TestRepoStateNilReceiver(t *testing.T) {
	t.Parallel()
	var nilRepo *RepoState
	nilRepo.TouchPRKey("azylman/aerial#101")
	nilRepo.RecordTombstone("abc123", 1)
	nilRepo.EvictTombstonesBelowGeneration(2)
	zeroRepo := &RepoState{}
	zeroRepo.TouchPRKey("azylman/aerial#101")
	zeroRepo.RecordTombstone("abc123", 1)
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "nil PRKeys", got: nilRepo.PRKeys(), want: []string(nil)},
		{name: "nil HasTombstone", got: nilRepo.HasTombstone("abc123"), want: false},
		{name: "nil RecordMainPushSHA", got: nilRepo.RecordMainPushSHA("abc123"), want: false},
		{name: "zero value PRKeys", got: zeroRepo.PRKeys(), want: []string{"azylman/aerial#101"}},
		{name: "zero value HasTombstone", got: zeroRepo.HasTombstone("abc123"), want: true},
		{name: "empty repo RecordMainPushSHA", got: NewRepoState("").RecordMainPushSHA("abc123"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

func TestPRState_ClaimOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		state     *PRState
		agent     string
		want      bool
		wantAgent string
	}{
		{name: "claims empty owner", state: &PRState{}, agent: "agent-alpha", want: true, wantAgent: "agent-alpha"},
		{name: "overwrites stored owner", state: &PRState{Agent: "aerial"}, agent: " agent-alpha ", want: true, wantAgent: "agent-alpha"},
		{name: "same owner is a no-op", state: &PRState{Agent: "agent-alpha"}, agent: "agent-alpha", want: false, wantAgent: "agent-alpha"},
		{name: "blank agent never clears owner", state: &PRState{Agent: "agent-alpha"}, agent: "  ", want: false, wantAgent: "agent-alpha"},
		{name: "nil state is a no-op", state: nil, agent: "agent-alpha", want: false, wantAgent: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.state.ClaimOwner(tc.agent); got != tc.want {
				t.Fatalf("ClaimOwner(%q) = %v, want %v", tc.agent, got, tc.want)
			}
			if tc.state != nil && tc.state.Agent != tc.wantAgent {
				t.Fatalf("Agent = %q, want %q", tc.state.Agent, tc.wantAgent)
			}
		})
	}
}

func TestPRState_ApplyAgentHint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		state     *PRState
		agent     string
		want      bool
		wantAgent string
	}{
		{name: "fills empty owner", state: &PRState{}, agent: "agent-beta", want: true, wantAgent: "agent-beta"},
		{name: "never overwrites stored owner", state: &PRState{Agent: "agent-alpha"}, agent: "aerial", want: false, wantAgent: "agent-alpha"},
		{name: "blank hint leaves owner empty", state: &PRState{}, agent: "", want: false, wantAgent: ""},
		{name: "nil state is a no-op", state: nil, agent: "agent-beta", want: false, wantAgent: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.state.ApplyAgentHint(tc.agent); got != tc.want {
				t.Fatalf("ApplyAgentHint(%q) = %v, want %v", tc.agent, got, tc.want)
			}
			if tc.state != nil && tc.state.Agent != tc.wantAgent {
				t.Fatalf("Agent = %q, want %q", tc.state.Agent, tc.wantAgent)
			}
		})
	}
}

func TestPRState_OwnerAgent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		state *PRState
		want  string
	}{
		{name: "stored owner", state: &PRState{Agent: "agent-alpha"}, want: "agent-alpha"},
		{name: "empty owner falls back to aerial", state: &PRState{}, want: "aerial"},
		{name: "nil state falls back to aerial", state: nil, want: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.state.OwnerAgent(); got != tc.want {
				t.Fatalf("OwnerAgent() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsMergeOnlyRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		head  string
		merge string
		want  bool
	}{
		{name: "squash push carries the merge commit as head", head: "c112", merge: "c112", want: true},
		{name: "case and space are ignored", head: " C112 ", merge: "c112", want: true},
		{name: "distinct head is a real head", head: "h112", merge: "c112"},
		{name: "empty head is not a ref", head: "", merge: ""},
		{name: "zero head is not a ref", head: "0000000000000000000000000000000000000000", merge: "0000000000000000000000000000000000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsMergeOnlyRef(tc.head, tc.merge); got != tc.want {
				t.Fatalf("IsMergeOnlyRef(%q, %q) = %v, want %v", tc.head, tc.merge, got, tc.want)
			}
		})
	}
}

func TestPRBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		branch string
		want   string
	}{
		{name: "pr branch is kept", branch: "feat/x", want: "feat/x"},
		{name: "main is dropped", branch: "main", want: ""},
		{name: "main ref is dropped", branch: " refs/heads/main ", want: ""},
		{name: "other refs are kept", branch: "refs/heads/feat/x", want: "refs/heads/feat/x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := prBranch(tc.branch); got != tc.want {
				t.Fatalf("prBranch(%q) = %q, want %q", tc.branch, got, tc.want)
			}
		})
	}
}
