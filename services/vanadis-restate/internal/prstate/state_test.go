package prstate

import (
	"encoding/json"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

func TestPRStateRecordFailedCISHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		repo string
		pr   int
		sha  string
		want bool
	}{
		{name: "records fresh sha", repo: "azylman/aerial", pr: 101, sha: "abc123", want: true},
		{name: "rejects empty sha", repo: "azylman/aerial", pr: 101, sha: "", want: false},
		{name: "rejects zero sha", repo: "azylman/aerial", pr: 101, sha: "0000000", want: false},
		{name: "rejects zero pr", repo: "azylman/aerial", pr: 0, sha: "abc123", want: false},
		{name: "rejects empty repo", repo: "", pr: 101, sha: "abc123", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState(tc.repo, tc.pr)
			if got := s.RecordFailedCISHA(tc.sha); got != tc.want {
				t.Fatalf("RecordFailedCISHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		})
	}
}

func TestPRStateRecordFailedCISHAChange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		first  string
		second string
		want   bool
	}{
		{name: "same sha not new", first: "abc123", second: "abc123", want: false},
		{name: "same sha different case not new", first: "ABC123", second: "abc123", want: false},
		{name: "changed sha is new", first: "abc123", second: "def456", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCISHA(tc.first)
			if got := s.RecordFailedCISHA(tc.second); got != tc.want {
				t.Fatalf("second RecordFailedCISHA = %v, want %v", got, tc.want)
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

func TestPRStateFailedCheckRunDetailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		firstCheck         string
		secondCheck        string
		wantEmit           bool
		wantWasActive      bool
		wantHadPriorEver   bool
		secondEmit         bool
		secondWasActive    bool
		secondHadPriorEver bool
	}{
		{name: "first failure no emit not active no prior", firstCheck: "lint", secondCheck: "", wantEmit: false, wantWasActive: false, wantHadPriorEver: false},
		{name: "repeat failure emits was active", firstCheck: "lint", secondCheck: "lint", wantEmit: false, wantWasActive: false, wantHadPriorEver: false, secondEmit: false, secondWasActive: true, secondHadPriorEver: true},
		{name: "second distinct check emits", firstCheck: "lint", secondCheck: "build", wantEmit: false, wantWasActive: false, wantHadPriorEver: false, secondEmit: true, secondWasActive: false, secondHadPriorEver: false},
		{name: "roborev first no emit", firstCheck: "roborev", secondCheck: "", wantEmit: false, wantWasActive: false, wantHadPriorEver: false},
		{name: "roborev repeat was active", firstCheck: "roborev", secondCheck: "roborev", wantEmit: false, wantWasActive: false, wantHadPriorEver: false, secondEmit: false, secondWasActive: true, secondHadPriorEver: true},
		{name: "non roborev after roborev emits", firstCheck: "roborev", secondCheck: "lint", wantEmit: false, wantWasActive: false, wantHadPriorEver: false, secondEmit: true, secondWasActive: false, secondHadPriorEver: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			emit, wasActive, hadPrior := s.RecordFailedCheckRunDetailed("abc123", tc.firstCheck)
			if emit != tc.wantEmit || wasActive != tc.wantWasActive || hadPrior != tc.wantHadPriorEver {
				t.Fatalf("first record = (%v,%v,%v), want (%v,%v,%v)", emit, wasActive, hadPrior, tc.wantEmit, tc.wantWasActive, tc.wantHadPriorEver)
			}
			if tc.secondCheck == "" {
				return
			}
			emit2, wasActive2, hadPrior2 := s.RecordFailedCheckRunDetailed("abc123", tc.secondCheck)
			if emit2 != tc.secondEmit || wasActive2 != tc.secondWasActive || hadPrior2 != tc.secondHadPriorEver {
				t.Fatalf("second record = (%v,%v,%v), want (%v,%v,%v)", emit2, wasActive2, hadPrior2, tc.secondEmit, tc.secondWasActive, tc.secondHadPriorEver)
			}
		})
	}
}

func TestPRStateFailedCheckRunSHAReset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		check     string
		wantPrior bool
	}{
		{name: "new sha resets ever failed for lint", check: "lint", wantPrior: false},
		{name: "new sha resets roborev ever failed", check: "roborev", wantPrior: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCheckRunDetailed("abc123", tc.check)
			_, _, hadPrior := s.RecordFailedCheckRunDetailed("def456", tc.check)
			if hadPrior != tc.wantPrior {
				t.Fatalf("hadPrior after SHA change = %v, want %v", hadPrior, tc.wantPrior)
			}
			if s.Checks.HeadSHA != "def456" {
				t.Fatalf("HeadSHA = %q, want %q", s.Checks.HeadSHA, "def456")
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
			s.RecordNonRoboRevFailedSHA(tc.recordSHA)
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

func TestPRStateClearFailedCISHAFanout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sha  string
	}{
		{name: "clears all tracking for sha", sha: "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCISHA(tc.sha)
			s.RecordNonRoboRevFailedSHA(tc.sha)
			s.RecordCancelledCheckSHA(tc.sha)
			s.RecordFailedCheckRunDetailed(tc.sha, "lint")
			s.MarkCollaboratorAlerted(tc.sha)
			s.MarkCollaboratorConflict(tc.sha)
			s.MarkConflictPublished(tc.sha)
			s.MarkUpdatedForMain()
			s.ClearFailedCISHA()
			if s.HasFailedCISHA("") {
				t.Fatalf("HasFailedCISHA still true after clear")
			}
			if s.HasNonRoboRevFailedSHA("") {
				t.Fatalf("non-roborev SHA still tracked after clear")
			}
			if s.HasCancelledCheckSHA(tc.sha) {
				t.Fatalf("cancelled SHA still tracked after clear")
			}
			if s.HasActiveFailedCheckRuns(tc.sha) {
				t.Fatalf("active check runs still tracked after clear")
			}
			if s.WasCollaboratorAlerted(tc.sha) {
				t.Fatalf("collaborator alert still tracked after clear")
			}
			if s.WasCollaboratorConflict(tc.sha) {
				t.Fatalf("collaborator conflict still tracked after clear")
			}
			if s.WasConflictPublished(tc.sha) {
				t.Fatalf("conflict published still tracked after clear")
			}
			if !s.WasUpdatedForMain() {
				t.Fatalf("updated-for-main must survive ClearFailedCISHA")
			}
		})
	}
}

func TestPRStateClearActiveCheckRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		record     string
		clear      string
		sha        string
		wantActive bool
		wantEver   bool
	}{
		{name: "clears matching non roborev", record: "lint", clear: "lint", sha: "abc123", wantActive: false, wantEver: true},
		{name: "case insensitive clear", record: "Lint", clear: "lint", sha: "abc123", wantActive: false, wantEver: true},
		{name: "stale sha keeps active", record: "lint", clear: "lint", sha: "def456", wantActive: true, wantEver: true},
		{name: "empty clear noop", record: "lint", clear: "", sha: "abc123", wantActive: true, wantEver: true},
		{name: "clears roborev active keeps ever", record: "roborev", clear: "roborev", sha: "abc123", wantActive: false, wantEver: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCheckRunDetailed("abc123", tc.record)
			s.ClearActiveFailedCheckRun(tc.sha, tc.clear)
			if got := s.HasActiveCheckRun("abc123", tc.record); got != tc.wantActive {
				t.Fatalf("HasActiveCheckRun = %v, want %v", got, tc.wantActive)
			}
			if got := s.HasEverFailedCheckRun("abc123", tc.record); got != tc.wantEver {
				t.Fatalf("HasEverFailedCheckRun = %v, want %v", got, tc.wantEver)
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
		name          string
		checks        []string
		sha           string
		wantAny       bool
		wantNonRoboRO bool
		wantRoboRev   bool
	}{
		{name: "no checks nothing active", checks: nil, sha: "abc123", wantAny: false, wantNonRoboRO: false, wantRoboRev: false},
		{name: "lint active non roborev", checks: []string{"lint"}, sha: "abc123", wantAny: true, wantNonRoboRO: true, wantRoboRev: false},
		{name: "roborev only", checks: []string{"roborev"}, sha: "abc123", wantAny: true, wantNonRoboRO: false, wantRoboRev: true},
		{name: "stale sha nothing active", checks: []string{"lint"}, sha: "def456", wantAny: false, wantNonRoboRO: false, wantRoboRev: false},
		{name: "empty sha nothing active", checks: []string{"lint"}, sha: "", wantAny: false, wantNonRoboRO: false, wantRoboRev: false},
		{name: "zero sha nothing active", checks: []string{"lint", "roborev"}, sha: "0000000", wantAny: false, wantNonRoboRO: false, wantRoboRev: false},
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
			if got := s.HasActiveNonRoboRevFailures(tc.sha); got != tc.wantNonRoboRO {
				t.Fatalf("HasActiveNonRoboRevFailures = %v, want %v", got, tc.wantNonRoboRO)
			}
			if got := s.HasActiveRoboRevFailure(tc.sha); got != tc.wantRoboRev {
				t.Fatalf("HasActiveRoboRevFailure = %v, want %v", got, tc.wantRoboRev)
			}
		})
	}
}

func TestPRStateCanRecoverNonGateCheckSuccess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   []string
		recover string
		sha     string
		want    bool
	}{
		{name: "sole lint success recovers", setup: []string{"lint"}, recover: "lint", sha: "abc123", want: true},
		{name: "lint success with build still failing does not recover", setup: []string{"lint", "build"}, recover: "lint", sha: "abc123", want: false},
		{name: "lint success with roborev active does not recover", setup: []string{"lint", "roborev"}, recover: "lint", sha: "abc123", want: false},
		{name: "unknown check never recovers", setup: []string{"lint"}, recover: "build", sha: "abc123", want: false},
		{name: "stale sha never recovers", setup: []string{"lint"}, recover: "lint", sha: "def456", want: false},
		{name: "empty check never recovers", setup: []string{"lint"}, recover: "", sha: "abc123", want: false},
		{name: "zero sha never recovers", setup: []string{"lint"}, recover: "lint", sha: "0000000", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			for _, c := range tc.setup {
				s.RecordFailedCheckRunDetailed("abc123", c)
			}
			if got := s.CanRecoverNonGateCheckSuccess(tc.recover, tc.sha); got != tc.want {
				t.Fatalf("CanRecoverNonGateCheckSuccess = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPRStateGenerationGating(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		kind       string
		wantGen1   uint64
		wantGen2   uint64
		wantOldKey bool
	}{
		{name: "discord alert keyed to generation", kind: prwire.EffectDiscordAlert, wantGen1: 1, wantGen2: 2, wantOldKey: false},
		{name: "deploy dispatch keyed to generation", kind: prwire.EffectDeployDispatch, wantGen1: 1, wantGen2: 2, wantOldKey: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			gen := s.RecordKustomizationRequests()
			if gen != tc.wantGen1 {
				t.Fatalf("first generation = %d, want %d", gen, tc.wantGen1)
			}
			k1 := s.EffectKeyFor(tc.kind, "abc123", "eri", "lint")
			s.MarkEffectDispatched(k1)
			if !s.WasEffectDispatched(k1) {
				t.Fatalf("effect key should be dispatched in generation %d", gen)
			}
			gen2 := s.RecordKustomizationRequests()
			if gen2 != tc.wantGen2 {
				t.Fatalf("second generation = %d, want %d", gen2, tc.wantGen2)
			}
			if got := s.WasEffectDispatched(k1); got != tc.wantOldKey {
				t.Fatalf("old generation key dispatched = %v, want %v", got, tc.wantOldKey)
			}
			k2 := s.EffectKeyFor(tc.kind, "abc123", "eri", "lint")
			if s.WasEffectDispatched(k2) {
				t.Fatalf("new generation key must start undispatched")
			}
		})
	}
}

func TestPRStateRetryNeverMintsGeneration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sha  string
	}{
		{name: "retry preserves generation and keys", sha: "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			gen := s.RecordKustomizationRequests()
			k := s.EffectKeyFor(prwire.EffectDiscordAlert, tc.sha, "", "")
			s.MarkEffectDispatched(k)
			s.RetryPRReadiness()
			if s.GateGeneration != gen {
				t.Fatalf("GateGeneration = %d, want %d", s.GateGeneration, gen)
			}
			if !s.WasEffectDispatched(k) {
				t.Fatalf("effect key must survive readiness retry")
			}
		})
	}
}

func TestPRStateDeployDedupBothKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		job     string
		sha     string
		wantPR  bool
		wantSHA bool
	}{
		{name: "pr key alone dispatches", job: "", sha: "", wantPR: true, wantSHA: false},
		{name: "sha job key alone dispatches", job: "eri", sha: "abc123", wantPR: true, wantSHA: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordDeployedPR()
			s.RecordDeployedSHAJob(tc.job, tc.sha)
			if got := s.WasDeployedPR(); got != tc.wantPR {
				t.Fatalf("WasDeployedPR = %v, want %v", got, tc.wantPR)
			}
			if tc.job != "" {
				if got := s.WasDeployedSHAJob(tc.job, tc.sha); got != tc.wantSHA {
					t.Fatalf("WasDeployedSHAJob = %v, want %v", got, tc.wantSHA)
				}
				if got := s.WasDeployedSHAJob(tc.job, "other-sha"); got {
					t.Fatalf("WasDeployedSHAJob(other) = true, want false")
				}
			}
		})
	}
}

func TestPRStateTerminalAndReopen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		terminal TerminalState
		epoch    uint64
		want     EventDisposition
	}{
		{name: "merged absorbs same epoch", terminal: TerminalMerged, epoch: 0, want: DispositionAbsorbTerminal},
		{name: "closed absorbs same epoch", terminal: TerminalClosed, epoch: 0, want: DispositionAbsorbTerminal},
		{name: "newer epoch applies through terminal", terminal: TerminalMerged, epoch: 1, want: DispositionApply},
		{name: "no terminal applies", terminal: TerminalNone, epoch: 0, want: DispositionApply},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCISHA("abc123")
			if tc.terminal != TerminalNone {
				s.MarkTerminal(tc.terminal)
			}
			ev := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", ReopenEpoch: tc.epoch}
			if got := s.ApplyEvent(ev); got != tc.want {
				t.Fatalf("ApplyEvent = %q, want %q", got, tc.want)
			}
			if tc.want == DispositionAbsorbTerminal {
				s.Reopen()
				if s.Terminal != TerminalNone {
					t.Fatalf("Terminal after reopen = %q, want none", s.Terminal)
				}
				if s.ReopenEpoch != 1 {
					t.Fatalf("ReopenEpoch = %d, want 1", s.ReopenEpoch)
				}
				s.RecordFailedCISHA("def456")
				ev2 := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "def456", ReopenEpoch: 1}
				if got := s.ApplyEvent(ev2); got != DispositionApply {
					t.Fatalf("ApplyEvent after reopen = %q, want %q", got, DispositionApply)
				}
			}
		})
	}
}

func TestPRStateApplyEventStale(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		current string
		stored  string
		event   string
		merge   string
		want    EventDisposition
	}{
		{name: "current sha applies", current: "abc123", event: "abc123", merge: "", want: DispositionApply},
		{name: "stale sha drops", current: "abc123", event: "def456", merge: "", want: DispositionDropStale},
		{name: "no current head applies", current: "", event: "abc123", merge: "", want: DispositionApply},
		{name: "merge sha match applies", current: "abc123", event: "", merge: "abc123", want: DispositionApply},
		{name: "merge-only sha before the merge is known applies", current: "abc123", event: "", merge: "zzz999", want: DispositionApply},
		{name: "merge-only sha matching the stored merge applies", current: "abc123", stored: "zzz999", event: "", merge: "zzz999", want: DispositionApply},
		{name: "merge-only sha against another stored merge drops", current: "abc123", stored: "mmm111", event: "", merge: "zzz999", want: DispositionDropStale},
		{name: "squash push carrying the merge sha as head applies", current: "abc123", event: "zzz999", merge: "zzz999", want: DispositionApply},
		{name: "squash push against another stored merge drops", current: "abc123", stored: "mmm111", event: "zzz999", merge: "zzz999", want: DispositionDropStale},
		{name: "empty event applies", current: "abc123", event: "", merge: "", want: DispositionApply},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			s.RecordFailedCISHA(tc.current)
			s.MergeSHA = tc.stored
			ev := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: tc.event, MergeSHA: tc.merge}
			if got := s.ApplyEvent(ev); got != tc.want {
				t.Fatalf("ApplyEvent = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPRStateCollaboratorAndConflict(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sha  string
		want bool
	}{
		{name: "alert round trip", sha: "abc123", want: true},
		{name: "zero sha never alerts", sha: "0000000", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			if got := s.MarkCollaboratorAlerted(tc.sha); got != tc.want {
				t.Fatalf("MarkCollaboratorAlerted = %v, want %v", got, tc.want)
			}
			if got := s.WasCollaboratorAlerted(tc.sha); got != tc.want {
				t.Fatalf("WasCollaboratorAlerted = %v, want %v", got, tc.want)
			}
			s.ClearCollaboratorAlerted(tc.sha)
			if s.WasCollaboratorAlerted(tc.sha) {
				t.Fatalf("WasCollaboratorAlerted after clear = true, want false")
			}
			if got := s.MarkCollaboratorConflict(tc.sha); got != tc.want {
				t.Fatalf("MarkCollaboratorConflict = %v, want %v", got, tc.want)
			}
			if got := s.WasCollaboratorConflict(tc.sha); got != tc.want {
				t.Fatalf("WasCollaboratorConflict = %v, want %v", got, tc.want)
			}
			s.ClearCollaboratorConflict(tc.sha)
			s.MarkConflictPublished(tc.sha)
			if got, want := s.WasConflictPublished(tc.sha), tc.sha != ""; got != want {
				t.Fatalf("WasConflictPublished = %v, want %v", got, want)
			}
			s.ClearConflictPublished()
		})
	}
}

func TestPRStateCachedFailedCISHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		setupFail string
		setupNon  string
		setupCan  string
		query     string
		want      bool
	}{
		{name: "failed sha cached", setupFail: "abc123", query: "abc123", want: true},
		{name: "non roborev sha cached", setupNon: "abc123", query: "abc123", want: true},
		{name: "cancelled sha cached", setupCan: "abc123", query: "abc123", want: true},
		{name: "unknown sha not cached", query: "zzz999", want: false},
		{name: "zero sha never cached", setupFail: "abc123", query: "0000000", want: false},
		{name: "empty query not cached", setupFail: "abc123", query: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			if tc.setupFail != "" {
				s.RecordFailedCISHA(tc.setupFail)
			}
			if tc.setupNon != "" {
				s.RecordNonRoboRevFailedSHA(tc.setupNon)
			}
			if tc.setupCan != "" {
				s.RecordCancelledCheckSHA(tc.setupCan)
			}
			if got := s.IsCachedFailedCISHA(tc.query); got != tc.want {
				t.Fatalf("IsCachedFailedCISHA(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestRepoStateMainPushIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		shas  []string
		want  bool
		wantN int
	}{
		{name: "records fresh sha", shas: []string{"aaa111"}, want: true, wantN: 1},
		{name: "duplicate sha not new", shas: []string{"aaa111", "aaa111"}, want: false, wantN: 1},
		{name: "zero sha rejected", shas: []string{"0000000"}, want: false, wantN: 0},
		{name: "empty sha rejected", shas: []string{""}, want: false, wantN: 0},
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
		})
	}
}

func TestRepoStateMainPushBounded(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		count int
		wantN int
	}{
		{name: "index bounded at fifty", count: 60, wantN: 50},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRepoState("azylman/aerial")
			for i := range tc.count {
				s.RecordMainPushSHA("sha" + string(rune('a'+i%26)) + string(rune('0'+i/26)) + "x1")
			}
			if n := len(s.MainPushIndex); n != tc.wantN {
				t.Fatalf("index length = %d, want %d", n, tc.wantN)
			}
			if s.LatestMainSHA == "" {
				t.Fatalf("LatestMainSHA empty after pushes")
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
		want   bool
	}{
		{name: "tombstone survives at generation", sha: "abc123", gen: 2, minGen: 2, want: true},
		{name: "tombstone evicted below generation", sha: "abc123", gen: 1, minGen: 2, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRepoState("azylman/aerial")
			s.RecordTombstone(tc.sha, tc.gen)
			s.EvictTombstonesBelowGeneration(tc.minGen)
			if got := s.HasTombstone(tc.sha); got != tc.want {
				t.Fatalf("HasTombstone = %v, want %v", got, tc.want)
			}
		})
	}
}

func checkRecordedSlotEdges(t *testing.T, s *PRState) {
	t.Helper()
	s.RecordNonRoboRevFailedSHA("")
	s.RecordCancelledCheckSHA("0000000")
	s.RecordFailedCISHA("0000000")
	if s.HasNonRoboRevFailedSHA("zzz999") {
		t.Fatalf("mismatched non-roborev query must miss")
	}
	s.RecordFailedCheckRunDetailed("abc123", "lint")
	if !s.HasEverFailedCheckRun("abc123", "lint") {
		t.Fatalf("lint must be ever-failed")
	}
	if s.HasEverFailedCheckRun("0000000", "lint") {
		t.Fatalf("zero sha must never be ever-failed")
	}
	if s.HasEverFailedCheckRun("abc123", "") {
		t.Fatalf("empty check must never be ever-failed")
	}
	if s.HasActiveCheckRun("0000000", "lint") {
		t.Fatalf("zero sha must never be active")
	}
	if s.HasActiveCheckRun("abc123", "") {
		t.Fatalf("empty check must never be active")
	}
	s.ClearActiveFailedCheckRun("abc123", "lint")
	if !s.MarkCollaboratorAlerted("abc123") {
		t.Fatalf("first alert mark must report true")
	}
	if s.MarkCollaboratorAlerted("abc123") {
		t.Fatalf("duplicate alert mark must report false")
	}
	if !s.MarkCollaboratorConflict("abc123") {
		t.Fatalf("first conflict mark must report true")
	}
	if s.MarkCollaboratorConflict("abc123") {
		t.Fatalf("duplicate conflict mark must report false")
	}
	s.MarkConflictPublished("abc123")
	if s.WasConflictPublished("") {
		t.Fatalf("empty conflict query must miss a recorded sha")
	}
}

func TestPRStateMiscSlots(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		record string
		wantOK bool
	}{
		{name: "get recorded failed sha", record: "abc123", wantOK: true},
		{name: "empty record misses", record: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewPRState("azylman/aerial", 101)
			if tc.record != "" {
				s.RecordFailedCISHA(tc.record)
				s.RecordNonRoboRevFailedSHA(tc.record)
				s.RecordCancelledCheckSHA(tc.record)
			}
			if got, ok := s.GetFailedCISHA(); ok != tc.wantOK || (ok && got != tc.record) {
				t.Fatalf("GetFailedCISHA = (%q,%v), want (%q,%v)", got, ok, tc.record, tc.wantOK)
			}
			if tc.record == "" {
				return
			}
			checkRecordedSlotEdges(t, s)
			s.DeleteNonRoboRevFailedSHA()
			if s.HasNonRoboRevFailedSHA("") {
				t.Fatalf("non-roborev slot survives delete")
			}
			s.DeleteCancelledCheckSHA()
			if s.HasCancelledCheckSHA("abc123") {
				t.Fatalf("cancelled slot survives delete")
			}
			s.MarkCollaboratorAlerted("abc123")
			s.ClearCollaboratorAlerted("other-sha")
			if !s.WasCollaboratorAlerted("") {
				t.Fatalf("empty query must match recorded alert")
			}
			s.MarkCollaboratorConflict("abc123")
			s.ClearCollaboratorConflict("other-sha")
			if !s.WasCollaboratorConflict("") {
				t.Fatalf("empty query must match recorded conflict")
			}
			s.MarkUpdatedForMain()
			if !s.WasUpdatedForMain() {
				t.Fatalf("updated-for-main not set")
			}
			s.ClearUpdatedForMain()
			if s.WasUpdatedForMain() {
				t.Fatalf("updated-for-main survives clear")
			}
			checkNilPRState(t)
		})
	}
}

func checkZeroValuePRState(t *testing.T) {
	t.Helper()
	zero := &PRState{}
	zero.MarkEffectDispatched(zero.EffectKeyFor(prwire.EffectDiscordAlert, "abc123", "", ""))
	if !zero.WasEffectDispatched(zero.EffectKeyFor(prwire.EffectDiscordAlert, "abc123", "", "")) {
		t.Fatalf("zero-value effect must dispatch")
	}
	if zero.WasEffectDispatched(zero.EffectKeyFor(prwire.EffectFluxForward, "abc123", "", "")) {
		t.Fatalf("zero-value other effect must be undispatched")
	}
	zero.RecordDeployedSHAJob("eri", "abc123")
	if !zero.WasDeployedSHAJob("eri", "abc123") || zero.WasDeployedSHAJob("eri", "zzz999") {
		t.Fatalf("zero-value sha job dedup mismatch")
	}
	zeroPR := NewPRState("azylman/aerial", 0)
	if zeroPR.RecordFailedCISHA("abc123") || zeroPR.MarkCollaboratorAlerted("abc123") || zeroPR.MarkCollaboratorConflict("abc123") {
		t.Fatalf("zero-pr marks must report false")
	}
	zeroPR.MarkConflictPublished("abc123")
	if zeroPR.WasConflictPublished("abc123") || zeroPR.IsCachedFailedCISHA("abc123") {
		t.Fatalf("zero-pr queries must miss")
	}
	if emit, active, prior := zeroPR.RecordFailedCheckRunDetailed("abc123", "lint"); emit || active || prior {
		t.Fatalf("zero-pr record = (%v,%v,%v), want zeros", emit, active, prior)
	}
}

func checkNilPRState(t *testing.T) {
	t.Helper()
	var nilState *PRState
	if nilState.HasFailedCISHA("") || nilState.WasUpdatedForMain() {
		t.Fatalf("nil state must report empty")
	}
	if _, ok := nilState.GetFailedCISHA(); ok {
		t.Fatalf("nil GetFailedCISHA = true, want false")
	}
	if nilState.HasNonRoboRevFailedSHA("") || nilState.HasCancelledCheckSHA("abc123") {
		t.Fatalf("nil slots must report empty")
	}
	if nilState.IsCachedFailedCISHA("abc123") {
		t.Fatalf("nil cached check must miss")
	}
	if emit, active, prior := nilState.RecordFailedCheckRunDetailed("abc123", "lint"); emit || active || prior {
		t.Fatalf("nil record = (%v,%v,%v), want zeros", emit, active, prior)
	}
	nilState.RollbackFailedCheckRun("abc123", "lint", false, false)
	nilState.ClearActiveFailedCheckRun("abc123", "lint")
	if nilState.ClearFailedCheckRun("abc123", "lint") {
		t.Fatalf("nil clear must report false")
	}
	if nilState.HasActiveFailedCheckRuns("abc123") || nilState.HasActiveNonRoboRevFailures("abc123") || nilState.HasActiveRoboRevFailure("abc123") {
		t.Fatalf("nil active queries must miss")
	}
	if nilState.HasActiveCheckRun("abc123", "lint") || nilState.HasEverFailedCheckRun("abc123", "lint") {
		t.Fatalf("nil per-check queries must miss")
	}
	if nilState.CanRecoverNonGateCheckSuccess("lint", "abc123") {
		t.Fatalf("nil recover must report false")
	}
	checkNilPRMutations(t, nilState)
}

func checkNilPRMutations(t *testing.T, nilState *PRState) {
	t.Helper()
	if nilState.WasCollaboratorAlerted("abc123") || nilState.WasCollaboratorConflict("abc123") || nilState.WasConflictPublished("abc123") {
		t.Fatalf("nil collaborator queries must miss")
	}
	if nilState.RecordFailedCISHA("abc123") || nilState.MarkCollaboratorAlerted("abc123") || nilState.MarkCollaboratorConflict("abc123") {
		t.Fatalf("nil marks must report false")
	}
	nilState.MarkUpdatedForMain()
	nilState.RecordNonRoboRevFailedSHA("abc123")
	nilState.RecordCancelledCheckSHA("abc123")
	nilState.MarkConflictPublished("abc123")
	if nilState.RecordKustomizationRequests() != 0 {
		t.Fatalf("nil generation must stay zero")
	}
	if nilState.WasEffectDispatched(nilState.EffectKeyFor(prwire.EffectDiscordAlert, "abc123", "", "")) {
		t.Fatalf("nil effect must be undispatched")
	}
	if nilState.WasDeployedPR() || nilState.WasDeployedSHAJob("eri", "abc123") {
		t.Fatalf("nil deploy dedup must miss")
	}
	checkZeroValuePRState(t)
	if k := nilState.EffectKeyFor(prwire.EffectDiscordAlert, "abc123", "", ""); k.String() == "" {
		t.Fatalf("nil EffectKeyFor must still render")
	}
	nilState.ClearFailedCISHA()
	nilState.DeleteNonRoboRevFailedSHA()
	nilState.DeleteCancelledCheckSHA()
	nilState.ClearCollaboratorAlerted("")
	nilState.ClearCollaboratorConflict("")
	nilState.ClearConflictPublished()
	nilState.ClearUpdatedForMain()
	nilState.MarkTerminal(TerminalMerged)
	nilState.Reopen()
	nilState.RetryPRReadiness()
	nilState.MarkEffectDispatched(nilState.EffectKeyFor(prwire.EffectDiscordAlert, "abc123", "", ""))
	nilState.RecordDeployedPR()
	nilState.RecordDeployedSHAJob("eri", "abc123")
	if nilState.ApplyEvent(PREvent{}) != DispositionDropStale {
		t.Fatalf("nil ApplyEvent must drop stale")
	}
}

func TestRepoStateMiscSlots(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sha  string
		want bool
	}{
		{name: "tombstone present", sha: "abc123", want: true},
		{name: "tombstone absent", sha: "zzz999", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewRepoState("azylman/aerial")
			s.RecordTombstone("abc123", 2)
			if got := s.HasTombstone(tc.sha); got != tc.want {
				t.Fatalf("HasTombstone(%q) = %v, want %v", tc.sha, got, tc.want)
			}
			keys := s.PRKeys()
			if len(keys) != 0 {
				t.Fatalf("PRKeys = %v, want empty", keys)
			}
			emptyRepo := NewRepoState("")
			if emptyRepo.RecordMainPushSHA("abc123") {
				t.Fatalf("empty repo push must report false")
			}
			zeroRepo := &RepoState{}
			zeroRepo.TouchPRKey("azylman/aerial#101")
			if got := zeroRepo.PRKeys(); len(got) != 1 {
				t.Fatalf("zero-value PRKeys = %v, want one key", got)
			}
			zeroRepo.RecordTombstone("abc123", 1)
			if !zeroRepo.HasTombstone("abc123") {
				t.Fatalf("zero-value tombstone must record")
			}
			var nilRepo *RepoState
			if nilRepo.PRKeys() != nil || nilRepo.HasTombstone("abc123") {
				t.Fatalf("nil repo state must report empty")
			}
			nilRepo.TouchPRKey("azylman/aerial#101")
			nilRepo.RecordTombstone("abc123", 1)
			nilRepo.EvictTombstonesBelowGeneration(2)
			if nilRepo.RecordMainPushSHA("abc123") {
				t.Fatalf("nil RecordMainPushSHA = true, want false")
			}
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
