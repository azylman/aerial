package prstate

// Conflict and CI-recovery verdict tests: the Restate PRObject — never the
// Postgres projection — decides whether synapse publishes a conflict
// republish or recovers from CI failure. The conflict verdict records
// nothing, so a publish failure leaves the next invocation returning
// should_publish again (at-least-once); only the publish-recorded ack marks
// the conflict SHA. CI recovery reads the Restate-owned failure slots.

import (
	"testing"
)

func conflictState(pr int, terminal TerminalState, published string) *PRState {
	s := NewPRState("aerial", pr)
	if terminal != TerminalNone {
		s.MarkTerminal(terminal)
	}
	if published != "" {
		s.MarkConflictPublished(published)
	}
	return s
}

func TestResolveConflictDispatchDecision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		nilState          bool
		terminal          TerminalState
		stateHead         string
		published         string
		evHead            string
		wantDisposition   EventDisposition
		wantShouldPublish bool
		wantDeduped       bool
		wantErr           bool
		wantPublished     string
	}{
		{name: "fresh state publishes new sha", evHead: "aaa111", wantDisposition: DispositionApply, wantShouldPublish: true},
		{name: "unstaged head publishes without stale drop", stateHead: "old999", evHead: "aaa111", wantDisposition: DispositionApply, wantShouldPublish: true, wantPublished: ""},
		{name: "same sha dedupes after recorded publish", evHead: "aaa111", published: "aaa111", wantDisposition: DispositionApply, wantDeduped: true, wantPublished: "aaa111"},
		{name: "same sha dedupes case-insensitive", evHead: "AAA111", published: "aaa111", wantDisposition: DispositionApply, wantDeduped: true, wantPublished: "aaa111"},
		{name: "new sha publishes after recorded publish", evHead: "bbb222", published: "aaa111", wantDisposition: DispositionApply, wantShouldPublish: true, wantPublished: "aaa111"},
		{name: "merged terminal absorbs conflict", terminal: TerminalMerged, evHead: "aaa111", wantDisposition: DispositionAbsorbTerminal, wantPublished: ""},
		{name: "closed terminal absorbs conflict", terminal: TerminalClosed, evHead: "aaa111", wantDisposition: DispositionAbsorbTerminal, wantPublished: ""},
		{name: "nil state drops with error", nilState: true, evHead: "aaa111", wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = conflictState(7, tc.terminal, tc.published)
				s.HeadSHA = tc.stateHead
			}
			ev := PREvent{Event: "conflict_detected", Repo: "aerial", PRNumber: 7, HeadSHA: tc.evHead}
			got, err := ResolveConflictDispatchDecision(s, ev)
			if tc.wantErr && err == nil {
				t.Fatal("ResolveConflictDispatchDecision err = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ResolveConflictDispatchDecision err = %v, want nil", err)
			}
			if !tc.wantErr {
				assertConflictDispatch(t, got, tc.wantDisposition, tc.wantShouldPublish, tc.wantDeduped)
				assertConflictResolveRecordedNothing(t, s, tc.nilState, tc.wantPublished)
			}
		})
	}
}

// assertConflictDispatch checks the resolve verdict fields.
func assertConflictDispatch(t *testing.T, got ConflictDispatchDecision, wantDisposition EventDisposition, wantShouldPublish, wantDeduped bool) {
	t.Helper()
	if got.Disposition != wantDisposition {
		t.Fatalf("Disposition = %q, want %q", got.Disposition, wantDisposition)
	}
	if got.ShouldPublish != wantShouldPublish || got.Deduped != wantDeduped {
		t.Fatalf("ShouldPublish/Deduped = %v/%v, want %v/%v", got.ShouldPublish, got.Deduped, wantShouldPublish, wantDeduped)
	}
}

// assertConflictResolveRecordedNothing checks the resolve recorded nothing.
func assertConflictResolveRecordedNothing(t *testing.T, s *PRState, nilState bool, wantPublished string) {
	t.Helper()
	if !nilState && s.ConflictPublished != wantPublished {
		t.Fatalf("ConflictPublished = %q, want %q (resolve must record nothing)", s.ConflictPublished, wantPublished)
	}
}

func TestRecordConflictPublished(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		nilState        bool
		terminal        TerminalState
		stateHead       string
		published       string
		evHead          string
		wantDisposition EventDisposition
		wantRecorded    bool
		wantDeduped     bool
		wantErr         bool
		wantPublished   string
	}{
		{name: "fresh publish records sha", evHead: "aaa111", wantDisposition: DispositionApply, wantRecorded: true, wantPublished: "aaa111"},
		{name: "recorded publish acks idempotent", published: "aaa111", evHead: "aaa111", wantDisposition: DispositionApply, wantRecorded: true, wantDeduped: true, wantPublished: "aaa111"},
		{name: "new sha overwrites recorded sha", published: "aaa111", evHead: "bbb222", wantDisposition: DispositionApply, wantRecorded: true, wantPublished: "bbb222"},
		{name: "unstaged head still records", stateHead: "old999", evHead: "aaa111", wantDisposition: DispositionApply, wantRecorded: true, wantPublished: "aaa111"},
		{name: "merged terminal absorbs without recording", terminal: TerminalMerged, evHead: "aaa111", wantDisposition: DispositionAbsorbTerminal, wantPublished: ""},
		{name: "closed terminal absorbs without recording", terminal: TerminalClosed, evHead: "aaa111", wantDisposition: DispositionAbsorbTerminal, wantPublished: ""},
		{name: "nil state drops with error", nilState: true, evHead: "aaa111", wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = conflictState(7, tc.terminal, tc.published)
				s.HeadSHA = tc.stateHead
			}
			ev := PREvent{Event: "conflict_detected", Repo: "aerial", PRNumber: 7, HeadSHA: tc.evHead}
			got, err := RecordConflictPublished(s, ev)
			if tc.wantErr {
				if err == nil {
					t.Fatal("RecordConflictPublished err = nil, want error")
				}
			} else {
				if err != nil {
					t.Fatalf("RecordConflictPublished err = %v, want nil", err)
				}
				assertConflictPublished(t, got, tc.wantDisposition, tc.wantRecorded, tc.wantDeduped)
				assertConflictPublishedState(t, s, tc.nilState, tc.wantPublished)
			}
		})
	}
}

// assertConflictPublished checks the publish-recorded outcome fields.
func assertConflictPublished(t *testing.T, got ConflictPublishedOutcome, wantDisposition EventDisposition, wantRecorded, wantDeduped bool) {
	t.Helper()
	if got.Disposition != wantDisposition {
		t.Fatalf("Disposition = %q, want %q", got.Disposition, wantDisposition)
	}
	if got.Recorded != wantRecorded || got.Deduped != wantDeduped {
		t.Fatalf("Recorded/Deduped = %v/%v, want %v/%v", got.Recorded, got.Deduped, wantRecorded, wantDeduped)
	}
}

// assertConflictPublishedState checks the recorded conflict SHA.
func assertConflictPublishedState(t *testing.T, s *PRState, nilState bool, wantPublished string) {
	t.Helper()
	if !nilState && s.ConflictPublished != wantPublished {
		t.Fatalf("ConflictPublished = %q, want %q", s.ConflictPublished, wantPublished)
	}
}

func recoveryState(pr int, failedSHA, checksHead string, activeChecks []string, roborevActive bool) *PRState {
	s := NewPRState("aerial", pr)
	if failedSHA != "" {
		s.RecordFailedCISHA(failedSHA)
	}
	if checksHead != "" {
		for _, c := range activeChecks {
			s.RecordFailedCheckRunDetailed(checksHead, c)
		}
		if roborevActive {
			s.RecordFailedCheckRunDetailed(checksHead, "roborev")
		}
	}
	return s
}

func TestIsCIFailedForSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilState  bool
		failedSHA string
		checks    []string
		roborev   bool
		querySHA  string
		want      bool
	}{
		{name: "recorded failed sha matches query", failedSHA: "aaa111", querySHA: "aaa111", want: true},
		{name: "recorded failed sha mismatches query", failedSHA: "aaa111", querySHA: "bbb222"},
		{name: "empty query matches any recorded failure", failedSHA: "aaa111", want: true},
		{name: "empty query without failure is clean", querySHA: "aaa111"},
		{name: "active check run matches query", checks: []string{"lint"}, querySHA: "aaa111", want: true},
		{name: "active roborev failure matches query", roborev: true, querySHA: "aaa111", want: true},
		{name: "active checks mismatch query", checks: []string{"lint"}, querySHA: "bbb222"},
		{name: "zero sha query never matches", failedSHA: "0000000", querySHA: "0000000"},
		{name: "nil state never failed", nilState: true, querySHA: "aaa111"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				head := tc.failedSHA
				if len(tc.checks) > 0 || tc.roborev {
					head = "aaa111"
				}
				s = recoveryState(7, tc.failedSHA, head, tc.checks, tc.roborev)
			}
			if got := s.IsCIFailedForSHA(tc.querySHA); got != tc.want {
				t.Fatalf("IsCIFailedForSHA(%q) = %v, want %v", tc.querySHA, got, tc.want)
			}
		})
	}
}

func TestResolveCIRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilState  bool
		failedSHA string
		querySHA  string
		want      bool
		wantErr   bool
	}{
		{name: "failed sha recovers", failedSHA: "aaa111", querySHA: "aaa111", want: true},
		{name: "sha mismatch does not recover", failedSHA: "aaa111", querySHA: "bbb222"},
		{name: "clean state does not recover", querySHA: "aaa111"},
		{name: "nil state errors", nilState: true, querySHA: "aaa111", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = recoveryState(7, tc.failedSHA, "", nil, false)
			}
			ev := PREvent{Event: "check_success", Repo: "aerial", PRNumber: 7, HeadSHA: tc.querySHA}
			got, err := ResolveCIRecovery(s, ev)
			if tc.wantErr && err == nil {
				t.Fatal("ResolveCIRecovery err = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ResolveCIRecovery err = %v, want nil", err)
			}
			if !tc.wantErr && got.CIFailed != tc.want {
				t.Fatalf("CIFailed = %v, want %v", got.CIFailed, tc.want)
			}
			if !tc.wantErr && got.Disposition != DispositionApply {
				t.Fatalf("Disposition = %q, want %q", got.Disposition, DispositionApply)
			}
		})
	}
}
