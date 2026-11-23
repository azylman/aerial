package prstate

// Conflict-republish and CI-recovery verdicts. PRObject is the single
// ordered authority for per-PR conflict and failure state; the Postgres
// pr_registry stays a read-only projection and is never consulted to decide
// whether a conflict republish or a CI recovery fires.
//
// HandleConflict and HandleCIRecovered resolve the verdict without
// recording anything, then commit and publish only when it fires (see
// runVerdict). Terminal states absorb same-epoch events with nothing
// published and nothing recorded.

// ConflictDispatchDecision is the conflict-republish verdict HandleConflict
// returns. PRObject computes it from the recorded conflict SHA before
// recording anything; only a ShouldPublish verdict commits
// (RecordConflictPublished) and publishes.
type ConflictDispatchDecision struct {
	Disposition   EventDisposition `json:"disposition"`
	ShouldPublish bool             `json:"should_publish"`
	Deduped       bool             `json:"deduped"`
}

// ConflictPublishedOutcome is the result of one conflict publish record.
// The record marks the conflict SHA, so it is idempotent: a repeated record
// reports Deduped.
type ConflictPublishedOutcome struct {
	Disposition EventDisposition `json:"disposition"`
	Recorded    bool             `json:"recorded"`
	Deduped     bool             `json:"deduped"`
}

// CIRecoveryDecision is the CI-recovery verdict HandleCIRecovered returns:
// whether the PR is CI-failed for the event head SHA from the Restate-owned
// failure slots.
// The resolve step is a pure read and records nothing.
type CIRecoveryDecision struct {
	Disposition EventDisposition `json:"disposition"`
	CIFailed    bool             `json:"cifailed"`
}

// ResolveConflictDispatchDecision computes the conflict-republish verdict
// without recording anything. Same-epoch terminal states absorb with no
// publish. An already-published head SHA dedupes; every other applied event
// must publish, so an unrecorded publish is always retried instead of lost.
// A head SHA the state has never seen still publishes: conflict detection
// carries the newest observed head, never a stale repeat.
func ResolveConflictDispatchDecision(s *PRState, ev PREvent) (ConflictDispatchDecision, error) {
	if s == nil {
		return ConflictDispatchDecision{Disposition: DispositionDropStale}, errNilPRState
	}
	if s.Terminal.Absorbing() && ev.ReopenEpoch <= s.ReopenEpoch {
		return ConflictDispatchDecision{Disposition: DispositionAbsorbTerminal}, nil
	}
	out := ConflictDispatchDecision{Disposition: DispositionApply}
	if s.WasConflictPublished(ev.HeadSHA) {
		out.Deduped = true
		return out, nil
	}
	out.ShouldPublish = true
	return out, nil
}

// RecordConflictPublished marks one confirmed conflict publish for the
// event head SHA. Same-epoch terminal states absorb with nothing recorded.
// The record is idempotent: repeating it for the recorded SHA reports
// Deduped, and a newer SHA overwrites the slot.
func RecordConflictPublished(s *PRState, ev PREvent) (ConflictPublishedOutcome, error) {
	if s == nil {
		return ConflictPublishedOutcome{Disposition: DispositionDropStale}, errNilPRState
	}
	if s.Terminal.Absorbing() && ev.ReopenEpoch <= s.ReopenEpoch {
		return ConflictPublishedOutcome{Disposition: DispositionAbsorbTerminal}, nil
	}
	out := ConflictPublishedOutcome{Disposition: DispositionApply}
	out.Deduped = s.WasConflictPublished(ev.HeadSHA)
	s.MarkConflictPublished(ev.HeadSHA)
	out.Recorded = true
	return out, nil
}

// ResolveCIRecovery computes whether the PR is CI-failed for the event head
// SHA from the Restate-owned failure slots. Same-epoch terminal states
// absorb with no recovery, matching the conflict and deploy verdicts: a
// merged or closed PR never reopens CI recovery. It records nothing.
func ResolveCIRecovery(s *PRState, ev PREvent) (CIRecoveryDecision, error) {
	if s == nil {
		return CIRecoveryDecision{Disposition: DispositionDropStale}, errNilPRState
	}
	if s.Terminal.Absorbing() && ev.ReopenEpoch <= s.ReopenEpoch {
		return CIRecoveryDecision{Disposition: DispositionAbsorbTerminal}, nil
	}
	return CIRecoveryDecision{Disposition: DispositionApply, CIFailed: s.IsCIFailedForSHA(ev.HeadSHA)}, nil
}
