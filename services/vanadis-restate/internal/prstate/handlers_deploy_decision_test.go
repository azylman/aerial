package prstate

// Deploy-dispatch decision tests: the Restate PRObject decides whether
// synapse publishes deploy_success, and the publish-recorded ack marks the
// dispatch complete. The decision never records state, so a publish failure
// leaves the next invocation returning should_publish again (at-least-once);
// only the ack records the generation-scoped dispatch key.

import (
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

func decisionState(pr int, head string, gen uint64, terminal TerminalState, recordDispatch, newGeneration bool, evHead, evJob string) *PRState {
	s := NewPRState("azylman/aerial", pr)
	s.HeadSHA = head
	s.GateGeneration = gen
	if terminal != TerminalNone {
		s.MarkTerminal(terminal)
	}
	if recordDispatch {
		s.MarkEffectDispatched(s.EffectKeyFor(prwire.EffectDeployDispatch, evHead, evJob, ""))
	}
	if newGeneration {
		s.RecordKustomizationRequests()
	}
	return s
}

func TestResolveDeployDispatchDecision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		nilState          bool
		stateHead         string
		stateGen          uint64
		terminal          TerminalState
		recordDispatch    bool
		newGeneration     bool
		dispatchJob       string
		evHead            string
		evMerge           string
		evJob             string
		wantDisposition   EventDisposition
		wantShouldPublish bool
		wantDeduped       bool
		wantErr           bool
		wantKeyRecorded   bool
	}{
		{name: "fresh dispatch publishes once", stateHead: "aaa111", stateGen: 2, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: false},
		{name: "redelivery for the same generation head and job dedupes", stateHead: "aaa111", stateGen: 2, recordDispatch: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantDeduped: true, wantKeyRecorded: true},
		{name: "new generation publishes again", stateHead: "aaa111", stateGen: 2, recordDispatch: true, newGeneration: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: false},
		{name: "another job in the same generation publishes", stateHead: "aaa111", stateGen: 2, recordDispatch: true, dispatchJob: "flux-b", evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: false},
		{name: "commit-only dispatch publishes", stateGen: 2, evMerge: "mmm999", evJob: "brain", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: false},
		{name: "stale head drops without publish", stateHead: "aaa111", stateGen: 2, evHead: "zzz999", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionDropStale},
		{name: "merged terminal publishes post-merge deploy", stateHead: "aaa111", stateGen: 2, terminal: TerminalMerged, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: false},
		{name: "closed terminal absorbs deploy", stateHead: "aaa111", stateGen: 2, terminal: TerminalClosed, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionAbsorbTerminal},
		{name: "nil state drops with error", nilState: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				dispatchJob := tc.evJob
				if tc.dispatchJob != "" {
					dispatchJob = tc.dispatchJob
				}
				s = decisionState(7, tc.stateHead, tc.stateGen, tc.terminal, tc.recordDispatch, tc.newGeneration, tc.evHead, dispatchJob)
			}
			ev := PREvent{Event: "deploy_success", Repo: "azylman/aerial", PRNumber: 7, HeadSHA: tc.evHead, MergeSHA: tc.evMerge, JobName: tc.evJob, GateGeneration: tc.stateGen}
			got, err := ResolveDeployDispatchDecision(s, ev)
			if tc.wantErr && err == nil {
				t.Fatal("ResolveDeployDispatchDecision err = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ResolveDeployDispatchDecision err = %v, want nil", err)
			}
			if got.Disposition != tc.wantDisposition {
				t.Fatalf("Disposition = %q, want %q", got.Disposition, tc.wantDisposition)
			}
			if got.ShouldPublish != tc.wantShouldPublish || got.Deduped != tc.wantDeduped {
				t.Fatalf("ShouldPublish/Deduped = %v/%v, want %v/%v", got.ShouldPublish, got.Deduped, tc.wantShouldPublish, tc.wantDeduped)
			}
			if !tc.nilState && s.WasEffectDispatched(s.EffectKeyFor(prwire.EffectDeployDispatch, tc.evHead, tc.evJob, "")) != tc.wantKeyRecorded {
				t.Fatalf("dispatch key recorded = %v, want %v", !tc.wantKeyRecorded, tc.wantKeyRecorded)
			}
		})
	}
}

func TestRecordDeployPublished(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		nilState        bool
		stateHead       string
		stateGen        uint64
		terminal        TerminalState
		recordDispatch  bool
		evHead          string
		evMerge         string
		evJob           string
		wantDisposition EventDisposition
		wantRecorded    bool
		wantDeduped     bool
		wantErr         bool
		wantHead        string
	}{
		{name: "fresh publish records the dispatch key", stateHead: "aaa111", stateGen: 2, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantRecorded: true, wantHead: "aaa111"},
		{name: "recorded publish acks idempotent", stateHead: "aaa111", stateGen: 2, recordDispatch: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantRecorded: true, wantDeduped: true, wantHead: "aaa111"},
		{name: "empty job records the dispatch key", stateHead: "aaa111", stateGen: 2, evHead: "aaa111", evMerge: "mmm999", wantDisposition: DispositionApply, wantRecorded: true, wantHead: "aaa111"},
		{name: "stale head drops without recording", stateHead: "aaa111", stateGen: 2, evHead: "zzz999", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionDropStale, wantHead: "aaa111"},
		{name: "merged terminal records post-merge publish", stateHead: "aaa111", stateGen: 2, terminal: TerminalMerged, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantRecorded: true, wantHead: "aaa111"},
		{name: "closed terminal absorbs without recording", stateHead: "aaa111", stateGen: 2, terminal: TerminalClosed, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionAbsorbTerminal, wantHead: "aaa111"},
		{name: "nil state drops with error", nilState: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = decisionState(7, tc.stateHead, tc.stateGen, tc.terminal, tc.recordDispatch, false, tc.evHead, tc.evJob)
			}
			ev := PREvent{Event: "deploy_success", Repo: "azylman/aerial", PRNumber: 7, HeadSHA: tc.evHead, MergeSHA: tc.evMerge, JobName: tc.evJob, GateGeneration: tc.stateGen}
			got, err := RecordDeployPublished(s, ev)
			if tc.wantErr {
				if err == nil {
					t.Fatal("RecordDeployPublished err = nil, want error")
				}
			} else {
				if err != nil {
					t.Fatalf("RecordDeployPublished err = %v, want nil", err)
				}
				assertDeployPublished(t, got, tc.wantDisposition, tc.wantRecorded, tc.wantDeduped)
				if !tc.nilState {
					assertDeployPublishedState(t, s, tc.evHead, tc.evJob, tc.wantRecorded, tc.wantHead)
				}
			}
		})
	}
}

// assertDeployPublished checks the publish-recorded outcome fields.
func assertDeployPublished(t *testing.T, got DeployPublishedOutcome, wantDisposition EventDisposition, wantRecorded, wantDeduped bool) {
	t.Helper()
	if got.Disposition != wantDisposition {
		t.Fatalf("Disposition = %q, want %q", got.Disposition, wantDisposition)
	}
	if got.Recorded != wantRecorded || got.Deduped != wantDeduped {
		t.Fatalf("Recorded/Deduped = %v/%v, want %v/%v", got.Recorded, got.Deduped, wantRecorded, wantDeduped)
	}
}

// assertDeployPublishedState checks the persisted dispatch key and the
// synced head SHA after a publish-recorded ack.
func assertDeployPublishedState(t *testing.T, s *PRState, evHead, evJob string, wantKeyRecorded bool, wantHead string) {
	t.Helper()
	key := s.EffectKeyFor(prwire.EffectDeployDispatch, evHead, evJob, "")
	if s.WasEffectDispatched(key) != wantKeyRecorded {
		t.Fatalf("dispatch key recorded = %v, want %v", !wantKeyRecorded, wantKeyRecorded)
	}
	if s.HeadSHA != wantHead {
		t.Fatalf("HeadSHA = %q, want %q", s.HeadSHA, wantHead)
	}
}
