package prstate

// Deploy-dispatch decision tests: the Restate PRObject decides whether
// synapse publishes deploy_success, and the publish-recorded ack marks the
// dispatch complete. The decision never records state, so a publish failure
// leaves the next invocation returning should_publish again (at-least-once);
// only the ack records the dispatch key and both dedup slots.

import (
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
)

func decisionState(pr int, head string, gen uint64, terminal TerminalState, recordDispatch, recordPRSlot, recordSHASlot bool, evHead, evJob string) *PRState {
	s := NewPRState("aerial", pr)
	s.HeadSHA = head
	s.GateGeneration = gen
	if terminal != TerminalNone {
		s.MarkTerminal(terminal)
	}
	if recordDispatch {
		s.MarkEffectDispatched(s.EffectKeyFor(EffectDeployDispatch, evHead, evJob, ""))
	}
	if recordPRSlot {
		s.RecordDeployedPR()
	}
	if recordSHASlot {
		s.RecordDeployedSHAJob(evJob, evHead)
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
		recordPRSlot      bool
		recordSHASlot     bool
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
		{name: "recorded publish dedupes redelivery", stateHead: "aaa111", stateGen: 2, recordDispatch: true, recordPRSlot: true, recordSHASlot: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantDeduped: true, wantKeyRecorded: true},
		{name: "slots without dispatch key republish at-least-once", stateHead: "aaa111", stateGen: 2, recordPRSlot: true, recordSHASlot: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: false},
		{name: "dispatch key without slots republishes at-least-once", stateHead: "aaa111", stateGen: 2, recordDispatch: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantShouldPublish: true, wantKeyRecorded: true},
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
				s = decisionState(7, tc.stateHead, tc.stateGen, tc.terminal, tc.recordDispatch, tc.recordPRSlot, tc.recordSHASlot, tc.evHead, tc.evJob)
			}
			ev := PREvent{Event: "deploy_success", Repo: "aerial", PRNumber: 7, HeadSHA: tc.evHead, MergeSHA: tc.evMerge, JobName: tc.evJob, GateGeneration: tc.stateGen}
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
			if !tc.nilState && s.WasEffectDispatched(s.EffectKeyFor(EffectDeployDispatch, tc.evHead, tc.evJob, "")) != tc.wantKeyRecorded {
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
		recordPRSlot    bool
		recordSHASlot   bool
		evHead          string
		evMerge         string
		evJob           string
		wantDisposition EventDisposition
		wantRecorded    bool
		wantDeduped     bool
		wantErr         bool
		wantPRSlot      bool
		wantSHASlot     bool
		wantHead        string
	}{
		{name: "fresh publish records dispatch and both slots", stateHead: "aaa111", stateGen: 2, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantRecorded: true, wantPRSlot: true, wantSHASlot: true, wantHead: "aaa111"},
		{name: "recorded publish acks idempotent", stateHead: "aaa111", stateGen: 2, recordDispatch: true, recordPRSlot: true, recordSHASlot: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantRecorded: true, wantDeduped: true, wantPRSlot: true, wantSHASlot: true, wantHead: "aaa111"},
		{name: "empty job records dispatch without sha slot", stateHead: "aaa111", stateGen: 2, evHead: "aaa111", evMerge: "mmm999", wantDisposition: DispositionApply, wantRecorded: true, wantPRSlot: true, wantHead: "aaa111"},
		{name: "stale head drops without recording", stateHead: "aaa111", stateGen: 2, evHead: "zzz999", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionDropStale, wantHead: "aaa111"},
		{name: "merged terminal records post-merge publish", stateHead: "aaa111", stateGen: 2, terminal: TerminalMerged, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionApply, wantRecorded: true, wantPRSlot: true, wantSHASlot: true, wantHead: "aaa111"},
		{name: "closed terminal absorbs without recording", stateHead: "aaa111", stateGen: 2, terminal: TerminalClosed, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionAbsorbTerminal, wantHead: "aaa111"},
		{name: "nil state drops with error", nilState: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = decisionState(7, tc.stateHead, tc.stateGen, tc.terminal, tc.recordDispatch, tc.recordPRSlot, tc.recordSHASlot, tc.evHead, tc.evJob)
			}
			ev := PREvent{Event: "deploy_success", Repo: "aerial", PRNumber: 7, HeadSHA: tc.evHead, MergeSHA: tc.evMerge, JobName: tc.evJob, GateGeneration: tc.stateGen}
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
					assertDeployPublishedState(t, s, tc.evHead, tc.evJob, tc.wantRecorded, tc.wantPRSlot, tc.wantSHASlot, tc.wantHead)
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

// assertDeployPublishedState checks the persisted dispatch key, both dedup
// slots, and the synced head SHA after a publish-recorded ack.
func assertDeployPublishedState(t *testing.T, s *PRState, evHead, evJob string, wantKeyRecorded, wantPRSlot, wantSHASlot bool, wantHead string) {
	t.Helper()
	key := s.EffectKeyFor(EffectDeployDispatch, evHead, evJob, "")
	if s.WasEffectDispatched(key) != wantKeyRecorded {
		t.Fatalf("dispatch key recorded = %v, want %v", !wantKeyRecorded, wantKeyRecorded)
	}
	if s.WasDeployedPR() != wantPRSlot {
		t.Fatalf("WasDeployedPR = %v, want %v", s.WasDeployedPR(), wantPRSlot)
	}
	if s.WasDeployedSHAJob(evJob, evHead) != wantSHASlot {
		t.Fatalf("WasDeployedSHAJob = %v, want %v", s.WasDeployedSHAJob(evJob, evHead), wantSHASlot)
	}
	if s.HeadSHA != wantHead {
		t.Fatalf("HeadSHA = %q, want %q", s.HeadSHA, wantHead)
	}
}

// deployServiceCase is one shared Restate-surface row for the deploy verdict
// handlers: the decision handler never persists, the ack handler persists
// exactly once per applied publish, and K/V failures surface.
type deployServiceCase struct {
	name           string
	ack            bool
	failGet        bool
	preset         bool
	stateHead      string
	stateGen       uint64
	terminal       TerminalState
	recordDispatch bool
	recordPRSlot   bool
	recordSHASlot  bool
	evHead         string
	evMerge        string
	evJob          string
	wantPublish    bool
	wantDeduped    bool
	wantRecorded   bool
	wantSet        bool
	wantErr        bool
}

// deployServiceMock builds the shared K/V mock for one deploy verdict call:
// a failing Get for error rows, else the preset state (nil for first touch).
func deployServiceMock(t *testing.T, tc deployServiceCase) (*mocks.MockContext, *stateCapture[*PRState], PREvent) {
	t.Helper()
	rec := &stateCapture[*PRState]{}
	ev := PREvent{Event: "deploy_success", Repo: "aerial", PRNumber: 101, HeadSHA: tc.evHead, MergeSHA: tc.evMerge, JobName: tc.evJob, GateGeneration: tc.stateGen}
	if tc.failGet {
		return newStateErrorMock(t), rec, ev
	}
	var existing *PRState
	if tc.preset {
		existing = decisionState(101, tc.stateHead, tc.stateGen, tc.terminal, tc.recordDispatch, tc.recordPRSlot, tc.recordSHASlot, tc.evHead, tc.evJob)
	}
	mc, rec := newStateMock(t, existing)
	return mc, rec, ev
}

// deployVerdict normalizes both handler outcomes so one table covers the
// decision and ack surface with a single comparison.
type deployVerdict struct {
	publish  bool
	deduped  bool
	recorded bool
}

// invokeDeployVerdict calls the decision or ack handler for one row and
// normalizes the outcome. The decision never persists; the ack persists on
// apply inside the handler.
func invokeDeployVerdict(mc *mocks.MockContext, ev PREvent, ack bool) (deployVerdict, error) {
	if ack {
		out, err := NewPRObjectService().HandleDeploySuccessPublished(restate.WithMockContext(mc), ev)
		return deployVerdict{deduped: out.Deduped, recorded: out.Recorded}, err
	}
	out, err := NewPRObjectService().HandleDeployDispatched(restate.WithMockContext(mc), ev)
	return deployVerdict{publish: out.ShouldPublish, deduped: out.Deduped}, err
}

// TestPRObjectService_DeployDecision pins the Restate surface for the two
// deploy verdict handlers through the shared K/V mock harness: the decision
// handler never persists, the ack handler persists exactly once per applied
// publish, and K/V failures surface without persisting.
func TestPRObjectService_DeployDecision(t *testing.T) {
	t.Parallel()
	tests := []deployServiceCase{
		{name: "decision resolves publish on fresh state", evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantPublish: true},
		{name: "decision dedupes recorded publish without persisting", preset: true, stateHead: "aaa111", stateGen: 2, recordDispatch: true, recordPRSlot: true, recordSHASlot: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantDeduped: true},
		{name: "decision surfaces kv failure", failGet: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantErr: true},
		{name: "ack records fresh publish", ack: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantRecorded: true, wantSet: true},
		{name: "ack replays recorded publish", ack: true, preset: true, stateHead: "aaa111", stateGen: 2, recordDispatch: true, recordPRSlot: true, recordSHASlot: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantRecorded: true, wantDeduped: true, wantSet: true},
		{name: "ack drops stale head without persisting", ack: true, preset: true, stateHead: "aaa111", stateGen: 2, evHead: "zzz999", evMerge: "mmm999", evJob: "flux-a"},
		{name: "ack surfaces kv failure", ack: true, failGet: true, evHead: "aaa111", evMerge: "mmm999", evJob: "flux-a", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec, ev := deployServiceMock(t, tc)
			got, err := invokeDeployVerdict(mc, ev, tc.ack)
			want := deployVerdict{publish: tc.wantPublish, deduped: tc.wantDeduped, recorded: tc.wantRecorded}
			if tc.wantErr {
				if err == nil {
					t.Fatal("deploy verdict err = nil, want error")
				}
			} else {
				if err != nil {
					t.Fatalf("deploy verdict err = %v, want nil", err)
				}
				if got != want {
					t.Fatalf("verdict = %+v, want %+v", got, want)
				}
			}
			if rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", rec.setCalled, tc.wantSet)
			}
		})
	}
}
