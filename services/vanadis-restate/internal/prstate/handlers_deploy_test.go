package prstate

// Rollout/deploy-dedup handler tests: journaled generation-scoped
// effects, BOTH WasDispatched keys, stale/terminal guards, readiness
// retry without minting generations. Fakes stand in for the Restate,
// GitHub, and Discord boundaries.

import (
	"context"
	"errors"
	"testing"
)

// fakeFluxForwarder is the hermetic Flux-forward boundary fake.
type fakeFluxForwarder struct {
	calls []EffectKey
	err   error
}

func (f *fakeFluxForwarder) ForwardFlux(_ context.Context, key EffectKey, _ PREvent) error {
	f.calls = append(f.calls, key)
	return f.err
}

// fakeImagePromoter is the hermetic image-promotion boundary fake.
type fakeImagePromoter struct {
	calls []EffectKey
	err   error
}

func (f *fakeImagePromoter) PromoteImages(_ context.Context, key EffectKey, _ PREvent) error {
	f.calls = append(f.calls, key)
	return f.err
}

func deployState(repo string, pr int, head string, gen uint64) *PRState {
	s := NewPRState(repo, pr)
	s.HeadSHA = head
	s.GateGeneration = gen
	return s
}

func seedFresh() *PRState { return deployState("aerial", 7, "aaa111", 2) }

func seedMerged() *PRState {
	s := seedFresh()
	s.MarkTerminal(TerminalMerged)
	return s
}

func seedMergedAt(mergeSHA string) *PRState {
	s := seedMerged()
	s.MergeSHA = mergeSHA
	return s
}

func seedClosed() *PRState {
	s := seedFresh()
	s.MarkTerminal(TerminalClosed)
	return s
}

func seedMergedKnown() *PRState { return seedMergedAt("mmm999") }

func seedKeysOnly() *PRState {
	s := seedFresh()
	s.MarkEffectDispatched(s.EffectKeyFor(EffectFluxForward, "aaa111", "flux-a", ""))
	s.MarkEffectDispatched(s.EffectKeyFor(EffectImagePromote, "mmm999", "", ""))
	return s
}

func seedDedupedNoMerge() *PRState {
	s := seedFresh()
	s.MarkEffectDispatched(s.EffectKeyFor(EffectFluxForward, "aaa111", "flux-b", ""))
	s.RecordDeployedPR()
	return s
}

func seedEmptyMergeDispatched() *PRState {
	s := seedFresh()
	s.MarkEffectDispatched(s.EffectKeyFor(EffectFluxForward, "aaa111", "flux-a", ""))
	s.RecordDeployedPR()
	s.RecordDeployedSHAJob("flux-a", "aaa111")
	return s
}

func seedFluxOnly() *PRState {
	s := seedFresh()
	s.MarkEffectDispatched(s.EffectKeyFor(EffectFluxForward, "aaa111", "flux-a", ""))
	return s
}

func seedPRSlot() *PRState {
	s := seedFresh()
	s.RecordDeployedPR()
	return s
}

func deployEv(head, merge, job string, gen uint64) PREvent {
	return PREvent{Event: "deploy_success", Repo: "aerial", PRNumber: 7, HeadSHA: head, MergeSHA: merge, JobName: job, GateGeneration: gen}
}

func TestHandleDeployEvent(t *testing.T) {
	t.Parallel()
	errBoom := errors.New("boom")
	full := deployEv("aaa111", "mmm999", "flux-a", 2)
	tests := []struct {
		name string
		seed func() *PRState
		ev   PREvent
		// fwdErr and promErr fail the matching boundary runner.
		fwdErr           error
		promErr          error
		wantDisposition  EventDisposition
		wantDispatched   bool
		wantDeduped      bool
		wantErr          bool
		wantFwdCalls     int
		wantPromCalls    int
		wantPRDispatched bool
	}{
		{name: "success dispatches both slots and both effects", seed: seedFresh, ev: full, wantDisposition: DispositionApply, wantDispatched: true, wantFwdCalls: 1, wantPromCalls: 1, wantPRDispatched: true},
		{name: "recorded keys replay without re-firing", seed: seedKeysOnly, ev: full, wantDisposition: DispositionApply, wantDispatched: true, wantPRDispatched: true},
		{name: "partial failure replays flux and retries promotion", seed: seedFluxOnly, ev: full, wantDisposition: DispositionApply, wantDispatched: true, wantPromCalls: 1, wantPRDispatched: true},
		{name: "merge arriving after empty-merge dispatch retries promotion", seed: seedEmptyMergeDispatched, ev: full, wantDisposition: DispositionApply, wantDispatched: true, wantPromCalls: 1, wantPRDispatched: true},
		{name: "pr slot with recorded flux fully dedupes no-merge redelivery", seed: seedDedupedNoMerge, ev: deployEv("aaa111", "", "flux-b", 2), wantDisposition: DispositionApply, wantDispatched: true, wantDeduped: true, wantPRDispatched: true},
		{name: "stale head drops without effects", seed: seedFresh, ev: deployEv("zzz999", "mmm999", "flux-a", 2), wantDisposition: DispositionDropStale},
		{name: "merged terminal applies post-merge deploy", seed: seedMerged, ev: full, wantDisposition: DispositionApply, wantDispatched: true, wantFwdCalls: 1, wantPromCalls: 1, wantPRDispatched: true},
		{name: "merged terminal applies deploy of its merge sha", seed: seedMergedKnown, ev: deployEv("mmm999", "mmm999", "flux-a", 2), wantDisposition: DispositionApply, wantDispatched: true, wantFwdCalls: 1, wantPromCalls: 1, wantPRDispatched: true},
		{name: "merged terminal drops foreign sha deploy", seed: seedMergedKnown, ev: deployEv("zzz999", "zzz999", "flux-a", 2), wantDisposition: DispositionDropStale},
		{name: "closed terminal absorbs deploy", seed: seedClosed, ev: full, wantDisposition: DispositionAbsorbTerminal},
		{name: "reopen epoch applies after terminal", seed: seedMerged, ev: PREvent{Event: "deploy_success", Repo: "aerial", PRNumber: 7, HeadSHA: "aaa111", MergeSHA: "mmm999", JobName: "flux-a", GateGeneration: 2, ReopenEpoch: 1}, wantDisposition: DispositionApply, wantDispatched: true, wantFwdCalls: 1, wantPromCalls: 1, wantPRDispatched: true},
		{name: "flux error propagates without dispatch", seed: seedFresh, ev: full, fwdErr: errBoom, wantDisposition: DispositionApply, wantErr: true, wantFwdCalls: 1},
		{name: "promotion error propagates after flux recorded", seed: seedFresh, ev: full, promErr: errBoom, wantDisposition: DispositionApply, wantErr: true, wantFwdCalls: 1, wantPromCalls: 1},
		{name: "empty merge skips promotion but dispatches", seed: seedFresh, ev: deployEv("aaa111", "", "flux-a", 2), wantDisposition: DispositionApply, wantDispatched: true, wantFwdCalls: 1, wantPRDispatched: true},
		{name: "nil state drops with error", seed: func() *PRState { return nil }, ev: full, wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := tc.seed()
			fwd := &fakeFluxForwarder{err: tc.fwdErr}
			prom := &fakeImagePromoter{err: tc.promErr}
			got, err := HandleDeployEvent(context.Background(), s, tc.ev, fwd, prom)
			if tc.wantErr && err == nil {
				t.Fatal("HandleDeployEvent err = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleDeployEvent err = %v, want nil", err)
			}
			if got.Disposition != tc.wantDisposition {
				t.Fatalf("Disposition = %q, want %q", got.Disposition, tc.wantDisposition)
			}
			if got.Dispatched != tc.wantDispatched || got.Deduped != tc.wantDeduped {
				t.Fatalf("Dispatched/Deduped = %v/%v, want %v/%v", got.Dispatched, got.Deduped, tc.wantDispatched, tc.wantDeduped)
			}
			if len(fwd.calls) != tc.wantFwdCalls || len(prom.calls) != tc.wantPromCalls {
				t.Fatalf("flux/promote calls = %d/%d, want %d/%d", len(fwd.calls), len(prom.calls), tc.wantFwdCalls, tc.wantPromCalls)
			}
			if s != nil && s.WasDeployedPR() != tc.wantPRDispatched {
				t.Fatalf("WasDeployedPR = %v, want %v", s.WasDeployedPR(), tc.wantPRDispatched)
			}
		})
	}
}

func TestHandleRolloutEvent(t *testing.T) {
	t.Parallel()
	errBoom := errors.New("boom")
	full := PREvent{Event: "rollout_progress", Repo: "aerial", PRNumber: 3, HeadSHA: "ccc333", JobName: "kustom-a", GateGeneration: 1}
	tests := []struct {
		name            string
		seed            func() *PRState
		ev              PREvent
		fwdErr          error
		wantDisposition EventDisposition
		wantForwarded   bool
		wantDeduped     bool
		wantErr         bool
		wantCalls       int
	}{
		{name: "progress forwards flux effect once", seed: func() *PRState { return deployState("aerial", 3, "ccc333", 1) }, ev: full, wantDisposition: DispositionApply, wantForwarded: true, wantCalls: 1},
		{name: "recorded key replays without re-firing", seed: func() *PRState {
			s := deployState("aerial", 3, "ccc333", 1)
			s.MarkEffectDispatched(s.EffectKeyFor(EffectFluxForward, "ccc333", "kustom-a", ""))
			return s
		}, ev: full, wantDisposition: DispositionApply, wantForwarded: true, wantDeduped: true},
		{name: "stale rollout drops without forwarding", seed: func() *PRState { return deployState("aerial", 3, "ccc333", 1) }, ev: PREvent{Event: "rollout_progress", Repo: "aerial", PRNumber: 3, HeadSHA: "ddd444", JobName: "kustom-a", GateGeneration: 1}, wantDisposition: DispositionDropStale},
		{name: "closed terminal absorbs rollout", seed: func() *PRState {
			s := deployState("aerial", 3, "ccc333", 1)
			s.MarkTerminal(TerminalClosed)
			return s
		}, ev: full, wantDisposition: DispositionAbsorbTerminal},
		{name: "forward error propagates", seed: func() *PRState { return deployState("aerial", 3, "ccc333", 1) }, ev: full, fwdErr: errBoom, wantDisposition: DispositionApply, wantErr: true, wantCalls: 1},
		{name: "nil state drops with error", seed: func() *PRState { return nil }, ev: full, wantDisposition: DispositionDropStale, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fwd := &fakeFluxForwarder{err: tc.fwdErr}
			got, err := HandleRolloutEvent(context.Background(), tc.seed(), tc.ev, fwd)
			if len(fwd.calls) != tc.wantCalls {
				t.Fatalf("flux calls = %d, want %d", len(fwd.calls), tc.wantCalls)
			}
			if tc.wantErr && err == nil {
				t.Fatal("HandleRolloutEvent err = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleRolloutEvent err = %v, want nil", err)
			}
			if got.Disposition != tc.wantDisposition {
				t.Fatalf("Disposition = %q, want %q", got.Disposition, tc.wantDisposition)
			}
			if got.Forwarded != tc.wantForwarded || got.Deduped != tc.wantDeduped {
				t.Fatalf("Forwarded/Deduped = %v/%v, want %v/%v", got.Forwarded, got.Deduped, tc.wantForwarded, tc.wantDeduped)
			}
		})
	}
}

func TestWasDispatchedDeployKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		seed     func() *PRState
		repo     string
		job      string
		sha      string
		prNumber int
		want     bool
	}{
		{name: "empty state never dispatched", seed: seedFresh, repo: "aerial", job: "flux-a", sha: "aaa111", prNumber: 7, want: false},
		{name: "pr slot alone reports dispatched", seed: seedPRSlot, repo: "aerial", job: "other-job", sha: "other-sha", prNumber: 7, want: true},
		{name: "job plus sha alone reports dispatched", seed: func() *PRState {
			s := seedFresh()
			s.RecordDeployedSHAJob("flux-a", "aaa111")
			return s
		}, repo: "aerial", job: "flux-a", sha: "aaa111", prNumber: 99, want: true},
		{name: "different job not dispatched", seed: func() *PRState {
			s := seedFresh()
			s.RecordDeployedSHAJob("flux-a", "aaa111")
			return s
		}, repo: "aerial", job: "flux-b", sha: "aaa111", prNumber: 99, want: false},
		{name: "empty job never dispatched", seed: seedFresh, repo: "aerial", job: "", sha: "aaa111", prNumber: 99, want: false},
		{name: "zero sha never dispatched", seed: seedFresh, repo: "aerial", job: "flux-a", sha: "0000", prNumber: 99, want: false},
		{name: "nil dedup map never dispatched", seed: func() *PRState { return &PRState{Repo: "azylman/aerial", PRNumber: 7} }, repo: "aerial", job: "flux-a", sha: "aaa111", prNumber: 99, want: false},
		{name: "nil state not dispatched", seed: func() *PRState { return nil }, repo: "aerial", job: "flux-a", sha: "aaa111", prNumber: 7, want: false},
		{name: "unscoped repo matches owner form", seed: func() *PRState {
			s := deployState("azylman/aerial", 7, "aaa111", 2)
			s.RecordDeployedSHAJob("flux-a", "aaa111")
			return s
		}, repo: "aerial", job: "flux-a", sha: "aaa111", prNumber: 99, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WasDispatched(tc.seed(), tc.repo, tc.job, tc.sha, tc.prNumber); got != tc.want {
				t.Fatalf("WasDispatched = %v, want %v", got, tc.want)
			}
		})
	}
}
