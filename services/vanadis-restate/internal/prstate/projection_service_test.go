package prstate

import (
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// projRunContext is the hermetic restate.RunContext for executing Run
// closures in mocks.
type projRunContext struct {
	context.Context
}

func (projRunContext) Log() *slog.Logger         { return slog.Default() }
func (projRunContext) Request() *restate.Request { return nil }

// runCapture records whether a journaled restate.Run executed.
type runCapture struct {
	ran bool
}

// stubRun executes every restate.Run closure once. A closure error stands
// in for retries exhausted and returns as a terminal error.
func stubRun(mc *mocks.MockContext) *runCapture {
	rec := &runCapture{}
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		rec.ran = true
		res, err := fn(projRunContext{Context: context.Background()})
		if err != nil {
			return restate.TerminalErrorf("%v", err)
		}
		if out := reflect.ValueOf(output); res != nil && out.Kind() == reflect.Pointer && !out.IsNil() {
			out.Elem().Set(reflect.ValueOf(res))
		}
		return nil
	}).Maybe()
	return rec
}

// fakeProjectionReader is the hermetic ProjectionReader.
type fakeProjectionReader struct {
	row   RegistryRow
	found bool
	err   error
}

func (f fakeProjectionReader) LoadRegistryRow(context.Context, string, int) (RegistryRow, bool, error) {
	return f.row, f.found, f.err
}

// projSvc builds a PRObject service with the requested projection deps.
func projSvc(store ProjectionStore, reader ProjectionReader) *PRObjectService {
	return NewPRObjectService().WithProjection(store, reader)
}

// openPR seeds an open PR on head h1.
func openPR() *PRState {
	return svcRow("h1", "", RegistryStatusOpen, TerminalNone)
}

// TestPRObjectServiceAfterCommit pins effect-owner routing after a commit:
// restate events self-send Project when a store is wired, legacy events
// send a delayed CompareProjection carrying the commit snapshot when a
// reader is wired, and unwired services send nothing.
func TestPRObjectServiceAfterCommit(t *testing.T) {
	t.Parallel()
	store := &FakeProjectionStore{}
	reader := fakeProjectionReader{}
	pendingSnap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Agent: "aerial", HeadSHA: "h1", Status: "open", Metadata: ProjectionMetadata{CIChecks: map[string]string{"ci": "pending"}}}
	tests := []struct {
		name      string
		svc       *PRObjectService
		owner     EffectsOwner
		wantSends []sentCall
	}{
		{name: "restate owner projects", svc: projSvc(store, reader), owner: EffectsOwnerRestate, wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial#101", method: "Project", input: ProjectRequest{}}}},
		{name: "legacy owner shadow compares", svc: projSvc(store, reader), owner: EffectsOwnerLegacy, wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial#101", method: "CompareProjection", input: pendingSnap, delayed: true}}},
		{name: "unknown stamp is legacy", svc: projSvc(store, reader), owner: "bogus", wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial#101", method: "CompareProjection", input: pendingSnap, delayed: true}}},
		{name: "restate owner without store sends nothing", svc: projSvc(nil, reader), owner: EffectsOwnerRestate, wantSends: nil},
		{name: "legacy owner without reader sends nothing", svc: projSvc(store, nil), owner: EffectsOwnerLegacy, wantSends: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, openPR())
			err := tc.svc.HandleCheckStatus(restate.WithMockContext(mc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci", CheckState: "pending", EffectsOwner: tc.owner})
			require.NoError(t, err)
			require.True(t, rec.setCalled)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestAfterCommitSkipsInvalidState pins that an unkeyed state never sends.
func TestAfterCommitSkipsInvalidState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		state *PRState
	}{
		{name: "nil state", state: nil},
		{name: "zero pr number", state: &PRState{Repo: "azylman/aerial"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			sends := captureSends(t, mc)
			projSvc(&FakeProjectionStore{}, fakeProjectionReader{}).afterCommit(restate.WithMockContext(mc), tc.state, EffectsOwnerRestate)
			require.Empty(t, sends.snapshot())
		})
	}
}

// TestPRObjectServiceProject pins the restate-owned writer: it projects the
// committed row once and records the projected key, skips an identical
// snapshot, does nothing without state or store, and turns an exhausted
// write into a delayed self-send instead of failing.
func TestPRObjectServiceProject(t *testing.T) {
	t.Parallel()
	projected := openPR()
	projected.ProjectedKey = ProjectionJournalKey(BuildProjection(openPR()))
	stale := openPR()
	stale.ProjectedKey = "projection:old"
	tests := []struct {
		name          string
		state         *PRState
		store         ProjectionStore
		storeErr      error
		wantRun       bool
		wantSet       bool
		wantKeyPrefix string
		wantRecorded  []ProjectionSnapshot
		wantSends     []sentCall
		failGet       bool
	}{
		{name: "kv failure surfaces", store: &FakeProjectionStore{}, failGet: true},
		{name: "missing state projects nothing", state: nil, store: &FakeProjectionStore{}},
		{name: "no store projects nothing", state: openPR(), store: nil},
		{name: "identical snapshot is skipped", state: projected, store: &FakeProjectionStore{}},
		{
			name: "changed snapshot writes the full row", state: stale, store: &FakeProjectionStore{}, wantRun: true, wantSet: true,
			wantKeyPrefix: "projection:aerial:azylman/aerial:101@h1:gen0:epoch0:",
			wantRecorded:  []ProjectionSnapshot{{Repo: "azylman/aerial", PRNumber: 101, Agent: "aerial", HeadSHA: "h1", Status: "open"}},
		},
		{
			name: "exhausted write retries later", state: openPR(), store: &FakeProjectionStore{Err: errFakeProjection}, wantRun: true,
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial", method: "Project", input: ProjectRequest{}, delayed: true}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			var rec *stateCapture[*PRState]
			if tc.failGet {
				mc, rec = newStateErrorMock(t), &stateCapture[*PRState]{sends: &sendCapture{}}
			} else {
				mc, rec = newStateMock(t, tc.state)
			}
			run := stubRun(mc)
			svc := NewPRObjectService()
			if tc.store != nil {
				svc = svc.WithProjection(tc.store, nil)
			}
			err := svc.Project(restate.WithMockContext(mc), ProjectRequest{})
			require.Equal(t, tc.failGet, err != nil, "err = %v", err)
			require.Equal(t, tc.wantRun, run.ran)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
			if tc.wantSet && !strings.HasPrefix(rec.saved.ProjectedKey, tc.wantKeyPrefix) {
				t.Fatalf("projected key = %q, want prefix %q", rec.saved.ProjectedKey, tc.wantKeyPrefix)
			}
			if fake, ok := tc.store.(*FakeProjectionStore); ok {
				require.Equal(t, tc.wantRecorded, fake.Recorded)
			}
		})
	}
}

// TestPRObjectServiceCompareProjection pins the shared shadow compare: it
// reads the legacy row through a journaled run for every verdict, never
// fails the invocation on a read error, and skips unwired or unkeyed input.
func TestPRObjectServiceCompareProjection(t *testing.T) {
	t.Parallel()
	snap := ProjectionSnapshot{Repo: "azylman/aerial", PRNumber: 101, Agent: "aerial", Branch: "feat", HeadSHA: "h1", Status: "open"}
	tests := []struct {
		name    string
		reader  ProjectionReader
		snap    ProjectionSnapshot
		wantRun bool
	}{
		{name: "matching row", reader: fakeProjectionReader{row: RegistryRow{Branch: "feat", HeadSHA: "h1", Status: "open"}, found: true}, snap: snap, wantRun: true},
		{name: "divergent row", reader: fakeProjectionReader{row: RegistryRow{Branch: "old", HeadSHA: "h1", Status: "open"}, found: true}, snap: snap, wantRun: true},
		{name: "superseded row", reader: fakeProjectionReader{row: RegistryRow{HeadSHA: "h2", Status: "open"}, found: true}, snap: snap, wantRun: true},
		{name: "missing row", reader: fakeProjectionReader{}, snap: snap, wantRun: true},
		{name: "read failure is dropped", reader: fakeProjectionReader{err: errFakeProjection}, snap: snap, wantRun: true},
		{name: "no reader skips", reader: nil, snap: snap, wantRun: false},
		{name: "unkeyed snapshot skips", reader: fakeProjectionReader{}, snap: ProjectionSnapshot{Status: "open"}, wantRun: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			run := stubRun(mc)
			require.NoError(t, projSvc(nil, tc.reader).CompareProjection(restate.WithMockContext(mc), tc.snap))
			require.Equal(t, tc.wantRun, run.ran)
		})
	}
}

// newKeyedStateMock is newStateMock with an explicit object key.
func newKeyedStateMock(t *testing.T, key string, existing *PRState) (*mocks.MockContext, *stateCapture[*PRState]) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	rec := &stateCapture[*PRState]{sends: captureSends(t, mc)}
	mc.On("Key").Return(key).Maybe()
	mc.On("Get", "state", mock.Anything).Run(func(args mock.Arguments) {
		if pp, ok := args.Get(1).(**PRState); ok && pp != nil {
			*pp = existing
		}
	}).Return(true, nil).Maybe()
	mc.On("Set", "state", mock.Anything).Run(func(args mock.Arguments) {
		rec.setCalled = true
		if saved, ok := args.Get(1).(*PRState); ok {
			rec.saved = saved
		}
	}).Maybe()
	return mc, rec
}

// TestPRObjectServiceHydrate pins the Restate surface of Hydrate: key
// validation, first-touch fill with RepoObject index sends (merged, plus
// deploying for a live rollout), idempotent replays, and K/V failures.
func TestPRObjectServiceHydrate(t *testing.T) {
	t.Parallel()
	deployingRow := RegistryRow{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", MergeSHA: "m1", TargetID: snowA, Status: RegistryStatusDeploying, Metadata: ProjectionMetadata{Agent: "suletta"}}
	openRow := RegistryRow{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", Status: RegistryStatusOpen, Metadata: ProjectionMetadata{BotID: "miorine"}}
	hydratedOpen := NewPRState("aerial", 101)
	hydratedOpen.Branch, hydratedOpen.HeadSHA, hydratedOpen.Status, hydratedOpen.Agent, hydratedOpen.BotID = "feat", "h1", RegistryStatusOpen, "miorine", "miorine"
	mergeOnly := svcRow("h1", "", RegistryStatusMerged, TerminalMerged)
	mergeOnly.Branch, mergeOnly.Agent = "feat", "suletta"
	tests := []struct {
		name      string
		key       string
		state     *PRState
		row       RegistryRow
		want      HydrateResponse
		wantErr   string
		wantSet   bool
		wantSends []sentCall
		failGet   bool
	}{
		{name: "kv failure surfaces", key: "azylman/aerial#101", row: openRow, failGet: true, wantErr: "kv unavailable"},
		{name: "row for another object is rejected", key: "azylman/aerial#102", row: openRow, wantErr: "prstate: hydrate row must name this object's repo and pr_number"},
		{name: "row without pr is rejected", key: "azylman/aerial#101", row: RegistryRow{Repo: "aerial"}, wantErr: "prstate: hydrate row must name this object's repo and pr_number"},
		{
			name: "first touch deploying row indexes merged and deploying", key: "azylman/aerial#101", row: deployingRow, wantSet: true,
			want: HydrateResponse{Changed: true, Agent: "suletta", Status: RegistryStatusDeploying},
			wantSends: []sentCall{
				{service: "RepoObject", key: "azylman/aerial", method: "HandlePRIndex", input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", MergeSHA: "m1", Lifecycle: IndexMerged}},
				{service: "RepoObject", key: "azylman/aerial", method: "HandlePRIndex", input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", MergeSHA: "m1", Lifecycle: IndexDeploying}},
			},
		},
		{
			name: "first touch open row indexes active", key: "azylman/aerial#101", row: openRow, wantSet: true,
			want:      HydrateResponse{Changed: true, Agent: "miorine", Status: RegistryStatusOpen},
			wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: "HandlePRIndex", input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", Lifecycle: IndexActive}}},
		},
		{
			name: "filling only the merge sha re-indexes merged", key: "azylman/aerial#101", state: mergeOnly, row: deployingRow, wantSet: true,
			want:      HydrateResponse{Changed: true, Agent: "suletta", Status: RegistryStatusMerged},
			wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: "HandlePRIndex", input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", MergeSHA: "m1", Lifecycle: IndexMerged}}},
		},
		{
			name: "closed row indexes closed", key: "azylman/aerial#101", row: RegistryRow{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", Status: RegistryStatusClosed}, wantSet: true,
			want:      HydrateResponse{Changed: true, Agent: "aerial", Status: RegistryStatusClosed},
			wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: "HandlePRIndex", input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Lifecycle: IndexClosed}}},
		},
		{name: "replay is a no-op", key: "azylman/aerial#101", state: hydratedOpen, row: openRow, want: HydrateResponse{Agent: "miorine", Status: RegistryStatusOpen}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var existing *PRState
			if tc.state != nil {
				clone := *tc.state
				existing = &clone
			}
			mc, rec := newKeyedStateMock(t, tc.key, existing)
			if tc.failGet {
				mc = newStateErrorMock(t)
				mc.On("Key").Return(tc.key)
			}
			got, err := NewPRObjectService().Hydrate(restate.WithMockContext(mc), HydrateRequest{Row: tc.row})
			require.Equal(t, tc.wantErr, errString(err))
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}
