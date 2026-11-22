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

// projSvc builds a PRObject service with the requested projection store.
func projSvc(store ProjectionStore) *PRObjectService {
	return NewPRObjectService().WithProjection(store)
}

// openPR seeds an open PR on head h1.
func openPR() *PRState {
	return svcRow("h1", "", RegistryStatusOpen, TerminalNone)
}

// TestPRObjectServiceAfterCommit pins the post-commit projection send: an
// event self-sends Project when a store is wired and an unwired service
// sends nothing.
func TestPRObjectServiceAfterCommit(t *testing.T) {
	t.Parallel()
	project := []sentCall{{service: "PRObject", key: "azylman/aerial#101", method: "Project", input: ProjectRequest{}}}
	tests := []struct {
		name      string
		svc       *PRObjectService
		wantSet   bool
		wantSends []sentCall
	}{
		{name: "wired store projects", svc: projSvc(&FakeProjectionStore{}), wantSet: true, wantSends: project},
		{name: "no store sends nothing", svc: projSvc(nil), wantSet: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, openPR())
			err := tc.svc.HandleCheckStatus(restate.WithMockContext(mc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci", CheckState: "pending"})
			require.NoError(t, err)
			require.Equal(t, tc.wantSet, rec.setCalled)
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
			projSvc(&FakeProjectionStore{}).afterCommit(restate.WithMockContext(mc), tc.state)
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
	tracked := openPR()
	tracked.TargetID = snowA
	projected := openPR()
	projected.TargetID = snowA
	projected.ProjectedKey = ProjectionJournalKey(BuildProjection(tracked))
	stale := openPR()
	stale.TargetID = snowA
	stale.ProjectedKey = "projection:old"
	untracked := openPR()
	untracked.ProjectedKey = "projection:old"
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
		{name: "untracked pr without a target projects nothing", state: untracked, store: &FakeProjectionStore{}},
		{
			name: "changed snapshot writes the full row", state: stale, store: &FakeProjectionStore{}, wantRun: true, wantSet: true,
			wantKeyPrefix: "projection:aerial:azylman/aerial:101@h1:gen0:epoch0:",
			wantRecorded:  []ProjectionSnapshot{{Repo: "azylman/aerial", PRNumber: 101, Agent: "aerial", HeadSHA: "h1", TargetID: snowA, Status: "open"}},
		},
		{
			name: "exhausted write retries later", state: tracked, store: &FakeProjectionStore{Err: errFakeProjection}, wantRun: true,
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
				svc = svc.WithProjection(tc.store)
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
