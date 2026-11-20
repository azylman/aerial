package prstate

import (
	"errors"
	"sort"
	"strings"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// stateCapture records whether the dark service persisted Restate K/V state
// and what it saved. State assertions use hand-derived literals, never the
// values the service computed.
type stateCapture[T any] struct {
	setCalled bool
	saved     T
}

// newStateMock stubs the Restate K/V surface for one dark handler call: Get
// returns existing (nil for first touch), Set records the save. Set is Maybe
// because drop/absorb/idempotent dispositions must not persist.
func newStateMock[T any](t *testing.T, existing T) (*mocks.MockContext, *stateCapture[T]) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	rec := &stateCapture[T]{}
	mc.On("Get", "state", mock.Anything).Run(func(args mock.Arguments) {
		if pp, ok := args.Get(1).(*T); ok && pp != nil {
			*pp = existing
		}
	}).Return(true, nil)
	mc.On("Set", "state", mock.Anything).Run(func(args mock.Arguments) {
		rec.setCalled = true
		if saved, ok := args.Get(1).(T); ok {
			rec.saved = saved
		}
	}).Maybe()
	return mc, rec
}

// errKVUnavailable is the hermetic Restate K/V failure for error-path rows.
var errKVUnavailable = errors.New("kv unavailable")

// newStateErrorMock stubs a failing Restate K/V Get: every handler must
// surface the error and persist nothing.
func newStateErrorMock(t *testing.T) *mocks.MockContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.On("Get", "state", mock.Anything).Return(false, restate.ToTerminalError(errKVUnavailable))
	return mc
}

// sortedHandlerNames joins reflected handler names for exact surface pins.
func sortedHandlerNames(def restate.ServiceDefinition) string {
	names := make([]string, 0, len(def.Handlers()))
	for name := range def.Handlers() {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// TestRegisterPRStateServices pins dark registration: frozen service names
// matching the synapse ingress invoker defaults, the exact 13-handler PR
// surface and 4-handler repo surface, and nil-safe dark defaults.
func TestRegisterPRStateServices(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		prNil           bool
		repoNil         bool
		wantPRName      string
		wantRepoName    string
		wantPRHandlers  string
		wantRepoHandler string
	}{
		{
			name:            "nil services get dark defaults",
			prNil:           true,
			repoNil:         true,
			wantPRName:      "PRObject",
			wantRepoName:    "RepoObject",
			wantPRHandlers:  "HandleCIRecovery,HandleCheckFailed,HandleCheckPassed,HandleCheckRerunRequested,HandleClosed,HandleCollaboratorAlert,HandleConflictDetected,HandleConflictPublished,HandleConflictResolved,HandleDeployDispatched,HandleDeploySuccessPublished,HandleKustomizationRequested,HandleMerged,HandleReadinessRetry,HandleReopened,HandleRolloutProgress",
			wantRepoHandler: "HandleFanout,HandleKustomizationStatus,HandleMainPush,ResolvePRBySHA",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var pr *PRObjectService
			var repo *RepoObjectService
			if !tc.prNil {
				pr = NewPRObjectService()
			}
			if !tc.repoNil {
				repo = NewRepoObjectService()
			}
			srv := server.NewRestate()
			gotPR, gotRepo := RegisterPRStateServices(srv, pr, repo)
			require.NotNil(t, gotPR)
			require.NotNil(t, gotRepo)
			if gotPR.ServiceName() != tc.wantPRName {
				t.Fatalf("pr service name = %q, want %q", gotPR.ServiceName(), tc.wantPRName)
			}
			if gotRepo.ServiceName() != tc.wantRepoName {
				t.Fatalf("repo service name = %q, want %q", gotRepo.ServiceName(), tc.wantRepoName)
			}
			if got := sortedHandlerNames(restate.Reflect(gotPR)); got != tc.wantPRHandlers {
				t.Fatalf("pr handlers = %q, want %q", got, tc.wantPRHandlers)
			}
			if got := sortedHandlerNames(restate.Reflect(gotRepo)); got != tc.wantRepoHandler {
				t.Fatalf("repo handlers = %q, want %q", got, tc.wantRepoHandler)
			}
		})
	}
}

// TestPRObjectService_DarkHandlers proves the forwarding and first-touch
// persistence wiring through representative handlers: applied events persist
// state exactly once with no external effects.
func TestPRObjectService_DarkHandlers(t *testing.T) {
	t.Parallel()
	ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", Event: "check_failed"}
	prMc01, prRec01 := newStateMock[*PRState](t, nil)
	prMc02, prRec02 := newStateMock[*PRState](t, nil)
	prMc03, prRec03 := newStateMock[*PRState](t, nil)
	prMc04, prRec04 := newStateMock[*PRState](t, nil)
	prMc05, prRec05 := newStateMock[*PRState](t, nil)
	prMc06, prRec06 := newStateMock[*PRState](t, nil)
	prMc07, prRec07 := newStateMock[*PRState](t, nil)
	prMc08, prRec08 := newStateMock[*PRState](t, nil)
	prMc09, prRec09 := newStateMock[*PRState](t, nil)
	prMc10, prRec10 := newStateMock[*PRState](t, nil)
	prMc11, prRec11 := newStateMock[*PRState](t, nil)
	tests := []struct {
		name         string
		got          error
		rec          *stateCapture[*PRState]
		wantSet      bool
		wantTerminal TerminalState
		wantAlert    string
		wantGen      uint64
	}{
		{name: "check failed applies", got: NewPRObjectService().HandleCheckFailed(restate.WithMockContext(prMc01), ev), rec: prRec01, wantSet: true},
		{name: "check passed applies", got: NewPRObjectService().HandleCheckPassed(restate.WithMockContext(prMc02), ev), rec: prRec02, wantSet: true},
		{name: "check rerun requested applies", got: NewPRObjectService().HandleCheckRerunRequested(restate.WithMockContext(prMc03), ev), rec: prRec03, wantSet: true},
		{name: "collaborator alert applies", got: NewPRObjectService().HandleCollaboratorAlert(restate.WithMockContext(prMc04), ev), rec: prRec04, wantSet: true, wantAlert: "abc123"},
		{name: "conflict resolved applies", got: NewPRObjectService().HandleConflictResolved(restate.WithMockContext(prMc05), ev), rec: prRec05, wantSet: true},
		{name: "merged applies", got: NewPRObjectService().HandleMerged(restate.WithMockContext(prMc06), ev), rec: prRec06, wantSet: true, wantTerminal: TerminalMerged},
		{name: "closed applies", got: NewPRObjectService().HandleClosed(restate.WithMockContext(prMc07), ev), rec: prRec07, wantSet: true, wantTerminal: TerminalClosed},
		{name: "reopened applies", got: NewPRObjectService().HandleReopened(restate.WithMockContext(prMc08), ev), rec: prRec08, wantSet: true},
		{name: "readiness retry applies", got: NewPRObjectService().HandleReadinessRetry(restate.WithMockContext(prMc09), ev), rec: prRec09, wantSet: true},
		{name: "rollout progress applies", got: NewPRObjectService().HandleRolloutProgress(restate.WithMockContext(prMc10), ev), rec: prRec10, wantSet: true},
		{name: "kustomization requested applies", got: NewPRObjectService().HandleKustomizationRequested(restate.WithMockContext(prMc11), ev), rec: prRec11, wantSet: true, wantGen: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != nil {
				t.Fatalf("handler returned %v, want nil", tc.got)
			}
			if tc.rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", tc.rec.setCalled, tc.wantSet)
			}
			if tc.wantSet {
				require.NotNil(t, tc.rec.saved)
				if tc.rec.saved.Repo != "azylman/aerial" || tc.rec.saved.PRNumber != 101 {
					t.Fatalf("saved identity = (%q,%d), want (azylman/aerial,101)", tc.rec.saved.Repo, tc.rec.saved.PRNumber)
				}
				if tc.wantTerminal != TerminalNone && tc.rec.saved.Terminal != tc.wantTerminal {
					t.Fatalf("saved terminal = %v, want %v", tc.rec.saved.Terminal, tc.wantTerminal)
				}
				if tc.wantAlert != "" && tc.rec.saved.CollaboratorAlerted != tc.wantAlert {
					t.Fatalf("saved collaborator alert = %q, want %q", tc.rec.saved.CollaboratorAlerted, tc.wantAlert)
				}
				if tc.wantGen != 0 && tc.rec.saved.GateGeneration != tc.wantGen {
					t.Fatalf("saved gate generation = %d, want %d", tc.rec.saved.GateGeneration, tc.wantGen)
				}
			}
		})
	}
}

// TestPRObjectService_HandleCheckFailed tests granular check recording transitions.
func TestPRObjectService_HandleCheckFailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		existing       *PRState
		ev             PREvent
		wantSet        bool
		wantFailedCI   string
		wantNonRoboRev string
		wantActiveFail bool
		wantRoboActive bool
	}{
		{
			name:           "non-roborev check failure",
			ev:             PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "ci/lint"},
			wantSet:        true,
			wantFailedCI:   "abc123",
			wantNonRoboRev: "abc123",
			wantActiveFail: true,
		},
		{
			name:           "roborev check failure",
			ev:             PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "roborev"},
			wantSet:        true,
			wantFailedCI:   "abc123",
			wantRoboActive: true,
		},
		{
			name:         "empty check name records failed ci sha only",
			ev:           PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"},
			wantSet:      true,
			wantFailedCI: "abc123",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock[*PRState](t, tc.existing)
			err := NewPRObjectService().HandleCheckFailed(restate.WithMockContext(mc), tc.ev)
			require.NoError(t, err)
			if rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", rec.setCalled, tc.wantSet)
			}
			if tc.wantSet {
				require.NotNil(t, rec.saved)
				if rec.saved.FailedCISHA != tc.wantFailedCI {
					t.Fatalf("FailedCISHA = %q, want %q", rec.saved.FailedCISHA, tc.wantFailedCI)
				}
				if rec.saved.NonRoboRevSHA != tc.wantNonRoboRev {
					t.Fatalf("NonRoboRevSHA = %q, want %q", rec.saved.NonRoboRevSHA, tc.wantNonRoboRev)
				}
				if tc.wantActiveFail && !rec.saved.Checks.ActiveFailed[CanonicalFailedCheckKey(tc.ev.CheckName)] {
					t.Fatalf("ActiveFailed[%s] = false, want true", tc.ev.CheckName)
				}
				if tc.wantRoboActive != rec.saved.Checks.RoboRevActiveFailed {
					t.Fatalf("RoboRevActiveFailed = %v, want %v", rec.saved.Checks.RoboRevActiveFailed, tc.wantRoboActive)
				}
			}
		})
	}
}

// TestPRObjectService_HandleCheckPassed tests granular check pass transitions.
func TestPRObjectService_HandleCheckPassed(t *testing.T) {
	t.Parallel()
	priorState := NewPRState("aerial", 101)
	priorState.RecordFailedCheckRunDetailed("abc123", "ci/lint")
	priorState.RecordNonRoboRevFailedSHA("abc123")
	priorState.RecordFailedCISHA("abc123")

	priorMultiState := NewPRState("aerial", 101)
	priorMultiState.RecordFailedCheckRunDetailed("abc123", "ci/lint")
	priorMultiState.RecordFailedCheckRunDetailed("abc123", "ci/unit")
	priorMultiState.RecordNonRoboRevFailedSHA("abc123")
	priorMultiState.RecordFailedCISHA("abc123")

	tests := []struct {
		name           string
		existing       *PRState
		ev             PREvent
		wantSet        bool
		wantFailedCI   string
		wantNonRoboRev string
	}{
		{
			name:     "clears last active failure and clears failed ci sha",
			existing: priorState,
			ev:       PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "ci/lint"},
			wantSet:  true,
		},
		{
			name:           "clears one failure but retains ci sha while other failure active",
			existing:       priorMultiState,
			ev:             PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "ci/lint"},
			wantSet:        true,
			wantFailedCI:   "abc123",
			wantNonRoboRev: "abc123",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.existing)
			err := NewPRObjectService().HandleCheckPassed(restate.WithMockContext(mc), tc.ev)
			require.NoError(t, err)
			if rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", rec.setCalled, tc.wantSet)
			}
			if tc.wantSet {
				require.NotNil(t, rec.saved)
				if rec.saved.FailedCISHA != tc.wantFailedCI {
					t.Fatalf("FailedCISHA = %q, want %q", rec.saved.FailedCISHA, tc.wantFailedCI)
				}
				if rec.saved.NonRoboRevSHA != tc.wantNonRoboRev {
					t.Fatalf("NonRoboRevSHA = %q, want %q", rec.saved.NonRoboRevSHA, tc.wantNonRoboRev)
				}
			}
		})
	}
}

// TestPRObjectService_DarkDispositions pins the shared stale-drop and
// terminal-absorbing guards through one representative handler: stale SHAs
// and same-epoch terminal states persist nothing, newer reopen epochs apply,
// and K/V failures surface without persisting.
func TestPRObjectService_DarkDispositions(t *testing.T) {
	t.Parallel()
	terminalState := NewPRState("aerial", 101)
	terminalState.HeadSHA = "aaa111"
	terminalState.Terminal = TerminalMerged
	reopenedState := NewPRState("aerial", 101)
	reopenedState.HeadSHA = "aaa111"
	reopenedState.Terminal = TerminalMerged
	staleState := NewPRState("aerial", 101)
	staleState.HeadSHA = "aaa111"
	tests := []struct {
		name       string
		existing   *PRState
		ev         PREvent
		failGet    bool
		expectNoKV bool
		wantSet    bool
		wantErr    bool
	}{
		{name: "fresh state applies", existing: nil, ev: PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, wantSet: true},
		{name: "stale sha drops", existing: staleState, ev: PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "bbb222"}, wantSet: false},
		{name: "terminal absorbs same epoch", existing: terminalState, ev: PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "aaa111"}, wantSet: false},
		{name: "reopen epoch applies", existing: reopenedState, ev: PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "aaa111", ReopenEpoch: 1}, wantSet: true},
		{name: "zero sha event applies on empty state", existing: nil, ev: PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "0000000"}, wantSet: true},
		{name: "state load error surfaces", failGet: true, ev: PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, wantSet: false, wantErr: true},
		{name: "empty repo drops before kv", existing: nil, ev: PREvent{Repo: "", PRNumber: 101, HeadSHA: "abc123"}, expectNoKV: true, wantSet: false},
		{name: "zero pr number drops before kv", existing: nil, ev: PREvent{Repo: "aerial", PRNumber: 0, HeadSHA: "abc123"}, expectNoKV: true, wantSet: false},
		{name: "negative pr number drops before kv", existing: nil, ev: PREvent{Repo: "aerial", PRNumber: -1, HeadSHA: "abc123"}, expectNoKV: true, wantSet: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			switch {
			case tc.expectNoKV:
				mc = mocks.NewMockContext(t)
			case tc.failGet:
				mc = newStateErrorMock(t)
			default:
				mc, rec = newStateMock(t, tc.existing)
			}
			err := NewPRObjectService().HandleCheckFailed(restate.WithMockContext(mc), tc.ev)
			if tc.wantErr && err == nil {
				t.Fatal("handler returned nil, want kv error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("handler returned %v, want nil", err)
			}
			if rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", rec.setCalled, tc.wantSet)
			}
		})
	}
}

// TestPRObjectService_ConflictResolve pins the conflict-republish verdict
// wiring: fresh and new SHAs publish, recorded SHAs dedupe, terminal
// states absorb, and the resolve never persists. K/V failures surface.
func TestPRObjectService_ConflictResolve(t *testing.T) {
	t.Parallel()
	recordedState := NewPRState("aerial", 101)
	recordedState.MarkConflictPublished("abc123")
	terminalState := NewPRState("aerial", 101)
	terminalState.MarkTerminal(TerminalMerged)
	tests := []struct {
		name              string
		existing          *PRState
		evHead            string
		failGet           bool
		wantShouldPublish bool
		wantDeduped       bool
		wantErr           bool
	}{
		{name: "fresh state publishes", evHead: "abc123", wantShouldPublish: true},
		{name: "new sha publishes after recorded publish", existing: recordedState, evHead: "def456", wantShouldPublish: true},
		{name: "recorded sha dedupes", existing: recordedState, evHead: "abc123", wantDeduped: true},
		{name: "terminal absorbs without publish", existing: terminalState, evHead: "abc123"},
		{name: "state load error surfaces", failGet: true, evHead: "abc123", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.existing)
			}
			ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: tc.evHead, Event: "conflict_detected"}
			got, err := NewPRObjectService().HandleConflictDetected(restate.WithMockContext(mc), ev)
			if tc.wantErr && err == nil {
				t.Fatal("HandleConflictDetected err = nil, want kv error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleConflictDetected err = %v, want nil", err)
			}
			if !tc.wantErr && (got.ShouldPublish != tc.wantShouldPublish || got.Deduped != tc.wantDeduped) {
				t.Fatalf("ShouldPublish/Deduped = %v/%v, want %v/%v", got.ShouldPublish, got.Deduped, tc.wantShouldPublish, tc.wantDeduped)
			}
			if rec.setCalled {
				t.Fatal("resolve persisted state, want no record")
			}
		})
	}
}

// TestPRObjectService_ConflictPublishedAck pins the publish-recorded ack
// wiring: fresh publishes record and persist, recorded SHAs re-ack
// idempotently, terminal states absorb without persisting. K/V failures
// surface.
func TestPRObjectService_ConflictPublishedAck(t *testing.T) {
	t.Parallel()
	recordedSame := NewPRState("aerial", 101)
	recordedSame.MarkConflictPublished("abc123")
	recordedNew := NewPRState("aerial", 101)
	recordedNew.MarkConflictPublished("abc123")
	terminalState := NewPRState("aerial", 101)
	terminalState.MarkTerminal(TerminalMerged)
	tests := []struct {
		name          string
		repo          string
		pr            int
		existing      *PRState
		evHead        string
		failGet       bool
		expectNoKV    bool
		wantRecorded  bool
		wantDeduped   bool
		wantSet       bool
		wantErr       bool
		wantPublished string
	}{
		{name: "fresh publish records and persists", repo: "aerial", pr: 101, evHead: "abc123", wantRecorded: true, wantSet: true, wantPublished: "abc123"},
		{name: "recorded publish re-acks idempotent", repo: "aerial", pr: 101, existing: recordedSame, evHead: "abc123", wantRecorded: true, wantDeduped: true, wantSet: true, wantPublished: "abc123"},
		{name: "new sha overwrites and persists", repo: "aerial", pr: 101, existing: recordedNew, evHead: "def456", wantRecorded: true, wantSet: true, wantPublished: "def456"},
		{name: "terminal absorbs without persisting", repo: "aerial", pr: 101, existing: terminalState, evHead: "abc123"},
		{name: "state load error surfaces", repo: "aerial", pr: 101, failGet: true, evHead: "abc123", wantErr: true},
		{name: "empty repo drops before kv", repo: "", pr: 101, evHead: "abc123", expectNoKV: true, wantSet: false},
		{name: "zero pr drops before kv", repo: "aerial", pr: 0, evHead: "abc123", expectNoKV: true, wantSet: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			switch {
			case tc.expectNoKV:
				mc = mocks.NewMockContext(t)
			case tc.failGet:
				mc = newStateErrorMock(t)
			default:
				mc, rec = newStateMock(t, tc.existing)
			}
			ev := PREvent{Repo: tc.repo, PRNumber: tc.pr, HeadSHA: tc.evHead, Event: "conflict_detected"}
			got, err := NewPRObjectService().HandleConflictPublished(restate.WithMockContext(mc), ev)
			if tc.wantErr && err == nil {
				t.Fatal("HandleConflictPublished err = nil, want kv error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleConflictPublished err = %v, want nil", err)
			}
			if !tc.wantErr && (got.Recorded != tc.wantRecorded || got.Deduped != tc.wantDeduped) {
				t.Fatalf("Recorded/Deduped = %v/%v, want %v/%v", got.Recorded, got.Deduped, tc.wantRecorded, tc.wantDeduped)
			}
			if rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", rec.setCalled, tc.wantSet)
			}
			if tc.wantSet && rec.saved.ConflictPublished != tc.wantPublished {
				t.Fatalf("saved ConflictPublished = %q, want %q", rec.saved.ConflictPublished, tc.wantPublished)
			}
		})
	}
}

// TestPRObjectService_DeployDispatched pins the deploy_success publish verdict wiring.
func TestPRObjectService_DeployDispatched(t *testing.T) {
	t.Parallel()
	dispatchedState := NewPRState("aerial", 101)
	dispatchedKey := dispatchedState.EffectKeyFor(EffectDeployDispatch, "abc123", "deploy-job", "")
	dispatchedState.MarkEffectDispatched(dispatchedKey)
	dispatchedState.RecordDeployedPR()
	dispatchedState.RecordDeployedSHAJob("deploy-job", "abc123")

	tests := []struct {
		name              string
		existing          *PRState
		evHead            string
		failGet           bool
		wantShouldPublish bool
		wantDeduped       bool
		wantErr           bool
	}{
		{name: "fresh dispatch publishes", evHead: "abc123", wantShouldPublish: true},
		{name: "recorded dispatch dedupes", existing: dispatchedState, evHead: "abc123", wantDeduped: true},
		{name: "state load error surfaces", failGet: true, evHead: "abc123", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, _ = newStateMock(t, tc.existing)
			}
			ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: tc.evHead, JobName: "deploy-job", Event: "deploy_dispatched"}
			got, err := NewPRObjectService().HandleDeployDispatched(restate.WithMockContext(mc), ev)
			if tc.wantErr && err == nil {
				t.Fatal("HandleDeployDispatched err = nil, want kv error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleDeployDispatched err = %v, want nil", err)
			}
			if !tc.wantErr && (got.ShouldPublish != tc.wantShouldPublish || got.Deduped != tc.wantDeduped) {
				t.Fatalf("ShouldPublish/Deduped = %v/%v, want %v/%v", got.ShouldPublish, got.Deduped, tc.wantShouldPublish, tc.wantDeduped)
			}
		})
	}
}

// TestPRObjectService_DeploySuccessPublishedAck pins the deploy_success ack recording and input guards.
func TestPRObjectService_DeploySuccessPublishedAck(t *testing.T) {
	t.Parallel()
	terminalState := NewPRState("aerial", 101)
	terminalState.MarkTerminal(TerminalMerged)

	tests := []struct {
		name         string
		repo         string
		pr           int
		existing     *PRState
		evHead       string
		failGet      bool
		expectNoKV   bool
		wantRecorded bool
		wantSet      bool
		wantErr      bool
	}{
		{name: "fresh publish records and persists", repo: "aerial", pr: 101, evHead: "abc123", wantRecorded: true, wantSet: true},
		{name: "empty repo drops before kv", repo: "", pr: 101, evHead: "abc123", expectNoKV: true, wantSet: false},
		{name: "zero pr drops before kv", repo: "aerial", pr: 0, evHead: "abc123", expectNoKV: true, wantSet: false},
		{name: "terminal absorbs without persisting", repo: "aerial", pr: 101, existing: terminalState, evHead: "abc123", wantSet: false},
		{name: "state load error surfaces", repo: "aerial", pr: 101, failGet: true, evHead: "abc123", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			switch {
			case tc.expectNoKV:
				mc = mocks.NewMockContext(t)
			case tc.failGet:
				mc = newStateErrorMock(t)
			default:
				mc, rec = newStateMock(t, tc.existing)
			}
			ev := PREvent{Repo: tc.repo, PRNumber: tc.pr, HeadSHA: tc.evHead, JobName: "deploy-job", Event: "deploy_success"}
			got, err := NewPRObjectService().HandleDeploySuccessPublished(restate.WithMockContext(mc), ev)
			if tc.wantErr && err == nil {
				t.Fatal("HandleDeploySuccessPublished err = nil, want kv error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleDeploySuccessPublished err = %v, want nil", err)
			}
			if !tc.wantErr && got.Recorded != tc.wantRecorded {
				t.Fatalf("Recorded = %v, want %v", got.Recorded, tc.wantRecorded)
			}
			if rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", rec.setCalled, tc.wantSet)
			}
		})
	}
}

// TestPRObjectService_CIRecoveryResolve pins the CI-recovery verdict wiring:
// recorded failures recover, clean states stay clean, and the resolve never
// persists. K/V failures surface.
func TestPRObjectService_CIRecoveryResolve(t *testing.T) {
	t.Parallel()
	failedState := NewPRState("aerial", 101)
	failedState.RecordFailedCISHA("abc123")
	tests := []struct {
		name       string
		existing   *PRState
		evHead     string
		failGet    bool
		wantFailed bool
		wantErr    bool
	}{
		{name: "recorded failure recovers", existing: failedState, evHead: "abc123", wantFailed: true},
		{name: "sha mismatch stays clean", existing: failedState, evHead: "def456"},
		{name: "clean state stays clean", evHead: "abc123"},
		{name: "state load error surfaces", failGet: true, evHead: "abc123", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.existing)
			}
			ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: tc.evHead, Event: "check_success"}
			got, err := NewPRObjectService().HandleCIRecovery(restate.WithMockContext(mc), ev)
			if tc.wantErr && err == nil {
				t.Fatal("HandleCIRecovery err = nil, want kv error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("HandleCIRecovery err = %v, want nil", err)
			}
			if !tc.wantErr && got.CIFailed != tc.wantFailed {
				t.Fatalf("CIFailed = %v, want %v", got.CIFailed, tc.wantFailed)
			}
			if rec.setCalled {
				t.Fatal("resolve persisted state, want no record")
			}
		})
	}
}

// TestRepoObjectService_DarkHandlers pins the frozen repo handlers: main
// pushes persist exactly once per new SHA, idempotent and invalid pushes
// persist nothing, kustomization statuses scope tombstones by generation,
// and K/V failures surface without persisting.
func TestRepoObjectService_DarkHandlers(t *testing.T) {
	t.Parallel()
	dupState := NewRepoState("aerial")
	dupState.RecordMainPushSHA("abc123")
	pushMc, pushRec := newStateMock(t, (*RepoState)(nil))
	dupMc, dupRec := newStateMock(t, dupState)
	badPushMc := mocks.NewMockContext(t)
	badRepoMc := mocks.NewMockContext(t)
	pushErrMc := newStateErrorMock(t)
	statusMc, statusRec := newStateMock(t, (*RepoState)(nil))
	emptyMc, emptyRec := newStateMock(t, (*RepoState)(nil))
	emptyRepoMc := mocks.NewMockContext(t)
	statusErrMc := newStateErrorMock(t)
	tests := []struct {
		name        string
		got         error
		rec         *stateCapture[*RepoState]
		wantSet     bool
		wantErr     bool
		wantLatest  string
		wantTombSHA string
		wantTombGen uint64
	}{
		{name: "main push persists", got: NewRepoObjectService().HandleMainPush(restate.WithMockContext(pushMc), RepoEvent{Repo: "aerial", MainSHA: "abc123"}), rec: pushRec, wantSet: true, wantLatest: "abc123"},
		{name: "duplicate push persists nothing", got: NewRepoObjectService().HandleMainPush(restate.WithMockContext(dupMc), RepoEvent{Repo: "aerial", MainSHA: "abc123"}), rec: dupRec, wantSet: false},
		{name: "empty sha push persists nothing", got: NewRepoObjectService().HandleMainPush(restate.WithMockContext(badPushMc), RepoEvent{Repo: "aerial"}), rec: &stateCapture[*RepoState]{}, wantSet: false},
		{name: "empty repo push persists nothing", got: NewRepoObjectService().HandleMainPush(restate.WithMockContext(badRepoMc), RepoEvent{MainSHA: "abc123"}), rec: &stateCapture[*RepoState]{}, wantSet: false},
		{name: "main push load error surfaces", got: NewRepoObjectService().HandleMainPush(restate.WithMockContext(pushErrMc), RepoEvent{Repo: "aerial", MainSHA: "abc123"}), rec: &stateCapture[*RepoState]{}, wantSet: false, wantErr: true},
		{name: "kustomization status scopes tombstone", got: NewRepoObjectService().HandleKustomizationStatus(restate.WithMockContext(statusMc), RepoEvent{Repo: "aerial", HeadSHA: "abc123", Generation: 2}), rec: statusRec, wantSet: true, wantTombSHA: "abc123", wantTombGen: 2},
		{name: "empty sha status persists nothing", got: NewRepoObjectService().HandleKustomizationStatus(restate.WithMockContext(emptyMc), RepoEvent{Repo: "aerial", Generation: 2}), rec: emptyRec, wantSet: false},
		{name: "empty repo status persists nothing", got: NewRepoObjectService().HandleKustomizationStatus(restate.WithMockContext(emptyRepoMc), RepoEvent{HeadSHA: "abc123", Generation: 2}), rec: &stateCapture[*RepoState]{}, wantSet: false},
		{name: "kustomization status load error surfaces", got: NewRepoObjectService().HandleKustomizationStatus(restate.WithMockContext(statusErrMc), RepoEvent{Repo: "aerial", HeadSHA: "abc123", Generation: 2}), rec: &stateCapture[*RepoState]{}, wantSet: false, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.wantErr && tc.got == nil {
				t.Fatal("handler returned nil, want kv error")
			}
			if !tc.wantErr && tc.got != nil {
				t.Fatalf("handler returned %v, want nil", tc.got)
			}
			if tc.rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", tc.rec.setCalled, tc.wantSet)
			}
			if !tc.wantSet {
				return
			}
			require.NotNil(t, tc.rec.saved)
			if tc.wantLatest != "" && tc.rec.saved.LatestMainSHA != tc.wantLatest {
				t.Fatalf("latest = %q, want %q", tc.rec.saved.LatestMainSHA, tc.wantLatest)
			}
			if tc.wantTombSHA != "" && tc.rec.saved.Tombstones[tc.wantTombSHA] != tc.wantTombGen {
				t.Fatalf("tombstone[%q] = %d, want %d", tc.wantTombSHA, tc.rec.saved.Tombstones[tc.wantTombSHA], tc.wantTombGen)
			}
		})
	}
}

// TestRepoObjectService_FanoutAndResolve pins the dark fanout and SHA resolve stubs.
func TestRepoObjectService_FanoutAndResolve(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	repoSvc := NewRepoObjectService()
	err := repoSvc.HandleFanout(restate.WithMockContext(mc), RepoEvent{Repo: "aerial"})
	require.NoError(t, err)

	resp, rErr := repoSvc.ResolvePRBySHA(restate.WithMockContext(mc), ResolveSHARequest{Repo: "aerial", SHA: "abc123"})
	require.NoError(t, rErr)
	require.False(t, resp.Found)
}

// TestPRObjectService_OwnerPreserved proves lifecycle events never clobber a
// stored owner: an event agent is only a hint that fills an empty slot.
func TestPRObjectService_OwnerPreserved(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		existing  *PRState
		evAgent   string
		wantAgent string
	}{
		{name: "stored owner survives a different event agent", existing: &PRState{Repo: "azylman/aerial", PRNumber: 7, Agent: "miorine"}, evAgent: "aerial", wantAgent: "miorine"},
		{name: "stored owner survives an empty event agent", existing: &PRState{Repo: "azylman/aerial", PRNumber: 7, Agent: "miorine"}, evAgent: "", wantAgent: "miorine"},
		{name: "hint fills an empty stored owner", existing: &PRState{Repo: "azylman/aerial", PRNumber: 7}, evAgent: " suletta ", wantAgent: "suletta"},
		{name: "first touch adopts the hint", existing: nil, evAgent: "suletta", wantAgent: "suletta"},
		{name: "first touch without hint leaves owner unset", existing: nil, evAgent: "", wantAgent: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock[*PRState](t, tc.existing)
			ev := PREvent{Repo: "aerial", PRNumber: 7, HeadSHA: "abc123", Agent: tc.evAgent, Event: "check_failed"}
			require.NoError(t, NewPRObjectService().HandleCheckFailed(restate.WithMockContext(mc), ev))
			require.True(t, rec.setCalled)
			assert.Equal(t, tc.wantAgent, rec.saved.Agent)
		})
	}
}
