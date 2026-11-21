package prstate

import (
	"errors"
	"sort"
	"strings"
	"sync"
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
	sends     *sendCapture
}

// sentCall is one Restate one-way send: target, handler, payload, and
// whether it carried a delay option.
type sentCall struct {
	service string
	key     string
	method  string
	input   any
	delayed bool
}

// sendCapture records every restate.ObjectSend in call order. ObjectSend
// resolves the client (Object) and sends on it immediately, so each Send
// completes the most recent Object call.
type sendCapture struct {
	mu    sync.Mutex
	calls []sentCall
}

func (c *sendCapture) record(args mock.Arguments) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, sentCall{service: args.String(0), key: args.String(1), method: args.String(2)})
}

func (c *sendCapture) complete(input any, delayed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	last := &c.calls[len(c.calls)-1]
	last.input = input
	last.delayed = delayed
}

func (c *sendCapture) snapshot() []sentCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sentCall(nil), c.calls...)
}

// captureSends stubs restate.ObjectSend on mc. Every expectation is Maybe so
// handlers that send nothing pass too; rows assert the exact send list.
func captureSends(t *testing.T, mc *mocks.MockContext) *sendCapture {
	t.Helper()
	rec := &sendCapture{}
	client := mocks.NewMockClient(t)
	invocation := mocks.NewMockInvocation(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Run(rec.record).Return(client).Maybe()
	client.On("Send", mock.Anything).Run(func(args mock.Arguments) { rec.complete(args.Get(0), false) }).Return(invocation).Maybe()
	client.On("Send", mock.Anything, mock.Anything).Run(func(args mock.Arguments) { rec.complete(args.Get(0), true) }).Return(invocation).Maybe()
	return rec
}

// newStateMock stubs the Restate K/V surface for one dark handler call: Get
// returns existing (nil for first touch), Set records the save. Set is Maybe
// because drop/absorb/idempotent dispositions must not persist.
func newStateMock[T any](t *testing.T, existing T) (*mocks.MockContext, *stateCapture[T]) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	rec := &stateCapture[T]{sends: captureSends(t, mc)}
	mc.On("Key").Return("azylman/aerial").Maybe()
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
// matching the synapse ingress invoker defaults, the exact 17-handler PR
// surface and 11-handler repo surface, and nil-safe dark defaults.
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
			wantPRHandlers:  "CompareProjection,GetBuildLifecycle,HandleBuildMetadata,HandleCIRecovery,HandleCheckFailed,HandleCheckPassed,HandleCheckRerunRequested,HandleCheckStatus,HandleClosed,HandleCollaboratorAlert,HandleConflictDetected,HandleConflictPublished,HandleConflictResolved,HandleDeployDispatched,HandleDeployFailed,HandleDeploySuccessPublished,HandleKustomizationRequested,HandleMainAdvanced,HandleMerged,HandleReadinessRetry,HandleReopened,HandleRolloutProgress,HandleSynchronized,Hydrate,Project,Register",
			wantRepoHandler: "HandleCheckRunBySHA,HandleFanout,HandleHeldExpiry,HandleHeldReplay,HandleImageReadyBySHA,HandleKustomizationStatus,HandleMainPush,HandlePRIndex,HandlePushBySHA,HandleWorkflowRunBySHA,ResolvePRBySHA",
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
	terminalState.MarkTerminal(TerminalClosed)

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
		{name: "closed terminal absorbs without persisting", repo: "aerial", pr: 101, existing: terminalState, evHead: "abc123", wantSet: false},
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
		{name: "stored owner survives a different event agent", existing: &PRState{Repo: "azylman/aerial", PRNumber: 7, Agent: "agent-alpha"}, evAgent: "aerial", wantAgent: "agent-alpha"},
		{name: "stored owner survives an empty event agent", existing: &PRState{Repo: "azylman/aerial", PRNumber: 7, Agent: "agent-alpha"}, evAgent: "", wantAgent: "agent-alpha"},
		{name: "hint fills an empty stored owner", existing: &PRState{Repo: "azylman/aerial", PRNumber: 7}, evAgent: " agent-beta ", wantAgent: "agent-beta"},
		{name: "first touch adopts the hint", existing: nil, evAgent: "agent-beta", wantAgent: "agent-beta"},
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

func prIndexSend(idx PRIndexEvent) sentCall {
	return sentCall{service: "RepoObject", key: "azylman/aerial", method: "HandlePRIndex", input: idx}
}

func prObjectSend(pr int, method string, ev PREvent) sentCall {
	return sentCall{service: "PRObject", key: PRObjectKey("aerial", pr), method: method, input: ev}
}

func assertSends(t *testing.T, got, want []sentCall) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	assert.Equal(t, want, got)
}

// TestPRObjectService_IndexSends pins the one-way HandlePRIndex updates
// PRObject sends RepoObject after commits that change a routed field, and
// that unchanged, stale, and deduped transitions send nothing.
func TestPRObjectService_IndexSends(t *testing.T) {
	t.Parallel()
	known := func() *PRState { return &PRState{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123"} }
	closed := known()
	closed.Terminal = TerminalClosed
	deduped := known()
	deduped.MarkEffectDispatched(deduped.EffectKeyFor(EffectDeployDispatch, "abc123", "eri", ""))
	firstMc, firstRec := newStateMock[*PRState](t, nil)
	sameMc, sameRec := newStateMock(t, known())
	staleMc, staleRec := newStateMock(t, known())
	mergedMc, mergedRec := newStateMock(t, known())
	closedMc, closedRec := newStateMock(t, known())
	reopenMc, reopenRec := newStateMock(t, closed)
	kustMc, kustRec := newStateMock(t, known())
	deployMc, deployRec := newStateMock(t, known())
	dedupMc, dedupRec := newStateMock(t, deduped)
	merged := func() *PRState {
		st := known()
		st.Terminal = TerminalMerged
		st.MergeSHA = "m1"
		st.Branch = "feat-a"
		return st
	}
	mergedKustMc, mergedKustRec := newStateMock(t, merged())
	mergedDeployMc, mergedDeployRec := newStateMock(t, merged())
	closedAgain := known()
	closedAgain.Terminal = TerminalClosed
	closedKustMc, closedKustRec := newStateMock(t, closedAgain)
	svc := NewPRObjectService()
	_, mergedDeployErr := svc.HandleDeploySuccessPublished(restate.WithMockContext(mergedDeployMc), PREvent{Repo: "aerial", PRNumber: 101, JobName: "eri", MergeSHA: "m1"})
	_, deployErr := svc.HandleDeploySuccessPublished(restate.WithMockContext(deployMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", JobName: "eri", MergeSHA: "m1"})
	_, dedupErr := svc.HandleDeploySuccessPublished(restate.WithMockContext(dedupMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", JobName: "eri"})
	tests := []struct {
		name      string
		got       error
		sends     *sendCapture
		wantSends []sentCall
	}{
		{name: "first head touch indexes active", got: svc.HandleCheckFailed(restate.WithMockContext(firstMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", Branch: " feat-a ", CheckName: "ci"}), sends: firstRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "abc123", Lifecycle: IndexActive})}},
		{name: "same head sends nothing", got: svc.HandleCheckPassed(restate.WithMockContext(sameMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}), sends: sameRec.sends},
		{name: "stale head sends nothing", got: svc.HandleCheckFailed(restate.WithMockContext(staleMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "def456"}), sends: staleRec.sends},
		{name: "merge indexes merge sha", got: svc.HandleMerged(restate.WithMockContext(mergedMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", MergeSHA: "m1"}), sends: mergedRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", MergeSHA: "m1", Lifecycle: IndexMerged})}},
		{name: "close indexes closed without merge sha", got: svc.HandleClosed(restate.WithMockContext(closedMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", MergeSHA: "m1"}), sends: closedRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", Lifecycle: IndexClosed})}},
		{name: "reopen indexes the new epoch", got: svc.HandleReopened(restate.WithMockContext(reopenMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", ReopenEpoch: 1}), sends: reopenRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", ReopenEpoch: 1, Lifecycle: IndexActive})}},
		{name: "kustomization request indexes deploying", got: svc.HandleKustomizationRequested(restate.WithMockContext(kustMc), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "abc123"}), sends: kustRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", MergeSHA: "abc123", Generation: 1, Lifecycle: IndexDeploying})}},
		{name: "recorded deploy success indexes deployed", got: deployErr, sends: deployRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", MergeSHA: "m1", Lifecycle: IndexDeployed})}},
		{name: "deduped deploy success sends nothing", got: dedupErr, sends: dedupRec.sends},
		{name: "merged kustomization request indexes deploying with stored merge sha", got: svc.HandleKustomizationRequested(restate.WithMockContext(mergedKustMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", Jobs: []string{"eri"}}), sends: mergedKustRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "abc123", MergeSHA: "m1", Generation: 1, Lifecycle: IndexDeploying})}},
		{name: "merged recorded deploy success indexes deployed", got: mergedDeployErr, sends: mergedDeployRec.sends, wantSends: []sentCall{prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "abc123", MergeSHA: "m1", Lifecycle: IndexDeployed})}},
		{name: "closed kustomization request sends nothing", got: svc.HandleKustomizationRequested(restate.WithMockContext(closedKustMc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}), sends: closedKustRec.sends},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, tc.got)
			assertSends(t, tc.sends.snapshot(), tc.wantSends)
		})
	}
}

// TestPRObjectService_HandleMainAdvanced proves the RepoObject main-push
// fan-out target clears updated-for-main and is absorbed by terminal PRs.
func TestPRObjectService_HandleMainAdvanced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		existing    *PRState
		wantSet     bool
		wantUpdated bool
	}{
		{name: "open pr clears updated for main", existing: &PRState{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", UpdatedForMain: true}, wantSet: true},
		{name: "merged pr absorbs the clear", existing: &PRState{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", UpdatedForMain: true, Terminal: TerminalMerged}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.existing)
			require.NoError(t, NewPRObjectService().HandleMainAdvanced(restate.WithMockContext(mc), PREvent{Event: "main_advanced", Repo: "aerial", PRNumber: 101}))
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				assert.Equal(t, tc.wantUpdated, rec.saved.UpdatedForMain)
			}
			assertSends(t, rec.sends.snapshot(), nil)
		})
	}
}

// bareRepoMock stubs a RepoObject invocation that must reject its input
// before touching Restate K/V or sending anything.
func bareRepoMock(t *testing.T) (*mocks.MockContext, *stateCapture[*RepoState]) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.On("Key").Return("azylman/aerial").Maybe()
	return mc, &stateCapture[*RepoState]{}
}

// repoMock stubs one RepoObject invocation over a seeded RepoState.
func repoMock(t *testing.T, seed repoSeed) (*mocks.MockContext, *stateCapture[*RepoState]) {
	t.Helper()
	var existing *RepoState
	if !seed.empty() {
		existing = loadRepoForTest(t, seedRepoStore(seed, false))
	}
	return newStateMock(t, existing)
}

// TestRepoObjectService_Handlers proves the Restate binding of every
// exclusive RepoObject handler: Restate K/V round-trips, one-way sends to
// PRObject, delayed self-sends, and terminal errors for malformed input.
func TestRepoObjectService_Handlers(t *testing.T) {
	t.Parallel()
	openPR := []PRIndexEvent{idxActive(101, "abc123", "feat-a", 0)}
	deploying := []PRIndexEvent{idxActive(101, "h1", "feat-a", 0), idxMerged(101, "h1", "m1"), idxDeploying(101, "m1", 2)}
	held := repoSeed{held: []SHAEvent{heldCheck("m9")}}
	svc := NewRepoObjectService()
	pushMc, pushRec := repoMock(t, repoSeed{mains: []string{"main1"}, index: openPR})
	dupMc, dupRec := repoMock(t, repoSeed{mains: []string{"main1"}})
	badPushMc, badPushRec := bareRepoMock(t)
	errPushMc := newStateErrorMock(t)
	fanMc, fanRec := repoMock(t, repoSeed{mains: []string{"main1"}, index: openPR})
	badFanMc, badFanRec := bareRepoMock(t)
	kustMc, kustRec := repoMock(t, repoSeed{index: deploying})
	badKustMc, badKustRec := bareRepoMock(t)
	idxMc, idxRec := repoMock(t, held)
	badIdxMc, badIdxRec := bareRepoMock(t)
	hitMc, hitRec := repoMock(t, repoSeed{index: openPR})
	missMc, missRec := repoMock(t, repoSeed{})
	pushHitMc, pushHitRec := repoMock(t, repoSeed{index: deploying})
	fullMc, fullRec := repoMock(t, repoSeed{held: heldFill(64)})
	badRouteMc, badRouteRec := bareRepoMock(t)
	errRouteMc := newStateErrorMock(t)
	replayMc, replayRec := repoMock(t, repoSeed{held: []SHAEvent{heldCheck("abc123")}, index: openPR})
	badReplayMc, badReplayRec := bareRepoMock(t)
	expireMc, expireRec := repoMock(t, held)
	staleExpireMc, staleExpireRec := repoMock(t, held)
	errExpireMc := newStateErrorMock(t)
	errExpireMc.On("Key").Return("azylman/aerial")
	forwardedCheck := PREvent{Event: "check_failed", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "abc123", CheckName: "ci", EffectsOwner: EffectsOwnerLegacy}
	tests := []struct {
		name         string
		got          error
		rec          *stateCapture[*RepoState]
		wantErr      bool
		wantTerminal bool
		wantSet      bool
		wantSends    []sentCall
	}{
		{name: "main push fans out to open prs", got: svc.HandleMainPush(restate.WithMockContext(pushMc), RepoEvent{Repo: "aerial", MainSHA: "main2"}), rec: pushRec, wantSet: true, wantSends: []sentCall{prObjectSend(101, "HandleMainAdvanced", PREvent{Event: "main_advanced", Repo: "azylman/aerial", PRNumber: 101, EffectsOwner: EffectsOwnerLegacy})}},
		{name: "duplicate main push persists nothing", got: svc.HandleMainPush(restate.WithMockContext(dupMc), RepoEvent{Repo: "aerial", MainSHA: "main1"}), rec: dupRec},
		{name: "zero sha main push fails terminally", got: svc.HandleMainPush(restate.WithMockContext(badPushMc), RepoEvent{Repo: "aerial", MainSHA: "0000000"}), rec: badPushRec, wantErr: true, wantTerminal: true},
		{name: "main push load error surfaces", got: svc.HandleMainPush(restate.WithMockContext(errPushMc), RepoEvent{Repo: "aerial", MainSHA: "main1"}), rec: &stateCapture[*RepoState]{}, wantErr: true, wantTerminal: true},
		{name: "fanout resends main advanced", got: svc.HandleFanout(restate.WithMockContext(fanMc), RepoEvent{Repo: "aerial"}), rec: fanRec, wantSends: []sentCall{prObjectSend(101, "HandleMainAdvanced", PREvent{Event: "main_advanced", Repo: "azylman/aerial", PRNumber: 101, EffectsOwner: EffectsOwnerLegacy})}},
		{name: "fanout without repo fails terminally", got: svc.HandleFanout(restate.WithMockContext(badFanMc), RepoEvent{}), rec: badFanRec, wantErr: true, wantTerminal: true},
		{name: "kustomization status records tombstone and forwards", got: svc.HandleKustomizationStatus(restate.WithMockContext(kustMc), RepoEvent{Repo: "aerial", HeadSHA: "dep1", Generation: 2, Kustomization: "eri"}), rec: kustRec, wantSet: true, wantSends: []sentCall{prObjectSend(101, "HandleRolloutProgress", PREvent{Event: "kustomization_status", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", GateGeneration: 2, EffectsOwner: EffectsOwnerLegacy, Kustomization: "eri"})}},
		{name: "kustomization status without repo fails terminally", got: svc.HandleKustomizationStatus(restate.WithMockContext(badKustMc), RepoEvent{HeadSHA: "dep1"}), rec: badKustRec, wantErr: true, wantTerminal: true},
		{name: "pr index of a held sha schedules delayed replay", got: svc.HandlePRIndex(restate.WithMockContext(idxMc), idxMerged(101, "h1", "m9")), rec: idxRec, wantSet: true, wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: "HandleHeldReplay", input: HeldReplayRequest{SHA: "m9"}, delayed: true}}},
		{name: "pr index without pr number fails terminally", got: svc.HandlePRIndex(restate.WithMockContext(badIdxMc), idxActive(0, "h1", "", 0)), rec: badIdxRec, wantErr: true, wantTerminal: true},
		{name: "check run hit forwards to probject", got: svc.HandleCheckRunBySHA(restate.WithMockContext(hitMc), heldCheck("abc123")), rec: hitRec, wantSends: []sentCall{prObjectSend(101, "HandleCheckFailed", forwardedCheck)}},
		{name: "workflow run miss holds with delayed expiry", got: svc.HandleWorkflowRunBySHA(restate.WithMockContext(missMc), heldCheck("abc123")), rec: missRec, wantSet: true, wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: "HandleHeldExpiry", input: HeldExpiryRequest{ID: 1}, delayed: true}}},
		{name: "push hit forwards merged", got: svc.HandlePushBySHA(restate.WithMockContext(pushHitMc), SHAEvent{Repo: "aerial", SHA: "m1", Handler: PRHandlerMerged, Event: PREvent{Event: "merged"}}), rec: pushHitRec, wantSends: []sentCall{prObjectSend(101, "HandleMerged", PREvent{Event: "merged", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", MergeSHA: "m1", EffectsOwner: EffectsOwnerLegacy})}},
		{name: "image ready miss at capacity evicts and holds", got: svc.HandleImageReadyBySHA(restate.WithMockContext(fullMc), SHAEvent{Repo: "aerial", SHA: "m2", Handler: PRHandlerRolloutProgress}), rec: fullRec, wantSet: true, wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: "HandleHeldExpiry", input: HeldExpiryRequest{ID: 65}, delayed: true}}},
		{name: "unroutable handler fails terminally", got: svc.HandleImageReadyBySHA(restate.WithMockContext(badRouteMc), SHAEvent{Repo: "aerial", SHA: "m2", Handler: "HandleDeployDispatched"}), rec: badRouteRec, wantErr: true, wantTerminal: true},
		{name: "route load error surfaces", got: svc.HandleCheckRunBySHA(restate.WithMockContext(errRouteMc), heldCheck("abc123")), rec: &stateCapture[*RepoState]{}, wantErr: true, wantTerminal: true},
		{name: "held replay forwards resolved events", got: svc.HandleHeldReplay(restate.WithMockContext(replayMc), HeldReplayRequest{SHA: "abc123"}), rec: replayRec, wantSet: true, wantSends: []sentCall{prObjectSend(101, "HandleCheckFailed", forwardedCheck)}},
		{name: "held replay without sha fails terminally", got: svc.HandleHeldReplay(restate.WithMockContext(badReplayMc), HeldReplayRequest{}), rec: badReplayRec, wantErr: true, wantTerminal: true},
		{name: "held expiry drops a waiting event", got: svc.HandleHeldExpiry(restate.WithMockContext(expireMc), HeldExpiryRequest{ID: 1}), rec: expireRec, wantSet: true},
		{name: "held expiry of a replayed id persists nothing", got: svc.HandleHeldExpiry(restate.WithMockContext(staleExpireMc), HeldExpiryRequest{ID: 9}), rec: staleExpireRec},
		{name: "held expiry load error surfaces", got: svc.HandleHeldExpiry(restate.WithMockContext(errExpireMc), HeldExpiryRequest{ID: 1}), rec: &stateCapture[*RepoState]{}, wantErr: true, wantTerminal: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if (tc.got != nil) != tc.wantErr {
				t.Fatalf("handler err = %v, wantErr %v", tc.got, tc.wantErr)
			}
			if restate.IsTerminalError(tc.got) != tc.wantTerminal {
				t.Fatalf("terminal = %v, want %v (err %v)", restate.IsTerminalError(tc.got), tc.wantTerminal, tc.got)
			}
			if tc.rec.setCalled != tc.wantSet {
				t.Fatalf("setCalled = %v, want %v", tc.rec.setCalled, tc.wantSet)
			}
			if tc.rec.sends != nil {
				assertSends(t, tc.rec.sends.snapshot(), tc.wantSends)
			}
		})
	}
}

// TestRepoObjectService_ResolvePRBySHA proves the shared read-only lookup
// answers from the durable index and never writes.
func TestRepoObjectService_ResolvePRBySHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		seed    repoSeed
		kvErr   bool
		sha     string
		want    ResolveSHAResponse
		wantErr bool
	}{
		{name: "hit resolves pr and branch", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "abc123", "feat-a", 0)}}, sha: "ABC123", want: ResolveSHAResponse{PRNumber: 101, Branch: "feat-a", Found: true}},
		{name: "miss reports not found", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "abc123", "feat-a", 0)}}, sha: "zzz999"},
		{name: "missing state reports not found", sha: "abc123"},
		{name: "kv error surfaces", kvErr: true, sha: "abc123", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			var rec *stateCapture[*RepoState]
			if tc.kvErr {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = repoMock(t, tc.seed)
			}
			got, err := NewRepoObjectService().ResolvePRBySHA(restate.WithMockContext(mc), ResolveSHARequest{Repo: "aerial", SHA: tc.sha})
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolvePRBySHA err = %v, wantErr %v", err, tc.wantErr)
			}
			assert.Equal(t, tc.want, got)
			if rec != nil && rec.setCalled {
				t.Fatal("shared resolve persisted state")
			}
		})
	}
}
