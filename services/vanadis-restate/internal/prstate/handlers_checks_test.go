package prstate

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// errFakeAlert is the injected Discord send failure for hermetic tests.
var errFakeAlert = errors.New("fake discord alert failure")

// errFakeCheckRoute is the injected BuildRouter failure for hermetic tests.
var errFakeCheckRoute = errors.New("fake check route failure")

// FakeDiscordAlerter is the hermetic journaled Discord send boundary. It
// records every fired EffectKey so tests prove exactly-once-per-key firing
// without touching a live Discord API.
type FakeDiscordAlerter struct {
	Keys []EffectKey
	Err  error
}

// SendDiscordAlert records the keyed send and returns the configured error.
func (f *FakeDiscordAlerter) SendDiscordAlert(ctx context.Context, key EffectKey, _ string) error {
	_ = ctx
	f.Keys = append(f.Keys, key)
	return f.Err
}

// fakeAlerterKeys returns the recorded keys, tolerating a nil fake.
func fakeAlerterKeys(f *FakeDiscordAlerter) []EffectKey {
	if f == nil {
		return nil
	}
	return f.Keys
}

// checkLiteralKey builds the expected journaled key from literal case values
// only, never via the production EffectKeyFor composition under test.
func checkLiteralKey(tc checkEventRow) EffectKey {
	return EffectKey{
		Repo:           "azylman/aerial",
		PRNumber:       tc.event.PRNumber,
		HeadSHA:        strings.TrimSpace(tc.event.HeadSHA),
		GateGeneration: 0,
		Kind:           EffectDiscordAlert,
		CheckName:      tc.event.CheckName,
	}
}

// collabLiteralKey builds the expected journaled key from literal case
// values only, never via the production EffectKeyFor composition under test.
func collabLiteralKey(tc collaboratorAlertRow) EffectKey {
	return EffectKey{
		Repo:           "azylman/aerial",
		PRNumber:       tc.event.PRNumber,
		HeadSHA:        strings.TrimSpace(tc.event.HeadSHA),
		GateGeneration: 0,
		Kind:           EffectDiscordAlert,
		CheckName:      tc.event.CheckName,
	}
}

// checkEventRow is one declarative check-handler case: seeding fields build
// the prestate, want fields assert the outcome. No closures.
type checkEventRow struct {
	name            string
	event           PREvent
	seedHeadSHA     string
	seedTerminal    TerminalState
	seedEpoch       uint64
	seedEffectFired bool
	seedAlertedSHA  string
	seedGranular    bool
	alertErr        bool
	routeErr        bool
	nilAlerter      bool
	wantDisposition EventDisposition
	wantAlertFired  bool
	wantRouted      bool
	wantRouterCalls int
	wantAlertCalls  int
	wantErr         bool
	wantHeadSHA     string
	wantAlertedSHA  string
	wantActiveCheck bool
	wantEverFailed  bool
}

// seedCheckPRState builds the prestate for one check-handler case.
func seedCheckPRState(tc checkEventRow) *PRState {
	s := NewPRState("aerial", tc.event.PRNumber)
	if tc.seedHeadSHA != "" {
		s.HeadSHA = tc.seedHeadSHA
		s.RecordFailedCISHA(tc.seedHeadSHA)
	}
	if tc.seedTerminal != TerminalNone {
		s.MarkTerminal(tc.seedTerminal)
	}
	for range tc.seedEpoch {
		s.Reopen()
	}
	if tc.seedTerminal != TerminalNone && tc.seedEpoch > 0 {
		s.MarkTerminal(tc.seedTerminal)
	}
	if tc.seedAlertedSHA != "" {
		s.MarkCollaboratorAlerted(tc.seedAlertedSHA)
	}
	if tc.seedEffectFired {
		s.MarkEffectDispatched(s.EffectKeyFor(EffectDiscordAlert, tc.event.HeadSHA, "", tc.event.CheckName))
	}
	if tc.seedGranular {
		granularSHA := tc.seedHeadSHA
		if granularSHA == "" {
			granularSHA = strings.TrimSpace(tc.event.HeadSHA)
		}
		s.RecordFailedCheckRunDetailed(granularSHA, tc.event.CheckName)
	}
	return s
}

// checkCheckRow executes one check-handler case and asserts the outcome.
func checkCheckRow(t *testing.T, tc checkEventRow) {
	t.Helper()
	ctx := context.Background()
	s := seedCheckPRState(tc)
	router := &FakeBuildRouterInvoker{}
	if tc.routeErr {
		router.Err = errFakeCheckRoute
	}
	var alerter DiscordAlerter
	var fakeAlerter *FakeDiscordAlerter
	if !tc.nilAlerter {
		fakeAlerter = &FakeDiscordAlerter{}
		if tc.alertErr {
			fakeAlerter.Err = errFakeAlert
		}
		alerter = fakeAlerter
	}
	out, err := HandleCheckEvent(ctx, s, tc.event, router, alerter)
	if tc.wantErr && err == nil {
		t.Fatalf("HandleCheckEvent error = nil, want error")
	}
	if !tc.wantErr && err != nil {
		t.Fatalf("HandleCheckEvent error = %v, want nil", err)
	}
	if out.Disposition != tc.wantDisposition {
		t.Fatalf("Disposition = %q, want %q", out.Disposition, tc.wantDisposition)
	}
	if out.AlertFired != tc.wantAlertFired {
		t.Fatalf("AlertFired = %v, want %v", out.AlertFired, tc.wantAlertFired)
	}
	if out.Routed != tc.wantRouted {
		t.Fatalf("Routed = %v, want %v", out.Routed, tc.wantRouted)
	}
	assertCheckFiring(t, tc, s, router, fakeAlerter)
}

// assertCheckFiring asserts alert counts, key scoping, state, and routing.
func assertCheckFiring(t *testing.T, tc checkEventRow, s *PRState, router *FakeBuildRouterInvoker, fakeAlerter *FakeDiscordAlerter) {
	t.Helper()
	alertCalls := 0
	if fakeAlerter != nil {
		alertCalls = len(fakeAlerter.Keys)
	}
	if alertCalls != tc.wantAlertCalls {
		t.Fatalf("alert calls = %d, want %d", alertCalls, tc.wantAlertCalls)
	}
	if s.HeadSHA != tc.wantHeadSHA {
		t.Fatalf("HeadSHA = %q, want %q", s.HeadSHA, tc.wantHeadSHA)
	}
	if s.CollaboratorAlerted != tc.wantAlertedSHA {
		t.Fatalf("CollaboratorAlerted = %q, want %q", s.CollaboratorAlerted, tc.wantAlertedSHA)
	}
	for _, k := range fakeAlerterKeys(fakeAlerter) {
		if k != checkLiteralKey(tc) {
			t.Fatalf("alert key = %q, want %q", k.String(), checkLiteralKey(tc).String())
		}
	}
	if tc.wantAlertCalls > 0 && !tc.wantErr {
		if !s.WasEffectDispatched(checkLiteralKey(tc)) {
			t.Fatalf("effect key %q not marked dispatched", checkLiteralKey(tc).String())
		}
	}
	if s.HasActiveCheckRun(strings.TrimSpace(tc.event.HeadSHA), tc.event.CheckName) != tc.wantActiveCheck {
		t.Fatalf("HasActiveCheckRun = %v, want %v", !tc.wantActiveCheck, tc.wantActiveCheck)
	}
	if s.HasEverFailedCheckRun(strings.TrimSpace(tc.event.HeadSHA), tc.event.CheckName) != tc.wantEverFailed {
		t.Fatalf("HasEverFailedCheckRun = %v, want %v", !tc.wantEverFailed, tc.wantEverFailed)
	}
	if router.CallCount() != tc.wantRouterCalls {
		t.Fatalf("router calls = %d, want %d", router.CallCount(), tc.wantRouterCalls)
	}
	if tc.wantRouterCalls > 0 {
		got := router.Calls[0].Event
		if got.PRNumber != tc.event.PRNumber {
			t.Fatalf("routed pr = %d, want %d", got.PRNumber, tc.event.PRNumber)
		}
		if !strings.EqualFold(got.HeadSHA, tc.event.HeadSHA) {
			t.Fatalf("routed sha = %q, want %q", got.HeadSHA, tc.event.HeadSHA)
		}
		if got.Repo != "azylman/aerial" {
			t.Fatalf("routed repo = %q, want %q", got.Repo, "azylman/aerial")
		}
	}
}

// checkEventCoreRows covers the apply/routing/alert core: owned alerts,
// unmanaged isolation, stale and terminal guards, and reopen epochs.
func checkEventCoreRows() []checkEventRow {
	return []checkEventRow{
		{
			name:            "owned check failed routes and alerts",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abcdef1234567890", CheckName: "lint", Branch: "feat/x", Title: "Add x", BuildID: "build-1"},
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  1,
			wantHeadSHA:     "abcdef1234567890",
			wantAlertedSHA:  "",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
		{
			name:            "nil alerter skips alert but routes owned",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			nilAlerter:      true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
		{
			name:            "unmanaged check failed alerts without routing",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 102, HeadSHA: "abc123", CheckName: "lint", Branch: "feat/y", Title: "Add y", Unmanaged: true},
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantRouterCalls: 0,
			wantAlertCalls:  1,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "abc123",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
		{
			name:            "unmanaged with build id still never routes",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 103, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-9", Unmanaged: true},
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantRouterCalls: 0,
			wantAlertCalls:  1,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "abc123",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
		{
			name:            "stale sha drops without effects",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "def456", CheckName: "lint", BuildID: "build-1"},
			seedHeadSHA:     "abc123",
			wantDisposition: DispositionDropStale,
			wantAlertFired:  false,
			wantRouterCalls: 0,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
		},
		{
			name:            "terminal merged absorbs same epoch",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			seedHeadSHA:     "abc123",
			seedTerminal:    TerminalMerged,
			wantDisposition: DispositionAbsorbTerminal,
			wantAlertFired:  false,
			wantRouterCalls: 0,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
		},
		{
			name:            "newer reopen epoch applies through terminal",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1", ReopenEpoch: 2},
			seedHeadSHA:     "abc123",
			seedTerminal:    TerminalClosed,
			seedEpoch:       1,
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  1,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
		{
			name:            "duplicate failure does not refire alert",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			seedHeadSHA:     "abc123",
			seedEffectFired: true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
	}
}

// checkEventEdgeRows covers skips, clears, reruns, and failure propagation.
func checkEventEdgeRows() []checkEventRow {
	return []checkEventRow{
		{
			name:            "empty check name skips alert but routes owned",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", BuildID: "build-1"},
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  0,
			wantHeadSHA:     "",
			wantAlertedSHA:  "",
		},
		{
			name:            "gate aggregator skips alert but routes owned",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "ci gate", BuildID: "build-1"},
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  0,
			wantHeadSHA:     "",
			wantAlertedSHA:  "",
		},
		{
			name:            "check passed clears failure and routes owned",
			event:           PREvent{Event: "check_passed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			seedHeadSHA:     "abc123",
			seedGranular:    true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
			wantActiveCheck: false,
			wantEverFailed:  true,
		},
		{
			name:            "unmanaged check passed never routes",
			event:           PREvent{Event: "check_passed", Repo: "aerial", PRNumber: 102, HeadSHA: "abc123", CheckName: "lint", Unmanaged: true},
			seedHeadSHA:     "abc123",
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouterCalls: 0,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
		},
		{
			name:            "rerun request routes owned without alert",
			event:           PREvent{Event: "check_rerun_requested", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			seedHeadSHA:     "abc123",
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouted:      true,
			wantRouterCalls: 1,
			wantAlertCalls:  0,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
		},
		{
			name:            "alert send error rolls back and reports",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			alertErr:        true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouterCalls: 0,
			wantAlertCalls:  1,
			wantErr:         true,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
		},
		{
			name:            "router error propagates after alert",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"},
			routeErr:        true,
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantRouted:      false,
			wantRouterCalls: 1,
			wantAlertCalls:  1,
			wantErr:         true,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "",
			wantActiveCheck: true,
			wantEverFailed:  true,
		},
		{
			name:            "sibling check failure preserves existing mark",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 102, HeadSHA: "abc123", CheckName: "build", Unmanaged: true},
			seedAlertedSHA:  "abc123",
			alertErr:        true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantRouterCalls: 0,
			wantAlertCalls:  1,
			wantErr:         true,
			wantHeadSHA:     "abc123",
			wantAlertedSHA:  "abc123",
		},
	}
}

// TestHandleCheckEvent freezes the PRObject check-run handler: stale-SHA
// drops, same-epoch terminal absorbs, Discord alerts fire exactly once per
// generation-scoped EffectKey, and unmanaged collaborator PRs never reach the
// BuildRouter.
func TestHandleCheckEvent(t *testing.T) {
	t.Parallel()
	tests := append(checkEventCoreRows(), checkEventEdgeRows()...)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checkCheckRow(t, tc)
		})
	}
}

// collaboratorAlertRow is one declarative collaborator-alert case.
type collaboratorAlertRow struct {
	name            string
	event           PREvent
	seedHeadSHA     string
	seedTerminal    TerminalState
	seedAlertedSHA  string
	seedEffectFired bool
	alertErr        bool
	nilAlerter      bool
	wantDisposition EventDisposition
	wantAlertFired  bool
	wantAlertCalls  int
	wantErr         bool
	wantAlertedSHA  string
}

// seedCollaboratorPRState builds the prestate for one collaborator case.
func seedCollaboratorPRState(tc collaboratorAlertRow) *PRState {
	s := NewPRState("aerial", tc.event.PRNumber)
	if tc.seedHeadSHA != "" {
		s.HeadSHA = tc.seedHeadSHA
	}
	if tc.seedTerminal != TerminalNone {
		s.MarkTerminal(tc.seedTerminal)
	}
	if tc.seedAlertedSHA != "" {
		s.MarkCollaboratorAlerted(tc.seedAlertedSHA)
	}
	if tc.seedEffectFired {
		s.MarkEffectDispatched(s.EffectKeyFor(EffectDiscordAlert, tc.event.HeadSHA, "", tc.event.CheckName))
	}
	return s
}

// checkCollaboratorRow executes one collaborator case and asserts the outcome.
func checkCollaboratorRow(t *testing.T, tc collaboratorAlertRow) {
	t.Helper()
	ctx := context.Background()
	s := seedCollaboratorPRState(tc)
	router := &FakeBuildRouterInvoker{}
	var alerter DiscordAlerter
	var fakeAlerter *FakeDiscordAlerter
	if !tc.nilAlerter {
		fakeAlerter = &FakeDiscordAlerter{}
		if tc.alertErr {
			fakeAlerter.Err = errFakeAlert
		}
		alerter = fakeAlerter
	}
	out, err := HandleCollaboratorAlert(ctx, s, tc.event, router, alerter)
	if tc.wantErr && err == nil {
		t.Fatalf("HandleCollaboratorAlert error = nil, want error")
	}
	if !tc.wantErr && err != nil {
		t.Fatalf("HandleCollaboratorAlert error = %v, want nil", err)
	}
	if out.Disposition != tc.wantDisposition {
		t.Fatalf("Disposition = %q, want %q", out.Disposition, tc.wantDisposition)
	}
	if out.AlertFired != tc.wantAlertFired {
		t.Fatalf("AlertFired = %v, want %v", out.AlertFired, tc.wantAlertFired)
	}
	assertCollaboratorFiring(t, tc, s, router, fakeAlerter)
}

// assertCollaboratorFiring asserts zero router calls, alert counts, and marks.
func assertCollaboratorFiring(t *testing.T, tc collaboratorAlertRow, s *PRState, router *FakeBuildRouterInvoker, fakeAlerter *FakeDiscordAlerter) {
	t.Helper()
	if router.CallCount() != 0 {
		t.Fatalf("router calls = %d, want 0 (collaborator path never routes)", router.CallCount())
	}
	alertCalls := 0
	if fakeAlerter != nil {
		alertCalls = len(fakeAlerter.Keys)
	}
	if alertCalls != tc.wantAlertCalls {
		t.Fatalf("alert calls = %d, want %d", alertCalls, tc.wantAlertCalls)
	}
	if !strings.EqualFold(s.CollaboratorAlerted, tc.wantAlertedSHA) {
		t.Fatalf("CollaboratorAlerted = %q, want %q", s.CollaboratorAlerted, tc.wantAlertedSHA)
	}
	for _, k := range fakeAlerterKeys(fakeAlerter) {
		if k != collabLiteralKey(tc) {
			t.Fatalf("alert key = %q, want %q", k.String(), collabLiteralKey(tc).String())
		}
	}
	if tc.wantAlertCalls > 0 && !tc.wantErr {
		if !s.WasEffectDispatched(collabLiteralKey(tc)) {
			t.Fatalf("effect key %q not marked dispatched", collabLiteralKey(tc).String())
		}
	}
}

// TestHandleCollaboratorAlert freezes the unmanaged collaborator path: alerts
// dedup per SHA, stale and terminal events never fire, failures roll the
// alert mark back, and the BuildRouter is never signalled even for
// build-tagged PRs.
func TestHandleCollaboratorAlert(t *testing.T) {
	t.Parallel()
	tests := []collaboratorAlertRow{
		{
			name:            "fresh failure alerts without routing",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "lint", Branch: "feat/z", Title: "Add z", Unmanaged: true},
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantAlertCalls:  1,
			wantAlertedSHA:  "abc123",
		},
		{
			name:            "build tagged collaborator event still never routes",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 202, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1", Unmanaged: true},
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantAlertCalls:  1,
			wantAlertedSHA:  "abc123",
		},
		{
			name:            "duplicate sha does not refire",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "lint", Unmanaged: true},
			seedHeadSHA:     "abc123",
			seedAlertedSHA:  "abc123",
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "abc123",
		},
		{
			name:            "new sha after prior alert fires again",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "def456", CheckName: "lint", Unmanaged: true},
			seedAlertedSHA:  "abc123",
			wantDisposition: DispositionApply,
			wantAlertFired:  true,
			wantAlertCalls:  1,
			wantAlertedSHA:  "def456",
		},
		{
			name:            "stale sha drops without firing",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "def456", CheckName: "lint", Unmanaged: true},
			seedHeadSHA:     "abc123",
			wantDisposition: DispositionDropStale,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "",
		},
		{
			name:            "terminal closed absorbs same epoch",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "lint", Unmanaged: true},
			seedHeadSHA:     "abc123",
			seedTerminal:    TerminalClosed,
			wantDisposition: DispositionAbsorbTerminal,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "",
		},
		{
			name:            "empty head sha never fires",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, CheckName: "lint", Unmanaged: true},
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "",
		},
		{
			name:            "empty check name never fires",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", Unmanaged: true},
			seedHeadSHA:     "abc123",
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "",
		},
		{
			name:            "gate aggregator never fires",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "ci gate", Unmanaged: true},
			seedHeadSHA:     "abc123",
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "",
		},
		{
			name:            "already dispatched key does not refire",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "lint", Unmanaged: true},
			seedHeadSHA:     "abc123",
			seedEffectFired: true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "abc123",
		},
		{
			name:            "alert error clears mark and reports",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "lint", Unmanaged: true},
			alertErr:        true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  1,
			wantErr:         true,
			wantAlertedSHA:  "",
		},
		{
			name:            "nil alerter leaves no mark",
			event:           PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 201, HeadSHA: "abc123", CheckName: "lint", Unmanaged: true},
			nilAlerter:      true,
			wantDisposition: DispositionApply,
			wantAlertFired:  false,
			wantAlertCalls:  0,
			wantAlertedSHA:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checkCollaboratorRow(t, tc)
		})
	}
}
