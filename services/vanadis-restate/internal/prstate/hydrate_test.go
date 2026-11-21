package prstate

import (
	"reflect"
	"testing"
)

// hydrateState seeds a PR state for repo#101 with optional owner fields.
func hydrateState(agent string, registered bool) *PRState {
	s := NewPRState("aerial", 101)
	s.Agent, s.Registered = agent, registered
	return s
}

// TestApplyHydrate pins fill-if-empty hydration: every empty field fills
// from the row, held fields are never overwritten, ownership comes from
// metadata.agent then bot_id (never the agent column), Register-claimed
// owners survive, zero SHAs are ignored, and the terminal state follows
// the registry status.
func TestApplyHydrate(t *testing.T) {
	t.Parallel()
	bm := &BuildMetadata{Version: 1, Kind: "final", RunNumber: 3}
	fullRow := RegistryRow{
		Repo: "azylman/aerial", PRNumber: 101, Agent: "aerial", Branch: "feat", HeadSHA: "h1", MergeSHA: "m1", TargetID: snowA, Status: RegistryStatusDeploying, Title: "T",
		Metadata: ProjectionMetadata{
			Agent: "agent-alpha", BotID: "agent-alpha", Banner: "b", BuildMetadata: bm, CIChecks: map[string]string{"ci": "passed"}, CIFailCount: 4,
			DeployFailedPublished: true, DeploySuccessPending: true, DeployedJobs: []string{"coven"}, DeployedKustomizations: []string{"core"},
			JobStates: map[string]string{"eri": "ready"}, Jobs: []string{"eri", "coven", "eri"}, KustomizationRequests: map[string]KustomizationRequest{"core": {Namespace: "flux-system"}},
			StartedJobs: []string{"eri"}, VanadisChunkID: "c1", WidgetState: "w",
		},
	}
	wantFull := NewPRState("aerial", 101)
	wantFull.Agent, wantFull.BotID, wantFull.Branch, wantFull.HeadSHA, wantFull.MergeSHA = "agent-alpha", "agent-alpha", "feat", "h1", "m1"
	wantFull.TargetID, wantFull.Status, wantFull.Title, wantFull.Terminal = snowA, RegistryStatusDeploying, "T", TerminalMerged
	wantFull.Banner, wantFull.BuildMetadata, wantFull.CIChecks, wantFull.CIFailCount = "b", &BuildMetadata{Version: 1, Kind: "final", RunNumber: 3}, map[string]string{"ci": "passed"}, 4
	wantFull.DeployFailedPublished, wantFull.DeploySuccessPending = true, true
	wantFull.DeployedJobs, wantFull.DeployedKustomizations, wantFull.JobStates = []string{"coven"}, []string{"core"}, map[string]string{"eri": "ready"}
	wantFull.Jobs, wantFull.KustomizationRequests, wantFull.StartedJobs = []string{"coven", "eri"}, map[string]KustomizationRequest{"core": {Namespace: "flux-system"}}, []string{"eri"}
	wantFull.VanadisChunkID, wantFull.WidgetState = "c1", "w"

	held := NewPRState("aerial", 101)
	held.Agent, held.Registered, held.Branch, held.HeadSHA, held.Status, held.Jobs, held.CIFailCount = "agent-beta", true, "live", "h9", RegistryStatusOpen, []string{"senses"}, 1
	held.BuildMetadata = &BuildMetadata{RunNumber: 9}
	held.Terminal = TerminalNone
	wantHeld := NewPRState("aerial", 101)
	wantHeld.Agent, wantHeld.Registered, wantHeld.Branch, wantHeld.HeadSHA, wantHeld.Status, wantHeld.Jobs, wantHeld.CIFailCount = "agent-beta", true, "live", "h9", RegistryStatusOpen, []string{"senses"}, 1
	wantHeld.BuildMetadata = &BuildMetadata{RunNumber: 9}
	wantHeld.MergeSHA, wantHeld.BotID, wantHeld.TargetID, wantHeld.Title = "m1", "agent-alpha", snowA, "T"
	wantHeld.Banner, wantHeld.CIChecks, wantHeld.DeployFailedPublished, wantHeld.DeploySuccessPending = "b", map[string]string{"ci": "passed"}, true, true
	wantHeld.DeployedJobs, wantHeld.DeployedKustomizations, wantHeld.JobStates = []string{"coven"}, []string{"core"}, map[string]string{"eri": "ready"}
	wantHeld.KustomizationRequests, wantHeld.StartedJobs = map[string]KustomizationRequest{"core": {Namespace: "flux-system"}}, []string{"eri"}
	wantHeld.VanadisChunkID, wantHeld.WidgetState = "c1", "w"

	hinted := hydrateState("aerial", false)
	wantHinted := hydrateState("agent-beta", false)
	wantHinted.BotID, wantHinted.Status, wantHinted.Terminal, wantHinted.HeadSHA = "agent-beta", RegistryStatusClosed, TerminalClosed, "h1"

	tests := []struct {
		name        string
		state       *PRState
		row         RegistryRow
		wantChanged bool
		want        *PRState
	}{
		{name: "nil state never applies", state: nil, row: fullRow, wantChanged: false, want: nil},
		{name: "row for another pr never applies", state: hydrateState("", false), row: RegistryRow{Repo: "aerial", PRNumber: 102, Branch: "x"}, wantChanged: false, want: hydrateState("", false)},
		{name: "fresh state fills every field", state: NewPRState("aerial", 101), row: fullRow, wantChanged: true, want: wantFull},
		{name: "held fields and registered owner survive", state: held, row: fullRow, wantChanged: true, want: wantHeld},
		{name: "hydrate twice is a no-op", state: wantFull, row: fullRow, wantChanged: false, want: wantFull},
		{name: "bot id owner replaces a hinted owner and closed row closes", state: hinted, row: RegistryRow{Repo: "aerial", PRNumber: 101, Agent: "aerial", HeadSHA: "h1", MergeSHA: "0000000", Status: RegistryStatusClosed, Metadata: ProjectionMetadata{BotID: "agent-beta"}}, wantChanged: true, want: wantHinted},
		{name: "open row keeps no terminal", state: NewPRState("aerial", 101), row: RegistryRow{Repo: "aerial", PRNumber: 101, Status: RegistryStatusOpen}, wantChanged: true, want: &PRState{Repo: "azylman/aerial", PRNumber: 101, Status: RegistryStatusOpen, Checks: FailedCheckState{EverFailed: map[string]bool{}, ActiveFailed: map[string]bool{}}, DispatchedEffects: map[string]bool{}, DeploySHAJob: map[string]bool{}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st *PRState
			if tc.state != nil {
				clone := *tc.state
				st = &clone
			}
			if got := st.ApplyHydrate(tc.row); got != tc.wantChanged {
				t.Fatalf("changed = %v, want %v", got, tc.wantChanged)
			}
			if !reflect.DeepEqual(st, tc.want) {
				t.Fatalf("state =\n%+v\nwant\n%+v", st, tc.want)
			}
		})
	}
}

// TestHydrateDeploying pins which hydrated rows stay deploy candidates.
func TestHydrateDeploying(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   string
		requests map[string]KustomizationRequest
		want     bool
	}{
		{name: "deploying row", status: RegistryStatusDeploying, want: true},
		{name: "deploy failed with requests", status: RegistryStatusDeployFailed, requests: map[string]KustomizationRequest{"core": {}}, want: true},
		{name: "deploy failed without requests", status: RegistryStatusDeployFailed, want: false},
		{name: "merged row", status: RegistryStatusMerged, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &PRState{Status: tc.status, KustomizationRequests: tc.requests}
			if got := s.hydrateDeploying(); got != tc.want {
				t.Fatalf("hydrateDeploying = %v, want %v", got, tc.want)
			}
		})
	}
}
