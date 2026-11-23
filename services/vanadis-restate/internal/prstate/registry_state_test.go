package prstate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/prwire"
)

const (
	snowA = "123456789012345678"
	snowB = "223456789012345678"
	snowC = "323456789012345678"
)

// rowState builds a registry-row fixture: repo#7 with head, merge, status.
func rowState(head, merge, status string) *PRState {
	s := NewPRState("azylman/aerial", 7)
	s.HeadSHA, s.MergeSHA, s.Status = head, merge, status
	return s
}

func meta(kind string, runNumber int64, runAttempt int, runID int64, source string, version int) *BuildMetadata {
	return &BuildMetadata{Version: version, Kind: kind, Repository: "azylman/aerial", WorkflowPath: "wf.yml", RunID: runID, RunNumber: runNumber, RunAttempt: runAttempt, Event: "push", SourceSHA: source, ExecutionSHA: source, Selected: []BuildSelection{{Service: "eri", ImageName: "aerial-eri"}}}
}

func TestIsValidDiscordSnowflake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{name: "18 digits", id: snowA, want: true},
		{name: "padded 17 digits", id: " 12345678901234567 ", want: true},
		{name: "16 digits", id: "1234567890123456", want: false},
		{name: "21 digits", id: "123456789012345678901", want: false},
		{name: "headless key", id: "headless:abc", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, IsValidDiscordSnowflake(tc.id))
		})
	}
}

func TestPRState_ResolveRegister(t *testing.T) {
	t.Parallel()
	owned := func(agent, target string) *PRState {
		s := NewPRState("azylman/aerial", 7)
		s.Agent, s.TargetID = agent, target
		return s
	}
	base := RegisterRequest{Repo: "azylman/aerial", PRNumber: 7, Branch: "feat", HeadSHA: "h1"}
	withTarget := func(r RegisterRequest, target, fallback string) RegisterRequest {
		r.TargetID, r.FallbackTargetID = target, fallback
		return r
	}
	withOwner := func(r RegisterRequest, expected string) RegisterRequest {
		r.EnforceOwner, r.ExpectedOwner = true, expected
		return r
	}
	tests := []struct {
		name        string
		state       *PRState
		req         RegisterRequest
		wantVerdict RegisterVerdict
		wantTarget  string
	}{
		{name: "request snowflake wins", state: owned("", snowB), req: withTarget(base, snowA, snowC), wantVerdict: RegisterOK, wantTarget: snowA},
		{name: "headless request falls back to stored target", state: owned("", snowB), req: withTarget(base, "headless:x", snowC), wantVerdict: RegisterOK, wantTarget: snowB},
		{name: "no stored target falls back to system channel", state: owned("", ""), req: withTarget(base, "", snowC), wantVerdict: RegisterOK, wantTarget: snowC},
		{name: "no valid target is rejected", state: owned("", ""), req: withTarget(base, "", ""), wantVerdict: RegisterBadTarget},
		{name: "enforced owner matches stored owner", state: owned("aerial", ""), req: withOwner(withTarget(base, snowA, ""), "aerial"), wantVerdict: RegisterOK, wantTarget: snowA},
		{name: "enforced owner mismatch rejected", state: owned("agent-alpha", ""), req: withOwner(withTarget(base, snowA, ""), "aerial"), wantVerdict: RegisterOwnerMismatch},
		{name: "enforced owner with no stored owner passes", state: owned("", ""), req: withOwner(withTarget(base, snowA, ""), ""), wantVerdict: RegisterOK, wantTarget: snowA},
		{name: "unenforced owner mismatch passes", state: owned("agent-alpha", ""), req: withTarget(base, snowA, ""), wantVerdict: RegisterOK, wantTarget: snowA},
		{name: "missing head is invalid", state: owned("", ""), req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 7, Branch: "feat", TargetID: snowA}, wantVerdict: RegisterInvalid},
		{name: "missing branch is invalid", state: owned("", ""), req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "h1", TargetID: snowA}, wantVerdict: RegisterInvalid},
		{name: "nil state is invalid", req: withTarget(base, snowA, ""), wantVerdict: RegisterInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			verdict, target := tc.state.ResolveRegister(tc.req)
			require.Equal(t, tc.wantVerdict, verdict)
			require.Equal(t, tc.wantTarget, target)
		})
	}
}

func TestPRState_ApplyRegister(t *testing.T) {
	t.Parallel()
	seeded := func(head, status string) *PRState {
		s := rowState(head, "", status)
		s.Title, s.Banner = "Old title", "boom"
		s.CIChecks = map[string]string{"ci": "failed"}
		s.BuildMetadata = meta("final", 1, 1, 10, head, 1)
		return s
	}
	tests := []struct {
		name       string
		state      *PRState
		req        RegisterRequest
		wantStatus string
		wantHead   string
		wantTitle  string
		wantBanner string
		wantChecks map[string]string
		wantMeta   bool
		wantAgent  string
	}{
		{name: "new head clears head-scoped metadata and reopens", state: seeded("h0", RegistryStatusCIFailed), req: RegisterRequest{Branch: "feat", HeadSHA: "h1", Title: "New", Agent: "aerial", Metadata: map[string]string{"vanadis_chunk_id": "chunk-1", "widget_state": "w", "other": "x"}}, wantStatus: RegistryStatusOpen, wantHead: "h1", wantTitle: "New", wantAgent: "aerial"},
		{name: "same head clears only banner", state: seeded("h0", RegistryStatusConflict), req: RegisterRequest{Branch: "feat", HeadSHA: "h0", BotID: "agent-alpha"}, wantStatus: RegistryStatusOpen, wantHead: "h0", wantTitle: "Old title", wantChecks: map[string]string{"ci": "failed"}, wantMeta: true, wantAgent: "agent-alpha"},
		{name: "post-merge status and metadata are kept", state: seeded("h0", RegistryStatusDeployed), req: RegisterRequest{Branch: "feat", HeadSHA: "h1"}, wantStatus: RegistryStatusDeployed, wantHead: "h1", wantTitle: "Old title", wantBanner: "boom", wantChecks: map[string]string{"ci": "failed"}, wantMeta: true},
		{name: "closed status reopens", state: seeded("h0", RegistryStatusClosed), req: RegisterRequest{Branch: "feat", HeadSHA: "h0"}, wantStatus: RegistryStatusOpen, wantHead: "h0", wantTitle: "Old title", wantChecks: map[string]string{"ci": "failed"}, wantMeta: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := tc.state
			s.ApplyRegister(tc.req, snowA)
			require.True(t, s.Registered)
			require.Equal(t, snowA, s.TargetID)
			require.Equal(t, "feat", s.Branch)
			require.Equal(t, tc.wantStatus, s.Status)
			require.Equal(t, tc.wantHead, s.HeadSHA)
			require.Equal(t, tc.wantTitle, s.Title)
			require.Equal(t, tc.wantBanner, s.Banner)
			require.Equal(t, tc.wantChecks, s.CIChecks)
			require.Equal(t, tc.wantMeta, s.BuildMetadata != nil)
			require.Equal(t, tc.wantAgent, s.Agent)
		})
	}
}

func TestPRState_ApplyRegistrySync(t *testing.T) {
	t.Parallel()
	bannered := func(status string) *PRState {
		s := rowState("h1", "", status)
		s.Banner = "b"
		s.CIChecks = map[string]string{"ci": "passed"}
		return s
	}
	tests := []struct {
		name       string
		state      *PRState
		prev, head string
		want       bool
		wantStatus string
		wantBanner string
		wantChecks map[string]string
	}{
		{name: "head change reopens and clears checks", state: bannered(RegistryStatusCIFailed), prev: "h0", head: "h1", want: true, wantStatus: RegistryStatusOpen, wantChecks: nil},
		{name: "same head conflict reopens keeping checks", state: bannered(RegistryStatusConflict), prev: "h1", head: "h1", want: true, wantStatus: RegistryStatusOpen, wantChecks: map[string]string{"ci": "passed"}},
		{name: "same head open with banner clears banner", state: bannered(RegistryStatusOpen), prev: "h1", head: "h1", want: true, wantStatus: RegistryStatusOpen, wantChecks: map[string]string{"ci": "passed"}},
		{name: "same head clean open is a no-op", state: rowState("h1", "", RegistryStatusOpen), prev: "h1", head: "h1", want: false, wantStatus: RegistryStatusOpen},
		{name: "locked status never syncs", state: bannered(RegistryStatusClosed), prev: "h0", head: "h1", want: false, wantStatus: RegistryStatusClosed, wantBanner: "b", wantChecks: map[string]string{"ci": "passed"}},
		{name: "known merge sha never syncs", state: rowState("h1", "m1", RegistryStatusOpen), prev: "h0", head: "h1", want: false, wantStatus: RegistryStatusOpen},
		{name: "zero head is ignored", state: bannered(RegistryStatusOpen), prev: "h0", head: "0000", want: false, wantStatus: RegistryStatusOpen, wantBanner: "b", wantChecks: map[string]string{"ci": "passed"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.state.ApplyRegistrySync(tc.prev, tc.head))
			require.Equal(t, tc.wantStatus, tc.state.Status)
			require.Equal(t, tc.wantBanner, tc.state.Banner)
			require.Equal(t, tc.wantChecks, tc.state.CIChecks)
		})
	}
}

func TestPRState_ApplyRegistryMerged(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		mergeSHA   string
		wantStatus string
		wantMerge  string
		wantBanner string
	}{
		{name: "open merges and records sha", status: RegistryStatusOpen, mergeSHA: "m1", wantStatus: RegistryStatusMerged, wantMerge: "m1"},
		{name: "deploying keeps status", status: RegistryStatusDeploying, mergeSHA: "m1", wantStatus: RegistryStatusDeploying, wantMerge: "m1"},
		{name: "deploy failed keeps banner", status: RegistryStatusDeployFailed, mergeSHA: "m1", wantStatus: RegistryStatusDeployFailed, wantMerge: "m1", wantBanner: "b"},
		{name: "empty merge sha keeps stored", status: RegistryStatusCIFailed, wantStatus: RegistryStatusMerged, wantMerge: "m0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "", tc.status)
			if tc.mergeSHA == "" {
				s.MergeSHA = "m0"
			}
			s.Banner = "b"
			s.ApplyRegistryMerged(tc.mergeSHA)
			require.Equal(t, tc.wantStatus, s.Status)
			require.Equal(t, tc.wantMerge, s.MergeSHA)
			require.Equal(t, tc.wantBanner, s.Banner)
		})
	}
}

func TestPRState_ApplyRegistryClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		status string
	}{
		{name: "open closes", status: RegistryStatusOpen},
		{name: "ci failed closes", status: RegistryStatusCIFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "", tc.status)
			s.Banner = "b"
			s.ApplyRegistryClosed()
			require.Equal(t, RegistryStatusClosed, s.Status)
			require.Empty(t, s.Banner)
		})
	}
}

func TestPRState_ApplyRegistryCIFailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		state      *PRState
		want       bool
		wantStatus string
	}{
		{name: "open fails", state: rowState("h1", "", ""), want: true, wantStatus: RegistryStatusCIFailed},
		{name: "conflict fails", state: rowState("h1", "", RegistryStatusConflict), want: true, wantStatus: RegistryStatusCIFailed},
		{name: "already failed is a no-op", state: rowState("h1", "", RegistryStatusCIFailed), want: false, wantStatus: RegistryStatusCIFailed},
		{name: "merged sha blocks", state: rowState("h1", "m1", RegistryStatusOpen), want: false, wantStatus: RegistryStatusOpen},
		{name: "deployed is locked", state: rowState("h1", "", RegistryStatusDeployed), want: false, wantStatus: RegistryStatusDeployed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.state.ApplyRegistryCIFailed())
			require.Equal(t, tc.wantStatus, tc.state.Status)
		})
	}
}

func TestPRState_ApplyRegistryConflict(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		state      *PRState
		want       bool
		wantStatus string
	}{
		{name: "open conflicts", state: rowState("h1", "", RegistryStatusOpen), want: true, wantStatus: RegistryStatusConflict},
		{name: "ci failed conflicts", state: rowState("h1", "", RegistryStatusCIFailed), want: true, wantStatus: RegistryStatusConflict},
		{name: "already conflict is a no-op", state: rowState("h1", "", RegistryStatusConflict), want: false, wantStatus: RegistryStatusConflict},
		{name: "merged sha blocks", state: rowState("h1", "m1", RegistryStatusOpen), want: false, wantStatus: RegistryStatusOpen},
		{name: "closed is locked", state: rowState("h1", "", RegistryStatusClosed), want: false, wantStatus: RegistryStatusClosed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.state.ApplyRegistryConflict())
			require.Equal(t, tc.wantStatus, tc.state.Status)
		})
	}
}

func TestPRState_ApplyRegistryRecovered(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		from       string
		want       bool
		wantStatus string
	}{
		{name: "ci failed recovers", status: RegistryStatusCIFailed, from: RegistryStatusCIFailed, want: true, wantStatus: RegistryStatusOpen},
		{name: "conflict recovers", status: RegistryStatusConflict, from: RegistryStatusConflict, want: true, wantStatus: RegistryStatusOpen},
		{name: "other status untouched", status: RegistryStatusConflict, from: RegistryStatusCIFailed, want: false, wantStatus: RegistryStatusConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "", tc.status)
			require.Equal(t, tc.want, s.ApplyRegistryRecovered(tc.from))
			require.Equal(t, tc.wantStatus, s.Status)
		})
	}
}

func TestPRState_RecordCheckState(t *testing.T) {
	t.Parallel()
	checked := func(head, value string) *PRState {
		s := rowState(head, "", "")
		s.CIChecks = map[string]string{"ci": value}
		return s
	}
	tests := []struct {
		name       string
		state      *PRState
		sha, check string
		state2     string
		want       bool
		wantChecks map[string]string
	}{
		{name: "open head records", state: rowState("h1", "", ""), sha: "h1", check: "ci", state2: prwire.CheckStateFailed, want: true, wantChecks: map[string]string{"ci": "failed"}},
		{name: "merged merge sha records", state: rowState("h1", "m1", RegistryStatusMerged), sha: "m1", check: "ci", state2: prwire.CheckStatePassed, want: true, wantChecks: map[string]string{"ci": "passed"}},
		{name: "merged head sha is ignored", state: rowState("h1", "m1", RegistryStatusMerged), sha: "h1", check: "ci", state2: prwire.CheckStatePassed, want: false},
		{name: "stale head is ignored", state: rowState("h1", "", RegistryStatusOpen), sha: "h0", check: "ci", state2: prwire.CheckStatePassed, want: false},
		{name: "deploying is ignored", state: rowState("h1", "m1", RegistryStatusDeploying), sha: "m1", check: "ci", state2: prwire.CheckStatePassed, want: false},
		{name: "blank check is ignored", state: rowState("h1", "", ""), sha: "h1", check: " ", state2: prwire.CheckStatePassed, want: false},
		{name: "same value is a no-op", state: checked("h1", "failed"), sha: "h1", check: "ci", state2: prwire.CheckStateFailed, want: false, wantChecks: map[string]string{"ci": "failed"}},
		{name: "new value replaces", state: checked("h1", "failed"), sha: "h1", check: "ci", state2: prwire.CheckStatePending, want: true, wantChecks: map[string]string{"ci": "pending"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.state.RecordCheckState(tc.sha, tc.check, tc.state2))
			require.Equal(t, tc.wantChecks, tc.state.CIChecks)
		})
	}
}

func TestPRState_RecordExpectedJobs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   string
		jobs     []string
		want     bool
		wantJobs []string
	}{
		{name: "unions minus deployed", status: RegistryStatusMerged, jobs: []string{"haro", " eri ", "coven"}, want: true, wantJobs: []string{"eri", "haro", "synapse"}},
		{name: "only deployed jobs is a no-op", status: RegistryStatusMerged, jobs: []string{"coven"}, want: false, wantJobs: []string{"synapse"}},
		{name: "closed is ignored", status: RegistryStatusClosed, jobs: []string{"eri"}, want: false, wantJobs: []string{"synapse"}},
		{name: "empty jobs is a no-op", status: RegistryStatusMerged, want: false, wantJobs: []string{"synapse"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.Jobs, s.DeployedJobs = []string{"synapse"}, []string{"coven"}
			require.Equal(t, tc.want, s.RecordExpectedJobs(tc.jobs))
			require.Equal(t, tc.wantJobs, s.Jobs)
		})
	}
}

func TestPRState_TransitionDeploying(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      string
		jobs        []string
		want        bool
		wantStatus  string
		wantJobs    []string
		wantStarted []string
		wantBanner  string
	}{
		{name: "merged starts deploying", status: RegistryStatusMerged, jobs: []string{"eri", "coven"}, want: true, wantStatus: RegistryStatusDeploying, wantJobs: []string{"eri", "synapse"}, wantStarted: []string{"coven", "eri", "haro"}},
		{name: "deploy failed restarts", status: RegistryStatusDeployFailed, jobs: []string{"eri"}, want: true, wantStatus: RegistryStatusDeploying, wantJobs: []string{"eri", "synapse"}, wantStarted: []string{"eri", "haro"}},
		{name: "all deployed is a no-op", status: RegistryStatusMerged, jobs: []string{"coven"}, want: false, wantStatus: RegistryStatusMerged, wantJobs: []string{"synapse"}, wantStarted: []string{"haro"}, wantBanner: "b"},
		{name: "conflict is ignored", status: RegistryStatusConflict, jobs: []string{"eri"}, want: false, wantStatus: RegistryStatusConflict, wantJobs: []string{"synapse"}, wantStarted: []string{"haro"}, wantBanner: "b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.Jobs, s.StartedJobs, s.DeployedJobs = []string{"synapse"}, []string{"haro"}, []string{"coven"}
			s.Banner = "b"
			require.Equal(t, tc.want, s.TransitionDeploying(tc.jobs))
			require.Equal(t, tc.wantStatus, s.Status)
			require.Equal(t, tc.wantJobs, s.Jobs)
			require.Equal(t, tc.wantStarted, s.StartedJobs)
			require.Equal(t, tc.wantBanner, s.Banner)
		})
	}
}

func TestPRState_RecordKustomizationRequestSet(t *testing.T) {
	t.Parallel()
	req := KustomizationRequest{Namespace: "flux-system", RequestedAt: "2026-11-20T00:00:00Z"}
	tests := []struct {
		name         string
		requests     map[string]KustomizationRequest
		want         bool
		wantRequests map[string]KustomizationRequest
		wantDeployed []string
		wantJobs     []string
	}{
		{name: "replaces wholesale and clears deployed", requests: map[string]KustomizationRequest{" aerial-core ": req}, want: true, wantRequests: map[string]KustomizationRequest{"aerial-core": req}},
		{name: "blank names only is a no-op", requests: map[string]KustomizationRequest{" ": req}, want: false, wantRequests: map[string]KustomizationRequest{"old": {}}, wantDeployed: []string{"old"}, wantJobs: []string{"eri"}},
		{name: "empty is a no-op", want: false, wantRequests: map[string]KustomizationRequest{"old": {}}, wantDeployed: []string{"old"}, wantJobs: []string{"eri"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", RegistryStatusDeploying)
			s.KustomizationRequests = map[string]KustomizationRequest{"old": {}}
			s.DeployedKustomizations, s.DeployedJobs = []string{"old"}, []string{"eri"}
			require.Equal(t, tc.want, s.RecordKustomizationRequestSet(tc.requests))
			require.Equal(t, tc.wantRequests, s.KustomizationRequests)
			require.Equal(t, tc.wantDeployed, s.DeployedKustomizations)
			require.Equal(t, tc.wantJobs, s.DeployedJobs)
		})
	}
}

func TestPRState_MarkJobsDeployed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       string
		jobs         []string
		started      []string
		want         bool
		wantDeployed []string
	}{
		{name: "eligible jobs union", status: RegistryStatusDeploying, jobs: []string{"eri", "other"}, started: []string{"eri"}, want: true, wantDeployed: []string{"coven", "eri"}},
		{name: "no expectations accept any job", status: RegistryStatusDeployFailed, jobs: []string{"other"}, want: true, wantDeployed: []string{"coven", "other"}},
		{name: "already deployed is a no-op", status: RegistryStatusDeploying, jobs: []string{"coven"}, started: []string{"coven"}, want: false, wantDeployed: []string{"coven"}},
		{name: "merged is ignored", status: RegistryStatusMerged, jobs: []string{"eri"}, started: []string{"eri"}, want: false, wantDeployed: []string{"coven"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.StartedJobs, s.DeployedJobs = tc.started, []string{"coven"}
			require.Equal(t, tc.want, s.MarkJobsDeployed(tc.jobs))
			require.Equal(t, tc.wantDeployed, s.DeployedJobs)
		})
	}
}

func TestPRState_MarkKustomizationsDeployed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       string
		names        []string
		want         bool
		wantStatus   string
		wantDeployed []string
		wantPending  bool
	}{
		{name: "last kustomization flips deployed", status: RegistryStatusDeploying, names: []string{"apps"}, want: true, wantStatus: RegistryStatusDeployed, wantDeployed: []string{"apps", "core"}, wantPending: true},
		{name: "unrequested name is ignored", status: RegistryStatusDeploying, names: []string{"other"}, want: false, wantStatus: RegistryStatusDeploying, wantDeployed: []string{"core"}},
		{name: "deploy failed can still complete", status: RegistryStatusDeployFailed, names: []string{"apps"}, want: true, wantStatus: RegistryStatusDeployed, wantDeployed: []string{"apps", "core"}, wantPending: true},
		{name: "merged is ignored", status: RegistryStatusMerged, names: []string{"apps"}, want: false, wantStatus: RegistryStatusMerged, wantDeployed: []string{"core"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.KustomizationRequests = map[string]KustomizationRequest{"core": {}, "apps": {}}
			s.DeployedKustomizations = []string{"core"}
			require.Equal(t, tc.want, s.MarkKustomizationsDeployed(tc.names))
			require.Equal(t, tc.wantStatus, s.Status)
			require.Equal(t, tc.wantDeployed, s.DeployedKustomizations)
			require.Equal(t, tc.wantPending, s.DeploySuccessPending)
		})
	}
}

func TestPRState_RecordJobState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		job, state string
		want       bool
		wantStates map[string]string
	}{
		{name: "deploying expected job records", status: RegistryStatusDeploying, job: "eri", state: "running", want: true, wantStates: map[string]string{"eri": "running"}},
		{name: "unexpected job is ignored", status: RegistryStatusDeploying, job: "haro", state: "running", want: false},
		{name: "deployed is ignored", status: RegistryStatusDeployed, job: "eri", state: "running", want: false},
		{name: "blank state is ignored", status: RegistryStatusDeploying, job: "eri", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.Jobs, s.StartedJobs = []string{"eri"}, []string{"eri"}
			require.Equal(t, tc.want, s.RecordJobState(tc.job, tc.state))
			require.Equal(t, tc.wantStates, s.JobStates)
		})
	}
}

func TestPRState_ApplyRegistryDeployFailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		banner     string
		want       bool
		wantStatus string
		wantBanner string
	}{
		{name: "deploying fails", status: RegistryStatusDeploying, banner: "boom", want: true, wantStatus: RegistryStatusDeployFailed, wantBanner: "boom"},
		{name: "merged fails", status: RegistryStatusMerged, banner: "boom", want: true, wantStatus: RegistryStatusDeployFailed, wantBanner: "boom"},
		{name: "synced fails", status: RegistryStatusSynced, want: true, wantStatus: RegistryStatusDeployFailed, wantBanner: "old"},
		{name: "repeated failure takes the newest banner", status: RegistryStatusDeployFailed, banner: "newest", want: true, wantStatus: RegistryStatusDeployFailed, wantBanner: "newest"},
		{name: "repeated failure without a banner keeps the stored one", status: RegistryStatusDeployFailed, want: true, wantStatus: RegistryStatusDeployFailed, wantBanner: "old"},
		{name: "open is ignored", status: RegistryStatusOpen, banner: "boom", want: false, wantStatus: RegistryStatusOpen, wantBanner: "old"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.Banner = "old"
			require.Equal(t, tc.want, s.ApplyRegistryDeployFailed(tc.banner))
			require.Equal(t, tc.wantStatus, s.Status)
			require.Equal(t, tc.wantBanner, s.Banner)
		})
	}
}

func TestPRState_AckDeploySuccess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		status string
	}{
		{name: "deployed pending clears", status: RegistryStatusDeployed},
		{name: "deploying flips deployed", status: RegistryStatusDeploying},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "m1", tc.status)
			s.DeploySuccessPending = true
			s.AckDeploySuccess()
			require.Equal(t, RegistryStatusDeployed, s.Status)
			require.False(t, s.DeploySuccessPending)
		})
	}
}

func TestPRState_RecordBuildMetadata(t *testing.T) {
	t.Parallel()
	stored := func(status string, m *BuildMetadata) *PRState {
		s := rowState("h1", "m1", status)
		s.BuildMetadata = m
		return s
	}
	otherRepo := meta("progress", 5, 1, 50, "h1", 1)
	otherRepo.Repository = "azylman/mirrormere"
	otherPR := meta("progress", 5, 1, 50, "h1", 1)
	otherPR.PRNumber = new(8)
	mismatchedSameRun := meta("final", 5, 1, 50, "h1", 1)
	mismatchedSameRun.Event = "workflow_dispatch"
	tests := []struct {
		name    string
		state   *PRState
		in      *BuildMetadata
		want    bool
		wantRun int64
	}{
		{name: "first snapshot on open head records", state: stored(RegistryStatusOpen, nil), in: meta("progress", 5, 1, 50, "h1", 1), want: true, wantRun: 5},
		{name: "merged records on merge sha", state: stored(RegistryStatusMerged, nil), in: meta("final", 5, 1, 50, "m1", 1), want: true, wantRun: 5},
		{name: "open rejects merge sha", state: stored(RegistryStatusOpen, nil), in: meta("final", 5, 1, 50, "m1", 1), want: false},
		{name: "deployed rejects any revision", state: stored(RegistryStatusDeployed, nil), in: meta("final", 5, 1, 50, "m1", 1), want: false},
		{name: "newer run replaces", state: stored(RegistryStatusOpen, meta("final", 4, 1, 40, "h1", 1)), in: meta("progress", 5, 1, 50, "h1", 1), want: true, wantRun: 5},
		{name: "older run is rejected", state: stored(RegistryStatusOpen, meta("progress", 6, 1, 60, "h1", 1)), in: meta("progress", 5, 1, 50, "h1", 1), want: false, wantRun: 6},
		{name: "same attempt final upgrades progress", state: stored(RegistryStatusOpen, meta("progress", 5, 1, 50, "h1", 1)), in: meta("final", 5, 1, 50, "h1", 1), want: true, wantRun: 5},
		{name: "same attempt progress cannot downgrade final", state: stored(RegistryStatusOpen, meta("final", 5, 1, 50, "h1", 1)), in: meta("progress", 5, 1, 50, "h1", 1), want: false, wantRun: 5},
		{name: "same run with mismatched identity is rejected", state: stored(RegistryStatusOpen, meta("progress", 5, 1, 50, "h1", 1)), in: mismatchedSameRun, want: false, wantRun: 5},
		{name: "result kind never records", state: stored(RegistryStatusOpen, nil), in: meta("result", 5, 1, 50, "h1", 1), want: false},
		{name: "other repo is rejected", state: stored(RegistryStatusOpen, nil), in: otherRepo, want: false},
		{name: "other pr is rejected", state: stored(RegistryStatusOpen, nil), in: otherPR, want: false},
		{name: "nil snapshot is ignored", state: stored(RegistryStatusOpen, nil), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.state.RecordBuildMetadata(tc.in))
			var gotRun int64
			if tc.state.BuildMetadata != nil {
				gotRun = tc.state.BuildMetadata.RunNumber
			}
			require.Equal(t, tc.wantRun, gotRun)
		})
	}
}

func TestPRState_BuildLifecycleView(t *testing.T) {
	t.Parallel()
	withMeta := func(status, source string, started []string) *PRState {
		s := rowState("h1", "m1", status)
		s.StartedJobs = started
		s.BuildMetadata = meta("final", 5, 1, 50, source, 1)
		return s
	}
	ownedState := func(agent string) *PRState {
		s := rowState("h1", "", "")
		s.Agent = agent
		return s
	}
	tests := []struct {
		name     string
		state    *PRState
		want     BuildLifecycle
		wantMeta bool
	}{
		{name: "open head metadata is visible", state: withMeta(RegistryStatusOpen, "h1", nil), want: BuildLifecycle{Found: true, Status: RegistryStatusOpen, HeadSHA: "h1", MergeSHA: "m1", HasBuildMetadata: true}, wantMeta: true},
		{name: "merged stale head metadata is hidden", state: withMeta(RegistryStatusMerged, "h1", []string{"eri"}), want: BuildLifecycle{Found: true, Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1", Started: true}},
		{name: "deploying hides metadata", state: withMeta(RegistryStatusDeploying, "m1", []string{"eri"}), want: BuildLifecycle{Found: true, Status: RegistryStatusDeploying, HeadSHA: "h1", MergeSHA: "m1", Started: true}},
		{name: "blank status reads open", state: rowState("h1", "", ""), want: BuildLifecycle{Found: true, Status: RegistryStatusOpen, HeadSHA: "h1"}},
		{name: "stored owner is exposed trimmed", state: ownedState(" bot-b "), want: BuildLifecycle{Found: true, Status: RegistryStatusOpen, HeadSHA: "h1", Agent: "bot-b"}},
		{name: "nil state is zero", want: BuildLifecycle{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.state.BuildLifecycleView()
			require.Equal(t, tc.wantMeta, got.BuildMetadata != nil)
			got.BuildMetadata = nil
			require.Equal(t, tc.want, got)
		})
	}
}

func TestPRState_ApplyIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		storedTitle   string
		branch, title string
		wantBranch    string
		wantTitle     string
	}{
		{name: "fills empty title and replaces branch", branch: " feat-b ", title: "T", wantBranch: "feat-b", wantTitle: "T"},
		{name: "keeps stored title and branch on blanks", storedTitle: "Old", wantBranch: "feat-a", wantTitle: "Old"},
		{name: "never overwrites stored title", storedTitle: "Old", title: "New", wantBranch: "feat-a", wantTitle: "Old"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState("h1", "", "")
			s.Branch, s.Title = "feat-a", tc.storedTitle
			s.ApplyIdentity(tc.branch, tc.title)
			require.Equal(t, tc.wantBranch, s.Branch)
			require.Equal(t, tc.wantTitle, s.Title)
		})
	}
}

func TestPRState_ApplyDeployEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		terminal TerminalState
		merge    string
		prior    []string
		ev       PREvent
		want     EventDisposition
	}{
		{name: "nil state drops", nilState: true, ev: PREvent{HeadSHA: "h1"}, want: DispositionDropStale},
		{name: "merged applies head sha deploy", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h1"}, want: DispositionApply},
		{name: "merged applies merge sha deploy", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "m1", MergeSHA: "m1"}, want: DispositionApply},
		{name: "merged applies merge commit as head without merge sha", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "m1"}, want: DispositionApply},
		{name: "merged applies shaless status", terminal: TerminalMerged, merge: "m1", ev: PREvent{}, want: DispositionApply},
		{name: "merged applies zero head without merge sha", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "0000000000000000000000000000000000000000"}, want: DispositionApply},
		{name: "merged same merge sha with stale head applies", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, want: DispositionApply},
		{name: "merged same merge sha with prior head applies", terminal: TerminalMerged, merge: "m1", prior: []string{"h0"}, ev: PREvent{HeadSHA: "h0", MergeSHA: "m1"}, want: DispositionApply},
		{name: "merged different merge sha drops stale", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h1", MergeSHA: "m9"}, want: DispositionDropStale},
		{name: "merged drops foreign merge sha alone", terminal: TerminalMerged, merge: "m1", ev: PREvent{MergeSHA: "m9"}, want: DispositionDropStale},
		{name: "merged no merge sha prior head drops stale", terminal: TerminalMerged, merge: "m1", prior: []string{"h0"}, ev: PREvent{HeadSHA: "h0"}, want: DispositionDropStale},
		{name: "merged no merge sha unknown head applies", terminal: TerminalMerged, merge: "m1", prior: []string{"h0"}, ev: PREvent{HeadSHA: "h2"}, want: DispositionApply},
		{name: "merged without merge sha applies any sha", terminal: TerminalMerged, ev: PREvent{MergeSHA: "m9"}, want: DispositionApply},
		{name: "merged without merge sha applies prior head", terminal: TerminalMerged, prior: []string{"h0"}, ev: PREvent{HeadSHA: "h0"}, want: DispositionApply},
		{name: "closed absorbs", terminal: TerminalClosed, ev: PREvent{HeadSHA: "h1"}, want: DispositionAbsorbTerminal},
		{name: "newer epoch falls through", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h2", ReopenEpoch: 1}, want: DispositionDropStale},
		{name: "newer epoch applies current head", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h1", ReopenEpoch: 1}, want: DispositionApply},
		{name: "open falls through to stale guard", ev: PREvent{HeadSHA: "h2"}, want: DispositionDropStale},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = rowState("h1", tc.merge, "")
				s.Terminal, s.PriorHeads = tc.terminal, tc.prior
			}
			require.Equal(t, tc.want, s.ApplyDeployEvent(tc.ev))
		})
	}
}

func TestPRState_RecoverMergedHead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilState  bool
		terminal  TerminalState
		merge     string
		prior     []string
		ev        PREvent
		wantPrev  string
		wantOK    bool
		wantHead  string
		wantPrior []string
	}{
		{name: "nil state", nilState: true, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}},
		{name: "same merge sha adopts real head", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, wantPrev: "h1", wantOK: true, wantHead: "h2", wantPrior: []string{"h1"}},
		{name: "no merge sha adopts unknown head", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h2"}, wantPrev: "h1", wantOK: true, wantHead: "h2", wantPrior: []string{"h1"}},
		{name: "merge-only ref never overwrites", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "m1", MergeSHA: "m1"}, wantHead: "h1"},
		{name: "merge commit without merge sha never overwrites", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "m1"}, wantHead: "h1"},
		{name: "other merge sha never overwrites", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h2", MergeSHA: "m9"}, wantHead: "h1"},
		{name: "prior head never overwrites", terminal: TerminalMerged, merge: "m1", prior: []string{"h0"}, ev: PREvent{HeadSHA: "h0", MergeSHA: "m1"}, wantHead: "h1", wantPrior: []string{"h0"}},
		{name: "same head is no change", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h1", MergeSHA: "m1"}, wantHead: "h1"},
		{name: "empty head is no change", terminal: TerminalMerged, merge: "m1", ev: PREvent{MergeSHA: "m1"}, wantHead: "h1"},
		{name: "unknown merge sha is no change", terminal: TerminalMerged, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, wantHead: "h1"},
		{name: "open pr is no change", merge: "m1", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, wantHead: "h1"},
		{name: "newer epoch is no change", terminal: TerminalMerged, merge: "m1", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1", ReopenEpoch: 1}, wantHead: "h1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = rowState("h1", tc.merge, "")
				s.Terminal, s.PriorHeads = tc.terminal, tc.prior
			}
			prev, ok := s.RecoverMergedHead(tc.ev)
			require.Equal(t, tc.wantPrev, prev)
			require.Equal(t, tc.wantOK, ok)
			if s != nil {
				require.Equal(t, tc.wantHead, s.HeadSHA)
				require.Equal(t, tc.wantPrior, s.PriorHeads)
			}
		})
	}
}

func TestPRState_MergeDisposition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		terminal TerminalState
		ev       PREvent
		want     EventDisposition
	}{
		{name: "nil state drops", nilState: true, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, want: DispositionDropStale},
		{name: "stale head with merge sha applies", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, want: DispositionApply},
		{name: "stored head with merge sha applies", ev: PREvent{HeadSHA: "h1", MergeSHA: "m1"}, want: DispositionApply},
		{name: "stale head without merge sha drops", ev: PREvent{HeadSHA: "h2"}, want: DispositionDropStale},
		{name: "zero merge sha is no merge sha", ev: PREvent{HeadSHA: "h2", MergeSHA: "0000000000000000000000000000000000000000"}, want: DispositionDropStale},
		{name: "merged pr absorbs", terminal: TerminalMerged, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, want: DispositionAbsorbTerminal},
		{name: "closed pr absorbs", terminal: TerminalClosed, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, want: DispositionAbsorbTerminal},
		{name: "closed pr newer epoch applies", terminal: TerminalClosed, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1", ReopenEpoch: 1}, want: DispositionApply},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = rowState("h1", "", "")
				s.Terminal = tc.terminal
			}
			require.Equal(t, tc.want, s.MergeDisposition(tc.ev))
		})
	}
}

func TestPRState_MergeEventHead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilState  bool
		stored    string
		prior     []string
		ev        PREvent
		wantHead  string
		wantStale bool
	}{
		{name: "nil state keeps the event", nilState: true, ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, wantHead: "h2"},
		{name: "new head is a stale-head recovery", stored: "h1", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, wantHead: "h2", wantStale: true},
		{name: "stored head is no recovery", stored: "h1", ev: PREvent{HeadSHA: "h1", MergeSHA: "m1"}, wantHead: "h1"},
		{name: "merge-only ref is no recovery", stored: "h1", ev: PREvent{HeadSHA: "m1", MergeSHA: "m1"}, wantHead: "m1"},
		{name: "empty head is no recovery", stored: "h1", ev: PREvent{MergeSHA: "m1"}},
		{name: "unknown stored head is no recovery", ev: PREvent{HeadSHA: "h2", MergeSHA: "m1"}, wantHead: "h2"},
		{name: "prior head is blanked", stored: "h1", prior: []string{"h0"}, ev: PREvent{HeadSHA: "h0", MergeSHA: "m1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = rowState(tc.stored, "", "")
				s.PriorHeads = tc.prior
			}
			ev, stale := s.MergeEventHead(tc.ev)
			require.Equal(t, tc.wantHead, ev.HeadSHA)
			require.Equal(t, tc.wantStale, stale)
			require.Equal(t, tc.ev.MergeSHA, ev.MergeSHA)
		})
	}
}

func TestPRState_SynchronizeDisposition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		stored   string
		prior    []string
		terminal TerminalState
		ev       PREvent
		want     EventDisposition
	}{
		{name: "before matches stored head", stored: "h1", ev: PREvent{HeadSHA: "h2", BeforeSHA: "H1"}, want: DispositionApply},
		{name: "unknown stored head applies", ev: PREvent{HeadSHA: "h2"}, want: DispositionApply},
		{name: "redelivered new head applies", stored: "h2", ev: PREvent{HeadSHA: "h2", BeforeSHA: "h1"}, want: DispositionApply},
		{name: "missed synchronize recovers stale head", stored: "c00b710c", prior: []string{"aaaa1111"}, ev: PREvent{HeadSHA: "701c341c", BeforeSHA: "79e9dfc2"}, want: DispositionRecoverStale},
		{name: "mismatch with empty ring recovers", stored: "h3", ev: PREvent{HeadSHA: "h4", BeforeSHA: "h1"}, want: DispositionRecoverStale},
		{name: "late redelivery of a prior head drops", stored: "h3", prior: []string{"h1", "h2"}, ev: PREvent{HeadSHA: "H2", BeforeSHA: "h1"}, want: DispositionDropStale},
		{name: "missing head drops", stored: "h1", ev: PREvent{BeforeSHA: "h1"}, want: DispositionDropStale},
		{name: "zero head drops", stored: "h1", ev: PREvent{HeadSHA: "0000000000000000000000000000000000000000", BeforeSHA: "h1"}, want: DispositionDropStale},
		{name: "closed absorbs", stored: "h1", terminal: TerminalClosed, ev: PREvent{HeadSHA: "h2", BeforeSHA: "h1"}, want: DispositionAbsorbTerminal},
		{name: "merged absorbs a mismatched head", stored: "h1", terminal: TerminalMerged, ev: PREvent{HeadSHA: "h9", BeforeSHA: "h8"}, want: DispositionAbsorbTerminal},
		{name: "nil state drops", nilState: true, ev: PREvent{HeadSHA: "h2"}, want: DispositionDropStale},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = rowState(tc.stored, "", "")
				s.PriorHeads = tc.prior
				s.Terminal = tc.terminal
			}
			require.Equal(t, tc.want, s.SynchronizeDisposition(tc.ev))
		})
	}
}

func TestPRState_SetHead(t *testing.T) {
	t.Parallel()
	full := []string{"p01", "p02", "p03", "p04", "p05", "p06", "p07", "p08", "p09", "p10", "p11", "p12", "p13", "p14", "p15", "p16", "p17", "p18", "p19", "p20"}
	tests := []struct {
		name      string
		stored    string
		prior     []string
		head      string
		wantHead  string
		wantPrior []string
	}{
		{name: "first head records no prior", head: " h1 ", wantHead: "h1"},
		{name: "advance appends the left head", stored: "h1", head: "h2", wantHead: "h2", wantPrior: []string{"h1"}},
		{name: "same head keeps the ring", stored: "h2", prior: []string{"h1"}, head: "H2", wantHead: "H2", wantPrior: []string{"h1"}},
		{name: "return to a prior head leaves the ring", stored: "h2", prior: []string{"h1"}, head: "h1", wantHead: "h1", wantPrior: []string{"h2"}},
		{name: "full ring drops the oldest", stored: "p21", prior: full, head: "h22", wantHead: "h22", wantPrior: append(append([]string(nil), full[1:]...), "p21")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := rowState(tc.stored, "", "")
			s.PriorHeads = append([]string(nil), tc.prior...)
			s.SetHead(tc.head)
			require.Equal(t, tc.wantHead, s.HeadSHA)
			require.Equal(t, tc.wantPrior, s.PriorHeads)
			require.LessOrEqual(t, len(s.PriorHeads), 20)
		})
	}
}

func TestPRState_HasPriorHead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilState bool
		prior    []string
		head     string
		want     bool
	}{
		{name: "case-insensitive hit", prior: []string{"abc1"}, head: " ABC1 ", want: true},
		{name: "miss", prior: []string{"abc1"}, head: "abc2"},
		{name: "empty head never matches", prior: []string{""}, head: " "},
		{name: "nil state", nilState: true, head: "abc1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *PRState
			if !tc.nilState {
				s = rowState("h", "", "")
				s.PriorHeads = tc.prior
			}
			require.Equal(t, tc.want, s.HasPriorHead(tc.head))
		})
	}
}

func TestPRState_Untracked(t *testing.T) {
	t.Parallel()
	fresh := func() *PRState { return NewPRState("kubernetes-sigs/gateway-api", 5433) }
	with := func(f func(*PRState)) *PRState { s := fresh(); f(s); return s }
	tests := []struct {
		name string
		st   *PRState
		want bool
	}{
		{name: "nil state has nothing to drop"},
		{name: "fresh state is untracked", st: fresh(), want: true},
		{name: "agent hint alone stays untracked", st: with(func(s *PRState) { s.Agent = "aerial" }), want: true},
		{name: "pre-guard deploying residue stays untracked", st: untrackedResidue(), want: true},
		{name: "registered pr is tracked", st: with(func(s *PRState) { s.Registered = true })},
		{name: "opened head is tracked", st: with(func(s *PRState) { s.HeadSHA = "h1" })},
		{name: "shadow-built opened pr without register is tracked", st: with(func(s *PRState) { s.HeadSHA, s.Agent = "h2304", "miorine" })},
		{name: "merge sha is tracked", st: with(func(s *PRState) { s.MergeSHA = "m1" })},
		{name: "target thread is tracked", st: with(func(s *PRState) { s.TargetID = "100000000000000001" })},
		{name: "opened with body bot metadata is tracked", st: with(func(s *PRState) {
			applyEventRow(s, PREvent{Repo: s.Repo, PRNumber: s.PRNumber, HeadSHA: "h1", Agent: "suletta"})
			s.ClaimOwner("suletta")
		})},
		{name: "merged terminal is tracked", st: with(func(s *PRState) { s.Terminal = TerminalMerged })},
		{name: "closed terminal is tracked", st: with(func(s *PRState) { s.Terminal = TerminalClosed })},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.st.Untracked(); got != tc.want {
				t.Fatalf("Untracked() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPRState_HasDeployResidue(t *testing.T) {
	t.Parallel()
	with := func(f func(*PRState)) *PRState { s := NewPRState("azylman/aerial", 101); f(s); return s }
	tests := []struct {
		name string
		st   *PRState
		want bool
	}{
		{name: "nil state has none"},
		{name: "fresh state has none", st: with(func(*PRState) {})},
		{name: "status is residue", st: with(func(s *PRState) { s.Status = RegistryStatusDeploying }), want: true},
		{name: "jobs are residue", st: with(func(s *PRState) { s.Jobs = []string{"eri"} }), want: true},
		{name: "started jobs are residue", st: with(func(s *PRState) { s.StartedJobs = []string{"eri"} }), want: true},
		{name: "requests are residue", st: with(func(s *PRState) { s.KustomizationRequests = map[string]KustomizationRequest{"core": {}} }), want: true},
		{name: "index membership is residue", st: with(func(s *PRState) { s.RolloutIndexed = true }), want: true},
		{name: "armed deploy failure is residue", st: with(func(s *PRState) { s.PendingDeployFailure = &PendingDeployFailure{} }), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.st.HasDeployResidue(); got != tc.want {
				t.Fatalf("HasDeployResidue() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPRState_ApplySelfRegistration pins the EnsurePRRegistered fill-only
// upsert: invalid targets write nothing, head/target/title only fill, the
// branch is replaced, and the owner seeds only when the target was empty.
func TestPRState_ApplySelfRegistration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                       string
		nilState                   bool
		stored                     PRState
		branch, head, target       string
		title, agent               string
		want                       bool
		wantBranch, wantHead       string
		wantTarget, wantTitle      string
		wantAgent                  string
		wantPriorHeads             []string
		wantRegistered, wantUntrkd bool
	}{
		{name: "nil state writes nothing", nilState: true, target: "100000000000000009"},
		{name: "invalid target writes nothing", branch: "dependabot/x", head: "h1", target: "dispatch-uuid", title: "T", wantUntrkd: true},
		{
			name: "unregistered dependabot fills everything without an owner", branch: " dependabot/x ", head: "h1", target: "100000000000000009", title: " T ", want: true,
			wantBranch: "dependabot/x", wantHead: "h1", wantTarget: "100000000000000009", wantTitle: "T",
		},
		{
			name: "insert seeds the lowercased body owner", head: "h1", target: "100000000000000002", title: "T", agent: " Miorine ", want: true,
			wantHead: "h1", wantTarget: "100000000000000002", wantTitle: "T", wantAgent: "miorine",
		},
		{
			name: "insert claims over a hinted owner", stored: PRState{Agent: "aerial"}, target: "100000000000000002", agent: "miorine", want: true,
			wantTarget: "100000000000000002", wantAgent: "miorine",
		},
		{
			name: "insert after register only fills an empty owner", stored: PRState{Registered: true, Agent: "aerial"}, target: "100000000000000002", agent: "miorine", want: true,
			wantTarget: "100000000000000002", wantAgent: "aerial", wantRegistered: true,
		},
		{
			name: "stored target wins and keeps the owner", stored: PRState{Branch: "feat", HeadSHA: "h1", TargetID: "100000000000000001", Title: "Old", Agent: "aerial"},
			branch: "feat", head: "h2", target: "100000000000000009", title: "New", agent: "miorine",
			wantBranch: "feat", wantHead: "h1", wantTarget: "100000000000000001", wantTitle: "Old", wantAgent: "aerial",
		},
		{
			name: "tracked PR fills a missing title and replaces the branch", stored: PRState{Branch: "old", HeadSHA: "h1", TargetID: "100000000000000001"},
			branch: "new", head: "h1", target: "100000000000000009", title: "T", want: true,
			wantBranch: "new", wantHead: "h1", wantTarget: "100000000000000001", wantTitle: "T",
		},
		{name: "zero head does not fill", head: "0000000000000000000000000000000000000000", target: "100000000000000009", want: true, wantTarget: "100000000000000009"},
		{
			name: "empty head fill keeps no prior head", stored: PRState{TargetID: "100000000000000001"}, head: "h3", target: "100000000000000001", want: true,
			wantHead: "h3", wantTarget: "100000000000000001",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilState {
				var s *PRState
				require.False(t, s.ApplySelfRegistration(tc.branch, tc.head, tc.target, tc.title, tc.agent))
				return
			}
			s := tc.stored
			s.Repo, s.PRNumber = "azylman/aerial", 2329
			require.Equal(t, tc.want, s.ApplySelfRegistration(tc.branch, tc.head, tc.target, tc.title, tc.agent))
			require.Equal(t, tc.wantBranch, s.Branch)
			require.Equal(t, tc.wantHead, s.HeadSHA)
			require.Equal(t, tc.wantTarget, s.TargetID)
			require.Equal(t, tc.wantTitle, s.Title)
			require.Equal(t, tc.wantAgent, s.Agent)
			require.Equal(t, tc.wantPriorHeads, s.PriorHeads)
			require.Equal(t, tc.wantRegistered, s.Registered)
			require.Equal(t, tc.wantUntrkd, s.Untracked())
		})
	}
}
