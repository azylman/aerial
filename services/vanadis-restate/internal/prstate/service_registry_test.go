package prstate

import (
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/require"
)

// registryRowView is the slice of PRState a pr_registry row projects.
type registryRowView struct {
	Status                 string
	HeadSHA                string
	MergeSHA               string
	Branch                 string
	Title                  string
	TargetID               string
	Jobs                   []string
	StartedJobs            []string
	DeployedJobs           []string
	DeployedKustomizations []string
	CIChecks               map[string]string
	Banner                 string
	DeploySuccessPending   bool
	DeployFailedPublished  bool
	BuildRun               int64
}

func viewRow(s *PRState) registryRowView {
	if s == nil {
		return registryRowView{}
	}
	v := registryRowView{
		Status: s.Status, HeadSHA: s.HeadSHA, MergeSHA: s.MergeSHA, Branch: s.Branch, Title: s.Title, TargetID: s.TargetID,
		Jobs: s.Jobs, StartedJobs: s.StartedJobs, DeployedJobs: s.DeployedJobs, DeployedKustomizations: s.DeployedKustomizations,
		CIChecks: s.CIChecks, Banner: s.Banner, DeploySuccessPending: s.DeploySuccessPending, DeployFailedPublished: s.DeployFailedPublished,
	}
	if s.BuildMetadata != nil {
		v.BuildRun = s.BuildMetadata.RunNumber
	}
	return v
}

// svcRow seeds a stored PRObject state for repo#101.
func svcRow(head, merge, status string, terminal TerminalState) *PRState {
	s := NewPRState("aerial", 101)
	s.HeadSHA, s.MergeSHA, s.Status, s.Terminal = head, merge, status, terminal
	return s
}

// TestPRObjectService_RegistryRow pins the registry row each PRObject
// handler records, including the deploy-phase handlers applying to merged
// PRs and closed PRs absorbing them.
func TestPRObjectService_RegistryRow(t *testing.T) {
	t.Parallel()
	svc := NewPRObjectService()
	deployingMerged := func() *PRState {
		s := svcRow("h1", "m1", RegistryStatusDeploying, TerminalMerged)
		s.Jobs, s.StartedJobs = []string{"eri"}, []string{"eri"}
		s.KustomizationRequests = map[string]KustomizationRequest{"core": {}}
		return s
	}
	failedDeploy := svcRow("h1", "m1", RegistryStatusDeployFailed, TerminalMerged)
	pendingDeployed := svcRow("h1", "m1", RegistryStatusDeployed, TerminalMerged)
	pendingDeployed.DeploySuccessPending = true
	pendingDeployed.KustomizationRequests = map[string]KustomizationRequest{"core": {Namespace: "flux-system"}}
	pendingDeployed.DeployedKustomizations = []string{"core"}
	conflicted := svcRow("h1", "", RegistryStatusConflict, TerminalNone)
	conflicted.MarkConflictPublished("h1")
	failedCI := svcRow("h1", "", RegistryStatusCIFailed, TerminalNone)
	failedCI.RecordFailedCheckRunDetailed("h1", "ci")
	failedCI.RecordNonRoboRevFailedSHA("h1")
	failedCI.RecordFailedCISHA("h1")
	synced := svcRow("h1", "", RegistryStatusCIFailed, TerminalNone)
	synced.CIChecks = map[string]string{"ci": "failed"}
	synced.Banner = "b"
	req := KustomizationRequest{Namespace: "flux-system", RequestedAt: "t0"}

	mc01, rec01 := newStateMock[*PRState](t, nil)
	mc02, rec02 := newStateMock(t, failedCI)
	mc03, rec03 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc04, rec04 := newStateMock(t, conflicted)
	mc05, rec05 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc06, rec06 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc07, rec07 := newStateMock(t, svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged))
	stubRun(mc07)
	mc08, rec08 := newStateMock(t, deployingMerged())
	mc09, rec09 := newStateMock(t, deployingMerged())
	mc10, rec10 := newStateMock(t, deployingMerged())
	mc11, rec11 := newStateMock(t, failedDeploy)
	mc12, rec12 := newStateMock(t, pendingDeployed)
	mc13, rec13 := newStateMock(t, svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged))
	mc14, rec14 := newStateMock(t, synced)
	mc15, rec15 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalClosed))
	mc16, rec16 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc17, rec17 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc18, rec18 := newStateMock(t, svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged))
	_, err12 := svc.HandleDeploySuccessPublished(restate.WithMockContext(mc12), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", JobName: "core"})
	_, err16 := svc.HandleConflictPublished(restate.WithMockContext(mc16), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1"})
	tests := []struct {
		name    string
		got     error
		rec     *stateCapture[*PRState]
		wantSet bool
		want    registryRowView
	}{
		{
			name: "check failed records ci_failed and identity", got: svc.HandleCheckFailed(restate.WithMockContext(mc01), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci", Branch: "feat", Title: "T", TargetID: snowA}), rec: rec01, wantSet: true,
			want: registryRowView{Status: RegistryStatusCIFailed, HeadSHA: "h1", Branch: "feat", Title: "T", TargetID: snowA},
		},
		{
			name: "check passed recovers ci_failed", got: svc.HandleCheckPassed(restate.WithMockContext(mc02), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci"}), rec: rec02, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h1"},
		},
		{
			name: "check status records pending", got: svc.HandleCheckStatus(restate.WithMockContext(mc03), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci", CheckState: "pending"}), rec: rec03, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h1", CIChecks: map[string]string{"ci": "pending"}},
		},
		{
			name: "merged check status records on merge sha", got: svc.HandleCheckStatus(restate.WithMockContext(mc18), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "m1", CheckName: "ci", CheckState: "passed"}), rec: rec18, wantSet: true,
			want: registryRowView{Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1", CIChecks: map[string]string{"ci": "passed"}},
		},
		{
			name: "conflict resolved reopens", got: svc.HandleConflictResolved(restate.WithMockContext(mc04), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1"}), rec: rec04, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h1"},
		},
		{
			name: "merged records merge sha", got: svc.HandleMerged(restate.WithMockContext(mc05), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1"}), rec: rec05, wantSet: true,
			want: registryRowView{Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1"},
		},
		{
			name: "closed records closed", got: svc.HandleClosed(restate.WithMockContext(mc06), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1"}), rec: rec06, wantSet: true,
			want: registryRowView{Status: RegistryStatusClosed, HeadSHA: "h1"},
		},
		{
			name: "merged kustomization request starts deploying", got: svc.HandleKustomizationRequested(restate.WithMockContext(mc07), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", Jobs: []string{"eri"}, KustomizationRequests: map[string]KustomizationRequest{"core": req}}), rec: rec07, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeploying, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}},
		},
		{
			name: "merged rollout marks ready jobs", got: svc.HandleRolloutProgress(restate.WithMockContext(mc08), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", DeployedJobs: []string{"eri"}}), rec: rec08, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeploying, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}, DeployedJobs: []string{"eri"}},
		},
		{
			name: "merged rollout completes kustomizations", got: svc.HandleRolloutProgress(restate.WithMockContext(mc09), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", DeployedKustomizations: []string{"core"}}), rec: rec09, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployed, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}, DeployedKustomizations: []string{"core"}, DeploySuccessPending: true},
		},
		{
			name: "merged deploy failure records banner", got: svc.HandleDeployFailed(restate.WithMockContext(mc10), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", Banner: "boom"}), rec: rec10, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployFailed, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}, Banner: "boom"},
		},
		{
			name: "deploy failure ack records published", got: svc.HandleDeployFailed(restate.WithMockContext(mc11), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", Acknowledged: true}), rec: rec11, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployFailed, HeadSHA: "h1", MergeSHA: "m1", DeployFailedPublished: true},
		},
		{
			name: "deploy success ack clears pending", got: err12, rec: rec12, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployed, HeadSHA: "h1", MergeSHA: "m1", DeployedKustomizations: []string{"core"}},
		},
		{
			name: "merged build metadata records on merge sha", got: svc.HandleBuildMetadata(restate.WithMockContext(mc13), PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", BuildMetadata: meta("final", 9, 1, 90, "m1", 1)}), rec: rec13, wantSet: true,
			want: registryRowView{Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1", BuildRun: 9},
		},
		{
			name: "synchronize from stored head syncs the row", got: svc.HandleSynchronized(restate.WithMockContext(mc14), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h2", BeforeSHA: "h1", Branch: "feat"}), rec: rec14, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h2", Branch: "feat"},
		},
		{name: "late synchronize drops stale", got: svc.HandleSynchronized(restate.WithMockContext(mc17), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h2", BeforeSHA: "h0"}), rec: rec17, wantSet: false},
		{name: "closed absorbs deploy failure", got: svc.HandleDeployFailed(restate.WithMockContext(mc15), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", Banner: "boom"}), rec: rec15, wantSet: false},
		{
			name: "conflict publish records conflict", got: err16, rec: rec16, wantSet: true,
			want: registryRowView{Status: RegistryStatusConflict, HeadSHA: "h1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, tc.got)
			require.Equal(t, tc.wantSet, tc.rec.setCalled)
			if tc.wantSet {
				require.Equal(t, tc.want, viewRow(tc.rec.saved))
			}
		})
	}
}

// TestPRObjectService_Register pins the register handler: owner fence,
// headless target resolution, terminal error codes, and the persisted row.
func TestPRObjectService_Register(t *testing.T) {
	t.Parallel()
	owned := svcRow("h0", "", RegistryStatusCIFailed, TerminalNone)
	owned.Agent, owned.TargetID = "agent-alpha", snowB
	tests := []struct {
		name       string
		existing   *PRState
		failGet    bool
		req        RegisterRequest
		wantCode   int
		wantErr    bool
		wantResp   RegisterResponse
		wantSet    bool
		wantStatus string
		wantSends  int
	}{
		{name: "new pr registers and indexes", req: RegisterRequest{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: snowA, Agent: "aerial"}, wantResp: RegisterResponse{TargetID: snowA, Agent: "aerial", Status: RegistryStatusOpen}, wantSet: true, wantStatus: RegistryStatusOpen, wantSends: 1},
		{name: "headless target resolves stored target", existing: owned, req: RegisterRequest{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: "headless:x", EnforceOwner: true, ExpectedOwner: "agent-alpha"}, wantResp: RegisterResponse{TargetID: snowB, Agent: "agent-alpha", Status: RegistryStatusOpen}, wantSet: true, wantStatus: RegistryStatusOpen, wantSends: 1},
		{name: "owner mismatch is a terminal 409", existing: owned, req: RegisterRequest{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: snowA, EnforceOwner: true, ExpectedOwner: "aerial"}, wantErr: true, wantCode: 409},
		{name: "no target is a terminal 400", req: RegisterRequest{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1"}, wantErr: true, wantCode: 400},
		{name: "missing branch is a terminal 400", req: RegisterRequest{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", TargetID: snowA}, wantErr: true, wantCode: 400},
		{name: "kv failure surfaces", failGet: true, req: RegisterRequest{Repo: "aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: snowA}, wantErr: true, wantCode: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{sends: &sendCapture{}}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.existing)
			}
			got, err := NewPRObjectService().Register(restate.WithMockContext(mc), tc.req)
			require.Equal(t, tc.wantErr, err != nil)
			if tc.wantErr {
				require.True(t, restate.IsTerminalError(err))
				require.Equal(t, restate.Code(tc.wantCode), restate.AsTerminalError(err).Code())
			}
			require.Equal(t, tc.wantResp, got)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Equal(t, tc.wantStatus, rec.saved.Status)
				require.True(t, rec.saved.Registered)
			}
			require.Len(t, rec.sends.snapshot(), tc.wantSends)
		})
	}
}

// TestPRObjectService_GetBuildLifecycle pins the shared lifecycle query.
func TestPRObjectService_GetBuildLifecycle(t *testing.T) {
	t.Parallel()
	started := svcRow("h1", "m1", RegistryStatusDeploying, TerminalMerged)
	started.StartedJobs = []string{"eri"}
	tests := []struct {
		name     string
		existing *PRState
		failGet  bool
		want     BuildLifecycle
		wantErr  bool
	}{
		{name: "missing pr is zero", want: BuildLifecycle{}},
		{name: "stored pr reports lifecycle", existing: started, want: BuildLifecycle{Found: true, Status: RegistryStatusDeploying, HeadSHA: "h1", MergeSHA: "m1", Started: true}},
		{name: "kv failure surfaces", failGet: true, wantErr: true},
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
			got, err := NewPRObjectService().GetBuildLifecycle(restate.WithMockContext(mc), BuildLifecycleRequest{})
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
		})
	}
}
