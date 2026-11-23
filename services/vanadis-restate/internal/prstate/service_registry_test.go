package prstate

import (
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/prwire"
)

// registryRowView is the slice of PRState the registry tests assert on.
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
	Banner                 string
	DeploySuccessPending   bool
	BuildRun               int64
}

func viewRow(s *PRState) registryRowView {
	if s == nil {
		return registryRowView{}
	}
	v := registryRowView{
		Status: s.Status, HeadSHA: s.HeadSHA, MergeSHA: s.MergeSHA, Branch: s.Branch, Title: s.Title, TargetID: s.TargetID,
		Jobs: s.Jobs, StartedJobs: s.StartedJobs, DeployedJobs: s.DeployedJobs, DeployedKustomizations: s.DeployedKustomizations,
		Banner: s.Banner, DeploySuccessPending: s.DeploySuccessPending,
	}
	if s.BuildMetadata != nil {
		v.BuildRun = s.BuildMetadata.RunNumber
	}
	return v
}

// svcRow seeds a stored PRObject state for repo#101.
func svcRow(head, merge, status string, terminal TerminalState) *PRState {
	s := NewPRState("azylman/aerial", 101)
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
	failedDeploy.Banner = "old"
	pendingDeployed := svcRow("h1", "m1", RegistryStatusDeployed, TerminalMerged)
	pendingDeployed.DeploySuccessPending = true
	pendingDeployed.KustomizationRequests = map[string]KustomizationRequest{"core": {Namespace: "flux-system"}}
	pendingDeployed.DeployedKustomizations = []string{"core"}
	conflicted := svcRow("h1", "", RegistryStatusConflict, TerminalNone)
	conflicted.MarkConflictPublished("h1")
	failedCI := svcRow("h1", "", RegistryStatusCIFailed, TerminalNone)
	failedCI.RecordFailedCheckRunDetailed("h1", "ci")
	failedCI.RecordFailedCISHA("h1")
	synced := svcRow("h1", "", RegistryStatusCIFailed, TerminalNone)
	synced.Banner = "b"
	req := KustomizationRequest{Namespace: "flux-system", RequestedAt: "t0"}

	mc01, rec01 := newStateMock[*PRState](t, nil)
	mc02, rec02 := newStateMock(t, failedCI)
	mc03, rec03 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc04, rec04 := newStateMock(t, conflicted)
	mc05, rec05 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc06, rec06 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc07, rec07 := newStateMock(t, svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged))
	stubRun(mc07, false)
	mc08, rec08 := newStateMock(t, deployingMerged())
	mc09, rec09 := newStateMock(t, deployingMerged())
	mc10, rec10 := newStateMock(t, deployingMerged())
	mc11, rec11 := newStateMock(t, failedDeploy)
	mc12, rec12 := newStateMock(t, pendingDeployed)
	mc13, rec13 := newStateMock(t, svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged))
	mc14, rec14 := newStateMock(t, synced)
	mc15, rec15 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalClosed))
	mc16, rec16 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	redelivered := svcRow("h1", "", RegistryStatusOpen, TerminalNone)
	redelivered.PriorHeads = []string{"h0", "h2"}
	mc17, rec17 := newStateMock(t, redelivered)
	mc19, rec19 := newStateMock(t, svcRow("h1", "", RegistryStatusOpen, TerminalNone))
	mc18, rec18 := newStateMock(t, svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged))
	_, err12 := svc.HandleDeploySuccess(restate.WithMockContext(mc12), PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", JobName: "core"})
	_, err16 := svc.HandleConflict(restate.WithMockContext(mc16), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1"})
	tests := []struct {
		name    string
		got     error
		rec     *stateCapture[*PRState]
		wantSet bool
		want    registryRowView
	}{
		{
			name: "check failed records ci_failed and identity", got: svc.HandleCheckFailed(restate.WithMockContext(mc01), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci", Branch: "feat", Title: "T", TargetID: snowA}), rec: rec01, wantSet: true,
			want: registryRowView{Status: RegistryStatusCIFailed, HeadSHA: "h1", Branch: "feat", Title: "T", TargetID: snowA},
		},
		{
			name: "check passed recovers ci_failed", got: svc.HandleCheckPassed(restate.WithMockContext(mc02), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci"}), rec: rec02, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h1"},
		},
		{
			name: "check status applies the event row", got: svc.HandleCheckStatus(restate.WithMockContext(mc03), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "ci", CheckState: "pending"}), rec: rec03, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h1"},
		},
		{
			name: "merged check status keeps the merged row", got: svc.HandleCheckStatus(restate.WithMockContext(mc18), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1", CheckName: "ci", CheckState: "passed"}), rec: rec18, wantSet: true,
			want: registryRowView{Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1"},
		},
		{
			name: "conflict resolved reopens", got: svc.HandleConflictResolved(restate.WithMockContext(mc04), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1"}), rec: rec04, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h1"},
		},
		{
			name: "merged records merge sha", got: svc.HandleMerged(restate.WithMockContext(mc05), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1"}), rec: rec05, wantSet: true,
			want: registryRowView{Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1"},
		},
		{
			name: "closed records closed", got: svc.HandleClosed(restate.WithMockContext(mc06), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1"}), rec: rec06, wantSet: true,
			want: registryRowView{Status: RegistryStatusClosed, HeadSHA: "h1"},
		},
		{
			name: "merged kustomization request starts deploying", got: svc.HandleKustomizationRequested(restate.WithMockContext(mc07), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Jobs: []string{"eri"}, KustomizationRequests: map[string]KustomizationRequest{"core": req}}), rec: rec07, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeploying, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}},
		},
		{
			name: "merged rollout marks ready jobs", got: svc.HandleRolloutProgress(restate.WithMockContext(mc08), PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", DeployedJobs: []string{"eri"}}), rec: rec08, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeploying, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}, DeployedJobs: []string{"eri"}},
		},
		{
			name: "merged rollout completes kustomizations", got: svc.HandleRolloutProgress(restate.WithMockContext(mc09), PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", DeployedKustomizations: []string{"core"}}), rec: rec09, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployed, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}, DeployedKustomizations: []string{"core"}, DeploySuccessPending: true},
		},
		{
			name: "merged deploy failure records banner", got: svc.HandleDeployFailed(restate.WithMockContext(mc10), PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", Banner: "boom"}), rec: rec10, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployFailed, HeadSHA: "h1", MergeSHA: "m1", Jobs: []string{"eri"}, StartedJobs: []string{"eri"}, Banner: "boom"},
		},
		{
			name: "repeated deploy failure takes the newest banner", got: svc.HandleDeployFailed(restate.WithMockContext(mc11), PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", Banner: "newest"}), rec: rec11, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployFailed, HeadSHA: "h1", MergeSHA: "m1", Banner: "newest"},
		},
		{
			name: "deploy success ack clears pending", got: err12, rec: rec12, wantSet: true,
			want: registryRowView{Status: RegistryStatusDeployed, HeadSHA: "h1", MergeSHA: "m1", DeployedKustomizations: []string{"core"}},
		},
		{
			name: "merged build metadata records on merge sha", got: svc.HandleBuildMetadata(restate.WithMockContext(mc13), PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", BuildMetadata: meta("final", 9, 1, 90, "m1", 1)}), rec: rec13, wantSet: true,
			want: registryRowView{Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1", BuildRun: 9},
		},
		{
			name: "synchronize from stored head syncs the row", got: svc.HandleSynchronized(restate.WithMockContext(mc14), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h2", BeforeSHA: "h1", Branch: "feat"}), rec: rec14, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h2", Branch: "feat"},
		},
		{name: "late synchronize of a prior head drops stale", got: svc.HandleSynchronized(restate.WithMockContext(mc17), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h2", BeforeSHA: "h0"}), rec: rec17, wantSet: false},
		{
			name: "missed synchronize recovers the stale head", got: svc.HandleSynchronized(restate.WithMockContext(mc19), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h3", BeforeSHA: "h2"}), rec: rec19, wantSet: true,
			want: registryRowView{Status: RegistryStatusOpen, HeadSHA: "h3"},
		},
		{name: "closed absorbs deploy failure", got: svc.HandleDeployFailed(restate.WithMockContext(mc15), PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Banner: "boom"}), rec: rec15, wantSet: false},
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
		{name: "new pr registers and indexes", req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: snowA, Agent: "aerial"}, wantResp: RegisterResponse{TargetID: snowA, Agent: "aerial", Status: RegistryStatusOpen}, wantSet: true, wantStatus: RegistryStatusOpen, wantSends: 1},
		{name: "headless target resolves stored target", existing: owned, req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: "headless:x", EnforceOwner: true, ExpectedOwner: "agent-alpha"}, wantResp: RegisterResponse{TargetID: snowB, Agent: "agent-alpha", Status: RegistryStatusOpen}, wantSet: true, wantStatus: RegistryStatusOpen, wantSends: 1},
		{name: "owner mismatch is a terminal 409", existing: owned, req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: snowA, EnforceOwner: true, ExpectedOwner: "aerial"}, wantErr: true, wantCode: 409},
		{name: "no target is a terminal 400", req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1"}, wantErr: true, wantCode: 400},
		{name: "missing branch is a terminal 400", req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", TargetID: snowA}, wantErr: true, wantCode: 400},
		{name: "kv failure surfaces", failGet: true, req: RegisterRequest{Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: snowA}, wantErr: true, wantCode: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := prKV(t, tc.existing, tc.failGet, false)
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
			mc, _ := prKV(t, tc.existing, tc.failGet, false)
			got, err := NewPRObjectService().GetBuildLifecycle(restate.WithMockContext(mc), BuildLifecycleRequest{})
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestPRObjectService_HandleSynchronized pins the synchronize head guard end
// to end: the committed head and PriorHeads ring, and the RepoObject index
// update that lets ResolvePRBySHA find the new head, for a normal advance, a
// recovered stale head and a late redelivery.
func TestPRObjectService_HandleSynchronized(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		stored        string
		status        string
		conflictSHA   string
		prior         []string
		ev            PREvent
		wantSet       bool
		wantHead      string
		wantPrior     []string
		wantStatus    string
		wantConflicts string
		wantSends     []sentCall
	}{
		{
			name: "before matches stored head", stored: "c00b710c",
			ev:      PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "79e9dfc2", BeforeSHA: "c00b710c", Branch: "feat/x"},
			wantSet: true, wantHead: "79e9dfc2", wantPrior: []string{"c00b710c"},
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "79e9dfc2", Lifecycle: IndexActive}}},
		},
		{
			name: "missed synchronize recovers and indexes the new head", stored: "c00b710c",
			ev:      PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "701c341c", BeforeSHA: "79e9dfc2", Branch: "feat/x"},
			wantSet: true, wantHead: "701c341c", wantPrior: []string{"c00b710c"},
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "701c341c", Lifecycle: IndexActive}}},
		},
		{
			name: "synchronize resolves a stored conflict", stored: "c00b710c", status: RegistryStatusConflict, conflictSHA: "c00b710c",
			ev:      PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "79e9dfc2", BeforeSHA: "c00b710c", Branch: "feat/x"},
			wantSet: true, wantHead: "79e9dfc2", wantPrior: []string{"c00b710c"}, wantStatus: RegistryStatusOpen,
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "79e9dfc2", Lifecycle: IndexActive}}},
		},
		{
			name: "synchronize of an open row keeps its published conflict record", stored: "c00b710c", conflictSHA: "c00b710c",
			ev:      PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "79e9dfc2", BeforeSHA: "c00b710c", Branch: "feat/x"},
			wantSet: true, wantHead: "79e9dfc2", wantPrior: []string{"c00b710c"}, wantStatus: RegistryStatusOpen, wantConflicts: "c00b710c",
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "79e9dfc2", Lifecycle: IndexActive}}},
		},
		{
			name: "late redelivery of a prior head drops", stored: "701c341c", prior: []string{"c00b710c", "79e9dfc2"},
			ev:       PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "79e9dfc2", BeforeSHA: "c00b710c", Branch: "feat/x"},
			wantHead: "701c341c", wantPrior: []string{"c00b710c", "79e9dfc2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seed := NewPRState("azylman/aerial", 2308)
			seed.HeadSHA, seed.Status, seed.Branch = tc.stored, RegistryStatusOpen, "feat/x"
			if tc.status != "" {
				seed.Status = tc.status
			}
			seed.ConflictPublished = tc.conflictSHA
			seed.PriorHeads = tc.prior
			mc, rec := newStateMock(t, seed)
			require.NoError(t, NewPRObjectService().HandleSynchronized(restate.WithMockContext(mc), tc.ev))
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantHead, seed.HeadSHA)
			require.Equal(t, tc.wantPrior, seed.PriorHeads)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
			if tc.wantStatus != "" {
				require.Equal(t, tc.wantStatus, seed.Status)
				require.Equal(t, tc.wantConflicts, seed.ConflictPublished)
			}
		})
	}
}

// TestPRObjectService_HandleMergedStaleHead pins the merged stale-head
// repair: a same-merge closed event carrying the real head adopts it
// (PriorHeads, RepoObject index), the first merge of an open PR whose
// stored head is stale applies on its merge SHA and adopts the carried head,
// while a merge-only ref, a prior head and another merge commit never move
// the head.
func TestPRObjectService_HandleMergedStaleHead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		open      bool
		prior     []string
		ev        PREvent
		wantSet   bool
		wantHead  string
		wantPrior []string
		wantSends []sentCall
	}{
		{
			name:    "same-merge closed event recovers and indexes the real head",
			ev:      PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "701c341c", MergeSHA: "913c2325", Branch: "feat/x"},
			wantSet: true, wantHead: "701c341c", wantPrior: []string{"c00b710c"},
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "701c341c", Lifecycle: IndexActive}}},
		},
		{
			name: "first merge of a stale open pr adopts the carried head", open: true,
			ev:      PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "701c341c", MergeSHA: "913c2325", Branch: "feat/x"},
			wantSet: true, wantHead: "701c341c", wantPrior: []string{"c00b710c"},
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "701c341c", MergeSHA: "913c2325", Lifecycle: IndexMerged}}},
		},
		{
			name:     "merge-only ref never overwrites the head",
			ev:       PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "913c2325", MergeSHA: "913c2325", Branch: "main"},
			wantHead: "c00b710c",
		},
		{
			name: "prior head is not adopted", prior: []string{"79e9dfc2"},
			ev:       PREvent{Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "79e9dfc2", MergeSHA: "913c2325", Branch: "feat/x"},
			wantHead: "c00b710c", wantPrior: []string{"79e9dfc2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seed := NewPRState("azylman/aerial", 2308)
			seed.HeadSHA, seed.MergeSHA, seed.Branch = "c00b710c", "913c2325", "feat/x"
			seed.Status, seed.Terminal, seed.PriorHeads = RegistryStatusMerged, TerminalMerged, tc.prior
			if tc.open {
				seed.Status, seed.Terminal, seed.MergeSHA = RegistryStatusOpen, TerminalNone, ""
			}
			mc, rec := newStateMock(t, seed)
			require.NoError(t, NewPRObjectService().HandleMerged(restate.WithMockContext(mc), tc.ev))
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantHead, seed.HeadSHA)
			require.Equal(t, tc.wantPrior, seed.PriorHeads)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestPRObjectService_DeploySuccessMergedStaleHead pins that a
// deploy_success commit on a PR merged with a stale head repairs the head
// and indexes it with the deployed index op.
func TestPRObjectService_DeploySuccessMergedStaleHead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		wantOut   DeployDispatchDecision
		wantSends []sentCall
	}{
		{
			name:    "first record indexes the repaired head as deployed",
			wantOut: DeployDispatchDecision{Disposition: DispositionApply, EffectKey: "azylman/aerial#2308@701c341c:gen0:deploy_dispatch:job=aerial-config", ShouldPublish: true},
			wantSends: []sentCall{{service: prwire.RepoObjectServiceName, key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{
				Repo: "azylman/aerial", PRNumber: 2308, Branch: "feat/x", HeadSHA: "701c341c", MergeSHA: "913c2325", Lifecycle: IndexDeployed,
			}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seed := NewPRState("azylman/aerial", 2308)
			seed.HeadSHA, seed.MergeSHA, seed.Branch = "c00b710c", "913c2325", "feat/x"
			seed.Status, seed.Terminal = RegistryStatusMerged, TerminalMerged
			mc, rec := newStateMock(t, seed)
			out, err := NewPRObjectService().HandleDeploySuccess(restate.WithMockContext(mc), PREvent{
				Repo: "azylman/aerial", PRNumber: 2308, HeadSHA: "701c341c", MergeSHA: "913c2325", JobName: "aerial-config",
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantOut, out)
			require.Equal(t, "701c341c", seed.HeadSHA)
			require.Equal(t, []string{"c00b710c"}, seed.PriorHeads)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}
