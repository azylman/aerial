package build

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/restatetest"
)

// TestDeploymentDeadlineRuntime pins the Restate glue for the deployment gate:
// state keys, journal step names, the DeploymentDeadline self-send target, and
// delay clamping. Behavior must stay byte-identical with the pre-extraction
// internal/build copies.
func TestDeploymentDeadlineRuntime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		delay     time.Duration
		effective time.Duration
	}{{"future", time.Minute, time.Minute}, {"overdue", -time.Minute, 0}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("b").Maybe()
			mc.EXPECT().Get(deploygate.DeploymentDeadlineKey, mock.Anything).Return(false, nil).Maybe()
			mc.EXPECT().Get(deploygate.PendingDeploymentKey, mock.Anything).Return(false, nil)
			mc.EXPECT().Set(deploygate.PendingDeploymentKey, map[string]deploygate.PendingStart{}, mock.Anything).Return()
			mc.EXPECT().Set(deploygate.DeploymentDeadlineKey, mock.Anything, mock.Anything).Return().Maybe()
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(restatetest.RunMockRunContext).Maybe()
			client := mocks.NewMockClient(t)
			invocation := mocks.NewMockInvocation(t)
			mc.EXPECT().Object("Build", "b", "DeploymentDeadline").Return(client)
			client.EXPECT().Send(deploygate.DeploymentDeadlineRequest{ChunkID: "c"}, mock.MatchedBy(func(option restate.SendOption) bool {
				return reflect.DeepEqual(option, restate.WithDelay(tc.effective))
			})).Return(invocation)
			ctx := restate.WithMockContext(mc)
			rt := restateObjectRuntime{ctx: ctx}
			attempts, err := rt.GetAttempts()
			require.NoError(t, err)
			require.Empty(t, attempts)
			rt.SetAttempts(map[string]deploygate.Attempt{})
			pending, err := rt.GetPendingStarts()
			require.NoError(t, err)
			require.Empty(t, pending)
			rt.SetPendingStarts(map[string]deploygate.PendingStart{})
			now, err := rt.Now()
			require.NoError(t, err)
			require.False(t, now.IsZero())
			rt.ScheduleDeadline(deploygate.DeploymentDeadlineRequest{ChunkID: "c"}, tc.delay)
			require.NoError(t, (&BuildObject{}).DeploymentDeadline(ctx, deploygate.DeploymentDeadlineRequest{ChunkID: "c"}))
		})
	}
}

func TestDeploymentGateRunStepJournal(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(restatetest.RunMockRunContext)
	ctx := restate.WithMockContext(mc)
	rt := restateObjectRuntime{ctx: ctx}
	out, err := rt.RunStep("deployment-retry-reset", func(context.Context) (deploygate.StepOutput, error) {
		return deploygate.StepOutput{BuildStatus: statusRunning}, nil
	})
	require.NoError(t, err)
	require.Equal(t, statusRunning, out.BuildStatus)
}

// TestDeploygateRetryDelegation pins the thin Mutate retry_deployment wiring:
// the deploygate outcome maps onto the result's BuildID and Status, and a
// runtime without the deployment surface is rejected. Fencing and deadline
// re-arming belong to the deploygate package tests.
func TestDeploygateRetryDelegation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	gateRuntime := func() objectRuntime {
		rt := &deploymentGateFakeRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
		rt.attempts = map[string]deploygate.Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(-time.Minute), Delivered: true}}
		return rt
	}
	tests := []struct {
		name                  string
		rt                    objectRuntime
		wantErr               string
		wantBuild, wantStatus string
	}{
		{name: "deployment runtime", rt: gateRuntime(), wantBuild: "b", wantStatus: statusRunning},
		{name: "runtime without deployment surface", rt: newFakeObjectRuntime("b"), wantErr: "deployment runtime unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed}}}
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return tc.rt }).WithAuthority(st, nil)
			out, err := bo.Mutate(nil, BuildMutationRequest{Action: "retry_deployment", ChunkID: "c", Revision: "sha", Generation: 1})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantBuild, out.BuildID)
			require.Equal(t, tc.wantStatus, out.Status)
		})
	}
}

// TestDeploygateWrapperRuntimeGuards pins the programming-error path: a
// runtime without the deployment surface fails fast instead of panicking.
func TestDeploygateWrapperRuntimeGuards(t *testing.T) {
	t.Parallel()
	bo := &BuildObject{}
	plain := newFakeObjectRuntime("b")
	require.Error(t, bo.expireDeploymentDeadline(plain, deploygate.DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
	require.Error(t, bo.armDeploymentStart(plain, "b", pubsub.PRLifecycleEvent{}))
	require.NoError(t, bo.syncDeploymentDeadlines(plain, nil, nil, nil))
}

// TestDeploygateDelegationErrorPaths pins the remaining thin-delegation
// branches: non-nil retry fencing conversion through the sync wrapper and
// durable snapshot failures surfacing from the retry wrapper.
func TestDeploygateDelegationErrorPaths(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rt := &deploymentGateFakeRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
	rt.attempts = map[string]deploygate.Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(-time.Minute)}}
	st := &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}}
	bo := NewBuildObject("", nil).WithAuthority(st, nil)
	state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}}
	require.NoError(t, bo.syncDeploymentDeadlines(rt, state, nil, &BuildMutationRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
	require.Equal(t, 2, rt.attempts["c"].Generation)

	rtErr := &deploymentGateFakeRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
	stErr := &checkStatusSpyStore{getBuildErr: errors.New("snapshot unavailable")}
	boErr := NewBuildObject("", nil).WithAuthority(stErr, nil)
	boErr.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rtErr }
	_, err := boErr.Mutate(nil, BuildMutationRequest{Action: "retry_deployment", ChunkID: "c", Revision: "sha", Generation: 1})
	require.Error(t, err)
}
