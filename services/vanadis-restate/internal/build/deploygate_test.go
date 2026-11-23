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
// runtime without the deployment surface is rejected and a durable snapshot
// failure surfaces. Fencing and deadline
// re-arming belong to the deploygate package tests.
func TestDeploygateRetryDelegation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	gateRuntime := func() objectRuntime {
		rt := &fakeObjectRuntime{key: "b", now: now}
		rt.attempts = map[string]deploygate.Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(-time.Minute), Delivered: true}}
		return rt
	}
	tests := []struct {
		name                  string
		rt                    objectRuntime
		storeErr              error
		wantErr               string
		wantBuild, wantStatus string
	}{
		{name: "deployment runtime", rt: gateRuntime(), wantBuild: "b", wantStatus: statusRunning},
		{name: "runtime without deployment surface", rt: struct{ objectRuntime }{newFakeObjectRuntime("b")}, wantErr: "deployment runtime unavailable"},
		{name: "durable snapshot failure surfaces", rt: &fakeObjectRuntime{key: "b", now: now}, storeErr: errors.New("snapshot unavailable"), wantErr: "snapshot unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &fakeStore{builds: []model.VanadisBuild{{ID: "b", Status: statusRunning, ThreadID: "th-1"}}, chunks: []model.VanadisChunk{{ID: "c", BuildID: "b", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed}}, errs: map[string]error{"GetBuild": tc.storeErr}}
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
	plain := struct{ objectRuntime }{newFakeObjectRuntime("b")}
	require.Error(t, bo.expireDeploymentDeadline(plain, deploygate.DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
	require.Error(t, bo.armDeploymentStart(plain, "b", pubsub.PRLifecycleEvent{}))
	require.NoError(t, bo.syncDeploymentDeadlines(plain, nil, nil, nil))
}

// TestDeploygateDelegationErrorPaths pins the remaining thin-delegation
// branch: non-nil retry fencing conversion through the sync wrapper.
func TestDeploygateDelegationErrorPaths(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rt := &fakeObjectRuntime{key: "b", now: now}
	rt.attempts = map[string]deploygate.Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(-time.Minute)}}
	st := &fakeStore{builds: []model.VanadisBuild{{ID: "b", Status: statusRunning, ThreadID: "th-1"}}, chunks: []model.VanadisChunk{{ID: "c", BuildID: "b", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}}
	bo := NewBuildObject("", nil).WithAuthority(st, nil)
	state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}}
	require.NoError(t, bo.syncDeploymentDeadlines(rt, state, nil, &BuildMutationRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
	require.Equal(t, 2, rt.attempts["c"].Generation)
}
