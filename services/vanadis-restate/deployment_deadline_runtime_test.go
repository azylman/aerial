package main

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
)

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
			mc.EXPECT().Get("deployment_deadlines", mock.Anything).Return(false, nil).Maybe()
			mc.EXPECT().Get("pending_deployment_starts", mock.Anything).Return(false, nil)
			mc.EXPECT().Set("pending_deployment_starts", map[string]pendingDeploymentStart{}, mock.Anything).Return()
			mc.EXPECT().Set("deployment_deadlines", mock.Anything, mock.Anything).Return().Maybe()
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Maybe()
			client := mocks.NewMockClient(t)
			invocation := mocks.NewMockInvocation(t)
			mc.EXPECT().Object("Build", "b", "DeploymentDeadline").Return(client)
			client.EXPECT().Send(DeploymentDeadlineRequest{ChunkID: "c"}, mock.MatchedBy(func(option restate.SendOption) bool {
				return reflect.DeepEqual(option, restate.WithDelay(tc.effective))
			})).Return(invocation)
			ctx := restate.WithMockContext(mc)
			rt := restateObjectRuntime{ctx: ctx}
			attempts, err := rt.GetDeploymentAttempts()
			require.NoError(t, err)
			require.Empty(t, attempts)
			rt.SetDeploymentAttempts(map[string]deploymentAttempt{})
			pending, err := rt.GetPendingDeploymentStarts()
			require.NoError(t, err)
			require.Empty(t, pending)
			rt.SetPendingDeploymentStarts(map[string]pendingDeploymentStart{})
			now, err := rt.DeploymentNow()
			require.NoError(t, err)
			require.False(t, now.IsZero())
			rt.ScheduleDeploymentDeadline(DeploymentDeadlineRequest{ChunkID: "c"}, tc.delay)
			require.NoError(t, (&BuildObject{}).DeploymentDeadline(ctx, DeploymentDeadlineRequest{ChunkID: "c"}))
		})
	}
}

func TestDeploymentDeadlineDeliveryFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		store *checkStatusSpyStore
	}{
		{"store unavailable", nil},
		{"read failed", &checkStatusSpyStore{getBuildErr: errors.New("read failed")}},
		{"banner failed", &checkStatusSpyStore{bannerErr: errors.New("banner failed")}},
		{"executor unavailable", &checkStatusSpyStore{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := &BuildObject{}
			if tc.store != nil {
				tc.store.chunks = []VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}
				b.store = tc.store
			}
			out, err := b.deliverDeploymentDeadline(context.Background(), "b", deploymentAttempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1})
			require.NoError(t, err)
			require.NotEmpty(t, out.Error)
		})
	}
}
