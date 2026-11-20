package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

func TestSessionManagerService_ServiceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		agentName string
		wantName  string
	}{
		{
			name:      "default aerial",
			agentName: "aerial",
			wantName:  "SessionManager",
		},
		{
			name:      "agent-alpha stack",
			agentName: "agent-alpha",
			wantName:  "SessionManager-agent-alpha",
		},
		{
			name:      "agent-beta stack",
			agentName: "agent-beta",
			wantName:  "SessionManager-agent-beta",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := NewSessionManagerService(nil, tc.agentName)
			assert.Equal(t, tc.wantName, mgr.ServiceName())
		})
	}
}

type cancelThreadTestCase struct {
	name                string
	threadID            string
	reason              string
	nilPool             bool
	pendingMessages     int
	activeInvocationID  string
	storeFail           bool
	wantErrSub          string
	wantTerminal        bool
	wantCancelledCount  int
	wantCancelledActive bool
}

func runCancelThreadCase(t *testing.T, tc cancelThreadTestCase) {
	t.Helper()
	var pool *WorkerPool
	if !tc.nilPool {
		store := dbtest.NewFakeStore()
		for i := range tc.pendingMessages {
			_ = store.InsertMessage(context.Background(), db.Message{
				ID:       fmt.Sprintf("%s-m%d", tc.threadID, i),
				ThreadID: tc.threadID,
				Status:   db.StatusPending,
			})
		}
		if tc.storeFail {
			store.FailNext("GetPendingOrProcessingMessages", errors.New("read fail"))
		}
		pool = NewWorkerPool(WorkerPoolConfig{Store: store})
	}

	mgr := NewSessionManagerService(pool, "aerial")
	if tc.activeInvocationID != "" && tc.threadID != "" {
		registerActiveInvocation(tc.threadID, tc.activeInvocationID)
		t.Cleanup(func() { unregisterActiveInvocation(tc.threadID, tc.activeInvocationID) })
	}

	mc, _ := setupActorMockContext(t, tc.threadID, "")
	resp, err := mgr.CancelThread(restate.WithMockContext(mc), CancelThreadControlRequest{
		ThreadID: tc.threadID,
		Reason:   tc.reason,
	})

	if tc.wantErrSub != "" {
		assertTerminalOrRetriableError(t, err, tc.wantErrSub, tc.wantTerminal)
		return
	}

	require.NoError(t, err)
	assert.Equal(t, "ok", resp.Status)
	assert.Equal(t, tc.wantCancelledCount, resp.CancelledCount)
	assert.Equal(t, tc.wantCancelledActive, resp.CancelledActiveTurn)
}

func TestSessionManagerService_CancelThread(t *testing.T) {
	t.Parallel()

	tests := []cancelThreadTestCase{
		{
			name:                "preempts active turn and cancels pending messages",
			threadID:            "th-ctrl-1",
			reason:              "operator cancel",
			pendingMessages:     2,
			activeInvocationID:  "inv-ctrl-active",
			wantCancelledCount:  2,
			wantCancelledActive: true,
		},
		{
			name:         "rejects empty thread id as terminal 400",
			threadID:     "",
			wantErrSub:   "thread_id is required",
			wantTerminal: true,
		},
		{
			name:               "default reason and nil pool",
			threadID:           "th-nil-pool",
			nilPool:            true,
			wantCancelledCount: 0,
		},
		{
			name:       "store error propagates as retriable non-terminal",
			threadID:   "th-err",
			storeFail:  true,
			wantErrSub: "read fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCancelThreadCase(t, tc)
		})
	}
}

type rotateThreadTestCase struct {
	name               string
	threadID           string
	nilPool            bool
	activeInvocationID string
	storeFail          bool
	wantErrSub         string
	wantTerminal       bool
	wantStatus         string
}

func runRotateThreadCase(t *testing.T, tc rotateThreadTestCase) {
	t.Helper()
	var pool *WorkerPool
	if !tc.nilPool {
		store := dbtest.NewFakeStore()
		if tc.storeFail {
			store.FailNext("RotateSessionID", errors.New("rotate fail"))
		}
		pool = NewWorkerPool(WorkerPoolConfig{Store: store})
	}

	mgr := NewSessionManagerService(pool, "aerial")
	if tc.activeInvocationID != "" && tc.threadID != "" {
		registerActiveInvocation(tc.threadID, tc.activeInvocationID)
		t.Cleanup(func() { unregisterActiveInvocation(tc.threadID, tc.activeInvocationID) })
	}

	mc, _ := setupActorMockContext(t, tc.threadID, "")
	resp, err := mgr.RotateThread(restate.WithMockContext(mc), RotateThreadControlRequest{
		ThreadID: tc.threadID,
	})

	if tc.wantErrSub != "" {
		assertTerminalOrRetriableError(t, err, tc.wantErrSub, tc.wantTerminal)
		return
	}

	require.NoError(t, err)
	assert.Equal(t, tc.wantStatus, resp.Status)
	assert.NotEmpty(t, resp.NewSessionID)
}

func TestSessionManagerService_RotateThread(t *testing.T) {
	t.Parallel()

	tests := []rotateThreadTestCase{
		{
			name:               "preempts active turn and rotates session",
			threadID:           "th-ctrl-rot",
			activeInvocationID: "inv-ctrl-rot",
			wantStatus:         "ok",
		},
		{
			name:         "rejects empty thread id as terminal 400",
			threadID:     "",
			wantErrSub:   "thread_id is required",
			wantTerminal: true,
		},
		{
			name:       "store error propagates as retriable non-terminal",
			threadID:   "th-rot-err",
			storeFail:  true,
			wantErrSub: "rotate fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRotateThreadCase(t, tc)
		})
	}
}

type sessionManagerCancelDispatchTestCase struct {
	name               string
	req                CancelDispatchControlRequest
	nilPool            bool
	pendingRows        []db.Dispatch
	activeInvocations  map[string]string
	storeFail          bool
	wantErrSub         string
	wantCancelledCount int
	wantStatus         string
}

func runSessionManagerCancelDispatchCase(t *testing.T, tc sessionManagerCancelDispatchTestCase) {
	t.Helper()
	var pool *WorkerPool
	if !tc.nilPool {
		store := dbtest.NewFakeStore()
		for _, row := range tc.pendingRows {
			_, _ = store.InsertDispatch(context.Background(), row)
		}
		if tc.storeFail {
			store.FailNext("CancelPendingDispatches", errors.New("cancel dispatch fail"))
		}
		pool = NewWorkerPool(WorkerPoolConfig{Store: store})
	}

	mgr := NewSessionManagerService(pool, "aerial")
	for k, inv := range tc.activeInvocations {
		registerActiveInvocation(k, inv)
		cleanupKey := k
		cleanupInv := inv
		t.Cleanup(func() { unregisterActiveInvocation(cleanupKey, cleanupInv) })
	}

	mc, _ := setupActorMockContext(t, tc.req.SessionKey, "")
	resp, err := mgr.CancelDispatch(restate.WithMockContext(mc), tc.req)

	if tc.wantErrSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrSub)
		return
	}

	require.NoError(t, err)
	assert.Equal(t, tc.wantStatus, resp.Status)
	assert.Equal(t, tc.wantCancelledCount, resp.CancelledCount)
}

func TestSessionManagerService_CancelDispatch(t *testing.T) {
	t.Parallel()

	tests := []sessionManagerCancelDispatchTestCase{
		{
			name: "preempts active dispatch and cancels pending rows",
			req: CancelDispatchControlRequest{
				SessionKey: "vanadis-chunk-ctrl",
				ChunkID:    "chunk-ctrl",
			},
			pendingRows: []db.Dispatch{
				{
					ID:             "disp-ctrl-0",
					SessionID:      "vanadis-chunk-ctrl",
					VanadisChunkID: "chunk-ctrl",
					Status:         db.StatusPending,
				},
				{
					ID:             "disp-ctrl-1",
					SessionID:      "vanadis-chunk-ctrl",
					VanadisChunkID: "chunk-ctrl",
					Status:         db.StatusPending,
				},
			},
			activeInvocations: map[string]string{
				"vanadis-chunk-ctrl": "inv-disp-ctrl",
			},
			wantCancelledCount: 2,
			wantStatus:         "ok",
		},
		{
			name: "prefix inference for dispatch session",
			req: CancelDispatchControlRequest{
				SessionKey: "dispatch-d-infer",
			},
			pendingRows: []db.Dispatch{
				{
					ID:        "d-infer",
					SessionID: "dispatch-d-infer",
					Status:    db.StatusPending,
				},
			},
			activeInvocations: map[string]string{
				"dispatch-d-infer": "inv-disp-infer",
			},
			wantCancelledCount: 1,
			wantStatus:         "ok",
		},
		{
			name: "prefix inference for vanadis session",
			req: CancelDispatchControlRequest{
				SessionKey: "vanadis-chunk-infer",
			},
			pendingRows: []db.Dispatch{
				{
					ID:             "d-vanadis-infer",
					VanadisChunkID: "chunk-infer",
					SessionID:      "vanadis-chunk-infer",
					Status:         db.StatusPending,
				},
			},
			activeInvocations: map[string]string{
				"vanadis-chunk-infer": "inv-van-infer",
			},
			wantCancelledCount: 1,
			wantStatus:         "ok",
		},
		{
			name: "lookup active invocation by dispatch id",
			req: CancelDispatchControlRequest{
				DispatchID: "d-lookup",
			},
			nilPool: true,
			activeInvocations: map[string]string{
				"dispatch-d-lookup": "inv-disp-lookup",
			},
			wantStatus: "ok",
		},
		{
			name: "lookup active invocation by build id",
			req: CancelDispatchControlRequest{
				BuildID: "b-lookup",
			},
			nilPool: true,
			activeInvocations: map[string]string{
				"vanadis-b-lookup": "inv-van-lookup",
			},
			wantStatus: "ok",
		},
		{
			name: "pool active processes cancellation",
			req: CancelDispatchControlRequest{
				SessionKey: "sess-1",
			},
			wantStatus: "ok",
		},
		{
			name: "store error propagates as retriable non-terminal",
			req: CancelDispatchControlRequest{
				DispatchID: "d-fail",
			},
			storeFail:  true,
			wantErrSub: "cancel dispatch fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSessionManagerCancelDispatchCase(t, tc)
		})
	}
}
