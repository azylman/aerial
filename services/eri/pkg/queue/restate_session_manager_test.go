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

func TestSessionManagerService_ThreadControls(t *testing.T) {
	t.Parallel()

	t.Run("service naming", func(t *testing.T) {
		t.Parallel()
		mgr := NewSessionManagerService(nil, "aerial")
		assert.Equal(t, "SessionManager", mgr.ServiceName())
		mgrMio := NewSessionManagerService(nil, "miorine")
		assert.Equal(t, "SessionManager-miorine", mgrMio.ServiceName())
	})

	t.Run("cancel thread preempts active turn and cancels pending messages", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		for i := range 2 {
			_ = store.InsertMessage(context.Background(), db.Message{
				ID:       fmt.Sprintf("th-ctrl-m%d", i),
				ThreadID: "th-ctrl-1",
				Status:   db.StatusPending,
			})
		}
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")

		registerActiveInvocation("th-ctrl-1", "inv-ctrl-active")
		defer unregisterActiveInvocation("th-ctrl-1", "inv-ctrl-active")

		mc, _ := setupActorMockContext(t, "th-ctrl-1", "")
		resp, err := mgr.CancelThread(restate.WithMockContext(mc), CancelThreadControlRequest{
			ThreadID: "th-ctrl-1",
			Reason:   "operator cancel",
		})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.Equal(t, 2, resp.CancelledCount)
		assert.True(t, resp.CancelledActiveTurn)
	})

	t.Run("cancel thread rejects empty thread id", func(t *testing.T) {
		t.Parallel()
		mgr := NewSessionManagerService(nil, "aerial")
		mc, _ := setupActorMockContext(t, "", "")
		_, err := mgr.CancelThread(restate.WithMockContext(mc), CancelThreadControlRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "thread_id is required")
	})

	t.Run("rotate thread preempts active turn and rotates session", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")

		registerActiveInvocation("th-ctrl-rot", "inv-ctrl-rot")
		defer unregisterActiveInvocation("th-ctrl-rot", "inv-ctrl-rot")

		mc, _ := setupActorMockContext(t, "th-ctrl-rot", "")
		resp, err := mgr.RotateThread(restate.WithMockContext(mc), RotateThreadControlRequest{
			ThreadID: "th-ctrl-rot",
		})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.NotEmpty(t, resp.NewSessionID)
	})

	t.Run("rotate thread rejects empty thread id", func(t *testing.T) {
		t.Parallel()
		mgr := NewSessionManagerService(nil, "aerial")
		mc, _ := setupActorMockContext(t, "", "")
		_, err := mgr.RotateThread(restate.WithMockContext(mc), RotateThreadControlRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "thread_id is required")
	})

	t.Run("nil service receiver", func(t *testing.T) {
		t.Parallel()
		var nilMgr *SessionManagerService
		assert.Equal(t, "SessionManager", nilMgr.ServiceName())
	})

	t.Run("cancel thread with default reason and nil pool", func(t *testing.T) {
		t.Parallel()
		mgr := NewSessionManagerService(nil, "aerial")
		mc, _ := setupActorMockContext(t, "th-nil-pool", "")
		resp, err := mgr.CancelThread(restate.WithMockContext(mc), CancelThreadControlRequest{ThreadID: "th-nil-pool"})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.Equal(t, 0, resp.CancelledCount)
	})

	t.Run("cancel thread with store error", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		store.FailNext("GetPendingOrProcessingMessages", errors.New("read fail"))
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")
		mc, _ := setupActorMockContext(t, "th-err", "")
		resp, err := mgr.CancelThread(restate.WithMockContext(mc), CancelThreadControlRequest{ThreadID: "th-err"})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.Equal(t, 0, resp.CancelledCount)
	})

	t.Run("rotate thread with store error handles gracefully", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		store.FailNext("RotateSessionID", errors.New("rotate fail"))
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")
		mc, _ := setupActorMockContext(t, "th-rot-err", "")
		resp, err := mgr.RotateThread(restate.WithMockContext(mc), RotateThreadControlRequest{ThreadID: "th-rot-err"})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.NotEmpty(t, resp.NewSessionID)
	})
}

func TestSessionManagerService_DispatchControls(t *testing.T) {
	t.Parallel()

	t.Run("cancel dispatch preempts active dispatch and cancels pending rows", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		for i := range 2 {
			_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
				ID:             fmt.Sprintf("disp-ctrl-%d", i),
				SessionID:      "vanadis-chunk-ctrl",
				VanadisChunkID: "chunk-ctrl",
				Status:         db.StatusPending,
			})
		}
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")

		registerActiveInvocation("vanadis-chunk-ctrl", "inv-disp-ctrl")
		defer unregisterActiveInvocation("vanadis-chunk-ctrl", "inv-disp-ctrl")

		mc, _ := setupActorMockContext(t, "vanadis-chunk-ctrl", "")
		resp, err := mgr.CancelDispatch(restate.WithMockContext(mc), CancelDispatchControlRequest{
			SessionKey: "vanadis-chunk-ctrl",
			ChunkID:    "chunk-ctrl",
		})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.Equal(t, 2, resp.CancelledCount)
	})

	t.Run("cancel dispatch with prefix inference and fallback invocations", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:        "d-infer",
			SessionID: "dispatch-d-infer",
			Status:    db.StatusPending,
		})
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:             "d-vanadis-infer",
			VanadisChunkID: "chunk-infer",
			SessionID:      "vanadis-chunk-infer",
			Status:         db.StatusPending,
		})
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")

		registerActiveInvocation("dispatch-d-infer", "inv-disp-infer")
		defer unregisterActiveInvocation("dispatch-d-infer", "inv-disp-infer")
		registerActiveInvocation("vanadis-chunk-infer", "inv-van-infer")
		defer unregisterActiveInvocation("vanadis-chunk-infer", "inv-van-infer")

		mc1, _ := setupActorMockContext(t, "dispatch-d-infer", "")
		resp1, err1 := mgr.CancelDispatch(restate.WithMockContext(mc1), CancelDispatchControlRequest{SessionKey: "dispatch-d-infer"})
		require.NoError(t, err1)
		assert.Equal(t, "ok", resp1.Status)
		assert.Equal(t, 1, resp1.CancelledCount)

		mc2, _ := setupActorMockContext(t, "vanadis-chunk-infer", "")
		resp2, err2 := mgr.CancelDispatch(restate.WithMockContext(mc2), CancelDispatchControlRequest{SessionKey: "vanadis-chunk-infer"})
		require.NoError(t, err2)
		assert.Equal(t, "ok", resp2.Status)
		assert.Equal(t, 1, resp2.CancelledCount)
	})

	t.Run("cancel dispatch with dispatchID and buildID active invocation lookup", func(t *testing.T) {
		t.Parallel()
		mgr := NewSessionManagerService(nil, "aerial")

		registerActiveInvocation("dispatch-d-lookup", "inv-disp-lookup")
		defer unregisterActiveInvocation("dispatch-d-lookup", "inv-disp-lookup")
		registerActiveInvocation("vanadis-b-lookup", "inv-van-lookup")
		defer unregisterActiveInvocation("vanadis-b-lookup", "inv-van-lookup")

		mc1, _ := setupActorMockContext(t, "", "")
		resp1, err1 := mgr.CancelDispatch(restate.WithMockContext(mc1), CancelDispatchControlRequest{DispatchID: "d-lookup"})
		require.NoError(t, err1)
		assert.Equal(t, "ok", resp1.Status)

		mc2, _ := setupActorMockContext(t, "", "")
		resp2, err2 := mgr.CancelDispatch(restate.WithMockContext(mc2), CancelDispatchControlRequest{BuildID: "b-lookup"})
		require.NoError(t, err2)
		assert.Equal(t, "ok", resp2.Status)
	})

	t.Run("cancel dispatch with pool active processes", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		mgr := NewSessionManagerService(pool, "aerial")
		mc, _ := setupActorMockContext(t, "sess-1", "")
		resp, err := mgr.CancelDispatch(restate.WithMockContext(mc), CancelDispatchControlRequest{SessionKey: "sess-1"})
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
	})
}
