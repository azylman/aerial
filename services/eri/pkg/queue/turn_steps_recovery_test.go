package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

func TestRecoverBurstProcessingRows_Errors(t *testing.T) {
	t.Parallel()

	// 1. Empty threadID returns existing without error
	existing := []db.Message{{ID: "m1"}}
	store := testStore(t)
	got, err := recoverBurstProcessingRows(context.Background(), store, "", existing)
	require.NoError(t, err)
	assert.Equal(t, existing, got)

	// 2. Store error in GetActiveTasks returns error
	sdb := openStepDB(t, stepFixture{closeDB: true, msgs: existing})
	_, err = recoverBurstProcessingRows(context.Background(), sdb.store, "thread-1", existing)
	require.Error(t, err)
}

func TestExpandDispatchActiveIDs_Errors(t *testing.T) {
	t.Parallel()

	// 1. Empty ids returns empty without error
	store := testStore(t)
	got, err := expandDispatchActiveIDs(context.Background(), store, nil)
	require.NoError(t, err)
	assert.Empty(t, got)

	// 2. Store error in GetDispatch returns error
	sdb := openStepDB(t, stepFixture{closeDB: true})
	_, err = expandDispatchActiveIDs(context.Background(), sdb.store, []string{"d1"})
	require.Error(t, err)
}

func TestFailActiveBurstRows_RecoversProcessingRowsWhenInitialRowsCompleted(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	th := "thread-fail-active-rec"
	m1 := db.Message{ID: "m1-comp", ThreadID: th, Status: db.StatusCompleted, CreatedAt: time.Now().UTC()}
	m2 := db.Message{ID: "m2-proc", ThreadID: th, Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedMessageRows(t, store, []db.Message{m1, m2})

	n, err := pool.failActiveBurstRows(context.Background(), store, []string{m1.ID}, AttemptSummary{Outcome: OutcomeFailed, Reason: "cleanup"})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	gotM2, err := store.GetMessage(context.Background(), m2.ID)
	require.NoError(t, err)
	require.NotNil(t, gotM2)
	assert.Equal(t, db.StatusFailed, gotM2.Status)
	assert.Equal(t, "cleanup", gotM2.ErrorMessage)
}
