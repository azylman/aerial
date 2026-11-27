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

	// 1. Nil store returns existing without error
	existing := []db.Message{{ID: "m1"}}
	got, err := recoverBurstProcessingRows(context.Background(), nil, "thread-1", existing)
	require.NoError(t, err)
	assert.Equal(t, existing, got)

	// 2. Empty threadID returns existing without error
	store := testStore(t)
	got, err = recoverBurstProcessingRows(context.Background(), store, "", existing)
	require.NoError(t, err)
	assert.Equal(t, existing, got)

	// 3. Store error in GetActiveTasks returns error
	sdb := openStepDB(t, stepFixture{closeDB: true, msgs: existing})
	_, err = recoverBurstProcessingRows(context.Background(), sdb.store, "thread-1", existing)
	require.Error(t, err)

	// 4. Normal recovery merges processing rows
	m2 := db.Message{ID: "m2", ThreadID: "thread-norm", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	seedMessageRows(t, store, []db.Message{m2})
	recovered, err := recoverBurstProcessingRows(context.Background(), store, "thread-norm", []db.Message{{ID: "m1", ThreadID: "thread-norm"}})
	require.NoError(t, err)
	require.Len(t, recovered, 2)
	assert.Equal(t, "m1", recovered[0].ID)
	assert.Equal(t, "m2", recovered[1].ID)
}

func TestExpandDispatchActiveIDs_Errors(t *testing.T) {
	t.Parallel()

	// 1. Nil store or empty ids returns ids without error
	got, err := expandDispatchActiveIDs(context.Background(), nil, []string{"d1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"d1"}, got)

	got, err = expandDispatchActiveIDs(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Empty(t, got)

	// 2. db.ErrRecordNotFound is skipped without error
	store := testStore(t)
	got, err = expandDispatchActiveIDs(context.Background(), store, []string{"nonexistent-d"})
	require.NoError(t, err)
	assert.Equal(t, []string{"nonexistent-d"}, got)

	// 3. Store error in GetDispatch returns error
	sdb := openStepDB(t, stepFixture{closeDB: true})
	_, err = expandDispatchActiveIDs(context.Background(), sdb.store, []string{"d1"})
	require.Error(t, err)

	// 4. Normal expansion merges processing dispatches in session
	d1 := db.Dispatch{ID: "d1", SessionID: "sess-exp", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	d2 := db.Dispatch{ID: "d2", SessionID: "sess-exp", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2})
	expanded, err := expandDispatchActiveIDs(context.Background(), store, []string{"d1"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d1", "d2"}, expanded)
}
