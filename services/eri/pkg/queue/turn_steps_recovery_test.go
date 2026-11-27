package queue

import (
	"context"
	"testing"

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
