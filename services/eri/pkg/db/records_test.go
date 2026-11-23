package db_test

import (
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

func TestSQLStore_RecordConformance(t *testing.T) {
	t.Parallel()
	dbtest.RunRecordStoreConformance(t, func(t *testing.T) db.Store {
		t.Helper()
		return db.NewTestStore(t)
	})
}
