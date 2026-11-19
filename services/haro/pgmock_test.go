package main

import (
	"testing"

	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func TestMockPostgresConnection(t *testing.T) {
	addr := testutil.StartMockPostgres(t)
	dsn := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"
	db, err := store.NewDB(t.Context(), &config.Config{DatabaseURL: dsn})
	if err != nil {
		t.Fatalf("store.NewDB with mock postgres failed: %v", err)
	}
	db.Close()
}
