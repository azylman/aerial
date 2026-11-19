package main

import (
	"testing"

	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func TestMockPostgresConnection(t *testing.T) {
	addr := testutil.StartMockPostgres(t)
	dsn := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"
	db, err := OpenDB(t.Context(), &Config{DatabaseURL: dsn})
	if err != nil {
		t.Fatalf("OpenDB with mock postgres failed: %v", err)
	}
	db.Close()
}
