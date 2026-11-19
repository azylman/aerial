package main

import (
	"context"
	"database/sql"
	"testing"

	"github.com/azylman/aerial/services/haro/internal/store"
)

func TestDirectDB_SummaryAndRunsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	closedDB, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()

	tests := []struct {
		name    string
		ctx     context.Context
		db      *sql.DB
		wantErr bool
	}{
		{"canceled ctx", ctx, closedDB, true},
		{"nil db", context.Background(), nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.GetScheduleSummaryMetrics(tc.ctx, tc.db); (err != nil) != tc.wantErr {
				t.Errorf("store.GetScheduleSummaryMetrics() error = %v, wantErr %v", err, tc.wantErr)
			}
			if _, _, err := store.GetScheduleRunsPaginated(tc.ctx, tc.db, 50, 0, "", ""); (err != nil) != tc.wantErr {
				t.Errorf("store.GetScheduleRunsPaginated() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
