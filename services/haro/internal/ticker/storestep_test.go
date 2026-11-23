package ticker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
)

func TestClassifyStoreError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantErr  bool
		wantCode int
	}{
		{name: "nil stays nil"},
		{name: "not found is terminal 404", err: fmt.Errorf("cron schedule %q not found: %w", "x", sql.ErrNoRows), wantErr: true, wantCode: http.StatusNotFound},
		{name: "unique violation is terminal 400", err: &pgconn.PgError{Code: "23505"}, wantErr: true, wantCode: http.StatusBadRequest},
		{name: "invalid text representation is terminal 400", err: fmt.Errorf("scan: %w", &pgconn.PgError{Code: "22P02"}), wantErr: true, wantCode: http.StatusBadRequest},
		{name: "undefined column is terminal 400", err: &pgconn.PgError{Code: "42703"}, wantErr: true, wantCode: http.StatusBadRequest},
		{name: "serialization failure stays retryable", err: &pgconn.PgError{Code: "40001"}, wantErr: true},
		{name: "deadlock stays retryable", err: &pgconn.PgError{Code: "40P01"}, wantErr: true},
		{name: "connection failure stays retryable", err: &pgconn.PgError{Code: "08006"}, wantErr: true},
		{name: "timeout stays retryable", err: context.DeadlineExceeded, wantErr: true},
		{name: "existing terminal keeps its code", err: restate.ToTerminalError(errors.New("gone"), restate.WithErrorCode(http.StatusPreconditionFailed)), wantErr: true, wantCode: http.StatusPreconditionFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyStoreError(tc.err)
			if (got != nil) != tc.wantErr {
				t.Fatalf("classifyStoreError(%v) = %v, wantErr %v", tc.err, got, tc.wantErr)
			}
			if code := terminalCode(got); code != tc.wantCode {
				t.Errorf("terminal code = %d, want %d", code, tc.wantCode)
			}
		})
	}
}
