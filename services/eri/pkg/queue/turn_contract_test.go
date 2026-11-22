package queue

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTurnRunnerServiceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		agent string
		want  string
	}{
		{"empty", "", "TurnRunner"},
		{"aerial", "aerial", "TurnRunner"},
		{"mixed case with spaces", "  AERIAL ", "TurnRunner"},
		{"miorine", "miorine", "TurnRunner-miorine"},
		{"suletta", "Suletta", "TurnRunner-suletta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, TurnRunnerServiceName(tc.agent))
		})
	}
}

func TestClassifyStoreError(t *testing.T) {
	t.Parallel()

	pg := func(code string) error { return &pgconn.PgError{Code: code} }
	tests := []struct {
		name         string
		err          error
		wantNil      bool
		wantTerminal bool
	}{
		{"nil", nil, true, false},
		{"unique violation", pg("23505"), false, true},
		{"fk violation", pg("23503"), false, true},
		{"invalid text", pg("22P02"), false, true},
		{"undefined column", pg("42703"), false, true},
		{"undefined table", pg("42P01"), false, true},
		{"connection failure", pg("08006"), false, false},
		{"admin shutdown", pg("57P01"), false, false},
		{"serialization", pg("40001"), false, false},
		{"deadlock", pg("40P01"), false, false},
		{"deadline", context.DeadlineExceeded, false, false},
		{"net op error", &net.OpError{Op: "dial", Err: errors.New("refused")}, false, false},
		{"plain error", errors.New("boom"), false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyStoreError(tc.err)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			assert.Equal(t, tc.wantTerminal, restate.IsTerminalError(got))
			if tc.wantTerminal {
				assert.Equal(t, restate.Code(500), restate.AsTerminalError(got).Code())
			}
		})
	}
}

func TestDeriveTurnID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key, invID string
		want       string
	}{
		{"simple", "t1", "inv1", "7a8e64dc-f615-5dea-bbe0-3d98f07dc6f0"},
		{"thread", "thread-9", "inv_abc", "24d80325-c57c-52c8-8efd-d46ee1f7c56e"},
		{"empty", "", "", "61380725-a3e6-56b9-a657-dddcdd89060a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, deriveTurnID(tc.key, tc.invID))
		})
	}
}
