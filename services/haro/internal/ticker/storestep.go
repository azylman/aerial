package ticker

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
)

// storeStepTimeout bounds every Postgres call made inside a restate.Run step
// so a hung query becomes a retryable error instead of a stuck step.
const storeStepTimeout = 5 * time.Second

// classifyStoreError makes deterministic store failures terminal (runbook 3.4):
// not-found is 404 and constraint/data/syntax-class SQLSTATEs are 400. Connection
// errors, timeouts, serialization failures and deadlocks stay retryable.
func classifyStoreError(err error) error {
	if err == nil || restate.IsTerminalError(err) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusNotFound))
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && len(pgErr.Code) >= 2 {
		switch strings.ToUpper(pgErr.Code[:2]) {
		case "22", "23", "42", "0A", "2B", "2D", "25":
			return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusBadRequest))
		}
	}
	return err
}

// withStoreTimeout runs a store call under storeStepTimeout and classifies its error.
func withStoreTimeout[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	tctx, cancel := context.WithTimeout(ctx, storeStepTimeout)
	defer cancel()
	out, err := fn(tctx)
	return out, classifyStoreError(err)
}
