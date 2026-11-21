package pgutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	// pgx database/sql driver backing OpenSQL.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/azylman/aerial/pkg/httpkit"
)

// SQLOptions tunes the *sql.DB pool OpenSQL returns. A zero field keeps the
// database/sql default.
type SQLOptions struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// connector holds the connection retry policy OpenSQL applies.
type connector struct {
	driver      string
	maxAttempts int
	retryBase   time.Duration
	pingTimeout time.Duration
	after       func(time.Duration) <-chan time.Time
}

// defaultConnector is the policy OpenSQL uses; pgutil's own tests build
// connectors with a fake driver and an immediate retry clock instead.
var defaultConnector = connector{
	driver:      "pgx",
	maxAttempts: 10,
	retryBase:   500 * time.Millisecond,
	pingTimeout: 5 * time.Second,
	after:       time.After,
}

const maxRetryBackoff = 5 * time.Second

// OpenSQL opens a PostgreSQL database/sql handle through the pgx driver. The
// DSN is trimmed and must use the postgres:// or postgresql:// scheme. Each of
// up to 10 attempts opens and pings with a 5s timeout; waits between attempts
// start at 500ms and grow x1.5 up to 5s. Cancelling ctx stops retrying.
func OpenSQL(ctx context.Context, dsn string, opts SQLOptions) (*sql.DB, error) {
	return defaultConnector.open(ctx, dsn, opts)
}

func (c connector) open(ctx context.Context, dsn string, opts SQLOptions) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return nil, errors.New("connection string cannot be empty")
	}
	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		return nil, fmt.Errorf("unsupported database scheme in %q: only postgres:// or postgresql:// supported", RedactDSN(trimmed))
	}

	var database *sql.DB
	var err error
	backoff := c.retryBase
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		database, err = c.ping(ctx, trimmed)
		if err == nil {
			break
		}
		log.Printf("[pgutil] Waiting for PostgreSQL (attempt %d/%d): %v", attempt, c.maxAttempts, err)
		if attempt == c.maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("gave up waiting for PostgreSQL after %d attempts: %w", attempt, errors.Join(err, ctx.Err()))
		case <-c.after(backoff):
		}
		backoff = nextBackoff(backoff)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", c.maxAttempts, err)
	}

	applySQLOptions(database, opts)
	log.Printf("[pgutil] PostgreSQL connected at %s", RedactDSN(trimmed))
	return database, nil
}

// ping runs one open+ping attempt, closing the handle when the ping fails.
func (c connector) ping(ctx context.Context, dsn string) (*sql.DB, error) {
	database, err := sql.Open(c.driver, dsn)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, c.pingTimeout)
	defer cancel()
	if err := database.PingContext(pingCtx); err != nil {
		httpkit.CloseWarn(database, "database on ping failure")
		return nil, err
	}
	return database, nil
}

// nextBackoff grows a retry wait by 1.5x, capped at 5s.
func nextBackoff(d time.Duration) time.Duration {
	return min(time.Duration(float64(d)*1.5), maxRetryBackoff)
}

func applySQLOptions(database *sql.DB, opts SQLOptions) {
	if opts.MaxOpenConns != 0 {
		database.SetMaxOpenConns(opts.MaxOpenConns)
	}
	if opts.MaxIdleConns != 0 {
		database.SetMaxIdleConns(opts.MaxIdleConns)
	}
	if opts.ConnMaxLifetime != 0 {
		database.SetConnMaxLifetime(opts.ConnMaxLifetime)
	}
	if opts.ConnMaxIdleTime != 0 {
		database.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	}
}

// RedactDSN returns a log-safe form of a database DSN: query string and fragment
// are dropped, any userinfo password is masked, and unparseable or host-less
// inputs (including libpq key=value DSNs) collapse to "[redacted]".
func RedactDSN(dsn string) string {
	u, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil || u.Host == "" {
		return "[redacted]"
	}
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.Redacted()
}
