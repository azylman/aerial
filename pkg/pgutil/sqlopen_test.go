package pgutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDriver fails the first failures[dsn] connection opens for a DSN, then
// hands out no-op connections.
type fakeDriver struct {
	mu       sync.Mutex
	failures map[string]int
}

func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failures[name] > 0 {
		d.failures[name]--
		return nil, errors.New("fake: connection refused")
	}
	return fakeConn{}, nil
}

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("fake: no statements") }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("fake: no transactions") }

var testDriver = &fakeDriver{failures: map[string]int{
	"postgres://retry@db/app":     2,
	"postgres://exhausted@db/app": 100,
	"postgres://cancel@db/app":    100,
}}

func init() {
	sql.Register("pgutil-fake", testDriver)
}

func immediateAfter(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- time.Time{}
	return ch
}

func neverAfter(time.Duration) <-chan time.Time { return nil }

func TestConnectorOpen(t *testing.T) {
	t.Parallel()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name         string
		driver       string
		ctx          context.Context
		after        func(time.Duration) <-chan time.Time
		dsn          string
		opts         SQLOptions
		wantErr      string
		wantMaxOpen  int
		wantCanceled bool
	}{
		{name: "empty", driver: "pgutil-fake", ctx: context.Background(), after: immediateAfter, dsn: "  ", wantErr: "connection string cannot be empty"},
		{name: "bad scheme redacts dsn", driver: "pgutil-fake", ctx: context.Background(), after: immediateAfter, dsn: "mysql://u:hunter2@db/app?password=x", wantErr: `unsupported database scheme in "mysql://u:xxxxx@db/app": only postgres:// or postgresql:// supported`},
		{name: "first attempt succeeds with defaults", driver: "pgutil-fake", ctx: context.Background(), after: immediateAfter, dsn: " postgresql://ok@db/app ", wantMaxOpen: 0},
		{name: "retry then success applies options", driver: "pgutil-fake", ctx: context.Background(), after: immediateAfter, dsn: "postgres://retry@db/app", opts: SQLOptions{MaxOpenConns: 10, MaxIdleConns: 5, ConnMaxLifetime: 30 * time.Minute, ConnMaxIdleTime: 5 * time.Minute}, wantMaxOpen: 10},
		{name: "exhausted retries", driver: "pgutil-fake", ctx: context.Background(), after: immediateAfter, dsn: "postgres://exhausted@db/app", wantErr: "failed to connect to PostgreSQL after 3 attempts: fake: connection refused"},
		{name: "unknown driver exhausts retries", driver: "pgutil-missing", ctx: context.Background(), after: immediateAfter, dsn: "postgres://ok@db/app", wantErr: `failed to connect to PostgreSQL after 3 attempts: sql: unknown driver "pgutil-missing"`},
		{name: "ctx cancel stops retrying", driver: "pgutil-fake", ctx: cancelled, after: neverAfter, dsn: "postgres://cancel@db/app", wantErr: "gave up waiting for PostgreSQL after 1 attempts", wantCanceled: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := connector{driver: tc.driver, maxAttempts: 3, retryBase: time.Millisecond, pingTimeout: time.Second, after: tc.after}
			db, err := c.open(tc.ctx, tc.dsn, tc.opts)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("open() err = %v, want substring %q", err, tc.wantErr)
				}
				if got := errors.Is(err, context.Canceled); got != tc.wantCanceled {
					t.Fatalf("errors.Is(err, context.Canceled) = %v, want %v", got, tc.wantCanceled)
				}
				if db != nil {
					t.Fatalf("open() db = %v, want nil on error", db)
				}
				return
			}
			if err != nil {
				t.Fatalf("open() err = %v", err)
			}
			defer func() { _ = db.Close() }()
			if got := db.Stats().MaxOpenConnections; got != tc.wantMaxOpen {
				t.Fatalf("MaxOpenConnections = %d, want %d", got, tc.wantMaxOpen)
			}
		})
	}
}

func TestOpenSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		wantErr string
	}{
		{name: "empty", dsn: "", wantErr: "connection string cannot be empty"},
		{name: "sqlite scheme rejected", dsn: "sqlite:///tmp/x.db", wantErr: "unsupported database scheme"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := OpenSQL(context.Background(), tc.dsn, SQLOptions{}); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("OpenSQL() err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestNextBackoff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "base grows 1.5x", in: 500 * time.Millisecond, want: 750 * time.Millisecond},
		{name: "mid grows 1.5x", in: 2 * time.Second, want: 3 * time.Second},
		{name: "capped at 5s", in: 4 * time.Second, want: 5 * time.Second},
		{name: "cap holds", in: 5 * time.Second, want: 5 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := nextBackoff(tc.in); got != tc.want {
				t.Fatalf("nextBackoff(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestRedactDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{"query removed", "postgresql://app:hunter2@db:5432/app?sslmode=disable&password=hunter2", "postgresql://app:xxxxx@db:5432/app"},
		{"empty", "", "[redacted]"},
		{"key value dsn", "host=db password=hunter2", "[redacted]"},
		{"unparseable", "postgres://[::1", "[redacted]"},
		{"whitespace trimmed", "  postgres://app:hunter2@db:5432/app#frag \n", "postgres://app:xxxxx@db:5432/app"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RedactDSN(tc.dsn); got != tc.want {
				t.Errorf("RedactDSN(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}
