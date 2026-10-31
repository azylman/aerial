package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// EriLeaderLockID is the PostgreSQL session-level advisory lock key ("AERIAL_E")
// used to serialize active queue leadership across canary deployments.
const EriLeaderLockID int64 = 0x41455249414C5F45

type SQLStore struct {
	db DBTX
}

var ErrNilDatabase = errors.New("database is nil")

func (s *SQLStore) checkDB() error {
	if s == nil || isDBTXNil(s.db) {
		return ErrNilDatabase
	}
	return nil
}

func isDBTXNil(d DBTX) bool {
	if d == nil {
		return true
	}
	v := reflect.ValueOf(d)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, d)
}

const defaultExecTimeout = 5 * time.Second

func (s *SQLStore) execWithTimeout(ctx context.Context, query string, args ...any) error {
	if err := s.checkDB(); err != nil {
		return err
	}
	queryCtx, cancel := withTimeout(ctx, defaultExecTimeout)
	defer cancel()
	_, err := s.db.ExecContext(queryCtx, query, args...)
	return err
}

func scanNonEmptyStrings(rows *sql.Rows) ([]string, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var res []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if s != "" {
			res = append(res, s)
		}
	}
	return res, rows.Err()
}

func NewSQLStore(database *sql.DB) Store {
	if database == nil {
		return nil
	}
	return &SQLStore{db: database}
}

func isPostgresDriver(d driver.Driver) bool {
	if d == nil {
		return false
	}
	return strings.Contains(reflect.TypeOf(d).String(), "stdlib.Driver")
}

// AcquireLeaderLock acquires a PostgreSQL session-level advisory lock on a dedicated
// connection and returns an idempotent release closure that unlocks and closes the connection.
// For non-PostgreSQL drivers (such as SQLite in unit tests), it returns a no-op release function.
func (s *SQLStore) AcquireLeaderLock(ctx context.Context) (func(), error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sqlDB, ok := s.db.(*sql.DB)
	if !ok {
		return func() {}, ctx.Err()
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if !isPostgresDriver(sqlDB.Driver()) {
		return func() {}, conn.Close()
	}
	lockQuery := fmt.Sprintf("SELECT pg_advisory_lock(%d)", EriLeaderLockID)
	if _, err := conn.ExecContext(ctx, lockQuery); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	unlockQuery := fmt.Sprintf("SELECT pg_advisory_unlock(%d)", EriLeaderLockID)
	var once sync.Once
	return func() {
		once.Do(func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), defaultExecTimeout)
			defer cancel()
			_, unlockErr := conn.ExecContext(unlockCtx, unlockQuery)
			if closeErr := errors.Join(unlockErr, conn.Close()); closeErr != nil {
				return
			}
		})
	}, nil
}
