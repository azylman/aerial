package db

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"
)

// SQLStore wraps a DBTX instance (either *sql.DB connection pool or *sql.Tx) and implements the Store interface.
type SQLStore struct {
	db         DBTX
	isPostgres bool
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

// NewSQLStore creates a new Store instance wrapping the provided *sql.DB connection.
func NewSQLStore(database *sql.DB) Store {
	if database == nil {
		return nil
	}
	return &SQLStore{
		db:         database,
		isPostgres: isPostgres(database),
	}
}

func (s *SQLStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if closer, ok := s.db.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
