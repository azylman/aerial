package db

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"
)

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
