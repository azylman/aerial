package db

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"
)

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

func (s *SQLStore) execWithTimeout(ctx context.Context, d time.Duration, query string, args ...any) (sql.Result, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	if d <= 0 {
		d = 5 * time.Second
	}
	queryCtx, cancel := withTimeout(ctx, d)
	defer cancel()
	return s.db.ExecContext(queryCtx, query, args...)
}

func (s *SQLStore) execRequireOneRow(ctx context.Context, d time.Duration, query string, notFoundErr error, args ...any) error {
	res, err := s.execWithTimeout(ctx, d, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		if notFoundErr != nil {
			return notFoundErr
		}
		return sql.ErrNoRows
	}
	return nil
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

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	out := make([]T, len(src))
	copy(out, src)
	return out
}

func NewSQLStore(database *sql.DB) Store {
	if database == nil {
		return nil
	}
	return &SQLStore{db: database, isPostgres: isPostgres(database)}
}

func (s *SQLStore) Close() error {
	if s != nil && s.db != nil {
		if closer, ok := s.db.(interface{ Close() error }); ok {
			return closer.Close()
		}
	}
	return nil
}
