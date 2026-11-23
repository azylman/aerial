package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"
)

// DefaultAgent is the fallback agent tenant identifier when none is explicitly specified.
const DefaultAgent = "aerial"

// ResolveAgentName returns candidate if non-empty (trimmed), else checks the AGENT_NAME
// environment variable, defaulting to DefaultAgent ("aerial") if neither is set.
func ResolveAgentName(candidate string) string {
	if trimmed := strings.TrimSpace(candidate); trimmed != "" {
		return trimmed
	}
	if env := strings.TrimSpace(os.Getenv("AGENT_NAME")); env != "" {
		return env
	}
	return DefaultAgent
}

type SQLStore struct {
	db    DBTX
	agent string
}

// WithAgent sets the tenant agent slug for SQLStore queries.
func (s *SQLStore) WithAgent(agent string) *SQLStore {
	if s == nil {
		return nil
	}
	s.agent = strings.TrimSpace(agent)
	return s
}

// effectiveAgent returns the configured agent slug or defaults to DefaultAgent.
func (s *SQLStore) effectiveAgent() string {
	if s == nil {
		return DefaultAgent
	}
	return ResolveAgentName(s.agent)
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
	_, err := s.execResultWithTimeout(ctx, query, args...)
	return err
}

func (s *SQLStore) execResultWithTimeout(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := s.checkDB(); err != nil {
		return nil, err
	}
	queryCtx, cancel := withTimeout(ctx, defaultExecTimeout)
	defer cancel()
	return s.db.ExecContext(queryCtx, query, args...)
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

func NewSQLStore(database *sql.DB) *SQLStore {
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
