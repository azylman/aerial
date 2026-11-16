package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// bareStore pins a store value with no usable database handle (nil receiver or nil db).
type bareStore struct{ store *PostgresVanadisStore }

// dbScript declaratively describes how a mocked Postgres connection answers a store call.
type dbScript struct {
	bare     *bareStore
	row      pgx.Row
	chunks   []VanadisChunk
	queryErr error
	scanErr  error
	rowsErr  error
}

func (s dbScript) open() (*PostgresVanadisStore, *[]string) {
	candidates := new([]string)
	if s.bare != nil {
		return s.bare.store, candidates
	}
	queryRow := func(_ context.Context, query string, args ...any) pgx.Row {
		return s.row
	}
	query := func(context.Context, string, ...any) (pgx.Rows, error) {
		if s.queryErr != nil {
			return nil, s.queryErr
		}
		return &mockRows{items: s.chunks, scanErr: s.scanErr, rowsErr: s.rowsErr}, nil
	}
	return newMockStore(mockDB{
		queryRowFn: queryRow,
		queryFn:    query,
		execFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SELECT 1"), nil
		},
	}), candidates
}

var (
	nilReceiver = &bareStore{}
	nilDB       = &bareStore{store: &PostgresVanadisStore{}}
)

func errRow(err error) *mockRow { return &mockRow{err: err} }

type genericMockRow struct {
	scanFn func(dest ...any) error
}

func (r *genericMockRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

func scanValues(dest []any, vals ...any) error {
	if len(vals) > len(dest) {
		return errors.New("too many values for scan destination")
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		switch d := dest[i].(type) {
		case *string:
			if s, ok := v.(string); ok {
				*d = s
			}
		case *int:
			if n, ok := v.(int); ok {
				*d = n
			}
		case *bool:
			if b, ok := v.(bool); ok {
				*d = b
			}
		case *sql.NullTime:
			if nt, ok := v.(sql.NullTime); ok {
				*d = nt
			}
		case *time.Time:
			if t, ok := v.(time.Time); ok {
				*d = t
			}
		case *[]string:
			if sl, ok := v.([]string); ok {
				*d = sl
			}
		case *[]byte:
			if bs, ok := v.([]byte); ok {
				*d = bs
			}
		default:
			// ignore or handle if needed
		}
	}
	return nil
}

func assertStoreErr(t *testing.T, err, wantErrIs error, wantErr bool) bool {
	t.Helper()
	switch {
	case wantErrIs != nil:
		if !errors.Is(err, wantErrIs) {
			t.Fatalf("err = %v, want %v", err, wantErrIs)
		}
		return true
	case wantErr:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		return true
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	}
	return false
}

func TestPostgresVanadisStore_GetChunk(t *testing.T) {
	t.Parallel()
	c1 := []VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk Title"}}
	tests := []struct {
		name        string
		chunkID     string
		db          dbScript
		wantErrIs   error
		wantErr     bool
		wantChunkID string
		wantBuildID string
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "query error", chunkID: "c1", db: dbScript{queryErr: errors.New("query err")}, wantErr: true},
		{name: "rows error", chunkID: "c1", db: dbScript{chunks: c1, rowsErr: errors.New("rows err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{chunks: []VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "build not found", chunkID: "c1", db: dbScript{chunks: c1, row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build query error", chunkID: "c1", db: dbScript{chunks: c1, row: errRow(errors.New("build query err"))}, wantErr: true},
		{name: "success", chunkID: "c1", db: dbScript{chunks: c1, row: &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build Title"}}}, wantChunkID: "c1", wantBuildID: "b1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			c, b, err := store.GetChunk(context.Background(), tc.chunkID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if c.ID != tc.wantChunkID || b.ID != tc.wantBuildID {
				t.Errorf("GetChunk = chunk %q build %q, want chunk %q build %q", c.ID, b.ID, tc.wantChunkID, tc.wantBuildID)
			}
		})
	}
}
