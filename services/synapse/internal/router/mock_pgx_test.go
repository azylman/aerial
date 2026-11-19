package router

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type mockSynapsePgxRow struct {
	scanFn func(dest ...any) error
}

func (r mockSynapsePgxRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type mockSynapsePgxRows struct {
	pgx.Rows

	intItems  []int
	idx       int
	scanErr   error
	rowsErr   error
	totalRows int
	scanRowFn func(idx int, dest ...any) error
}

func (*mockSynapsePgxRows) Close()       {}
func (r *mockSynapsePgxRows) Err() error { return r.rowsErr }
func (r *mockSynapsePgxRows) Next() bool {
	limit := max(r.totalRows, len(r.intItems))
	if r.idx < limit {
		r.idx++
		return true
	}
	return false
}

func (r *mockSynapsePgxRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.scanRowFn != nil {
		return r.scanRowFn(r.idx-1, dest...)
	}
	if len(r.intItems) > 0 {
		return scanValues(dest, r.intItems[r.idx-1])
	}
	return nil
}

func scanValues(dest []any, vals ...any) error {
	if len(vals) > len(dest) {
		return fmt.Errorf("scan: %d values for %d destinations", len(vals), len(dest))
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return fmt.Errorf("scan column %d: destination %T cannot hold %T", i, dest[i], v)
		}
		ptr.Elem().Set(val)
	}
	return nil
}

type mockSynapsePgxPool struct {
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (p *mockSynapsePgxPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if p.queryRowFn != nil {
		return p.queryRowFn(ctx, sql, args...)
	}
	return mockSynapsePgxRow{}
}

func (p *mockSynapsePgxPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.execFn != nil {
		return p.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (p *mockSynapsePgxPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.queryFn != nil {
		return p.queryFn(ctx, sql, args...)
	}
	return &mockSynapsePgxRows{}, nil
}
