package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"

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

var _ pgxQueryExecer = (*mockSynapsePgxPool)(nil)

func TestExtractJobsFromMetadata_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		metadata map[string]any
		expected []string
	}{
		{name: "nil metadata", metadata: nil, expected: nil},
		{name: "jobs array string", metadata: map[string]any{"jobs": []any{"brain", "scheduler-mcp"}}, expected: []string{"brain", "scheduler-mcp"}},
		{name: "deduplicates clean job names", metadata: map[string]any{"jobs": []any{"brain", "brain", "hangar"}}, expected: []string{"brain", "hangar"}},
		{name: "empty jobs array", metadata: map[string]any{"jobs": []any{}}, expected: nil},
		{name: "non-slice jobs", metadata: map[string]any{"jobs": 123}, expected: nil},
		{name: "typed string slice", metadata: map[string]any{"jobs": []string{"job-a", "job-b"}}, expected: []string{"job-a", "job-b"}},
		{name: "whitespace and empty jobs filtered", metadata: map[string]any{"jobs": []any{"", "  ", "brain"}}, expected: []string{"brain"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractJobsFromMetadata(tt.metadata)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("extractJobsFromMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestPostgresPRRegistryAlias(t *testing.T) {
	t.Parallel()
	reg := NewPostgresPRRegistry(nil)
	if reg == nil {
		t.Fatal("expected non-nil registry from alias")
	}
}

func TestGitHubStatusErrorCoverage(t *testing.T) {
	t.Parallel()
	err := &gitHubStatusError{StatusCode: 404, Message: "not found"}
	if err.Error() != "not found" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
	if !isGitHubNotFound(err) {
		t.Fatal("expected true for 404 error")
	}
	if isGitHubNotFound(&gitHubStatusError{StatusCode: 500}) {
		t.Fatal("expected false for 500 error")
	}
	if isGitHubNotFound(nil) {
		t.Fatal("expected false for nil error")
	}
}
