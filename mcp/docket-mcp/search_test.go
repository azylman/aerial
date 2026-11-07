package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type testConnector struct {
	conn *testConn
}

func (c *testConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (c *testConnector) Driver() driver.Driver {
	return &testDriver{}
}

type testDriver struct{}

func (d *testDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("open not supported")
}

type testConn struct {
	queryFn func(query string, args []driver.NamedValue) (driver.Rows, error)
	execFn  func(query string, args []driver.NamedValue) (driver.Result, error)
}

func (c *testConn) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (c *testConn) Close() error                              { return nil }
func (c *testConn) Begin() (driver.Tx, error)                 { return nil, nil }

func (c *testConn) CheckNamedValue(nv *driver.NamedValue) error {
	return nil
}

func (c *testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.queryFn != nil {
		return c.queryFn(query, args)
	}
	return nil, errors.New("unexpected query")
}

func (c *testConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.execFn != nil {
		return c.execFn(query, args)
	}
	return driver.RowsAffected(1), nil
}

type testRows struct {
	columns []string
	values  [][]driver.Value
	idx     int
}

func (r *testRows) Columns() []string { return r.columns }
func (r *testRows) Close() error      { return nil }
func (r *testRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.idx])
	r.idx++
	return nil
}

func newTestDB(t *testing.T, conn *testConn) *sql.DB {
	t.Helper()
	db := sql.OpenDB(&testConnector{conn: conn})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLTaskStore_UpdateTaskEmbedding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		taskID       int64
		embedding    []float32
		execErr      error
		wantExec     bool
		wantQuerySub string
		wantArgCount int
		wantErr      string
	}{
		{
			name:         "with embedding updates embedding",
			taskID:       1,
			embedding:    make([]float32, 384),
			execErr:      nil,
			wantExec:     true,
			wantQuerySub: "UPDATE tasks SET embedding = $1 WHERE id = $2;",
			wantArgCount: 2,
			wantErr:      "",
		},
		{
			name:         "empty embedding returns immediately without query",
			taskID:       2,
			embedding:    nil,
			execErr:      nil,
			wantExec:     false,
			wantQuerySub: "",
			wantArgCount: 0,
			wantErr:      "",
		},
		{
			name:         "exec failure returns error",
			taskID:       3,
			embedding:    make([]float32, 384),
			execErr:      errors.New("db error"),
			wantExec:     true,
			wantQuerySub: "UPDATE tasks SET embedding = $1 WHERE id = $2;",
			wantArgCount: 2,
			wantErr:      "failed to update task embedding: db error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var execCount int
			var calledQuery string
			var calledArgs []driver.NamedValue
			conn := &testConn{
				execFn: func(query string, args []driver.NamedValue) (driver.Result, error) {
					execCount++
					calledQuery = query
					calledArgs = args
					if tc.execErr != nil {
						return nil, tc.execErr
					}
					return driver.RowsAffected(1), nil
				},
			}
			db := newTestDB(t, conn)
			store := NewSQLTaskStore(db)

			err := store.UpdateTaskEmbedding(context.Background(), tc.taskID, tc.embedding)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if tc.wantExec && execCount == 0 {
				t.Fatalf("expected ExecContext to be called")
			}
			if !tc.wantExec && execCount > 0 {
				t.Fatalf("expected ExecContext NOT to be called, called %d times", execCount)
			}
			if tc.wantQuerySub != "" && !strings.Contains(calledQuery, tc.wantQuerySub) {
				t.Fatalf("query %q does not contain %q", calledQuery, tc.wantQuerySub)
			}
			if tc.wantArgCount > 0 && len(calledArgs) != tc.wantArgCount {
				t.Fatalf("args len = %d, want %d", len(calledArgs), tc.wantArgCount)
			}
		})
	}
}

func TestSQLTaskStore_UpdateTaskEmbeddingAndResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		taskID       int64
		embedding    []float32
		res          string
		execErr      error
		wantQuerySub string
		wantArgCount int
		wantErr      string
	}{
		{
			name:         "with embedding updates embedding and resolution",
			taskID:       1,
			embedding:    make([]float32, 384),
			res:          "completed",
			execErr:      nil,
			wantQuerySub: "embedding = $1, resolution = $2",
			wantArgCount: 3,
			wantErr:      "",
		},
		{
			name:         "without embedding updates resolution only",
			taskID:       2,
			embedding:    nil,
			res:          "wont_do",
			execErr:      nil,
			wantQuerySub: "resolution = $1 WHERE id = $2",
			wantArgCount: 2,
			wantErr:      "",
		},
		{
			name:         "exec failure returns error",
			taskID:       3,
			embedding:    make([]float32, 384),
			res:          "",
			execErr:      errors.New("db disk failure"),
			wantQuerySub: "embedding = $1, resolution = $2",
			wantArgCount: 3,
			wantErr:      "failed to update task embedding: db disk failure",
		},
		{
			name:         "exec failure without embedding returns error",
			taskID:       4,
			embedding:    nil,
			res:          "completed",
			execErr:      errors.New("exec error"),
			wantQuerySub: "resolution = $1 WHERE id = $2",
			wantArgCount: 2,
			wantErr:      "failed to update task resolution: exec error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calledQuery string
			var calledArgs []driver.NamedValue
			conn := &testConn{
				execFn: func(query string, args []driver.NamedValue) (driver.Result, error) {
					calledQuery = query
					calledArgs = args
					if tc.execErr != nil {
						return nil, tc.execErr
					}
					return driver.RowsAffected(1), nil
				},
			}
			db := newTestDB(t, conn)
			store := NewSQLTaskStore(db)

			err := store.UpdateTaskEmbeddingAndResolution(context.Background(), tc.taskID, tc.embedding, tc.res)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if tc.wantQuerySub != "" && !strings.Contains(calledQuery, tc.wantQuerySub) {
				t.Fatalf("query %q does not contain %q", calledQuery, tc.wantQuerySub)
			}
			if tc.wantArgCount > 0 && len(calledArgs) != tc.wantArgCount {
				t.Fatalf("args len = %d, want %d", len(calledArgs), tc.wantArgCount)
			}
		})
	}
}

func TestSQLTaskStore_UpdateTaskResolutionBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		taskIDs      []int64
		res          string
		execErr      error
		wantExec     bool
		wantQuerySub string
		wantErr      string
	}{
		{
			name:         "empty slice returns immediately without query",
			taskIDs:      nil,
			res:          "wont_do",
			wantExec:     false,
			wantQuerySub: "",
			wantErr:      "",
		},
		{
			name:         "batch updates successfully",
			taskIDs:      []int64{1, 2, 3},
			res:          "wont_do",
			wantExec:     true,
			wantQuerySub: "UPDATE tasks SET resolution = $1 WHERE id = ANY($2);",
			wantErr:      "",
		},
		{
			name:         "exec error returned properly",
			taskIDs:      []int64{10},
			res:          "completed",
			execErr:      errors.New("batch failure"),
			wantExec:     true,
			wantQuerySub: "UPDATE tasks SET resolution = $1 WHERE id = ANY($2);",
			wantErr:      "failed to batch update task resolutions: batch failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var execCount int
			var calledQuery string
			conn := &testConn{
				execFn: func(query string, args []driver.NamedValue) (driver.Result, error) {
					execCount++
					calledQuery = query
					if tc.execErr != nil {
						return nil, tc.execErr
					}
					return driver.RowsAffected(int64(len(tc.taskIDs))), nil
				},
			}
			db := newTestDB(t, conn)
			store := NewSQLTaskStore(db)

			err := store.UpdateTaskResolutionBatch(context.Background(), tc.taskIDs, tc.res)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if tc.wantExec && execCount == 0 {
				t.Fatalf("expected ExecContext to be called")
			}
			if !tc.wantExec && execCount > 0 {
				t.Fatalf("expected ExecContext NOT to be called, was called %d times", execCount)
			}
			if tc.wantQuerySub != "" && !strings.Contains(calledQuery, tc.wantQuerySub) {
				t.Fatalf("query %q does not contain %q", calledQuery, tc.wantQuerySub)
			}
		})
	}
}

func TestSQLTaskStore_FetchTaskResolutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		taskIDs  []int64
		rows     *testRows
		queryErr error
		wantMap  map[int64]string
		wantErr  string
	}{
		{
			name:    "empty IDs returns empty map",
			taskIDs: nil,
			wantMap: map[int64]string{},
		},
		{
			name:    "successful fetch of resolutions",
			taskIDs: []int64{1, 2},
			rows: &testRows{
				columns: []string{"id", "resolution"},
				values: [][]driver.Value{
					{int64(1), "completed"},
					{int64(2), "wont_do"},
				},
			},
			wantMap: map[int64]string{
				1: "completed",
				2: "wont_do",
			},
		},
		{
			name:     "query error returned",
			taskIDs:  []int64{3},
			queryErr: errors.New("query error"),
			wantErr:  "failed to fetch task resolutions: query error",
		},
		{
			name:    "scan error on wrong columns",
			taskIDs: []int64{4},
			rows: &testRows{
				columns: []string{"id"},
				values: [][]driver.Value{
					{int64(4)},
				},
			},
			wantErr: "failed to scan task resolution",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := &testConn{
				queryFn: func(query string, args []driver.NamedValue) (driver.Rows, error) {
					if tc.queryErr != nil {
						return nil, tc.queryErr
					}
					return tc.rows, nil
				},
			}
			db := newTestDB(t, conn)
			store := NewSQLTaskStore(db)

			got, err := store.FetchTaskResolutions(context.Background(), tc.taskIDs)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			for k, v := range tc.wantMap {
				if got[k] != v {
					t.Fatalf("got[%d] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestSQLTaskStore_SearchTasksRRF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		embedding     []float32
		queryText     string
		status        string
		limit         int
		rows          *testRows
		queryErr      error
		wantCount     int
		wantLimitArg  int
		wantFilterArg string
		wantVectorArg bool
		wantErr       string
	}{
		{
			name:      "successful search with rows",
			embedding: make([]float32, 384),
			queryText: "fix bug",
			status:    "open",
			limit:     5,
			rows: &testRows{
				columns: []string{"id", "title", "done", "resolution", "score"},
				values: [][]driver.Value{
					{int64(1), "Fix bug", false, "", 0.95},
					{int64(2), "Closed task", true, "completed", 0.70},
				},
			},
			wantCount:     2,
			wantLimitArg:  5,
			wantFilterArg: "open",
			wantVectorArg: true,
		},
		{
			name:      "empty embedding uses default vector and vector gate false",
			embedding: nil,
			queryText: "search term",
			status:    "",
			limit:     0,
			rows: &testRows{
				columns: []string{"id", "title", "done", "resolution", "score"},
				values:  [][]driver.Value{},
			},
			wantCount:     0,
			wantLimitArg:  10,
			wantFilterArg: "open",
			wantVectorArg: false,
		},
		{
			name:      "query error handled",
			embedding: make([]float32, 384),
			queryText: "query",
			queryErr:  errors.New("db timeout"),
			wantErr:   "hybrid rrf search failed: db timeout",
		},
		{
			name:      "scan error handled",
			embedding: make([]float32, 384),
			queryText: "query",
			rows: &testRows{
				columns: []string{"id"},
				values: [][]driver.Value{
					{int64(1)},
				},
			},
			wantErr: "failed to scan scored task row",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calledArgs []driver.NamedValue
			conn := &testConn{
				queryFn: func(query string, args []driver.NamedValue) (driver.Rows, error) {
					calledArgs = args
					if tc.queryErr != nil {
						return nil, tc.queryErr
					}
					return tc.rows, nil
				},
			}
			db := newTestDB(t, conn)
			store := NewSQLTaskStore(db)

			results, err := store.SearchTasksRRF(context.Background(), tc.embedding, tc.queryText, tc.status, tc.limit)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if len(results) != tc.wantCount {
				t.Fatalf("results len = %d, want %d", len(results), tc.wantCount)
			}
			if tc.wantLimitArg > 0 && len(calledArgs) > 1 {
				if calledArgs[1].Value != tc.wantLimitArg {
					t.Fatalf("limit arg = %v, want %d", calledArgs[1].Value, tc.wantLimitArg)
				}
			}
			if tc.wantFilterArg != "" && len(calledArgs) > 3 {
				if calledArgs[3].Value != tc.wantFilterArg {
					t.Fatalf("filter arg = %v, want %q", calledArgs[3].Value, tc.wantFilterArg)
				}
			}
			if len(calledArgs) > 4 {
				if calledArgs[4].Value != tc.wantVectorArg {
					t.Fatalf("hasVector arg = %v, want %v", calledArgs[4].Value, tc.wantVectorArg)
				}
			}
		})
	}
}
