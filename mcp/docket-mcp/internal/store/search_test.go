package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/mcp/docket-mcp/internal/config"
)

type testConnector struct {
	conn *testConn
}

func (c *testConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (*testConnector) Driver() driver.Driver {
	return &testDriver{}
}

type testDriver struct{}

func (*testDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("open not supported")
}

// sqlCall is one statement the fake connection received.
type sqlCall struct {
	query string
	args  []driver.Value
}

// testConn is a declarative database/sql driver connection: Exec fails with
// execErr, Query returns rows or queryErr, and every statement is recorded.
type testConn struct {
	execErr      error
	queryErr     error
	rows         *testRows
	rowsAffected *int64

	mu    sync.Mutex
	calls []sqlCall
}

func (*testConn) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (*testConn) Close() error                              { return nil }
func (*testConn) Begin() (driver.Tx, error)                 { return nil, nil }

func (*testConn) CheckNamedValue(nv *driver.NamedValue) error {
	return nil
}

func (c *testConn) record(query string, args []driver.NamedValue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make([]driver.Value, len(args))
	for i, a := range args {
		values[i] = a.Value
	}
	c.calls = append(c.calls, sqlCall{query: query, args: values})
}

// recordedArgs returns the args after the first (the embedding vector) of the
// single recorded statement, or nil when there is not exactly one.
func (c *testConn) recordedArgs() []driver.Value {
	calls := c.recorded()
	if len(calls) != 1 || len(calls[0].args) == 0 {
		return nil
	}
	return calls[0].args[1:]
}

func (c *testConn) recorded() []sqlCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sqlCall(nil), c.calls...)
}

func (c *testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.record(query, args)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	if c.rows == nil {
		return nil, errors.New("unexpected query")
	}
	return c.rows, nil
}

func (c *testConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.record(query, args)
	if c.execErr != nil {
		return nil, c.execErr
	}
	ra := int64(1)
	if c.rowsAffected != nil {
		ra = *c.rowsAffected
	}
	return driver.RowsAffected(ra), nil
}

type testRows struct {
	columns []string
	values  [][]driver.Value
	idx     int
}

func (r *testRows) Columns() []string { return r.columns }
func (*testRows) Close() error        { return nil }
func (r *testRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.idx])
	r.idx++
	return nil
}

// newSQLStore wraps conn in a SQLTaskStore closed via t.Cleanup.
func newSQLStore(t *testing.T, conn *testConn) *SQLTaskStore {
	t.Helper()
	db := sql.OpenDB(&testConnector{conn: conn})
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLTaskStore(db)
}

// newExecStore returns a store whose Exec calls fail with execErr (nil succeeds).
func newExecStore(t *testing.T, execErr error) (*SQLTaskStore, *testConn) {
	t.Helper()
	conn := &testConn{execErr: execErr}
	return newSQLStore(t, conn), conn
}

// newQueryStore returns a store whose Query calls return rows or queryErr.
func newQueryStore(t *testing.T, rows *testRows, queryErr error) (*SQLTaskStore, *testConn) {
	t.Helper()
	conn := &testConn{rows: rows, queryErr: queryErr}
	return newSQLStore(t, conn), conn
}

// sqlRows builds result rows with the given columns.
func sqlRows(columns []string, values ...[]driver.Value) *testRows {
	return &testRows{columns: columns, values: values}
}

// checkSQLCalls asserts conn received exactly one statement containing
// wantQuery with wantArgs args, or none when wantQuery is empty.
func checkSQLCalls(t *testing.T, conn *testConn, wantQuery string, wantArgs int) {
	t.Helper()
	calls := conn.recorded()
	if wantQuery == "" {
		if len(calls) != 0 {
			t.Fatalf("calls = %+v, want none", calls)
		}
		return
	}
	if len(calls) != 1 || !strings.Contains(calls[0].query, wantQuery) || len(calls[0].args) != wantArgs {
		t.Fatalf("calls = %+v, want one %q with %d args", calls, wantQuery, wantArgs)
	}
}

const (
	sqlEmbeddingUpdate  = "UPDATE tasks SET embedding = $1 WHERE id = $2;"
	sqlEmbeddingAndRes  = "UPDATE tasks SET embedding = $1, resolution = $2 WHERE id = $3;"
	sqlResolutionUpdate = "UPDATE tasks SET resolution = $1 WHERE id = $2;"
	sqlResolutionBatch  = "UPDATE tasks SET resolution = $1, claimed_by_agent = '', claimed_by_turn_id = '', claimed_at = NULL WHERE id = ANY($2);"
	sqlFetchResolutions = "SELECT id, COALESCE(resolution, '') FROM tasks WHERE id = ANY($1);"
	sqlFetchMeta        = "SELECT id, COALESCE(resolution, ''), COALESCE(claimed_by_agent, ''), COALESCE(claimed_by_turn_id, ''), claimed_at FROM tasks WHERE id = ANY($1);"
	sqlClaim            = "UPDATE tasks SET claimed_by_agent = $2, claimed_by_turn_id = $3, claimed_at = $4 WHERE id = $1 AND done = false AND COALESCE(claimed_by_agent, '') = '' AND COALESCE(claimed_by_turn_id, '') = '';"
	sqlUnclaim          = "UPDATE tasks SET claimed_by_agent = '', claimed_by_turn_id = '', claimed_at = NULL WHERE id = $1;"
)

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestSQLTaskStore_UpdateTaskEmbedding(t *testing.T) {
	t.Parallel()

	emb := make([]float32, 384)
	tests := []struct {
		name      string
		taskID    int64
		embedding []float32
		execErr   error
		wantQuery string
		wantArgs  int
		wantErr   string
	}{
		{"with embedding updates embedding", 1, emb, nil, sqlEmbeddingUpdate, 2, ""},
		{"empty embedding returns immediately without query", 2, nil, nil, "", 0, ""},
		{"exec failure returns error", 3, emb, errors.New("db error"), sqlEmbeddingUpdate, 2, "failed to update task embedding: db error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newExecStore(t, tc.execErr)
			if got := errText(store.UpdateTaskEmbedding(context.Background(), tc.taskID, tc.embedding)); got != tc.wantErr {
				t.Fatalf("err = %q, want %q", got, tc.wantErr)
			}
			checkSQLCalls(t, conn, tc.wantQuery, tc.wantArgs)
		})
	}
}

func TestSQLTaskStore_UpdateTaskEmbeddingAndResolution(t *testing.T) {
	t.Parallel()

	emb := make([]float32, 384)
	tests := []struct {
		name      string
		taskID    int64
		embedding []float32
		res       string
		execErr   error
		wantQuery string
		wantArgs  int
		wantErr   string
	}{
		{"with embedding updates embedding and resolution", 1, emb, "completed", nil, sqlEmbeddingAndRes, 3, ""},
		{"without embedding updates resolution only", 2, nil, "wont_do", nil, sqlResolutionUpdate, 2, ""},
		{"exec failure returns error", 3, emb, "", errors.New("db disk failure"), sqlEmbeddingAndRes, 3, "failed to update task embedding: db disk failure"},
		{"exec failure without embedding returns error", 4, nil, "completed", errors.New("exec error"), sqlResolutionUpdate, 2, "failed to update task resolution: exec error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newExecStore(t, tc.execErr)
			if got := errText(store.UpdateTaskEmbeddingAndResolution(context.Background(), tc.taskID, tc.embedding, tc.res)); got != tc.wantErr {
				t.Fatalf("err = %q, want %q", got, tc.wantErr)
			}
			checkSQLCalls(t, conn, tc.wantQuery, tc.wantArgs)
		})
	}
}

func TestSQLTaskStore_UpdateTaskResolutionBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		taskIDs   []int64
		res       string
		execErr   error
		wantQuery string
		wantErr   string
	}{
		{"empty slice returns immediately without query", nil, "wont_do", nil, "", ""},
		{"batch updates successfully", []int64{1, 2, 3}, "wont_do", nil, sqlResolutionBatch, ""},
		{"exec error returned properly", []int64{10}, "completed", errors.New("batch failure"), sqlResolutionBatch, "failed to batch update task resolutions: batch failure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newExecStore(t, tc.execErr)
			if got := errText(store.UpdateTaskResolutionBatch(context.Background(), tc.taskIDs, tc.res)); got != tc.wantErr {
				t.Fatalf("err = %q, want %q", got, tc.wantErr)
			}
			checkSQLCalls(t, conn, tc.wantQuery, 2)
		})
	}
}

func TestSQLTaskStore_FetchTaskResolutions(t *testing.T) {
	t.Parallel()

	metaCols := []string{"id", "resolution", "claimed_by_agent", "claimed_by_turn_id", "claimed_at"}
	tests := []struct {
		name      string
		taskIDs   []int64
		rows      *testRows
		queryErr  error
		want      map[int64]string
		wantQuery string
		wantErr   string
	}{
		{name: "empty IDs returns empty map", want: map[int64]string{}},
		{name: "successful fetch of resolutions", taskIDs: []int64{1, 2}, rows: sqlRows(metaCols, []driver.Value{int64(1), "completed", "", "", nil}, []driver.Value{int64(2), "wont_do", "", "", nil}), want: map[int64]string{1: "completed", 2: "wont_do"}, wantQuery: sqlFetchMeta},
		{name: "query error returned", taskIDs: []int64{3}, queryErr: errors.New("query error"), wantQuery: sqlFetchMeta, wantErr: "failed to fetch task resolutions: failed to fetch task meta: query error"},
		{name: "scan error on wrong columns", taskIDs: []int64{4}, rows: sqlRows([]string{"id"}, []driver.Value{int64(4)}), wantQuery: sqlFetchMeta, wantErr: "failed to fetch task resolutions: failed to scan task meta: sql: expected 1 destination arguments in Scan, not 5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newQueryStore(t, tc.rows, tc.queryErr)
			got, err := store.FetchTaskResolutions(context.Background(), tc.taskIDs)
			if errText(err) != tc.wantErr || !maps.Equal(got, tc.want) {
				t.Fatalf("FetchTaskResolutions = %v, %q; want %v, %q", got, errText(err), tc.want, tc.wantErr)
			}
			checkSQLCalls(t, conn, tc.wantQuery, 1)
		})
	}
}

func TestSQLTaskStore_FetchTaskMeta(t *testing.T) {
	t.Parallel()

	metaCols := []string{"id", "resolution", "claimed_by_agent", "claimed_by_turn_id", "claimed_at"}
	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		taskIDs   []int64
		rows      *testRows
		queryErr  error
		want      map[int64]config.TaskMeta
		wantQuery string
		wantErr   string
	}{
		{name: "empty IDs returns empty map", want: map[int64]config.TaskMeta{}},
		{
			name:    "successful fetch of meta",
			taskIDs: []int64{1, 2},
			rows: sqlRows(metaCols,
				[]driver.Value{int64(1), "completed", "", "", nil},
				[]driver.Value{int64(2), "", "eri", "turn-123", now},
			),
			want: map[int64]config.TaskMeta{
				1: {Resolution: "completed"},
				2: {ClaimedByAgent: "eri", ClaimedByTurnID: "turn-123", ClaimedAt: &now},
			},
			wantQuery: sqlFetchMeta,
		},
		{name: "query error returned", taskIDs: []int64{3}, queryErr: errors.New("meta query error"), wantQuery: sqlFetchMeta, wantErr: "failed to fetch task meta: meta query error"},
		{name: "scan error on wrong columns", taskIDs: []int64{4}, rows: sqlRows([]string{"id"}, []driver.Value{int64(4)}), wantQuery: sqlFetchMeta, wantErr: "failed to scan task meta: sql: expected 1 destination arguments in Scan, not 5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newQueryStore(t, tc.rows, tc.queryErr)
			got, err := store.FetchTaskMeta(context.Background(), tc.taskIDs)
			if errText(err) != tc.wantErr || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("FetchTaskMeta = %+v, %q; want %+v, %q", got, errText(err), tc.want, tc.wantErr)
			}
			checkSQLCalls(t, conn, tc.wantQuery, 1)
		})
	}
}

func TestSQLTaskStore_ClaimTask(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	zeroRows := int64(0)
	tests := []struct {
		name         string
		taskID       int64
		agent        string
		turnID       string
		claimedAt    time.Time
		rowsAffected *int64
		execErr      error
		want         bool
		wantErr      string
	}{
		{name: "successful claim", taskID: 1, agent: "eri", turnID: "turn-1", claimedAt: now, want: true},
		{name: "already claimed or done", taskID: 2, agent: "eri", turnID: "turn-1", claimedAt: now, rowsAffected: &zeroRows, want: false},
		{name: "exec error returned", taskID: 3, agent: "eri", turnID: "turn-1", claimedAt: now, execErr: errors.New("exec error"), wantErr: "failed to claim task: exec error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newExecStore(t, tc.execErr)
			if tc.rowsAffected != nil {
				conn.rowsAffected = tc.rowsAffected
			}
			got, err := store.ClaimTask(context.Background(), tc.taskID, tc.agent, tc.turnID, tc.claimedAt)
			if errText(err) != tc.wantErr || got != tc.want {
				t.Fatalf("ClaimTask = %v, %q; want %v, %q", got, errText(err), tc.want, tc.wantErr)
			}
			checkSQLCalls(t, conn, sqlClaim, 4)
		})
	}
}

func TestSQLTaskStore_UnclaimTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		taskID  int64
		execErr error
		wantErr string
	}{
		{name: "successful unclaim", taskID: 1},
		{name: "exec error returned", taskID: 2, execErr: errors.New("unclaim failure"), wantErr: "failed to unclaim task: unclaim failure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newExecStore(t, tc.execErr)
			err := store.UnclaimTask(context.Background(), tc.taskID)
			if errText(err) != tc.wantErr {
				t.Fatalf("UnclaimTask = %q; want %q", errText(err), tc.wantErr)
			}
			checkSQLCalls(t, conn, sqlUnclaim, 1)
		})
	}
}

func TestSQLTaskStore_SearchTasksRRF(t *testing.T) {
	t.Parallel()

	scored := []string{"id", "title", "done", "resolution", "score", "claimed_by_agent", "claimed_by_turn_id", "claimed_at"}
	emb := make([]float32, 384)
	tests := []struct {
		name      string
		embedding []float32
		queryText string
		status    string
		limit     int
		rows      *testRows
		queryErr  error
		want      []config.ScoredTaskSummary
		wantArgs  []driver.Value // limit, query text, status filter, hasVector
		wantErr   string
	}{
		{
			name: "successful search with rows", embedding: emb, queryText: "fix bug", status: "open", limit: 5,
			rows: sqlRows(scored,
				[]driver.Value{int64(1), "Fix bug", false, "", 0.95, "", "", nil},
				[]driver.Value{int64(2), "Closed task", true, "completed", 0.70, "", "", nil},
			),
			want:     []config.ScoredTaskSummary{{ID: 1, Title: "Fix bug", Status: "open", Score: 0.95}, {ID: 2, Title: "Closed task", Status: "completed", Score: 0.70}},
			wantArgs: []driver.Value{5, "fix bug", "open", true},
		},
		{name: "empty embedding uses default vector and vector gate false", queryText: "search term", rows: sqlRows(scored), wantArgs: []driver.Value{10, "search term", "open", false}},
		{name: "query error handled", embedding: emb, queryText: "query", queryErr: errors.New("db timeout"), wantArgs: []driver.Value{10, "query", "open", true}, wantErr: "hybrid rrf search failed: db timeout"},
		{name: "scan error handled", embedding: emb, queryText: "query", rows: sqlRows([]string{"id"}, []driver.Value{int64(1)}), wantArgs: []driver.Value{10, "query", "open", true}, wantErr: "failed to scan scored task row: sql: expected 1 destination arguments in Scan, not 8"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, conn := newQueryStore(t, tc.rows, tc.queryErr)
			got, err := store.SearchTasksRRF(context.Background(), tc.embedding, tc.queryText, tc.status, tc.limit)
			if errText(err) != tc.wantErr || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SearchTasksRRF = %+v, %q; want %+v, %q", got, errText(err), tc.want, tc.wantErr)
			}
			if gotArgs := conn.recordedArgs(); !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Fatalf("query args = %#v, want %#v", gotArgs, tc.wantArgs)
			}
		})
	}
}
