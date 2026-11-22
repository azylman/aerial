package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/coverage-ingest/internal/config"
)

func TestCovdataArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dirs []string
		want []string
	}{
		{name: "single dir", dirs: []string{"/w/a"}, want: []string{"tool", "covdata", "func", "-i=/w/a"}},
		{name: "merged dirs", dirs: []string{"/w/a", "/w/b", "/w/c"}, want: []string{"tool", "covdata", "func", "-i=/w/a,/w/b,/w/c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := covdataArgs(tc.dirs); !slices.Equal(got, tc.want) {
				t.Fatalf("args = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExecCovdataFunc(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		goBin   string
		wantErr string
	}{
		{name: "missing go binary", goBin: filepath.Join(t.TempDir(), "no-such-go"), wantErr: "covdata func"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := ExecCovdata{GoBin: tc.goBin}.Func(context.Background(), []string{t.TempDir()})
			assertErrContains(t, err, tc.wantErr)
			if out != nil {
				t.Fatalf("out = %q, want nil", out)
			}
		})
	}
}

// fakeTx is a declarative pgx.Tx: Exec fails for statements whose SQL contains a key
// of execErrs, and Commit returns commitErr. It records what reached the database.
type fakeTx struct {
	pgx.Tx

	execErrs  map[string]error
	commitErr error
	// queryRows are the (service, count) rows Query returns; queryErr fails Query,
	// scanErr fails every Scan and rowsErr is reported by Rows.Err.
	queryRows []prunedRow
	queryErr  error
	scanErr   error
	rowsErr   error

	mu         sync.Mutex
	execs      []recordedExec
	committed  bool
	rolledBack bool
}

type recordedExec struct {
	kind string
	args []any
}

func sqlKind(sql string) string {
	if strings.HasPrefix(sql, "INSERT") {
		return "upsert"
	}
	return "delete"
}

func (f *fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kind := sqlKind(sql)
	f.execs = append(f.execs, recordedExec{kind: kind, args: args})
	return pgconn.CommandTag{}, f.execErrs[kind]
}

// prunedRow is one (service, deleted count) row of the prune query.
type prunedRow struct {
	service string
	count   int64
}

func (f *fakeTx) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, recordedExec{kind: "prune", args: args})
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &fakeRows{rows: f.queryRows, scanErr: f.scanErr, err: f.rowsErr}, nil
}

// fakeRows iterates canned prune rows.
type fakeRows struct {
	pgx.Rows

	rows    []prunedRow
	next    int
	scanErr error
	err     error
}

func (r *fakeRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.next-1]
	service, ok := dest[0].(*string)
	if !ok {
		return errors.New("scan: service dest is not *string")
	}
	count, ok := dest[1].(*int64)
	if !ok {
		return errors.New("scan: count dest is not *int64")
	}
	*service, *count = row.service, row.count
	return nil
}

func (*fakeRows) Close() {}

func (r *fakeRows) Err() error { return r.err }

func (f *fakeTx) Commit(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr == nil {
		f.committed = true
	}
	return f.commitErr
}

func (f *fakeTx) Rollback(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rolledBack = true
	return nil
}

type fakeBeginner struct {
	tx       *fakeTx
	beginErr error
}

func (b fakeBeginner) Begin(context.Context) (pgx.Tx, error) {
	if b.beginErr != nil {
		return nil, b.beginErr
	}
	return b.tx, nil
}

func TestPostgresStoreUpsertService(t *testing.T) {
	t.Parallel()
	twoRows := []config.CoverageRow{{File: "a.go", Function: "F", Percent: 50}, {File: "b.go", Function: "G", Percent: 0}}
	wantArgs := []recordedExec{
		{kind: "upsert", args: []any{"eri", []string{"a.go", "b.go"}, []string{"F", "G"}, []float32{50, 0}}},
		{kind: "delete", args: []any{"eri", []string{"a.go", "b.go"}, []string{"F", "G"}}},
	}
	tests := []struct {
		name           string
		rows           []config.CoverageRow
		beginErr       error
		execErrs       map[string]error
		commitErr      error
		wantErr        string
		wantExecs      []recordedExec
		wantCommitted  bool
		wantRolledBack bool
	}{
		{name: "upsert then prune", rows: twoRows, wantExecs: wantArgs, wantCommitted: true},
		{name: "empty rows is a no-op", rows: nil},
		{name: "begin fails", rows: twoRows, beginErr: errors.New("conn refused"), wantErr: "begin eri: conn refused"},
		{
			name: "upsert fails rolls back", rows: twoRows, execErrs: map[string]error{"upsert": errors.New("boom")},
			wantErr: "upsert eri: boom", wantExecs: wantArgs[:1], wantRolledBack: true,
		},
		{
			name: "prune fails rolls back", rows: twoRows, execErrs: map[string]error{"delete": errors.New("boom")},
			wantErr: "prune eri: boom", wantExecs: wantArgs, wantRolledBack: true,
		},
		{
			name: "commit fails rolls back", rows: twoRows, commitErr: errors.New("serialization"),
			wantErr: "commit eri: serialization", wantExecs: wantArgs, wantRolledBack: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &fakeTx{execErrs: tc.execErrs, commitErr: tc.commitErr}
			err := PostgresStore{DB: fakeBeginner{tx: tx, beginErr: tc.beginErr}}.UpsertService(context.Background(), "eri", tc.rows)
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(tx.execs, tc.wantExecs) {
				t.Fatalf("execs = %+v, want %+v", tx.execs, tc.wantExecs)
			}
			if tx.committed != tc.wantCommitted || tx.rolledBack != tc.wantRolledBack {
				t.Fatalf("committed=%v rolledBack=%v, want %v/%v", tx.committed, tx.rolledBack, tc.wantCommitted, tc.wantRolledBack)
			}
		})
	}
}

func TestPostgresStorePruneServices(t *testing.T) {
	t.Parallel()
	keep := []string{"eri", "eri-restate"}
	pruneExec := []recordedExec{{kind: "prune", args: []any{keep}}}
	twoServices := []prunedRow{{service: "dashboard", count: 12}, {service: "hangar", count: 3}}
	tests := []struct {
		name           string
		keep           []string
		beginErr       error
		queryRows      []prunedRow
		queryErr       error
		scanErr        error
		rowsErr        error
		commitErr      error
		want           map[string]int64
		wantErr        string
		wantExecs      []recordedExec
		wantCommitted  bool
		wantRolledBack bool
	}{
		{name: "prunes retired services", keep: keep, queryRows: twoServices, want: map[string]int64{"dashboard": 12, "hangar": 3}, wantExecs: pruneExec, wantCommitted: true},
		{name: "nothing to prune", keep: keep, want: map[string]int64{}, wantExecs: pruneExec, wantCommitted: true},
		{name: "empty keep set refused", keep: nil, wantErr: "refusing empty keep set"},
		{name: "begin fails", keep: keep, beginErr: errors.New("conn refused"), wantErr: "begin prune: conn refused"},
		{name: "query fails rolls back", keep: keep, queryErr: errors.New("boom"), wantErr: "prune: boom", wantExecs: pruneExec, wantRolledBack: true},
		{name: "scan fails rolls back", keep: keep, queryRows: twoServices, scanErr: errors.New("bad type"), wantErr: "prune scan: bad type", wantExecs: pruneExec, wantRolledBack: true},
		{name: "rows error rolls back", keep: keep, rowsErr: errors.New("conn reset"), wantErr: "prune rows: conn reset", wantExecs: pruneExec, wantRolledBack: true},
		{name: "commit fails rolls back", keep: keep, queryRows: twoServices, commitErr: errors.New("serialization"), wantErr: "commit prune: serialization", wantExecs: pruneExec, wantRolledBack: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &fakeTx{queryRows: tc.queryRows, queryErr: tc.queryErr, scanErr: tc.scanErr, rowsErr: tc.rowsErr, commitErr: tc.commitErr}
			got, err := PostgresStore{DB: fakeBeginner{tx: tx, beginErr: tc.beginErr}}.PruneServices(context.Background(), tc.keep)
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("pruned = %v, want %v", got, tc.want)
			}
			if !reflect.DeepEqual(tx.execs, tc.wantExecs) {
				t.Fatalf("execs = %+v, want %+v", tx.execs, tc.wantExecs)
			}
			if tx.committed != tc.wantCommitted || tx.rolledBack != tc.wantRolledBack {
				t.Fatalf("committed=%v rolledBack=%v, want %v/%v", tx.committed, tx.rolledBack, tc.wantCommitted, tc.wantRolledBack)
			}
		})
	}
}

func assertErrContains(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}
