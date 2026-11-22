package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// run() opens a pool from the libpq PG* environment and the pull loop prunes through
	// it, so point it at a socket directory that cannot exist: tests never reach a real
	// database even when the shell exports one.
	for _, k := range []string{"PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGPORT"} {
		_ = os.Unsetenv(k)
	}
	_ = os.Setenv("PGHOST", "/nonexistent/coverage-ingest-test")
	_ = os.Setenv("PGPASSFILE", "/nonexistent/coverage-ingest-test/pgpass")
	_ = os.Setenv("PGCONNECT_TIMEOUT", "1")
	goleak.VerifyTestMain(m)
}

// withDefaults fills the zero fields of c with the values ParseConfig applies by default.
func withDefaults(c Config) Config {
	if c.FlushTimeout == 0 {
		c.FlushTimeout = 60 * time.Second
	}
	if c.ConnectRetryWindow == 0 {
		c.ConnectRetryWindow = 30 * time.Second
	}
	if c.ConnectRetryInterval == 0 {
		c.ConnectRetryInterval = time.Second
	}
	if c.PullInterval == 0 {
		c.PullInterval = 15 * time.Minute
	}
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.MaxPushBytes == 0 {
		c.MaxPushBytes = 64 << 20
	}
	return c
}

func TestParseConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		yaml    string
		want    Config
		wantErr string
	}{
		{
			name: "explicit timeout and shared service",
			yaml: "targets:\n  - service: eri\n    url: http://eri.aerial/debug/coverage/flush\n  - service: eri\n    url: http://eri-agent-alpha.aerial/debug/coverage/flush\nflush_timeout: 90s\n",
			want: withDefaults(Config{Targets: []Target{
				{Service: "eri", URL: "http://eri.aerial/debug/coverage/flush"},
				{Service: "eri", URL: "http://eri-agent-alpha.aerial/debug/coverage/flush"},
			}, FlushTimeout: 90 * time.Second}),
		},
		{
			name: "explicit connect retry",
			yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_window: 10s\nconnect_retry_interval: 250ms\n",
			want: withDefaults(Config{Targets: []Target{{Service: "eri", URL: "http://x"}}, ConnectRetryWindow: 10 * time.Second, ConnectRetryInterval: 250 * time.Millisecond}),
		},
		{
			name: "default timeout",
			yaml: "targets:\n  - service: haro\n    url: http://haro.aerial/debug/coverage/flush\n",
			want: withDefaults(Config{Targets: []Target{{Service: "haro", URL: "http://haro.aerial/debug/coverage/flush"}}}),
		},
		{
			name: "explicit zero retry durations take the defaults",
			yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_window: 0s\nconnect_retry_interval: 0s\n",
			want: withDefaults(Config{Targets: []Target{{Service: "eri", URL: "http://x"}}}),
		},
		{
			name: "push only with explicit server settings",
			yaml: "push_services: [photos-sync]\npull_interval: 1m\nlisten: ':9090'\nmax_push_bytes: 1024\n",
			want: withDefaults(Config{PushServices: []string{"photos-sync"}, PullInterval: time.Minute, Listen: ":9090", MaxPushBytes: 1024}),
		},
		{
			name: "pull targets and distinct push services",
			yaml: "targets:\n  - service: eri\n    url: http://x\npush_services: [photos-sync, backup]\n",
			want: withDefaults(Config{Targets: []Target{{Service: "eri", URL: "http://x"}}, PushServices: []string{"photos-sync", "backup"}}),
		},
		{name: "no targets", yaml: "flush_timeout: 10s\n", wantErr: "no targets or push_services"},
		{name: "blank push service", yaml: "push_services: ['']\n", wantErr: "push_services 0 is empty"},
		{name: "duplicate push service", yaml: "push_services: [photos-sync, photos-sync]\n", wantErr: `push service "photos-sync" is listed twice`},
		{name: "push service also pulled", yaml: "targets:\n  - service: eri\n    url: http://x\npush_services: [eri]\n", wantErr: `push service "eri" is also a pull target`},
		{name: "negative pull interval", yaml: "push_services: [p]\npull_interval: -1m\n", wantErr: "pull_interval must not be negative"},
		{name: "negative max push bytes", yaml: "push_services: [p]\nmax_push_bytes: -1\n", wantErr: "max_push_bytes must not be negative"},
		{name: "missing url", yaml: "targets:\n  - service: eri\n", wantErr: "target 0 needs service and url"},
		{name: "blank service", yaml: "targets:\n  - service: ' '\n    url: http://x\n", wantErr: "target 0 needs service and url"},
		{name: "negative timeout", yaml: "targets:\n  - service: eri\n    url: http://x\nflush_timeout: -1s\n", wantErr: "flush_timeout must not be negative"},
		{name: "negative retry window", yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_window: -1s\n", wantErr: "connect_retry_window must not be negative"},
		{name: "negative retry interval", yaml: "targets:\n  - service: eri\n    url: http://x\nconnect_retry_interval: -1ms\n", wantErr: "connect_retry_interval must not be negative"},
		{name: "unknown field", yaml: "targetz: []\n", wantErr: "parse config"},
		{name: "invalid yaml", yaml: "targets: [\n", wantErr: "parse config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseConfig([]byte(tc.yaml))
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseFuncReport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		out  string
		want []CoverageRow
	}{
		{
			name: "plain and receiver functions",
			out: "github.com/azylman/aerial/services/eri/funnel.go:45:\t\tsyncBotMemberRoles\t\t57.1%\n" +
				"github.com/azylman/aerial/services/eri/funnel.go:90:\t\t(*Funnel).Start\t\t0.0%\n" +
				"total\t\t\t\t\t(statements)\t\t41.3%\n",
			want: []CoverageRow{
				{File: "github.com/azylman/aerial/services/eri/funnel.go", Function: "syncBotMemberRoles", Percent: 57.1},
				{File: "github.com/azylman/aerial/services/eri/funnel.go", Function: "Start", Percent: 0},
			},
		},
		{
			name: "duplicate keys after receiver strip keep max",
			out: "pkg/a.go:10:\tA.Close\t12.5%\n" +
				"pkg/a.go:20:\tB.Close\t80.0%\n" +
				"pkg/a.go:30:\tC.Close\t40.0%\n",
			want: []CoverageRow{{File: "pkg/a.go", Function: "Close", Percent: 80}},
		},
		{
			name: "short, total, and malformed lines skipped",
			out:  "\n  \npkg/a.go:1:\tonlyTwo\ntotal: (statements) 10.0%\npkg/b.go:2:\tf\tNaNx%\npkg/c.go:3:\tg\t100.0%\n",
			want: []CoverageRow{{File: "pkg/c.go", Function: "g", Percent: 100}},
		},
		{
			name: "location without line suffix kept as is",
			out:  "pkg/d.go\th\t5.0%\npkg/e.go:x:\ti\t6.0%\n",
			want: []CoverageRow{{File: "pkg/d.go", Function: "h", Percent: 5}, {File: "pkg/e.go:x", Function: "i", Percent: 6}},
		},
		{name: "trailing-dot function name skipped", out: "pkg/f.go:1:\tT.\t9.0%\n", want: nil},
		{name: "empty", out: "", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseFuncReport([]byte(tc.out)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("rows = %+v, want %+v", got, tc.want)
			}
		})
	}
}

type tarEntry struct {
	name     string
	typeflag byte
	body     string
	size     int64 // header size override; 0 means len(body)
}

func buildTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		size := e.size
		if size == 0 {
			size = int64(len(e.body))
		}
		typ := e.typeflag
		if typ == 0 {
			typ = tar.TypeReg
		}
		if err := tw.WriteHeader(&tar.Header{Name: e.name, Typeflag: typ, Mode: 0o600, Size: size, Linkname: "x"}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if e.size == 0 && e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("write body: %v", err)
			}
		}
	}
	// Oversize fixtures leave the last entry short, so Close may fail; the header bytes
	// already reached buf, which is all those cases need.
	_ = tw.Close()
	return buf.Bytes()
}

func reg(name, body string) tarEntry { return tarEntry{name: name, body: body} }

func TestExtractFlushArchive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		archive   []byte
		want      int
		wantFiles []string
		wantErr   string
	}{
		{
			name:      "meta and counters",
			archive:   buildTar(t, reg("covmeta.abc", "m"), reg("covcounters.abc.1.2", "c1"), reg("covcounters.abc.3.4", "c2")),
			want:      2,
			wantFiles: []string{"covcounters.abc.1.2", "covcounters.abc.3.4", "covmeta.abc"},
		},
		{name: "empty archive", archive: buildTar(t), want: 0, wantFiles: []string{}},
		{name: "meta only", archive: buildTar(t, reg("covmeta.abc", "m")), want: 0, wantFiles: []string{"covmeta.abc"}},
		{name: "slash in name", archive: buildTar(t, reg("sub/covcounters.x", "c")), wantErr: "unsafe archive entry", wantFiles: []string{}},
		{name: "backslash in name", archive: buildTar(t, reg(`sub\covcounters.x`, "c")), wantErr: "unsafe archive entry", wantFiles: []string{}},
		{name: "dotdot in name", archive: buildTar(t, reg("..covcounters", "c")), wantErr: "unsafe archive entry", wantFiles: []string{}},
		{name: "symlink entry", archive: buildTar(t, tarEntry{name: "covmeta.l", typeflag: tar.TypeSymlink}), wantErr: "not a regular file", wantFiles: []string{}},
		{name: "oversize entry", archive: buildTar(t, tarEntry{name: "covcounters.big", size: maxArchiveEntryBytes + 1}), wantErr: "exceeds", wantFiles: []string{}},
		{
			name:      "duplicate name",
			archive:   buildTar(t, reg("covcounters.a", "1"), reg("covcounters.a", "2")),
			want:      1,
			wantErr:   "create covcounters.a",
			wantFiles: []string{"covcounters.a"},
		},
		{name: "garbage", archive: []byte(strings.Repeat("x", 600)), wantErr: "read archive", wantFiles: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			got, err := ExtractFlushArchive(bytes.NewReader(tc.archive), dir)
			assertErrContains(t, err, tc.wantErr)
			if got != tc.want {
				t.Fatalf("counters = %d, want %d", got, tc.want)
			}
			if files := listDir(t, dir); !slices.Equal(files, tc.wantFiles) {
				t.Fatalf("files = %v, want %v", files, tc.wantFiles)
			}
		})
	}
}

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
	twoRows := []CoverageRow{{File: "a.go", Function: "F", Percent: 50}, {File: "b.go", Function: "G", Percent: 0}}
	wantArgs := []recordedExec{
		{kind: "upsert", args: []any{"eri", []string{"a.go", "b.go"}, []string{"F", "G"}, []float32{50, 0}}},
		{kind: "delete", args: []any{"eri", []string{"a.go", "b.go"}, []string{"F", "G"}}},
	}
	tests := []struct {
		name           string
		rows           []CoverageRow
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

func TestPruneKeepSet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     Config
		want    []string
		wantErr string
	}{
		{
			name: "targets and push services deduped with eri-restate",
			cfg:  Config{Targets: []Target{{Service: "haro"}, {Service: "eri"}, {Service: "eri"}}, PushServices: []string{"photos-sync"}},
			want: []string{"eri", "eri-restate", "haro", "photos-sync"},
		},
		{
			name: "eri-restate target is not duplicated",
			cfg:  Config{Targets: []Target{{Service: "eri-restate"}, {Service: "eri"}}},
			want: []string{"eri", "eri-restate"},
		},
		{name: "push services only", cfg: Config{PushServices: []string{"photos-sync"}}, want: []string{"eri-restate", "photos-sync"}},
		{name: "empty config refused", cfg: Config{}, wantErr: "keep set is empty"},
		{name: "blank names refused", cfg: Config{Targets: []Target{{Service: " "}}, PushServices: []string{""}}, wantErr: "keep set is empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := PruneKeepSet(tc.cfg)
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("PruneKeepSet = %v, want %v", got, tc.want)
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

// fakeRunner returns out (or err) for every call and records the dir count per call.
type fakeRunner struct {
	out []byte
	err error

	mu        sync.Mutex
	dirCounts []int
	fileSets  [][]string
}

func (r *fakeRunner) Func(_ context.Context, dirs []string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dirCounts = append(r.dirCounts, len(dirs))
	var files []string
	for _, d := range dirs {
		entries, _ := os.ReadDir(d)
		for _, e := range entries {
			files = append(files, e.Name())
		}
	}
	slices.Sort(files)
	r.fileSets = append(r.fileSets, files)
	return r.out, r.err
}

// fakeStore fails services listed in errs and holds upserted rows per service; a prune
// drops services outside the keep set unless pruneErr is set.
type fakeStore struct {
	errs     map[string]error
	pruneErr error

	mu      sync.Mutex
	upserts map[string][]CoverageRow
}

func (s *fakeStore) UpsertService(_ context.Context, service string, rows []CoverageRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.errs[service]; err != nil {
		return err
	}
	if s.upserts == nil {
		s.upserts = map[string][]CoverageRow{}
	}
	s.upserts[service] = rows
	return nil
}

func (s *fakeStore) PruneServices(_ context.Context, keep []string) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pruneErr != nil {
		return nil, s.pruneErr
	}
	pruned := map[string]int64{}
	for service, rows := range s.upserts {
		if !slices.Contains(keep, service) {
			pruned[service] = int64(len(rows))
			delete(s.upserts, service)
		}
	}
	return pruned, nil
}

// services returns the sorted services that still hold rows.
func (s *fakeStore) services() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.upserts))
}

// flushResponse is a canned HTTP reply for one flush path on the shared test server.
type flushResponse struct {
	status int
	body   []byte
}

func newFlushServer(t *testing.T, responses map[string]flushResponse) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, ok := responses[r.URL.Path]
		if !ok || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(resp.status)
		_, _ = w.Write(resp.body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

var sampleRows = []CoverageRow{{File: "github.com/x/svc/a.go", Function: "Run", Percent: 75}, {File: "github.com/x/svc/a.go", Function: "Stop", Percent: 0}}

const sampleReport = "github.com/x/svc/a.go:3:\t\tRun\t\t75.0%\ngithub.com/x/svc/a.go:9:\t\t(*S).Stop\t\t0.0%\ntotal\t(statements)\t50.0%\n"

func TestIngest(t *testing.T) {
	t.Parallel()
	good := func(id string) flushResponse {
		return flushResponse{status: http.StatusOK, body: buildTar(t, reg("covmeta."+id, "m"), reg("covcounters."+id+".1.1", "c"))}
	}
	tests := []struct {
		name          string
		paths         []Target // URL holds only the server path
		responses     map[string]flushResponse
		runnerOut     string
		runnerErr     error
		storeErrs     map[string]error
		wantErrs      []string
		wantDirCounts []int
		wantFileSets  [][]string
		wantUpserts   map[string][]CoverageRow
	}{
		{
			name:          "multi-source service merges every stack",
			paths:         []Target{{Service: "eri", URL: "/eri"}, {Service: "eri", URL: "/eri-agent-alpha"}, {Service: "haro", URL: "/haro"}},
			responses:     map[string]flushResponse{"/eri": good("e1"), "/eri-agent-alpha": good("e2"), "/haro": good("h1")},
			runnerOut:     sampleReport,
			wantDirCounts: []int{2, 1},
			wantFileSets: [][]string{
				{"covcounters.e1.1.1", "covcounters.e2.1.1", "covmeta.e1", "covmeta.e2"},
				{"covcounters.h1.1.1", "covmeta.h1"},
			},
			wantUpserts: map[string][]CoverageRow{"eri": sampleRows, "haro": sampleRows},
		},
		{
			name:          "failing targets are skipped and named",
			paths:         []Target{{Service: "eri", URL: "/eri"}, {Service: "eri", URL: "/eri-agent-beta"}, {Service: "coven", URL: "/coven"}, {Service: "vanadis", URL: "/vanadis"}},
			responses:     map[string]flushResponse{"/eri": good("e1"), "/eri-agent-beta": {status: http.StatusInternalServerError, body: []byte("no meta")}, "/vanadis": {status: http.StatusOK, body: buildTar(t, reg("covmeta.v", "m"))}},
			runnerOut:     sampleReport,
			wantErrs:      []string{"target eri (", "/eri-agent-beta): flush: HTTP 500: no meta", "target coven (", "HTTP 404", "target vanadis (", "no covcounters files"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}},
			wantUpserts:   map[string][]CoverageRow{"eri": sampleRows},
		},
		{
			name:          "unsafe archive fails the target",
			paths:         []Target{{Service: "eri", URL: "/eri"}},
			responses:     map[string]flushResponse{"/eri": {status: http.StatusOK, body: buildTar(t, reg("../covcounters.x", "c"))}},
			wantErrs:      []string{"unsafe archive entry"},
			wantDirCounts: nil,
		},
		{
			name:          "covdata failure names the service",
			paths:         []Target{{Service: "eri", URL: "/eri"}},
			responses:     map[string]flushResponse{"/eri": good("e1")},
			runnerErr:     errors.New("covdata func: exit status 1"),
			wantErrs:      []string{"service eri: covdata func: exit status 1"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}},
		},
		{
			name:          "empty report never reaches the store",
			paths:         []Target{{Service: "eri", URL: "/eri"}},
			responses:     map[string]flushResponse{"/eri": good("e1")},
			runnerOut:     "total\t(statements)\t0.0%\n",
			wantErrs:      []string{"service eri: covdata report has no functions"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}},
		},
		{
			name:          "store failure does not stop later services",
			paths:         []Target{{Service: "eri", URL: "/eri"}, {Service: "haro", URL: "/haro"}},
			responses:     map[string]flushResponse{"/eri": good("e1"), "/haro": good("h1")},
			runnerOut:     sampleReport,
			storeErrs:     map[string]error{"eri": errors.New("commit eri: deadlock")},
			wantErrs:      []string{"service eri: commit eri: deadlock"},
			wantDirCounts: []int{1, 1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}, {"covcounters.h1.1.1", "covmeta.h1"}},
			wantUpserts:   map[string][]CoverageRow{"haro": sampleRows},
		},
		{
			name:          "refused target gives up after its window and later services still ingest",
			paths:         []Target{{Service: "eri", URL: refusedPath}, {Service: "haro", URL: "/haro"}},
			responses:     map[string]flushResponse{"/haro": good("h1")},
			runnerOut:     sampleReport,
			wantErrs:      []string{"target eri (", "connection refused"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.h1.1.1", "covmeta.h1"}},
			wantUpserts:   map[string][]CoverageRow{"haro": sampleRows},
		},
		{
			name:     "malformed url fails the target",
			paths:    []Target{{Service: "eri", URL: "\x7f"}},
			wantErrs: []string{"build request"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newFlushServer(t, tc.responses)
			cfg := Config{FlushTimeout: 5 * time.Second, ConnectRetryWindow: 20 * time.Millisecond, ConnectRetryInterval: time.Millisecond}
			for _, p := range tc.paths {
				url := srv.URL + p.URL
				if p.URL == refusedPath {
					url = closedURL(t)
				}
				cfg.Targets = append(cfg.Targets, Target{Service: p.Service, URL: url})
			}
			runner := &fakeRunner{out: []byte(tc.runnerOut), err: tc.runnerErr}
			store := &fakeStore{errs: tc.storeErrs}
			err := NewServer(cfg, srv.Client(), runner, store).Ingest(context.Background(), cfg)
			if len(tc.wantErrs) == 0 && err != nil {
				t.Fatalf("Ingest error = %v, want nil", err)
			}
			for _, want := range tc.wantErrs {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Ingest error = %v, want it to contain %q", err, want)
				}
			}
			if !slices.Equal(runner.dirCounts, tc.wantDirCounts) {
				t.Fatalf("covdata dir counts = %v, want %v", runner.dirCounts, tc.wantDirCounts)
			}
			if !reflect.DeepEqual(runner.fileSets, tc.wantFileSets) {
				t.Fatalf("covdata inputs = %v, want %v", runner.fileSets, tc.wantFileSets)
			}
			if !reflect.DeepEqual(store.upserts, tc.wantUpserts) {
				t.Fatalf("upserts = %+v, want %+v", store.upserts, tc.wantUpserts)
			}
		})
	}
}

// refusedPath marks a TestIngest target that must point at a closed listener.
const refusedPath = "<refused>"

// closedURL returns a flush URL on a loopback port nothing listens on, so dials fail
// with ECONNREFUSED.
func closedURL(t *testing.T) string {
	t.Helper()
	return "http://" + freeAddr(t) + "/debug/coverage/flush"
}

// errRefused mirrors the error chain a dial rejected by the peer produces.
var errRefused = &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

// scriptedTransport fails successive round trips with errs, then serves body with 200.
type scriptedTransport struct {
	mu   sync.Mutex
	errs []error
	body []byte
}

func scripted(body []byte, errs ...error) *scriptedTransport {
	return &scriptedTransport{errs: errs, body: body}
}

func (s *scriptedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(s.body))}, nil
}

func TestFetchTargetErrors(t *testing.T) {
	t.Parallel()
	archive := buildTar(t, reg("covmeta.e1", "m"), reg("covcounters.e1.1.1", "c"))
	const eriURL = "http://eri.aerial/debug/coverage/flush"
	tests := []struct {
		name        string
		transport   http.RoundTripper // nil dials for real
		url         string            // empty means a closed loopback port
		timeout     time.Duration
		window      time.Duration
		interval    time.Duration
		dir         string
		wantErr     string
		wantRefused bool
		wantFiles   []string
	}{
		{name: "non-refused transport error fails without retry", transport: scripted(archive, errors.New("tls handshake failure")), url: eriURL, timeout: time.Second, window: time.Second, interval: time.Millisecond, dir: "fresh", wantErr: "flush: Post \"http://eri.aerial/debug/coverage/flush\": tls handshake failure"},
		{name: "refused dials recover within the window", transport: scripted(archive, errRefused, errRefused), url: eriURL, timeout: time.Second, window: time.Second, interval: time.Millisecond, dir: "fresh", wantFiles: []string{"covcounters.e1.1.1", "covmeta.e1"}},
		{name: "refused without a retry window fails at once", transport: scripted(archive, errRefused), url: eriURL, timeout: time.Second, dir: "fresh", wantErr: "connection refused", wantRefused: true},
		{name: "closed port gives up after the window", timeout: 5 * time.Second, window: 20 * time.Millisecond, interval: time.Millisecond, dir: "fresh", wantErr: "connection refused", wantRefused: true},
		{name: "flush timeout ends the retry wait", timeout: 20 * time.Millisecond, window: time.Minute, interval: 10 * time.Second, dir: "fresh", wantErr: "connection refused", wantRefused: true},
		{name: "existing target dir", transport: scripted(archive), url: eriURL, timeout: time.Second, dir: "", wantErr: "create target dir"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: tc.transport}
			url := tc.url
			if url == "" {
				url = closedURL(t)
			}
			cfg := Config{FlushTimeout: tc.timeout, ConnectRetryWindow: tc.window, ConnectRetryInterval: tc.interval}
			dir := filepath.Join(t.TempDir(), tc.dir)
			err := fetchTarget(context.Background(), client, cfg, Target{Service: "eri", URL: url}, dir)
			assertErrContains(t, err, tc.wantErr)
			if got := errors.Is(err, syscall.ECONNREFUSED); got != tc.wantRefused {
				t.Fatalf("errors.Is(%v, ECONNREFUSED) = %v, want %v", err, got, tc.wantRefused)
			}
			if tc.wantFiles != nil && !slices.Equal(listDir(t, dir), tc.wantFiles) {
				t.Fatalf("extracted = %v, want %v", listDir(t, dir), tc.wantFiles)
			}
		})
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "bad.yaml"), "targets: [\n")
	writeFile(t, filepath.Join(dir, "badlisten.yaml"), "push_services: [photos-sync]\nlisten: '127.0.0.1:-1'\n")
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
	}{
		{name: "unknown flag", args: []string{"-nope"}, wantCode: 2, wantStderr: "flag provided but not defined"},
		{name: "missing config flag", args: nil, wantCode: 2, wantStderr: "-config is required"},
		{name: "unreadable config", args: []string{"-config", filepath.Join(dir, "absent.yaml")}, wantCode: 1, wantStderr: "read config"},
		{name: "invalid config", args: []string{"-config", filepath.Join(dir, "bad.yaml")}, wantCode: 1, wantStderr: "parse config"},
		{name: "unusable listen address", args: []string{"-config", filepath.Join(dir, "badlisten.yaml")}, wantCode: 1, wantStderr: "listen:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stderr bytes.Buffer
			if code := run(context.Background(), tc.args, &stderr); code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

// TestRunEnvFailures cannot run in parallel because it rewrites process environment.
func TestRunEnvFailures(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	writeFile(t, cfgPath, "targets:\n  - service: eri\n    url: http://127.0.0.1:1/debug/coverage/flush\n")
	tests := []struct {
		name       string
		envKey     string
		envVal     string
		wantCode   int
		wantStderr string
	}{
		{name: "invalid postgres env", envKey: "PGPORT", envVal: "not-a-port", wantCode: 1, wantStderr: "postgres config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)
			var stderr bytes.Buffer
			if code := run(context.Background(), []string{"-config", cfgPath}, &stderr); code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

// TestIngestEnvFailures cannot run in parallel because it rewrites process environment.
func TestIngestEnvFailures(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	tests := []struct {
		name    string
		envKey  string
		envVal  string
		wantErr string
	}{
		{name: "unusable temp dir", envKey: "TMPDIR", envVal: missing, wantErr: "create work dir"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)
			cfg := Config{Targets: []Target{{Service: "eri", URL: "http://127.0.0.1:1/debug/coverage/flush"}}}
			err := NewServer(cfg, nil, &fakeRunner{}, &fakeStore{}).Ingest(context.Background(), cfg)
			assertErrContains(t, err, tc.wantErr)
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

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
