package pgtest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	code := Main(m)
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

// writeExec creates path (and its parents) with the given mode.
func writeExec(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestResolveBinDir(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeExec(t, filepath.Join(root, "full", "initdb"), 0o755)
	writeExec(t, filepath.Join(root, "full", "pg_ctl"), 0o755)
	writeExec(t, filepath.Join(root, "partial", "initdb"), 0o755)
	writeExec(t, filepath.Join(root, "noexec", "initdb"), 0o644)
	writeExec(t, filepath.Join(root, "noexec", "pg_ctl"), 0o644)
	if err := os.MkdirAll(filepath.Join(root, "dirs", "initdb"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeExec(t, filepath.Join(root, "dirs", "pg_ctl"), 0o755)

	tests := []struct {
		name       string
		candidates []string
		want       string
		wantErr    string
	}{
		{"first complete dir wins", []string{"full", "partial"}, "full", ""},
		{"falls back to pg_config bindir", []string{"missing", "full"}, "full", ""},
		{"empty pg_config output skipped", []string{"full", ""}, "full", ""},
		{"partial install rejected", []string{"partial", ""}, "", "services/github-runner/Dockerfile"},
		{"non-executable files rejected", []string{"noexec"}, "", "initdb, pg_ctl"},
		{"directory named initdb rejected", []string{"dirs"}, "", "not found"},
		{"no candidates", nil, "", "services/github-runner/Dockerfile"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidates := make([]string, 0, len(tc.candidates))
			for _, c := range tc.candidates {
				if c != "" {
					c = filepath.Join(root, c)
				}
				candidates = append(candidates, c)
			}
			got, err := resolveBinDir(candidates)
			want := ""
			if tc.want != "" {
				want = filepath.Join(root, tc.want)
			}
			if got != want {
				t.Errorf("resolveBinDir() = %q, want %q", got, want)
			}
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("resolveBinDir() err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestFindSchema(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	schemaPath := filepath.Join(root, "repo", "services", "eri", "pkg", "db", "schema.sql")
	if err := os.MkdirAll(filepath.Dir(schemaPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(schemaPath, []byte("CREATE TABLE t (id INT);"), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	nested := filepath.Join(root, "repo", "services", "transcripts", "internal", "store")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// A directory named schema.sql below the real one must not stop the walk.
	if err := os.MkdirAll(filepath.Join(nested, "services", "eri", "pkg", "db", "schema.sql"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "elsewhere"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	tests := []struct {
		name    string
		start   string
		want    string
		wantErr string
	}{
		{"repo root", "repo", "CREATE TABLE t (id INT);", ""},
		{"nested package walks up", "repo/services/transcripts/internal/store", "CREATE TABLE t (id INT);", ""},
		{"outside the repo", "elsewhere", "", "services/eri/pkg/db/schema.sql not found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := findSchema(filepath.Join(root, tc.start))
			if string(got) != tc.want {
				t.Errorf("findSchema() = %q, want %q", got, tc.want)
			}
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("findSchema() err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// probeRunner stands in for *testing.M: it records the server published while
// the tests "run" and optionally leaves a stale pid file so stop fails.
type probeRunner struct {
	slot      *atomic.Pointer[server]
	code      int
	staleStop bool
	ran       bool
	dir       string
	tables    int
}

func (r *probeRunner) Run() int {
	r.ran = true
	srv := r.slot.Load()
	if srv == nil {
		return 99
	}
	r.dir = srv.dir
	conn, err := pgx.Connect(context.Background(), srv.dsn(templateDB))
	if err != nil {
		return 98
	}
	if err := conn.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables WHERE table_name IN ('session_summaries', 'transcript_steps')`).Scan(&r.tables); err != nil {
		r.tables = -1
	}
	if err := conn.Close(context.Background()); err != nil {
		return 97
	}
	if r.staleStop {
		// Stop the real server, then leave a pid file naming a process that
		// cannot exist (above any pid_max) so the final pg_ctl stop fails.
		if err := srv.command("pg_ctl", "-D", srv.dataDir(), "-m", "immediate", "-w", "stop"); err != nil {
			return 96
		}
		if err := os.WriteFile(filepath.Join(srv.dataDir(), "postmaster.pid"), []byte("99999999\n"+srv.dataDir()+"\n"), 0o600); err != nil {
			return 95
		}
	}
	return r.code
}

// runFixtures are the directories TestRun's rows point run at.
type runFixtures struct {
	realBin, repo, broken, empty, failInit, failStart string
}

// newRunFixtures resolves the real binaries and repo, and builds a broken
// schema tree plus fake initdb/pg_ctl installs that fail at each stage.
func newRunFixtures(t *testing.T) runFixtures {
	t.Helper()
	pgConfig, pgConfigErr := exec.Command("pg_config", "--bindir").Output()
	realBin, err := resolveBinDir(newSetup(1000, "", nil, pgConfig, pgConfigErr).binDirs)
	if err != nil {
		t.Fatalf("resolve binaries: %v", err)
	}
	repo, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	f := runFixtures{realBin: realBin, repo: repo, broken: t.TempDir(), empty: t.TempDir()}
	f.failInit = filepath.Join(f.empty, "fail-initdb")
	f.failStart = filepath.Join(f.empty, "fail-start")
	for path, content := range map[string]string{
		filepath.Join(f.broken, "services", "eri", "pkg", "db", "schema.sql"): "CREATE TABLE broken (",
		filepath.Join(f.failInit, "initdb"):                                   "#!/bin/sh\necho initdb broke >&2\nexit 1\n",
		filepath.Join(f.failInit, "pg_ctl"):                                   "#!/bin/sh\nexit 0\n",
		filepath.Join(f.failStart, "initdb"):                                  "#!/bin/sh\nexit 0\n",
		filepath.Join(f.failStart, "pg_ctl"):                                  "#!/bin/sh\necho start broke >&2\nexit 1\n",
	} {
		writeExec(t, path, 0o755)
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return f
}

func TestRun(t *testing.T) {
	t.Parallel()
	f := newRunFixtures(t)
	realBin, repo, broken, empty, failInit, failStart := f.realBin, f.repo, f.broken, f.empty, f.failInit, f.failStart

	tests := []struct {
		name       string
		uid        int
		binDir     string
		workDir    string
		runCode    int
		staleStop  bool
		wantCode   int
		wantRan    bool
		wantTables int
		wantErr    string
	}{
		{"root refused", 0, realBin, repo, 0, false, 1, false, 0, "services/github-runner/Dockerfile"},
		{"missing binaries", 1000, filepath.Join(empty, "bin"), repo, 0, false, 1, false, 0, "services/github-runner/Dockerfile"},
		{"initdb failure", 1000, failInit, repo, 0, false, 1, false, 0, "initdb broke"},
		{"server start failure", 1000, failStart, repo, 0, false, 1, false, 0, "start broke"},
		{"missing schema", 1000, realBin, empty, 0, false, 1, false, 0, "schema.sql not found"},
		{"broken schema", 1000, realBin, broken, 0, false, 1, false, 0, "build template database from services/eri/pkg/db/schema.sql"},
		{"passing run", 1000, realBin, repo, 0, false, 0, true, 2, ""},
		{"failing run keeps code", 1000, realBin, repo, 3, false, 3, true, 2, ""},
		{"stop failure fails a passing run", 1000, realBin, repo, 0, true, 1, true, 2, "pg_ctl"},
		{"stop failure keeps failing code", 1000, realBin, repo, 4, true, 4, true, 2, "pg_ctl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var slot atomic.Pointer[server]
			r := &probeRunner{slot: &slot, code: tc.runCode, staleStop: tc.staleStop}
			code, err := run(r, setup{uid: tc.uid, binDirs: []string{tc.binDir}, workDir: tc.workDir}, &slot)
			msg := ""
			if err != nil {
				msg = err.Error()
			}
			if code != tc.wantCode {
				t.Errorf("run() = %d, want %d (err %q)", code, tc.wantCode, msg)
			}
			if r.ran != tc.wantRan || r.tables != tc.wantTables {
				t.Errorf("ran = %v tables = %d, want %v and %d", r.ran, r.tables, tc.wantRan, tc.wantTables)
			}
			if !strings.Contains(msg, tc.wantErr) || tc.wantErr == "" && msg != "" {
				t.Errorf("run() err = %q, want substring %q", msg, tc.wantErr)
			}
			if slot.Load() != nil {
				t.Errorf("slot still holds a server after run")
			}
			if r.dir != "" {
				if _, err := os.Stat(r.dir); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("server dir %s still present: %v", r.dir, err)
				}
			}
		})
	}
}

// databaseName extracts the database from a DSN NewDSN returned.
func databaseName(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn %q: %v", dsn, err)
	}
	return strings.TrimPrefix(u.Path, "/")
}

func TestNewDSNIsolation(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var names []string
	// Parent cleanups run after both parallel subtests and their database
	// drops have finished.
	t.Cleanup(func() {
		if len(names) != 2 || names[0] == names[1] {
			t.Errorf("database names = %v, want two distinct names", names)
			return
		}
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, active.Load().dsn("postgres"))
		if err != nil {
			t.Errorf("connect admin: %v", err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		var left int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_database WHERE datname = ANY($1)`, names).Scan(&left); err != nil {
			t.Errorf("count databases: %v", err)
		}
		if left != 0 {
			t.Errorf("%d per-test databases survived cleanup, want 0", left)
		}
	})
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dsn := NewDSN(t)
			mu.Lock()
			names = append(names, databaseName(t, dsn))
			mu.Unlock()
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }()
			if _, err := conn.Exec(ctx, `INSERT INTO session_summaries (agent, session_id, summary) VALUES ('pgtest', 'same-key', $1)`, name); err != nil {
				t.Fatalf("insert same key: %v", err)
			}
			var count int
			var summary string
			if err := conn.QueryRow(ctx, `SELECT count(*), max(summary) FROM session_summaries`).Scan(&count, &summary); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 1 || summary != name {
				t.Errorf("database holds %d rows (summary %q), want only its own row %q", count, summary, name)
			}
		})
	}
}

func TestNewSetup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wdErr       error
		pgConfig    string
		pgConfigErr error
		want        setup
	}{
		{"both resolved", nil, "/opt/pg/bin\n", nil, setup{uid: 1000, binDirs: []string{"/usr/lib/postgresql/19/bin", "/usr/lib/postgresql/18/bin", "/opt/pg/bin"}, workDir: "/repo"}},
		{"getwd failure clears work dir", errors.New("gone"), "/opt/pg/bin\n", nil, setup{uid: 1000, binDirs: []string{"/usr/lib/postgresql/19/bin", "/usr/lib/postgresql/18/bin", "/opt/pg/bin"}, workDir: ""}},
		{"missing pg_config keeps default only", nil, "partial", errors.New("not found"), setup{uid: 1000, binDirs: []string{"/usr/lib/postgresql/19/bin", "/usr/lib/postgresql/18/bin", ""}, workDir: "/repo"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := newSetup(1000, "/repo", tc.wdErr, []byte(tc.pgConfig), tc.pgConfigErr)
			if got.uid != tc.want.uid || got.workDir != tc.want.workDir || !slices.Equal(got.binDirs, tc.want.binDirs) {
				t.Errorf("newSetup() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// recordTB captures failures and cleanups so NewDSN's failure paths can be
// asserted without failing the enclosing test.
type recordTB struct {
	testing.TB

	msgs     []string
	cleanups []func()
}

func (*recordTB) Helper() {}

func (r *recordTB) Fatalf(format string, args ...any) {
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func (r *recordTB) Errorf(format string, args ...any) {
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func (r *recordTB) Cleanup(fn func()) { r.cleanups = append(r.cleanups, fn) }

func TestNewDSNFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		srv       *server
		useActive bool
		predrop   bool
		wantDSN   bool
		wantMsg   string
	}{
		{"no server running", nil, false, false, false, "call os.Exit(pgtest.Main(m)) from TestMain"},
		{"unreachable server", &server{dir: "/nonexistent-pgtest"}, false, false, false, "create database pgtest_1"},
		{"clean drop", nil, true, false, true, ""},
		{"drop of a vanished database reported", nil, true, true, true, "drop database"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := tc.srv
			if tc.useActive {
				srv = active.Load()
			}
			rec := &recordTB{TB: t}
			dsn := srv.newDSN(rec)
			if (dsn != "") != tc.wantDSN {
				t.Fatalf("newDSN() = %q, want DSN %v", dsn, tc.wantDSN)
			}
			if tc.predrop {
				if err := srv.exec(context.Background(), "postgres", "DROP DATABASE "+databaseName(t, dsn)); err != nil {
					t.Fatalf("predrop: %v", err)
				}
			}
			for _, fn := range slices.Backward(rec.cleanups) {
				fn()
			}
			got := strings.Join(rec.msgs, "\n")
			if !strings.Contains(got, tc.wantMsg) || tc.wantMsg == "" && got != "" {
				t.Errorf("messages = %q, want substring %q", got, tc.wantMsg)
			}
		})
	}
}
