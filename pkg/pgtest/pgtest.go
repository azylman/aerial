// Package pgtest runs DB-backed tests against a real throwaway PostgreSQL 18 +
// pgvector server loaded from the production schema.
//
// A test package calls Main from TestMain; Main starts one unix-socket-only
// server per test binary, builds a template database from
// services/eri/pkg/db/schema.sql and stops the server after the run. Each test
// calls NewDSN to get its own database cloned from the template, so tests can
// run in parallel without seeing each other's rows.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

const (
	// defaultBinDir is where the Debian/Ubuntu postgresql-19 package installs
	// its server binaries (the CI runner images use it).
	defaultBinDir = "/usr/lib/postgresql/19/bin"
	// fallbackBinDir allows seamless fallback during migration from postgresql-18.
	fallbackBinDir = "/usr/lib/postgresql/18/bin"
	// schemaRelPath is the single declarative production schema.
	schemaRelPath = "services/eri/pkg/db/schema.sql"
	// templateDB holds the loaded schema every test database is cloned from.
	templateDB = "pgtest_template"
	// runnerHint names where the CI image installs the server.
	runnerHint = "install postgresql-19 and postgresql-19-pgvector as in services/github-runner/Dockerfile"
)

// active is the server Main started for this test binary.
var active atomic.Pointer[server]

// runner is the part of *testing.M that Main drives.
type runner interface {
	Run() int
}

// setup is the environment Main resolves before starting the server.
type setup struct {
	uid     int
	binDirs []string
	workDir string
}

// server is one running throwaway PostgreSQL instance.
type server struct {
	binDir string
	dir    string
	seq    atomic.Int64
}

// Main starts the per-package PostgreSQL server, runs the tests and stops the
// server. Call it from TestMain as os.Exit(pgtest.Main(m)). Missing binaries,
// a missing schema or running as root fail the whole package; tests are never
// skipped.
func Main(m *testing.M) int {
	wd, wdErr := os.Getwd()
	pgConfig, pgConfigErr := exec.CommandContext(context.Background(), "pg_config", "--bindir").Output()
	code, err := run(m, newSetup(os.Getuid(), wd, wdErr, pgConfig, pgConfigErr), &active)
	if err != nil {
		log.Printf("pgtest: %v", err)
	}
	return code
}

// newSetup turns the raw process environment into Main's inputs. A failed
// Getwd leaves the work dir empty (findSchema then reports the schema as not
// found); a failed pg_config leaves only the default bin dir.
func newSetup(uid int, wd string, wdErr error, pgConfig []byte, pgConfigErr error) setup {
	if wdErr != nil {
		wd = ""
	}
	pgConfigDir := ""
	if pgConfigErr == nil {
		pgConfigDir = strings.TrimSpace(string(pgConfig))
	}
	return setup{uid: uid, binDirs: []string{defaultBinDir, fallbackBinDir, pgConfigDir}, workDir: wd}
}

// run is Main's core: it validates env, starts a server published through
// slot for the duration of m.Run, and always stops it afterwards. A failed
// stop turns a passing run into a failure.
func run(m runner, env setup, slot *atomic.Pointer[server]) (int, error) {
	srv, err := start(env)
	if err != nil {
		return 1, err
	}
	slot.Store(srv)
	code := m.Run()
	slot.Store(nil)
	stopErr := srv.stop()
	if stopErr != nil && code == 0 {
		code = 1
	}
	return code, stopErr
}

// start checks the environment, then initializes, boots and loads a server.
func start(env setup) (*server, error) {
	if env.uid == 0 {
		return nil, fmt.Errorf("postgres refuses to run as root (uid 0); run tests as a regular user (%s)", runnerHint)
	}
	binDir, err := resolveBinDir(env.binDirs)
	if err != nil {
		return nil, err
	}
	schema, err := findSchema(env.workDir)
	if err != nil {
		return nil, err
	}
	// A short os.MkdirTemp path in /tmp keeps the unix socket under the ~107-byte limit
	// even when Bazel sets $TMPDIR/$TEST_TMPDIR to a deeply nested execroot path.
	dir, err := os.MkdirTemp("/tmp", "pg")
	if err != nil {
		return nil, fmt.Errorf("create server dir: %w", err)
	}
	srv := &server{binDir: binDir, dir: dir}
	if err := srv.boot(schema); err != nil {
		return nil, errors.Join(err, srv.stop())
	}
	return srv, nil
}

// resolveBinDir returns the first candidate holding initdb and pg_ctl.
func resolveBinDir(candidates []string) (string, error) {
	for _, dir := range candidates {
		if dir == "" {
			continue
		}
		if isExecutable(filepath.Join(dir, "initdb")) && isExecutable(filepath.Join(dir, "pg_ctl")) {
			return dir, nil
		}
	}
	return "", fmt.Errorf("PostgreSQL 19 server binaries (initdb, pg_ctl) not found in %v; %s", candidates, runnerHint)
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0
}

// findSchema walks up from start to the directory containing the schema and
// returns its contents.
func findSchema(start string) ([]byte, error) {
	dir := filepath.Clean(start)
	for {
		if schema, err := os.ReadFile(filepath.Join(dir, schemaRelPath)); err == nil {
			return schema, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil, fmt.Errorf("%s not found in %s or any parent directory", schemaRelPath, start)
		}
		dir = parent
	}
}

func (s *server) dataDir() string { return filepath.Join(s.dir, "data") }

// boot runs initdb, starts the server on a unix socket only and builds the
// template database from schema.
func (s *server) boot(schema []byte) error {
	if err := s.command("initdb", "-D", s.dataDir(), "-A", "trust", "-U", "postgres", "--no-sync", "-E", "UTF8", "--locale=C",
		"-c", "listen_addresses=", "-c", "unix_socket_directories="+s.dir,
		"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off", "-c", "max_connections=300"); err != nil {
		return err
	}
	// -w blocks until the server accepts connections.
	logPath := filepath.Join(s.dir, "server.log")
	if err := s.command("pg_ctl", "-D", s.dataDir(), "-l", logPath, "-w", "start"); err != nil {
		serverLog, readErr := os.ReadFile(logPath)
		return errors.Join(fmt.Errorf("%w\nserver log:\n%s", err, serverLog), readErr)
	}
	steps := []struct{ db, sql string }{
		{"postgres", "CREATE DATABASE " + templateDB},
		// The vector extension lives outside schema.sql (the base DB manages it).
		{templateDB, "CREATE EXTENSION IF NOT EXISTS vector;\n" + string(schema)},
	}
	for _, step := range steps {
		if err := s.exec(context.Background(), step.db, step.sql); err != nil {
			return fmt.Errorf("build template database from %s: %w", schemaRelPath, err)
		}
	}
	return nil
}

// stop shuts the server down immediately and removes its directory.
func (s *server) stop() error {
	var stopErr error
	if _, err := os.Stat(filepath.Join(s.dataDir(), "postmaster.pid")); err == nil {
		stopErr = s.command("pg_ctl", "-D", s.dataDir(), "-m", "immediate", "-w", "stop")
	}
	return errors.Join(stopErr, os.RemoveAll(s.dir))
}

func (s *server) command(name string, args ...string) error {
	out, err := exec.CommandContext(context.Background(), filepath.Join(s.binDir, name), args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// dsn returns a pgx-compatible postgres:// URL for database name.
func (s *server) dsn(name string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.User("postgres"),
		Path:     "/" + name,
		RawQuery: url.Values{"host": {s.dir}, "sslmode": {"disable"}}.Encode(),
	}
	return u.String()
}

// exec runs sql (one or more statements, simple protocol) in database name.
func (s *server) exec(ctx context.Context, name, sql string) error {
	conn, err := pgx.Connect(ctx, s.dsn(name))
	if err != nil {
		return err
	}
	_, execErr := conn.Exec(ctx, sql)
	return errors.Join(execErr, conn.Close(ctx))
}

// NewDSN clones a fresh database from the schema template for t, drops it
// (forcibly) when t finishes and returns its DSN. Open it with the production
// constructor. Tests using NewDSN may call t.Parallel.
func NewDSN(t testing.TB) string {
	t.Helper()
	return active.Load().newDSN(t)
}

func (s *server) newDSN(t testing.TB) string {
	t.Helper()
	if s == nil {
		t.Fatalf("pgtest: no server running; call os.Exit(pgtest.Main(m)) from TestMain")
		return ""
	}
	name := fmt.Sprintf("pgtest_%d", s.seq.Add(1))
	ctx := context.Background()
	if err := s.exec(ctx, "postgres", fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s STRATEGY FILE_COPY", name, templateDB)); err != nil {
		t.Fatalf("pgtest: create database %s: %v", name, err)
		return ""
	}
	t.Cleanup(func() {
		if err := s.exec(context.Background(), "postgres", fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", name)); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
	})
	return s.dsn(name)
}
