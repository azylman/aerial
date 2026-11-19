package testutil

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSplitSQLiteFixture(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		fixture            string
		wantSchemaContains string
		wantMigrations     bool
	}{
		{"embedded fixture splits out migrations", haroSQLiteSchema, "CREATE TABLE", true},
		{"missing marker keeps whole", "CREATE TABLE t (id TEXT);", "CREATE TABLE", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema, migrations := splitSQLiteFixture(tc.fixture)
			if !strings.Contains(schema, tc.wantSchemaContains) {
				t.Errorf("schema = %q, want containing %q", schema, tc.wantSchemaContains)
			}
			if (len(migrations) > 0) != tc.wantMigrations {
				t.Errorf("migrations present = %v, want %v", len(migrations) > 0, tc.wantMigrations)
			}
		})
	}
}

func TestMockStatementVerb(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"blank", "   ", ""},
		{"select", "SELECT 1", "SELECT"},
		{"leading space alter", "  alter table t", "ALTER"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := mockStatementVerb(tc.input); got != tc.want {
				t.Errorf("mockStatementVerb(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// errCloser fails every Close for close-error path tests.
type errCloser struct{}

func (errCloser) Close() error { return errors.New("close boom") }

func TestCloseListener(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		fail bool
	}{
		{"clean close quiet", false},
		{"close error logged", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.fail {
				closeListener(t, errCloser{})
				return
			}
			client, server := net.Pipe()
			closeListener(t, server)
			_ = client.Close()
		})
	}
}

func TestOpenSQLiteTestDB(t *testing.T) {
	tmpDir := t.TempDir()
	fileDB := tmpDir + "/haro.db"
	tests := []struct {
		name      string
		dsn       string
		wantErr   string
		wantTable bool
	}{
		{"memory ok", ":memory:", "", true},
		{"sqlite prefix stripped", "sqlite://:memory:", "", true},
		{"file ok", fileDB, "", true},
		{"query pragma path ok", fileDB + "?cache=shared", "", true},
		{"mkdir error", "/proc/haro-test/x.db", "failed to create db directory", false},
		{"directory path fails pragmas", tmpDir + "/asdir", "failed to execute PRAGMAs", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dsn := tc.dsn
			if tc.name == "directory path fails pragmas" {
				if err := os.MkdirAll(dsn, 0o755); err != nil {
					t.Fatalf("MkdirAll err = %v", err)
				}
			}
			db, err := OpenSQLiteTestDB(dsn)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("OpenSQLiteTestDB err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenSQLiteTestDB err = %v", err)
			}
			defer db.Close()
			if !tc.wantTable {
				return
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cron_schedules'`).Scan(&count); err != nil {
				t.Fatalf("schema check err = %v", err)
			}
			if count != 1 {
				t.Errorf("cron_schedules tables = %d, want 1", count)
			}
		})
	}
}

func TestMockPostgresDriverFlow(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"ping query exec close", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := StartMockPostgres(t)
			db, err := sql.Open("pgx", "postgres://mock_user:mock_pass@"+addr+"/mock_db?sslmode=disable")
			if err != nil {
				t.Fatalf("sql.Open err = %v", err)
			}
			defer db.Close()
			if err := db.Ping(); err != nil {
				t.Fatalf("Ping err = %v", err)
			}
			// The canned description advertises one int8 bind parameter, so
			// the extended-flow queries below bind one integer each.
			// Replies are canned; the assertions pin the mock contract.
			var one bool
			if err := db.QueryRow("SELECT $1", 1).Scan(&one); err != nil {
				t.Fatalf("QueryRow err = %v", err)
			}
			if !tc.want || !one {
				t.Errorf("QueryRow = %v, want true", one)
			}
			if _, err := db.Exec("SELECT $1", 1); err != nil {
				t.Fatalf("Exec err = %v", err)
			}
			// Parameterless statements travel the simple-query path; the
			// mock answers the ALTER verb with its own completion tag.
			if _, err := db.Exec("ALTER TABLE t ADD COLUMN c INT"); err != nil {
				t.Fatalf("Exec alter err = %v", err)
			}
		})
	}
}

// scriptConn is a net.Conn stub serving canned reads while failing the
// nth write and Close on demand, for deterministic handler error-path
// tests. wOK counts successful writes; -1 never fails.
type scriptConn struct {
	net.Conn

	reads   [][]byte
	pending []byte
	writes  int
	wOK     int
	cErr    error
	written [][]byte
}

func (c *scriptConn) Read(b []byte) (int, error) {
	for len(c.pending) == 0 {
		if len(c.reads) == 0 {
			return 0, errors.New("script exhausted")
		}
		c.pending = append(c.pending, c.reads[0]...)
		c.reads = c.reads[1:]
	}
	n := copy(b, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *scriptConn) Write(b []byte) (int, error) {
	if c.wOK >= 0 && c.writes >= c.wOK {
		return 0, errors.New("write boom")
	}
	c.writes++
	cp := make([]byte, len(b))
	copy(cp, b)
	c.written = append(c.written, cp)
	return len(b), nil
}

// flatWrites concatenates recorded writes for subsequence assertions.
func flatWrites(t *testing.T, c *scriptConn) []byte {
	t.Helper()
	var flat []byte
	for _, w := range c.written {
		flat = append(flat, w...)
	}
	return flat
}

func (c *scriptConn) Close() error { return c.cErr }

// mockStartupBytes encodes a minimal startup message.
func mockStartupBytes() []byte {
	body := append([]byte{}, 0, 3, 0, 0)
	body = append(body, "user\x00mock\x00\x00"...)
	msg := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(msg, uint32(len(msg)))
	copy(msg[4:], body)
	return msg
}

// mockFlowBytes encodes one full extended-flow script after startup.
func mockFlowBytes() [][]byte {
	return [][]byte{
		mockStartupBytes(),
		{'P', 0, 0, 0, 5, 'x'},
		{'B', 0, 0, 0, 5, 'x'},
		{'D', 0, 0, 0, 5, 'S'},
		{'E', 0, 0, 0, 4},
		{'S', 0, 0, 0, 4},
	}
}

func TestHandlePgConnScripts(t *testing.T) {
	partial := mockStartupBytes()[:4]
	fullFlow := []string{"server_version", "res\x00", "SELECT 1"}
	tests := []struct {
		name         string
		reads        [][]byte
		wOK          int
		cErr         bool
		wantWrites   int
		wantContains []string
		wantLog      string
	}{
		{"full flow writes protocol", mockFlowBytes(), -1, false, 11, fullFlow, ""},
		{"startup read fails", nil, -1, false, 0, nil, ""},
		{"startup rest read fails", [][]byte{partial}, -1, false, 0, nil, ""},
		{"auth write fails", mockFlowBytes(), 0, false, 0, nil, ""},
		{"client param write fails", mockFlowBytes(), 2, false, 2, nil, ""},
		{"length read fails", [][]byte{mockStartupBytes(), {'Q'}}, -1, false, 4, nil, ""},
		{"body read fails", [][]byte{mockStartupBytes(), {'Q', 0, 0, 0, 9, 'S'}}, -1, false, 4, nil, ""},
		{"param write fails", mockFlowBytes(), 1, false, 1, nil, ""},
		{"parse write fails", mockFlowBytes(), 4, false, 4, nil, ""},
		{"bind write fails", mockFlowBytes(), 5, false, 5, nil, ""},
		{"describe param write fails", mockFlowBytes(), 6, false, 6, nil, ""},
		{"describe row write fails", mockFlowBytes(), 7, false, 7, nil, ""},
		{"execute data write fails", mockFlowBytes(), 8, false, 8, nil, ""},
		{"execute tag write fails", mockFlowBytes(), 9, false, 9, nil, ""},
		{"sync write fails", mockFlowBytes(), 10, false, 10, nil, ""},
		{"close error logs", [][]byte{mockStartupBytes()}, -1, true, 4, nil, "mock pg conn close"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cErr error
			if tc.cErr {
				cErr = errors.New("close boom")
			}
			sc := &scriptConn{reads: tc.reads, wOK: tc.wOK, cErr: cErr}
			if tc.wantLog != "" {
				var buf bytes.Buffer
				oldOut := log.Writer()
				log.SetOutput(&buf)
				handlePgConn(sc)
				log.SetOutput(oldOut)
				if !strings.Contains(buf.String(), tc.wantLog) {
					t.Errorf("log = %q, want containing %q", buf.String(), tc.wantLog)
				}
			} else {
				handlePgConn(sc)
			}
			if len(sc.written) != tc.wantWrites {
				t.Errorf("writes = %d, want %d", len(sc.written), tc.wantWrites)
			}
			got := flatWrites(t, sc)
			for _, want := range tc.wantContains {
				if !bytes.Contains(got, []byte(want)) {
					t.Errorf("writes missing %q in %q", want, got)
				}
			}
		})
	}
}

// readUntilReady reads protocol bytes until a ReadyForQuery marker or EOF.
