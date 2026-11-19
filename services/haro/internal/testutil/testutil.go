// Package testutil provides direct hermetic database constructors for haro
// tests. It is imported ONLY from *_test.go files - never from production
// code - so its sqlite driver import never reaches the production binary.
// Tests call these constructors and pass the resulting *sql.DB explicitly;
// they never route through store.OpenDB or assign unexported hooks.
//
// The sqlite schema lives in testdata/haro_sqlite_schema.sql (embedded):
// test-only fixture DDL stays out of .go sources so the imperative-DDL
// verification gate keeps passing on application code.
package testutil

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	// sqlite driver for hermetic test databases; this package is imported
	// only from *_test.go files so the driver never reaches production.
	_ "modernc.org/sqlite"

	"github.com/azylman/aerial/pkg/httpkit"
)

// sqliteMemoryDSN is the shared in-memory database name for hermetic tests.
const sqliteMemoryDSN = ":memory:"

//go:embed testdata/haro_sqlite_schema.sql
var haroSQLiteSchema string

// migrationSectionMarker separates the base schema from the safe
// column-migration statements inside the embedded fixture.
const migrationSectionMarker = "-- HARO_SQLITE_MIGRATIONS_BELOW"

// OpenSQLiteTestDB opens a hermetic SQLite database for tests, applying the
// production schema and safe column migrations. The dsn accepts the same
// forms as the old sqliteTestInitHook (":memory:", file paths, sqlite://
// prefixes). Every caller gets an independent handle.
func OpenSQLiteTestDB(dsn string) (*sql.DB, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(dsn), "sqlite://")
	if trimmed != sqliteMemoryDSN {
		if err := os.MkdirAll(filepath.Dir(trimmed), 0o755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
	}

	sqliteDSN := trimmed
	if trimmed != sqliteMemoryDSN && !strings.Contains(trimmed, "_pragma") {
		if strings.Contains(trimmed, "?") {
			sqliteDSN += "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		} else {
			sqliteDSN += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}

	database, err := sql.Open("sqlite", sqliteDSN)
	if err != nil {
		return nil, err
	}

	if trimmed == sqliteMemoryDSN || strings.Contains(trimmed, "mode=memory") {
		database.SetMaxOpenConns(1)
	}

	ctx := context.Background()
	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	`
	if _, err := database.ExecContext(ctx, pragmas); err != nil {
		httpkit.CloseWarn(database, "database on pragma error")
		return nil, fmt.Errorf("failed to execute PRAGMAs: %w", err)
	}

	schema, migrations := splitSQLiteFixture(haroSQLiteSchema)
	if _, err := database.ExecContext(ctx, schema); err != nil {
		httpkit.CloseWarn(database, "database on schema error")
		return nil, err
	}

	// Safe column migrations on existing tables
	for _, alterStmt := range migrations {
		if _, alterErr := database.ExecContext(ctx, alterStmt); alterErr != nil {
			if !strings.Contains(strings.ToLower(alterErr.Error()), "duplicate column") {
				httpkit.CloseWarn(database, "database on migration error")
				return nil, fmt.Errorf("failed to execute migration %q: %w", alterStmt, alterErr)
			}
			log.Printf("[Scheduler DB] Column migration notice (safe to ignore if exists): %v", alterErr)
		}
	}

	log.Printf("[Scheduler DB] SQLite database initialized at %s", trimmed)
	return database, nil
}

// splitSQLiteFixture splits the embedded fixture into the base schema blob
// (executed as one multi-statement script) and the per-statement safe
// column migrations applied after it.
func splitSQLiteFixture(fixture string) (schema string, migrations []string) {
	schema, rest, found := strings.Cut(fixture, migrationSectionMarker)
	if !found {
		return fixture, nil
	}
	for stmt := range strings.SplitSeq(rest, ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			migrations = append(migrations, trimmed)
		}
	}
	return schema, migrations
}

// StartMockPostgres starts a minimal PostgreSQL-protocol stub that answers
// startup auth and simple/extended queries with canned success rows. Tests
// use the returned address to build a postgres:// DSN for OpenDB.
func StartMockPostgres(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on mock pg: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handlePgConn(conn)
		}
	}()

	t.Cleanup(func() { closeListener(t, listener) })

	return listener.Addr().String()
}

func handlePgConn(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("[testutil] mock pg conn close: %v", err)
		}
	}()

	// Send the startup handshake, reporting false when the peer is gone.
	if !mockHandshake(conn) {
		return
	}

	// Message loop
	var lenBuf [4]byte
	for {
		msgType, msgBody, ok := mockReadMessage(conn, lenBuf[:])
		if !ok {
			return
		}
		if done := mockDispatch(conn, msgType, msgBody); done {
			return
		}
	}
}

// mockHandshake answers the startup message with auth-ok, parameters and
// ready-for-query, reporting false when the peer is gone.
func mockHandshake(conn net.Conn) bool {
	// 1. Read startup message length
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return false
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	rest := make([]byte, length-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return false
	}

	// Send AuthenticationOk ('R', len 8, 0)
	if _, err := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}); err != nil {
		return false
	}
	if !writeMock(conn, mockParamStatus("server_version", "16.0")) {
		return false
	}
	if !writeMock(conn, mockParamStatus("client_encoding", "UTF8")) {
		return false
	}

	// Send ReadyForQuery ('Z', len 5, 'I')
	_, err := conn.Write([]byte{'Z', 0, 0, 0, 5, 'I'})
	return err == nil
}

// mockParamStatus encodes one ParameterStatus message.
func mockParamStatus(k, v string) []byte {
	body := k + "\x00" + v + "\x00"
	pkt := make([]byte, 5+len(body))
	pkt[0] = 'S'
	binary.BigEndian.PutUint32(pkt[1:5], uint32(4+len(body)))
	copy(pkt[5:], body)
	return pkt
}

// mockReadMessage reads one tagged protocol message, reporting false at EOF.
func mockReadMessage(conn net.Conn, lenBuf []byte) (byte, []byte, bool) {
	var typeBuf [1]byte
	if _, err := io.ReadFull(conn, typeBuf[:]); err != nil {
		return 0, nil, false
	}
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return 0, nil, false
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	msgBody := make([]byte, msgLen-4)
	if _, err := io.ReadFull(conn, msgBody); err != nil {
		return 0, nil, false
	}
	return typeBuf[0], msgBody, true
}

// mockDispatch answers one protocol message, reporting true when the
// connection is done (peer gone or terminate).
func mockDispatch(conn net.Conn, msgType byte, msgBody []byte) bool {
	ready := []byte{'Z', 0, 0, 0, 5, 'I'}
	switch msgType {
	case 'Q': // Simple Query
		return !mockQueryReply(conn, msgBody, ready)
	case 'P': // Parse
		return !writeMock(conn, []byte{'1', 0, 0, 0, 4})
	case 'B': // Bind
		return !writeMock(conn, []byte{'2', 0, 0, 0, 4})
	case 'D': // Describe
		return !mockDescribeReply(conn, msgBody)
	case 'E': // Execute
		// DataRow 'D', len 11: 1 col, len 1, binary true (0x01)
		if !writeMock(conn, []byte{'D', 0, 0, 0, 11, 0, 1, 0, 0, 0, 1, 0x01}) {
			return true
		}
		return !writeMock(conn, []byte{'C', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0})
	case 'S': // Sync
		return !writeMock(conn, ready)
	default: // Terminate and anything unknown closes the handler.
		return true
	}
}

// mockQueryReply answers a simple query with a canned completion tag. The
// tag is opaque to the tests (they only assert connection success); the
// statement verb is echoed for realism.
func mockQueryReply(conn net.Conn, msgBody, ready []byte) bool {
	tag := "SELECT 1"
	if verb := mockStatementVerb(string(msgBody)); verb == "ALTER" || verb == "CREATE" {
		tag = verb
	}
	cmdBytes := append([]byte{'C'}, 0, 0, 0, 0)
	cmdBytes = append(cmdBytes, []byte(tag+"\x00")...)
	binary.BigEndian.PutUint32(cmdBytes[1:5], uint32(len(cmdBytes)-1))
	return writeMock(conn, cmdBytes) && writeMock(conn, ready)
}

// mockDescribeReply answers a describe with a one-column row description.
func mockDescribeReply(conn net.Conn, msgBody []byte) bool {
	if len(msgBody) > 0 && msgBody[0] == 'S' { // describe statement
		// ParameterDescription: 't', len (4 + 2 + 4 = 10), num_params=1, oid=20 (int8)
		if !writeMock(conn, []byte{'t', 0, 0, 0, 10, 0, 1, 0, 0, 0, 20}) {
			return false
		}
	}
	// RowDescription: 'T', 1 field
	fieldName := "res\x00"
	rowDesc := make([]byte, 7+len(fieldName)+18)
	rowDesc[0] = 'T'
	binary.BigEndian.PutUint32(rowDesc[1:5], uint32(len(rowDesc)-1))
	binary.BigEndian.PutUint16(rowDesc[5:7], 1) // 1 column
	copy(rowDesc[7:], fieldName)
	offset := 7 + len(fieldName)
	// type oid (4, bool=16), type size (2, 1), type mod (4, -1), format (2, 1 = binary)
	binary.BigEndian.PutUint32(rowDesc[offset:], 0)
	binary.BigEndian.PutUint16(rowDesc[offset+4:], 0)
	binary.BigEndian.PutUint32(rowDesc[offset+6:], 16)
	binary.BigEndian.PutUint16(rowDesc[offset+10:], 1)
	binary.BigEndian.PutUint32(rowDesc[offset+12:], 0xffffffff)
	binary.BigEndian.PutUint16(rowDesc[offset+16:], 1)
	return writeMock(conn, rowDesc)
}

// writeMock writes one protocol message, reporting false when the peer is
// gone so the handler stops instead of writing into a dead connection.
func writeMock(conn net.Conn, msg []byte) bool {
	_, err := conn.Write(msg)
	return err == nil
}

// closeListener closes a mock listener, logging unexpected errors.
func closeListener(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Logf("mock pg listener close: %v", err)
	}
}

// mockStatementVerb returns the upper-cased leading SQL verb of a simple
// query string, or "" when there is none.
func mockStatementVerb(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(fields[0])
}
