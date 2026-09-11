package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
)

func startMockPostgres(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
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

	t.Cleanup(func() {
		_ = listener.Close()
	})

	return listener.Addr().String()
}

func handlePgConn(conn net.Conn) {
	defer conn.Close()

	// 1. Read first packet length
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return
	}
	length := binary.BigEndian.Uint32(lenBuf[:])

	// SSL negotiation check (length == 8 and code == 80877103)
	if length == 8 {
		var codeBuf [4]byte
		if _, err := io.ReadFull(conn, codeBuf[:]); err != nil {
			return
		}
		// Write 'N' to indicate SSL not supported
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return
		}
		// Read actual startup message length
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint32(lenBuf[:])
	}

	rest := make([]byte, length-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return
	}

	// Send AuthenticationOk ('R', len 8, 0)
	authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	if _, err := conn.Write(authOk); err != nil {
		return
	}

	// Send ParameterStatus packets
	paramStatus := func(k, v string) []byte {
		body := k + "\x00" + v + "\x00"
		pkt := make([]byte, 5+len(body))
		pkt[0] = 'S'
		binary.BigEndian.PutUint32(pkt[1:5], uint32(4+len(body)))
		copy(pkt[5:], body)
		return pkt
	}
	_, _ = conn.Write(paramStatus("server_version", "16.0"))
	_, _ = conn.Write(paramStatus("client_encoding", "UTF8"))
	_, _ = conn.Write(paramStatus("standard_conforming_strings", "on"))

	// Send ReadyForQuery ('Z', len 5, 'I')
	ready := []byte{'Z', 0, 0, 0, 5, 'I'}
	if _, err := conn.Write(ready); err != nil {
		return
	}

	// Message loop
	for {
		var typeBuf [1]byte
		if _, err := io.ReadFull(conn, typeBuf[:]); err != nil {
			return
		}
		msgType := typeBuf[0]

		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		msgBody := make([]byte, msgLen-4)
		if _, err := io.ReadFull(conn, msgBody); err != nil {
			return
		}

		switch msgType {
		case 'Q': // Simple Query
			tag := "SELECT 1"
			s := string(msgBody)
			if strings.HasPrefix(s, "ALTER") {
				tag = "ALTER TABLE"
			} else if strings.HasPrefix(s, "CREATE") || strings.Contains(s, "CREATE TABLE") {
				tag = "CREATE TABLE"
			}
			cmdBytes := append([]byte{'C'}, 0, 0, 0, 0)
			cmdBytes = append(cmdBytes, []byte(tag+"\x00")...)
			binary.BigEndian.PutUint32(cmdBytes[1:5], uint32(len(cmdBytes)-1))

			_, _ = conn.Write(cmdBytes)
			_, _ = conn.Write(ready)

		case 'P': // Parse
			_, _ = conn.Write([]byte{'1', 0, 0, 0, 4})
		case 'B': // Bind
			_, _ = conn.Write([]byte{'2', 0, 0, 0, 4})
		case 'D': // Describe
			describeType := msgBody[0]
			if describeType == 'S' { // describe statement
				// ParameterDescription: 't', len (4 + 2 + 4 = 10), num_params=1, oid=20 (int8)
				paramDesc := []byte{'t', 0, 0, 0, 10, 0, 1, 0, 0, 0, 20}
				_, _ = conn.Write(paramDesc)
			}
			// RowDescription: 'T', 1 field
			fieldName := "res\x00"
			rowDesc := make([]byte, 7+len(fieldName)+18)
			rowDesc[0] = 'T'
			binary.BigEndian.PutUint32(rowDesc[1:5], uint32(len(rowDesc)-1))
			binary.BigEndian.PutUint16(rowDesc[5:7], 1) // 1 column
			copy(rowDesc[7:], fieldName)
			offset := 7 + len(fieldName)
			// table oid (4), col idx (2), type oid (4, bool=16), type size (2, 1), type mod (4, -1), format (2, 0)
			binary.BigEndian.PutUint32(rowDesc[offset:], 0)
			binary.BigEndian.PutUint16(rowDesc[offset+4:], 0)
			binary.BigEndian.PutUint32(rowDesc[offset+6:], 16)
			binary.BigEndian.PutUint16(rowDesc[offset+10:], 1)
			binary.BigEndian.PutUint32(rowDesc[offset+12:], 0xffffffff)
			binary.BigEndian.PutUint16(rowDesc[offset+16:], 0)
			_, _ = conn.Write(rowDesc)

		case 'E': // Execute
			// DataRow 'D', len 11: 1 col, len 1, 't'
			dataRow := []byte{'D', 0, 0, 0, 11, 0, 1, 0, 0, 0, 1, 't'}
			_, _ = conn.Write(dataRow)
			cmdBytes := []byte{'C', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
			_, _ = conn.Write(cmdBytes)
		case 'S': // Sync
			_, _ = conn.Write(ready)
		case 'X': // Terminate
			return
		}
	}
}

type dummyDBTXNoDriver struct{}

func (d dummyDBTXNoDriver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, nil
}
func (d dummyDBTXNoDriver) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return nil, nil
}
func (d dummyDBTXNoDriver) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return nil
}

type dummyDBTXWithNilDriver struct{}

func (d dummyDBTXWithNilDriver) Driver() driver.Driver {
	return nil
}
func (d dummyDBTXWithNilDriver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, nil
}
func (d dummyDBTXWithNilDriver) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return nil, nil
}
func (d dummyDBTXWithNilDriver) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return nil
}

func TestPostgresWireMockAndInitDB(t *testing.T) {
	addr := startMockPostgres(t)

	origAttempts := postgresMaxAttempts
	origRetryBase := postgresRetryBase
	postgresMaxAttempts = 2
	postgresRetryBase = 1 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origAttempts
		postgresRetryBase = origRetryBase
	}()

	dsn := fmt.Sprintf("postgres://mock:mock@%s/testdb?sslmode=disable", addr)

	// Test InitDB with postgres DSN
	db, err := InitDB(dsn)
	if err != nil {
		t.Fatalf("InitDB with mock postgres failed: %v", err)
	}
	defer db.Close()

	// Verify isPostgres returns true for pgx driver
	if !isPostgres(db) {
		t.Errorf("expected isPostgres(db) to be true for pgx driver, got false")
	}

	// Verify initSchemaPostgres runs cleanly on connected database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := initSchemaPostgres(ctx, db); err != nil {
		t.Errorf("initSchemaPostgres on mock postgres failed: %v", err)
	}

	// Test New with *config.Config wrapping postgres DSN
	cfg := config.NewFromData(&config.ConfigData{DatabaseURL: dsn})
	dbCfg, err := New(cfg)
	if err != nil {
		t.Fatalf("New(cfg) with mock postgres failed: %v", err)
	}
	defer dbCfg.Close()

	// Test InitDB with *config.Config
	dbInitCfg, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB(cfg) with mock postgres failed: %v", err)
	}
	defer dbInitCfg.Close()
}

func TestIsPostgresDriverBranches(t *testing.T) {
	// 1. Nil database
	if isPostgres(nil) {
		t.Errorf("expected isPostgres(nil) to be false")
	}

	// 2. Non-driverGetter DBTX
	if isPostgres(dummyDBTXNoDriver{}) {
		t.Errorf("expected isPostgres(dummyDBTXNoDriver{}) to be false")
	}

	// 3. DriverGetter returning nil driver
	if isPostgres(dummyDBTXWithNilDriver{}) {
		t.Errorf("expected isPostgres(dummyDBTXWithNilDriver{}) to be false")
	}

	// 4. SQLite database
	sqliteDB := setupTestDB(t)
	defer sqliteDB.Close()
	if isPostgres(sqliteDB) {
		t.Errorf("expected isPostgres(sqliteDB) to be false")
	}
}

func TestInitSchemaPostgresAcquireConnError(t *testing.T) {
	// Acquire conn error by passing a closed DB
	addr := startMockPostgres(t)
	origAttempts := postgresMaxAttempts
	origRetryBase := postgresRetryBase
	postgresMaxAttempts = 1
	postgresRetryBase = 1 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origAttempts
		postgresRetryBase = origRetryBase
	}()

	dsn := fmt.Sprintf("postgres://mock:mock@%s/testdb?sslmode=disable", addr)
	db, err := InitDB(dsn)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = db.Close() // Close database so Conn() fails

	ctx := context.Background()
	err = initSchemaPostgres(ctx, db)
	if err == nil {
		t.Errorf("expected error from initSchemaPostgres on closed DB, got nil")
	}
}
