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
