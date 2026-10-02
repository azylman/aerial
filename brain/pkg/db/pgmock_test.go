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

type mockPgOptions struct {
	factsExist     bool
	revisionExists bool
}

func startMockPostgres(t *testing.T) string {
	return startMockPostgresWithOptions(t, mockPgOptions{factsExist: true, revisionExists: true})
}

func startMockPostgresWithOptions(t *testing.T, opts mockPgOptions) string {
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
			go handlePgConnWithOptions(conn, opts)
		}
	}()

	t.Cleanup(func() {
		_ = listener.Close()
	})

	return listener.Addr().String()
}

func handlePgConnWithOptions(conn net.Conn, opts mockPgOptions) {
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
	statements := make(map[string]string)
	portals := make(map[string]string)
	var lastQuery string

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
			s := string(msgBody)
			if idx := strings.IndexByte(s, 0); idx != -1 {
				stmtName := s[:idx]
				rem := s[idx+1:]
				if endIdx := strings.IndexByte(rem, 0); endIdx != -1 {
					statements[stmtName] = rem[:endIdx]
					lastQuery = rem[:endIdx]
				} else {
					statements[stmtName] = rem
					lastQuery = rem
				}
			} else {
				lastQuery = s
			}
			_, _ = conn.Write([]byte{'1', 0, 0, 0, 4})

		case 'B': // Bind
			s := string(msgBody)
			if idx := strings.IndexByte(s, 0); idx != -1 {
				portalName := s[:idx]
				rem := s[idx+1:]
				if endIdx := strings.IndexByte(rem, 0); endIdx != -1 {
					stmtName := rem[:endIdx]
					if q, ok := statements[stmtName]; ok {
						portals[portalName] = q
						lastQuery = q
					}
				}
			}
			_, _ = conn.Write([]byte{'2', 0, 0, 0, 4})

		case 'D': // Describe
			describeType := msgBody[0]
			name := strings.TrimRight(string(msgBody[1:]), "\x00")
			if describeType == 'S' {
				if q, ok := statements[name]; ok {
					lastQuery = q
				}
				numParams := 0
				for p := 1; p <= 32; p++ {
					if strings.Contains(lastQuery, fmt.Sprintf("$%d", p)) {
						numParams = p
					}
				}
				if numParams > 0 {
					paramDesc := make([]byte, 7+numParams*4)
					paramDesc[0] = 't'
					binary.BigEndian.PutUint32(paramDesc[1:5], uint32(len(paramDesc)-1))
					binary.BigEndian.PutUint16(paramDesc[5:7], uint16(numParams))
					for k := 0; k < numParams; k++ {
						binary.BigEndian.PutUint32(paramDesc[7+k*4:], 0) // unspecified OID
					}
					_, _ = conn.Write(paramDesc)
				} else {
					paramDesc := []byte{'t', 0, 0, 0, 6, 0, 0}
					_, _ = conn.Write(paramDesc)
				}
			} else if describeType == 'P' {
				if q, ok := portals[name]; ok {
					lastQuery = q
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
			colType := uint32(16) // bool
			typeSize := uint16(1)
			if strings.Contains(lastQuery, "COUNT") {
				colType = 20 // int8
				typeSize = 8
			}
			binary.BigEndian.PutUint32(rowDesc[offset:], 0)
			binary.BigEndian.PutUint16(rowDesc[offset+4:], 0)
			binary.BigEndian.PutUint32(rowDesc[offset+6:], colType)
			binary.BigEndian.PutUint16(rowDesc[offset+10:], typeSize)
			binary.BigEndian.PutUint32(rowDesc[offset+12:], 0xffffffff)
			binary.BigEndian.PutUint16(rowDesc[offset+16:], 0)
			_, _ = conn.Write(rowDesc)

		case 'E': // Execute
			s := string(msgBody)
			if idx := strings.IndexByte(s, 0); idx != -1 {
				portalName := s[:idx]
				if q, ok := portals[portalName]; ok {
					lastQuery = q
				}
			}
			val := byte('t')
			if strings.Contains(lastQuery, "COUNT") {
				val = '0'
			}
			if strings.Contains(lastQuery, "to_regclass") {
				if opts.factsExist {
					val = 't'
				} else {
					val = 'f'
				}
			}
			if strings.Contains(lastQuery, "EXISTS") {
				if opts.revisionExists {
					val = 't'
				} else {
					val = 'f'
				}
			}
			dataRow := []byte{'D', 0, 0, 0, 11, 0, 1, 0, 0, 0, 1, val}
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
