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

	"github.com/azylman/aerial/services/eri/pkg/config"
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
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String()
}

func handlePgConn(conn net.Conn) {
	defer conn.Close()
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 8 {
		var codeBuf [4]byte
		if _, err := io.ReadFull(conn, codeBuf[:]); err != nil {
			return
		}
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return
		}
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint32(lenBuf[:])
	}
	rest := make([]byte, length-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return
	}
	if _, err := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}); err != nil {
		return
	}
	for _, kv := range [][2]string{{"server_version", "16.0"}, {"client_encoding", "UTF8"}, {"standard_conforming_strings", "on"}} {
		body := kv[0] + "\x00" + kv[1] + "\x00"
		pkt := make([]byte, 5+len(body))
		pkt[0] = 'S'
		binary.BigEndian.PutUint32(pkt[1:5], uint32(4+len(body)))
		copy(pkt[5:], body)
		if _, err := conn.Write(pkt); err != nil {
			return
		}
	}
	ready := []byte{'Z', 0, 0, 0, 5, 'I'}
	if _, err := conn.Write(ready); err != nil {
		return
	}

	for {
		var typeBuf [1]byte
		if _, err := io.ReadFull(conn, typeBuf[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		msgBody := make([]byte, binary.BigEndian.Uint32(lenBuf[:])-4)
		if _, err := io.ReadFull(conn, msgBody); err != nil {
			return
		}

		switch typeBuf[0] {
		case 'Q':
			tag := "SELECT 1"
			s := string(msgBody)
			if strings.HasPrefix(s, "ALTER") {
				tag = "ALTER TABLE"
			} else if strings.HasPrefix(s, "CREATE") || strings.Contains(s, "CREATE TABLE") {
				tag = "CREATE TABLE"
			}
			cmdBytes := append([]byte{'C', 0, 0, 0, 0}, (tag + "\x00")...)
			binary.BigEndian.PutUint32(cmdBytes[1:5], uint32(len(cmdBytes)-1))
			_, _ = conn.Write(cmdBytes)
			_, _ = conn.Write(ready)
		case 'S':
			_, _ = conn.Write(ready)
		case 'X':
			return
		}
	}
}

type (
	dummyDBTX              struct{ d driver.Driver }
	dummyDBTXNoDriver      = dummyDBTX
	dummyDBTXWithNilDriver = dummyDBTX
)

func (d dummyDBTX) Driver() driver.Driver                                           { return d.d }
func (d dummyDBTX) ExecContext(context.Context, string, ...any) (sql.Result, error) { return nil, nil }

func (d dummyDBTX) QueryContext(context.Context, string, ...any) (*sql.Rows, error) { return nil, nil }
func (d dummyDBTX) QueryRowContext(context.Context, string, ...any) *sql.Row        { return nil }

func TestPostgresWireMockAndOpen(t *testing.T) {
	addr := startMockPostgres(t)
	dsn := fmt.Sprintf("postgres://mock:mock@%s/testdb?sslmode=disable", addr)
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open with mock postgres failed: (%v, %v)", db, err)
	}
	defer db.Close()

	cfg := config.NewFromData(&config.ConfigData{DatabaseURL: dsn})
	dbCfg, err := New(cfg)
	if err != nil {
		t.Fatalf("New(cfg) with mock postgres failed: %v", err)
	}
	defer dbCfg.Close()

	pgStore := NewSQLStore(db)
	rel, err := pgStore.AcquireLeaderLock(context.Background())
	if err != nil || rel == nil {
		t.Fatalf("AcquireLeaderLock on mock postgres failed: %v", err)
	}
	rel()
	rel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pgStore.AcquireLeaderLock(canceledCtx); err == nil {
		t.Errorf("expected error for AcquireLeaderLock with canceled context")
	}
}
