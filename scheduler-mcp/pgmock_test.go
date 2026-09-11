package main

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
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

	// 1. Read startup message length
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	rest := make([]byte, length-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return
	}

	// Send AuthenticationOk ('R', len 8, 0)
	authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	if _, err := conn.Write(authOk); err != nil {
		return
	}

	// Send ParameterStatus server_version
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

func TestMockPostgresConnection(t *testing.T) {
	addr := startMockPostgres(t)
	dsn := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"
	db, err := InitDB(&Config{DatabaseURL: dsn})
	if err != nil {
		t.Fatalf("InitDB with mock postgres failed: %v", err)
	}
	defer db.Close()
}
