package pgproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/sanitizer"
)

type startValidationCase struct {
	name             string
	nilDSN           bool
	emptyPath        bool
	badParent        bool
	tooLongPath      bool
	staleFile        bool
	staleNonEmptyDir bool
	cancelCtx        bool
	euid             int
	chownErr         error
	chmodErr         error
	wantErrSub       string
	wantMode         os.FileMode
	wantChownUID     int
	wantChownGID     int
}

func prepareStartSocketPath(t *testing.T, tc startValidationCase) string {
	t.Helper()
	if tc.emptyPath {
		return ""
	}
	dir := t.TempDir()
	sockPath := filepath.Join(dir, DefaultSocketName)
	if tc.badParent {
		blocker := filepath.Join(dir, "file")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		sockPath = filepath.Join(blocker, "sub", DefaultSocketName)
	}
	if tc.tooLongPath {
		sockPath = filepath.Join(dir, strings.Repeat("a", 120), DefaultSocketName)
	}
	if tc.staleFile {
		if err := os.WriteFile(sockPath, []byte("stale"), 0o600); err != nil {
			t.Fatalf("WriteFile stale: %v", err)
		}
	}
	if tc.staleNonEmptyDir {
		if err := os.MkdirAll(filepath.Join(sockPath, "nested"), 0o755); err != nil {
			t.Fatalf("MkdirAll stale dir: %v", err)
		}
	}
	return sockPath
}

func runStartValidationCase(t *testing.T, tc startValidationCase) {
	t.Helper()
	sockPath := prepareStartSocketPath(t, tc)

	gotChownUID, gotChownGID := -1, -1
	var gotMode os.FileMode
	var dsnFn func() string
	if !tc.nilDSN {
		dsnFn = func() string { return "postgres://localhost/aerial" }
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stop, err := Start(ctx, Options{
		SocketPath: sockPath,
		DSNFunc:    dsnFn,
		Geteuid:    func() int { return tc.euid },
		Chown: func(_ string, uid, gid int) error {
			gotChownUID, gotChownGID = uid, gid
			return tc.chownErr
		},
		Chmod: func(p string, mode os.FileMode) error {
			gotMode = mode
			if tc.chmodErr != nil {
				return tc.chmodErr
			}
			return os.Chmod(p, mode)
		},
	})

	if tc.wantErrSub != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("Start() err = %v, want substring %q", err, tc.wantErrSub)
		}
		return
	}
	if err != nil {
		t.Fatalf("Start() unexpected error: %v", err)
	}
	if tc.cancelCtx {
		cancel()
	}
	defer stop()

	if gotMode != tc.wantMode {
		t.Errorf("chmod mode = %o, want %o", gotMode, tc.wantMode)
	}
	if gotChownUID != tc.wantChownUID || gotChownGID != tc.wantChownGID {
		t.Errorf("chown = (%d, %d), want (%d, %d)", gotChownUID, gotChownGID, tc.wantChownUID, tc.wantChownGID)
	}
}

func TestStartValidationAndPermissions(t *testing.T) {
	t.Parallel()

	tests := []startValidationCase{
		{name: "nil_dsn", nilDSN: true, wantErrSub: "DSNFunc is required"},
		{name: "bad_parent", badParent: true, euid: 1000, wantErrSub: "create postgres socket dir"},
		{name: "listen_err", tooLongPath: true, euid: 1000, wantErrSub: "listen on postgres socket"},
		{name: "stale_dir_err", staleNonEmptyDir: true, euid: 1000, wantErrSub: "remove stale postgres socket"},
		{name: "non_root_0600", staleFile: true, cancelCtx: true, euid: 1000, wantMode: 0o600, wantChownUID: -1, wantChownGID: -1},
		{name: "root_0660", euid: 0, wantMode: 0o660, wantChownUID: 0, wantChownGID: 1000},
		{name: "chown_err", euid: 0, chownErr: errors.New("chown denied"), wantErrSub: "chown postgres socket"},
		{name: "chmod_err", euid: 1000, chmodErr: errors.New("chmod denied"), wantErrSub: "chmod postgres socket"},
		{name: "empty_socket_path_default", emptyPath: true, euid: 1000, wantErrSub: "postgres socket"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStartValidationCase(t, tc)
		})
	}
}

func buildRawFrame(length uint32, body []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], length)
	return append(hdr[:], body...)
}

func mustEncodeStartup(msg pgproto3.FrontendMessage) []byte {
	b, err := msg.Encode(nil)
	if err != nil {
		panic(err)
	}
	return b
}

func TestReadStartupMessage(t *testing.T) {
	t.Parallel()

	startupBytes := mustEncodeStartup(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "aerial"},
	})
	sslBytes := mustEncodeStartup(&pgproto3.SSLRequest{})
	gssBytes := mustEncodeStartup(&pgproto3.GSSEncRequest{})
	cancelBytes := mustEncodeStartup(&pgproto3.CancelRequest{ProcessID: 42, SecretKey: []byte{1, 2, 3, 4}})

	tests := []struct {
		name      string
		raw       []byte
		wantType  string
		wantTrail string
		wantErr   bool
	}{
		{name: "startup_pipelined", raw: append(slicesClone(startupBytes), []byte("pipelined")...), wantType: "*pgproto3.StartupMessage", wantTrail: "pipelined"},
		{name: "ssl_request", raw: sslBytes, wantType: "*pgproto3.SSLRequest"},
		{name: "gss_request", raw: gssBytes, wantType: "*pgproto3.GSSEncRequest"},
		{name: "cancel_request", raw: cancelBytes, wantType: "*pgproto3.CancelRequest"},
		{name: "truncated_hdr", raw: []byte{0, 0}, wantErr: true},
		{name: "len_too_small", raw: buildRawFrame(3, nil), wantErr: true},
		{name: "len_too_large", raw: buildRawFrame(maxStartupPacketLen+1, nil), wantErr: true},
		{name: "truncated_body", raw: buildRawFrame(16, []byte{1, 2}), wantErr: true},
		{name: "bad_proto_ver", raw: buildRawFrame(8, []byte{0x99, 0x99, 0x99, 0x99}), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := bytes.NewReader(tc.raw)
			msg, frame, err := readStartupMessage(r)
			if (err != nil) != tc.wantErr {
				t.Fatalf("readStartupMessage() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if gotType := fmt.Sprintf("%T", msg); gotType != tc.wantType {
				t.Errorf("msg type = %s, want %s", gotType, tc.wantType)
			}
			if len(frame) == 0 {
				t.Errorf("expected non-empty raw frame")
			}
			trail, rErr := io.ReadAll(r)
			if rErr != nil {
				t.Fatalf("ReadAll: %v", rErr)
			}
			if string(trail) != tc.wantTrail {
				t.Errorf("trailing bytes = %q, want %q", string(trail), tc.wantTrail)
			}
		})
	}
}

func slicesClone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func proxyClientDSN(socketDir, user, dbName, sslMode, extraQuery string) string {
	q := url.Values{"host": {socketDir}, "sslmode": {sslMode}}
	if extraQuery != "" {
		if parsed, err := url.ParseQuery(extraQuery); err == nil {
			maps.Copy(q, parsed)
		}
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, "ignored-client-password"),
		Path:     "/" + dbName,
		RawQuery: q.Encode(),
	}
	return u.String()
}

type proxyConnCase struct {
	name        string
	upstreamDSN string
	clientUser  string
	clientDB    string
	sslMode     string
	extraQuery  string
	query       string
	wantVal     string
	wantErrCode string
	wantErrSub  string
	wantOmitSub string
}

func assertConnectErr(t *testing.T, conn *pgx.Conn, err error, tc proxyConnCase) {
	t.Helper()
	if err == nil {
		if cErr := conn.Close(t.Context()); cErr != nil {
			t.Logf("Close: %v", cErr)
		}
		t.Fatalf("expected connection error with code %s, got nil", tc.wantErrCode)
	}
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != tc.wantErrCode {
		t.Fatalf("Connect error = %v (pgErr=%+v), want code %s", err, pgErr, tc.wantErrCode)
	}
	if tc.wantErrSub != "" && !strings.Contains(pgErr.Message, tc.wantErrSub) {
		t.Errorf("pgErr.Message = %q, want substring %q", pgErr.Message, tc.wantErrSub)
	}
	if tc.wantOmitSub != "" && strings.Contains(err.Error(), tc.wantOmitSub) {
		t.Errorf("Connect error leaked secret %q: %v", tc.wantOmitSub, err)
	}
}

func resolveUpstreamDB(val, upstream string) string {
	if val != "{upstream}" {
		return val
	}
	uCfg, err := pgconn.ParseConfig(upstream)
	if err != nil {
		return val
	}
	return uCfg.Database
}

func runProxyConnCase(t *testing.T, tc proxyConnCase) {
	t.Helper()
	upstream := tc.upstreamDSN
	if upstream == "" {
		upstream = pgtest.NewDSN(t)
	}
	sockDir := t.TempDir()
	stop, err := Start(t.Context(), Options{
		SocketPath: filepath.Join(sockDir, DefaultSocketName),
		DSNFunc:    func() string { return upstream },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stop()

	clientDB := resolveUpstreamDB(tc.clientDB, upstream)
	wantVal := resolveUpstreamDB(tc.wantVal, upstream)

	cDSN := proxyClientDSN(sockDir, tc.clientUser, clientDB, tc.sslMode, tc.extraQuery)
	conn, err := pgx.Connect(t.Context(), cDSN)
	if tc.wantErrCode != "" {
		assertConnectErr(t, conn, err, tc)
		return
	}
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() {
		if cErr := conn.Close(t.Context()); cErr != nil {
			t.Logf("Close: %v", cErr)
		}
	}()

	var got string
	if err := conn.QueryRow(t.Context(), tc.query).Scan(&got); err != nil {
		t.Fatalf("QueryRow(%q): %v", tc.query, err)
	}
	if got != wantVal {
		t.Errorf("QueryRow(%q) = %q, want %q", tc.query, got, wantVal)
	}
}

func TestProxyConnections(t *testing.T) {
	t.Parallel()

	const leakedSecret = "super_secret_pg_password_token_12345"
	sanitizer.RegisterSensitiveTokens(leakedSecret)

	tests := []proxyConnCase{
		{
			name:       "ubuntu_default_db",
			clientUser: "ubuntu",
			clientDB:   "ubuntu",
			sslMode:    "prefer",
			extraQuery: "application_name=eri_proxy_test",
			query:      "SELECT current_setting('application_name')",
			wantVal:    "eri_proxy_test",
		},
		{
			name:       "matching_upstream_db",
			clientUser: "unprivileged",
			clientDB:   "{upstream}",
			sslMode:    "disable",
			query:      "SELECT current_database()",
			wantVal:    "{upstream}",
		},
		{
			name:        "disallowed_db_3D000",
			clientUser:  "aerial",
			clientDB:    "disallowed_catalog_99999",
			sslMode:     "disable",
			wantErrCode: "3D000",
			wantErrSub:  "is not permitted; proxy is restricted to",
		},
		{
			name:        "replication_rejected",
			clientUser:  "aerial",
			clientDB:    "aerial",
			sslMode:     "disable",
			extraQuery:  "replication=database",
			wantErrCode: "0A000",
			wantErrSub:  "replication connections are not permitted",
		},
		{
			name:        "empty_upstream_dsn",
			upstreamDSN: "   ",
			clientUser:  "aerial",
			clientDB:    "aerial",
			sslMode:     "disable",
			wantErrCode: "08006",
			wantErrSub:  "upstream database DSN is not configured",
		},
		{
			name:        "secret_redacted_err",
			upstreamDSN: "postgres://aerial:" + leakedSecret + "@127.0.0.1:1/aerial?sslmode=disable&connect_timeout=1",
			clientUser:  "aerial",
			clientDB:    "aerial",
			sslMode:     "disable",
			wantErrCode: "08006",
			wantOmitSub: leakedSecret,
		},
		{
			name:        "nonexistent_db_3D000",
			clientUser:  "aerial",
			clientDB:    "nonexistent_db_99999",
			sslMode:     "disable",
			wantErrCode: "3D000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProxyConnCase(t, tc)
		})
	}
}

func TestProxyPipelinedQueryAndCancel(t *testing.T) {
	t.Parallel()

	upstream := pgtest.NewDSN(t)
	sockDir := t.TempDir()
	sockPath := filepath.Join(sockDir, DefaultSocketName)
	stop, err := Start(t.Context(), Options{
		SocketPath: sockPath,
		DSNFunc:    func() string { return upstream },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stop()

	// 1. Send GSSRequest + StartupMessage + pipelined SimpleQuery + Terminate in raw frames
	rawConn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", sockPath)
	if err != nil {
		t.Fatalf("Dial unix: %v", err)
	}
	defer closeQuietly(rawConn)

	gssFrame := mustEncodeStartup(&pgproto3.GSSEncRequest{})
	if _, wErr := rawConn.Write(gssFrame); wErr != nil {
		t.Fatalf("Write GSSEncRequest: %v", wErr)
	}
	var gssResp [1]byte
	if _, rErr := io.ReadFull(rawConn, gssResp[:]); rErr != nil || gssResp[0] != 'N' {
		t.Fatalf("GSSEncRequest response = %q (err=%v), want 'N'", gssResp, rErr)
	}

	msgs := []pgproto3.FrontendMessage{
		&pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters:      map[string]string{"user": "ubuntu", "database": "ubuntu"},
		},
		&pgproto3.Query{String: "SELECT 'pipelined_ok'"},
		&pgproto3.Terminate{},
	}
	pipelined := make([]byte, 0, 128)
	for _, msg := range msgs {
		pipelined = append(pipelined, mustEncodeStartup(msg)...)
	}

	if _, wErr := rawConn.Write(pipelined); wErr != nil {
		t.Fatalf("Write pipelined startup+query: %v", wErr)
	}
	allResp, err := io.ReadAll(rawConn)
	if err != nil {
		t.Fatalf("ReadAll pipelined response: %v", err)
	}
	if !bytes.Contains(allResp, []byte("pipelined_ok")) {
		t.Fatalf("pipelined response did not contain 'pipelined_ok': %q", string(allResp))
	}

	// 2. Test CancelRequest forwarding through the proxy
	cDSN := proxyClientDSN(sockDir, "ubuntu", "ubuntu", "disable", "")
	pgConn, err := pgconn.Connect(t.Context(), cDSN)
	if err != nil {
		t.Fatalf("pgconn.Connect: %v", err)
	}
	defer func() {
		if cErr := pgConn.Close(t.Context()); cErr != nil {
			t.Logf("Close: %v", cErr)
		}
	}()

	if cErr := pgConn.CancelRequest(t.Context()); cErr != nil {
		t.Fatalf("pgConn.CancelRequest through proxy: %v", cErr)
	}

	// 3. Truncated startup frame and double SSLRequest close cleanly
	for _, payload := range [][]byte{
		{0, 0},
		append(mustEncodeStartup(&pgproto3.SSLRequest{}), mustEncodeStartup(&pgproto3.SSLRequest{})...),
	} {
		c, dErr := (&net.Dialer{}).DialContext(t.Context(), "unix", sockPath)
		if dErr != nil {
			t.Fatalf("Dial unix: %v", dErr)
		}
		if _, wErr := c.Write(payload); wErr != nil {
			t.Fatalf("Write payload: %v", wErr)
		}
		if _, rErr := io.ReadAll(c); rErr != nil {
			t.Fatalf("ReadAll payload response: %v", rErr)
		}
		if cErr := c.Close(); cErr != nil {
			t.Fatalf("Close payload conn: %v", cErr)
		}
	}
}

func TestProxyHandleCancelRequest(t *testing.T) {
	t.Parallel()

	fakeUpstream, lErr := net.Listen("tcp", "127.0.0.1:0")
	if lErr != nil {
		t.Fatalf("Listen fake upstream: %v", lErr)
	}
	defer closeQuietly(fakeUpstream)

	cancelFrame := mustEncodeStartup(&pgproto3.CancelRequest{ProcessID: 1, SecretKey: []byte{1, 2, 3, 4}})
	receivedCancel := make(chan []byte, 1)
	go func() {
		uConn, aErr := fakeUpstream.Accept()
		if aErr != nil {
			return
		}
		defer closeQuietly(uConn)
		buf := make([]byte, len(cancelFrame))
		if _, rErr := io.ReadFull(uConn, buf); rErr == nil {
			receivedCancel <- buf
		}
	}()

	fakeDSN := fmt.Sprintf("postgres://%s/aerial?sslmode=disable&connect_timeout=1", fakeUpstream.Addr().String())
	s := &server{dsnFn: func() string { return fakeDSN }, startupTimeout: time.Second}
	s.handleCancelRequest(t.Context(), cancelFrame)

	select {
	case got := <-receivedCancel:
		if !bytes.Equal(got, cancelFrame) {
			t.Fatalf("handleCancelRequest forwarded %v, want %v", got, cancelFrame)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancel frame to arrive at fake upstream")
	}

	for _, dsn := range []string{"", "postgres://127.0.0.1:1/aerial?sslmode=disable&connect_timeout=1"} {
		errServer := &server{dsnFn: func() string { return dsn }, startupTimeout: 100 * time.Millisecond}
		errServer.handleCancelRequest(t.Context(), cancelFrame)
	}
}

func TestWriteConnectError(t *testing.T) {
	t.Parallel()
	ignoreErr(errors.New("test"))
	var buf bytes.Buffer
	writeConnectError(&buf, &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "auth failed"})
	if !bytes.Contains(buf.Bytes(), []byte("28P01")) {
		t.Fatalf("writeConnectError missing code: %q", buf.String())
	}
}
