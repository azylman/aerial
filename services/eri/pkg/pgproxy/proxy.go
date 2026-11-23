// Package pgproxy implements a local Unix domain socket PostgreSQL wire-protocol
// proxy that authenticates upstream using a JIT-resolved root DSN so unprivileged
// agent processes (uid=1000) can connect via /var/run/postgresql/.s.PGSQL.5432
// without reading POSTGRES_PASSWORD or DATABASE_URL.
//
// Operational boundary & security model:
//   - Zero-token environment: The proxy eliminates the need to inject raw database
//     credentials into agent environment variables or log streams.
//   - Database scoping: Connections through the proxy are restricted strictly to
//     the upstream-configured database; requests targeting other catalogs are rejected.
//   - Operational guardrails: Within the execution container, hook-guard acts as
//     an operational safety guardrail (enforcing query row limits, projection checks,
//     and schema hints) rather than a hardened cryptographic sandbox boundary between
//     local processes with access to the Unix socket.
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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/azylman/aerial/pkg/sanitizer"
)

const (
	// DefaultSocketDir is the directory where libpq/psql looks for local Unix domain sockets by default.
	DefaultSocketDir = "/var/run/postgresql"
	// DefaultSocketName is the PostgreSQL port 5432 Unix socket filename.
	DefaultSocketName = ".s.PGSQL.5432"
	// DefaultSocketPath is the canonical Unix domain socket path for local PostgreSQL connections.
	DefaultSocketPath = "/var/run/postgresql/.s.PGSQL.5432"

	defaultAgentUID     = 1000
	maxStartupPacketLen = 10000
	defaultStartupWait  = 10 * time.Second
)

// Options configures the local PostgreSQL Unix domain socket proxy.
type Options struct {
	SocketPath     string
	DSNFunc        func() string
	Geteuid        func() int
	Chown          func(string, int, int) error
	Chmod          func(string, os.FileMode) error
	StartupTimeout time.Duration
}

type server struct {
	dsnFn          func() string
	startupTimeout time.Duration

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

func ignoreErr(_ error) {}

func closeQuietly(c io.Closer) {
	ignoreErr(c.Close())
}

func removeQuietly(path string) {
	ignoreErr(os.Remove(path))
}

func setDeadlineQuietly(c net.Conn, t time.Time) {
	ignoreErr(c.SetDeadline(t))
}

func encodeBackendMsg(dst []byte, msg pgproto3.BackendMessage) []byte {
	out, err := msg.Encode(dst)
	ignoreErr(err)
	return out
}

// Start creates the Unix domain socket listener, configures ownership and permissions,
// and begins serving proxied PostgreSQL connections in the background until ctx is
// canceled or the returned stop function is called.
func Start(ctx context.Context, opts Options) (func(), error) {
	if opts.DSNFunc == nil {
		return nil, errors.New("pgproxy: DSNFunc is required")
	}
	socketPath := strings.TrimSpace(opts.SocketPath)
	if socketPath == "" {
		socketPath = DefaultSocketPath
	}
	geteuid := opts.Geteuid
	if geteuid == nil {
		geteuid = os.Geteuid
	}
	chownFn := opts.Chown
	if chownFn == nil {
		chownFn = os.Chown
	}
	chmodFn := opts.Chmod
	if chmodFn == nil {
		chmodFn = os.Chmod
	}
	startupWait := opts.StartupTimeout
	if startupWait <= 0 {
		startupWait = defaultStartupWait
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, fmt.Errorf("create postgres socket dir: %w", err)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale postgres socket: %w", err)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on postgres socket %s: %w", socketPath, err)
	}

	mode := os.FileMode(0o600)
	if geteuid() == 0 {
		if err := chownFn(socketPath, 0, defaultAgentUID); err != nil {
			closeQuietly(ln)
			removeQuietly(socketPath)
			return nil, fmt.Errorf("chown postgres socket %s: %w", socketPath, err)
		}
		mode = 0o660
	}
	if err := chmodFn(socketPath, mode); err != nil {
		closeQuietly(ln)
		removeQuietly(socketPath)
		return nil, fmt.Errorf("chmod postgres socket %s: %w", socketPath, err)
	}

	srv := &server{
		dsnFn:          opts.DSNFunc,
		startupTimeout: startupWait,
		conns:          make(map[net.Conn]struct{}),
	}

	stopCh := make(chan struct{})
	var stopOnce sync.Once
	shutdown := func() {
		stopOnce.Do(func() {
			close(stopCh)
			closeQuietly(ln)
			removeQuietly(socketPath)
			srv.closeAllConns()
		})
	}

	srv.wg.Add(2)
	go func() {
		defer srv.wg.Done()
		select {
		case <-ctx.Done():
			shutdown()
		case <-stopCh:
		}
	}()
	go func() {
		defer srv.wg.Done()
		srv.acceptLoop(ctx, ln)
	}()

	return func() {
		shutdown()
		srv.wg.Wait()
	}, nil
}

func (s *server) acceptLoop(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		if !s.trackConn(conn) {
			closeQuietly(conn)
			return
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer s.untrackConn(c)
			defer closeQuietly(c)
			s.handleConn(ctx, c)
		}(conn)
	}
}

func (s *server) trackConn(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *server) untrackConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

func (s *server) closeAllConns() {
	s.mu.Lock()
	s.closed = true
	active := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		active = append(active, c)
	}
	s.mu.Unlock()

	for _, c := range active {
		closeQuietly(c)
	}
}

// readStartupMessage reads exactly one length-prefixed PostgreSQL startup packet
// from r without wrapping r in a buffered reader, ensuring pipelined query bytes
// sent immediately after StartupMessage remain unread on r.
func readStartupMessage(r io.Reader) (pgproto3.FrontendMessage, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, nil, err
	}
	length := int(binary.BigEndian.Uint32(lenBuf[:]))
	if length < 4 || length > maxStartupPacketLen {
		return nil, nil, fmt.Errorf("invalid startup packet length %d", length)
	}
	frame := make([]byte, length)
	copy(frame[:4], lenBuf[:])
	if _, err := io.ReadFull(r, frame[4:]); err != nil {
		return nil, nil, err
	}
	backend := pgproto3.NewBackend(bytes.NewReader(frame), io.Discard)
	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return nil, nil, err
	}
	return msg, frame, nil
}

func (s *server) handleConn(ctx context.Context, clientConn net.Conn) {
	setDeadlineQuietly(clientConn, time.Now().Add(s.startupTimeout))
	for range 2 {
		msg, rawFrame, err := readStartupMessage(clientConn)
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := clientConn.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.CancelRequest:
			s.handleCancelRequest(ctx, rawFrame)
			return
		case *pgproto3.StartupMessage:
			s.handleStartup(ctx, clientConn, m)
			return
		}
	}
}

func (s *server) parseUpstreamConfig() (*pgconn.Config, error) {
	dsn := strings.TrimSpace(s.dsnFn())
	if dsn == "" {
		return nil, errors.New("upstream database DSN is not configured")
	}
	return pgconn.ParseConfig(dsn)
}

func (s *server) handleCancelRequest(ctx context.Context, rawFrame []byte) {
	upstreamCfg, err := s.parseUpstreamConfig()
	if err != nil {
		return
	}
	cancelCtx, cancel := context.WithTimeout(ctx, s.startupTimeout)
	defer cancel()

	network, addr := pgconn.NetworkAddress(upstreamCfg.Host, upstreamCfg.Port)
	upstreamConn, err := upstreamCfg.DialFunc(cancelCtx, network, addr)
	if err != nil {
		return
	}
	defer closeQuietly(upstreamConn)
	setDeadlineQuietly(upstreamConn, time.Now().Add(s.startupTimeout))

	if _, err := upstreamConn.Write(rawFrame); err != nil {
		return
	}
	var buf [1]byte
	if _, err := upstreamConn.Read(buf[:]); err != nil {
		return
	}
}

func (s *server) handleStartup(ctx context.Context, clientConn net.Conn, msg *pgproto3.StartupMessage) {
	if strings.TrimSpace(msg.Parameters["replication"]) != "" {
		writeErrorResponse(clientConn, "FATAL", "0A000", "replication connections are not permitted")
		return
	}

	upstreamCfg, err := s.parseUpstreamConfig()
	if err != nil {
		writeConnectError(clientConn, err)
		return
	}

	if clientDB := strings.TrimSpace(msg.Parameters["database"]); clientDB != "" && clientDB != "ubuntu" {
		if upstreamCfg.Database != "" && clientDB != upstreamCfg.Database {
			writeErrorResponse(clientConn, "FATAL", "3D000", sanitizer.SanitizeString(fmt.Sprintf("database %q is not permitted; proxy is restricted to %q", clientDB, upstreamCfg.Database)))
			return
		}
		upstreamCfg.Database = clientDB
	}
	for k, v := range msg.Parameters {
		switch k {
		case "user", "database", "password", "replication":
			continue
		default:
			if v != "" {
				upstreamCfg.RuntimeParams[k] = v
			}
		}
	}

	connectCtx, cancel := context.WithTimeout(ctx, s.startupTimeout)
	hijacked, err := connectAndHijack(connectCtx, upstreamCfg)
	cancel()
	if err != nil {
		writeConnectError(clientConn, err)
		return
	}
	defer closeQuietly(hijacked.Conn)

	if !s.trackConn(hijacked.Conn) {
		return
	}
	defer s.untrackConn(hijacked.Conn)

	if _, err := clientConn.Write(encodeStartupResponse(hijacked)); err != nil {
		return
	}
	setDeadlineQuietly(clientConn, time.Time{})
	spliceConns(clientConn, hijacked.Conn)
}

func connectAndHijack(ctx context.Context, cfg *pgconn.Config) (*pgconn.HijackedConn, error) {
	pgConn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ignoreErr(pgConn.SyncConn(ctx))
	return pgConn.Hijack()
}

func encodeStartupResponse(hijacked *pgconn.HijackedConn) []byte {
	var buf []byte
	buf = encodeBackendMsg(buf, &pgproto3.AuthenticationOk{})
	for _, k := range slices.Sorted(maps.Keys(hijacked.ParameterStatuses)) {
		buf = encodeBackendMsg(buf, &pgproto3.ParameterStatus{Name: k, Value: hijacked.ParameterStatuses[k]})
	}
	buf = encodeBackendMsg(buf, &pgproto3.BackendKeyData{ProcessID: hijacked.PID, SecretKey: hijacked.SecretKey})
	buf = encodeBackendMsg(buf, &pgproto3.ReadyForQuery{TxStatus: hijacked.TxStatus})
	return buf
}

func writeConnectError(w io.Writer, err error) {
	sev, code := "FATAL", "08006"
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		if pgErr.Severity != "" {
			sev = pgErr.Severity
		}
		if pgErr.Code != "" {
			code = pgErr.Code
		}
	}
	writeErrorResponse(w, sev, code, sanitizer.SanitizeString(err.Error()))
}

func writeErrorResponse(w io.Writer, severity, code, msg string) {
	buf := encodeBackendMsg(nil, &pgproto3.ErrorResponse{
		Severity:            severity,
		SeverityUnlocalized: severity,
		Code:                code,
		Message:             msg,
	})
	_, err := w.Write(buf)
	ignoreErr(err)
}

func spliceConns(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	copySide := func(dst, src net.Conn) {
		defer wg.Done()
		_, err := io.Copy(dst, src)
		ignoreErr(err)
		closeQuietly(dst)
		closeQuietly(src)
	}
	go copySide(a, b)
	go copySide(b, a)
	wg.Wait()
}
