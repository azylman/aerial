package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/services/coven/internal/config"
)

// pinLockConns pins n real connections on one private test database, so
// advisory locks taken on one are contended by the others.
func pinLockConns(t *testing.T, n int) []*sql.Conn {
	t.Helper()
	db := newTestDB(t)
	conns := make([]*sql.Conn, 0, n)
	for range n {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("pin conn: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		conns = append(conns, conn)
	}
	return conns
}

// advisoryKeyFree reports whether another session holds key: it tries the
// session lock on conn and immediately frees it again when taken.
func advisoryKeyFree(t *testing.T, conn *sql.Conn, key string) bool {
	t.Helper()
	ctx := context.Background()
	var got bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&got); err != nil {
		t.Fatalf("try lock %q: %v", key, err)
	}
	if got {
		execSQL(t, conn, `SELECT pg_advisory_unlock(hashtext($1))`, key)
	}
	return got
}

// waitForAdvisoryWaiter blocks until pg_locks shows an ungranted advisory
// lock request (a session parked inside pg_advisory_lock).
func waitForAdvisoryWaiter(t *testing.T, observer *sql.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		var waiting int
		if err := observer.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&waiting); err != nil {
			t.Fatalf("no advisory waiter appeared: %v", err)
		}
		if waiting > 0 {
			return
		}
	}
}

func TestThreadLocker_Lock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		threadID         string
		agent            []string
		nilDB            bool
		cancelBeforeLock bool
		closeConn        bool
		wantLockErr      bool
		wantHeld         []string
		wantFree         []string
	}{
		{name: "valid thread acquires with release", threadID: "th-1", wantHeld: []string{"th-1"}, wantFree: []string{"agent-alpha/th-1"}},
		{name: "thread id is trimmed", threadID: "  th-1 ", wantHeld: []string{"th-1"}},
		{name: "empty thread refuses lock", threadID: "   ", wantLockErr: true},
		{name: "nil db refuses lock", threadID: "th-1", nilDB: true, wantLockErr: true, wantFree: []string{"th-1"}},
		{name: "canceled lock context fails fast", threadID: "th-1", cancelBeforeLock: true, wantLockErr: true, wantFree: []string{"th-1"}},
		{name: "closed conn fails to acquire", threadID: "th-1", closeConn: true, wantLockErr: true, wantFree: []string{"th-1"}},
		{name: "scoped agent locks scoped key only", threadID: "th-1", agent: []string{"agent-alpha"}, wantHeld: []string{"agent-alpha/th-1"}, wantFree: []string{"th-1"}},
		{name: "blank agent falls back to bare thread", threadID: "th-1", agent: []string{"  "}, wantHeld: []string{"th-1"}, wantFree: []string{"agent-alpha/th-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conns := pinLockConns(t, 2)
			holder, other := conns[0], conns[1]
			locker := NewThreadLocker(holder)
			if tc.nilDB {
				locker = NewThreadLocker(nil)
			}
			if tc.closeConn {
				_ = holder.Close()
			}
			lockCtx, cancelLock := context.WithCancel(context.Background())
			defer cancelLock()
			if tc.cancelBeforeLock {
				cancelLock()
			}

			release, err := locker.Lock(lockCtx, tc.threadID, tc.agent...)
			if (err != nil) != tc.wantLockErr || (release == nil) != tc.wantLockErr {
				t.Fatalf("Lock() = (release non-nil %v, %v), wantLockErr %v", release != nil, err, tc.wantLockErr)
			}
			assertAdvisoryKeys(t, other, tc.wantHeld, tc.wantFree)
			if release == nil {
				return
			}
			if err := release(); err != nil {
				t.Fatalf("release() = %v", err)
			}
			assertAdvisoryKeys(t, other, nil, tc.wantHeld)
		})
	}
}

// assertAdvisoryKeys checks from another session that every held key is
// taken (both for a session try-lock and for WithFactWriteLock, which share
// one key space) and every free key can be taken.
func assertAdvisoryKeys(t *testing.T, other *sql.Conn, held, free []string) {
	t.Helper()
	for _, key := range held {
		if advisoryKeyFree(t, other, key) {
			t.Errorf("key %q free on another session, want held", key)
		}
		if err := WithFactWriteLock(context.Background(), other, key, func(context.Context) error { return nil }); !errors.Is(err, ErrAdvisoryLockBusy) {
			t.Errorf("WithFactWriteLock(%q) on another session = %v, want ErrAdvisoryLockBusy", key, err)
		}
	}
	for _, key := range free {
		if !advisoryKeyFree(t, other, key) {
			t.Errorf("key %q held, want free", key)
		}
	}
}

func TestThreadLocker_Release(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		cancelBeforeUnlock bool
		closeBeforeUnlock  bool
		wantReleaseErr     bool
	}{
		{name: "release wakes the blocked locker"},
		{name: "ctx-expired unlock still releases via WithoutCancel", cancelBeforeUnlock: true},
		{name: "unlock error on closed conn surfaces and lock stays held", closeBeforeUnlock: true, wantReleaseErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conns := pinLockConns(t, 3)
			holder, waiterConn, observer := conns[0], conns[1], conns[2]

			lockCtx, cancelLock := context.WithCancel(context.Background())
			defer cancelLock()
			release, err := NewThreadLocker(holder).Lock(lockCtx, "th-1")
			if err != nil {
				t.Fatalf("Lock() = %v", err)
			}

			// A second session parks inside pg_advisory_lock until the holder unlocks.
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelWait()
			waiter := make(chan error, 1)
			go func() {
				rel, lockErr := NewThreadLocker(waiterConn).Lock(waitCtx, "th-1")
				if lockErr == nil {
					lockErr = rel()
				}
				waiter <- lockErr
			}()
			waitForAdvisoryWaiter(t, observer)

			if tc.cancelBeforeUnlock {
				cancelLock()
			}
			if tc.closeBeforeUnlock {
				_ = holder.Close()
			}
			err = release()
			if (err != nil) != tc.wantReleaseErr {
				t.Fatalf("release() = %v, wantReleaseErr %v", err, tc.wantReleaseErr)
			}
			if tc.wantReleaseErr {
				// The pooled backend still holds the session lock: the waiter stays
				// blocked until its own context gives up.
				cancelWait()
				if waitErr := <-waiter; waitErr == nil {
					t.Fatalf("waiter acquired the lock after a failed release")
				}
				return
			}
			if waitErr := <-waiter; waitErr != nil {
				t.Fatalf("waiter Lock/release after holder released = %v", waitErr)
			}
			if !advisoryKeyFree(t, observer, "th-1") {
				t.Errorf("key still held after both sessions released")
			}
		})
	}
}

// locksProbeTable has a deferred unique constraint so a duplicate insert only
// fails at COMMIT.
const locksProbeTable = `CREATE TABLE lock_probe (v int UNIQUE DEFERRABLE INITIALLY DEFERRED)`

// locksSabotageXactLock shadows hashtext on one connection (search_path puts
// public before pg_catalog), so the xact-lock query fails after BEGIN.
const locksSabotageXactLock = `CREATE FUNCTION public.hashtext(text) RETURNS integer LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'sabotaged'; END$$; SET search_path = public, pg_catalog`

func TestWithFactWriteLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilDB     bool
		lockKey   string
		heldKey   string // held by another session inside its own open WithFactWriteLock
		connSetup string
		cancelCtx bool
		closeConn bool
		fnSQL     string
		fnErr     error
		wantErr   bool
		wantIs    error
		wantProbe int
	}{
		{name: "success commits the write", lockKey: "th-1", fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantProbe: 1},
		{name: "nil db refuses", nilDB: true, lockKey: "th-1", fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantErr: true},
		{name: "empty key refuses without running fn", lockKey: "  ", fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantErr: true},
		{name: "fn error rolls back the write", lockKey: "th-1", fnSQL: `INSERT INTO lock_probe VALUES (1)`, fnErr: errLocksFn, wantIs: errLocksFn},
		{name: "busy key held by another xact lock", lockKey: "th-1", heldKey: "th-1", fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantIs: ErrAdvisoryLockBusy},
		{name: "scoped key busy for same agent", lockKey: config.ScopedThreadKey("agent-alpha", " th-1 "), heldKey: "agent-alpha/th-1", fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantIs: ErrAdvisoryLockBusy},
		{name: "scoped and unscoped keys do not contend", lockKey: "th-1", heldKey: config.ScopedThreadKey("agent-alpha", "th-1"), fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantProbe: 1},
		{name: "cancelled ctx fails begin", lockKey: "th-1", cancelCtx: true, fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantErr: true},
		{name: "closed conn fails begin", lockKey: "th-1", closeConn: true, fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantErr: true},
		{name: "lock query error rolls back", lockKey: "th-1", connSetup: locksSabotageXactLock, fnSQL: `INSERT INTO lock_probe VALUES (1)`, wantErr: true},
		{name: "commit error rolls back", lockKey: "th-1", fnSQL: `INSERT INTO lock_probe VALUES (2), (2)`, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conns := pinLockConns(t, 3)
			conn, holderConn, observer := conns[0], conns[1], conns[2]
			execSQL(t, observer, locksProbeTable)
			execSQL(t, conn, tc.connSetup)

			releaseHolder := startFactWriteHolder(t, holderConn, tc.heldKey)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}
			if tc.closeConn {
				_ = conn.Close()
			}
			var db DBTX = conn
			if tc.nilDB {
				db = nil
			}
			fn := probeWriter(conn, tc.fnSQL, tc.fnErr)
			assertSQLStoreErr(t, WithFactWriteLock(ctx, db, tc.lockKey, fn), tc.wantErr, tc.wantIs)
			assertLockProbe(t, observer, tc.wantProbe)
			if !tc.closeConn && !tc.cancelCtx {
				// A cancelled call or Close retires the *sql.Conn; otherwise the
				// same session must see no dangling uncommitted write.
				assertLockProbe(t, conn, tc.wantProbe)
			}
			releaseHolder()
			if tc.heldKey == "" || tc.wantIs == nil {
				return
			}
			// Once the holder commits, the same key is free and the write lands.
			if err := WithFactWriteLock(context.Background(), conn, tc.lockKey, fn); err != nil {
				t.Fatalf("retry after holder released = %v", err)
			}
			assertLockProbe(t, observer, 1)
		})
	}
}

// startFactWriteHolder parks another session inside an open WithFactWriteLock
// for key and returns the func that lets it commit; an empty key holds nothing.
func startFactWriteHolder(t *testing.T, conn *sql.Conn, key string) func() {
	t.Helper()
	if key == "" {
		return func() {}
	}
	holding := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- WithFactWriteLock(context.Background(), conn, key, func(context.Context) error {
			close(holding)
			<-release
			return nil
		})
	}()
	select {
	case <-holding:
	case err := <-done:
		t.Fatalf("holder WithFactWriteLock = %v", err)
	}
	return func() {
		close(release)
		if err := <-done; err != nil {
			t.Errorf("holder WithFactWriteLock = %v", err)
		}
	}
}

// errLocksFn is the error a probe writer returns after its write.
var errLocksFn = errors.New("fn exploded")

// probeWriter returns a WithFactWriteLock fn that runs query on conn and then
// returns fnErr.
func probeWriter(conn *sql.Conn, query string, fnErr error) func(context.Context) error {
	return func(ctx context.Context) error {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return err
		}
		return fnErr
	}
}

func assertLockProbe(t *testing.T, conn *sql.Conn, want int) {
	t.Helper()
	var n int
	if err := conn.QueryRowContext(context.Background(), `SELECT count(*) FROM lock_probe`).Scan(&n); err != nil {
		t.Fatalf("count lock_probe: %v", err)
	}
	if n != want {
		t.Errorf("lock_probe rows = %d, want %d", n, want)
	}
}
