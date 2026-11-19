package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrAdvisoryLockBusy reports a pg_try_advisory_xact_lock that found the key held.
var ErrAdvisoryLockBusy = errors.New("coven lock: advisory lock busy")

// ThreadLocker serializes extraction for one thread with a PostgreSQL session
// advisory lock. The release closure unlocks over context.WithoutCancel so an
// expired parent context still frees the lock; when unlock itself errors the
// caller must destroy the backing connection instead of returning it to the
// pool, because a held session lock would otherwise leak to the next borrower.
//
// Single-connection precondition: db must be pinned to one connection
// (for example *sql.Conn). Never pass a *sql.DB pool: the session lock and
// its unlock could land on different connections and leak. The indexed-event
// cutover chunk owns the pinning when it wires this helper.
type ThreadLocker struct {
	db DBTX
}

// NewThreadLocker builds a ThreadLocker over db.
func NewThreadLocker(db DBTX) *ThreadLocker {
	return &ThreadLocker{db: db}
}

// Lock acquires the advisory lock for threadID and returns a release closure.
func (l *ThreadLocker) Lock(ctx context.Context, threadID string) (func() error, error) {
	if l == nil || l.db == nil {
		return nil, fmt.Errorf("coven lock: locker database is not initialized")
	}
	key := strings.TrimSpace(threadID)
	if key == "" {
		return nil, fmt.Errorf("coven lock: thread id cannot be empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := l.db.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, key); err != nil {
		return nil, fmt.Errorf("coven lock: failed to acquire thread lock: %w", err)
	}
	return func() error {
		unlockCtx := context.WithoutCancel(ctx)
		if _, err := l.db.ExecContext(unlockCtx, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
			return fmt.Errorf("coven lock: failed to release thread lock (destroy backing conn): %w", err)
		}
		return nil
	}, nil
}

// withFactWriteLock runs fn while holding a transactional advisory lock for
// lockKey. The xact lock releases automatically at COMMIT/ROLLBACK, so no
// unlock path can leak. A busy lock rolls back and returns ErrAdvisoryLockBusy.
//
// Single-connection precondition: db must be pinned to one connection
// (for example *sql.Conn). Never pass a *sql.DB pool: BEGIN, the lock, fn,
// and COMMIT could land on different connections and the transaction would
// not be atomic. The indexed-event cutover chunk owns the pinning when it
// wires this helper.
func withFactWriteLock(ctx context.Context, db DBTX, lockKey string, fn func(ctx context.Context) error) error {
	if db == nil {
		return fmt.Errorf("coven lock: database is not initialized")
	}
	key := strings.TrimSpace(lockKey)
	if key == "" {
		return fmt.Errorf("coven lock: lock key cannot be empty")
	}
	if _, err := db.ExecContext(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("coven lock: failed to begin fact-write lock: %w", err)
	}
	committed := false
	rollback := func() {
		if !committed {
			_, rollbackErr := db.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			_ = rollbackErr
		}
	}
	var acquired int64
	if err := db.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, key).Scan(&acquired); err != nil {
		rollback()
		return fmt.Errorf("coven lock: failed to take fact-write lock: %w", err)
	}
	if acquired == 0 {
		rollback()
		return ErrAdvisoryLockBusy
	}
	if err := fn(ctx); err != nil {
		rollback()
		return err
	}
	if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("coven lock: failed to commit fact-write lock: %w", err)
	}
	committed = true
	return nil
}
