package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

type lockFakeDB struct {
	mu           sync.Mutex
	queries      []string
	dbMem        *sql.DB
	xactAcquired bool
	failOn       string
	failErr      error
}

func newLockFakeDB(t *testing.T) *lockFakeDB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &lockFakeDB{dbMem: db, xactAcquired: true}
}

func lockStageOf(query string) string {
	switch {
	case query == "BEGIN":
		return "BEGIN"
	case query == "COMMIT":
		return "COMMIT"
	case query == "ROLLBACK":
		return "ROLLBACK"
	case strings.Contains(query, "pg_try_advisory_xact_lock("):
		return "XACTLOCK"
	case strings.Contains(query, "pg_advisory_unlock("):
		return "UNLOCK"
	case strings.Contains(query, "pg_advisory_lock("):
		return "LOCK"
	default:
		return "OTHER"
	}
}

func (f *lockFakeDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, query)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.failOn != "" && lockStageOf(query) == f.failOn {
		return nil, f.failErr
	}
	return testCustomResult{rows: 1}, nil
}

func (f *lockFakeDB) QueryContext(_ context.Context, query string, _ ...any) (*sql.Rows, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, query)
	return nil, errors.New("lockFakeDB: QueryContext is unused by lock helpers")
}

func (f *lockFakeDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	f.mu.Lock()
	f.queries = append(f.queries, query)
	acquired := f.xactAcquired
	failLock := f.failOn == "XACTLOCK"
	f.mu.Unlock()
	if failLock {
		return f.dbMem.QueryRowContext(ctx, "SELECT syntax error")
	}
	if acquired {
		return f.dbMem.QueryRowContext(ctx, "SELECT 1")
	}
	return f.dbMem.QueryRowContext(ctx, "SELECT 0")
}

func (f *lockFakeDB) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

func lockQueriesContain(queries []string, substr string) bool {
	for _, q := range queries {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

func TestThreadLocker_Lock(t *testing.T) {
	tests := []struct {
		name             string
		threadID         string
		nilDB            bool
		cancelBeforeLock bool
		wantLockErr      bool
		wantRelease      bool
	}{
		{
			name:        "valid thread acquires with release",
			threadID:    "th-1",
			wantRelease: true,
		},
		{
			name:        "empty thread refuses lock",
			threadID:    "   ",
			wantLockErr: true,
		},
		{
			name:        "nil db refuses lock",
			threadID:    "th-1",
			nilDB:       true,
			wantLockErr: true,
		},
		{
			name:             "canceled lock context fails fast",
			threadID:         "th-1",
			cancelBeforeLock: true,
			wantLockErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newLockFakeDB(t)
			locker := NewThreadLocker(fake)
			if tc.nilDB {
				locker = NewThreadLocker(nil)
			}

			lockCtx, cancelLock := context.WithCancel(context.Background())
			defer cancelLock()
			if tc.cancelBeforeLock {
				cancelLock()
			}
			release, err := locker.Lock(lockCtx, tc.threadID)
			if (err != nil) != tc.wantLockErr {
				t.Fatalf("Lock() err = %v, wantLockErr = %v", err, tc.wantLockErr)
			}
			if (release != nil) != tc.wantRelease {
				t.Errorf("Lock() release non-nil = %v, want %v", release != nil, tc.wantRelease)
			}
		})
	}
}

func TestThreadLocker_Release(t *testing.T) {
	tests := []struct {
		name               string
		cancelBeforeUnlock bool
		failOn             string
		wantUnlockCalled   bool
		wantReleaseErr     bool
	}{
		{
			name:             "release frees the session lock",
			wantUnlockCalled: true,
		},
		{
			name:               "ctx-expired unlock still releases via WithoutCancel",
			cancelBeforeUnlock: true,
			wantUnlockCalled:   true,
		},
		{
			name:             "unlock error surfaces for conn destroy",
			failOn:           "UNLOCK",
			wantUnlockCalled: true,
			wantReleaseErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newLockFakeDB(t)
			fake.failOn = tc.failOn
			if tc.failOn != "" {
				fake.failErr = errors.New("unlock exploded")
			}
			locker := NewThreadLocker(fake)

			lockCtx, cancelLock := context.WithCancel(context.Background())
			defer cancelLock()
			release, err := locker.Lock(lockCtx, "th-1")
			if err != nil {
				t.Fatalf("Lock() err = %v", err)
			}
			if tc.cancelBeforeUnlock {
				cancelLock()
			}
			err = release()
			if (err != nil) != tc.wantReleaseErr {
				t.Errorf("release() err = %v, wantReleaseErr = %v", err, tc.wantReleaseErr)
			}
			if got := lockQueriesContain(fake.recorded(), "pg_advisory_unlock("); got != tc.wantUnlockCalled {
				t.Errorf("unlock called = %v, want %v (queries=%v)", got, tc.wantUnlockCalled, fake.recorded())
			}
		})
	}
}

func TestWithFactWriteLock(t *testing.T) {
	tests := []struct {
		name         string
		lockKey      string
		xactAcquired bool
		failOn       string
		fnErr        bool
		wantErr      bool
		wantBusy     bool
		wantQueries  []string
	}{
		{
			name:         "happy path begins locks and commits",
			lockKey:      "th-1",
			xactAcquired: true,
			wantQueries:  []string{"BEGIN", "pg_try_advisory_xact_lock(", "COMMIT"},
		},
		{
			name:         "empty key refuses without queries",
			lockKey:      "  ",
			xactAcquired: true,
			wantErr:      true,
			wantQueries:  []string{},
		},
		{
			name:         "busy lock rolls back with sentinel",
			lockKey:      "th-1",
			xactAcquired: false,
			wantErr:      true,
			wantBusy:     true,
			wantQueries:  []string{"BEGIN", "pg_try_advisory_xact_lock(", "ROLLBACK"},
		},
		{
			name:         "fn error rolls back",
			lockKey:      "th-1",
			xactAcquired: true,
			fnErr:        true,
			wantErr:      true,
			wantQueries:  []string{"BEGIN", "pg_try_advisory_xact_lock(", "ROLLBACK"},
		},
		{
			name:         "begin error aborts",
			lockKey:      "th-1",
			xactAcquired: true,
			failOn:       "BEGIN",
			wantErr:      true,
			wantQueries:  []string{"BEGIN"},
		},
		{
			name:         "commit error rolls back",
			lockKey:      "th-1",
			xactAcquired: true,
			failOn:       "COMMIT",
			wantErr:      true,
			wantQueries:  []string{"BEGIN", "pg_try_advisory_xact_lock(", "COMMIT", "ROLLBACK"},
		},
		{
			name:         "lock query error rolls back",
			lockKey:      "th-1",
			xactAcquired: true,
			failOn:       "XACTLOCK",
			wantErr:      true,
			wantQueries:  []string{"BEGIN", "pg_try_advisory_xact_lock(", "ROLLBACK"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newLockFakeDB(t)
			fake.xactAcquired = tc.xactAcquired
			fake.failOn = tc.failOn
			if tc.failOn != "" {
				fake.failErr = errors.New("injected failure")
			}
			fnErr := errors.New("fn exploded")
			wantFnErr := tc.fnErr
			err := withFactWriteLock(context.Background(), fake, tc.lockKey, func(_ context.Context) error {
				if wantFnErr {
					return fnErr
				}
				return nil
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("withFactWriteLock() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantBusy && !errors.Is(err, ErrAdvisoryLockBusy) {
				t.Errorf("withFactWriteLock() err = %v, want ErrAdvisoryLockBusy", err)
			}
			if tc.fnErr && !errors.Is(err, fnErr) {
				t.Errorf("withFactWriteLock() err = %v, want fn error", err)
			}
			got := fake.recorded()
			if len(got) != len(tc.wantQueries) {
				t.Fatalf("queries = %v, want %d queries %v", got, len(tc.wantQueries), tc.wantQueries)
			}
			for i, want := range tc.wantQueries {
				if !strings.Contains(got[i], want) {
					t.Errorf("queries[%d] = %q, want substring %q", i, got[i], want)
				}
			}
		})
	}
}
