package runner

import (
	"sync"
	"sync/atomic"
	"time"
)

// QuotaCoordinator coordinates cross-turn quota lockouts (e.g. 429 RESOURCE_EXHAUSTED
// or temporary capacity issues), providing atomic lockout status checks for edge fast-rejection
// and a broadcast resume channel to unblock paused workers.
type QuotaCoordinator struct {
	quotaLockedUntil atomic.Int64
	resumeCh         chan struct{}
	mu               sync.Mutex
	timer            *time.Timer
}

// NewQuotaCoordinator returns a coordinator in the unlocked state:
// resumeCh is an already-closed channel (so non-blocking on select),
// quotaLockedUntil is 0, and timer is nil.
func NewQuotaCoordinator() *QuotaCoordinator {
	ch := make(chan struct{})
	close(ch)
	return &QuotaCoordinator{
		resumeCh: ch,
	}
}

// IsLocked performs a pure atomic read returning true if the current time is before
// the quota locked until timestamp. This is suitable for high-frequency gateway ingress checks.
func (q *QuotaCoordinator) IsLocked() bool {
	return time.Now().Unix() < q.quotaLockedUntil.Load()
}

// ResumeChannel returns the current broadcast resume channel under lock.
// Listeners unblock when the channel is closed upon lockout expiration.
func (q *QuotaCoordinator) ResumeChannel() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.resumeCh
}

// SetLockout sets a lockout until the specified timestamp.
// It atomically records the lockout expiry timestamp, stops any existing timer,
// and initializes a fresh resume channel that closes when the lockout expires.
func (q *QuotaCoordinator) SetLockout(until time.Time) {
	q.quotaLockedUntil.Store(until.Unix())

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}

	q.resumeCh = make(chan struct{})
	remaining := time.Until(until)
	if remaining <= 0 {
		close(q.resumeCh)
		return
	}

	ch := q.resumeCh
	q.timer = time.AfterFunc(remaining, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.resumeCh == ch {
			close(ch)
			q.timer = nil
		}
	})
}
