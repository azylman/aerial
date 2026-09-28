package runner

import (
	"sync"
	"testing"
	"time"
)

func TestQuotaCoordinator_LockoutAndResume(t *testing.T) {
	qc := NewQuotaCoordinator()
	if qc.IsLocked() {
		t.Fatal("expected coordinator to be unlocked initially")
	}

	select {
	case <-qc.ResumeChannel():
	default:
		t.Fatal("expected resume channel to be unblocked initially")
	}

	// Set a 1.2s lockout to ensure until.Unix() > time.Now().Unix()
	until := time.Now().Add(1200 * time.Millisecond)
	qc.SetLockout(until)

	if !qc.IsLocked() {
		t.Fatal("expected coordinator to be locked")
	}

	ch := qc.ResumeChannel()
	select {
	case <-ch:
		t.Fatal("expected resume channel to be blocked during lockout")
	default:
	}

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("expected resume channel to unblock after lockout expires")
	}

	if qc.IsLocked() {
		t.Fatal("expected coordinator to be unlocked after lockout expires")
	}
}

func TestQuotaCoordinator_PastExpiration(t *testing.T) {
	qc := NewQuotaCoordinator()

	// Direct past expiration
	past := time.Now().Add(-1 * time.Second)
	qc.SetLockout(past)

	if qc.IsLocked() {
		t.Fatal("expected IsLocked to be false for past lockout")
	}

	select {
	case <-qc.ResumeChannel():
	default:
		t.Fatal("expected resume channel to be closed immediately for past lockout")
	}

	// Active lockout superseded by past expiration
	future := time.Now().Add(10 * time.Second)
	qc.SetLockout(future)
	if !qc.IsLocked() {
		t.Fatal("expected coordinator to be locked")
	}

	qc.SetLockout(past)
	if qc.IsLocked() {
		t.Fatal("expected coordinator to immediately unlock when set to past lockout")
	}

	select {
	case <-qc.ResumeChannel():
	default:
		t.Fatal("expected resume channel to be closed immediately after canceling with past lockout")
	}
}

func TestQuotaCoordinator_OverwriteLockout_Longer(t *testing.T) {
	qc := NewQuotaCoordinator()

	// Initial short lockout of 50ms
	qc.SetLockout(time.Now().Add(50 * time.Millisecond))
	ch1 := qc.ResumeChannel()

	// Overwrite with longer lockout of 2 seconds
	qc.SetLockout(time.Now().Add(2 * time.Second))
	ch2 := qc.ResumeChannel()

	if ch1 == ch2 {
		t.Fatal("expected old channel to be replaced on new lockout")
	}

	// Wait 100ms: old timer (50ms) should have been cancelled so ch1 does NOT close
	select {
	case <-ch1:
		t.Fatal("expected ch1 to not close because its timer was cancelled")
	case <-time.After(100 * time.Millisecond):
	}

	// ch2 must still be blocked
	select {
	case <-ch2:
		t.Fatal("expected ch2 to be blocked")
	default:
	}

	if !qc.IsLocked() {
		t.Fatal("expected coordinator to remain locked")
	}
}

func TestQuotaCoordinator_OverwriteLockout_Shorter(t *testing.T) {
	qc := NewQuotaCoordinator()

	// Initial long lockout of 10 seconds
	qc.SetLockout(time.Now().Add(10 * time.Second))
	ch1 := qc.ResumeChannel()

	// Overwrite with shorter lockout of 50ms
	qc.SetLockout(time.Now().Add(50 * time.Millisecond))
	ch2 := qc.ResumeChannel()

	if ch1 == ch2 {
		t.Fatal("expected old channel to be replaced on new lockout")
	}

	// ch2 should unblock quickly (~50ms)
	select {
	case <-ch2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ch2 to unblock within 500ms")
	}
}

func TestQuotaCoordinator_Concurrent(t *testing.T) {
	qc := NewQuotaCoordinator()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Readers calling IsLocked
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = qc.IsLocked()
				}
			}
		}()
	}

	// Readers calling ResumeChannel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					ch := qc.ResumeChannel()
					select {
					case <-ch:
					default:
					}
				}
			}
		}()
	}

	// Writers calling SetLockout
	for i := 0; i < 5; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					switch workerID % 3 {
					case 0:
						qc.SetLockout(time.Now().Add(-100 * time.Millisecond))
					case 1:
						qc.SetLockout(time.Now().Add(10 * time.Millisecond))
					case 2:
						qc.SetLockout(time.Now().Add(1 * time.Second))
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
}
