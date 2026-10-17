package runner

import (
	"sync"
	"testing"
	"time"
)

func TestQuotaCoordinator_LifecycleAndOverwrites(t *testing.T) {
	qc := NewQuotaCoordinator()
	if qc.IsLocked() {
		t.Fatal("expected coordinator to be unlocked initially")
	}

	select {
	case <-qc.ResumeChannel():
	default:
		t.Fatal("expected resume channel to be unblocked initially")
	}

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

	// Set 25ms lockout
	until := time.Now().Add(25 * time.Millisecond)
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
	case <-time.After(1 * time.Second):
		t.Fatal("expected resume channel to unblock after lockout expires")
	}

	if qc.IsLocked() {
		t.Fatal("expected coordinator to be unlocked after lockout expires")
	}

	// Overwrite shorter with longer
	qc.SetLockout(time.Now().Add(25 * time.Millisecond))
	ch1 := qc.ResumeChannel()
	qc.SetLockout(time.Now().Add(2 * time.Second))
	ch2 := qc.ResumeChannel()
	if ch1 == ch2 {
		t.Fatal("expected old channel to be replaced on new lockout")
	}

	select {
	case <-ch1:
		t.Fatal("expected ch1 to not close because its timer was cancelled")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-ch2:
		t.Fatal("expected ch2 to be blocked")
	default:
	}
	if !qc.IsLocked() {
		t.Fatal("expected coordinator to remain locked")
	}

	// Overwrite longer with shorter (25ms)
	qc.SetLockout(time.Now().Add(25 * time.Millisecond))
	ch3 := qc.ResumeChannel()
	if ch2 == ch3 {
		t.Fatal("expected ch2 to be replaced by ch3")
	}
	select {
	case <-ch3:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ch3 to unblock within 500ms")
	}

	// Overwrite active lockout with past expiration
	qc.SetLockout(time.Now().Add(10 * time.Second))
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

func TestQuotaCoordinator_Concurrent(t *testing.T) {
	qc := NewQuotaCoordinator()
	var wg sync.WaitGroup

	// Readers calling IsLocked
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = qc.IsLocked()
			}
		}()
	}

	// Readers calling ResumeChannel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ch := qc.ResumeChannel()
				select {
				case <-ch:
				default:
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
			for j := 0; j < 100; j++ {
				switch (workerID + j) % 3 {
				case 0:
					qc.SetLockout(time.Now().Add(-100 * time.Millisecond))
				case 1:
					qc.SetLockout(time.Now().Add(10 * time.Millisecond))
				case 2:
					qc.SetLockout(time.Now().Add(1 * time.Second))
				}
			}
		}()
	}

	wg.Wait()
}
