package queue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

const (
	MaxHardRestarts            = 5
	CrashLoopVelocityThreshold = 30 * time.Second
)

func isPoisonPillMessage(m db.Message, maxAttempts int) (isPoison bool, reason string, isInstantCrashLoop bool) {
	if m.Status != db.StatusProcessing {
		return false, "", false
	}
	isInstantCrashLoop = time.Since(m.UpdatedAt) < CrashLoopVelocityThreshold
	if m.RestartCount >= MaxHardRestarts || (m.RestartCount >= DefaultMaxRestarts && isInstantCrashLoop) || m.RetryCount >= maxAttempts {
		if m.RetryCount >= maxAttempts {
			return true, "poison pill: exceeded retry limit during crash recovery", isInstantCrashLoop
		}
		return true, "poison pill: exceeded restart limit during crash recovery", isInstantCrashLoop
	}
	return false, "", isInstantCrashLoop
}

func handlePoisonPillMessage(ctx context.Context, store db.Store, pool *WorkerPool, m db.Message, reason string) {
	log.Printf("[Startup Recovery] Poison pill detected for message %s (restart_count=%d, retry_count=%d): %s. Dropping message.", m.ID, m.RestartCount, m.RetryCount, reason)
	cleanSnippet := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(sanitizeErrorText(m.Content), "\r", ""), "\n", " "))
	if runes := []rune(cleanSnippet); len(runes) > 60 {
		cleanSnippet = string(runes[:57]) + "..."
	}
	desc := "a message caused repeated crashes and had to be dropped"
	if cleanSnippet != "" {
		desc = fmt.Sprintf("a message caused repeated crashes and had to be dropped (message snippet: %q)", cleanSnippet)
	}
	agyBin, apiKey := pool.runnerCredentials()
	notif := pool.cfg.NotifierFunc(agyBin, apiKey, desc)
	if m.AuthorID != "http-client" && pool.cfg.DeliveryFunc != nil {
		if err := pool.cfg.DeliveryFunc(pool.getDiscordSession(), m.ThreadID, notif); err != nil {
			log.Printf("[Startup Recovery] Failed to deliver poison pill notice for message %s: %v", m.ID, err)
		}
	}
	if err := store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, reason); err != nil {
		log.Printf("[Startup Recovery] Failed to update message %s status to failed: %v", m.ID, err)
	}
}

func resetInterruptedProcessingMessage(ctx context.Context, store db.Store, m *db.Message, isInstantCrashLoop bool) {
	if m.Status != db.StatusProcessing {
		return
	}
	if m.RestartCount >= DefaultMaxRestarts && !isInstantCrashLoop {
		log.Printf("[Startup Recovery] Message %s was active for %v before restart (restart_count=%d < %d). Treating as long-running task interrupted by deployment rather than instant crash loop.", m.ID, time.Since(m.UpdatedAt), m.RestartCount, MaxHardRestarts)
	}
	if err := store.ResetMessageToPendingWithRestart(ctx, m.ID, "interrupted during restart"); err != nil {
		log.Printf("[Startup Recovery] Failed to reset message %s to pending: %v", m.ID, err)
	}
	m.Status = db.StatusPending
	m.RestartCount++
}

func recoverInterruptedMessages(ctx context.Context, store db.Store, pool *WorkerPool, maxAttempts int) {
	messages, err := store.GetPendingOrProcessingMessages(ctx, 0)
	if err != nil {
		log.Printf("[Startup Recovery] Error querying pending/processing messages: %v", err)
		return
	}
	if len(messages) == 0 {
		return
	}

	log.Printf("[Startup Recovery] Resuming %d interrupted message(s) in chronological FIFO order...", len(messages))
	for _, m := range messages {
		isPoison, poisonReason, isInstantCrashLoop := isPoisonPillMessage(m, maxAttempts)
		if isPoison {
			handlePoisonPillMessage(ctx, store, pool, m, poisonReason)
			continue
		}

		resetInterruptedProcessingMessage(ctx, store, &m, isInstantCrashLoop)

		metrics.InterruptedTurnsRecovered.Inc()
		log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, restart_count: %d, retry_count: %d)",
			m.ID, m.ThreadID, m.Status, m.RestartCount, m.RetryCount)
		pool.Enqueue(m)
	}
}

func recoverInterruptedDispatches(ctx context.Context, store db.Store, pool *WorkerPool, maxAttempts int) {
	dispatches, err := store.GetPendingOrProcessingDispatches(ctx, 0)
	if err != nil {
		log.Printf("[Startup Recovery] Error querying pending/processing dispatches: %v", err)
		return
	}
	if len(dispatches) == 0 {
		return
	}

	log.Printf("[Startup Recovery] Resuming %d interrupted dispatch(es)...", len(dispatches))
	for _, d := range dispatches {
		if d.RestartCount >= DefaultMaxRestarts || d.RetryCount >= maxAttempts {
			log.Printf("[Startup Recovery] Dispatch %s exceeded max restarts/retries. Marking FAILED.", d.ID)
			if err := store.UpdateDispatchStatus(ctx, d.ID, db.StatusFailed, "poison pill: exceeded max restarts or attempts"); err != nil {
				log.Printf("[Startup Recovery] Failed to update dispatch %s status to FAILED: %v", d.ID, err)
			}
			continue
		}
		if d.Status == db.StatusProcessing {
			if err := store.ResetDispatchToPendingWithRestart(ctx, d.ID, "startup_recovery"); err != nil {
				log.Printf("[Startup Recovery] Failed to reset dispatch %s to pending: %v", d.ID, err)
			}
			d.Status = db.StatusPending
			d.RestartCount++
		}
		metrics.InterruptedTurnsRecovered.Inc()
		log.Printf("[Startup Recovery] Enqueuing dispatch %s (source: %s, session_id: %s, status: %s, restart_count: %d, retry_count: %d)",
			d.ID, d.Source, d.SessionID, d.Status, d.RestartCount, d.RetryCount)
		pool.EnqueueDispatch(d)
	}
}

// RecoverInterrupted resumes all PENDING and PROCESSING messages from the database on startup in chronological order.
// If a message in PROCESSING has restart_count >= DefaultMaxRestarts or retry_count >= maxAttempts (poison pill),
// it is not re-enqueued; instead, a poison pill notification is sent and the message is marked FAILED.
func RecoverInterrupted(store db.Store, pool *WorkerPool) {
	if store == nil || pool == nil {
		return
	}

	ctx := context.Background()
	maxAttempts := pool.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	recoverInterruptedMessages(ctx, store, pool, maxAttempts)
	recoverInterruptedDispatches(ctx, store, pool, maxAttempts)
}
