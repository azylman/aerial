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

// RecoverInterrupted resumes all PENDING and PROCESSING messages from the database on startup in chronological order.
// If a message in PROCESSING has restart_count >= DefaultMaxRestarts or retry_count >= maxAttempts (poison pill),
// it is not re-enqueued; instead, a poison pill notification is sent and the message is marked FAILED.
func RecoverInterrupted(store db.Store, pool *WorkerPool) {
	if store == nil || pool == nil {
		return
	}

	ctx := context.Background()

	messages, err := store.GetPendingOrProcessingMessages(ctx, 0)
	if err != nil {
		log.Printf("[Startup Recovery] Error querying pending/processing messages: %v", err)
		return
	}
	if len(messages) == 0 {
		log.Printf("[Startup Recovery] No interrupted messages found. System clean.")
		return
	}

	maxAttempts := pool.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	stalenessTTL := pool.cfg.StalenessTTL
	if stalenessTTL <= 0 {
		stalenessTTL = 30 * time.Minute
	}

	log.Printf("[Startup Recovery] Resuming %d interrupted message(s) in chronological FIFO order...", len(messages))
	for _, m := range messages {
		isInstantCrashLoop := m.Status == db.StatusProcessing && time.Since(m.UpdatedAt) < CrashLoopVelocityThreshold

		if m.Status == db.StatusProcessing && (m.RestartCount >= MaxHardRestarts || (m.RestartCount >= DefaultMaxRestarts && isInstantCrashLoop) || m.RetryCount >= maxAttempts) {
			reason := "poison pill: exceeded restart limit during crash recovery"
			if m.RetryCount >= maxAttempts {
				reason = "poison pill: exceeded retry limit during crash recovery"
			}
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
			continue
		}

		// Check for superseding completed turns or expired stranded pending messages
		var isStale bool
		var staleReason string

		stats, statsErr := store.GetSessionActivityStats(ctx, m.ThreadID)
		if statsErr == nil && stats != nil && !stats.LastCompletedMessageCreatedAt.IsZero() && stats.LastCompletedMessageCreatedAt.After(m.CreatedAt) {
			isStale = true
			staleReason = fmt.Sprintf("superseded by newer completed message in thread (completed created_at %s > msg %s)",
				stats.LastCompletedMessageCreatedAt.Format(time.RFC3339), m.CreatedAt.Format(time.RFC3339))
		} else if m.Status == db.StatusPending && !m.CreatedAt.IsZero() && time.Since(m.CreatedAt) > stalenessTTL {
			isStale = true
			staleReason = fmt.Sprintf("stranded pending message age %v exceeds staleness TTL %v",
				time.Since(m.CreatedAt).Round(time.Second), stalenessTTL)
		}

		if isStale {
			log.Printf("[Startup Recovery] Dropping stale message %s (thread: %s): %s. Marked [EXPIRED_STALE].",
				m.ID, m.ThreadID, staleReason)
			if err := store.UpdateMessageCompleted(ctx, m.ID, "[EXPIRED_STALE]"); err != nil {
				log.Printf("[Startup Recovery] Failed to update message %s status to [EXPIRED_STALE]: %v", m.ID, err)
			}
			continue
		}

		if m.Status == db.StatusProcessing {
			if m.RestartCount >= DefaultMaxRestarts && !isInstantCrashLoop {
				log.Printf("[Startup Recovery] Message %s was active for %v before restart (restart_count=%d < %d). Treating as long-running task interrupted by deployment rather than instant crash loop.", m.ID, time.Since(m.UpdatedAt), m.RestartCount, MaxHardRestarts)
			}
			if err := store.ResetMessageToPendingWithRestart(ctx, m.ID, "interrupted during restart"); err != nil {
				log.Printf("[Startup Recovery] Failed to reset message %s to pending: %v", m.ID, err)
			}
			m.Status = db.StatusPending
			m.RestartCount++
		}

		metrics.InterruptedTurnsRecovered.Inc()
		log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, restart_count: %d, retry_count: %d)",
			m.ID, m.ThreadID, m.Status, m.RestartCount, m.RetryCount)
		pool.Enqueue(m)
	}
}
