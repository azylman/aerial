package queue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
)

const (
	MaxHardRestarts            = 5
	CrashLoopVelocityThreshold = 30 * time.Second
)

// RecoverInterrupted resumes all PENDING and PROCESSING messages from the database on startup in chronological order.
// If a message in PROCESSING has restart_count >= DefaultMaxRestarts or retry_count >= maxAttempts (poison pill),
// it is not re-enqueued; instead, a poison pill notification is sent and the message is marked FAILED.
func RecoverInterrupted(dbOrStore any, pool *WorkerPool) {
	store := resolveStore(dbOrStore)
	if store == nil || pool == nil {
		return
	}

	ctx := context.Background()

	if reconciled, err := store.ReconcileOrphanedScheduleRuns(ctx); err != nil {
		log.Printf("[Startup Recovery] Error reconciling orphaned schedule runs: %v", err)
	} else if reconciled > 0 {
		log.Printf("[Startup Recovery] Reconciled %d orphaned schedule run(s) from previous run", reconciled)
	}

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
			cleanSnippet := sanitizeErrorText(m.Content)
			cleanSnippet = strings.ReplaceAll(cleanSnippet, "\n", " ")
			cleanSnippet = strings.ReplaceAll(cleanSnippet, "\r", "")
			cleanSnippet = strings.TrimSpace(cleanSnippet)
			if len([]rune(cleanSnippet)) > 60 {
				cleanSnippet = string([]rune(cleanSnippet)[:57]) + "..."
			}
			snippet := cleanSnippet
			agyBin := pool.cfg.AgyBin
			apiKey := pool.cfg.APIKey
			if pool.appCfg != nil {
				if cur := pool.appCfg.Current(); cur != nil {
					if cur.AgyBin != "" {
						agyBin = cur.AgyBin
					}
					if cur.APIKey != "" {
						apiKey = cur.APIKey
					}
				}
			}
			desc := "a message caused repeated crashes and had to be dropped"
			if strings.TrimSpace(snippet) != "" {
				desc = fmt.Sprintf("a message caused repeated crashes and had to be dropped (message snippet: %q)", snippet)
			}
			notif := pool.cfg.NotifierFunc(agyBin, apiKey, desc)
			if m.AuthorID != "http-client" {
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
			if m.ScheduleRunID != "" {
				if err := store.UpdateScheduleRunStatus(ctx, db.UpdateRunParams{
					RunID:       m.ScheduleRunID,
					MessageID:   m.ID,
					Status:      "completed",
					CompletedAt: time.Now().UTC(),
					DurationMs:  0,
					Error:       "[EXPIRED_STALE]",
				}); err != nil {
					log.Printf("[Startup Recovery] Failed to update schedule run %s status: %v", m.ScheduleRunID, err)
				}
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
