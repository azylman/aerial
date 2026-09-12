package queue

import (
	"database/sql"
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
func RecoverInterrupted(database *sql.DB, pool *WorkerPool) {
	if database == nil || pool == nil {
		return
	}

	if reconciled, err := db.ReconcileOrphanedScheduleRuns(database); err != nil {
		log.Printf("[Startup Recovery] Error reconciling orphaned schedule runs: %v", err)
	} else if reconciled > 0 {
		log.Printf("[Startup Recovery] Reconciled %d orphaned schedule run(s) from previous run", reconciled)
	}

	messages, err := db.GetPendingOrProcessingMessages(database)
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

	log.Printf("[Startup Recovery] Resuming %d interrupted message(s) in chronological FIFO order...", len(messages))
	for _, m := range messages {
		isInstantCrashLoop := m.Status == db.StatusProcessing && time.Since(m.UpdatedAt) < CrashLoopVelocityThreshold

		if m.Status == db.StatusProcessing && (m.RestartCount >= MaxHardRestarts || (m.RestartCount >= DefaultMaxRestarts && isInstantCrashLoop) || m.RetryCount >= maxAttempts) {
			reason := "poison pill: exceeded restart limit during crash recovery"
			if m.RetryCount >= maxAttempts {
				reason = "poison pill: exceeded retry limit during crash recovery"
			}
			log.Printf("[Startup Recovery] Poison pill detected for message %s (restart_count=%d, retry_count=%d): %s. Dropping message.", m.ID, m.RestartCount, m.RetryCount, reason)
			snippet := m.Content
			if len([]rune(snippet)) > 60 {
				snippet = string([]rune(snippet)[:57]) + "..."
			}
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
			_ = db.UpdateMessageStatus(database, m.ID, db.StatusFailed, reason)
			continue
		}

		if m.Status == db.StatusProcessing {
			if m.RestartCount >= DefaultMaxRestarts && !isInstantCrashLoop {
				log.Printf("[Startup Recovery] Message %s was active for %v before restart (restart_count=%d < %d). Treating as long-running task interrupted by deployment rather than instant crash loop.", m.ID, time.Since(m.UpdatedAt), m.RestartCount, MaxHardRestarts)
			}
			_ = db.ResetMessageToPendingWithRestart(database, m.ID, "interrupted during restart")
			m.Status = db.StatusPending
			m.RestartCount++
		}

		metrics.InterruptedTurnsRecovered.Inc()
		log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, restart_count: %d, retry_count: %d)",
			m.ID, m.ThreadID, m.Status, m.RestartCount, m.RetryCount)
		pool.Enqueue(m)
	}
}
