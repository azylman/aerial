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

func evaluatePoisonPill(status string, restartCount, retryCount, maxAttempts int, updatedAt time.Time) (isPoison bool, reason string, isInstantCrashLoop bool) {
	if status != db.StatusProcessing {
		return false, "", false
	}
	isInstantCrashLoop = time.Since(updatedAt) < CrashLoopVelocityThreshold
	if restartCount >= MaxHardRestarts || (restartCount >= DefaultMaxRestarts && isInstantCrashLoop) || retryCount >= maxAttempts {
		if retryCount >= maxAttempts {
			return true, "poison pill: exceeded retry limit during crash recovery", isInstantCrashLoop
		}
		return true, "poison pill: exceeded restart limit during crash recovery", isInstantCrashLoop
	}
	return false, "", isInstantCrashLoop
}

func isPoisonPillMessage(m db.Message, maxAttempts int) (isPoison bool, reason string, isInstantCrashLoop bool) {
	return evaluatePoisonPill(m.Status, m.RestartCount, m.RetryCount, maxAttempts, m.UpdatedAt)
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

func prepareInterruptedMessages(ctx context.Context, store db.Store, pool *WorkerPool, maxAttempts int) []db.Message {
	messages, err := store.GetPendingOrProcessingMessages(ctx, 0)
	if err != nil {
		log.Printf("[Startup Recovery] Error querying pending/processing messages: %v", err)
		return nil
	}
	if len(messages) == 0 {
		return nil
	}

	var ready []db.Message
	for _, m := range messages {
		isPoison, poisonReason, isInstantCrashLoop := isPoisonPillMessage(m, maxAttempts)
		if isPoison {
			handlePoisonPillMessage(ctx, store, pool, m, poisonReason)
			continue
		}
		resetInterruptedProcessingMessage(ctx, store, &m, isInstantCrashLoop)
		ready = append(ready, m)
	}
	return ready
}

func resetOrFailInterruptedDispatch(ctx context.Context, store db.Store, d *db.Dispatch, maxAttempts int) bool {
	isPoison, poisonReason, isInstantCrashLoop := evaluatePoisonPill(d.Status, d.RestartCount, d.RetryCount, maxAttempts, d.UpdatedAt)
	if isPoison {
		log.Printf("[Startup Recovery] Dispatch %s exceeded max restarts/retries (%s). Marking FAILED.", d.ID, poisonReason)
		if err := store.UpdateDispatchStatus(ctx, d.ID, db.StatusFailed, poisonReason); err != nil {
			log.Printf("[Startup Recovery] Failed to update dispatch %s status to FAILED: %v", d.ID, err)
		}
		return false
	}
	if d.Status == db.StatusProcessing {
		if d.RestartCount >= DefaultMaxRestarts && !isInstantCrashLoop {
			log.Printf("[Startup Recovery] Dispatch %s was active for %v before restart (restart_count=%d < %d). Treating as long-running dispatch.", d.ID, time.Since(d.UpdatedAt), d.RestartCount, MaxHardRestarts)
		}
		if err := store.ResetDispatchToPendingWithRestart(ctx, d.ID, "startup_recovery"); err != nil {
			log.Printf("[Startup Recovery] Failed to reset dispatch %s to pending: %v", d.ID, err)
		}
		d.Status = db.StatusPending
		d.RestartCount++
	}
	return true
}

func prepareInterruptedDispatches(ctx context.Context, store db.Store, maxAttempts int) []db.Dispatch {
	dispatches, err := store.GetPendingOrProcessingDispatches(ctx, 0)
	if err != nil {
		log.Printf("[Startup Recovery] Error querying pending/processing dispatches: %v", err)
		return nil
	}
	var ready []db.Dispatch
	for _, d := range dispatches {
		if resetOrFailInterruptedDispatch(ctx, store, &d, maxAttempts) {
			ready = append(ready, d)
		}
	}
	return ready
}

func mergeHeldPendingEntries(ctx context.Context, store db.Store, msgs []db.Message, disps []db.Dispatch) ([]db.Message, []db.Dispatch) {
	seenMsgs := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		seenMsgs[m.ID] = true
	}
	if extraMsgs, err := store.GetPendingOrProcessingMessages(ctx, 0); err == nil {
		for _, m := range extraMsgs {
			if m.Status == db.StatusPending && !seenMsgs[m.ID] {
				msgs = append(msgs, m)
			}
		}
	}
	seenDisps := make(map[string]bool, len(disps))
	for _, d := range disps {
		seenDisps[d.ID] = true
	}
	if extraDisps, err := store.GetPendingOrProcessingDispatches(ctx, 0); err == nil {
		for _, d := range extraDisps {
			if d.Status == db.StatusPending && !seenDisps[d.ID] {
				disps = append(disps, d)
			}
		}
	}
	return msgs, disps
}

// RecoverInterrupted resumes all PENDING and PROCESSING messages and dispatches from the database on startup in chronological order.
// Orphaned PROCESSING entries are reset to PENDING before releasing any canary leader hold on the pool.
func RecoverInterrupted(store db.Store, pool *WorkerPool) {
	if store == nil || pool == nil {
		return
	}

	ctx := context.Background()
	maxAttempts := pool.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	msgs := prepareInterruptedMessages(ctx, store, pool, maxAttempts)
	disps := prepareInterruptedDispatches(ctx, store, maxAttempts)

	if pool.ReleaseLeaderHold() {
		msgs, disps = mergeHeldPendingEntries(ctx, store, msgs, disps)
	}

	if len(msgs) > 0 {
		log.Printf("[Startup Recovery] Resuming %d interrupted message(s) in chronological FIFO order...", len(msgs))
		for _, m := range msgs {
			metrics.InterruptedTurnsRecovered.Inc()
			log.Printf("[Startup Recovery] Enqueuing message %s (thread: %s, status: %s, restart_count: %d, retry_count: %d)",
				m.ID, m.ThreadID, m.Status, m.RestartCount, m.RetryCount)
			pool.Enqueue(m)
		}
	}
	if len(disps) > 0 {
		log.Printf("[Startup Recovery] Resuming %d interrupted dispatch(es)...", len(disps))
		for _, d := range disps {
			metrics.InterruptedTurnsRecovered.Inc()
			log.Printf("[Startup Recovery] Enqueuing dispatch %s (source: %s, session_id: %s, status: %s, restart_count: %d, retry_count: %d)",
				d.ID, d.Source, d.SessionID, d.Status, d.RestartCount, d.RetryCount)
			pool.EnqueueDispatch(d)
		}
	}
}
