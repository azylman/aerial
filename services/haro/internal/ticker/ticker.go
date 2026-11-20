// Package ticker implements Haro's scheduling loop (Layer 3).
//
// Ticker owns the scheduling half of the former root ticker.go: periodic
// evaluation of due cron, one-shot and heartbeat schedules, outcome timing
// for dispatch results, monotonic heartbeat watermark advancement, and the
// dispatch-outcome consumer handler. It imports model, metrics, store and
// dispatch only; it never imports config, tools or server. The 30s tick
// cadence is supplied by the caller (main.go), and dispatch payloads,
// metric names/labels, and store query semantics are unchanged.
package ticker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/metrics"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

// Ticker periodically evaluates due schedules and dispatches them.
type Ticker struct {
	db         *sql.DB
	dispatcher dispatch.PromptDispatcher
	interval   time.Duration
	mu         sync.Mutex
	lastPrune  time.Time
}

// NewTicker returns a Ticker that evaluates schedules on interval.
// A non-positive interval defaults to 30 seconds.
func NewTicker(db *sql.DB, dispatcher dispatch.PromptDispatcher, interval time.Duration) *Ticker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Ticker{
		db:         db,
		dispatcher: dispatcher,
		interval:   interval,
	}
}

// EvaluateDueSchedules fires all due cron, one-shot and heartbeat schedules
// as of now, then prunes old schedule runs at most once per 24 hours.
func (t *Ticker) EvaluateDueSchedules(ctx context.Context, now time.Time) {
	if t == nil || t.db == nil || t.dispatcher == nil {
		return
	}

	t.evaluateDueCronSchedules(ctx, now)
	t.evaluateDueOneShotSchedules(ctx, now)
	t.evaluateDueHeartbeatSchedules(ctx, now)

	// Daily Retention Pruning
	t.mu.Lock()
	shouldPrune := model.ShouldPruneRuns(now, t.lastPrune, 24*time.Hour)
	if shouldPrune {
		t.lastPrune = now
	}
	t.mu.Unlock()

	if shouldPrune {
		if _, pruneErr := store.PruneScheduleRuns(ctx, t.db, 1000, 30*24*time.Hour, now); pruneErr != nil {
			log.Printf("[Haro Ticker] Error pruning schedule runs: %v", pruneErr)
		}
	}
}

func (t *Ticker) evaluateDueCronSchedules(ctx context.Context, now time.Time) {
	dueCrons, err := store.GetDueCronSchedules(ctx, t.db, now)
	if err != nil {
		log.Printf("[Haro Ticker] Error querying due cron schedules: %v", err)
		return
	}
	for _, c := range dueCrons {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.processDueCron(ctx, c, now)
	}
}

func (t *Ticker) processDueCron(ctx context.Context, c model.CronSchedule, now time.Time) {
	nextRun, calcErr := model.CalculateNextRun(c.CronExpr, c.Timezone, now)
	if calcErr != nil {
		log.Printf("[Haro Ticker] Failed to calculate next run for cron %s (%q): %v", c.ID, c.CronExpr, calcErr)
		nextRun = now.Add(24 * time.Hour)
	}

	runID := deterministicScheduleRunID(model.ScheduleTypeCron, c.ID, c.NextRunAt)
	if model.IsScheduleStale(c.NextRunAt, now, 24*time.Hour) {
		log.Printf("[Haro Ticker] Cron %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
			c.ID, c.CronExpr, c.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
		if advErr := store.AdvanceCronNextRun(ctx, t.db, c.ID, nextRun); advErr != nil {
			log.Printf("[Haro Ticker] Error advancing cron %s next_run_at: %v", c.ID, advErr)
		}
		completedAt := now
		staleRun := model.ScheduleRun{
			ID:           runID,
			ScheduleID:   c.ID,
			ScheduleType: model.ScheduleTypeCron,
			MessageID:    runID,
			TargetID:     c.TargetID,
			ThreadID:     c.TargetID,
			Title:        c.TitlePrefix,
			Prompt:       c.Prompt,
			Status:       model.StatusFailed,
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   0,
			Error:        "stale schedule (>24h overdue)",
			Effort:       c.Effort,
			Agent:        c.Agent,
		}
		if createErr := store.CreateScheduleRun(ctx, t.db, staleRun); createErr != nil {
			log.Printf("[Haro Ticker] Error creating stale schedule run for cron %s: %v", c.ID, createErr)
		}
		metrics.RecordScheduleRun(model.ScheduleTypeCron, model.StatusStale)
		return
	}

	if advErr := store.AdvanceCronNextRun(ctx, t.db, c.ID, nextRun); advErr != nil {
		log.Printf("[Haro Ticker] Error advancing cron %s next_run_at: %v", c.ID, advErr)
		return
	}

	req := dispatch.PromptDispatchRequest{
		Prompt:    c.Prompt,
		ChannelID: c.TargetID,
		Title:     c.TitlePrefix,
		MessageID: runID,
		Effort:    c.Effort,
		Agent:     c.Agent,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := dispatch.TraceFire(ctx, model.ScheduleTypeCron, c.ID, func(ctx context.Context) (dispatch.PromptDispatchResponse, error) {
		return t.dispatcher.DispatchPrompt(ctx, req)
	})
	metrics.RecordScheduleDispatchDuration(model.ScheduleTypeCron, time.Since(dispatchStart).Seconds())
	completedAt := time.Now().UTC()
	durationMs := completedAt.Sub(now).Milliseconds()
	status := model.StatusCompleted
	errMsg := ""
	threadID := resp.ChannelID
	if threadID == "" {
		threadID = c.TargetID
	}
	if dispatchErr != nil {
		status = model.StatusFailed
		errMsg = dispatchErr.Error()
		log.Printf("[Haro Ticker] Dispatch error for cron %s: %v", c.ID, dispatchErr)
	}
	metrics.RecordScheduleRun(model.ScheduleTypeCron, status)
	msgID := resp.MessageID
	if msgID == "" {
		msgID = runID
	}
	run := model.ScheduleRun{
		ID:           runID,
		ScheduleID:   c.ID,
		ScheduleType: model.ScheduleTypeCron,
		MessageID:    msgID,
		TargetID:     c.TargetID,
		ThreadID:     threadID,
		Title:        c.TitlePrefix,
		Prompt:       c.Prompt,
		Status:       status,
		StartedAt:    now,
		CompletedAt:  &completedAt,
		DurationMs:   durationMs,
		Error:        errMsg,
		Effort:       c.Effort,
		Agent:        c.Agent,
	}
	if createErr := store.CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for cron %s: %v", c.ID, createErr)
	}
}

func (t *Ticker) evaluateDueOneShotSchedules(ctx context.Context, now time.Time) {
	dueOneShots, err := store.GetDueOneShotSchedules(ctx, t.db, now)
	if err != nil {
		log.Printf("[Haro Ticker] Error querying due one-shot schedules: %v", err)
		return
	}
	for _, s := range dueOneShots {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.processDueOneShot(ctx, s, now)
	}
}

func (t *Ticker) processDueOneShot(ctx context.Context, s model.OneShotSchedule, now time.Time) {
	deleted, delErr := store.DeleteOneShotSchedule(ctx, t.db, s.ID)
	if delErr != nil || !deleted {
		if delErr != nil {
			log.Printf("[Haro Ticker] Error deleting one-shot schedule %s: %v", s.ID, delErr)
		}
		return
	}

	runID := deterministicScheduleRunID(model.ScheduleTypeOneShot, s.ID, s.RunAt)
	if model.IsScheduleStale(s.RunAt, now, 24*time.Hour) {
		log.Printf("[Haro Ticker] One-shot %s is stale (>24h overdue: due %s, now %s). Skipping dispatch.",
			s.ID, s.RunAt.Format(time.RFC3339), now.Format(time.RFC3339))
		completedAt := now
		staleRun := model.ScheduleRun{
			ID:           runID,
			ScheduleID:   s.ID,
			ScheduleType: model.ScheduleTypeOneShot,
			MessageID:    runID,
			TargetID:     s.ThreadID,
			ThreadID:     s.ThreadID,
			Prompt:       s.Prompt,
			Status:       model.StatusFailed,
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   0,
			Error:        "stale schedule (>24h overdue)",
			Effort:       s.Effort,
			Agent:        s.Agent,
		}
		if createErr := store.CreateScheduleRun(ctx, t.db, staleRun); createErr != nil {
			log.Printf("[Haro Ticker] Error creating stale schedule run for one-shot %s: %v", s.ID, createErr)
		}
		metrics.RecordScheduleRun(model.ScheduleTypeOneShot, model.StatusStale)
		return
	}

	req := dispatch.PromptDispatchRequest{
		Prompt:    s.Prompt,
		ChannelID: s.ThreadID,
		MessageID: runID,
		Effort:    s.Effort,
		Agent:     s.Agent,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := dispatch.TraceFire(ctx, model.ScheduleTypeOneShot, s.ID, func(ctx context.Context) (dispatch.PromptDispatchResponse, error) {
		return t.dispatcher.DispatchPrompt(ctx, req)
	})
	metrics.RecordScheduleDispatchDuration(model.ScheduleTypeOneShot, time.Since(dispatchStart).Seconds())
	completedAt := time.Now().UTC()
	durationMs := completedAt.Sub(now).Milliseconds()
	status := model.StatusCompleted
	errMsg := ""
	threadID := resp.ChannelID
	if threadID == "" {
		threadID = s.ThreadID
	}
	if dispatchErr != nil {
		status = model.StatusFailed
		errMsg = dispatchErr.Error()
		log.Printf("[Haro Ticker] Dispatch error for one-shot %s: %v", s.ID, dispatchErr)
	}
	metrics.RecordScheduleRun(model.ScheduleTypeOneShot, status)
	msgID := resp.MessageID
	if msgID == "" {
		msgID = runID
	}
	run := model.ScheduleRun{
		ID:           runID,
		ScheduleID:   s.ID,
		ScheduleType: model.ScheduleTypeOneShot,
		MessageID:    msgID,
		TargetID:     s.ThreadID,
		ThreadID:     threadID,
		Prompt:       s.Prompt,
		Status:       status,
		StartedAt:    now,
		CompletedAt:  &completedAt,
		DurationMs:   durationMs,
		Error:        errMsg,
		Effort:       s.Effort,
		Agent:        s.Agent,
	}
	if createErr := store.CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for one-shot %s: %v", s.ID, createErr)
	}
}

func (t *Ticker) evaluateDueHeartbeatSchedules(ctx context.Context, now time.Time) {
	dueHeartbeats, err := store.GetDueHeartbeatSchedules(ctx, t.db, now)
	if err != nil {
		log.Printf("[Haro Ticker] Error querying due heartbeat schedules: %v", err)
		return
	}
	for _, hb := range dueHeartbeats {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.processDueHeartbeat(ctx, hb, now)
	}
}

func (t *Ticker) processDueHeartbeat(ctx context.Context, hb model.HeartbeatSchedule, now time.Time) {
	nextRun, calcErr := model.CalculateNextRun(hb.CronExpr, hb.Timezone, now)
	if calcErr != nil {
		log.Printf("[Haro Ticker] Failed to calculate next run for heartbeat %s (%q): %v", hb.ID, hb.CronExpr, calcErr)
		nextRun = now.Add(24 * time.Hour)
	}

	runID := deterministicScheduleRunID(model.ScheduleTypeHeartbeat, hb.ID, hb.NextRunAt)
	if model.IsScheduleStale(hb.NextRunAt, now, 24*time.Hour) {
		log.Printf("[Haro Ticker] Heartbeat %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
			hb.ID, hb.CronExpr, hb.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
		if advErr := store.AdvanceHeartbeatNextRun(ctx, t.db, hb.ID, nextRun); advErr != nil {
			log.Printf("[Haro Ticker] Error advancing heartbeat %s next_run_at: %v", hb.ID, advErr)
		}
		completedAt := now
		staleRun := model.ScheduleRun{
			ID:           runID,
			ScheduleID:   hb.ID,
			ScheduleType: model.ScheduleTypeHeartbeat,
			MessageID:    runID,
			TargetID:     hb.TargetID,
			ThreadID:     "heartbeat-" + hb.ID,
			Title:        hb.TitlePrefix,
			Prompt:       hb.Prompt,
			Status:       model.StatusFailed,
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   0,
			Error:        "stale heartbeat (>24h overdue)",
			Effort:       hb.Effort,
			Agent:        hb.Agent,
		}
		if createErr := store.CreateScheduleRun(ctx, t.db, staleRun); createErr != nil {
			log.Printf("[Haro Ticker] Error creating stale schedule run for heartbeat %s: %v", hb.ID, createErr)
		}
		metrics.RecordScheduleRun(model.ScheduleTypeHeartbeat, model.StatusStale)
		return
	}

	if advErr := store.AdvanceHeartbeatNextRun(ctx, t.db, hb.ID, nextRun); advErr != nil {
		log.Printf("[Haro Ticker] Error advancing heartbeat %s next_run_at: %v", hb.ID, advErr)
		return
	}

	sessionKey := "heartbeat-" + hb.ID
	if active, activeErr := store.HasRunningHeartbeatRun(ctx, t.db, hb.ID, now.Add(-2*time.Hour)); activeErr == nil && active {
		metrics.RecordScheduleRun(model.ScheduleTypeHeartbeat, model.StatusSkippedActive)
		slog.Info("Skipping heartbeat run: schedule has an active run in progress", "id", hb.ID, "target_id", hb.TargetID, "thread_id", sessionKey)
		return
	}

	wrappedPrompt := model.BuildHeartbeatPrompt(hb, now)
	req := dispatch.DispatchRequest{
		ID:           runID,
		Source:       model.ScheduleTypeHeartbeat,
		SessionID:    "",
		TargetID:     strings.TrimSpace(hb.TargetID),
		Prompt:       wrappedPrompt,
		Effort:       hb.Effort,
		SkipIfActive: true,
		Agent:        hb.Agent,
	}

	dispatchStart := time.Now()
	resp, dispatchErr := dispatch.TraceFire(ctx, model.ScheduleTypeHeartbeat, hb.ID, func(ctx context.Context) (dispatch.DispatchResponse, error) {
		return t.dispatcher.Dispatch(ctx, req)
	})
	metrics.RecordScheduleDispatchDuration(model.ScheduleTypeHeartbeat, time.Since(dispatchStart).Seconds())
	if dispatchErr == nil && resp.Status == model.StatusSkippedActive {
		metrics.RecordScheduleRun(model.ScheduleTypeHeartbeat, model.StatusSkippedActive)
		slog.Info("Skipping heartbeat run: thread has an active turn in progress", "id", hb.ID, "target_id", hb.TargetID, "thread_id", sessionKey)
		return
	}
	if dispatchErr != nil {
		completedAt := time.Now().UTC()
		durationMs := completedAt.Sub(now).Milliseconds()
		log.Printf("[Haro Ticker] Dispatch error for heartbeat %s: %v", hb.ID, dispatchErr)
		metrics.RecordScheduleRun(model.ScheduleTypeHeartbeat, model.StatusFailed)
		run := model.ScheduleRun{
			ID:           runID,
			ScheduleID:   hb.ID,
			ScheduleType: model.ScheduleTypeHeartbeat,
			MessageID:    runID,
			TargetID:     hb.TargetID,
			ThreadID:     sessionKey,
			Title:        hb.TitlePrefix,
			Prompt:       hb.Prompt,
			Status:       model.StatusFailed,
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   durationMs,
			Error:        dispatchErr.Error(),
			Effort:       hb.Effort,
			Agent:        hb.Agent,
		}
		if createErr := store.CreateScheduleRun(ctx, t.db, run); createErr != nil {
			log.Printf("[Haro Ticker] Error creating schedule run for heartbeat %s: %v", hb.ID, createErr)
		}
		return
	}

	msgID := model.FirstNonEmpty(resp.DispatchID, resp.MessageID, runID)
	run := model.ScheduleRun{
		ID:           runID,
		ScheduleID:   hb.ID,
		ScheduleType: model.ScheduleTypeHeartbeat,
		MessageID:    msgID,
		TargetID:     hb.TargetID,
		ThreadID:     sessionKey,
		Title:        hb.TitlePrefix,
		Prompt:       hb.Prompt,
		Status:       "running",
		StartedAt:    now,
		CompletedAt:  nil,
		DurationMs:   0,
		Error:        "",
		Effort:       hb.Effort,
		Agent:        hb.Agent,
	}
	if createErr := store.CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for heartbeat %s: %v", hb.ID, createErr)
	}
}

// calculateOutcomeTiming resolves the completed/started timestamps and the
// duration for a dispatch outcome event, falling back to the run start and
// wall-clock time when the event omits them.
func calculateOutcomeTiming(ev pubsub.DispatchOutcomeEvent, runStartedAt time.Time) (time.Time, int64, time.Time) {
	completedAt := ev.CompletedAt
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	startedAt := ev.StartedAt
	if startedAt.IsZero() {
		startedAt = runStartedAt
	}
	durationMs := ev.DurationMs
	if durationMs <= 0 && !startedAt.IsZero() {
		durationMs = completedAt.Sub(startedAt).Milliseconds()
	}
	return completedAt, durationMs, startedAt
}

// advanceHeartbeatWatermark monotonically advances a heartbeat's
// last_completed_at watermark for completed runs only.
func advanceHeartbeatWatermark(ctx context.Context, database *sql.DB, run *model.ScheduleRun, status string, startedAt time.Time) {
	if status != model.StatusCompleted || run.ScheduleType != model.ScheduleTypeHeartbeat || startedAt.IsZero() {
		return
	}
	if err := store.AdvanceHeartbeatLastCompletedAt(ctx, database, run.ScheduleID, startedAt); err != nil {
		log.Printf("[Haro] failed to advance last_completed_at for heartbeat %s: %v", run.ScheduleID, err)
	}
}

// HandleDispatchOutcome updates schedule_runs and monotonically advances heartbeat last_completed_at.
func HandleDispatchOutcome(ctx context.Context, database *sql.DB, msg pubsub.Message) error {
	if database == nil || msg == nil {
		return errors.New("database and message cannot be nil")
	}
	var ev pubsub.DispatchOutcomeEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[Haro] warning: failed to unmarshal dispatch outcome event: %v", err)
		return nil
	}
	dispatchID := strings.TrimSpace(ev.DispatchID)
	if dispatchID == "" || dispatchID == "healthcheck-ping" {
		return nil
	}
	if strings.TrimSpace(ev.Source) != "" && ev.Source != model.ScheduleTypeHeartbeat {
		return nil
	}

	run, err := store.GetScheduleRun(ctx, database, dispatchID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "not found") {
			if msg.NumDelivered() < 5 {
				return msg.NakWithDelay(1 * time.Second)
			}
			return nil
		}
		log.Printf("[Haro] error querying schedule run for %s: %v", dispatchID, err)
		return err
	}

	switch strings.ToLower(strings.TrimSpace(run.Status)) {
	case model.StatusCompleted, model.StatusFailed, model.StatusStale, model.StatusSkippedActive:
		return nil
	}

	status := strings.ToLower(strings.TrimSpace(ev.Status))
	completedAt, durationMs, startedAt := calculateOutcomeTiming(ev, run.StartedAt)

	if err := store.UpdateScheduleRunOutcome(ctx, database, dispatchID, status, ev.Error, completedAt, durationMs); err != nil {
		log.Printf("[Haro] failed to update schedule run outcome for %s: %v", dispatchID, err)
		return err
	}

	metrics.RecordScheduleRun(run.ScheduleType, status)
	advanceHeartbeatWatermark(ctx, database, run, status, startedAt)

	return nil
}

// deterministicScheduleRunID computes a deterministic UUIDv5 for a schedule run
// derived from scheduleType, scheduleID, and the scheduled run timestamp (UTC RFC3339Nano).
func deterministicScheduleRunID(scheduleType, scheduleID string, dueAt time.Time) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(scheduleType+":"+strings.TrimSpace(scheduleID)+":"+dueAt.UTC().Format(time.RFC3339Nano))).String()
}

// Start evaluates due schedules immediately, then on every tick of the
// configured interval until ctx is cancelled. It must be run bound to a
// child context so main.go can await it before closing the database.
func (t *Ticker) Start(ctx context.Context) {
	if t == nil {
		return
	}
	t.EvaluateDueSchedules(ctx, time.Now().UTC())

	interval := t.interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t.EvaluateDueSchedules(ctx, now.UTC())
		}
	}
}
