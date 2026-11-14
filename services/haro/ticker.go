package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type PromptDispatchRequest struct {
	Prompt       string `json:"prompt"`
	ChannelID    string `json:"channel_id,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	Title        string `json:"title,omitempty"`
	MessageID    string `json:"message_id,omitempty"`
	Effort       string `json:"effort,omitempty"`
	Silent       bool   `json:"silent,omitempty"`
	Ephemeral    *bool  `json:"ephemeral,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

type PromptDispatchResponse struct {
	Status    string `json:"status"`
	ChannelID string `json:"channel_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
}

type DispatchRequest struct {
	ID           string `json:"id,omitempty"`
	Source       string `json:"source"`
	SessionID    string `json:"session_id,omitempty"`
	TargetID     string `json:"target_id,omitempty"`
	Prompt       string `json:"prompt"`
	Effort       string `json:"effort,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

type DispatchResponse struct {
	Status     string `json:"status"`
	DispatchID string `json:"dispatch_id,omitempty"`
	MessageID  string `json:"message_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
}

type PromptDispatcher interface {
	DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error)
	Dispatch(ctx context.Context, req DispatchRequest) (DispatchResponse, error)
}

type HTTPPromptDispatcher struct {
	brainURL   string
	httpClient *http.Client
}

func NewHTTPPromptDispatcher(brainURL string) *HTTPPromptDispatcher {
	trimmed := strings.TrimRight(strings.TrimSpace(brainURL), "/")
	if trimmed == "" {
		panic("ticker: brainURL cannot be empty")
	}
	return &HTTPPromptDispatcher{
		brainURL: trimmed,
		// Traced so each eri turn joins the trace of the schedule or trigger that fired it.
		httpClient: tracing.Client(&http.Client{
			Timeout: 15 * time.Second,
		}),
	}
}

// traceFire runs one schedule, heartbeat or event-trigger dispatch under its own span, so the
// eri turn it starts joins a trace that begins where the work began. Inside an inbound request
// (event triggers) the span continues the caller's trace. Only the schedule type and id are
// recorded; dispatch errors set a generic status since their text can echo eri responses.
func traceFire[R any](ctx context.Context, scheduleType, scheduleID string, dispatch func(context.Context) (R, error)) (R, error) {
	ctx, span := tracing.Tracer().Start(ctx, "haro.fire "+scheduleType,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("haro.schedule.type", scheduleType),
			attribute.String("haro.schedule.id", scheduleID),
		),
	)
	defer span.End()
	resp, err := dispatch(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "dispatch failed")
	}
	return resp, err
}

func postBrainJSON[Req any, Resp any](ctx context.Context, d *HTTPPromptDispatcher, path, opLabel string, req Req) (Resp, error) {
	var zero Resp
	if d == nil {
		return zero, fmt.Errorf("dispatcher is nil")
	}
	brainURL := strings.TrimRight(strings.TrimSpace(d.brainURL), "/")
	if brainURL == "" {
		return zero, fmt.Errorf("brain URL is not configured")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("failed to marshal %s request: %w", opLabel, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+path, bytes.NewReader(body))
	if err != nil {
		return zero, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := d.httpClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return zero, fmt.Errorf("%s failed: %w", opLabel, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp struct {
			Error string `json:"error"`
		}
		if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil && strings.TrimSpace(errResp.Error) != "" {
			return zero, fmt.Errorf("brain %s error (%d): %s", path, resp.StatusCode, errResp.Error)
		}
		return zero, fmt.Errorf("brain %s returned HTTP %d: %s", path, resp.StatusCode, string(respBody))
	}

	var dispatchResp Resp
	if err := json.Unmarshal(respBody, &dispatchResp); err != nil {
		return zero, fmt.Errorf("failed to parse %s response: %w", opLabel, err)
	}

	return dispatchResp, nil
}

func (d *HTTPPromptDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	return postBrainJSON[PromptDispatchRequest, PromptDispatchResponse](ctx, d, "/prompt", "prompt dispatch", req)
}

func (d *HTTPPromptDispatcher) Dispatch(ctx context.Context, req DispatchRequest) (DispatchResponse, error) {
	return postBrainJSON[DispatchRequest, DispatchResponse](ctx, d, "/dispatch", "dispatch", req)
}

type Ticker struct {
	db         *sql.DB
	dispatcher PromptDispatcher
	interval   time.Duration
	mu         sync.Mutex
	lastPrune  time.Time
}

func NewTicker(db *sql.DB, dispatcher PromptDispatcher, interval time.Duration) *Ticker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Ticker{
		db:         db,
		dispatcher: dispatcher,
		interval:   interval,
	}
}

func (t *Ticker) EvaluateDueSchedules(ctx context.Context, now time.Time) {
	if t == nil || t.db == nil || t.dispatcher == nil {
		return
	}

	t.evaluateDueCronSchedules(ctx, now)
	t.evaluateDueOneShotSchedules(ctx, now)
	t.evaluateDueHeartbeatSchedules(ctx, now)

	// Daily Retention Pruning
	t.mu.Lock()
	shouldPrune := ShouldPruneRuns(now, t.lastPrune, 24*time.Hour)
	if shouldPrune {
		t.lastPrune = now
	}
	t.mu.Unlock()

	if shouldPrune {
		if _, pruneErr := PruneScheduleRuns(ctx, t.db, 1000, 30*24*time.Hour, now); pruneErr != nil {
			log.Printf("[Haro Ticker] Error pruning schedule runs: %v", pruneErr)
		}
	}
}

func (t *Ticker) evaluateDueCronSchedules(ctx context.Context, now time.Time) {
	dueCrons, err := GetDueCronSchedules(ctx, t.db, now)
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

func (t *Ticker) processDueCron(ctx context.Context, c CronSchedule, now time.Time) {
	nextRun, calcErr := CalculateNextRun(c.CronExpr, c.Timezone, now)
	if calcErr != nil {
		log.Printf("[Haro Ticker] Failed to calculate next run for cron %s (%q): %v", c.ID, c.CronExpr, calcErr)
		nextRun = now.Add(24 * time.Hour)
	}

	if IsScheduleStale(c.NextRunAt, now, 24*time.Hour) {
		log.Printf("[Haro Ticker] Cron %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
			c.ID, c.CronExpr, c.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
		if advErr := AdvanceCronNextRun(ctx, t.db, c.ID, nextRun); advErr != nil {
			log.Printf("[Haro Ticker] Error advancing cron %s next_run_at: %v", c.ID, advErr)
		}
		runID := uuid.NewString()
		completedAt := now
		staleRun := ScheduleRun{
			ID:           runID,
			ScheduleID:   c.ID,
			ScheduleType: "cron",
			MessageID:    runID,
			TargetID:     c.TargetID,
			ThreadID:     c.TargetID,
			Title:        c.TitlePrefix,
			Prompt:       c.Prompt,
			Status:       "failed",
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   0,
			Error:        "stale schedule (>24h overdue)",
			Effort:       c.Effort,
		}
		if createErr := CreateScheduleRun(ctx, t.db, staleRun); createErr != nil {
			log.Printf("[Haro Ticker] Error creating stale schedule run for cron %s: %v", c.ID, createErr)
		}
		RecordScheduleRun("cron", "stale")
		return
	}

	if advErr := AdvanceCronNextRun(ctx, t.db, c.ID, nextRun); advErr != nil {
		log.Printf("[Haro Ticker] Error advancing cron %s next_run_at: %v", c.ID, advErr)
		return
	}

	runID := uuid.NewString()
	req := PromptDispatchRequest{
		Prompt:    c.Prompt,
		ChannelID: c.TargetID,
		Title:     c.TitlePrefix,
		MessageID: runID,
		Effort:    c.Effort,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := traceFire(ctx, "cron", c.ID, func(ctx context.Context) (PromptDispatchResponse, error) {
		return t.dispatcher.DispatchPrompt(ctx, req)
	})
	RecordScheduleDispatchDuration("cron", time.Since(dispatchStart).Seconds())
	completedAt := time.Now().UTC()
	durationMs := completedAt.Sub(now).Milliseconds()
	status := "completed"
	errMsg := ""
	threadID := resp.ChannelID
	if threadID == "" {
		threadID = c.TargetID
	}
	if dispatchErr != nil {
		status = "failed"
		errMsg = dispatchErr.Error()
		log.Printf("[Haro Ticker] Dispatch error for cron %s: %v", c.ID, dispatchErr)
	}
	RecordScheduleRun("cron", status)
	msgID := resp.MessageID
	if msgID == "" {
		msgID = runID
	}
	run := ScheduleRun{
		ID:           runID,
		ScheduleID:   c.ID,
		ScheduleType: "cron",
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
	}
	if createErr := CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for cron %s: %v", c.ID, createErr)
	}
}

func (t *Ticker) evaluateDueOneShotSchedules(ctx context.Context, now time.Time) {
	dueOneShots, err := GetDueOneShotSchedules(ctx, t.db, now)
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

func (t *Ticker) processDueOneShot(ctx context.Context, s OneShotSchedule, now time.Time) {
	deleted, delErr := DeleteOneShotSchedule(ctx, t.db, s.ID)
	if delErr != nil || !deleted {
		if delErr != nil {
			log.Printf("[Haro Ticker] Error deleting one-shot schedule %s: %v", s.ID, delErr)
		}
		return
	}

	runID := uuid.NewString()
	if IsScheduleStale(s.RunAt, now, 24*time.Hour) {
		log.Printf("[Haro Ticker] One-shot %s is stale (>24h overdue: due %s, now %s). Skipping dispatch.",
			s.ID, s.RunAt.Format(time.RFC3339), now.Format(time.RFC3339))
		completedAt := now
		staleRun := ScheduleRun{
			ID:           runID,
			ScheduleID:   s.ID,
			ScheduleType: "one_shot",
			MessageID:    runID,
			TargetID:     s.ThreadID,
			ThreadID:     s.ThreadID,
			Prompt:       s.Prompt,
			Status:       "failed",
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   0,
			Error:        "stale schedule (>24h overdue)",
			Effort:       s.Effort,
		}
		if createErr := CreateScheduleRun(ctx, t.db, staleRun); createErr != nil {
			log.Printf("[Haro Ticker] Error creating stale schedule run for one-shot %s: %v", s.ID, createErr)
		}
		RecordScheduleRun("one_shot", "stale")
		return
	}

	req := PromptDispatchRequest{
		Prompt:    s.Prompt,
		ChannelID: s.ThreadID,
		MessageID: runID,
		Effort:    s.Effort,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := traceFire(ctx, "one_shot", s.ID, func(ctx context.Context) (PromptDispatchResponse, error) {
		return t.dispatcher.DispatchPrompt(ctx, req)
	})
	RecordScheduleDispatchDuration("one_shot", time.Since(dispatchStart).Seconds())
	completedAt := time.Now().UTC()
	durationMs := completedAt.Sub(now).Milliseconds()
	status := "completed"
	errMsg := ""
	threadID := resp.ChannelID
	if threadID == "" {
		threadID = s.ThreadID
	}
	if dispatchErr != nil {
		status = "failed"
		errMsg = dispatchErr.Error()
		log.Printf("[Haro Ticker] Dispatch error for one-shot %s: %v", s.ID, dispatchErr)
	}
	RecordScheduleRun("one_shot", status)
	msgID := resp.MessageID
	if msgID == "" {
		msgID = runID
	}
	run := ScheduleRun{
		ID:           runID,
		ScheduleID:   s.ID,
		ScheduleType: "one_shot",
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
	}
	if createErr := CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for one-shot %s: %v", s.ID, createErr)
	}
}

func (t *Ticker) evaluateDueHeartbeatSchedules(ctx context.Context, now time.Time) {
	dueHeartbeats, err := GetDueHeartbeatSchedules(ctx, t.db, now)
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

func (t *Ticker) processDueHeartbeat(ctx context.Context, hb HeartbeatSchedule, now time.Time) {
	nextRun, calcErr := CalculateNextRun(hb.CronExpr, hb.Timezone, now)
	if calcErr != nil {
		log.Printf("[Haro Ticker] Failed to calculate next run for heartbeat %s (%q): %v", hb.ID, hb.CronExpr, calcErr)
		nextRun = now.Add(24 * time.Hour)
	}

	if IsScheduleStale(hb.NextRunAt, now, 24*time.Hour) {
		log.Printf("[Haro Ticker] Heartbeat %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
			hb.ID, hb.CronExpr, hb.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
		if advErr := AdvanceHeartbeatNextRun(ctx, t.db, hb.ID, nextRun); advErr != nil {
			log.Printf("[Haro Ticker] Error advancing heartbeat %s next_run_at: %v", hb.ID, advErr)
		}
		runID := uuid.NewString()
		completedAt := now
		staleRun := ScheduleRun{
			ID:           runID,
			ScheduleID:   hb.ID,
			ScheduleType: "heartbeat",
			MessageID:    runID,
			TargetID:     hb.TargetID,
			ThreadID:     "heartbeat-" + hb.ID,
			Title:        hb.TitlePrefix,
			Prompt:       hb.Prompt,
			Status:       "failed",
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   0,
			Error:        "stale heartbeat (>24h overdue)",
			Effort:       hb.Effort,
		}
		if createErr := CreateScheduleRun(ctx, t.db, staleRun); createErr != nil {
			log.Printf("[Haro Ticker] Error creating stale schedule run for heartbeat %s: %v", hb.ID, createErr)
		}
		RecordScheduleRun("heartbeat", "stale")
		return
	}

	if advErr := AdvanceHeartbeatNextRun(ctx, t.db, hb.ID, nextRun); advErr != nil {
		log.Printf("[Haro Ticker] Error advancing heartbeat %s next_run_at: %v", hb.ID, advErr)
		return
	}

	sessionKey := "heartbeat-" + hb.ID
	runID := uuid.NewString()
	wrappedPrompt := BuildHeartbeatPrompt(hb, now)
	req := DispatchRequest{
		ID:           runID,
		Source:       "heartbeat",
		SessionID:    sessionKey,
		TargetID:     strings.TrimSpace(hb.TargetID),
		Prompt:       wrappedPrompt,
		Effort:       hb.Effort,
		SkipIfActive: true,
	}

	dispatchStart := time.Now()
	resp, dispatchErr := traceFire(ctx, "heartbeat", hb.ID, func(ctx context.Context) (DispatchResponse, error) {
		return t.dispatcher.Dispatch(ctx, req)
	})
	RecordScheduleDispatchDuration("heartbeat", time.Since(dispatchStart).Seconds())
	if dispatchErr == nil && resp.Status == "skipped_active" {
		RecordScheduleRun("heartbeat", "skipped_active")
		slog.Info("Skipping heartbeat run: thread has an active turn in progress", "id", hb.ID, "target_id", hb.TargetID, "thread_id", sessionKey)
		return
	}
	if dispatchErr != nil {
		completedAt := time.Now().UTC()
		durationMs := completedAt.Sub(now).Milliseconds()
		log.Printf("[Haro Ticker] Dispatch error for heartbeat %s: %v", hb.ID, dispatchErr)
		RecordScheduleRun("heartbeat", "failed")
		run := ScheduleRun{
			ID:           runID,
			ScheduleID:   hb.ID,
			ScheduleType: "heartbeat",
			MessageID:    runID,
			TargetID:     hb.TargetID,
			ThreadID:     sessionKey,
			Title:        hb.TitlePrefix,
			Prompt:       hb.Prompt,
			Status:       "failed",
			StartedAt:    now,
			CompletedAt:  &completedAt,
			DurationMs:   durationMs,
			Error:        dispatchErr.Error(),
			Effort:       hb.Effort,
		}
		if createErr := CreateScheduleRun(ctx, t.db, run); createErr != nil {
			log.Printf("[Haro Ticker] Error creating schedule run for heartbeat %s: %v", hb.ID, createErr)
		}
		return
	}

	msgID := firstNonEmpty(resp.DispatchID, resp.MessageID, runID)
	run := ScheduleRun{
		ID:           runID,
		ScheduleID:   hb.ID,
		ScheduleType: "heartbeat",
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
	}
	if createErr := CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for heartbeat %s: %v", hb.ID, createErr)
	}
}

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

func advanceHeartbeatWatermark(ctx context.Context, database *sql.DB, run *ScheduleRun, status string, startedAt time.Time) {
	if status != "completed" || run.ScheduleType != "heartbeat" || startedAt.IsZero() {
		return
	}
	if err := AdvanceHeartbeatLastCompletedAt(ctx, database, run.ScheduleID, startedAt); err != nil {
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
	if strings.TrimSpace(ev.Source) != "" && ev.Source != "heartbeat" {
		return nil
	}

	run, err := GetScheduleRun(ctx, database, dispatchID)
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

	status := strings.ToLower(strings.TrimSpace(ev.Status))
	completedAt, durationMs, startedAt := calculateOutcomeTiming(ev, run.StartedAt)

	if err := UpdateScheduleRunOutcome(ctx, database, dispatchID, status, ev.Error, completedAt, durationMs); err != nil {
		log.Printf("[Haro] failed to update schedule run outcome for %s: %v", dispatchID, err)
		return err
	}

	RecordScheduleRun(run.ScheduleType, status)
	advanceHeartbeatWatermark(ctx, database, run, status, startedAt)

	return nil
}

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
