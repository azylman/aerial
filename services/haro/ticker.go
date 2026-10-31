package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	return &HTTPPromptDispatcher{
		brainURL: strings.TrimRight(strings.TrimSpace(brainURL), "/"),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (d *HTTPPromptDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	if d == nil {
		return PromptDispatchResponse{}, fmt.Errorf("dispatcher is nil")
	}
	brainURL := strings.TrimRight(strings.TrimSpace(d.brainURL), "/")
	if brainURL == "" {
		brainURL = "http://eri.aerial"
	}

	body, err := json.Marshal(req)
	if err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("failed to marshal prompt dispatch request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+"/prompt", bytes.NewReader(body))
	if err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := d.httpClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("prompt dispatch failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp struct {
			Error string `json:"error"`
		}
		if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil && strings.TrimSpace(errResp.Error) != "" {
			return PromptDispatchResponse{}, fmt.Errorf("brain /prompt error (%d): %s", resp.StatusCode, errResp.Error)
		}
		return PromptDispatchResponse{}, fmt.Errorf("brain /prompt returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var dispatchResp PromptDispatchResponse
	if err := json.Unmarshal(respBody, &dispatchResp); err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("failed to parse prompt dispatch response: %w", err)
	}

	return dispatchResp, nil
}

func (d *HTTPPromptDispatcher) Dispatch(ctx context.Context, req DispatchRequest) (DispatchResponse, error) {
	if d == nil {
		return DispatchResponse{}, fmt.Errorf("dispatcher is nil")
	}
	brainURL := strings.TrimRight(strings.TrimSpace(d.brainURL), "/")
	if brainURL == "" {
		brainURL = "http://eri.aerial"
	}

	body, err := json.Marshal(req)
	if err != nil {
		return DispatchResponse{}, fmt.Errorf("failed to marshal dispatch request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+"/dispatch", bytes.NewReader(body))
	if err != nil {
		return DispatchResponse{}, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := d.httpClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return DispatchResponse{}, fmt.Errorf("dispatch failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return DispatchResponse{}, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp struct {
			Error string `json:"error"`
		}
		if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil && strings.TrimSpace(errResp.Error) != "" {
			return DispatchResponse{}, fmt.Errorf("brain /dispatch error (%d): %s", resp.StatusCode, errResp.Error)
		}
		return DispatchResponse{}, fmt.Errorf("brain /dispatch returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var dispatchResp DispatchResponse
	if err := json.Unmarshal(respBody, &dispatchResp); err != nil {
		return DispatchResponse{}, fmt.Errorf("failed to parse dispatch response: %w", err)
	}

	return dispatchResp, nil
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
	resp, dispatchErr := t.dispatcher.DispatchPrompt(ctx, req)
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
	resp, dispatchErr := t.dispatcher.DispatchPrompt(ctx, req)
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
	wrappedPrompt := BuildHeartbeatPrompt(hb)
	req := DispatchRequest{
		ID:           runID,
		Source:       "heartbeat",
		SessionID:    sessionKey,
		Prompt:       wrappedPrompt,
		Effort:       hb.Effort,
		SkipIfActive: true,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := t.dispatcher.Dispatch(ctx, req)
	RecordScheduleDispatchDuration("heartbeat", time.Since(dispatchStart).Seconds())
	if dispatchErr == nil && resp.Status == "skipped_active" {
		RecordScheduleRun("heartbeat", "skipped_active")
		slog.Info("Skipping heartbeat run: thread has an active turn in progress", "id", hb.ID, "target_id", hb.TargetID, "thread_id", sessionKey)
		return
	}
	completedAt := time.Now().UTC()
	durationMs := completedAt.Sub(now).Milliseconds()
	status := "completed"
	errMsg := ""
	if dispatchErr != nil {
		status = "failed"
		errMsg = dispatchErr.Error()
		log.Printf("[Haro Ticker] Dispatch error for heartbeat %s: %v", hb.ID, dispatchErr)
	}
	RecordScheduleRun("heartbeat", status)
	msgID := firstNonEmpty(resp.DispatchID, resp.MessageID, runID)
	run := ScheduleRun{
		ID:           runID,
		ScheduleID:   hb.ID,
		ScheduleType: "heartbeat",
		MessageID:    msgID,
		TargetID:     hb.TargetID,
		ThreadID:     "heartbeat-" + hb.ID,
		Title:        hb.TitlePrefix,
		Prompt:       hb.Prompt,
		Status:       status,
		StartedAt:    now,
		CompletedAt:  &completedAt,
		DurationMs:   durationMs,
		Error:        errMsg,
		Effort:       hb.Effort,
	}
	if createErr := CreateScheduleRun(ctx, t.db, run); createErr != nil {
		log.Printf("[Haro Ticker] Error creating schedule run for heartbeat %s: %v", hb.ID, createErr)
	}
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
