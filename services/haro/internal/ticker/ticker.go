// Package ticker implements Haro's scheduling loop (Layer 3) and Restate Virtual Objects.
//
// Ticker owns schedule execution and event-watch dispatch: periodic evaluation,
// Restate HaroSchedule and HaroEventWatch Virtual Objects, outcome timing
// for dispatch results, monotonic heartbeat watermark advancement, and the
// dispatch-outcome consumer handler.
package ticker

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
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/metrics"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

// ArmRequest is the input payload for HaroSchedule.Arm.
type ArmRequest struct {
	ScheduleType string `json:"schedule_type,omitempty"`
}

// FireRequest is the input payload for HaroSchedule.Fire.
type FireRequest struct {
	Generation   int64     `json:"generation"`
	ScheduleType string    `json:"schedule_type,omitempty"`
	DueAt        time.Time `json:"due_at"`
}

// ScheduleRow represents a loaded schedule record across cron, one-shot, or heartbeat types.
type ScheduleRow struct {
	Found        bool      `json:"found"`
	Enabled      bool      `json:"enabled"`
	ScheduleType string    `json:"schedule_type"`
	ID           string    `json:"id"`
	TargetID     string    `json:"target_id"`
	Prompt       string    `json:"prompt"`
	TitlePrefix  string    `json:"title_prefix"`
	CronExpr     string    `json:"cron_expr"`
	Timezone     string    `json:"timezone"`
	NextRunAt    time.Time `json:"next_run_at"`
	Effort       string    `json:"effort"`
	Agent        string    `json:"agent"`
	DelayMs      int64     `json:"delay_ms"`
	// LastCompletedAt is the heartbeat watermark (start of the latest completed run); nil before the first completed run.
	LastCompletedAt *time.Time `json:"last_completed_at,omitempty"`
}

// DispatchStepResult carries the outcome of dispatch-turn.
type DispatchStepResult struct {
	SkippedActive bool      `json:"skipped_active"`
	IsStale       bool      `json:"is_stale"`
	Status        string    `json:"status"`
	Error         string    `json:"error"`
	MessageID     string    `json:"message_id"`
	ThreadID      string    `json:"thread_id"`
	DispatchedAt  time.Time `json:"dispatched_at"`
	DurationMs    int64     `json:"duration_ms"`
}

// FinalizeResult carries the outcome of finalize-schedule.
type FinalizeResult struct {
	IsOneShot bool      `json:"is_one_shot"`
	NextRunAt time.Time `json:"next_run_at"`
	DelayMs   int64     `json:"delay_ms"`
}

// HaroSchedule is a Restate Virtual Object keyed by schedule ID managing durable schedule firing.
type HaroSchedule struct {
	db         *sql.DB
	dispatcher dispatch.PromptDispatcher
	nowFn      func() time.Time
}

// NewHaroSchedule constructs a HaroSchedule Virtual Object handler.
func NewHaroSchedule(db *sql.DB, dispatcher dispatch.PromptDispatcher) *HaroSchedule {
	return &HaroSchedule{
		db:         db,
		dispatcher: dispatcher,
	}
}

func (s *HaroSchedule) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now().UTC()
}

const (
	defaultRestatePort      = "9080"
	errStaleScheduleOverdue = "stale schedule (>24h overdue)"
)

func mapCronToRow(c *model.CronSchedule) ScheduleRow {
	return ScheduleRow{
		Found:        true,
		Enabled:      c.Enabled,
		ScheduleType: model.ScheduleTypeCron,
		ID:           c.ID,
		TargetID:     c.TargetID,
		Prompt:       c.Prompt,
		TitlePrefix:  c.TitlePrefix,
		CronExpr:     c.CronExpr,
		Timezone:     c.Timezone,
		NextRunAt:    c.NextRunAt,
		Effort:       c.Effort,
		Agent:        c.Agent,
	}
}

func mapHeartbeatToRow(hb *model.HeartbeatSchedule) ScheduleRow {
	return ScheduleRow{
		Found:           true,
		Enabled:         hb.Enabled,
		ScheduleType:    model.ScheduleTypeHeartbeat,
		ID:              hb.ID,
		TargetID:        hb.TargetID,
		Prompt:          hb.Prompt,
		TitlePrefix:     hb.TitlePrefix,
		CronExpr:        hb.CronExpr,
		Timezone:        hb.Timezone,
		NextRunAt:       hb.NextRunAt,
		Effort:          hb.Effort,
		Agent:           hb.Agent,
		LastCompletedAt: hb.LastCompletedAt,
	}
}

func (s *HaroSchedule) loadScheduleRow(ctx context.Context, id, schedType string) (ScheduleRow, error) {
	id = strings.TrimSpace(id)
	schedType = strings.TrimSpace(schedType)

	if s.db == nil {
		return ScheduleRow{Found: false}, nil
	}

	now := s.now()

	if schedType == model.ScheduleTypeCron || schedType == "" {
		c, err := store.GetCronSchedule(ctx, s.db, id)
		if err == nil {
			row := mapCronToRow(c)
			if row.NextRunAt.After(now) {
				row.DelayMs = row.NextRunAt.Sub(now).Milliseconds()
			}
			return row, nil
		}
		if !errors.Is(err, sql.ErrNoRows) && !strings.Contains(err.Error(), "not found") {
			return ScheduleRow{}, err
		}
	}

	if schedType == model.ScheduleTypeOneShot || schedType == "" {
		if o, err := store.GetOneShotSchedule(ctx, s.db, id); err == nil {
			row := ScheduleRow{
				Found:        true,
				Enabled:      true,
				ScheduleType: model.ScheduleTypeOneShot,
				ID:           o.ID,
				TargetID:     o.ThreadID,
				Prompt:       o.Prompt,
				NextRunAt:    o.RunAt,
				Effort:       o.Effort,
				Agent:        o.Agent,
			}
			if row.NextRunAt.After(now) {
				row.DelayMs = row.NextRunAt.Sub(now).Milliseconds()
			}
			return row, nil
		} else if !errors.Is(err, sql.ErrNoRows) && !strings.Contains(err.Error(), "not found") {
			return ScheduleRow{}, err
		}
	}

	if schedType == model.ScheduleTypeHeartbeat || schedType == "" {
		hb, err := store.GetHeartbeatSchedule(ctx, s.db, id)
		if err == nil {
			row := mapHeartbeatToRow(hb)
			if row.NextRunAt.After(now) {
				row.DelayMs = row.NextRunAt.Sub(now).Milliseconds()
			}
			return row, nil
		}
		if !errors.Is(err, sql.ErrNoRows) && !strings.Contains(err.Error(), "not found") {
			return ScheduleRow{}, err
		}
	}

	return ScheduleRow{Found: false}, nil
}

// fireIdempotencyKey names the logical "fire this schedule at this due tick"
// operation. Re-arming the same tick (backfill, tool update, Fire's own
// follow-up send) collapses into one pending Fire via Restate idempotency;
// the generation is part of the key so a Cancel followed by a fresh Arm of
// the same tick schedules a new Fire.
func fireIdempotencyKey(scheduleID string, gen int64, dueAt time.Time) string {
	return fmt.Sprintf("haro:fire:%s:%d:%s", scheduleID, gen, dueAt.UTC().Format(time.RFC3339Nano))
}

// scheduleGoneError is the terminal result for a Fire or Arm whose schedule
// was deleted (404) or disabled (412); state is fenced before it is returned.
func scheduleGoneError(scheduleID string, loaded ScheduleRow) error {
	if !loaded.Found {
		return restate.ToTerminalError(fmt.Errorf("schedule %q not found", scheduleID), restate.WithErrorCode(http.StatusNotFound))
	}
	return restate.ToTerminalError(fmt.Errorf("schedule %q is disabled", scheduleID), restate.WithErrorCode(http.StatusPreconditionFailed))
}

// fenceSchedule bumps the generation so every pending Fire becomes a no-op.
func fenceSchedule(ctx restate.ObjectContext, gen int64) {
	restate.Set(ctx, "gen", gen+1)
	restate.Clear(ctx, "armed_due_at")
}

// sendFire schedules Fire for dueAt, deduplicated per schedule, generation and due tick.
func sendFire(ctx restate.ObjectContext, scheduleID string, gen int64, schedType string, dueAt time.Time, delay time.Duration) {
	restate.Set(ctx, "armed_due_at", dueAt.UTC().Format(time.RFC3339Nano))
	_ = restate.ObjectSend(ctx, "HaroSchedule", scheduleID, "Fire").Send(
		FireRequest{
			Generation:   gen,
			ScheduleType: schedType,
			DueAt:        dueAt,
		},
		restate.WithDelay(max(0, delay)),
		restate.WithIdempotencyKey(fireIdempotencyKey(scheduleID, gen, dueAt)),
	)
}

// Arm loads the schedule and sends Fire for its next due tick. Arm is safe to
// repeat: the Fire send is keyed by schedule, generation and due tick, so a
// pending Fire for the same tick is deduplicated by Restate while a lost one
// is re-armed. A deleted or disabled schedule fences pending Fires and ends
// terminally.
func (s *HaroSchedule) Arm(ctx restate.ObjectContext, req ArmRequest) (restate.Void, error) {
	scheduleID := restate.Key(ctx)
	gen, err := restate.Get[int64](ctx, "gen")
	if err != nil {
		return restate.Void{}, err
	}

	loaded, err := restate.Run(ctx, func(rc restate.RunContext) (ScheduleRow, error) {
		return withStoreTimeout(rc, func(c context.Context) (ScheduleRow, error) {
			return s.loadScheduleRow(c, scheduleID, req.ScheduleType)
		})
	}, restate.WithName("load-schedule"))
	if err != nil {
		return restate.Void{}, err
	}
	if !loaded.Found || !loaded.Enabled {
		fenceSchedule(ctx, gen)
		return restate.Void{}, scheduleGoneError(scheduleID, loaded)
	}

	schedType := strutil.FirstNonEmpty(loaded.ScheduleType, req.ScheduleType)
	sendFire(ctx, scheduleID, gen, schedType, loaded.NextRunAt, time.Duration(loaded.DelayMs)*time.Millisecond)
	return restate.Void{}, nil
}

// Fire evaluates generation fencing, loads schedule, dispatches turn to eri, and finalizes schedule.
func (s *HaroSchedule) Fire(ctx restate.ObjectContext, req FireRequest) (restate.Void, error) {
	scheduleID := restate.Key(ctx)
	currentGen, err := restate.Get[int64](ctx, "gen")
	if err != nil {
		return restate.Void{}, err
	}
	if req.Generation != currentGen {
		return restate.Void{}, nil
	}
	// A Fire for a tick that has since been superseded (re-armed for another
	// due time, or already fired and advanced) is a no-op.
	armedDue, err := restate.Get[string](ctx, "armed_due_at")
	if err != nil {
		return restate.Void{}, err
	}
	if !req.DueAt.IsZero() && armedDue != "" && armedDue != req.DueAt.UTC().Format(time.RFC3339Nano) {
		return restate.Void{}, nil
	}

	// Step 1: load-schedule
	loaded, err := restate.Run(ctx, func(rc restate.RunContext) (ScheduleRow, error) {
		return withStoreTimeout(rc, func(c context.Context) (ScheduleRow, error) {
			return s.loadScheduleRow(c, scheduleID, req.ScheduleType)
		})
	}, restate.WithName("load-schedule"))
	if err != nil {
		return restate.Void{}, err
	}
	if !loaded.Found || !loaded.Enabled {
		fenceSchedule(ctx, currentGen)
		return restate.Void{}, scheduleGoneError(scheduleID, loaded)
	}

	schedType := strutil.FirstNonEmpty(loaded.ScheduleType, req.ScheduleType)
	dueAt := req.DueAt
	if dueAt.IsZero() {
		dueAt = loaded.NextRunAt
	}
	idempotencyKey := fmt.Sprintf("%s:%s:%s", schedType, scheduleID, dueAt.UTC().Format(time.RFC3339Nano))
	runID := deterministicScheduleRunID(schedType, scheduleID, dueAt)

	// Step 2: dispatch-turn
	dispatchRes, err := restate.Run(ctx, func(rc restate.RunContext) (DispatchStepResult, error) {
		now := s.now()
		if model.IsScheduleStale(dueAt, now, 24*time.Hour) {
			log.Printf("[Haro Schedule] Schedule %s (%s) is stale (>24h overdue: due %s, now %s). Skipping dispatch.",
				scheduleID, schedType, dueAt.Format(time.RFC3339), now.Format(time.RFC3339))
			metrics.RecordScheduleRun(schedType, model.StatusStale)
			return DispatchStepResult{
				IsStale:      true,
				Status:       model.StatusFailed,
				Error:        errStaleScheduleOverdue,
				DispatchedAt: now,
			}, nil
		}

		if schedType == model.ScheduleTypeHeartbeat {
			return s.dispatchHeartbeatStep(rc, loaded, scheduleID, idempotencyKey, runID, now)
		}
		return s.dispatchPromptStep(rc, loaded, schedType, scheduleID, idempotencyKey, runID, now)
	}, restate.WithName("dispatch-turn"))
	if err != nil {
		return restate.Void{}, err
	}

	// Step 3: record-run (the dispatch's completion record, separate from the dispatch itself)
	if _, err = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		return restate.Void{}, s.recordRunStep(rc, loaded, schedType, scheduleID, runID, dispatchRes, s.now())
	}, restate.WithName("record-run")); err != nil {
		return restate.Void{}, err
	}

	// Step 4: finalize-schedule (delete the one-shot or advance the recurring schedule)
	finRes, err := restate.Run(ctx, func(rc restate.RunContext) (FinalizeResult, error) {
		return s.finalizeStep(rc, loaded, schedType, scheduleID, s.now())
	}, restate.WithName("finalize-schedule"))
	if err != nil {
		return restate.Void{}, err
	}

	if finRes.IsOneShot {
		fenceSchedule(ctx, currentGen)
		return restate.Void{}, nil
	}

	sendFire(ctx, scheduleID, currentGen, schedType, finRes.NextRunAt, time.Duration(finRes.DelayMs)*time.Millisecond)
	return restate.Void{}, nil
}

// permanentDispatchFailure turns a terminal dispatcher error (eri or ingress
// 4xx, invalid request) into a failed run so the schedule records it and moves
// on to its next tick instead of retrying a dispatch that cannot succeed.
func permanentDispatchFailure(schedType string, err error, now time.Time) (DispatchStepResult, error) {
	if !restate.IsTerminalError(err) {
		return DispatchStepResult{}, err
	}
	log.Printf("[Haro Schedule] permanent %s dispatch failure, recording failed run: %v", schedType, err)
	return DispatchStepResult{
		Status:       model.StatusFailed,
		Error:        err.Error(),
		DispatchedAt: now,
	}, nil
}

func (s *HaroSchedule) dispatchHeartbeatStep(rc restate.RunContext, loaded ScheduleRow, scheduleID, idempotencyKey, runID string, now time.Time) (DispatchStepResult, error) {
	sessionKey := "heartbeat-" + scheduleID
	active, activeErr := withStoreTimeout(rc, func(c context.Context) (bool, error) {
		return store.HasRunningHeartbeatRun(c, s.db, scheduleID, now.Add(-2*time.Hour))
	})
	if activeErr == nil && active {
		metrics.RecordScheduleRun(model.ScheduleTypeHeartbeat, model.StatusSkippedActive)
		slog.Info("Skipping heartbeat run: schedule has an active run in progress", "id", scheduleID, "thread_id", sessionKey)
		return DispatchStepResult{
			SkippedActive: true,
			Status:        model.StatusSkippedActive,
			ThreadID:      sessionKey,
			DispatchedAt:  now,
		}, nil
	}

	if s.dispatcher == nil {
		return DispatchStepResult{}, errors.New("dispatcher not initialized")
	}

	hb := model.HeartbeatSchedule{
		ID:              scheduleID,
		TargetID:        loaded.TargetID,
		TitlePrefix:     loaded.TitlePrefix,
		CronExpr:        loaded.CronExpr,
		Prompt:          loaded.Prompt,
		Timezone:        loaded.Timezone,
		Effort:          loaded.Effort,
		Agent:           loaded.Agent,
		LastCompletedAt: loaded.LastCompletedAt,
	}
	wrappedPrompt := model.BuildHeartbeatPrompt(hb, now)
	dispReq := eriwire.BrainDispatchRequest{
		ID:           idempotencyKey,
		Source:       model.ScheduleTypeHeartbeat,
		SessionID:    "",
		TargetID:     strings.TrimSpace(loaded.TargetID),
		Prompt:       wrappedPrompt,
		Effort:       loaded.Effort,
		SkipIfActive: true,
		Agent:        loaded.Agent,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := dispatch.TraceFire(rc, model.ScheduleTypeHeartbeat, scheduleID, func(dispCtx context.Context) (eriwire.DispatchEnqueueResponse, error) {
		return s.dispatcher.Dispatch(dispCtx, dispReq)
	})
	metrics.RecordScheduleDispatchDuration(model.ScheduleTypeHeartbeat, time.Since(dispatchStart).Seconds())
	if dispatchErr != nil {
		return permanentDispatchFailure(model.ScheduleTypeHeartbeat, dispatchErr, now)
	}
	if resp.Status == eriwire.StatusSkippedActive {
		metrics.RecordScheduleRun(model.ScheduleTypeHeartbeat, model.StatusSkippedActive)
		return DispatchStepResult{
			SkippedActive: true,
			Status:        model.StatusSkippedActive,
			ThreadID:      sessionKey,
			DispatchedAt:  now,
		}, nil
	}
	msgID := strutil.FirstNonEmpty(resp.DispatchID, runID)
	return DispatchStepResult{
		Status:       "running",
		MessageID:    msgID,
		ThreadID:     sessionKey,
		DispatchedAt: now,
	}, nil
}

func (s *HaroSchedule) dispatchPromptStep(rc restate.RunContext, loaded ScheduleRow, schedType, scheduleID, idempotencyKey, runID string, now time.Time) (DispatchStepResult, error) {
	if s.dispatcher == nil {
		return DispatchStepResult{}, errors.New("dispatcher not initialized")
	}

	dispReq := dispatch.PromptDispatchRequest{
		Prompt:    loaded.Prompt,
		ChannelID: loaded.TargetID,
		Title:     loaded.TitlePrefix,
		MessageID: idempotencyKey,
		Effort:    loaded.Effort,
		Agent:     loaded.Agent,
	}
	dispatchStart := time.Now()
	resp, dispatchErr := dispatch.TraceFire(rc, schedType, scheduleID, func(dispCtx context.Context) (dispatch.PromptDispatchResponse, error) {
		return s.dispatcher.DispatchPrompt(dispCtx, dispReq)
	})
	metrics.RecordScheduleDispatchDuration(schedType, time.Since(dispatchStart).Seconds())
	if dispatchErr != nil {
		return permanentDispatchFailure(schedType, dispatchErr, now)
	}
	completedAt := time.Now().UTC()
	durationMs := completedAt.Sub(now).Milliseconds()
	threadID := resp.ChannelID
	if threadID == "" {
		threadID = loaded.TargetID
	}
	msgID := resp.MessageID
	if msgID == "" {
		msgID = runID
	}
	return DispatchStepResult{
		Status:       model.StatusCompleted,
		MessageID:    msgID,
		ThreadID:     threadID,
		DispatchedAt: now,
		DurationMs:   durationMs,
	}, nil
}

// recordRunStep upserts the run row for this due tick (idempotent on the deterministic run ID).
func (s *HaroSchedule) recordRunStep(rc restate.RunContext, loaded ScheduleRow, schedType, scheduleID, runID string, dispatchRes DispatchStepResult, now time.Time) error {
	if dispatchRes.SkippedActive {
		return nil
	}
	var completedAt *time.Time
	if dispatchRes.Status == model.StatusCompleted || dispatchRes.Status == model.StatusFailed {
		c := now
		completedAt = &c
	}
	run := model.ScheduleRun{
		ID:           runID,
		ScheduleID:   scheduleID,
		ScheduleType: schedType,
		MessageID:    strutil.FirstNonEmpty(dispatchRes.MessageID, runID),
		TargetID:     loaded.TargetID,
		ThreadID:     strutil.FirstNonEmpty(dispatchRes.ThreadID, loaded.TargetID),
		Title:        loaded.TitlePrefix,
		Prompt:       loaded.Prompt,
		Status:       dispatchRes.Status,
		StartedAt:    dispatchRes.DispatchedAt,
		CompletedAt:  completedAt,
		DurationMs:   dispatchRes.DurationMs,
		Error:        dispatchRes.Error,
		Effort:       loaded.Effort,
		Agent:        loaded.Agent,
	}
	if _, createErr := withStoreTimeout(rc, func(c context.Context) (struct{}, error) {
		return struct{}{}, store.CreateScheduleRun(c, s.db, run)
	}); createErr != nil {
		log.Printf("[Haro Schedule] Error creating schedule run for %s (%s): %v", scheduleID, schedType, createErr)
		return createErr
	}
	metrics.RecordScheduleRun(schedType, dispatchRes.Status)
	return nil
}

// finalizeStep deletes a fired one-shot or advances a recurring schedule to its next tick.
func (s *HaroSchedule) finalizeStep(rc restate.RunContext, loaded ScheduleRow, schedType, scheduleID string, now time.Time) (FinalizeResult, error) {
	if schedType == model.ScheduleTypeOneShot {
		if _, delErr := withStoreTimeout(rc, func(c context.Context) (bool, error) {
			return store.DeleteOneShotSchedule(c, s.db, scheduleID)
		}); delErr != nil {
			return FinalizeResult{}, delErr
		}
		return FinalizeResult{IsOneShot: true}, nil
	}

	if schedType == model.ScheduleTypeCron || schedType == model.ScheduleTypeHeartbeat {
		nextRun, advErr := s.advanceRecurringSchedule(rc, loaded, schedType, scheduleID, now)
		if advErr != nil {
			return FinalizeResult{}, advErr
		}
		delay := time.Duration(0)
		if nextRun.After(now) {
			delay = nextRun.Sub(now)
		}
		return FinalizeResult{NextRunAt: nextRun, DelayMs: delay.Milliseconds()}, nil
	}

	return FinalizeResult{}, nil
}

func (s *HaroSchedule) advanceRecurringSchedule(rc restate.RunContext, loaded ScheduleRow, schedType, scheduleID string, now time.Time) (time.Time, error) {
	nextRun, calcErr := model.CalculateNextRun(loaded.CronExpr, loaded.Timezone, now)
	if calcErr != nil {
		nextRun = now.Add(24 * time.Hour)
	}
	_, err := withStoreTimeout(rc, func(c context.Context) (struct{}, error) {
		if schedType == model.ScheduleTypeCron {
			return struct{}{}, store.AdvanceCronNextRun(c, s.db, scheduleID, nextRun)
		}
		return struct{}{}, store.AdvanceHeartbeatNextRun(c, s.db, scheduleID, nextRun)
	})
	return nextRun, err
}

// Cancel increments the generation counter to fence off pending Fires.
func (*HaroSchedule) Cancel(ctx restate.ObjectContext, _ struct{}) (restate.Void, error) {
	gen, err := restate.Get[int64](ctx, "gen")
	if err != nil {
		return restate.Void{}, err
	}
	restate.Set(ctx, "gen", gen+1)
	restate.Clear(ctx, "armed_due_at")
	return restate.Void{}, nil
}

// Ticker periodically evaluates due schedules and coordinates Restate Virtual Objects.
type Ticker struct {
	db                *sql.DB
	dispatcher        dispatch.PromptDispatcher
	interval          time.Duration
	mu                sync.Mutex
	lastPrune         time.Time
	restatePort       string
	ingressURL        string
	adminURL          string
	advertiseURL      string
	httpClient        *http.Client
	restateRegistered bool
	restateBackfilled bool
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
		httpClient: http.DefaultClient,
	}
}

// SetRestateConfig configures Restate server port and URLs for the Ticker.
func (t *Ticker) SetRestateConfig(port, ingressURL, adminURL, advertiseURL string) {
	if t == nil {
		return
	}
	t.restatePort = strings.TrimSpace(port)
	t.ingressURL = strings.TrimRight(strings.TrimSpace(ingressURL), "/")
	t.adminURL = strings.TrimRight(strings.TrimSpace(adminURL), "/")
	t.advertiseURL = strings.TrimRight(strings.TrimSpace(advertiseURL), "/")
}

// RegisterDeployment registers advertiseURL with the Restate admin deployments endpoint.
func RegisterDeployment(ctx context.Context, adminURL, advertiseURL string, client *http.Client) error {
	adminURL = strings.TrimRight(strings.TrimSpace(adminURL), "/")
	advertiseURL = strings.TrimSpace(advertiseURL)
	if adminURL == "" || advertiseURL == "" {
		return nil
	}
	body, err := json.Marshal(map[string]any{
		"uri":   advertiseURL,
		"force": true,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, adminURL+"/deployments", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, rErr := io.ReadAll(resp.Body)
		if rErr != nil {
			return fmt.Errorf("restate admin HTTP %d: %w", resp.StatusCode, rErr)
		}
		return fmt.Errorf("restate admin HTTP %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// BackfillActiveSchedules arms all active cron, one-shot, and heartbeat schedules via Restate ingress.
func (t *Ticker) BackfillActiveSchedules(ctx context.Context) error {
	if t == nil || t.db == nil || strings.TrimSpace(t.ingressURL) == "" {
		return nil
	}
	var errs []error
	crons, err := store.ListCronSchedules(ctx, t.db, "", "")
	if err != nil {
		return err
	}
	for _, c := range crons {
		if !c.Enabled {
			continue
		}
		key := fmt.Sprintf("haro:arm:%s:%s:%s", model.ScheduleTypeCron, c.ID, c.NextRunAt.UTC().Format(time.RFC3339Nano))
		endpoint := fmt.Sprintf("%s/HaroSchedule/%s/Arm/send", t.ingressURL, url.PathEscape(c.ID))
		if _, postErr := restateingress.Post(ctx, t.httpClient, endpoint, key, ArmRequest{ScheduleType: model.ScheduleTypeCron}, 0); postErr != nil {
			log.Printf("[Haro Ticker] warning: failed to backfill cron %s: %v", c.ID, postErr)
			errs = append(errs, postErr)
		}
	}

	oneshots, err := store.ListOneShotSchedules(ctx, t.db, "", "")
	if err != nil {
		return err
	}
	for _, o := range oneshots {
		key := fmt.Sprintf("haro:arm:%s:%s:%s", model.ScheduleTypeOneShot, o.ID, o.RunAt.UTC().Format(time.RFC3339Nano))
		endpoint := fmt.Sprintf("%s/HaroSchedule/%s/Arm/send", t.ingressURL, url.PathEscape(o.ID))
		if _, postErr := restateingress.Post(ctx, t.httpClient, endpoint, key, ArmRequest{ScheduleType: model.ScheduleTypeOneShot}, 0); postErr != nil {
			log.Printf("[Haro Ticker] warning: failed to backfill one-shot %s: %v", o.ID, postErr)
			errs = append(errs, postErr)
		}
	}

	heartbeats, err := store.ListHeartbeatSchedules(ctx, t.db, "", "")
	if err != nil {
		return err
	}
	for _, hb := range heartbeats {
		if !hb.Enabled {
			continue
		}
		key := fmt.Sprintf("haro:arm:%s:%s:%s", model.ScheduleTypeHeartbeat, hb.ID, hb.NextRunAt.UTC().Format(time.RFC3339Nano))
		endpoint := fmt.Sprintf("%s/HaroSchedule/%s/Arm/send", t.ingressURL, url.PathEscape(hb.ID))
		if _, postErr := restateingress.Post(ctx, t.httpClient, endpoint, key, ArmRequest{ScheduleType: model.ScheduleTypeHeartbeat}, 0); postErr != nil {
			log.Printf("[Haro Ticker] warning: failed to backfill heartbeat %s: %v", hb.ID, postErr)
			errs = append(errs, postErr)
		}
	}

	return errors.Join(errs...)
}

func (t *Ticker) isRestateEnabled() bool {
	return t != nil && t.restatePort != ""
}

// EvaluateDueSchedules fires all due cron, one-shot and heartbeat schedules
// as of now, then prunes old schedule runs at most once per 24 hours.
// When Restate is enabled (restatePort is configured), schedule firing is
// managed exclusively by HaroSchedule Virtual Objects, and polling evaluation
// is skipped to prevent duplicate dispatches. In addition, when Restate is
// enabled, active schedules are periodically reconciled via BackfillActiveSchedules
// to ensure schedules created or updated during transient ingress outages are armed.
func (t *Ticker) EvaluateDueSchedules(ctx context.Context, now time.Time) {
	if t == nil || t.db == nil || t.dispatcher == nil {
		return
	}

	if !t.isRestateEnabled() {
		t.evaluateDueCronSchedules(ctx, now)
		t.evaluateDueOneShotSchedules(ctx, now)
		t.evaluateDueHeartbeatSchedules(ctx, now)
	} else {
		t.ensureRegisteredAndBackfilled(ctx)
	}

	t.PruneOldScheduleRuns(ctx, now)
}

// PruneOldScheduleRuns prunes old schedule runs if at least 24h has elapsed since the last prune.
func (t *Ticker) PruneOldScheduleRuns(ctx context.Context, now time.Time) {
	if t == nil || t.db == nil {
		return
	}
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
			Error:        errStaleScheduleOverdue,
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
			Error:        errStaleScheduleOverdue,
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
	req := eriwire.BrainDispatchRequest{
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
	resp, dispatchErr := dispatch.TraceFire(ctx, model.ScheduleTypeHeartbeat, hb.ID, func(ctx context.Context) (eriwire.DispatchEnqueueResponse, error) {
		return t.dispatcher.Dispatch(ctx, req)
	})
	metrics.RecordScheduleDispatchDuration(model.ScheduleTypeHeartbeat, time.Since(dispatchStart).Seconds())
	if dispatchErr == nil && resp.Status == eriwire.StatusSkippedActive {
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

	msgID := strutil.FirstNonEmpty(resp.DispatchID, runID)
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

	// The run row is keyed by a deterministic UUID; the ID eri echoes back is the
	// dispatch ID haro sent, which finalize stores as the run's message_id.
	run, err := store.GetScheduleRunByDispatchID(ctx, database, dispatchID)
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

	if err := store.UpdateScheduleRunOutcome(ctx, database, run.ID, status, ev.Error, completedAt, durationMs); err != nil {
		log.Printf("[Haro] failed to update schedule run outcome for %s (run %s): %v", dispatchID, run.ID, err)
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

func (t *Ticker) startRestateServer(ctx context.Context, port string) (chan error, error) {
	restateServer := server.NewRestate().
		Bind(restate.Reflect(NewHaroSchedule(t.db, t.dispatcher), restate.WithInactivityTimeout(time.Hour), restate.WithAbortTimeout(time.Hour))).
		Bind(restate.Reflect(NewHaroEventWatch(t.db, t.dispatcher), restate.WithInactivityTimeout(time.Hour), restate.WithAbortTimeout(time.Hour)))

	h, err := restateServer.Handler()
	if err != nil {
		return nil, fmt.Errorf("create restate handler: %w", err)
	}

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Handler:           h,
		Protocols:         &protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("listen on restate port %s: %w", port, err)
	}

	errChan := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if shutErr := srv.Shutdown(shutdownCtx); shutErr != nil {
			log.Printf("[Haro Ticker] Restate server shutdown notice: %v", shutErr)
		}
	}()

	go func() {
		log.Printf("[Haro Ticker] Restate server listening on %s", ln.Addr().String())
		if sErr := srv.Serve(ln); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			errChan <- sErr
		}
	}()
	return errChan, nil
}

func (t *Ticker) ensureRegisteredAndBackfilled(ctx context.Context) {
	if t == nil {
		return
	}

	t.mu.Lock()
	registered := t.restateRegistered
	backfilled := t.restateBackfilled
	t.mu.Unlock()

	if !registered {
		if t.adminURL != "" && t.advertiseURL != "" {
			regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
			err := RegisterDeployment(regCtx, t.adminURL, t.advertiseURL, t.httpClient)
			regCancel()
			if err != nil {
				log.Printf("[Haro Ticker] warning: failed to register deployment with restate admin: %v", err)
				return
			}
		}
		t.mu.Lock()
		t.restateRegistered = true
		t.mu.Unlock()
	}

	if !backfilled && t.ingressURL != "" {
		backfillCtx, backfillCancel := context.WithTimeout(ctx, 10*time.Second)
		defer backfillCancel()
		if err := t.BackfillActiveSchedules(backfillCtx); err != nil {
			log.Printf("[Haro Ticker] warning: failed to backfill active schedules: %v", err)
			return
		}
		t.mu.Lock()
		t.restateBackfilled = true
		t.mu.Unlock()
	}
}

// Start boots the Restate server on restatePort (if configured), registers with Restate Admin,
// backfills active schedules, and monitors ctx until cancellation.
// It returns an error if the Restate server fails to listen or serve, ensuring the process
// does not silently run with an inactive scheduler.
func (t *Ticker) Start(ctx context.Context) error {
	if t == nil {
		return nil
	}

	port := t.restatePort
	if port == "" {
		port = defaultRestatePort
	}

	var errChan chan error
	if t.restatePort != "" && t.db != nil {
		var sErr error
		errChan, sErr = t.startRestateServer(ctx, port)
		if sErr != nil {
			log.Printf("[Haro Ticker] failed to start Restate server: %v", sErr)
			return fmt.Errorf("restate server listen on :%s: %w", port, sErr)
		}
		t.ensureRegisteredAndBackfilled(ctx)
	} else {
		t.EvaluateDueSchedules(ctx, time.Now().UTC())
	}

	interval := t.interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case rErr := <-errChan:
			log.Printf("[Haro Ticker] Restate server stopped with error: %v", rErr)
			return fmt.Errorf("restate server error: %w", rErr)
		case now := <-ticker.C:
			t.EvaluateDueSchedules(ctx, now.UTC())
		}
	}
}
