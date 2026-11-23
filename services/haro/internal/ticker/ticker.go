// Package ticker hosts Haro's Restate Virtual Objects, its only firing path.
//
// HaroSchedule fires cron, one-shot and heartbeat schedules and HaroEventWatch
// fires event watches. Ticker serves them, registers the deployment and arms
// active schedules once at startup. The package also handles dispatch outcomes
// (run timing and monotonic heartbeat watermark advancement).
package ticker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
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

// retireSchedule ends a schedule whose Fire or Arm found it gone: a deleted
// schedule has nothing left to fence, so its object state is cleared; a
// disabled one is fenced so it can be re-armed later.
func retireSchedule(ctx restate.ObjectContext, scheduleID string, gen int64, loaded ScheduleRow) error {
	if loaded.Found {
		fenceSchedule(ctx, gen)
	} else {
		restate.ClearAll(ctx)
	}
	return scheduleGoneError(scheduleID, loaded)
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
		return restate.Void{}, retireSchedule(ctx, scheduleID, gen, loaded)
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
	armedDue, err := restate.Get[string](ctx, "armed_due_at")
	if err != nil {
		return restate.Void{}, err
	}
	// Cancel clears all state, so a pending Fire then finds no generation and
	// no armed tick. It is allowed through and re-checks the schedule row: a
	// deleted or disabled schedule ends there, while one re-enabled for the
	// same tick (whose Arm Restate deduplicated) still fires.
	cleared := currentGen == 0 && armedDue == ""
	if req.Generation != currentGen && !cleared {
		return restate.Void{}, nil
	}
	// A Fire for a tick that has since been superseded (re-armed for another
	// due time, or already fired and advanced) is a no-op.
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
		return restate.Void{}, retireSchedule(ctx, scheduleID, currentGen, loaded)
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
		restate.ClearAll(ctx)
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

// Cancel clears the object's state; it is sent after the schedule row was
// deleted or disabled. A Fire still pending re-checks the row and ends there.
func (*HaroSchedule) Cancel(ctx restate.ObjectContext, _ struct{}) (restate.Void, error) {
	restate.ClearAll(ctx)
	return restate.Void{}, nil
}

// Ticker hosts haro's Restate endpoint (HaroSchedule, HaroEventWatch). At
// startup it registers the deployment, arms every active schedule once and
// prunes old run records; after that all firing is driven by the Restate
// objects themselves.
type Ticker struct {
	db                *sql.DB
	dispatcher        dispatch.PromptDispatcher
	restatePort       string
	ingressURL        string
	adminURL          string
	advertiseURL      string
	httpClient        *http.Client
	startupBackoff    time.Duration
	restateRegistered bool
	restateBackfilled bool
}

const (
	startupInitialBackoff = time.Second
	startupMaxBackoff     = 30 * time.Second
	startupAttemptTimeout = 10 * time.Second
	runRetentionCount     = 1000
	runRetentionAge       = 30 * 24 * time.Hour
)

// NewTicker returns a Ticker serving HaroSchedule and HaroEventWatch.
func NewTicker(db *sql.DB, dispatcher dispatch.PromptDispatcher) *Ticker {
	return &Ticker{
		db:             db,
		dispatcher:     dispatcher,
		httpClient:     http.DefaultClient,
		startupBackoff: startupInitialBackoff,
	}
}

// SetRestateConfig configures Restate server port and URLs for the Ticker.
func (t *Ticker) SetRestateConfig(port, ingressURL, adminURL, advertiseURL string) {
	t.restatePort = strings.TrimSpace(port)
	t.ingressURL = strings.TrimRight(strings.TrimSpace(ingressURL), "/")
	t.adminURL = strings.TrimRight(strings.TrimSpace(adminURL), "/")
	t.advertiseURL = strings.TrimRight(strings.TrimSpace(advertiseURL), "/")
}

// RegisterDeployment registers advertiseURL with the Restate admin deployments endpoint.
func RegisterDeployment(ctx context.Context, adminURL, advertiseURL string, client *http.Client) error {
	return restateingress.RegisterDeployment(ctx, client, adminURL, advertiseURL)
}

// scheduleArm is one schedule tick the startup backfill arms.
type scheduleArm struct {
	scheduleType string
	id           string
	dueAt        time.Time
}

// activeScheduleArms lists the next tick of every enabled schedule.
func activeScheduleArms(ctx context.Context, db *sql.DB) ([]scheduleArm, error) {
	crons, err := store.ListCronSchedules(ctx, db, "", "")
	if err != nil {
		return nil, err
	}
	oneshots, err := store.ListOneShotSchedules(ctx, db, "", "")
	if err != nil {
		return nil, err
	}
	heartbeats, err := store.ListHeartbeatSchedules(ctx, db, "", "")
	if err != nil {
		return nil, err
	}
	arms := make([]scheduleArm, 0, len(crons)+len(oneshots)+len(heartbeats))
	for _, c := range crons {
		if c.Enabled {
			arms = append(arms, scheduleArm{model.ScheduleTypeCron, c.ID, c.NextRunAt})
		}
	}
	for _, o := range oneshots {
		arms = append(arms, scheduleArm{model.ScheduleTypeOneShot, o.ID, o.RunAt})
	}
	for _, hb := range heartbeats {
		if hb.Enabled {
			arms = append(arms, scheduleArm{model.ScheduleTypeHeartbeat, hb.ID, hb.NextRunAt})
		}
	}
	return arms, nil
}

// BackfillActiveSchedules arms all active cron, one-shot, and heartbeat schedules via Restate ingress.
func (t *Ticker) BackfillActiveSchedules(ctx context.Context) error {
	if t.db == nil || t.ingressURL == "" {
		return nil
	}
	arms, err := activeScheduleArms(ctx, t.db)
	if err != nil {
		return err
	}
	var errs []error
	for _, a := range arms {
		endpoint := fmt.Sprintf("%s/HaroSchedule/%s/Arm/send", t.ingressURL, url.PathEscape(a.id))
		if _, postErr := restateingress.Post(ctx, t.httpClient, endpoint, model.ArmIdempotencyKey(a.scheduleType, a.id, a.dueAt), ArmRequest{ScheduleType: a.scheduleType}, 0); postErr != nil {
			log.Printf("[Haro Ticker] warning: failed to backfill %s %s: %v", a.scheduleType, a.id, postErr)
			errs = append(errs, postErr)
		}
	}
	return errors.Join(errs...)
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

	_, _, errChan, err := restateingress.StartServer(ctx, ":"+port, h)
	if err != nil {
		return nil, fmt.Errorf("listen on restate port %s: %w", port, err)
	}

	out := make(chan error, 1)
	go func() {
		for e := range errChan {
			out <- e
		}
	}()
	return out, nil
}

// ensureRegisteredAndBackfilled registers the deployment, then arms every
// active schedule. It reports whether both are done; a failed step is retried
// on the next call while a finished step is skipped.
func (t *Ticker) ensureRegisteredAndBackfilled(ctx context.Context) bool {
	if t == nil {
		return true
	}
	if !t.restateRegistered {
		regCtx, regCancel := context.WithTimeout(ctx, startupAttemptTimeout)
		err := RegisterDeployment(regCtx, t.adminURL, t.advertiseURL, t.httpClient)
		regCancel()
		if err != nil {
			log.Printf("[Haro Ticker] warning: failed to register deployment with restate admin: %v", err)
			return false
		}
		t.restateRegistered = true
	}
	if !t.restateBackfilled {
		backfillCtx, backfillCancel := context.WithTimeout(ctx, startupAttemptTimeout)
		err := t.BackfillActiveSchedules(backfillCtx)
		backfillCancel()
		if err != nil {
			log.Printf("[Haro Ticker] warning: failed to backfill active schedules: %v", err)
			return false
		}
		t.restateBackfilled = true
	}
	return true
}

// pruneScheduleRuns drops run records past the retention window once at startup.
func (t *Ticker) pruneScheduleRuns(ctx context.Context, now time.Time) {
	if n, err := store.PruneScheduleRuns(ctx, t.db, runRetentionCount, runRetentionAge, now); err != nil {
		log.Printf("[Haro Ticker] Error pruning schedule runs: %v", err)
	} else if n > 0 {
		log.Printf("[Haro Ticker] Pruned %d old schedule runs", n)
	}
}

// armOnStartup retries registration and the one-time backfill of arms with
// capped doubling backoff until both succeed, then prunes old run records. It
// stops quietly when ctx ends and returns an error if the Restate server fails.
func (t *Ticker) armOnStartup(ctx context.Context, serverErr <-chan error) error {
	backoff := max(t.startupBackoff, time.Millisecond)
	for !t.ensureRegisteredAndBackfilled(ctx) {
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case rErr := <-serverErr:
			timer.Stop()
			return fmt.Errorf("restate server error: %w", rErr)
		case <-timer.C:
		}
		backoff = min(2*backoff, startupMaxBackoff)
	}
	t.pruneScheduleRuns(ctx, time.Now().UTC())
	return nil
}

// Start boots the Restate server, registers it with Restate admin and arms all
// active schedules (retrying both with capped doubling backoff until they
// succeed), prunes old run records once, then serves until ctx ends. It
// returns an error if the Restate server fails to listen or serve.
func (t *Ticker) Start(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.db == nil {
		return errors.New("ticker: database is required")
	}
	port := strutil.FirstNonEmpty(t.restatePort, defaultRestatePort)
	errChan, err := t.startRestateServer(ctx, port)
	if err != nil {
		log.Printf("[Haro Ticker] failed to start Restate server: %v", err)
		return fmt.Errorf("restate server listen on :%s: %w", port, err)
	}

	if err := t.armOnStartup(ctx, errChan); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return nil
	case rErr := <-errChan:
		log.Printf("[Haro Ticker] Restate server stopped with error: %v", rErr)
		return fmt.Errorf("restate server error: %w", rErr)
	}
}
