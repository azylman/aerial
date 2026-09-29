package db

import (
	"context"
	"testing"
	"time"
)

func TestOneShotSchedule_EffortDefaultAndCoalesce(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 1. Unspecified effort defaults to "low"
	oneShot := OneShotSchedule{
		ID:       "oneshot-default",
		ThreadID: "thread-1",
		Prompt:   "hello test",
		RunAt:    time.Now().UTC().Add(-1 * time.Minute),
	}
	if err := CreateOneShotSchedule(db, oneShot); err != nil {
		t.Fatalf("failed to create one shot: %v", err)
	}

	due, err := GetDueOneShotSchedules(db)
	if err != nil {
		t.Fatalf("failed to get due: %v", err)
	}
	if len(due) == 0 {
		t.Fatalf("expected due schedule, got 0")
	}
	if due[0].Effort != "low" {
		t.Fatalf("expected default effort 'low', got %q", due[0].Effort)
	}

	// 2. Explicit high effort is preserved
	oneShotHigh := OneShotSchedule{
		ID:       "oneshot-high",
		ThreadID: "thread-2",
		Prompt:   "high reasoning",
		RunAt:    time.Now().UTC().Add(-1 * time.Minute),
		Effort:   "high",
	}
	if err := CreateOneShotSchedule(db, oneShotHigh); err != nil {
		t.Fatalf("failed to create high one shot: %v", err)
	}

	all, err := GetAllOneShotSchedules(db, "thread-2")
	if err != nil {
		t.Fatalf("failed to get all: %v", err)
	}
	if len(all) == 0 || all[0].Effort != "high" {
		t.Fatalf("expected explicit effort 'high', got %q", all[0].Effort)
	}

	// 3. Nil DB returns error
	if err := CreateOneShotSchedule(nil, oneShot); err == nil {
		t.Fatalf("expected error on nil database, got nil")
	}
}

func TestSchedules_CoalesceNullAndEmptyStringEffort(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// 1. Raw insert into one_shot_schedules with empty string effort
	_, err := db.ExecContext(ctx, `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6)`,
		"os-raw-empty", "thread-coalesce", "prompt 1", now.Add(-1*time.Minute), now, "")
	if err != nil {
		t.Fatalf("failed raw insert one_shot_schedules: %v", err)
	}

	dueOS, err := GetDueOneShotSchedules(db)
	if err != nil {
		t.Fatalf("GetDueOneShotSchedules failed: %v", err)
	}
	var foundOS bool
	for _, s := range dueOS {
		if s.ID == "os-raw-empty" {
			foundOS = true
			if s.Effort != "low" {
				t.Fatalf("expected effort to COALESCE to 'low', got %q", s.Effort)
			}
		}
	}
	if !foundOS {
		t.Fatalf("expected to find os-raw-empty in due one shots")
	}

	allOS, err := GetAllOneShotSchedules(db, "thread-coalesce")
	if err != nil {
		t.Fatalf("GetAllOneShotSchedules failed: %v", err)
	}
	if len(allOS) == 0 || allOS[0].Effort != "low" {
		t.Fatalf("expected allOS effort to COALESCE to 'low', got %v", allOS)
	}

	// 2. Raw insert into cron_schedules with empty string effort
	_, err = db.ExecContext(ctx, `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"cron-raw-empty", "target-coalesce", "Cron", "0 0 * * *", "daily prompt", "UTC", now.Add(-1*time.Minute), true, now, "")
	if err != nil {
		t.Fatalf("failed raw insert cron_schedules: %v", err)
	}

	dueCron, err := GetDueCronSchedules(db)
	if err != nil {
		t.Fatalf("GetDueCronSchedules failed: %v", err)
	}
	var foundCron bool
	for _, c := range dueCron {
		if c.ID == "cron-raw-empty" {
			foundCron = true
			if c.Effort != "low" {
				t.Fatalf("expected cron effort to COALESCE to 'low', got %q", c.Effort)
			}
		}
	}
	if !foundCron {
		t.Fatalf("expected to find cron-raw-empty in due crons")
	}

	allCron, err := GetAllCronSchedules(db, "target-coalesce")
	if err != nil {
		t.Fatalf("GetAllCronSchedules failed: %v", err)
	}
	if len(allCron) == 0 || allCron[0].Effort != "low" {
		t.Fatalf("expected allCron effort to COALESCE to 'low', got %v", allCron)
	}

	// 3. Raw insert into schedule_runs with empty string effort
	_, err = db.ExecContext(ctx, `INSERT INTO schedule_runs (id, schedule_id, schedule_type, message_id, target_id, thread_id, title, prompt, status, started_at, duration_ms, error, effort, model) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		"run-raw-empty", "cron-raw-empty", "cron", "m1", "target-coalesce", "thread-1", "Run", "prompt", "enqueued", now, 0, "", "", "")
	if err != nil {
		t.Fatalf("failed raw insert schedule_runs: %v", err)
	}

	runs, total, err := GetScheduleRunsPaginated(db, 10, 0, "cron-raw-empty", "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated failed: %v", err)
	}
	if total != 1 || len(runs) != 1 || runs[0].Effort != "low" {
		t.Fatalf("expected runs[0].Effort to COALESCE to 'low', got %q (total=%d)", runs[0].Effort, total)
	}
}

func TestSchedules_NilDatabaseAndNormalization(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 1. CreateCronSchedule with nil database
	if err := CreateCronSchedule(nil, CronSchedule{}); err == nil {
		t.Fatalf("expected error from CreateCronSchedule on nil DB, got nil")
	}

	// 2. CreateScheduleRun with nil database
	if err := CreateScheduleRun(nil, ScheduleRun{}); err == nil {
		t.Fatalf("expected error from CreateScheduleRun on nil DB, got nil")
	}

	// 3. Normalization checks for CreateCronSchedule
	c1 := CronSchedule{
		ID:        "c-norm-1",
		TargetID:  "t1",
		CronExpr:  "* * * * *",
		Prompt:    "p",
		NextRunAt: time.Now().UTC().Add(time.Hour),
		Enabled:   true,
		Effort:    "  HIGH  ",
	}
	if err := CreateCronSchedule(db, c1); err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}
	crons, err := GetAllCronSchedules(db, "t1")
	if err != nil || len(crons) == 0 {
		t.Fatalf("GetAllCronSchedules failed: %v", err)
	}
	if crons[0].Effort != "high" {
		t.Fatalf("expected effort 'high', got %q", crons[0].Effort)
	}

	// 4. UpdateCronScheduleEffort normalization
	if err := UpdateCronScheduleEffort(db, "c-norm-1", "something_else"); err != nil {
		t.Fatalf("UpdateCronScheduleEffort failed: %v", err)
	}
	crons, err = GetAllCronSchedules(db, "t1")
	if err != nil || len(crons) == 0 {
		t.Fatalf("GetAllCronSchedules failed: %v", err)
	}
	if crons[0].Effort != "low" {
		t.Fatalf("expected effort updated to 'low', got %q", crons[0].Effort)
	}

	// 5. CreateScheduleRun normalization
	run := ScheduleRun{
		ID:           "run-norm-1",
		ScheduleID:   "c-norm-1",
		ScheduleType: "cron",
		TargetID:     "t1",
		ThreadID:     "th1",
		Prompt:       "p",
		Effort:       "unknown-effort",
	}
	if err := CreateScheduleRun(db, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}
	runs, _, err := GetScheduleRunsPaginated(db, 10, 0, "c-norm-1", "")
	if err != nil || len(runs) == 0 {
		t.Fatalf("GetScheduleRunsPaginated failed: %v", err)
	}
	if runs[0].Effort != "low" {
		t.Fatalf("expected run effort normalized to 'low', got %q", runs[0].Effort)
	}
}
