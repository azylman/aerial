package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func setupTestDB(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()
	tempPath := filepath.Join(t.TempDir(), fmt.Sprintf("test_%d.db", time.Now().UnixNano()))
	db, err := testutil.OpenSQLiteTestDB(tempPath) //nolint:contextcheck // testutil seam takes no ctx by design
	if err != nil {
		t.Fatalf("setupTestDB OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	schema := `
	CREATE TABLE IF NOT EXISTS schedule_runs (
		id TEXT PRIMARY KEY,
		agent TEXT NOT NULL DEFAULT 'aerial',
		schedule_id TEXT NOT NULL,
		schedule_type TEXT NOT NULL,
		message_id TEXT NOT NULL DEFAULT '',
		target_id TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		title TEXT NOT NULL DEFAULT '',
		prompt TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'enqueued',
		started_at TIMESTAMP NOT NULL,
		completed_at TIMESTAMP,
		duration_ms INTEGER DEFAULT 0,
		effort TEXT NOT NULL DEFAULT 'low',
		model TEXT NOT NULL DEFAULT '',
		error TEXT NOT NULL DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("setupTestDB failed to create schedule_runs: %v", err)
	}
	return db
}

func TestDB_CronOperations(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

	cron1 := model.CronSchedule{
		ID:          "cron-due-1",
		TargetID:    "chan-1",
		TitlePrefix: "Title 1",
		CronExpr:    "0 12 * * *",
		Prompt:      "prompt 1",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Minute),
		Enabled:     true,
		Effort:      "low",
	}
	cronFuture := model.CronSchedule{
		ID:          "cron-future-2",
		TargetID:    "chan-1",
		TitlePrefix: "Title 2",
		CronExpr:    "0 14 * * *",
		Prompt:      "prompt 2",
		Timezone:    "UTC",
		NextRunAt:   now.Add(2 * time.Hour),
		Enabled:     true,
		Effort:      "medium",
	}
	cronDisabled := model.CronSchedule{
		ID:          "cron-disabled-3",
		TargetID:    "chan-1",
		TitlePrefix: "Title 3",
		CronExpr:    "0 12 * * *",
		Prompt:      "prompt 3",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-20 * time.Minute),
		Enabled:     false,
		Effort:      "high",
	}

	for _, c := range []model.CronSchedule{cron1, cronFuture, cronDisabled} {
		if err := store.InsertCronSchedule(ctx, db, c); err != nil {
			t.Fatalf("store.InsertCronSchedule() err = %v", err)
		}
	}

	dueCrons, err := store.GetDueCronSchedules(ctx, db, now)
	if err != nil {
		t.Fatalf("store.GetDueCronSchedules() err = %v", err)
	}
	if len(dueCrons) != 1 || dueCrons[0].ID != cron1.ID {
		t.Fatalf("store.GetDueCronSchedules() returned %+v, want only [%s]", dueCrons, cron1.ID)
	}

	// Advance next run
	newNext := now.Add(24 * time.Hour)
	if err = store.AdvanceCronNextRun(ctx, db, cron1.ID, newNext); err != nil {
		t.Fatalf("store.AdvanceCronNextRun() err = %v", err)
	}

	// Advance non-existent schedule fails
	if err = store.AdvanceCronNextRun(ctx, db, "missing-cron", newNext); err == nil {
		t.Errorf("store.AdvanceCronNextRun() on missing ID expected error, got nil")
	}

	// Advance empty ID fails
	if err = store.AdvanceCronNextRun(ctx, db, "", newNext); err == nil {
		t.Errorf("store.AdvanceCronNextRun() on empty ID expected error, got nil")
	}

	// Verify no more due crons
	dueCronsAfter, err := store.GetDueCronSchedules(ctx, db, now)
	if err != nil {
		t.Fatalf("store.GetDueCronSchedules() after advance err = %v", err)
	}
	if len(dueCronsAfter) != 0 {
		t.Fatalf("store.GetDueCronSchedules() expected 0 due, got %d", len(dueCronsAfter))
	}
}

func TestDB_OneShotOperations(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

	osDue := model.OneShotSchedule{
		ID:       "oneshot-due-1",
		ThreadID: "th-1",
		Prompt:   "one shot prompt 1",
		RunAt:    now.Add(-5 * time.Minute),
		Effort:   "medium",
	}
	osFuture := model.OneShotSchedule{
		ID:       "oneshot-future-2",
		ThreadID: "th-2",
		Prompt:   "one shot prompt 2",
		RunAt:    now.Add(1 * time.Hour),
		Effort:   "low",
	}

	for _, s := range []model.OneShotSchedule{osDue, osFuture} {
		if err := store.InsertOneShotSchedule(ctx, db, s); err != nil {
			t.Fatalf("store.InsertOneShotSchedule() err = %v", err)
		}
	}

	dueOneShots, err := store.GetDueOneShotSchedules(ctx, db, now)
	if err != nil {
		t.Fatalf("store.GetDueOneShotSchedules() err = %v", err)
	}
	if len(dueOneShots) != 1 || dueOneShots[0].ID != osDue.ID {
		t.Fatalf("store.GetDueOneShotSchedules() returned %+v, want [%s]", dueOneShots, osDue.ID)
	}

	// Delete schedule
	deleted, err := store.DeleteOneShotSchedule(ctx, db, osDue.ID)
	if err != nil || !deleted {
		t.Fatalf("store.DeleteOneShotSchedule() deleted = %v, err = %v", deleted, err)
	}

	// Delete again returns false
	deletedAgain, err := store.DeleteOneShotSchedule(ctx, db, osDue.ID)
	if err != nil || deletedAgain {
		t.Fatalf("store.DeleteOneShotSchedule() again deleted = %v, err = %v", deletedAgain, err)
	}

	// Empty ID returns error
	if _, err := store.DeleteOneShotSchedule(ctx, db, ""); err == nil {
		t.Errorf("store.DeleteOneShotSchedule() empty ID expected error, got nil")
	}
}

func TestDB_ScheduleRunsAndPruning(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

	// Create model.ScheduleRun
	completedAt := now.Add(2 * time.Second)
	run1 := model.ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		MessageID:    "msg-1",
		TargetID:     "chan-1",
		ThreadID:     "chan-1",
		Title:        "Title 1",
		Prompt:       "Run Prompt",
		Status:       "completed",
		StartedAt:    now.Add(-10 * time.Minute),
		CompletedAt:  &completedAt,
		DurationMs:   2000,
		Effort:       "medium",
	}
	if err := store.CreateScheduleRun(ctx, db, run1); err != nil {
		t.Fatalf("store.CreateScheduleRun() err = %v", err)
	}

	// Create run with zero StartedAt (defaults to current time)
	runZero := model.ScheduleRun{
		ID:           "run-zero-started",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
	}
	if err := store.CreateScheduleRun(ctx, db, runZero); err != nil {
		t.Fatalf("store.CreateScheduleRun(zero StartedAt) err = %v", err)
	}

	// Create invalid run (empty ID)
	if err := store.CreateScheduleRun(ctx, db, model.ScheduleRun{}); err == nil {
		t.Errorf("store.CreateScheduleRun() empty ID expected error, got nil")
	}

	// Insert older runs to test pruning
	for i := 2; i <= 6; i++ {
		r := model.ScheduleRun{
			ID:           fmt.Sprintf("run-%d", i),
			ScheduleID:   "cron-old",
			ScheduleType: "cron",
			StartedAt:    now.Add(time.Duration(-40*24*i) * time.Hour),
			Status:       "completed",
		}
		if err := store.CreateScheduleRun(ctx, db, r); err != nil {
			t.Fatalf("store.CreateScheduleRun(run-%d) err = %v", i, err)
		}
	}

	// Prune older than 30 days
	pruned, err := store.PruneScheduleRuns(ctx, db, 2, 30*24*time.Hour, now)
	if err != nil {
		t.Fatalf("store.PruneScheduleRuns() err = %v", err)
	}
	if pruned < 1 {
		t.Errorf("store.PruneScheduleRuns() expected pruned >= 1, got %d", pruned)
	}
}

func TestGetScheduleSummaryMetrics_MissingRunsTable(t *testing.T) {
	ctx := context.Background()
	db := setupTestDB(t.Context(), t)
	if _, err := db.ExecContext(ctx, "DROP TABLE schedule_runs"); err != nil {
		t.Fatalf("drop schedule_runs failed: %v", err)
	}
	tests := []struct {
		name    string
		wantErr string
	}{
		{"missing runs table surfaces query error", "failed to query schedule runs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.GetScheduleSummaryMetrics(ctx, db)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("GetScheduleSummaryMetrics err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestDB_NilDBGuards(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	tests := []struct {
		name string
		err  error
	}{
		{"GetDueCronSchedules", func() error { _, err := store.GetDueCronSchedules(ctx, nil, now); return err }()},
		{"AdvanceCronNextRun", store.AdvanceCronNextRun(ctx, nil, "id", now)},
		{"GetDueOneShotSchedules", func() error { _, err := store.GetDueOneShotSchedules(ctx, nil, now); return err }()},
		{"DeleteOneShotSchedule", func() error { _, err := store.DeleteOneShotSchedule(ctx, nil, "id"); return err }()},
		{"CreateScheduleRun", store.CreateScheduleRun(ctx, nil, model.ScheduleRun{ID: "run-id"})},
		{"PruneScheduleRuns", func() error { _, err := store.PruneScheduleRuns(ctx, nil, 10, 24*time.Hour, now); return err }()},
		{"InsertHeartbeatSchedule", store.InsertHeartbeatSchedule(ctx, nil, model.HeartbeatSchedule{})},
		{"ListHeartbeatSchedules", func() error { _, err := store.ListHeartbeatSchedules(ctx, nil, ""); return err }()},
		{"GetCronSchedule", func() error { _, err := store.GetCronSchedule(ctx, nil, "id"); return err }()},
		{"GetCronSchedule empty id", func() error { _, err := store.GetCronSchedule(ctx, setupTestDB(ctx, t), "  "); return err }()},
		{"GetOneShotSchedule", func() error { _, err := store.GetOneShotSchedule(ctx, nil, "id"); return err }()},
		{"GetOneShotSchedule empty id", func() error { _, err := store.GetOneShotSchedule(ctx, setupTestDB(ctx, t), "  "); return err }()},
		{"GetHeartbeatSchedule", func() error { _, err := store.GetHeartbeatSchedule(ctx, nil, "id"); return err }()},
		{"UpdateHeartbeatSchedule", store.UpdateHeartbeatSchedule(ctx, nil, model.HeartbeatSchedule{ID: "id"})},
		{"GetDueHeartbeatSchedules", func() error { _, err := store.GetDueHeartbeatSchedules(ctx, nil, now); return err }()},
		{"AdvanceHeartbeatNextRun", store.AdvanceHeartbeatNextRun(ctx, nil, "id", now)},
		{"HasRunningHeartbeatRun", func() error { _, err := store.HasRunningHeartbeatRun(ctx, nil, "id", now); return err }()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				t.Errorf("%s nil db expected error, got nil", tc.name)
			}
		})
	}
}

type mockDispatcher struct {
	dispatched []dispatch.PromptDispatchRequest
	response   dispatch.PromptDispatchResponse
	err        error

	dispatchedRequests []eriwire.BrainDispatchRequest
	dispatchResponse   eriwire.DispatchEnqueueResponse
	dispatchErr        error
}

func (m *mockDispatcher) DispatchPrompt(ctx context.Context, req dispatch.PromptDispatchRequest) (dispatch.PromptDispatchResponse, error) {
	m.dispatched = append(m.dispatched, req)
	return m.response, m.err
}

func (m *mockDispatcher) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	m.dispatchedRequests = append(m.dispatchedRequests, req)
	return m.dispatchResponse, m.dispatchErr
}
