package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/haro/internal/store"
)

func setupTestDB(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()
	tempPath := filepath.Join(t.TempDir(), fmt.Sprintf("test_%d.db", time.Now().UnixNano()))
	db, err := OpenDB(ctx, &Config{DatabaseURL: tempPath})
	if err != nil {
		t.Fatalf("setupTestDB OpenDB(t.Context(), ) failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	schema := `
	CREATE TABLE IF NOT EXISTS schedule_runs (
		id TEXT PRIMARY KEY,
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

func TestHandleOneShotJob_Scenarios(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

	// Nil message returns error
	if err := HandleOneShotJob(ctx, nil, db, nil); err == nil {
		t.Error("expected error for nil message")
	}

	// Nil database returns error
	msgValid := &memoryMsg{data: []byte(`{"id":"os-valid-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}
	if err := HandleOneShotJob(ctx, nil, nil, msgValid); err == nil {
		t.Error("expected error for nil database")
	}

	// Invalid JSON drops cleanly
	msgBadJSON := &memoryMsg{data: []byte(`not-json`)}
	if err := HandleOneShotJob(ctx, nil, db, msgBadJSON); err != nil {
		t.Errorf("expected nil error for bad json, got %v", err)
	}

	// Missing required fields drops cleanly
	msgMissing := &memoryMsg{data: []byte(`{"thread_id":""}`)}
	if err := HandleOneShotJob(ctx, nil, db, msgMissing); err != nil {
		t.Errorf("expected nil error for missing fields, got %v", err)
	}

	// Valid insert succeeds
	if err := HandleOneShotJob(ctx, nil, db, msgValid); err != nil {
		t.Fatalf("HandleOneShotJob valid failed: %v", err)
	}
	sched, err := store.GetOneShotSchedule(ctx, db, "os-valid-1")
	if err != nil {
		t.Fatalf("GetOneShotSchedule failed: %v", err)
	}
	if sched.Prompt != "p1" || sched.ThreadID != "th-1" {
		t.Errorf("unexpected schedule: %+v", sched)
	}

	// Duplicate insert is idempotent (no error, remains valid)
	if err := HandleOneShotJob(ctx, nil, db, msgValid); err != nil {
		t.Fatalf("HandleOneShotJob duplicate failed: %v", err)
	}

	// Server invalidates cache on insert
	toolHandler := NewToolHandler(&Config{DatabaseURL: ":memory:"}, db)
	srv := NewServer(toolHandler)
	msgWithServer := &memoryMsg{data: []byte(fmt.Sprintf(`{"thread_id":"th-2","prompt":"p2","run_at":%q}`, now.Format(time.RFC3339)))}
	if err := HandleOneShotJob(ctx, srv, nil, msgWithServer); err != nil {
		t.Fatalf("HandleOneShotJob with server failed: %v", err)
	}
}

func TestHandleOneShotJob_ClosedDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	closedDB, err := OpenDB(ctx, &Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	msgClosed := &memoryMsg{data: []byte(`{"id":"os-closed-1","thread_id":"th-1","prompt":"p1","run_at":"2026-11-14T12:00:00Z"}`)}
	if err := HandleOneShotJob(ctx, nil, closedDB, msgClosed); err == nil {
		t.Error("expected error for closed database")
	}
}

func TestDB_CronOperations(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

	cron1 := CronSchedule{
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
	cronFuture := CronSchedule{
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
	cronDisabled := CronSchedule{
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

	for _, c := range []CronSchedule{cron1, cronFuture, cronDisabled} {
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

	osDue := OneShotSchedule{
		ID:       "oneshot-due-1",
		ThreadID: "th-1",
		Prompt:   "one shot prompt 1",
		RunAt:    now.Add(-5 * time.Minute),
		Effort:   "medium",
	}
	osFuture := OneShotSchedule{
		ID:       "oneshot-future-2",
		ThreadID: "th-2",
		Prompt:   "one shot prompt 2",
		RunAt:    now.Add(1 * time.Hour),
		Effort:   "low",
	}

	for _, s := range []OneShotSchedule{osDue, osFuture} {
		if err := InsertOneShotSchedule(ctx, db, s); err != nil {
			t.Fatalf("InsertOneShotSchedule() err = %v", err)
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

	// Create ScheduleRun
	completedAt := now.Add(2 * time.Second)
	run1 := ScheduleRun{
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
	runZero := ScheduleRun{
		ID:           "run-zero-started",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
	}
	if err := store.CreateScheduleRun(ctx, db, runZero); err != nil {
		t.Fatalf("store.CreateScheduleRun(zero StartedAt) err = %v", err)
	}

	// Create invalid run (empty ID)
	if err := store.CreateScheduleRun(ctx, db, ScheduleRun{}); err == nil {
		t.Errorf("store.CreateScheduleRun() empty ID expected error, got nil")
	}

	// Insert older runs to test pruning
	for i := 2; i <= 6; i++ {
		r := ScheduleRun{
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
			_, err := GetScheduleSummaryMetrics(ctx, db)
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
		{"CreateScheduleRun", store.CreateScheduleRun(ctx, nil, ScheduleRun{ID: "run-id"})},
		{"PruneScheduleRuns", func() error { _, err := store.PruneScheduleRuns(ctx, nil, 10, 24*time.Hour, now); return err }()},
		{"InsertHeartbeatSchedule", store.InsertHeartbeatSchedule(ctx, nil, HeartbeatSchedule{})},
		{"ListHeartbeatSchedules", func() error { _, err := ListHeartbeatSchedules(ctx, nil, ""); return err }()},
		{"GetCronSchedule", func() error { _, err := store.GetCronSchedule(ctx, nil, "id"); return err }()},
		{"GetCronSchedule empty id", func() error { _, err := store.GetCronSchedule(ctx, setupTestDB(ctx, t), "  "); return err }()},
		{"GetOneShotSchedule", func() error { _, err := store.GetOneShotSchedule(ctx, nil, "id"); return err }()},
		{"GetOneShotSchedule empty id", func() error { _, err := store.GetOneShotSchedule(ctx, setupTestDB(ctx, t), "  "); return err }()},
		{"GetHeartbeatSchedule", func() error { _, err := store.GetHeartbeatSchedule(ctx, nil, "id"); return err }()},
		{"UpdateHeartbeatSchedule", store.UpdateHeartbeatSchedule(ctx, nil, HeartbeatSchedule{ID: "id"})},
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
	dispatched []PromptDispatchRequest
	response   PromptDispatchResponse
	err        error

	dispatchedRequests []DispatchRequest
	dispatchResponse   DispatchResponse
	dispatchErr        error
}

func (m *mockDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	m.dispatched = append(m.dispatched, req)
	return m.response, m.err
}

func (m *mockDispatcher) Dispatch(ctx context.Context, req DispatchRequest) (DispatchResponse, error) {
	m.dispatchedRequests = append(m.dispatchedRequests, req)
	return m.dispatchResponse, m.dispatchErr
}
