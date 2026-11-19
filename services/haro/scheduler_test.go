package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	sched, err := GetOneShotSchedule(ctx, db, "os-valid-1")
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
		if err := InsertCronSchedule(ctx, db, c); err != nil {
			t.Fatalf("InsertCronSchedule() err = %v", err)
		}
	}

	dueCrons, err := GetDueCronSchedules(ctx, db, now)
	if err != nil {
		t.Fatalf("GetDueCronSchedules() err = %v", err)
	}
	if len(dueCrons) != 1 || dueCrons[0].ID != cron1.ID {
		t.Fatalf("GetDueCronSchedules() returned %+v, want only [%s]", dueCrons, cron1.ID)
	}

	// Advance next run
	newNext := now.Add(24 * time.Hour)
	if err = AdvanceCronNextRun(ctx, db, cron1.ID, newNext); err != nil {
		t.Fatalf("AdvanceCronNextRun() err = %v", err)
	}

	// Advance non-existent schedule fails
	if err = AdvanceCronNextRun(ctx, db, "missing-cron", newNext); err == nil {
		t.Errorf("AdvanceCronNextRun() on missing ID expected error, got nil")
	}

	// Advance empty ID fails
	if err = AdvanceCronNextRun(ctx, db, "", newNext); err == nil {
		t.Errorf("AdvanceCronNextRun() on empty ID expected error, got nil")
	}

	// Verify no more due crons
	dueCronsAfter, err := GetDueCronSchedules(ctx, db, now)
	if err != nil {
		t.Fatalf("GetDueCronSchedules() after advance err = %v", err)
	}
	if len(dueCronsAfter) != 0 {
		t.Fatalf("GetDueCronSchedules() expected 0 due, got %d", len(dueCronsAfter))
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

	dueOneShots, err := GetDueOneShotSchedules(ctx, db, now)
	if err != nil {
		t.Fatalf("GetDueOneShotSchedules() err = %v", err)
	}
	if len(dueOneShots) != 1 || dueOneShots[0].ID != osDue.ID {
		t.Fatalf("GetDueOneShotSchedules() returned %+v, want [%s]", dueOneShots, osDue.ID)
	}

	// Delete schedule
	deleted, err := DeleteOneShotSchedule(ctx, db, osDue.ID)
	if err != nil || !deleted {
		t.Fatalf("DeleteOneShotSchedule() deleted = %v, err = %v", deleted, err)
	}

	// Delete again returns false
	deletedAgain, err := DeleteOneShotSchedule(ctx, db, osDue.ID)
	if err != nil || deletedAgain {
		t.Fatalf("DeleteOneShotSchedule() again deleted = %v, err = %v", deletedAgain, err)
	}

	// Empty ID returns error
	if _, err := DeleteOneShotSchedule(ctx, db, ""); err == nil {
		t.Errorf("DeleteOneShotSchedule() empty ID expected error, got nil")
	}
}

func TestDB_DeleteByID(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()

	_ = InsertOneShotSchedule(ctx, db, OneShotSchedule{ID: "del-test-1", ThreadID: "th-1", RunAt: time.Now()})

	tests := []struct {
		name            string
		nilDB           bool
		query           string
		emptyErrMsg     string
		errPrefix       string
		id              string
		wantErr         bool
		wantErrContains string
		wantDeleted     bool
	}{
		{
			name:        "NilDB",
			nilDB:       true,
			query:       "DELETE FROM one_shot_schedules WHERE id = $1",
			id:          "del-test-1",
			wantErr:     true,
			wantDeleted: false,
		},
		{
			name:            "EmptyIDCustomMessage",
			query:           "DELETE FROM one_shot_schedules WHERE id = $1",
			emptyErrMsg:     "custom id required",
			id:              "   ",
			wantErr:         true,
			wantErrContains: "custom id required",
			wantDeleted:     false,
		},
		{
			name:            "BadQuerySyntax",
			query:           "DELETE FROM non_existent_table_xyz WHERE id = $1",
			errPrefix:       "failed to delete test item",
			id:              "del-test-1",
			wantErr:         true,
			wantErrContains: "failed to delete test item",
			wantDeleted:     false,
		},
		{
			name:        "SuccessExisting",
			query:       "DELETE FROM one_shot_schedules WHERE id = $1",
			id:          "del-test-1",
			wantErr:     false,
			wantDeleted: true,
		},
		{
			name:        "SuccessNonExistent",
			query:       "DELETE FROM one_shot_schedules WHERE id = $1",
			id:          "del-test-nonexistent",
			wantErr:     false,
			wantDeleted: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targetDB := db
			if tc.nilDB {
				targetDB = nil
			}
			deleted, err := deleteByID(ctx, targetDB, tc.query, tc.emptyErrMsg, tc.errPrefix, tc.id)
			if (err != nil) != tc.wantErr {
				t.Fatalf("deleteByID() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("deleteByID() err = %v, wantErrContains = %q", err, tc.wantErrContains)
				}
			}
			if deleted != tc.wantDeleted {
				t.Errorf("deleteByID() deleted = %v, want %v", deleted, tc.wantDeleted)
			}
		})
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
	if err := CreateScheduleRun(ctx, db, run1); err != nil {
		t.Fatalf("CreateScheduleRun() err = %v", err)
	}

	// Create run with zero StartedAt (defaults to current time)
	runZero := ScheduleRun{
		ID:           "run-zero-started",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
	}
	if err := CreateScheduleRun(ctx, db, runZero); err != nil {
		t.Fatalf("CreateScheduleRun(zero StartedAt) err = %v", err)
	}

	// Create invalid run (empty ID)
	if err := CreateScheduleRun(ctx, db, ScheduleRun{}); err == nil {
		t.Errorf("CreateScheduleRun() empty ID expected error, got nil")
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
		if err := CreateScheduleRun(ctx, db, r); err != nil {
			t.Fatalf("CreateScheduleRun(run-%d) err = %v", i, err)
		}
	}

	// Prune older than 30 days
	pruned, err := PruneScheduleRuns(ctx, db, 2, 30*24*time.Hour, now)
	if err != nil {
		t.Fatalf("PruneScheduleRuns() err = %v", err)
	}
	if pruned < 1 {
		t.Errorf("PruneScheduleRuns() expected pruned >= 1, got %d", pruned)
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
		{"GetDueCronSchedules", func() error { _, err := GetDueCronSchedules(ctx, nil, now); return err }()},
		{"AdvanceCronNextRun", AdvanceCronNextRun(ctx, nil, "id", now)},
		{"GetDueOneShotSchedules", func() error { _, err := GetDueOneShotSchedules(ctx, nil, now); return err }()},
		{"DeleteOneShotSchedule", func() error { _, err := DeleteOneShotSchedule(ctx, nil, "id"); return err }()},
		{"CreateScheduleRun", CreateScheduleRun(ctx, nil, ScheduleRun{ID: "run-id"})},
		{"PruneScheduleRuns", func() error { _, err := PruneScheduleRuns(ctx, nil, 10, 24*time.Hour, now); return err }()},
		{"InsertHeartbeatSchedule", InsertHeartbeatSchedule(ctx, nil, HeartbeatSchedule{})},
		{"ListHeartbeatSchedules", func() error { _, err := ListHeartbeatSchedules(ctx, nil, ""); return err }()},
		{"GetCronSchedule", func() error { _, err := GetCronSchedule(ctx, nil, "id"); return err }()},
		{"GetCronSchedule empty id", func() error { _, err := GetCronSchedule(ctx, setupTestDB(ctx, t), "  "); return err }()},
		{"GetOneShotSchedule", func() error { _, err := GetOneShotSchedule(ctx, nil, "id"); return err }()},
		{"GetOneShotSchedule empty id", func() error { _, err := GetOneShotSchedule(ctx, setupTestDB(ctx, t), "  "); return err }()},
		{"GetHeartbeatSchedule", func() error { _, err := GetHeartbeatSchedule(ctx, nil, "id"); return err }()},
		{"UpdateHeartbeatSchedule", UpdateHeartbeatSchedule(ctx, nil, HeartbeatSchedule{ID: "id"})},
		{"GetDueHeartbeatSchedules", func() error { _, err := GetDueHeartbeatSchedules(ctx, nil, now); return err }()},
		{"AdvanceHeartbeatNextRun", AdvanceHeartbeatNextRun(ctx, nil, "id", now)},
		{"HasRunningHeartbeatRun", func() error { _, err := HasRunningHeartbeatRun(ctx, nil, "id", now); return err }()},
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

type evaluateDueSchedulesTestCase struct {
	name              string
	setupCron         *CronSchedule
	setupOneShot      *OneShotSchedule
	setupHeartbeat    *HeartbeatSchedule
	dispatcherStatus  string
	responseChannelID string
	responseMessageID string
	dispatcherErr     error
	evaluateNowOffset time.Duration
	wantDispatched    int
	wantRunStatus     string
	wantRunError      string
}

func runEvaluateDueSchedulesCase(t *testing.T, tc evaluateDueSchedulesTestCase) {
	t.Helper()
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC).Add(tc.evaluateNowOffset)

	var scheduleID string
	if tc.setupCron != nil {
		scheduleID = tc.setupCron.ID
		if err := InsertCronSchedule(ctx, db, *tc.setupCron); err != nil {
			t.Fatalf("InsertCronSchedule() err = %v", err)
		}
	}
	if tc.setupOneShot != nil {
		scheduleID = tc.setupOneShot.ID
		if err := InsertOneShotSchedule(ctx, db, *tc.setupOneShot); err != nil {
			t.Fatalf("InsertOneShotSchedule() err = %v", err)
		}
	}
	if tc.setupHeartbeat != nil {
		scheduleID = tc.setupHeartbeat.ID
		if err := InsertHeartbeatSchedule(ctx, db, *tc.setupHeartbeat); err != nil {
			t.Fatalf("InsertHeartbeatSchedule() err = %v", err)
		}
	}

	mock := &mockDispatcher{
		response: PromptDispatchResponse{
			Status:    tc.dispatcherStatus,
			ChannelID: tc.responseChannelID,
			MessageID: tc.responseMessageID,
		},
		err: tc.dispatcherErr,
		dispatchResponse: DispatchResponse{
			Status:     tc.dispatcherStatus,
			DispatchID: tc.responseMessageID,
			MessageID:  tc.responseMessageID,
		},
		dispatchErr: tc.dispatcherErr,
	}

	ticker := NewTicker(db, mock, 30*time.Second)
	ticker.EvaluateDueSchedules(ctx, now)

	totalDispatched := len(mock.dispatched) + len(mock.dispatchedRequests)
	if totalDispatched != tc.wantDispatched {
		t.Fatalf("dispatched count = %d, want %d", totalDispatched, tc.wantDispatched)
	}
	if tc.setupHeartbeat != nil && len(mock.dispatchedRequests) > 0 {
		if mock.dispatchedRequests[0].TargetID != tc.setupHeartbeat.TargetID {
			t.Errorf("dispatchedRequest TargetID = %q, want %q", mock.dispatchedRequests[0].TargetID, tc.setupHeartbeat.TargetID)
		}
	}

	assertScheduleRunStatus(ctx, t, db, scheduleID, tc.wantRunStatus, tc.wantRunError)
}

func assertScheduleRunStatus(ctx context.Context, t *testing.T, db *sql.DB, scheduleID, wantStatus, wantError string) {
	t.Helper()
	if wantStatus == "" {
		return
	}
	row := db.QueryRowContext(ctx, "SELECT status, error FROM schedule_runs WHERE schedule_id = ?", scheduleID)
	var status, errMsg string
	if err := row.Scan(&status, &errMsg); err != nil {
		t.Fatalf("failed to query schedule_runs for %s: %v", scheduleID, err)
	}
	if status != wantStatus {
		t.Errorf("schedule_run status = %q, want %q", status, wantStatus)
	}
	if wantError != "" && !strings.Contains(errMsg, wantError) {
		t.Errorf("schedule_run error = %q, want substring %q", errMsg, wantError)
	}
}

func cronDueSchedulesTestCases() []evaluateDueSchedulesTestCase {
	return []evaluateDueSchedulesTestCase{
		{
			name: "due cron dispatched successfully",
			setupCron: &CronSchedule{
				ID:          "cron-eval-1",
				TargetID:    "123456789",
				TitlePrefix: "Eval Title",
				CronExpr:    "0 12 * * *",
				Prompt:      "Daily Eval",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "medium",
			},
			dispatcherStatus:  "accepted",
			responseChannelID: "123456789",
			responseMessageID: "msg-1",
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "completed",
		},
		{
			name: "due cron with invalid cron expression uses fallback next run and dispatches",
			setupCron: &CronSchedule{
				ID:          "cron-eval-invalid-expr",
				TargetID:    "123456789",
				TitlePrefix: "Invalid Expr",
				CronExpr:    "not a valid cron",
				Prompt:      "Fallback Eval",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "medium",
			},
			dispatcherStatus:  "accepted",
			responseChannelID: "123456789",
			responseMessageID: "msg-invalid",
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "completed",
		},
		{
			name: "due cron with empty response fields falls back to target and run IDs",
			setupCron: &CronSchedule{
				ID:          "cron-eval-empty-resp",
				TargetID:    "123456789",
				TitlePrefix: "Empty Resp",
				CronExpr:    "0 12 * * *",
				Prompt:      "Empty Resp Eval",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "medium",
			},
			dispatcherStatus:  "accepted",
			responseChannelID: "",
			responseMessageID: "",
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "completed",
		},
		{
			name: "due cron stale over 24h skips dispatch",
			setupCron: &CronSchedule{
				ID:          "cron-eval-stale",
				TargetID:    "123456789",
				TitlePrefix: "Stale Title",
				CronExpr:    "0 12 * * *",
				Prompt:      "Stale Check",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC), // >24h overdue
				Enabled:     true,
				Effort:      "low",
			},
			evaluateNowOffset: 0,
			wantDispatched:    0,
			wantRunStatus:     "failed",
			wantRunError:      "stale schedule (>24h overdue)",
		},
		{
			name: "due cron dispatch error records failed run",
			setupCron: &CronSchedule{
				ID:          "cron-eval-err",
				TargetID:    "123456789",
				TitlePrefix: "Error Title",
				CronExpr:    "0 12 * * *",
				Prompt:      "Error Check",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "high",
			},
			dispatcherErr:     fmt.Errorf("connection refused"),
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "failed",
			wantRunError:      "connection refused",
		},
	}
}

func oneShotDueSchedulesTestCases() []evaluateDueSchedulesTestCase {
	return []evaluateDueSchedulesTestCase{
		{
			name: "due one shot dispatched successfully",
			setupOneShot: &OneShotSchedule{
				ID:       "oneshot-eval-1",
				ThreadID: "987654321",
				Prompt:   "Remind me",
				RunAt:    time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Effort:   "low",
			},
			dispatcherStatus:  "accepted",
			responseChannelID: "987654321",
			responseMessageID: "msg-os-1",
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "completed",
		},
		{
			name: "due one shot with empty response fields falls back to thread and run IDs",
			setupOneShot: &OneShotSchedule{
				ID:       "oneshot-eval-empty-resp",
				ThreadID: "987654321",
				Prompt:   "Remind me empty",
				RunAt:    time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Effort:   "low",
			},
			dispatcherStatus:  "accepted",
			responseChannelID: "",
			responseMessageID: "",
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "completed",
		},
		{
			name: "due one shot stale over 24h skips dispatch",
			setupOneShot: &OneShotSchedule{
				ID:       "oneshot-eval-stale",
				ThreadID: "987654321",
				Prompt:   "Stale Reminder",
				RunAt:    time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC), // >24h overdue
				Effort:   "low",
			},
			evaluateNowOffset: 0,
			wantDispatched:    0,
			wantRunStatus:     "failed",
			wantRunError:      "stale schedule (>24h overdue)",
		},
		{
			name: "due one shot dispatch error records failed run",
			setupOneShot: &OneShotSchedule{
				ID:       "oneshot-eval-err",
				ThreadID: "987654321",
				Prompt:   "Error Reminder",
				RunAt:    time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Effort:   "medium",
			},
			dispatcherErr:     fmt.Errorf("network timeout"),
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "failed",
			wantRunError:      "network timeout",
		},
	}
}

func evaluateDueSchedulesTestCases() []evaluateDueSchedulesTestCase {
	return append(cronDueSchedulesTestCases(), oneShotDueSchedulesTestCases()...)
}

func TestTicker_EvaluateDueSchedules_Scenarios(t *testing.T) {
	tests := evaluateDueSchedulesTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runEvaluateDueSchedulesCase(t, tc)
		})
	}
}

func TestTicker_GuardsAndPruning(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	mock := &mockDispatcher{response: PromptDispatchResponse{Status: "ok"}}
	ticker := NewTicker(db, mock, 30*time.Second)

	// Interval <= 0 defaults to 30s in NewTicker
	defaultTicker := NewTicker(db, mock, 0)
	if defaultTicker.interval != 30*time.Second {
		t.Errorf("NewTicker with 0 interval = %v, want 30s", defaultTicker.interval)
	}

	// Nil ticker does not panic
	var nilTicker *Ticker
	nilTicker.EvaluateDueSchedules(context.Background(), time.Now().UTC())
	nilTicker.Start(context.Background())

	// Nil DB or nil dispatcher does not panic
	nilDBTicker := NewTicker(nil, mock, 30*time.Second)
	nilDBTicker.EvaluateDueSchedules(context.Background(), time.Now().UTC())
	nilDispTicker := NewTicker(db, nil, 30*time.Second)
	nilDispTicker.EvaluateDueSchedules(context.Background(), time.Now().UTC())

	// Cancelled context returns early
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ticker.EvaluateDueSchedules(cancelledCtx, time.Now().UTC())

	// Pruning is triggered when lastPrune is zero
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	ticker.EvaluateDueSchedules(context.Background(), now)
	if ticker.lastPrune.IsZero() {
		t.Errorf("expected lastPrune to be updated, got zero")
	}

	// Immediate next evaluate does not prune again
	prevPrune := ticker.lastPrune
	ticker.EvaluateDueSchedules(context.Background(), now.Add(1*time.Minute))
	if ticker.lastPrune != prevPrune {
		t.Errorf("expected lastPrune unchanged, got %v", ticker.lastPrune)
	}
}

func TestTicker_StartLifecycle(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	mock := &mockDispatcher{response: PromptDispatchResponse{Status: "ok"}}
	ticker := NewTicker(db, mock, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		ticker.Start(ctx)
	}()

	cancel()

	select {
	case <-doneCh:
		// stopped cleanly without time.Sleep
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ticker.Start() to stop")
	}

	// Start with interval <= 0 on Ticker struct
	zeroTicker := &Ticker{db: db, dispatcher: mock}
	zeroCtx, zeroCancel := context.WithCancel(context.Background())
	zeroCancel()
	zeroTicker.Start(zeroCtx)
}
