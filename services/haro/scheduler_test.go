package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tempPath := filepath.Join(t.TempDir(), fmt.Sprintf("test_%d.db", time.Now().UnixNano()))
	db, err := OpenDB(&Config{DatabaseURL: tempPath})
	if err != nil {
		t.Fatalf("setupTestDB OpenDB() failed: %v", err)
	}
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
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestPureHelpers_IsScheduleStale(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		scheduledAt time.Time
		maxOverdue  time.Duration
		wantStale   bool
	}{
		{
			name:        "not stale within maxOverdue",
			scheduledAt: now.Add(-1 * time.Hour),
			maxOverdue:  24 * time.Hour,
			wantStale:   false,
		},
		{
			name:        "stale when overdue by more than maxOverdue",
			scheduledAt: now.Add(-25 * time.Hour),
			maxOverdue:  24 * time.Hour,
			wantStale:   true,
		},
		{
			name:        "zero maxOverdue never reports stale",
			scheduledAt: now.Add(-48 * time.Hour),
			maxOverdue:  0,
			wantStale:   false,
		},
		{
			name:        "negative maxOverdue never reports stale",
			scheduledAt: now.Add(-48 * time.Hour),
			maxOverdue:  -1 * time.Hour,
			wantStale:   false,
		},
		{
			name:        "future scheduledAt is not stale",
			scheduledAt: now.Add(1 * time.Hour),
			maxOverdue:  24 * time.Hour,
			wantStale:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsScheduleStale(tc.scheduledAt, now, tc.maxOverdue)
			if got != tc.wantStale {
				t.Errorf("IsScheduleStale() = %v, want %v", got, tc.wantStale)
			}
		})
	}
}

func TestPureHelpers_ShouldPruneRuns(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		lastPrune   time.Time
		interval    time.Duration
		shouldPrune bool
	}{
		{
			name:        "zero lastPrune triggers prune",
			lastPrune:   time.Time{},
			interval:    24 * time.Hour,
			shouldPrune: true,
		},
		{
			name:        "interval elapsed triggers prune",
			lastPrune:   now.Add(-25 * time.Hour),
			interval:    24 * time.Hour,
			shouldPrune: true,
		},
		{
			name:        "exact interval triggers prune",
			lastPrune:   now.Add(-24 * time.Hour),
			interval:    24 * time.Hour,
			shouldPrune: true,
		},
		{
			name:        "less than interval does not trigger prune",
			lastPrune:   now.Add(-12 * time.Hour),
			interval:    24 * time.Hour,
			shouldPrune: false,
		},
		{
			name:        "zero or negative interval does not trigger prune",
			lastPrune:   now.Add(-48 * time.Hour),
			interval:    0,
			shouldPrune: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldPruneRuns(now, tc.lastPrune, tc.interval)
			if got != tc.shouldPrune {
				t.Errorf("ShouldPruneRuns() = %v, want %v", got, tc.shouldPrune)
			}
		})
	}
}

func TestPureHelpers_CalculateNextRun(t *testing.T) {
	now := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		cronExpr string
		timezone string
		wantErr  bool
		wantHour int
	}{
		{
			name:     "valid cron expression UTC",
			cronExpr: "0 12 * * *",
			timezone: "UTC",
			wantErr:  false,
			wantHour: 12,
		},
		{
			name:     "valid cron expression America/Los_Angeles",
			cronExpr: "0 12 * * *",
			timezone: "America/Los_Angeles",
			wantErr:  false,
			wantHour: 19, // 12 PM PDT is 19:00 UTC
		},
		{
			name:     "invalid cron expression",
			cronExpr: "not a cron",
			timezone: "UTC",
			wantErr:  true,
		},
		{
			name:     "invalid timezone",
			cronExpr: "0 12 * * *",
			timezone: "NonExistent/Zone",
			wantErr:  true,
		},
		{
			name:     "empty timezone",
			cronExpr: "0 12 * * *",
			timezone: "",
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next, err := CalculateNextRun(tc.cronExpr, tc.timezone, now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CalculateNextRun() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && next.Hour() != tc.wantHour {
				t.Errorf("CalculateNextRun() hour = %d, want %d", next.Hour(), tc.wantHour)
			}
		})
	}
}

func TestHTTPPromptDispatcher(t *testing.T) {
	tests := []struct {
		name           string
		serverStatus   int
		serverResponse string
		req            PromptDispatchRequest
		closeServer    bool
		nilDispatcher  bool
		wantErr        bool
		wantChannelID  string
	}{
		{
			name:           "success accepted 202",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","channel_id":"123456789","message_id":"msg-1"}`,
			req: PromptDispatchRequest{
				Prompt:    "echo test",
				ChannelID: "123456789",
				Title:     "Daily Check",
				MessageID: "run-1",
				Effort:    "low",
			},
			wantChannelID: "123456789",
		},
		{
			name:           "success ok 200",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"ok","channel_id":"123456789","message_id":"msg-2"}`,
			req: PromptDispatchRequest{
				Prompt:    "echo test 2",
				ChannelID: "123456789",
			},
			wantChannelID: "123456789",
		},
		{
			name:           "server returns error json 400",
			serverStatus:   http.StatusBadRequest,
			serverResponse: `{"error":"prompt cannot be empty"}`,
			req: PromptDispatchRequest{
				ChannelID: "123456789",
			},
			wantErr: true,
		},
		{
			name:           "server returns non-json error 500",
			serverStatus:   http.StatusInternalServerError,
			serverResponse: `Internal Server Error`,
			req: PromptDispatchRequest{
				Prompt: "fail",
			},
			wantErr: true,
		},
		{
			name:           "server returns invalid json on 200",
			serverStatus:   http.StatusOK,
			serverResponse: `not valid json`,
			req: PromptDispatchRequest{
				Prompt: "test invalid json",
			},
			wantErr: true,
		},
		{
			name:        "closed server network error",
			closeServer: true,
			req: PromptDispatchRequest{
				Prompt: "test closed",
			},
			wantErr: true,
		},
		{
			name:          "nil dispatcher returns error",
			nilDispatcher: true,
			req:           PromptDispatchRequest{Prompt: "test"},
			wantErr:       true,
		},
		{
			name:           "default http client used when httpClient is nil",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","channel_id":"nil-client-chan","message_id":"msg-nc"}`,
			req: PromptDispatchRequest{
				Prompt:    "nil client test",
				ChannelID: "nil-client-chan",
			},
			wantChannelID: "nil-client-chan",
		},
		{
			name: "empty brain url defaults to eri.aerial",
			req: PromptDispatchRequest{
				Prompt: "test empty url",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %q", r.Header.Get("Content-Type"))
				}
				if r.URL.Path != "/prompt" {
					t.Errorf("expected path /prompt, got %q", r.URL.Path)
				}
				w.WriteHeader(tc.serverStatus)
				_, _ = w.Write([]byte(tc.serverResponse))
			}))
			defer server.Close()

			var dispatcher PromptDispatcher
			if tc.nilDispatcher {
				dispatcher = (*HTTPPromptDispatcher)(nil)
			} else if tc.name == "default http client used when httpClient is nil" {
				dispatcher = &HTTPPromptDispatcher{brainURL: server.URL}
			} else if tc.name == "empty brain url defaults to eri.aerial" {
				dispatcher = &HTTPPromptDispatcher{}
			} else {
				serverURL := server.URL + "/"
				if tc.closeServer {
					server.Close()
				}
				dispatcher = NewHTTPPromptDispatcher(serverURL)
			}

			ctx := context.Background()
			if tc.name == "empty brain url defaults to eri.aerial" {
				cancelCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelCtx
			}

			resp, err := dispatcher.DispatchPrompt(ctx, tc.req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DispatchPrompt() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && resp.ChannelID != tc.wantChannelID {
				t.Errorf("resp.ChannelID = %q, want %q", resp.ChannelID, tc.wantChannelID)
			}
		})
	}
}

func TestDB_CronOperations(t *testing.T) {
	db := setupTestDB(t)
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
	if err := AdvanceCronNextRun(ctx, db, cron1.ID, newNext); err != nil {
		t.Fatalf("AdvanceCronNextRun() err = %v", err)
	}

	// Advance non-existent schedule fails
	if err := AdvanceCronNextRun(ctx, db, "missing-cron", newNext); err == nil {
		t.Errorf("AdvanceCronNextRun() on missing ID expected error, got nil")
	}

	// Advance empty ID fails
	if err := AdvanceCronNextRun(ctx, db, "", newNext); err == nil {
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
	db := setupTestDB(t)
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

func TestDB_ScheduleRunsAndPruning(t *testing.T) {
	db := setupTestDB(t)
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
}

func (m *mockDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	m.dispatched = append(m.dispatched, req)
	return m.response, m.err
}

func TestTicker_EvaluateDueSchedules_Scenarios(t *testing.T) {
	tests := []struct {
		name              string
		setupCron         *CronSchedule
		setupOneShot      *OneShotSchedule
		dispatcherStatus  string
		responseChannelID string
		responseMessageID string
		dispatcherErr     error
		evaluateNowOffset time.Duration
		wantDispatched    int
		wantRunStatus     string
		wantRunError      string
	}{
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
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

			mock := &mockDispatcher{
				response: PromptDispatchResponse{
					Status:    tc.dispatcherStatus,
					ChannelID: tc.responseChannelID,
					MessageID: tc.responseMessageID,
				},
				err: tc.dispatcherErr,
			}

			ticker := NewTicker(db, mock, 30*time.Second)
			ticker.EvaluateDueSchedules(ctx, now)

			if len(mock.dispatched) != tc.wantDispatched {
				t.Fatalf("dispatched count = %d, want %d", len(mock.dispatched), tc.wantDispatched)
			}

			// Query schedule_runs
			row := db.QueryRowContext(ctx, "SELECT status, error FROM schedule_runs WHERE schedule_id = ?", scheduleID)
			var status, errMsg string
			if err := row.Scan(&status, &errMsg); err != nil {
				t.Fatalf("failed to query schedule_runs for %s: %v", scheduleID, err)
			}
			if status != tc.wantRunStatus {
				t.Errorf("schedule_run status = %q, want %q", status, tc.wantRunStatus)
			}
			if tc.wantRunError != "" && !strings.Contains(errMsg, tc.wantRunError) {
				t.Errorf("schedule_run error = %q, want substring %q", errMsg, tc.wantRunError)
			}
		})
	}
}

func TestTicker_GuardsAndPruning(t *testing.T) {
	db := setupTestDB(t)
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
	db := setupTestDB(t)
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
