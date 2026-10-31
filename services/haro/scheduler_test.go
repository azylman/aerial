package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tempPath := filepath.Join(t.TempDir(), fmt.Sprintf("test_%d.db", time.Now().UnixNano()))
	db, err := OpenDB(&Config{DatabaseURL: tempPath})
	if err != nil {
		t.Fatalf("setupTestDB OpenDB() failed: %v", err)
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

func TestHTTPPromptDispatcher_Dispatch(t *testing.T) {
	tests := []struct {
		name           string
		serverStatus   int
		serverResponse string
		req            DispatchRequest
		closeServer    bool
		nilDispatcher  bool
		wantErr        bool
		wantDispatchID string
	}{
		{
			name:           "success accepted 202",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","dispatch_id":"disp-1","message_id":"disp-1"}`,
			req: DispatchRequest{
				Source:       "heartbeat",
				SessionID:    "heartbeat-hb-1",
				Prompt:       "echo test",
				Effort:       "low",
				SkipIfActive: true,
			},
			wantDispatchID: "disp-1",
		},
		{
			name:           "server returns error json 400",
			serverStatus:   http.StatusBadRequest,
			serverResponse: `{"error":"prompt cannot be empty"}`,
			req: DispatchRequest{
				Source: "heartbeat",
			},
			wantErr: true,
		},
		{
			name:           "server returns non-json error 500",
			serverStatus:   http.StatusInternalServerError,
			serverResponse: `Internal Server Error`,
			req: DispatchRequest{
				Source: "heartbeat",
				Prompt: "fail",
			},
			wantErr: true,
		},
		{
			name:           "server returns invalid json on 200",
			serverStatus:   http.StatusOK,
			serverResponse: `not valid json`,
			req: DispatchRequest{
				Source: "heartbeat",
				Prompt: "test invalid json",
			},
			wantErr: true,
		},
		{
			name:        "closed server network error",
			closeServer: true,
			req: DispatchRequest{
				Source: "heartbeat",
				Prompt: "test closed",
			},
			wantErr: true,
		},
		{
			name:          "nil dispatcher returns error",
			nilDispatcher: true,
			req:           DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:       true,
		},
		{
			name:           "default http client used when httpClient is nil",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","dispatch_id":"disp-nc","message_id":"disp-nc"}`,
			req: DispatchRequest{
				Source:    "heartbeat",
				SessionID: "heartbeat-hb-nc",
				Prompt:    "nil client test",
			},
			wantDispatchID: "disp-nc",
		},
		{
			name:    "empty brain url defaults to eri.aerial",
			req:     DispatchRequest{Source: "heartbeat", Prompt: "test default url"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %q", r.Header.Get("Content-Type"))
				}
				if r.URL.Path != "/dispatch" {
					t.Errorf("expected path /dispatch, got %q", r.URL.Path)
				}
				w.WriteHeader(tc.serverStatus)
				_, _ = w.Write([]byte(tc.serverResponse))
			}))
			defer server.Close()

			var dispatcher *HTTPPromptDispatcher
			if tc.nilDispatcher {
				dispatcher = nil
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

			resp, err := dispatcher.Dispatch(ctx, tc.req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Dispatch() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && resp.DispatchID != tc.wantDispatchID {
				t.Errorf("resp.DispatchID = %q, want %q", resp.DispatchID, tc.wantDispatchID)
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

func TestDB_DeleteByID(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	_ = InsertOneShotSchedule(ctx, db, OneShotSchedule{ID: "del-test-1", ThreadID: "th-1", RunAt: time.Now()})

	tests := []struct {
		name        string
		nilDB       bool
		query       string
		emptyErrMsg string
		errPrefix   string
		id          string
		wantErr     bool
		wantDeleted bool
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
			name:        "EmptyIDCustomMessage",
			query:       "DELETE FROM one_shot_schedules WHERE id = $1",
			emptyErrMsg: "custom id required",
			id:          "   ",
			wantErr:     true,
			wantDeleted: false,
		},
		{
			name:        "BadQuerySyntax",
			query:       "DELETE FROM non_existent_table_xyz WHERE id = $1",
			errPrefix:   "failed to delete test item",
			id:          "del-test-1",
			wantErr:     true,
			wantDeleted: false,
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
			if deleted != tc.wantDeleted {
				t.Errorf("deleteByID() deleted = %v, want %v", deleted, tc.wantDeleted)
			}
		})
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
		{"InsertHeartbeatSchedule", InsertHeartbeatSchedule(ctx, nil, HeartbeatSchedule{})},
		{"ListHeartbeatSchedules", func() error { _, err := ListHeartbeatSchedules(ctx, nil, ""); return err }()},
		{"GetCronSchedule", func() error { _, err := GetCronSchedule(ctx, nil, "id"); return err }()},
		{"GetCronSchedule empty id", func() error { _, err := GetCronSchedule(ctx, setupTestDB(t), "  "); return err }()},
		{"GetOneShotSchedule", func() error { _, err := GetOneShotSchedule(ctx, nil, "id"); return err }()},
		{"GetOneShotSchedule empty id", func() error { _, err := GetOneShotSchedule(ctx, setupTestDB(t), "  "); return err }()},
		{"GetHeartbeatSchedule", func() error { _, err := GetHeartbeatSchedule(ctx, nil, "id"); return err }()},
		{"UpdateHeartbeatSchedule", UpdateHeartbeatSchedule(ctx, nil, HeartbeatSchedule{ID: "id"})},
		{"GetDueHeartbeatSchedules", func() error { _, err := GetDueHeartbeatSchedules(ctx, nil, now); return err }()},
		{"AdvanceHeartbeatNextRun", AdvanceHeartbeatNextRun(ctx, nil, "id", now)},
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

func TestTicker_EvaluateDueSchedules_Scenarios(t *testing.T) {
	tests := []struct {
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
		{
			name: "due heartbeat dispatched successfully",
			setupHeartbeat: &HeartbeatSchedule{
				ID:          "hb-eval-1",
				TargetID:    "123456789",
				TitlePrefix: "Heartbeat Check",
				CronExpr:    "0 12 * * *",
				Prompt:      "System Heartbeat",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "low",
			},
			dispatcherStatus:  "accepted",
			responseMessageID: "disp-hb-1",
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "completed",
		},
		{
			name: "due heartbeat skipped when thread has active turn",
			setupHeartbeat: &HeartbeatSchedule{
				ID:          "hb-eval-skipped",
				TargetID:    "123456789",
				TitlePrefix: "Heartbeat Skip",
				CronExpr:    "0 12 * * *",
				Prompt:      "System Heartbeat",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "low",
			},
			dispatcherStatus:  "skipped_active",
			evaluateNowOffset: 0,
			wantDispatched:    1,
		},
		{
			name: "due heartbeat stale over 24h skips dispatch",
			setupHeartbeat: &HeartbeatSchedule{
				ID:          "hb-eval-stale",
				TargetID:    "123456789",
				TitlePrefix: "Stale Heartbeat",
				CronExpr:    "0 12 * * *",
				Prompt:      "Stale Check",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC), // >24h overdue
				Enabled:     true,
				Effort:      "low",
			},
			evaluateNowOffset: 0,
			wantDispatched:    0,
			wantRunStatus:     "failed",
			wantRunError:      "stale heartbeat (>24h overdue)",
		},
		{
			name: "due heartbeat dispatch error records failed run",
			setupHeartbeat: &HeartbeatSchedule{
				ID:          "hb-eval-err",
				TargetID:    "123456789",
				TitlePrefix: "Heartbeat Error",
				CronExpr:    "0 12 * * *",
				Prompt:      "Error Check",
				Timezone:    "UTC",
				NextRunAt:   time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC),
				Enabled:     true,
				Effort:      "low",
			},
			dispatcherErr:     fmt.Errorf("connection refused"),
			evaluateNowOffset: 0,
			wantDispatched:    1,
			wantRunStatus:     "failed",
			wantRunError:      "connection refused",
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

			if tc.wantRunStatus != "" {
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

func TestPureHelpers_BuildHeartbeatPrompt(t *testing.T) {
	tests := []struct {
		name         string
		schedule     HeartbeatSchedule
		wantContains []string
	}{
		{
			name: "default title when empty",
			schedule: HeartbeatSchedule{
				ID:          "hb-1",
				TargetID:    "1234567890",
				TitlePrefix: "",
				Prompt:      "Check node exporter disk",
			},
			wantContains: []string{
				"Target Discord Channel ID for Escalations: 1234567890",
				`channel_id "1234567890"`,
				`title "Heartbeat Alert"`,
				"Check node exporter disk",
				`<\HEARTBEAT_INSTRUCTIONS>`,
				"do NOT scan /data or filesystem transcripts on disk",
				"jq -Rs --arg channel_id",
				"<<'EOF' | curl -sS -f -X POST http://eri.aerial/prompt",
				"Retry the POST if curl fails",
				"NEVER send a second POST once Eri returns a successful HTTP 2xx response",
			},
		},
		{
			name: "custom title prefix and trimmed fields",
			schedule: HeartbeatSchedule{
				ID:          "hb-2",
				TargetID:    " 9876543210 ",
				TitlePrefix: " Disk Space Alert ",
				Prompt:      "  Check high disk usage  ",
			},
			wantContains: []string{
				"Target Discord Channel ID for Escalations: 9876543210",
				`channel_id "9876543210"`,
				`title "Disk Space Alert"`,
				"Check high disk usage",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildHeartbeatPrompt(tc.schedule)
			for _, substr := range tc.wantContains {
				if !strings.Contains(got, substr) {
					t.Errorf("BuildHeartbeatPrompt() missing %q, got:\n%s", substr, got)
				}
			}
		})
	}
}

func TestPureHelpers_NormalizeEffort(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantResult string
		wantErr    bool
	}{
		{name: "empty defaults to low", input: "", wantResult: "low", wantErr: false},
		{name: "whitespace defaults to low", input: "   ", wantResult: "low", wantErr: false},
		{name: "low lowercase", input: "low", wantResult: "low", wantErr: false},
		{name: "low uppercase trimmed", input: " LOW ", wantResult: "low", wantErr: false},
		{name: "medium lowercase", input: "medium", wantResult: "medium", wantErr: false},
		{name: "medium uppercase", input: "MEDIUM", wantResult: "medium", wantErr: false},
		{name: "high lowercase", input: "high", wantResult: "high", wantErr: false},
		{name: "high uppercase", input: "HIGH", wantResult: "high", wantErr: false},
		{name: "invalid tier ultra", input: "ultra", wantResult: "", wantErr: true},
		{name: "invalid tier numeric", input: "10", wantResult: "", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeEffort(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NormalizeEffort(%q) error = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if got != tc.wantResult {
				t.Errorf("NormalizeEffort(%q) = %q, want %q", tc.input, got, tc.wantResult)
			}
		})
	}
}

func TestPureHelpers_IsNumericSnowflake(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "empty string", input: "", want: false},
		{name: "whitespace only", input: "   ", want: false},
		{name: "valid numeric snowflake", input: "1562847268833337377", want: true},
		{name: "valid short numeric", input: "12345", want: true},
		{name: "valid with whitespace padding", input: " 1562847268833337377 ", want: true},
		{name: "alphanumeric string", input: "12345abc", want: false},
		{name: "alpha string", input: "general", want: false},
		{name: "negative number", input: "-12345", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsNumericSnowflake(tc.input); got != tc.want {
				t.Errorf("IsNumericSnowflake(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestHeartbeatDB_Operations(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

	// 1. InsertHeartbeatSchedule
	hb1 := HeartbeatSchedule{
		ID:          "hb-op-1",
		TargetID:    "chan-1",
		TitlePrefix: "Alert 1",
		CronExpr:    "0 * * * *",
		Prompt:      "Check logs",
		Timezone:    "UTC",
		Effort:      "low",
		NextRunAt:   now.Add(1 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb1); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	// Insert with zero CreatedAt, empty Timezone, empty Effort
	hb2 := HeartbeatSchedule{
		ID:        "hb-op-2",
		TargetID:  "chan-2",
		CronExpr:  "*/15 * * * *",
		Prompt:    "Check disk",
		NextRunAt: now.Add(15 * time.Minute),
		Enabled:   true,
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb2); err != nil {
		t.Fatalf("InsertHeartbeatSchedule defaults failed: %v", err)
	}

	// 2. GetHeartbeatSchedule
	got, err := GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule(hb-op-1) failed: %v", err)
	}
	if got.ID != "hb-op-1" || got.TargetID != "chan-1" || got.Effort != "low" {
		t.Errorf("GetHeartbeatSchedule got %+v", got)
	}

	// GetHeartbeatSchedule missing
	if _, err := GetHeartbeatSchedule(ctx, db, "non-existent"); err == nil {
		t.Error("expected error for non-existent heartbeat")
	}
	if _, err := GetHeartbeatSchedule(ctx, db, "  "); err == nil {
		t.Error("expected error for empty id")
	}

	// 3. ListHeartbeatSchedules
	allHBs, err := ListHeartbeatSchedules(ctx, db, "")
	if err != nil {
		t.Fatalf("ListHeartbeatSchedules failed: %v", err)
	}
	if len(allHBs) != 2 {
		t.Errorf("ListHeartbeatSchedules all got %d items, want 2", len(allHBs))
	}

	filteredHBs, err := ListHeartbeatSchedules(ctx, db, "chan-1")
	if err != nil {
		t.Fatalf("ListHeartbeatSchedules(chan-1) failed: %v", err)
	}
	if len(filteredHBs) != 1 || filteredHBs[0].ID != "hb-op-1" {
		t.Errorf("ListHeartbeatSchedules(chan-1) got %+v, want hb-op-1", filteredHBs)
	}

	// 4. UpdateHeartbeatSchedule
	hb1.TitlePrefix = "Updated Title"
	hb1.Effort = "high"
	if err := UpdateHeartbeatSchedule(ctx, db, hb1); err != nil {
		t.Fatalf("UpdateHeartbeatSchedule failed: %v", err)
	}
	updated, err := GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule after update failed: %v", err)
	}
	if updated.TitlePrefix != "Updated Title" || updated.Effort != "high" {
		t.Errorf("updated heartbeat = %+v, want title 'Updated Title' and effort 'high'", updated)
	}

	// Update non-existent
	hbMissing := hb1
	hbMissing.ID = "missing"
	if err := UpdateHeartbeatSchedule(ctx, db, hbMissing); err == nil {
		t.Error("expected error updating missing heartbeat")
	}
	hbEmptyID := hb1
	hbEmptyID.ID = ""
	if err := UpdateHeartbeatSchedule(ctx, db, hbEmptyID); err == nil {
		t.Error("expected error updating empty id")
	}
	hbBadEffort := hb1
	hbBadEffort.Effort = "bad-tier"
	if err := UpdateHeartbeatSchedule(ctx, db, hbBadEffort); err == nil {
		t.Error("expected error updating with invalid effort")
	}

	// 5. GetDueHeartbeatSchedules
	due, err := GetDueHeartbeatSchedules(ctx, db, now.Add(20*time.Minute))
	if err != nil {
		t.Fatalf("GetDueHeartbeatSchedules failed: %v", err)
	}
	if len(due) != 1 || due[0].ID != "hb-op-2" {
		t.Errorf("GetDueHeartbeatSchedules got %+v, want only hb-op-2", due)
	}

	// 6. AdvanceHeartbeatNextRun
	nextTime := now.Add(2 * time.Hour)
	if err := AdvanceHeartbeatNextRun(ctx, db, "hb-op-1", nextTime); err != nil {
		t.Fatalf("AdvanceHeartbeatNextRun failed: %v", err)
	}
	advanced, _ := GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if !advanced.NextRunAt.Equal(nextTime) {
		t.Errorf("advanced NextRunAt = %v, want %v", advanced.NextRunAt, nextTime)
	}
	if err := AdvanceHeartbeatNextRun(ctx, db, "missing", nextTime); err == nil {
		t.Error("expected error advancing missing heartbeat")
	}
	if err := AdvanceHeartbeatNextRun(ctx, db, "  ", nextTime); err == nil {
		t.Error("expected error advancing empty id")
	}

	// 7. DeleteSchedule deleting heartbeat
	deleted, err := DeleteSchedule(ctx, db, "hb-op-1")
	if err != nil || !deleted {
		t.Fatalf("DeleteSchedule(hb-op-1) failed: deleted=%v, err=%v", deleted, err)
	}
	if _, err := GetHeartbeatSchedule(ctx, db, "hb-op-1"); err == nil {
		t.Error("expected hb-op-1 to be deleted")
	}
}

func TestHeartbeatTools_HandleScheduleHeartbeat(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	tests := []struct {
		name      string
		defaultTZ string
		args      map[string]any
		wantErr   bool
	}{
		{
			name:      "missing channel_id",
			defaultTZ: "UTC",
			args:      map[string]any{"cron_expr": "0 * * * *", "prompt": "check"},
			wantErr:   true,
		},
		{
			name:      "non-numeric snowflake channel_id",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "invalid-snowflake", "cron_expr": "0 * * * *", "prompt": "check"},
			wantErr:   true,
		},
		{
			name:      "missing cron_expr",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "prompt": "check"},
			wantErr:   true,
		},
		{
			name:      "invalid cron_expr",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "not-a-cron", "prompt": "check"},
			wantErr:   true,
		},
		{
			name:      "missing prompt",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "0 * * * *"},
			wantErr:   true,
		},
		{
			name:      "invalid effort",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "0 * * * *", "prompt": "check", "effort": "ultra"},
			wantErr:   true,
		},
		{
			name:      "success with defaults",
			defaultTZ: "UTC",
			args:      map[string]any{"channel_id": "1562847268833337377", "cron_expr": "0 0 * * *", "prompt": "Monitor system health"},
			wantErr:   false,
		},
		{
			name:      "success with explicit fields",
			defaultTZ: "America/Los_Angeles",
			args: map[string]any{
				"channel_id":   "1562847268833337378",
				"cron_expr":    "*/10 * * * *",
				"prompt":       "Check memory leaks",
				"title_prefix": "Memory Alert",
				"timezone":     "UTC",
				"effort":       "high",
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleScheduleHeartbeat(ctx, db, tc.defaultTZ, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleScheduleHeartbeat() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult content")
				}
				textContent, ok := res.Content[0].(*mcp.TextContent)
				if !ok || textContent.Text == "" {
					t.Fatalf("expected TextContent, got %+v", res.Content[0])
				}
			}
		})
	}
}

func TestHeartbeatTools_HandleUpdateHeartbeat(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	// Seed an existing heartbeat
	seedRes, err := HandleScheduleHeartbeat(ctx, db, "UTC", map[string]any{
		"channel_id": "1562847268833337377",
		"cron_expr":  "0 0 * * *",
		"prompt":     "Original Prompt",
		"effort":     "low",
	})
	if err != nil {
		t.Fatalf("failed to seed heartbeat: %v", err)
	}
	var seedMap map[string]any
	tc := seedRes.Content[0].(*mcp.TextContent)
	if err := json.Unmarshal([]byte(tc.Text), &seedMap); err != nil {
		t.Fatalf("unmarshal seed map failed: %v", err)
	}
	hbID := seedMap["schedule_id"].(string)

	tests := []struct {
		name    string
		args    map[string]any
		wantErr bool
	}{
		{
			name:    "missing id",
			args:    map[string]any{"prompt": "new prompt"},
			wantErr: true,
		},
		{
			name:    "not found id",
			args:    map[string]any{"id": "missing-uuid", "prompt": "new"},
			wantErr: true,
		},
		{
			name:    "invalid channel snowflake",
			args:    map[string]any{"id": hbID, "channel_id": "abc"},
			wantErr: true,
		},
		{
			name:    "invalid cron_expr",
			args:    map[string]any{"id": hbID, "cron_expr": "bad cron"},
			wantErr: true,
		},
		{
			name:    "invalid timezone",
			args:    map[string]any{"id": hbID, "timezone": "Invalid/Zone"},
			wantErr: true,
		},
		{
			name:    "invalid effort",
			args:    map[string]any{"id": hbID, "effort": "extreme"},
			wantErr: true,
		},
		{
			name: "valid full update",
			args: map[string]any{
				"id":           hbID,
				"channel_id":   "1562847268833337999",
				"cron_expr":    "*/30 * * * *",
				"prompt":       "Updated Prompt",
				"title_prefix": "New Prefix",
				"timezone":     "America/New_York",
				"effort":       "medium",
				"enabled":      true,
			},
			wantErr: false,
		},
		{
			name: "disable heartbeat",
			args: map[string]any{
				"id":      hbID,
				"enabled": false,
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := HandleUpdateHeartbeat(ctx, db, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleUpdateHeartbeat() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult content")
				}
			}
		})
	}
}

func TestToolHandler_ListSchedules_WithHeartbeats(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	handler := NewToolHandler(&Config{Timezone: "UTC"}, db)

	// Initially empty
	emptyOut, err := handler.ListSchedules(ctx, ListSchedulesArgs{})
	if err != nil {
		t.Fatalf("ListSchedules empty failed: %v", err)
	}
	if emptyOut.Summary != "No active schedules found." {
		t.Errorf("empty summary = %q, want 'No active schedules found.'", emptyOut.Summary)
	}

	// Add 1 recurring, 1 one-shot, and 1 heartbeat
	_, err = handler.ScheduleRecurring(ctx, ScheduleRecurringArgs{
		ChannelID:      "1562847268833337377",
		CronExpression: "0 12 * * *",
		Prompt:         "Daily Summary",
	})
	if err != nil {
		t.Fatalf("ScheduleRecurring failed: %v", err)
	}

	_, err = handler.ScheduleOnce(ctx, ScheduleOnceArgs{
		TargetID: "1562847268833337377",
		RunAt:    "1h",
		Prompt:   "One shot reminder",
	})
	if err != nil {
		t.Fatalf("ScheduleOnce failed: %v", err)
	}

	_, err = HandleScheduleHeartbeat(ctx, db, "UTC", map[string]any{
		"channel_id": "1562847268833337377",
		"cron_expr":  "*/5 * * * *",
		"prompt":     "Monitor ingress",
	})
	if err != nil {
		t.Fatalf("HandleScheduleHeartbeat failed: %v", err)
	}

	out, err := handler.ListSchedules(ctx, ListSchedulesArgs{})
	if err != nil {
		t.Fatalf("ListSchedules failed: %v", err)
	}
	if len(out.Recurring) != 1 || len(out.OneShot) != 1 || len(out.Heartbeats) != 1 {
		t.Errorf("ListSchedules count mismatch: recurring=%d, oneshot=%d, heartbeats=%d",
			len(out.Recurring), len(out.OneShot), len(out.Heartbeats))
	}
	if !strings.Contains(out.Summary, "[RECURRING]") {
		t.Errorf("summary missing [RECURRING]:\n%s", out.Summary)
	}
	if !strings.Contains(out.Summary, "[ONE_SHOT]") {
		t.Errorf("summary missing [ONE_SHOT]:\n%s", out.Summary)
	}
	if !strings.Contains(out.Summary, "[HEARTBEAT]") {
		t.Errorf("summary missing [HEARTBEAT]:\n%s", out.Summary)
	}
}

func TestTicker_EvaluateDueHeartbeats_Scenarios(t *testing.T) {
	now := time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name              string
		hbID              string
		cronExpr          string
		nextRunAt         time.Time
		dispatcherStatus  string
		dispatcherErr     error
		wantDispatched    bool
		wantSkippedRun    bool
		wantStaleRun      bool
		wantFailedRun     bool
		wantSessionPrefix string
	}{
		{
			name:              "nominal due heartbeat dispatched",
			hbID:              "hb-nominal",
			cronExpr:          "*/5 * * * *",
			nextRunAt:         now.Add(-1 * time.Minute),
			wantDispatched:    true,
			wantSessionPrefix: "heartbeat-hb-nominal",
		},
		{
			name:             "active thread turn skipped without schedule run",
			hbID:             "hb-coalesced",
			cronExpr:         "*/5 * * * *",
			nextRunAt:        now.Add(-1 * time.Minute),
			dispatcherStatus: "skipped_active",
			wantDispatched:   true,
			wantSkippedRun:   true,
		},
		{
			name:           "stale heartbeat overdue by more than 24h",
			hbID:           "hb-stale",
			cronExpr:       "0 * * * *",
			nextRunAt:      now.Add(-25 * time.Hour),
			wantDispatched: false,
			wantStaleRun:   true,
		},
		{
			name:           "dispatch failure recorded in schedule_runs",
			hbID:           "hb-fail",
			cronExpr:       "*/10 * * * *",
			nextRunAt:      now.Add(-2 * time.Minute),
			dispatcherErr:  fmt.Errorf("brain down"),
			wantDispatched: true,
			wantFailedRun:  true,
		},
		{
			name:           "invalid cron expression uses 24h fallback",
			hbID:           "hb-bad-cron",
			cronExpr:       "invalid cron string",
			nextRunAt:      now.Add(-1 * time.Minute),
			wantDispatched: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			ctx := context.Background()

			hb := HeartbeatSchedule{
				ID:          tc.hbID,
				TargetID:    "1562847268833337377",
				TitlePrefix: "Test HB",
				CronExpr:    tc.cronExpr,
				Prompt:      "Raw monitoring prompt for " + tc.hbID,
				Timezone:    "UTC",
				Effort:      "low",
				NextRunAt:   tc.nextRunAt,
				Enabled:     true,
				CreatedAt:   now.Add(-48 * time.Hour),
			}
			if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
				t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
			}

			dispStatus := "ok"
			if tc.dispatcherStatus != "" {
				dispStatus = tc.dispatcherStatus
			}
			mock := &mockDispatcher{
				response:         PromptDispatchResponse{Status: dispStatus, MessageID: "msg-dispatched"},
				err:              tc.dispatcherErr,
				dispatchResponse: DispatchResponse{Status: dispStatus, DispatchID: "msg-dispatched", MessageID: "msg-dispatched"},
				dispatchErr:      tc.dispatcherErr,
			}
			ticker := NewTicker(db, mock, 30*time.Second)

			ticker.EvaluateDueSchedules(ctx, now)

			if tc.wantDispatched {
				if len(mock.dispatchedRequests) == 0 {
					t.Errorf("expected dispatcher to be called for %s", tc.hbID)
				}
				req := mock.dispatchedRequests[len(mock.dispatchedRequests)-1]
				if !strings.Contains(req.Prompt, "Raw monitoring prompt for "+tc.hbID) {
					t.Errorf("dispatcher prompt missing raw prompt:\n%s", req.Prompt)
				}
				if !strings.Contains(req.Prompt, `<\HEARTBEAT_INSTRUCTIONS>`) {
					t.Errorf("dispatcher prompt missing <\\HEARTBEAT_INSTRUCTIONS> wrapper:\n%s", req.Prompt)
				}
				if req.SessionID != "heartbeat-"+tc.hbID {
					t.Errorf("req.SessionID = %q, want 'heartbeat-%s'", req.SessionID, tc.hbID)
				}
				if req.Source != "heartbeat" {
					t.Errorf("req.Source = %q, want 'heartbeat'", req.Source)
				}
				if !req.SkipIfActive {
					t.Errorf("expected req.SkipIfActive = true")
				}
			} else {
				if len(mock.dispatchedRequests) > 0 {
					t.Errorf("dispatcher should NOT be called for %s, called %d times", tc.hbID, len(mock.dispatchedRequests))
				}
			}

			// Verify schedule_runs table
			var runCount int
			var runStatus, runPrompt, runType string
			_ = db.QueryRow("SELECT COUNT(*), COALESCE(MAX(status), ''), COALESCE(MAX(prompt), ''), COALESCE(MAX(schedule_type), '') FROM schedule_runs WHERE schedule_id = ?", tc.hbID).Scan(&runCount, &runStatus, &runPrompt, &runType)

			if tc.wantSkippedRun {
				if runCount != 0 {
					t.Errorf("expected 0 runs for skipped active heartbeat %s, got count=%d", tc.hbID, runCount)
				}
			}
			if tc.wantStaleRun {
				if runCount == 0 || runStatus != "failed" {
					t.Errorf("expected stale failed run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
				}
			}
			if tc.wantFailedRun {
				if runCount == 0 || runStatus != "failed" {
					t.Errorf("expected failed run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
				}
			}
			if tc.wantDispatched && tc.dispatcherErr == nil && !tc.wantSkippedRun {
				if runCount == 0 || runStatus != "completed" {
					t.Errorf("expected completed run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
				}
			}
			if runCount > 0 {
				if runType != "heartbeat" {
					t.Errorf("expected schedule_type='heartbeat', got %s", runType)
				}
				// Verify raw prompt invariant: NEVER the wrapped <\HEARTBEAT_INSTRUCTIONS> string!
				if strings.Contains(runPrompt, `HEARTBEAT_INSTRUCTIONS`) {
					t.Errorf("schedule_runs.prompt must NOT contain HEARTBEAT_INSTRUCTIONS, got:\n%s", runPrompt)
				}
			}

			// Verify next_run_at advanced past tc.nextRunAt
			after, err := GetHeartbeatSchedule(ctx, db, tc.hbID)
			if err != nil {
				t.Fatalf("failed to reload heartbeat %s: %v", tc.hbID, err)
			}
			if !after.NextRunAt.After(tc.nextRunAt) {
				t.Errorf("next_run_at was not advanced: before=%v, after=%v", tc.nextRunAt, after.NextRunAt)
			}
		})
	}
}

func TestPureHelpers_MapArgExtractors(t *testing.T) {
	stringTests := []struct {
		name string
		args map[string]any
		keys []string
		want string
	}{
		{name: "nil map", args: nil, keys: []string{"a"}, want: ""},
		{name: "missing key", args: map[string]any{"b": "val"}, keys: []string{"a"}, want: ""},
		{name: "empty string", args: map[string]any{"a": "   "}, keys: []string{"a"}, want: ""},
		{name: "non string", args: map[string]any{"a": 123}, keys: []string{"a"}, want: ""},
		{name: "valid string trimmed", args: map[string]any{"a": "  hello  "}, keys: []string{"a"}, want: "hello"},
		{name: "fallback key", args: map[string]any{"b": "fallback"}, keys: []string{"a", "b"}, want: "fallback"},
	}
	for _, tc := range stringTests {
		t.Run("string_"+tc.name, func(t *testing.T) {
			if got := getMapString(tc.args, tc.keys...); got != tc.want {
				t.Errorf("getMapString() = %q, want %q", got, tc.want)
			}
		})
	}

	boolTests := []struct {
		name      string
		args      map[string]any
		key       string
		wantVal   bool
		wantFound bool
	}{
		{name: "nil map", args: nil, key: "k", wantVal: false, wantFound: false},
		{name: "missing key", args: map[string]any{"other": true}, key: "k", wantVal: false, wantFound: false},
		{name: "bool true", args: map[string]any{"k": true}, key: "k", wantVal: true, wantFound: true},
		{name: "bool false", args: map[string]any{"k": false}, key: "k", wantVal: false, wantFound: true},
		{name: "string true", args: map[string]any{"k": "true"}, key: "k", wantVal: true, wantFound: true},
		{name: "string 1", args: map[string]any{"k": "1"}, key: "k", wantVal: true, wantFound: true},
		{name: "string false", args: map[string]any{"k": "false"}, key: "k", wantVal: false, wantFound: true},
		{name: "string 0", args: map[string]any{"k": "0"}, key: "k", wantVal: false, wantFound: true},
		{name: "string other", args: map[string]any{"k": "maybe"}, key: "k", wantVal: false, wantFound: false},
		{name: "non-bool type", args: map[string]any{"k": 123}, key: "k", wantVal: false, wantFound: false},
	}
	for _, tc := range boolTests {
		t.Run("bool_"+tc.name, func(t *testing.T) {
			valPtr, found := getMapBool(tc.args, tc.key)
			if found != tc.wantFound {
				t.Fatalf("getMapBool() found = %v, want %v", found, tc.wantFound)
			}
			if found {
				if valPtr == nil || *valPtr != tc.wantVal {
					t.Errorf("getMapBool() val = %v, want %v", valPtr, tc.wantVal)
				}
			}
		})
	}
}

func TestPureHelpers_ValidateEventWatchSpec(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		target        string
		eventType     string
		extraSources  []string
		wantSource    string
		wantTarget    string
		wantEventType string
		wantErr       bool
	}{
		// empty target
		{"empty target", "vanadis_build", "", "completed", nil, "", "", "", true},
		// vanadis_build
		{"vanadis_build completed", "vanadis_build", "build-123-abc", "completed", nil, "vanadis_build", "build-123-abc", "completed", false},
		{"vanadis_build failed", "VANADIS_BUILD", "build-456", "FAILED", nil, "vanadis_build", "build-456", "failed", false},
		{"vanadis_build invalid event_type", "vanadis_build", "build-123", "running", nil, "", "", "", true},
		{"vanadis_build target with chunk rejected", "vanadis_build", "build-123-chunk-1", "completed", nil, "", "", "", true},
		{"vanadis_build invalid target regex", "vanadis_build", "invalid_build", "completed", nil, "", "", "", true},
		// vanadis_chunk
		{"vanadis_chunk merged", "vanadis_chunk", "build-123-chunk-1", "merged", nil, "vanadis_chunk", "build-123-chunk-1", "merged", false},
		{"vanadis_chunk failed", "vanadis_chunk", "anyprefix-chunk-99", "failed", nil, "vanadis_chunk", "anyprefix-chunk-99", "failed", false},
		{"vanadis_chunk invalid event_type", "vanadis_chunk", "build-123-chunk-1", "completed", nil, "", "", "", true},
		{"vanadis_chunk invalid target", "vanadis_chunk", "build-123", "merged", nil, "", "", "", true},
		// hangar
		{"hangar bare repo sync_success", "hangar", "aerial", "sync_success", nil, "hangar", "azylman/aerial", "sync_success", false},
		{"hangar full repo sync_failed", "hangar", "azylman/aerial-config", "sync_failed", nil, "hangar", "azylman/aerial-config", "sync_failed", false},
		{"hangar invalid event_type", "hangar", "aerial", "deployed", nil, "", "", "", true},
		{"hangar unmanaged repo", "hangar", "other/repo", "sync_success", nil, "", "", "", true},
		// github
		{"github repo only pr_merged", "github", "aerial", "pr_merged", nil, "github", "azylman/aerial", "pr_merged", false},
		{"github repo with branch ci_passed", "github", "azylman/aerial:main", "ci_passed", nil, "github", "azylman/aerial:main", "ci_passed", false},
		{"github refs heads stripped ci_failed", "github", "aerial:refs/heads/feature/test", "ci_failed", nil, "github", "azylman/aerial:feature/test", "ci_failed", false},
		{"github PR number with hash rejected", "github", "aerial:#123", "ci_passed", nil, "", "", "", true},
		{"github PR number numeric rejected", "github", "aerial:123", "ci_passed", nil, "", "", "", true},
		{"github 40-char SHA rejected", "github", "aerial:0123456789abcdef0123456789abcdef01234567", "ci_passed", nil, "", "", "", true},
		{"github empty branch rejected", "github", "aerial:", "ci_passed", nil, "", "", "", true},
		{"github unmanaged repo rejected", "github", "unmanaged/repo:main", "ci_passed", nil, "", "", "", true},
		{"github invalid event_type", "github", "aerial", "completed", nil, "", "", "", true},
		// nomad
		{"nomad deploy_success", "nomad", "aerial-brain", "deploy_success", nil, "nomad", "aerial-brain", "deploy_success", false},
		{"nomad uppercase normalized deploy_failed", "nomad", "AERIAL-POSTGRES", "deploy_failed", nil, "nomad", "aerial-postgres", "deploy_failed", false},
		{"nomad invalid event_type", "nomad", "aerial-brain", "started", nil, "", "", "", true},
		{"nomad invalid target characters", "nomad", "aerial_brain", "deploy_success", nil, "", "", "", true},
		// unknown source vs configured extra_event_sources
		{"unknown source without extraSources", "custom_iot", "device.washer", "idle", nil, "", "", "", true},
		{"configured extra source valid", "Custom_IoT", "device.washer", "IDLE", []string{"custom_iot"}, "custom_iot", "device.washer", "idle", false},
		{"configured extra source empty event_type", "custom_iot", "device.washer", "   ", []string{"custom_iot"}, "", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, target, ev, err := ValidateEventWatchSpec(tc.source, tc.target, tc.eventType, tc.extraSources...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateEventWatchSpec() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if s != tc.wantSource || target != tc.wantTarget || ev != tc.wantEventType {
					t.Errorf("got (%q, %q, %q), want (%q, %q, %q)", s, target, ev, tc.wantSource, tc.wantTarget, tc.wantEventType)
				}
			}
		})
	}
}

func TestPureHelpers_FormatEventTriggerPrompt(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		prompt  string
		wantStr string
	}{
		{"both header and prompt present", "Event trigger: test", "Proceed with task", "Event trigger: test\n\nProceed with task"},
		{"empty header returns prompt", "   ", "Proceed with task", "Proceed with task"},
		{"empty prompt returns header", "Event trigger: test", "   ", "Event trigger: test"},
		{"both empty returns empty", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatEventTriggerPrompt(tc.header, tc.prompt); got != tc.wantStr {
				t.Fatalf("FormatEventTriggerPrompt() = %q, want %q", got, tc.wantStr)
			}
		})
	}
}

func TestDB_EventTriggers(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	mockBuilds := map[string]string{
		"b-running":   "running",
		"b-completed": "completed",
		"b-failed":    "failed",
		"b-cancelled": "cancelled",
		"b-canceled":  "canceled",
	}
	mockChunks := map[string]string{
		"c-pending":   "pending",
		"c-merged":    "merged",
		"c-completed": "completed",
		"c-failed":    "failed",
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/builds/") {
			id := strings.TrimPrefix(r.URL.Path, "/builds/")
			if id == "b-500" {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if id == "b-badjson" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("{bad-json"))
				return
			}
			if st, ok := mockBuilds[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": st}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/chunks/") {
			id := strings.TrimPrefix(r.URL.Path, "/chunks/")
			if id == "c-500" {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if id == "c-badjson" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("{bad-json"))
				return
			}
			if st, ok := mockChunks[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"chunk": map[string]string{"status": st}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	oldVanadis := GetVanadisURL()
	SetVanadisURL(ts.URL)
	defer SetVanadisURL(oldVanadis)

	validateTests := []struct {
		name    string
		source  string
		target  string
		wantErr bool
	}{
		{"vanadis_build running", "vanadis_build", "b-running", false},
		{"vanadis_build completed", "vanadis_build", "b-completed", true},
		{"vanadis_build failed", "vanadis_build", "b-failed", true},
		{"vanadis_build cancelled", "vanadis_build", "b-cancelled", true},
		{"vanadis_build canceled", "vanadis_build", "b-canceled", true},
		{"vanadis_build not found", "vanadis_build", "b-missing", true},
		{"vanadis_build 500 error", "vanadis_build", "b-500", true},
		{"vanadis_build bad json", "vanadis_build", "b-badjson", true},
		{"vanadis_chunk pending", "vanadis_chunk", "c-pending", false},
		{"vanadis_chunk merged", "vanadis_chunk", "c-merged", true},
		{"vanadis_chunk completed", "vanadis_chunk", "c-completed", true},
		{"vanadis_chunk failed", "vanadis_chunk", "c-failed", true},
		{"vanadis_chunk not found", "vanadis_chunk", "c-missing", true},
		{"vanadis_chunk 500 error", "vanadis_chunk", "c-500", true},
		{"vanadis_chunk bad json", "vanadis_chunk", "c-badjson", true},
		{"empty target", "vanadis_build", "   ", true},
		{"other source hangar", "hangar", "azylman/aerial", false},
		{"other source github", "github", "azylman/aerial:main", false},
		{"other source nomad", "nomad", "aerial-brain", false},
	}
	for _, tc := range validateTests {
		t.Run("validate_"+tc.name, func(t *testing.T) {
			err := ValidateLiveEventTarget(ctx, db, tc.source, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateLiveEventTarget(%q, %q) err = %v, wantErr = %v", tc.source, tc.target, err, tc.wantErr)
			}
		})
	}

	// Insert, List, Delete tests
	trig1 := EventTrigger{
		ID:        "et-1",
		ChannelID: "111111111111111111",
		Source:    "hangar",
		Target:    "azylman/aerial",
		EventType: "sync_success",
		Prompt:    "Hangar sync finished",
		Effort:    "low",
		CreatedAt: time.Now().UTC(),
	}
	trig2 := EventTrigger{
		ID:        "et-2",
		ChannelID: "222222222222222222",
		Source:    "vanadis_build",
		Target:    "b-running",
		EventType: "completed",
		Prompt:    "Build completed",
		CreatedAt: time.Now().UTC(),
	}
	trig3 := EventTrigger{
		ID:        "et-3",
		ChannelID: "111111111111111111",
		Source:    "nomad",
		Target:    "aerial-brain",
		EventType: "deploy_success",
		Prompt:    "Brain deployed",
		CreatedAt: time.Now().UTC(),
	}
	trig4 := EventTrigger{
		ID:        "et-4",
		ChannelID: "111111111111111111",
		Source:    "nomad",
		Target:    "aerial-brain",
		EventType: "deploy_success",
		Prompt:    "Brain deployed with defaults",
		Effort:    "invalid-effort",
	}
	trig5 := EventTrigger{
		ID:         "et-5",
		ChannelID:  "111111111111111111",
		Source:     "camera",
		Target:     "garage",
		EventType:  "person_arrived",
		Prompt:     "Standing camera watch garage",
		Effort:     "medium",
		CreatedAt:  time.Now().UTC(),
		Persistent: true,
	}
	trig6 := EventTrigger{
		ID:         "et-6",
		ChannelID:  "111111111111111111",
		Source:     "camera",
		Target:     "garage",
		EventType:  "person_arrived",
		Prompt:     "One-shot camera watch",
		Effort:     "low",
		CreatedAt:  time.Now().UTC(),
		Persistent: false,
	}

	if err := InsertEventTrigger(ctx, db, trig1); err != nil {
		t.Fatalf("InsertEventTrigger 1 failed: %v", err)
	}
	if err := InsertEventTrigger(ctx, db, trig2); err != nil {
		t.Fatalf("InsertEventTrigger 2 failed: %v", err)
	}
	if err := InsertEventTrigger(ctx, db, trig3); err != nil {
		t.Fatalf("InsertEventTrigger 3 failed: %v", err)
	}
	if err := InsertEventTrigger(ctx, db, trig4); err != nil {
		t.Fatalf("InsertEventTrigger 4 failed: %v", err)
	}
	if err := InsertEventTrigger(ctx, db, trig5); err != nil {
		t.Fatalf("InsertEventTrigger 5 failed: %v", err)
	}
	if err := InsertEventTrigger(ctx, db, trig6); err != nil {
		t.Fatalf("InsertEventTrigger 6 failed: %v", err)
	}

	listTests := []struct {
		name      string
		channelID string
		wantCount int
	}{
		{"list all", "", 6},
		{"list channel 1", "111111111111111111", 5},
		{"list channel 2", "222222222222222222", 1},
		{"list non-existent channel", "999999999999999999", 0},
	}
	for _, tc := range listTests {
		t.Run("list_"+tc.name, func(t *testing.T) {
			res, err := ListEventTriggers(ctx, db, tc.channelID)
			if err != nil {
				t.Fatalf("ListEventTriggers failed: %v", err)
			}
			if len(res) != tc.wantCount {
				t.Errorf("ListEventTriggers count = %d, want %d", len(res), tc.wantCount)
			}
		})
	}

	// Delete via DeleteEventTrigger
	delTests := []struct {
		name       string
		id         string
		wantDelete bool
		wantErr    bool
	}{
		{"delete existing", "et-1", true, false},
		{"delete already deleted", "et-1", false, false},
		{"delete non-existent", "et-missing", false, false},
		{"delete empty id", "", false, true},
	}
	for _, tc := range delTests {
		t.Run("delete_"+tc.name, func(t *testing.T) {
			deleted, err := DeleteEventTrigger(ctx, db, tc.id)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeleteEventTrigger(%q) err = %v, wantErr = %v", tc.id, err, tc.wantErr)
			}
			if deleted != tc.wantDelete {
				t.Errorf("DeleteEventTrigger(%q) deleted = %v, want %v", tc.id, deleted, tc.wantDelete)
			}
		})
	}

	// Verify DeleteSchedule deletes from event_triggers
	delSchedTests := []struct {
		name       string
		id         string
		wantDelete bool
	}{
		{"delete trigger via DeleteSchedule", "et-2", true},
		{"delete non-existent via DeleteSchedule", "et-missing-2", false},
	}
	for _, tc := range delSchedTests {
		t.Run("delete_sched_"+tc.name, func(t *testing.T) {
			deleted, err := DeleteSchedule(ctx, db, tc.id)
			if err != nil {
				t.Fatalf("DeleteSchedule failed: %v", err)
			}
			if deleted != tc.wantDelete {
				t.Errorf("DeleteSchedule deleted = %v, want %v", deleted, tc.wantDelete)
			}
		})
	}

	// Verify ClaimEventTriggers persistent vs non-persistent retention
	claimTests := []struct {
		name      string
		source    string
		targets   []string
		eventType string
		wantCount int
		wantIDs   []string
	}{
		{"first claim returns both persistent and non-persistent", "camera", []string{"garage"}, "person_arrived", 2, []string{"et-5", "et-6"}},
		{"second claim retains persistent and deletes non-persistent", "camera", []string{"garage"}, "person_arrived", 1, []string{"et-5"}},
		{"third claim still retains persistent", "camera", []string{"garage"}, "person_arrived", 1, []string{"et-5"}},
	}
	for _, tc := range claimTests {
		t.Run("claim_"+tc.name, func(t *testing.T) {
			claimed, err := ClaimEventTriggers(ctx, db, tc.source, tc.targets, tc.eventType)
			if err != nil {
				t.Fatalf("ClaimEventTriggers failed: %v", err)
			}
			if len(claimed) != tc.wantCount {
				t.Fatalf("ClaimEventTriggers count = %d, want %d", len(claimed), tc.wantCount)
			}
			for i, id := range tc.wantIDs {
				if claimed[i].ID != id {
					t.Errorf("claimed[%d].ID = %s, want %s", i, claimed[i].ID, id)
				}
			}
		})
	}
}

func TestEventWatchTools(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	mockBuilds := map[string]string{
		"build-running-1": "running",
		"build-term-1":    "completed",
	}
	mockChunks := map[string]string{
		"chunk-pending-1":         "pending",
		"chunk-term-1":            "merged",
		"build-running-1-chunk-1": "pending",
	}
	var toctouCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/builds/") {
			id := strings.TrimPrefix(r.URL.Path, "/builds/")
			if id == "build-toctou-1" {
				toctouCalls++
				if toctouCalls == 1 {
					httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": "running"}})
					return
				}
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": "completed"}})
				return
			}
			if st, ok := mockBuilds[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": map[string]string{"status": st}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/chunks/") {
			id := strings.TrimPrefix(r.URL.Path, "/chunks/")
			if st, ok := mockChunks[id]; ok {
				httpkit.WriteJSON(w, http.StatusOK, map[string]any{"chunk": map[string]string{"status": st}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	oldVanadis := GetVanadisURL()
	SetVanadisURL(ts.URL)
	defer SetVanadisURL(oldVanadis)

	watchTests := []struct {
		name         string
		args         map[string]any
		extraSources []string
		wantErr      bool
	}{
		{
			name:    "missing channel_id",
			args:    map[string]any{"source": "hangar", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "non-numeric snowflake channel_id",
			args:    map[string]any{"channel_id": "bad-id", "source": "hangar", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "missing prompt",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "hangar", "target": "aerial", "event_type": "sync_success"},
			wantErr: true,
		},
		{
			name:    "invalid effort",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "hangar", "target": "aerial", "event_type": "sync_success", "prompt": "ok", "effort": "extreme"},
			wantErr: true,
		},
		{
			name:    "invalid spec",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "unknown", "target": "aerial", "event_type": "sync_success", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "live validation failure build not found",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "vanadis_build", "target": "build-not-found", "event_type": "completed", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "live validation failure build in terminal state",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "vanadis_build", "target": "build-term-1", "event_type": "completed", "prompt": "ok"},
			wantErr: true,
		},
		{
			name:    "success with defaults",
			args:    map[string]any{"channel_id": "1562847268833337377", "source": "hangar", "target": "aerial", "event_type": "sync_success", "prompt": "deploy ready"},
			wantErr: false,
		},
		{
			name: "success with explicit fields and vanadis chunk",
			args: map[string]any{
				"channel_id": "1562847268833337378",
				"source":     "vanadis_chunk",
				"target":     "build-running-1-chunk-1",
				"event_type": "merged",
				"prompt":     "chunk 1 merged",
				"effort":     "high",
			},
			wantErr: false,
		},
		{
			name: "success with extra source and persistent true",
			args: map[string]any{
				"channel_id": "1562847268833337378",
				"source":     "camera",
				"target":     "garage",
				"event_type": "person_arrived",
				"prompt":     "garage person detected",
				"persistent": true,
			},
			extraSources: []string{"camera"},
			wantErr:      false,
		},
	}

	for _, tc := range watchTests {
		t.Run("watch_"+tc.name, func(t *testing.T) {
			res, err := HandleWatchEvent(ctx, db, tc.args, tc.extraSources...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleWatchEvent() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult")
				}
				if wantPers, ok := tc.args["persistent"].(bool); ok && wantPers {
					txt, isTxt := res.Content[0].(*mcp.TextContent)
					if !isTxt || !strings.Contains(txt.Text, `"persistent":true`) {
						t.Errorf("expected persistent:true in output, got %v", res.Content[0])
					}
				}
			}
		})
	}

	// Test TOCTOU guard
	toctouTests := []struct {
		name       string
		setupBuild string
		args       map[string]any
		wantErr    bool
	}{
		{
			name:       "trigger deletes inserted row on terminal race",
			setupBuild: "build-toctou-1",
			args: map[string]any{
				"channel_id": "1562847268833337377",
				"source":     "vanadis_build",
				"target":     "build-toctou-1",
				"event_type": "completed",
				"prompt":     "racing build",
			},
			wantErr: true,
		},
	}

	for _, tc := range toctouTests {
		t.Run("toctou_"+tc.name, func(t *testing.T) {
			_, err := HandleWatchEvent(ctx, db, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleWatchEvent TOCTOU err = %v, wantErr = %v", err, tc.wantErr)
			}
			// Verify that event_triggers contains no row for this target (it was rolled back / deleted)
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_triggers WHERE target = ?", tc.setupBuild).Scan(&count); err != nil {
				t.Fatalf("failed to count event triggers: %v", err)
			}
			if count != 0 {
				t.Errorf("expected 0 triggers remaining after TOCTOU cleanup, got %d", count)
			}
		})
	}

	// Test HandleListEventWatches
	listTests := []struct {
		name        string
		args        map[string]any
		wantAtLeast int
	}{
		{"list all watches", map[string]any{}, 2},
		{"list filtered channel", map[string]any{"channel_id": "1562847268833337377"}, 1},
		{"list filtered target_id alias", map[string]any{"target_id": "1562847268833337378"}, 1},
		{"list empty channel", map[string]any{"channel_id": "999999999999999999"}, 0},
	}
	for _, tc := range listTests {
		t.Run("list_"+tc.name, func(t *testing.T) {
			res, err := HandleListEventWatches(ctx, db, tc.args)
			if err != nil {
				t.Fatalf("HandleListEventWatches failed: %v", err)
			}
			if res == nil || len(res.Content) == 0 {
				t.Fatalf("expected non-empty CallToolResult")
			}
			text, ok := res.Content[0].(*mcp.TextContent)
			if !ok {
				t.Fatalf("expected *mcp.TextContent")
			}
			var out struct {
				EventWatches []EventTrigger `json:"event_watches"`
				Count        int            `json:"count"`
			}
			if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
				t.Fatalf("failed to unmarshal output: %v", err)
			}
			if len(out.EventWatches) < tc.wantAtLeast {
				t.Errorf("got %d event watches, want at least %d", len(out.EventWatches), tc.wantAtLeast)
			}
		})
	}

	// Test HandleCancelEventWatch
	// Seed 3 event triggers to test different cancel keys
	_ = InsertEventTrigger(ctx, db, EventTrigger{ID: "watch-key-1", ChannelID: "1562847268833337377", Source: "hangar", Target: "azylman/aerial", EventType: "sync_success", Prompt: "p1"})
	_ = InsertEventTrigger(ctx, db, EventTrigger{ID: "watch-key-2", ChannelID: "1562847268833337377", Source: "hangar", Target: "azylman/aerial", EventType: "sync_success", Prompt: "p2"})
	_ = InsertEventTrigger(ctx, db, EventTrigger{ID: "watch-key-3", ChannelID: "1562847268833337377", Source: "hangar", Target: "azylman/aerial", EventType: "sync_success", Prompt: "p3"})

	cancelTests := []struct {
		name    string
		args    map[string]any
		wantErr bool
	}{
		{"missing watch_id", map[string]any{}, true},
		{"non-existent watch_id", map[string]any{"watch_id": "not-found-id"}, true},
		{"cancel via watch_id", map[string]any{"watch_id": "watch-key-1"}, false},
		{"cancel via id alias", map[string]any{"id": "watch-key-2"}, false},
		{"cancel via schedule_id alias", map[string]any{"schedule_id": "watch-key-3"}, false},
	}
	for _, tc := range cancelTests {
		t.Run("cancel_"+tc.name, func(t *testing.T) {
			res, err := HandleCancelEventWatch(ctx, db, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleCancelEventWatch err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if res == nil || len(res.Content) == 0 {
					t.Fatalf("expected non-empty CallToolResult")
				}
			}
		})
	}
}

func TestVanadisURL_Precedence(t *testing.T) {
	oldVanadis := GetVanadisURL()
	defer SetVanadisURL(oldVanadis)

	tests := []struct {
		name    string
		setURL  string
		envURL  string
		wantURL string
	}{
		{"explicit setURL takes precedence over env", "http://explicit.vanadis", "http://env.vanadis", "http://explicit.vanadis"},
		{"fallback to env when setURL empty", "", "http://env.vanadis", "http://env.vanadis"},
		{"trailing slash stripped from explicit", "http://explicit.vanadis/api/", "", "http://explicit.vanadis/api"},
		{"trailing slash stripped from env", "", "http://env.vanadis/api/", "http://env.vanadis/api"},
		{"default fallback when both empty", "", "", "http://vanadis.aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("VANADIS_URL", tc.envURL)
			SetVanadisURL(tc.setURL)
			got := GetVanadisURL()
			if got != tc.wantURL {
				t.Errorf("GetVanadisURL() = %q, want %q", got, tc.wantURL)
			}
		})
	}
}
