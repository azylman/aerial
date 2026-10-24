package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
	}()

	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(testCfg, []byte("port: \"4005\"\ntimezone: \"America/Los_Angeles\"\n"), 0o644)
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	runCalled := false
	runAppFn = func(ctx context.Context, cfg *Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn called")
	}

	exitCalled := false
	exitFn = func(string, ...interface{}) { exitCalled = true }
	runAppFn = func(ctx context.Context, cfg *Config) error { return http.ErrServerClosed }
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	runAppFn = func(ctx context.Context, cfg *Config) error { return context.DeadlineExceeded }
	main()
	if !exitCalled {
		t.Error("expected exitFn on server error")
	}

	exitCalled = false
	configPath = filepath.Join(tmpDir, "nonexistent.yaml")
	t.Setenv("CONFIG_PATH", configPath)
	main()
	if !exitCalled {
		t.Error("expected exitFn on config load error")
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tempDB := filepath.Join(t.TempDir(), "runapp.db")

	errCh := make(chan error, 1)
	go func() { errCh <- RunApp(ctx, &Config{Port: "59483", DatabaseURL: tempDB}) }()

	var resp *http.Response
	for start := time.Now(); time.Since(start) < 3*time.Second; {
		var err error
		if resp, err = http.Get("http://127.0.0.1:59483/health"); err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		select {
		case <-time.After(20 * time.Millisecond):
		}
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health check failed, status: %v", resp)
	}
	_ = resp.Body.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from RunApp shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RunApp shutdown")
	}

	origMax, origBase := postgresMaxAttempts, postgresRetryBase
	postgresMaxAttempts, postgresRetryBase = 1, 1*time.Millisecond
	defer func() { postgresMaxAttempts, postgresRetryBase = origMax, origBase }()

	for _, cfg := range []*Config{
		{Port: "9999", DatabaseURL: "postgres://invalid:pass@127.0.0.1:59996/db?sslmode=disable"},
		nil,
		{Port: "", DatabaseURL: ":memory:"},
		{Port: "-1", DatabaseURL: ":memory:"},
	} {
		if err := RunApp(context.Background(), cfg); err == nil {
			t.Errorf("expected error running app with %v", cfg)
		}
	}
}

func TestCalculateNextCronRun(t *testing.T) {
	baseTime := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		expr    string
		tz      string
		want    time.Time
		wantErr bool
	}{
		{"friday 20:00 utc", "0 20 * * 5", "UTC", time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC), false},
		{"weekly descriptor utc", "@weekly", "UTC", time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC), false},
		{"9am los angeles", "0 9 * * *", "America/Los_Angeles", time.Date(2026, 8, 28, 16, 0, 0, 0, time.UTC), false},
		{"9am new york", "0 9 * * *", "America/New_York", time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC), false},
		{"empty tz error", "0 9 * * *", "", time.Time{}, true},
		{"invalid tz error", "0 9 * * *", "Invalid/Timezone", time.Time{}, true},
		{"invalid cron error", "not a cron expr", "UTC", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CalculateNextCronRun(tc.expr, tc.tz, baseTime)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CalculateNextCronRun() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && !got.Equal(tc.want) {
				t.Fatalf("CalculateNextCronRun() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseRunAtWithTimezone(t *testing.T) {
	baseTime := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		input   string
		tz      string
		want    time.Time
		wantErr bool
	}{
		{"relative 30s", "30s", "UTC", baseTime.Add(30 * time.Second), false},
		{"relative 45 seconds", "45 seconds", "UTC", baseTime.Add(45 * time.Second), false},
		{"relative 15m", "15m", "UTC", baseTime.Add(15 * time.Minute), false},
		{"relative 2h", "2h", "UTC", baseTime.Add(2 * time.Hour), false},
		{"relative 1d", "1d", "UTC", baseTime.Add(24 * time.Hour), false},
		{"relative 1w", "1w", "UTC", baseTime.Add(7 * 24 * time.Hour), false},
		{"duration 1h30m", "1h30m", "UTC", baseTime.Add(90 * time.Minute), false},
		{"rfc3339 utc", "2026-08-28T21:00:00Z", "UTC", time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC), false},
		{"rfc3339nano utc", "2026-08-28T21:00:00.123456789Z", "UTC", time.Date(2026, 8, 28, 21, 0, 0, 123456789, time.UTC), false},
		{"local datetime LA", "2026-08-28 15:00:00", "America/Los_Angeles", time.Date(2026, 8, 28, 22, 0, 0, 0, time.UTC), false},
		{"local datetime NY", "2026-08-28 15:00:00", "America/New_York", time.Date(2026, 8, 28, 19, 0, 0, 0, time.UTC), false},
		{"local datetime Tokyo", "2026-08-28 15:00:00", "Asia/Tokyo", time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC), false},
		{"local iso without zone", "2026-08-28T21:00", "America/Los_Angeles", time.Date(2026, 8, 29, 4, 0, 0, 0, time.UTC), false},
		{"local space without seconds", "2026-08-28 21:00", "America/Los_Angeles", time.Date(2026, 8, 29, 4, 0, 0, 0, time.UTC), false},
		{"local date only", "2026-08-28", "America/Los_Angeles", time.Date(2026, 8, 28, 7, 0, 0, 0, time.UTC), false},
		{"empty input", "", "UTC", time.Time{}, true},
		{"invalid duration", "invalid duration", "UTC", time.Time{}, true},
		{"overflow duration", "99999999999999999999999999999999999999999999999s", "UTC", time.Time{}, true},
		{"empty timezone", "15m", "", time.Time{}, true},
		{"invalid timezone", "15m", "Invalid/Timezone", time.Time{}, true},
		{"unrecognized format", "not-a-valid-date", "UTC", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRunAtWithTimezone(tc.input, tc.tz, baseTime)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseRunAtWithTimezone(%q, %q) err = %v, wantErr = %v", tc.input, tc.tz, err, tc.wantErr)
			}
			if !tc.wantErr && !got.Equal(tc.want) {
				t.Fatalf("ParseRunAtWithTimezone(%q, %q) = %v, want %v", tc.input, tc.tz, got, tc.want)
			}
		})
	}
}

func TestResolveTimezone_Table(t *testing.T) {
	tests := []struct {
		name     string
		tz       string
		fallback string
		want     string
	}{
		{"explicit timezone", "America/New_York", "UTC", "America/New_York"},
		{"fallback timezone", "", "America/Chicago", "America/Chicago"},
		{"whitespace explicit timezone", "  America/Denver  ", "UTC", "America/Denver"},
		{"whitespace fallback timezone", "", "  UTC  ", "UTC"},
		{"default timezone", "", "", DefaultTimezone},
		{"whitespace both default", "   ", "   ", DefaultTimezone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveTimezone(tc.tz, tc.fallback)
			if got != tc.want {
				t.Errorf("resolveTimezone(%q, %q) = %q, want %q", tc.tz, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestToolHandler_ScheduleAndList(t *testing.T) {
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected NewToolHandler(nil, nil) to panic")
			}
		}()
		NewToolHandler(nil, nil)
	}()

	if err := InsertOneShotSchedule(context.Background(), nil, OneShotSchedule{ID: "n1"}); err == nil {
		t.Error("expected error for InsertOneShotSchedule with nil db")
	}

	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)
	ctx := context.Background()

	if h.cfg != cfg {
		t.Errorf("expected handler to retain *Config")
	}

	recErrTests := []struct {
		name       string
		args       ScheduleRecurringArgs
		wantErrMsg string
	}{
		{"missing channel_id", ScheduleRecurringArgs{CronExpression: "0 9 * * *", Prompt: "p"}, "'channel_id' is required"},
		{"missing cron_expression", ScheduleRecurringArgs{ChannelID: "c1", Prompt: "p"}, "'cron_expression' is required"},
		{"missing prompt", ScheduleRecurringArgs{ChannelID: "c1", CronExpression: "0 9 * * *"}, "'prompt' is required"},
		{"invalid cron", ScheduleRecurringArgs{ChannelID: "c1", CronExpression: "not-a-cron", Prompt: "p"}, "invalid cron expression"},
	}
	for _, tc := range recErrTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.ScheduleRecurring(ctx, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			}
		})
	}

	onceErrTests := []struct {
		name       string
		args       ScheduleOnceArgs
		wantErrMsg string
	}{
		{"missing target_id", ScheduleOnceArgs{RunAt: "15m", Prompt: "p"}, "'target_id' is required"},
		{"missing run_at", ScheduleOnceArgs{TargetID: "t1", Prompt: "p"}, "'run_at' is required"},
		{"missing prompt", ScheduleOnceArgs{TargetID: "t1", RunAt: "15m"}, "'prompt' is required"},
		{"invalid run_at", ScheduleOnceArgs{TargetID: "t1", RunAt: "bad-time", Prompt: "p"}, "unrecognized run_at format"},
	}
	for _, tc := range onceErrTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.ScheduleOnce(ctx, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			}
		})
	}

	recSuccessTests := []struct {
		name       string
		args       ScheduleRecurringArgs
		wantEffort string
	}{
		{"default tz and effort", ScheduleRecurringArgs{ChannelID: "ch-1", CronExpression: "0 9 * * *", Prompt: "p1"}, "medium"},
		{"explicit tz and high effort", ScheduleRecurringArgs{ChannelID: "ch-2", CronExpression: "0 20 * * 5", Prompt: "p2", Timezone: "America/New_York", Effort: "high", TitlePrefix: "Weekly"}, "high"},
		{"untrimmed lower effort", ScheduleRecurringArgs{ChannelID: "ch-3", CronExpression: "0 9 * * *", Prompt: "p3", Effort: "  LOW  "}, "low"},
		{"invalid effort fallback", ScheduleRecurringArgs{ChannelID: "ch-4", CronExpression: "0 9 * * *", Prompt: "p4", Effort: "unknown"}, "medium"},
	}
	for _, tc := range recSuccessTests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.ScheduleRecurring(ctx, tc.args)
			if err != nil || res.Status != "success" || res.Effort != tc.wantEffort {
				t.Fatalf("unexpected ScheduleRecurring result: %+v, err: %v", res, err)
			}
		})
	}

	now := time.Now().UTC()
	onceSuccessTests := []struct {
		name       string
		args       ScheduleOnceArgs
		wantEffort string
	}{
		{"target_id relative medium effort", ScheduleOnceArgs{TargetID: "t-1", RunAt: "10m", Prompt: "p1"}, "medium"},
		{"thread_id explicit high effort", ScheduleOnceArgs{ThreadID: "t-2", RunAt: "15m", Prompt: "p2", Effort: "high"}, "high"},
		{"untrimmed effort and absolute tz", ScheduleOnceArgs{TargetID: "t-3", RunAt: "2026-08-28 20:00:00", Timezone: "America/Los_Angeles", Prompt: "p3", Effort: "  LOW  "}, "low"},
		{"invalid effort fallback", ScheduleOnceArgs{TargetID: "t-4", RunAt: "1h", Prompt: "p4", Effort: "invalid"}, "medium"},
	}
	for _, tc := range onceSuccessTests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.ScheduleOnce(ctx, tc.args)
			if err != nil || res.Status != "success" || res.Effort != tc.wantEffort {
				t.Fatalf("unexpected ScheduleOnce result: %+v, err: %v", res, err)
			}
		})
	}

	sZero := OneShotSchedule{ID: "s-zero", ThreadID: "t-zero", Prompt: "p", RunAt: now.Add(time.Hour)}
	if err := InsertOneShotSchedule(nil, db, sZero); err != nil {
		t.Fatalf("InsertOneShotSchedule nil ctx error: %v", err)
	}
	listZero, err := ListOneShotSchedules(ctx, db, "t-zero")
	if err != nil || len(listZero) != 1 || listZero[0].CreatedAt.IsZero() {
		t.Fatalf("expected non-zero CreatedAt, got %+v, err: %v", listZero, err)
	}

	_, _ = db.Exec("INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ('c-empty', 'ch-raw', '', '0 0 * * *', 'p', 'UTC', ?, TRUE, ?, '')", now, now)
	_, _ = db.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ('o-empty', 'ch-raw', 'p', ?, ?, '')", now, now)

	listAll, err := h.ListSchedules(ctx, ListSchedulesArgs{})
	if err != nil || len(listAll.Recurring) == 0 || len(listAll.OneShot) == 0 {
		t.Fatalf("expected non-empty lists, got %+v, err: %v", listAll, err)
	}

	listFiltered, err := h.ListSchedules(ctx, ListSchedulesArgs{TargetID: "ch-raw"})
	if err != nil || len(listFiltered.Recurring) != 1 || listFiltered.Recurring[0].Effort != "medium" || len(listFiltered.OneShot) != 1 || listFiltered.OneShot[0].Effort != "medium" {
		t.Fatalf("expected medium effort on empty string, got %+v, err: %v", listFiltered, err)
	}

	closedDB, _ := sql.Open("sqlite", ":memory:")
	_ = closedDB.Close()
	hClosed := NewToolHandler(cfg, closedDB)

	if _, err := hClosed.ScheduleRecurring(ctx, ScheduleRecurringArgs{ChannelID: "c", CronExpression: "0 9 * * *", Prompt: "p"}); err == nil {
		t.Error("expected error with closed db in ScheduleRecurring")
	}
	if _, err := hClosed.ScheduleOnce(ctx, ScheduleOnceArgs{TargetID: "t", RunAt: "15m", Prompt: "p"}); err == nil {
		t.Error("expected error with closed db in ScheduleOnce")
	}
	if _, err := hClosed.ListSchedules(ctx, ListSchedulesArgs{TargetID: "t"}); err == nil {
		t.Error("expected error with closed db in ListSchedules")
	}
	effHigh := "high"
	if _, err := hClosed.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{ScheduleID: "c1", Effort: &effHigh}); err == nil {
		t.Error("expected error updating cron with closed db")
	}
	if _, err := hClosed.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: "s1"}); err == nil {
		t.Error("expected error canceling schedule with closed db")
	}
}

func TestToolHandler_UpdateAndCancel(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)
	ctx := context.Background()

	now := time.Now().UTC()
	c := CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "prompt", Timezone: "America/Los_Angeles", NextRunAt: now, Enabled: true, Effort: "medium"}
	if err := InsertCronSchedule(ctx, db, c); err != nil {
		t.Fatalf("InsertCronSchedule error: %v", err)
	}
	s := OneShotSchedule{ID: "s1", ThreadID: "t1", Prompt: "prompt", RunAt: now, Effort: "medium"}
	if err := InsertOneShotSchedule(ctx, db, s); err != nil {
		t.Fatalf("InsertOneShotSchedule error: %v", err)
	}

	eff, expr, prompt, prefix, tz := "high", "0 12 * * *", "updated", "NewPrefix", "UTC"
	upRes, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID: "c1", Effort: &eff, CronExpression: &expr, Prompt: &prompt, TitlePrefix: &prefix, Timezone: &tz,
	})
	if err != nil || upRes.Status != "success" || upRes.Effort != "high" || upRes.NextRunAt == "" {
		t.Fatalf("unexpected UpdateCronSchedule output: %+v, err: %v", upRes, err)
	}

	badExpr := "not-a-cron"
	updateErrTests := []struct {
		name       string
		args       UpdateCronScheduleArgs
		wantErrMsg string
	}{
		{"missing schedule_id", UpdateCronScheduleArgs{ScheduleID: "  "}, "'schedule_id' is required"},
		{"invalid cron expression", UpdateCronScheduleArgs{ScheduleID: "c1", CronExpression: &badExpr}, "invalid cron expression"},
		{"non-existent schedule", UpdateCronScheduleArgs{ScheduleID: "missing-id", Effort: &eff}, "failed to update recurring schedule"},
	}
	for _, tc := range updateErrTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.UpdateCronSchedule(ctx, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			}
		})
	}

	for _, id := range []string{"c1", "s1"} {
		if out, err := h.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: id}); err != nil || out.Status != "success" {
			t.Fatalf("CancelSchedule %s failed: %v", id, err)
		}
	}

	cancelErrTests := []struct {
		name       string
		id         string
		wantErrMsg string
	}{
		{"empty schedule_id", "  ", "'schedule_id' is required"},
		{"non-existent schedule", "missing-id", "not found"},
	}
	for _, tc := range cancelErrTests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: tc.id})
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			}
		})
	}
}
