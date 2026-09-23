package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig_TimezoneResolution(t *testing.T) {
	// 1. Fallback when both DEFAULT_TIMEZONE and TZ are unset
	cfg1, err := LoadConfigFromLookup(func(k string) string {
		if k == "DATABASE_URL" {
			return ":memory:"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("LoadConfigFromLookup failed: %v", err)
	}
	if cfg1.Timezone != DefaultTimezone {
		t.Errorf("Expected fallback %q, got %q", DefaultTimezone, cfg1.Timezone)
	}

	// 2. TZ environment variable fallback
	cfg2, err := LoadConfigFromLookup(func(k string) string {
		if k == "DATABASE_URL" {
			return ":memory:"
		}
		if k == "TZ" {
			return "America/Chicago"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("LoadConfigFromLookup failed: %v", err)
	}
	if cfg2.Timezone != "America/Chicago" {
		t.Errorf("Expected TZ 'America/Chicago', got %q", cfg2.Timezone)
	}

	// 3. DEFAULT_TIMEZONE environment variable takes highest precedence
	cfg3, err := LoadConfigFromLookup(func(k string) string {
		if k == "DATABASE_URL" {
			return ":memory:"
		}
		if k == "DEFAULT_TIMEZONE" {
			return "America/New_York"
		}
		if k == "TZ" {
			return "America/Chicago"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("LoadConfigFromLookup failed: %v", err)
	}
	if cfg3.Timezone != "America/New_York" {
		t.Errorf("Expected DEFAULT_TIMEZONE 'America/New_York', got %q", cfg3.Timezone)
	}
}

func TestParseRunAt(t *testing.T) {
	baseTime := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		input    string
		expected time.Time
		hasError bool
	}{
		// Relative durations
		{"30s", baseTime.Add(30 * time.Second), false},
		{"45 seconds", baseTime.Add(45 * time.Second), false},
		{"15m", baseTime.Add(15 * time.Minute), false},
		{"30 mins", baseTime.Add(30 * time.Minute), false},
		{"10 minutes", baseTime.Add(10 * time.Minute), false},
		{"2h", baseTime.Add(2 * time.Hour), false},
		{"3 hours", baseTime.Add(3 * time.Hour), false},
		{"1d", baseTime.Add(24 * time.Hour), false},
		{"2 days", baseTime.Add(48 * time.Hour), false},
		{"1w", baseTime.Add(7 * 24 * time.Hour), false},
		{"1h30m", baseTime.Add(90 * time.Minute), false},

		// Explicit UTC / RFC3339 timestamps
		{"2026-08-28T21:00:00Z", time.Date(2026, time.August, 28, 21, 0, 0, 0, time.UTC), false},

		// Absolute timestamps in default timezone America/Los_Angeles (PDT = UTC-7 in August)
		// 21:00 PDT -> 04:00 UTC (Aug 29)
		{"2026-08-28 21:00:00", time.Date(2026, time.August, 29, 4, 0, 0, 0, time.UTC), false},
		{"2026-08-28T21:00", time.Date(2026, time.August, 29, 4, 0, 0, 0, time.UTC), false},
		// 00:00 PDT -> 07:00 UTC
		{"2026-08-28", time.Date(2026, time.August, 28, 7, 0, 0, 0, time.UTC), false},

		// Errors
		{"", time.Time{}, true},
		{"invalid duration", time.Time{}, true},
	}

	for _, tc := range tests {
		got, err := ParseRunAtWithTimezone(tc.input, "America/Los_Angeles", baseTime)
		if (err != nil) != tc.hasError {
			t.Errorf("ParseRunAtWithTimezone(%q) error = %v, expected error = %t", tc.input, err, tc.hasError)
			continue
		}
		if !tc.hasError && !got.Equal(tc.expected) {
			t.Errorf("ParseRunAtWithTimezone(%q) = %v, expected %v", tc.input, got, tc.expected)
		}
	}

	// Empty timezone error
	if _, err := ParseRunAtWithTimezone("15m", "", baseTime); err == nil {
		t.Error("expected error for empty timezone")
	}

	// Invalid timezone error
	if _, err := ParseRunAtWithTimezone("15m", "Invalid/Timezone", baseTime); err == nil {
		t.Error("expected error for invalid timezone")
	}
}

func TestCalculateNextCronRun(t *testing.T) {
	// Friday Aug 28, 2026 12:00:00 UTC (05:00:00 PDT)
	baseTime := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)

	// "0 20 * * 5" -> Friday at 20:00 UTC
	next, err := CalculateNextCronRun("0 20 * * 5", "UTC", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextCronRun failed: %v", err)
	}
	expected := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("Expected next run %s, got %s", expected, next)
	}

	// "@weekly" -> Sunday midnight UTC
	nextWeekly, err := CalculateNextCronRun("@weekly", "UTC", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextCronRun @weekly failed: %v", err)
	}
	expectedWeekly := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	if !nextWeekly.Equal(expectedWeekly) {
		t.Errorf("Expected next weekly %s, got %s", expectedWeekly, nextWeekly)
	}

	// Timezone test with America/Los_Angeles (PDT = UTC-7 in August)
	// "0 9 * * *" (9 AM PDT) from Aug 28 12:00 UTC (5:00 AM PDT) -> Aug 28 9:00 PDT (16:00 UTC)
	nextLA, err := CalculateNextCronRun("0 9 * * *", "America/Los_Angeles", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextCronRun America/Los_Angeles failed: %v", err)
	}
	expectedLA := time.Date(2026, time.August, 28, 16, 0, 0, 0, time.UTC)
	if !nextLA.Equal(expectedLA) {
		t.Errorf("Expected next LA run %s, got %s", expectedLA, nextLA)
	}

	// Timezone test with America/New_York (EDT = UTC-4 in August)
	// 9 AM EDT from 12:00 UTC (8:00 AM EDT) -> Aug 28 9:00 EDT (13:00 UTC)
	nextNY, err := CalculateNextCronRun("0 9 * * *", "America/New_York", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextCronRun America/New_York failed: %v", err)
	}
	expectedNY := time.Date(2026, time.August, 28, 13, 0, 0, 0, time.UTC)
	if !nextNY.Equal(expectedNY) {
		t.Errorf("Expected next NY run %s, got %s", expectedNY, nextNY)
	}

	// Empty timezone must return an error
	if _, err := CalculateNextCronRun("0 9 * * *", "", baseTime); err == nil {
		t.Error("Expected error on empty timezone")
	}

	// Invalid timezone must return an error
	if _, err := CalculateNextCronRun("0 9 * * *", "Invalid/Timezone", baseTime); err == nil {
		t.Error("Expected error on invalid timezone")
	}

	// Invalid cron
	if _, err := CalculateNextCronRun("not a cron expr", "UTC", baseTime); err == nil {
		t.Error("Expected error on invalid cron expression")
	}
}

func TestInitDB_ConfigPointer(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Chicago"}
	database, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer database.Close()

	handler := NewToolHandler(cfg, database)
	if handler.cfg != cfg {
		t.Errorf("expected handler to retain *Config")
	}
}

func TestToolHandlerOperations(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	handler := NewToolHandler(cfg, db)

	// 1. Schedule recurring (explicit timezone America/Los_Angeles and low effort)
	recRes, err := handler.ScheduleRecurring(ctx, ScheduleRecurringArgs{
		ChannelID:      "1542423172400291873",
		CronExpression: "0 20 * * 5",
		Prompt:         "Message me with a weekly meal plan",
		TitlePrefix:    "Weekly Meal Plan",
		Timezone:       "America/Los_Angeles",
		Effort:         "low",
	})
	if err != nil {
		t.Fatalf("ScheduleRecurring failed: %v", err)
	}
	if recRes.Status != "success" || recRes.ScheduleID == "" || recRes.Effort != "low" {
		t.Fatalf("Unexpected recurring response: %+v", recRes)
	}
	schedID := recRes.ScheduleID

	// 2. Schedule one-shot (relative duration)
	onceRes, err := handler.ScheduleOnce(ctx, ScheduleOnceArgs{
		TargetID: "thread-456",
		RunAt:    "30m",
		Prompt:   "Remind me about the appointment",
	})
	if err != nil {
		t.Fatalf("ScheduleOnce failed: %v", err)
	}
	if onceRes.Status != "success" || onceRes.ScheduleID == "" {
		t.Fatalf("Unexpected once response: %+v", onceRes)
	}
	onceID := onceRes.ScheduleID

	// 3. List schedules (all)
	listRes, err := handler.ListSchedules(ctx, ListSchedulesArgs{})
	if err != nil {
		t.Fatalf("ListSchedules failed: %v", err)
	}
	if len(listRes.Recurring) != 1 || len(listRes.OneShot) != 1 {
		t.Errorf("Expected 1 recurring and 1 once, got %d, %d", len(listRes.Recurring), len(listRes.OneShot))
	}
	if listRes.Recurring[0].Timezone != "America/Los_Angeles" {
		t.Errorf("Expected timezone 'America/Los_Angeles', got %q", listRes.Recurring[0].Timezone)
	}

	// 4. List schedules filtered by target_id
	filteredRes, err := handler.ListSchedules(ctx, ListSchedulesArgs{TargetID: "1542423172400291873"})
	if err != nil {
		t.Fatalf("Filtered ListSchedules failed: %v", err)
	}
	if len(filteredRes.Recurring) != 1 || len(filteredRes.OneShot) != 0 {
		t.Errorf("Expected 1 recurring and 0 once for target_id, got %d, %d", len(filteredRes.Recurring), len(filteredRes.OneShot))
	}

	// 5. Cancel schedule (recurring)
	cancelRecRes, err := handler.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: schedID})
	if err != nil {
		t.Fatalf("CancelSchedule recurring failed: %v", err)
	}
	if cancelRecRes.Status != "success" {
		t.Errorf("Expected success cancel response, got %+v", cancelRecRes)
	}

	// 6. Cancel schedule (one-shot)
	cancelOnceRes, err := handler.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: onceID})
	if err != nil {
		t.Fatalf("CancelSchedule once failed: %v", err)
	}
	if cancelOnceRes.Status != "success" {
		t.Errorf("Expected success cancel response, got %+v", cancelOnceRes)
	}

	// 7. Cancel non-existent schedule
	_, err = handler.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: "non-existent"})
	if err == nil {
		t.Error("Expected error canceling non-existent schedule")
	}
}

func TestToolHandler_DefaultTimezoneFallback(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	handler := NewToolHandler(cfg, db)

	// Omitted timezone in recurring schedule should default to America/Los_Angeles (and effort != "low" defaults to "high")
	recRes, err := handler.ScheduleRecurring(context.Background(), ScheduleRecurringArgs{
		ChannelID:      "chan-default-tz",
		CronExpression: "0 9 * * *",
		Prompt:         "Morning routine without timezone",
		Effort:         "high",
	})
	if err != nil {
		t.Fatalf("ScheduleRecurring failed: %v", err)
	}
	schedID := recRes.ScheduleID

	crons, err := ListCronSchedules(db, "chan-default-tz")
	if err != nil || len(crons) != 1 {
		t.Fatalf("Expected 1 cron in DB, got %v (err: %v)", crons, err)
	}
	if crons[0].ID != schedID {
		t.Errorf("Expected schedule ID %s, got %s", schedID, crons[0].ID)
	}
	if crons[0].Timezone != "America/Los_Angeles" {
		t.Errorf("Expected default Timezone 'America/Los_Angeles', got %q", crons[0].Timezone)
	}
	if crons[0].Effort != "high" {
		t.Errorf("Expected effort 'high', got %q", crons[0].Effort)
	}
}

func TestToolHandlerValidationErrors(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, _ := InitDB(cfg)
	defer func() { _ = db.Close() }()
	handler := NewToolHandler(cfg, db)
	ctx := context.Background()

	// Missing channel_id
	_, err := handler.ScheduleRecurring(ctx, ScheduleRecurringArgs{CronExpression: "0 20 * * 5", Prompt: "p"})
	if err == nil {
		t.Error("Expected error for missing channel_id")
	}

	// Missing cron_expression
	_, err = handler.ScheduleRecurring(ctx, ScheduleRecurringArgs{ChannelID: "123", Prompt: "p"})
	if err == nil {
		t.Error("Expected error for missing cron_expression")
	}

	// Missing prompt in once
	_, err = handler.ScheduleOnce(ctx, ScheduleOnceArgs{TargetID: "123", RunAt: "30m"})
	if err == nil {
		t.Error("Expected error for missing prompt")
	}

	// Missing schedule_id in cancel
	_, err = handler.CancelSchedule(ctx, CancelScheduleArgs{})
	if err == nil {
		t.Error("Expected error for missing schedule_id")
	}
}

func TestParseRunAtWithTimezone(t *testing.T) {
	baseTime := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)

	// America/Los_Angeles (PDT = UTC-7 in August)
	// 2026-08-28 15:00:00 PDT -> 2026-08-28 22:00:00 UTC
	tLA, err := ParseRunAtWithTimezone("2026-08-28 15:00:00", "America/Los_Angeles", baseTime)
	if err != nil {
		t.Fatalf("ParseRunAtWithTimezone America/Los_Angeles failed: %v", err)
	}
	expectedLA := time.Date(2026, time.August, 28, 22, 0, 0, 0, time.UTC)
	if !tLA.Equal(expectedLA) {
		t.Errorf("Expected %s, got %s", expectedLA, tLA)
	}

	// America/New_York (EDT = UTC-4 in August)
	// 2026-08-28 15:00:00 EDT -> 2026-08-28 19:00:00 UTC
	tNY, err := ParseRunAtWithTimezone("2026-08-28 15:00:00", "America/New_York", baseTime)
	if err != nil {
		t.Fatalf("ParseRunAtWithTimezone America/New_York failed: %v", err)
	}
	expectedNY := time.Date(2026, time.August, 28, 19, 0, 0, 0, time.UTC)
	if !tNY.Equal(expectedNY) {
		t.Errorf("Expected %s, got %s", expectedNY, tNY)
	}

	// Asia/Tokyo (JST = UTC+9)
	// 2026-08-28 15:00:00 JST -> 2026-08-28 06:00:00 UTC
	tTokyo, err := ParseRunAtWithTimezone("2026-08-28 15:00:00", "Asia/Tokyo", baseTime)
	if err != nil {
		t.Fatalf("ParseRunAtWithTimezone Asia/Tokyo failed: %v", err)
	}
	expectedTokyo := time.Date(2026, time.August, 28, 6, 0, 0, 0, time.UTC)
	if !tTokyo.Equal(expectedTokyo) {
		t.Errorf("Expected %s, got %s", expectedTokyo, tTokyo)
	}

	// Relative duration with timezone (should be relative from now)
	tRel, err := ParseRunAtWithTimezone("45m", "America/Los_Angeles", baseTime)
	if err != nil {
		t.Fatalf("ParseRunAtWithTimezone relative failed: %v", err)
	}
	expectedRel := baseTime.Add(45 * time.Minute)
	if !tRel.Equal(expectedRel) {
		t.Errorf("Expected %s, got %s", expectedRel, tRel)
	}
}

func TestHandleScheduleOnce_WithTimezone(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()
	handler := NewToolHandler(cfg, db)

	res, err := handler.ScheduleOnce(context.Background(), ScheduleOnceArgs{
		TargetID: "thread-tz-test",
		RunAt:    "2026-08-28 20:00:00",
		Prompt:   "Timezone reminder",
		Timezone: "America/Los_Angeles",
	})
	if err != nil {
		t.Fatalf("ScheduleOnce with timezone failed: %v", err)
	}
	if res.Status != "success" {
		t.Errorf("Expected success, got %+v", res)
	}

	// 20:00 PDT = 03:00 UTC (Aug 29)
	expectedUTC := "2026-08-29T03:00:00Z"
	if res.RunAt != expectedUTC {
		t.Errorf("Expected run_at %s, got %v", expectedUTC, res.RunAt)
	}
}

func TestFileDBDSNPragmasAndIndices(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mcpdbtest-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "scheduler.db")
	database, err := InitDB(&Config{DatabaseURL: dbPath})
	if err != nil {
		t.Fatalf("InitDB file failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	var busyTimeout int
	if err := database.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("Query PRAGMA busy_timeout failed: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("Expected busy_timeout 5000, got %d", busyTimeout)
	}

	var journalMode string
	if err := database.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("Query PRAGMA journal_mode failed: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("Expected journal_mode=wal, got %s", journalMode)
	}

	// Verify standardized indices exist
	rows, err := database.Query("SELECT name FROM sqlite_master WHERE type = 'index'")
	if err != nil {
		t.Fatalf("Query sqlite_master for indices failed: %v", err)
	}
	defer rows.Close()

	indexMap := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			indexMap[name] = true
		}
	}

	if !indexMap["idx_cron_schedules_next_run_at"] {
		t.Errorf("Expected idx_cron_schedules_next_run_at to exist in DB, got indices: %v", indexMap)
	}
	if !indexMap["idx_one_shot_schedules_run_at"] {
		t.Errorf("Expected idx_one_shot_schedules_run_at to exist in DB, got indices: %v", indexMap)
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tempDB := filepath.Join(t.TempDir(), "runapp.db")

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunApp(ctx, &Config{Port: "59483", DatabaseURL: tempDB})
	}()

	// Poll health endpoint until server is ready or timeout
	var resp *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		resp, err = http.Get("http://127.0.0.1:59483/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health check failed, status: %v", resp)
	}
	_ = resp.Body.Close()

	// Trigger shutdown
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from RunApp shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RunApp shutdown")
	}
}

func TestToolHandlers_ArgumentValidationErrors(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)

	ctx := context.Background()

	// 1. ScheduleRecurring errors
	if _, err := h.ScheduleRecurring(ctx, ScheduleRecurringArgs{CronExpression: "* * * * *", Prompt: "p"}); err == nil {
		t.Error("expected error for missing channel_id")
	}
	if _, err := h.ScheduleRecurring(ctx, ScheduleRecurringArgs{ChannelID: "c", Prompt: "p"}); err == nil {
		t.Error("expected error for missing cron_expression")
	}
	if _, err := h.ScheduleRecurring(ctx, ScheduleRecurringArgs{ChannelID: "c", CronExpression: "* * * * *"}); err == nil {
		t.Error("expected error for missing prompt")
	}
	if _, err := h.ScheduleRecurring(ctx, ScheduleRecurringArgs{ChannelID: "c", CronExpression: "invalid cron", Prompt: "p"}); err == nil {
		t.Error("expected error for invalid cron expr")
	}

	// 2. ScheduleOnce errors
	if _, err := h.ScheduleOnce(ctx, ScheduleOnceArgs{RunAt: "15m", Prompt: "p"}); err == nil {
		t.Error("expected error for missing target_id")
	}
	if _, err := h.ScheduleOnce(ctx, ScheduleOnceArgs{TargetID: "t", Prompt: "p"}); err == nil {
		t.Error("expected error for missing run_at")
	}
	if _, err := h.ScheduleOnce(ctx, ScheduleOnceArgs{TargetID: "t", RunAt: "15m"}); err == nil {
		t.Error("expected error for missing prompt")
	}
	if _, err := h.ScheduleOnce(ctx, ScheduleOnceArgs{TargetID: "t", RunAt: "unparseable-date", Prompt: "p"}); err == nil {
		t.Error("expected error for unparseable run_at")
	}

	// 3. CancelSchedule errors
	if _, err := h.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: ""}); err == nil {
		t.Error("expected error for empty schedule_id")
	}
	if _, err := h.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: "non-existent"}); err == nil {
		t.Error("expected error for non-existent schedule")
	}

	// 4. Closed DB errors for all handlers
	closedDB, _ := sql.Open("sqlite", ":memory:")
	_ = closedDB.Close()
	hClosed := NewToolHandler(cfg, closedDB)

	if _, err := hClosed.ScheduleRecurring(ctx, ScheduleRecurringArgs{ChannelID: "c", CronExpression: "* * * * *", Prompt: "p"}); err == nil {
		t.Error("expected error with closed db in ScheduleRecurring")
	}
	if _, err := hClosed.ScheduleOnce(ctx, ScheduleOnceArgs{TargetID: "t", RunAt: "15m", Prompt: "p"}); err == nil {
		t.Error("expected error with closed db in ScheduleOnce")
	}
	if _, err := hClosed.ListSchedules(ctx, ListSchedulesArgs{TargetID: "t"}); err == nil {
		t.Error("expected error with closed db in ListSchedules")
	}
	if _, err := hClosed.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: "s1"}); err == nil {
		t.Error("expected error with closed db in CancelSchedule")
	}

	// 5. RunApp startup failure with invalid DSN and invalid port
	origMax := postgresMaxAttempts
	origBase := postgresRetryBase
	postgresMaxAttempts = 1
	postgresRetryBase = 1 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origMax
		postgresRetryBase = origBase
	}()

	if err := RunApp(context.Background(), &Config{Port: "9999", DatabaseURL: "postgres://invalid:pass@127.0.0.1:59996/db?sslmode=disable"}); err == nil {
		t.Error("expected error running app with invalid db")
	}

	if err := RunApp(context.Background(), nil); err == nil {
		t.Error("expected error running app with nil config")
	}

	if err := RunApp(context.Background(), &Config{Port: "", DatabaseURL: ":memory:"}); err == nil {
		t.Error("expected error running app with empty port")
	}

	if err := RunApp(context.Background(), &Config{Port: "-1", DatabaseURL: ":memory:"}); err == nil {
		t.Error("expected error running app with invalid port")
	}
}


