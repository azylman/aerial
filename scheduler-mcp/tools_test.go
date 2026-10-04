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

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	yamlContent := `
port: "4005"
timezone: "America/Chicago"
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	// 1. Success case
	cfg, err := LoadConfigFile(cfgPath, "postgres://user:pass@host:5432/db")
	if err != nil {
		t.Fatalf("LoadConfigFile failed: %v", err)
	}
	if cfg.Port != "4005" {
		t.Errorf("expected port 4005, got %s", cfg.Port)
	}
	if cfg.Timezone != "America/Chicago" {
		t.Errorf("expected timezone America/Chicago, got %s", cfg.Timezone)
	}
	if cfg.DatabaseURL != "postgres://user:pass@host:5432/db" {
		t.Errorf("expected dbURL postgres://user:pass@host:5432/db, got %s", cfg.DatabaseURL)
	}

	// 2. Missing file -> fail fast
	if _, err := LoadConfigFile(filepath.Join(tmpDir, "nonexistent.yaml"), "postgres://..."); err == nil {
		t.Error("expected error for nonexistent config file")
	}

	// 3. Malformed YAML -> fail fast
	malformedPath := filepath.Join(tmpDir, "malformed.yaml")
	_ = os.WriteFile(malformedPath, []byte("port: [unclosed"), 0644)
	if _, err := LoadConfigFile(malformedPath, "postgres://..."); err == nil {
		t.Error("expected error for malformed YAML")
	}

	// 4. Missing port -> fail fast
	noPortPath := filepath.Join(tmpDir, "no_port.yaml")
	_ = os.WriteFile(noPortPath, []byte("timezone: \"America/Chicago\"\n"), 0644)
	if _, err := LoadConfigFile(noPortPath, "postgres://..."); err == nil {
		t.Error("expected error when port is missing")
	}

	// 5. Missing timezone -> fail fast
	badPath := filepath.Join(tmpDir, "bad.yaml")
	_ = os.WriteFile(badPath, []byte("port: \"4005\"\n"), 0644)
	if _, err := LoadConfigFile(badPath, "postgres://..."); err == nil {
		t.Error("expected error when timezone is missing")
	}

	// 6. Missing dbURL -> fail fast
	if _, err := LoadConfigFile(cfgPath, ""); err == nil {
		t.Error("expected error when dbURL is empty")
	}

	// 7. Empty path -> fail fast
	if _, err := LoadConfigFile("", "postgres://..."); err == nil {
		t.Error("expected error when path is empty")
	}
}

func TestMain_Execution(t *testing.T) {
	oldRun := runAppFn
	oldExit := exitFn
	oldPath := configPath
	defer func() {
		runAppFn = oldRun
		exitFn = oldExit
		configPath = oldPath
	}()

	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(testCfg, []byte("port: \"4005\"\ntimezone: \"America/Los_Angeles\"\n"), 0644)
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	// 1. Success path
	runCalled := false
	runAppFn = func(ctx context.Context, cfg *Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn to be called")
	}

	// 2. Server closed error (should not trigger exitFn)
	exitCalled := false
	exitFn = func(format string, v ...interface{}) {
		exitCalled = true
	}
	runAppFn = func(ctx context.Context, cfg *Config) error {
		return http.ErrServerClosed
	}
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	// 3. Server generic failure (should trigger exitFn)
	exitCalled = false
	runAppFn = func(ctx context.Context, cfg *Config) error {
		return context.DeadlineExceeded
	}
	main()
	if !exitCalled {
		t.Error("expected exitFn on server error")
	}

	// 4. Config loading failure (should trigger exitFn)
	exitCalled = false
	configPath = filepath.Join(tmpDir, "nonexistent.yaml")
	t.Setenv("CONFIG_PATH", configPath)
	main()
	if !exitCalled {
		t.Error("expected exitFn on config load error")
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

	crons, err := ListCronSchedules(context.Background(), db, "chan-default-tz")
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

func setupTestDB(t *testing.T) (*sql.DB, error) {
	t.Helper()
	cfg := &Config{DatabaseURL: ":memory:", Timezone: DefaultTimezone}
	return InitDB(cfg)
}

func TestScheduleOnce_EffortDefaultAndExplicit(t *testing.T) {
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}

	// 1. Unspecified effort defaults to "low"
	out1, err := handleScheduleOnce(context.Background(), db, ScheduleOnceArgs{
		Prompt:   "remind me",
		RunAt:    time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339),
		ThreadID: "thread-mcp-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out1.Effort != "low" {
		t.Fatalf("expected effort 'low', got %q", out1.Effort)
	}

	// 2. Explicit "high" effort is preserved
	out2, err := handleScheduleOnce(context.Background(), db, ScheduleOnceArgs{
		Prompt:   "remind high",
		RunAt:    time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339),
		ThreadID: "thread-mcp-2",
		Effort:   "high",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out2.Effort != "high" {
		t.Fatalf("expected effort 'high', got %q", out2.Effort)
	}
}

func TestScheduleRecurring_EffortDefaultAndExplicit(t *testing.T) {
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}

	cfg := &Config{DatabaseURL: ":memory:", Timezone: DefaultTimezone}
	h := NewToolHandler(cfg, db)

	// 1. Unspecified effort defaults to "low"
	out1, err := h.ScheduleRecurring(context.Background(), ScheduleRecurringArgs{
		ChannelID:      "chan-rec-1",
		CronExpression: "0 9 * * *",
		Prompt:         "routine prompt",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out1.Effort != "low" {
		t.Fatalf("expected recurring effort 'low', got %q", out1.Effort)
	}

	// 2. Explicit "high" effort is preserved
	out2, err := h.ScheduleRecurring(context.Background(), ScheduleRecurringArgs{
		ChannelID:      "chan-rec-2",
		CronExpression: "0 9 * * *",
		Prompt:         "routine prompt high",
		Effort:         "high",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out2.Effort != "high" {
		t.Fatalf("expected recurring effort 'high', got %q", out2.Effort)
	}

	// 3. Untrimmed uppercase "  HIGH  " canonicalizes to "high"
	out3, err := h.ScheduleRecurring(context.Background(), ScheduleRecurringArgs{
		ChannelID:      "chan-rec-3",
		CronExpression: "0 9 * * *",
		Prompt:         "routine prompt untrimmed",
		Effort:         "  HIGH  ",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out3.Effort != "high" {
		t.Fatalf("expected recurring effort 'high', got %q", out3.Effort)
	}

	// 4. Invalid effort defaults to "low"
	out4, err := h.ScheduleRecurring(context.Background(), ScheduleRecurringArgs{
		ChannelID:      "chan-rec-4",
		CronExpression: "0 9 * * *",
		Prompt:         "routine prompt invalid",
		Effort:         "invalid-tier",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out4.Effort != "low" {
		t.Fatalf("expected recurring effort 'low', got %q", out4.Effort)
	}
}

func TestScheduleOnce_TargetIDAndEffortCanonicalization(t *testing.T) {
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}

	// 1. Using TargetID and untrimmed uppercase "  HIGH  "
	out1, err := handleScheduleOnce(context.Background(), db, ScheduleOnceArgs{
		TargetID: "target-canonical-1",
		Prompt:   "canonical high",
		RunAt:    "10m",
		Effort:   "  HIGH  ",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out1.Effort != "high" {
		t.Fatalf("expected effort 'high', got %q", out1.Effort)
	}

	// 2. Using TargetID and invalid effort -> "low"
	out2, err := handleScheduleOnce(context.Background(), db, ScheduleOnceArgs{
		TargetID: "target-canonical-2",
		Prompt:   "canonical invalid",
		RunAt:    "15m",
		Effort:   "medium",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out2.Effort != "low" {
		t.Fatalf("expected effort 'low', got %q", out2.Effort)
	}

	// Verify persistence in DB
	schedules, err := ListOneShotSchedules(context.Background(), db, "target-canonical-1")
	if err != nil || len(schedules) != 1 {
		t.Fatalf("expected 1 schedule for target-canonical-1, got %v, err: %v", schedules, err)
	}
	if schedules[0].Effort != "high" {
		t.Fatalf("expected persisted effort 'high', got %q", schedules[0].Effort)
	}

	schedules2, err := ListOneShotSchedules(context.Background(), db, "target-canonical-2")
	if err != nil || len(schedules2) != 1 {
		t.Fatalf("expected 1 schedule for target-canonical-2, got %v, err: %v", schedules2, err)
	}
	if schedules2[0].Effort != "low" {
		t.Fatalf("expected persisted effort 'low', got %q", schedules2[0].Effort)
	}
}

func TestScheduleOnce_CoalesceNullAndEmptyString(t *testing.T) {
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}

	// Raw SQL insert with empty string into one_shot_schedules
	now := time.Now().UTC()
	_, err = db.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES (?, ?, ?, ?, ?, ?)",
		"raw-empty-effort", "thread-raw-1", "test empty effort", now, now, "")
	if err != nil {
		t.Fatalf("failed raw insert into one_shot_schedules: %v", err)
	}

	schedules, err := ListOneShotSchedules(context.Background(), db, "thread-raw-1")
	if err != nil || len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %v, err: %v", schedules, err)
	}
	if schedules[0].Effort != "low" {
		t.Fatalf("expected COALESCE(NULLIF(effort, ''), 'low') to produce 'low', got %q", schedules[0].Effort)
	}

	// Raw SQL insert with empty string into cron_schedules
	_, err = db.Exec("INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"raw-cron-empty", "chan-raw-1", "", "0 0 * * *", "test empty cron", "America/Los_Angeles", now, true, now, "")
	if err != nil {
		t.Fatalf("failed raw insert into cron_schedules: %v", err)
	}

	crons, err := ListCronSchedules(context.Background(), db, "chan-raw-1")
	if err != nil || len(crons) != 1 {
		t.Fatalf("expected 1 cron schedule, got %v, err: %v", crons, err)
	}
	if crons[0].Effort != "low" {
		t.Fatalf("expected cron COALESCE(NULLIF(effort, ''), 'low') to produce 'low', got %q", crons[0].Effort)
	}
}

func TestInsertOneShotSchedule_NilDBAndDefaults(t *testing.T) {
	// Nil DB check
	err := InsertOneShotSchedule(context.Background(), nil, OneShotSchedule{
		ID:       "oneshot-nil",
		ThreadID: "thread-nil",
		Prompt:   "nil db test",
		RunAt:    time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected error with nil db, got nil")
	}
	if err.Error() != "database is nil" {
		t.Fatalf("expected 'database is nil', got %q", err.Error())
	}

	// Zero CreatedAt gets auto-populated
	db, err := setupTestDB(t)
	if err != nil {
		t.Fatalf("failed to setup test db: %v", err)
	}
	s := OneShotSchedule{
		ID:       "oneshot-zero-created",
		ThreadID: "thread-zero-created",
		Prompt:   "zero created at",
		RunAt:    time.Now().UTC().Add(time.Hour),
	}
	if err := InsertOneShotSchedule(nil, db, s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	list, err := ListOneShotSchedules(context.Background(), db, "thread-zero-created")
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 schedule, got %v, err: %v", list, err)
	}
	if list[0].CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be auto-populated, got zero")
	}
	if list[0].Effort != "low" {
		t.Fatalf("expected effort 'low', got %q", list[0].Effort)
	}
}



