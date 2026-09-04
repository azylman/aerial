package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGetDefaultTimezone(t *testing.T) {
	// 1. Fallback when both DEFAULT_TIMEZONE and TZ are unset
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	if tz := GetDefaultTimezone(); tz != "America/Los_Angeles" {
		t.Errorf("Expected fallback 'America/Los_Angeles', got %q", tz)
	}

	// 2. TZ environment variable fallback
	t.Setenv("TZ", "America/Chicago")
	if tz := GetDefaultTimezone(); tz != "America/Chicago" {
		t.Errorf("Expected TZ 'America/Chicago', got %q", tz)
	}

	// 3. DEFAULT_TIMEZONE environment variable takes highest precedence
	t.Setenv("DEFAULT_TIMEZONE", "America/New_York")
	if tz := GetDefaultTimezone(); tz != "America/New_York" {
		t.Errorf("Expected DEFAULT_TIMEZONE 'America/New_York', got %q", tz)
	}
}

func TestParseRunAt(t *testing.T) {
	t.Setenv("DEFAULT_TIMEZONE", "America/Los_Angeles")
	t.Setenv("TZ", "")

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
		got, err := ParseRunAt(tc.input, baseTime)
		if (err != nil) != tc.hasError {
			t.Errorf("ParseRunAt(%q) error = %v, expected error = %t", tc.input, err, tc.hasError)
			continue
		}
		if !tc.hasError && !got.Equal(tc.expected) {
			t.Errorf("ParseRunAt(%q) = %v, expected %v", tc.input, got, tc.expected)
		}
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

	// Empty timezone defaults to GetDefaultTimezone() ("America/Los_Angeles")
	t.Setenv("DEFAULT_TIMEZONE", "America/Los_Angeles")
	t.Setenv("TZ", "")
	nextDefault, err := CalculateNextCronRun("0 9 * * *", "", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextCronRun with empty timezone failed: %v", err)
	}
	if !nextDefault.Equal(expectedLA) {
		t.Errorf("Expected default next run %s, got %s", expectedLA, nextDefault)
	}

	// Configurable DEFAULT_TIMEZONE override: America/New_York (EDT = UTC-4)
	// 9 AM EDT from 12:00 UTC (8:00 AM EDT) -> Aug 28 9:00 EDT (13:00 UTC)
	t.Setenv("DEFAULT_TIMEZONE", "America/New_York")
	nextNYDefault, err := CalculateNextCronRun("0 9 * * *", "", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextCronRun with DEFAULT_TIMEZONE override failed: %v", err)
	}
	expectedNY := time.Date(2026, time.August, 28, 13, 0, 0, 0, time.UTC)
	if !nextNYDefault.Equal(expectedNY) {
		t.Errorf("Expected next NY run %s, got %s", expectedNY, nextNYDefault)
	}

	// Invalid cron
	_, err = CalculateNextCronRun("not a cron expr", "UTC", baseTime)
	if err == nil {
		t.Error("Expected error on invalid cron expression")
	}
}

func TestToolHandlerOperations(t *testing.T) {
	t.Setenv("DEFAULT_TIMEZONE", "America/Los_Angeles")
	t.Setenv("TZ", "")

	db, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	handler := NewToolHandler(db)

	// 1. Schedule recurring (explicit timezone America/Los_Angeles)
	recArgJSON := []byte(`{
		"channel_id": "1542423172400291873",
		"cron_expression": "0 20 * * 5",
		"prompt": "Message me with a weekly meal plan",
		"title_prefix": "Weekly Meal Plan",
		"timezone": "America/Los_Angeles"
	}`)

	recRes, err := handler.HandleScheduleRecurring(recArgJSON)
	if err != nil {
		t.Fatalf("HandleScheduleRecurring failed: %v", err)
	}
	recMap, ok := recRes.(map[string]interface{})
	if !ok || recMap["status"] != "success" {
		t.Fatalf("Unexpected recurring response: %+v", recRes)
	}
	schedID, ok := recMap["schedule_id"].(string)
	if !ok || schedID == "" {
		t.Fatalf("Expected schedule_id in response")
	}

	// 2. Schedule one-shot (relative duration)
	onceArgJSON := []byte(`{
		"target_id": "thread-456",
		"run_at": "30m",
		"prompt": "Remind me about the appointment"
	}`)

	onceRes, err := handler.HandleScheduleOnce(onceArgJSON)
	if err != nil {
		t.Fatalf("HandleScheduleOnce failed: %v", err)
	}
	onceMap, ok := onceRes.(map[string]interface{})
	if !ok || onceMap["status"] != "success" {
		t.Fatalf("Unexpected once response: %+v", onceRes)
	}
	onceID, ok := onceMap["schedule_id"].(string)
	if !ok || onceID == "" {
		t.Fatalf("Expected schedule_id in response")
	}

	// 3. List schedules (all)
	listRes, err := handler.HandleListSchedules(nil)
	if err != nil {
		t.Fatalf("HandleListSchedules failed: %v", err)
	}
	listMap := listRes.(map[string]interface{})
	recList := listMap["recurring"].([]CronSchedule)
	onceList := listMap["one_shot"].([]OneShotSchedule)
	if len(recList) != 1 || len(onceList) != 1 {
		t.Errorf("Expected 1 recurring and 1 once, got %d, %d", len(recList), len(onceList))
	}
	if recList[0].Timezone != "America/Los_Angeles" {
		t.Errorf("Expected timezone 'America/Los_Angeles', got %q", recList[0].Timezone)
	}

	// 4. List schedules filtered by target_id
	filterJSON, _ := json.Marshal(map[string]string{"target_id": "1542423172400291873"})
	filteredRes, err := handler.HandleListSchedules(filterJSON)
	if err != nil {
		t.Fatalf("Filtered HandleListSchedules failed: %v", err)
	}
	filteredMap := filteredRes.(map[string]interface{})
	fRec := filteredMap["recurring"].([]CronSchedule)
	fOnce := filteredMap["one_shot"].([]OneShotSchedule)
	if len(fRec) != 1 || len(fOnce) != 0 {
		t.Errorf("Expected 1 recurring and 0 once for target_id, got %d, %d", len(fRec), len(fOnce))
	}

	// 5. Cancel schedule (recurring)
	cancelRecJSON, _ := json.Marshal(map[string]string{"schedule_id": schedID})
	cancelRecRes, err := handler.HandleCancelSchedule(cancelRecJSON)
	if err != nil {
		t.Fatalf("HandleCancelSchedule recurring failed: %v", err)
	}
	cancelRecMap := cancelRecRes.(map[string]interface{})
	if cancelRecMap["status"] != "success" {
		t.Errorf("Expected success cancel response, got %+v", cancelRecRes)
	}

	// 6. Cancel schedule (one-shot)
	cancelOnceJSON, _ := json.Marshal(map[string]string{"schedule_id": onceID})
	cancelOnceRes, err := handler.HandleCancelSchedule(cancelOnceJSON)
	if err != nil {
		t.Fatalf("HandleCancelSchedule once failed: %v", err)
	}
	cancelOnceMap := cancelOnceRes.(map[string]interface{})
	if cancelOnceMap["status"] != "success" {
		t.Errorf("Expected success cancel response, got %+v", cancelOnceRes)
	}

	// 7. Cancel non-existent schedule
	_, err = handler.HandleCancelSchedule([]byte(`{"schedule_id":"non-existent"}`))
	if err == nil {
		t.Error("Expected error canceling non-existent schedule")
	}
}

func TestToolHandler_DefaultTimezoneFallback(t *testing.T) {
	t.Setenv("DEFAULT_TIMEZONE", "America/Los_Angeles")
	t.Setenv("TZ", "")

	db, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	handler := NewToolHandler(db)

	// Omitted timezone in recurring schedule should default to America/Los_Angeles
	recArgJSON := []byte(`{
		"channel_id": "chan-default-tz",
		"cron_expression": "0 9 * * *",
		"prompt": "Morning routine without timezone"
	}`)

	recRes, err := handler.HandleScheduleRecurring(recArgJSON)
	if err != nil {
		t.Fatalf("HandleScheduleRecurring failed: %v", err)
	}
	recMap := recRes.(map[string]interface{})
	schedID := recMap["schedule_id"].(string)

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
}

func TestToolHandlerValidationErrors(t *testing.T) {
	db, _ := InitDB(":memory:")
	defer func() { _ = db.Close() }()
	handler := NewToolHandler(db)

	// Missing channel_id
	_, err := handler.HandleScheduleRecurring([]byte(`{"cron_expression":"0 20 * * 5","prompt":"p"}`))
	if err == nil {
		t.Error("Expected error for missing channel_id")
	}

	// Missing cron_expression
	_, err = handler.HandleScheduleRecurring([]byte(`{"channel_id":"123","prompt":"p"}`))
	if err == nil {
		t.Error("Expected error for missing cron_expression")
	}

	// Missing prompt in once
	_, err = handler.HandleScheduleOnce([]byte(`{"target_id":"123","run_at":"30m"}`))
	if err == nil {
		t.Error("Expected error for missing prompt")
	}

	// Missing schedule_id in cancel
	_, err = handler.HandleCancelSchedule([]byte(`{}`))
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
	db, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()
	handler := NewToolHandler(db)

	payload := []byte(`{
		"target_id": "thread-tz-test",
		"run_at": "2026-08-28 20:00:00",
		"prompt": "Timezone reminder",
		"timezone": "America/Los_Angeles"
	}`)

	res, err := handler.HandleScheduleOnce(payload)
	if err != nil {
		t.Fatalf("HandleScheduleOnce with timezone failed: %v", err)
	}
	resMap := res.(map[string]interface{})
	if resMap["status"] != "success" {
		t.Errorf("Expected success, got %+v", resMap)
	}

	// 20:00 PDT = 03:00 UTC (Aug 29)
	expectedUTC := "2026-08-29T03:00:00Z"
	if resMap["run_at"] != expectedUTC {
		t.Errorf("Expected run_at %s, got %v", expectedUTC, resMap["run_at"])
	}
}

func TestFileDBDSNPragmasAndIndices(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mcpdbtest-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "scheduler.db")
	database, err := InitDB(dbPath)
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

func isProductionDSN(dsn string) bool {
	lower := strings.ToLower(dsn)
	if strings.Contains(lower, "aerial_test") || strings.Contains(lower, "test_") || strings.Contains(lower, "_test") {
		return false
	}
	return strings.Contains(lower, "@postgres:5432/aerial") ||
		strings.Contains(lower, "@127.0.0.1:5432/aerial") ||
		strings.Contains(lower, "@localhost:5432/aerial") ||
		strings.Contains(lower, ":5432/aerial") ||
		strings.Contains(lower, "/aerial")
}

func TestPostgresSchedules(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:aerial_test@127.0.0.1:54329/aerial_test?sslmode=disable"
	}

	// Defensive invariant: Refuse to run test setup or truncate tables if DSN targets production
	if isProductionDSN(dsn) {
		t.Fatalf("CRITICAL SAFETY CHECK: TestPostgresSchedules detected production database in DSN %q; refusing to truncate", dsn)
		return
	}

	// Quick connectivity probe (200ms) to avoid multi-minute retry delays in offline test runners
	quickDB, openErr := sql.Open("pgx", dsn)
	if openErr != nil {
		t.Skipf("Skipping postgres test: database driver error: %v", openErr)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	pingErr := quickDB.PingContext(ctx)
	cancel()
	_ = quickDB.Close()
	if pingErr != nil {
		t.Skipf("Skipping postgres test: PostgreSQL not reachable at %s: %v", dsn, pingErr)
		return
	}

	database, err := InitDB(dsn)
	if err != nil {
		t.Skipf("Skipping postgres test: failed to connect: %v", err)
		return
	}
	defer func() { _ = database.Close() }()

	_, err = database.Exec("TRUNCATE TABLE cron_schedules, one_shot_schedules RESTART IDENTITY CASCADE;")
	if err != nil {
		t.Fatalf("Failed to truncate tables: %v", err)
	}

	now := time.Now().UTC()
	cron := CronSchedule{
		ID:          "cron-pg-1",
		TargetID:    "chan-123",
		TitlePrefix: "Weather",
		CronExpr:    "0 6 * * *",
		Prompt:      "Morning forecast",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(1 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := InsertCronSchedule(database, cron); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	cList, err := ListCronSchedules(database, "chan-123")
	if err != nil {
		t.Fatalf("ListCronSchedules failed: %v", err)
	}
	if len(cList) != 1 || cList[0].ID != "cron-pg-1" {
		t.Fatalf("Unexpected cron list: %+v", cList)
	}

	oneshot := OneShotSchedule{
		ID:        "oneshot-pg-1",
		ThreadID:  "thread-456",
		Prompt:    "Reminder prompt",
		RunAt:     now.Add(10 * time.Minute),
		CreatedAt: now,
	}
	if err := InsertOneShotSchedule(database, oneshot); err != nil {
		t.Fatalf("InsertOneShotSchedule failed: %v", err)
	}

	oList, err := ListOneShotSchedules(database, "thread-456")
	if err != nil {
		t.Fatalf("ListOneShotSchedules failed: %v", err)
	}
	if len(oList) != 1 || oList[0].ID != "oneshot-pg-1" {
		t.Fatalf("Unexpected oneshot list: %+v", oList)
	}

	deleted, err := DeleteSchedule(database, "cron-pg-1")
	if err != nil || !deleted {
		t.Fatalf("DeleteSchedule cron failed: deleted=%v, err=%v", deleted, err)
	}

	deleted, err = DeleteSchedule(database, "oneshot-pg-1")
	if err != nil || !deleted {
		t.Fatalf("DeleteSchedule oneshot failed: deleted=%v, err=%v", deleted, err)
	}
}

