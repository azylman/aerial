package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// -------------------------------------------------------------
// 1. config.go coverage
// -------------------------------------------------------------
func TestConfig_NewAndLoadCoverage(t *testing.T) {
	// Nil lookup error check
	if _, err := LoadConfigFromLookup(nil); err == nil {
		t.Error("expected error when lookup is nil")
	}

	// LoadConfigFromLookup with only POSTGRES_HOST (covering default user/pass/port/db)
	envHostOnly := map[string]string{
		"POSTGRES_HOST": "db.internal",
	}
	cfgHost, err := LoadConfigFromLookup(func(k string) string { return envHostOnly[k] })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedPG := "postgres://aerial:aerial_secure_pass@db.internal:5432/aerial?sslmode=disable"
	if cfgHost.DatabaseURL != expectedPG {
		t.Errorf("expected %s, got %s", expectedPG, cfgHost.DatabaseURL)
	}
	if cfgHost.Port != DefaultPort {
		t.Errorf("expected default port %s, got %s", DefaultPort, cfgHost.Port)
	}
	if cfgHost.Timezone != DefaultTimezone {
		t.Errorf("expected default tz %s, got %s", DefaultTimezone, cfgHost.Timezone)
	}
}

// -------------------------------------------------------------
// 2. tools.go coverage
// -------------------------------------------------------------
func TestTools_NewToolHandler_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic when cfg is nil")
		}
	}()
	_ = NewToolHandler(nil, nil)
}

func TestTools_ParseRunAt_Overflow(t *testing.T) {
	_, err := ParseRunAtWithTimezone("99999999999999999999999999999999999999999999999s", "UTC", time.Now())
	if err == nil {
		t.Errorf("expected error on int overflow")
	}
}

func TestTools_UpdateCronSchedule_AllBranches(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)
	ctx := context.Background()

	// Empty schedule_id
	if _, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{ScheduleID: "  "}); err == nil {
		t.Error("expected error on empty schedule_id")
	}

	// Insert a schedule to update
	insertRes, err := h.ScheduleRecurring(ctx, ScheduleRecurringArgs{
		ChannelID:      "chan-test",
		CronExpression: "0 10 * * *",
		Prompt:         "original prompt",
		Effort:         "high",
	})
	if err != nil {
		t.Fatalf("ScheduleRecurring failed: %v", err)
	}
	schedID := insertRes.ScheduleID

	// Invalid cron expression in update
	badCron := "invalid-cron"
	if _, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID:     schedID,
		CronExpression: &badCron,
	}); err == nil {
		t.Error("expected error on invalid cron expression")
	}

	// Valid update with cron_expression, custom timezone, low effort, title prefix, prompt
	effLow := "low"
	cronExpr := "0 12 * * *"
	prompt := "updated prompt"
	prefix := "updated prefix"
	tz := "America/New_York"
	res, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID:     schedID,
		Effort:         &effLow,
		CronExpression: &cronExpr,
		Prompt:         &prompt,
		TitlePrefix:    &prefix,
		Timezone:       &tz,
	})
	if err != nil {
		t.Fatalf("UpdateCronSchedule failed: %v", err)
	}
	if res.Status != "success" || res.Effort != "low" || res.NextRunAt == "" {
		t.Errorf("unexpected update response: %+v", res)
	}

	// Valid update with cron_expression without timezone (fallback to handler timezone) and effort != "low" ("HIGH")
	effHigh := "HIGH"
	cronExpr2 := "0 14 * * *"
	res2, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID:     schedID,
		Effort:         &effHigh,
		CronExpression: &cronExpr2,
	})
	if err != nil {
		t.Fatalf("UpdateCronSchedule failed: %v", err)
	}
	if res2.Effort != "high" {
		t.Errorf("expected effort high, got %v", res2.Effort)
	}

	// Update non-existent schedule -> db error
	if _, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID: "nonexistent",
		Prompt:     &prompt,
	}); err == nil {
		t.Error("expected error when updating nonexistent schedule")
	}
}

func TestTools_ListSchedules_EmptyAndError(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)
	ctx := context.Background()

	// Empty database -> crons == nil and oneShots == nil branches executed
	res, err := h.ListSchedules(ctx, ListSchedulesArgs{})
	if err != nil {
		t.Fatalf("ListSchedules failed on empty db: %v", err)
	}
	if len(res.Recurring) != 0 || len(res.OneShot) != 0 {
		t.Errorf("expected empty slices, got %+v", res)
	}

	// Drop one_shot_schedules table so ListCronSchedules succeeds but ListOneShotSchedules fails
	if _, err := db.Exec("DROP TABLE one_shot_schedules;"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
	if _, err := h.ListSchedules(ctx, ListSchedulesArgs{}); err == nil {
		t.Error("expected error when one_shot_schedules table is dropped")
	}
}

// -------------------------------------------------------------
// 3. db.go coverage
// -------------------------------------------------------------
func TestDB_ErrorBranchesAndEdgeCases(t *testing.T) {
	// initDB empty DSN
	if _, err := initDB(""); err == nil {
		t.Error("expected error on empty DSN")
	}

	// initDB directory creation error (hermetic across Windows and POSIX)
	tmpDir := t.TempDir()
	blockerFile := filepath.Join(tmpDir, "blocker.txt")
	if err := os.WriteFile(blockerFile, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to write blocker file: %v", err)
	}
	if _, err := initDB(filepath.Join(blockerFile, "forbidden", "db.sqlite")); err == nil {
		t.Error("expected error creating dir inside a regular file")
	}

	// initDB sqlite without ? and without _pragma
	freshDB := filepath.Join(tmpDir, "fresh.db")
	db1, err := initDB(freshDB)
	if err != nil {
		t.Fatalf("initDB fresh.db failed: %v", err)
	}
	_ = db1.Close()

	// initDB sqlite with ? but without _pragma
	freshDB2 := filepath.Join(tmpDir, "fresh2.db") + "?cache=shared"
	db2, err := initDB(freshDB2)
	if err != nil {
		t.Fatalf("initDB fresh2.db failed: %v", err)
	}
	_ = db2.Close()

	// initDB schema error when conflicting view exists
	viewDBPath := filepath.Join(tmpDir, "view_conflict.db")
	vdb, err := sql.Open("sqlite", viewDBPath)
	if err != nil {
		t.Fatalf("failed to open view db: %v", err)
	}
	if _, err := vdb.Exec("CREATE VIEW cron_schedules AS SELECT 1 AS id;"); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}
	_ = vdb.Close()

	if _, err := initDB(viewDBPath); err == nil {
		t.Error("expected error creating schema on conflicting view database")
	}
}

func TestDB_UpdateCronSchedule_Branches(t *testing.T) {
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Insert low effort schedule
	sched := CronSchedule{
		ID:        "test-cron-1",
		TargetID:  "chan-1",
		CronExpr:  "0 0 * * *",
		Prompt:    "prompt 1",
		Timezone:  "America/Los_Angeles",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
		Effort:    "low",
	}
	if err := InsertCronSchedule(ctx, db, sched); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	// len(sets) == 0 (no updates)
	if err := UpdateCronSchedule(ctx, db, "test-cron-1", nil, nil, nil, nil, nil, nil); err != nil {
		t.Errorf("expected nil error on empty sets, got %v", err)
	}

	// Update with all fields non-nil
	eff := "low"
	cronExpr := "0 1 * * *"
	prompt := "new prompt"
	prefix := "new prefix"
	tz := "America/Chicago"
	nextRun := time.Now().UTC().Add(time.Hour)
	if err := UpdateCronSchedule(ctx, db, "test-cron-1", &eff, &cronExpr, &prompt, &prefix, &tz, &nextRun); err != nil {
		t.Fatalf("UpdateCronSchedule failed: %v", err)
	}

	// Update with effort != low (high)
	effHigh := "high"
	if err := UpdateCronSchedule(ctx, db, "test-cron-1", &effHigh, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("UpdateCronSchedule failed: %v", err)
	}

	// Schedule not found
	if err := UpdateCronSchedule(ctx, db, "non-existent-cron", &effHigh, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error when updating nonexistent schedule")
	}

	// DB error on closed db
	closedDB, _ := sql.Open("sqlite", ":memory:")
	_ = closedDB.Close()
	if err := UpdateCronSchedule(ctx, closedDB, "test-cron-1", &effHigh, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error on closed db")
	}
}

func TestUpdateCronSchedule_EmptyID(t *testing.T) {
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := UpdateCronSchedule(ctx, db, "   ", nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error for empty schedule ID")
	}
}

func TestDB_ListAndScanErrors(t *testing.T) {
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Insert valid data with Enabled: true
	_ = InsertCronSchedule(ctx, db, CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "* * * * *", Prompt: "p", NextRunAt: time.Now(), Enabled: true})
	_ = InsertOneShotSchedule(ctx, db, OneShotSchedule{ID: "o1", ThreadID: "t1", Prompt: "p", RunAt: time.Now()})

	// ListCronSchedules scan error: update next_run_at to unparseable string
	if _, err := db.Exec("UPDATE cron_schedules SET next_run_at = 'not-a-date';"); err == nil {
		if _, err := ListCronSchedules(ctx, db, ""); err == nil {
			t.Error("expected scan error on invalid next_run_at")
		}
	}

	// ListOneShotSchedules scan error: update run_at to unparseable string
	if _, err := db.Exec("UPDATE one_shot_schedules SET run_at = 'not-a-date';"); err == nil {
		if _, err := ListOneShotSchedules(ctx, db, ""); err == nil {
			t.Error("expected scan error on invalid run_at")
		}
	}

	// ListOneShotSchedules query error: drop table
	if _, err := db.Exec("DROP TABLE one_shot_schedules;"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
	if _, err := ListOneShotSchedules(ctx, db, ""); err == nil {
		t.Error("expected query error when table dropped")
	}

	// DeleteSchedule error on second query: since one_shot_schedules is dropped, resOneShot returns error!
	if _, err := DeleteSchedule(ctx, db, "c1"); err == nil {
		t.Error("expected error in DeleteSchedule when one_shot_schedules is dropped")
	}
}

// -------------------------------------------------------------
// 4. server.go coverage
// -------------------------------------------------------------
type errorReader struct{}

func (errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated body read error")
}

func TestServer_ProcessRequest_AndEdgeCases(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	// handleMCP GET /health path
	reqHealth, _ := http.NewRequest(http.MethodGet, ts.URL+"/health", nil)
	respHealth, err := http.DefaultClient.Do(reqHealth)
	if err != nil || respHealth.StatusCode != http.StatusOK {
		t.Fatalf("handleMCP /health failed: %v", err)
	}
	_ = respHealth.Body.Close()

	// handleMCP body read error via direct handler invocation with httptest
	rec := httptest.NewRecorder()
	reqErr := httptest.NewRequest(http.MethodPost, "/mcp", errorReader{})
	server.handleMCP(rec, reqErr)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for read error, got %d", rec.Code)
	}

	// handleMCP empty body
	recEmpty := httptest.NewRecorder()
	reqEmpty := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("   "))
	server.handleMCP(recEmpty, reqEmpty)
	if recEmpty.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty body, got %d", recEmpty.Code)
	}

	// handleMCP method not allowed
	recMethod := httptest.NewRecorder()
	reqMethod := httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	server.handleMCP(recMethod, reqMethod)
	if recMethod.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for DELETE, got %d", recMethod.Code)
	}
}

func TestServer_HandleMCP_HealthDirect(t *testing.T) {
	server, _ := setupTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	server.handleMCP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for handleMCP /health, got %d", rec.Code)
	}
}

type mockErrWriter struct {
	header http.Header
	err    error
}

func (m *mockErrWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}

func (m *mockErrWriter) Write(p []byte) (int, error) {
	return 0, m.err
}

func (m *mockErrWriter) WriteHeader(statusCode int) {}

func TestServer_WriteResponseAndDisconnect(t *testing.T) {
	// 1. Successful write
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	writeResponse(rec, req, []byte("ok"))
	if rec.Body.String() != "ok" {
		t.Errorf("expected ok, got %s", rec.Body.String())
	}

	// 2. isClientDisconnect with nil error
	if isClientDisconnect(req, nil) {
		t.Errorf("expected false for nil error")
	}

	// 3. Client context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(ctx)
	if !isClientDisconnect(reqCanceled, errors.New("write err")) {
		t.Errorf("expected true for canceled context")
	}

	// 4. EPIPE error
	if !isClientDisconnect(req, syscall.EPIPE) {
		t.Errorf("expected true for EPIPE")
	}

	// 5. ECONNRESET error
	if !isClientDisconnect(req, syscall.ECONNRESET) {
		t.Errorf("expected true for ECONNRESET")
	}

	// 6. net.ErrClosed error
	if !isClientDisconnect(req, net.ErrClosed) {
		t.Errorf("expected true for net.ErrClosed")
	}

	// 7. Generic error write
	mockGeneric := &mockErrWriter{err: errors.New("disk full")}
	writeResponse(mockGeneric, req, []byte("data"))
}

type errReadCloser struct{}

func (e *errReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (e *errReadCloser) Close() error {
	return errors.New("simulated close error")
}

func TestCloseWarn(t *testing.T) {
	// 1. nil closer
	closeWarn(nil, "nil closer")

	// 2. valid closer
	closeWarn(io.NopCloser(strings.NewReader("")), "valid closer")

	// 3. error closer
	closeWarn(&errReadCloser{}, "error closer")
}


func TestDB_UnsupportedScheme(t *testing.T) {
	_, err := initDB("unsupported://foo")
	if err == nil {
		t.Errorf("expected error on unsupported scheme")
	}
}


