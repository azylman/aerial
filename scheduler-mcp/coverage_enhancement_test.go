package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// -------------------------------------------------------------
// 1. config.go coverage
// -------------------------------------------------------------
func TestConfig_NewAndLoadCoverage(t *testing.T) {
	// NewConfig with empty fallbacks
	c1 := NewConfig("file:mem_sched_mcp?mode=memory&cache=shared", "", "")
	if c1.Timezone != DefaultTimezone || c1.Port != DefaultPort {
		t.Errorf("expected default tz and port, got tz=%s, port=%s", c1.Timezone, c1.Port)
	}

	// NewConfig explicit
	c2 := NewConfig("file:mem_sched_mcp?mode=memory&cache=shared", "America/Denver", "9099")
	if c2.Timezone != "America/Denver" || c2.Port != "9099" {
		t.Errorf("expected custom tz and port, got tz=%s, port=%s", c2.Timezone, c2.Port)
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

func TestTools_HandleUpdateCronSchedule_AllBranches(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)

	// Malformed JSON
	if _, err := h.HandleUpdateCronSchedule([]byte("{bad")); err == nil {
		t.Error("expected error on malformed json")
	}

	// Empty schedule_id
	if _, err := h.HandleUpdateCronSchedule([]byte(`{"schedule_id":"  "}`)); err == nil {
		t.Error("expected error on empty schedule_id")
	}

	// Insert a schedule to update
	insertRes, err := h.HandleScheduleRecurring([]byte(`{
		"channel_id": "chan-test",
		"cron_expression": "0 10 * * *",
		"prompt": "original prompt",
		"effort": "high"
	}`))
	if err != nil {
		t.Fatalf("HandleScheduleRecurring failed: %v", err)
	}
	schedID := insertRes.(map[string]interface{})["schedule_id"].(string)

	// Invalid cron expression in update
	badCronPayload, _ := json.Marshal(map[string]string{
		"schedule_id":     schedID,
		"cron_expression": "invalid-cron",
	})
	if _, err := h.HandleUpdateCronSchedule(badCronPayload); err == nil {
		t.Error("expected error on invalid cron expression")
	}

	// Valid update with cron_expression, custom timezone, low effort, title prefix, prompt
	effLow := "low"
	cronExpr := "0 12 * * *"
	prompt := "updated prompt"
	prefix := "updated prefix"
	tz := "America/New_York"
	goodPayload, _ := json.Marshal(UpdateCronScheduleArgs{
		ScheduleID:     schedID,
		Effort:         &effLow,
		CronExpression: &cronExpr,
		Prompt:         &prompt,
		TitlePrefix:    &prefix,
		Timezone:       &tz,
	})
	res, err := h.HandleUpdateCronSchedule(goodPayload)
	if err != nil {
		t.Fatalf("HandleUpdateCronSchedule failed: %v", err)
	}
	resMap := res.(map[string]interface{})
	if resMap["status"] != "success" || resMap["effort"] != "low" || resMap["next_run_at"] == nil {
		t.Errorf("unexpected update response: %+v", resMap)
	}

	// Valid update with cron_expression without timezone (fallback to handler timezone) and effort != "low" ("HIGH")
	effHigh := "HIGH"
	cronExpr2 := "0 14 * * *"
	goodPayload2, _ := json.Marshal(UpdateCronScheduleArgs{
		ScheduleID:     schedID,
		Effort:         &effHigh,
		CronExpression: &cronExpr2,
	})
	res2, err := h.HandleUpdateCronSchedule(goodPayload2)
	if err != nil {
		t.Fatalf("HandleUpdateCronSchedule failed: %v", err)
	}
	resMap2 := res2.(map[string]interface{})
	if resMap2["effort"] != "high" {
		t.Errorf("expected effort high, got %v", resMap2["effort"])
	}

	// Update non-existent schedule -> db error
	nonExistPayload, _ := json.Marshal(UpdateCronScheduleArgs{
		ScheduleID: "nonexistent",
		Prompt:     &prompt,
	})
	if _, err := h.HandleUpdateCronSchedule(nonExistPayload); err == nil {
		t.Error("expected error when updating nonexistent schedule")
	}
}

func TestTools_HandleListSchedules_EmptyAndError(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	h := NewToolHandler(cfg, db)

	// Empty database -> crons == nil and oneShots == nil branches executed
	res, err := h.HandleListSchedules([]byte(`{}`))
	if err != nil {
		t.Fatalf("HandleListSchedules failed on empty db: %v", err)
	}
	resMap := res.(map[string]interface{})
	if len(resMap["recurring"].([]CronSchedule)) != 0 || len(resMap["one_shot"].([]OneShotSchedule)) != 0 {
		t.Errorf("expected empty slices, got %+v", resMap)
	}

	// Drop one_shot_schedules table so ListCronSchedules succeeds but ListOneShotSchedules fails
	if _, err := db.Exec("DROP TABLE one_shot_schedules;"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
	if _, err := h.HandleListSchedules(nil); err == nil {
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
	if err := InsertCronSchedule(db, sched); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	// len(sets) == 0 (no updates)
	if err := UpdateCronSchedule(db, "test-cron-1", nil, nil, nil, nil, nil, nil); err != nil {
		t.Errorf("expected nil error on empty sets, got %v", err)
	}

	// Update with all fields non-nil
	eff := "low"
	cronExpr := "0 1 * * *"
	prompt := "new prompt"
	prefix := "new prefix"
	tz := "America/Chicago"
	nextRun := time.Now().UTC().Add(time.Hour)
	if err := UpdateCronSchedule(db, "test-cron-1", &eff, &cronExpr, &prompt, &prefix, &tz, &nextRun); err != nil {
		t.Fatalf("UpdateCronSchedule failed: %v", err)
	}

	// Update with effort != low (high)
	effHigh := "high"
	if err := UpdateCronSchedule(db, "test-cron-1", &effHigh, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("UpdateCronSchedule failed: %v", err)
	}

	// Schedule not found
	if err := UpdateCronSchedule(db, "non-existent-cron", &effHigh, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error when updating nonexistent schedule")
	}

	// DB error on closed db
	closedDB, _ := sql.Open("sqlite", ":memory:")
	_ = closedDB.Close()
	if err := UpdateCronSchedule(closedDB, "test-cron-1", &effHigh, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error on closed db")
	}
}

func TestUpdateCronSchedule_EmptyID(t *testing.T) {
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()

	if err := UpdateCronSchedule(db, "   ", nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error for empty schedule ID")
	}
}

func TestDB_ListAndScanErrors(t *testing.T) {
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()

	// Insert valid data with Enabled: true
	_ = InsertCronSchedule(db, CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "* * * * *", Prompt: "p", NextRunAt: time.Now(), Enabled: true})
	_ = InsertOneShotSchedule(db, OneShotSchedule{ID: "o1", ThreadID: "t1", Prompt: "p", RunAt: time.Now()})

	// ListCronSchedules scan error: update next_run_at to unparseable string
	if _, err := db.Exec("UPDATE cron_schedules SET next_run_at = 'not-a-date';"); err == nil {
		if _, err := ListCronSchedules(db, ""); err == nil {
			t.Error("expected scan error on invalid next_run_at")
		}
	}

	// ListOneShotSchedules scan error: update run_at to unparseable string
	if _, err := db.Exec("UPDATE one_shot_schedules SET run_at = 'not-a-date';"); err == nil {
		if _, err := ListOneShotSchedules(db, ""); err == nil {
			t.Error("expected scan error on invalid run_at")
		}
	}

	// ListOneShotSchedules query error: drop table
	if _, err := db.Exec("DROP TABLE one_shot_schedules;"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
	if _, err := ListOneShotSchedules(db, ""); err == nil {
		t.Error("expected query error when table dropped")
	}

	// DeleteSchedule error on second query: since one_shot_schedules is dropped, resOneShot returns error!
	if _, err := DeleteSchedule(db, "c1"); err == nil {
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

	// processRequest tools/call invalid params JSON
	respInvParams := server.processRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      100,
		Method:  "tools/call",
		Params:  json.RawMessage(`"not an object"`),
	})
	if respInvParams == nil || respInvParams.Error == nil || respInvParams.Error.Code != -32602 {
		t.Errorf("expected -32602 for invalid params, got %+v", respInvParams)
	}

	// processRequest notification with unknown method without ID
	respUnknownNotif := server.processRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "custom/unknown_notification",
	})
	if respUnknownNotif != nil {
		t.Errorf("expected nil response for unknown notification without ID, got %+v", respUnknownNotif)
	}

	// processRequest tools/call execution error (callErr != nil)
	respToolErr := server.processRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      101,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"schedule_recurring","arguments":{"channel_id":""}}`),
	})
	if respToolErr == nil || respToolErr.Result == nil {
		t.Fatalf("expected tool error result, got %+v", respToolErr)
	}
	resMap := respToolErr.Result.(map[string]interface{})
	if resMap["isError"] != true {
		t.Errorf("expected isError true, got %+v", resMap)
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
