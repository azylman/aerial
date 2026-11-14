package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestPureHelpers_BuildHeartbeatPrompt(t *testing.T) {
	completedTime := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		schedule     HeartbeatSchedule
		wantContains []string
	}{
		{
			name: "default title when empty with initial watermark",
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
				"<HEARTBEAT_WATERMARK>",
				"Last Successful Check Started At: none (initial run)",
				"Watermark Rule: Only inspect events, logs, metrics, or alerts that occurred AFTER Last Successful Check Started At",
				"</HEARTBEAT_WATERMARK>",
			},
		},
		{
			name: "custom title prefix and trimmed fields with last_completed_at watermark",
			schedule: HeartbeatSchedule{
				ID:              "hb-2",
				TargetID:        " 9876543210 ",
				TitlePrefix:     " Disk Space Alert ",
				Prompt:          "  Check high disk usage  ",
				LastCompletedAt: &completedTime,
			},
			wantContains: []string{
				"Target Discord Channel ID for Escalations: 9876543210",
				`channel_id "9876543210"`,
				`title "Disk Space Alert"`,
				"Check high disk usage",
				"<HEARTBEAT_WATERMARK>",
				"Last Successful Check Started At: 2026-11-14T01:00:00Z (1794618000)",
				"Current Check Started At:",
				"</HEARTBEAT_WATERMARK>",
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

func testHeartbeatInsertAndGet(ctx context.Context, t *testing.T, db *sql.DB, now time.Time) HeartbeatSchedule {
	t.Helper()
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

	got, err := GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule(hb-op-1) failed: %v", err)
	}
	if got.ID != "hb-op-1" || got.TargetID != "chan-1" || got.Effort != "low" {
		t.Errorf("GetHeartbeatSchedule got %+v", got)
	}

	if _, err := GetHeartbeatSchedule(ctx, db, "non-existent"); err == nil {
		t.Error("expected error for non-existent heartbeat")
	}
	if _, err := GetHeartbeatSchedule(ctx, db, "  "); err == nil {
		t.Error("expected error for empty id")
	}
	return hb1
}

func testHeartbeatListAndUpdate(ctx context.Context, t *testing.T, db *sql.DB, hb1 HeartbeatSchedule) {
	t.Helper()
	allHBs, err := ListHeartbeatSchedules(ctx, db, "")
	if err != nil || len(allHBs) != 2 {
		t.Fatalf("ListHeartbeatSchedules all failed: err=%v, count=%d", err, len(allHBs))
	}

	filteredHBs, err := ListHeartbeatSchedules(ctx, db, "chan-1")
	if err != nil || len(filteredHBs) != 1 || filteredHBs[0].ID != "hb-op-1" {
		t.Fatalf("ListHeartbeatSchedules(chan-1) failed: err=%v, items=%+v", err, filteredHBs)
	}

	hb1.TitlePrefix = "Updated Title"
	hb1.Effort = "high"
	if err := UpdateHeartbeatSchedule(ctx, db, hb1); err != nil {
		t.Fatalf("UpdateHeartbeatSchedule failed: %v", err)
	}
	updated, err := GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if err != nil || updated.TitlePrefix != "Updated Title" || updated.Effort != "high" {
		t.Fatalf("updated heartbeat error=%v, got %+v", err, updated)
	}

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
}

func testHeartbeatDueAdvanceDelete(ctx context.Context, t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	due, err := GetDueHeartbeatSchedules(ctx, db, now.Add(20*time.Minute))
	if err != nil || len(due) != 1 || due[0].ID != "hb-op-2" {
		t.Fatalf("GetDueHeartbeatSchedules failed: err=%v, got %+v", err, due)
	}

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

	deleted, err := DeleteSchedule(ctx, db, "hb-op-1")
	if err != nil || !deleted {
		t.Fatalf("DeleteSchedule(hb-op-1) failed: deleted=%v, err=%v", deleted, err)
	}
	if _, err := GetHeartbeatSchedule(ctx, db, "hb-op-1"); err == nil {
		t.Error("expected hb-op-1 to be deleted")
	}
}

func TestHeartbeatDB_Operations(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

	hb1 := testHeartbeatInsertAndGet(ctx, t, db, now)
	testHeartbeatListAndUpdate(ctx, t, db, hb1)
	testHeartbeatDueAdvanceDelete(ctx, t, db, now)
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

type evaluateDueHeartbeatsTestCase struct {
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
}

func assertHeartbeatDispatch(t *testing.T, mock *mockDispatcher, tc evaluateDueHeartbeatsTestCase) {
	t.Helper()
	if !tc.wantDispatched {
		if len(mock.dispatchedRequests) > 0 {
			t.Errorf("dispatcher should NOT be called for %s, called %d times", tc.hbID, len(mock.dispatchedRequests))
		}
		return
	}
	if len(mock.dispatchedRequests) == 0 {
		t.Errorf("expected dispatcher to be called for %s", tc.hbID)
		return
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
}

func assertHeartbeatScheduleRuns(t *testing.T, db *sql.DB, tc evaluateDueHeartbeatsTestCase) {
	t.Helper()
	var runCount int
	var runStatus, runPrompt, runType string
	_ = db.QueryRow("SELECT COUNT(*), COALESCE(MAX(status), ''), COALESCE(MAX(prompt), ''), COALESCE(MAX(schedule_type), '') FROM schedule_runs WHERE schedule_id = ?", tc.hbID).Scan(&runCount, &runStatus, &runPrompt, &runType)

	if tc.wantSkippedRun && runCount != 0 {
		t.Errorf("expected 0 runs for skipped active heartbeat %s, got count=%d", tc.hbID, runCount)
	}
	if tc.wantStaleRun && (runCount == 0 || runStatus != "failed") {
		t.Errorf("expected stale failed run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
	}
	if tc.wantFailedRun && (runCount == 0 || runStatus != "failed") {
		t.Errorf("expected failed run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
	}
	if tc.wantDispatched && tc.dispatcherErr == nil && !tc.wantSkippedRun && (runCount == 0 || runStatus != "running") {
		t.Errorf("expected running run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
	}
	if runCount > 0 {
		if runType != "heartbeat" {
			t.Errorf("expected schedule_type='heartbeat', got %s", runType)
		}
		if strings.Contains(runPrompt, `HEARTBEAT_INSTRUCTIONS`) {
			t.Errorf("schedule_runs.prompt must NOT contain HEARTBEAT_INSTRUCTIONS, got:\n%s", runPrompt)
		}
	}
}

func assertHeartbeatNextRunAdvanced(ctx context.Context, t *testing.T, db *sql.DB, tc evaluateDueHeartbeatsTestCase) {
	t.Helper()
	after, err := GetHeartbeatSchedule(ctx, db, tc.hbID)
	if err != nil {
		t.Fatalf("failed to reload heartbeat %s: %v", tc.hbID, err)
	}
	if !after.NextRunAt.After(tc.nextRunAt) {
		t.Errorf("next_run_at was not advanced: before=%v, after=%v", tc.nextRunAt, after.NextRunAt)
	}
}

func runEvaluateDueHeartbeatsCase(ctx context.Context, t *testing.T, tc evaluateDueHeartbeatsTestCase, now time.Time) {
	t.Helper()
	db := setupTestDB(t)

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

	assertHeartbeatDispatch(t, mock, tc)
	assertHeartbeatScheduleRuns(t, db, tc)
	assertHeartbeatNextRunAdvanced(ctx, t, db, tc)
}

func TestTicker_EvaluateDueHeartbeats_Scenarios(t *testing.T) {
	now := time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)

	tests := []evaluateDueHeartbeatsTestCase{
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

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runEvaluateDueHeartbeatsCase(ctx, t, tc, now)
		})
	}
}
