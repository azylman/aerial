package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/services/haro/internal/store"
)

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
	if err := store.InsertHeartbeatSchedule(ctx, db, hb1); err != nil {
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
	if err := store.InsertHeartbeatSchedule(ctx, db, hb2); err != nil {
		t.Fatalf("InsertHeartbeatSchedule defaults failed: %v", err)
	}

	got, err := store.GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if err != nil {
		t.Fatalf("store.GetHeartbeatSchedule(hb-op-1) failed: %v", err)
	}
	if got.ID != "hb-op-1" || got.TargetID != "chan-1" || got.Effort != "low" {
		t.Errorf("GetHeartbeatSchedule got %+v", got)
	}

	if _, err := store.GetHeartbeatSchedule(ctx, db, "non-existent"); err == nil {
		t.Error("expected error for non-existent heartbeat")
	}
	if _, err := store.GetHeartbeatSchedule(ctx, db, "  "); err == nil {
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
	if err = store.UpdateHeartbeatSchedule(ctx, db, hb1); err != nil {
		t.Fatalf("UpdateHeartbeatSchedule failed: %v", err)
	}
	updated, err := store.GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if err != nil || updated.TitlePrefix != "Updated Title" || updated.Effort != "high" {
		t.Fatalf("updated heartbeat error=%v, got %+v", err, updated)
	}

	hbMissing := hb1
	hbMissing.ID = "missing"
	if err := store.UpdateHeartbeatSchedule(ctx, db, hbMissing); err == nil {
		t.Error("expected error updating missing heartbeat")
	}
	hbEmptyID := hb1
	hbEmptyID.ID = ""
	if err := store.UpdateHeartbeatSchedule(ctx, db, hbEmptyID); err == nil {
		t.Error("expected error updating empty id")
	}
	hbBadEffort := hb1
	hbBadEffort.Effort = "bad-tier"
	if err := store.UpdateHeartbeatSchedule(ctx, db, hbBadEffort); err == nil {
		t.Error("expected error updating with invalid effort")
	}
}

func testHeartbeatDueAdvanceDelete(ctx context.Context, t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	due, err := store.GetDueHeartbeatSchedules(ctx, db, now.Add(20*time.Minute))
	if err != nil || len(due) != 1 || due[0].ID != "hb-op-2" {
		t.Fatalf("GetDueHeartbeatSchedules failed: err=%v, got %+v", err, due)
	}

	nextTime := now.Add(2 * time.Hour)
	if err = store.AdvanceHeartbeatNextRun(ctx, db, "hb-op-1", nextTime); err != nil {
		t.Fatalf("AdvanceHeartbeatNextRun failed: %v", err)
	}
	advanced, _ := store.GetHeartbeatSchedule(ctx, db, "hb-op-1")
	if !advanced.NextRunAt.Equal(nextTime) {
		t.Errorf("advanced NextRunAt = %v, want %v", advanced.NextRunAt, nextTime)
	}
	if err = store.AdvanceHeartbeatNextRun(ctx, db, "missing", nextTime); err == nil {
		t.Error("expected error advancing missing heartbeat")
	}
	if err = store.AdvanceHeartbeatNextRun(ctx, db, "  ", nextTime); err == nil {
		t.Error("expected error advancing empty id")
	}

	deleted, err := store.DeleteSchedule(ctx, db, "hb-op-1")
	if err != nil || !deleted {
		t.Fatalf("store.DeleteSchedule(hb-op-1) failed: deleted=%v, err=%v", deleted, err)
	}
	if _, err := store.GetHeartbeatSchedule(ctx, db, "hb-op-1"); err == nil {
		t.Error("expected hb-op-1 to be deleted")
	}
}

func TestHeartbeatDB_Operations(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

	hb1 := testHeartbeatInsertAndGet(ctx, t, db, now)
	testHeartbeatListAndUpdate(ctx, t, db, hb1)
	testHeartbeatDueAdvanceDelete(ctx, t, db, now)

	hasRunningEmpty, err := store.HasRunningHeartbeatRun(ctx, db, "", now)
	if err != nil || hasRunningEmpty {
		t.Errorf("HasRunningHeartbeatRun with empty scheduleID got (%v, %v), want (false, nil)", hasRunningEmpty, err)
	}

	// Invalid effort falls back to low on heartbeat insert
	hbInvalid := HeartbeatSchedule{ID: "hb-invalid-effort", TargetID: "chan-1", CronExpr: "0 * * * *", Prompt: "p", Timezone: "UTC", Effort: "bogus", NextRunAt: now, Enabled: true, CreatedAt: now}
	if err = store.InsertHeartbeatSchedule(ctx, db, hbInvalid); err != nil {
		t.Fatalf("InsertHeartbeatSchedule invalid effort failed: %v", err)
	}
	gotHB, err := store.GetHeartbeatSchedule(ctx, db, "hb-invalid-effort")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	if gotHB.Effort != "low" {
		t.Errorf("invalid effort fallback = %q, want %q", gotHB.Effort, "low")
	}
}

func TestHeartbeatTools_HandleScheduleHeartbeat(t *testing.T) {
	db := setupTestDB(t.Context(), t)
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
	db := setupTestDB(t.Context(), t)
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
	tc, ok := seedRes.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("seed result content is %T, want *mcp.TextContent", seedRes.Content[0])
	}
	if err := json.Unmarshal([]byte(tc.Text), &seedMap); err != nil {
		t.Fatalf("unmarshal seed map failed: %v", err)
	}
	hbID, ok := seedMap["schedule_id"].(string)
	if !ok {
		t.Fatalf("schedule_id is %T, want string", seedMap["schedule_id"])
	}

	// Seed a heartbeat with a stored cron expression that no longer parses,
	// bypassing tool validation via direct insert.
	badCronHB := HeartbeatSchedule{ID: "hb-bad-cron", TargetID: "1562847268833337377", CronExpr: "bad cron", Prompt: "p", Timezone: "UTC", Effort: "low", NextRunAt: time.Now().UTC(), Enabled: true, CreatedAt: time.Now().UTC()}
	if err := store.InsertHeartbeatSchedule(ctx, db, badCronHB); err != nil {
		t.Fatalf("failed to seed bad-cron heartbeat: %v", err)
	}

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
			name:    "recompute fails on stored invalid cron",
			args:    map[string]any{"id": "hb-bad-cron", "timezone": "UTC"},
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
	db := setupTestDB(t.Context(), t)
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
