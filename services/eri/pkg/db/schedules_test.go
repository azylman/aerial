package db

import (
	"context"
	"testing"
	"time"
)

func TestSQLStore_HeartbeatSchedules(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	// Seed heartbeat schedules
	_, err := database.ExecContext(ctx, `INSERT INTO heartbeat_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, effort, next_run_at, enabled, created_at) VALUES 
		('hb-1', 'chan-1', 'HB 1', '*/5 * * * *', 'Prompt 1', 'UTC', 'low', $1, true, $2),
		('hb-2', 'chan-1', 'HB 2', '*/10 * * * *', 'Prompt 2', 'UTC', 'medium', $3, false, $4),
		('hb-3', 'chan-2', 'HB 3', '0 * * * *', 'Prompt 3', 'America/Los_Angeles', 'high', $5, true, $6)`,
		now.Add(5*time.Minute), now.Add(-10*time.Minute),
		now.Add(10*time.Minute), now.Add(-5*time.Minute),
		now.Add(15*time.Minute), now.Add(-2*time.Minute),
	)
	mustOK(t, err, "insert heartbeat schedules")

	tests := []struct {
		name      string
		targetID  string
		wantCount int
		wantFirst string
	}{
		{
			name:      "filter by target chan-1 only enabled",
			targetID:  "chan-1",
			wantCount: 1,
			wantFirst: "hb-1",
		},
		{
			name:      "filter by target chan-2",
			targetID:  "chan-2",
			wantCount: 1,
			wantFirst: "hb-3",
		},
		{
			name:      "all enabled heartbeats",
			targetID:  "",
			wantCount: 2,
			wantFirst: "hb-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.GetAllHeartbeatSchedules(ctx, tc.targetID)
			if err != nil {
				t.Fatalf("GetAllHeartbeatSchedules(%q) unexpected err: %v", tc.targetID, err)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("GetAllHeartbeatSchedules(%q) count = %d, want %d", tc.targetID, len(got), tc.wantCount)
			}
			if tc.wantCount > 0 && got[0].ID != tc.wantFirst {
				t.Errorf("GetAllHeartbeatSchedules(%q) first ID = %q, want %q", tc.targetID, got[0].ID, tc.wantFirst)
			}
		})
	}
}

func TestSQLStore_GetScheduleSummaryMetrics_WithHeartbeats(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	// Seed cron, one-shot, and heartbeat schedules
	_, err := database.ExecContext(ctx, `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES 
		('cron-1', 'chan-1', 'Cron', '0 6 * * *', 'Prompt', 'UTC', $1, true, $2, 'low')`,
		now.Add(10*time.Minute), now,
	)
	mustOK(t, err, "insert cron")

	_, err = database.ExecContext(ctx, `INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES 
		('os-1', 'th-1', 'Prompt', $1, $2, 'low')`,
		now.Add(20*time.Minute), now,
	)
	mustOK(t, err, "insert one-shot")

	_, err = database.ExecContext(ctx, `INSERT INTO heartbeat_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, effort, next_run_at, enabled, created_at) VALUES 
		('hb-1', 'chan-1', 'HB', '*/5 * * * *', 'Prompt', 'UTC', 'low', $1, true, $2)`,
		now.Add(5*time.Minute), now,
	)
	mustOK(t, err, "insert heartbeat")

	metrics, err := store.GetScheduleSummaryMetrics(ctx)
	if err != nil {
		t.Fatalf("GetScheduleSummaryMetrics error: %v", err)
	}

	if metrics.CronCount != 1 {
		t.Errorf("CronCount = %d, want 1", metrics.CronCount)
	}
	if metrics.HeartbeatCount != 1 {
		t.Errorf("HeartbeatCount = %d, want 1", metrics.HeartbeatCount)
	}
	if metrics.OneShotCount != 1 {
		t.Errorf("OneShotCount = %d, want 1", metrics.OneShotCount)
	}
	if metrics.TotalActive != 3 {
		t.Errorf("TotalActive = %d, want 3", metrics.TotalActive)
	}
	if metrics.NextRunAt == nil || !metrics.NextRunAt.Equal(now.Add(5*time.Minute)) {
		t.Errorf("NextRunAt = %v, want %v", metrics.NextRunAt, now.Add(5*time.Minute))
	}
}
