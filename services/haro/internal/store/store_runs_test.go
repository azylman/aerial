package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/haro/internal/model"
)

func verifyScheduleRun(ctx context.Context, t *testing.T, db *sql.DB, id, wantEffort, wantStatus string, wantEcho bool) {
	t.Helper()
	got, err := getRunByID(ctx, db, id)
	if err != nil {
		t.Fatalf("getRunByID err = %v", err)
	}
	if got.Effort != wantEffort {
		t.Errorf("Effort = %q, want %q", got.Effort, wantEffort)
	}
	if got.Status != wantStatus {
		t.Errorf("Status = %q, want %q", got.Status, wantStatus)
	}
	if got.StartedAt.IsZero() {
		t.Error("StartedAt is zero, want defaulted")
	}
	if wantEcho && (got.MessageID != "m" || got.Status != "completed" || got.DurationMs != 42 || got.Model != "m1") {
		t.Errorf("getRunByID = %+v, want echoed fields", got)
	}
}

func TestCreateScheduleRun(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name       string
		seedRow    model.ScheduleRun
		row        model.ScheduleRun
		nilDB      bool
		wantErr    string
		wantEffort string
		wantStatus string
		wantEcho   bool
	}{
		{"full row round trips", model.ScheduleRun{}, model.ScheduleRun{ID: "r-1", ScheduleID: "c-1", ScheduleType: "cron", MessageID: "m", TargetID: "t", ThreadID: "th", Title: "title", Prompt: "p", Status: "completed", StartedAt: now, DurationMs: 42, Error: "", Effort: "HIGH", Model: "m1"}, false, "", "high", "completed", true},
		{"idempotent retry updates row", model.ScheduleRun{ID: "r-dup", ScheduleID: "c-1", ScheduleType: "cron", Status: "running", StartedAt: now}, model.ScheduleRun{ID: "r-dup", ScheduleID: "c-1", ScheduleType: "cron", MessageID: "m", TargetID: "t", ThreadID: "th", Title: "title", Prompt: "p", Status: "completed", StartedAt: now, DurationMs: 42, Error: "", Effort: "HIGH", Model: "m1"}, false, "", "high", "completed", true},
		{"defaults applied", model.ScheduleRun{}, model.ScheduleRun{ID: "r-2", ScheduleID: "c-1", ScheduleType: "cron", Status: "failed", Effort: "bogus"}, false, "", "medium", "failed", false},
		{"empty id errors", model.ScheduleRun{}, model.ScheduleRun{}, false, "run id cannot be empty", "", "", false},
		{"nil db errors", model.ScheduleRun{}, model.ScheduleRun{ID: "r-3"}, true, "database is nil", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.seedRow.ID != "" && database != nil {
				if err := CreateScheduleRun(ctx, database, tc.seedRow); err != nil {
					t.Fatalf("seed CreateScheduleRun err = %v", err)
				}
			}
			if err := CreateScheduleRun(ctx, database, tc.row); err != nil {
				if tc.wantErr == "" {
					t.Fatalf("CreateScheduleRun err = %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("CreateScheduleRun err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if tc.wantErr != "" {
				t.Fatalf("CreateScheduleRun err = nil, want %q", tc.wantErr)
			}
			verifyScheduleRun(ctx, t, db, tc.row.ID, tc.wantEffort, tc.wantStatus, tc.wantEcho)
		})
	}
}

// getRunByID reads one schedule run by its row ID through the production scanner.
func getRunByID(ctx context.Context, db *sql.DB, id string) (*model.ScheduleRun, error) {
	return getScheduleRunWhere(ctx, db, "run id", id, `SELECT `+scheduleRunColumns+` FROM schedule_runs WHERE id = $1`)
}

func TestGetScheduleRunByDispatchID(t *testing.T) {
	base := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	seed := []model.ScheduleRun{
		{ID: "11111111-1111-5111-8111-111111111111", ScheduleID: "hb-1", ScheduleType: "heartbeat", MessageID: "heartbeat:hb-1:2026-11-22T10:00:00Z", Status: "running", StartedAt: base},
		{ID: "22222222-2222-5222-8222-222222222222", ScheduleID: "hb-1", ScheduleType: "heartbeat", MessageID: "heartbeat:hb-1:2026-11-22T10:05:00Z", Status: "completed", StartedAt: base.Add(5 * time.Minute)},
		{ID: "33333333-3333-5333-8333-333333333333", ScheduleID: "hb-2", ScheduleType: "heartbeat", MessageID: "heartbeat:hb-2:2026-11-22T10:00:00Z", Status: "running", StartedAt: base},
		{ID: "44444444-4444-5444-8444-444444444444", ScheduleID: "hb-2", ScheduleType: "heartbeat", MessageID: "heartbeat:hb-2:2026-11-22T10:00:00Z", Status: "running", StartedAt: base.Add(time.Minute)},
	}
	tests := []struct {
		name       string
		dispatchID string
		nilDB      bool
		closedDB   bool
		wantID     string
		wantStatus string
		wantErr    string
	}{
		{"resolves uuid row by dispatch id", "heartbeat:hb-1:2026-11-22T10:00:00Z", false, false, "11111111-1111-5111-8111-111111111111", "running", ""},
		{"distinguishes due ticks of one schedule", "heartbeat:hb-1:2026-11-22T10:05:00Z", false, false, "22222222-2222-5222-8222-222222222222", "completed", ""},
		{"duplicate dispatch id picks newest run", " heartbeat:hb-2:2026-11-22T10:00:00Z ", false, false, "44444444-4444-5444-8444-444444444444", "running", ""},
		{"run row id is not a dispatch id", "11111111-1111-5111-8111-111111111111", false, false, "", "", "not found"},
		{"unknown dispatch id", "heartbeat:hb-9:2026-11-22T10:00:00Z", false, false, "", "", "not found"},
		{"empty dispatch id", "  ", false, false, "", "", "dispatch id cannot be empty"},
		{"closed db", "heartbeat:hb-1:2026-11-22T10:00:00Z", false, true, "", "", "database is closed"},
		{"nil db", "heartbeat:hb-1:2026-11-22T10:00:00Z", true, false, "", "", "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := openStoreTestDB(t)
			for _, r := range seed {
				if err := CreateScheduleRun(ctx, db, r); err != nil {
					t.Fatalf("CreateScheduleRun(%s): %v", r.ID, err)
				}
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			got, err := GetScheduleRunByDispatchID(ctx, database, tc.dispatchID)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("GetScheduleRunByDispatchID err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetScheduleRunByDispatchID err = %v", err)
			}
			if got.ID != tc.wantID || got.Status != tc.wantStatus {
				t.Errorf("got run %s/%s, want %s/%s", got.ID, got.Status, tc.wantID, tc.wantStatus)
			}
		})
	}
}

func TestHasRunningHeartbeatRun(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name       string
		scheduleID string
		since      time.Duration
		status     string
		seedType   string
		nilDB      bool
		closedDB   bool
		want       bool
		wantErr    string
	}{
		{"running recent run", "hb-1", time.Hour, "running", "heartbeat", false, false, true, ""},
		{"running run too old", "hb-1", -time.Hour, "running", "heartbeat", false, false, false, ""},
		{"completed run ignored", "hb-1", 48 * time.Hour, "completed", "heartbeat", false, false, false, ""},
		{"other type ignored", "hb-1", 48 * time.Hour, "running", "cron", false, false, false, ""},
		{"empty id false", "", time.Hour, "running", "heartbeat", false, false, false, ""},
		{"closed db errors", "hb-1", time.Hour, "running", "heartbeat", false, true, false, "database is closed"},
		{"nil db errors", "hb-1", time.Hour, "running", "heartbeat", true, false, false, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if !tc.nilDB && !tc.closedDB && tc.scheduleID != "" {
				mustCreateRun(ctx, t, db, model.ScheduleRun{ID: "r-1", ScheduleID: tc.scheduleID, ScheduleType: tc.seedType, Status: tc.status, StartedAt: now})
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			got, err := HasRunningHeartbeatRun(ctx, database, tc.scheduleID, now.Add(-tc.since))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("HasRunningHeartbeatRun err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("HasRunningHeartbeatRun err = %v", err)
			}
			if got != tc.want {
				t.Errorf("HasRunningHeartbeatRun = %v, want %v", got, tc.want)
			}
		})
	}
}

func seedOutcomeRun(ctx context.Context, t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	mustCreateRun(ctx, t, db, model.ScheduleRun{ID: "r-1", ScheduleID: "c-1", ScheduleType: "cron", Status: "running", StartedAt: now})
}

func verifyOutcomeRun(ctx context.Context, t *testing.T, db *sql.DB, completed time.Time) {
	t.Helper()
	got, err := getRunByID(ctx, db, "r-1")
	if err != nil {
		t.Fatalf("getRunByID err = %v", err)
	}
	if got.Status != "completed" || got.DurationMs != 60000 {
		t.Errorf("getRunByID = %+v, want completed/60000", got)
	}
	if got.CompletedAt == nil || !got.CompletedAt.Equal(completed) {
		t.Errorf("CompletedAt = %v, want %v", got.CompletedAt, completed)
	}
}

func TestUpdateScheduleRunOutcome(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name     string
		id       string
		nilDB    bool
		closedDB bool
		wantErr  string
	}{
		{"records outcome", "r-1", false, false, ""},
		{"unknown id not found", "missing", false, false, `schedule run schedule "missing" not found`},
		{"empty id errors", "", false, false, "schedule_id cannot be empty"},
		{"closed db errors", "r-1", false, true, "database is closed"},
		{"nil db errors", "r-1", true, false, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedOutcomeRun(ctx, t, db, now)
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			completed := now.Add(time.Minute)
			err := UpdateScheduleRunOutcome(ctx, database, tc.id, "completed", "", completed, 60000)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("UpdateScheduleRunOutcome err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateScheduleRunOutcome err = %v", err)
			}
			verifyOutcomeRun(ctx, t, db, completed)
		})
	}
}

func TestPruneScheduleRuns(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name      string
		maxCount  int
		maxAge    time.Duration
		wantKept  []string
		wantErr   string
		nilDB     bool
		closedDB  bool
		wantCount int64
	}{
		{"prunes old and over-count", 1, 24 * time.Hour, []string{"r-new"}, "", false, false, 2},
		{"keeps all within limits", 10, 24 * time.Hour, []string{"r-new", "r-mid"}, "", false, false, 1},
		{"closed db errors", 10, 24 * time.Hour, nil, "database is closed", false, true, 0},
		{"nil db errors", 10, 24 * time.Hour, nil, "database is nil", true, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedPruneRuns(ctx, t, db, now)
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			pruned, err := PruneScheduleRuns(ctx, database, tc.maxCount, tc.maxAge, now)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("PruneScheduleRuns err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PruneScheduleRuns err = %v", err)
			}
			if pruned != tc.wantCount {
				t.Errorf("PruneScheduleRuns pruned = %d, want %d", pruned, tc.wantCount)
			}
			verifyPruneKept(ctx, t, db, tc.wantKept)
		})
	}
}

func seedPruneRuns(ctx context.Context, t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	for id, started := range map[string]time.Time{"r-new": now, "r-mid": now.Add(-time.Hour), "r-old": now.Add(-48 * time.Hour)} {
		mustCreateRun(ctx, t, db, model.ScheduleRun{ID: id, ScheduleID: "c-1", ScheduleType: "cron", Status: "completed", StartedAt: started})
	}
}

func verifyPruneKept(ctx context.Context, t *testing.T, db *sql.DB, wantKept []string) {
	t.Helper()
	for _, id := range wantKept {
		if _, err := getRunByID(ctx, db, id); err != nil {
			t.Errorf("getRunByID(%q) err = %v, want kept", id, err)
		}
	}
}

func seedSummaryDB(ctx context.Context, t *testing.T, db *sql.DB, now, nextRun time.Time) {
	t.Helper()
	mustInsertCron(ctx, t, db, model.CronSchedule{ID: "c-1", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", Timezone: "UTC", NextRunAt: nextRun, Enabled: true})
	mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: "UTC", NextRunAt: nextRun.Add(time.Hour), Enabled: true})
	mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-1", ThreadID: "th", Prompt: "p", RunAt: nextRun.Add(2 * time.Hour)})
	for id, status := range map[string]string{"r-1": "completed", "r-2": "enqueued", "r-3": "failed"} {
		mustCreateRun(ctx, t, db, model.ScheduleRun{ID: id, ScheduleID: "c-1", ScheduleType: "cron", Status: status, StartedAt: now})
	}
	mustCreateRun(ctx, t, db, model.ScheduleRun{ID: "r-old", ScheduleID: "c-1", ScheduleType: "cron", Status: "completed", StartedAt: now.Add(-48 * time.Hour)})
}

func verifySummaryMetrics(t *testing.T, got model.ScheduleSummaryMetrics, wantActive, wantRuns int, wantSuccess float64, nextRun time.Time, wantNextRunNil bool) {
	t.Helper()
	if got.TotalActive != wantActive || got.CronCount+got.HeartbeatCount+got.OneShotCount != wantActive {
		t.Errorf("counts = %+v, want total active %d", got, wantActive)
	}
	if got.TotalRuns24h != wantRuns {
		t.Errorf("TotalRuns24h = %d, want %d", got.TotalRuns24h, wantRuns)
	}
	if got.SuccessRate24h != wantSuccess {
		t.Errorf("SuccessRate24h = %v, want %v", got.SuccessRate24h, wantSuccess)
	}
	if wantNextRunNil && got.NextRunAt != nil {
		t.Errorf("NextRunAt = %v, want nil", got.NextRunAt)
	}
	if !wantNextRunNil && wantActive > 0 && (got.NextRunAt == nil || !got.NextRunAt.Equal(nextRun)) {
		t.Errorf("NextRunAt = %v, want %v", got.NextRunAt, nextRun)
	}
}

func TestGetScheduleSummaryMetrics(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name           string
		seed           bool
		wantActive     int
		wantRuns       int
		wantSuccess    float64
		wantNextRunNil bool
	}{
		{"empty database", false, 0, 0, 0, true},
		{"counts schedules and runs", true, 3, 3, 66.7, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			nextRun := now.Add(time.Hour).UTC().Truncate(time.Second)
			if tc.seed {
				seedSummaryDB(ctx, t, db, now, nextRun)
			}
			got, err := GetScheduleSummaryMetrics(ctx, db)
			if err != nil {
				t.Fatalf("GetScheduleSummaryMetrics err = %v", err)
			}
			verifySummaryMetrics(t, got, tc.wantActive, tc.wantRuns, tc.wantSuccess, nextRun, tc.wantNextRunNil)
		})
	}
}

func TestGetScheduleSummaryMetricsErrors(t *testing.T) {
	tests := []struct {
		name      string
		dropTable string
		nilDB     bool
		wantErr   string
	}{
		{"nil db errors", "", true, "database is nil"},
		{"missing cron table", "cron_schedules", false, "failed to list cron schedules"},
		{"missing heartbeat table", "heartbeat_schedules", false, "failed to list heartbeat schedules"},
		{"missing oneshot table", "one_shot_schedules", false, "failed to list one-shot schedules"},
		{"missing runs table", "schedule_runs", false, "failed to query schedule runs for 24h metrics"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.dropTable != "" {
				if _, err := db.ExecContext(ctx, `DROP TABLE `+tc.dropTable); err != nil {
					t.Fatalf("drop table err = %v", err)
				}
			}
			_, err := GetScheduleSummaryMetrics(ctx, database)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("GetScheduleSummaryMetrics err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestGetScheduleRunsPaginated(t *testing.T) {
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name        string
		limit       int
		offset      int
		scheduleID  string
		status      string
		closedDB    bool
		nilDB       bool
		wantErr     string
		wantTotal   int
		wantCount   int
		wantFirstID string
	}{
		{"defaults return newest first", 0, -1, "", "", false, false, "", 3, 3, "r-3"},
		{"clamped limit returns all", 500, 0, "", "", false, false, "", 3, 3, "r-3"},
		{"offset skips newest", 50, 1, "", "", false, false, "", 3, 2, "r-9"},
		{"filter by schedule", 50, 0, "c-9", "", false, false, "", 1, 1, "r-9"},
		{"filter by status case insensitive", 50, 0, "", "COMPLETED", false, false, "", 1, 1, "r-1"},
		{"no match empty", 50, 0, "missing", "failed", false, false, "", 0, 0, ""},
		{"closed db errors", 50, 0, "", "", true, false, "failed to count schedule runs", 0, 0, ""},
		{"nil db errors", 50, 0, "", "", false, true, "database is nil", 0, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedPaginatedRuns(ctx, t, db, base)
			database := db
			if tc.closedDB {
				_ = db.Close()
			}
			if tc.nilDB {
				database = nil
			}
			got, total, err := GetScheduleRunsPaginated(ctx, database, tc.limit, tc.offset, tc.scheduleID, tc.status)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("GetScheduleRunsPaginated err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetScheduleRunsPaginated err = %v", err)
			}
			if total != tc.wantTotal {
				t.Errorf("total = %d, want %d", total, tc.wantTotal)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("count = %d, want %d", len(got), tc.wantCount)
			}
			if tc.wantFirstID != "" && got[0].ID != tc.wantFirstID {
				t.Errorf("first ID = %q, want %q", got[0].ID, tc.wantFirstID)
			}
		})
	}
}

func seedPaginatedRuns(ctx context.Context, t *testing.T, db *sql.DB, base time.Time) {
	t.Helper()
	for _, r := range []model.ScheduleRun{
		{ID: "r-1", ScheduleID: "c-1", ScheduleType: "cron", Status: "completed", StartedAt: base},
		{ID: "r-9", ScheduleID: "c-9", ScheduleType: "heartbeat", Status: "running", StartedAt: base.Add(time.Minute)},
		{ID: "r-3", ScheduleID: "c-1", ScheduleType: "cron", Status: "failed", StartedAt: base.Add(2 * time.Minute)},
	} {
		mustCreateRun(ctx, t, db, r)
	}
}
