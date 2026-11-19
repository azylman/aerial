package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func openStoreTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Shared UpdateCronSchedule fixtures. The update path only reads these
// pointers, so shared addresses are safe across sequential table cases.
var (
	updateEffortHigh  = "high"
	updateEffortBogus = "bogus"
	updateCronBlank   = "   "
	updatePromptBlank = "  "
	updatePromptNew   = "updated"
	updateCronExpr    = "0 5 * * *"
	updatePromptP     = "p"
	updateTitle       = "Title"
	updateTimezoneUTC = "UTC"
)

func mustInsertCron(ctx context.Context, t *testing.T, db *sql.DB, c model.CronSchedule) {
	t.Helper()
	if err := InsertCronSchedule(ctx, db, c); err != nil {
		t.Fatalf("InsertCronSchedule err = %v", err)
	}
}

func mustInsertOneShot(ctx context.Context, t *testing.T, db *sql.DB, s model.OneShotSchedule) {
	t.Helper()
	if err := InsertOneShotSchedule(ctx, db, s); err != nil {
		t.Fatalf("InsertOneShotSchedule err = %v", err)
	}
}

func mustInsertHeartbeat(ctx context.Context, t *testing.T, db *sql.DB, h model.HeartbeatSchedule) {
	t.Helper()
	if err := InsertHeartbeatSchedule(ctx, db, h); err != nil {
		t.Fatalf("InsertHeartbeatSchedule err = %v", err)
	}
}

func mustCreateRun(ctx context.Context, t *testing.T, db *sql.DB, r model.ScheduleRun) {
	t.Helper()
	if err := CreateScheduleRun(ctx, db, r); err != nil {
		t.Fatalf("CreateScheduleRun err = %v", err)
	}
}

func TestNormalizeScheduleEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"low passthrough", "low", "low"},
		{"medium passthrough", "medium", "medium"},
		{"high passthrough", "high", "high"},
		{"case and space insensitive", " HIGH ", "high"},
		{"empty defaults medium", "", "medium"},
		{"blank defaults medium", "   ", "medium"},
		{"invalid defaults medium", "bogus", "medium"},
		{"zero value defaults medium", "none", "medium"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeScheduleEffort(tc.input); got != tc.want {
				t.Errorf("NormalizeScheduleEffort(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFindEarliestNextRun(t *testing.T) {
	t.Parallel()
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mid := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		crons      []model.CronSchedule
		heartbeats []model.HeartbeatSchedule
		oneshots   []model.OneShotSchedule
		wantNil    bool
		want       time.Time
	}{
		{"all empty", nil, nil, nil, true, time.Time{}},
		{"zero times ignored", []model.CronSchedule{{ID: "c"}}, nil, nil, true, time.Time{}},
		{"single cron", []model.CronSchedule{{ID: "c", NextRunAt: mid}}, nil, nil, false, mid},
		{"heartbeat earliest", []model.CronSchedule{{ID: "c", NextRunAt: late}}, []model.HeartbeatSchedule{{ID: "h", NextRunAt: early}}, nil, false, early},
		{"oneshot earliest", []model.CronSchedule{{ID: "c", NextRunAt: mid}}, []model.HeartbeatSchedule{{ID: "h", NextRunAt: late}}, []model.OneShotSchedule{{ID: "o", RunAt: early}}, false, early},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := findEarliestNextRun(tc.crons, tc.heartbeats, tc.oneshots)
			if tc.wantNil {
				if got != nil {
					t.Errorf("findEarliestNextRun = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("findEarliestNextRun = nil, want %v", tc.want)
			}
			if !got.Equal(tc.want) {
				t.Errorf("findEarliestNextRun = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOpenDBValidation(t *testing.T) {
	addr := testutil.StartMockPostgres(t)
	pgCfg := &config.Config{DatabaseURL: "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"}
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr string
		wantOK  bool
	}{
		{"nil config", nil, "config cannot be nil", false},
		{"empty dsn", &config.Config{DatabaseURL: ""}, "db: connection string cannot be empty", false},
		{"unsupported scheme", &config.Config{DatabaseURL: "mysql://localhost/db"}, "db: unsupported database scheme", false},
		{"sqlite without hook unsupported", &config.Config{DatabaseURL: ":memory:"}, "db: unsupported database scheme", false},
		{"mock postgres success", pgCfg, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := OpenDB(context.Background(), tc.cfg)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("OpenDB err = %v", err)
				}
				_ = db.Close()
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("OpenDB err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestOpenDBPostgresRetry(t *testing.T) {
	origMax, origBase, origAfter := postgresMaxAttempts, postgresRetryBase, postgresRetryAfter
	t.Cleanup(func() { postgresMaxAttempts, postgresRetryBase, postgresRetryAfter = origMax, origBase, origAfter })
	tests := []struct {
		name     string
		attempts int
		canceled bool
		wantErr  string
		wantCode error
	}{
		{"gives up after attempts", 2, false, "after 2 attempts", nil},
		{"canceled context aborts", 10, true, "after 1 attempts", context.Canceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			postgresMaxAttempts, postgresRetryBase = tc.attempts, time.Millisecond
			postgresRetryAfter = func(time.Duration) <-chan time.Time {
				fired := make(chan time.Time, 1)
				fired <- time.Time{}
				return fired
			}
			ctx := context.Background()
			if tc.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				postgresRetryAfter = func(time.Duration) <-chan time.Time { return nil }
			}
			_, err := openDB(ctx, "postgres://invalid:invalid@127.0.0.1:59997/db?sslmode=disable")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("openDB err = %v, want containing %q", err, tc.wantErr)
			}
			if tc.wantCode != nil && !errors.Is(err, tc.wantCode) {
				t.Errorf("openDB err = %v, want wrapping %v", err, tc.wantCode)
			}
		})
	}
}

func TestIsSQLiteDB(t *testing.T) {
	t.Parallel()
	sqliteDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })
	pgDB, err := sql.Open("pgx", "postgres://invalid:invalid@127.0.0.1:59997/db?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open pgx failed: %v", err)
	}
	t.Cleanup(func() { _ = pgDB.Close() })
	tests := []struct {
		name string
		db   *sql.DB
		want bool
	}{
		{"nil db", nil, false},
		{"sqlite db", sqliteDB, true},
		{"postgres db", pgDB, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isSQLiteDB(tc.db); got != tc.want {
				t.Errorf("isSQLiteDB = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInsertCronSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name         string
		row          model.CronSchedule
		nilDB        bool
		wantErr      string
		wantTimezone string
		wantEffort   string
	}{
		{"defaults applied", model.CronSchedule{ID: "c-default", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true}, false, "", "America/Los_Angeles", "medium"},
		{"explicit preserved", model.CronSchedule{ID: "c-explicit", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", Timezone: "UTC", NextRunAt: now, Enabled: true, Effort: "HIGH"}, false, "", "UTC", "high"},
		{"invalid effort medium", model.CronSchedule{ID: "c-effort", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true, Effort: "bogus"}, false, "", "America/Los_Angeles", "medium"},
		{"nil db errors", model.CronSchedule{ID: "c-nil"}, true, "database is nil", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			if err := InsertCronSchedule(ctx, database, tc.row); err != nil {
				if tc.wantErr == "" {
					t.Fatalf("InsertCronSchedule err = %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("InsertCronSchedule err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if tc.wantErr != "" {
				t.Fatalf("InsertCronSchedule err = nil, want %q", tc.wantErr)
			}
			got, err := GetCronSchedule(ctx, db, tc.row.ID)
			if err != nil {
				t.Fatalf("GetCronSchedule err = %v", err)
			}
			if got.Timezone != tc.wantTimezone {
				t.Errorf("Timezone = %q, want %q", got.Timezone, tc.wantTimezone)
			}
			if got.Effort != tc.wantEffort {
				t.Errorf("Effort = %q, want %q", got.Effort, tc.wantEffort)
			}
			if got.CreatedAt.IsZero() {
				t.Error("CreatedAt is zero, want defaulted")
			}
		})
	}
}

func TestCronScheduleRoundTrip(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name      string
		targetID  string
		wantCount int
		wantIDs   []string
	}{
		{"unfiltered lists all enabled", "", 2, []string{"c-1", "c-2"}},
		{"filtered by target", "t-1", 1, []string{"c-1"}},
		{"unknown target empty", "nope", 0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seed := []model.CronSchedule{
				{ID: "c-1", TargetID: "t-1", CronExpr: "0 0 * * *", Prompt: "p1", Timezone: "UTC", NextRunAt: now, Enabled: true, Effort: "high"},
				{ID: "c-2", TargetID: "t-2", CronExpr: "0 1 * * *", Prompt: "p2", Timezone: "UTC", NextRunAt: now, Enabled: true, Effort: "low"},
				{ID: "c-off", TargetID: "t-1", CronExpr: "0 2 * * *", Prompt: "p3", Timezone: "UTC", NextRunAt: now, Enabled: false, Effort: "low"},
			}
			for _, c := range seed {
				mustInsertCron(ctx, t, db, c)
			}
			got, err := ListCronSchedules(ctx, db, tc.targetID)
			if err != nil {
				t.Fatalf("ListCronSchedules err = %v", err)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("ListCronSchedules count = %d, want %d", len(got), tc.wantCount)
			}
			for i, id := range tc.wantIDs {
				if got[i].ID != id {
					t.Errorf("row %d ID = %q, want %q", i, got[i].ID, id)
				}
			}
			if tc.wantCount > 0 && got[0].Prompt == "" {
				t.Error("Prompt empty, want seeded value")
			}
		})
	}
}

func TestGetCronScheduleErrors(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		nilDB    bool
		nilCtx   bool
		closedDB bool
		wantErr  string
	}{
		{"empty id", "", false, false, false, "schedule_id cannot be empty"},
		{"blank id", "  ", false, false, false, "schedule_id cannot be empty"},
		{"not found", "missing", false, false, false, `cron schedule "missing" not found`},
		{"nil ctx defaults", "missing", false, true, false, `cron schedule "missing" not found`},
		{"nil db", "c-1", true, false, false, "database is nil"},
		{"closed db", "c-1", false, false, true, "database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			ctx := context.Background()
			if tc.nilCtx {
				ctx = nil
			}
			_, err := GetCronSchedule(ctx, database, tc.id)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("GetCronSchedule err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestUpdateCronSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	later := now.Add(2 * time.Hour)
	tests := []struct {
		name        string
		id          string
		effort      *string
		cronExpr    *string
		prompt      *string
		titlePrefix *string
		timezone    *string
		nextRunAt   *time.Time
		nilDB       bool
		closedDB    bool
		wantErr     string
		wantEffort  string
		wantPrompt  string
	}{
		{"patch prompt and effort", "c-1", &updateEffortHigh, nil, &updatePromptNew, nil, nil, nil, false, false, "", "high", "updated"},
		{"patch all columns", "c-1", &updateEffortBogus, &updateCronExpr, &updatePromptP, &updateTitle, &updateTimezoneUTC, &later, false, false, "", "medium", "p"},
		{"blank values ignored", "c-1", nil, &updateCronBlank, &updatePromptBlank, nil, nil, nil, false, false, "", "", ""},
		{"empty id", "", &updateEffortHigh, nil, nil, nil, nil, nil, false, false, "schedule_id cannot be empty", "", ""},
		{"unknown id", "missing", &updateEffortHigh, nil, nil, nil, nil, nil, false, false, `cron schedule "missing" not found`, "", ""},
		{"closed db exec errors", "c-1", &updateEffortHigh, nil, nil, nil, nil, nil, false, true, "database is closed", "", ""},
		{"nil db", "c-1", &updateEffortHigh, nil, nil, nil, nil, nil, true, false, "database is nil", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertCron(ctx, t, db, model.CronSchedule{ID: "c-1", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "orig", Timezone: "UTC", NextRunAt: now, Enabled: true, Effort: "low"})
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			err := UpdateCronSchedule(ctx, database, tc.id, tc.effort, tc.cronExpr, tc.prompt, tc.titlePrefix, tc.timezone, tc.nextRunAt)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("UpdateCronSchedule err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateCronSchedule err = %v", err)
			}
			// The no-change update is a silent no-op success.
			if tc.effort == nil && tc.cronExpr != nil && strings.TrimSpace(*tc.cronExpr) == "" {
				return
			}
			verifyCronPatch(ctx, t, db, tc.wantEffort, tc.wantPrompt)
		})
	}
}

func verifyCronPatch(ctx context.Context, t *testing.T, db *sql.DB, wantEffort, wantPrompt string) {
	t.Helper()
	got, err := GetCronSchedule(ctx, db, "c-1")
	if err != nil {
		t.Fatalf("GetCronSchedule err = %v", err)
	}
	if got.Effort != wantEffort {
		t.Errorf("Effort = %q, want %q", got.Effort, wantEffort)
	}
	if got.Prompt != wantPrompt {
		t.Errorf("Prompt = %q, want %q", got.Prompt, wantPrompt)
	}
}

func TestOneShotScheduleRoundTrip(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name       string
		threadID   string
		wantCount  int
		wantEffort string
	}{
		{"unfiltered lists all", "", 2, ""},
		{"filtered by thread", "th-1", 1, "high"},
		{"unknown thread empty", "nope", 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-1", ThreadID: "th-1", Prompt: "p1", RunAt: now, Effort: "HIGH"})
			mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-2", ThreadID: "th-2", Prompt: "p2", RunAt: now.Add(time.Hour), Effort: "bogus"})
			got, err := ListOneShotSchedules(ctx, db, tc.threadID)
			if err != nil {
				t.Fatalf("ListOneShotSchedules err = %v", err)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("ListOneShotSchedules count = %d, want %d", len(got), tc.wantCount)
			}
			if tc.wantEffort != "" && got[0].Effort != tc.wantEffort {
				t.Errorf("Effort = %q, want %q", got[0].Effort, tc.wantEffort)
			}
			fetched, err := GetOneShotSchedule(ctx, db, "s-1")
			if err != nil {
				t.Fatalf("GetOneShotSchedule err = %v", err)
			}
			if fetched.ThreadID != "th-1" || fetched.Prompt != "p1" {
				t.Errorf("GetOneShotSchedule = %+v, want th-1/p1", fetched)
			}
		})
	}
}

func TestOneShotScheduleErrors(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		nilDB    bool
		closedDB bool
		wantErr  string
	}{
		{"empty get id", "", false, false, "schedule_id cannot be empty"},
		{"get not found", "missing", false, false, `one-shot schedule "missing" not found`},
		{"get nil db", "s-1", true, false, "database is nil"},
		{"delete empty id", "", false, false, "schedule_id cannot be empty"},
		{"insert nil db", "s-1", true, false, "database is nil"},
		{"list nil db", "s-1", true, false, "database is nil"},
		{"list closed db", "s-1", false, true, "database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			var err error
			switch {
			case strings.HasPrefix(tc.name, "delete"):
				_, err = DeleteOneShotSchedule(ctx, database, tc.id)
			case strings.HasPrefix(tc.name, "insert"):
				err = InsertOneShotSchedule(ctx, database, model.OneShotSchedule{ID: tc.id})
			case strings.HasPrefix(tc.name, "list"):
				_, err = ListOneShotSchedules(ctx, database, tc.id)
			default:
				_, err = GetOneShotSchedule(ctx, database, tc.id)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestDeleteOneShotSchedule(t *testing.T) {
	tests := []struct {
		name        string
		seed        bool
		id          string
		closedDB    bool
		wantDeleted bool
		wantErr     string
	}{
		{"deletes existing", true, "s-1", false, true, ""},
		{"missing returns false", false, "missing", false, false, ""},
		{"closed db errors", false, "s-1", true, false, "database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.seed {
				mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-1", ThreadID: "th", Prompt: "p", RunAt: time.Now().UTC()})
			}
			database := db
			if tc.closedDB {
				_ = db.Close()
			}
			deleted, err := DeleteOneShotSchedule(ctx, database, tc.id)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("DeleteOneShotSchedule err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeleteOneShotSchedule err = %v", err)
			}
			if deleted != tc.wantDeleted {
				t.Errorf("deleted = %v, want %v", deleted, tc.wantDeleted)
			}
		})
	}
}

func TestHeartbeatScheduleRoundTrip(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name         string
		effort       string
		timezone     string
		wantEffort   string
		wantTimezone string
	}{
		{"valid effort", "high", "UTC", "high", "UTC"},
		{"empty effort low timezone utc", "", "", "low", "UTC"},
		{"invalid effort low", "bogus", "UTC", "low", "UTC"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: tc.timezone, NextRunAt: now, Enabled: true, Effort: tc.effort})
			got, err := GetHeartbeatSchedule(ctx, db, "h-1")
			if err != nil {
				t.Fatalf("GetHeartbeatSchedule err = %v", err)
			}
			if got.Effort != tc.wantEffort {
				t.Errorf("Effort = %q, want %q", got.Effort, tc.wantEffort)
			}
			if got.Timezone != tc.wantTimezone {
				t.Errorf("Timezone = %q, want %q", got.Timezone, tc.wantTimezone)
			}
			if got.LastCompletedAt != nil {
				t.Errorf("LastCompletedAt = %v, want nil", got.LastCompletedAt)
			}
			listed, err := ListHeartbeatSchedules(ctx, db, "t")
			if err != nil {
				t.Fatalf("ListHeartbeatSchedules err = %v", err)
			}
			if len(listed) != 1 || listed[0].ID != "h-1" {
				t.Errorf("ListHeartbeatSchedules = %+v, want one h-1 row", listed)
			}
		})
	}
}

func TestHeartbeatScheduleErrors(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		nilDB   bool
		wantErr string
	}{
		{"empty get id", "", false, "schedule_id cannot be empty"},
		{"get not found", "missing", false, `heartbeat schedule "missing" not found`},
		{"get nil db", "h-1", true, "database is nil"},
		{"insert nil db", "h-1", true, "database is nil"},
		{"list nil db", "h-1", true, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			var err error
			switch {
			case strings.HasPrefix(tc.name, "insert"):
				err = InsertHeartbeatSchedule(ctx, database, model.HeartbeatSchedule{ID: tc.id})
			case strings.HasPrefix(tc.name, "list"):
				_, err = ListHeartbeatSchedules(ctx, database, tc.id)
			default:
				_, err = GetHeartbeatSchedule(ctx, database, tc.id)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestUpdateHeartbeatSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name       string
		id         string
		effort     string
		prompt     string
		wantErr    string
		wantPrompt string
	}{
		{"replaces row", "h-1", "high", "new prompt", "", "new prompt"},
		{"invalid effort errors", "h-1", "bogus", "p", "invalid effort tier", ""},
		{"unknown id not found", "missing", "low", "p", `heartbeat schedule "missing" not found`, ""},
		{"empty id errors", "", "low", "p", "schedule_id cannot be empty", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "orig", Timezone: "UTC", NextRunAt: now, Enabled: true, Effort: "low"})
			err := UpdateHeartbeatSchedule(ctx, db, model.HeartbeatSchedule{ID: tc.id, TargetID: "t", CronExpr: "0 * * * *", Prompt: tc.prompt, Timezone: "UTC", NextRunAt: now, Enabled: true, Effort: tc.effort})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("UpdateHeartbeatSchedule err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateHeartbeatSchedule err = %v", err)
			}
			got, err := GetHeartbeatSchedule(ctx, db, "h-1")
			if err != nil {
				t.Fatalf("GetHeartbeatSchedule err = %v", err)
			}
			if got.Prompt != tc.wantPrompt {
				t.Errorf("Prompt = %q, want %q", got.Prompt, tc.wantPrompt)
			}
		})
	}
}

func seedDueSchedules(ctx context.Context, t *testing.T, db *sql.DB, past, future time.Time) {
	t.Helper()
	for _, c := range []model.CronSchedule{
		{ID: "c-due", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", Timezone: "UTC", NextRunAt: past, Enabled: true},
		{ID: "c-future", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", Timezone: "UTC", NextRunAt: future, Enabled: true},
		{ID: "c-off", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", Timezone: "UTC", NextRunAt: past, Enabled: false},
	} {
		mustInsertCron(ctx, t, db, c)
	}
	for _, h := range []model.HeartbeatSchedule{
		{ID: "h-due", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: "UTC", NextRunAt: past, Enabled: true},
		{ID: "h-future", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: "UTC", NextRunAt: future, Enabled: true},
	} {
		mustInsertHeartbeat(ctx, t, db, h)
	}
	for _, s := range []model.OneShotSchedule{
		{ID: "s-due", ThreadID: "th", Prompt: "p", RunAt: past},
		{ID: "s-future", ThreadID: "th", Prompt: "p", RunAt: future},
	} {
		mustInsertOneShot(ctx, t, db, s)
	}
}

func TestGetDueSchedules(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name   string
		kind   string
		wantID string
	}{
		{"due cron", "cron", "c-due"},
		{"due heartbeat", "heartbeat", "h-due"},
		{"due oneshot", "oneshot", "s-due"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedDueSchedules(ctx, t, db, now.Add(-time.Hour), now.Add(time.Hour))
			if gotID := fetchDueID(ctx, t, db, tc.kind, now); gotID != tc.wantID {
				t.Errorf("due ID = %q, want %q", gotID, tc.wantID)
			}
		})
	}
}

func fetchDueID(ctx context.Context, t *testing.T, db *sql.DB, kind string, now time.Time) string {
	t.Helper()
	switch kind {
	case "cron":
		dues, err := GetDueCronSchedules(ctx, db, now)
		if err != nil {
			t.Fatalf("GetDueCronSchedules err = %v", err)
		}
		if len(dues) != 1 {
			t.Fatalf("GetDueCronSchedules = %+v, want one row", dues)
		}
		return dues[0].ID
	case "heartbeat":
		dues, err := GetDueHeartbeatSchedules(ctx, db, now)
		if err != nil {
			t.Fatalf("GetDueHeartbeatSchedules err = %v", err)
		}
		if len(dues) != 1 {
			t.Fatalf("GetDueHeartbeatSchedules = %+v, want one row", dues)
		}
		return dues[0].ID
	default:
		dues, err := GetDueOneShotSchedules(ctx, db, now)
		if err != nil {
			t.Fatalf("GetDueOneShotSchedules err = %v", err)
		}
		if len(dues) != 1 {
			t.Fatalf("GetDueOneShotSchedules = %+v, want one row", dues)
		}
		return dues[0].ID
	}
}

func TestAdvanceDueSchedules(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	future := now.Add(time.Hour)
	tests := []struct {
		name    string
		id      string
		wantErr string
	}{
		{"advance cron clears due", "c-due", ""},
		{"advance cron missing", "missing", `cron schedule "missing" not found`},
		{"advance heartbeat clears due", "h-due", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedDueSchedules(ctx, t, db, now.Add(-time.Hour), future)
			var err error
			if strings.HasPrefix(tc.id, "h-") {
				err = AdvanceHeartbeatNextRun(ctx, db, tc.id, future)
			} else {
				err = AdvanceCronNextRun(ctx, db, tc.id, future)
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("advance err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("advance err = %v", err)
			}
			dues, err := GetDueCronSchedules(ctx, db, now)
			if err != nil {
				t.Fatalf("GetDueCronSchedules err = %v", err)
			}
			for _, d := range dues {
				if d.ID == tc.id {
					t.Errorf("schedule %q still due after advance", tc.id)
				}
			}
		})
	}
}

func TestAdvanceScheduleGuards(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		nilDB    bool
		closedDB bool
		wantErr  string
	}{
		{"cron empty id", "", false, false, "schedule_id cannot be empty"},
		{"cron nil db", "c-1", true, false, "database is nil"},
		{"heartbeat empty id", "", false, false, "schedule_id cannot be empty"},
		{"heartbeat nil db", "h-1", true, false, "database is nil"},
		{"watermark empty id", "", false, false, "schedule_id cannot be empty"},
		{"watermark nil db", "h-1", true, false, "database is nil"},
		{"due cron nil db", "c-1", true, false, "database is nil"},
		{"due cron closed db", "c-1", false, true, "database is closed"},
		{"due heartbeat nil db", "h-1", true, false, "database is nil"},
		{"due oneshot nil db", "s-1", true, false, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			now := time.Now().UTC()
			var err error
			switch {
			case strings.HasPrefix(tc.name, "cron"):
				err = AdvanceCronNextRun(ctx, database, tc.id, now)
			case strings.HasPrefix(tc.name, "heartbeat"):
				err = AdvanceHeartbeatNextRun(ctx, database, tc.id, now)
			case strings.HasPrefix(tc.name, "watermark"):
				err = AdvanceHeartbeatLastCompletedAt(ctx, database, tc.id, now)
			case strings.HasPrefix(tc.name, "due cron"):
				_, err = GetDueCronSchedules(ctx, database, now)
			case strings.HasPrefix(tc.name, "due heartbeat"):
				_, err = GetDueHeartbeatSchedules(ctx, database, now)
			default:
				_, err = GetDueOneShotSchedules(ctx, database, now)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func seedWatermarkHeartbeat(ctx context.Context, t *testing.T, db *sql.DB, now time.Time, preset bool) {
	t.Helper()
	mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: "UTC", NextRunAt: now, Enabled: true})
	if preset {
		if err := AdvanceHeartbeatLastCompletedAt(ctx, db, "h-1", now); err != nil {
			t.Fatalf("AdvanceHeartbeatLastCompletedAt err = %v", err)
		}
	}
}

func verifyWatermark(ctx context.Context, t *testing.T, db *sql.DB, want time.Time) {
	t.Helper()
	got, err := GetHeartbeatSchedule(ctx, db, "h-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule err = %v", err)
	}
	if got.LastCompletedAt == nil || !got.LastCompletedAt.Equal(want) {
		t.Errorf("LastCompletedAt = %v, want %v", got.LastCompletedAt, want)
	}
}

func TestAdvanceHeartbeatLastCompletedAt(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name      string
		id        string
		completed time.Time
		wantErr   string
		preset    bool
	}{
		{"sets watermark", "h-1", now, "", false},
		{"monotonic keeps later", "h-1", now.Add(-time.Hour), "", true},
		{"unknown id not found", "missing", now, `heartbeat schedule "missing" not found`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedWatermarkHeartbeat(ctx, t, db, now, tc.preset)
			err := AdvanceHeartbeatLastCompletedAt(ctx, db, tc.id, tc.completed)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.wantErr) {
					t.Errorf("AdvanceHeartbeatLastCompletedAt err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("AdvanceHeartbeatLastCompletedAt err = %v", err)
			}
			verifyWatermark(ctx, t, db, now)
		})
	}
}

func seedSharedSchedules(ctx context.Context, t *testing.T, db *sql.DB, id string, now time.Time) {
	t.Helper()
	mustInsertCron(ctx, t, db, model.CronSchedule{ID: id, TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true})
	mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: id, ThreadID: "th", Prompt: "p", RunAt: now})
	mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: id, TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", NextRunAt: now, Enabled: true})
	if _, err := db.ExecContext(ctx, `INSERT INTO event_triggers (id, channel_id, source, target, event_type, prompt, effort, created_at, persistent) VALUES (?, 'ch', 'vanadis_build', 't', 'ev', 'p', 'medium', ?, FALSE)`, id, now); err != nil {
		t.Fatalf("seed event trigger err = %v", err)
	}
}

func TestDeleteSchedule(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		nilDB       bool
		closedDB    bool
		wantDeleted bool
		wantErr     string
	}{
		{"deletes across tables", "shared", false, false, true, ""},
		{"missing returns false", "missing", false, false, false, ""},
		{"closed db errors", "shared", false, true, false, "database is closed"},
		{"nil db errors", "shared", true, false, false, "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			now := time.Now().UTC()
			if tc.closedDB {
				_ = db.Close()
			}
			if tc.wantDeleted {
				seedSharedSchedules(ctx, t, db, tc.id, now)
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			deleted, err := DeleteSchedule(ctx, database, tc.id)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("DeleteSchedule err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeleteSchedule err = %v", err)
			}
			if deleted != tc.wantDeleted {
				t.Errorf("deleted = %v, want %v", deleted, tc.wantDeleted)
			}
		})
	}
}
