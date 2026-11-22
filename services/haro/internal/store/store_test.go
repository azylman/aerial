package store

import (
	"context"
	"database/sql"
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
		{"empty dsn", &config.Config{DatabaseURL: ""}, "connection string cannot be empty", false},
		{"unsupported scheme", &config.Config{DatabaseURL: "mysql://localhost/db"}, "unsupported database scheme", false},
		{"sqlite without hook unsupported", &config.Config{DatabaseURL: ":memory:"}, "unsupported database scheme", false},
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

// scheduleCaseDB applies a row's nilDB/closedDB inputs after seeding.
func scheduleCaseDB(db *sql.DB, nilDB, closedDB bool) *sql.DB {
	if closedDB {
		_ = db.Close()
	}
	if nilDB {
		return nil
	}
	return db
}

// assertScheduleErr fails unless err matches wantErr ("" = no error) and reports whether the row expected one.
func assertScheduleErr(t *testing.T, err error, wantErr string) bool {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected err = %v", err)
		}
		return false
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("err = %v, want %q", err, wantErr)
	}
	return true
}

func seedOneShotPair(ctx context.Context, t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-1", ThreadID: "th-1", Prompt: "p1", RunAt: now, Effort: "HIGH"})
	mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-2", ThreadID: "th-2", Prompt: "p2", RunAt: now.Add(time.Hour), Effort: "bogus"})
}

func TestListOneShotSchedules(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name       string
		threadID   string
		nilDB      bool
		closedDB   bool
		wantCount  int
		wantEffort string
		wantErr    string
	}{
		{name: "unfiltered lists all", wantCount: 2},
		{name: "filtered by thread", threadID: "th-1", wantCount: 1, wantEffort: "high"},
		{name: "unknown thread empty", threadID: "nope"},
		{name: "nil db", nilDB: true, wantErr: "database is nil"},
		{name: "closed db", closedDB: true, wantErr: "database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedOneShotPair(ctx, t, db, now)
			got, err := ListOneShotSchedules(ctx, scheduleCaseDB(db, tc.nilDB, tc.closedDB), tc.threadID)
			if assertScheduleErr(t, err, tc.wantErr) {
				return
			}
			if len(got) != tc.wantCount {
				t.Fatalf("ListOneShotSchedules count = %d, want %d", len(got), tc.wantCount)
			}
			if tc.wantEffort != "" && got[0].Effort != tc.wantEffort {
				t.Errorf("Effort = %q, want %q", got[0].Effort, tc.wantEffort)
			}
		})
	}
}

func TestGetOneShotSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name       string
		id         string
		nilDB      bool
		wantThread string
		wantPrompt string
		wantEffort string
		wantErr    string
	}{
		{name: "found normalizes effort", id: "s-1", wantThread: "th-1", wantPrompt: "p1", wantEffort: "high"},
		{name: "invalid stored effort reads medium", id: "s-2", wantThread: "th-2", wantPrompt: "p2", wantEffort: "medium"},
		{name: "empty id", wantErr: "schedule_id cannot be empty"},
		{name: "not found", id: "missing", wantErr: `one-shot schedule "missing" not found`},
		{name: "nil db", id: "s-1", nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedOneShotPair(ctx, t, db, now)
			got, err := GetOneShotSchedule(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.id)
			if assertScheduleErr(t, err, tc.wantErr) {
				return
			}
			if got.ThreadID != tc.wantThread || got.Prompt != tc.wantPrompt || got.Effort != tc.wantEffort {
				t.Errorf("GetOneShotSchedule = %+v, want %s/%s/%s", got, tc.wantThread, tc.wantPrompt, tc.wantEffort)
			}
		})
	}
}

func TestInsertOneShotSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name    string
		row     model.OneShotSchedule
		nilDB   bool
		wantErr string
	}{
		{name: "inserts row", row: model.OneShotSchedule{ID: "s-1", ThreadID: "th-1", Prompt: "p1", RunAt: now}},
		{name: "nil db", row: model.OneShotSchedule{ID: "s-1"}, nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if assertScheduleErr(t, InsertOneShotSchedule(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.row), tc.wantErr) {
				return
			}
			listed, err := ListOneShotSchedules(ctx, db, tc.row.ThreadID)
			if err != nil || len(listed) != 1 || listed[0].Prompt != tc.row.Prompt || listed[0].Effort != "medium" || listed[0].CreatedAt.IsZero() {
				t.Errorf("ListOneShotSchedules = %+v, %v; want one %s row with medium effort and defaulted CreatedAt", listed, err, tc.row.Prompt)
			}
		})
	}
}

// TestDeleteOneShotSchedule pins the wrapper's wiring (table and empty-id text); deleteByID's
// branches live in TestDeleteByID.
func TestDeleteOneShotSchedule(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		wantDeleted bool
		wantErr     string
	}{
		{name: "deletes existing", id: "s-1", wantDeleted: true},
		{name: "empty id", wantErr: "schedule_id cannot be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-1", ThreadID: "th", Prompt: "p", RunAt: time.Now().UTC()})
			deleted, err := DeleteOneShotSchedule(ctx, db, tc.id)
			if assertScheduleErr(t, err, tc.wantErr) {
				return
			}
			if deleted != tc.wantDeleted {
				t.Errorf("deleted = %v, want %v", deleted, tc.wantDeleted)
			}
		})
	}
}

func TestDeleteByID(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name        string
		query       string
		emptyErrMsg string
		errPrefix   string
		id          string
		seed        bool
		nilDB       bool
		closedDB    bool
		wantDeleted bool
		wantErr     string
	}{
		{"nil db errors", "DELETE FROM one_shot_schedules WHERE id = $1", "", "", "s-1", false, true, false, false, "database is nil"},
		{"empty id custom message", "DELETE FROM one_shot_schedules WHERE id = $1", "custom id required", "", "   ", false, false, false, false, "custom id required"},
		{"closed db prefixed error", "DELETE FROM one_shot_schedules WHERE id = $1", "", "failed to delete test item", "s-1", false, false, true, false, "failed to delete test item"},
		{"deletes existing", "DELETE FROM one_shot_schedules WHERE id = $1", "", "", "s-1", true, false, false, true, ""},
		{"missing returns false", "DELETE FROM one_shot_schedules WHERE id = $1", "", "", "missing", false, false, false, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.seed {
				mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-1", ThreadID: "th", Prompt: "p", RunAt: now})
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			if tc.closedDB {
				_ = db.Close()
			}
			deleted, err := deleteByID(ctx, database, tc.query, tc.emptyErrMsg, tc.errPrefix, tc.id)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("deleteByID err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("deleteByID err = %v", err)
			}
			if deleted != tc.wantDeleted {
				t.Errorf("deleted = %v, want %v", deleted, tc.wantDeleted)
			}
		})
	}
}

func TestGetHeartbeatSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name         string
		effort       string
		timezone     string
		id           string
		nilDB        bool
		wantEffort   string
		wantTimezone string
		wantErr      string
	}{
		{name: "valid effort", effort: "high", timezone: "UTC", id: "h-1", wantEffort: "high", wantTimezone: "UTC"},
		{name: "empty effort low timezone utc", id: "h-1", wantEffort: "low", wantTimezone: "UTC"},
		{name: "invalid effort low", effort: "bogus", timezone: "UTC", id: "h-1", wantEffort: "low", wantTimezone: "UTC"},
		{name: "empty id", wantErr: "schedule_id cannot be empty"},
		{name: "not found", id: "missing", wantErr: `heartbeat schedule "missing" not found`},
		{name: "nil db", id: "h-1", nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: tc.timezone, NextRunAt: now, Enabled: true, Effort: tc.effort})
			got, err := GetHeartbeatSchedule(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.id)
			if assertScheduleErr(t, err, tc.wantErr) {
				return
			}
			if got.Effort != tc.wantEffort || got.Timezone != tc.wantTimezone {
				t.Errorf("Effort/Timezone = %q/%q, want %q/%q", got.Effort, got.Timezone, tc.wantEffort, tc.wantTimezone)
			}
			if got.LastCompletedAt != nil {
				t.Errorf("LastCompletedAt = %v, want nil", got.LastCompletedAt)
			}
		})
	}
}

func TestListHeartbeatSchedules(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name     string
		targetID string
		nilDB    bool
		wantIDs  []string
		wantErr  string
	}{
		{name: "filtered by target", targetID: "t", wantIDs: []string{"h-1"}},
		{name: "unknown target empty", targetID: "nope"},
		{name: "nil db", targetID: "t", nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", Timezone: "UTC", NextRunAt: now, Enabled: true})
			listed, err := ListHeartbeatSchedules(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.targetID)
			if assertScheduleErr(t, err, tc.wantErr) {
				return
			}
			ids := make([]string, 0, len(listed))
			for _, h := range listed {
				ids = append(ids, h.ID)
			}
			if strings.Join(ids, ",") != strings.Join(tc.wantIDs, ",") {
				t.Errorf("ListHeartbeatSchedules IDs = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
}

func TestInsertHeartbeatSchedule(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name    string
		row     model.HeartbeatSchedule
		nilDB   bool
		wantErr string
	}{
		{name: "inserts row", row: model.HeartbeatSchedule{ID: "h-1", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", NextRunAt: now, Enabled: true}},
		{name: "nil db", row: model.HeartbeatSchedule{ID: "h-1"}, nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if assertScheduleErr(t, InsertHeartbeatSchedule(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.row), tc.wantErr) {
				return
			}
			got, err := GetHeartbeatSchedule(ctx, db, tc.row.ID)
			if err != nil || got.TargetID != tc.row.TargetID || got.CronExpr != tc.row.CronExpr {
				t.Errorf("GetHeartbeatSchedule = %+v, %v; want %s/%s", got, err, tc.row.TargetID, tc.row.CronExpr)
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

func TestGetDueCronSchedulesErrors(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name     string
		badSQL   string
		nilDB    bool
		closedDB bool
		wantErr  string
	}{
		{name: "nil db", nilDB: true, wantErr: "database is nil"},
		{name: "closed db", closedDB: true, wantErr: "database is closed"},
		{
			name:    "bad row fails scan",
			badSQL:  `INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ('c-bad', 't1', '', '0 0 * * *', 'p', 'UTC', 0, TRUE, CURRENT_TIMESTAMP, 'medium')`,
			wantErr: "Scan error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.badSQL != "" {
				if _, err := db.ExecContext(ctx, tc.badSQL); err != nil {
					t.Fatalf("insert bad row err = %v", err)
				}
			}
			_, err := GetDueCronSchedules(ctx, scheduleCaseDB(db, tc.nilDB, tc.closedDB), now)
			assertScheduleErr(t, err, tc.wantErr)
		})
	}
}

func TestGetDueHeartbeatSchedulesErrors(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name    string
		badSQL  string
		nilDB   bool
		wantErr string
	}{
		{name: "nil db", nilDB: true, wantErr: "database is nil"},
		{
			name:    "bad row fails scan",
			badSQL:  `INSERT INTO heartbeat_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, effort, next_run_at, enabled, created_at) VALUES ('h-bad', 't1', '', '0 0 * * *', 'p', 'UTC', 'low', 0, TRUE, CURRENT_TIMESTAMP)`,
			wantErr: "Scan error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			if tc.badSQL != "" {
				if _, err := db.ExecContext(ctx, tc.badSQL); err != nil {
					t.Fatalf("insert bad row err = %v", err)
				}
			}
			_, err := GetDueHeartbeatSchedules(ctx, scheduleCaseDB(db, tc.nilDB, false), now)
			assertScheduleErr(t, err, tc.wantErr)
		})
	}
}

func TestGetDueOneShotSchedulesErrors(t *testing.T) {
	tests := []struct {
		name    string
		nilDB   bool
		wantErr string
	}{
		{name: "nil db", nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			_, err := GetDueOneShotSchedules(context.Background(), scheduleCaseDB(db, tc.nilDB, false), time.Now().UTC())
			assertScheduleErr(t, err, tc.wantErr)
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

type advanceNextRunCase struct {
	name    string
	id      string
	nilDB   bool
	wantErr string
}

// advanceNextRunCases are the shared rows for the cron and heartbeat Advance*NextRun wrappers.
func advanceNextRunCases(dueID, kind string) []advanceNextRunCase {
	return []advanceNextRunCase{
		{name: "advance clears due", id: dueID},
		{name: "missing", id: "missing", wantErr: kind + ` schedule "missing" not found`},
		{name: "empty id", wantErr: "schedule_id cannot be empty"},
		{name: "nil db", id: dueID, nilDB: true, wantErr: "database is nil"},
	}
}

func assertNoneDue(t *testing.T, due int, err error) {
	t.Helper()
	if err != nil || due != 0 {
		t.Errorf("due schedules after advance = %d, err %v; want none", due, err)
	}
}

func TestAdvanceCronNextRun(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, tc := range advanceNextRunCases("c-due", "cron") {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedDueSchedules(ctx, t, db, now.Add(-time.Hour), now.Add(time.Hour))
			if !assertScheduleErr(t, AdvanceCronNextRun(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.id, now.Add(time.Hour)), tc.wantErr) {
				dues, err := GetDueCronSchedules(ctx, db, now)
				assertNoneDue(t, len(dues), err)
			}
		})
	}
}

func TestAdvanceHeartbeatNextRun(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, tc := range advanceNextRunCases("h-due", "heartbeat") {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedDueSchedules(ctx, t, db, now.Add(-time.Hour), now.Add(time.Hour))
			future := now.Add(time.Hour)
			if assertScheduleErr(t, AdvanceHeartbeatNextRun(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.id, future), tc.wantErr) {
				return
			}
			dues, err := GetDueHeartbeatSchedules(ctx, db, now)
			assertNoneDue(t, len(dues), err)
			got, err := GetHeartbeatSchedule(ctx, db, tc.id)
			if err != nil {
				t.Fatalf("GetHeartbeatSchedule err = %v", err)
			}
			if !got.NextRunAt.Equal(future) {
				t.Errorf("NextRunAt = %v, want %v", got.NextRunAt, future)
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
		preset    bool
		nilDB     bool
		wantErr   string
	}{
		{name: "sets watermark", id: "h-1", completed: now},
		{name: "monotonic keeps later", id: "h-1", completed: now.Add(-time.Hour), preset: true},
		{name: "unknown id not found", id: "missing", completed: now, wantErr: `heartbeat schedule "missing" not found`},
		{name: "empty id", completed: now, wantErr: "schedule_id cannot be empty"},
		{name: "nil db", id: "h-1", completed: now, nilDB: true, wantErr: "database is nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			seedWatermarkHeartbeat(ctx, t, db, now, tc.preset)
			if assertScheduleErr(t, AdvanceHeartbeatLastCompletedAt(ctx, scheduleCaseDB(db, tc.nilDB, false), tc.id, tc.completed), tc.wantErr) {
				return
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

func TestGetScheduleAgent(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name      string
		id        string
		seedKind  string
		seedAgent string
		nilDB     bool
		wantAgent string
		wantErr   string
	}{
		{name: "cron schedule agent", id: "c-agent-1", seedKind: "cron", seedAgent: "agent-alpha", wantAgent: "agent-alpha"},
		{name: "oneshot schedule agent", id: "s-agent-1", seedKind: "oneshot", seedAgent: "agent-beta", wantAgent: "agent-beta"},
		{name: "heartbeat schedule agent", id: "h-agent-1", seedKind: "heartbeat", seedAgent: "agent-gamma", wantAgent: "agent-gamma"},
		{name: "event trigger agent", id: "et-agent-1", seedKind: "event", seedAgent: "agent-delta", wantAgent: "agent-delta"},
		{name: "empty id errors", id: "   ", wantErr: "schedule_id cannot be empty"},
		{name: "nil db errors", id: "c-1", nilDB: true, wantErr: "database is nil"},
		{name: "not found errors", id: "missing-id", wantErr: `schedule "missing-id" not found`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openStoreTestDB(t)
			ctx := context.Background()
			switch tc.seedKind {
			case "cron":
				mustInsertCron(ctx, t, db, model.CronSchedule{ID: tc.id, TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true, Agent: tc.seedAgent})
			case "oneshot":
				mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: tc.id, ThreadID: "th", Prompt: "p", RunAt: now, Agent: tc.seedAgent})
			case "heartbeat":
				mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: tc.id, TargetID: "t", CronExpr: "0 * * * *", Prompt: "p", NextRunAt: now, Enabled: true, Agent: tc.seedAgent})
			case "event":
				if err := InsertEventTrigger(ctx, db, model.EventTrigger{ID: tc.id, ChannelID: "ch", Source: "flux", Target: "t", EventType: "ev", Prompt: "p", Agent: tc.seedAgent}); err != nil {
					t.Fatalf("InsertEventTrigger err = %v", err)
				}
			}
			database := db
			if tc.nilDB {
				database = nil
			}
			got, err := GetScheduleAgent(ctx, database, tc.id)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("GetScheduleAgent err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetScheduleAgent err = %v", err)
			}
			if got != tc.wantAgent {
				t.Errorf("GetScheduleAgent(%q) = %q, want %q", tc.id, got, tc.wantAgent)
			}
		})
	}
}

func TestMultiAgentScheduleFiltering(t *testing.T) {
	now := time.Now().UTC()
	db := openStoreTestDB(t)
	ctx := context.Background()

	mustInsertCron(ctx, t, db, model.CronSchedule{ID: "c-alpha", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p1", NextRunAt: now, Enabled: true, Agent: "agent-alpha"})
	mustInsertCron(ctx, t, db, model.CronSchedule{ID: "c-beta", TargetID: "t", CronExpr: "0 0 * * *", Prompt: "p2", NextRunAt: now, Enabled: true, Agent: "agent-beta"})

	mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-alpha", ThreadID: "th", Prompt: "p1", RunAt: now, Agent: "agent-alpha"})
	mustInsertOneShot(ctx, t, db, model.OneShotSchedule{ID: "s-beta", ThreadID: "th", Prompt: "p2", RunAt: now, Agent: "agent-beta"})

	mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-alpha", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p1", NextRunAt: now, Enabled: true, Agent: "agent-alpha"})
	mustInsertHeartbeat(ctx, t, db, model.HeartbeatSchedule{ID: "h-beta", TargetID: "t", CronExpr: "0 * * * *", Prompt: "p2", NextRunAt: now, Enabled: true, Agent: "agent-beta"})

	tests := []struct {
		name        string
		agentFilter string
		wantCrons   int
		wantOneshot int
		wantHB      int
	}{
		{"filter agent-alpha", "agent-alpha", 1, 1, 1},
		{"filter agent-beta", "agent-beta", 1, 1, 1},
		{"wildcard filter all", "*", 2, 2, 2},
		{"empty filter all", "", 2, 2, 2},
		{"non-existent agent", "agent-none", 0, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			crons, err := ListCronSchedules(ctx, db, "", tc.agentFilter)
			if err != nil {
				t.Fatalf("ListCronSchedules err = %v", err)
			}
			if len(crons) != tc.wantCrons {
				t.Errorf("ListCronSchedules count = %d, want %d", len(crons), tc.wantCrons)
			}
			oneshots, err := ListOneShotSchedules(ctx, db, "", tc.agentFilter)
			if err != nil {
				t.Fatalf("ListOneShotSchedules err = %v", err)
			}
			if len(oneshots) != tc.wantOneshot {
				t.Errorf("ListOneShotSchedules count = %d, want %d", len(oneshots), tc.wantOneshot)
			}
			hbs, err := ListHeartbeatSchedules(ctx, db, "", tc.agentFilter)
			if err != nil {
				t.Fatalf("ListHeartbeatSchedules err = %v", err)
			}
			if len(hbs) != tc.wantHB {
				t.Errorf("ListHeartbeatSchedules count = %d, want %d", len(hbs), tc.wantHB)
			}
			metrics, err := GetScheduleSummaryMetrics(ctx, db, tc.agentFilter)
			if err != nil {
				t.Fatalf("GetScheduleSummaryMetrics err = %v", err)
			}
			if metrics.CronCount != tc.wantCrons || metrics.OneShotCount != tc.wantOneshot || metrics.HeartbeatCount != tc.wantHB {
				t.Errorf("summary metrics %+v, want crons=%d oneshot=%d hb=%d", metrics, tc.wantCrons, tc.wantOneshot, tc.wantHB)
			}
		})
	}
}
