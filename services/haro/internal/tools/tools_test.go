package tools

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

func openToolsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	return testutil.NewDB(t)
}

// newMemoryToolHandler opens a hermetic in-memory haro DB and returns a
// ToolHandler over it; the DB closes on test cleanup.
func newMemoryToolHandler(t *testing.T) (*ToolHandler, *sql.DB) {
	t.Helper()
	db := openToolsTestDB(t)
	cfg := &config.Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}
	return NewToolHandler(cfg, db), db
}

func TestParseRunAtWithTimezone(t *testing.T) {
	t.Parallel()
	baseTime := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		input   string
		tz      string
		want    time.Time
		wantErr bool
	}{
		{"relative 30s", "30s", "UTC", baseTime.Add(30 * time.Second), false},
		{"relative 45 seconds", "45 seconds", "UTC", baseTime.Add(45 * time.Second), false},
		{"relative 15m", "15m", "UTC", baseTime.Add(15 * time.Minute), false},
		{"relative 2h", "2h", "UTC", baseTime.Add(2 * time.Hour), false},
		{"relative 1d", "1d", "UTC", baseTime.Add(24 * time.Hour), false},
		{"relative 1w", "1w", "UTC", baseTime.Add(7 * 24 * time.Hour), false},
		{"duration 1h30m", "1h30m", "UTC", baseTime.Add(90 * time.Minute), false},
		{"rfc3339 utc", "2026-08-28T21:00:00Z", "UTC", time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC), false},
		{"rfc3339nano utc", "2026-08-28T21:00:00.123456789Z", "UTC", time.Date(2026, 8, 28, 21, 0, 0, 123456789, time.UTC), false},
		{"local datetime LA", "2026-08-28 15:00:00", "America/Los_Angeles", time.Date(2026, 8, 28, 22, 0, 0, 0, time.UTC), false},
		{"local datetime NY", "2026-08-28 15:00:00", "America/New_York", time.Date(2026, 8, 28, 19, 0, 0, 0, time.UTC), false},
		{"local datetime Tokyo", "2026-08-28 15:00:00", "Asia/Tokyo", time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC), false},
		{"local iso without zone", "2026-08-28T21:00", "America/Los_Angeles", time.Date(2026, 8, 29, 4, 0, 0, 0, time.UTC), false},
		{"local space without seconds", "2026-08-28 21:00", "America/Los_Angeles", time.Date(2026, 8, 29, 4, 0, 0, 0, time.UTC), false},
		{"local date only", "2026-08-28", "America/Los_Angeles", time.Date(2026, 8, 28, 7, 0, 0, 0, time.UTC), false},
		{"empty input", "", "UTC", time.Time{}, true},
		{"invalid duration", "invalid duration", "UTC", time.Time{}, true},
		{"overflow duration", "99999999999999999999999999999999999999999999999s", "UTC", time.Time{}, true},
		{"empty timezone", "15m", "", time.Time{}, true},
		{"invalid timezone", "15m", "Invalid/Timezone", time.Time{}, true},
		{"unrecognized format", "not-a-valid-date", "UTC", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseRunAtWithTimezone(tc.input, tc.tz, baseTime)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseRunAtWithTimezone(%q, %q) err = %v, wantErr = %v", tc.input, tc.tz, err, tc.wantErr)
			}
			if !tc.wantErr && !got.Equal(tc.want) {
				t.Fatalf("ParseRunAtWithTimezone(%q, %q) = %v, want %v", tc.input, tc.tz, got, tc.want)
			}
		})
	}
}

func TestResolveTimezone_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		tz       string
		fallback string
		want     string
	}{
		{"explicit timezone", "America/New_York", "UTC", "America/New_York"},
		{"fallback timezone", "", "America/Chicago", "America/Chicago"},
		{"whitespace explicit timezone", "  America/Denver  ", "UTC", "America/Denver"},
		{"whitespace fallback timezone", "", "  UTC  ", "UTC"},
		{"default timezone", "", "", config.DefaultTimezone},
		{"whitespace both default", "   ", "   ", config.DefaultTimezone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveTimezone(tc.tz, tc.fallback); got != tc.want {
				t.Errorf("resolveTimezone(%q, %q) = %q, want %q", tc.tz, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestNewToolHandler_Table(t *testing.T) {
	oldVanadis := store.GetVanadisURL()
	defer store.SetVanadisURL(oldVanadis)
	tests := []struct {
		name        string
		cfg         *config.Config
		wantPanic   bool
		wantVanadis string
	}{
		{name: "nil config and db panic", wantPanic: true},
		{
			name: "config vanadis URL applied",
			cfg:  &config.Config{DatabaseURL: ":memory:", Timezone: "UTC", VanadisURL: "http://vanadis.tools-test"}, wantVanadis: "http://vanadis.tools-test",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); (r != nil) != tc.wantPanic {
					t.Errorf("recover() = %v, wantPanic %v", r, tc.wantPanic)
				}
			}()
			var db *sql.DB
			if tc.cfg != nil {
				db = openToolsTestDB(t)
			}
			NewToolHandler(tc.cfg, db)
			if got := store.GetVanadisURL(); got != tc.wantVanadis {
				t.Errorf("VanadisURL = %q, want %q", got, tc.wantVanadis)
			}
		})
	}
}

func TestFormatSchedulesSummary_Table(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 28, 22, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		out      ListSchedulesOutput
		contains []string
	}{
		{"empty", ListSchedulesOutput{}, []string{"No active schedules found."}},
		{
			"recurring only",
			ListSchedulesOutput{Recurring: []model.CronSchedule{{ID: "c1", TargetID: "ch", TitlePrefix: "T", CronExpr: "0 0 * * *", Effort: "medium", NextRunAt: now}}},
			[]string{"[RECURRING]", "c1"},
		},
		{
			"one-shot only",
			ListSchedulesOutput{OneShot: []model.OneShotSchedule{{ID: "o1", ThreadID: "th", Effort: "low", RunAt: now}}},
			[]string{"[ONE_SHOT]", "o1"},
		},
		{
			"heartbeat only",
			ListSchedulesOutput{Heartbeats: []model.HeartbeatSchedule{{ID: "h1", TargetID: "ch", CronExpr: "0 * * * *", Effort: "high", NextRunAt: now}}},
			[]string{"[HEARTBEAT]", "h1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatSchedulesSummary(tc.out)
			for _, want := range tc.contains {
				if !strings.Contains(got, want) {
					t.Errorf("FormatSchedulesSummary() = %q, want substring %q", got, want)
				}
			}
		})
	}
}

// newClosedToolHandler returns a handler over an already-closed database.
func newClosedToolHandler(t *testing.T) *ToolHandler {
	t.Helper()
	db := testutil.NewClosedDB(t)
	return NewToolHandler(&config.Config{DatabaseURL: ":memory:", Timezone: "America/Los_Angeles"}, db)
}

// pickToolHandler returns closed for closedDB rows, else open.
func pickToolHandler(open, closed *ToolHandler, closedDB bool) *ToolHandler {
	if closedDB {
		return closed
	}
	return open
}

// assertScheduleCreated checks a Schedule* result: wantErr substring, or success with wantEffort.
func assertScheduleCreated(t *testing.T, status, effort string, err error, wantErr, wantEffort string) {
	t.Helper()
	if wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("expected error containing %q, got %v", wantErr, err)
		}
		return
	}
	if err != nil || status != "success" || effort != wantEffort {
		t.Fatalf("status %q effort %q err %v; want success with effort %q", status, effort, err, wantEffort)
	}
}

func TestToolHandler_ScheduleRecurring(t *testing.T) {
	closed := newClosedToolHandler(t)
	h, db := newMemoryToolHandler(t)
	if db == nil {
		t.Fatal("expected non-nil db")
	}
	ctx := context.Background()
	tests := []struct {
		name       string
		args       ScheduleRecurringArgs
		closedDB   bool
		wantErr    string
		wantEffort string
	}{
		{name: "missing channel_id", args: ScheduleRecurringArgs{CronExpression: "0 9 * * *", Prompt: "p"}, wantErr: "'channel_id' is required"},
		{name: "missing cron_expression", args: ScheduleRecurringArgs{ChannelID: "c1", Prompt: "p"}, wantErr: "'cron_expression' is required"},
		{name: "missing prompt", args: ScheduleRecurringArgs{ChannelID: "c1", CronExpression: "0 9 * * *"}, wantErr: "'prompt' is required"},
		{name: "invalid cron", args: ScheduleRecurringArgs{ChannelID: "c1", CronExpression: "not-a-cron", Prompt: "p"}, wantErr: "invalid cron expression"},
		{
			name: "closed db",
			args: ScheduleRecurringArgs{ChannelID: "c", CronExpression: "0 9 * * *", Prompt: "p"}, closedDB: true, wantErr: "database is closed",
		},
		{name: "default tz and effort", args: ScheduleRecurringArgs{ChannelID: "ch-1", CronExpression: "0 9 * * *", Prompt: "p1"}, wantEffort: "medium"},
		{
			name:       "explicit tz and high effort",
			args:       ScheduleRecurringArgs{ChannelID: "ch-2", CronExpression: "0 20 * * 5", Prompt: "p2", Timezone: "America/New_York", Effort: "high", TitlePrefix: "Weekly"},
			wantEffort: "high",
		},
		{
			name: "untrimmed lower effort",
			args: ScheduleRecurringArgs{ChannelID: "ch-3", CronExpression: "0 9 * * *", Prompt: "p3", Effort: "  LOW  "}, wantEffort: "low",
		},
		{
			name: "invalid effort fallback",
			args: ScheduleRecurringArgs{ChannelID: "ch-4", CronExpression: "0 9 * * *", Prompt: "p4", Effort: "unknown"}, wantEffort: "medium",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := pickToolHandler(h, closed, tc.closedDB).ScheduleRecurring(ctx, tc.args)
			assertScheduleCreated(t, res.Status, res.Effort, err, tc.wantErr, tc.wantEffort)
		})
	}
}

func TestToolHandler_ScheduleOnce(t *testing.T) {
	closed := newClosedToolHandler(t)
	h, db := newMemoryToolHandler(t)
	if db == nil {
		t.Fatal("expected non-nil db")
	}
	ctx := context.Background()
	tests := []struct {
		name       string
		args       ScheduleOnceArgs
		closedDB   bool
		wantErr    string
		wantEffort string
	}{
		{name: "missing target_id", args: ScheduleOnceArgs{RunAt: "15m", Prompt: "p"}, wantErr: "'target_id' is required"},
		{name: "missing run_at", args: ScheduleOnceArgs{TargetID: "t1", Prompt: "p"}, wantErr: "'run_at' is required"},
		{name: "missing prompt", args: ScheduleOnceArgs{TargetID: "t1", RunAt: "15m"}, wantErr: "'prompt' is required"},
		{name: "invalid run_at", args: ScheduleOnceArgs{TargetID: "t1", RunAt: "bad-time", Prompt: "p"}, wantErr: "unrecognized run_at format"},
		{name: "closed db", args: ScheduleOnceArgs{TargetID: "t", RunAt: "15m", Prompt: "p"}, closedDB: true, wantErr: "database is closed"},
		{name: "target_id relative medium effort", args: ScheduleOnceArgs{TargetID: "t-1", RunAt: "10m", Prompt: "p1"}, wantEffort: "medium"},
		{name: "thread_id explicit high effort", args: ScheduleOnceArgs{ThreadID: "t-2", RunAt: "15m", Prompt: "p2", Effort: "high"}, wantEffort: "high"},
		{
			name:       "untrimmed effort and absolute tz",
			args:       ScheduleOnceArgs{TargetID: "t-3", RunAt: "2026-08-28 20:00:00", Timezone: "America/Los_Angeles", Prompt: "p3", Effort: "  LOW  "},
			wantEffort: "low",
		},
		{name: "invalid effort fallback", args: ScheduleOnceArgs{TargetID: "t-4", RunAt: "1h", Prompt: "p4", Effort: "invalid"}, wantEffort: "medium"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := pickToolHandler(h, closed, tc.closedDB).ScheduleOnce(ctx, tc.args)
			assertScheduleCreated(t, res.Status, res.Effort, err, tc.wantErr, tc.wantEffort)
		})
	}
}

// seedToolSchedules stores rows with empty effort (read back as medium) for target ch-raw, a one-shot
// inserted with a nil context for t-zero, and heartbeats for ch-hb-list and ch-raw.
func seedToolSchedules(t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	sZero := model.OneShotSchedule{ID: "s-zero", ThreadID: "t-zero", Prompt: "p", RunAt: now.Add(time.Hour)}
	if err := store.InsertOneShotSchedule(nil, db, sZero); err != nil {
		t.Fatalf("InsertOneShotSchedule nil ctx error: %v", err)
	}
	listZero, err := store.ListOneShotSchedules(context.Background(), db, "t-zero")
	if err != nil || len(listZero) != 1 || listZero[0].CreatedAt.IsZero() {
		t.Fatalf("expected non-zero CreatedAt, got %+v, err: %v", listZero, err)
	}
	for _, stmt := range []string{
		"INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ('c-empty', 'ch-raw', '', '0 0 * * *', 'p', 'UTC', $1, TRUE, $2, '')",
		"INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ('o-empty', 'ch-raw', 'p', $1, $2, '')",
	} {
		if _, err := db.Exec(stmt, now, now); err != nil {
			t.Fatalf("raw insert err = %v", err)
		}
	}
	for _, hb := range []model.HeartbeatSchedule{
		{ID: "hb-list", TargetID: "ch-hb-list", CronExpr: "0 0 * * *", Prompt: "hb-p", NextRunAt: now, Enabled: true},
		{ID: "hb-get", TargetID: "ch-hb-get", CronExpr: "0 0 * * *", Prompt: "hb-p", NextRunAt: now, Enabled: true},
	} {
		if err := store.InsertHeartbeatSchedule(context.Background(), db, hb); err != nil {
			t.Fatalf("InsertHeartbeatSchedule err = %v", err)
		}
	}
}

func TestToolHandler_ListSchedules(t *testing.T) {
	closed := newClosedToolHandler(t)
	h, db := newMemoryToolHandler(t)
	seedToolSchedules(t, db)
	ctx := context.Background()
	tests := []struct {
		name           string
		args           ListSchedulesArgs
		closedDB       bool
		wantRecurring  int
		wantOneShot    int
		wantHeartbeats int
		wantEffort     string
		wantErr        string
	}{
		{name: "unfiltered lists all", wantRecurring: 1, wantOneShot: 2, wantHeartbeats: 2},
		{name: "empty stored effort reads medium", args: ListSchedulesArgs{TargetID: "ch-raw"}, wantRecurring: 1, wantOneShot: 1, wantEffort: "medium"},
		{name: "heartbeat target", args: ListSchedulesArgs{TargetID: "ch-hb-list"}, wantHeartbeats: 1},
		{name: "closed db", args: ListSchedulesArgs{TargetID: "t"}, closedDB: true, wantErr: "failed to query recurring schedules"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := pickToolHandler(h, closed, tc.closedDB).ListSchedules(ctx, tc.args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil || len(out.Recurring) != tc.wantRecurring || len(out.OneShot) != tc.wantOneShot || len(out.Heartbeats) != tc.wantHeartbeats {
				t.Fatalf("ListSchedules = %+v, err: %v; want %d recurring, %d one-shot, %d heartbeats", out, err, tc.wantRecurring, tc.wantOneShot, tc.wantHeartbeats)
			}
			assertListedWithoutPrompts(t, out, tc.wantEffort)
		})
	}
}

// assertListedWithoutPrompts checks ListSchedules omits prompts everywhere and, when wantEffort is set,
// reports it on every recurring and one-shot row.
func assertListedWithoutPrompts(t *testing.T, out ListSchedulesOutput, wantEffort string) {
	t.Helper()
	if strings.Contains(out.Summary, "Prompt:") {
		t.Errorf("summary includes prompts: %q", out.Summary)
	}
	for _, r := range out.Recurring {
		if r.Prompt != "" || (wantEffort != "" && r.Effort != wantEffort) {
			t.Errorf("recurring %+v, want no prompt and effort %q", r, wantEffort)
		}
	}
	for _, o := range out.OneShot {
		if o.Prompt != "" || (wantEffort != "" && o.Effort != wantEffort) {
			t.Errorf("one-shot %+v, want no prompt and effort %q", o, wantEffort)
		}
	}
	for _, hb := range out.Heartbeats {
		if hb.Prompt != "" {
			t.Errorf("heartbeat %+v, want no prompt", hb)
		}
	}
}

func TestToolHandler_GetSchedule(t *testing.T) {
	closed := newClosedToolHandler(t)
	h, db := newMemoryToolHandler(t)
	seedToolSchedules(t, db)
	ctx := context.Background()
	tests := []struct {
		name       string
		id         string
		closedDB   bool
		wantType   string
		wantPrompt string
		wantErr    string
	}{
		{name: "recurring", id: "c-empty", wantType: "recurring", wantPrompt: "p"},
		{name: "one-shot", id: "o-empty", wantType: "one_shot", wantPrompt: "p"},
		{name: "heartbeat", id: "hb-get", wantType: "heartbeat", wantPrompt: "hb-p"},
		{name: "empty schedule_id", id: "  ", wantErr: "'schedule_id' is required"},
		{name: "missing schedule", id: "missing-id", wantErr: `schedule with ID "missing-id" not found`},
		{name: "closed db", id: "c1", closedDB: true, wantErr: "failed to query recurring schedule c1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := pickToolHandler(h, closed, tc.closedDB).GetSchedule(ctx, GetScheduleArgs{ScheduleID: tc.id})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil || out.Type != tc.wantType {
				t.Fatalf("GetSchedule = %+v, err: %v; want type %q", out, err, tc.wantType)
			}
			var prompt string
			switch {
			case out.Recurring != nil:
				prompt = out.Recurring.Prompt
			case out.OneShot != nil:
				prompt = out.OneShot.Prompt
			case out.Heartbeat != nil:
				prompt = out.Heartbeat.Prompt
			}
			if prompt != tc.wantPrompt {
				t.Errorf("prompt = %q, want %q", prompt, tc.wantPrompt)
			}
		})
	}
}

func TestToolHandler_UpdateCronSchedule(t *testing.T) {
	closed := newClosedToolHandler(t)
	h, db := newMemoryToolHandler(t)
	ctx := context.Background()

	now := time.Now().UTC()
	for _, c := range []model.CronSchedule{
		{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "prompt", Timezone: "America/Los_Angeles", NextRunAt: now, Enabled: true, Effort: "medium"},
		{ID: "c-tokyo", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "prompt", Timezone: "Asia/Tokyo", NextRunAt: now, Enabled: true, Effort: "medium"},
	} {
		if err := store.InsertCronSchedule(ctx, db, c); err != nil {
			t.Fatalf("InsertCronSchedule %s error: %v", c.ID, err)
		}
	}

	// Rows run sequentially and share the seeded schedules: later c-tokyo rows depend on the timezone stored by earlier ones.
	eff, expr, prompt, prefix, tzUTC := "high", "0 12 * * *", "updated", "NewPrefix", "UTC"
	cron9, cron1730, tzNY, badExpr := "0 9 * * *", "30 17 * * *", "America/New_York", "not-a-cron"
	tests := []struct {
		name       string
		args       UpdateCronScheduleArgs
		wantErrMsg string
		wantEffort string
		wantTZ     string
		wantClock  string
		closedDB   bool
	}{
		{"full update recomputes next run", UpdateCronScheduleArgs{ScheduleID: "c1", Effort: &eff, CronExpression: &expr, Prompt: &prompt, TitlePrefix: &prefix, Timezone: &tzUTC}, "", "high", "UTC", "12:00", false},
		{"cron-only update uses stored timezone", UpdateCronScheduleArgs{ScheduleID: "c-tokyo", CronExpression: &cron9}, "", "", "Asia/Tokyo", "09:00", false},
		{"timezone-only update recomputes with stored cron", UpdateCronScheduleArgs{ScheduleID: "c-tokyo", Timezone: &tzNY}, "", "", "America/New_York", "09:00", false},
		{"cron-only update after timezone change uses new stored timezone", UpdateCronScheduleArgs{ScheduleID: "c-tokyo", CronExpression: &cron1730}, "", "", "America/New_York", "17:30", false},
		{"content-only update leaves next_run_at untouched", UpdateCronScheduleArgs{ScheduleID: "c-tokyo", Prompt: &prompt}, "", "", "", "", false},
		{"missing schedule_id", UpdateCronScheduleArgs{ScheduleID: "  "}, "'schedule_id' is required", "", "", "", false},
		{"invalid cron expression", UpdateCronScheduleArgs{ScheduleID: "c1", CronExpression: &badExpr}, "invalid cron expression", "", "", "", false},
		{"non-existent schedule", UpdateCronScheduleArgs{ScheduleID: "missing-id", Effort: &eff}, "failed to update recurring schedule", "", "", "", false},
		{"non-existent schedule with cron change", UpdateCronScheduleArgs{ScheduleID: "missing-id", CronExpression: &cron9}, "failed to update recurring schedule", "", "", "", false},
		{"closed db", UpdateCronScheduleArgs{ScheduleID: "c1", Effort: &eff}, "failed to update recurring schedule", "", "", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := pickToolHandler(h, closed, tc.closedDB).UpdateCronSchedule(ctx, tc.args)
			if tc.wantErrMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
				}
				return
			}
			if err != nil || out.Status != "success" {
				t.Fatalf("UpdateCronSchedule = %+v, err: %v", out, err)
			}
			if tc.wantEffort != "" && out.Effort != tc.wantEffort {
				t.Errorf("Effort = %q, want %q", out.Effort, tc.wantEffort)
			}
			assertNextRunClock(t, out.NextRunAt, tc.wantTZ, tc.wantClock)
		})
	}
}

// assertNextRunClock checks nextRunAt falls within the next day at wantClock in wantTZ; an empty wantClock means
// next_run_at must be omitted.
func assertNextRunClock(t *testing.T, nextRunAt, wantTZ, wantClock string) {
	t.Helper()
	if wantClock == "" {
		if nextRunAt != "" {
			t.Fatalf("expected no next_run_at, got %q", nextRunAt)
		}
		return
	}
	got, err := time.Parse(time.RFC3339, nextRunAt)
	if err != nil {
		t.Fatalf("parse next_run_at %q: %v", nextRunAt, err)
	}
	loc, err := time.LoadLocation(wantTZ)
	if err != nil {
		t.Fatalf("LoadLocation %q: %v", wantTZ, err)
	}
	if clock := got.In(loc).Format("15:04"); clock != wantClock {
		t.Fatalf("next_run_at %s in %s = %s, want %s", nextRunAt, wantTZ, clock, wantClock)
	}
	if d := time.Until(got); d <= 0 || d > 25*time.Hour {
		t.Fatalf("next_run_at %s not within the next day", nextRunAt)
	}
}

func TestToolHandler_CancelSchedule(t *testing.T) {
	closed := newClosedToolHandler(t)
	h, db := newMemoryToolHandler(t)
	ctx := context.Background()

	now := time.Now().UTC()
	c := model.CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "prompt", Timezone: "America/Los_Angeles", NextRunAt: now, Enabled: true, Effort: "medium"}
	if err := store.InsertCronSchedule(ctx, db, c); err != nil {
		t.Fatalf("InsertCronSchedule error: %v", err)
	}
	s := model.OneShotSchedule{ID: "s1", ThreadID: "t1", Prompt: "prompt", RunAt: now, Effort: "medium"}
	if err := store.InsertOneShotSchedule(ctx, db, s); err != nil {
		t.Fatalf("InsertOneShotSchedule error: %v", err)
	}

	tests := []struct {
		name       string
		id         string
		closedDB   bool
		wantErrMsg string
	}{
		{"cancels recurring schedule", "c1", false, ""},
		{"cancels one-shot schedule", "s1", false, ""},
		{"empty schedule_id", "  ", false, "'schedule_id' is required"},
		{"non-existent schedule", "missing-id", false, "not found"},
		{"closed db", "s1", true, "database is closed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := pickToolHandler(h, closed, tc.closedDB).CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: tc.id})
			if tc.wantErrMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
				}
				return
			}
			if err != nil || out.Status != "success" {
				t.Fatalf("CancelSchedule %s = %+v, err: %v", tc.id, out, err)
			}
		})
	}
}

func TestToolHandler_RestateArmingAndCancelling(t *testing.T) {
	t.Parallel()
	db := openToolsTestDB(t)
	ctx := context.Background()

	var captured []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = append(captured, r.URL.Path+" key="+r.Header.Get("Idempotency-Key"))
		// A failed arm or cancel is only logged; every tool call still succeeds.
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := &config.Config{
		DatabaseURL:       ":memory:",
		Timezone:          "UTC",
		RestateIngressURL: srv.URL,
	}
	h := NewToolHandler(cfg, db)
	h.httpClient = srv.Client()

	// 1. ScheduleRecurring arms
	recOut, err := h.ScheduleRecurring(ctx, ScheduleRecurringArgs{
		ChannelID:      "100000000000000001",
		CronExpression: "0 * * * *",
		Prompt:         "Prompt recurring",
	})
	if err != nil || recOut.ScheduleID == "" {
		t.Fatalf("ScheduleRecurring error: %v", err)
	}

	// 2. ScheduleOnce arms
	onceOut, err := h.ScheduleOnce(ctx, ScheduleOnceArgs{
		ThreadID: "100000000000000001",
		Prompt:   "Prompt once",
		RunAt:    "2099-01-01T10:00:00Z",
	})
	if err != nil || onceOut.ScheduleID == "" {
		t.Fatalf("ScheduleOnce error: %v", err)
	}

	// 3. UpdateCronSchedule (enabled) arms
	newExpr := "*/10 * * * *"
	upOut, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID:     recOut.ScheduleID,
		CronExpression: &newExpr,
	})
	if err != nil || upOut.Status != "success" {
		t.Fatalf("UpdateCronSchedule error: %v", err)
	}

	// 4. UpdateCronSchedule on disabled schedule cancels, keyed by its armed tick
	if insErr := store.InsertCronSchedule(ctx, db, model.CronSchedule{
		ID:        "c-disabled-1",
		TargetID:  "100000000000000001",
		CronExpr:  "0 * * * *",
		Prompt:    "Disabled prompt",
		NextRunAt: time.Date(2099, 1, 1, 10, 0, 0, 0, time.UTC),
		Enabled:   false,
	}); insErr != nil {
		t.Fatalf("InsertCronSchedule failed: %v", insErr)
	}
	newPrompt := "Still disabled"
	upDisOut, err := h.UpdateCronSchedule(ctx, UpdateCronScheduleArgs{
		ScheduleID: "c-disabled-1",
		Prompt:     &newPrompt,
	})
	if err != nil || upDisOut.Status != "success" {
		t.Fatalf("UpdateCronSchedule disabled error: %v", err)
	}

	// 5. CancelSchedule cancels, keyed by the deleted schedule alone
	canOut, err := h.CancelSchedule(ctx, CancelScheduleArgs{ScheduleID: recOut.ScheduleID})
	if err != nil || canOut.Status != "success" {
		t.Fatalf("CancelSchedule error: %v", err)
	}

	want := []string{
		"/HaroSchedule/" + recOut.ScheduleID + "/Arm/send key=haro:arm:cron:" + recOut.ScheduleID + ":" + utcTick(t, recOut.NextRunAt),
		"/HaroSchedule/" + onceOut.ScheduleID + "/Arm/send key=haro:arm:one_shot:" + onceOut.ScheduleID + ":2099-01-01T10:00:00Z",
		"/HaroSchedule/" + recOut.ScheduleID + "/Arm/send key=haro:arm:cron:" + recOut.ScheduleID + ":" + utcTick(t, upOut.NextRunAt),
		"/HaroSchedule/c-disabled-1/Cancel/send key=haro:cancel:c-disabled-1:2099-01-01T10:00:00Z",
		"/HaroSchedule/" + recOut.ScheduleID + "/Cancel/send key=haro:cancel:" + recOut.ScheduleID,
	}
	if strings.Join(captured, "\n") != strings.Join(want, "\n") {
		t.Errorf("ingress requests =\n%s\nwant\n%s", strings.Join(captured, "\n"), strings.Join(want, "\n"))
	}
}

// utcTick renders a tool-reported RFC3339 due tick the way ingress keys carry it (UTC).
func utcTick(t *testing.T, reported string) string {
	t.Helper()
	tick, err := time.Parse(time.RFC3339, reported)
	if err != nil {
		t.Fatalf("parse reported tick %q: %v", reported, err)
	}
	return tick.UTC().Format(time.RFC3339)
}

func TestToolHandler_ArmScheduleTick(t *testing.T) {
	t.Parallel()
	due := time.Date(2026, 11, 23, 9, 30, 0, 0, time.UTC)
	tests := []struct {
		name        string
		noIngress   bool
		id          string
		status      int
		wantErr     bool
		wantRequest string
	}{
		{name: "no ingress configured skips arming", noIngress: true, id: "os-1"},
		{name: "blank id skips arming", id: "  ", status: http.StatusAccepted},
		{name: "arm keyed by type id and due tick", id: " os-2 ", status: http.StatusAccepted, wantRequest: "/HaroSchedule/os-2/Arm/send key=haro:arm:one_shot:os-2:2026-11-23T09:30:00Z"},
		{name: "ingress failure is returned", id: "os-3", status: http.StatusServiceUnavailable, wantErr: true, wantRequest: "/HaroSchedule/os-3/Arm/send key=haro:arm:one_shot:os-3:2026-11-23T09:30:00Z"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = append(got, r.URL.Path+" key="+r.Header.Get("Idempotency-Key"))
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)
			cfg := &config.Config{RestateIngressURL: srv.URL}
			if tc.noIngress {
				cfg.RestateIngressURL = ""
			}
			err := NewToolHandler(cfg, nil).ArmScheduleTick(context.Background(), model.ScheduleTypeOneShot, tc.id, due)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ArmScheduleTick err = %v, wantErr %v", err, tc.wantErr)
			}
			want := []string(nil)
			if tc.wantRequest != "" {
				want = []string{tc.wantRequest}
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Errorf("requests = %v, want %v", got, want)
			}
		})
	}
}
