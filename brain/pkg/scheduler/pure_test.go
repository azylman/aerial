package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func mkCron(id, targetID, prefix, expr, prompt, tz, effort string, nextRun time.Time) db.CronSchedule {
	return db.CronSchedule{ID: id, TargetID: targetID, TitlePrefix: prefix, CronExpr: expr, Prompt: prompt, Timezone: tz, Effort: effort, NextRunAt: nextRun, Enabled: true}
}

func mkOneShot(id, threadID, prompt, effort string, runAt time.Time) db.OneShotSchedule {
	return db.OneShotSchedule{ID: id, ThreadID: threadID, Prompt: prompt, Effort: effort, RunAt: runAt}
}

func TestCalculateNextRun_TableDriven(t *testing.T) {
	baseTime := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		cronExpr  string
		timezone  string
		from      time.Time
		expectErr bool
		expected  time.Time
	}{
		{"zero from timestamp", "0 20 * * 5", "UTC", time.Time{}, true, time.Time{}},
		{"empty timezone", "0 20 * * 5", "   ", baseTime, true, time.Time{}},
		{"invalid cron expression", "invalid-cron", "UTC", baseTime, true, time.Time{}},
		{"valid 5-field UTC cron", "0 20 * * 5", "UTC", baseTime, false, time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)},
		{"valid descriptor @daily", "@daily", "UTC", baseTime, false, time.Date(2026, time.August, 29, 0, 0, 0, 0, time.UTC)},
		{"timezone America/Los_Angeles", "0 9 * * *", "America/Los_Angeles", baseTime, false, time.Date(2026, time.August, 28, 16, 0, 0, 0, time.UTC)},
		{"timezone America/New_York", "0 9 * * *", "America/New_York", baseTime, false, time.Date(2026, time.August, 28, 13, 0, 0, 0, time.UTC)},
		{"unknown timezone falls back to UTC", "0 20 * * *", "Invalid/Unknown_TZ", baseTime, false, time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CalculateNextRun(tc.cronExpr, tc.timezone, tc.from)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestFormatThreadTitle_TableDriven(t *testing.T) {
	testTime := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		prefix   string
		expected string
		maxRunes int
	}{
		{"empty prefix", "", "Scheduled Routine – Aug 28, 2026", 100},
		{"whitespace prefix", "   \t\n  ", "Scheduled Routine – Aug 28, 2026", 100},
		{"custom prefix", "Weekly Meal Plan", "Weekly Meal Plan – Aug 28, 2026", 100},
		{"long ASCII prefix exceeding 100 runes", strings.Repeat("A", 120), strings.Repeat("A", 97) + "...", 100},
		{"multi-byte unicode prefix exceeding 100 runes", strings.Repeat("🚀", 120), strings.Repeat("🚀", 97) + "...", 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatThreadTitle(tc.prefix, testTime)
			runes := []rune(got)
			if len(runes) > tc.maxRunes {
				t.Errorf("expected <= %d runes, got %d", tc.maxRunes, len(runes))
			}
			if tc.prefix == "" || tc.prefix == "   \t\n  " || tc.prefix == "Weekly Meal Plan" {
				if got != tc.expected {
					t.Errorf("expected %q, got %q", tc.expected, got)
				}
			} else {
				if !strings.HasSuffix(got, "...") || len(runes) != 100 {
					t.Errorf("expected 100 runes ending in '...', got %d: %q", len(runes), got)
				}
			}
		})
	}
}

func TestIsScheduleDue_TableDriven(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		nextRunAt time.Time
		now       time.Time
		expected  bool
	}{
		{"zero nextRunAt", time.Time{}, now, false},
		{"zero now", now, time.Time{}, false},
		{"future nextRunAt", now.Add(5 * time.Minute), now, false},
		{"exact match", now, now, true},
		{"past nextRunAt", now.Add(-5 * time.Minute), now, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsScheduleDue(tc.nextRunAt, tc.now); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestEvaluateCronStaleness_TableDriven(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		nextRunAt time.Time
		now       time.Time
		expected  bool
	}{
		{"zero nextRunAt", time.Time{}, now, false},
		{"zero now", now, time.Time{}, false},
		{"future nextRunAt", now.Add(1 * time.Hour), now, false},
		{"recent overdue (< 24h)", now.Add(-2 * time.Hour), now, false},
		{"exactly 24h overdue", now.Add(-24 * time.Hour), now, false},
		{"stale overdue (> 24h)", now.Add(-24*time.Hour - time.Second), now, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := EvaluateCronStaleness(tc.nextRunAt, tc.now); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestResolveCronTimezone_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		scheduleTz string
		defaultTz  string
		expected   string
	}{
		{"schedule timezone takes precedence", "America/New_York", "America/Los_Angeles", "America/New_York"},
		{"schedule timezone trimmed", "  Asia/Tokyo  ", "UTC", "Asia/Tokyo"},
		{"empty schedule tz falls back to default", "   ", "America/Los_Angeles", "America/Los_Angeles"},
		{"both empty fall back to UTC", "", "   ", "UTC"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveCronTimezone(tc.scheduleTz, tc.defaultTz); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestShouldCreateThread_TableDriven(t *testing.T) {
	tests := []struct {
		name             string
		isAlreadyThread  bool
		policyMode       string
		hasThreadCreator bool
		expected         bool
	}{
		{"target is already a thread", true, "threads", true, false},
		{"channel policy is channel mode", false, "channel", true, false},
		{"channel policy is CHANNEL (case insensitive)", false, " CHANNEL ", true, false},
		{"thread creator is nil/false", false, "threads", false, false},
		{"valid thread mode with creator", false, "threads", true, true},
		{"default/empty policy mode with creator", false, "", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldCreateThread(tc.isAlreadyThread, tc.policyMode, tc.hasThreadCreator); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestBuildMessageSummaries_TableDriven(t *testing.T) {
	tests := []struct {
		name           string
		titlePrefix    string
		prompt         string
		expectedCron   string
		expectedPrompt string
	}{
		{"empty prefix", "", "Review morning telemetry", "Review morning telemetry", "[Reminder] Review morning telemetry"},
		{"whitespace prefix", "   ", "Review morning telemetry", "Review morning telemetry", "[Reminder] Review morning telemetry"},
		{"custom prefix", "Daily Telemetry", "Review morning telemetry", "[Daily Telemetry] Review morning telemetry", "[Reminder] Review morning telemetry"},
		{"prompt with user request tags", "Routine", "<USER_REQUEST>\nPrompt:\nDaily review\n</USER_REQUEST>", "[Routine] Daily review", "[Reminder] Daily review"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if cronSum := BuildCronMessageSummary(tc.titlePrefix, tc.prompt); cronSum != tc.expectedCron {
				t.Errorf("BuildCronMessageSummary: expected %q, got %q", tc.expectedCron, cronSum)
			}
			if oneShotSum := BuildOneShotMessageSummary(tc.prompt); oneShotSum != tc.expectedPrompt {
				t.Errorf("BuildOneShotMessageSummary: expected %q, got %q", tc.expectedPrompt, oneShotSum)
			}
		})
	}
}

func TestPlanDueCronTurn_TableDriven(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		cron        db.CronSchedule
		now         time.Time
		wantStale   bool
		wantFire    bool
		wantErr     bool
		wantThread  bool
		wantEffort  string
		wantNextRun time.Time
	}{
		{"fresh due cron", mkCron("cron-1", "chan-1", "Health Check", "0 20 * * 5", "Check node health", "UTC", "high", now.Add(-5*time.Minute)), now, false, true, false, true, "high", time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)},
		{"stale overdue >24h", mkCron("cron-stale", "", "", "0 20 * * 5", "", "", "", now.Add(-25*time.Hour)), now, true, false, false, false, "", time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)},
		{"invalid cron 24h fallback", mkCron("cron-invalid", "", "", "invalid-syntax", "", "", "", now.Add(-10*time.Minute)), now, false, true, true, false, "", now.Add(24 * time.Hour)},
		{"zero now timestamp fallback", mkCron("cron-zero-now", "", "", "@daily", "", "", "", time.Now().Add(-time.Minute)), time.Time{}, false, true, false, false, "", time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanDueCronTurn(tc.cron, tc.now, false, "threads", tc.wantThread, "UTC")
			if got.IsStale != tc.wantStale || got.ShouldFire != tc.wantFire || (got.NextRunError != nil) != tc.wantErr || got.ShouldCreateThread != tc.wantThread {
				t.Fatalf("got %+v, want stale=%v fire=%v err=%v thread=%v", got, tc.wantStale, tc.wantFire, tc.wantErr, tc.wantThread)
			}
			if tc.wantEffort != "" && got.Effort != tc.wantEffort {
				t.Errorf("effort: got %q, want %q", got.Effort, tc.wantEffort)
			}
			if !tc.wantNextRun.IsZero() && !got.NextRunAt.Equal(tc.wantNextRun) {
				t.Errorf("next run: got %v, want %v", got.NextRunAt, tc.wantNextRun)
			}
			if tc.wantNextRun.IsZero() && got.NextRunAt.IsZero() {
				t.Errorf("expected non-zero NextRunAt")
			}
		})
	}
}

func TestStructBuilders_TableDriven(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		cron         db.CronSchedule
		oneShot      db.OneShotSchedule
		title        string
		summary      string
		targetThread string
		wantRunType  string
		wantTargetID string
		wantTitle    string
		wantPrompt   string
		wantEffort   string
	}{
		{
			name:         "cron standard",
			cron:         mkCron("cron-1", "chan-main", "Weekly Report", "0 9 * * 1", "Compile weekly report", "UTC", "medium", now),
			title:        "Weekly Report – Aug 28, 2026",
			summary:      "[Weekly Report] Compile weekly report",
			targetThread: "th-100",
			wantRunType:  "cron",
			wantTargetID: "chan-main",
			wantTitle:    "Weekly Report – Aug 28, 2026",
			wantPrompt:   "Compile weekly report",
			wantEffort:   "medium",
		},
		{
			name:         "oneshot standard",
			oneShot:      mkOneShot("oneshot-1", "th-200", "Submit timesheet", "standard", now),
			title:        "One-shot Reminder",
			summary:      "[Reminder] Submit timesheet",
			targetThread: "th-200",
			wantRunType:  "one_shot",
			wantTargetID: "th-200",
			wantTitle:    "One-shot Reminder",
			wantPrompt:   "Submit timesheet",
			wantEffort:   "standard",
		},
		{
			name:         "oneshot low effort propagation",
			oneShot:      mkOneShot("oneshot-2", "th-201", "Low reminder", "low", now),
			title:        "One-shot Reminder",
			summary:      "[Reminder] Low reminder",
			targetThread: "th-201",
			wantRunType:  "one_shot",
			wantTargetID: "th-201",
			wantTitle:    "One-shot Reminder",
			wantPrompt:   "Low reminder",
			wantEffort:   "low",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var run db.ScheduleRun
			var msg db.Message
			if tc.cron.ID != "" {
				run = BuildCronScheduleRun("run-1", "msg-1", tc.targetThread, tc.cron, tc.title, now)
				msg = BuildCronMessage("msg-1", "run-1", tc.targetThread, tc.cron, tc.summary, now)
			} else {
				run = BuildOneShotScheduleRun("run-1", "msg-1", tc.oneShot, now)
				msg = BuildOneShotMessage("msg-1", "run-1", tc.oneShot, tc.summary, now)
			}
			if run.ScheduleType != tc.wantRunType || run.TargetID != tc.wantTargetID || run.Title != tc.wantTitle || run.Prompt != tc.wantPrompt || run.Effort != tc.wantEffort {
				t.Fatalf("run mismatch: %+v", run)
			}
			if msg.ThreadID != tc.targetThread || msg.Summary != tc.summary || msg.Effort != tc.wantEffort || msg.Content != tc.wantPrompt {
				t.Fatalf("msg mismatch: %+v", msg)
			}
		})
	}
}

func TestPeriodicTickPredicates_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		tickCount   int
		wantFactExt bool
		wantPrune   bool
		wantDecay   bool
		wantSync    bool
	}{
		{"negative tick", -10, false, false, false, false},
		{"zero tick", 0, false, false, false, false},
		{"regular tick 1", 1, false, false, false, false},
		{"transcript sync tick 10", 10, false, false, false, true},
		{"memory decay tick 60", 60, false, false, true, true},
		{"fact extraction tick 120", 120, true, false, false, true},
		{"staggered decay 2940", 2940, false, false, true, true},
		{"full daily prune 2880", 2880, true, true, false, true},
		{"double daily prune 5760", 5760, true, true, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldRunFactExtraction(tc.tickCount); got != tc.wantFactExt {
				t.Errorf("ShouldRunFactExtraction: got %v, want %v", got, tc.wantFactExt)
			}
			if got := ShouldRunPruneRetention(tc.tickCount); got != tc.wantPrune {
				t.Errorf("ShouldRunPruneRetention: got %v, want %v", got, tc.wantPrune)
			}
			if got := ShouldRunMemoryDecay(tc.tickCount); got != tc.wantDecay {
				t.Errorf("ShouldRunMemoryDecay: got %v, want %v", got, tc.wantDecay)
			}
			if got := ShouldRunTranscriptSync(tc.tickCount); got != tc.wantSync {
				t.Errorf("ShouldRunTranscriptSync: got %v, want %v", got, tc.wantSync)
			}
		})
	}
}
