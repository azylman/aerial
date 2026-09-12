package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

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
		{
			name:      "zero from timestamp",
			cronExpr:  "0 20 * * 5",
			timezone:  "UTC",
			from:      time.Time{},
			expectErr: true,
		},
		{
			name:      "empty timezone",
			cronExpr:  "0 20 * * 5",
			timezone:  "   ",
			from:      baseTime,
			expectErr: true,
		},
		{
			name:      "invalid cron expression",
			cronExpr:  "invalid-cron",
			timezone:  "UTC",
			from:      baseTime,
			expectErr: true,
		},
		{
			name:      "valid 5-field UTC cron",
			cronExpr:  "0 20 * * 5",
			timezone:  "UTC",
			from:      baseTime,
			expectErr: false,
			expected:  time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC),
		},
		{
			name:      "valid descriptor @daily",
			cronExpr:  "@daily",
			timezone:  "UTC",
			from:      baseTime,
			expectErr: false,
			expected:  time.Date(2026, time.August, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "timezone America/Los_Angeles",
			cronExpr:  "0 9 * * *",
			timezone:  "America/Los_Angeles",
			from:      baseTime,
			expectErr: false,
			expected:  time.Date(2026, time.August, 28, 16, 0, 0, 0, time.UTC),
		},
		{
			name:      "timezone America/New_York",
			cronExpr:  "0 9 * * *",
			timezone:  "America/New_York",
			from:      baseTime,
			expectErr: false,
			expected:  time.Date(2026, time.August, 28, 13, 0, 0, 0, time.UTC),
		},
		{
			name:      "unknown timezone falls back to UTC",
			cronExpr:  "0 20 * * *",
			timezone:  "Invalid/Unknown_TZ",
			from:      baseTime,
			expectErr: false,
			expected:  time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC),
		},
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
		{
			name:     "empty prefix",
			prefix:   "",
			expected: "Scheduled Routine – Aug 28, 2026",
			maxRunes: 100,
		},
		{
			name:     "whitespace prefix",
			prefix:   "   \t\n  ",
			expected: "Scheduled Routine – Aug 28, 2026",
			maxRunes: 100,
		},
		{
			name:     "custom prefix",
			prefix:   "Weekly Meal Plan",
			expected: "Weekly Meal Plan – Aug 28, 2026",
			maxRunes: 100,
		},
		{
			name:     "long ASCII prefix exceeding 100 runes",
			prefix:   strings.Repeat("A", 120),
			expected: strings.Repeat("A", 97) + "...",
			maxRunes: 100,
		},
		{
			name:     "multi-byte unicode prefix exceeding 100 runes",
			prefix:   strings.Repeat("🚀", 120),
			expected: strings.Repeat("🚀", 97) + "...",
			maxRunes: 100,
		},
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
				if !strings.HasSuffix(got, "...") {
					t.Errorf("expected title to end with '...', got %q", got)
				}
				if len(runes) != 100 {
					t.Errorf("expected exactly 100 runes, got %d", len(runes))
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
		{
			name:      "zero nextRunAt",
			nextRunAt: time.Time{},
			now:       now,
			expected:  false,
		},
		{
			name:      "zero now",
			nextRunAt: now,
			now:       time.Time{},
			expected:  false,
		},
		{
			name:      "future nextRunAt",
			nextRunAt: now.Add(5 * time.Minute),
			now:       now,
			expected:  false,
		},
		{
			name:      "exact match",
			nextRunAt: now,
			now:       now,
			expected:  true,
		},
		{
			name:      "past nextRunAt",
			nextRunAt: now.Add(-5 * time.Minute),
			now:       now,
			expected:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsScheduleDue(tc.nextRunAt, tc.now)
			if got != tc.expected {
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
		{
			name:      "zero nextRunAt",
			nextRunAt: time.Time{},
			now:       now,
			expected:  false,
		},
		{
			name:      "zero now",
			nextRunAt: now,
			now:       time.Time{},
			expected:  false,
		},
		{
			name:      "future nextRunAt",
			nextRunAt: now.Add(1 * time.Hour),
			now:       now,
			expected:  false,
		},
		{
			name:      "recent overdue (< 24h)",
			nextRunAt: now.Add(-2 * time.Hour),
			now:       now,
			expected:  false,
		},
		{
			name:      "exactly 24h overdue",
			nextRunAt: now.Add(-24 * time.Hour),
			now:       now,
			expected:  false,
		},
		{
			name:      "stale overdue (> 24h)",
			nextRunAt: now.Add(-24*time.Hour - time.Second),
			now:       now,
			expected:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateCronStaleness(tc.nextRunAt, tc.now)
			if got != tc.expected {
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
		{
			name:       "schedule timezone takes precedence",
			scheduleTz: "America/New_York",
			defaultTz:  "America/Los_Angeles",
			expected:   "America/New_York",
		},
		{
			name:       "schedule timezone trimmed",
			scheduleTz: "  Asia/Tokyo  ",
			defaultTz:  "UTC",
			expected:   "Asia/Tokyo",
		},
		{
			name:       "empty schedule tz falls back to default",
			scheduleTz: "   ",
			defaultTz:  "America/Los_Angeles",
			expected:   "America/Los_Angeles",
		},
		{
			name:       "both empty fall back to UTC",
			scheduleTz: "",
			defaultTz:  "   ",
			expected:   "UTC",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveCronTimezone(tc.scheduleTz, tc.defaultTz)
			if got != tc.expected {
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
		{
			name:             "target is already a thread",
			isAlreadyThread:  true,
			policyMode:       "threads",
			hasThreadCreator: true,
			expected:         false,
		},
		{
			name:             "channel policy is channel mode",
			isAlreadyThread:  false,
			policyMode:       "channel",
			hasThreadCreator: true,
			expected:         false,
		},
		{
			name:             "channel policy is CHANNEL (case insensitive)",
			isAlreadyThread:  false,
			policyMode:       " CHANNEL ",
			hasThreadCreator: true,
			expected:         false,
		},
		{
			name:             "thread creator is nil/false",
			isAlreadyThread:  false,
			policyMode:       "threads",
			hasThreadCreator: false,
			expected:         false,
		},
		{
			name:             "valid thread mode with creator",
			isAlreadyThread:  false,
			policyMode:       "threads",
			hasThreadCreator: true,
			expected:         true,
		},
		{
			name:             "default/empty policy mode with creator",
			isAlreadyThread:  false,
			policyMode:       "",
			hasThreadCreator: true,
			expected:         true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldCreateThread(tc.isAlreadyThread, tc.policyMode, tc.hasThreadCreator)
			if got != tc.expected {
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
		{
			name:           "empty prefix",
			titlePrefix:    "",
			prompt:         "Review morning telemetry",
			expectedCron:   "Review morning telemetry",
			expectedPrompt: "[Reminder] Review morning telemetry",
		},
		{
			name:           "whitespace prefix",
			titlePrefix:    "   ",
			prompt:         "Review morning telemetry",
			expectedCron:   "Review morning telemetry",
			expectedPrompt: "[Reminder] Review morning telemetry",
		},
		{
			name:           "custom prefix",
			titlePrefix:    "Daily Telemetry",
			prompt:         "Review morning telemetry",
			expectedCron:   "[Daily Telemetry] Review morning telemetry",
			expectedPrompt: "[Reminder] Review morning telemetry",
		},
		{
			name:           "prompt with user request tags",
			titlePrefix:    "Routine",
			prompt:         "<USER_REQUEST>\nPrompt:\nDaily review\n</USER_REQUEST>",
			expectedCron:   "[Routine] Daily review",
			expectedPrompt: "[Reminder] Daily review",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cronSum := BuildCronMessageSummary(tc.titlePrefix, tc.prompt)
			if cronSum != tc.expectedCron {
				t.Errorf("BuildCronMessageSummary: expected %q, got %q", tc.expectedCron, cronSum)
			}

			oneShotSum := BuildOneShotMessageSummary(tc.prompt)
			if oneShotSum != tc.expectedPrompt {
				t.Errorf("BuildOneShotMessageSummary: expected %q, got %q", tc.expectedPrompt, oneShotSum)
			}
		})
	}
}

func TestPlanDueCronTurn_TableDriven(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)

	t.Run("fresh due cron normal execution", func(t *testing.T) {
		cron := db.CronSchedule{
			ID:          "cron-1",
			TargetID:    "chan-1",
			TitlePrefix: "Health Check",
			CronExpr:    "0 20 * * 5",
			Prompt:      "Check node health",
			Timezone:    "UTC",
			NextRunAt:   now.Add(-5 * time.Minute),
			Effort:      "high",
		}
		action := PlanDueCronTurn(cron, now, false, "threads", true, "UTC")
		if action.IsStale {
			t.Errorf("expected IsStale=false")
		}
		if !action.ShouldFire {
			t.Errorf("expected ShouldFire=true")
		}
		if action.NextRunError != nil {
			t.Errorf("unexpected NextRunError: %v", action.NextRunError)
		}
		expectedNext := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)
		if !action.NextRunAt.Equal(expectedNext) {
			t.Errorf("expected next run %v, got %v", expectedNext, action.NextRunAt)
		}
		if !action.ShouldCreateThread {
			t.Errorf("expected ShouldCreateThread=true")
		}
		if action.Effort != "high" {
			t.Errorf("expected effort high, got %q", action.Effort)
		}
	})

	t.Run("stale cron overdue > 24h", func(t *testing.T) {
		cron := db.CronSchedule{
			ID:        "cron-stale",
			CronExpr:  "0 20 * * 5",
			NextRunAt: now.Add(-25 * time.Hour),
		}
		action := PlanDueCronTurn(cron, now, false, "threads", true, "UTC")
		if !action.IsStale {
			t.Errorf("expected IsStale=true")
		}
		if action.ShouldFire {
			t.Errorf("expected ShouldFire=false")
		}
	})

	t.Run("invalid cron expression applies 24h fallback", func(t *testing.T) {
		cron := db.CronSchedule{
			ID:        "cron-invalid",
			CronExpr:  "invalid-syntax",
			NextRunAt: now.Add(-10 * time.Minute),
		}
		action := PlanDueCronTurn(cron, now, false, "threads", true, "UTC")
		if action.NextRunError == nil {
			t.Errorf("expected NextRunError to be non-nil")
		}
		expectedFallback := now.Add(24 * time.Hour)
		if !action.NextRunAt.Equal(expectedFallback) {
			t.Errorf("expected next run fallback %v, got %v", expectedFallback, action.NextRunAt)
		}
		if !action.ShouldFire {
			t.Errorf("expected ShouldFire=true for non-stale invalid cron")
		}
	})

	t.Run("zero now timestamp fallback", func(t *testing.T) {
		cron := db.CronSchedule{
			ID:        "cron-zero-now",
			CronExpr:  "@daily",
			NextRunAt: time.Now().Add(-1 * time.Minute),
		}
		action := PlanDueCronTurn(cron, time.Time{}, false, "threads", true, "UTC")
		if action.NextRunAt.IsZero() {
			t.Errorf("expected non-zero NextRunAt")
		}
	})
}

func TestStructBuilders_TableDriven(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)

	t.Run("BuildCronScheduleRun and BuildCronMessage", func(t *testing.T) {
		cron := db.CronSchedule{
			ID:          "cron-struct-1",
			TargetID:    "chan-main",
			TitlePrefix: "Weekly Report",
			CronExpr:    "0 9 * * 1",
			Prompt:      "Compile weekly report",
			Effort:      "medium",
		}

		run := BuildCronScheduleRun("run-1", "msg-1", "th-100", cron, "Weekly Report – Aug 28, 2026", now)
		if run.ID != "run-1" || run.ScheduleID != "cron-struct-1" || run.ScheduleType != "cron" ||
			run.MessageID != "msg-1" || run.TargetID != "chan-main" || run.ThreadID != "th-100" ||
			run.Title != "Weekly Report – Aug 28, 2026" || run.Prompt != "Compile weekly report" ||
			run.Status != "enqueued" || !run.StartedAt.Equal(now) || run.Effort != "medium" {
			t.Errorf("BuildCronScheduleRun field mismatch: %+v", run)
		}

		msg := BuildCronMessage("msg-1", "run-1", "th-100", cron, "[Weekly Report] Compile weekly report", now)
		if msg.ID != "msg-1" || msg.ThreadID != "th-100" || msg.GuildID != "scheduled" ||
			msg.AuthorID != "scheduler" || msg.AuthorName != "Scheduler" || msg.Content != "Compile weekly report" ||
			msg.Summary != "[Weekly Report] Compile weekly report" || msg.Status != db.StatusPending ||
			msg.ScheduleRunID != "run-1" || !msg.CreatedAt.Equal(now) || !msg.UpdatedAt.Equal(now) ||
			msg.Effort != "medium" {
			t.Errorf("BuildCronMessage field mismatch: %+v", msg)
		}
	})

	t.Run("BuildOneShotScheduleRun and BuildOneShotMessage", func(t *testing.T) {
		oneShot := db.OneShotSchedule{
			ID:       "oneshot-1",
			ThreadID: "th-200",
			Prompt:   "Submit timesheet",
		}

		run := BuildOneShotScheduleRun("run-2", "msg-2", oneShot, now)
		if run.ID != "run-2" || run.ScheduleID != "oneshot-1" || run.ScheduleType != "one_shot" ||
			run.MessageID != "msg-2" || run.TargetID != "th-200" || run.ThreadID != "th-200" ||
			run.Title != "One-shot Reminder" || run.Prompt != "Submit timesheet" ||
			run.Status != "enqueued" || !run.StartedAt.Equal(now) {
			t.Errorf("BuildOneShotScheduleRun field mismatch: %+v", run)
		}

		msg := BuildOneShotMessage("msg-2", "run-2", oneShot, "[Reminder] Submit timesheet", now)
		if msg.ID != "msg-2" || msg.ThreadID != "th-200" || msg.GuildID != "scheduled" ||
			msg.AuthorID != "scheduler" || msg.AuthorName != "Scheduler" || msg.Content != "Submit timesheet" ||
			msg.Summary != "[Reminder] Submit timesheet" || msg.Status != db.StatusPending ||
			msg.ScheduleRunID != "run-2" || !msg.CreatedAt.Equal(now) || !msg.UpdatedAt.Equal(now) {
			t.Errorf("BuildOneShotMessage field mismatch: %+v", msg)
		}
	})
}

func TestPeriodicTickPredicates_TableDriven(t *testing.T) {
	tests := []struct {
		tickCount       int
		expectFactExt   bool
		expectRetention bool
	}{
		{tickCount: -1, expectFactExt: false, expectRetention: false},
		{tickCount: 0, expectFactExt: false, expectRetention: false},
		{tickCount: 1, expectFactExt: false, expectRetention: false},
		{tickCount: 119, expectFactExt: false, expectRetention: false},
		{tickCount: 120, expectFactExt: true, expectRetention: false},
		{tickCount: 121, expectFactExt: false, expectRetention: false},
		{tickCount: 240, expectFactExt: true, expectRetention: false},
		{tickCount: 2879, expectFactExt: false, expectRetention: false},
		{tickCount: 2880, expectFactExt: true, expectRetention: true},
		{tickCount: 2881, expectFactExt: false, expectRetention: false},
		{tickCount: 5760, expectFactExt: true, expectRetention: true},
	}

	for _, tc := range tests {
		factGot := ShouldRunFactExtraction(tc.tickCount)
		if factGot != tc.expectFactExt {
			t.Errorf("tick %d: ShouldRunFactExtraction expected %v, got %v", tc.tickCount, tc.expectFactExt, factGot)
		}

		retGot := ShouldRunPruneRetention(tc.tickCount)
		if retGot != tc.expectRetention {
			t.Errorf("tick %d: ShouldRunPruneRetention expected %v, got %v", tc.tickCount, tc.expectRetention, retGot)
		}
	}
}
