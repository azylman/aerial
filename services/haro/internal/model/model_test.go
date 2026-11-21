package model

import (
	"strings"
	"testing"
	"time"
)

func TestPureHelpers_IsScheduleStale(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		scheduledAt time.Time
		maxOverdue  time.Duration
		wantStale   bool
	}{
		{
			name:        "not stale within maxOverdue",
			scheduledAt: now.Add(-1 * time.Hour),
			maxOverdue:  24 * time.Hour,
			wantStale:   false,
		},
		{
			name:        "stale when overdue by more than maxOverdue",
			scheduledAt: now.Add(-25 * time.Hour),
			maxOverdue:  24 * time.Hour,
			wantStale:   true,
		},
		{
			name:        "zero maxOverdue never reports stale",
			scheduledAt: now.Add(-48 * time.Hour),
			maxOverdue:  0,
			wantStale:   false,
		},
		{
			name:        "negative maxOverdue never reports stale",
			scheduledAt: now.Add(-48 * time.Hour),
			maxOverdue:  -1 * time.Hour,
			wantStale:   false,
		},
		{
			name:        "future scheduledAt is not stale",
			scheduledAt: now.Add(1 * time.Hour),
			maxOverdue:  24 * time.Hour,
			wantStale:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsScheduleStale(tc.scheduledAt, now, tc.maxOverdue)
			if got != tc.wantStale {
				t.Errorf("IsScheduleStale() = %v, want %v", got, tc.wantStale)
			}
		})
	}
}

func TestPureHelpers_ShouldPruneRuns(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		lastPrune   time.Time
		interval    time.Duration
		shouldPrune bool
	}{
		{
			name:        "zero lastPrune triggers prune",
			lastPrune:   time.Time{},
			interval:    24 * time.Hour,
			shouldPrune: true,
		},
		{
			name:        "interval elapsed triggers prune",
			lastPrune:   now.Add(-25 * time.Hour),
			interval:    24 * time.Hour,
			shouldPrune: true,
		},
		{
			name:        "exact interval triggers prune",
			lastPrune:   now.Add(-24 * time.Hour),
			interval:    24 * time.Hour,
			shouldPrune: true,
		},
		{
			name:        "less than interval does not trigger prune",
			lastPrune:   now.Add(-12 * time.Hour),
			interval:    24 * time.Hour,
			shouldPrune: false,
		},
		{
			name:        "zero or negative interval does not trigger prune",
			lastPrune:   now.Add(-48 * time.Hour),
			interval:    0,
			shouldPrune: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldPruneRuns(now, tc.lastPrune, tc.interval)
			if got != tc.shouldPrune {
				t.Errorf("ShouldPruneRuns() = %v, want %v", got, tc.shouldPrune)
			}
		})
	}
}

func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name string
		vals []string
		want string
	}{
		{name: "no values", vals: nil, want: ""},
		{name: "all blank", vals: []string{"", "   "}, want: ""},
		{name: "first wins", vals: []string{"a", "b"}, want: "a"},
		{name: "skips blanks and trims", vals: []string{"  ", " b ", "c"}, want: "b"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FirstNonEmpty(tc.vals...); got != tc.want {
				t.Errorf("FirstNonEmpty(%q) = %q, want %q", tc.vals, got, tc.want)
			}
		})
	}
}

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
				"Last Successful Check Started At: none (initial run)",
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
				"Last Successful Check Started At: 2026-11-14T01:00:00Z (1794618000)",
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

func TestPureHelpers_ValidateEventWatchSpec(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		target        string
		eventType     string
		extraSources  []string
		wantSource    string
		wantTarget    string
		wantEventType string
		wantErr       bool
	}{
		// empty target
		{"empty target", "vanadis_build", "", "completed", nil, "", "", "", true},
		// vanadis_build
		{"vanadis_build completed", "vanadis_build", "build-123-abc", "completed", nil, "vanadis_build", "build-123-abc", "completed", false},
		{"vanadis_build failed", "VANADIS_BUILD", "build-456", "FAILED", nil, "vanadis_build", "build-456", "failed", false},
		{"vanadis_build invalid event_type", "vanadis_build", "build-123", "running", nil, "", "", "", true},
		{"vanadis_build target with chunk rejected", "vanadis_build", "build-123-chunk-1", "completed", nil, "", "", "", true},
		{"vanadis_build invalid target regex", "vanadis_build", "invalid_build", "completed", nil, "", "", "", true},
		// vanadis_chunk
		{"vanadis_chunk merged", "vanadis_chunk", "build-123-chunk-1", "merged", nil, "vanadis_chunk", "build-123-chunk-1", "merged", false},
		{"vanadis_chunk completed", "vanadis_chunk", "chunk-6-vanadis-pr-events", "completed", nil, "vanadis_chunk", "chunk-6-vanadis-pr-events", "completed", false},
		{"vanadis_chunk failed", "vanadis_chunk", "anyprefix-chunk-99", "failed", nil, "vanadis_chunk", "anyprefix-chunk-99", "failed", false},
		{"vanadis_chunk invalid event_type", "vanadis_chunk", "build-123-chunk-1", "running", nil, "", "", "", true},
		{"vanadis_chunk invalid target empty", "vanadis_chunk", "", "merged", nil, "", "", "", true},
		{"vanadis_chunk invalid target characters", "vanadis_chunk", "chunk/with/slashes", "merged", nil, "", "", "", true},
		// flux
		{"flux bare repo sync_success", "flux", "aerial", "sync_success", nil, "flux", "azylman/aerial", "sync_success", false},
		{"flux full repo sync_failed", "flux", "azylman/aerial-config", "sync_failed", nil, "flux", "azylman/aerial-config", "sync_failed", false},
		{"flux invalid event_type", "flux", "aerial", "deployed", nil, "", "", "", true},
		{"flux unmanaged repo", "flux", "other/repo", "sync_success", nil, "", "", "", true},
		// github
		{"github repo only pr_merged", "github", "aerial", "pr_merged", nil, "github", "azylman/aerial", "pr_merged", false},
		{"github repo with branch ci_passed", "github", "azylman/aerial:main", "ci_passed", nil, "github", "azylman/aerial:main", "ci_passed", false},
		{"github refs heads stripped ci_failed", "github", "aerial:refs/heads/feature/test", "ci_failed", nil, "github", "azylman/aerial:feature/test", "ci_failed", false},
		{"github PR number with hash rejected", "github", "aerial:#123", "ci_passed", nil, "", "", "", true},
		{"github PR number numeric rejected", "github", "aerial:123", "ci_passed", nil, "", "", "", true},
		{"github 40-char SHA rejected", "github", "aerial:0123456789abcdef0123456789abcdef01234567", "ci_passed", nil, "", "", "", true},
		{"github empty branch rejected", "github", "aerial:", "ci_passed", nil, "", "", "", true},
		{"github unmanaged repo rejected", "github", "unmanaged/repo:main", "ci_passed", nil, "", "", "", true},
		{"github invalid event_type", "github", "aerial", "completed", nil, "", "", "", true},
		// unknown source vs configured extra_event_sources
		{"unknown source without extraSources", "custom_iot", "device.washer", "idle", nil, "", "", "", true},
		{"configured extra source valid", "Custom_IoT", "device.washer", "IDLE", []string{"custom_iot"}, "custom_iot", "device.washer", "idle", false},
		{"configured extra source empty event_type", "custom_iot", "device.washer", "   ", []string{"custom_iot"}, "", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, target, ev, err := ValidateEventWatchSpec(tc.source, tc.target, tc.eventType, tc.extraSources...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateEventWatchSpec() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if s != tc.wantSource || target != tc.wantTarget || ev != tc.wantEventType {
					t.Errorf("got (%q, %q, %q), want (%q, %q, %q)", s, target, ev, tc.wantSource, tc.wantTarget, tc.wantEventType)
				}
			}
		})
	}
}

func TestPureHelpers_FormatEventTriggerPrompt(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		prompt  string
		wantStr string
	}{
		{"both header and prompt present", "Event trigger: test", "Proceed with task", "Event trigger: test\n\nProceed with task"},
		{"empty header returns prompt", "   ", "Proceed with task", "Proceed with task"},
		{"empty prompt returns header", "Event trigger: test", "   ", "Event trigger: test"},
		{"both empty returns empty", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatEventTriggerPrompt(tc.header, tc.prompt); got != tc.wantStr {
				t.Fatalf("FormatEventTriggerPrompt() = %q, want %q", got, tc.wantStr)
			}
		})
	}
}

func TestOrdinal(t *testing.T) {
	tests := []struct {
		name string
		num  int
		want string
	}{
		{"first", 1, "1st"},
		{"second", 2, "2nd"},
		{"third", 3, "3rd"},
		{"fourth", 4, "4th"},
		{"eleventh", 11, "11th"},
		{"twelfth", 12, "12th"},
		{"thirteenth", 13, "13th"},
		{"twenty-first", 21, "21st"},
		{"twenty-second", 22, "22nd"},
		{"twenty-third", 23, "23rd"},
		{"negative number", -1, "-1st"},
		{"negative eleventh", -11, "-11th"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Ordinal(tc.num)
			if got != tc.want {
				t.Errorf("Ordinal(%d) = %q, want %q", tc.num, got, tc.want)
			}
		})
	}
}

func TestFormatCronDescription(t *testing.T) {
	tests := []struct {
		name     string
		cronExpr string
		want     string
	}{
		{"empty", "", ""},
		{"yearly", "@yearly", "Every year on Jan 1st at 00:00"},
		{"annually", "@annually", "Every year on Jan 1st at 00:00"},
		{"monthly", "@monthly", "1st of every month at 00:00"},
		{"weekly", "@weekly", "Every week on Sunday at 00:00"},
		{"daily", "@daily", "Every day at 00:00"},
		{"midnight", "@midnight", "Every day at 00:00"},
		{"hourly", "@hourly", "Every hour"},
		{"invalid field count", "0 0 * *", "0 0 * *"},
		{"every minute", "* * * * *", "Every minute"},
		{"step minutes", "*/15 * * * *", "Every 15 minutes"},
		{"step hours", "0 */2 * * *", "Every 2 hours"},
		{"invalid min or hour", "xx yy * * *", "xx yy * * *"},
		{"every day at 09:30", "30 9 * * *", "Every day at 09:30"},
		{"weekdays numeric", "0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"weekdays text", "0 9 * * MON-FRI", "Weekdays (Mon–Fri) at 09:00"},
		{"weekends numeric 0-6", "0 10 * * 0,6", "Weekends (Sat–Sun) at 10:00"},
		{"weekends numeric 6-0", "0 10 * * 6,0", "Weekends (Sat–Sun) at 10:00"},
		{"weekends text", "0 10 * * SAT,SUN", "Weekends (Sat–Sun) at 10:00"},
		{"single day numeric", "0 12 * * 2", "Every Tuesday at 12:00"},
		{"multiple days list", "0 12 * * 1,3,5", "Mon, Wed, Fri at 12:00"},
		{"specific day of month", "15 14 1 * *", "1st of every month at 14:15"},
		{"day of month and month", "0 8 25 12 *", "Every year on Dec 25th at 08:00"},
		{"multiple days text", "0 12 * * MON,WED", "MON, WED at 12:00"},
		{"day of month non-numeric month", "0 8 25 FOO *", "At 08:00 (cron: 0 8 25 FOO *)"},
		{"non-numeric day of month with month", "0 8 1-5 12 *", "At 08:00 (cron: 0 8 1-5 12 *)"},
		{"other cron pattern", "0 9 * 5 1-3", "At 09:00 (cron: 0 9 * 5 1-3)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatCronDescription(tc.cronExpr)
			if got != tc.want {
				t.Errorf("FormatCronDescription(%q) = %q, want %q", tc.cronExpr, got, tc.want)
			}
		})
	}
}

type formatScheduleTestCase struct {
	name          string
	cronCount     int
	hbCount       int
	oneShotCount  int
	wantChannelID string
}

func assertFormatCronSchedules(t *testing.T, tc formatScheduleTestCase, formattedCrons []CronScheduleWithDesc) {
	t.Helper()
	if len(formattedCrons) != tc.cronCount {
		t.Fatalf("FormatCronSchedulesWithDesc got len=%d, want %d", len(formattedCrons), tc.cronCount)
	}
	if tc.cronCount > 0 {
		if formattedCrons[0].ChannelID != tc.wantChannelID || formattedCrons[0].TargetID != tc.wantChannelID {
			t.Errorf("channel/target id mismatch: %v", formattedCrons[0])
		}
		if formattedCrons[0].TitlePrefix != "Daily Report" || formattedCrons[0].Prompt != "Check the build queue" {
			t.Errorf("cron text altered: %v", formattedCrons[0])
		}
		if formattedCrons[0].CronDescription == "" {
			t.Errorf("cron description missing")
		}
	}
}

func assertFormatHeartbeatSchedules(t *testing.T, tc formatScheduleTestCase, formattedHBs []HeartbeatScheduleWithDesc) {
	t.Helper()
	if len(formattedHBs) != tc.hbCount {
		t.Fatalf("FormatHeartbeatSchedulesWithDesc got len=%d, want %d", len(formattedHBs), tc.hbCount)
	}
	if tc.hbCount > 0 && formattedHBs[0].Prompt != "Heartbeat check flight status" {
		t.Errorf("heartbeat text altered: %v", formattedHBs[0])
	}
}

func assertFormatOneShotSchedules(t *testing.T, tc formatScheduleTestCase, formattedOS []OneShotScheduleJSON) {
	t.Helper()
	if len(formattedOS) != tc.oneShotCount {
		t.Fatalf("FormatOneShotSchedulesJSON got len=%d, want %d", len(formattedOS), tc.oneShotCount)
	}
	if tc.oneShotCount > 0 && formattedOS[0].Prompt != "Remind about laundry" {
		t.Errorf("oneshot text altered: %v", formattedOS[0])
	}
}

func runFormatScheduleCase(t *testing.T, tc formatScheduleTestCase) {
	t.Helper()
	var crons []CronSchedule
	var hbs []HeartbeatSchedule
	var oneshots []OneShotSchedule
	if tc.cronCount > 0 {
		crons = append(crons, CronSchedule{
			ID:          "cron-1",
			TargetID:    tc.wantChannelID,
			TitlePrefix: "Daily Report",
			CronExpr:    "0 9 * * 1-5",
			Prompt:      "Check the build queue",
			Timezone:    "America/Los_Angeles",
			NextRunAt:   time.Now().UTC(),
			Enabled:     true,
			Effort:      "low",
		})
	}
	if tc.hbCount > 0 {
		hbs = append(hbs, HeartbeatSchedule{
			ID:          "hb-1",
			TargetID:    tc.wantChannelID,
			TitlePrefix: "HB Alert",
			CronExpr:    "*/5 * * * *",
			Prompt:      "Heartbeat check flight status",
			Timezone:    "UTC",
			Effort:      "high",
			NextRunAt:   time.Now().UTC(),
			Enabled:     true,
		})
	}
	if tc.oneShotCount > 0 {
		oneshots = append(oneshots, OneShotSchedule{
			ID:       "os-1",
			ThreadID: tc.wantChannelID,
			Prompt:   "Remind about laundry",
			RunAt:    time.Now().UTC(),
			Effort:   "medium",
		})
	}

	assertFormatCronSchedules(t, tc, FormatCronSchedulesWithDesc(crons))
	assertFormatHeartbeatSchedules(t, tc, FormatHeartbeatSchedulesWithDesc(hbs))
	assertFormatOneShotSchedules(t, tc, FormatOneShotSchedulesJSON(oneshots))
}

func TestFormatScheduleHelpers(t *testing.T) {
	tests := []formatScheduleTestCase{
		{"empty slices", 0, 0, 0, ""},
		{"populated slices", 1, 1, 1, "chan-123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runFormatScheduleCase(t, tc)
		})
	}
}

func TestRedactDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{"query removed", "postgresql://app:hunter2@db:5432/app?sslmode=disable&password=hunter2", "postgresql://app:xxxxx@db:5432/app"},
		{"empty", "", "[redacted]"},
		{"key value dsn", "host=db password=hunter2", "[redacted]"},
		{"whitespace trimmed", "  postgres://app:hunter2@db:5432/app#frag \n", "postgres://app:xxxxx@db:5432/app"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RedactDSN(tc.dsn); got != tc.want {
				t.Errorf("RedactDSN(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}
