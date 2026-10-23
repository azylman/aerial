package queue

import (
	"strings"
	"testing"
	"time"
)

func TestFormatThreadTitle_TableDriven(t *testing.T) {
	t.Parallel()
	testTime := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		prefix   string
		prompt   string
		expected string
		maxRunes int
	}{
		{"empty prefix and prompt", "", "", "Scheduled Routine – Aug 28, 2026", 100},
		{"whitespace prefix", "   \t\n  ", "", "Scheduled Routine – Aug 28, 2026", 100},
		{"custom prefix", "Weekly Meal Plan", "", "Weekly Meal Plan – Aug 28, 2026", 100},
		{"empty prefix with prompt", "", "Check system health", "Check system health – Aug 28, 2026", 100},
		{"whitespace prefix with prompt", "   ", "Disk cleanup required", "Disk cleanup required – Aug 28, 2026", 100},
		{"long ASCII prefix exceeding 100 runes", strings.Repeat("A", 120), "", strings.Repeat("A", 97) + "...", 100},
		{"multi-byte unicode prefix exceeding 100 runes", strings.Repeat("🚀", 120), "", strings.Repeat("🚀", 97) + "...", 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatThreadTitle(tc.prefix, tc.prompt, testTime)
			runes := []rune(got)
			if len(runes) > tc.maxRunes {
				t.Errorf("expected <= %d runes, got %d", tc.maxRunes, len(runes))
			}
			if tc.expected != "" && !strings.HasSuffix(tc.expected, "...") {
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

func TestShouldCreateThread_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		isAlreadyThread bool
		channelMode     string
		expected        bool
	}{
		{"target is already a thread", true, "threads", false},
		{"channel policy is channel mode", false, "channel", false},
		{"channel policy is CHANNEL (case insensitive)", false, " CHANNEL ", false},
		{"valid thread mode", false, "threads", true},
		{"default/empty policy mode", false, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ShouldCreateThread(tc.isAlreadyThread, tc.channelMode); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}
