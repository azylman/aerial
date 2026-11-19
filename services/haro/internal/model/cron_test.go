package model

import (
	"testing"
	"time"
)

func TestCalculateNextCronRun(t *testing.T) {
	baseTime := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		expr    string
		tz      string
		want    time.Time
		wantErr bool
	}{
		{"friday 20:00 utc", "0 20 * * 5", "UTC", time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC), false},
		{"weekly descriptor utc", "@weekly", "UTC", time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC), false},
		{"9am los angeles", "0 9 * * *", "America/Los_Angeles", time.Date(2026, 8, 28, 16, 0, 0, 0, time.UTC), false},
		{"9am new york", "0 9 * * *", "America/New_York", time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC), false},
		{"empty tz error", "0 9 * * *", "", time.Time{}, true},
		{"invalid tz error", "0 9 * * *", "Invalid/Timezone", time.Time{}, true},
		{"invalid cron error", "not a cron expr", "UTC", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CalculateNextCronRun(tc.expr, tc.tz, baseTime)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CalculateNextCronRun() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && !got.Equal(tc.want) {
				t.Fatalf("CalculateNextCronRun() = %v, want %v", got, tc.want)
			}
		})
	}
}
