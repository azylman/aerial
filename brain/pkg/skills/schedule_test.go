package skills

import (
	"context"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func TestScheduleTools(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	tools := NewScheduleTools(database)
	ctx := context.Background()

	// Test ScheduleOneShot
	runAt := time.Now().UTC().Add(10 * time.Minute)
	res, err := tools.ScheduleOneShot(ctx, "thread-123", "Reminder text", runAt)
	if err != nil {
		t.Fatalf("ScheduleOneShot failed: %v", err)
	}
	if res == "" {
		t.Errorf("Expected non-empty response from ScheduleOneShot")
	}

	// Test ScheduleCron
	nextRun := time.Now().UTC().Add(1 * time.Hour)
	cronRes, err := tools.ScheduleCron(ctx, "target-456", "0 * * * *", "Hourly prompt", nextRun)
	if err != nil {
		t.Fatalf("ScheduleCron failed: %v", err)
	}
	if cronRes == "" {
		t.Errorf("Expected non-empty response from ScheduleCron")
	}

	// Test CancelSchedule for cron
	cronCancelRes, err := tools.CancelSchedule(ctx, "cron", "cron-id")
	if err != nil {
		t.Fatalf("CancelSchedule for cron failed: %v", err)
	}
	if cronCancelRes == "" {
		t.Errorf("Expected non-empty response from CancelSchedule cron")
	}

	// Test nil DB error handling
	nilTools := NewScheduleTools(nil)
	if _, err := nilTools.ScheduleOneShot(ctx, "t", "p", runAt); err == nil {
		t.Errorf("Expected error for nil DB in ScheduleOneShot")
	}
	if _, err := nilTools.ScheduleCron(ctx, "t", "* * * * *", "p", nextRun); err == nil {
		t.Errorf("Expected error for nil DB in ScheduleCron")
	}
	if _, err := nilTools.CancelSchedule(ctx, "one_shot", "id"); err == nil {
		t.Errorf("Expected error for nil DB in CancelSchedule")
	}
}
