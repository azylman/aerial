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

	// Test CancelSchedule
	cancelRes, err := tools.CancelSchedule(ctx, "one_shot", "sched-id")
	if err != nil {
		t.Fatalf("CancelSchedule failed: %v", err)
	}
	if cancelRes == "" {
		t.Errorf("Expected non-empty response from CancelSchedule")
	}
}
