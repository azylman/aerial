package db

import (
	"database/sql"
	"testing"
	"time"
)

func TestDBInitializationAndConversationMapping(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize in-memory SQLite database: %v", err)
	}
	defer func() { _ = database.Close() }()

	internalID, err := GetInternalConversationID(database, "ext-12345")
	if err != nil {
		t.Fatalf("Error querying non-existent external conversation ID: %v", err)
	}
	if internalID != "" {
		t.Errorf("Expected empty internal ID for non-existent record, got: %s", internalID)
	}

	extID := "discord-thread-999"
	intID := "conv-uuid-abc"
	if err := SaveConversationMapping(database, extID, intID); err != nil {
		t.Fatalf("Failed to save conversation mapping: %v", err)
	}

	gotInternal, err := GetInternalConversationID(database, extID)
	if err != nil {
		t.Fatalf("Error retrieving internal conversation ID: %v", err)
	}
	if gotInternal != intID {
		t.Errorf("Expected internal ID %s, got: %s", intID, gotInternal)
	}

	gotExternal, err := GetExternalConversationID(database, intID)
	if err != nil {
		t.Fatalf("Error retrieving external conversation ID: %v", err)
	}
	if gotExternal != extID {
		t.Errorf("Expected external ID %s, got: %s", extID, gotExternal)
	}

	updatedIntID := "conv-uuid-updated"
	if err := SaveConversationMapping(database, extID, updatedIntID); err != nil {
		t.Fatalf("Failed to update conversation mapping: %v", err)
	}

	gotUpdated, err := GetInternalConversationID(database, extID)
	if err != nil {
		t.Fatalf("Error retrieving updated conversation ID: %v", err)
	}
	if gotUpdated != updatedIntID {
		t.Errorf("Expected updated internal ID %s, got: %s", updatedIntID, gotUpdated)
	}
}

func TestDBNilHandling(t *testing.T) {
	internalID, err := GetInternalConversationID(nil, "ext-123")
	if err != nil {
		t.Errorf("Expected nil error for nil db, got: %v", err)
	}
	if internalID != "" {
		t.Errorf("Expected empty result for nil db, got: %s", internalID)
	}

	externalID, err := GetExternalConversationID(nil, "int-123")
	if err != nil {
		t.Errorf("Expected nil error for nil db, got: %v", err)
	}
	if externalID != "" {
		t.Errorf("Expected empty result for nil db, got: %s", externalID)
	}

	if err := SaveConversationMapping(nil, "ext", "int"); err != nil {
		t.Errorf("Expected nil error for nil db save, got: %v", err)
	}
}

func TestDBErrorCases(t *testing.T) {
	// Test InitDB with invalid path
	_, err := InitDB("/proc/invalid_path/aerial.db")
	if err == nil {
		t.Error("Expected error when creating DB in invalid directory, got nil")
	}

	// Test query on closed database
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = database.Close()

	if _, err := GetInternalConversationID(database, "ext-1"); err == nil {
		t.Error("Expected error querying closed database for GetInternalConversationID, got nil")
	}

	if _, err := GetExternalConversationID(database, "int-1"); err == nil {
		t.Error("Expected error querying closed database for GetExternalConversationID, got nil")
	}

	if err := SaveConversationMapping(database, "ext-1", "int-1"); err == nil {
		t.Error("Expected error saving mapping to closed database, got nil")
	}
}

func TestPhase1SchedulesAndMigration(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	// Verify columns on conversations
	var isProcessing bool
	var lastMsgID string
	err = database.QueryRow("SELECT is_processing, last_message_id FROM conversations LIMIT 1").Scan(&isProcessing, &lastMsgID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("Error querying conversations table new columns: %v", err)
	}

	// Verify one_shot_schedules table exists
	_, err = database.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at) VALUES ('1', 't1', 'p1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
	if err != nil {
		t.Fatalf("Failed to insert into one_shot_schedules: %v", err)
	}

	// Verify cron_schedules table exists
	_, err = database.Exec("INSERT INTO cron_schedules (id, target_id, cron_expr, prompt, next_run_at, enabled, created_at) VALUES ('1', 'ch1', '0 8 * * *', 'p1', CURRENT_TIMESTAMP, TRUE, CURRENT_TIMESTAMP)")
	if err != nil {
		t.Fatalf("Failed to insert into cron_schedules: %v", err)
	}
}

func TestPhase2TurnLifecycle(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	extID := "thread-phase2-123"
	intID := "conv-phase2-abc"
	if err := SaveConversationMapping(database, extID, intID); err != nil {
		t.Fatalf("Failed to save conversation mapping: %v", err)
	}

	// Initial state check
	state, err := GetTurnState(database, extID)
	if err != nil || state == nil {
		t.Fatalf("Failed to retrieve initial turn state: %v", err)
	}
	if state.IsProcessing {
		t.Errorf("Expected initial IsProcessing to be false, got true")
	}

	// Lock turn (turn start)
	msgID := "msg-999"
	if err := SetTurnProcessing(database, extID, true, msgID); err != nil {
		t.Fatalf("Failed to set turn processing: %v", err)
	}

	state, err = GetTurnState(database, extID)
	if err != nil || state == nil {
		t.Fatalf("Failed to retrieve locked turn state: %v", err)
	}
	if !state.IsProcessing {
		t.Errorf("Expected IsProcessing to be true after locking, got false")
	}
	if state.LastMessageID != msgID {
		t.Errorf("Expected LastMessageID to be %s, got %s", msgID, state.LastMessageID)
	}

	// Unlock turn (turn end)
	if err := SetTurnProcessing(database, extID, false, ""); err != nil {
		t.Fatalf("Failed to unlock turn processing: %v", err)
	}

	state, err = GetTurnState(database, extID)
	if err != nil || state == nil {
		t.Fatalf("Failed to retrieve unlocked turn state: %v", err)
	}
	if state.IsProcessing {
		t.Errorf("Expected IsProcessing to be false after unlocking, got true")
	}
	if state.LastMessageID != msgID {
		t.Errorf("Expected LastMessageID to remain %s, got %s", msgID, state.LastMessageID)
	}
}

func TestPhase3CrashRecoveryQuery(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	// 1. Create two conversations, one completed, one interrupted
	SaveConversationMapping(database, "thread-1", "conv-1")
	SaveConversationMapping(database, "thread-2", "conv-2")

	SetTurnProcessing(database, "thread-2", true, "msg-interrupted-123")

	interrupted, err := GetInterruptedTurns(database)
	if err != nil {
		t.Fatalf("Failed to query interrupted turns: %v", err)
	}
	if len(interrupted) != 1 {
		t.Fatalf("Expected 1 interrupted turn, got: %d", len(interrupted))
	}

	if interrupted[0].ExternalID != "thread-2" || interrupted[0].LastMessageID != "msg-interrupted-123" {
		t.Errorf("Unexpected interrupted turn state: %+v", interrupted[0])
	}

	// 2. Clear lock
	SetTurnProcessing(database, "thread-2", false, "")
	interrupted, err = GetInterruptedTurns(database)
	if err != nil {
		t.Fatalf("Failed to query interrupted turns after clearing: %v", err)
	}
	if len(interrupted) != 0 {
		t.Errorf("Expected 0 interrupted turns after unlocking, got: %d", len(interrupted))
	}
}

func TestPhase4SchedulerQueries(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	// One-shot schedule test
	oneShot := OneShotSchedule{
		ID:       "sched-1",
		ThreadID: "thread-abc",
		Prompt:   "Reminder prompt",
		RunAt:    time.Now().UTC().Add(-1 * time.Minute),
	}
	if err := CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("Failed to create one shot schedule: %v", err)
	}

	dueOneShots, err := GetDueOneShotSchedules(database)
	if err != nil {
		t.Fatalf("Failed to fetch due one-shot schedules: %v", err)
	}
	if len(dueOneShots) != 1 {
		t.Fatalf("Expected 1 due one-shot schedule, got: %d", len(dueOneShots))
	}
	if dueOneShots[0].ID != "sched-1" {
		t.Errorf("Unexpected schedule ID: %s", dueOneShots[0].ID)
	}

	if err := DeleteOneShotSchedule(database, "sched-1"); err != nil {
		t.Fatalf("Failed to delete one shot schedule: %v", err)
	}
	dueOneShots, err = GetDueOneShotSchedules(database)
	if err != nil || len(dueOneShots) != 0 {
		t.Errorf("Expected 0 due one-shot schedules after deletion, got: %d", len(dueOneShots))
	}

	// Cron schedule test
	cronSched := CronSchedule{
		ID:        "cron-1",
		TargetID:  "chan-xyz",
		CronExpr:  "0 * * * *",
		Prompt:    "Hourly check",
		NextRunAt: time.Now().UTC().Add(-5 * time.Minute),
		Enabled:   true,
	}
	if err := CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	dueCrons, err := GetDueCronSchedules(database)
	if err != nil {
		t.Fatalf("Failed to fetch due cron schedules: %v", err)
	}
	if len(dueCrons) != 1 {
		t.Fatalf("Expected 1 due cron schedule, got: %d", len(dueCrons))
	}

	nextRun := time.Now().UTC().Add(1 * time.Hour)
	if err := UpdateCronNextRun(database, "cron-1", nextRun); err != nil {
		t.Fatalf("Failed to update cron next run: %v", err)
	}

	dueCrons, err = GetDueCronSchedules(database)
	if err != nil || len(dueCrons) != 0 {
		t.Errorf("Expected 0 due cron schedules after next run update, got: %d", len(dueCrons))
	}
}
