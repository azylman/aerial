package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/google/uuid"
)

type mockThreadCreator struct {
	createdThreads map[string]string
	mu             sync.Mutex
}

func newMockThreadCreator() *mockThreadCreator {
	return &mockThreadCreator{
		createdThreads: make(map[string]string),
	}
}

func (m *mockThreadCreator) CreatePublicThread(channelID, name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID := fmt.Sprintf("th-%s-%d", channelID, len(m.createdThreads)+1)
	m.createdThreads[channelID] = threadID
	return threadID, nil
}

type mockEnqueuer struct {
	messages []db.Message
	mu       sync.Mutex
}

func newMockEnqueuer() *mockEnqueuer {
	return &mockEnqueuer{}
}

func (m *mockEnqueuer) Enqueue(msg db.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *mockEnqueuer) getMessages() []db.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]db.Message, len(m.messages))
	copy(copied, m.messages)
	return copied
}

func TestFormatThreadTitle(t *testing.T) {
	testTime := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)

	title := FormatThreadTitle("Weekly Meal Plan", testTime)
	expected := "Weekly Meal Plan – Aug 28, 2026"
	if title != expected {
		t.Errorf("Expected %q, got %q", expected, title)
	}

	defaultTitle := FormatThreadTitle("", testTime)
	expectedDefault := "Scheduled Routine – Aug 28, 2026"
	if defaultTitle != expectedDefault {
		t.Errorf("Expected %q, got %q", expectedDefault, defaultTitle)
	}

	// Long title > 100 runes truncation test (ASCII)
	longPrefix := "A very long routine title prefix that exceeds the maximum allowable limit of one hundred characters easily and goes on and on"
	longTitle := FormatThreadTitle(longPrefix, testTime)
	runes := []rune(longTitle)
	if len(runes) != 100 {
		t.Errorf("Expected truncated title length of 100 runes, got %d", len(runes))
	}
	if string(runes[97:]) != "..." {
		t.Errorf("Expected title to end with '...', got %q", string(runes[97:]))
	}

	// Unicode multi-byte runes test (> 100 runes)
	unicodePrefix := "🔥🚀 非常に長いルーチンのプレフィックスで、制限を超えるマルチバイト文字列のテストを行っています。さらに文章を追加して確実に100文字を超えるように長文の日本語テキストを追加配置します。テストテスト。"
	unicodeTitle := FormatThreadTitle(unicodePrefix, testTime)
	uRunes := []rune(unicodeTitle)
	if len(uRunes) != 100 {
		t.Errorf("Expected unicode title length 100 runes, got %d", len(uRunes))
	}
	if string(uRunes[97:]) != "..." {
		t.Errorf("Expected unicode title to end with '...', got %q", string(uRunes[97:]))
	}
}

func TestGetDefaultTimezone(t *testing.T) {
	// 1. Fallback when both DEFAULT_TIMEZONE and TZ are unset
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	if tz := GetDefaultTimezone(); tz != "America/Los_Angeles" {
		t.Errorf("Expected fallback 'America/Los_Angeles', got %q", tz)
	}

	// 2. TZ environment variable fallback
	t.Setenv("TZ", "America/Chicago")
	if tz := GetDefaultTimezone(); tz != "America/Chicago" {
		t.Errorf("Expected TZ 'America/Chicago', got %q", tz)
	}

	// 3. DEFAULT_TIMEZONE environment variable takes highest precedence
	t.Setenv("DEFAULT_TIMEZONE", "America/New_York")
	if tz := GetDefaultTimezone(); tz != "America/New_York" {
		t.Errorf("Expected DEFAULT_TIMEZONE 'America/New_York', got %q", tz)
	}
}

func TestCalculateNextRun(t *testing.T) {
	// Friday Aug 28, 2026 12:00:00 UTC (05:00:00 PDT)
	baseTime := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)

	// "0 20 * * 5" -> Friday at 20:00 UTC
	next, err := CalculateNextRun("0 20 * * 5", "UTC", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextRun failed: %v", err)
	}
	expected := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("Expected next run %s, got %s", expected, next)
	}

	// Timezone test with embedded tzdata: America/Los_Angeles (PDT = UTC-7 in August)
	// "0 9 * * *" (9 AM America/Los_Angeles) from Aug 28 12:00 UTC (5:00 AM PDT) -> Aug 28 9:00 PDT (16:00 UTC)
	nextLA, err := CalculateNextRun("0 9 * * *", "America/Los_Angeles", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextRun America/Los_Angeles failed: %v", err)
	}
	expectedLA := time.Date(2026, time.August, 28, 16, 0, 0, 0, time.UTC)
	if !nextLA.Equal(expectedLA) {
		t.Errorf("Expected next LA run %s, got %s", expectedLA, nextLA)
	}

	// Empty timezone defaults to GetDefaultTimezone() ("America/Los_Angeles")
	t.Setenv("DEFAULT_TIMEZONE", "America/Los_Angeles")
	t.Setenv("TZ", "")
	nextDefault, err := CalculateNextRun("0 9 * * *", "", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextRun with empty timezone failed: %v", err)
	}
	if !nextDefault.Equal(expectedLA) {
		t.Errorf("Expected default next run %s, got %s", expectedLA, nextDefault)
	}

	// Configurable DEFAULT_TIMEZONE override: America/New_York (EDT = UTC-4 in August)
	// "0 9 * * *" (9 AM America/New_York) from Aug 28 12:00 UTC (8:00 AM EDT) -> Aug 28 9:00 EDT (13:00 UTC)
	t.Setenv("DEFAULT_TIMEZONE", "America/New_York")
	nextNY, err := CalculateNextRun("0 9 * * *", "", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextRun America/New_York override failed: %v", err)
	}
	expectedNY := time.Date(2026, time.August, 28, 13, 0, 0, 0, time.UTC)
	if !nextNY.Equal(expectedNY) {
		t.Errorf("Expected next NY run %s, got %s", expectedNY, nextNY)
	}

	// Timezone test: Asia/Tokyo (JST = UTC+9)
	// "0 9 * * *" from Aug 28 12:00 UTC (21:00 JST) -> Aug 29 9:00 JST (00:00 UTC on Aug 29)
	nextTokyo, err := CalculateNextRun("0 9 * * *", "Asia/Tokyo", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextRun Asia/Tokyo failed: %v", err)
	}
	expectedTokyo := time.Date(2026, time.August, 29, 0, 0, 0, 0, time.UTC)
	if !nextTokyo.Equal(expectedTokyo) {
		t.Errorf("Expected next Tokyo run %s, got %s", expectedTokyo, nextTokyo)
	}

	// "@daily"
	nextDaily, err := CalculateNextRun("@daily", "UTC", baseTime)
	if err != nil {
		t.Fatalf("CalculateNextRun @daily failed: %v", err)
	}
	expectedDaily := time.Date(2026, time.August, 29, 0, 0, 0, 0, time.UTC)
	if !nextDaily.Equal(expectedDaily) {
		t.Errorf("Expected next daily %s, got %s", expectedDaily, nextDaily)
	}

	// Invalid cron
	_, err = CalculateNextRun("invalid cron expr", "UTC", baseTime)
	if err == nil {
		t.Error("Expected error on invalid cron expression")
	}
}

func TestProcessDueCronSchedules(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// 1. Insert due cron schedule
	cronSched := db.CronSchedule{
		ID:          "cron-1",
		TargetID:    "chan-999",
		TitlePrefix: "Weekly Meal Plan",
		CronExpr:    "0 20 * * 5",
		Prompt:      "Generate meal plan for the week",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-1 * time.Minute),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	if err := db.CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	// 2. Insert future cron schedule (should NOT trigger)
	futureCron := db.CronSchedule{
		ID:          "cron-2",
		TargetID:    "chan-999",
		TitlePrefix: "Future Routine",
		CronExpr:    "0 20 * * 5",
		Prompt:      "Future prompt",
		Timezone:    "UTC",
		NextRunAt:   now.Add(1 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := db.CreateCronSchedule(database, futureCron); err != nil {
		t.Fatalf("Failed to create future cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	// Process due schedules
	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// Verify enqueued message
	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 enqueued message, got %d", len(msgs))
	}
	if msgs[0].Content != "Generate meal plan for the week" {
		t.Errorf("Expected prompt match, got %q", msgs[0].Content)
	}
	if msgs[0].ThreadID != "th-chan-999-1" {
		t.Errorf("Expected message ThreadID to match newly created thread 'th-chan-999-1', got %q", msgs[0].ThreadID)
	}
	if msgs[0].AuthorID != "scheduler" {
		t.Errorf("Expected AuthorID 'scheduler', got %q", msgs[0].AuthorID)
	}
	if msgs[0].GuildID != "scheduled" {
		t.Errorf("Expected GuildID 'scheduled', got %q", msgs[0].GuildID)
	}

	// Verify message in DB
	dbMsg, err := db.GetMessage(database, msgs[0].ID)
	if err != nil || dbMsg == nil {
		t.Fatalf("Message not found in DB: %v", err)
	}
	if dbMsg.Status != db.StatusPending {
		t.Errorf("Expected status PENDING, got %s", dbMsg.Status)
	}

	// Verify next_run_at was advanced and cron-1 is no longer due
	dueCrons, err := db.GetDueCronSchedules(database)
	if err != nil {
		t.Fatalf("GetDueCronSchedules error: %v", err)
	}
	if len(dueCrons) != 0 {
		t.Errorf("Expected 0 due cron schedules after processing, got %d", len(dueCrons))
	}
}

func TestProcessDueCronSchedules_24hStalenessGuard(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// Insert stale cron schedule (>24h overdue, e.g. 26h ago)
	staleCron := db.CronSchedule{
		ID:          "cron-stale-1",
		TargetID:    "chan-stale",
		TitlePrefix: "Stale Routine",
		CronExpr:    "0 20 * * 5",
		Prompt:      "Stale prompt that should not fire",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-26 * time.Hour),
		Enabled:     true,
		CreatedAt:   now.Add(-48 * time.Hour),
	}
	if err := db.CreateCronSchedule(database, staleCron); err != nil {
		t.Fatalf("Failed to create stale cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// Staleness guard MUST NOT fire the turn
	msgs := enqueuer.getMessages()
	if len(msgs) != 0 {
		t.Fatalf("Expected 0 enqueued messages due to 24h staleness guard, got %d", len(msgs))
	}

	// Stale cron schedule's next_run_at MUST be advanced into future
	dueCrons, err := db.GetDueCronSchedules(database)
	if err != nil {
		t.Fatalf("GetDueCronSchedules error: %v", err)
	}
	if len(dueCrons) != 0 {
		t.Errorf("Expected 0 due cron schedules after advancing stale cron, got %d", len(dueCrons))
	}

	allCrons, _ := db.GetAllCronSchedules(database, "chan-stale")
	if len(allCrons) != 1 {
		t.Fatalf("Expected 1 cron in DB, got %d", len(allCrons))
	}
	if !allCrons[0].NextRunAt.After(now) {
		t.Errorf("Expected next_run_at %s to be advanced into future after now %s", allCrons[0].NextRunAt, now)
	}
}

func TestProcessDueOneShotSchedules(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// 1. Insert due one-shot schedule
	oneShot := db.OneShotSchedule{
		ID:        uuid.New().String(),
		ThreadID:  "thread-existing-123",
		Prompt:    "Check stove reminder",
		RunAt:     now.Add(-2 * time.Minute),
		CreatedAt: now.Add(-10 * time.Minute),
	}
	if err := db.CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("Failed to create one shot schedule: %v", err)
	}

	// 2. Insert future one-shot schedule
	futureOneShot := db.OneShotSchedule{
		ID:        uuid.New().String(),
		ThreadID:  "thread-existing-123",
		Prompt:    "Future stove reminder",
		RunAt:     now.Add(30 * time.Minute),
		CreatedAt: now,
	}
	if err := db.CreateOneShotSchedule(database, futureOneShot); err != nil {
		t.Fatalf("Failed to create future one-shot schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// Verify enqueued message targeting existing thread
	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 enqueued message, got %d", len(msgs))
	}
	if msgs[0].ThreadID != "thread-existing-123" {
		t.Errorf("Expected ThreadID 'thread-existing-123', got %q", msgs[0].ThreadID)
	}
	if msgs[0].GuildID != "scheduled" {
		t.Errorf("Expected GuildID 'scheduled', got %q", msgs[0].GuildID)
	}
	if msgs[0].Content != "Check stove reminder" {
		t.Errorf("Expected content 'Check stove reminder', got %q", msgs[0].Content)
	}

	// Verify one-shot schedule was atomically deleted
	dueAfter, _ := db.GetDueOneShotSchedules(database)
	if len(dueAfter) != 0 {
		t.Errorf("Expected 0 due one-shot schedules after execution, got %d", len(dueAfter))
	}
	allAfter, _ := db.GetAllOneShotSchedules(database, "thread-existing-123")
	if len(allAfter) != 1 || allAfter[0].ID != futureOneShot.ID {
		t.Errorf("Expected only future schedule to remain, got %d schedules", len(allAfter))
	}
}

func TestSchedulerStartAndStop(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB: database,
	})

	stop := Start(context.Background(), database, pool, nil)
	time.Sleep(50 * time.Millisecond)

	stoppedCh := make(chan struct{})
	go func() {
		stop()
		// Test idempotence
		stop()
		close(stoppedCh)
	}()

	select {
	case <-stoppedCh:
		// Clean exit
	case <-time.After(1 * time.Second):
		t.Fatal("Start returned stop func did not exit cleanly within 1s")
	}
}

func TestSchedulerRunContextCancellation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	doneCh := make(chan struct{})
	go func() {
		Run(ctx, database, enqueuer, threadCreator, 10*time.Millisecond)
		close(doneCh)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-doneCh:
		// Success: scheduler stopped cleanly on context cancellation
	case <-time.After(1 * time.Second):
		t.Fatal("Scheduler did not stop cleanly within 1s after context cancellation")
	}
}
