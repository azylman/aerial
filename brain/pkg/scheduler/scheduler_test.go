package scheduler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
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
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	// 0. Test with runtime config timezone set
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	_ = os.WriteFile(yamlPath, []byte("timezone: 'Europe/Paris'\nchannels:\n  default:\n    mode: 'threads'\n"), 0644)
	_, _ = config.LoadConfigFromPaths(yamlPath)

	if tz := GetDefaultTimezone(); tz != "Europe/Paris" {
		t.Errorf("Expected runtime config 'Europe/Paris', got %q", tz)
	}

	// 1. DEFAULT_TIMEZONE environment variable takes precedence when config timezone is empty
	t.Setenv("DEFAULT_TIMEZONE", "America/New_York")
	t.Setenv("TZ", "")
	_ = os.WriteFile(yamlPath, []byte("channels:\n  default:\n    mode: 'threads'\n"), 0644)
	_, _ = config.LoadConfigFromPaths(yamlPath)

	if tz := GetDefaultTimezone(); tz != "America/New_York" {
		t.Errorf("Expected DEFAULT_TIMEZONE 'America/New_York', got %q", tz)
	}

	// 2. TZ environment variable fallback
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "America/Chicago")
	_, _ = config.LoadConfigFromPaths(yamlPath)

	if tz := GetDefaultTimezone(); tz != "America/Chicago" {
		t.Errorf("Expected TZ 'America/Chicago', got %q", tz)
	}

	// 3. Fallback when both DEFAULT_TIMEZONE and TZ are unset
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	_, _ = config.LoadConfigFromPaths(yamlPath)

	if tz := GetDefaultTimezone(); tz != "America/Los_Angeles" {
		t.Errorf("Expected fallback 'America/Los_Angeles', got %q", tz)
	}
}

func TestCalculateNextRun(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(yamlPath, []byte("channels:\n  default:\n    mode: 'threads'\n"), 0644)

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
	_, _ = config.LoadConfigFromPaths(yamlPath)

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
	_, _ = config.LoadConfigFromPaths(yamlPath)

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
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return `{"event":"result","result":{"status":"SUCCESS","response":"mock test response"}}`, "", 0, nil
		},
		NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
			return "mock notification"
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
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

func TestProcessDueCronSchedules_CreatesScheduleRun(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	cronSched := db.CronSchedule{
		ID:          "cron-run-test",
		TargetID:    "chan-cron-test",
		TitlePrefix: "Daily Standup",
		CronExpr:    "0 9 * * *",
		Prompt:      "Post daily standup",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Second),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	if err := db.CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].ScheduleRunID == "" {
		t.Fatalf("Expected ScheduleRunID to be populated on enqueued message, got empty string")
	}

	// Verify schedule run entry exists in DB
	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, cronSched.ID, "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated error: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("Expected 1 schedule run created, got total=%d, len=%d", total, len(runs))
	}

	run := runs[0]
	if run.ID != msgs[0].ScheduleRunID {
		t.Errorf("Expected run ID %q to match message ScheduleRunID %q", run.ID, msgs[0].ScheduleRunID)
	}
	if run.ScheduleID != cronSched.ID {
		t.Errorf("Expected schedule ID %q, got %q", cronSched.ID, run.ScheduleID)
	}
	if run.ScheduleType != "cron" {
		t.Errorf("Expected schedule type 'cron', got %q", run.ScheduleType)
	}
	if run.MessageID != msgs[0].ID {
		t.Errorf("Expected message ID %q, got %q", msgs[0].ID, run.MessageID)
	}
	if run.TargetID != cronSched.TargetID {
		t.Errorf("Expected target ID %q, got %q", cronSched.TargetID, run.TargetID)
	}
	if run.ThreadID != msgs[0].ThreadID {
		t.Errorf("Expected thread ID %q, got %q", msgs[0].ThreadID, run.ThreadID)
	}
	if run.Status != "enqueued" {
		t.Errorf("Expected status 'enqueued', got %q", run.Status)
	}
	if run.Prompt != cronSched.Prompt {
		t.Errorf("Expected prompt %q, got %q", cronSched.Prompt, run.Prompt)
	}
}

func TestProcessDueCronSchedules_EffortPropagation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	cronSched := db.CronSchedule{
		ID:          "cron-effort-test",
		TargetID:    "chan-effort-test",
		TitlePrefix: "Low Effort Cron",
		CronExpr:    "0 9 * * *",
		Prompt:      "Post low effort update",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Second),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
		Effort:      "low",
	}
	if err := db.CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].Effort != "low" {
		t.Errorf("Expected message effort 'low', got %q", msgs[0].Effort)
	}

	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, cronSched.ID, "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated error: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("Expected 1 schedule run created, got total=%d, len=%d", total, len(runs))
	}
	if runs[0].Effort != "low" {
		t.Errorf("Expected schedule run effort 'low', got %q", runs[0].Effort)
	}
}

func TestProcessDueOneShotSchedules_CreatesScheduleRun(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	oneShot := db.OneShotSchedule{
		ID:        "oneshot-run-test",
		ThreadID:  "thread-oneshot-target",
		Prompt:    "Water the plants",
		RunAt:     now.Add(-5 * time.Second),
		CreatedAt: now.Add(-30 * time.Minute),
	}
	if err := db.CreateOneShotSchedule(database, oneShot); err != nil {
		t.Fatalf("Failed to create one shot schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].ScheduleRunID == "" {
		t.Fatalf("Expected ScheduleRunID to be populated on enqueued message, got empty string")
	}

	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, oneShot.ID, "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated error: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("Expected 1 schedule run created, got total=%d, len=%d", total, len(runs))
	}

	run := runs[0]
	if run.ID != msgs[0].ScheduleRunID {
		t.Errorf("Expected run ID %q to match message ScheduleRunID %q", run.ID, msgs[0].ScheduleRunID)
	}
	if run.ScheduleID != oneShot.ID {
		t.Errorf("Expected schedule ID %q, got %q", oneShot.ID, run.ScheduleID)
	}
	if run.ScheduleType != "one_shot" {
		t.Errorf("Expected schedule type 'one_shot', got %q", run.ScheduleType)
	}
	if run.Status != "enqueued" {
		t.Errorf("Expected status 'enqueued', got %q", run.Status)
	}
	if run.Prompt != oneShot.Prompt {
		t.Errorf("Expected prompt %q, got %q", oneShot.Prompt, run.Prompt)
	}
}

func TestInsertMessageAndConsumeOneShot_RollbackOnCancelled(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	msg := db.Message{
		ID:        "msg-cancelled-1",
		ThreadID:  "thread-1",
		Content:   "Reminder prompt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}

	// Schedule does not exist in DB (e.g. was cancelled concurrently)
	err = db.InsertMessageAndConsumeOneShot(database, "non-existent-sched-id", msg)
	if err == nil {
		t.Fatal("Expected error when consuming non-existent one-shot schedule, got nil")
	}

	// Verify message was NOT inserted due to rollback
	dbMsg, _ := db.GetMessage(database, "msg-cancelled-1")
	if dbMsg != nil {
		t.Errorf("Expected message to NOT exist in DB after transaction rollback, but found: %+v", dbMsg)
	}
}

func TestProcessDueCronSchedules_ChannelMode_NoThreadCreated(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	cfgContent := `
channels:
  default:
    mode: "threads"
  general:
    mode: "channel"
`
	if err := os.WriteFile(yamlPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}
	if _, err := config.LoadConfigFromPaths(yamlPath); err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	chanID := "1464358486849622120"
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:   chanID,
		Name: "general",
		Type: discordgo.ChannelTypeGuildText,
	})

	now := time.Now().UTC()
	cronSched := db.CronSchedule{
		ID:          "cron-general-channel-mode",
		TargetID:    chanID,
		TitlePrefix: "Daily Weather Forecast",
		CronExpr:    "0 6 * * *",
		Prompt:      "Post daily weather",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Second),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	if err := db.CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// 1. Thread creator should NOT have been invoked for chanID
	if thID, exists := threadCreator.createdThreads[chanID]; exists {
		t.Fatalf("Expected NO thread to be created in channel mode, but thread %q was created", thID)
	}

	// 2. Enqueued message ThreadID must match channel ID
	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].ThreadID != chanID {
		t.Errorf("Expected message ThreadID %q (channel ID), got %q", chanID, msgs[0].ThreadID)
	}

	// 3. Schedule run record ThreadID must match channel ID
	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, cronSched.ID, "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated error: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("Expected 1 schedule run created, got total=%d", total)
	}
	if runs[0].ThreadID != chanID {
		t.Errorf("Expected run ThreadID %q, got %q", chanID, runs[0].ThreadID)
	}
}

func TestProcessDueCronSchedules_ThreadsMode_ThreadCreated(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	cfgContent := `
channels:
  default:
    mode: "ignore"
  aerial-dev:
    mode: "threads"
`
	if err := os.WriteFile(yamlPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}
	if _, err := config.LoadConfigFromPaths(yamlPath); err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	chanID := "chan-aerial-dev-123"
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:   chanID,
		Name: "aerial-dev",
		Type: discordgo.ChannelTypeGuildText,
	})

	now := time.Now().UTC()
	cronSched := db.CronSchedule{
		ID:          "cron-aerial-dev-threads-mode",
		TargetID:    chanID,
		TitlePrefix: "System Health Report",
		CronExpr:    "0 12 * * *",
		Prompt:      "Post health report",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Second),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	if err := db.CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// 1. Thread creator SHOULD have been invoked for chanID
	createdThID, exists := threadCreator.createdThreads[chanID]
	if !exists || createdThID == "" {
		t.Fatalf("Expected thread to be created in threads mode, but none found")
	}

	// 2. Enqueued message ThreadID must match created thread ID
	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].ThreadID != createdThID {
		t.Errorf("Expected message ThreadID %q, got %q", createdThID, msgs[0].ThreadID)
	}

	// 3. Schedule run record ThreadID must match created thread ID
	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, cronSched.ID, "")
	if err != nil {
		t.Fatalf("GetScheduleRunsPaginated error: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("Expected 1 schedule run created, got total=%d", total)
	}
	if runs[0].ThreadID != createdThID {
		t.Errorf("Expected run ThreadID %q, got %q", createdThID, runs[0].ThreadID)
	}
}

func TestProcessDueCronSchedules_TargetAlreadyThread_NoThreadCreated(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	cfgContent := `
channels:
  default:
    mode: "threads"
`
	if err := os.WriteFile(yamlPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}
	if _, err := config.LoadConfigFromPaths(yamlPath); err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadTargetID := "thread-existing-target-999"
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:       threadTargetID,
		Name:     "Existing Discussion Thread",
		ParentID: "parent-chan-111",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})

	now := time.Now().UTC()
	cronSched := db.CronSchedule{
		ID:          "cron-existing-thread-target",
		TargetID:    threadTargetID,
		TitlePrefix: "Thread Update",
		CronExpr:    "0 15 * * *",
		Prompt:      "Update existing thread",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Second),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	if err := db.CreateCronSchedule(database, cronSched); err != nil {
		t.Fatalf("Failed to create cron schedule: %v", err)
	}

	threadCreator := newMockThreadCreator()
	enqueuer := newMockEnqueuer()

	if err := ProcessDueSchedules(context.Background(), database, enqueuer, threadCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// 1. Thread creator should NOT have been invoked because target is already a thread
	if thID, exists := threadCreator.createdThreads[threadTargetID]; exists {
		t.Fatalf("Expected NO nested thread to be created, but thread %q was created", thID)
	}

	// 2. Enqueued message ThreadID must match threadTargetID
	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].ThreadID != threadTargetID {
		t.Errorf("Expected message ThreadID %q, got %q", threadTargetID, msgs[0].ThreadID)
	}
}

type mockRoundTripper struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func TestDiscordThreadCreator_CreatePublicThread(t *testing.T) {
	// 1. Nil receiver / nil session
	var nilCreator *DiscordThreadCreator
	th, err := nilCreator.CreatePublicThread("chan1", "title")
	if err != nil || th != "chan1" {
		t.Errorf("expected channelID fallback for nil creator, got %q, %v", th, err)
	}

	creatorNilSess := NewDiscordThreadCreator(nil)
	th2, err := creatorNilSess.CreatePublicThread("chan1", "title")
	if err != nil || th2 != "chan1" {
		t.Errorf("expected channelID fallback for creator with nil session, got %q, %v", th2, err)
	}

	// 2. Mock Discord session success
	dg, _ := discordgo.New("Bot mock_token")
	dg.Client = &http.Client{
		Transport: &mockRoundTripper{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"id":"th-mock-999","name":"mock title"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	creator := NewDiscordThreadCreator(dg)
	thID, err := creator.CreatePublicThread("chan1", "mock title")
	if err != nil || thID != "th-mock-999" {
		t.Errorf("expected th-mock-999, got %q (err: %v)", thID, err)
	}

	// 3. Mock Discord session error
	dgErr, _ := discordgo.New("Bot mock_token")
	dgErr.Client = &http.Client{
		Transport: &mockRoundTripper{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(strings.NewReader(`{"message":"Bad request"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	creatorErr := NewDiscordThreadCreator(dgErr)
	_, err = creatorErr.CreatePublicThread("chan1", "title")
	if err == nil {
		t.Error("expected error from Discord API failure, got nil")
	}
}

func TestCalculateNextRun_UnknownTimezone(t *testing.T) {
	now := time.Now().UTC()
	next, err := CalculateNextRun("0 9 * * *", "Unknown/Invalid_Zone_12345", now)
	if err != nil {
		t.Fatalf("expected fallback to UTC on unknown timezone, got error: %v", err)
	}
	if next.IsZero() {
		t.Errorf("expected non-zero next run")
	}
}

func TestProcessDueSchedules_NilDBAndCancelledCtx(t *testing.T) {
	// 1. Nil DB
	if err := ProcessDueSchedules(context.Background(), nil, nil, nil); err != nil {
		t.Errorf("expected nil error for nil DB, got %v", err)
	}

	// 2. Cancelled context
	database, _ := db.InitDB(":memory:")
	defer database.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	_ = ProcessDueSchedules(ctx, database, nil, nil)
}

func TestSchedulerRun_TickLoop(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	enqueuer := newMockEnqueuer()
	threadCreator := newMockThreadCreator()

	// Run with 100 microseconds interval for 350ms so it ticks > 3000 times (exercising 120 and 2880 tick branches)
	go func() {
		time.Sleep(350 * time.Millisecond)
		cancel()
	}()

	Run(ctx, database, enqueuer, threadCreator, 100*time.Microsecond)
}

func TestProcessDueSchedules_ClosedDBAndErrors(t *testing.T) {
	database, err := db.InitDB(filepath.Join(t.TempDir(), "test_closed.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = database.Close()

	// Closed DB returns query error
	err = ProcessDueSchedules(context.Background(), database, nil, nil)
	if err == nil {
		t.Error("expected error for closed DB in ProcessDueSchedules")
	}
}

type errThreadCreator struct{}

func (e *errThreadCreator) CreatePublicThread(channelID, name string) (string, error) {
	return "", fmt.Errorf("failed to create thread")
}

func TestProcessDueCronSchedules_EmptyTitlePrefixAndThreadCreationError(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	cronSched := db.CronSchedule{
		ID:          "cron-empty-title",
		TargetID:    "chan-threads-target",
		TitlePrefix: "", // empty title prefix
		CronExpr:    "0 9 * * *",
		Prompt:      "Routine prompt",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-10 * time.Second),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	_ = db.CreateCronSchedule(database, cronSched)

	enqueuer := newMockEnqueuer()
	errCreator := &errThreadCreator{}

	// When thread creation fails, it falls back to target channel ID
	if err := ProcessDueSchedules(context.Background(), database, enqueuer, errCreator); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message enqueued, got %d", len(msgs))
	}
	if msgs[0].ThreadID != "chan-threads-target" {
		t.Errorf("expected fallback ThreadID chan-threads-target, got %q", msgs[0].ThreadID)
	}
}

func TestSchedulerRun_WithActiveConversationsAndFactExtraction(t *testing.T) {
	tmpDir := t.TempDir()
	mockAgy := filepath.Join(tmpDir, "mock_agy.sh")
	_ = os.WriteFile(mockAgy, []byte("#!/bin/sh\necho '{\"status\":\"SUCCESS\",\"response\":\"[{\\\"subject\\\":\\\"Alex\\\",\\\"predicate\\\":\\\"likes\\\",\\\"object\\\":\\\"matcha\\\",\\\"category\\\":\\\"preference\\\",\\\"confidence\\\":0.9}]\"}'\n"), 0755)
	t.Setenv("AGY_BIN", mockAgy)
	t.Setenv("GEMINI_API_KEY", "mock_key")

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	_ = db.InsertMessage(database, db.Message{
		ID:         "msg-active-1",
		ThreadID:   "thread-active-facts",
		AuthorID:   "user-100",
		AuthorName: "Alex",
		Content:    "I love drinking iced matcha in the morning",
		Status:     db.StatusCompleted,
		CreatedAt:  now,
		UpdatedAt:  now,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	enqueuer := newMockEnqueuer()
	threadCreator := newMockThreadCreator()

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	Run(ctx, database, enqueuer, threadCreator, 10*time.Millisecond)
}

func TestSchedulerStart_Stop(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := Start(ctx, database, nil, nil)
	time.Sleep(20 * time.Millisecond)
	stop()
	// Calling stop second time should be safe
	stop()
}

func TestProcessDueSchedules_ContextCancellationInLoops(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	// Insert 2 due crons
	for i := 1; i <= 2; i++ {
		_ = db.CreateCronSchedule(database, db.CronSchedule{
			ID:          fmt.Sprintf("cron-cancel-%d", i),
			TargetID:    "chan-test",
			TitlePrefix: "Cancel Test",
			CronExpr:    "0 9 * * *",
			Prompt:      "test",
			Timezone:    "UTC",
			NextRunAt:   now.Add(-10 * time.Minute),
			Enabled:     true,
			CreatedAt:   now.Add(-1 * time.Hour),
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	err = ProcessDueSchedules(ctx, database, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context canceled error, got %v", err)
	}

	// Insert 2 due one-shots
	for i := 1; i <= 2; i++ {
		_ = db.CreateOneShotSchedule(database, db.OneShotSchedule{
			ID:        fmt.Sprintf("oneshot-cancel-%d", i),
			ThreadID:  "th-test",
			Prompt:    "test",
			RunAt:     now.Add(-10 * time.Minute),
			CreatedAt: now.Add(-1 * time.Hour),
		})
	}

	err = ProcessDueSchedules(ctx, database, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context canceled error, got %v", err)
	}
}

func TestProcessDueSchedules_StaleCronWithInvalidCronExpr(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	// Overdue by > 24 hours with invalid cron expr
	cronSched := db.CronSchedule{
		ID:          "cron-stale-invalid",
		TargetID:    "chan-test",
		TitlePrefix: "Stale Invalid",
		CronExpr:    "invalid-cron-syntax",
		Prompt:      "stale prompt",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-48 * time.Hour),
		Enabled:     true,
		CreatedAt:   now.Add(-72 * time.Hour),
	}
	_ = db.CreateCronSchedule(database, cronSched)

	enqueuer := newMockEnqueuer()
	if err := ProcessDueSchedules(context.Background(), database, enqueuer, nil); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	// It should NOT enqueue a message since it's stale
	msgs := enqueuer.getMessages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for stale cron, got %d", len(msgs))
	}
}

func TestProcessDueSchedules_DueCronWithInvalidCronExpr(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	// Overdue by 5 minutes with invalid cron expr (not stale >24h, but invalid)
	cronSched := db.CronSchedule{
		ID:          "cron-due-invalid",
		TargetID:    "chan-test",
		TitlePrefix: "Due Invalid",
		CronExpr:    "invalid-cron-syntax",
		Prompt:      "due prompt",
		Timezone:    "UTC",
		NextRunAt:   now.Add(-5 * time.Minute),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	_ = db.CreateCronSchedule(database, cronSched)

	enqueuer := newMockEnqueuer()
	if err := ProcessDueSchedules(context.Background(), database, enqueuer, nil); err != nil {
		t.Fatalf("ProcessDueSchedules error: %v", err)
	}

	msgs := enqueuer.getMessages()
	if len(msgs) != 1 {
		t.Errorf("expected 1 message for due cron, got %d", len(msgs))
	}
}

func TestExtractFactsLLM_SuccessAndModelOverride(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{
		APIKey: "mock_key",
		Model:  "gemini-1.5-pro-custom",
	})
	sched := New(cfg, nil, nil, nil, WithRunnerFunc(func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		if model != "gemini-1.5-pro-custom" {
			return "", "", 1, fmt.Errorf("expected model gemini-1.5-pro-custom, got %s", model)
		}
		return `{"status":"SUCCESS","response":"extracted facts json"}`, "", 0, nil
	}))

	res, err := sched.ExtractFactsLLM(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("ExtractFactsLLM failed: %v", err)
	}
	if res != "extracted facts json" {
		t.Errorf("expected 'extracted facts json', got %q", res)
	}

	// Verify package compatibility wrapper returns error when unconfigured
	if _, err := ExtractFactsLLM(context.Background(), "test prompt"); err == nil {
		t.Errorf("expected error from unconfigured package ExtractFactsLLM")
	}
}

func TestExtractFactsLLM_DefaultModel(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{
		APIKey: "mock_key",
		Model:  "gemini-2.5-flash",
	})
	sched := New(cfg, nil, nil, nil, WithRunnerFunc(func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		if model != "gemini-2.5-flash" {
			return "", "", 1, fmt.Errorf("expected model gemini-2.5-flash, got %s", model)
		}
		return `{"status":"SUCCESS","response":"default model result"}`, "", 0, nil
	}))

	res, err := sched.ExtractFactsLLM(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("ExtractFactsLLM failed: %v", err)
	}
	if res != "default model result" {
		t.Errorf("expected 'default model result', got %q", res)
	}
}

func TestExtractFactsLLM_Errors(t *testing.T) {
	// 1. Runner fails with nonzero exit code
	cfgFail := config.NewFromData(&config.ConfigData{Model: "test-model"})
	schedFail := New(cfgFail, nil, nil, nil, WithRunnerFunc(func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		return "", "fatal error", 1, fmt.Errorf("exit 1")
	}))
	_, err := schedFail.ExtractFactsLLM(context.Background(), "prompt")
	if err == nil {
		t.Error("expected error on nonzero exit code")
	}

	// 2. Runner returns invalid JSON
	cfgBad := config.NewFromData(&config.ConfigData{Model: "test-model"})
	schedBad := New(cfgBad, nil, nil, nil, WithRunnerFunc(func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		return "not-json", "", 0, nil
	}))
	_, err = schedBad.ExtractFactsLLM(context.Background(), "prompt")
	if err == nil {
		t.Error("expected error on invalid JSON output")
	}
}

func TestExtractFactsLLM_PrefersLowEffortModel(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{
		APIKey:         "mock_key",
		Model:          "high-effort-model",
		LowEffortModel: "low-effort-model",
	})
	var capturedModel string
	sched := New(cfg, nil, nil, nil, WithRunnerFunc(func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		capturedModel = model
		return `{"status":"SUCCESS","response":"ok"}`, "", 0, nil
	}))

	_, err := sched.ExtractFactsLLM(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("ExtractFactsLLM failed: %v", err)
	}
	if capturedModel != "low-effort-model" {
		t.Errorf("expected low-effort-model, got %q", capturedModel)
	}
}

func TestRunPruneRetention(t *testing.T) {
	// 1. Nil DB
	RunPruneRetention(nil)

	// 2. Closed DB (error branch)
	database, err := db.InitDB(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = database.Close()
	RunPruneRetention(database)

	// 3. Valid DB with old runs pruned
	dbValid, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbValid.Close()

	oldTime := time.Now().UTC().Add(-60 * 24 * time.Hour)
	_ = db.CreateScheduleRun(dbValid, db.ScheduleRun{
		ID:           "old-run-1",
		ScheduleID:   "sched-1",
		ScheduleType: "cron",
		MessageID:    "msg-1",
		TargetID:     "chan-1",
		ThreadID:     "th-1",
		Title:        "Old Run",
		Prompt:       "Old Prompt",
		Status:       "completed",
		StartedAt:    oldTime,
		CompletedAt:  &oldTime,
	})

	RunPruneRetention(dbValid)
}

func TestRunFactExtraction(t *testing.T) {
	// 1. Nil arguments
	RunFactExtraction(nil, nil, nil, nil)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	client := memory.NewClient("")
	llmFunc := func(ctx context.Context, prompt string) (string, error) {
		return "[]", nil
	}

	// 2. Valid execution
	RunFactExtraction(context.Background(), database, client, llmFunc)

	// 3. Closed DB error
	closedDB, err := db.InitDB(filepath.Join(t.TempDir(), "fact_closed.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = closedDB.Close()
	RunFactExtraction(context.Background(), closedDB, client, llmFunc)
}

func TestScheduler_ConfigInjection(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	appCfg := config.NewFromData(&config.ConfigData{
		Timezone: "America/New_York",
		Model:    "custom-scheduler-model",
	})
	enqueuer := newMockEnqueuer()
	threadCreator := newMockThreadCreator()

	s := New(appCfg, database, enqueuer, threadCreator)
	if s == nil {
		t.Fatalf("expected non-nil scheduler")
	}
	if s.cfg != appCfg {
		t.Errorf("expected s.cfg to match injected appCfg")
	}
	if s.cfg.Current().Timezone != "America/New_York" {
		t.Errorf("expected timezone America/New_York, got %q", s.cfg.Current().Timezone)
	}

	if err := s.ProcessDueSchedules(context.Background()); err != nil {
		t.Errorf("unexpected error in ProcessDueSchedules: %v", err)
	}
}

func TestScheduler_WithSessionRoots(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	enqueuer := newMockEnqueuer()
	threadCreator := newMockThreadCreator()

	s := New(nil, database, enqueuer, threadCreator, WithSessionRoots(" /custom/root1 ", "", " /custom/root2 "))
	if len(s.sessionRoots) != 2 || s.sessionRoots[0] != "/custom/root1" || s.sessionRoots[1] != "/custom/root2" {
		t.Errorf("expected clean sessionRoots [/custom/root1 /custom/root2], got %v", s.sessionRoots)
	}
}



