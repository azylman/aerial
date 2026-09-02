package queue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/bwmarrin/discordgo"
)

func TestQueueSuccessLifecycleAndSessionSaving(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var deliveredText string
	var deliveredChannel string
	var mu sync.Mutex

	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			homeDir, _ := os.UserHomeDir()
			if homeDir == "" {
				homeDir = "/root"
			}
			sessDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", "session-uuid-123")
			_ = os.MkdirAll(sessDir, 0755)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)
			return "Clean output response", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredChannel = channelID
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:         "msg-101",
		ThreadID:   "thread-202",
		GuildID:    "guild-303",
		AuthorID:   "user-404",
		AuthorName: "User",
		Content:    "Hello Aerial",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	// Verify DB state
	dbMsg, err := db.GetMessage(database, "msg-101")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", dbMsg.Status)
	}

	// Verify Session saved
	savedSess, err := db.GetSessionID(database, "thread-202")
	if err != nil || savedSess != "session-uuid-123" {
		t.Errorf("Expected saved session session-uuid-123, got: %s (err: %v)", savedSess, err)
	}

	// Verify Delivery
	mu.Lock()
	if deliveredChannel != "thread-202" || deliveredText != "Clean output response" {
		t.Errorf("Unexpected delivery: channel=%q, text=%q", deliveredChannel, deliveredText)
	}
	mu.Unlock()
}

func TestQueueMultiThreadConcurrencyAndSingleThreadFIFO(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	var executionOrder []string
	var allDone sync.WaitGroup
	allDone.Add(3)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			executionOrder = append(executionOrder, prompt+"_start")
			mu.Unlock()

			if prompt == "A1" {
				time.Sleep(100 * time.Millisecond)
			} else if prompt == "B1" {
				time.Sleep(20 * time.Millisecond)
			}

			mu.Lock()
			executionOrder = append(executionOrder, prompt+"_end")
			mu.Unlock()
			return "OK", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			allDone.Done()
		},
	})
	pool.Start()
	defer pool.Stop()

	// Thread A: msg A1 then msg A2
	msgA1 := db.Message{ID: "m-A1", ThreadID: "ThreadA", Content: "A1"}
	msgA2 := db.Message{ID: "m-A2", ThreadID: "ThreadA", Content: "A2"}
	// Thread B: msg B1
	msgB1 := db.Message{ID: "m-B1", ThreadID: "ThreadB", Content: "B1"}

	_ = db.InsertMessage(database, msgA1)
	_ = db.InsertMessage(database, msgA2)
	_ = db.InsertMessage(database, msgB1)

	pool.Enqueue(msgA1)
	pool.Enqueue(msgA2)
	pool.Enqueue(msgB1)

	allDone.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Verify A1 starts before A2, and A1 ends before A2 starts (strict FIFO for Thread A)
	a1StartIndex, a1EndIndex, a2StartIndex, b1StartIndex := -1, -1, -1, -1
	for i, event := range executionOrder {
		switch event {
		case "A1_start":
			a1StartIndex = i
		case "A1_end":
			a1EndIndex = i
		case "A2_start":
			a2StartIndex = i
		case "B1_start":
			b1StartIndex = i
		}
	}

	if a1StartIndex > a1EndIndex || a1EndIndex > a2StartIndex {
		t.Errorf("Thread A was not serialized FIFO! Order: %v", executionOrder)
	}

	// Verify B1 executed concurrently without waiting for A2
	if b1StartIndex < 0 {
		t.Errorf("Thread B did not execute! Order: %v", executionOrder)
	}
}

func TestQueueTransientRetryPreservesSession(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.SaveSessionID(database, "thread-retry", "initial-session-uuid")

	attemptCount := 0
	var receivedSessions []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attemptCount++
			receivedSessions = append(receivedSessions, sessionID)
			curAttempt := attemptCount
			mu.Unlock()

			if curAttempt < 3 {
				return "", "Error 503: high demand unavailable", 1, fmt.Errorf("503")
			}
			return "Success after retries!", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-retry-1", ThreadID: "thread-retry", Content: "Hello"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for retry completion")
	}

	mu.Lock()
	if attemptCount != 3 {
		t.Errorf("Expected 3 attempts, got %d", attemptCount)
	}
	for i, sess := range receivedSessions {
		if sess != "initial-session-uuid" {
			t.Errorf("Attempt %d did not preserve session UUID: got %q", i+1, sess)
		}
	}
	mu.Unlock()

	dbMsg, _ := db.GetMessage(database, "msg-retry-1")
	if dbMsg.Status != db.StatusCompleted || dbMsg.RetryCount != 2 {
		t.Errorf("Expected status COMPLETED and retry_count 2, got status=%s, count=%d", dbMsg.Status, dbMsg.RetryCount)
	}
}

func TestQueueSessionCorruptionRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.SaveSessionID(database, "thread-corrupt", "broken-session-uuid")

	var notifiedMessages []string
	var sessionIDsPassed []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			sessionIDsPassed = append(sessionIDsPassed, sessionID)
			mu.Unlock()

			if sessionID == "broken-session-uuid" {
				return "", "Error: failed to load conversation: session corrupted", 1, fmt.Errorf("corrupt")
			}
			homeDir, _ := os.UserHomeDir()
			if homeDir == "" {
				homeDir = "/root"
			}
			sessDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", "fresh-uuid-999")
			_ = os.MkdirAll(sessDir, 0755)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)
			return "Clean output after session reset", "", 0, nil
		},
		NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
			return "I refreshed our conversation! ???"
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			notifiedMessages = append(notifiedMessages, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-corrupt-1", ThreadID: "thread-corrupt", GuildID: "guild-1", Content: "Hello"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for session corruption recovery")
	}

	mu.Lock()
	if len(sessionIDsPassed) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(sessionIDsPassed))
	}
	if sessionIDsPassed[0] != "broken-session-uuid" || sessionIDsPassed[1] != "" {
		t.Errorf("Expected broken session on attempt 1, then empty session on attempt 2: got %v", sessionIDsPassed)
	}
	if len(notifiedMessages) < 2 {
		t.Fatalf("Expected notification + final reply, got %d messages", len(notifiedMessages))
	}
	mu.Unlock()

	// Verify session in DB updated to fresh session
	finalSess, _ := db.GetSessionID(database, "thread-corrupt")
	if finalSess != "fresh-uuid-999" {
		t.Errorf("Expected final session fresh-uuid-999, got: %s", finalSess)
	}
}

func TestQueueTotalExhaustion(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	var deliveredNotifications []string
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "", "status: unavailable 503", 1, fmt.Errorf("unavailable")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredNotifications = append(deliveredNotifications, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-exhaust", ThreadID: "thread-exhaust", GuildID: "guild-1", Content: "Hello"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message exhaustion")
	}

	dbMsg, _ := db.GetMessage(database, "msg-exhaust")
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected status FAILED, got: %s", dbMsg.Status)
	}
	if dbMsg.RetryCount != 3 {
		t.Errorf("Expected retry_count 3, got: %d", dbMsg.RetryCount)
	}

	mu.Lock()
	expectedNotif := notifier.ModelUnavailableMessage()
	if len(deliveredNotifications) != 1 || deliveredNotifications[0] != expectedNotif {
		t.Errorf("Expected failure notification %q, got: %v", expectedNotif, deliveredNotifications)
	}
	mu.Unlock()
}

func TestRecoverInterrupted(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	t1 := time.Now().UTC().Add(-10 * time.Minute)
	t2 := time.Now().UTC().Add(-5 * time.Minute)
	t3 := time.Now().UTC().Add(-1 * time.Minute)

	// Same thread to test strict FIFO recovery
	msg1 := db.Message{ID: "m1", ThreadID: "thread-same", GuildID: "g1", Status: db.StatusPending, CreatedAt: t1}
	msg2 := db.Message{ID: "m2", ThreadID: "thread-same", GuildID: "g1", Status: db.StatusProcessing, CreatedAt: t2}
	msg3 := db.Message{ID: "m3", ThreadID: "thread-same", GuildID: "g1", Status: db.StatusCompleted, CreatedAt: t3}

	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)
	_ = db.InsertMessage(database, msg3)

	var recoveredIDs []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "OK", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			recoveredIDs = append(recoveredIDs, msg.ID)
			mu.Unlock()
			wg.Done()
		},
	})
	pool.Start()
	defer pool.Stop()

	RecoverInterrupted(database, pool)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(recoveredIDs) != 2 {
		t.Fatalf("Expected 2 recovered messages, got %d (%v)", len(recoveredIDs), recoveredIDs)
	}
	if recoveredIDs[0] != "m1" || recoveredIDs[1] != "m2" {
		t.Errorf("Expected m1 then m2, got: %v", recoveredIDs)
	}
}

func TestRecoverInterruptedPoisonPill(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	t1 := time.Now().UTC().Add(-10 * time.Minute)
	t2 := time.Now().UTC().Add(-5 * time.Minute)

	// msgPoison has StatusProcessing and RetryCount >= 3
	msgPoison := db.Message{
		ID:         "msg-poison",
		ThreadID:   "thread-poison",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		Content:    "crash command",
		Status:     db.StatusProcessing,
		RetryCount: 3,
		CreatedAt:  t1,
	}

	// msgNormal is a normal pending message
	msgNormal := db.Message{
		ID:         "msg-normal",
		ThreadID:   "thread-normal",
		GuildID:    "guild-1",
		AuthorID:   "user-2",
		Content:    "normal prompt",
		Status:     db.StatusPending,
		RetryCount: 0,
		CreatedAt:  t2,
	}

	_ = db.InsertMessage(database, msgPoison)
	_ = db.InsertMessage(database, msgNormal)

	var mu sync.Mutex
	var deliveredNotifs []string
	var completedIDs []string
	var wg sync.WaitGroup
	wg.Add(1) // Only msgNormal should be processed by worker pool

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "OK", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredNotifs = append(deliveredNotifs, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			completedIDs = append(completedIDs, msg.ID)
			mu.Unlock()
			wg.Done()
		},
	})
	pool.Start()
	defer pool.Stop()

	// Fake session to allow delivery
	pool.SetDiscordSession(&discordgo.Session{})

	RecoverInterrupted(database, pool)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Verify poison pill was NOT enqueued/completed by worker, but normal was
	if len(completedIDs) != 1 || completedIDs[0] != "msg-normal" {
		t.Errorf("Expected only msg-normal to be completed by worker, got: %v", completedIDs)
	}

	// Verify poison pill status in DB is FAILED
	poisonDB, err := db.GetMessage(database, "msg-poison")
	if err != nil || poisonDB == nil {
		t.Fatalf("Failed to query poison message: %v", err)
	}
	if poisonDB.Status != db.StatusFailed {
		t.Errorf("Expected poison pill status FAILED, got: %s", poisonDB.Status)
	}

	// Verify poison pill notification was delivered
	if len(deliveredNotifs) < 1 {
		t.Errorf("Expected poison pill notice to be delivered, got none")
	}
}

func TestQueueSkipDiscordLogic(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	var deliveredTo []string
	var wg sync.WaitGroup
	wg.Add(3)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "AI reply for " + prompt, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredTo = append(deliveredTo, channelID)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			wg.Done()
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. Scheduler message (should be delivered to Discord)
	msgScheduler := db.Message{
		ID:         "msg-sched-1",
		ThreadID:   "thread-scheduled-1",
		GuildID:    "scheduled",
		AuthorID:   "scheduler",
		AuthorName: "Scheduler",
		Content:    "Scheduled routine prompt",
	}

	// 2. HTTP client message (should SKIP Discord delivery)
	msgHTTP := db.Message{
		ID:         "msg-http-1",
		ThreadID:   "thread-http-1",
		GuildID:    "",
		AuthorID:   "http-client",
		AuthorName: "HTTP Client",
		Content:    "HTTP prompt",
	}

	// 3. Normal user message (should be delivered to Discord)
	msgUser := db.Message{
		ID:         "msg-user-1",
		ThreadID:   "thread-user-1",
		GuildID:    "guild-100",
		AuthorID:   "user-999",
		AuthorName: "Alice",
		Content:    "User prompt",
	}

	_ = db.InsertMessage(database, msgScheduler)
	_ = db.InsertMessage(database, msgHTTP)
	_ = db.InsertMessage(database, msgUser)

	pool.Enqueue(msgScheduler)
	pool.Enqueue(msgHTTP)
	pool.Enqueue(msgUser)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Delivered channels should contain thread-scheduled-1 and thread-user-1, but NOT thread-http-1
	deliveredMap := make(map[string]bool)
	for _, ch := range deliveredTo {
		deliveredMap[ch] = true
	}

	if !deliveredMap["thread-scheduled-1"] {
		t.Errorf("Expected scheduler message to be delivered to Discord, but it was skipped: %v", deliveredTo)
	}
	if deliveredMap["thread-http-1"] {
		t.Errorf("Expected http-client message to SKIP Discord delivery, but it was delivered: %v", deliveredTo)
	}
	if !deliveredMap["thread-user-1"] {
		t.Errorf("Expected user message to be delivered to Discord, but it was skipped: %v", deliveredTo)
	}
}

func TestWorkerPoolUpdateRuntimeConfig(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var receivedModel string
	var receivedTimeout int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		Model:          "initial-model-v1",
		TimeoutMinutes: 10,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			receivedModel = model
			receivedTimeout = timeoutMinutes
			mu.Unlock()
			return "OK", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Initial check
	m, tm := pool.GetRuntimeConfig()
	if m != "initial-model-v1" || tm != 10 {
		t.Errorf("Expected initial-model-v1 and 10, got model=%s timeout=%d", m, tm)
	}

	// Update runtime config
	pool.UpdateRuntimeConfig("updated-model-v2", 35)

	m2, tm2 := pool.GetRuntimeConfig()
	if m2 != "updated-model-v2" || tm2 != 35 {
		t.Errorf("Expected updated-model-v2 and 35, got model=%s timeout=%d", m2, tm2)
	}

	msg := db.Message{
		ID:         "msg-update-test",
		ThreadID:   "thread-update-test",
		Content:    "Hello update",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	if receivedModel != "updated-model-v2" || receivedTimeout != 35 {
		t.Errorf("Runner received unexpected runtime config: model=%q timeout=%d", receivedModel, receivedTimeout)
	}
	mu.Unlock()
}

func TestQueueScheduleRunLifecycle_Success(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			time.Sleep(10 * time.Millisecond)
			return "Success output", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	runID := "run-lifecycle-success-1"
	run := db.ScheduleRun{
		ID:           runID,
		ScheduleID:   "cron-s-1",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		ThreadID:     "thread-sched-s",
		Title:        "Morning Sync",
		Prompt:       "Sync prompt",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC().Add(-1 * time.Second),
	}
	if err := db.CreateScheduleRun(database, run); err != nil {
		t.Fatalf("Failed to create schedule run: %v", err)
	}

	msg := db.Message{
		ID:            "msg-sched-s-1",
		ThreadID:      "thread-sched-s",
		GuildID:       "scheduled",
		AuthorID:      "scheduler",
		Content:       "Sync prompt",
		Status:        db.StatusPending,
		ScheduleRunID: runID,
		CreatedAt:     time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for schedule message execution")
	}

	// Verify schedule run status transitioned to completed
	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, "cron-s-1", "")
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("Failed to get schedule run: %v (total=%d)", err, total)
	}

	r := runs[0]
	if r.Status != "completed" {
		t.Errorf("Expected status 'completed', got %q", r.Status)
	}
	if r.MessageID != "msg-sched-s-1" {
		t.Errorf("Expected message_id 'msg-sched-s-1', got %q", r.MessageID)
	}
	if r.CompletedAt == nil {
		t.Errorf("Expected CompletedAt to be set, got nil")
	}
	if r.DurationMs <= 0 {
		t.Errorf("Expected DurationMs > 0, got %d", r.DurationMs)
	}
	if r.Error != "" {
		t.Errorf("Expected Error to be empty, got %q", r.Error)
	}
}

func TestQueueScheduleRunLifecycle_Failure(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "", "Fatal execution error: out of memory", 1, fmt.Errorf("out of memory")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	runID := "run-lifecycle-fail-1"
	run := db.ScheduleRun{
		ID:           runID,
		ScheduleID:   "cron-f-1",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		ThreadID:     "thread-sched-f",
		Title:        "Failing Routine",
		Prompt:       "Fail prompt",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC().Add(-1 * time.Second),
	}
	if err := db.CreateScheduleRun(database, run); err != nil {
		t.Fatalf("Failed to create schedule run: %v", err)
	}

	msg := db.Message{
		ID:            "msg-sched-f-1",
		ThreadID:      "thread-sched-f",
		GuildID:       "scheduled",
		AuthorID:      "scheduler",
		Content:       "Fail prompt",
		Status:        db.StatusPending,
		ScheduleRunID: runID,
		CreatedAt:     time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for failing message processing")
	}

	// Verify schedule run status transitioned to failed with error
	runs, total, err := db.GetScheduleRunsPaginated(database, 10, 0, "cron-f-1", "")
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("Failed to get schedule run: %v (total=%d)", err, total)
	}

	r := runs[0]
	if r.Status != "failed" {
		t.Errorf("Expected status 'failed', got %q", r.Status)
	}
	if r.CompletedAt == nil {
		t.Errorf("Expected CompletedAt to be set, got nil")
	}
	if r.Error == "" {
		t.Errorf("Expected Error to be set on failure, got empty string")
	}
}

func TestQueueScheduleRunLifecycle_PanicRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			panic("simulated critical worker panic")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	runID := "run-panic-1"
	run := db.ScheduleRun{
		ID:           runID,
		ScheduleID:   "cron-panic-sched",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		ThreadID:     "thread-panic",
		Title:        "Panic Routine",
		Prompt:       "Panic prompt",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC(),
	}
	if err := db.CreateScheduleRun(database, run); err != nil {
		t.Fatalf("Failed to create schedule run: %v", err)
	}

	msg := db.Message{
		ID:            "msg-panic-1",
		ThreadID:      "thread-panic",
		GuildID:       "scheduled",
		AuthorID:      "scheduler",
		Content:       "Panic prompt",
		Status:        db.StatusPending,
		ScheduleRunID: runID,
		CreatedAt:     time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for panic recovery")
	}

	// Verify message in DB was marked FAILED
	dbMsg, err := db.GetMessage(database, "msg-panic-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected message status FAILED, got %s", dbMsg.Status)
	}

	// Verify schedule run was marked failed with panic info
	runs, _, err := db.GetScheduleRunsPaginated(database, 10, 0, "cron-panic-sched", "")
	if err != nil || len(runs) != 1 {
		t.Fatalf("Failed to get schedule run: %v", err)
	}
	if runs[0].Status != "failed" {
		t.Errorf("Expected schedule run status 'failed', got %q", runs[0].Status)
	}
	if runs[0].Error == "" || runs[0].CompletedAt == nil {
		t.Errorf("Expected Error and CompletedAt to be set on panic, got error=%q completedAt=%v", runs[0].Error, runs[0].CompletedAt)
	}
}

func TestRecoverInterrupted_ReconcilesOrphanedScheduleRuns(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Insert orphaned runs stuck in 'enqueued' and 'running'
	orphanedEnqueued := db.ScheduleRun{
		ID:           "run-orphan-enqueued",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "chan-1",
		ThreadID:     "thread-1",
		Prompt:       "Orphan prompt 1",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC().Add(-1 * time.Hour),
	}
	orphanedRunning := db.ScheduleRun{
		ID:           "run-orphan-running",
		ScheduleID:   "cron-2",
		ScheduleType: "cron",
		TargetID:     "chan-2",
		ThreadID:     "thread-2",
		Prompt:       "Orphan prompt 2",
		Status:       "running",
		StartedAt:    time.Now().UTC().Add(-1 * time.Hour),
	}
	completedRun := db.ScheduleRun{
		ID:           "run-already-completed",
		ScheduleID:   "cron-3",
		ScheduleType: "cron",
		TargetID:     "chan-3",
		ThreadID:     "thread-3",
		Prompt:       "Completed prompt",
		Status:       "completed",
		StartedAt:    time.Now().UTC().Add(-2 * time.Hour),
	}

	_ = db.CreateScheduleRun(database, orphanedEnqueued)
	_ = db.CreateScheduleRun(database, orphanedRunning)
	_ = db.CreateScheduleRun(database, completedRun)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
	})
	pool.Start()
	defer pool.Stop()

	RecoverInterrupted(database, pool)

	// Verify orphaned runs are reconciled to 'failed'
	runs, _, err := db.GetScheduleRunsPaginated(database, 10, 0, "", "")
	if err != nil {
		t.Fatalf("Failed to query schedule runs: %v", err)
	}

	for _, r := range runs {
		if r.ID == "run-orphan-enqueued" || r.ID == "run-orphan-running" {
			if r.Status != "failed" {
				t.Errorf("Expected run %s to be 'failed', got %q", r.ID, r.Status)
			}
			if r.Error != "Interrupted by server restart" {
				t.Errorf("Expected error 'Interrupted by server restart', got %q", r.Error)
			}
			if r.CompletedAt == nil {
				t.Errorf("Expected CompletedAt to be set for reconciled run %s", r.ID)
			}
		} else if r.ID == "run-already-completed" {
			if r.Status != "completed" {
				t.Errorf("Expected run %s to remain 'completed', got %q", r.ID, r.Status)
			}
		}
	}
}

func TestWorkerPool_InjectsSemanticMemoryFacts(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var capturedPrompt string
	doneCh := make(chan struct{})

	mockRetriever := func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
		return []db.Fact{
			{Category: "system_config", FactText: "Server runs on port 8080", Importance: 1.0},
		}, nil
	}

	mockRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
		capturedPrompt = prompt
		return "Response text", "", 0, nil
	}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:                  database,
		MemoryRetrieverFunc: mockRetriever,
		RunnerFunc:          mockRunner,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	originalContent := "What port does the server run on?"
	msg := db.Message{
		ID:        "msg-mem-1",
		ThreadID:  "thread-mem-1",
		AuthorID:  "user-1",
		Content:   originalContent,
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message execution")
	}

	// Verify capturedPrompt contains memory block
	expectedBlock := "<retrieved_memory>\n- [system_config] Server runs on port 8080\n</retrieved_memory>"
	if !strings.Contains(capturedPrompt, expectedBlock) {
		t.Errorf("Expected capturedPrompt to contain %q, got: %s", expectedBlock, capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, originalContent) {
		t.Errorf("Expected capturedPrompt to contain %q, got: %s", originalContent, capturedPrompt)
	}

	// Verify DB message content was preserved as original
	dbMsg, _ := db.GetMessage(database, "msg-mem-1")
	if dbMsg.Content != originalContent {
		t.Errorf("Expected DB message content to be %q, got %q", originalContent, dbMsg.Content)
	}
}

func TestWorkerPool_SemanticMemoryGracefulFallbackOnError(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var capturedPrompt string
	doneCh := make(chan struct{})

	mockRetriever := func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
		return nil, fmt.Errorf("ollama connection refused")
	}

	mockRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
		capturedPrompt = prompt
		return "Response text", "", 0, nil
	}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:                  database,
		MemoryRetrieverFunc: mockRetriever,
		RunnerFunc:          mockRunner,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	originalContent := "What port does the server run on?"
	msg := db.Message{
		ID:        "msg-mem-2",
		ThreadID:  "thread-mem-2",
		AuthorID:  "user-1",
		Content:   originalContent,
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message execution")
	}

	// Verify capturedPrompt equals originalContent without injected block
	if capturedPrompt != originalContent {
		t.Errorf("Expected capturedPrompt to equal %q, got: %s", originalContent, capturedPrompt)
	}
}

func TestQueueSilentSentinelSuppression(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var deliveryCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		MemoryRetrieverFunc: func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "[NO_REPLY]", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:        "msg-sentinel-1",
		ThreadID:  "thread-sentinel",
		AuthorID:  "user-1",
		Content:   "Silent prompt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for sentinel message processing")
	}

	mu.Lock()
	if deliveryCalls != 0 {
		t.Errorf("Expected 0 delivery calls for silent sentinel [NO_REPLY], got %d", deliveryCalls)
	}
	mu.Unlock()

	// Verify message in SQLite is COMPLETED
	dbMsg, err := db.GetMessage(database, "msg-sentinel-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", dbMsg.Status)
	}
	if dbMsg.ResponseText != "[NO_REPLY]" {
		t.Errorf("Expected response text '[NO_REPLY]', got %q", dbMsg.ResponseText)
	}
}

func TestQueueBurstCoalescing(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var capturedPrompt string
	var deliveredTexts []string
	var mu sync.Mutex
	var completedCount int
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		MemoryRetrieverFunc: func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			capturedPrompt = prompt
			mu.Unlock()
			return "Coalesced reply from agent", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredTexts = append(deliveredTexts, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			completedCount++
			if completedCount == 3 {
				close(doneCh)
			}
			mu.Unlock()
		},
	})

	t0 := time.Now().UTC().Add(-10 * time.Second)
	t1 := t0.Add(3 * time.Second)
	t2 := t0.Add(6 * time.Second)
	msg1 := db.Message{ID: "m-b-1", ThreadID: "thread-burst", AuthorName: "Alice", Content: "Hello from Alice", Status: db.StatusPending, CreatedAt: t0}
	msg2 := db.Message{ID: "m-b-2", ThreadID: "thread-burst", AuthorName: "Bob", Content: "Hello from Bob", Status: db.StatusPending, CreatedAt: t1}
	msg3 := db.Message{ID: "m-b-3", ThreadID: "thread-burst", AuthorName: "Charlie", Content: "Hello from Charlie", Status: db.StatusPending, CreatedAt: t2}

	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)
	_ = db.InsertMessage(database, msg3)

	// Enqueue all 3 in rapid succession to form a burst
	pool.Enqueue(msg1)
	pool.Enqueue(msg2)
	pool.Enqueue(msg3)

	pool.Start()
	defer pool.Stop()

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for burst execution")
	}

	mu.Lock()
	defer mu.Unlock()

	// 1. Verify prompt coalescing format
	if !strings.Contains(capturedPrompt, "<USER_REQUEST>") || !strings.Contains(capturedPrompt, "[Multiple messages received in channel]") {
		t.Errorf("Expected coalesced prompt header, got: %s", capturedPrompt)
	}
	expectedM1 := fmt.Sprintf("--- Message 1 (by @Alice at %s) ---", t0.Format("15:04:05"))
	if !strings.Contains(capturedPrompt, expectedM1) || !strings.Contains(capturedPrompt, "Hello from Alice") {
		t.Errorf("Expected message 1 (%s) in coalesced prompt, got: %s", expectedM1, capturedPrompt)
	}
	expectedM2 := fmt.Sprintf("--- Message 2 (by @Bob at %s) ---", t1.Format("15:04:05"))
	if !strings.Contains(capturedPrompt, expectedM2) || !strings.Contains(capturedPrompt, "Hello from Bob") {
		t.Errorf("Expected message 2 (%s) in coalesced prompt, got: %s", expectedM2, capturedPrompt)
	}
	expectedM3 := fmt.Sprintf("--- Message 3 (by @Charlie at %s) ---", t2.Format("15:04:05"))
	if !strings.Contains(capturedPrompt, expectedM3) || !strings.Contains(capturedPrompt, "Hello from Charlie") {
		t.Errorf("Expected message 3 (%s) in coalesced prompt, got: %s", expectedM3, capturedPrompt)
	}

	// 2. Verify all messages marked COMPLETED in SQLite
	for _, id := range []string{"m-b-1", "m-b-2", "m-b-3"} {
		m, err := db.GetMessage(database, id)
		if err != nil || m == nil || m.Status != db.StatusCompleted {
			t.Errorf("Expected message %s to be COMPLETED, got: %+v (err: %v)", id, m, err)
		}
	}

	// 3. Verify single delivery call for the combined turn
	if len(deliveredTexts) != 1 || deliveredTexts[0] != "Coalesced reply from agent" {
		t.Errorf("Expected 1 delivery with agent reply, got: %v", deliveredTexts)
	}
}

func TestQueueStalenessDrop(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int
	var deliveryCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		MemoryRetrieverFunc: func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return "Should not run", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// 6-minute old message
	staleCreatedAt := time.Now().UTC().Add(-6 * time.Minute)
	msg := db.Message{
		ID:        "msg-stale-1",
		ThreadID:  "thread-stale",
		AuthorID:  "user-1",
		Content:   "Old stale prompt",
		Status:    db.StatusPending,
		CreatedAt: staleCreatedAt,
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for stale message processing")
	}

	mu.Lock()
	if runnerCalls != 0 {
		t.Errorf("Expected 0 runner calls for stale message, got %d", runnerCalls)
	}
	if deliveryCalls != 0 {
		t.Errorf("Expected 0 delivery calls for stale message, got %d", deliveryCalls)
	}
	mu.Unlock()

	// Verify message marked COMPLETED with [EXPIRED_STALE]
	dbMsg, err := db.GetMessage(database, "msg-stale-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to query stale message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", dbMsg.Status)
	}
	if dbMsg.ResponseText != "[EXPIRED_STALE]" {
		t.Errorf("Expected response text '[EXPIRED_STALE]', got %q", dbMsg.ResponseText)
	}
}

func TestQueueTurnCountSessionRotation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "channel-rotation-test"
	initialSessionID := "sess-channel-init-123"
	_ = db.SaveSessionID(database, channelID, initialSessionID)

	var mu sync.Mutex
	var completedCh chan struct{}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		ResolveChannelPolicy: func(cID, cName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:            "channel",
				MaxSessionTurns: 3,
				TypingIndicator: "always",
			}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "OK response", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			ch := completedCh
			mu.Unlock()
			if ch != nil {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	// Send turn 1
	completedCh = make(chan struct{}, 1)
	msg1 := db.Message{ID: "m-rot-1", ThreadID: channelID, Content: "Turn 1", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg1)
	pool.Enqueue(msg1)
	<-completedCh

	c1, _ := db.GetSessionTurnCount(database, channelID)
	s1, _ := db.GetSessionID(database, channelID)
	if c1 != 1 || s1 != initialSessionID {
		t.Fatalf("Expected turn_count=1 and initial session ID after turn 1, got count=%d, sess=%s", c1, s1)
	}

	// Send turn 2
	completedCh = make(chan struct{}, 1)
	msg2 := db.Message{ID: "m-rot-2", ThreadID: channelID, Content: "Turn 2", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg2)
	pool.Enqueue(msg2)
	<-completedCh

	c2, _ := db.GetSessionTurnCount(database, channelID)
	s2, _ := db.GetSessionID(database, channelID)
	if c2 != 2 || s2 != initialSessionID {
		t.Fatalf("Expected turn_count=2 and initial session ID after turn 2, got count=%d, sess=%s", c2, s2)
	}

	// Send turn 3 (reaches limit of 3 turns)
	completedCh = make(chan struct{}, 1)
	msg3 := db.Message{ID: "m-rot-3", ThreadID: channelID, Content: "Turn 3", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg3)
	pool.Enqueue(msg3)
	<-completedCh

	// Verify post-3-turns state:
	// Session ID should be rotated to a new UUID (not initialSessionID)
	// turn_count should be reset to 0
	finalSessionID, err := db.GetSessionID(database, channelID)
	if err != nil {
		t.Fatalf("Failed to query session ID: %v", err)
	}
	if finalSessionID == "" || finalSessionID == initialSessionID {
		t.Errorf("Expected session ID to be rotated from %s, got: %s", initialSessionID, finalSessionID)
	}

	finalTurnCount, err := db.GetSessionTurnCount(database, channelID)
	if err != nil {
		t.Fatalf("Failed to query turn count: %v", err)
	}
	if finalTurnCount != 0 {
		t.Errorf("Expected turn_count=0 after rotation, got %d", finalTurnCount)
	}
}

func TestQueueTypingIndicatorPolicies(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var typingCalls int
	var mu sync.Mutex
	var currentPolicy config.ChannelPolicy

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		ResolveChannelPolicy: func(cID, cName string) config.ChannelPolicy {
			mu.Lock()
			defer mu.Unlock()
			return currentPolicy
		},
		MemoryRetrieverFunc: func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "OK", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			mu.Lock()
			typingCalls++
			mu.Unlock()
			return func() {}
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. Policy: TypingIndicator = "never"
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "channel", TypingIndicator: "never"}
	typingCalls = 0
	mu.Unlock()

	doneCh1 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh1) }
	msg1 := db.Message{ID: "m-type-1", ThreadID: "th-never", Content: "Hello @Aerial", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg1)
	pool.Enqueue(msg1)
	<-doneCh1

	mu.Lock()
	if typingCalls != 0 {
		t.Errorf("Expected 0 typing calls for policy 'never', got %d", typingCalls)
	}
	mu.Unlock()

	// 2. Policy: TypingIndicator = "on_mention", without mention
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "channel", TypingIndicator: "on_mention"}
	typingCalls = 0
	mu.Unlock()

	doneCh2 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh2) }
	msg2 := db.Message{ID: "m-type-2", ThreadID: "th-mention-no", Content: "Just talking to friends", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg2)
	pool.Enqueue(msg2)
	<-doneCh2

	mu.Lock()
	if typingCalls != 0 {
		t.Errorf("Expected 0 typing calls for policy 'on_mention' without mention, got %d", typingCalls)
	}
	mu.Unlock()

	// 3. Policy: TypingIndicator = "on_mention", with mention
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "channel", TypingIndicator: "on_mention"}
	typingCalls = 0
	mu.Unlock()

	doneCh3 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh3) }
	msg3 := db.Message{ID: "m-type-3", ThreadID: "th-mention-yes", Content: "Hey @Aerial help me", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg3)
	pool.Enqueue(msg3)
	<-doneCh3

	mu.Lock()
	if typingCalls != 1 {
		t.Errorf("Expected 1 typing call for policy 'on_mention' with mention, got %d", typingCalls)
	}
	mu.Unlock()

	// 4. Policy: TypingIndicator = "always"
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "threads", TypingIndicator: "always"}
	typingCalls = 0
	mu.Unlock()

	doneCh4 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh4) }
	msg4 := db.Message{ID: "m-type-4", ThreadID: "th-always", Content: "Any prompt", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg4)
	pool.Enqueue(msg4)
	<-doneCh4

	mu.Lock()
	if typingCalls != 1 {
		t.Errorf("Expected 1 typing call for policy 'always', got %d", typingCalls)
	}
	mu.Unlock()
}

func TestQueueIgnoredChannelPolicy(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	runnerCalls := 0
	deliveryCalls := 0

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return "Should not execute", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			mu.Unlock()
			return nil
		},
		ResolveChannelPolicy: func(threadID, channelName string) config.ChannelPolicy {
			if threadID == "chan-ignored-123" {
				return config.ChannelPolicy{Mode: "ignore"}
			}
			return config.ChannelPolicy{Mode: "threads"}
		},
	})
	pool.Start()
	defer pool.Stop()

	doneCh := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
		close(doneCh)
	}

	msg := db.Message{
		ID:        "msg-ignored-1",
		ThreadID:  "chan-ignored-123",
		Content:   "Hello ignored room",
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for message completion in ignored channel")
	}

	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 0 {
		t.Errorf("Expected 0 runner calls for ignored channel, got %d", runnerCalls)
	}
	if deliveryCalls != 0 {
		t.Errorf("Expected 0 delivery calls for ignored channel, got %d", deliveryCalls)
	}

	savedMsg, err := db.GetMessage(database, "msg-ignored-1")
	if err != nil || savedMsg == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if savedMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status COMPLETED, got %s", savedMsg.Status)
	}
	if savedMsg.ErrorMessage != "[IGNORE]" {
		t.Errorf("Expected message error detail '[IGNORE]', got %q", savedMsg.ErrorMessage)
	}
}






