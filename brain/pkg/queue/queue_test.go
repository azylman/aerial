package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
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



