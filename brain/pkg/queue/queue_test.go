package queue

import (
	"bytes"
	"context"
	"image"
	"image/png"
	_ "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

func TestWorkerPool_ConfigInjection(t *testing.T) {
	appCfg := config.NewFromData(&config.ConfigData{
		Model:         "initial-model",
		SystemChannel: "initial-alerts",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})
	pool := New(appCfg, WorkerPoolConfig{})
	if pool == nil {
		t.Fatalf("expected non-nil pool")
	}
	if pool.appCfg != appCfg {
		t.Errorf("expected pool.appCfg to match injected pointer")
	}
	if pool.appCfg.Current().Model != "initial-model" {
		t.Errorf("expected initial model, got %q", pool.appCfg.Current().Model)
	}
}

func mockJSONResponse(convID, responseText string) string {
	if convID == "" {
		convID = uuid.New().String()
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"conversation_id":  convID,
		"status":           "SUCCESS",
		"response":         responseText,
		"duration_seconds": 1.0,
		"num_turns":        1,
	})
	return string(payload)
}

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

	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	t.Setenv("USERPROFILE", tmpHome)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", "f1111111-2222-3333-4444-555555555555")
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)
			return mockJSONResponse("f1111111-2222-3333-4444-555555555555", "Clean output response"), "", 0, nil
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
	if err != nil || savedSess != "f1111111-2222-3333-4444-555555555555" {
		t.Errorf("Expected saved session f1111111-2222-3333-4444-555555555555, got: %s (err: %v)", savedSess, err)
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
			key := prompt
			if strings.Contains(prompt, "A1") {
				key = "A1"
			} else if strings.Contains(prompt, "B1") {
				key = "B1"
			}

			mu.Lock()
			executionOrder = append(executionOrder, key+"_start")
			mu.Unlock()

			if strings.Contains(prompt, "A1") {
				time.Sleep(100 * time.Millisecond)
			} else if strings.Contains(prompt, "B1") {
				time.Sleep(20 * time.Millisecond)
			}

			mu.Lock()
			executionOrder = append(executionOrder, key+"_end")
			mu.Unlock()
			return mockJSONResponse("f2222222-2222-3333-4444-555555555555", "OK"), "", 0, nil
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
	pool.Enqueue(msgB1)
	time.Sleep(50 * time.Millisecond)
	pool.Enqueue(msgA2)

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

	_ = db.SaveSessionID(database, "thread-retry", "b1111111-2222-3333-4444-555555555555")

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
			return mockJSONResponse("b1111111-2222-3333-4444-555555555555", "Success after retries!"), "", 0, nil
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
		if sess != "b1111111-2222-3333-4444-555555555555" {
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

	_ = db.SaveSessionID(database, "thread-corrupt", "a1111111-2222-3333-4444-555555555555")

	var notifiedMessages []string
	var sessionIDsPassed []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	t.Setenv("USERPROFILE", tmpHome)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			sessionIDsPassed = append(sessionIDsPassed, sessionID)
			mu.Unlock()

			if sessionID == "a1111111-2222-3333-4444-555555555555" {
				return "", "Error: failed to load conversation: session corrupted", 1, fmt.Errorf("corrupt")
			}
			sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", "d8b5e679-7425-40de-944b-e07fc1f90ae7")
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)
			return mockJSONResponse("d8b5e679-7425-40de-944b-e07fc1f90ae7", "Clean output after session reset"), "", 0, nil
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
	if sessionIDsPassed[0] != "a1111111-2222-3333-4444-555555555555" || sessionIDsPassed[1] != "" {
		t.Errorf("Expected broken session on attempt 1, then empty session on attempt 2: got %v", sessionIDsPassed)
	}
	if len(notifiedMessages) < 2 {
		t.Fatalf("Expected notification + final reply, got %d messages", len(notifiedMessages))
	}
	mu.Unlock()

	// Verify session in DB updated to fresh session
	finalSess, _ := db.GetSessionID(database, "thread-corrupt")
	if finalSess != "d8b5e679-7425-40de-944b-e07fc1f90ae7" {
		t.Errorf("Expected final session d8b5e679-7425-40de-944b-e07fc1f90ae7, got: %s", finalSess)
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
			return mockJSONResponse("e1111111-2222-3333-4444-555555555555", "OK"), "", 0, nil
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
			return mockJSONResponse("e1111111-2222-3333-4444-555555555555", "OK"), "", 0, nil
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
			return mockJSONResponse("", "AI reply for " + prompt), "", 0, nil
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
			mu.Unlock()
			return mockJSONResponse("", "OK"), "", 0, nil
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
	m := pool.GetRuntimeConfig()
	if m != "initial-model-v1" {
		t.Errorf("Expected initial-model-v1, got model=%s", m)
	}

	// Update runtime config
	pool.UpdateRuntimeConfig("updated-model-v2")

	m2 := pool.GetRuntimeConfig()
	if m2 != "updated-model-v2" {
		t.Errorf("Expected updated-model-v2, got model=%s", m2)
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
	if receivedModel != "updated-model-v2" {
		t.Errorf("Runner received unexpected runtime config: model=%q", receivedModel)
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
			return mockJSONResponse("e1111111-2222-3333-4444-555555555555", "Success output"), "", 0, nil
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

	mockRetriever := func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
		return []db.Fact{
			{Category: "system_config", FactText: "Server runs on port 8080", Importance: 1.0},
		}, nil
	}

	mockRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
		capturedPrompt = prompt
		return mockJSONResponse("", "Response text"), "", 0, nil
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

	mockRetriever := func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
		return nil, fmt.Errorf("ollama connection refused")
	}

	mockRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
		capturedPrompt = prompt
		return mockJSONResponse("e1111111-2222-3333-4444-555555555555", "Response text"), "", 0, nil
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
	if !strings.Contains(capturedPrompt, originalContent) || strings.Contains(capturedPrompt, "<FACTS>") {
		t.Errorf("Expected capturedPrompt to contain %q without <FACTS>, got: %s", originalContent, capturedPrompt)
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
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return mockJSONResponse("", ""), "", 0, nil
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
		t.Errorf("Expected 0 delivery calls for empty response, got %d", deliveryCalls)
	}
	mu.Unlock()

	// Verify message in DB is COMPLETED
	dbMsg, err := db.GetMessage(database, "msg-sentinel-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", dbMsg.Status)
	}
	if dbMsg.ResponseText != "" {
		t.Errorf("Expected empty response text, got %q", dbMsg.ResponseText)
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
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			capturedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse("", "Coalesced reply from agent"), "", 0, nil
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

	// 2. Verify all messages marked COMPLETED in DB
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
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "Should not run"), "", 0, nil
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

	// 1. Message created 10 minutes ago should NOT be dropped with default 30-minute TTL
	validCreatedAt := time.Now().UTC().Add(-10 * time.Minute)
	msgValid := db.Message{
		ID:        "msg-valid-1",
		ThreadID:  "thread-valid",
		AuthorID:  "user-1",
		Content:   "Recent prompt within 30m window",
		Status:    db.StatusPending,
		CreatedAt: validCreatedAt,
	}
	_ = db.InsertMessage(database, msgValid)
	pool.Enqueue(msgValid)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for valid message processing")
	}

	mu.Lock()
	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for 10-minute-old message (within 30m TTL), got %d", runnerCalls)
	}
	mu.Unlock()

	// 2. Message created 31 minutes ago SHOULD be dropped as [EXPIRED_STALE]
	doneChStale := make(chan struct{})
	pool.mu.Lock()
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
		close(doneChStale)
	}
	pool.mu.Unlock()

	staleCreatedAt := time.Now().UTC().Add(-31 * time.Minute)
	msgStale := db.Message{
		ID:        "msg-stale-1",
		ThreadID:  "thread-stale",
		AuthorID:  "user-1",
		Content:   "Old stale prompt",
		Status:    db.StatusPending,
		CreatedAt: staleCreatedAt,
	}
	_ = db.InsertMessage(database, msgStale)
	pool.Enqueue(msgStale)

	select {
	case <-doneChStale:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for stale message processing")
	}

	mu.Lock()
	if runnerCalls != 1 {
		t.Errorf("Expected runner calls to remain 1 after stale message, got %d", runnerCalls)
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

func TestQueueCustomStalenessTTL(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		StalenessTTL:   10 * time.Minute,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "OK"), "", 0, nil
		},
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

	// 12-minute old message with 10-minute StalenessTTL should be dropped
	msg := db.Message{
		ID:        "msg-custom-stale",
		ThreadID:  "thread-custom-stale",
		AuthorID:  "user-1",
		Content:   "Should be stale for 10m TTL",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC().Add(-12 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for custom stale message processing")
	}

	mu.Lock()
	if runnerCalls != 0 {
		t.Errorf("Expected 0 runner calls for custom stale message, got %d", runnerCalls)
	}
	mu.Unlock()

	dbMsg, err := db.GetMessage(database, "msg-custom-stale")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to query custom stale message: %v", err)
	}
	if dbMsg.ResponseText != "[EXPIRED_STALE]" {
		t.Errorf("Expected response text '[EXPIRED_STALE]', got %q", dbMsg.ResponseText)
	}
}

func TestQueueTurnCountSessionRotation(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "channel-rotation-test"
	initialSessionID := "sess-channel-init-123"
	_ = db.SaveSessionID(database, channelID, initialSessionID)

	// Seed turn_count to DefaultMaxSessionTurns - 2 (48 turns)
	for i := 0; i < DefaultMaxSessionTurns-2; i++ {
		_, _ = db.IncrementSessionTurnCount(database, channelID)
	}

	_, _ = session.EnsureSessionDir(initialSessionID)
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, initialSessionID+".pb"), []byte("mock-pb"), 0644)

	var mu sync.Mutex
	var completedCh chan struct{}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		ResolveChannelPolicy: func(cID, cName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode: "channel",
			}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			if sessionID == "" {
				sessionID = "sess-turn-" + uuid.New().String()
			}
			stderr = fmt.Sprintf("Starting conversation update stream for %s\n", sessionID)
			return mockJSONResponse(sessionID, "OK response"), stderr, 0, nil
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

	// Send turn 49 (DefaultMaxSessionTurns - 1)
	completedCh = make(chan struct{}, 1)
	msg1 := db.Message{ID: "m-rot-1", ThreadID: channelID, Content: "Aerial Turn 49", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg1)
	pool.Enqueue(msg1)
	<-completedCh

	c1, _ := db.GetSessionTurnCount(database, channelID)
	s1, _ := db.GetSessionID(database, channelID)
	if c1 != DefaultMaxSessionTurns-1 || s1 != initialSessionID {
		t.Fatalf("Expected turn_count=%d and initial session ID after turn 49, got count=%d, sess=%s", DefaultMaxSessionTurns-1, c1, s1)
	}

	// Send turn 50 (hits DefaultMaxSessionTurns limit)
	completedCh = make(chan struct{}, 1)
	msg2 := db.Message{ID: "m-rot-2", ThreadID: channelID, Content: "Aerial Turn 50", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg2)
	pool.Enqueue(msg2)
	<-completedCh

	// Verify post-50-turns state:
	// Session ID should be rotated to cold state ""
	// turn_count should be reset to 0
	finalSessionID, err := db.GetSessionID(database, channelID)
	if err != nil {
		t.Fatalf("Failed to query session ID: %v", err)
	}
	if finalSessionID != "" {
		t.Errorf("Expected session ID to be reset to cold state \"\", got: %s", finalSessionID)
	}

	finalTurnCount, err := db.GetSessionTurnCount(database, channelID)
	if err != nil {
		t.Fatalf("Failed to query turn count: %v", err)
	}
	if finalTurnCount != 0 {
		t.Errorf("Expected turn_count=0 after rotation, got %d", finalTurnCount)
	}
}

func TestQueueUniversalActiveTurnTyping(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var typingCalls int
	var mu sync.Mutex
	var currentPolicy config.ChannelPolicy
	var classifierConfidence float64 = 0.9

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		mu.Lock()
		conf := classifierConfidence
		mu.Unlock()
		return fmt.Sprintf(`{"confidence": %.2f, "reason": "test classification"}`, conf), nil
	}))

	threshold := 0.8
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		Classifier:     cls,
		ResolveChannelPolicy: func(cID, cName string) config.ChannelPolicy {
			mu.Lock()
			defer mu.Unlock()
			return currentPolicy
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return mockJSONResponse("", "OK"), "", 0, nil
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

	// 1. Thread mode active turn -> typing indicator invoked
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "threads"}
	typingCalls = 0
	mu.Unlock()

	doneCh1 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh1) }
	msg1 := db.Message{ID: "m-type-1", ThreadID: "th-active-1", Content: "Hello thread", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg1)
	pool.Enqueue(msg1)
	<-doneCh1

	mu.Lock()
	if typingCalls != 1 {
		t.Errorf("Expected 1 typing call for threads mode active turn, got %d", typingCalls)
	}
	mu.Unlock()

	// 2. Channel mode direct mention wake -> typing indicator invoked
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}
	typingCalls = 0
	mu.Unlock()

	doneCh2 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh2) }
	msg2 := db.Message{ID: "m-type-2", ThreadID: "th-mention-yes", Content: "<@Aerial> help me", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg2)
	pool.Enqueue(msg2)
	<-doneCh2

	mu.Lock()
	if typingCalls != 1 {
		t.Errorf("Expected 1 typing call for channel mode direct mention, got %d", typingCalls)
	}
	mu.Unlock()

	// 3. Channel mode ambient classifier wake -> typing indicator invoked
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: &threshold}
	classifierConfidence = 0.95
	typingCalls = 0
	mu.Unlock()

	doneCh3 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh3) }
	msg3 := db.Message{ID: "m-type-3", ThreadID: "th-ambient-wake", Content: "Let's change all of these to 8am", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg3)
	pool.Enqueue(msg3)
	<-doneCh3

	mu.Lock()
	if typingCalls != 1 {
		t.Errorf("Expected 1 typing call for channel mode ambient classifier wake, got %d", typingCalls)
	}
	mu.Unlock()

	// 4. Channel mode pure ambient non-wake chatter -> typing indicator NOT invoked
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: &threshold}
	classifierConfidence = 0.1
	typingCalls = 0
	mu.Unlock()

	doneCh4 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh4) }
	msg4 := db.Message{ID: "m-type-4", ThreadID: "th-ambient-drop", Content: "Just random human chat between people", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg4)
	pool.Enqueue(msg4)
	<-doneCh4

	mu.Lock()
	if typingCalls != 0 {
		t.Errorf("Expected 0 typing calls for channel mode non-wake ambient chatter, got %d", typingCalls)
	}
	mu.Unlock()

	// 5. Ignored channel policy -> typing indicator NOT invoked
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "ignore"}
	typingCalls = 0
	mu.Unlock()

	doneCh5 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh5) }
	msg5 := db.Message{ID: "m-type-5", ThreadID: "th-ignored", Content: "Ignored message @Aerial", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg5)
	pool.Enqueue(msg5)
	<-doneCh5

	mu.Lock()
	if typingCalls != 0 {
		t.Errorf("Expected 0 typing calls for ignored channel policy, got %d", typingCalls)
	}
	mu.Unlock()

	// 6. HTTP client request (skipDiscord: true) -> typing indicator NOT invoked
	mu.Lock()
	currentPolicy = config.ChannelPolicy{Mode: "threads"}
	typingCalls = 0
	mu.Unlock()

	doneCh6 := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh6) }
	msg6 := db.Message{ID: "m-type-6", ThreadID: "th-http", AuthorID: "http-client", Content: "CLI request", CreatedAt: time.Now().UTC()}
	_ = db.InsertMessage(database, msg6)
	pool.Enqueue(msg6)
	<-doneCh6

	mu.Lock()
	if typingCalls != 0 {
		t.Errorf("Expected 0 typing calls for HTTP client request, got %d", typingCalls)
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
			return mockJSONResponse("", "Should not execute"), "", 0, nil
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

func TestQueueIgnoredChannelPolicy_NilCallback(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("", "Should not execute"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		ResolveChannelPolicy: func(threadID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "ignore"}
		},
		// OnMessageCompleted is intentionally nil (matching production default)
		OnMessageCompleted: nil,
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:        "msg-nil-cb-1",
		ThreadID:  "chan-ignored-nil-cb",
		Content:   "Testing nil callback safety",
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	// Poll database for completion
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, err := db.GetMessage(database, "msg-nil-cb-1")
		if err == nil && m != nil && m.Status == db.StatusCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	savedMsg, err := db.GetMessage(database, "msg-nil-cb-1")
	if err != nil || savedMsg == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if savedMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status COMPLETED, got %s", savedMsg.Status)
	}
}

func TestQueueThreadInheritsParentChannelPolicy(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	dg := &discordgo.Session{
		State: discordgo.NewState(),
	}
	_ = dg.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	// Parent channel #spam (ignored)
	chanSpam := &discordgo.Channel{
		ID:      "parent-spam-id",
		GuildID: "guild-1",
		Name:    "spam",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = dg.State.ChannelAdd(chanSpam)

	// Thread spawned inside #spam
	threadInSpam := &discordgo.Channel{
		ID:       "thread-in-spam-id",
		GuildID:  "guild-1",
		ParentID: "parent-spam-id",
		Name:     "Spam Discussion Thread",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	_ = dg.State.ChannelAdd(threadInSpam)

	var mu sync.Mutex
	runnerCalls := 0

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "Should not execute"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		ResolveChannelPolicy: func(threadID, channelName string) config.ChannelPolicy {
			if threadID == "parent-spam-id" || channelName == "spam" {
				return config.ChannelPolicy{Mode: "ignore"}
			}
			return config.ChannelPolicy{Mode: "threads"}
		},
	})
	pool.SetDiscordSession(dg)
	pool.Start()
	defer pool.Stop()

	doneCh := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
		close(doneCh)
	}

	msg := db.Message{
		ID:        "msg-thread-spam-1",
		ThreadID:  "thread-in-spam-id",
		Content:   "Hello in thread in spam",
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for message completion")
	}

	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 0 {
		t.Errorf("Expected 0 runner calls because thread parent #spam is ignored, got %d", runnerCalls)
	}

	savedMsg, err := db.GetMessage(database, "msg-thread-spam-1")
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

func TestQueueHTTPClient_NotDroppedByDefaultDenyIgnore(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	runnerCalls := 0

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "HTTP prompt execution response"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		ResolveChannelPolicy: func(threadID, channelName string) config.ChannelPolicy {
			// In default-deny mode, all unrecognized Discord channels resolve to ignore
			return config.ChannelPolicy{Mode: "ignore"}
		},
	})
	pool.Start()
	defer pool.Stop()

	doneCh := make(chan struct{})
	pool.cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
		close(doneCh)
	}

	msg := db.Message{
		ID:        "msg-http-prompt-1",
		ThreadID:  "synthetic-http-thread-uuid",
		AuthorID:  "http-client",
		Content:   "Explain Kubernetes architecture",
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for HTTP prompt message completion")
	}

	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for HTTP prompt client despite default-deny ignore mode, got %d", runnerCalls)
	}

	savedMsg, err := db.GetMessage(database, "msg-http-prompt-1")
	if err != nil || savedMsg == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if savedMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status COMPLETED, got %s", savedMsg.Status)
	}
	if savedMsg.ResponseText != "HTTP prompt execution response" {
		t.Errorf("Expected response text 'HTTP prompt execution response', got %q", savedMsg.ResponseText)
	}
}

func ptrFloat(f float64) *float64 {
	return &f
}

func TestProcessBurst_PureAmbient(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	if err := db.SaveSessionID(database, "chan-lounge", sessionID); err != nil {
		t.Fatalf("Failed to save session ID: %v", err)
	}
	_, err = session.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, sessionID+".pb"), []byte("mock-pb"), 0644)

	var mu sync.Mutex
	runnerCalls := 0
	deliveryCalls := 0
	typingCalls := 0

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		return `{"confidence": 0.25, "reason": "casual chit-chat"}`, nil
	}))

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "Should not run"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			mu.Lock()
			typingCalls++
			mu.Unlock()
			return func() {}
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	msg1 := db.Message{
		ID:         "msg-amb-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "Hello everyone",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	msg2 := db.Message{
		ID:         "msg-amb-2",
		ThreadID:   "chan-lounge",
		AuthorName: "Bob",
		Content:    "Nice weather today",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(5 * time.Second),
	}
	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)

	pool.processBurst([]db.Message{msg1, msg2})

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != 0 {
		t.Errorf("Expected 0 runner calls for pure ambient burst, got %d", runnerCalls)
	}
	if deliveryCalls != 0 {
		t.Errorf("Expected 0 delivery calls for pure ambient burst, got %d", deliveryCalls)
	}
	if typingCalls != 0 {
		t.Errorf("Expected 0 typing calls for pure ambient burst, got %d", typingCalls)
	}

	turnCount, err := db.GetSessionTurnCount(database, "chan-lounge")
	if err != nil {
		t.Fatalf("GetSessionTurnCount error: %v", err)
	}
	if turnCount != 0 {
		t.Errorf("Expected turn_count to remain 0, got %d", turnCount)
	}

	for _, id := range []string{"msg-amb-1", "msg-amb-2"} {
		saved, err := db.GetMessage(database, id)
		if err != nil || saved == nil {
			t.Fatalf("Failed to retrieve %s: %v", id, err)
		}
		if saved.Status != db.StatusCompleted {
			t.Errorf("Expected status COMPLETED for %s, got %s", id, saved.Status)
		}
		if !strings.Contains(saved.ErrorMessage, "[AMBIENT score=0.25/0.80 reason=\"casual chit-chat\"]") {
			t.Errorf("Expected telemetry in error_message for %s, got %q", id, saved.ErrorMessage)
		}
	}
}

func TestProcessBurst_Tier1Wake(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	_ = db.SaveSessionID(database, "chan-lounge", sessionID)
	_, _ = session.EnsureSessionDir(sessionID)

	var mu sync.Mutex
	runnerCalls := 0
	deliveryCalls := 0
	deliveredText := ""
	typingCalls := 0

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{ID: "bot-aerial-id", Username: "Aerial"}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		DiscordSession: s,
		TimeoutMinutes: 1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "I am Aerial, here to help!"), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			mu.Lock()
			typingCalls++
			mu.Unlock()
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	msg := db.Message{
		ID:         "msg-tier1-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "<@bot-aerial-id> can you help me with this bug?",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for Tier 1 wake, got %d", runnerCalls)
	}
	if typingCalls == 0 {
		t.Errorf("Expected typing indicator to be started for Tier 1 wake")
	}
	if deliveryCalls != 1 || deliveredText != "I am Aerial, here to help!" {
		t.Errorf("Expected 1 delivery with response, got calls=%d, text=%q", deliveryCalls, deliveredText)
	}

	turnCount, err := db.GetSessionTurnCount(database, "chan-lounge")
	if err != nil {
		t.Fatalf("GetSessionTurnCount error: %v", err)
	}
	if turnCount != 1 {
		t.Errorf("Expected turn_count to increment to 1, got %d", turnCount)
	}

	saved, err := db.GetMessage(database, "msg-tier1-1")
	if err != nil || saved == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if saved.Status != db.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", saved.Status)
	}
	if saved.ResponseText != "I am Aerial, here to help!" {
		t.Errorf("Expected response text, got %q", saved.ResponseText)
	}
}

func TestProcessBurst_Tier2Wake(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	_ = db.SaveSessionID(database, "chan-lounge", sessionID)
	_, _ = session.EnsureSessionDir(sessionID)

	var mu sync.Mutex
	runnerCalls := 0
	deliveryCalls := 0
	deliveredText := ""
	typingCalls := 0

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		return `{"confidence": 0.90, "reason": "user is asking for system health report"}`, nil
	}))

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "All systems operational."), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			mu.Lock()
			typingCalls++
			mu.Unlock()
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	// Unaddressed message: no mention, no keyword
	msg := db.Message{
		ID:         "msg-tier2-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "Does anyone know if all services are healthy?",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for Tier 2 wake, got %d", runnerCalls)
	}
	if typingCalls == 0 {
		t.Errorf("Expected typing indicator to be started for Tier 2 wake")
	}
	if deliveryCalls != 1 || deliveredText != "All systems operational." {
		t.Errorf("Expected 1 delivery, got calls=%d, text=%q", deliveryCalls, deliveredText)
	}

	turnCount, err := db.GetSessionTurnCount(database, "chan-lounge")
	if err != nil {
		t.Fatalf("GetSessionTurnCount error: %v", err)
	}
	if turnCount != 1 {
		t.Errorf("Expected turn_count to increment to 1, got %d", turnCount)
	}

	saved, err := db.GetMessage(database, "msg-tier2-1")
	if err != nil || saved == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if saved.Status != db.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", saved.Status)
	}
}

func TestProcessBurst_MixedBurst(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	_ = db.SaveSessionID(database, "chan-lounge", sessionID)
	_, _ = session.EnsureSessionDir(sessionID)
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, sessionID+".pb"), []byte("mock-pb"), 0644)

	var mu sync.Mutex
	runnerCalls := 0
	var receivedPrompt string

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		// msg1 is ambient banter
		return `{"confidence": 0.15, "reason": "unrelated lunch discussion"}`, nil
	}))

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			receivedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse("", "Done deploying!"), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	// Ambient1
	msg1 := db.Message{
		ID:         "msg-mixed-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "I had tacos for lunch today",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	// Wake2 (Tier 1 keyword wake)
	msg2 := db.Message{
		ID:         "msg-mixed-2",
		ThreadID:   "chan-lounge",
		AuthorName: "Bob",
		Content:    "Hey Aerial, please deploy the backend",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(2 * time.Second),
	}
	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)

	pool.processBurst([]db.Message{msg1, msg2})

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != 1 {
		t.Errorf("Expected exactly 1 runner call for mixed burst, got %d", runnerCalls)
	}
	if !strings.Contains(receivedPrompt, "<CHANNEL_HISTORY>") || !strings.Contains(receivedPrompt, "I had tacos for lunch today") {
		t.Errorf("Runner prompt should contain ambient message in CHANNEL_HISTORY, got: %s", receivedPrompt)
	}
	if !strings.Contains(receivedPrompt, "please deploy the backend") {
		t.Errorf("Runner prompt should contain Wake2, got: %s", receivedPrompt)
	}



	// Verify DB statuses
	m1Saved, _ := db.GetMessage(database, "msg-mixed-1")
	if m1Saved == nil || m1Saved.Status != db.StatusCompleted || !strings.Contains(m1Saved.ErrorMessage, "[AMBIENT score=") {
		t.Errorf("Expected msg1 to be COMPLETED with [AMBIENT score=...], got: %+v", m1Saved)
	}
	m2Saved, _ := db.GetMessage(database, "msg-mixed-2")
	if m2Saved == nil || m2Saved.Status != db.StatusCompleted || m2Saved.ResponseText != "Done deploying!" {
		t.Errorf("Expected msg2 to be COMPLETED with response text, got: %+v", m2Saved)
	}

	turnCount, _ := db.GetSessionTurnCount(database, "chan-lounge")
	if turnCount != 1 {
		t.Errorf("Expected turn_count = 1 after mixed burst, got %d", turnCount)
	}
}

func TestExtractMessageBody_Multiline(t *testing.T) {
	prompt := `<USER_REQUEST>
Here's a message someone sent you from Discord:

- id: 12345
- channel_id: chan-1
- thread_id: thread-1
- guild_id: guild-1
- author_id: user-1
- author_username: alice
- author_global_name: Alice
- author_bot: false
- is_admin: false
- content: First line of message
Second line of message
Third line of message
- timestamp: 2026-09-02T12:00:00Z
- mentions: []
- attachments: []

Please formulate your response and output it clearly.
</USER_REQUEST>`

	extracted := extractMessageBody(prompt)
	expected := "First line of message\nSecond line of message\nThird line of message"
	if extracted != expected {
		t.Errorf("Expected multiline extraction:\n%q\ngot:\n%q", expected, extracted)
	}
}

func TestIsTier1Wake_ReplyingToNonAerialWithAerialContent(t *testing.T) {
	// A message replying to @bob, where bob's quoted content mentions "aerial photo"
	// and the user's content is "Nice shot!"
	prompt := `<USER_REQUEST>
Here's a message someone sent you from Discord:

- id: msg-reply-1
- channel_id: chan-1
- thread_id: thread-1
- guild_id: guild-1
- author_id: user-1
- author_username: alice
- author_global_name: Alice
- author_bot: false
- is_admin: false
- replying_to:
    author: "@bob"
    content: "Look at this aerial photo of the bridge"
- content: Nice shot!
- timestamp: 2026-09-02T12:00:00Z
- mentions: []
- attachments: []

Please formulate your response and output it clearly.
</USER_REQUEST>`

	msg := db.Message{
		ID:        "msg-reply-1",
		ThreadID:  "chan-1",
		Content:   prompt,
		CreatedAt: time.Now().UTC(),
	}

	if isTier1Wake(msg, "bot-aerial-id", nil, "classifier") {
		t.Errorf("Expected isTier1Wake to be FALSE when replying to @bob whose content contains 'aerial photo'")
	}
}

func TestProcessBurst_SessionRotationBeforeLeadingAmbient(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-rot-ambient"
	initialSessionID := uuid.New().String()
	_ = db.SaveSessionID(database, channelID, initialSessionID)
	_, _ = session.EnsureSessionDir(initialSessionID)
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, initialSessionID+".pb"), []byte("mock-pb"), 0644)

	// Set turn_count to DefaultMaxSessionTurns - 1 (49 turns).
	// The incoming burst has [Ambient1, Wake2].
	// Since wakeIdx = 1 and currentTurns + 1 = 50 >= DefaultMaxSessionTurns,
	// pre-burst rotation resets session to cold state so Ambient1 is written to the NEW session directory.
	for i := 0; i < DefaultMaxSessionTurns-1; i++ {
		_, _ = db.IncrementSessionTurnCount(database, channelID)
	}

	var mu sync.Mutex
	runnerCalls := 0
	var passedSessID string

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		return `{"confidence": 0.10, "reason": "ambient banter"}`, nil
	}))

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			passedSessID = sessID
			mu.Unlock()
			return mockJSONResponse("c9b5e679-7425-40de-944b-e07fc1f90ae7", "I am answering your question!"), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	msg1 := db.Message{
		ID:         "msg-rot-amb-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Random ambient chatter before question",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	msg2 := db.Message{
		ID:         "msg-rot-wake-2",
		ThreadID:   channelID,
		AuthorName: "Bob",
		Content:    "Hey Aerial, what is 2+2?",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(2 * time.Second),
	}
	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)

	pool.processBurst([]db.Message{msg1, msg2})

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call, got %d", runnerCalls)
	}

	if passedSessID != "" {
		t.Errorf("Expected RunnerFunc to be called with empty session ID on rotated turn, got %q", passedSessID)
	}

	// Verify that Ambient1 was NOT written to the old session directory, but marked COMPLETED in DB!
	oldSessDir, _ := session.EnsureSessionDir(initialSessionID)
	oldTranscriptPath := filepath.Join(oldSessDir, ".system_generated", "logs", "transcript.jsonl")
	dataOld, _ := os.ReadFile(oldTranscriptPath)
	if strings.Contains(string(dataOld), "Random ambient chatter before question") {
		t.Errorf("Ambient message should NOT have been written to old session directory")
	}

	m1Saved, _ := db.GetMessage(database, "msg-rot-amb-1")
	if m1Saved == nil || m1Saved.Status != db.StatusCompleted || !strings.Contains(m1Saved.ErrorMessage, "[AMBIENT score=") {
		t.Errorf("Expected leading ambient message to be marked COMPLETED with [AMBIENT score=...], got: %+v", m1Saved)
	}
}

func TestProcessBurst_TrailingAmbient(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	_ = db.SaveSessionID(database, "chan-lounge", sessionID)
	_, _ = session.EnsureSessionDir(sessionID)
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, sessionID+".pb"), []byte("mock-pb"), 0644)

	var mu sync.Mutex
	runnerCalls := 0
	var receivedPrompt string

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		return `{"confidence": 0.10, "reason": "ambient banter"}`, nil
	}))

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			receivedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse(sessionID, "Aerial answer"), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	// Burst: [Wake1, Ambient2]
	msg1 := db.Message{
		ID:         "msg-trail-wake-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "Hey Aerial, explain gravity",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	msg2 := db.Message{
		ID:         "msg-trail-amb-2",
		ThreadID:   "chan-lounge",
		AuthorName: "Bob",
		Content:    "I love physics too",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(2 * time.Second),
	}
	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)

	pool.processBurst([]db.Message{msg1, msg2})

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for wake message, got %d", runnerCalls)
	}
	// Prompt should only contain Wake1, NOT Ambient2!
	if strings.Contains(receivedPrompt, "I love physics too") {
		t.Errorf("Runner prompt should NOT contain trailing ambient message, got: %s", receivedPrompt)
	}



	// Ambient2 in DB should have [AMBIENT score=...]
	m2Saved, _ := db.GetMessage(database, "msg-trail-amb-2")
	if m2Saved == nil || m2Saved.Status != db.StatusCompleted || !strings.Contains(m2Saved.ErrorMessage, "[AMBIENT score=") {
		t.Errorf("Expected trailing ambient msg to be COMPLETED with [AMBIENT score=...], got: %+v", m2Saved)
	}
}

func TestProcessBurst_CustomAmbientWakePrompt(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var capturedPrompt string
	var mu sync.Mutex

	customPrompt := "Wake up only when aerospace or aviation topics are discussed."

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		mu.Lock()
		capturedPrompt = prompt
		mu.Unlock()
		return `{"confidence": 0.95, "reason": "matches aviation directive"}`, nil
	}))

	runnerCalls := 0
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "Aviation response"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
				AmbientWakePrompt:    customPrompt,
			}
		},
	})

	now := time.Now().UTC()
	msg := db.Message{
		ID:         "msg-aviation-1",
		ThreadID:   "chan-aviation",
		AuthorName: "Pilot",
		Content:    "What is the stall speed of a Cessna 172?",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()

	if !strings.Contains(capturedPrompt, customPrompt) {
		t.Errorf("Expected classifier prompt to contain custom prompt %q, got:\n%s", customPrompt, capturedPrompt)
	}
	if strings.Contains(capturedPrompt, classifier.DefaultAmbientWakePrompt) {
		t.Errorf("Expected classifier prompt NOT to contain DefaultAmbientWakePrompt when custom directive is provided")
	}
	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call, got %d", runnerCalls)
	}
}

func TestResolveEffectiveChannel_NilSession(t *testing.T) {
	// Empty channel ID
	effID, effName, isThread := ResolveEffectiveChannel(nil, "")
	if effID != "" || effName != "" || isThread {
		t.Errorf("Expected empty channel ID to return \"\", \"\", false; got %q, %q, %v", effID, effName, isThread)
	}

	// Non-numeric ID with nil session
	effID, effName, isThread = ResolveEffectiveChannel(nil, "uuid-test-123")
	if effID != "uuid-test-123" || effName != "" || isThread {
		t.Errorf("Expected non-numeric ID with nil session to return input ID, \"\", false; got %q, %q, %v", effID, effName, isThread)
	}

	// Numeric snowflake with nil session
	effID, effName, isThread = ResolveEffectiveChannel(nil, "123456789012345678")
	if effID != "123456789012345678" || effName != "" || isThread {
		t.Errorf("Expected snowflake with nil session to return input ID, \"\", false without panic; got %q, %q, %v", effID, effName, isThread)
	}
}

func TestResolveEffectiveChannel_SyntheticNonNumericID(t *testing.T) {
	s := &discordgo.Session{
		Token: "fake-token",
		State: discordgo.NewState(),
	}

	syntheticIDs := []string{
		"http-client-session-1234",
		"abc-def-ghi",
		"1234-5678",
		"channel_name_test",
	}

	for _, id := range syntheticIDs {
		effID, effName, isThread := ResolveEffectiveChannel(s, id)
		if effID != id || effName != "" || isThread {
			t.Errorf("Synthetic ID %q: expected %q, \"\", false; got %q, %q, %v", id, id, effID, effName, isThread)
		}
	}
}

func TestResolveEffectiveChannel_NormalChannel(t *testing.T) {
	channelID := "100200300400500601"
	channelName := "general-chat"

	CacheDiscordChannel(&discordgo.Channel{
		ID:   channelID,
		Name: channelName,
		Type: discordgo.ChannelTypeGuildText,
	})
	defer InvalidateChannelCache(channelID)

	s := &discordgo.Session{}
	effID, effName, isThread := ResolveEffectiveChannel(s, channelID)
	if effID != channelID || effName != channelName || isThread {
		t.Errorf("Expected %q, %q, false; got %q, %q, %v", channelID, channelName, effID, effName, isThread)
	}
}

func TestResolveEffectiveChannel_ThreadParent(t *testing.T) {
	parentID := "100200300400500600"
	parentName := "announcements"
	threadID := "100200300400500602"
	threadName := "v1.2-discussion"

	CacheDiscordChannel(&discordgo.Channel{
		ID:   parentID,
		Name: parentName,
		Type: discordgo.ChannelTypeGuildText,
	})
	defer InvalidateChannelCache(parentID)

	CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID,
		Name:     threadName,
		ParentID: parentID,
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	defer InvalidateChannelCache(threadID)

	s := &discordgo.Session{}
	effID, effName, isThread := ResolveEffectiveChannel(s, threadID)
	if effID != parentID {
		t.Errorf("Expected effectiveID = %q (parent ID), got %q", parentID, effID)
	}
	if effName != parentName {
		t.Errorf("Expected effectiveName = %q (parent name), got %q", parentName, effName)
	}
	if !isThread {
		t.Errorf("Expected isThread = true, got %v", isThread)
	}
}

func TestResolveEffectiveChannel_ThreadMissingParent(t *testing.T) {
	parentID := "999888777666555444"
	threadID := "100200300400500603"
	threadName := "isolated-thread"

	CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID,
		Name:     threadName,
		ParentID: parentID,
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	defer InvalidateChannelCache(threadID)

	s := &discordgo.Session{}
	effID, effName, isThread := ResolveEffectiveChannel(s, threadID)
	if effID != parentID {
		t.Errorf("Expected parentID %q, got %q", parentID, effID)
	}
	if effName != "" {
		t.Errorf("Expected empty parent name when unresolvable, got %q", effName)
	}
	if !isThread {
		t.Errorf("Expected isThread = true, got %v", isThread)
	}
}

func TestResolveEffectiveChannel_InvalidateCache(t *testing.T) {
	channelID := "100200300400500604"
	ch := &discordgo.Channel{
		ID:   channelID,
		Name: "temporary-channel",
		Type: discordgo.ChannelTypeGuildText,
	}

	CacheDiscordChannel(ch)
	snap, ok := GetCachedChannel(channelID)
	if !ok || snap.Name != "temporary-channel" {
		t.Fatalf("Expected channel to be in cache")
	}

	InvalidateChannelCache(channelID)
	_, ok = GetCachedChannel(channelID)
	if ok {
		t.Errorf("Expected channel %q to be removed from cache after InvalidateChannelCache", channelID)
	}
}

func TestResolveEffectiveChannel_StateFallback(t *testing.T) {
	channelID := "100200300400500605"
	InvalidateChannelCache(channelID)
	defer InvalidateChannelCache(channelID)

	state := discordgo.NewState()
	_ = state.GuildAdd(&discordgo.Guild{ID: "guild-1"})
	_ = state.ChannelAdd(&discordgo.Channel{
		ID:      channelID,
		GuildID: "guild-1",
		Name:    "state-channel",
		Type:    discordgo.ChannelTypeGuildText,
	})

	s := &discordgo.Session{
		State: state,
	}

	effID, effName, isThread := ResolveEffectiveChannel(s, channelID)
	if effID != channelID || effName != "state-channel" || isThread {
		t.Errorf("Expected %q, \"state-channel\", false; got %q, %q, %v", channelID, effID, effName, isThread)
	}

	// Verify it was populated into cache
	snap, ok := GetCachedChannel(channelID)
	if !ok || snap.Name != "state-channel" {
		t.Errorf("Expected channel to be cached from State")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestResolveEffectiveChannel_SingleFlightREST(t *testing.T) {
	channelID := "100200300400500606"
	InvalidateChannelCache(channelID)
	defer InvalidateChannelCache(channelID)

	var reqCount int32
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(&reqCount, 1)
			time.Sleep(20 * time.Millisecond)
			ch := &discordgo.Channel{
				ID:   channelID,
				Name: "singleflight-rest-channel",
				Type: discordgo.ChannelTypeGuildText,
			}
			data, _ := json.Marshal(ch)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(data)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	s, _ := discordgo.New("Bot fake-token")
	s.Client = client

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			effID, effName, isThread := ResolveEffectiveChannel(s, channelID)
			if effID != channelID || effName != "singleflight-rest-channel" || isThread {
				t.Errorf("Unexpected result: %q, %q, %v", effID, effName, isThread)
			}
		}()
	}
	wg.Wait()

	if count := atomic.LoadInt32(&reqCount); count != 1 {
		t.Errorf("Expected exactly 1 REST request due to singleflight deduplication, got %d", count)
	}

	// Verify cached
	snap, ok := GetCachedChannel(channelID)
	if !ok || snap.Name != "singleflight-rest-channel" {
		t.Errorf("Expected channel to be cached after REST fetch")
	}
}

func TestProcessBurst_ChannelInstructionsInjection(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	if err := os.MkdirAll(channelsDir, 0755); err != nil {
		t.Fatalf("Failed to create channels dir: %v", err)
	}
	devInstructions := "Follow Go idioms and test thoroughly."
	if err := os.WriteFile(filepath.Join(channelsDir, "dev.md"), []byte(devInstructions), 0644); err != nil {
		t.Fatalf("Failed to write dev.md: %v", err)
	}

	oldDirs := config.ChannelInstructionsDirs
	config.ChannelInstructionsDirs = []string{channelsDir}
	defer func() { config.ChannelInstructionsDirs = oldDirs }()

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "100200300400500701"
	CacheDiscordChannel(&discordgo.Channel{
		ID:   channelID,
		Name: "dev",
		Type: discordgo.ChannelTypeGuildText,
	})
	defer InvalidateChannelCache(channelID)

	var capturedPrompt string
	var mu sync.Mutex

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			capturedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse("", "Success"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
	})

	msg := db.Message{
		ID:        "msg-inject-1",
		ThreadID:  channelID,
		Content:   "Build the queue worker",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()

	expectedHeader := "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\n" + devInstructions + "\n</CHANNEL_INSTRUCTIONS>"
	if !strings.HasPrefix(capturedPrompt, expectedHeader) {
		t.Fatalf("Expected prompt to start with channel instructions block, got:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, "Build the queue worker") {
		t.Fatalf("Expected prompt to contain base prompt content, got:\n%s", capturedPrompt)
	}
}

func TestProcessBurst_ChannelInstructionsThreadInheritance(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	if err := os.MkdirAll(channelsDir, 0755); err != nil {
		t.Fatalf("Failed to create channels dir: %v", err)
	}
	devInstructions := "Guidelines for dev channel discussions."
	if err := os.WriteFile(filepath.Join(channelsDir, "dev.md"), []byte(devInstructions), 0644); err != nil {
		t.Fatalf("Failed to write dev.md: %v", err)
	}

	oldDirs := config.ChannelInstructionsDirs
	config.ChannelInstructionsDirs = []string{channelsDir}
	defer func() { config.ChannelInstructionsDirs = oldDirs }()

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	parentID := "100200300400500702"
	threadID := "100200300400500703"

	CacheDiscordChannel(&discordgo.Channel{
		ID:   parentID,
		Name: "dev",
		Type: discordgo.ChannelTypeGuildText,
	})
	CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID,
		Name:     "feature-thread",
		ParentID: parentID,
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	defer InvalidateChannelCache(parentID)
	defer InvalidateChannelCache(threadID)

	var capturedPrompt string
	var mu sync.Mutex

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			capturedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse("", "Success"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
	})

	msg := db.Message{
		ID:        "msg-thread-inherit-1",
		ThreadID:  threadID,
		Content:   "Thread discussion prompt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()

	expectedHeader := "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\n" + devInstructions + "\n</CHANNEL_INSTRUCTIONS>"
	if !strings.HasPrefix(capturedPrompt, expectedHeader) {
		t.Fatalf("Expected thread prompt to inherit channel instructions from parent #dev, got:\n%s", capturedPrompt)
	}
}

func TestProcessBurst_NoDuplicationOnRetry(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	if err := os.MkdirAll(channelsDir, 0755); err != nil {
		t.Fatalf("Failed to create channels dir: %v", err)
	}
	devInstructions := "Channel instructions for retry test."
	if err := os.WriteFile(filepath.Join(channelsDir, "dev.md"), []byte(devInstructions), 0644); err != nil {
		t.Fatalf("Failed to write dev.md: %v", err)
	}

	oldDirs := config.ChannelInstructionsDirs
	config.ChannelInstructionsDirs = []string{channelsDir}
	defer func() { config.ChannelInstructionsDirs = oldDirs }()

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "100200300400500704"
	CacheDiscordChannel(&discordgo.Channel{
		ID:   channelID,
		Name: "dev",
		Type: discordgo.ChannelTypeGuildText,
	})
	defer InvalidateChannelCache(channelID)

	var attemptCount int
	var capturedPrompts []string
	var mu sync.Mutex

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			attemptCount++
			capturedPrompts = append(capturedPrompts, prompt)
			currentAttempt := attemptCount
			mu.Unlock()

			if currentAttempt == 1 {
				// Simulate transient failure on attempt 1
				return "", "Error 503: high demand unavailable", 1, nil
			}
			return mockJSONResponse("session-retry-123", "Success on retry"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
	})

	msg := db.Message{
		ID:        "msg-retry-1",
		ThreadID:  channelID,
		Content:   "Retry test prompt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()

	if attemptCount != 2 {
		t.Fatalf("Expected 2 attempts, got %d", attemptCount)
	}
	if len(capturedPrompts) != 2 {
		t.Fatalf("Expected 2 captured prompts, got %d", len(capturedPrompts))
	}

	for i, p := range capturedPrompts {
		count := strings.Count(p, "<CHANNEL_INSTRUCTIONS>")
		if count != 1 {
			t.Errorf("Attempt %d: expected exactly 1 <CHANNEL_INSTRUCTIONS> block, got %d. Prompt:\n%s", i+1, count, p)
		}
		closingCount := strings.Count(p, "</CHANNEL_INSTRUCTIONS>")
		if closingCount != 1 {
			t.Errorf("Attempt %d: expected exactly 1 </CHANNEL_INSTRUCTIONS> closing tag, got %d", i+1, closingCount)
		}
	}
}

func TestProcessBurst_Tier1PreScan_SkipsClassifier(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer func() { _ = database.Close() }()

	var classifierCalls int
	cls := classifier.NewClassifier(
		classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			classifierCalls++
			return `{"confidence": 0.0, "reason": "should not be called"}`, nil
		}),
	)

	var runnerCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:         database,
		Classifier: cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			runnerCalls++
			return mockJSONResponse("", "Hello there!"), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	// Burst: [Ambient1, Tier1Wake, Ambient3]
	m1 := db.Message{
		ID:         "msg-prescan-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "just hanging out",
		CreatedAt:  now,
	}
	m2 := db.Message{
		ID:         "msg-prescan-2",
		ThreadID:   "chan-lounge",
		AuthorName: "Bob",
		Content:    "@Aerial what's up?",
		CreatedAt:  now.Add(1 * time.Second),
	}
	m3 := db.Message{
		ID:         "msg-prescan-3",
		ThreadID:   "chan-lounge",
		AuthorName: "Charlie",
		Content:    "lol",
		CreatedAt:  now.Add(2 * time.Second),
	}
	_ = db.InsertMessage(database, m1)
	_ = db.InsertMessage(database, m2)
	_ = db.InsertMessage(database, m3)

	pool.processBurst([]db.Message{m1, m2, m3})

	if classifierCalls != 0 {
		t.Errorf("Expected 0 classifier calls due to Tier-1 pre-scan fast path, got %d", classifierCalls)
	}
	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for Tier-1 wake message, got %d", runnerCalls)
	}
}

func TestProcessBurst_CoalescedAmbientBurst(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer func() { _ = database.Close() }()

	var classifierCalls int
	var capturedPrompt string
	cls := classifier.NewClassifier(
		classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			classifierCalls++
			capturedPrompt = prompt
			return `{"confidence": 0.95, "reason": "urgent issue needing answer"}`, nil
		}),
	)

	var runnerCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:         database,
		Classifier: cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			runnerCalls++
			return mockJSONResponse("", "I can help with that database error!"), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	m1 := db.Message{
		ID:         "msg-coalesce-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "Does anyone know why postgres crashed?",
		CreatedAt:  now,
	}
	m2 := db.Message{
		ID:         "msg-coalesce-2",
		ThreadID:   "chan-lounge",
		AuthorName: "Bob",
		Content:    "brb grabbing coffee",
		CreatedAt:  now.Add(1 * time.Second),
	}
	_ = db.InsertMessage(database, m1)
	_ = db.InsertMessage(database, m2)

	pool.processBurst([]db.Message{m1, m2})

	if classifierCalls != 1 {
		t.Errorf("Expected exactly 1 coalesced classifier call for burst of 2 ambient messages, got %d", classifierCalls)
	}
	if !strings.Contains(capturedPrompt, "<target_burst>") {
		t.Errorf("Expected prompt to contain <target_burst>, got:\n%s", capturedPrompt)
	}
	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call for woken burst, got %d", runnerCalls)
	}
}

func TestProcessBurst_GhostSessionRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-lounge"
	_ = db.SaveSessionID(database, channelID, "stale-ghost-uuid")

	// Create mock session dir on disk with non-empty transcript.jsonl for "a8b5e679-7425-40de-944b-e07fc1f90ae7"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", "a8b5e679-7425-40de-944b-e07fc1f90ae7")
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	var mu sync.Mutex
	runnerCalls := 0
	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			stderr := "warning: conversation \"stale-ghost-uuid\" not found\nStarting conversation update stream for a8b5e679-7425-40de-944b-e07fc1f90ae7\n"
			return mockJSONResponse("a8b5e679-7425-40de-944b-e07fc1f90ae7", "Hello! I am ready to help."), stderr, 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode: "channel",
			}
		},
	})

	now := time.Now().UTC()
	msg := db.Message{
		ID:         "msg-ghost-recovery-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Hey Aerial, are you awake?",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 1 {
		t.Fatalf("Expected 1 runner call, got %d", calls)
	}

	savedSessionID, err := db.GetSessionID(database, channelID)
	if err != nil {
		t.Fatalf("Failed to get session ID: %v", err)
	}
	if savedSessionID != "a8b5e679-7425-40de-944b-e07fc1f90ae7" {
		t.Errorf("Expected db.GetSessionID to equal 'a8b5e679-7425-40de-944b-e07fc1f90ae7', got %q", savedSessionID)
	}
}

func TestProcessBurst_MultiTurnContinuity_AfterRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-lounge"
	_ = db.SaveSessionID(database, channelID, "stale-ghost-uuid")

	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", "a8b5e679-7425-40de-944b-e07fc1f90ae7")
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	var mu sync.Mutex
	var receivedSessionIDs []string

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			receivedSessionIDs = append(receivedSessionIDs, sessionID)
			mu.Unlock()
			stderr := "warning: conversation \"stale-ghost-uuid\" not found\nStarting conversation update stream for a8b5e679-7425-40de-944b-e07fc1f90ae7\n"
			return mockJSONResponse("a8b5e679-7425-40de-944b-e07fc1f90ae7", "Clean reply"), stderr, 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode: "channel",
			}
		},
	})

	now := time.Now().UTC()
	// Turn 1 (triggers recovery)
	msg1 := db.Message{
		ID:         "msg-recov-turn-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Hello Aerial",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg1)
	pool.processBurst([]db.Message{msg1})

	// Turn 2 (multi-turn follow-up)
	msg2 := db.Message{
		ID:         "msg-recov-turn-2",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Hey Aerial, what did I just say?",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(1 * time.Second),
	}
	_ = db.InsertMessage(database, msg2)
	pool.processBurst([]db.Message{msg2})

	mu.Lock()
	count := len(receivedSessionIDs)
	mu.Unlock()

	if count != 2 {
		t.Fatalf("Expected 2 runner invocations across turns, got %d", count)
	}

	if receivedSessionIDs[0] != "" {
		t.Errorf("Expected Turn 1 (cold start) to receive sessionID == '', got %q", receivedSessionIDs[0])
	}
	if receivedSessionIDs[1] != "a8b5e679-7425-40de-944b-e07fc1f90ae7" {
		t.Errorf("Expected Turn 2 to receive sessionID == 'a8b5e679-7425-40de-944b-e07fc1f90ae7', got %q", receivedSessionIDs[1])
	}
}

func TestProcessBurst_ColdChannel_NoStubDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-cold-123"

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		return `{"confidence": 0.05, "reason": "ambient chatter"}`, nil
	}))

	var runnerCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:         database,
		Classifier: cls,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			runnerCalls++
			return mockJSONResponse("", ""), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})

	now := time.Now().UTC()
	msg := db.Message{
		ID:         "msg-cold-ambient-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "just saying hello to everyone in the room",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	if runnerCalls != 0 {
		t.Errorf("Expected 0 runner calls for pure ambient burst, got %d", runnerCalls)
	}

	sessID, err := db.GetSessionID(database, channelID)
	if err != nil {
		t.Fatalf("GetSessionID failed: %v", err)
	}
	if sessID != "" {
		t.Errorf("Expected db.GetSessionID to be empty \"\", got %q", sessID)
	}

	// Assert no directory was created for "chan-cold-123" in /data/brain or home brain roots
	roots := []string{
		"/data/brain/" + channelID,
		filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", channelID),
		filepath.Join(tmpDir, ".gemini", "antigravity", "brain", channelID),
	}
	for _, root := range roots {
		if fi, err := os.Stat(root); err == nil {
			t.Errorf("Found unexpected directory created for cold channel at %s (isDir=%t)", root, fi.IsDir())
		}
	}
}

func TestProcessBurst_Turn1ContextInjection(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-history"

	// Mock session dir for Turn 1 synchronized session
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f")
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	var mu sync.Mutex
	var capturedPrompts []string
	var receivedSessionIDs []string

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		HistoryFetcher: func(ctx context.Context, cID string, beforeID string, limit int) ([]HistoryMessage, error) {
			return []HistoryMessage{
				{
					ID:         "hist-1",
					AuthorName: "Alice",
					Role:       "User",
					Content:    "First historical message",
					CreatedAt:  time.Now().UTC().Add(-15 * time.Minute),
				},
				{
					ID:         "hist-2",
					AuthorName: "Bob",
					Role:       "User",
					Content:    "Second historical message",
					CreatedAt:  time.Now().UTC().Add(-10 * time.Minute),
				},
			}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			capturedPrompts = append(capturedPrompts, prompt)
			receivedSessionIDs = append(receivedSessionIDs, sessID)
			mu.Unlock()
			stderr := "Starting conversation update stream for e5b7fb78-d85d-4bd6-ba9e-330f1e30596f\n"
			return mockJSONResponse("e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", "Answering wake question!"), stderr, 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode: "channel",
			}
		},
	})

	now := time.Now().UTC()
	// Turn 1 on cold channel
	msg1 := db.Message{
		ID:         "msg-hist-turn-1",
		ThreadID:   channelID,
		AuthorName: "Charlie",
		Content:    "Hey Aerial, turn 1 wake question",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg1)
	pool.processBurst([]db.Message{msg1})

	// Turn 2 on now-warm channel
	msg2 := db.Message{
		ID:         "msg-hist-turn-2",
		ThreadID:   channelID,
		AuthorName: "Charlie",
		Content:    "Hey Aerial, turn 2 follow-up question",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(5 * time.Second),
	}
	_ = db.InsertMessage(database, msg2)
	pool.processBurst([]db.Message{msg2})

	mu.Lock()
	defer mu.Unlock()

	if len(capturedPrompts) != 2 {
		t.Fatalf("Expected 2 runner prompts, got %d", len(capturedPrompts))
	}

	// Turn 1 assertions
	turn1Prompt := capturedPrompts[0]
	if !strings.Contains(turn1Prompt, "<CHANNEL_HISTORY>") {
		t.Errorf("Expected Turn 1 prompt to contain <CHANNEL_HISTORY>, got:\n%s", turn1Prompt)
	}
	if !strings.Contains(turn1Prompt, "First historical message") || !strings.Contains(turn1Prompt, "Second historical message") {
		t.Errorf("Expected Turn 1 prompt to contain formatted historical messages, got:\n%s", turn1Prompt)
	}
	if receivedSessionIDs[0] != "" {
		t.Errorf("Expected Turn 1 to have empty sessionID, got %q", receivedSessionIDs[0])
	}

	// Turn 2 assertions
	turn2Prompt := capturedPrompts[1]
	if strings.Contains(turn2Prompt, "<CHANNEL_HISTORY>") {
		t.Errorf("Expected Turn 2 prompt to NOT contain <CHANNEL_HISTORY>, got:\n%s", turn2Prompt)
	}
	if receivedSessionIDs[1] != "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f" {
		t.Errorf("Expected Turn 2 to have sessionID == 'e5b7fb78-d85d-4bd6-ba9e-330f1e30596f', got %q", receivedSessionIDs[1])
	}
}

func TestProcessBurst_SessionRotation_ResetsToColdState(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-rotation-cold"

	// Mock session dir for Turn 1
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", "sess-rot-1")
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
	past := time.Now().Add(-10 * time.Second)
	_ = os.Chtimes(sessDir, past, past)

	var mu sync.Mutex
	var capturedPrompts []string
	var receivedSessionIDs []string
	historyFetchCalls := 0

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		HistoryFetcher: func(ctx context.Context, cID string, beforeID string, limit int) ([]HistoryMessage, error) {
			mu.Lock()
			historyFetchCalls++
			mu.Unlock()
			return []HistoryMessage{
				{
					ID:         "hist-boot-1",
					AuthorName: "Alice",
					Role:       "User",
					Content:    "Historical message before bootstrap",
					CreatedAt:  time.Now().UTC().Add(-5 * time.Minute),
				},
			}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			capturedPrompts = append(capturedPrompts, prompt)
			receivedSessionIDs = append(receivedSessionIDs, sessID)
			callNum := len(receivedSessionIDs)
			mu.Unlock()

			if callNum == 1 {
				// Turn 1 mints genuine session
				stderr := "Starting conversation update stream for c9b5e679-7425-40de-944b-e07fc1f90ae7\n"
				return mockJSONResponse("c9b5e679-7425-40de-944b-e07fc1f90ae7", "Turn 1 answer"), stderr, 0, nil
			}
			return mockJSONResponse("c9b5e679-7425-40de-944b-e07fc1f90ae7", "Subsequent turn answer"), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode: "channel",
			}
		},
	})

	now := time.Now().UTC()

	// 1. Process Turn 1 -> turnCount becomes 1
	msg1 := db.Message{
		ID:         "msg-turn-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Aerial Turn 1",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg1)
	pool.processBurst([]db.Message{msg1})

	tc1, err := db.GetSessionTurnCount(database, channelID)
	if err != nil {
		t.Fatalf("Failed to get turn count after turn 1: %v", err)
	}
	if tc1 != 1 {
		t.Errorf("Expected turnCount to become 1 after Turn 1, got %d", tc1)
	}
	s1, _ := db.GetSessionID(database, channelID)
	if s1 != "c9b5e679-7425-40de-944b-e07fc1f90ae7" {
		t.Errorf("Expected session ID 'c9b5e679-7425-40de-944b-e07fc1f90ae7' after Turn 1, got %q", s1)
	}

	// Seed turn_count to DefaultMaxSessionTurns - 1 (49) so next turn hits 50 and triggers rotation
	for i := 1; i < DefaultMaxSessionTurns-1; i++ {
		_, _ = db.IncrementSessionTurnCount(database, channelID)
	}

	// 2. Process Turn 50 -> turnCount hits DefaultMaxSessionTurns (50), rotates session_id to ""
	msg2 := db.Message{
		ID:         "msg-turn-2",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Aerial Turn 50",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(2 * time.Second),
	}
	_ = db.InsertMessage(database, msg2)
	pool.processBurst([]db.Message{msg2})

	s2, err := db.GetSessionID(database, channelID)
	if err != nil {
		t.Fatalf("Failed to get session ID after turn 2: %v", err)
	}
	if s2 != "" {
		t.Errorf("Expected db.GetSessionID to be empty \"\" after Turn 2 rotation, got %q", s2)
	}

	// 3. Process Turn 3 -> executes as Turn 1 cold bootstrap (fetches history, passes sessionID = "")
	msg3 := db.Message{
		ID:         "msg-turn-3",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Aerial Turn 3",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(4 * time.Second),
	}
	_ = db.InsertMessage(database, msg3)
	pool.processBurst([]db.Message{msg3})

	mu.Lock()
	defer mu.Unlock()

	if len(receivedSessionIDs) != 3 {
		t.Fatalf("Expected 3 runner calls, got %d", len(receivedSessionIDs))
	}
	if receivedSessionIDs[2] != "" {
		t.Errorf("Expected Turn 3 runner call to receive empty sessionID = \"\", got %q", receivedSessionIDs[2])
	}
	if !strings.Contains(capturedPrompts[2], "<CHANNEL_HISTORY>") {
		t.Errorf("Expected Turn 3 to execute as cold bootstrap and contain <CHANNEL_HISTORY>, got:\n%s", capturedPrompts[2])
	}
	if !strings.Contains(capturedPrompts[2], "Historical message before bootstrap") {
		t.Errorf("Expected Turn 3 prompt to contain fetched history message, got:\n%s", capturedPrompts[2])
	}
	if historyFetchCalls < 2 {
		t.Errorf("Expected HistoryFetcher to be called at least twice (Turn 1 and Turn 3), got %d", historyFetchCalls)
	}
}

func TestProcessBurst_Turn1Crash_DoesNotPersistGhostUUID(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	channelID := "chan-crash"

	var runnerCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:          database,
		MaxAttempts: 1,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			runnerCalls++
			stderr := "Starting conversation update stream for crash-uuid-999\nError: fatal process crash\n"
			return "", stderr, 1, fmt.Errorf("exit status 1")
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode: "channel",
			}
		},
	})

	now := time.Now().UTC()
	msg := db.Message{
		ID:         "msg-crash-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Hey Aerial, please do something risky",
		Status:     db.StatusPending,
		CreatedAt:  now,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	if runnerCalls != 1 {
		t.Errorf("Expected 1 runner call, got %d", runnerCalls)
	}

	sessID, err := db.GetSessionID(database, channelID)
	if err != nil {
		t.Fatalf("GetSessionID failed: %v", err)
	}
	if sessID == "crash-uuid-999" {
		t.Errorf("Expected db.GetSessionID to NOT equal 'crash-uuid-999', but got 'crash-uuid-999'")
	}
	if sessID != "" {
		t.Errorf("Expected db.GetSessionID to be empty \"\", got %q", sessID)
	}
}

func TestQueue_ClassifierParseErrorTriggersSystemAlert(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	var alertChannel string
	var alertTitle string
	var alertBody string
	alertReceived := make(chan struct{}, 1)

	cls := classifier.NewClassifier(
		classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			return "```json\n{\"confidence\": 0.85}\n```", nil // markdown fence triggers strict parse error
		}),
	)

	// Mock Discord Session with state
	dgSession := &discordgo.Session{
		State: &discordgo.State{
			Ready: discordgo.Ready{
				User: &discordgo.User{ID: "bot-999"},
			},
		},
	}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		DiscordSession: dgSession,
		TimeoutMinutes: 1,
		Classifier:     cls,
		SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, body string) error {
			mu.Lock()
			alertChannel = channelNameOrID
			alertTitle = title
			alertBody = body
			mu.Unlock()
			select {
			case alertReceived <- struct{}{}:
			default:
			}
			return nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.80),
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:         "msg-parse-err-1",
		ThreadID:   "chan-lounge",
		AuthorName: "Alice",
		Content:    "Hello is anybody there?",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	select {
	case <-alertReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for classifier parse error alert")
	}

	mu.Lock()
	defer mu.Unlock()

	if alertTitle != "Classifier JSON Parse Error" {
		t.Errorf("expected alert title 'Classifier JSON Parse Error', got %q", alertTitle)
	}
	if !strings.Contains(alertBody, "Parse Error") || !strings.Contains(alertBody, "Raw Output") {
		t.Errorf("expected alert body to contain parse error and raw output, got %q", alertBody)
	}
	if alertChannel != config.GetSystemChannel() {
		t.Errorf("expected alert channel %q, got %q", config.GetSystemChannel(), alertChannel)
	}
}

func TestIsTier1Wake_WakeModeMention(t *testing.T) {
	botID := "bot-12345"

	// 1. Direct mention <@bot-12345>
	msgMention := db.Message{Content: "Hey <@" + botID + "> how are you?"}
	if !isTier1Wake(msgMention, botID, nil, "mention") {
		t.Errorf("expected isTier1Wake to be TRUE for direct mention in mention mode")
	}

	// 2. Direct reply to Aerial in envelope
	msgReply := db.Message{Content: `<USER_REQUEST>
- replying_to:
    author: "@aerial"
    content: "previous response"
- content: What was that?
</USER_REQUEST>`}
	if !isTier1Wake(msgReply, botID, nil, "mention") {
		t.Errorf("expected isTier1Wake to be TRUE for reply to Aerial in mention mode")
	}

	// 3. Mentions envelope containing Aerial
	msgEnvelopeMention := db.Message{Content: `<USER_REQUEST>
- mentions: [Aerial]
- content: Hello there!
</USER_REQUEST>`}
	if !isTier1Wake(msgEnvelopeMention, botID, nil, "mention") {
		t.Errorf("expected isTier1Wake to be TRUE for mentions envelope in mention mode")
	}

	// 4. Plaintext name drop "aerial is great"
	msgBareKeyword := db.Message{Content: "I think aerial is a great anime"}
	if isTier1Wake(msgBareKeyword, botID, nil, "mention") {
		t.Errorf("expected isTier1Wake to be FALSE for bare keyword in mention mode")
	}
	if !isTier1Wake(msgBareKeyword, botID, nil, "classifier") {
		t.Errorf("expected isTier1Wake to be TRUE for bare keyword in classifier mode")
	}

	// 5. Plaintext keyword "gundam"
	msgGundam := db.Message{Content: "I love gundam models"}
	if isTier1Wake(msgGundam, botID, nil, "mention") {
		t.Errorf("expected isTier1Wake to be FALSE for 'gundam' in mention mode")
	}

	// 6. Direct role mention <@&role-123> with botRoleIDs matching
	msgRoleMention := db.Message{Content: "Hey <@&role-aerial-managed> check this out"}
	if !isTier1Wake(msgRoleMention, botID, []string{"role-aerial-managed"}, "mention") {
		t.Errorf("expected isTier1Wake to be TRUE for bot role mention in mention mode")
	}
	if isTier1Wake(msgRoleMention, botID, []string{"role-unrelated"}, "mention") {
		t.Errorf("expected isTier1Wake to be FALSE for unrelated role mention in mention mode")
	}
}

func TestProcessBurst_WakeModeMention_BypassClassifier(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	channelID := "chan-lounge-mention"
	_ = db.SaveSessionID(database, channelID, sessionID)
	_, _ = session.EnsureSessionDir(sessionID)
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, sessionID+".pb"), []byte("mock-pb"), 0644)

	var classifierCalls int
	var runnerCalls int
	var deliveredMsgs []string
	var mu sync.Mutex

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return `{"conversation_id": "` + sID + `", "response": "Response!"}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, chID, text string) error {
			mu.Lock()
			deliveredMsgs = append(deliveredMsgs, text)
			mu.Unlock()
			return nil
		},
		ResolveChannelPolicy: func(chID, chName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:     "channel",
				WakeMode: "mention",
			}
		},
	})
	pool.cfg.Classifier = classifier.NewClassifier(
		classifier.WithLLMFunc(func(ctx context.Context, model string, prompt string) (string, error) {
			mu.Lock()
			classifierCalls++
			mu.Unlock()
			return `{"confidence": 0.95, "reason": "classifier should not be called"}`, nil
		}),
	)
	pool.Start()
	defer pool.Stop()

	// 1. Send ambient message (no mention)
	msgAmbient := db.Message{
		ID:         "msg-amb-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    "Just talking about aerial views and drones",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msgAmbient)

	pool.processBurst([]db.Message{msgAmbient})

	mu.Lock()
	if classifierCalls != 0 {
		t.Errorf("expected 0 classifier calls in mention mode, got %d", classifierCalls)
	}
	if runnerCalls != 0 {
		t.Errorf("expected 0 runner calls for ambient message in mention mode, got %d", runnerCalls)
	}
	if len(deliveredMsgs) != 0 {
		t.Errorf("expected 0 delivered messages, got %d", len(deliveredMsgs))
	}
	mu.Unlock()


}

func TestProcessBurst_WakeModeMention_DirectMentionWakes(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	sessionID := uuid.New().String()
	channelID := "chan-lounge-wake"
	_ = db.SaveSessionID(database, channelID, sessionID)
	_, _ = session.EnsureSessionDir(sessionID)
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	_ = os.MkdirAll(cliPbDir, 0755)
	_ = os.WriteFile(filepath.Join(cliPbDir, sessionID+".pb"), []byte("mock-pb"), 0644)

	var runnerCalls int
	var deliveredMsgs []string
	var mu sync.Mutex

	pool := NewWorkerPool(WorkerPoolConfig{
		DB: database,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return `{"conversation_id": "` + sID + `", "response": "Hello Alice!"}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, chID, text string) error {
			mu.Lock()
			deliveredMsgs = append(deliveredMsgs, text)
			mu.Unlock()
			return nil
		},
		ResolveChannelPolicy: func(chID, chName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:     "channel",
				WakeMode: "mention",
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	// Direct mention message
	msgMention := db.Message{
		ID:         "msg-mention-1",
		ThreadID:   channelID,
		AuthorName: "Alice",
		Content:    `<USER_REQUEST>
- mentions: [Aerial]
- content: @Aerial what is the plan today?
</USER_REQUEST>`,
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msgMention)

	pool.processBurst([]db.Message{msgMention})

	mu.Lock()
	if runnerCalls != 1 {
		t.Errorf("expected 1 runner call for direct mention, got %d", runnerCalls)
	}
	if len(deliveredMsgs) != 1 || deliveredMsgs[0] != "Hello Alice!" {
		t.Errorf("expected delivered message 'Hello Alice!', got %v", deliveredMsgs)
	}
	mu.Unlock()
}


func TestWorkerPool_ImageDeliveryAndSanitization(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	tempDir := t.TempDir()
	// Create a valid test PNG in tempDir
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	var imgBuf bytes.Buffer
	_ = png.Encode(&imgBuf, img)
	testImgPath := filepath.Join(tempDir, "metrics_chart.png")
	_ = os.WriteFile(testImgPath, imgBuf.Bytes(), 0600)

	var deliveredText string
	var deliveredAttachments []*delivery.Attachment
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			respText := "Telemetry Summary:\n\n![Daily Chart](" + testImgPath + ")\n\nAll systems nominal."
			return mockJSONResponse("c2222222-2222-3333-4444-555555555555", respText), "", 0, nil
		},
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error {
			mu.Lock()
			deliveredText = text
			deliveredAttachments = attachments
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
		ID:         "msg-img-1",
		ThreadID:   "thread-img-1",
		GuildID:    "guild-img-1",
		AuthorID:   "user-img-1",
		AuthorName: "User",
		Content:    "Show me the metrics chart",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(deliveredAttachments) != 1 {
		t.Fatalf("Expected 1 attachment delivered, got %d", len(deliveredAttachments))
	}
	if deliveredAttachments[0].Filename != "metrics_chart.png" {
		t.Errorf("Expected attachment filename metrics_chart.png, got %s", deliveredAttachments[0].Filename)
	}
	if strings.Contains(deliveredText, testImgPath) {
		t.Errorf("Delivered text should not contain local filepath %q", testImgPath)
	}
	if !strings.Contains(deliveredText, "**Daily Chart**") {
		t.Errorf("Expected caption **Daily Chart** in delivered text, got: %s", deliveredText)
	}
}

func TestWorkerPoolShutdown_PreservesProcessingMessageWithoutApology(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var deliveredMessages []string
	var mu sync.Mutex

	runnerStarted := make(chan struct{})
	runnerBlock := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			close(runnerStarted)
			// Wait until shutdown cancels ctx or runnerBlock is closed
			select {
			case <-ctx.Done():
				return "", "process killed by signal", -1, ctx.Err()
			case <-runnerBlock:
				return mockJSONResponse(sessionID, "late response"), "", 0, nil
			}
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredMessages = append(deliveredMessages, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
	})
	pool.Start()

	msg := db.Message{
		ID:         "msg-shutdown-1",
		ThreadID:   "thread-shutdown-1",
		GuildID:    "guild-shutdown-1",
		AuthorID:   "user-shutdown-1",
		AuthorName: "User",
		Content:    "Hello during shutdown",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	pool.Enqueue(msg)

	// Wait for runner to begin execution
	select {
	case <-runnerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for runner to start")
	}

	// Trigger shutdown while runner is running
	pool.Stop()

	mu.Lock()
	if len(deliveredMessages) > 0 {
		t.Errorf("Expected 0 apology/error messages delivered on shutdown, got %d: %v", len(deliveredMessages), deliveredMessages)
	}
	mu.Unlock()

	// Verify database state: message should remain in PROCESSING (not FAILED), and retry_count should NOT have incremented
	var status string
	var retryCount int
	if err := database.QueryRow("SELECT status, retry_count FROM messages WHERE id = $1", msg.ID).Scan(&status, &retryCount); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if status != db.StatusProcessing {
		t.Errorf("Expected message status %q in DB on shutdown, got %q", db.StatusProcessing, status)
	}
	if retryCount != 0 {
		t.Errorf("Expected retry_count 0 on shutdown exit, got %d", retryCount)
	}

	// Now simulate fresh container boot and startup recovery
	newDoneCh := make(chan struct{})
	var finalDeliveredText string
	newPool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return mockJSONResponse("c3333333-2222-3333-4444-555555555555", "Successfully processed on new container!"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			finalDeliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		OnMessageCompleted: func(m db.Message, finalStatus string) {
			close(newDoneCh)
		},
	})
	newPool.Start()
	defer newPool.Stop()

	// Run RecoverInterrupted on fresh pool
	RecoverInterrupted(database, newPool)

	select {
	case <-newDoneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for recovered message to complete on new pool")
	}

	mu.Lock()
	if !strings.Contains(finalDeliveredText, "Successfully processed on new container!") {
		t.Errorf("Expected recovery response delivered, got: %s", finalDeliveredText)
	}
	mu.Unlock()

	// Verify final DB status
	var finalStatus string
	var finalRetryCount, finalRestartCount int
	if err := database.QueryRow("SELECT status, retry_count, restart_count FROM messages WHERE id = $1", msg.ID).Scan(&finalStatus, &finalRetryCount, &finalRestartCount); err != nil {
		t.Fatalf("Final DB query failed: %v", err)
	}
	if finalStatus != db.StatusCompleted {
		t.Errorf("Expected final message status %q, got %q", db.StatusCompleted, finalStatus)
	}
	if finalRestartCount != 1 {
		t.Errorf("Expected final restart_count 1 (incremented during startup recovery), got %d", finalRestartCount)
	}
	if finalRetryCount != 0 {
		t.Errorf("Expected final retry_count 0 (execution retries separate from restart), got %d", finalRetryCount)
	}
}

func TestWorkerPoolShutdown_RunnerSuccessPreserved(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var deliveredText string
	var mu sync.Mutex

	runnerStarted := make(chan struct{})
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			close(runnerStarted)
			// Small sleep, then return success even if ctx is cancelling
			time.Sleep(50 * time.Millisecond)
			return mockJSONResponse("c4444444-2222-3333-4444-555555555555", "Finished just before pool stopped"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
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

	msg := db.Message{
		ID:         "msg-success-shutdown-1",
		ThreadID:   "thread-success-shutdown-1",
		GuildID:    "guild-success-shutdown-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "Complete fast",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	pool.Enqueue(msg)

	<-runnerStarted
	// Stop pool while runner is in-flight; runner returns exit 0
	pool.Stop()

	mu.Lock()
	defer mu.Unlock()
	if deliveredText != "Finished just before pool stopped" {
		t.Errorf("Expected successful response delivered despite shutdown, got: %s", deliveredText)
	}

	var status string
	if err := database.QueryRow("SELECT status FROM messages WHERE id = $1", msg.ID).Scan(&status); err != nil {
		t.Fatalf("DB query failed: %v", err)
	}
	if status != db.StatusCompleted {
		t.Errorf("Expected status %q, got %q", db.StatusCompleted, status)
	}
}

func TestWorkerPoolShutdown_AmbientClassifierCancellationPreserved(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	classifyStarted := make(chan struct{})

	cls := classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		close(classifyStarted)
		<-ctx.Done()
		return "", ctx.Err()
	}))

	// We configure a pool with a classifier that blocks until context is cancelled
	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		Classifier:     cls,
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				AmbientWakeThreshold: ptrFloat(0.75),
			}
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return mockJSONResponse(sessionID, "late"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
	})
	pool.Start()

	// Channel mode message (ThreadID == GuildID)
	msg := db.Message{
		ID:         "msg-ambient-shutdown-1",
		ThreadID:   "channel-shutdown-1",
		GuildID:    "channel-shutdown-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "hey did anyone see that movie?",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	pool.Enqueue(msg)

	select {
	case <-classifyStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for classifier to start")
	}
	pool.Stop()

	// Verify database state: message should NOT be marked COMPLETED [AMBIENT]
	var status string
	if err := database.QueryRow("SELECT status FROM messages WHERE id = $1", msg.ID).Scan(&status); err != nil {
		t.Fatalf("DB query failed: %v", err)
	}
	if status == db.StatusCompleted {
		t.Errorf("Message was mistakenly marked COMPLETED [AMBIENT] during shutdown cancellation")
	}
	if status != db.StatusProcessing {
		t.Errorf("Expected message status %q in DB on shutdown, got %q", db.StatusProcessing, status)
	}
}

func TestQueue_WatchdogInactivityRetryAndExhaustion(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int32
	doneCh := make(chan struct{})

	var deliveredText string
	var deliveredChannel string
	var mu sync.Mutex

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			atomic.AddInt32(&runnerCalls, 1)
			return "", "[watchdog] inactivity timeout exceeded (5m without output)", -1, fmt.Errorf("exit status 255")
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
		ID:         "msg-watchdog-101",
		ThreadID:   "thread-watchdog-202",
		GuildID:    "guild-303",
		AuthorID:   "user-404",
		AuthorName: "User",
		Content:    "Run a long task",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 3 {
		t.Errorf("Expected RunnerFunc to be retried exactly 3 times, got: %d", calls)
	}

	dbMsg, err := db.GetMessage(database, "msg-watchdog-101")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to get message from DB: %v", err)
	}
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected message status %s, got: %s", db.StatusFailed, dbMsg.Status)
	}
	if !strings.Contains(dbMsg.ErrorMessage, "watchdog") && !strings.Contains(dbMsg.ErrorMessage, "inactivity timeout exceeded") {
		t.Errorf("Expected db error_message to mention watchdog error, got: %q", dbMsg.ErrorMessage)
	}

	mu.Lock()
	if deliveredChannel != "thread-watchdog-202" || !strings.Contains(deliveredText, "timed out") {
		t.Errorf("Unexpected delivery: channel=%q, text=%q", deliveredChannel, deliveredText)
	}
	mu.Unlock()
}

func TestQueue_WatchdogInactivityRetryAndRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int32
	var promptsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	var deliveredText string
	var deliveredChannel string

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	mockSessID := "recovery-uuid-505"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", mockSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("mock transcript data\n"), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}
	_ = db.SaveSessionID(database, "thread-recovery-505", mockSessID)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			mu.Unlock()

			if call == 1 {
				// Attempt 1 fails due to watchdog inactivity timeout
				return "", "[watchdog] inactivity timeout exceeded (5m without output)", -1, fmt.Errorf("exit status 255")
			}

			// Attempt 2 recovers and succeeds cleanly
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Recovered successfully on retry!"}`, sessionID), "", 0, nil
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
		ID:         "msg-recovery-101",
		ThreadID:   "thread-recovery-505",
		GuildID:    "guild-303",
		AuthorID:   "user-404",
		AuthorName: "User",
		Content:    "Run a long task",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 2 {
		t.Errorf("Expected RunnerFunc to be called exactly 2 times (failure then success), got: %d", calls)
	}

	mu.Lock()
	if len(promptsSeen) >= 2 {
		// Attempt 1 should contain user request
		if !strings.Contains(promptsSeen[0], "Run a long task") {
			t.Errorf("Attempt 1 prompt missing original user request: %q", promptsSeen[0])
		}
		// Attempt 2 should contain continuation prompt
		if !strings.Contains(promptsSeen[1], "timed out or was interrupted") {
			t.Errorf("Attempt 2 prompt missing continuation instruction: %q", promptsSeen[1])
		}
	}
	if deliveredChannel != "thread-recovery-505" || !strings.Contains(deliveredText, "Recovered successfully on retry!") {
		t.Errorf("Unexpected delivery: channel=%q, text=%q", deliveredChannel, deliveredText)
	}
	mu.Unlock()

	dbMsg, err := db.GetMessage(database, "msg-recovery-101")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to get message from DB: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status %s, got: %s", db.StatusCompleted, dbMsg.Status)
	}
}

func TestDefaultTimeoutMinutes_Is60(t *testing.T) {
	if DefaultTimeoutMinutes != 60 {
		t.Errorf("Expected DefaultTimeoutMinutes to be 60, got %d", DefaultTimeoutMinutes)
	}
}

func TestProcessBurst_ColdStartWatchdogRecoveryAndContinuation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int32
	var promptsSeen []string
	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	var deliveredText string
	var deliveredChannel string

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	coldSessID := "70707070-aaaa-4bbb-cccc-111122223333"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", coldSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("mock transcript data\n"), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	// NOTE: We do NOT seed db.SaveSessionID here; this is a cold start turn!

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			sessionsSeen = append(sessionsSeen, sessionID)
			mu.Unlock()

			if call == 1 {
				// Cold start: sessionID must be empty
				if sessionID != "" {
					t.Errorf("Attempt 1: expected empty sessionID on cold start, got %q", sessionID)
				}
				// Attempt 1 discovers coldSessID in stderr before watchdog kill
				return "", fmt.Sprintf("Starting conversation update stream for %s\n[watchdog] inactivity timeout exceeded (5m without output)", coldSessID), -1, fmt.Errorf("exit status 255")
			}

			// Attempt 2 should attach to the latched coldSessID and receive continuation prompt
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Cold start recovered successfully on retry!"}`, sessionID), "", 0, nil
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
		ID:         "msg-cold-101",
		ThreadID:   "thread-cold-606",
		GuildID:    "guild-303",
		AuthorID:   "user-404",
		AuthorName: "User",
		Content:    "Execute initial task cold",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for cold-start message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 2 {
		t.Errorf("Expected RunnerFunc to be called exactly 2 times (failure then success), got: %d", calls)
	}

	mu.Lock()
	if len(sessionsSeen) >= 2 {
		if sessionsSeen[0] != "" {
			t.Errorf("Attempt 1 should have empty sessionID, got: %q", sessionsSeen[0])
		}
		if sessionsSeen[1] != coldSessID {
			t.Errorf("Attempt 2 should have latched sessionID %q, got: %q", coldSessID, sessionsSeen[1])
		}
	}
	if len(promptsSeen) >= 2 {
		// Attempt 1 should contain user request
		if !strings.Contains(promptsSeen[0], "Execute initial task cold") {
			t.Errorf("Attempt 1 prompt missing original user request: %q", promptsSeen[0])
		}
		// Attempt 2 should contain continuation prompt
		if !strings.Contains(promptsSeen[1], "timed out or was interrupted") {
			t.Errorf("Attempt 2 prompt missing continuation instruction: %q", promptsSeen[1])
		}
	}
	if deliveredChannel != "thread-cold-606" || !strings.Contains(deliveredText, "Cold start recovered successfully on retry!") {
		t.Errorf("Unexpected delivery: channel=%q, text=%q", deliveredChannel, deliveredText)
	}
	mu.Unlock()

	// Verify database has latched the session ID
	savedSess, err := db.GetSessionID(database, "thread-cold-606")
	if err != nil || savedSess != coldSessID {
		t.Errorf("Expected latched session %q in DB for thread, got: %q (err: %v)", coldSessID, savedSess, err)
	}
}

func TestProcessBurst_ColdStartStreamJsonInitLatchingOnFailure(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int32
	var promptsSeen []string
	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	var deliveredText string
	var deliveredChannel string

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	coldSessID := "99999999-bbbb-4ccc-dddd-555566667777"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", coldSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("mock transcript data\n"), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			sessionsSeen = append(sessionsSeen, sessionID)
			mu.Unlock()

			if call == 1 {
				// Cold start: sessionID must be empty
				if sessionID != "" {
					t.Errorf("Attempt 1: expected empty sessionID on cold start, got %q", sessionID)
				}
				// Attempt 1 emits stream-json init event on stdout, but exits with error
				stdout := fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n{\"event\":\"step_update\"}\n", coldSessID)
				stderr := "[watchdog] inactivity timeout exceeded (5m without output)\n"
				return stdout, stderr, -1, fmt.Errorf("exit status 255")
			}

			// Attempt 2 attaches to latched coldSessID
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Stream-json cold start recovered!"}`, sessionID), "", 0, nil
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
		ID:        "msg-cold-stream-1",
		ThreadID:  "thread-cold-stream-1",
		Content:   "Please write the code for stream-json",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for cold start recovery")
	}

	mu.Lock()
	if runnerCalls != 2 {
		t.Fatalf("Expected 2 runner calls (failure then success), got %d", runnerCalls)
	}
	if len(sessionsSeen) >= 2 {
		if sessionsSeen[0] != "" {
			t.Errorf("Attempt 1 sessionID should be empty, got %q", sessionsSeen[0])
		}
		if sessionsSeen[1] != coldSessID {
			t.Errorf("Attempt 2 sessionID should be latched %q, got %q", coldSessID, sessionsSeen[1])
		}
	}
	if deliveredChannel != "thread-cold-stream-1" || !strings.Contains(deliveredText, "Stream-json cold start recovered!") {
		t.Errorf("Unexpected delivery: channel=%q, text=%q", deliveredChannel, deliveredText)
	}
	mu.Unlock()

	savedSess, err := db.GetSessionID(database, "thread-cold-stream-1")
	if err != nil || savedSess != coldSessID {
		t.Errorf("Expected latched session %q in DB, got: %q (err: %v)", coldSessID, savedSess, err)
	}
}

func TestProcessBurst_ColdStartTransientRecoveryAndContinuation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int32
	var promptsSeen []string
	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	transientSessID := "80808080-bbbb-4ccc-dddd-444455556666"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", transientSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("mock transcript data\n"), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			sessionsSeen = append(sessionsSeen, sessionID)
			mu.Unlock()

			if call == 1 {
				return "", fmt.Sprintf("Starting conversation update stream for %s\nError 503: high demand service unavailable", transientSessID), 1, fmt.Errorf("exit status 1")
			}

			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Transient error recovered on retry!"}`, sessionID), "", 0, nil
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

	msg := db.Message{
		ID:         "msg-transient-101",
		ThreadID:   "thread-transient-707",
		GuildID:    "guild-303",
		AuthorID:   "user-404",
		AuthorName: "User",
		Content:    "Run transient task cold",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 2 {
		t.Errorf("Expected RunnerFunc to be called exactly 2 times, got: %d", calls)
	}

	mu.Lock()
	if len(sessionsSeen) >= 2 {
		if sessionsSeen[0] != "" {
			t.Errorf("Attempt 1 should have empty sessionID, got: %q", sessionsSeen[0])
		}
		if sessionsSeen[1] != transientSessID {
			t.Errorf("Attempt 2 should have latched sessionID %q, got: %q", transientSessID, sessionsSeen[1])
		}
	}
	if len(promptsSeen) >= 2 {
		if strings.Contains(promptsSeen[1], "timed out or was interrupted") {
			t.Errorf("Attempt 2 prompt should not contain timeout continuation instruction: %q", promptsSeen[1])
		}
		if !strings.Contains(promptsSeen[1], "Run transient task cold") {
			t.Errorf("Attempt 2 prompt missing original prompt: %q", promptsSeen[1])
		}
	}
	mu.Unlock()

	savedSess, err := db.GetSessionID(database, "thread-transient-707")
	if err != nil || savedSess != transientSessID {
		t.Errorf("Expected latched session %q in DB, got: %q (err: %v)", transientSessID, savedSess, err)
	}
}

func TestProcessBurst_EmptyStdout_TranscriptRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	testSessID := "sess-transcript-recovery-999"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", testSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)

	transcriptContent := `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-06T12:00:00Z","content":"Finish it"}
{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-09-06T12:01:00Z","content":"Finished the task with 100% coverage!"}
`
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(transcriptContent), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	_ = db.SaveSessionID(database, "thread-empty-stdout-1", testSessID)

	var deliveredText string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			// agy exits 0 with empty stdout (buffering or background task yield), but response is on disk
			return "", "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
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
		ID:         "msg-empty-1",
		ThreadID:   "thread-empty-stdout-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "Finish it",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	if !strings.Contains(deliveredText, "Finished the task with 100% coverage!") {
		t.Errorf("Expected recovered transcript response to be delivered, got: %q", deliveredText)
	}
	mu.Unlock()

	dbMsg, err := db.GetMessage(database, "msg-empty-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status COMPLETED, got: %s", dbMsg.Status)
	}
}

func TestProcessBurst_StreamInterrupted_TranscriptRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	testSessID := "11111111-2222-4333-8444-555555555555"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", testSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)

	transcriptContent := `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-07T21:24:20Z","content":"Have the gang review"}
{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"DONE","created_at":"2026-09-07T21:25:06Z","content":"Error: The stream was interrupted. Please continue the task you were working on."}
{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-09-07T21:26:39Z","content":"Here is the complete gang review with high consensus!"}
`
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(transcriptContent), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	_ = db.SaveSessionID(database, "thread-interrupted-1", testSessID)

	var runnerCalls int32
	var deliveredText string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			atomic.AddInt32(&runnerCalls, 1)
			// agy exits 0, but stream-json emits error status with "stream was interrupted"
			out := fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n{\"event\":\"result\",\"status\":\"error\",\"error\":\"The stream was interrupted. Please continue the task you were working on.\"}", sessionID)
			return out, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
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
		ID:         "msg-interrupted-1",
		ThreadID:   "thread-interrupted-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "Have the gang review",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 1 {
		t.Errorf("Expected exactly 1 runner call (recovered immediately), got: %d", calls)
	}

	mu.Lock()
	if !strings.Contains(deliveredText, "Here is the complete gang review with high consensus!") {
		t.Errorf("Expected recovered transcript response to be delivered, got: %q", deliveredText)
	}
	mu.Unlock()

	dbMsg, err := db.GetMessage(database, "msg-interrupted-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status COMPLETED, got: %s", dbMsg.Status)
	}
}

func TestProcessBurst_StreamInterrupted_NoResponse_RotatesSessionCorrupt(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	testSessID := "22222222-3333-4444-8555-666666666666"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", testSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)

	// No PLANNER_RESPONSE after USER_INPUT, only error step
	transcriptContent := `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-07T21:24:20Z","content":"Have the gang review"}
{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"DONE","created_at":"2026-09-07T21:25:06Z","content":"Error: The stream was interrupted. Please continue the task you were working on."}
`
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(transcriptContent), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	_ = db.SaveSessionID(database, "thread-interrupted-corrupt-2", testSessID)

	var runnerCalls int32
	var sessionsSeen []string
	var deliveredTexts []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			sessionsSeen = append(sessionsSeen, sessionID)
			mu.Unlock()

			if call == 1 {
				// Attempt 1 fails with stream interruption and no response in transcript
				out := fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n{\"event\":\"result\",\"status\":\"error\",\"error\":\"The stream was interrupted. Please continue the task you were working on.\"}", sessionID)
				return out, "", 0, nil
			}

			// Attempt 2 should run with rotated/empty session ID and succeed
			return `{"conversation_id":"33333333-4444-4555-8666-777777777777","status":"SUCCESS","response":"Recovered on clean session!"}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredTexts = append(deliveredTexts, text)
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
		ID:         "msg-interrupted-corrupt-2",
		ThreadID:   "thread-interrupted-corrupt-2",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "Have the gang review",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 2 {
		t.Errorf("Expected exactly 2 runner calls, got: %d", calls)
	}

	mu.Lock()
	if len(sessionsSeen) >= 2 {
		if sessionsSeen[0] != testSessID {
			t.Errorf("Attempt 1 expected session %q, got %q", testSessID, sessionsSeen[0])
		}
		// Attempt 2 MUST be called with empty session ID (rotated due to session corruption)
		if sessionsSeen[1] != "" {
			t.Errorf("Attempt 2 expected empty session (rotated due to corruption), got %q", sessionsSeen[1])
		}
	}
	mu.Unlock()

	dbMsg, err := db.GetMessage(database, "msg-interrupted-corrupt-2")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to retrieve message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status COMPLETED, got: %s", dbMsg.Status)
	}
}

func TestProcessBurst_EmptyStdout_TransientRetryAndContinuation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	testSessID := "sess-empty-retry-888"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", testSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)

	// No PLANNER_RESPONSE after USER_INPUT on disk
	transcriptContent := `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-06T12:00:00Z","content":"Finish it"}
`
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(transcriptContent), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	_ = db.SaveSessionID(database, "thread-empty-retry-2", testSessID)

	var runnerCalls int32
	var promptsSeen []string
	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			sessionsSeen = append(sessionsSeen, sessionID)
			mu.Unlock()

			if call == 1 {
				// Empty stdout on exit 0 with no transcript response -> transient error
				return "", "", 0, nil
			}

			// Attempt 2 should receive original prompt and testSessID
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Recovered on attempt 2!"}`, sessionID), "", 0, nil
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

	msg := db.Message{
		ID:         "msg-empty-2",
		ThreadID:   "thread-empty-retry-2",
		GuildID:    "guild-2",
		AuthorID:   "user-2",
		AuthorName: "User",
		Content:    "Finish it",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 2 {
		t.Errorf("Expected exactly 2 runner calls, got: %d", calls)
	}

	mu.Lock()
	if len(sessionsSeen) >= 2 {
		if sessionsSeen[0] != testSessID {
			t.Errorf("Attempt 1 expected session %q, got %q", testSessID, sessionsSeen[0])
		}
		if sessionsSeen[1] != testSessID {
			t.Errorf("Attempt 2 expected preserved session %q, got %q", testSessID, sessionsSeen[1])
		}
	}
	if len(promptsSeen) >= 2 {
		if strings.Contains(promptsSeen[1], "timed out or was interrupted") {
			t.Errorf("Attempt 2 prompt should not contain timeout continuation instruction: %q", promptsSeen[1])
		}
		if !strings.Contains(promptsSeen[1], "Finish it") {
			t.Errorf("Attempt 2 prompt missing original prompt: %q", promptsSeen[1])
		}
	}
	mu.Unlock()
}

func TestProcessBurst_GeneralFailure_PreservesSessionOnDisk(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	testSessID := "sess-general-fail-777"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", testSessID)
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)

	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("existing transcript data\n"), 0600); err != nil {
		t.Fatalf("failed to write mock transcript: %v", err)
	}

	_ = db.SaveSessionID(database, "thread-general-fail-3", testSessID)

	var runnerCalls int32
	var promptsSeen []string
	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := atomic.AddInt32(&runnerCalls, 1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			sessionsSeen = append(sessionsSeen, sessionID)
			mu.Unlock()

			if call == 1 {
				// Non-transient exit 1 with generic error
				return "", "fatal execution error: out of memory", 1, fmt.Errorf("exit status 1")
			}

			// Attempt 2: verify session was preserved on disk and passed in with original prompt
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Recovered from exit 1!"}`, sessionID), "", 0, nil
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

	msg := db.Message{
		ID:         "msg-fail-3",
		ThreadID:   "thread-general-fail-3",
		GuildID:    "guild-3",
		AuthorID:   "user-3",
		AuthorName: "User",
		Content:    "Finish it",
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
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	calls := atomic.LoadInt32(&runnerCalls)
	if calls != 2 {
		t.Errorf("Expected exactly 2 runner calls, got: %d", calls)
	}

	mu.Lock()
	if len(sessionsSeen) >= 2 {
		if sessionsSeen[0] != testSessID {
			t.Errorf("Attempt 1 expected session %q, got %q", testSessID, sessionsSeen[0])
		}
		if sessionsSeen[1] != testSessID {
			t.Errorf("Attempt 2 expected preserved session %q, got %q", testSessID, sessionsSeen[1])
		}
	}
	if len(promptsSeen) >= 2 {
		if strings.Contains(promptsSeen[1], "timed out or was interrupted") {
			t.Errorf("Attempt 2 prompt should not contain timeout continuation instruction: %q", promptsSeen[1])
		}
		if !strings.Contains(promptsSeen[1], "Finish it") {
			t.Errorf("Attempt 2 prompt missing original prompt: %q", promptsSeen[1])
		}
	}
	mu.Unlock()
}

func TestWorkerPool_FullBuffer_NoEvictionZombieRace(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	const totalMsgs = 150
	var processedCount atomic.Int32

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		IdleTimeout:    20 * time.Millisecond,
		MaxAttempts:    1,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			time.Sleep(2 * time.Millisecond)
			return mockJSONResponse(sessionID, "OK"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			processedCount.Add(1)
		},
	})
	pool.Start()

	// Concurrently enqueue 150 messages to the same thread (channel buffer is 100)
	var wg sync.WaitGroup
	for i := 0; i < totalMsgs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m := db.Message{
				ID:         fmt.Sprintf("msg-race-%d", idx),
				ThreadID:   "thread-evict-race",
				GuildID:    "guild-race",
				AuthorID:   "user-race",
				AuthorName: "User",
				Content:    fmt.Sprintf("msg content %d", idx),
				Status:     db.StatusPending,
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
			_ = db.InsertMessage(database, m)
			pool.Enqueue(m)
		}(i)
	}
	wg.Wait()

	// Wait for all messages to complete
	deadline := time.Now().Add(10 * time.Second)
	for processedCount.Load() < totalMsgs && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if got := processedCount.Load(); got != totalMsgs {
		t.Fatalf("Expected %d messages processed, got %d", totalMsgs, got)
	}

	pool.StopWithTimeout(2 * time.Second)
}

func TestProcessBurst_TransientError_RetainsOriginalPrompt(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	testSessID := "sess-transient-503"
	_, err = session.EnsureSessionDir(testSessID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}

	var mu sync.Mutex
	var promptsSeen []string
	var runnerCalls atomic.Int32
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			call := runnerCalls.Add(1)
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			mu.Unlock()

			if call == 1 {
				// Simulate transient 503 error
				return "", "Error 503: Service Unavailable. High demand.", 1, fmt.Errorf("exit code 1")
			}
			return mockJSONResponse(sessionID, "Recovered cleanly"), "", 0, nil
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

	msg := db.Message{
		ID:         "msg-503-1",
		ThreadID:   "thread-503",
		GuildID:    "guild-503",
		AuthorID:   "user-503",
		AuthorName: "User",
		Content:    "Original prompt text that must not be wiped",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)
	_ = db.SaveSessionID(database, msg.ThreadID, testSessID)

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(promptsSeen) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(promptsSeen))
	}
	// Attempt 2 must RETAIN the original prompt, not overwrite with continuation text
	if strings.Contains(promptsSeen[1], "timed out or was interrupted") {
		t.Errorf("Attempt 2 prompt should NOT have been overwritten with timeout continuation: %q", promptsSeen[1])
	}
	if !strings.Contains(promptsSeen[1], "Original prompt text that must not be wiped") {
		t.Errorf("Attempt 2 prompt missing original prompt content: %q", promptsSeen[1])
	}
}

func TestIsTier1Wake_SchedulerMessage(t *testing.T) {
	// 1. AuthorID == "scheduler"
	msgSched := db.Message{
		ID:        "m-sched-1",
		AuthorID:  "scheduler",
		Content:   "check system health",
		CreatedAt: time.Now().UTC(),
	}
	if !isTier1Wake(msgSched, "bot-123", nil, "mention") {
		t.Errorf("Expected isTier1Wake to return true for AuthorID == scheduler in mention mode")
	}

	// 2. ScheduleRunID != ""
	msgRun := db.Message{
		ID:            "m-run-1",
		AuthorID:      "user-456",
		ScheduleRunID: "run-uuid-999",
		Content:       "run cron report",
		CreatedAt:     time.Now().UTC(),
	}
	if !isTier1Wake(msgRun, "bot-123", nil, "mention") {
		t.Errorf("Expected isTier1Wake to return true for non-empty ScheduleRunID in mention mode")
	}
}

func TestResolveBotRoleIDs(t *testing.T) {
	// Nil session/state safety
	if roles := ResolveBotRoleIDs(nil, "guild-1", "bot-1"); roles != nil {
		t.Errorf("Expected nil for nil session, got %v", roles)
	}

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{
		ID:       "bot-1",
		Username: "Aerial",
	}

	guild := &discordgo.Guild{
		ID: "guild-1",
		Roles: []*discordgo.Role{
			{ID: "role-aerial-managed", Name: "Aerial", Managed: true},
			{ID: "role-gundam-alt", Name: "Gundam"},
			{ID: "role-unrelated", Name: "Member"},
		},
		Members: []*discordgo.Member{
			{
				User:  &discordgo.User{ID: "bot-1"},
				Roles: []string{"role-assigned-1"},
			},
		},
	}
	_ = s.State.GuildAdd(guild)

	roles := ResolveBotRoleIDs(s, "guild-1", "bot-1")
	expected := []string{"role-aerial-managed", "role-assigned-1", "role-gundam-alt"}
	sort.Strings(roles)
	sort.Strings(expected)
	if strings.Join(roles, ",") != strings.Join(expected, ",") {
		t.Errorf("Expected roles %v, got %v", expected, roles)
	}
}

func TestProcessBurst_QuotaPause_SchedulesOneShotAndNotifies(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int
	var deliveredTexts []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return "", "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", 1, errors.New("exit 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredTexts = append(deliveredTexts, text)
			mu.Unlock()
			return nil
		},
		OnMessageCompleted: func(m db.Message, status string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	threadID := "chan-quota-1"
	msg := db.Message{
		ID:        "msg-quota-1",
		ThreadID:  threadID,
		Content:   "<@bot> can you analyze our code coverage?",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message to complete")
	}

	mu.Lock()
	defer mu.Unlock()

	// 1. Must fail-fast: exactly 1 runner call, zero retries
	if runnerCalls != 1 {
		t.Fatalf("Expected exactly 1 runner call (fail-fast), got %d", runnerCalls)
	}

	// 2. Must deliver transparent notification with countdown and epoch timestamp
	if len(deliveredTexts) != 1 {
		t.Fatalf("Expected 1 delivered notice, got %d", len(deliveredTexts))
	}
	notice := deliveredTexts[0]
	if !strings.Contains(notice, "16m 58s") {
		t.Errorf("Expected '16m 58s' in notice, got: %s", notice)
	}
	if !strings.Contains(notice, "<t:") || !strings.Contains(notice, ":R>") {
		t.Errorf("Expected relative Discord timestamp in notice, got: %s", notice)
	}
	if !strings.Contains(notice, "automatically scheduled a retry") {
		t.Errorf("Expected auto-retry confirmation in notice, got: %s", notice)
	}

	// 3. One-shot schedule must be inserted into database for this thread with future run_at
	var schedPrompt string
	var runAt time.Time
	err = database.QueryRow(`SELECT prompt, run_at FROM one_shot_schedules WHERE thread_id = $1`, threadID).Scan(&schedPrompt, &runAt)
	if err != nil {
		t.Fatalf("Error querying one_shot_schedules: %v", err)
	}
	if !strings.Contains(schedPrompt, "[QUOTA_RETRY]") || !strings.Contains(schedPrompt, "code coverage") {
		t.Errorf("Expected retry prompt with [QUOTA_RETRY] and coverage query, got: %q", schedPrompt)
	}
	if time.Until(runAt) < 16*time.Minute {
		t.Errorf("Expected runAt to be at least 16m in the future, got %v (runAt: %s)", time.Until(runAt), runAt)
	}

	// 4. Triggering message marked FAILED with [QUOTA_PAUSED]
	dbMsg, _ := db.GetMessage(database, msg.ID)
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected message status FAILED, got %s", dbMsg.Status)
	}
	if !strings.Contains(dbMsg.ErrorMessage, "[QUOTA_PAUSED") {
		t.Errorf("Expected [QUOTA_PAUSED] in error message, got: %s", dbMsg.ErrorMessage)
	}
}

func TestProcessBurst_QuotaPause_CircuitBreakerDoesNotReschedule(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int
	var deliveredTexts []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return "", "Individual quota reached. Resets in 10m.", 1, errors.New("exit 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredTexts = append(deliveredTexts, text)
			mu.Unlock()
			return nil
		},
		OnMessageCompleted: func(m db.Message, status string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	threadID := "chan-circuit-1"
	// Message was already generated by an automated retry
	msg := db.Message{
		ID:            "msg-circuit-1",
		ThreadID:      threadID,
		ScheduleRunID: "run-sched-previous-1",
		Content:       "[QUOTA_RETRY] Previous failed question",
		Status:        db.StatusPending,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msg)

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message completion")
	}

	mu.Lock()
	defer mu.Unlock()

	// Circuit breaker: must NOT insert another one-shot schedule!
	var schedCount int
	_ = database.QueryRow(`SELECT COUNT(*) FROM one_shot_schedules WHERE thread_id = $1`, threadID).Scan(&schedCount)
	if schedCount != 0 {
		t.Fatalf("Expected 0 one-shot schedules created on circuit breaker, got %d", schedCount)
	}

	// Must notify user with circuit breaker warning
	if len(deliveredTexts) != 1 {
		t.Fatalf("Expected 1 delivered text, got %d", len(deliveredTexts))
	}
	if !strings.Contains(deliveredTexts[0], "paused automated retries") {
		t.Errorf("Expected circuit breaker notice in delivery, got: %s", deliveredTexts[0])
	}
}

func TestProcessBurst_TrailingBurstSuppression_OnQuotaPause(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var runnerCalls int
	var mu sync.Mutex
	completedMsgs := make(map[string]string)
	var completedCount int
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel"}
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return "", "Individual quota reached. Resets in 16m58s.", 1, errors.New("exit 1")
		},
		OnMessageCompleted: func(m db.Message, status string) {
			mu.Lock()
			completedMsgs[m.ID] = status
			completedCount++
			if completedCount == 2 {
				close(doneCh)
			}
			mu.Unlock()
		},
	})
	pool.Start()
	defer pool.Stop()

	threadID := "chan-burst-suppress-1"
	t0 := time.Now().UTC()
	msg1 := db.Message{
		ID:        "msg-burst-1",
		ThreadID:  threadID,
		Content:   "Aerial Question 1",
		Status:    db.StatusPending,
		CreatedAt: t0,
		UpdatedAt: t0,
	}
	msg2 := db.Message{
		ID:        "msg-burst-2",
		ThreadID:  threadID,
		Content:   "Aerial Question 2",
		Status:    db.StatusPending,
		CreatedAt: t0.Add(100 * time.Millisecond),
		UpdatedAt: t0.Add(100 * time.Millisecond),
	}
	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)

	// Simulate burst arrival
	pool.processBurst([]db.Message{msg1, msg2})

	mu.Lock()
	defer mu.Unlock()

	// Only message 1 should have invoked runner; message 2 must be suppressed
	if runnerCalls != 1 {
		t.Fatalf("Expected runner called only once, got %d", runnerCalls)
	}

	// Message 2 should be marked failed with trailing suppression reason
	m2, _ := db.GetMessage(database, msg2.ID)
	if m2.Status != db.StatusFailed {
		t.Errorf("Expected msg2 status FAILED, got %s", m2.Status)
	}
	if !strings.Contains(m2.ErrorMessage, "Trailing message suppressed") {
		t.Errorf("Expected trailing suppression error on msg2, got: %s", m2.ErrorMessage)
	}
}







func TestQueueWorker_PerScopeSerialization(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	var activeWorkers int
	var maxActiveWorkers int

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			activeWorkers++
			if activeWorkers > maxActiveWorkers {
				maxActiveWorkers = activeWorkers
			}
			mu.Unlock()
			
			time.Sleep(50 * time.Millisecond) // Simulate work

			mu.Lock()
			activeWorkers--
			mu.Unlock()
			
			return mockJSONResponse("", "OK"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
	})
	
	// Create multiple messages for the same thread but bypass Enqueue routing to test processBurst serialization directly.
	msg1 := db.Message{ID: "msg-scope-1", ThreadID: "thread-scope-test", Content: "Msg 1"}
	msg2 := db.Message{ID: "msg-scope-2", ThreadID: "thread-scope-test", Content: "Msg 2"}
	msg3 := db.Message{ID: "msg-scope-3", ThreadID: "thread-scope-test", Content: "Msg 3"}
	_ = db.InsertMessage(database, msg1)
	_ = db.InsertMessage(database, msg2)
	_ = db.InsertMessage(database, msg3)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); pool.processBurst([]db.Message{msg1}) }()
	go func() { defer wg.Done(); pool.processBurst([]db.Message{msg2}) }()
	go func() { defer wg.Done(); pool.processBurst([]db.Message{msg3}) }()

	wg.Wait()

	if maxActiveWorkers > 1 {
		t.Errorf("Expected per-scope worker serialization, but found %d concurrent workers for same scope", maxActiveWorkers)
	}
}

func TestThreadSession_RotationAt50Turns(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.SaveSessionID(database, "thread-rotate-test", "old-session-id")
	for i := 0; i < DefaultMaxSessionTurns; i++ {
		_, _ = db.IncrementSessionTurnCount(database, "thread-rotate-test")
	}

	var mu sync.Mutex
	var gotSessionID string
	doneCh := make(chan struct{})

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"}, // Thread mode
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			gotSessionID = sessionID
			mu.Unlock()
			return mockJSONResponse(uuid.New().String(), "OK"), "", 0, nil
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

	msg := db.Message{ID: "msg-rotate", ThreadID: "thread-rotate-test", Content: fmt.Sprintf("Turn %d", DefaultMaxSessionTurns+1)}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	mu.Lock()
	if gotSessionID != "" {
		t.Errorf("Expected cold start (empty session ID) after %d turns for thread mode, got: %q", DefaultMaxSessionTurns, gotSessionID)
	}
	mu.Unlock()
}

func TestRunner_DefensiveLatchingAndCold429(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	var receivedIDs []string
	doneCh := make(chan struct{}, 2)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			receivedIDs = append(receivedIDs, prompt)
			mu.Unlock()

			if strings.Contains(prompt, "Test429") {
				return "", "HTTP 429 Too Many Requests: context length exceeded", 1, fmt.Errorf("429")
			}
			if strings.Contains(prompt, "TestEmptyLatch") {
				return "{}", "", 0, nil // Invalid JSON with no conversation ID
			}
			return "", "", 0, fmt.Errorf("unknown")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. Test Cold 429 (should not retry)
	msg1 := db.Message{ID: "msg-429", ThreadID: "thread-429", Content: "Test429"}
	_ = db.InsertMessage(database, msg1)
	pool.Enqueue(msg1)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for 429")
	}

	mu.Lock()
	dbMsg1, _ := db.GetMessage(database, "msg-429")
	if dbMsg1.Status != db.StatusFailed {
		t.Errorf("Expected 429 to fail fast, got status: %s", dbMsg1.Status)
	}
	mu.Unlock()

	// 2. Test Empty Latch (should not retry)
	msg2 := db.Message{ID: "msg-latch", ThreadID: "thread-latch", Content: "TestEmptyLatch"}
	_ = db.InsertMessage(database, msg2)
	pool.Enqueue(msg2)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for empty latch")
	}

	mu.Lock()
	dbMsg2, _ := db.GetMessage(database, "msg-latch")
	if dbMsg2.Status != db.StatusFailed {
		t.Errorf("Expected empty latch to fail fast, got status: %s", dbMsg2.Status)
	}
	mu.Unlock()
}

func TestThreadColdStartSummarization_Integration(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Register channel snapshots in cache
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "thread-100",
		ParentID: "channel-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "channel-1",
		ParentID: "",
		Type:     discordgo.ChannelTypeGuildText,
	})

	var mu sync.Mutex
	var lastPromptReceived string
	var llmCallCount int

	doneCh := make(chan struct{}, 10)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
			mu.Lock()
			llmCallCount++
			mu.Unlock()
			if strings.Contains(prompt, "fail-llm") {
				return "", errors.New("llm error")
			}
			if strings.Contains(prompt, "invalid-xml") {
				return "No tags summary", nil
			}
			return "<THREAD_SUMMARY>\n- Technical Decision: Architectural shift to SQLite\n</THREAD_SUMMARY>", nil
		},
		HistoryFetcher: func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			if strings.Contains(channelID, "no-history") {
				return nil, nil
			}
			content := "Let's adopt SQLite for storage"
			if strings.Contains(channelID, "fail-llm") {
				content = "fail-llm in thread history"
			}
			return []HistoryMessage{
				{
					ID:         "msg-hist-1",
					AuthorName: "Alice",
					Role:       "User",
					Content:    content,
					CreatedAt:  time.Now().UTC().Add(-10 * time.Minute),
				},
			}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			lastPromptReceived = prompt
			mu.Unlock()
			stderr = "Starting conversation update stream for b1b7fb78-d85d-4bd6-ba9e-330f1e30596f\n"
			return mockJSONResponse("b1b7fb78-d85d-4bd6-ba9e-330f1e30596f", "Done"), stderr, 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) (stop func()) { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. Thread Cold Start -> Should summarize and inject <THREAD_SUMMARY>
	t.Run("Thread Cold Start Summarizes History", func(t *testing.T) {
		msg := db.Message{ID: "m1", ThreadID: "thread-100", Content: "Hello thread"}
		_ = db.InsertMessage(database, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for thread message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		calls := llmCallCount
		mu.Unlock()

		if !strings.Contains(prompt, "<THREAD_SUMMARY>") {
			t.Errorf("Expected prompt to contain <THREAD_SUMMARY>, got:\n%s", prompt)
		}
		if calls != 1 {
			t.Errorf("Expected 1 LLM call, got %d", calls)
		}

		// Verify saved summary and watermark in SQLite
		sum, lastMsgID, err := db.GetThreadSummary(database, "thread-100")
		if err != nil || sum == "" || lastMsgID != "msg-hist-1" {
			t.Errorf("Expected saved summary and watermark msg-hist-1, got sum=%q, watermark=%q, err=%v", sum, lastMsgID, err)
		}
	})

	// 2. Main Channel -> Should NOT summarize history
	t.Run("Main Channel Does Not Summarize", func(t *testing.T) {
		mu.Lock()
		prevCalls := llmCallCount
		mu.Unlock()

		msg := db.Message{ID: "m2", ThreadID: "channel-1", Content: "Hello main channel"}
		_ = db.InsertMessage(database, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for main channel message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		calls := llmCallCount
		mu.Unlock()

		if strings.Contains(prompt, "<THREAD_SUMMARY>") {
			t.Errorf("Main channel prompt should NOT contain <THREAD_SUMMARY>, got:\n%s", prompt)
		}
		if calls != prevCalls {
			t.Errorf("Expected no additional LLM call for main channel, got %d calls (was %d)", calls, prevCalls)
		}
	})

	// 3. Thread Cold Start Watermark Cache Hit -> Should reuse cached summary without calling LLM again
	t.Run("Thread Cold Start Cache Watermark Hit", func(t *testing.T) {
		mu.Lock()
		prevCalls := llmCallCount
		mu.Unlock()

		CacheDiscordChannel(&discordgo.Channel{
			ID:       "thread-cached",
			ParentID: "channel-1",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		})
		_ = db.SaveThreadSummary(database, "thread-cached", "<THREAD_SUMMARY>\n- Existing cached summary\n</THREAD_SUMMARY>", "msg-hist-1")

		msg := db.Message{ID: "m3", ThreadID: "thread-cached", Content: "Cached test"}
		_ = db.InsertMessage(database, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for cached thread message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		calls := llmCallCount
		mu.Unlock()

		if !strings.Contains(prompt, "Existing cached summary") {
			t.Errorf("Expected prompt to contain cached summary, got:\n%s", prompt)
		}
		if calls != prevCalls {
			t.Errorf("Expected 0 new LLM calls due to cache hit, but got %d calls (was %d)", calls, prevCalls)
		}
	})

	// 4. Fallback on LLM Failure / Malformed XML
	t.Run("Fallback on LLM Failure", func(t *testing.T) {
		CacheDiscordChannel(&discordgo.Channel{
			ID:       "thread-fail-llm",
			ParentID: "channel-1",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		})

		msg := db.Message{ID: "m4", ThreadID: "thread-fail-llm", Content: "fail-llm request"}
		_ = db.InsertMessage(database, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for failing thread message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		mu.Unlock()

		if strings.Contains(prompt, "<THREAD_SUMMARY>") {
			t.Errorf("Expected prompt NOT to contain <THREAD_SUMMARY> on LLM failure, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, "<CHANNEL_HISTORY>") {
			t.Errorf("Expected fallback to raw <CHANNEL_HISTORY>, got:\n%s", prompt)
		}
	})
}

func TestProcessBurst_ColdStart429_RetriesTransiently(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	attempts := 0
	doneCh := make(chan struct{}, 1)

	validUUID := uuid.New().String()
	pbPath := filepath.Join(tmpDir, ".gemini", "antigravity", "conversations", validUUID+".pb")
	_ = os.MkdirAll(filepath.Dir(pbPath), 0755)
	_ = os.WriteFile(pbPath, []byte("protobuf-data"), 0644)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attempts++
			curr := attempts
			mu.Unlock()

			if curr < 3 {
				// Cold start 429 returns transient error on attempts 1 and 2
				return "", "HTTP 429: Resource has been exhausted (e.g. check quota)", 1, fmt.Errorf("exit status 1")
			}
			// Attempt 3 succeeds with valid JSON and conversation ID
			return fmt.Sprintf(`{"conversation_id":%q,"response":"Success after 429 backoff"}`, validUUID), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-cold-429", ThreadID: "thread-cold-429", Content: "Hello 429"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message to complete")
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Errorf("Expected exactly 3 attempts (2 retries on 429 then success), got %d attempts", attempts)
	}

	dbMsg, _ := db.GetMessage(database, "msg-cold-429")
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status to be COMPLETED, got %s", dbMsg.Status)
	}
}

func TestProcessBurst_NonTransient_FailsFastOnAttempt1(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	existingSess := uuid.New().String()
	_ = db.SaveSessionID(database, "thread-fast-fail", existingSess)

	var mu sync.Mutex
	attempts := 0
	var deliveredText string
	doneCh := make(chan struct{}, 1)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			return "", "Error: invalid api key provided", 1, fmt.Errorf("exit status 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-auth-fail", ThreadID: "thread-fast-fail", Content: "Run with bad auth"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message to fail fast")
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts != 1 {
		t.Errorf("Expected exactly 1 attempt for non-transient error, got %d", attempts)
	}

	dbMsg, _ := db.GetMessage(database, "msg-auth-fail")
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected message status to be FAILED, got %s", dbMsg.Status)
	}

	savedSess, _ := db.GetSessionID(database, "thread-fast-fail")
	if savedSess != "" {
		t.Errorf("Expected latched session in DB to be cleared on non-transient fail-fast, got %q", savedSess)
	}

	if strings.Contains(deliveredText, "temporarily unavailable") {
		t.Errorf("Expected non-transient failure NOT to gaslight user with temporary model unavailability, got: %s", deliveredText)
	}
	if deliveredText == "" {
		t.Errorf("Expected delivery notification to be sent to user")
	}
}

func TestProcessBurst_Exit0_NonTransientJSON_FailsFastOnAttempt1(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	attempts := 0
	doneCh := make(chan struct{}, 1)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			return `{"status":"ERROR","error":"unknown flag: --unsupported-flag"}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-flag-fail", ThreadID: "thread-flag-fail", Content: "Run with bad flag"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message to fail fast")
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts != 1 {
		t.Errorf("Expected exactly 1 attempt for exit 0 non-transient error, got %d", attempts)
	}

	dbMsg, _ := db.GetMessage(database, "msg-flag-fail")
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected message status to be FAILED, got %s", dbMsg.Status)
	}
}

func TestProcessBurst_UnknownError_RetriesTransientByDefault(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	attempts := 0
	doneCh := make(chan struct{}, 1)

	validUUID := uuid.New().String()
	pbPath := filepath.Join(tmpDir, ".gemini", "antigravity", "conversations", validUUID+".pb")
	_ = os.MkdirAll(filepath.Dir(pbPath), 0755)
	_ = os.WriteFile(pbPath, []byte("protobuf-data"), 0644)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attempts++
			curr := attempts
			mu.Unlock()

			if curr < 3 {
				// Unclassified internal error defaults to transient
				return "", "some mysterious unexpected exit from child process", 1, fmt.Errorf("exit status 1")
			}
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Success on attempt 3!"}`, validUUID), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-unknown-retry", ThreadID: "thread-unknown-retry", Content: "Run unknown error"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message to complete")
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts != 3 {
		t.Errorf("Expected 3 attempts (transient-by-default retry), got %d", attempts)
	}

	dbMsg, _ := db.GetMessage(database, "msg-unknown-retry")
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected message status to be COMPLETED, got %s", dbMsg.Status)
	}
}

func TestProcessBurst_ColdStartContextWindow_FailsFast(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var mu sync.Mutex
	attempts := 0
	doneCh := make(chan struct{}, 1)

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			return `{"status":"ERROR","error":"maximum context length exceeded"}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-ctx-cold", ThreadID: "thread-ctx-cold", Content: "Massive prompt"}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for cold start context window fail-fast")
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts != 1 {
		t.Errorf("Expected exactly 1 attempt on cold start context window exceeded, got %d", attempts)
	}

	dbMsg, _ := db.GetMessage(database, "msg-ctx-cold")
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected message status to be FAILED, got %s", dbMsg.Status)
	}
}

func TestGetSessionLastActivity_ChecksBothDBAndDiskLogs(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	threadID := "thread-act-1"
	sessionID := "sess-act-uuid-1"

	// 1. Initially empty thread -> cold thread, zero time
	act, isCold, err := GetSessionLastActivity(database, threadID)
	if err != nil || !isCold || !act.IsZero() {
		t.Fatalf("expected cold thread with zero time, got act=%v, isCold=%v, err=%v", act, isCold, err)
	}

	// 2. Insert session with updated_at = 20m ago
	tSess := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Second)
	_, err = database.Exec(`
		INSERT INTO sessions (thread_id, internal_session_id, turn_count, updated_at)
		VALUES ($1, $2, $3, $4)
	`, threadID, sessionID, 1, tSess)
	if err != nil {
		t.Fatalf("failed to insert session: %v", err)
	}

	act, isCold, err = GetSessionLastActivity(database, threadID)
	if err != nil || isCold {
		t.Fatalf("expected non-cold thread, got act=%v, isCold=%v, err=%v", act, isCold, err)
	}
	if !act.Equal(tSess) {
		t.Errorf("expected session updated_at %v, got %v", tSess, act)
	}

	// 3. Insert completed message with updated_at = 10m ago (newer than session updated_at)
	tMsg := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	_, err = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-1', $1, 'COMPLETED', 'some answer', $2, $2)
	`, threadID, tMsg)
	if err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	act, isCold, err = GetSessionLastActivity(database, threadID)
	if err != nil || isCold {
		t.Fatalf("expected non-cold thread, got act=%v, isCold=%v, err=%v", act, isCold, err)
	}
	if !act.Equal(tMsg) {
		t.Errorf("expected msg updated_at %v, got %v", tMsg, act)
	}

	// 4. Create on-disk task log with timestamp = 2m ago (newer than DB)
	tasksDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessionID, ".system_generated", "tasks")
	if err := os.MkdirAll(tasksDir, 0755); err != nil {
		t.Fatalf("failed to create tasksDir: %v", err)
	}
	tDisk := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	taskLog := filepath.Join(tasksDir, "task-1.log")
	if err := os.WriteFile(taskLog, []byte("build output\n"), 0644); err != nil {
		t.Fatalf("failed to write taskLog: %v", err)
	}
	_ = os.Chtimes(taskLog, tDisk, tDisk)

	act, isCold, err = GetSessionLastActivity(database, threadID)
	if err != nil || isCold {
		t.Fatalf("expected non-cold thread, got act=%v, isCold=%v, err=%v", act, isCold, err)
	}
	if !act.Equal(tDisk) {
		t.Errorf("expected disk activity %v to win, got %v", tDisk, act)
	}
}

func TestGetSessionLastActivity_RotatedSessionNotNew(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-rotated-1"

	// Session has reached 15 turns and rotated: internal_session_id is "", turn_count is 0
	_, err = database.Exec(`
		INSERT INTO sessions (thread_id, internal_session_id, turn_count, updated_at)
		VALUES ($1, '', 0, $2)
	`, threadID, time.Now().UTC().Add(-15*time.Minute))
	if err != nil {
		t.Fatalf("failed to insert session: %v", err)
	}

	// But messages table has prior completed turn from 4 minutes ago
	tCompleted := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	_, err = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-prev', $1, 'COMPLETED', 'previous answer', $2, $2)
	`, threadID, tCompleted)
	if err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	act, isCold, err := GetSessionLastActivity(database, threadID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isCold {
		t.Errorf("expected isCold=false for rotated session with completed turns, got true")
	}
	if !act.Equal(tCompleted) {
		t.Errorf("expected lastActivity %v, got %v", tCompleted, act)
	}
}

func TestGetSessionLastActivity_IgnoresExpiredStaleSentinel(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-stale-sentinel"

	// Message completed with [EXPIRED_STALE]
	_, err = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-stale-drop', $1, 'COMPLETED', '[EXPIRED_STALE]', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, threadID)
	if err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	// Also insert ambient evaluated message
	_, err = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-ambient', $1, 'COMPLETED', '[AMBIENT: IGNORED]', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, threadID)
	if err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	// No real completed messages -> should be cold thread
	act, isCold, err := GetSessionLastActivity(database, threadID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isCold {
		t.Errorf("expected isCold=true when only stale/ambient messages exist, got false")
	}
	if !act.IsZero() {
		t.Errorf("expected zero activity time, got %v", act)
	}
}

func TestProcessBurst_Staleness_ExistingSessionRecentDiskActivity_Retained(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	threadID := "thread-burst-disk-active"
	sessionID := "sess-burst-disk-active"

	// Session registered in DB
	_ = db.SaveSessionID(database, threadID, sessionID)

	// Task log active 5m ago
	tasksDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessionID, ".system_generated", "tasks")
	_ = os.MkdirAll(tasksDir, 0755)
	tDisk := time.Now().UTC().Add(-5 * time.Minute)
	taskLog := filepath.Join(tasksDir, "task-1.log")
	_ = os.WriteFile(taskLog, []byte("running build"), 0644)
	_ = os.Chtimes(taskLog, tDisk, tDisk)

	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse(sessionID, "retained and executed"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Message created 35 minutes ago (> 30m default staleness TTL)
	msg := db.Message{
		ID:        "msg-old-retained",
		ThreadID:  threadID,
		AuthorID:  "user-1",
		Content:   "Please continue the task",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC().Add(-35 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 1 {
		t.Errorf("Expected 1 runner call for message in active session, got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-old-retained")
	if dbMsg.Status != db.StatusCompleted || dbMsg.ResponseText == "[EXPIRED_STALE]" {
		t.Errorf("Expected message to be completed normally, got status=%s, response=%s", dbMsg.Status, dbMsg.ResponseText)
	}
}

func TestProcessBurst_Staleness_ExistingSessionRecentDBActivity_Retained(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-burst-db-active"

	// Previous message completed 5 minutes ago
	tPrev := time.Now().UTC().Add(-5 * time.Minute)
	_, _ = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-prev-active', $1, 'COMPLETED', 'Finished earlier turn', $2, $2)
	`, threadID, tPrev)

	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("sess-new", "done"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Message created 35 minutes ago (> 30m TTL)
	msg := db.Message{
		ID:        "msg-db-active-retained",
		ThreadID:  threadID,
		AuthorID:  "user-1",
		Content:   "Next step please",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC().Add(-35 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 1 {
		t.Errorf("Expected 1 runner call for message with recent DB activity, got %d", calls)
	}
}

func TestProcessBurst_Staleness_ExistingSessionInactive_Dropped(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-burst-inactive"
	sessionID := "sess-burst-inactive"

	// Session registered, but updated_at is 45m ago
	tInactive := time.Now().UTC().Add(-45 * time.Minute)
	_, _ = database.Exec(`
		INSERT INTO sessions (thread_id, internal_session_id, turn_count, updated_at)
		VALUES ($1, $2, 1, $3)
	`, threadID, sessionID, tInactive)

	// Previous turn completed 45m ago
	_, _ = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-old-completed', $1, 'COMPLETED', 'old answer', $2, $2)
	`, threadID, tInactive)

	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse(sessionID, "should not execute"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Message created 35m ago (> 30m TTL) and session inactive for 45m (> 30m TTL)
	msg := db.Message{
		ID:        "msg-inactive-dropped",
		ThreadID:  threadID,
		AuthorID:  "user-1",
		Content:   "old prompt",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC().Add(-35 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for stale message drop")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 0 {
		t.Errorf("Expected 0 runner calls for inactive session, got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-inactive-dropped")
	if dbMsg.ResponseText != "[EXPIRED_STALE]" {
		t.Errorf("Expected [EXPIRED_STALE], got %s", dbMsg.ResponseText)
	}
}

func TestProcessBurst_Staleness_NewSession_Dropped(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-new-drop"
	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("sess-new", "should not run"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// New thread, message 31m old
	msg := db.Message{
		ID:        "msg-new-stale",
		ThreadID:  threadID,
		AuthorID:  "user-1",
		Content:   "brand new thread message",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC().Add(-31 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for new thread stale drop")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 0 {
		t.Errorf("Expected 0 runner calls, got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-new-stale")
	if dbMsg.ResponseText != "[EXPIRED_STALE]" {
		t.Errorf("Expected [EXPIRED_STALE], got %s", dbMsg.ResponseText)
	}
}

func TestGetSessionLastActivity_FailOpenOnDBError(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	_ = database.Close()

	// When DB is closed, GetSessionLastActivity returns error
	act, isCold, err := GetSessionLastActivity(database, "thread-closed")
	if err == nil {
		t.Fatalf("expected error from closed DB, got nil")
	}
	if isCold {
		t.Errorf("expected isCold=false on DB error, got true")
	}
	if !act.IsZero() {
		t.Errorf("expected zero activity time on DB error, got %v", act)
	}
}

func TestProcessBurst_Staleness_HardCeiling_Dropped(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-hard-ceiling"

	// Session is actively running right now (updated 1 minute ago)
	_, _ = database.Exec(`
		INSERT INTO sessions (thread_id, internal_session_id, turn_count, updated_at)
		VALUES ($1, 'sess-ceiling', 1, CURRENT_TIMESTAMP)
	`, threadID)
	_, _ = database.Exec(`
		INSERT INTO messages (id, thread_id, status, response_text, created_at, updated_at)
		VALUES ('msg-recent', $1, 'COMPLETED', 'recent turn', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, threadID)

	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("sess-ceiling", "should not run"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Message is 2 hours 10 minutes old (> MaxMessageAbsoluteAge)
	msg := db.Message{
		ID:        "msg-poison-pill",
		ThreadID:  threadID,
		AuthorID:  "user-1",
		Content:   "very old command that survived in queue",
		Status:    db.StatusPending,
		CreatedAt: time.Now().UTC().Add(-130 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for hard ceiling drop")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 0 {
		t.Errorf("Expected 0 runner calls for message older than MaxMessageAbsoluteAge, got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-poison-pill")
	if dbMsg.ResponseText != "[EXPIRED_STALE]" {
		t.Errorf("Expected [EXPIRED_STALE], got %s", dbMsg.ResponseText)
	}
}

func TestProcessBurst_Staleness_RecoveredMessage_Retained(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-recovered-msg"
	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("sess-rec", "recovered execution done"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Recovered message during restart has RetryCount > 0, age is 45m (> 30m)
	msg := db.Message{
		ID:         "msg-recovered-1",
		ThreadID:   threadID,
		AuthorID:   "user-1",
		Content:    "interrupted turn resumed",
		Status:     db.StatusPending,
		RetryCount: 1,
		CreatedAt:  time.Now().UTC().Add(-45 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for recovered message processing")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 1 {
		t.Errorf("Expected 1 runner call for recovered message with RetryCount > 0, got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-recovered-1")
	if dbMsg.ResponseText == "[EXPIRED_STALE]" {
		t.Errorf("Recovered message should NOT be marked [EXPIRED_STALE]")
	}
}

func TestRecoverInterrupted_PreservesExecutionRetryBudget(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-retry-budget"
	msg := db.Message{
		ID:           "msg-restarted-budget",
		ThreadID:     threadID,
		AuthorID:     "user-1",
		Content:      "turn interrupted by container restart",
		Status:       db.StatusProcessing,
		RestartCount: 2,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
		UpdatedAt:    time.Now().UTC().Add(-10 * time.Minute),
	}
	if err := db.InsertMessage(database, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}

	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			// Simulate runner failure (exit code 1) on every attempt
			return "", "transient connection error", 1, errors.New("exit status 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// RecoverInterrupted should reset status to PENDING and increment restart_count to 3, leaving retry_count at 0
	RecoverInterrupted(database, pool)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	// Crucial invariant: The turn must get all 3 execution attempts despite having restarted twice!
	if calls != 3 {
		t.Errorf("Expected 3 runner execution attempts (full retry budget), got %d", calls)
	}

	dbMsg, err := db.GetMessage(database, "msg-restarted-budget")
	if err != nil || dbMsg == nil {
		t.Fatalf("Failed to fetch message: %v", err)
	}
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected final status %s, got %s", db.StatusFailed, dbMsg.Status)
	}
	if dbMsg.RestartCount != 3 {
		t.Errorf("Expected final restart_count 3, got %d", dbMsg.RestartCount)
	}
	if dbMsg.RetryCount != 3 {
		t.Errorf("Expected final retry_count 3 (after 3 failed execution attempts), got %d", dbMsg.RetryCount)
	}
}

func TestRecoverInterrupted_RestartPoisonPill_Boundaries(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	// msgPoison has StatusProcessing and RestartCount >= DefaultMaxRestarts (3)
	msgPoison := db.Message{
		ID:           "msg-poison-restart",
		ThreadID:     "thread-poison-restart",
		AuthorID:     "user-1",
		Content:      "crash-looping container trigger",
		Status:       db.StatusProcessing,
		RestartCount: DefaultMaxRestarts,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-15 * time.Minute),
	}

	// msgValid has StatusProcessing and RestartCount < DefaultMaxRestarts (2)
	msgValid := db.Message{
		ID:           "msg-valid-restart",
		ThreadID:     "thread-valid-restart",
		AuthorID:     "user-2",
		Content:      "valid restart message",
		Status:       db.StatusProcessing,
		RestartCount: DefaultMaxRestarts - 1,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
	}

	_ = db.InsertMessage(database, msgPoison)
	_ = db.InsertMessage(database, msgValid)

	var mu sync.Mutex
	var deliveredNotifs []string
	var completedIDs []string
	var wg sync.WaitGroup
	wg.Add(1) // Only msgValid should be completed by worker pool

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("d1111111-2222-3333-4444-555555555555", "Processed OK"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredNotifs = append(deliveredNotifs, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() { return func() {} },
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
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
	pool.SetDiscordSession(&discordgo.Session{})

	RecoverInterrupted(database, pool)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Verify only msgValid was processed
	if len(completedIDs) != 1 || completedIDs[0] != "msg-valid-restart" {
		t.Errorf("Expected only msg-valid-restart to complete, got: %v", completedIDs)
	}

	// Verify msgPoison was dropped and marked FAILED with restart limit error
	poisonDB, err := db.GetMessage(database, "msg-poison-restart")
	if err != nil || poisonDB == nil {
		t.Fatalf("Failed to query poison message: %v", err)
	}
	if poisonDB.Status != db.StatusFailed {
		t.Errorf("Expected poison pill status FAILED, got: %s", poisonDB.Status)
	}
	if !strings.Contains(poisonDB.ErrorMessage, "exceeded restart limit") {
		t.Errorf("Expected error to mention exceeded restart limit, got: %s", poisonDB.ErrorMessage)
	}

	// Verify msgValid was successfully completed and restart_count incremented
	validDB, err := db.GetMessage(database, "msg-valid-restart")
	if err != nil || validDB == nil {
		t.Fatalf("Failed to query valid message: %v", err)
	}
	if validDB.Status != db.StatusCompleted {
		t.Errorf("Expected valid message status COMPLETED, got: %s", validDB.Status)
	}
	if validDB.RestartCount != DefaultMaxRestarts {
		t.Errorf("Expected valid message restart_count %d, got %d", DefaultMaxRestarts, validDB.RestartCount)
	}
}

func TestProcessBurst_Staleness_RestartedMessage_Retained(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-restarted-staleness"
	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("d2222222-2222-3333-4444-555555555555", "execution done"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Restarted message has RestartCount > 0, RetryCount == 0, age is 45m (> 30m)
	msg := db.Message{
		ID:           "msg-restarted-retained",
		ThreadID:     threadID,
		AuthorID:     "user-1",
		Content:      "turn interrupted by restart resumed",
		Status:       db.StatusPending,
		RestartCount: 1,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-45 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for restarted message processing")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 1 {
		t.Errorf("Expected 1 runner call for restarted message with RestartCount > 0, got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-restarted-retained")
	if dbMsg.ResponseText == "[EXPIRED_STALE]" {
		t.Errorf("Restarted message should NOT be marked [EXPIRED_STALE]")
	}
}

func TestProcessBurst_Staleness_RestartedMessage_HardCeiling_Dropped(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	threadID := "thread-restarted-hard-ceiling"
	var runnerCalls int
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		DB:             database,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("sess-restart-drop", "unexpected"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		MemoryRetrieverFunc: func(ctx context.Context, database any, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error) {
			return nil, nil
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	// Restarted message has RestartCount > 0, but age is 130m (> 2h MaxMessageAbsoluteAge)
	msg := db.Message{
		ID:           "msg-restarted-hard-ceiling",
		ThreadID:     threadID,
		AuthorID:     "user-1",
		Content:      "stuck across multiple restarts for over 2 hours",
		Status:       db.StatusPending,
		RestartCount: 2,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC().Add(-130 * time.Minute),
	}
	_ = db.InsertMessage(database, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message drop")
	}

	mu.Lock()
	calls := runnerCalls
	mu.Unlock()

	if calls != 0 {
		t.Errorf("Expected 0 runner calls for message exceeding MaxMessageAbsoluteAge (hard ceiling), got %d", calls)
	}

	dbMsg, _ := db.GetMessage(database, "msg-restarted-hard-ceiling")
	if dbMsg.ResponseText != "[EXPIRED_STALE]" {
		t.Errorf("Expected [EXPIRED_STALE], got %s", dbMsg.ResponseText)
	}
}

func TestWorkerPool_EffortRouting(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	var executedModels []string
	var mu sync.Mutex

	appCfg := config.NewFromData(&config.ConfigData{
		Model:          "claude-opus-4-6-thinking",
		LowEffortModel: "claude-sonnet-4-6-thinking",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})

	doneCh := make(chan struct{}, 2)

	pool := New(appCfg, WorkerPoolConfig{
		DB: database,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			executedModels = append(executedModels, model)
			mu.Unlock()
			return mockJSONResponse(sessionID, "Done!"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. High effort / default message
	runID1 := "run-high-1"
	_ = db.CreateScheduleRun(database, db.ScheduleRun{
		ID:           runID1,
		ScheduleID:   "cron-high",
		ScheduleType: "cron",
		MessageID:    "msg-high-1",
		TargetID:     "thread-high",
		ThreadID:     "thread-high",
		Title:        "High Routine",
		Prompt:       "Run high routine",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC(),
		Effort:       "high",
	})
	msgHigh := db.Message{
		ID:            "msg-high-1",
		ThreadID:      "thread-high",
		AuthorID:      "scheduler",
		AuthorName:    "Scheduler",
		Content:       "Run high routine",
		Status:        db.StatusPending,
		ScheduleRunID: runID1,
		Effort:        "high",
		CreatedAt:     time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msgHigh)
	pool.Enqueue(msgHigh)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for high effort message completion")
	}

	// 2. Low effort message
	runID2 := "run-low-1"
	_ = db.CreateScheduleRun(database, db.ScheduleRun{
		ID:           runID2,
		ScheduleID:   "cron-low",
		ScheduleType: "cron",
		MessageID:    "msg-low-1",
		TargetID:     "thread-low",
		ThreadID:     "thread-low",
		Title:        "Low Routine",
		Prompt:       "Run low routine",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC(),
		Effort:       "low",
	})
	msgLow := db.Message{
		ID:            "msg-low-1",
		ThreadID:      "thread-low",
		AuthorID:      "scheduler",
		AuthorName:    "Scheduler",
		Content:       "Run low routine",
		Status:        db.StatusPending,
		ScheduleRunID: runID2,
		Effort:        "low",
		CreatedAt:     time.Now().UTC(),
	}
	_ = db.InsertMessage(database, msgLow)
	pool.Enqueue(msgLow)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for low effort message completion")
	}

	mu.Lock()
	models := append([]string{}, executedModels...)
	mu.Unlock()

	if len(models) != 2 {
		t.Fatalf("Expected 2 runner invocations, got %d (%v)", len(models), models)
	}
	if models[0] != "claude-opus-4-6-thinking" {
		t.Errorf("Expected first run with high effort model 'claude-opus-4-6-thinking', got %q", models[0])
	}
	if models[1] != "claude-sonnet-4-6-thinking" {
		t.Errorf("Expected second run with low effort model 'claude-sonnet-4-6-thinking', got %q", models[1])
	}

	// Verify schedule run records recorded the executed model
	runsHigh, _, _ := db.GetScheduleRunsPaginated(database, 10, 0, "cron-high", "")
	if len(runsHigh) != 1 || runsHigh[0].Model != "claude-opus-4-6-thinking" {
		t.Errorf("Expected schedule run high model to be 'claude-opus-4-6-thinking', got %+v", runsHigh)
	}
	runsLow, _, _ := db.GetScheduleRunsPaginated(database, 10, 0, "cron-low", "")
	if len(runsLow) != 1 || runsLow[0].Model != "claude-sonnet-4-6-thinking" {
		t.Errorf("Expected schedule run low model to be 'claude-sonnet-4-6-thinking', got %+v", runsLow)
	}
}

func TestProcessBurst_RecordsTokensTelemetry(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	customModel := "test-token-model"
	mockOutput := `{"conversation_id":"c1111111-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":150,"output_tokens":75,"thinking_tokens":30,"cache_read_tokens":10,"total_tokens":265}}`

	pool := New(nil, WorkerPoolConfig{
		DB:    database,
		Model: customModel,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockOutput, "", 0, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	threadID := "chan-token-test-1"
	t0 := time.Now().UTC()
	msg := db.Message{
		ID:        "msg-token-1",
		ThreadID:  threadID,
		Content:   "Aerial Question Token Test",
		Status:    db.StatusPending,
		CreatedAt: t0,
		UpdatedAt: t0,
	}
	_ = db.InsertMessage(database, msg)

	pool.processBurst([]db.Message{msg})

	// Query metrics registry
	handler := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `aerial_brain_tokens_total{model="test-token-model",type="total"} 265`) {
		t.Errorf("expected total tokens 265 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{model="test-token-model",type="input"} 150`) {
		t.Errorf("expected input tokens 150 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{model="test-token-model",type="output"} 75`) {
		t.Errorf("expected output tokens 75 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{model="test-token-model",type="thinking"} 30`) {
		t.Errorf("expected thinking tokens 30 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{model="test-token-model",type="cache_read"} 10`) {
		t.Errorf("expected cache_read tokens 10 for test-token-model, got metrics body: %s", body)
	}
}

