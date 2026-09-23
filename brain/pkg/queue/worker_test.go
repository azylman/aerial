package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
)

func TestWorker_PersistentDaemonYieldTrapAutoResume(t *testing.T) {
	tempDir := t.TempDir()
	taskDir := filepath.Join(tempDir, ".system_generated", "tasks")
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		t.Fatalf("failed to create taskDir: %v", err)
	}

	logFile := filepath.Join(taskDir, "task-test-1.log")
	if err := os.WriteFile(logFile, []byte("Task output\n"), 0644); err != nil {
		t.Fatalf("failed to write logFile: %v", err)
	}

	exitCode, logPath, err := watchTaskCompletion(context.Background(), tempDir, "task-test-1")
	if err != nil {
		t.Fatalf("watchTaskCompletion failed: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d", exitCode)
	}
	if logPath != logFile {
		t.Errorf("expected log path %s, got %s", logFile, logPath)
	}
}

func TestWorker_WatchTaskCompletion_ContextTimeout(t *testing.T) {
	tempDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	code, _, err := watchTaskCompletion(ctx, tempDir, "non-existent-task")
	if err == nil {
		t.Errorf("expected context error on non-existent task")
	}
	if code != -1 {
		t.Errorf("expected exit code -1 on timeout, got %d", code)
	}
}

func TestWorker_WatchTaskCompletion_TickerTrigger(t *testing.T) {
	tempDir := t.TempDir()
	taskDir := filepath.Join(tempDir, ".system_generated", "tasks")
	_ = os.MkdirAll(taskDir, 0755)

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(50 * time.Millisecond)
		logFile := filepath.Join(taskDir, "delayed-task.log")
		_ = os.WriteFile(logFile, []byte("finished"), 0644)
	}()

	code, logPath, err := watchTaskCompletion(context.Background(), tempDir, "delayed-task")
	if err != nil {
		t.Fatalf("watchTaskCompletion failed: %v", err)
	}
	if code != 0 || filepath.Base(logPath) != "delayed-task.log" {
		t.Errorf("unexpected result: code=%d, path=%s", code, logPath)
	}
	<-done
}

func TestWorker_PoolDaemonAccessors(t *testing.T) {
	cfg := config.NewTestConfig()
	pool := NewWorkerPool(WorkerPoolConfig{})
	defer pool.Stop()

	if pool.DaemonPool() == nil {
		t.Errorf("expected non-nil DaemonPool from pool.DaemonPool()")
	}

	var nilPool *WorkerPool
	if nilPool.DaemonPool() != nil {
		t.Errorf("expected nil DaemonPool for nil WorkerPool")
	}

	_ = cfg
}

func TestWorker_PersistentDaemonTurnExecution(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	script := `#!/bin/sh
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"Executed via persistent daemon","usage":{"total_tokens":10}}}'
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	threadID := "thread-persist-test"
	sessID := "a1111111-2222-3333-4444-555555555555"
	_ = store.SaveSessionID(context.Background(), threadID, sessID)
	sessDir := filepath.Join(tempData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	msg := db.Message{
		ID:        "msg-persist-1",
		ThreadID:  threadID,
		Content:   "Hello daemon",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
		// success
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}

	if pool.DaemonPool() == nil || !pool.DaemonPool().HasDaemon(threadID) {
		t.Errorf("expected daemon to exist for thread %s in pool", threadID)
	}
}

func TestWorker_ThreadWorker_ActiveTasksPreventReap(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	script := `#!/bin/sh
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"ok","usage":{"total_tokens":5}}}'
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	threadID := "thread-active-tasks-reap"
	sessID := "b1111111-2222-3333-4444-555555555555"
	_ = store.SaveSessionID(context.Background(), threadID, sessID)
	sessDir := filepath.Join(tempData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		MaxAttempts:          1,
		IdleTimeout:          20 * time.Millisecond,
		OnMessageCompleted: func(msg db.Message, status string) {
			select {
			case <-doneCh:
			default:
				close(doneCh)
			}
		},
	})
	defer pool.Stop()

	// Pre-create daemon with active task
	ctx := context.Background()
	daemon, err := pool.DaemonPool().GetOrCreateDaemon(ctx, threadID, sessID, "default-model")
	if err != nil {
		t.Fatalf("failed to create daemon: %v", err)
	}
	daemon.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "task-busy-prevent-reap",
		StartedAt: time.Now(),
	})

	msg := db.Message{
		ID:        "msg-reap-1",
		ThreadID:  threadID,
		Content:   "Hello daemon reap test",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}

	// Verify worker remains active because background task is still running
	time.Sleep(60 * time.Millisecond)
	pool.mu.Lock()
	_, workerStillActive := pool.threadChs[threadID]
	pool.mu.Unlock()

	if !workerStillActive {
		t.Errorf("expected worker to remain active because background task was running")
	}

	// Remove active task and poll for worker reap
	daemon.TaskTracker().Remove("task-busy-prevent-reap")
	reaped := false
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		pool.mu.Lock()
		_, active := pool.threadChs[threadID]
		pool.mu.Unlock()
		if !active {
			reaped = true
			break
		}
	}
	if !reaped {
		t.Errorf("expected worker to be reaped after background task finished")
	}
}

func TestWorker_PersistentDaemonExecutionErrors(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	// 1. Test daemon spawn error (invalid binary)
	cfgBadBin := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = "/invalid/nonexistent/bin/path"
		d.DataDir = tempData
	})

	failedCh := make(chan struct{})
	poolBadBin := New(cfgBadBin, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		MaxAttempts:          1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusFailed {
				select {
				case <-failedCh:
				default:
					close(failedCh)
				}
			}
		},
	})
	defer poolBadBin.Stop()

	threadBad := "thread-bad-daemon"
	sessBad := "c1111111-2222-3333-4444-555555555555"
	_ = store.SaveSessionID(context.Background(), threadBad, sessBad)
	msgBad := db.Message{
		ID:        "msg-bad-1",
		ThreadID:  threadBad,
		Content:   "fail",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msgBad)
	poolBadBin.Enqueue(msgBad)

	select {
	case <-failedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for bad daemon turn failure")
	}

	// 2. Test turn execution error (script exits immediately)
	mockBin := filepath.Join(tmpHome, "mock_agy_err.sh")
	script := `#!/bin/sh
exit 1
`
	_ = os.WriteFile(mockBin, []byte(script), 0755)

	cfgErr := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	failedTurnCh := make(chan struct{})
	poolErr := New(cfgErr, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		MaxAttempts:          1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusFailed {
				select {
				case <-failedTurnCh:
				default:
					close(failedTurnCh)
				}
			}
		},
	})
	defer poolErr.Stop()

	threadErr := "thread-err-daemon"
	sessErr := "d1111111-2222-3333-4444-555555555555"
	_ = store.SaveSessionID(context.Background(), threadErr, sessErr)
	msgErr := db.Message{
		ID:        "msg-err-1",
		ThreadID:  threadErr,
		Content:   "fail turn",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msgErr)
	poolErr.Enqueue(msgErr)

	select {
	case <-failedTurnCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for turn error failure")
	}
}

func TestWorkerPool_PersistentDaemon_ColdStart_Success(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "c1111111-2222-3333-4444-555555555555"
	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	script := fmt.Sprintf(`#!/bin/sh
echo '{"event":"init","conversation_id":%q}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"Cold start completed successfully!","usage":{"total_tokens":25}}}'
done
`, validUUID)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error {
			deliveredText = content
			return nil
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	threadID := "thread-cold-persist-success"
	// Ensure NO session is pre-saved in DB (clean cold start)
	msg := db.Message{
		ID:        "msg-cold-1",
		ThreadID:  threadID,
		Content:   "cold start hello",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for cold start message completion")
	}

	savedSess, err := store.GetSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to get session id: %v", err)
	}
	if savedSess != validUUID {
		t.Errorf("expected session synchronized to %q, got %q", validUUID, savedSess)
	}
	if !strings.Contains(deliveredText, "Cold start completed successfully!") {
		t.Errorf("expected delivered text to contain completion response, got: %q", deliveredText)
	}
}

func TestWorkerPool_PersistentDaemon_ColdStart_SessionMgrFallback(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	diskUUID := "f1111111-2222-3333-4444-555555555555"
	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	script := `#!/bin/sh
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"Turn completed with fallback session","usage":{"total_tokens":15}}}'
done
`
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error {
			deliveredText = content
			return nil
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	// Pre-create session directory in session roots after execStart
	threadID := "thread-cold-persist-fallback"
	msg := db.Message{
		ID:        "msg-cold-fallback",
		ThreadID:  threadID,
		Content:   "cold start fallback",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)

	// Create disk session dir with a future modtime so FindLatestSessionDir picks it up
	brainDir := filepath.Join(tempData, "brain", diskUUID)
	_ = os.MkdirAll(brainDir, 0755)
	futureTime := time.Now().Add(5 * time.Second)
	_ = os.Chtimes(brainDir, futureTime, futureTime)

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for fallback session message completion")
	}

	savedSess, err := store.GetSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to get session id: %v", err)
	}
	if savedSess != diskUUID {
		t.Errorf("expected session synchronized to diskUUID %q, got %q", diskUUID, savedSess)
	}
	if !strings.Contains(deliveredText, "Turn completed with fallback session") {
		t.Errorf("expected delivered text to contain completion response, got: %q", deliveredText)
	}
}

func TestWorkerPool_EmptyResponse_TranscriptFallback(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "e1111111-2222-3333-4444-555555555555"
	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	// Runner returns empty response in result event
	script := fmt.Sprintf(`#!/bin/sh
echo '{"event":"init","conversation_id":%q}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":10}}}'
done
`, validUUID)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	// Prepare transcript on disk with substantive text
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	transcriptLine := `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered from transcript!"}` + "\n"
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptLine), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error {
			deliveredText = content
			return nil
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	threadID := "thread-empty-resp-fallback"
	msg := db.Message{
		ID:        "msg-empty-resp",
		ThreadID:  threadID,
		Content:   "test empty resp",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}

	if !strings.Contains(deliveredText, "Recovered from transcript!") {
		t.Errorf("expected delivered text to contain recovered transcript content, got: %q", deliveredText)
	}
}

func TestWorkerPool_QuotaExhaustion_FromTranscript(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "f2222222-3333-4444-5555-666666666666"
	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	// Runner returns empty response with exit 0 (emulating agy daemon on 429 quota exhaustion)
	script := fmt.Sprintf(`#!/bin/sh
echo '{"event":"init","conversation_id":%q}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":0}}}'
done
`, validUUID)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	// Prepare transcript on disk with Gemini 429 quota exhaustion ERROR_MESSAGE
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	tNow := time.Now().UTC()
	transcriptLine := fmt.Sprintf(`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 4h51m59s.","created_at":%q}`+"\n", tNow.Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptLine), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error {
			deliveredText = content
			return nil
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusFailed {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	threadID := "thread-quota-exhaustion-test"
	msg := db.Message{
		ID:        "msg-quota-429",
		ThreadID:  threadID,
		Content:   "are you alive",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for quota failure completion")
	}

	// 1. Verify message in DB is marked FAILED with [QUOTA_PAUSED]
	dbMsg, err := store.GetMessage(context.Background(), "msg-quota-429")
	if err != nil || dbMsg == nil {
		t.Fatalf("failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("expected message status %s, got %s", db.StatusFailed, dbMsg.Status)
	}
	if !strings.Contains(dbMsg.ErrorMessage, "[QUOTA_PAUSED") {
		t.Errorf("expected error to contain [QUOTA_PAUSED], got %q", dbMsg.ErrorMessage)
	}

	// 2. Verify DeliveryFunc received quota pause notice
	if !strings.Contains(deliveredText, "Gemini API quota") && !strings.Contains(deliveredText, "Paused") && !strings.Contains(deliveredText, "resets in") {
		t.Errorf("expected delivery text to announce quota pause, got: %q", deliveredText)
	}

	// 3. Verify one-shot retry schedule was created
	schedules, err := store.GetAllOneShotSchedules(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to list one-shot schedules: %v", err)
	}
	if len(schedules) == 0 {
		t.Errorf("expected at least 1 one-shot retry schedule, got 0")
	} else {
		found := false
		for _, s := range schedules {
			if s.ThreadID == threadID && strings.Contains(s.Prompt, "[QUOTA_RETRY]") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected one-shot retry schedule for thread %s with [QUOTA_RETRY]", threadID)
		}
	}
}

func TestWorkerPool_QuotaPause_RotatesBloatedSession(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "b3333333-4444-5555-6666-777777777777"
	mockBin := filepath.Join(tmpHome, "mock_agy.sh")
	script := fmt.Sprintf(`#!/bin/sh
echo '{"event":"init","conversation_id":%q}'
while IFS= read -r line; do
  echo '{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":0}}}'
done
`, validUUID)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	// Prepare transcript on disk with step_index >= DefaultMaxSessionSteps and quota exhaustion error
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	tNow := time.Now().UTC()
	transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2h30m.","created_at":%q}
`, DefaultMaxSessionSteps, tNow.Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.AgyBin = mockBin
		d.DataDir = tempData
	})

	threadID := "thread-quota-bloat-rotation"
	_ = store.SaveSessionID(context.Background(), threadID, validUUID)

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error {
			return nil
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusFailed {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	msg := db.Message{
		ID:        "msg-quota-bloated",
		ThreadID:  threadID,
		Content:   "run big workflow",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for quota failure completion")
	}

	// Session should be rotated to cold state "" because steps >= DefaultMaxSessionSteps
	currSess, err := store.GetSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to query session ID: %v", err)
	}
	if currSess != "" {
		t.Errorf("expected session ID to be rotated to empty \"\", got %q", currSess)
	}

	prevSess, err := store.GetPreviousSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to query previous session ID: %v", err)
	}
	if prevSess != validUUID {
		t.Errorf("expected previous session ID to be %q, got %q", validUUID, prevSess)
	}
}


