package queue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
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
	mockSpawner := runner.NewMockDaemonSpawner()
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	pool := NewWorkerPool(WorkerPoolConfig{
		ProcessPool: procPool,
	})
	defer pool.Stop()

	if pool.ProcessPool() == nil {
		t.Errorf("expected non-nil ProcessPool from pool.ProcessPool()")
	}

	var nilPool *WorkerPool
	if nilPool.ProcessPool() != nil {
		t.Errorf("expected nil ProcessPool for nil WorkerPool")
	}
}

func TestWorker_PersistentDaemonTurnExecution(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"a1111111-2222-3333-4444-555555555555\"}\n"))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Executed via persistent daemon\",\"usage\":{\"total_tokens\":10}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
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

	if pool.ProcessPool() == nil || !pool.ProcessPool().HasDaemon(threadID) {
		t.Errorf("expected daemon to exist for thread %s in pool", threadID)
	}

	// Verify runner execution and latency metrics recorded with source="discord"
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	metricsBody := rec.Body.String()
	if !strings.Contains(metricsBody, `source="discord"`) {
		t.Errorf("expected metrics to contain source=discord, got:\n%s", metricsBody)
	}
	if !strings.Contains(metricsBody, `aerial_brain_runner_executions_total{`) || !strings.Contains(metricsBody, `source="discord"`) {
		t.Errorf("expected runner executions metric with source=discord, got:\n%s", metricsBody)
	}
	if !strings.Contains(metricsBody, `aerial_brain_runner_duration_seconds_bucket{`) || !strings.Contains(metricsBody, `source="discord"`) {
		t.Errorf("expected runner duration bucket metric with source=discord, got:\n%s", metricsBody)
	}
}

func TestWorker_ThreadWorker_ActiveTasksPreventReap(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"b1111111-2222-3333-4444-555555555555\"}\n"))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok\",\"usage\":{\"total_tokens\":5}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
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
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		MaxAttempts:    1,
		IdleTimeout:    20 * time.Millisecond,
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
	daemon, err := pool.ProcessPool().GetOrCreate(ctx, threadID)
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
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		MaxAttempts:    1,
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
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		MaxAttempts:    1,
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
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", validUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Cold start completed successfully!\",\"usage\":{\"total_tokens\":25}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
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

func TestWorkerPool_PersistentDaemon_ColdStart_RejectsForeignSessionOnDisk(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	daemonUUID := "d1111111-2222-3333-4444-555555555555"
	foreignUUID := "f1111111-2222-3333-4444-555555555555"
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", daemonUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Turn completed\",\"usage\":{\"total_tokens\":15}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
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

	// Pre-create foreign session directory on disk with future modtime (simulating concurrent cron/thread session)
	threadID := "thread-cold-persist-no-foreign"
	msg := db.Message{
		ID:        "msg-cold-no-foreign",
		ThreadID:  threadID,
		Content:   "cold start with foreign session on disk",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)

	brainDir := filepath.Join(tempData, "brain", foreignUUID)
	_ = os.MkdirAll(brainDir, 0755)
	futureTime := time.Now().Add(5 * time.Second)
	_ = os.Chtimes(brainDir, futureTime, futureTime)

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}

	savedSess, err := store.GetSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to get session id: %v", err)
	}
	if savedSess == foreignUUID {
		t.Fatalf("security violation: worker latched newer foreign session %q from disk instead of daemon session", foreignUUID)
	}
	if savedSess != daemonUUID {
		t.Errorf("expected session synchronized to daemonUUID %q, got %q", daemonUUID, savedSess)
	}
	if !strings.Contains(deliveredText, "Turn completed") {
		t.Errorf("expected delivered text to contain completion response, got: %q", deliveredText)
	}
}

func TestWorkerPool_EmptyResponse_TranscriptFallback(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "e1111111-2222-3333-4444-555555555555"
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", validUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"\",\"usage\":{\"total_tokens\":10}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	// Prepare transcript on disk with substantive text
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	transcriptLine := `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered from transcript!"}` + "\n"
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptLine), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
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

func TestWorkerPool_DaemonEmptyResponse_TranscriptFallback(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "e3333333-4444-5555-6666-777777777777"
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", validUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"step_update\",\"step_update\":{\"step_index\":1,\"step_type\":\"tool\",\"tool_name\":\"run_command\"}}\n"))
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"\"}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	// Prepare transcript on disk with substantive text
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	transcriptLine := `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered after daemon empty response!"}` + "\n"
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptLine), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
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

	threadID := "thread-daemon-empty-fallback"

	msg := db.Message{
		ID:        "msg-daemon-empty",
		ThreadID:  threadID,
		Content:   "test daemon empty",
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

	if !strings.Contains(deliveredText, "Recovered after daemon empty response!") {
		t.Errorf("expected delivered text to contain recovered transcript content, got: %q", deliveredText)
	}
}

func TestWorkerPool_QuotaExhaustion_FromTranscript(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "f2222222-3333-4444-5555-666666666666"
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", validUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"\",\"usage\":{\"total_tokens\":0}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	// Prepare transcript on disk with Gemini 429 quota exhaustion ERROR_MESSAGE
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	tNow := time.Now().UTC()
	transcriptLine := fmt.Sprintf(`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 4h51m59s.","created_at":%q}`+"\n", tNow.Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptLine), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	var deliveredText string
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
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
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", validUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"\",\"usage\":{\"total_tokens\":0}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	// Prepare transcript on disk with step_index >= DefaultMaxSessionSteps and quota exhaustion error
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	tNow := time.Now().UTC()
	transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2h30m.","created_at":%q}
`, DefaultMaxSessionSteps, tNow.Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	threadID := "thread-quota-bloat-rotation"
	_ = store.SaveSessionID(context.Background(), threadID, validUUID)

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
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

func TestWorkerPool_TransientRetry_RotatesBloatedSession(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	validUUID := "c4444444-5555-6666-7777-888888888888"

	// Prepare transcript on disk with initial steps below guardrail (e.g. 100 < 180)
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	tNow := time.Now().UTC()
	transcriptFile := filepath.Join(sessDir, "transcript.jsonl")
	transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":100,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"working...","created_at":%q}
`, tNow.Format(time.RFC3339))
	_ = os.WriteFile(transcriptFile, []byte(transcriptContent), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	threadID := "thread-transient-bloat-rotation"
	_ = store.SaveSessionID(context.Background(), threadID, validUUID)

	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := New(cfg, WorkerPoolConfig{
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		MaxAttempts:    2,
		BackoffBase:    10 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			sessionsSeen = append(sessionsSeen, sessionID)
			attemptNum := len(sessionsSeen)
			mu.Unlock()

			if attemptNum == 1 {
				// During attempt 1, the session executes steps that exceed the guardrail
				bloatedContent := fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"tool runaway","created_at":%q}`+"\n",
					DefaultMaxSessionSteps+5, time.Now().UTC().Format(time.RFC3339))
				f, _ := os.OpenFile(transcriptFile, os.O_APPEND|os.O_WRONLY, 0644)
				if f != nil {
					_, _ = f.WriteString(bloatedContent)
					_ = f.Close()
				}
				return "", fmt.Sprintf("Starting conversation update stream for %s\nError 503: high demand service unavailable", validUUID), 1, fmt.Errorf("exit status 1")
			}
			return mockJSONResponse(uuid.New().String(), "Recovered successfully"), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error {
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

	msg := db.Message{
		ID:        "msg-transient-bloated",
		ThreadID:  threadID,
		Content:   "run transient test",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for transient recovery completion")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sessionsSeen) != 2 {
		t.Fatalf("expected 2 runner attempts, got %d", len(sessionsSeen))
	}
	if sessionsSeen[0] != validUUID {
		t.Errorf("expected attempt 1 to use session %q, got %q", validUUID, sessionsSeen[0])
	}
	if sessionsSeen[1] != "" {
		t.Errorf("expected attempt 2 to be cold retry (empty session ID) due to guardrail rotation, got %q", sessionsSeen[1])
	}

	prevSess, err := store.GetPreviousSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to query previous session ID: %v", err)
	}
	if prevSess != validUUID {
		t.Errorf("expected previous session ID to be %q, got %q", validUUID, prevSess)
	}
}

func TestTurnExecution_GetRecentThreadMessages_Discord(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	t1 := now.Add(-10 * time.Minute)
	t2 := now.Add(-5 * time.Minute)

	mockDiscordMsgs := []*discordgo.Message{
		nil, // dm == nil test
		{
			ID:        "1552732003076669002",
			ChannelID: "1552732003076669460",
			Content:   "Can we add the SVG icon?",
			Timestamp: t2,
			Author: &discordgo.User{
				ID:       "169260920550195200",
				Username: "arcane103",
			},
			Mentions: []*discordgo.User{
				{ID: "bot-123", Username: "Aerial"},
			},
			ReferencedMessage: &discordgo.Message{
				Author: &discordgo.User{
					Username: "Aerial",
				},
			},
		},
		{
			ID:        "1552732003076669001",
			ChannelID: "1552732003076669460",
			Content:   "Weather widget looks good",
			Timestamp: t1,
			Author: &discordgo.User{
				ID:       "169260920550195200",
				Username: "arcane103",
			},
		},
		{
			ID:        "1552732003076669000",
			ChannelID: "1552732003076669460",
			Content:   "Webhook notification",
			Timestamp: time.Time{}, // zero timestamp to test SnowflakeTimestamp fallback
			WebhookID: "hook-123",
		},
		{
			ID:        "1552732003076669099",
			ChannelID: "1552732003076669460",
			Content:   "Current burst message to filter",
			Timestamp: now,
		},
	}

	dg, err := discordgo.New("Bot test-token")
	if err != nil {
		t.Fatalf("discordgo.New failed: %v", err)
	}
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(mockDiscordMsgs)
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})

	pool := &WorkerPool{
		cfg: WorkerPoolConfig{
			DiscordSession: dg,
		},
	}
	te := &turnExecution{
		pool: pool,
		burst: []db.Message{
			{ID: "1552732003076669099"},
		},
	}

	msgs, err := te.getRecentThreadMessages("1552732003076669460", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 1 nil skipped, 1 burst skipped, 3 valid messages returned
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	// Verified chronological order: oldest (1552732003076669000) first, newest (1552732003076669002) last
	if msgs[0].ID != "1552732003076669000" {
		t.Errorf("expected oldest message first, got %s", msgs[0].ID)
	}
	if msgs[0].AuthorName != "Webhook" {
		t.Errorf("expected Webhook author name, got %s", msgs[0].AuthorName)
	}
	if msgs[2].ID != "1552732003076669002" {
		t.Errorf("expected newest message last, got %s", msgs[2].ID)
	}
	if msgs[2].Metadata.ReplyingToAuthor != "@Aerial" {
		t.Errorf("expected replying to @Aerial, got %s", msgs[2].Metadata.ReplyingToAuthor)
	}
	if len(msgs[2].Metadata.Mentions) != 1 || msgs[2].Metadata.Mentions[0] != "Aerial" {
		t.Errorf("expected mention of Aerial, got %+v", msgs[2].Metadata.Mentions)
	}
}

func TestTurnExecution_GetRecentThreadMessages_FallbackFiltered(t *testing.T) {
	t.Parallel()
	fakeStore := db.NewFakeStore()
	ctx := context.Background()

	// Insert normal user message
	_ = fakeStore.InsertMessage(ctx, db.Message{
		ID:         "msg-1",
		ThreadID:   "1552732003076669460",
		AuthorName: "arcane103",
		Content:    "Normal chat message",
		CreatedAt:  time.Now().Add(-2 * time.Minute),
	})
	// Insert scheduler prompt injection message
	_ = fakeStore.InsertMessage(ctx, db.Message{
		ID:            "msg-sched",
		ThreadID:      "1552732003076669460",
		AuthorName:    "Scheduler",
		Content:       "Check on PR status and deployment prompt injection...",
		ScheduleRunID: "run-123",
		CreatedAt:     time.Now().Add(-1 * time.Minute),
	})

	// Discord session errors, triggering database fallback
	dg, _ := discordgo.New("Bot test")
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("discord simulated network error")
	})

	pool := &WorkerPool{
		cfg: WorkerPoolConfig{
			DiscordSession: dg,
			Store:          fakeStore,
		},
	}
	te := &turnExecution{pool: pool}

	msgs, err := te.getRecentThreadMessages("1552732003076669460", 0) // limit 0 -> defaults to 10
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (scheduler filtered out), got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].ID != "msg-1" {
		t.Errorf("expected msg-1, got %s", msgs[0].ID)
	}

	// Test limit clamp > 100
	msgsClamped, err := te.getRecentThreadMessages("1552732003076669460", 200)
	if err != nil {
		t.Fatalf("unexpected error on clamped limit: %v", err)
	}
	if len(msgsClamped) != 1 {
		t.Errorf("expected 1 message, got %d", len(msgsClamped))
	}

	// Test DB store error branch
	errStore := &mockErrRecentStore{}
	teErr := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{Store: errStore}}}
	if _, err := teErr.getRecentThreadMessages("1552732003076669460", 10); err == nil {
		t.Errorf("expected error on store failure")
	}

	// Test nil pool and nil store returns nil, nil
	teNil := &turnExecution{}
	if msgs, err := teNil.getRecentThreadMessages("1552732003076669460", 10); msgs != nil || err != nil {
		t.Errorf("expected nil, nil on nil store, got %v, %v", msgs, err)
	}
}

type mockErrRecentStore struct {
	db.Store
}

func (m *mockErrRecentStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]db.Message, error) {
	return nil, errors.New("simulated db failure")
}

func TestWorker_RotateSessionID_EvictsDaemons(t *testing.T) {
	t.Parallel()
	fakeStore := setupTestStore(t)
	mockSpawner := runner.NewMockDaemonSpawner()
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		if err := procPool.Close(); err != nil {
			t.Logf("cleanup unified pool: %v", err)
		}
	}()

	ctx := context.Background()
	_, err := procPool.GetOrCreate(ctx, "thread-evict-1")
	if err != nil {
		t.Fatalf("failed to create daemon: %v", err)
	}

	pool := &WorkerPool{
		cfg: WorkerPoolConfig{
			Store: fakeStore,
		},
		processPool: procPool,
	}
	te := &turnExecution{
		pool:     pool,
		threadID: "thread-evict-1",
	}

	// 1. When newSessionID != "", daemons are NOT evicted/rotated
	te.rotateSessionID("thread-evict-1", "new-sess-1")
	if !procPool.HasDaemon("thread-evict-1") {
		t.Errorf("expected daemon to remain when newSessionID is non-empty")
	}

	// 2. When newSessionID == "", daemon IS rotated
	oldD, _ := procPool.Get("thread-evict-1")
	te.rotateSessionID("thread-evict-1", "")
	newD, _ := procPool.Get("thread-evict-1")
	if oldD == newD {
		t.Errorf("expected daemon instance to change upon rotation")
	}

	// 3. Nil pool / nil te safe handling
	teNil := &turnExecution{threadID: "thread-evict-2"}
	teNil.rotateSessionID("thread-evict-2", "")
	var teNull *turnExecution
	teNull.rotateSessionID("thread-evict-3", "")
}

func TestWorker_SessionDBRotation_PreTurn(t *testing.T) {
	t.Parallel()

	// Channel mode pre-flight DB rotation
	t.Run("ChannelMode_PreFlight_DBLimit", func(t *testing.T) {
		t.Parallel()
		store := setupTestStore(t)
		tmpDir := t.TempDir()
		sessMgr := session.New(tmpDir, tmpDir)

		oldSess := "sess-chan-db-rot"
		convDir := filepath.Join(tmpDir, "conversations")
		if err := os.MkdirAll(convDir, 0755); err != nil {
			t.Fatalf("failed to create convDir: %v", err)
		}
		// Write .db alone (1.6 MB) >= DefaultMaxSessionDBBytes (1.5 MB)
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, 1600*1024), 0644)
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db-wal"), make([]byte, 600*1024), 0644)

		_ = saveSessionID(store, "chan-db-thread", oldSess)
		_, _ = incrementSessionTurnCount(store, "chan-db-thread")

		var mu sync.Mutex
		var gotSessionID string
		doneCh := make(chan struct{})

		appCfg := config.NewFromData(&config.ConfigData{
			Channels: map[string]config.ChannelPolicy{
				"default": {Mode: "channel"},
			},
		})

		pool := New(appCfg, WorkerPoolConfig{
			SessionManager: sessMgr,
			Store:          store,
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

		msg := db.Message{ID: "msg-chan-db", ThreadID: "chan-db-thread", Content: "<@aerial> hello"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for message")
		}

		mu.Lock()
		if gotSessionID != "" {
			t.Errorf("Expected cold start (empty session ID) on DB size limit, got: %q", gotSessionID)
		}
		mu.Unlock()

		prevSess, _ := getPreviousSessionID(store, "chan-db-thread")
		if prevSess != oldSess {
			t.Errorf("Expected previous session ID %q in store, got %q", oldSess, prevSess)
		}
	})

	// Channel mode pre-flight WAL ignored (does not rotate when DB alone is below limit)
	t.Run("ChannelMode_PreFlight_DBLimit_WALIgnored", func(t *testing.T) {
		t.Parallel()
		store := setupTestStore(t)
		tmpDir := t.TempDir()
		sessMgr := session.New(tmpDir, tmpDir)

		oldSess := "sess-chan-db-wal"
		convDir := filepath.Join(tmpDir, "conversations")
		if err := os.MkdirAll(convDir, 0755); err != nil {
			t.Fatalf("failed to create convDir: %v", err)
		}
		// Write .db (1 MB) and .db-wal (600 KB) -> DB alone (1 MB) < DefaultMaxSessionDBBytes (1.5 MB)
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, 1000*1024), 0644)
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db-wal"), make([]byte, 600*1024), 0644)

		_ = saveSessionID(store, "chan-db-wal-thread", oldSess)
		_, _ = incrementSessionTurnCount(store, "chan-db-wal-thread")

		var mu sync.Mutex
		var gotSessionID string
		doneCh := make(chan struct{})

		appCfg := config.NewFromData(&config.ConfigData{
			Channels: map[string]config.ChannelPolicy{
				"default": {Mode: "channel"},
			},
		})

		pool := New(appCfg, WorkerPoolConfig{
			SessionManager: sessMgr,
			Store:          store,
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

		msg := db.Message{ID: "msg-chan-db-wal", ThreadID: "chan-db-wal-thread", Content: "<@aerial> hello"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for message")
		}

		mu.Lock()
		if gotSessionID != oldSess {
			t.Errorf("Expected session ID %q preserved when WAL is ignored, got: %q", oldSess, gotSessionID)
		}
		mu.Unlock()

		prevSess, _ := getPreviousSessionID(store, "chan-db-wal-thread")
		if prevSess != "" {
			t.Errorf("Expected no rotation (empty previous session ID), got %q", prevSess)
		}
	})

	// Thread mode pre-flight DB rotation
	t.Run("ThreadMode_PreFlight_DBLimit", func(t *testing.T) {
		t.Parallel()
		store := setupTestStore(t)
		tmpDir := t.TempDir()
		sessMgr := session.New(tmpDir, tmpDir)

		oldSess := "sess-thread-db-rot"
		convDir := filepath.Join(tmpDir, "conversations")
		if err := os.MkdirAll(convDir, 0755); err != nil {
			t.Fatalf("failed to create convDir: %v", err)
		}
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, DefaultMaxSessionDBBytes+1024), 0644)

		_ = saveSessionID(store, "thread-db-thread", oldSess)
		_, _ = incrementSessionTurnCount(store, "thread-db-thread")

		var mu sync.Mutex
		var gotSessionID string
		doneCh := make(chan struct{})

		appCfg := config.NewFromData(&config.ConfigData{
			Channels: map[string]config.ChannelPolicy{
				"default": {Mode: "thread"},
			},
		})

		pool := New(appCfg, WorkerPoolConfig{
			SessionManager: sessMgr,
			Store:          store,
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

		msg := db.Message{ID: "msg-thread-db", ThreadID: "thread-db-thread", Content: "hello"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for message")
		}

		mu.Lock()
		if gotSessionID != "" {
			t.Errorf("Expected cold start (empty session ID) on thread DB limit, got: %q", gotSessionID)
		}
		mu.Unlock()
	})
}

func TestWorker_SessionDBRotation_QuotaPause(t *testing.T) {
	t.Parallel()

	// Rotate on Quota Pause when DB size >= DefaultMaxQuotaPauseDBBytes
	t.Run("RotateWhenDBSizeExceedsQuotaPauseThreshold", func(t *testing.T) {
		t.Parallel()
		store := setupTestStore(t)
		tmpDir := t.TempDir()
		sessMgr := session.New(tmpDir, tmpDir)

		oldSess := "sess-quota-pause-db-rot"
		convDir := filepath.Join(tmpDir, "conversations")
		_ = os.MkdirAll(convDir, 0755)
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, DefaultMaxQuotaPauseDBBytes+1024), 0644)

		_ = saveSessionID(store, "thread-qp-rot", oldSess)

		doneCh := make(chan struct{})
		appCfg := config.NewFromData(&config.ConfigData{
			Channels: map[string]config.ChannelPolicy{
				"default": {Mode: "thread"},
			},
		})

		pool := New(appCfg, WorkerPoolConfig{
			SessionManager: sessMgr,
			Store:          store,
			TimeoutMinutes: 1,
			BackoffBase:    10 * time.Millisecond,
			MaxAttempts:    1,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
				return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Resets in 30s.", 1, errors.New("exit code 1")
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

		msg := db.Message{ID: "msg-qp-rot", ThreadID: "thread-qp-rot", Content: "Trigger quota pause"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for message")
		}

		curSess, _ := getSessionID(store, "thread-qp-rot")
		if curSess != "" {
			t.Errorf("Expected session to be rotated (empty) on quota pause with large DB, got: %q", curSess)
		}
		prevSess, _ := getPreviousSessionID(store, "thread-qp-rot")
		if prevSess != oldSess {
			t.Errorf("Expected previous session ID %q, got: %q", oldSess, prevSess)
		}
	})

	// Preserve session on Quota Pause when DB size < DefaultMaxQuotaPauseDBBytes
	t.Run("PreserveWhenDBSizeBelowQuotaPauseThreshold", func(t *testing.T) {
		t.Parallel()
		store := setupTestStore(t)
		tmpDir := t.TempDir()
		sessMgr := session.New(tmpDir, tmpDir)

		oldSess := "sess-quota-pause-db-keep"
		convDir := filepath.Join(tmpDir, "conversations")
		_ = os.MkdirAll(convDir, 0755)
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, 200*1024), 0644)

		_ = saveSessionID(store, "thread-qp-keep", oldSess)

		doneCh := make(chan struct{})
		appCfg := config.NewFromData(&config.ConfigData{
			Channels: map[string]config.ChannelPolicy{
				"default": {Mode: "thread"},
			},
		})

		pool := New(appCfg, WorkerPoolConfig{
			SessionManager: sessMgr,
			Store:          store,
			TimeoutMinutes: 1,
			BackoffBase:    10 * time.Millisecond,
			MaxAttempts:    1,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
				return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Resets in 30s.", 1, errors.New("exit code 1")
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

		msg := db.Message{ID: "msg-qp-keep", ThreadID: "thread-qp-keep", Content: "Trigger quota pause"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for message")
		}

		curSess, _ := getSessionID(store, "thread-qp-keep")
		if curSess != oldSess {
			t.Errorf("Expected session %q to be preserved on quota pause when small, got: %q", oldSess, curSess)
		}
	})
}

func TestWorker_SessionDBRotation_WatchdogTimeout(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	bloatedSessionID := "sess-wd-db-bloat"
	convDir := filepath.Join(tmpDir, "conversations")
	_ = os.MkdirAll(convDir, 0755)

	_ = saveSessionID(store, "thread-wd-db-bloat", bloatedSessionID)

	var mu sync.Mutex
	var attempts []string
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			attempts = append(attempts, sessionID)
			currentAttempt := len(attempts)
			mu.Unlock()

			if currentAttempt == 1 {
				// During attempt 1, write a bloated DB (exceeding DefaultMaxSessionDBBytes)
				_ = os.WriteFile(filepath.Join(convDir, bloatedSessionID+".db"), make([]byte, DefaultMaxSessionDBBytes+1024), 0644)
				return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
			}

			return mockJSONResponse(uuid.New().String(), "Recovered on cold retry"), "", 0, nil
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

	msg := db.Message{ID: "msg-wd-db-bloat", ThreadID: "thread-wd-db-bloat", Content: "Run long task"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message completed")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(attempts))
	}
	if attempts[0] != bloatedSessionID {
		t.Errorf("Expected attempt 1 to use session %q, got %q", bloatedSessionID, attempts[0])
	}
	if attempts[1] != "" {
		t.Errorf("Expected attempt 2 to have rotated to empty session (cold start), got: %q", attempts[1])
	}
}

func TestWorker_SessionDBRotation_PostExecution(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	executedSessionID := uuid.New().String()
	convDir := filepath.Join(tmpDir, "conversations")
	_ = os.MkdirAll(convDir, 0755)

	doneCh := make(chan struct{})
	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			// During execution, write a DB that exceeds DefaultMaxSessionDBBytes
			_ = os.WriteFile(filepath.Join(convDir, executedSessionID+".db"), make([]byte, DefaultMaxSessionDBBytes+500), 0644)
			return mockJSONResponse(executedSessionID, "Execution heavy DB output"), "", 0, nil
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

	msg := db.Message{ID: "msg-post-db", ThreadID: "thread-post-db-test", Content: "Run DB heavy"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	curSess, _ := getSessionID(store, "thread-post-db-test")
	if curSess != "" {
		t.Errorf("Expected session to be rotated (empty) post execution, got: %q", curSess)
	}
	prevSess, _ := getPreviousSessionID(store, "thread-post-db-test")
	if prevSess != executedSessionID {
		t.Errorf("Expected previous session ID %q post execution, got %q", executedSessionID, prevSess)
	}
}

func TestWorker_SessionDBRotation_TransientRetry(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	transSessionID := "sess-trans-db-rot"
	convDir := filepath.Join(tmpDir, "conversations")
	_ = os.MkdirAll(convDir, 0755)

	_ = saveSessionID(store, "thread-trans-db-rot", transSessionID)

	var mu sync.Mutex
	var attempts []string
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attempts = append(attempts, sessionID)
			currentAttempt := len(attempts)
			mu.Unlock()

			if currentAttempt == 1 {
				// During attempt 1, write a bloated DB (exceeding DefaultMaxSessionDBBytes)
				_ = os.WriteFile(filepath.Join(convDir, transSessionID+".db"), make([]byte, DefaultMaxSessionDBBytes+1024), 0644)
				// Return a transient error
				return `{"status": "ERROR", "error": "temporary network timeout"}`, "", 0, nil
			}

			return mockJSONResponse(uuid.New().String(), "Recovered on cold retry"), "", 0, nil
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

	msg := db.Message{ID: "msg-trans-db", ThreadID: "thread-trans-db-rot", Content: "Run transient task"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message completed")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(attempts))
	}
	if attempts[0] != transSessionID {
		t.Errorf("Expected attempt 1 to use session %q, got %q", transSessionID, attempts[0])
	}
	if attempts[1] != "" {
		t.Errorf("Expected attempt 2 to have rotated to empty session (cold start), got: %q", attempts[1])
	}
}

func TestWorkerPool_CapacityBlip_LocalRetry_Success(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	var mu sync.Mutex
	var attemptCount int
	var deliveredText string
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:        5 * time.Millisecond,
		RetryDelayOverride: 10 * time.Millisecond,
		MaxAttempts:        2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			attemptCount++
			cur := attemptCount
			mu.Unlock()

			if cur == 1 {
				// Capacity blip with 0s countdown
				return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
			}
			return mockJSONResponse(uuid.New().String(), "Recovered on attempt 2!"), "", 0, nil
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
			select {
			case doneCh <- struct{}{}:
			default:
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-cap-retry-succ", ThreadID: "thread-cap-succ", Content: "hello"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message completed")
	}

	mu.Lock()
	defer mu.Unlock()

	if attemptCount != 2 {
		t.Fatalf("Expected 2 attempts for capacity blip retry, got %d", attemptCount)
	}

	dbMsg, err := store.GetMessage(context.Background(), "msg-cap-retry-succ")
	if err != nil || dbMsg == nil {
		t.Fatalf("failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusCompleted {
		t.Errorf("Expected StatusCompleted, got %s (err: %s)", dbMsg.Status, dbMsg.ErrorMessage)
	}
	if !strings.Contains(deliveredText, "Recovered on attempt 2!") {
		t.Errorf("Expected delivery text to contain recovered response, got %q", deliveredText)
	}

	// Verify global queue was NEVER locked
	if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
		t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
	}
}

func TestWorkerPool_CapacityBlip_DoesNotBlockConcurrentThreads(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	var mu sync.Mutex
	var threadAAttempts int
	var threadBCompleted time.Time
	var threadACompleted time.Time
	doneA := make(chan struct{}, 1)
	doneB := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:        5 * time.Millisecond,
		RetryDelayOverride: 10 * time.Millisecond,
		MaxAttempts:        2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			if strings.Contains(prompt, "Thread A") {
				// Thread A hits capacity blip on attempt 1, succeeds on attempt 2
				mu.Lock()
				threadAAttempts++
				att := threadAAttempts
				mu.Unlock()
				if att == 1 {
					return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
				}
				return mockJSONResponse(uuid.New().String(), "Thread A success"), "", 0, nil
			}
			// Thread B succeeds immediately
			return mockJSONResponse(uuid.New().String(), "Thread B success"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			defer mu.Unlock()
			if msg.ThreadID == "thread-A" && finalStatus == db.StatusCompleted {
				threadACompleted = time.Now()
				select {
				case doneA <- struct{}{}:
				default:
				}
			} else if msg.ThreadID == "thread-B" && finalStatus == db.StatusCompleted {
				threadBCompleted = time.Now()
				select {
				case doneB <- struct{}{}:
				default:
				}
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	msgA := db.Message{ID: "msg-thread-A", ThreadID: "thread-A", Content: "Thread A prompt"}
	msgB := db.Message{ID: "msg-thread-B", ThreadID: "thread-B", Content: "Thread B prompt"}
	_ = insertMessage(store, msgA)
	_ = insertMessage(store, msgB)

	// Enqueue Thread A first
	pool.Enqueue(msgA)
	// Enqueue Thread B immediately
	pool.Enqueue(msgB)

	select {
	case <-doneB:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for Thread B completed")
	}

	select {
	case <-doneA:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for Thread A completed")
	}

	// Verify global queue was NEVER locked
	if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
		t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
	}

	mu.Lock()
	if threadBCompleted.IsZero() {
		t.Errorf("Expected Thread B to complete successfully")
	}
	if threadACompleted.IsZero() {
		t.Errorf("Expected Thread A to complete successfully")
	}
	mu.Unlock()

	dbMsgB, _ := store.GetMessage(context.Background(), "msg-thread-B")
	if dbMsgB == nil || dbMsgB.Status != db.StatusCompleted {
		t.Errorf("Expected Thread B to be StatusCompleted, got %v", dbMsgB)
	}
}

func TestWorkerPool_CapacityBlip_ContextCancellation(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	startedCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			select {
			case startedCh <- struct{}{}:
			default:
			}
			return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-cap-cancel", ThreadID: "thread-cap-cancel", Content: "cancel me"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	// Wait for attempt 1 to hit capacity blip
	select {
	case <-startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for attempt 1")
	}

	// Cancel pool context while worker is in capacity backoff sleep (delay is 2s-3s)
	time.Sleep(100 * time.Millisecond)
	pool.cancel()

	// Wait a moment for worker goroutine to process pool.ctx.Done()
	time.Sleep(300 * time.Millisecond)

	dbMsg, err := store.GetMessage(context.Background(), "msg-cap-cancel")
	if err != nil || dbMsg == nil {
		t.Fatalf("failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusPending {
		t.Errorf("Expected StatusPending on graceful shutdown, got %s (err: %s)", dbMsg.Status, dbMsg.ErrorMessage)
	}
	if !strings.Contains(dbMsg.ErrorMessage, "interrupted by graceful deployment") {
		t.Errorf("Expected error to contain 'interrupted by graceful deployment', got %q", dbMsg.ErrorMessage)
	}
	if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
		t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
	}
}

func TestWorkerPool_ProgressiveCapacityBackoff(t *testing.T) {
	t.Parallel()

	// 1. Backoff floor calculation tests
	t.Run("FloorCalculation", func(t *testing.T) {
		t.Parallel()
		// Attempt 1 floor >= 30s
		for i := 0; i < 20; i++ {
			d1 := calculateCapacityBackoff(1, 0)
			if d1 < 30*time.Second {
				t.Fatalf("attempt 1 backoff floor expected >= 30s, got %v", d1)
			}
			if d1 > 33*time.Second {
				t.Fatalf("attempt 1 backoff jitter expected <= 33s, got %v", d1)
			}
		}

		// Attempt 2 floor >= 60s
		for i := 0; i < 20; i++ {
			d2 := calculateCapacityBackoff(2, 0)
			if d2 < 60*time.Second {
				t.Fatalf("attempt 2 backoff floor expected >= 60s, got %v", d2)
			}
			if d2 > 63*time.Second {
				t.Fatalf("attempt 2 backoff jitter expected <= 63s, got %v", d2)
			}
		}

		// resetDur override when resetDur > minFloor (e.g. 75s)
		for i := 0; i < 20; i++ {
			dOverride := calculateCapacityBackoff(1, 75*time.Second)
			if dOverride < 75*time.Second {
				t.Fatalf("resetDur override expected >= 75s, got %v", dOverride)
			}
			if dOverride > 78*time.Second {
				t.Fatalf("resetDur override jitter expected <= 78s, got %v", dOverride)
			}
		}
	})

	// 2. Context cancellation during backoff triggers clean pending status and calls te.stopTyping()
	t.Run("ContextCancellationDuringBackoff", func(t *testing.T) {
		t.Parallel()
		store := setupTestStore(t)
		tmpDir := t.TempDir()
		sessMgr := session.New(tmpDir, tmpDir)

		startedCh := make(chan struct{}, 1)
		var typingMu sync.Mutex
		var typingActive bool
		var typingStopped bool

		appCfg := config.NewFromData(&config.ConfigData{
			Channels: map[string]config.ChannelPolicy{
				"default": {Mode: "thread"},
			},
		})

		pool := New(appCfg, WorkerPoolConfig{
			SessionManager: sessMgr,
			Store:          store,
			TimeoutMinutes: 1,
			MaxAttempts:    2,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
				select {
				case startedCh <- struct{}{}:
				default:
				}
				return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
			},
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return nil
			},
			TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
				typingMu.Lock()
				typingActive = true
				typingMu.Unlock()
				return func() {
					typingMu.Lock()
					typingStopped = true
					typingMu.Unlock()
				}
			},
		})
		pool.Start()
		defer pool.Stop()

		msg := db.Message{ID: "msg-cap-prog-cancel", ThreadID: "thread-cap-prog-cancel", Content: "cancel progressive backoff"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-startedCh:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for attempt 1 runner invocation")
		}

		// Allow worker to enter backoff sleep (which has delay >= 30s)
		time.Sleep(100 * time.Millisecond)

		typingMu.Lock()
		if !typingActive {
			t.Errorf("Expected typing to have started")
		}
		if typingStopped {
			t.Errorf("Expected te.stopTyping() NOT to be called during backoff delay (typing indicator must continue pulsing)")
		}
		typingMu.Unlock()

		// Cancel pool context during progressive capacity backoff
		pool.cancel()

		// Wait for worker goroutine to process pool.ctx.Done()
		time.Sleep(300 * time.Millisecond)

		typingMu.Lock()
		if !typingStopped {
			t.Errorf("Expected te.stopTyping() to be called on context cancellation during backoff")
		}
		typingMu.Unlock()

		dbMsg, err := store.GetMessage(context.Background(), "msg-cap-prog-cancel")
		if err != nil || dbMsg == nil {
			t.Fatalf("failed to query message: %v", err)
		}
		if dbMsg.Status != db.StatusPending {
			t.Errorf("Expected StatusPending on context cancellation, got %s (err: %s)", dbMsg.Status, dbMsg.ErrorMessage)
		}
		if !strings.Contains(dbMsg.ErrorMessage, "interrupted by graceful deployment") {
			t.Errorf("Expected error to contain 'interrupted by graceful deployment', got %q", dbMsg.ErrorMessage)
		}
		if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
			t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
		}
	})
}

func TestWorkerPool_CapacityBlip_Exhaustion_LocalFailure(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	var deliveredText string
	var mu sync.Mutex
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
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
			select {
			case doneCh <- struct{}{}:
			default:
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-cap-exhaust", ThreadID: "thread-cap-exhaust", Content: "exhaust me"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message completion")
	}

	dbMsg, err := store.GetMessage(context.Background(), "msg-cap-exhaust")
	if err != nil || dbMsg == nil {
		t.Fatalf("failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected StatusFailed, got %s", dbMsg.Status)
	}
	if !strings.Contains(dbMsg.ErrorMessage, "[CAPACITY_EXHAUSTED") {
		t.Errorf("Expected error to contain [CAPACITY_EXHAUSTED], got %q", dbMsg.ErrorMessage)
	}

	// Verify global queue was NEVER locked
	if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
		t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
	}

	// Verify no one-shot schedules were created for quota pause
	schedules, _ := store.GetAllOneShotSchedules(context.Background(), "thread-cap-exhaust")
	if len(schedules) > 0 {
		t.Errorf("Expected 0 one-shot schedules on capacity blip exhaustion, got %d", len(schedules))
	}

	mu.Lock()
	if !strings.Contains(deliveredText, "Gemini API quota") && !strings.Contains(deliveredText, "Paused") && !strings.Contains(deliveredText, "capacity") {
		t.Errorf("Expected capacity pause notice in delivered text, got %q", deliveredText)
	}
	mu.Unlock()
}

func TestWorkerPool_CapacityBlip_WithoutCountdown_Fallback70s(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	var deliveredText string
	var mu sync.Mutex
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			// Model capacity error with NO countdown data
			return "", "API error: RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model.", 1, errors.New("exit code 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredText = text
			mu.Unlock()
			select {
			case doneCh <- struct{}{}:
			default:
			}
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-cap-nocountdown", ThreadID: "thread-cap-nocountdown", Content: "test capacity without countdown"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message completion")
	}

	dbMsg, err := store.GetMessage(context.Background(), "msg-cap-nocountdown")
	if err != nil || dbMsg == nil {
		t.Fatalf("failed to query message: %v", err)
	}
	if dbMsg.Status != db.StatusFailed {
		t.Errorf("Expected StatusFailed, got %s", dbMsg.Status)
	}
	if !strings.Contains(dbMsg.ErrorMessage, "[CAPACITY_EXHAUSTED") {
		t.Errorf("Expected error to contain [CAPACITY_EXHAUSTED], got %q", dbMsg.ErrorMessage)
	}

	// Verify global queue was NEVER locked
	if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
		t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
	}

	// Verify no one-shot schedules were created
	schedules, _ := store.GetAllOneShotSchedules(context.Background(), "thread-cap-nocountdown")
	if len(schedules) > 0 {
		t.Errorf("Expected 0 one-shot schedules on capacity blip exhaustion, got %d", len(schedules))
	}

	mu.Lock()
	if !strings.Contains(deliveredText, "Gemini API quota") && !strings.Contains(deliveredText, "Paused") && !strings.Contains(deliveredText, "capacity") {
		t.Errorf("Expected capacity pause notice in delivered text, got %q", deliveredText)
	}
	mu.Unlock()
}

func TestDiscordTurnSink_Callbacks(t *testing.T) {
	t.Parallel()
	sink := newDiscordTurnSink(nil)
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnTextDelta("some text")
	sink.OnError(errors.New("test error"))
	res, err := sink.Wait(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Response != "some text" {
		t.Errorf("expected recovered text 'some text', got %q", res.Response)
	}

	// When no substantive deltas were streamed, error is delivered
	sinkNoDeltas := newDiscordTurnSink(nil)
	expectedErr := errors.New("hard failure")
	sinkNoDeltas.OnError(expectedErr)
	_, err = sinkNoDeltas.Wait(context.Background())
	if err == nil || !errors.Is(err, expectedErr) {
		t.Errorf("unexpected error received: %v", err)
	}
}

func TestWorker_DualPoolRouting_LowEffortAndFallback(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	var primarySpawnCount, lowSpawnCount int
	var mu sync.Mutex

	primarySpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			mu.Lock()
			primarySpawnCount++
			mu.Unlock()
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"c1111111-2222-3333-4444-555555555555\"}\n"))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"executed on primary\",\"usage\":{\"total_tokens\":10}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	lowSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			mu.Lock()
			lowSpawnCount++
			mu.Unlock()
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"d1111111-2222-3333-4444-555555555555\"}\n"))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"executed on low effort\",\"usage\":{\"total_tokens\":5}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12346), nil
		},
	}

	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowPool.Close()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
		d.LowEffortModel = "gemini-3.8-flash-low"
	})

	doneCh := make(chan struct{}, 5)
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowPool,
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				doneCh <- struct{}{}
			}
		},
	})
	defer pool.Stop()

	// 1. Enqueue low-effort message -> should route to lowPool
	lowMsg := db.Message{
		ID:        "msg-low-route-1",
		ThreadID:  "thread-low-route",
		Content:   "run low task",
		Effort:    "low",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), lowMsg)
	pool.Enqueue(lowMsg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for low effort turn completion")
	}

	mu.Lock()
	if lowSpawnCount != 1 || primarySpawnCount != 0 {
		t.Errorf("expected lowSpawnCount=1 and primarySpawnCount=0, got low=%d primary=%d", lowSpawnCount, primarySpawnCount)
	}
	mu.Unlock()

	// 2. Enqueue normal message -> should route to primaryPool
	normMsg := db.Message{
		ID:        "msg-norm-route-1",
		ThreadID:  "thread-norm-route",
		Content:   "run normal task",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), normMsg)
	pool.Enqueue(normMsg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for normal turn completion")
	}

	mu.Lock()
	if primarySpawnCount != 1 {
		t.Errorf("expected primarySpawnCount=1, got %d", primarySpawnCount)
	}
	mu.Unlock()

	// 3. Fallback: pool with LowEffortProcessPool=nil routes low effort message to primaryPool
	poolFallback := New(cfg, WorkerPoolConfig{
		ProcessPool:    primaryPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				doneCh <- struct{}{}
			}
		},
	})
	defer poolFallback.Stop()

	fallbackMsg := db.Message{
		ID:        "msg-fallback-1",
		ThreadID:  "thread-fallback",
		Content:   "run fallback task",
		Effort:    "low",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), fallbackMsg)
	poolFallback.Enqueue(fallbackMsg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for fallback turn completion")
	}

	mu.Lock()
	if primarySpawnCount != 2 {
		t.Errorf("expected primarySpawnCount=2 after fallback, got %d", primarySpawnCount)
	}
	mu.Unlock()
}

func TestWorker_SessionAntiFlapping_ProtectsPrimarySession(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	primarySessUUID := "11111111-1111-1111-1111-111111111111"
	lowSessUUID := "22222222-2222-2222-2222-222222222222"

	lowSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n", lowSessUUID)))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok low effort\",\"conversation_id\":%q,\"usage\":{\"total_tokens\":5}}}\n", lowSessUUID)))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	primarySpawner := runner.NewMockSpawner()

	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowPool.Close()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
		d.LowEffortModel = "gemini-3.8-flash-low"
	})

	doneCh := make(chan struct{}, 1)
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowPool,
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				doneCh <- struct{}{}
			}
		},
	})
	defer pool.Stop()

	threadID := "thread-antiflap-1"
	// Seed thread with existing primary session
	_ = store.SaveSessionID(context.Background(), threadID, primarySessUUID)

	msg := db.Message{
		ID:        "msg-antiflap-1",
		ThreadID:  threadID,
		Content:   "run scheduled low-effort maintenance",
		Effort:    "low",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for message completion")
	}

	// Verify that the sessions table was NOT overwritten
	currentSess, err := store.GetSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to get session ID: %v", err)
	}
	if currentSess != primarySessUUID {
		t.Fatalf("ANTI-FLAPPING VIOLATION: expected primary session %s to be preserved, got %s", primarySessUUID, currentSess)
	}
}

func TestWorker_ThreadWorker_ActiveTasksInLowEffortPoolPreventReap(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	lowSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"e1111111-2222-3333-4444-555555555555\"}\n"))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok\",\"usage\":{\"total_tokens\":5}}}\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, runner.NewMockSpawner())
	defer primaryPool.Close()

	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowPool.Close()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	threadID := "thread-low-reap-test"
	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowPool,
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

	// Pre-create daemon in lowPool with active task
	ctx := context.Background()
	daemon, err := pool.LowEffortProcessPool().GetOrCreate(ctx, threadID)
	if err != nil {
		t.Fatalf("failed to create daemon in low pool: %v", err)
	}
	daemon.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "task-low-busy-prevent-reap",
		StartedAt: time.Now(),
	})

	msg := db.Message{
		ID:        "msg-low-reap-1",
		ThreadID:  threadID,
		Content:   "hello low reap",
		Effort:    "low",
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

	// Verify worker remains active because background task in lowEffortProcessPool is still running
	time.Sleep(60 * time.Millisecond)
	pool.mu.Lock()
	_, workerStillActive := pool.threadChs[threadID]
	pool.mu.Unlock()

	if !workerStillActive {
		t.Errorf("expected worker to remain active because background task was running in LowEffortProcessPool")
	}
}

func TestWorker_YieldTrap_AlternatePoolFallback(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	sessID := "f1111111-2222-3333-4444-555555555555"
	sessDir := filepath.Join(tempData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	var turnCount int
	var mu sync.Mutex

	primarySpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n", sessID)))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					mu.Lock()
					turnCount++
					tc := turnCount
					mu.Unlock()
					if tc == 1 {
						// Turn 1 completes normally without yield error in output
						_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"waiting for bg task\",\"usage\":{\"total_tokens\":5}}}\n"))
					} else {
						// Auto-resumed turn completes with final success
						_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"all tasks done\",\"usage\":{\"total_tokens\":5}}}\n"))
					}
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}

	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"d1111111-2222-3333-4444-555555555555\"}\n"))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12347), nil
		},
	}
	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowPool.Close()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	threadID := "thread-yield-fallback"
	_ = store.SaveSessionID(context.Background(), threadID, sessID)

	lowSessID := "d1111111-2222-3333-4444-555555555555"
	taskDir := filepath.Join(tempData, "brain", lowSessID, ".system_generated", "tasks")
	_ = os.MkdirAll(taskDir, 0755)
	_ = os.WriteFile(filepath.Join(taskDir, "bg-task-in-low-pool.log"), []byte("done"), 0644)

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowPool,
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

	// Put an active background task in the ALTERNATE pool (lowPool) for this thread
	ctx := context.Background()
	lowDaemon, err := lowPool.GetOrCreate(ctx, threadID)
	if err != nil {
		t.Fatalf("failed to create low daemon: %v", err)
	}
	lowDaemon.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "bg-task-in-low-pool",
		StartedAt: time.Now(),
	})

	// Enqueue turn on primary pool (high effort)
	msg := db.Message{
		ID:        "msg-yield-fb-1",
		ThreadID:  threadID,
		Content:   "run command and wait",
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

	mu.Lock()
	defer mu.Unlock()
	if turnCount < 2 {
		t.Errorf("expected auto-resumption from yield-trap detection in alternate pool, turnCount=%d", turnCount)
	}
}

func TestWorker_QuotaPause_PreservesEffort(t *testing.T) {
	for _, effort := range []string{"low", "high"} {
		t.Run("Effort_"+effort, func(t *testing.T) {
			store := setupTestStore(t)
			tmpHome := t.TempDir()
			tempData := t.TempDir()

			quotaErrSpawner := &runner.MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
					inR, inW := io.Pipe()
					outR, outW := io.Pipe()
					errR, errW := io.Pipe()
					go func() {
						defer errW.Close()
						defer outW.Close()
						_, _ = outW.Write([]byte("{\"event\":\"init\",\"conversation_id\":\"11111111-2222-3333-4444-555555555555\"}\n"))
						scanner := bufio.NewScanner(inR)
						for scanner.Scan() {
							_, _ = errW.Write([]byte("RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m.\n"))
							_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"exit_code\":1,\"error\":\"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m.\",\"stderr\":\"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m.\"}}\n"))
						}
					}()
					return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
				},
			}

			poolConfig := runner.PoolConfig{Model: "gemini-3.7-pro"}
			primaryPool := runner.NewUnifiedProcessPool(poolConfig, quotaErrSpawner)
			defer primaryPool.Close()

			lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, quotaErrSpawner)
			defer lowPool.Close()

			cfg := config.NewTestConfig(func(d *config.ConfigData) {
				d.DataDir = tempData
				d.LowEffortModel = "gemini-3.8-flash-low"
			})

			doneCh := make(chan struct{})
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool:          primaryPool,
				LowEffortProcessPool: lowPool,
				SessionManager:       session.New(tmpHome, tempData),
				Store:                store,
				TimeoutMinutes:       1,
				MaxAttempts:          1,
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

			threadID := "thread-quota-" + effort
			msg := db.Message{
				ID:        "msg-quota-" + effort,
				ThreadID:  threadID,
				Content:   "run task",
				Effort:    effort,
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

			schedules, err := store.GetAllOneShotSchedules(context.Background(), threadID)
			if err != nil {
				t.Fatalf("failed to get one shot schedules: %v", err)
			}
			if len(schedules) != 1 {
				t.Fatalf("expected 1 one-shot schedule, got %d", len(schedules))
			}
			expectedEffort := effort
			if expectedEffort == "" {
				expectedEffort = "high"
			}
			if schedules[0].Effort != expectedEffort {
				t.Fatalf("expected oneShot.Effort=%q, got %q", expectedEffort, schedules[0].Effort)
			}
		})
	}
}

func TestWorkerPool_SessionRotationInjectsToolActions(t *testing.T) {
	t.Run("EndToEnd_SessionRotationInjectsAndRetainsActions", func(t *testing.T) {
		tmpHome := t.TempDir()
		tempData := t.TempDir()
		store := setupTestStore(t)

		validUUID := uuid.New().String()
		sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
		if err := os.MkdirAll(sessDir, 0755); err != nil {
			t.Fatalf("failed to create logs dir: %v", err)
		}

		// Initial transcript with 1 step so session is warm at turn start
		initTranscript := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"investigate memory leak"}` + "\n"
		if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(initTranscript), 0644); err != nil {
			t.Fatalf("failed to write initial transcript: %v", err)
		}

		cfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.DataDir = tempData
		})

		threadID := "thread-rotation-actions-" + uuid.New().String()
		if err := store.SaveSessionID(context.Background(), threadID, validUUID); err != nil {
			t.Fatalf("failed to save session ID: %v", err)
		}

		var mu sync.Mutex
		var recordedPrompts []string
		var recordedSessions []string
		doneCh := make(chan struct{})

		pool := New(cfg, WorkerPoolConfig{
			SessionManager:      session.New(tmpHome, tempData),
			Store:               store,
			TimeoutMinutes:      1,
			MaxAttempts:         3,
			RetryDelayOverride:  10 * time.Millisecond,
			BackoffBase:         5 * time.Millisecond,
			RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				mu.Lock()
				recordedPrompts = append(recordedPrompts, prompt)
				recordedSessions = append(recordedSessions, sessionID)
				attemptNum := len(recordedPrompts)
				mu.Unlock()

				if attemptNum == 1 {
					// During attempt 1, tool calls and step count grew past guardrail before quota pause
					tNow := time.Now().UTC()
					transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"investigate memory leak"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"run_command","args":{"command_line":"docker stats --no-stream"}}]}
{"step_index":2,"source":"TOOL","type":"GENERIC","status":"DONE","content":"CONTAINER ID aerial-brain MEM USAGE 120MiB"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2s.","created_at":%q}
`, DefaultMaxSessionSteps, tNow.Format(time.RFC3339))
					if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
						t.Errorf("failed to update transcript during attempt 1: %v", err)
					}

					// Attempt 1: Capacity throttle error triggering quota pause rotation
					return "", "RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2s.", 1, errors.New("429 capacity blip")
				}
				if attemptNum == 2 {
					// Attempt 2: Another capacity blip to verify context retention across multiple retries
					return "", "RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2s.", 1, errors.New("429 capacity blip 2")
				}

				// Attempt 3: Success
				return mockJSONResponse(uuid.New().String(), "Diagnosis complete"), "", 0, nil
			},
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return nil
			},
			TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
				return func() {}
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
		pool.Start()
		defer pool.Stop()

		msg := db.Message{
			ID:        "msg-rotation-actions-" + uuid.New().String(),
			ThreadID:  threadID,
			Content:   "check system diagnostics",
			Status:    db.StatusPending,
			CreatedAt: time.Now(),
		}
		if err := store.InsertMessage(context.Background(), msg); err != nil {
			t.Fatalf("failed to insert message: %v", err)
		}
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for turn completion")
		}

		mu.Lock()
		defer mu.Unlock()

		if len(recordedPrompts) != 3 {
			t.Fatalf("expected 3 runner attempts, got %d", len(recordedPrompts))
		}

		// Attempt 1: uses initial validUUID session and does NOT have <PREVIOUS_TURN_ACTIONS>
		if recordedSessions[0] != validUUID {
			t.Errorf("expected attempt 1 to use session %q, got %q", validUUID, recordedSessions[0])
		}
		if strings.Contains(recordedPrompts[0], "<PREVIOUS_TURN_ACTIONS>") {
			t.Errorf("expected attempt 1 prompt to not contain PREVIOUS_TURN_ACTIONS, got: %s", recordedPrompts[0])
		}

		// Attempt 2: session was rotated to empty string (cold start) and prompt contains <PREVIOUS_TURN_ACTIONS>
		if recordedSessions[1] != "" {
			t.Errorf("expected attempt 2 session to be rotated to empty string, got %q", recordedSessions[1])
		}
		if !strings.Contains(recordedPrompts[1], "<PREVIOUS_TURN_ACTIONS>") {
			t.Errorf("expected attempt 2 prompt to contain <PREVIOUS_TURN_ACTIONS>, got: %s", recordedPrompts[1])
		}
		if !strings.Contains(recordedPrompts[1], "docker stats --no-stream") {
			t.Errorf("expected attempt 2 prompt to contain tool command, got: %s", recordedPrompts[1])
		}

		// Attempt 3: context retention verified - prompt still contains <PREVIOUS_TURN_ACTIONS> and is not duplicated
		if recordedSessions[2] != "" {
			t.Errorf("expected attempt 3 session to be empty string, got %q", recordedSessions[2])
		}
		if !strings.Contains(recordedPrompts[2], "<PREVIOUS_TURN_ACTIONS>") {
			t.Errorf("expected attempt 3 prompt to retain <PREVIOUS_TURN_ACTIONS>, got: %s", recordedPrompts[2])
		}
		if count := strings.Count(recordedPrompts[2], "<PREVIOUS_TURN_ACTIONS>"); count != 1 {
			t.Errorf("expected exactly 1 instance of <PREVIOUS_TURN_ACTIONS> in attempt 3 prompt, got %d", count)
		}
	})

	t.Run("EndToEnd_TransientRotationInjectsToolActions", func(t *testing.T) {
		tmpHome := t.TempDir()
		tempData := t.TempDir()
		store := setupTestStore(t)

		validUUID := uuid.New().String()
		sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
		if err := os.MkdirAll(sessDir, 0755); err != nil {
			t.Fatalf("failed to create logs dir: %v", err)
		}

		initTranscript := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"check build logs"}` + "\n"
		if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(initTranscript), 0644); err != nil {
			t.Fatalf("failed to write initial transcript: %v", err)
		}

		cfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.DataDir = tempData
		})

		threadID := "thread-transient-rotation-" + uuid.New().String()
		if err := store.SaveSessionID(context.Background(), threadID, validUUID); err != nil {
			t.Fatalf("failed to save session ID: %v", err)
		}

		var mu sync.Mutex
		var recordedPrompts []string
		var recordedSessions []string
		doneCh := make(chan struct{})

		pool := New(cfg, WorkerPoolConfig{
			SessionManager:     session.New(tmpHome, tempData),
			Store:              store,
			TimeoutMinutes:     1,
			MaxAttempts:        2,
			BackoffBase:        5 * time.Millisecond,
			RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				mu.Lock()
				recordedPrompts = append(recordedPrompts, prompt)
				recordedSessions = append(recordedSessions, sessionID)
				attemptNum := len(recordedPrompts)
				mu.Unlock()

				if attemptNum == 1 {
					// Grow step count past guardrail and trigger transient retry error
					transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"check build logs"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"view_file","args":{"target_file":"/tmp/build.log"}}]}
{"step_index":2,"source":"TOOL","type":"GENERIC","status":"DONE","content":"build failed at line 42"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"transient network timeout"}
`, DefaultMaxSessionSteps)
					if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
						t.Errorf("failed to update transcript during attempt 1: %v", err)
					}
					return "", "transient connection reset", 1, errors.New("transient error")
				}

				return mockJSONResponse(uuid.New().String(), "Retried successfully"), "", 0, nil
			},
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return nil
			},
			TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
				return func() {}
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
		pool.Start()
		defer pool.Stop()

		msg := db.Message{
			ID:        "msg-transient-rotation-" + uuid.New().String(),
			ThreadID:  threadID,
			Content:   "check build logs",
			Status:    db.StatusPending,
			CreatedAt: time.Now(),
		}
		if err := store.InsertMessage(context.Background(), msg); err != nil {
			t.Fatalf("failed to insert message: %v", err)
		}
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for turn completion")
		}

		mu.Lock()
		defer mu.Unlock()

		if len(recordedPrompts) != 2 {
			t.Fatalf("expected 2 runner attempts, got %d", len(recordedPrompts))
		}
		if recordedSessions[0] != validUUID {
			t.Errorf("expected attempt 1 to use session %q, got %q", validUUID, recordedSessions[0])
		}
		if recordedSessions[1] != "" {
			t.Errorf("expected attempt 2 session to be rotated to empty string, got %q", recordedSessions[1])
		}
		if !strings.Contains(recordedPrompts[1], "<PREVIOUS_TURN_ACTIONS>") {
			t.Errorf("expected attempt 2 prompt to contain <PREVIOUS_TURN_ACTIONS>, got: %s", recordedPrompts[1])
		}
		if !strings.Contains(recordedPrompts[1], "view_file") {
			t.Errorf("expected attempt 2 prompt to contain tool action, got: %s", recordedPrompts[1])
		}
	})

	t.Run("condenseTurnActions_DirectHelpers", func(t *testing.T) {
		te := &turnExecution{}

		// 1. Short string (<1500 chars) returns raw directly
		shortActions := "<PREVIOUS_TURN_ACTIONS>\n- Action: run_command | Command/Args: ls | Status: DONE | Result: ok\n</PREVIOUS_TURN_ACTIONS>"
		if got := te.condenseTurnActions(shortActions); got != shortActions {
			t.Errorf("condenseTurnActions short string failed: got %q, want %q", got, shortActions)
		}

		// 2. Long string (>1500 chars) with mock LLMFunc returning tagged response
		longActions := "<PREVIOUS_TURN_ACTIONS>\n" + strings.Repeat("- Action: run_command | Command/Args: test_long_command | Status: DONE | Result: long_output_data\n", 30) + "</PREVIOUS_TURN_ACTIONS>"
		if len(longActions) <= 1500 {
			t.Fatalf("expected longActions > 1500 chars, got %d", len(longActions))
		}

		mockLLMCalled := false
		mockLLM := func(ctx context.Context, model, prompt string) (string, error) {
			mockLLMCalled = true
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Errorf("expected context to have a deadline")
			} else {
				remaining := time.Until(deadline)
				if remaining < 15*time.Second {
					t.Errorf("expected deadline remaining >= 15s (20s timeout), got %v", remaining)
				}
			}
			if !strings.Contains(prompt, "Condense the following tool actions") {
				t.Errorf("expected summarizer prompt, got %q", prompt)
			}
			return "<PREVIOUS_TURN_ACTIONS>\n- Condensed summary of 30 commands\n</PREVIOUS_TURN_ACTIONS>", nil
		}

		teWithLLM := &turnExecution{
			pool: &WorkerPool{
				cfg: WorkerPoolConfig{
					LLMFunc: mockLLM,
				},
			},
		}

		condensed := teWithLLM.condenseTurnActions(longActions)
		if !mockLLMCalled {
			t.Errorf("expected mock LLMFunc to be called for longActions")
		}
		if !strings.Contains(condensed, "- Condensed summary of 30 commands") {
			t.Errorf("expected condensed output to contain summary, got %q", condensed)
		}

		// 3. Long string (>1500 chars) where mock LLM returns without tags (should be auto-wrapped)
		mockLLMUntagged := func(ctx context.Context, model, prompt string) (string, error) {
			return "- Bare summary without tags", nil
		}
		teUntagged := &turnExecution{
			pool: &WorkerPool{
				cfg: WorkerPoolConfig{
					LLMFunc: mockLLMUntagged,
				},
			},
		}
		condensedUntagged := teUntagged.condenseTurnActions(longActions)
		if !strings.Contains(condensedUntagged, "<PREVIOUS_TURN_ACTIONS>") || !strings.Contains(condensedUntagged, "- Bare summary without tags") {
			t.Errorf("expected wrapped tags for untagged response, got %q", condensedUntagged)
		}

		// 4. Long string (>1500 chars) with mock LLM error fallback (returns raw clamped string)
		mockLLMErr := func(ctx context.Context, model, prompt string) (string, error) {
			return "", errors.New("simulated rate limit error")
		}
		teErr := &turnExecution{
			pool: &WorkerPool{
				cfg: WorkerPoolConfig{
					LLMFunc: mockLLMErr,
				},
			},
		}
		fallbackErr := teErr.condenseTurnActions(longActions)
		if fallbackErr != longActions {
			t.Errorf("expected fallback to rawActions on LLM error, got %q", fallbackErr)
		}

		// 5. Long string (>1500 chars) when summarizer is unavailable (nil pool / nil LLMFunc)
		fallbackNoLLM := te.condenseTurnActions(longActions)
		if fallbackNoLLM != longActions {
			t.Errorf("expected fallback to rawActions when summarizer unavailable, got %q", fallbackNoLLM)
		}

		// 6. Long string (>1500 chars) where mock LLM returns empty string
		mockLLMEmpty := func(ctx context.Context, model, prompt string) (string, error) {
			return "   ", nil
		}
		teEmpty := &turnExecution{
			pool: &WorkerPool{
				cfg: WorkerPoolConfig{
					LLMFunc: mockLLMEmpty,
				},
			},
		}
		fallbackEmpty := teEmpty.condenseTurnActions(longActions)
		if fallbackEmpty != longActions {
			t.Errorf("expected fallback to rawActions on empty LLM response, got %q", fallbackEmpty)
		}

		// 7. Long string (>1500 chars) where mock LLM returns oversized output (>2000 chars)
		mockLLMOversized := func(ctx context.Context, model, prompt string) (string, error) {
			return "<PREVIOUS_TURN_ACTIONS>\n" + strings.Repeat("ExtremelyLongSummaryDataPoint-", 150) + "\n</PREVIOUS_TURN_ACTIONS>", nil
		}
		teOversized := &turnExecution{
			pool: &WorkerPool{
				cfg: WorkerPoolConfig{
					LLMFunc: mockLLMOversized,
				},
			},
		}
		clamped := teOversized.condenseTurnActions(longActions)
		if len(clamped) > 2000 {
			t.Errorf("expected clamped output <= 2000 chars, got %d", len(clamped))
		}
		if !strings.HasPrefix(clamped, "<PREVIOUS_TURN_ACTIONS>") {
			t.Errorf("missing prefix in clamped output: %s", clamped)
		}
		if !strings.HasSuffix(clamped, "</PREVIOUS_TURN_ACTIONS>") {
			t.Errorf("missing suffix in clamped output: %s", clamped)
		}
	})

	t.Run("preparePrompt_DirectHelpers", func(t *testing.T) {
		te := &turnExecution{}
		basePrompt := "Base user request"

		// When previousTurnActions is empty, prompt is untouched
		if got := te.preparePrompt(basePrompt); got != basePrompt {
			t.Errorf("preparePrompt with empty previousTurnActions modified prompt: got %q", got)
		}

		// When previousTurnActions is set, prompt is appended with actions
		te.previousTurnActions = "<PREVIOUS_TURN_ACTIONS>\n- Action: test\n</PREVIOUS_TURN_ACTIONS>"
		prepared := te.preparePrompt(basePrompt)
		expected := basePrompt + "\n\n" + te.previousTurnActions
		if prepared != expected {
			t.Errorf("preparePrompt failed: got %q, want %q", prepared, expected)
		}

		// When called again with already prepared prompt, it does not duplicate
		rePrepared := te.preparePrompt(prepared)
		if rePrepared != prepared {
			t.Errorf("preparePrompt duplicated actions on re-preparation: got %q, want %q", rePrepared, prepared)
		}
	})
}

func TestWorker_ColdStart_RetainsLookbackWithSummary(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	threadID := "thread-cold-with-summary"
	_ = store.SaveThreadSummary(context.Background(), threadID, "<THREAD_SUMMARY>Previously discussed OAuth</THREAD_SUMMARY>", "msg-recent-1")

	CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID,
		GuildID:  "g1",
		ParentID: "parent-chan",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})

	var mu sync.Mutex
	var capturedPrompt string
	doneCh := make(chan struct{})

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"},
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		HistoryFetcher: func(ctx context.Context, tid, beforeID string, limit int) ([]HistoryMessage, error) {
			return []HistoryMessage{
				{ID: "msg-recent-1", AuthorName: "alex", Content: "where is the link?", CreatedAt: time.Now()},
			}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			capturedPrompt = prompt
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

	msg := db.Message{ID: "msg-trigger", ThreadID: threadID, Content: "what link did you give me"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	mu.Lock()
	defer mu.Unlock()

	if !strings.Contains(capturedPrompt, "<THREAD_SUMMARY>Previously discussed OAuth</THREAD_SUMMARY>") {
		t.Errorf("Expected prompt to contain thread summary, got: %s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, "<CHANNEL_HISTORY>") || !strings.Contains(capturedPrompt, "where is the link?") {
		t.Errorf("Expected prompt to contain channel history lookback on cold start, got: %s", capturedPrompt)
	}
}






