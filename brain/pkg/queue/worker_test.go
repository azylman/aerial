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

func TestWorkerPool_PersistentDaemon_ColdStart_SessionMgrFallback(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	store := setupTestStore(t)

	diskUUID := "f1111111-2222-3333-4444-555555555555"
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				// Init event with diskUUID
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", diskUUID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Turn completed with fallback session\",\"usage\":{\"total_tokens\":15}}}\n"))
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
		// Write .db (1 MB) and .db-wal (600 KB) -> 1.6 MB total >= DefaultMaxSessionDBBytes
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, 1000*1024), 0644)
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
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
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
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
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

func TestTurnResultSink_Callbacks(t *testing.T) {
	t.Parallel()
	sink := &turnResultSink{
		resCh: make(chan *runner.TurnResult, 1),
		errCh: make(chan error, 1),
	}
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnTextDelta("some text")
	sink.OnError(errors.New("test error"))
	select {
	case err := <-sink.errCh:
		if err == nil || err.Error() != "test error" {
			t.Errorf("unexpected error received: %v", err)
		}
	default:
		t.Fatalf("expected error on errCh")
	}
}





