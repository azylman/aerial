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
	"sync/atomic"
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

func TestWorker_WatchTaskCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		preWrite   bool
		delayWrite bool
		taskID     string
		timeout    time.Duration
		wantCode   int
		wantErr    bool
	}{
		{
			name:     "YieldTrapAutoResume_PreWritten",
			preWrite: true,
			taskID:   "task-test-1",
			wantCode: 0,
			wantErr:  false,
		},
		{
			name:     "ContextTimeout_NonExistent",
			taskID:   "non-existent-task",
			timeout:  50 * time.Millisecond,
			wantCode: -1,
			wantErr:  true,
		},
		{
			name:       "TickerTrigger_DelayedWrite",
			delayWrite: true,
			taskID:     "delayed-task",
			wantCode:   0,
			wantErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tempDir := t.TempDir()
			taskDir := filepath.Join(tempDir, ".system_generated", "tasks")
			_ = os.MkdirAll(taskDir, 0755)
			logFile := filepath.Join(taskDir, tc.taskID+".log")

			if tc.preWrite {
				if err := os.WriteFile(logFile, []byte("Task output\n"), 0644); err != nil {
					t.Fatalf("failed to write logFile: %v", err)
				}
			}

			var done chan struct{}
			if tc.delayWrite {
				done = make(chan struct{})
				go func() {
					defer close(done)
					time.Sleep(50 * time.Millisecond)
					_ = os.WriteFile(logFile, []byte("finished"), 0644)
				}()
			}

			ctx := context.Background()
			if tc.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.timeout)
				defer cancel()
			}

			code, logPath, err := watchTaskCompletion(ctx, tempDir, tc.taskID)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if logPath != logFile {
					t.Errorf("expected log path %s, got %s", logFile, logPath)
				}
			}
			if code != tc.wantCode {
				t.Errorf("expected code %d, got %d", tc.wantCode, code)
			}

			if done != nil {
				<-done
			}
		})
	}
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

	sessID := "a1111111-2222-3333-4444-555555555555"
	mockSpawner := newScriptedDaemonSpawner(sessID,
		`{"event":"result","result":{"status":"SUCCESS","response":"Executed via persistent daemon","usage":{"total_tokens":10}}}`+"\n",
	)
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		_ = procPool.Close()
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
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}

	if pool.ProcessPool() == nil || !pool.ProcessPool().HasDaemon(threadID) {
		t.Errorf("expected daemon to exist for thread %s in pool", threadID)
	}

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

func TestWorker_PassesSessionIDToAgentPool(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	var mu sync.Mutex
	capturedConfigs := make([]runner.DaemonConfig, 0)

	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			mu.Lock()
			capturedConfigs = append(capturedConfigs, cfg)
			mu.Unlock()

			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				sessID := cfg.SessionID
				if sessID == "" {
					sessID = "c1111111-2222-3333-4444-555555555555"
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","conversation_id":%q}`+"\n", sessID)))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte(`{"event":"result","result":{"status":"SUCCESS","response":"ok","usage":{"total_tokens":5}}}` + "\n"))
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12346), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer procPool.Close()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	completedCh := make(chan string, 10)
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				completedCh <- msg.ID
			}
		},
	})
	defer pool.Stop()

	// Case 1: Thread with existing session
	threadWithSess := "thread-with-sess"
	expectedSessID := "a1111111-2222-3333-4444-555555555555"
	_ = store.SaveSessionID(context.Background(), threadWithSess, expectedSessID)

	msg1 := db.Message{ID: "msg-with-sess", ThreadID: threadWithSess, Content: "Resume turn", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg1)
	pool.Enqueue(msg1)

	select {
	case id := <-completedCh:
		if id != "msg-with-sess" {
			t.Fatalf("unexpected message completed: %s", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for msg-with-sess")
	}

	// Case 2: Fresh thread without session
	freshThread := "thread-fresh"
	msg2 := db.Message{ID: "msg-fresh", ThreadID: freshThread, Content: "First turn", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg2)
	pool.Enqueue(msg2)

	select {
	case id := <-completedCh:
		if id != "msg-fresh" {
			t.Fatalf("unexpected message completed: %s", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for msg-fresh")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(capturedConfigs) != 2 {
		t.Fatalf("expected 2 daemon spawns, got %d", len(capturedConfigs))
	}
	if capturedConfigs[0].SessionID != expectedSessID {
		t.Errorf("expected DaemonConfig.SessionID %q, got %q", expectedSessID, capturedConfigs[0].SessionID)
	}
	if capturedConfigs[1].SessionID != "" {
		t.Errorf("expected DaemonConfig.SessionID empty for fresh thread, got %q", capturedConfigs[1].SessionID)
	}
}

func TestWorker_ThreadWorker_ActiveTasksPreventReap(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	sessID := "b1111111-2222-3333-4444-555555555555"
	mockSpawner := newScriptedDaemonSpawner(sessID,
		`{"event":"result","result":{"status":"SUCCESS","response":"ok","usage":{"total_tokens":5}}}`+"\n",
	)
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer func() {
		_ = procPool.Close()
	}()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
	})

	threadID := "thread-active-tasks-reap"
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

	ctx := context.Background()
	daemon, err := pool.ProcessPool().GetOrCreate(ctx, threadID, "")
	if err != nil {
		t.Fatalf("failed to create daemon: %v", err)
	}
	daemon.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "task-busy-prevent-reap",
		StartedAt: time.Now(),
	})

	msg := db.Message{ID: "msg-reap-1", ThreadID: threadID, Content: "Hello daemon reap test", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}

	time.Sleep(60 * time.Millisecond)
	pool.mu.Lock()
	_, workerStillActive := pool.threadChs[threadID]
	pool.mu.Unlock()
	if !workerStillActive {
		t.Errorf("expected worker to remain active because background task was running")
	}

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

	tests := []struct {
		name     string
		agyBin   string
		threadID string
		sessID   string
		script   string
	}{
		{
			name:     "InvalidBinary",
			agyBin:   "/invalid/nonexistent/bin/path",
			threadID: "thread-bad-daemon",
			sessID:   "c1111111-2222-3333-4444-555555555555",
		},
		{
			name:     "ScriptExitError",
			threadID: "thread-err-daemon",
			sessID:   "d1111111-2222-3333-4444-555555555555",
			script:   "#!/bin/sh\nexit 1\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			binPath := tc.agyBin
			if tc.script != "" {
				binPath = filepath.Join(t.TempDir(), "mock_err.sh")
				_ = os.WriteFile(binPath, []byte(tc.script), 0755)
			}

			cfg := config.NewTestConfig(func(d *config.ConfigData) {
				d.AgyBin = binPath
				d.DataDir = tempData
			})

			failedCh := make(chan struct{})
			pool := New(cfg, WorkerPoolConfig{
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
			defer pool.Stop()

			_ = store.SaveSessionID(context.Background(), tc.threadID, tc.sessID)
			msg := db.Message{
				ID:        "msg-" + tc.threadID,
				ThreadID:  tc.threadID,
				Content:   "fail",
				Status:    db.StatusPending,
				CreatedAt: time.Now(),
			}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)

			select {
			case <-failedCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for failure completion")
			}
		})
	}
}

func TestWorkerPool_PersistentDaemonTurns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		threadID            string
		daemonUUID          string
		presetSessionInDB   string
		turnResponses       []string
		transcriptContent   string
		foreignDiskUUID     string
		expectedFinalStatus string
		wantDeliveredSubstr string
		wantSavedSession    string
		checkQuotaSchedule  bool
		checkQuotaDbError   bool
		expectSessionReject string
	}{
		{
			name:                "ColdStart_Success",
			threadID:            "thread-cold-persist-success",
			daemonUUID:          "c1111111-2222-3333-4444-555555555555",
			turnResponses:       []string{`{"event":"result","result":{"status":"SUCCESS","response":"Cold start completed successfully!","usage":{"total_tokens":25}}}` + "\n"},
			expectedFinalStatus: db.StatusCompleted,
			wantDeliveredSubstr: "Cold start completed successfully!",
			wantSavedSession:    "c1111111-2222-3333-4444-555555555555",
		},
		{
			name:                "ColdStart_RejectsForeignSessionOnDisk",
			threadID:            "thread-cold-persist-no-foreign",
			daemonUUID:          "d1111111-2222-3333-4444-555555555555",
			turnResponses:       []string{`{"event":"result","result":{"status":"SUCCESS","response":"Turn completed","usage":{"total_tokens":15}}}` + "\n"},
			foreignDiskUUID:     "f1111111-2222-3333-4444-555555555555",
			expectedFinalStatus: db.StatusCompleted,
			wantDeliveredSubstr: "Turn completed",
			wantSavedSession:    "d1111111-2222-3333-4444-555555555555",
			expectSessionReject: "f1111111-2222-3333-4444-555555555555",
		},
		{
			name:                "EmptyResponse_TranscriptFallback",
			threadID:            "thread-empty-resp-fallback",
			daemonUUID:          "e1111111-2222-3333-4444-555555555555",
			turnResponses:       []string{`{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":10}}}` + "\n"},
			transcriptContent:   `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered from transcript!"}` + "\n",
			expectedFinalStatus: db.StatusCompleted,
			wantDeliveredSubstr: "Recovered from transcript!",
		},
		{
			name:       "DaemonEmptyResponse_TranscriptFallback",
			threadID:   "thread-daemon-empty-fallback",
			daemonUUID: "e3333333-4444-5555-6666-777777777777",
			turnResponses: []string{
				`{"event":"step_update","step_update":{"step_index":1,"step_type":"tool","tool_name":"run_command"}}` + "\n" +
					`{"event":"result","result":{"status":"SUCCESS","response":""}}` + "\n",
			},
			transcriptContent:   `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered after daemon empty response!"}` + "\n",
			expectedFinalStatus: db.StatusCompleted,
			wantDeliveredSubstr: "Recovered after daemon empty response!",
		},
		{
			name:              "QuotaExhaustion_FromTranscript",
			threadID:          "thread-quota-exhaustion-test",
			daemonUUID:        "f2222222-3333-4444-5555-666666666666",
			presetSessionInDB: "f2222222-3333-4444-5555-666666666666",
			turnResponses:     []string{`{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":0}}}` + "\n"},
			transcriptContent: fmt.Sprintf(`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 4h51m59s.","created_at":%q}`+"\n",
				time.Now().UTC().Format(time.RFC3339)),
			expectedFinalStatus: db.StatusFailed,
			wantDeliveredSubstr: "resets in",
			checkQuotaSchedule:  true,
			checkQuotaDbError:   true,
		},
		{
			name:              "QuotaPause_RotatesBloatedSession",
			threadID:          "thread-quota-bloat-rotation",
			daemonUUID:        "b3333333-4444-5555-6666-777777777777",
			presetSessionInDB: "b3333333-4444-5555-6666-777777777777",
			turnResponses:     []string{`{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":0}}}` + "\n"},
			transcriptContent: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2h30m.","created_at":%q}
`, session.DefaultMaxSessionSteps, time.Now().UTC().Format(time.RFC3339)),
			expectedFinalStatus: db.StatusFailed,
			wantSavedSession:    "b3333333-4444-5555-6666-777777777777",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			tmpHome := t.TempDir()
			tempData := t.TempDir()

			mockSpawner := newScriptedDaemonSpawner(tc.daemonUUID, tc.turnResponses...)
			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
			defer func() {
				_ = procPool.Close()
			}()

			cfg := config.NewTestConfig(func(d *config.ConfigData) {
				d.DataDir = tempData
			})

			if tc.presetSessionInDB != "" {
				_ = store.SaveSessionID(context.Background(), tc.threadID, tc.presetSessionInDB)
			}

			if tc.transcriptContent != "" {
				sessDir := filepath.Join(tempData, "brain", tc.daemonUUID, ".system_generated", "logs")
				_ = os.MkdirAll(sessDir, 0755)
				_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(tc.transcriptContent), 0644)
			}

			if tc.foreignDiskUUID != "" {
				brainDir := filepath.Join(tempData, "brain", tc.foreignDiskUUID)
				_ = os.MkdirAll(brainDir, 0755)
				futureTime := time.Now().Add(5 * time.Second)
				_ = os.Chtimes(brainDir, futureTime, futureTime)
			}

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
					if status == tc.expectedFinalStatus {
						select {
						case <-doneCh:
						default:
							close(doneCh)
						}
					}
				},
			})
			defer pool.Stop()

			msgID := "msg-" + tc.threadID
			msg := db.Message{
				ID:        msgID,
				ThreadID:  tc.threadID,
				Content:   "run turn",
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

			if tc.wantDeliveredSubstr != "" && !strings.Contains(deliveredText, tc.wantDeliveredSubstr) {
				t.Errorf("expected delivered text to contain %q, got: %q", tc.wantDeliveredSubstr, deliveredText)
			}

			if tc.wantSavedSession != "" {
				savedSess, err := store.GetSessionID(context.Background(), tc.threadID)
				if err != nil {
					t.Fatalf("failed to query session id: %v", err)
				}
				if savedSess != tc.wantSavedSession {
					t.Errorf("expected session synchronized to %q, got %q", tc.wantSavedSession, savedSess)
				}
			}

			if tc.expectSessionReject != "" {
				savedSess, _ := store.GetSessionID(context.Background(), tc.threadID)
				if savedSess == tc.expectSessionReject {
					t.Fatalf("security violation: worker latched newer foreign session %q from disk instead of daemon session", tc.expectSessionReject)
				}
			}

			if tc.checkQuotaDbError {
				dbMsg, err := store.GetMessage(context.Background(), msgID)
				if err != nil || dbMsg == nil {
					t.Fatalf("failed to query message: %v", err)
				}
				if !strings.Contains(dbMsg.ErrorMessage, "[QUOTA_PAUSED") {
					t.Errorf("expected error message to contain [QUOTA_PAUSED], got %q", dbMsg.ErrorMessage)
				}
			}

			if tc.checkQuotaSchedule {
				schedules, err := store.GetAllOneShotSchedules(context.Background(), tc.threadID)
				if err != nil {
					t.Fatalf("failed to list one-shot schedules: %v", err)
				}
				found := false
				for _, s := range schedules {
					if s.ThreadID == tc.threadID && strings.Contains(s.Prompt, "[QUOTA_RETRY]") {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected one-shot retry schedule for thread %s with [QUOTA_RETRY]", tc.threadID)
				}
			}
		})
	}
}

func TestWorkerPool_TransientRetry_RotatesBloatedSession(t *testing.T) {
	tmpHome := t.TempDir()
	tempData := t.TempDir()
	store := setupTestStore(t)

	validUUID := "c4444444-5555-6666-7777-888888888888"
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
				bloatedContent := fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"tool runaway","created_at":%q}`+"\n",
					session.DefaultMaxSessionSteps+5, time.Now().UTC().Format(time.RFC3339))
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

	msg := db.Message{ID: "msg-transient-bloated", ThreadID: threadID, Content: "run transient test", Status: db.StatusPending, CreatedAt: time.Now()}
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
	if sessionsSeen[0] != validUUID || sessionsSeen[1] != validUUID {
		t.Errorf("expected attempts to preserve session ID %q across retry, got: %v", validUUID, sessionsSeen)
	}
	prevSess, _ := store.GetPreviousSessionID(context.Background(), threadID)
	if prevSess != "" {
		t.Errorf("expected previous session ID to be empty, got %q", prevSess)
	}
}

func TestTurnExecution_GetRecentThreadMessages_DiscordAPI(t *testing.T) {
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
			Author:    &discordgo.User{ID: "169260920550195200", Username: "arcane103"},
			Mentions:  []*discordgo.User{{ID: "bot-123", Username: "Aerial"}},
			ReferencedMessage: &discordgo.Message{
				Author: &discordgo.User{Username: "Aerial"},
			},
		},
		{
			ID:        "1552732003076669001",
			ChannelID: "1552732003076669460",
			Content:   "Weather widget looks good",
			Timestamp: t1,
			Author:    &discordgo.User{ID: "169260920550195200", Username: "arcane103"},
		},
		{
			ID:        "1552732003076669000",
			ChannelID: "1552732003076669460",
			Content:   "Webhook notification",
			Timestamp: time.Time{},
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

	pool := &WorkerPool{cfg: WorkerPoolConfig{DiscordSession: dg}}
	te := &turnExecution{
		pool:  pool,
		burst: []db.Message{{ID: "1552732003076669099"}},
	}

	msgs, err := te.getRecentThreadMessages("1552732003076669460", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].ID != "1552732003076669000" || msgs[0].AuthorName != "Webhook" {
		t.Errorf("oldest message mismatch: %+v", msgs[0])
	}
	if msgs[2].ID != "1552732003076669002" || msgs[2].Metadata.ReplyingToAuthor != "@Aerial" {
		t.Errorf("newest message mismatch: %+v", msgs[2])
	}
}

func TestTurnExecution_GetRecentThreadMessages_FallbackFiltered(t *testing.T) {
	t.Parallel()
	fakeStore := db.NewFakeStore()
	ctx := context.Background()

	_ = fakeStore.InsertMessage(ctx, db.Message{
		ID:         "msg-1",
		ThreadID:   "1552732003076669460",
		AuthorName: "arcane103",
		Content:    "Normal chat message",
		CreatedAt:  time.Now().Add(-2 * time.Minute),
	})
	_ = fakeStore.InsertMessage(ctx, db.Message{
		ID:            "msg-sched",
		ThreadID:      "1552732003076669460",
		AuthorName:    "Scheduler",
		Content:       "Check on PR status and deployment prompt injection...",
		ScheduleRunID: "run-123",
		CreatedAt:     time.Now().Add(-1 * time.Minute),
	})

	dg, _ := discordgo.New("Bot test")
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("discord simulated network error")
	})

	pool := &WorkerPool{cfg: WorkerPoolConfig{DiscordSession: dg, Store: fakeStore}}
	te := &turnExecution{pool: pool}

	msgs, err := te.getRecentThreadMessages("1552732003076669460", 0)
	if err != nil || len(msgs) != 1 || msgs[0].ID != "msg-1" {
		t.Fatalf("expected 1 message msg-1, got %d (err: %v)", len(msgs), err)
	}

	msgsClamped, err := te.getRecentThreadMessages("1552732003076669460", 200)
	if err != nil || len(msgsClamped) != 1 {
		t.Errorf("expected 1 clamped message, got %d", len(msgsClamped))
	}

	errStore := &mockErrRecentStore{}
	teErr := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{Store: errStore}}}
	if _, err := teErr.getRecentThreadMessages("1552732003076669460", 10); err == nil {
		t.Errorf("expected error on store failure")
	}

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
		_ = procPool.Close()
	}()

	ctx := context.Background()
	_, _ = procPool.GetOrCreate(ctx, "thread-evict-1", "")

	pool := &WorkerPool{
		cfg:         WorkerPoolConfig{Store: fakeStore},
		processPool: procPool,
	}
	te := &turnExecution{pool: pool, threadID: "thread-evict-1"}

	te.rotateSessionID("thread-evict-1", "new-sess-1")
	if !procPool.HasDaemon("thread-evict-1") {
		t.Errorf("expected daemon to remain when newSessionID is non-empty")
	}

	te.rotateSessionID("thread-evict-1", "")
	storedSess, _ := fakeStore.GetSessionID(ctx, "thread-evict-1")
	if storedSess != "" {
		t.Errorf("expected empty session in store after rotation, got %q", storedSess)
	}

	teNil := &turnExecution{threadID: "thread-evict-2"}
	teNil.rotateSessionID("thread-evict-2", "")
	var teNull *turnExecution
	teNull.rotateSessionID("thread-evict-3", "")
}

func runSessionDBRotationTurn(t *testing.T, name, channelMode string, preWriteDBBytes, preWriteWALBytes int64, maxAttempts int, configureCfg func(*WorkerPoolConfig, string, string)) {
	t.Helper()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	oldSess := "sess-" + name
	convDir := filepath.Join(tmpDir, "conversations")
	_ = os.MkdirAll(convDir, 0755)
	if preWriteDBBytes > 0 {
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, preWriteDBBytes), 0644)
	}
	if preWriteWALBytes > 0 {
		_ = os.WriteFile(filepath.Join(convDir, oldSess+".db-wal"), make([]byte, preWriteWALBytes), 0644)
	}

	threadID := "thread-" + name
	_ = saveSessionID(store, threadID, oldSess)
	_, _ = incrementSessionTurnCount(store, threadID)

	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: channelMode},
		},
	})

	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	wpCfg := WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    maxAttempts,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
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
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse(sessionID, "OK"), "", 0, nil
		},
	}

	if configureCfg != nil {
		configureCfg(&wpCfg, convDir, oldSess)
	}

	pool := New(appCfg, wpCfg)
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-" + name, ThreadID: threadID, Content: "test message"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message completion")
	}

	curSess, _ := getSessionID(store, threadID)
	if curSess != oldSess {
		t.Errorf("Expected session %q preserved, got: %q", oldSess, curSess)
	}
}

func TestWorker_SessionDBRotation_PreTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		channelMode      string
		preWriteDBBytes  int64
		preWriteWALBytes int64
	}{
		{name: "ChannelMode_DBLimit", channelMode: "channel", preWriteDBBytes: session.DefaultMaxSessionDBBytes + 1024, preWriteWALBytes: 600 * 1024},
		{name: "ChannelMode_DBLimit_WALIgnored", channelMode: "channel", preWriteDBBytes: 1000 * 1024, preWriteWALBytes: 600 * 1024},
		{name: "ThreadMode_DBLimit", channelMode: "thread", preWriteDBBytes: session.DefaultMaxSessionDBBytes + 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSessionDBRotationTurn(t, tc.name, tc.channelMode, tc.preWriteDBBytes, tc.preWriteWALBytes, 1, nil)
		})
	}
}

func TestWorker_SessionDBRotation_QuotaPause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		preWriteDBBytes int64
	}{
		{name: "DBAboveThreshold", preWriteDBBytes: session.DefaultMaxQuotaPauseDBBytes + 1024},
		{name: "DBBelowThreshold", preWriteDBBytes: 200 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSessionDBRotationTurn(t, tc.name, "thread", tc.preWriteDBBytes, 0, 1, func(cfg *WorkerPoolConfig, convDir, oldSess string) {
				cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Resets in 30s.", 1, errors.New("exit code 1")
				}
			})
		})
	}
}

func TestWorker_SessionDBRotation_PostExecutionPreserved(t *testing.T) {
	t.Parallel()
	runSessionDBRotationTurn(t, "post-execution", "thread", 0, 0, 1, func(cfg *WorkerPoolConfig, convDir, oldSess string) {
		cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, session.DefaultMaxSessionDBBytes+500), 0644)
			return mockJSONResponse(oldSess, "Execution heavy DB output"), "", 0, nil
		}
	})
}

func TestWorker_SessionDBRotation_WatchdogRetryPreservesSession(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var attempts []string
	oldSessName := "sess-watchdog"

	runSessionDBRotationTurn(t, "watchdog", "thread", 0, 0, 2, func(cfg *WorkerPoolConfig, convDir, oldSess string) {
		cfg.RunnerWithOptionsFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			attempts = append(attempts, sessionID)
			currentAttempt := len(attempts)
			mu.Unlock()

			if currentAttempt == 1 {
				_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, session.DefaultMaxSessionDBBytes+1024), 0644)
				return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
			}
			return mockJSONResponse(sessionID, "Recovered on cold retry"), "", 0, nil
		}
	})

	if len(attempts) != 2 {
		t.Fatalf("Expected 2 attempts, got: %d", len(attempts))
	}
	for i, a := range attempts {
		if a != oldSessName {
			t.Errorf("Expected attempt %d to use session %q, got: %q", i+1, oldSessName, a)
		}
	}
}

func TestWorker_SessionDBRotation_TransientRetryPreservesSession(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var attempts []string
	oldSessName := "sess-transient"

	runSessionDBRotationTurn(t, "transient", "thread", 0, 0, 2, func(cfg *WorkerPoolConfig, convDir, oldSess string) {
		cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			attempts = append(attempts, sessionID)
			currentAttempt := len(attempts)
			mu.Unlock()

			if currentAttempt == 1 {
				_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, session.DefaultMaxSessionDBBytes+1024), 0644)
				return `{"status": "ERROR", "error": "temporary network timeout"}`, "", 0, nil
			}
			return mockJSONResponse(sessionID, "Recovered on cold retry"), "", 0, nil
		}
	})

	if len(attempts) != 2 {
		t.Fatalf("Expected 2 attempts, got: %d", len(attempts))
	}
	for i, a := range attempts {
		if a != oldSessName {
			t.Errorf("Expected attempt %d to use session %q, got: %q", i+1, oldSessName, a)
		}
	}
}

func TestWorkerPool_CapacityBlipMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		maxAttempts          int
		runnerErr            string
		wantAttempts         int
		wantFinalStatus      string
		wantErrContains      string
		wantDeliveryContains string
		checkGlobalLockZero  bool
		checkNoSchedules     bool
	}{
		{
			name:                 "LocalRetry_Success",
			maxAttempts:          2,
			runnerErr:            "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.",
			wantAttempts:         2,
			wantFinalStatus:      db.StatusCompleted,
			wantDeliveryContains: "Recovered on attempt 2!",
			checkGlobalLockZero:  true,
		},
		{
			name:                 "Exhaustion_LocalFailure",
			maxAttempts:          1,
			runnerErr:            "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.",
			wantAttempts:         1,
			wantFinalStatus:      db.StatusFailed,
			wantErrContains:      "[CAPACITY_EXHAUSTED",
			wantDeliveryContains: "capacity",
			checkGlobalLockZero:  true,
			checkNoSchedules:     true,
		},
		{
			name:                 "WithoutCountdown_Fallback70s",
			maxAttempts:          1,
			runnerErr:            "API error: RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model.",
			wantAttempts:         1,
			wantFinalStatus:      db.StatusFailed,
			wantErrContains:      "[CAPACITY_EXHAUSTED",
			wantDeliveryContains: "capacity",
			checkGlobalLockZero:  true,
			checkNoSchedules:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				SessionManager:     sessMgr,
				Store:              store,
				TimeoutMinutes:     1,
				BackoffBase:        5 * time.Millisecond,
				RetryDelayOverride: 10 * time.Millisecond,
				MaxAttempts:        tt.maxAttempts,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					mu.Lock()
					attemptCount++
					cur := attemptCount
					mu.Unlock()

					if cur == 1 && tt.wantAttempts > 1 {
						return "", tt.runnerErr, 1, errors.New("exit code 1")
					} else if cur == 1 && tt.wantAttempts == 1 {
						return "", tt.runnerErr, 1, errors.New("exit code 1")
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

			threadID := "thread-" + strings.ToLower(tt.name)
			msgID := "msg-" + strings.ToLower(tt.name)
			msg := db.Message{ID: msgID, ThreadID: threadID, Content: "hello " + tt.name}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for message completed")
			}
			pool.Stop()

			mu.Lock()
			attempts := attemptCount
			delivered := deliveredText
			mu.Unlock()

			if attempts != tt.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}

			dbMsg, err := store.GetMessage(context.Background(), msgID)
			if err != nil || dbMsg == nil {
				t.Fatalf("failed to query message: %v", err)
			}
			if dbMsg.Status != tt.wantFinalStatus {
				t.Errorf("dbMsg.Status = %s, want %s (err: %s)", dbMsg.Status, tt.wantFinalStatus, dbMsg.ErrorMessage)
			}
			if tt.wantErrContains != "" && !strings.Contains(dbMsg.ErrorMessage, tt.wantErrContains) {
				t.Errorf("expected dbMsg.ErrorMessage to contain %q, got %q", tt.wantErrContains, dbMsg.ErrorMessage)
			}
			if tt.wantDeliveryContains != "" && !strings.Contains(delivered, tt.wantDeliveryContains) {
				t.Errorf("expected deliveredText to contain %q, got %q", tt.wantDeliveryContains, delivered)
			}

			if tt.checkGlobalLockZero {
				if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
					t.Errorf("expected quotaLockedUntil to remain 0, got %d", lockedUntil)
				}
			}
			if tt.checkNoSchedules {
				schedules, _ := store.GetAllOneShotSchedules(context.Background(), threadID)
				if len(schedules) > 0 {
					t.Errorf("expected 0 one-shot schedules, got %d", len(schedules))
				}
			}
		})
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
		SessionManager:     sessMgr,
		Store:              store,
		TimeoutMinutes:     1,
		BackoffBase:        5 * time.Millisecond,
		RetryDelayOverride: 10 * time.Millisecond,
		MaxAttempts:        2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			if strings.Contains(prompt, "Thread A") {
				mu.Lock()
				threadAAttempts++
				att := threadAAttempts
				mu.Unlock()
				if att == 1 {
					return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
				}
				return mockJSONResponse(uuid.New().String(), "Thread A success"), "", 0, nil
			}
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

	pool.Enqueue(msgA)
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

	select {
	case <-startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for attempt 1")
	}

	// Cancel pool context while worker is in capacity backoff sleep (delay is 2s-3s)
	pool.cancel()
	waitPoolWorkersDone(t, pool, 3*time.Second)

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

	tests := []struct {
		name       string
		attempt    int
		resetDur   time.Duration
		minFloor   time.Duration
		maxCeiling time.Duration
	}{
		{name: "Attempt1_Floor30s", attempt: 1, resetDur: 0, minFloor: 30 * time.Second, maxCeiling: 33 * time.Second},
		{name: "Attempt2_Floor60s", attempt: 2, resetDur: 0, minFloor: 60 * time.Second, maxCeiling: 63 * time.Second},
		{name: "ResetDurOverride_75s", attempt: 1, resetDur: 75 * time.Second, minFloor: 75 * time.Second, maxCeiling: 78 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 20; i++ {
				d := calculateCapacityBackoff(tt.attempt, tt.resetDur)
				if d < tt.minFloor {
					t.Fatalf("backoff floor expected >= %v, got %v", tt.minFloor, d)
				}
				if d > tt.maxCeiling {
					t.Fatalf("backoff jitter expected <= %v, got %v", tt.maxCeiling, d)
				}
			}
		})
	}
}

func TestWorkerPool_ProgressiveCapacityBackoff_ContextCancellation(t *testing.T) {
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
	pool.cancel()
	waitPoolWorkersDone(t, pool, 3*time.Second)

	typingMu.Lock()
	if !typingActive {
		t.Errorf("Expected typing to have started")
	}
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

	sinkNoDeltas := newDiscordTurnSink(nil)
	expectedErr := errors.New("hard failure")
	sinkNoDeltas.OnError(expectedErr)
	_, err = sinkNoDeltas.Wait(context.Background())
	if err == nil || !errors.Is(err, expectedErr) {
		t.Errorf("unexpected error received: %v", err)
	}
}

func TestDiscordTurnSink_StatusUpdaterLifecycle(t *testing.T) {
	t.Parallel()
	updater := NewStatusUpdater(nil, "thread-123", true, WithStatusMockFuncs(
		func(channelID, text string) (string, error) { return "msg-1", nil },
		func(channelID, messageID, text string) error { return nil },
		func(channelID, messageID string) error { return nil },
	))
	defer updater.Stop()

	sink := newDiscordTurnSink(updater)

	assertUpdater := func(step, wantPhase, wantTool, wantCmd string) {
		t.Helper()
		updater.mu.Lock()
		defer updater.mu.Unlock()
		if updater.phase != wantPhase {
			t.Errorf("[%s] phase = %q, want %q", step, updater.phase, wantPhase)
		}
		if wantTool != "" && updater.activeTool != wantTool {
			t.Errorf("[%s] activeTool = %q, want %q", step, updater.activeTool, wantTool)
		}
		if wantCmd != "" && updater.activeCommand != wantCmd {
			t.Errorf("[%s] activeCommand = %q, want %q", step, updater.activeCommand, wantCmd)
		}
	}

	sink.OnThinking()
	assertUpdater("thinking", "thinking", "", "")

	sink.OnToolCall("run_command", "echo hello")
	assertUpdater("tool_call", "tool", "run_command", "echo")

	sink.OnToolCompleted("run_command", "native", 100*time.Millisecond, "ok")
	assertUpdater("tool_completed", "thinking", "", "")

	sink.OnTextDelta("Streaming response token")
	assertUpdater("text_delta", "responding", "", "")

	sink.OnThinking()
	assertUpdater("thinking", "thinking", "", "")

	sink.OnTextDelta("   \n\t  ")
	assertUpdater("whitespace_delta", "thinking", "", "")

	sink.OnToolCall("get_me", "")
	assertUpdater("tool_call", "tool", "get_me", "")

	sink.OnToolCompleted("get_me", "github", 50*time.Millisecond, "error")
	assertUpdater("tool_error", "thinking", "", "")

	nilSink := newDiscordTurnSink(nil)
	nilSink.OnThinking()
	nilSink.OnToolCall("run_command", "ls")
	nilSink.OnToolCompleted("run_command", "native", time.Millisecond, "ok")
	nilSink.OnTextDelta("delta")
	nilSink.OnSkillActivated("skill", "source")
}

func TestWorker_DualPoolRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		effort                string
		useFallbackNilLowPool bool
		useFallbackNilMedPool bool
		wantPrimaryCount      int
		wantLowCount          int
		wantMedCount          int
	}{
		{name: "LowEffortRoutesToLowPool", effort: "low", wantPrimaryCount: 0, wantLowCount: 1, wantMedCount: 0},
		{name: "MediumEffortRoutesToMediumPool", effort: "medium", wantPrimaryCount: 0, wantLowCount: 0, wantMedCount: 1},
		{name: "NormalRoutesToPrimaryPool", effort: "", wantPrimaryCount: 1, wantLowCount: 0, wantMedCount: 0},
		{name: "FallbackNilLowPoolRoutesToPrimary", effort: "low", useFallbackNilLowPool: true, wantPrimaryCount: 1, wantLowCount: 0, wantMedCount: 0},
		{name: "FallbackNilMediumPoolRoutesToPrimary", effort: "medium", useFallbackNilMedPool: true, wantPrimaryCount: 1, wantLowCount: 0, wantMedCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			tmpHome := t.TempDir()
			tempData := t.TempDir()

			var primarySpawnCount, lowSpawnCount, medSpawnCount atomic.Int32

			primarySpawner := newScriptedDaemonSpawner("c1111111-2222-3333-4444-555555555555",
				"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"executed on primary\",\"usage\":{\"total_tokens\":10}}}\n")
			oldPrimarySpawn := primarySpawner.SpawnFn
			primarySpawner.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				primarySpawnCount.Add(1)
				return oldPrimarySpawn(ctx, cfg)
			}

			lowSpawner := newScriptedDaemonSpawner("d1111111-2222-3333-4444-555555555555",
				"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"executed on low effort\",\"usage\":{\"total_tokens\":5}}}\n")
			oldLowSpawn := lowSpawner.SpawnFn
			lowSpawner.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				lowSpawnCount.Add(1)
				return oldLowSpawn(ctx, cfg)
			}

			medSpawner := newScriptedDaemonSpawner("f1111111-2222-3333-4444-555555555555",
				"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"executed on medium effort\",\"usage\":{\"total_tokens\":8}}}\n")
			oldMedSpawn := medSpawner.SpawnFn
			medSpawner.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				medSpawnCount.Add(1)
				return oldMedSpawn(ctx, cfg)
			}

			primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
			defer primaryPool.Close()

			var lowPool runner.AgentPool
			if !tt.useFallbackNilLowPool {
				lp := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
				defer lp.Close()
				lowPool = lp
			}

			var medPool runner.AgentPool
			if !tt.useFallbackNilMedPool {
				mp := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-medium"}, medSpawner)
				defer mp.Close()
				medPool = mp
			}

			cfg := config.NewTestConfig(func(d *config.ConfigData) {
				d.DataDir = tempData
				d.LowEffortModel = "gemini-3.8-flash-low"
				d.MediumEffortModel = "gemini-3.8-flash-medium"
			})

			doneCh := make(chan struct{}, 1)
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool:             primaryPool,
				LowEffortProcessPool:    lowPool,
				MediumEffortProcessPool: medPool,
				SessionManager:          session.New(tmpHome, tempData),
				Store:                   store,
				TimeoutMinutes:          1,
				OnMessageCompleted: func(msg db.Message, status string) {
					if status == db.StatusCompleted {
						select {
						case doneCh <- struct{}{}:
						default:
						}
					}
				},
			})
			pool.Start()
			defer pool.Stop()

			threadID := "thread-" + strings.ToLower(tt.name)
			msg := db.Message{
				ID:        "msg-" + strings.ToLower(tt.name),
				ThreadID:  threadID,
				Content:   "run task",
				Effort:    tt.effort,
				Status:    db.StatusPending,
				CreatedAt: time.Now(),
			}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout waiting for turn completion")
			}

			if p := int(primarySpawnCount.Load()); p != tt.wantPrimaryCount {
				t.Errorf("primarySpawnCount = %d, want %d", p, tt.wantPrimaryCount)
			}
			if l := int(lowSpawnCount.Load()); l != tt.wantLowCount {
				t.Errorf("lowSpawnCount = %d, want %d", l, tt.wantLowCount)
			}
			if m := int(medSpawnCount.Load()); m != tt.wantMedCount {
				t.Errorf("medSpawnCount = %d, want %d", m, tt.wantMedCount)
			}
		})
	}
}

func TestWorkerPool_LowEffortAndPrimary_ConcurrentExecutionSameThread(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	lowStartedCh := make(chan struct{}, 1)
	lowBlockCh := make(chan struct{})
	normDoneCh := make(chan struct{})
	lowDoneCh := make(chan struct{})

	primarySpawner := newScriptedDaemonSpawner("c1111111-2222-3333-4444-555555555555",
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"primary reply\",\"usage\":{\"total_tokens\":10}}}\n")

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
					select {
					case lowStartedCh <- struct{}{}:
					default:
					}
					<-lowBlockCh
					_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"low reply\",\"usage\":{\"total_tokens\":5}}}\n"))
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

	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowPool,
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if msg.ID == "msg-norm-concurrent" && status == db.StatusCompleted {
				close(normDoneCh)
			}
			if msg.ID == "msg-low-concurrent" && status == db.StatusCompleted {
				close(lowDoneCh)
			}
		},
	})
	defer pool.Stop()

	threadID := "th-shared-concurrent"

	lowMsg := db.Message{
		ID:        "msg-low-concurrent",
		ThreadID:  threadID,
		Content:   "long low-effort task",
		Effort:    "low",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), lowMsg)
	pool.Enqueue(lowMsg)

	select {
	case <-lowStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for low-effort turn to start")
	}

	normMsg := db.Message{
		ID:        "msg-norm-concurrent",
		ThreadID:  threadID,
		Content:   "user asks what is happening",
		Effort:    "medium",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(context.Background(), normMsg)
	pool.Enqueue(normMsg)

	select {
	case <-normDoneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("primary message was blocked by low-effort task on same thread")
	}

	close(lowBlockCh)
	select {
	case <-lowDoneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for low-effort task to complete")
	}
}

func TestWorker_EphemeralLowEffortPool_SkipsColdStartContextAndRotates(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	primarySessUUID := "11111111-1111-1111-1111-111111111111"
	prevSessUUID := "99999999-9999-9999-9999-999999999999"

	var spawnSeq int
	var mu sync.Mutex
	var capturedPrompts []string

	lowSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			mu.Lock()
			spawnSeq++
			seq := spawnSeq
			mu.Unlock()
			sessUUID := fmt.Sprintf("22222222-2222-2222-2222-%012d", seq)
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n", sessUUID)))
				buf := make([]byte, 16384)
				for {
					n, rErr := inR.Read(buf)
					if rErr != nil {
						return
					}
					if n > 0 {
						mu.Lock()
						capturedPrompts = append(capturedPrompts, string(buf[:n]))
						mu.Unlock()
						_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ephemeral ok\",\"conversation_id\":%q,\"usage\":{\"total_tokens\":5}}}\n", sessUUID)))
					}
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345 + seq), nil
		},
	}
	primarySpawner := runner.NewMockSpawner()

	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowUnderlying := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowUnderlying.Close()

	lowPool := runner.NewInterchangeablePool(lowUnderlying, runner.InterchangeablePoolConfig{
		WorkerCount: 1,
		KeyPrefix:   "discord:worker",
	})
	defer lowPool.Close()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = tempData
		d.LowEffortModel = "gemini-3.8-flash-low"
	})

	doneCh := make(chan struct{}, 2)
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:          primaryPool,
		LowEffortProcessPool: lowPool,
		SessionManager:       session.New(tmpHome, tempData),
		Store:                store,
		TimeoutMinutes:       1,
		HistoryFetcher: func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			return []HistoryMessage{
				{ID: "hist-1", AuthorName: "alex", Content: "prior chat history should not be injected", CreatedAt: time.Now().Add(-5 * time.Minute)},
			}, nil
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				doneCh <- struct{}{}
			}
		},
	})
	defer pool.Stop()

	threadID := "1560318118792790111"
	ctx := context.Background()
	_ = store.SaveSessionID(ctx, threadID, prevSessUUID)
	_ = store.RotateSessionID(ctx, threadID, primarySessUUID)
	_ = store.SaveThreadSummary(ctx, threadID, "Cached summary that must not be injected", "hist-1")

	msg1 := db.Message{
		ID:        "msg-ephem-1",
		ThreadID:  threadID,
		Content:   "execute vanadis chunk 1",
		Effort:    "low",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(ctx, msg1)
	pool.Enqueue(msg1)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ephemeral turn 1")
	}

	lowPool.WaitBackground()

	msg2 := db.Message{
		ID:        "msg-ephem-2",
		ThreadID:  threadID,
		Content:   "execute vanadis chunk 2",
		Effort:    "low",
		Status:    db.StatusPending,
		CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(ctx, msg2)
	pool.Enqueue(msg2)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ephemeral turn 2")
	}

	lowPool.WaitBackground()

	mu.Lock()
	prompts := append([]string(nil), capturedPrompts...)
	spawns := spawnSeq
	mu.Unlock()

	if len(prompts) != 2 {
		t.Fatalf("expected 2 captured prompts, got %d", len(prompts))
	}
	for i, p := range prompts {
		if strings.Contains(p, "<THREAD_SUMMARY>") || strings.Contains(p, "Cached summary that must not be injected") {
			t.Errorf("turn %d prompt unexpectedly contained <THREAD_SUMMARY>: %s", i+1, p)
		}
		if strings.Contains(p, "<CHANNEL_HISTORY>") || strings.Contains(p, "prior chat history should not be injected") {
			t.Errorf("turn %d prompt unexpectedly contained <CHANNEL_HISTORY>: %s", i+1, p)
		}
		if strings.Contains(p, "<PREVIOUS_SESSION>") || strings.Contains(p, prevSessUUID) {
			t.Errorf("turn %d prompt unexpectedly contained <PREVIOUS_SESSION>: %s", i+1, p)
		}
	}
	if spawns < 2 {
		t.Errorf("expected worker daemon rotation between turns (spawns >= 2), got %d", spawns)
	}

	currentSess, err := store.GetSessionID(ctx, threadID)
	if err != nil {
		t.Fatalf("failed to get session ID: %v", err)
	}
	if currentSess != primarySessUUID {
		t.Errorf("expected primary session %s to be untouched, got %s", primarySessUUID, currentSess)
	}
	turnCount, err := store.GetSessionTurnCount(ctx, threadID)
	if err != nil {
		t.Fatalf("failed to get turn count: %v", err)
	}
	if turnCount != 0 {
		t.Errorf("expected primary session turn count to remain 0 across ephemeral turns, got %d", turnCount)
	}
}

func TestWorker_DualPool_AntiFlapping(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	primarySessUUID := "11111111-1111-1111-1111-111111111111"
	lowSessUUID := "22222222-2222-2222-2222-222222222222"

	lowSpawner := newScriptedDaemonSpawner(lowSessUUID,
		fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok low effort\",\"conversation_id\":%q,\"usage\":{\"total_tokens\":5}}}\n", lowSessUUID))
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

	currentSess, err := store.GetSessionID(context.Background(), threadID)
	if err != nil {
		t.Fatalf("failed to get session ID: %v", err)
	}
	if currentSess != primarySessUUID {
		t.Fatalf("ANTI-FLAPPING VIOLATION: expected primary session %s to be preserved, got %s", primarySessUUID, currentSess)
	}
}

func TestWorker_DualPool_ActiveTasksInLowEffortPoolPreventReap(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	lowSpawner := newScriptedDaemonSpawner("e1111111-2222-3333-4444-555555555555",
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok\",\"usage\":{\"total_tokens\":5}}}\n")
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

	ctx := context.Background()
	daemon, err := pool.LowEffortProcessPool().GetOrCreate(ctx, threadID, "")
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

	time.Sleep(60 * time.Millisecond)
	pool.mu.Lock()
	_, workerStillActive := pool.threadChs["low_effort:"+threadID]
	pool.mu.Unlock()

	if !workerStillActive {
		t.Errorf("expected worker to remain active because background task was running in LowEffortProcessPool")
	}
}

func TestWorker_DualPool_YieldTrapFallback(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	tempData := t.TempDir()

	sessID := "f1111111-2222-3333-4444-555555555555"
	sessDir := filepath.Join(tempData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)

	var turnCount atomic.Int32

	primarySpawner := newScriptedDaemonSpawner(sessID,
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"waiting for bg task\",\"usage\":{\"total_tokens\":5}}}\n",
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"all tasks done\",\"usage\":{\"total_tokens\":5}}}\n",
	)
	oldSpawn := primarySpawner.SpawnFn
	primarySpawner.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
		inW, outR, errR, handle, err := oldSpawn(ctx, cfg)
		return inW, outR, errR, handle, err
	}

	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowSpawner := newScriptedDaemonSpawner("d1111111-2222-3333-4444-555555555555")
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
				turnCount.Add(1)
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	ctx := context.Background()
	lowDaemon, err := lowPool.GetOrCreate(ctx, threadID, "")
	if err != nil {
		t.Fatalf("failed to create low daemon: %v", err)
	}
	lowDaemon.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "bg-task-in-low-pool",
		StartedAt: time.Now(),
	})

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

	if tc := int(turnCount.Load()); tc < 1 {
		t.Errorf("expected auto-resumption from yield-trap detection in alternate pool, turnCount=%d", tc)
	}
}

func TestWorker_QuotaPause_PreservesEffort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		effort         string
		expectedEffort string
	}{
		{name: "LowEffort", effort: "low", expectedEffort: "low"},
		{name: "MediumEffort", effort: "medium", expectedEffort: "medium"},
		{name: "HighEffort", effort: "high", expectedEffort: "high"},
		{name: "DefaultEffort", effort: "", expectedEffort: "high"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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

			threadID := "thread-quota-" + tt.name
			msg := db.Message{
				ID:        "msg-quota-" + tt.name,
				ThreadID:  threadID,
				Content:   "run task",
				Effort:    tt.effort,
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
			if schedules[0].Effort != tt.expectedEffort {
				t.Fatalf("expected oneShot.Effort=%q, got %q", tt.expectedEffort, schedules[0].Effort)
			}
		})
	}
}

func TestWorkerPool_SessionRotationInjectsToolActions(t *testing.T) {
	t.Parallel()

	rotationTests := []struct {
		name              string
		maxAttempts       int
		triggerErr        string
		transientErr      bool
		wantAttempts      int
		wantRetainSession bool
	}{
		{
			name:              "EndToEnd_SessionRotationInjectsAndRetainsActions",
			maxAttempts:       3,
			triggerErr:        "RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2s.",
			wantAttempts:      3,
			wantRetainSession: true,
		},
		{
			name:              "EndToEnd_TransientRotationInjectsToolActions",
			maxAttempts:       2,
			triggerErr:        "transient connection reset",
			transientErr:      true,
			wantAttempts:      2,
			wantRetainSession: true,
		},
	}

	for _, tt := range rotationTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpHome := t.TempDir()
			tempData := t.TempDir()
			store := setupTestStore(t)

			validUUID := uuid.New().String()
			sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
			if err := os.MkdirAll(sessDir, 0755); err != nil {
				t.Fatalf("failed to create logs dir: %v", err)
			}

			initTranscript := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"investigate"}` + "\n"
			if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(initTranscript), 0644); err != nil {
				t.Fatalf("failed to write initial transcript: %v", err)
			}

			cfg := config.NewTestConfig(func(d *config.ConfigData) {
				d.DataDir = tempData
			})

			threadID := "thread-rot-" + strings.ToLower(tt.name)
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
				MaxAttempts:        tt.maxAttempts,
				RetryDelayOverride: 10 * time.Millisecond,
				BackoffBase:        5 * time.Millisecond,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					mu.Lock()
					recordedPrompts = append(recordedPrompts, prompt)
					recordedSessions = append(recordedSessions, sessionID)
					attemptNum := len(recordedPrompts)
					mu.Unlock()

					if attemptNum < tt.wantAttempts {
						tNow := time.Now().UTC()
						transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"investigate"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"run_command","args":{"command_line":"docker stats"}}]}
{"step_index":2,"source":"TOOL","type":"GENERIC","status":"DONE","content":"CONTAINER ID aerial-brain MEM USAGE 120MiB"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":%q,"created_at":%q}
`, session.DefaultMaxSessionSteps, tt.triggerErr, tNow.Format(time.RFC3339))
						_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0644)
						return "", tt.triggerErr, 1, errors.New("simulated error")
					}
					return mockJSONResponse(uuid.New().String(), "Completed"), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
				TypingFunc:   func(s *discordgo.Session, channelID string) (stop func()) { return func() {} },
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
				ID:        "msg-rot-" + strings.ToLower(tt.name),
				ThreadID:  threadID,
				Content:   "run investigation",
				Status:    db.StatusPending,
				CreatedAt: time.Now(),
			}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(10 * time.Second):
				t.Fatalf("timed out waiting for turn completion")
			}

			mu.Lock()
			defer mu.Unlock()

			if len(recordedPrompts) != tt.wantAttempts {
				t.Fatalf("expected %d attempts, got %d", tt.wantAttempts, len(recordedPrompts))
			}
			if recordedSessions[0] != validUUID {
				t.Errorf("expected attempt 1 to use session %q, got %q", validUUID, recordedSessions[0])
			}
			if strings.Contains(recordedPrompts[0], "<PREVIOUS_TURN_ACTIONS>") {
				t.Errorf("expected attempt 1 prompt to not contain PREVIOUS_TURN_ACTIONS")
			}
			for i := 1; i < tt.wantAttempts; i++ {
				if tt.wantRetainSession && recordedSessions[i] != validUUID {
					t.Errorf("expected attempt %d session %q, got %q", i+1, validUUID, recordedSessions[i])
				}
			}
		})
	}
}

func TestTurnExecution_CondenseTurnActions(t *testing.T) {
	t.Parallel()
	te := &turnExecution{}

	shortActions := "<PREVIOUS_TURN_ACTIONS>\n- Action: run_command | Command/Args: ls | Status: DONE | Result: ok\n</PREVIOUS_TURN_ACTIONS>"
	if got := te.condenseTurnActions(shortActions); got != shortActions {
		t.Errorf("condenseTurnActions short string failed: got %q, want %q", got, shortActions)
	}

	longActions := "<PREVIOUS_TURN_ACTIONS>\n" + strings.Repeat("- Action: run_command | Command/Args: test_long_command | Status: DONE | Result: long_output_data\n", 30) + "</PREVIOUS_TURN_ACTIONS>"

	mockLLM := func(ctx context.Context, model, prompt string) (string, error) {
		if !strings.Contains(prompt, "Condense the following tool actions") {
			t.Errorf("expected summarizer prompt, got %q", prompt)
		}
		return "<PREVIOUS_TURN_ACTIONS>\n- Condensed summary of 30 commands\n</PREVIOUS_TURN_ACTIONS>", nil
	}
	teWithLLM := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{LLMFunc: mockLLM}}}
	condensed := teWithLLM.condenseTurnActions(longActions)
	if !strings.Contains(condensed, "- Condensed summary of 30 commands") {
		t.Errorf("expected condensed output to contain summary, got %q", condensed)
	}

	mockLLMUntagged := func(ctx context.Context, model, prompt string) (string, error) {
		return "- Bare summary without tags", nil
	}
	teUntagged := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{LLMFunc: mockLLMUntagged}}}
	condensedUntagged := teUntagged.condenseTurnActions(longActions)
	if !strings.Contains(condensedUntagged, "<PREVIOUS_TURN_ACTIONS>") || !strings.Contains(condensedUntagged, "- Bare summary without tags") {
		t.Errorf("expected wrapped tags for untagged response, got %q", condensedUntagged)
	}

	mockLLMErr := func(ctx context.Context, model, prompt string) (string, error) {
		return "", errors.New("simulated rate limit error")
	}
	teErr := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{LLMFunc: mockLLMErr}}}
	if fallbackErr := teErr.condenseTurnActions(longActions); fallbackErr != longActions {
		t.Errorf("expected fallback to rawActions on LLM error, got %q", fallbackErr)
	}

	if fallbackNoLLM := te.condenseTurnActions(longActions); fallbackNoLLM != longActions {
		t.Errorf("expected fallback to rawActions when summarizer unavailable, got %q", fallbackNoLLM)
	}

	mockLLMEmpty := func(ctx context.Context, model, prompt string) (string, error) {
		return "   ", nil
	}
	teEmpty := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{LLMFunc: mockLLMEmpty}}}
	if fallbackEmpty := teEmpty.condenseTurnActions(longActions); fallbackEmpty != longActions {
		t.Errorf("expected fallback to rawActions on empty LLM response, got %q", fallbackEmpty)
	}

	mockLLMOversized := func(ctx context.Context, model, prompt string) (string, error) {
		return "<PREVIOUS_TURN_ACTIONS>\n" + strings.Repeat("ExtremelyLongSummaryDataPoint-", 150) + "\n</PREVIOUS_TURN_ACTIONS>", nil
	}
	teOversized := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{LLMFunc: mockLLMOversized}}}
	clamped := teOversized.condenseTurnActions(longActions)
	if len(clamped) > 2000 {
		t.Errorf("expected clamped output <= 2000 chars, got %d", len(clamped))
	}
}

func TestTurnExecution_PreparePrompt(t *testing.T) {
	t.Parallel()
	te := &turnExecution{}
	basePrompt := "Base user request"

	if got := te.preparePrompt(basePrompt); got != basePrompt {
		t.Errorf("preparePrompt with empty previousTurnActions modified prompt: got %q", got)
	}

	te.previousTurnActions = "<PREVIOUS_TURN_ACTIONS>\n- Action: test\n</PREVIOUS_TURN_ACTIONS>"
	prepared := te.preparePrompt(basePrompt)
	expected := basePrompt + "\n\n" + te.previousTurnActions
	if prepared != expected {
		t.Errorf("preparePrompt failed: got %q, want %q", prepared, expected)
	}

	rePrepared := te.preparePrompt(prepared)
	if rePrepared != prepared {
		t.Errorf("preparePrompt duplicated actions on re-preparation: got %q, want %q", rePrepared, prepared)
	}
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

func TestBuildTurnPrompt_AmbientResolver(t *testing.T) {
	t.Parallel()

	ambCfg := &config.AmbientContextConfig{Template: "Kitchen Temp: {{.temp}}"}

	tests := []struct {
		name               string
		ambCfg             *config.AmbientContextConfig
		resolverErr        error
		resolverText       string
		wantResolverCalled bool
		wantPromptContains []string
		wantPromptExcludes []string
	}{
		{
			name:               "ConfiguredChannel",
			ambCfg:             ambCfg,
			resolverText:       "<ambient_context>\nKitchen Temp: 71F\n</ambient_context>",
			wantResolverCalled: true,
			wantPromptContains: []string{"<ambient_context>", "Kitchen Temp: 71F", "turn off kitchen lights"},
		},
		{
			name:               "UnconfiguredChannel_Bypass",
			ambCfg:             nil,
			resolverText:       "<ambient_context>should not appear</ambient_context>",
			wantResolverCalled: false,
			wantPromptContains: []string{"turn off kitchen lights"},
			wantPromptExcludes: []string{"<ambient_context>"},
		},
		{
			name:               "ResolverErrorTolerance",
			ambCfg:             ambCfg,
			resolverErr:        errors.New("mcp tool timed out"),
			wantResolverCalled: true,
			wantPromptContains: []string{"turn off kitchen lights"},
			wantPromptExcludes: []string{"<ambient_context>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var resolverCalled atomic.Bool
			pool := &WorkerPool{
				ctx: context.Background(),
				cfg: WorkerPoolConfig{
					AmbientResolver: func(ctx context.Context, cfg *config.AmbientContextConfig) (string, error) {
						resolverCalled.Store(true)
						if tt.resolverErr != nil {
							return "", tt.resolverErr
						}
						return tt.resolverText, nil
					},
				},
			}

			te := &turnExecution{
				pool:             pool,
				effectiveName:    "kitchen",
				threadID:         "t-amb-1",
				currentSessionID: "sess-amb-1",
				policy: config.ChannelPolicy{
					AmbientContext: tt.ambCfg,
				},
				burst: []db.Message{{ID: "m1", ThreadID: "t-amb-1", Content: "turn off kitchen lights"}},
			}
			te.buildTurnPrompt()

			if called := resolverCalled.Load(); called != tt.wantResolverCalled {
				t.Errorf("resolverCalled = %t, want %t", called, tt.wantResolverCalled)
			}
			for _, sub := range tt.wantPromptContains {
				if !strings.Contains(te.turnPrompt, sub) {
					t.Errorf("expected turnPrompt to contain %q, got: %q", sub, te.turnPrompt)
				}
			}
			for _, sub := range tt.wantPromptExcludes {
				if strings.Contains(te.turnPrompt, sub) {
					t.Errorf("expected turnPrompt NOT to contain %q, got: %q", sub, te.turnPrompt)
				}
			}
		})
	}
}

type mockRotatorPool struct {
	shouldRotate bool
	rotReason    string
	rotateErr    error
	newSession   runner.AgentSession
	rotateCalled bool
	evictedKey   string
	evictCalls   int
}

func (m *mockRotatorPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (runner.AgentSession, error) {
	return nil, nil
}
func (m *mockRotatorPool) Initialize(ctx context.Context) error { return nil }
func (m *mockRotatorPool) Close() error                         { return nil }

func (m *mockRotatorPool) ShouldRotateSession(sess runner.AgentSession) (bool, string) {
	return m.shouldRotate, m.rotReason
}

func (m *mockRotatorPool) RotateSession(ctx context.Context, targetKey string) (runner.AgentSession, error) {
	m.rotateCalled = true
	if m.rotateErr != nil {
		return nil, m.rotateErr
	}
	return m.newSession, nil
}

func (m *mockRotatorPool) EvictSession(targetKey string) error {
	m.evictedKey = targetKey
	m.evictCalls++
	return nil
}

type mockRotatorSession struct {
	sessionID string
}

func (s *mockRotatorSession) Send(prompt string, turn *runner.TurnContext) error { return nil }
func (s *mockRotatorSession) SessionID() string                                  { return s.sessionID }

type mockRotateStore struct {
	*db.FakeStore
	rotateFn func(ctx context.Context, sessionKey, newSessionID string) error
}

func (m *mockRotateStore) RotateSessionID(ctx context.Context, sessionKey, newSessionID string) error {
	if m.rotateFn != nil {
		return m.rotateFn(ctx, sessionKey, newSessionID)
	}
	return m.FakeStore.RotateSessionID(ctx, sessionKey, newSessionID)
}

func runCheckTurnEndRotation(t *testing.T, store db.Store, mode string, initTurns int, shouldRotate bool, rotateErr error) (*turnExecution, *mockRotatorPool, string) {
	t.Helper()
	threadID := "thread-test"
	if store == nil {
		fakeStore := setupTestStore(t)
		_ = fakeStore.SaveSessionID(context.Background(), threadID, "sess-old")
		store = fakeStore
	}

	pool := &WorkerPool{
		ctx: context.Background(),
		cfg: WorkerPoolConfig{Store: store},
	}
	rotPool := &mockRotatorPool{
		shouldRotate: shouldRotate,
		rotReason:    "turn_limit",
		rotateErr:    rotateErr,
		newSession:   &mockRotatorSession{sessionID: "sess-new"},
	}
	te := &turnExecution{
		pool:             pool,
		threadID:         threadID,
		currentSessionID: "sess-old",
		turnCount:        initTurns,
		policy:           config.ChannelPolicy{Mode: mode},
	}
	execSession := &mockRotatorSession{sessionID: "sess-old"}

	te.checkTurnEndRotation(rotPool, execSession)
	return te, rotPool, threadID
}

func TestTurnExecution_CheckTurnEndRotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		channelMode     string
		shouldRotate    bool
		wantTurnCount   int
		wantSession     string
		wantPrevSession string
	}{
		{name: "NormalRotation_Thread", channelMode: "thread", shouldRotate: true, wantTurnCount: 0, wantSession: "sess-new", wantPrevSession: "sess-old"},
		{name: "NormalRotation_Channel", channelMode: "channel", shouldRotate: true, wantTurnCount: 0, wantSession: "sess-new"},
		{name: "NoRotation", channelMode: "thread", shouldRotate: false, wantTurnCount: 3, wantSession: "sess-old"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			initTurns := tt.wantTurnCount
			if tt.shouldRotate {
				initTurns = 10
			}
			te, rotPool, threadID := runCheckTurnEndRotation(t, nil, tt.channelMode, initTurns, tt.shouldRotate, nil)

			if rotPool.rotateCalled != tt.shouldRotate {
				t.Errorf("rotPool.rotateCalled = %t, want %t", rotPool.rotateCalled, tt.shouldRotate)
			}
			if te.turnCount != tt.wantTurnCount {
				t.Errorf("te.turnCount = %d, want %d", te.turnCount, tt.wantTurnCount)
			}
			if te.currentSessionID != tt.wantSession {
				t.Errorf("te.currentSessionID = %q, want %q", te.currentSessionID, tt.wantSession)
			}
			if tt.wantPrevSession != "" && te.previousSessionID != tt.wantPrevSession {
				t.Errorf("te.previousSessionID = %q, want %q", te.previousSessionID, tt.wantPrevSession)
			}

			dbSessID, _ := te.pool.cfg.Store.GetSessionID(context.Background(), threadID)
			if dbSessID != tt.wantSession {
				t.Errorf("DB session ID = %q, want %q", dbSessID, tt.wantSession)
			}
		})
	}
}

func TestTurnExecution_CheckTurnEndRotation_SpawnFailure(t *testing.T) {
	t.Parallel()
	te, rotPool, threadID := runCheckTurnEndRotation(t, nil, "thread", 12, true, errors.New("spawn failed: oom"))

	if !rotPool.rotateCalled {
		t.Errorf("rotPool.rotateCalled = false, want true")
	}
	if te.turnCount != 12 {
		t.Errorf("te.turnCount = %d, want 12", te.turnCount)
	}
	if te.currentSessionID != "" {
		t.Errorf("te.currentSessionID = %q, want empty", te.currentSessionID)
	}
	if rotPool.evictedKey != threadID {
		t.Errorf("rotPool.evictedKey = %q, want %q", rotPool.evictedKey, threadID)
	}
	dbSessID, _ := te.pool.cfg.Store.GetSessionID(context.Background(), threadID)
	if dbSessID != "" {
		t.Errorf("DB session ID = %q, want empty", dbSessID)
	}
}

func TestTurnExecution_CheckTurnEndRotation_DBLatchFailure(t *testing.T) {
	t.Parallel()
	fakeStore := setupTestStore(t)
	threadID := "thread-test"
	_ = fakeStore.SaveSessionID(context.Background(), threadID, "sess-old")
	store := &mockRotateStore{
		FakeStore: fakeStore,
		rotateFn: func(ctx context.Context, sessionKey, newSessionID string) error {
			if newSessionID != "" {
				return errors.New("db connection lost")
			}
			return fakeStore.RotateSessionID(ctx, sessionKey, newSessionID)
		},
	}

	te, rotPool, _ := runCheckTurnEndRotation(t, store, "thread", 10, true, nil)

	if !rotPool.rotateCalled {
		t.Errorf("rotPool.rotateCalled = false, want true")
	}
	if te.turnCount != 10 {
		t.Errorf("te.turnCount = %d, want 10", te.turnCount)
	}
	if te.currentSessionID != "" {
		t.Errorf("te.currentSessionID = %q, want empty", te.currentSessionID)
	}
	if rotPool.evictedKey != threadID {
		t.Errorf("rotPool.evictedKey = %q, want %q", rotPool.evictedKey, threadID)
	}
	dbSessID, _ := fakeStore.GetSessionID(context.Background(), threadID)
	if dbSessID != "" {
		t.Errorf("DB session ID = %q, want empty", dbSessID)
	}
}

func TestTurnExecution_CheckTurnEndRotation_Guards(t *testing.T) {
	t.Parallel()
	rotPool := &mockRotatorPool{shouldRotate: true}
	execSession := &mockRotatorSession{sessionID: "s1"}

	var teNil *turnExecution
	teNil.checkTurnEndRotation(rotPool, execSession)

	teNoPool := &turnExecution{threadID: "t1"}
	teNoPool.checkTurnEndRotation(rotPool, execSession)

	teWithPool := &turnExecution{pool: &WorkerPool{ctx: context.Background()}}
	teWithPool.checkTurnEndRotation(nil, execSession)
	teWithPool.checkTurnEndRotation(rotPool, nil)

	nonRotatorPool := &dummyNonRotatorPool{}
	teWithPool.checkTurnEndRotation(nonRotatorPool, execSession)
}

func TestWorker_RotatedSession_TreatsTurnOneAsColdStart(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	threadID := "thread-rotated-cold-test"
	oldSessionID := "11111111-1111-1111-1111-111111111111"
	newSessionID := "22222222-2222-2222-2222-222222222222"

	_ = store.SaveSessionID(context.Background(), threadID, oldSessionID)
	_ = store.RotateSessionID(context.Background(), threadID, newSessionID)
	_ = store.SaveThreadSummary(context.Background(), threadID, "<THREAD_SUMMARY>Discussed earlier deployment</THREAD_SUMMARY>", "msg-pre-rot")

	CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID,
		GuildID:  "g1",
		ParentID: "parent-chan",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})

	var mu sync.Mutex
	var capturedPrompts []string
	doneCh := make(chan struct{}, 2)

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
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
				{ID: "msg-hist-1", AuthorName: "alex", Content: "pre-rotation message", CreatedAt: time.Now().Add(-10 * time.Minute)},
			}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			if strings.Contains(prompt, "Synthesize the following") {
				return mockJSONResponse(sessionID, "<THREAD_SUMMARY>Discussed earlier deployment</THREAD_SUMMARY>"), "", 0, nil
			}
			mu.Lock()
			capturedPrompts = append(capturedPrompts, prompt)
			mu.Unlock()
			return mockJSONResponse(sessionID, "Turn response"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) (stop func()) { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	msg1 := db.Message{ID: "msg-post-rot-1", ThreadID: threadID, Content: "what was discussed earlier?"}
	_ = insertMessage(store, msg1)
	pool.Enqueue(msg1)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message 1")
	}

	msg2 := db.Message{ID: "msg-post-rot-2", ThreadID: threadID, Content: "follow-up question"}
	_ = insertMessage(store, msg2)
	pool.Enqueue(msg2)

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message 2")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(capturedPrompts) != 2 {
		t.Fatalf("Expected 2 captured prompts, got %d", len(capturedPrompts))
	}

	turn1Prompt := capturedPrompts[0]
	if !strings.Contains(turn1Prompt, "<THREAD_SUMMARY>Discussed earlier deployment</THREAD_SUMMARY>") {
		t.Errorf("Expected Turn 1 to contain <THREAD_SUMMARY>, got:\n%s", turn1Prompt)
	}
	if !strings.Contains(turn1Prompt, "<PREVIOUS_SESSION>") {
		t.Errorf("Expected Turn 1 to contain <PREVIOUS_SESSION>, got:\n%s", turn1Prompt)
	}
	if !strings.Contains(turn1Prompt, "<CHANNEL_HISTORY>") {
		t.Errorf("Expected Turn 1 to contain <CHANNEL_HISTORY>, got:\n%s", turn1Prompt)
	}

	turn2Prompt := capturedPrompts[1]
	if strings.Contains(turn2Prompt, "<THREAD_SUMMARY>") {
		t.Errorf("Expected Turn 2 to NOT contain <THREAD_SUMMARY>, got:\n%s", turn2Prompt)
	}
	if strings.Contains(turn2Prompt, "<PREVIOUS_SESSION>") {
		t.Errorf("Expected Turn 2 to NOT contain <PREVIOUS_SESSION>, got:\n%s", turn2Prompt)
	}
	if strings.Contains(turn2Prompt, "<CHANNEL_HISTORY>") {
		t.Errorf("Expected Turn 2 to NOT contain <CHANNEL_HISTORY>, got:\n%s", turn2Prompt)
	}
}

func TestWorker_BurstHelpers_SilentAndLowEffort(t *testing.T) {
	t.Parallel()

	burstTests := []struct {
		name       string
		burst      []db.Message
		wantModel  string
		wantSilent bool
		wantEffort string
	}{
		{name: "NilBurst", burst: nil, wantModel: "", wantSilent: false, wantEffort: "high"},
		{
			name: "NormalBurst",
			burst: []db.Message{
				{ID: "m1", Effort: "high", Metadata: db.MessageMetadata{Model: "gemini-3.1-pro"}},
				{ID: "m2", Metadata: db.MessageMetadata{Silent: false}},
			},
			wantModel:  "gemini-3.1-pro",
			wantSilent: false,
			wantEffort: "high",
		},
		{
			name: "SilentFlashBurst",
			burst: []db.Message{
				{ID: "m1", Metadata: db.MessageMetadata{Silent: true}},
				{ID: "m2", Metadata: db.MessageMetadata{Model: "  gemini-4-flash  "}},
			},
			wantModel:  "gemini-4-flash",
			wantSilent: true,
			wantEffort: "low",
		},
		{
			name: "LowEffortFieldBurst",
			burst: []db.Message{
				{ID: "m1", Effort: " LOW "},
			},
			wantModel:  "",
			wantSilent: false,
			wantEffort: "low",
		},
		{
			name: "MediumEffortFieldBurst",
			burst: []db.Message{
				{ID: "m1", Effort: " MEDIUM "},
			},
			wantModel:  "",
			wantSilent: false,
			wantEffort: "medium",
		},
		{
			name: "FlashMediumModelBurstNotLow",
			burst: []db.Message{
				{ID: "m1", Metadata: db.MessageMetadata{Model: "gemini-3.8-flash-medium"}},
			},
			wantModel:  "gemini-3.8-flash-medium",
			wantSilent: false,
			wantEffort: "medium",
		},
	}

	for _, tt := range burstTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractBurstModel(tt.burst); got != tt.wantModel {
				t.Errorf("extractBurstModel = %q, want %q", got, tt.wantModel)
			}
			if got := extractBurstSilent(tt.burst); got != tt.wantSilent {
				t.Errorf("extractBurstSilent = %t, want %t", got, tt.wantSilent)
			}
			if got := resolveBurstEffort(tt.burst); got != tt.wantEffort {
				t.Errorf("resolveBurstEffort = %q, want %q", got, tt.wantEffort)
			}
		})
	}
}

func TestWorker_SilentBurst_Execution(t *testing.T) {
	t.Parallel()
	store := db.NewFakeStore()
	ctx := context.Background()

	threadID := "1542423172400291873"
	sessionID := "a1111111-2222-3333-4444-555555555555"

	var mu sync.Mutex
	var deliveredMsgs []string
	var typingCalls int
	doneCh := make(chan string, 1)

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		MaxAttempts:    1,
		BackoffBase:    1 * time.Millisecond,
		TimeoutMinutes: 1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse(sessionID, "silent internal work completed"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, chID, text string) error {
			mu.Lock()
			deliveredMsgs = append(deliveredMsgs, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: func(s *discordgo.Session, chID string) func() {
			mu.Lock()
			typingCalls++
			mu.Unlock()
			return func() {}
		},
		OnMessageCompleted: func(msg db.Message, status string) {
			doneCh <- status
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:       "msg-silent-exec-1",
		ThreadID: threadID,
		Content:  "Execute silent task",
		Status:   db.StatusPending,
		Metadata: db.MessageMetadata{
			Model:  "gemini-4-flash",
			Silent: true,
		},
	}
	_ = store.InsertMessage(ctx, msg)
	pool.Enqueue(msg)

	select {
	case status := <-doneCh:
		if status != db.StatusCompleted {
			t.Fatalf("expected StatusCompleted, got %s", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for silent message completion")
	}

	mu.Lock()
	defer mu.Unlock()
	if typingCalls != 0 {
		t.Errorf("expected 0 typing calls for silent burst, got %d", typingCalls)
	}
	if len(deliveredMsgs) != 0 {
		t.Errorf("expected 0 delivered messages for silent burst, got %d: %v", len(deliveredMsgs), deliveredMsgs)
	}

	storedMsg, err := store.GetMessage(ctx, msg.ID)
	if err != nil || storedMsg == nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if storedMsg.Status != db.StatusCompleted {
		t.Errorf("expected stored message StatusCompleted, got %s", storedMsg.Status)
	}
}
