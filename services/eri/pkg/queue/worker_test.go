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
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestWorker_PoolDaemonAccessors(t *testing.T) {
	mockSpawner := newMockDaemonSpawner()
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer procPool.Close()

	pool := NewWorkerPool(WorkerPoolConfig{ProcessPool: procPool})
	defer pool.Stop()

	if pool.ProcessPool() == nil {
		t.Errorf("expected non-nil ProcessPool")
	}
	var nilPool *WorkerPool
	if nilPool.ProcessPool() != nil {
		t.Errorf("expected nil ProcessPool for nil WorkerPool")
	}
}

func TestWorker_PassesSessionIDToAgentPool(t *testing.T) {
	t.Parallel()
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	var mu sync.Mutex
	capturedConfigs := make([]runner.DaemonConfig, 0)

	mockSpawner := &mockDaemonSpawner{
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
			return inW, outR, errR, newMockProcessHandle(12346), nil
		},
	}
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	defer procPool.Close()

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
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

	expectedSessID := "a1111111-2222-3333-4444-555555555555"
	_ = store.SaveSessionID(context.Background(), "thread-with-sess", expectedSessID)
	msg1 := db.Message{ID: "msg-with-sess", ThreadID: "thread-with-sess", Content: "Resume turn", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg1)
	pool.Enqueue(msg1)

	waitCompleted := func(expectedID string) {
		select {
		case id := <-completedCh:
			if id != expectedID {
				t.Fatalf("unexpected message completed: %s", id)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for %s", expectedID)
		}
	}
	waitCompleted("msg-with-sess")

	msg2 := db.Message{ID: "msg-fresh", ThreadID: "thread-fresh", Content: "First turn", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg2)
	pool.Enqueue(msg2)
	waitCompleted("msg-fresh")

	mu.Lock()
	defer mu.Unlock()
	if len(capturedConfigs) != 2 {
		t.Fatalf("expected 2 daemon spawns, got %d", len(capturedConfigs))
	}
	if capturedConfigs[0].SessionID != expectedSessID {
		t.Errorf("expected DaemonConfig.SessionID %q, got %q", expectedSessID, capturedConfigs[0].SessionID)
	}
	if capturedConfigs[1].SessionID != "" {
		t.Errorf("expected DaemonConfig.SessionID empty, got %q", capturedConfigs[1].SessionID)
	}
}

func TestWorker_PersistentDaemonExecutionErrors(t *testing.T) {
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	tests := []struct {
		name, agyBin, threadID, sessID, script string
	}{
		{name: "InvalidBinary", agyBin: "/invalid/nonexistent/bin/path", threadID: "thread-bad-daemon", sessID: "c1111111-2222-3333-4444-555555555555"},
		{name: "ScriptExitError", threadID: "thread-err-daemon", sessID: "d1111111-2222-3333-4444-555555555555", script: "#!/bin/sh\nexit 1\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			binPath := tc.agyBin
			if tc.script != "" {
				binPath = filepath.Join(t.TempDir(), "mock_err.sh")
				_ = os.WriteFile(binPath, []byte(tc.script), 0o755)
			}
			cfg := newTestConfig(func(d *config.ConfigData) { d.AgyBin, d.DataDir = binPath, tempData })
			failedCh := make(chan struct{})
			pool := New(cfg, WorkerPoolConfig{
				SessionManager: session.New(tmpHome, tempData), Store: store,
				TimeoutMinutes: 1, MaxAttempts: 1,
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
			msg := db.Message{ID: "msg-" + tc.threadID, ThreadID: tc.threadID, Content: "fail", Status: db.StatusPending, CreatedAt: time.Now()}
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
		name, threadID, daemonUUID, presetSessionInDB string
		turnResponses                                 []string
		transcriptContent, foreignDiskUUID            string
		expectedFinalStatus, wantDeliveredSubstr      string
		wantSavedSession, expectSessionReject         string
		checkQuotaSchedule, checkQuotaDbError         bool
		checkMetrics                                  bool
	}{
		{
			name: "ColdStart_Success", threadID: "thread-cold-persist-success", daemonUUID: "c1111111-2222-3333-4444-555555555555",
			turnResponses:       []string{`{"event":"result","result":{"status":"SUCCESS","response":"Cold start completed successfully!","usage":{"total_tokens":25}}}` + "\n"},
			expectedFinalStatus: db.StatusCompleted, wantDeliveredSubstr: "Cold start completed successfully!", wantSavedSession: "c1111111-2222-3333-4444-555555555555", checkMetrics: true,
		},
		{
			name: "ColdStart_RejectsForeignSessionOnDisk", threadID: "thread-cold-persist-no-foreign", daemonUUID: "d1111111-2222-3333-4444-555555555555",
			turnResponses:   []string{`{"event":"result","result":{"status":"SUCCESS","response":"Turn completed","usage":{"total_tokens":15}}}` + "\n"},
			foreignDiskUUID: "f1111111-2222-3333-4444-555555555555", expectedFinalStatus: db.StatusCompleted, wantDeliveredSubstr: "Turn completed",
			wantSavedSession: "d1111111-2222-3333-4444-555555555555", expectSessionReject: "f1111111-2222-3333-4444-555555555555",
		},
		{
			name: "EmptyResponse_TranscriptFallback", threadID: "thread-empty-resp-fallback", daemonUUID: "e1111111-2222-3333-4444-555555555555",
			turnResponses:       []string{`{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":10}}}` + "\n"},
			transcriptContent:   `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered from transcript!"}` + "\n",
			expectedFinalStatus: db.StatusCompleted, wantDeliveredSubstr: "Recovered from transcript!",
		},
		{
			name: "DaemonEmptyResponse_TranscriptFallback", threadID: "thread-daemon-empty-fallback", daemonUUID: "e3333333-4444-5555-6666-777777777777",
			turnResponses:       []string{`{"event":"step_update","step_update":{"step_index":1,"step_type":"tool","tool_name":"run_command"}}` + "\n" + `{"event":"result","result":{"status":"SUCCESS","response":""}}` + "\n"},
			transcriptContent:   `{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered after daemon empty response!"}` + "\n",
			expectedFinalStatus: db.StatusCompleted, wantDeliveredSubstr: "Recovered after daemon empty response!",
		},
		{
			name: "QuotaExhaustion_FromTranscript", threadID: "thread-quota-exhaustion-test", daemonUUID: "f2222222-3333-4444-5555-666666666666", presetSessionInDB: "f2222222-3333-4444-5555-666666666666",
			turnResponses:       []string{`{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":0}}}` + "\n"},
			transcriptContent:   fmt.Sprintf(`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 4h51m59s.","created_at":%q}`+"\n", time.Now().UTC().Format(time.RFC3339)),
			expectedFinalStatus: db.StatusFailed, wantDeliveredSubstr: "resets in", checkQuotaSchedule: true, checkQuotaDbError: true,
		},
		{
			name: "QuotaPause_RotatesBloatedSession", threadID: "thread-quota-bloat-rotation", daemonUUID: "b3333333-4444-5555-6666-777777777777", presetSessionInDB: "b3333333-4444-5555-6666-777777777777",
			turnResponses: []string{`{"event":"result","result":{"status":"SUCCESS","response":"","usage":{"total_tokens":0}}}` + "\n"},
			transcriptContent: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2h30m.","created_at":%q}
`, session.DefaultMaxSessionSteps, time.Now().UTC().Format(time.RFC3339)),
			expectedFinalStatus: db.StatusFailed, wantSavedSession: "b3333333-4444-5555-6666-777777777777",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, newScriptedDaemonSpawner(tc.daemonUUID, tc.turnResponses...))
			defer procPool.Close()

			cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
			if tc.presetSessionInDB != "" {
				_ = store.SaveSessionID(context.Background(), tc.threadID, tc.presetSessionInDB)
			}
			if tc.transcriptContent != "" {
				sessDir := filepath.Join(tempData, "brain", tc.daemonUUID, ".system_generated", "logs")
				_ = os.MkdirAll(sessDir, 0o755)
				_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(tc.transcriptContent), 0o644)
			}
			if tc.foreignDiskUUID != "" {
				brainDir := filepath.Join(tempData, "brain", tc.foreignDiskUUID)
				_ = os.MkdirAll(brainDir, 0o755)
				ft := time.Now().Add(5 * time.Second)
				_ = os.Chtimes(brainDir, ft, ft)
			}

			var deliveredText string
			doneCh := make(chan struct{})
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool: procPool, SessionManager: session.New(tmpHome, tempData),
				Store: store, TimeoutMinutes: 1,
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
			msg := db.Message{ID: msgID, ThreadID: tc.threadID, Content: "run turn", Status: db.StatusPending, CreatedAt: time.Now()}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for message completion")
			}

			if tc.checkMetrics {
				if pool.ProcessPool() == nil {
					t.Errorf("expected daemon for %s", tc.threadID)
				} else if _, ok := pool.ProcessPool().Get(tc.threadID); !ok {
					t.Errorf("expected daemon for %s", tc.threadID)
				}
				rec := httptest.NewRecorder()
				metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				b := rec.Body.String()
				if !strings.Contains(b, `source="discord"`) || !strings.Contains(b, `aerial_brain_runner_executions_total{`) || !strings.Contains(b, `aerial_brain_runner_duration_seconds_bucket{`) {
					t.Errorf("metrics missing expected discord labels")
				}
			}
			if tc.wantDeliveredSubstr != "" && !strings.Contains(deliveredText, tc.wantDeliveredSubstr) {
				t.Errorf("delivered text = %q, want substr %q", deliveredText, tc.wantDeliveredSubstr)
			}
			if tc.wantSavedSession != "" {
				s, _ := store.GetSessionID(context.Background(), tc.threadID)
				if s != tc.wantSavedSession {
					t.Errorf("saved session = %q, want %q", s, tc.wantSavedSession)
				}
			}
			if tc.expectSessionReject != "" {
				s, _ := store.GetSessionID(context.Background(), tc.threadID)
				if s == tc.expectSessionReject {
					t.Fatalf("security violation: latched foreign session %q", tc.expectSessionReject)
				}
			}
			if tc.checkQuotaDbError {
				dbMsg, _ := store.GetMessage(context.Background(), msgID)
				if dbMsg == nil || !strings.Contains(dbMsg.ErrorMessage, "[QUOTA_PAUSED") {
					t.Errorf("expected [QUOTA_PAUSED]")
				}
			}
			if tc.checkQuotaSchedule {
				schedules, _ := store.GetAllOneShotSchedules(context.Background(), tc.threadID)
				found := false
				for _, s := range schedules {
					if s.ThreadID == tc.threadID && strings.Contains(s.Prompt, "[QUOTA_RETRY]") {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected one-shot retry with [QUOTA_RETRY]")
				}
			}
		})
	}
}

func TestWorkerPool_TransientRetry_RotatesBloatedSession(t *testing.T) {
	tmpHome, tempData, store := t.TempDir(), t.TempDir(), setupTestStore(t)
	validUUID := "c4444444-5555-6666-7777-888888888888"
	sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	transcriptFile := filepath.Join(sessDir, "transcript.jsonl")
	_ = os.WriteFile(transcriptFile, []byte(fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":100,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"working...","created_at":%q}
`, time.Now().UTC().Format(time.RFC3339))), 0o644)

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
	threadID := "thread-transient-bloat-rotation"
	_ = store.SaveSessionID(context.Background(), threadID, validUUID)

	var sessionsSeen []string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := New(cfg, WorkerPoolConfig{
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1, MaxAttempts: 2, BackoffBase: 10 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			sessionsSeen = append(sessionsSeen, sessionID)
			att := len(sessionsSeen)
			mu.Unlock()
			if att == 1 {
				bloatedContent := fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"tool runaway","created_at":%q}`+"\n", session.DefaultMaxSessionSteps+5, time.Now().UTC().Format(time.RFC3339))
				if f, _ := os.OpenFile(transcriptFile, os.O_APPEND|os.O_WRONLY, 0o644); f != nil {
					_, _ = f.WriteString(bloatedContent)
					_ = f.Close()
				}
				return "", fmt.Sprintf("Starting conversation update stream for %s\nError 503: high demand service unavailable", validUUID), 1, fmt.Errorf("exit status 1")
			}
			return mockJSONResponse(uuid.New().String(), "Recovered successfully"), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, content string) error { return nil },
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
	if len(sessionsSeen) != 2 || sessionsSeen[0] != validUUID || sessionsSeen[1] != validUUID {
		t.Errorf("expected attempts to preserve session ID %q across retry, got: %v", validUUID, sessionsSeen)
	}
	if prevSess, _ := store.GetPreviousSessionID(context.Background(), threadID); prevSess != "" {
		t.Errorf("expected previous session ID empty, got %q", prevSess)
	}
}

func TestTurnExecution_GetRecentThreadMessages(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	mockDiscordMsgs := []*discordgo.Message{
		nil,
		{ID: "1552732003076669002", ChannelID: "1552732003076669460", Content: "Can we add SVG?", Timestamp: now.Add(-5 * time.Minute), Author: &discordgo.User{ID: "100000000000000001", Username: "test-user"}, Mentions: []*discordgo.User{{ID: "bot-123", Username: "Aerial"}}, ReferencedMessage: &discordgo.Message{Author: &discordgo.User{Username: "Aerial"}}},
		{ID: "1552732003076669001", ChannelID: "1552732003076669460", Content: "Weather widget", Timestamp: now.Add(-10 * time.Minute), Author: &discordgo.User{ID: "100000000000000001", Username: "test-user"}},
		{ID: "1552732003076669000", ChannelID: "1552732003076669460", Content: "Webhook notification", WebhookID: "hook-123"},
		{ID: "1552732003076669099", ChannelID: "1552732003076669460", Content: "Current burst msg", Timestamp: now},
	}
	dgSuccess, _ := discordgo.New("Bot test-token")
	dgSuccess.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(mockDiscordMsgs)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	dgFail, _ := discordgo.New("Bot test")
	dgFail.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) { return nil, errors.New("simulated error") })

	fakeStore := dbtest.NewFakeStore()
	ctx := context.Background()
	_ = fakeStore.InsertMessage(ctx, db.Message{ID: "msg-1", ThreadID: "1552732003076669460", AuthorName: "test-user", Content: "Normal chat", CreatedAt: now.Add(-2 * time.Minute)})
	_ = fakeStore.InsertMessage(ctx, db.Message{ID: "msg-sched", ThreadID: "1552732003076669460", AuthorName: "Scheduler", Content: "Check PR", ScheduleRunID: "run-123", CreatedAt: now.Add(-1 * time.Minute)})

	tests := []struct {
		name, firstID, firstAuthor string
		dg                         *discordgo.Session
		store                      db.Store
		burst                      []db.Message
		limit, wantCount           int
		wantErr                    bool
	}{
		{name: "DiscordAPI_Success", dg: dgSuccess, burst: []db.Message{{ID: "1552732003076669099"}}, limit: 10, wantCount: 3, firstID: "1552732003076669000", firstAuthor: "Webhook"},
		{name: "FallbackDB_Filtered", dg: dgFail, store: fakeStore, limit: 0, wantCount: 1, firstID: "msg-1", firstAuthor: "test-user"},
		{name: "FallbackDB_ClampedLimit", dg: dgFail, store: fakeStore, limit: 200, wantCount: 1, firstID: "msg-1", firstAuthor: "test-user"},
		{name: "StoreError", store: &mockErrRecentStore{}, limit: 10, wantErr: true},
		{name: "NilStore", limit: 10, wantCount: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := &WorkerPool{cfg: WorkerPoolConfig{DiscordSession: tc.dg, Store: tc.store}}
			te := &turnExecution{pool: pool, burst: tc.burst}
			msgs, err := te.getRecentThreadMessages("1552732003076669460", tc.limit)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(msgs) != tc.wantCount {
				t.Fatalf("expected %d messages, got %d", tc.wantCount, len(msgs))
			}
			if tc.wantCount > 0 && (msgs[0].ID != tc.firstID || msgs[0].AuthorName != tc.firstAuthor) {
				t.Errorf("mismatch on first message: got %+v, want ID=%s Author=%s", msgs[0], tc.firstID, tc.firstAuthor)
			}
		})
	}
}

type mockErrRecentStore struct{ db.Store }

func (m *mockErrRecentStore) GetRecentThreadMessages(ctx context.Context, threadID string, limit int) ([]db.Message, error) {
	return nil, errors.New("simulated db failure")
}

func TestWorker_RotateSessionID_EvictsDaemons(t *testing.T) {
	t.Parallel()
	fakeStore := setupTestStore(t)
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, newMockDaemonSpawner())
	defer procPool.Close()

	ctx := context.Background()
	_, _ = procPool.GetOrCreate(ctx, "thread-evict-1", "")
	te := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{Store: fakeStore}, processPool: procPool}, threadID: "thread-evict-1"}

	te.rotateSessionID("thread-evict-1", "new-sess-1")
	if _, ok := procPool.Get("thread-evict-1"); !ok {
		t.Errorf("expected daemon to remain when newSessionID is non-empty")
	}

	te.rotateSessionID("thread-evict-1", "")
	if storedSess, _ := fakeStore.GetSessionID(ctx, "thread-evict-1"); storedSess != "" {
		t.Errorf("expected empty session in store after rotation, got %q", storedSess)
	}

	teNil := &turnExecution{threadID: "thread-evict-2"}
	teNil.rotateSessionID("thread-evict-2", "")
	var teNull *turnExecution
	teNull.rotateSessionID("thread-evict-3", "")
}

func TestWorker_SessionDBRotation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, channelMode, runnerMode     string
		preWriteDBBytes, preWriteWALBytes int64
		maxAttempts, wantAttempts         int
	}{
		{name: "PreTurn_Channel_DBLimit", channelMode: "channel", preWriteDBBytes: session.DefaultMaxSessionDBBytes + 1024, preWriteWALBytes: 600 * 1024, maxAttempts: 1, wantAttempts: 1},
		{name: "PreTurn_Channel_WALIgnored", channelMode: "channel", preWriteDBBytes: 1000 * 1024, preWriteWALBytes: 600 * 1024, maxAttempts: 1, wantAttempts: 1},
		{name: "PreTurn_Thread_DBLimit", channelMode: "thread", preWriteDBBytes: session.DefaultMaxSessionDBBytes + 1024, maxAttempts: 1, wantAttempts: 1},
		{name: "QuotaPause_AboveThreshold", channelMode: "thread", preWriteDBBytes: session.DefaultMaxQuotaPauseDBBytes + 1024, runnerMode: "quota_429", maxAttempts: 1, wantAttempts: 1},
		{name: "QuotaPause_BelowThreshold", channelMode: "thread", preWriteDBBytes: 200 * 1024, runnerMode: "quota_429", maxAttempts: 1, wantAttempts: 1},
		{name: "PostExecutionPreserved", channelMode: "thread", runnerMode: "post_heavy", maxAttempts: 1, wantAttempts: 1},
		{name: "WatchdogRetryPreserved", channelMode: "thread", runnerMode: "retry_watchdog", maxAttempts: 2, wantAttempts: 2},
		{name: "TransientRetryPreserved", channelMode: "thread", runnerMode: "retry_transient", maxAttempts: 2, wantAttempts: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			sessMgr := session.New(tmpDir, tmpDir)
			oldSess, convDir := "sess-"+tc.name, filepath.Join(tmpDir, "conversations")
			_ = os.MkdirAll(convDir, 0o755)
			if tc.preWriteDBBytes > 0 {
				_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, tc.preWriteDBBytes), 0o644)
			}
			if tc.preWriteWALBytes > 0 {
				_ = os.WriteFile(filepath.Join(convDir, oldSess+".db-wal"), make([]byte, tc.preWriteWALBytes), 0o644)
			}

			threadID := "thread-" + tc.name
			_ = saveSessionID(store, threadID, oldSess)
			_, _ = incrementSessionTurnCount(store, threadID)

			doneCh := make(chan struct{}, 1)
			var mu sync.Mutex
			var attempts []string
			appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: tc.channelMode}}})
			pool := New(appCfg, WorkerPoolConfig{
				SessionManager: sessMgr, Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond, MaxAttempts: tc.maxAttempts,
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
				TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					select {
					case doneCh <- struct{}{}:
					default:
					}
				},
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					mu.Lock()
					attempts = append(attempts, sessionID)
					cur := len(attempts)
					mu.Unlock()
					switch tc.runnerMode {
					case "quota_429":
						return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Resets in 30s.", 1, errors.New("exit code 1")
					case "post_heavy":
						_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, session.DefaultMaxSessionDBBytes+500), 0o644)
						return mockJSONResponse(oldSess, "heavy output"), "", 0, nil
					case "retry_watchdog":
						if cur == 1 {
							_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, session.DefaultMaxSessionDBBytes+1024), 0o644)
							return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
						}
						return mockJSONResponse(sessionID, "Recovered"), "", 0, nil
					case "retry_transient":
						if cur == 1 {
							_ = os.WriteFile(filepath.Join(convDir, oldSess+".db"), make([]byte, session.DefaultMaxSessionDBBytes+1024), 0o644)
							return `{"status": "ERROR", "error": "temporary network timeout"}`, "", 0, nil
						}
						return mockJSONResponse(sessionID, "Recovered"), "", 0, nil
					default:
						return mockJSONResponse(sessionID, "OK"), "", 0, nil
					}
				},
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: "msg-" + tc.name, ThreadID: threadID, Content: "<@aerial> test message"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for message completion")
			}
			mu.Lock()
			gotAttempts := len(attempts)
			mu.Unlock()
			if gotAttempts != tc.wantAttempts {
				t.Fatalf("attempts = %d, want %d", gotAttempts, tc.wantAttempts)
			}
			if curSess, _ := getSessionID(store, threadID); curSess != oldSess {
				t.Errorf("session %q != %q", curSess, oldSess)
			}
		})
	}
}

func TestWorkerPool_CapacityBlipMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, runnerErr, wantFinalStatus, wantErrContains, wantDeliveryContains string
		maxAttempts, wantAttempts                                               int
		presetGlobalLockSec                                                     int64
		deliveryErr                                                             error
		checkGlobalLockZero, checkNoSchedules                                   bool
	}{
		{name: "LocalRetry_Success", maxAttempts: 2, runnerErr: "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", wantAttempts: 2, wantFinalStatus: db.StatusCompleted, wantDeliveryContains: "Recovered on attempt 2!", checkGlobalLockZero: true},
		{name: "Exhaustion_LocalFailure", maxAttempts: 1, runnerErr: "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", wantAttempts: 1, wantFinalStatus: db.StatusFailed, wantErrContains: "[CAPACITY_EXHAUSTED", wantDeliveryContains: "capacity", checkGlobalLockZero: true, checkNoSchedules: true},
		{name: "WithoutCountdown_Fallback70s", maxAttempts: 1, runnerErr: "API error: RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model.", wantAttempts: 1, wantFinalStatus: db.StatusFailed, wantErrContains: "[CAPACITY_EXHAUSTED", wantDeliveryContains: "capacity", checkGlobalLockZero: true, checkNoSchedules: true},
		{name: "GlobalQuotaLockout_PreCheckFailFast", maxAttempts: 2, presetGlobalLockSec: 300, wantAttempts: 0, wantFinalStatus: db.StatusFailed, wantErrContains: "global quota pause active", wantDeliveryContains: "quota", deliveryErr: errors.New("discord delivery error"), checkNoSchedules: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			var mu sync.Mutex
			var attemptCount int
			var deliveredText string
			doneCh := make(chan struct{}, 1)
			appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "thread"}}})
			pool := New(appCfg, WorkerPoolConfig{
				SessionManager: session.New(tmpDir, tmpDir), Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
				RetryDelayOverride: 10 * time.Millisecond, MaxAttempts: tt.maxAttempts,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					mu.Lock()
					attemptCount++
					cur := attemptCount
					mu.Unlock()
					if cur == 1 {
						return "", tt.runnerErr, 1, errors.New("exit code 1")
					}
					return mockJSONResponse(uuid.New().String(), "Recovered on attempt 2!"), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredText = text
					mu.Unlock()
					return tt.deliveryErr
				},
				TypingFunc: func(s *discordgo.Session, channelID string) func() { return func() {} },
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					select {
					case doneCh <- struct{}{}:
					default:
					}
				},
			})
			pool.Start()
			defer pool.Stop()

			if tt.presetGlobalLockSec > 0 {
				pool.quotaLockedUntil.Store(time.Now().Add(time.Duration(tt.presetGlobalLockSec) * time.Second).Unix())
			}

			threadID, msgID := "thread-"+strings.ToLower(tt.name), "msg-"+strings.ToLower(tt.name)
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
			attempts, delivered := attemptCount, deliveredText
			mu.Unlock()
			if attempts != tt.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
			dbMsg, _ := store.GetMessage(context.Background(), msgID)
			if dbMsg.Status != tt.wantFinalStatus {
				t.Errorf("status = %s, want %s", dbMsg.Status, tt.wantFinalStatus)
			}
			if tt.wantErrContains != "" && !strings.Contains(dbMsg.ErrorMessage, tt.wantErrContains) {
				t.Errorf("expected error %q in %q", tt.wantErrContains, dbMsg.ErrorMessage)
			}
			if tt.wantDeliveryContains != "" && !strings.Contains(delivered, tt.wantDeliveryContains) {
				t.Errorf("expected delivery %q in %q", tt.wantDeliveryContains, delivered)
			}
			if tt.checkGlobalLockZero && pool.quotaLockedUntil.Load() > 0 {
				t.Errorf("expected quotaLockedUntil == 0")
			}
			if tt.checkNoSchedules {
				s, _ := store.GetAllOneShotSchedules(context.Background(), threadID)
				if len(s) > 0 {
					t.Errorf("expected 0 schedules, got %d", len(s))
				}
			}
		})
	}
}

func TestWorkerPool_CapacityBlip_DoesNotBlockConcurrentThreads(t *testing.T) {
	t.Parallel()
	store, tmpDir := setupTestStore(t), t.TempDir()
	var mu sync.Mutex
	var threadAAttempts int
	var threadACompleted, threadBCompleted time.Time
	doneA, doneB := make(chan struct{}, 1), make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "thread"}}})
	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: session.New(tmpDir, tmpDir), Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
		RetryDelayOverride: 10 * time.Millisecond, MaxAttempts: 2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
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
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
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

	msgA, msgB := db.Message{ID: "msg-thread-A", ThreadID: "thread-A", Content: "Thread A prompt"}, db.Message{ID: "msg-thread-B", ThreadID: "thread-B", Content: "Thread B prompt"}
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
	aDone, bDone := threadACompleted.IsZero(), threadBCompleted.IsZero()
	mu.Unlock()
	if aDone || bDone {
		t.Errorf("Expected both threads to complete")
	}
	dbMsgB, _ := store.GetMessage(context.Background(), "msg-thread-B")
	if dbMsgB == nil || dbMsgB.Status != db.StatusCompleted {
		t.Errorf("Expected Thread B StatusCompleted")
	}
}

func TestWorkerPool_CapacityBlip_ContextCancellation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, msgID, threadID string
		checkTyping           bool
	}{
		{name: "ImmediateBackoff", msgID: "msg-cap-cancel", threadID: "thread-cap-cancel"},
		{name: "ProgressiveBackoff", msgID: "msg-cap-prog-cancel", threadID: "thread-cap-prog-cancel", checkTyping: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			startedCh := make(chan struct{}, 1)
			var typingMu sync.Mutex
			var typingActive, typingStopped bool
			appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "thread"}}})
			pool := New(appCfg, WorkerPoolConfig{
				SessionManager: session.New(tmpDir, tmpDir), Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond, MaxAttempts: 2,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					select {
					case startedCh <- struct{}{}:
					default:
					}
					return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
				TypingFunc: func(s *discordgo.Session, channelID string) func() {
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

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, Content: "cancel me"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			select {
			case <-startedCh:
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for attempt 1")
			}
			pool.cancel()
			waitPoolWorkersDone(t, pool, 3*time.Second)

			if tc.checkTyping {
				typingMu.Lock()
				active, stopped := typingActive, typingStopped
				typingMu.Unlock()
				if !active || !stopped {
					t.Errorf("expected typing active and then stopped on cancel")
				}
			}
			dbMsg, _ := store.GetMessage(context.Background(), tc.msgID)
			if dbMsg == nil || dbMsg.Status != db.StatusPending || !strings.Contains(dbMsg.ErrorMessage, "interrupted by graceful deployment") {
				t.Errorf("expected StatusPending with interrupted message")
			}
			if pool.quotaLockedUntil.Load() > 0 {
				t.Errorf("expected quotaLockedUntil == 0")
			}
		})
	}
}

func TestWorkerPool_ProgressiveCapacityBackoff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		attempt              int
		resetDur             time.Duration
		minFloor, maxCeiling time.Duration
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
				if d < tt.minFloor || d > tt.maxCeiling {
					t.Fatalf("backoff expected between %v and %v, got %v", tt.minFloor, tt.maxCeiling, d)
				}
			}
		})
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
	if err != nil || res.Response != "some text" {
		t.Fatalf("unexpected error/response: %v, %q", err, res.Response)
	}

	sinkNoDeltas := newDiscordTurnSink(nil)
	expectedErr := errors.New("hard failure")
	sinkNoDeltas.OnError(expectedErr)
	if _, err := sinkNoDeltas.Wait(context.Background()); err == nil || !errors.Is(err, expectedErr) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDiscordTurnSink_StatusUpdaterLifecycle(t *testing.T) {
	t.Parallel()
	updater := NewStatusUpdater(nil, "thread-123", true)
	updater.sendFunc = func(c, text string) (string, error) { return "msg-1", nil }
	updater.editFunc = func(c, m, text string) error { return nil }
	updater.deleteFunc = func(c, m string) error { return nil }
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
		name                                         string
		effort                                       string
		useFallbackNilLowPool, useFallbackNilMedPool bool
		wantPrimaryCount, wantLowCount, wantMedCount int
	}{
		{name: "LowEffortRoutesToLowPool", effort: "low", wantLowCount: 1},
		{name: "MediumEffortRoutesToMediumPool", effort: "medium", wantMedCount: 1},
		{name: "NormalRoutesToPrimaryPool", effort: "", wantPrimaryCount: 1},
		{name: "FallbackNilLowPoolRoutesToPrimary", effort: "low", useFallbackNilLowPool: true, wantPrimaryCount: 1},
		{name: "FallbackNilMediumPoolRoutesToPrimary", effort: "medium", useFallbackNilMedPool: true, wantPrimaryCount: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			var primarySpawnCount, lowSpawnCount, medSpawnCount atomic.Int32
			newCountedPool := func(sessID, resp, model string, counter *atomic.Int32) (*runner.UnifiedProcessPool, func()) {
				sp := newScriptedDaemonSpawner(sessID, fmt.Sprintf(`{"event":"result","result":{"status":"SUCCESS","response":%q,"usage":{"total_tokens":5}}}`+"\n", resp))
				oldSpawn := sp.SpawnFn
				sp.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
					counter.Add(1)
					return oldSpawn(ctx, cfg)
				}
				p := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: model}, sp)
				return p, func() { _ = p.Close() }
			}

			primaryPool, closeP := newCountedPool("c1111111-2222-3333-4444-555555555555", "primary", "gemini-3.7-pro", &primarySpawnCount)
			defer closeP()
			var lowPool, medPool runner.AgentPool
			if !tt.useFallbackNilLowPool {
				lp, closeL := newCountedPool("d1111111-2222-3333-4444-555555555555", "low", "gemini-3.8-flash-low", &lowSpawnCount)
				defer closeL()
				lowPool = lp
			}
			if !tt.useFallbackNilMedPool {
				mp, closeM := newCountedPool("f1111111-2222-3333-4444-555555555555", "med", "gemini-3.8-flash-medium", &medSpawnCount)
				defer closeM()
				medPool = mp
			}

			cfg := newTestConfig(func(d *config.ConfigData) {
				d.DataDir, d.LowEffortModel, d.MediumEffortModel = tempData, "gemini-3.8-flash-low", "gemini-3.8-flash-medium"
			})
			doneCh := make(chan struct{}, 1)
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool: primaryPool, LowEffortProcessPool: lowPool, MediumEffortProcessPool: medPool,
				SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
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
			msg := db.Message{ID: "msg-" + strings.ToLower(tt.name), ThreadID: threadID, Content: "run task", Effort: tt.effort, Status: db.StatusPending, CreatedAt: time.Now()}
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
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	lowStartedCh, lowBlockCh, normDoneCh, lowDoneCh := make(chan struct{}, 1), make(chan struct{}), make(chan struct{}), make(chan struct{})

	primarySpawner := newScriptedDaemonSpawner("c1111111-2222-3333-4444-555555555555",
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"primary reply\",\"usage\":{\"total_tokens\":10}}}\n")
	lowSpawner := &mockDaemonSpawner{
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
			return inW, outR, errR, newMockProcessHandle(12346), nil
		},
	}

	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()
	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowPool.Close()

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir, d.LowEffortModel = tempData, "gemini-3.8-flash-low" })
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
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
	lowMsg := db.Message{ID: "msg-low-concurrent", ThreadID: threadID, Content: "long low-effort task", Effort: "low", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), lowMsg)
	pool.Enqueue(lowMsg)

	select {
	case <-lowStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for low-effort turn to start")
	}

	normMsg := db.Message{ID: "msg-norm-concurrent", ThreadID: threadID, Content: "user asks what is happening", Effort: "medium", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), normMsg)
	pool.Enqueue(normMsg)

	select {
	case <-normDoneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("primary message was blocked by low-effort task")
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
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	primarySessUUID, prevSessUUID := "11111111-1111-1111-1111-111111111111", "99999999-9999-9999-9999-999999999999"

	var spawnSeq int
	var mu sync.Mutex
	var capturedPrompts []string

	lowSpawner := &mockDaemonSpawner{
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
			return inW, outR, errR, newMockProcessHandle(12345 + seq), nil
		},
	}
	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, newMockSpawner())
	defer primaryPool.Close()

	lowUnderlying := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowSpawner)
	defer lowUnderlying.Close()

	lowPool := runner.NewInterchangeablePool(lowUnderlying, runner.InterchangeablePoolConfig{WorkerCount: 1, KeyPrefix: "discord:worker"})
	defer lowPool.Close()

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir, d.LowEffortModel = tempData, "gemini-3.8-flash-low" })
	doneCh := make(chan struct{}, 2)
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
		HistoryFetcher: func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			return []HistoryMessage{{ID: "hist-1", AuthorName: "test-user", Content: "prior chat history should not be injected", CreatedAt: time.Now().Add(-5 * time.Minute)}}, nil
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

	msg1 := db.Message{ID: "msg-ephem-1", ThreadID: threadID, Content: "execute vanadis chunk 1", Effort: "low", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(ctx, msg1)
	pool.Enqueue(msg1)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ephemeral turn 1")
	}

	msg2 := db.Message{ID: "msg-ephem-2", ThreadID: threadID, Content: "execute vanadis chunk 2", Effort: "low", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(ctx, msg2)
	pool.Enqueue(msg2)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ephemeral turn 2")
	}

	mu.Lock()
	prompts := append([]string(nil), capturedPrompts...)
	spawns := spawnSeq
	mu.Unlock()

	if len(prompts) != 2 {
		t.Fatalf("expected 2 captured prompts, got %d", len(prompts))
	}
	for i, p := range prompts {
		if strings.Contains(p, "<THREAD_SUMMARY>") || strings.Contains(p, "Cached summary that must not be injected") {
			t.Errorf("turn %d prompt unexpectedly contained <THREAD_SUMMARY>", i+1)
		}
		if strings.Contains(p, "<CHANNEL_HISTORY>") || strings.Contains(p, "prior chat history should not be injected") {
			t.Errorf("turn %d prompt unexpectedly contained <CHANNEL_HISTORY>", i+1)
		}
		if strings.Contains(p, "<PREVIOUS_SESSION>") || strings.Contains(p, prevSessUUID) {
			t.Errorf("turn %d prompt unexpectedly contained <PREVIOUS_SESSION>", i+1)
		}
	}
	if spawns < 2 {
		t.Errorf("expected worker daemon rotation (spawns >= 2), got %d", spawns)
	}
	currentSess, err := store.GetSessionID(ctx, threadID)
	if err != nil || currentSess != primarySessUUID {
		t.Errorf("expected primary session %s untouched, got %s", primarySessUUID, currentSess)
	}
	turnCount, err := store.GetSessionTurnCount(ctx, threadID)
	if err != nil || turnCount != 0 {
		t.Errorf("expected primary session turn count 0, got %d", turnCount)
	}
}

func TestWorker_DualPool_YieldTrapFallback(t *testing.T) {
	t.Parallel()
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	sessID := "f1111111-2222-3333-4444-555555555555"
	sessDir := filepath.Join(tempData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0o644)

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

	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, newScriptedDaemonSpawner("d1111111-2222-3333-4444-555555555555"))
	defer lowPool.Close()

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
	threadID := "thread-yield-fallback"
	_ = store.SaveSessionID(context.Background(), threadID, sessID)

	taskDir := filepath.Join(tempData, "brain", "d1111111-2222-3333-4444-555555555555", ".system_generated", "tasks")
	_ = os.MkdirAll(taskDir, 0o755)
	_ = os.WriteFile(filepath.Join(taskDir, "bg-task-in-low-pool.log"), []byte("done"), 0o644)

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
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
	if _, err := lowPool.GetOrCreate(ctx, threadID, ""); err != nil {
		t.Fatalf("failed to create low daemon: %v", err)
	}

	msg := db.Message{ID: "msg-yield-fb-1", ThreadID: threadID, Content: "run command and wait", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}
	if tc := int(turnCount.Load()); tc < 1 {
		t.Errorf("expected auto-resumption from yield-trap detection, turnCount=%d", tc)
	}
}

func TestWorker_QuotaPause_PreservesEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, effort, expectedEffort string
	}{
		{name: "LowEffort", effort: "low", expectedEffort: "low"},
		{name: "MediumEffort", effort: "medium", expectedEffort: "medium"},
		{name: "HighEffort", effort: "high", expectedEffort: "high"},
		{name: "DefaultEffort", effort: "", expectedEffort: "high"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			quotaErrSpawner := newScriptedDaemonSpawner("11111111-2222-3333-4444-555555555555",
				`{"event":"result","result":{"status":"ERROR","exit_code":1,"error":"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m.","stderr":"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m."}}`+"\n",
			)
			primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, quotaErrSpawner)
			defer primaryPool.Close()
			lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, quotaErrSpawner)
			defer lowPool.Close()

			cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir, d.LowEffortModel = tempData, "gemini-3.8-flash-low" })
			doneCh := make(chan struct{})
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
				SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
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
			msg := db.Message{ID: "msg-quota-" + tt.name, ThreadID: threadID, Content: "run task", Effort: tt.effort, Status: db.StatusPending, CreatedAt: time.Now()}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for message completion")
			}
			schedules, err := store.GetAllOneShotSchedules(context.Background(), threadID)
			if err != nil || len(schedules) != 1 || schedules[0].Effort != tt.expectedEffort {
				t.Fatalf("expected 1 schedule with effort %q, got %v (err=%v)", tt.expectedEffort, schedules, err)
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
		{name: "EndToEnd_SessionRotationInjectsAndRetainsActions", maxAttempts: 3, triggerErr: "RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2s.", wantAttempts: 3, wantRetainSession: true},
		{name: "EndToEnd_TransientRotationInjectsToolActions", maxAttempts: 2, triggerErr: "transient connection reset", transientErr: true, wantAttempts: 2, wantRetainSession: true},
	}
	for _, tt := range rotationTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpHome, tempData, store := t.TempDir(), t.TempDir(), setupTestStore(t)
			validUUID := uuid.New().String()
			sessDir := filepath.Join(tempData, "brain", validUUID, ".system_generated", "logs")
			_ = os.MkdirAll(sessDir, 0o755)
			_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"investigate"}`+"\n"), 0o644)

			cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
			threadID := "thread-rot-" + strings.ToLower(tt.name)
			_ = store.SaveSessionID(context.Background(), threadID, validUUID)

			var mu sync.Mutex
			var recordedPrompts, recordedSessions []string
			doneCh := make(chan struct{})

			pool := New(cfg, WorkerPoolConfig{
				SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1, MaxAttempts: tt.maxAttempts,
				RetryDelayOverride: 10 * time.Millisecond, BackoffBase: 5 * time.Millisecond,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					mu.Lock()
					recordedPrompts = append(recordedPrompts, prompt)
					recordedSessions = append(recordedSessions, sessionID)
					attemptNum := len(recordedPrompts)
					mu.Unlock()
					if attemptNum < tt.wantAttempts {
						transcriptContent := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"investigate"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"run_command","args":{"command_line":"docker stats"}}]}
{"step_index":2,"source":"TOOL","type":"GENERIC","status":"DONE","content":"CONTAINER ID aerial-brain MEM USAGE 120MiB"}
{"step_index":%d,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":%q,"created_at":%q}
`, session.DefaultMaxSessionSteps, tt.triggerErr, time.Now().UTC().Format(time.RFC3339))
						_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0o644)
						return "", tt.triggerErr, 1, errors.New("simulated error")
					}
					return mockJSONResponse(uuid.New().String(), "Completed"), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
				TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
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

			msg := db.Message{ID: "msg-rot-" + strings.ToLower(tt.name), ThreadID: threadID, Content: "run investigation", Status: db.StatusPending, CreatedAt: time.Now()}
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
				t.Errorf("expected attempt 1 session %q, got %q", validUUID, recordedSessions[0])
			}
			if strings.Contains(recordedPrompts[0], "<PREVIOUS_TURN_ACTIONS>") {
				t.Errorf("unexpected PREVIOUS_TURN_ACTIONS in attempt 1")
			}
			for i := 1; i < tt.wantAttempts; i++ {
				if tt.wantRetainSession && recordedSessions[i] != validUUID {
					t.Errorf("expected attempt %d session %q, got %q", i+1, validUUID, recordedSessions[i])
				}
			}
		})
	}
}

func TestBuildTurnPrompt_AmbientResolver(t *testing.T) {
	t.Parallel()
	ambCfg := &config.AmbientContextConfig{Template: "Kitchen Temp: {{.temp}}"}
	tests := []struct {
		name                                   string
		ambCfg                                 *config.AmbientContextConfig
		resolverErr                            error
		resolverText                           string
		wantResolverCalled                     bool
		wantPromptContains, wantPromptExcludes []string
	}{
		{name: "ConfiguredChannel", ambCfg: ambCfg, resolverText: "<ambient_context>\nKitchen Temp: 71F\n</ambient_context>", wantResolverCalled: true, wantPromptContains: []string{"<ambient_context>", "Kitchen Temp: 71F", "turn off kitchen lights"}},
		{name: "UnconfiguredChannel_Bypass", ambCfg: nil, resolverText: "<ambient_context>should not appear</ambient_context>", wantResolverCalled: false, wantPromptContains: []string{"turn off kitchen lights"}, wantPromptExcludes: []string{"<ambient_context>"}},
		{name: "ResolverErrorTolerance", ambCfg: ambCfg, resolverErr: errors.New("mcp tool timed out"), wantResolverCalled: true, wantPromptContains: []string{"turn off kitchen lights"}, wantPromptExcludes: []string{"<ambient_context>"}},
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
				pool: pool, effectiveName: "kitchen", threadID: "t-amb-1", currentSessionID: "sess-amb-1",
				policy: config.ChannelPolicy{AmbientContext: tt.ambCfg},
				burst:  []db.Message{{ID: "m1", ThreadID: "t-amb-1", Content: "turn off kitchen lights"}},
			}
			te.buildTurnPrompt()
			if called := resolverCalled.Load(); called != tt.wantResolverCalled {
				t.Errorf("resolverCalled = %t, want %t", called, tt.wantResolverCalled)
			}
			for _, sub := range tt.wantPromptContains {
				if !strings.Contains(te.turnPrompt, sub) {
					t.Errorf("expected prompt containing %q", sub)
				}
			}
			for _, sub := range tt.wantPromptExcludes {
				if strings.Contains(te.turnPrompt, sub) {
					t.Errorf("expected prompt NOT containing %q", sub)
				}
			}
		})
	}
}

func TestWorker_RotatedSession_TreatsTurnOneAsColdStart(t *testing.T) {
	t.Parallel()
	store, tmpDir := setupTestStore(t), t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)
	threadID, oldSessionID, newSessionID := "thread-rotated-cold-test", "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"

	_ = store.SaveSessionID(context.Background(), threadID, oldSessionID)
	_ = store.RotateSessionID(context.Background(), threadID, newSessionID)
	_ = store.SaveThreadSummary(context.Background(), threadID, "<THREAD_SUMMARY>Discussed earlier deployment</THREAD_SUMMARY>", "msg-pre-rot")

	CacheDiscordChannel(&discordgo.Channel{ID: threadID, GuildID: "g1", ParentID: "parent-chan", Type: discordgo.ChannelTypeGuildPublicThread})

	var mu sync.Mutex
	var capturedPrompts []string
	doneCh := make(chan struct{}, 2)
	appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "threads"}}})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: sessMgr, Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
		HistoryFetcher: func(ctx context.Context, tid, beforeID string, limit int) ([]HistoryMessage, error) {
			return []HistoryMessage{{ID: "msg-hist-1", AuthorName: "test-user", Content: "pre-rotation message", CreatedAt: time.Now().Add(-10 * time.Minute)}}, nil
		},
		LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
			return "<THREAD_SUMMARY>Discussed earlier deployment</THREAD_SUMMARY>", nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			capturedPrompts = append(capturedPrompts, prompt)
			mu.Unlock()
			return mockJSONResponse(sessionID, "Turn response"), "", 0, nil
		},
		DeliveryFunc:       func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:         func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) { doneCh <- struct{}{} },
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
	if !strings.Contains(capturedPrompts[0], "<THREAD_SUMMARY>Discussed earlier deployment</THREAD_SUMMARY>") || !strings.Contains(capturedPrompts[0], "<PREVIOUS_SESSION>") || !strings.Contains(capturedPrompts[0], "<CHANNEL_HISTORY>") {
		t.Errorf("Turn 1 prompt missing expected cold-start context")
	}
	if strings.Contains(capturedPrompts[1], "<THREAD_SUMMARY>") || strings.Contains(capturedPrompts[1], "<PREVIOUS_SESSION>") || strings.Contains(capturedPrompts[1], "<CHANNEL_HISTORY>") {
		t.Errorf("Turn 2 prompt unexpectedly contained cold-start context")
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
		{name: "NormalBurst", burst: []db.Message{{ID: "m1", Effort: "high", Metadata: db.MessageMetadata{Model: "gemini-3.1-pro"}}, {ID: "m2", Metadata: db.MessageMetadata{Silent: false}}}, wantModel: "gemini-3.1-pro", wantSilent: false, wantEffort: "high"},
		{name: "SilentFlashBurst", burst: []db.Message{{ID: "m1", Metadata: db.MessageMetadata{Silent: true}}, {ID: "m2", Metadata: db.MessageMetadata{Silent: true, Model: "  gemini-4-flash  "}}}, wantModel: "gemini-4-flash", wantSilent: true, wantEffort: "low"},
		{name: "MixedSilentAndNormalBurst", burst: []db.Message{{ID: "m1", Metadata: db.MessageMetadata{Silent: true}}, {ID: "m2", Metadata: db.MessageMetadata{Silent: false, Model: "  gemini-4-flash  "}}}, wantModel: "gemini-4-flash", wantSilent: false, wantEffort: "low"},
		{name: "LowEffortFieldBurst", burst: []db.Message{{ID: "m1", Effort: " LOW "}}, wantModel: "", wantSilent: false, wantEffort: "low"},
		{name: "MediumEffortFieldBurst", burst: []db.Message{{ID: "m1", Effort: " MEDIUM "}}, wantModel: "", wantSilent: false, wantEffort: "medium"},
		{name: "FlashMediumModelBurstNotLow", burst: []db.Message{{ID: "m1", Metadata: db.MessageMetadata{Model: "gemini-3.8-flash-medium"}}}, wantModel: "gemini-3.8-flash-medium", wantSilent: false, wantEffort: "medium"},
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
	store := dbtest.NewFakeStore()
	ctx := context.Background()
	threadID, sessionID := "1542423172400291873", "a1111111-2222-3333-4444-555555555555"

	var mu sync.Mutex
	var deliveredMsgs []string
	var typingCalls int
	doneCh := make(chan string, 1)

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, MaxAttempts: 1, BackoffBase: 1 * time.Millisecond, TimeoutMinutes: 1,
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
		OnMessageCompleted: func(msg db.Message, status string) { doneCh <- status },
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-silent-exec-1", ThreadID: threadID, Content: "Execute silent task", Status: db.StatusPending, Metadata: db.MessageMetadata{Model: "gemini-4-flash", Silent: true}}
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
	if typingCalls != 0 || len(deliveredMsgs) != 0 {
		t.Errorf("expected 0 typing and delivery, got %d typing, %d delivered", typingCalls, len(deliveredMsgs))
	}
	storedMsg, err := store.GetMessage(ctx, msg.ID)
	if err != nil || storedMsg == nil || storedMsg.Status != db.StatusCompleted {
		t.Errorf("expected stored message StatusCompleted, got %v (err=%v)", storedMsg, err)
	}
}

func TestYieldTrap_Scenarios(t *testing.T) {
	tests := []struct {
		name, threadID, sessionID, msgID, metricOutcome, metricSignature, expectDeliveredText, expectedSecondPrompt string
		maxYields                                                                                                   int
		expectInvocations                                                                                           int32
		verifyPromptsSeen                                                                                           bool
	}{
		{
			name: "auto-resumption on stderr signature success", threadID: "thread-yield-auto", sessionID: "f2222222-3333-4444-5555-666666666666", msgID: "msg-yield-1",
			maxYields: 1, expectInvocations: 2, expectDeliveredText: "Job completed after resumption!", metricOutcome: "resumed", metricSignature: "stderr_signature", verifyPromptsSeen: true, expectedSecondPrompt: YieldTrapResumePrompt,
		},
		{
			name: "circuit breaker trips after max resumes", threadID: "thread-yield-cb", sessionID: "f3333333-4444-5555-6666-777777777777", msgID: "msg-circuit-1",
			maxYields: 99, expectInvocations: 3, expectDeliveredText: "Still waiting turn 3", metricOutcome: "circuit_breaker", metricSignature: "stderr_signature",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, tmpHome := setupTestStore(t), t.TempDir()
			var runnerInvocations atomic.Int32
			var promptsSeenMu, deliveredMu sync.Mutex
			var promptsSeen []string
			var deliveredText string
			doneCh := make(chan struct{})

			sessMgr := session.New(tmpHome, "")
			sessDir, _ := ensureTestSessionDir(sessMgr, tc.sessionID)
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0o644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)

			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: sessMgr, Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					inv := runnerInvocations.Add(1)
					promptsSeenMu.Lock()
					promptsSeen = append(promptsSeen, prompt)
					promptsSeenMu.Unlock()
					_ = os.Chtimes(sessDir, time.Now(), time.Now())
					if int(inv) <= tc.maxYields {
						msg := fmt.Sprintf("Still waiting turn %d", inv)
						if tc.maxYields == 1 {
							msg = "Waiting on background task task-444"
						}
						return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q,"usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}`, tc.sessionID, msg), "terminating 1 background task(s) on exit\n", 0, nil
					}
					return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q,"usage":{"input_tokens":150,"output_tokens":35,"total_tokens":185}}`, tc.sessionID, tc.expectDeliveredText), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					deliveredMu.Lock()
					deliveredText = text
					deliveredMu.Unlock()
					return nil
				},
				TypingFunc:         func(s *discordgo.Session, channelID string) func() { return func() {} },
				OnMessageCompleted: func(msg db.Message, finalStatus string) { close(doneCh) },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, GuildID: "guild-1", AuthorID: "user-1", AuthorName: "test-user", Content: "Run task", Status: db.StatusPending, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
			_ = insertMessage(store, msg)
			model := pool.GetRuntimeConfig()
			initialMetric := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues(tc.metricOutcome, tc.metricSignature, model))
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for message processing")
			}

			if runnerInvocations.Load() != tc.expectInvocations {
				t.Fatalf("invocations = %d, want %d", runnerInvocations.Load(), tc.expectInvocations)
			}
			if tc.verifyPromptsSeen {
				promptsSeenMu.Lock()
				seenCount := len(promptsSeen)
				secondPrompt := ""
				if seenCount > 1 {
					secondPrompt = promptsSeen[1]
				}
				promptsSeenMu.Unlock()
				if seenCount != int(tc.expectInvocations) || secondPrompt != tc.expectedSecondPrompt {
					t.Errorf("prompts mismatch: count=%d second=%q", seenCount, secondPrompt)
				}
			}
			deliveredMu.Lock()
			finalText := deliveredText
			deliveredMu.Unlock()
			if finalText != tc.expectDeliveredText {
				t.Errorf("delivered = %q, want %q", finalText, tc.expectDeliveredText)
			}
			newMetric := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues(tc.metricOutcome, tc.metricSignature, model))
			if newMetric != initialMetric+1 {
				t.Errorf("metric delta = %.0f, want 1", newMetric-initialMetric)
			}
		})
	}
}
