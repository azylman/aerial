package queue

import (
	"bufio"
	"context"
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

	"github.com/azylman/aerial/services/eri/pkg/classifier"
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
			haroSrv, getOneShots := newMockHaroOneShotServer(t)
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool: procPool, SessionManager: session.New(tmpHome, tempData),
				Store: store, TimeoutMinutes: 1, HaroURL: haroSrv.URL,
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
				schedules := getOneShots(tc.threadID)
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

func TestWorker_EphemeralHighEffort_FailsBurst(t *testing.T) {
	t.Parallel()
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	primarySessUUID := "11111111-1111-1111-1111-111111111111"

	var spawned atomic.Int32
	spawner := &mockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			spawned.Add(1)
			return nil, nil, nil, nil, errors.New("should not spawn")
		},
	}
	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, spawner)
	defer primaryPool.Close()

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
	doneCh := make(chan string, 1)
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    primaryPool,
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
		OnMessageCompleted: func(msg db.Message, status string) {
			doneCh <- status
		},
	})
	defer pool.Stop()

	threadID := "1560318118792790999"
	ctx := context.Background()
	_ = store.SaveSessionID(ctx, threadID, primarySessUUID)

	trueVal := true
	msg := db.Message{
		ID: "msg-ephem-high-fail", ThreadID: threadID, Content: "execute invalid ephemeral high effort",
		Effort: "high", Ephemeral: &trueVal, Status: db.StatusPending, CreatedAt: time.Now(),
	}
	_ = store.InsertMessage(ctx, msg)
	pool.Enqueue(msg)
	select {
	case status := <-doneCh:
		if status != db.StatusFailed {
			t.Fatalf("expected status %q, got %q", db.StatusFailed, status)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for failed burst")
	}

	if got := spawned.Load(); got != 0 {
		t.Errorf("expected 0 spawns on primaryPool, got %d", got)
	}
	currentSess, err := store.GetSessionID(ctx, threadID)
	if err != nil || currentSess != primarySessUUID {
		t.Errorf("expected primary session %s untouched, got %s", primarySessUUID, currentSess)
	}
}

func TestWorker_StatefulLowAndMediumPools_ResumeAndPersistSessions(t *testing.T) {
	t.Parallel()
	falseVal := false
	tests := []struct {
		name   string
		effort string
	}{
		{"stateful_low", "low"},
		{"stateful_medium", "medium"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			existingSessUUID := "11111111-1111-1111-1111-111111111111"
			rotatedSessUUID := "22222222-2222-2222-2222-222222222222"

			var mu sync.Mutex
			var spawnedSessionID string
			var ephemSpawns atomic.Int32

			statefulSpawner := &mockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
					mu.Lock()
					spawnedSessionID = cfg.SessionID
					mu.Unlock()

					inR, inW := io.Pipe()
					outR, outW := io.Pipe()
					errR, errW := io.Pipe()
					defer errW.Close()
					go func() {
						defer outW.Close()
						_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n", rotatedSessUUID)))
						buf := make([]byte, 16384)
						for {
							n, rErr := inR.Read(buf)
							if rErr != nil {
								return
							}
							if n > 0 {
								_, _ = outW.Write([]byte(fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok\",\"conversation_id\":%q,\"usage\":{\"total_tokens\":5}}}\n", rotatedSessUUID)))
							}
						}
					}()
					return inW, outR, errR, newMockProcessHandle(23456), nil
				},
			}
			ephemSpawner := &mockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
					ephemSpawns.Add(1)
					return nil, nil, nil, nil, errors.New("ephemeral pool should not be used")
				},
			}

			primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, newMockSpawner())
			defer primaryPool.Close()
			statefulLow := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, statefulSpawner)
			defer statefulLow.Close()
			statefulMed := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-medium"}, statefulSpawner)
			defer statefulMed.Close()
			ephemLowU := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, ephemSpawner)
			defer ephemLowU.Close()
			ephemLow := runner.NewInterchangeablePool(ephemLowU, runner.InterchangeablePoolConfig{WorkerCount: 1, KeyPrefix: "discord:worker"})
			defer ephemLow.Close()
			ephemMedU := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-medium"}, ephemSpawner)
			defer ephemMedU.Close()
			ephemMed := runner.NewInterchangeablePool(ephemMedU, runner.InterchangeablePoolConfig{WorkerCount: 1, KeyPrefix: "discord:medium-worker"})
			defer ephemMed.Close()

			cfg := newTestConfig(func(d *config.ConfigData) {
				d.DataDir = tempData
				d.LowEffortModel = "gemini-3.8-flash-low"
				d.MediumEffortModel = "gemini-3.8-flash-medium"
			})
			doneCh := make(chan struct{}, 1)
			pool := New(cfg, WorkerPoolConfig{
				Pools: map[PoolKey]runner.AgentPool{
					{Ephemeral: false, Effort: "high"}:   primaryPool,
					{Ephemeral: false, Effort: "medium"}: statefulMed,
					{Ephemeral: false, Effort: "low"}:    statefulLow,
					{Ephemeral: true, Effort: "medium"}:  ephemMed,
					{Ephemeral: true, Effort: "low"}:     ephemLow,
				},
				SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
				OnMessageCompleted: func(msg db.Message, status string) {
					if status == db.StatusCompleted {
						doneCh <- struct{}{}
					}
				},
			})
			defer pool.Stop()

			threadID := "1560318118792790888-" + tc.effort
			ctx := context.Background()
			_ = store.SaveSessionID(ctx, threadID, existingSessUUID)

			msg := db.Message{
				ID: "msg-stateful-" + tc.effort, ThreadID: threadID, Content: "execute persistent " + tc.effort,
				Effort: tc.effort, Ephemeral: &falseVal, Status: db.StatusPending, CreatedAt: time.Now(),
			}
			_ = store.InsertMessage(ctx, msg)
			pool.Enqueue(msg)
			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout waiting for stateful %s turn", tc.effort)
			}
			pool.Stop()

			if got := ephemSpawns.Load(); got != 0 {
				t.Errorf("expected 0 spawns on ephemeral pools, got %d", got)
			}
			mu.Lock()
			gotSpawned := spawnedSessionID
			mu.Unlock()
			if gotSpawned != existingSessUUID {
				t.Errorf("expected spawner to receive existing session %s, got %s", existingSessUUID, gotSpawned)
			}
			currentSess, err := store.GetSessionID(ctx, threadID)
			if err != nil || currentSess != rotatedSessUUID {
				t.Errorf("expected rotated session %s persisted in DB, got %s (err=%v)", rotatedSessUUID, currentSess, err)
			}
			turnCount, err := store.GetSessionTurnCount(ctx, threadID)
			if err != nil || turnCount != 1 {
				t.Errorf("expected session turn count 1 in DB, got %d (err=%v)", turnCount, err)
			}
		})
	}
}

func TestWorker_ResolveExecutionPool_EphemeralRouting_TableDriven(t *testing.T) {
	t.Parallel()
	primaryPool := &runner.UnifiedProcessPool{}
	lowStateful := &runner.UnifiedProcessPool{}
	medStateful := &runner.UnifiedProcessPool{}
	lowEphem := &runner.UnifiedProcessPool{}
	medEphem := &runner.UnifiedProcessPool{}

	trueVal := true
	falseVal := false

	tests := []struct {
		name        string
		effort      string
		ephemeral   *bool
		wantPool    runner.AgentPool
		wantPoolKey PoolKey
		wantErr     bool
	}{
		{
			name:        "default low effort routes to ephemeral low pool",
			effort:      "low",
			ephemeral:   nil,
			wantPool:    lowEphem,
			wantPoolKey: PoolKey{Ephemeral: true, Effort: "low"},
		},
		{
			name:        "explicit ephemeral true with low effort routes to ephemeral low pool",
			effort:      "low",
			ephemeral:   &trueVal,
			wantPool:    lowEphem,
			wantPoolKey: PoolKey{Ephemeral: true, Effort: "low"},
		},
		{
			name:        "explicit ephemeral false with low effort routes to stateful low pool",
			effort:      "low",
			ephemeral:   &falseVal,
			wantPool:    lowStateful,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "low"},
		},
		{
			name:        "default medium effort routes to ephemeral medium pool",
			effort:      "medium",
			ephemeral:   nil,
			wantPool:    medEphem,
			wantPoolKey: PoolKey{Ephemeral: true, Effort: "medium"},
		},
		{
			name:        "explicit ephemeral true with medium effort routes to ephemeral medium pool",
			effort:      "medium",
			ephemeral:   &trueVal,
			wantPool:    medEphem,
			wantPoolKey: PoolKey{Ephemeral: true, Effort: "medium"},
		},
		{
			name:        "explicit ephemeral false with medium effort routes to stateful medium pool",
			effort:      "medium",
			ephemeral:   &falseVal,
			wantPool:    medStateful,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "medium"},
		},
		{
			name:        "default high effort routes to stateful high pool",
			effort:      "high",
			ephemeral:   nil,
			wantPool:    primaryPool,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "high"},
		},
		{
			name:      "explicit ephemeral true with high effort returns error",
			effort:    "high",
			ephemeral: &trueVal,
			wantErr:   true,
		},
		{
			name:      "explicit ephemeral true with empty effort returns error",
			effort:    "",
			ephemeral: &trueVal,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := &WorkerPool{
				pools: map[PoolKey]runner.AgentPool{
					{Ephemeral: false, Effort: "high"}:   primaryPool,
					{Ephemeral: false, Effort: "medium"}: medStateful,
					{Ephemeral: false, Effort: "low"}:    lowStateful,
					{Ephemeral: true, Effort: "medium"}:  medEphem,
					{Ephemeral: true, Effort: "low"}:     lowEphem,
				},
			}
			te := &turnExecution{
				pool:     wp,
				threadID: "thread-ephem-test",
				burst: []db.Message{
					{
						ID:        "msg-1",
						Effort:    tc.effort,
						Ephemeral: tc.ephemeral,
					},
				},
			}

			activePool, poolKey, err := te.resolveExecutionPool()
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveExecutionPool() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if activePool != tc.wantPool {
				t.Errorf("activePool = %v, want %v", activePool, tc.wantPool)
			}
			if poolKey != tc.wantPoolKey {
				t.Errorf("poolKey = %+v, want %+v", poolKey, tc.wantPoolKey)
			}
		})
	}
}

func TestWorker_ResolveTurnPolicy_ParentChannelFallback_TableDriven(t *testing.T) {
	t.Parallel()
	CacheDiscordChannel(&discordgo.Channel{
		ID:   "5566778899",
		Name: "aerial-dev",
		Type: discordgo.ChannelTypeGuildText,
	})

	tests := []struct {
		name              string
		threadID          string
		parentChannelID   string
		wantEffectiveName string
	}{
		{
			name:              "custom session key with parent channel metadata resolves parent name",
			threadID:          "heartbeat-session-123",
			parentChannelID:   "5566778899",
			wantEffectiveName: "aerial-dev",
		},
		{
			name:              "custom session key without parent channel metadata has empty effective name",
			threadID:          "custom-key-no-parent",
			parentChannelID:   "",
			wantEffectiveName: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := &WorkerPool{
				cfg: WorkerPoolConfig{
					Store: dbtest.NewFakeStore(),
					ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
						return config.ChannelPolicy{Mode: "channel"}
					},
				},
			}
			te := &turnExecution{
				pool:     pool,
				threadID: tc.threadID,
				burst: []db.Message{
					{
						ID:        "msg-fallback",
						ThreadID:  tc.threadID,
						ChannelID: tc.parentChannelID,
					},
				},
			}
			_ = te.resolveTurnPolicy()
			if te.effectiveName != tc.wantEffectiveName {
				t.Errorf("te.effectiveName = %q, want %q", te.effectiveName, tc.wantEffectiveName)
			}
		})
	}
}

func TestWorker_EvaluateAmbientWake_HTTPAndSilent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authorID      string
		silent        bool
		isThread      bool
		mode          string
		wantSkipClass bool
	}{
		{
			name:          "standard user message in channel mode is evaluated for ambient",
			authorID:      "user-1",
			silent:        false,
			isThread:      false,
			mode:          "channel",
			wantSkipClass: false,
		},
		{
			name:          "http-client author message skips ambient classifier",
			authorID:      "http-client",
			silent:        false,
			isThread:      false,
			mode:          "channel",
			wantSkipClass: true,
		},
		{
			name:          "silent message skips ambient classifier",
			authorID:      "user-1",
			silent:        true,
			isThread:      false,
			mode:          "channel",
			wantSkipClass: true,
		},
		{
			name:          "thread message skips channel ambient classifier",
			authorID:      "user-1",
			silent:        false,
			isThread:      true,
			mode:          "channel",
			wantSkipClass: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := &WorkerPool{
				cfg: WorkerPoolConfig{
					Classifier: classifier.New(nil),
				},
			}
			te := &turnExecution{
				pool:     pool,
				threadID: "chan-ambient-test",
				isThread: tc.isThread,
				policy:   config.ChannelPolicy{Mode: tc.mode},
				burst: []db.Message{
					{
						ID:       "msg-ambient",
						AuthorID: tc.authorID,
						Content:  "ambient message text",
						Silent:   tc.silent,
					},
				},
			}

			_ = te.evaluateAmbientWake()
			if tc.wantSkipClass {
				if te.wakeInfos != nil {
					t.Errorf("expected ambient wake to be skipped, but wakeInfos was populated: %v", te.wakeInfos)
				}
			}
		})
	}
}
