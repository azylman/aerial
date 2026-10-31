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
		Pools: map[PoolKey]runner.AgentPool{
			{Ephemeral: false, Effort: "high"}: primaryPool,
			{Ephemeral: false, Effort: "low"}:  lowPool,
		},
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
	doneCh := make(chan struct{}, 2)

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
						select {
						case doneCh <- struct{}{}:
						default:
						}
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
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
		HistoryFetcher: func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			return []HistoryMessage{{ID: "hist-1", AuthorName: "test-user", Content: "prior chat history should not be injected", CreatedAt: time.Now().Add(-5 * time.Minute)}}, nil
		},
	})
	defer pool.Stop()

	threadID := "1560318118792790111"
	ctx := context.Background()
	_ = store.SaveSessionID(ctx, threadID, prevSessUUID)
	_ = store.RotateSessionID(ctx, threadID, primarySessUUID)
	_ = store.SaveThreadSummary(ctx, threadID, "Cached summary that must not be injected", "hist-1")

	d1 := db.Dispatch{ID: "msg-ephem-1", Source: "vanadis", Prompt: "execute vanadis chunk 1", Effort: "low", Status: db.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	_ = store.InsertDispatch(ctx, d1)
	pool.EnqueueDispatch(d1)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ephemeral turn 1")
	}

	d2 := db.Dispatch{ID: "msg-ephem-2", Source: "vanadis", Prompt: "execute vanadis chunk 2", Effort: "low", Status: db.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	_ = store.InsertDispatch(ctx, d2)
	pool.EnqueueDispatch(d2)
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

type mockAcquireLeaseFailPool struct {
	runner.AgentPool
}

func (m *mockAcquireLeaseFailPool) AcquireLease(ctx context.Context, targetKey string) (runner.SessionLease, error) {
	return nil, errors.New("lease acquisition failed")
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
		ProcessPool:    &mockAcquireLeaseFailPool{AgentPool: primaryPool},
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
		OnMessageCompleted: func(msg db.Message, status string) {
			doneCh <- status
		},
	})
	defer pool.Stop()

	threadID := "1560318118792790999"
	ctx := context.Background()
	_ = store.SaveSessionID(ctx, threadID, primarySessUUID)

	msg := db.Message{
		ID: "msg-ephem-high-fail", ThreadID: threadID, Content: "execute turn with failing lease",
		Effort: "high", Status: db.StatusPending, CreatedAt: time.Now(),
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
				Effort: tc.effort, Status: db.StatusPending, CreatedAt: time.Now(),
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

	tests := []struct {
		name        string
		dispatch    *db.Dispatch
		burst       []db.Message
		wantPool    runner.AgentPool
		wantPoolKey PoolKey
	}{
		{
			name:        "dispatch with low effort routes to ephemeral low pool",
			dispatch:    &db.Dispatch{Effort: "low"},
			wantPool:    lowEphem,
			wantPoolKey: PoolKey{Ephemeral: true, Effort: "low"},
		},
		{
			name:        "dispatch with medium effort routes to ephemeral medium pool",
			dispatch:    &db.Dispatch{Effort: "medium"},
			wantPool:    medEphem,
			wantPoolKey: PoolKey{Ephemeral: true, Effort: "medium"},
		},
		{
			name:        "stateful dispatch with low effort routes to stateful low pool",
			dispatch:    &db.Dispatch{SessionID: "hb-sess-1", Effort: "low"},
			wantPool:    lowStateful,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "low"},
		},
		{
			name:        "stateful dispatch with medium effort routes to stateful medium pool",
			dispatch:    &db.Dispatch{SessionID: "hb-sess-2", Effort: "medium"},
			wantPool:    medStateful,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "medium"},
		},
		{
			name:        "message burst with low effort routes to stateful low pool",
			burst:       []db.Message{{ID: "m1", Effort: "low"}},
			wantPool:    lowStateful,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "low"},
		},
		{
			name:        "message burst with medium effort routes to stateful medium pool",
			burst:       []db.Message{{ID: "m1", Effort: "medium"}},
			wantPool:    medStateful,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "medium"},
		},
		{
			name:        "message burst with high effort routes to stateful high pool",
			burst:       []db.Message{{ID: "m1", Effort: "high"}},
			wantPool:    primaryPool,
			wantPoolKey: PoolKey{Ephemeral: false, Effort: "high"},
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
				dispatch: tc.dispatch,
				burst:    tc.burst,
			}

			activePool, poolKey, err := te.resolveExecutionPool()
			if err != nil {
				t.Fatalf("resolveExecutionPool() unexpected error: %v", err)
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
		isThread      bool
		mode          string
		wantSkipClass bool
	}{
		{
			name:          "standard user message in channel mode is evaluated for ambient",
			authorID:      "user-1",
			isThread:      false,
			mode:          "channel",
			wantSkipClass: false,
		},
		{
			name:          "http-client author message skips ambient classifier",
			authorID:      "http-client",
			isThread:      false,
			mode:          "channel",
			wantSkipClass: true,
		},
		{
			name:          "thread message skips channel ambient classifier",
			authorID:      "user-1",
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
					},
				},
			}

			_ = te.evaluateAmbientWake()
			defer te.stopIndicators()
			if tc.wantSkipClass {
				if te.wakeInfos != nil {
					t.Errorf("expected ambient wake to be skipped, but wakeInfos was populated: %v", te.wakeInfos)
				}
			}
		})
	}
}

type retryNotifyStore struct {
	db.Store
	onRetry func()
}

func (s *retryNotifyStore) IncrementMessageRetry(ctx context.Context, id, errMsg string) error {
	err := s.Store.IncrementMessageRetry(ctx, id, errMsg)
	if s.onRetry != nil {
		s.onRetry()
	}
	return err
}

func TestWorkerPool_InterruptThread(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		drainQueue         bool
		reset              bool
		firstAttemptErr    string
		enqueueSecondMsg   bool
		backoffBase        time.Duration
		wantMsg1Status     string
		wantMsg1Err        string
		wantMsg1Retries    int
		wantMsg1Attempts   int
		wantMsg2Status     string
		wantMsg2Err        string
		wantMsg2Resp       string
		wantSessionRotated bool
	}{
		{
			name:               "ActiveTurn_DrainQueue_SuppressesRetryAndPreservesSession",
			drainQueue:         true,
			reset:              false,
			firstAttemptErr:    "",
			enqueueSecondMsg:   true,
			backoffBase:        20 * time.Millisecond,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    0,
			wantMsg1Attempts:   1,
			wantMsg2Status:     db.StatusFailed,
			wantMsg2Err:        InterruptedByOperatorReason,
			wantSessionRotated: false,
		},
		{
			name:               "DuringRetryBackoff_WithReset_AbortsSleepAndRotatesSession",
			drainQueue:         true,
			reset:              true,
			firstAttemptErr:    "503 service unavailable",
			enqueueSecondMsg:   false,
			backoffBase:        30 * time.Second,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    1,
			wantMsg1Attempts:   1,
			wantSessionRotated: true,
		},
		{
			name:               "DuringRetryBackoffNoReset",
			drainQueue:         false,
			reset:              false,
			firstAttemptErr:    "503 service unavailable",
			enqueueSecondMsg:   false,
			backoffBase:        30 * time.Second,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    1,
			wantMsg1Attempts:   1,
			wantSessionRotated: false,
		},
		{
			name:               "DuringCapacityBlipBackoff_AbortsSleepAndFails",
			drainQueue:         true,
			reset:              false,
			firstAttemptErr:    "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.",
			enqueueSecondMsg:   false,
			backoffBase:        30 * time.Second,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    1,
			wantMsg1Attempts:   1,
			wantSessionRotated: false,
		},
		{
			name:               "ActiveTurn_NoDrain_AbortsActiveTurnAndRunsQueuedMessage",
			drainQueue:         false,
			reset:              false,
			firstAttemptErr:    "",
			enqueueSecondMsg:   true,
			backoffBase:        20 * time.Millisecond,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    0,
			wantMsg1Attempts:   1,
			wantMsg2Status:     db.StatusCompleted,
			wantMsg2Resp:       "second turn ok",
			wantSessionRotated: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseStore := setupTestStore(t)
			tmpHome, tempData := t.TempDir(), t.TempDir()
			initialSessID := "11111111-2222-3333-4444-555555555555"
			threadID := "thread-interrupt-" + strings.ToLower(tc.name)

			interruptReadyCh := make(chan struct{})
			var readyOnce sync.Once
			signalReady := func() {
				readyOnce.Do(func() { close(interruptReadyCh) })
			}

			store := &retryNotifyStore{
				Store: baseStore,
				onRetry: func() {
					if tc.firstAttemptErr != "" {
						signalReady()
					}
				},
			}
			_ = store.SaveSessionID(context.Background(), threadID, initialSessID)

			var msg1Attempts atomic.Int32
			var spawnCount atomic.Int32
			spawner := &mockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
					inR, inW := io.Pipe()
					outR, outW := io.Pipe()
					errR, errW := io.Pipe()
					n := spawnCount.Add(1)
					sessID := cfg.SessionID
					if sessID == "" {
						if n == 1 {
							sessID = initialSessID
						} else {
							sessID = fmt.Sprintf("sess-rotated-%d", n)
						}
					}
					go func() {
						defer outW.Close()
						defer errW.Close()
						_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID)
						scanner := bufio.NewScanner(inR)
						for scanner.Scan() {
							line := scanner.Text()
							if strings.Contains(line, "first turn prompt") {
								att := msg1Attempts.Add(1)
								if tc.firstAttemptErr != "" && att == 1 {
									_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":%q,\"conversation_id\":%q}}\n", tc.firstAttemptErr, sessID)
									continue
								}
								if tc.firstAttemptErr == "" && att == 1 {
									signalReady()
									_, _ = io.Copy(io.Discard, inR)
									return
								}
							}
							_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"second turn ok\",\"conversation_id\":%q,\"usage\":{\"total_tokens\":5}}}\n", sessID)
						}
					}()
					return inW, outR, errR, newMockProcessHandle(34567), nil
				},
			}

			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
				DefaultModel: "gemini-2.5-flash",
			}, spawner)
			defer procPool.Close()

			expectedCompletions := 1
			if tc.enqueueSecondMsg {
				expectedCompletions = 2
			}
			var completedCount atomic.Int32
			doneCh := make(chan struct{})

			cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool:    procPool,
				SessionManager: session.New(tmpHome, tempData),
				Store:          store,
				TimeoutMinutes: 1,
				MaxAttempts:    3,
				BackoffBase:    tc.backoffBase,
				OnMessageCompleted: func(msg db.Message, status string) {
					if int(completedCount.Add(1)) >= expectedCompletions {
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
			msg1 := db.Message{ID: "msg-1-" + tc.name, ThreadID: threadID, Content: "first turn prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
			_ = store.InsertMessage(ctx, msg1)
			pool.Enqueue(msg1)

			select {
			case <-interruptReadyCh:
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for turn 1 to reach interrupt point")
			}

			msg2ID := "msg-2-" + tc.name
			if tc.enqueueSecondMsg {
				msg2 := db.Message{ID: msg2ID, ThreadID: threadID, Content: "second turn prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
				_ = store.InsertMessage(ctx, msg2)
				pool.Enqueue(msg2)
			}
			if tc.drainQueue {
				_ = store.InsertMessage(ctx, db.Message{ID: "msg-db-pending-" + tc.name, ThreadID: threadID, Content: "unclaimed pending", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
			}

			pool.InterruptThread(ctx, threadID, tc.drainQueue, tc.reset)

			select {
			case <-doneCh:
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for messages to finalize after InterruptThread")
			}

			gotMsg1, err := store.GetMessage(ctx, msg1.ID)
			if err != nil || gotMsg1 == nil {
				t.Fatalf("failed to get msg1: %v", err)
			}
			if gotMsg1.Status != tc.wantMsg1Status {
				t.Errorf("msg1 status = %q, want %q", gotMsg1.Status, tc.wantMsg1Status)
			}
			if gotMsg1.ErrorMessage != tc.wantMsg1Err {
				t.Errorf("msg1 error = %q, want %q", gotMsg1.ErrorMessage, tc.wantMsg1Err)
			}
			if gotMsg1.RetryCount != tc.wantMsg1Retries {
				t.Errorf("msg1 retry_count = %d, want %d", gotMsg1.RetryCount, tc.wantMsg1Retries)
			}
			if got := int(msg1Attempts.Load()); got != tc.wantMsg1Attempts {
				t.Errorf("msg1 attempts = %d, want %d", got, tc.wantMsg1Attempts)
			}

			if tc.enqueueSecondMsg {
				gotMsg2, err := store.GetMessage(ctx, msg2ID)
				if err != nil || gotMsg2 == nil {
					t.Fatalf("failed to get msg2: %v", err)
				}
				if gotMsg2.Status != tc.wantMsg2Status {
					t.Errorf("msg2 status = %q, want %q", gotMsg2.Status, tc.wantMsg2Status)
				}
				if gotMsg2.ErrorMessage != tc.wantMsg2Err {
					t.Errorf("msg2 error = %q, want %q", gotMsg2.ErrorMessage, tc.wantMsg2Err)
				}
				if gotMsg2.ResponseText != tc.wantMsg2Resp {
					t.Errorf("msg2 response = %q, want %q", gotMsg2.ResponseText, tc.wantMsg2Resp)
				}
			}

			gotSess, _ := store.GetSessionID(ctx, threadID)
			if tc.wantSessionRotated {
				if gotSess == "" || gotSess == initialSessID {
					t.Errorf("expected session ID to rotate away from %q, got %q", initialSessID, gotSess)
				}
			} else if gotSess != initialSessID {
				t.Errorf("expected session ID %q to be preserved, got %q", initialSessID, gotSess)
			}
		})
	}
}

func TestTurnExecution_IsDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want bool
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: false,
		},
		{
			name: "turn without dispatch",
			te:   &turnExecution{threadID: "123"},
			want: false,
		},
		{
			name: "turn with dispatch",
			te:   &turnExecution{threadID: "123", dispatch: &db.Dispatch{ID: "disp-1"}},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.isDispatch(); got != tc.want {
				t.Errorf("isDispatch() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_LeaseKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want string
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: "",
		},
		{
			name: "dispatch turn uses threadID as lease key",
			te:   &turnExecution{threadID: "task-dispatch-1", dispatch: &db.Dispatch{ID: "disp-1"}},
			want: "task-dispatch-1",
		},
		{
			name: "discord turn uses discord prefix",
			te:   &turnExecution{threadID: "123456789"},
			want: "discord-123456789",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.leaseKey(); got != tc.want {
				t.Errorf("leaseKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_InitialRetryCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want int
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: 0,
		},
		{
			name: "dispatch turn uses dispatch retry count",
			te:   &turnExecution{dispatch: &db.Dispatch{ID: "d1", RetryCount: 3}},
			want: 3,
		},
		{
			name: "burst turn uses max burst retry count",
			te: &turnExecution{burst: []db.Message{
				{ID: "m1", RetryCount: 1},
				{ID: "m2", RetryCount: 4},
				{ID: "m3", RetryCount: 2},
			}},
			want: 4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.initialRetryCount(); got != tc.want {
				t.Errorf("initialRetryCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_TurnEffortAndModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		te         *turnExecution
		wantEffort string
		wantModel  string
	}{
		{
			name:       "nil receiver",
			te:         nil,
			wantEffort: "",
			wantModel:  "",
		},
		{
			name: "dispatch turn with explicit model and effort",
			te: &turnExecution{
				dispatch: &db.Dispatch{ID: "d1", Model: "gemini-custom", Effort: "high", SessionID: "sess-1"},
			},
			wantEffort: "high",
			wantModel:  "gemini-custom",
		},
		{
			name: "dispatch turn ephemeral defaults to medium effort",
			te: &turnExecution{
				isEphemeral: true,
				dispatch:    &db.Dispatch{ID: "d2"},
			},
			wantEffort: "medium",
			wantModel:  "",
		},
		{
			name: "burst turn with effort and model in messages",
			te: &turnExecution{
				burst: []db.Message{
					{ID: "m1", Model: "model-alpha", Effort: "medium"},
				},
			},
			wantEffort: "medium",
			wantModel:  "model-alpha",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotEffort, gotModel := tc.te.turnEffortAndModel()
			if gotEffort != tc.wantEffort || gotModel != tc.wantModel {
				t.Errorf("turnEffortAndModel() = (%q, %q), want (%q, %q)", gotEffort, gotModel, tc.wantEffort, tc.wantModel)
			}
		})
	}
}

func TestTurnExecution_OwnsTurnItem(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		te     *turnExecution
		itemID string
		want   bool
	}{
		{
			name:   "nil receiver",
			te:     nil,
			itemID: "item-1",
			want:   false,
		},
		{
			name:   "dispatch matching id",
			te:     &turnExecution{dispatch: &db.Dispatch{ID: "d-123"}},
			itemID: "d-123",
			want:   true,
		},
		{
			name:   "dispatch mismatching id",
			te:     &turnExecution{dispatch: &db.Dispatch{ID: "d-123"}},
			itemID: "d-999",
			want:   false,
		},
		{
			name:   "burst message matching id",
			te:     &turnExecution{burst: []db.Message{{ID: "m-100"}, {ID: "m-101"}}},
			itemID: "m-101",
			want:   true,
		},
		{
			name:   "burst message mismatching id",
			te:     &turnExecution{burst: []db.Message{{ID: "m-100"}}},
			itemID: "m-200",
			want:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.ownsTurnItem(tc.itemID); got != tc.want {
				t.Errorf("ownsTurnItem(%q) = %t, want %t", tc.itemID, got, tc.want)
			}
		})
	}
}

func TestTurnExecution_IncrementTurnRetries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		isDispatch    bool
		isNil         bool
		wantDispRetry int
		wantMsgRetry  int
	}{
		{
			name:       "nil receiver",
			isNil:      true,
			isDispatch: false,
		},
		{
			name:          "dispatch turn increments dispatch retry in store",
			isDispatch:    true,
			wantDispRetry: 1,
		},
		{
			name:         "burst turn increments message retry in store",
			isDispatch:   false,
			wantMsgRetry: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.isNil {
				var nilTE *turnExecution
				nilTE.incrementTurnRetries("nil error")
				return
			}
			store := dbtest.NewFakeStore()
			ctx := context.Background()
			pool := New(config.NewFromData(&config.ConfigData{}), WorkerPoolConfig{Store: store})
			te := &turnExecution{pool: pool}
			if tc.isDispatch {
				d := db.Dispatch{ID: "d-retry-1", Status: db.StatusProcessing}
				_ = store.InsertDispatch(ctx, d)
				te.dispatch = &d
			} else {
				m := db.Message{ID: "m-retry-1", ThreadID: "t-1", Status: db.StatusProcessing}
				_ = store.InsertMessage(ctx, m)
				te.burst = []db.Message{m}
			}
			te.incrementTurnRetries("timeout error")
			if tc.isDispatch {
				gotD, _ := store.GetDispatch(ctx, "d-retry-1")
				if gotD != nil && gotD.RetryCount != tc.wantDispRetry {
					t.Errorf("dispatch retry count = %d, want %d", gotD.RetryCount, tc.wantDispRetry)
				}
			} else {
				gotM, _ := store.GetMessage(ctx, "m-retry-1")
				if gotM != nil && gotM.RetryCount != tc.wantMsgRetry {
					t.Errorf("message retry count = %d, want %d", gotM.RetryCount, tc.wantMsgRetry)
				}
			}
		})
	}
}

func TestResolveBurstModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		burst []db.Message
		want  string
	}{
		{
			name:  "empty burst",
			burst: nil,
			want:  "",
		},
		{
			name: "first message with model",
			burst: []db.Message{
				{ID: "1", Model: "gemini-pro"},
				{ID: "2", Model: "gemini-flash"},
			},
			want: "gemini-pro",
		},
		{
			name: "second message has model",
			burst: []db.Message{
				{ID: "1"},
				{ID: "2", Model: " gemini-flash "},
			},
			want: "gemini-flash",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveBurstModel(tc.burst); got != tc.want {
				t.Errorf("resolveBurstModel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_PrepareAttemptPrompt(t *testing.T) {
	t.Parallel()
	sessUUID := "44444444-4444-4444-4444-444444444444"
	origPrompt := "Execute task"
	contPrompt := fmt.Sprintf(ContinuationPromptTemplate, origPrompt)

	tests := []struct {
		name          string
		attempt       int
		promptToSend  string
		lastErrDetail string
		lastStderr    string
		hasSessionID  bool
		hasOnDiskSess bool
		isDispatch    bool
		restartCount  int
		errorMessage  string
		burst         []db.Message
		wantPrompt    string
	}{
		{
			name:         "empty prompt defaults to turn prompt",
			attempt:      1,
			promptToSend: "",
			wantPrompt:   origPrompt,
		},
		{
			name:         "first attempt normal dispatch returns original prompt",
			attempt:      1,
			promptToSend: origPrompt,
			isDispatch:   true,
			wantPrompt:   origPrompt,
		},
		{
			name:          "attempt 2 with context deadline exceeded and on-disk session returns continuation template",
			attempt:       2,
			promptToSend:  origPrompt,
			lastErrDetail: "context deadline exceeded",
			hasSessionID:  true,
			hasOnDiskSess: true,
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted dispatch attempt 1 with on-disk session returns continuation template",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: true,
			isDispatch:    true,
			restartCount:  1,
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted discord burst attempt 1 with on-disk session returns continuation template",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: true,
			isDispatch:    false,
			burst:         []db.Message{{ID: "m1", RestartCount: 1}},
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted dispatch attempt 1 without on-disk session returns original prompt",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: false,
			isDispatch:    true,
			restartCount:  1,
			wantPrompt:    origPrompt,
		},
		{
			name:         "custom prompt to send preserved without modification",
			attempt:      1,
			promptToSend: YieldTrapResumePrompt,
			isDispatch:   true,
			restartCount: 1,
			wantPrompt:   YieldTrapResumePrompt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpHome := t.TempDir()
			tempData := t.TempDir()
			mgr := session.New(tmpHome, tempData)
			if tc.hasOnDiskSess {
				sessDir, _ := ensureTestSessionDir(mgr, sessUUID)
				_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755)
				_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("{\"step_index\":0}\n"), 0o644)
			}

			pool := &WorkerPool{sessionMgr: mgr}
			te := &turnExecution{
				pool:       pool,
				turnPrompt: origPrompt,
				burst:      tc.burst,
			}
			if tc.hasSessionID {
				te.currentSessionID = sessUUID
			}
			if tc.isDispatch {
				te.dispatch = &db.Dispatch{
					ID:           "disp-test-1",
					RestartCount: tc.restartCount,
					ErrorMessage: tc.errorMessage,
				}
			}

			got := te.prepareAttemptPrompt(tc.attempt, tc.promptToSend, tc.lastErrDetail, tc.lastStderr)
			if got != tc.wantPrompt {
				t.Errorf("prepareAttemptPrompt() = %q, want %q", got, tc.wantPrompt)
			}
		})
	}
}

func TestWorker_ProcessDispatch_StatefulEvictionAndRestartContinuation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                   string
		sessionID              string
		restartCount           int
		hasExistingTranscript  bool
		yieldTrap              bool
		wantStatus             string
		wantEvictedFromPool    bool
		wantSessionInStore     bool
		wantContinuationPrompt bool
		wantYieldTrapPrompt    bool
	}{
		{
			name:                   "stateful dispatch evicts active pool session while preserving conversation in store",
			sessionID:              "vanadis-chunk-1",
			restartCount:           0,
			hasExistingTranscript:  false,
			yieldTrap:              false,
			wantStatus:             db.StatusCompleted,
			wantEvictedFromPool:    true,
			wantSessionInStore:     true,
			wantContinuationPrompt: false,
			wantYieldTrapPrompt:    false,
		},
		{
			name:                   "restarted stateful dispatch sends continuation prompt and transitions to yield trap resume",
			sessionID:              "vanadis-chunk-2",
			restartCount:           1,
			hasExistingTranscript:  true,
			yieldTrap:              true,
			wantStatus:             db.StatusCompleted,
			wantEvictedFromPool:    true,
			wantSessionInStore:     true,
			wantContinuationPrompt: true,
			wantYieldTrapPrompt:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			ctx := context.Background()
			tmpHome := t.TempDir()
			tempData := t.TempDir()
			sessMgr := session.New(tmpHome, tempData)

			sessUUID := "33333333-3333-3333-3333-333333333333"
			if tc.hasExistingTranscript {
				sessDir, _ := ensureTestSessionDir(sessMgr, sessUUID)
				_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755)
				_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("{\"step_index\":0}\n"), 0o644)
				_ = store.SaveSessionID(ctx, tc.sessionID, sessUUID)
			}

			var mu sync.Mutex
			var promptsSeen []string
			var invCount atomic.Int32
			doneCh := make(chan struct{})

			origPrompt := "[VANADIS BUILD CHUNK EXECUTION] Chunk execution test"

			pool := NewWorkerPool(WorkerPoolConfig{
				Store:          store,
				SessionManager: sessMgr,
				TimeoutMinutes: 1,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					mu.Lock()
					promptsSeen = append(promptsSeen, prompt)
					mu.Unlock()

					inv := invCount.Add(1)
					if tc.yieldTrap && inv == 1 {
						return `{"conversation_id":"` + sessUUID + `","status":"SUCCESS","response":"Waiting for task to complete"}`, "terminating 1 background task(s)", 0, nil
					}
					defer close(doneCh)
					return `{"conversation_id":"` + sessUUID + `","status":"SUCCESS","response":"chunk executed successfully"}`, "", 0, nil
				},
			})
			defer pool.Stop()

			disp := db.Dispatch{
				ID:           uuid.New().String(),
				Source:       "vanadis",
				SessionID:    tc.sessionID,
				Prompt:       origPrompt,
				RestartCount: tc.restartCount,
				Status:       db.StatusPending,
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
			}
			_ = store.InsertDispatch(ctx, disp)
			pool.EnqueueDispatch(disp)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout waiting for dispatch execution")
			}

			waitPoolIdle(t, pool, 3*time.Second)
			pool.Stop()

			d, err := store.GetDispatch(ctx, disp.ID)
			if err != nil || d == nil {
				t.Fatalf("failed to fetch dispatch: %v", err)
			}
			if d.Status != tc.wantStatus {
				t.Fatalf("dispatch status = %s, want %s", d.Status, tc.wantStatus)
			}

			if tc.wantSessionInStore {
				gotSess, _ := store.GetSessionID(ctx, tc.sessionID)
				if gotSess != sessUUID {
					t.Errorf("store session ID = %q, want %q", gotSess, sessUUID)
				}
			}

			if tc.wantEvictedFromPool {
				if testPool, ok := pool.AgentPool().(*testRunnerAgentPool); ok {
					if testPool.HasSession(tc.sessionID) {
						t.Errorf("expected session %q to be evicted from active pool", tc.sessionID)
					}
				}
			}

			mu.Lock()
			seen := append([]string(nil), promptsSeen...)
			mu.Unlock()

			if tc.wantContinuationPrompt {
				if len(seen) == 0 || !strings.Contains(seen[0], "Your previous execution timed out or was interrupted while working") {
					t.Errorf("first attempt prompt does not contain continuation template, got:\n%s", seen[0])
				}
			}

			if tc.wantYieldTrapPrompt {
				if len(seen) < 2 || seen[1] != YieldTrapResumePrompt {
					t.Errorf("second attempt prompt = %q, want %q", seen[1], YieldTrapResumePrompt)
				}
			}
		})
	}
}
