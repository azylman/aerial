package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

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
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	dgFail, _ := discordgo.New("Bot test")
	dgFail.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) { return nil, errors.New("simulated error") })

	fakeStore := dbtest.NewFakeStore()
	ctx := context.Background()
	_ = fakeStore.InsertMessage(ctx, db.Message{ID: "msg-1", ThreadID: "1552732003076669460", AuthorName: "test-user", Content: "Normal chat", CreatedAt: now.Add(-2 * time.Minute)})

	tests := []struct {
		name, firstID, firstAuthor string
		dg                         *discordgo.Session
		store                      db.Store
		burst                      []db.Message
		limit, wantCount           int
		wantErr                    bool
	}{
		{name: "DiscordAPI_Success", dg: dgSuccess, burst: []db.Message{{ID: "1552732003076669099"}}, limit: 10, wantCount: 3, firstID: "1552732003076669000", firstAuthor: "Webhook"},
		{name: "FallbackDB_Direct", dg: dgFail, store: fakeStore, limit: 0, wantCount: 1, firstID: "msg-1", firstAuthor: "test-user"},
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
	te := &turnExecution{pool: &WorkerPool{cfg: WorkerPoolConfig{Store: fakeStore}, pools: map[PoolKey]runner.AgentPool{{Ephemeral: false, Effort: "high"}: procPool}}, threadID: "thread-evict-1"}

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

func TestWorker_BurstHelpers_LowEffort(t *testing.T) {
	t.Parallel()
	burstTests := []struct {
		name       string
		burst      []db.Message
		wantModel  string
		wantEffort string
	}{
		{name: "NilBurst", burst: nil, wantModel: "", wantEffort: "high"},
		{name: "NormalBurst", burst: []db.Message{{ID: "m1", Effort: "high", Model: "gemini-3.1-pro"}, {ID: "m2"}}, wantModel: "gemini-3.1-pro", wantEffort: "high"},
		{name: "FlashBurst", burst: []db.Message{{ID: "m1"}, {ID: "m2", Model: "  gemini-4-flash  "}}, wantModel: "gemini-4-flash", wantEffort: "low"},
		{name: "LowEffortFieldBurst", burst: []db.Message{{ID: "m1", Effort: " LOW "}}, wantModel: "", wantEffort: "low"},
		{name: "MediumEffortFieldBurst", burst: []db.Message{{ID: "m1", Effort: " MEDIUM "}}, wantModel: "", wantEffort: "medium"},
		{name: "FlashMediumModelBurstNotLow", burst: []db.Message{{ID: "m1", Model: "gemini-3.8-flash-medium"}}, wantModel: "gemini-3.8-flash-medium", wantEffort: "medium"},
	}
	for _, tt := range burstTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractBurstModel(tt.burst); got != tt.wantModel {
				t.Errorf("extractBurstModel = %q, want %q", got, tt.wantModel)
			}
			if got := resolveBurstEffort(tt.burst); got != tt.wantEffort {
				t.Errorf("resolveBurstEffort = %q, want %q", got, tt.wantEffort)
			}
		})
	}
}
