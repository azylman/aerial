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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestTurnExecution_GetRecentThreadMessages(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	mockDiscordMsgs := []*discordgo.Message{
		nil,
		{
			ID:        "1552732003076669003",
			ChannelID: "1552732003076669460",
			Content:   "",
			Timestamp: now.Add(-2 * time.Minute),
			Author:    &discordgo.User{ID: "bot-123", Username: "Aerial"},
			Components: []discordgo.MessageComponent{
				&discordgo.Container{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "### 🛠️ Vanadis Build: Widget Support"},
						&discordgo.Section{
							Components: []discordgo.MessageComponent{
								&discordgo.TextDisplay{Content: "✅ `aerial`: Build completed"},
							},
							Accessory: &discordgo.Button{
								Label: "PR #888 ↗",
								URL:   "https://github.com/azylman/aerial/pull/888",
							},
						},
					},
				},
			},
		},
		{ID: "1552732003076669002", ChannelID: "1552732003076669460", Content: "Can we add SVG?", Timestamp: now.Add(-5 * time.Minute), Author: &discordgo.User{ID: "100000000000000001", Username: "test-user"}, Mentions: []*discordgo.User{{ID: "bot-123", Username: "Aerial"}}, ReferencedMessage: &discordgo.Message{Author: &discordgo.User{Username: "Aerial"}}},
		{ID: "1552732003076669001", ChannelID: "1552732003076669460", Content: "Weather widget", Timestamp: now.Add(-10 * time.Minute), Author: &discordgo.User{ID: "100000000000000001", Username: "test-user"}},
		{ID: "1552732003076669000", ChannelID: "1552732003076669460", Content: "Webhook notification", WebhookID: "hook-123"},
		{ID: "1552732003076669099", ChannelID: "1552732003076669460", Content: "Current burst msg", Timestamp: now},
	}
	marshalMockMessages := func(msgs []*discordgo.Message) []byte {
		payload := make([]any, len(msgs))
		for i, m := range msgs {
			if m == nil {
				payload[i] = nil
				continue
			}
			payload[i] = struct {
				*discordgo.Message

				Components []discordgo.MessageComponent `json:"components,omitempty"`
			}{
				Message:    m,
				Components: m.Components,
			}
		}
		body, _ := json.Marshal(payload)
		return body
	}
	mockBody := marshalMockMessages(mockDiscordMsgs)
	dgSuccess, _ := discordgo.New("Bot test-token")
	dgSuccess.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(mockBody))}, nil
	})
	var flakyCalls atomic.Int32
	dgFlaky, _ := discordgo.New("Bot test-flaky")
	dgFlaky.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if flakyCalls.Add(1) == 1 {
			return nil, errors.New("transient 502")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(mockBody))}, nil
	})
	dgFail, _ := discordgo.New("Bot test")
	dgFail.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) { return nil, errors.New("simulated error") })

	tests := []struct {
		name, firstID, firstAuthor string
		wantLastContent            string
		dg                         *discordgo.Session
		burst                      []db.Message
		limit, wantCount           int
		wantErr                    bool
	}{
		{
			name:            "DiscordAPI_Success",
			dg:              dgSuccess,
			burst:           []db.Message{{ID: "1552732003076669099"}},
			limit:           10,
			wantCount:       4,
			firstID:         "1552732003076669000",
			firstAuthor:     "Webhook",
			wantLastContent: "### 🛠️ Vanadis Build: Widget Support\n✅ `aerial`: Build completed [PR #888 ↗](https://github.com/azylman/aerial/pull/888)",
		},
		{
			name:            "TransientError_RetriesAndSucceeds",
			dg:              dgFlaky,
			burst:           []db.Message{{ID: "1552732003076669099"}},
			limit:           0,
			wantCount:       4,
			firstID:         "1552732003076669000",
			firstAuthor:     "Webhook",
			wantLastContent: "### 🛠️ Vanadis Build: Widget Support\n✅ `aerial`: Build completed [PR #888 ↗](https://github.com/azylman/aerial/pull/888)",
		},
		{name: "PersistentError_ClampedLimit", dg: dgFail, limit: 200, wantErr: true},
		{name: "NilSession", limit: 10, wantCount: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := &WorkerPool{cfg: WorkerPoolConfig{DiscordSession: tc.dg}}
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
			if tc.wantLastContent != "" && msgs[len(msgs)-1].Content != tc.wantLastContent {
				t.Errorf("last message content mismatch: got %q, want %q", msgs[len(msgs)-1].Content, tc.wantLastContent)
			}
		})
	}
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

type sessionDBRotationCase struct {
	name, channelMode, runnerMode     string
	preWriteDBBytes, preWriteWALBytes int64
	maxAttempts, wantAttempts         int
}

func execSessionDBRotationStep(tc sessionDBRotationCase, sessionID, oldSess, convDir string, cur int) (string, string, int, error) {
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
}

func runSessionDBRotationCase(t *testing.T, tc sessionDBRotationCase) {
	t.Helper()
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
			return execSessionDBRotationStep(tc, sessionID, oldSess, convDir, cur)
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
}

func TestWorker_SessionDBRotation(t *testing.T) {
	t.Parallel()
	tests := []sessionDBRotationCase{
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
			runSessionDBRotationCase(t, tc)
		})
	}
}

type sessionRotationInjectCase struct {
	name              string
	maxAttempts       int
	triggerErr        string
	transientErr      bool
	wantAttempts      int
	wantRetainSession bool
}

func runSessionRotationInjectCase(t *testing.T, tt sessionRotationInjectCase) {
	t.Helper()
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
}

func TestWorkerPool_SessionRotationInjectsToolActions(t *testing.T) {
	t.Parallel()
	rotationTests := []sessionRotationInjectCase{
		{name: "EndToEnd_SessionRotationInjectsAndRetainsActions", maxAttempts: 3, triggerErr: "RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 2s.", wantAttempts: 3, wantRetainSession: true},
		{name: "EndToEnd_TransientRotationInjectsToolActions", maxAttempts: 2, triggerErr: "transient connection reset", transientErr: true, wantAttempts: 2, wantRetainSession: true},
	}
	for _, tt := range rotationTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runSessionRotationInjectCase(t, tt)
		})
	}
}

// buildTurnPromptCase is one buildTurnPrompt scenario; newBuildTurnPromptFixture turns it into a turnExecution.
type buildTurnPromptCase struct {
	name                                    string
	ambCfg                                  *config.AmbientContextConfig
	resolverErr                             error
	resolverText                            string
	isCold, threadSnapshot                  bool
	mode                                    string
	histMsgs                                []HistoryMessage
	fetchErr                                error
	cachedSum, cachedMsgID                  string
	wantResolverCalled, wantSummarizeThread bool
	wantPromptContains, wantPromptExcludes  []string
	wantSpans                               []wantPromptSpan
	wantAbsent                              []string
}

func newBuildTurnPromptFixture(t *testing.T, tc buildTurnPromptCase, threadID string, tp trace.TracerProvider, resolverCalled *atomic.Bool) *turnExecution {
	t.Helper()
	store := setupTestStore(t)
	if tc.cachedSum != "" {
		if err := store.SaveThreadSummary(context.Background(), threadID, tc.cachedSum, tc.cachedMsgID); err != nil {
			t.Fatalf("seed thread summary: %v", err)
		}
	}
	pool := &WorkerPool{
		ctx: context.Background(),
		cfg: WorkerPoolConfig{
			Store:          store,
			TracerProvider: tp,
			AmbientResolver: func(ctx context.Context, cfg *config.AmbientContextConfig) (string, error) {
				resolverCalled.Store(true)
				return tc.resolverText, tc.resolverErr
			},
			HistoryFetcher: func(ctx context.Context, channelID, beforeID string, limit int) ([]HistoryMessage, error) {
				return tc.histMsgs, tc.fetchErr
			},
			LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
				t.Errorf("summarizer called while building the prompt")
				return "", nil
			},
		},
	}
	return &turnExecution{
		pool: pool, effectiveName: "kitchen", threadID: threadID, currentSessionID: "sess-amb-1", isCold: tc.isCold,
		policy: config.ChannelPolicy{AmbientContext: tc.ambCfg, Mode: tc.mode},
		burst:  []db.Message{{ID: "m1", ThreadID: threadID, Content: "turn off kitchen lights"}},
	}
}

func TestBuildTurnPrompt(t *testing.T) {
	t.Parallel()
	ambCfg := &config.AmbientContextConfig{Template: "Kitchen Temp: {{.temp}}"}
	now := time.Now().UTC()
	twoMsgs := []HistoryMessage{
		{ID: "h-1", AuthorName: "Alex", Content: "plan the build", CreatedAt: now.Add(-time.Hour)},
		{ID: "h-2", AuthorName: "Alex", Content: "ship it", CreatedAt: now.Add(-time.Minute)},
	}
	summaryOut := "<THREAD_SUMMARY>Shipping the build</THREAD_SUMMARY>"
	tests := []buildTurnPromptCase{
		{
			name: "ConfiguredChannel", ambCfg: ambCfg, resolverText: "<ambient_context>\nKitchen Temp: 71F\n</ambient_context>", wantResolverCalled: true,
			wantPromptContains: []string{"<ambient_context>", "Kitchen Temp: 71F", "turn off kitchen lights"},
			wantSpans: []wantPromptSpan{
				{name: spanPrompt, attrs: []attribute.KeyValue{attrPromptColdStart.Bool(false)}},
				{name: spanPromptChannelSnapshot, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSnapshotOK.Bool(false)}},
				{name: spanPromptScopeInstr, parent: spanPrompt},
				{name: spanPromptAmbientContext, parent: spanPrompt},
			},
			wantAbsent: []string{spanPromptThreadSummary, spanPromptHistoryFetch, spanPromptPrevSessionID},
		},
		{
			name: "UnconfiguredChannel_Bypass", resolverText: "<ambient_context>should not appear</ambient_context>",
			wantPromptContains: []string{"turn off kitchen lights"}, wantPromptExcludes: []string{"<ambient_context>"},
			wantSpans:  []wantPromptSpan{{name: spanPromptScopeInstr, parent: spanPrompt}},
			wantAbsent: []string{spanPromptAmbientContext},
		},
		{
			name: "ResolverErrorTolerance", ambCfg: ambCfg, resolverErr: errors.New("mcp tool timed out"), wantResolverCalled: true,
			wantPromptContains: []string{"turn off kitchen lights"}, wantPromptExcludes: []string{"<ambient_context>"},
			wantSpans: []wantPromptSpan{{name: spanPromptAmbientContext, parent: spanPrompt, errored: true}},
		},
		{
			name: "ColdNewThreadRunsOnRawLookback", isCold: true, threadSnapshot: true, histMsgs: twoMsgs, wantSummarizeThread: true,
			wantPromptContains: []string{"ship it", "turn off kitchen lights"}, wantPromptExcludes: []string{"<THREAD_SUMMARY>"},
			wantSpans: []wantPromptSpan{
				{name: spanPrompt, attrs: []attribute.KeyValue{attrPromptColdStart.Bool(true)}},
				{name: spanPromptChannelSnapshot, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSnapshotOK.Bool(true)}},
				{name: spanPromptThreadSummary, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSummaryOutcome.String("no_history")}},
				{name: spanPromptSummaryLoad, parent: spanPromptThreadSummary},
				{name: spanPromptHistoryFetch, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptHistoryLimit.Int(10), attrPromptHistoryCount.Int(2)}},
				{name: spanPromptPrevSessionID, parent: spanPrompt},
				{name: spanPromptScopeInstr, parent: spanPrompt},
			},
			wantAbsent: []string{spanPromptAmbientContext, spanPromptPreviousSession, spanPromptSummaryLLM, spanPromptSummarySave},
		},
		{
			name: "ColdThreadCacheHit", isCold: true, threadSnapshot: true, histMsgs: twoMsgs, cachedSum: summaryOut, cachedMsgID: "h-2", wantSummarizeThread: true,
			wantPromptContains: []string{"Shipping the build", "ship it"},
			wantSpans: []wantPromptSpan{
				{name: spanPromptThreadSummary, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSummaryOutcome.String("cache_hit")}},
				{name: spanPromptHistoryFetch, parent: spanPromptThreadSummary, attrs: []attribute.KeyValue{attrPromptHistoryLimit.Int(10), attrPromptHistoryCount.Int(2)}},
			},
			wantAbsent: []string{spanPromptSummaryLLM, spanPromptSummarySave},
		},
		{
			name: "ColdThreadStaleSummaryStillInjected", isCold: true, threadSnapshot: true, histMsgs: twoMsgs, cachedSum: summaryOut, cachedMsgID: "h-0", wantSummarizeThread: true,
			wantPromptContains: []string{"Shipping the build", "ship it"},
			wantSpans: []wantPromptSpan{
				{name: spanPromptThreadSummary, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSummaryOutcome.String("stale")}},
			},
			wantAbsent: []string{spanPromptSummaryLLM, spanPromptSummarySave},
		},
		{
			name: "ColdThreadHistoryFetchFails", isCold: true, threadSnapshot: true, fetchErr: errors.New("discord 503"), cachedSum: summaryOut, cachedMsgID: "h-2", wantSummarizeThread: true,
			wantPromptContains: []string{"Shipping the build"},
			wantSpans: []wantPromptSpan{
				{name: spanPromptThreadSummary, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSummaryOutcome.String("stale")}},
				{name: spanPromptHistoryFetch, parent: spanPromptThreadSummary, errored: true, attrs: []attribute.KeyValue{attrPromptHistoryLimit.Int(10), attrPromptHistoryCount.Int(0)}},
			},
			wantAbsent: []string{spanPromptSummaryLLM},
		},
		{
			name: "WarmThreadMarksSummarizeThread", threadSnapshot: true, histMsgs: twoMsgs, cachedSum: summaryOut, cachedMsgID: "h-2", wantSummarizeThread: true,
			wantPromptContains: []string{"turn off kitchen lights"}, wantPromptExcludes: []string{"<THREAD_SUMMARY>", "ship it"},
			wantAbsent: []string{spanPromptThreadSummary, spanPromptHistoryFetch},
		},
		{
			name: "ChannelModeThreadDoesNotSummarize", isCold: true, threadSnapshot: true, mode: "channel", histMsgs: twoMsgs, cachedSum: summaryOut, cachedMsgID: "h-2",
			wantPromptContains: []string{"ship it"}, wantPromptExcludes: []string{"<THREAD_SUMMARY>"},
			wantAbsent: []string{spanPromptThreadSummary},
		},
		{
			name: "WarmChannelModeLookback", mode: "channel", histMsgs: twoMsgs[1:],
			wantPromptContains: []string{"ship it"},
			wantSpans: []wantPromptSpan{
				{name: spanPromptHistoryFetch, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptHistoryLimit.Int(10), attrPromptHistoryCount.Int(1)}},
			},
			wantAbsent: []string{spanPromptThreadSummary, spanPromptPrevSessionID},
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			threadID := fmt.Sprintf("t-btp-%d", i)
			if tc.threadSnapshot {
				CacheDiscordChannel(&discordgo.Channel{ID: threadID, ParentID: "p-" + threadID, Type: discordgo.ChannelTypeGuildPublicThread})
			}

			var resolverCalled, untracedResolverCalled atomic.Bool
			rec, tp, ctx, root := newPromptBuildRecorder()
			te := newBuildTurnPromptFixture(t, tc, threadID, tp, &resolverCalled)
			te.buildTurnPrompt(ctx)
			root.End()

			// Same inputs without a recording tracer must yield byte-identical prompt text.
			untraced := newBuildTurnPromptFixture(t, tc, threadID, nil, &untracedResolverCalled)
			untraced.buildTurnPrompt(context.Background())
			if te.turnPrompt != untraced.turnPrompt {
				t.Errorf("traced prompt differs from untraced prompt:\n%s\n---\n%s", te.turnPrompt, untraced.turnPrompt)
			}

			if te.summarizeThread != tc.wantSummarizeThread {
				t.Errorf("summarizeThread = %t, want %t", te.summarizeThread, tc.wantSummarizeThread)
			}
			if called := resolverCalled.Load(); called != tc.wantResolverCalled {
				t.Errorf("resolverCalled = %t, want %t", called, tc.wantResolverCalled)
			}
			for _, sub := range tc.wantPromptContains {
				if !strings.Contains(te.turnPrompt, sub) {
					t.Errorf("expected prompt containing %q", sub)
				}
			}
			for _, sub := range tc.wantPromptExcludes {
				if strings.Contains(te.turnPrompt, sub) {
					t.Errorf("expected prompt NOT containing %q", sub)
				}
			}
			assertPromptSpans(t, rec.Ended(), tc.wantSpans, tc.wantAbsent)
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

func TestResolveThreadColdStartSummary_LoadOnly(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	lookback := []HistoryMessage{
		{ID: "msg-9", Content: "older", CreatedAt: now.Add(-time.Hour)},
		{ID: "msg-10", Content: "newest pre-burst", CreatedAt: now},
	}
	saved := "<THREAD_SUMMARY>Saved</THREAD_SUMMARY>"

	tests := []struct {
		name             string
		cachedSum        string
		cachedMsgID      string
		histMsgs         []HistoryMessage
		wantSummary      string
		wantFetchLimits  []int
		wantOutcome      string
		wantSavedSummary string
		wantSavedMsgID   string
	}{
		{name: "watermark is latest pre-burst message", cachedSum: saved, cachedMsgID: "msg-10", histMsgs: lookback, wantSummary: saved, wantFetchLimits: []int{10}, wantOutcome: "cache_hit", wantSavedSummary: saved, wantSavedMsgID: "msg-10"},
		{name: "watermark inside lookback", cachedSum: saved, cachedMsgID: "msg-9", histMsgs: lookback, wantSummary: saved, wantFetchLimits: []int{10}, wantOutcome: "cache_hit", wantSavedSummary: saved, wantSavedMsgID: "msg-9"},
		{name: "watermark older than lookback is stale but injected", cachedSum: saved, cachedMsgID: "msg-1", histMsgs: lookback, wantSummary: saved, wantFetchLimits: []int{10}, wantOutcome: "stale", wantSavedSummary: saved, wantSavedMsgID: "msg-1"},
		{name: "empty lookback is stale but injected", cachedSum: saved, cachedMsgID: "msg-10", wantSummary: saved, wantFetchLimits: []int{10}, wantOutcome: "stale", wantSavedSummary: saved, wantSavedMsgID: "msg-10"},
		{name: "no saved summary skips the fetch", histMsgs: lookback, wantOutcome: "no_history"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var llmPrompts []string
			var fetchLimits []int

			store := setupTestStore(t)
			if tc.cachedSum != "" {
				_ = store.SaveThreadSummary(context.Background(), "th-test", tc.cachedSum, tc.cachedMsgID)
			}
			rec, tp, ctx, root := newPromptBuildRecorder()
			pool := &WorkerPool{ctx: context.Background(), cfg: WorkerPoolConfig{
				Store: store, TracerProvider: tp,
				LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
					mu.Lock()
					llmPrompts = append(llmPrompts, prompt)
					mu.Unlock()
					return "<THREAD_SUMMARY>unexpected</THREAD_SUMMARY>", nil
				},
			}}
			te := &turnExecution{pool: pool, threadID: "th-test"}

			got := te.resolveThreadColdStartSummary(ctx, func(_ context.Context, limit int) []HistoryMessage {
				fetchLimits = append(fetchLimits, limit)
				return tc.histMsgs
			})
			root.End()

			assertPromptSpans(t, rec.Ended(), []wantPromptSpan{
				{name: spanPromptThreadSummary, parent: spanPrompt, attrs: []attribute.KeyValue{attrPromptSummaryOutcome.String(tc.wantOutcome)}},
				{name: spanPromptSummaryLoad, parent: spanPromptThreadSummary},
			}, []string{spanPromptSummaryLLM, spanPromptSummarySave})
			if got != tc.wantSummary {
				t.Errorf("resolveThreadColdStartSummary() = %q, want %q", got, tc.wantSummary)
			}
			if !slices.Equal(fetchLimits, tc.wantFetchLimits) {
				t.Errorf("history fetch limits = %v, want %v", fetchLimits, tc.wantFetchLimits)
			}
			if len(llmPrompts) != 0 {
				t.Errorf("cold start sent %d summarizer prompts, want 0", len(llmPrompts))
			}
			sum, msgID, _ := store.GetThreadSummary(context.Background(), "th-test")
			if sum != tc.wantSavedSummary || msgID != tc.wantSavedMsgID {
				t.Errorf("stored summary = (%q, %q), want (%q, %q)", sum, msgID, tc.wantSavedSummary, tc.wantSavedMsgID)
			}
		})
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

// turnEndHistory returns thread messages with IDs first..last, one minute apart; even IDs are Assistant replies.
func turnEndHistory(base time.Time, first, last int) []HistoryMessage {
	msgs := make([]HistoryMessage, 0, last-first+1)
	for id := first; id <= last; id++ {
		role := "User"
		if id%2 == 0 {
			role = "Assistant"
		}
		msgs = append(msgs, HistoryMessage{ID: strconv.Itoa(id), Role: role, AuthorName: role, Content: fmt.Sprintf("content-%d", id), CreatedAt: base.Add(time.Duration(id-first) * time.Minute)})
	}
	return msgs
}

func TestRefreshThreadSummaryAtTurnEnd(t *testing.T) {
	t.Parallel()
	base := time.Now().UTC().Add(-time.Hour)
	prior, fresh := "<THREAD_SUMMARY>Prior</THREAD_SUMMARY>", "<THREAD_SUMMARY>Fresh</THREAD_SUMMARY>"
	window := turnEndHistory(base, 1000, 1008)

	tests := []turnEndRefreshCase{
		{name: "stale watermark refreshes with prior summary", cachedSum: prior, cachedMsgID: "1001", histMsgs: window, burstID: "1007", turnStatus: "success", wantOutcome: "refreshed", wantLLMPrompts: 1, wantPromptContains: []string{"<prior_thread_summary>", "Prior", "<thread_origin_anchor>", "content-1008"}, wantSum: fresh, wantMsgID: "1008"},
		{name: "first summary sends whole window", histMsgs: window, burstID: "1007", turnStatus: "success", wantOutcome: "refreshed", wantLLMPrompts: 1, wantPromptContains: []string{"content-1000", "content-1008"}, wantPromptExcludes: []string{"<prior_thread_summary>"}, wantSum: fresh, wantMsgID: "1008"},
		{name: "watermark at delivered reply is cache hit", cachedSum: prior, cachedMsgID: "1008", histMsgs: window, burstID: "1007", turnStatus: "success", wantOutcome: "cache_hit", wantSum: prior, wantMsgID: "1008"},
		{name: "watermark among newest five is fresh enough", cachedSum: prior, cachedMsgID: "1004", histMsgs: window, burstID: "1007", turnStatus: "success", wantOutcome: "fresh_enough", wantSum: prior, wantMsgID: "1004"},
		{name: "llm error leaves stored summary untouched", cachedSum: prior, cachedMsgID: "1001", histMsgs: window, burstID: "1007", turnStatus: "success", llmErr: errors.New("ephemeral pool busy"), wantOutcome: "failed", wantLLMPrompts: 1, wantSum: prior, wantMsgID: "1001"},
		{name: "next burst user message excluded from watermark", cachedSum: prior, cachedMsgID: "1001", histMsgs: turnEndHistory(base, 1000, 1009), burstID: "1007", turnStatus: "success", wantOutcome: "refreshed", wantLLMPrompts: 1, wantPromptContains: []string{"content-1008"}, wantPromptExcludes: []string{"content-1009"}, wantSum: fresh, wantMsgID: "1008"},
		{name: "no history fetched", cachedSum: prior, cachedMsgID: "1001", burstID: "1007", turnStatus: "success", wantOutcome: "no_history", wantSum: prior, wantMsgID: "1001"},
		{name: "non-thread turn skipped", cachedSum: prior, cachedMsgID: "1001", histMsgs: window, burstID: "1007", turnStatus: "success", notThread: true, wantSum: prior, wantMsgID: "1001"},
		{name: "dispatch turn skipped", cachedSum: prior, cachedMsgID: "1001", histMsgs: window, burstID: "1007", turnStatus: "success", dispatch: true, wantSum: prior, wantMsgID: "1001"},
		{name: "ephemeral turn skipped", cachedSum: prior, cachedMsgID: "1001", histMsgs: window, burstID: "1007", turnStatus: "success", ephem: true, wantSum: prior, wantMsgID: "1001"},
		{name: "failed turn skipped", cachedSum: prior, cachedMsgID: "1001", histMsgs: window, burstID: "1007", turnStatus: "failed", wantSum: prior, wantMsgID: "1001"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTurnEndRefreshCase(t, tc, fresh)
		})
	}
}

// turnEndRefreshCase is one refreshThreadSummaryAtTurnEnd scenario.
type turnEndRefreshCase struct {
	name                       string
	cachedSum, cachedMsgID     string
	histMsgs                   []HistoryMessage
	burstID, turnStatus        string
	notThread, dispatch, ephem bool
	llmErr                     error
	wantOutcome                string // "" when the refresh is skipped and records no span
	wantLLMPrompts             int
	wantPromptContains         []string
	wantPromptExcludes         []string
	wantSum, wantMsgID         string
}

// runTurnEndRefreshCase runs refreshThreadSummaryAtTurnEnd against a fake store, history fetcher and
// LLM that answers llmOut, then checks the span outcome, recorded summarizer prompts and stored summary.
func runTurnEndRefreshCase(t *testing.T, tc turnEndRefreshCase, llmOut string) {
	t.Helper()
	var mu sync.Mutex
	var llmPrompts, fetchBeforeIDs []string
	store := setupTestStore(t)
	if tc.cachedSum != "" {
		_ = store.SaveThreadSummary(context.Background(), "th-end", tc.cachedSum, tc.cachedMsgID)
	}
	rec, tp, ctx, root := newPromptBuildRecorder()
	pool := &WorkerPool{ctx: context.Background(), cfg: WorkerPoolConfig{
		Store: store, TracerProvider: tp,
		HistoryFetcher: func(_ context.Context, _, beforeID string, _ int) ([]HistoryMessage, error) {
			mu.Lock()
			fetchBeforeIDs = append(fetchBeforeIDs, beforeID)
			mu.Unlock()
			return tc.histMsgs, nil
		},
		LLMFunc: func(_ context.Context, _, prompt string) (string, error) {
			mu.Lock()
			llmPrompts = append(llmPrompts, prompt)
			mu.Unlock()
			return llmOut, tc.llmErr
		},
	}}
	te := &turnExecution{
		pool: pool, threadID: "th-end", burst: []db.Message{{ID: tc.burstID, ThreadID: "th-end"}},
		turnStatus: tc.turnStatus, summarizeThread: !tc.notThread, isEphemeral: tc.ephem,
	}
	if tc.dispatch {
		te.dispatch = &db.Dispatch{ID: "d-1"}
	}
	te.refreshThreadSummaryAtTurnEnd(ctx)
	root.End()

	var wantSpans []wantPromptSpan
	var wantFetches []string
	absent := []string{spanTurnSummaryRefresh, spanPromptHistoryFetch}
	if tc.wantOutcome != "" {
		wantSpans = []wantPromptSpan{{name: spanTurnSummaryRefresh, parent: spanPrompt, errored: tc.llmErr != nil, attrs: []attribute.KeyValue{attrPromptSummaryOutcome.String(tc.wantOutcome)}}}
		wantFetches, absent = []string{""}, nil
	}
	assertPromptSpans(t, rec.Ended(), wantSpans, absent)
	if !slices.Equal(fetchBeforeIDs, wantFetches) {
		t.Errorf("history fetch beforeIDs = %q, want %q", fetchBeforeIDs, wantFetches)
	}
	if len(llmPrompts) != tc.wantLLMPrompts {
		t.Fatalf("summarizer prompts = %d, want %d", len(llmPrompts), tc.wantLLMPrompts)
	}
	for _, p := range llmPrompts {
		assertSubstrings(t, "summarizer prompt", p, tc.wantPromptContains, tc.wantPromptExcludes)
	}
	sum, msgID, _ := store.GetThreadSummary(context.Background(), "th-end")
	if sum != tc.wantSum || msgID != tc.wantMsgID {
		t.Errorf("stored summary = (%q, %q), want (%q, %q)", sum, msgID, tc.wantSum, tc.wantMsgID)
	}
}

func assertSubstrings(t *testing.T, label, got string, contains, excludes []string) {
	t.Helper()
	for _, sub := range contains {
		if !strings.Contains(got, sub) {
			t.Errorf("%s missing %q", label, sub)
		}
	}
	for _, sub := range excludes {
		if strings.Contains(got, sub) {
			t.Errorf("%s unexpectedly contains %q", label, sub)
		}
	}
}
