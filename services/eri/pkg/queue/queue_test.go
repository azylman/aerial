package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

func TestWorkerPool_ConfigInjection(t *testing.T) {
	t.Parallel()
	appCfg := config.NewFromData(&config.ConfigData{
		Model:         "initial-model",
		SystemChannel: "initial-alerts",
		Channels:      map[string]config.ChannelPolicy{"default": {Mode: "threads"}},
	})
	pool := New(appCfg, WorkerPoolConfig{
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
	if pool == nil || pool.appCfg != appCfg || pool.appCfg.Current().Model != "initial-model" {
		t.Fatalf("unexpected pool config injection: %+v", pool)
	}
}

func TestWorkerPool_ParentContext(t *testing.T) {
	t.Parallel()
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	pool := New(nil, WorkerPoolConfig{ParentContext: parentCtx})
	defer pool.Stop()

	parentCancel()
	select {
	case <-pool.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("expected pool.ctx to be cancelled when parent context is cancelled")
	}
}

func mockJSONResponse(convID, responseText string) string {
	if convID == "" {
		convID = uuid.New().String()
	}
	payload, _ := json.Marshal(map[string]any{
		"conversation_id":  convID,
		"status":           "SUCCESS",
		"response":         responseText,
		"duration_seconds": 1.0,
		"num_turns":        1,
	})
	return string(payload)
}

func TestQueueLifecycleAndRetryMatrix(t *testing.T) {
	t.Parallel()
	t0 := time.Now().UTC().Add(-10 * time.Second)

	tests := []workerTestCase{
		{name: "SuccessLifecycleAndSessionSaving", useSessionMgr: true, maxAttempts: 3, messages: []db.Message{{ID: "msg-101", ThreadID: "thread-202", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Hello Aerial"}}, runnerSteps: []mockRunnerStep{{convID: "f1111111-2222-3333-4444-555555555555", response: "Clean output response", writeTranscript: true}}, wantStatus: db.StatusCompleted, wantSavedSession: "f1111111-2222-3333-4444-555555555555", wantDeliveredTexts: []string{"Clean output response"}, wantDeliveredChannels: []string{"thread-202"}},
		{name: "TransientRetryPreservesSession", initialSessionID: "b1111111-2222-3333-4444-555555555555", maxAttempts: 3, messages: []db.Message{{ID: "msg-retry-1", ThreadID: "thread-retry", Content: "Hello"}}, runnerSteps: []mockRunnerStep{{stderr: "Error 503: high demand unavailable", exitCode: 1, errMsg: "503"}, {stderr: "Error 503: high demand unavailable", exitCode: 1, errMsg: "503"}, {convID: "b1111111-2222-3333-4444-555555555555", response: "Success after retries!"}}, wantRunnerCalls: 3, checkRunnerCalls: true, wantPassedSessions: []string{"b1111111-2222-3333-4444-555555555555", "b1111111-2222-3333-4444-555555555555", "b1111111-2222-3333-4444-555555555555"}, wantStatus: db.StatusCompleted, wantRetryCount: 2},
		{name: "SessionCorruptionRecovery", useSessionMgr: true, initialSessionID: "a1111111-2222-3333-4444-555555555555", maxAttempts: 3, notifierReply: "I refreshed our conversation! 🔄", messages: []db.Message{{ID: "msg-corrupt-1", ThreadID: "thread-corrupt", GuildID: "guild-1", Content: "Hello"}}, runnerSteps: []mockRunnerStep{{stderr: "Error: failed to load conversation: session corrupted", exitCode: 1, errMsg: "corrupt"}, {convID: "d8b5e679-7425-40de-944b-e07fc1f90ae7", response: "Clean output after session reset", writeTranscript: true}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"a1111111-2222-3333-4444-555555555555", ""}, wantDeliveredTexts: []string{"I refreshed our conversation! 🔄", "Clean output after session reset"}, wantSavedSession: "d8b5e679-7425-40de-944b-e07fc1f90ae7", wantStatus: db.StatusCompleted},
		{name: "TotalExhaustion", maxAttempts: 3, messages: []db.Message{{ID: "msg-exhaust", ThreadID: "thread-exhaust", GuildID: "guild-1", Content: "Hello"}}, runnerSteps: []mockRunnerStep{{stderr: "status: unavailable 503", exitCode: 1, errMsg: "unavailable"}}, wantRunnerCalls: 3, checkRunnerCalls: true, wantStatus: db.StatusFailed, wantRetryCount: 3, wantDeliveredTexts: []string{notifier.ModelUnavailableMessage()}},
		{name: "PromptPreservedWithoutMemoryInjection", messages: []db.Message{{ID: "msg-mem-1", ThreadID: "thread-mem-1", AuthorID: "user-1", Content: "What port does the server run on?"}}, runnerSteps: []mockRunnerStep{{response: "Response text"}}, wantStatus: db.StatusCompleted, wantPromptContains: []string{"What port does the server run on?"}, wantPromptExcludes: []string{"<retrieved_memory>"}, wantDBContent: "What port does the server run on?"},
		{name: "EmptyResponseFailure", messages: []db.Message{{ID: "msg-empty-resp-1", ThreadID: "thread-empty-resp", AuthorID: "user-1", Content: "Empty response prompt"}}, runnerSteps: []mockRunnerStep{{response: ""}}, wantStatus: db.StatusFailed},
		{name: "BurstCoalescing", enqueueAsBurst: true, messages: []db.Message{{ID: "m-b-1", ThreadID: "thread-burst", AuthorName: "Alice", Content: "Hello from Alice", CreatedAt: t0}, {ID: "m-b-2", ThreadID: "thread-burst", AuthorName: "Bob", Content: "Hello from Bob", CreatedAt: t0.Add(3 * time.Second)}, {ID: "m-b-3", ThreadID: "thread-burst", AuthorName: "Charlie", Content: "Hello from Charlie", CreatedAt: t0.Add(6 * time.Second)}}, runnerSteps: []mockRunnerStep{{response: "Coalesced reply from agent"}}, wantStatus: db.StatusCompleted, wantDeliveredTexts: []string{"Coalesced reply from agent"}, wantPromptContains: []string{"<USER_REQUEST>", "[Multiple messages received in channel]", "--- Message 1 (by @Alice", "Hello from Alice", "--- Message 2 (by @Bob", "Hello from Bob", "--- Message 3 (by @Charlie", "Hello from Charlie"}},
		{name: "SkipDiscord_AllSourcesDelivered", messages: []db.Message{{ID: "msg-sched-1", ThreadID: "thread-scheduled-1", GuildID: "scheduled", AuthorID: "scheduler", AuthorName: "Scheduler", Content: "Scheduled routine prompt"}, {ID: "msg-http-1", ThreadID: "thread-http-1", AuthorID: "http-client", AuthorName: "HTTP Client", Content: "HTTP prompt"}, {ID: "msg-user-1", ThreadID: "thread-user-1", GuildID: "guild-100", AuthorID: "user-999", AuthorName: "Alice", Content: "User prompt"}}, runnerSteps: []mockRunnerStep{{response: "AI reply"}}, wantStatus: db.StatusCompleted, wantDeliveredChannels: []string{"thread-scheduled-1", "thread-http-1", "thread-user-1"}},
		{name: "TurnCountSessionPreservation", useSessionMgr: true, initialSessionID: "sess-channel-init-123", seedTurnCount: 8, policyMode: "channel", sequentialTurns: true, messages: []db.Message{{ID: "m-rot-1", ThreadID: "channel-rotation-test", Content: "<@aerial> Turn 9"}, {ID: "m-rot-2", ThreadID: "channel-rotation-test", Content: "<@aerial> Turn 10"}, {ID: "m-rot-3", ThreadID: "channel-rotation-test", Content: "<@aerial> Turn 11"}}, runnerSteps: []mockRunnerStep{{response: "OK response", writeTranscript: true}}, wantSavedSession: "sess-channel-init-123", wantTurnCount: 11, checkTurnCount: true, wantStatus: db.StatusCompleted},
		{name: "IdleTimeout_ChannelSession", useSessionMgr: true, initialSessionID: "sess-channel-old-999", seedTurnCount: 1, seedIdleAge: 25 * time.Hour, policyMode: "channel", sequentialTurns: true, messages: []db.Message{{ID: "m-idle-1", ThreadID: "channel-rotation-test", Content: "<@aerial> wake up"}, {ID: "m-idle-2", ThreadID: "channel-rotation-test", Content: "<@aerial> follow up"}}, runnerSteps: []mockRunnerStep{{response: "OK response", writeTranscript: true}}, wantPassedSessions: []string{"sess-channel-old-999", "sess-channel-old-999"}, wantSavedSession: "sess-channel-old-999", wantTurnCount: 3, checkTurnCount: true, wantStatus: db.StatusCompleted},
		{name: "IdleTimeout_ThreadSession", useSessionMgr: true, initialSessionID: "sess-thread-old-888", seedTurnCount: 1, seedIdleAge: 25 * time.Hour, policyMode: "thread", sequentialTurns: true, messages: []db.Message{{ID: "m-idle-t1", ThreadID: "thread-idle-rot-test", Content: "<@aerial> wake up"}, {ID: "m-idle-t2", ThreadID: "thread-idle-rot-test", Content: "<@aerial> follow up"}}, runnerSteps: []mockRunnerStep{{response: "OK response", writeTranscript: true}}, wantPassedSessions: []string{"sess-thread-old-888", "sess-thread-old-888"}, wantSavedSession: "sess-thread-old-888", wantTurnCount: 3, checkTurnCount: true, wantStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWorkerTestCase(t, tc)
		})
	}
}

func TestQueueMultiThreadConcurrencyAndSingleThreadFIFO(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var mu sync.Mutex
	var executionOrder []string
	var allDone sync.WaitGroup
	allDone.Add(3)
	a1Started, b1Finished := make(chan struct{}), make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
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
				close(a1Started)
				select {
				case <-b1Finished:
				case <-time.After(2 * time.Second):
					t.Error("timed out waiting for B1 while A1 active")
				}
			} else if strings.Contains(prompt, "B1") {
				defer close(b1Finished)
			}
			mu.Lock()
			executionOrder = append(executionOrder, key+"_end")
			mu.Unlock()
			return mockJSONResponse("f2222222-2222-3333-4444-555555555555", "OK"), "", 0, nil
		},
		DeliveryFunc: noopDelivery, TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) { allDone.Done() },
	})
	pool.Start()
	defer pool.Stop()

	msgA1, msgA2, msgB1 := db.Message{ID: "m-A1", ThreadID: "ThreadA", Content: "A1"}, db.Message{ID: "m-A2", ThreadID: "ThreadA", Content: "A2"}, db.Message{ID: "m-B1", ThreadID: "ThreadB", Content: "B1"}
	_ = insertMessage(store, msgA1)
	_ = insertMessage(store, msgA2)
	_ = insertMessage(store, msgB1)
	pool.Enqueue(msgA1)
	pool.Enqueue(msgB1)
	waitForCompletion(t, a1Started, 2*time.Second, "A1 start")
	pool.Enqueue(msgA2)
	allDone.Wait()

	mu.Lock()
	defer mu.Unlock()
	a1s, a1e, a2s, b1s := -1, -1, -1, -1
	for i, event := range executionOrder {
		switch event {
		case "A1_start":
			a1s = i
		case "A1_end":
			a1e = i
		case "A2_start":
			a2s = i
		case "B1_start":
			b1s = i
		}
	}
	if a1s > a1e || a1e > a2s {
		t.Errorf("Thread A not serialized FIFO: %v", executionOrder)
	}
	if b1s < 0 {
		t.Errorf("Thread B did not execute: %v", executionOrder)
	}
}

type recoverInterruptedCase struct {
	name              string
	seedMessages      []db.Message
	seedCompletedMsg  *db.Message
	appCfg            *config.Config
	withSess          bool
	maxAttempts       int
	wantCompletedIDs  []string
	wantMsgStatuses   map[string]string
	wantMinDeliveries int
}

func runRecoverInterruptedCase(t *testing.T, tc recoverInterruptedCase) {
	t.Helper()
	store := setupTestStore(t)
	for _, m := range tc.seedMessages {
		_ = insertMessage(store, m)
	}
	if tc.seedCompletedMsg != nil {
		_ = insertMessage(store, *tc.seedCompletedMsg)
	}

	var mu sync.Mutex
	var completedIDs, deliveredNotifs []string
	doneCh := make(chan struct{})
	if len(tc.wantCompletedIDs) == 0 {
		close(doneCh)
	}
	maxAttempts := tc.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	pool := New(tc.appCfg, WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: maxAttempts,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("e1111111-2222-3333-4444-555555555555", "OK"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredNotifs = append(deliveredNotifs, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			completedIDs = append(completedIDs, msg.ID)
			if len(completedIDs) == len(tc.wantCompletedIDs) && len(tc.wantCompletedIDs) > 0 {
				close(doneCh)
			}
			mu.Unlock()
		},
	})
	pool.Start()
	defer pool.Stop()
	if tc.withSess {
		pool.SetDiscordSession(&discordgo.Session{})
	}

	RecoverInterrupted(store, pool)
	waitForCompletion(t, doneCh, 3*time.Second, tc.name)

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(completedIDs, tc.wantCompletedIDs) {
		t.Fatalf("completedIDs = %v, want %v", completedIDs, tc.wantCompletedIDs)
	}
	for msgID, wantStatus := range tc.wantMsgStatuses {
		if m, err := getMessage(store, msgID); err != nil || m == nil || m.Status != wantStatus {
			t.Errorf("msg %s status=%v, want %s", msgID, m, wantStatus)
		}
	}
	if len(deliveredNotifs) < tc.wantMinDeliveries {
		t.Errorf("delivered count=%d, want >= %d", len(deliveredNotifs), tc.wantMinDeliveries)
	}
}

func TestRecoverInterrupted(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	RecoverInterrupted(nil, nil)
	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	RecoverInterrupted(closedStore, NewWorkerPool(WorkerPoolConfig{Store: closedStore}))

	tests := []recoverInterruptedCase{
		{name: "FIFORecoveryAndScheduleRuns", seedMessages: []db.Message{{ID: "m1", ThreadID: "th-same", Status: db.StatusPending, CreatedAt: now.Add(-10 * time.Minute)}, {ID: "m2", ThreadID: "th-same", Status: db.StatusProcessing, CreatedAt: now.Add(-5 * time.Minute), UpdatedAt: now.Add(-2 * time.Minute)}, {ID: "m3", ThreadID: "th-same", Status: db.StatusCompleted, CreatedAt: now.Add(-15 * time.Minute)}}, wantCompletedIDs: []string{"m1", "m2"}},
		{name: "PoisonPill_RetryAndRestartLimits", seedMessages: []db.Message{{ID: "m-retry-poison", ThreadID: "th-p1", AuthorID: "user-1", Content: "retry crash", Status: db.StatusProcessing, RetryCount: 3, CreatedAt: now.Add(-10 * time.Minute)}, {ID: "m-hard-restart", ThreadID: "th-p2", AuthorID: "http-client", Content: strings.Repeat("x", 80), Status: db.StatusProcessing, RestartCount: 5, CreatedAt: now.Add(-10 * time.Minute)}, {ID: "m-instant-crash", ThreadID: "th-p3", AuthorID: "user-2", Content: "instant crash", Status: db.StatusProcessing, RestartCount: 3, CreatedAt: now.Add(-10 * time.Minute), UpdatedAt: now.Add(-5 * time.Second)}, {ID: "m-long-running", ThreadID: "th-ok", AuthorID: "user-3", Content: "long task", Status: db.StatusProcessing, RestartCount: 3, CreatedAt: now.Add(-20 * time.Minute), UpdatedAt: now.Add(-40 * time.Second)}}, withSess: true, appCfg: config.NewFromData(&config.ConfigData{AgyBin: "agy-custom", APIKey: "key-custom"}), wantCompletedIDs: []string{"m-long-running"}, wantMsgStatuses: map[string]string{"m-retry-poison": db.StatusFailed, "m-hard-restart": db.StatusFailed, "m-instant-crash": db.StatusFailed}, wantMinDeliveries: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRecoverInterruptedCase(t, tc)
		})
	}
}

type queueChannelPolicyCase struct {
	name, ignoredTargetID, expectStatus, expectErrorMessage, expectResponseText string
	policy                                                                      config.ChannelPolicy
	seedSpamThread, nilOnMessageCompleted                                       bool
	classifierConfidence                                                        float64
	message                                                                     db.Message
	expectRunnerCalls, expectDeliveryCalls, expectTypingCalls                   int
}

func runQueueChannelPolicyCase(t *testing.T, tc queueChannelPolicyCase) {
	t.Helper()
	store := setupTestStore(t)
	var mu sync.Mutex
	var runnerCalls, deliveryCalls, typingCalls int
	doneCh := make(chan struct{})

	var cls *classifier.Classifier
	if tc.classifierConfidence > 0 {
		cls = classifier.New(nil)
		cls.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
			return fmt.Sprintf(`{"confidence": %.2f, "reason": "test classification"}`, tc.classifierConfidence), nil
		}
	}

	cfg := WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1, Classifier: cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse("", "HTTP prompt execution response"), "", 0, nil
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
		ResolveChannelPolicy: func(threadID, channelName string) config.ChannelPolicy {
			if tc.ignoredTargetID != "" && (threadID == tc.ignoredTargetID || channelName == "spam") {
				return config.ChannelPolicy{Mode: "ignore"}
			}
			return tc.policy
		},
	}
	if !tc.nilOnMessageCompleted {
		cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) { close(doneCh) }
	}
	pool := NewWorkerPool(cfg)
	if tc.seedSpamThread {
		dg := &discordgo.Session{State: discordgo.NewState()}
		_ = dg.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})
		_ = dg.State.ChannelAdd(&discordgo.Channel{ID: "parent-spam-id", GuildID: "guild-1", Name: "spam", Type: discordgo.ChannelTypeGuildText})
		_ = dg.State.ChannelAdd(&discordgo.Channel{ID: "thread-in-spam-id", GuildID: "guild-1", ParentID: "parent-spam-id", Name: "Spam Discussion Thread", Type: discordgo.ChannelTypeGuildPublicThread})
		pool.SetDiscordSession(dg)
	}

	msg := tc.message
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}
	_ = insertMessage(store, msg)

	if tc.nilOnMessageCompleted {
		pool.processBurst([]db.Message{msg})
	} else {
		pool.Start()
		defer pool.Stop()
		pool.Enqueue(msg)
		waitForCompletion(t, doneCh, 5*time.Second, tc.name)
	}

	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != tc.expectRunnerCalls || deliveryCalls != tc.expectDeliveryCalls || typingCalls != tc.expectTypingCalls {
		t.Errorf("calls mismatch: runner=%d (want %d), delivery=%d (want %d), typing=%d (want %d)", runnerCalls, tc.expectRunnerCalls, deliveryCalls, tc.expectDeliveryCalls, typingCalls, tc.expectTypingCalls)
	}
	savedMsg, err := getMessage(store, msg.ID)
	if err != nil || savedMsg == nil {
		t.Fatalf("getMessage: %v", err)
	}
	if savedMsg.Status != tc.expectStatus {
		t.Errorf("status=%s, want %s", savedMsg.Status, tc.expectStatus)
	}
	if tc.expectErrorMessage != "" && savedMsg.ErrorMessage != tc.expectErrorMessage {
		t.Errorf("err=%q, want %q", savedMsg.ErrorMessage, tc.expectErrorMessage)
	}
	if tc.expectResponseText != "" && savedMsg.ResponseText != tc.expectResponseText {
		t.Errorf("resp=%q, want %q", savedMsg.ResponseText, tc.expectResponseText)
	}
}

func TestQueueChannelPolicyIgnoredAndDefaultDenyMatrix(t *testing.T) {
	t.Parallel()
	tests := []queueChannelPolicyCase{
		{name: "IgnoredChannelPolicy", policy: config.ChannelPolicy{Mode: "threads"}, ignoredTargetID: "chan-ignored-123", message: db.Message{ID: "msg-ignored-1", ThreadID: "chan-ignored-123", Content: "Hello ignored room"}, expectStatus: db.StatusCompleted, expectErrorMessage: "[IGNORE]"},
		{name: "IgnoredChannelPolicy_NilCallback", policy: config.ChannelPolicy{Mode: "ignore"}, message: db.Message{ID: "msg-nil-cb-1", ThreadID: "chan-ignored-nil-cb", Content: "Testing nil callback safety"}, nilOnMessageCompleted: true, expectStatus: db.StatusCompleted},
		{name: "ThreadInheritsParentChannelPolicy", policy: config.ChannelPolicy{Mode: "threads"}, ignoredTargetID: "parent-spam-id", seedSpamThread: true, message: db.Message{ID: "msg-thread-spam-1", ThreadID: "thread-in-spam-id", Content: "Hello in thread in spam"}, expectStatus: db.StatusCompleted, expectErrorMessage: "[IGNORE]"},
		{name: "HTTPClient_NotDroppedByDefaultDenyIgnore", policy: config.ChannelPolicy{Mode: "ignore"}, message: db.Message{ID: "msg-http-prompt-1", ThreadID: "synthetic-http-thread-uuid", AuthorID: "http-client", Content: "Explain Kubernetes architecture"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted, expectResponseText: "HTTP prompt execution response"},
		{name: "Typing_ThreadsModeActiveTurn", policy: config.ChannelPolicy{Mode: "threads"}, message: db.Message{ID: "m-type-1", ThreadID: "th-active-1", Content: "Hello thread"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted},
		{name: "Typing_ChannelModeDirectMentionWake", policy: config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}, message: db.Message{ID: "m-type-2", ThreadID: "th-mention-yes", Content: "<@Aerial> help me"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted},
		{name: "Typing_ChannelModeAmbientClassifierWake", policy: config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: new(0.8)}, classifierConfidence: 0.95, message: db.Message{ID: "m-type-3", ThreadID: "th-ambient-wake", Content: "Let's change all of these to 8am"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted},
		{name: "Typing_ChannelModeAmbientNonWakeChatter", policy: config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: new(0.8)}, classifierConfidence: 0.10, message: db.Message{ID: "m-type-4", ThreadID: "th-ambient-drop", Content: "Just random human chat between people"}, expectStatus: db.StatusCompleted},
		{name: "Typing_IgnoredChannelPolicy", policy: config.ChannelPolicy{Mode: "ignore"}, message: db.Message{ID: "m-type-5", ThreadID: "th-ignored", Content: "Ignored message @Aerial"}, expectStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runQueueChannelPolicyCase(t, tc)
		})
	}
}

func TestProcessBurst_AmbientAndWakeMatrix(t *testing.T) {
	t.Parallel()
	defaultChannelPolicy := config.ChannelPolicy{
		Mode:                 "channel",
		AmbientWakeThreshold: new(0.80),
	}

	tests := []burstTestCase{
		{name: "PureAmbient", channelID: "chan-lounge", seedSession: true, classifierJSON: `{"confidence": 0.25, "reason": "casual chit-chat"}`, policy: defaultChannelPolicy, runnerResponse: "Should not run", messages: []db.Message{{ID: "msg-amb-1", AuthorName: "Alice", Content: "Hello everyone"}, {ID: "msg-amb-2", AuthorName: "Bob", Content: "Nice weather today"}}, wantRunnerCalls: 0, wantDeliveryCalls: 0, checkDeliveryCalls: true, wantMaxTypingCalls: 0, checkMaxTypingCalls: true, wantTurnCount: 0, wantMsgErrors: map[string]string{"msg-amb-1": `[AMBIENT score=0.25/0.80 reason="casual chit-chat"]`, "msg-amb-2": `[AMBIENT score=0.25/0.80 reason="casual chit-chat"]`}},
		{name: "Tier1Wake", channelID: "chan-lounge", seedSession: true, botUserID: "bot-aerial-id", policy: defaultChannelPolicy, runnerResponse: "I am Aerial, here to help!", messages: []db.Message{{ID: "msg-tier1-1", AuthorName: "Alice", Content: "<@bot-aerial-id> can you help me with this bug?"}}, wantRunnerCalls: 1, wantDeliveryCalls: 1, checkDeliveryCalls: true, wantDeliveredText: "I am Aerial, here to help!", wantMinTypingCalls: 1, wantTurnCount: 1, wantMsgResponses: map[string]string{"msg-tier1-1": "I am Aerial, here to help!"}},
		{name: "Tier2Wake", channelID: "chan-lounge", seedSession: true, classifierJSON: `{"confidence": 0.90, "reason": "user is asking for system health report"}`, policy: defaultChannelPolicy, runnerResponse: "All systems operational.", messages: []db.Message{{ID: "msg-tier2-1", AuthorName: "Alice", Content: "Does anyone know if all services are healthy?"}}, wantRunnerCalls: 1, wantDeliveryCalls: 1, checkDeliveryCalls: true, wantDeliveredText: "All systems operational.", wantMinTypingCalls: 1, wantTurnCount: 1},
		{name: "MixedBurst", channelID: "chan-lounge", seedSession: true, historyMessages: []HistoryMessage{{ID: "msg-mixed-1", AuthorName: "Alice", Role: "User", Content: "I had tacos for lunch today", CreatedAt: time.Now().UTC().Add(-2 * time.Minute)}}, classifierJSON: `{"confidence": 0.15, "reason": "unrelated lunch discussion"}`, policy: defaultChannelPolicy, runnerResponse: "Done deploying!", messages: []db.Message{{ID: "msg-mixed-1", AuthorName: "Alice", Content: "I had tacos for lunch today"}, {ID: "msg-mixed-2", AuthorName: "Bob", Content: "<@aerial> please deploy the backend"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptContains: []string{"<CHANNEL_HISTORY>", "I had tacos for lunch today", "please deploy the backend"}, wantMsgResponses: map[string]string{"msg-mixed-1": "Done deploying!", "msg-mixed-2": "Done deploying!"}},
		{name: "AmbientWakePromptDefault", channelID: "chan-aviation", classifierJSON: `{"confidence": 0.95, "reason": "matches aviation directive"}`, policy: defaultChannelPolicy, runnerResponse: "Aviation response", messages: []db.Message{{ID: "msg-aviation-1", AuthorName: "Pilot", Content: "What is the stall speed of a Cessna 172?"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantClassifierPromptContains: classifier.DefaultAmbientWakePrompt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBurstTestCase(t, tc)
		})
	}
}

func TestExtractMessageBody_Multiline(t *testing.T) {
	t.Parallel()
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

	extracted := db.ExtractMessageBody(prompt)
	expected := "First line of message\nSecond line of message\nThird line of message"
	if extracted != expected {
		t.Errorf("Expected multiline extraction:\n%q\ngot:\n%q", expected, extracted)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type resolveEffectiveChannelCase struct {
	name, wantEffName, wantThreadName, wantCachedName string
	nilSession, testInvalidate, wantIsThread          bool
	inputIDs, wantEffIDs                              []string
	cached                                            []*discordgo.Channel
	stateChannel, restChannel                         *discordgo.Channel
	concurrent                                        int
}

func testResolveEffectiveChannelConcurrent(t *testing.T, tc resolveEffectiveChannelCase) {
	t.Helper()
	var reqCount atomic.Int32
	reqEntered, releaseReq := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s, _ := discordgo.New("Bot fake-token")
	s.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		reqCount.Add(1)
		once.Do(func() { close(reqEntered) })
		<-releaseReq
		data, _ := json.Marshal(tc.restChannel)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(data)), Header: make(http.Header)}, nil
	})}
	id := tc.inputIDs[0]
	var doneWg sync.WaitGroup
	for range tc.concurrent {
		doneWg.Go(func() {
			effID, effName, isThread := ResolveEffectiveChannel(s, id)
			if effID != tc.wantEffIDs[0] || effName != tc.wantEffName || isThread != tc.wantIsThread {
				t.Errorf("ResolveEffectiveChannel(%q) = (%q, %q, %v), want (%q, %q, %v)", id, effID, effName, isThread, tc.wantEffIDs[0], tc.wantEffName, tc.wantIsThread)
			}
		})
	}
	<-reqEntered
	waitForSingleflightFollowers(t, "ResolveEffectiveChannel", tc.concurrent-1)
	close(releaseReq)
	doneWg.Wait()
	if count := reqCount.Load(); count != 1 {
		t.Errorf("expected 1 REST request due to singleflight, got %d", count)
	}
}

func testResolveEffectiveChannelSequential(t *testing.T, s *discordgo.Session, tc resolveEffectiveChannelCase) {
	t.Helper()
	for i, id := range tc.inputIDs {
		effID, effName, isThread := ResolveEffectiveChannel(s, id)
		if effID != tc.wantEffIDs[i] || effName != tc.wantEffName || isThread != tc.wantIsThread {
			t.Errorf("ResolveEffectiveChannel(%q) = (%q, %q, %v), want (%q, %q, %v)", id, effID, effName, isThread, tc.wantEffIDs[i], tc.wantEffName, tc.wantIsThread)
		}
		if tc.wantThreadName != "" {
			effID2, effName2, isThread2, resolvedThreadName := ResolveChannelAndThread(s, id)
			if effID2 != tc.wantEffIDs[i] || effName2 != tc.wantEffName || !isThread2 || resolvedThreadName != tc.wantThreadName {
				t.Errorf("ResolveChannelAndThread(%q) = (%q, %q, %v, %q), want (%q, %q, true, %q)", id, effID2, effName2, isThread2, resolvedThreadName, tc.wantEffIDs[i], tc.wantEffName, tc.wantThreadName)
			}
		}
	}
}

func runResolveEffectiveChannelCase(t *testing.T, tc resolveEffectiveChannelCase) {
	t.Helper()
	for _, ch := range tc.cached {
		CacheDiscordChannel(ch)
		defer InvalidateChannelCache(ch.ID)
	}
	for _, id := range tc.inputIDs {
		if len(tc.cached) == 0 && id != "" {
			InvalidateChannelCache(id)
			defer InvalidateChannelCache(id)
		}
	}
	var s *discordgo.Session
	if !tc.nilSession {
		s = &discordgo.Session{State: discordgo.NewState()}
		if tc.stateChannel != nil {
			_ = s.State.GuildAdd(&discordgo.Guild{ID: tc.stateChannel.GuildID})
			_ = s.State.ChannelAdd(tc.stateChannel)
		}
	}
	if tc.testInvalidate {
		id := tc.inputIDs[0]
		if snap, ok := GetCachedChannel(id); !ok || snap.Name != tc.wantEffName {
			t.Fatalf("expected channel %q in cache before invalidation", id)
		}
		InvalidateChannelCache(id)
		if _, ok := GetCachedChannel(id); ok {
			t.Errorf("expected channel %q removed from cache after InvalidateChannelCache", id)
		}
		return
	}
	if tc.restChannel != nil && tc.concurrent > 1 {
		testResolveEffectiveChannelConcurrent(t, tc)
	} else {
		testResolveEffectiveChannelSequential(t, s, tc)
	}
	if tc.wantCachedName != "" {
		if snap, ok := GetCachedChannel(tc.inputIDs[0]); !ok || snap.Name != tc.wantCachedName {
			t.Errorf("expected channel %q cached with name %q, got ok=%v snap=%+v", tc.inputIDs[0], tc.wantCachedName, ok, snap)
		}
	}
}

func TestResolveEffectiveChannel(t *testing.T) {
	tests := []resolveEffectiveChannelCase{
		{name: "NilSession", nilSession: true, inputIDs: []string{"", "uuid-test-123", "123456789012345678"}, wantEffIDs: []string{"", "uuid-test-123", "123456789012345678"}},
		{name: "SyntheticNonNumericID", inputIDs: []string{"http-client-session-1234", "abc-def-ghi", "1234-5678", "channel_name_test"}, wantEffIDs: []string{"http-client-session-1234", "abc-def-ghi", "1234-5678", "channel_name_test"}},
		{name: "NormalChannel", inputIDs: []string{"100200300400500601"}, cached: []*discordgo.Channel{{ID: "100200300400500601", Name: "general-chat", Type: discordgo.ChannelTypeGuildText}}, wantEffIDs: []string{"100200300400500601"}, wantEffName: "general-chat"},
		{name: "ThreadParent", inputIDs: []string{"100200300400500602"}, cached: []*discordgo.Channel{{ID: "100200300400500600", Name: "announcements", Type: discordgo.ChannelTypeGuildText}, {ID: "100200300400500602", Name: "v1.2-discussion", ParentID: "100200300400500600", Type: discordgo.ChannelTypeGuildPublicThread}}, wantEffIDs: []string{"100200300400500600"}, wantEffName: "announcements", wantIsThread: true, wantThreadName: "v1.2-discussion"},
		{name: "ThreadMissingParent", inputIDs: []string{"100200300400500603"}, cached: []*discordgo.Channel{{ID: "100200300400500603", Name: "isolated-thread", ParentID: "999888777666555444", Type: discordgo.ChannelTypeGuildPublicThread}}, wantEffIDs: []string{"999888777666555444"}, wantIsThread: true},
		{name: "InvalidateCache", inputIDs: []string{"100200300400500604"}, cached: []*discordgo.Channel{{ID: "100200300400500604", Name: "temporary-channel", Type: discordgo.ChannelTypeGuildText}}, testInvalidate: true, wantEffIDs: []string{"100200300400500604"}, wantEffName: "temporary-channel"},
		{name: "StateFallback", inputIDs: []string{"100200300400500605"}, stateChannel: &discordgo.Channel{ID: "100200300400500605", GuildID: "guild-1", Name: "state-channel", Type: discordgo.ChannelTypeGuildText}, wantEffIDs: []string{"100200300400500605"}, wantEffName: "state-channel", wantCachedName: "state-channel"},
		{name: "SingleFlightREST", inputIDs: []string{"100200300400500606"}, restChannel: &discordgo.Channel{ID: "100200300400500606", Name: "singleflight-rest-channel", Type: discordgo.ChannelTypeGuildText}, concurrent: 5, wantEffIDs: []string{"100200300400500606"}, wantEffName: "singleflight-rest-channel", wantCachedName: "singleflight-rest-channel"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runResolveEffectiveChannelCase(t, tc)
		})
	}
}

func TestProcessBurst_InstructionsAndBurstMatrix(t *testing.T) {
	tests := []burstTestCase{
		{name: "ChannelInstructionsInjection", channelID: "100200300400500701", channelInstructions: map[string]string{"dev.md": "Follow Go idioms and test thoroughly."}, cachedChannels: []*discordgo.Channel{{ID: "100200300400500701", Name: "dev", Type: discordgo.ChannelTypeGuildText}}, runnerResponse: "Success", messages: []db.Message{{ID: "msg-inject-1", Content: "Build the queue worker"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptPrefix: "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\nFollow Go idioms and test thoroughly.\n</CHANNEL_INSTRUCTIONS>", wantPromptContains: []string{"Build the queue worker"}},
		{name: "ChannelInstructionsThreadInheritance", channelID: "100200300400500703", channelInstructions: map[string]string{"dev.md": "Guidelines for dev channel discussions."}, cachedChannels: []*discordgo.Channel{{ID: "100200300400500702", Name: "dev", Type: discordgo.ChannelTypeGuildText}, {ID: "100200300400500703", Name: "feature-thread", ParentID: "100200300400500702", Type: discordgo.ChannelTypeGuildPublicThread}}, runnerResponse: "Success", messages: []db.Message{{ID: "msg-thread-inherit-1", Content: "Thread discussion prompt"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptPrefix: "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\nGuidelines for dev channel discussions.\n</CHANNEL_INSTRUCTIONS>"},
		{name: "NoDuplicationOnRetry", channelID: "100200300400500704", channelInstructions: map[string]string{"dev.md": "Channel instructions for retry test."}, cachedChannels: []*discordgo.Channel{{ID: "100200300400500704", Name: "dev", Type: discordgo.ChannelTypeGuildText}}, maxAttempts: 2, runnerSteps: []mockRunnerStep{{returnRawStdout: true, stderr: "Error 503: high demand unavailable", exitCode: 1}, {convID: "session-retry-123", response: "Success on retry"}}, messages: []db.Message{{ID: "msg-retry-1", Content: "Retry test prompt"}}, wantRunnerCalls: 2, wantTurnCount: 1, wantSingleInstructionTagPerPrompt: true},
		{name: "Tier1PreScan_SkipsClassifier", channelID: "chan-lounge", classifierJSON: `{"confidence": 0.0, "reason": "should not be called"}`, policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: new(0.80)}, runnerResponse: "Hello there!", messages: []db.Message{{ID: "msg-prescan-1", AuthorName: "Alice", Content: "just hanging out"}, {ID: "msg-prescan-2", AuthorName: "Bob", Content: "<@aerial> what's up?"}, {ID: "msg-prescan-3", AuthorName: "Charlie", Content: "lol"}}, wantClassifierCalls: 0, checkClassifierCalls: true, wantRunnerCalls: 1, wantTurnCount: 1},
		{name: "CoalescedAmbientBurst", channelID: "chan-lounge", classifierJSON: `{"confidence": 0.95, "reason": "urgent issue needing answer"}`, policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: new(0.80)}, runnerResponse: "I can help with that database error!", messages: []db.Message{{ID: "msg-coalesce-1", AuthorName: "Alice", Content: "Does anyone know why postgres crashed?"}, {ID: "msg-coalesce-2", AuthorName: "Bob", Content: "brb grabbing coffee"}}, wantClassifierCalls: 1, checkClassifierCalls: true, wantClassifierPromptContains: "<target_burst>", wantRunnerCalls: 1, wantTurnCount: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runBurstTestCase(t, tc)
		})
	}
}

func TestProcessBurst_SessionContinuityAndRotationMatrix(t *testing.T) {
	t.Parallel()

	histMessages := []HistoryMessage{
		{ID: "hist-1", AuthorName: "Alice", Role: "User", Content: "First historical message", CreatedAt: time.Now().UTC().Add(-15 * time.Minute)},
		{ID: "hist-2", AuthorName: "Bob", Role: "User", Content: "Second historical message", CreatedAt: time.Now().UTC().Add(-10 * time.Minute)},
		{ID: "hist-3", AuthorName: "Aerial", Role: "Assistant", Content: "Previous assistant response", CreatedAt: time.Now().UTC().Add(-5 * time.Minute)},
	}

	tests := []burstTestCase{
		{name: "GhostSessionRecovery", channelID: "chan-lounge", seedSessionID: "stale-ghost-uuid", seedTranscripts: []string{"a8b5e679-7425-40de-944b-e07fc1f90ae7"}, policy: config.ChannelPolicy{Mode: "channel"}, runnerSteps: []mockRunnerStep{{convID: "a8b5e679-7425-40de-944b-e07fc1f90ae7", response: "Hello! I am ready to help.", stderr: "warning: conversation \"stale-ghost-uuid\" not found\nStarting conversation update stream for a8b5e679-7425-40de-944b-e07fc1f90ae7\n"}}, messages: []db.Message{{ID: "msg-ghost-recovery-1", AuthorName: "Alice", Content: "<@aerial> are you awake?"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantSavedSession: "a8b5e679-7425-40de-944b-e07fc1f90ae7", checkSavedSession: true},
		{name: "MultiTurnContinuity_AfterRecovery", channelID: "chan-lounge", seedSessionID: "stale-ghost-uuid", seedTranscripts: []string{"a8b5e679-7425-40de-944b-e07fc1f90ae7"}, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "a8b5e679-7425-40de-944b-e07fc1f90ae7", response: "Clean reply", stderr: "warning: conversation \"stale-ghost-uuid\" not found\nStarting conversation update stream for a8b5e679-7425-40de-944b-e07fc1f90ae7\n"}}, messages: []db.Message{{ID: "msg-recov-turn-1", AuthorName: "Alice", Content: "<@aerial> hello"}, {ID: "msg-recov-turn-2", AuthorName: "Alice", Content: "<@aerial> what did I just say?"}}, wantRunnerCalls: 2, wantTurnCount: 2, wantPassedSessions: []string{"", "a8b5e679-7425-40de-944b-e07fc1f90ae7"}},
		{name: "ColdChannel_NoStubDirectory", channelID: "chan-cold-123", classifierJSON: `{"confidence": 0.05, "reason": "ambient chatter"}`, policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: new(0.80)}, messages: []db.Message{{ID: "msg-cold-ambient-1", AuthorName: "Alice", Content: "just saying hello to everyone in the room"}}, wantRunnerCalls: 0, wantTurnCount: 0, wantSavedSession: "", checkSavedSession: true, wantNoStubDir: true},
		{name: "Turn1ContextInjection", channelID: "chan-history", seedTranscripts: []string{"e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"}, historyMessages: histMessages, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", response: "Answering wake question!"}}, messages: []db.Message{{ID: "msg-hist-turn-1", AuthorName: "Charlie", Content: "<@aerial> turn 1 wake question"}, {ID: "msg-hist-turn-2", AuthorName: "Charlie", Content: "<@aerial> turn 2 follow-up question"}}, wantRunnerCalls: 2, wantTurnCount: 2, wantPassedSessions: []string{"", "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"}, wantTurnPromptContains: map[int][]string{0: {"<CHANNEL_HISTORY>", "First historical message", "Second historical message"}, 1: {"<CHANNEL_HISTORY>", "Previous assistant response"}}},
		{name: "ThreadsMode_WarmTurnSuppressesChannelHistory", channelID: "thread-history-suppress", seedTranscripts: []string{"e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"}, historyMessages: histMessages[:1], policy: config.ChannelPolicy{Mode: "threads"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", response: "Answering wake question!"}}, messages: []db.Message{{ID: "msg-thread-turn-1", AuthorName: "Charlie", Content: "Hey Aerial, turn 1 thread question"}, {ID: "msg-thread-turn-2", AuthorName: "Charlie", Content: "Hey Aerial, turn 2 follow-up question"}}, wantRunnerCalls: 2, wantTurnCount: 2, wantTurnPromptContains: map[int][]string{0: {"<CHANNEL_HISTORY>"}}, wantTurnPromptExcludes: map[int][]string{1: {"<CHANNEL_HISTORY>"}}},
		{name: "SessionRotation_ResetsToColdState", channelID: "chan-rotation-cold", seedTranscripts: []string{"c9b5e679-7425-40de-944b-e07fc1f90ae7"}, historyMessages: []HistoryMessage{{ID: "hist-boot-1", AuthorName: "Alice", Role: "User", Content: "Historical message before bootstrap", CreatedAt: time.Now().UTC().Add(-5 * time.Minute)}}, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", response: "Turn 1 answer"}, {convID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", response: "Subsequent turn answer"}}, messages: []db.Message{{ID: "msg-turn-1", AuthorName: "Alice", Content: "<@aerial> Turn 1"}, {ID: "msg-turn-2", AuthorName: "Alice", Content: "<@aerial> Turn 2"}, {ID: "msg-turn-3", AuthorName: "Alice", Content: "<@aerial> Turn 3"}}, wantRunnerCalls: 3, wantTurnCount: 3, wantSavedSession: "c9b5e679-7425-40de-944b-e07fc1f90ae7", checkSavedSession: true, wantPassedSessions: []string{"", "c9b5e679-7425-40de-944b-e07fc1f90ae7", "c9b5e679-7425-40de-944b-e07fc1f90ae7"}, wantTurnPromptContains: map[int][]string{2: {"<CHANNEL_HISTORY>"}}, wantTurnPromptExcludes: map[int][]string{0: {"<PREVIOUS_SESSION>"}, 2: {"<PREVIOUS_SESSION>"}}},
		{name: "SessionRotation_SeedsPreviousSessionID", channelID: "chan-rotation-seed-test", seedTranscripts: []string{"22222222-2222-2222-2222-222222222222"}, enableHistoryFetcher: true, maxAttempts: 1, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, rotateBeforeTurn: 3, runnerSteps: []mockRunnerStep{{convID: "11111111-1111-1111-1111-111111111111", response: "Hello from turn"}, {convID: "11111111-1111-1111-1111-111111111111", response: "Hello from turn"}, {convID: "22222222-2222-2222-2222-222222222222", response: "Hello from turn"}, {convID: "22222222-2222-2222-2222-222222222222", response: "Hello from turn"}}, messages: []db.Message{{ID: "msg-seed-1", AuthorName: "Alice", Content: "<@aerial> Turn 1"}, {ID: "msg-seed-2", AuthorName: "Alice", Content: "<@aerial> Turn 2"}, {ID: "msg-seed-3", AuthorName: "Alice", Content: "<@aerial> Turn 1 of Session 2"}, {ID: "msg-seed-4", AuthorName: "Alice", Content: "<@aerial> Turn 2 of Session 2"}}, wantRunnerCalls: 4, wantTurnCount: 2, wantSavedSession: "22222222-2222-2222-2222-222222222222", checkSavedSession: true, wantTurnPromptContains: map[int][]string{2: {"<PREVIOUS_SESSION>", "11111111-1111-1111-1111-111111111111"}}, wantTurnPromptExcludes: map[int][]string{0: {"<PREVIOUS_SESSION>"}, 1: {"<PREVIOUS_SESSION>"}, 2: {"transcript.jsonl"}, 3: {"<PREVIOUS_SESSION>"}}},
		{name: "Turn1Crash_DoesNotPersistGhostUUID", channelID: "chan-crash", maxAttempts: 1, policy: config.ChannelPolicy{Mode: "channel"}, runnerSteps: []mockRunnerStep{{stderr: "Starting conversation update stream for crash-uuid-999\nError: fatal process crash\n", exitCode: 1, errMsg: "exit status 1"}}, messages: []db.Message{{ID: "msg-crash-1", AuthorName: "Alice", Content: "<@aerial> please do something risky"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantFinalStatus: db.StatusFailed, wantSavedSession: "", checkSavedSession: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBurstTestCase(t, tc)
		})
	}
}

func TestWakeModeMentionAndClassifierAlertMatrix(t *testing.T) {
	t.Parallel()

	type tier1Case struct {
		authorID string
		content  string
		roles    []string
		want     bool
	}

	tests := []struct {
		name       string
		burst      *burstTestCase
		tier1Cases []tier1Case
	}{
		{name: "ClassifierParseErrorTriggersSystemAlert", burst: &burstTestCase{channelID: "chan-lounge", botUserID: "bot-999", classifierJSON: "```json\n{\"confidence\": 0.85}\n```", policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: new(0.80)}, messages: []db.Message{{ID: "msg-parse-err-1", AuthorName: "Alice", Content: "Hello is anybody there?"}}, wantRunnerCalls: 0, wantTurnCount: 0, wantAlertTitle: "Classifier JSON Parse Error", wantAlertChannel: config.DefaultConfigData().SystemChannel, wantAlertBodyContains: []string{"Parse Error", "Raw Output"}}},
		{name: "IsTier1Wake_WakeModeMention", tier1Cases: []tier1Case{
			{content: "Hey <@bot-12345> how are you?", want: true},
			{content: "<USER_REQUEST>\n- replying_to:\n    author: \"@aerial\"\n    content: \"previous response\"\n- content: What was that?\n</USER_REQUEST>", want: true},
			{content: "<USER_REQUEST>\n- mentions: [Aerial]\n- content: Hello there!\n</USER_REQUEST>", want: true},
			{content: "I think aerial is a great anime", want: false},
			{content: "I love gundam models", want: false},
			{content: "Hey <@&role-aerial-managed> check this out", roles: []string{"role-aerial-managed"}, want: true},
			{content: "Hey <@&role-aerial-managed> check this out", roles: []string{"role-unrelated"}, want: false},
			{content: "<USER_REQUEST>\n- mention_user_ids: [bot-12345 98765]\n- content: Hello bot!\n</USER_REQUEST>", want: true},
			{content: "<USER_REQUEST>\n- mention_role_ids: [role-aerial-managed]\n- content: Ping all robots\n</USER_REQUEST>", roles: []string{"role-aerial-managed"}, want: true},
			{content: "<USER_REQUEST>\n- content: Here is the role '<@&role-aerial-managed>' in quotes\n- mention_role_ids: []\n- mentions: [harperwallbanger]\n</USER_REQUEST>", roles: []string{"role-aerial-managed"}, want: false},
			{content: "<USER_REQUEST>\n- content: Did you see '<@bot-12345>' in code?\n- mention_user_ids: []\n- mentions: []\n</USER_REQUEST>", want: false},
			{content: "<USER_REQUEST>\n- replying_to:\n    author: \"@bob\"\n    content: \"Look at this aerial photo of the bridge\"\n- content: Nice shot!\n</USER_REQUEST>", want: false},
			{authorID: "http-client", content: "check system health", want: true},
		}},
		{name: "ProcessBurst_WakeModeMention_BypassClassifier", burst: &burstTestCase{channelID: "chan-lounge-mention", seedSession: true, classifierJSON: `{"confidence": 0.95, "reason": "classifier should not be called"}`, policy: config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}, runnerResponse: "Response!", messages: []db.Message{{ID: "msg-amb-1", AuthorName: "Alice", Content: "Just talking about aerial views and drones"}}, wantClassifierCalls: 0, checkClassifierCalls: true, wantRunnerCalls: 0, wantDeliveryCalls: 0, checkDeliveryCalls: true, wantTurnCount: 0}},
		{name: "ProcessBurst_WakeModeMention_DirectMentionWakes", burst: &burstTestCase{channelID: "chan-lounge-wake", seedSession: true, policy: config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}, runnerResponse: "Hello Alice!", preserveSessionInRunner: true, messages: []db.Message{{ID: "msg-mention-1", AuthorName: "Alice", Content: "<USER_REQUEST>\n- mentions: [Aerial]\n- content: @Aerial what is the plan today?\n</USER_REQUEST>"}}, wantRunnerCalls: 1, wantDeliveryCalls: 1, checkDeliveryCalls: true, wantDeliveredText: "Hello Alice!", wantTurnCount: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.burst != nil {
				runBurstTestCase(t, *tc.burst)
			}
			for _, c := range tc.tier1Cases {
				m := db.Message{AuthorID: c.authorID, Content: c.content}
				if got := isTier1Wake(m, "bot-12345", c.roles); got != c.want {
					t.Errorf("isTier1Wake(m=%+v, roles=%v) = %v, want %v", m, c.roles, got, c.want)
				}
			}
		})
	}
}

type workerPoolShutdownCase struct {
	name, runnerResponse, wantStatusAfterStop, wantDelivered string
	blockRunnerOnCtx, blockClassifierCtx, verifyRecovery     bool
	msg                                                      db.Message
}

func verifyShutdownRecovery(t *testing.T, store db.Store, msg db.Message) {
	t.Helper()
	newDoneCh := make(chan struct{})
	var finalDelivered string
	var mu sync.Mutex
	newPool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse("c3333333-2222-3333-4444-555555555555", "Successfully processed on new container!"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			finalDelivered = text
			mu.Unlock()
			return nil
		},
		TypingFunc:         noopTyping,
		OnMessageCompleted: func(m db.Message, finalStatus string) { close(newDoneCh) },
	})
	newPool.Start()
	defer newPool.Stop()
	RecoverInterrupted(store, newPool)
	waitForCompletion(t, newDoneCh, 5*time.Second, "recovered message")

	mu.Lock()
	if !strings.Contains(finalDelivered, "Successfully processed on new container!") {
		t.Errorf("finalDelivered = %q, want recovery text", finalDelivered)
	}
	mu.Unlock()

	msgFinal, err := getMessage(store, msg.ID)
	if err != nil || msgFinal == nil || msgFinal.Status != db.StatusCompleted || msgFinal.RestartCount != 1 || msgFinal.RetryCount != 0 {
		t.Errorf("final msg = %+v, want COMPLETED with 1 restart and 0 retries", msgFinal)
	}
}

func runWorkerPoolShutdownCase(t *testing.T, tc workerPoolShutdownCase) {
	t.Helper()
	store := setupTestStore(t)
	var mu sync.Mutex
	var deliveredMessages []string
	stageStarted, completedCh := make(chan struct{}), make(chan struct{})
	var once sync.Once

	var cls *classifier.Classifier
	if tc.blockClassifierCtx {
		cls = classifier.New(nil)
		cls.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
			once.Do(func() { close(stageStarted) })
			<-ctx.Done()
			return "", ctx.Err()
		}
	}

	cfg := WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3, Classifier: cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			once.Do(func() { close(stageStarted) })
			if tc.blockRunnerOnCtx {
				<-ctx.Done()
				return "", "process killed by signal", -1, ctx.Err()
			}
			return mockJSONResponse("c4444444-2222-3333-4444-555555555555", tc.runnerResponse), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredMessages = append(deliveredMessages, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: noopTyping,
		OnMessageCompleted: func(m db.Message, finalStatus string) {
			select {
			case <-completedCh:
			default:
				close(completedCh)
			}
		},
	}
	if tc.blockClassifierCtx {
		cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: new(0.75)}
		}
		tc.msg.Content = "hey did anyone see that movie?"
	}

	pool := NewWorkerPool(cfg)
	pool.Start()
	msg := tc.msg
	msg.Status, msg.CreatedAt, msg.UpdatedAt = db.StatusPending, time.Now().UTC(), time.Now().UTC()
	if err := insertMessage(store, msg); err != nil {
		t.Fatalf("InsertMessage failed: %v", err)
	}
	pool.Enqueue(msg)

	waitForCompletion(t, stageStarted, 3*time.Second, "stage started")
	if !tc.blockRunnerOnCtx && !tc.blockClassifierCtx {
		waitForCompletion(t, completedCh, 3*time.Second, "completed before stop")
	}
	pool.Stop()

	mu.Lock()
	if tc.wantDelivered == "" && len(deliveredMessages) > 0 {
		t.Errorf("expected 0 delivered, got %v", deliveredMessages)
	}
	if tc.wantDelivered != "" && (len(deliveredMessages) == 0 || deliveredMessages[0] != tc.wantDelivered) {
		t.Errorf("deliveredMessages = %v, want %q", deliveredMessages, tc.wantDelivered)
	}
	mu.Unlock()

	msgCheck, err := getMessage(store, msg.ID)
	if err != nil || msgCheck == nil || msgCheck.Status != tc.wantStatusAfterStop || msgCheck.RetryCount != 0 {
		t.Errorf("msgCheck = %+v, want status %q with 0 retries", msgCheck, tc.wantStatusAfterStop)
	}

	if tc.verifyRecovery {
		verifyShutdownRecovery(t, store, msg)
	}
}

func TestWorkerPoolShutdown_PreservationMatrix(t *testing.T) {
	t.Parallel()
	tests := []workerPoolShutdownCase{
		{name: "PreservesProcessingMessageWithoutApology", blockRunnerOnCtx: true, verifyRecovery: true, msg: db.Message{ID: "msg-shutdown-1", ThreadID: "thread-shutdown-1", GuildID: "guild-shutdown-1", AuthorID: "user-shutdown-1", AuthorName: "User", Content: "Hello during shutdown"}, wantStatusAfterStop: db.StatusProcessing},
		{name: "RunnerSuccessPreserved", runnerResponse: "Finished just before pool stopped", msg: db.Message{ID: "msg-success-shutdown-1", ThreadID: "thread-success-shutdown-1", GuildID: "guild-success-shutdown-1", AuthorID: "user-1", AuthorName: "User", Content: "Complete fast"}, wantStatusAfterStop: db.StatusCompleted, wantDelivered: "Finished just before pool stopped"},
		{name: "AmbientClassifierCancellationPreserved", blockClassifierCtx: true, verifyRecovery: true, msg: db.Message{ID: "msg-ambient-shutdown-1", ThreadID: "channel-shutdown-1", GuildID: "channel-shutdown-1", AuthorID: "user-1", AuthorName: "User", Content: "<@aerial> hey did anyone see that movie?"}, wantStatusAfterStop: db.StatusProcessing},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWorkerPoolShutdownCase(t, tc)
		})
	}
}

func waitPoolIdle(t *testing.T, pool *WorkerPool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pool.mu.Lock()
		busy := false
		for _, state := range pool.threadChs {
			if state != nil && (state.inFlight || len(state.ch) > 0 || state.activeEnqueuers > 0) {
				busy = true
				break
			}
		}
		pool.mu.Unlock()
		if !busy {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for worker pool to become idle")
}

func TestWorkerPool_HoldUntilLeaderAndRecovery_Isolation(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	orphanMsg := db.Message{ID: "msg-orphan", ThreadID: "th-1", Content: "orphan msg", Status: db.StatusProcessing, CreatedAt: now.Add(-10 * time.Second), UpdatedAt: now.Add(-10 * time.Second)}
	heldMsg := db.Message{ID: "msg-held", ThreadID: "th-2", Content: "held canary msg", Status: db.StatusPending, CreatedAt: now, UpdatedAt: now}
	if err := insertMessage(store, orphanMsg); err != nil {
		t.Fatalf("insert orphanMsg: %v", err)
	}
	if err := insertMessage(store, heldMsg); err != nil {
		t.Fatalf("insert heldMsg: %v", err)
	}

	orphanDisp := db.Dispatch{ID: "disp-orphan", Source: "vanadis", Prompt: "p1", Status: db.StatusProcessing, CreatedAt: now.Add(-45 * time.Second), UpdatedAt: now.Add(-45 * time.Second), RestartCount: DefaultMaxRestarts}
	heldDisp := db.Dispatch{ID: "disp-held", Source: "heartbeat", Prompt: "p2", Status: db.StatusPending, CreatedAt: now, UpdatedAt: now}
	if _, err := store.InsertDispatch(ctx, orphanDisp); err != nil {
		t.Fatalf("insert orphanDisp: %v", err)
	}
	if _, err := store.InsertDispatch(ctx, heldDisp); err != nil {
		t.Fatalf("insert heldDisp: %v", err)
	}

	var completedMsgs atomic.Int32
	msgDone := make(chan struct{})
	dispDone := make(chan struct{})
	var dispCount atomic.Int32

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			if strings.Contains(prompt, "p1") || strings.Contains(prompt, "p2") {
				if dispCount.Add(1) == 2 {
					close(dispDone)
				}
			}
			return mockJSONResponse("c5555555-2222-3333-4444-555555555555", "ok"), "", 0, nil
		},
		DeliveryFunc: noopDelivery,
		TypingFunc:   noopTyping,
		OnMessageCompleted: func(m db.Message, finalStatus string) {
			if completedMsgs.Add(1) == 2 {
				close(msgDone)
			}
		},
	})
	pool.HoldUntilLeader()
	pool.Start()
	defer pool.Stop()

	// Enqueue while leaderHold is active: neither message nor dispatch should spawn a worker yet.
	pool.Enqueue(heldMsg)
	pool.EnqueueDispatch(heldDisp)

	gotHeldMsg, _ := getMessage(store, heldMsg.ID)
	if gotHeldMsg.Status != db.StatusPending {
		t.Fatalf("expected heldMsg to stay PENDING during leaderHold, got %s", gotHeldMsg.Status)
	}

	// RecoverInterrupted resets PROCESSING orphans, releases leaderHold, and enqueues all PENDING items.
	RecoverInterrupted(store, pool)
	waitForCompletion(t, msgDone, 5*time.Second, "recovered messages")
	waitForCompletion(t, dispDone, 5*time.Second, "recovered dispatches")
	waitPoolIdle(t, pool, 3*time.Second)

	gotOrphanMsg, _ := getMessage(store, orphanMsg.ID)
	gotHeldMsgAfter, _ := getMessage(store, heldMsg.ID)
	if gotOrphanMsg.Status != db.StatusCompleted || gotOrphanMsg.RestartCount != 1 {
		t.Errorf("orphanMsg = %+v, want COMPLETED with RestartCount=1", gotOrphanMsg)
	}
	if gotHeldMsgAfter.Status != db.StatusCompleted || gotHeldMsgAfter.RestartCount != 0 {
		t.Errorf("heldMsg = %+v, want COMPLETED with RestartCount=0", gotHeldMsgAfter)
	}

	gotOrphanDisp, _ := store.GetDispatch(ctx, orphanDisp.ID)
	if gotOrphanDisp.Status != db.StatusCompleted || gotOrphanDisp.RestartCount != DefaultMaxRestarts+1 {
		t.Errorf("orphanDisp = %+v, want COMPLETED with RestartCount=%d", gotOrphanDisp, DefaultMaxRestarts+1)
	}
}

func TestQueue_WatchdogAndRecoveryMatrix(t *testing.T) {
	t.Parallel()

	tests := []workerTestCase{
		{name: "WatchdogInactivityRetryAndExhaustion", maxAttempts: 3, messages: []db.Message{{ID: "msg-watchdog-101", ThreadID: "thread-watchdog-202", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Run a long task"}}, runnerSteps: []mockRunnerStep{{stderr: "[watchdog] inactivity timeout exceeded (5m without output)", exitCode: -1, errMsg: "exit status 255"}}, wantRunnerCalls: 3, checkRunnerCalls: true, wantStatus: db.StatusFailed, wantErrorContains: "inactivity timeout exceeded", wantDeliveredChannels: []string{"thread-watchdog-202"}, wantDeliveredSubstrings: []string{"timed out"}},
		{name: "WatchdogInactivityRetryAndRecovery", maxAttempts: 3, initialSessionID: "recovery-uuid-505", seedTranscriptFiles: map[string]string{"recovery-uuid-505": "mock transcript data\n"}, messages: []db.Message{{ID: "msg-recovery-101", ThreadID: "thread-recovery-505", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Run a long task"}}, runnerSteps: []mockRunnerStep{{stderr: "[watchdog] inactivity timeout exceeded (5m without output)", exitCode: -1, errMsg: "exit status 255"}, {response: "Recovered successfully on retry!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantStatus: db.StatusCompleted, wantDeliveredChannels: []string{"thread-recovery-505"}, wantDeliveredSubstrings: []string{"Recovered successfully on retry!"}, wantAttemptPromptContains: map[int][]string{0: {"Run a long task"}, 1: {"timed out or was interrupted"}}},
		{name: "DefaultTimeoutMinutes_Is60", wantDefaultTimeout: 60},
		{name: "ColdStartWatchdogRecoveryAndContinuation", maxAttempts: 3, seedTranscriptFiles: map[string]string{"88888888-aaaa-4bbb-cccc-111122223333": "mock transcript data\n"}, messages: []db.Message{{ID: "msg-cold-101", ThreadID: "thread-cold-606", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Execute initial task cold"}}, runnerSteps: []mockRunnerStep{{stderr: "Starting conversation update stream for 88888888-aaaa-4bbb-cccc-111122223333\n[watchdog] inactivity timeout exceeded (5m without output)", exitCode: -1, errMsg: "exit status 255"}, {response: "Cold start recovered successfully on retry!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"", "88888888-aaaa-4bbb-cccc-111122223333"}, wantSavedSession: "88888888-aaaa-4bbb-cccc-111122223333", wantDeliveredChannels: []string{"thread-cold-606"}, wantDeliveredSubstrings: []string{"Cold start recovered successfully on retry!"}, wantAttemptPromptContains: map[int][]string{0: {"Execute initial task cold"}, 1: {"timed out or was interrupted"}}},
		{name: "ColdStartStreamJsonInitLatchingOnFailure", maxAttempts: 3, seedTranscriptFiles: map[string]string{"99999999-bbbb-4ccc-dddd-555566667777": "mock transcript data\n"}, messages: []db.Message{{ID: "msg-cold-stream-1", ThreadID: "thread-cold-stream-1", Content: "Please write the code for stream-json"}}, runnerSteps: []mockRunnerStep{{rawStdout: "{\"event\":\"init\",\"conversation_id\":\"99999999-bbbb-4ccc-dddd-555566667777\"}\n{\"event\":\"step_update\"}\n", stderr: "[watchdog] inactivity timeout exceeded (5m without output)\n", exitCode: -1, errMsg: "exit status 255"}, {response: "Stream-json cold start recovered!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"", "99999999-bbbb-4ccc-dddd-555566667777"}, wantSavedSession: "99999999-bbbb-4ccc-dddd-555566667777", wantDeliveredChannels: []string{"thread-cold-stream-1"}, wantDeliveredSubstrings: []string{"Stream-json cold start recovered!"}},
		{name: "ColdStartTransientRecoveryAndContinuation", maxAttempts: 3, seedTranscriptFiles: map[string]string{"80808080-bbbb-4ccc-dddd-444455556666": "mock transcript data\n"}, messages: []db.Message{{ID: "msg-transient-101", ThreadID: "thread-transient-707", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Run transient task cold"}}, runnerSteps: []mockRunnerStep{{stderr: "Starting conversation update stream for 80808080-bbbb-4ccc-dddd-444455556666\nError 503: high demand service unavailable", exitCode: 1, errMsg: "exit status 1"}, {response: "Transient error recovered on retry!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"", "80808080-bbbb-4ccc-dddd-444455556666"}, wantSavedSession: "80808080-bbbb-4ccc-dddd-444455556666", wantAttemptPromptContains: map[int][]string{1: {"Run transient task cold"}}, wantAttemptPromptExcludes: map[int][]string{1: {"timed out or was interrupted"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWorkerTestCase(t, tc)
		})
	}
}

func TestProcessBurst_TranscriptAndErrorRecoveryMatrix(t *testing.T) {
	t.Parallel()

	tests := []workerTestCase{
		{name: "EmptyStdout_TranscriptRecovery", maxAttempts: 3, initialSessionID: "sess-transcript-recovery-999", seedTranscriptFiles: map[string]string{"sess-transcript-recovery-999": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"{{TS0}}","content":"Finish it"}` + "\n" + `{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"{{TS1}}","content":"Finished the task with 100% coverage!"}` + "\n"}, messages: []db.Message{{ID: "msg-empty-1", ThreadID: "thread-empty-stdout-1", GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Finish it"}}, runnerSteps: []mockRunnerStep{{returnRawStdout: true}}, wantStatus: db.StatusCompleted, wantDeliveredSubstrings: []string{"Finished the task with 100% coverage!"}},
		{name: "StreamInterrupted_TranscriptRecovery", maxAttempts: 3, initialSessionID: "11111111-2222-4333-8444-555555555555", seedTranscriptFiles: map[string]string{"11111111-2222-4333-8444-555555555555": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"{{TS0}}","content":"Have the gang review"}` + "\n" + `{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"DONE","created_at":"{{TS1}}","content":"Error: The stream was interrupted. Please continue the task you were working on."}` + "\n" + `{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"{{TS2}}","content":"Here is the complete gang review with high consensus!"}` + "\n"}, messages: []db.Message{{ID: "msg-interrupted-1", ThreadID: "thread-interrupted-1", GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Have the gang review"}}, runnerSteps: []mockRunnerStep{{rawStdout: `{"event":"init","conversation_id":"11111111-2222-4333-8444-555555555555"}` + "\n" + `{"event":"result","status":"error","error":"The stream was interrupted. Please continue the task you were working on."}`}}, wantRunnerCalls: 1, checkRunnerCalls: true, wantStatus: db.StatusCompleted, wantDeliveredSubstrings: []string{"Here is the complete gang review with high consensus!"}},
		{name: "StreamInterrupted_NoResponse_RotatesSessionCorrupt", maxAttempts: 3, initialSessionID: "22222222-3333-4444-8555-666666666666", seedTranscriptFiles: map[string]string{"22222222-3333-4444-8555-666666666666": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-07T21:24:20Z","content":"Have the gang review"}` + "\n" + `{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"DONE","created_at":"2026-09-07T21:25:06Z","content":"Error: The stream was interrupted. Please continue the task you were working on."}` + "\n"}, messages: []db.Message{{ID: "msg-interrupted-corrupt-2", ThreadID: "thread-interrupted-corrupt-2", GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Have the gang review"}}, runnerSteps: []mockRunnerStep{{rawStdout: `{"event":"init","conversation_id":"22222222-3333-4444-8555-666666666666"}` + "\n" + `{"event":"result","status":"error","error":"The stream was interrupted. Please continue the task you were working on."}`}, {convID: "33333333-4444-4555-8666-777777777777", response: "Recovered on clean session!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"22222222-3333-4444-8555-666666666666", ""}, wantStatus: db.StatusCompleted},
		{name: "EmptyStdout_TransientRetryAndContinuation", maxAttempts: 3, initialSessionID: "sess-empty-retry-888", seedTranscriptFiles: map[string]string{"sess-empty-retry-888": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-06T12:00:00Z","content":"Finish it"}` + "\n"}, messages: []db.Message{{ID: "msg-empty-2", ThreadID: "thread-empty-retry-2", GuildID: "guild-2", AuthorID: "user-2", AuthorName: "User", Content: "Finish it"}}, runnerSteps: []mockRunnerStep{{returnRawStdout: true}, {response: "Recovered on attempt 2!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"sess-empty-retry-888", "sess-empty-retry-888"}, wantAttemptPromptContains: map[int][]string{1: {"Finish it"}}, wantAttemptPromptExcludes: map[int][]string{1: {"timed out or was interrupted"}}},
		{name: "GeneralFailure_PreservesSessionOnDisk", maxAttempts: 3, initialSessionID: "sess-general-fail-777", seedTranscriptFiles: map[string]string{"sess-general-fail-777": "existing transcript data\n"}, messages: []db.Message{{ID: "msg-fail-3", ThreadID: "thread-general-fail-3", GuildID: "guild-3", AuthorID: "user-3", AuthorName: "User", Content: "Finish it"}}, runnerSteps: []mockRunnerStep{{stderr: "fatal execution error: out of memory", exitCode: 1, errMsg: "exit status 1"}, {response: "Recovered from exit 1!"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantPassedSessions: []string{"sess-general-fail-777", "sess-general-fail-777"}, wantAttemptPromptContains: map[int][]string{1: {"Finish it"}}, wantAttemptPromptExcludes: map[int][]string{1: {"timed out or was interrupted"}}},
		{name: "WorkerPool_FullBuffer_NoEvictionZombieRace", maxAttempts: 1, idleTimeout: 20 * time.Millisecond, concurrentEnqueueCount: 150, runnerSteps: []mockRunnerStep{{response: "OK"}}},
		{name: "TransientError_RetainsOriginalPrompt", maxAttempts: 2, useSessionMgr: true, initialSessionID: "sess-transient-503", messages: []db.Message{{ID: "msg-503-1", ThreadID: "thread-503", GuildID: "guild-503", AuthorID: "user-503", AuthorName: "User", Content: "Original prompt text that must not be wiped"}}, runnerSteps: []mockRunnerStep{{stderr: "Error 503: Service Unavailable. High demand.", exitCode: 1, errMsg: "exit code 1"}, {response: "Recovered cleanly"}}, wantRunnerCalls: 2, checkRunnerCalls: true, wantAttemptPromptContains: map[int][]string{1: {"Original prompt text that must not be wiped"}}, wantAttemptPromptExcludes: map[int][]string{1: {"timed out or was interrupted"}}},
		{name: "ImageDeliveryAndSanitization", maxAttempts: 3, seedPNGImage: image.NewRGBA(image.Rect(0, 0, 10, 10)), seedPNGLevel: png.DefaultCompression, seedPNGFilename: "metrics_chart.png", messages: []db.Message{{ID: "msg-img-1", ThreadID: "thread-img-1", GuildID: "guild-img-1", AuthorID: "user-img-1", AuthorName: "User", Content: "Show me the metrics chart"}}, runnerSteps: []mockRunnerStep{{convID: "c2222222-2222-3333-4444-555555555555", response: "Telemetry Summary:\n\n![Daily Chart]({{IMAGE_PATH}})\n\nAll systems nominal."}}, wantAttachments: []*delivery.Attachment{{Filename: "metrics_chart.png"}}, wantDeliveredSubstrings: []string{"**Daily Chart**"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWorkerTestCase(t, tc)
		})
	}
}

type resolveBotRoleIDsCase struct {
	name, guildID, botID                           string
	checkNilSession, addGuild, testCooldownPruning bool
	guildRoles                                     []*discordgo.Role
	guildMembers                                   []*discordgo.Member
	fetchRoles, wantRoles                          []string
	fetchErr                                       error
	calls, concurrentCallers                       int
	wantFetchCalls                                 int32
}

func testResolveBotRoleIDsConcurrent(t *testing.T, s *discordgo.Session, tc resolveBotRoleIDsCase, wantJoined string, wantSorted []string, enteredCh, releaseCh chan struct{}) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(tc.concurrentCallers)
	for range tc.concurrentCallers {
		go func() {
			defer wg.Done()
			r := append([]string(nil), ResolveBotRoleIDs(s, tc.guildID, tc.botID)...)
			sort.Strings(r)
			if strings.Join(r, ",") != wantJoined {
				t.Errorf("Expected %v, got %v", wantSorted, r)
			}
		}()
	}
	<-enteredCh
	waitForSingleflightFollowers(t, "ResolveBotRoleIDs", tc.concurrentCallers-1)
	close(releaseCh)
	wg.Wait()
}

func runResolveBotRoleIDsCase(t *testing.T, tc resolveBotRoleIDsCase) {
	t.Helper()
	sfKey := fmt.Sprintf("bot_member:%s:%s", tc.guildID, tc.botID)
	restCooldownMu.Lock()
	delete(restCooldown, sfKey)
	restCooldownMu.Unlock()
	t.Cleanup(func() {
		restCooldownMu.Lock()
		delete(restCooldown, sfKey)
		restCooldownMu.Unlock()
	})

	if tc.checkNilSession && ResolveBotRoleIDs(nil, tc.guildID, tc.botID) != nil {
		t.Errorf("Expected nil for nil session")
	}
	var fetchCalls atomic.Int32
	enteredCh, releaseCh := make(chan struct{}), make(chan struct{})
	var once sync.Once
	guildMemberFetcher = func(sess *discordgo.Session, gID, uID string) (*discordgo.Member, error) {
		fetchCalls.Add(1)
		if tc.concurrentCallers > 0 {
			once.Do(func() { close(enteredCh) })
			<-releaseCh
		}
		if tc.fetchErr != nil {
			return nil, tc.fetchErr
		}
		if gID == tc.guildID && uID == tc.botID {
			return &discordgo.Member{GuildID: gID, User: &discordgo.User{ID: uID}, Roles: tc.fetchRoles}, nil
		}
		return nil, errors.New("not found")
	}

	s := &discordgo.Session{State: discordgo.NewState()}
	s.State.User = &discordgo.User{ID: tc.botID, Username: "Aerial"}
	if tc.addGuild {
		_ = s.State.GuildAdd(&discordgo.Guild{ID: tc.guildID, Roles: tc.guildRoles, Members: tc.guildMembers})
	}

	wantSorted := append([]string(nil), tc.wantRoles...)
	sort.Strings(wantSorted)
	wantJoined := strings.Join(wantSorted, ",")

	if tc.concurrentCallers > 0 {
		testResolveBotRoleIDsConcurrent(t, s, tc, wantJoined, wantSorted, enteredCh, releaseCh)
	} else {
		for i := range tc.calls {
			r := append([]string(nil), ResolveBotRoleIDs(s, tc.guildID, tc.botID)...)
			sort.Strings(r)
			if strings.Join(r, ",") != wantJoined {
				t.Fatalf("call %d expected %v, got %v", i+1, wantSorted, r)
			}
		}
	}

	if got := fetchCalls.Load(); got != tc.wantFetchCalls {
		t.Fatalf("fetchCalls = %d, want %d", got, tc.wantFetchCalls)
	}
	if tc.testCooldownPruning {
		for i := range 150 {
			setRESTCooldown(fmt.Sprintf("suite-key-%d", i), -time.Minute)
		}
		setRESTCooldown("suite-key-active", time.Minute)
		if !isRESTInCooldown("suite-key-active") || isRESTInCooldown("suite-key-expired") {
			t.Fatalf("unexpected cooldown state")
		}
	}
}

func TestResolveBotRoleIDsSuite(t *testing.T) {
	origFetcher := guildMemberFetcher
	defer func() { guildMemberFetcher = origFetcher }()
	if _, err := origFetcher(nil, "g", "u"); err == nil {
		t.Errorf("expected error from origFetcher(nil)")
	}
	if _, err := origFetcher(&discordgo.Session{}, "g", "u"); err == nil {
		t.Errorf("expected error from origFetcher(empty)")
	}

	tests := []resolveBotRoleIDsCase{
		{name: "NilSessionAndStateResolution", guildID: "guild-suite-1", botID: "bot-suite-1", checkNilSession: true, addGuild: true, guildRoles: []*discordgo.Role{{ID: "role-aerial-managed", Name: "Aerial", Managed: true}, {ID: "role-gundam-alt", Name: "Gundam"}, {ID: "role-unrelated", Name: "Member"}}, guildMembers: []*discordgo.Member{{User: &discordgo.User{ID: "bot-suite-1"}, Roles: []string{"role-assigned-1"}}}, calls: 1, wantRoles: []string{"role-aerial-managed", "role-assigned-1", "role-gundam-alt"}},
		{name: "RESTFallbackAndCaching", guildID: "guild-suite-rest", botID: "bot-suite-rest", addGuild: true, guildRoles: []*discordgo.Role{{ID: "role-aerial-managed", Name: "Aerial", Managed: true}}, fetchRoles: []string{"mock-role-robot-123"}, calls: 2, wantRoles: []string{"mock-role-robot-123", "role-aerial-managed"}, wantFetchCalls: 1},
		{name: "SingleflightDeduplication", guildID: "guild-suite-sf", botID: "bot-suite-sf", addGuild: true, fetchRoles: []string{"mock-role-robot-123"}, concurrentCallers: 10, wantRoles: []string{"mock-role-robot-123"}, wantFetchCalls: 1},
		{name: "CooldownAndUncachedGuild", guildID: "guild-suite-uncached", botID: "bot-suite-cd", fetchErr: errors.New("simulated 429 rate limit"), calls: 2, testCooldownPruning: true, wantFetchCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runResolveBotRoleIDsCase(t, tc)
		})
	}
}

type processBurstQuotaPauseCase struct {
	name, threadID, policyMode, runnerStderr, wantLastMsgStatus, wantLastMsgErrSub string
	useProcessBurst                                                                bool
	messages                                                                       []db.Message
	wantOneShotCount                                                               int
	wantPromptSub, wantDeliveredSub                                                []string
	wantMinRunAtDelay                                                              time.Duration
}

func runProcessBurstQuotaPauseCase(t *testing.T, tc processBurstQuotaPauseCase) {
	t.Helper()
	store := setupTestStore(t)
	var runnerCalls int
	var deliveredTexts []string
	var mu sync.Mutex
	doneCh := make(chan struct{}, len(tc.messages))
	haroSrv, getOneShots := newMockHaroOneShotServer(t)

	cfg := WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
		HaroURL: haroSrv.URL,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return "", tc.runnerStderr, 1, errors.New("exit 1")
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredTexts = append(deliveredTexts, text)
			mu.Unlock()
			return nil
		},
		OnMessageCompleted: func(m db.Message, status string) { doneCh <- struct{}{} },
	}
	if tc.policyMode != "" {
		cfg.ResolveChannelPolicy = func(cID, cName string) config.ChannelPolicy { return config.ChannelPolicy{Mode: tc.policyMode} }
	}
	pool := NewWorkerPool(cfg)
	pool.Start()
	defer pool.Stop()

	t0 := time.Now().UTC()
	msgs := make([]db.Message, len(tc.messages))
	for i, m := range tc.messages {
		m.ThreadID, m.Status, m.CreatedAt, m.UpdatedAt = tc.threadID, db.StatusPending, t0.Add(time.Duration(i*100)*time.Millisecond), t0.Add(time.Duration(i*100)*time.Millisecond)
		msgs[i] = m
		_ = insertMessage(store, m)
	}
	if tc.useProcessBurst {
		pool.processBurst(msgs)
	} else {
		pool.Enqueue(msgs[0])
		waitForCompletion(t, doneCh, 5*time.Second, tc.name)
	}

	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 1 {
		t.Fatalf("runnerCalls = %d, want 1", runnerCalls)
	}
	for _, sub := range tc.wantDeliveredSub {
		if len(deliveredTexts) == 0 || !strings.Contains(deliveredTexts[0], sub) {
			t.Errorf("deliveredTexts missing %q", sub)
		}
	}
	oneShots := getOneShots(tc.threadID)
	if len(oneShots) != tc.wantOneShotCount {
		t.Fatalf("oneShots len = %d, want %d", len(oneShots), tc.wantOneShotCount)
	}
	if tc.wantOneShotCount > 0 {
		for _, sub := range tc.wantPromptSub {
			if !strings.Contains(oneShots[0].Prompt, sub) {
				t.Errorf("oneShot prompt %q missing %q", oneShots[0].Prompt, sub)
			}
		}
		if tc.wantMinRunAtDelay > 0 && time.Until(oneShots[0].RunAt) < tc.wantMinRunAtDelay {
			t.Errorf("RunAt delay = %v, want >= %v", time.Until(oneShots[0].RunAt), tc.wantMinRunAtDelay)
		}
	}
	lastMsg, _ := getMessage(store, msgs[len(msgs)-1].ID)
	if lastMsg.Status != tc.wantLastMsgStatus {
		t.Errorf("lastMsg status = %s, want %s", lastMsg.Status, tc.wantLastMsgStatus)
	}
	if tc.wantLastMsgErrSub != "" && !strings.Contains(lastMsg.ErrorMessage, tc.wantLastMsgErrSub) {
		t.Errorf("lastMsg error = %q, want %q", lastMsg.ErrorMessage, tc.wantLastMsgErrSub)
	}
}

func TestProcessBurst_QuotaPause_Suite(t *testing.T) {
	t.Parallel()
	tests := []processBurstQuotaPauseCase{
		{name: "SchedulesOneShotAndNotifies", threadID: "chan-quota-suite-1", runnerStderr: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", messages: []db.Message{{ID: "msg-quota-suite-1", Content: "<@bot> can you analyze our code coverage?"}}, wantOneShotCount: 1, wantPromptSub: []string{"[QUOTA_RETRY]", "code coverage"}, wantMinRunAtDelay: 16 * time.Minute, wantDeliveredSub: []string{"16m 58s", "<t:", ":R>", "automatically scheduled a retry"}, wantLastMsgStatus: db.StatusFailed, wantLastMsgErrSub: "[QUOTA_PAUSED"},
		{name: "CircuitBreakerDoesNotReschedule", threadID: "chan-circuit-suite-1", runnerStderr: "Individual quota reached. Resets in 10m.", messages: []db.Message{{ID: "msg-circuit-suite-1", Content: "[QUOTA_RETRY] Previous failed question"}}, wantOneShotCount: 0, wantDeliveredSub: []string{"paused automated retries"}, wantLastMsgStatus: db.StatusFailed},
		{name: "TrailingBurstSuppression", threadID: "chan-burst-suppress-suite-1", policyMode: "channel", useProcessBurst: true, runnerStderr: "Individual quota reached. Resets in 16m58s.", messages: []db.Message{{ID: "msg-burst-suite-1", Content: "<@aerial> Question 1"}, {ID: "msg-burst-suite-2", Content: "<@aerial> Question 2"}}, wantOneShotCount: 1, wantLastMsgStatus: db.StatusFailed, wantLastMsgErrSub: "[QUOTA_PAUSED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessBurstQuotaPauseCase(t, tc)
		})
	}
}

func TestQueueWorker_PerScopeSerialization(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var mu sync.Mutex
	var activeWorkers, maxActiveWorkers int

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			activeWorkers++
			if activeWorkers > maxActiveWorkers {
				maxActiveWorkers = activeWorkers
			}
			mu.Unlock()
			runtime.Gosched()
			mu.Lock()
			activeWorkers--
			mu.Unlock()
			return mockJSONResponse("", "OK"), "", 0, nil
		},
		DeliveryFunc: noopDelivery, TypingFunc: noopTyping,
	})

	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		msg := db.Message{ID: fmt.Sprintf("msg-scope-%d", i), ThreadID: "thread-scope-test", Content: fmt.Sprintf("Msg %d", i)}
		_ = insertMessage(store, msg)
		wg.Add(1)
		go func(m db.Message) {
			defer wg.Done()
			pool.processBurst([]db.Message{m})
		}(msg)
	}
	wg.Wait()
	if maxActiveWorkers > 1 {
		t.Errorf("Expected per-scope worker serialization, found %d", maxActiveWorkers)
	}
}

func TestThreadColdStartSummarization_Integration(t *testing.T) {
	store := setupTestStore(t)
	for _, ch := range []*discordgo.Channel{
		{ID: "thread-100", ParentID: "channel-1", Type: discordgo.ChannelTypeGuildPublicThread},
		{ID: "channel-1", ParentID: "", Type: discordgo.ChannelTypeGuildText},
		{ID: "thread-cached", ParentID: "channel-1", Type: discordgo.ChannelTypeGuildPublicThread},
		{ID: "thread-fail-llm", ParentID: "channel-1", Type: discordgo.ChannelTypeGuildPublicThread},
	} {
		CacheDiscordChannel(ch)
	}
	_ = saveThreadSummary(store, "thread-cached", "<THREAD_SUMMARY>\n- Existing cached summary\n</THREAD_SUMMARY>", "msg-hist-1")

	var mu sync.Mutex
	var lastPromptReceived string
	var llmCallCount int
	doneCh := make(chan struct{}, 10)

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
		LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
			mu.Lock()
			llmCallCount++
			mu.Unlock()
			if strings.Contains(prompt, "fail-llm") {
				return "", errors.New("llm error")
			}
			return "<THREAD_SUMMARY>\n- Technical Decision: Architectural shift to SQLite\n</THREAD_SUMMARY>", nil
		},
		HistoryFetcher: func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			content := "Let's adopt SQLite for storage"
			if strings.Contains(channelID, "fail-llm") {
				content = "fail-llm in thread history"
			}
			return []HistoryMessage{{ID: "msg-hist-1", AuthorName: "Alice", Role: "User", Content: content, CreatedAt: time.Now().UTC().Add(-10 * time.Minute)}}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			lastPromptReceived = prompt
			mu.Unlock()
			return mockJSONResponse("b1b7fb78-d85d-4bd6-ba9e-330f1e30596f", "Done"), "Starting conversation update stream for b1b7fb78-d85d-4bd6-ba9e-330f1e30596f\n", 0, nil
		},
		DeliveryFunc: noopDelivery, TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) { doneCh <- struct{}{} },
	})
	pool.Start()
	defer pool.Stop()

	steps := []struct {
		name, wantPromptContains, wantPromptExcludes, wantSavedWatermark string
		msg                                                              db.Message
		wantLLMDelta                                                     int
	}{
		{name: "Thread Cold Start Summarizes History", msg: db.Message{ID: "m1", ThreadID: "thread-100", Content: "Hello thread"}, wantLLMDelta: 1, wantPromptContains: "<THREAD_SUMMARY>", wantSavedWatermark: "msg-hist-1"},
		{name: "Main Channel Does Not Summarize", msg: db.Message{ID: "m2", ThreadID: "channel-1", Content: "Hello main channel"}, wantPromptExcludes: "<THREAD_SUMMARY>"},
		{name: "Thread Cold Start Cache Watermark Hit", msg: db.Message{ID: "m3", ThreadID: "thread-cached", Content: "Cached test"}, wantPromptContains: "Existing cached summary"},
		{name: "Fallback on LLM Failure", msg: db.Message{ID: "m4", ThreadID: "thread-fail-llm", Content: "fail-llm request"}, wantLLMDelta: 1, wantPromptContains: "<CHANNEL_HISTORY>", wantPromptExcludes: "<THREAD_SUMMARY>"},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			mu.Lock()
			prevCalls := llmCallCount
			mu.Unlock()
			_ = insertMessage(store, st.msg)
			pool.Enqueue(st.msg)
			waitForCompletion(t, doneCh, 3*time.Second, st.name)

			mu.Lock()
			prompt, calls := lastPromptReceived, llmCallCount
			mu.Unlock()
			if calls-prevCalls != st.wantLLMDelta {
				t.Errorf("LLM call delta = %d, want %d", calls-prevCalls, st.wantLLMDelta)
			}
			if st.wantPromptContains != "" && !strings.Contains(prompt, st.wantPromptContains) {
				t.Errorf("prompt missing %q:\n%s", st.wantPromptContains, prompt)
			}
			if st.wantPromptExcludes != "" && strings.Contains(prompt, st.wantPromptExcludes) {
				t.Errorf("prompt should not contain %q:\n%s", st.wantPromptExcludes, prompt)
			}
			if st.wantSavedWatermark != "" {
				sum, lastMsgID, err := getThreadSummary(store, st.msg.ThreadID)
				if err != nil || sum == "" || lastMsgID != st.wantSavedWatermark {
					t.Errorf("saved summary/watermark = %q/%q (err=%v)", sum, lastMsgID, err)
				}
			}
		})
	}
}

type processBurstErrorClassificationCase struct {
	name, threadID, errStdout, errStderr, expectedStatus                                        string
	existingSession, seedPBFile, checkLatchedCleared, checkNoticeDelivered, prohibitUnavailable bool
	failUntilAttempt, errExitCode, expectedAttempts                                             int
}

func runProcessBurstErrorClassificationCase(t *testing.T, tt processBurstErrorClassificationCase) {
	t.Helper()
	tmpDir, store := t.TempDir(), setupTestStore(t)
	var validUUID string
	if tt.seedPBFile {
		validUUID = uuid.New().String()
		pbPath := filepath.Join(tmpDir, ".gemini", "antigravity", "conversations", validUUID+".pb")
		_ = os.MkdirAll(filepath.Dir(pbPath), 0o755)
		_ = os.WriteFile(pbPath, []byte("protobuf-data"), 0o644)
	}
	if tt.existingSession {
		_ = saveSessionID(store, tt.threadID, uuid.New().String())
	}

	var mu sync.Mutex
	attempts := 0
	var deliveredText string
	doneCh := make(chan struct{}, 1)

	pool := NewWorkerPool(WorkerPoolConfig{
		SessionManager: session.New(tmpDir, ""), Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if tt.failUntilAttempt == 0 || attempts < tt.failUntilAttempt {
				var err error
				if tt.errExitCode != 0 {
					err = fmt.Errorf("exit status %d", tt.errExitCode)
				}
				return tt.errStdout, tt.errStderr, tt.errExitCode, err
			}
			return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Success!"}`, validUUID), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc:         noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) { doneCh <- struct{}{} },
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{ID: "msg-" + tt.name, ThreadID: tt.threadID, Content: "prompt " + tt.name}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)
	waitForCompletion(t, doneCh, 5*time.Second, tt.name)

	mu.Lock()
	defer mu.Unlock()
	if attempts != tt.expectedAttempts {
		t.Errorf("attempts = %d, want %d", attempts, tt.expectedAttempts)
	}
	dbMsg, _ := getMessage(store, msg.ID)
	if dbMsg.Status != tt.expectedStatus {
		t.Errorf("status = %s, want %s", dbMsg.Status, tt.expectedStatus)
	}
	if tt.checkLatchedCleared {
		if s, _ := getSessionID(store, tt.threadID); s != "" {
			t.Errorf("latched session not cleared: %q", s)
		}
	}
	if tt.checkNoticeDelivered && deliveredText == "" {
		t.Errorf("expected delivery notification")
	}
	if tt.prohibitUnavailable && strings.Contains(deliveredText, "temporarily unavailable") {
		t.Errorf("unexpected unavailable message: %s", deliveredText)
	}
}

func TestProcessBurst_ErrorClassificationMatrix(t *testing.T) {
	t.Parallel()
	tests := []processBurstErrorClassificationCase{
		{name: "ColdStart429_RetriesTransiently", threadID: "thread-cold-429", seedPBFile: true, failUntilAttempt: 3, errStderr: "HTTP 429: Resource has been exhausted (e.g. check quota)", errExitCode: 1, expectedAttempts: 3, expectedStatus: db.StatusCompleted},
		{name: "NonTransient_FailsFastOnAttempt1", threadID: "thread-fast-fail", existingSession: true, errStderr: "Error: invalid api key provided", errExitCode: 1, expectedAttempts: 1, expectedStatus: db.StatusFailed, checkLatchedCleared: true, checkNoticeDelivered: true, prohibitUnavailable: true},
		{name: "Exit0_NonTransientJSON_FailsFastOnAttempt1", threadID: "thread-flag-fail", errStdout: `{"status":"ERROR","error":"unknown flag: --unsupported-flag"}`, expectedAttempts: 1, expectedStatus: db.StatusFailed},
		{name: "UnknownError_RetriesTransientByDefault", threadID: "thread-unknown-retry", seedPBFile: true, failUntilAttempt: 3, errStderr: "some mysterious unexpected exit from child process", errExitCode: 1, expectedAttempts: 3, expectedStatus: db.StatusCompleted},
		{name: "ColdStartContextWindow_FailsFast", threadID: "thread-ctx-cold", errStdout: `{"status":"ERROR","error":"maximum context length exceeded"}`, expectedAttempts: 1, expectedStatus: db.StatusFailed},
		{name: "Cold429_ContextLengthExceeded_FailsFast", threadID: "thread-429", errStderr: "HTTP 429 Too Many Requests: context length exceeded", errExitCode: 1, expectedAttempts: 1, expectedStatus: db.StatusFailed},
		{name: "EmptyLatch_InvalidJSON_FailsFast", threadID: "thread-latch", errStdout: "{}", expectedAttempts: 3, expectedStatus: db.StatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runProcessBurstErrorClassificationCase(t, tt)
		})
	}
}

func TestWorkerPool_EffortRouting(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var executedModels []string
	var mu sync.Mutex
	doneCh := make(chan struct{}, 2)

	appCfg := config.NewFromData(&config.ConfigData{
		Model: "claude-opus-4-6-thinking", LowEffortModel: "claude-sonnet-4-6-thinking",
		Channels: map[string]config.ChannelPolicy{"default": {Mode: "threads"}},
	})
	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: session.New(t.TempDir(), ""), Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			executedModels = append(executedModels, model)
			mu.Unlock()
			return mockJSONResponse(sessionID, "Done!"), "", 0, nil
		},
		DeliveryFunc: noopDelivery, TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) { doneCh <- struct{}{} },
	})
	pool.Start()
	defer pool.Stop()

	cases := []struct{ runID, schedID, msgID, threadID, effort, wantModel string }{
		{"run-high-1", "cron-high", "msg-high-1", "thread-high", "high", "claude-opus-4-6-thinking"},
		{"run-low-1", "cron-low", "msg-low-1", "thread-low", "low", "claude-sonnet-4-6-thinking"},
	}
	for _, c := range cases {
		msg := db.Message{ID: c.msgID, ThreadID: c.threadID, AuthorID: "scheduler", AuthorName: "Scheduler", Content: "Run routine", Status: db.StatusPending, Effort: c.effort, CreatedAt: time.Now().UTC()}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)
		waitForCompletion(t, doneCh, 3*time.Second, c.msgID)
	}

	mu.Lock()
	models := append([]string{}, executedModels...)
	mu.Unlock()
	if len(models) != 2 || models[0] != cases[0].wantModel || models[1] != cases[1].wantModel {
		t.Fatalf("executedModels = %v, want [%s, %s]", models, cases[0].wantModel, cases[1].wantModel)
	}
}
