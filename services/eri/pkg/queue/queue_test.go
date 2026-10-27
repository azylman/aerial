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
	"net/http/httptest"
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
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
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
	// Drain timeout defaults and custom overrides
	poolDef := NewWorkerPool(WorkerPoolConfig{})
	if poolDef.cfg.DrainTimeout != 10*time.Second {
		t.Errorf("expected default DrainTimeout 10s, got %v", poolDef.cfg.DrainTimeout)
	}
	poolCustom := NewWorkerPool(WorkerPoolConfig{DrainTimeout: 250 * time.Millisecond})
	if poolCustom.cfg.DrainTimeout != 250*time.Millisecond {
		t.Errorf("expected custom DrainTimeout 250ms, got %v", poolCustom.cfg.DrainTimeout)
	}
}

func mockJSONResponse(convID, responseText string) string {
	if convID == "" {
		convID = uuid.New().String()
	}
	payload, _ := json.Marshal(map[string]interface{}{
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
		{name: "Staleness_WithinDefault30mTTL", messageAge: 10 * time.Minute, messages: []db.Message{{ID: "msg-stale-1", ThreadID: "thread-stale-1", AuthorID: "user-1", Content: "Staleness test prompt"}}, runnerSteps: []mockRunnerStep{{response: "OK"}}, wantRunnerCalls: 1, checkRunnerCalls: true, wantStatus: db.StatusCompleted},
		{name: "Staleness_ExceedsDefault30mTTL", messageAge: 31 * time.Minute, messages: []db.Message{{ID: "msg-stale-2", ThreadID: "thread-stale-2", AuthorID: "user-1", Content: "Staleness test prompt"}}, runnerSteps: []mockRunnerStep{{response: "OK"}}, wantRunnerCalls: 0, checkRunnerCalls: true, wantStatus: db.StatusCompleted, wantResponseText: "[EXPIRED_STALE]"},
		{name: "Staleness_ExceedsCustom10mTTL", stalenessTTL: 10 * time.Minute, messageAge: 12 * time.Minute, messages: []db.Message{{ID: "msg-stale-3", ThreadID: "thread-stale-3", AuthorID: "user-1", Content: "Staleness test prompt"}}, runnerSteps: []mockRunnerStep{{response: "OK"}}, wantRunnerCalls: 0, checkRunnerCalls: true, wantStatus: db.StatusCompleted, wantResponseText: "[EXPIRED_STALE]"},
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

func TestRecoverInterrupted(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	RecoverInterrupted(nil, nil)
	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	RecoverInterrupted(closedStore, NewWorkerPool(WorkerPoolConfig{Store: closedStore}))

	tests := []struct {
		name              string
		seedMessages      []db.Message
		seedCompletedMsg  *db.Message
		appCfg            *config.Config
		withSess          bool
		maxAttempts       int
		stalenessTTL      time.Duration
		wantCompletedIDs  []string
		wantMsgStatuses   map[string]string
		wantMinDeliveries int
	}{
		{name: "FIFORecoveryAndScheduleRuns", seedMessages: []db.Message{{ID: "m1", ThreadID: "th-same", Status: db.StatusPending, CreatedAt: now.Add(-10 * time.Minute)}, {ID: "m2", ThreadID: "th-same", Status: db.StatusProcessing, CreatedAt: now.Add(-5 * time.Minute), UpdatedAt: now.Add(-2 * time.Minute)}, {ID: "m3", ThreadID: "th-same", Status: db.StatusCompleted, CreatedAt: now.Add(-15 * time.Minute)}}, wantCompletedIDs: []string{"m1", "m2"}},
		{name: "PoisonPill_RetryAndRestartLimits", seedMessages: []db.Message{{ID: "m-retry-poison", ThreadID: "th-p1", AuthorID: "user-1", Content: "retry crash", Status: db.StatusProcessing, RetryCount: 3, CreatedAt: now.Add(-10 * time.Minute)}, {ID: "m-hard-restart", ThreadID: "th-p2", AuthorID: "http-client", Content: strings.Repeat("x", 80), Status: db.StatusProcessing, RestartCount: 5, CreatedAt: now.Add(-10 * time.Minute)}, {ID: "m-instant-crash", ThreadID: "th-p3", AuthorID: "user-2", Content: "instant crash", Status: db.StatusProcessing, RestartCount: 3, CreatedAt: now.Add(-10 * time.Minute), UpdatedAt: now.Add(-5 * time.Second)}, {ID: "m-long-running", ThreadID: "th-ok", AuthorID: "user-3", Content: "long task", Status: db.StatusProcessing, RestartCount: 3, CreatedAt: now.Add(-20 * time.Minute), UpdatedAt: now.Add(-40 * time.Second)}}, withSess: true, appCfg: config.NewFromData(&config.ConfigData{AgyBin: "agy-custom", APIKey: "key-custom"}), wantCompletedIDs: []string{"m-long-running"}, wantMsgStatuses: map[string]string{"m-retry-poison": db.StatusFailed, "m-hard-restart": db.StatusFailed, "m-instant-crash": db.StatusFailed}, wantMinDeliveries: 2},
		{name: "Staleness_TTLAndSuperseded", seedMessages: []db.Message{{ID: "m-stale-pending", ThreadID: "th-stale", AuthorID: "user-1", Status: db.StatusPending, CreatedAt: now.Add(-40 * time.Minute)}, {ID: "m-superseded", ThreadID: "th-super", AuthorID: "user-2", Status: db.StatusPending, CreatedAt: now.Add(-20 * time.Minute)}, {ID: "m-http-retained", ThreadID: "th-super", AuthorID: "http-client", Status: db.StatusPending, CreatedAt: now.Add(-45 * time.Minute)}}, seedCompletedMsg: &db.Message{ID: "m-newer-completed", ThreadID: "th-super", Status: db.StatusCompleted, CreatedAt: now.Add(-10 * time.Minute)}, wantCompletedIDs: []string{"m-http-retained"}, wantMsgStatuses: map[string]string{"m-stale-pending": db.StatusCompleted, "m-superseded": db.StatusCompleted, "m-http-retained": db.StatusCompleted}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
				Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: maxAttempts, StalenessTTL: tc.stalenessTTL,
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
		})
	}
}

func ptrFloat(f float64) *float64 { return &f }

func TestQueueChannelPolicyIgnoredAndDefaultDenyMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, ignoredTargetID, expectStatus, expectErrorMessage, expectResponseText string
		policy                                                                      config.ChannelPolicy
		seedSpamThread, nilOnMessageCompleted                                       bool
		classifierConfidence                                                        float64
		message                                                                     db.Message
		expectRunnerCalls, expectDeliveryCalls, expectTypingCalls                   int
	}{
		{name: "IgnoredChannelPolicy", policy: config.ChannelPolicy{Mode: "threads"}, ignoredTargetID: "chan-ignored-123", message: db.Message{ID: "msg-ignored-1", ThreadID: "chan-ignored-123", Content: "Hello ignored room"}, expectStatus: db.StatusCompleted, expectErrorMessage: "[IGNORE]"},
		{name: "IgnoredChannelPolicy_NilCallback", policy: config.ChannelPolicy{Mode: "ignore"}, message: db.Message{ID: "msg-nil-cb-1", ThreadID: "chan-ignored-nil-cb", Content: "Testing nil callback safety"}, nilOnMessageCompleted: true, expectStatus: db.StatusCompleted},
		{name: "ThreadInheritsParentChannelPolicy", policy: config.ChannelPolicy{Mode: "threads"}, ignoredTargetID: "parent-spam-id", seedSpamThread: true, message: db.Message{ID: "msg-thread-spam-1", ThreadID: "thread-in-spam-id", Content: "Hello in thread in spam"}, expectStatus: db.StatusCompleted, expectErrorMessage: "[IGNORE]"},
		{name: "HTTPClient_NotDroppedByDefaultDenyIgnore", policy: config.ChannelPolicy{Mode: "ignore"}, message: db.Message{ID: "msg-http-prompt-1", ThreadID: "synthetic-http-thread-uuid", AuthorID: "http-client", Content: "Explain Kubernetes architecture"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted, expectResponseText: "HTTP prompt execution response"},
		{name: "Typing_ThreadsModeActiveTurn", policy: config.ChannelPolicy{Mode: "threads"}, message: db.Message{ID: "m-type-1", ThreadID: "th-active-1", Content: "Hello thread"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted},
		{name: "Typing_ChannelModeDirectMentionWake", policy: config.ChannelPolicy{Mode: "channel", WakeMode: "mention"}, message: db.Message{ID: "m-type-2", ThreadID: "th-mention-yes", Content: "<@Aerial> help me"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted},
		{name: "Typing_ChannelModeAmbientClassifierWake", policy: config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrFloat(0.8)}, classifierConfidence: 0.95, message: db.Message{ID: "m-type-3", ThreadID: "th-ambient-wake", Content: "Let's change all of these to 8am"}, expectRunnerCalls: 1, expectDeliveryCalls: 1, expectTypingCalls: 1, expectStatus: db.StatusCompleted},
		{name: "Typing_ChannelModeAmbientNonWakeChatter", policy: config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrFloat(0.8)}, classifierConfidence: 0.10, message: db.Message{ID: "m-type-4", ThreadID: "th-ambient-drop", Content: "Just random human chat between people"}, expectStatus: db.StatusCompleted},
		{name: "Typing_IgnoredChannelPolicy", policy: config.ChannelPolicy{Mode: "ignore"}, message: db.Message{ID: "m-type-5", ThreadID: "th-ignored", Content: "Ignored message @Aerial"}, expectStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
		})
	}
}

func TestProcessBurst_AmbientAndWakeMatrix(t *testing.T) {
	t.Parallel()
	defaultChannelPolicy := config.ChannelPolicy{
		Mode:                 "channel",
		AmbientWakeThreshold: ptrFloat(0.80),
	}

	tests := []burstTestCase{
		{name: "PureAmbient", channelID: "chan-lounge", seedSession: true, classifierJSON: `{"confidence": 0.25, "reason": "casual chit-chat"}`, policy: defaultChannelPolicy, runnerResponse: "Should not run", messages: []db.Message{{ID: "msg-amb-1", AuthorName: "Alice", Content: "Hello everyone"}, {ID: "msg-amb-2", AuthorName: "Bob", Content: "Nice weather today"}}, wantRunnerCalls: 0, wantDeliveryCalls: 0, checkDeliveryCalls: true, wantMaxTypingCalls: 0, checkMaxTypingCalls: true, wantTurnCount: 0, wantMsgErrors: map[string]string{"msg-amb-1": `[AMBIENT score=0.25/0.80 reason="casual chit-chat"]`, "msg-amb-2": `[AMBIENT score=0.25/0.80 reason="casual chit-chat"]`}},
		{name: "Tier1Wake", channelID: "chan-lounge", seedSession: true, botUserID: "bot-aerial-id", policy: defaultChannelPolicy, runnerResponse: "I am Aerial, here to help!", messages: []db.Message{{ID: "msg-tier1-1", AuthorName: "Alice", Content: "<@bot-aerial-id> can you help me with this bug?"}}, wantRunnerCalls: 1, wantDeliveryCalls: 1, checkDeliveryCalls: true, wantDeliveredText: "I am Aerial, here to help!", wantMinTypingCalls: 1, wantTurnCount: 1, wantMsgResponses: map[string]string{"msg-tier1-1": "I am Aerial, here to help!"}},
		{name: "Tier2Wake", channelID: "chan-lounge", seedSession: true, classifierJSON: `{"confidence": 0.90, "reason": "user is asking for system health report"}`, policy: defaultChannelPolicy, runnerResponse: "All systems operational.", messages: []db.Message{{ID: "msg-tier2-1", AuthorName: "Alice", Content: "Does anyone know if all services are healthy?"}}, wantRunnerCalls: 1, wantDeliveryCalls: 1, checkDeliveryCalls: true, wantDeliveredText: "All systems operational.", wantMinTypingCalls: 1, wantTurnCount: 1},
		{name: "MixedBurst", channelID: "chan-lounge", seedSession: true, classifierJSON: `{"confidence": 0.15, "reason": "unrelated lunch discussion"}`, policy: defaultChannelPolicy, runnerResponse: "Done deploying!", messages: []db.Message{{ID: "msg-mixed-1", AuthorName: "Alice", Content: "I had tacos for lunch today"}, {ID: "msg-mixed-2", AuthorName: "Bob", Content: "<@aerial> please deploy the backend"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptContains: []string{"<CHANNEL_HISTORY>", "I had tacos for lunch today", "please deploy the backend"}, wantMsgErrors: map[string]string{"msg-mixed-1": "[AMBIENT score="}, wantMsgResponses: map[string]string{"msg-mixed-2": "Done deploying!"}},
		{name: "SessionRotationBeforeLeadingAmbient", channelID: "chan-rot-ambient", seedSession: true, seedTurnCount: 5, classifierJSON: `{"confidence": 0.10, "reason": "ambient banter"}`, policy: defaultChannelPolicy, runnerConvID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", runnerResponse: "I am answering your question!", messages: []db.Message{{ID: "msg-rot-amb-1", AuthorName: "Alice", Content: "Random ambient chatter before question"}, {ID: "msg-rot-wake-2", AuthorName: "Bob", Content: "Hey <@aerial>, what is 2+2?"}}, wantRunnerCalls: 1, wantTurnCount: 6, wantPassedInitialSession: true, wantOldTranscriptExcludes: "Random ambient chatter before question", wantMsgErrors: map[string]string{"msg-rot-amb-1": "[AMBIENT score="}},
		{name: "TrailingAmbient", channelID: "chan-lounge", seedSession: true, classifierJSON: `{"confidence": 0.10, "reason": "ambient banter"}`, policy: defaultChannelPolicy, runnerResponse: "Aerial answer", preserveSessionInRunner: true, messages: []db.Message{{ID: "msg-trail-wake-1", AuthorName: "Alice", Content: "<@aerial> explain gravity"}, {ID: "msg-trail-amb-2", AuthorName: "Bob", Content: "I love physics too"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptExcludes: []string{"I love physics too"}, wantMsgErrors: map[string]string{"msg-trail-amb-2": "[AMBIENT score="}},
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

func TestResolveEffectiveChannel(t *testing.T) {
	tests := []struct {
		name, wantEffName, wantThreadName, wantCachedName string
		nilSession, testInvalidate, wantIsThread          bool
		inputIDs, wantEffIDs                              []string
		cached                                            []*discordgo.Channel
		stateChannel, restChannel                         *discordgo.Channel
		concurrent                                        int
	}{
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
				var reqCount int32
				reqEntered, releaseReq := make(chan struct{}), make(chan struct{})
				var once sync.Once
				s, _ = discordgo.New("Bot fake-token")
				s.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					atomic.AddInt32(&reqCount, 1)
					once.Do(func() { close(reqEntered) })
					<-releaseReq
					data, _ := json.Marshal(tc.restChannel)
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(data)), Header: make(http.Header)}, nil
				})}
				id := tc.inputIDs[0]
				var doneWg sync.WaitGroup
				for i := 0; i < tc.concurrent; i++ {
					doneWg.Add(1)
					go func() {
						defer doneWg.Done()
						effID, effName, isThread := ResolveEffectiveChannel(s, id)
						if effID != tc.wantEffIDs[0] || effName != tc.wantEffName || isThread != tc.wantIsThread {
							t.Errorf("ResolveEffectiveChannel(%q) = (%q, %q, %v), want (%q, %q, %v)", id, effID, effName, isThread, tc.wantEffIDs[0], tc.wantEffName, tc.wantIsThread)
						}
					}()
				}
				<-reqEntered
				waitForSingleflightFollowers(t, "ResolveEffectiveChannel", tc.concurrent-1)
				close(releaseReq)
				doneWg.Wait()
				if count := atomic.LoadInt32(&reqCount); count != 1 {
					t.Errorf("expected 1 REST request due to singleflight, got %d", count)
				}
			} else {
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
			if tc.wantCachedName != "" {
				if snap, ok := GetCachedChannel(tc.inputIDs[0]); !ok || snap.Name != tc.wantCachedName {
					t.Errorf("expected channel %q cached with name %q, got ok=%v snap=%+v", tc.inputIDs[0], tc.wantCachedName, ok, snap)
				}
			}
		})
	}
}

func TestProcessBurst_InstructionsAndBurstMatrix(t *testing.T) {
	tests := []burstTestCase{
		{name: "ChannelInstructionsInjection", channelID: "100200300400500701", channelInstructions: map[string]string{"dev.md": "Follow Go idioms and test thoroughly."}, cachedChannels: []*discordgo.Channel{{ID: "100200300400500701", Name: "dev", Type: discordgo.ChannelTypeGuildText}}, runnerResponse: "Success", messages: []db.Message{{ID: "msg-inject-1", Content: "Build the queue worker"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptPrefix: "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\nFollow Go idioms and test thoroughly.\n</CHANNEL_INSTRUCTIONS>", wantPromptContains: []string{"Build the queue worker"}},
		{name: "ChannelInstructionsThreadInheritance", channelID: "100200300400500703", channelInstructions: map[string]string{"dev.md": "Guidelines for dev channel discussions."}, cachedChannels: []*discordgo.Channel{{ID: "100200300400500702", Name: "dev", Type: discordgo.ChannelTypeGuildText}, {ID: "100200300400500703", Name: "feature-thread", ParentID: "100200300400500702", Type: discordgo.ChannelTypeGuildPublicThread}}, runnerResponse: "Success", messages: []db.Message{{ID: "msg-thread-inherit-1", Content: "Thread discussion prompt"}}, wantRunnerCalls: 1, wantTurnCount: 1, wantPromptPrefix: "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\nGuidelines for dev channel discussions.\n</CHANNEL_INSTRUCTIONS>"},
		{name: "NoDuplicationOnRetry", channelID: "100200300400500704", channelInstructions: map[string]string{"dev.md": "Channel instructions for retry test."}, cachedChannels: []*discordgo.Channel{{ID: "100200300400500704", Name: "dev", Type: discordgo.ChannelTypeGuildText}}, maxAttempts: 2, runnerSteps: []mockRunnerStep{{returnRawStdout: true, stderr: "Error 503: high demand unavailable", exitCode: 1}, {convID: "session-retry-123", response: "Success on retry"}}, messages: []db.Message{{ID: "msg-retry-1", Content: "Retry test prompt"}}, wantRunnerCalls: 2, wantTurnCount: 1, wantSingleInstructionTagPerPrompt: true},
		{name: "Tier1PreScan_SkipsClassifier", channelID: "chan-lounge", classifierJSON: `{"confidence": 0.0, "reason": "should not be called"}`, policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)}, runnerResponse: "Hello there!", messages: []db.Message{{ID: "msg-prescan-1", AuthorName: "Alice", Content: "just hanging out"}, {ID: "msg-prescan-2", AuthorName: "Bob", Content: "<@aerial> what's up?"}, {ID: "msg-prescan-3", AuthorName: "Charlie", Content: "lol"}}, wantClassifierCalls: 0, checkClassifierCalls: true, wantRunnerCalls: 1, wantTurnCount: 1},
		{name: "CoalescedAmbientBurst", channelID: "chan-lounge", classifierJSON: `{"confidence": 0.95, "reason": "urgent issue needing answer"}`, policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)}, runnerResponse: "I can help with that database error!", messages: []db.Message{{ID: "msg-coalesce-1", AuthorName: "Alice", Content: "Does anyone know why postgres crashed?"}, {ID: "msg-coalesce-2", AuthorName: "Bob", Content: "brb grabbing coffee"}}, wantClassifierCalls: 1, checkClassifierCalls: true, wantClassifierPromptContains: "<target_burst>", wantRunnerCalls: 1, wantTurnCount: 1},
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
		{name: "ColdChannel_NoStubDirectory", channelID: "chan-cold-123", classifierJSON: `{"confidence": 0.05, "reason": "ambient chatter"}`, policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)}, messages: []db.Message{{ID: "msg-cold-ambient-1", AuthorName: "Alice", Content: "just saying hello to everyone in the room"}}, wantRunnerCalls: 0, wantTurnCount: 0, wantSavedSession: "", checkSavedSession: true, wantNoStubDir: true},
		{name: "Turn1ContextInjection", channelID: "chan-history", seedTranscripts: []string{"e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"}, historyMessages: histMessages, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", response: "Answering wake question!"}}, messages: []db.Message{{ID: "msg-hist-turn-1", AuthorName: "Charlie", Content: "<@aerial> turn 1 wake question"}, {ID: "msg-hist-turn-2", AuthorName: "Charlie", Content: "<@aerial> turn 2 follow-up question"}}, wantRunnerCalls: 2, wantTurnCount: 2, wantPassedSessions: []string{"", "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"}, wantTurnPromptContains: map[int][]string{0: {"<CHANNEL_HISTORY>", "First historical message", "Second historical message"}, 1: {"<CHANNEL_HISTORY>"}}, wantTurnPromptExcludes: map[int][]string{1: {"Previous assistant response"}}},
		{name: "ThreadsMode_WarmTurnSuppressesChannelHistory", channelID: "thread-history-suppress", seedTranscripts: []string{"e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"}, historyMessages: histMessages[:1], policy: config.ChannelPolicy{Mode: "threads"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", response: "Answering wake question!"}}, messages: []db.Message{{ID: "msg-thread-turn-1", AuthorName: "Charlie", Content: "Hey Aerial, turn 1 thread question"}, {ID: "msg-thread-turn-2", AuthorName: "Charlie", Content: "Hey Aerial, turn 2 follow-up question"}}, wantRunnerCalls: 2, wantTurnCount: 2, wantTurnPromptContains: map[int][]string{0: {"<CHANNEL_HISTORY>"}}, wantTurnPromptExcludes: map[int][]string{1: {"<CHANNEL_HISTORY>"}}},
		{name: "SessionRotation_ResetsToColdState", channelID: "chan-rotation-cold", seedTranscripts: []string{"c9b5e679-7425-40de-944b-e07fc1f90ae7"}, historyMessages: []HistoryMessage{{ID: "hist-boot-1", AuthorName: "Alice", Role: "User", Content: "Historical message before bootstrap", CreatedAt: time.Now().UTC().Add(-5 * time.Minute)}}, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, runnerSteps: []mockRunnerStep{{convID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", response: "Turn 1 answer"}, {convID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", response: "Subsequent turn answer"}}, messages: []db.Message{{ID: "msg-turn-1", AuthorName: "Alice", Content: "<@aerial> Turn 1"}, {ID: "msg-turn-2", AuthorName: "Alice", Content: "<@aerial> Turn 2"}, {ID: "msg-turn-3", AuthorName: "Alice", Content: "<@aerial> Turn 3"}}, wantRunnerCalls: 3, wantTurnCount: 3, wantSavedSession: "c9b5e679-7425-40de-944b-e07fc1f90ae7", checkSavedSession: true, wantPassedSessions: []string{"", "c9b5e679-7425-40de-944b-e07fc1f90ae7", "c9b5e679-7425-40de-944b-e07fc1f90ae7"}, wantTurnPromptContains: map[int][]string{2: {"<CHANNEL_HISTORY>"}}, wantTurnPromptExcludes: map[int][]string{0: {"<PREVIOUS_SESSION>"}, 2: {"<PREVIOUS_SESSION>"}}},
		{name: "SessionRotation_SeedsPreviousSessionID", channelID: "chan-rotation-seed-test", seedTranscripts: []string{"22222222-2222-2222-2222-222222222222"}, enableHistoryFetcher: true, maxAttempts: 1, policy: config.ChannelPolicy{Mode: "channel"}, sequentialBursts: true, rotateBeforeTurn: 3, runnerSteps: []mockRunnerStep{{convID: "11111111-1111-1111-1111-111111111111", response: "Hello from turn"}, {convID: "11111111-1111-1111-1111-111111111111", response: "Hello from turn"}, {convID: "22222222-2222-2222-2222-222222222222", response: "Hello from turn"}, {convID: "22222222-2222-2222-2222-222222222222", response: "Hello from turn"}}, messages: []db.Message{{ID: "msg-seed-1", AuthorName: "Alice", Content: "<@aerial> Turn 1"}, {ID: "msg-seed-2", AuthorName: "Alice", Content: "<@aerial> Turn 2"}, {ID: "msg-seed-3", AuthorName: "Alice", Content: "<@aerial> Turn 1 of Session 2"}, {ID: "msg-seed-4", AuthorName: "Alice", Content: "<@aerial> Turn 2 of Session 2"}}, wantRunnerCalls: 4, wantTurnCount: 2, wantSavedSession: "22222222-2222-2222-2222-222222222222", checkSavedSession: true, wantTurnPromptContains: map[int][]string{2: {"<PREVIOUS_SESSION>"}}, wantTurnPromptExcludes: map[int][]string{0: {"<PREVIOUS_SESSION>"}, 1: {"<PREVIOUS_SESSION>"}, 2: {"11111111-1111-1111-1111-111111111111", "transcript.jsonl"}, 3: {"<PREVIOUS_SESSION>"}}},
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
		authorID      string
		scheduleRunID string
		content       string
		roles         []string
		want          bool
	}

	tests := []struct {
		name       string
		burst      *burstTestCase
		tier1Cases []tier1Case
	}{
		{name: "ClassifierParseErrorTriggersSystemAlert", burst: &burstTestCase{channelID: "chan-lounge", botUserID: "bot-999", classifierJSON: "```json\n{\"confidence\": 0.85}\n```", policy: config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)}, messages: []db.Message{{ID: "msg-parse-err-1", AuthorName: "Alice", Content: "Hello is anybody there?"}}, wantRunnerCalls: 0, wantTurnCount: 0, wantAlertTitle: "Classifier JSON Parse Error", wantAlertChannel: config.DefaultConfigData().SystemChannel, wantAlertBodyContains: []string{"Parse Error", "Raw Output"}}},
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
			{authorID: "scheduler", content: "check system health", want: true},
			{authorID: "user-456", scheduleRunID: "run-uuid-999", content: "run cron report", want: true},
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
				m := db.Message{AuthorID: c.authorID, ScheduleRunID: c.scheduleRunID, Content: c.content}
				if got := isTier1Wake(m, "bot-12345", c.roles); got != c.want {
					t.Errorf("isTier1Wake(m=%+v, roles=%v) = %v, want %v", m, c.roles, got, c.want)
				}
			}
		})
	}
}

func TestWorkerPoolShutdown_PreservationMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, runnerResponse, wantStatusAfterStop, wantDelivered string
		drainTimeout                                             time.Duration
		blockRunnerOnCtx, blockClassifierCtx, verifyRecovery     bool
		msg                                                      db.Message
	}{
		{name: "PreservesProcessingMessageWithoutApology", drainTimeout: 5 * time.Millisecond, blockRunnerOnCtx: true, verifyRecovery: true, msg: db.Message{ID: "msg-shutdown-1", ThreadID: "thread-shutdown-1", GuildID: "guild-shutdown-1", AuthorID: "user-shutdown-1", AuthorName: "User", Content: "Hello during shutdown"}, wantStatusAfterStop: db.StatusPending},
		{name: "RunnerSuccessPreserved", runnerResponse: "Finished just before pool stopped", msg: db.Message{ID: "msg-success-shutdown-1", ThreadID: "thread-success-shutdown-1", GuildID: "guild-success-shutdown-1", AuthorID: "user-1", AuthorName: "User", Content: "Complete fast"}, wantStatusAfterStop: db.StatusCompleted, wantDelivered: "Finished just before pool stopped"},
		{name: "AmbientClassifierCancellationPreserved", drainTimeout: 5 * time.Millisecond, blockClassifierCtx: true, msg: db.Message{ID: "msg-ambient-shutdown-1", ThreadID: "channel-shutdown-1", GuildID: "channel-shutdown-1", AuthorID: "user-1", AuthorName: "User", Content: "hey did anyone see that movie?"}, wantStatusAfterStop: db.StatusPending},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			var mu sync.Mutex
			var deliveredMessages []string
			stageStarted, releaseRunner := make(chan struct{}), make(chan struct{})
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
				Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3, DrainTimeout: tc.drainTimeout, Classifier: cls,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					once.Do(func() { close(stageStarted) })
					if tc.blockRunnerOnCtx {
						<-ctx.Done()
						return "", "process killed by signal", -1, ctx.Err()
					}
					<-releaseRunner
					return mockJSONResponse("c4444444-2222-3333-4444-555555555555", tc.runnerResponse), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredMessages = append(deliveredMessages, text)
					mu.Unlock()
					return nil
				},
				TypingFunc: noopTyping,
			}
			if tc.blockClassifierCtx {
				cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
					return config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.75)}
				}
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
			stopDone := make(chan struct{})
			go func() {
				pool.Stop()
				close(stopDone)
			}()
			close(releaseRunner)
			waitForCompletion(t, stopDone, 5*time.Second, "pool stop")

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
				newDoneCh := make(chan struct{})
				var finalDelivered string
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
				if err != nil || msgFinal == nil || msgFinal.Status != db.StatusCompleted || msgFinal.RestartCount != 0 || msgFinal.RetryCount != 0 {
					t.Errorf("final msg = %+v, want COMPLETED with 0 restarts/retries", msgFinal)
				}
			}
		})
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

func TestResolveBotRoleIDsSuite(t *testing.T) {
	origFetcher := guildMemberFetcher
	defer func() { guildMemberFetcher = origFetcher }()
	if _, err := origFetcher(nil, "g", "u"); err == nil {
		t.Errorf("expected error from origFetcher(nil)")
	}
	if _, err := origFetcher(&discordgo.Session{}, "g", "u"); err == nil {
		t.Errorf("expected error from origFetcher(empty)")
	}

	tests := []struct {
		name, guildID, botID                           string
		checkNilSession, addGuild, testCooldownPruning bool
		guildRoles                                     []*discordgo.Role
		guildMembers                                   []*discordgo.Member
		fetchRoles, wantRoles                          []string
		fetchErr                                       error
		calls, concurrentCallers                       int
		wantFetchCalls                                 int32
	}{
		{name: "NilSessionAndStateResolution", guildID: "guild-suite-1", botID: "bot-suite-1", checkNilSession: true, addGuild: true, guildRoles: []*discordgo.Role{{ID: "role-aerial-managed", Name: "Aerial", Managed: true}, {ID: "role-gundam-alt", Name: "Gundam"}, {ID: "role-unrelated", Name: "Member"}}, guildMembers: []*discordgo.Member{{User: &discordgo.User{ID: "bot-suite-1"}, Roles: []string{"role-assigned-1"}}}, calls: 1, wantRoles: []string{"role-aerial-managed", "role-assigned-1", "role-gundam-alt"}},
		{name: "RESTFallbackAndCaching", guildID: "guild-suite-rest", botID: "bot-suite-rest", addGuild: true, guildRoles: []*discordgo.Role{{ID: "role-aerial-managed", Name: "Aerial", Managed: true}}, fetchRoles: []string{"mock-role-robot-123"}, calls: 2, wantRoles: []string{"mock-role-robot-123", "role-aerial-managed"}, wantFetchCalls: 1},
		{name: "SingleflightDeduplication", guildID: "guild-suite-sf", botID: "bot-suite-sf", addGuild: true, fetchRoles: []string{"mock-role-robot-123"}, concurrentCallers: 10, wantRoles: []string{"mock-role-robot-123"}, wantFetchCalls: 1},
		{name: "CooldownAndUncachedGuild", guildID: "guild-suite-uncached", botID: "bot-suite-cd", fetchErr: errors.New("simulated 429 rate limit"), calls: 2, testCooldownPruning: true, wantFetchCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			var fetchCalls int32
			enteredCh, releaseCh := make(chan struct{}), make(chan struct{})
			var once sync.Once
			guildMemberFetcher = func(sess *discordgo.Session, gID, uID string) (*discordgo.Member, error) {
				atomic.AddInt32(&fetchCalls, 1)
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
				var wg sync.WaitGroup
				wg.Add(tc.concurrentCallers)
				for i := 0; i < tc.concurrentCallers; i++ {
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
			} else {
				for i := 0; i < tc.calls; i++ {
					r := append([]string(nil), ResolveBotRoleIDs(s, tc.guildID, tc.botID)...)
					sort.Strings(r)
					if strings.Join(r, ",") != wantJoined {
						t.Fatalf("call %d expected %v, got %v", i+1, wantSorted, r)
					}
				}
			}

			if got := atomic.LoadInt32(&fetchCalls); got != tc.wantFetchCalls {
				t.Fatalf("fetchCalls = %d, want %d", got, tc.wantFetchCalls)
			}
			if tc.testCooldownPruning {
				for i := 0; i < 150; i++ {
					setRESTCooldown(fmt.Sprintf("suite-key-%d", i), -time.Minute)
				}
				setRESTCooldown("suite-key-active", time.Minute)
				if !isRESTInCooldown("suite-key-active") || isRESTInCooldown("suite-key-expired") {
					t.Fatalf("unexpected cooldown state")
				}
			}
		})
	}
}

func TestProcessBurst_QuotaPause_Suite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, policyMode, runnerStderr, wantLastMsgStatus, wantLastMsgErrSub string
		useProcessBurst                                                                bool
		messages                                                                       []db.Message
		wantOneShotCount                                                               int
		wantPromptSub, wantDeliveredSub                                                []string
		wantMinRunAtDelay                                                              time.Duration
	}{
		{name: "SchedulesOneShotAndNotifies", threadID: "chan-quota-suite-1", runnerStderr: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", messages: []db.Message{{ID: "msg-quota-suite-1", Content: "<@bot> can you analyze our code coverage?"}}, wantOneShotCount: 1, wantPromptSub: []string{"[QUOTA_RETRY]", "code coverage"}, wantMinRunAtDelay: 16 * time.Minute, wantDeliveredSub: []string{"16m 58s", "<t:", ":R>", "automatically scheduled a retry"}, wantLastMsgStatus: db.StatusFailed, wantLastMsgErrSub: "[QUOTA_PAUSED"},
		{name: "CircuitBreakerDoesNotReschedule", threadID: "chan-circuit-suite-1", runnerStderr: "Individual quota reached. Resets in 10m.", messages: []db.Message{{ID: "msg-circuit-suite-1", ScheduleRunID: "run-sched-previous-1", Content: "[QUOTA_RETRY] Previous failed question"}}, wantOneShotCount: 0, wantDeliveredSub: []string{"paused automated retries"}, wantLastMsgStatus: db.StatusFailed},
		{name: "TrailingBurstSuppression", threadID: "chan-burst-suppress-suite-1", policyMode: "channel", useProcessBurst: true, runnerStderr: "Individual quota reached. Resets in 16m58s.", messages: []db.Message{{ID: "msg-burst-suite-1", Content: "<@aerial> Question 1"}, {ID: "msg-burst-suite-2", Content: "<@aerial> Question 2"}}, wantOneShotCount: 1, wantLastMsgStatus: db.StatusFailed, wantLastMsgErrSub: "Trailing message suppressed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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

func TestProcessBurst_ErrorClassificationMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, errStdout, errStderr, expectedStatus                                        string
		existingSession, seedPBFile, checkLatchedCleared, checkNoticeDelivered, prohibitUnavailable bool
		failUntilAttempt, errExitCode, expectedAttempts                                             int
	}{
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
		})
	}
}

func TestGetSessionLastActivity_Suite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, sessionID                                             string
		seedSession, passSessMgr, closeStore, wantCold, wantZeroTime, wantErr bool
		sessionTurnCount                                                      int
		sessionAge, msgAge, diskLogAge, wantAge                               time.Duration
		msgResponses                                                          []string
	}{
		{name: "ColdThread_ZeroTime", threadID: "thread-act-cold", passSessMgr: true, wantCold: true, wantZeroTime: true},
		{name: "SessionUpdatedAtOnly", threadID: "thread-act-sess", sessionID: "sess-act-uuid-1", seedSession: true, sessionTurnCount: 1, sessionAge: 20 * time.Minute, passSessMgr: true, wantCold: false, wantAge: 20 * time.Minute},
		{name: "CompletedMessageNewerThanSession", threadID: "thread-act-msg", sessionID: "sess-act-uuid-2", seedSession: true, sessionTurnCount: 1, sessionAge: 20 * time.Minute, msgResponses: []string{"some answer"}, msgAge: 10 * time.Minute, passSessMgr: true, wantCold: false, wantAge: 10 * time.Minute},
		{name: "DiskLogNewerThanDB", threadID: "thread-act-disk", sessionID: "sess-act-uuid-3", seedSession: true, sessionTurnCount: 1, sessionAge: 20 * time.Minute, msgResponses: []string{"some answer"}, msgAge: 10 * time.Minute, diskLogAge: 2 * time.Minute, passSessMgr: true, wantCold: false, wantAge: 2 * time.Minute},
		{name: "RotatedSessionNotNew", threadID: "thread-rotated-1", seedSession: true, sessionAge: 15 * time.Minute, msgResponses: []string{"previous answer"}, msgAge: 4 * time.Minute, wantCold: false, wantAge: 4 * time.Minute},
		{name: "IgnoresExpiredStaleSentinel", threadID: "thread-stale-sentinel", msgResponses: []string{"[EXPIRED_STALE]", "[AMBIENT: IGNORED]"}, wantCold: true, wantZeroTime: true},
		{name: "FailOpenOnDBError", threadID: "thread-closed", closeStore: true, wantCold: false, wantZeroTime: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			sessMgr, now := session.New(tmpDir, ""), time.Now().UTC().Truncate(time.Second)

			if tc.seedSession {
				tSess := now.Add(-tc.sessionAge)
				store.SetSessionInfo(db.SessionInfo{ThreadID: tc.threadID, InternalSessionID: tc.sessionID, TurnCount: tc.sessionTurnCount, CreatedAt: tSess, UpdatedAt: tSess})
			}
			for i, resp := range tc.msgResponses {
				tMsg := now.Add(-tc.msgAge)
				_ = insertMessage(store, db.Message{ID: fmt.Sprintf("msg-%d", i), ThreadID: tc.threadID, Status: db.StatusCompleted, ResponseText: resp, CreatedAt: tMsg, UpdatedAt: tMsg})
			}
			if tc.diskLogAge > 0 {
				tasksDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", tc.sessionID, ".system_generated", "tasks")
				_ = os.MkdirAll(tasksDir, 0o755)
				tDisk := now.Add(-tc.diskLogAge)
				taskLog := filepath.Join(tasksDir, "task-1.log")
				_ = os.WriteFile(taskLog, []byte("build output\n"), 0o644)
				_ = os.Chtimes(taskLog, tDisk, tDisk)
			}
			if tc.closeStore {
				_ = store.Close()
			}

			var act time.Time
			var isCold bool
			var err error
			if tc.passSessMgr {
				act, isCold, err = GetSessionLastActivity(store, tc.threadID, sessMgr)
			} else {
				act, isCold, err = GetSessionLastActivity(store, tc.threadID)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if isCold != tc.wantCold {
				t.Errorf("isCold = %v, want %v", isCold, tc.wantCold)
			}
			if tc.wantZeroTime && !act.IsZero() {
				t.Errorf("expected zero time, got %v", act)
			}
			if tc.wantAge > 0 && !act.Equal(now.Add(-tc.wantAge)) {
				t.Errorf("act = %v, want %v", act, now.Add(-tc.wantAge))
			}
		})
	}
}

func TestProcessBurst_StalenessMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, sessionID, expectStatus     string
		msgAge, diskLogAge, dbMsgAge, sessionAge    time.Duration
		retryCount, restartCount, expectRunnerCalls int
		expectExpiredStale, prohibitExpiredStale    bool
	}{
		{name: "ExistingSessionRecentDiskActivity_Retained", threadID: "thread-burst-disk-active", sessionID: "sess-burst-disk-active", msgAge: 35 * time.Minute, diskLogAge: 5 * time.Minute, expectRunnerCalls: 1, expectStatus: db.StatusCompleted, prohibitExpiredStale: true},
		{name: "ExistingSessionRecentDBActivity_Retained", threadID: "thread-burst-db-active", msgAge: 35 * time.Minute, dbMsgAge: 5 * time.Minute, expectRunnerCalls: 1},
		{name: "ExistingSessionInactive_Dropped", threadID: "thread-burst-inactive", sessionID: "sess-burst-inactive", msgAge: 35 * time.Minute, sessionAge: 45 * time.Minute, dbMsgAge: 45 * time.Minute, expectRunnerCalls: 0, expectExpiredStale: true},
		{name: "NewSession_Dropped", threadID: "thread-new-drop", msgAge: 31 * time.Minute, expectRunnerCalls: 0, expectExpiredStale: true},
		{name: "HardCeiling_Dropped", threadID: "thread-hard-ceiling", sessionID: "sess-ceiling", msgAge: 130 * time.Minute, sessionAge: 1 * time.Second, dbMsgAge: 1 * time.Second, expectRunnerCalls: 0, expectExpiredStale: true},
		{name: "RecoveredMessage_Retained", threadID: "thread-recovered-msg", sessionID: "sess-rec", msgAge: 45 * time.Minute, retryCount: 1, expectRunnerCalls: 1, prohibitExpiredStale: true},
		{name: "RestartedMessage_Retained", threadID: "thread-restarted-staleness", sessionID: "d2222222-2222-3333-4444-555555555555", msgAge: 45 * time.Minute, restartCount: 1, expectRunnerCalls: 1, prohibitExpiredStale: true},
		{name: "RestartedMessage_HardCeiling_Dropped", threadID: "thread-restarted-hard-ceiling", msgAge: 130 * time.Minute, restartCount: 2, expectRunnerCalls: 0, expectExpiredStale: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			now := time.Now().UTC()
			if tt.diskLogAge > 0 {
				_ = saveSessionID(store, tt.threadID, tt.sessionID)
				tasksDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", tt.sessionID, ".system_generated", "tasks")
				_ = os.MkdirAll(tasksDir, 0o755)
				tDisk := now.Add(-tt.diskLogAge)
				taskLog := filepath.Join(tasksDir, "task-1.log")
				_ = os.WriteFile(taskLog, []byte("running build"), 0o644)
				_ = os.Chtimes(taskLog, tDisk, tDisk)
			}
			if tt.sessionAge > 0 {
				store.SetSessionInfo(db.SessionInfo{ThreadID: tt.threadID, InternalSessionID: tt.sessionID, TurnCount: 1, UpdatedAt: now.Add(-tt.sessionAge)})
			}
			if tt.dbMsgAge > 0 {
				tPrev := now.Add(-tt.dbMsgAge)
				_ = insertMessage(store, db.Message{ID: "msg-prev-" + tt.name, ThreadID: tt.threadID, Status: db.StatusCompleted, ResponseText: "earlier turn", CreatedAt: tPrev, UpdatedAt: tPrev})
			}

			var runnerCalls int
			var mu sync.Mutex
			doneCh := make(chan struct{})
			sessIDToReturn := tt.sessionID
			if sessIDToReturn == "" {
				sessIDToReturn = "sess-fallback"
			}

			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: session.New(tmpDir, ""), Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					mu.Lock()
					runnerCalls++
					mu.Unlock()
					return mockJSONResponse(sessIDToReturn, "executed"), "", 0, nil
				},
				DeliveryFunc: noopDelivery, TypingFunc: noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) { close(doneCh) },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: "msg-" + tt.name, ThreadID: tt.threadID, AuthorID: "user-1", Content: "prompt " + tt.name, Status: db.StatusPending, RetryCount: tt.retryCount, RestartCount: tt.restartCount, CreatedAt: now.Add(-tt.msgAge)}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)
			waitForCompletion(t, doneCh, 3*time.Second, tt.name)

			mu.Lock()
			calls := runnerCalls
			mu.Unlock()
			if calls != tt.expectRunnerCalls {
				t.Errorf("runnerCalls = %d, want %d", calls, tt.expectRunnerCalls)
			}
			dbMsg, _ := getMessage(store, msg.ID)
			if tt.expectStatus != "" && dbMsg.Status != tt.expectStatus {
				t.Errorf("status = %s, want %s", dbMsg.Status, tt.expectStatus)
			}
			if tt.expectExpiredStale && dbMsg.ResponseText != "[EXPIRED_STALE]" {
				t.Errorf("Expected [EXPIRED_STALE], got %s", dbMsg.ResponseText)
			}
			if tt.prohibitExpiredStale && dbMsg.ResponseText == "[EXPIRED_STALE]" {
				t.Errorf("Expected response NOT to be [EXPIRED_STALE]")
			}
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
		msg := db.Message{ID: c.msgID, ThreadID: c.threadID, AuthorID: "scheduler", AuthorName: "Scheduler", Content: "Run routine", Status: db.StatusPending, ScheduleRunID: c.runID, Effort: c.effort, CreatedAt: time.Now().UTC()}
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

func TestProcessBurst_RecordsTokensTelemetry(t *testing.T) {
	cases := []struct {
		name         string
		model        string
		threadID     string
		mockOutput   string
		wantContains []string
		wantExcludes string
	}{
		{name: "NamedChannel", model: "test-token-model", threadID: "chan-token-test-1", mockOutput: `{"conversation_id":"c1111111-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":150,"output_tokens":75,"thinking_tokens":30,"cache_read_tokens":10,"total_tokens":265}}`, wantContains: []string{`aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="total"} 265`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="input"} 150`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="output"} 75`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="thinking"} 30`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="cache_read"} 10`}},
		{name: "SnowflakeChannelSanitized", model: "test-snowflake-model", threadID: "1534436119888793750", mockOutput: `{"conversation_id":"c2222222-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":100,"output_tokens":50,"thinking_tokens":20,"cache_read_tokens":5,"total_tokens":175}}`, wantContains: []string{`aerial_brain_tokens_total{channel="unknown",model="test-snowflake-model",type="total"} 175`}, wantExcludes: "1534436119888793750"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := setupTestStore(t)
			pool := New(nil, WorkerPoolConfig{
				Store: store,
				Model: tc.model,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.mockOutput, "", 0, nil
				},
			})
			pool.Start()
			defer pool.Stop()

			t0 := time.Now().UTC()
			msg := db.Message{
				ID:        "msg-token-" + tc.name,
				ThreadID:  tc.threadID,
				Content:   "<@aerial> Question Token Test",
				Status:    db.StatusPending,
				CreatedAt: t0,
				UpdatedAt: t0,
			}
			_ = insertMessage(store, msg)
			pool.processBurst([]db.Message{msg})

			rec := httptest.NewRecorder()
			metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := rec.Body.String()
			for _, sub := range tc.wantContains {
				if !strings.Contains(body, sub) {
					t.Errorf("metrics body missing %q:\n%s", sub, body)
				}
			}
			if tc.wantExcludes != "" && strings.Contains(body, tc.wantExcludes) {
				t.Errorf("metrics body should not contain %q:\n%s", tc.wantExcludes, body)
			}
		})
	}
}

func TestThreadStatusQueueIntegration(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	threadID := "thread-status-test-123"
	var finalDelivered atomic.Int32

	sess, _ := discordgo.New("Bot mock_token")
	sess.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		msgJSON := `{"id":"mock-status-msg-123","channel_id":"thread-status-test-123","content":"mock status"}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(msgJSON)),
		}, nil
	})
	sess.State = discordgo.NewState()
	_ = sess.State.ChannelAdd(&discordgo.Channel{
		ID: threadID, Name: "status-thread", Type: discordgo.ChannelTypeGuildPublicThread, ParentID: "parent-chan-1",
	})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			finalDelivered.Add(1)
			return nil
		},
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			if opts.StepUpdateHandler != nil {
				opts.StepUpdateHandler(&runner.StepUpdateEvent{StepType: "tool", ToolName: "view_file", State: "ACTIVE"})
			}
			return `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"Hello from thread!"}`, "", 0, nil
		},
	})
	pool.SetDiscordSession(sess)

	msg := db.Message{ID: "msg-thread-1", ThreadID: threadID, AuthorID: "user-1", Content: "check my file", CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	pool.processBurst([]db.Message{msg})

	if finalDelivered.Load() != 1 {
		t.Errorf("Expected 1 final message delivered, got %d", finalDelivered.Load())
	}
}

func TestWorkerPool_HermeticByDefaultWithoutRetriever(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var runnerCalled bool
	poolWithDB := NewWorkerPool(WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			runnerCalled = true
			return mockJSONResponse("sess-test-1", "Test Response"), "", 0, nil
		},
		DeliveryFunc: noopDelivery,
		TypingFunc:   noopTyping,
	})
	msg := db.Message{ID: "msg-hermetic-1", ThreadID: "thread-hermetic-1", AuthorID: "user-1", Content: "Hermetic test without retriever", CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	poolWithDB.processBurst([]db.Message{msg})
	if !runnerCalled {
		t.Errorf("Expected runner to be called cleanly without error")
	}
}

func TestProcessBurst_GracefulShutdown_ResetsToPendingWithoutRestartPenalty(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	msg := db.Message{ID: "msg-graceful-shutdown", ThreadID: "thread-graceful", Content: "task interrupted by deployment", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	pool.ctx = ctx
	pool.processBurst([]db.Message{msg})

	updated, err := getMessage(store, "msg-graceful-shutdown")
	if err != nil || updated == nil || updated.Status != db.StatusPending || updated.RestartCount != 0 {
		t.Fatalf("updated = %+v (err=%v), want PENDING with restart_count=0", updated, err)
	}
}

func TestIsRateLimitError_ExtendedKeywords(t *testing.T) {
	t.Parallel()
	tests := []struct {
		errDetail, extra string
		want             bool
	}{
		{errDetail: "Error 503", want: true},
		{errDetail: "rate limit exceeded", want: true},
		{errDetail: "RESOURCE_EXHAUSTED", want: true},
		{errDetail: "HTTP 429", want: true},
		{errDetail: "high demand, please wait", want: true},
		{errDetail: "Daily quota exceeded", want: true},
		{errDetail: "Too Many Requests", want: true},
		{errDetail: "model is overloaded", want: true},
		{errDetail: "503 Service Unavailable", want: true},
		{errDetail: "resource has been exhausted by caller", want: true},
		{errDetail: "Individual quota reached for user", want: true},
		{errDetail: "Please upgrade your subscription to continue", want: true},
		{errDetail: "generic error", extra: "upstream returned 429 Too Many Requests", want: true},
		{errDetail: "write: disk quota exceeded", want: false},
		{errDetail: "syntax error on line 42", want: false},
		{errDetail: "", want: false},
	}
	for _, tt := range tests {
		if got := isRateLimitError(tt.errDetail, tt.extra); got != tt.want {
			t.Errorf("isRateLimitError(%q, %q) = %t, want %t", tt.errDetail, tt.extra, got, tt.want)
		}
	}
}

func TestWorkerPool_NotifierInvocationMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, seedSessionID, runnerStderr, notifierReturn, expectDeliveredNotice, expectedDescContains string
		maxAttempts, runnerExitCode, wantMaxDescLen                                                              int
		runnerErr, deliveryErr                                                                                   error
		expectNotifierCalled, expectUnavailableMsg                                                               bool
	}{
		{name: "WatchdogExhaustion_InvokesNotifierFunc", threadID: "thread-wd-1", maxAttempts: 1, runnerStderr: "inactivity timeout exceeded [watchdog]", runnerErr: errors.New("watchdog timeout"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "execution timed out", notifierReturn: "DEADASS TIMED OUT 💀", expectDeliveredNotice: "DEADASS TIMED OUT 💀"},
		{name: "NonTransient_InvokesNotifierFunc", threadID: "thread-nt-1", maxAttempts: 3, runnerStderr: "Error: unknown flag: --bogus-flag", runnerErr: errors.New("exit status 1"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "non-transient error", notifierReturn: "UNACCEPTABLE GARBAGE DETECTED 🔥", expectDeliveredNotice: "UNACCEPTABLE GARBAGE DETECTED 🔥"},
		{name: "RetryExhaustion_InvokesNotifierFunc", threadID: "thread-exhaust-1", maxAttempts: 2, runnerStderr: "connection reset by peer", runnerErr: errors.New("network drop"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "exhausting 2 attempts", notifierReturn: "ROASTED TO ASHES 🤘", expectDeliveredNotice: "ROASTED TO ASHES 🤘"},
		{name: "RateLimitBypassesNotifierFunc", threadID: "thread-rl-1", maxAttempts: 1, runnerStderr: "Error 429: Too Many Requests", runnerErr: errors.New("rate limit"), runnerExitCode: 1, expectUnavailableMsg: true},
		{name: "Watchdog_RateLimitBypassesNotifier", threadID: "thread-wd-rl", maxAttempts: 1, runnerStderr: "inactivity timeout exceeded: [watchdog] Resource has been exhausted (quota limit reached)", runnerErr: context.DeadlineExceeded, runnerExitCode: 124, expectUnavailableMsg: true},
		{name: "SessionCorruption_RateLimitBypassesNotifier", threadID: "thread-sc-rl", maxAttempts: 1, seedSessionID: "sess-existing-1", runnerStderr: "conversation not found: 429 rate limit exceeded", runnerErr: errors.New("rate limited corruption"), runnerExitCode: 1, expectUnavailableMsg: true},
		{name: "NonTransient_LongSnippetAndDeliveryErr", threadID: "thread-nt-long", maxAttempts: 1, runnerStderr: strings.Repeat("VeryBadError-", 50), runnerErr: errors.New("fatal command syntax"), runnerExitCode: 1, deliveryErr: errors.New("discord delivery network failed"), expectNotifierCalled: true, wantMaxDescLen: 350, notifierReturn: "DEADASS BROKEN 💀"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, doneCh := setupTestStore(t), make(chan struct{}, 1)
			if tt.seedSessionID != "" {
				_ = rotateSessionID(store, tt.threadID, tt.seedSessionID)
			}
			var capturedDesc, deliveredNotif string
			var mu sync.Mutex
			notifierCalled := false

			pool := NewWorkerPool(WorkerPoolConfig{
				Store: store, MaxAttempts: tt.maxAttempts, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					return "", tt.runnerStderr, tt.runnerExitCode, tt.runnerErr
				},
				NotifierFunc: func(agyBin, apiKey, desc string) string {
					mu.Lock()
					notifierCalled, capturedDesc = true, desc
					mu.Unlock()
					return tt.notifierReturn
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					if tt.deliveryErr != nil {
						return tt.deliveryErr
					}
					mu.Lock()
					deliveredNotif = text
					mu.Unlock()
					return nil
				},
				OnMessageCompleted: func(msg db.Message, finalStatus string) { doneCh <- struct{}{} },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: "msg-" + tt.name, ThreadID: tt.threadID, Content: "test content"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)
			waitForCompletion(t, doneCh, 3*time.Second, tt.name)

			mu.Lock()
			defer mu.Unlock()
			if notifierCalled != tt.expectNotifierCalled {
				t.Errorf("notifierCalled=%t, want %t", notifierCalled, tt.expectNotifierCalled)
			}
			if tt.expectedDescContains != "" && !strings.Contains(capturedDesc, tt.expectedDescContains) {
				t.Errorf("desc missing %q", tt.expectedDescContains)
			}
			if tt.wantMaxDescLen > 0 && len([]rune(capturedDesc)) > tt.wantMaxDescLen {
				t.Errorf("desc len %d > %d", len([]rune(capturedDesc)), tt.wantMaxDescLen)
			}
			if tt.expectDeliveredNotice != "" && deliveredNotif != tt.expectDeliveredNotice {
				t.Errorf("delivered notice=%q, want %q", deliveredNotif, tt.expectDeliveredNotice)
			}
			if tt.expectUnavailableMsg && deliveredNotif != notifier.ModelUnavailableMessage() {
				t.Errorf("delivered=%q, want %q", deliveredNotif, notifier.ModelUnavailableMessage())
			}
		})
	}
}

func TestWorkerPool_CancellationDuringBackoffSuite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, scheduleRunID, stderr string
		maxAttempts, exitCode                 int
		backoffBase                           time.Duration
		err                                   error
		expectDeliveredCheck                  bool
	}{
		{name: "Watchdog_CancelledDuringBackoff", threadID: "thread-wd-cancel", scheduleRunID: "run-wd-cancel", maxAttempts: 3, backoffBase: 500 * time.Millisecond, stderr: "inactivity timeout exceeded: [watchdog] hung process", exitCode: 124, err: context.DeadlineExceeded},
		{name: "Watchdog_CancelledPostWatchdog", threadID: "thread-wd-post-cancel", maxAttempts: 1, backoffBase: 5 * time.Millisecond, stderr: "inactivity timeout exceeded: [watchdog] single attempt cancel", exitCode: 124, err: context.DeadlineExceeded, expectDeliveredCheck: true},
		{name: "NonTransient_CancelledDuringTurn", threadID: "thread-nt-cancel", maxAttempts: 1, backoffBase: 5 * time.Millisecond, stderr: "Fatal: invalid prompt parameters", exitCode: 2, err: errors.New("non transient err"), expectDeliveredCheck: true},
		{name: "RetryExhaustion_CancelledWithScheduleRunID", threadID: "thread-exh-cancel", scheduleRunID: "run-exh-cancel", maxAttempts: 1, backoffBase: 5 * time.Millisecond, stderr: "connection timed out", exitCode: 1, err: errors.New("timeout")},
		{name: "RateLimit_CancelledDuringBackoff", threadID: "thread-rl-cancel", scheduleRunID: "run-rl-cancel", maxAttempts: 3, backoffBase: 500 * time.Millisecond, stderr: "Error 429: Resource has been exhausted (quota exceeded)", exitCode: 1, err: errors.New("rate limited")},
		{name: "Transient_CancelledDuringBackoff", threadID: "thread-tr-cancel", scheduleRunID: "run-tr-cancel", maxAttempts: 3, backoffBase: 500 * time.Millisecond, stderr: "connection reset by peer", exitCode: 1, err: errors.New("transient network drop")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			var pool *WorkerPool
			deliveredCalled := false
			var mu sync.Mutex

			pool = NewWorkerPool(WorkerPoolConfig{
				Store: store, MaxAttempts: tt.maxAttempts, TimeoutMinutes: 1, BackoffBase: tt.backoffBase,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					if tt.maxAttempts > 1 {
						go func() {
							for {
								select {
								case <-pool.ctx.Done():
									return
								default:
								}
								if m, _ := getMessage(store, "msg-"+tt.name); m != nil && m.RetryCount > 0 {
									pool.cancel()
									return
								}
								runtime.Gosched()
							}
						}()
					} else {
						pool.cancel()
					}
					return "", tt.stderr, tt.exitCode, tt.err
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredCalled = true
					mu.Unlock()
					return nil
				},
			})
			pool.Start()
			defer pool.Stop()

			msgID := "msg-" + tt.name
			msg := db.Message{ID: msgID, ThreadID: tt.threadID, Content: "cancel test", ScheduleRunID: tt.scheduleRunID}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)
			waitPoolWorkersDone(t, pool, 2*time.Second)

			if tt.expectDeliveredCheck {
				mu.Lock()
				called := deliveredCalled
				mu.Unlock()
				if called {
					t.Errorf("DeliveryFunc should not be called when pool is cancelled")
				}
			}
			if dbMsg, _ := getMessage(store, msgID); dbMsg != nil && dbMsg.Status != db.StatusPending {
				t.Errorf("Expected status PENDING after cancellation, got: %s", dbMsg.Status)
			}
		})
	}
}

type mockWebhookDispatcher struct {
	callWakeHookFunc     func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error)
	callPreTurnHookFunc  func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error)
	callPostTurnHookFunc func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error)
}

func (m *mockWebhookDispatcher) CallWakeHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
	if m.callWakeHookFunc != nil {
		return m.callWakeHookFunc(ctx, endpoint, req)
	}
	return nil, nil
}

func (m *mockWebhookDispatcher) CallPreTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
	if m.callPreTurnHookFunc != nil {
		return m.callPreTurnHookFunc(ctx, endpoint, req)
	}
	return nil, nil
}

func (m *mockWebhookDispatcher) CallPostTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
	if m.callPostTurnHookFunc != nil {
		return m.callPostTurnHookFunc(ctx, endpoint, req)
	}
	return nil, nil
}

func runWakeHookEval(t *testing.T, hookResp *WakeResponse, hookErr error, timeoutAction, classifierResp string, inputMsgs []db.Message, emptyEffID bool, schedRunID string) (bool, bool, bool, *turnExecution, db.Store, *WakeRequest) {
	t.Helper()
	store, now := setupTestStore(t), time.Now().UTC()
	var classifierCalled, hookCalled bool
	var cls *classifier.Classifier
	if classifierResp != "" {
		cls = classifier.New(nil)
		cls.Model = "test-flash"
		cls.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
			classifierCalled = true
			return classifierResp, nil
		}
	}
	var capturedReq *WakeRequest
	pool := NewWorkerPool(WorkerPoolConfig{Store: store, Classifier: cls, WebhookDispatcher: &mockWebhookDispatcher{callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
		hookCalled = true
		capturedReq = req
		return hookResp, hookErr
	}}})
	msgs := make([]db.Message, len(inputMsgs))
	for i, m := range inputMsgs {
		m.CreatedAt = now.Add(time.Duration(i) * time.Second)
		msgs[i] = m
		_ = insertMessage(store, m)
	}
	effID := msgs[0].ThreadID
	if emptyEffID {
		effID = ""
	}
	te := &turnExecution{
		pool: pool, burst: msgs, threadID: msgs[0].ThreadID, effectiveID: effID, triggerType: "message", execStart: now,
		policy: config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrFloat(0.80), Hooks: config.ChannelHooksConfig{OnWake: &config.WebhookEndpoint{URL: "http://sidecar.local/wake", TimeoutMs: 1000, OnTimeoutAction: timeoutAction}}},
	}
	return te.evaluateAmbientWake(), hookCalled, classifierCalled, te, store, capturedReq
}

func TestOnWakeHook_Lifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, timeoutAction, classifierResp, schedRunID string
		msgs                                            []db.Message
		hookResp                                        *WakeResponse
		hookErr                                         error
		isSched, wantExit, wantClassifier               bool
		wantBurstLen, wantTrailLen, wantTurns           int
	}{
		{name: "WakeOverride", msgs: []db.Message{{ID: "m-w1", ThreadID: "ch-gen", Content: "hi", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "wake", Reason: "hook_priority_wake"}, classifierResp: `{"confidence": 0.10}`},
		{name: "DropOverride", msgs: []db.Message{{ID: "m-d1", ThreadID: "ch-gen", Content: "ambient", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "drop", Reason: "hook_drop_override"}, classifierResp: `{"confidence": 0.95}`, wantExit: true},
		{name: "TimeoutFallback_Classify", msgs: []db.Message{{ID: "m-c1", ThreadID: "ch-gen", Content: "hello world", Status: db.StatusProcessing}}, hookErr: context.DeadlineExceeded, timeoutAction: "classify", classifierResp: `{"confidence": 0.92, "reason": "classified wake"}`, wantClassifier: true},
		{name: "TimeoutFallback_Wake", msgs: []db.Message{{ID: "m-tw1", ThreadID: "ch-gen", Content: "err-wake", Status: db.StatusProcessing}}, hookErr: errors.New("conn refused"), timeoutAction: "wake", classifierResp: `{"confidence": 0.10}`},
		{name: "TimeoutFallback_Drop", msgs: []db.Message{{ID: "m-td1", ThreadID: "ch-gen", Content: "err-drop", Status: db.StatusProcessing}}, hookErr: errors.New("timeout"), timeoutAction: "drop", classifierResp: `{"confidence": 0.95}`, wantExit: true},
		{name: "MultiMessageBurst", msgs: []db.Message{{ID: "m-m1", ThreadID: "ch-multi", Content: "wake 1", Status: db.StatusProcessing}, {ID: "m-m2", ThreadID: "ch-multi", Content: "trail 2", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "wake"}, wantBurstLen: 1, wantTrailLen: 1, wantTurns: 1},
		{name: "DropWithScheduleRun", msgs: []db.Message{{ID: "m-s1", ThreadID: "ch-sched", Content: "sched noise", ScheduleRunID: "sched-run-amb", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "drop"}, isSched: true, schedRunID: "sched-run-amb", wantExit: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shouldExit, hookCalled, classifierCalled, te, store, capturedReq := runWakeHookEval(t, tc.hookResp, tc.hookErr, tc.timeoutAction, tc.classifierResp, tc.msgs, tc.isSched, tc.schedRunID)
			if shouldExit != tc.wantExit || !hookCalled || classifierCalled != tc.wantClassifier {
				t.Fatalf("%s: got exit=%v hook=%v classifier=%v (want exit=%v classifier=%v)", tc.name, shouldExit, hookCalled, classifierCalled, tc.wantExit, tc.wantClassifier)
			}
			if !tc.wantExit {
				if te.wakeIdx != 0 || len(te.wakeInfos) == 0 || !te.wakeInfos[0].IsWake {
					t.Errorf("wakeIdx=%d wakeInfos=%+v", te.wakeIdx, te.wakeInfos)
				}
				if tc.wantBurstLen > 0 && len(te.burst) != tc.wantBurstLen {
					t.Errorf("burst len = %d, want %d", len(te.burst), tc.wantBurstLen)
				}
				if tc.wantTrailLen > 0 && len(te.trailingMsgs) != tc.wantTrailLen {
					t.Errorf("trailing len = %d, want %d", len(te.trailingMsgs), tc.wantTrailLen)
				}
				if tc.wantTurns > 0 {
					if turnCount, _ := getSessionTurnCount(store, tc.msgs[0].ThreadID); turnCount != tc.wantTurns {
						t.Errorf("turnCount = %d, want %d", turnCount, tc.wantTurns)
					}
				}
			} else {
				if te.wakeIdx != -1 {
					t.Errorf("wakeIdx=%d, want -1", te.wakeIdx)
				}
				if saved, _ := getMessage(store, tc.msgs[0].ID); saved == nil || saved.Status != db.StatusCompleted {
					t.Fatalf("saved msg = %+v", saved)
				}
			}
			if capturedReq == nil || capturedReq.ChannelID == "" || capturedReq.Timestamp.IsZero() {
				t.Errorf("capturedReq invalid: %+v", capturedReq)
			}
		})
	}
}

func TestOnWakeHook_FullProcessBurst(t *testing.T) {
	t.Parallel()
	store, now := setupTestStore(t), time.Now().UTC()
	var mu sync.Mutex
	var runnerCalls, deliveryCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, SessionManager: session.New(t.TempDir(), ""), TimeoutMinutes: 1,
		WebhookDispatcher: &mockWebhookDispatcher{callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
			if req.Message.Content == "wake message" {
				return &WakeResponse{Override: "wake"}, nil
			}
			return &WakeResponse{Override: "drop"}, nil
		}},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse(sessionID, "Bot response"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			mu.Unlock()
			return nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrFloat(0.80), Hooks: config.ChannelHooksConfig{OnWake: &config.WebhookEndpoint{URL: "http://sidecar.local/wake"}}}
		},
	})

	msgDrop, msgWake := db.Message{ID: "msg-burst-drop", ThreadID: "chan-full-test", AuthorName: "user-foxtrot", Content: "drop message", Status: db.StatusPending, CreatedAt: now}, db.Message{ID: "msg-burst-wake", ThreadID: "chan-full-test", AuthorName: "user-golf", Content: "wake message", Status: db.StatusPending, CreatedAt: now.Add(time.Second)}
	_ = insertMessage(store, msgDrop)
	pool.processBurst([]db.Message{msgDrop})
	if savedDrop, _ := getMessage(store, "msg-burst-drop"); savedDrop == nil || savedDrop.Status != db.StatusCompleted || !strings.Contains(savedDrop.ErrorMessage, "[AMBIENT") {
		t.Errorf("expected drop message completed as ambient, got %+v", savedDrop)
	}
	_ = insertMessage(store, msgWake)
	pool.processBurst([]db.Message{msgWake})
	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 1 || deliveryCalls != 1 {
		t.Errorf("runnerCalls=%d, deliveryCalls=%d, want 1, 1", runnerCalls, deliveryCalls)
	}
	if savedWake, _ := getMessage(store, "msg-burst-wake"); savedWake == nil || savedWake.Status != db.StatusCompleted || savedWake.ResponseText != "Bot response" {
		t.Errorf("expected wake message completed with response, got %+v", savedWake)
	}
}

func TestOnWakeHook_AccessorsAndFallbacks(t *testing.T) {
	t.Parallel()
	var nilPool *WorkerPool
	if nilPool.WebhookDispatcher() != nil {
		t.Errorf("expected nil for nilPool.WebhookDispatcher")
	}
	nilPool.SetWebhookDispatcher(nil)

	poolDef := NewWorkerPool(WorkerPoolConfig{})
	if poolDef.WebhookDispatcher() == nil {
		t.Errorf("expected non-nil default dispatcher")
	}
	mockDisp := &mockWebhookDispatcher{}
	poolDef.SetWebhookDispatcher(mockDisp)
	if poolDef.WebhookDispatcher() != mockDisp {
		t.Errorf("expected mockDisp after SetWebhookDispatcher")
	}
	poolDef.SetWebhookDispatcher(nil)
	if poolDef.WebhookDispatcher() == nil {
		t.Errorf("expected non-nil default dispatcher after SetWebhookDispatcher(nil)")
	}

	teNil := &turnExecution{pool: nil, burst: []db.Message{{ID: "m-nil"}}, policy: config.ChannelPolicy{Mode: "channel", Hooks: config.ChannelHooksConfig{OnWake: &config.WebhookEndpoint{URL: "http://sidecar.local/wake"}}}}
	teNil.markAmbientBurst()
}

func runPreTurnHookBurst(t *testing.T, hookResps []*PreTurnResponse, hookErr error, timeoutAction string, initialRetry int, schedRunID string, waitAsync bool) (int, int, string, *PreTurnRequest, *db.Message, db.Store) {
	t.Helper()
	store, doneCh := setupTestStore(t), make(chan struct{})
	var mu sync.Mutex
	var runnerCalls, hookCalls int
	var capturedPrompt string
	var capturedReq *PreTurnRequest

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, SessionManager: session.New(t.TempDir(), ""), TimeoutMinutes: 1, MaxAttempts: 3,
		RetryDelayOverride: 10 * time.Millisecond, DeliveryFunc: noopDelivery,
		WebhookDispatcher: &mockWebhookDispatcher{callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
			mu.Lock()
			hookCalls++
			cnt := hookCalls
			capturedReq = req
			mu.Unlock()
			if hookErr != nil {
				return nil, hookErr
			}
			idx := min(cnt-1, len(hookResps)-1)
			return hookResps[idx], nil
		}},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			capturedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse(sessionID, "Turn proceeded"), "", 0, nil
		},
		OnMessageCompleted: func(m db.Message, status string) {
			if m.ID == "msg-preturn-test" {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn", TimeoutMs: 1500, OnTimeoutAction: timeoutAction},
			}}
		},
	})
	now := time.Now().UTC()
	msg := db.Message{ID: "msg-preturn-test", ThreadID: "chan-preturn", AuthorName: "user-alpha", Content: "test prompt for pre-turn", Status: db.StatusPending, RetryCount: initialRetry, CreatedAt: now}
	if schedRunID != "" {
		msg.ScheduleRunID = schedRunID
	}
	_ = insertMessage(store, msg)
	pool.processBurst([]db.Message{msg})
	if waitAsync {
		waitForCompletion(t, doneCh, 5*time.Second, "preturn-wait")
	}

	mu.Lock()
	defer mu.Unlock()
	saved, _ := getMessage(store, "msg-preturn-test")
	return hookCalls, runnerCalls, capturedPrompt, capturedReq, saved, store
}

func TestPreTurnHook_Lifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, timeoutAction, schedRunID, wantStatus, wantErrSub, wantPromptSub string
		hookResps                                                              []*PreTurnResponse
		hookErr                                                                error
		initialRetry, wantHookCalls, wantRunner                                int
		waitAsync                                                              bool
	}{
		{name: "Rejected_DropAction", hookResps: []*PreTurnResponse{{Allow: false, Action: "drop", Reason: "resource busy"}}, wantHookCalls: 1, wantStatus: db.StatusCompleted, wantErrSub: "resource busy"},
		{name: "TimeoutFallback_Drop", hookErr: context.DeadlineExceeded, timeoutAction: "drop", wantHookCalls: 1, wantStatus: db.StatusCompleted, wantErrSub: "pre_turn_timeout_drop"},
		{name: "TimeoutFallback_Proceed", hookErr: errors.New("webhook failed"), timeoutAction: "proceed", wantHookCalls: 1, wantRunner: 1, wantStatus: db.StatusCompleted},
		{name: "TimeoutFallback_Retry", hookErr: errors.New("gateway timeout"), timeoutAction: "retry", wantHookCalls: 1, wantStatus: db.StatusPending},
		{name: "ApprovedWithInjectedContext", hookResps: []*PreTurnResponse{{Allow: true, InjectedContext: "system instructions: be concise", Metadata: map[string]any{"lease_token": "token-123"}}}, wantHookCalls: 1, wantRunner: 1, wantStatus: db.StatusCompleted, wantPromptSub: "<COORDINATION_CONTEXT>\nsystem instructions: be concise\n</COORDINATION_CONTEXT>"},
		{name: "RejectedRetryAction", hookResps: []*PreTurnResponse{{Allow: false, Action: "retry", RetryAfterSeconds: 1, Reason: "sidecar lock wait"}, {Allow: true}}, waitAsync: true, wantHookCalls: 2, wantRunner: 1, wantStatus: db.StatusCompleted},
		{name: "RetryExhaustionWithScheduleRun", hookResps: []*PreTurnResponse{{Allow: false, Action: "retry", RetryAfterSeconds: 1}}, initialRetry: 2, schedRunID: "run-exhaust-1", wantHookCalls: 1, wantRunner: 0, wantStatus: db.StatusFailed, wantErrSub: "[EXHAUSTED_PRE_TURN_RETRIES]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hc, rc, cp, cr, saved, _ := runPreTurnHookBurst(t, tc.hookResps, tc.hookErr, tc.timeoutAction, tc.initialRetry, tc.schedRunID, tc.waitAsync)
			if hc != tc.wantHookCalls || rc != tc.wantRunner {
				t.Fatalf("%s: hookCalls=%d runnerCalls=%d", tc.name, hc, rc)
			}
			if saved == nil || saved.Status != tc.wantStatus {
				t.Fatalf("%s: saved status=%+v, want %s", tc.name, saved, tc.wantStatus)
			}
			if tc.wantErrSub != "" && !strings.Contains(saved.ErrorMessage, tc.wantErrSub) {
				t.Errorf("%s: ErrorMessage=%q, want %q", tc.name, saved.ErrorMessage, tc.wantErrSub)
			}
			if tc.wantPromptSub != "" && !strings.Contains(cp, tc.wantPromptSub) {
				t.Errorf("%s: prompt missing %q", tc.name, tc.wantPromptSub)
			}
			if cr == nil || cr.Timestamp.IsZero() {
				t.Errorf("%s: invalid captured request: %+v", tc.name, cr)
			}
		})
	}
}

func TestPreTurnHook_HelperEdgeCases(t *testing.T) {
	t.Parallel()
	if reqNil := buildPreTurnRequest(nil); reqNil == nil || reqNil.Timestamp.IsZero() {
		t.Errorf("expected valid from nil te")
	}
	msgA, msgB := db.Message{ID: "mA", RetryCount: 1, Content: "promptA"}, db.Message{ID: "mB", RetryCount: 3, Content: "promptB"}
	if req := buildPreTurnRequest(&turnExecution{threadID: "t-fallback", burst: []db.Message{msgA, msgB}}); req.ChannelID != "t-fallback" || req.RetryCount != 3 {
		t.Errorf("req = %+v", req)
	}
	teNoPool := &turnExecution{pool: nil, burst: []db.Message{msgA}}
	teNoPool.rescheduleBurst(0)
	teNoPool.abortBurst("test drop nil pool")

	store, completedStatus := setupTestStore(t), ""
	pool := NewWorkerPool(WorkerPoolConfig{Store: store, OnMessageCompleted: func(msg db.Message, status string) { completedStatus = status }})
	schedMsg := db.Message{ID: "msg-sched-abort", ThreadID: "chan-sched", ScheduleRunID: "run-sched-abort", Status: db.StatusPending}
	_ = insertMessage(store, schedMsg)
	(&turnExecution{pool: pool, burst: []db.Message{schedMsg}, threadID: "chan-sched"}).abortBurst("manual abort")
	if completedStatus != db.StatusCompleted {
		t.Errorf("completedStatus=%s, want COMPLETED", completedStatus)
	}
}

func runPostTurnHookBurst(t *testing.T, preResp *PreTurnResponse, runnerStderr string, runnerErr error, runnerPanic string) *PostTurnRequest {
	t.Helper()
	store := setupTestStore(t)
	var mu sync.Mutex
	var postCalled bool
	var postReqReceived *PostTurnRequest
	expectedUsage := runner.TokenUsage{InputTokens: 100, OutputTokens: 50, ThinkingTokens: 10, CacheReadTokens: 5, TotalTokens: 150}

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				if preResp != nil {
					return preResp, nil
				}
				return &PreTurnResponse{Allow: true}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				mu.Lock()
				postCalled = true
				reqCopy := *req
				postReqReceived = &reqCopy
				mu.Unlock()
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			if runnerPanic != "" {
				panic(runnerPanic)
			}
			if runnerErr != nil {
				return "", runnerStderr, 1, runnerErr
			}
			return mockJSONResponseWithUsage(sessionID, "Clean success response from runner", expectedUsage), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	msg := db.Message{ID: "msg-postturn-test", ThreadID: "chan-postturn-test", AuthorName: "user-alpha", Content: "run postturn test", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()
	if !postCalled || postReqReceived == nil {
		t.Fatalf("expected post_turn hook to be called")
	}
	return postReqReceived
}

func TestPostTurnHook_Lifecycle(t *testing.T) {
	t.Parallel()
	expectedUsage := runner.TokenUsage{InputTokens: 100, OutputTokens: 50, ThinkingTokens: 10, CacheReadTokens: 5, TotalTokens: 150}
	tests := []struct {
		name, runnerStderr, runnerPanic, wantStatus, wantErrSub string
		preResp                                                 *PreTurnResponse
		runnerErr                                               error
		checkMeta                                               bool
	}{
		{name: "SuccessfulTurnWithMetadata", preResp: &PreTurnResponse{Allow: true, Metadata: map[string]any{"lease_id": "lease-abc", "turn_epoch": float64(42)}}, wantStatus: "success", checkMeta: true},
		{name: "FailedTurn_AgyError", runnerStderr: "non-transient runner failed: syntax error in script", runnerErr: fmt.Errorf("exit status 1"), wantStatus: "failed", wantErrSub: "syntax error"},
		{name: "RunnerPanicRecovery", runnerPanic: "simulated runner crash inside runner function", wantStatus: "failed", wantErrSub: "simulated runner crash"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := runPostTurnHookBurst(t, tc.preResp, tc.runnerStderr, tc.runnerErr, tc.runnerPanic)
			if req.ChannelID != "chan-postturn-test" || req.ThreadID != "chan-postturn-test" || req.DurationMs < 0 {
				t.Errorf("postReq metadata: %+v", req)
			}
			if req.Status != tc.wantStatus {
				t.Errorf("Status=%q, want %q", req.Status, tc.wantStatus)
			}
			if tc.checkMeta && (req.ResponseText != "Clean success response from runner" || req.TokenUsage != expectedUsage || req.Metadata["lease_id"] != "lease-abc") {
				t.Errorf("payload mismatch: resp=%q usage=%+v meta=%+v", req.ResponseText, req.TokenUsage, req.Metadata)
			}
			if tc.wantErrSub != "" && !strings.Contains(req.Error, tc.wantErrSub) {
				t.Errorf("Error=%q, want %q", req.Error, tc.wantErrSub)
			}
		})
	}
}

func TestPostTurnHook_SerializedRelease(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var mu sync.Mutex
	turn1PostStarted, turn2Launched := make(chan struct{}), make(chan struct{})
	turn1PostFinished, turn2PreCalledWhileTurn1InPost, turn2PreCalled := false, false, false

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				mu.Lock()
				if req.ThreadID == "chan-serialized" && len(req.Messages) > 0 && req.Messages[0].ID == "msg-turn-2" {
					turn2PreCalled = true
					if !turn1PostFinished {
						turn2PreCalledWhileTurn1InPost = true
					}
				}
				mu.Unlock()
				return &PreTurnResponse{Allow: true}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				if req.ThreadID == "chan-serialized" {
					select {
					case <-turn1PostStarted:
					default:
						close(turn1PostStarted)
						<-turn2Launched
						for i := 0; i < 64; i++ {
							runtime.Gosched()
						}
						mu.Lock()
						turn1PostFinished = true
						mu.Unlock()
					}
				}
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse(sessionID, "ok"), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})

	now := time.Now().UTC()
	msg1, msg2 := db.Message{ID: "msg-turn-1", ThreadID: "chan-serialized", AuthorName: "user-one", Content: "turn 1", Status: db.StatusPending, CreatedAt: now}, db.Message{ID: "msg-turn-2", ThreadID: "chan-serialized", AuthorName: "user-two", Content: "turn 2", Status: db.StatusPending, CreatedAt: now.Add(time.Millisecond)}
	_ = insertMessage(store, msg1)
	_ = insertMessage(store, msg2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		pool.processBurst([]db.Message{msg1})
	}()
	<-turn1PostStarted
	go func() {
		defer wg.Done()
		close(turn2Launched)
		pool.processBurst([]db.Message{msg2})
	}()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if !turn2PreCalled || turn2PreCalledWhileTurn1InPost {
		t.Errorf("turn2PreCalled=%v, turn2PreCalledWhileTurn1InPost=%v", turn2PreCalled, turn2PreCalledWhileTurn1InPost)
	}
}

func TestPostTurnHook_HelperEdgeCases(t *testing.T) {
	t.Parallel()
	if reqNil := buildPostTurnRequest(nil); reqNil == nil || reqNil.Timestamp.IsZero() {
		t.Errorf("expected valid from nil te")
	}
	reqFallback := buildPostTurnRequest(&turnExecution{
		threadID: "t-fallback", turnStatus: "success", turnResponseText: "fallback response",
		execStart: time.Now().UTC().Add(-50 * time.Millisecond), hookMetadata: map[string]any{"key": "val"},
	})
	if reqFallback.ChannelID != "t-fallback" || reqFallback.DurationMs <= 0 || reqFallback.Metadata["key"] != "val" {
		t.Errorf("reqFallback = %+v", reqFallback)
	}
	if reqEmpty := buildPostTurnRequest(&turnExecution{threadID: "t-empty"}); reqEmpty.Status != "failed" {
		t.Errorf("default status failed, got %s", reqEmpty.Status)
	}

	store := setupTestStore(t)
	var postHookFired bool
	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				return &PreTurnResponse{Allow: false, Action: "drop", Reason: "denied"}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				postHookFired = true
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	msgDrop := db.Message{ID: "msg-preturn-dropped", ThreadID: "chan-preturn-dropped", AuthorName: "user-drop", Content: "drop me", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msgDrop)
	pool.processBurst([]db.Message{msgDrop})
	if postHookFired {
		t.Errorf("post_turn hook must NOT fire when pre_turn dropped turn")
	}
}

func TestWorkerPool_MultiTurn_AlwaysDeliversFinalOutputText(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, convID, msgID, threadID, transcriptContent, runnerResponse string
	}{
		{name: "AlwaysDeliversFinalOutputText", convID: "c2222222-2222-3333-4444-555555555555", msgID: "msg-delivered-1", threadID: "thread-delivered-1", transcriptContent: "{\"step_index\":0,\"source\":\"USER_EXPLICIT\",\"type\":\"USER_INPUT\",\"content\":\"Check sync\"}\n{\"step_index\":1,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"Checking status now...\",\"tool_calls\":[{\"name\":\"status\"}]}\n{\"step_index\":2,\"source\":\"SYSTEM\",\"type\":\"USER_INPUT\",\"content\":\"All in sync\"}\n{\"step_index\":3,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"Wait for background task task-645 to complete.\"}\n", runnerResponse: "Wait for background task task-645 to complete."},
		{name: "TranscriptSilent_PreservesSubstantiveRunnerResponse", convID: "c3333333-3333-4444-5555-666666666666", msgID: "msg-substantive-fallback-1", threadID: "thread-substantive-fallback-1", transcriptContent: "{\"step_index\":0,\"source\":\"USER_EXPLICIT\",\"type\":\"USER_INPUT\",\"content\":\"Check deployment\"}\n{\"step_index\":1,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"Checking status now...\",\"tool_calls\":[{\"name\":\"status\"}]}\n{\"step_index\":2,\"source\":\"SYSTEM\",\"type\":\"USER_INPUT\",\"content\":\"All containers deployed\"}\n{\"step_index\":3,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"   \"}\n", runnerResponse: "Continuous Deployment for PR #322 has completed successfully!"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tempDir := setupTestStore(t), t.TempDir()
			sessDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", tc.convID, ".system_generated", "logs")
			_ = os.MkdirAll(sessDir, 0o755)
			_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(tc.transcriptContent), 0o644)

			var deliveredText string
			var mu sync.Mutex
			doneCh := make(chan struct{})
			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: session.New(tempDir, ""), Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return mockMultiTurnJSONResponse(tc.convID, tc.runnerResponse, 2), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredText = text
					mu.Unlock()
					return nil
				},
				TypingFunc:         noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) { close(doneCh) },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Check", Status: db.StatusPending, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)
			waitForCompletion(t, doneCh, 3*time.Second, tc.name)

			mu.Lock()
			defer mu.Unlock()
			if deliveredText != tc.runnerResponse {
				t.Errorf("deliveredText = %q, want %q", deliveredText, tc.runnerResponse)
			}
			if dbMsg, err := getMessage(store, tc.msgID); err != nil || dbMsg == nil || dbMsg.Status != db.StatusCompleted || dbMsg.ResponseText != tc.runnerResponse {
				t.Errorf("DB response mismatch: %+v (err %v)", dbMsg, err)
			}
		})
	}
}

type sessionRotationCase struct {
	name, mode, threadID                                                                string
	saveInitialSession, useWatchdog, checkStoreSession                                  bool
	seedTurns, preTranscriptSteps, preTranscriptBytes, postTranscriptSteps              int
	postTranscriptBytes, watchdogTranscriptSteps, watchdogTranscriptBytes, wantAttempts int
}

func runSessionRotationCase(t *testing.T, tc sessionRotationCase) {
	t.Helper()
	t.Parallel()
	store, tmpDir := setupTestStore(t), t.TempDir()
	sessMgr := session.New(tmpDir, "")

	sessionID := "sess-" + strings.ToLower(tc.name)
	sessDir, _ := ensureTestSessionDir(sessMgr, sessionID)
	tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")

	if tc.saveInitialSession {
		_ = saveSessionID(store, tc.threadID, sessionID)
	}
	for i := 0; i < tc.seedTurns; i++ {
		_, _ = incrementSessionTurnCount(store, tc.threadID)
	}
	if tc.preTranscriptSteps > 0 {
		_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.preTranscriptSteps)), 0o644)
	}
	if tc.preTranscriptBytes > 0 {
		_ = os.WriteFile(tPath, bytes.Repeat([]byte("A"), tc.preTranscriptBytes), 0o644)
	}

	var mu sync.Mutex
	var attempts []string
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: tc.mode}}})
	poolCfg := WorkerPoolConfig{
		SessionManager: sessMgr, Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond, MaxAttempts: 2,
		DeliveryFunc: noopDelivery, TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		},
	}

	if tc.useWatchdog {
		poolCfg.RunnerWithOptionsFunc = func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			attempts = append(attempts, sID)
			curAttempt := len(attempts)
			mu.Unlock()
			if curAttempt == 1 {
				if tc.watchdogTranscriptSteps > 0 {
					_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.watchdogTranscriptSteps)), 0o644)
				}
				if tc.watchdogTranscriptBytes > 0 {
					_ = os.WriteFile(tPath, make([]byte, tc.watchdogTranscriptBytes), 0o644)
				}
				return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
			}
			return mockJSONResponse(sID, "Recovered on warm retry"), "", 0, nil
		}
	} else {
		poolCfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			attempts = append(attempts, sID)
			mu.Unlock()
			if tc.postTranscriptSteps > 0 {
				_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.postTranscriptSteps)), 0o644)
			}
			if tc.postTranscriptBytes > 0 {
				_ = os.WriteFile(tPath, make([]byte, tc.postTranscriptBytes), 0o644)
			}
			retID := sID
			if retID == "" {
				retID = sessionID
			}
			return mockJSONResponse(retID, "OK"), "", 0, nil
		}
	}

	pool := New(appCfg, poolCfg)
	pool.Start()
	defer pool.Stop()
	msg := db.Message{ID: "msg-rot-" + tc.name, ThreadID: tc.threadID, Content: "<@aerial> run"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)
	waitForCompletion(t, doneCh, 3*time.Second, tc.name)

	mu.Lock()
	attCopy := append([]string(nil), attempts...)
	mu.Unlock()
	if tc.wantAttempts > 0 {
		if len(attCopy) != tc.wantAttempts {
			t.Fatalf("len(attempts) = %d (%v), want %d", len(attCopy), attCopy, tc.wantAttempts)
		}
		for i, got := range attCopy {
			if got != sessionID {
				t.Errorf("attempts[%d] = %q, want %q", i, got, sessionID)
			}
		}
	}
	if tc.checkStoreSession {
		if curSess, _ := getSessionID(store, tc.threadID); curSess != sessionID {
			t.Errorf("store session = %q, want %q", curSess, sessionID)
		}
	}
}

func TestSession_RotationThresholds(t *testing.T) {
	t.Parallel()
	tests := []sessionRotationCase{
		{name: "TurnLimit_PreFlight", mode: "thread", threadID: "thread-rotate-test", saveInitialSession: true, seedTurns: 10, wantAttempts: 1, checkStoreSession: true},
		{name: "StepLimit_PreFlight", mode: "thread", threadID: "thread-rot-threshold", saveInitialSession: true, seedTurns: 1, preTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 1, checkStoreSession: true},
		{name: "ByteLimit_PreFlight", mode: "thread", threadID: "thread-rot-threshold", saveInitialSession: true, seedTurns: 1, preTranscriptBytes: session.DefaultMaxTranscriptBytes + 1024, wantAttempts: 1, checkStoreSession: true},
		{name: "StepLimit_PostExecution", mode: "thread", threadID: "thread-rot-threshold", postTranscriptSteps: session.DefaultMaxSessionSteps, checkStoreSession: true},
		{name: "WatchdogCircuitBreaker_OnBloat", mode: "thread", threadID: "thread-rot-threshold", saveInitialSession: true, useWatchdog: true, watchdogTranscriptBytes: session.DefaultMaxTranscriptBytes + 2048, wantAttempts: 2},
		{name: "ChannelMode_PreFlight_ByteLimit", mode: "channel", threadID: "chan-byte-test", saveInitialSession: true, seedTurns: 1, preTranscriptBytes: session.DefaultMaxTranscriptBytes + 1024, wantAttempts: 1},
		{name: "ChannelMode_PreFlight_StepLimit", mode: "channel", threadID: "chan-step-test", saveInitialSession: true, seedTurns: 1, preTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 1},
		{name: "PostExecution_ByteLimit", mode: "thread", threadID: "thread-post-byte-test", saveInitialSession: true, seedTurns: 1, postTranscriptBytes: session.DefaultMaxTranscriptBytes + 500, checkStoreSession: true},
		{name: "Watchdog_StepLimit", mode: "thread", threadID: "thread-wd-step", saveInitialSession: true, seedTurns: 1, useWatchdog: true, watchdogTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runSessionRotationCase(t, tc) })
	}
}

func TestWorkerPool_DeliverDirect(t *testing.T) {
	t.Parallel()
	dummySession := &discordgo.Session{}
	customErr := errors.New("custom delivery error")
	tests := []struct {
		name, channelID, text, expectErrContains string
		nilPool, nilSession                      bool
		funcErr, expectErr                       error
	}{
		{name: "nil pool returns error", nilPool: true, channelID: "123", text: "hello", expectErrContains: "worker pool is nil"},
		{name: "nil session with nil DeliveryFunc returns error", nilSession: true, channelID: "123", text: "hello", expectErrContains: "discord session is not connected"},
		{name: "injected DeliveryFunc invoked", channelID: "123", text: "test message"},
		{name: "session error propagation", funcErr: customErr, channelID: "123", text: "test message", expectErr: customErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilPool || tc.nilSession {
				var p *WorkerPool
				if !tc.nilPool {
					p = &WorkerPool{}
				}
				if err := p.DeliverDirect(tc.channelID, tc.text); err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}
			var gotChan, gotText string
			var gotSess *discordgo.Session
			p := &WorkerPool{cfg: WorkerPoolConfig{
				DiscordSession: dummySession,
				DeliveryFunc: func(s *discordgo.Session, cID, txt string) error {
					gotSess, gotChan, gotText = s, cID, txt
					return tc.funcErr
				},
			}}
			err := p.DeliverDirect(tc.channelID, tc.text)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil || gotSess != dummySession || gotChan != tc.channelID || gotText != tc.text {
				t.Fatalf("unexpected delivery: err=%v chan=%q text=%q", err, gotChan, gotText)
			}
		})
	}
}

func TestWorkerPool_DeliverStatusWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, widgetMsgID, expectErrContains, expectMsgID string
		nilPool, nilSession                               bool
		req                                               discord.StatusWidgetRequest
	}{
		{name: "nil pool returns error", nilPool: true, expectErrContains: "worker pool is nil"},
		{name: "nil session returns error", nilSession: true, req: discord.StatusWidgetRequest{ChannelID: "chan-1"}, expectErrContains: "discord session is not connected"},
		{name: "injected WidgetDeliveryFunc is invoked", widgetMsgID: "widget-msg-123", req: discord.StatusWidgetRequest{ChannelID: "chan-1"}, expectMsgID: "widget-msg-123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilPool || tc.nilSession {
				var p *WorkerPool
				if !tc.nilPool {
					p = &WorkerPool{}
				}
				if _, err := p.DeliverStatusWidget(tc.req); err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}
			p := &WorkerPool{}
			p.SetWidgetDeliveryFunc(func(req discord.StatusWidgetRequest) (string, error) { return tc.widgetMsgID, nil })
			if msgID, err := p.DeliverStatusWidget(tc.req); err != nil || msgID != tc.expectMsgID {
				t.Fatalf("unexpected delivery: err=%v msgID=%q", err, msgID)
			}
		})
	}
}

func TestWorkerPool_ProcessPoolWiringAndTurnRouting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                                                                                                     string
		nilPool, defaultConfig, withProcessPool, withVoicePool, checkAutoLLM, interchangeable, cleanAllThree, reconcilePrewarmed bool
		prewarmedTargets, expectedVoiceTargets                                                                                   []string
	}{
		{name: "nil pool returns nil pools", nilPool: true},
		{name: "default config has nil pools", defaultConfig: true},
		{name: "configured process pool populates and accesses", withProcessPool: true, prewarmedTargets: []string{"kiosk", "ephemeral:classifier", "ephemeral:summarizer"}},
		{name: "auto populates LLMFunc from process pool", withProcessPool: true, checkAutoLLM: true},
		{name: "voice process pool wiring and accessors", withProcessPool: true, withVoicePool: true},
		{name: "interchangeable pool returns underlying pool", interchangeable: true},
		{name: "mark dirty and stop cleans all three pools", cleanAllThree: true},
		{name: "mark dirty reconciles voice prewarmed targets", reconcilePrewarmed: true, prewarmedTargets: []string{"kiosk"}, expectedVoiceTargets: []string{"touch-kiosk-kitchen"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilPool {
				var p *WorkerPool
				if p.ProcessPool() != nil || p.LowEffortProcessPool() != nil || p.MediumEffortProcessPool() != nil || p.StatefulProcessPools() != nil {
					t.Fatalf("expected nil pools from nil WorkerPool")
				}
				p.MarkDirty()
				return
			}
			if tc.defaultConfig {
				p := New(nil, WorkerPoolConfig{})
				if p.ProcessPool() != nil || p.LowEffortProcessPool() != nil || p.MediumEffortProcessPool() != nil {
					t.Fatalf("expected nil pools by default")
				}
				return
			}
			if tc.interchangeable {
				spawner := newMockDaemonSpawner()
				u1 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, spawner)
				defer func() { _ = u1.Close() }()
				i1 := runner.NewInterchangeablePool(u1, runner.InterchangeablePoolConfig{WorkerCount: 2})
				defer func() { _ = i1.Close() }()
				u2 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-medium"}, spawner)
				defer func() { _ = u2.Close() }()
				i2 := runner.NewInterchangeablePool(u2, runner.InterchangeablePoolConfig{WorkerCount: 2})
				defer func() { _ = i2.Close() }()
				p := New(nil, WorkerPoolConfig{LowEffortProcessPool: i1, MediumEffortProcessPool: i2})
				defer p.Stop()
				if p.LowEffortProcessPool() != u1 || p.MediumEffortProcessPool() != u2 {
					t.Fatalf("expected underlying pools returned")
				}
				return
			}
			if tc.cleanAllThree {
				pPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, newMockSpawner())
				defer func() { _ = pPool.Close() }()
				lSpawner := newMockSpawner()
				lPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lSpawner)
				defer func() { _ = lPool.Close() }()
				vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash"}, newMockSpawner())
				defer func() { _ = vPool.Close() }()
				pool := NewWorkerPool(WorkerPoolConfig{ProcessPool: pPool, LowEffortProcessPool: lPool, VoiceProcessPool: vPool})
				if len(pool.StatefulProcessPools()) != 1 {
					t.Fatalf("expected 1 stateful pool")
				}
				_, _ = pool.cfg.LLMFunc(context.Background(), "", "summarize")
				if got := lSpawner.LastSpawnCfg().Model; got != "gemini-3.8-flash-low" {
					t.Fatalf("expected LowEffortProcessPool model, got %q", got)
				}
				pool.MarkDirty()
				pool.Stop()
				return
			}
			if tc.reconcilePrewarmed {
				vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash", PrewarmedTargets: tc.prewarmedTargets}, newMockDaemonSpawner())
				defer func() { _ = vPool.Close() }()
				appCfg := newTestConfig(func(d *config.ConfigData) { d.Voice.PrewarmedTargets = tc.expectedVoiceTargets })
				pool := New(appCfg, WorkerPoolConfig{VoiceProcessPool: vPool})
				defer pool.Stop()
				pool.MarkDirty()
				_, _ = vPool.GetOrCreate(context.Background(), tc.expectedVoiceTargets[0], "")
				targets := vPool.PrewarmedTargets()
				_, hasDaemon := vPool.Get(tc.expectedVoiceTargets[0])
				if len(targets) != len(tc.expectedVoiceTargets) || targets[0] != tc.expectedVoiceTargets[0] || !hasDaemon {
					t.Fatalf("prewarmed targets mismatch: %v", targets)
				}
				return
			}
			mockSpawner := newMockDaemonSpawner()
			var unifiedPool, voicePool runner.AgentPool
			if tc.withProcessPool {
				up := runner.NewUnifiedProcessPool(runner.PoolConfig{PrewarmedTargets: tc.prewarmedTargets, DefaultModel: "gemini-2.5-flash"}, mockSpawner)
				defer func() { _ = up.Close() }()
				unifiedPool = up
			}
			if tc.withVoicePool {
				vp := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
				defer func() { _ = vp.Close() }()
				voicePool = vp
			}
			p := New(nil, WorkerPoolConfig{ProcessPool: unifiedPool, VoiceProcessPool: voicePool})
			defer p.Stop()

			if tc.withProcessPool && p.ProcessPool() != unifiedPool {
				t.Fatalf("expected ProcessPool %p, got %p", unifiedPool, p.ProcessPool())
			}
			if tc.checkAutoLLM && p.cfg.LLMFunc == nil {
				t.Fatalf("expected cfg.LLMFunc to be auto-populated")
			}
			if tc.withVoicePool {
				p.MarkDirty()
			}
		})
	}
}

type spyPoolSessionDeps struct {
	runner.AgentPool
	callbacksSet bool
	managerSet   bool
}

func (s *spyPoolSessionDeps) SetSessionCallbacks(getRec func(context.Context, string) (runner.SessionRecord, error), onRot func(context.Context, string, string, string) error) {
	s.callbacksSet = true
}

func (s *spyPoolSessionDeps) SetSessionManager(sm *session.Manager) {
	s.managerSet = true
}

func (s *spyPoolSessionDeps) SetTranscriptRescuer(r func(string, time.Time) string) {}

func TestWorkerPool_WirePoolSessionDependencies_TableDriven(t *testing.T) {
	t.Parallel()
	sessMgr := &session.Manager{}
	fakeStore := dbtest.NewFakeStore()

	tests := []struct {
		name                   string
		hasStore               bool
		wantStatefulCallbacks  bool
		wantEphemeralCallbacks bool
		wantMgrWired           bool
	}{
		{
			name:                   "stateful pools receive callbacks while ephemeral pools do not when store provided",
			hasStore:               true,
			wantStatefulCallbacks:  true,
			wantEphemeralCallbacks: false,
			wantMgrWired:           true,
		},
		{
			name:                   "nil store wires manager to all pools but not callbacks",
			hasStore:               false,
			wantStatefulCallbacks:  false,
			wantEphemeralCallbacks: false,
			wantMgrWired:           true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			primary := &spyPoolSessionDeps{}
			statefulLow := &spyPoolSessionDeps{}
			ephemLow := &spyPoolSessionDeps{}

			var store db.Store
			if tc.hasStore {
				store = fakeStore
			}

			pools := map[PoolKey]runner.AgentPool{
				{Ephemeral: false, Effort: "high"}: primary,
				{Ephemeral: false, Effort: "low"}:  statefulLow,
				{Ephemeral: true, Effort: "low"}:   ephemLow,
			}
			wirePoolSessionDependencies(pools, sessMgr, store)

			if primary.callbacksSet != tc.wantStatefulCallbacks || statefulLow.callbacksSet != tc.wantStatefulCallbacks {
				t.Errorf("stateful callbacksSet = (primary:%v, statefulLow:%v), want %v", primary.callbacksSet, statefulLow.callbacksSet, tc.wantStatefulCallbacks)
			}
			if ephemLow.callbacksSet != tc.wantEphemeralCallbacks {
				t.Errorf("ephemLow.callbacksSet = %v, want %v", ephemLow.callbacksSet, tc.wantEphemeralCallbacks)
			}
			if primary.managerSet != tc.wantMgrWired || statefulLow.managerSet != tc.wantMgrWired || ephemLow.managerSet != tc.wantMgrWired {
				t.Errorf("managerSet = (primary:%v, statefulLow:%v, ephemLow:%v), want %v", primary.managerSet, statefulLow.managerSet, ephemLow.managerSet, tc.wantMgrWired)
			}
		})
	}
}

type mockErrSessionStore struct {
	db.Store
	findErr error
}

func (m *mockErrSessionStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]db.SessionInfo, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}
	if m.Store != nil {
		return m.Store.FindUnrotatedSessions(ctx, minTurns, maxAge...)
	}
	return nil, nil
}

func TestWorkerPool_CheckUnrotatedSessions_Scenarios(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                string
		nilPool, nilStore, cancelContext    bool
		storeErr                            error
		dormantBackdate                     time.Duration
		sessionTurns                        []int
		expectAlertCalls, expectReportCount int
		expectOverflowMsg                   string
	}{
		{name: "nil worker pool", nilPool: true},
		{name: "nil store", nilStore: true},
		{name: "store error handled gracefully", storeErr: errors.New("database connection failed")},
		{name: "cancelled context exits early", cancelContext: true},
		{name: "excludes dormant sessions older than horizon", sessionTurns: []int{15}, dormantBackdate: 3 * time.Hour},
		{name: "top 5 display cap with overflow message", sessionTurns: []int{10, 11, 12, 13, 14, 15, 16}, expectAlertCalls: 1, expectReportCount: 7, expectOverflowMsg: "...and 2 more runaway session(s)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilPool {
				var nilPool *WorkerPool
				nilPool.checkUnrotatedSessions(context.Background(), make(map[string]int))
				return
			}
			if tc.nilStore {
				(&WorkerPool{}).checkUnrotatedSessions(context.Background(), make(map[string]int))
				return
			}
			ctx := context.Background()
			if tc.cancelContext {
				cCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cCtx
			}
			var mu sync.Mutex
			var alertCalls int
			var lastBody string
			recordAlert := func(s *discordgo.Session, ch, title, body string) error {
				mu.Lock()
				alertCalls++
				lastBody = body
				mu.Unlock()
				return nil
			}

			var store db.Store
			if tc.storeErr != nil {
				store = &mockErrSessionStore{findErr: tc.storeErr}
			} else {
				fake := dbtest.NewFakeStore()
				defer func() { _ = fake.Close() }()
				store = fake
				for i, turns := range tc.sessionTurns {
					thID := fmt.Sprintf("th-scen-%d", i+1)
					_ = fake.SaveSessionID(context.Background(), thID, fmt.Sprintf("sess-scen-%d", i+1))
					for j := 0; j < turns; j++ {
						_, _ = fake.IncrementSessionTurnCount(context.Background(), thID)
					}
					if tc.dormantBackdate > 0 {
						fake.SetSessionUpdatedAt(thID, time.Now().UTC().Add(-tc.dormantBackdate))
					}
				}
			}

			pool := &WorkerPool{cfg: WorkerPoolConfig{Store: store, SystemAlertFunc: recordAlert}}
			lastReported := make(map[string]int)
			pool.checkUnrotatedSessions(ctx, lastReported)

			mu.Lock()
			calls, body := alertCalls, lastBody
			mu.Unlock()
			if calls != tc.expectAlertCalls {
				t.Fatalf("expected %d alert calls, got %d", tc.expectAlertCalls, calls)
			}
			if tc.expectReportCount > 0 && len(lastReported) != tc.expectReportCount {
				t.Fatalf("expected %d recorded, got %d", tc.expectReportCount, len(lastReported))
			}
			if tc.expectOverflowMsg != "" && !strings.Contains(body, tc.expectOverflowMsg) {
				t.Fatalf("overflow message missing in:\n%s", body)
			}
		})
	}
}

func TestWorkerPool_CheckUnrotatedSessions_DeduplicationAndPruning(t *testing.T) {
	t.Parallel()
	fake := dbtest.NewFakeStore()
	defer func() { _ = fake.Close() }()
	ctx := context.Background()
	var mu sync.Mutex
	var alertCalls int
	var lastTitle, lastBody string
	recordAlert := func(s *discordgo.Session, ch, title, body string) error {
		mu.Lock()
		alertCalls++
		lastTitle, lastBody = title, body
		mu.Unlock()
		return nil
	}
	p := &WorkerPool{cfg: WorkerPoolConfig{Store: fake, SystemAlertFunc: recordAlert}}
	lastReported := make(map[string]int)

	_ = fake.SaveSessionID(ctx, "th-test", "sess-1")
	for i := 0; i < 5; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls := alertCalls
	mu.Unlock()
	if calls != 0 || len(lastReported) != 0 {
		t.Fatalf("expected 0 alerts, got %d", calls)
	}

	for i := 0; i < 4; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls, body, title := alertCalls, lastBody, lastTitle
	mu.Unlock()
	if calls != 1 || !strings.Contains(body, "th-test") || !strings.Contains(body, "**9** turns") || title != "Unrotated Runaway Sessions" {
		t.Fatalf("unexpected alert: calls=%d body=%s title=%s", calls, body, title)
	}
	if lastReported["th-test"] != 9 {
		t.Errorf("expected 9, got %d", lastReported["th-test"])
	}

	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls = alertCalls
	mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls, body = alertCalls, lastBody
	mu.Unlock()
	if calls != 2 || !strings.Contains(body, "**10** turns") {
		t.Fatalf("expected 2 calls, got %d body=%s", calls, body)
	}

	_ = fake.RotateSessionID(ctx, "th-test", "sess-fresh")
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	calls = alertCalls
	mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
	if _, exists := lastReported["th-test"]; exists {
		t.Errorf("expected pruned: %v", lastReported)
	}
}

func TestWorkerPool_MaintenanceTicker_FiresCheck(t *testing.T) {
	t.Parallel()
	fake := dbtest.NewFakeStore()
	defer func() { _ = fake.Close() }()
	ctx := context.Background()
	_ = fake.SaveSessionID(ctx, "th-ticker-test", "sess-ticker")
	for i := 0; i < 11; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-ticker-test")
	}

	alertCh := make(chan string, 5)
	p := NewWorkerPool(WorkerPoolConfig{
		Store: fake, MaintenanceInterval: 10 * time.Millisecond,
		SystemAlertFunc: func(s *discordgo.Session, ch, title, body string) error {
			select {
			case alertCh <- body:
			default:
			}
			return nil
		},
	})
	p.Start()
	select {
	case body := <-alertCh:
		if !strings.Contains(body, "th-ticker-test") || !strings.Contains(body, "**11** turns") {
			t.Errorf("unexpected alert body: %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for maintenance ticker")
	}
	p.StopWithTimeout(time.Second)
}

func TestFormatUnrotatedSessionLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		cachedChans []*discordgo.Channel
		session     db.SessionInfo
		expected    string
	}{
		{name: "non-numeric fallback", session: db.SessionInfo{ThreadID: "th-custom", TurnCount: 9, InternalSessionID: "sess-1"}, expected: "- Thread `th-custom`: **9** turns (session: `sess-1`)"},
		{name: "channel snowflake", cachedChans: []*discordgo.Channel{{ID: "1464358486849622120", Type: discordgo.ChannelTypeGuildText, Name: "general"}}, session: db.SessionInfo{ThreadID: "1464358486849622120", TurnCount: 10, InternalSessionID: "sess-chan"}, expected: "- Channel <#1464358486849622120>: **10** turns (session: `sess-chan`)"},
		{name: "thread with parent", cachedChans: []*discordgo.Channel{{ID: "1542423172400291873", Type: discordgo.ChannelTypeGuildText, Name: "aerial-dev"}, {ID: "1557949887948005398", ParentID: "1542423172400291873", Type: discordgo.ChannelTypeGuildPublicThread, Name: "Thread"}}, session: db.SessionInfo{ThreadID: "1557949887948005398", TurnCount: 12, InternalSessionID: "sess-th"}, expected: "- Thread <#1557949887948005398> (in <#1542423172400291873>): **12** turns (session: `sess-th`)"},
		{name: "orphan thread", cachedChans: []*discordgo.Channel{{ID: "1557949887948005399", Type: discordgo.ChannelTypeGuildPublicThread, Name: "Orphan"}}, session: db.SessionInfo{ThreadID: "1557949887948005399", TurnCount: 15, InternalSessionID: "sess-orph"}, expected: "- Thread <#1557949887948005399>: **15** turns (session: `sess-orph`)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, ch := range tc.cachedChans {
				CacheDiscordChannel(ch)
				defer InvalidateChannelCache(ch.ID)
			}
			if got := formatUnrotatedSessionLine(nil, tc.session); got != tc.expected {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestWorkerPool_Accessors(t *testing.T) {
	t.Parallel()
	fake := setupTestStore(t)
	noopLLM := func(context.Context, string, string) (string, error) { return "", nil }
	tests := []struct {
		name                                                           string
		pool                                                           *WorkerPool
		wantStoreNil, wantSummaryNil, wantAgentPoolNil, wantLLMFuncNil bool
	}{
		{"nil", nil, true, true, true, true},
		{"empty", &WorkerPool{}, true, false, true, true},
		{"with_fake_and_agentpool", &WorkerPool{cfg: WorkerPoolConfig{Store: fake, LLMFunc: noopLLM}, pools: map[PoolKey]runner.AgentPool{{Ephemeral: false, Effort: "high"}: &testRunnerAgentPool{}}}, false, false, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if (tc.pool.Store() == nil) != tc.wantStoreNil || (tc.pool.SummaryGroup() == nil) != tc.wantSummaryNil || (tc.pool.AgentPool() == nil) != tc.wantAgentPoolNil || (tc.pool.LLMFunc() == nil) != tc.wantLLMFuncNil {
				t.Errorf("accessor mismatch: store=%v summary=%v agent=%v llm=%v", tc.pool.Store() == nil, tc.pool.SummaryGroup() == nil, tc.pool.AgentPool() == nil, tc.pool.LLMFunc() == nil)
			}
		})
	}
}

func TestWorkerPool_ClassifierAlertDebouncing(t *testing.T) {
	t.Parallel()
	appCfg := config.NewFromData(&config.ConfigData{SystemChannel: "alerts", APIKey: "k", AgyBin: "agy"})
	dg, _ := discordgo.New("Bot fake")

	tests := []struct {
		name, model, rawOutput, endpoint, wantParseSubstr, wantSysSubstr string
	}{
		{"TruncatedAndSanitizedOutput", "gemini-flash", strings.Repeat("x", 700) + "\n```json\n@everyone @here\n```", "http://192.168.1.70:8000/v1/systemone", "... (truncated)", "http://192.168.1.70:8000/v1/systemone"},
		{"ShortOutputAndMentions", "gemini-pro", "```json\n@everyone @here\n```", "http://127.0.0.1:8000/v1/systemone", "'''json", "http://127.0.0.1:8000/v1/systemone"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var alerts []string
			alertCh := make(chan struct{}, 4)
			pool := New(appCfg, WorkerPoolConfig{
				DiscordSession: dg,
				SystemAlertFunc: func(s *discordgo.Session, ch, title, text string) error {
					mu.Lock()
					alerts = append(alerts, text)
					mu.Unlock()
					alertCh <- struct{}{}
					return nil
				},
			})

			pool.cfg.Classifier.OnParseError(tc.model, tc.rawOutput, errors.New("parse 1"))
			waitForCompletion(t, alertCh, time.Second, tc.name+"/parse")
			pool.cfg.Classifier.OnParseError(tc.model, "duplicate", errors.New("parse 2"))

			pool.cfg.Classifier.OnSystemAlert(tc.endpoint, errors.New("refused 1"))
			waitForCompletion(t, alertCh, time.Second, tc.name+"/sys")
			pool.cfg.Classifier.OnSystemAlert(tc.endpoint, errors.New("refused 2"))

			mu.Lock()
			defer mu.Unlock()
			if len(alerts) != 2 {
				t.Fatalf("expected 2 debounced alerts, got %d", len(alerts))
			}
			if !strings.Contains(alerts[0], tc.wantParseSubstr) {
				t.Errorf("parse alert missing %q", tc.wantParseSubstr)
			}
			if !strings.Contains(alerts[1], tc.wantSysSubstr) {
				t.Errorf("sys alert missing %q", tc.wantSysSubstr)
			}
		})
	}
}

func TestWorkerPool_ResolveChannelID(t *testing.T) {
	t.Parallel()
	mockSess, _ := discordgo.New("Bot fake-token")
	mockSess.State = discordgo.NewState()
	mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "1564150557705248879", Name: "prospera"},
		},
	})
	pool := New(nil, WorkerPoolConfig{DiscordSession: mockSess})
	nilSessPool := New(nil, WorkerPoolConfig{})

	tests := []struct {
		name       string
		pool       *WorkerPool
		input      string
		want       string
		wantErr    bool
		wantErrSub string
	}{
		{name: "nil pool with snowflake", pool: nil, input: "1564150557705248879", want: "1564150557705248879"},
		{name: "nil pool with hash snowflake", pool: nil, input: "#1564150557705248879", want: "1564150557705248879"},
		{name: "nil pool with name", pool: nil, input: "prospera", wantErr: true, wantErrSub: "worker pool is nil"},
		{name: "direct numeric snowflake", pool: pool, input: "1564150557705248879", want: "1564150557705248879"},
		{name: "hash numeric snowflake", pool: pool, input: "#1564150557705248879", want: "1564150557705248879"},
		{name: "whitespace padded snowflake", pool: pool, input: "  1564150557705248879  ", want: "1564150557705248879"},
		{name: "channel name resolved", pool: pool, input: "prospera", want: "1564150557705248879"},
		{name: "hash channel name resolved", pool: pool, input: "#prospera", want: "1564150557705248879"},
		{name: "nil discord session error", pool: nilSessPool, input: "prospera", wantErr: true, wantErrSub: "discord session is nil"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.pool.ResolveChannelID(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveChannelID(%q) error = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if tc.wantErrSub != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSub)) {
				t.Fatalf("ResolveChannelID(%q) error = %v, want error containing %q", tc.input, err, tc.wantErrSub)
			}
			if got != tc.want {
				t.Fatalf("ResolveChannelID(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestWorkerPool_DeliverDirectWithAttachments(t *testing.T) {
	t.Parallel()
	mockPool := New(nil, WorkerPoolConfig{
		DeliveryWithAttachmentsFunc: func(s *discordgo.Session, channelID, text string, atts []*delivery.Attachment) error {
			if channelID == "error-channel" {
				return errors.New("delivery failed")
			}
			return nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			if channelID == "error-channel" {
				return errors.New("direct delivery failed")
			}
			return nil
		},
	})
	nilHandlerPool := &WorkerPool{}
	nilSessWithHandlerPool := &WorkerPool{
		cfg: WorkerPoolConfig{
			DeliveryWithAttachmentsFunc: nil,
		},
	}

	att := &delivery.Attachment{Filename: "audio.wav", ContentType: "audio/wav", Data: []byte("wavdata")}

	tests := []struct {
		name        string
		pool        *WorkerPool
		channelID   string
		text        string
		attachments []*delivery.Attachment
		wantErr     bool
		wantErrSub  string
	}{
		{name: "nil pool with attachments", pool: nil, channelID: "1564150557705248879", text: "hello", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "worker pool is nil"},
		{name: "nil pool with zero attachments delegates to DeliverDirect", pool: nil, channelID: "1564150557705248879", text: "hello", attachments: nil, wantErr: true, wantErrSub: "worker pool is nil"},
		{name: "zero attachments delegates to DeliverDirect success", pool: mockPool, channelID: "1564150557705248879", text: "text only", attachments: nil, wantErr: false},
		{name: "zero attachments delegates to DeliverDirect error", pool: mockPool, channelID: "error-channel", text: "text only", attachments: nil, wantErr: true, wantErrSub: "direct delivery failed"},
		{name: "valid delivery with attachments success", pool: mockPool, channelID: "1564150557705248879", text: "with audio", attachments: []*delivery.Attachment{att}, wantErr: false},
		{name: "valid delivery with attachments error", pool: mockPool, channelID: "error-channel", text: "with audio", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "delivery failed"},
		{name: "nil discord session and nil handler returns error", pool: nilHandlerPool, channelID: "1564150557705248879", text: "hello", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "discord session is not connected"},
		{name: "nil discord session with nil handler on struct", pool: nilSessWithHandlerPool, channelID: "1564150557705248879", text: "hello", attachments: []*delivery.Attachment{att}, wantErr: true, wantErrSub: "discord session is not connected"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.pool.DeliverDirectWithAttachments(tc.channelID, tc.text, tc.attachments)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeliverDirectWithAttachments error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErrSub != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSub)) {
				t.Fatalf("DeliverDirectWithAttachments error = %v, want error containing %q", err, tc.wantErrSub)
			}
		})
	}
}

func TestWorkerPool_ActiveTurnSessionID(t *testing.T) {
	t.Parallel()
	leaseWithSess := &testSessionLease{sessionID: "lease-ephem-sess-1"}
	emptyLease := &testSessionLease{sessionID: ""}

	tests := []struct {
		name       string
		nilPool    bool
		msgID      string
		activeTurn *turnExecution
		wantSess   string
		wantOK     bool
	}{
		{name: "nil pool", nilPool: true, msgID: "msg-1", wantSess: "", wantOK: false},
		{name: "empty msgID", msgID: "", wantSess: "", wantOK: false},
		{name: "unmatched msgID", msgID: "msg-missing", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-other"}}, activeSessionID: "sess-1"}, wantSess: "", wantOK: false},
		{name: "matched activeSessionID priority", msgID: "msg-1", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-1"}}, activeSessionID: "active-sess-1", lease: leaseWithSess}, wantSess: "active-sess-1", wantOK: true},
		{name: "matched ephemeral lease session ID", msgID: "msg-2", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-2"}}, lease: leaseWithSess, isEphemeral: true, currentSessionID: "parent-sess-ignored"}, wantSess: "lease-ephem-sess-1", wantOK: true},
		{name: "matched stateful currentSessionID fallback", msgID: "msg-3", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-3"}}, lease: emptyLease, isEphemeral: false, currentSessionID: "stateful-sess-1"}, wantSess: "stateful-sess-1", wantOK: true},
		{name: "matched ephemeral pre-lease suppresses parent session", msgID: "msg-4", activeTurn: &turnExecution{burst: []db.Message{{ID: "msg-4"}}, lease: emptyLease, isEphemeral: true, currentSessionID: "parent-sess-ignored"}, wantSess: "", wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p *WorkerPool
			if !tc.nilPool {
				p = New(nil, WorkerPoolConfig{})
				if tc.activeTurn != nil {
					p.registerActiveTurn("scope-1", tc.activeTurn)
				}
			}
			gotSess, gotOK := p.ActiveTurnSessionID(tc.msgID)
			if gotSess != tc.wantSess || gotOK != tc.wantOK {
				t.Fatalf("ActiveTurnSessionID(%q) = (%q, %v), want (%q, %v)", tc.msgID, gotSess, gotOK, tc.wantSess, tc.wantOK)
			}
		})
	}
}
