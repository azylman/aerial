package queue

import (
	"bytes"
	"context"
	"image"
	"image/png"
	_ "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/notifier"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

func TestWorkerPool_ConfigInjection(t *testing.T) {
	t.Parallel()
	appCfg := config.NewFromData(&config.ConfigData{
		Model:         "initial-model",
		SystemChannel: "initial-alerts",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})
	pool := New(appCfg, WorkerPoolConfig{
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
	if pool == nil {
		t.Fatalf("expected non-nil pool")
	}
	if pool.appCfg != appCfg {
		t.Errorf("expected pool.appCfg to match injected pointer")
	}
	if pool.appCfg.Current().Model != "initial-model" {
		t.Errorf("expected initial model, got %q", pool.appCfg.Current().Model)
	}
}

func TestWorkerPool_DrainTimeoutConfig(t *testing.T) {
	t.Parallel()
	// Default when unset or <= 0
	poolDef := NewWorkerPool(WorkerPoolConfig{
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
	if poolDef.cfg.DrainTimeout != 10*time.Second {
		t.Errorf("expected default DrainTimeout 10s, got %v", poolDef.cfg.DrainTimeout)
	}

	// Explicit custom DrainTimeout
	poolCustom := NewWorkerPool(WorkerPoolConfig{
		DrainTimeout: 250 * time.Millisecond,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return "", "", 0, nil
		},
	})
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
		{
			name:          "SuccessLifecycleAndSessionSaving",
			useSessionMgr: true,
			maxAttempts:   3,
			messages: []db.Message{{
				ID: "msg-101", ThreadID: "thread-202", GuildID: "guild-303",
				AuthorID: "user-404", AuthorName: "User", Content: "Hello Aerial",
			}},
			runnerSteps: []mockRunnerStep{{
				convID: "f1111111-2222-3333-4444-555555555555", response: "Clean output response", writeTranscript: true,
			}},
			wantStatus:            db.StatusCompleted,
			wantSavedSession:      "f1111111-2222-3333-4444-555555555555",
			wantDeliveredTexts:    []string{"Clean output response"},
			wantDeliveredChannels: []string{"thread-202"},
		},
		{
			name:             "TransientRetryPreservesSession",
			initialSessionID: "b1111111-2222-3333-4444-555555555555",
			maxAttempts:      3,
			messages:         []db.Message{{ID: "msg-retry-1", ThreadID: "thread-retry", Content: "Hello"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "Error 503: high demand unavailable", exitCode: 1, errMsg: "503"},
				{stderr: "Error 503: high demand unavailable", exitCode: 1, errMsg: "503"},
				{convID: "b1111111-2222-3333-4444-555555555555", response: "Success after retries!"},
			},
			wantRunnerCalls:  3,
			checkRunnerCalls: true,
			wantPassedSessions: []string{
				"b1111111-2222-3333-4444-555555555555",
				"b1111111-2222-3333-4444-555555555555",
				"b1111111-2222-3333-4444-555555555555",
			},
			wantStatus:     db.StatusCompleted,
			wantRetryCount: 2,
		},
		{
			name:             "SessionCorruptionRecovery",
			useSessionMgr:    true,
			initialSessionID: "a1111111-2222-3333-4444-555555555555",
			maxAttempts:      3,
			notifierReply:    "I refreshed our conversation! 🔄",
			messages:         []db.Message{{ID: "msg-corrupt-1", ThreadID: "thread-corrupt", GuildID: "guild-1", Content: "Hello"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "Error: failed to load conversation: session corrupted", exitCode: 1, errMsg: "corrupt"},
				{convID: "d8b5e679-7425-40de-944b-e07fc1f90ae7", response: "Clean output after session reset", writeTranscript: true},
			},
			wantRunnerCalls:    2,
			checkRunnerCalls:   true,
			wantPassedSessions: []string{"a1111111-2222-3333-4444-555555555555", ""},
			wantDeliveredTexts: []string{"I refreshed our conversation! 🔄", "Clean output after session reset"},
			wantSavedSession:   "d8b5e679-7425-40de-944b-e07fc1f90ae7",
			wantStatus:         db.StatusCompleted,
		},
		{
			name:        "TotalExhaustion",
			maxAttempts: 3,
			messages:    []db.Message{{ID: "msg-exhaust", ThreadID: "thread-exhaust", GuildID: "guild-1", Content: "Hello"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "status: unavailable 503", exitCode: 1, errMsg: "unavailable"},
			},
			wantRunnerCalls:    3,
			checkRunnerCalls:   true,
			wantStatus:         db.StatusFailed,
			wantRetryCount:     3,
			wantDeliveredTexts: []string{notifier.ModelUnavailableMessage()},
		},
		{
			name: "PromptPreservedWithoutMemoryInjection",
			messages: []db.Message{{
				ID: "msg-mem-1", ThreadID: "thread-mem-1", AuthorID: "user-1", Content: "What port does the server run on?",
			}},
			runnerSteps:        []mockRunnerStep{{response: "Response text"}},
			wantStatus:         db.StatusCompleted,
			wantPromptContains: []string{"What port does the server run on?"},
			wantPromptExcludes: []string{"<retrieved_memory>"},
			wantDBContent:      "What port does the server run on?",
		},
		{
			name:        "EmptyResponseFailure",
			messages:    []db.Message{{ID: "msg-empty-resp-1", ThreadID: "thread-empty-resp", AuthorID: "user-1", Content: "Empty response prompt"}},
			runnerSteps: []mockRunnerStep{{response: ""}},
			wantStatus:  db.StatusFailed,
		},
		{
			name:           "BurstCoalescing",
			enqueueAsBurst: true,
			messages: []db.Message{
				{ID: "m-b-1", ThreadID: "thread-burst", AuthorName: "Alice", Content: "Hello from Alice", CreatedAt: t0},
				{ID: "m-b-2", ThreadID: "thread-burst", AuthorName: "Bob", Content: "Hello from Bob", CreatedAt: t0.Add(3 * time.Second)},
				{ID: "m-b-3", ThreadID: "thread-burst", AuthorName: "Charlie", Content: "Hello from Charlie", CreatedAt: t0.Add(6 * time.Second)},
			},
			runnerSteps:        []mockRunnerStep{{response: "Coalesced reply from agent"}},
			wantStatus:         db.StatusCompleted,
			wantDeliveredTexts: []string{"Coalesced reply from agent"},
			wantPromptContains: []string{
				"<USER_REQUEST>",
				"[Multiple messages received in channel]",
				"--- Message 1 (by @Alice",
				"Hello from Alice",
				"--- Message 2 (by @Bob",
				"Hello from Bob",
				"--- Message 3 (by @Charlie",
				"Hello from Charlie",
			},
		},
		{
			name:             "Staleness_WithinDefault30mTTL",
			messageAge:       10 * time.Minute,
			messages:         []db.Message{{ID: "msg-stale-1", ThreadID: "thread-stale-1", AuthorID: "user-1", Content: "Staleness test prompt"}},
			runnerSteps:      []mockRunnerStep{{response: "OK"}},
			wantRunnerCalls:  1,
			checkRunnerCalls: true,
			wantStatus:       db.StatusCompleted,
		},
		{
			name:             "Staleness_ExceedsDefault30mTTL",
			messageAge:       31 * time.Minute,
			messages:         []db.Message{{ID: "msg-stale-2", ThreadID: "thread-stale-2", AuthorID: "user-1", Content: "Staleness test prompt"}},
			runnerSteps:      []mockRunnerStep{{response: "OK"}},
			wantRunnerCalls:  0,
			checkRunnerCalls: true,
			wantStatus:       db.StatusCompleted,
			wantResponseText: "[EXPIRED_STALE]",
		},
		{
			name:             "Staleness_ExceedsCustom10mTTL",
			stalenessTTL:     10 * time.Minute,
			messageAge:       12 * time.Minute,
			messages:         []db.Message{{ID: "msg-stale-3", ThreadID: "thread-stale-3", AuthorID: "user-1", Content: "Staleness test prompt"}},
			runnerSteps:      []mockRunnerStep{{response: "OK"}},
			wantRunnerCalls:  0,
			checkRunnerCalls: true,
			wantStatus:       db.StatusCompleted,
			wantResponseText: "[EXPIRED_STALE]",
		},
		{
			name: "SkipDiscord_AllSourcesDelivered",
			messages: []db.Message{
				{ID: "msg-sched-1", ThreadID: "thread-scheduled-1", GuildID: "scheduled", AuthorID: "scheduler", AuthorName: "Scheduler", Content: "Scheduled routine prompt"},
				{ID: "msg-http-1", ThreadID: "thread-http-1", AuthorID: "http-client", AuthorName: "HTTP Client", Content: "HTTP prompt"},
				{ID: "msg-user-1", ThreadID: "thread-user-1", GuildID: "guild-100", AuthorID: "user-999", AuthorName: "Alice", Content: "User prompt"},
			},
			runnerSteps:           []mockRunnerStep{{response: "AI reply"}},
			wantStatus:            db.StatusCompleted,
			wantDeliveredChannels: []string{"thread-scheduled-1", "thread-http-1", "thread-user-1"},
		},
		{
			name:             "TurnCountSessionPreservation",
			useSessionMgr:    true,
			initialSessionID: "sess-channel-init-123",
			seedTurnCount:    8,
			policyMode:       "channel",
			sequentialTurns:  true,
			messages: []db.Message{
				{ID: "m-rot-1", ThreadID: "channel-rotation-test", Content: "<@aerial> Turn 9"},
				{ID: "m-rot-2", ThreadID: "channel-rotation-test", Content: "<@aerial> Turn 10"},
				{ID: "m-rot-3", ThreadID: "channel-rotation-test", Content: "<@aerial> Turn 11"},
			},
			runnerSteps:      []mockRunnerStep{{response: "OK response", writeTranscript: true}},
			wantSavedSession: "sess-channel-init-123",
			wantTurnCount:    11,
			checkTurnCount:   true,
			wantStatus:       db.StatusCompleted,
		},
		{
			name:             "IdleTimeout_ChannelSession",
			useSessionMgr:    true,
			initialSessionID: "sess-channel-old-999",
			seedTurnCount:    1,
			seedIdleAge:      25 * time.Hour,
			policyMode:       "channel",
			sequentialTurns:  true,
			messages: []db.Message{
				{ID: "m-idle-1", ThreadID: "channel-rotation-test", Content: "<@aerial> wake up"},
				{ID: "m-idle-2", ThreadID: "channel-rotation-test", Content: "<@aerial> follow up"},
			},
			runnerSteps:        []mockRunnerStep{{response: "OK response", writeTranscript: true}},
			wantPassedSessions: []string{"sess-channel-old-999", "sess-channel-old-999"},
			wantSavedSession:   "sess-channel-old-999",
			wantTurnCount:      3,
			checkTurnCount:     true,
			wantStatus:         db.StatusCompleted,
		},
		{
			name:             "IdleTimeout_ThreadSession",
			useSessionMgr:    true,
			initialSessionID: "sess-thread-old-888",
			seedTurnCount:    1,
			seedIdleAge:      25 * time.Hour,
			policyMode:       "thread",
			sequentialTurns:  true,
			messages: []db.Message{
				{ID: "m-idle-t1", ThreadID: "thread-idle-rot-test", Content: "<@aerial> wake up"},
				{ID: "m-idle-t2", ThreadID: "thread-idle-rot-test", Content: "<@aerial> follow up"},
			},
			runnerSteps:        []mockRunnerStep{{response: "OK response", writeTranscript: true}},
			wantPassedSessions: []string{"sess-thread-old-888", "sess-thread-old-888"},
			wantSavedSession:   "sess-thread-old-888",
			wantTurnCount:      3,
			checkTurnCount:     true,
			wantStatus:         db.StatusCompleted,
		},
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

	a1Started := make(chan struct{})
	b1Finished := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
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
					t.Error("timed out waiting for B1 to complete concurrently while A1 is active")
				}
			} else if strings.Contains(prompt, "B1") {
				defer close(b1Finished)
			}

			mu.Lock()
			executionOrder = append(executionOrder, key+"_end")
			mu.Unlock()
			return mockJSONResponse("f2222222-2222-3333-4444-555555555555", "OK"), "", 0, nil
		},
		DeliveryFunc: noopDelivery,
		TypingFunc:   noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			allDone.Done()
		},
	})
	pool.Start()
	defer pool.Stop()

	msgA1 := db.Message{ID: "m-A1", ThreadID: "ThreadA", Content: "A1"}
	msgA2 := db.Message{ID: "m-A2", ThreadID: "ThreadA", Content: "A2"}
	msgB1 := db.Message{ID: "m-B1", ThreadID: "ThreadB", Content: "B1"}
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

	a1StartIndex, a1EndIndex, a2StartIndex, b1StartIndex := -1, -1, -1, -1
	for i, event := range executionOrder {
		switch event {
		case "A1_start":
			a1StartIndex = i
		case "A1_end":
			a1EndIndex = i
		case "A2_start":
			a2StartIndex = i
		case "B1_start":
			b1StartIndex = i
		}
	}
	if a1StartIndex > a1EndIndex || a1EndIndex > a2StartIndex {
		t.Errorf("Thread A was not serialized FIFO! Order: %v", executionOrder)
	}
	if b1StartIndex < 0 {
		t.Errorf("Thread B did not execute! Order: %v", executionOrder)
	}
}

func TestRecoverInterrupted(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	tests := []struct {
		name               string
		seedMessages       []db.Message
		seedRuns           []db.ScheduleRun
		withDiscordSession bool
		wantCompletedIDs   []string
		wantMsgStatuses    map[string]string
		wantMinDeliveries  int
		wantRunStatuses    map[string]string
		wantRunError       string
	}{
		{
			name: "FIFORecovery",
			seedMessages: []db.Message{
				{ID: "m1", ThreadID: "thread-same", GuildID: "g1", Status: db.StatusPending, CreatedAt: now.Add(-10 * time.Minute)},
				{ID: "m2", ThreadID: "thread-same", GuildID: "g1", Status: db.StatusProcessing, CreatedAt: now.Add(-5 * time.Minute)},
				{ID: "m3", ThreadID: "thread-same", GuildID: "g1", Status: db.StatusCompleted, CreatedAt: now.Add(-15 * time.Minute)},
			},
			wantCompletedIDs: []string{"m1", "m2"},
		},
		{
			name: "PoisonPill",
			seedMessages: []db.Message{
				{ID: "msg-poison", ThreadID: "thread-poison", GuildID: "guild-1", AuthorID: "user-1", Content: "crash command", Status: db.StatusProcessing, RetryCount: 3, CreatedAt: now.Add(-10 * time.Minute)},
				{ID: "msg-normal", ThreadID: "thread-normal", GuildID: "guild-1", AuthorID: "user-2", Content: "normal prompt", Status: db.StatusPending, RetryCount: 0, CreatedAt: now.Add(-5 * time.Minute)},
			},
			withDiscordSession: true,
			wantCompletedIDs:   []string{"msg-normal"},
			wantMsgStatuses:    map[string]string{"msg-poison": db.StatusFailed},
			wantMinDeliveries:  1,
		},
		{
			name: "ReconcilesOrphanedScheduleRuns",
			seedRuns: []db.ScheduleRun{
				{ID: "run-orphan-enqueued", ScheduleID: "cron-1", ScheduleType: "cron", TargetID: "chan-1", ThreadID: "thread-1", Prompt: "Orphan prompt 1", Status: "enqueued", StartedAt: now.Add(-1 * time.Hour)},
				{ID: "run-orphan-running", ScheduleID: "cron-2", ScheduleType: "cron", TargetID: "chan-2", ThreadID: "thread-2", Prompt: "Orphan prompt 2", Status: "running", StartedAt: now.Add(-1 * time.Hour)},
				{ID: "run-already-completed", ScheduleID: "cron-3", ScheduleType: "cron", TargetID: "chan-3", ThreadID: "thread-3", Prompt: "Completed prompt", Status: "completed", StartedAt: now.Add(-2 * time.Hour)},
			},
			wantRunStatuses: map[string]string{
				"run-orphan-enqueued":   "failed",
				"run-orphan-running":    "failed",
				"run-already-completed": "completed",
			},
			wantRunError: "Interrupted by server restart",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			for _, m := range tc.seedMessages {
				_ = insertMessage(store, m)
			}
			for _, r := range tc.seedRuns {
				_ = createScheduleRun(store, r)
			}

			var mu sync.Mutex
			var completedIDs []string
			var deliveredNotifs []string
			doneCh := make(chan struct{})
			if len(tc.wantCompletedIDs) == 0 {
				close(doneCh)
			}

			pool := NewWorkerPool(WorkerPoolConfig{
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    1,
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
			if tc.withDiscordSession {
				pool.SetDiscordSession(&discordgo.Session{})
			}

			RecoverInterrupted(store, pool)
			waitForCompletion(t, doneCh, 3*time.Second, tc.name)

			mu.Lock()
			defer mu.Unlock()

			if len(completedIDs) != len(tc.wantCompletedIDs) {
				t.Fatalf("completedIDs = %v, want %v", completedIDs, tc.wantCompletedIDs)
			}
			for i, wantID := range tc.wantCompletedIDs {
				if completedIDs[i] != wantID {
					t.Errorf("completedIDs[%d] = %q, want %q", i, completedIDs[i], wantID)
				}
			}
			for msgID, wantStatus := range tc.wantMsgStatuses {
				m, err := getMessage(store, msgID)
				if err != nil || m == nil || m.Status != wantStatus {
					t.Errorf("message %s status = %v (err: %v), want %s", msgID, m, err, wantStatus)
				}
			}
			if len(deliveredNotifs) < tc.wantMinDeliveries {
				t.Errorf("deliveredNotifs = %d, want >= %d", len(deliveredNotifs), tc.wantMinDeliveries)
			}
			for runID, wantStatus := range tc.wantRunStatuses {
				r, err := getScheduleRun(store, runID)
				if err != nil || r == nil {
					t.Fatalf("failed to get schedule run %s: %v", runID, err)
				}
				if r.Status != wantStatus {
					t.Errorf("run %s status = %q, want %q", runID, r.Status, wantStatus)
				}
				if wantStatus == "failed" {
					if r.Error != tc.wantRunError || r.CompletedAt == nil {
						t.Errorf("run %s error=%q completedAt=%v, want error=%q and non-nil completedAt", runID, r.Error, r.CompletedAt, tc.wantRunError)
					}
				}
			}
		})
	}
}

func TestWorkerPoolUpdateRuntimeConfig(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var receivedModel string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		Model:          "initial-model-v1",
		TimeoutMinutes: 10,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			receivedModel = model
			mu.Unlock()
			return mockJSONResponse("", "OK"), "", 0, nil
		},
		DeliveryFunc:       noopDelivery,
		TypingFunc:         noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) { close(doneCh) },
	})
	pool.Start()
	defer pool.Stop()

	if m := pool.GetRuntimeConfig(); m != "initial-model-v1" {
		t.Errorf("Expected initial-model-v1, got model=%s", m)
	}
	pool.UpdateRuntimeConfig("updated-model-v2")
	if m2 := pool.GetRuntimeConfig(); m2 != "updated-model-v2" {
		t.Errorf("Expected updated-model-v2, got model=%s", m2)
	}

	msg := db.Message{ID: "msg-update-test", ThreadID: "thread-update-test", Content: "Hello update", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)
	waitForCompletion(t, doneCh, 3*time.Second, "runtime config update")

	mu.Lock()
	if receivedModel != "updated-model-v2" {
		t.Errorf("Runner received unexpected runtime config: model=%q", receivedModel)
	}
	mu.Unlock()
}

func TestQueueScheduleRunLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		runID         string
		scheduleID    string
		threadID      string
		step          mockRunnerStep
		wantMsgStatus string
		wantRunStatus string
		wantEmptyErr  bool
	}{
		{
			name:          "Success",
			runID:         "run-lifecycle-success-1",
			scheduleID:    "cron-s-1",
			threadID:      "thread-sched-s",
			step:          mockRunnerStep{convID: "e1111111-2222-3333-4444-555555555555", response: "Success output"},
			wantMsgStatus: db.StatusCompleted,
			wantRunStatus: "completed",
			wantEmptyErr:  true,
		},
		{
			name:          "Failure",
			runID:         "run-lifecycle-fail-1",
			scheduleID:    "cron-f-1",
			threadID:      "thread-sched-f",
			step:          mockRunnerStep{stderr: "Fatal execution error: out of memory", exitCode: 1, errMsg: "out of memory"},
			wantMsgStatus: db.StatusFailed,
			wantRunStatus: "failed",
			wantEmptyErr:  false,
		},
		{
			name:          "PanicRecovery",
			runID:         "run-panic-1",
			scheduleID:    "cron-panic-sched",
			threadID:      "thread-panic",
			step:          mockRunnerStep{panicMsg: "simulated critical worker panic"},
			wantMsgStatus: db.StatusFailed,
			wantRunStatus: "failed",
			wantEmptyErr:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			doneCh := make(chan struct{})

			pool := NewWorkerPool(WorkerPoolConfig{
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    1,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return execMockRunnerStep(tc.step, "", sessionID)
				},
				DeliveryFunc: noopDelivery,
				TypingFunc:   noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					close(doneCh)
				},
			})
			pool.Start()
			defer pool.Stop()

			run := db.ScheduleRun{
				ID:           tc.runID,
				ScheduleID:   tc.scheduleID,
				ScheduleType: "cron",
				TargetID:     "chan-1",
				ThreadID:     tc.threadID,
				Title:        tc.name + " Routine",
				Prompt:       "Sync prompt",
				Status:       "enqueued",
				StartedAt:    time.Now().UTC().Add(-1 * time.Second),
			}
			if err := createScheduleRun(store, run); err != nil {
				t.Fatalf("Failed to create schedule run: %v", err)
			}

			msg := db.Message{
				ID:            "msg-" + tc.runID,
				ThreadID:      tc.threadID,
				GuildID:       "scheduled",
				AuthorID:      "scheduler",
				Content:       "Sync prompt",
				Status:        db.StatusPending,
				ScheduleRunID: tc.runID,
				CreatedAt:     time.Now().UTC(),
			}
			if err := insertMessage(store, msg); err != nil {
				t.Fatalf("Failed to insert message: %v", err)
			}

			pool.Enqueue(msg)
			waitForCompletion(t, doneCh, 3*time.Second, "schedule message execution")

			dbMsg, err := getMessage(store, msg.ID)
			if err != nil || dbMsg == nil {
				t.Fatalf("Failed to get message: %v", err)
			}
			if dbMsg.Status != tc.wantMsgStatus {
				t.Errorf("message status = %s, want %s", dbMsg.Status, tc.wantMsgStatus)
			}

			runs, total, err := getScheduleRunsPaginated(store, 10, 0, tc.scheduleID, "")
			if err != nil || total != 1 || len(runs) != 1 {
				t.Fatalf("Failed to get schedule run: %v (total=%d)", err, total)
			}
			r := runs[0]
			if r.Status != tc.wantRunStatus {
				t.Errorf("run status = %q, want %q", r.Status, tc.wantRunStatus)
			}
			if r.MessageID != msg.ID {
				t.Errorf("run message_id = %q, want %q", r.MessageID, msg.ID)
			}
			if r.CompletedAt == nil {
				t.Errorf("Expected CompletedAt to be set, got nil")
			}
			if r.DurationMs < 0 {
				t.Errorf("Expected DurationMs >= 0, got %d", r.DurationMs)
			}
			if tc.wantEmptyErr && r.Error != "" {
				t.Errorf("Expected empty Error, got %q", r.Error)
			}
			if !tc.wantEmptyErr && r.Error == "" {
				t.Errorf("Expected non-empty Error on failure/panic, got empty")
			}
		})
	}
}

func ptrFloat(f float64) *float64 {
	return &f
}

func TestQueueChannelPolicyIgnoredAndDefaultDenyMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		policy                config.ChannelPolicy
		ignoredTargetID       string
		seedSpamThread        bool
		classifierConfidence  float64
		message               db.Message
		nilOnMessageCompleted bool
		expectRunnerCalls     int
		expectDeliveryCalls   int
		expectTypingCalls     int
		expectStatus          string
		expectErrorMessage    string
		expectResponseText    string
	}{
		{
			name:                "IgnoredChannelPolicy",
			policy:              config.ChannelPolicy{Mode: "threads"},
			ignoredTargetID:     "chan-ignored-123",
			message:             db.Message{ID: "msg-ignored-1", ThreadID: "chan-ignored-123", Content: "Hello ignored room"},
			expectRunnerCalls:   0,
			expectDeliveryCalls: 0,
			expectTypingCalls:   0,
			expectStatus:        db.StatusCompleted,
			expectErrorMessage:  "[IGNORE]",
		},
		{
			name:                  "IgnoredChannelPolicy_NilCallback",
			policy:                config.ChannelPolicy{Mode: "ignore"},
			message:               db.Message{ID: "msg-nil-cb-1", ThreadID: "chan-ignored-nil-cb", Content: "Testing nil callback safety"},
			nilOnMessageCompleted: true,
			expectRunnerCalls:     0,
			expectDeliveryCalls:   0,
			expectTypingCalls:     0,
			expectStatus:          db.StatusCompleted,
		},
		{
			name:                "ThreadInheritsParentChannelPolicy",
			policy:              config.ChannelPolicy{Mode: "threads"},
			ignoredTargetID:     "parent-spam-id",
			seedSpamThread:      true,
			message:             db.Message{ID: "msg-thread-spam-1", ThreadID: "thread-in-spam-id", Content: "Hello in thread in spam"},
			expectRunnerCalls:   0,
			expectDeliveryCalls: 0,
			expectTypingCalls:   0,
			expectStatus:        db.StatusCompleted,
			expectErrorMessage:  "[IGNORE]",
		},
		{
			name:                "HTTPClient_NotDroppedByDefaultDenyIgnore",
			policy:              config.ChannelPolicy{Mode: "ignore"},
			message:             db.Message{ID: "msg-http-prompt-1", ThreadID: "synthetic-http-thread-uuid", AuthorID: "http-client", Content: "Explain Kubernetes architecture"},
			expectRunnerCalls:   1,
			expectDeliveryCalls: 1,
			expectTypingCalls:   1,
			expectStatus:        db.StatusCompleted,
			expectResponseText:  "HTTP prompt execution response",
		},
		{
			name:                "Typing_ThreadsModeActiveTurn",
			policy:              config.ChannelPolicy{Mode: "threads"},
			message:             db.Message{ID: "m-type-1", ThreadID: "th-active-1", Content: "Hello thread"},
			expectRunnerCalls:   1,
			expectDeliveryCalls: 1,
			expectTypingCalls:   1,
			expectStatus:        db.StatusCompleted,
		},
		{
			name:                "Typing_ChannelModeDirectMentionWake",
			policy:              config.ChannelPolicy{Mode: "channel", WakeMode: "mention"},
			message:             db.Message{ID: "m-type-2", ThreadID: "th-mention-yes", Content: "<@Aerial> help me"},
			expectRunnerCalls:   1,
			expectDeliveryCalls: 1,
			expectTypingCalls:   1,
			expectStatus:        db.StatusCompleted,
		},
		{
			name:                 "Typing_ChannelModeAmbientClassifierWake",
			policy:               config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrFloat(0.8)},
			classifierConfidence: 0.95,
			message:              db.Message{ID: "m-type-3", ThreadID: "th-ambient-wake", Content: "Let's change all of these to 8am"},
			expectRunnerCalls:    1,
			expectDeliveryCalls:  1,
			expectTypingCalls:    1,
			expectStatus:         db.StatusCompleted,
		},
		{
			name:                 "Typing_ChannelModeAmbientNonWakeChatter",
			policy:               config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: ptrFloat(0.8)},
			classifierConfidence: 0.10,
			message:              db.Message{ID: "m-type-4", ThreadID: "th-ambient-drop", Content: "Just random human chat between people"},
			expectRunnerCalls:    0,
			expectDeliveryCalls:  0,
			expectTypingCalls:    0,
			expectStatus:         db.StatusCompleted,
		},
		{
			name:                "Typing_IgnoredChannelPolicy",
			policy:              config.ChannelPolicy{Mode: "ignore"},
			message:             db.Message{ID: "m-type-5", ThreadID: "th-ignored", Content: "Ignored message @Aerial"},
			expectRunnerCalls:   0,
			expectDeliveryCalls: 0,
			expectTypingCalls:   0,
			expectStatus:        db.StatusCompleted,
		},
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
				cls = classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					return fmt.Sprintf(`{"confidence": %.2f, "reason": "test classification"}`, tc.classifierConfidence), nil
				}))
			}

			cfg := WorkerPoolConfig{
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    1,
				Classifier:     cls,
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
				cfg.OnMessageCompleted = func(msg db.Message, finalStatus string) {
					close(doneCh)
				}
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
			if runnerCalls != tc.expectRunnerCalls {
				t.Errorf("Expected %d runner calls, got %d", tc.expectRunnerCalls, runnerCalls)
			}
			if deliveryCalls != tc.expectDeliveryCalls {
				t.Errorf("Expected %d delivery calls, got %d", tc.expectDeliveryCalls, deliveryCalls)
			}
			if typingCalls != tc.expectTypingCalls {
				t.Errorf("Expected %d typing calls, got %d", tc.expectTypingCalls, typingCalls)
			}

			savedMsg, err := getMessage(store, msg.ID)
			if err != nil || savedMsg == nil {
				t.Fatalf("Failed to retrieve message: %v", err)
			}
			if savedMsg.Status != tc.expectStatus {
				t.Errorf("Expected status %s, got %s", tc.expectStatus, savedMsg.Status)
			}
			if tc.expectErrorMessage != "" && savedMsg.ErrorMessage != tc.expectErrorMessage {
				t.Errorf("Expected error message %q, got %q", tc.expectErrorMessage, savedMsg.ErrorMessage)
			}
			if tc.expectResponseText != "" && savedMsg.ResponseText != tc.expectResponseText {
				t.Errorf("Expected response text %q, got %q", tc.expectResponseText, savedMsg.ResponseText)
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
		{
			name:           "PureAmbient",
			channelID:      "chan-lounge",
			seedSession:    true,
			classifierJSON: `{"confidence": 0.25, "reason": "casual chit-chat"}`,
			policy:         defaultChannelPolicy,
			runnerResponse: "Should not run",
			messages: []db.Message{
				{ID: "msg-amb-1", AuthorName: "Alice", Content: "Hello everyone"},
				{ID: "msg-amb-2", AuthorName: "Bob", Content: "Nice weather today"},
			},
			wantRunnerCalls:     0,
			wantDeliveryCalls:   0,
			checkDeliveryCalls:  true,
			wantMaxTypingCalls:  0,
			checkMaxTypingCalls: true,
			wantTurnCount:       0,
			wantMsgErrors: map[string]string{
				"msg-amb-1": `[AMBIENT score=0.25/0.80 reason="casual chit-chat"]`,
				"msg-amb-2": `[AMBIENT score=0.25/0.80 reason="casual chit-chat"]`,
			},
		},
		{
			name:               "Tier1Wake",
			channelID:          "chan-lounge",
			seedSession:        true,
			botUserID:          "bot-aerial-id",
			policy:             defaultChannelPolicy,
			runnerResponse:     "I am Aerial, here to help!",
			messages:           []db.Message{{ID: "msg-tier1-1", AuthorName: "Alice", Content: "<@bot-aerial-id> can you help me with this bug?"}},
			wantRunnerCalls:    1,
			wantDeliveryCalls:  1,
			checkDeliveryCalls: true,
			wantDeliveredText:  "I am Aerial, here to help!",
			wantMinTypingCalls: 1,
			wantTurnCount:      1,
			wantMsgResponses:   map[string]string{"msg-tier1-1": "I am Aerial, here to help!"},
		},
		{
			name:               "Tier2Wake",
			channelID:          "chan-lounge",
			seedSession:        true,
			classifierJSON:     `{"confidence": 0.90, "reason": "user is asking for system health report"}`,
			policy:             defaultChannelPolicy,
			runnerResponse:     "All systems operational.",
			messages:           []db.Message{{ID: "msg-tier2-1", AuthorName: "Alice", Content: "Does anyone know if all services are healthy?"}},
			wantRunnerCalls:    1,
			wantDeliveryCalls:  1,
			checkDeliveryCalls: true,
			wantDeliveredText:  "All systems operational.",
			wantMinTypingCalls: 1,
			wantTurnCount:      1,
		},
		{
			name:           "MixedBurst",
			channelID:      "chan-lounge",
			seedSession:    true,
			classifierJSON: `{"confidence": 0.15, "reason": "unrelated lunch discussion"}`,
			policy:         defaultChannelPolicy,
			runnerResponse: "Done deploying!",
			messages: []db.Message{
				{ID: "msg-mixed-1", AuthorName: "Alice", Content: "I had tacos for lunch today"},
				{ID: "msg-mixed-2", AuthorName: "Bob", Content: "<@aerial> please deploy the backend"},
			},
			wantRunnerCalls:    1,
			wantTurnCount:      1,
			wantPromptContains: []string{"<CHANNEL_HISTORY>", "I had tacos for lunch today", "please deploy the backend"},
			wantMsgErrors:      map[string]string{"msg-mixed-1": "[AMBIENT score="},
			wantMsgResponses:   map[string]string{"msg-mixed-2": "Done deploying!"},
		},
		{
			name:           "SessionRotationBeforeLeadingAmbient",
			channelID:      "chan-rot-ambient",
			seedSession:    true,
			seedTurnCount:  5,
			classifierJSON: `{"confidence": 0.10, "reason": "ambient banter"}`,
			policy:         defaultChannelPolicy,
			runnerConvID:   "c9b5e679-7425-40de-944b-e07fc1f90ae7",
			runnerResponse: "I am answering your question!",
			messages: []db.Message{
				{ID: "msg-rot-amb-1", AuthorName: "Alice", Content: "Random ambient chatter before question"},
				{ID: "msg-rot-wake-2", AuthorName: "Bob", Content: "Hey <@aerial>, what is 2+2?"},
			},
			wantRunnerCalls:           1,
			wantTurnCount:             6,
			wantPassedInitialSession:  true,
			wantOldTranscriptExcludes: "Random ambient chatter before question",
			wantMsgErrors:             map[string]string{"msg-rot-amb-1": "[AMBIENT score="},
		},
		{
			name:                    "TrailingAmbient",
			channelID:               "chan-lounge",
			seedSession:             true,
			classifierJSON:          `{"confidence": 0.10, "reason": "ambient banter"}`,
			policy:                  defaultChannelPolicy,
			runnerResponse:          "Aerial answer",
			preserveSessionInRunner: true,
			messages: []db.Message{
				{ID: "msg-trail-wake-1", AuthorName: "Alice", Content: "<@aerial> explain gravity"},
				{ID: "msg-trail-amb-2", AuthorName: "Bob", Content: "I love physics too"},
			},
			wantRunnerCalls:    1,
			wantTurnCount:      1,
			wantPromptExcludes: []string{"I love physics too"},
			wantMsgErrors:      map[string]string{"msg-trail-amb-2": "[AMBIENT score="},
		},
		{
			name:                         "AmbientWakePromptDefault",
			channelID:                    "chan-aviation",
			classifierJSON:               `{"confidence": 0.95, "reason": "matches aviation directive"}`,
			policy:                       defaultChannelPolicy,
			runnerResponse:               "Aviation response",
			messages:                     []db.Message{{ID: "msg-aviation-1", AuthorName: "Pilot", Content: "What is the stall speed of a Cessna 172?"}},
			wantRunnerCalls:              1,
			wantTurnCount:                1,
			wantClassifierPromptContains: classifier.DefaultAmbientWakePrompt,
		},
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

	extracted := extractMessageBody(prompt)
	expected := "First line of message\nSecond line of message\nThird line of message"
	if extracted != expected {
		t.Errorf("Expected multiline extraction:\n%q\ngot:\n%q", expected, extracted)
	}
}

func TestIsTier1Wake_ReplyingToNonAerialWithAerialContent(t *testing.T) {
	t.Parallel()
	// A message replying to @bob, where bob's quoted content mentions "aerial photo"
	// and the user's content is "Nice shot!"
	prompt := `<USER_REQUEST>
Here's a message someone sent you from Discord:

- id: msg-reply-1
- channel_id: chan-1
- thread_id: thread-1
- guild_id: guild-1
- author_id: user-1
- author_username: alice
- author_global_name: Alice
- author_bot: false
- is_admin: false
- replying_to:
    author: "@bob"
    content: "Look at this aerial photo of the bridge"
- content: Nice shot!
- timestamp: 2026-09-02T12:00:00Z
- mentions: []
- attachments: []

Please formulate your response and output it clearly.
</USER_REQUEST>`

	msg := db.Message{
		ID:        "msg-reply-1",
		ThreadID:  "chan-1",
		Content:   prompt,
		CreatedAt: time.Now().UTC(),
	}

	if isTier1Wake(msg, "bot-aerial-id", nil) {
		t.Errorf("Expected isTier1Wake to be FALSE when replying to @bob whose content contains 'aerial photo'")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestResolveEffectiveChannel(t *testing.T) {
	tests := []struct {
		name           string
		nilSession     bool
		inputIDs       []string
		cached         []*discordgo.Channel
		stateChannel   *discordgo.Channel
		restChannel    *discordgo.Channel
		concurrent     int
		testInvalidate bool
		wantEffIDs     []string
		wantEffName    string
		wantIsThread   bool
		wantThreadName string
		wantCachedName string
	}{
		{
			name:       "NilSession",
			nilSession: true,
			inputIDs:   []string{"", "uuid-test-123", "123456789012345678"},
			wantEffIDs: []string{"", "uuid-test-123", "123456789012345678"},
		},
		{
			name:       "SyntheticNonNumericID",
			inputIDs:   []string{"http-client-session-1234", "abc-def-ghi", "1234-5678", "channel_name_test"},
			wantEffIDs: []string{"http-client-session-1234", "abc-def-ghi", "1234-5678", "channel_name_test"},
		},
		{
			name:     "NormalChannel",
			inputIDs: []string{"100200300400500601"},
			cached: []*discordgo.Channel{
				{ID: "100200300400500601", Name: "general-chat", Type: discordgo.ChannelTypeGuildText},
			},
			wantEffIDs:  []string{"100200300400500601"},
			wantEffName: "general-chat",
		},
		{
			name:     "ThreadParent",
			inputIDs: []string{"100200300400500602"},
			cached: []*discordgo.Channel{
				{ID: "100200300400500600", Name: "announcements", Type: discordgo.ChannelTypeGuildText},
				{ID: "100200300400500602", Name: "v1.2-discussion", ParentID: "100200300400500600", Type: discordgo.ChannelTypeGuildPublicThread},
			},
			wantEffIDs:     []string{"100200300400500600"},
			wantEffName:    "announcements",
			wantIsThread:   true,
			wantThreadName: "v1.2-discussion",
		},
		{
			name:     "ThreadMissingParent",
			inputIDs: []string{"100200300400500603"},
			cached: []*discordgo.Channel{
				{ID: "100200300400500603", Name: "isolated-thread", ParentID: "999888777666555444", Type: discordgo.ChannelTypeGuildPublicThread},
			},
			wantEffIDs:   []string{"999888777666555444"},
			wantIsThread: true,
		},
		{
			name:     "InvalidateCache",
			inputIDs: []string{"100200300400500604"},
			cached: []*discordgo.Channel{
				{ID: "100200300400500604", Name: "temporary-channel", Type: discordgo.ChannelTypeGuildText},
			},
			testInvalidate: true,
			wantEffIDs:     []string{"100200300400500604"},
			wantEffName:    "temporary-channel",
		},
		{
			name:     "StateFallback",
			inputIDs: []string{"100200300400500605"},
			stateChannel: &discordgo.Channel{
				ID: "100200300400500605", GuildID: "guild-1", Name: "state-channel", Type: discordgo.ChannelTypeGuildText,
			},
			wantEffIDs:     []string{"100200300400500605"},
			wantEffName:    "state-channel",
			wantCachedName: "state-channel",
		},
		{
			name:     "SingleFlightREST",
			inputIDs: []string{"100200300400500606"},
			restChannel: &discordgo.Channel{
				ID: "100200300400500606", Name: "singleflight-rest-channel", Type: discordgo.ChannelTypeGuildText,
			},
			concurrent:     5,
			wantEffIDs:     []string{"100200300400500606"},
			wantEffName:    "singleflight-rest-channel",
			wantCachedName: "singleflight-rest-channel",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, ch := range tc.cached {
				CacheDiscordChannel(ch)
				chID := ch.ID
				defer InvalidateChannelCache(chID)
			}
			for _, id := range tc.inputIDs {
				if len(tc.cached) == 0 && id != "" {
					InvalidateChannelCache(id)
					idCopy := id
					defer InvalidateChannelCache(idCopy)
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
				reqEntered := make(chan struct{})
				releaseReq := make(chan struct{})
				var once sync.Once
				s, _ = discordgo.New("Bot fake-token")
				s.Client = &http.Client{
					Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
						atomic.AddInt32(&reqCount, 1)
						once.Do(func() { close(reqEntered) })
						<-releaseReq
						data, _ := json.Marshal(tc.restChannel)
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewReader(data)),
							Header:     make(http.Header),
						}, nil
					}),
				}
				id := tc.inputIDs[0]
				var readyWg, doneWg sync.WaitGroup
				for i := 0; i < tc.concurrent; i++ {
					readyWg.Add(1)
					doneWg.Add(1)
					go func() {
						defer doneWg.Done()
						readyWg.Done()
						effID, effName, isThread := ResolveEffectiveChannel(s, id)
						if effID != tc.wantEffIDs[0] || effName != tc.wantEffName || isThread != tc.wantIsThread {
							t.Errorf("ResolveEffectiveChannel(%q) = (%q, %q, %v), want (%q, %q, %v)",
								id, effID, effName, isThread, tc.wantEffIDs[0], tc.wantEffName, tc.wantIsThread)
						}
					}()
				}
				readyWg.Wait()
				<-reqEntered
				for i := 0; i < 1000; i++ {
					runtime.Gosched()
				}
				close(releaseReq)
				doneWg.Wait()
				if count := atomic.LoadInt32(&reqCount); count != 1 {
					t.Errorf("expected 1 REST request due to singleflight, got %d", count)
				}
			} else {
				for i, id := range tc.inputIDs {
					effID, effName, isThread := ResolveEffectiveChannel(s, id)
					if effID != tc.wantEffIDs[i] || effName != tc.wantEffName || isThread != tc.wantIsThread {
						t.Errorf("ResolveEffectiveChannel(%q) = (%q, %q, %v), want (%q, %q, %v)",
							id, effID, effName, isThread, tc.wantEffIDs[i], tc.wantEffName, tc.wantIsThread)
					}
					if tc.wantThreadName != "" {
						effID2, effName2, isThread2, resolvedThreadName := ResolveChannelAndThread(s, id)
						if effID2 != tc.wantEffIDs[i] || effName2 != tc.wantEffName || !isThread2 || resolvedThreadName != tc.wantThreadName {
							t.Errorf("ResolveChannelAndThread(%q) = (%q, %q, %v, %q), want (%q, %q, true, %q)",
								id, effID2, effName2, isThread2, resolvedThreadName, tc.wantEffIDs[i], tc.wantEffName, tc.wantThreadName)
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
		{
			name:                "ChannelInstructionsInjection",
			channelID:           "100200300400500701",
			channelInstructions: map[string]string{"dev.md": "Follow Go idioms and test thoroughly."},
			cachedChannels: []*discordgo.Channel{
				{ID: "100200300400500701", Name: "dev", Type: discordgo.ChannelTypeGuildText},
			},
			runnerResponse:     "Success",
			messages:           []db.Message{{ID: "msg-inject-1", Content: "Build the queue worker"}},
			wantRunnerCalls:    1,
			wantTurnCount:      1,
			wantPromptPrefix:   "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\nFollow Go idioms and test thoroughly.\n</CHANNEL_INSTRUCTIONS>",
			wantPromptContains: []string{"Build the queue worker"},
		},
		{
			name:                "ChannelInstructionsThreadInheritance",
			channelID:           "100200300400500703",
			channelInstructions: map[string]string{"dev.md": "Guidelines for dev channel discussions."},
			cachedChannels: []*discordgo.Channel{
				{ID: "100200300400500702", Name: "dev", Type: discordgo.ChannelTypeGuildText},
				{ID: "100200300400500703", Name: "feature-thread", ParentID: "100200300400500702", Type: discordgo.ChannelTypeGuildPublicThread},
			},
			runnerResponse:   "Success",
			messages:         []db.Message{{ID: "msg-thread-inherit-1", Content: "Thread discussion prompt"}},
			wantRunnerCalls:  1,
			wantTurnCount:    1,
			wantPromptPrefix: "<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\nGuidelines for dev channel discussions.\n</CHANNEL_INSTRUCTIONS>",
		},
		{
			name:                "NoDuplicationOnRetry",
			channelID:           "100200300400500704",
			channelInstructions: map[string]string{"dev.md": "Channel instructions for retry test."},
			cachedChannels: []*discordgo.Channel{
				{ID: "100200300400500704", Name: "dev", Type: discordgo.ChannelTypeGuildText},
			},
			maxAttempts: 2,
			runnerSteps: []mockRunnerStep{
				{returnRawStdout: true, stderr: "Error 503: high demand unavailable", exitCode: 1},
				{convID: "session-retry-123", response: "Success on retry"},
			},
			messages:                          []db.Message{{ID: "msg-retry-1", Content: "Retry test prompt"}},
			wantRunnerCalls:                   2,
			wantTurnCount:                     1,
			wantSingleInstructionTagPerPrompt: true,
		},
		{
			name:           "Tier1PreScan_SkipsClassifier",
			channelID:      "chan-lounge",
			classifierJSON: `{"confidence": 0.0, "reason": "should not be called"}`,
			policy:         config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)},
			runnerResponse: "Hello there!",
			messages: []db.Message{
				{ID: "msg-prescan-1", AuthorName: "Alice", Content: "just hanging out"},
				{ID: "msg-prescan-2", AuthorName: "Bob", Content: "<@aerial> what's up?"},
				{ID: "msg-prescan-3", AuthorName: "Charlie", Content: "lol"},
			},
			wantClassifierCalls:  0,
			checkClassifierCalls: true,
			wantRunnerCalls:      1,
			wantTurnCount:        1,
		},
		{
			name:           "CoalescedAmbientBurst",
			channelID:      "chan-lounge",
			classifierJSON: `{"confidence": 0.95, "reason": "urgent issue needing answer"}`,
			policy:         config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)},
			runnerResponse: "I can help with that database error!",
			messages: []db.Message{
				{ID: "msg-coalesce-1", AuthorName: "Alice", Content: "Does anyone know why postgres crashed?"},
				{ID: "msg-coalesce-2", AuthorName: "Bob", Content: "brb grabbing coffee"},
			},
			wantClassifierCalls:          1,
			checkClassifierCalls:         true,
			wantClassifierPromptContains: "<target_burst>",
			wantRunnerCalls:              1,
			wantTurnCount:                1,
		},
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
		{
			name:            "GhostSessionRecovery",
			channelID:       "chan-lounge",
			seedSessionID:   "stale-ghost-uuid",
			seedTranscripts: []string{"a8b5e679-7425-40de-944b-e07fc1f90ae7"},
			policy:          config.ChannelPolicy{Mode: "channel"},
			runnerSteps: []mockRunnerStep{
				{
					convID:   "a8b5e679-7425-40de-944b-e07fc1f90ae7",
					response: "Hello! I am ready to help.",
					stderr:   "warning: conversation \"stale-ghost-uuid\" not found\nStarting conversation update stream for a8b5e679-7425-40de-944b-e07fc1f90ae7\n",
				},
			},
			messages:          []db.Message{{ID: "msg-ghost-recovery-1", AuthorName: "Alice", Content: "<@aerial> are you awake?"}},
			wantRunnerCalls:   1,
			wantTurnCount:     1,
			wantSavedSession:  "a8b5e679-7425-40de-944b-e07fc1f90ae7",
			checkSavedSession: true,
		},
		{
			name:             "MultiTurnContinuity_AfterRecovery",
			channelID:        "chan-lounge",
			seedSessionID:    "stale-ghost-uuid",
			seedTranscripts:  []string{"a8b5e679-7425-40de-944b-e07fc1f90ae7"},
			policy:           config.ChannelPolicy{Mode: "channel"},
			sequentialBursts: true,
			runnerSteps: []mockRunnerStep{
				{
					convID:   "a8b5e679-7425-40de-944b-e07fc1f90ae7",
					response: "Clean reply",
					stderr:   "warning: conversation \"stale-ghost-uuid\" not found\nStarting conversation update stream for a8b5e679-7425-40de-944b-e07fc1f90ae7\n",
				},
			},
			messages: []db.Message{
				{ID: "msg-recov-turn-1", AuthorName: "Alice", Content: "<@aerial> hello"},
				{ID: "msg-recov-turn-2", AuthorName: "Alice", Content: "<@aerial> what did I just say?"},
			},
			wantRunnerCalls:    2,
			wantTurnCount:      2,
			wantPassedSessions: []string{"", "a8b5e679-7425-40de-944b-e07fc1f90ae7"},
		},
		{
			name:              "ColdChannel_NoStubDirectory",
			channelID:         "chan-cold-123",
			classifierJSON:    `{"confidence": 0.05, "reason": "ambient chatter"}`,
			policy:            config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)},
			messages:          []db.Message{{ID: "msg-cold-ambient-1", AuthorName: "Alice", Content: "just saying hello to everyone in the room"}},
			wantRunnerCalls:   0,
			wantTurnCount:     0,
			wantSavedSession:  "",
			checkSavedSession: true,
			wantNoStubDir:     true,
		},
		{
			name:             "Turn1ContextInjection",
			channelID:        "chan-history",
			seedTranscripts:  []string{"e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"},
			historyMessages:  histMessages,
			policy:           config.ChannelPolicy{Mode: "channel"},
			sequentialBursts: true,
			runnerSteps: []mockRunnerStep{
				{convID: "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", response: "Answering wake question!"},
			},
			messages: []db.Message{
				{ID: "msg-hist-turn-1", AuthorName: "Charlie", Content: "<@aerial> turn 1 wake question"},
				{ID: "msg-hist-turn-2", AuthorName: "Charlie", Content: "<@aerial> turn 2 follow-up question"},
			},
			wantRunnerCalls:    2,
			wantTurnCount:      2,
			wantPassedSessions: []string{"", "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"},
			wantTurnPromptContains: map[int][]string{
				0: {"<CHANNEL_HISTORY>", "First historical message", "Second historical message"},
				1: {"<CHANNEL_HISTORY>"},
			},
			wantTurnPromptExcludes: map[int][]string{
				1: {"Previous assistant response"},
			},
		},
		{
			name:             "ThreadsMode_WarmTurnSuppressesChannelHistory",
			channelID:        "thread-history-suppress",
			seedTranscripts:  []string{"e5b7fb78-d85d-4bd6-ba9e-330f1e30596f"},
			historyMessages:  histMessages[:1],
			policy:           config.ChannelPolicy{Mode: "threads"},
			sequentialBursts: true,
			runnerSteps: []mockRunnerStep{
				{convID: "e5b7fb78-d85d-4bd6-ba9e-330f1e30596f", response: "Answering wake question!"},
			},
			messages: []db.Message{
				{ID: "msg-thread-turn-1", AuthorName: "Charlie", Content: "Hey Aerial, turn 1 thread question"},
				{ID: "msg-thread-turn-2", AuthorName: "Charlie", Content: "Hey Aerial, turn 2 follow-up question"},
			},
			wantRunnerCalls: 2,
			wantTurnCount:   2,
			wantTurnPromptContains: map[int][]string{
				0: {"<CHANNEL_HISTORY>"},
			},
			wantTurnPromptExcludes: map[int][]string{
				1: {"<CHANNEL_HISTORY>"},
			},
		},
		{
			name:            "SessionRotation_ResetsToColdState",
			channelID:       "chan-rotation-cold",
			seedTranscripts: []string{"c9b5e679-7425-40de-944b-e07fc1f90ae7"},
			historyMessages: []HistoryMessage{
				{ID: "hist-boot-1", AuthorName: "Alice", Role: "User", Content: "Historical message before bootstrap", CreatedAt: time.Now().UTC().Add(-5 * time.Minute)},
			},
			policy:           config.ChannelPolicy{Mode: "channel"},
			sequentialBursts: true,
			runnerSteps: []mockRunnerStep{
				{convID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", response: "Turn 1 answer"},
				{convID: "c9b5e679-7425-40de-944b-e07fc1f90ae7", response: "Subsequent turn answer"},
			},
			messages: []db.Message{
				{ID: "msg-turn-1", AuthorName: "Alice", Content: "<@aerial> Turn 1"},
				{ID: "msg-turn-2", AuthorName: "Alice", Content: "<@aerial> Turn 2"},
				{ID: "msg-turn-3", AuthorName: "Alice", Content: "<@aerial> Turn 3"},
			},
			wantRunnerCalls:    3,
			wantTurnCount:      3,
			wantSavedSession:   "c9b5e679-7425-40de-944b-e07fc1f90ae7",
			checkSavedSession:  true,
			wantPassedSessions: []string{"", "c9b5e679-7425-40de-944b-e07fc1f90ae7", "c9b5e679-7425-40de-944b-e07fc1f90ae7"},
			wantTurnPromptContains: map[int][]string{
				2: {"<CHANNEL_HISTORY>"},
			},
			wantTurnPromptExcludes: map[int][]string{
				0: {"<PREVIOUS_SESSION>"},
				2: {"<PREVIOUS_SESSION>"},
			},
		},
		{
			name:                 "SessionRotation_SeedsPreviousSessionID",
			channelID:            "chan-rotation-seed-test",
			seedTranscripts:      []string{"22222222-2222-2222-2222-222222222222"},
			enableHistoryFetcher: true,
			maxAttempts:          1,
			policy:               config.ChannelPolicy{Mode: "channel"},
			sequentialBursts:     true,
			rotateBeforeTurn:     3,
			runnerSteps: []mockRunnerStep{
				{convID: "11111111-1111-1111-1111-111111111111", response: "Hello from turn"},
				{convID: "11111111-1111-1111-1111-111111111111", response: "Hello from turn"},
				{convID: "22222222-2222-2222-2222-222222222222", response: "Hello from turn"},
				{convID: "22222222-2222-2222-2222-222222222222", response: "Hello from turn"},
			},
			messages: []db.Message{
				{ID: "msg-seed-1", AuthorName: "Alice", Content: "<@aerial> Turn 1"},
				{ID: "msg-seed-2", AuthorName: "Alice", Content: "<@aerial> Turn 2"},
				{ID: "msg-seed-3", AuthorName: "Alice", Content: "<@aerial> Turn 1 of Session 2"},
				{ID: "msg-seed-4", AuthorName: "Alice", Content: "<@aerial> Turn 2 of Session 2"},
			},
			wantRunnerCalls:   4,
			wantTurnCount:     2,
			wantSavedSession:  "22222222-2222-2222-2222-222222222222",
			checkSavedSession: true,
			wantTurnPromptContains: map[int][]string{
				2: {"<PREVIOUS_SESSION>"},
			},
			wantTurnPromptExcludes: map[int][]string{
				0: {"<PREVIOUS_SESSION>"},
				1: {"<PREVIOUS_SESSION>"},
				2: {"11111111-1111-1111-1111-111111111111", "transcript.jsonl"},
				3: {"<PREVIOUS_SESSION>"},
			},
		},
		{
			name:        "Turn1Crash_DoesNotPersistGhostUUID",
			channelID:   "chan-crash",
			maxAttempts: 1,
			policy:      config.ChannelPolicy{Mode: "channel"},
			runnerSteps: []mockRunnerStep{
				{
					stderr:   "Starting conversation update stream for crash-uuid-999\nError: fatal process crash\n",
					exitCode: 1,
					errMsg:   "exit status 1",
				},
			},
			messages:          []db.Message{{ID: "msg-crash-1", AuthorName: "Alice", Content: "Hey Aerial, please do something risky"}},
			wantRunnerCalls:   1,
			wantTurnCount:     0,
			wantSavedSession:  "",
			checkSavedSession: true,
		},
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
		content string
		roles   []string
		want    bool
	}

	tests := []struct {
		name       string
		burst      *burstTestCase
		tier1Cases []tier1Case
	}{
		{
			name: "ClassifierParseErrorTriggersSystemAlert",
			burst: &burstTestCase{
				channelID:             "chan-lounge",
				botUserID:             "bot-999",
				classifierJSON:        "```json\n{\"confidence\": 0.85}\n```",
				policy:                config.ChannelPolicy{Mode: "channel", AmbientWakeThreshold: ptrFloat(0.80)},
				messages:              []db.Message{{ID: "msg-parse-err-1", AuthorName: "Alice", Content: "Hello is anybody there?"}},
				wantRunnerCalls:       0,
				wantTurnCount:         0,
				wantAlertTitle:        "Classifier JSON Parse Error",
				wantAlertChannel:      config.DefaultConfigData().SystemChannel,
				wantAlertBodyContains: []string{"Parse Error", "Raw Output"},
			},
		},
		{
			name: "IsTier1Wake_WakeModeMention",
			tier1Cases: []tier1Case{
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
			},
		},
		{
			name: "ProcessBurst_WakeModeMention_BypassClassifier",
			burst: &burstTestCase{
				channelID:            "chan-lounge-mention",
				seedSession:          true,
				classifierJSON:       `{"confidence": 0.95, "reason": "classifier should not be called"}`,
				policy:               config.ChannelPolicy{Mode: "channel", WakeMode: "mention"},
				runnerResponse:       "Response!",
				messages:             []db.Message{{ID: "msg-amb-1", AuthorName: "Alice", Content: "Just talking about aerial views and drones"}},
				wantClassifierCalls:  0,
				checkClassifierCalls: true,
				wantRunnerCalls:      0,
				wantDeliveryCalls:    0,
				checkDeliveryCalls:   true,
				wantTurnCount:        0,
			},
		},
		{
			name: "ProcessBurst_WakeModeMention_DirectMentionWakes",
			burst: &burstTestCase{
				channelID:               "chan-lounge-wake",
				seedSession:             true,
				policy:                  config.ChannelPolicy{Mode: "channel", WakeMode: "mention"},
				runnerResponse:          "Hello Alice!",
				preserveSessionInRunner: true,
				messages: []db.Message{
					{ID: "msg-mention-1", AuthorName: "Alice", Content: "<USER_REQUEST>\n- mentions: [Aerial]\n- content: @Aerial what is the plan today?\n</USER_REQUEST>"},
				},
				wantRunnerCalls:    1,
				wantDeliveryCalls:  1,
				checkDeliveryCalls: true,
				wantDeliveredText:  "Hello Alice!",
				wantTurnCount:      1,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.burst != nil {
				runBurstTestCase(t, *tc.burst)
			}
			for _, c := range tc.tier1Cases {
				if got := isTier1Wake(db.Message{Content: c.content}, "bot-12345", c.roles); got != c.want {
					t.Errorf("isTier1Wake(%q, roles=%v) = %v, want %v", c.content, c.roles, got, c.want)
				}
			}
		})
	}
}

func TestWorkerPoolShutdown_PreservationMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		drainTimeout       time.Duration
		blockRunnerOnCtx   bool
		blockClassifierCtx bool
		verifyRecovery     bool
		runnerResponse     string
		msg                db.Message
		wantStatusAfterStop string
		wantDelivered      string
	}{
		{
			name:             "PreservesProcessingMessageWithoutApology",
			drainTimeout:     5 * time.Millisecond,
			blockRunnerOnCtx: true,
			verifyRecovery:   true,
			msg: db.Message{
				ID: "msg-shutdown-1", ThreadID: "thread-shutdown-1", GuildID: "guild-shutdown-1",
				AuthorID: "user-shutdown-1", AuthorName: "User", Content: "Hello during shutdown",
			},
			wantStatusAfterStop: db.StatusPending,
		},
		{
			name:           "RunnerSuccessPreserved",
			runnerResponse: "Finished just before pool stopped",
			msg: db.Message{
				ID: "msg-success-shutdown-1", ThreadID: "thread-success-shutdown-1", GuildID: "guild-success-shutdown-1",
				AuthorID: "user-1", AuthorName: "User", Content: "Complete fast",
			},
			wantStatusAfterStop: db.StatusCompleted,
			wantDelivered:       "Finished just before pool stopped",
		},
		{
			name:               "AmbientClassifierCancellationPreserved",
			drainTimeout:       5 * time.Millisecond,
			blockClassifierCtx: true,
			msg: db.Message{
				ID: "msg-ambient-shutdown-1", ThreadID: "channel-shutdown-1", GuildID: "channel-shutdown-1",
				AuthorID: "user-1", AuthorName: "User", Content: "hey did anyone see that movie?",
			},
			wantStatusAfterStop: db.StatusPending,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			var mu sync.Mutex
			var deliveredMessages []string
			stageStarted := make(chan struct{})
			releaseRunner := make(chan struct{})
			var once sync.Once

			var cls *classifier.Classifier
			if tc.blockClassifierCtx {
				cls = classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					once.Do(func() { close(stageStarted) })
					<-ctx.Done()
					return "", ctx.Err()
				}))
			}

			cfg := WorkerPoolConfig{
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    3,
				DrainTimeout:   tc.drainTimeout,
				Classifier:     cls,
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
			msg.Status = db.StatusPending
			msg.CreatedAt = time.Now().UTC()
			msg.UpdatedAt = time.Now().UTC()
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
				t.Errorf("expected 0 messages delivered on shutdown, got %v", deliveredMessages)
			}
			if tc.wantDelivered != "" && (len(deliveredMessages) == 0 || deliveredMessages[0] != tc.wantDelivered) {
				t.Errorf("deliveredMessages = %v, want %q", deliveredMessages, tc.wantDelivered)
			}
			mu.Unlock()

			msgCheck, err := getMessage(store, msg.ID)
			if err != nil || msgCheck == nil {
				t.Fatalf("getMessage failed: %v", err)
			}
			if msgCheck.Status != tc.wantStatusAfterStop {
				t.Errorf("status after stop = %q, want %q", msgCheck.Status, tc.wantStatusAfterStop)
			}
			if msgCheck.RetryCount != 0 {
				t.Errorf("retryCount after stop = %d, want 0", msgCheck.RetryCount)
			}

			if tc.verifyRecovery {
				newDoneCh := make(chan struct{})
				var finalDelivered string
				newPool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    3,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						return mockJSONResponse("c3333333-2222-3333-4444-555555555555", "Successfully processed on new container!"), "", 0, nil
					},
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						mu.Lock()
						finalDelivered = text
						mu.Unlock()
						return nil
					},
					TypingFunc: noopTyping,
					OnMessageCompleted: func(m db.Message, finalStatus string) {
						close(newDoneCh)
					},
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
				if err != nil || msgFinal == nil {
					t.Fatalf("final getMessage failed: %v", err)
				}
				if msgFinal.Status != db.StatusCompleted || msgFinal.RestartCount != 0 || msgFinal.RetryCount != 0 {
					t.Errorf("final msg = %+v, want COMPLETED with 0 restarts/retries", msgFinal)
				}
			}
		})
	}
}

func TestQueue_WatchdogAndRecoveryMatrix(t *testing.T) {
	t.Parallel()

	tests := []workerTestCase{
		{
			name:        "WatchdogInactivityRetryAndExhaustion",
			maxAttempts: 3,
			messages:    []db.Message{{ID: "msg-watchdog-101", ThreadID: "thread-watchdog-202", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Run a long task"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "[watchdog] inactivity timeout exceeded (5m without output)", exitCode: -1, errMsg: "exit status 255"},
			},
			wantRunnerCalls:         3,
			checkRunnerCalls:        true,
			wantStatus:              db.StatusFailed,
			wantErrorContains:       "inactivity timeout exceeded",
			wantDeliveredChannels:   []string{"thread-watchdog-202"},
			wantDeliveredSubstrings: []string{"timed out"},
		},
		{
			name:                "WatchdogInactivityRetryAndRecovery",
			maxAttempts:         3,
			initialSessionID:    "recovery-uuid-505",
			seedTranscriptFiles: map[string]string{"recovery-uuid-505": "mock transcript data\n"},
			messages:            []db.Message{{ID: "msg-recovery-101", ThreadID: "thread-recovery-505", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Run a long task"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "[watchdog] inactivity timeout exceeded (5m without output)", exitCode: -1, errMsg: "exit status 255"},
				{response: "Recovered successfully on retry!"},
			},
			wantRunnerCalls:         2,
			checkRunnerCalls:        true,
			wantStatus:              db.StatusCompleted,
			wantDeliveredChannels:   []string{"thread-recovery-505"},
			wantDeliveredSubstrings: []string{"Recovered successfully on retry!"},
			wantAttemptPromptContains: map[int][]string{
				0: {"Run a long task"},
				1: {"timed out or was interrupted"},
			},
		},
		{
			name:               "DefaultTimeoutMinutes_Is60",
			wantDefaultTimeout: 60,
		},
		{
			name:                "ColdStartWatchdogRecoveryAndContinuation",
			maxAttempts:         3,
			seedTranscriptFiles: map[string]string{"88888888-aaaa-4bbb-cccc-111122223333": "mock transcript data\n"},
			messages:            []db.Message{{ID: "msg-cold-101", ThreadID: "thread-cold-606", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Execute initial task cold"}},
			runnerSteps: []mockRunnerStep{
				{
					stderr:   "Starting conversation update stream for 88888888-aaaa-4bbb-cccc-111122223333\n[watchdog] inactivity timeout exceeded (5m without output)",
					exitCode: -1,
					errMsg:   "exit status 255",
				},
				{response: "Cold start recovered successfully on retry!"},
			},
			wantRunnerCalls:         2,
			checkRunnerCalls:        true,
			wantPassedSessions:      []string{"", "88888888-aaaa-4bbb-cccc-111122223333"},
			wantSavedSession:        "88888888-aaaa-4bbb-cccc-111122223333",
			wantDeliveredChannels:   []string{"thread-cold-606"},
			wantDeliveredSubstrings: []string{"Cold start recovered successfully on retry!"},
			wantAttemptPromptContains: map[int][]string{
				0: {"Execute initial task cold"},
				1: {"timed out or was interrupted"},
			},
		},
		{
			name:                "ColdStartStreamJsonInitLatchingOnFailure",
			maxAttempts:         3,
			seedTranscriptFiles: map[string]string{"99999999-bbbb-4ccc-dddd-555566667777": "mock transcript data\n"},
			messages:            []db.Message{{ID: "msg-cold-stream-1", ThreadID: "thread-cold-stream-1", Content: "Please write the code for stream-json"}},
			runnerSteps: []mockRunnerStep{
				{
					rawStdout: "{\"event\":\"init\",\"conversation_id\":\"99999999-bbbb-4ccc-dddd-555566667777\"}\n{\"event\":\"step_update\"}\n",
					stderr:    "[watchdog] inactivity timeout exceeded (5m without output)\n",
					exitCode:  -1,
					errMsg:    "exit status 255",
				},
				{response: "Stream-json cold start recovered!"},
			},
			wantRunnerCalls:         2,
			checkRunnerCalls:        true,
			wantPassedSessions:      []string{"", "99999999-bbbb-4ccc-dddd-555566667777"},
			wantSavedSession:        "99999999-bbbb-4ccc-dddd-555566667777",
			wantDeliveredChannels:   []string{"thread-cold-stream-1"},
			wantDeliveredSubstrings: []string{"Stream-json cold start recovered!"},
		},
		{
			name:                "ColdStartTransientRecoveryAndContinuation",
			maxAttempts:         3,
			seedTranscriptFiles: map[string]string{"80808080-bbbb-4ccc-dddd-444455556666": "mock transcript data\n"},
			messages:            []db.Message{{ID: "msg-transient-101", ThreadID: "thread-transient-707", GuildID: "guild-303", AuthorID: "user-404", AuthorName: "User", Content: "Run transient task cold"}},
			runnerSteps: []mockRunnerStep{
				{
					stderr:   "Starting conversation update stream for 80808080-bbbb-4ccc-dddd-444455556666\nError 503: high demand service unavailable",
					exitCode: 1,
					errMsg:   "exit status 1",
				},
				{response: "Transient error recovered on retry!"},
			},
			wantRunnerCalls:    2,
			checkRunnerCalls:   true,
			wantPassedSessions: []string{"", "80808080-bbbb-4ccc-dddd-444455556666"},
			wantSavedSession:   "80808080-bbbb-4ccc-dddd-444455556666",
			wantAttemptPromptContains: map[int][]string{
				1: {"Run transient task cold"},
			},
			wantAttemptPromptExcludes: map[int][]string{
				1: {"timed out or was interrupted"},
			},
		},
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
		{
			name:             "EmptyStdout_TranscriptRecovery",
			maxAttempts:      3,
			initialSessionID: "sess-transcript-recovery-999",
			seedTranscriptFiles: map[string]string{
				"sess-transcript-recovery-999": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"{{TS0}}","content":"Finish it"}` + "\n" +
					`{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"{{TS1}}","content":"Finished the task with 100% coverage!"}` + "\n",
			},
			messages:                []db.Message{{ID: "msg-empty-1", ThreadID: "thread-empty-stdout-1", GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Finish it"}},
			runnerSteps:             []mockRunnerStep{{returnRawStdout: true}},
			wantStatus:              db.StatusCompleted,
			wantDeliveredSubstrings: []string{"Finished the task with 100% coverage!"},
		},
		{
			name:             "StreamInterrupted_TranscriptRecovery",
			maxAttempts:      3,
			initialSessionID: "11111111-2222-4333-8444-555555555555",
			seedTranscriptFiles: map[string]string{
				"11111111-2222-4333-8444-555555555555": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"{{TS0}}","content":"Have the gang review"}` + "\n" +
					`{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"DONE","created_at":"{{TS1}}","content":"Error: The stream was interrupted. Please continue the task you were working on."}` + "\n" +
					`{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"{{TS2}}","content":"Here is the complete gang review with high consensus!"}` + "\n",
			},
			messages: []db.Message{{ID: "msg-interrupted-1", ThreadID: "thread-interrupted-1", GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Have the gang review"}},
			runnerSteps: []mockRunnerStep{
				{rawStdout: `{"event":"init","conversation_id":"11111111-2222-4333-8444-555555555555"}` + "\n" + `{"event":"result","status":"error","error":"The stream was interrupted. Please continue the task you were working on."}`},
			},
			wantRunnerCalls:         1,
			checkRunnerCalls:        true,
			wantStatus:              db.StatusCompleted,
			wantDeliveredSubstrings: []string{"Here is the complete gang review with high consensus!"},
		},
		{
			name:             "StreamInterrupted_NoResponse_RotatesSessionCorrupt",
			maxAttempts:      3,
			initialSessionID: "22222222-3333-4444-8555-666666666666",
			seedTranscriptFiles: map[string]string{
				"22222222-3333-4444-8555-666666666666": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-07T21:24:20Z","content":"Have the gang review"}` + "\n" +
					`{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"DONE","created_at":"2026-09-07T21:25:06Z","content":"Error: The stream was interrupted. Please continue the task you were working on."}` + "\n",
			},
			messages: []db.Message{{ID: "msg-interrupted-corrupt-2", ThreadID: "thread-interrupted-corrupt-2", GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Have the gang review"}},
			runnerSteps: []mockRunnerStep{
				{rawStdout: `{"event":"init","conversation_id":"22222222-3333-4444-8555-666666666666"}` + "\n" + `{"event":"result","status":"error","error":"The stream was interrupted. Please continue the task you were working on."}`},
				{convID: "33333333-4444-4555-8666-777777777777", response: "Recovered on clean session!"},
			},
			wantRunnerCalls:    2,
			checkRunnerCalls:   true,
			wantPassedSessions: []string{"22222222-3333-4444-8555-666666666666", ""},
			wantStatus:         db.StatusCompleted,
		},
		{
			name:             "EmptyStdout_TransientRetryAndContinuation",
			maxAttempts:      3,
			initialSessionID: "sess-empty-retry-888",
			seedTranscriptFiles: map[string]string{
				"sess-empty-retry-888": `{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-06T12:00:00Z","content":"Finish it"}` + "\n",
			},
			messages: []db.Message{{ID: "msg-empty-2", ThreadID: "thread-empty-retry-2", GuildID: "guild-2", AuthorID: "user-2", AuthorName: "User", Content: "Finish it"}},
			runnerSteps: []mockRunnerStep{
				{returnRawStdout: true},
				{response: "Recovered on attempt 2!"},
			},
			wantRunnerCalls:    2,
			checkRunnerCalls:   true,
			wantPassedSessions: []string{"sess-empty-retry-888", "sess-empty-retry-888"},
			wantAttemptPromptContains: map[int][]string{
				1: {"Finish it"},
			},
			wantAttemptPromptExcludes: map[int][]string{
				1: {"timed out or was interrupted"},
			},
		},
		{
			name:                "GeneralFailure_PreservesSessionOnDisk",
			maxAttempts:         3,
			initialSessionID:    "sess-general-fail-777",
			seedTranscriptFiles: map[string]string{"sess-general-fail-777": "existing transcript data\n"},
			messages:            []db.Message{{ID: "msg-fail-3", ThreadID: "thread-general-fail-3", GuildID: "guild-3", AuthorID: "user-3", AuthorName: "User", Content: "Finish it"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "fatal execution error: out of memory", exitCode: 1, errMsg: "exit status 1"},
				{response: "Recovered from exit 1!"},
			},
			wantRunnerCalls:    2,
			checkRunnerCalls:   true,
			wantPassedSessions: []string{"sess-general-fail-777", "sess-general-fail-777"},
			wantAttemptPromptContains: map[int][]string{
				1: {"Finish it"},
			},
			wantAttemptPromptExcludes: map[int][]string{
				1: {"timed out or was interrupted"},
			},
		},
		{
			name:                   "WorkerPool_FullBuffer_NoEvictionZombieRace",
			maxAttempts:            1,
			idleTimeout:            20 * time.Millisecond,
			concurrentEnqueueCount: 150,
			runnerSteps:            []mockRunnerStep{{response: "OK"}},
		},
		{
			name:             "TransientError_RetainsOriginalPrompt",
			maxAttempts:      2,
			useSessionMgr:    true,
			initialSessionID: "sess-transient-503",
			messages:         []db.Message{{ID: "msg-503-1", ThreadID: "thread-503", GuildID: "guild-503", AuthorID: "user-503", AuthorName: "User", Content: "Original prompt text that must not be wiped"}},
			runnerSteps: []mockRunnerStep{
				{stderr: "Error 503: Service Unavailable. High demand.", exitCode: 1, errMsg: "exit code 1"},
				{response: "Recovered cleanly"},
			},
			wantRunnerCalls:  2,
			checkRunnerCalls: true,
			wantAttemptPromptContains: map[int][]string{
				1: {"Original prompt text that must not be wiped"},
			},
			wantAttemptPromptExcludes: map[int][]string{
				1: {"timed out or was interrupted"},
			},
		},
		{
			name:            "ImageDeliveryAndSanitization",
			maxAttempts:     3,
			seedPNGImage:    image.NewRGBA(image.Rect(0, 0, 10, 10)),
			seedPNGLevel:    png.DefaultCompression,
			seedPNGFilename: "metrics_chart.png",
			messages:        []db.Message{{ID: "msg-img-1", ThreadID: "thread-img-1", GuildID: "guild-img-1", AuthorID: "user-img-1", AuthorName: "User", Content: "Show me the metrics chart"}},
			runnerSteps: []mockRunnerStep{
				{convID: "c2222222-2222-3333-4444-555555555555", response: "Telemetry Summary:\n\n![Daily Chart]({{IMAGE_PATH}})\n\nAll systems nominal."},
			},
			wantAttachments:         []*delivery.Attachment{{Filename: "metrics_chart.png"}},
			wantDeliveredSubstrings: []string{"**Daily Chart**"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWorkerTestCase(t, tc)
		})
	}
}

func TestIsTier1Wake_SchedulerMessage(t *testing.T) {
	t.Parallel()
	// 1. AuthorID == "scheduler"
	msgSched := db.Message{
		ID:        "m-sched-1",
		AuthorID:  "scheduler",
		Content:   "check system health",
		CreatedAt: time.Now().UTC(),
	}
	if !isTier1Wake(msgSched, "bot-123", nil) {
		t.Errorf("Expected isTier1Wake to return true for AuthorID == scheduler in mention mode")
	}

	// 2. ScheduleRunID != ""
	msgRun := db.Message{
		ID:            "m-run-1",
		AuthorID:      "user-456",
		ScheduleRunID: "run-uuid-999",
		Content:       "run cron report",
		CreatedAt:     time.Now().UTC(),
	}
	if !isTier1Wake(msgRun, "bot-123", nil) {
		t.Errorf("Expected isTier1Wake to return true for non-empty ScheduleRunID in mention mode")
	}
}

func TestResolveBotRoleIDsSuite(t *testing.T) {
	origFetcher := guildMemberFetcher
	defer func() { guildMemberFetcher = origFetcher }()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "NilSessionAndStateResolution",
			run: func(t *testing.T) {
				if roles := ResolveBotRoleIDs(nil, "guild-suite-1", "bot-suite-1"); roles != nil {
					t.Errorf("Expected nil for nil session, got %v", roles)
				}

				s := &discordgo.Session{
					State: discordgo.NewState(),
				}
				s.State.User = &discordgo.User{
					ID:       "bot-suite-1",
					Username: "Aerial",
				}

				guild := &discordgo.Guild{
					ID: "guild-suite-1",
					Roles: []*discordgo.Role{
						{ID: "role-aerial-managed", Name: "Aerial", Managed: true},
						{ID: "role-gundam-alt", Name: "Gundam"},
						{ID: "role-unrelated", Name: "Member"},
					},
					Members: []*discordgo.Member{
						{
							User:  &discordgo.User{ID: "bot-suite-1"},
							Roles: []string{"role-assigned-1"},
						},
					},
				}
				_ = s.State.GuildAdd(guild)

				roles := ResolveBotRoleIDs(s, "guild-suite-1", "bot-suite-1")
				expected := []string{"role-aerial-managed", "role-assigned-1", "role-gundam-alt"}
				sort.Strings(roles)
				sort.Strings(expected)
				if strings.Join(roles, ",") != strings.Join(expected, ",") {
					t.Errorf("Expected roles %v, got %v", expected, roles)
				}
			},
		},
		{
			name: "RESTFallbackAndCaching",
			run: func(t *testing.T) {
				var fetchCalls int32
				guildMemberFetcher = func(sess *discordgo.Session, guildID string, userID string) (*discordgo.Member, error) {
					atomic.AddInt32(&fetchCalls, 1)
					if guildID == "guild-suite-rest" && userID == "bot-suite-rest" {
						return &discordgo.Member{
							GuildID: guildID,
							User:    &discordgo.User{ID: userID},
							Roles:   []string{"mock-role-robot-123"},
						}, nil
					}
					return nil, errors.New("not found")
				}

				s := &discordgo.Session{
					State: discordgo.NewState(),
				}
				s.State.User = &discordgo.User{
					ID:       "bot-suite-rest",
					Username: "Aerial",
				}

				guild := &discordgo.Guild{
					ID: "guild-suite-rest",
					Roles: []*discordgo.Role{
						{ID: "role-aerial-managed", Name: "Aerial", Managed: true},
					},
				}
				_ = s.State.GuildAdd(guild)

				roles := ResolveBotRoleIDs(s, "guild-suite-rest", "bot-suite-rest")
				expected := []string{"mock-role-robot-123", "role-aerial-managed"}
				sort.Strings(roles)
				sort.Strings(expected)
				if strings.Join(roles, ",") != strings.Join(expected, ",") {
					t.Fatalf("First call expected %v, got %v", expected, roles)
				}
				if atomic.LoadInt32(&fetchCalls) != 1 {
					t.Fatalf("Expected 1 fetch call, got %d", atomic.LoadInt32(&fetchCalls))
				}

				roles2 := ResolveBotRoleIDs(s, "guild-suite-rest", "bot-suite-rest")
				sort.Strings(roles2)
				if strings.Join(roles2, ",") != strings.Join(expected, ",") {
					t.Fatalf("Second call expected %v, got %v", expected, roles2)
				}
				if atomic.LoadInt32(&fetchCalls) != 1 {
					t.Fatalf("Expected fetcher not to be called again (cache hit), but fetchCalls=%d", atomic.LoadInt32(&fetchCalls))
				}
			},
		},
		{
			name: "SingleflightDeduplication",
			run: func(t *testing.T) {
				var fetchCalls int32
				blockCh := make(chan struct{})
				guildMemberFetcher = func(sess *discordgo.Session, guildID string, userID string) (*discordgo.Member, error) {
					atomic.AddInt32(&fetchCalls, 1)
					<-blockCh
					return &discordgo.Member{
						GuildID: guildID,
						User:    &discordgo.User{ID: userID},
						Roles:   []string{"mock-role-robot-123"},
					}, nil
				}

				s := &discordgo.Session{
					State: discordgo.NewState(),
				}
				s.State.User = &discordgo.User{ID: "bot-suite-sf", Username: "Aerial"}
				guild := &discordgo.Guild{
					ID: "guild-suite-sf",
				}
				_ = s.State.GuildAdd(guild)

				var wg sync.WaitGroup
				numCallers := 10
				wg.Add(numCallers)

				for i := 0; i < numCallers; i++ {
					go func() {
						defer wg.Done()
						roles := ResolveBotRoleIDs(s, "guild-suite-sf", "bot-suite-sf")
						if len(roles) != 1 || roles[0] != "mock-role-robot-123" {
							t.Errorf("Expected role mock-role-robot-123, got %v", roles)
						}
					}()
				}

				time.Sleep(20 * time.Millisecond)
				close(blockCh)
				wg.Wait()

				if calls := atomic.LoadInt32(&fetchCalls); calls != 1 {
					t.Fatalf("Expected exactly 1 singleflight REST call for 10 concurrent requests, got %d", calls)
				}
			},
		},
		{
			name: "CooldownAndUncachedGuild",
			run: func(t *testing.T) {
				var fetchCalls int32
				shouldFail := true
				guildMemberFetcher = func(sess *discordgo.Session, guildID string, userID string) (*discordgo.Member, error) {
					atomic.AddInt32(&fetchCalls, 1)
					if shouldFail {
						return nil, errors.New("simulated 429 rate limit")
					}
					return &discordgo.Member{
						GuildID: guildID,
						User:    &discordgo.User{ID: userID},
						Roles:   []string{"mock-role-robot-123"},
					}, nil
				}

				s := &discordgo.Session{
					State: discordgo.NewState(),
				}
				s.State.User = &discordgo.User{ID: "bot-suite-cd", Username: "Aerial"}

				// Note: Guild is NOT in s.State, testing fallback for uncached guild
				// 1. First call fails and sets cooldown
				roles := ResolveBotRoleIDs(s, "guild-suite-uncached", "bot-suite-cd")
				if len(roles) != 0 {
					t.Fatalf("expected empty roles on failure, got %v", roles)
				}
				if atomic.LoadInt32(&fetchCalls) != 1 {
					t.Fatalf("expected 1 fetch call, got %d", fetchCalls)
				}

				// 2. Second call while in cooldown skips REST call
				roles2 := ResolveBotRoleIDs(s, "guild-suite-uncached", "bot-suite-cd")
				if len(roles2) != 0 {
					t.Fatalf("expected empty roles, got %v", roles2)
				}
				if atomic.LoadInt32(&fetchCalls) != 1 {
					t.Fatalf("expected fetchCalls to stay 1 due to cooldown, got %d", fetchCalls)
				}

				// 3. Test cooldown helper direct invocation for map pruning
				for i := 0; i < 150; i++ {
					setRESTCooldown(fmt.Sprintf("suite-key-%d", i), -1*time.Minute)
				}
				setRESTCooldown("suite-key-active", 1*time.Minute)
				if !isRESTInCooldown("suite-key-active") {
					t.Fatalf("expected suite-key-active to be in cooldown")
				}
				if isRESTInCooldown("suite-key-expired") {
					t.Fatalf("expected suite-key-expired not to be in cooldown")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func TestProcessBurst_QuotaPause_Suite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "SchedulesOneShotAndNotifies",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				var runnerCalls int
				var deliveredTexts []string
				var mu sync.Mutex
				doneCh := make(chan struct{})

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    3,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
						mu.Lock()
						runnerCalls++
						mu.Unlock()
						return "", "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", 1, errors.New("exit 1")
					},
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						mu.Lock()
						deliveredTexts = append(deliveredTexts, text)
						mu.Unlock()
						return nil
					},
					OnMessageCompleted: func(m db.Message, status string) {
						close(doneCh)
					},
				})
				pool.Start()
				defer pool.Stop()

				threadID := "chan-quota-suite-1"
				msg := db.Message{
					ID:        "msg-quota-suite-1",
					ThreadID:  threadID,
					Content:   "<@bot> can you analyze our code coverage?",
					Status:    db.StatusPending,
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				}
				_ = insertMessage(store, msg)
				pool.Enqueue(msg)

				waitForCompletion(t, doneCh, 5*time.Second, "quota pause completion")

				mu.Lock()
				defer mu.Unlock()

				if runnerCalls != 1 {
					t.Fatalf("Expected exactly 1 runner call (fail-fast), got %d", runnerCalls)
				}
				if len(deliveredTexts) != 1 {
					t.Fatalf("Expected 1 delivered notice, got %d", len(deliveredTexts))
				}
				notice := deliveredTexts[0]
				if !strings.Contains(notice, "16m 58s") {
					t.Errorf("Expected '16m 58s' in notice, got: %s", notice)
				}
				if !strings.Contains(notice, "<t:") || !strings.Contains(notice, ":R>") {
					t.Errorf("Expected relative Discord timestamp in notice, got: %s", notice)
				}
				if !strings.Contains(notice, "automatically scheduled a retry") {
					t.Errorf("Expected auto-retry confirmation in notice, got: %s", notice)
				}

				oneShots, err := store.GetAllOneShotSchedules(context.Background(), threadID)
				if err != nil || len(oneShots) == 0 {
					t.Fatalf("Error querying one_shot_schedules: %v (count=%d)", err, len(oneShots))
				}
				schedPrompt := oneShots[0].Prompt
				runAt := oneShots[0].RunAt
				if !strings.Contains(schedPrompt, "[QUOTA_RETRY]") || !strings.Contains(schedPrompt, "code coverage") {
					t.Errorf("Expected retry prompt with [QUOTA_RETRY] and coverage query, got: %q", schedPrompt)
				}
				if time.Until(runAt) < 16*time.Minute {
					t.Errorf("Expected runAt to be at least 16m in the future, got %v (runAt: %s)", time.Until(runAt), runAt)
				}

				dbMsg, _ := getMessage(store, msg.ID)
				if dbMsg.Status != db.StatusFailed {
					t.Errorf("Expected message status FAILED, got %s", dbMsg.Status)
				}
				if !strings.Contains(dbMsg.ErrorMessage, "[QUOTA_PAUSED") {
					t.Errorf("Expected [QUOTA_PAUSED] in error message, got: %s", dbMsg.ErrorMessage)
				}
			},
		},
		{
			name: "CircuitBreakerDoesNotReschedule",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				var runnerCalls int
				var deliveredTexts []string
				var mu sync.Mutex
				doneCh := make(chan struct{})

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    3,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
						mu.Lock()
						runnerCalls++
						mu.Unlock()
						return "", "Individual quota reached. Resets in 10m.", 1, errors.New("exit 1")
					},
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						mu.Lock()
						deliveredTexts = append(deliveredTexts, text)
						mu.Unlock()
						return nil
					},
					OnMessageCompleted: func(m db.Message, status string) {
						close(doneCh)
					},
				})
				pool.Start()
				defer pool.Stop()

				threadID := "chan-circuit-suite-1"
				msg := db.Message{
					ID:            "msg-circuit-suite-1",
					ThreadID:      threadID,
					ScheduleRunID: "run-sched-previous-1",
					Content:       "[QUOTA_RETRY] Previous failed question",
					Status:        db.StatusPending,
					CreatedAt:     time.Now().UTC(),
					UpdatedAt:     time.Now().UTC(),
				}
				_ = insertMessage(store, msg)
				pool.Enqueue(msg)

				waitForCompletion(t, doneCh, 5*time.Second, "circuit breaker completion")

				mu.Lock()
				defer mu.Unlock()

				oneShots, _ := store.GetAllOneShotSchedules(context.Background(), threadID)
				if len(oneShots) != 0 {
					t.Fatalf("Expected 0 one-shot schedules created on circuit breaker, got %d", len(oneShots))
				}
				if len(deliveredTexts) != 1 {
					t.Fatalf("Expected 1 delivered text, got %d", len(deliveredTexts))
				}
				if !strings.Contains(deliveredTexts[0], "paused automated retries") {
					t.Errorf("Expected circuit breaker notice in delivery, got: %s", deliveredTexts[0])
				}
			},
		},
		{
			name: "TrailingBurstSuppression",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				var runnerCalls int
				var mu sync.Mutex
				completedMsgs := make(map[string]string)
				var completedCount int
				doneCh := make(chan struct{})

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    3,
					ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
						return config.ChannelPolicy{Mode: "channel"}
					},
					DeliveryFunc: noopDelivery,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
						mu.Lock()
						runnerCalls++
						mu.Unlock()
						return "", "Individual quota reached. Resets in 16m58s.", 1, errors.New("exit 1")
					},
					OnMessageCompleted: func(m db.Message, status string) {
						mu.Lock()
						completedMsgs[m.ID] = status
						completedCount++
						if completedCount == 2 {
							close(doneCh)
						}
						mu.Unlock()
					},
				})
				pool.Start()
				defer pool.Stop()

				threadID := "chan-burst-suppress-suite-1"
				t0 := time.Now().UTC()
				msg1 := db.Message{
					ID:        "msg-burst-suite-1",
					ThreadID:  threadID,
					Content:   "<@aerial> Question 1",
					Status:    db.StatusPending,
					CreatedAt: t0,
					UpdatedAt: t0,
				}
				msg2 := db.Message{
					ID:        "msg-burst-suite-2",
					ThreadID:  threadID,
					Content:   "<@aerial> Question 2",
					Status:    db.StatusPending,
					CreatedAt: t0.Add(100 * time.Millisecond),
					UpdatedAt: t0.Add(100 * time.Millisecond),
				}
				_ = insertMessage(store, msg1)
				_ = insertMessage(store, msg2)

				pool.processBurst([]db.Message{msg1, msg2})

				mu.Lock()
				defer mu.Unlock()

				if runnerCalls != 1 {
					t.Fatalf("Expected runner called only once, got %d", runnerCalls)
				}
				m2, _ := getMessage(store, msg2.ID)
				if m2.Status != db.StatusFailed {
					t.Errorf("Expected msg2 status FAILED, got %s", m2.Status)
				}
				if !strings.Contains(m2.ErrorMessage, "Trailing message suppressed") {
					t.Errorf("Expected trailing suppression error on msg2, got: %s", m2.ErrorMessage)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}








func TestQueueWorker_PerScopeSerialization(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	var mu sync.Mutex
	var activeWorkers int
	var maxActiveWorkers int

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			activeWorkers++
			if activeWorkers > maxActiveWorkers {
				maxActiveWorkers = activeWorkers
			}
			mu.Unlock()
			
			time.Sleep(2 * time.Millisecond) // Simulate work

			mu.Lock()
			activeWorkers--
			mu.Unlock()
			
			return mockJSONResponse("", "OK"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) (stop func()) {
			return func() {}
		},
	})
	
	// Create multiple messages for the same thread but bypass Enqueue routing to test processBurst serialization directly.
	msg1 := db.Message{ID: "msg-scope-1", ThreadID: "thread-scope-test", Content: "Msg 1"}
	msg2 := db.Message{ID: "msg-scope-2", ThreadID: "thread-scope-test", Content: "Msg 2"}
	msg3 := db.Message{ID: "msg-scope-3", ThreadID: "thread-scope-test", Content: "Msg 3"}
	_ = insertMessage(store, msg1)
	_ = insertMessage(store, msg2)
	_ = insertMessage(store, msg3)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); pool.processBurst([]db.Message{msg1}) }()
	go func() { defer wg.Done(); pool.processBurst([]db.Message{msg2}) }()
	go func() { defer wg.Done(); pool.processBurst([]db.Message{msg3}) }()

	wg.Wait()

	if maxActiveWorkers > 1 {
		t.Errorf("Expected per-scope worker serialization, but found %d concurrent workers for same scope", maxActiveWorkers)
	}
}

func TestThreadSession_RotationAtTurnLimit(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	_ = saveSessionID(store, "thread-rotate-test", "old-session-id")
	for i := 0; i < 10; i++ {
		_, _ = incrementSessionTurnCount(store, "thread-rotate-test")
	}

	var mu sync.Mutex
	var gotSessionID string
	doneCh := make(chan struct{})

	appCfg := config.NewFromData(&config.ConfigData{
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "thread"}, // Thread mode
		},
	})

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: session.New(t.TempDir(), ""),
		Store: store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			gotSessionID = sessionID
			mu.Unlock()
			return mockJSONResponse(sessionID, "OK"), "", 0, nil
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

	msg := db.Message{ID: "msg-rotate", ThreadID: "thread-rotate-test", Content: "Turn 11"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	mu.Lock()
	if gotSessionID != "old-session-id" {
		t.Errorf("Expected session to be preserved (%q), got: %q", "old-session-id", gotSessionID)
	}
	mu.Unlock()

	curSess, err := getSessionID(store, "thread-rotate-test")
	if err != nil {
		t.Fatalf("Failed to get session ID: %v", err)
	}
	if curSess != "old-session-id" {
		t.Errorf("Expected session in store to be preserved (%q), got: %q", "old-session-id", curSess)
	}
}

func TestRunner_DefensiveLatchingAndCold429(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	var mu sync.Mutex
	var receivedIDs []string
	doneCh := make(chan struct{}, 2)

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			receivedIDs = append(receivedIDs, prompt)
			mu.Unlock()

			if strings.Contains(prompt, "Test429") {
				return "", "HTTP 429 Too Many Requests: context length exceeded", 1, fmt.Errorf("429")
			}
			if strings.Contains(prompt, "TestEmptyLatch") {
				return "{}", "", 0, nil // Invalid JSON with no conversation ID
			}
			return "", "", 0, fmt.Errorf("unknown")
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

	// 1. Test Cold 429 (should not retry)
	msg1 := db.Message{ID: "msg-429", ThreadID: "thread-429", Content: "Test429"}
	_ = insertMessage(store, msg1)
	pool.Enqueue(msg1)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for 429")
	}

	mu.Lock()
	dbMsg1, _ := getMessage(store, "msg-429")
	if dbMsg1.Status != db.StatusFailed {
		t.Errorf("Expected 429 to fail fast, got status: %s", dbMsg1.Status)
	}
	mu.Unlock()

	// 2. Test Empty Latch (should not retry)
	msg2 := db.Message{ID: "msg-latch", ThreadID: "thread-latch", Content: "TestEmptyLatch"}
	_ = insertMessage(store, msg2)
	pool.Enqueue(msg2)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for empty latch")
	}

	mu.Lock()
	dbMsg2, _ := getMessage(store, "msg-latch")
	if dbMsg2.Status != db.StatusFailed {
		t.Errorf("Expected empty latch to fail fast, got status: %s", dbMsg2.Status)
	}
	mu.Unlock()
}

func TestThreadColdStartSummarization_Integration(t *testing.T) {
	store := setupTestStore(t)

	// Register channel snapshots in cache
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "thread-100",
		ParentID: "channel-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	CacheDiscordChannel(&discordgo.Channel{
		ID:       "channel-1",
		ParentID: "",
		Type:     discordgo.ChannelTypeGuildText,
	})

	var mu sync.Mutex
	var lastPromptReceived string
	var llmCallCount int

	doneCh := make(chan struct{}, 10)

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
			mu.Lock()
			llmCallCount++
			mu.Unlock()
			if strings.Contains(prompt, "fail-llm") {
				return "", errors.New("llm error")
			}
			if strings.Contains(prompt, "invalid-xml") {
				return "No tags summary", nil
			}
			return "<THREAD_SUMMARY>\n- Technical Decision: Architectural shift to SQLite\n</THREAD_SUMMARY>", nil
		},
		HistoryFetcher: func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error) {
			if strings.Contains(channelID, "no-history") {
				return nil, nil
			}
			content := "Let's adopt SQLite for storage"
			if strings.Contains(channelID, "fail-llm") {
				content = "fail-llm in thread history"
			}
			return []HistoryMessage{
				{
					ID:         "msg-hist-1",
					AuthorName: "Alice",
					Role:       "User",
					Content:    content,
					CreatedAt:  time.Now().UTC().Add(-10 * time.Minute),
				},
			}, nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			mu.Lock()
			lastPromptReceived = prompt
			mu.Unlock()
			stderr = "Starting conversation update stream for b1b7fb78-d85d-4bd6-ba9e-330f1e30596f\n"
			return mockJSONResponse("b1b7fb78-d85d-4bd6-ba9e-330f1e30596f", "Done"), stderr, 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) (stop func()) { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. Thread Cold Start -> Should summarize and inject <THREAD_SUMMARY>
	t.Run("Thread Cold Start Summarizes History", func(t *testing.T) {
		msg := db.Message{ID: "m1", ThreadID: "thread-100", Content: "Hello thread"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for thread message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		calls := llmCallCount
		mu.Unlock()

		if !strings.Contains(prompt, "<THREAD_SUMMARY>") {
			t.Errorf("Expected prompt to contain <THREAD_SUMMARY>, got:\n%s", prompt)
		}
		if calls != 1 {
			t.Errorf("Expected 1 LLM call, got %d", calls)
		}

		// Verify saved summary and watermark in SQLite
		sum, lastMsgID, err := getThreadSummary(store, "thread-100")
		if err != nil || sum == "" || lastMsgID != "msg-hist-1" {
			t.Errorf("Expected saved summary and watermark msg-hist-1, got sum=%q, watermark=%q, err=%v", sum, lastMsgID, err)
		}
	})

	// 2. Main Channel -> Should NOT summarize history
	t.Run("Main Channel Does Not Summarize", func(t *testing.T) {
		mu.Lock()
		prevCalls := llmCallCount
		mu.Unlock()

		msg := db.Message{ID: "m2", ThreadID: "channel-1", Content: "Hello main channel"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for main channel message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		calls := llmCallCount
		mu.Unlock()

		if strings.Contains(prompt, "<THREAD_SUMMARY>") {
			t.Errorf("Main channel prompt should NOT contain <THREAD_SUMMARY>, got:\n%s", prompt)
		}
		if calls != prevCalls {
			t.Errorf("Expected no additional LLM call for main channel, got %d calls (was %d)", calls, prevCalls)
		}
	})

	// 3. Thread Cold Start Watermark Cache Hit -> Should reuse cached summary without calling LLM again
	t.Run("Thread Cold Start Cache Watermark Hit", func(t *testing.T) {
		mu.Lock()
		prevCalls := llmCallCount
		mu.Unlock()

		CacheDiscordChannel(&discordgo.Channel{
			ID:       "thread-cached",
			ParentID: "channel-1",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		})
		_ = saveThreadSummary(store, "thread-cached", "<THREAD_SUMMARY>\n- Existing cached summary\n</THREAD_SUMMARY>", "msg-hist-1")

		msg := db.Message{ID: "m3", ThreadID: "thread-cached", Content: "Cached test"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for cached thread message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		calls := llmCallCount
		mu.Unlock()

		if !strings.Contains(prompt, "Existing cached summary") {
			t.Errorf("Expected prompt to contain cached summary, got:\n%s", prompt)
		}
		if calls != prevCalls {
			t.Errorf("Expected 0 new LLM calls due to cache hit, but got %d calls (was %d)", calls, prevCalls)
		}
	})

	// 4. Fallback on LLM Failure / Malformed XML
	t.Run("Fallback on LLM Failure", func(t *testing.T) {
		CacheDiscordChannel(&discordgo.Channel{
			ID:       "thread-fail-llm",
			ParentID: "channel-1",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		})

		msg := db.Message{ID: "m4", ThreadID: "thread-fail-llm", Content: "fail-llm request"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Timeout waiting for failing thread message")
		}

		mu.Lock()
		prompt := lastPromptReceived
		mu.Unlock()

		if strings.Contains(prompt, "<THREAD_SUMMARY>") {
			t.Errorf("Expected prompt NOT to contain <THREAD_SUMMARY> on LLM failure, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, "<CHANNEL_HISTORY>") {
			t.Errorf("Expected fallback to raw <CHANNEL_HISTORY>, got:\n%s", prompt)
		}
	})
}

func TestProcessBurst_ErrorClassificationMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		threadID             string
		existingSession      bool
		setupFiles           func(t *testing.T, tmpDir string) string
		runnerFunc           func(attempts *int, validUUID string) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error)
		expectedAttempts     int
		expectedStatus       string
		checkLatchedCleared  bool
		checkNoticeDelivered bool
		prohibitUnavailable  bool
	}{
		{
			name:     "ColdStart429_RetriesTransiently",
			threadID: "thread-cold-429",
			setupFiles: func(t *testing.T, tmpDir string) string {
				validUUID := uuid.New().String()
				pbPath := filepath.Join(tmpDir, ".gemini", "antigravity", "conversations", validUUID+".pb")
				_ = os.MkdirAll(filepath.Dir(pbPath), 0755)
				_ = os.WriteFile(pbPath, []byte("protobuf-data"), 0644)
				return validUUID
			},
			runnerFunc: func(attempts *int, validUUID string) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					*attempts++
					if *attempts < 3 {
						return "", "HTTP 429: Resource has been exhausted (e.g. check quota)", 1, fmt.Errorf("exit status 1")
					}
					return fmt.Sprintf(`{"conversation_id":%q,"response":"Success after 429 backoff"}`, validUUID), "", 0, nil
				}
			},
			expectedAttempts: 3,
			expectedStatus:   db.StatusCompleted,
		},
		{
			name:            "NonTransient_FailsFastOnAttempt1",
			threadID:        "thread-fast-fail",
			existingSession: true,
			runnerFunc: func(attempts *int, validUUID string) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					*attempts++
					return "", "Error: invalid api key provided", 1, fmt.Errorf("exit status 1")
				}
			},
			expectedAttempts:     1,
			expectedStatus:       db.StatusFailed,
			checkLatchedCleared:  true,
			checkNoticeDelivered: true,
			prohibitUnavailable:  true,
		},
		{
			name:     "Exit0_NonTransientJSON_FailsFastOnAttempt1",
			threadID: "thread-flag-fail",
			runnerFunc: func(attempts *int, validUUID string) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					*attempts++
					return `{"status":"ERROR","error":"unknown flag: --unsupported-flag"}`, "", 0, nil
				}
			},
			expectedAttempts: 1,
			expectedStatus:   db.StatusFailed,
		},
		{
			name:     "UnknownError_RetriesTransientByDefault",
			threadID: "thread-unknown-retry",
			setupFiles: func(t *testing.T, tmpDir string) string {
				validUUID := uuid.New().String()
				pbPath := filepath.Join(tmpDir, ".gemini", "antigravity", "conversations", validUUID+".pb")
				_ = os.MkdirAll(filepath.Dir(pbPath), 0755)
				_ = os.WriteFile(pbPath, []byte("protobuf-data"), 0644)
				return validUUID
			},
			runnerFunc: func(attempts *int, validUUID string) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					*attempts++
					if *attempts < 3 {
						return "", "some mysterious unexpected exit from child process", 1, fmt.Errorf("exit status 1")
					}
					return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Success on attempt 3!"}`, validUUID), "", 0, nil
				}
			},
			expectedAttempts: 3,
			expectedStatus:   db.StatusCompleted,
		},
		{
			name:     "ColdStartContextWindow_FailsFast",
			threadID: "thread-ctx-cold",
			runnerFunc: func(attempts *int, validUUID string) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					*attempts++
					return `{"status":"ERROR","error":"maximum context length exceeded"}`, "", 0, nil
				}
			},
			expectedAttempts: 1,
			expectedStatus:   db.StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			store := setupTestStore(t)

			var validUUID string
			if tt.setupFiles != nil {
				validUUID = tt.setupFiles(t, tmpDir)
			}
			if tt.existingSession {
				_ = saveSessionID(store, tt.threadID, uuid.New().String())
			}

			var mu sync.Mutex
			attempts := 0
			var deliveredText string
			doneCh := make(chan struct{}, 1)

			runner := tt.runnerFunc(&attempts, validUUID)

			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: session.New(tmpDir, ""),
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    3,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					mu.Lock()
					defer mu.Unlock()
					return runner(ctx, agyBin, prompt, sessionID, apiKey, model, timeoutMinutes)
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredText = text
					mu.Unlock()
					return nil
				},
				TypingFunc: noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					doneCh <- struct{}{}
				},
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
				t.Errorf("Expected %d attempts, got %d", tt.expectedAttempts, attempts)
			}

			dbMsg, _ := getMessage(store, msg.ID)
			if dbMsg.Status != tt.expectedStatus {
				t.Errorf("Expected message status %s, got %s", tt.expectedStatus, dbMsg.Status)
			}

			if tt.checkLatchedCleared {
				savedSess, _ := getSessionID(store, tt.threadID)
				if savedSess != "" {
					t.Errorf("Expected latched session to be cleared, got %q", savedSess)
				}
			}
			if tt.checkNoticeDelivered && deliveredText == "" {
				t.Errorf("Expected delivery notification to be sent to user")
			}
			if tt.prohibitUnavailable && strings.Contains(deliveredText, "temporarily unavailable") {
				t.Errorf("Expected non-transient failure NOT to gaslight user with temporary model unavailability, got: %s", deliveredText)
			}
		})
	}
}

func TestGetSessionLastActivity_Suite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "ChecksBothDBAndDiskLogs",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				tmpDir := t.TempDir()
				sessMgr := session.New(tmpDir, "")

				threadID := "thread-act-1"
				sessionID := "sess-act-uuid-1"

				// 1. Initially empty thread -> cold thread, zero time
				act, isCold, err := GetSessionLastActivity(store, threadID, sessMgr)
				if err != nil || !isCold || !act.IsZero() {
					t.Fatalf("expected cold thread with zero time, got act=%v, isCold=%v, err=%v", act, isCold, err)
				}

				// 2. Insert session with updated_at = 20m ago
				tSess := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Second)
				store.SetSessionInfo(db.SessionInfo{
					ThreadID:          threadID,
					InternalSessionID: sessionID,
					TurnCount:         1,
					CreatedAt:         tSess,
					UpdatedAt:         tSess,
				})

				act, isCold, err = GetSessionLastActivity(store, threadID, sessMgr)
				if err != nil || isCold {
					t.Fatalf("expected non-cold thread, got act=%v, isCold=%v, err=%v", act, isCold, err)
				}
				if !act.Equal(tSess) {
					t.Errorf("expected session updated_at %v, got %v", tSess, act)
				}

				// 3. Insert completed message with updated_at = 10m ago (newer than session updated_at)
				tMsg := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
				_ = insertMessage(store, db.Message{
					ID:           "msg-1",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "some answer",
					CreatedAt:    tMsg,
					UpdatedAt:    tMsg,
				})

				act, isCold, err = GetSessionLastActivity(store, threadID, sessMgr)
				if err != nil || isCold {
					t.Fatalf("expected non-cold thread, got act=%v, isCold=%v, err=%v", act, isCold, err)
				}
				if !act.Equal(tMsg) {
					t.Errorf("expected msg updated_at %v, got %v", tMsg, act)
				}

				// 4. Create on-disk task log with timestamp = 2m ago (newer than DB)
				tasksDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessionID, ".system_generated", "tasks")
				if err := os.MkdirAll(tasksDir, 0755); err != nil {
					t.Fatalf("failed to create tasksDir: %v", err)
				}
				tDisk := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
				taskLog := filepath.Join(tasksDir, "task-1.log")
				if err := os.WriteFile(taskLog, []byte("build output\n"), 0644); err != nil {
					t.Fatalf("failed to write taskLog: %v", err)
				}
				_ = os.Chtimes(taskLog, tDisk, tDisk)

				act, isCold, err = GetSessionLastActivity(store, threadID, sessMgr)
				if err != nil || isCold {
					t.Fatalf("expected non-cold thread, got act=%v, isCold=%v, err=%v", act, isCold, err)
				}
				if !act.Equal(tDisk) {
					t.Errorf("expected disk activity %v to win, got %v", tDisk, act)
				}
			},
		},
		{
			name: "RotatedSessionNotNew",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				threadID := "thread-rotated-1"

				store.SetSessionInfo(db.SessionInfo{
					ThreadID:          threadID,
					InternalSessionID: "",
					TurnCount:         0,
					UpdatedAt:         time.Now().UTC().Add(-15 * time.Minute),
				})

				tCompleted := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
				_ = insertMessage(store, db.Message{
					ID:           "msg-prev",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "previous answer",
					CreatedAt:    tCompleted,
					UpdatedAt:    tCompleted,
				})

				act, isCold, err := GetSessionLastActivity(store, threadID)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if isCold {
					t.Errorf("expected isCold=false for rotated session with completed turns, got true")
				}
				if !act.Equal(tCompleted) {
					t.Errorf("expected lastActivity %v, got %v", tCompleted, act)
				}
			},
		},
		{
			name: "IgnoresExpiredStaleSentinel",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				threadID := "thread-stale-sentinel"

				_ = insertMessage(store, db.Message{
					ID:           "msg-stale-drop",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "[EXPIRED_STALE]",
				})
				_ = insertMessage(store, db.Message{
					ID:           "msg-ambient",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "[AMBIENT: IGNORED]",
				})

				act, isCold, err := GetSessionLastActivity(store, threadID)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !isCold {
					t.Errorf("expected isCold=true when only stale/ambient messages exist, got false")
				}
				if !act.IsZero() {
					t.Errorf("expected zero activity time, got %v", act)
				}
			},
		},
		{
			name: "FailOpenOnDBError",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				_ = store.Close()

				act, isCold, err := GetSessionLastActivity(store, "thread-closed")
				if err == nil {
					t.Fatalf("expected error from closed DB, got nil")
				}
				if isCold {
					t.Errorf("expected isCold=false on DB error, got true")
				}
				if !act.IsZero() {
					t.Errorf("expected zero activity time on DB error, got %v", act)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func TestProcessBurst_StalenessMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		threadID             string
		sessionID            string
		msgAge               time.Duration
		retryCount           int
		restartCount         int
		setupState           func(t *testing.T, store *db.FakeStore, threadID, sessionID string, tmpDir string)
		expectRunnerCalls    int
		expectStatus         string
		expectExpiredStale   bool
		prohibitExpiredStale bool
	}{
		{
			name:      "ExistingSessionRecentDiskActivity_Retained",
			threadID:  "thread-burst-disk-active",
			sessionID: "sess-burst-disk-active",
			msgAge:    35 * time.Minute,
			setupState: func(t *testing.T, store *db.FakeStore, threadID, sessionID string, tmpDir string) {
				_ = saveSessionID(store, threadID, sessionID)
				tasksDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessionID, ".system_generated", "tasks")
				_ = os.MkdirAll(tasksDir, 0755)
				tDisk := time.Now().UTC().Add(-5 * time.Minute)
				taskLog := filepath.Join(tasksDir, "task-1.log")
				_ = os.WriteFile(taskLog, []byte("running build"), 0644)
				_ = os.Chtimes(taskLog, tDisk, tDisk)
			},
			expectRunnerCalls:    1,
			expectStatus:         db.StatusCompleted,
			prohibitExpiredStale: true,
		},
		{
			name:     "ExistingSessionRecentDBActivity_Retained",
			threadID: "thread-burst-db-active",
			msgAge:   35 * time.Minute,
			setupState: func(t *testing.T, store *db.FakeStore, threadID, sessionID string, tmpDir string) {
				tPrev := time.Now().UTC().Add(-5 * time.Minute)
				_ = insertMessage(store, db.Message{
					ID:           "msg-prev-active",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "Finished earlier turn",
					CreatedAt:    tPrev,
					UpdatedAt:    tPrev,
				})
			},
			expectRunnerCalls: 1,
		},
		{
			name:      "ExistingSessionInactive_Dropped",
			threadID:  "thread-burst-inactive",
			sessionID: "sess-burst-inactive",
			msgAge:    35 * time.Minute,
			setupState: func(t *testing.T, store *db.FakeStore, threadID, sessionID string, tmpDir string) {
				tInactive := time.Now().UTC().Add(-45 * time.Minute)
				store.SetSessionInfo(db.SessionInfo{
					ThreadID:          threadID,
					InternalSessionID: sessionID,
					TurnCount:         1,
					UpdatedAt:         tInactive,
				})
				_ = insertMessage(store, db.Message{
					ID:           "msg-old-completed",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "old answer",
					CreatedAt:    tInactive,
					UpdatedAt:    tInactive,
				})
			},
			expectRunnerCalls:  0,
			expectExpiredStale: true,
		},
		{
			name:               "NewSession_Dropped",
			threadID:           "thread-new-drop",
			msgAge:             31 * time.Minute,
			expectRunnerCalls:  0,
			expectExpiredStale: true,
		},
		{
			name:      "HardCeiling_Dropped",
			threadID:  "thread-hard-ceiling",
			sessionID: "sess-ceiling",
			msgAge:    130 * time.Minute,
			setupState: func(t *testing.T, store *db.FakeStore, threadID, sessionID string, tmpDir string) {
				store.SetSessionInfo(db.SessionInfo{
					ThreadID:          threadID,
					InternalSessionID: sessionID,
					TurnCount:         1,
					UpdatedAt:         time.Now().UTC(),
				})
				_ = insertMessage(store, db.Message{
					ID:           "msg-recent",
					ThreadID:     threadID,
					Status:       db.StatusCompleted,
					ResponseText: "recent turn",
					CreatedAt:    time.Now().UTC(),
					UpdatedAt:    time.Now().UTC(),
				})
			},
			expectRunnerCalls:  0,
			expectExpiredStale: true,
		},
		{
			name:                 "RecoveredMessage_Retained",
			threadID:             "thread-recovered-msg",
			sessionID:            "sess-rec",
			msgAge:               45 * time.Minute,
			retryCount:           1,
			expectRunnerCalls:    1,
			prohibitExpiredStale: true,
		},
		{
			name:                 "RestartedMessage_Retained",
			threadID:             "thread-restarted-staleness",
			sessionID:            "d2222222-2222-3333-4444-555555555555",
			msgAge:               45 * time.Minute,
			restartCount:         1,
			expectRunnerCalls:    1,
			prohibitExpiredStale: true,
		},
		{
			name:               "RestartedMessage_HardCeiling_Dropped",
			threadID:           "thread-restarted-hard-ceiling",
			msgAge:             130 * time.Minute,
			restartCount:       2,
			expectRunnerCalls:  0,
			expectExpiredStale: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			tmpDir := t.TempDir()

			if tt.setupState != nil {
				tt.setupState(t, store, tt.threadID, tt.sessionID, tmpDir)
			}

			var runnerCalls int
			var mu sync.Mutex
			doneCh := make(chan struct{})

			sessIDToReturn := tt.sessionID
			if sessIDToReturn == "" {
				sessIDToReturn = "sess-fallback"
			}

			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: session.New(tmpDir, ""),
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    3,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					mu.Lock()
					runnerCalls++
					mu.Unlock()
					return mockJSONResponse(sessIDToReturn, "executed"), "", 0, nil
				},
				DeliveryFunc: noopDelivery,
				TypingFunc:   noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					close(doneCh)
				},
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{
				ID:           "msg-" + tt.name,
				ThreadID:     tt.threadID,
				AuthorID:     "user-1",
				Content:      "prompt " + tt.name,
				Status:       db.StatusPending,
				RetryCount:   tt.retryCount,
				RestartCount: tt.restartCount,
				CreatedAt:    time.Now().UTC().Add(-tt.msgAge),
			}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			waitForCompletion(t, doneCh, 3*time.Second, tt.name)

			mu.Lock()
			calls := runnerCalls
			mu.Unlock()

			if calls != tt.expectRunnerCalls {
				t.Errorf("Expected %d runner calls, got %d", tt.expectRunnerCalls, calls)
			}

			dbMsg, _ := getMessage(store, msg.ID)
			if tt.expectStatus != "" && dbMsg.Status != tt.expectStatus {
				t.Errorf("Expected message status %s, got %s", tt.expectStatus, dbMsg.Status)
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

func TestRecoverInterrupted_PoisonPillSuite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "PreservesExecutionRetryBudget",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				threadID := "thread-retry-budget"
				msg := db.Message{
					ID:           "msg-restarted-budget",
					ThreadID:     threadID,
					AuthorID:     "user-1",
					Content:      "turn interrupted by container restart",
					Status:       db.StatusProcessing,
					RestartCount: 2,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
					UpdatedAt:    time.Now().UTC().Add(-10 * time.Minute),
				}
				if err := insertMessage(store, msg); err != nil {
					t.Fatalf("InsertMessage failed: %v", err)
				}

				var runnerCalls int
				var mu sync.Mutex
				doneCh := make(chan struct{})

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    3,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						mu.Lock()
						runnerCalls++
						mu.Unlock()
						return "", "transient connection error", 1, errors.New("exit status 1")
					},
					DeliveryFunc: noopDelivery,
					TypingFunc:   noopTyping,
					OnMessageCompleted: func(msg db.Message, finalStatus string) {
						close(doneCh)
					},
				})
				pool.Start()
				defer pool.Stop()

				RecoverInterrupted(store, pool)

				waitForCompletion(t, doneCh, 5*time.Second, "retry budget processing")

				mu.Lock()
				calls := runnerCalls
				mu.Unlock()

				if calls != 3 {
					t.Errorf("Expected 3 runner execution attempts (full retry budget), got %d", calls)
				}

				dbMsg, err := getMessage(store, "msg-restarted-budget")
				if err != nil || dbMsg == nil {
					t.Fatalf("Failed to fetch message: %v", err)
				}
				if dbMsg.Status != db.StatusFailed {
					t.Errorf("Expected final status %s, got %s", db.StatusFailed, dbMsg.Status)
				}
				if dbMsg.RestartCount != 3 {
					t.Errorf("Expected final restart_count 3, got %d", dbMsg.RestartCount)
				}
				if dbMsg.RetryCount != 3 {
					t.Errorf("Expected final retry_count 3, got %d", dbMsg.RetryCount)
				}
			},
		},
		{
			name: "RestartPoisonPill_Boundaries",
			run: func(t *testing.T) {
				store := setupTestStore(t)

				msgPoison := db.Message{
					ID:           "msg-poison-restart",
					ThreadID:     "thread-poison-restart",
					AuthorID:     "user-1",
					Content:      "crash-looping container trigger",
					Status:       db.StatusProcessing,
					RestartCount: DefaultMaxRestarts,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC().Add(-15 * time.Minute),
				}
				msgValid := db.Message{
					ID:           "msg-valid-restart",
					ThreadID:     "thread-valid-restart",
					AuthorID:     "user-2",
					Content:      "valid restart message",
					Status:       db.StatusProcessing,
					RestartCount: DefaultMaxRestarts - 1,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
				}

				_ = insertMessage(store, msgPoison)
				_ = insertMessage(store, msgValid)

				var mu sync.Mutex
				var completedIDs []string
				var wg sync.WaitGroup
				wg.Add(1)

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    3,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						return mockJSONResponse("d1111111-2222-3333-4444-555555555555", "Processed OK"), "", 0, nil
					},
					DeliveryFunc: noopDelivery,
					TypingFunc:   noopTyping,
					OnMessageCompleted: func(msg db.Message, finalStatus string) {
						mu.Lock()
						completedIDs = append(completedIDs, msg.ID)
						mu.Unlock()
						wg.Done()
					},
				})
				pool.Start()
				defer pool.Stop()
				pool.SetDiscordSession(&discordgo.Session{})

				RecoverInterrupted(store, pool)

				wg.Wait()

				mu.Lock()
				defer mu.Unlock()

				if len(completedIDs) != 1 || completedIDs[0] != "msg-valid-restart" {
					t.Errorf("Expected only msg-valid-restart to complete, got: %v", completedIDs)
				}

				poisonDB, err := getMessage(store, "msg-poison-restart")
				if err != nil || poisonDB == nil {
					t.Fatalf("Failed to query poison message: %v", err)
				}
				if poisonDB.Status != db.StatusFailed {
					t.Errorf("Expected poison pill status FAILED, got: %s", poisonDB.Status)
				}
				if !strings.Contains(poisonDB.ErrorMessage, "exceeded restart limit") {
					t.Errorf("Expected error to mention exceeded restart limit, got: %s", poisonDB.ErrorMessage)
				}

				validDB, err := getMessage(store, "msg-valid-restart")
				if err != nil || validDB == nil {
					t.Fatalf("Failed to query valid message: %v", err)
				}
				if validDB.Status != db.StatusCompleted {
					t.Errorf("Expected valid message status COMPLETED, got: %s", validDB.Status)
				}
				if validDB.RestartCount != DefaultMaxRestarts {
					t.Errorf("Expected valid message restart_count %d, got %d", DefaultMaxRestarts, validDB.RestartCount)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}


func TestWorkerPool_EffortRouting(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	var executedModels []string
	var mu sync.Mutex

	appCfg := config.NewFromData(&config.ConfigData{
		Model:          "claude-opus-4-6-thinking",
		LowEffortModel: "claude-sonnet-4-6-thinking",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})

	doneCh := make(chan struct{}, 2)

	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: session.New(t.TempDir(), ""),
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			executedModels = append(executedModels, model)
			mu.Unlock()
			return mockJSONResponse(sessionID, "Done!"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			doneCh <- struct{}{}
		},
	})
	pool.Start()
	defer pool.Stop()

	// 1. High effort / default message
	runID1 := "run-high-1"
	_ = createScheduleRun(store, db.ScheduleRun{
		ID:           runID1,
		ScheduleID:   "cron-high",
		ScheduleType: "cron",
		MessageID:    "msg-high-1",
		TargetID:     "thread-high",
		ThreadID:     "thread-high",
		Title:        "High Routine",
		Prompt:       "Run high routine",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC(),
		Effort:       "high",
	})
	msgHigh := db.Message{
		ID:            "msg-high-1",
		ThreadID:      "thread-high",
		AuthorID:      "scheduler",
		AuthorName:    "Scheduler",
		Content:       "Run high routine",
		Status:        db.StatusPending,
		ScheduleRunID: runID1,
		Effort:        "high",
		CreatedAt:     time.Now().UTC(),
	}
	_ = insertMessage(store, msgHigh)
	pool.Enqueue(msgHigh)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for high effort message completion")
	}

	// 2. Low effort message
	runID2 := "run-low-1"
	_ = createScheduleRun(store, db.ScheduleRun{
		ID:           runID2,
		ScheduleID:   "cron-low",
		ScheduleType: "cron",
		MessageID:    "msg-low-1",
		TargetID:     "thread-low",
		ThreadID:     "thread-low",
		Title:        "Low Routine",
		Prompt:       "Run low routine",
		Status:       "enqueued",
		StartedAt:    time.Now().UTC(),
		Effort:       "low",
	})
	msgLow := db.Message{
		ID:            "msg-low-1",
		ThreadID:      "thread-low",
		AuthorID:      "scheduler",
		AuthorName:    "Scheduler",
		Content:       "Run low routine",
		Status:        db.StatusPending,
		ScheduleRunID: runID2,
		Effort:        "low",
		CreatedAt:     time.Now().UTC(),
	}
	_ = insertMessage(store, msgLow)
	pool.Enqueue(msgLow)

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for low effort message completion")
	}

	mu.Lock()
	models := append([]string{}, executedModels...)
	mu.Unlock()

	if len(models) != 2 {
		t.Fatalf("Expected 2 runner invocations, got %d (%v)", len(models), models)
	}
	if models[0] != "claude-opus-4-6-thinking" {
		t.Errorf("Expected first run with high effort model 'claude-opus-4-6-thinking', got %q", models[0])
	}
	if models[1] != "claude-sonnet-4-6-thinking" {
		t.Errorf("Expected second run with low effort model 'claude-sonnet-4-6-thinking', got %q", models[1])
	}

	// Verify schedule run records recorded the executed model
	runsHigh, _, _ := getScheduleRunsPaginated(store, 10, 0, "cron-high", "")
	if len(runsHigh) != 1 || runsHigh[0].Model != "claude-opus-4-6-thinking" {
		t.Errorf("Expected schedule run high model to be 'claude-opus-4-6-thinking', got %+v", runsHigh)
	}
	runsLow, _, _ := getScheduleRunsPaginated(store, 10, 0, "cron-low", "")
	if len(runsLow) != 1 || runsLow[0].Model != "claude-sonnet-4-6-thinking" {
		t.Errorf("Expected schedule run low model to be 'claude-sonnet-4-6-thinking', got %+v", runsLow)
	}
}

func TestProcessBurst_RecordsTokensTelemetry(t *testing.T) {
	store := setupTestStore(t)

	customModel := "test-token-model"
	mockOutput := `{"conversation_id":"c1111111-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":150,"output_tokens":75,"thinking_tokens":30,"cache_read_tokens":10,"total_tokens":265}}`

	pool := New(nil, WorkerPoolConfig{
		Store: store,
		Model: customModel,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockOutput, "", 0, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	threadID := "chan-token-test-1"
	t0 := time.Now().UTC()
	msg := db.Message{
		ID:        "msg-token-1",
		ThreadID:  threadID,
		Content:   "<@aerial> Question Token Test",
		Status:    db.StatusPending,
		CreatedAt: t0,
		UpdatedAt: t0,
	}
	_ = insertMessage(store, msg)

	pool.processBurst([]db.Message{msg})

	// Query metrics registry
	handler := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="total"} 265`) {
		t.Errorf("expected total tokens 265 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="input"} 150`) {
		t.Errorf("expected input tokens 150 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="output"} 75`) {
		t.Errorf("expected output tokens 75 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="thinking"} 30`) {
		t.Errorf("expected thinking tokens 30 for test-token-model, got metrics body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="cache_read"} 10`) {
		t.Errorf("expected cache_read tokens 10 for test-token-model, got metrics body: %s", body)
	}
}

func TestProcessBurst_RecordsTokensTelemetry_SnowflakeChannelSanitized(t *testing.T) {
	store := setupTestStore(t)

	customModel := "test-snowflake-model"
	mockOutput := `{"conversation_id":"c2222222-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":100,"output_tokens":50,"thinking_tokens":20,"cache_read_tokens":5,"total_tokens":175}}`

	pool := New(nil, WorkerPoolConfig{
		Store: store,
		Model: customModel,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockOutput, "", 0, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	// Use numeric Discord snowflake ID
	threadID := "1534436119888793750"
	t0 := time.Now().UTC()
	msg := db.Message{
		ID:        "msg-token-snowflake-1",
		ThreadID:  threadID,
		Content:   "<@aerial> Test snowflake sanitization",
		Status:    db.StatusPending,
		CreatedAt: t0,
		UpdatedAt: t0,
	}
	_ = insertMessage(store, msg)

	pool.processBurst([]db.Message{msg})

	handler := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "1534436119888793750") {
		t.Errorf("expected snowflake ID not to appear in metrics body, got: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="unknown",model="test-snowflake-model",type="total"} 175`) {
		t.Errorf("expected total tokens 175 for test-snowflake-model under channel unknown, got metrics body: %s", body)
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
		ID:       threadID,
		Name:     "status-thread",
		Type:     discordgo.ChannelTypeGuildPublicThread,
		ParentID: "parent-chan-1",
	})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			finalDelivered.Add(1)
			return nil
		},
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error) {
			if opts.StepUpdateHandler != nil {
				opts.StepUpdateHandler(&runner.StepUpdateEvent{
					StepType: "tool",
					ToolName: "view_file",
					State:    "ACTIVE",
				})
			}
			return `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"Hello from thread!"}`, "", 0, nil
		},
	})
	pool.SetDiscordSession(sess)

	msg := db.Message{
		ID:        "msg-thread-1",
		ThreadID:  threadID,
		AuthorID:  "user-1",
		Content:   "check my file",
		CreatedAt: time.Now().UTC(),
	}
	_ = insertMessage(store, msg)

	pool.processBurst([]db.Message{msg})

	// Final response was delivered
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
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
	})

	msg := db.Message{
		ID:        "msg-hermetic-1",
		ThreadID:  "thread-hermetic-1",
		AuthorID:  "user-1",
		Content:   "Hermetic test without retriever",
		CreatedAt: time.Now().UTC(),
	}
	_ = insertMessage(store, msg)

	poolWithDB.processBurst([]db.Message{msg})
	if !runnerCalled {
		t.Errorf("Expected runner to be called cleanly without error")
	}
}

func TestProcessBurst_GracefulShutdown_ResetsToPendingWithoutRestartPenalty(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	msg := db.Message{
		ID:           "msg-graceful-shutdown",
		ThreadID:     "thread-graceful",
		Content:      "task interrupted by deployment",
		Status:       db.StatusPending,
		RestartCount: 0,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC(),
	}
	_ = insertMessage(store, msg)

	// Simulate cancelled pool context (SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-cancel context

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store,
	})
	pool.ctx = ctx

	// Process burst under cancelled context
	pool.processBurst([]db.Message{msg})

	updated, err := getMessage(store, "msg-graceful-shutdown")
	if err != nil || updated == nil {
		t.Fatalf("Failed to fetch message: %v", err)
	}
	if updated.Status != db.StatusPending {
		t.Errorf("Expected status %s, got %s", db.StatusPending, updated.Status)
	}
	if updated.RestartCount != 0 {
		t.Errorf("Expected restart_count 0, got %d", updated.RestartCount)
	}
}

func TestRecoverInterrupted_PoisonPillAndStrandedSuite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "LongRunningTask_DeploymentVelocityProtection",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				msg := db.Message{
					ID:           "msg-long-running",
					ThreadID:     "thread-long",
					AuthorID:     "user-1",
					Content:      "long task",
					Status:       db.StatusProcessing,
					RestartCount: DefaultMaxRestarts,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
					UpdatedAt:    time.Now().UTC().Add(-5 * time.Minute),
				}
				_ = insertMessage(store, msg)
				store.SetMessageUpdatedAt(msg.ID, time.Now().UTC().Add(-5*time.Minute))

				var mu sync.Mutex
				var completedIDs []string
				doneCh := make(chan struct{})

				pool := NewWorkerPool(WorkerPoolConfig{
					Store: store,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						return mockJSONResponse("e1111111-2222-3333-4444-555555555555", "Recovered successfully"), "", 0, nil
					},
					DeliveryFunc: noopDelivery,
					TypingFunc:   noopTyping,
					OnMessageCompleted: func(m db.Message, finalStatus string) {
						mu.Lock()
						completedIDs = append(completedIDs, m.ID)
						mu.Unlock()
						close(doneCh)
					},
				})
				pool.Start()
				defer pool.Stop()

				RecoverInterrupted(store, pool)

				waitForCompletion(t, doneCh, 3*time.Second, "long running task recovery")

				updated, _ := getMessage(store, "msg-long-running")
				if updated.Status != db.StatusCompleted {
					t.Errorf("Expected long running task to be completed, got %s", updated.Status)
				}
				if updated.RestartCount != 4 {
					t.Errorf("Expected restart_count 4 after recovery, got %d", updated.RestartCount)
				}
			},
		},
		{
			name: "PoisonPill_HardCeiling",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				msg := db.Message{
					ID:           "msg-hard-ceiling",
					ThreadID:     "thread-ceiling",
					AuthorID:     "user-1",
					Content:      "hard ceiling test",
					Status:       db.StatusProcessing,
					RestartCount: 5,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC().Add(-20 * time.Minute),
				}
				_ = insertMessage(store, msg)
				store.SetMessageUpdatedAt(msg.ID, time.Now().UTC().Add(-5*time.Minute))

				var deliveredNotifs []string
				var mu sync.Mutex

				pool := NewWorkerPool(WorkerPoolConfig{
					Store: store,
					NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
						return "Poison pill dropped"
					},
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						mu.Lock()
						deliveredNotifs = append(deliveredNotifs, text)
						mu.Unlock()
						return nil
					},
				})
				pool.Start()
				defer pool.Stop()

				RecoverInterrupted(store, pool)

				updated, _ := getMessage(store, "msg-hard-ceiling")
				if updated.Status != db.StatusFailed {
					t.Errorf("Expected hard ceiling message to be marked FAILED, got %s", updated.Status)
				}
				if !strings.Contains(updated.ErrorMessage, "poison pill") {
					t.Errorf("Expected poison pill error message, got %s", updated.ErrorMessage)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(deliveredNotifs) == 0 {
					t.Errorf("Expected poison pill notification delivered to Discord")
				}
			},
		},
		{
			name: "DiscordSessionDeliversPoisonNotice",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				msg := db.Message{
					ID:           "msg-delivery-check",
					ThreadID:     "thread-delivery-check",
					AuthorID:     "user-1",
					Content:      "poison message",
					Status:       db.StatusProcessing,
					RestartCount: DefaultMaxRestarts,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC(),
					UpdatedAt:    time.Now().UTC(),
				}
				_ = insertMessage(store, msg)

				var capturedSession *discordgo.Session
				var deliveredThreadID string
				var deliveredText string
				var mu sync.Mutex

				mockSession := &discordgo.Session{
					State: discordgo.NewState(),
				}

				pool := NewWorkerPool(WorkerPoolConfig{
					Store: store,
					NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
						return "Poison pill notice"
					},
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						mu.Lock()
						capturedSession = s
						deliveredThreadID = channelID
						deliveredText = text
						mu.Unlock()
						return nil
					},
				})
				pool.SetDiscordSession(mockSession)
				pool.Start()
				defer pool.Stop()

				RecoverInterrupted(store, pool)

				mu.Lock()
				defer mu.Unlock()
				if capturedSession != mockSession {
					t.Errorf("Expected DeliveryFunc to receive mockSession %p, got %p", mockSession, capturedSession)
				}
				if deliveredThreadID != "thread-delivery-check" {
					t.Errorf("Expected thread-delivery-check, got %s", deliveredThreadID)
				}
				if deliveredText != "Poison pill notice" {
					t.Errorf("Expected poison notice, got %s", deliveredText)
				}
			},
		},
		{
			name: "CoverageEdgeCases",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				msg := db.Message{
					ID:           "msg-edge-case-cov",
					ThreadID:     "thread-cov",
					AuthorID:     "user-1",
					Content:      "short",
					Status:       db.StatusProcessing,
					RestartCount: DefaultMaxRestarts,
					RetryCount:   0,
					CreatedAt:    time.Now().UTC(),
					UpdatedAt:    time.Now().UTC(),
				}
				_ = insertMessage(store, msg)

				mockSess := &discordgo.Session{State: discordgo.NewState()}
				appCfg := config.NewFromData(&config.ConfigData{
					Model:  "test-model",
					AgyBin: "agy-custom",
					APIKey: "custom-api-key",
				})

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:       store,
					MaxAttempts: 0,
					NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
						if apiKey != "custom-api-key" || agyBin != "agy-custom" {
							t.Errorf("expected apiKey custom-api-key and agyBin agy-custom, got %s / %s", apiKey, agyBin)
						}
						return "Poison notice"
					},
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						return errors.New("discord delivery failed")
					},
				})
				pool.appCfg = appCfg
				pool.SetDiscordSession(mockSess)

				if pool.DiscordSession() != mockSess {
					t.Errorf("expected DiscordSession() to return mockSession")
				}

				RecoverInterrupted(store, pool)

				updated, _ := getMessage(store, "msg-edge-case-cov")
				if updated.Status != db.StatusFailed {
					t.Errorf("expected status FAILED, got %s", updated.Status)
				}
			},
		},
		{
			name: "DropsSupersededMessages",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				t1 := time.Now().UTC().Add(-20 * time.Minute)
				t2 := time.Now().UTC().Add(-10 * time.Minute)

				msgOld := db.Message{
					ID:        "msg-stranded-old",
					ThreadID:  "thread-superseded",
					GuildID:   "g1",
					Status:    db.StatusPending,
					CreatedAt: t1,
				}
				msgNewer := db.Message{
					ID:           "msg-completed-newer",
					ThreadID:     "thread-superseded",
					GuildID:      "g1",
					Status:       db.StatusCompleted,
					ResponseText: "Already answered newer question!",
					CreatedAt:    t2,
				}

				_ = insertMessage(store, msgOld)
				_ = insertMessage(store, msgNewer)

				var recoveredIDs []string
				var mu sync.Mutex

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    1,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						return mockJSONResponse("sess-1", "OK"), "", 0, nil
					},
					DeliveryFunc: noopDelivery,
					TypingFunc:   noopTyping,
					OnMessageCompleted: func(msg db.Message, finalStatus string) {
						mu.Lock()
						recoveredIDs = append(recoveredIDs, msg.ID)
						mu.Unlock()
					},
				})
				pool.Start()
				defer pool.Stop()

				RecoverInterrupted(store, pool)

				mu.Lock()
				recCount := len(recoveredIDs)
				mu.Unlock()

				if recCount != 0 {
					t.Fatalf("expected 0 recovered messages, got %d (%v)", recCount, recoveredIDs)
				}

				updated, err := getMessage(store, "msg-stranded-old")
				if err != nil {
					t.Fatalf("failed to get message: %v", err)
				}
				if updated.Status != db.StatusCompleted {
					t.Errorf("expected status %s, got %s", db.StatusCompleted, updated.Status)
				}
				if updated.ResponseText != "[EXPIRED_STALE]" {
					t.Errorf("expected response_text '[EXPIRED_STALE]', got %q", updated.ResponseText)
				}
			},
		},
		{
			name: "DropsStrandedPendingOlderThanTTL",
			run: func(t *testing.T) {
				store := setupTestStore(t)
				msgPending := db.Message{
					ID:            "msg-stranded-pending",
					ThreadID:      "thread-expired-pending",
					GuildID:       "g1",
					Status:        db.StatusPending,
					ScheduleRunID: "run-stranded-1",
					CreatedAt:     time.Now().UTC().Add(-45 * time.Minute),
				}
				_ = insertMessage(store, msgPending)

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:          store,
					StalenessTTL:   30 * time.Minute,
					TimeoutMinutes: 1,
					BackoffBase:    10 * time.Millisecond,
					MaxAttempts:    1,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						return mockJSONResponse("sess-1", "OK"), "", 0, nil
					},
					DeliveryFunc: noopDelivery,
					TypingFunc:   noopTyping,
				})
				pool.Start()
				defer pool.Stop()

				RecoverInterrupted(store, pool)

				updated, err := getMessage(store, "msg-stranded-pending")
				if err != nil {
					t.Fatalf("failed to get message: %v", err)
				}
				if updated.Status != db.StatusCompleted {
					t.Errorf("expected status %s, got %s", db.StatusCompleted, updated.Status)
				}
				if updated.ResponseText != "[EXPIRED_STALE]" {
					t.Errorf("expected response_text '[EXPIRED_STALE]', got %q", updated.ResponseText)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}


func TestIsRateLimitError_ExtendedKeywords(t *testing.T) {
	t.Parallel()
	tests := []struct {
		errDetail string
		extra     string
		want      bool
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
		got := isRateLimitError(tt.errDetail, tt.extra)
		if got != tt.want {
			t.Errorf("isRateLimitError(%q, %q) = %t, want %t", tt.errDetail, tt.extra, got, tt.want)
		}
	}
}

func TestWorkerPool_NotifierInvocationMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		threadID             string
		maxAttempts          int
		preSetup             func(store db.Store, threadID string)
		runnerStderr         string
		runnerErr            error
		runnerExitCode       int
		expectNotifierCalled bool
		expectedDescContains string
		notifierReturn       string
		expectDeliveredNotice string
		expectUnavailableMsg bool
	}{
		{
			name:                 "WatchdogExhaustion_InvokesNotifierFunc",
			threadID:             "thread-wd-1",
			maxAttempts:          1,
			runnerStderr:         "inactivity timeout exceeded [watchdog]",
			runnerErr:            errors.New("watchdog timeout"),
			runnerExitCode:       1,
			expectNotifierCalled: true,
			expectedDescContains: "execution timed out",
			notifierReturn:       "DEADASS TIMED OUT 💀",
			expectDeliveredNotice: "DEADASS TIMED OUT 💀",
		},
		{
			name:                 "NonTransient_InvokesNotifierFunc",
			threadID:             "thread-nt-1",
			maxAttempts:          3,
			runnerStderr:         "Error: unknown flag: --bogus-flag",
			runnerErr:            errors.New("exit status 1"),
			runnerExitCode:       1,
			expectNotifierCalled: true,
			expectedDescContains: "non-transient error",
			notifierReturn:       "UNACCEPTABLE GARBAGE DETECTED 🔥",
			expectDeliveredNotice: "UNACCEPTABLE GARBAGE DETECTED 🔥",
		},
		{
			name:                 "RetryExhaustion_InvokesNotifierFunc",
			threadID:             "thread-exhaust-1",
			maxAttempts:          2,
			runnerStderr:         "connection reset by peer",
			runnerErr:            errors.New("network drop"),
			runnerExitCode:       1,
			expectNotifierCalled: true,
			expectedDescContains: "exhausting 2 attempts",
			notifierReturn:       "ROASTED TO ASHES 🤘",
			expectDeliveredNotice: "ROASTED TO ASHES 🤘",
		},
		{
			name:                 "RateLimitBypassesNotifierFunc",
			threadID:             "thread-rl-1",
			maxAttempts:          1,
			runnerStderr:         "Error 429: Too Many Requests",
			runnerErr:            errors.New("rate limit"),
			runnerExitCode:       1,
			expectNotifierCalled: false,
			expectUnavailableMsg: true,
		},
		{
			name:                 "Watchdog_RateLimitBypassesNotifier",
			threadID:             "thread-wd-rl",
			maxAttempts:          1,
			runnerStderr:         "inactivity timeout exceeded: [watchdog] Resource has been exhausted (quota limit reached)",
			runnerErr:            context.DeadlineExceeded,
			runnerExitCode:       124,
			expectNotifierCalled: false,
			expectUnavailableMsg: true,
		},
		{
			name:        "SessionCorruption_RateLimitBypassesNotifier",
			threadID:    "thread-sc-rl",
			maxAttempts: 1,
			preSetup: func(store db.Store, threadID string) {
				_ = rotateSessionID(store, threadID, "sess-existing-1")
			},
			runnerStderr:         "conversation not found: 429 rate limit exceeded",
			runnerErr:            errors.New("rate limited corruption"),
			runnerExitCode:       1,
			expectNotifierCalled: false,
			expectUnavailableMsg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			if tt.preSetup != nil {
				tt.preSetup(store, tt.threadID)
			}

			var capturedDesc string
			var deliveredNotif string
			var mu sync.Mutex
			notifierCalled := false
			doneCh := make(chan struct{}, 1)

			pool := NewWorkerPool(WorkerPoolConfig{
				Store:          store,
				MaxAttempts:    tt.maxAttempts,
				TimeoutMinutes: 1,
				BackoffBase:    5 * time.Millisecond,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					return "", tt.runnerStderr, tt.runnerExitCode, tt.runnerErr
				},
				NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
					mu.Lock()
					notifierCalled = true
					capturedDesc = contextDescription
					mu.Unlock()
					return tt.notifierReturn
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredNotif = text
					mu.Unlock()
					return nil
				},
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					doneCh <- struct{}{}
				},
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
				t.Errorf("Expected notifierCalled=%t, got %t", tt.expectNotifierCalled, notifierCalled)
			}
			if tt.expectedDescContains != "" && !strings.Contains(capturedDesc, tt.expectedDescContains) {
				t.Errorf("Expected NotifierFunc description to contain %q, got: %q", tt.expectedDescContains, capturedDesc)
			}
			if tt.expectDeliveredNotice != "" && deliveredNotif != tt.expectDeliveredNotice {
				t.Errorf("Expected delivered notice %q, got: %q", tt.expectDeliveredNotice, deliveredNotif)
			}
			if tt.expectUnavailableMsg && deliveredNotif != notifier.ModelUnavailableMessage() {
				t.Errorf("Expected ModelUnavailableMessage, got: %q", deliveredNotif)
			}
		})
	}

	t.Run("NonTransient_LongSnippetAndDeliveryErr", func(t *testing.T) {
		store := setupTestStore(t)
		var capturedDesc string
		var mu sync.Mutex
		doneCh := make(chan struct{}, 1)

		longError := strings.Repeat("VeryBadError-", 50)
		pool := NewWorkerPool(WorkerPoolConfig{
			Store:          store,
			MaxAttempts:    1,
			TimeoutMinutes: 1,
			BackoffBase:    5 * time.Millisecond,
			RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return "", longError, 1, errors.New("fatal command syntax")
			},
			NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
				mu.Lock()
				capturedDesc = contextDescription
				mu.Unlock()
				return "DEADASS BROKEN 💀"
			},
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return errors.New("discord delivery network failed")
			},
			OnMessageCompleted: func(msg db.Message, finalStatus string) {
				doneCh <- struct{}{}
			},
		})
		pool.Start()
		defer pool.Stop()

		msg := db.Message{ID: "msg-nt-long", ThreadID: "thread-nt-long", Content: "long err test"}
		_ = insertMessage(store, msg)
		pool.Enqueue(msg)

		waitForCompletion(t, doneCh, 3*time.Second, "long snippet test")

		mu.Lock()
		defer mu.Unlock()
		if len([]rune(capturedDesc)) > 350 {
			t.Errorf("Expected truncated description, got length %d: %q", len([]rune(capturedDesc)), capturedDesc)
		}
	})

	t.Run("New_NotifierRunnerFuncWrapped", func(t *testing.T) {
		store := setupTestStore(t)
		runnerCalled := false
		pool := New(nil, WorkerPoolConfig{
			Store: store,
			NotifierRunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				runnerCalled = true
				return mockJSONResponse("", "DYNAMIC FALLBACK NOTICE"), "", 0, nil
			},
		})

		if pool.cfg.NotifierFunc == nil {
			t.Fatal("Expected NotifierFunc to be initialized")
		}
		res := pool.cfg.NotifierFunc("agy", "key", "test description")
		if !runnerCalled {
			t.Errorf("Expected NotifierRunnerFunc to be invoked")
		}
		if res != "DYNAMIC FALLBACK NOTICE" {
			t.Errorf("Expected dynamic fallback notice, got: %q", res)
		}
	})
}

func TestWorkerPool_CancellationDuringBackoffSuite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		threadID             string
		scheduleRunID        string
		maxAttempts          int
		backoffBase          time.Duration
		runnerWithOptions    func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error)
		expectDeliveredCheck bool
	}{
		{
			name:          "Watchdog_CancelledDuringBackoff",
			threadID:      "thread-wd-cancel",
			scheduleRunID: "run-wd-cancel",
			maxAttempts:   3,
			backoffBase:   500 * time.Millisecond,
			runnerWithOptions: func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					go func() {
						time.Sleep(10 * time.Millisecond)
						(*poolPtr).cancel()
					}()
					return "", "inactivity timeout exceeded: [watchdog] hung process", 124, context.DeadlineExceeded
				}
			},
		},
		{
			name:                 "Watchdog_CancelledPostWatchdog",
			threadID:             "thread-wd-post-cancel",
			maxAttempts:          1,
			backoffBase:          5 * time.Millisecond,
			expectDeliveredCheck: true,
			runnerWithOptions: func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					(*poolPtr).cancel()
					return "", "inactivity timeout exceeded: [watchdog] single attempt cancel", 124, context.DeadlineExceeded
				}
			},
		},
		{
			name:                 "NonTransient_CancelledDuringTurn",
			threadID:             "thread-nt-cancel",
			maxAttempts:          1,
			backoffBase:          5 * time.Millisecond,
			expectDeliveredCheck: true,
			runnerWithOptions: func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					(*poolPtr).cancel()
					return "", "Fatal: invalid prompt parameters", 2, errors.New("non transient err")
				}
			},
		},
		{
			name:          "RetryExhaustion_CancelledWithScheduleRunID",
			threadID:      "thread-exh-cancel",
			scheduleRunID: "run-exh-cancel",
			maxAttempts:   2,
			backoffBase:   5 * time.Millisecond,
			runnerWithOptions: func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					(*poolPtr).cancel()
					return "", "connection timed out", 1, errors.New("timeout")
				}
			},
		},
		{
			name:          "RateLimit_CancelledDuringBackoff",
			threadID:      "thread-rl-cancel",
			scheduleRunID: "run-rl-cancel",
			maxAttempts:   3,
			backoffBase:   500 * time.Millisecond,
			runnerWithOptions: func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					go func() {
						time.Sleep(10 * time.Millisecond)
						(*poolPtr).cancel()
					}()
					return "", "Error 429: Resource has been exhausted (quota exceeded)", 1, errors.New("rate limited")
				}
			},
		},
		{
			name:          "Transient_CancelledDuringBackoff",
			threadID:      "thread-tr-cancel",
			scheduleRunID: "run-tr-cancel",
			maxAttempts:   3,
			backoffBase:   500 * time.Millisecond,
			runnerWithOptions: func(poolPtr **WorkerPool, deliveredCalled *bool, mu *sync.Mutex) func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					go func() {
						time.Sleep(10 * time.Millisecond)
						(*poolPtr).cancel()
					}()
					return "", "connection reset by peer", 1, errors.New("transient network drop")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			var pool *WorkerPool
			deliveredCalled := false
			var mu sync.Mutex

			pool = NewWorkerPool(WorkerPoolConfig{
				Store:                 store,
				MaxAttempts:           tt.maxAttempts,
				TimeoutMinutes:        1,
				BackoffBase:           tt.backoffBase,
				RunnerWithOptionsFunc: tt.runnerWithOptions(&pool, &deliveredCalled, &mu),
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredCalled = true
					mu.Unlock()
					return nil
				},
			})
			pool.Start()
			defer pool.Stop()

			if tt.scheduleRunID != "" {
				_ = createScheduleRun(store, db.ScheduleRun{
					ID:           tt.scheduleRunID,
					ScheduleID:   "sched-" + tt.name,
					ScheduleType: "cron",
					Status:       "running",
					StartedAt:    time.Now().UTC(),
				})
			}

			msgID := "msg-" + tt.name
			msg := db.Message{ID: msgID, ThreadID: tt.threadID, Content: "cancel test", ScheduleRunID: tt.scheduleRunID}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			waitPoolWorkersDone(t, pool, 2*time.Second)

			if tt.expectDeliveredCheck {
				mu.Lock()
				if deliveredCalled {
					t.Errorf("DeliveryFunc should not be called when pool is cancelled")
				}
				mu.Unlock()
			}

			dbMsg, _ := getMessage(store, msgID)
			if dbMsg != nil && dbMsg.Status != db.StatusPending {
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

func TestOnWakeHook(t *testing.T) {
	t.Run("OverridesAndTimeoutFallbacks", func(t *testing.T) {
		tests := []struct {
			name             string
			hookResp         *WakeResponse
			hookErr          error
			timeoutAction    string
			classifierResp   string
			classifierCode   bool
			messages         []db.Message
			expectExit       bool
			expectWakeIdx    int
			expectClassifier bool
			expectHookCall   bool
			verify           func(t *testing.T, store db.Store, te *turnExecution, capturedReq *WakeRequest)
		}{
			{
				name:     "WakeOverride",
				hookResp: &WakeResponse{Override: "wake", Reason: "hook_priority_wake"},
				classifierResp: `{"confidence": 0.10, "reason": "ambient banter"}`,
				messages: []db.Message{{
					ID:         "msg-wake-1",
					ThreadID:   "chan-general",
					AuthorName: "user-alpha",
					Content:    "hello world",
					Status:     db.StatusProcessing,
				}},
				expectExit:       false,
				expectWakeIdx:    0,
				expectClassifier: false,
				expectHookCall:   true,
				verify: func(t *testing.T, store db.Store, te *turnExecution, capturedReq *WakeRequest) {
					if capturedReq == nil {
						t.Fatalf("expected capturedReq to be populated")
					}
					if capturedReq.ChannelID != "chan-general" {
						t.Errorf("expected ChannelID 'chan-general', got %q", capturedReq.ChannelID)
					}
					if capturedReq.ThreadID != "chan-general" {
						t.Errorf("expected ThreadID 'chan-general', got %q", capturedReq.ThreadID)
					}
					if capturedReq.Message.ID != "msg-wake-1" {
						t.Errorf("expected Message ID 'msg-wake-1', got %q", capturedReq.Message.ID)
					}
					if capturedReq.Timestamp.IsZero() {
						t.Errorf("expected non-zero Timestamp in WakeRequest")
					}
					if len(te.wakeInfos) == 0 || !te.wakeInfos[0].IsWake {
						t.Errorf("expected wakeInfo[0].isWake = true")
					}
				},
			},
			{
				name:     "WakeOverride_MultiMessageBurstAndTurnCount",
				hookResp: &WakeResponse{Override: "wake", Reason: "hook_wake_override"},
				messages: []db.Message{
					{
						ID:         "msg-wake-multi-1",
						ThreadID:   "chan-multi-test",
						AuthorName: "user-alpha",
						Content:    "first message wakes",
						Status:     db.StatusProcessing,
					},
					{
						ID:         "msg-wake-multi-2",
						ThreadID:   "chan-multi-test",
						AuthorName: "user-beta",
						Content:    "second trailing message",
						Status:     db.StatusProcessing,
					},
				},
				expectExit:       false,
				expectWakeIdx:    0,
				expectClassifier: false,
				expectHookCall:   true,
				verify: func(t *testing.T, store db.Store, te *turnExecution, capturedReq *WakeRequest) {
					if len(te.burst) != 1 || te.burst[0].ID != "msg-wake-multi-1" {
						t.Errorf("expected active burst to be partitioned to first message, got %+v", te.burst)
					}
					if len(te.trailingMsgs) != 1 || te.trailingMsgs[0].ID != "msg-wake-multi-2" {
						t.Errorf("expected trailingMsgs to contain second message, got %+v", te.trailingMsgs)
					}
					turnCount, err := getSessionTurnCount(store, "chan-multi-test")
					if err != nil {
						t.Fatalf("failed to get session turn count: %v", err)
					}
					if turnCount != 1 {
						t.Errorf("expected session turn count to increment to 1, got %d", turnCount)
					}
				},
			},
			{
				name:           "DropOverride",
				hookResp:       &WakeResponse{Override: "drop", Reason: "hook_drop_override"},
				classifierResp: `{"confidence": 0.95, "reason": "strong intent"}`,
				messages: []db.Message{{
					ID:         "msg-drop-1",
					ThreadID:   "chan-general",
					AuthorName: "user-bravo",
					Content:    "random chit-chat",
					Status:     db.StatusProcessing,
				}},
				expectExit:       true,
				expectWakeIdx:    -1,
				expectClassifier: false,
				expectHookCall:   true,
				verify: func(t *testing.T, store db.Store, te *turnExecution, capturedReq *WakeRequest) {
					saved, err := getMessage(store, "msg-drop-1")
					if err != nil || saved == nil {
						t.Fatalf("Failed to retrieve msg-drop-1: %v", err)
					}
					if saved.Status != db.StatusCompleted {
						t.Errorf("expected message status COMPLETED, got %s", saved.Status)
					}
					if !strings.Contains(saved.ErrorMessage, "[AMBIENT") {
						t.Errorf("expected message ErrorMessage to contain [AMBIENT, got %q", saved.ErrorMessage)
					}
				},
			},
			{
				name:             "TimeoutFallback_Classify",
				hookErr:          context.DeadlineExceeded,
				timeoutAction:    "classify",
				classifierResp:   `{"confidence": 0.92, "reason": "classified wake"}`,
				messages: []db.Message{{
					ID:         "msg-timeout-1",
					ThreadID:   "chan-general",
					AuthorName: "user-charlie",
					Content:    "please help me calculate total memory usage",
					Status:     db.StatusProcessing,
				}},
				expectExit:       false,
				expectWakeIdx:    0,
				expectClassifier: true,
				expectHookCall:   true,
			},
			{
				name:             "TimeoutFallback_Wake",
				hookErr:          errors.New("connection refused"),
				timeoutAction:    "wake",
				classifierResp:   `{"confidence": 0.10, "reason": "ambient"}`,
				messages: []db.Message{{
					ID:         "msg-timeout-2",
					ThreadID:   "chan-general",
					AuthorName: "user-delta",
					Content:    "query for bot",
					Status:     db.StatusProcessing,
				}},
				expectExit:       false,
				expectWakeIdx:    0,
				expectClassifier: false,
				expectHookCall:   true,
			},
			{
				name:             "TimeoutFallback_Drop",
				hookErr:          errors.New("timeout reached"),
				timeoutAction:    "drop",
				classifierResp:   `{"confidence": 0.95, "reason": "strong intent"}`,
				messages: []db.Message{{
					ID:         "msg-timeout-drop",
					ThreadID:   "chan-general",
					AuthorName: "user-echo",
					Content:    "drop me on error",
					Status:     db.StatusProcessing,
				}},
				expectExit:       true,
				expectWakeIdx:    -1,
				expectClassifier: false,
				expectHookCall:   true,
				verify: func(t *testing.T, store db.Store, te *turnExecution, capturedReq *WakeRequest) {
					saved, err := getMessage(store, "msg-timeout-drop")
					if err != nil || saved == nil {
						t.Fatalf("Failed to retrieve msg-timeout-drop: %v", err)
					}
					if saved.Status != db.StatusCompleted {
						t.Errorf("expected message status COMPLETED, got %s", saved.Status)
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				store := setupTestStore(t)
				var classifierCalled bool
				var cls *classifier.Classifier
				if tc.classifierResp != "" {
					cls = classifier.NewClassifier(
						classifier.WithModel("test-flash"),
						classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
							classifierCalled = true
							return tc.classifierResp, nil
						}),
					)
				}

				var hookCalled bool
				var capturedReq *WakeRequest
				mockDisp := &mockWebhookDispatcher{
					callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
						hookCalled = true
						capturedReq = req
						return tc.hookResp, tc.hookErr
					},
				}

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:             store,
					Classifier:        cls,
					WebhookDispatcher: mockDisp,
				})

				now := time.Now().UTC()
				msgs := make([]db.Message, len(tc.messages))
				for i, m := range tc.messages {
					m.CreatedAt = now.Add(time.Duration(i) * time.Second)
					msgs[i] = m
					_ = insertMessage(store, m)
				}

				threadID := msgs[0].ThreadID
				te := &turnExecution{
					pool:        pool,
					burst:       msgs,
					threadID:    threadID,
					effectiveID: threadID,
					triggerType: "message",
					execStart:   now,
					policy: config.ChannelPolicy{
						Mode:                 "channel",
						WakeMode:             "classifier",
						AmbientWakeThreshold: ptrFloat(0.80),
						Hooks: config.ChannelHooksConfig{
							OnWake: &config.WebhookEndpoint{
								URL:             "http://sidecar.local/wake",
								TimeoutMs:       1000,
								OnTimeoutAction: tc.timeoutAction,
							},
						},
					},
				}

				shouldExit := te.evaluateAmbientWake()
				if shouldExit != tc.expectExit {
					t.Fatalf("expected shouldExit = %v, got %v", tc.expectExit, shouldExit)
				}
				if hookCalled != tc.expectHookCall {
					t.Errorf("expected hookCalled = %v, got %v", tc.expectHookCall, hookCalled)
				}
				if classifierCalled != tc.expectClassifier {
					t.Errorf("expected classifierCalled = %v, got %v", tc.expectClassifier, classifierCalled)
				}
				if te.wakeIdx != tc.expectWakeIdx {
					t.Errorf("expected wakeIdx = %d, got %d", tc.expectWakeIdx, te.wakeIdx)
				}
				if tc.verify != nil {
					tc.verify(t, store, te, capturedReq)
				}
			})
		}
	})

	t.Run("FullProcessBurst_WakeAndDrop", func(t *testing.T) {
		tmpDir := t.TempDir()
		store := setupTestStore(t)

		sessMgr := session.New(tmpDir, "")

		var mu sync.Mutex
		runnerCalls := 0
		deliveryCalls := 0

		mockDisp := &mockWebhookDispatcher{
			callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
				if req.Message.Content == "wake message" {
					return &WakeResponse{Override: "wake"}, nil
				}
				return &WakeResponse{Override: "drop"}, nil
			},
		}

		pool := NewWorkerPool(WorkerPoolConfig{
			Store:             store,
			SessionManager:    sessMgr,
			TimeoutMinutes:    1,
			WebhookDispatcher: mockDisp,
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
				return config.ChannelPolicy{
					Mode:                 "channel",
					WakeMode:             "classifier",
					AmbientWakeThreshold: ptrFloat(0.80),
					Hooks: config.ChannelHooksConfig{
						OnWake: &config.WebhookEndpoint{
							URL: "http://sidecar.local/wake",
						},
					},
				}
			},
		})

		now := time.Now().UTC()
		// Test drop burst
		msgDrop := db.Message{
			ID:         "msg-burst-drop",
			ThreadID:   "chan-full-test",
			AuthorName: "user-foxtrot",
			Content:    "drop message",
			Status:     db.StatusPending,
			CreatedAt:  now,
		}
		_ = insertMessage(store, msgDrop)
		pool.processBurst([]db.Message{msgDrop})

		mu.Lock()
		if runnerCalls != 0 {
			t.Errorf("expected 0 runner calls for drop, got %d", runnerCalls)
		}
		if deliveryCalls != 0 {
			t.Errorf("expected 0 delivery calls for drop, got %d", deliveryCalls)
		}
		mu.Unlock()

		savedDrop, _ := getMessage(store, "msg-burst-drop")
		if savedDrop == nil || savedDrop.Status != db.StatusCompleted || !strings.Contains(savedDrop.ErrorMessage, "[AMBIENT") {
			t.Errorf("expected drop message to be completed as ambient, got %+v", savedDrop)
		}

		// Test wake burst
		msgWake := db.Message{
			ID:         "msg-burst-wake",
			ThreadID:   "chan-full-test",
			AuthorName: "user-golf",
			Content:    "wake message",
			Status:     db.StatusPending,
			CreatedAt:  now.Add(time.Second),
		}
		_ = insertMessage(store, msgWake)
		pool.processBurst([]db.Message{msgWake})

		mu.Lock()
		if runnerCalls != 1 {
			t.Errorf("expected 1 runner call for wake, got %d", runnerCalls)
		}
		if deliveryCalls != 1 {
			t.Errorf("expected 1 delivery call for wake, got %d", deliveryCalls)
		}
		mu.Unlock()

		savedWake, _ := getMessage(store, "msg-burst-wake")
		if savedWake == nil || savedWake.Status != db.StatusCompleted || savedWake.ResponseText != "Bot response" {
			t.Errorf("expected wake message to be completed with response, got %+v", savedWake)
		}
	})

	t.Run("AccessorsAndFallbacks", func(t *testing.T) {
		var nilPool *WorkerPool
		if nilPool.WebhookDispatcher() != nil {
			t.Errorf("expected nil for nilPool.WebhookDispatcher")
		}
		nilPool.SetWebhookDispatcher(nil) // should not panic

		poolDef := NewWorkerPool(WorkerPoolConfig{})
		if poolDef.WebhookDispatcher() == nil {
			t.Errorf("expected non-nil default dispatcher from NewWorkerPool")
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

		store := setupTestStore(t)

		_ = createScheduleRun(store, db.ScheduleRun{
			ID:           "sched-run-ambient",
			ScheduleID:   "sched-1",
			ScheduleType: "cron",
			TargetID:     "chan-fallback",
			ThreadID:     "chan-fallback",
			Title:        "Scheduled Run",
			Prompt:       "prompt",
			StartedAt:    time.Now().UTC(),
			Status:       "pending",
		})

		var capturedReq *WakeRequest
		mockDispWithCapture := &mockWebhookDispatcher{
			callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
				capturedReq = req
				return &WakeResponse{Override: "drop"}, nil
			},
		}

		pool := NewWorkerPool(WorkerPoolConfig{
			Store:             store,
			WebhookDispatcher: mockDispWithCapture,
		})

		now := time.Now().UTC()
		schedMsg := db.Message{
			ID:            "msg-sched-ambient",
			ThreadID:      "chan-fallback",
			AuthorName:    "user-sched",
			Content:       "scheduled noise",
			ScheduleRunID: "sched-run-ambient",
			Status:        db.StatusProcessing,
			CreatedAt:     now,
		}
		_ = insertMessage(store, schedMsg)

		teFallback := &turnExecution{
			pool:        pool,
			burst:       []db.Message{schedMsg},
			threadID:    "chan-fallback",
			effectiveID: "", // triggers channelID == "" -> te.threadID fallback
			triggerType: "message",
			execStart:   now,
			policy: config.ChannelPolicy{
				Mode: "channel",
				Hooks: config.ChannelHooksConfig{
					OnWake: &config.WebhookEndpoint{
						URL: "http://sidecar.local/wake",
					},
				},
			},
		}

		shouldExit := teFallback.evaluateAmbientWake()
		if !shouldExit {
			t.Fatalf("expected shouldExit = true for drop override")
		}
		if capturedReq == nil || capturedReq.ChannelID != "chan-fallback" {
			t.Errorf("expected capturedReq.ChannelID to fallback to te.threadID, got %+v", capturedReq)
		}

		runAmbient, err := getScheduleRun(store, "sched-run-ambient")
		if err != nil || runAmbient == nil {
			t.Fatalf("Failed to fetch schedule run: %v", err)
		}
		runStatus := runAmbient.Status
		if runStatus != "completed" {
			t.Errorf("expected schedule run status 'completed', got %q", runStatus)
		}

		// Edge case: nil pool in markAmbientBurst and evaluateAmbientWake
		teNil := &turnExecution{
			pool:  nil,
			burst: []db.Message{{ID: "m-nil"}},
			policy: config.ChannelPolicy{
				Mode: "channel",
				Hooks: config.ChannelHooksConfig{
					OnWake: &config.WebhookEndpoint{
						URL: "http://sidecar.local/wake",
					},
				},
			},
		}
		teNil.markAmbientBurst()
	})
}

func TestPreTurnHook(t *testing.T) {
	t.Run("HookActionsAndFallbacks", func(t *testing.T) {
		tests := []struct {
			name           string
			hookRespFunc   func(callCount int) (*PreTurnResponse, error)
			timeoutAction  string
			initialRetry   int
			scheduleRun    bool
			expectRunner   int
			expectHookCall int
			verify         func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest)
		}{
			{
				name: "Approved_WithInjectedContext",
				hookRespFunc: func(int) (*PreTurnResponse, error) {
					return &PreTurnResponse{
						Allow:           true,
						InjectedContext: "system instructions: be concise",
						Metadata:        map[string]any{"lease_token": "token-123"},
					}, nil
				},
				expectRunner:   1,
				expectHookCall: 1,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					if capturedReq == nil {
						t.Fatalf("expected capturedReq to be populated")
					}
					if capturedReq.ChannelID != "chan-preturn" || capturedReq.ThreadID != "chan-preturn" {
						t.Errorf("channel/thread ID mismatch: got %q, %q", capturedReq.ChannelID, capturedReq.ThreadID)
					}
					if capturedReq.BurstCount != 1 || capturedReq.RetryCount != 0 || capturedReq.Timestamp.IsZero() {
						t.Errorf("request metadata mismatch: %+v", capturedReq)
					}
					if !strings.Contains(capturedPrompt, "<COORDINATION_CONTEXT>\nsystem instructions: be concise\n</COORDINATION_CONTEXT>") {
						t.Errorf("expected prompt to contain COORDINATION_CONTEXT, got:\n%s", capturedPrompt)
					}
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil || saved.Status != db.StatusCompleted {
						t.Errorf("expected msg-preturn-test StatusCompleted, got %+v (err %v)", saved, err)
					}
				},
			},
			{
				name: "Rejected_DropAction",
				hookRespFunc: func(int) (*PreTurnResponse, error) {
					return &PreTurnResponse{
						Allow:  false,
						Action: "drop",
						Reason: "resource busy",
					}, nil
				},
				expectRunner:   0,
				expectHookCall: 1,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil {
						t.Fatalf("failed to retrieve message: %v", err)
					}
					if saved.Status != db.StatusCompleted {
						t.Errorf("expected StatusCompleted for dropped message, got %s", saved.Status)
					}
					if !strings.Contains(saved.ErrorMessage, "resource busy") {
						t.Errorf("expected ErrorMessage to contain 'resource busy', got %q", saved.ErrorMessage)
					}
				},
			},
			{
				name: "Rejected_RetryAction",
				hookRespFunc: func(callCount int) (*PreTurnResponse, error) {
					if callCount == 1 {
						return &PreTurnResponse{
							Allow:             false,
							Action:            "retry",
							RetryAfterSeconds: 1,
							Reason:            "sidecar lock wait",
						}, nil
					}
					return &PreTurnResponse{Allow: true}, nil
				},
				expectRunner:   1,
				expectHookCall: 2,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil {
						t.Fatalf("failed to retrieve message: %v", err)
					}
					if saved.RetryCount != 1 {
						t.Errorf("expected RetryCount 1, got %d", saved.RetryCount)
					}
					if saved.Status != db.StatusCompleted {
						t.Errorf("expected StatusCompleted after retry execution, got %s", saved.Status)
					}
				},
			},
			{
				name: "RetryExhaustion",
				hookRespFunc: func(int) (*PreTurnResponse, error) {
					return &PreTurnResponse{
						Allow:             false,
						Action:            "retry",
						RetryAfterSeconds: 1,
					}, nil
				},
				initialRetry:   2,
				scheduleRun:    true,
				expectRunner:   0,
				expectHookCall: 1,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil {
						t.Fatalf("failed to retrieve message: %v", err)
					}
					if saved.Status != db.StatusFailed {
						t.Errorf("expected StatusFailed on retry exhaustion, got %s", saved.Status)
					}
					if !strings.Contains(saved.ErrorMessage, "[EXHAUSTED_PRE_TURN_RETRIES]") {
						t.Errorf("expected ErrorMessage to contain [EXHAUSTED_PRE_TURN_RETRIES], got %q", saved.ErrorMessage)
					}
					if r, _ := getScheduleRun(store, "run-exhaust-1"); r == nil || r.Status != "failed" {
						t.Errorf("expected schedule run status 'failed', got %+v", r)
					}
				},
			},
			{
				name: "TimeoutFallback_Drop",
				hookRespFunc: func(int) (*PreTurnResponse, error) {
					return nil, context.DeadlineExceeded
				},
				timeoutAction:  "drop",
				expectRunner:   0,
				expectHookCall: 1,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil || saved.Status != db.StatusCompleted || !strings.Contains(saved.ErrorMessage, "pre_turn_timeout_drop") {
						t.Errorf("expected StatusCompleted with pre_turn_timeout_drop, got %+v", saved)
					}
				},
			},
			{
				name: "TimeoutFallback_Proceed",
				hookRespFunc: func(int) (*PreTurnResponse, error) {
					return nil, errors.New("webhook connection failed")
				},
				timeoutAction:  "proceed",
				expectRunner:   1,
				expectHookCall: 1,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil || saved.Status != db.StatusCompleted {
						t.Errorf("expected StatusCompleted, got %+v", saved)
					}
				},
			},
			{
				name: "TimeoutFallback_Retry",
				hookRespFunc: func(int) (*PreTurnResponse, error) {
					return nil, errors.New("gateway timeout")
				},
				timeoutAction:  "retry",
				expectRunner:   0,
				expectHookCall: 1,
				verify: func(t *testing.T, store db.Store, runnerCalls int, capturedPrompt string, capturedReq *PreTurnRequest) {
					saved, err := getMessage(store, "msg-preturn-test")
					if err != nil || saved == nil || saved.RetryCount != 1 || saved.Status != db.StatusPending {
						t.Errorf("expected StatusPending with RetryCount 1, got %+v", saved)
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				store := setupTestStore(t)
				tmpDir := t.TempDir()
				sessMgr := session.New(tmpDir, "")

				var mu sync.Mutex
				var runnerCalls int
				var capturedPrompt string
				var hookCalls int
				var capturedReq *PreTurnRequest
				completedChan := make(chan struct{})

				mockDisp := &mockWebhookDispatcher{
					callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
						mu.Lock()
						hookCalls++
						cnt := hookCalls
						capturedReq = req
						mu.Unlock()
						return tc.hookRespFunc(cnt)
					},
				}

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:              store,
					SessionManager:     sessMgr,
					TimeoutMinutes:     1,
					MaxAttempts:        3,
					RetryDelayOverride: 10 * time.Millisecond,
					WebhookDispatcher:  mockDisp,
					RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						mu.Lock()
						runnerCalls++
						capturedPrompt = prompt
						mu.Unlock()
						return mockJSONResponse(sessionID, "Turn proceeded"), "", 0, nil
					},
					DeliveryFunc: noopDelivery,
					OnMessageCompleted: func(m db.Message, status string) {
						if m.ID == "msg-preturn-test" {
							select {
							case <-completedChan:
							default:
								close(completedChan)
							}
						}
					},
					ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
						return config.ChannelPolicy{
							Mode:     "channel",
							WakeMode: "all",
							Hooks: config.ChannelHooksConfig{
								PreTurn: &config.WebhookEndpoint{
									URL:             "http://sidecar.local/pre-turn",
									TimeoutMs:       1500,
									OnTimeoutAction: tc.timeoutAction,
								},
							},
						}
					},
				})

				now := time.Now().UTC()
				msg := db.Message{
					ID:         "msg-preturn-test",
					ThreadID:   "chan-preturn",
					AuthorName: "user-alpha",
					Content:    "test prompt for pre-turn",
					Status:     db.StatusPending,
					RetryCount: tc.initialRetry,
					CreatedAt:  now,
				}
				if tc.scheduleRun {
					msg.ScheduleRunID = "run-exhaust-1"
					_ = createScheduleRun(store, db.ScheduleRun{
						ID:           "run-exhaust-1",
						ScheduleID:   "sched-1",
						ScheduleType: "cron",
						Status:       "running",
						StartedAt:    now,
					})
				}
				_ = insertMessage(store, msg)

				pool.processBurst([]db.Message{msg})

				if tc.name == "Rejected_RetryAction" {
					select {
					case <-completedChan:
					case <-time.After(5 * time.Second):
						t.Fatalf("timed out waiting for message completion after retry")
					}
				}

				mu.Lock()
				rc := runnerCalls
				hc := hookCalls
				cp := capturedPrompt
				cr := capturedReq
				mu.Unlock()

				if hc != tc.expectHookCall {
					t.Errorf("expected hookCalls = %d, got %d", tc.expectHookCall, hc)
				}
				if rc != tc.expectRunner {
					t.Errorf("expected runnerCalls = %d, got %d", tc.expectRunner, rc)
				}
				if tc.verify != nil {
					tc.verify(t, store, rc, cp, cr)
				}
			})
		}
	})

	t.Run("HelperEdgeCases", func(t *testing.T) {
		// 1. buildPreTurnRequest with nil te
		reqNil := buildPreTurnRequest(nil)
		if reqNil == nil || reqNil.Timestamp.IsZero() {
			t.Errorf("expected valid PreTurnRequest from nil te, got %+v", reqNil)
		}

		// 2. buildPreTurnRequest with empty effectiveID and multiple messages
		msgA := db.Message{ID: "mA", RetryCount: 1, Content: "promptA"}
		msgB := db.Message{ID: "mB", RetryCount: 3, Content: "promptB"}
		teEffective := &turnExecution{
			threadID:    "t-fallback",
			effectiveID: "",
			burst:       []db.Message{msgA, msgB},
		}
		reqEffective := buildPreTurnRequest(teEffective)
		if reqEffective.ChannelID != "t-fallback" {
			t.Errorf("expected ChannelID fallback to t-fallback, got %s", reqEffective.ChannelID)
		}
		if reqEffective.RetryCount != 3 {
			t.Errorf("expected max RetryCount 3, got %d", reqEffective.RetryCount)
		}

		// 3. rescheduleBurst and abortBurst with nil pool (nil-safety check)
		teNoPool := &turnExecution{
			pool:  nil,
			burst: []db.Message{msgA},
		}
		teNoPool.rescheduleBurst(0)
		teNoPool.abortBurst("test drop nil pool")

		// 4. abortBurst with ScheduleRunID and OnMessageCompleted
		store := setupTestStore(t)

		var completedStatus string
		pool := NewWorkerPool(WorkerPoolConfig{
			Store: store,
			OnMessageCompleted: func(msg db.Message, status string) {
				completedStatus = status
			},
		})

		schedMsg := db.Message{
			ID:            "msg-sched-abort",
			ThreadID:      "chan-sched",
			ScheduleRunID: "run-sched-abort",
			Status:        db.StatusPending,
		}
		_ = insertMessage(store, schedMsg)
		_ = createScheduleRun(store, db.ScheduleRun{
			ID:           "run-sched-abort",
			ScheduleID:   "s1",
			ScheduleType: "cron",
			Status:       "running",
			StartedAt:    time.Now().UTC(),
		})

		teAbort := &turnExecution{
			pool:     pool,
			burst:    []db.Message{schedMsg},
			threadID: "chan-sched",
		}
		teAbort.abortBurst("manual abort")

		if completedStatus != db.StatusCompleted {
			t.Errorf("expected completedStatus COMPLETED, got %s", completedStatus)
		}

		var runStatus string
		if r, _ := getScheduleRun(store, "run-sched-abort"); r != nil {
			runStatus = r.Status
		}
		if runStatus != "completed" {
			t.Errorf("expected schedule run status 'completed', got %q", runStatus)
		}
	})
}

func TestPostTurnHook(t *testing.T) {
	t.Run("HookOutcomes", func(t *testing.T) {
		expectedUsage := runner.TokenUsage{
			InputTokens:     100,
			OutputTokens:    50,
			ThinkingTokens:  10,
			CacheReadTokens: 5,
			TotalTokens:     150,
		}
		expectedResponse := "Clean success response from runner"

		tests := []struct {
			name        string
			preTurnResp *PreTurnResponse
			runnerFunc  func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error)
			verify      func(t *testing.T, req *PostTurnRequest)
		}{
			{
				name: "SuccessfulTurn",
				preTurnResp: &PreTurnResponse{
					Allow: true,
					Metadata: map[string]any{
						"lease_id":   "lease-abc",
						"turn_epoch": float64(42),
					},
				},
				runnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return mockJSONResponseWithUsage(sessionID, expectedResponse, expectedUsage), "", 0, nil
				},
				verify: func(t *testing.T, req *PostTurnRequest) {
					if req.Status != "success" {
						t.Errorf("expected status 'success', got %q", req.Status)
					}
					if req.ResponseText != expectedResponse {
						t.Errorf("expected response text %q, got %q", expectedResponse, req.ResponseText)
					}
					if req.TokenUsage != expectedUsage {
						t.Errorf("expected token usage %+v, got %+v", expectedUsage, req.TokenUsage)
					}
					if req.Metadata == nil || req.Metadata["lease_id"] != "lease-abc" || req.Metadata["turn_epoch"] != float64(42) {
						t.Errorf("expected echoed metadata, got %+v", req.Metadata)
					}
					if req.Error != "" {
						t.Errorf("expected empty error on success, got %q", req.Error)
					}
				},
			},
			{
				name: "FailedTurn_AgyError",
				runnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return "", "non-transient runner failed: syntax error in script", 1, fmt.Errorf("exit status 1")
				},
				verify: func(t *testing.T, req *PostTurnRequest) {
					if req.Status != "failed" {
						t.Errorf("expected status 'failed', got %q", req.Status)
					}
					if !strings.Contains(req.Error, "syntax error") {
						t.Errorf("expected error to contain 'syntax error', got %q", req.Error)
					}
					if req.ResponseText != "" {
						t.Errorf("expected empty response text on failure, got %q", req.ResponseText)
					}
				},
			},
			{
				name: "RunnerPanicRecovery",
				runnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					panic("simulated runner crash inside runner function")
				},
				verify: func(t *testing.T, req *PostTurnRequest) {
					if req.Status != "failed" {
						t.Errorf("expected status 'failed', got %q", req.Status)
					}
					if !strings.Contains(req.Error, "simulated runner crash") {
						t.Errorf("expected error to contain panic message, got %q", req.Error)
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				store := setupTestStore(t)
				var mu sync.Mutex
				var postCalled bool
				var postReqReceived *PostTurnRequest

				mockDisp := &mockWebhookDispatcher{
					callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
						if tc.preTurnResp != nil {
							return tc.preTurnResp, nil
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
				}

				pool := NewWorkerPool(WorkerPoolConfig{
					Store:             store,
					TimeoutMinutes:    1,
					MaxAttempts:       1,
					WebhookDispatcher: mockDisp,
					RunnerFunc:        tc.runnerFunc,
					ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
						return config.ChannelPolicy{
							Mode:     "channel",
							WakeMode: "all",
							Hooks: config.ChannelHooksConfig{
								PreTurn: &config.WebhookEndpoint{
									URL: "http://sidecar.local/pre-turn",
								},
								PostTurn: &config.WebhookEndpoint{
									URL: "http://sidecar.local/post-turn",
								},
							},
						}
					},
				})

				now := time.Now().UTC()
				msg := db.Message{
					ID:         "msg-postturn-test",
					ThreadID:   "chan-postturn-test",
					AuthorName: "user-alpha",
					Content:    "run postturn test",
					Status:     db.StatusPending,
					CreatedAt:  now,
				}
				_ = insertMessage(store, msg)

				pool.processBurst([]db.Message{msg})

				mu.Lock()
				defer mu.Unlock()
				if !postCalled {
					t.Fatalf("expected post_turn hook to be called")
				}
				if postReqReceived == nil {
					t.Fatalf("expected post_turn request to be captured")
				}
				if postReqReceived.ChannelID != "chan-postturn-test" || postReqReceived.ThreadID != "chan-postturn-test" {
					t.Errorf("channel/thread ID mismatch: got %q, %q", postReqReceived.ChannelID, postReqReceived.ThreadID)
				}
				if postReqReceived.DurationMs < 0 {
					t.Errorf("expected durationMs >= 0, got %d", postReqReceived.DurationMs)
				}
				tc.verify(t, postReqReceived)
			})
		}
	})

	t.Run("SerializedRelease", func(t *testing.T) {
		store := setupTestStore(t)

		var mu sync.Mutex
		turn1PostStarted := make(chan struct{})
		turn1PostFinished := false
		turn2PreCalledWhileTurn1InPost := false
		var turn2PreCalled bool

		mockDisp := &mockWebhookDispatcher{
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
						// Already closed (turn 2)
					default:
						close(turn1PostStarted)
						time.Sleep(50 * time.Millisecond)
						mu.Lock()
						turn1PostFinished = true
						mu.Unlock()
					}
				}
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		}

		pool := NewWorkerPool(WorkerPoolConfig{
			Store: store,
			TimeoutMinutes:    1,
			MaxAttempts:       1,
			WebhookDispatcher: mockDisp,
			RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
				return mockJSONResponse(sessionID, "ok"), "", 0, nil
			},
			ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
				return config.ChannelPolicy{
					Mode:     "channel",
					WakeMode: "all",
					Hooks: config.ChannelHooksConfig{
						PreTurn: &config.WebhookEndpoint{
							URL: "http://sidecar.local/pre-turn",
						},
						PostTurn: &config.WebhookEndpoint{
							URL: "http://sidecar.local/post-turn",
						},
					},
				}
			},
		})

		now := time.Now().UTC()
		msg1 := db.Message{
			ID:         "msg-turn-1",
			ThreadID:   "chan-serialized",
			AuthorName: "user-one",
			Content:    "turn 1",
			Status:     db.StatusPending,
			CreatedAt:  now,
		}
		msg2 := db.Message{
			ID:         "msg-turn-2",
			ThreadID:   "chan-serialized",
			AuthorName: "user-two",
			Content:    "turn 2",
			Status:     db.StatusPending,
			CreatedAt:  now.Add(time.Millisecond),
		}
		_ = insertMessage(store, msg1)
		_ = insertMessage(store, msg2)

		var wg sync.WaitGroup
		wg.Add(2)

		// Start Turn 1
		go func() {
			defer wg.Done()
			pool.processBurst([]db.Message{msg1})
		}()

		// Wait until Turn 1 enters post_turn
		<-turn1PostStarted

		// Start Turn 2 concurrently while Turn 1 is inside post_turn
		go func() {
			defer wg.Done()
			pool.processBurst([]db.Message{msg2})
		}()

		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		if !turn2PreCalled {
			t.Fatalf("expected Turn 2 pre_turn to be called")
		}
		if turn2PreCalledWhileTurn1InPost {
			t.Errorf("Turn 2 entered pre_turn BEFORE Turn 1 post_turn finished! Mutex lock was released prematurely!")
		}
	})

	t.Run("HelperEdgeCases", func(t *testing.T) {
		// 1. buildPostTurnRequest with nil te
		reqNil := buildPostTurnRequest(nil)
		if reqNil == nil || reqNil.Timestamp.IsZero() {
			t.Errorf("expected valid PostTurnRequest from nil te, got %+v", reqNil)
		}

		// 2. buildPostTurnRequest with empty effectiveID and fallback duration
		teFallback := &turnExecution{
			threadID:         "t-fallback",
			effectiveID:      "",
			turnStatus:       "success",
			turnResponseText: "fallback response",
			execStart:        time.Now().UTC().Add(-50 * time.Millisecond),
			hookMetadata:     map[string]any{"key": "val"},
		}
		reqFallback := buildPostTurnRequest(teFallback)
		if reqFallback.ChannelID != "t-fallback" {
			t.Errorf("expected ChannelID fallback to t-fallback, got %s", reqFallback.ChannelID)
		}
		if reqFallback.DurationMs <= 0 {
			t.Errorf("expected calculated DurationMs > 0, got %d", reqFallback.DurationMs)
		}
		if reqFallback.Metadata["key"] != "val" {
			t.Errorf("expected echoed metadata, got %+v", reqFallback.Metadata)
		}

		// 3. buildPostTurnRequest with empty turnStatus defaults to "failed"
		teEmptyStatus := &turnExecution{
			threadID: "t-empty",
		}
		reqEmptyStatus := buildPostTurnRequest(teEmptyStatus)
		if reqEmptyStatus.Status != "failed" {
			t.Errorf("expected default status 'failed', got %s", reqEmptyStatus.Status)
		}

		// 4. PreTurn rejection means turn was never attempted; post_turn must NOT fire
		store := setupTestStore(t)

		var postHookFired bool
		mockDisp := &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				return &PreTurnResponse{
					Allow:  false,
					Action: "drop",
					Reason: "denied",
				}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				postHookFired = true
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		}

		pool := NewWorkerPool(WorkerPoolConfig{
			Store: store,
			TimeoutMinutes:    1,
			MaxAttempts:       1,
			WebhookDispatcher: mockDisp,
			ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
				return config.ChannelPolicy{
					Mode:     "channel",
					WakeMode: "all",
					Hooks: config.ChannelHooksConfig{
						PreTurn: &config.WebhookEndpoint{
							URL: "http://sidecar.local/pre-turn",
						},
						PostTurn: &config.WebhookEndpoint{
							URL: "http://sidecar.local/post-turn",
						},
					},
				}
			},
		})

		msgDrop := db.Message{
			ID:         "msg-preturn-dropped",
			ThreadID:   "chan-preturn-dropped",
			AuthorName: "user-drop",
			Content:    "drop me",
			Status:     db.StatusPending,
			CreatedAt:  time.Now().UTC(),
		}
		_ = insertMessage(store, msgDrop)

		pool.processBurst([]db.Message{msgDrop})

		if postHookFired {
			t.Errorf("post_turn hook must NOT fire when pre_turn rejected/dropped the turn")
		}
	})
}

func TestWorkerPool_MultiTurn_AlwaysDeliversFinalOutputText(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tempDir := t.TempDir()
	convID := "c2222222-2222-3333-4444-555555555555"

	sessDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", convID)
	if err := os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	transcriptContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Check sync"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Checking status now...","tool_calls":[{"name":"status"}]}
{"step_index":2,"source":"SYSTEM","type":"USER_INPUT","content":"All in sync"}
{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Wait for background task task-645 to complete."}
`
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	var deliveredText string
	var deliveryCalled bool
	var mu sync.Mutex
	doneCh := make(chan struct{})

	expected := "Wait for background task task-645 to complete."
	sessMgr := session.New(tempDir, "")
	pool := NewWorkerPool(WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return mockMultiTurnJSONResponse(convID, expected, 2), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalled = true
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:         "msg-delivered-1",
		ThreadID:   "thread-delivered-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "Check sync",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	waitForCompletion(t, doneCh, 3*time.Second, "message processing")

	mu.Lock()
	defer mu.Unlock()
	if !deliveryCalled || deliveredText != expected {
		t.Errorf("Delivered text mismatch: called=%v, got=%q, want=%q", deliveryCalled, deliveredText, expected)
	}
	dbMsg, err := getMessage(store, "msg-delivered-1")
	if err != nil || dbMsg == nil || dbMsg.Status != db.StatusCompleted || dbMsg.ResponseText != expected {
		t.Errorf("DB response mismatch: %+v (err %v)", dbMsg, err)
	}
}

func TestSession_RotationThresholds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		preSetup   func(t *testing.T, sessMgr *session.Manager, store db.Store, oldSessionID string)
		runnerFunc func(sessDir string, oldSessionID string, mu *sync.Mutex, attempts *[]string) runnerFunc
		watchdogFn func(sessDir string, oldSessionID string, mu *sync.Mutex, attempts *[]string) runnerWithOptionsFunc
		verify     func(t *testing.T, store db.Store, oldSessionID string, attempts []string)
	}{
		{
			name: "StepLimit_PreFlight",
			preSetup: func(t *testing.T, sessMgr *session.Manager, store db.Store, oldSessionID string) {
				sessDir, _ := sessMgr.EnsureSessionDir(oldSessionID)
				tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
				content := fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\\n", session.DefaultMaxSessionSteps)
				_ = os.WriteFile(tPath, []byte(content), 0644)
				_ = saveSessionID(store, "thread-rot-threshold", oldSessionID)
				_, _ = incrementSessionTurnCount(store, "thread-rot-threshold")
			},
			runnerFunc: func(sessDir string, oldSessionID string, mu *sync.Mutex, attempts *[]string) runnerFunc {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					mu.Lock()
					*attempts = append(*attempts, sessionID)
					mu.Unlock()
					return mockJSONResponse(sessionID, "OK"), "", 0, nil
				}
			},
			verify: func(t *testing.T, store db.Store, oldSessionID string, attempts []string) {
				if len(attempts) != 1 || attempts[0] != oldSessionID {
					t.Errorf("Expected session %q preserved in runner, got: %v", oldSessionID, attempts)
				}
				curSess, _ := getSessionID(store, "thread-rot-threshold")
				if curSess != oldSessionID {
					t.Errorf("Expected session in store to remain %q, got %q", oldSessionID, curSess)
				}
			},
		},
		{
			name: "ByteLimit_PreFlight",
			preSetup: func(t *testing.T, sessMgr *session.Manager, store db.Store, oldSessionID string) {
				sessDir, _ := sessMgr.EnsureSessionDir(oldSessionID)
				tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
				blob := make([]byte, session.DefaultMaxTranscriptBytes+1024)
				for i := range blob {
					blob[i] = 'A'
				}
				_ = os.WriteFile(tPath, blob, 0644)
				_ = saveSessionID(store, "thread-rot-threshold", oldSessionID)
				_, _ = incrementSessionTurnCount(store, "thread-rot-threshold")
			},
			runnerFunc: func(sessDir string, oldSessionID string, mu *sync.Mutex, attempts *[]string) runnerFunc {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					mu.Lock()
					*attempts = append(*attempts, sessionID)
					mu.Unlock()
					return mockJSONResponse(sessionID, "OK"), "", 0, nil
				}
			},
			verify: func(t *testing.T, store db.Store, oldSessionID string, attempts []string) {
				if len(attempts) != 1 || attempts[0] != oldSessionID {
					t.Errorf("Expected session %q preserved in runner, got: %v", oldSessionID, attempts)
				}
				curSess, _ := getSessionID(store, "thread-rot-threshold")
				if curSess != oldSessionID {
					t.Errorf("Expected session in store to remain %q, got %q", oldSessionID, curSess)
				}
			},
		},
		{
			name: "StepLimit_PostExecution",
			preSetup: func(t *testing.T, sessMgr *session.Manager, store db.Store, oldSessionID string) {
				_, _ = sessMgr.EnsureSessionDir(oldSessionID)
			},
			runnerFunc: func(sessDir string, oldSessionID string, mu *sync.Mutex, attempts *[]string) runnerFunc {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
					content := fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\\n", session.DefaultMaxSessionSteps)
					_ = os.WriteFile(tPath, []byte(content), 0644)
					return mockJSONResponse(oldSessionID, "Execution finished with many steps"), "", 0, nil
				}
			},
			verify: func(t *testing.T, store db.Store, oldSessionID string, attempts []string) {
				curSess, _ := getSessionID(store, "thread-rot-threshold")
				if curSess != oldSessionID {
					t.Errorf("Expected session to be preserved post execution (%q), got: %q", oldSessionID, curSess)
				}
			},
		},
		{
			name: "WatchdogCircuitBreaker_OnBloat",
			preSetup: func(t *testing.T, sessMgr *session.Manager, store db.Store, oldSessionID string) {
				_, _ = sessMgr.EnsureSessionDir(oldSessionID)
				_ = saveSessionID(store, "thread-rot-threshold", oldSessionID)
			},
			watchdogFn: func(sessDir string, oldSessionID string, mu *sync.Mutex, attempts *[]string) runnerWithOptionsFunc {
				return func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					mu.Lock()
					*attempts = append(*attempts, sessionID)
					currentAttempt := len(*attempts)
					mu.Unlock()

					if currentAttempt == 1 {
						tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
						blob := make([]byte, session.DefaultMaxTranscriptBytes+2048)
						_ = os.WriteFile(tPath, blob, 0644)
						return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
					}
					return mockJSONResponse(sessionID, "Recovered on warm retry"), "", 0, nil
				}
			},
			verify: func(t *testing.T, store db.Store, oldSessionID string, attempts []string) {
				if len(attempts) != 2 {
					t.Fatalf("Expected 2 attempts, got %d", len(attempts))
				}
				if attempts[0] != oldSessionID || attempts[1] != oldSessionID {
					t.Errorf("Expected both attempts to preserve session %q, got: %v", oldSessionID, attempts)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			tmpDir := t.TempDir()
			sessMgr := session.New(tmpDir, "")

			sessionID := "sess-rot-" + strings.ToLower(tc.name)
			sessDir, _ := sessMgr.EnsureSessionDir(sessionID)
			if tc.preSetup != nil {
				tc.preSetup(t, sessMgr, store, sessionID)
			}

			var mu sync.Mutex
			var attempts []string
			doneCh := make(chan struct{}, 1)

			appCfg := config.NewFromData(&config.ConfigData{
				Channels: map[string]config.ChannelPolicy{
					"default": {Mode: "thread"},
				},
			})

			poolCfg := WorkerPoolConfig{
				SessionManager: sessMgr,
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    5 * time.Millisecond,
				MaxAttempts:    2,
				DeliveryFunc:   noopDelivery,
				TypingFunc:     noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					select {
					case doneCh <- struct{}{}:
					default:
					}
				},
			}
			if tc.runnerFunc != nil {
				poolCfg.RunnerFunc = tc.runnerFunc(sessDir, sessionID, &mu, &attempts)
			}
			if tc.watchdogFn != nil {
				poolCfg.RunnerWithOptionsFunc = tc.watchdogFn(sessDir, sessionID, &mu, &attempts)
			}

			pool := New(appCfg, poolCfg)
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: "msg-rot-" + tc.name, ThreadID: "thread-rot-threshold", Content: "Run rotation test"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			waitForCompletion(t, doneCh, 3*time.Second, "rotation threshold message")

			mu.Lock()
			attCopy := append([]string(nil), attempts...)
			mu.Unlock()

			tc.verify(t, store, sessionID, attCopy)
		})
	}
}

func TestSession_Rotation_ChannelModeAndRemainingBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       string
		threadID   string
		setupSess  func(sessDir string)
		runnerFunc func(sessDir string, sessionID string, mu *sync.Mutex, attempts *[]string) runnerFunc
		watchdogFn func(sessDir string, sessionID string, mu *sync.Mutex, attempts *[]string) runnerWithOptionsFunc
		verify     func(t *testing.T, store db.Store, sessionID string, attempts []string)
	}{
		{
			name:     "ChannelMode_PreFlight_ByteLimit",
			mode:     "channel",
			threadID: "chan-byte-test",
			setupSess: func(sessDir string) {
				tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
				blob := make([]byte, session.DefaultMaxTranscriptBytes+1024)
				for i := range blob {
					blob[i] = 'A'
				}
				_ = os.WriteFile(tPath, blob, 0644)
			},
			verify: func(t *testing.T, store db.Store, sessionID string, attempts []string) {
				if len(attempts) != 1 || attempts[0] != sessionID {
					t.Errorf("Expected session %q preserved, got: %v", sessionID, attempts)
				}
			},
		},
		{
			name:     "ChannelMode_PreFlight_StepLimit",
			mode:     "channel",
			threadID: "chan-step-test",
			setupSess: func(sessDir string) {
				tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
				content := fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\\n", session.DefaultMaxSessionSteps)
				_ = os.WriteFile(tPath, []byte(content), 0644)
			},
			verify: func(t *testing.T, store db.Store, sessionID string, attempts []string) {
				if len(attempts) != 1 || attempts[0] != sessionID {
					t.Errorf("Expected session %q preserved, got: %v", sessionID, attempts)
				}
			},
		},
		{
			name:     "PostExecution_ByteLimit",
			mode:     "thread",
			threadID: "thread-post-byte-test",
			runnerFunc: func(sessDir string, sessionID string, mu *sync.Mutex, attempts *[]string) runnerFunc {
				return func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
					_ = os.WriteFile(tPath, make([]byte, session.DefaultMaxTranscriptBytes+500), 0644)
					return mockJSONResponse(sessionID, "Execution heavy output"), "", 0, nil
				}
			},
			verify: func(t *testing.T, store db.Store, sessionID string, attempts []string) {
				curSess, _ := getSessionID(store, "thread-post-byte-test")
				if curSess != sessionID {
					t.Errorf("Expected session preserved post execution (%q), got: %q", sessionID, curSess)
				}
			},
		},
		{
			name:     "Watchdog_StepLimit",
			mode:     "thread",
			threadID: "thread-wd-step",
			watchdogFn: func(sessDir string, sessionID string, mu *sync.Mutex, attempts *[]string) runnerWithOptionsFunc {
				return func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					mu.Lock()
					*attempts = append(*attempts, sID)
					currentAttempt := len(*attempts)
					mu.Unlock()

					if currentAttempt == 1 {
						tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
						content := fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\\n", session.DefaultMaxSessionSteps)
						_ = os.WriteFile(tPath, []byte(content), 0644)
						return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
					}
					return mockJSONResponse(sID, "Recovered on warm retry"), "", 0, nil
				}
			},
			verify: func(t *testing.T, store db.Store, sessionID string, attempts []string) {
				if len(attempts) != 2 {
					t.Fatalf("Expected 2 attempts, got %d", len(attempts))
				}
				if attempts[1] != sessionID {
					t.Errorf("Expected attempt 2 to preserve session %q, got: %q", sessionID, attempts[1])
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := setupTestStore(t)
			tmpDir := t.TempDir()
			sessMgr := session.New(tmpDir, "")

			sessionID := "sess-" + strings.ToLower(tc.name)
			sessDir, _ := sessMgr.EnsureSessionDir(sessionID)
			_ = saveSessionID(store, tc.threadID, sessionID)
			_, _ = incrementSessionTurnCount(store, tc.threadID)

			if tc.setupSess != nil {
				tc.setupSess(sessDir)
			}

			var mu sync.Mutex
			var attempts []string
			doneCh := make(chan struct{}, 1)

			appCfg := config.NewFromData(&config.ConfigData{
				Channels: map[string]config.ChannelPolicy{
					"default": {Mode: tc.mode},
				},
			})

			poolCfg := WorkerPoolConfig{
				SessionManager: sessMgr,
				Store:          store,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    2,
				DeliveryFunc:   noopDelivery,
				TypingFunc:     noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					select {
					case doneCh <- struct{}{}:
					default:
					}
				},
			}
			if tc.runnerFunc != nil {
				poolCfg.RunnerFunc = tc.runnerFunc(sessDir, sessionID, &mu, &attempts)
			} else if tc.watchdogFn != nil {
				poolCfg.RunnerWithOptionsFunc = tc.watchdogFn(sessDir, sessionID, &mu, &attempts)
			} else {
				poolCfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					mu.Lock()
					attempts = append(attempts, sID)
					mu.Unlock()
					return mockJSONResponse(sID, "OK"), "", 0, nil
				}
			}

			pool := New(appCfg, poolCfg)
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: "msg-" + tc.name, ThreadID: tc.threadID, Content: "<@aerial> run"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			waitForCompletion(t, doneCh, 3*time.Second, "channel mode rotation test")

			mu.Lock()
			attCopy := append([]string(nil), attempts...)
			mu.Unlock()

			tc.verify(t, store, sessionID, attCopy)
		})
	}
}

func TestWorkerPool_MultiTurn_TranscriptSilent_PreservesSubstantiveRunnerResponse(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	tempDir := t.TempDir()
	convID := "c3333333-3333-4444-5555-666666666666"

	sessDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", convID)
	if err := os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	transcriptContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Check deployment"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Checking status now...","tool_calls":[{"name":"status"}]}
{"step_index":2,"source":"SYSTEM","type":"USER_INPUT","content":"All containers deployed"}
{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","content":"   "}
`
	if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	var deliveredText string
	var mu sync.Mutex
	doneCh := make(chan struct{})

	sessMgr := session.New(tempDir, "")
	expectedText := "Continuous Deployment for PR #322 has completed successfully!"
	pool := NewWorkerPool(WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    1,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			return mockMultiTurnJSONResponse(convID, expectedText, 2), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			close(doneCh)
		},
	})
	pool.Start()
	defer pool.Stop()

	msg := db.Message{
		ID:         "msg-substantive-fallback-1",
		ThreadID:   "thread-substantive-fallback-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "User",
		Content:    "Check deployment",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)

	waitForCompletion(t, doneCh, 3*time.Second, "message processing")

	mu.Lock()
	defer mu.Unlock()
	if deliveredText != expectedText {
		t.Errorf("Delivered text mismatch:\ngot:  %q\nwant: %q", deliveredText, expectedText)
	}
	dbMsg, err := getMessage(store, "msg-substantive-fallback-1")
	if err != nil || dbMsg == nil || dbMsg.ResponseText != expectedText {
		t.Errorf("DB response text mismatch: %+v (err %v)", dbMsg, err)
	}
}
