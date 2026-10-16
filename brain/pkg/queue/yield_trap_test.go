package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
)

func TestYieldTrap_Scenarios(t *testing.T) {
	tests := []struct {
		name                 string
		threadID             string
		sessionID            string
		msgID                string
		yieldType            string // "stderr" or "daemon_tracker"
		maxYields            int    // how many turns yield before success (or all yield)
		expectInvocations    int32
		expectDeliveredText  string
		metricOutcome        string // "resumed" or "circuit_breaker"
		metricSignature      string // "stderr_signature" or "daemon_tracker"
		verifyPromptsSeen    bool
		expectedSecondPrompt string
	}{
		{
			name:                 "auto-resumption on stderr signature success",
			threadID:             "thread-yield-auto",
			sessionID:            "f2222222-3333-4444-5555-666666666666",
			msgID:                "msg-yield-1",
			yieldType:            "stderr",
			maxYields:            1,
			expectInvocations:    2,
			expectDeliveredText:  "Job completed after resumption!",
			metricOutcome:        "resumed",
			metricSignature:      "stderr_signature",
			verifyPromptsSeen:    true,
			expectedSecondPrompt: YieldTrapResumePrompt,
		},
		{
			name:                "circuit breaker trips after max resumes",
			threadID:            "thread-yield-cb",
			sessionID:           "f3333333-4444-5555-6666-777777777777",
			msgID:               "msg-circuit-1",
			yieldType:           "stderr",
			maxYields:           99, // all invocations yield
			expectInvocations:   3,
			expectDeliveredText: "Still waiting turn 3",
			metricOutcome:       "circuit_breaker",
			metricSignature:     "stderr_signature",
		},
		{
			name:                "daemon tracker triggers auto-resumption",
			threadID:            "thread-yield-dt",
			sessionID:           "f4444444-5555-6666-7777-888888888888",
			msgID:               "msg-audit-1",
			yieldType:           "daemon_tracker",
			maxYields:           1,
			expectInvocations:   2,
			expectDeliveredText: "Recovered via daemon tracker!",
			metricOutcome:       "resumed",
			metricSignature:     "daemon_tracker",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := setupTestStore(t)
			tmpHome := t.TempDir()

			var runnerInvocations atomic.Int32
			var promptsSeenMu sync.Mutex
			var promptsSeen []string

			var deliveredMu sync.Mutex
			var deliveredText string
			doneCh := make(chan struct{})

			sessMgr := session.New(tmpHome, "")
			sessDir, err := sessMgr.EnsureSessionDir(tc.sessionID)
			if err != nil {
				t.Fatalf("Failed to ensure session dir: %v", err)
			}
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)

			var procPool runner.AgentPool
			var daemon *runner.StreamingDaemon
			mockBin := ""

			if tc.yieldType == "daemon_tracker" {
				tasksDir := filepath.Join(sessDir, ".system_generated", "tasks")
				_ = os.MkdirAll(tasksDir, 0755)
				_ = os.WriteFile(filepath.Join(tasksDir, "task-999.log"), []byte("Task output\n"), 0644)

				mockBin = filepath.Join(tmpHome, "mock_agy.sh")
				_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

				mockSpawner := runner.NewMockDaemonSpawner()
				pPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
				defer func() {
					if err := pPool.Close(); err != nil {
						t.Logf("cleanup unified pool: %v", err)
					}
				}()
				procPool = pPool
			}

			pool := NewWorkerPool(WorkerPoolConfig{
				ProcessPool:    procPool,
				SessionManager: sessMgr,
				Store:          store,
				AgyBin:         mockBin,
				TimeoutMinutes: 1,
				BackoffBase:    10 * time.Millisecond,
				MaxAttempts:    3,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
					inv := runnerInvocations.Add(1)
					promptsSeenMu.Lock()
					promptsSeen = append(promptsSeen, prompt)
					promptsSeenMu.Unlock()

					_ = os.Chtimes(sessDir, time.Now(), time.Now())

					if int(inv) <= tc.maxYields {
						if tc.yieldType == "daemon_tracker" {
							out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Waiting on background task task-999 to complete.","usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}`, tc.sessionID)
							return out, "", 0, nil
						}
						// stderr yield
						msg := fmt.Sprintf("Still waiting turn %d", inv)
						if tc.maxYields == 1 {
							msg = "Waiting on background task task-444"
						}
						out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q,"usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}`, tc.sessionID, msg)
						return out, "terminating 1 background task(s) on exit\n", 0, nil
					}

					if daemon != nil {
						daemon.TaskTracker().Remove("task-999")
					}
					out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q,"usage":{"input_tokens":150,"output_tokens":35,"total_tokens":185}}`, tc.sessionID, tc.expectDeliveredText)
					return out, "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					deliveredMu.Lock()
					deliveredText = text
					deliveredMu.Unlock()
					return nil
				},
				TypingFunc: func(s *discordgo.Session, channelID string) func() {
					return func() {}
				},
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					close(doneCh)
				},
			})
			pool.Start()
			defer pool.Stop()

			if tc.yieldType == "daemon_tracker" {
				d, err := pool.ProcessPool().GetOrCreate(context.Background(), tc.threadID, "")
				if err != nil {
					t.Fatalf("Failed to create daemon: %v", err)
				}
				daemon = d
				daemon.TaskTracker().Add(runner.TaskMetadata{
					TaskID:    "task-999",
					StartedAt: time.Now(),
				})
			}

			msg := db.Message{
				ID:         tc.msgID,
				ThreadID:   tc.threadID,
				GuildID:    "guild-1",
				AuthorID:   "user-1",
				AuthorName: "Alex",
				Content:    "Run task",
				Status:     db.StatusPending,
				CreatedAt:  time.Now().UTC(),
				UpdatedAt:  time.Now().UTC(),
			}
			if err := insertMessage(store, msg); err != nil {
				t.Fatalf("Failed to insert message: %v", err)
			}

			model := pool.GetRuntimeConfig()
			initialMetric := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues(tc.metricOutcome, tc.metricSignature, model))

			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for message processing")
			}

			if runnerInvocations.Load() != tc.expectInvocations {
				t.Fatalf("expected %d runner invocations, got %d", tc.expectInvocations, runnerInvocations.Load())
			}

			if tc.verifyPromptsSeen {
				promptsSeenMu.Lock()
				seenCount := len(promptsSeen)
				secondPrompt := ""
				if seenCount > 1 {
					secondPrompt = promptsSeen[1]
				}
				promptsSeenMu.Unlock()

				if seenCount != int(tc.expectInvocations) {
					t.Fatalf("expected %d prompts seen, got %d", tc.expectInvocations, seenCount)
				}
				if secondPrompt != tc.expectedSecondPrompt {
					t.Errorf("expected resumed prompt %q, got %q", tc.expectedSecondPrompt, secondPrompt)
				}
			}

			deliveredMu.Lock()
			finalText := deliveredText
			deliveredMu.Unlock()
			if finalText != tc.expectDeliveredText {
				t.Errorf("expected delivered text %q, got %q", tc.expectDeliveredText, finalText)
			}

			newMetric := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues(tc.metricOutcome, tc.metricSignature, model))
			if newMetric != initialMetric+1 {
				t.Errorf("expected metric %s/%s to increment by 1, got delta %.0f", tc.metricOutcome, tc.metricSignature, newMetric-initialMetric)
			}
		})
	}
}
