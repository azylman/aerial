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
	"github.com/azylman/aerial/brain/pkg/session"
)

func TestYieldTrap_AutoResumption_Success(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	sessID := "f2222222-3333-4444-5555-666666666666"

	var runnerInvocations atomic.Int32
	var promptsSeenMu sync.Mutex
	var promptsSeen []string

	var deliveredMu sync.Mutex
	var deliveredText string
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		SessionManager: session.New(tmpHome, ""),
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			inv := runnerInvocations.Add(1)
			promptsSeenMu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			promptsSeenMu.Unlock()

			sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", sessID)
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)

			if inv == 1 {
				// Turn 1 yields prematurely on background task
				out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Waiting on background task task-444","usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}`, sessID)
				return out, "terminating 1 background task(s) on exit\n", 0, nil
			}

			// Resumed turn finishes cleanly
			out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Job completed after resumption!","usage":{"input_tokens":150,"output_tokens":35,"total_tokens":185}}`, sessID)
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

	msg := db.Message{
		ID:         "msg-yield-1",
		ThreadID:   "thread-yield-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "Alex",
		Content:    "Run long operation",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := insertMessage(store, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	model := pool.GetRuntimeConfig()
	initialResumed := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues("resumed", "stderr_signature", model))

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}

	if runnerInvocations.Load() != 2 {
		t.Fatalf("expected 2 runner invocations (1 initial yield + 1 auto-resume), got %d", runnerInvocations.Load())
	}

	promptsSeenMu.Lock()
	defer promptsSeenMu.Unlock()
	if len(promptsSeen) != 2 {
		t.Fatalf("expected 2 prompts seen, got %d", len(promptsSeen))
	}
	if promptsSeen[1] != YieldTrapResumePrompt {
		t.Errorf("expected resumed prompt to be %q, got %q", YieldTrapResumePrompt, promptsSeen[1])
	}

	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	if deliveredText != "Job completed after resumption!" {
		t.Errorf("expected final substantive output, got %q", deliveredText)
	}

	newResumed := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues("resumed", "stderr_signature", model))
	if newResumed != initialResumed+1 {
		t.Errorf("expected resumed metric to increment by 1, got delta %.0f", newResumed-initialResumed)
	}
}

func TestYieldTrap_CircuitBreaker_TripsAfterMaxResumes(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	sessID := "f3333333-4444-5555-6666-777777777777"

	var runnerInvocations atomic.Int32
	var deliveredMu sync.Mutex
	var deliveredText string
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		SessionManager: session.New(tmpHome, ""),
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			inv := runnerInvocations.Add(1)
			sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", sessID)
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)

			// All invocations yield on background task
			out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Still waiting turn %d","usage":{"input_tokens":50,"output_tokens":10,"total_tokens":60}}`, sessID, inv)
			return out, "terminating 1 background task(s) on exit\n", 0, nil
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

	msg := db.Message{
		ID:         "msg-circuit-1",
		ThreadID:   "thread-circuit-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "Alex",
		Content:    "Run loop",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := insertMessage(store, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	model := pool.GetRuntimeConfig()
	initialCB := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues("circuit_breaker", "stderr_signature", model))

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for circuit breaker to complete")
	}

	// 1 initial run + 2 auto-resumptions = 3 total invocations
	if runnerInvocations.Load() != 3 {
		t.Fatalf("expected 3 runner invocations before circuit breaker trip, got %d", runnerInvocations.Load())
	}

	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	if deliveredText != "Still waiting turn 3" {
		t.Errorf("expected final invocation output delivered on circuit breaker trip, got %q", deliveredText)
	}

	newCB := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues("circuit_breaker", "stderr_signature", model))
	if newCB != initialCB+1 {
		t.Errorf("expected circuit_breaker metric to increment by 1, got delta %.0f", newCB-initialCB)
	}
}

func TestYieldTrap_TranscriptAudit_TriggersAutoResumption(t *testing.T) {
	store := setupTestStore(t)
	tmpHome := t.TempDir()
	sessID := "f4444444-5555-6666-7777-888888888888"

	var runnerInvocations atomic.Int32
	var deliveredMu sync.Mutex
	var deliveredText string
	doneCh := make(chan struct{})

	pool := NewWorkerPool(WorkerPoolConfig{
		SessionManager: session.New(tmpHome, ""),
		Store:          store,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    3,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
			inv := runnerInvocations.Add(1)
			sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", sessID)
			logsDir := filepath.Join(sessDir, ".system_generated", "logs")
			_ = os.MkdirAll(logsDir, 0755)

			if inv == 1 {
				// Turn 1 writes an unfinished background task to transcript, but stderr is empty
				transcript := `{"step_index": 0, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "run task"}
{"step_index": 1, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: ` + sessID + `/task-999"}
{"step_index": 2, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Waiting on background task task-999 to complete."}
`
				_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644)
				out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Waiting on background task task-999 to complete.","usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}`, sessID)
				// Zero stderr notice, but transcript shows unfinished task!
				return out, "", 0, nil
			}

			// Resumed turn marks task finished in transcript
			transcript := `{"step_index": 0, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "run task"}
{"step_index": 1, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: ` + sessID + `/task-999"}
{"step_index": 2, "source": "SYSTEM", "type": "SYSTEM_MESSAGE", "status": "DONE", "content": "Task id \"` + sessID + `/task-999\" finished with result:\nDone"}
{"step_index": 3, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Recovered via transcript audit!"}
`
			_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644)
			out := fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":"Recovered via transcript audit!","usage":{"input_tokens":120,"output_tokens":30,"total_tokens":150}}`, sessID)
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

	msg := db.Message{
		ID:         "msg-audit-1",
		ThreadID:   "thread-audit-1",
		GuildID:    "guild-1",
		AuthorID:   "user-1",
		AuthorName: "Alex",
		Content:    "Run task with silent stderr",
		Status:     db.StatusPending,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := insertMessage(store, msg); err != nil {
		t.Fatalf("Failed to insert message: %v", err)
	}

	model := pool.GetRuntimeConfig()
	initialAudit := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues("resumed", "session_audit", model))

	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for transcript audit test to complete")
	}

	if runnerInvocations.Load() != 2 {
		t.Fatalf("expected 2 runner invocations (1 intercepted via transcript + 1 auto-resume), got %d", runnerInvocations.Load())
	}

	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	if deliveredText != "Recovered via transcript audit!" {
		t.Errorf("expected final substantive output, got %q", deliveredText)
	}

	newAudit := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues("resumed", "session_audit", model))
	if newAudit != initialAudit+1 {
		t.Errorf("expected session_audit resumed metric to increment by 1, got delta %.0f", newAudit-initialAudit)
	}
}

