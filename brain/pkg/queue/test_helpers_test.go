package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/runner"
)

// setupTestStore returns a clean FakeStore for testing.
func setupTestStore(t testing.TB) *db.FakeStore {
	return db.NewFakeStore()
}

func insertMessage(store db.Store, msg db.Message) error {
	if store == nil {
		return nil
	}
	return store.InsertMessage(context.Background(), msg)
}

func getMessage(store db.Store, id string) (*db.Message, error) {
	if store == nil {
		return nil, nil
	}
	return store.GetMessage(context.Background(), id)
}

func saveSessionID(store db.Store, threadID, sessionID string) error {
	if store == nil {
		return nil
	}
	return store.SaveSessionID(context.Background(), threadID, sessionID)
}

func getSessionID(store db.Store, threadID string) (string, error) {
	if store == nil {
		return "", nil
	}
	return store.GetSessionID(context.Background(), threadID)
}

func getPreviousSessionID(store db.Store, threadID string) (string, error) {
	if store == nil {
		return "", nil
	}
	return store.GetPreviousSessionID(context.Background(), threadID)
}

func createScheduleRun(store db.Store, run db.ScheduleRun) error {
	if store == nil {
		return nil
	}
	return store.CreateScheduleRun(context.Background(), run)
}

func getScheduleRun(store db.Store, id string) (*db.ScheduleRun, error) {
	if store == nil {
		return nil, nil
	}
	if fake, ok := store.(*db.FakeStore); ok {
		return fake.GetScheduleRun(id)
	}
	runs, _, err := store.GetScheduleRunsPaginated(context.Background(), 100, 0, "", "")
	if err != nil {
		return nil, err
	}
	for _, r := range runs {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, nil
}

func getSessionTurnCount(store db.Store, sessionKey string) (int, error) {
	if store == nil {
		return 0, nil
	}
	return store.GetSessionTurnCount(context.Background(), sessionKey)
}

func incrementSessionTurnCount(store db.Store, sessionKey string) (int, error) {
	if store == nil {
		return 0, nil
	}
	return store.IncrementSessionTurnCount(context.Background(), sessionKey)
}

func getScheduleRunsPaginated(store db.Store, limit, offset int, scheduleID, status string) ([]db.ScheduleRun, int, error) {
	if store == nil {
		return nil, 0, nil
	}
	return store.GetScheduleRunsPaginated(context.Background(), limit, offset, scheduleID, status)
}

func getThreadSummary(store db.Store, threadID string) (string, string, error) {
	if store == nil {
		return "", "", nil
	}
	return store.GetThreadSummary(context.Background(), threadID)
}

func saveThreadSummary(store db.Store, threadID, summary, lastMsgID string) error {
	if store == nil {
		return nil
	}
	return store.SaveThreadSummary(context.Background(), threadID, summary, lastMsgID)
}

func rotateSessionID(store db.Store, sessionKey, newSessionID string) error {
	if store == nil {
		return nil
	}
	return store.RotateSessionID(context.Background(), sessionKey, newSessionID)
}

// noopDelivery returns a discordgo delivery function that always succeeds.
func noopDelivery(s *discordgo.Session, channelID, text string) error {
	return nil
}

// noopTyping returns a discordgo typing indicator function that does nothing.
func noopTyping(s *discordgo.Session, channelID string) func() {
	return func() {}
}

// waitForCompletion waits for doneCh with a timeout, failing the test if expired.
func waitForCompletion(t testing.TB, doneCh <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-doneCh:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for completion: %s", msg)
	}
}

func waitPoolWorkersDone(t *testing.T, pool *WorkerPool, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		pool.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for worker pool goroutines to exit")
	}
}

func mockJSONResponseWithUsage(convID, responseText string, usage runner.TokenUsage) string {
	if convID == "" {
		convID = uuid.New().String()
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"conversation_id":  convID,
		"status":           "SUCCESS",
		"response":         responseText,
		"duration_seconds": 1.0,
		"num_turns":        1,
		"usage":            usage,
	})
	return string(payload)
}

func mockMultiTurnJSONResponse(convID, responseText string, numTurns int) string {
	if convID == "" {
		convID = uuid.New().String()
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"conversation_id":  convID,
		"status":           "SUCCESS",
		"response":         responseText,
		"duration_seconds": 1.0,
		"num_turns":        numTurns,
	})
	return string(payload)
}


type runnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
type runnerWithOptionsFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)

// TestSinkMethods_CoverageAudit audits sink.go no-ops and ScopeKey to satisfy target coverage.
func TestSinkMethods_CoverageAudit(t *testing.T) {
	pool := NewWorkerPool(WorkerPoolConfig{})
	msg := db.Message{ID: "m1", ThreadID: "t1"}
	if k := pool.ScopeKey(msg); k != "t1" {
		t.Errorf("expected ScopeKey t1, got %s", k)
	}

	ds := &DiscordTurnSink{}
	ds.OnTurnStarted()
	ds.OnThinking()
	ds.OnToolCompleted("tool", "srv", time.Second, "ok")
	ds.OnSkillActivated("skill", "src")

	vs := &VoiceTurnSink{}
	vs.OnTurnStarted()
	vs.OnThinking()
	vs.OnToolCall("tool", "cmd")
	vs.OnToolCompleted("tool", "srv", time.Second, "ok")
	vs.OnSkillActivated("skill", "src")
}
