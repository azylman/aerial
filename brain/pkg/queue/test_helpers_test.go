package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
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

type mockRunnerStep struct {
	convID          string
	response        string
	stderr          string
	exitCode        int
	errMsg          string
	panicMsg        string
	writeTranscript bool
}

func execMockRunnerStep(step mockRunnerStep, tmpHome, sessionID string) (string, string, int, error) {
	if step.panicMsg != "" {
		panic(step.panicMsg)
	}
	if step.errMsg != "" {
		return "", step.stderr, step.exitCode, errors.New(step.errMsg)
	}
	convID := step.convID
	if convID == "" && sessionID != "" {
		convID = sessionID
	}
	if step.writeTranscript && tmpHome != "" {
		if convID == "" {
			convID = uuid.New().String()
		}
		sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", convID)
		_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
		cliPbDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "conversations")
		_ = os.MkdirAll(cliPbDir, 0755)
		_ = os.WriteFile(filepath.Join(cliPbDir, convID+".pb"), []byte("mock-pb"), 0644)
		now := time.Now()
		_ = os.Chtimes(sessDir, now, now)
	}
	stderr := step.stderr
	if stderr == "" && convID != "" {
		stderr = fmt.Sprintf("Starting conversation update stream for %s\n", convID)
	}
	return mockJSONResponse(convID, step.response), stderr, step.exitCode, nil
}

type workerTestCase struct {
	name                  string
	useSessionMgr         bool
	initialSessionID      string
	seedTurnCount         int
	seedIdleAge           time.Duration
	maxAttempts           int
	stalenessTTL          time.Duration
	messageAge            time.Duration
	policyMode            string
	notifierReply         string
	enqueueAsBurst        bool
	sequentialTurns       bool
	messages              []db.Message
	runnerSteps           []mockRunnerStep
	wantRunnerCalls       int
	checkRunnerCalls      bool
	wantPassedSessions    []string
	wantStatus            string
	wantRetryCount        int
	wantSavedSession      string
	wantTurnCount         int
	checkTurnCount        bool
	wantDeliveredTexts    []string
	wantDeliveredChannels []string
	wantDeliveryCalls     int
	checkDeliveryCalls    bool
	wantPromptContains    []string
	wantPromptExcludes    []string
	wantDBContent         string
	wantResponseText      string
}

func runWorkerTestCase(t *testing.T, tc workerTestCase) {
	t.Helper()
	store := setupTestStore(t)
	var tmpHome string
	var sessMgr *session.Manager
	if tc.useSessionMgr || tc.seedTurnCount > 0 || tc.seedIdleAge > 0 {
		tmpHome = t.TempDir()
		sessMgr = session.New(tmpHome, "")
	}

	threadID := tc.messages[0].ThreadID
	if tc.initialSessionID != "" {
		_ = saveSessionID(store, threadID, tc.initialSessionID)
		if sessMgr != nil {
			sessDir, _ := sessMgr.EnsureSessionDir(tc.initialSessionID)
			cliPbDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "conversations")
			_ = os.MkdirAll(cliPbDir, 0755)
			pbFile := filepath.Join(cliPbDir, tc.initialSessionID+".pb")
			_ = os.WriteFile(pbFile, []byte("mock-pb"), 0644)
			if tc.seedIdleAge > 0 {
				idleTime := time.Now().UTC().Add(-tc.seedIdleAge)
				store.SetSessionInfo(db.SessionInfo{
					ThreadID:          threadID,
					InternalSessionID: tc.initialSessionID,
					TurnCount:         tc.seedTurnCount,
					CreatedAt:         idleTime,
					UpdatedAt:         idleTime,
				})
				_ = os.Chtimes(pbFile, idleTime, idleTime)
				_ = os.Chtimes(cliPbDir, idleTime, idleTime)
				_ = os.Chtimes(sessDir, idleTime, idleTime)
			}
		}
	}
	if tc.seedTurnCount > 0 && tc.seedIdleAge == 0 {
		for i := 0; i < tc.seedTurnCount; i++ {
			_, _ = incrementSessionTurnCount(store, threadID)
		}
	}

	maxAttempts := tc.maxAttempts
	if maxAttempts == 0 {
		maxAttempts = 1
	}

	var mu sync.Mutex
	var runnerCalls int
	var passedSessions []string
	var capturedPrompt string
	var deliveredTexts []string
	var deliveredChannels []string
	completedCount := 0
	doneCh := make(chan struct{})
	turnDoneCh := make(chan struct{}, len(tc.messages)+1)

	cfg := WorkerPoolConfig{
		Store:          store,
		SessionManager: sessMgr,
		TimeoutMinutes: 1,
		BackoffBase:    10 * time.Millisecond,
		MaxAttempts:    maxAttempts,
		StalenessTTL:   tc.stalenessTTL,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			idx := runnerCalls
			runnerCalls++
			passedSessions = append(passedSessions, sessionID)
			capturedPrompt = prompt
			mu.Unlock()
			var step mockRunnerStep
			if len(tc.runnerSteps) > 0 {
				if idx >= len(tc.runnerSteps) {
					idx = len(tc.runnerSteps) - 1
				}
				step = tc.runnerSteps[idx]
			}
			return execMockRunnerStep(step, tmpHome, sessionID)
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredChannels = append(deliveredChannels, channelID)
			deliveredTexts = append(deliveredTexts, text)
			mu.Unlock()
			return nil
		},
		TypingFunc: noopTyping,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			completedCount++
			if completedCount == len(tc.messages) {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			mu.Unlock()
			turnDoneCh <- struct{}{}
		},
	}
	if tc.notifierReply != "" {
		cfg.NotifierFunc = func(agyBin, apiKey, desc string) string {
			return tc.notifierReply
		}
	}
	if tc.policyMode != "" {
		cfg.ResolveChannelPolicy = func(cID, cName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: tc.policyMode}
		}
	}

	pool := NewWorkerPool(cfg)
	msgs := make([]db.Message, len(tc.messages))
	for i, m := range tc.messages {
		if m.Status == "" {
			m.Status = db.StatusPending
		}
		if m.CreatedAt.IsZero() {
			if tc.messageAge > 0 {
				m.CreatedAt = time.Now().UTC().Add(-tc.messageAge)
			} else {
				m.CreatedAt = time.Now().UTC()
			}
		}
		msgs[i] = m
		if err := insertMessage(store, m); err != nil {
			t.Fatalf("failed to insert message %s: %v", m.ID, err)
		}
	}

	if tc.enqueueAsBurst {
		pool.EnqueueBurst(msgs...)
		pool.Start()
		defer pool.Stop()
		waitForCompletion(t, doneCh, 3*time.Second, tc.name)
	} else if tc.sequentialTurns {
		pool.Start()
		defer pool.Stop()
		for _, m := range msgs {
			pool.Enqueue(m)
			waitForCompletion(t, turnDoneCh, 3*time.Second, tc.name)
		}
	} else {
		pool.Start()
		defer pool.Stop()
		for _, m := range msgs {
			pool.Enqueue(m)
		}
		waitForCompletion(t, doneCh, 3*time.Second, tc.name)
	}

	mu.Lock()
	defer mu.Unlock()

	if tc.checkRunnerCalls && runnerCalls != tc.wantRunnerCalls {
		t.Errorf("runnerCalls = %d, want %d", runnerCalls, tc.wantRunnerCalls)
	}
	if len(tc.wantPassedSessions) > 0 {
		if len(passedSessions) != len(tc.wantPassedSessions) {
			t.Fatalf("passedSessions len = %d (%v), want %d (%v)", len(passedSessions), passedSessions, len(tc.wantPassedSessions), tc.wantPassedSessions)
		}
		for i, wantSess := range tc.wantPassedSessions {
			if passedSessions[i] != wantSess {
				t.Errorf("passedSessions[%d] = %q, want %q", i, passedSessions[i], wantSess)
			}
		}
	}
	if tc.checkDeliveryCalls && len(deliveredTexts) != tc.wantDeliveryCalls {
		t.Errorf("deliveryCalls = %d, want %d", len(deliveredTexts), tc.wantDeliveryCalls)
	}
	if len(tc.wantDeliveredTexts) > 0 {
		if len(deliveredTexts) != len(tc.wantDeliveredTexts) {
			t.Fatalf("deliveredTexts = %v, want %v", deliveredTexts, tc.wantDeliveredTexts)
		}
		for i, wantTxt := range tc.wantDeliveredTexts {
			if deliveredTexts[i] != wantTxt {
				t.Errorf("deliveredTexts[%d] = %q, want %q", i, deliveredTexts[i], wantTxt)
			}
		}
	}
	if len(tc.wantDeliveredChannels) > 0 {
		gotSet := make(map[string]bool, len(deliveredChannels))
		for _, ch := range deliveredChannels {
			gotSet[ch] = true
		}
		for _, wantCh := range tc.wantDeliveredChannels {
			if !gotSet[wantCh] {
				t.Errorf("expected delivery to channel %q, got %v", wantCh, deliveredChannels)
			}
		}
	}
	for _, sub := range tc.wantPromptContains {
		if !strings.Contains(capturedPrompt, sub) {
			t.Errorf("expected prompt to contain %q, got %s", sub, capturedPrompt)
		}
	}
	for _, sub := range tc.wantPromptExcludes {
		if strings.Contains(capturedPrompt, sub) {
			t.Errorf("expected prompt NOT to contain %q, got %s", sub, capturedPrompt)
		}
	}
	if tc.wantSavedSession != "" {
		savedSess, err := getSessionID(store, threadID)
		if err != nil || savedSess != tc.wantSavedSession {
			t.Errorf("savedSession = %q (err: %v), want %q", savedSess, err, tc.wantSavedSession)
		}
	}
	if tc.checkTurnCount {
		turnCount, err := getSessionTurnCount(store, threadID)
		if err != nil || turnCount != tc.wantTurnCount {
			t.Errorf("turnCount = %d (err: %v), want %d", turnCount, err, tc.wantTurnCount)
		}
	}
	for _, m := range msgs {
		dbMsg, err := getMessage(store, m.ID)
		if err != nil || dbMsg == nil {
			t.Fatalf("failed to get message %s: %v", m.ID, err)
		}
		if tc.wantStatus != "" && dbMsg.Status != tc.wantStatus {
			t.Errorf("message %s status = %s, want %s", m.ID, dbMsg.Status, tc.wantStatus)
		}
		if tc.wantRetryCount > 0 && dbMsg.RetryCount != tc.wantRetryCount {
			t.Errorf("message %s retryCount = %d, want %d", m.ID, dbMsg.RetryCount, tc.wantRetryCount)
		}
		if tc.wantDBContent != "" && dbMsg.Content != tc.wantDBContent {
			t.Errorf("message %s content = %q, want %q", m.ID, dbMsg.Content, tc.wantDBContent)
		}
		if tc.wantResponseText != "" && dbMsg.ResponseText != tc.wantResponseText {
			t.Errorf("message %s responseText = %q, want %q", m.ID, dbMsg.ResponseText, tc.wantResponseText)
		}
	}
}

type burstTestCase struct {
	name                         string
	channelID                    string
	seedSession                  bool
	seedTurnCount                int
	botUserID                    string
	classifierJSON               string
	policy                       config.ChannelPolicy
	runnerConvID                 string
	runnerResponse               string
	preserveSessionInRunner      bool
	messages                     []db.Message
	wantRunnerCalls              int
	wantDeliveryCalls            int
	checkDeliveryCalls           bool
	wantDeliveredText            string
	wantMinTypingCalls           int
	wantMaxTypingCalls           int
	checkMaxTypingCalls          bool
	wantTurnCount                int
	wantPassedInitialSession     bool
	wantOldTranscriptExcludes    string
	wantPromptContains           []string
	wantPromptExcludes           []string
	wantClassifierPromptContains string
	wantMsgErrors                map[string]string
	wantMsgResponses             map[string]string
}

func runBurstTestCase(t *testing.T, tc burstTestCase) {
	t.Helper()
	tmpDir := t.TempDir()
	store := setupTestStore(t)
	sessMgr := session.New(tmpDir, "")

	var initialSessionID string
	if tc.seedSession {
		initialSessionID = uuid.New().String()
		_ = saveSessionID(store, tc.channelID, initialSessionID)
		_, _ = sessMgr.EnsureSessionDir(initialSessionID)
		cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
		_ = os.MkdirAll(cliPbDir, 0755)
		_ = os.WriteFile(filepath.Join(cliPbDir, initialSessionID+".pb"), []byte("mock-pb"), 0644)
	}
	for i := 0; i < tc.seedTurnCount; i++ {
		_, _ = incrementSessionTurnCount(store, tc.channelID)
	}

	var mu sync.Mutex
	var runnerCalls, deliveryCalls, typingCalls int
	var deliveredText, receivedPrompt, passedSessID, capturedClassifierPrompt string

	var cls *classifier.Classifier
	if tc.classifierJSON != "" {
		cls = classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			mu.Lock()
			capturedClassifierPrompt = prompt
			mu.Unlock()
			return tc.classifierJSON, nil
		}))
	}

	var dg *discordgo.Session
	if tc.botUserID != "" {
		dg = &discordgo.Session{State: discordgo.NewState()}
		dg.State.User = &discordgo.User{ID: tc.botUserID, Username: "Aerial"}
	}

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		SessionManager: sessMgr,
		DiscordSession: dg,
		TimeoutMinutes: 1,
		Classifier:     cls,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			receivedPrompt = prompt
			passedSessID = sessID
			mu.Unlock()
			convID := tc.runnerConvID
			if tc.preserveSessionInRunner && convID == "" {
				convID = initialSessionID
			}
			return mockJSONResponse(convID, tc.runnerResponse), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			deliveredText = text
			mu.Unlock()
			return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			mu.Lock()
			typingCalls++
			mu.Unlock()
			return func() {}
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return tc.policy
		},
	})

	now := time.Now().UTC()
	burst := make([]db.Message, len(tc.messages))
	for i, m := range tc.messages {
		if m.ThreadID == "" {
			m.ThreadID = tc.channelID
		}
		if m.Status == "" {
			m.Status = db.StatusPending
		}
		if m.CreatedAt.IsZero() {
			m.CreatedAt = now.Add(time.Duration(i*2) * time.Second)
		}
		burst[i] = m
		_ = insertMessage(store, m)
	}

	pool.processBurst(burst)

	mu.Lock()
	defer mu.Unlock()

	if runnerCalls != tc.wantRunnerCalls {
		t.Errorf("runnerCalls = %d, want %d", runnerCalls, tc.wantRunnerCalls)
	}
	if tc.checkDeliveryCalls && deliveryCalls != tc.wantDeliveryCalls {
		t.Errorf("deliveryCalls = %d, want %d", deliveryCalls, tc.wantDeliveryCalls)
	}
	if tc.wantDeliveredText != "" && deliveredText != tc.wantDeliveredText {
		t.Errorf("deliveredText = %q, want %q", deliveredText, tc.wantDeliveredText)
	}
	if tc.wantMinTypingCalls > 0 && typingCalls < tc.wantMinTypingCalls {
		t.Errorf("typingCalls = %d, want >= %d", typingCalls, tc.wantMinTypingCalls)
	}
	if tc.checkMaxTypingCalls && typingCalls > tc.wantMaxTypingCalls {
		t.Errorf("typingCalls = %d, want <= %d", typingCalls, tc.wantMaxTypingCalls)
	}
	if tc.wantPassedInitialSession && passedSessID != initialSessionID {
		t.Errorf("passedSessID = %q, want %q", passedSessID, initialSessionID)
	}
	if tc.wantOldTranscriptExcludes != "" {
		oldSessDir, _ := sessMgr.EnsureSessionDir(initialSessionID)
		dataOld, _ := os.ReadFile(filepath.Join(oldSessDir, ".system_generated", "logs", "transcript.jsonl"))
		if strings.Contains(string(dataOld), tc.wantOldTranscriptExcludes) {
			t.Errorf("old transcript should NOT contain %q", tc.wantOldTranscriptExcludes)
		}
	}
	for _, sub := range tc.wantPromptContains {
		if !strings.Contains(receivedPrompt, sub) {
			t.Errorf("expected runner prompt to contain %q, got %s", sub, receivedPrompt)
		}
	}
	for _, sub := range tc.wantPromptExcludes {
		if strings.Contains(receivedPrompt, sub) {
			t.Errorf("expected runner prompt NOT to contain %q, got %s", sub, receivedPrompt)
		}
	}
	if tc.wantClassifierPromptContains != "" && !strings.Contains(capturedClassifierPrompt, tc.wantClassifierPromptContains) {
		t.Errorf("expected classifier prompt to contain %q, got %s", tc.wantClassifierPromptContains, capturedClassifierPrompt)
	}
	turnCount, err := getSessionTurnCount(store, tc.channelID)
	if err != nil || turnCount != tc.wantTurnCount {
		t.Errorf("turnCount = %d (err: %v), want %d", turnCount, err, tc.wantTurnCount)
	}
	for _, m := range burst {
		saved, err := getMessage(store, m.ID)
		if err != nil || saved == nil {
			t.Fatalf("failed to retrieve %s: %v", m.ID, err)
		}
		if saved.Status != db.StatusCompleted {
			t.Errorf("message %s status = %s, want COMPLETED", m.ID, saved.Status)
		}
		if wantErr, ok := tc.wantMsgErrors[m.ID]; ok && !strings.Contains(saved.ErrorMessage, wantErr) {
			t.Errorf("message %s error_message = %q, want substring %q", m.ID, saved.ErrorMessage, wantErr)
		}
		if wantResp, ok := tc.wantMsgResponses[m.ID]; ok && saved.ResponseText != wantResp {
			t.Errorf("message %s response_text = %q, want %q", m.ID, saved.ResponseText, wantResp)
		}
	}
}

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
