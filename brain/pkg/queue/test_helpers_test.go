package queue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/session"
)

// setupTestStore returns a clean FakeStore for testing.
func setupTestStore(t testing.TB) *db.FakeStore { return db.NewFakeStore() }

func insertMessage(store db.Store, msg db.Message) error {
	if store == nil { return nil }
	return store.InsertMessage(context.Background(), msg)
}

func getMessage(store db.Store, id string) (*db.Message, error) {
	if store == nil { return nil, nil }
	return store.GetMessage(context.Background(), id)
}

func saveSessionID(store db.Store, threadID, sessionID string) error {
	if store == nil { return nil }
	return store.SaveSessionID(context.Background(), threadID, sessionID)
}

func getSessionID(store db.Store, threadID string) (string, error) {
	if store == nil { return "", nil }
	return store.GetSessionID(context.Background(), threadID)
}

func getPreviousSessionID(store db.Store, threadID string) (string, error) {
	if store == nil { return "", nil }
	return store.GetPreviousSessionID(context.Background(), threadID)
}

func createScheduleRun(store db.Store, run db.ScheduleRun) error {
	if store == nil { return nil }
	return store.CreateScheduleRun(context.Background(), run)
}

func getScheduleRun(store db.Store, id string) (*db.ScheduleRun, error) {
	if store == nil { return nil, nil }
	if fake, ok := store.(*db.FakeStore); ok { return fake.GetScheduleRun(id) }
	runs, _, err := store.GetScheduleRunsPaginated(context.Background(), 100, 0, "", "")
	if err != nil { return nil, err }
	for _, r := range runs {
		if r.ID == id { return &r, nil }
	}
	return nil, nil
}

func getSessionTurnCount(store db.Store, sessionKey string) (int, error) {
	if store == nil { return 0, nil }
	return store.GetSessionTurnCount(context.Background(), sessionKey)
}

func incrementSessionTurnCount(store db.Store, sessionKey string) (int, error) {
	if store == nil { return 0, nil }
	return store.IncrementSessionTurnCount(context.Background(), sessionKey)
}

func getScheduleRunsPaginated(store db.Store, limit, offset int, scheduleID, status string) ([]db.ScheduleRun, int, error) {
	if store == nil { return nil, 0, nil }
	return store.GetScheduleRunsPaginated(context.Background(), limit, offset, scheduleID, status)
}

func getThreadSummary(store db.Store, threadID string) (string, string, error) {
	if store == nil { return "", "", nil }
	return store.GetThreadSummary(context.Background(), threadID)
}

func saveThreadSummary(store db.Store, threadID, summary, lastMsgID string) error {
	if store == nil { return nil }
	return store.SaveThreadSummary(context.Background(), threadID, summary, lastMsgID)
}

func rotateSessionID(store db.Store, sessionKey, newSessionID string) error {
	if store == nil { return nil }
	return store.RotateSessionID(context.Background(), sessionKey, newSessionID)
}

func noopDelivery(s *discordgo.Session, channelID, text string) error { return nil }
func noopTyping(s *discordgo.Session, channelID string) func()         { return func() {} }

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
	go func() { pool.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for worker pool goroutines to exit")
	}
}

func mockJSONResponseWithUsage(convID, responseText string, usage runner.TokenUsage) string {
	if convID == "" { convID = uuid.New().String() }
	payload, _ := json.Marshal(map[string]interface{}{
		"conversation_id": convID, "status": "SUCCESS", "response": responseText,
		"duration_seconds": 1.0, "num_turns": 1, "usage": usage,
	})
	return string(payload)
}

func mockMultiTurnJSONResponse(convID, responseText string, numTurns int) string {
	if convID == "" { convID = uuid.New().String() }
	payload, _ := json.Marshal(map[string]interface{}{
		"conversation_id": convID, "status": "SUCCESS", "response": responseText,
		"duration_seconds": 1.0, "num_turns": numTurns,
	})
	return string(payload)
}

type runnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)
type runnerWithOptionsFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (stdout, stderr string, exitCode int, err error)

type mockRunnerStep struct {
	convID          string
	response        string
	rawStdout       string
	returnRawStdout bool
	stderr          string
	exitCode        int
	errMsg          string
	panicMsg        string
	writeTranscript bool
}

func execMockRunnerStep(step mockRunnerStep, tmpHome, sessionID string) (string, string, int, error) {
	if step.panicMsg != "" { panic(step.panicMsg) }
	convID := step.convID
	if convID == "" && sessionID != "" { convID = sessionID }
	if step.writeTranscript && tmpHome != "" {
		if convID == "" { convID = uuid.New().String() }
		sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", convID)
		_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
		cliPbDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "conversations")
		_ = os.MkdirAll(cliPbDir, 0755)
		_ = os.WriteFile(filepath.Join(cliPbDir, convID+".pb"), []byte("mock-pb"), 0644)
		now := time.Now()
		_ = os.Chtimes(sessDir, now, now)
	}
	if step.errMsg != "" { return step.rawStdout, step.stderr, step.exitCode, errors.New(step.errMsg) }
	if step.returnRawStdout || step.rawStdout != "" { return step.rawStdout, step.stderr, step.exitCode, nil }
	stderr := step.stderr
	if stderr == "" && convID != "" { stderr = fmt.Sprintf("Starting conversation update stream for %s\n", convID) }
	return mockJSONResponse(convID, step.response), stderr, step.exitCode, nil
}

func assertContainsAll(t *testing.T, target string, subs []string, label string) {
	t.Helper()
	for _, s := range subs {
		if !strings.Contains(target, s) {
			t.Errorf("expected %s to contain %q, got: %s", label, s, target)
		}
	}
}

func assertExcludesAll(t *testing.T, target string, subs []string, label string) {
	t.Helper()
	for _, s := range subs {
		if strings.Contains(target, s) {
			t.Errorf("expected %s NOT to contain %q, got: %s", label, s, target)
		}
	}
}

type workerTestCase struct {
	name, initialSessionID, policyMode, notifierReply, wantStatus, wantSavedSession, wantErrorContains, wantDBContent, wantResponseText string
	useSessionMgr, enqueueAsBurst, sequentialTurns, checkRunnerCalls, checkTurnCount, checkDeliveryCalls                                  bool
	seedTurnCount, maxAttempts, concurrentEnqueueCount, wantDefaultTimeout, wantRunnerCalls, wantRetryCount, wantTurnCount, wantDeliveryCalls int
	seedIdleAge, idleTimeout, stalenessTTL, messageAge                                                                                    time.Duration
	seedTranscriptFiles                                                                                                                   map[string]string
	seedPNGImage                                                                                                                          image.Image
	seedPNGLevel                                                                                                                          png.CompressionLevel
	seedPNGFilename                                                                                                                       string
	messages                                                                                                                              []db.Message
	runnerSteps                                                                                                                           []mockRunnerStep
	wantPassedSessions, wantDeliveredTexts, wantDeliveredSubstrings, wantDeliveredExcludes, wantDeliveredChannels                        []string
	wantAttachments                                                                                                                       []*delivery.Attachment
	wantPromptContains, wantPromptExcludes                                                                                                []string
	wantAttemptPromptContains, wantAttemptPromptExcludes                                                                                  map[int][]string
}

func runWorkerTestCase(t *testing.T, tc workerTestCase) {
	t.Helper()
	if tc.wantDefaultTimeout > 0 && len(tc.messages) == 0 && tc.concurrentEnqueueCount == 0 {
		if DefaultTimeoutMinutes != tc.wantDefaultTimeout {
			t.Errorf("DefaultTimeoutMinutes = %d, want %d", DefaultTimeoutMinutes, tc.wantDefaultTimeout)
		}
		return
	}

	store := setupTestStore(t)
	var tmpHome string
	var sessMgr *session.Manager
	if tc.useSessionMgr || tc.seedTurnCount > 0 || tc.seedIdleAge > 0 || len(tc.seedTranscriptFiles) > 0 {
		tmpHome = t.TempDir()
		sessMgr = session.New(tmpHome, "")
	}

	var threadID string
	if len(tc.messages) > 0 {
		threadID = tc.messages[0].ThreadID
	}
	if tc.initialSessionID != "" {
		_ = saveSessionID(store, threadID, tc.initialSessionID)
		if sessMgr != nil && len(tc.seedTranscriptFiles) == 0 {
			sessDir, _ := sessMgr.EnsureSessionDir(tc.initialSessionID)
			cliPbDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "conversations")
			_ = os.MkdirAll(cliPbDir, 0755)
			pbFile := filepath.Join(cliPbDir, tc.initialSessionID+".pb")
			_ = os.WriteFile(pbFile, []byte("mock-pb"), 0644)
			if tc.seedIdleAge > 0 {
				idleTime := time.Now().UTC().Add(-tc.seedIdleAge)
				store.SetSessionInfo(db.SessionInfo{
					ThreadID: threadID, InternalSessionID: tc.initialSessionID,
					TurnCount: tc.seedTurnCount, CreatedAt: idleTime, UpdatedAt: idleTime,
				})
				_ = os.Chtimes(pbFile, idleTime, idleTime)
				_ = os.Chtimes(cliPbDir, idleTime, idleTime)
				_ = os.Chtimes(sessDir, idleTime, idleTime)
			}
		}
	}
	tNow := time.Now().UTC()
	for sessID, content := range tc.seedTranscriptFiles {
		sessDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
		_ = os.MkdirAll(sessDir, 0755)
		_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(strings.NewReplacer(
			"{{TS0}}", tNow.Format(time.RFC3339Nano),
			"{{TS1}}", tNow.Add(time.Second).Format(time.RFC3339Nano),
			"{{TS2}}", tNow.Add(2*time.Second).Format(time.RFC3339Nano),
		).Replace(content)), 0600)
	}

	steps := append([]mockRunnerStep(nil), tc.runnerSteps...)
	wantDeliveredExcludes := append([]string(nil), tc.wantDeliveredExcludes...)
	if tc.seedPNGFilename != "" && tc.seedPNGImage != nil {
		var imgBuf bytes.Buffer
		_ = (&png.Encoder{CompressionLevel: tc.seedPNGLevel}).Encode(&imgBuf, tc.seedPNGImage)
		testImgPath := filepath.Join(t.TempDir(), tc.seedPNGFilename)
		_ = os.WriteFile(testImgPath, imgBuf.Bytes(), 0600)
		for i := range steps {
			steps[i].response = strings.ReplaceAll(steps[i].response, "{{IMAGE_PATH}}", testImgPath)
		}
		wantDeliveredExcludes = append(wantDeliveredExcludes, testImgPath)
	}

	if tc.seedTurnCount > 0 && tc.seedIdleAge == 0 {
		for i := 0; i < tc.seedTurnCount; i++ {
			_, _ = incrementSessionTurnCount(store, threadID)
		}
	}

	maxAttempts := tc.maxAttempts
	if maxAttempts == 0 { maxAttempts = 1 }
	expectedCompletions := len(tc.messages)
	if tc.concurrentEnqueueCount > 0 { expectedCompletions = tc.concurrentEnqueueCount }

	var mu sync.Mutex
	var runnerCalls int
	var passedSessions, promptsSeen, deliveredTexts, deliveredChannels []string
	var capturedPrompt string
	var deliveredAttachments []*delivery.Attachment
	var completedCount atomic.Int32
	doneCh := make(chan struct{})
	turnDoneCh := make(chan struct{}, expectedCompletions+1)

	cfg := WorkerPoolConfig{
		Store: store, SessionManager: sessMgr, TimeoutMinutes: 1, IdleTimeout: tc.idleTimeout,
		BackoffBase: 5 * time.Millisecond, MaxAttempts: maxAttempts, StalenessTTL: tc.stalenessTTL,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			idx := runnerCalls
			runnerCalls++
			passedSessions = append(passedSessions, sessionID)
			promptsSeen = append(promptsSeen, prompt)
			capturedPrompt = prompt
			mu.Unlock()
			var step mockRunnerStep
			if len(steps) > 0 {
				if idx >= len(steps) { idx = len(steps) - 1 }
				step = steps[idx]
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
			if int(completedCount.Add(1)) == expectedCompletions {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			turnDoneCh <- struct{}{}
		},
	}
	if len(tc.wantAttachments) > 0 {
		cfg.DeliveryWithAttachmentsFunc = func(s *discordgo.Session, channelID, text string, attachments []*delivery.Attachment) error {
			mu.Lock()
			deliveredChannels = append(deliveredChannels, channelID)
			deliveredTexts = append(deliveredTexts, text)
			deliveredAttachments = attachments
			mu.Unlock()
			return nil
		}
	}
	if tc.notifierReply != "" {
		cfg.NotifierFunc = func(agyBin, apiKey, desc string) string { return tc.notifierReply }
	}
	if tc.policyMode != "" {
		cfg.ResolveChannelPolicy = func(cID, cName string) config.ChannelPolicy { return config.ChannelPolicy{Mode: tc.policyMode} }
	}

	pool := NewWorkerPool(cfg)
	if tc.concurrentEnqueueCount > 0 {
		pool.Start()
		var wg sync.WaitGroup
		for i := 0; i < tc.concurrentEnqueueCount; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				m := db.Message{
					ID: fmt.Sprintf("msg-race-%d", idx), ThreadID: "thread-evict-race",
					GuildID: "guild-race", AuthorID: "user-race", AuthorName: "User",
					Content: fmt.Sprintf("msg content %d", idx), Status: db.StatusPending,
					CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
				}
				_ = insertMessage(store, m)
				pool.Enqueue(m)
			}(i)
		}
		wg.Wait()
		waitForCompletion(t, doneCh, 5*time.Second, tc.name)
		pool.StopWithTimeout(2 * time.Second)
		return
	}

	msgs := make([]db.Message, len(tc.messages))
	for i, m := range tc.messages {
		if m.Status == "" { m.Status = db.StatusPending }
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
		waitForCompletion(t, doneCh, 5*time.Second, tc.name)
	}

	mu.Lock()
	defer mu.Unlock()

	if tc.checkRunnerCalls && runnerCalls != tc.wantRunnerCalls {
		t.Errorf("runnerCalls = %d, want %d", runnerCalls, tc.wantRunnerCalls)
	}
	if len(tc.wantPassedSessions) > 0 && !reflect.DeepEqual(passedSessions, tc.wantPassedSessions) {
		t.Errorf("passedSessions = %v, want %v", passedSessions, tc.wantPassedSessions)
	}
	if tc.checkDeliveryCalls && len(deliveredTexts) != tc.wantDeliveryCalls {
		t.Errorf("deliveryCalls = %d, want %d", len(deliveredTexts), tc.wantDeliveryCalls)
	}
	if len(tc.wantDeliveredTexts) > 0 && !reflect.DeepEqual(deliveredTexts, tc.wantDeliveredTexts) {
		t.Errorf("deliveredTexts = %v, want %v", deliveredTexts, tc.wantDeliveredTexts)
	}
	joinedDelivered := strings.Join(deliveredTexts, "\n")
	assertContainsAll(t, joinedDelivered, tc.wantDeliveredSubstrings, "delivered text")
	assertExcludesAll(t, joinedDelivered, wantDeliveredExcludes, "delivered text")

	if len(tc.wantAttachments) > 0 {
		if len(deliveredAttachments) != len(tc.wantAttachments) {
			t.Fatalf("deliveredAttachments len = %d, want %d", len(deliveredAttachments), len(tc.wantAttachments))
		}
		for i, wantAtt := range tc.wantAttachments {
			if deliveredAttachments[i].Filename != wantAtt.Filename {
				t.Errorf("attachment[%d] filename = %q, want %q", i, deliveredAttachments[i].Filename, wantAtt.Filename)
			}
		}
	}
	for _, wantCh := range tc.wantDeliveredChannels {
		found := false
		for _, ch := range deliveredChannels {
			if ch == wantCh { found = true; break }
		}
		if !found {
			t.Errorf("expected delivery to channel %q, got %v", wantCh, deliveredChannels)
		}
	}
	assertContainsAll(t, capturedPrompt, tc.wantPromptContains, "prompt")
	assertExcludesAll(t, capturedPrompt, tc.wantPromptExcludes, "prompt")

	for attemptIdx, subs := range tc.wantAttemptPromptContains {
		if attemptIdx >= len(promptsSeen) {
			t.Fatalf("expected attempt %d prompt, only saw %d attempts", attemptIdx, len(promptsSeen))
		}
		assertContainsAll(t, promptsSeen[attemptIdx], subs, fmt.Sprintf("attempt %d prompt", attemptIdx))
	}
	for attemptIdx, subs := range tc.wantAttemptPromptExcludes {
		if attemptIdx >= len(promptsSeen) {
			t.Fatalf("expected attempt %d prompt, only saw %d attempts", attemptIdx, len(promptsSeen))
		}
		assertExcludesAll(t, promptsSeen[attemptIdx], subs, fmt.Sprintf("attempt %d prompt", attemptIdx))
	}
	if tc.wantSavedSession != "" {
		if savedSess, _ := getSessionID(store, threadID); savedSess != tc.wantSavedSession {
			t.Errorf("savedSession = %q, want %q", savedSess, tc.wantSavedSession)
		}
	}
	if tc.checkTurnCount {
		if turnCount, _ := getSessionTurnCount(store, threadID); turnCount != tc.wantTurnCount {
			t.Errorf("turnCount = %d, want %d", turnCount, tc.wantTurnCount)
		}
	}
	for _, m := range msgs {
		dbMsg, err := getMessage(store, m.ID)
		if err != nil || dbMsg == nil {
			t.Fatalf("failed to get message %s: %v (msg=%v)", m.ID, err, dbMsg)
		}
		if tc.wantStatus != "" && dbMsg.Status != tc.wantStatus {
			t.Errorf("message %s status = %s, want %s", m.ID, dbMsg.Status, tc.wantStatus)
		}
		if tc.wantRetryCount > 0 && dbMsg.RetryCount != tc.wantRetryCount {
			t.Errorf("message %s retryCount = %d, want %d", m.ID, dbMsg.RetryCount, tc.wantRetryCount)
		}
		if tc.wantErrorContains != "" && !strings.Contains(dbMsg.ErrorMessage, tc.wantErrorContains) {
			t.Errorf("message %s errorMessage = %q, want substring %q", m.ID, dbMsg.ErrorMessage, tc.wantErrorContains)
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
	name, channelID, seedSessionID, botUserID, classifierJSON, runnerConvID, runnerResponse, wantSavedSession, wantFinalStatus, wantDeliveredText, wantOldTranscriptExcludes, wantPromptPrefix, wantClassifierPromptContains, wantAlertTitle, wantAlertChannel string
	seedSession, enableHistoryFetcher, sequentialBursts, preserveSessionInRunner, checkClassifierCalls, checkSavedSession, wantNoStubDir, checkDeliveryCalls, checkMaxTypingCalls, wantPassedInitialSession, wantSingleInstructionTagPerPrompt                      bool
	seedTurnCount, maxAttempts, rotateBeforeTurn, wantClassifierCalls, wantRunnerCalls, wantDeliveryCalls, wantMinTypingCalls, wantMaxTypingCalls, wantTurnCount                                                                                                        int
	seedTranscripts, wantPassedSessions, wantDeliveredSubstrings, wantDeliveredExcludes, wantPromptContains, wantPromptExcludes, wantAlertBodyContains                                                                                                                  []string
	channelInstructions, wantMsgErrors, wantMsgResponses                                                                                                                                                                                                                 map[string]string
	cachedChannels                                                                                                                                                                                                                                                       []*discordgo.Channel
	historyMessages                                                                                                                                                                                                                                                      []HistoryMessage
	policy                                                                                                                                                                                                                                                               config.ChannelPolicy
	runnerSteps                                                                                                                                                                                                                                                          []mockRunnerStep
	messages                                                                                                                                                                                                                                                             []db.Message
	wantTurnPromptContains, wantTurnPromptExcludes                                                                                                                                                                                                                       map[int][]string
}

func runBurstTestCase(t *testing.T, tc burstTestCase) {
	t.Helper()
	tmpDir := t.TempDir()
	store := setupTestStore(t)
	sessMgr := session.New(tmpDir, "")

	if len(tc.channelInstructions) > 0 {
		channelsDir := filepath.Join(tmpDir, "channels")
		_ = os.MkdirAll(channelsDir, 0755)
		for fname, content := range tc.channelInstructions {
			_ = os.WriteFile(filepath.Join(channelsDir, fname), []byte(content), 0644)
		}
		oldDirs := config.ChannelInstructionsDirs
		config.ChannelInstructionsDirs = []string{channelsDir}
		defer func() { config.ChannelInstructionsDirs = oldDirs }()
	}

	for _, ch := range tc.cachedChannels {
		CacheDiscordChannel(ch)
		chID := ch.ID
		defer InvalidateChannelCache(chID)
	}

	var initialSessionID string
	if tc.seedSession {
		initialSessionID = uuid.New().String()
		_ = saveSessionID(store, tc.channelID, initialSessionID)
		_, _ = sessMgr.EnsureSessionDir(initialSessionID)
		cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
		_ = os.MkdirAll(cliPbDir, 0755)
		_ = os.WriteFile(filepath.Join(cliPbDir, initialSessionID+".pb"), []byte("mock-pb"), 0644)
	}
	if tc.seedSessionID != "" {
		initialSessionID = tc.seedSessionID
		_ = saveSessionID(store, tc.channelID, tc.seedSessionID)
	}
	for _, sessID := range tc.seedTranscripts {
		sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID)
		_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0755)
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644)
		past := time.Now().Add(-10 * time.Second)
		_ = os.Chtimes(sessDir, past, past)
	}
	for i := 0; i < tc.seedTurnCount; i++ {
		_, _ = incrementSessionTurnCount(store, tc.channelID)
	}

	var mu sync.Mutex
	var classifierCalls, runnerCalls, deliveryCalls, typingCalls int
	var deliveredText, receivedPrompt, passedSessID, capturedClassifierPrompt string
	var capturedPrompts, passedSessions []string
	var alertChannel, alertTitle, alertBody string
	alertReceived := make(chan struct{}, 1)

	var cls *classifier.Classifier
	if tc.classifierJSON != "" {
		cls = classifier.NewClassifier(classifier.WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			mu.Lock()
			classifierCalls++
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

	cfg := WorkerPoolConfig{
		Store: store, SessionManager: sessMgr, DiscordSession: dg, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
		MaxAttempts: tc.maxAttempts, Classifier: cls,
		SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, body string) error {
			mu.Lock()
			alertChannel = channelNameOrID; alertTitle = title; alertBody = body
			mu.Unlock()
			select {
			case alertReceived <- struct{}{}:
			default:
			}
			return nil
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			idx := runnerCalls
			runnerCalls++
			receivedPrompt = prompt
			capturedPrompts = append(capturedPrompts, prompt)
			passedSessID = sessID
			passedSessions = append(passedSessions, sessID)
			mu.Unlock()
			if len(tc.runnerSteps) > 0 {
				if idx >= len(tc.runnerSteps) { idx = len(tc.runnerSteps) - 1 }
				return execMockRunnerStep(tc.runnerSteps[idx], tmpDir, sessID)
			}
			convID := tc.runnerConvID
			if tc.preserveSessionInRunner && convID == "" { convID = initialSessionID }
			return mockJSONResponse(convID, tc.runnerResponse), "", 0, nil
		},
		DeliveryFunc: func(sess *discordgo.Session, channelID, text string) error {
			mu.Lock(); deliveryCalls++; deliveredText = text; mu.Unlock(); return nil
		},
		TypingFunc: func(sess *discordgo.Session, channelID string) func() {
			mu.Lock(); typingCalls++; mu.Unlock(); return func() {}
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy { return tc.policy },
	}
	if tc.enableHistoryFetcher || len(tc.historyMessages) > 0 {
		cfg.HistoryFetcher = func(ctx context.Context, cID, beforeID string, limit int) ([]HistoryMessage, error) {
			return tc.historyMessages, nil
		}
	}

	pool := NewWorkerPool(cfg)
	if tc.wantAlertTitle != "" { pool.Start(); defer pool.Stop() }

	now := time.Now().UTC()
	burst := make([]db.Message, len(tc.messages))
	for i, m := range tc.messages {
		if m.ThreadID == "" { m.ThreadID = tc.channelID }
		if m.Status == "" { m.Status = db.StatusPending }
		if m.CreatedAt.IsZero() { m.CreatedAt = now.Add(time.Duration(i*2) * time.Second) }
		burst[i] = m
	}

	for i, m := range burst {
		if tc.sequentialBursts && tc.rotateBeforeTurn > 0 && i+1 == tc.rotateBeforeTurn {
			_ = store.RotateSessionID(context.Background(), tc.channelID, "")
		}
		_ = insertMessage(store, m)
		if tc.sequentialBursts {
			pool.processBurst([]db.Message{m})
		}
	}
	if !tc.sequentialBursts {
		pool.processBurst(burst)
	}

	if tc.wantAlertTitle != "" {
		select {
		case <-alertReceived:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for system alert")
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if tc.wantAlertTitle != "" {
		if alertTitle != tc.wantAlertTitle { t.Errorf("alertTitle = %q, want %q", alertTitle, tc.wantAlertTitle) }
		if tc.wantAlertChannel != "" && alertChannel != tc.wantAlertChannel { t.Errorf("alertChannel = %q, want %q", alertChannel, tc.wantAlertChannel) }
		assertContainsAll(t, alertBody, tc.wantAlertBodyContains, "alertBody")
	}
	if tc.checkClassifierCalls && classifierCalls != tc.wantClassifierCalls {
		t.Errorf("classifierCalls = %d, want %d", classifierCalls, tc.wantClassifierCalls)
	}
	if runnerCalls != tc.wantRunnerCalls {
		t.Errorf("runnerCalls = %d, want %d", runnerCalls, tc.wantRunnerCalls)
	}
	if len(tc.wantPassedSessions) > 0 && !reflect.DeepEqual(passedSessions, tc.wantPassedSessions) {
		t.Errorf("passedSessions = %v, want %v", passedSessions, tc.wantPassedSessions)
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
	if tc.wantPromptPrefix != "" && !strings.HasPrefix(receivedPrompt, tc.wantPromptPrefix) {
		t.Errorf("expected prompt to start with %q, got:\n%s", tc.wantPromptPrefix, receivedPrompt)
	}
	if tc.wantSingleInstructionTagPerPrompt {
		for i, p := range capturedPrompts {
			if c := strings.Count(p, "<CHANNEL_INSTRUCTIONS>"); c != 1 {
				t.Errorf("attempt %d: <CHANNEL_INSTRUCTIONS> count = %d, want 1", i+1, c)
			}
			if c := strings.Count(p, "</CHANNEL_INSTRUCTIONS>"); c != 1 {
				t.Errorf("attempt %d: </CHANNEL_INSTRUCTIONS> count = %d, want 1", i+1, c)
			}
		}
	}
	assertContainsAll(t, receivedPrompt, tc.wantPromptContains, "runner prompt")
	assertExcludesAll(t, receivedPrompt, tc.wantPromptExcludes, "runner prompt")

	for turnIdx, subs := range tc.wantTurnPromptContains {
		if turnIdx >= len(capturedPrompts) {
			t.Fatalf("expected turn %d prompt, only saw %d turns", turnIdx, len(capturedPrompts))
		}
		assertContainsAll(t, capturedPrompts[turnIdx], subs, fmt.Sprintf("turn %d prompt", turnIdx))
	}
	for turnIdx, subs := range tc.wantTurnPromptExcludes {
		if turnIdx >= len(capturedPrompts) {
			t.Fatalf("expected turn %d prompt, only saw %d turns", turnIdx, len(capturedPrompts))
		}
		assertExcludesAll(t, capturedPrompts[turnIdx], subs, fmt.Sprintf("turn %d prompt", turnIdx))
	}
	if tc.wantClassifierPromptContains != "" && !strings.Contains(capturedClassifierPrompt, tc.wantClassifierPromptContains) {
		t.Errorf("expected classifier prompt to contain %q, got %s", tc.wantClassifierPromptContains, capturedClassifierPrompt)
	}
	if tc.checkSavedSession {
		if savedSess, _ := getSessionID(store, tc.channelID); savedSess != tc.wantSavedSession {
			t.Errorf("savedSession = %q, want %q", savedSess, tc.wantSavedSession)
		}
	}
	if tc.wantNoStubDir {
		for _, root := range []string{
			"/data/brain/" + tc.channelID,
			filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", tc.channelID),
			filepath.Join(tmpDir, ".gemini", "antigravity", "brain", tc.channelID),
		} {
			if fi, err := os.Stat(root); err == nil {
				t.Errorf("found unexpected directory at %s (isDir=%t)", root, fi.IsDir())
			}
		}
	}
	if turnCount, _ := getSessionTurnCount(store, tc.channelID); turnCount != tc.wantTurnCount {
		t.Errorf("turnCount = %d, want %d", turnCount, tc.wantTurnCount)
	}
	wantStatus := tc.wantFinalStatus
	if wantStatus == "" { wantStatus = db.StatusCompleted }
	for _, m := range burst {
		saved, err := getMessage(store, m.ID)
		if err != nil || saved == nil {
			t.Fatalf("failed to get message %s: %v (msg=%v)", m.ID, err, saved)
		}
		if saved.Status != wantStatus {
			t.Errorf("message %s status = %s, want %s", m.ID, saved.Status, wantStatus)
		}
		if wantErr, ok := tc.wantMsgErrors[m.ID]; ok && !strings.Contains(saved.ErrorMessage, wantErr) {
			t.Errorf("message %s error_message = %q, want substring %q", m.ID, saved.ErrorMessage, wantErr)
		}
		if wantResp, ok := tc.wantMsgResponses[m.ID]; ok && saved.ResponseText != wantResp {
			t.Errorf("message %s response_text = %q, want %q", m.ID, saved.ResponseText, wantResp)
		}
	}
}

func TestWorkerPool_ScopeKeyAndPreviousSessionFallback(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(WorkerPoolConfig{})
	msg := db.Message{ID: "m1", ThreadID: "t1"}
	if k := pool.ScopeKey(msg); k != "t1" {
		t.Errorf("expected ScopeKey t1, got %s", k)
	}
	if prev, err := getPreviousSessionID(nil, ""); err != nil || prev != "" {
		t.Errorf("expected empty previous session for nil store, got %q (err=%v)", prev, err)
	}
}

// newScriptedDaemonSpawner returns a MockDaemonSpawner that responds with pre-scripted events.
func newScriptedDaemonSpawner(conversationID string, turnResponses ...string) *runner.MockDaemonSpawner {
	return &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()

			convID := conversationID
			if convID == "" { convID = cfg.SessionID }
			if convID == "" { convID = uuid.New().String() }

			go func() {
				defer outW.Close()
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", convID)
				scanner := bufio.NewScanner(inR)
				idx := 0
				for scanner.Scan() {
					if idx < len(turnResponses) {
						_, _ = outW.Write([]byte(turnResponses[idx]))
						idx++
					} else {
						_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"ok\",\"usage\":{\"total_tokens\":5}}}\n"))
					}
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}
}

type errQueueStore struct {
	db.Store
}

func (e *errQueueStore) UpdateMessageStatus(ctx context.Context, id, status, errorMsg string) error {
	return errors.New("errQueueStore: update message status error")
}
func (e *errQueueStore) UpdateScheduleRunStatus(ctx context.Context, params db.UpdateRunParams) error {
	return errors.New("errQueueStore: update schedule run status error")
}
func (e *errQueueStore) UpdateMessageCompleted(ctx context.Context, id, responseText string) error {
	return errors.New("errQueueStore: update message completed error")
}
func (e *errQueueStore) IncrementMessageRetry(ctx context.Context, id, errorMsg string) error {
	return errors.New("errQueueStore: increment message retry error")
}
func (e *errQueueStore) SaveSessionID(ctx context.Context, threadID, sessionID string) error {
	return errors.New("errQueueStore: save session id error")
}
func (e *errQueueStore) RotateSessionID(ctx context.Context, threadID, newSessionID string) error {
	return errors.New("errQueueStore: rotate session id error")
}

type mockDrainErrStore struct {
	db.Store
	messages []db.Message
}

func (m *mockDrainErrStore) GetPendingOrProcessingMessages(ctx context.Context, limit int) ([]db.Message, error) {
	if len(m.messages) > 0 { return m.messages, nil }
	return []db.Message{{ID: "msg-proc-1", Status: db.StatusProcessing}}, nil
}
func (m *mockDrainErrStore) UpdateMessageStatus(ctx context.Context, id, status, errReason string) error {
	return errors.New("mock drain update error")
}

type mockClaimStore struct {
	db.Store
	claimErr error
	claimed  bool
}

func (m *mockClaimStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	return m.claimed, m.claimErr
}
