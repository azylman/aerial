package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestTurnExecution_IsDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want bool
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: false,
		},
		{
			name: "turn without dispatch",
			te:   &turnExecution{threadID: "123"},
			want: false,
		},
		{
			name: "turn with dispatch",
			te:   &turnExecution{threadID: "123", dispatch: &db.Dispatch{ID: "disp-1"}},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.isDispatch(); got != tc.want {
				t.Errorf("isDispatch() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_LeaseKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want string
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: "",
		},
		{
			name: "dispatch turn uses threadID as lease key",
			te:   &turnExecution{threadID: "task-dispatch-1", dispatch: &db.Dispatch{ID: "disp-1"}},
			want: "task-dispatch-1",
		},
		{
			name: "discord turn uses discord prefix",
			te:   &turnExecution{threadID: "123456789"},
			want: "discord-123456789",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.leaseKey(); got != tc.want {
				t.Errorf("leaseKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_TurnEffortAndModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		te         *turnExecution
		wantEffort string
		wantModel  string
	}{
		{
			name:       "nil receiver",
			te:         nil,
			wantEffort: "",
			wantModel:  "",
		},
		{
			name: "dispatch turn with explicit model and effort",
			te: &turnExecution{
				dispatch: &db.Dispatch{ID: "d1", Model: "gemini-custom", Effort: "high", SessionID: "sess-1"},
			},
			wantEffort: "high",
			wantModel:  "gemini-custom",
		},
		{
			name: "dispatch turn ephemeral defaults to medium effort",
			te: &turnExecution{
				isEphemeral: true,
				dispatch:    &db.Dispatch{ID: "d2"},
			},
			wantEffort: "medium",
			wantModel:  "",
		},
		{
			name: "burst turn with effort and model in messages",
			te: &turnExecution{
				burst: []db.Message{
					{ID: "m1", Model: "model-alpha", Effort: "medium"},
				},
			},
			wantEffort: "medium",
			wantModel:  "model-alpha",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotEffort, gotModel := tc.te.turnEffortAndModel()
			if gotEffort != tc.wantEffort || gotModel != tc.wantModel {
				t.Errorf("turnEffortAndModel() = (%q, %q), want (%q, %q)", gotEffort, gotModel, tc.wantEffort, tc.wantModel)
			}
		})
	}
}

type finalizeInterruptedTurnCase struct {
	name          string
	isDispatch    bool
	seedStatus    string
	seedErr       string
	reason        string
	wantRowStatus string
	wantRowErr    string
	wantTurnErr   string
}

func runFinalizeInterruptedTurnCase(t *testing.T, tc finalizeInterruptedTurnCase) {
	t.Helper()
	store := testStore(t)
	ctx := context.Background()
	pool := New(config.NewFromData(&config.ConfigData{}), WorkerPoolConfig{Store: store})
	te := &turnExecution{pool: pool, threadID: "t-int"}
	te.setInterruptReason(tc.reason)
	if tc.isDispatch {
		d := db.Dispatch{ID: "d-int", Status: db.StatusProcessing}
		if _, err := store.InsertDispatch(ctx, d); err != nil {
			t.Fatalf("InsertDispatch: %v", err)
		}
		if err := store.UpdateDispatchStatus(ctx, d.ID, tc.seedStatus, tc.seedErr); err != nil {
			t.Fatalf("UpdateDispatchStatus: %v", err)
		}
		te.dispatch = &d
	} else {
		m := db.Message{ID: "m-int", ThreadID: "t-int", Status: tc.seedStatus}
		if err := store.InsertMessage(ctx, m); err != nil {
			t.Fatalf("InsertMessage: %v", err)
		}
		te.burst = []db.Message{m}
	}
	te.finalizeInterruptedTurn("model-x")
	if te.turnStatus != "cancelled" || te.turnError != tc.wantTurnErr {
		t.Errorf("turn outcome = %q/%q, want cancelled/%q", te.turnStatus, te.turnError, tc.wantTurnErr)
	}
	assertInterruptedTurnRow(ctx, t, store, tc.isDispatch, tc.wantRowStatus, tc.wantRowErr)
}

func assertInterruptedTurnRow(ctx context.Context, t *testing.T, store db.Store, isDispatch bool, wantStatus, wantErr string) {
	t.Helper()
	var gotStatus, gotErr string
	if isDispatch {
		d, err := store.GetDispatch(ctx, "d-int")
		if err != nil {
			t.Fatalf("GetDispatch: %v", err)
		}
		gotStatus, gotErr = d.Status, d.ErrorMessage
	} else {
		m, err := store.GetMessage(ctx, "m-int")
		if err != nil {
			t.Fatalf("GetMessage: %v", err)
		}
		gotStatus, gotErr = m.Status, m.ErrorMessage
	}
	if gotStatus != wantStatus || gotErr != wantErr {
		t.Errorf("row = %q/%q, want %q/%q", gotStatus, gotErr, wantStatus, wantErr)
	}
}

func TestTurnExecution_FinalizeInterruptedTurn(t *testing.T) {
	t.Parallel()
	tests := []finalizeInterruptedTurnCase{
		{name: "burst without reason fails rows as operator interrupt", seedStatus: db.StatusProcessing, wantRowStatus: db.StatusFailed, wantRowErr: "interrupted by operator via slash command", wantTurnErr: "interrupted by operator via slash command"},
		{name: "burst with stop reason fails rows with that reason", seedStatus: db.StatusProcessing, reason: "operator stop", wantRowStatus: db.StatusFailed, wantRowErr: "operator stop", wantTurnErr: "operator stop"},
		{name: "dispatch already failed by cancel keeps its row", isDispatch: true, seedStatus: db.StatusFailed, seedErr: "cancelled by operator", reason: "operator stop", wantRowStatus: db.StatusFailed, wantRowErr: "cancelled by operator", wantTurnErr: "operator stop"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runFinalizeInterruptedTurnCase(t, tc)
		})
	}
}

func TestResolveBurstModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		burst []db.Message
		want  string
	}{
		{
			name:  "empty burst",
			burst: nil,
			want:  "",
		},
		{
			name: "first message with model",
			burst: []db.Message{
				{ID: "1", Model: "gemini-pro"},
				{ID: "2", Model: "gemini-flash"},
			},
			want: "gemini-pro",
		},
		{
			name: "second message has model",
			burst: []db.Message{
				{ID: "1"},
				{ID: "2", Model: " gemini-flash "},
			},
			want: "gemini-flash",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveBurstModel(tc.burst); got != tc.want {
				t.Errorf("resolveBurstModel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_PrepareAttemptPrompt(t *testing.T) {
	t.Parallel()
	sessUUID := "44444444-4444-4444-4444-444444444444"
	origPrompt := "Execute task"
	contPrompt := fmt.Sprintf(ContinuationPromptTemplate, origPrompt)

	tests := []struct {
		name          string
		attempt       int
		promptToSend  string
		lastErrDetail string
		lastStderr    string
		hasSessionID  bool
		hasOnDiskSess bool
		isDispatch    bool
		restartCount  int
		errorMessage  string
		burst         []db.Message
		wantPrompt    string
	}{
		{
			name:         "empty prompt defaults to turn prompt",
			attempt:      1,
			promptToSend: "",
			wantPrompt:   origPrompt,
		},
		{
			name:         "first attempt normal dispatch returns original prompt",
			attempt:      1,
			promptToSend: origPrompt,
			isDispatch:   true,
			wantPrompt:   origPrompt,
		},
		{
			name:          "attempt 2 with context deadline exceeded and on-disk session returns continuation template",
			attempt:       2,
			promptToSend:  origPrompt,
			lastErrDetail: "context deadline exceeded",
			hasSessionID:  true,
			hasOnDiskSess: true,
			wantPrompt:    contPrompt,
		},
		{
			name:          "attempt 2 with daemon turn completed with empty response and on-disk session returns continuation template",
			attempt:       2,
			promptToSend:  origPrompt,
			lastErrDetail: "daemon turn completed with empty response",
			hasSessionID:  true,
			hasOnDiskSess: true,
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted dispatch attempt 1 with on-disk session returns continuation template",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: true,
			isDispatch:    true,
			restartCount:  1,
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted discord burst attempt 1 with on-disk session returns continuation template",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: true,
			isDispatch:    false,
			burst:         []db.Message{{ID: "m1", RestartCount: 1}},
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted dispatch attempt 1 without on-disk session returns original prompt",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: false,
			isDispatch:    true,
			restartCount:  1,
			wantPrompt:    origPrompt,
		},
		{
			name:         "custom prompt to send preserved without modification",
			attempt:      1,
			promptToSend: YieldTrapResumePrompt,
			isDispatch:   true,
			restartCount: 1,
			wantPrompt:   YieldTrapResumePrompt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpHome := t.TempDir()
			tempData := t.TempDir()
			mgr := session.New(tmpHome, tempData)
			if tc.hasOnDiskSess {
				sessDir, err := ensureTestSessionDir(mgr, sessUUID)
				if err != nil {
					t.Fatalf("ensureTestSessionDir: %v", err)
				}
				if err := os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
				if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("{\"step_index\":0}\n"), 0o644); err != nil {
					t.Fatalf("WriteFile: %v", err)
				}
			}

			pool := &WorkerPool{sessionMgr: mgr}
			te := &turnExecution{
				pool:       pool,
				turnPrompt: origPrompt,
				burst:      tc.burst,
			}
			if tc.hasSessionID {
				te.currentSessionID = sessUUID
			}
			if tc.isDispatch {
				te.dispatch = &db.Dispatch{
					ID:           "disp-test-1",
					RestartCount: tc.restartCount,
					ErrorMessage: tc.errorMessage,
				}
			}

			got := te.prepareAttemptPrompt(tc.attempt, tc.promptToSend, tc.lastErrDetail, tc.lastStderr)
			if got != tc.wantPrompt {
				t.Errorf("prepareAttemptPrompt() = %q, want %q", got, tc.wantPrompt)
			}
		})
	}
}

type statefulEvictionTestCase struct {
	name                   string
	sessionID              string
	restartCount           int
	hasExistingTranscript  bool
	yieldTrap              bool
	wantStatus             string
	wantEvictedFromPool    bool
	wantSessionInStore     bool
	wantContinuationPrompt bool
	wantYieldTrapPrompt    bool
}

func assertStatefulEvictionResults(ctx context.Context, t *testing.T, tc statefulEvictionTestCase, store db.Store, dispID, sessUUID string, pool *WorkerPool, promptsSeen []string) {
	t.Helper()
	d, err := store.GetDispatch(ctx, dispID)
	if err != nil || d == nil {
		t.Fatalf("failed to fetch dispatch: %v", err)
	}
	if d.Status != tc.wantStatus {
		t.Fatalf("dispatch status = %s, want %s", d.Status, tc.wantStatus)
	}

	if tc.wantSessionInStore {
		gotSess, err := store.GetSessionID(ctx, tc.sessionID)
		if err != nil || gotSess != sessUUID {
			t.Errorf("store session ID = %q (err=%v), want %q", gotSess, err, sessUUID)
		}
	}

	if tc.wantEvictedFromPool {
		if testPool, ok := pool.AgentPool().(*testRunnerAgentPool); ok {
			if testPool.HasSession(tc.sessionID) {
				t.Errorf("expected session %q to be evicted from active pool", tc.sessionID)
			}
		}
	}

	if tc.wantContinuationPrompt {
		if len(promptsSeen) == 0 || !strings.Contains(promptsSeen[0], "Your previous execution timed out or was interrupted while working") {
			t.Errorf("first attempt prompt does not contain continuation template, got:\n%s", promptsSeen[0])
		}
	}

	if tc.wantYieldTrapPrompt {
		if len(promptsSeen) < 2 || promptsSeen[1] != YieldTrapResumePrompt {
			t.Errorf("second attempt prompt = %q, want %q", promptsSeen[1], YieldTrapResumePrompt)
		}
	}
}

func runStatefulEvictionCase(t *testing.T, tc statefulEvictionTestCase) {
	t.Helper()
	store := testStore(t)
	ctx := context.Background()
	tmpHome := t.TempDir()
	tempData := t.TempDir()
	sessMgr := session.New(tmpHome, tempData)

	sessUUID := "33333333-3333-3333-3333-333333333333"
	if tc.hasExistingTranscript {
		sessDir, err := ensureTestSessionDir(sessMgr, sessUUID)
		if err != nil {
			t.Fatalf("ensureTestSessionDir: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}
`), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := store.SaveSessionID(ctx, tc.sessionID, sessUUID); err != nil {
			t.Fatalf("SaveSessionID: %v", err)
		}
	}

	var mu sync.Mutex
	var promptsSeen []string
	var invCount atomic.Int32

	origPrompt := "[VANADIS BUILD CHUNK EXECUTION] Chunk execution test"

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		SessionManager: sessMgr,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			mu.Unlock()

			inv := invCount.Add(1)
			if tc.yieldTrap && inv == 1 {
				return `{"conversation_id":"` + sessUUID + `","status":"SUCCESS","response":"Waiting for task to complete"}`, "terminating 1 background task(s)", 0, nil
			}
			return `{"conversation_id":"` + sessUUID + `","status":"SUCCESS","response":"chunk executed successfully"}`, "", 0, nil
		},
	})
	defer pool.Stop()

	disp := db.Dispatch{
		ID:           uuid.New().String(),
		Source:       "vanadis",
		SessionID:    tc.sessionID,
		Prompt:       origPrompt,
		RestartCount: tc.restartCount,
		Status:       db.StatusPending,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	if _, err := store.InsertDispatch(ctx, disp); err != nil {
		t.Fatalf("InsertDispatch: %v", err)
	}
	if lt := runLiveDispatch(ctx, pool, tc.sessionID, "turn-1", disp.ID); lt.err != nil {
		t.Fatalf("turn err = %v", lt.err)
	}
	pool.Stop()

	mu.Lock()
	seen := append([]string(nil), promptsSeen...)
	mu.Unlock()

	assertStatefulEvictionResults(ctx, t, tc, store, disp.ID, sessUUID, pool, seen)
}

var statefulEvictionCases = []statefulEvictionTestCase{
	{
		name:                   "stateful dispatch evicts active pool session while preserving conversation in store",
		sessionID:              "vanadis-chunk-1",
		restartCount:           0,
		hasExistingTranscript:  false,
		yieldTrap:              false,
		wantStatus:             db.StatusCompleted,
		wantEvictedFromPool:    true,
		wantSessionInStore:     true,
		wantContinuationPrompt: false,
		wantYieldTrapPrompt:    false,
	},
	{
		name:                   "restarted stateful dispatch sends continuation prompt and transitions to yield trap resume",
		sessionID:              "vanadis-chunk-2",
		restartCount:           1,
		hasExistingTranscript:  true,
		yieldTrap:              true,
		wantStatus:             db.StatusCompleted,
		wantEvictedFromPool:    true,
		wantSessionInStore:     true,
		wantContinuationPrompt: true,
		wantYieldTrapPrompt:    true,
	},
}

func TestWorker_ProcessDispatch_StatefulEvictionAndRestartContinuation(t *testing.T) {
	t.Parallel()
	for _, tc := range statefulEvictionCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStatefulEvictionCase(t, tc)
		})
	}
}
