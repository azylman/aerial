package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return &restate.Request{ID: "req-1"} }

func setupActorMockContext(t *testing.T, key, activeInvID string) (*mocks.MockContext, *mocks.MockClient) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.On("Key").Return(key).Maybe()
	mc.On("Request").Return(&restate.Request{ID: "inv-turn-123"}).Maybe()
	mc.On("Value", mock.Anything).Return(nil).Maybe()

	if activeInvID != "" {
		mc.On("Get", "active_invocation_id", mock.Anything).Run(func(args mock.Arguments) {
			if outPtr, ok := args.Get(1).(*string); ok && outPtr != nil {
				*outPtr = activeInvID
			}
		}).Return(true, nil).Maybe()
	} else {
		mc.On("Get", "active_invocation_id", mock.Anything).Return(false, nil).Maybe()
	}

	mc.On("Set", "active_invocation_id", mock.Anything).Return().Maybe()
	mc.On("Clear", "active_invocation_id").Return().Maybe()
	mc.On("ClearAll").Return().Maybe()
	mc.On("CancelInvocation", mock.Anything).Return().Maybe()

	runHandler := func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if err != nil {
			return restate.ToTerminalError(err)
		}
		if output != nil && res != nil {
			outVal := reflect.ValueOf(output)
			resVal := reflect.ValueOf(res)
			if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
				outVal.Elem().Set(resVal)
			}
		}
		return nil
	}
	mc.On("Run", mock.Anything, mock.Anything).Return(runHandler).Maybe()
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(runHandler).Maybe()

	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.On("Send", mock.Anything).Return(mInv).Maybe()

	return mc, mClient
}

type threadEnqueueTestCase struct {
	name           string
	threadID       string
	req            ThreadEnqueueRequest
	activeInvID    string
	storeActive    bool
	storeActiveErr bool
	storeFail      bool
	nilPool        bool
	redirectBuild  bool
	sysActive      bool
	wantStatus     string
	wantInsert     bool
	wantErrorSub   string
	wantTerminal   bool
}

func assertTerminalOrRetriableError(t *testing.T, err error, wantErrorSub string, wantTerminal bool) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), wantErrorSub)
	require.True(t, restate.IsTerminalError(err))
	termErr := restate.AsTerminalError(err)
	if wantTerminal {
		assert.Equal(t, restate.Code(400), termErr.Code(), "expected 400 terminal error")
	} else {
		assert.Equal(t, restate.Code(500), termErr.Code(), "expected non-terminal retriable error")
	}
}

func runThreadSessionEnqueueCase(t *testing.T, tc threadEnqueueTestCase) {
	t.Helper()
	store := dbtest.NewFakeStore()
	if tc.storeActive {
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:       "msg-active",
			ThreadID: tc.threadID,
			Status:   db.StatusProcessing,
		})
	}
	if tc.storeActiveErr {
		store.FailNext("HasActiveThreadTurn", errors.New("turn check error"))
	}
	if tc.storeFail {
		store.FailNext("InsertMessage", errors.New("db error"))
	}

	var pool *WorkerPool
	if !tc.nilPool {
		pool = NewWorkerPool(WorkerPoolConfig{Store: store})
		if tc.redirectBuild {
			pool.cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
				return config.ChannelPolicy{NoBuildUpdates: true}
			}
			pool.appCfg = config.NewFromData(&config.ConfigData{SystemChannel: "1564150557705248879"})
			if tc.sysActive {
				_ = store.InsertMessage(context.Background(), db.Message{
					ID:       "msg-active-sys",
					ThreadID: "1564150557705248879",
					Status:   db.StatusProcessing,
				})
			}
		}
	}
	obj := NewThreadSessionObject(pool, "aerial")

	mc, _ := setupActorMockContext(t, tc.threadID, tc.activeInvID)
	sharedCtx := restate.WithMockContext(mc)

	var resp ThreadEnqueueResponse
	var err error
	resp, err = obj.Enqueue(sharedCtx, tc.req)

	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
		if tc.wantTerminal {
			require.True(t, restate.IsTerminalError(err), "expected restate.TerminalError")
			termErr := restate.AsTerminalError(err)
			assert.Equal(t, restate.Code(400), termErr.Code())
		}
		return
	}

	require.NoError(t, err)
	assert.Equal(t, tc.wantStatus, resp.Status)
	if tc.wantInsert {
		assert.NotEmpty(t, resp.MessageID)
		msg, gErr := store.GetMessage(context.Background(), resp.MessageID)
		require.NoError(t, gErr)
		require.NotNil(t, msg)
		assert.Equal(t, db.StatusPending, msg.Status)
	} else if tc.req.MessageID != "" {
		msg, _ := store.GetMessage(context.Background(), tc.req.MessageID)
		assert.Nil(t, msg)
	}
}

func TestThreadSessionObject_Enqueue(t *testing.T) {
	t.Parallel()

	tests := []threadEnqueueTestCase{
		{
			name:       "normal prompt enqueue accepted",
			threadID:   "th-100",
			req:        ThreadEnqueueRequest{Prompt: "hello aerial", MessageID: "msg-100"},
			wantStatus: "accepted",
			wantInsert: true,
		},
		{
			name:       "message object enqueue accepted",
			threadID:   "th-200",
			req:        ThreadEnqueueRequest{Message: &db.Message{ID: "msg-200", ThreadID: "th-200", Content: "discord msg"}},
			wantStatus: "accepted",
			wantInsert: true,
		},
		{
			name:       "fallback to channelID when key empty and message fields empty",
			threadID:   "",
			req:        ThreadEnqueueRequest{ChannelID: "chan-fallback", Message: &db.Message{Content: "no ids"}},
			wantStatus: "accepted",
			wantInsert: true,
		},
		{
			name:         "empty thread and channel id returns error",
			threadID:     "",
			req:          ThreadEnqueueRequest{Prompt: "no thread or channel"},
			wantErrorSub: "thread_id or channel_id is required",
			wantTerminal: true,
		},
		{
			name:        "skip if active when invocation state active",
			threadID:    "th-400",
			req:         ThreadEnqueueRequest{Prompt: "skip me", SkipIfActive: true},
			activeInvID: "inv-active-999",
			wantStatus:  "skipped_active",
			wantInsert:  false,
		},
		{
			name:        "skip if active when store has active turn",
			threadID:    "th-500",
			req:         ThreadEnqueueRequest{Prompt: "skip me store", SkipIfActive: true},
			storeActive: true,
			wantStatus:  "skipped_active",
			wantInsert:  false,
		},
		{
			name:          "skip if active redirects build completion prompt and skips when redirected channel is active",
			threadID:      "user-chan-1",
			req:           ThreadEnqueueRequest{Prompt: "Vanadis build b-123 completed successfully", SkipIfActive: true},
			redirectBuild: true,
			sysActive:     true,
			wantStatus:    "skipped_active",
			wantInsert:    false,
		},
		{
			name:          "skip if active redirects build completion prompt and does not skip when pre-redirect thread is active but target is idle",
			threadID:      "user-chan-2",
			activeInvID:   "inv-user-chan-2",
			req:           ThreadEnqueueRequest{Prompt: "Vanadis build b-123 completed successfully", SkipIfActive: true},
			redirectBuild: true,
			sysActive:     false,
			wantStatus:    "accepted",
			wantInsert:    true,
		},
		{
			name:           "skip if active fails when store active check errors",
			threadID:       "th-550",
			req:            ThreadEnqueueRequest{Prompt: "skip me err", SkipIfActive: true},
			storeActiveErr: true,
			wantErrorSub:   "turn check error",
		},
		{
			name:         "store insert failure returns error",
			threadID:     "th-600",
			req:          ThreadEnqueueRequest{Prompt: "boom", MessageID: "msg-600"},
			storeFail:    true,
			wantErrorSub: "db error",
		},
		{
			name:       "nil pool enqueue succeeds without db insertion",
			threadID:   "th-700",
			req:        ThreadEnqueueRequest{Prompt: "no pool"},
			nilPool:    true,
			wantStatus: "accepted",
			wantInsert: false,
		},
		{
			name:       "skip if active with nil pool returns accepted",
			threadID:   "th-skip-nil",
			req:        ThreadEnqueueRequest{Prompt: "nil pool skip", SkipIfActive: true},
			nilPool:    true,
			wantStatus: "accepted",
			wantInsert: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runThreadSessionEnqueueCase(t, tc)
		})
	}
}

func TestThreadSessionObject_CancelTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		threadID            string
		activeInvID         string
		pendingCount        int
		req                 CancelTurnRequest
		nilPool             bool
		storeFail           bool
		wantCancelledCount  int
		wantCancelledActive bool
		wantErrorSub        string
	}{
		{
			name:                "cancel with pending messages and active invocation",
			threadID:            "th-cancel-1",
			activeInvID:         "inv-cancel-1",
			pendingCount:        2,
			req:                 CancelTurnRequest{Reason: "user cancelled"},
			wantCancelledCount:  2,
			wantCancelledActive: true,
		},
		{
			name:                "cancel idle thread with 0 pending",
			threadID:            "th-cancel-2",
			activeInvID:         "",
			pendingCount:        0,
			req:                 CancelTurnRequest{},
			wantCancelledCount:  0,
			wantCancelledActive: false,
		},
		{
			name:                "cancel with nil pool",
			threadID:            "th-cancel-nil",
			pendingCount:        0,
			nilPool:             true,
			wantCancelledCount:  0,
			wantCancelledActive: false,
		},
		{
			name:         "cancel with store failure returns non-terminal error",
			threadID:     "th-cancel-fail",
			pendingCount: 0,
			storeFail:    true,
			wantErrorSub: "get msgs fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			for i := range tc.pendingCount {
				_ = store.InsertMessage(context.Background(), db.Message{
					ID:       fmt.Sprintf("%s-m%d", tc.threadID, i),
					ThreadID: tc.threadID,
					Status:   db.StatusPending,
				})
			}
			if tc.storeFail {
				store.FailNext("GetPendingOrProcessingMessages", errors.New("get msgs fail"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				pool = NewWorkerPool(WorkerPoolConfig{Store: store})
			}
			obj := NewThreadSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.threadID, tc.activeInvID)
			resp, err := obj.CancelTurn(restate.WithMockContext(mc), tc.req)
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				assert.Equal(t, restate.Code(500), restate.AsTerminalError(err).Code(), "expected non-terminal retriable error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "ok", resp.Status)
			assert.Equal(t, tc.wantCancelledCount, resp.CancelledCount)
			assert.Equal(t, tc.wantCancelledActive, resp.CancelledActiveTurn)

			if !tc.nilPool && !tc.storeFail {
				msgs, _ := store.GetPendingOrProcessingMessages(context.Background(), 100)
				for _, m := range msgs {
					if m.ThreadID == tc.threadID {
						assert.Equal(t, db.StatusFailed, m.Status)
					}
				}
			}
		})
	}
}

func TestThreadSessionObject_RunTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		threadID      string
		pendingCount  int
		storeErr      bool
		claimFail     bool
		nilPool       bool
		withRunner    bool
		unfinalized   bool
		wantProcessed int
		wantDrained   int
		wantErrorSub  string
	}{
		{
			name:          "no pending messages breaks immediately",
			threadID:      "th-empty",
			pendingCount:  0,
			wantProcessed: 0,
			wantDrained:   0,
		},
		{
			name:          "drains up to 5 pending messages in a burst",
			threadID:      "th-burst-3",
			pendingCount:  3,
			wantProcessed: 1,
			wantDrained:   3,
		},
		{
			name:          "burst caps at 5 messages per iteration",
			threadID:      "th-burst-6",
			pendingCount:  6,
			wantProcessed: 2,
			wantDrained:   6,
		},
		{
			name:          "claim pending message failure skips message",
			threadID:      "th-claim-fail",
			pendingCount:  1,
			claimFail:     true,
			wantProcessed: 0,
			wantDrained:   0,
		},
		{
			name:          "nil pool breaks immediately",
			threadID:      "th-nil-pool",
			nilPool:       true,
			wantProcessed: 0,
			wantDrained:   0,
		},
		{
			name:         "store get pending error returns error",
			threadID:     "th-err",
			storeErr:     true,
			wantErrorSub: "store read error",
		},
		{
			name:          "burst executes with runner pool and finalizes successfully",
			threadID:      "th-runner-success",
			pendingCount:  2,
			withRunner:    true,
			wantProcessed: 1,
			wantDrained:   2,
		},
		{
			name:         "burst aborts unfinalized when store fails to update status",
			threadID:     "th-runner-unfinalized",
			pendingCount: 1,
			withRunner:   true,
			unfinalized:  true,
			wantErrorSub: "burst execution aborted unfinalized",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			for i := range tc.pendingCount {
				_ = store.InsertMessage(context.Background(), db.Message{
					ID:       fmt.Sprintf("%s-msg-%d", tc.threadID, i),
					ThreadID: tc.threadID,
					Content:  fmt.Sprintf("msg %d", i),
					Status:   db.StatusPending,
				})
			}
			if tc.storeErr {
				store.FailNext("GetPendingOrProcessingMessages", errors.New("store read error"))
			}
			if tc.claimFail {
				store.FailNext("ClaimPendingMessage", errors.New("claim fail"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				cfg := WorkerPoolConfig{Store: store}
				if tc.withRunner {
					cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMins int) (string, string, int, error) {
						return `{"response": "completed successfully", "status": "SUCCESS"}`, "", 0, nil
					}
				}
				pool = NewWorkerPool(cfg)
				pool.startedAt = time.Now().UTC()
			}
			if tc.unfinalized {
				store.FailNext("UpdateMessageStatus", errors.New("db write failed"))
			}
			obj := NewThreadSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.threadID, "")
			res, err := obj.RunTurn(restate.WithMockContext(mc), ThreadTurnRequest{})
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "completed", res.Status)
			assert.Equal(t, tc.wantProcessed, res.ProcessedTurns)
			assert.Equal(t, tc.wantDrained, res.MessagesDrained)
		})
	}
}

func TestDispatchSessionObject_Enqueue(t *testing.T) {
	t.Parallel()
	insertAndSend := []string{"Run [{insert-pending-dispatch}]", "Send RunTurn"}
	skipCheck := []string{"Get active_invocation_id", "Run [{check-active-dispatch-turn}]"}

	tests := []struct {
		name           string
		sessionKey     string
		req            eriwire.BrainDispatchRequest
		activeInvID    string
		storeActive    bool
		storeActiveErr bool
		seedExisting   bool
		storeFail      bool
		nilPool        bool
		wantStatus     string
		wantErrorSub   string
		wantRowPending bool
		wantCommands   []string
	}{
		{
			name:           "normal dispatch enqueue accepted",
			sessionKey:     "dispatch-100",
			req:            eriwire.BrainDispatchRequest{ID: "d-100", Source: "vanadis", Prompt: "build chunk 1"},
			wantStatus:     "accepted",
			wantRowPending: true,
			wantCommands:   insertAndSend,
		},
		{name: "empty key and empty id generates dispatch key and id", req: eriwire.BrainDispatchRequest{Prompt: "auto dispatch"}, wantStatus: "accepted", wantCommands: insertAndSend},
		{
			name:           "already existing dispatch returns already_exists",
			sessionKey:     "dispatch-200",
			req:            eriwire.BrainDispatchRequest{ID: "d-200", Source: "vanadis", Prompt: "build chunk 2"},
			seedExisting:   true,
			wantStatus:     "already_exists",
			wantRowPending: true,
			wantCommands:   insertAndSend,
		},
		{
			name:           "stale marker without active row is accepted and inserts row",
			sessionKey:     "vanadis-g3-shadow-soak-r11",
			req:            eriwire.BrainDispatchRequest{ID: "d-300", Source: "vanadis", Prompt: "retry chunk", SkipIfActive: true},
			activeInvID:    "inv-cancelled-300",
			wantStatus:     "accepted",
			wantRowPending: true,
			wantCommands:   []string{"Get active_invocation_id", "Run [{check-active-dispatch-turn}]", "Run [{insert-pending-dispatch}]", "Send RunTurn"},
		},
		{
			name:         "marker with active row still skips",
			sessionKey:   "dispatch-350",
			req:          eriwire.BrainDispatchRequest{ID: "d-350", Source: "vanadis", Prompt: "build chunk 35", SkipIfActive: true},
			activeInvID:  "inv-disp-350",
			storeActive:  true,
			wantStatus:   "skipped_active",
			wantCommands: skipCheck,
		},
		{
			name:         "skip if active when store has active dispatch turn",
			sessionKey:   "dispatch-400",
			req:          eriwire.BrainDispatchRequest{ID: "d-400", Source: "vanadis", Prompt: "build chunk 4", SkipIfActive: true},
			storeActive:  true,
			wantStatus:   "skipped_active",
			wantCommands: skipCheck,
		},
		{
			name:           "skip if active fails when store active check errors",
			sessionKey:     "dispatch-450",
			req:            eriwire.BrainDispatchRequest{ID: "d-450", Source: "vanadis", Prompt: "skip err", SkipIfActive: true},
			storeActiveErr: true,
			wantErrorSub:   "active check error",
			wantCommands:   skipCheck,
		},
		{
			name:         "store insert failure returns error",
			sessionKey:   "dispatch-500",
			req:          eriwire.BrainDispatchRequest{ID: "d-500", Source: "vanadis", Prompt: "fail me"},
			storeFail:    true,
			wantErrorSub: "db error",
			wantCommands: []string{"Run [{insert-pending-dispatch}]"},
		},
		{name: "nil pool accepted without store insertion", sessionKey: "dispatch-600", req: eriwire.BrainDispatchRequest{ID: "d-600", Prompt: "nil pool"}, nilPool: true, wantStatus: "accepted", wantCommands: insertAndSend},
		{
			name:         "skip if active with nil pool returns accepted",
			sessionKey:   "dispatch-skip-nil",
			req:          eriwire.BrainDispatchRequest{ID: "d-skip-nil", Prompt: "nil pool skip", SkipIfActive: true},
			nilPool:      true,
			wantStatus:   "accepted",
			wantCommands: []string{"Get active_invocation_id", "Run [{insert-pending-dispatch}]", "Send RunTurn"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			if tc.seedExisting {
				_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
					ID:        tc.req.ID,
					SessionID: tc.sessionKey,
					Prompt:    tc.req.Prompt,
					Status:    db.StatusPending,
				})
			}
			if tc.storeActive {
				_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
					ID:        "d-active",
					SessionID: tc.sessionKey,
					Prompt:    "active",
					Status:    db.StatusProcessing,
				})
			}
			if tc.storeActiveErr {
				store.FailNext("HasActiveDispatchTurn", errors.New("active check error"))
			}
			if tc.storeFail {
				store.FailNext("InsertDispatch", errors.New("db error"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				pool = NewWorkerPool(WorkerPoolConfig{Store: store})
			}
			obj := NewDispatchSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.sessionKey, tc.activeInvID)
			resp, err := obj.Enqueue(restate.WithMockContext(mc), tc.req)
			assert.Equal(t, tc.wantCommands, actorJournalCommands(mc))
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, resp.Status)
			row, gErr := store.GetDispatch(context.Background(), tc.req.ID)
			require.NoError(t, gErr)
			assert.Equal(t, tc.wantRowPending, row != nil && row.Status == db.StatusPending)
		})
	}
}

func TestDispatchSessionObject_RunTurn(t *testing.T) {
	t.Parallel()
	drainedOne := []string{"Set active_invocation_id", "Run [{execute-dispatch}]", "Run [{execute-dispatch}]", "ClearAll"}
	idle := []string{"Set active_invocation_id", "Run [{execute-dispatch}]", "ClearAll"}
	failedCleared := []string{"Set active_invocation_id", "Run [{execute-dispatch}]", "Clear active_invocation_id"}

	tests := []struct {
		name           string
		sessionKey     string
		pendingCount   int
		storeErr       error
		claimFail      bool
		nilPool        bool
		withRunner     bool
		unfinalized    bool
		wantDisparches int
		wantErrorSub   string
		wantCommands   []string
	}{
		{
			name:         "no pending dispatches breaks immediately",
			sessionKey:   "dispatch-empty",
			wantCommands: idle,
		},
		{
			name:           "claims and drains matching pending dispatch",
			sessionKey:     "vanadis-chunk-1",
			pendingCount:   1,
			wantDisparches: 1,
			wantCommands:   drainedOne,
		},
		{
			name:         "claim failure skips dispatch",
			sessionKey:   "dispatch-claim-fail",
			pendingCount: 1,
			claimFail:    true,
			wantCommands: idle,
		},
		{name: "nil pool breaks immediately", sessionKey: "dispatch-nil", nilPool: true, wantCommands: []string{"Set active_invocation_id", "ClearAll"}},
		{
			name:         "store get pending error clears marker",
			sessionKey:   "dispatch-err",
			storeErr:     errors.New("disp read error"),
			wantErrorSub: "disp read error",
			wantCommands: failedCleared,
		},
		{
			name:         "cancelled turn clears marker",
			sessionKey:   "vanadis-g3-shadow-soak-r11",
			storeErr:     restate.ToTerminalError(errors.New("invocation cancelled"), restate.WithErrorCode(409)),
			wantErrorSub: "invocation cancelled",
			wantCommands: failedCleared,
		},
		{
			name:           "dispatch executes with runner pool and finalizes successfully",
			sessionKey:     "vanadis-runner-success",
			pendingCount:   1,
			withRunner:     true,
			wantDisparches: 1,
			wantCommands:   drainedOne,
		},
		{
			name:         "dispatch aborts unfinalized and clears marker",
			sessionKey:   "vanadis-runner-unfinalized",
			pendingCount: 1,
			withRunner:   true,
			unfinalized:  true,
			wantErrorSub: "dispatch execution aborted unfinalized",
			wantCommands: failedCleared,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			for i := range tc.pendingCount {
				_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
					ID:             fmt.Sprintf("%s-d%d", tc.sessionKey, i),
					SessionID:      tc.sessionKey,
					VanadisChunkID: "chunk-1",
					Source:         "vanadis",
					Prompt:         "run chunk",
					Status:         db.StatusPending,
				})
			}
			if tc.storeErr != nil {
				store.FailNext("GetPendingOrProcessingDispatches", tc.storeErr)
			}
			if tc.claimFail {
				store.FailNext("ClaimPendingDispatch", errors.New("claim disp fail"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				cfg := WorkerPoolConfig{Store: store}
				if tc.withRunner {
					cfg.RunnerFunc = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMins int) (string, string, int, error) {
						return `{"response": "chunk executed", "status": "SUCCESS"}`, "", 0, nil
					}
				}
				pool = NewWorkerPool(cfg)
				pool.startedAt = time.Now().UTC()
			}
			if tc.unfinalized {
				store.FailNext("UpdateDispatchStatus", errors.New("db write failed"))
			}
			obj := NewDispatchSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.sessionKey, "")
			res, err := obj.RunTurn(restate.WithMockContext(mc), eriwire.BrainDispatchRequest{VanadisChunkID: "chunk-1"})
			assert.Equal(t, tc.wantCommands, actorJournalCommands(mc))
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "completed", res.Status)
			assert.Equal(t, tc.wantDisparches, res.DispatchesExecuted)
		})
	}
}

func TestDispatchSessionObject_Cancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		sessionKey         string
		req                CancelDispatchRequest
		activeInvID        string
		seedCount          int
		storeFail          bool
		nilPool            bool
		wantCancelledCount int
		wantErrorSub       string
	}{
		{
			name:               "cancel pending dispatches by chunk id and cancel active invocation",
			sessionKey:         "vanadis-chunk-cancel",
			req:                CancelDispatchRequest{ChunkID: "chunk-cancel", Reason: "build failed"},
			activeInvID:        "inv-chunk-cancel",
			seedCount:          2,
			wantCancelledCount: 2,
		},
		{
			name:               "cancel with sessionKey dispatch- prefix infers dispatch ID",
			sessionKey:         "dispatch-d-inferred",
			req:                CancelDispatchRequest{},
			activeInvID:        "",
			seedCount:          1,
			wantCancelledCount: 1,
		},
		{
			name:               "cancel with sessionKey vanadis- prefix infers build ID",
			sessionKey:         "vanadis-b-inferred",
			req:                CancelDispatchRequest{},
			activeInvID:        "",
			seedCount:          1,
			wantCancelledCount: 1,
		},
		{
			name:               "cancel idle dispatch session",
			sessionKey:         "dispatch-idle",
			req:                CancelDispatchRequest{DispatchID: "d-idle"},
			activeInvID:        "",
			seedCount:          0,
			wantCancelledCount: 0,
		},
		{
			name:         "cancel with store failure returns non-terminal error",
			sessionKey:   "vanadis-fail",
			req:          CancelDispatchRequest{},
			storeFail:    true,
			wantErrorSub: "cancel err",
		},
		{
			name:               "cancel with nil pool handles gracefully",
			sessionKey:         "vanadis-nil",
			req:                CancelDispatchRequest{},
			nilPool:            true,
			wantCancelledCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			for i := range tc.seedCount {
				dID := fmt.Sprintf("d-%d", i)
				if cut, ok := strings.CutPrefix(tc.sessionKey, "dispatch-"); ok {
					dID = cut
				}
				_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
					ID:             dID,
					SessionID:      tc.sessionKey,
					VanadisChunkID: "chunk-cancel",
					Source:         "vanadis",
					Prompt:         "task",
					Status:         db.StatusPending,
				})
			}
			if tc.storeFail {
				store.FailNext("CancelPendingDispatches", errors.New("cancel err"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				pool = NewWorkerPool(WorkerPoolConfig{Store: store})
			}
			obj := NewDispatchSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.sessionKey, tc.activeInvID)
			resp, err := obj.Cancel(restate.WithMockContext(mc), tc.req)
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				assert.Equal(t, restate.Code(500), restate.AsTerminalError(err).Code(), "expected non-terminal retriable error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "ok", resp.Status)
			assert.Equal(t, tc.wantCancelledCount, resp.CancelledCount)
		})
	}
}

func TestActiveInvocationRegistry(t *testing.T) {
	t.Parallel()

	registerActiveInvocation("", "")
	registerActiveInvocation("valid", "")
	assert.Empty(t, getActiveInvocation(""))

	registerActiveInvocation("k1", "inv1")
	assert.Equal(t, "inv1", getActiveInvocation("k1"))

	unregisterActiveInvocation("", "")
	unregisterActiveInvocation("k1", "wrong-inv")
	assert.Equal(t, "inv1", getActiveInvocation("k1"))

	unregisterActiveInvocation("k1", "inv1")
	assert.Empty(t, getActiveInvocation("k1"))
}

func TestDeriveDeterministicIDs(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, deriveDeterministicMessageID(nil, "th-1"))
	assert.NotEmpty(t, deriveDeterministicDispatchID(nil, "sess-1"))
}

func TestThreadSessionObject_Enqueue_IdempotentRetry(t *testing.T) {
	t.Parallel()

	store := dbtest.NewFakeStore()
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	obj := NewThreadSessionObject(pool, "aerial")

	mc, _ := setupActorMockContext(t, "th-retry", "")
	sharedCtx := restate.WithMockContext(mc)

	// First attempt without caller-supplied message ID
	resp1, err1 := obj.Enqueue(sharedCtx, ThreadEnqueueRequest{Prompt: "idempotent test"})
	require.NoError(t, err1)
	assert.Equal(t, "accepted", resp1.Status)
	assert.NotEmpty(t, resp1.MessageID)

	// Retry of the same Restate invocation (same mock context with same Request.ID)
	resp2, err2 := obj.Enqueue(sharedCtx, ThreadEnqueueRequest{Prompt: "idempotent test"})
	require.NoError(t, err2)
	assert.Equal(t, "accepted", resp2.Status)
	assert.Equal(t, resp1.MessageID, resp2.MessageID)

	// Only 1 message inserted in store
	msgs, err := store.GetPendingOrProcessingMessages(context.Background(), 0)
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
}

func TestDispatchSessionObject_Enqueue_IdempotentRetry(t *testing.T) {
	t.Parallel()

	store := dbtest.NewFakeStore()
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	obj := NewDispatchSessionObject(pool, "aerial")

	mc, _ := setupActorMockContext(t, "disp-retry", "")
	sharedCtx := restate.WithMockContext(mc)

	// First attempt without caller-supplied dispatch ID
	resp1, err1 := obj.Enqueue(sharedCtx, eriwire.BrainDispatchRequest{Source: "vanadis", Prompt: "idempotent dispatch"})
	require.NoError(t, err1)
	assert.Equal(t, "accepted", resp1.Status)
	assert.NotEmpty(t, resp1.DispatchID)

	// Retry of the same Restate invocation (same mock context with same Request.ID)
	resp2, err2 := obj.Enqueue(sharedCtx, eriwire.BrainDispatchRequest{Source: "vanadis", Prompt: "idempotent dispatch"})
	require.NoError(t, err2)
	assert.Equal(t, "accepted", resp2.Status)
	assert.Equal(t, resp1.DispatchID, resp2.DispatchID)

	// Only 1 dispatch in store
	disps, err := store.GetPendingOrProcessingDispatches(context.Background(), 0)
	require.NoError(t, err)
	assert.Len(t, disps, 1)
}

func TestThreadSessionObject_RunTurn_Exceeds100PendingMessages(t *testing.T) {
	t.Parallel()

	store := dbtest.NewFakeStore()
	// Seed 105 messages for other threads
	for i := range 105 {
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:        fmt.Sprintf("msg-other-%d", i),
			ThreadID:  "th-other",
			ChannelID: "th-other",
			Content:   "other message",
			Status:    db.StatusPending,
		})
	}
	// Seed 1 message for our target thread after the 105 other messages
	_ = store.InsertMessage(context.Background(), db.Message{
		ID:        "msg-target-1",
		ThreadID:  "th-target",
		ChannelID: "th-target",
		Content:   "target message",
		Status:    db.StatusPending,
	})

	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	pool.startedAt = time.Now().UTC()
	obj := NewThreadSessionObject(pool, "aerial")

	mc, _ := setupActorMockContext(t, "th-target", "")
	res, err := obj.RunTurn(restate.WithMockContext(mc), ThreadTurnRequest{})
	require.NoError(t, err)
	assert.Equal(t, "completed", res.Status)
	assert.Equal(t, 1, res.MessagesDrained)
}

func TestThreadSessionObject_CancelTurn_Exceeds100PendingMessages(t *testing.T) {
	t.Parallel()

	store := dbtest.NewFakeStore()
	// Seed 105 messages for other threads
	for i := range 105 {
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:        fmt.Sprintf("msg-other-%d", i),
			ThreadID:  "th-other",
			ChannelID: "th-other",
			Content:   "other message",
			Status:    db.StatusPending,
		})
	}
	// Seed 2 messages for our target thread after the 105 other messages
	_ = store.InsertMessage(context.Background(), db.Message{
		ID:        "msg-target-cancel-1",
		ThreadID:  "th-cancel",
		ChannelID: "th-cancel",
		Content:   "target message 1",
		Status:    db.StatusPending,
	})
	_ = store.InsertMessage(context.Background(), db.Message{
		ID:        "msg-target-cancel-2",
		ThreadID:  "th-cancel",
		ChannelID: "th-cancel",
		Content:   "target message 2",
		Status:    db.StatusPending,
	})

	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	obj := NewThreadSessionObject(pool, "aerial")

	mc, _ := setupActorMockContext(t, "th-cancel", "")
	res, err := obj.CancelTurn(restate.WithMockContext(mc), CancelTurnRequest{Reason: "stop"})
	require.NoError(t, err)
	assert.Equal(t, "ok", res.Status)
	assert.Equal(t, 2, res.CancelledCount)
}

func TestDispatchSessionObject_RunTurn_Exceeds100PendingDispatches(t *testing.T) {
	t.Parallel()

	store := dbtest.NewFakeStore()
	// Seed 105 dispatches for other sessions
	for i := range 105 {
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:        fmt.Sprintf("d-other-%d", i),
			SessionID: "sess-other",
			Source:    "vanadis",
			Prompt:    "other dispatch",
			Status:    db.StatusPending,
		})
	}
	// Seed 1 dispatch for our target session after the 105 other dispatches
	_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
		ID:             "d-target-1",
		SessionID:      "vanadis-target",
		VanadisChunkID: "target-chunk-1",
		Source:         "vanadis",
		Prompt:         "target dispatch",
		Status:         db.StatusPending,
	})

	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	pool.startedAt = time.Now().UTC()
	obj := NewDispatchSessionObject(pool, "aerial")

	mc, _ := setupActorMockContext(t, "vanadis-target", "")
	res, err := obj.RunTurn(restate.WithMockContext(mc), eriwire.BrainDispatchRequest{VanadisChunkID: "target-chunk-1"})
	require.NoError(t, err)
	assert.Equal(t, "completed", res.Status)
	assert.Equal(t, 1, res.DispatchesExecuted)
}

func TestAtLeastOnceRecovery(t *testing.T) {
	t.Parallel()

	t.Run("thread recovers in-flight processing message and increments restart count", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:           "msg-proc-1",
			ThreadID:     "th-recover",
			Status:       db.StatusProcessing,
			RestartCount: 0,
			Content:      "recover me",
		})
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		pool.startedAt = time.Now().UTC()
		obj := NewThreadSessionObject(pool, "aerial")

		mc, _ := setupActorMockContext(t, "th-recover", "")
		res, err := obj.RunTurn(restate.WithMockContext(mc), ThreadTurnRequest{})
		require.NoError(t, err)
		assert.Equal(t, 1, res.MessagesDrained)
		savedMsg, mErr := store.GetMessage(context.Background(), "msg-proc-1")
		require.NoError(t, mErr)
		assert.Equal(t, 1, savedMsg.RestartCount)
	})

	t.Run("dispatch recovers in-flight processing dispatch and increments restart count", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:           "d-proc-1",
			SessionID:    "sess-recover",
			Status:       db.StatusProcessing,
			RestartCount: 0,
			Prompt:       "recover dispatch",
		})
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		pool.startedAt = time.Now().UTC()
		obj := NewDispatchSessionObject(pool, "aerial")

		mc, _ := setupActorMockContext(t, "sess-recover", "")
		res, err := obj.RunTurn(restate.WithMockContext(mc), eriwire.BrainDispatchRequest{})
		require.NoError(t, err)
		assert.Equal(t, 1, res.DispatchesExecuted)
		savedDisp, dErr := store.GetDispatch(context.Background(), "d-proc-1")
		require.NoError(t, dErr)
		assert.Equal(t, 1, savedDisp.RestartCount)
	})

	t.Run("dispatch enqueue sends run_turn even when already exists", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:        "d-conflict",
			SessionID: "sess-conflict",
			Prompt:    "conflict",
			Status:    db.StatusPending,
		})
		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		obj := NewDispatchSessionObject(pool, "aerial")

		mc, client := setupActorMockContext(t, "sess-conflict", "")
		resp, err := obj.Enqueue(restate.WithMockContext(mc), eriwire.BrainDispatchRequest{ID: "d-conflict", Prompt: "conflict"})
		require.NoError(t, err)
		assert.Equal(t, "already_exists", resp.Status)
		client.AssertCalled(t, "Send", mock.Anything)
	})

	t.Run("claim pending burst recovery edge cases", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		// 1. Completed message should be skipped
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:       "msg-done",
			ThreadID: "th-edge",
			Status:   db.StatusCompleted,
		})
		// 2. Processing message where reset fails
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:       "msg-proc-reset-fail",
			ThreadID: "th-edge",
			Status:   db.StatusProcessing,
		})
		// 3. Processing message where claim fails
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:       "msg-proc-claim-fail",
			ThreadID: "th-edge",
			Status:   db.StatusProcessing,
		})
		// 4. Processing message that recovers successfully
		_ = store.InsertMessage(context.Background(), db.Message{
			ID:       "msg-proc-ok",
			ThreadID: "th-edge",
			Status:   db.StatusProcessing,
		})

		store.FailNext("ResetMessageToPendingWithRestart", errors.New("reset fail"))
		store.FailNext("ClaimPendingMessage", errors.New("claim fail"))

		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		obj := NewThreadSessionObject(pool, "aerial")

		burst, err := obj.claimPendingBurst(context.Background(), "th-edge")
		require.NoError(t, err)
		require.Len(t, burst, 1)
		assert.Equal(t, "msg-proc-ok", burst[0].ID)
		assert.Equal(t, 1, burst[0].RestartCount)
	})

	t.Run("claim next dispatch recovery edge cases", func(t *testing.T) {
		t.Parallel()
		store := dbtest.NewFakeStore()
		// 1. Completed dispatch should be skipped
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:        "d-done",
			SessionID: "sess-edge",
			Status:    db.StatusCompleted,
		})
		// 2. Processing dispatch where reset fails
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:        "d-proc-reset-fail",
			SessionID: "sess-edge",
			Status:    db.StatusProcessing,
		})
		// 3. Processing dispatch that recovers successfully
		_, _ = store.InsertDispatch(context.Background(), db.Dispatch{
			ID:        "d-proc-ok",
			SessionID: "sess-edge",
			Status:    db.StatusProcessing,
		})

		store.FailNext("ResetDispatchToPendingWithRestart", errors.New("reset fail"))

		pool := NewWorkerPool(WorkerPoolConfig{Store: store})
		obj := NewDispatchSessionObject(pool, "aerial")

		claimed, found, err := obj.claimNextDispatch(context.Background(), "sess-edge", "")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "d-proc-ok", claimed.ID)
		assert.Equal(t, 1, claimed.RestartCount)
	})
}

type cancelCheckStore struct {
	*dbtest.FakeStore
}

func (s *cancelCheckStore) GetMessage(ctx context.Context, id string) (*db.Message, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return s.FakeStore.GetMessage(ctx, id)
}

func (s *cancelCheckStore) GetDispatch(ctx context.Context, id string) (*db.Dispatch, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return s.FakeStore.GetDispatch(ctx, id)
}

func TestTurnExecution_IsRowStillProcessing(t *testing.T) {
	baseStore := dbtest.NewFakeStore()
	_ = baseStore.InsertMessage(context.Background(), db.Message{ID: "m-proc", Status: db.StatusProcessing})
	_ = baseStore.InsertMessage(context.Background(), db.Message{ID: "m-done", Status: db.StatusCompleted})
	_, _ = baseStore.InsertDispatch(context.Background(), db.Dispatch{ID: "d-proc", Status: db.StatusProcessing})
	_, _ = baseStore.InsertDispatch(context.Background(), db.Dispatch{ID: "d-done", Status: db.StatusCompleted})

	store := &cancelCheckStore{FakeStore: baseStore}
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})

	tests := []struct {
		name       string
		te         *turnExecution
		failStore  string
		cancelCtx  bool
		wantResult bool
	}{
		{
			name:       "nil turnExecution returns false",
			te:         nil,
			wantResult: false,
		},
		{
			name:       "nil store returns false",
			te:         &turnExecution{pool: &WorkerPool{}},
			wantResult: false,
		},
		{
			name:       "dispatch with nil dispatch struct returns false",
			te:         &turnExecution{pool: pool, dispatch: nil},
			wantResult: false,
		},
		{
			name:       "dispatch with store error fails closed returning true",
			te:         &turnExecution{pool: pool, dispatch: &db.Dispatch{ID: "d-done"}},
			failStore:  "GetDispatch",
			wantResult: true,
		},
		{
			name:       "dispatch still processing returns true",
			te:         &turnExecution{pool: pool, dispatch: &db.Dispatch{ID: "d-proc"}},
			wantResult: true,
		},
		{
			name:       "dispatch completed returns false",
			te:         &turnExecution{pool: pool, dispatch: &db.Dispatch{ID: "d-done"}},
			wantResult: false,
		},
		{
			name:       "burst with store error fails closed returning true",
			te:         &turnExecution{pool: pool, burst: []db.Message{{ID: "m-done"}}},
			failStore:  "GetMessage",
			wantResult: true,
		},
		{
			name:       "burst with message still processing returns true",
			te:         &turnExecution{pool: pool, burst: []db.Message{{ID: "m-proc"}}},
			wantResult: true,
		},
		{
			name:       "burst with message completed returns false",
			te:         &turnExecution{pool: pool, burst: []db.Message{{ID: "m-done"}}},
			wantResult: false,
		},
		{
			name:       "caller context canceled does not prevent check from returning false for completed dispatch",
			te:         &turnExecution{pool: pool, dispatch: &db.Dispatch{ID: "d-done"}},
			cancelCtx:  true,
			wantResult: false,
		},
		{
			name:       "caller context canceled does not prevent check from returning false for completed burst",
			te:         &turnExecution{pool: pool, burst: []db.Message{{ID: "m-done"}}},
			cancelCtx:  true,
			wantResult: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.failStore != "" {
				baseStore.FailNext(tc.failStore, errors.New("simulated db err"))
			}
			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			res := tc.te.isRowStillProcessing(ctx)
			assert.Equal(t, tc.wantResult, res)
		})
	}
}

func TestWorkerPool_CheckTurnCompletionError(t *testing.T) {
	baseStore := dbtest.NewFakeStore()
	_ = baseStore.InsertMessage(context.Background(), db.Message{ID: "m-done", Status: db.StatusCompleted})
	_ = baseStore.InsertMessage(context.Background(), db.Message{ID: "m-proc", Status: db.StatusProcessing})

	store := &cancelCheckStore{FakeStore: baseStore}
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	abortErr := errors.New("abort error")

	tests := []struct {
		name        string
		finalized   bool
		interrupted bool
		burst       []db.Message
		cancelCtx   bool
		wantErr     error
	}{
		{
			name:        "finalized and completed row returns nil even when context is canceled",
			finalized:   true,
			interrupted: false,
			burst:       []db.Message{{ID: "m-done"}},
			cancelCtx:   true,
			wantErr:     nil,
		},
		{
			name:        "finalized but row still processing returns abort error",
			finalized:   true,
			interrupted: false,
			burst:       []db.Message{{ID: "m-proc"}},
			cancelCtx:   false,
			wantErr:     abortErr,
		},
		{
			name:        "not finalized returns abort error",
			finalized:   false,
			interrupted: false,
			burst:       []db.Message{{ID: "m-done"}},
			cancelCtx:   false,
			wantErr:     abortErr,
		},
		{
			name:        "interrupted skips abort error",
			finalized:   false,
			interrupted: true,
			burst:       []db.Message{{ID: "m-proc"}},
			cancelCtx:   false,
			wantErr:     nil,
		},
		{
			name:        "interrupted skips caller context error",
			finalized:   false,
			interrupted: true,
			burst:       []db.Message{{ID: "m-done"}},
			cancelCtx:   true,
			wantErr:     nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			te := &turnExecution{
				pool:  pool,
				burst: tc.burst,
			}
			te.finalized.Store(tc.finalized)
			te.interrupted.Store(tc.interrupted)

			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err := pool.checkTurnCompletionError(ctx, te, abortErr)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
