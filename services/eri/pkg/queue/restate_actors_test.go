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

	"github.com/azylman/aerial/pkg/pubsub"
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

func TestServiceNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		agent         string
		wantNormalize string
		wantThreadSvc string
		wantDispSvc   string
		wantVoiceSvc  string
	}{
		{
			name:          "empty agent defaults to aerial canonical names",
			agent:         "",
			wantNormalize: "",
			wantThreadSvc: "ThreadSession",
			wantDispSvc:   "DispatchSession",
			wantVoiceSvc:  "VoiceSession",
		},
		{
			name:          "aerial bot id defaults to aerial canonical names",
			agent:         "aerial",
			wantNormalize: "aerial",
			wantThreadSvc: "ThreadSession",
			wantDispSvc:   "DispatchSession",
			wantVoiceSvc:  "VoiceSession",
		},
		{
			name:          "aerial bot id with spaces and caps",
			agent:         "  AERIAL  ",
			wantNormalize: "aerial",
			wantThreadSvc: "ThreadSession",
			wantDispSvc:   "DispatchSession",
			wantVoiceSvc:  "VoiceSession",
		},
		{
			name:          "miorine persona namespaced",
			agent:         "miorine",
			wantNormalize: "miorine",
			wantThreadSvc: "ThreadSession-miorine",
			wantDispSvc:   "DispatchSession-miorine",
			wantVoiceSvc:  "VoiceSession-miorine",
		},
		{
			name:          "suletta persona namespaced with whitespace",
			agent:         " Suletta ",
			wantNormalize: "suletta",
			wantThreadSvc: "ThreadSession-suletta",
			wantDispSvc:   "DispatchSession-suletta",
			wantVoiceSvc:  "VoiceSession-suletta",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantNormalize, NormalizeAgentName(tc.agent))
			assert.Equal(t, tc.wantThreadSvc, ThreadSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantDispSvc, DispatchSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantVoiceSvc, VoiceSessionServiceName(tc.agent))

			thObj := NewThreadSessionObject(nil, tc.agent)
			dispObj := NewDispatchSessionObject(nil, tc.agent)
			voiceObj := NewVoiceSessionObject(nil, tc.agent)

			assert.Equal(t, tc.wantThreadSvc, thObj.ServiceName())
			assert.Equal(t, tc.wantDispSvc, dispObj.ServiceName())
			assert.Equal(t, tc.wantVoiceSvc, voiceObj.ServiceName())
		})
	}

	assert.Equal(t, "ThreadSession", (*ThreadSessionObject)(nil).ServiceName())
	assert.Equal(t, "DispatchSession", (*DispatchSessionObject)(nil).ServiceName())
	assert.Equal(t, "VoiceSession", (*VoiceSessionObject)(nil).ServiceName())
}

func TestResolveDispatchSessionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		req        BrainDispatchRequest
		wantPrefix string
		wantExact  string
	}{
		{
			name:      "session_id takes priority",
			req:       BrainDispatchRequest{SessionID: "sess-abc", VanadisChunkID: "c-1", ID: "d-1"},
			wantExact: "sess-abc",
		},
		{
			name:      "vanadis_chunk_id used when session_id empty",
			req:       BrainDispatchRequest{VanadisChunkID: "chunk-xyz", ID: "d-2"},
			wantExact: "vanadis-chunk-xyz",
		},
		{
			name:      "dispatch id used when others empty",
			req:       BrainDispatchRequest{ID: "d-3"},
			wantExact: "dispatch-d-3",
		},
		{
			name:       "generates dispatch uuid when all empty",
			req:        BrainDispatchRequest{},
			wantPrefix: "dispatch-",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveDispatchSessionKey(tc.req)
			if tc.wantExact != "" {
				assert.Equal(t, tc.wantExact, got)
			}
			if tc.wantPrefix != "" {
				assert.True(t, strings.HasPrefix(got, tc.wantPrefix))
			}
		})
	}
}

func TestResolveActorPromptEffort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		model  string
		effort string
		want   string
	}{
		{name: "explicit effort takes priority", model: "gemini-flash", effort: config.EffortHigh, want: config.EffortHigh},
		{name: "medium model sets medium effort", model: "gemini-2.5-flash-medium", effort: "", want: config.EffortMedium},
		{name: "flash model sets low effort", model: "gemini-2.5-flash", effort: "", want: config.EffortLow},
		{name: "other model returns empty", model: "custom-model", effort: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveActorPromptEffort(tc.model, tc.effort)
			assert.Equal(t, tc.want, got)
		})
	}
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
	isAliasCall    bool
	wantErrorSub   string
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
	if tc.isAliasCall {
		resp, err = obj.EnqueuePrompt(sharedCtx, tc.req)
	} else {
		resp, err = obj.Enqueue(sharedCtx, tc.req)
	}

	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
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
		},
		{
			name:        "alias enqueue prompt accepted",
			threadID:    "th-300",
			req:         ThreadEnqueueRequest{Prompt: "alias prompt"},
			wantStatus:  "accepted",
			wantInsert:  true,
			isAliasCall: true,
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
			name:                "cancel with store failure",
			threadID:            "th-cancel-fail",
			pendingCount:        0,
			storeFail:           true,
			wantCancelledCount:  0,
			wantCancelledActive: false,
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

func TestThreadSessionObject_RotateSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		threadID    string
		activeInvID string
		req         RotateSessionRequest
		storeFail   bool
		nilPool     bool
	}{
		{
			name:        "rotate session with active invocation",
			threadID:    "th-rotate-1",
			activeInvID: "inv-active-rot",
			req:         RotateSessionRequest{Topic: "refactor"},
		},
		{
			name:        "rotate bare session",
			threadID:    "th-rotate-2",
			activeInvID: "",
			req:         RotateSessionRequest{Bare: true},
		},
		{
			name:        "rotate session handles db store failure gracefully",
			threadID:    "th-rotate-3",
			activeInvID: "",
			req:         RotateSessionRequest{},
			storeFail:   true,
		},
		{
			name:        "rotate session with nil pool",
			threadID:    "th-rotate-4",
			activeInvID: "",
			req:         RotateSessionRequest{},
			nilPool:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := dbtest.NewFakeStore()
			if tc.storeFail {
				store.FailNext("RotateSessionID", errors.New("db rotate err"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				pool = NewWorkerPool(WorkerPoolConfig{Store: store})
			}
			obj := NewThreadSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.threadID, tc.activeInvID)
			resp, err := obj.RotateSession(restate.WithMockContext(mc), tc.req)
			require.NoError(t, err)
			assert.Equal(t, "ok", resp.Status)
			assert.NotEmpty(t, resp.NewSessionID)

			if !tc.nilPool && !tc.storeFail {
				currentSess, _ := store.GetSessionID(context.Background(), tc.threadID)
				assert.Equal(t, resp.NewSessionID, currentSess)
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
				pool = NewWorkerPool(WorkerPoolConfig{Store: store})
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

	tests := []struct {
		name           string
		sessionKey     string
		req            BrainDispatchRequest
		activeInvID    string
		storeActive    bool
		storeActiveErr bool
		seedExisting   bool
		storeFail      bool
		nilPool        bool
		wantStatus     string
		wantErrorSub   string
	}{
		{
			name:       "normal dispatch enqueue accepted",
			sessionKey: "dispatch-100",
			req:        BrainDispatchRequest{ID: "d-100", Source: "vanadis", Prompt: "build chunk 1"},
			wantStatus: "accepted",
		},
		{
			name:       "empty key and empty id generates dispatch key and id",
			sessionKey: "",
			req:        BrainDispatchRequest{Prompt: "auto dispatch"},
			wantStatus: "accepted",
		},
		{
			name:         "already existing dispatch returns already_exists",
			sessionKey:   "dispatch-200",
			req:          BrainDispatchRequest{ID: "d-200", Source: "vanadis", Prompt: "build chunk 2"},
			seedExisting: true,
			wantStatus:   "already_exists",
		},
		{
			name:        "skip if active when invocation state active",
			sessionKey:  "dispatch-300",
			req:         BrainDispatchRequest{ID: "d-300", Source: "vanadis", Prompt: "build chunk 3", SkipIfActive: true},
			activeInvID: "inv-disp-300",
			wantStatus:  "skipped_active",
		},
		{
			name:        "skip if active when store has active dispatch turn",
			sessionKey:  "dispatch-400",
			req:         BrainDispatchRequest{ID: "d-400", Source: "vanadis", Prompt: "build chunk 4", SkipIfActive: true},
			storeActive: true,
			wantStatus:  "skipped_active",
		},
		{
			name:           "skip if active fails when store active check errors",
			sessionKey:     "dispatch-450",
			req:            BrainDispatchRequest{ID: "d-450", Source: "vanadis", Prompt: "skip err", SkipIfActive: true},
			storeActiveErr: true,
			wantErrorSub:   "active check error",
		},
		{
			name:         "store insert failure returns error",
			sessionKey:   "dispatch-500",
			req:          BrainDispatchRequest{ID: "d-500", Source: "vanadis", Prompt: "fail me"},
			storeFail:    true,
			wantErrorSub: "db error",
		},
		{
			name:       "nil pool accepted without store insertion",
			sessionKey: "dispatch-600",
			req:        BrainDispatchRequest{ID: "d-600", Prompt: "nil pool"},
			nilPool:    true,
			wantStatus: "accepted",
		},
		{
			name:       "skip if active with nil pool returns accepted",
			sessionKey: "dispatch-skip-nil",
			req:        BrainDispatchRequest{ID: "d-skip-nil", Prompt: "nil pool skip", SkipIfActive: true},
			nilPool:    true,
			wantStatus: "accepted",
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
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, resp.Status)
		})
	}
}

func TestDispatchSessionObject_RunTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sessionKey     string
		pendingCount   int
		storeErr       bool
		claimFail      bool
		nilPool        bool
		wantDisparches int
		wantErrorSub   string
	}{
		{
			name:           "no pending dispatches breaks immediately",
			sessionKey:     "dispatch-empty",
			pendingCount:   0,
			wantDisparches: 0,
		},
		{
			name:           "claims and drains matching pending dispatch",
			sessionKey:     "vanadis-chunk-1",
			pendingCount:   1,
			wantDisparches: 1,
		},
		{
			name:           "claim failure skips dispatch",
			sessionKey:     "dispatch-claim-fail",
			pendingCount:   1,
			claimFail:      true,
			wantDisparches: 0,
		},
		{
			name:           "nil pool breaks immediately",
			sessionKey:     "dispatch-nil",
			nilPool:        true,
			wantDisparches: 0,
		},
		{
			name:         "store get pending error returns error",
			sessionKey:   "dispatch-err",
			storeErr:     true,
			wantErrorSub: "disp read error",
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
			if tc.storeErr {
				store.FailNext("GetPendingOrProcessingDispatches", errors.New("disp read error"))
			}
			if tc.claimFail {
				store.FailNext("ClaimPendingDispatch", errors.New("claim disp fail"))
			}

			var pool *WorkerPool
			if !tc.nilPool {
				pool = NewWorkerPool(WorkerPoolConfig{Store: store})
			}
			obj := NewDispatchSessionObject(pool, "aerial")

			mc, _ := setupActorMockContext(t, tc.sessionKey, "")
			res, err := obj.RunTurn(restate.WithMockContext(mc), BrainDispatchRequest{VanadisChunkID: "chunk-1"})
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
			name:               "cancel with store failure handles gracefully",
			sessionKey:         "vanadis-fail",
			req:                CancelDispatchRequest{},
			storeFail:          true,
			wantCancelledCount: 0,
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
			require.NoError(t, err)
			assert.Equal(t, "ok", resp.Status)
			assert.Equal(t, tc.wantCancelledCount, resp.CancelledCount)
		})
	}
}

type voiceAskTestCase struct {
	name         string
	deviceKey    string
	req          VoiceSessionRequest
	nilPool      bool
	storeFail    bool
	runnerReply  string
	runnerConvID string
	runnerErr    error
	wantReply    string
	wantErrorSub string
}

func resolveEffectiveVoiceKey(deviceKey string, req VoiceSessionRequest) string {
	if deviceKey != "" {
		return deviceKey
	}
	if req.DeviceKey != "" {
		return req.DeviceKey
	}
	if req.SessionID != "" {
		return req.SessionID
	}
	return req.ConversationID
}

func runVoiceSessionAskCase(t *testing.T, tc voiceAskTestCase) {
	t.Helper()
	var pool *WorkerPool
	store := dbtest.NewFakeStore()
	if tc.storeFail {
		store.FailNext("RotateSessionID", errors.New("store voice rotate err"))
	}

	if !tc.nilPool {
		memBus := pubsub.NewMemoryBus()
		t.Cleanup(func() { _ = memBus.Close() })
		cfg := WorkerPoolConfig{
			Store:    store,
			EventBus: memBus,
			VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
				if tc.runnerErr != nil {
					return "", "", tc.runnerErr
				}
				if onStatus != nil {
					onStatus("thinking")
				}
				if onSentence != nil {
					onSentence("sentence text")
				}
				return tc.runnerReply, tc.runnerConvID, nil
			},
		}
		pool = NewWorkerPool(cfg)
	}

	obj := NewVoiceSessionObject(pool, "aerial")
	mc, _ := setupActorMockContext(t, tc.deviceKey, "")

	res, err := obj.Ask(restate.WithMockContext(mc), tc.req)
	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
		return
	}

	require.NoError(t, err)
	assert.Equal(t, tc.wantReply, res.Reply)
	assert.Equal(t, tc.runnerConvID, res.ConversationID)
	assert.Equal(t, resolveEffectiveVoiceKey(tc.deviceKey, tc.req), res.DeviceKey)
}

func TestVoiceSessionObject_Ask(t *testing.T) {
	t.Parallel()

	tests := []voiceAskTestCase{
		{
			name:         "empty device_key returns error",
			deviceKey:    "",
			req:          VoiceSessionRequest{Prompt: "hello"},
			wantErrorSub: "device_key is required",
		},
		{
			name:         "empty prompt returns error",
			deviceKey:    "dev-1",
			req:          VoiceSessionRequest{DeviceKey: "dev-1", Prompt: ""},
			wantErrorSub: "prompt is required",
		},
		{
			name:         "nil pool returns error",
			deviceKey:    "dev-2",
			req:          VoiceSessionRequest{DeviceKey: "dev-2", Prompt: "hello"},
			nilPool:      true,
			wantErrorSub: "worker pool is nil",
		},
		{
			name:         "fallback from empty key to session id",
			deviceKey:    "",
			req:          VoiceSessionRequest{SessionID: "sess-voice-dev-1", Prompt: "what is up?"},
			runnerReply:  "chilling",
			runnerConvID: "conv-1",
			wantReply:    "chilling",
		},
		{
			name:         "fallback from empty key to conversation id",
			deviceKey:    "",
			req:          VoiceSessionRequest{ConversationID: "conv-voice-dev-2", Prompt: "hey"},
			runnerReply:  "sup",
			runnerConvID: "conv-2",
			wantReply:    "sup",
		},
		{
			name:         "successful voice turn streams events and rotates session",
			deviceKey:    "kiosk-living-room",
			req:          VoiceSessionRequest{DeviceKey: "kiosk-living-room", Prompt: "what time is it?", TurnID: "turn-voice-1"},
			runnerReply:  "it is 6 PM",
			runnerConvID: "conv-voice-1",
			wantReply:    "it is 6 PM",
		},
		{
			name:         "successful voice turn handles store rotate error gracefully",
			deviceKey:    "kiosk-bedroom",
			req:          VoiceSessionRequest{DeviceKey: "kiosk-bedroom", Prompt: "lights off"},
			storeFail:    true,
			runnerReply:  "done",
			runnerConvID: "conv-voice-3",
			wantReply:    "done",
		},
		{
			name:         "runner failure streams error event and returns terminal error",
			deviceKey:    "kiosk-kitchen",
			req:          VoiceSessionRequest{DeviceKey: "kiosk-kitchen", Prompt: "bad turn", TurnID: "turn-voice-2"},
			runnerErr:    errors.New("voice backend failure"),
			wantErrorSub: "voice backend failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceSessionAskCase(t, tc)
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
	resp1, err1 := obj.Enqueue(sharedCtx, BrainDispatchRequest{Source: "vanadis", Prompt: "idempotent dispatch"})
	require.NoError(t, err1)
	assert.Equal(t, "accepted", resp1.Status)
	assert.NotEmpty(t, resp1.DispatchID)

	// Retry of the same Restate invocation (same mock context with same Request.ID)
	resp2, err2 := obj.Enqueue(sharedCtx, BrainDispatchRequest{Source: "vanadis", Prompt: "idempotent dispatch"})
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
	res, err := obj.RunTurn(restate.WithMockContext(mc), BrainDispatchRequest{VanadisChunkID: "target-chunk-1"})
	require.NoError(t, err)
	assert.Equal(t, "completed", res.Status)
	assert.Equal(t, 1, res.DispatchesExecuted)
}
