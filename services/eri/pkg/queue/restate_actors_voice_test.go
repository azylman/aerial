package queue

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

type voiceAskTestCase struct {
	name         string
	deviceKey    string
	req          VoiceSessionRequest
	nilPool      bool
	sabotageSQL  string
	runnerReply  string
	runnerConvID string
	runnerErr    error
	wantReply    string
	wantErrorSub string
	wantCode     restate.Code
	// wantSession is the conversation persisted for the voice-<key> session ("" = no row).
	wantSession string
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
	// Only turns that reach the session rotation (a runner conversation ID) touch the store.
	var store db.Store
	var sqlStore *db.SQLStore
	if tc.runnerConvID != "" {
		var sqlDB *sql.DB
		sqlStore, sqlDB = newTestStore(t)
		execSQL(t, sqlDB, tc.sabotageSQL)
		store = sqlStore
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
	mc, state := setupActorMockContext(t, tc.deviceKey, "")
	require.NotNil(t, state)

	res, err := obj.Ask(restate.WithMockContext(mc), tc.req)
	if tc.wantErrorSub != "" {
		assertHandlerErrorCode(t, err, tc.wantErrorSub, tc.wantCode)
		return
	}

	require.NoError(t, err)
	assert.Equal(t, tc.wantReply, res.Reply)
	assert.Equal(t, tc.runnerConvID, res.ConversationID)
	assert.Equal(t, resolveEffectiveVoiceKey(tc.deviceKey, tc.req), res.DeviceKey)
	sessionID, err := sqlStore.GetSessionID(context.Background(), "voice-"+res.DeviceKey)
	require.NoError(t, err)
	assert.Equal(t, tc.wantSession, sessionID)
}

func TestVoiceSessionObject_Ask(t *testing.T) {
	t.Parallel()

	tests := []voiceAskTestCase{
		{
			name:         "empty device_key returns error",
			deviceKey:    "",
			req:          VoiceSessionRequest{Prompt: "hello"},
			wantErrorSub: "device_key is required",
			wantCode:     400,
		},
		{
			name:         "empty prompt returns error",
			deviceKey:    "dev-1",
			req:          VoiceSessionRequest{DeviceKey: "dev-1", Prompt: ""},
			wantErrorSub: "prompt is required",
			wantCode:     400,
		},
		{
			name:         "nil pool returns error",
			deviceKey:    "dev-2",
			req:          VoiceSessionRequest{DeviceKey: "dev-2", Prompt: "hello"},
			nilPool:      true,
			wantErrorSub: "worker pool is nil",
			wantCode:     500,
		},
		{
			name:         "fallback from empty key to session id",
			deviceKey:    "",
			req:          VoiceSessionRequest{SessionID: "sess-voice-dev-1", Prompt: "what is up?"},
			runnerReply:  "chilling",
			runnerConvID: "conv-1",
			wantReply:    "chilling",
			wantSession:  "conv-1",
		},
		{
			name:         "fallback from empty key to conversation id",
			deviceKey:    "",
			req:          VoiceSessionRequest{ConversationID: "conv-voice-dev-2", Prompt: "hey"},
			runnerReply:  "sup",
			runnerConvID: "conv-2",
			wantReply:    "sup",
			wantSession:  "conv-2",
		},
		{
			name:         "successful voice turn streams events and rotates session",
			deviceKey:    "kiosk-living-room",
			req:          VoiceSessionRequest{DeviceKey: "kiosk-living-room", Prompt: "what time is it?", TurnID: "turn-voice-1"},
			runnerReply:  "it is 6 PM",
			runnerConvID: "conv-voice-1",
			wantReply:    "it is 6 PM",
			wantSession:  "conv-voice-1",
		},
		{
			name:         "successful voice turn handles store rotate error gracefully",
			deviceKey:    "kiosk-bedroom",
			req:          VoiceSessionRequest{DeviceKey: "kiosk-bedroom", Prompt: "lights off"},
			sabotageSQL:  failTriggerSQL("sessions", "INSERT", "40001"),
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
			wantCode:     500,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceSessionAskCase(t, tc)
		})
	}
}
