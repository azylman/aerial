package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

var defaultFunnelMemberFetcher = funnelMemberFetcher

func init() {
	discordSessionOpener = func(dg *discordgo.Session) error {
		return nil
	}
	funnelMemberFetcher = func(s *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
		return &discordgo.Member{
			GuildID: guildID,
			User:    &discordgo.User{ID: userID},
			Roles:   []string{"mock-role-bot-123"},
		}, nil
	}
}

func setDiscordSessionOpenerForTest(t *testing.T, fn func(*discordgo.Session) error) {
	t.Helper()
	old := discordSessionOpener
	discordSessionOpener = fn
	t.Cleanup(func() {
		discordSessionOpener = old
	})
}

// newTestWorkerPool instantiates a fully isolated WorkerPool with mocked runner, notifier, and delivery hooks.
func newTestWorkerPool(dbOrStore any) *queue.WorkerPool {
	var s db.Store
	if dbOrStore != nil {
		switch v := dbOrStore.(type) {
		case db.Store:
			s = v
		case *sql.DB:
			s = db.NewSQLStore(v)
		}
	}
	return queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: s,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return `{"event":"result","result":{"status":"SUCCESS","response":"mock test response"}}`, "", 0, nil
		},
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(status string)) (string, string, error) {
			return "mock voice response", sessionID, nil
		},
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(status string), onSentence func(sentence string)) (string, string, error) {
			if onSentence != nil {
				onSentence("mock voice response")
			}
			return "mock voice response", sessionID, nil
		},
		NotifierFunc: func(agyBin, apiKey, contextDescription string) string {
			return "mock notification"
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			return nil
		},
		TypingFunc: func(s *discordgo.Session, channelID string) func() {
			return func() {}
		},
	})
}

func setupTestConfig(t *testing.T, yamlContent string) {
	t.Helper()
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test config.yaml: %v", err)
	}
	cfg, err := config.LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("Failed to load test config from %s: %v", yamlPath, err)
	}
	SetFunnelConfig(cfg)
	t.Cleanup(func() {
		SetFunnelConfig(nil)
	})
}

func resetFunnelGlobals(t *testing.T) {
	t.Helper()
	sweepMu.Lock()
	lastSweepAt = time.Time{}
	isSweeping.Store(false)
	sweepMu.Unlock()
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type mockCatchUpRoundTripper = roundTripperFunc

func newMockDiscordSession(t *testing.T, rt roundTripperFunc, stateChannels ...*discordgo.Channel) *discordgo.Session {
	t.Helper()
	s, err := discordgo.New("Bot test-token")
	if err != nil {
		t.Fatalf("failed to create discordgo session: %v", err)
	}
	s.StateEnabled = true
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-123", Username: "Aerial"}
	for _, ch := range stateChannels {
		if ch != nil {
			_ = s.State.ChannelAdd(ch)
		}
	}
	if rt != nil {
		s.Client = &http.Client{Transport: rt}
	}
	return s
}

func jsonHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func dispatchSessionEvent(s *discordgo.Session, event interface{}) {
	sVal := reflect.ValueOf(s).Elem()
	handlersField := sVal.FieldByName("handlers")
	hMapVal := reflect.NewAt(handlersField.Type(), unsafe.Pointer(handlersField.UnsafeAddr())).Elem()
	iter := hMapVal.MapRange()
	for iter.Next() {
		sliceVal := iter.Value()
		for i := 0; i < sliceVal.Len(); i++ {
			elem := sliceVal.Index(i).Elem()
			ehField := elem.FieldByName("eventHandler")
			ehVal := reflect.NewAt(ehField.Type(), unsafe.Pointer(ehField.UnsafeAddr())).Elem()
			if eh, ok := ehVal.Interface().(discordgo.EventHandler); ok {
				eh.Handle(s, event)
			}
		}
	}
}

type errExistsStore struct {
	db.Store
}

func (e *errExistsStore) MessageExists(ctx context.Context, id string) (bool, error) {
	return false, errors.New("simulated message exists error")
}

func TestFunnelHelpers(t *testing.T) {
	yamlContent := `
model: "gemini-2.5-flash"
admin_users:
  - "user-admin"
channels:
  default:
    mode: "threads"
`
	setupTestConfig(t, yamlContent)

	s := newMockDiscordSession(t, nil)

	if _, err := defaultFunnelMemberFetcher(nil, "g1", "u1"); err == nil {
		t.Errorf("expected error for nil session")
	}

	dmMsg := &discordgo.Message{
		ID:        "msg-1",
		ChannelID: "chan-dm",
		GuildID:   "",
		Content:   "Hello Aerial",
		Timestamp: time.Now(),
		Author: &discordgo.User{
			ID:       "user-1",
			Username: "testuser",
			Bot:      false,
		},
	}

	targetThreadID, isThread := getOrCreateThreadID(s, dmMsg)
	if targetThreadID != "chan-dm" || isThread {
		t.Errorf("Expected DM targetThreadID chan-dm and false, got: %s, %t", targetThreadID, isThread)
	}

	mCreate := &discordgo.MessageCreate{Message: dmMsg}
	if !isFunnelBotTargeted(s, mCreate) {
		t.Errorf("Expected DM message to be bot targeted")
	}
}

func TestIsFunnelBotTargeted(t *testing.T) {
	yamlContent := `
model: "gemini-2.5-flash"
admin_users:
  - "admin-123"
channels:
  default:
    mode: "threads"
    ignore_bots: false
  general:
    mode: "channel"
    ignore_bots: true
  bot-lab:
    mode: "channel"
    ignore_bots: false
  spam:
    mode: "ignore"
  "111999":
    mode: "ignore"
  muted-room:
    mode: "ignore"
  disabled-room:
    mode: "disabled"
  aerial-dev:
    mode: "threads"
`
	setupTestConfig(t, yamlContent)

	s := newMockDiscordSession(t, nil)
	s.State.User = &discordgo.User{ID: "bot-self-id", Username: "AerialBot"}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	channels := []*discordgo.Channel{
		{ID: "chan-general", GuildID: "guild-1", Name: "general", Type: discordgo.ChannelTypeGuildText},
		{ID: "chan-bot-lab", GuildID: "guild-1", Name: "bot-lab", Type: discordgo.ChannelTypeGuildText},
		{ID: "chan-threads", GuildID: "guild-1", Name: "threads-chan", Type: discordgo.ChannelTypeGuildText},
		{ID: "chan-spam", GuildID: "guild-1", Name: "spam", Type: discordgo.ChannelTypeGuildText},
		{ID: "chan-muted", GuildID: "guild-1", Name: "muted-room", Type: discordgo.ChannelTypeGuildText},
		{ID: "111999", GuildID: "guild-1", Name: "random-snowflake", Type: discordgo.ChannelTypeGuildText},
		{ID: "chan-allowed", GuildID: "guild-1", Name: "general", Type: discordgo.ChannelTypeGuildText},
		{ID: "chan-dev-101", GuildID: "guild-1", Name: "aerial-dev", Type: discordgo.ChannelTypeGuildText},
		{ID: "thread-dev-999", GuildID: "guild-1", ParentID: "chan-dev-101", Name: "Discussion on Features", Type: discordgo.ChannelTypeGuildPublicThread},
		{ID: "thread-spam-888", GuildID: "guild-1", ParentID: "chan-spam", Name: "Spam Discussion Thread", Type: discordgo.ChannelTypeGuildPublicThread},
		{ID: "thread-sub-1", GuildID: "guild-1", ParentID: "chan-threads", Name: "sub-discussion", Type: discordgo.ChannelTypeGuildPublicThread},
	}
	for _, ch := range channels {
		_ = s.State.ChannelAdd(ch)
	}

	tests := []struct {
		name     string
		msg      *discordgo.MessageCreate
		expected bool
	}{
		{
			name:     "nil message create",
			msg:      nil,
			expected: false,
		},
		{
			name:     "nil inner message",
			msg:      &discordgo.MessageCreate{Message: nil},
			expected: false,
		},
		{
			name:     "nil author",
			msg:      &discordgo.MessageCreate{Message: &discordgo.Message{Author: nil}},
			expected: false,
		},
		{
			name: "bot self message",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-self",
					ChannelID: "chan-general",
					GuildID:   "guild-1",
					Content:   "I am responding",
					Author:    &discordgo.User{ID: "bot-self-id", Username: "AerialBot", Bot: true},
				},
			},
			expected: false,
		},
		{
			name: "channel mode user ambient",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-amb",
					ChannelID: "chan-general",
					GuildID:   "guild-1",
					Content:   "Just chatting about weather",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "channel mode ignore_bots true with bot",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-bot-ign",
					ChannelID: "chan-general",
					GuildID:   "guild-1",
					Content:   "Automated CI alert",
					Author:    &discordgo.User{ID: "other-bot-id", Username: "CiBot", Bot: true},
				},
			},
			expected: false,
		},
		{
			name: "channel mode ignore_bots false with bot",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-bot-allowed",
					ChannelID: "chan-bot-lab",
					GuildID:   "guild-1",
					Content:   "Bot to bot ping",
					Author:    &discordgo.User{ID: "other-bot-id", Username: "CiBot", Bot: true},
				},
			},
			expected: true,
		},
		{
			name: "threads mode ambient without triggers",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-thr-amb",
					ChannelID: "chan-threads",
					GuildID:   "guild-1",
					Content:   "Hey everyone what is up",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: false,
		},
		{
			name: "threads mode with aerial keyword",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-kw-1",
					ChannelID: "chan-threads",
					GuildID:   "guild-1",
					Content:   "Hey aerial can you help?",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "threads mode with gundam keyword",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-kw-2",
					ChannelID: "chan-threads",
					GuildID:   "guild-1",
					Content:   "Launch the gundam units",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "threads mode with brain keyword",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-kw-3",
					ChannelID: "chan-threads",
					GuildID:   "guild-1",
					Content:   "Query the brain state",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "threads mode with bot keyword",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-kw-4",
					ChannelID: "chan-threads",
					GuildID:   "guild-1",
					Content:   "Good bot thanks",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "threads mode with mention",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-mention",
					ChannelID: "chan-threads",
					GuildID:   "guild-1",
					Content:   "Check this out",
					Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "threads mode inside active thread",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-in-thread",
					ChannelID: "thread-sub-1",
					GuildID:   "guild-1",
					Content:   "Continuing discussion in thread",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "ignored channel with mention",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-spam-1",
					ChannelID: "chan-spam",
					GuildID:   "guild-1",
					Content:   "Hey aerial help me here",
					Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: false,
		},
		{
			name: "mode ignore channel",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-muted-1",
					ChannelID: "chan-muted",
					GuildID:   "guild-1",
					Content:   "Hey aerial help me here",
					Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: false,
		},
		{
			name: "snowflake ignored channel",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-snow-1",
					ChannelID: "111999",
					GuildID:   "guild-1",
					Content:   "Hey aerial help me here",
					Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: false,
		},
		{
			name: "allowed channel with mention",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-allowed-1",
					ChannelID: "chan-allowed",
					GuildID:   "guild-1",
					Content:   "Hey aerial help me here",
					Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "thread inherits whitelisted parent",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-dev-thread-1",
					ChannelID: "thread-dev-999",
					GuildID:   "guild-1",
					Content:   "Continuing discussion in whitelisted thread",
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: true,
		},
		{
			name: "thread inherits ignored parent",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-spam-thread-1",
					ChannelID: "thread-spam-888",
					GuildID:   "guild-1",
					Content:   "Hey aerial answer me in spam thread",
					Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
					Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
				},
			},
			expected: false,
		},
		{
			name: "message with ReferencedMessage",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:                "msg-ref-1",
					ChannelID:         "chan-threads",
					GuildID:           "guild-1",
					Content:           "simple discussion without keyword",
					Author:            &discordgo.User{ID: "user-ref-1", Username: "user1"},
					ReferencedMessage: &discordgo.Message{ID: "ref-0"},
				},
			},
			expected: true,
		},
		{
			name: "message with MessageReference",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:               "msg-ref-2",
					ChannelID:        "chan-threads",
					GuildID:          "guild-1",
					Content:          "simple discussion with message reference",
					Author:           &discordgo.User{ID: "user-ref-2", Username: "user2"},
					MessageReference: &discordgo.MessageReference{MessageID: "ref-0"},
				},
			},
			expected: true,
		},
		{
			name: "message with MentionRoles",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:           "msg-role-1",
					ChannelID:    "chan-threads",
					GuildID:      "guild-1",
					Content:      "hey team ping",
					Author:       &discordgo.User{ID: "user-role-1", Username: "user1"},
					MentionRoles: []string{"mock-role-bot-123"},
				},
			},
			expected: true,
		},
		{
			name: "direct message channel without guild",
			msg: &discordgo.MessageCreate{
				Message: &discordgo.Message{
					ID:        "msg-dm-1",
					ChannelID: "chan-dm-1",
					GuildID:   "",
					Content:   "Hello in private",
					Author:    &discordgo.User{ID: "user-dm-1", Username: "alice"},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isFunnelBotTargeted(s, tt.msg)
			if got != tt.expected {
				t.Errorf("isFunnelBotTargeted() = %v; want %v", got, tt.expected)
			}
		})
	}
}

func TestGetDiscordChannel(t *testing.T) {
	if ch := getDiscordChannel(nil, ""); ch != nil {
		t.Errorf("Expected nil for empty channel ID, got %v", ch)
	}

	chanID := "100200300400500802"
	queue.InvalidateChannelCache(chanID)
	defer queue.InvalidateChannelCache(chanID)

	s := newMockDiscordSession(t, nil)
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	chState := &discordgo.Channel{
		ID:      chanID,
		Name:    "state-cached-channel",
		Type:    discordgo.ChannelTypeGuildText,
		GuildID: "guild-1",
	}
	_ = s.State.ChannelAdd(chState)

	ch := getDiscordChannel(s, chanID)
	if ch == nil || ch.Name != "state-cached-channel" {
		t.Fatalf("Expected channel from state, got: %+v", ch)
	}

	snap, ok := queue.GetCachedChannel(chanID)
	if !ok || snap.Name != "state-cached-channel" {
		t.Fatalf("Expected channel to be cached into queue cache, got: %+v", snap)
	}

	_ = s.State.ChannelRemove(chState)
	chCached := getDiscordChannel(s, chanID)
	if chCached == nil || chCached.Name != "state-cached-channel" {
		t.Fatalf("Expected reconstructed channel from queue cache, got: %+v", chCached)
	}

	queue.InvalidateChannelCache("snap-th-100")
	defer queue.InvalidateChannelCache("snap-th-100")
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:       "snap-th-100",
		Name:     "snap-thread",
		GuildID:  "guild-1",
		ParentID: chanID,
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	chThread := getDiscordChannel(nil, "snap-th-100")
	if chThread == nil || chThread.Type != discordgo.ChannelTypeGuildPublicThread {
		t.Errorf("Expected public thread channel from cache, got %+v", chThread)
	}

	queue.InvalidateChannelCache(chanID)
	if chNone := getDiscordChannel(s, chanID); chNone != nil {
		t.Errorf("Expected nil when cache and state are empty, got: %+v", chNone)
	}

	sRest := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		restCh := &discordgo.Channel{
			ID:   chanID,
			Name: "rest-fetched-channel",
			Type: discordgo.ChannelTypeGuildText,
		}
		data, _ := json.Marshal(restCh)
		return jsonHTTPResponse(http.StatusOK, string(data)), nil
	}))
	chRest := getDiscordChannel(sRest, chanID)
	if chRest == nil || chRest.Name != "rest-fetched-channel" {
		t.Fatalf("Expected channel fetched via REST, got: %+v", chRest)
	}
	if snap, ok := queue.GetCachedChannel(chanID); !ok || snap.Name != "rest-fetched-channel" {
		t.Fatalf("Expected REST-fetched channel to be in queue cache, got: %+v", snap)
	}
}

func TestGetOrCreateThreadID_ModesAndCaching(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
admin_users:
  - "admin-123"
channels:
  default:
    mode: "threads"
  general:
    mode: "channel"
  "111222333":
    mode: "channel"
`)

	s := newMockDiscordSession(t, nil)
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	if thID, isTh := getOrCreateThreadID(s, nil); thID != "" || isTh {
		t.Errorf("Expected empty string and false for nil message, got %s, %t", thID, isTh)
	}

	dmMsg := &discordgo.Message{
		ID:        "msg-dm",
		ChannelID: "chan-dm",
		GuildID:   "",
		Content:   "Hello Aerial",
		Author:    &discordgo.User{ID: "user-1"},
	}
	if thID, isTh := getOrCreateThreadID(s, dmMsg); thID != "chan-dm" || isTh {
		t.Errorf("Expected DM to return chan-dm and false, got %s, %t", thID, isTh)
	}

	chanGeneral := &discordgo.Channel{
		ID:      "111222333",
		GuildID: "guild-1",
		Name:    "general",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = s.State.ChannelAdd(chanGeneral)

	msgGeneral := &discordgo.Message{
		ID:        "msg-general-1",
		ChannelID: "111222333",
		GuildID:   "guild-1",
		Content:   "Hello in general",
		Author:    &discordgo.User{ID: "user-1", Username: "alice"},
	}
	if thID, isTh := getOrCreateThreadID(s, msgGeneral); thID != "111222333" || isTh {
		t.Errorf("Expected channel mode to return 111222333 and false, got %s, %t", thID, isTh)
	}

	threadChan := &discordgo.Channel{
		ID:       "thread-444",
		GuildID:  "guild-1",
		ParentID: "111222333",
		Name:     "discussion-thread",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	_ = s.State.ChannelAdd(threadChan)

	msgInThread := &discordgo.Message{
		ID:        "msg-thread-1",
		ChannelID: "thread-444",
		GuildID:   "guild-1",
		Content:   "Follow up in thread",
		Author:    &discordgo.User{ID: "user-1", Username: "alice"},
	}
	if thID, isTh := getOrCreateThreadID(s, msgInThread); thID != "thread-444" || !isTh {
		t.Errorf("Expected existing thread to return thread-444 and true, got %s, %t", thID, isTh)
	}

	threadID1 := "th-cached-101"
	queue.InvalidateChannelCache(threadID1)
	defer queue.InvalidateChannelCache(threadID1)
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID1,
		GuildID:  "guild-1",
		ParentID: "chan-threads-555",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	mCacheHit := &discordgo.Message{
		ID:        threadID1,
		ChannelID: "chan-threads-555",
		GuildID:   "guild-1",
	}
	if thID, isTh := getOrCreateThreadID(s, mCacheHit); thID != threadID1 || !isTh {
		t.Errorf("Expected in-memory cache hit (%s, true), got (%s, %t)", threadID1, thID, isTh)
	}

	threadID2 := "th-state-102"
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID:       threadID2,
		GuildID:  "guild-1",
		ParentID: "chan-threads-555",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	mStateHit := &discordgo.Message{
		ID:        threadID2,
		ChannelID: "chan-threads-555",
		GuildID:   "guild-1",
	}
	if thID, isTh := getOrCreateThreadID(s, mStateHit); thID != threadID2 || !isTh {
		t.Errorf("Expected session state hit (%s, true), got (%s, %t)", threadID2, thID, isTh)
	}

	sNoToken := &discordgo.Session{State: discordgo.NewState()}
	_ = sNoToken.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})
	chanDev := &discordgo.Channel{
		ID:      "chan-threads-555",
		GuildID: "guild-1",
		Name:    "dev-chat",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = sNoToken.State.ChannelAdd(chanDev)
	msgDev := &discordgo.Message{
		ID:        "msg-dev-1",
		ChannelID: "chan-threads-555",
		GuildID:   "guild-1",
		Content:   "Start a new thread please",
		Author:    &discordgo.User{ID: "user-1", Username: "alice"},
	}
	if thID, isTh := getOrCreateThreadID(sNoToken, msgDev); thID != "chan-threads-555" || isTh {
		t.Errorf("Expected threads mode without active REST to return chan-threads-555 and false, got %s, %t", thID, isTh)
	}

	thID, _ := getOrCreateThreadID(sNoToken, msgDev, false)
	if thID != "chan-threads-555" {
		t.Errorf("expected chan-threads-555 with allowSummarize=false")
	}

	clsErr := classifier.New(currentFunnelConfig(), func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		return "", "", 1, errors.New("summarizer forced failure")
	})
	store := db.NewFakeStore()
	pool := queue.New(currentFunnelConfig(), queue.WorkerPoolConfig{
		Classifier: clsErr,
		Store:      store,
	})
	SetFunnelPool(pool)
	defer SetFunnelPool(nil)

	thIDFail, _ := getOrCreateThreadID(sNoToken, msgDev, true)
	if thIDFail != "chan-threads-555" {
		t.Errorf("expected chan-threads-555 on summarizer failure")
	}

	titleSummarizeSem <- struct{}{}
	titleSummarizeSem <- struct{}{}
	defer func() {
		<-titleSummarizeSem
		<-titleSummarizeSem
	}()
	thID2, _ := getOrCreateThreadID(sNoToken, msgDev, true)
	if thID2 != "chan-threads-555" {
		t.Errorf("expected chan-threads-555 when semaphore saturated")
	}
}

func TestGetOrCreateThreadID_RESTCreationAnd160004Recovery(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)

	guildID := "g-rest-1"
	parentChanID := "ch-rest-parent"

	tests := []struct {
		name         string
		messageID    string
		content      string
		rt           roundTripperFunc
		wantThreadID string
		wantIsThread bool
		checkCache   bool
		wantParentID string
	}{
		{
			name:      "200 thread created successfully",
			messageID: "msg-200-ok",
			content:   "deploy service",
			rt: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/threads") {
					respBody, _ := json.Marshal(&discordgo.Channel{
						ID:       "th-created-200",
						GuildID:  guildID,
						ParentID: parentChanID,
						Name:     "deploy-service",
						Type:     discordgo.ChannelTypeGuildPublicThread,
					})
					return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
				}
				return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
			},
			wantThreadID: "th-created-200",
			wantIsThread: true,
			checkCache:   true,
			wantParentID: parentChanID,
		},
		{
			name:      "200 thread created with missing parent and guild",
			messageID: "msg-200-empty-parent",
			content:   "start missing parent thread",
			rt: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/threads") {
					respBody, _ := json.Marshal(&discordgo.Channel{
						ID:   "th-created-empty-parent",
						Type: discordgo.ChannelTypeGuildPublicThread,
					})
					return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
				}
				return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
			},
			wantThreadID: "th-created-empty-parent",
			wantIsThread: true,
			checkCache:   true,
			wantParentID: parentChanID,
		},
		{
			name:      "160004 thread already exists with channel re-fetch success",
			messageID: "200300400500600701",
			content:   "test 160004 refetch",
			rt: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/threads") {
					return jsonHTTPResponse(http.StatusBadRequest, `{"code": 160004, "message": "Thread already exists"}`), nil
				}
				if strings.Contains(req.URL.Path, "/channels/200300400500600701") {
					respBody, _ := json.Marshal(&discordgo.Channel{
						ID:       "200300400500600701",
						GuildID:  guildID,
						ParentID: parentChanID,
						Type:     discordgo.ChannelTypeGuildPublicThread,
					})
					return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
				}
				return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
			},
			wantThreadID: "200300400500600701",
			wantIsThread: true,
			checkCache:   true,
			wantParentID: parentChanID,
		},
		{
			name:      "160004 thread already exists with channel error fallback",
			messageID: "200300400500600702",
			content:   "test 160004 fallback",
			rt: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/threads") {
					return jsonHTTPResponse(http.StatusBadRequest, `{"code": 160004, "message": "Thread already exists"}`), nil
				}
				return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
			},
			wantThreadID: "200300400500600702",
			wantIsThread: true,
			checkCache:   true,
			wantParentID: parentChanID,
		},
		{
			name:      "160004 non-numeric message ID falls back without recovery",
			messageID: "not-a-numeric-snowflake",
			content:   "test non-numeric",
			rt: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/threads") {
					return jsonHTTPResponse(http.StatusBadRequest, `{"code": 160004, "message": "Thread already exists"}`), nil
				}
				return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
			},
			wantThreadID: parentChanID,
			wantIsThread: false,
			checkCache:   false,
		},
		{
			name:      "REST forbidden 50001 error fallback",
			messageID: "msg-forbidden-1",
			content:   "test forbidden",
			rt: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/threads") {
					return jsonHTTPResponse(http.StatusForbidden, `{"code": 50001, "message": "Missing Access"}`), nil
				}
				return jsonHTTPResponse(http.StatusOK, "{}"), nil
			},
			wantThreadID: parentChanID,
			wantIsThread: false,
			checkCache:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue.InvalidateChannelCache(tt.messageID)
			defer queue.InvalidateChannelCache(tt.messageID)
			if tt.wantThreadID != parentChanID {
				queue.InvalidateChannelCache(tt.wantThreadID)
				defer queue.InvalidateChannelCache(tt.wantThreadID)
			}

			s := newMockDiscordSession(t, tt.rt)
			_ = s.State.GuildAdd(&discordgo.Guild{ID: guildID})
			_ = s.State.ChannelAdd(&discordgo.Channel{
				ID:      parentChanID,
				GuildID: guildID,
				Name:    "feature-requests",
				Type:    discordgo.ChannelTypeGuildText,
			})

			m := &discordgo.Message{
				ID:        tt.messageID,
				ChannelID: parentChanID,
				GuildID:   guildID,
				Content:   tt.content,
				Author:    &discordgo.User{ID: "user-42", Username: "alex"},
			}

			targetID, isThread := getOrCreateThreadID(s, m)
			if targetID != tt.wantThreadID || isThread != tt.wantIsThread {
				t.Fatalf("getOrCreateThreadID() = (%q, %t); want (%q, %t)", targetID, isThread, tt.wantThreadID, tt.wantIsThread)
			}

			if tt.checkCache {
				snap, ok := queue.GetCachedChannel(tt.wantThreadID)
				if !ok {
					t.Fatalf("Expected thread %q to be in queue cache", tt.wantThreadID)
				}
				if !snap.IsThread || snap.ParentID != tt.wantParentID {
					t.Errorf("Unexpected cached thread metadata: %+v", snap)
				}
			}
		})
	}
}

func TestConnectDiscordFunnel_OpenerRetry(t *testing.T) {
	oldBackoff := funnelRetryBackoff
	funnelRetryBackoff = 2 * time.Millisecond
	defer func() { funnelRetryBackoff = oldBackoff }()

	attempts := 0
	done := make(chan struct{})
	setDiscordSessionOpenerForTest(t, func(dg *discordgo.Session) error {
		attempts++
		if attempts == 1 {
			return errors.New("first open error")
		}
		if attempts == 2 {
			return errors.New("second open error")
		}
		select {
		case <-done:
		default:
			close(done)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := connectDiscordFunnel(ctx, nil, nil, "mock-token-retry")
	if s == nil {
		t.Fatal("Expected non-nil session")
	}
	defer func() { _ = s.Close() }()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed out waiting for retry loop")
	}
}

func TestConnectDiscordFunnel_OpenerContextCanceled(t *testing.T) {
	setDiscordSessionOpenerForTest(t, func(dg *discordgo.Session) error {
		return errors.New("mock open error")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := connectDiscordFunnel(ctx, nil, nil, "mock-token-ctx-cancel")
	if s == nil {
		t.Fatal("Expected non-nil session")
	}
	_ = s.Close()
}

func TestConnectDiscordFunnel_EventHandlers_And_Lifecycle(t *testing.T) {
	if dg := connectDiscordFunnel(nil, nil, nil, ""); dg != nil {
		t.Errorf("Expected nil session for empty token, got %v", dg)
	}

	cancelCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	if dg := connectDiscordFunnel(cancelCtx, nil, nil, ""); dg != nil {
		t.Errorf("Expected nil session for empty token with canceled ctx, got %v", dg)
	}

	setupTestConfig(t, `
model: "gemini-2.5-flash"
admin_users:
  - "user-admin"
channels:
  default:
    mode: "channel"
`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := db.NewFakeStore()

	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	s := connectDiscordFunnel(ctx, store, pool, "mock-valid-token-for-handlers")
	if s == nil {
		t.Fatalf("Expected non-nil discordgo.Session")
	}
	defer func() { _ = s.Close() }()

	s.State.User = &discordgo.User{ID: "bot-aerial-id", Username: "AerialBot"}

	// 1. Ready event
	g1 := &discordgo.Guild{
		ID: "g-ready-1",
		Channels: []*discordgo.Channel{
			{ID: "ch-ready-1", Name: "ready-chan", GuildID: "g-ready-1", Type: discordgo.ChannelTypeGuildText},
		},
		Threads: []*discordgo.Channel{
			{ID: "th-ready-1", Name: "ready-thread", GuildID: "g-ready-1", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	}
	_ = s.State.GuildAdd(g1)
	sNilCtx := connectDiscordFunnel(nil, store, pool, "mock-token-nil-ctx")
	if sNilCtx != nil {
		_ = sNilCtx.Close()
	}

	dispatchSessionEvent(s, &discordgo.InteractionCreate{})
	dispatchSessionEvent(s, &discordgo.GuildCreate{Guild: nil})
	dispatchSessionEvent(s, &discordgo.ChannelCreate{Channel: nil})

	dispatchSessionEvent(s, &discordgo.Ready{
		User: &discordgo.User{ID: "bot-aerial-id", Username: "AerialBot", Discriminator: "0001"},
	})

	if _, ok := queue.GetCachedChannel("ch-ready-1"); !ok {
		t.Errorf("Expected ch-ready-1 to be cached on Ready")
	}
	if _, ok := queue.GetCachedChannel("th-ready-1"); !ok {
		t.Errorf("Expected th-ready-1 to be cached on Ready")
	}

	// 2. GuildCreate event
	g2 := &discordgo.Guild{
		ID: "g-create-2",
		Channels: []*discordgo.Channel{
			{ID: "ch-create-2", Name: "gc-chan", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildText},
		},
		Threads: []*discordgo.Channel{
			{ID: "th-create-2", Name: "gc-thread", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	}
	_ = s.State.GuildAdd(g2)
	dispatchSessionEvent(s, &discordgo.GuildCreate{Guild: g2})
	if _, ok := queue.GetCachedChannel("ch-create-2"); !ok {
		t.Errorf("Expected ch-create-2 to be cached on GuildCreate")
	}
	if _, ok := queue.GetCachedChannel("th-create-2"); !ok {
		t.Errorf("Expected th-create-2 to be cached on GuildCreate")
	}

	// 3. ChannelCreate / Update / Delete
	ch3 := &discordgo.Channel{ID: "ch-crud-3", Name: "crud-chan", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildText}
	dispatchSessionEvent(s, &discordgo.ChannelCreate{Channel: ch3})
	if snap, ok := queue.GetCachedChannel("ch-crud-3"); !ok || snap.Name != "crud-chan" {
		t.Errorf("Expected ch-crud-3 cached on ChannelCreate")
	}

	ch3Updated := &discordgo.Channel{ID: "ch-crud-3", Name: "crud-chan-updated", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildText}
	dispatchSessionEvent(s, &discordgo.ChannelUpdate{Channel: ch3Updated})
	if snap, ok := queue.GetCachedChannel("ch-crud-3"); !ok || snap.Name != "crud-chan-updated" {
		t.Errorf("Expected ch-crud-3 updated in cache on ChannelUpdate")
	}

	dispatchSessionEvent(s, &discordgo.ChannelDelete{Channel: ch3})
	if _, ok := queue.GetCachedChannel("ch-crud-3"); ok {
		t.Errorf("Expected ch-crud-3 removed on ChannelDelete")
	}

	// 4. ThreadCreate / Update / Delete
	th4 := &discordgo.Channel{ID: "th-crud-4", Name: "crud-thread", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildPublicThread}
	dispatchSessionEvent(s, &discordgo.ThreadCreate{Channel: th4})
	if snap, ok := queue.GetCachedChannel("th-crud-4"); !ok || snap.Name != "crud-thread" {
		t.Errorf("Expected th-crud-4 cached on ThreadCreate")
	}

	th4Updated := &discordgo.Channel{ID: "th-crud-4", Name: "crud-thread-updated", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildPublicThread}
	dispatchSessionEvent(s, &discordgo.ThreadUpdate{Channel: th4Updated})
	if snap, ok := queue.GetCachedChannel("th-crud-4"); !ok || snap.Name != "crud-thread-updated" {
		t.Errorf("Expected th-crud-4 updated on ThreadUpdate")
	}

	dispatchSessionEvent(s, &discordgo.ThreadDelete{Channel: th4})
	if _, ok := queue.GetCachedChannel("th-crud-4"); ok {
		t.Errorf("Expected th-crud-4 removed on ThreadDelete")
	}

	// 5. Disconnect and Resumed
	dispatchSessionEvent(s, &discordgo.Disconnect{})
	dispatchSessionEvent(s, &discordgo.Resumed{})

	// 5b. GuildMemberUpdate
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "g-member-update-3"})
	dispatchSessionEvent(s, &discordgo.GuildMemberUpdate{
		Member: &discordgo.Member{
			GuildID: "g-member-update-3",
			User:    &discordgo.User{ID: "bot-aerial-id"},
			Roles:   []string{"mock-role-robot-123"},
		},
	})
	mUpdated, err := s.State.Member("g-member-update-3", "bot-aerial-id")
	if err != nil || len(mUpdated.Roles) != 1 || mUpdated.Roles[0] != "mock-role-robot-123" {
		t.Errorf("Expected bot member roles updated on GuildMemberUpdate, got %v (err: %v)", mUpdated, err)
	}

	// 6. MessageCreate: Self Bot Message (Ignored)
	dispatchSessionEvent(s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-self-1",
			ChannelID: "ch-create-2",
			Author:    &discordgo.User{ID: "bot-aerial-id", Username: "AerialBot"},
			Content:   "Self message",
		},
	})

	// 7. MessageCreate: Targeted message in channel mode
	dispatchSessionEvent(s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-incoming-100",
			ChannelID: "ch-create-2",
			GuildID:   "g-create-2",
			Author:    &discordgo.User{ID: "user-human-1", Username: "alex", Bot: false},
			Content:   "Hello Aerial, deploy the stack!",
			Timestamp: time.Now().UTC(),
		},
	})

	var dbMsg *db.Message
	for i := 0; i < 50; i++ {
		dbMsg, _ = store.GetMessage(context.Background(), "msg-incoming-100")
		if dbMsg != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if dbMsg == nil {
		t.Fatalf("Expected msg-incoming-100 to be inserted into DB by MessageCreate handler")
	}
	if dbMsg.AuthorName != "alex" || !strings.Contains(dbMsg.Content, "deploy the stack") {
		t.Errorf("Unexpected persisted message: %+v", dbMsg)
	}

	// 8. MessageCreate: Untargeted message in threads mode
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	dispatchSessionEvent(s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-incoming-200",
			ChannelID: "ch-create-2",
			GuildID:   "g-create-2",
			Author:    &discordgo.User{ID: "user-human-2", Username: "sam", Bot: false},
			Content:   "unrelated message with no triggers",
			Timestamp: time.Now().UTC(),
		},
	})

	time.Sleep(50 * time.Millisecond)
	if ignoredMsg, _ := store.GetMessage(context.Background(), "msg-incoming-200"); ignoredMsg != nil {
		t.Errorf("Expected msg-incoming-200 to be ignored, but was found in DB")
	}

	// 9. DB insert error during MessageCreate handled cleanly
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	poolClosed := newTestWorkerPool(closedStore)
	sClosed := connectDiscordFunnel(ctx, closedStore, poolClosed, "mock-token-closed-db")
	if sClosed != nil {
		defer func() { _ = sClosed.Close() }()
		_ = sClosed.State.GuildAdd(&discordgo.Guild{ID: "g-closed-1"})
		_ = sClosed.State.ChannelAdd(&discordgo.Channel{ID: "ch-closed-1", GuildID: "g-closed-1", Type: discordgo.ChannelTypeGuildText})
		dispatchSessionEvent(sClosed, &discordgo.MessageCreate{
			Message: &discordgo.Message{
				ID:        "msg-closed-db-1",
				ChannelID: "ch-closed-1",
				GuildID:   "g-closed-1",
				Author:    &discordgo.User{ID: "u-1", Username: "alex"},
				Content:   "test insert failure",
				Timestamp: time.Now().UTC(),
			},
		})
		time.Sleep(30 * time.Millisecond)
	}
}

func TestFunnelStartupRecovery(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	queue.RecoverInterrupted(store, pool)
}

func TestResolveGuildID_ExtendedEdgeCases(t *testing.T) {
	if id := resolveGuildID(nil, nil); id != "" {
		t.Errorf("Expected empty string for nil, got %q", id)
	}

	m1 := &discordgo.Message{GuildID: "direct-guild"}
	if id := resolveGuildID(nil, m1); id != "direct-guild" {
		t.Errorf("Expected direct-guild, got %q", id)
	}

	queue.InvalidateChannelCache("cached-chan-99")
	defer queue.InvalidateChannelCache("cached-chan-99")
	queue.CacheDiscordChannel(&discordgo.Channel{ID: "cached-chan-99", GuildID: "cached-guild-99"})

	m2 := &discordgo.Message{ChannelID: "cached-chan-99"}
	if id := resolveGuildID(nil, m2); id != "cached-guild-99" {
		t.Errorf("Expected cached-guild-99, got %q", id)
	}

	s := newMockDiscordSession(t, nil)
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "session-guild-77"})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "session-chan-77", GuildID: "session-guild-77"})

	m3 := &discordgo.Message{ChannelID: "session-chan-77"}
	if id := resolveGuildID(s, m3); id != "session-guild-77" {
		t.Errorf("Expected session-guild-77, got %q", id)
	}

	m4 := &discordgo.Message{ChannelID: "unknown-chan"}
	if id := resolveGuildID(s, m4); id != "" {
		t.Errorf("Expected empty string for unknown channel, got %q", id)
	}
}

func TestRunStartupCatchUpSweep_SafeguardsAndCooldown(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)

	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)

	// 1. Nil safety
	RunStartupCatchUpSweep(context.Background(), nil, nil, nil)
	RunStartupCatchUpSweep(context.Background(), store, pool, nil)
	RunStartupCatchUpSweep(nil, store, pool, nil)

	// 2. Empty session completes safely (including nil context fallback)
	sEmpty := newMockDiscordSession(t, nil)
	RunStartupCatchUpSweep(context.Background(), store, pool, sEmpty)
	resetFunnelGlobals(t)
	RunStartupCatchUpSweep(nil, store, pool, sEmpty)

	// 2b. s.Token == "" with guilds in state triggers loop break
	sNoToken := &discordgo.Session{
		State: discordgo.NewState(),
		Token: "",
	}
	_ = sNoToken.State.GuildAdd(&discordgo.Guild{ID: "g-no-token"})
	RunStartupCatchUpSweep(context.Background(), store, pool, sNoToken)

	// 3. isSweeping already true skips duplicate run
	resetFunnelGlobals(t)
	isSweeping.Store(true)
	RunStartupCatchUpSweep(context.Background(), store, pool, sEmpty)
	isSweeping.Store(false)

	// 4. 2-minute cooldown rate-limit skip
	resetFunnelGlobals(t)
	sCooldown := newMockDiscordSession(t, nil)
	RunStartupCatchUpSweep(context.Background(), store, pool, sCooldown)
	// Immediately running again hits cooldown
	RunStartupCatchUpSweep(context.Background(), store, pool, sCooldown)

	// 5. Canceled context aborts sweep
	resetFunnelGlobals(t)
	cancelCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	RunStartupCatchUpSweep(cancelCtx, store, pool, sCooldown)

	// 6. Panic recovery on malformed transport
	resetFunnelGlobals(t)
	sPanic := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		panic("deliberate panic for recovery test")
	}))
	_ = sPanic.State.GuildAdd(&discordgo.Guild{ID: "g-panic"})
	_ = sPanic.State.ChannelAdd(&discordgo.Channel{ID: "ch-panic", GuildID: "g-panic", Type: discordgo.ChannelTypeGuildText})
	RunStartupCatchUpSweep(context.Background(), store, pool, sPanic)
}

func TestRunStartupCatchUpSweep_RecoveryAndFiltering(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)

	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
  general:
    mode: "channel"
    ignore_bots: false
  chan-bot-ignored:
    mode: "channel"
    ignore_bots: true
  ignored-ch:
    mode: "ignore"
`)

	t.Run("bot policy filtering", func(t *testing.T) {
		resetFunnelGlobals(t)
		setupTestConfig(t, `
channels:
  default:
    mode: "threads"
  chan-bot-allowed:
    mode: "channel"
    ignore_bots: false
  chan-bot-ignored:
    mode: "channel"
    ignore_bots: true
`)
		store := db.NewFakeStore()
		pool := newTestWorkerPool(store)
		pool.Start()
		defer pool.Stop()

		nowStr := time.Now().UTC().Format(time.RFC3339)
		s := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "threads/active") {
				return jsonHTTPResponse(http.StatusOK, `{"threads":[]}`), nil
			}
			if strings.Contains(req.URL.Path, "guilds/") && strings.Contains(req.URL.Path, "/channels") {
				return jsonHTTPResponse(http.StatusOK, `[{"id":"chan-bot-allowed","name":"chan-bot-allowed","guild_id":"g-1","type":0},{"id":"chan-bot-ignored","name":"chan-bot-ignored","guild_id":"g-1","type":0}]`), nil
			}
			if strings.Contains(req.URL.Path, "chan-bot-allowed") {
				return jsonHTTPResponse(http.StatusOK, `[{"id":"msg-bot-allowed","channel_id":"chan-bot-allowed","content":"Hey Aerial please help","timestamp":"`+nowStr+`","author":{"id":"peer-bot-1","username":"PeerBot","bot":true}}]`), nil
			}
			if strings.Contains(req.URL.Path, "chan-bot-ignored") {
				return jsonHTTPResponse(http.StatusOK, `[{"id":"msg-bot-ignored","channel_id":"chan-bot-ignored","content":"Hey Aerial please help","timestamp":"`+nowStr+`","author":{"id":"peer-bot-2","username":"PeerBot","bot":true}}]`), nil
			}
			return jsonHTTPResponse(http.StatusOK, `[]`), nil
		}))
		s.State.User = &discordgo.User{ID: "bot-aerial-id", Username: "Aerial"}

		guild := &discordgo.Guild{
			ID: "g-1",
			Channels: []*discordgo.Channel{
				{ID: "chan-bot-allowed", GuildID: "g-1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "chan-bot-ignored", GuildID: "g-1", Name: "chan-bot-ignored", Type: discordgo.ChannelTypeGuildText},
			},
			Roles: []*discordgo.Role{
				{ID: "g-1", Permissions: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory},
			},
			Members: []*discordgo.Member{
				{GuildID: "g-1", User: &discordgo.User{ID: "bot-aerial-id"}, Permissions: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory},
			},
		}
		_ = s.State.GuildAdd(guild)
		_ = s.State.ChannelAdd(guild.Channels[0])
		_ = s.State.ChannelAdd(guild.Channels[1])
		_ = s.State.MemberAdd(guild.Members[0])

		RunStartupCatchUpSweep(context.Background(), store, pool, s)

		existsAllowed, err := store.MessageExists(context.Background(), "msg-bot-allowed")
		if err != nil || !existsAllowed {
			t.Fatalf("Expected msg-bot-allowed inserted into DB when ignore_bots is false (exists: %t, err: %v)", existsAllowed, err)
		}
		existsIgnored, err := store.MessageExists(context.Background(), "msg-bot-ignored")
		if err != nil || existsIgnored {
			t.Fatalf("Expected msg-bot-ignored skipped when ignore_bots is true (exists: %t, err: %v)", existsIgnored, err)
		}
	})

	t.Run("message filtering and deduplication", func(t *testing.T) {
		resetFunnelGlobals(t)
		setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
  general:
    mode: "channel"
  ignored-ch:
    mode: "ignore"
`)
		store := db.NewFakeStore()
		pool := newTestWorkerPool(store)
		pool.Start()
		defer pool.Stop()

		_ = store.InsertMessage(context.Background(), db.Message{
			ID:        "msg-existing-1",
			ThreadID:  "ch-text-1",
			GuildID:   "g-sweep-1",
			Status:    db.StatusCompleted,
			CreatedAt: time.Now().UTC().Add(-10 * time.Minute),
			UpdatedAt: time.Now().UTC(),
		})

		_ = store.InsertMessage(context.Background(), db.Message{
			ID:           "msg-completed-later",
			ThreadID:     "ch-superseded",
			GuildID:      "g-sweep-1",
			Status:       db.StatusCompleted,
			ResponseText: "Already replied",
			CreatedAt:    time.Now().UTC().Add(-10 * time.Minute),
			UpdatedAt:    time.Now().UTC(),
		})

		recentTime := time.Now().UTC().Add(-5 * time.Minute)
		oldTime := time.Now().UTC().Add(-4 * time.Hour) // > 2 hour cutoff

		mockMessagesText := []*discordgo.Message{
			{
				ID:        "msg-new-sweep-1",
				ChannelID: "ch-text-1",
				GuildID:   "g-sweep-1",
				Author:    &discordgo.User{ID: "u-alex", Username: "alex", Bot: false},
				Content:   "Hey aerial help me deploy",
				Timestamp: recentTime,
			},
			{
				ID:        "msg-existing-1",
				ChannelID: "ch-text-1",
				GuildID:   "g-sweep-1",
				Author:    &discordgo.User{ID: "u-alex", Username: "alex", Bot: false},
				Content:   "Hey aerial already processed message",
				Timestamp: recentTime,
			},
			{
				ID:        "msg-old-sweep-1",
				ChannelID: "ch-text-1",
				GuildID:   "g-sweep-1",
				Author:    &discordgo.User{ID: "u-alex", Username: "alex", Bot: false},
				Content:   "Old message",
				Timestamp: oldTime,
			},
			{
				ID:        "msg-bot-self-sweep",
				ChannelID: "ch-text-1",
				GuildID:   "g-sweep-1",
				Author:    &discordgo.User{ID: "bot-sweep-id"},
				Timestamp: recentTime,
			},
			{
				ID:        "msg-untargeted-sweep",
				ChannelID: "ch-text-1",
				GuildID:   "g-sweep-1",
				Author:    &discordgo.User{ID: "u-other", Username: "other"},
				Content:   "random unmentioned chatter",
				Timestamp: recentTime,
			},
		}

		mockMessagesSuper := []*discordgo.Message{
			{
				ID:        "msg-missed-superseded",
				ChannelID: "ch-superseded",
				GuildID:   "g-sweep-1",
				Author:    &discordgo.User{ID: "u-alex", Username: "alex", Bot: false},
				Content:   "aerial missed older message",
				Timestamp: time.Now().UTC().Add(-20 * time.Minute),
			},
		}

		s := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads/active") {
				respBody, _ := json.Marshal(map[string]interface{}{
					"threads": []*discordgo.Channel{
						{ID: "th-active-1", Name: "sub-thread", GuildID: "g-sweep-1", ParentID: "ch-text-1", Type: discordgo.ChannelTypeGuildPublicThread},
					},
				})
				return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
			}
			if strings.Contains(req.URL.Path, "/channels/ch-text-1/messages") {
				respBody, _ := json.Marshal(mockMessagesText)
				return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
			}
			if strings.Contains(req.URL.Path, "/channels/ch-superseded/messages") {
				respBody, _ := json.Marshal(mockMessagesSuper)
				return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
			}
			return jsonHTTPResponse(http.StatusOK, "[]"), nil
		}))
		s.State.User = &discordgo.User{ID: "bot-sweep-id", Username: "AerialBot"}

		g := &discordgo.Guild{
			ID: "g-sweep-1",
			Channels: []*discordgo.Channel{
				{ID: "ch-text-1", Name: "threads-chat", GuildID: "g-sweep-1", Type: discordgo.ChannelTypeGuildText},
				{ID: "ch-superseded", Name: "threads-chat", GuildID: "g-sweep-1", Type: discordgo.ChannelTypeGuildText},
				{ID: "ch-voice-1", Name: "Voice", GuildID: "g-sweep-1", Type: discordgo.ChannelTypeGuildVoice},
			},
			Threads: []*discordgo.Channel{
				{ID: "th-active-1", Name: "sub-thread", GuildID: "g-sweep-1", ParentID: "ch-text-1", Type: discordgo.ChannelTypeGuildPublicThread},
			},
		}
		_ = s.State.GuildAdd(g)

		RunStartupCatchUpSweep(context.Background(), store, pool, s)

		dbMsg, err := store.GetMessage(context.Background(), "msg-new-sweep-1")
		if err != nil || dbMsg == nil {
			t.Fatalf("Expected msg-new-sweep-1 recovered during sweep, err: %v", err)
		}

		existsSuper, _ := store.MessageExists(context.Background(), "msg-missed-superseded")
		if existsSuper {
			t.Errorf("Expected msg-missed-superseded to be skipped due to newer completed message")
		}

		existsUntargeted, _ := store.MessageExists(context.Background(), "msg-untargeted-sweep")
		if existsUntargeted {
			t.Errorf("Expected msg-untargeted-sweep to be skipped")
		}
	})

	t.Run("guild discovery fallback and channel permissions", func(t *testing.T) {
		resetFunnelGlobals(t)
		store := db.NewFakeStore()
		pool := newTestWorkerPool(store)
		pool.Start()
		defer pool.Stop()

		s := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/users/@me/guilds") {
				respBody, _ := json.Marshal([]*discordgo.UserGuild{
					{ID: "g-sweep-fallback"},
				})
				return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
			}
			if strings.Contains(req.URL.Path, "/guilds/g-sweep-fallback/threads/active") {
				respBody, _ := json.Marshal(&discordgo.ThreadsList{
					Threads: []*discordgo.Channel{
						{ID: "th-fallback-1", Type: discordgo.ChannelTypeGuildPublicThread, GuildID: "g-sweep-fallback"},
					},
				})
				return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
			}
			if strings.Contains(req.URL.Path, "/guilds/g-sweep-fallback/channels") {
				respBody, _ := json.Marshal([]*discordgo.Channel{
					{ID: "ignored-ch", Type: discordgo.ChannelTypeGuildText, GuildID: "g-sweep-fallback"},
					{ID: "no-perm-ch", Type: discordgo.ChannelTypeGuildText, GuildID: "g-sweep-fallback"},
					{ID: "ch-fallback-1", Type: discordgo.ChannelTypeGuildText, GuildID: "g-sweep-fallback"},
				})
				return jsonHTTPResponse(http.StatusOK, string(respBody)), nil
			}
			if strings.Contains(req.URL.Path, "/messages") {
				return jsonHTTPResponse(http.StatusOK, "[]"), nil
			}
			return jsonHTTPResponse(http.StatusOK, "{}"), nil
		}))
		s.State.User = &discordgo.User{ID: "bot-user-id"}
		// Guild with permissions for bot-user-id = 0 for no-perm-ch
		gFallback := &discordgo.Guild{
			ID: "g-sweep-fallback",
			Roles: []*discordgo.Role{
				{ID: "role-zero", Permissions: 0},
			},
			Members: []*discordgo.Member{
				{GuildID: "g-sweep-fallback", User: &discordgo.User{ID: "bot-user-id"}, Roles: []string{"role-zero"}},
			},
		}
		_ = s.State.GuildAdd(gFallback)
		_ = s.State.ChannelAdd(&discordgo.Channel{ID: "ignored-ch", GuildID: "g-sweep-fallback", Type: discordgo.ChannelTypeGuildText})
		_ = s.State.ChannelAdd(&discordgo.Channel{ID: "no-perm-ch", GuildID: "g-sweep-fallback", Type: discordgo.ChannelTypeGuildText})
		_ = s.State.ChannelAdd(&discordgo.Channel{
			ID:      "ch-fallback-1",
			GuildID: "g-sweep-fallback",
			Type:    discordgo.ChannelTypeGuildText,
			PermissionOverwrites: []*discordgo.PermissionOverwrite{
				{
					ID:    "bot-user-id",
					Type:  discordgo.PermissionOverwriteTypeMember,
					Allow: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory,
				},
			},
		})
		s.State.Guilds = nil // Force UserGuilds fallback

		RunStartupCatchUpSweep(context.Background(), store, pool, s)
	})

	t.Run("circuit breaker and store errors", func(t *testing.T) {
		resetFunnelGlobals(t)
		store := db.NewFakeStore()
		pool := newTestWorkerPool(store)
		pool.Start()
		defer pool.Stop()

		cbChannels := `[
			{"id":"ch-cb-1","guild_id":"g-cb","type":0},
			{"id":"ch-cb-2","guild_id":"g-cb","type":0},
			{"id":"ch-cb-3","guild_id":"g-cb","type":0},
			{"id":"ch-cb-4","guild_id":"g-cb","type":0},
			{"id":"ch-cb-5","guild_id":"g-cb","type":0},
			{"id":"ch-cb-6","guild_id":"g-cb","type":0}
		]`

		sCircuit := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/channels") && strings.Contains(req.URL.Path, "/guilds/g-cb") {
				return jsonHTTPResponse(http.StatusOK, cbChannels), nil
			}
			if strings.Contains(req.URL.Path, "/messages") {
				return jsonHTTPResponse(http.StatusInternalServerError, `{"message":"internal error"}`), nil
			}
			return jsonHTTPResponse(http.StatusOK, "[]"), nil
		}))
		_ = sCircuit.State.GuildAdd(&discordgo.Guild{ID: "g-cb"})

		RunStartupCatchUpSweep(context.Background(), store, pool, sCircuit)

		// MessageExists store error recovery & InsertMessage error recovery
		nowStr := time.Now().UTC().Format(time.RFC3339)
		sweepMsg := `[{
			"id":"msg-sweep-err",
			"channel_id":"chan-sweep-1",
			"guild_id":"guild-sweep",
			"content":"Hey aerial test sweep",
			"timestamp":"` + nowStr + `",
			"author":{"id":"user-1","username":"user1","bot":false}
		}]`

		sErr := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/channels") && strings.Contains(req.URL.Path, "/guilds/guild-sweep") {
				return jsonHTTPResponse(http.StatusOK, `[{"id":"chan-sweep-1","guild_id":"guild-sweep","type":0}]`), nil
			}
			if strings.Contains(req.URL.Path, "/messages") {
				return jsonHTTPResponse(http.StatusOK, sweepMsg), nil
			}
			return jsonHTTPResponse(http.StatusOK, "[]"), nil
		}))
		sErr.State.User = &discordgo.User{ID: "bot-1"}
		_ = sErr.State.GuildAdd(&discordgo.Guild{ID: "guild-sweep"})
		_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "chan-sweep-1", GuildID: "guild-sweep", Type: discordgo.ChannelTypeGuildText})

		resetFunnelGlobals(t)
		errStore := &errExistsStore{Store: store}
		RunStartupCatchUpSweep(context.Background(), errStore, pool, sErr)

		// DB insert failure during recovery
		resetFunnelGlobals(t)
		closedStore := db.NewFakeStore()
		_ = closedStore.Close()
		RunStartupCatchUpSweep(context.Background(), closedStore, pool, sErr)
	})
}

func TestSyncBotMemberRoles(t *testing.T) {
	origFetcher := funnelMemberFetcher
	defer func() { funnelMemberFetcher = origFetcher }()

	var fetchCalls int32
	shouldFail := false
	funnelMemberFetcher = func(s *discordgo.Session, guildID, userID string) (*discordgo.Member, error) {
		atomic.AddInt32(&fetchCalls, 1)
		if shouldFail {
			return nil, errors.New("simulated fetch failure")
		}
		return &discordgo.Member{
			GuildID: "",
			User:    &discordgo.User{ID: userID},
			Roles:   []string{"mock-role-robot-123"},
		}, nil
	}

	s := newMockDiscordSession(t, nil)
	s.State.User = &discordgo.User{ID: "bot-1"}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	syncBotMemberRoles(context.Background(), nil, "guild-1", "bot-1")
	syncBotMemberRoles(context.Background(), s, "", "bot-1")
	syncBotMemberRoles(context.Background(), s, "guild-1", "")
	if atomic.LoadInt32(&fetchCalls) != 0 {
		t.Fatalf("expected 0 calls for invalid args, got %d", fetchCalls)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	syncBotMemberRoles(canceledCtx, s, "guild-1", "bot-1")
	if atomic.LoadInt32(&fetchCalls) != 0 {
		t.Fatalf("expected 0 calls for canceled context, got %d", fetchCalls)
	}

	shouldFail = true
	syncBotMemberRoles(context.Background(), s, "guild-1", "bot-1")
	if atomic.LoadInt32(&fetchCalls) != 1 {
		t.Fatalf("expected 1 call on failure, got %d", fetchCalls)
	}
	shouldFail = false

	syncBotMemberRoles(context.Background(), s, "guild-1", "bot-1")
	if atomic.LoadInt32(&fetchCalls) != 2 {
		t.Fatalf("expected 2 calls total, got %d", fetchCalls)
	}

	m, err := s.State.Member("guild-1", "bot-1")
	if err != nil || len(m.Roles) != 1 || m.Roles[0] != "mock-role-robot-123" {
		t.Fatalf("expected cached member with role, got %v (err: %v)", m, err)
	}
	if m.GuildID != "guild-1" {
		t.Fatalf("expected guildID populated, got %s", m.GuildID)
	}

	syncBotMemberRoles(context.Background(), s, "guild-1", "bot-1")
	if atomic.LoadInt32(&fetchCalls) != 2 {
		t.Fatalf("expected fetchCalls to stay 2, got %d", fetchCalls)
	}

	syncBotMemberRoles(context.Background(), s, "guild-not-in-state", "bot-1")

	_ = DefaultGeminiHomeDir()
	_ = DefaultDataDir()
}

func TestHandleDiscordReady(t *testing.T) {
	handleDiscordReady(nil, nil, nil, nil, nil)

	s := newMockDiscordSession(t, nil)
	g := &discordgo.Guild{
		ID: "g-ready-1",
		Channels: []*discordgo.Channel{
			{ID: "c-ready-1", GuildID: "g-ready-1", Type: discordgo.ChannelTypeGuildText},
		},
		Threads: []*discordgo.Channel{
			{ID: "t-ready-1", GuildID: "g-ready-1", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	}
	s.State.GuildAdd(g)

	ready := &discordgo.Ready{
		User: &discordgo.User{
			ID:            "bot-ready-1",
			Username:      "AerialBot",
			Discriminator: "0001",
		},
	}
	mockStore := db.NewFakeStore()
	pool := newTestWorkerPool(mockStore)
	handleDiscordReady(context.Background(), s, ready, mockStore, pool)
}

func TestHandleGuildMemberUpdate(t *testing.T) {
	handleGuildMemberUpdate(nil, nil)

	s := newMockDiscordSession(t, nil)
	s.State.User = &discordgo.User{ID: "bot-123"}

	mOther := &discordgo.GuildMemberUpdate{
		Member: &discordgo.Member{
			User: &discordgo.User{ID: "user-456"},
		},
	}
	handleGuildMemberUpdate(s, mOther)

	mBot1 := &discordgo.GuildMemberUpdate{
		Member: &discordgo.Member{
			User:    &discordgo.User{ID: "bot-123"},
			GuildID: "guild-1",
			Roles:   []string{"role-1"},
		},
	}
	mBot1.User = &discordgo.User{ID: "bot-123"}
	handleGuildMemberUpdate(s, mBot1)

	mBot2 := &discordgo.GuildMemberUpdate{
		Member: &discordgo.Member{
			User:    &discordgo.User{ID: "bot-123"},
			GuildID: "guild-2",
			Roles:   []string{"role-2"},
		},
	}
	mBot2.User = nil
	mBot2.GuildID = "guild-2"
	mBot2.Member.GuildID = ""
	handleGuildMemberUpdate(s, mBot2)
}

func TestFunnelDefaultFuncs(t *testing.T) {
	_ = discordSessionOpener(&discordgo.Session{})
	_, _ = funnelMemberFetcher(&discordgo.Session{}, "g1", "u1")
}

func TestExtractDiscordMetadata(t *testing.T) {
	empty := extractDiscordMetadata(nil, nil, "")
	if !empty.IsEmpty() {
		t.Errorf("expected empty metadata for nil message")
	}

	s := newMockDiscordSession(t, nil)
	s.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Roles: []*discordgo.Role{
			{ID: "role-mod", Name: "Moderator"},
		},
	})

	m := &discordgo.Message{
		ID:        "msg-1",
		ChannelID: "chan-1",
		GuildID:   "guild-1",
		Content:   "test content",
		Author: &discordgo.User{
			ID:         "user-1",
			Username:   "testuser\n",
			GlobalName: "Test User\r",
			Bot:        false,
		},
		ReferencedMessage: &discordgo.Message{
			Author: &discordgo.User{
				Username: "bob",
			},
			Content: "hello ref",
		},
		Mentions: []*discordgo.User{
			nil,
			{ID: "u-mention-1", Username: "alice\n"},
		},
		MentionRoles: []string{"role-mod", "role-unknown"},
		Attachments: []*discordgo.MessageAttachment{
			nil,
			{URL: "https://example.com/att.png"},
			{URL: ""},
		},
	}

	meta := extractDiscordMetadata(s, m, "thread-1")
	if meta.ChannelID != "chan-1" || meta.TargetThreadID != "thread-1" || meta.GuildID != "guild-1" {
		t.Errorf("metadata IDs mismatch: %+v", meta)
	}
	if meta.AuthorUsername != "testuser\n" || meta.AuthorGlobalName != "Test User\r" {
		t.Errorf("author name mismatch: %+v", meta)
	}
	if meta.ReplyingToAuthor != "@bob" || meta.ReplyingToContent != "hello ref" {
		t.Errorf("replyingTo mismatch: %s / %s", meta.ReplyingToAuthor, meta.ReplyingToContent)
	}
	if len(meta.MentionUserIDs) != 1 || meta.MentionUserIDs[0] != "u-mention-1" {
		t.Errorf("mentionUserIDs mismatch: %v", meta.MentionUserIDs)
	}
	if len(meta.MentionRoleIDs) != 2 || meta.MentionRoleIDs[0] != "role-mod" {
		t.Errorf("mentionRoleIDs mismatch: %v", meta.MentionRoleIDs)
	}
	if len(meta.Attachments) != 1 || meta.Attachments[0] != "https://example.com/att.png" {
		t.Errorf("attachments mismatch: %v", meta.Attachments)
	}

	mNilAuth := &discordgo.Message{
		ChannelID: "chan-2",
		ReferencedMessage: &discordgo.Message{
			Content: "no author ref",
		},
	}
	metaNilAuth := extractDiscordMetadata(nil, mNilAuth, "thread-2")
	if metaNilAuth.ReplyingToAuthor != "Unknown" {
		t.Errorf("expected Unknown for referenced message with nil author, got %q", metaNilAuth.ReplyingToAuthor)
	}
}

func TestResolveMessageContent(t *testing.T) {
	if got := resolveMessageContent(nil, nil); got != "" {
		t.Errorf("expected empty string for nil message, got %q", got)
	}

	m := &discordgo.Message{
		Content: "<@1542285964213358633> work on tasks 265, 263",
		Mentions: []*discordgo.User{
			{ID: "1542285964213358633", Username: "Zero"},
		},
	}
	got := resolveMessageContent(nil, m)
	expected := "@Zero work on tasks 265, 263"
	if got != expected {
		t.Errorf("resolveMessageContent(nil, m) = %q, want %q", got, expected)
	}

	s := newMockDiscordSession(t, nil)
	s.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Roles: []*discordgo.Role{
			{ID: "role-1", Name: "Admins", Mentionable: false},
		},
	})
	s.State.ChannelAdd(&discordgo.Channel{
		ID:      "chan-1",
		GuildID: "guild-1",
	})
	mWithRoles := &discordgo.Message{
		ChannelID: "chan-1",
		GuildID:   "guild-1",
		Content:   "Pinging <@&role-1> and <@1542285964213358633>",
		Mentions: []*discordgo.User{
			{ID: "1542285964213358633", Username: "Zero"},
		},
		MentionRoles: []string{"role-1"},
	}
	gotSession := resolveMessageContent(s, mWithRoles)
	expectedSession := "Pinging @Admins and @Zero"
	if gotSession != expectedSession {
		t.Errorf("resolveMessageContent(s, mWithRoles) = %q, want %q", gotSession, expectedSession)
	}

	mEmpty := &discordgo.Message{Content: ""}
	if got := resolveMessageContent(nil, mEmpty); got != "" {
		t.Errorf("expected empty string for empty content, got %q", got)
	}

	sStateErr := &discordgo.Session{StateEnabled: true, State: discordgo.NewState()}
	mFallback := &discordgo.Message{
		ChannelID: "chan-not-found",
		Content:   "Pinging <@1542285964213358633>",
		Mentions: []*discordgo.User{
			{ID: "1542285964213358633", Username: "Zero"},
		},
	}
	if got := resolveMessageContent(sStateErr, mFallback); got != "Pinging @Zero" {
		t.Errorf("expected Pinging @Zero, got %q", got)
	}

	mRoleNotFound := &discordgo.Message{
		ChannelID:    "chan-1",
		GuildID:      "guild-1",
		Content:      "Pinging <@&role-999>",
		MentionRoles: []string{"role-999"},
	}
	if got := resolveMessageContent(s, mRoleNotFound); got != "Pinging <@&role-999>" {
		t.Errorf("expected original content when role not found, got %q", got)
	}

	if meta := extractDiscordMetadata(nil, nil, "th-1"); meta.TargetThreadID != "" {
		t.Errorf("expected empty metadata for nil message")
	}
}
