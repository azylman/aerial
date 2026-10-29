package main

import (
	"context"
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

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

var defaultFunnelMemberFetcher = funnelMemberFetcher

func init() {
	discordSessionOpener = func(dg *discordgo.Session) error { return nil }
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
	t.Cleanup(func() { discordSessionOpener = old })
}

// newTestWorkerPool instantiates a fully isolated WorkerPool with mocked runner, notifier, and delivery hooks.
func newTestWorkerPool(store db.Store) *queue.WorkerPool {
	return queue.New(nil, queue.WorkerPoolConfig{
		Store: store,
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
		NotifierFunc: func(agyBin, apiKey, contextDescription string) string { return "mock notification" },
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
	})
}

func setupTestConfig(t *testing.T, yamlContent string) {
	t.Helper()
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("Failed to write test config.yaml: %v", err)
	}
	cfg, err := config.LoadConfigFromPaths(yamlPath)
	if err != nil {
		t.Fatalf("Failed to load test config from %s: %v", yamlPath, err)
	}
	SetFunnelConfig(cfg)
	t.Cleanup(func() { SetFunnelConfig(nil) })
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
	} else {
		s.Client = &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusNotFound, `{"message":"not found"}`), nil
		})}
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
	setupTestConfig(t, `
model: "gemini-2.5-flash"
admin_users:
  - "user-admin"
channels:
  default:
    mode: "threads"
`)

	s := newMockDiscordSession(t, nil)

	if _, err := defaultFunnelMemberFetcher(nil, "g1", "u1"); err == nil {
		t.Errorf("expected error for nil session")
	}

	dmMsg := &discordgo.Message{
		ID:        "msg-1",
		ChannelID: "chan-dm",
		Content:   "Hello Aerial",
		Timestamp: time.Now(),
		Author:    &discordgo.User{ID: "user-1", Username: "testuser", Bot: false},
	}

	targetThreadID, isThread := getOrCreateThreadID(s, dmMsg)
	if targetThreadID != "chan-dm" || isThread {
		t.Errorf("Expected DM targetThreadID chan-dm and false, got: %s, %t", targetThreadID, isThread)
	}

	mCreate := &discordgo.MessageCreate{Message: dmMsg}
	if !isFunnelBotTargeted(s, mCreate) {
		t.Errorf("Expected DM message to be bot targeted")
	}

	// Default funcs verification
	_ = discordSessionOpener(&discordgo.Session{})
	_, _ = funnelMemberFetcher(s, "g1", "u1")

	// ResolveGuildID extended edge cases
	if id := resolveGuildID(nil, nil); id != "" {
		t.Errorf("Expected empty string for nil, got %q", id)
	}
	if id := resolveGuildID(nil, &discordgo.Message{GuildID: "direct-guild"}); id != "direct-guild" {
		t.Errorf("Expected direct-guild, got %q", id)
	}

	queue.InvalidateChannelCache("cached-chan-99")
	defer queue.InvalidateChannelCache("cached-chan-99")
	queue.CacheDiscordChannel(&discordgo.Channel{ID: "cached-chan-99", GuildID: "cached-guild-99"})
	if id := resolveGuildID(nil, &discordgo.Message{ChannelID: "cached-chan-99"}); id != "cached-guild-99" {
		t.Errorf("Expected cached-guild-99, got %q", id)
	}

	_ = s.State.GuildAdd(&discordgo.Guild{ID: "session-guild-77"})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "session-chan-77", GuildID: "session-guild-77"})
	if id := resolveGuildID(s, &discordgo.Message{ChannelID: "session-chan-77"}); id != "session-guild-77" {
		t.Errorf("Expected session-guild-77, got %q", id)
	}
	if id := resolveGuildID(s, &discordgo.Message{ChannelID: "unknown-chan"}); id != "" {
		t.Errorf("Expected empty string for unknown channel, got %q", id)
	}
}

func TestIsFunnelBotTargeted(t *testing.T) {
	setupTestConfig(t, `
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
`)

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

	user := func(id, username string, bot bool) *discordgo.User {
		return &discordgo.User{ID: id, Username: username, Bot: bot}
	}
	withMentions := func(users ...*discordgo.User) func(*discordgo.Message) {
		return func(m *discordgo.Message) { m.Mentions = users }
	}
	withRef := func(id string) func(*discordgo.Message) {
		return func(m *discordgo.Message) { m.ReferencedMessage = &discordgo.Message{ID: id} }
	}
	withMsgRef := func(id string) func(*discordgo.Message) {
		return func(m *discordgo.Message) { m.MessageReference = &discordgo.MessageReference{MessageID: id} }
	}
	withRoles := func(roles ...string) func(*discordgo.Message) {
		return func(m *discordgo.Message) { m.MentionRoles = roles }
	}
	funnelMsg := func(id, chanID, guildID, content string, author *discordgo.User, opts ...func(*discordgo.Message)) *discordgo.MessageCreate {
		m := &discordgo.Message{ID: id, ChannelID: chanID, GuildID: guildID, Content: content, Author: author}
		for _, opt := range opts {
			opt(m)
		}
		return &discordgo.MessageCreate{Message: m}
	}

	tests := []struct {
		name     string
		msg      *discordgo.MessageCreate
		expected bool
	}{
		{"nil message create", nil, false},
		{"nil inner message", &discordgo.MessageCreate{}, false},
		{"nil author", funnelMsg("1", "ch", "g", "hi", nil), false},
		{"bot self message", funnelMsg("msg-self", "chan-general", "guild-1", "I am responding", user("bot-self-id", "AerialBot", true)), false},
		{"channel mode user ambient", funnelMsg("msg-amb", "chan-general", "guild-1", "Just chatting about weather", user("user-1", "alice", false)), true},
		{"channel mode ignore_bots true with bot", funnelMsg("msg-bot-ign", "chan-general", "guild-1", "Automated CI alert", user("other-bot-id", "CiBot", true)), false},
		{"channel mode ignore_bots false with bot", funnelMsg("msg-bot-allowed", "chan-bot-lab", "guild-1", "Bot to bot ping", user("other-bot-id", "CiBot", true)), true},
		{"threads mode ambient without triggers", funnelMsg("msg-thr-amb", "chan-threads", "guild-1", "Hey everyone what is up", user("user-1", "alice", false)), false},
		{"threads mode with aerial keyword", funnelMsg("msg-kw-1", "chan-threads", "guild-1", "Hey aerial can you help?", user("user-1", "alice", false)), true},
		{"threads mode with gundam keyword", funnelMsg("msg-kw-2", "chan-threads", "guild-1", "Launch the gundam units", user("user-1", "alice", false)), true},
		{"threads mode with brain keyword", funnelMsg("msg-kw-3", "chan-threads", "guild-1", "Query the brain state", user("user-1", "alice", false)), true},
		{"threads mode with bot keyword", funnelMsg("msg-kw-4", "chan-threads", "guild-1", "Good bot thanks", user("user-1", "alice", false)), true},
		{"threads mode with mention", funnelMsg("msg-mention", "chan-threads", "guild-1", "Check this out", user("user-1", "alice", false), withMentions(user("bot-self-id", "AerialBot", false))), true},
		{"threads mode inside active thread", funnelMsg("msg-in-thread", "thread-sub-1", "guild-1", "Continuing discussion in thread", user("user-1", "alice", false)), true},
		{"ignored channel with mention", funnelMsg("msg-spam-1", "chan-spam", "guild-1", "Hey aerial help me here", user("user-1", "alice", false), withMentions(user("bot-self-id", "AerialBot", false))), false},
		{"mode ignore channel", funnelMsg("msg-muted-1", "chan-muted", "guild-1", "Hey aerial help me here", user("user-1", "alice", false), withMentions(user("bot-self-id", "AerialBot", false))), false},
		{"snowflake ignored channel", funnelMsg("msg-snow-1", "111999", "guild-1", "Hey aerial help me here", user("user-1", "alice", false), withMentions(user("bot-self-id", "AerialBot", false))), false},
		{"allowed channel with mention", funnelMsg("msg-allowed-1", "chan-allowed", "guild-1", "Hey aerial help me here", user("user-1", "alice", false), withMentions(user("bot-self-id", "AerialBot", false))), true},
		{"thread inherits whitelisted parent", funnelMsg("msg-dev-thread-1", "thread-dev-999", "guild-1", "Continuing discussion in whitelisted thread", user("user-1", "alice", false)), true},
		{"thread inherits ignored parent", funnelMsg("msg-spam-thread-1", "thread-spam-888", "guild-1", "Hey aerial answer me in spam thread", user("user-1", "alice", false), withMentions(user("bot-self-id", "AerialBot", false))), false},
		{"message with ReferencedMessage", funnelMsg("msg-ref-1", "chan-threads", "guild-1", "simple discussion without keyword", user("user-ref-1", "user1", false), withRef("ref-0")), true},
		{"message with MessageReference", funnelMsg("msg-ref-2", "chan-threads", "guild-1", "simple discussion with message reference", user("user-ref-2", "user2", false), withMsgRef("ref-0")), true},
		{"message with MentionRoles", funnelMsg("msg-role-1", "chan-threads", "guild-1", "hey team ping", user("user-role-1", "user1", false), withRoles("mock-role-bot-123")), true},
		{"direct message channel without guild", funnelMsg("msg-dm-1", "chan-dm-1", "", "Hello in private", user("user-dm-1", "alice", false)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFunnelBotTargeted(s, tt.msg); got != tt.expected {
				t.Errorf("isFunnelBotTargeted() = %v; want %v", got, tt.expected)
			}
		})
	}
}

func TestGetDiscordChannel(t *testing.T) {
	if ch := getDiscordChannel(nil, ""); ch != nil {
		t.Errorf("Expected nil for empty channel ID, got %v", ch)
	}
	s := newMockDiscordSession(t, nil)
	if ch := getDiscordChannel(s, "12345"); ch != nil {
		t.Errorf("Expected nil for non-existent channel, got %v", ch)
	}

	chOriginal := &discordgo.Channel{
		ID:       "chan-test-1",
		Name:     "test-channel",
		GuildID:  "guild-1",
		ParentID: "parent-1",
		Type:     discordgo.ChannelTypeGuildText,
	}
	queue.CacheDiscordChannel(chOriginal)
	defer queue.InvalidateChannelCache("chan-test-1")

	chFromCache := getDiscordChannel(nil, "chan-test-1")
	if chFromCache == nil || chFromCache.Name != "test-channel" || chFromCache.Type != discordgo.ChannelTypeGuildText {
		t.Errorf("Failed to retrieve text channel from queue cache: %+v", chFromCache)
	}

	thOriginal := &discordgo.Channel{
		ID:       "thread-test-1",
		Name:     "test-thread",
		GuildID:  "guild-1",
		ParentID: "parent-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	queue.CacheDiscordChannel(thOriginal)
	defer queue.InvalidateChannelCache("thread-test-1")

	thFromCache := getDiscordChannel(nil, "thread-test-1")
	if thFromCache == nil || thFromCache.Name != "test-thread" || thFromCache.Type != discordgo.ChannelTypeGuildPublicThread {
		t.Errorf("Failed to retrieve thread channel from queue cache: %+v", thFromCache)
	}

	sWithChan := newMockDiscordSession(t, nil, chOriginal)
	chFromSession := getDiscordChannel(sWithChan, "chan-test-1")
	if chFromSession == nil || chFromSession.Name != "test-channel" {
		t.Errorf("Failed to retrieve channel from session state: %+v", chFromSession)
	}

	sWithREST := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/channels/123456789012345678") {
			body, _ := json.Marshal(&discordgo.Channel{ID: "123456789012345678", Name: "rest-chan"})
			return jsonHTTPResponse(http.StatusOK, string(body)), nil
		}
		return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
	}))
	sWithREST.Token = "test-token"
	chREST := getDiscordChannel(sWithREST, "123456789012345678")
	if chREST == nil || chREST.Name != "rest-chan" {
		t.Errorf("expected rest-chan, got %v", chREST)
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
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "111222333", GuildID: "guild-1", Name: "general", Type: discordgo.ChannelTypeGuildText})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "thread-444", GuildID: "guild-1", ParentID: "111222333", Name: "discussion-thread", Type: discordgo.ChannelTypeGuildPublicThread})

	if thID, isTh := getOrCreateThreadID(s, nil); thID != "" || isTh {
		t.Errorf("Expected empty string and false for nil message, got %s, %t", thID, isTh)
	}

	dmMsg := &discordgo.Message{ID: "msg-dm", ChannelID: "chan-dm", Content: "Hello Aerial", Author: &discordgo.User{ID: "user-1"}}
	if thID, isTh := getOrCreateThreadID(s, dmMsg); thID != "chan-dm" || isTh {
		t.Errorf("Expected DM to return chan-dm and false, got %s, %t", thID, isTh)
	}

	msgGeneral := &discordgo.Message{ID: "msg-general-1", ChannelID: "111222333", GuildID: "guild-1", Content: "Hello in general", Author: &discordgo.User{ID: "user-1", Username: "alice"}}
	if thID, isTh := getOrCreateThreadID(s, msgGeneral); thID != "111222333" || isTh {
		t.Errorf("Expected channel mode to return 111222333 and false, got %s, %t", thID, isTh)
	}

	msgInThread := &discordgo.Message{ID: "msg-thread-1", ChannelID: "thread-444", GuildID: "guild-1", Content: "Follow up in thread", Author: &discordgo.User{ID: "user-1", Username: "alice"}}
	if thID, isTh := getOrCreateThreadID(s, msgInThread); thID != "thread-444" || !isTh {
		t.Errorf("Expected existing thread to return thread-444 and true, got %s, %t", thID, isTh)
	}

	threadID1 := "th-cached-101"
	queue.InvalidateChannelCache(threadID1)
	defer queue.InvalidateChannelCache(threadID1)
	queue.CacheDiscordChannel(&discordgo.Channel{ID: threadID1, GuildID: "guild-1", ParentID: "chan-threads-555", Type: discordgo.ChannelTypeGuildPublicThread})
	if thID, isTh := getOrCreateThreadID(s, &discordgo.Message{ID: threadID1, ChannelID: "chan-threads-555", GuildID: "guild-1"}); thID != threadID1 || !isTh {
		t.Errorf("Expected in-memory cache hit (%s, true), got (%s, %t)", threadID1, thID, isTh)
	}

	threadID2 := "th-state-102"
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: threadID2, GuildID: "guild-1", ParentID: "chan-threads-555", Type: discordgo.ChannelTypeGuildPublicThread})
	if thID, isTh := getOrCreateThreadID(s, &discordgo.Message{ID: threadID2, ChannelID: "chan-threads-555", GuildID: "guild-1"}); thID != threadID2 || !isTh {
		t.Errorf("Expected session state hit (%s, true), got (%s, %t)", threadID2, thID, isTh)
	}

	sNoToken := &discordgo.Session{State: discordgo.NewState()}
	_ = sNoToken.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})
	_ = sNoToken.State.ChannelAdd(&discordgo.Channel{ID: "chan-threads-555", GuildID: "guild-1", Name: "dev-chat", Type: discordgo.ChannelTypeGuildText})
	msgDev := &discordgo.Message{ID: "msg-dev-1", ChannelID: "chan-threads-555", GuildID: "guild-1", Content: "Start a new thread please", Author: &discordgo.User{ID: "user-1", Username: "alice"}}
	if thID, isTh := getOrCreateThreadID(sNoToken, msgDev); thID != "chan-threads-555" || isTh {
		t.Errorf("Expected threads mode without active REST to return chan-threads-555 and false, got %s, %t", thID, isTh)
	}
	if thID, _ := getOrCreateThreadID(sNoToken, msgDev, false); thID != "chan-threads-555" {
		t.Errorf("expected chan-threads-555 with allowSummarize=false")
	}

	clsErr := classifier.New(currentFunnelConfig())
	clsErr.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
		return "", errors.New("summarizer forced failure")
	}
	pool := queue.New(currentFunnelConfig(), queue.WorkerPoolConfig{Classifier: clsErr, Store: dbtest.NewFakeStore()})
	SetFunnelPool(pool)
	defer SetFunnelPool(nil)

	if thIDFail, _ := getOrCreateThreadID(sNoToken, msgDev, true); thIDFail != "chan-threads-555" {
		t.Errorf("expected chan-threads-555 on summarizer failure")
	}

	titleSummarizeSem <- struct{}{}
	titleSummarizeSem <- struct{}{}
	defer func() {
		<-titleSummarizeSem
		<-titleSummarizeSem
	}()
	if thID2, _ := getOrCreateThreadID(sNoToken, msgDev, true); thID2 != "chan-threads-555" {
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

	mockRT := func(threadsCode int, threadsBody string, chCode int, chBody string) roundTripperFunc {
		return func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				return jsonHTTPResponse(threadsCode, threadsBody), nil
			}
			if chCode != 0 && strings.Contains(req.URL.Path, "/channels/") {
				return jsonHTTPResponse(chCode, chBody), nil
			}
			return jsonHTTPResponse(http.StatusNotFound, "{}"), nil
		}
	}

	tests := []struct {
		name         string
		messageID    string
		content      string
		threadsCode  int
		threadsBody  string
		chCode       int
		chBody       string
		wantThreadID string
		wantIsThread bool
		checkCache   bool
		wantParentID string
	}{
		{"200 thread created successfully", "msg-200-ok", "deploy service", 200, `{"id":"th-created-200","guild_id":"g-rest-1","parent_id":"ch-rest-parent","name":"deploy-service","type":11}`, 0, "", "th-created-200", true, true, parentChanID},
		{"200 thread created with missing parent and guild", "msg-200-empty-parent", "start missing parent thread", 200, `{"id":"th-created-empty-parent","type":11}`, 0, "", "th-created-empty-parent", true, true, parentChanID},
		{"160004 thread already exists with channel re-fetch success", "200300400500600701", "test 160004 refetch", 400, `{"code":160004,"message":"Thread already exists"}`, 200, `{"id":"200300400500600701","guild_id":"g-rest-1","parent_id":"ch-rest-parent","type":11}`, "200300400500600701", true, true, parentChanID},
		{"160004 thread already exists with channel error fallback", "200300400500600702", "test 160004 fallback", 400, `{"code":160004,"message":"Thread already exists"}`, 0, "", "200300400500600702", true, true, parentChanID},
		{"160004 non-numeric message ID falls back without recovery", "not-a-numeric-snowflake", "test non-numeric", 400, `{"code":160004,"message":"Thread already exists"}`, 0, "", parentChanID, false, false, ""},
		{"REST forbidden 50001 error fallback", "msg-forbidden-1", "test forbidden", 403, `{"code":50001,"message":"Missing Access"}`, 0, "", parentChanID, false, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue.InvalidateChannelCache(tt.messageID)
			defer queue.InvalidateChannelCache(tt.messageID)
			if tt.wantThreadID != parentChanID {
				queue.InvalidateChannelCache(tt.wantThreadID)
				defer queue.InvalidateChannelCache(tt.wantThreadID)
			}

			s := newMockDiscordSession(t, mockRT(tt.threadsCode, tt.threadsBody, tt.chCode, tt.chBody))
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
				Author:    &discordgo.User{ID: "user-42", Username: "test-user"},
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
  gc-chan:
    mode: "channel"
    effort: "low"
`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := dbtest.NewFakeStore()
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

	// 3 & 4. Channel and Thread Create / Update / Delete
	for _, ch := range []*discordgo.Channel{
		{ID: "ch-crud-3", Name: "crud-chan", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildText},
		{ID: "th-crud-4", Name: "crud-thread", GuildID: "g-create-2", Type: discordgo.ChannelTypeGuildPublicThread},
	} {
		if ch.IsThread() {
			dispatchSessionEvent(s, &discordgo.ThreadCreate{Channel: ch})
		} else {
			dispatchSessionEvent(s, &discordgo.ChannelCreate{Channel: ch})
		}
		if snap, ok := queue.GetCachedChannel(ch.ID); !ok || snap.Name != ch.Name {
			t.Errorf("Expected %s cached on Create", ch.ID)
		}
		ch.Name += "-updated"
		if ch.IsThread() {
			dispatchSessionEvent(s, &discordgo.ThreadUpdate{Channel: ch})
			dispatchSessionEvent(s, &discordgo.ThreadDelete{Channel: ch})
		} else {
			dispatchSessionEvent(s, &discordgo.ChannelUpdate{Channel: ch})
			dispatchSessionEvent(s, &discordgo.ChannelDelete{Channel: ch})
		}
		if _, ok := queue.GetCachedChannel(ch.ID); ok {
			t.Errorf("Expected %s removed on Delete", ch.ID)
		}
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
			Author:    &discordgo.User{ID: "user-human-1", Username: "test-user", Bot: false},
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
	if dbMsg.AuthorName != "test-user" || !strings.Contains(dbMsg.Content, "deploy the stack") || dbMsg.Effort != "low" {
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
	closedStore := dbtest.NewFakeStore()
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
				Author:    &discordgo.User{ID: "u-1", Username: "test-user"},
				Content:   "Hey aerial test insert failure",
				Timestamp: time.Now().UTC(),
			},
		})
		time.Sleep(30 * time.Millisecond)
	}
}

func TestRunStartupCatchUpSweep_SafeguardsAndCooldown(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)

	store := dbtest.NewFakeStore()
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
	sNoToken := &discordgo.Session{State: discordgo.NewState(), Token: ""}
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

func TestRunStartupCatchUpSweep_BotPolicyFiltering(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)
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
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	nowStr := time.Now().UTC().Format(time.RFC3339)
	channelsJSON := `[{"id":"chan-bot-allowed","name":"general","guild_id":"g-1","type":0},{"id":"chan-bot-ignored","name":"chan-bot-ignored","guild_id":"g-1","type":0}]`
	msgAllowedJSON := `[{"id":"msg-bot-allowed","channel_id":"chan-bot-allowed","content":"Hey Aerial please help","timestamp":"` + nowStr + `","author":{"id":"peer-bot-1","username":"PeerBot","bot":true}}]`
	msgIgnoredJSON := `[{"id":"msg-bot-ignored","channel_id":"chan-bot-ignored","content":"Hey Aerial please help","timestamp":"` + nowStr + `","author":{"id":"peer-bot-2","username":"PeerBot","bot":true}}]`
	s := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		p := req.URL.Path
		if strings.Contains(p, "threads/active") {
			return jsonHTTPResponse(http.StatusOK, `{"threads":[]}`), nil
		}
		if strings.Contains(p, "guilds/") && strings.Contains(p, "/channels") {
			return jsonHTTPResponse(http.StatusOK, channelsJSON), nil
		}
		if strings.Contains(p, "chan-bot-allowed") {
			return jsonHTTPResponse(http.StatusOK, msgAllowedJSON), nil
		}
		if strings.Contains(p, "chan-bot-ignored") {
			return jsonHTTPResponse(http.StatusOK, msgIgnoredJSON), nil
		}
		return jsonHTTPResponse(http.StatusOK, `[]`), nil
	}))
	s.State.User = &discordgo.User{ID: "bot-aerial-id", Username: "Aerial"}

	g := &discordgo.Guild{
		ID: "g-1",
		Channels: []*discordgo.Channel{
			{ID: "chan-bot-allowed", GuildID: "g-1", Name: "general", Type: discordgo.ChannelTypeGuildText},
			{ID: "chan-bot-ignored", GuildID: "g-1", Name: "chan-bot-ignored", Type: discordgo.ChannelTypeGuildText},
		},
		Roles:   []*discordgo.Role{{ID: "g-1", Permissions: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory}},
		Members: []*discordgo.Member{{GuildID: "g-1", User: &discordgo.User{ID: "bot-aerial-id"}, Permissions: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory}},
	}
	_ = s.State.GuildAdd(g)
	_ = s.State.ChannelAdd(g.Channels[0])
	_ = s.State.ChannelAdd(g.Channels[1])
	_ = s.State.MemberAdd(g.Members[0])

	RunStartupCatchUpSweep(context.Background(), store, pool, s)

	if exists, err := store.MessageExists(context.Background(), "msg-bot-allowed"); err != nil || !exists {
		t.Fatalf("Expected msg-bot-allowed inserted into DB when ignore_bots is false (exists: %t, err: %v)", exists, err)
	}
	if exists, err := store.MessageExists(context.Background(), "msg-bot-ignored"); err != nil || exists {
		t.Fatalf("Expected msg-bot-ignored skipped when ignore_bots is true (exists: %t, err: %v)", exists, err)
	}
}

func TestRunStartupCatchUpSweep_MessageFilteringAndDeduplication(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)
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
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	_ = store.InsertMessage(context.Background(), db.Message{
		ID: "msg-existing-1", ThreadID: "ch-text-1", GuildID: "g-sweep-1", Status: db.StatusCompleted, CreatedAt: time.Now().UTC().Add(-10 * time.Minute), UpdatedAt: time.Now().UTC(),
	})
	_ = store.InsertMessage(context.Background(), db.Message{
		ID: "msg-completed-later", ThreadID: "ch-superseded", GuildID: "g-sweep-1", Status: db.StatusCompleted, ResponseText: "Already replied", CreatedAt: time.Now().UTC().Add(-10 * time.Minute), UpdatedAt: time.Now().UTC(),
	})

	recentTime := time.Now().UTC().Add(-5 * time.Minute)
	oldTime := time.Now().UTC().Add(-4 * time.Hour)
	mkMsg := func(id, chanID, content string, u *discordgo.User, t time.Time) *discordgo.Message {
		return &discordgo.Message{ID: id, ChannelID: chanID, GuildID: "g-sweep-1", Content: content, Author: u, Timestamp: t}
	}
	txtJSON, _ := json.Marshal([]*discordgo.Message{
		mkMsg("msg-new-sweep-1", "ch-text-1", "Hey aerial help me deploy", &discordgo.User{ID: "u-test", Username: "test-user"}, recentTime),
		mkMsg("msg-existing-1", "ch-text-1", "Hey aerial already processed message", &discordgo.User{ID: "u-test", Username: "test-user"}, recentTime),
		mkMsg("msg-old-sweep-1", "ch-text-1", "Old message", &discordgo.User{ID: "u-test", Username: "test-user"}, oldTime),
		mkMsg("msg-bot-self-sweep", "ch-text-1", "", &discordgo.User{ID: "bot-sweep-id"}, recentTime),
		mkMsg("msg-untargeted-sweep", "ch-text-1", "random unmentioned chatter", &discordgo.User{ID: "u-other", Username: "other"}, recentTime),
	})
	superJSON, _ := json.Marshal([]*discordgo.Message{
		mkMsg("msg-missed-superseded", "ch-superseded", "aerial missed older message", &discordgo.User{ID: "u-test", Username: "test-user"}, time.Now().UTC().Add(-20*time.Minute)),
	})
	thJSON, _ := json.Marshal(map[string]interface{}{
		"threads": []*discordgo.Channel{{ID: "th-active-1", Name: "sub-thread", GuildID: "g-sweep-1", ParentID: "ch-text-1", Type: discordgo.ChannelTypeGuildPublicThread}},
	})

	s := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		p := req.URL.Path
		if strings.Contains(p, "/threads/active") {
			return jsonHTTPResponse(http.StatusOK, string(thJSON)), nil
		}
		if strings.Contains(p, "/channels/ch-text-1/messages") {
			return jsonHTTPResponse(http.StatusOK, string(txtJSON)), nil
		}
		if strings.Contains(p, "/channels/ch-superseded/messages") {
			return jsonHTTPResponse(http.StatusOK, string(superJSON)), nil
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

	if dbMsg, err := store.GetMessage(context.Background(), "msg-new-sweep-1"); err != nil || dbMsg == nil {
		t.Fatalf("Expected msg-new-sweep-1 recovered during sweep, err: %v", err)
	}
	if exists, _ := store.MessageExists(context.Background(), "msg-missed-superseded"); exists {
		t.Errorf("Expected msg-missed-superseded skipped")
	}
	if exists, _ := store.MessageExists(context.Background(), "msg-untargeted-sweep"); exists {
		t.Errorf("Expected msg-untargeted-sweep skipped")
	}
}

func TestRunStartupCatchUpSweep_GuildDiscoveryFallbackAndPermissions(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	s := newMockDiscordSession(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		p := req.URL.Path
		if strings.Contains(p, "/users/@me/guilds") {
			return jsonHTTPResponse(http.StatusOK, `[{"id":"g-sweep-fallback"}]`), nil
		}
		if strings.Contains(p, "/threads/active") {
			return jsonHTTPResponse(http.StatusOK, `{"threads":[{"id":"th-fallback-1","type":11,"guild_id":"g-sweep-fallback"}]}`), nil
		}
		if strings.Contains(p, "/channels") {
			return jsonHTTPResponse(http.StatusOK, `[{"id":"ignored-ch","type":0,"guild_id":"g-sweep-fallback"},{"id":"no-perm-ch","type":0,"guild_id":"g-sweep-fallback"},{"id":"ch-fallback-1","type":0,"guild_id":"g-sweep-fallback"}]`), nil
		}
		return jsonHTTPResponse(http.StatusOK, "[]"), nil
	}))
	s.State.User = &discordgo.User{ID: "bot-user-id"}
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID:      "g-sweep-fallback",
		Roles:   []*discordgo.Role{{ID: "role-zero", Permissions: 0}},
		Members: []*discordgo.Member{{GuildID: "g-sweep-fallback", User: &discordgo.User{ID: "bot-user-id"}, Roles: []string{"role-zero"}}},
	})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "ignored-ch", GuildID: "g-sweep-fallback", Type: discordgo.ChannelTypeGuildText})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "no-perm-ch", GuildID: "g-sweep-fallback", Type: discordgo.ChannelTypeGuildText})
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID: "ch-fallback-1", GuildID: "g-sweep-fallback", Type: discordgo.ChannelTypeGuildText,
		PermissionOverwrites: []*discordgo.PermissionOverwrite{
			{ID: "bot-user-id", Type: discordgo.PermissionOverwriteTypeMember, Allow: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory},
		},
	})
	s.State.Guilds = nil // Force UserGuilds fallback

	RunStartupCatchUpSweep(context.Background(), store, pool, s)
}

func TestRunStartupCatchUpSweep_CircuitBreakerAndStoreErrors(t *testing.T) {
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)
	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	cbChannels := `[{"id":"ch-cb-1","guild_id":"g-cb","type":0},{"id":"ch-cb-2","guild_id":"g-cb","type":0},{"id":"ch-cb-3","guild_id":"g-cb","type":0},{"id":"ch-cb-4","guild_id":"g-cb","type":0},{"id":"ch-cb-5","guild_id":"g-cb","type":0},{"id":"ch-cb-6","guild_id":"g-cb","type":0}]`

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

	nowStr := time.Now().UTC().Format(time.RFC3339)
	sweepMsg := `[{"id":"msg-sweep-err","channel_id":"chan-sweep-1","guild_id":"guild-sweep","content":"Hey aerial test sweep","timestamp":"` + nowStr + `","author":{"id":"user-1","username":"user1","bot":false}}]`

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

	resetFunnelGlobals(t)
	closedStore := dbtest.NewFakeStore()
	_ = closedStore.Close()
	RunStartupCatchUpSweep(context.Background(), closedStore, pool, sErr)
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
	_ = s.State.GuildAdd(g)

	ready := &discordgo.Ready{
		User: &discordgo.User{
			ID:            "bot-ready-1",
			Username:      "AerialBot",
			Discriminator: "0001",
		},
	}
	mockStore := dbtest.NewFakeStore()
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

func TestPopulateDiscordMessageFields(t *testing.T) {
	var empty db.Message
	populateDiscordMessageFields(nil, nil, &empty)
	if empty.HasStructuredFields() {
		t.Errorf("expected empty fields for nil message")
	}

	s := newMockDiscordSession(t, nil)
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID:    "guild-1",
		Roles: []*discordgo.Role{{ID: "role-mod", Name: "Moderator"}},
	})

	m := &discordgo.Message{
		ID: "msg-1", ChannelID: "chan-1", GuildID: "guild-1", Content: "test content",
		Author:            &discordgo.User{ID: "user-1", Username: "testuser\n", GlobalName: "Test User\r", Bot: false},
		ReferencedMessage: &discordgo.Message{Author: &discordgo.User{Username: "bob"}, Content: "hello ref", Attachments: []*discordgo.MessageAttachment{{URL: "https://example.com/ref.png"}}},
		Mentions:          []*discordgo.User{nil, {ID: "u-mention-1", Username: "alice\n"}},
		MentionRoles:      []string{"role-mod", "role-unknown"},
		Attachments:       []*discordgo.MessageAttachment{nil, {URL: "https://example.com/att.png"}, {URL: ""}},
	}

	var msg db.Message
	populateDiscordMessageFields(s, m, &msg)
	if msg.ChannelID != "chan-1" {
		t.Errorf("channel ID mismatch: %+v", msg)
	}
	if msg.AuthorGlobalName != "Test User\r" {
		t.Errorf("author name mismatch: %+v", msg)
	}
	if msg.ReplyingToAuthor != "@bob" || msg.ReplyingToContent != "hello ref [attachments: https://example.com/ref.png]" {
		t.Errorf("replyingTo mismatch: %s / %s", msg.ReplyingToAuthor, msg.ReplyingToContent)
	}
	if len(msg.MentionUserIDs) != 1 || msg.MentionUserIDs[0] != "u-mention-1" {
		t.Errorf("mentionUserIDs mismatch: %v", msg.MentionUserIDs)
	}
	if len(msg.MentionRoleIDs) != 2 || msg.MentionRoleIDs[0] != "role-mod" {
		t.Errorf("mentionRoleIDs mismatch: %v", msg.MentionRoleIDs)
	}
	if len(msg.Attachments) != 1 || msg.Attachments[0] != "https://example.com/att.png" {
		t.Errorf("attachments mismatch: %v", msg.Attachments)
	}

	mNilAuth := &discordgo.Message{
		ChannelID:         "chan-2",
		ReferencedMessage: &discordgo.Message{Content: "no author ref"},
	}
	var msgNilAuth db.Message
	populateDiscordMessageFields(nil, mNilAuth, &msgNilAuth)
	if msgNilAuth.ReplyingToAuthor != "Unknown" {
		t.Errorf("expected Unknown for referenced message with nil author, got %q", msgNilAuth.ReplyingToAuthor)
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
	if got := resolveMessageContent(nil, m); got != "@Zero work on tasks 265, 263" {
		t.Errorf("resolveMessageContent(nil, m) = %q, want @Zero work on tasks 265, 263", got)
	}

	s := newMockDiscordSession(t, nil)
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID:    "guild-1",
		Roles: []*discordgo.Role{{ID: "role-1", Name: "Admins", Mentionable: false}},
	})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "chan-1", GuildID: "guild-1"})
	mWithRoles := &discordgo.Message{
		ChannelID: "chan-1", GuildID: "guild-1", Content: "Pinging <@&role-1> and <@1542285964213358633>",
		Mentions:     []*discordgo.User{{ID: "1542285964213358633", Username: "Zero"}},
		MentionRoles: []string{"role-1"},
	}
	if got := resolveMessageContent(s, mWithRoles); got != "Pinging @Admins and @Zero" {
		t.Errorf("resolveMessageContent(s, mWithRoles) = %q, want Pinging @Admins and @Zero", got)
	}

	if got := resolveMessageContent(nil, &discordgo.Message{Content: ""}); got != "" {
		t.Errorf("expected empty string for empty content, got %q", got)
	}

	sStateErr := &discordgo.Session{StateEnabled: true, State: discordgo.NewState()}
	mFallback := &discordgo.Message{
		ChannelID: "chan-not-found", Content: "Pinging <@1542285964213358633>",
		Mentions: []*discordgo.User{{ID: "1542285964213358633", Username: "Zero"}},
	}
	if got := resolveMessageContent(sStateErr, mFallback); got != "Pinging @Zero" {
		t.Errorf("expected Pinging @Zero, got %q", got)
	}

	mRoleNotFound := &discordgo.Message{
		ChannelID: "chan-1", GuildID: "guild-1", Content: "Pinging <@&role-999>",
		MentionRoles: []string{"role-999"},
	}
	if got := resolveMessageContent(s, mRoleNotFound); got != "Pinging <@&role-999>" {
		t.Errorf("expected original content when role not found, got %q", got)
	}
}
