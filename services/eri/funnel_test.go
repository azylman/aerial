package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

	seedSessionStateForIngress(nil, "", nil)
	seedSessionStateForIngress(s, "", nil)
	if !isSelfOrNilMessage(nil, nil, "") {
		t.Errorf("expected isSelfOrNilMessage to return true for nil message")
	}
	seedSessionStateForIngress(s, "bot-exists", nil) // m == nil
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-already-present"})
	seedSessionStateForIngress(s, "bot-exists", &discordgo.Message{GuildID: "guild-already-present"})
	seedSessionStateForIngress(s, "bot-exists", &discordgo.Message{GuildID: "guild-new-to-add"})

	sEmptyUser := &discordgo.Session{StateEnabled: true, State: discordgo.NewState()}
	seedSessionStateForIngress(sEmptyUser, "bot-new-id", nil)
	if sEmptyUser.State.User == nil || sEmptyUser.State.User.ID != "bot-new-id" {
		t.Errorf("expected sEmptyUser.State.User to be seeded")
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

func TestConnectDiscordFunnel_DisabledAndLifecycle(t *testing.T) {
	if dg := connectDiscordFunnel(nil, nil, nil, ""); dg != nil {
		t.Errorf("Expected nil session for empty token, got %v", dg)
	}

	cancelCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	if dg := connectDiscordFunnel(cancelCtx, nil, nil, ""); dg != nil {
		t.Errorf("Expected nil session for empty token with canceled ctx, got %v", dg)
	}

	store := dbtest.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	s := connectDiscordFunnel(cancelCtx, store, pool, "mock-valid-token")
	if s == nil {
		t.Fatalf("Expected non-nil discordgo.Session")
	}
	defer func() { _ = s.Close() }()

	sNilCtx := connectDiscordFunnel(nil, store, pool, "mock-nil-ctx-token")
	if sNilCtx == nil {
		t.Fatalf("Expected non-nil discordgo.Session for nil ctx")
	}
	_ = sNilCtx.Close()

	if activeInteractionRouter.Load() == nil {
		t.Errorf("Expected activeInteractionRouter to be initialized")
	}
}

func TestHydrateDiscordSessionREST(t *testing.T) {
	tests := []struct {
		name         string
		nilSession   bool
		nilState     bool
		token        string
		userStatus   int
		userBody     string
		guildsStatus int
		guildsBody   string
		wantErr      bool
		wantGuildID  string
		wantChanID   string
		wantThreadID string
		wantRoleID   string
	}{
		{
			name:       "NilSession",
			nilSession: true,
			wantErr:    false,
		},
		{
			name:    "EmptyToken",
			token:   "",
			wantErr: false,
		},
		{
			name:       "UserMeError",
			token:      "mock-token",
			userStatus: http.StatusInternalServerError,
			userBody:   `{"message":"internal error"}`,
			wantErr:    true,
		},
		{
			name:         "UserGuildsError",
			token:        "mock-token",
			userStatus:   http.StatusOK,
			userBody:     `{"id":"bot-hydrate-1","username":"Aerial"}`,
			guildsStatus: http.StatusBadGateway,
			guildsBody:   `{"message":"bad gateway"}`,
			wantErr:      true,
		},
		{
			name:         "EmptyGuildIDHandling",
			token:        "mock-token",
			userStatus:   http.StatusOK,
			userBody:     `{"id":"bot-hydrate-1","username":"Aerial"}`,
			guildsStatus: http.StatusOK,
			guildsBody:   `[{"id":""}]`,
			wantErr:      false,
		},
		{
			name:         "NilStateAutoInitialized",
			nilState:     true,
			token:        "mock-token",
			userStatus:   http.StatusOK,
			userBody:     `{"id":"bot-hydrate-1","username":"Aerial"}`,
			guildsStatus: http.StatusOK,
			guildsBody:   `[]`,
			wantErr:      false,
		},
		{
			name:         "SuccessfulHydration",
			token:        "mock-token",
			userStatus:   http.StatusOK,
			userBody:     `{"id":"bot-hydrate-1","username":"Aerial"}`,
			guildsStatus: http.StatusOK,
			guildsBody:   `[{"id":"g-hydrated-1","name":"Main Guild"}]`,
			wantErr:      false,
			wantGuildID:  "g-hydrated-1",
			wantChanID:   "ch-hydrated-1",
			wantThreadID: "th-hydrated-1",
			wantRoleID:   "role-hydrated-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilSession {
				if err := hydrateDiscordSessionREST(nil); err != nil {
					t.Fatalf("unexpected error for nil session: %v", err)
				}
				return
			}

			s, err := discordgo.New("Bot " + tc.token)
			if err != nil {
				t.Fatalf("failed to create session: %v", err)
			}
			s.StateEnabled = true
			if !tc.nilState {
				s.State = discordgo.NewState()
			}

			s.Client = &http.Client{
				Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
					p := req.URL.Path
					switch {
					case strings.Contains(p, "/users/@me/guilds"):
						if tc.guildsStatus != 0 {
							return jsonHTTPResponse(tc.guildsStatus, tc.guildsBody), nil
						}
						return jsonHTTPResponse(http.StatusOK, `[]`), nil
					case strings.Contains(p, "/users/@me"):
						if tc.userStatus != 0 {
							return jsonHTTPResponse(tc.userStatus, tc.userBody), nil
						}
						return jsonHTTPResponse(http.StatusOK, `{"id":"bot-1"}`), nil
					case strings.Contains(p, "/roles"):
						return jsonHTTPResponse(http.StatusOK, `[{"id":"role-hydrated-1","name":"Admins"},null]`), nil
					case strings.Contains(p, "/threads/active"):
						return jsonHTTPResponse(http.StatusOK, `{"threads":[{"id":"th-hydrated-1","name":"Active Thread","type":11},null]}`), nil
					case strings.Contains(p, "/channels"):
						return jsonHTTPResponse(http.StatusOK, `[{"id":"ch-hydrated-1","name":"general","type":0},null]`), nil
					default:
						return jsonHTTPResponse(http.StatusOK, `{}`), nil
					}
				}),
			}

			err = hydrateDiscordSessionREST(s)
			if (err != nil) != tc.wantErr {
				t.Fatalf("hydrateDiscordSessionREST() error = %v, wantErr = %v", err, tc.wantErr)
			}

			if tc.wantGuildID != "" {
				g, gErr := s.State.Guild(tc.wantGuildID)
				if gErr != nil || g == nil {
					t.Fatalf("expected guild %s in state: %v", tc.wantGuildID, gErr)
				}
			}
			if tc.wantChanID != "" {
				if _, ok := queue.GetCachedChannel(tc.wantChanID); !ok {
					t.Errorf("expected channel %s in channel cache", tc.wantChanID)
				}
				if _, cErr := s.State.Channel(tc.wantChanID); cErr != nil {
					t.Errorf("expected channel %s in session state: %v", tc.wantChanID, cErr)
				}
			}
			if tc.wantThreadID != "" {
				if _, ok := queue.GetCachedChannel(tc.wantThreadID); !ok {
					t.Errorf("expected thread %s in channel cache", tc.wantThreadID)
				}
				if _, tErr := s.State.Channel(tc.wantThreadID); tErr != nil {
					t.Errorf("expected thread %s in session state: %v", tc.wantThreadID, tErr)
				}
			}
			if tc.wantRoleID != "" {
				if _, rErr := s.State.Role(tc.wantGuildID, tc.wantRoleID); rErr != nil {
					t.Errorf("expected role %s in session state: %v", tc.wantRoleID, rErr)
				}
			}
		})
	}

	hydrateGuildState(nil, nil, "bot-1")
	errSess := newMockDiscordSession(t, nil)
	hydrateGuildRoles(errSess, "g1")
	hydrateGuildChannels(errSess, "g1")
	hydrateGuildThreads(errSess, "g1")
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
	populateDiscordMessageFields(s, m, &msg, map[string]string{"role-unknown": "CustomRole"})
	if msg.ChannelID != "chan-1" {
		t.Errorf("channel ID mismatch: %+v", msg)
	}
	if msg.AuthorGlobalName != "Test User\r" {
		t.Errorf("author name mismatch: %+v", msg)
	}
	if msg.ReplyingToAuthor != "@bob" || msg.ReplyingToContent != "hello ref [attachments: https://example.com/ref.png]" {
		t.Errorf("replyingTo mismatch: %s / %s", msg.ReplyingToAuthor, msg.ReplyingToContent)
	}
	if m.ReferencedMessage.GuildID != "guild-1" {
		t.Errorf("referenced message guild ID not copied: %q", m.ReferencedMessage.GuildID)
	}
	if len(msg.MentionUserIDs) != 1 || msg.MentionUserIDs[0] != "u-mention-1" {
		t.Errorf("mentionUserIDs mismatch: %v", msg.MentionUserIDs)
	}
	if len(msg.MentionRoleIDs) != 2 || msg.MentionRoleIDs[0] != "role-mod" {
		t.Errorf("mentionRoleIDs mismatch: %v", msg.MentionRoleIDs)
	}
	foundCustom := false
	for _, mention := range msg.Mentions {
		if mention == "CustomRole" {
			foundCustom = true
			break
		}
	}
	if !foundCustom {
		t.Errorf("expected CustomRole in mentions, got: %v", msg.Mentions)
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

	mRoleExtra := &discordgo.Message{
		ChannelID: "chan-1", GuildID: "guild-1", Content: "Pinging <@&role-extra>",
		MentionRoles: []string{"role-extra"},
	}
	if got := resolveMessageContent(nil, mRoleExtra, map[string]string{"role-extra": "ExtraRole"}); got != "Pinging @ExtraRole" {
		t.Errorf("expected Pinging @ExtraRole, got %q", got)
	}
}

func TestProcessFunnelMessageIngress_Scenarios(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
  chan-ignored:
    mode: "ignore"
`)

	tests := []struct {
		name         string
		botUserID    string
		sessionUser  string
		authorID     string
		channelID    string
		guildID      string
		msgID        string
		preInsertMsg bool
		errStore     bool
		wantResult   string
		wantErr      bool
		wantEnqueued bool
	}{
		{
			name:       "IgnoredSelfPayloadBotUser",
			botUserID:  "bot-123",
			authorID:   "bot-123",
			channelID:  "chan-main",
			msgID:      "msg-self-arg",
			wantResult: "ignored_self",
		},
		{
			name:        "IgnoredSelfSessionUser",
			sessionUser: "session-bot-99",
			authorID:    "session-bot-99",
			channelID:   "chan-main",
			msgID:       "msg-self-sess",
			wantResult:  "ignored_self",
		},
		{
			name:       "IgnoredByPolicy",
			channelID:  "chan-ignored",
			authorID:   "user-normal",
			msgID:      "msg-policy-ign",
			wantResult: "ignored",
		},
		{
			name:         "DuplicateInStore",
			channelID:    "chan-main",
			authorID:     "user-normal",
			msgID:        "msg-dup-store",
			preInsertMsg: true,
			wantResult:   "duplicate",
		},
		{
			name:       "StoreError",
			channelID:  "chan-main",
			authorID:   "user-normal",
			msgID:      "msg-store-err",
			errStore:   true,
			wantResult: "",
			wantErr:    true,
		},
		{
			name:         "SuccessfulEnqueue",
			channelID:    "chan-main",
			authorID:     "user-normal",
			msgID:        "msg-ok-enq",
			wantResult:   "enqueued",
			wantEnqueued: true,
		},
		{
			name:         "SeedSessionStateWithGuild",
			botUserID:    "bot-seed",
			authorID:     "user-normal",
			channelID:    "chan-main",
			guildID:      "g-seed-new",
			msgID:        "msg-seed-ok",
			wantResult:   "enqueued",
			wantEnqueued: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store db.Store
			fakeStore := dbtest.NewFakeStore()
			if tc.errStore {
				store = &errExistsStore{Store: fakeStore}
			} else {
				store = fakeStore
			}

			if tc.preInsertMsg {
				_ = store.InsertMessage(context.Background(), db.Message{
					ID:        tc.msgID,
					ChannelID: tc.channelID,
					Status:    db.StatusPending,
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				})
			}

			s := newMockDiscordSession(t, nil)
			if tc.sessionUser != "" {
				s.State.User = &discordgo.User{ID: tc.sessionUser, Username: "AerialBot"}
			}

			pool := newTestWorkerPool(store)
			m := &discordgo.Message{
				ID:        tc.msgID,
				ChannelID: tc.channelID,
				GuildID:   tc.guildID,
				Content:   "Hello Aerial",
				Author:    &discordgo.User{ID: tc.authorID, Username: "testuser"},
				Timestamp: time.Now().UTC(),
			}

			res, err := processFunnelMessageIngress(context.Background(), s, store, pool, m, tc.botUserID, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("processFunnelMessageIngress error = %v, wantErr = %v", err, tc.wantErr)
			}
			if res != tc.wantResult {
				t.Fatalf("processFunnelMessageIngress result = %q, want %q", res, tc.wantResult)
			}

			if tc.wantEnqueued {
				dbMsg, err := store.GetMessage(context.Background(), tc.msgID)
				if err != nil || dbMsg == nil {
					t.Fatalf("expected message %s to be persisted in store", tc.msgID)
				}
			}
		})
	}
}
