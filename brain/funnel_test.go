package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

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

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}

	// 1. Direct Message channel
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

	title := deriveThreadTitle("<@1542035925603713086> Write a python script for Docker")
	if title != "Write a python script for Docker" {
		t.Errorf("Expected title 'Write a python script for Docker', got: %q", title)
	}

	targetThreadID, isThread := getOrCreateThreadID(s, dmMsg)
	if targetThreadID != "chan-dm" || isThread {
		t.Errorf("Expected DM targetThreadID 'chan-dm' and false, got: %s, %t", targetThreadID, isThread)
	}

	prompt := buildDiscordPrompt(dmMsg, "thread-12345", config.ChannelPolicy{Mode: "threads"})
	if prompt == "" {
		t.Errorf("Expected non-empty prompt for message")
	}

	mCreate := &discordgo.MessageCreate{Message: dmMsg}
	if !isFunnelBotTargeted(s, mCreate) {
		t.Errorf("Expected DM message to be bot targeted")
	}
}

func TestGetOrCreateThreadID_ChannelAndThreadModes(t *testing.T) {
	yamlContent := `
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
`
	setupTestConfig(t, yamlContent)

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	// Channel in channel mode
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

	// 1. Channel mode should return channel ID and isThread = false
	thID, isTh := getOrCreateThreadID(s, msgGeneral)
	if thID != "111222333" || isTh {
		t.Errorf("Expected channel mode to return 111222333 and false, got %s, %t", thID, isTh)
	}

	// 2. Message in an existing thread channel should return thread ID and isThread = true
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

	thID, isTh = getOrCreateThreadID(s, msgInThread)
	if thID != "thread-444" || !isTh {
		t.Errorf("Expected existing thread to return thread-444 and true, got %s, %t", thID, isTh)
	}

	// 3. Channel in threads mode (default) with no Discord token (can't spawn) returns ChannelID, false
	chanDev := &discordgo.Channel{
		ID:      "chan-threads-555",
		GuildID: "guild-1",
		Name:    "dev-chat",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = s.State.ChannelAdd(chanDev)

	msgDev := &discordgo.Message{
		ID:        "msg-dev-1",
		ChannelID: "chan-threads-555",
		GuildID:   "guild-1",
		Content:   "Start a new thread please",
		Author:    &discordgo.User{ID: "user-1", Username: "alice"},
	}

	thID, isTh = getOrCreateThreadID(s, msgDev)
	if thID != "chan-threads-555" || isTh {
		t.Errorf("Expected threads mode without active REST to return chan-threads-555 and false, got %s, %t", thID, isTh)
	}
}

func TestIsFunnelBotTargeted_ChannelAndThreadModes(t *testing.T) {
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
`
	setupTestConfig(t, yamlContent)

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{ID: "bot-self-id", Username: "AerialBot"}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	chanGeneral := &discordgo.Channel{
		ID:      "chan-general",
		GuildID: "guild-1",
		Name:    "general",
		Type:    discordgo.ChannelTypeGuildText,
	}
	chanBotLab := &discordgo.Channel{
		ID:      "chan-bot-lab",
		GuildID: "guild-1",
		Name:    "bot-lab",
		Type:    discordgo.ChannelTypeGuildText,
	}
	chanThreads := &discordgo.Channel{
		ID:      "chan-threads",
		GuildID: "guild-1",
		Name:    "threads-chan",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = s.State.ChannelAdd(chanGeneral)
	_ = s.State.ChannelAdd(chanBotLab)
	_ = s.State.ChannelAdd(chanThreads)

	// A. Channel mode ambient message from user -> true
	msgAmbient := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-1",
			ChannelID: "chan-general",
			GuildID:   "guild-1",
			Content:   "Just chatting about weather",
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if !isFunnelBotTargeted(s, msgAmbient) {
		t.Errorf("Expected ambient user message in channel mode to be targeted")
	}

	// B. Channel mode bot's own message -> false
	msgOwn := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-2",
			ChannelID: "chan-general",
			GuildID:   "guild-1",
			Content:   "I am the bot responding",
			Author:    &discordgo.User{ID: "bot-self-id", Username: "AerialBot", Bot: true},
		},
	}
	if isFunnelBotTargeted(s, msgOwn) {
		t.Errorf("Expected bot's own message to NOT be targeted")
	}

	// C. Channel mode with ignore_bots: true, from another bot -> false
	msgOtherBotIgnored := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-3",
			ChannelID: "chan-general",
			GuildID:   "guild-1",
			Content:   "Automated CI alert",
			Author:    &discordgo.User{ID: "other-bot-id", Username: "CiBot", Bot: true},
		},
	}
	if isFunnelBotTargeted(s, msgOtherBotIgnored) {
		t.Errorf("Expected other bot to be ignored in channel with ignore_bots: true")
	}

	// D. Channel mode with ignore_bots: false, from another bot -> true
	msgOtherBotAllowed := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-4",
			ChannelID: "chan-bot-lab",
			GuildID:   "guild-1",
			Content:   "Bot to bot ping",
			Author:    &discordgo.User{ID: "other-bot-id", Username: "CiBot", Bot: true},
		},
	}
	if !isFunnelBotTargeted(s, msgOtherBotAllowed) {
		t.Errorf("Expected other bot to be targeted in channel with ignore_bots: false")
	}

	// E. Nil message or nil author -> false
	if isFunnelBotTargeted(s, nil) {
		t.Errorf("Expected nil message create to return false")
	}
	if isFunnelBotTargeted(s, &discordgo.MessageCreate{Message: &discordgo.Message{Author: nil}}) {
		t.Errorf("Expected nil author to return false")
	}

	// F. Threads mode ambient message without mention/keyword -> false
	msgThreadsAmbient := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-5",
			ChannelID: "chan-threads",
			GuildID:   "guild-1",
			Content:   "Hey everyone what's up",
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if isFunnelBotTargeted(s, msgThreadsAmbient) {
		t.Errorf("Expected ambient message in threads mode without mention to NOT be targeted")
	}

	// G. Threads mode with keyword -> true
	msgThreadsKeyword := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-6",
			ChannelID: "chan-threads",
			GuildID:   "guild-1",
			Content:   "Hey aerial can you help?",
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if !isFunnelBotTargeted(s, msgThreadsKeyword) {
		t.Errorf("Expected keyword message in threads mode to be targeted")
	}

	// H. Threads mode with mention -> true
	msgThreadsMention := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-7",
			ChannelID: "chan-threads",
			GuildID:   "guild-1",
			Content:   "Check this out",
			Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if !isFunnelBotTargeted(s, msgThreadsMention) {
		t.Errorf("Expected mention message in threads mode to be targeted")
	}

	// I. Threads mode inside an active thread -> true
	threadInThreads := &discordgo.Channel{
		ID:       "thread-sub-1",
		GuildID:  "guild-1",
		ParentID: "chan-threads",
		Name:     "sub-discussion",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	_ = s.State.ChannelAdd(threadInThreads)

	msgInsideThread := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-8",
			ChannelID: "thread-sub-1",
			GuildID:   "guild-1",
			Content:   "Continuing discussion in thread",
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if !isFunnelBotTargeted(s, msgInsideThread) {
		t.Errorf("Expected message inside active thread to be targeted")
	}
}

func TestIsFunnelBotTargeted_IgnoredChannels(t *testing.T) {
	yamlContent := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
  spam:
    mode: "ignore"
  "111999":
    mode: "ignore"
  muted-room:
    mode: "ignore"
  disabled-room:
    mode: "disabled"
`
	setupTestConfig(t, yamlContent)

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{ID: "bot-self-id", Username: "AerialBot"}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	chanSpam := &discordgo.Channel{
		ID:      "chan-spam",
		GuildID: "guild-1",
		Name:    "spam",
		Type:    discordgo.ChannelTypeGuildText,
	}
	chanMuted := &discordgo.Channel{
		ID:      "chan-muted",
		GuildID: "guild-1",
		Name:    "muted-room",
		Type:    discordgo.ChannelTypeGuildText,
	}
	chanSnowflakeIgnored := &discordgo.Channel{
		ID:      "111999",
		GuildID: "guild-1",
		Name:    "random-snowflake",
		Type:    discordgo.ChannelTypeGuildText,
	}
	chanAllowed := &discordgo.Channel{
		ID:      "chan-allowed",
		GuildID: "guild-1",
		Name:    "general",
		Type:    discordgo.ChannelTypeGuildText,
	}

	_ = s.State.ChannelAdd(chanSpam)
	_ = s.State.ChannelAdd(chanMuted)
	_ = s.State.ChannelAdd(chanSnowflakeIgnored)
	_ = s.State.ChannelAdd(chanAllowed)

	// 1. Direct mention in ignored channel (via ignored_channels) -> false
	msgInSpam := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-spam-1",
			ChannelID: "chan-spam",
			GuildID:   "guild-1",
			Content:   "Hey aerial help me here",
			Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if isFunnelBotTargeted(s, msgInSpam) {
		t.Errorf("Expected message in ignored channel 'spam' to return false even with mention")
	}

	// 2. Message in channel with mode: "ignore" -> false
	msgInMuted := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-muted-1",
			ChannelID: "chan-muted",
			GuildID:   "guild-1",
			Content:   "Hey aerial help me here",
			Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if isFunnelBotTargeted(s, msgInMuted) {
		t.Errorf("Expected message in channel with mode: ignore to return false")
	}

	// 3. Message in channel with snowflake ID ignored -> false
	msgInSnowflake := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-snow-1",
			ChannelID: "111999",
			GuildID:   "guild-1",
			Content:   "Hey aerial help me here",
			Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if isFunnelBotTargeted(s, msgInSnowflake) {
		t.Errorf("Expected message in channel with ignored snowflake ID to return false")
	}

	// 4. Message in non-ignored channel with mention -> true
	msgInAllowed := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-allowed-1",
			ChannelID: "chan-allowed",
			GuildID:   "guild-1",
			Content:   "Hey aerial help me here",
			Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if !isFunnelBotTargeted(s, msgInAllowed) {
		t.Errorf("Expected message in allowed channel with mention to return true")
	}
}

func TestBuildDiscordPrompt_AdminFlag_ReplyContext_NoReplyGuidance(t *testing.T) {
	yamlContent := `
model: "gemini-2.5-flash"
admin_users:
  - "admin-user-999"
channels:
  default:
    mode: "threads"
  "chan-channel-mode":
    mode: "channel"
`
	setupTestConfig(t, yamlContent)

	// 1. Admin message in threads mode
	adminMsg := &discordgo.Message{
		ID:        "msg-admin-1",
		ChannelID: "chan-threads-mode",
		GuildID:   "guild-1",
		Content:   "Deploy release v1.0",
		Timestamp: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
		Author: &discordgo.User{
			ID:         "admin-user-999",
			Username:   "sysadmin",
			GlobalName: "System Admin",
			Bot:        false,
		},
	}

	promptAdmin := buildDiscordPrompt(adminMsg, "thread-admin-1", config.ChannelPolicy{Mode: "threads"})
	if !strings.Contains(promptAdmin, "- is_admin: true") {
		t.Errorf("Expected prompt to contain '- is_admin: true', got:\n%s", promptAdmin)
	}
	if strings.Contains(promptAdmin, "[NO_REPLY]") {
		t.Errorf("Expected threads mode prompt NOT to contain '[NO_REPLY]', got:\n%s", promptAdmin)
	}
	if !strings.Contains(promptAdmin, "Discord thread") {
		t.Errorf("Expected threads mode prompt to mention 'Discord thread', got:\n%s", promptAdmin)
	}

	// 2. Non-admin message in channel mode with reply context
	nonAdminMsg := &discordgo.Message{
		ID:        "msg-nonadmin-1",
		ChannelID: "chan-channel-mode",
		GuildID:   "guild-1",
		Content:   "I agree with the above proposal",
		Timestamp: time.Date(2026, 9, 2, 12, 5, 0, 0, time.UTC),
		Author: &discordgo.User{
			ID:         "regular-user-111",
			Username:   "bob",
			GlobalName: "Bob Builder",
			Bot:        false,
		},
		ReferencedMessage: &discordgo.Message{
			ID:      "ref-msg-0",
			Content: "Should we migrate the database?",
			Author: &discordgo.User{
				ID:       "alice-user-222",
				Username: "alice",
			},
		},
	}

	promptNonAdmin := buildDiscordPrompt(nonAdminMsg, "chan-channel-mode", config.ChannelPolicy{Mode: "channel"})
	if !strings.Contains(promptNonAdmin, "- is_admin: false") {
		t.Errorf("Expected prompt to contain '- is_admin: false', got:\n%s", promptNonAdmin)
	}
	if !strings.Contains(promptNonAdmin, "- replying_to:\n    author: \"@alice\"\n    content: \"Should we migrate the database?\"") {
		t.Errorf("Expected prompt to contain formatted reply reference, got:\n%s", promptNonAdmin)
	}
	if strings.Contains(promptNonAdmin, "[NO_REPLY]") {
		t.Errorf("Expected channel mode prompt NOT to contain [NO_REPLY] guidance, got:\n%s", promptNonAdmin)
	}
	if !strings.Contains(promptNonAdmin, "Discord channel") {
		t.Errorf("Expected channel mode prompt to mention 'Discord channel', got:\n%s", promptNonAdmin)
	}
}

func TestIsFunnelBotTargeted_ThreadInheritsParentChannelPolicy(t *testing.T) {
	yamlContent := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "ignore"
  aerial-dev:
    mode: "threads"
  spam-room:
    mode: "ignore"
`
	setupTestConfig(t, yamlContent)

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{ID: "bot-self-id", Username: "AerialBot"}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	// Parent channel 1: #aerial-dev (whitelisted)
	chanDev := &discordgo.Channel{
		ID:      "chan-dev-101",
		GuildID: "guild-1",
		Name:    "aerial-dev",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = s.State.ChannelAdd(chanDev)

	// Thread inside #aerial-dev
	threadDev := &discordgo.Channel{
		ID:       "thread-dev-999",
		GuildID:  "guild-1",
		ParentID: "chan-dev-101",
		Name:     "Discussion on Features",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	_ = s.State.ChannelAdd(threadDev)

	// Parent channel 2: #spam-room (explicitly ignored)
	chanSpam := &discordgo.Channel{
		ID:      "chan-spam-202",
		GuildID: "guild-1",
		Name:    "spam-room",
		Type:    discordgo.ChannelTypeGuildText,
	}
	_ = s.State.ChannelAdd(chanSpam)

	// Thread inside #spam-room
	threadSpam := &discordgo.Channel{
		ID:       "thread-spam-888",
		GuildID:  "guild-1",
		ParentID: "chan-spam-202",
		Name:     "Spam Discussion Thread",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	}
	_ = s.State.ChannelAdd(threadSpam)

	// A. Thread inside #aerial-dev (under default-deny) should inherit mode: "threads" and be targeted!
	msgInDevThread := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-dev-thread-1",
			ChannelID: "thread-dev-999",
			GuildID:   "guild-1",
			Content:   "Continuing discussion in whitelisted thread",
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if !isFunnelBotTargeted(s, msgInDevThread) {
		t.Errorf("Expected thread inside whitelisted #aerial-dev to be targeted under default-deny")
	}

	// B. Thread inside #spam-room should inherit mode: "ignore" and NOT be targeted even with mention!
	msgInSpamThread := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-spam-thread-1",
			ChannelID: "thread-spam-888",
			GuildID:   "guild-1",
			Content:   "Hey aerial answer me in spam thread",
			Mentions:  []*discordgo.User{{ID: "bot-self-id", Username: "AerialBot"}},
			Author:    &discordgo.User{ID: "user-1", Username: "alice", Bot: false},
		},
	}
	if isFunnelBotTargeted(s, msgInSpamThread) {
		t.Errorf("Expected thread inside ignored #spam-room to return false")
	}
}


func TestFunnelStartupRecovery(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB: database,
	})
	pool.Start()
	defer pool.Stop()

	// Should run cleanly on empty DB
	queue.RecoverInterrupted(database, pool)
}

func TestIsMessageableChannel(t *testing.T) {
	valid := []discordgo.ChannelType{
		discordgo.ChannelTypeGuildText,
		discordgo.ChannelTypeGuildNews,
		discordgo.ChannelTypeGuildNewsThread,
		discordgo.ChannelTypeGuildPublicThread,
		discordgo.ChannelTypeGuildPrivateThread,
	}
	for _, vt := range valid {
		if !isMessageableChannel(vt) {
			t.Errorf("Expected channel type %d to be messageable", vt)
		}
	}

	invalid := []discordgo.ChannelType{
		discordgo.ChannelTypeGuildVoice,
		discordgo.ChannelTypeGuildCategory,
		discordgo.ChannelTypeGuildForum,
		discordgo.ChannelTypeGuildStageVoice,
	}
	for _, it := range invalid {
		if isMessageableChannel(it) {
			t.Errorf("Expected channel type %d NOT to be messageable", it)
		}
	}
}

func TestRunStartupCatchUpSweep_NilAndEmptySafeguards(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})

	// 1. Nil session / DB / pool should be safe no-op
	RunStartupCatchUpSweep(context.Background(), nil, nil, nil)
	RunStartupCatchUpSweep(context.Background(), database, pool, nil)

	// 2. Valid empty session should complete without panic
	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{ID: "bot-123", Username: "Aerial"}

	// Force lastSweepAt to zero for test
	sweepMu.Lock()
	lastSweepAt = time.Time{}
	isSweeping.Store(false)
	sweepMu.Unlock()

	RunStartupCatchUpSweep(context.Background(), database, pool, s)
}

type mockCatchUpRoundTripper func(*http.Request) (*http.Response, error)

func (m mockCatchUpRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m(req)
}

func TestRunStartupCatchUpSweep_BotPolicy(t *testing.T) {
	setupTestConfig(t, `
channels:
  default:
    mode: "ignore"
  chan-bot-allowed:
    mode: "channel"
    ignore_bots: false
  chan-bot-ignored:
    mode: "channel"
    ignore_bots: true
`)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	defer pool.Stop()

	nowStr := time.Now().UTC().Format(time.RFC3339)
	s, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			var body []byte
			if strings.Contains(req.URL.Path, "threads/active") {
				body = []byte(`{"threads":[]}`)
			} else if strings.Contains(req.URL.Path, "guilds/") && strings.Contains(req.URL.Path, "/channels") {
				body = []byte(`[{"id":"chan-bot-allowed","guild_id":"guild-1","type":0},{"id":"chan-bot-ignored","guild_id":"guild-1","type":0}]`)
			} else if strings.Contains(req.URL.Path, "chan-bot-allowed") {
				body = []byte(`[{"id":"msg-bot-allowed","channel_id":"chan-bot-allowed","content":"Hey Aerial please help","timestamp":"` + nowStr + `","author":{"id":"peer-bot-1","username":"PeerBot","bot":true}}]`)
			} else if strings.Contains(req.URL.Path, "chan-bot-ignored") {
				body = []byte(`[{"id":"msg-bot-ignored","channel_id":"chan-bot-ignored","content":"Hey Aerial please help","timestamp":"` + nowStr + `","author":{"id":"peer-bot-2","username":"PeerBot","bot":true}}]`)
			} else {
				body = []byte(`[]`)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(body)),
			}, nil
		}),
	}
	s.State.User = &discordgo.User{ID: "bot-aerial-id", Username: "Aerial"}

	guild := &discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: "chan-bot-allowed", GuildID: "guild-1", Type: discordgo.ChannelTypeGuildText},
			{ID: "chan-bot-ignored", GuildID: "guild-1", Type: discordgo.ChannelTypeGuildText},
		},
		Roles: []*discordgo.Role{
			{
				ID:          "guild-1",
				Permissions: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory,
			},
		},
		Members: []*discordgo.Member{
			{
				GuildID:     "guild-1",
				User:        &discordgo.User{ID: "bot-aerial-id", Username: "Aerial"},
				Permissions: discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory,
			},
		},
	}
	_ = s.State.GuildAdd(guild)
	s.State.Guilds = []*discordgo.Guild{guild}
	_ = s.State.ChannelAdd(guild.Channels[0])
	_ = s.State.ChannelAdd(guild.Channels[1])
	_ = s.State.MemberAdd(guild.Members[0])

	sweepMu.Lock()
	lastSweepAt = time.Time{}
	isSweeping.Store(false)
	sweepMu.Unlock()

	RunStartupCatchUpSweep(context.Background(), database, pool, s)

	existsAllowed, err := db.MessageExists(database, "msg-bot-allowed")
	if err != nil {
		t.Fatalf("Failed to check message existence: %v", err)
	}
	if !existsAllowed {
		t.Errorf("Expected msg-bot-allowed to be retained and inserted into DB when ignore_bots is false")
	}

	existsIgnored, err := db.MessageExists(database, "msg-bot-ignored")
	if err != nil {
		t.Fatalf("Failed to check message existence: %v", err)
	}
	if existsIgnored {
		t.Errorf("Expected msg-bot-ignored to be skipped when ignore_bots is true")
	}
}

func TestGetDiscordChannel_QueueCacheIntegration(t *testing.T) {
	chanID := "100200300400500802"
	queue.InvalidateChannelCache(chanID)
	defer queue.InvalidateChannelCache(chanID)

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})

	// 1. Channel in state -> cached into queue and returned
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

	// 2. Remove from state, getDiscordChannel should retrieve from queue cache
	_ = s.State.ChannelRemove(chState)
	chCached := getDiscordChannel(s, chanID)
	if chCached == nil || chCached.Name != "state-cached-channel" {
		t.Fatalf("Expected reconstructed channel from queue cache, got: %+v", chCached)
	}

	// 3. Invalidate cache -> now returns nil when state is empty and no REST
	queue.InvalidateChannelCache(chanID)
	if chNone := getDiscordChannel(s, chanID); chNone != nil {
		t.Errorf("Expected nil when cache and state are empty, got: %+v", chNone)
	}

	// 4. REST fetch populates cache and state
	sRest, _ := discordgo.New("Bot fake-token")
	sRest.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			restCh := &discordgo.Channel{
				ID:   chanID,
				Name: "rest-fetched-channel",
				Type: discordgo.ChannelTypeGuildText,
			}
			data, _ := json.Marshal(restCh)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(data)),
			}, nil
		}),
	}
	chRest := getDiscordChannel(sRest, chanID)
	if chRest == nil || chRest.Name != "rest-fetched-channel" {
		t.Fatalf("Expected channel fetched via REST, got: %+v", chRest)
	}
	if snap, ok := queue.GetCachedChannel(chanID); !ok || snap.Name != "rest-fetched-channel" {
		t.Fatalf("Expected REST-fetched channel to be in queue cache, got: %+v", snap)
	}
}

func TestGetOrCreateThreadID_CachesSpawnedThread(t *testing.T) {
	setupTestConfig(t, `
channels:
  default:
    mode: "threads"
`)

	threadID := "100200300400500801"
	s, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				ch := &discordgo.Channel{
					ID:       threadID,
					GuildID:  "guild-1",
					ParentID: "100200300400500800",
					Name:     "Test Discussion Thread",
					Type:     discordgo.ChannelTypeGuildPublicThread,
				}
				data, _ := json.Marshal(ch)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(data)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}),
	}
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"})
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID:      "100200300400500800",
		GuildID: "guild-1",
		Name:    "threads-parent",
		Type:    discordgo.ChannelTypeGuildText,
	})

	m := &discordgo.Message{
		ID:        "msg-thread-spawn-1",
		ChannelID: "100200300400500800",
		GuildID:   "guild-1",
		Content:   "Start thread test",
		Author:    &discordgo.User{ID: "user-1", Username: "alice"},
	}

	queue.InvalidateChannelCache(threadID)
	defer queue.InvalidateChannelCache(threadID)

	thID, isThread := getOrCreateThreadID(s, m)
	if thID != threadID || !isThread {
		t.Fatalf("Expected spawned thread ID %q and true, got %q, %v", threadID, thID, isThread)
	}

	// Verify the spawned thread is now in queue cache!
	snap, ok := queue.GetCachedChannel(threadID)
	if !ok {
		t.Fatalf("Expected spawned thread %q to be in queue cache", threadID)
	}
	if snap.Name != "Test Discussion Thread" || !snap.IsThread || snap.ParentID != "100200300400500800" {
		t.Errorf("Unexpected snapshot in queue cache: %+v", snap)
	}
}

func TestConnectDiscordFunnel_Registration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := connectDiscordFunnel(ctx, nil, nil, "mock-gateway-token")
	if s == nil {
		t.Fatal("Expected connectDiscordFunnel to return non-nil session")
	}
	defer func() { _ = s.Close() }()

	chanID := "100200300400500803"
	queue.InvalidateChannelCache(chanID)
	defer queue.InvalidateChannelCache(chanID)

	ch := &discordgo.Channel{
		ID:   chanID,
		Name: "gateway-channel",
		Type: discordgo.ChannelTypeGuildText,
	}
	queue.CacheDiscordChannel(ch)
	snap, ok := queue.GetCachedChannel(chanID)
	if !ok || snap.Name != "gateway-channel" {
		t.Fatalf("Expected channel in cache")
	}

	queue.InvalidateChannelCache(chanID)
	if _, ok := queue.GetCachedChannel(chanID); ok {
		t.Fatalf("Expected channel to be removed from cache on invalidation")
	}
}

func TestIsThreadAlreadyExistsError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name: "discordgo RESTError with code 160004",
			err: &discordgo.RESTError{
				Message: &discordgo.APIErrorMessage{
					Code:    160004,
					Message: "A thread has already been created for this message",
				},
			},
			expected: true,
		},
		{
			name: "discordgo RESTError with raw response body containing 160004",
			err: &discordgo.RESTError{
				ResponseBody: []byte(`{"message": "A thread has already been created for this message", "code": 160004}`),
			},
			expected: true,
		},
		{
			name: "discordgo RESTError with 50001 Missing Access",
			err: &discordgo.RESTError{
				Message: &discordgo.APIErrorMessage{
					Code:    50001,
					Message: "Missing Access",
				},
			},
			expected: false,
		},
		{
			name:     "wrapped string error with 160004 code",
			err:      errors.New("HTTP 400 Bad Request, code 160004: A thread has already been created for this message"),
			expected: true,
		},
		{
			name:     "generic system error",
			err:      errors.New("network connection timeout"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isThreadAlreadyExistsError(tc.err)
			if got != tc.expected {
				t.Errorf("isThreadAlreadyExistsError() = %v; want %v", got, tc.expected)
			}
		})
	}
}

func TestGetOrCreateThreadID_ThreadAlreadyExists(t *testing.T) {
	setupTestConfig(t, `
channels:
  default:
    mode: "threads"
`)

	messageID := "200300400500600701"
	parentChanID := "200300400500600700"
	guildID := "guild-alpha-1"

	queue.InvalidateChannelCache(messageID)
	defer queue.InvalidateChannelCache(messageID)

	s, err := discordgo.New("Bot mock-token")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				errBody := []byte(`{"message": "A thread has already been created for this message", "code": 160004}`)
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(errBody)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}),
	}

	_ = s.State.GuildAdd(&discordgo.Guild{ID: guildID})
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID:      parentChanID,
		GuildID: guildID,
		Name:    "feature-requests",
		Type:    discordgo.ChannelTypeGuildText,
	})

	m := &discordgo.Message{
		ID:        messageID,
		ChannelID: parentChanID,
		GuildID:   guildID,
		Content:   "Build an automated test suite",
		Author:    &discordgo.User{ID: "user-42", Username: "alex"},
	}

	targetID, isThread := getOrCreateThreadID(s, m)
	if targetID != messageID || !isThread {
		t.Fatalf("getOrCreateThreadID() = (%q, %t); want (%q, true)", targetID, isThread, messageID)
	}

	snap, ok := queue.GetCachedChannel(messageID)
	if !ok {
		t.Fatalf("Expected thread %q to be in queue cache", messageID)
	}
	if !snap.IsThread || snap.ParentID != parentChanID {
		t.Errorf("Unexpected cached thread metadata: %+v", snap)
	}
}

func resetFunnelGlobals(t *testing.T) {
	t.Helper()
	sweepMu.Lock()
	lastSweepAt = time.Time{}
	isSweeping.Store(false)
	sweepMu.Unlock()
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

func TestConnectDiscordFunnel_NilAndDefaults(t *testing.T) {
	if dg := connectDiscordFunnel(nil, nil, nil, ""); dg != nil {
		t.Errorf("Expected nil session for empty token, got %v", dg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = connectDiscordFunnel(ctx, nil, nil, "")
}

func TestConnectDiscordFunnel_EventHandlers_And_Lifecycle(t *testing.T) {
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

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "funnel_test.db")
	database, err := db.InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	s := connectDiscordFunnel(ctx, database, pool, "mock-valid-token-for-handlers")
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

	// Poll for message in DB
	var dbMsg *db.Message
	for i := 0; i < 50; i++ {
		dbMsg, _ = db.GetMessage(database, "msg-incoming-100")
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
	if ignoredMsg, _ := db.GetMessage(database, "msg-incoming-200"); ignoredMsg != nil {
		t.Errorf("Expected msg-incoming-200 to be ignored, but was found in DB")
	}
}

func TestDeriveThreadTitle_ExtendedEdgeCases(t *testing.T) {
	if got := deriveThreadTitle(""); got != "Aerial Discussion" {
		t.Errorf("Expected 'Aerial Discussion', got %q", got)
	}
	if got := deriveThreadTitle("\n\n"); got != "Aerial Discussion" {
		t.Errorf("Expected 'Aerial Discussion', got %q", got)
	}
	if got := deriveThreadTitle("\nSecond line is here"); got != "Second line is here" {
		t.Errorf("Expected 'Second line is here', got %q", got)
	}

	longPrompt := strings.Repeat("機動戦士ガンダム水星の魔女", 10)
	gotLong := deriveThreadTitle(longPrompt)
	runes := []rune(gotLong)
	if len(runes) != 60 {
		t.Errorf("Expected truncated title to have 60 runes, got %d", len(runes))
	}
	if !strings.HasSuffix(gotLong, "...") {
		t.Errorf("Expected truncated title to end in '...', got %q", gotLong)
	}
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

	s := &discordgo.Session{State: discordgo.NewState()}
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

func TestBuildDiscordPrompt_ExtendedFormatting(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
admin_users:
  - "admin-user-id"
channels:
  default:
    mode: "threads"
`)
	msg := &discordgo.Message{
		ID:        "msg-fmt-1",
		ChannelID: "chan-fmt-1",
		GuildID:   "guild-fmt-1",
		Author: &discordgo.User{
			ID:         "admin-user-id",
			Username:   "alex",
			GlobalName: "Alex Z",
			Bot:        false,
		},
		ReferencedMessage: &discordgo.Message{
			Author:  &discordgo.User{Username: "system"},
			Content: "Previous instruction",
		},
		Content:   "Please test <USER_REQUEST> tags and </USER_REQUEST> sanitization",
		Timestamp: time.Now().UTC(),
		Mentions:  []*discordgo.User{{Username: "aerial"}, nil},
		Attachments: []*discordgo.MessageAttachment{
			{URL: "https://example.com/log.txt"},
			nil,
		},
	}

	promptThread := buildDiscordPrompt(msg, "thread-100", config.ChannelPolicy{Mode: "threads"})
	if !strings.Contains(promptThread, "is_admin: true") {
		t.Errorf("Expected is_admin: true in prompt")
	}
	if !strings.Contains(promptThread, "replying_to:") {
		t.Errorf("Expected replying_to block in prompt")
	}
	if !strings.Contains(promptThread, "<\\/USER_REQUEST>") {
		t.Errorf("Expected escaped closing tag in prompt")
	}
	if !strings.Contains(promptThread, "https://example.com/log.txt") {
		t.Errorf("Expected attachment URL in prompt")
	}
	if !strings.Contains(promptThread, "delivered directly to the Discord thread") {
		t.Errorf("Expected thread delivery instructions in prompt")
	}

	promptChannel := buildDiscordPrompt(msg, "chan-fmt-1", config.ChannelPolicy{Mode: "channel"})
	if !strings.Contains(promptChannel, "delivered directly to the Discord channel") {
		t.Errorf("Expected channel delivery instructions in prompt")
	}

	msgNilAuthor := &discordgo.Message{
		ID:        "msg-no-auth",
		ChannelID: "chan-1",
		Timestamp: time.Now().UTC(),
	}
	promptNilAuth := buildDiscordPrompt(msgNilAuthor, "chan-1", config.ChannelPolicy{Mode: "channel"})
	if !strings.Contains(promptNilAuth, "is_admin: false") {
		t.Errorf("Expected is_admin: false for nil author")
	}
}

func TestRunStartupCatchUpSweep_AllBranches(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
`)
	resetFunnelGlobals(t)
	defer resetFunnelGlobals(t)

	// 1. Nil safety check
	RunStartupCatchUpSweep(nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "sweep_test.db")
	database, err := db.InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	s, _ := discordgo.New("Bot mock-sweep-token")
	s.State.User = &discordgo.User{ID: "bot-sweep-id", Username: "AerialBot"}

	// Guild with channels and threads
	g := &discordgo.Guild{
		ID: "g-sweep-1",
		Channels: []*discordgo.Channel{
			{ID: "ch-text-1", Name: "general", GuildID: "g-sweep-1", Type: discordgo.ChannelTypeGuildText},
			{ID: "ch-voice-1", Name: "Voice", GuildID: "g-sweep-1", Type: discordgo.ChannelTypeGuildVoice},
		},
		Threads: []*discordgo.Channel{
			{ID: "th-active-1", Name: "sub-thread", GuildID: "g-sweep-1", ParentID: "ch-text-1", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	}
	_ = s.State.GuildAdd(g)

	// Pre-insert an existing message so it tests deduplication
	_ = db.InsertMessage(database, db.Message{
		ID:        "msg-existing-1",
		ThreadID:  "ch-text-1",
		GuildID:   "g-sweep-1",
		Status:    db.StatusCompleted,
		CreatedAt: time.Now().UTC().Add(-10 * time.Minute),
		UpdatedAt: time.Now().UTC(),
	})

	recentTime := time.Now().UTC().Add(-5 * time.Minute)
	oldTime := time.Now().UTC().Add(-4 * time.Hour) // > 2 hour cutoff

	mockMessages := []*discordgo.Message{
		{
			ID:        "msg-new-sweep-1",
			ChannelID: "ch-text-1",
			GuildID:   "g-sweep-1",
			Author:    &discordgo.User{ID: "u-alex", Username: "alex", Bot: false},
			Content:   "Missed message during downtime",
			Timestamp: recentTime,
		},
		{
			ID:        "msg-existing-1",
			ChannelID: "ch-text-1",
			GuildID:   "g-sweep-1",
			Author:    &discordgo.User{ID: "u-alex", Username: "alex", Bot: false},
			Content:   "Already processed message",
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
	}

	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads/active") {
				respBody, _ := json.Marshal(map[string]interface{}{
					"threads": []*discordgo.Channel{
						{ID: "th-active-1", Name: "sub-thread", GuildID: "g-sweep-1", ParentID: "ch-text-1", Type: discordgo.ChannelTypeGuildPublicThread},
					},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/messages") {
				respBody, _ := json.Marshal(mockMessages)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}),
	}

	RunStartupCatchUpSweep(ctx, database, pool, s)

	// Verify msg-new-sweep-1 was inserted and enqueued
	dbMsg, err := db.GetMessage(database, "msg-new-sweep-1")
	if err != nil || dbMsg == nil {
		t.Fatalf("Expected msg-new-sweep-1 to be inserted during sweep, got err: %v", err)
	}

	// 2. Test 2-minute rate limit cooldown
	RunStartupCatchUpSweep(ctx, database, pool, s) // should exit immediately

	// 3. Test Circuit Breaker with 5 consecutive REST errors
	resetFunnelGlobals(t)
	s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"message":"internal error"}`))),
		}, nil
	})
	RunStartupCatchUpSweep(ctx, database, pool, s)
}

func TestGetOrCreateThreadID_AllDetailedBranches(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)

	guildID := "g-detail-1"
	parentID := "ch-detail-parent"

	s, _ := discordgo.New("Bot mock-detail-token")
	_ = s.State.GuildAdd(&discordgo.Guild{ID: guildID})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: parentID, GuildID: guildID, Type: discordgo.ChannelTypeGuildText})

	// 1. In-memory cache hit for thread
	threadID1 := "th-cached-101"
	queue.InvalidateChannelCache(threadID1)
	defer queue.InvalidateChannelCache(threadID1)
	queue.CacheDiscordChannel(&discordgo.Channel{ID: threadID1, GuildID: guildID, ParentID: parentID, Type: discordgo.ChannelTypeGuildPublicThread})

	mCacheHit := &discordgo.Message{ID: threadID1, ChannelID: parentID, GuildID: guildID}
	gotID, isTh := getOrCreateThreadID(s, mCacheHit)
	if gotID != threadID1 || !isTh {
		t.Errorf("Expected in-memory cache hit (%s, true), got (%s, %t)", threadID1, gotID, isTh)
	}

	// 2. Session state hit for thread
	threadID2 := "th-state-102"
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: threadID2, GuildID: guildID, ParentID: parentID, Type: discordgo.ChannelTypeGuildPublicThread})
	mStateHit := &discordgo.Message{ID: threadID2, ChannelID: parentID, GuildID: guildID}
	gotID, isTh = getOrCreateThreadID(s, mStateHit)
	if gotID != threadID2 || !isTh {
		t.Errorf("Expected session state hit (%s, true), got (%s, %t)", threadID2, gotID, isTh)
	}

	// 3. REST error other than 160004 (e.g. 50001 Missing Access)
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				return &http.Response{
					StatusCode: http.StatusForbidden,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"message": "Missing Access", "code": 50001}`))),
				}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte("{}")))}, nil
		}),
	}
	mRestErr := &discordgo.Message{ID: "msg-rest-err-1", ChannelID: parentID, GuildID: guildID, Content: "test prompt"}
	gotID, isTh = getOrCreateThreadID(s, mRestErr)
	if gotID != parentID || isTh {
		t.Errorf("Expected fallback to channel ID on REST error, got (%s, %t)", gotID, isTh)
	}

	// 4. REST success creating thread
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				respBody, _ := json.Marshal(&discordgo.Channel{
					ID:       "th-created-new-200",
					GuildID:  guildID,
					ParentID: parentID,
					Type:     discordgo.ChannelTypeGuildPublicThread,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte("{}")))}, nil
		}),
	}
	mRestOk := &discordgo.Message{ID: "msg-rest-ok-1", ChannelID: parentID, GuildID: guildID, Content: "test ok prompt"}
	gotID, isTh = getOrCreateThreadID(s, mRestOk)
	if gotID != "th-created-new-200" || !isTh {
		t.Errorf("Expected created thread ID (th-created-new-200, true), got (%s, %t)", gotID, isTh)
	}

	// 5. REST 160004 with s.Channel fallback success
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"message": "A thread has already been created for this message", "code": 160004}`))),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/channels/200300400500600799") {
				respBody, _ := json.Marshal(&discordgo.Channel{
					ID:       "200300400500600799",
					GuildID:  guildID,
					ParentID: parentID,
					Type:     discordgo.ChannelTypeGuildPublicThread,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(bytes.NewReader([]byte("{}")))}, nil
		}),
	}
	m160004 := &discordgo.Message{ID: "200300400500600799", ChannelID: parentID, GuildID: guildID, Content: "test 160004"}
	gotID, isTh = getOrCreateThreadID(s, m160004)
	if gotID != "200300400500600799" || !isTh {
		t.Errorf("Expected resolved thread ID (200300400500600799, true), got (%s, %t)", gotID, isTh)
	}
}

func TestGetDiscordChannel_Extended(t *testing.T) {
	if ch := getDiscordChannel(nil, ""); ch != nil {
		t.Errorf("Expected nil for empty channel ID, got %v", ch)
	}

	queue.InvalidateChannelCache("snap-th-100")
	defer queue.InvalidateChannelCache("snap-th-100")
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:       "snap-th-100",
		Name:     "snap-thread",
		GuildID:  "g-1",
		ParentID: "p-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})

	ch := getDiscordChannel(nil, "snap-th-100")
	if ch == nil || ch.Type != discordgo.ChannelTypeGuildPublicThread {
		t.Errorf("Expected public thread channel from cache, got %+v", ch)
	}

	s, _ := discordgo.New("Bot mock-token")
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			respBody, _ := json.Marshal(&discordgo.Channel{
				ID:      "100200300400500600",
				Name:    "fetched-channel",
				GuildID: "g-1",
				Type:    discordgo.ChannelTypeGuildText,
			})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			}, nil
		}),
	}
	fetchedCh := getDiscordChannel(s, "100200300400500600")
	if fetchedCh == nil || fetchedCh.Name != "fetched-channel" {
		t.Errorf("Expected fetched channel, got %+v", fetchedCh)
	}
}

func TestConnectDiscordFunnel_DBInsertError(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: closedDB})
	s := connectDiscordFunnel(ctx, closedDB, pool, "mock-token-closed-db")
	if s == nil {
		t.Fatalf("Expected non-nil session")
	}
	defer func() { _ = s.Close() }()

	_ = s.State.GuildAdd(&discordgo.Guild{ID: "g-closed-1"})
	_ = s.State.ChannelAdd(&discordgo.Channel{ID: "ch-closed-1", GuildID: "g-closed-1", Type: discordgo.ChannelTypeGuildText})

	dispatchSessionEvent(s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-closed-db-1",
			ChannelID: "ch-closed-1",
			GuildID:   "g-closed-1",
			Author:    &discordgo.User{ID: "u-1", Username: "alex"},
			Content:   "test insert failure",
			Timestamp: time.Now().UTC(),
		},
	})
	time.Sleep(50 * time.Millisecond)
}

func TestDeriveThreadTitle_AdditionalLines(t *testing.T) {
	content := "\n  Real Subject Title Here  \nThird Line"
	got := deriveThreadTitle(content)
	if got != "Real Subject Title Here" {
		t.Errorf("Expected 'Real Subject Title Here', got %q", got)
	}
}

func TestGetOrCreateThreadID_ParentAndGuildMissing(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	s, _ := discordgo.New("Bot mock-token")
	s.State = discordgo.NewState()
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "g-empty-test"})
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID:      "ch-empty-test",
		GuildID: "g-empty-test",
		Type:    discordgo.ChannelTypeGuildText,
	})

	// 1. Thread creation returns thread with empty ParentID and GuildID
	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				respBody, _ := json.Marshal(&discordgo.Channel{
					ID:   "thread-created-1",
					Type: discordgo.ChannelTypeGuildPublicThread,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}),
	}

	m := &discordgo.Message{
		ID:        "msg-thread-empty",
		ChannelID: "ch-empty-test",
		GuildID:   "g-empty-test",
		Content:   "start thread",
	}

	thID, ok := getOrCreateThreadID(s, m)
	if !ok || thID != "thread-created-1" {
		t.Errorf("Expected thread-created-1, got %s (ok=%t)", thID, ok)
	}

	// 2. nil message
	nilID, nilOk := getOrCreateThreadID(s, nil)
	if nilOk || nilID != "" {
		t.Errorf("Expected empty thread ID for nil message, got %s (ok=%t)", nilID, nilOk)
	}
}

func TestIsFunnelBotTargeted_ReferencesAndRoles(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	s, _ := discordgo.New("Bot mock-token")

	// Message with ReferencedMessage
	msgRef := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:                "msg-ref-1",
			ChannelID:         "ch-1",
			GuildID:           "guild-ref-test",
			Content:           "simple discussion without keyword",
			Author:            &discordgo.User{ID: "user-ref-1", Username: "user1"},
			ReferencedMessage: &discordgo.Message{ID: "ref-0"},
		},
	}
	if !isFunnelBotTargeted(s, msgRef) {
		t.Errorf("Expected isFunnelBotTargeted=true for message with ReferencedMessage")
	}

	// Message with MentionRoles
	msgRole := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:           "msg-role-1",
			ChannelID:    "ch-1",
			GuildID:      "guild-role-test",
			Content:      "simple discussion without keyword",
			Author:       &discordgo.User{ID: "user-role-1", Username: "user2"},
			MentionRoles: []string{"role-999"},
		},
	}
	if !isFunnelBotTargeted(s, msgRole) {
		t.Errorf("Expected isFunnelBotTargeted=true for message with MentionRoles")
	}
}

func TestConnectDiscordFunnel_NilContextAndEmptyToken(t *testing.T) {
	// 1. Empty token
	if s := connectDiscordFunnel(context.Background(), nil, nil, ""); s != nil {
		t.Errorf("Expected nil session for empty token")
	}

	// 2. Nil context with mock token
	s := connectDiscordFunnel(nil, nil, nil, "mock-nil-ctx-token")
	if s == nil {
		t.Errorf("Expected non-nil session for nil context with token")
	} else {
		_ = s.Close()
	}
}

func TestRunStartupCatchUpSweep_ExtendedBranches(t *testing.T) {
	resetFunnelGlobals(t)
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	// 1. Fallback to s.UserGuilds when s.State.Guilds is empty
	s, _ := discordgo.New("Bot mock-token")
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-user-id"}

	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/users/@me/guilds") {
				respBody, _ := json.Marshal([]*discordgo.UserGuild{
					{ID: "g-sweep-fallback"},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/guilds/g-sweep-fallback/threads/active") {
				respBody, _ := json.Marshal(&discordgo.ThreadsList{
					Threads: []*discordgo.Channel{
						{ID: "th-fallback-1", Type: discordgo.ChannelTypeGuildPublicThread, GuildID: "g-sweep-fallback"},
					},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/guilds/g-sweep-fallback/channels") {
				respBody, _ := json.Marshal([]*discordgo.Channel{
					{ID: "ch-fallback-1", Type: discordgo.ChannelTypeGuildText, GuildID: "g-sweep-fallback"},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/channels/th-fallback-1/messages") || strings.Contains(req.URL.Path, "/channels/ch-fallback-1/messages") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}),
	}

	RunStartupCatchUpSweep(context.Background(), database, pool, s)

	// 2. Circuit breaker path (5 consecutive errors)
	resetFunnelGlobals(t)
	sErr, _ := discordgo.New("Bot mock-token")
	sErr.State = discordgo.NewState()
	_ = sErr.State.GuildAdd(&discordgo.Guild{ID: "g-err-1"})
	_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "ch-err-1", GuildID: "g-err-1", Type: discordgo.ChannelTypeGuildText})
	_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "ch-err-2", GuildID: "g-err-1", Type: discordgo.ChannelTypeGuildText})
	_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "ch-err-3", GuildID: "g-err-1", Type: discordgo.ChannelTypeGuildText})
	_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "ch-err-4", GuildID: "g-err-1", Type: discordgo.ChannelTypeGuildText})
	_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "ch-err-5", GuildID: "g-err-1", Type: discordgo.ChannelTypeGuildText})
	_ = sErr.State.ChannelAdd(&discordgo.Channel{ID: "ch-err-6", GuildID: "g-err-1", Type: discordgo.ChannelTypeGuildText})

	sErr.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/guilds/g-err-1/channels") {
				respBody, _ := json.Marshal([]*discordgo.Channel{
					{ID: "ch-err-1", Type: discordgo.ChannelTypeGuildText, GuildID: "g-err-1"},
					{ID: "ch-err-2", Type: discordgo.ChannelTypeGuildText, GuildID: "g-err-1"},
					{ID: "ch-err-3", Type: discordgo.ChannelTypeGuildText, GuildID: "g-err-1"},
					{ID: "ch-err-4", Type: discordgo.ChannelTypeGuildText, GuildID: "g-err-1"},
					{ID: "ch-err-5", Type: discordgo.ChannelTypeGuildText, GuildID: "g-err-1"},
					{ID: "ch-err-6", Type: discordgo.ChannelTypeGuildText, GuildID: "g-err-1"},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/messages") {
				return nil, errors.New("simulated network failure")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}),
	}

	RunStartupCatchUpSweep(context.Background(), database, pool, sErr)

	// 3. Bot-self message and untargeted message skipped
	resetFunnelGlobals(t)
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	sTarget, _ := discordgo.New("Bot mock-token")
	sTarget.State = discordgo.NewState()
	sTarget.State.User = &discordgo.User{ID: "bot-user-123"}
	_ = sTarget.State.GuildAdd(&discordgo.Guild{ID: "g-target-1"})
	_ = sTarget.State.ChannelAdd(&discordgo.Channel{ID: "ch-target-1", GuildID: "g-target-1", Type: discordgo.ChannelTypeGuildText})

	sTarget.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/messages") {
				respBody, _ := json.Marshal([]*discordgo.Message{
					{
						ID:        "msg-from-bot-self",
						ChannelID: "ch-target-1",
						GuildID:   "g-target-1",
						Author:    &discordgo.User{ID: "bot-user-123"},
						Timestamp: time.Now().UTC(),
					},
					{
						ID:        "msg-not-targeted",
						ChannelID: "ch-target-1",
						GuildID:   "g-target-1",
						Author:    &discordgo.User{ID: "random-user-1"},
						Content:   "just random chatter not mentioning bot",
						Timestamp: time.Now().UTC(),
					},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}),
	}

	RunStartupCatchUpSweep(context.Background(), database, pool, sTarget)

	// 4. Cancelled context aborting sweep loop
	resetFunnelGlobals(t)
	cancelCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	RunStartupCatchUpSweep(cancelCtx, database, pool, sTarget)
}

func TestGetOrCreateThreadID_160004_EmptyParentAndGuild(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	s, _ := discordgo.New("Bot mock-token")
	s.State = discordgo.NewState()
	_ = s.State.GuildAdd(&discordgo.Guild{ID: "g-160004"})
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID:      "ch-160004",
		GuildID: "g-160004",
		Type:    discordgo.ChannelTypeGuildText,
	})

	s.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/threads") {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"code": 160004, "message": "Thread already exists for this message"}`))),
				}, nil
			}
			if strings.Contains(req.URL.Path, "/channels/160004123456789012") {
				respBody, _ := json.Marshal(&discordgo.Channel{
					ID:   "160004123456789012",
					Type: discordgo.ChannelTypeGuildPublicThread,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}),
	}

	m := &discordgo.Message{
		ID:        "160004123456789012",
		ChannelID: "ch-160004",
		GuildID:   "g-160004",
		Content:   "thread exists test",
	}

	thID, ok := getOrCreateThreadID(s, m)
	if !ok || thID != "160004123456789012" {
		t.Errorf("Expected 160004123456789012, got %s (ok=%t)", thID, ok)
	}
}

func TestRunStartupCatchUpSweep_AllDetailedBranches(t *testing.T) {
	resetFunnelGlobals(t)
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	// 1. Nil context branch
	RunStartupCatchUpSweep(nil, database, pool, nil)

	// 2. Already sweeping branch
	resetFunnelGlobals(t)
	isSweeping.Store(true)
	RunStartupCatchUpSweep(context.Background(), database, pool, &discordgo.Session{})
	isSweeping.Store(false)

	// 3. s.Token == "" with guilds in state (triggers break)
	resetFunnelGlobals(t)
	sNoToken := &discordgo.Session{
		State: discordgo.NewState(),
		Token: "",
	}
	_ = sNoToken.State.GuildAdd(&discordgo.Guild{ID: "g-no-token"})
	RunStartupCatchUpSweep(context.Background(), database, pool, sNoToken)

	// 4. Permission denial, ignored channel, nil author, bot self, untargeted, and insert error
	resetFunnelGlobals(t)
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
  ignored-ch:
    mode: "ignore"
`)

	sFull, _ := discordgo.New("Bot mock-token")
	sFull.State = discordgo.NewState()
	sFull.State.User = &discordgo.User{ID: "bot-user-id"}
	_ = sFull.State.GuildAdd(&discordgo.Guild{ID: "g-full"})
	_ = sFull.State.ChannelAdd(&discordgo.Channel{ID: "ignored-ch", GuildID: "g-full", Type: discordgo.ChannelTypeGuildText})
	_ = sFull.State.ChannelAdd(&discordgo.Channel{ID: "no-perm-ch", GuildID: "g-full", Type: discordgo.ChannelTypeGuildText})
	_ = sFull.State.ChannelAdd(&discordgo.Channel{ID: "valid-ch", GuildID: "g-full", Type: discordgo.ChannelTypeGuildText})

	_ = sFull.State.MemberAdd(&discordgo.Member{
		GuildID: "g-full",
		User:    &discordgo.User{ID: "bot-user-id"},
		Roles:   []string{"role-zero"},
	})
	_ = sFull.State.RoleAdd("g-full", &discordgo.Role{
		ID:          "role-zero",
		Permissions: 0,
	})

	sFull.Client = &http.Client{
		Transport: mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/channels/valid-ch/messages") {
				respBody, _ := json.Marshal([]*discordgo.Message{
					{
						ID:        "msg-nil-author",
						ChannelID: "valid-ch",
						GuildID:   "g-full",
						Author:    nil,
					},
					{
						ID:        "msg-bot-self",
						ChannelID: "valid-ch",
						GuildID:   "g-full",
						Author:    &discordgo.User{ID: "bot-user-id"},
						Timestamp: time.Now().UTC(),
					},
					{
						ID:        "msg-untargeted",
						ChannelID: "valid-ch",
						GuildID:   "g-full",
						Author:    &discordgo.User{ID: "other-user"},
						Content:   "untargeted chatter",
						Timestamp: time.Now().UTC(),
					},
					{
						ID:        "msg-targeted-valid",
						ChannelID: "valid-ch",
						GuildID:   "g-full",
						Author:    &discordgo.User{ID: "other-user", Username: "User"},
						Content:   "aerial help me",
						Timestamp: time.Now().UTC(),
					},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}),
	}

	RunStartupCatchUpSweep(context.Background(), database, pool, sFull)

	// 5. DB insert error during sweep
	resetFunnelGlobals(t)
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	RunStartupCatchUpSweep(context.Background(), closedDB, pool, sFull)
}







