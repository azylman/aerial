package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"


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
	if _, err := config.LoadConfigFromPaths(yamlPath); err != nil {
		t.Fatalf("Failed to load test config from %s: %v", yamlPath, err)
	}
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

func TestConnectDiscordFunnel_Variants(t *testing.T) {
	// 1. Empty token -> returns nil
	if dg := connectDiscordFunnel(context.Background(), nil, nil, ""); dg != nil {
		t.Errorf("expected nil for empty token")
	}

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so background reconnect loop returns

	dg := connectDiscordFunnel(ctx, database, pool, "fake-bot-token")
	if dg == nil {
		t.Fatalf("expected non-nil discordgo session")
	}
}

func TestFunnel_PromptAndSweepEdgeCases(t *testing.T) {
	// 1. deriveThreadTitle with very long string > 60 runes and multiple lines
	longPrompt := "Line 1 of the user message that is quite long and goes on and on\nLine 2"
	title := deriveThreadTitle(longPrompt)
	if len([]rune(title)) > 60 || strings.Contains(title, "\n") {
		t.Errorf("expected clean one-line title <= 60 runes, got %q", title)
	}

	// 2. resolveGuildID fallbacks
	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	m := &discordgo.Message{
		ID:        "m1",
		ChannelID: "c1",
		GuildID:   "",
	}
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID: "g-state-1",
		Channels: []*discordgo.Channel{
			{
				ID:      "c1",
				GuildID: "g-state-1",
			},
		},
	})
	if gID := resolveGuildID(s, m); gID != "g-state-1" {
		t.Errorf("expected guild ID g-state-1 from channel state, got %q", gID)
	}

	// 3. buildDiscordPrompt with attachments and referenced message
	mWithAttach := &discordgo.Message{
		ID:        "m-attach",
		ChannelID: "c1",
		Content:   "Check this file",
		Attachments: []*discordgo.MessageAttachment{
			{
				ID:          "att-1",
				Filename:    "image.png",
				ContentType: "image/png",
				URL:         "https://cdn.discordapp.com/attachments/1/2/image.png",
				Size:        1024,
			},
		},
		ReferencedMessage: &discordgo.Message{
			ID:      "ref-1",
			Content: "Previous question",
			Author:  &discordgo.User{Username: "Alice"},
		},
	}
	p := buildDiscordPrompt(mWithAttach, "th-1", config.ChannelPolicy{Mode: "threads"})
	if !strings.Contains(p, "image.png") || !strings.Contains(p, "Alice") {
		t.Errorf("expected prompt to contain attachment and reference details, got: %s", p)
	}

	// 4. RunStartupCatchUpSweep with nil session/db/pool
	RunStartupCatchUpSweep(nil, nil, nil, nil)
}

func TestDiscordGatewayHandlers(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	var enqueuedCount atomic.Int32
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB: database,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			enqueuedCount.Add(1)
		},
	})
	pool.Start()
	defer pool.Stop()

	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	s.State.User = &discordgo.User{
		ID:       "bot-100",
		Username: "aerial",
	}

	// 1. Ready handler
	s.State.GuildAdd(&discordgo.Guild{
		ID: "g-ready",
		Channels: []*discordgo.Channel{
			{ID: "c-ready-1", Name: "general", GuildID: "g-ready"},
		},
		Threads: []*discordgo.Channel{
			{ID: "th-ready-1", Name: "thread-1", ParentID: "c-ready-1", GuildID: "g-ready", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	})
	handleDiscordReady(context.Background(), database, pool, s, &discordgo.Ready{
		User: &discordgo.User{ID: "bot-100", Username: "aerial"},
	})

	// 2. GuildCreate handler
	handleDiscordGuildCreate(s, &discordgo.GuildCreate{
		Guild: &discordgo.Guild{
			ID: "g-create",
			Channels: []*discordgo.Channel{
				{ID: "c-guild-1", Name: "chat", GuildID: "g-create"},
			},
			Threads: []*discordgo.Channel{
				{ID: "th-guild-1", Name: "guild-thread", ParentID: "c-guild-1", GuildID: "g-create", Type: discordgo.ChannelTypeGuildPublicThread},
			},
		},
	})

	// 3. Channel create, update, delete
	ch1 := &discordgo.Channel{ID: "c-crud-1", Name: "crud-chan"}
	handleDiscordChannelCreate(s, &discordgo.ChannelCreate{Channel: ch1})
	handleDiscordChannelUpdate(s, &discordgo.ChannelUpdate{Channel: ch1})
	handleDiscordChannelDelete(s, &discordgo.ChannelDelete{Channel: ch1})

	// 4. Thread create, update, delete
	th1 := &discordgo.Channel{ID: "th-crud-1", Name: "crud-thread", Type: discordgo.ChannelTypeGuildPublicThread}
	handleDiscordThreadCreate(s, &discordgo.ThreadCreate{Channel: th1})
	handleDiscordThreadUpdate(s, &discordgo.ThreadUpdate{Channel: th1})
	handleDiscordThreadDelete(s, &discordgo.ThreadDelete{Channel: th1})

	// 5. Disconnect and Resumed
	handleDiscordDisconnect(s, &discordgo.Disconnect{})
	handleDiscordResumed(s, &discordgo.Resumed{})

	// 6. MessageCreate handler
	// Message from bot itself -> ignored
	handleDiscordMessageCreate(database, pool, s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:      "m-bot-self",
			Author:  &discordgo.User{ID: "bot-100"},
			Content: "self message",
		},
	})

	// Message targeted at bot
	handleDiscordMessageCreate(database, pool, s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "m-user-targeted",
			ChannelID: "dm-user-1",
			GuildID:   "",
			Author:    &discordgo.User{ID: "u-42", Username: "alex"},
			Content:   "Aerial please test",
			Timestamp: time.Now().UTC(),
		},
	})

	// Verify message in DB
	msg, err := db.GetMessage(database, "m-user-targeted")
	if err != nil || msg == nil {
		t.Fatalf("expected m-user-targeted to be in DB: %v", err)
	}
	if msg.AuthorID != "u-42" {
		t.Errorf("expected author u-42, got %s", msg.AuthorID)
	}
}

func TestRunStartupCatchUpSweep_Scenarios(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	// Reset sweep throttle
	sweepMu.Lock()
	lastSweepAt = time.Time{}
	isSweeping.Store(false)
	sweepMu.Unlock()

	// Insert recent thread into DB
	_ = db.InsertMessage(database, db.Message{
		ID:         "m-exist-1",
		ThreadID:   "th-sweep-1",
		AuthorID:   "u-1",
		AuthorName: "User",
		Content:    "Existing message",
		Status:     db.StatusCompleted,
		CreatedAt:  time.Now().UTC().Add(-10 * time.Minute),
		UpdatedAt:  time.Now().UTC(),
	})

	s, err := discordgo.New("Bot test-token")
	if err != nil {
		t.Fatalf("discordgo.New failed: %v", err)
	}
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-sweep-1", Username: "aerial"}

	// Mock Discord API response for ChannelMessages
	now := time.Now().UTC()
	recentMsg := &discordgo.Message{
		ID:        "m-missed-1",
		ChannelID: "th-sweep-1",
		Author:    &discordgo.User{ID: "u-2", Username: "Alex"},
		Content:   "Aerial please check this missed task",
		Timestamp: now.Add(-5 * time.Minute),
	}
	oldMsg := &discordgo.Message{
		ID:        "m-old-1",
		ChannelID: "th-sweep-1",
		Author:    &discordgo.User{ID: "u-2", Username: "Alex"},
		Content:   "Aerial old prompt",
		Timestamp: now.Add(-5 * time.Hour), // Older than 2 hour cutoff
	}
	existMsg := &discordgo.Message{
		ID:        "m-exist-1",
		ChannelID: "th-sweep-1",
		Author:    &discordgo.User{ID: "u-1", Username: "User"},
		Content:   "Existing message",
		Timestamp: now.Add(-10 * time.Minute),
	}

	msgsData, _ := json.Marshal([]*discordgo.Message{recentMsg, oldMsg, existMsg})

	s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/messages") {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(msgsData)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			Header:     make(http.Header),
		}, nil
	})

	RunStartupCatchUpSweep(context.Background(), database, pool, s)

	// Verify that missed message was recovered into DB
	recovered, err := db.GetMessage(database, "m-missed-1")
	if err != nil || recovered == nil {
		t.Fatalf("expected m-missed-1 to be recovered into DB: %v", err)
	}
	if recovered.AuthorName != "Alex" {
		t.Errorf("expected AuthorName Alex, got %s", recovered.AuthorName)
	}

	// Test duplicate sweep skip within 2 minutes
	RunStartupCatchUpSweep(context.Background(), database, pool, s)
}

func TestGetOrCreateThreadID_AllBranches(t *testing.T) {
	// 1. Nil message
	if id, isTh := getOrCreateThreadID(nil, nil); id != "" || isTh {
		t.Errorf("expected empty/false for nil message, got %s, %t", id, isTh)
	}

	// 2. Already in thread channel
	s := &discordgo.Session{
		State: discordgo.NewState(),
	}
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID: "g1",
		Threads: []*discordgo.Channel{
			{ID: "th-already-1", ParentID: "c1", GuildID: "g1", Type: discordgo.ChannelTypeGuildPublicThread},
		},
	})
	mTh := &discordgo.Message{
		ID:        "m-th-1",
		ChannelID: "th-already-1",
		GuildID:   "g1",
	}
	if id, isTh := getOrCreateThreadID(s, mTh); id != "th-already-1" || !isTh {
		t.Errorf("expected th-already-1 and true, got %s, %t", id, isTh)
	}

	// 3. Channel mode (policy.Mode == "channel")
	config.SetRuntimeConfigForTesting(config.Config{
		Channels: map[string]config.ChannelPolicy{
			"c-chan-mode": {Mode: "channel"},
		},
	})
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID: "g1",
		Channels: []*discordgo.Channel{
			{ID: "c-chan-mode", Name: "c-chan-mode", GuildID: "g1", Type: discordgo.ChannelTypeGuildText},
		},
	})
	mChan := &discordgo.Message{
		ID:        "m-c-1",
		ChannelID: "c-chan-mode",
		GuildID:   "g1",
	}
	if id, isTh := getOrCreateThreadID(s, mChan); id != "c-chan-mode" || isTh {
		t.Errorf("expected c-chan-mode and false, got %s, %t", id, isTh)
	}

	// 4. MessageThreadStart generic error
	sWithToken, _ := discordgo.New("Bot test-token")
	sWithToken.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("network error starting thread")
	})
	mErr := &discordgo.Message{
		ID:        "m-err-1",
		ChannelID: "c-generic",
		GuildID:   "g1",
		Content:   "Start thread",
	}
	if id, isTh := getOrCreateThreadID(sWithToken, mErr); id != "c-generic" || isTh {
		t.Errorf("expected fallback to channel on thread start error, got %s, %t", id, isTh)
	}
}

func TestRunStartupCatchUpSweep_EdgeCases(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	t.Run("UserGuildsFallbackAndChannels", func(t *testing.T) {
		sweepMu.Lock()
		lastSweepAt = time.Time{}
		isSweeping.Store(false)
		sweepMu.Unlock()

		s, _ := discordgo.New("Bot test-token")
		s.State = discordgo.NewState()
		s.State.User = &discordgo.User{ID: "bot-user-1"}

		s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			path := req.URL.Path
			var body string
			if strings.Contains(path, "/users/@me/guilds") {
				body = `[{"id": "g-fallback-1"}]`
			} else if strings.Contains(path, "/guilds/g-fallback-1/threads/active") {
				body = `{"threads": [{"id": "th-active-1", "type": 11, "parent_id": "c-parent-1", "guild_id": "g-fallback-1"}]}`
			} else if strings.Contains(path, "/guilds/g-fallback-1/channels") {
				body = `[{"id": "c-chan-1", "type": 0, "guild_id": "g-fallback-1"}]`
			} else if strings.Contains(path, "/channels/th-active-1/messages") {
				body = `[]`
			} else if strings.Contains(path, "/channels/c-chan-1/messages") {
				body = `[]`
			} else {
				return &http.Response{
					StatusCode: 404,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"message": "Not Found"}`)),
				}, nil
			}
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})

		RunStartupCatchUpSweep(context.Background(), database, pool, s)
	})

	t.Run("CircuitBreakerConsecutiveErrors", func(t *testing.T) {
		sweepMu.Lock()
		lastSweepAt = time.Time{}
		isSweeping.Store(false)
		sweepMu.Unlock()

		// Populate 6 thread IDs in DB
		for i := 1; i <= 6; i++ {
			_ = db.InsertMessage(database, db.Message{
				ID:        fmt.Sprintf("msg-breaker-%d", i),
				ThreadID:  fmt.Sprintf("th-breaker-%d", i),
				AuthorID:  "user-1",
				Content:   "test",
				CreatedAt: time.Now().UTC(),
			})
		}

		s, _ := discordgo.New("Bot test-token")
		s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("simulated network failure")
		})

		RunStartupCatchUpSweep(context.Background(), database, pool, s)
	})

	t.Run("CancelledContext", func(t *testing.T) {
		sweepMu.Lock()
		lastSweepAt = time.Time{}
		isSweeping.Store(false)
		sweepMu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		s, _ := discordgo.New("Bot test-token")
		RunStartupCatchUpSweep(ctx, database, pool, s)
	})
}

func TestHandleDiscordMessageCreate_NonTargeted(t *testing.T) {
	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	s, _ := discordgo.New("Bot test-token")
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-id-123"}

	config.SetRuntimeConfigForTesting(config.Config{
		Channels: map[string]config.ChannelPolicy{
			"c-mention-only": {Mode: "channel", WakeMode: "mention"},
		},
	})

	_ = s.State.GuildAdd(&discordgo.Guild{
		ID: "g1",
		Channels: []*discordgo.Channel{
			{ID: "c-mention-only", Name: "c-mention-only", GuildID: "g1", Type: discordgo.ChannelTypeGuildText},
		},
	})

	mNotTargeted := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-ignored-1",
			ChannelID: "c-mention-only",
			GuildID:   "g1",
			Content:   "Hello without mention",
			Author:    &discordgo.User{ID: "other-user", Bot: false},
		},
	}

	// Should not panic, should return without enqueuing
	handleDiscordMessageCreate(database, pool, s, mNotTargeted)
}

func TestFunnel_AdditionalHelperCoverage(t *testing.T) {
	// 1. deriveThreadTitle leading newline, empty, and mention-only
	if title := deriveThreadTitle("\nLeading newline text"); title != "Leading newline text" {
		t.Errorf("expected 'Leading newline text', got %q", title)
	}
	if title := deriveThreadTitle("<@123456789>"); title != "Aerial Discussion" {
		t.Errorf("expected 'Aerial Discussion', got %q", title)
	}
	if title := deriveThreadTitle(""); title != "Aerial Discussion" {
		t.Errorf("expected 'Aerial Discussion', got %q", title)
	}

	// 2. getDiscordChannel nil / empty

	if ch := getDiscordChannel(nil, ""); ch != nil {
		t.Errorf("expected nil for empty channel ID")
	}

	// 3. getDiscordChannel REST fetch & cache
	s, _ := discordgo.New("Bot test-token")
	s.State = discordgo.NewState()
	s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/channels/100200300400500600") {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"id":"100200300400500600","name":"test-rest-chan","type":0,"guild_id":"g1"}`)),
			}, nil
		}
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})

	ch := getDiscordChannel(s, "100200300400500600")
	if ch == nil || ch.Name != "test-rest-chan" {
		t.Fatalf("expected test-rest-chan, got %+v", ch)
	}

	// 4. getDiscordChannel cached thread
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:       "cached-thread-snap-1",
		Name:     "cached-thread",
		ParentID: "parent-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	chCached := getDiscordChannel(s, "cached-thread-snap-1")
	if chCached == nil || chCached.Type != discordgo.ChannelTypeGuildPublicThread {
		t.Errorf("expected thread channel type from cache")
	}

	// 5. resolveGuildID with nil / fallback from channel
	if gID := resolveGuildID(nil, nil); gID != "" {
		t.Errorf("expected empty string for nil message")
	}

	// 6. getOrCreateThreadID nil message
	if thID, isTh := getOrCreateThreadID(nil, nil); thID != "" || isTh {
		t.Errorf("expected empty and false for nil message")
	}

	// 7. buildDiscordPrompt with nil author, reply context, mentions and attachments
	mNilAuthor := &discordgo.Message{
		ID:        "m-nil-auth",
		ChannelID: "c1",
		GuildID:   "g1",
		Content:   "Hello from webhook",
		Timestamp: time.Now().UTC(),
		Mentions: []*discordgo.User{
			{Username: "mentionedUser"},
		},
		Attachments: []*discordgo.MessageAttachment{
			{URL: "https://cdn.discordapp.com/file.png"},
		},
		ReferencedMessage: &discordgo.Message{
			Author:  &discordgo.User{Username: "repliedUser"},
			Content: "Original question",
		},
	}
	prompt := buildDiscordPrompt(mNilAuthor, "th1", config.ChannelPolicy{Mode: "threads"})
	if !strings.Contains(prompt, "repliedUser") || !strings.Contains(prompt, "mentionedUser") || !strings.Contains(prompt, "file.png") {
		t.Errorf("prompt missing fields: %s", prompt)
	}
}

func TestGetOrCreateThreadID_AllRemainingBranches(t *testing.T) {
	// 1. Thread creation succeeds with empty ParentID and GuildID (fills from args)
	s, _ := discordgo.New("Bot test-token")
	s.State = discordgo.NewState()
	_ = s.State.GuildAdd(&discordgo.Guild{
		ID: "g-create-1",
		Channels: []*discordgo.Channel{
			{ID: "c-create-1", Name: "c-create-1", GuildID: "g-create-1", Type: discordgo.ChannelTypeGuildText},
		},
	})
	s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/threads") {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"id":"th-spawned-1","name":"Spawned Thread","type":11}`)),
			}, nil
		}
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})

	mSuccess := &discordgo.Message{
		ID:        "m-success-1",
		ChannelID: "c-create-1",
		GuildID:   "g-create-1",
		Content:   "Create a new thread please",
	}
	thID, isTh := getOrCreateThreadID(s, mSuccess)
	if thID != "th-spawned-1" || !isTh {
		t.Errorf("expected th-spawned-1 and true, got %s and %t", thID, isTh)
	}

	// 2. Thread cached in snapshot
	queue.CacheDiscordChannel(&discordgo.Channel{
		ID:       "m-cached-snap-thread",
		Name:     "cached-snap",
		ParentID: "c-create-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	mCachedSnap := &discordgo.Message{
		ID:        "m-cached-snap-thread",
		ChannelID: "c-create-1",
		GuildID:   "g-create-1",
	}
	if id, isTh := getOrCreateThreadID(s, mCachedSnap); id != "m-cached-snap-thread" || !isTh {
		t.Errorf("expected cached snapshot thread, got %s and %t", id, isTh)
	}

	// 3. Thread found in session state
	_ = s.State.ChannelAdd(&discordgo.Channel{
		ID:       "m-state-thread",
		Name:     "state-thread",
		ParentID: "c-create-1",
		GuildID:  "g-create-1",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	mStateTh := &discordgo.Message{
		ID:        "m-state-thread",
		ChannelID: "c-create-1",
		GuildID:   "g-create-1",
	}
	if id, isTh := getOrCreateThreadID(s, mStateTh); id != "m-state-thread" || !isTh {
		t.Errorf("expected state thread, got %s and %t", id, isTh)
	}

	// 4. Thread already exists (160004) with REST fetch returning channel
	sRest, _ := discordgo.New("Bot test-token")
	sRest.State = discordgo.NewState()
	_ = sRest.State.GuildAdd(&discordgo.Guild{
		ID: "g-exist-1",
		Channels: []*discordgo.Channel{
			{ID: "c-exist-1", Name: "c-exist-1", GuildID: "g-exist-1", Type: discordgo.ChannelTypeGuildText},
		},
	})
	sRest.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/threads") {
			return &http.Response{
				StatusCode: 400,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"code":160004,"message":"Thread already exists"}`)),
			}, nil
		}
		if strings.Contains(req.URL.Path, "/channels/100200300400500999") {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"id":"100200300400500999","name":"existing-th","type":11,"parent_id":"c-exist-1","guild_id":"g-exist-1"}`)),
			}, nil
		}
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})

	mExist := &discordgo.Message{
		ID:        "100200300400500999",
		ChannelID: "c-exist-1",
		GuildID:   "g-exist-1",
		Content:   "Existing thread message",
	}
	if id, isTh := getOrCreateThreadID(sRest, mExist); id != "100200300400500999" || !isTh {
		t.Errorf("expected 100200300400500999 and true, got %s and %t", id, isTh)
	}
}

func TestRunStartupCatchUpSweep_MessageSkippingAndRecoveryMatrix(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	// Insert pre-existing message
	_ = db.InsertMessage(database, db.Message{
		ID:        "msg-already-in-db",
		ThreadID:  "th-1",
		Content:   "already inserted",
		Status:    db.StatusCompleted,
		CreatedAt: time.Now().UTC(),
	})

	sweepMu.Lock()
	lastSweepAt = time.Time{}
	isSweeping.Store(false)
	sweepMu.Unlock()

	s, _ := discordgo.New("Bot test-token")
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-user-aerial"}

	_ = s.State.GuildAdd(&discordgo.Guild{
		ID: "g-sweep-matrix",
		Channels: []*discordgo.Channel{
			{ID: "c-sweep-matrix", Name: "c-sweep-matrix", GuildID: "g-sweep-matrix", Type: discordgo.ChannelTypeGuildText},
		},
	})

	now := time.Now().UTC()
	oldTime := now.Add(-5 * time.Hour).Format(time.RFC3339)
	recentTime := now.Add(-5 * time.Minute).Format(time.RFC3339)

	s.Client.Transport = mockCatchUpRoundTripper(func(req *http.Request) (*http.Response, error) {
		path := req.URL.Path
		if strings.Contains(path, "/threads/active") {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"threads":[]}`))}, nil
		}
		if strings.Contains(path, "/guilds/g-sweep-matrix/channels") {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[{"id":"c-sweep-matrix","type":0,"guild_id":"g-sweep-matrix"}]`))}, nil
		}
		if strings.Contains(path, "/channels/c-sweep-matrix/messages") {
			messagesJSON := fmt.Sprintf(`[
				{"id":"msg-bot-self","content":"<@bot-user-aerial> self","timestamp":"%s","author":{"id":"bot-user-aerial","bot":true}},
				{"id":"msg-too-old","content":"<@bot-user-aerial> old","timestamp":"%s","author":{"id":"user-1"}},
				{"id":"msg-not-targeted","content":"just chatting without mention","timestamp":"%s","author":{"id":"user-1"}},
				{"id":"msg-already-in-db","content":"<@bot-user-aerial> already there","timestamp":"%s","author":{"id":"user-1"}},
				{"id":"msg-to-recover","content":"<@bot-user-aerial> please help","timestamp":"%s","author":{"id":"user-2","username":"alex"}}
			]`, recentTime, oldTime, recentTime, recentTime, recentTime)

			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(messagesJSON)),
			}, nil
		}
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})

	config.SetRuntimeConfigForTesting(config.Config{
		Channels: map[string]config.ChannelPolicy{
			"c-sweep-matrix": {Mode: "channel", WakeMode: "mention"},
		},
	})

	RunStartupCatchUpSweep(context.Background(), database, pool, s)
}

func TestConnectDiscordFunnel_SessionAndCallbacks(t *testing.T) {
	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately so background retry and ticker exit immediately

	dg := connectDiscordFunnel(ctx, database, pool, "mock-test-bot-token")
	if dg == nil {
		t.Fatalf("expected non-nil discordgo session")
	}
	defer func() { _ = dg.Close() }()

	// Trigger ready event handler directly
	handleDiscordReady(ctx, database, pool, dg, &discordgo.Ready{
		User: &discordgo.User{ID: "bot-123", Username: "Aerial"},
		Guilds: []*discordgo.Guild{
			{ID: "g-ready-1"},
		},
	})

	// Trigger message create event handler directly
	handleDiscordMessageCreate(database, pool, dg, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-ready-1",
			ChannelID: "c-ready-1",
			Content:   "<@bot-123> hello",
			Author:    &discordgo.User{ID: "user-1", Username: "alex"},
			Timestamp: time.Now().UTC(),
		},
	})
}

func TestHandleDiscordMessageCreate_AllNilAndErrorBranches(t *testing.T) {
	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	s, _ := discordgo.New("Bot test-token")
	s.State = discordgo.NewState()
	s.State.User = &discordgo.User{ID: "bot-self-id"}

	// 1. Nil message
	handleDiscordMessageCreate(database, pool, s, nil)

	// 2. Nil author
	handleDiscordMessageCreate(database, pool, s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-nil-author",
			ChannelID: "c1",
		},
	})

	// 3. Own bot message (m.Author.ID == s.State.User.ID)
	handleDiscordMessageCreate(database, pool, s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-self-bot",
			ChannelID: "c1",
			Author:    &discordgo.User{ID: "bot-self-id"},
		},
	})

	// 4. Closed DB insert error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handleDiscordMessageCreate(closedDB, nil, s, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg-targeted-closed-db",
			ChannelID: "c1",
			Content:   "<@bot-self-id> hello",
			Author:    &discordgo.User{ID: "user-1", Username: "alex"},
			Timestamp: time.Now().UTC(),
		},
	})
}

func TestIsFunnelBotTargeted_ComprehensiveBranches(t *testing.T) {
	// 1. Nil checks
	if isFunnelBotTargeted(nil, nil) {
		t.Errorf("expected false for nil")
	}
	if isFunnelBotTargeted(nil, &discordgo.MessageCreate{}) {
		t.Errorf("expected false for empty MessageCreate")
	}
	if isFunnelBotTargeted(nil, &discordgo.MessageCreate{Message: &discordgo.Message{}}) {
		t.Errorf("expected false for nil Author")
	}

	// 2. Reply reference / MessageReference
	s, _ := discordgo.New("Bot test-token")
	mRef := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:               "m-ref-1",
			ChannelID:        "c-ref-1",
			GuildID:          "g-ref-1",
			Author:           &discordgo.User{ID: "u1"},
			Content:          "random reply message",
			MessageReference: &discordgo.MessageReference{MessageID: "orig-msg"},
		},
	}
	if !isFunnelBotTargeted(s, mRef) {
		t.Errorf("expected true for MessageReference")
	}

	// 3. Mention roles
	mRole := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:           "m-role-1",
			ChannelID:    "c-role-1",
			GuildID:      "g-role-1",
			Author:       &discordgo.User{ID: "u1"},
			Content:      "random message with role mention",
			MentionRoles: []string{"role-123"},
		},
	}
	if !isFunnelBotTargeted(s, mRole) {
		t.Errorf("expected true for MentionRoles")
	}
}















