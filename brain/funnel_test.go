package main

import (
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/bwmarrin/discordgo"
)

func TestFunnelHelpers(t *testing.T) {
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

	title := deriveThreadTitle("<@1542035925603713086> Write a python script for Home Assistant")
	if title != "Write a python script for Home Assistant" {
		t.Errorf("Expected title 'Write a python script for Home Assistant', got: %q", title)
	}

	targetThreadID, isThread := getOrCreateThreadID(s, dmMsg)
	if targetThreadID != "chan-dm" || isThread {
		t.Errorf("Expected DM targetThreadID 'chan-dm' and false, got: %s, %t", targetThreadID, isThread)
	}

	prompt := buildDiscordPrompt(dmMsg, "thread-12345")
	if prompt == "" {
		t.Errorf("Expected non-empty prompt for message")
	}

	mCreate := &discordgo.MessageCreate{Message: dmMsg}
	if !isFunnelBotTargeted(s, mCreate) {
		t.Errorf("Expected DM message to be bot targeted")
	}
}

func TestRecoverStartupInterruptedTurns(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Case 1: Clean DB (0 interrupted turns)
	recoverStartupInterruptedTurns(database, "agy", "key", "model", "prompt", 15, nil)

	// Case 2: Interrupted turn present (without prompt payload, unlocks)
	_ = db.SaveConversationMapping(database, "thread-interrupted", "conv-int")
	_ = db.SetTurnProcessing(database, "thread-interrupted", true, "msg-100")

	recoverStartupInterruptedTurns(database, "agy", "key", "model", "prompt", 15, nil)

	state, err := db.GetTurnState(database, "thread-interrupted")
	if err != nil || state == nil {
		t.Fatalf("Failed to retrieve turn state: %v", err)
	}
	if state.IsProcessing {
		t.Errorf("Expected IsProcessing to be false after startup recovery without prompt, got true")
	}
}
