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

	convID, isThread := getFunnelConversationID(s, dmMsg)
	if convID != "chan-dm" || isThread {
		t.Errorf("Expected DM conversation ID 'chan-dm' and false, got: %s, %t", convID, isThread)
	}

	prompt := buildDiscordPrompt(dmMsg, false)
	if prompt == "" {
		t.Errorf("Expected non-empty prompt for DM message")
	}

	promptThread := buildDiscordPrompt(dmMsg, true)
	if promptThread == "" {
		t.Errorf("Expected non-empty prompt for thread message")
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
	defer database.Close()

	// Case 1: Clean DB (0 interrupted turns)
	recoverStartupInterruptedTurns(database)

	// Case 2: Interrupted turn present
	db.SaveConversationMapping(database, "thread-interrupted", "conv-int")
	db.SetTurnProcessing(database, "thread-interrupted", true, "msg-100")

	recoverStartupInterruptedTurns(database)

	state, err := db.GetTurnState(database, "thread-interrupted")
	if err != nil || state == nil {
		t.Fatalf("Failed to retrieve turn state: %v", err)
	}
	if state.IsProcessing {
		t.Errorf("Expected IsProcessing to be false after startup recovery, got true")
	}
}
