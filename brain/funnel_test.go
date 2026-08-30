package main

import (
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
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

	title := deriveThreadTitle("<@1542035925603713086> Write a python script for Docker")
	if title != "Write a python script for Docker" {
		t.Errorf("Expected title 'Write a python script for Docker', got: %q", title)
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
