package main

import (
	"context"
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

