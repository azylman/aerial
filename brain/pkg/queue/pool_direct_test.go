package queue

import (
	"errors"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestWorkerPool_DeliverDirect(t *testing.T) {
	t.Run("nil pool returns error", func(t *testing.T) {
		var p *WorkerPool
		err := p.DeliverDirect("123456789012345678", "hello")
		if err == nil {
			t.Fatal("expected error for nil pool, got nil")
		}
		if err.Error() != "worker pool is nil" {
			t.Fatalf("expected 'worker pool is nil', got %q", err.Error())
		}
	})

	t.Run("nil session with nil DeliveryFunc returns discord session is not connected", func(t *testing.T) {
		p := &WorkerPool{}
		err := p.DeliverDirect("123456789012345678", "hello")
		if err == nil {
			t.Fatal("expected error for nil session and nil DeliveryFunc, got nil")
		}
		if err.Error() != "discord session is not connected" {
			t.Fatalf("expected 'discord session is not connected', got %q", err.Error())
		}
	})

	t.Run("injected DeliveryFunc is invoked with channelID and text", func(t *testing.T) {
		var capturedChannelID, capturedText string
		var capturedSession *discordgo.Session
		invoked := false

		dummySession := &discordgo.Session{}
		p := &WorkerPool{
			cfg: WorkerPoolConfig{
				DiscordSession: dummySession,
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					invoked = true
					capturedSession = s
					capturedChannelID = channelID
					capturedText = text
					return nil
				},
			},
		}

		err := p.DeliverDirect("123456789012345678", "test message")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !invoked {
			t.Fatal("expected DeliveryFunc to be invoked")
		}
		if capturedSession != dummySession {
			t.Fatalf("expected session %v, got %v", dummySession, capturedSession)
		}
		if capturedChannelID != "123456789012345678" {
			t.Fatalf("expected channelID '123456789012345678', got %q", capturedChannelID)
		}
		if capturedText != "test message" {
			t.Fatalf("expected text 'test message', got %q", capturedText)
		}
	})

	t.Run("session error propagation", func(t *testing.T) {
		expectedErr := errors.New("custom delivery error")
		p := &WorkerPool{
			cfg: WorkerPoolConfig{
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					return expectedErr
				},
			},
		}

		err := p.DeliverDirect("123456789012345678", "test message")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
	})
}
