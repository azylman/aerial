package queue

import (
	"errors"
	"testing"

	"github.com/azylman/aerial/brain/pkg/discord"
	"github.com/bwmarrin/discordgo"
)

func TestWorkerPool_DeliverDirect(t *testing.T) {
	dummySession := &discordgo.Session{}
	customErr := errors.New("custom delivery error")

	tests := []struct {
		name              string
		nilPool           bool
		nilSession        bool
		useDeliveryFunc   bool
		funcErr           error
		channelID         string
		text              string
		expectErrContains string
		expectErr         error
	}{
		{
			name:              "nil pool returns error",
			nilPool:           true,
			channelID:         "123456789012345678",
			text:              "hello",
			expectErrContains: "worker pool is nil",
		},
		{
			name:              "nil session with nil DeliveryFunc returns discord session is not connected",
			nilSession:        true,
			channelID:         "123456789012345678",
			text:              "hello",
			expectErrContains: "discord session is not connected",
		},
		{
			name:            "injected DeliveryFunc is invoked with channelID and text",
			useDeliveryFunc: true,
			channelID:       "123456789012345678",
			text:            "test message",
		},
		{
			name:            "session error propagation",
			useDeliveryFunc: true,
			funcErr:         customErr,
			channelID:       "123456789012345678",
			text:            "test message",
			expectErr:       customErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilPool {
				var p *WorkerPool
				err := p.DeliverDirect(tc.channelID, tc.text)
				if err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}
			if tc.nilSession {
				p := &WorkerPool{}
				err := p.DeliverDirect(tc.channelID, tc.text)
				if err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}

			var capturedChannelID, capturedText string
			var capturedSession *discordgo.Session
			invoked := false

			p := &WorkerPool{
				cfg: WorkerPoolConfig{
					DiscordSession: dummySession,
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						invoked = true
						capturedSession = s
						capturedChannelID = channelID
						capturedText = text
						return tc.funcErr
					},
				},
			}
			err := p.DeliverDirect(tc.channelID, tc.text)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !invoked || capturedSession != dummySession || capturedChannelID != tc.channelID || capturedText != tc.text {
				t.Fatalf("unexpected invocation state: invoked=%v chan=%q text=%q", invoked, capturedChannelID, capturedText)
			}
		})
	}
}

func TestWorkerPool_DeliverStatusWidget(t *testing.T) {
	tests := []struct {
		name               string
		nilPool            bool
		nilSession         bool
		useDeliveryFunc    bool
		widgetMsgID        string
		req                discord.StatusWidgetRequest
		expectErrContains  string
		expectMsgID        string
	}{
		{
			name:              "nil pool returns error",
			nilPool:           true,
			req:               discord.StatusWidgetRequest{},
			expectErrContains: "worker pool is nil",
		},
		{
			name:              "nil session with nil WidgetDeliveryFunc returns discord session is not connected",
			nilSession:        true,
			req:               discord.StatusWidgetRequest{ChannelID: "chan-1"},
			expectErrContains: "discord session is not connected",
		},
		{
			name:            "injected WidgetDeliveryFunc is invoked",
			useDeliveryFunc: true,
			widgetMsgID:     "widget-msg-123",
			req:             discord.StatusWidgetRequest{ChannelID: "chan-1"},
			expectMsgID:     "widget-msg-123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilPool {
				var p *WorkerPool
				_, err := p.DeliverStatusWidget(tc.req)
				if err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}
			if tc.nilSession {
				p := &WorkerPool{}
				_, err := p.DeliverStatusWidget(tc.req)
				if err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}

			p := &WorkerPool{
				cfg: WorkerPoolConfig{
					WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
						return tc.widgetMsgID, nil
					},
				},
			}
			msgID, err := p.DeliverStatusWidget(tc.req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if msgID != tc.expectMsgID {
				t.Fatalf("expected msgID %q, got %s", tc.expectMsgID, msgID)
			}
		})
	}
}
