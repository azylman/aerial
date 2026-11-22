package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestProcessDiscordIngress(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		nilDispatcher bool
		dispatchErr   bool
		agent         string
		wantErr       bool
		wantSubstr    string
	}{
		{
			name: "ProcessDiscordIngress success",
		},
		{
			name:  "ProcessDiscordIngress with agent success",
			agent: "agent-alpha",
		},
		{
			name:          "ProcessDiscordIngress nil dispatcher",
			nilDispatcher: true,
			wantErr:       true,
			wantSubstr:    "dispatcher not configured",
		},
		{
			name:        "ProcessDiscordIngress dispatch error",
			dispatchErr: true,
			wantErr:     true,
			wantSubstr:  "downstream error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			if tc.dispatchErr {
				disp.discordMessageErr = errors.New("downstream error")
			}
			srv := newTestRouter(Config{}, nil, disp)
			if tc.nilDispatcher {
				srv.SetDispatcher(nil)
			}

			err := srv.ProcessDiscordIngress(ctx, DiscordIngressArgs{
				MessageID: "msg-1",
				BotUserID: "bot-1",
				Message:   &discordgo.Message{ID: "msg-1", Content: "hello"},
				Agent:     tc.agent,
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if tc.wantErr {
				return
			}
			msgCalls := disp.DiscordMessageCalls()
			if len(msgCalls) != 1 {
				t.Fatalf("DiscordMessageCalls() len = %d, want 1", len(msgCalls))
			}
			if msgCalls[0].Agent != tc.agent {
				t.Errorf("DiscordMessageCalls()[0].Agent = %q, want %q", msgCalls[0].Agent, tc.agent)
			}
		})
	}
}

func TestProcessDiscordInteraction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		nilDispatcher bool
		dispatchErr   bool
		agent         string
		wantErr       bool
		wantSubstr    string
	}{
		{
			name: "ProcessDiscordInteraction success",
		},
		{
			name:  "ProcessDiscordInteraction with agent success",
			agent: "agent-alpha",
		},
		{
			name:          "ProcessDiscordInteraction nil dispatcher",
			nilDispatcher: true,
			wantErr:       true,
			wantSubstr:    "dispatcher not configured",
		},
		{
			name:        "ProcessDiscordInteraction dispatch error",
			dispatchErr: true,
			wantErr:     true,
			wantSubstr:  "downstream error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			if tc.dispatchErr {
				disp.discordInteractionErr = errors.New("downstream error")
			}
			srv := newTestRouter(Config{}, nil, disp)
			if tc.nilDispatcher {
				srv.SetDispatcher(nil)
			}

			err := srv.ProcessDiscordInteraction(ctx, DiscordInteractionArgs{
				InteractionID: "inter-1",
				Interaction:   &discordgo.Interaction{ID: "inter-1", Type: discordgo.InteractionApplicationCommand},
				Agent:         tc.agent,
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if tc.wantErr {
				return
			}
			if len(disp.DiscordInteractionCalls()) != 1 {
				t.Errorf("DiscordInteractionCalls() len = %d, want 1", len(disp.DiscordInteractionCalls()))
			}
			agentCalls := disp.DiscordInteractionAgentCalls()
			if tc.agent != "" {
				if len(agentCalls) != 1 || agentCalls[0] != tc.agent {
					t.Errorf("DiscordInteractionAgentCalls() = %v, want [%q]", agentCalls, tc.agent)
				}
			} else if len(agentCalls) != 0 {
				t.Errorf("DiscordInteractionAgentCalls() = %v, want empty", agentCalls)
			}
		})
	}
}

func TestStartDiscordGatewayWithoutTokenIsNoop(t *testing.T) {
	t.Parallel()
	stop, err := startDiscordGateway(context.Background(), newTestRouter(Config{}, nil, nil), "")
	if err != nil {
		t.Fatalf("startDiscordGateway() err = %v", err)
	}
	stop()
}

func TestRouterServerConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		srv     *RouterServer
		wantNil bool
	}{
		{"nil router", nil, true},
		{"nil config source", &RouterServer{}, true},
		{"configured router", newTestRouter(Config{SystemChannelID: "1"}, nil, nil), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.srv.Config(); (got == nil) != tc.wantNil {
				t.Errorf("Config() = %v, want nil %v", got, tc.wantNil)
			}
		})
	}
}

func TestPublishJob(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		bus      *mockEventBus
		wantErr  bool
		wantJobs int
	}{
		{name: "publishes the job on the bus", bus: &mockEventBus{}, wantJobs: 1},
		{name: "bus publish error is returned", bus: &mockEventBus{err: errors.New("nats down")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTestRouter(Config{}, nil, nil, tc.bus)
			err := srv.PublishJob(context.Background(), DiscordIngressArgs{MessageID: "m1", Message: &discordgo.Message{ID: "m1"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("PublishJob() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := len(tc.bus.insertedDiscordJobs); got != tc.wantJobs {
				t.Errorf("insertedDiscordJobs = %d, want %d", got, tc.wantJobs)
			}
		})
	}
}
