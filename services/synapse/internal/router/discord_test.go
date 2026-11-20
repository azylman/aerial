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
			srv := newTestRouter(Config{}, nil, nil, disp)
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
			srv := newTestRouter(Config{}, nil, nil, disp)
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

func TestDiscordDelegations(t *testing.T) {
	ctx := context.Background()

	// 1. startDiscordGateway delegation with empty token (disabled)
	srv := newTestRouter(Config{}, nil, nil, nil)
	stopFn, err := startDiscordGateway(ctx, srv, "")
	if err != nil {
		t.Fatalf("startDiscordGateway(empty) err = %v", err)
	}
	stopFn()

	// 2. Server Config and PublishJob methods
	var nilSrv *RouterServer
	if cfg := nilSrv.Config(); cfg != nil {
		t.Errorf("nil RouterServer Config() = %v, want nil", cfg)
	}
	if cfg := srv.Config(); cfg == nil {
		t.Error("expected non-nil Config() from configured router")
	}

	bus := &mockEventBus{}
	srv.SetEventBus(bus)
	if err := srv.PublishJob(ctx, DiscordIngressArgs{MessageID: "m1", Message: &discordgo.Message{ID: "m1"}}); err != nil {
		t.Fatalf("PublishJob err = %v", err)
	}
	if len(bus.insertedDiscordJobs) != 1 {
		t.Errorf("insertedDiscordJobs = %d, want 1", len(bus.insertedDiscordJobs))
	}
}
