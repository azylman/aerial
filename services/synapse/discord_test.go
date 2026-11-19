package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/synapse/internal/discord"
)

func TestProcessDiscordIngress_And_Interaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		isInteraction bool
		nilDispatcher bool
		dispatchErr   bool
		wantErr       bool
		wantSubstr    string
	}{
		{
			name: "ProcessDiscordIngress success",
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
		{
			name:          "ProcessDiscordInteraction success",
			isInteraction: true,
		},
		{
			name:          "ProcessDiscordInteraction nil dispatcher",
			isInteraction: true,
			nilDispatcher: true,
			wantErr:       true,
			wantSubstr:    "dispatcher not configured",
		},
		{
			name:          "ProcessDiscordInteraction dispatch error",
			isInteraction: true,
			dispatchErr:   true,
			wantErr:       true,
			wantSubstr:    "downstream error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			if tc.dispatchErr {
				disp.discordMessageErr = errors.New("downstream error")
				disp.discordInteractionErr = errors.New("downstream error")
			}
			srv := newTestRouter(Config{}, nil, nil, disp)
			if tc.nilDispatcher {
				srv.SetDispatcher(nil)
			}

			var err error
			if tc.isInteraction {
				err = srv.ProcessDiscordInteraction(ctx, DiscordInteractionArgs{
					InteractionID: "inter-1",
					Interaction:   &discordgo.Interaction{ID: "inter-1", Type: discordgo.InteractionApplicationCommand},
				})
			} else {
				err = srv.ProcessDiscordIngress(ctx, DiscordIngressArgs{
					MessageID: "msg-1",
					BotUserID: "bot-1",
					Message:   &discordgo.Message{ID: "msg-1", Content: "hello"},
				})
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
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

	// 2. runDiscordCatchUpSweep delegation
	runDiscordCatchUpSweep(ctx, nil, srv, time.Now().UTC())

	// 3. Watermark and snowflake delegations
	discord.ResetWatermarksForTest()
	defer discord.ResetWatermarksForTest()

	if wm := getChannelWatermark("c1"); wm != "" {
		t.Errorf("getChannelWatermark = %q, want empty", wm)
	}
	advanceChannelWatermark("c1", "100")
	if wm := getChannelWatermark("c1"); wm != "100" {
		t.Errorf("getChannelWatermark = %q, want 100", wm)
	}

	if v := parseSnowflakeUint64("123"); v != 123 {
		t.Errorf("parseSnowflakeUint64 = %d, want 123", v)
	}
	if s := discordSnowflakeFromTime(time.Unix(0, 0)); s != "0" {
		t.Errorf("discordSnowflakeFromTime = %q, want 0", s)
	}
	if cmp := compareDiscordSnowflakes("200", "100"); cmp != 1 {
		t.Errorf("compareDiscordSnowflakes = %d, want 1", cmp)
	}
	if gotMax := maxDiscordSnowflake("100", "200"); gotMax != "200" {
		t.Errorf("maxDiscordSnowflake = %q, want 200", gotMax)
	}

	// 4. Server Config and PublishJob methods
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
