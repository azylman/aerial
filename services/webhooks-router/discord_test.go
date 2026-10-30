package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
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

func TestStartDiscordGateway(t *testing.T) {
	ctx := context.Background()

	origOpener := discordGatewayOpener
	t.Cleanup(func() {
		discordGatewayOpener = origOpener
	})

	// Exercise default opener code path hermetically
	sess, err := discordgo.New("Bot dummy")
	if err == nil {
		sess.Client = &http.Client{Transport: &zeroCallTransport{}}
		_ = origOpener(sess)
	}

	tests := []struct {
		name       string
		token      string
		openerErr  error
		wantErr    bool
		wantSubstr string
	}{
		{
			name:  "empty token returns no-op cleanup and no error",
			token: "",
		},
		{
			name:  "whitespace only token returns no-op cleanup",
			token: "   \t  ",
		},
		{
			name:       "opener failure returns error",
			token:      "my-test-token",
			openerErr:  errors.New("gateway connection refused"),
			wantErr:    true,
			wantSubstr: "open discord gateway",
		},
		{
			name:  "valid token with opener success configures session and cleanup is idempotent",
			token: "Bot secret-token-123",
		},
		{
			name:  "token without Bot prefix adds Bot prefix and succeeds",
			token: "secret-token-raw",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			discordGatewayOpener = func(dg *discordgo.Session) error {
				if tc.openerErr != nil {
					return tc.openerErr
				}
				expectedIntents := discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
				if dg.Identify.Intents != expectedIntents {
					t.Errorf("Intents = %v, want %v", dg.Identify.Intents, expectedIntents)
				}
				if dg.SyncEvents {
					t.Errorf("SyncEvents = true, want false")
				}
				return nil
			}

			srv := newTestRouter(Config{}, nil, nil, nil)
			stopFn, err := startDiscordGateway(ctx, srv, tc.token)
			if (err != nil) != tc.wantErr {
				t.Fatalf("startDiscordGateway() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if stopFn != nil {
				// verify idempotency
				stopFn()
				stopFn()
			}
		})
	}
}

func TestHandleGatewayMessageCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name            string
		nilServer       bool
		nilMsg          bool
		nilAuthor       bool
		botUserID       string
		authorID        string
		refGuildID      string
		msgGuildID      string
		hasRiver        bool
		riverErr        bool
		directErr       bool
		hasRolesInState bool
		mentionRoles    []string
		refRoles        []string
		wantEnqueued    int
		wantProcessed   int
	}{
		{
			name:      "nil server ignored",
			nilServer: true,
		},
		{
			name:   "nil message ignored",
			nilMsg: true,
		},
		{
			name:      "nil author ignored",
			nilAuthor: true,
		},
		{
			name:      "author is bot self ignored",
			botUserID: "bot-id-123",
			authorID:  "bot-id-123",
		},
		{
			name:          "direct dispatch fallback when river is nil",
			botUserID:     "bot-id-123",
			authorID:      "user-456",
			msgGuildID:    "guild-1",
			refGuildID:    "",
			mentionRoles:  []string{"role-a"},
			wantProcessed: 1,
		},
		{
			name:         "river enqueue when river is configured",
			botUserID:    "bot-id-123",
			authorID:     "user-456",
			hasRiver:     true,
			wantEnqueued: 1,
		},
		{
			name:         "river insert error handled cleanly",
			botUserID:    "bot-id-123",
			authorID:     "user-456",
			hasRiver:     true,
			riverErr:     true,
			wantEnqueued: 0,
		},
		{
			name:          "direct dispatch error handled cleanly",
			botUserID:     "bot-id-123",
			authorID:      "user-456",
			directErr:     true,
			wantProcessed: 1,
		},
		{
			name:            "mention roles and referenced roles resolved via session state",
			botUserID:       "bot-id-123",
			authorID:        "user-456",
			msgGuildID:      "guild-1",
			refGuildID:      "",
			mentionRoles:    []string{"role-a"},
			refRoles:        []string{"role-b"},
			hasRolesInState: true,
			wantProcessed:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			if tc.directErr {
				disp.discordMessageErr = errors.New("dispatch direct error")
			}
			river := &mockRiverInserter{}
			if tc.riverErr {
				river.err = errors.New("river insert error")
			}

			var srv *RouterServer
			if !tc.nilServer {
				srv = newTestRouter(Config{}, nil, nil, disp)
				if tc.hasRiver {
					srv.SetRiverClient(river)
				}
			}

			session, err := discordgo.New("Bot dummy-token")
			if err != nil {
				t.Fatalf("discordgo.New() err = %v", err)
			}
			if tc.botUserID != "" {
				session.State.User = &discordgo.User{ID: tc.botUserID}
			}
			if tc.hasRolesInState {
				_ = session.State.GuildAdd(&discordgo.Guild{
					ID: "guild-1",
					Roles: []*discordgo.Role{
						{ID: "role-a", Name: "AdminRole"},
						{ID: "role-b", Name: "ReviewerRole"},
					},
				})
			}

			var mc *discordgo.MessageCreate
			if !tc.nilMsg {
				mc = &discordgo.MessageCreate{
					Message: &discordgo.Message{
						ID:           "msg-101",
						ChannelID:    "chan-202",
						GuildID:      tc.msgGuildID,
						Content:      "hello world",
						MentionRoles: tc.mentionRoles,
					},
				}
				if !tc.nilAuthor {
					mc.Author = &discordgo.User{ID: tc.authorID}
				}
				if tc.refGuildID != "" || tc.msgGuildID != "" {
					mc.ReferencedMessage = &discordgo.Message{
						ID:           "ref-999",
						GuildID:      tc.refGuildID,
						MentionRoles: tc.refRoles,
					}
				}
			}

			handleGatewayMessageCreate(ctx, srv, session, mc)

			if tc.hasRiver {
				if len(river.insertedDiscordJobs) != tc.wantEnqueued {
					t.Errorf("insertedDiscordJobs = %d, want %d", len(river.insertedDiscordJobs), tc.wantEnqueued)
				}
			} else if !tc.nilServer {
				calls := disp.DiscordMessageCalls()
				if len(calls) != tc.wantProcessed {
					t.Errorf("DiscordMessageCalls = %d, want %d", len(calls), tc.wantProcessed)
				}
			}
		})
	}
}

func TestHandleGatewayInteractionCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name            string
		nilServer       bool
		nilInteraction  bool
		interactionType discordgo.InteractionType
		hasRiver        bool
		riverErr        bool
		directErr       bool
		wantEnqueued    int
		wantProcessed   int
	}{
		{
			name:      "nil server ignored",
			nilServer: true,
		},
		{
			name:           "nil interaction ignored",
			nilInteraction: true,
		},
		{
			name:            "non-command interaction ignored",
			interactionType: discordgo.InteractionMessageComponent,
		},
		{
			name:            "direct dispatch fallback when river is nil",
			interactionType: discordgo.InteractionApplicationCommand,
			wantProcessed:   1,
		},
		{
			name:            "river enqueue when river is configured",
			interactionType: discordgo.InteractionApplicationCommand,
			hasRiver:        true,
			wantEnqueued:    1,
		},
		{
			name:            "river insert error handled cleanly",
			interactionType: discordgo.InteractionApplicationCommand,
			hasRiver:        true,
			riverErr:        true,
			wantEnqueued:    0,
		},
		{
			name:            "direct dispatch error handled cleanly",
			interactionType: discordgo.InteractionApplicationCommand,
			directErr:       true,
			wantProcessed:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			if tc.directErr {
				disp.discordInteractionErr = errors.New("dispatch direct error")
			}
			river := &mockRiverInserter{}
			if tc.riverErr {
				river.err = errors.New("river insert error")
			}

			var srv *RouterServer
			if !tc.nilServer {
				srv = newTestRouter(Config{}, nil, nil, disp)
				if tc.hasRiver {
					srv.SetRiverClient(river)
				}
			}

			var ic *discordgo.InteractionCreate
			if !tc.nilInteraction {
				ic = &discordgo.InteractionCreate{
					Interaction: &discordgo.Interaction{
						ID:   "inter-101",
						Type: tc.interactionType,
					},
				}
			}

			handleGatewayInteractionCreate(ctx, srv, ic)

			if tc.hasRiver {
				if len(river.insertedInteractJobs) != tc.wantEnqueued {
					t.Errorf("insertedInteractJobs = %d, want %d", len(river.insertedInteractJobs), tc.wantEnqueued)
				}
			} else if !tc.nilServer {
				calls := disp.DiscordInteractionCalls()
				if len(calls) != tc.wantProcessed {
					t.Errorf("DiscordInteractionCalls = %d, want %d", len(calls), tc.wantProcessed)
				}
			}
		})
	}
}

func TestDiscordGatewayHandlerClosures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		isInteraction bool
	}{
		{
			name: "message create handler closure",
		},
		{
			name:          "interaction create handler closure",
			isInteraction: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			srv := newTestRouter(Config{}, nil, nil, disp)

			if tc.isInteraction {
				h := newDiscordGatewayInteractionCreateHandler(ctx, srv)
				h(nil, &discordgo.InteractionCreate{
					Interaction: &discordgo.Interaction{
						ID:   "inter-closure-1",
						Type: discordgo.InteractionApplicationCommand,
					},
				})
				if len(disp.DiscordInteractionCalls()) != 1 {
					t.Errorf("expected 1 interaction call, got %d", len(disp.DiscordInteractionCalls()))
				}
			} else {
				h := newDiscordGatewayMessageCreateHandler(ctx, srv)
				h(nil, &discordgo.MessageCreate{
					Message: &discordgo.Message{
						ID:      "msg-closure-1",
						Content: "closure test",
						Author:  &discordgo.User{ID: "author-closure-1"},
					},
				})
				if len(disp.DiscordMessageCalls()) != 1 {
					t.Errorf("expected 1 message call, got %d", len(disp.DiscordMessageCalls()))
				}
			}
		})
	}
}

func TestSessionBotUserID_And_RoleNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		session      *discordgo.Session
		guildID      string
		roles        [][]string
		wantBotID    string
		wantRoleLens int
	}{
		{
			name:      "nil session returns empty",
			session:   nil,
			wantBotID: "",
		},
		{
			name:      "session with nil state returns empty",
			session:   &discordgo.Session{},
			wantBotID: "",
		},
		{
			name:      "session with nil user returns empty",
			session:   &discordgo.Session{State: &discordgo.State{}},
			wantBotID: "",
		},
		{
			name: "session with user returns bot id",
			session: &discordgo.Session{
				State: &discordgo.State{
					User: &discordgo.User{ID: "bot-999"},
				},
			},
			wantBotID: "bot-999",
		},
		{
			name: "extractMentionRoleNames empty guildID returns nil",
			session: &discordgo.Session{
				State: &discordgo.State{},
			},
			guildID: "",
		},
		{
			name:    "extractMentionRoleNames nil state returns nil",
			session: &discordgo.Session{},
			guildID: "guild-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotID := sessionBotUserID(tc.session)
			if gotID != tc.wantBotID {
				t.Errorf("sessionBotUserID() = %q, want %q", gotID, tc.wantBotID)
			}
			gotRoles := extractMentionRoleNames(tc.session, tc.guildID, tc.roles...)
			if len(gotRoles) != tc.wantRoleLens {
				t.Errorf("extractMentionRoleNames() len = %d, want %d", len(gotRoles), tc.wantRoleLens)
			}
		})
	}
}
