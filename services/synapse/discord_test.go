package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

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

type gatewayMessageCreateCase struct {
	name            string
	nilServer       bool
	nilMsg          bool
	nilAuthor       bool
	botUserID       string
	authorID        string
	refGuildID      string
	msgGuildID      string
	hasBus          bool
	busErr          bool
	directErr       bool
	hasRolesInState bool
	mentionRoles    []string
	refRoles        []string
	wantEnqueued    int
	wantProcessed   int
}

func buildGatewayMessage(tc gatewayMessageCreateCase) *discordgo.MessageCreate {
	if tc.nilMsg {
		return nil
	}
	mc := &discordgo.MessageCreate{
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
	return mc
}

func runGatewayMessageCreateCase(ctx context.Context, t *testing.T, tc gatewayMessageCreateCase) {
	t.Helper()
	disp := &mockOutboundDispatcher{}
	if tc.directErr {
		disp.discordMessageErr = errors.New("dispatch direct error")
	}
	bus := &mockEventBus{}
	if tc.busErr {
		bus.err = errors.New("bus insert error")
	}

	var srv *RouterServer
	if !tc.nilServer {
		srv = newTestRouter(Config{}, nil, nil, disp)
		if tc.hasBus {
			srv.SetEventBus(bus)
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

	mc := buildGatewayMessage(tc)
	handleGatewayMessageCreate(ctx, srv, session, mc)

	if tc.hasBus {
		if len(bus.insertedDiscordJobs) != tc.wantEnqueued {
			t.Errorf("insertedDiscordJobs = %d, want %d", len(bus.insertedDiscordJobs), tc.wantEnqueued)
		}
		for _, tp := range bus.publishedTraceparents {
			assertRootTraceparent(ctx, t, tp)
		}
	} else if !tc.nilServer {
		calls := disp.DiscordMessageCalls()
		if len(calls) != tc.wantProcessed {
			t.Errorf("DiscordMessageCalls = %d, want %d", len(calls), tc.wantProcessed)
		}
	}
}

func TestHandleGatewayMessageCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []gatewayMessageCreateCase{
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
			name:          "direct dispatch fallback when bus is nil",
			botUserID:     "bot-id-123",
			authorID:      "user-456",
			msgGuildID:    "guild-1",
			refGuildID:    "",
			mentionRoles:  []string{"role-a"},
			wantProcessed: 1,
		},
		{
			name:         "bus enqueue when bus is configured",
			botUserID:    "bot-id-123",
			authorID:     "user-456",
			hasBus:       true,
			wantEnqueued: 1,
		},
		{
			name:         "bus insert error handled cleanly",
			botUserID:    "bot-id-123",
			authorID:     "user-456",
			hasBus:       true,
			busErr:       true,
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
			runGatewayMessageCreateCase(ctx, t, tc)
		})
	}
}

type gatewayInteractionCreateCase struct {
	name            string
	nilServer       bool
	nilInteraction  bool
	interactionType discordgo.InteractionType
	hasBus          bool
	busErr          bool
	directErr       bool
	wantEnqueued    int
	wantProcessed   int
}

func runGatewayInteractionCreateCase(ctx context.Context, t *testing.T, tc gatewayInteractionCreateCase) {
	t.Helper()
	disp := &mockOutboundDispatcher{}
	if tc.directErr {
		disp.discordInteractionErr = errors.New("dispatch direct error")
	}
	bus := &mockEventBus{}
	if tc.busErr {
		bus.err = errors.New("bus insert error")
	}

	var srv *RouterServer
	if !tc.nilServer {
		srv = newTestRouter(Config{}, nil, nil, disp)
		if tc.hasBus {
			srv.SetEventBus(bus)
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

	if tc.hasBus {
		if len(bus.insertedInteractJobs) != tc.wantEnqueued {
			t.Errorf("insertedInteractJobs = %d, want %d", len(bus.insertedInteractJobs), tc.wantEnqueued)
		}
		for _, tp := range bus.publishedTraceparents {
			assertRootTraceparent(ctx, t, tp)
		}
	} else if !tc.nilServer {
		calls := disp.DiscordInteractionCalls()
		if len(calls) != tc.wantProcessed {
			t.Errorf("DiscordInteractionCalls = %d, want %d", len(calls), tc.wantProcessed)
		}
	}
}

func TestHandleGatewayInteractionCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []gatewayInteractionCreateCase{
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
			name:            "direct dispatch fallback when bus is nil",
			interactionType: discordgo.InteractionApplicationCommand,
			wantProcessed:   1,
		},
		{
			name:            "bus enqueue when bus is configured",
			interactionType: discordgo.InteractionApplicationCommand,
			hasBus:          true,
			wantEnqueued:    1,
		},
		{
			name:            "bus insert error handled cleanly",
			interactionType: discordgo.InteractionApplicationCommand,
			hasBus:          true,
			busErr:          true,
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
			runGatewayInteractionCreateCase(ctx, t, tc)
		})
	}
}

func TestDiscordGatewayHandlerClosures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		isInteraction bool
		isReady       bool
	}{
		{
			name: "message create handler closure",
		},
		{
			name:          "interaction create handler closure",
			isInteraction: true,
		},
		{
			name:    "ready handler closure",
			isReady: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			srv := newTestRouter(Config{}, nil, nil, disp)

			switch {
			case tc.isReady:
				canceledCtx, cancel := context.WithCancel(ctx)
				cancel()
				session, err := discordgo.New("Bot dummy-ready")
				if err != nil {
					t.Fatalf("discordgo.New() err = %v", err)
				}
				session.Client = &http.Client{Transport: &mockDiscordTransport{}}
				h := newDiscordGatewayReadyHandler(canceledCtx, srv)
				h(session, &discordgo.Ready{
					User: &discordgo.User{ID: "bot-ready-1"},
				})
				if session.State.User == nil || session.State.User.ID != "bot-ready-1" {
					t.Errorf("User = %v, want bot-ready-1", session.State.User)
				}
			case tc.isInteraction:
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
			default:
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

func TestDiscordSnowflakeFromTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    time.Time
		wantSnow string
	}{
		{
			name:     "zero time returns 0",
			input:    time.Time{},
			wantSnow: "0",
		},
		{
			name:     "before discord epoch returns 0",
			input:    time.Date(2014, 12, 31, 23, 59, 59, 0, time.UTC),
			wantSnow: "0",
		},
		{
			name:     "exact discord epoch returns 0",
			input:    time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			wantSnow: "0",
		},
		{
			name:     "1000ms after epoch computes shifted snowflake",
			input:    time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC),
			wantSnow: "4194304000",
		},
		{
			name:     "known 2026 timestamp",
			input:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			wantSnow: "1456074443980800000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := discordSnowflakeFromTime(tc.input)
			if got != tc.wantSnow {
				t.Errorf("discordSnowflakeFromTime(%v) = %q, want %q", tc.input, got, tc.wantSnow)
			}
		})
	}
}

func TestCompareDiscordSnowflakes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    string
		b    string
		want int
	}{
		{name: "both empty", a: "", b: "", want: 0},
		{name: "a empty b non-empty", a: "", b: "100", want: -1},
		{name: "a non-empty b empty", a: "100", b: "", want: 1},
		{name: "both zero string", a: "0", b: "0", want: 0},
		{name: "invalid non-numeric treated as zero", a: "invalid", b: "", want: 0},
		{name: "a smaller than b", a: "100", b: "200", want: -1},
		{name: "a larger than b", a: "500", b: "200", want: 1},
		{name: "equal large snowflakes", a: "1456074443980800000", b: "1456074443980800000", want: 0},
		{name: "whitespace trimmed", a: " 100 ", b: "100", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := compareDiscordSnowflakes(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("compareDiscordSnowflakes(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestMaxDiscordSnowflake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    string
		b    string
		want string
	}{
		{name: "a larger", a: "200", b: "100", want: "200"},
		{name: "b larger", a: "100", b: "200", want: "200"},
		{name: "both equal", a: "100", b: "100", want: "100"},
		{name: "a empty b valid", a: "", b: "300", want: "300"},
		{name: "a valid b empty", a: "300", b: "", want: "300"},
		{name: "both empty", a: "", b: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := maxDiscordSnowflake(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("maxDiscordSnowflake(%q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
