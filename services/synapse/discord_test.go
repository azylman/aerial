package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
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

			if tc.isReady {
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
			} else if tc.isInteraction {
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

func resetChannelWatermarks() {
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	channelWatermarks = make(map[string]string)
}

func TestChannelWatermarks(t *testing.T) {
	resetChannelWatermarks()
	t.Cleanup(resetChannelWatermarks)

	tests := []struct {
		name       string
		channelID  string
		advanceID  string
		wantBefore string
		wantAfter  string
	}{
		{
			name:       "empty channel returns empty",
			channelID:  "chan-new",
			advanceID:  "",
			wantBefore: "",
			wantAfter:  "",
		},
		{
			name:       "first advance sets watermark",
			channelID:  "chan-new",
			advanceID:  "100",
			wantBefore: "",
			wantAfter:  "100",
		},
		{
			name:       "smaller advance does not regress watermark",
			channelID:  "chan-new",
			advanceID:  "50",
			wantBefore: "100",
			wantAfter:  "100",
		},
		{
			name:       "larger advance advances watermark",
			channelID:  "chan-new",
			advanceID:  "200",
			wantBefore: "100",
			wantAfter:  "200",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := getChannelWatermark(tc.channelID)
			if before != tc.wantBefore {
				t.Errorf("before = %q, want %q", before, tc.wantBefore)
			}
			advanceChannelWatermark(tc.channelID, tc.advanceID)
			after := getChannelWatermark(tc.channelID)
			if after != tc.wantAfter {
				t.Errorf("after = %q, want %q", after, tc.wantAfter)
			}
		})
	}
}

type sweepTestCase struct {
	name                string
	nilSession          bool
	nilServer           bool
	sweepAlreadyRunning bool
	canceledCtx         bool
	now                 time.Time
	botUserID           string
	initialWatermark    string
	channelID           string
	lastMessageID       string
	channelGuildID      string
	guildID             string
	nilGuildInState     bool
	nilChannelInGuild   bool
	threadsInState      []*discordgo.Channel
	restChannels        []*discordgo.Channel
	restThreads         []*discordgo.Channel
	restChannelsErr     bool
	restThreadsErr      bool
	userGuildsResp      []*discordgo.UserGuild
	userGuildsErr       bool
	messagesResp        []*discordgo.Message
	messagesErrCode     int
	hasRiver            bool
	runSecondSweep      bool

	wantFetchCalls  int
	wantAfterQuery  string
	wantEnqueuedIDs []string
	wantWatermark   string
}

func TestRunDiscordCatchUpSweep(t *testing.T) {
	ctx := context.Background()

	baseNow := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	minSnow := discordSnowflakeFromTime(baseNow.Add(-30 * time.Minute))
	staleSnow := discordSnowflakeFromTime(baseNow.Add(-45 * time.Minute))
	freshSnow1 := discordSnowflakeFromTime(baseNow.Add(-15 * time.Minute))
	freshSnow2 := discordSnowflakeFromTime(baseNow.Add(-5 * time.Minute))
	botSnow := discordSnowflakeFromTime(baseNow.Add(-10 * time.Minute))

	freshMsg1 := &discordgo.Message{
		ID:        freshSnow1,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "user-alpha"},
		Content:   "older fresh message",
		Timestamp: baseNow.Add(-15 * time.Minute),
	}
	freshMsg2 := &discordgo.Message{
		ID:        freshSnow2,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "user-beta"},
		Content:   "newer fresh message",
		Timestamp: baseNow.Add(-5 * time.Minute),
		ReferencedMessage: &discordgo.Message{
			ID: "ref-parent",
		},
	}
	botMsg := &discordgo.Message{
		ID:        botSnow,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "bot-123"},
		Content:   "bot response",
		Timestamp: baseNow.Add(-10 * time.Minute),
	}
	staleMsg := &discordgo.Message{
		ID:        staleSnow,
		ChannelID: "chan-sweep-5",
		Author:    &discordgo.User{ID: "user-gamma"},
		Content:   "stale message older than 30m",
		Timestamp: baseNow.Add(-45 * time.Minute),
	}

	tests := []sweepTestCase{
		{
			name:       "nil session returns immediately",
			nilSession: true,
		},
		{
			name:      "nil server returns immediately",
			nilServer: true,
		},
		{
			name:                "concurrent sweep already running returns immediately",
			sweepAlreadyRunning: true,
		},
		{
			name:        "canceled context halts sweep early",
			now:         baseNow,
			canceledCtx: true,
			channelID:   "chan-canceled",
		},
		{
			name:          "zero now defaults to UTC now",
			now:           time.Time{},
			channelID:     "chan-zero-now",
			lastMessageID: "",
		},
		{
			name:          "fast-path skip on empty LastMessageID",
			now:           baseNow,
			channelID:     "chan-sweep-1",
			lastMessageID: "",
		},
		{
			name:          "fast-path skip when LastMessageID <= minSnowflake",
			now:           baseNow,
			channelID:     "chan-sweep-2",
			lastMessageID: staleSnow,
		},
		{
			name:             "fast-path skip when LastMessageID <= watermark",
			now:              baseNow,
			channelID:        "chan-sweep-3",
			lastMessageID:    freshSnow1,
			initialWatermark: freshSnow1,
		},
		{
			name:            "ChannelMessages fetch error logged and channel skipped",
			now:             baseNow,
			channelID:       "chan-sweep-4",
			lastMessageID:   freshSnow2,
			messagesErrCode: http.StatusInternalServerError,
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
		},
		{
			name:          "chronological enqueue skipping stale and bot messages, advancing watermark",
			now:           baseNow,
			botUserID:     "bot-123",
			channelID:     "chan-sweep-5",
			guildID:       "guild-123",
			lastMessageID: freshSnow2,
			// Discord REST returns descending (newest first)
			messagesResp:    []*discordgo.Message{freshMsg2, freshMsg1, botMsg, staleMsg},
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow1, freshSnow2},
			wantWatermark:   freshSnow2,
		},
		{
			name:            "subsequent sweep makes zero calls once watermark advanced",
			now:             baseNow,
			botUserID:       "bot-123",
			channelID:       "chan-sweep-6",
			guildID:         "guild-123",
			lastMessageID:   freshSnow2,
			messagesResp:    []*discordgo.Message{freshMsg2, freshMsg1},
			runSecondSweep:  true,
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow1, freshSnow2},
			wantWatermark:   freshSnow2,
		},
		{
			name:            "river client enqueue in chronological order",
			now:             baseNow,
			botUserID:       "bot-123",
			channelID:       "chan-sweep-7",
			guildID:         "guild-123",
			lastMessageID:   freshSnow2,
			hasRiver:        true,
			messagesResp:    []*discordgo.Message{freshMsg2, freshMsg1},
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow1, freshSnow2},
			wantWatermark:   freshSnow2,
		},
		{
			name:           "empty state guilds fetches user guilds from REST",
			now:            baseNow,
			userGuildsResp: []*discordgo.UserGuild{{ID: "user-guild-1"}},
			restChannels:   []*discordgo.Channel{{ID: "ch-rest-1", LastMessageID: ""}},
			wantFetchCalls: 0,
		},
		{
			name:          "user guilds fetch error handled gracefully",
			now:           baseNow,
			userGuildsErr: true,
		},
		{
			name:            "guild channels and active threads error handled gracefully",
			now:             baseNow,
			guildID:         "guild-err",
			channelID:       "chan-err-fallback",
			lastMessageID:   "",
			restChannelsErr: true,
			restThreadsErr:  true,
		},
		{
			name:           "REST active threads and state threads merged and processed",
			now:            baseNow,
			guildID:        "guild-threads",
			channelID:      "chan-main",
			lastMessageID:  "",
			restThreads:    []*discordgo.Channel{{ID: "thread-rest", LastMessageID: ""}},
			threadsInState: []*discordgo.Channel{{ID: "thread-state", LastMessageID: ""}},
		},
		{
			name:              "nil guild in state and nil channel ignored safely",
			now:               baseNow,
			guildID:           "guild-valid",
			channelID:         "chan-valid",
			lastMessageID:     "",
			nilGuildInState:   true,
			nilChannelInGuild: true,
		},
		{
			name:            "nil and empty ID messages in fetched batch skipped",
			now:             baseNow,
			guildID:         "guild-nil-msg",
			channelID:       "chan-nil-msg",
			lastMessageID:   freshSnow2,
			messagesResp:    []*discordgo.Message{nil, {ID: ""}, freshMsg2},
			wantFetchCalls:  1,
			wantAfterQuery:  minSnow,
			wantEnqueuedIDs: []string{freshSnow2},
			wantWatermark:   freshSnow2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetChannelWatermarks()
			defer resetChannelWatermarks()

			if tc.initialWatermark != "" && tc.channelID != "" {
				advanceChannelWatermark(tc.channelID, tc.initialWatermark)
			}

			disp := &mockOutboundDispatcher{}
			river := &mockRiverInserter{}
			var srv *RouterServer
			if !tc.nilServer {
				srv = newTestRouter(Config{}, nil, nil, disp)
				if tc.hasRiver {
					srv.SetRiverClient(river)
				}
			}

			targetCtx := ctx
			if tc.canceledCtx {
				cCtx, cancel := context.WithCancel(ctx)
				cancel()
				targetCtx = cCtx
			}

			var session *discordgo.Session
			var transport *mockDiscordTransport
			if !tc.nilSession {
				var err error
				session, err = discordgo.New("Bot dummy-token")
				if err != nil {
					t.Fatalf("discordgo.New() err = %v", err)
				}
				gID := tc.guildID
				if gID == "" {
					gID = "default-guild"
				}
				transport = &mockDiscordTransport{
					messagesResp:    map[string][]*discordgo.Message{tc.channelID: tc.messagesResp},
					messagesErrCode: map[string]int{tc.channelID: tc.messagesErrCode},
					channelsResp:    map[string][]*discordgo.Channel{gID: tc.restChannels},
					channelsErr:     map[string]bool{gID: tc.restChannelsErr},
					threadsResp:     map[string]*discordgo.ThreadsList{gID: {Threads: tc.restThreads}},
					threadsErr:      map[string]bool{gID: tc.restThreadsErr},
					userGuildsResp:  tc.userGuildsResp,
					userGuildsErr:   tc.userGuildsErr,
				}
				session.Client = &http.Client{Transport: transport}
				if tc.botUserID != "" {
					session.State.User = &discordgo.User{ID: tc.botUserID}
				}
				if tc.userGuildsResp != nil || tc.userGuildsErr {
					session.State.Guilds = nil
				} else if tc.channelID != "" || tc.nilGuildInState {
					var chs []*discordgo.Channel
					if tc.nilChannelInGuild {
						chs = append(chs, nil, &discordgo.Channel{ID: ""})
					}
					if tc.channelID != "" {
						chs = append(chs, &discordgo.Channel{
							ID:            tc.channelID,
							GuildID:       tc.channelGuildID,
							LastMessageID: tc.lastMessageID,
						})
					}
					if tc.nilGuildInState {
						session.State.Guilds = append(session.State.Guilds, nil, &discordgo.Guild{ID: ""})
					}
					session.State.Guilds = append(session.State.Guilds, &discordgo.Guild{
						ID:       gID,
						Channels: chs,
						Threads:  tc.threadsInState,
					})
				}
			}

			if tc.sweepAlreadyRunning {
				discordSweepRunning.Store(true)
				defer discordSweepRunning.Store(false)
			}

			runDiscordCatchUpSweep(targetCtx, session, srv, tc.now)

			if tc.runSecondSweep {
				runDiscordCatchUpSweep(targetCtx, session, srv, tc.now)
			}

			if transport != nil {
				if len(transport.messageCalls) != tc.wantFetchCalls {
					t.Errorf("fetch calls = %d, want %d", len(transport.messageCalls), tc.wantFetchCalls)
				}
				if tc.wantAfterQuery != "" && len(transport.afterQueries) > 0 {
					if transport.afterQueries[0] != tc.wantAfterQuery {
						t.Errorf("after query = %q, want %q", transport.afterQueries[0], tc.wantAfterQuery)
					}
				}
			}

			if tc.hasRiver {
				if len(river.insertedDiscordJobs) != len(tc.wantEnqueuedIDs) {
					t.Errorf("insertedDiscordJobs = %d, want %d", len(river.insertedDiscordJobs), len(tc.wantEnqueuedIDs))
				}
				for idx, wantID := range tc.wantEnqueuedIDs {
					if idx < len(river.insertedDiscordJobs) && river.insertedDiscordJobs[idx].MessageID != wantID {
						t.Errorf("job[%d] MessageID = %q, want %q", idx, river.insertedDiscordJobs[idx].MessageID, wantID)
					}
				}
			} else if srv != nil {
				calls := disp.DiscordMessageCalls()
				if len(calls) != len(tc.wantEnqueuedIDs) {
					t.Errorf("DiscordMessageCalls = %d, want %d", len(calls), len(tc.wantEnqueuedIDs))
				}
				for idx, wantID := range tc.wantEnqueuedIDs {
					if idx < len(calls) && calls[idx].Message.ID != wantID {
						t.Errorf("call[%d] MessageID = %q, want %q", idx, calls[idx].Message.ID, wantID)
					}
				}
				// Verify guild ID fallback propagation
				if len(calls) > 1 && tc.guildID != "" {
					if calls[1].Message.GuildID != tc.guildID {
						t.Errorf("call[1] GuildID = %q, want %q", calls[1].Message.GuildID, tc.guildID)
					}
					if calls[1].Message.ReferencedMessage != nil && calls[1].Message.ReferencedMessage.GuildID != tc.guildID {
						t.Errorf("call[1] RefMessage GuildID = %q, want %q", calls[1].Message.ReferencedMessage.GuildID, tc.guildID)
					}
				}
			}

			if tc.channelID != "" && tc.wantWatermark != "" {
				wm := getChannelWatermark(tc.channelID)
				if wm != tc.wantWatermark {
					t.Errorf("channel watermark = %q, want %q", wm, tc.wantWatermark)
				}
			}
		})
	}
}

func TestEnqueueDiscordMessageDirect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		nilServer     bool
		nilMsg        bool
		nilAuthor     bool
		botUserID     string
		authorID      string
		refGuildID    string
		msgGuildID    string
		hasRiver      bool
		wantProcessed int
	}{
		{
			name:      "nil server returns",
			nilServer: true,
		},
		{
			name:   "nil message returns",
			nilMsg: true,
		},
		{
			name:      "nil author returns",
			nilAuthor: true,
		},
		{
			name:      "author is bot returns",
			botUserID: "bot-1",
			authorID:  "bot-1",
		},
		{
			name:          "valid direct message dispatch with ref message guild ID",
			botUserID:     "bot-1",
			authorID:      "user-1",
			msgGuildID:    "guild-1",
			refGuildID:    "guild-1",
			wantProcessed: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			var srv *RouterServer
			if !tc.nilServer {
				srv = newTestRouter(Config{}, nil, nil, disp)
			}
			var m *discordgo.Message
			if !tc.nilMsg {
				m = &discordgo.Message{
					ID:        "msg-direct-1",
					ChannelID: "chan-direct-1",
					GuildID:   tc.msgGuildID,
				}
				if !tc.nilAuthor {
					m.Author = &discordgo.User{ID: tc.authorID}
				}
				if tc.refGuildID != "" {
					m.ReferencedMessage = &discordgo.Message{
						ID:      "ref-direct-1",
						GuildID: tc.refGuildID,
					}
				}
			}

			enqueueDiscordMessage(ctx, srv, nil, m, tc.botUserID)

			if len(disp.DiscordMessageCalls()) != tc.wantProcessed {
				t.Errorf("DiscordMessageCalls = %d, want %d", len(disp.DiscordMessageCalls()), tc.wantProcessed)
			}
		})
	}
}

type mockDiscordTransport struct {
	mu              sync.Mutex
	messageCalls    []string
	afterQueries    []string
	channelsResp    map[string][]*discordgo.Channel
	channelsErr     map[string]bool
	threadsResp     map[string]*discordgo.ThreadsList
	threadsErr      map[string]bool
	userGuildsResp  []*discordgo.UserGuild
	userGuildsErr   bool
	messagesResp    map[string][]*discordgo.Message
	messagesErrCode map[string]int
}

func (m *mockDiscordTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := req.URL.Path
	var bodyBytes []byte
	statusCode := http.StatusOK

	if strings.HasSuffix(path, "/users/@me/guilds") {
		if m.userGuildsErr {
			statusCode = http.StatusInternalServerError
			bodyBytes = []byte(`{"message": "error"}`)
		} else {
			bodyBytes, _ = json.Marshal(m.userGuildsResp)
		}
	} else if strings.Contains(path, "/threads/active") {
		parts := strings.Split(path, "/")
		guildID := ""
		for idx, p := range parts {
			if p == "guilds" && idx+1 < len(parts) {
				guildID = parts[idx+1]
				break
			}
		}
		if m.threadsErr != nil && m.threadsErr[guildID] {
			statusCode = http.StatusInternalServerError
			bodyBytes = []byte(`{"message": "error"}`)
		} else {
			thList := &discordgo.ThreadsList{}
			if m.threadsResp != nil && m.threadsResp[guildID] != nil {
				thList = m.threadsResp[guildID]
			}
			bodyBytes, _ = json.Marshal(thList)
		}
	} else if strings.HasSuffix(path, "/channels") {
		parts := strings.Split(path, "/")
		guildID := ""
		for idx, p := range parts {
			if p == "guilds" && idx+1 < len(parts) {
				guildID = parts[idx+1]
				break
			}
		}
		if m.channelsErr != nil && m.channelsErr[guildID] {
			statusCode = http.StatusInternalServerError
			bodyBytes = []byte(`{"message": "error"}`)
		} else {
			var chList []*discordgo.Channel
			if m.channelsResp != nil && m.channelsResp[guildID] != nil {
				chList = m.channelsResp[guildID]
			}
			bodyBytes, _ = json.Marshal(chList)
		}
	} else if strings.HasSuffix(path, "/messages") {
		parts := strings.Split(path, "/")
		channelID := ""
		for idx, p := range parts {
			if p == "channels" && idx+1 < len(parts) {
				channelID = parts[idx+1]
				break
			}
		}
		m.messageCalls = append(m.messageCalls, channelID)
		m.afterQueries = append(m.afterQueries, req.URL.Query().Get("after"))

		if m.messagesErrCode != nil && m.messagesErrCode[channelID] > 0 {
			statusCode = m.messagesErrCode[channelID]
			bodyBytes = []byte(`{"message": "error"}`)
		} else {
			var msgList []*discordgo.Message
			if m.messagesResp != nil && m.messagesResp[channelID] != nil {
				msgList = m.messagesResp[channelID]
			}
			bodyBytes, _ = json.Marshal(msgList)
		}
	} else {
		bodyBytes = []byte(`{}`)
	}

	resp := &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
		Header:     http.Header{"Content-Type": {"application/json"}},
		Request:    req,
	}
	return resp, nil
}
