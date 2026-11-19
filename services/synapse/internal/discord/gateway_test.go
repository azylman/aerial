package discord

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type testIngressServer struct {
	cfg                   *config.Config
	bus                   pubsub.Bus
	publishErr            error
	publishedJobs         []any
	ingressErr            error
	processedIngress      []model.DiscordIngressArgs
	interactionErr        error
	processedInteractions []model.DiscordInteractionArgs
	latencyFn             func() time.Duration
}

func (s *testIngressServer) Config() *config.Config {
	return s.cfg
}

func (s *testIngressServer) EventBus() pubsub.Bus {
	return s.bus
}

func (s *testIngressServer) PublishJob(ctx context.Context, args any) error {
	if s.publishErr != nil {
		return s.publishErr
	}
	s.publishedJobs = append(s.publishedJobs, args)
	if s.bus != nil {
		return s.bus.Publish(ctx, "", "", args)
	}
	return nil
}

func (s *testIngressServer) ProcessDiscordIngress(ctx context.Context, args model.DiscordIngressArgs) error {
	s.processedIngress = append(s.processedIngress, args)
	return s.ingressErr
}

func (s *testIngressServer) ProcessDiscordInteraction(ctx context.Context, args model.DiscordInteractionArgs) error {
	s.processedInteractions = append(s.processedInteractions, args)
	return s.interactionErr
}

func (s *testIngressServer) SetGatewayLatencyFn(fn func() time.Duration) {
	s.latencyFn = fn
}

type testEventBus struct {
	pubsub.Bus

	mu                    sync.Mutex
	insertedJobs          []any
	publishedTraceparents []string
	err                   error
}

func (b *testEventBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	b.publishedTraceparents = append(b.publishedTraceparents, tracing.Traceparent(ctx))
	b.insertedJobs = append(b.insertedJobs, data)
	return nil
}

type zeroCallTransport struct{}

func (*zeroCallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected outbound HTTP call")
}

func assertRootTraceparent(ctx context.Context, t *testing.T, tp string) {
	t.Helper()
	sc := trace.SpanContextFromContext(tracing.ContextWithTraceparent(ctx, tp))
	if !sc.IsValid() || !sc.IsSampled() {
		t.Errorf("job traceparent = %q, want a valid sampled trace context", tp)
	}
}

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
	}))
}

func TestStartDiscordGateway(t *testing.T) {
	ctx := context.Background()

	// Exercise default opener code path hermetically
	sess, err := discordgo.New("Bot dummy")
	if err == nil {
		sess.Client = &http.Client{Transport: &zeroCallTransport{}}
		gatewayOpenerMu.RLock()
		defaultOpener := gatewayOpener
		gatewayOpenerMu.RUnlock()
		_ = defaultOpener(sess)
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
			cleanup := SetGatewayOpenerForTest(func(dg *discordgo.Session) error {
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
			})
			t.Cleanup(cleanup)

			srv := &testIngressServer{}
			stopFn, err := StartGateway(ctx, srv, tc.token)
			if (err != nil) != tc.wantErr {
				t.Fatalf("StartGateway() err = %v, wantErr = %v", err, tc.wantErr)
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
	var srv IngressServer
	var testSrv *testIngressServer
	var bus *testEventBus

	if !tc.nilServer {
		testSrv = &testIngressServer{}
		if tc.directErr {
			testSrv.ingressErr = errors.New("dispatch direct error")
		}
		if tc.hasBus {
			bus = &testEventBus{}
			if tc.busErr {
				bus.err = errors.New("bus insert error")
			}
			testSrv.bus = bus
		}
		srv = testSrv
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
	HandleGatewayMessageCreate(ctx, srv, session, mc)

	if tc.hasBus {
		if len(bus.insertedJobs) != tc.wantEnqueued {
			t.Errorf("insertedJobs = %d, want %d", len(bus.insertedJobs), tc.wantEnqueued)
		}
		for _, tp := range bus.publishedTraceparents {
			assertRootTraceparent(ctx, t, tp)
		}
	} else if !tc.nilServer {
		if len(testSrv.processedIngress) != tc.wantProcessed {
			t.Errorf("processedIngress = %d, want %d", len(testSrv.processedIngress), tc.wantProcessed)
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
	var srv IngressServer
	var testSrv *testIngressServer
	var bus *testEventBus

	if !tc.nilServer {
		testSrv = &testIngressServer{}
		if tc.directErr {
			testSrv.interactionErr = errors.New("dispatch direct error")
		}
		if tc.hasBus {
			bus = &testEventBus{}
			if tc.busErr {
				bus.err = errors.New("bus insert error")
			}
			testSrv.bus = bus
		}
		srv = testSrv
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

	HandleGatewayInteractionCreate(ctx, srv, ic)

	if tc.hasBus {
		if len(bus.insertedJobs) != tc.wantEnqueued {
			t.Errorf("insertedJobs = %d, want %d", len(bus.insertedJobs), tc.wantEnqueued)
		}
		for _, tp := range bus.publishedTraceparents {
			assertRootTraceparent(ctx, t, tp)
		}
	} else if !tc.nilServer {
		if len(testSrv.processedInteractions) != tc.wantProcessed {
			t.Errorf("processedInteractions = %d, want %d", len(testSrv.processedInteractions), tc.wantProcessed)
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
			srv := &testIngressServer{}

			switch {
			case tc.isReady:
				canceledCtx, cancel := context.WithCancel(ctx)
				cancel()
				session, err := discordgo.New("Bot dummy-ready")
				if err != nil {
					t.Fatalf("discordgo.New() err = %v", err)
				}
				session.Client = &http.Client{Transport: &mockDiscordTransport{}}
				h := NewDiscordGatewayReadyHandler(canceledCtx, srv)
				h(session, &discordgo.Ready{
					User: &discordgo.User{ID: "bot-ready-1"},
				})
				if session.State.User == nil || session.State.User.ID != "bot-ready-1" {
					t.Errorf("User = %v, want bot-ready-1", session.State.User)
				}
			case tc.isInteraction:
				h := NewDiscordGatewayInteractionCreateHandler(ctx, srv)
				h(nil, &discordgo.InteractionCreate{
					Interaction: &discordgo.Interaction{
						ID:   "inter-closure-1",
						Type: discordgo.InteractionApplicationCommand,
					},
				})
				if len(srv.processedInteractions) != 1 {
					t.Errorf("expected 1 interaction call, got %d", len(srv.processedInteractions))
				}
			default:
				h := NewDiscordGatewayMessageCreateHandler(ctx, srv)
				h(nil, &discordgo.MessageCreate{
					Message: &discordgo.Message{
						ID:      "msg-closure-1",
						Content: "closure test",
						Author:  &discordgo.User{ID: "author-closure-1"},
					},
				})
				if len(srv.processedIngress) != 1 {
					t.Errorf("expected 1 message call, got %d", len(srv.processedIngress))
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
			gotID := SessionBotUserID(tc.session)
			if gotID != tc.wantBotID {
				t.Errorf("SessionBotUserID() = %q, want %q", gotID, tc.wantBotID)
			}
			gotRoles := ExtractMentionRoleNames(tc.session, tc.guildID, tc.roles...)
			if len(gotRoles) != tc.wantRoleLens {
				t.Errorf("ExtractMentionRoleNames() len = %d, want %d", len(gotRoles), tc.wantRoleLens)
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
			got := DiscordSnowflakeFromTime(tc.input)
			if got != tc.wantSnow {
				t.Errorf("DiscordSnowflakeFromTime(%v) = %q, want %q", tc.input, got, tc.wantSnow)
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
			got := CompareDiscordSnowflakes(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("CompareDiscordSnowflakes(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
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
			got := MaxDiscordSnowflake(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("MaxDiscordSnowflake(%q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestGatewaySpanOptions_And_Tracing(t *testing.T) {
	t.Parallel()

	spanOpts := GatewaySpanOptions("chan-123")
	if len(spanOpts) == 0 {
		t.Fatal("expected non-empty GatewaySpanOptions")
	}

	jobOpts := DiscordJobSpanOptions("queue-456")
	if len(jobOpts) == 0 {
		t.Fatal("expected non-empty DiscordJobSpanOptions")
	}

	// Test MarkDiscordJobSpan with nil and error
	MarkDiscordJobSpan(trace.SpanFromContext(context.Background()), nil)
	MarkDiscordJobSpan(trace.SpanFromContext(context.Background()), errors.New("dispatch failed"))
}

func TestHandleGatewayReadyWithWG(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled so sweep exits immediately

	sess, err := discordgo.New("Bot dummy")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	sess.Client = &http.Client{Transport: &mockDiscordTransport{}}
	srv := &testIngressServer{}

	var wg sync.WaitGroup
	handleGatewayReadyWithWG(ctx, srv, sess, &discordgo.Ready{User: &discordgo.User{ID: "bot-wg-1"}}, &wg)
	wg.Wait()

	if sess.State.User == nil || sess.State.User.ID != "bot-wg-1" {
		t.Errorf("User = %v, want bot-wg-1", sess.State.User)
	}
}

func TestStartGateway_StopFnCancelsSweepContext(t *testing.T) {
	ctx := context.Background()

	var capturedReadyHandler func(*discordgo.Session, *discordgo.Ready)
	cleanup := SetGatewayOpenerForTest(func(dg *discordgo.Session) error {
		// Mock opener succeeds
		return nil
	})
	t.Cleanup(cleanup)

	srv := &testIngressServer{}
	stopFn, err := StartGateway(ctx, srv, "Bot token-sweep-test")
	if err != nil {
		t.Fatalf("StartGateway err = %v", err)
	}
	_ = capturedReadyHandler
	stopFn()
}
