package discord

import (
	"context"
	"errors"
	"net/http"
	"slices"
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

// gatewayBots builds a bot map from agent/token pairs; the first agent is the default owner.
func gatewayBots(pairs ...string) map[string]config.BotConfig {
	bots := make(map[string]config.BotConfig, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		bots[pairs[i]] = config.BotConfig{DiscordToken: pairs[i+1], DefaultOwner: i == 0}
	}
	return bots
}

func gatewayServer(bots map[string]config.BotConfig) *testIngressServer {
	return &testIngressServer{cfg: &config.Config{Bots: bots}}
}

func TestDefaultGatewayOpener(t *testing.T) {
	sess, err := discordgo.New("Bot dummy")
	if err != nil {
		t.Fatalf("discordgo.New() err = %v", err)
	}
	sess.Client = &http.Client{Transport: &zeroCallTransport{}}
	gatewayOpenerMu.RLock()
	opener := gatewayOpener
	gatewayOpenerMu.RUnlock()
	if err := opener(sess); err == nil || !strings.Contains(err.Error(), "unexpected outbound HTTP call") {
		t.Errorf("default opener err = %v, want the blocked REST gateway lookup error", err)
	}
}

func TestStartDiscordGateway(t *testing.T) {
	ctx := context.Background()
	errRefused := errors.New("gateway connection refused")
	const refused = "gateway connection refused"
	wantIntents := discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent

	tests := []struct {
		name        string
		srv         *testIngressServer
		failToken   string
		wantErr     string
		wantTokens  []string
		wantLatency bool
	}{
		{name: "nil server returns no-op"},
		{name: "nil config returns no-op", srv: &testIngressServer{}},
		{name: "empty bots returns no-op", srv: gatewayServer(map[string]config.BotConfig{})},
		{name: "empty token skipped", srv: gatewayServer(gatewayBots("alpha", ""))},
		{name: "whitespace token skipped", srv: gatewayServer(gatewayBots("alpha", "   \t  "))},
		{
			name: "opener failure returns error", srv: gatewayServer(gatewayBots("alpha", "alpha-tok")), failToken: "Bot alpha-tok",
			wantErr: `open discord gateway for agent "alpha": ` + refused, wantTokens: []string{"Bot alpha-tok"},
		},
		{
			name: "second bot opener failure closes the first", srv: gatewayServer(gatewayBots("beta", "beta-tok", "alpha", "alpha-tok")),
			failToken: "Bot beta-tok", wantErr: `open discord gateway for agent "beta": ` + refused, wantTokens: []string{"Bot alpha-tok", "Bot beta-tok"},
			wantLatency: true,
		},
		{
			name: "Bot prefix kept", srv: gatewayServer(gatewayBots("alpha", "Bot secret-token-123")), wantTokens: []string{"Bot secret-token-123"},
			wantLatency: true,
		},
		{
			name: "raw token gains Bot prefix", srv: gatewayServer(gatewayBots("alpha", "secret-token-raw")), wantTokens: []string{"Bot secret-token-raw"},
			wantLatency: true,
		},
		{
			name: "multiple bots open in sorted order", srv: gatewayServer(gatewayBots("beta", "Bot beta-token", "alpha", "alpha-token")),
			wantTokens: []string{"Bot alpha-token", "Bot beta-token"}, wantLatency: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var openedTokens []string
			cleanup := SetGatewayOpenerForTest(func(dg *discordgo.Session) error {
				openedTokens = append(openedTokens, dg.Token)
				if dg.Identify.Intents != wantIntents {
					t.Errorf("Intents = %v, want %v", dg.Identify.Intents, wantIntents)
				}
				if dg.SyncEvents {
					t.Errorf("SyncEvents = true, want false")
				}
				if dg.Token == tc.failToken {
					return errRefused
				}
				return nil
			})
			t.Cleanup(cleanup)

			stopFn, err := StartDiscordGateway(ctx, tc.srv)
			if gotErr := errString(err); gotErr != tc.wantErr {
				t.Fatalf("StartDiscordGateway() err = %q, want %q", gotErr, tc.wantErr)
			}
			if !slices.Equal(openedTokens, tc.wantTokens) {
				t.Errorf("openedTokens = %v, want %v", openedTokens, tc.wantTokens)
			}
			if gotLatency := tc.srv != nil && tc.srv.latencyFn != nil; gotLatency != tc.wantLatency {
				t.Errorf("latency fn set = %v, want %v", gotLatency, tc.wantLatency)
			}
			if stopFn != nil {
				stopFn()
				stopFn() // idempotent
			}
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
		if err := session.State.GuildAdd(&discordgo.Guild{
			ID: "guild-1",
			Roles: []*discordgo.Role{
				{ID: "role-a", Name: "AdminRole"},
				{ID: "role-b", Name: "ReviewerRole"},
			},
		}); err != nil {
			t.Fatalf("session.State.GuildAdd() err = %v", err)
		}
	}

	mc := buildGatewayMessage(tc)
	NewDiscordGatewayMessageCreateHandlerForAgent(ctx, srv, "")(session, mc)

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
		{name: "nil server ignored", nilServer: true},
		{name: "nil message ignored", nilMsg: true},
		{name: "nil author ignored", nilAuthor: true},
		{name: "author is bot self ignored", botUserID: "bot-id-123", authorID: "bot-id-123"},
		{
			name: "direct dispatch fallback when bus is nil", botUserID: "bot-id-123", authorID: "user-456", msgGuildID: "guild-1",
			mentionRoles: []string{"role-a"}, wantProcessed: 1,
		},
		{name: "bus enqueue when bus is configured", botUserID: "bot-id-123", authorID: "user-456", hasBus: true, wantEnqueued: 1},
		{name: "bus insert error handled cleanly", botUserID: "bot-id-123", authorID: "user-456", hasBus: true, busErr: true},
		{name: "direct dispatch error handled cleanly", botUserID: "bot-id-123", authorID: "user-456", directErr: true, wantProcessed: 1},
		{
			name: "mention roles and referenced roles resolved via session state", botUserID: "bot-id-123", authorID: "user-456", msgGuildID: "guild-1",
			mentionRoles: []string{"role-a"}, refRoles: []string{"role-b"}, hasRolesInState: true, wantProcessed: 1,
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

	NewDiscordGatewayInteractionCreateHandlerForAgent(ctx, srv, "")(nil, ic)

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
	const command = discordgo.InteractionApplicationCommand

	tests := []gatewayInteractionCreateCase{
		{name: "nil server ignored", nilServer: true},
		{name: "nil interaction ignored", nilInteraction: true},
		{name: "non-command interaction ignored", interactionType: discordgo.InteractionMessageComponent},
		{name: "direct dispatch fallback when bus is nil", interactionType: command, wantProcessed: 1},
		{name: "bus enqueue when bus is configured", interactionType: command, hasBus: true, wantEnqueued: 1},
		{name: "bus insert error handled cleanly", interactionType: command, hasBus: true, busErr: true},
		{name: "direct dispatch error handled cleanly", interactionType: command, directErr: true, wantProcessed: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runGatewayInteractionCreateCase(ctx, t, tc)
		})
	}
}

func TestHandleGatewayReady(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled so the background sweep exits immediately

	tests := []struct {
		name      string
		wg        *sync.WaitGroup
		agent     string
		stateUser string
		readyUser string
		wantUser  string
	}{
		{name: "sweep tracked by wait group", wg: &sync.WaitGroup{}, agent: "ready-wg", readyUser: "bot-wg-1", wantUser: "bot-wg-1"},
		{name: "untracked sweep without wait group", agent: "ready-nowg", readyUser: "bot-ready-1", wantUser: "bot-ready-1"},
		{name: "existing state user kept", wg: &sync.WaitGroup{}, agent: "ready-kept", stateUser: "bot-old", readyUser: "bot-new", wantUser: "bot-old"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess, err := discordgo.New("Bot dummy")
			if err != nil {
				t.Fatalf("discordgo.New: %v", err)
			}
			sess.Client = &http.Client{Transport: &mockDiscordTransport{}}
			if tc.stateUser != "" {
				sess.State.User = &discordgo.User{ID: tc.stateUser}
			}

			NewDiscordGatewayReadyHandlerForAgent(ctx, &testIngressServer{}, tc.wg, tc.agent)(sess, &discordgo.Ready{User: &discordgo.User{ID: tc.readyUser}})
			if tc.wg != nil {
				tc.wg.Wait()
			}

			sess.State.RLock()
			defer sess.State.RUnlock()
			if sess.State.User == nil || sess.State.User.ID != tc.wantUser {
				t.Errorf("User = %v, want %s", sess.State.User, tc.wantUser)
			}
		})
	}
}

func TestSessionBotUserID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		session *discordgo.Session
		want    string
	}{
		{"nil session returns empty", nil, ""},
		{"nil state returns empty", &discordgo.Session{}, ""},
		{"nil user returns empty", &discordgo.Session{State: &discordgo.State{}}, ""},
		{"state user returns bot id", &discordgo.Session{State: &discordgo.State{User: &discordgo.User{ID: "bot-999"}}}, "bot-999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SessionBotUserID(tc.session); got != tc.want {
				t.Errorf("SessionBotUserID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDiscordSnowflakeFromTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{"zero time returns 0", time.Time{}, "0"},
		{"before discord epoch returns 0", time.Date(2014, 12, 31, 23, 59, 59, 0, time.UTC), "0"},
		{"exact discord epoch returns 0", time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC), "0"},
		{"1000ms after epoch computes shifted snowflake", time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC), "4194304000"},
		{"known 2026 timestamp", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "1456074443980800000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DiscordSnowflakeFromTime(tc.input); got != tc.want {
				t.Errorf("DiscordSnowflakeFromTime(%v) = %q, want %q", tc.input, got, tc.want)
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
