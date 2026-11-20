package discord

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const discordEpochMillis int64 = 1420070400000

// IngressServer defines the server interface required by Discord Gateway handlers.
type IngressServer interface {
	Config() *config.Config
	EventBus() pubsub.Bus
	PublishJob(ctx context.Context, args any) error
	ProcessDiscordIngress(ctx context.Context, args model.DiscordIngressArgs) error
	ProcessDiscordInteraction(ctx context.Context, args model.DiscordInteractionArgs) error
	SetGatewayLatencyFn(fn func() time.Duration)
}

var (
	channelWatermarksMu sync.RWMutex
	channelWatermarks   = make(map[string]string)

	gatewayOpenerMu sync.RWMutex
	gatewayOpener   = func(dg *discordgo.Session) error {
		return dg.Open()
	}
)

// GetAgentChannelWatermark returns the highest message snowflake processed for an agent's channel.
func GetAgentChannelWatermark(agent, channelID string) string {
	channelWatermarksMu.RLock()
	defer channelWatermarksMu.RUnlock()
	if agent != "" {
		return channelWatermarks[agent+":"+channelID]
	}
	return channelWatermarks[channelID]
}

// AdvanceAgentChannelWatermark advances the tracked watermark for an agent's channel if messageID is newer.
func AdvanceAgentChannelWatermark(agent, channelID, messageID string) {
	if channelID == "" || messageID == "" {
		return
	}
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	key := channelID
	if agent != "" {
		key = agent + ":" + channelID
	}
	curr := channelWatermarks[key]
	if CompareDiscordSnowflakes(messageID, curr) > 0 {
		channelWatermarks[key] = messageID
	}
}

// ParseSnowflakeUint64 parses a snowflake string as uint64, returning 0 if empty or invalid.
func ParseSnowflakeUint64(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// DiscordSnowflakeFromTime computes a Discord snowflake ID corresponding to the given timestamp.
func DiscordSnowflakeFromTime(t time.Time) string {
	ms := t.UTC().UnixMilli() - discordEpochMillis
	if ms <= 0 {
		return "0"
	}
	return strconv.FormatInt(ms<<22, 10)
}

// CompareDiscordSnowflakes compares two Discord snowflake string IDs numerically.
func CompareDiscordSnowflakes(a, b string) int {
	valA := ParseSnowflakeUint64(a)
	valB := ParseSnowflakeUint64(b)
	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

// MaxDiscordSnowflake returns the larger of two Discord snowflake string IDs.
func MaxDiscordSnowflake(a, b string) string {
	cmp := CompareDiscordSnowflakes(a, b)
	if cmp > 0 {
		return a
	}
	if cmp < 0 {
		return b
	}
	if a != "" {
		return a
	}
	return b
}

// GatewaySpanOptions configure the root span of a turn's trace, opened when synapse receives a
// Discord gateway event.
func GatewaySpanOptions(channelID string, extraAttrs ...attribute.KeyValue) []trace.SpanStartOption {
	attrs := make([]attribute.KeyValue, 0, 2+len(extraAttrs))
	attrs = append(attrs,
		attribute.String("messaging.system", "discord"),
		attribute.String("discord.channel.id", channelID),
	)
	attrs = append(attrs, extraAttrs...)
	return []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	}
}

// DiscordJobSpanOptions configure the consumer span a Discord job opens under the gateway trace.
func DiscordJobSpanOptions(queue string) []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", queue),
		),
	}
}

// MarkDiscordJobSpan records the job outcome without error text.
func MarkDiscordJobSpan(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "eri dispatch failed")
	}
}

func closeSessions(sessions []*discordgo.Session) {
	for _, s := range sessions {
		if s != nil {
			if err := s.Close(); err != nil {
				log.Printf("[synapse] [discord] error closing discord gateway session: %v", err)
			}
		}
	}
}

// StartDiscordGateway initializes and starts Discord Gateway WebSocket connections for all configured bots.
func StartDiscordGateway(ctx context.Context, server IngressServer) (func(), error) {
	if isNilServer(server) {
		return func() {}, nil
	}
	cfg := server.Config()
	if cfg == nil {
		return func() {}, nil
	}
	if len(cfg.Bots) == 0 {
		return StartGateway(ctx, server, cfg.DiscordToken)
	}

	var agentNames []string
	for agent := range cfg.Bots {
		agentNames = append(agentNames, agent)
	}
	sort.Strings(agentNames)

	gatewayCtx, cancelGateway := context.WithCancel(ctx)
	var sweepWG sync.WaitGroup
	var sessions []*discordgo.Session
	var latencies []func() time.Duration

	gatewayOpenerMu.RLock()
	opener := gatewayOpener
	gatewayOpenerMu.RUnlock()

	for _, agent := range agentNames {
		botCfg := cfg.Bots[agent]
		cleanToken := strings.TrimSpace(botCfg.DiscordToken)
		if cleanToken == "" {
			log.Printf("[synapse] [discord] bot %q Discord token is empty; gateway session skipped", agent)
			continue
		}
		botToken := cleanToken
		if !strings.HasPrefix(botToken, "Bot ") {
			botToken = "Bot " + botToken
		}

		dg, err := discordgo.New(botToken)
		if err != nil {
			cancelGateway()
			closeSessions(sessions)
			return nil, fmt.Errorf("create discordgo session for agent %q: %w", agent, err)
		}

		dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
		dg.SyncEvents = false

		dg.AddHandler(NewAgentDiscordGatewayMessageCreateHandler(gatewayCtx, server, agent))
		dg.AddHandler(NewAgentDiscordGatewayInteractionCreateHandler(gatewayCtx, server, agent))
		dg.AddHandler(NewAgentDiscordGatewayReadyHandler(gatewayCtx, server, agent, &sweepWG))

		if err := opener(dg); err != nil {
			cancelGateway()
			closeSessions(sessions)
			return nil, fmt.Errorf("open discord gateway for agent %q: %w", agent, err)
		}
		log.Printf("[synapse] [discord] Discord Gateway connected successfully for agent %q", agent)
		sessions = append(sessions, dg)
		latencies = append(latencies, dg.HeartbeatLatency)
	}

	if len(sessions) == 0 {
		log.Println("[synapse] [discord] no active Discord Gateway sessions started")
		cancelGateway()
		return func() {}, nil
	}

	if server != nil && len(latencies) > 0 {
		primaryLatency := latencies[0]
		server.SetGatewayLatencyFn(primaryLatency)
		metrics.SetDiscordGatewayLatency(primaryLatency().Seconds())
	}

	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(func() {
			cancelGateway()
			sweepWG.Wait()
			closeSessions(sessions)
		})
	}
	return stopFn, nil
}

// StartGateway initializes and starts the Discord Gateway WebSocket connection for a single token.
func StartGateway(ctx context.Context, server IngressServer, token string) (func(), error) {
	cleanToken := strings.TrimSpace(token)
	if cleanToken == "" {
		log.Println("[synapse] [discord] DISCORD_TOKEN is empty; Discord Gateway disabled")
		return func() {}, nil
	}

	botToken := cleanToken
	if !strings.HasPrefix(botToken, "Bot ") {
		botToken = "Bot " + botToken
	}

	dg, err := discordgo.New(botToken)
	if err != nil {
		return nil, fmt.Errorf("create discordgo session: %w", err)
	}

	dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
	dg.SyncEvents = false

	gatewayCtx, cancelGateway := context.WithCancel(ctx)
	var sweepWG sync.WaitGroup

	dg.AddHandler(NewDiscordGatewayMessageCreateHandler(gatewayCtx, server))
	dg.AddHandler(NewDiscordGatewayInteractionCreateHandler(gatewayCtx, server))
	dg.AddHandler(NewDiscordGatewayReadyHandler(gatewayCtx, server, &sweepWG))

	gatewayOpenerMu.RLock()
	opener := gatewayOpener
	gatewayOpenerMu.RUnlock()

	if err := opener(dg); err != nil {
		cancelGateway()
		return nil, fmt.Errorf("open discord gateway: %w", err)
	}
	log.Println("[synapse] [discord] Discord Gateway connected successfully")

	if server != nil {
		server.SetGatewayLatencyFn(dg.HeartbeatLatency)
		metrics.SetDiscordGatewayLatency(dg.HeartbeatLatency().Seconds())
	}

	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(func() {
			cancelGateway()
			sweepWG.Wait()
			if err := dg.Close(); err != nil {
				log.Printf("[synapse] [discord] error closing discord gateway session: %v", err)
			}
		})
	}
	return stopFn, nil
}

// NewDiscordGatewayMessageCreateHandler creates a discordgo MessageCreate handler.
func NewDiscordGatewayMessageCreateHandler(ctx context.Context, server IngressServer) func(s *discordgo.Session, m *discordgo.MessageCreate) {
	return NewAgentDiscordGatewayMessageCreateHandler(ctx, server, "")
}

// NewAgentDiscordGatewayMessageCreateHandler creates an agent-scoped discordgo MessageCreate handler.
func NewAgentDiscordGatewayMessageCreateHandler(ctx context.Context, server IngressServer, agent string) func(s *discordgo.Session, m *discordgo.MessageCreate) {
	return func(s *discordgo.Session, m *discordgo.MessageCreate) {
		HandleAgentGatewayMessageCreate(ctx, server, s, m, agent)
	}
}

// NewDiscordGatewayInteractionCreateHandler creates a discordgo InteractionCreate handler.
func NewDiscordGatewayInteractionCreateHandler(ctx context.Context, server IngressServer) func(s *discordgo.Session, i *discordgo.InteractionCreate) {
	return NewAgentDiscordGatewayInteractionCreateHandler(ctx, server, "")
}

// NewAgentDiscordGatewayInteractionCreateHandler creates an agent-scoped discordgo InteractionCreate handler.
func NewAgentDiscordGatewayInteractionCreateHandler(ctx context.Context, server IngressServer, agent string) func(s *discordgo.Session, i *discordgo.InteractionCreate) {
	return func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		HandleAgentGatewayInteractionCreate(ctx, server, i, agent)
	}
}

// NewDiscordGatewayReadyHandler creates a discordgo Ready handler.
func NewDiscordGatewayReadyHandler(ctx context.Context, server IngressServer, wg *sync.WaitGroup) func(s *discordgo.Session, r *discordgo.Ready) {
	return NewAgentDiscordGatewayReadyHandler(ctx, server, "", wg)
}

// NewAgentDiscordGatewayReadyHandler creates an agent-scoped discordgo Ready handler.
func NewAgentDiscordGatewayReadyHandler(ctx context.Context, server IngressServer, agent string, wg *sync.WaitGroup) func(s *discordgo.Session, r *discordgo.Ready) {
	return func(s *discordgo.Session, r *discordgo.Ready) {
		HandleAgentGatewayReady(ctx, server, s, r, agent, wg)
	}
}

// HandleAgentGatewayReady updates session state and triggers an agent-scoped background catch-up sweep.
func HandleAgentGatewayReady(ctx context.Context, server IngressServer, s *discordgo.Session, r *discordgo.Ready, agent string, wg *sync.WaitGroup) {
	if s != nil && r != nil && r.User != nil {
		if s.State != nil {
			s.State.Lock()
			if s.State.User == nil || s.State.User.ID == "" {
				s.State.User = r.User
			}
			s.State.Unlock()
		}
	}
	if wg != nil {
		wg.Go(func() {
			RunStartupCatchup(ctx, s, server, agent, time.Now().UTC())
		})
		return
	}
	go RunStartupCatchup(ctx, s, server, agent, time.Now().UTC())
}

// SessionBotUserID safely extracts the current bot user ID from discordgo session state.
func SessionBotUserID(s *discordgo.Session) string {
	if s == nil || s.State == nil {
		return ""
	}
	s.State.RLock()
	defer s.State.RUnlock()
	if s.State.User == nil {
		return ""
	}
	return s.State.User.ID
}

// ExtractMentionRoleNames resolves role names from guild state for mention role IDs.
func ExtractMentionRoleNames(s *discordgo.Session, guildID string, roleGroups ...[]string) map[string]string {
	if s == nil || s.State == nil || guildID == "" {
		return nil
	}
	names := make(map[string]string)
	for _, group := range roleGroups {
		for _, roleID := range group {
			if r, err := s.State.Role(guildID, roleID); err == nil && r != nil && r.Name != "" {
				names[roleID] = r.Name
			}
		}
	}
	return names
}

func isNilServer(s IngressServer) bool {
	if s == nil {
		return true
	}
	val := reflect.ValueOf(s)
	return val.Kind() == reflect.Pointer && val.IsNil()
}

// HandleAgentGatewayMessageCreate advances agent watermarks and enqueues inbound messages with agent attribution.
func HandleAgentGatewayMessageCreate(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.MessageCreate, agent string) {
	if m != nil && m.ChannelID != "" && m.ID != "" {
		AdvanceAgentChannelWatermark(agent, m.ChannelID, m.ID)
	}
	if isNilServer(server) || m == nil || m.Message == nil || m.Author == nil {
		return
	}
	botUserID := SessionBotUserID(s)
	if botUserID != "" && m.Author.ID == botUserID {
		return
	}
	EnqueueAgentDiscordMessage(ctx, server, s, m.Message, botUserID, agent)
}

// EnqueueAgentDiscordMessage prepares and publishes or dispatches an inbound Discord message with agent attribution.
func EnqueueAgentDiscordMessage(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.Message, botUserID, agent string) {
	if isNilServer(server) || m == nil || m.Author == nil {
		return
	}
	if botUserID != "" && m.Author.ID == botUserID {
		return
	}

	if agent == "" && server.Config() != nil {
		agent = server.Config().DefaultOwnerAgent()
	}

	if m.ReferencedMessage != nil && m.ReferencedMessage.GuildID == "" {
		m.ReferencedMessage.GuildID = m.GuildID
	}

	var refRoles []string
	if m.ReferencedMessage != nil {
		refRoles = m.ReferencedMessage.MentionRoles
	}
	roleNames := ExtractMentionRoleNames(s, m.GuildID, m.MentionRoles, refRoles)

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	var spanOpts []trace.SpanStartOption
	if agent != "" {
		spanOpts = GatewaySpanOptions(m.ChannelID, attribute.String("aerial.agent", agent))
	} else {
		spanOpts = GatewaySpanOptions(m.ChannelID)
	}
	enqueueCtx, span := tracing.Tracer().Start(enqueueCtx, "discord.gateway MESSAGE_CREATE", spanOpts...)
	defer span.End()

	args := model.DiscordIngressArgs{
		MessageID: m.ID,
		BotUserID: botUserID,
		RoleNames: roleNames,
		Message:   m,
		Agent:     agent,
	}

	if server.EventBus() != nil {
		if err := server.PublishJob(enqueueCtx, args); err != nil {
			slog.ErrorContext(enqueueCtx, "[synapse] [discord] failed to publish message to jetstream", "message_id", m.ID, "error", err)
		}
	} else {
		if err := server.ProcessDiscordIngress(enqueueCtx, args); err != nil {
			slog.ErrorContext(enqueueCtx, "[synapse] [discord] failed to process message directly", "message_id", m.ID, "error", err)
		}
	}
}

// HandleAgentGatewayInteractionCreate enqueues application command interactions with agent attribution.
func HandleAgentGatewayInteractionCreate(ctx context.Context, server IngressServer, i *discordgo.InteractionCreate, agent string) {
	if isNilServer(server) || i == nil || i.Interaction == nil || i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	if agent == "" && server.Config() != nil {
		agent = server.Config().DefaultOwnerAgent()
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()

	var spanOpts []trace.SpanStartOption
	if agent != "" {
		spanOpts = GatewaySpanOptions(i.ChannelID, attribute.String("aerial.agent", agent))
	} else {
		spanOpts = GatewaySpanOptions(i.ChannelID)
	}
	enqueueCtx, span := tracing.Tracer().Start(enqueueCtx, "discord.gateway INTERACTION_CREATE", spanOpts...)
	defer span.End()

	args := model.DiscordInteractionArgs{
		InteractionID: i.ID,
		Interaction:   i.Interaction,
		Agent:         agent,
	}

	if server.EventBus() != nil {
		if err := server.PublishJob(enqueueCtx, args); err != nil {
			slog.ErrorContext(enqueueCtx, "[synapse] [discord] failed to publish interaction to jetstream", "interaction_id", i.ID, "error", err)
		}
	} else {
		if err := server.ProcessDiscordInteraction(enqueueCtx, args); err != nil {
			slog.ErrorContext(enqueueCtx, "[synapse] [discord] failed to process interaction directly", "interaction_id", i.ID, "error", err)
		}
	}
}
