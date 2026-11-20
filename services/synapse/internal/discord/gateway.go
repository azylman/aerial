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

func watermarkKey(agent, channelID string) string {
	if agent == "" {
		return channelID
	}
	return agent + ":" + channelID
}

// GetChannelWatermarkForAgent returns the highest message snowflake processed for a channel under a specific agent.
func GetChannelWatermarkForAgent(agent, channelID string) string {
	channelWatermarksMu.RLock()
	defer channelWatermarksMu.RUnlock()
	key := watermarkKey(agent, channelID)
	return channelWatermarks[key]
}

// AdvanceChannelWatermarkForAgent advances the tracked watermark for a channel under a specific agent if messageID is newer.
func AdvanceChannelWatermarkForAgent(agent, channelID, messageID string) {
	if channelID == "" || messageID == "" {
		return
	}
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	key := watermarkKey(agent, channelID)
	if CompareDiscordSnowflakes(messageID, channelWatermarks[key]) > 0 {
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
func GatewaySpanOptions(channelID string) []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "discord"),
			attribute.String("discord.channel.id", channelID),
		),
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

// StartDiscordGateway initializes and starts Discord Gateway WebSocket connections for all configured bots in cfg.Bots.
func StartDiscordGateway(ctx context.Context, server IngressServer) (func(), error) {
	if isNilServer(server) || server.Config() == nil {
		return func() {}, nil
	}
	cfg := server.Config()
	bots := cfg.Bots
	if len(bots) == 0 {
		return func() {}, nil
	}

	agents := make([]string, 0, len(bots))
	for agent := range bots {
		agents = append(agents, agent)
	}
	sort.Strings(agents)

	gatewayCtx, cancelGateway := context.WithCancel(ctx)
	var sweepWG sync.WaitGroup
	var sessions []*discordgo.Session

	gatewayOpenerMu.RLock()
	opener := gatewayOpener
	gatewayOpenerMu.RUnlock()

	cleanupAll := func() {
		cancelGateway()
		sweepWG.Wait()
		for _, dg := range sessions {
			if err := dg.Close(); err != nil {
				log.Printf("[synapse] [discord] error closing discord gateway session: %v", err)
			}
		}
	}

	for _, agent := range agents {
		bot := bots[agent]
		cleanToken := strings.TrimSpace(bot.DiscordToken)
		if cleanToken == "" {
			log.Printf("[synapse] [discord] DiscordToken for agent %q is empty; skipping Gateway", agent)
			continue
		}
		botToken := cleanToken
		if !strings.HasPrefix(botToken, "Bot ") {
			botToken = "Bot " + botToken
		}

		dg, err := discordgo.New(botToken)
		if err != nil {
			cleanupAll()
			return nil, fmt.Errorf("create discordgo session for agent %q: %w", agent, err)
		}

		dg.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages | discordgo.IntentMessageContent
		dg.SyncEvents = false

		dg.AddHandler(NewDiscordGatewayMessageCreateHandlerForAgent(gatewayCtx, server, agent))
		dg.AddHandler(NewDiscordGatewayInteractionCreateHandlerForAgent(gatewayCtx, server, agent))
		dg.AddHandler(NewDiscordGatewayReadyHandlerForAgent(gatewayCtx, server, &sweepWG, agent))

		sessions = append(sessions, dg)
		if err := opener(dg); err != nil {
			cleanupAll()
			return nil, fmt.Errorf("open discord gateway for agent %q: %w", agent, err)
		}
		log.Printf("[synapse] [discord] Discord Gateway for agent %q connected successfully", agent)

		if agent == cfg.DefaultOwnerAgent() || len(sessions) == 1 {
			server.SetGatewayLatencyFn(dg.HeartbeatLatency)
			metrics.SetDiscordGatewayLatency(dg.HeartbeatLatency().Seconds())
		}
	}

	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(cleanupAll)
	}
	return stopFn, nil
}

// NewDiscordGatewayMessageCreateHandlerForAgent creates a discordgo MessageCreate handler for a specific agent.
func NewDiscordGatewayMessageCreateHandlerForAgent(ctx context.Context, server IngressServer, agent string) func(s *discordgo.Session, m *discordgo.MessageCreate) {
	return func(s *discordgo.Session, m *discordgo.MessageCreate) {
		HandleGatewayMessageCreateForAgent(ctx, server, s, m, agent)
	}
}

// NewDiscordGatewayInteractionCreateHandlerForAgent creates a discordgo InteractionCreate handler for a specific agent.
func NewDiscordGatewayInteractionCreateHandlerForAgent(ctx context.Context, server IngressServer, agent string) func(s *discordgo.Session, i *discordgo.InteractionCreate) {
	return func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		HandleGatewayInteractionCreateForAgent(ctx, server, i, agent)
	}
}

// NewDiscordGatewayReadyHandlerForAgent creates a discordgo Ready handler for a specific agent.
func NewDiscordGatewayReadyHandlerForAgent(ctx context.Context, server IngressServer, wg *sync.WaitGroup, agent string) func(s *discordgo.Session, r *discordgo.Ready) {
	return func(s *discordgo.Session, r *discordgo.Ready) {
		HandleGatewayReadyForAgent(ctx, server, s, r, wg, agent)
	}
}

// HandleGatewayReadyForAgent updates session state and triggers a background catch-up sweep for an agent.
func HandleGatewayReadyForAgent(ctx context.Context, server IngressServer, s *discordgo.Session, r *discordgo.Ready, wg *sync.WaitGroup, agent string) {
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
			RunCatchUpSweepForAgent(ctx, s, server, time.Now().UTC(), agent)
		})
		return
	}
	go RunCatchUpSweepForAgent(ctx, s, server, time.Now().UTC(), agent)
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

// HandleGatewayMessageCreateForAgent advances watermarks and enqueues inbound messages for an agent.
func HandleGatewayMessageCreateForAgent(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.MessageCreate, agent string) {
	if m != nil && m.ChannelID != "" && m.ID != "" {
		AdvanceChannelWatermarkForAgent(agent, m.ChannelID, m.ID)
	}
	if isNilServer(server) || m == nil || m.Message == nil || m.Author == nil {
		return
	}
	botUserID := SessionBotUserID(s)
	if botUserID != "" && m.Author.ID == botUserID {
		return
	}
	EnqueueDiscordMessageForAgent(ctx, server, s, m.Message, botUserID, agent)
}

// EnqueueDiscordMessageForAgent prepares and publishes or dispatches an inbound Discord message for an agent.
func EnqueueDiscordMessageForAgent(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.Message, botUserID, agent string) {
	if isNilServer(server) || m == nil || m.Author == nil {
		return
	}
	if botUserID != "" && m.Author.ID == botUserID {
		return
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

	spanOpts := GatewaySpanOptions(m.ChannelID)
	if agent != "" {
		spanOpts = append(spanOpts, trace.WithAttributes(attribute.String("aerial.agent", agent)))
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

// HandleGatewayInteractionCreateForAgent enqueues application command interactions for an agent.
func HandleGatewayInteractionCreateForAgent(ctx context.Context, server IngressServer, i *discordgo.InteractionCreate, agent string) {
	if isNilServer(server) || i == nil || i.Interaction == nil || i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()

	spanOpts := GatewaySpanOptions(i.ChannelID)
	if agent != "" {
		spanOpts = append(spanOpts, trace.WithAttributes(attribute.String("aerial.agent", agent)))
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
