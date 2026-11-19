package discord

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"reflect"
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

// GetChannelWatermark returns the highest message snowflake processed for a channel.
func GetChannelWatermark(channelID string) string {
	channelWatermarksMu.RLock()
	defer channelWatermarksMu.RUnlock()
	return channelWatermarks[channelID]
}

// AdvanceChannelWatermark advances the tracked watermark for a channel if messageID is newer.
func AdvanceChannelWatermark(channelID, messageID string) {
	if channelID == "" || messageID == "" {
		return
	}
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	curr := channelWatermarks[channelID]
	if CompareDiscordSnowflakes(messageID, curr) > 0 {
		channelWatermarks[channelID] = messageID
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

// StartGateway initializes and starts the Discord Gateway WebSocket connection.
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
	return func(s *discordgo.Session, m *discordgo.MessageCreate) {
		HandleGatewayMessageCreate(ctx, server, s, m)
	}
}

// NewDiscordGatewayInteractionCreateHandler creates a discordgo InteractionCreate handler.
func NewDiscordGatewayInteractionCreateHandler(ctx context.Context, server IngressServer) func(s *discordgo.Session, i *discordgo.InteractionCreate) {
	return func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		HandleGatewayInteractionCreate(ctx, server, i)
	}
}

// NewDiscordGatewayReadyHandler creates a discordgo Ready handler.
func NewDiscordGatewayReadyHandler(ctx context.Context, server IngressServer, wg *sync.WaitGroup) func(s *discordgo.Session, r *discordgo.Ready) {
	return func(s *discordgo.Session, r *discordgo.Ready) {
		HandleGatewayReady(ctx, server, s, r, wg)
	}
}

// HandleGatewayReady updates session state and triggers a background catch-up sweep.
func HandleGatewayReady(ctx context.Context, server IngressServer, s *discordgo.Session, r *discordgo.Ready, wg *sync.WaitGroup) {
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
			RunCatchUpSweep(ctx, s, server, time.Now().UTC())
		})
		return
	}
	go RunCatchUpSweep(ctx, s, server, time.Now().UTC())
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

// HandleGatewayMessageCreate advances watermarks and enqueues inbound messages.
func HandleGatewayMessageCreate(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.MessageCreate) {
	if m != nil && m.ChannelID != "" && m.ID != "" {
		AdvanceChannelWatermark(m.ChannelID, m.ID)
	}
	if isNilServer(server) || m == nil || m.Message == nil || m.Author == nil {
		return
	}
	botUserID := SessionBotUserID(s)
	if botUserID != "" && m.Author.ID == botUserID {
		return
	}
	EnqueueDiscordMessage(ctx, server, s, m.Message, botUserID)
}

// EnqueueDiscordMessage prepares and publishes or dispatches an inbound Discord message.
func EnqueueDiscordMessage(ctx context.Context, server IngressServer, s *discordgo.Session, m *discordgo.Message, botUserID string) {
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
	enqueueCtx, span := tracing.Tracer().Start(enqueueCtx, "discord.gateway MESSAGE_CREATE", GatewaySpanOptions(m.ChannelID)...)
	defer span.End()

	args := model.DiscordIngressArgs{
		MessageID: m.ID,
		BotUserID: botUserID,
		RoleNames: roleNames,
		Message:   m,
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

// HandleGatewayInteractionCreate enqueues application command interactions.
func HandleGatewayInteractionCreate(ctx context.Context, server IngressServer, i *discordgo.InteractionCreate) {
	if isNilServer(server) || i == nil || i.Interaction == nil || i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	enqueueCtx, span := tracing.Tracer().Start(enqueueCtx, "discord.gateway INTERACTION_CREATE", GatewaySpanOptions(i.ChannelID)...)
	defer span.End()

	args := model.DiscordInteractionArgs{
		InteractionID: i.ID,
		Interaction:   i.Interaction,
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
