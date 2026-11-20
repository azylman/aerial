package widget

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

// Widget sink constants controlling when a buried widget is re-posted at the bottom of a thread.
const (
	WidgetSinkMessageThreshold = 4
	WidgetSinkQueryLimit       = 20
	WidgetSinkIdleThreshold    = 15 * time.Minute

	configCacheTTL         = 1 * time.Minute
	channelResolveCacheTTL = 5 * time.Minute
	MaxChunkActiveSteps    = 500
)

// Active step storage for live build chunks.
var (
	chunkActiveStepsMu sync.RWMutex
	chunkActiveSteps   = make(map[string]string)
	chunkActiveOrder   []string
)

func removeChunkFromOrderLocked(id string) {
	n := 0
	for _, chunkID := range chunkActiveOrder {
		if chunkID != id {
			chunkActiveOrder[n] = chunkID
			n++
		}
	}
	chunkActiveOrder = chunkActiveOrder[:n]
}

// SetChunkActiveStep sets the live active step description for a chunk.
func SetChunkActiveStep(chunkID, step string) {
	chunkActiveStepsMu.Lock()
	defer chunkActiveStepsMu.Unlock()
	clean := strings.TrimSpace(step)
	if clean == "" {
		delete(chunkActiveSteps, chunkID)
		removeChunkFromOrderLocked(chunkID)
		return
	}
	if _, exists := chunkActiveSteps[chunkID]; !exists {
		for len(chunkActiveSteps) >= MaxChunkActiveSteps {
			if len(chunkActiveOrder) > 0 {
				oldest := chunkActiveOrder[0]
				chunkActiveOrder = chunkActiveOrder[1:]
				delete(chunkActiveSteps, oldest)
			} else {
				for k := range chunkActiveSteps {
					delete(chunkActiveSteps, k)
					break
				}
			}
		}
		chunkActiveOrder = append(chunkActiveOrder, chunkID)
	}
	chunkActiveSteps[chunkID] = clean
}

// GetChunkActiveStep retrieves the live active step description for a chunk.
func GetChunkActiveStep(chunkID string) string {
	chunkActiveStepsMu.RLock()
	defer chunkActiveStepsMu.RUnlock()
	return chunkActiveSteps[chunkID]
}

// ClearChunkActiveStep clears the live active step description for a chunk.
func ClearChunkActiveStep(chunkID string) {
	chunkActiveStepsMu.Lock()
	defer chunkActiveStepsMu.Unlock()
	delete(chunkActiveSteps, chunkID)
	removeChunkFromOrderLocked(chunkID)
}

// DiscordWidgetSender defines the interface required by DeliverStatusWidget to send, edit, query, and delete Discord messages.
type DiscordWidgetSender interface {
	ChannelMessageSendComplex(channelID string, data *discordgo.MessageSend, options ...discordgo.RequestOption) (*discordgo.Message, error)
	ChannelMessageEditComplex(m *discordgo.MessageEdit, options ...discordgo.RequestOption) (*discordgo.Message, error)
	ChannelMessages(channelID string, limit int, beforeID, afterID, aroundID string, options ...discordgo.RequestOption) ([]*discordgo.Message, error)
	ChannelMessageDelete(channelID, messageID string, options ...discordgo.RequestOption) error
}

type cachedResolvedChannel struct {
	id        string
	expiresAt time.Time
}

type cachedChannelMeta struct {
	ch        *discordgo.Channel
	expiresAt time.Time
}

// DiscordWidgetDeliverer delivers Components V2 status widgets directly to Discord REST.
type DiscordWidgetDeliverer struct {
	session               *discordgo.Session
	sender                DiscordWidgetSender
	configPath            string
	nowFn                 func() time.Time
	channelPolicyResolver func(channelID string) (noBuildUpdates bool, systemChannel string)
	channelResolver       func(nameOrID string) (string, error)

	configMu         sync.RWMutex
	cachedConfig     *rawConfigFile
	cachedConfigPath string
	configReadAt     time.Time

	resolvedMu       sync.RWMutex
	resolvedChannels map[string]cachedResolvedChannel

	channelMetaMu    sync.RWMutex
	channelMetaCache map[string]cachedChannelMeta
}

// NewDiscordWidgetDeliverer constructs a DiscordWidgetDeliverer using the provided token or DISCORD_TOKEN env.
func NewDiscordWidgetDeliverer(token string, configPath ...string) (*DiscordWidgetDeliverer, error) {
	cleanToken := strings.TrimSpace(token)
	if cleanToken == "" {
		cleanToken = strings.TrimSpace(os.Getenv("DISCORD_TOKEN"))
	}
	cfgPath := "/share/aerial-config/config.yaml"
	if len(configPath) > 0 && strings.TrimSpace(configPath[0]) != "" {
		cfgPath = strings.TrimSpace(configPath[0])
	}

	var sess *discordgo.Session
	if cleanToken != "" {
		botToken := cleanToken
		if !strings.HasPrefix(botToken, "Bot ") {
			botToken = "Bot " + botToken
		}
		var err error
		sess, err = discordgo.New(botToken)
		if err != nil {
			return nil, fmt.Errorf("create discord session: %w", err)
		}
	}

	var sender DiscordWidgetSender
	if sess != nil {
		sender = sess
	}
	return &DiscordWidgetDeliverer{
		session:          sess,
		sender:           sender,
		configPath:       cfgPath,
		resolvedChannels: make(map[string]cachedResolvedChannel),
		channelMetaCache: make(map[string]cachedChannelMeta),
	}, nil
}

// shouldSinkWidget reports whether the status widget messageID is buried enough to re-post: either
// WidgetSinkMessageThreshold regular messages sit below it, or at least one does and the newest of
// them is at least WidgetSinkIdleThreshold older than now. Other Components V2 widgets never count.
func shouldSinkWidget(s DiscordWidgetSender, channelID, messageID string, now time.Time, opts ...discordgo.RequestOption) bool {
	if s == nil || strings.TrimSpace(channelID) == "" || strings.TrimSpace(messageID) == "" {
		return false
	}
	msgs, err := s.ChannelMessages(channelID, WidgetSinkQueryLimit, "", messageID, "", opts...)
	if err != nil {
		return false
	}
	nonWidgetCount := 0
	var newest time.Time
	for _, m := range msgs {
		if m == nil || m.ID == messageID || m.Flags&discordgo.MessageFlagsIsComponentsV2 != 0 {
			continue
		}
		nonWidgetCount++
		if nonWidgetCount >= WidgetSinkMessageThreshold {
			return true
		}
		if m.Timestamp.After(newest) {
			newest = m.Timestamp
		}
	}
	return !newest.IsZero() && now.Sub(newest) >= WidgetSinkIdleThreshold
}

const (
	statusCoding       = "coding"
	statusCIFailed     = "ci_failed"
	statusConflict     = "conflict"
	statusDeployFailed = "deploy_failed"
	statusError        = "error"
	statusClosed       = "closed"
	statusPaused       = "paused"
	statusStalled      = "stalled"
	statusPending      = "pending"
)

type channelConfigData struct {
	NoBuildUpdates bool `yaml:"no_build_updates"`
}

type rawConfigFile struct {
	SystemChannel string                       `yaml:"system_channel"`
	Channels      map[string]channelConfigData `yaml:"channels"`
}

func isNumericSnowflake(s string) bool {
	clean := strings.TrimSpace(s)
	if clean == "" {
		return false
	}
	for i := range len(clean) {
		if clean[i] < '0' || clean[i] > '9' {
			return false
		}
	}
	return true
}

func matchChannelPolicy(key string, channels map[string]channelConfigData) bool {
	clean := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(key), "#"))
	if clean == "" {
		return false
	}
	for k, v := range channels {
		if strings.ToLower(strings.TrimPrefix(strings.TrimSpace(k), "#")) == clean {
			return v.NoBuildUpdates
		}
	}
	return false
}

func (d *DiscordWidgetDeliverer) getCachedChannelMeta(channelID string, now time.Time) (*discordgo.Channel, bool) {
	d.channelMetaMu.RLock()
	defer d.channelMetaMu.RUnlock()
	if d.channelMetaCache == nil {
		return nil, false
	}
	entry, ok := d.channelMetaCache[channelID]
	if !ok || now.After(entry.expiresAt) {
		return nil, false
	}
	return entry.ch, true
}

func (d *DiscordWidgetDeliverer) setCachedChannelMeta(channelID string, ch *discordgo.Channel, now time.Time) {
	d.channelMetaMu.Lock()
	defer d.channelMetaMu.Unlock()
	if d.channelMetaCache == nil {
		d.channelMetaCache = make(map[string]cachedChannelMeta)
	}
	d.channelMetaCache[channelID] = cachedChannelMeta{
		ch:        ch,
		expiresAt: now.Add(channelResolveCacheTTL),
	}
}

func (d *DiscordWidgetDeliverer) lookupSessionChannel(channelID string) *discordgo.Channel {
	if d.session == nil {
		return nil
	}
	if d.session.State != nil {
		if ch, err := d.session.State.Channel(channelID); err == nil && ch != nil {
			return ch
		}
	}
	if ch, err := d.session.Channel(channelID); err == nil && ch != nil {
		return ch
	}
	return nil
}

func (d *DiscordWidgetDeliverer) findChannel(channelID string, now ...time.Time) *discordgo.Channel {
	currentTime := d.now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}
	if cachedCh, ok := d.getCachedChannelMeta(channelID, currentTime); ok {
		return cachedCh
	}
	ch := d.lookupSessionChannel(channelID)
	d.setCachedChannelMeta(channelID, ch, currentTime)
	return ch
}

func (d *DiscordWidgetDeliverer) checkNoBuildUpdates(channelID string, channels map[string]channelConfigData, now ...time.Time) bool {
	if matchChannelPolicy(channelID, channels) {
		return true
	}
	ch := d.findChannel(channelID, now...)
	if ch == nil {
		return false
	}
	if matchChannelPolicy(ch.Name, channels) || matchChannelPolicy(ch.ID, channels) {
		return true
	}
	if !ch.IsThread() || ch.ParentID == "" {
		return false
	}
	if matchChannelPolicy(ch.ParentID, channels) {
		return true
	}
	parentCh := d.findChannel(ch.ParentID, now...)
	if parentCh != nil && (matchChannelPolicy(parentCh.Name, channels) || matchChannelPolicy(parentCh.ID, channels)) {
		return true
	}
	return false
}

func (d *DiscordWidgetDeliverer) now() time.Time {
	if d != nil && d.nowFn != nil {
		return d.nowFn()
	}
	return time.Now()
}

func (d *DiscordWidgetDeliverer) getCachedChannelID(target string, now time.Time) (string, bool) {
	d.resolvedMu.RLock()
	defer d.resolvedMu.RUnlock()
	if d.resolvedChannels == nil {
		return "", false
	}
	entry, ok := d.resolvedChannels[target]
	if !ok || now.After(entry.expiresAt) {
		return "", false
	}
	return entry.id, true
}

func (d *DiscordWidgetDeliverer) setCachedChannelID(target, id string, now time.Time) {
	d.resolvedMu.Lock()
	defer d.resolvedMu.Unlock()
	if d.resolvedChannels == nil {
		d.resolvedChannels = make(map[string]cachedResolvedChannel)
	}
	d.resolvedChannels[target] = cachedResolvedChannel{
		id:        id,
		expiresAt: now.Add(channelResolveCacheTTL),
	}
}

func (d *DiscordWidgetDeliverer) resolveChannelID(targetNameOrID, fallback string, now ...time.Time) string {
	target := strings.TrimSpace(targetNameOrID)
	target = strings.TrimPrefix(target, "#")
	if target == "" {
		return fallback
	}
	if isNumericSnowflake(target) {
		return target
	}
	currentTime := d.now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}
	if cachedID, ok := d.getCachedChannelID(target, currentTime); ok {
		return cachedID
	}
	if d.channelResolver != nil {
		if id, err := d.channelResolver(target); err == nil && isNumericSnowflake(id) {
			d.setCachedChannelID(target, id, currentTime)
			return id
		}
		return fallback
	}
	if id := d.resolveGuildChannelID(target); id != "" {
		d.setCachedChannelID(target, id, currentTime)
		return id
	}
	return fallback
}

func (d *DiscordWidgetDeliverer) resolveGuildChannelID(target string) string {
	if d.session == nil {
		return ""
	}
	if d.session.State != nil {
		for _, g := range d.session.State.Guilds {
			for _, ch := range g.Channels {
				if strings.EqualFold(ch.Name, target) {
					return ch.ID
				}
			}
		}
	}
	userGuilds, err := d.session.UserGuilds(100, "", "", false)
	if err != nil {
		return ""
	}
	for _, g := range userGuilds {
		channels, err := d.session.GuildChannels(g.ID)
		if err != nil {
			continue
		}
		for _, ch := range channels {
			if strings.EqualFold(ch.Name, target) {
				return ch.ID
			}
		}
	}
	return ""
}

func (d *DiscordWidgetDeliverer) loadConfigFile(now time.Time) *rawConfigFile {
	d.configMu.Lock()
	defer d.configMu.Unlock()

	cfgPath := d.configPath
	if cfgPath == "" {
		cfgPath = "/share/aerial-config/config.yaml"
	}

	if d.cachedConfig != nil && now.Sub(d.configReadAt) < configCacheTTL && d.cachedConfigPath == cfgPath {
		return d.cachedConfig
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil
	}

	var cfg rawConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil
	}

	d.cachedConfig = &cfg
	d.cachedConfigPath = cfgPath
	d.configReadAt = now
	return d.cachedConfig
}

func (d *DiscordWidgetDeliverer) redirectChannelIfNoBuildUpdates(channelID string, now ...time.Time) string {
	if strings.TrimSpace(channelID) == "" {
		return channelID
	}
	currentTime := d.now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}
	if d.channelPolicyResolver != nil {
		noBuild, sysChan := d.channelPolicyResolver(channelID)
		if !noBuild || sysChan == "" {
			return channelID
		}
		return d.resolveChannelID(sysChan, channelID, currentTime)
	}

	cfg := d.loadConfigFile(currentTime)
	if cfg == nil {
		return channelID
	}

	systemChannel := strings.TrimSpace(cfg.SystemChannel)
	if systemChannel == "" {
		return channelID
	}

	if !d.checkNoBuildUpdates(channelID, cfg.Channels, currentTime) {
		return channelID
	}

	return d.resolveChannelID(systemChannel, channelID, currentTime)
}

func isNilSender(s DiscordWidgetSender) bool {
	if s == nil {
		return true
	}
	if sess, ok := s.(*discordgo.Session); ok && sess == nil {
		return true
	}
	return false
}

// DeliverStatusWidget delivers or updates a status widget via Discord Components V2.
func (d *DiscordWidgetDeliverer) DeliverStatusWidget(ctx context.Context, req ericlient.StatusWidgetRequest) (string, error) {
	if d == nil {
		return "", errors.New("discord widget deliverer is nil")
	}
	sender := d.sender
	if isNilSender(sender) && !isNilSender(d.session) {
		sender = d.session
	}
	if isNilSender(sender) {
		return "", errors.New("discord widget sender is nil")
	}

	now := d.now()
	req.ChannelID = d.redirectChannelIfNoBuildUpdates(req.ChannelID, now)

	var opts []discordgo.RequestOption
	if ctx != nil {
		opts = append(opts, discordgo.WithContext(ctx))
	}

	return DeliverStatusWidget(sender, req, now, opts...)
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if restErr, ok := errors.AsType[*discordgo.RESTError](err); ok {
		if restErr.Response != nil && restErr.Response.StatusCode == http.StatusNotFound {
			return true
		}
		if restErr.Message != nil && restErr.Message.Code == 10008 {
			return true
		}
	}
	return false
}

func newWidgetMessageSend(components []discordgo.MessageComponent) *discordgo.MessageSend {
	return &discordgo.MessageSend{
		Flags:           discordgo.MessageFlagsIsComponentsV2 | discordgo.MessageFlagsSuppressNotifications,
		Components:      components,
		AllowedMentions: &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}},
	}
}

func trySinkStatusWidget(s DiscordWidgetSender, channelID, trimmedMsgID string, components []discordgo.MessageComponent, opts ...discordgo.RequestOption) (string, bool) {
	send := newWidgetMessageSend(components)
	created, sendErr := s.ChannelMessageSendComplex(channelID, send, opts...)
	if sendErr != nil || created == nil {
		return "", false
	}
	if delErr := s.ChannelMessageDelete(channelID, trimmedMsgID, opts...); delErr != nil && !isNotFoundError(delErr) {
		log.Printf("[Widget] Warning: failed to delete old status widget message %s: %v", trimmedMsgID, delErr)
	}
	return created.ID, true
}

func updateExistingStatusWidget(s DiscordWidgetSender, req ericlient.StatusWidgetRequest, trimmedMsgID string, components []discordgo.MessageComponent, now time.Time, opts ...discordgo.RequestOption) (string, error) {
	if !req.SkipSink && shouldSinkWidget(s, req.ChannelID, trimmedMsgID, now, opts...) {
		if id, ok := trySinkStatusWidget(s, req.ChannelID, trimmedMsgID, components, opts...); ok {
			return id, nil
		}
	}

	edit := &discordgo.MessageEdit{
		Channel:         req.ChannelID,
		ID:              trimmedMsgID,
		Content:         nil,
		Embeds:          nil,
		Flags:           discordgo.MessageFlagsIsComponentsV2,
		Components:      &components,
		AllowedMentions: &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}},
	}
	msg, editErr := s.ChannelMessageEditComplex(edit, opts...)
	if editErr == nil && msg != nil {
		return msg.ID, nil
	}

	if isNotFoundError(editErr) {
		send := newWidgetMessageSend(components)
		created, sendErr := s.ChannelMessageSendComplex(req.ChannelID, send, opts...)
		if sendErr != nil {
			return "", fmt.Errorf("failed to send new status widget message after 404 edit: %w", sendErr)
		}
		if created == nil {
			return "", errors.New("failed to send new status widget message after 404 edit: received nil message")
		}
		return created.ID, nil
	}

	return "", fmt.Errorf("failed to edit status widget message %s: %w", trimmedMsgID, editErr)
}

// DeliverStatusWidget delivers or updates a status widget via Discord Components V2.
func DeliverStatusWidget(s DiscordWidgetSender, req ericlient.StatusWidgetRequest, now time.Time, opts ...discordgo.RequestOption) (string, error) {
	if isNilSender(s) {
		return "", errors.New("discord widget sender is nil")
	}
	if strings.TrimSpace(req.ChannelID) == "" {
		return "", errors.New("channel_id is required")
	}

	components, err := BuildStatusWidgetComponents(req, now)
	if err != nil {
		return "", fmt.Errorf("failed to build status widget components: %w", err)
	}

	trimmedMsgID := strings.TrimSpace(req.MessageID)
	if trimmedMsgID != "" {
		return updateExistingStatusWidget(s, req, trimmedMsgID, components, now, opts...)
	}

	send := newWidgetMessageSend(components)
	created, sendErr := s.ChannelMessageSendComplex(req.ChannelID, send, opts...)
	if sendErr != nil {
		return "", fmt.Errorf("failed to send status widget message: %w", sendErr)
	}
	if created == nil {
		return "", errors.New("failed to send status widget message: received nil message")
	}
	return created.ID, nil
}

// AccentColor returns the hex color integer for the given status string.
func AccentColor(status string) int {
	norm := strings.ToLower(strings.TrimSpace(status))
	switch norm {
	case statusCompleted, statusDeployed, dispatchStatusSuccess, "green":
		return 0x10B981 // emerald
	case statusFailed, statusCIFailed, statusDeployFailed, statusConflict, statusClosed, statusError, "cancelled_error":
		return 0xEF4444 // crimson
	case statusPaused, statusStalled, statusCancelled:
		return 0xF59E0B // amber
	default:
		return 0xA855F7 // purple
	}
}

func statusBadge(status string) string {
	norm := strings.ToLower(strings.TrimSpace(status))
	switch norm {
	case statusRunning, statusCoding:
		return "🔨"
	case statusPROpened, statusOpen:
		return "👀"
	case statusMerged:
		return "🔀"
	case statusSynced:
		return "🔄"
	case statusDeploying:
		return "🚀"
	case statusDeployed, statusCompleted, dispatchStatusSuccess:
		return "✅"
	case statusFailed, statusCIFailed, statusDeployFailed, statusConflict, statusError:
		return "❌"
	default:
		return "⏳"
	}
}

// NormalizeRepo returns the full repository path (e.g. "azylman/aerial") and short name ("aerial").
func NormalizeRepo(repo string) (fullRepo string, shortRepo string) {
	trimmed := strings.TrimSpace(repo)
	if trimmed == "" {
		return "", ""
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 1 {
		return "azylman/" + parts[0], parts[0]
	}
	return trimmed, parts[len(parts)-1]
}

// FormatChunkDuration formats a chunk execution duration into a concise human-readable string.
func FormatChunkDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	totalSecs := int(d.Round(time.Second).Seconds())
	if totalSecs < 60 {
		return fmt.Sprintf("%ds", totalSecs)
	}
	if totalSecs < 3600 {
		mins := totalSecs / 60
		secs := totalSecs % 60
		if secs == 0 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	hours := totalSecs / 3600
	mins := (totalSecs % 3600) / 60
	if mins == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh %dm", hours, mins)
}

func formatChunkRuntimeSuffix(c ericlient.StatusWidgetChunk) string {
	if c.StartedAt.IsZero() {
		return ""
	}
	if isChunkTerminal(c.Status) {
		end := c.CompletedAt
		if end.IsZero() {
			end = c.UpdatedAt
		}
		if end.IsZero() {
			return ""
		}
		return fmt.Sprintf(" *(%s)*", FormatChunkDuration(end.Sub(c.StartedAt)))
	}
	norm := strings.ToLower(strings.TrimSpace(c.Status))
	switch norm {
	case statusRunning, statusCoding, statusPROpened, statusOpen, statusDeploying:
		return fmt.Sprintf(" *(started <t:%d:R>)*", c.StartedAt.Unix())
	default:
		return ""
	}
}

func buildWidgetHeaderText(req ericlient.StatusWidgetRequest) string {
	isBuild := strings.HasPrefix(req.ID, "build-") || len(req.Chunks) > 1 || req.Tier > 0
	if isBuild {
		if trimmedTitle := strings.TrimSpace(req.Title); trimmedTitle != "" {
			return "### 🛠️ " + trimmedTitle
		}
		return "### 🛠️ Vanadis Build"
	}

	prNum := 0
	shortRepo := ""
	if len(req.Chunks) == 1 {
		prNum = req.Chunks[0].PRNumber
		_, shortRepo = NormalizeRepo(req.Chunks[0].Repo)
	}
	repoTag := shortRepo
	if repoTag == "" {
		repoTag = "aerial"
	}
	if prNum > 0 {
		return fmt.Sprintf("### 🛠️ Pull Request #%d · `%s`\n%s", prNum, repoTag, strings.TrimSpace(req.Title))
	}
	return fmt.Sprintf("### 🛠️ Pull Request · `%s`\n%s", repoTag, strings.TrimSpace(req.Title))
}

func collapseCompletedWidgetChunks(chunks []ericlient.StatusWidgetChunk) ([]ericlient.StatusWidgetChunk, string) {
	if len(chunks) <= 10 {
		return chunks, ""
	}

	var activeChunks []ericlient.StatusWidgetChunk
	var collapsedMerged []ericlient.StatusWidgetChunk

	for i, c := range chunks {
		norm := strings.ToLower(strings.TrimSpace(c.Status))
		isCompleted := (norm == statusMerged || norm == statusDeployed || norm == statusCompleted || norm == dispatchStatusSuccess)
		if isCompleted && i < len(chunks)-5 {
			collapsedMerged = append(collapsedMerged, c)
		} else {
			activeChunks = append(activeChunks, c)
		}
	}

	if len(collapsedMerged) > 0 {
		summaryText := fmt.Sprintf("✅ ***%d/%d** earlier chunks completed and merged*", len(collapsedMerged), len(chunks))
		return activeChunks, summaryText
	}
	return chunks, ""
}

func formatChunkActiveStepLines(activeStep string) string {
	trimmed := strings.TrimSpace(activeStep)
	if trimmed == "" {
		return ""
	}
	lines := strings.Split(trimmed, "\n")
	var b strings.Builder
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		for {
			if after, ok := strings.CutPrefix(line, "-#"); ok {
				line = strings.TrimSpace(after)
				continue
			}
			if after, ok := strings.CutPrefix(line, "↳"); ok {
				line = strings.TrimSpace(after)
				continue
			}
			break
		}
		if line == "" {
			continue
		}
		b.WriteString("\n-# ↳ ")
		b.WriteString(line)
	}
	return b.String()
}

func isChunkTerminal(status string) bool {
	norm := strings.ToLower(strings.TrimSpace(status))
	switch norm {
	case statusDeployed, statusCompleted, dispatchStatusSuccess, statusFailed, statusCIFailed, statusDeployFailed, statusConflict, statusClosed, statusError, statusCancelled, statusMerged, statusSynced:
		return true
	default:
		return false
	}
}

func buildWidgetChunkComponent(c ericlient.StatusWidgetChunk) discordgo.MessageComponent {
	badge := statusBadge(c.Status)
	fullRepo, shortRepo := NormalizeRepo(c.Repo)
	repoTag := shortRepo
	switch {
	case c.Mode == chunkModeAction:
		repoTag = chunkModeAction
	case repoTag == "":
		repoTag = "aerial"
	}

	rowText := fmt.Sprintf("%s **`%s`**: %s%s", badge, repoTag, strings.TrimSpace(c.Title), formatChunkRuntimeSuffix(c))
	step := c.ActiveStep
	if isChunkTerminal(c.Status) {
		ClearChunkActiveStep(c.ID)
		step = ""
	} else if step == "" {
		step = GetChunkActiveStep(c.ID)
	}
	rowText += formatChunkActiveStepLines(step)

	if c.PRNumber > 0 && fullRepo != "" {
		prURL := fmt.Sprintf("https://github.com/%s/pull/%d", fullRepo, c.PRNumber)
		return &discordgo.Section{
			Components: []discordgo.MessageComponent{
				&discordgo.TextDisplay{
					Content: rowText,
				},
			},
			Accessory: &discordgo.Button{
				Style: discordgo.LinkButton,
				Label: fmt.Sprintf("PR #%d ↗", c.PRNumber),
				URL:   prURL,
			},
		}
	}
	return &discordgo.TextDisplay{
		Content: rowText,
	}
}

func resolveWidgetStartedAt(req ericlient.StatusWidgetRequest) time.Time {
	started := req.StartedAt
	for _, c := range req.Chunks {
		if !c.StartedAt.IsZero() && (started.IsZero() || c.StartedAt.Before(started)) {
			started = c.StartedAt
		}
	}
	return started
}

func isWidgetCompleted(req ericlient.StatusWidgetRequest) bool {
	status := strings.ToLower(strings.TrimSpace(req.Status))
	switch status {
	case statusRunning, statusCoding, statusPROpened, statusOpen, statusDeploying, statusPending, statusPaused, statusStalled, "":
		return false
	}

	for _, c := range req.Chunks {
		if !isChunkTerminal(c.Status) {
			return false
		}
	}
	return true
}

func resolveWidgetCompletedAt(req ericlient.StatusWidgetRequest, now time.Time) time.Time {
	var latest time.Time
	for _, c := range req.Chunks {
		end := c.CompletedAt
		if end.IsZero() {
			end = c.UpdatedAt
		}
		if !end.IsZero() && end.After(latest) {
			latest = end
		}
	}
	if !latest.IsZero() {
		return latest
	}
	return now
}

func formatWidgetFooterText(req ericlient.StatusWidgetRequest, now time.Time) string {
	if now.IsZero() {
		now = time.Now()
	}
	started := resolveWidgetStartedAt(req)
	if !started.IsZero() {
		if isWidgetCompleted(req) {
			end := resolveWidgetCompletedAt(req, now)
			return fmt.Sprintf("*Started <t:%d:R> · Duration %s*", started.Unix(), FormatChunkDuration(end.Sub(started)))
		}
		return fmt.Sprintf("*Started <t:%d:R> · Updated <t:%d:R>*", started.Unix(), now.Unix())
	}
	return fmt.Sprintf("*Updated <t:%d:R>*", now.Unix())
}

// BuildStatusWidgetComponents constructs a Components V2 Container message component.
func BuildStatusWidgetComponents(req ericlient.StatusWidgetRequest, now time.Time) ([]discordgo.MessageComponent, error) {
	// Evict stale active step entries for terminal chunks or completed widgets.
	completed := isWidgetCompleted(req)
	for _, c := range req.Chunks {
		if completed || isChunkTerminal(c.Status) {
			ClearChunkActiveStep(c.ID)
		}
	}

	color := AccentColor(req.Status)
	var containerComponents []discordgo.MessageComponent

	// Header TextDisplay
	headerText := buildWidgetHeaderText(req)
	containerComponents = append(containerComponents, &discordgo.TextDisplay{
		Content: strings.TrimSpace(headerText),
	})

	// Optional Banner
	if strings.TrimSpace(req.Banner) != "" {
		containerComponents = append(containerComponents, &discordgo.TextDisplay{
			Content: strings.TrimSpace(req.Banner),
		})
	}

	// 40-Component Ceiling Guard
	// If len(req.Chunks) > 10, collapse older completed/merged chunks into a single summary TextDisplay.
	chunks, summaryText := collapseCompletedWidgetChunks(req.Chunks)
	if summaryText != "" {
		containerComponents = append(containerComponents, &discordgo.TextDisplay{
			Content: summaryText,
		})
	}

	// Chunk rows
	for _, c := range chunks {
		containerComponents = append(containerComponents, buildWidgetChunkComponent(c))
	}

	// Footer TextDisplay
	footerText := formatWidgetFooterText(req, now)
	containerComponents = append(containerComponents, &discordgo.TextDisplay{
		Content: footerText,
	})

	container := &discordgo.Container{
		AccentColor: &color,
		Components:  containerComponents,
	}

	return []discordgo.MessageComponent{container}, nil
}
