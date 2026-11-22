package widget

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/discordutil"
	"github.com/azylman/aerial/pkg/githubapi"
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

type cachedConfigEntry struct {
	cfg    *rawConfigFile
	readAt time.Time
}

// DiscordWidgetDeliverer delivers Components V2 status widgets directly to Discord REST.
type DiscordWidgetDeliverer struct {
	session               *discordgo.Session
	agentSenders          map[string]DiscordWidgetSender
	configPath            string
	gitHubOwner           string
	nowFn                 func() time.Time
	channelPolicyResolver func(channelID string) (noBuildUpdates bool, systemChannel string)
	channelResolver       func(nameOrID string) (string, error)

	configMu      sync.RWMutex
	cachedConfigs map[string]cachedConfigEntry

	resolvedMu       sync.RWMutex
	resolvedChannels map[string]cachedResolvedChannel

	channelMetaMu    sync.RWMutex
	channelMetaCache map[string]cachedChannelMeta

	channelLocks       sync.Map
	replacedMessageIDs sync.Map
	buildMessageIDs    sync.Map
}

// normalizeRunnerAgent lowercases and trims a runner's AGENT_NAME, mapping "" to the default agent.
func normalizeRunnerAgent(agent string) string {
	clean := strings.ToLower(strings.TrimSpace(agent))
	if clean == "" {
		return defaultAgent
	}
	return clean
}

// discoverAgentTokens loads every DISCORD_TOKEN_<AGENT> entry from environ, then keys the plain
// DISCORD_TOKEN (or DISCORD_BOT_TOKEN) under the runner's own normalized agent unless an explicit
// entry exists. environ uses os.Environ's KEY=VALUE form; the caller owns reading the process env.
func discoverAgentTokens(runnerAgent string, environ []string) map[string]string {
	tokens := discordutil.AgentTokensFromEnv(environ)
	owner := normalizeRunnerAgent(runnerAgent)
	if _, ok := tokens[owner]; ok {
		return tokens
	}
	plain := make(map[string]string)
	for _, env := range environ {
		key, rawVal, ok := strings.Cut(env, "=")
		if ok && (key == "DISCORD_TOKEN" || key == "DISCORD_BOT_TOKEN") {
			if val := strings.TrimSpace(rawVal); val != "" {
				plain[key] = val
			}
		}
	}
	if t := plain["DISCORD_TOKEN"]; t != "" {
		tokens[owner] = t
	} else if t := plain["DISCORD_BOT_TOKEN"]; t != "" {
		tokens[owner] = t
	}
	return tokens
}

// NewDiscordWidgetDeliverer constructs a DiscordWidgetDeliverer for the runner agent agentName.
// The explicit token (or DISCORD_TOKEN in environ) is keyed under the normalized agentName and
// becomes the deliverer's default session; DISCORD_TOKEN_<AGENT> entries in environ stay available
// per agent. configPath is the channel-policy config.yaml; empty disables channel policy lookups.
// gitHubOwner qualifies bare chunk repos in PR links.
func NewDiscordWidgetDeliverer(agentName, token, gitHubOwner string, environ []string, configPath string) (*DiscordWidgetDeliverer, error) {
	owner := normalizeRunnerAgent(agentName)
	tokens := discoverAgentTokens(owner, environ)
	cleanToken := strings.TrimSpace(token)
	if cleanToken != "" {
		tokens[owner] = cleanToken
	}
	cfgPath := strings.TrimSpace(configPath)

	agentSenders := make(map[string]DiscordWidgetSender)
	var defaultSess *discordgo.Session
	for agent, tok := range tokens {
		botToken := tok
		if !strings.HasPrefix(botToken, "Bot ") {
			botToken = "Bot " + botToken
		}
		sess, err := discordgo.New(botToken)
		if err != nil {
			continue
		}
		agentSenders[agent] = sess
		if agent == owner {
			defaultSess = sess
		}
	}

	return &DiscordWidgetDeliverer{
		session:          defaultSess,
		agentSenders:     agentSenders,
		configPath:       cfgPath,
		cachedConfigs:    make(map[string]cachedConfigEntry),
		gitHubOwner:      gitHubOwner,
		resolvedChannels: make(map[string]cachedResolvedChannel),
		channelMetaCache: make(map[string]cachedChannelMeta),
	}, nil
}

// configPathForAgent resolves the config path for the requested agent:
// empty or default agent ("aerial") returns d.configPath; other agents probe
// services/<agent>/config.yaml next to configPath, falling back to d.configPath.
func (d *DiscordWidgetDeliverer) configPathForAgent(agent string) string {
	if d == nil || d.configPath == "" {
		return ""
	}
	cleanAgent := normalizeRunnerAgent(agent)
	if cleanAgent == defaultAgent {
		return d.configPath
	}
	agentCfgPath := filepath.Join(filepath.Dir(d.configPath), "services", cleanAgent, "config.yaml")
	info, err := os.Stat(agentCfgPath)
	if err == nil && !info.IsDir() && info.Size() > 0 {
		return agentCfgPath
	}
	return d.configPath
}

// sessionForAgent returns the *discordgo.Session for the requested agent from
// agentSenders if present, falling back to d.session.
func (d *DiscordWidgetDeliverer) sessionForAgent(agent string) *discordgo.Session {
	if d == nil {
		return nil
	}
	clean := normalizeRunnerAgent(agent)
	d.configMu.RLock()
	s, ok := d.agentSenders[clean]
	d.configMu.RUnlock()
	if ok && !isNilSender(s) {
		if sess, ok := s.(*discordgo.Session); ok && sess != nil {
			return sess
		}
	}
	return d.session
}

// ErrNoSenderForAgent means no Discord sender is configured for the requested agent. Delivery never
// falls back to another agent's bot; callers surface it so the Restate step retries loudly.
var ErrNoSenderForAgent = errors.New("no discord sender configured for agent")

// senderForAgent returns the sender keyed under agent ("" is the default agent), or
// ErrNoSenderForAgent. It never substitutes another agent's sender.
func (d *DiscordWidgetDeliverer) senderForAgent(agent string) (DiscordWidgetSender, error) {
	clean := strings.ToLower(strings.TrimSpace(agent))
	if clean == "" {
		clean = defaultAgent
	}
	if d == nil {
		return nil, fmt.Errorf("%w %q", ErrNoSenderForAgent, clean)
	}
	d.configMu.RLock()
	s, ok := d.agentSenders[clean]
	d.configMu.RUnlock()
	if !ok || isNilSender(s) {
		return nil, fmt.Errorf("%w %q", ErrNoSenderForAgent, clean)
	}
	return s, nil
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

func (d *DiscordWidgetDeliverer) lookupSessionChannel(agent, channelID string) *discordgo.Channel {
	sess := d.sessionForAgent(agent)
	if sess == nil {
		return nil
	}
	if sess.State != nil {
		if ch, err := sess.State.Channel(channelID); err == nil && ch != nil {
			return ch
		}
	}
	if ch, err := sess.Channel(channelID); err == nil && ch != nil {
		return ch
	}
	return nil
}

func (d *DiscordWidgetDeliverer) findChannel(agent, channelID string, now ...time.Time) *discordgo.Channel {
	currentTime := d.now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}
	if cachedCh, ok := d.getCachedChannelMeta(channelID, currentTime); ok {
		return cachedCh
	}
	ch := d.lookupSessionChannel(agent, channelID)
	d.setCachedChannelMeta(channelID, ch, currentTime)
	return ch
}

func (d *DiscordWidgetDeliverer) checkNoBuildUpdates(agent, channelID string, channels map[string]channelConfigData, now ...time.Time) bool {
	if matchChannelPolicy(channelID, channels) {
		return true
	}
	ch := d.findChannel(agent, channelID, now...)
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
	parentCh := d.findChannel(agent, ch.ParentID, now...)
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

func (d *DiscordWidgetDeliverer) resolveChannelID(agent, targetNameOrID, fallback string, now ...time.Time) string {
	target := strings.TrimSpace(targetNameOrID)
	target = strings.TrimPrefix(target, "#")
	if target == "" {
		return fallback
	}
	if discordutil.IsSnowflake(strings.TrimSpace(target)) {
		return target
	}
	currentTime := d.now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}
	cacheKey := normalizeRunnerAgent(agent) + ":" + target
	if cachedID, ok := d.getCachedChannelID(cacheKey, currentTime); ok {
		return cachedID
	}
	if d.channelResolver != nil {
		if id, err := d.channelResolver(target); err == nil && discordutil.IsSnowflake(strings.TrimSpace(id)) {
			d.setCachedChannelID(cacheKey, id, currentTime)
			return id
		}
		return fallback
	}
	if id := d.resolveGuildChannelID(agent, target); id != "" {
		d.setCachedChannelID(cacheKey, id, currentTime)
		return id
	}
	return fallback
}

func (d *DiscordWidgetDeliverer) resolveGuildChannelID(agent, target string) string {
	sess := d.sessionForAgent(agent)
	if sess == nil {
		return ""
	}
	if sess.State != nil {
		for _, g := range sess.State.Guilds {
			for _, ch := range g.Channels {
				if strings.EqualFold(ch.Name, target) {
					return ch.ID
				}
			}
		}
	}
	userGuilds, err := sess.UserGuilds(100, "", "", false)
	if err != nil {
		return ""
	}
	for _, g := range userGuilds {
		channels, err := sess.GuildChannels(g.ID)
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

func (d *DiscordWidgetDeliverer) loadConfigFile(cfgPath string, now time.Time) *rawConfigFile {
	d.configMu.Lock()
	defer d.configMu.Unlock()

	if cfgPath == "" {
		return nil
	}

	if d.cachedConfigs == nil {
		d.cachedConfigs = make(map[string]cachedConfigEntry)
	}

	if entry, ok := d.cachedConfigs[cfgPath]; ok && now.Sub(entry.readAt) < configCacheTTL && entry.cfg != nil {
		return entry.cfg
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil
	}

	var cfg rawConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil
	}

	d.cachedConfigs[cfgPath] = cachedConfigEntry{
		cfg:    &cfg,
		readAt: now,
	}
	return &cfg
}

func (d *DiscordWidgetDeliverer) redirectChannelIfNoBuildUpdates(agent, channelID string, now ...time.Time) string {
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
		return d.resolveChannelID(agent, sysChan, channelID, currentTime)
	}

	cfgPath := d.configPathForAgent(agent)
	cfg := d.loadConfigFile(cfgPath, currentTime)
	if cfg == nil {
		return channelID
	}

	systemChannel := strings.TrimSpace(cfg.SystemChannel)
	if systemChannel == "" {
		return channelID
	}

	if !d.checkNoBuildUpdates(agent, channelID, cfg.Channels, currentTime) {
		return channelID
	}

	return d.resolveChannelID(agent, systemChannel, channelID, currentTime)
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

func (d *DiscordWidgetDeliverer) resolveLatestMessageID(buildID, msgID string) string {
	if d == nil {
		return msgID
	}
	curr := strings.TrimSpace(msgID)
	if curr != "" {
		visited := make(map[string]bool)
		for {
			val, ok := d.replacedMessageIDs.Load(curr)
			if !ok {
				break
			}
			next, ok := val.(string)
			if !ok {
				break
			}
			next = strings.TrimSpace(next)
			if next == "" || next == curr || visited[next] {
				break
			}
			visited[curr] = true
			curr = next
		}
		if curr != strings.TrimSpace(msgID) {
			return curr
		}
	}
	cleanBuildID := strings.TrimSpace(buildID)
	if cleanBuildID == "" {
		return curr
	}
	val, ok := d.buildMessageIDs.Load(cleanBuildID)
	if !ok {
		return curr
	}
	latest, ok := val.(string)
	if ok && latest != "" && curr == "" {
		return latest
	}
	return curr
}

func (d *DiscordWidgetDeliverer) recordMessageIDAdvance(buildID, oldMsgID, newMsgID string) {
	if d == nil {
		return
	}
	cleanNew := strings.TrimSpace(newMsgID)
	if cleanNew == "" {
		return
	}
	cleanOld := strings.TrimSpace(oldMsgID)
	if cleanOld != "" && cleanOld != cleanNew {
		d.replacedMessageIDs.Store(cleanOld, cleanNew)
	}
	cleanBuildID := strings.TrimSpace(buildID)
	if cleanBuildID != "" {
		d.buildMessageIDs.Store(cleanBuildID, cleanNew)
	}
}

// DeliverStatusWidget delivers or updates a status widget via Discord Components V2.
func (d *DiscordWidgetDeliverer) DeliverStatusWidget(ctx context.Context, req ericlient.StatusWidgetRequest) (string, error) {
	if d == nil {
		return "", errors.New("discord widget deliverer is nil")
	}
	sender, err := d.senderForAgent(req.Agent)
	if err != nil {
		return "", err
	}

	now := d.now()
	req.ChannelID = d.redirectChannelIfNoBuildUpdates(req.Agent, req.ChannelID, now)

	var opts []discordgo.RequestOption
	if ctx != nil {
		opts = append(opts, discordgo.WithContext(ctx))
	}

	lockVal, _ := d.channelLocks.LoadOrStore(req.ChannelID, &sync.Mutex{})
	if chLock, ok := lockVal.(*sync.Mutex); ok && chLock != nil {
		chLock.Lock()
		defer chLock.Unlock()
	}

	origMsgID := strings.TrimSpace(req.MessageID)
	if latestMsgID := d.resolveLatestMessageID(req.ID, origMsgID); latestMsgID != "" && latestMsgID != origMsgID {
		req.MessageID = latestMsgID
	}

	newID, err := DeliverStatusWidget(sender, d.gitHubOwner, req, now, opts...)
	if err == nil && newID != "" && (newID != origMsgID || newID != req.MessageID) {
		d.recordMessageIDAdvance(req.ID, origMsgID, newID)
		if req.MessageID != origMsgID {
			d.recordMessageIDAdvance(req.ID, req.MessageID, newID)
		}
	}
	return newID, err
}

// DeleteStatusWidget removes a status widget message with agent's own sender,
// resolving the channel the same way DeliverStatusWidget does. Deletion is
// best effort: a missing message is a success and Discord failures are only
// logged; only a missing sender for agent is returned.
func (d *DiscordWidgetDeliverer) DeleteStatusWidget(ctx context.Context, agent, channelID, messageID string) error {
	if d == nil {
		return errors.New("discord widget deliverer is nil")
	}
	sender, err := d.senderForAgent(agent)
	if err != nil {
		return err
	}
	cleanChannelID, cleanMessageID := strings.TrimSpace(channelID), strings.TrimSpace(messageID)
	if cleanChannelID == "" || cleanMessageID == "" {
		return nil
	}
	targetChannelID := d.redirectChannelIfNoBuildUpdates(agent, cleanChannelID, d.now())
	var opts []discordgo.RequestOption
	if ctx != nil {
		opts = append(opts, discordgo.WithContext(ctx))
	}
	deleteOldWidgetBestEffort(sender, targetChannelID, cleanMessageID, opts...)
	return nil
}

// DeliverDirectMessageWithAgent delivers a direct plain-text channel alert message using the specified agent sender.
func (d *DiscordWidgetDeliverer) DeliverDirectMessageWithAgent(ctx context.Context, agent, channelID, content string) error {
	if d == nil {
		return errors.New("discord widget deliverer is nil")
	}
	sender, err := d.senderForAgent(agent)
	if err != nil {
		return err
	}

	cleanChannelID := strings.TrimSpace(channelID)
	cleanContent := strings.TrimSpace(content)
	if cleanChannelID == "" || cleanContent == "" {
		return nil
	}

	now := d.now()
	targetChannelID := d.resolveChannelID(agent, d.redirectChannelIfNoBuildUpdates(agent, cleanChannelID, now), cleanChannelID, now)

	var opts []discordgo.RequestOption
	if ctx != nil {
		opts = append(opts, discordgo.WithContext(ctx))
	}

	lockVal, _ := d.channelLocks.LoadOrStore(targetChannelID, &sync.Mutex{})
	if chLock, ok := lockVal.(*sync.Mutex); ok && chLock != nil {
		chLock.Lock()
		defer chLock.Unlock()
	}

	_, err = sender.ChannelMessageSendComplex(targetChannelID, &discordgo.MessageSend{
		Content:         cleanContent,
		AllowedMentions: &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}},
	}, opts...)
	if err != nil {
		return fmt.Errorf("failed to send direct discord message: %w", err)
	}
	return nil
}

// DeliverDirectMessage delivers a direct plain-text channel alert message to Discord REST.
func (d *DiscordWidgetDeliverer) DeliverDirectMessage(ctx context.Context, channelID, content string) error {
	return d.DeliverDirectMessageWithAgent(ctx, defaultAgent, channelID, content)
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

// Discord JSON error codes for widget edit/delete recovery.
const (
	discordCodeCannotEditForeignAuthor = 50005
	discordCodeMissingPermissions      = 50013
)

// discordErrorCode returns the Discord JSON error code carried by a RESTError, or 0.
func discordErrorCode(err error) int {
	if restErr, ok := errors.AsType[*discordgo.RESTError](err); ok && restErr.Message != nil {
		return restErr.Message.Code
	}
	return 0
}

// isForeignAuthorError reports whether an edit failed because the message was authored by
// a different bot (403 code 50005), e.g. after a per-agent token rotation.
func isForeignAuthorError(err error) bool {
	return discordErrorCode(err) == discordCodeCannotEditForeignAuthor
}

// deleteOldWidgetBestEffort removes a replaced widget message with the owner's own sender.
// Failures never propagate: the replacement card is already posted, and the owner's bot may
// lack permission to delete another bot's message (403 code 50013). No other agent's sender
// is ever used to touch the message.
func deleteOldWidgetBestEffort(s DiscordWidgetSender, channelID, msgID string, opts ...discordgo.RequestOption) {
	delErr := s.ChannelMessageDelete(channelID, msgID, opts...)
	switch {
	case delErr == nil, isNotFoundError(delErr):
		return
	case discordErrorCode(delErr) == discordCodeMissingPermissions:
		log.Printf("[Widget] Old status widget message %s left in place: missing permission to delete it: %v", msgID, delErr)
	default:
		log.Printf("[Widget] Warning: failed to delete old status widget message %s: %v", msgID, delErr)
	}
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
	deleteOldWidgetBestEffort(s, channelID, trimmedMsgID, opts...)
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

	if isForeignAuthorError(editErr) {
		slog.Warn("[Widget] status widget authored by another bot; re-posting with the owner's sender",
			"build_id", req.ID, "agent", req.Agent, "channel_id", req.ChannelID, "message_id", trimmedMsgID)
		send := newWidgetMessageSend(components)
		created, sendErr := s.ChannelMessageSendComplex(req.ChannelID, send, opts...)
		if sendErr != nil {
			return "", fmt.Errorf("failed to send new status widget message after foreign-author edit: %w", sendErr)
		}
		if created == nil {
			return "", errors.New("failed to send new status widget message after foreign-author edit: received nil message")
		}
		deleteOldWidgetBestEffort(s, req.ChannelID, trimmedMsgID, opts...)
		return created.ID, nil
	}

	return "", fmt.Errorf("failed to edit status widget message %s: %w", trimmedMsgID, editErr)
}

// DeliverStatusWidget delivers or updates a status widget via Discord Components V2 using only s.
func DeliverStatusWidget(s DiscordWidgetSender, owner string, req ericlient.StatusWidgetRequest, now time.Time, opts ...discordgo.RequestOption) (string, error) {
	if isNilSender(s) {
		return "", errors.New("discord widget sender is nil")
	}
	if strings.TrimSpace(req.ChannelID) == "" {
		return "", errors.New("channel_id is required")
	}

	components, err := BuildStatusWidgetComponents(owner, req, now)
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

// NormalizeRepo returns repo qualified with owner (e.g. "acme/aerial") and
// its short name, the last path segment ("aerial").
func NormalizeRepo(owner, repo string) (fullRepo string, shortRepo string) {
	fullRepo = githubapi.QualifyRepo(owner, repo)
	return fullRepo, fullRepo[strings.LastIndex(fullRepo, "/")+1:]
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
	case statusRunning, statusCoding, statusPROpened, statusOpen, statusDeploying, statusCIFailed, statusConflict:
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
		_, shortRepo = NormalizeRepo("", req.Chunks[0].Repo)
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

// collapseCompletedWidgetChunks folds all but the 2 most recently completed done
// chunks into a summary line. Input must already be ordered by orderWidgetChunks.
func collapseCompletedWidgetChunks(chunks []ericlient.StatusWidgetChunk) ([]ericlient.StatusWidgetChunk, string) {
	const visibleDone = 2
	if len(chunks) <= 10 {
		return chunks, ""
	}

	doneTotal := 0
	for _, c := range chunks {
		if isWidgetChunkDone(c.Status) {
			doneTotal++
		}
	}
	collapseCount := doneTotal - visibleDone
	if collapseCount <= 0 {
		return chunks, ""
	}

	activeChunks := make([]ericlient.StatusWidgetChunk, 0, len(chunks)-collapseCount)
	collapsed := 0
	for _, c := range chunks {
		if collapsed < collapseCount && isWidgetChunkDone(c.Status) {
			collapsed++
			continue
		}
		activeChunks = append(activeChunks, c)
	}

	summaryText := fmt.Sprintf("✅ ***%d/%d** earlier chunks completed and merged*", collapsed, len(chunks))
	return activeChunks, summaryText
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
	case statusDeployed, statusCompleted, dispatchStatusSuccess, statusFailed, statusDeployFailed, statusClosed, statusError, statusCancelled, statusMerged, statusSynced:
		return true
	default:
		return false
	}
}

func buildWidgetChunkComponent(owner string, c ericlient.StatusWidgetChunk) discordgo.MessageComponent {
	badge := statusBadge(c.Status)
	fullRepo, shortRepo := NormalizeRepo(owner, c.Repo)
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

// Discord Components V2 per-message limits: total components (nested ones
// included) and total TextDisplay characters.
const (
	maxWidgetComponents = 40
	maxWidgetTextRunes  = 4000
)

// widgetRow is one rendered chunk row tagged with its widget lifecycle stage.
type widgetRow struct {
	stage     int
	component discordgo.MessageComponent
}

// widgetStagePriority lists the stages in the order rows keep their place when
// the card overflows: in-progress (incl. failed) first, then done, then pending.
var widgetStagePriority = [...]int{widgetStageInProgress, widgetStageDone, widgetStagePending}

// widgetComponentCount counts c and every nested component, as Discord does.
func widgetComponentCount(c discordgo.MessageComponent) int {
	switch v := c.(type) {
	case *discordgo.Section:
		n := 1
		for _, child := range v.Components {
			n += widgetComponentCount(child)
		}
		if v.Accessory != nil {
			n++
		}
		return n
	default:
		return 1
	}
}

// widgetTextRunes totals the TextDisplay content runes in comps, including Section text.
func widgetTextRunes(comps []discordgo.MessageComponent) int {
	n := 0
	for _, c := range comps {
		switch v := c.(type) {
		case *discordgo.TextDisplay:
			n += utf8.RuneCountInString(v.Content)
		case *discordgo.Section:
			n += widgetTextRunes(v.Components)
		}
	}
	return n
}

// widgetFoldText is the single line that replaces rows folded out of a stage.
func widgetFoldText(stage, n int) string {
	switch stage {
	case widgetStageInProgress:
		return fmt.Sprintf("🔨 *+%d more active chunks*", n)
	case widgetStageDone:
		return fmt.Sprintf("✅ *+%d more completed chunks*", n)
	default:
		return fmt.Sprintf("⏳ *+%d more pending chunks*", n)
	}
}

// assembleWidgetRows renders the kept rows in order; each stage with folded rows
// gets its fold line at the position of that stage's last row.
func assembleWidgetRows(rows []widgetRow, keep []bool) []discordgo.MessageComponent {
	var dropped [widgetStagePending + 1]int
	last := map[int]int{}
	for i, r := range rows {
		last[r.stage] = i
		if !keep[i] {
			dropped[r.stage]++
		}
	}
	out := make([]discordgo.MessageComponent, 0, len(rows))
	for i, r := range rows {
		if keep[i] {
			out = append(out, r.component)
		}
		if last[r.stage] == i && dropped[r.stage] > 0 {
			out = append(out, &discordgo.TextDisplay{Content: widgetFoldText(r.stage, dropped[r.stage])})
		}
	}
	return out
}

// fitWidgetRows returns the chunk rows that fit beside fixedComponents and
// fixedRunes within maxWidgetComponents and maxWidgetTextRunes. Rows that fit
// already are returned unchanged. Otherwise rows are kept by stage priority in
// plan order, and each stage's overflow folds into one summary line.
func fitWidgetRows(rows []widgetRow, fixedComponents, fixedRunes int) []discordgo.MessageComponent {
	all := make([]discordgo.MessageComponent, 0, len(rows))
	total := fixedComponents
	for _, r := range rows {
		all = append(all, r.component)
		total += widgetComponentCount(r.component)
	}
	if total <= maxWidgetComponents && fixedRunes+widgetTextRunes(all) <= maxWidgetTextRunes {
		return all
	}

	byStage := map[int][]int{}
	for i, r := range rows {
		byStage[r.stage] = append(byStage[r.stage], i)
	}
	keep := make([]bool, len(rows))
	remaining := maxWidgetComponents - fixedComponents
	for p, stage := range widgetStagePriority {
		// Every later non-empty stage can always fall back to one fold line.
		reserve := 0
		for _, later := range widgetStagePriority[p+1:] {
			if len(byStage[later]) > 0 {
				reserve++
			}
		}
		idxs := byStage[stage]
		kept := 0
		for k, i := range idxs {
			more := 0
			if k < len(idxs)-1 {
				more = 1
			}
			cost := widgetComponentCount(rows[i].component)
			if remaining-cost < more+reserve {
				break
			}
			keep[i] = true
			remaining -= cost
			kept++
		}
		if kept < len(idxs) {
			remaining--
		}
	}

	out := assembleWidgetRows(rows, keep)
	for fixedRunes+widgetTextRunes(out) > maxWidgetTextRunes && dropLastWidgetRow(rows, keep) {
		out = assembleWidgetRows(rows, keep)
	}
	return out
}

// dropLastWidgetRow unkeeps the last kept row of the lowest-priority stage that
// still has one, reporting whether any row was dropped.
func dropLastWidgetRow(rows []widgetRow, keep []bool) bool {
	for p := len(widgetStagePriority) - 1; p >= 0; p-- {
		for i := len(rows) - 1; i >= 0; i-- {
			if keep[i] && rows[i].stage == widgetStagePriority[p] {
				keep[i] = false
				return true
			}
		}
	}
	return false
}

// BuildStatusWidgetComponents constructs a Components V2 Container message
// component; owner qualifies bare chunk repos in PR links.
func BuildStatusWidgetComponents(owner string, req ericlient.StatusWidgetRequest, now time.Time) ([]discordgo.MessageComponent, error) {
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

	// Fold older done chunks into one summary line, then fit the remaining rows
	// within Discord's per-message component and text limits.
	chunks, summaryText := collapseCompletedWidgetChunks(req.Chunks)
	if summaryText != "" {
		containerComponents = append(containerComponents, &discordgo.TextDisplay{
			Content: summaryText,
		})
	}

	rows := make([]widgetRow, 0, len(chunks))
	for _, c := range chunks {
		rows = append(rows, widgetRow{stage: widgetChunkStage(c.Status), component: buildWidgetChunkComponent(owner, c)})
	}

	footer := &discordgo.TextDisplay{
		Content: formatWidgetFooterText(req, now),
	}
	// Container + header/banner/summary + footer.
	fixedComponents := 1 + len(containerComponents) + 1
	fixedRunes := widgetTextRunes(containerComponents) + utf8.RuneCountInString(footer.Content)
	containerComponents = append(containerComponents, fitWidgetRows(rows, fixedComponents, fixedRunes)...)
	containerComponents = append(containerComponents, footer)

	container := &discordgo.Container{
		AccentColor: &color,
		Components:  containerComponents,
	}

	return []discordgo.MessageComponent{container}, nil
}
