package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/azylman/aerial/pkg/sanitizer"
)

var (
	instructionsCacheMu      sync.RWMutex
	instructionsCache        = make(map[string]string)
	reChannelInstructionsTag = regexp.MustCompile(`(?i)</channel_instructions\s*>`)
	reScopeInstructionsTag   = regexp.MustCompile(`(?i)</scope_instructions\s*>`)
)

var ScopeInstructionsDirs = []string{
	"/share/aerial-config/channels",
	"/share/aerial/channels",
	"/app/channels",
	"./channels",
	"/share/aerial-config/scopes",
	"/share/aerial/scopes",
	"/app/scopes",
	"./scopes",
	"/share/aerial-config/nodes",
	"/share/aerial/nodes",
	"/app/nodes",
	"./nodes",
}

var ChannelInstructionsDirs = []string{
	"/share/aerial-config/channels",
	"/share/aerial/channels",
	"/app/channels",
	"./channels",
}

func validateChannelPolicy(name string, p *ChannelPolicy, isDefault bool, targetPath string) error {
	prefix := fmt.Sprintf("channel %q", name)
	if isDefault {
		prefix = "channels.default"
	}
	if isDefault || p.Mode != "" {
		m := strings.ToLower(strings.TrimSpace(p.Mode))
		if m != "thread" && m != "threads" && m != "classifier" && m != "mention" && m != "ignore" && m != "channel" && m != "disabled" {
			log.Printf("[Config] Validation error: %s mode must be 'threads', 'channel', 'ignore', or 'disabled' (or 'thread', 'classifier', 'mention'), got %q in %s.", prefix, p.Mode, targetPath)
			return fmt.Errorf("%s mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q", prefix, p.Mode)
		}
		p.Mode = m
	}
	if p.AmbientWakeThreshold != nil {
		if *p.AmbientWakeThreshold < 0.0 || *p.AmbientWakeThreshold > 1.0 {
			log.Printf("[Config] Validation error: %s ambient_wake_threshold must be between 0.0 and 1.0, got %f in %s.", prefix, *p.AmbientWakeThreshold, targetPath)
			return fmt.Errorf("%s ambient_wake_threshold must be between 0.0 and 1.0, got %f", prefix, *p.AmbientWakeThreshold)
		}
	}
	if p.WakeMode != "" {
		switch strings.ToLower(strings.TrimSpace(p.WakeMode)) {
		case "mention", "mentions", "direct", "classifier", "ambient", "all", "always":
			p.WakeMode = p.GetWakeMode()
		default:
			log.Printf("[Config] Validation error: %s wake_mode must be 'mention', 'classifier', or 'all', got %q in %s.", prefix, p.WakeMode, targetPath)
			return fmt.Errorf("%s wake_mode must be 'mention', 'classifier', or 'all', got %q", prefix, p.WakeMode)
		}
	}
	if p.Effort != "" {
		switch eLower := strings.ToLower(strings.TrimSpace(p.Effort)); eLower {
		case "low", "medium", "high":
			p.Effort = eLower
		default:
			log.Printf("[Config] Validation error: %s effort must be 'low', 'medium', or 'high', got %q in %s.", prefix, p.Effort, targetPath)
			return fmt.Errorf("%s effort must be 'low', 'medium', or 'high', got %q", prefix, p.Effort)
		}
	}
	return validateHooks(name, p.Hooks, targetPath)
}

func validateChannels(parsed *ConfigData, targetPath string) error {
	if strings.TrimSpace(parsed.Model) == "" {
		parsed.Model = DefaultConfigData().Model
	}
	if strings.TrimSpace(parsed.LowEffortModel) == "" {
		parsed.LowEffortModel = DefaultConfigData().LowEffortModel
	}
	if strings.TrimSpace(parsed.MediumEffortModel) == "" {
		parsed.MediumEffortModel = DefaultConfigData().MediumEffortModel
	}

	if parsed.ClassifierProtocol != "" {
		pLower := strings.ToLower(strings.TrimSpace(parsed.ClassifierProtocol))
		if pLower != "systemone" && pLower != "ollama" {
			log.Printf("[Config] Validation error: classifier_protocol must be 'systemone' or 'ollama', got %q in %s.", parsed.ClassifierProtocol, targetPath)
			return fmt.Errorf("classifier_protocol must be 'systemone' or 'ollama', got %q", parsed.ClassifierProtocol)
		}
		if pLower == "systemone" && strings.TrimSpace(parsed.ClassifierURL) == "" {
			log.Printf("[Config] Validation error: classifier_url is required when classifier_protocol is 'systemone' in %s.", targetPath)
			return fmt.Errorf("classifier_url is required when classifier_protocol is 'systemone'")
		}
		parsed.ClassifierProtocol = pLower
	}

	if parsed.Channels == nil {
		log.Printf("[Config] Validation error: channels.default is required in %s.", targetPath)
		return fmt.Errorf("channels.default is required")
	}

	defPolicy, hasDefault := parsed.Channels["default"]
	if !hasDefault {
		for k, v := range parsed.Channels {
			if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == "default" {
				defPolicy = v
				hasDefault = true
				break
			}
		}
	}

	if !hasDefault {
		log.Printf("[Config] Validation error: channels.default is required in %s.", targetPath)
		return fmt.Errorf("channels.default is required")
	}

	if err := validateChannelPolicy("default", &defPolicy, true, targetPath); err != nil {
		return err
	}
	parsed.Channels["default"] = defPolicy

	for k, policy := range parsed.Channels {
		if k == "default" {
			continue
		}
		if err := validateChannelPolicy(k, &policy, false, targetPath); err != nil {
			return err
		}
		parsed.Channels[k] = policy
	}
	return nil
}

func validateWebhookEndpoint(channelName, hookName string, ep *WebhookEndpoint, targetPath string) error {
	if ep == nil {
		return nil
	}
	rawURL := strings.TrimSpace(ep.URL)
	if rawURL == "" {
		log.Printf("[Config] Validation error: channel %q %s webhook url is required in %s.", channelName, hookName, targetPath)
		return fmt.Errorf("channel %q %s webhook url is required", channelName, hookName)
	}
	parsedURL, err := url.ParseRequestURI(rawURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		log.Printf("[Config] Validation error: channel %q %s webhook url must be a valid http or https URL, got %q in %s.", channelName, hookName, ep.URL, targetPath)
		return fmt.Errorf("channel %q %s webhook url must be a valid http or https URL, got %q", channelName, hookName, ep.URL)
	}
	if ep.TimeoutMs < 0 {
		log.Printf("[Config] Validation error: channel %q %s webhook timeout_ms cannot be negative, got %d in %s.", channelName, hookName, ep.TimeoutMs, targetPath)
		return fmt.Errorf("channel %q %s webhook timeout_ms cannot be negative, got %d", channelName, hookName, ep.TimeoutMs)
	}
	return nil
}

func validateHooks(channelName string, hooks ChannelHooksConfig, targetPath string) error {
	for name, ep := range map[string]*WebhookEndpoint{"on_wake": hooks.OnWake, "pre_turn": hooks.PreTurn, "post_turn": hooks.PostTurn} {
		if ep != nil {
			if err := validateWebhookEndpoint(channelName, name, ep, targetPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func IsAdmin(adminUsers []string, identifiers ...string) bool {
	if len(adminUsers) == 0 {
		return false
	}
	for _, id := range identifiers {
		trimmedID := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "@")
		if trimmedID == "" {
			continue
		}
		for _, admin := range adminUsers {
			trimmedAdmin := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(admin)), "@")
			if trimmedAdmin == trimmedID {
				return true
			}
		}
	}
	return false
}

func resolveDefaultChannelPolicy(channels map[string]ChannelPolicy) ChannelPolicy {
	def, hasDef := getChannelPolicyRaw(channels, "default")
	if !hasDef {
		defaultIgnoreBots := true
		return ChannelPolicy{
			Mode:       "threads",
			IgnoreBots: &defaultIgnoreBots,
		}
	}
	if def.Mode == "" {
		def.Mode = "threads"
	}
	return def
}

func matchChannelPolicy(channels map[string]ChannelPolicy, channelID, channelName string) (ChannelPolicy, bool) {
	if normID := strings.TrimSpace(channelID); normID != "" {
		if matched, found := getChannelPolicyRaw(channels, normID); found {
			return matched, true
		}
	}
	if normName := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(channelName)), "#"); normName != "" {
		return getChannelPolicyRaw(channels, normName)
	}
	return ChannelPolicy{}, false
}

func inheritHooks(res, def ChannelHooksConfig) ChannelHooksConfig {
	out := res
	if out.OnWake == nil {
		out.OnWake = def.OnWake
	}
	if out.PreTurn == nil {
		out.PreTurn = def.PreTurn
	}
	if out.PostTurn == nil {
		out.PostTurn = def.PostTurn
	}
	out.OnWake = clonePtr(out.OnWake)
	out.PreTurn = clonePtr(out.PreTurn)
	out.PostTurn = clonePtr(out.PostTurn)
	return out
}

func ResolveChannelPolicy(channels map[string]ChannelPolicy, channelID, channelName string) ChannelPolicy {
	def := resolveDefaultChannelPolicy(channels)
	matched, found := matchChannelPolicy(channels, channelID, channelName)
	if !found {
		return cloneChannelPolicy(def)
	}

	res := matched
	if res.Mode == "" {
		res.Mode = def.Mode
	}
	if res.IsIgnored() {
		return res
	}
	if res.WakeMode == "" && def.WakeMode != "" {
		res.WakeMode = def.WakeMode
	}
	if res.Effort == "" && def.Effort != "" {
		res.Effort = def.Effort
	}
	if !res.BlockBuildCards {
		res.BlockBuildCards = def.BlockBuildCards
	}
	if res.IgnoreBots == nil {
		res.IgnoreBots = clonePtr(def.IgnoreBots)
	} else {
		res.IgnoreBots = clonePtr(res.IgnoreBots)
	}
	if res.AmbientWakeThreshold == nil {
		res.AmbientWakeThreshold = clonePtr(def.AmbientWakeThreshold)
	} else {
		res.AmbientWakeThreshold = clonePtr(res.AmbientWakeThreshold)
	}
	res.Hooks = inheritHooks(res.Hooks, def.Hooks)
	if res.AmbientContext != nil {
		res.AmbientContext = cloneAmbientContextConfig(res.AmbientContext)
	} else if def.AmbientContext != nil {
		res.AmbientContext = cloneAmbientContextConfig(def.AmbientContext)
	}
	return res
}

func getChannelPolicyRaw(channels map[string]ChannelPolicy, key string) (ChannelPolicy, bool) {
	if len(channels) == 0 || key == "" {
		return ChannelPolicy{}, false
	}
	if p, ok := channels[key]; ok {
		return p, true
	}
	normKey := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(key)), "#")
	for k, v := range channels {
		if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == normKey {
			return v, true
		}
	}
	return ChannelPolicy{}, false
}

func (c *Config) ResolveChannelPolicy(channelID, channelName string) ChannelPolicy {
	return ResolveChannelPolicy(c.Current().Channels, channelID, channelName)
}

func (c *Config) IsAdmin(identifiers ...string) bool {
	return IsAdmin(c.Current().AdminUsers, identifiers...)
}

// NormalizeChannelName standardizes channel names for config key and file lookups.
// It trims whitespace, strips any leading '#', lowercases, and strictly blocks directory traversal.
func NormalizeChannelName(channelName string) string {
	s := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(channelName)), "#")
	if strings.Contains(s, "..") || strings.Contains(s, "/") || strings.Contains(s, "\\") {
		return ""
	}
	s = filepath.Base(s)
	if s == "." || s == "/" || s == "\\" || s == "" {
		return ""
	}
	return s
}

// NormalizeScopeName standardizes scope names (channels, nodes, devices) for config key and file lookups.
// It trims whitespace, strips any leading '#', lowercases, and strictly blocks directory traversal.
func NormalizeScopeName(scopeName string) string {
	return NormalizeChannelName(scopeName)
}

func readScopeInstructionsFile(cleanDir, cand, normName string) (string, bool) {
	targetPath := filepath.Join(cleanDir, cand+".md")
	cleanTarget := filepath.Clean(targetPath)
	if !strings.HasPrefix(cleanTarget, cleanDir+string(filepath.Separator)) {
		return "", false
	}
	info, err := os.Stat(cleanTarget)
	if err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	f, err := os.Open(cleanTarget)
	if err != nil {
		return "", false
	}
	data, err := io.ReadAll(io.LimitReader(f, 64*1024))
	if closeErr := f.Close(); closeErr != nil {
		log.Printf("[Config] Warning closing instructions file %s: %v", cleanTarget, closeErr)
	}
	if err != nil {
		return "", false
	}
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) > 0 {
		// Defensively escape any closing delimiter tags to prevent breakout
		sanitized := reChannelInstructionsTag.ReplaceAllString(trimmed, "<\\/CHANNEL_INSTRUCTIONS>")
		sanitized = reScopeInstructionsTag.ReplaceAllString(sanitized, "<\\/SCOPE_INSTRUCTIONS>")
		instructionsCacheMu.Lock()
		instructionsCache[normName] = sanitized
		instructionsCacheMu.Unlock()
		return sanitized, true
	}

	instructionsCacheMu.RLock()
	cached, hasCached := instructionsCache[normName]
	instructionsCacheMu.RUnlock()
	if hasCached && cached != "" {
		log.Printf("[Config] Active file is empty (possible GitSync torn read), using cached instructions for %q", normName)
		return cached, true
	}
	return "", false
}

// LoadScopeInstructions loads per-scope (channel, node, kiosk) instructions from convention-based markdown files.
// It normalizes the scope name, searches ScopeInstructionsDirs with path confinement,
// enforces regular file checks, caps reading at 64KB, and sanitizes instructions closing tags.
func LoadScopeInstructions(scopeName string) string {
	normName := NormalizeScopeName(scopeName)
	if normName == "" {
		return ""
	}

	candidateNames := []string{normName}
	if strings.Contains(normName, " ") {
		candidateNames = append(candidateNames, strings.ReplaceAll(normName, " ", "-"))
	}
	if strings.Contains(normName, "-") {
		candidateNames = append(candidateNames, strings.ReplaceAll(normName, "-", " "))
	}

	searchDirs := make([]string, 0, len(ScopeInstructionsDirs)+len(ChannelInstructionsDirs))
	seenDirs := make(map[string]bool)
	for _, d := range append(append([]string{}, ChannelInstructionsDirs...), ScopeInstructionsDirs...) {
		cd := filepath.Clean(d)
		if !seenDirs[cd] {
			seenDirs[cd] = true
			searchDirs = append(searchDirs, cd)
		}
	}

	for _, cleanDir := range searchDirs {
		for _, cand := range candidateNames {
			if content, ok := readScopeInstructionsFile(cleanDir, cand, normName); ok {
				return content
			}
		}
	}
	return ""
}

func collectCoreTokens(cur *ConfigData) []string {
	var tokens []string
	if cur.DiscordToken != "" {
		tokens = append(tokens, cur.DiscordToken)
	}
	if cur.Secrets.DiscordToken != "" && cur.Secrets.DiscordToken != cur.DiscordToken {
		tokens = append(tokens, cur.Secrets.DiscordToken)
	}
	if cur.APIKey != "" {
		tokens = append(tokens, cur.APIKey)
	}
	if cur.HarnessAPIKey != "" {
		tokens = append(tokens, cur.HarnessAPIKey)
	}
	if cur.GitHubPAT != "" {
		tokens = append(tokens, cur.GitHubPAT)
	}
	if cur.DatabaseURL != "" {
		if u, err := url.Parse(cur.DatabaseURL); err == nil && u.User != nil {
			if pass, ok := u.User.Password(); ok && pass != "" {
				tokens = append(tokens, pass)
			}
		}
	}
	return tokens
}

func extractMapTokens(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var tokens []string
	for _, k := range keys {
		if v := m[k]; v != "" {
			tokens = append(tokens, v)
		}
	}
	return tokens
}

func extractMCPServerTokens(srv MCPServerConfig) []string {
	var tokens []string
	tokens = append(tokens, extractMapTokens(srv.Env)...)
	tokens = append(tokens, extractMapTokens(srv.Headers)...)
	return tokens
}

func collectMCPServerTokens(serverMaps []map[string]MCPServerConfig) []string {
	var tokens []string
	for _, m := range serverMaps {
		if len(m) == 0 {
			continue
		}
		names := make([]string, 0, len(m))
		for name := range m {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			tokens = append(tokens, extractMCPServerTokens(m[name])...)
		}
	}
	return tokens
}

func collectRawMCPServerTokens(rawMaps []map[string]json.RawMessage) []string {
	var tokens []string
	for _, m := range rawMaps {
		if len(m) == 0 {
			continue
		}
		names := make([]string, 0, len(m))
		for name := range m {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			raw := m[name]
			if len(raw) == 0 {
				continue
			}
			var srv MCPServerConfig
			if err := json.Unmarshal(raw, &srv); err != nil {
				continue
			}
			tokens = append(tokens, extractMCPServerTokens(srv)...)
		}
	}
	return tokens
}

// RegisterConfigTokens extracts secrets from the injected *Config and registers them
// into the global redaction pool.
func RegisterConfigTokens(cfg *Config) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()

	var tokens []string
	tokens = append(tokens, collectCoreTokens(cur)...)
	tokens = append(tokens, collectMCPServerTokens([]map[string]MCPServerConfig{
		cur.MCPServers.Common,
		cur.MCPServers.Eri,
		cur.MCPServers.Voice,
		cur.MCPServers.Ephemeral,
	})...)
	tokens = append(tokens, collectRawMCPServerTokens([]map[string]json.RawMessage{
		cur.McpServers.Common,
		cur.McpServers.Discord,
		cur.McpServers.Voice,
		cur.McpServers.Ephemeral,
	})...)

	if len(tokens) > 0 {
		sanitizer.RegisterSensitiveTokens(tokens...)
	}
}
