package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"gopkg.in/yaml.v3"
)

// Config holds runtime configuration for synapse.
type Config struct {
	Port                string
	KubeAPIServer       string
	KubeToken           string
	GitHubToken         string
	GitHubAPIURL        string
	PostgresURL         string
	FluxReceiverURL     string
	BrainURL            string
	EriURL              string
	VanadisURL          string
	HaroURL             string
	NATSURL             string
	SystemChannelID     string
	DiscordToken        string
	AutoUpdatePRAuthors []string
	BotID               string
	DefaultOwner        bool
	WebhookForwardURLs  map[string]string
}

// RouterYAMLConfig defines the declarative file schema for services/synapse/synapse.yaml.
type RouterYAMLConfig struct {
	Port                string            `yaml:"port"`
	FluxReceiverURL     string            `yaml:"flux_receiver_url"`
	EriURL              string            `yaml:"eri_url"`
	VanadisURL          string            `yaml:"vanadis_url"`
	HaroURL             string            `yaml:"haro_url"`
	NATSURL             string            `yaml:"nats_url"`
	SystemChannelID     string            `yaml:"system_channel_id"`
	SystemChannel       string            `yaml:"system_channel"`
	AutoUpdatePRAuthors []string          `yaml:"auto_update_pr_authors"`
	BotID               string            `yaml:"bot_id"`
	DefaultOwner        bool              `yaml:"default_owner"`
	WebhookForwardURLs  map[string]string `yaml:"webhook_forward_urls"`
}

var (
	synapseFallbackConfigPath = "/config/synapse.yaml"
	synapseAerialConfigPath   = "/share/aerial-config/config.yaml"
)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func normalizeWebhookForwardURLs(raw map[string]string) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]string)
	for k, v := range raw {
		key := strings.TrimSpace(k)
		val := strings.TrimRight(strings.TrimSpace(v), "/")
		if key != "" && val != "" {
			out[key] = val
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func loadAerialConfigFallback(path, sysChan, botID string, autoUpdateAuthors []string) (string, string, []string) {
	data, err := os.ReadFile(path)
	if err != nil && path == "/share/aerial-config/config.yaml" {
		data, err = os.ReadFile("/config/config.yaml")
	}
	if err != nil {
		return sysChan, botID, autoUpdateAuthors
	}
	var aerialCfg RouterYAMLConfig
	if err := yaml.Unmarshal(data, &aerialCfg); err != nil {
		return sysChan, botID, autoUpdateAuthors
	}
	if sysChan == "" {
		sysChan = firstNonEmpty(aerialCfg.SystemChannelID, aerialCfg.SystemChannel)
	}
	if botID == "" {
		botID = strings.TrimSpace(aerialCfg.BotID)
	}
	if len(autoUpdateAuthors) == 0 {
		autoUpdateAuthors = dedupeTrimmedStrings(aerialCfg.AutoUpdatePRAuthors)
	}
	return sysChan, botID, autoUpdateAuthors
}

// ConfigSource yields the live synapse configuration snapshot. Callers read
// Current() once per request, call, or worker tick and never mutate it.
type ConfigSource interface {
	Current() *Config
}

// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
// A YAML file that fails to parse is logged and ignored so startup always proceeds.
func LoadConfig(configPath string) Config {
	fileCfg, err := readRouterYAML(configPath)
	if err != nil {
		log.Printf("[Synapse] Warning: %v", err)
	}
	return buildConfig(fileCfg)
}

// LoadConfigStrict resolves configuration exactly like LoadConfig but returns
// an error when the resolved YAML file exists and cannot be read or parsed.
// Hot reloads use it so a bad commit never replaces the last known good config.
func LoadConfigStrict(configPath string) (Config, error) {
	fileCfg, err := readRouterYAML(configPath)
	if err != nil {
		return Config{}, err
	}
	return buildConfig(fileCfg), nil
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
}

// loadConfigFromEnvStrict is the hot-reload loader: it re-reads CONFIG_PATH
// through LoadConfigStrict so a malformed file keeps the last known good config.
func loadConfigFromEnvStrict() (*Config, error) {
	cfg, err := LoadConfigStrict(os.Getenv("CONFIG_PATH"))
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// synapseRestartRequired reports whether moving from old to next changes a
// field that is bound once at startup (listener, database pool, Discord
// gateway session, outbound dispatcher and registry clients) and therefore
// can only take effect through a graceful process restart.
func synapseRestartRequired(old, next *Config) bool {
	return old.Port != next.Port ||
		old.PostgresURL != next.PostgresURL ||
		old.DiscordToken != next.DiscordToken ||
		old.BotID != next.BotID ||
		old.BrainURL != next.BrainURL ||
		old.FluxReceiverURL != next.FluxReceiverURL ||
		old.KubeAPIServer != next.KubeAPIServer ||
		old.KubeToken != next.KubeToken ||
		old.VanadisURL != next.VanadisURL ||
		old.HaroURL != next.HaroURL ||
		old.NATSURL != next.NATSURL
}

// resolveRouterYAMLPath returns the YAML file to read: configPath when it is a
// non-empty file, otherwise the fallback path when that is a non-empty file.
func resolveRouterYAMLPath(configPath string) string {
	if targetPath := strings.TrimSpace(configPath); targetPath != "" {
		if fi, err := os.Stat(targetPath); err == nil && fi.Size() > 0 {
			return targetPath
		}
	}
	if fi, err := os.Stat(synapseFallbackConfigPath); err == nil && fi.Size() > 0 {
		return synapseFallbackConfigPath
	}
	return ""
}

// readRouterYAML reads the resolved YAML file. It returns whatever fields
// decoded alongside any read or parse error so LoadConfig can keep its
// warn-and-continue semantics while LoadConfigStrict rejects the file.
func readRouterYAML(configPath string) (RouterYAMLConfig, error) {
	var fileCfg RouterYAMLConfig
	targetPath := resolveRouterYAMLPath(configPath)
	if targetPath == "" {
		return fileCfg, nil
	}
	data, err := os.ReadFile(targetPath)
	if err != nil {
		return fileCfg, fmt.Errorf("failed to read yaml config at %s: %w", targetPath, err)
	}
	if err := yaml.Unmarshal(data, &fileCfg); err != nil {
		return fileCfg, fmt.Errorf("failed to parse yaml config at %s: %w", targetPath, err)
	}
	return fileCfg, nil
}

// buildConfig layers environment overrides and the aerial config fallback on
// top of the decoded YAML file.
func buildConfig(fileCfg RouterYAMLConfig) Config {
	autoUpdateAuthors := dedupeTrimmedStrings(fileCfg.AutoUpdatePRAuthors)
	sysChan := firstNonEmpty(fileCfg.SystemChannelID, fileCfg.SystemChannel)
	var aerialBotID string
	if (sysChan == "" || len(autoUpdateAuthors) == 0 || aerialBotID == "") && synapseAerialConfigPath != "" {
		sysChan, aerialBotID, autoUpdateAuthors = loadAerialConfigFallback(synapseAerialConfigPath, sysChan, aerialBotID, autoUpdateAuthors)
	}

	botID := strings.TrimSpace(firstNonEmpty(os.Getenv("AERIAL_BOT_ID"), fileCfg.BotID, aerialBotID, "aerial"))
	defaultOwner := fileCfg.DefaultOwner
	if envVal := strings.TrimSpace(firstNonEmpty(os.Getenv("AERIAL_DEFAULT_OWNER"), os.Getenv("DEFAULT_OWNER"))); envVal != "" {
		defaultOwner = strings.EqualFold(envVal, "true") || envVal == "1"
	}

	pgURL := firstNonEmpty(httpkit.BuildPostgresDSN(os.Getenv, "aerial"), "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable")

	kubeServer := os.Getenv("KUBE_API_SERVER")
	if kubeServer == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			kubeServer = "https://" + net.JoinHostPort(host, port)
		}
	}
	kubeToken := os.Getenv("KUBE_TOKEN")

	return Config{
		Port:                firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4020"),
		KubeAPIServer:       strings.TrimRight(kubeServer, "/"),
		KubeToken:           kubeToken,
		GitHubToken:         os.Getenv("GITHUB_PAT"),
		GitHubAPIURL:        strings.TrimRight(firstNonEmpty(os.Getenv("GITHUB_API_URL"), "https://api.github.com"), "/"),
		PostgresURL:         pgURL,
		FluxReceiverURL:     strings.TrimRight(firstNonEmpty(os.Getenv("FLUX_RECEIVER_URL"), fileCfg.FluxReceiverURL), "/"),
		BrainURL:            strings.TrimRight(firstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL, "http://127.0.0.1:8088"), "/"),
		EriURL:              strings.TrimRight(firstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL, "http://127.0.0.1:8088"), "/"),
		VanadisURL:          strings.TrimRight(firstNonEmpty(os.Getenv("VANADIS_URL"), fileCfg.VanadisURL, "http://vanadis.aerial"), "/"),
		HaroURL:             strings.TrimRight(firstNonEmpty(os.Getenv("HARO_URL"), fileCfg.HaroURL, "http://haro.aerial"), "/"),
		NATSURL:             firstNonEmpty(os.Getenv("NATS_URL"), fileCfg.NATSURL, "nats://nats.aerial:4222"),
		SystemChannelID:     strings.TrimSpace(firstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), sysChan)),
		DiscordToken:        os.Getenv("DISCORD_TOKEN"),
		AutoUpdatePRAuthors: autoUpdateAuthors,
		BotID:               botID,
		DefaultOwner:        defaultOwner,
		WebhookForwardURLs:  normalizeWebhookForwardURLs(fileCfg.WebhookForwardURLs),
	}
}

func randomNonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// normalizeRepo ensures repository names are normalized with the owner prefix.
func normalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	if !strings.Contains(repo, "/") {
		return "azylman/" + repo
	}
	return repo
}

// cleanRepoName strips the owner prefix (e.g. "azylman/") for concise Discord notifications.
func cleanRepoName(repo string) string {
	return strings.TrimPrefix(strings.TrimSpace(repo), "azylman/")
}

// formatDirectMessagePrefix formats the bold PR and repo prefix for direct Discord messages.
func formatDirectMessagePrefix(prNumber int, repo string) string {
	repoName := cleanRepoName(repo)
	switch {
	case prNumber > 0 && repoName != "":
		return fmt.Sprintf("**(PR: #%d, repo: %s)** ", prNumber, repoName)
	case repoName != "":
		return fmt.Sprintf("**(repo: %s)** ", repoName)
	case prNumber > 0:
		return fmt.Sprintf("**(PR: #%d)** ", prNumber)
	default:
		return ""
	}
}

func dedupeTrimmedStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var out []string
	for _, item := range items {
		if clean := strings.TrimSpace(item); clean != "" {
			if _, exists := seen[clean]; !exists {
				seen[clean] = struct{}{}
				out = append(out, clean)
			}
		}
	}
	return out
}

// Discord Snowflake and Channel Name validation
var (
	snowflakeRegex   = regexp.MustCompile(`^[0-9]{17,20}$`)
	channelNameRegex = regexp.MustCompile(`^#?[a-z0-9_-]{1,100}$`)
)

// NormalizeDiscordTargetID strips Model 2 session prefixes ("discord-" and optional
// colon-delimited pool scopes like "stateful_medium:discord-") from a target identifier.
func NormalizeDiscordTargetID(id string) string {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

// IsHeadlessSessionKey returns true if id is a Model 2 headless dispatch, heartbeat,
// or pooled worker session key (e.g. "dispatch-<uuid>", "heartbeat-<id>", "discord:worker-0",
// "discord:medium-worker-0", or "ephemeral:worker-0").
func IsHeadlessSessionKey(id string) bool {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.HasPrefix(clean, "dispatch-") ||
		strings.HasPrefix(clean, "heartbeat-") ||
		strings.HasPrefix(clean, "worker-") ||
		strings.Contains(clean, "-worker-")
}

func IsValidDiscordSnowflake(id string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(id))
}

func IsValidDiscordChannel(id string) bool {
	clean := strings.TrimSpace(id)
	return IsValidDiscordSnowflake(clean) || channelNameRegex.MatchString(clean)
}

var re4xx = regexp.MustCompile(`\bstatus 4\d{2}\b`)

// is4xxClientError returns true if the error indicates a 4xx HTTP client error or invalid Discord snowflake.
func is4xxClientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return re4xx.MatchString(msg) || strings.Contains(msg, "invalid discord snowflake")
}

func formatErrorDetailsBlock(header, details, extraLine, footer string) string {
	lines := []string{header}
	if clean := strings.TrimSpace(details); clean != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(clean)))
	}
	if cleanExtra := strings.TrimSpace(extraLine); cleanExtra != "" {
		lines = append(lines, cleanExtra)
	}
	if footer != "" {
		lines = append(lines, footer)
	}
	return strings.Join(lines, "\n")
}

func normalizeWidgetStatus(dbStatus string) string {
	switch strings.ToLower(strings.TrimSpace(dbStatus)) {
	case "ci_failed", "deploy_failed":
		return "failed"
	default:
		return strings.ToLower(strings.TrimSpace(dbStatus))
	}
}

func formatVanadisBuildCompletedPrompt(res *VanadisAdvancementResult) string {
	if res == nil {
		return ""
	}
	total := len(res.Chunks)
	lines := []string{
		fmt.Sprintf("Vanadis build %s completed (Tier %d, %d chunks merged).", res.BuildID, res.Tier, total),
	}
	if obj := strings.TrimSpace(res.Objective); obj != "" {
		lines = append(lines, "Objective: "+obj)
	}
	if total > 0 {
		lines = append(lines, "", "Merged Chunks Ledger:")
		for _, c := range res.Chunks {
			cRepo := strings.TrimSpace(c.Repo)
			if cRepo == "" {
				cRepo = "azylman/aerial"
			}
			prLabel := fmt.Sprintf(" (%s)", cRepo)
			if c.PRNumber > 0 {
				prLabel = fmt.Sprintf(" (%s, PR #%d)", cRepo, c.PRNumber)
			}
			line := fmt.Sprintf("- Chunk %d/%d: %s%s", c.ChunkIndex+1, total, c.Title, prLabel)
			lines = append(lines, line)
		}
	}
	lines = append(lines, "", "All Vanadis build chunks have merged into main. Summarize the completed build and verification outcomes for the user.")
	return strings.Join(lines, "\n")
}

const (
	maxPromptRunes           = 1500
	maxCIRemediationAttempts = 3
	maxRoboRevCommentRunes   = 4000
)

func truncatePromptDetails(s string) string {
	return truncatePromptRunes(strings.ReplaceAll(s, "```", "'''"), maxPromptRunes)
}

func truncatePromptRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}

func readBodyString(r io.Reader) string {
	body, err := io.ReadAll(r)
	if err != nil {
		return ""
	}
	return string(body)
}
