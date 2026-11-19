package config

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// Config holds runtime configuration for synapse.
type Config struct {
	LegacyBuildWorkflows []LegacyBuildWorkflow
	Port                 string
	KubeAPIServer        string
	KubeToken            string
	GitHubToken          string
	GitHubAPIURL         string
	GitHubWebhookSecret  string
	PostgresURL          string
	FluxReceiverURL      string
	BrainURL             string
	EriURL               string
	NATSURL              string
	SystemChannelID      string
	DiscordToken         string
	AutoUpdatePRAuthors  []string
	BotID                string
	DefaultOwner         bool
	WebhookForwardURLs   map[string]string
	CollaboratorCIAlerts []CollaboratorCIAlertRule
}

// CollaboratorCIAlertRule defines a notification rule for unregistered collaborator pull requests.
type CollaboratorCIAlertRule struct {
	Repo      string            `yaml:"repo"`
	ChannelID string            `yaml:"channel_id"`
	Authors   map[string]string `yaml:"authors"`
}

// RouterYAMLConfig defines the declarative file schema for services/synapse/synapse.yaml.
type RouterYAMLConfig struct {
	LegacyBuildWorkflows []LegacyBuildWorkflow     `yaml:"legacy_build_workflows"`
	Port                 string                    `yaml:"port"`
	FluxReceiverURL      string                    `yaml:"flux_receiver_url"`
	EriURL               string                    `yaml:"eri_url"`
	NATSURL              string                    `yaml:"nats_url"`
	SystemChannelID      string                    `yaml:"system_channel_id"`
	SystemChannel        string                    `yaml:"system_channel"`
	AutoUpdatePRAuthors  []string                  `yaml:"auto_update_pr_authors"`
	BotID                string                    `yaml:"bot_id"`
	DefaultOwner         bool                      `yaml:"default_owner"`
	WebhookForwardURLs   map[string]string         `yaml:"webhook_forward_urls"`
	CollaboratorCIAlerts []CollaboratorCIAlertRule `yaml:"collaborator_ci_alerts"`
}

// LegacyBuildWorkflow pins one immutable historical producer revision.
type LegacyBuildWorkflow struct {
	Repository   string `yaml:"repository"`
	WorkflowPath string `yaml:"workflow_path"`
	BlobSHA      string `yaml:"blob_sha"`
}

var (
	SynapseFallbackConfigPath = "/config/synapse.yaml"
	SynapseAerialConfigPath   = "/share/aerial-config/config.yaml"
)

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
	if _, err := NormalizeLegacyBuildWorkflows(fileCfg.LegacyBuildWorkflows); err != nil {
		return Config{}, err
	}
	return buildConfig(fileCfg), nil
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
}

// LoadConfigFromEnvStrict is the hot-reload loader: it re-reads CONFIG_PATH
// through LoadConfigStrict so a malformed file keeps the last known good config.
func LoadConfigFromEnvStrict() (*Config, error) {
	cfg, err := LoadConfigStrict(os.Getenv("CONFIG_PATH"))
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// RestartRequired reports whether moving from old to next changes a
// field that is bound once at startup (listener, database pool, Discord
// gateway session, outbound dispatcher and registry clients) and therefore
// can only take effect through a graceful process restart.
func RestartRequired(old, next *Config) bool {
	if old == nil || next == nil {
		return true
	}
	return old.Port != next.Port ||
		old.PostgresURL != next.PostgresURL ||
		old.DiscordToken != next.DiscordToken ||
		old.BotID != next.BotID ||
		old.BrainURL != next.BrainURL ||
		old.FluxReceiverURL != next.FluxReceiverURL ||
		old.KubeAPIServer != next.KubeAPIServer ||
		old.KubeToken != next.KubeToken ||
		old.NATSURL != next.NATSURL
}

// HasCollaboratorCIAlertsForRepo reports whether any collaborator alert rules are configured for repo.
func HasCollaboratorCIAlertsForRepo(cfg *Config, repo string) bool {
	if cfg == nil || len(cfg.CollaboratorCIAlerts) == 0 {
		return false
	}
	targetRepo := model.NormalizeRepo(repo)
	if targetRepo == "" {
		return false
	}
	for _, rule := range cfg.CollaboratorCIAlerts {
		if strings.EqualFold(rule.Repo, targetRepo) {
			return true
		}
	}
	return false
}

// LookupCollaboratorCIAlert finds the matching channel ID and user mention for an author in repo.
func LookupCollaboratorCIAlert(cfg *Config, repo, author string) (channelID, mention string, ok bool) {
	if cfg == nil || len(cfg.CollaboratorCIAlerts) == 0 {
		return "", "", false
	}
	targetRepo := model.NormalizeRepo(repo)
	cleanAuthor := strings.ToLower(strings.TrimSpace(author))
	if targetRepo == "" || cleanAuthor == "" {
		return "", "", false
	}
	for _, rule := range cfg.CollaboratorCIAlerts {
		if strings.EqualFold(rule.Repo, targetRepo) {
			if m, found := rule.Authors[cleanAuthor]; found && strings.TrimSpace(m) != "" {
				return rule.ChannelID, strings.TrimSpace(m), true
			}
		}
	}
	return "", "", false
}

func NormalizeWebhookForwardURLs(raw map[string]string) map[string]string {
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

func NormalizeCollaboratorCIAlerts(rules []CollaboratorCIAlertRule) []CollaboratorCIAlertRule {
	if len(rules) == 0 {
		return nil
	}
	var out []CollaboratorCIAlertRule
	for _, rule := range rules {
		repo := model.NormalizeRepo(rule.Repo)
		chanID := model.NormalizeDiscordTargetID(rule.ChannelID)
		if repo == "" || !model.IsValidDiscordSnowflake(chanID) {
			continue
		}
		cleanedAuthors := make(map[string]string)
		for k, v := range rule.Authors {
			author := strings.ToLower(strings.TrimSpace(k))
			mention := strings.TrimSpace(v)
			if author != "" && mention != "" {
				cleanedAuthors[author] = mention
			}
		}
		if len(cleanedAuthors) == 0 {
			continue
		}
		out = append(out, CollaboratorCIAlertRule{
			Repo:      repo,
			ChannelID: chanID,
			Authors:   cleanedAuthors,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func NormalizeLegacyBuildWorkflows(raw []LegacyBuildWorkflow) ([]LegacyBuildWorkflow, error) {
	out := make([]LegacyBuildWorkflow, 0, len(raw))
	seen := map[LegacyBuildWorkflow]bool{}
	for _, v := range raw {
		v.Repository = strings.ToLower(strings.TrimSpace(v.Repository))
		v.WorkflowPath = strings.TrimSpace(v.WorkflowPath)
		v.BlobSHA = strings.ToLower(strings.TrimSpace(v.BlobSHA))
		if !model.MetadataRepo.MatchString(v.Repository) || !model.ValidWorkflowPath(v.WorkflowPath) || !model.MetadataSHA.MatchString(v.BlobSHA) {
			return nil, fmt.Errorf("invalid legacy build workflow")
		}
		if !seen[v] {
			out = append(out, v)
			seen[v] = true
		}
	}
	return out, nil
}

func resolveRouterYAMLPath(configPath string) string {
	if targetPath := strings.TrimSpace(configPath); targetPath != "" {
		if fi, err := os.Stat(targetPath); err == nil && fi.Size() > 0 {
			return targetPath
		}
	}
	if fi, err := os.Stat(SynapseFallbackConfigPath); err == nil && fi.Size() > 0 {
		return SynapseFallbackConfigPath
	}
	return ""
}

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

func loadAerialConfigFallback(path, sysChan, botID string, autoUpdateAuthors []string, collaboratorAlerts []CollaboratorCIAlertRule) (string, string, []string, []CollaboratorCIAlertRule) {
	data, err := os.ReadFile(path)
	if err != nil && path == "/share/aerial-config/config.yaml" {
		data, err = os.ReadFile("/config/config.yaml")
	}
	if err != nil {
		return sysChan, botID, autoUpdateAuthors, collaboratorAlerts
	}
	var aerialCfg RouterYAMLConfig
	if err := yaml.Unmarshal(data, &aerialCfg); err != nil {
		return sysChan, botID, autoUpdateAuthors, collaboratorAlerts
	}
	if sysChan == "" {
		sysChan = model.FirstNonEmpty(aerialCfg.SystemChannelID, aerialCfg.SystemChannel)
	}
	if botID == "" {
		botID = strings.TrimSpace(aerialCfg.BotID)
	}
	if len(autoUpdateAuthors) == 0 {
		autoUpdateAuthors = model.DedupeTrimmedStrings(aerialCfg.AutoUpdatePRAuthors)
	}
	if len(collaboratorAlerts) == 0 {
		collaboratorAlerts = NormalizeCollaboratorCIAlerts(aerialCfg.CollaboratorCIAlerts)
	}
	return sysChan, botID, autoUpdateAuthors, collaboratorAlerts
}

func buildConfig(fileCfg RouterYAMLConfig) Config {
	legacy, err := NormalizeLegacyBuildWorkflows(fileCfg.LegacyBuildWorkflows)
	if err != nil {
		log.Printf("[Synapse] invalid legacy workflows: %v", err)
	}
	autoUpdateAuthors := model.DedupeTrimmedStrings(fileCfg.AutoUpdatePRAuthors)
	sysChan := model.FirstNonEmpty(fileCfg.SystemChannelID, fileCfg.SystemChannel)
	collaboratorAlerts := NormalizeCollaboratorCIAlerts(fileCfg.CollaboratorCIAlerts)
	var aerialBotID string
	if (sysChan == "" || len(autoUpdateAuthors) == 0 || aerialBotID == "" || len(collaboratorAlerts) == 0) && SynapseAerialConfigPath != "" {
		sysChan, aerialBotID, autoUpdateAuthors, collaboratorAlerts = loadAerialConfigFallback(SynapseAerialConfigPath, sysChan, aerialBotID, autoUpdateAuthors, collaboratorAlerts)
	}

	botID := strings.TrimSpace(model.FirstNonEmpty(os.Getenv("AERIAL_BOT_ID"), fileCfg.BotID, aerialBotID, "aerial"))
	defaultOwner := fileCfg.DefaultOwner
	if envVal := strings.TrimSpace(model.FirstNonEmpty(os.Getenv("AERIAL_DEFAULT_OWNER"), os.Getenv("DEFAULT_OWNER"))); envVal != "" {
		defaultOwner = strings.EqualFold(envVal, "true") || envVal == "1"
	}

	// Delegate local fallback DSN synthesis to httpkit.BuildPostgresDSN so config.go
	// embeds zero plaintext default password literals (enforced by commit-safety-guard).
	pgURL := model.FirstNonEmpty(
		httpkit.BuildPostgresDSN(os.Getenv, "aerial"),
		httpkit.BuildPostgresDSN(func(k string) string {
			if k == "POSTGRES_HOST" {
				return "127.0.0.1"
			}
			return ""
		}, "aerial"),
	)

	kubeServer := os.Getenv("KUBE_API_SERVER")
	if kubeServer == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			kubeServer = "https://" + net.JoinHostPort(host, port)
		}
	}
	kubeToken := os.Getenv("KUBE_TOKEN")

	return Config{
		LegacyBuildWorkflows: legacy,
		Port:                 model.FirstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4020"),
		KubeAPIServer:        strings.TrimRight(kubeServer, "/"),
		KubeToken:            kubeToken,
		GitHubToken:          os.Getenv("GITHUB_PAT"),
		GitHubAPIURL:         strings.TrimRight(model.FirstNonEmpty(os.Getenv("GITHUB_API_URL"), "https://api.github.com"), "/"),
		GitHubWebhookSecret:  strings.TrimSpace(os.Getenv("GITHUB_WEBHOOK_SECRET")),
		PostgresURL:          pgURL,
		FluxReceiverURL:      strings.TrimRight(model.FirstNonEmpty(os.Getenv("FLUX_RECEIVER_URL"), fileCfg.FluxReceiverURL), "/"),
		BrainURL:             strings.TrimRight(model.FirstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL, "http://127.0.0.1:8088"), "/"),
		EriURL:               strings.TrimRight(model.FirstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL, "http://127.0.0.1:8088"), "/"),
		NATSURL:              model.FirstNonEmpty(os.Getenv("NATS_URL"), fileCfg.NATSURL, "nats://nats.aerial:4222"),
		SystemChannelID:      strings.TrimSpace(model.FirstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), sysChan)),
		DiscordToken:         os.Getenv("DISCORD_TOKEN"),
		AutoUpdatePRAuthors:  autoUpdateAuthors,
		BotID:                botID,
		DefaultOwner:         defaultOwner,
		WebhookForwardURLs:   NormalizeWebhookForwardURLs(fileCfg.WebhookForwardURLs),
		CollaboratorCIAlerts: collaboratorAlerts,
	}
}
