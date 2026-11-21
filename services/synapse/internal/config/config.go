package config

import (
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// BotConfig holds configuration for one Discord bot managed by synapse.
type BotConfig struct {
	TokenEnv        string `yaml:"token_env"`
	DiscordToken    string `yaml:"-"`
	EriURL          string `yaml:"eri_url"`
	SystemChannelID string `yaml:"system_channel_id"`
	DefaultOwner    bool   `yaml:"default_owner"`
}

// Config holds runtime configuration for synapse.
type Config struct {
	LegacyBuildWorkflows []LegacyBuildWorkflow
	Port                 string
	KubeAPIServer        string
	KubeToken            string
	GitHubToken          string
	GitHubAPIURL         string
	GitHubCredential     string
	GitHubWebhookSecret  string
	RestateURL           string
	RestatePRService     string
	RestateRepoService   string
	RestateAuthoritative bool
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
	Bots                 map[string]BotConfig
	K8sAuthEnabled       bool
	K8sAuthConfig        k8sauth.Config
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
	LegacyBuildWorkflows []LegacyBuildWorkflow `yaml:"legacy_build_workflows"`
	Port                 string                `yaml:"port"`
	FluxReceiverURL      string                `yaml:"flux_receiver_url"`
	EriURL               string                `yaml:"eri_url"`
	NATSURL              string                `yaml:"nats_url"`
	RestateURL           string                `yaml:"restate_url"`
	RestatePRService     string                `yaml:"restate_pr_service"`
	RestateRepoService   string                `yaml:"restate_repo_service"`
	// RestateAuthoritative flips github/deploy consumer ownership to the
	// Restate PRObject/RepoObject journal. Rollback is a YAML rebind
	// (restate_authoritative: false) with no code revert and no restart:
	// the flag is read live per request, so it is deliberately absent from
	// RestartRequired. The rebind rollback window is one deploy soak
	// (about 48h); it stays valid only until the first Restate-committed
	// external effect, after which rollback is forward-fix only because
	// the journal owns effects legacy state cannot replay.
	RestateAuthoritative bool                      `yaml:"restate_authoritative"`
	SystemChannelID      string                    `yaml:"system_channel_id"`
	SystemChannel        string                    `yaml:"system_channel"`
	AutoUpdatePRAuthors  []string                  `yaml:"auto_update_pr_authors"`
	BotID                string                    `yaml:"bot_id"`
	DefaultOwner         bool                      `yaml:"default_owner"`
	Bots                 map[string]BotConfig      `yaml:"bots"`
	K8sAuthEnabled       bool                      `yaml:"k8s_auth_enabled"`
	K8sAuthConfig        k8sauth.Config            `yaml:"k8s_auth"`
	CollaboratorCIAlerts []CollaboratorCIAlertRule `yaml:"collaborator_ci_alerts"`
	// GitHubCredential picks the credential synapse attaches to GitHub REST
	// calls: "app" (GitHub App installation token, the default when unset)
	// or "pat" (GITHUB_PAT). It is read per request, so an aerial-config PR
	// moves traffic without a restart. With the GITHUB_APP_* env unset,
	// "app" also stays on the PAT. A hot reload carrying any other value is
	// rejected and the last good config stays live.
	GitHubCredential string `yaml:"github_credential"`
}

// GitHub credential choices for github_credential.
const (
	GitHubCredentialApp = "app"
	GitHubCredentialPAT = "pat"
)

// NormalizeGitHubCredential lowercases and trims a github_credential value;
// unset selects app. ok is false for anything other than app or pat.
func NormalizeGitHubCredential(raw string) (string, bool) {
	switch v := strings.ToLower(strings.TrimSpace(raw)); v {
	case "", GitHubCredentialApp:
		return GitHubCredentialApp, true
	case GitHubCredentialPAT:
		return v, true
	default:
		return v, false
	}
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
	if v, ok := NormalizeGitHubCredential(fileCfg.GitHubCredential); !ok {
		return Config{}, fmt.Errorf("github_credential %q is not app or pat", v)
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

const defaultAgentName = "aerial"

// DefaultOwnerAgent returns the agent with DefaultOwner == true, falling back to "aerial".
func (c *Config) DefaultOwnerAgent() string {
	if c == nil || len(c.Bots) == 0 {
		return defaultAgentName
	}
	for agent, bot := range c.Bots {
		if bot.DefaultOwner {
			return agent
		}
	}
	if _, ok := c.Bots[defaultAgentName]; ok {
		return defaultAgentName
	}
	var keys []string
	for k := range c.Bots {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if len(keys) > 0 {
		return keys[0]
	}
	return defaultAgentName
}

func botsEqual(a, b map[string]BotConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va != vb {
			return false
		}
	}
	return true
}

// RestartRequired reports whether moving from old to next changes a
// field that is bound once at startup (listener, database pool, Discord
// gateway session, outbound dispatcher and registry clients) and therefore
// can only take effect through a graceful process restart.
// RestateAuthoritative is deliberately excluded: the consumer cutover and
// its YAML-rebind rollback take effect on the next request without restart.
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
		old.NATSURL != next.NATSURL ||
		old.RestateURL != next.RestateURL ||
		old.RestatePRService != next.RestatePRService ||
		old.RestateRepoService != next.RestateRepoService ||
		old.K8sAuthEnabled != next.K8sAuthEnabled ||
		!botsEqual(old.Bots, next.Bots)
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

// Single Restate PR lifecycle Virtual Objects. Every bot's events address
// these one services; restate_pr_service/restate_repo_service override them
// explicitly and are never derived from the bot ID.
const (
	defaultRestatePRService   = "PRObject"
	defaultRestateRepoService = "RepoObject"
)

func parseBotsConfig(fileCfg RouterYAMLConfig, botID, eriURL, sysChan string, defaultOwner bool) map[string]BotConfig {
	bots := make(map[string]BotConfig)
	if len(fileCfg.Bots) == 0 {
		tokenEnv := "DISCORD_TOKEN"
		if botID != defaultAgentName && botID != "" {
			tokenEnv = "DISCORD_TOKEN_" + strings.ToUpper(botID)
		}
		discordToken := strings.TrimSpace(os.Getenv(tokenEnv))
		if discordToken == "" {
			discordToken = strings.TrimSpace(os.Getenv("DISCORD_TOKEN"))
		}
		bots[botID] = BotConfig{
			TokenEnv:        tokenEnv,
			DiscordToken:    discordToken,
			EriURL:          eriURL,
			SystemChannelID: sysChan,
			DefaultOwner:    defaultOwner,
		}
		return bots
	}

	hasExplicitDefault := false
	for k, bot := range fileCfg.Bots {
		agent := strings.ToLower(strings.TrimSpace(k))
		if agent == "" {
			continue
		}
		tokenEnv := strings.TrimSpace(bot.TokenEnv)
		if tokenEnv == "" {
			if agent == defaultAgentName {
				tokenEnv = "DISCORD_TOKEN"
			} else {
				tokenEnv = "DISCORD_TOKEN_" + strings.ToUpper(agent)
			}
		}
		bot.TokenEnv = tokenEnv
		bot.DiscordToken = strings.TrimSpace(os.Getenv(tokenEnv))
		if agent == defaultAgentName && bot.DiscordToken == "" {
			bot.DiscordToken = strings.TrimSpace(os.Getenv("DISCORD_TOKEN"))
		}
		if bot.EriURL == "" {
			bot.EriURL = eriURL
		} else {
			bot.EriURL = strings.TrimRight(bot.EriURL, "/")
		}
		if bot.DefaultOwner {
			hasExplicitDefault = true
		}
		bots[agent] = bot
	}
	if !hasExplicitDefault && defaultOwner {
		if targetBot, ok := bots[botID]; ok {
			targetBot.DefaultOwner = true
			bots[botID] = targetBot
		} else if targetBot, ok := bots[defaultAgentName]; ok {
			targetBot.DefaultOwner = true
			bots[defaultAgentName] = targetBot
		}
	}
	return bots
}

func resolveK8sAuthConfig(fileCfg RouterYAMLConfig, kubeServer string) (bool, k8sauth.Config) {
	k8sAuthEnabled := fileCfg.K8sAuthEnabled
	if envVal := os.Getenv("K8S_AUTH_ENABLED"); envVal != "" {
		k8sAuthEnabled = strings.EqualFold(envVal, "true") || envVal == "1"
	}
	k8sCfg := fileCfg.K8sAuthConfig
	k8sCfg.Enabled = k8sAuthEnabled
	if k8sCfg.APIServerURL == "" && kubeServer != "" {
		k8sCfg.APIServerURL = strings.TrimRight(kubeServer, "/")
	}
	return k8sAuthEnabled, k8sCfg
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

	botID := strings.TrimSpace(model.FirstNonEmpty(os.Getenv("AERIAL_BOT_ID"), fileCfg.BotID, aerialBotID, defaultAgentName))
	defaultOwner := fileCfg.DefaultOwner
	if envVal := strings.TrimSpace(model.FirstNonEmpty(os.Getenv("AERIAL_DEFAULT_OWNER"), os.Getenv("DEFAULT_OWNER"))); envVal != "" {
		defaultOwner = strings.EqualFold(envVal, "true") || envVal == "1"
	}

	// Delegate local fallback DSN synthesis to httpkit.BuildPostgresDSN so config.go
	// embeds zero plaintext default password literals (enforced by commit-safety-guard).
	pgURL := model.FirstNonEmpty(
		httpkit.BuildPostgresDSN(os.Getenv, defaultAgentName),
		httpkit.BuildPostgresDSN(func(k string) string {
			if k == "POSTGRES_HOST" {
				return "127.0.0.1"
			}
			return ""
		}, defaultAgentName),
	)

	kubeServer := os.Getenv("KUBE_API_SERVER")
	if kubeServer == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			kubeServer = "https://" + net.JoinHostPort(host, port)
		}
	}
	kubeToken := os.Getenv("KUBE_TOKEN")
	eriURL := strings.TrimRight(model.FirstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL, "http://127.0.0.1:8088"), "/")

	bots := parseBotsConfig(fileCfg, botID, eriURL, sysChan, defaultOwner)

	discordToken := os.Getenv("DISCORD_TOKEN")
	if b, ok := bots[botID]; ok && b.DiscordToken != "" {
		discordToken = b.DiscordToken
	} else if b, ok := bots["aerial"]; ok && b.DiscordToken != "" {
		discordToken = b.DiscordToken
	}

	k8sAuthEnabled, k8sCfg := resolveK8sAuthConfig(fileCfg, kubeServer)

	ghCredential, ok := NormalizeGitHubCredential(fileCfg.GitHubCredential)
	if !ok {
		log.Printf("[Synapse] config error: github_credential %q is not app or pat; using app", ghCredential)
		ghCredential = GitHubCredentialApp
	}

	return Config{
		LegacyBuildWorkflows: legacy,
		Port:                 model.FirstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4020"),
		KubeAPIServer:        strings.TrimRight(kubeServer, "/"),
		KubeToken:            kubeToken,
		GitHubToken:          os.Getenv("GITHUB_PAT"),
		GitHubAPIURL:         strings.TrimRight(model.FirstNonEmpty(os.Getenv("GITHUB_API_URL"), "https://api.github.com"), "/"),
		GitHubCredential:     ghCredential,
		GitHubWebhookSecret:  strings.TrimSpace(os.Getenv("GITHUB_WEBHOOK_SECRET")),
		PostgresURL:          pgURL,
		FluxReceiverURL:      strings.TrimRight(model.FirstNonEmpty(os.Getenv("FLUX_RECEIVER_URL"), fileCfg.FluxReceiverURL), "/"),
		BrainURL:             eriURL,
		EriURL:               eriURL,
		NATSURL:              model.FirstNonEmpty(os.Getenv("NATS_URL"), fileCfg.NATSURL, "nats://nats.aerial:4222"),
		RestateURL:           strings.TrimRight(strings.TrimSpace(fileCfg.RestateURL), "/"),
		RestatePRService:     model.FirstNonEmpty(strings.TrimSpace(fileCfg.RestatePRService), defaultRestatePRService),
		RestateRepoService:   model.FirstNonEmpty(strings.TrimSpace(fileCfg.RestateRepoService), defaultRestateRepoService),
		RestateAuthoritative: fileCfg.RestateAuthoritative,
		SystemChannelID:      strings.TrimSpace(model.FirstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), sysChan)),
		DiscordToken:         discordToken,
		AutoUpdatePRAuthors:  autoUpdateAuthors,
		BotID:                botID,
		DefaultOwner:         defaultOwner,
		Bots:                 bots,
		K8sAuthEnabled:       k8sAuthEnabled,
		K8sAuthConfig:        k8sCfg,
		CollaboratorCIAlerts: collaboratorAlerts,
	}
}
