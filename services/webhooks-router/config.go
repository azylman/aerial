package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds runtime configuration for webhooks-router.
type Config struct {
	Port                  string
	InfisicalURL          string
	InfisicalClientID     string
	InfisicalClientSecret string
	InfisicalProjectID    string
	InfisicalEnvironment  string
	NomadAddr             string
	NomadToken            string
	GitHubToken           string
	GitHubAPIURL          string
	PostgresURL           string
	HangarURL             string
	BrainURL              string
	VanadisURL            string
	HaroURL               string
	SystemChannelID       string
}

// RouterYAMLConfig defines the declarative file schema for services/webhooks-router/webhooks-router.yaml.
type RouterYAMLConfig struct {
	Port                 string `yaml:"port"`
	InfisicalURL         string `yaml:"infisical_url"`
	InfisicalEnvironment string `yaml:"infisical_environment"`
	NomadAddr            string `yaml:"nomad_addr"`
	HangarURL            string `yaml:"hangar_url"`
	BrainURL             string `yaml:"brain_url"`
	VanadisURL           string `yaml:"vanadis_url"`
	HaroURL              string `yaml:"haro_url"`
	SystemChannelID      string `yaml:"system_channel_id"`
	SystemChannel        string `yaml:"system_channel"`
}

var (
	webhooksRouterFallbackConfigPath = "/config/webhooks-router.yaml"
	webhooksRouterAerialConfigPath   = "/config/config.yaml"
)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg RouterYAMLConfig
	targetPath := strings.TrimSpace(configPath)
	if targetPath != "" {
		if fi, err := os.Stat(targetPath); err != nil || fi.Size() == 0 {
			targetPath = ""
		}
	}
	if targetPath == "" {
		if fi, err := os.Stat(webhooksRouterFallbackConfigPath); err == nil && fi.Size() > 0 {
			targetPath = webhooksRouterFallbackConfigPath
		}
	}
	if targetPath != "" {
		if data, err := os.ReadFile(targetPath); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[WebhooksRouter] Warning: failed to parse yaml config at %s: %v", targetPath, err)
			}
		}
	}

	sysChan := firstNonEmpty(fileCfg.SystemChannelID, fileCfg.SystemChannel)
	if sysChan == "" && webhooksRouterAerialConfigPath != "" {
		if data, err := os.ReadFile(webhooksRouterAerialConfigPath); err == nil {
			var aerialCfg RouterYAMLConfig
			if yaml.Unmarshal(data, &aerialCfg) == nil {
				sysChan = firstNonEmpty(aerialCfg.SystemChannelID, aerialCfg.SystemChannel)
			}
		}
	}

	pgURL := firstNonEmpty(os.Getenv("POSTGRES_URL"), os.Getenv("DATABASE_URL"), "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable")

	return Config{
		Port:                  firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4020"),
		InfisicalURL:          strings.TrimRight(firstNonEmpty(os.Getenv("INFISICAL_URL"), os.Getenv("INFISICAL_HOST_URL"), fileCfg.InfisicalURL, "http://infisical.aerial"), "/"),
		InfisicalClientID:     firstNonEmpty(os.Getenv("INFISICAL_CLIENT_ID"), os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID")),
		InfisicalClientSecret: firstNonEmpty(os.Getenv("INFISICAL_CLIENT_SECRET"), os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET")),
		InfisicalProjectID:    firstNonEmpty(os.Getenv("INFISICAL_PROJECT_ID"), os.Getenv("INFISICAL_WORKSPACE_ID")),
		InfisicalEnvironment:  firstNonEmpty(os.Getenv("INFISICAL_ENVIRONMENT"), fileCfg.InfisicalEnvironment, "prod"),
		NomadAddr:             strings.TrimRight(firstNonEmpty(os.Getenv("NOMAD_ADDR"), fileCfg.NomadAddr, "http://127.0.0.1:4646"), "/"),
		NomadToken:            os.Getenv("NOMAD_TOKEN"),
		GitHubToken:           firstNonEmpty(os.Getenv("GITHUB_PAT"), os.Getenv("GITHUB_TOKEN")),
		GitHubAPIURL:          strings.TrimRight(firstNonEmpty(os.Getenv("GITHUB_API_URL"), "https://api.github.com"), "/"),
		PostgresURL:           pgURL,
		HangarURL:             strings.TrimRight(firstNonEmpty(os.Getenv("HANGAR_URL"), fileCfg.HangarURL, "http://127.0.0.1:8087"), "/"),
		BrainURL:              strings.TrimRight(firstNonEmpty(os.Getenv("BRAIN_INTERNAL_URL"), os.Getenv("BRAIN_URL"), fileCfg.BrainURL, "http://127.0.0.1:8088"), "/"),
		VanadisURL:            strings.TrimRight(firstNonEmpty(os.Getenv("VANADIS_URL"), fileCfg.VanadisURL, "http://vanadis.aerial"), "/"),
		HaroURL:               strings.TrimRight(firstNonEmpty(os.Getenv("HARO_URL"), fileCfg.HaroURL, "http://haro.aerial"), "/"),
		SystemChannelID:       strings.TrimSpace(firstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), os.Getenv("DISCORD_CHANNEL_ID"), sysChan)),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
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

// IsHeadlessSessionKey returns true if id is a Model 2 headless dispatch or heartbeat session key
// (e.g. "dispatch-<uuid>", "heartbeat-<id>", or pool-scoped "*:dispatch-<uuid>").
func IsHeadlessSessionKey(id string) bool {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.HasPrefix(clean, "dispatch-") || strings.HasPrefix(clean, "heartbeat-")
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

func isHangarSyncedRepo(repo string) bool {
	norm := strings.ToLower(normalizeRepo(repo))
	return norm == "azylman/aerial" || norm == "azylman/aerial-config"
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
		lines = append(lines, fmt.Sprintf("Objective: %s", obj))
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
