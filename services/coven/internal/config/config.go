package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/coven.yaml"

// DefaultPort is the fallback port when unspecified.
const DefaultPort = "4008"

// DefaultRestatePort is the fallback port for Restate HTTP/2 service.
const DefaultRestatePort = "9080"

// DefaultRestateIngressURL is the default ingress URL for Restate cluster.
const DefaultRestateIngressURL = "http://restate.aerial:8080"

// DefaultRestateAdminURL is the default admin URL for Restate cluster.
const DefaultRestateAdminURL = "http://restate.aerial:9070"

// DefaultRestateAdvertiseURL is the default advertisement URL for Coven to Restate.
const DefaultRestateAdvertiseURL = "http://coven.default.svc.cluster.local:9080"

// DefaultOllamaModel is the default embedding model.
const DefaultOllamaModel = "bge-small-en-v1.5"

// DefaultOllamaNumCtx is the default context length for embedding generation.
const DefaultOllamaNumCtx = 512

// DefaultCustomSkillsDir is the default path to scanned custom skills.
const DefaultCustomSkillsDir = "/config/custom-skills"

// DefaultConfigRepoOwner is the default owner of the configuration repository.
const DefaultConfigRepoOwner = "azylman"

// DefaultConfigRepoName is the default repository name of the configuration repository.
const DefaultConfigRepoName = "aerial-config"

var systemChannelIDRegex = regexp.MustCompile(`^\d{17,20}$`)

var localConfigPath = "/local/config.yaml"

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	if fallbackPath != "" {
		return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, fallbackPath)
	}
	return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, DefaultConfigPath, "config.yaml")
}

// OllamaConfig defines Ollama embedding client settings.
type OllamaConfig struct {
	BaseURL     string `yaml:"base_url"`
	Model       string `yaml:"model"`
	QueryPrefix string `yaml:"query_prefix"`
	NumCtx      int    `yaml:"num_ctx"`
}

// DefaultStack is the stack every coven serves when no stack is configured (K8s N2).
const DefaultStack = "aerial"

// DefaultExtractTimeout is the fallback per-thread extraction deadline.
const DefaultExtractTimeout = 120 * time.Second

// DefaultExtractMaxSteps caps one extraction batch before timeout-driven halving.
const DefaultExtractMaxSteps = 50

// Config represents runtime configuration for the memory MCP server.
type Config struct {
	Port                  string         `yaml:"port"`
	RestatePort           string         `yaml:"restate_port"`
	RestateIngressURL     string         `yaml:"restate_ingress_url"`
	RestateAdminURL       string         `yaml:"restate_admin_url"`
	RestateAdvertiseURL   string         `yaml:"restate_advertise_url"`
	DatabaseURL           string         `yaml:"database_url"`
	EriURL                string         `yaml:"eri_url"`
	EriLLMURL             string         `yaml:"eri_llm_url"`
	Ollama                OllamaConfig   `yaml:"ollama"`
	CustomSkillsDir       string         `yaml:"custom_skills_dir"`
	GitHubPAT             string         `yaml:"github_pat"`
	ConfigRepoOwner       string         `yaml:"config_repo_owner"`
	ConfigRepoName        string         `yaml:"config_repo_name"`
	PRRegisterURL         string         `yaml:"pr_register_url"`
	SystemChannelID       string         `yaml:"system_channel_id"`
	NATSURL               string         `yaml:"nats_url"`
	Stack                 string         `yaml:"stack"`
	Role                  string         `yaml:"role"`
	TranscriptsURL        string         `yaml:"transcripts_url"`
	ExtractTimeoutSeconds int            `yaml:"extract_timeout_seconds"`
	ExtractMaxSteps       int            `yaml:"extract_max_steps"`
	Auth                  k8sauth.Config `yaml:"auth"`
}

// NormalizeStack trims s and defaults empty stacks to DefaultStack.
func NormalizeStack(s string) string {
	if trimmed := strings.TrimSpace(s); trimmed != "" {
		return trimmed
	}
	return DefaultStack
}

// ScopedThreadKey namespaces a thread under its stack for watermark reads.
func ScopedThreadKey(stack, threadID string) string {
	return NormalizeStack(stack) + "/" + strings.TrimSpace(threadID)
}

// ParseScopedThreadKey splits a scoped thread key into (stack, threadID).
func ParseScopedThreadKey(key string) (string, string) {
	key = strings.TrimSpace(key)
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 2 {
		return NormalizeStack(parts[0]), strings.TrimSpace(parts[1])
	}
	return DefaultStack, key
}

// EffectiveStack returns the configured stack, defaulting to DefaultStack.
func (c *Config) EffectiveStack() string {
	if c == nil {
		return DefaultStack
	}
	return NormalizeStack(c.Stack)
}

// ExtractTimeout returns the configured per-thread extraction deadline.
func (c *Config) ExtractTimeout() time.Duration {
	if c == nil || c.ExtractTimeoutSeconds <= 0 {
		return DefaultExtractTimeout
	}
	return time.Duration(c.ExtractTimeoutSeconds) * time.Second
}

// ExtractBatchLimit returns the configured max steps per extraction batch.
func (c *Config) ExtractBatchLimit() int {
	if c == nil || c.ExtractMaxSteps <= 0 {
		return DefaultExtractMaxSteps
	}
	return c.ExtractMaxSteps
}

// resolvePositiveInt prefers a positive env override, falling back to the YAML value.
func resolvePositiveInt(env string, yamlVal int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(env)); err == nil && n > 0 {
		return n
	}
	return yamlVal
}

// LoadConfig loads configuration from path using the database DSN built from getenv.
func LoadConfig(path string, getenv func(string) string) (*Config, error) {
	return LoadConfigFile(path, httpkit.BuildPostgresDSN(getenv, "aerial"), getenv)
}

// LoadConfigFile loads and strictly validates configuration from a YAML file and secrets.
// Fails fast if the file is missing, malformed, or required fields are absent.
func LoadConfigFile(path string, dbURL string, getenv func(string) string) (*Config, error) {
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return nil, fmt.Errorf("config: path cannot be empty")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
	}

	var ycfg struct {
		Port                string         `yaml:"port"`
		RestatePort         string         `yaml:"restate_port"`
		RestateIngressURL   string         `yaml:"restate_ingress_url"`
		RestateAdminURL     string         `yaml:"restate_admin_url"`
		RestateAdvertiseURL string         `yaml:"restate_advertise_url"`
		DatabaseURL         string         `yaml:"database_url"`
		EriURL              string         `yaml:"eri_url"`
		EriLLMURL           string         `yaml:"eri_llm_url"`
		Ollama              OllamaConfig   `yaml:"ollama"`
		CustomSkillsDir     string         `yaml:"custom_skills_dir"`
		GitHubPAT           string         `yaml:"github_pat"`
		ConfigRepoOwner     string         `yaml:"config_repo_owner"`
		ConfigRepoName      string         `yaml:"config_repo_name"`
		PRRegisterURL       string         `yaml:"pr_register_url"`
		EriPRRegisterURL    string         `yaml:"eri_pr_register_url"`
		SystemChannelID     string         `yaml:"system_channel_id"`
		NATSURL             string         `yaml:"nats_url"`
		Stack               string         `yaml:"stack"`
		Role                string         `yaml:"role"`
		TranscriptsURL      string         `yaml:"transcripts_url"`
		IndexerURL          string         `yaml:"indexer_url"`
		ExtractTimeout      int            `yaml:"extract_timeout_seconds"`
		ExtractMaxSteps     int            `yaml:"extract_max_steps"`
		Auth                k8sauth.Config `yaml:"auth"`
	}
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return nil, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	eriURL := strutil.FirstNonEmpty(getenv("ERI_URL"), ycfg.EriURL)
	if eriURL == "" {
		return nil, fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	eriLLMURL := strutil.FirstNonEmpty(getenv("ERI_LLM_URL"), ycfg.EriLLMURL)
	if eriLLMURL == "" {
		return nil, fmt.Errorf("config: eri_llm_url is required in %s", cleanPath)
	}
	role := strutil.FirstNonEmpty(getenv("COVEN_ROLE"), ycfg.Role)
	stack := strutil.FirstNonEmpty(getenv("COVEN_STACK"), getenv("STACK"), ycfg.Stack)

	prRegisterURL := strutil.FirstNonEmpty(getenv("PR_REGISTER_URL"), getenv("SYNAPSE_PR_REGISTER_URL"), ycfg.PRRegisterURL, ycfg.EriPRRegisterURL)
	customSkillsDir := strings.TrimSpace(ycfg.CustomSkillsDir)
	configRepoOwner := strings.TrimSpace(ycfg.ConfigRepoOwner)
	configRepoName := strings.TrimSpace(ycfg.ConfigRepoName)

	if err := validateSkillSynthesisConfig(role, prRegisterURL, customSkillsDir, configRepoOwner, configRepoName, cleanPath); err != nil {
		return nil, err
	}
	ollamaBaseURL := strings.TrimSpace(ycfg.Ollama.BaseURL)
	if ollamaBaseURL == "" {
		return nil, fmt.Errorf("config: ollama.base_url is required in %s", cleanPath)
	}
	ollamaModel := strings.TrimSpace(ycfg.Ollama.Model)
	if ollamaModel == "" {
		return nil, fmt.Errorf("config: ollama.model is required in %s", cleanPath)
	}
	if ycfg.Ollama.NumCtx <= 0 {
		return nil, fmt.Errorf("config: ollama.num_ctx must be positive in %s", cleanPath)
	}

	databaseURL := strutil.FirstNonEmpty(dbURL, httpkit.BuildPostgresDSN(getenv, "aerial"), ycfg.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	gitHubPAT := strutil.FirstNonEmpty(getenv("GITHUB_PAT"), ycfg.GitHubPAT)
	systemChannelID := strutil.FirstNonEmpty(getenv("SYSTEM_CHANNEL_ID"), ycfg.SystemChannelID)
	if !systemChannelIDRegex.MatchString(systemChannelID) {
		systemChannelID = ""
	}
	natsURL := strutil.FirstNonEmpty(getenv("NATS_URL"), ycfg.NATSURL)
	transcriptsURL := strutil.FirstNonEmpty(getenv("TRANSCRIPTS_URL"), ycfg.TranscriptsURL, ycfg.IndexerURL)
	if transcriptsURL == "" && NormalizeStack(stack) == DefaultStack {
		transcriptsURL = "http://transcripts.aerial"
	}

	authCfg := ycfg.Auth
	if err := k8sauth.ApplyEnvOverride(&authCfg, getenv); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	restatePort := strutil.FirstNonEmpty(getenv("RESTATE_PORT"), ycfg.RestatePort, DefaultRestatePort)
	restateIngressURL := strutil.FirstNonEmpty(getenv("RESTATE_INGRESS_URL"), ycfg.RestateIngressURL, DefaultRestateIngressURL)
	restateAdminURL := strutil.FirstNonEmpty(getenv("RESTATE_ADMIN_URL"), ycfg.RestateAdminURL, DefaultRestateAdminURL)
	restateAdvertiseURL := strutil.FirstNonEmpty(getenv("RESTATE_ADVERTISE_URL"), ycfg.RestateAdvertiseURL, DefaultRestateAdvertiseURL)

	return &Config{
		Port:                  port,
		RestatePort:           strings.TrimSpace(restatePort),
		RestateIngressURL:     strings.TrimSpace(restateIngressURL),
		RestateAdminURL:       strings.TrimSpace(restateAdminURL),
		RestateAdvertiseURL:   strings.TrimSpace(restateAdvertiseURL),
		DatabaseURL:           databaseURL,
		EriURL:                eriURL,
		EriLLMURL:             eriLLMURL,
		CustomSkillsDir:       customSkillsDir,
		GitHubPAT:             gitHubPAT,
		ConfigRepoOwner:       configRepoOwner,
		ConfigRepoName:        configRepoName,
		PRRegisterURL:         prRegisterURL,
		SystemChannelID:       systemChannelID,
		NATSURL:               natsURL,
		Stack:                 strings.TrimSpace(stack),
		Role:                  strings.TrimSpace(role),
		TranscriptsURL:        strings.TrimSpace(transcriptsURL),
		ExtractTimeoutSeconds: resolvePositiveInt(getenv("EXTRACT_TIMEOUT_SECONDS"), ycfg.ExtractTimeout),
		ExtractMaxSteps:       resolvePositiveInt(getenv("EXTRACT_MAX_STEPS"), ycfg.ExtractMaxSteps),
		Ollama: OllamaConfig{
			BaseURL:     ollamaBaseURL,
			Model:       ollamaModel,
			QueryPrefix: ycfg.Ollama.QueryPrefix,
			NumCtx:      ycfg.Ollama.NumCtx,
		},
		Auth: authCfg,
	}, nil
}

func validateSkillSynthesisConfig(role, prRegisterURL, customSkillsDir, configRepoOwner, configRepoName, cleanPath string) error {
	if strings.EqualFold(role, "extractor") {
		return nil
	}
	if prRegisterURL == "" {
		return fmt.Errorf("config: pr_register_url is required in %s", cleanPath)
	}
	if customSkillsDir == "" {
		return fmt.Errorf("config: custom_skills_dir is required in %s", cleanPath)
	}
	if configRepoOwner == "" {
		return fmt.Errorf("config: config_repo_owner is required in %s", cleanPath)
	}
	if configRepoName == "" {
		return fmt.Errorf("config: config_repo_name is required in %s", cleanPath)
	}
	return nil
}
