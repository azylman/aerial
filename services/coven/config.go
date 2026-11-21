package main

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
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/coven.yaml"

// DefaultPort is the fallback port when unspecified.
const DefaultPort = "4008"

// DefaultOllamaModel is the default embedding model.
const DefaultOllamaModel = "all-minilm"

// DefaultOllamaNumCtx is the default context length for embedding generation.
const DefaultOllamaNumCtx = 512

// DefaultCustomSkillsDir is the default path to scanned custom skills.
const DefaultCustomSkillsDir = "/config/custom-skills"

// DefaultRulesDir is the default path to configuration rules.
const DefaultRulesDir = "/config/rules"

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
	DatabaseURL           string         `yaml:"database_url"`
	EriURL                string         `yaml:"eri_url"`
	EriLLMURL             string         `yaml:"eri_llm_url"`
	Ollama                OllamaConfig   `yaml:"ollama"`
	CustomSkillsDir       string         `yaml:"custom_skills_dir"`
	RulesDir              string         `yaml:"rules_dir"`
	GitHubPAT             string         `yaml:"github_pat"`
	ConfigRepoOwner       string         `yaml:"config_repo_owner"`
	ConfigRepoName        string         `yaml:"config_repo_name"`
	PRRegisterURL         string         `yaml:"pr_register_url"`
	SystemChannelID       string         `yaml:"system_channel_id"`
	NATSURL               string         `yaml:"nats_url"`
	Stack                 string         `yaml:"stack"`
	Role                  string         `yaml:"role"`
	TranscriptsURL        string         `yaml:"transcripts_url"`
	LLMEffort             string         `yaml:"llm_effort"`
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

func resolveFirstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
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
		Port             string         `yaml:"port"`
		DatabaseURL      string         `yaml:"database_url"`
		EriURL           string         `yaml:"eri_url"`
		EriLLMURL        string         `yaml:"eri_llm_url"`
		Ollama           OllamaConfig   `yaml:"ollama"`
		CustomSkillsDir  string         `yaml:"custom_skills_dir"`
		RulesDir         string         `yaml:"rules_dir"`
		GitHubPAT        string         `yaml:"github_pat"`
		ConfigRepoOwner  string         `yaml:"config_repo_owner"`
		ConfigRepoName   string         `yaml:"config_repo_name"`
		PRRegisterURL    string         `yaml:"pr_register_url"`
		EriPRRegisterURL string         `yaml:"eri_pr_register_url"`
		SystemChannelID  string         `yaml:"system_channel_id"`
		NATSURL          string         `yaml:"nats_url"`
		Stack            string         `yaml:"stack"`
		Role             string         `yaml:"role"`
		TranscriptsURL   string         `yaml:"transcripts_url"`
		IndexerURL       string         `yaml:"indexer_url"`
		LLMEffort        string         `yaml:"llm_effort"`
		ExtractTimeout   int            `yaml:"extract_timeout_seconds"`
		ExtractMaxSteps  int            `yaml:"extract_max_steps"`
		Auth             k8sauth.Config `yaml:"auth"`
	}
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return nil, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	eriURL := resolveFirstNonEmpty(getenv("ERI_URL"), ycfg.EriURL)
	if eriURL == "" {
		return nil, fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	eriLLMURL := resolveFirstNonEmpty(getenv("ERI_LLM_URL"), ycfg.EriLLMURL)
	if eriLLMURL == "" {
		return nil, fmt.Errorf("config: eri_llm_url is required in %s", cleanPath)
	}
	role := resolveFirstNonEmpty(getenv("COVEN_ROLE"), ycfg.Role)
	stack := resolveFirstNonEmpty(getenv("COVEN_STACK"), getenv("STACK"), ycfg.Stack)

	prRegisterURL := resolveFirstNonEmpty(getenv("PR_REGISTER_URL"), getenv("SYNAPSE_PR_REGISTER_URL"), ycfg.PRRegisterURL, ycfg.EriPRRegisterURL)
	customSkillsDir := strings.TrimSpace(ycfg.CustomSkillsDir)
	rulesDir := strings.TrimSpace(ycfg.RulesDir)
	configRepoOwner := strings.TrimSpace(ycfg.ConfigRepoOwner)
	configRepoName := strings.TrimSpace(ycfg.ConfigRepoName)

	if err := validateSkillSynthesisConfig(role, prRegisterURL, customSkillsDir, rulesDir, configRepoOwner, configRepoName, cleanPath); err != nil {
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

	databaseURL := resolveFirstNonEmpty(dbURL, httpkit.BuildPostgresDSN(getenv, "aerial"), ycfg.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	gitHubPAT := resolveFirstNonEmpty(getenv("GITHUB_PAT"), ycfg.GitHubPAT)
	systemChannelID := resolveFirstNonEmpty(getenv("SYSTEM_CHANNEL_ID"), ycfg.SystemChannelID)
	if !systemChannelIDRegex.MatchString(systemChannelID) {
		systemChannelID = ""
	}
	natsURL := resolveFirstNonEmpty(getenv("NATS_URL"), ycfg.NATSURL)
	transcriptsURL := resolveFirstNonEmpty(getenv("TRANSCRIPTS_URL"), ycfg.TranscriptsURL, ycfg.IndexerURL)
	if transcriptsURL == "" && NormalizeStack(stack) == DefaultStack {
		transcriptsURL = "http://transcripts.aerial"
	}
	llmEffort := resolveFirstNonEmpty(getenv("LLM_EFFORT"), ycfg.LLMEffort)

	authCfg := ycfg.Auth
	if envVal := strings.TrimSpace(getenv("K8S_AUTH_ENABLED")); envVal != "" {
		if enabled, err := strconv.ParseBool(envVal); err == nil {
			authCfg.Enabled = enabled
		}
	}

	return &Config{
		Port:                  port,
		DatabaseURL:           databaseURL,
		EriURL:                eriURL,
		EriLLMURL:             eriLLMURL,
		CustomSkillsDir:       customSkillsDir,
		RulesDir:              rulesDir,
		GitHubPAT:             gitHubPAT,
		ConfigRepoOwner:       configRepoOwner,
		ConfigRepoName:        configRepoName,
		PRRegisterURL:         prRegisterURL,
		SystemChannelID:       systemChannelID,
		NATSURL:               natsURL,
		Stack:                 strings.TrimSpace(stack),
		Role:                  strings.TrimSpace(role),
		TranscriptsURL:        strings.TrimSpace(transcriptsURL),
		LLMEffort:             strings.TrimSpace(llmEffort),
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

func validateSkillSynthesisConfig(role, prRegisterURL, customSkillsDir, rulesDir, configRepoOwner, configRepoName, cleanPath string) error {
	if strings.EqualFold(role, "extractor") {
		return nil
	}
	if prRegisterURL == "" {
		return fmt.Errorf("config: pr_register_url is required in %s", cleanPath)
	}
	if customSkillsDir == "" {
		return fmt.Errorf("config: custom_skills_dir is required in %s", cleanPath)
	}
	if rulesDir == "" {
		return fmt.Errorf("config: rules_dir is required in %s", cleanPath)
	}
	if configRepoOwner == "" {
		return fmt.Errorf("config: config_repo_owner is required in %s", cleanPath)
	}
	if configRepoName == "" {
		return fmt.Errorf("config: config_repo_name is required in %s", cleanPath)
	}
	return nil
}
