package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/coven.yaml"

// DefaultPort is the fallback port when unspecified.
const DefaultPort = "4008"

// DefaultBrainURL is the fallback internal brain HTTP base URL.
const DefaultBrainURL = "http://eri.aerial"

// DefaultBrainLLMURL is the fallback brain LLM proxy URL.
const DefaultBrainLLMURL = "http://eri.aerial/internal/llm"

// DefaultOllamaURL is the fallback Ollama API base URL.
const DefaultOllamaURL = "http://ollama:11434"

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

// DefaultBrainPRRegisterURL is the default internal brain PR registration endpoint.
const DefaultBrainPRRegisterURL = "http://synapse.aerial/internal/pr/register"

var systemChannelIDRegex = regexp.MustCompile(`^[0-9]{17,20}$`)

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

// Config represents runtime configuration for the memory MCP server.
type Config struct {
	Port               string       `yaml:"port"`
	DatabaseURL        string       `yaml:"database_url"`
	BrainURL           string       `yaml:"brain_url"`
	EriURL             string       `yaml:"eri_url"`
	BrainLLMURL        string       `yaml:"brain_llm_url"`
	EriLLMURL          string       `yaml:"eri_llm_url"`
	Ollama             OllamaConfig `yaml:"ollama"`
	CustomSkillsDir    string       `yaml:"custom_skills_dir"`
	RulesDir           string       `yaml:"rules_dir"`
	GitHubPAT          string       `yaml:"github_pat"`
	ConfigRepoOwner    string       `yaml:"config_repo_owner"`
	ConfigRepoName     string       `yaml:"config_repo_name"`
	BrainPRRegisterURL string       `yaml:"brain_pr_register_url"`
	PRRegisterURL      string       `yaml:"pr_register_url"`
	SystemChannelID    string       `yaml:"system_channel_id"`
}

// MemoryYAMLConfig defines the declarative file schema for /config/config.yaml.
type MemoryYAMLConfig struct {
	Port             string       `yaml:"port"`
	DatabaseURL      string       `yaml:"database_url"`
	EriURL           string       `yaml:"eri_url"`
	EriLLMURL        string       `yaml:"eri_llm_url"`
	Ollama           OllamaConfig `yaml:"ollama"`
	CustomSkillsDir  string       `yaml:"custom_skills_dir"`
	RulesDir         string       `yaml:"rules_dir"`
	GitHubPAT        string       `yaml:"github_pat"`
	ConfigRepoOwner  string       `yaml:"config_repo_owner"`
	ConfigRepoName   string       `yaml:"config_repo_name"`
	PRRegisterURL    string       `yaml:"pr_register_url"`
	EriPRRegisterURL string       `yaml:"eri_pr_register_url"`
	SystemChannelID  string       `yaml:"system_channel_id"`
}

func resolveFirstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// LoadConfig loads configuration from path using DATABASE_URL from the environment.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigFile(path, os.Getenv("DATABASE_URL"))
}

// LoadConfigFile loads and strictly validates configuration from a YAML file and secrets.
// Fails fast if the file is missing, malformed, or required fields are absent.
func LoadConfigFile(path string, dbURL string) (*Config, error) {
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return nil, fmt.Errorf("config: path cannot be empty")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
	}

	var ycfg MemoryYAMLConfig
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return nil, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	eriURL := resolveFirstNonEmpty(os.Getenv("ERI_URL"), ycfg.EriURL)
	if eriURL == "" {
		return nil, fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	eriLLMURL := resolveFirstNonEmpty(os.Getenv("ERI_LLM_URL"), ycfg.EriLLMURL)
	if eriLLMURL == "" {
		return nil, fmt.Errorf("config: eri_llm_url is required in %s", cleanPath)
	}
	prRegisterURL := resolveFirstNonEmpty(os.Getenv("PR_REGISTER_URL"), ycfg.PRRegisterURL, ycfg.EriPRRegisterURL)
	if prRegisterURL == "" {
		return nil, fmt.Errorf("config: pr_register_url is required in %s", cleanPath)
	}
	customSkillsDir := strings.TrimSpace(ycfg.CustomSkillsDir)
	if customSkillsDir == "" {
		return nil, fmt.Errorf("config: custom_skills_dir is required in %s", cleanPath)
	}
	rulesDir := strings.TrimSpace(ycfg.RulesDir)
	if rulesDir == "" {
		return nil, fmt.Errorf("config: rules_dir is required in %s", cleanPath)
	}
	configRepoOwner := strings.TrimSpace(ycfg.ConfigRepoOwner)
	if configRepoOwner == "" {
		return nil, fmt.Errorf("config: config_repo_owner is required in %s", cleanPath)
	}
	configRepoName := strings.TrimSpace(ycfg.ConfigRepoName)
	if configRepoName == "" {
		return nil, fmt.Errorf("config: config_repo_name is required in %s", cleanPath)
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

	databaseURL := resolveFirstNonEmpty(dbURL, os.Getenv("DATABASE_URL"), ycfg.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	gitHubPAT := resolveFirstNonEmpty(os.Getenv("GITHUB_PAT"), ycfg.GitHubPAT)
	systemChannelID := resolveFirstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), ycfg.SystemChannelID)
	if !systemChannelIDRegex.MatchString(systemChannelID) {
		systemChannelID = ""
	}

	return &Config{
		Port:               port,
		DatabaseURL:        databaseURL,
		BrainURL:           eriURL,
		EriURL:             eriURL,
		BrainLLMURL:        eriLLMURL,
		EriLLMURL:          eriLLMURL,
		CustomSkillsDir:    customSkillsDir,
		RulesDir:           rulesDir,
		GitHubPAT:          gitHubPAT,
		ConfigRepoOwner:    configRepoOwner,
		ConfigRepoName:     configRepoName,
		BrainPRRegisterURL: prRegisterURL,
		PRRegisterURL:      prRegisterURL,
		SystemChannelID:    systemChannelID,
		Ollama: OllamaConfig{
			BaseURL:     ollamaBaseURL,
			Model:       ollamaModel,
			QueryPrefix: ycfg.Ollama.QueryPrefix,
			NumCtx:      ycfg.Ollama.NumCtx,
		},
	}, nil
}
