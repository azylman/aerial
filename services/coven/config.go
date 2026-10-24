package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/config.yaml"

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

// DefaultConfigRepoOwner is the default owner of the configuration repository.
const DefaultConfigRepoOwner = "azylman"

// DefaultConfigRepoName is the default repository name of the configuration repository.
const DefaultConfigRepoName = "aerial-config"

// DefaultBrainPRRegisterURL is the default internal brain PR registration endpoint.
const DefaultBrainPRRegisterURL = "http://webhooks-router.aerial/internal/pr/register"

var systemChannelIDRegex = regexp.MustCompile(`^[0-9]{17,20}$`)

var localConfigPath = "/local/config.yaml"

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	if env := strings.TrimSpace(os.Getenv("CONFIG_PATH")); env != "" {
		return env
	}
	if fi, err := os.Stat(localConfigPath); err == nil && fi.Size() > 0 {
		return localConfigPath
	}
	return fallbackPath
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
	BrainLLMURL        string       `yaml:"brain_llm_url"`
	Ollama             OllamaConfig `yaml:"ollama"`
	CustomSkillsDir    string       `yaml:"custom_skills_dir"`
	GitHubPAT          string       `yaml:"github_pat"`
	ConfigRepoOwner    string       `yaml:"config_repo_owner"`
	ConfigRepoName     string       `yaml:"config_repo_name"`
	BrainPRRegisterURL string       `yaml:"brain_pr_register_url"`
	SystemChannelID    string       `yaml:"system_channel_id"`
}

// MemoryYAMLConfig defines the declarative file schema for /config/config.yaml.
type MemoryYAMLConfig struct {
	Port               string       `yaml:"port"`
	DatabaseURL        string       `yaml:"database_url"`
	BrainURL           string       `yaml:"brain_url"`
	BrainLLMURL        string       `yaml:"brain_llm_url"`
	Ollama             OllamaConfig `yaml:"ollama"`
	CustomSkillsDir    string       `yaml:"custom_skills_dir"`
	GitHubPAT          string       `yaml:"github_pat"`
	ConfigRepoOwner    string       `yaml:"config_repo_owner"`
	ConfigRepoName     string       `yaml:"config_repo_name"`
	BrainPRRegisterURL string       `yaml:"brain_pr_register_url"`
	SystemChannelID    string       `yaml:"system_channel_id"`
}

func resolveFirstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func resolveBrainURLs(envURL, yamlURL, envLLM, yamlLLM string) (string, string) {
	brainURL := resolveFirstNonEmpty(envURL, yamlURL)
	brainLLMURL := resolveFirstNonEmpty(envLLM, yamlLLM)

	if brainURL == "" {
		if brainLLMURL != "" {
			brainURL = strings.TrimSuffix(brainLLMURL, "/internal/llm")
		} else {
			brainURL = DefaultBrainURL
		}
	}
	if brainLLMURL == "" {
		brainLLMURL = strings.TrimRight(brainURL, "/") + "/internal/llm"
	}
	return brainURL, brainLLMURL
}

func resolveOllamaConfig(ycfg OllamaConfig) OllamaConfig {
	baseURL := resolveFirstNonEmpty(os.Getenv("OLLAMA_BASE_URL"), ycfg.BaseURL, DefaultOllamaURL)
	model := resolveFirstNonEmpty(os.Getenv("OLLAMA_MODEL"), ycfg.Model, DefaultOllamaModel)
	queryPrefix := os.Getenv("OLLAMA_QUERY_PREFIX")
	if queryPrefix == "" {
		queryPrefix = ycfg.QueryPrefix
	}
	numCtx := ycfg.NumCtx
	if numCtx <= 0 {
		numCtx = DefaultOllamaNumCtx
	}
	return OllamaConfig{
		BaseURL:     baseURL,
		Model:       model,
		QueryPrefix: queryPrefix,
		NumCtx:      numCtx,
	}
}

// LoadConfigFile loads and validates the config from path and environment variables.
// The config file is optional: if path does not exist, defaults and environment variables are used.
func LoadConfigFile(path string, dbURL string) (*Config, error) {
	var ycfg MemoryYAMLConfig

	cleanPath := strings.TrimSpace(path)
	if cleanPath != "" {
		data, err := os.ReadFile(cleanPath)
		if err == nil {
			if err := yaml.Unmarshal(data, &ycfg); err != nil {
				return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
		}
	}

	port := resolveFirstNonEmpty(os.Getenv("PORT"), ycfg.Port, DefaultPort)
	databaseURL := resolveFirstNonEmpty(dbURL, os.Getenv("DATABASE_URL"), os.Getenv("POSTGRES_URL"), ycfg.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	brainURL, brainLLMURL := resolveBrainURLs(
		os.Getenv("BRAIN_URL"), ycfg.BrainURL,
		os.Getenv("BRAIN_LLM_URL"), ycfg.BrainLLMURL,
	)

	customSkillsDir := resolveFirstNonEmpty(os.Getenv("CUSTOM_SKILLS_DIR"), ycfg.CustomSkillsDir, DefaultCustomSkillsDir)
	gitHubPAT := resolveFirstNonEmpty(os.Getenv("GITHUB_PAT"), ycfg.GitHubPAT)
	configRepoOwner := resolveFirstNonEmpty(os.Getenv("CONFIG_REPO_OWNER"), ycfg.ConfigRepoOwner, DefaultConfigRepoOwner)
	configRepoName := resolveFirstNonEmpty(os.Getenv("CONFIG_REPO_NAME"), ycfg.ConfigRepoName, DefaultConfigRepoName)
	brainPRRegisterURL := resolveFirstNonEmpty(os.Getenv("BRAIN_PR_REGISTER_URL"), ycfg.BrainPRRegisterURL, DefaultBrainPRRegisterURL)

	systemChannelID := resolveFirstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), ycfg.SystemChannelID)
	if !systemChannelIDRegex.MatchString(systemChannelID) {
		systemChannelID = ""
	}

	return &Config{
		Port:               port,
		DatabaseURL:        databaseURL,
		BrainURL:           brainURL,
		BrainLLMURL:        brainLLMURL,
		CustomSkillsDir:    customSkillsDir,
		GitHubPAT:          gitHubPAT,
		ConfigRepoOwner:    configRepoOwner,
		ConfigRepoName:     configRepoName,
		BrainPRRegisterURL: brainPRRegisterURL,
		SystemChannelID:    systemChannelID,
		Ollama:             resolveOllamaConfig(ycfg.Ollama),
	}, nil
}
