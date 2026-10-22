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
const DefaultBrainPRRegisterURL = "http://eri.aerial/internal/pr/register"

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
	BrainLLMURL        string       `yaml:"brain_llm_url"`
	Ollama             OllamaConfig `yaml:"ollama"`
	CustomSkillsDir    string       `yaml:"custom_skills_dir"`
	GitHubPAT          string       `yaml:"github_pat"`
	ConfigRepoOwner    string       `yaml:"config_repo_owner"`
	ConfigRepoName     string       `yaml:"config_repo_name"`
	BrainPRRegisterURL string       `yaml:"brain_pr_register_url"`
	SystemChannelID    string       `yaml:"system_channel_id"`
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

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = strings.TrimSpace(ycfg.Port)
	}
	if port == "" {
		port = DefaultPort
	}

	databaseURL := strings.TrimSpace(dbURL)
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(os.Getenv("POSTGRES_URL"))
	}
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(ycfg.DatabaseURL)
	}
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	brainLLMURL := strings.TrimSpace(os.Getenv("BRAIN_LLM_URL"))
	if brainLLMURL == "" {
		brainLLMURL = strings.TrimSpace(ycfg.BrainLLMURL)
	}
	if brainLLMURL == "" {
		brainLLMURL = DefaultBrainLLMURL
	}

	ollamaBaseURL := strings.TrimSpace(os.Getenv("OLLAMA_BASE_URL"))
	if ollamaBaseURL == "" {
		ollamaBaseURL = strings.TrimSpace(ycfg.Ollama.BaseURL)
	}
	if ollamaBaseURL == "" {
		ollamaBaseURL = DefaultOllamaURL
	}

	ollamaModel := strings.TrimSpace(os.Getenv("OLLAMA_MODEL"))
	if ollamaModel == "" {
		ollamaModel = strings.TrimSpace(ycfg.Ollama.Model)
	}
	if ollamaModel == "" {
		ollamaModel = DefaultOllamaModel
	}

	queryPrefix := os.Getenv("OLLAMA_QUERY_PREFIX")
	if queryPrefix == "" {
		queryPrefix = ycfg.Ollama.QueryPrefix
	}

	numCtx := ycfg.Ollama.NumCtx
	if numCtx <= 0 {
		numCtx = DefaultOllamaNumCtx
	}

	customSkillsDir := strings.TrimSpace(os.Getenv("CUSTOM_SKILLS_DIR"))
	if customSkillsDir == "" {
		customSkillsDir = strings.TrimSpace(ycfg.CustomSkillsDir)
	}
	if customSkillsDir == "" {
		customSkillsDir = DefaultCustomSkillsDir
	}

	gitHubPAT := strings.TrimSpace(os.Getenv("GITHUB_PAT"))
	if gitHubPAT == "" {
		gitHubPAT = strings.TrimSpace(ycfg.GitHubPAT)
	}

	configRepoOwner := strings.TrimSpace(os.Getenv("CONFIG_REPO_OWNER"))
	if configRepoOwner == "" {
		configRepoOwner = strings.TrimSpace(ycfg.ConfigRepoOwner)
	}
	if configRepoOwner == "" {
		configRepoOwner = DefaultConfigRepoOwner
	}

	configRepoName := strings.TrimSpace(os.Getenv("CONFIG_REPO_NAME"))
	if configRepoName == "" {
		configRepoName = strings.TrimSpace(ycfg.ConfigRepoName)
	}
	if configRepoName == "" {
		configRepoName = DefaultConfigRepoName
	}

	brainPRRegisterURL := strings.TrimSpace(os.Getenv("BRAIN_PR_REGISTER_URL"))
	if brainPRRegisterURL == "" {
		brainPRRegisterURL = strings.TrimSpace(ycfg.BrainPRRegisterURL)
	}
	if brainPRRegisterURL == "" {
		brainPRRegisterURL = DefaultBrainPRRegisterURL
	}

	systemChannelID := strings.TrimSpace(os.Getenv("SYSTEM_CHANNEL_ID"))
	if systemChannelID == "" {
		systemChannelID = strings.TrimSpace(ycfg.SystemChannelID)
	}
	if !systemChannelIDRegex.MatchString(systemChannelID) {
		systemChannelID = ""
	}

	return &Config{
		Port:               port,
		DatabaseURL:        databaseURL,
		BrainLLMURL:        brainLLMURL,
		CustomSkillsDir:    customSkillsDir,
		GitHubPAT:          gitHubPAT,
		ConfigRepoOwner:    configRepoOwner,
		ConfigRepoName:     configRepoName,
		BrainPRRegisterURL: brainPRRegisterURL,
		SystemChannelID:    systemChannelID,
		Ollama: OllamaConfig{
			BaseURL:     ollamaBaseURL,
			Model:       ollamaModel,
			QueryPrefix: queryPrefix,
			NumCtx:      numCtx,
		},
	}, nil
}
