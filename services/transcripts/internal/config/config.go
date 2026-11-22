package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/transcripts.yaml"

// DefaultPort is the fallback port when unspecified.
const DefaultPort = "4017"

// DefaultStack is the stack this indexer writes when the config omits one.
const DefaultStack = "aerial"

// DefaultSummarizerBatchLimit caps sessions summarized per RunOnce sweep.
const DefaultSummarizerBatchLimit = 10

// DefaultSummarizerMaxSteps caps stored steps folded into one summary prompt.
const DefaultSummarizerMaxSteps = 50

// DefaultSummarizerIdleThreshold bounds how recently a session must have been updated to be considered active.
const DefaultSummarizerIdleThreshold = 10 * time.Minute

// DefaultSummarizerMinStepDelta is the minimum number of new steps required before re-summarizing an already-summarized session.
const DefaultSummarizerMinStepDelta = 20

// SummarizerConfig tunes the dark-by-default per-stack idle summarizer.
// Enabled defaults to false; the indexer ships dark until the summaries
// cutover chunk flips it on.
type SummarizerConfig struct {
	Enabled       bool          `yaml:"enabled"`
	BatchLimit    int           `yaml:"batch_limit"`
	MaxSteps      int           `yaml:"max_steps"`
	IdleThreshold time.Duration `yaml:"idle_threshold"`
	MinStepDelta  int           `yaml:"min_step_delta"`
	EriLLMURL     string        `yaml:"eri_llm_url"`
	SensesURL     string        `yaml:"senses_url"`
	SensesModel   string        `yaml:"senses_model"`
}

// WithDefaults fills non-positive sweep bounds; Enabled stays as configured.
func (c SummarizerConfig) WithDefaults() SummarizerConfig {
	if c.BatchLimit <= 0 {
		c.BatchLimit = DefaultSummarizerBatchLimit
	}
	if c.MaxSteps <= 0 {
		c.MaxSteps = DefaultSummarizerMaxSteps
	}
	if c.IdleThreshold <= 0 {
		c.IdleThreshold = DefaultSummarizerIdleThreshold
	}
	if c.MinStepDelta <= 0 {
		c.MinStepDelta = DefaultSummarizerMinStepDelta
	}
	return c
}

// Config represents runtime configuration for the transcripts indexer.
type Config struct {
	Port        string           `yaml:"port"`
	Stack       string           `yaml:"stack"`
	Auth        k8sauth.Config   `yaml:"auth"`
	DatabaseURL string           `yaml:"database_url"`
	NATSURL     string           `yaml:"nats_url"`
	Summarizer  SummarizerConfig `yaml:"summarizer"`
}

var localConfigPath = "/local/config.yaml"

// ResolveConfigPath returns the config file path, checking CONFIG_PATH first.
func ResolveConfigPath(fallbackPath string) string {
	if strings.TrimSpace(fallbackPath) != "" {
		return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, fallbackPath)
	}
	return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, DefaultConfigPath, "config.yaml")
}

// LoadConfig loads configuration from path using DATABASE_URL from the environment.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigFile(path, httpkit.BuildPostgresDSN(os.Getenv, "aerial"))
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
	var ycfg struct {
		Port        string         `yaml:"port"`
		Stack       string         `yaml:"stack"`
		Auth        k8sauth.Config `yaml:"auth"`
		DatabaseURL string         `yaml:"database_url"`
		NATSURL     string         `yaml:"nats_url"`
		EriLLMURL   string         `yaml:"eri_llm_url"`
		Ollama      struct {
			BaseURL string `yaml:"base_url"`
			Model   string `yaml:"model"`
		} `yaml:"ollama"`
		Embedder struct {
			BaseURL string `yaml:"base_url"`
			Model   string `yaml:"model"`
		} `yaml:"embedder"`
		Summarizer SummarizerConfig `yaml:"summarizer"`
	}
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}
	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		port = DefaultPort
	}
	stack := strings.TrimSpace(ycfg.Stack)
	if stack == "" {
		stack = DefaultStack
	}
	auth := ycfg.Auth
	if err := k8sauth.ApplyEnvOverride(&auth, os.Getenv); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	databaseURL := strutil.FirstNonEmpty(dbURL, httpkit.BuildPostgresDSN(os.Getenv, "aerial"), ycfg.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}
	natsURL := strutil.FirstNonEmpty(os.Getenv("NATS_URL"), ycfg.NATSURL)
	summarizer := ycfg.Summarizer.WithDefaults()
	if strings.TrimSpace(summarizer.EriLLMURL) == "" {
		summarizer.EriLLMURL = strings.TrimSpace(ycfg.EriLLMURL)
	}
	if strings.TrimSpace(summarizer.SensesURL) == "" {
		summarizer.SensesURL = strutil.FirstNonEmpty(ycfg.Embedder.BaseURL, ycfg.Ollama.BaseURL)
	}
	if strings.TrimSpace(summarizer.SensesModel) == "" {
		summarizer.SensesModel = strutil.FirstNonEmpty(ycfg.Embedder.Model, ycfg.Ollama.Model)
	}
	return &Config{
		Port:        port,
		Stack:       stack,
		Auth:        auth,
		DatabaseURL: databaseURL,
		NATSURL:     natsURL,
		Summarizer:  summarizer,
	}, nil
}
