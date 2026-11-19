package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/transcripts.yaml"

// DefaultPort is the fallback port when unspecified.
const DefaultPort = "4017"

// DefaultStack is the stack this indexer writes when the config omits one.
const DefaultStack = "aerial"

// Config represents runtime configuration for the transcripts indexer.
type Config struct {
	Port        string           `yaml:"port"`
	Stack       string           `yaml:"stack"`
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
		Port        string           `yaml:"port"`
		Stack       string           `yaml:"stack"`
		DatabaseURL string           `yaml:"database_url"`
		NATSURL     string           `yaml:"nats_url"`
		Summarizer  SummarizerConfig `yaml:"summarizer"`
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
	databaseURL := resolveFirstNonEmpty(dbURL, httpkit.BuildPostgresDSN(os.Getenv, "aerial"), ycfg.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}
	natsURL := resolveFirstNonEmpty(os.Getenv("NATS_URL"), ycfg.NATSURL)
	return &Config{
		Port:        port,
		Stack:       stack,
		DatabaseURL: databaseURL,
		NATSURL:     natsURL,
		Summarizer:  ycfg.Summarizer.withDefaults(),
	}, nil
}
