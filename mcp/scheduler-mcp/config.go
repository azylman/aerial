package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/config.yaml"

// DefaultTimezone is the fallback timezone for tool invocations when unspecified in tool payload.
const DefaultTimezone = "America/Los_Angeles"

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

// Config represents the runtime configuration for the scheduler MCP server.
type Config struct {
	DatabaseURL string
	Timezone    string `yaml:"timezone"`
	Port        string `yaml:"port"`
}

// SchedulerYAMLConfig defines the declarative file schema for /config/config.yaml.
type SchedulerYAMLConfig struct {
	Port     string `yaml:"port"`
	Timezone string `yaml:"timezone"`
}

// LoadConfigFile loads and validates the YAML config from path.
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

	var ycfg SchedulerYAMLConfig
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return nil, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	tz := strings.TrimSpace(ycfg.Timezone)
	if tz == "" {
		return nil, fmt.Errorf("config: timezone is required in %s", cleanPath)
	}

	cleanDB := strings.TrimSpace(dbURL)
	if cleanDB == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	return &Config{
		DatabaseURL: cleanDB,
		Timezone:    tz,
		Port:        port,
	}, nil
}
