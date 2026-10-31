package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/haro.yaml"

// DefaultTimezone is the fallback timezone for tool invocations when unspecified in tool payload.
const DefaultTimezone = "America/Los_Angeles"

var (
	defaultConfigPath = "/config/haro.yaml"
	localConfigPath   = "/local/config.yaml"
)

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	if fallbackPath != "" {
		return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, fallbackPath)
	}
	return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, defaultConfigPath, "config.yaml")
}

// Config represents the runtime configuration for the scheduler MCP server.
type Config struct {
	DatabaseURL       string
	Timezone          string   `yaml:"timezone"`
	Port              string   `yaml:"port"`
	BrainURL          string   `yaml:"brain_url"`
	EriURL            string   `yaml:"eri_url"`
	VanadisURL        string   `yaml:"vanadis_url"`
	ExtraEventSources []string `yaml:"extra_event_sources"`
}

// SchedulerYAMLConfig defines the declarative file schema for /config/config.yaml.
type SchedulerYAMLConfig struct {
	Port              string   `yaml:"port"`
	Timezone          string   `yaml:"timezone"`
	BrainURL          string   `yaml:"brain_url"`
	EriURL            string   `yaml:"eri_url"`
	VanadisURL        string   `yaml:"vanadis_url"`
	ExtraEventSources []string `yaml:"extra_event_sources"`
}

// LoadConfig loads configuration from path using DATABASE_URL from the environment.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigFile(path, os.Getenv("DATABASE_URL"))
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

	eriURL := os.Getenv("ERI_URL")
	if eriURL == "" {
		eriURL = os.Getenv("ERI_INTERNAL_URL")
	}
	if eriURL == "" {
		eriURL = os.Getenv("BRAIN_INTERNAL_URL")
	}
	if eriURL == "" {
		eriURL = os.Getenv("BRAIN_URL")
	}
	if eriURL == "" {
		eriURL = ycfg.EriURL
	}
	if eriURL == "" {
		eriURL = ycfg.BrainURL
	}
	eriURL = strings.TrimSpace(eriURL)
	if eriURL == "" {
		eriURL = "http://eri.aerial"
	}
	eriURL = strings.TrimRight(eriURL, "/")

	vanadisURL := os.Getenv("VANADIS_URL")
	if vanadisURL == "" {
		vanadisURL = ycfg.VanadisURL
	}
	vanadisURL = strings.TrimSpace(vanadisURL)
	if vanadisURL == "" {
		vanadisURL = "http://vanadis.aerial"
	}
	vanadisURL = strings.TrimRight(vanadisURL, "/")

	var extraSources []string
	seenSources := make(map[string]struct{}, len(ycfg.ExtraEventSources))
	for _, raw := range ycfg.ExtraEventSources {
		if s := strings.ToLower(strings.TrimSpace(raw)); s != "" {
			if _, ok := seenSources[s]; !ok {
				seenSources[s] = struct{}{}
				extraSources = append(extraSources, s)
			}
		}
	}

	return &Config{
		DatabaseURL:       cleanDB,
		Timezone:          tz,
		Port:              port,
		BrainURL:          eriURL,
		EriURL:            eriURL,
		VanadisURL:        vanadisURL,
		ExtraEventSources: extraSources,
	}, nil
}
