// Package config holds Vanadis runtime configuration types and loaders.
package config

import (
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
)

// FallbackConfigPath is the default YAML config path when CONFIG_PATH is unset.
var FallbackConfigPath = "/config/vanadis.yaml"

// Config holds runtime configuration for the Vanadis orchestrator service.
type Config struct {
	Port         string
	PostgresURL  string
	BrainURL     string
	EriURL       string
	NATSURL      string
	GitHubAPIURL string
	GitHubToken  string
	ConfigPath   string
	DeployUIURL  string
}

// VanadisYAMLConfig defines the declarative file schema for /config/vanadis.yaml.
type VanadisYAMLConfig struct {
	Port         string `yaml:"port"`
	PostgresURL  string `yaml:"postgres_url"`
	EriURL       string `yaml:"eri_url"`
	NATSURL      string `yaml:"nats_url"`
	GitHubAPIURL string `yaml:"github_api_url"`
	GitHubToken  string `yaml:"github_token"`
	DeployUIURL  string `yaml:"deploy_ui_url"`
}

// RestateConfig holds Restate durable workflow runtime configuration for Vanadis.
type RestateConfig struct {
	Enabled           bool   `yaml:"enabled"`
	RestateIngressURL string `yaml:"restate_ingress_url"`
	AgentName         string `yaml:"agent_name"`
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg VanadisYAMLConfig
	target := strings.TrimSpace(configPath)
	if fi, err := os.Stat(target); err != nil || fi.Size() == 0 {
		target = ""
		if fi, err := os.Stat(FallbackConfigPath); err == nil && fi.Size() > 0 {
			target = FallbackConfigPath
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis] Warning: failed to parse yaml config at %s: %v", target, err)
			}
		}
	}
	pgURL := firstNonEmpty(httpkit.BuildPostgresDSN(os.Getenv, "aerial"), strings.TrimSpace(fileCfg.PostgresURL))
	eriURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL)), "/")
	return Config{
		Port:         firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4025"),
		PostgresURL:  pgURL,
		BrainURL:     eriURL,
		EriURL:       eriURL,
		NATSURL:      firstNonEmpty(os.Getenv("NATS_URL"), strings.TrimSpace(fileCfg.NATSURL)),
		GitHubAPIURL: strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("GITHUB_API_URL"), fileCfg.GitHubAPIURL, "https://api.github.com")), "/"),
		GitHubToken:  firstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GITHUB_PAT"), fileCfg.GitHubToken),
		ConfigPath:   strings.TrimSpace(configPath),
		DeployUIURL:  strings.TrimRight(strings.TrimSpace(fileCfg.DeployUIURL), "/"),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config { return LoadConfig(os.Getenv("CONFIG_PATH")) }

// NormalizeAgentName trims whitespace for per-agent scoped Restate names.
func NormalizeAgentName(agent string) string {
	return strings.ToLower(strings.TrimSpace(agent))
}

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
// Empty and aerial resolve to the legacy Build name so Phase 1 stays dark.
func BuildServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == "aerial" {
		return "Build"
	}
	return "Build-" + name
}

// BuildRouterServiceName derives the Restate BuildRouter service name for an agent.
// Empty and aerial resolve to the legacy BuildRouter name so Phase 1 stays dark.
func BuildRouterServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == "aerial" {
		return "BuildRouter"
	}
	return "BuildRouter-" + name
}

// LoadRestateConfig loads Restate configuration from an optional YAML file with environment variable overrides.
func LoadRestateConfig(configPath string) RestateConfig {
	var fileCfg RestateConfig
	target := strings.TrimSpace(configPath)
	if fi, err := os.Stat(target); err != nil || fi.Size() == 0 {
		target = ""
		if fi, err := os.Stat(FallbackConfigPath); err == nil && fi.Size() > 0 {
			target = FallbackConfigPath
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis] Warning: failed to parse restate yaml config at %s: %v", target, err)
			}
		}
	}
	enabled := fileCfg.Enabled || strings.EqualFold(os.Getenv("VANADIS_RESTATE_ENABLED"), "true") || strings.EqualFold(os.Getenv("RESTATE_ENABLED"), "true")
	ingressURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("RESTATE_INGRESS_URL"), fileCfg.RestateIngressURL)), "/")
	if ingressURL != "" && !strings.EqualFold(os.Getenv("VANADIS_RESTATE_ENABLED"), "false") && !strings.EqualFold(os.Getenv("RESTATE_ENABLED"), "false") {
		enabled = true
	}
	return RestateConfig{
		Enabled:           enabled,
		RestateIngressURL: ingressURL,
		AgentName:         strings.TrimSpace(firstNonEmpty(os.Getenv("AGENT_NAME"), fileCfg.AgentName, "aerial")),
	}
}
