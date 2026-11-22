package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/pkg/strutil"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/haro.yaml"

// DefaultTimezone is the fallback timezone for tool invocations when unspecified in tool payload.
// This is its single home: it is the configurable fallback for Config.Timezone,
// resolved by config loading and tools resolveTimezone. No other package redeclares it.
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
	DatabaseURL         string
	Timezone            string         `yaml:"timezone"`
	Port                string         `yaml:"port"`
	EriURL              string         `yaml:"eri_url"`
	VanadisURL          string         `yaml:"vanadis_url"`
	NATSURL             string         `yaml:"nats_url"`
	RestateURL          string         `yaml:"restate_url"`
	RestatePort         string         `yaml:"restate_port"`
	RestateIngressURL   string         `yaml:"restate_ingress_url"`
	RestateAdminURL     string         `yaml:"restate_admin_url"`
	RestateAdvertiseURL string         `yaml:"restate_advertise_url"`
	AgentName           string         `yaml:"agent_name"`
	ExtraEventSources   []string       `yaml:"extra_event_sources"`
	Auth                k8sauth.Config `yaml:"auth"`
}

// LoadConfig loads configuration from path using DATABASE_URL from the environment.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigFile(path, httpkit.BuildPostgresDSN(os.Getenv, "aerial"))
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

	var ycfg struct {
		Port                string         `yaml:"port"`
		Timezone            string         `yaml:"timezone"`
		EriURL              string         `yaml:"eri_url"`
		VanadisURL          string         `yaml:"vanadis_url"`
		NATSURL             string         `yaml:"nats_url"`
		RestateURL          string         `yaml:"restate_url"`
		RestatePort         string         `yaml:"restate_port"`
		RestateIngressURL   string         `yaml:"restate_ingress_url"`
		RestateAdminURL     string         `yaml:"restate_admin_url"`
		RestateAdvertiseURL string         `yaml:"restate_advertise_url"`
		AgentName           string         `yaml:"agent_name"`
		ExtraEventSources   []string       `yaml:"extra_event_sources"`
		Auth                k8sauth.Config `yaml:"auth"`
	}
	if unmarshalErr := yaml.Unmarshal(data, &ycfg); unmarshalErr != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, unmarshalErr)
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
		cleanDB = httpkit.BuildPostgresDSN(os.Getenv, "aerial")
	}
	if cleanDB == "" {
		return nil, fmt.Errorf("config: database connection string is required")
	}

	eriURL := os.Getenv("ERI_URL")
	if eriURL == "" {
		eriURL = ycfg.EriURL
	}
	eriURL = strings.TrimSpace(eriURL)
	if eriURL == "" {
		return nil, fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	eriURL = strings.TrimRight(eriURL, "/")

	vanadisURL := os.Getenv("VANADIS_URL")
	if vanadisURL == "" {
		vanadisURL = ycfg.VanadisURL
	}
	vanadisURL = strings.TrimSpace(vanadisURL)
	if vanadisURL == "" {
		return nil, fmt.Errorf("config: vanadis_url is required in %s", cleanPath)
	}
	vanadisURL = strings.TrimRight(vanadisURL, "/")

	extraSources := resolveExtraSources(ycfg.ExtraEventSources)
	natsURL := resolveEnvOrDefault("NATS_URL", ycfg.NATSURL, "nats://nats.aerial:4222")
	restatePort := resolveEnvOrDefault("RESTATE_PORT", ycfg.RestatePort, "9080")
	restateIngressURL := strings.TrimRight(resolveEnvOrDefault("RESTATE_INGRESS_URL", strutil.FirstNonEmpty(ycfg.RestateIngressURL, ycfg.RestateURL), "http://restate.aerial:8080"), "/")
	restateAdminURL := strings.TrimRight(resolveEnvOrDefault("RESTATE_ADMIN_URL", ycfg.RestateAdminURL, "http://restate.aerial:9070"), "/")
	restateAdvertiseURL := strings.TrimRight(resolveEnvOrDefault("RESTATE_ADVERTISE_URL", ycfg.RestateAdvertiseURL, "http://haro.default.svc.cluster.local:9080"), "/")
	agentName := resolveEnvOrDefault("AGENT_NAME", ycfg.AgentName, "aerial")
	authCfg := ycfg.Auth
	if err := k8sauth.ApplyEnvOverride(&authCfg, os.Getenv); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	cfg := &Config{
		DatabaseURL:         cleanDB,
		Timezone:            tz,
		Port:                port,
		EriURL:              eriURL,
		VanadisURL:          vanadisURL,
		NATSURL:             natsURL,
		RestateURL:          restateIngressURL,
		RestatePort:         restatePort,
		RestateIngressURL:   restateIngressURL,
		RestateAdminURL:     restateAdminURL,
		RestateAdvertiseURL: restateAdvertiseURL,
		AgentName:           agentName,
		ExtraEventSources:   extraSources,
		Auth:                authCfg,
	}
	RegisterConfigTokens(cfg)
	return cfg, nil
}

func resolveExtraSources(sources []string) []string {
	var extraSources []string
	seenSources := make(map[string]struct{}, len(sources))
	for _, raw := range sources {
		if s := strings.ToLower(strings.TrimSpace(raw)); s != "" {
			if _, ok := seenSources[s]; !ok {
				seenSources[s] = struct{}{}
				extraSources = append(extraSources, s)
			}
		}
	}
	return extraSources
}

func resolveEnvOrDefault(envKey, fileVal, defaultVal string) string {
	val := os.Getenv(envKey)
	if val == "" {
		val = fileVal
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return defaultVal
	}
	return val
}

// RegisterConfigTokens registers sensitive tokens from Haro's configuration,
// database credentials, environment variables, and shared secrets into the global redaction pool.
func RegisterConfigTokens(cfg *Config) {
	var tokens []string
	if cfg != nil {
		if dsn := strings.TrimSpace(cfg.DatabaseURL); dsn != "" && dsn != ":memory:" {
			tokens = append(tokens, dsn)
		}
		if natsURL := strings.TrimSpace(cfg.NATSURL); natsURL != "" {
			tokens = append(tokens, natsURL)
		}
	}
	for _, envKey := range []string{"POSTGRES_PASSWORD", "DATABASE_URL", "GITHUB_PAT", "NATS_URL"} {
		if val := strings.TrimSpace(os.Getenv(envKey)); val != "" {
			tokens = append(tokens, val)
		}
	}
	sanitizer.RegisterSharedSecrets(sanitizer.DefaultSharedSecretsPath)
	if len(tokens) > 0 {
		sanitizer.RegisterSensitiveTokens(tokens...)
	}
}
