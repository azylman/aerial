package config

import (
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/strutil"
)

// Config holds runtime configuration for the vanadis-nats bridge service.
type Config struct {
	Port              string
	NATSURL           string
	RestateIngressURL string
	ConfigPath        string
	AgentName         string
}

// NATSBridgeYAMLConfig defines the declarative file schema for vanadis-nats configuration.
type NATSBridgeYAMLConfig struct {
	Port              string `yaml:"port"`
	NATSURL           string `yaml:"nats_url"`
	RestateIngressURL string `yaml:"restate_ingress_url"`
	AgentName         string `yaml:"agent_name"`
}

// FallbackConfigPaths lists declarative config locations probed when no explicit path is given.
var FallbackConfigPaths = []string{
	"/share/aerial-config/services/vanadis-nats/vanadis-nats.yaml",
	"/config/vanadis-nats.yaml",
}

// LoadConfig loads configuration from an optional YAML file with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg NATSBridgeYAMLConfig
	target := strings.TrimSpace(configPath)
	if target == "" {
		for _, p := range FallbackConfigPaths {
			if fi, err := os.Stat(p); err == nil && fi.Size() > 0 {
				target = p
				break
			}
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis-nats] Warning: failed to parse yaml config at %s: %v", target, err)
			}
		}
	}

	return Config{
		Port:              strutil.FirstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4027"),
		NATSURL:           strutil.FirstNonEmpty(os.Getenv("NATS_URL"), fileCfg.NATSURL),
		RestateIngressURL: strings.TrimRight(strings.TrimSpace(strutil.FirstNonEmpty(os.Getenv("RESTATE_INGRESS_URL"), fileCfg.RestateIngressURL, "http://restate.restate.svc.cluster.local:8080")), "/"),
		ConfigPath:        strings.TrimSpace(configPath),
		AgentName:         strings.TrimSpace(strutil.FirstNonEmpty(os.Getenv("AGENT_NAME"), fileCfg.AgentName, "aerial")),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
}
