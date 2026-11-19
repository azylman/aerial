package main

// Root compatibility shims for the Layer-0 config package.
//
// Every config symbol formerly defined here now lives in
// internal/config (Layer 0). This file keeps zero-logic aliases only so all
// not-yet-moved callers (main.go, server.go, tools.go) compile unchanged
// until the final purge (haro-07b). Shims contain no logic and carry no
// coverage-bearing statements.

import (
	"github.com/azylman/aerial/services/haro/internal/config"
)

// Config represents the runtime configuration for the scheduler MCP server.
type Config = config.Config

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = config.DefaultConfigPath

// DefaultTimezone is the fallback timezone for tool invocations when unspecified in tool payload.
const DefaultTimezone = config.DefaultTimezone

var (
	defaultConfigPath = config.DefaultConfigPath
	localConfigPath   = "/local/config.yaml"
)

// Keep the local override path declared for compatibility with
// pre-extraction callers; resolution itself lives in internal/config.
var _ = localConfigPath

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	return config.ResolveConfigPath(fallbackPath)
}

// LoadConfig loads configuration from path using DATABASE_URL from the environment.
func LoadConfig(path string) (*Config, error) {
	return config.LoadConfig(path)
}

// LoadConfigFile loads and validates the YAML config from path.
// Fails fast if the file is missing, malformed, or required fields are absent.
func LoadConfigFile(path string, dbURL string) (*Config, error) {
	return config.LoadConfigFile(path, dbURL)
}

// RegisterConfigTokens registers sensitive tokens from Haro's configuration,
// database credentials, environment variables, and shared secrets into the global redaction pool.
func RegisterConfigTokens(cfg *Config) {
	config.RegisterConfigTokens(cfg)
}
