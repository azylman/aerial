package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	validYAML := filepath.Join(tmpDir, "config.yaml")
	require.NoError(t, os.WriteFile(validYAML, []byte("port: \"5050\"\nnats_url: \"nats://file:4222\"\nrestate_ingress_url: \"http://restate-file:8080/\"\n"), 0o600))

	invalidYAML := filepath.Join(tmpDir, "invalid.yaml")
	require.NoError(t, os.WriteFile(invalidYAML, []byte("invalid: ["), 0o600))

	fallbackYAML := filepath.Join(tmpDir, "fallback.yaml")
	require.NoError(t, os.WriteFile(fallbackYAML, []byte("port: \"4033\"\n"), 0o600))

	tests := []struct {
		name          string
		configPath    string
		envPort       string
		envNATS       string
		envRestate    string
		fallbackPaths []string
		wantPort      string
		wantNATS      string
		wantRestate   string
	}{
		{
			name:        "defaults when empty",
			wantPort:    "4027",
			wantNATS:    "",
			wantRestate: "http://restate.restate.svc.cluster.local:8080",
		},
		{
			name:        "loaded from yaml file",
			configPath:  validYAML,
			wantPort:    "5050",
			wantNATS:    "nats://file:4222",
			wantRestate: "http://restate-file:8080",
		},
		{
			name:        "env overrides file",
			configPath:  validYAML,
			envPort:     "6060",
			envNATS:     "nats://env:4222",
			envRestate:  "http://restate-env:8080/",
			wantPort:    "6060",
			wantNATS:    "nats://env:4222",
			wantRestate: "http://restate-env:8080",
		},
		{
			name:        "invalid yaml falls back to defaults",
			configPath:  invalidYAML,
			wantPort:    "4027",
			wantNATS:    "",
			wantRestate: "http://restate.restate.svc.cluster.local:8080",
		},
		{
			name:          "fallback config path matches",
			configPath:    "",
			fallbackPaths: []string{filepath.Join(tmpDir, "nonexistent.yaml"), fallbackYAML},
			wantPort:      "4033",
			wantNATS:      "",
			wantRestate:   "http://restate.restate.svc.cluster.local:8080",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PORT", tc.envPort)
			t.Setenv("NATS_URL", tc.envNATS)
			t.Setenv("RESTATE_INGRESS_URL", tc.envRestate)

			if len(tc.fallbackPaths) > 0 {
				origFallbacks := FallbackConfigPaths
				defer func() { FallbackConfigPaths = origFallbacks }()
				FallbackConfigPaths = tc.fallbackPaths
			}

			cfg := LoadConfig(tc.configPath)
			assert.Equal(t, tc.wantPort, cfg.Port)
			assert.Equal(t, tc.wantNATS, cfg.NATSURL)
			assert.Equal(t, tc.wantRestate, cfg.RestateIngressURL)
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "from_env.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("port: \"4029\"\n"), 0o600))
	otherFile := filepath.Join(tmpDir, "other.yaml")
	require.NoError(t, os.WriteFile(otherFile, []byte("port: \"4031\"\n"), 0o600))

	tests := []struct {
		name       string
		configPath string
		envPort    string
		wantPort   string
	}{
		{name: "config path file port", configPath: configFile, envPort: "", wantPort: "4029"},
		{name: "env port overrides file", configPath: otherFile, envPort: "4077", wantPort: "4077"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.configPath)
			t.Setenv("PORT", tc.envPort)
			cfg := LoadConfigFromEnv()
			assert.Equal(t, tc.wantPort, cfg.Port)
			assert.Equal(t, tc.configPath, cfg.ConfigPath)
		})
	}
}

func TestBuildServiceName_Derivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		agent      string
		wantRouter string
	}{
		{name: "empty defaults to legacy", agent: "", wantRouter: "BuildRouter"},
		{name: "aerial defaults to legacy", agent: "aerial", wantRouter: "BuildRouter"},
		{name: "whitespace aerial defaults to legacy", agent: "  Aerial  ", wantRouter: "BuildRouter"},
		{name: "agent-beta scoped", agent: "agent-beta", wantRouter: "BuildRouter-agent-beta"},
		{name: "agent-alpha scoped", agent: "agent-alpha", wantRouter: "BuildRouter-agent-alpha"},
		{name: "mixed case scoped normalized", agent: "Agent-Beta", wantRouter: "BuildRouter-agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantRouter, BuildRouterServiceName(tc.agent))
			assert.Equal(t, NormalizeAgentName(tc.agent), NormalizeAgentName(tc.agent))
		})
	}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{name: "empty returns empty", values: nil, want: ""},
		{name: "all blank returns empty", values: []string{"", "   "}, want: ""},
		{name: "first non blank wins", values: []string{"", "  a  ", "b"}, want: "a"},
		{name: "single value", values: []string{"x"}, want: "x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, FirstNonEmpty(tc.values...))
		})
	}
}
