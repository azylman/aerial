package main

import (
	"github.com/azylman/aerial/pkg/httpkit"
)

// staticConfig is a fixed ConfigSource for tests.
type staticConfig struct {
	cfg *Config
}

func (s staticConfig) Current() *Config { return s.cfg }

// newTestConfigSource wraps cfg in a real Reloadable whose loader always
// yields cfg, so reload requests report "unchanged".
func newTestConfigSource(cfg *Config) *httpkit.Reloadable[Config] {
	return httpkit.NewReloadable(cfg, func() (*Config, error) { return cfg, nil }, prosperaRestartRequired)
}
