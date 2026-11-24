package config

import (
	"os"
	"path/filepath"
	"testing"
)

type restateAdminURLCase struct {
	name    string
	yamlVal string
	envVal  string
	want    string
}

func runRestateAdminURLCase(t *testing.T, tc restateAdminURLCase) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	yamlBody := "model: test-model\ntimezone: America/Los_Angeles\nchannels:\n  default:\n    mode: \"threads\"\n"
	if tc.yamlVal != "" {
		yamlBody += "restate_admin_url: " + tc.yamlVal + "\n"
	}
	if err := os.WriteFile(cfgPath, []byte(yamlBody), 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	env := map[string]string{}
	if tc.envVal != "" {
		env["RESTATE_ADMIN_URL"] = tc.envVal
	}
	cfg, err := LoadConfigFromLookup(mapLookup(env), cfgPath)
	if err != nil {
		t.Fatalf("LoadConfigFromLookup failed: %v", err)
	}
	if got := cfg.Current().RestateAdminURL; got != tc.want {
		t.Errorf("RestateAdminURL = %q, want %q", got, tc.want)
	}
}

func TestConfigData_RestateAdminURL_Suite(t *testing.T) {
	tests := []restateAdminURLCase{
		{name: "yaml value kept", yamlVal: "http://restate.aerial:9070", want: "http://restate.aerial:9070"},
		{name: "yaml trailing slash trimmed", yamlVal: "http://restate.aerial:9070/", want: "http://restate.aerial:9070"},
		{name: "yaml whitespace trimmed", yamlVal: "   http://restate.aerial:9070/   ", want: "http://restate.aerial:9070"},
		{name: "env overrides yaml", yamlVal: "http://yaml.restate:9070", envVal: "http://env.restate:9070", want: "http://env.restate:9070"},
		{name: "env supplies default", envVal: "http://env.restate:9070", want: "http://env.restate:9070"},
		{name: "empty defaults empty", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runRestateAdminURLCase(t, tc)
		})
	}
}
