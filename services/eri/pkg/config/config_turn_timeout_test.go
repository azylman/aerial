package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigData_UnmarshalTurnSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		yaml            string
		wantTurnTimeout int
		errContains     string
	}{
		{name: "turn timeout absent defaults to 120", yaml: "model: m\n", wantTurnTimeout: 120},
		{name: "turn timeout zero defaults to 120", yaml: "turn_timeout_minutes: 0\n", wantTurnTimeout: 120},
		{name: "turn timeout configured 90", yaml: "turn_timeout_minutes: 90\n", wantTurnTimeout: 90},
		{name: "turn timeout max 240 accepted", yaml: "turn_timeout_minutes: 240\n", wantTurnTimeout: 240},
		{name: "turn timeout negative rejected", yaml: "turn_timeout_minutes: -5\n", errContains: "invalid turn_timeout_minutes -5"},
		{name: "turn timeout over max rejected", yaml: "turn_timeout_minutes: 241\n", errContains: "invalid turn_timeout_minutes 241"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg ConfigData
			err := yaml.Unmarshal([]byte(tc.yaml), &cfg)
			if tc.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("expected error containing %q, got: %v", tc.errContains, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := cfg.TurnTimeout(); got != tc.wantTurnTimeout {
				t.Fatalf("cfg.TurnTimeout() = %d, want %d", got, tc.wantTurnTimeout)
			}
		})
	}
}
