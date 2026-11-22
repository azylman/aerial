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
		wantTurnQueue   string
		wantTurnTimeout int
		errContains     string
	}{
		{name: "absent key", yaml: "model: m\n", wantTurnQueue: "", wantTurnTimeout: 120},
		{name: "blank value", yaml: "turn_queue: \"\"\n", wantTurnQueue: "", wantTurnTimeout: 120},
		{name: "legacy", yaml: "turn_queue: legacy\n", wantTurnQueue: "legacy", wantTurnTimeout: 120},
		{name: "coordinator", yaml: "turn_queue: coordinator\n", wantTurnQueue: "coordinator", wantTurnTimeout: 120},
		{name: "whitespace trimmed", yaml: "turn_queue: \" coordinator \"\n", wantTurnQueue: "coordinator", wantTurnTimeout: 120},
		{name: "turn timeout zero defaults to 120", yaml: "turn_timeout_minutes: 0\n", wantTurnTimeout: 120},
		{name: "turn timeout configured 90", yaml: "turn_timeout_minutes: 90\n", wantTurnTimeout: 90},
		{name: "turn timeout max 240 accepted", yaml: "turn_timeout_minutes: 240\n", wantTurnTimeout: 240},
		{name: "turn timeout negative rejected", yaml: "turn_timeout_minutes: -5\n", errContains: "invalid turn_timeout_minutes -5"},
		{name: "turn timeout over max rejected", yaml: "turn_timeout_minutes: 241\n", errContains: "invalid turn_timeout_minutes 241"},
		{name: "unknown value rejected", yaml: "turn_queue: restate\n", errContains: `invalid turn_queue "restate"`},
		{name: "wrong case rejected", yaml: "turn_queue: Coordinator\n", errContains: `invalid turn_queue "Coordinator"`},
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
			if cfg.TurnQueue != tc.wantTurnQueue {
				t.Fatalf("cfg.TurnQueue = %q, want %q", cfg.TurnQueue, tc.wantTurnQueue)
			}
			if got := cfg.TurnTimeout(); got != tc.wantTurnTimeout {
				t.Fatalf("cfg.TurnTimeout() = %d, want %d", got, tc.wantTurnTimeout)
			}
		})
	}
}

func TestParseTurnQueueMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       string
		wantMode string
		wantErr  bool
	}{
		{name: "blank is legacy", in: "", wantMode: "legacy"},
		{name: "whitespace only is legacy", in: " \n", wantMode: "legacy"},
		{name: "legacy", in: "legacy", wantMode: "legacy"},
		{name: "coordinator with newline", in: "coordinator\n", wantMode: "coordinator"},
		{name: "unknown", in: "both", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseTurnQueueMode(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseTurnQueueMode(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if got != tc.wantMode {
				t.Fatalf("ParseTurnQueueMode(%q) = %q, want %q", tc.in, got, tc.wantMode)
			}
		})
	}
}
