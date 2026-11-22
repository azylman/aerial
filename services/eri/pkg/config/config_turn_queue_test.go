package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigData_UnmarshalTurnQueue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		yaml          string
		wantTurnQueue string
		errContains   string
	}{
		{name: "absent key", yaml: "model: m\n", wantTurnQueue: ""},
		{name: "blank value", yaml: "turn_queue: \"\"\n", wantTurnQueue: ""},
		{name: "legacy", yaml: "turn_queue: legacy\n", wantTurnQueue: "legacy"},
		{name: "coordinator", yaml: "turn_queue: coordinator\n", wantTurnQueue: "coordinator"},
		{name: "whitespace trimmed", yaml: "turn_queue: \" coordinator \"\n", wantTurnQueue: "coordinator"},
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
