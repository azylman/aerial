package env

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDefaultCanonicalHooksJSON(t *testing.T) {
	t.Parallel()
	repoHooks, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "scripts", "hooks", "hooks.json"))
	if err != nil {
		t.Fatalf("read scripts/hooks/hooks.json: %v", err)
	}
	wantTrace := map[string]any{"PreToolUse": []any{map[string]any{
		"matcher": "call_mcp_tool",
		"hooks":   []any{map[string]any{"type": "command", "command": "hook-guard trace", "timeout": float64(5)}},
	}}}
	tests := []struct {
		name    string
		content string
	}{
		{name: "default constant", content: defaultCanonicalHooksJSON},
		{name: "scripts/hooks/hooks.json", content: string(repoHooks)},
	}
	parsedAll := make([]map[string]any, 0, len(tests))
	for _, tc := range tests {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(tc.content), &parsed); err != nil {
			t.Fatalf("%s is not valid JSON: %v", tc.name, err)
		}
		for _, expected := range []string{"share-guard", "schedule-guard", "batch-guard", "search-guard", "commit-guard"} {
			if _, ok := parsed[expected]; !ok {
				t.Errorf("%s: missing group %q", tc.name, expected)
			}
		}
		if !reflect.DeepEqual(parsed["trace-announce"], wantTrace) {
			t.Errorf("%s: trace-announce = %v, want %v", tc.name, parsed["trace-announce"], wantTrace)
		}
		parsedAll = append(parsedAll, parsed)
	}
	if !reflect.DeepEqual(parsedAll[0], parsedAll[1]) {
		t.Errorf("default constant and scripts/hooks/hooks.json diverge")
	}
}
