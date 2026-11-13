package env

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestDefaultCanonicalHooksJSON keeps the built-in fallback hooks in sync with the shipped
// scripts/hooks/hooks.json, so runtimes provisioned without the scripts directory register the
// same guards.
func TestDefaultCanonicalHooksJSON(t *testing.T) {
	t.Parallel()
	repoHooks, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "scripts", "hooks", "hooks.json"))
	if err != nil {
		t.Fatalf("read scripts/hooks/hooks.json: %v", err)
	}
	var fallback, shipped map[string]any
	if err := json.Unmarshal([]byte(defaultCanonicalHooksJSON), &fallback); err != nil {
		t.Fatalf("defaultCanonicalHooksJSON is not valid JSON: %v", err)
	}
	if err := json.Unmarshal(repoHooks, &shipped); err != nil {
		t.Fatalf("scripts/hooks/hooks.json is not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(fallback, shipped) {
		t.Errorf("defaultCanonicalHooksJSON and scripts/hooks/hooks.json diverge")
	}
}
