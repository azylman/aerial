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

type museHooksCase struct {
	name            string
	existingContent string
	blockDir        bool
	wantMCPServer   bool
}

func TestProvisionTargetMuseHooks(t *testing.T) {
	tests := []museHooksCase{
		{
			name: "creates settings.json with default hooks in empty directory",
		},
		{
			name:            "preserves existing mcpServers when updating hooks",
			existingContent: `{"schema_version":1,"mcpServers":{"my_server":{"type":"stdio"}}}`,
			wantMCPServer:   true,
		},
		{
			name:     "gracefully handles mkdir failure when path is blocked by file",
			blockDir: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			targetDir := filepath.Join(tmp, ".config", "muse")
			p := newDisabledProvisioner(t, tmp, tmp)

			if tc.blockDir {
				writeTestFile(t, targetDir, "blocker")
			} else if tc.existingContent != "" {
				writeTestFile(t, filepath.Join(targetDir, "settings.json"), tc.existingContent)
			}

			p.provisionTargetMuseHooks(targetDir)

			if tc.blockDir {
				return
			}

			data, err := os.ReadFile(filepath.Join(targetDir, "settings.json"))
			if err != nil {
				t.Fatalf("expected settings.json: %v", err)
			}
			var parsed map[string]any
			if err := json.Unmarshal(data, &parsed); err != nil {
				t.Fatalf("settings.json is not valid json: %v", err)
			}
			if _, ok := parsed["hooks"]; !ok {
				t.Errorf("expected hooks in settings.json: %v", parsed)
			}
			if tc.wantMCPServer {
				servers, ok := parsed["mcpServers"].(map[string]any)
				if !ok || servers["my_server"] == nil {
					t.Errorf("expected preserved mcpServers in settings.json: %v", parsed)
				}
			}
		})
	}
}
