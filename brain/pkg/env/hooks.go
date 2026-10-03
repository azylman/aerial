package env

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Default canonical hooks.json payload used when external file is absent.
const defaultCanonicalHooksJSON = `{
  "share-guard": {
    "PreToolUse": [
      {
        "matcher": "replace_file_content|write_to_file|run_command",
        "hooks": [
          {
            "type": "command",
            "command": "hook-guard share",
            "timeout": 5
          }
        ]
      }
    ]
  },
  "schedule-guard": {
    "PreToolUse": [
      {
        "matcher": "schedule|run_command",
        "hooks": [
          {
            "type": "command",
            "command": "hook-guard schedule",
            "timeout": 5
          }
        ]
      }
    ]
  },
  "commit-guard": {
    "PreToolUse": [
      {
        "matcher": "run_command",
        "hooks": [
          {
            "type": "command",
            "command": "hook-guard commit",
            "timeout": 5
          }
        ]
      }
    ]
  }
}
`

// DefaultHooksSearchPaths defines priority search roots for hook assets.
var DefaultHooksSearchPaths = []string{
	"/share/aerial/scripts/hooks",
	"/app/scripts/hooks",
	"./scripts/hooks",
	"../scripts/hooks",
	"../../scripts/hooks",
}

// SyncHooks provisions hooks.json and executable hook scripts into primary ~/.gemini and all runtime targets.
func (p *Provisioner) SyncHooks() error {
	if p == nil || p.homeDir == "" {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. Locate source hooks directory and read hooks.json
	hooksDir := ""
	if p.hooksDir != "" {
		if fi, err := os.Stat(p.hooksDir); err == nil && fi.IsDir() {
			hooksDir = p.hooksDir
		}
	}
	if hooksDir == "" {
		for _, candidate := range DefaultHooksSearchPaths {
			if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
				hooksDir = candidate
				break
			}
		}
	}

	hooksJSONContent := defaultCanonicalHooksJSON
	if hooksDir != "" {
		sourceHooksJSON := filepath.Join(hooksDir, "hooks.json")
		if data, err := os.ReadFile(sourceHooksJSON); err == nil && len(strings.TrimSpace(string(data))) > 0 {
			hooksJSONContent = string(data)
		}
	}

	// 2. Identify all target gemini roots
	targetRoots := []string{
		filepath.Join(p.homeDir, ".gemini"),
	}

	if p.dataDir != "" {
		runtimesRoot := filepath.Join(p.dataDir, "runtimes")
		targetRoots = append(targetRoots,
			filepath.Join(runtimesRoot, string(TargetDiscord), ".gemini"),
			filepath.Join(runtimesRoot, string(TargetVoice), ".gemini"),
			filepath.Join(runtimesRoot, string(TargetEphemeral), ".gemini"),
		)
	}

	// 3. Provision hooks into each target root
	for _, targetGemini := range targetRoots {
		configDir := filepath.Join(targetGemini, "config")
		if err := os.MkdirAll(configDir, 0755); err != nil {
			log.Printf("[Env] Warning: failed to mkdir %s for hooks: %v", configDir, err)
			continue
		}

		// Write hooks.json
		targetHooksJSON := filepath.Join(configDir, "hooks.json")
		if err := p.writeAtomic(targetHooksJSON, hooksJSONContent); err != nil {
			log.Printf("[Env] Warning: failed to write hooks.json to %s: %v", targetHooksJSON, err)
		}

		// Copy hook scripts if source directory exists
		if hooksDir != "" {
			targetHooksScriptsDir := filepath.Join(configDir, "hooks")
			if err := os.MkdirAll(targetHooksScriptsDir, 0755); err != nil {
				log.Printf("[Env] Warning: failed to mkdir %s for hook scripts: %v", targetHooksScriptsDir, err)
				continue
			}

			entries, err := os.ReadDir(hooksDir)
			if err == nil {
				for _, entry := range entries {
					if entry.IsDir() || entry.Name() == "hooks.json" {
						continue
					}
					srcPath := filepath.Join(hooksDir, entry.Name())
					dstPath := filepath.Join(targetHooksScriptsDir, entry.Name())

					scriptData, rErr := os.ReadFile(srcPath)
					if rErr != nil {
						continue
					}
					if wErr := p.writeAtomic(dstPath, string(scriptData)); wErr != nil {
						log.Printf("[Env] Warning: failed to write hook script %s: %v", dstPath, wErr)
					} else if cErr := os.Chmod(dstPath, 0755); cErr != nil {
						log.Printf("[Env] Warning: failed to chmod hook script %s: %v", dstPath, cErr)
					}
				}
			}
		}
	}

	log.Printf("[Env] Synchronized lifecycle hooks across %d runtime targets", len(targetRoots))
	return nil
}
