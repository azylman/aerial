package env

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Default canonical claude settings.json hooks configuration.
const defaultCanonicalClaudeSettingsJSON = `{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "hook-guard pre-invocation"
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": ".*",
        "hooks": [
          {
            "type": "command",
            "command": "hook-guard all"
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Edit|MultiEdit|Write",
        "hooks": [
          {
            "type": "command",
            "command": "hook-guard post-batch"
          }
        ]
      }
    ]
  }
}`

// Default canonical codex hooks.json hooks configuration.
const defaultCanonicalCodexHooksJSON = `{
  "hooks": {
    "UserPromptSubmit": [
      {
        "type": "command",
        "command": "/usr/local/bin/hook-guard pre-invocation",
        "hooks": [
          {
            "type": "command",
            "command": "/usr/local/bin/hook-guard pre-invocation"
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": ".*",
        "type": "command",
        "command": "/usr/local/bin/hook-guard all",
        "hooks": [
          {
            "type": "command",
            "command": "/usr/local/bin/hook-guard all"
          }
        ]
      }
    ]
  }
}`

// Default canonical hooks.json payload used when external file is absent.
const defaultCanonicalHooksJSON = `{"share-guard":{"PreToolUse":[{"matcher":"replace_file_content|write_to_file|edit_file|write_file|run_command","hooks":[{"type":"command","command":"hook-guard share","timeout":5}]}]},"live-state-guard":{"PreToolUse":[{"matcher":"replace_file_content|write_to_file|edit_file|write_file|run_command","hooks":[{"type":"command","command":"hook-guard live-state","timeout":5}]}]},"schedule-guard":{"PreToolUse":[{"matcher":"schedule|run_command|manage_task|manage_subagents|view_file|read_file","hooks":[{"type":"command","command":"hook-guard schedule","timeout":5}]}]},"batch-guard":{"PreInvocation":[{"type":"command","command":"hook-guard pre-invocation","timeout":5}],"PreToolUse":[{"matcher":"view_file|read_file|replace_file_content|write_to_file|edit_file|write_file|run_command","hooks":[{"type":"command","command":"hook-guard batch","timeout":5}]}],"PostToolUse":[{"matcher":"replace_file_content|write_to_file|edit_file|write_file","hooks":[{"type":"command","command":"hook-guard post-batch","timeout":5}]}]},"search-guard":{"PreToolUse":[{"matcher":"run_command|call_mcp_tool","hooks":[{"type":"command","command":"hook-guard search","timeout":5}]}]},"commit-guard":{"PreToolUse":[{"matcher":"run_command|call_mcp_tool","hooks":[{"type":"command","command":"hook-guard commit","timeout":5}]}]},"trace-announce":{"PreToolUse":[{"matcher":"call_mcp_tool","hooks":[{"type":"command","command":"hook-guard trace","timeout":5}]}]}}`

// DefaultHooksSearchPaths defines priority search roots for hook assets.
var DefaultHooksSearchPaths = []string{
	"/share/aerial/scripts/hooks", "/app/scripts/hooks", "./scripts/hooks", "../scripts/hooks", "../../scripts/hooks", "../../../scripts/hooks", "../../../../scripts/hooks",
}

func (p *Provisioner) copyHookScriptsToTarget(hooksDir, targetHooksScriptsDir string) {
	if hooksDir == "" {
		return
	}
	if err := os.MkdirAll(targetHooksScriptsDir, 0o755); err != nil {
		log.Printf("[Env] Warning: failed to mkdir %s for hook scripts: %v", targetHooksScriptsDir, err)
		return
	}

	entries, err := os.ReadDir(hooksDir)
	if err != nil {
		return
	}
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
		} else {
			ignore(os.Chmod(dstPath, 0o755))
		}
	}
}

func (p *Provisioner) provisionTargetHooks(targetGemini, hooksJSONContent, hooksDir string) {
	configDir := filepath.Join(targetGemini, "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		log.Printf("[Env] Warning: failed to mkdir %s for hooks: %v", configDir, err)
		return
	}

	// Write hooks.json
	targetHooksJSON := filepath.Join(configDir, "hooks.json")
	if err := p.writeAtomic(targetHooksJSON, hooksJSONContent); err != nil {
		log.Printf("[Env] Warning: failed to write hooks.json to %s: %v", targetHooksJSON, err)
	}

	// Copy hook scripts if source directory exists
	targetHooksScriptsDir := filepath.Join(configDir, "hooks")
	p.copyHookScriptsToTarget(hooksDir, targetHooksScriptsDir)
}

func (p *Provisioner) provisionTargetClaudeSettings(claudeDir string) {
	if err := os.MkdirAll(claudeDir, 0o755); err != nil {
		log.Printf("[Env] Warning: failed to mkdir %s for claude settings: %v", claudeDir, err)
		return
	}
	settingsPath := filepath.Join(claudeDir, "settings.json")
	if err := p.writeAtomic(settingsPath, defaultCanonicalClaudeSettingsJSON); err != nil {
		log.Printf("[Env] Warning: failed to write settings.json to %s: %v", claudeDir, err)
	}
}

func (p *Provisioner) provisionTargetCodexHooks(codexDir string) {
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		log.Printf("[Env] Warning: failed to mkdir %s for codex hooks: %v", codexDir, err)
		return
	}
	hooksPath := filepath.Join(codexDir, "hooks.json")
	if err := p.writeAtomic(hooksPath, defaultCanonicalCodexHooksJSON); err != nil {
		log.Printf("[Env] Warning: failed to write hooks.json to %s: %v", codexDir, err)
	}
}

// SyncHooks provisions hooks.json and executable hook scripts into primary ~/.gemini and all runtime targets.
func (p *Provisioner) SyncHooks() error {
	if p == nil || p.homeDir == "" {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. Locate source hooks directory and read hooks.json
	hooksDir := resolveDir(p.hooksDir, DefaultHooksSearchPaths)
	if fi, err := os.Stat(hooksDir); err != nil || !fi.IsDir() {
		hooksDir = ""
	}

	hooksJSONContent := defaultCanonicalHooksJSON
	if hooksDir != "" {
		sourceHooksJSON := filepath.Join(hooksDir, "hooks.json")
		if data, err := os.ReadFile(sourceHooksJSON); err == nil && strings.TrimSpace(string(data)) != "" {
			hooksJSONContent = string(data)
		}
	}

	// 2. Identify all target gemini roots
	targetRoots := []string{
		filepath.Join(p.homeDir, ".gemini"),
	}

	if p.dataDir != "" {
		for _, target := range allRuntimeTargets {
			targetRoots = append(targetRoots, filepath.Join(p.runtimeHomeDir(target), ".gemini"))
		}
	}

	// 3. Provision hooks into each target root
	for _, targetGemini := range targetRoots {
		p.provisionTargetHooks(targetGemini, hooksJSONContent, hooksDir)
	}

	// 4. Provision Claude settings.json into primary ~/.claude and each runtime target ~/.claude
	p.provisionTargetClaudeSettings(filepath.Join(p.homeDir, ".claude"))
	if p.dataDir != "" {
		for _, target := range allRuntimeTargets {
			p.provisionTargetClaudeSettings(filepath.Join(p.runtimeHomeDir(target), ".claude"))
		}
	}

	// 5. Provision Codex hooks.json into primary ~/.codex and each runtime target ~/.codex
	p.provisionTargetCodexHooks(filepath.Join(p.homeDir, ".codex"))
	if p.dataDir != "" {
		for _, target := range allRuntimeTargets {
			p.provisionTargetCodexHooks(filepath.Join(p.runtimeHomeDir(target), ".codex"))
		}
	}

	log.Printf("[Env] Synchronized lifecycle hooks across %d runtime targets", len(targetRoots))
	return nil
}
