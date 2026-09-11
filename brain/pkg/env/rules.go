package env

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	systemRulesMu sync.Mutex

	// DefaultAgentInstructionsSearchPaths specifies standard locations for AGENTS.md in priority order.
	DefaultAgentInstructionsSearchPaths = []string{
		"/share/aerial-config/AGENTS.local.md",
		"/share/aerial-config/AGENTS.md",
		"/share/aerial/AGENTS.md",
		"/app/AGENTS.md",
		"/data/AGENTS.md",
		"/data/.AGENTS.md.lkgc",
		"./AGENTS.local.md",
		"./AGENTS.md",
	}
)

// SyncRules compiles the user persona, AGENTS.md, and custom system prompt into ~/.gemini rules.
func (p *Provisioner) SyncRules(customPrompt string) error {
	systemRulesMu.Lock()
	defer systemRulesMu.Unlock()

	var sb strings.Builder
	sb.WriteString("---\ndescription: User persona, tone, and identity overrides\ntrigger: always_on\n---\n\n")

	foundPersona := false
	tornRead := false
	var personaContent string
	var personaSource string

	searchPaths := p.agentInstructionsPaths
	if len(searchPaths) == 0 {
		searchPaths = DefaultAgentInstructionsSearchPaths
	}

	for _, path := range searchPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if len(bytes.TrimSpace(data)) > 0 {
			personaContent = string(data)
			personaSource = filepath.Base(path)
			foundPersona = true
			log.Printf("Loaded agent instructions from %s", path)
			break
		}
		// File exists but is 0-bytes or whitespace-only (torn read during git sync)
		log.Printf("[Env] Active agent instructions file %s is empty (possible GitSync torn read), engaging Last Known Good Persona (LKGC)", path)
		tornRead = true
		break
	}

	p.mu.Lock()
	if tornRead {
		personaContent = p.lkgcPersona
		personaSource = p.lkgcPersonaSource
		if personaSource == "" {
			personaSource = "AGENTS.md"
		}
		if personaContent != "" {
			foundPersona = true
		}
	} else if foundPersona {
		p.lkgcPersona = personaContent
		p.lkgcPersonaSource = personaSource
		if personaSource != ".AGENTS.md.lkgc" {
			_ = p.writeAtomic(filepath.Join(p.dataDir, ".AGENTS.md.lkgc"), personaContent)
		}
	}
	p.mu.Unlock()

	foundInstructions := false
	if foundPersona && personaContent != "" {
		sb.WriteString(fmt.Sprintf("# User Persona Overrides (%s)\n\n%s\n\n", personaSource, personaContent))
		foundInstructions = true
	}

	// Environment Prompt Override
	if strings.TrimSpace(customPrompt) != "" {
		sb.WriteString(fmt.Sprintf("# Environment Prompt Override\n\n%s\n\n", strings.TrimSpace(customPrompt)))
		foundInstructions = true
	}

	var content string
	p.mu.Lock()
	if foundInstructions {
		content = sb.String()
		p.lkgcRules = content
	} else {
		content = p.lkgcRules
		if content == "" {
			p.mu.Unlock()
			return nil
		}
		log.Printf("Using Last Known Good Configuration (LKGC) for system rules")
	}
	p.mu.Unlock()

	primaryRulesDir := filepath.Join(p.homeDir, ".gemini", "rules")
	if err := os.MkdirAll(primaryRulesDir, 0755); err != nil {
		return fmt.Errorf("failed to create primary rules directory: %w", err)
	}

	configRulesDir := filepath.Join(p.homeDir, ".gemini", "config", "rules")
	if err := os.MkdirAll(configRulesDir, 0755); err != nil {
		return fmt.Errorf("failed to create config rules directory: %w", err)
	}

	// Clean up any legacy, conflicting, or repository-level generated rule files
	staleRuleFiles := []string{
		filepath.Join(primaryRulesDir, "system_instructions.md"),
		filepath.Join(primaryRulesDir, "SYSTEM_INSTRUCTIONS.md"),
		filepath.Join(primaryRulesDir, "system.md"),
		filepath.Join(primaryRulesDir, "SYSTEM.md"),
		filepath.Join(primaryRulesDir, "gemini.md"),
		filepath.Join(primaryRulesDir, "GEMINI.md"),
		filepath.Join(primaryRulesDir, "agents.md"),
		filepath.Join(primaryRulesDir, "custom_instructions.md"),

		filepath.Join(configRulesDir, "system_instructions.md"),
		filepath.Join(configRulesDir, "SYSTEM_INSTRUCTIONS.md"),
		filepath.Join(configRulesDir, "system.md"),
		filepath.Join(configRulesDir, "SYSTEM.md"),
		filepath.Join(configRulesDir, "gemini.md"),
		filepath.Join(configRulesDir, "GEMINI.md"),
		filepath.Join(configRulesDir, "agents.md"),
		filepath.Join(configRulesDir, "custom_instructions.md"),

		"/app/.agents/rules/system_instructions.md",
		"/app/.agents/rules/custom_instructions.md",
		"/app/.agents/rules/agents.md",
		"/app/.agents/rules/system.md",
		"/app/.agents/rules/gemini.md",
	}
	for _, stale := range staleRuleFiles {
		_ = os.Remove(stale)
	}

	primaryRuleFile := filepath.Join(primaryRulesDir, "user_persona.md")
	if err := p.writeAtomic(primaryRuleFile, content); err != nil {
		return fmt.Errorf("failed to write primary system rules: %w", err)
	}
	log.Printf("Configured always_on user persona in %s", primaryRuleFile)

	// Also sync to ~/.gemini/config/rules for compatibility
	configRuleFile := filepath.Join(configRulesDir, "user_persona.md")
	_ = p.writeAtomic(configRuleFile, content)

	return nil
}
