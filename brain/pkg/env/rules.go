package env

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// MaxRuleFileSizeBytes is the maximum allowed byte length for compiled runtime rules
	// in ~/.gemini/rules/ to prevent silent Antigravity prompt truncation (23 KB hard ceiling).
	MaxRuleFileSizeBytes = 23 * 1024

	// MaxSourceRuleFileSizeBytes is the maximum allowed byte length for source rule files
	// (GEMINI.md, AGENTS.md), leaving buffer for YAML frontmatter and header wrapping.
	MaxSourceRuleFileSizeBytes = 23040
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

	// DefaultSystemInstructionsSearchPaths specifies standard locations for GEMINI.md in priority order.
	DefaultSystemInstructionsSearchPaths = []string{
		"/share/aerial-config/GEMINI.local.md",
		"/share/aerial-config/GEMINI.md",
		"/share/aerial/GEMINI.md",
		"/app/GEMINI.md",
		"/data/GEMINI.md",
		"/data/.GEMINI.md.lkgc",
		"./GEMINI.local.md",
		"./GEMINI.md",
	}
)

// SyncRules compiles the user persona (AGENTS.md) and base system architecture/invariants (GEMINI.md)
// into separate ~/.gemini rules (user_persona.md and system_invariants.md).
func (p *Provisioner) SyncRules(customPrompt string) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	systemRulesMu.Lock()
	defer systemRulesMu.Unlock()

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

	foundGemini := false
	tornGeminiRead := false
	var geminiContent string
	var geminiSource string

	systemPaths := p.systemInstructionsPaths
	if len(systemPaths) == 0 {
		systemPaths = DefaultSystemInstructionsSearchPaths
	}

	for _, path := range systemPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if len(bytes.TrimSpace(data)) > 0 {
			geminiContent = string(data)
			geminiSource = filepath.Base(path)
			foundGemini = true
			log.Printf("Loaded system instructions from %s", path)
			break
		}
		// File exists but is 0-bytes or whitespace-only (torn read during git sync)
		log.Printf("[Env] Active system instructions file %s is empty (possible GitSync torn read), engaging Last Known Good System Instructions (LKGC)", path)
		tornGeminiRead = true
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
		if len(personaContent) <= MaxSourceRuleFileSizeBytes {
			p.lkgcPersona = personaContent
			p.lkgcPersonaSource = personaSource
			if personaSource != ".AGENTS.md.lkgc" && p.dataDir != "" {
				if err := p.writeAtomic(filepath.Join(p.dataDir, ".AGENTS.md.lkgc"), personaContent); err != nil {
					log.Printf("[Env] Warning writing persona LKGC: %v", err)
				}
			}
		} else {
			log.Printf("[Env] Refusing to persist oversized persona to LKGC (%d bytes > %d bytes)", len(personaContent), MaxSourceRuleFileSizeBytes)
		}
	}

	if tornGeminiRead {
		geminiContent = p.lkgcGemini
		geminiSource = p.lkgcGeminiSource
		if geminiSource == "" {
			geminiSource = "GEMINI.md"
		}
		if geminiContent != "" {
			foundGemini = true
		}
	} else if foundGemini {
		if len(geminiContent) <= MaxSourceRuleFileSizeBytes {
			p.lkgcGemini = geminiContent
			p.lkgcGeminiSource = geminiSource
			if geminiSource != ".GEMINI.md.lkgc" && p.dataDir != "" {
				if err := p.writeAtomic(filepath.Join(p.dataDir, ".GEMINI.md.lkgc"), geminiContent); err != nil {
					log.Printf("[Env] Warning writing gemini LKGC: %v", err)
				}
			}
		} else {
			log.Printf("[Env] Refusing to persist oversized system instructions to LKGC (%d bytes > %d bytes)", len(geminiContent), MaxSourceRuleFileSizeBytes)
		}
	}
	p.mu.Unlock()

	var personaRuleContent string
	if (foundPersona && personaContent != "") || strings.TrimSpace(customPrompt) != "" {
		var personaSB strings.Builder
		personaSB.WriteString("---\ndescription: User persona, tone, and identity overrides\ntrigger: always_on\n---\n\n")
		if foundPersona && personaContent != "" {
			personaSB.WriteString(fmt.Sprintf("# User Persona Overrides (%s)\n\n%s\n\n", personaSource, personaContent))
		}
		if strings.TrimSpace(customPrompt) != "" {
			personaSB.WriteString(fmt.Sprintf("# Environment Prompt Override\n\n%s\n\n", strings.TrimSpace(customPrompt)))
		}
		personaRuleContent = personaSB.String()
	}

	var geminiRuleContent string
	if foundGemini && geminiContent != "" {
		var geminiSB strings.Builder
		geminiSB.WriteString("---\ndescription: Base system architecture, operational invariants, and guidelines\ntrigger: always_on\n---\n\n")
		geminiSB.WriteString(fmt.Sprintf("# Base System Architecture & Operational Rules (%s)\n\n%s\n\n", geminiSource, geminiContent))
		geminiRuleContent = geminiSB.String()
	}

	p.mu.Lock()
	if personaRuleContent != "" {
		if len(personaRuleContent) > MaxRuleFileSizeBytes {
			log.Printf("[Env] ERROR: user_persona.md exceeds 23 KB ceiling (%d bytes > %d bytes); Antigravity prompt truncation will occur!", len(personaRuleContent), MaxRuleFileSizeBytes)
			if p.lkgcPersonaRule != "" && len(p.lkgcPersonaRule) <= MaxRuleFileSizeBytes {
				log.Printf("[Env] Falling back to Last Known Good Configuration (LKGC) for user_persona.md to prevent prompt truncation")
				personaRuleContent = p.lkgcPersonaRule
			}
		} else {
			p.lkgcPersonaRule = personaRuleContent
		}
	} else {
		personaRuleContent = p.lkgcPersonaRule
		if personaRuleContent != "" {
			log.Printf("Using Last Known Good Configuration (LKGC) for user persona")
		}
	}

	if geminiRuleContent != "" {
		if len(geminiRuleContent) > MaxRuleFileSizeBytes {
			log.Printf("[Env] ERROR: system_invariants.md exceeds 23 KB ceiling (%d bytes > %d bytes); Antigravity prompt truncation will occur!", len(geminiRuleContent), MaxRuleFileSizeBytes)
			if p.lkgcGeminiRule != "" && len(p.lkgcGeminiRule) <= MaxRuleFileSizeBytes {
				log.Printf("[Env] Falling back to Last Known Good Configuration (LKGC) for system_invariants.md to prevent prompt truncation")
				geminiRuleContent = p.lkgcGeminiRule
			}
		} else {
			p.lkgcGeminiRule = geminiRuleContent
		}
	} else {
		geminiRuleContent = p.lkgcGeminiRule
		if geminiRuleContent != "" {
			log.Printf("Using Last Known Good Configuration (LKGC) for system invariants")
		}
	}

	if personaRuleContent == "" && geminiRuleContent == "" {
		p.mu.Unlock()
		return nil
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
		if err := os.Remove(stale); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("[Env] Warning removing stale rule file %s: %v", stale, err)
		}
	}

	if personaRuleContent != "" {
		primaryRuleFile := filepath.Join(primaryRulesDir, "user_persona.md")
		if err := p.writeAtomic(primaryRuleFile, personaRuleContent); err != nil {
			return fmt.Errorf("failed to write primary system rules: %w", err)
		}
		log.Printf("Configured always_on user persona in %s", primaryRuleFile)

		// Also sync to ~/.gemini/config/rules for compatibility
		configRuleFile := filepath.Join(configRulesDir, "user_persona.md")
		if err := p.writeAtomic(configRuleFile, personaRuleContent); err != nil {
			log.Printf("[Env] Warning syncing user persona to config rules: %v", err)
		}
	}

	if geminiRuleContent != "" {
		primaryGeminiFile := filepath.Join(primaryRulesDir, "system_invariants.md")
		if err := p.writeAtomic(primaryGeminiFile, geminiRuleContent); err != nil {
			return fmt.Errorf("failed to write primary system invariants: %w", err)
		}
		log.Printf("Configured always_on system invariants in %s", primaryGeminiFile)

		// Also sync to ~/.gemini/config/rules for compatibility
		configGeminiFile := filepath.Join(configRulesDir, "system_invariants.md")
		if err := p.writeAtomic(configGeminiFile, geminiRuleContent); err != nil {
			log.Printf("[Env] Warning syncing system invariants to config rules: %v", err)
		}
	}

	return nil
}
