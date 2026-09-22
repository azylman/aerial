package env

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/azylman/aerial/brain/pkg/config"
)

// Provisioner manages filesystem provisioning and synchronization of ~/.gemini runtime environments.
type Provisioner struct {
	homeDir                 string
	dataDir                 string
	customSkillsDir         string
	superpowersDir          string
	agentsSkillsDir         string
	agentInstructionsPaths  []string
	systemInstructionsPaths []string

	mu                sync.Mutex
	lkgcPersona       string
	lkgcPersonaSource string
	lkgcGemini        string
	lkgcGeminiSource  string
	lkgcPersonaRule   string
	lkgcGeminiRule    string
}

// New creates a Provisioner targeting the given homeDir and dataDir.
func New(homeDir, dataDir string) *Provisioner {
	skillsDir := "/opt/skills"
	if _, err := os.Stat(skillsDir); err != nil {
		if _, errLegacy := os.Stat("/opt/superpowers/skills"); errLegacy == nil {
			skillsDir = "/opt/superpowers/skills"
		}
	}
	return &Provisioner{
		homeDir:                 strings.TrimSpace(homeDir),
		dataDir:                 strings.TrimSpace(dataDir),
		customSkillsDir:         "/share/aerial-config/custom-skills",
		superpowersDir:          skillsDir,
		agentsSkillsDir:         "/app/.agents/skills",
		agentInstructionsPaths:  DefaultAgentInstructionsSearchPaths,
		systemInstructionsPaths: DefaultSystemInstructionsSearchPaths,
	}
}

// NewFromConfig creates a Provisioner using the directories configured in cfg.
func NewFromConfig(cfg *config.Config) *Provisioner {
	if cfg == nil {
		return New("", "")
	}
	return New(cfg.GeminiHomeDir(), cfg.DataDir())
}

// SetCustomSkillsDir sets the search path for custom user skills.
func (p *Provisioner) SetCustomSkillsDir(dir string) {
	p.customSkillsDir = dir
}

// SetSuperpowersDir sets the search path for methodology skills.
func (p *Provisioner) SetSuperpowersDir(dir string) {
	p.superpowersDir = dir
}

// SetAgentsSkillsDir sets the search path for application built-in skills.
func (p *Provisioner) SetAgentsSkillsDir(dir string) {
	p.agentsSkillsDir = dir
}

// SetAgentInstructionsSearchPaths sets the search paths for AGENTS.md instructions.
func (p *Provisioner) SetAgentInstructionsSearchPaths(paths []string) {
	p.agentInstructionsPaths = paths
}

// SetSystemInstructionsSearchPaths sets the search paths for GEMINI.md system instructions.
func (p *Provisioner) SetSystemInstructionsSearchPaths(paths []string) {
	p.systemInstructionsPaths = paths
}

// HomeDir returns the target home directory for this Provisioner.
func (p *Provisioner) HomeDir() string {
	return p.homeDir
}

// DataDir returns the persistent data directory for this Provisioner.
func (p *Provisioner) DataDir() string {
	return p.dataDir
}

// Sync provisions all runtime requirements into ~/.gemini based on current configuration.
func (p *Provisioner) Sync(ctx context.Context, cfg *config.Config) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	var cur *config.ConfigData
	if cfg != nil {
		cur = cfg.Current()
	} else {
		cur = config.DefaultConfigData()
	}

	if err := p.SyncSettings(cur.APIKey, cur.Model); err != nil {
		return fmt.Errorf("failed to sync settings: %w", err)
	}

	if err := p.SyncRules(cur.SystemPrompt); err != nil {
		return fmt.Errorf("failed to sync rules: %w", err)
	}

	if err := p.SyncMCP(ctx, cfg); err != nil {
		return fmt.Errorf("failed to sync mcp config: %w", err)
	}

	if err := p.SyncSkills(); err != nil {
		return fmt.Errorf("failed to sync skills: %w", err)
	}

	return nil
}

func (p *Provisioner) writeAtomic(targetPath, content string) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	pattern := fmt.Sprintf(".%s.tmp.*", filepath.Base(targetPath))
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer func() {
		if rmErr := os.Remove(tmpName); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			log.Printf("[Env] Warning removing temporary file %s: %v", tmpName, rmErr)
		}
	}()

	if _, err := f.WriteString(content); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Env] Warning closing temporary file %s: %v", tmpName, closeErr)
		}
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
		if rmErr := os.Remove(targetPath); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			log.Printf("[Env] Warning removing target file %s prior to fallback rename: %v", targetPath, rmErr)
		}
		return os.Rename(tmpName, targetPath)
	}
	return nil
}
