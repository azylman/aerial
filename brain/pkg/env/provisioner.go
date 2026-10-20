package env

import (
	"context"
	"encoding/json"
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
	homeDir         string
	dataDir         string
	customSkillsDir string
	superpowersDir  string
	agentsSkillsDir string
	aerialRulesDir  string
	configRulesDir  string
	hooksDir        string

	mu sync.Mutex
}

var (
	statOptSkills    = os.Stat
	statAgentsSkills = os.Stat
)

// New creates a Provisioner targeting the given homeDir and dataDir.
func New(homeDir, dataDir string) *Provisioner {
	skillsDir := "/opt/skills"
	if _, err := statOptSkills(skillsDir); err != nil {
		if _, errLegacy := statOptSkills("/opt/superpowers/skills"); errLegacy == nil {
			skillsDir = "/opt/superpowers/skills"
		}
	}
	agentsSkillsDir := "/share/aerial/.agents/skills"
	if fi, err := statAgentsSkills(agentsSkillsDir); err != nil || !fi.IsDir() {
		agentsSkillsDir = "/app/.agents/skills"
	}
	return &Provisioner{
		homeDir:         strings.TrimSpace(homeDir),
		dataDir:         strings.TrimSpace(dataDir),
		customSkillsDir: "/share/aerial-config/custom-skills",
		superpowersDir:  skillsDir,
		agentsSkillsDir: agentsSkillsDir,
		aerialRulesDir:  "/share/aerial/rules",
		configRulesDir:  "/share/aerial-config/rules",
		hooksDir:        "/share/aerial/scripts/hooks",
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

// SetAerialRulesDir sets the search path for aerial system rules.
func (p *Provisioner) SetAerialRulesDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.aerialRulesDir = dir
}

// SetConfigRulesDir sets the search path for aerial-config user rules.
func (p *Provisioner) SetConfigRulesDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.configRulesDir = dir
}

// SetHooksDir sets the search path for lifecycle hooks.
func (p *Provisioner) SetHooksDir(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hooksDir = dir
}

func (p *Provisioner) runtimeHomeDir(t RuleTarget) string {
	return filepath.Join(p.dataDir, "runtimes", string(t))
}

// SyncSettings writes ~/.gemini/antigravity-cli/settings.json matching the authentication mode.
func (p *Provisioner) SyncSettings(apiKey, model string) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	configDir := filepath.Join(p.homeDir, ".gemini", "antigravity-cli")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	settingsPath := filepath.Join(configDir, "settings.json")
	settings := make(map[string]interface{})

	if data, err := os.ReadFile(settingsPath); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			log.Printf("Corrupted settings.json detected at %s, recreating: %v", settingsPath, err)
			settings = make(map[string]interface{})
		}
	}

	if apiKey == "" {
		// OAuth / ADC mode: modelProvider must NOT be set to "gemini"
		if mp, exists := settings["modelProvider"]; exists {
			log.Printf("Purging modelProvider=%v from %s (OAuth/ADC mode requires omitting modelProvider)", mp, settingsPath)
			delete(settings, "modelProvider")
		}
		delete(settings, "apiKey")
	} else {
		// API Key authentication mode
		settings["apiKey"] = apiKey
		settings["modelProvider"] = "gemini"
	}

	if model != "" {
		settings["model"] = model
	}

	updatedData, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings.json: %w", err)
	}

	return p.writeAtomic(settingsPath, string(updatedData))
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

	if err := p.SyncHooks(); err != nil {
		return fmt.Errorf("failed to sync hooks: %w", err)
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
		ignore(os.Remove(tmpName))
	}()

	if _, err := f.WriteString(content); err != nil {
ignore(f.Close())
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
ignore(os.Remove(targetPath))
		return os.Rename(tmpName, targetPath)
	}
	return nil
}

func ignore(err error) {
	if err != nil {
		return
	}
}
