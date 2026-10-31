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

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/google/uuid"
)

// Provisioner manages filesystem provisioning and synchronization of ~/.gemini runtime environments.
type Provisioner struct {
	homeDir              string
	dataDir              string
	customSkillsDir      string
	superpowersDir       string
	agentsSkillsDir      string
	aerialRulesDir       string
	configRulesDir       string
	hooksDir             string
	oauthTokenSourcePath string

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
		homeDir:              strings.TrimSpace(homeDir),
		dataDir:              strings.TrimSpace(dataDir),
		customSkillsDir:      "/share/aerial-config/custom-skills",
		superpowersDir:       skillsDir,
		agentsSkillsDir:      agentsSkillsDir,
		aerialRulesDir:       "/share/aerial/rules",
		configRulesDir:       "/share/aerial-config/rules",
		hooksDir:             "/share/aerial/scripts/hooks",
		oauthTokenSourcePath: "/local/antigravity-oauth-token",
	}
}

// NewFromConfig creates a Provisioner using the directories configured in cfg.
func NewFromConfig(cfg *config.Config) *Provisioner {
	if cfg == nil {
		return New("", "")
	}
	return New(cfg.GeminiHomeDir(), cfg.DataDir())
}

func (p *Provisioner) runtimeHomeDir(t RuleTarget) string {
	return filepath.Join(p.dataDir, "runtimes", string(t))
}

func fileMissingOrEmpty(path string) bool {
	data, err := os.ReadFile(path)
	return err != nil || len(strings.TrimSpace(string(data))) == 0
}

func formatJetskiState(installID string) string {
	return fmt.Sprintf(`post_onboarding:  {
  completed_steps:  POST_ONBOARDING_STEP_TYPE_MANAGER_WELCOME
  completed_steps:  POST_ONBOARDING_STEP_TYPE_USAGE_MODE
  completed_steps:  POST_ONBOARDING_STEP_TYPE_AGENT_CONFIGURATION
  completed_steps:  POST_ONBOARDING_STEP_TYPE_ADD_WORKSPACE
}
installation_uuid:  %q
migrations:  {
  key:  3
  value:  MIGRATION_STATUS_COMPLETED
}
migrations:  {
  key:  4
  value:  MIGRATION_STATUS_COMPLETED
}
migrations:  {
  key:  5
  value:  MIGRATION_STATUS_COMPLETED
}
`, installID)
}

func (p *Provisioner) provisionOAuthState(configDir string) error {
	p.mu.Lock()
	srcPath := p.oauthTokenSourcePath
	p.mu.Unlock()

	tokenDest := filepath.Join(configDir, "antigravity-oauth-token")
	if srcPath != "" && fileMissingOrEmpty(tokenDest) {
		if srcData, err := os.ReadFile(srcPath); err == nil && len(strings.TrimSpace(string(srcData))) > 0 {
			if err := os.WriteFile(tokenDest, srcData, 0o600); err != nil {
				return fmt.Errorf("failed to seed antigravity-oauth-token: %w", err)
			}
		}
	}

	installIDPath := filepath.Join(configDir, "installation_id")
	var installID string
	if existingID, err := os.ReadFile(installIDPath); err == nil && len(strings.TrimSpace(string(existingID))) > 0 {
		installID = strings.TrimSpace(string(existingID))
	} else {
		installID = uuid.New().String()
		if err := os.WriteFile(installIDPath, []byte(installID+"\n"), 0o600); err != nil {
			return fmt.Errorf("failed to write installation_id: %w", err)
		}
	}

	jetskiPath := filepath.Join(configDir, "jetski_state.pbtxt")
	if fileMissingOrEmpty(jetskiPath) {
		if err := os.WriteFile(jetskiPath, []byte(formatJetskiState(installID)), 0o600); err != nil {
			return fmt.Errorf("failed to write jetski_state.pbtxt: %w", err)
		}
	}

	return nil
}

func (p *Provisioner) applyOAuthSettings(settings map[string]interface{}, configDir, settingsPath string) error {
	// OAuth / ADC mode: modelProvider must NOT be set to "gemini"
	if mp, exists := settings["modelProvider"]; exists {
		log.Printf("Purging modelProvider=%v from %s (OAuth/ADC mode requires omitting modelProvider)", mp, settingsPath)
		delete(settings, "modelProvider")
	}
	delete(settings, "apiKey")
	if _, exists := settings["useG1Credits"]; !exists {
		settings["useG1Credits"] = true
	}
	if _, exists := settings["trustedWorkspaces"]; !exists {
		settings["trustedWorkspaces"] = []string{"/app"}
	}
	return p.provisionOAuthState(configDir)
}

// SyncSettings writes ~/.gemini/antigravity-cli/settings.json matching the authentication mode.
func (p *Provisioner) SyncSettings(apiKey, model string) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	configDir := filepath.Join(p.homeDir, ".gemini", "antigravity-cli")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
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
		if err := p.applyOAuthSettings(settings, configDir, settingsPath); err != nil {
			return err
		}
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
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
