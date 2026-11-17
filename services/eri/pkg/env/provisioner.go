package env

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

// sharedAgentsSkillsDir is the built-in skills tree on the read-only engine checkout.
const sharedAgentsSkillsDir = "/share/aerial/.agents/skills"

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
	codexAuthSourcePath  string
	mcpProxy             *MCPProxyRoutes

	mu sync.Mutex
}

var (
	getEuid          = os.Geteuid
	chownPath        = os.Lchown
	chmodPath        = os.Chmod
	statOptSkills    = os.Stat
	statAgentsSkills = os.Stat
	rootHomeDir      = "/root"
)

// New creates a Provisioner targeting the given homeDir and dataDir.
func New(homeDir, dataDir string, sources config.CredentialSources) (*Provisioner, error) {
	if err := ValidateCredentialSources(sources); err != nil {
		return nil, err
	}
	skillsDir := "/opt/skills"
	if _, err := statOptSkills(skillsDir); err != nil {
		if _, errLegacy := statOptSkills("/opt/superpowers/skills"); errLegacy == nil {
			skillsDir = "/opt/superpowers/skills"
		}
	}
	agentsSkillsDir := sharedAgentsSkillsDir
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
		oauthTokenSourcePath: strings.TrimSpace(sources.OAuth.Path),
		codexAuthSourcePath:  strings.TrimSpace(sources.Codex.Path),
	}, nil
}

// NewFromConfig creates a Provisioner using the directories configured in cfg.
func NewFromConfig(cfg *config.Config) (*Provisioner, error) {
	if cfg == nil {
		return nil, &CredentialError{Err: errors.New("credential configuration is required")}
	}
	cur := cfg.Current()
	return New(cur.GeminiHomeDir, cur.DataDir, cur.CredentialSources)
}

type CredentialError struct{ Err error }

func (e *CredentialError) Error() string { return e.Err.Error() }
func (e *CredentialError) Unwrap() error { return e.Err }
func ValidateCredentialSources(sources config.CredentialSources) error {
	if err := sources.Validate(); err != nil {
		return &CredentialError{Err: err}
	}
	for _, entry := range []struct {
		name   string
		source config.CredentialSource
	}{
		{"oauth", sources.OAuth}, {"codex", sources.Codex},
	} {
		if entry.source.Disabled {
			continue
		}
		if _, err := os.ReadFile(strings.TrimSpace(entry.source.Path)); err != nil {
			return &CredentialError{Err: fmt.Errorf("credential_sources.%s: %w", entry.name, err)}
		}
	}
	return nil
}

// WithMCPProxy routes the agents' HTTP MCP servers through eri's tracing proxy; nil (tracing
// disabled) writes MCP configs verbatim.
func (p *Provisioner) WithMCPProxy(routes *MCPProxyRoutes) *Provisioner {
	if p != nil {
		p.mcpProxy = routes
	}
	return p
}

func (p *Provisioner) runtimeHomeDir(t RuleTarget) string {
	return filepath.Join(p.dataDir, "runtimes", string(t))
}

func fileMissingOrEmpty(path string) bool {
	data, err := os.ReadFile(path)
	return err != nil || strings.TrimSpace(string(data)) == ""
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
		srcData, err := os.ReadFile(srcPath)
		if err != nil {
			return &CredentialError{Err: fmt.Errorf("failed to read oauth source: %w", err)}
		}
		if strings.TrimSpace(string(srcData)) != "" {
			if err := os.WriteFile(tokenDest, srcData, 0o600); err != nil {
				return fmt.Errorf("failed to seed antigravity-oauth-token: %w", err)
			}
		}
	}

	installIDPath := filepath.Join(configDir, "installation_id")
	var installID string
	if existingID, err := os.ReadFile(installIDPath); err == nil && strings.TrimSpace(string(existingID)) != "" {
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

func seedCodexAuth(srcPath, canonicalCodexDir, destAuthPath, seedSHAPath string) error {
	if srcPath == "" {
		return nil
	}
	srcData, err := os.ReadFile(srcPath)
	if err != nil {
		return &CredentialError{Err: fmt.Errorf("failed to read codex auth source: %w", err)}
	}
	if len(bytes.TrimSpace(srcData)) == 0 {
		return nil
	}
	srcSHA := fmt.Sprintf("%x", sha256.Sum256(srcData))
	destMissingOrEmpty := fileMissingOrEmpty(destAuthPath)

	var seedSHADiffers bool
	if seedData, sErr := os.ReadFile(seedSHAPath); sErr != nil || strings.TrimSpace(string(seedData)) != srcSHA {
		seedSHADiffers = true
	}

	if !destMissingOrEmpty && !seedSHADiffers {
		return nil
	}
	if err := os.MkdirAll(canonicalCodexDir, 0o755); err != nil {
		return fmt.Errorf("failed to create codex dir %s: %w", canonicalCodexDir, err)
	}
	if err := os.WriteFile(destAuthPath, srcData, 0o600); err != nil {
		return fmt.Errorf("failed to write %s: %w", destAuthPath, err)
	}
	if err := os.WriteFile(seedSHAPath, []byte(srcSHA+"\n"), 0o600); err != nil {
		return fmt.Errorf("failed to write %s: %w", seedSHAPath, err)
	}
	return nil
}

func (p *Provisioner) symlinkCodexAuthToRuntimes(destAuthPath string) {
	if p.dataDir == "" {
		return
	}
	if _, err := os.Stat(destAuthPath); err != nil {
		return
	}
	primaryCodexDir := filepath.Join(p.homeDir, ".codex")
	ignore(os.MkdirAll(primaryCodexDir, 0o755))
	p.linkOrCopyAsset(destAuthPath, filepath.Join(primaryCodexDir, "auth.json"))

	for _, target := range allRuntimeTargets {
		rtCodexDir := filepath.Join(p.runtimeHomeDir(target), ".codex")
		ignore(os.MkdirAll(rtCodexDir, 0o755))
		p.linkOrCopyAsset(destAuthPath, filepath.Join(rtCodexDir, "auth.json"))
	}
}

func (p *Provisioner) provisionCodexAuthState() error {
	if p == nil || p.homeDir == "" {
		return nil
	}

	p.mu.Lock()
	srcPath := p.codexAuthSourcePath
	p.mu.Unlock()

	var canonicalCodexDir string
	if p.dataDir != "" {
		canonicalCodexDir = filepath.Join(p.dataDir, "codex")
	} else {
		canonicalCodexDir = filepath.Join(p.homeDir, ".codex")
	}

	destAuthPath := filepath.Join(canonicalCodexDir, "auth.json")
	seedSHAPath := filepath.Join(canonicalCodexDir, ".auth.seed-sha256")

	if err := seedCodexAuth(srcPath, canonicalCodexDir, destAuthPath, seedSHAPath); err != nil {
		return err
	}

	p.symlinkCodexAuthToRuntimes(destAuthPath)
	return nil
}

func (p *Provisioner) applyOAuthSettings(settings map[string]any, configDir, settingsPath string) error {
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
	settings := make(map[string]any)

	if data, err := os.ReadFile(settingsPath); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			log.Printf("Corrupted settings.json detected at %s, recreating: %v", settingsPath, err)
			settings = make(map[string]any)
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
		_ = cfg.Engine()
	} else {
		cur = config.DefaultConfigData()
	}

	if cfg != nil {
		if err := ValidateCredentialSources(cur.CredentialSources); err != nil {
			return err
		}
		p.mu.Lock()
		p.oauthTokenSourcePath = strings.TrimSpace(cur.CredentialSources.OAuth.Path)
		p.codexAuthSourcePath = strings.TrimSpace(cur.CredentialSources.Codex.Path)
		p.mu.Unlock()
	}
	if err := p.SyncSettings(cur.APIKey, cur.Model); err != nil {
		return fmt.Errorf("failed to sync settings: %w", err)
	}

	if err := p.provisionCodexAuthState(); err != nil {
		return fmt.Errorf("failed to provision codex auth state: %w", err)
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

	p.chownProvisionedPaths()

	return nil
}

func chownTree(root string) {
	ignore(filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && info.Mode()&os.ModeSymlink == 0 {
			ignore(chownPath(path, 1000, 1000))
		}
		return nil
	}))
}

func (p *Provisioner) chownProvisionedPaths() {
	if getEuid() != 0 {
		return
	}
	if p.homeDir == rootHomeDir {
		ignore(chmodPath(rootHomeDir, 0o755))
	}
	chownTree(filepath.Join(p.homeDir, ".gemini"))
	chownTree(filepath.Join(p.homeDir, ".claude"))
	chownTree(filepath.Join(p.homeDir, ".codex"))

	for _, target := range allRuntimeTargets {
		rtHome := p.runtimeHomeDir(target)
		ignore(chownPath(rtHome, 1000, 1000))
		base := filepath.Join(rtHome, ".gemini")
		ignore(chownPath(base, 1000, 1000))
		for _, sub := range []string{"rules", "config", "brain", "conversations"} {
			chownTree(filepath.Join(base, sub))
		}
		cliDir := filepath.Join(base, "antigravity-cli")
		ignore(chownPath(cliDir, 1000, 1000))
		if entries, err := os.ReadDir(cliDir); err == nil {
			for _, entry := range entries {
				if entry.Type()&os.ModeSymlink == 0 {
					ignore(chownPath(filepath.Join(cliDir, entry.Name()), 1000, 1000))
				}
			}
		}
		rtClaude := filepath.Join(rtHome, ".claude")
		ignore(chownPath(rtClaude, 1000, 1000))
		chownTree(rtClaude)
		rtCodex := filepath.Join(rtHome, ".codex")
		ignore(chownPath(rtCodex, 1000, 1000))
		chownTree(rtCodex)
	}
	if p.dataDir != "" {
		ignore(chownPath(p.dataDir, 1000, 1000))
		ignore(chownPath(filepath.Join(p.dataDir, "runtimes"), 1000, 1000))
		for _, dir := range []string{
			filepath.Join(p.dataDir, "brain"),
			filepath.Join(p.dataDir, "conversations"),
			filepath.Join(p.dataDir, "cache"),
			filepath.Join(p.dataDir, "claude"),
			filepath.Join(p.dataDir, "codex"),
		} {
			chownTree(dir)
		}
	}
}

func (*Provisioner) writeAtomic(targetPath, content string) error {
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
