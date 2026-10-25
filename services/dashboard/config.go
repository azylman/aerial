package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type QuickLaunchLink struct {
	Name        string `json:"name" yaml:"name"`
	URL         string `json:"url" yaml:"url"`
	Icon        string `json:"icon,omitempty" yaml:"icon,omitempty"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Target      string `json:"target,omitempty" yaml:"target,omitempty"`
	IsCore      bool   `json:"is_core,omitempty" yaml:"is_core,omitempty"`
	IsCustom    bool   `json:"is_custom,omitempty" yaml:"is_custom,omitempty"`
}

var defaultGitCommitPaths = []string{
	"/share/aerial/.git/refs/heads/main",
	"/share/aerial/.git/refs/heads/master",
	"/share/aerial/.git/HEAD",
}

func getGitCommit(customPaths ...string) string {
	paths := defaultGitCommitPaths
	if len(customPaths) > 0 {
		paths = customPaths
	}

	for _, p := range paths {
		if data, err := os.ReadFile(p); err == nil {
			trimmed := strings.TrimSpace(string(data))
			if strings.HasPrefix(trimmed, "ref: ") {
				refPath := filepath.Join(filepath.Dir(p), strings.TrimPrefix(trimmed, "ref: "))
				if refData, err := os.ReadFile(refPath); err == nil {
					refTrimmed := strings.TrimSpace(string(refData))
					if len(refTrimmed) >= 7 {
						return refTrimmed[:7]
					}
				}
			} else if len(trimmed) >= 7 {
				return trimmed[:7]
			}
		}
	}

	return "latest"
}

// DefaultQuickLaunchLinks returns the core default gateway launch targets.
func DefaultQuickLaunchLinks() []QuickLaunchLink {
	return []QuickLaunchLink{}
}

type rawUserConfig struct {
	Dashboard struct {
		QuickLinks       []QuickLaunchLink `yaml:"quick_links"`
		QuickLaunchLinks []QuickLaunchLink `yaml:"quick_launch_links"`
		GitHubRepos      []string          `yaml:"github_repos"`
		Repos            []string          `yaml:"repos"`
		VanadisURL       string            `yaml:"vanadis_url"`
		HaroURL          string            `yaml:"haro_url"`
	} `yaml:"dashboard"`
	QuickLinks  []QuickLaunchLink `yaml:"quick_links"`
	GitHubRepos []string          `yaml:"github_repos"`
	VanadisURL  string            `yaml:"vanadis_url"`
	HaroURL     string            `yaml:"haro_url"`
}

// loadRawUserConfig parses and unmarshals rawUserConfig from the given path.
func loadRawUserConfig(configPath string) (rawUserConfig, bool) {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return rawUserConfig{}, false
	}
	configPath = filepath.Clean(configPath)

	data, err := os.ReadFile(configPath)
	if err != nil {
		return rawUserConfig{}, false
	}

	var cfg rawUserConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Printf("[Dashboard] Warning: failed to parse %s: %v", configPath, err)
		return rawUserConfig{}, false
	}
	return cfg, true
}

// loadDashboardVanadisURL parses the Vanadis upstream URL configured in config.yaml.
func loadDashboardVanadisURL(configPath string) string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return ""
	}

	val := strings.TrimSpace(cfg.Dashboard.VanadisURL)
	if val == "" {
		val = strings.TrimSpace(cfg.VanadisURL)
	}
	return val
}

// loadDashboardHaroURL parses the Haro upstream URL configured in config.yaml.
func loadDashboardHaroURL(configPath string) string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return ""
	}

	val := strings.TrimSpace(cfg.Dashboard.HaroURL)
	if val == "" {
		val = strings.TrimSpace(cfg.HaroURL)
	}
	return val
}

// loadDashboardRepos parses the list of GitHub repositories configured for status dashboard monitoring.
func loadDashboardRepos(configPath string) []string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return nil
	}

	rawList := cfg.Dashboard.GitHubRepos
	if len(rawList) == 0 {
		rawList = cfg.Dashboard.Repos
	}
	if len(rawList) == 0 {
		rawList = cfg.GitHubRepos
	}

	var repos []string
	seen := make(map[string]bool)
	for _, r := range rawList {
		trimmed := strings.TrimSpace(r)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		repos = append(repos, trimmed)
	}

	return repos
}

// loadQuickLaunchLinks merges default core links with custom links defined in config.yaml.
func loadQuickLaunchLinks(configPath string) []QuickLaunchLink {
	links := DefaultQuickLaunchLinks()

	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return links
	}

	custom := cfg.Dashboard.QuickLinks
	if len(custom) == 0 {
		custom = cfg.Dashboard.QuickLaunchLinks
	}
	if len(custom) == 0 {
		custom = cfg.QuickLinks
	}

	for _, l := range custom {
		if l.Name == "" || l.URL == "" {
			continue
		}
		if l.Target == "" {
			l.Target = "_blank"
		}
		l.IsCustom = true
		links = append(links, l)
	}

	return links
}

// DashboardConfig holds configuration settings for the dashboard web server.
type DashboardConfig struct {
	Port       string
	BrainURL   string
	MemoryURL  string
	GHRepo     string
	GHRepos    []string
	GHToken    string
	GitCommit  string
	GitSyncURL string
	HangarURL  string
	ConfigPath string
	APIBaseURL string
	NomadAddr  string
	VanadisURL string
	HaroURL    string
}

// DashboardYAMLConfig defines the declarative file schema for dashboard.yaml.
type DashboardYAMLConfig struct {
	Port       string   `yaml:"port"`
	BrainURL   string   `yaml:"brain_url"`
	MemoryURL  string   `yaml:"memory_url"`
	HangarURL  string   `yaml:"hangar_url"`
	GitSyncURL string   `yaml:"gitsync_url"`
	GHRepo     string   `yaml:"github_repo"`
	GHRepos    []string `yaml:"github_repos"`
	ConfigPath string   `yaml:"config_path"`
	NomadAddr  string   `yaml:"nomad_addr"`
	VanadisURL string   `yaml:"vanadis_url"`
	HaroURL    string   `yaml:"haro_url"`
}

// NewDashboardConfigFromLookup parses DashboardConfig using a key lookup function with YAML config file support.
func NewDashboardConfigFromLookup(lookup func(string) string) DashboardConfig {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}

	cfgFile := strings.TrimSpace(lookup("CONFIG_PATH"))
	if cfgFile == "" && flag.Lookup("test.v") == nil {
		if _, err := os.Stat("/config/config.yaml"); err == nil {
			cfgFile = "/config/config.yaml"
		} else if _, err := os.Stat("/share/aerial-config/services/dashboard/dashboard.yaml"); err == nil {
			cfgFile = "/share/aerial-config/services/dashboard/dashboard.yaml"
		}
	}

	var ycfg DashboardYAMLConfig
	if cfgFile != "" {
		if data, err := os.ReadFile(cfgFile); err == nil {
			if err := yaml.Unmarshal(data, &ycfg); err != nil {
				log.Printf("[dashboard] Warning: Failed to parse %s: %v", cfgFile, err)
			}
		}
	}

	brainURL := strings.TrimSpace(lookup("BRAIN_URL"))
	if brainURL == "" && ycfg.BrainURL != "" {
		brainURL = strings.TrimSpace(ycfg.BrainURL)
	}
	if brainURL == "" {
		brainURL = "http://brain:8080"
	}

	memoryURL := strings.TrimSpace(lookup("MEMORY_MCP_URL"))
	if memoryURL == "" && ycfg.MemoryURL != "" {
		memoryURL = strings.TrimSpace(ycfg.MemoryURL)
	}
	if memoryURL == "" {
		memoryURL = "http://coven.aerial"
	}

	ghRepo, ghRepos := resolveDashboardGitHubRepos(lookup, ycfg.GHRepos, ycfg.GHRepo)

	ghToken := strings.TrimSpace(lookup("GITHUB_PAT"))
	if ghToken == "" {
		ghToken = strings.TrimSpace(lookup("GITHUB_PERSONAL_ACCESS_TOKEN"))
	}

	port := strings.TrimSpace(lookup("PORT"))
	if port == "" && ycfg.Port != "" {
		port = strings.TrimSpace(ycfg.Port)
	}
	if port == "" {
		port = "8080"
	}

	hangarURL := strings.TrimSpace(lookup("HANGAR_URL"))
	if hangarURL == "" {
		hangarURL = strings.TrimSpace(lookup("GITSYNC_URL"))
	}
	if hangarURL == "" && ycfg.HangarURL != "" {
		hangarURL = strings.TrimSpace(ycfg.HangarURL)
	}
	if hangarURL == "" && ycfg.GitSyncURL != "" {
		hangarURL = strings.TrimSpace(ycfg.GitSyncURL)
	}
	if hangarURL == "" {
		hangarURL = "http://hangar:8080"
	}

	configPath := strings.TrimSpace(lookup("AERIAL_CONFIG_PATH"))
	if configPath == "" && ycfg.ConfigPath != "" {
		configPath = strings.TrimSpace(ycfg.ConfigPath)
	}
	if configPath == "" {
		configPath = "/share/aerial-config/config.yaml"
	}

	gitCommit := normalizeGitCommit(lookup("GIT_COMMIT"))
	if gitCommit == "" {
		gitCommit = "latest"
	}

	nomadAddr := strings.TrimSpace(lookup("NOMAD_ADDR"))
	if nomadAddr == "" && ycfg.NomadAddr != "" {
		nomadAddr = strings.TrimSpace(ycfg.NomadAddr)
	}
	if nomadAddr == "" && flag.Lookup("test.v") == nil {
		nomadAddr = "http://127.0.0.1:4646"
	}

	vanadisURL := resolveDashboardVanadisURL(configPath, ycfg.VanadisURL)
	haroURL := resolveDashboardHaroURL(lookup, configPath, ycfg.HaroURL)

	return DashboardConfig{
		Port:       port,
		BrainURL:   brainURL,
		MemoryURL:  memoryURL,
		GHRepo:     ghRepo,
		GHRepos:    ghRepos,
		GHToken:    ghToken,
		GitSyncURL: hangarURL,
		HangarURL:  hangarURL,
		ConfigPath: configPath,
		GitCommit:  gitCommit,
		NomadAddr:  nomadAddr,
		VanadisURL: vanadisURL,
		HaroURL:    haroURL,
	}
}

// NewDashboardConfigFromEnv parses DashboardConfig from environment variables.
func NewDashboardConfigFromEnv() DashboardConfig {
	cfg := NewDashboardConfigFromLookup(os.Getenv)
	if cfg.GitCommit == "latest" || cfg.GitCommit == "" {
		cfg.GitCommit = getGitCommit()
	}
	return cfg
}
