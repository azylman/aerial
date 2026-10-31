package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/dashboard.yaml"

var localConfigPath = "/local/config.yaml"

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	if fallbackPath != "" {
		return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, fallbackPath)
	}
	return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, DefaultConfigPath, "dashboard.yaml", "config.yaml")
}

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
	QuickLinks       []QuickLaunchLink `yaml:"quick_links"`
	QuickLaunchLinks []QuickLaunchLink `yaml:"quick_launch_links"`
	GitHubRepos      []string          `yaml:"github_repos"`
	VanadisURL       string            `yaml:"vanadis_url"`
	HaroURL          string            `yaml:"haro_url"`
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
		custom = cfg.QuickLaunchLinks
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
	EriURL     string
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
	EriURL     string   `yaml:"eri_url"`
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

// LoadConfig loads configuration from path using os.Getenv.
func LoadConfig(path string) (DashboardConfig, error) {
	return LoadConfigFile(path, os.Getenv)
}

func parseDashboardRepos(rawRepos []string, fallbackRepo string) []string {
	if len(rawRepos) == 0 && strings.TrimSpace(fallbackRepo) != "" {
		rawRepos = []string{strings.TrimSpace(fallbackRepo)}
	}
	var repos []string
	seen := make(map[string]bool)
	for _, r := range rawRepos {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			repos = append(repos, trimmed)
		}
	}
	return repos
}

// LoadConfigFile loads and strictly validates dashboard configuration from a YAML file.
func LoadConfigFile(path string, lookup func(string) string) (DashboardConfig, error) {
	if lookup == nil {
		lookup = os.Getenv
	}

	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return DashboardConfig{}, fmt.Errorf("config: path cannot be empty")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return DashboardConfig{}, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
	}

	var ycfg DashboardYAMLConfig
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return DashboardConfig{}, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return DashboardConfig{}, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	eriURL := strings.TrimSpace(ycfg.EriURL)
	if eriURL == "" {
		eriURL = strings.TrimSpace(lookup("ERI_URL"))
	}
	if eriURL == "" {
		return DashboardConfig{}, fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	memoryURL := strings.TrimSpace(ycfg.MemoryURL)
	if memoryURL == "" {
		return DashboardConfig{}, fmt.Errorf("config: memory_url is required in %s", cleanPath)
	}
	hangarURL := strings.TrimSpace(ycfg.HangarURL)
	if hangarURL == "" {
		hangarURL = strings.TrimSpace(ycfg.GitSyncURL)
	}
	if hangarURL == "" {
		return DashboardConfig{}, fmt.Errorf("config: hangar_url is required in %s", cleanPath)
	}
	haroURL := strings.TrimSpace(ycfg.HaroURL)
	if haroURL == "" {
		return DashboardConfig{}, fmt.Errorf("config: haro_url is required in %s", cleanPath)
	}
	vanadisURL := strings.TrimSpace(ycfg.VanadisURL)
	if vanadisURL == "" {
		return DashboardConfig{}, fmt.Errorf("config: vanadis_url is required in %s", cleanPath)
	}

	repos := parseDashboardRepos(ycfg.GHRepos, ycfg.GHRepo)
	if len(repos) == 0 {
		return DashboardConfig{}, fmt.Errorf("config: github_repos is required in %s", cleanPath)
	}

	ghToken := strings.TrimSpace(lookup("GITHUB_PAT"))
	if ghToken == "" {
		ghToken = strings.TrimSpace(lookup("GITHUB_PERSONAL_ACCESS_TOKEN"))
	}

	gitCommit := normalizeGitCommit(lookup("GIT_COMMIT"))
	if gitCommit == "" {
		gitCommit = getGitCommit()
	}

	cfgPath := strings.TrimSpace(ycfg.ConfigPath)
	if cfgPath == "" {
		cfgPath = cleanPath
	}

	nomadAddr := strings.TrimSpace(ycfg.NomadAddr)

	return DashboardConfig{
		Port:       port,
		BrainURL:   eriURL,
		EriURL:     eriURL,
		MemoryURL:  memoryURL,
		GHRepo:     repos[0],
		GHRepos:    repos,
		GHToken:    ghToken,
		GitCommit:  gitCommit,
		GitSyncURL: hangarURL,
		HangarURL:  hangarURL,
		ConfigPath: cfgPath,
		NomadAddr:  nomadAddr,
		VanadisURL: vanadisURL,
		HaroURL:    haroURL,
	}, nil
}
