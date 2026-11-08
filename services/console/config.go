package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/console.yaml"

var localConfigPath = "/local/config.yaml"

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	if fallbackPath != "" {
		return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, fallbackPath)
	}
	return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, DefaultConfigPath, "console.yaml", "config.yaml")
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

// DefaultQuickLaunchLinks returns the core default gateway launch targets.
func DefaultQuickLaunchLinks() []QuickLaunchLink {
	return []QuickLaunchLink{}
}

type rawUserConfig struct {
	Console struct {
		QuickLinks       []QuickLaunchLink `yaml:"quick_links"`
		QuickLaunchLinks []QuickLaunchLink `yaml:"quick_launch_links"`
		GitHubRepos      []string          `yaml:"github_repos"`
		Repos            []string          `yaml:"repos"`
		VanadisURL       string            `yaml:"vanadis_url"`
		HaroURL          string            `yaml:"haro_url"`
	} `yaml:"console"`
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
		log.Printf("[Console] Warning: failed to parse %s: %v", configPath, err)
		return rawUserConfig{}, false
	}
	return cfg, true
}

// loadConsoleRepos parses the list of GitHub repositories configured for console monitoring.
func loadConsoleRepos(configPath string) []string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return nil
	}

	rawList := cfg.Console.GitHubRepos
	if len(rawList) == 0 {
		rawList = cfg.Console.Repos
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

	custom := cfg.Console.QuickLinks
	if len(custom) == 0 {
		custom = cfg.Console.QuickLaunchLinks
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

// ConsoleConfig holds configuration settings for the console web server.
type ConsoleConfig struct {
	Port          string
	BrainURL      string
	EriURL        string
	MemoryURL     string
	GHRepo        string
	GHRepos       []string
	GHToken       string
	GitCommit     string
	GitSyncURL    string
	ConfigPath    string
	APIBaseURL    string
	KubeAPIServer string
	VanadisURL    string
	HaroURL       string
}

// ConsoleYAMLConfig defines the declarative file schema for console.yaml.
type ConsoleYAMLConfig struct {
	Port          string   `yaml:"port"`
	EriURL        string   `yaml:"eri_url"`
	MemoryURL     string   `yaml:"memory_url"`
	GHRepos       []string `yaml:"github_repos"`
	ConfigPath    string   `yaml:"config_path"`
	KubeAPIServer string   `yaml:"kube_api_server"`
	VanadisURL    string   `yaml:"vanadis_url"`
	HaroURL       string   `yaml:"haro_url"`
}

// LoadConfig loads configuration from path using os.Getenv.
func LoadConfig(path string) (ConsoleConfig, error) {
	return LoadConfigFile(path, os.Getenv)
}

func parseConsoleRepos(rawRepos []string) []string {
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

func firstNonEmptyPort(val, fallback string) string {
	trimmed := strings.TrimSpace(val)
	if trimmed != "" {
		return trimmed
	}
	return fallback
}

// LoadConfigFile loads and strictly validates console configuration from a YAML file.
func LoadConfigFile(path string, lookup func(string) string) (ConsoleConfig, error) {
	if lookup == nil {
		lookup = os.Getenv
	}

	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return ConsoleConfig{}, fmt.Errorf("config: path cannot be empty")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return ConsoleConfig{}, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
	}

	var ycfg ConsoleYAMLConfig
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return ConsoleConfig{}, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return ConsoleConfig{}, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	eriURL := strings.TrimSpace(ycfg.EriURL)
	if eriURL == "" {
		eriURL = strings.TrimSpace(lookup("ERI_URL"))
	}
	if eriURL == "" {
		return ConsoleConfig{}, fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	memoryURL := strings.TrimSpace(ycfg.MemoryURL)
	if memoryURL == "" {
		return ConsoleConfig{}, fmt.Errorf("config: memory_url is required in %s", cleanPath)
	}
	haroURL := strings.TrimSpace(ycfg.HaroURL)
	if haroURL == "" {
		return ConsoleConfig{}, fmt.Errorf("config: haro_url is required in %s", cleanPath)
	}
	vanadisURL := strings.TrimSpace(ycfg.VanadisURL)
	if vanadisURL == "" {
		return ConsoleConfig{}, fmt.Errorf("config: vanadis_url is required in %s", cleanPath)
	}

	repos := parseConsoleRepos(ycfg.GHRepos)
	if len(repos) == 0 {
		return ConsoleConfig{}, fmt.Errorf("config: github_repos is required in %s", cleanPath)
	}

	ghToken := strings.TrimSpace(lookup("GITHUB_PAT"))

	cfgPath := strings.TrimSpace(ycfg.ConfigPath)
	if cfgPath == "" {
		cfgPath = cleanPath
	}

	kubeAPIServer := strings.TrimSpace(ycfg.KubeAPIServer)
	if kubeAPIServer == "" {
		kubeAPIServer = strings.TrimSpace(lookup("KUBE_API_SERVER"))
	}
	if kubeAPIServer == "" {
		host := strings.TrimSpace(lookup("KUBERNETES_SERVICE_HOST"))
		if host != "" {
			port := firstNonEmptyPort(lookup("KUBERNETES_SERVICE_PORT"), "443")
			kubeAPIServer = "https://" + net.JoinHostPort(host, port)
		}
	}

	return ConsoleConfig{
		Port:          port,
		BrainURL:      eriURL,
		EriURL:        eriURL,
		MemoryURL:     memoryURL,
		GHRepo:        repos[0],
		GHRepos:       repos,
		GHToken:       ghToken,
		ConfigPath:    cfgPath,
		KubeAPIServer: kubeAPIServer,
		VanadisURL:    vanadisURL,
		HaroURL:       haroURL,
	}, nil
}
