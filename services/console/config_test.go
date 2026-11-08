package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadConsoleYAMLConfigs_TableDriven(t *testing.T) {
	t.Parallel()
	defaults := DefaultQuickLaunchLinks()
	if len(defaults) != 0 {
		t.Fatalf("expected 0 default quick launch links, got %d", len(defaults))
	}

	tests := []struct {
		name        string
		yamlData    string
		noFile      bool
		nonExistent bool
		wantLinks   []QuickLaunchLink
		wantRepos   []string
	}{
		{
			name:      "EmptyPath",
			noFile:    true,
			wantLinks: []QuickLaunchLink{},
			wantRepos: nil,
		},
		{
			name:        "NonExistentFile",
			nonExistent: true,
			wantLinks:   []QuickLaunchLink{},
			wantRepos:   nil,
		},
		{
			name: "InvalidYAML",
			yamlData: `console: [invalid-yaml
invalid: [yaml: broken
`,
			wantLinks: []QuickLaunchLink{},
			wantRepos: nil,
		},
		{
			name: "QuickLaunchLinks_ConsoleConfig",
			yamlData: `
console:
  quick_launch_links:
    - name: "HOME"
      url: "https://home.zylman.com"
      icon: "🏠"
      description: "Home Infrastructure Hub"
    - name: ""
      url: "https://skip.com"
    - name: "NO_URL"
      url: ""
`,
			wantLinks: []QuickLaunchLink{
				{
					Name:        "HOME",
					URL:         "https://home.zylman.com",
					Icon:        "🏠",
					Description: "Home Infrastructure Hub",
					Target:      "_blank",
					IsCustom:    true,
				},
			},
		},
		{
			name: "QuickLaunchLinks_RootQuickLinksFallback",
			yamlData: `
quick_links:
  - name: "HOME_ROOT"
    url: "https://home.root.zylman.com"
    target: "_self"
`,
			wantLinks: []QuickLaunchLink{
				{
					Name:     "HOME_ROOT",
					URL:      "https://home.root.zylman.com",
					Target:   "_self",
					IsCustom: true,
				},
			},
		},
		{
			name: "QuickLaunchLinks_TopLevelQuickLaunchLinks",
			yamlData: `
quick_launch_links:
  - name: "GRAFANA"
    url: "https://grafana.zylman.com"
    icon: "📊"
    description: "Grafana Telemetry"
`,
			wantLinks: []QuickLaunchLink{
				{
					Name:        "GRAFANA",
					URL:         "https://grafana.zylman.com",
					Icon:        "📊",
					Description: "Grafana Telemetry",
					Target:      "_blank",
					IsCustom:    true,
				},
			},
		},
		{
			name: "GitHubRepos_ConsoleBlock",
			yamlData: `
console:
  github_repos:
    - "azylman/aerial"
    - " azylman/aerial "
    - ""
    - "azylman/mirrormere"
`,
			wantLinks: []QuickLaunchLink{},
			wantRepos: []string{"azylman/aerial", "azylman/mirrormere"},
		},
		{
			name: "GitHubRepos_ConsoleReposFallback",
			yamlData: `
console:
  repos:
    - "azylman/coven"
`,
			wantLinks: []QuickLaunchLink{},
			wantRepos: []string{"azylman/coven"},
		},
		{
			name: "GitHubRepos_RootFallback",
			yamlData: `
github_repos:
  - "azylman/root-repo"
`,
			wantLinks: []QuickLaunchLink{},
			wantRepos: []string{"azylman/root-repo"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := ""
			if !tc.noFile {
				if tc.nonExistent {
					path = filepath.Join(t.TempDir(), "nonexistent.yaml")
				} else {
					path = filepath.Join(t.TempDir(), "config.yaml")
					_ = os.WriteFile(path, []byte(tc.yamlData), 0o644)
				}
			}

			if tc.wantLinks != nil || tc.noFile || tc.nonExistent || tc.yamlData != "" {
				gotLinks := loadQuickLaunchLinks(path)
				if len(gotLinks) != len(tc.wantLinks) {
					t.Fatalf("expected %d links, got %d", len(tc.wantLinks), len(gotLinks))
				}
				for i := range gotLinks {
					if gotLinks[i] != tc.wantLinks[i] {
						t.Errorf("link [%d] = %+v, want %+v", i, gotLinks[i], tc.wantLinks[i])
					}
				}
			}

			if tc.wantRepos != nil || tc.noFile || tc.nonExistent || tc.yamlData != "" {
				gotRepos := loadConsoleRepos(path)
				if len(gotRepos) != len(tc.wantRepos) {
					t.Fatalf("expected %d repos, got %d: %v vs %v", len(tc.wantRepos), len(gotRepos), tc.wantRepos, gotRepos)
				}
				for i := range gotRepos {
					if gotRepos[i] != tc.wantRepos[i] {
						t.Errorf("repo [%d] = %q, want %q", i, gotRepos[i], tc.wantRepos[i])
					}
				}
			}
		})
	}
}

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal := filepath.Join(tmpDir, "local_valid.yaml")
	_ = os.WriteFile(validLocal, []byte("port: '8084'\n"), 0o644)
	emptyLocal := filepath.Join(tmpDir, "local_empty.yaml")
	_ = os.WriteFile(emptyLocal, []byte(""), 0o644)
	nonExistent := filepath.Join(tmpDir, "nonexistent.yaml")
	fallbackFile := filepath.Join(tmpDir, "fallback.yaml")
	_ = os.WriteFile(fallbackFile, []byte("port: '8084'\n"), 0o644)

	fallback := "/config/console.yaml"
	tests := []struct {
		name         string
		envVal       string
		localPath    string
		fallbackPath string
		want         string
	}{
		{"env override takes precedence", "/custom/override.yaml", validLocal, fallback, "/custom/override.yaml"},
		{"local config non-empty exists", "", validLocal, fallback, validLocal},
		{"local config empty falls back to fallbackFile", "", emptyLocal, fallbackFile, fallbackFile},
		{"local config missing falls back to fallbackFile", "", nonExistent, fallbackFile, fallbackFile},
		{"fallback missing falls back to default", "", nonExistent, fallback, fallback},
		{"fallback empty falls back to DefaultConfigPath", "", nonExistent, "", DefaultConfigPath},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.envVal)
			oldLocal := localConfigPath
			localConfigPath = tc.localPath
			defer func() {
				localConfigPath = oldLocal
			}()

			if got := ResolveConfigPath(tc.fallbackPath); got != tc.want {
				t.Errorf("ResolveConfigPath(%q) = %q, want %q", tc.fallbackPath, got, tc.want)
			}
		})
	}
}

func TestLoadConfigFile_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()

	validYAML := `
port: "8084"
eri_url: "http://eri.aerial"
memory_url: "http://coven.aerial"
hangar_url: "http://hangar.aerial"
haro_url: "http://haro.aerial"
vanadis_url: "http://vanadis.aerial"
nomad_addr: "http://nomad.cluster:4646"
github_repos:
  - "azylman/aerial"
  - "azylman/aerial-config"
  - "azylman/aerial"
`
	validPath := filepath.Join(tmpDir, "console.yaml")
	_ = os.WriteFile(validPath, []byte(validYAML), 0o644)

	singleRepoYAML := `
port: "8084"
eri_url: "http://eri.aerial"
memory_url: "http://coven.aerial"
hangar_url: "http://hangar.aerial"
haro_url: "http://haro.aerial"
vanadis_url: "http://vanadis.aerial"
github_repos:
  - "azylman/single-repo"
config_path: "/custom/path/config.yaml"
`
	singleRepoPath := filepath.Join(tmpDir, "single_repo.yaml")
	_ = os.WriteFile(singleRepoPath, []byte(singleRepoYAML), 0o644)

	eriYAML := `
port: "8084"
eri_url: "http://eri-direct.aerial"
memory_url: "http://coven.aerial"
hangar_url: "http://hangar.aerial"
haro_url: "http://haro.aerial"
vanadis_url: "http://vanadis.aerial"
github_repos:
  - "azylman/aerial"
`
	eriPath := filepath.Join(tmpDir, "eri.yaml")
	_ = os.WriteFile(eriPath, []byte(eriYAML), 0o644)

	noBrainYAML := `
port: "8084"
memory_url: "http://coven.aerial"
hangar_url: "http://hangar.aerial"
haro_url: "http://haro.aerial"
vanadis_url: "http://vanadis.aerial"
github_repos:
  - "azylman/aerial"
`
	noBrainPath := filepath.Join(tmpDir, "no_brain.yaml")
	_ = os.WriteFile(noBrainPath, []byte(noBrainYAML), 0o644)

	kubeYAML := `
port: "8084"
eri_url: "http://eri.aerial"
memory_url: "http://coven.aerial"
hangar_url: "http://hangar.aerial"
haro_url: "http://haro.aerial"
vanadis_url: "http://vanadis.aerial"
kube_api_server: "http://kube.cluster:6443"
github_repos:
  - "azylman/aerial"
`
	kubePath := filepath.Join(tmpDir, "kube.yaml")
	_ = os.WriteFile(kubePath, []byte(kubeYAML), 0o644)

	badYAMLPath := filepath.Join(tmpDir, "bad.yaml")
	_ = os.WriteFile(badYAMLPath, []byte("port: [unclosed"), 0o644)

	tests := []struct {
		name              string
		path              string
		env               map[string]string
		nilLookup         bool
		callLoadConfig    bool
		wantErr           bool
		wantPort          string
		wantBrainURL      string
		wantMemoryURL     string
		wantHaroURL       string
		wantVanadisURL    string
		wantGHRepo        string
		wantGHRepos       []string
		wantGHToken       string
		wantKubeAPIServer string
		wantConfigPath    string
	}{
		{
			name: "valid full configuration with token and commit",
			path: validPath,
			env: map[string]string{
				"GITHUB_PAT": "pat-12345",
				"GIT_COMMIT": "abcdef012345",
			},
			wantPort:       "8084",
			wantBrainURL:   "http://eri.aerial",
			wantMemoryURL:  "http://coven.aerial",
			wantHaroURL:    "http://haro.aerial",
			wantVanadisURL: "http://vanadis.aerial",
			wantGHRepo:     "azylman/aerial",
			wantGHRepos:    []string{"azylman/aerial", "azylman/aerial-config"},
			wantGHToken:    "pat-12345",
			wantConfigPath: validPath,
		},
		{
			name: "single repo and custom config_path",
			path: singleRepoPath,
			env: map[string]string{
				"GITHUB_PAT": "token-from-env",
			},
			wantPort:       "8084",
			wantBrainURL:   "http://eri.aerial",
			wantMemoryURL:  "http://coven.aerial",
			wantHaroURL:    "http://haro.aerial",
			wantVanadisURL: "http://vanadis.aerial",
			wantGHRepo:     "azylman/single-repo",
			wantGHRepos:    []string{"azylman/single-repo"},
			wantGHToken:    "token-from-env",
			wantConfigPath: "/custom/path/config.yaml",
		},
		{
			name:           "eri_url in yaml",
			path:           eriPath,
			wantPort:       "8084",
			wantBrainURL:   "http://eri-direct.aerial",
			wantMemoryURL:  "http://coven.aerial",
			wantHaroURL:    "http://haro.aerial",
			wantVanadisURL: "http://vanadis.aerial",
			wantGHRepo:     "azylman/aerial",
			wantGHRepos:    []string{"azylman/aerial"},
			wantConfigPath: eriPath,
		},
		{
			name: "ERI_URL env fallback when absent from yaml",
			path: noBrainPath,
			env: map[string]string{
				"ERI_URL": "http://env-eri.aerial:8088",
			},
			wantPort:       "8084",
			wantBrainURL:   "http://env-eri.aerial:8088",
			wantMemoryURL:  "http://coven.aerial",
			wantHaroURL:    "http://haro.aerial",
			wantVanadisURL: "http://vanadis.aerial",
			wantGHRepo:     "azylman/aerial",
			wantGHRepos:    []string{"azylman/aerial"},
			wantConfigPath: noBrainPath,
		},
		{
			name:           "LoadConfig wrapper",
			path:           validPath,
			callLoadConfig: true,
			wantPort:       "8084",
			wantGHRepo:     "azylman/aerial",
		},
		{
			name:      "nil lookup defaults to os.Getenv",
			path:      validPath,
			nilLookup: true,
			wantPort:  "8084",
		},
		{
			name:              "kube_api_server in yaml",
			path:              kubePath,
			wantPort:          "8084",
			wantBrainURL:      "http://eri.aerial",
			wantMemoryURL:     "http://coven.aerial",
			wantHaroURL:       "http://haro.aerial",
			wantVanadisURL:    "http://vanadis.aerial",
			wantGHRepo:        "azylman/aerial",
			wantGHRepos:       []string{"azylman/aerial"},
			wantKubeAPIServer: "http://kube.cluster:6443",
			wantConfigPath:    kubePath,
		},
		{
			name: "KUBE_API_SERVER env fallback when absent from yaml",
			path: validPath,
			env: map[string]string{
				"KUBE_API_SERVER": "https://k8s-env.cluster:6443",
			},
			wantPort:          "8084",
			wantBrainURL:      "http://eri.aerial",
			wantMemoryURL:     "http://coven.aerial",
			wantHaroURL:       "http://haro.aerial",
			wantVanadisURL:    "http://vanadis.aerial",
			wantGHRepo:        "azylman/aerial",
			wantGHRepos:       []string{"azylman/aerial", "azylman/aerial-config"},
			wantKubeAPIServer: "https://k8s-env.cluster:6443",
			wantConfigPath:    validPath,
		},
		{
			name: "KUBERNETES_SERVICE_HOST and PORT env fallback",
			path: validPath,
			env: map[string]string{
				"KUBERNETES_SERVICE_HOST": "10.43.0.1",
				"KUBERNETES_SERVICE_PORT": "6443",
			},
			wantPort:          "8084",
			wantBrainURL:      "http://eri.aerial",
			wantMemoryURL:     "http://coven.aerial",
			wantHaroURL:       "http://haro.aerial",
			wantVanadisURL:    "http://vanadis.aerial",
			wantGHRepo:        "azylman/aerial",
			wantGHRepos:       []string{"azylman/aerial", "azylman/aerial-config"},
			wantKubeAPIServer: "https://10.43.0.1:6443",
			wantConfigPath:    validPath,
		},
		{
			name: "KUBERNETES_SERVICE_HOST default port 443 fallback",
			path: validPath,
			env: map[string]string{
				"KUBERNETES_SERVICE_HOST": "10.43.0.1",
			},
			wantPort:          "8084",
			wantBrainURL:      "http://eri.aerial",
			wantMemoryURL:     "http://coven.aerial",
			wantHaroURL:       "http://haro.aerial",
			wantVanadisURL:    "http://vanadis.aerial",
			wantGHRepo:        "azylman/aerial",
			wantGHRepos:       []string{"azylman/aerial", "azylman/aerial-config"},
			wantKubeAPIServer: "https://10.43.0.1:443",
			wantConfigPath:    validPath,
		},
		{
			name:    "empty path error",
			path:    "",
			wantErr: true,
		},
		{
			name:    "nonexistent file error",
			path:    filepath.Join(tmpDir, "nonexistent.yaml"),
			wantErr: true,
		},
		{
			name:    "bad yaml syntax error",
			path:    badYAMLPath,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.callLoadConfig {
				for k, v := range tc.env {
					t.Setenv(k, v)
				}
				cfg, err := LoadConfig(tc.path)
				if (err != nil) != tc.wantErr {
					t.Fatalf("LoadConfig() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantErr {
					return
				}
				if cfg.Port != tc.wantPort {
					t.Errorf("Port = %q, want %q", cfg.Port, tc.wantPort)
				}
				return
			}

			var lookup func(string) string
			if !tc.nilLookup {
				lookup = func(k string) string {
					if tc.env == nil {
						return ""
					}
					return tc.env[k]
				}
			}

			cfg, err := LoadConfigFile(tc.path, lookup)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if tc.wantPort != "" && cfg.Port != tc.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if tc.wantBrainURL != "" && (cfg.BrainURL != tc.wantBrainURL || cfg.EriURL != tc.wantBrainURL) {
				t.Errorf("BrainURL = %q, EriURL = %q, want %q", cfg.BrainURL, cfg.EriURL, tc.wantBrainURL)
			}
			if tc.wantMemoryURL != "" && cfg.MemoryURL != tc.wantMemoryURL {
				t.Errorf("MemoryURL = %q, want %q", cfg.MemoryURL, tc.wantMemoryURL)
			}
			if tc.wantHaroURL != "" && cfg.HaroURL != tc.wantHaroURL {
				t.Errorf("HaroURL = %q, want %q", cfg.HaroURL, tc.wantHaroURL)
			}
			if tc.wantVanadisURL != "" && cfg.VanadisURL != tc.wantVanadisURL {
				t.Errorf("VanadisURL = %q, want %q", cfg.VanadisURL, tc.wantVanadisURL)
			}
			if tc.wantGHRepo != "" && cfg.GHRepo != tc.wantGHRepo {
				t.Errorf("GHRepo = %q, want %q", cfg.GHRepo, tc.wantGHRepo)
			}
			if len(tc.wantGHRepos) > 0 && !reflect.DeepEqual(cfg.GHRepos, tc.wantGHRepos) {
				t.Errorf("GHRepos = %v, want %v", cfg.GHRepos, tc.wantGHRepos)
			}
			if tc.wantGHToken != "" && cfg.GHToken != tc.wantGHToken {
				t.Errorf("GHToken = %q, want %q", cfg.GHToken, tc.wantGHToken)
			}
			if tc.wantKubeAPIServer != "" && cfg.KubeAPIServer != tc.wantKubeAPIServer {
				t.Errorf("KubeAPIServer = %q, want %q", cfg.KubeAPIServer, tc.wantKubeAPIServer)
			}
			if tc.wantConfigPath != "" && cfg.ConfigPath != tc.wantConfigPath {
				t.Errorf("ConfigPath = %q, want %q", cfg.ConfigPath, tc.wantConfigPath)
			}
		})
	}
}
