package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGetGitCommit(t *testing.T) {
	if c := normalizeGitCommit("abcdef"); c != "abcdef" {
		t.Errorf("expected 'abcdef', got %q", c)
	}
	if c := normalizeGitCommit("abcdef0123456789"); c != "abcdef0" {
		t.Errorf("expected 'abcdef0', got %q", c)
	}

	tests := []struct {
		name        string
		headContent string
		refRelPath  string
		refContent  string
		noFile      bool
		defaultCall bool
		want        string
	}{
		{
			name:        "DefaultLookup",
			defaultCall: true,
		},
		{
			name:        "FromRefFile",
			headContent: "ref: refs/heads/main\n",
			refRelPath:  "refs/heads/main",
			refContent:  "1234567890abcdef\n",
			want:        "1234567",
		},
		{
			name:        "FromDirectSHAFile",
			headContent: "fedcba9876543210\n",
			want:        "fedcba9",
		},
		{
			name:   "FallbackToLatest",
			noFile: true,
			want:   "latest",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.defaultCall {
				commit := getGitCommit()
				if commit == "" {
					t.Errorf("expected non-empty git commit")
				}
				if len(commit) > 7 {
					t.Errorf("expected short commit <= 7 chars, got %s", commit)
				}
				return
			}
			tempDir := t.TempDir()
			headFile := filepath.Join(tempDir, "HEAD")
			if tc.noFile {
				headFile = filepath.Join(tempDir, "nonexistent")
			} else {
				if tc.refRelPath != "" {
					refFile := filepath.Join(tempDir, tc.refRelPath)
					_ = os.MkdirAll(filepath.Dir(refFile), 0o755)
					_ = os.WriteFile(refFile, []byte(tc.refContent), 0o644)
				}
				_ = os.WriteFile(headFile, []byte(tc.headContent), 0o644)
			}
			if c := getGitCommit(headFile); c != tc.want {
				t.Errorf("expected %q, got %q", tc.want, c)
			}
		})
	}
}

func TestLoadDashboardYAMLConfigs_TableDriven(t *testing.T) {
	defaults := DefaultQuickLaunchLinks()
	if len(defaults) != 0 {
		t.Fatalf("expected 0 default quick launch links, got %d", len(defaults))
	}

	tests := []struct {
		name           string
		yamlData       string
		noFile         bool
		nonExistent    bool
		wantLinks      []QuickLaunchLink
		wantRepos      []string
		wantVanadisURL string
		wantHaroURL    string
	}{
		{
			name:           "EmptyPath",
			noFile:         true,
			wantLinks:      []QuickLaunchLink{},
			wantRepos:      nil,
			wantVanadisURL: "",
			wantHaroURL:    "",
		},
		{
			name:           "NonExistentFile",
			nonExistent:    true,
			wantLinks:      []QuickLaunchLink{},
			wantRepos:      nil,
			wantVanadisURL: "",
			wantHaroURL:    "",
		},
		{
			name: "InvalidYAML",
			yamlData: `dashboard: [invalid-yaml
invalid: [yaml: broken
`,
			wantLinks:      []QuickLaunchLink{},
			wantRepos:      nil,
			wantVanadisURL: "",
			wantHaroURL:    "",
		},
		{
			name: "QuickLaunchLinks_DashboardConfig",
			yamlData: `
dashboard:
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
  - name: "INTERNAL"
    url: "http://internal.lan"
    target: "_self"
`,
			wantLinks: []QuickLaunchLink{
				{
					Name:     "INTERNAL",
					URL:      "http://internal.lan",
					Target:   "_self",
					IsCustom: true,
				},
			},
		},
		{
			name: "DashboardRepos_ValidListTrimmingAndDeduplication",
			yamlData: `
dashboard:
  github_repos:
    - " azylman/aerial "
    - "azylman/aerial-sidecars"
    - "azylman/aerial-config"
    - "azylman/aerial"
    - ""
`,
			wantRepos: []string{"azylman/aerial", "azylman/aerial-sidecars", "azylman/aerial-config"},
		},
		{
			name: "DashboardRepos_DashboardReposFallback",
			yamlData: `
dashboard:
  repos:
    - "org/repo-a"
    - "org/repo-b"
`,
			wantRepos: []string{"org/repo-a", "org/repo-b"},
		},
		{
			name: "DashboardRepos_RootFallback",
			yamlData: `
github_repos:
  - "root/repo-1"
  - "root/repo-2"
`,
			wantRepos: []string{"root/repo-1", "root/repo-2"},
		},
		{
			name: "DashboardRepos_EmptyListsReturnNil",
			yamlData: `
dashboard:
  github_repos: []
`,
			wantRepos: nil,
		},
		{
			name: "VanadisURL_DashboardResolutionWithTrimming",
			yamlData: `
dashboard:
  vanadis_url: "  http://vanadis.internal:8088  "
`,
			wantVanadisURL: "http://vanadis.internal:8088",
		},
		{
			name: "VanadisURL_RootFallback",
			yamlData: `
vanadis_url: "http://vanadis-root.internal:8088"
`,
			wantVanadisURL: "http://vanadis-root.internal:8088",
		},
		{
			name: "VanadisURL_DashboardPrecedenceOverRoot",
			yamlData: `
dashboard:
  vanadis_url: "http://vanadis-dash.internal:8088"
vanadis_url: "http://vanadis-root.internal:8088"
`,
			wantVanadisURL: "http://vanadis-dash.internal:8088",
		},
		{
			name: "VanadisURL_EmptyValuesReturnEmpty",
			yamlData: `
dashboard:
  vanadis_url: ""
vanadis_url: ""
`,
			wantVanadisURL: "",
		},
		{
			name: "HaroURL_DashboardResolutionWithTrimming",
			yamlData: `
dashboard:
  haro_url: "  http://haro.internal:8088  "
`,
			wantHaroURL: "http://haro.internal:8088",
		},
		{
			name: "HaroURL_RootFallback",
			yamlData: `
haro_url: "http://haro-root.internal:8088"
`,
			wantHaroURL: "http://haro-root.internal:8088",
		},
		{
			name: "HaroURL_DashboardPrecedenceOverRoot",
			yamlData: `
dashboard:
  haro_url: "http://haro-dash.internal:8088"
haro_url: "http://haro-root.internal:8088"
`,
			wantHaroURL: "http://haro-dash.internal:8088",
		},
		{
			name: "HaroURL_EmptyValuesReturnEmpty",
			yamlData: `
dashboard:
  haro_url: ""
haro_url: ""
`,
			wantHaroURL: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := ""
			if tc.nonExistent {
				path = filepath.Join(t.TempDir(), "nonexistent.yaml")
			} else if !tc.noFile {
				path = filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte(tc.yamlData), 0o644); err != nil {
					t.Fatalf("failed to write test yaml: %v", err)
				}
			}

			if tc.wantLinks != nil || tc.noFile || tc.nonExistent || strings.Contains(tc.yamlData, "invalid") {
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

			if tc.wantRepos != nil || tc.noFile || tc.nonExistent || strings.Contains(tc.yamlData, "invalid") || strings.Contains(tc.name, "EmptyLists") {
				gotRepos := loadDashboardRepos(path)
				if len(gotRepos) != len(tc.wantRepos) {
					t.Fatalf("expected %d repos, got %d: %v vs %v", len(tc.wantRepos), len(gotRepos), tc.wantRepos, gotRepos)
				}
				for i := range gotRepos {
					if gotRepos[i] != tc.wantRepos[i] {
						t.Errorf("repo [%d] = %q, want %q", i, gotRepos[i], tc.wantRepos[i])
					}
				}
			}

			if tc.wantVanadisURL != "" || tc.noFile || tc.nonExistent || strings.Contains(tc.yamlData, "invalid") || strings.Contains(tc.name, "EmptyValues") {
				gotVanadis := loadDashboardVanadisURL(path)
				if gotVanadis != tc.wantVanadisURL {
					t.Errorf("loadDashboardVanadisURL() = %q, want %q", gotVanadis, tc.wantVanadisURL)
				}
			}

			if tc.wantHaroURL != "" || tc.noFile || tc.nonExistent || strings.Contains(tc.yamlData, "invalid") || strings.Contains(tc.name, "EmptyValues") {
				gotHaro := loadDashboardHaroURL(path)
				if gotHaro != tc.wantHaroURL {
					t.Errorf("loadDashboardHaroURL() = %q, want %q", gotHaro, tc.wantHaroURL)
				}
			}
		})
	}
}

func TestNewDashboardConfigFromLookup_TableDriven(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "dashboard.yaml")
	yamlContent := `
port: "8084"
brain_url: "http://brain:8088"
hangar_url: "http://hangar:8087"
github_repo: "azylman/aerial"
github_repos:
  - "azylman/aerial"
  - "azylman/aerial-config"
config_path: "/custom/aerial-config/config.yaml"
vanadis_url: "http://vanadis:8089"
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("failed to write test dashboard.yaml: %v", err)
	}

	tests := []struct {
		name                 string
		env                  map[string]string
		nilLookup            bool
		useEnvSmoke          bool
		testSetupMux         bool
		customYAML           string
		userYAML             string
		wantPort             string
		wantBrainURL         string
		wantMemoryURL        string
		wantGHRepo           string
		wantGHToken          string
		wantGitSyncURL       string
		wantHangarURL        string
		wantConfigPath       string
		wantGitCommit        string
		wantVanadisURL       string
		wantHaroURL          string
		wantNomadAddr        string
		assertEmptyNomadAddr bool
		wantGHRepos          []string
	}{
		{
			name:     "Custom_WithHangarURL",
			userYAML: "dashboard:\n  vanadis_url: http://vanadis-config:9090\n",
			env: map[string]string{
				"PORT": "9090", "BRAIN_URL": "http://brain-custom:9090", "GITHUB_REPO": "custom/repo",
				"GITHUB_PAT": "custom_pat", "GITHUB_PERSONAL_ACCESS_TOKEN": "fallback_token",
				"HANGAR_URL": "http://hangar-custom:9090", "GITSYNC_URL": "http://gitsync-custom:9090",
				"GIT_COMMIT": "0123456789", "VANADIS_URL": "http://vanadis-env-ignored:9090",
			},
			wantPort:       "9090",
			wantBrainURL:   "http://brain-custom:9090",
			wantGHRepo:     "custom/repo",
			wantGHToken:    "custom_pat",
			wantGitSyncURL: "http://hangar-custom:9090",
			wantHangarURL:  "http://hangar-custom:9090",
			wantGitCommit:  "0123456",
			wantVanadisURL: "http://vanadis-config:9090",
		},
		{
			name:           "Custom_FallbackGitSyncURL",
			env:            map[string]string{"GITSYNC_URL": "http://gitsync-custom:9090"},
			wantGitSyncURL: "http://gitsync-custom:9090",
			wantHangarURL:  "http://gitsync-custom:9090",
		},
		{
			name:           "Defaults_NilLookup",
			nilLookup:      true,
			wantPort:       "8080",
			wantBrainURL:   "http://brain:8080",
			wantMemoryURL:  "http://coven.aerial",
			wantGHRepo:     "azylman/aerial",
			wantGHRepos:    []string{"azylman/aerial"},
			wantGHToken:    "",
			wantGitSyncURL: "http://hangar:8080",
			wantHangarURL:  "http://hangar:8080",
			wantConfigPath: "/share/aerial-config/config.yaml",
			wantGitCommit:  "latest",
			wantVanadisURL: "http://vanadis.aerial",
			wantHaroURL:    "http://haro.aerial",
		},
		{
			name:        "Defaults_FallbackToken",
			env:         map[string]string{"GITHUB_PERSONAL_ACCESS_TOKEN": "fallback_token_123"},
			wantGHToken: "fallback_token_123",
		},
		{
			name:           "YAML_LoadFromConfigPath",
			customYAML:     yamlContent,
			wantPort:       "8084",
			wantBrainURL:   "http://brain:8088",
			wantHangarURL:  "http://hangar:8087",
			wantGitSyncURL: "http://hangar:8087",
			wantGHRepos:    []string{"azylman/aerial", "azylman/aerial-config"},
			wantConfigPath: "/custom/aerial-config/config.yaml",
			wantVanadisURL: "http://vanadis:8089",
		},
		{
			name:       "YAML_EnvOverridePrecedence",
			customYAML: yamlContent,
			env:        map[string]string{"PORT": "9999"},
			wantPort:   "9999",
		},
		{
			name:        "MultiRepo_GithubReposList",
			env:         map[string]string{"GITHUB_REPOS": "azylman/aerial, azylman/aerial-sidecars, azylman/aerial-config"},
			wantGHRepos: []string{"azylman/aerial", "azylman/aerial-sidecars", "azylman/aerial-config"},
			wantGHRepo:  "azylman/aerial",
		},
		{
			name:        "MultiRepo_SingleRepoFallback",
			env:         map[string]string{"GITHUB_REPO": "custom/single-repo"},
			wantGHRepos: []string{"custom/single-repo"},
			wantGHRepo:  "custom/single-repo",
		},
		{
			name:          "MemoryURL_FromLookup",
			env:           map[string]string{"MEMORY_MCP_URL": "http://memory-custom:4008"},
			wantMemoryURL: "http://memory-custom:4008",
		},
		{
			name:          "MemoryURL_FromYAML",
			customYAML:    "memory_url: http://memory-yaml:4008\n",
			wantMemoryURL: "http://memory-yaml:4008",
		},
		{
			name:        "HaroURL_FromLookup",
			env:         map[string]string{"HARO_URL": "http://haro-custom:8080"},
			wantHaroURL: "http://haro-custom:8080",
		},
		{
			name:        "HaroURL_FromSCHEDULER_MCP_URL_TrimMCP",
			env:         map[string]string{"SCHEDULER_MCP_URL": "http://scheduler.aerial/mcp"},
			wantHaroURL: "http://scheduler.aerial",
		},
		{
			name:        "HaroURL_FromSCHEDULER_MCP_URL_TrimSSE",
			env:         map[string]string{"SCHEDULER_MCP_URL": "http://scheduler.aerial:8080/sse/"},
			wantHaroURL: "http://scheduler.aerial:8080",
		},
		{
			name: "HaroURL_HARO_URL_PrecedenceOverSCHEDULER_MCP_URL",
			env: map[string]string{
				"HARO_URL":          "http://haro-direct:8080",
				"SCHEDULER_MCP_URL": "http://scheduler.aerial/mcp",
			},
			wantHaroURL: "http://haro-direct:8080",
		},
		{
			name:        "HaroURL_FromYAML",
			customYAML:  "haro_url: http://haro-yaml:8080\n",
			wantHaroURL: "http://haro-yaml:8080",
		},
		{
			name:        "HaroURL_FromUserConfigYAML",
			userYAML:    "dashboard:\n  haro_url: http://haro-user:8080\n",
			wantHaroURL: "http://haro-user:8080",
		},
		{
			name:          "NomadAddr_FromLookup",
			env:           map[string]string{"NOMAD_ADDR": "http://10.1.2.3:4646"},
			wantNomadAddr: "http://10.1.2.3:4646",
		},
		{
			name:          "NomadAddr_FromYAML",
			customYAML:    "nomad_addr: http://10.9.8.7:4646\n",
			wantNomadAddr: "http://10.9.8.7:4646",
		},
		{
			name:                 "NomadAddr_DefaultsEmptyUnderTests",
			env:                  map[string]string{},
			assertEmptyNomadAddr: true,
		},
		{
			name:        "NewDashboardConfigFromEnv_Smoke",
			useEnvSmoke: true,
		},
		{
			name:          "SetupDashboardMux_PassesNomadAddr",
			testSetupMux:  true,
			wantPort:      "8080",
			wantNomadAddr: "http://127.0.0.1:4646",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.useEnvSmoke {
				cfg := NewDashboardConfigFromEnv()
				if cfg.Port == "" || cfg.BrainURL == "" || cfg.GHRepo == "" || cfg.GitSyncURL == "" || cfg.ConfigPath == "" || cfg.GitCommit == "" || cfg.VanadisURL == "" || cfg.HaroURL == "" {
					t.Errorf("expected non-empty fields in NewDashboardConfigFromEnv, got %+v", cfg)
				}
				return
			}
			if tc.testSetupMux {
				cfg := DashboardConfig{
					Port:      tc.wantPort,
					NomadAddr: tc.wantNomadAddr,
				}
				handler := SetupDashboardMux(cfg, nil)
				if handler == nil {
					t.Fatal("expected non-nil mux handler")
				}
				return
			}

			var envFn func(string) string
			if !tc.nilLookup {
				envMap := make(map[string]string)
				for k, v := range tc.env {
					envMap[k] = v
				}
				if tc.userYAML != "" {
					userPath := filepath.Join(t.TempDir(), "user-config.yaml")
					if err := os.WriteFile(userPath, []byte(tc.userYAML), 0o644); err != nil {
						t.Fatal(err)
					}
					envMap["AERIAL_CONFIG_PATH"] = userPath
				}
				if tc.customYAML != "" {
					customPath := filepath.Join(t.TempDir(), "dashboard.yaml")
					if err := os.WriteFile(customPath, []byte(tc.customYAML), 0o644); err != nil {
						t.Fatal(err)
					}
					envMap["CONFIG_PATH"] = customPath
				}
				envFn = func(k string) string {
					return envMap[k]
				}
			}

			cfg := NewDashboardConfigFromLookup(envFn)
			if tc.wantPort != "" && cfg.Port != tc.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if tc.wantBrainURL != "" && cfg.BrainURL != tc.wantBrainURL {
				t.Errorf("BrainURL = %q, want %q", cfg.BrainURL, tc.wantBrainURL)
			}
			if tc.wantMemoryURL != "" && cfg.MemoryURL != tc.wantMemoryURL {
				t.Errorf("MemoryURL = %q, want %q", cfg.MemoryURL, tc.wantMemoryURL)
			}
			if tc.wantGHRepo != "" && cfg.GHRepo != tc.wantGHRepo {
				t.Errorf("GHRepo = %q, want %q", cfg.GHRepo, tc.wantGHRepo)
			}
			if tc.wantGHToken != "" && cfg.GHToken != tc.wantGHToken {
				t.Errorf("GHToken = %q, want %q", cfg.GHToken, tc.wantGHToken)
			}
			if tc.wantGitSyncURL != "" && cfg.GitSyncURL != tc.wantGitSyncURL {
				t.Errorf("GitSyncURL = %q, want %q", cfg.GitSyncURL, tc.wantGitSyncURL)
			}
			if tc.wantHangarURL != "" && cfg.HangarURL != tc.wantHangarURL {
				t.Errorf("HangarURL = %q, want %q", cfg.HangarURL, tc.wantHangarURL)
			}
			if tc.wantConfigPath != "" && cfg.ConfigPath != tc.wantConfigPath {
				t.Errorf("ConfigPath = %q, want %q", cfg.ConfigPath, tc.wantConfigPath)
			}
			if tc.wantGitCommit != "" && cfg.GitCommit != tc.wantGitCommit {
				t.Errorf("GitCommit = %q, want %q", cfg.GitCommit, tc.wantGitCommit)
			}
			if tc.wantVanadisURL != "" && cfg.VanadisURL != tc.wantVanadisURL {
				t.Errorf("VanadisURL = %q, want %q", cfg.VanadisURL, tc.wantVanadisURL)
			}
			if tc.wantHaroURL != "" && cfg.HaroURL != tc.wantHaroURL {
				t.Errorf("HaroURL = %q, want %q", cfg.HaroURL, tc.wantHaroURL)
			}
			if tc.wantNomadAddr != "" && cfg.NomadAddr != tc.wantNomadAddr {
				t.Errorf("NomadAddr = %q, want %q", cfg.NomadAddr, tc.wantNomadAddr)
			}
			if tc.assertEmptyNomadAddr && cfg.NomadAddr != "" {
				t.Errorf("expected empty NomadAddr under test, got %q", cfg.NomadAddr)
			}
			if len(tc.wantGHRepos) > 0 {
				if !reflect.DeepEqual(cfg.GHRepos, tc.wantGHRepos) {
					t.Errorf("GHRepos = %v, want %v", cfg.GHRepos, tc.wantGHRepos)
				}
			}
		})
	}
}
