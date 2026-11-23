package config

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// configTestPath writes yaml (when non-empty) to name inside a temp dir and returns the
// path to pass the loader; an empty name stays "" so the empty-path guard is exercised.
func configTestPath(t *testing.T, name, yaml string) string {
	t.Helper()
	if name == "" {
		return ""
	}
	path := filepath.Join(t.TempDir(), name)
	if yaml != "" {
		if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func TestLoadQuickLaunchLinks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string
		yaml string
		want []QuickLaunchLink
	}{
		{name: "empty path", want: []QuickLaunchLink{}},
		{name: "missing file", file: "missing.yaml", want: []QuickLaunchLink{}},
		{name: "invalid yaml", file: "c.yaml", yaml: "console: [invalid-yaml\ninvalid: [yaml: broken\n", want: []QuickLaunchLink{}},
		{name: "repos only", file: "c.yaml", yaml: "github_repos: [\"example/alpha\"]\n", want: []QuickLaunchLink{}},
		{
			name: "console quick_launch_links skip nameless and urlless",
			file: "c.yaml",
			yaml: "console:\n  quick_launch_links:\n    - {name: HOME, url: \"https://home.example.com\", icon: \"🏠\", description: Hub}\n" +
				"    - {name: \"\", url: \"https://skip.example.com\"}\n    - {name: NO_URL, url: \"\"}\n",
			want: []QuickLaunchLink{{Name: "HOME", URL: "https://home.example.com", Icon: "🏠", Description: "Hub", Target: "_blank", IsCustom: true}},
		},
		{
			name: "root quick_links keeps target",
			file: "c.yaml",
			yaml: "quick_links:\n  - {name: ROOT, url: \"https://root.example.com\", target: _self}\n",
			want: []QuickLaunchLink{{Name: "ROOT", URL: "https://root.example.com", Target: "_self", IsCustom: true}},
		},
		{
			name: "root quick_launch_links",
			file: "c.yaml",
			yaml: "quick_launch_links:\n  - {name: GRAFANA, url: \"https://grafana.example.com\", icon: \"📊\"}\n",
			want: []QuickLaunchLink{{Name: "GRAFANA", URL: "https://grafana.example.com", Icon: "📊", Target: "_blank", IsCustom: true}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := LoadQuickLaunchLinks(configTestPath(t, tc.file, tc.yaml)); !slices.Equal(got, tc.want) {
				t.Errorf("LoadQuickLaunchLinks() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLoadConsoleRepos(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string
		yaml string
		want []string
	}{
		{name: "empty path"},
		{name: "missing file", file: "missing.yaml"},
		{name: "invalid yaml", file: "c.yaml", yaml: "console: [invalid-yaml\n"},
		{
			name: "console github_repos trimmed and deduped",
			file: "c.yaml",
			yaml: "console:\n  github_repos: [\"example/alpha\", \" example/alpha \", \"\", \"example/beta\"]\n",
			want: []string{"example/alpha", "example/beta"},
		},
		{name: "console repos fallback", file: "c.yaml", yaml: "console:\n  repos: [\"example/gamma\"]\n", want: []string{"example/gamma"}},
		{name: "root github_repos fallback", file: "c.yaml", yaml: "github_repos: [\"example/root\"]\n", want: []string{"example/root"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := LoadConsoleRepos(configTestPath(t, tc.file, tc.yaml)); !slices.Equal(got, tc.want) {
				t.Errorf("LoadConsoleRepos() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	validLocal := filepath.Join(tmpDir, "local_valid.yaml")
	if err := os.WriteFile(validLocal, []byte("port: '8084'\n"), 0o644); err != nil {
		t.Fatalf("write validLocal: %v", err)
	}
	emptyLocal := filepath.Join(tmpDir, "local_empty.yaml")
	if err := os.WriteFile(emptyLocal, []byte(""), 0o644); err != nil {
		t.Fatalf("write emptyLocal: %v", err)
	}
	nonExistent := filepath.Join(tmpDir, "nonexistent.yaml")
	fallbackFile := filepath.Join(tmpDir, "fallback.yaml")
	if err := os.WriteFile(fallbackFile, []byte("port: '8084'\n"), 0o644); err != nil {
		t.Fatalf("write fallbackFile: %v", err)
	}

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
		{"fallback empty falls back to the in-container default", "", nonExistent, "", "/config/console.yaml"},
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

// consoleYAMLServices is the required service block every valid console.yaml row shares.
const consoleYAMLServices = "port: \"8084\"\nmemory_url: \"http://coven.example\"\nharo_url: \"http://haro.example\"\nvanadis_url: \"http://vanadis.example\"\n"

// loadConfigEnvKeys are the env vars LoadConfig and a nil-lookup LoadConfigFile read from the
// process env; every row pins them so the pod's own values never leak into a result.
var loadConfigEnvKeys = []string{"ERI_URL", "GITHUB_PAT", "KUBE_API_SERVER", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"}

// consoleConfigWant is the expected config for a consoleYAMLServices file; ConfigPath ""
// means the loaded file's own path.
func consoleConfigWant(eri, kube string, repos ...string) ConsoleConfig {
	return ConsoleConfig{
		Port: "8084", BrainURL: eri, EriURL: eri, MemoryURL: "http://coven.example", HaroURL: "http://haro.example",
		VanadisURL: "http://vanadis.example", GHRepo: repos[0], GHRepos: repos, KubeAPIServer: kube,
	}
}

func TestLoadConfigFile(t *testing.T) {
	eriAlpha := "eri_url: \"http://eri.example\"\ngithub_repos: [\"example/alpha\"]\n"
	tests := []struct {
		name    string
		file    string
		yaml    string
		env     map[string]string // nil passes a nil lookup, which reads the (pinned-empty) process env
		want    ConsoleConfig
		wantErr string
	}{
		{
			name: "full configuration with token",
			file: "c.yaml",
			yaml: consoleYAMLServices + "eri_url: \"http://eri.example\"\ndocket_url: \" http://docket.example \"\ngithub_repos: [\"example/alpha\", \"example/beta\", \"example/alpha\"]\n",
			env:  map[string]string{"GITHUB_PAT": "pat-12345", "GIT_COMMIT": "abcdef012345"},
			want: ConsoleConfig{
				Port: "8084", BrainURL: "http://eri.example", EriURL: "http://eri.example", MemoryURL: "http://coven.example",
				HaroURL: "http://haro.example", VanadisURL: "http://vanadis.example", DocketURL: "http://docket.example",
				GHRepo: "example/alpha", GHRepos: []string{"example/alpha", "example/beta"}, GHToken: "pat-12345",
			},
		},
		{
			name: "single repo and custom config_path",
			file: "c.yaml",
			yaml: consoleYAMLServices + eriAlpha + "config_path: \"/custom/path/config.yaml\"\n",
			env:  map[string]string{"GITHUB_PAT": "token-from-env"},
			want: ConsoleConfig{
				Port: "8084", BrainURL: "http://eri.example", EriURL: "http://eri.example", MemoryURL: "http://coven.example",
				HaroURL: "http://haro.example", VanadisURL: "http://vanadis.example", GHRepo: "example/alpha", GHRepos: []string{"example/alpha"},
				GHToken: "token-from-env", ConfigPath: "/custom/path/config.yaml",
			},
		},
		{
			name: "nil lookup reads process env",
			file: "c.yaml",
			yaml: consoleYAMLServices + eriAlpha,
			want: consoleConfigWant("http://eri.example", "", "example/alpha"),
		},
		{
			name: "ERI_URL env fallback when absent from yaml",
			file: "c.yaml",
			yaml: consoleYAMLServices + "github_repos: [\"example/alpha\"]\n",
			env:  map[string]string{"ERI_URL": "http://env-eri.example:8088"},
			want: consoleConfigWant("http://env-eri.example:8088", "", "example/alpha"),
		},
		{
			name: "kube_api_server in yaml",
			file: "c.yaml",
			yaml: consoleYAMLServices + eriAlpha + "kube_api_server: \"http://kube.example:6443\"\n",
			env:  map[string]string{},
			want: consoleConfigWant("http://eri.example", "http://kube.example:6443", "example/alpha"),
		},
		{
			name: "KUBE_API_SERVER env fallback",
			file: "c.yaml",
			yaml: consoleYAMLServices + eriAlpha,
			env:  map[string]string{"KUBE_API_SERVER": "https://k8s-env.example:6443"},
			want: consoleConfigWant("http://eri.example", "https://k8s-env.example:6443", "example/alpha"),
		},
		{
			name: "KUBERNETES_SERVICE_HOST and PORT env fallback",
			file: "c.yaml",
			yaml: consoleYAMLServices + eriAlpha,
			env:  map[string]string{"KUBERNETES_SERVICE_HOST": "192.0.2.1", "KUBERNETES_SERVICE_PORT": "6443"},
			want: consoleConfigWant("http://eri.example", "https://192.0.2.1:6443", "example/alpha"),
		},
		{
			name: "KUBERNETES_SERVICE_HOST default port 443",
			file: "c.yaml",
			yaml: consoleYAMLServices + eriAlpha,
			env:  map[string]string{"KUBERNETES_SERVICE_HOST": "192.0.2.1"},
			want: consoleConfigWant("http://eri.example", "https://192.0.2.1:443", "example/alpha"),
		},
		{name: "empty path", wantErr: "config: path cannot be empty"},
		{name: "missing file", file: "missing.yaml", wantErr: "config: failed to read"},
		{name: "bad yaml syntax", file: "c.yaml", yaml: "port: [unclosed", wantErr: "config: failed to parse YAML"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range loadConfigEnvKeys {
				t.Setenv(k, "")
			}
			path := configTestPath(t, tc.file, tc.yaml)
			var lookup func(string) string
			if tc.env != nil {
				lookup = func(k string) string { return tc.env[k] }
			}
			got, err := LoadConfigFile(path, lookup)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadConfigFile() err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfigFile() err = %v", err)
			}
			want := tc.want
			if want.ConfigPath == "" {
				want.ConfigPath = path
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("LoadConfigFile() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		env     map[string]string
		want    ConsoleConfig
		wantErr string
	}{
		{
			name: "reads token from process env",
			file: "c.yaml",
			env:  map[string]string{"GITHUB_PAT": "pat-env"},
			want: ConsoleConfig{
				Port: "8084", BrainURL: "http://eri.example", EriURL: "http://eri.example", MemoryURL: "http://coven.example",
				HaroURL: "http://haro.example", VanadisURL: "http://vanadis.example", GHRepo: "example/alpha", GHRepos: []string{"example/alpha"}, GHToken: "pat-env",
			},
		},
		{name: "empty path", wantErr: "config: path cannot be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range loadConfigEnvKeys {
				t.Setenv(k, tc.env[k])
			}
			path := configTestPath(t, tc.file, consoleYAMLServices+"eri_url: \"http://eri.example\"\ngithub_repos: [\"example/alpha\"]\n")
			got, err := LoadConfig(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadConfig() err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() err = %v", err)
			}
			tc.want.ConfigPath = path
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("LoadConfig() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
