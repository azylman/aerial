package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_RestateInvoker(t *testing.T) {
	tests := []struct {
		name              string
		yaml              string
		env               map[string]string
		wantURL           string
		wantPRService     string
		wantRepoService   string
		wantAuthoritative bool
	}{
		{name: "restate endpoint and services load from yaml", yaml: "restate_url: \"http://restate.aerial:8080/\"\nrestate_pr_service: \"PRObject\"\nrestate_repo_service: \"RepoObject\"\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject", wantRepoService: "RepoObject"},
		{name: "custom service names preserved", yaml: "restate_url: \"http://restate.aerial:8080\"\nrestate_pr_service: \"PRObject-green\"\nrestate_repo_service: \"RepoObject-green\"\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject-green", wantRepoService: "RepoObject-green"},
		{name: "absent keys stay dark with frozen defaults", yaml: "port: \"4020\"\n", wantURL: "", wantPRService: "PRObject", wantRepoService: "RepoObject"},
		{name: "blank service names fall back to frozen defaults", yaml: "restate_url: \"http://restate.aerial:8080\"\nrestate_pr_service: \"   \"\nrestate_repo_service: \"\"\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject", wantRepoService: "RepoObject"},
		{name: "no env var injects restate endpoint", yaml: "port: \"4020\"\n", env: map[string]string{"RESTATE_URL": "http://evil:8080"}, wantURL: "", wantPRService: "PRObject", wantRepoService: "RepoObject"},
		{name: "authoritative cutover defaults off", yaml: "restate_url: \"http://restate.aerial:8080\"\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject", wantRepoService: "RepoObject"},
		{name: "authoritative cutover flips from yaml", yaml: "restate_url: \"http://restate.aerial:8080\"\nrestate_authoritative: true\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject", wantRepoService: "RepoObject", wantAuthoritative: true},
		{name: "scoped services for suletta bot_id", yaml: "restate_url: \"http://restate.aerial:8080\"\nbot_id: \"suletta\"\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject-suletta", wantRepoService: "RepoObject-suletta"},
		{name: "scoped services for miorine bot_id", yaml: "restate_url: \"http://restate.aerial:8080\"\nbot_id: \"miorine\"\n", wantURL: "http://restate.aerial:8080", wantPRService: "PRObject-miorine", wantRepoService: "RepoObject-miorine"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "synapse.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o644); err != nil {
				t.Fatalf("failed to write yaml: %v", err)
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			t.Setenv("CONFIG_PATH", "")
			t.Setenv("AERIAL_BOT_ID", "")
			cfg := LoadConfig(path)
			if cfg.RestateURL != tc.wantURL {
				t.Errorf("RestateURL = %q, want %q", cfg.RestateURL, tc.wantURL)
			}
			if cfg.RestatePRService != tc.wantPRService {
				t.Errorf("RestatePRService = %q, want %q", cfg.RestatePRService, tc.wantPRService)
			}
			if cfg.RestateRepoService != tc.wantRepoService {
				t.Errorf("RestateRepoService = %q, want %q", cfg.RestateRepoService, tc.wantRepoService)
			}
			if cfg.RestateAuthoritative != tc.wantAuthoritative {
				t.Errorf("RestateAuthoritative = %v, want %v", cfg.RestateAuthoritative, tc.wantAuthoritative)
			}
		})
	}
}

func TestRestartRequired_Restate(t *testing.T) {
	t.Parallel()
	base := Config{Port: "4020", RestateURL: "http://restate.aerial:8080", RestatePRService: "PRObject", RestateRepoService: "RepoObject"}
	tests := []struct {
		name string
		next Config
		want bool
	}{
		{name: "identical restate config", next: base, want: false},
		{name: "restate url requires restart", next: withField(base, Config{RestateURL: "http://restate2.aerial:8080"}), want: true},
		{name: "restate pr service requires restart", next: withField(base, Config{RestatePRService: "PRObject-green"}), want: true},
		{name: "restate repo service requires restart", next: withField(base, Config{RestateRepoService: "RepoObject-green"}), want: true},
		{name: "authoritative flip needs no restart for yaml rebind rollback", next: withField(base, Config{RestateAuthoritative: true}), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			old := base
			next := tc.next
			if got := RestartRequired(&old, &next); got != tc.want {
				t.Errorf("RestartRequired() = %v, want %v", got, tc.want)
			}
		})
	}
}
