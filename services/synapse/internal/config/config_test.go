package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var synapseConfigKeysToClear = []string{
	"PORT", "KUBE_API_SERVER",
	"KUBE_TOKEN", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT",
	"GITHUB_PAT", "GITHUB_API_URL", "GITHUB_WEBHOOK_SECRET",
	"DATABASE_URL", "POSTGRES_URL", "POSTGRES_USER", "POSTGRES_PASSWORD",
	"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
	"DISCORD_TOKEN", "BOT_ID", "AERIAL_BOT_ID", "BRAIN_URL", "ERI_URL",
	"FLUX_RECEIVER_URL", "HARO_URL", "HANGAR_URL", "NATS_URL",
	"SYSTEM_CHANNEL_ID", "AUTO_UPDATE_PR_AUTHORS",
	"WEBHOOK_FORWARD_URLS", "SYNAPSE_DEFAULT_OWNER", "DEFAULT_OWNER", "AERIAL_DEFAULT_OWNER",
}

func TestLoadConfig_EnvAndFallbacks(t *testing.T) {
	tests := []struct {
		name                string
		env                 map[string]string
		wantPort            string
		wantKubeServer      string
		wantKubeTok         string
		wantGHTok           string
		wantGHURL           string
		wantGHWebhookSecret string
		wantPGURL           string
		wantFluxURL         string
		wantBrainURL        string
		wantEriURL          string
		wantSysChan         string
		wantDiscTok         string
		wantNATSURL         string
		wantEmptyURL        bool
	}{
		{
			name:      "primary port env",
			env:       map[string]string{"PORT": "9999"},
			wantPort:  "9999",
			wantGHURL: "https://api.github.com",
			wantPGURL: "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
		},
		{
			name:         "default fallback loading with empty envs",
			env:          map[string]string{},
			wantPort:     "4020",
			wantGHURL:    "https://api.github.com",
			wantPGURL:    "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
			wantEmptyURL: true,
		},
		{
			name:           "primary kube envs",
			env:            map[string]string{"KUBE_API_SERVER": "https://kube.aerial:6443/", "KUBE_TOKEN": "secret-token"},
			wantKubeServer: "https://kube.aerial:6443",
			wantKubeTok:    "secret-token",
		},
		{
			name:           "fallback kube host and port",
			env:            map[string]string{"KUBERNETES_SERVICE_HOST": "10.43.0.1", "KUBERNETES_SERVICE_PORT": "443"},
			wantKubeServer: "https://10.43.0.1:443",
		},
		{
			name:      "github and postgres primary envs",
			env:       map[string]string{"GITHUB_PAT": "pat123", "GITHUB_API_URL": "https://api.github.test/", "DATABASE_URL": "postgres://custom:pass@db:5432/customdb"},
			wantGHTok: "pat123",
			wantGHURL: "https://api.github.test",
			wantPGURL: "postgres://custom:pass@db:5432/customdb",
		},
		{
			name:                "github webhook secret primary env",
			env:                 map[string]string{"GITHUB_WEBHOOK_SECRET": "secret-webhook-key"},
			wantGHWebhookSecret: "secret-webhook-key",
		},
		{
			name: "decomposed postgres envs",
			env: map[string]string{
				"POSTGRES_HOST": "custom-pg.aerial", "POSTGRES_PORT": "5432", "POSTGRES_USER": "aerial", "POSTGRES_PASSWORD": "custom_pass", "POSTGRES_DB": "aerial",
			},
			wantPGURL: "postgres://aerial:custom_pass@custom-pg.aerial:5432/aerial?sslmode=disable",
		},
		{
			name:        "system channel id primary env",
			env:         map[string]string{"SYSTEM_CHANNEL_ID": "123456789012345678"},
			wantSysChan: "123456789012345678",
		},
		{
			name:        "discord token primary env",
			env:         map[string]string{"DISCORD_TOKEN": "bot-secret-token-123"},
			wantDiscTok: "bot-secret-token-123",
		},
		{
			name:         "eri url primary env",
			env:          map[string]string{"ERI_URL": "http://eri-primary.aerial:8088"},
			wantBrainURL: "http://eri-primary.aerial:8088",
			wantEriURL:   "http://eri-primary.aerial:8088",
		},
		{
			name:        "flux receiver url primary env",
			env:         map[string]string{"FLUX_RECEIVER_URL": "http://flux-receiver.aerial/webhook/"},
			wantFluxURL: "http://flux-receiver.aerial/webhook",
		},
		{
			name:        "nats url primary env",
			env:         map[string]string{"NATS_URL": "nats://custom-nats:4222"},
			wantNATSURL: "nats://custom-nats:4222",
		},
	}

	keysToClear := []string{
		"PORT", "KUBE_API_SERVER",
		"KUBE_TOKEN", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT",
		"GITHUB_PAT", "GITHUB_API_URL", "GITHUB_WEBHOOK_SECRET",
		"DATABASE_URL", "POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT",
		"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
		"HANGAR_URL", "FLUX_RECEIVER_URL", "ERI_URL", "HARO_URL", "NATS_URL",
		"SYSTEM_CHANNEL_ID", "CONFIG_PATH", "DISCORD_TOKEN",
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range keysToClear {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg := LoadConfigFromEnv()
			for _, check := range []struct{ got, want, field string }{
				{cfg.Port, tc.wantPort, "Port"},
				{cfg.KubeAPIServer, tc.wantKubeServer, "KubeAPIServer"},
				{cfg.KubeToken, tc.wantKubeTok, "KubeToken"},
				{cfg.GitHubToken, tc.wantGHTok, "GitHubToken"},
				{cfg.GitHubAPIURL, tc.wantGHURL, "GitHubAPIURL"},
				{cfg.GitHubWebhookSecret, tc.wantGHWebhookSecret, "GitHubWebhookSecret"},
				{cfg.PostgresURL, tc.wantPGURL, "PostgresURL"},
				{cfg.FluxReceiverURL, tc.wantFluxURL, "FluxReceiverURL"},
				{cfg.BrainURL, tc.wantBrainURL, "BrainURL"},
				{cfg.EriURL, tc.wantEriURL, "EriURL"},
				{cfg.SystemChannelID, tc.wantSysChan, "SystemChannelID"},
				{cfg.DiscordToken, tc.wantDiscTok, "DiscordToken"},
				{cfg.NATSURL, tc.wantNATSURL, "NATSURL"},
			} {
				if check.want != "" && check.got != check.want {
					t.Errorf("%s = %q, want %q", check.field, check.got, check.want)
				}
			}
		})
	}
}

type loadConfigCase struct {
	name                   string
	localYAML              string
	fallbackYAML           string
	aerialYAML             string
	useEmptyPath           bool
	missingFallback        bool
	env                    map[string]string
	wantPort               string
	wantFluxURL            string
	wantBrainURL           string
	wantSysChan            string
	wantAuthors            []string
	wantBotID              string
	wantDefaultOwner       bool
	wantForwardURLs        map[string]string
	wantCollaboratorAlerts []CollaboratorCIAlertRule
	wantStrictErr          bool
}

func setupSynapseTestConfigFiles(t *testing.T, tc loadConfigCase) string {
	t.Helper()
	tmpDir := t.TempDir()
	var targetPath string
	if !tc.useEmptyPath {
		targetPath = filepath.Join(tmpDir, "local.yaml")
		if err := os.WriteFile(targetPath, []byte(tc.localYAML), 0o644); err != nil {
			t.Fatalf("failed to write local config: %v", err)
		}
	}

	oldAerial := SynapseAerialConfigPath
	if tc.aerialYAML != "" {
		aerialPath := filepath.Join(tmpDir, "config.yaml")
		if err := os.WriteFile(aerialPath, []byte(tc.aerialYAML), 0o644); err != nil {
			t.Fatalf("failed to write aerial config: %v", err)
		}
		SynapseAerialConfigPath = aerialPath
	} else {
		SynapseAerialConfigPath = filepath.Join(tmpDir, "nonexistent-config.yaml")
	}
	t.Cleanup(func() { SynapseAerialConfigPath = oldAerial })

	oldFallback := SynapseFallbackConfigPath
	switch {
	case tc.missingFallback:
		SynapseFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
	case tc.fallbackYAML != "":
		fbPath := filepath.Join(tmpDir, "fallback.yaml")
		if err := os.WriteFile(fbPath, []byte(tc.fallbackYAML), 0o644); err != nil {
			t.Fatalf("failed to write fallback config: %v", err)
		}
		SynapseFallbackConfigPath = fbPath
	default:
		SynapseFallbackConfigPath = filepath.Join(tmpDir, "nonexistent-synapse.yaml")
	}
	t.Cleanup(func() {
		SynapseFallbackConfigPath = oldFallback
	})

	for _, k := range synapseConfigKeysToClear {
		t.Setenv(k, "")
	}
	for k, v := range tc.env {
		t.Setenv(k, v)
	}
	return targetPath
}

func assertLoadedConfigFields(t *testing.T, tc loadConfigCase, cfg Config) {
	t.Helper()
	for _, check := range []struct{ got, want, field string }{
		{cfg.Port, tc.wantPort, "Port"},
		{cfg.FluxReceiverURL, tc.wantFluxURL, "FluxReceiverURL"},
		{cfg.BrainURL, tc.wantBrainURL, "BrainURL"},
		{cfg.EriURL, tc.wantBrainURL, "EriURL"},
		{cfg.SystemChannelID, tc.wantSysChan, "SystemChannelID"},
		{cfg.BotID, tc.wantBotID, "BotID"},
	} {
		if check.want != "" && check.got != check.want {
			t.Errorf("%s = %q, want %q", check.field, check.got, check.want)
		}
	}
	if tc.wantDefaultOwner != cfg.DefaultOwner && (tc.wantDefaultOwner || tc.localYAML != "") {
		t.Errorf("DefaultOwner = %v, want %v", cfg.DefaultOwner, tc.wantDefaultOwner)
	}
	if tc.wantForwardURLs != nil {
		if len(cfg.WebhookForwardURLs) != len(tc.wantForwardURLs) {
			t.Errorf("WebhookForwardURLs len = %d, want %d", len(cfg.WebhookForwardURLs), len(tc.wantForwardURLs))
		}
		for k, v := range tc.wantForwardURLs {
			if cfg.WebhookForwardURLs[k] != v {
				t.Errorf("WebhookForwardURLs[%q] = %q, want %q", k, cfg.WebhookForwardURLs[k], v)
			}
		}
	}
	if tc.wantAuthors != nil {
		if len(cfg.AutoUpdatePRAuthors) != len(tc.wantAuthors) {
			t.Errorf("AutoUpdatePRAuthors len = %d, want %d", len(cfg.AutoUpdatePRAuthors), len(tc.wantAuthors))
		} else {
			for i, a := range tc.wantAuthors {
				if cfg.AutoUpdatePRAuthors[i] != a {
					t.Errorf("AutoUpdatePRAuthors[%d] = %q, want %q", i, cfg.AutoUpdatePRAuthors[i], a)
				}
			}
		}
	}
	if tc.wantCollaboratorAlerts != nil {
		if !reflect.DeepEqual(cfg.CollaboratorCIAlerts, tc.wantCollaboratorAlerts) {
			t.Errorf("CollaboratorCIAlerts = %+v, want %+v", cfg.CollaboratorCIAlerts, tc.wantCollaboratorAlerts)
		}
	}
}

func runLoadConfigCase(t *testing.T, tc loadConfigCase) {
	t.Helper()
	targetPath := setupSynapseTestConfigFiles(t, tc)
	cfg := LoadConfig(targetPath)
	strictCfg, strictErr := LoadConfigStrict(targetPath)
	if (strictErr != nil) != tc.wantStrictErr {
		t.Errorf("LoadConfigStrict() err = %v, wantStrictErr %v", strictErr, tc.wantStrictErr)
	}
	if strictErr == nil && !reflect.DeepEqual(strictCfg, cfg) {
		t.Errorf("LoadConfigStrict() = %+v, want LoadConfig result %+v", strictCfg, cfg)
	}
	assertLoadedConfigFields(t, tc, cfg)
}

func loadConfigYAMLTestCases() []loadConfigCase {
	return append(loadConfigYAMLBasicCases(), loadConfigYAMLAdvancedCases()...)
}

func loadConfigYAMLBasicCases() []loadConfigCase {
	return []loadConfigCase{
		{
			name: "yaml config without overrides",
			localYAML: `port: "4099"
eri_url: "http://brain.custom:8088"
`,
			wantPort:     "4099",
			wantBrainURL: "http://brain.custom:8088",
		},
		{
			name: "yaml config with flux_receiver_url",
			localYAML: `port: "4099"
flux_receiver_url: "http://flux-receiver.custom:8080/hook/"
`,
			wantPort:    "4099",
			wantFluxURL: "http://flux-receiver.custom:8080/hook",
		},
		{
			name: "yaml config with eri_url",
			localYAML: `port: "4099"
eri_url: "http://eri.custom:8088"
`,
			wantPort:     "4099",
			wantBrainURL: "http://eri.custom:8088",
		},
		{
			name: "env overrides yaml values",
			localYAML: `port: "4099"
eri_url: "http://brain.custom:8088"
`,
			env: map[string]string{
				"PORT": "5000",
			},
			wantPort:     "5000",
			wantBrainURL: "http://brain.custom:8088",
		},
		{
			name:         "zero-byte local config with existing fallback",
			localYAML:    "",
			fallbackYAML: "port: \"4088\"\n",
			wantPort:     "4088",
		},
		{
			name:            "zero-byte local config with missing fallback uses defaults",
			localYAML:       "",
			missingFallback: true,
			wantPort:        "4020",
		},
		{
			name:         "empty config path with valid fallback",
			useEmptyPath: true,
			fallbackYAML: "port: \"4088\"\n",
			wantPort:     "4088",
		},
		{
			name:         "non-empty local config takes precedence over fallback",
			localYAML:    "port: \"4077\"\n",
			fallbackYAML: "port: \"4088\"\n",
			wantPort:     "4077",
		},
		{
			name:        "system channel id loaded from yaml",
			localYAML:   "system_channel_id: \"112233445566778899\"\n",
			wantSysChan: "112233445566778899",
		},
		{
			name:        "system_channel loaded from local yaml",
			localYAML:   "system_channel: \"aerial-dev\"\n",
			wantSysChan: "aerial-dev",
		},
		{
			name:        "system_channel loaded from aerial config.yaml fallback",
			localYAML:   "port: \"4020\"\n",
			aerialYAML:  "system_channel: \"aerial-dev\"\n",
			wantSysChan: "aerial-dev",
		},
		{
			name:        "system_channel_id loaded from aerial config.yaml fallback",
			localYAML:   "port: \"4020\"\n",
			aerialYAML:  "system_channel_id: \"998877665544332211\"\n",
			wantSysChan: "998877665544332211",
		},
	}
}

func loadConfigYAMLAdvancedCases() []loadConfigCase {
	return []loadConfigCase{
		{
			name:        "auto_update_pr_authors loaded from yaml",
			localYAML:   "auto_update_pr_authors:\n  - \"trusted-author\"\n  - \"peer-author\"\n",
			wantAuthors: []string{"trusted-author", "peer-author"},
		},
		{
			name:        "auto_update_pr_authors loaded from aerial config.yaml fallback",
			localYAML:   "port: \"4020\"\n",
			aerialYAML:  "auto_update_pr_authors:\n  - \"fallback-author\"\n",
			wantAuthors: []string{"fallback-author"},
		},
		{
			name:          "invalid yaml logs warning and uses defaults, strict load errors",
			localYAML:     "invalid: [yaml",
			wantPort:      "4020",
			wantStrictErr: true,
		},
		{
			name:          "type-mismatched yaml keeps decoded fields, strict load errors",
			localYAML:     "port: \"4099\"\nwebhook_forward_urls: [\"not-a-map\"]\n",
			wantPort:      "4099",
			wantStrictErr: true,
		},
		{
			name:          "invalid fallback yaml errors in strict load",
			useEmptyPath:  true,
			fallbackYAML:  "port: [4088\n",
			wantPort:      "4020",
			wantStrictErr: true,
		},
		{
			name:             "bot_id and default_owner loaded from local yaml",
			localYAML:        "bot_id: \"eri-sub\"\ndefault_owner: true\n",
			wantBotID:        "eri-sub",
			wantDefaultOwner: true,
		},
		{
			name:             "bot_id loaded from aerial config fallback",
			localYAML:        "port: \"4020\"\n",
			aerialYAML:       "bot_id: \"aerial-fallback\"\n",
			wantBotID:        "aerial-fallback",
			wantDefaultOwner: false,
		},
		{
			name:      "webhook_forward_urls normalized from yaml",
			localYAML: "webhook_forward_urls:\n  \" eri-sub \": \" http://eri-sub:4020/ \"\n  \"\": \"http://empty-key:4020/\"\n  \"empty-val\": \"   \"\n",
			wantForwardURLs: map[string]string{
				"eri-sub": "http://eri-sub:4020",
			},
		},
		{
			name:             "env overrides for bot_id and default_owner",
			localYAML:        "bot_id: \"yaml-bot\"\ndefault_owner: false\n",
			env:              map[string]string{"AERIAL_BOT_ID": "env-bot", "DEFAULT_OWNER": "true"},
			wantBotID:        "env-bot",
			wantDefaultOwner: true,
		},
		{
			name:      "default bot_id is aerial when unset",
			localYAML: "port: \"4020\"\n",
			wantBotID: "aerial",
		},
		{
			name:      "collaborator_ci_alerts loaded from local yaml",
			localYAML: "collaborator_ci_alerts:\n  - repo: \"mirrormere\"\n    channel_id: \"123456789012345678\"\n    authors:\n      Collaborator-User: \"<@111222>\"\n",
			wantCollaboratorAlerts: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/mirrormere",
					ChannelID: "123456789012345678",
					Authors: map[string]string{
						"collaborator-user": "<@111222>",
					},
				},
			},
		},
		{
			name:       "collaborator_ci_alerts loaded from aerial config fallback",
			localYAML:  "port: \"4020\"\n",
			aerialYAML: "collaborator_ci_alerts:\n  - repo: \"azylman/aerial\"\n    channel_id: \"987654321098765432\"\n    authors:\n      Collaborator: \"<@333444>\"\n",
			wantCollaboratorAlerts: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/aerial",
					ChannelID: "987654321098765432",
					Authors: map[string]string{
						"collaborator": "<@333444>",
					},
				},
			},
		},
	}
}

func TestLoadConfig_YAMLAndOverrides(t *testing.T) {
	tests := loadConfigYAMLTestCases()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runLoadConfigCase(t, tc)
		})
	}
}

func TestNormalizeWebhookForwardURLs(t *testing.T) {
	t.Parallel()

	paddedKey := " bot1 "
	whitespaceKey := "   "

	tests := []struct {
		name string
		raw  map[string]string
		want map[string]string
	}{
		{
			name: "nil input returns nil",
			raw:  nil,
			want: nil,
		},
		{
			name: "empty input returns nil",
			raw:  map[string]string{},
			want: nil,
		},
		{
			name: "all empty keys or values returns nil",
			raw: map[string]string{
				"":            "http://example.com",
				whitespaceKey: "http://example.com",
				"bot1":        "",
				"bot2":        "   ",
			},
			want: nil,
		},
		{
			name: "trims whitespace and trailing slashes",
			raw: map[string]string{
				paddedKey: " http://example.com/api/ ",
				"bot2":    "http://example.org///",
			},
			want: map[string]string{
				"bot1": "http://example.com/api",
				"bot2": "http://example.org",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeWebhookForwardURLs(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("len(got) = %d, want %d: %v", len(got), len(tc.want), got)
			}
			for k, wantVal := range tc.want {
				if gotVal, ok := got[k]; !ok || gotVal != wantVal {
					t.Errorf("got[%q] = %q (ok=%v), want %q", k, gotVal, ok, wantVal)
				}
			}
		})
	}
}

func withField(base, overlay Config) Config {
	dst := reflect.ValueOf(&base).Elem()
	src := reflect.ValueOf(overlay)
	for i := range src.NumField() {
		if !src.Field(i).IsZero() {
			dst.Field(i).Set(src.Field(i))
		}
	}
	return base
}

func TestRestartRequired(t *testing.T) {
	t.Parallel()
	base := Config{
		Port:                "4020",
		PostgresURL:         "postgres://a@db/aerial",
		DiscordToken:        "tok",
		BotID:               "aerial",
		BrainURL:            "http://eri:8080",
		EriURL:              "http://eri:8080",
		FluxReceiverURL:     "http://flux/hook",
		KubeAPIServer:       "https://10.0.0.1:443",
		KubeToken:           "kube",
		SystemChannelID:     "111",
		AutoUpdatePRAuthors: []string{"a"},
		WebhookForwardURLs:  map[string]string{"marida": "http://marida:4020"},
	}
	tests := []struct {
		name string
		next Config
		want bool
	}{
		{name: "identical config", next: base, want: false},
		{name: "system channel changes live", next: withField(base, Config{SystemChannelID: "222"}), want: false},
		{name: "auto-update authors change live", next: withField(base, Config{AutoUpdatePRAuthors: []string{"b"}}), want: false},
		{name: "webhook forward urls change live", next: withField(base, Config{WebhookForwardURLs: map[string]string{"x": "http://x"}}), want: false},
		{name: "default owner changes live", next: withField(base, Config{DefaultOwner: true}), want: false},
		{name: "collaborator alerts change live", next: withField(base, Config{CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "azylman/aerial", ChannelID: "111", Authors: map[string]string{"dev": "<@1>"}}}}), want: false},
		{name: "port requires restart", next: withField(base, Config{Port: "4021"}), want: true},
		{name: "postgres url requires restart", next: withField(base, Config{PostgresURL: "postgres://b@db/aerial"}), want: true},
		{name: "discord token requires restart", next: withField(base, Config{DiscordToken: "tok2"}), want: true},
		{name: "bot id requires restart", next: withField(base, Config{BotID: "marida"}), want: true},
		{name: "eri url requires restart", next: withField(base, Config{BrainURL: "http://eri2:8080", EriURL: "http://eri2:8080"}), want: true},
		{name: "flux receiver url requires restart", next: withField(base, Config{FluxReceiverURL: "http://flux2/hook"}), want: true},
		{name: "kube api server requires restart", next: withField(base, Config{KubeAPIServer: "https://10.0.0.2:443"}), want: true},
		{name: "kube token requires restart", next: withField(base, Config{KubeToken: "kube2"}), want: true},
		{name: "nats url requires restart", next: withField(base, Config{NATSURL: "nats://nats2:4222"}), want: true},
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

	if !RestartRequired(nil, &base) || !RestartRequired(&base, nil) {
		t.Fatal("expected RestartRequired to return true for nil configs")
	}
}

func TestLoadConfigFromEnvStrict(t *testing.T) {
	tmpDir := t.TempDir()
	validYAML := filepath.Join(tmpDir, "valid.yaml")
	if err := os.WriteFile(validYAML, []byte("port: \"5050\"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("PORT", "")
	t.Setenv("CONFIG_PATH", validYAML)
	cfg, err := LoadConfigFromEnvStrict()
	if err != nil {
		t.Fatalf("LoadConfigFromEnvStrict() err = %v", err)
	}
	if cfg == nil || cfg.Port != "5050" {
		t.Errorf("cfg.Port = %v, want 5050", cfg)
	}

	invalidYAML := filepath.Join(tmpDir, "invalid.yaml")
	_ = os.WriteFile(invalidYAML, []byte("bad: [yaml"), 0o644)
	t.Setenv("CONFIG_PATH", invalidYAML)
	if _, err := LoadConfigFromEnvStrict(); err == nil {
		t.Errorf("expected error from LoadConfigFromEnvStrict on bad yaml")
	}
}

func TestNormalizeCollaboratorCIAlerts(t *testing.T) {
	t.Parallel()

	paddedAuthor := " Collaborator-User "

	tests := []struct {
		name  string
		rules []CollaboratorCIAlertRule
		want  []CollaboratorCIAlertRule
	}{
		{
			name:  "nil input returns nil",
			rules: nil,
			want:  nil,
		},
		{
			name:  "empty input returns nil",
			rules: []CollaboratorCIAlertRule{},
			want:  nil,
		},
		{
			name: "drops rules with invalid channel id or empty repo",
			rules: []CollaboratorCIAlertRule{
				{Repo: "", ChannelID: "123456789012345678", Authors: map[string]string{"user": "<@1>"}},
				{Repo: "aerial", ChannelID: "invalid-id", Authors: map[string]string{"user": "<@1>"}},
				{Repo: "aerial", ChannelID: "123456789012345678", Authors: map[string]string{"": "<@1>", "user": "  "}},
			},
			want: nil,
		},
		{
			name: "normalizes valid rules and lowercases author logins",
			rules: []CollaboratorCIAlertRule{
				{
					Repo:      " mirrormere ",
					ChannelID: "123456789012345678",
					Authors: map[string]string{
						paddedAuthor: " <@111222> ",
						"":           "<@222333>",
					},
				},
			},
			want: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/mirrormere",
					ChannelID: "123456789012345678",
					Authors: map[string]string{
						"collaborator-user": "<@111222>",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeCollaboratorCIAlerts(tc.rules)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("NormalizeCollaboratorCIAlerts() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLookupCollaboratorCIAlert(t *testing.T) {
	t.Parallel()

	rules := []CollaboratorCIAlertRule{
		{
			Repo:      "azylman/mirrormere",
			ChannelID: "123456789012345678",
			Authors: map[string]string{
				"collaborator-user": "<@111222>",
			},
		},
	}
	cfg := Config{CollaboratorCIAlerts: rules}

	tests := []struct {
		name          string
		cfg           *Config
		repo          string
		author        string
		wantChannelID string
		wantMention   string
		wantOk        bool
	}{
		{
			name:          "matching repo and case-insensitive author returns alert info",
			cfg:           &cfg,
			repo:          "mirrormere",
			author:        "Collaborator-User",
			wantChannelID: "123456789012345678",
			wantMention:   "<@111222>",
			wantOk:        true,
		},
		{
			name:   "unconfigured author returns false",
			cfg:    &cfg,
			repo:   "mirrormere",
			author: "someone-else",
			wantOk: false,
		},
		{
			name:   "unconfigured repo returns false",
			cfg:    &cfg,
			repo:   "aerial",
			author: "collaborator-user",
			wantOk: false,
		},
		{
			name:   "nil config returns false",
			cfg:    nil,
			repo:   "mirrormere",
			author: "collaborator-user",
			wantOk: false,
		},
		{
			name:   "empty repo returns false",
			cfg:    &cfg,
			repo:   "",
			author: "collaborator-user",
			wantOk: false,
		},
		{
			name:   "empty author returns false",
			cfg:    &cfg,
			repo:   "mirrormere",
			author: "",
			wantOk: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			channelID, mention, ok := LookupCollaboratorCIAlert(tc.cfg, tc.repo, tc.author)
			if ok != tc.wantOk || channelID != tc.wantChannelID || mention != tc.wantMention {
				t.Errorf("LookupCollaboratorCIAlert(%q, %q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.repo, tc.author, channelID, mention, ok, tc.wantChannelID, tc.wantMention, tc.wantOk)
			}
		})
	}

	if !HasCollaboratorCIAlertsForRepo(&cfg, "mirrormere") {
		t.Errorf("expected HasCollaboratorCIAlertsForRepo(mirrormere) to be true")
	}
	if HasCollaboratorCIAlertsForRepo(&cfg, "aerial") {
		t.Errorf("expected HasCollaboratorCIAlertsForRepo(aerial) to be false")
	}
	if HasCollaboratorCIAlertsForRepo(&cfg, "") {
		t.Errorf("expected HasCollaboratorCIAlertsForRepo with empty repo to be false")
	}
	cfgNoAlerts := Config{}
	if HasCollaboratorCIAlertsForRepo(&cfgNoAlerts, "mirrormere") {
		t.Errorf("expected false for cfgNoAlerts HasCollaboratorCIAlertsForRepo")
	}
	if _, _, ok := LookupCollaboratorCIAlert(&cfgNoAlerts, "mirrormere", "collaborator-user"); ok {
		t.Errorf("expected false for cfgNoAlerts LookupCollaboratorCIAlert")
	}
	if HasCollaboratorCIAlertsForRepo(nil, "mirrormere") {
		t.Errorf("expected HasCollaboratorCIAlertsForRepo on nil config to be false")
	}
}

func TestNormalizeLegacyBuildWorkflows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, path, sha string
		valid                 bool
	}{
		{"valid", "owner/repo", ".github/workflows/ci.yml", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"bad repo", "repo", ".github/workflows/ci.yml", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"traversal", "owner/repo", ".github/workflows/../ci.yml", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"bad hash", "owner/repo", ".github/workflows/ci.yml", "main", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NormalizeLegacyBuildWorkflows([]LegacyBuildWorkflow{{Repository: tc.repo, WorkflowPath: tc.path, BlobSHA: tc.sha}})
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
