package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfig_EnvAndFallbacks(t *testing.T) {
	tests := []struct {
		name           string
		env            map[string]string
		wantPort       string
		wantInfURL     string
		wantCID        string
		wantCSec       string
		wantPID        string
		wantEnv        string
		wantKubeServer string
		wantKubeTok    string
		wantGHTok      string
		wantGHURL      string
		wantPGURL      string
		wantFluxURL    string
		wantBrainURL   string
		wantEriURL     string
		wantSysChan    string
		wantDiscTok    string
	}{
		{
			name: "primary infisical envs",
			env: map[string]string{
				"PORT":                    "9999",
				"INFISICAL_URL":           "http://infisical:8085/",
				"INFISICAL_CLIENT_ID":     "cid",
				"INFISICAL_CLIENT_SECRET": "csec",
				"INFISICAL_PROJECT_ID":    "pid",
				"INFISICAL_ENVIRONMENT":   "staging",
			},
			wantPort:   "9999",
			wantInfURL: "http://infisical:8085",
			wantCID:    "cid",
			wantCSec:   "csec",
			wantPID:    "pid",
			wantEnv:    "staging",
			wantGHURL:  "https://api.github.com",
			wantPGURL:  "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
		},
		{
			name:       "default fallback loading with empty envs",
			env:        map[string]string{},
			wantPort:   "4020",
			wantInfURL: "http://infisical.aerial",
			wantEnv:    "default",
			wantGHURL:  "https://api.github.com",
			wantPGURL:  "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
		},
		{
			name: "primary kube envs",
			env: map[string]string{
				"KUBE_API_SERVER": "https://kube.aerial:6443/",
				"KUBE_TOKEN":      "secret-token",
			},
			wantKubeServer: "https://kube.aerial:6443",
			wantKubeTok:    "secret-token",
		},
		{
			name: "fallback kube host and port",
			env: map[string]string{
				"KUBERNETES_SERVICE_HOST": "10.43.0.1",
				"KUBERNETES_SERVICE_PORT": "443",
			},
			wantKubeServer: "https://10.43.0.1:443",
		},
		{
			name: "github and postgres primary envs",
			env: map[string]string{
				"GITHUB_PAT":     "pat123",
				"GITHUB_API_URL": "https://api.github.test/",
				"DATABASE_URL":   "postgres://custom:pass@db:5432/customdb",
			},
			wantGHTok: "pat123",
			wantGHURL: "https://api.github.test",
			wantPGURL: "postgres://custom:pass@db:5432/customdb",
		},
		{
			name: "decomposed postgres envs",
			env: map[string]string{
				"POSTGRES_HOST":     "custom-pg.aerial",
				"POSTGRES_PORT":     "5432",
				"POSTGRES_USER":     "aerial",
				"POSTGRES_PASSWORD": "custom_pass",
				"POSTGRES_DB":       "aerial",
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
			name: "eri url primary env",
			env: map[string]string{
				"ERI_URL": "http://eri-primary.aerial:8088",
			},
			wantBrainURL: "http://eri-primary.aerial:8088",
			wantEriURL:   "http://eri-primary.aerial:8088",
		},
		{
			name: "flux receiver url primary env",
			env: map[string]string{
				"FLUX_RECEIVER_URL": "http://flux-receiver.aerial/webhook/",
			},
			wantFluxURL: "http://flux-receiver.aerial/webhook",
		},
	}

	keysToClear := []string{
		"PORT", "INFISICAL_URL", "INFISICAL_CLIENT_ID",
		"INFISICAL_CLIENT_SECRET", "INFISICAL_PROJECT_ID",
		"INFISICAL_ENVIRONMENT", "KUBE_API_SERVER",
		"KUBE_TOKEN", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT",
		"GITHUB_PAT", "GITHUB_API_URL",
		"DATABASE_URL", "POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT",
		"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
		"HANGAR_URL", "FLUX_RECEIVER_URL", "ERI_URL",
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
				{cfg.InfisicalURL, tc.wantInfURL, "InfisicalURL"},
				{cfg.InfisicalClientID, tc.wantCID, "InfisicalClientID"},
				{cfg.InfisicalClientSecret, tc.wantCSec, "InfisicalClientSecret"},
				{cfg.InfisicalProjectID, tc.wantPID, "InfisicalProjectID"},
				{cfg.InfisicalEnvironment, tc.wantEnv, "InfisicalEnvironment"},
				{cfg.KubeAPIServer, tc.wantKubeServer, "KubeAPIServer"},
				{cfg.KubeToken, tc.wantKubeTok, "KubeToken"},
				{cfg.GitHubToken, tc.wantGHTok, "GitHubToken"},
				{cfg.GitHubAPIURL, tc.wantGHURL, "GitHubAPIURL"},
				{cfg.PostgresURL, tc.wantPGURL, "PostgresURL"},
				{cfg.FluxReceiverURL, tc.wantFluxURL, "FluxReceiverURL"},
				{cfg.BrainURL, tc.wantBrainURL, "BrainURL"},
				{cfg.EriURL, tc.wantEriURL, "EriURL"},
				{cfg.SystemChannelID, tc.wantSysChan, "SystemChannelID"},
				{cfg.DiscordToken, tc.wantDiscTok, "DiscordToken"},
			} {
				if check.want != "" && check.got != check.want {
					t.Errorf("%s = %q, want %q", check.field, check.got, check.want)
				}
			}
		})
	}
}

func TestLoadConfig_YAMLAndOverrides(t *testing.T) {
	tests := []struct {
		name             string
		localYAML        string
		fallbackYAML     string
		aerialYAML       string
		useEmptyPath     bool
		missingFallback  bool
		env              map[string]string
		wantPort         string
		wantInfURL       string
		wantEnv          string
		wantFluxURL      string
		wantBrainURL     string
		wantSysChan      string
		wantAuthors      []string
		wantBotID        string
		wantDefaultOwner bool
		wantForwardURLs  map[string]string
	}{
		{
			name: "yaml config without overrides",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
eri_url: "http://brain.custom:8088"
`,
			wantPort:     "4099",
			wantInfURL:   "http://infisical.custom:8085",
			wantEnv:      "staging",
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
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
eri_url: "http://eri.custom:8088"
`,
			wantPort:     "4099",
			wantInfURL:   "http://infisical.custom:8085",
			wantEnv:      "staging",
			wantBrainURL: "http://eri.custom:8088",
		},
		{
			name: "env overrides yaml values",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
eri_url: "http://brain.custom:8088"
`,
			env: map[string]string{
				"PORT":                  "5000",
				"INFISICAL_ENVIRONMENT": "dev",
			},
			wantPort:     "5000",
			wantInfURL:   "http://infisical.custom:8085",
			wantEnv:      "dev",
			wantBrainURL: "http://brain.custom:8088",
		},
		{
			name:         "zero-byte local config with existing fallback",
			localYAML:    "",
			fallbackYAML: "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\n",
			wantPort:     "4088",
			wantInfURL:   "http://infisical.fallback:8085",
		},
		{
			name:            "zero-byte local config with missing fallback uses defaults",
			localYAML:       "",
			missingFallback: true,
			wantPort:        "4020",
			wantInfURL:      "http://infisical.aerial",
		},
		{
			name:         "empty config path with valid fallback",
			useEmptyPath: true,
			fallbackYAML: "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\n",
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
			name:      "invalid yaml logs warning and uses defaults",
			localYAML: "invalid: [yaml",
			wantPort:  "4020",
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
	}

	keysToClear := []string{
		"PORT", "INFISICAL_URL", "INFISICAL_ENVIRONMENT",
		"HANGAR_URL", "FLUX_RECEIVER_URL", "ERI_URL", "SYSTEM_CHANNEL_ID",
		"AERIAL_BOT_ID", "DEFAULT_OWNER", "AERIAL_DEFAULT_OWNER",
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var targetPath string
			if !tc.useEmptyPath {
				targetPath = filepath.Join(tmpDir, "local.yaml")
				if err := os.WriteFile(targetPath, []byte(tc.localYAML), 0o644); err != nil {
					t.Fatalf("failed to write local config: %v", err)
				}
			}

			oldAerial := synapseAerialConfigPath
			if tc.aerialYAML != "" {
				aerialPath := filepath.Join(tmpDir, "config.yaml")
				if err := os.WriteFile(aerialPath, []byte(tc.aerialYAML), 0o644); err != nil {
					t.Fatalf("failed to write aerial config: %v", err)
				}
				synapseAerialConfigPath = aerialPath
			} else {
				synapseAerialConfigPath = filepath.Join(tmpDir, "nonexistent-config.yaml")
			}
			t.Cleanup(func() { synapseAerialConfigPath = oldAerial })

			oldFallback := synapseFallbackConfigPath
			if tc.fallbackYAML != "" || tc.missingFallback {
				if tc.missingFallback {
					synapseFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
				} else {
					fbPath := filepath.Join(tmpDir, "fallback.yaml")
					if err := os.WriteFile(fbPath, []byte(tc.fallbackYAML), 0o644); err != nil {
						t.Fatalf("failed to write fallback config: %v", err)
					}
					synapseFallbackConfigPath = fbPath
				}
			} else {
				synapseFallbackConfigPath = filepath.Join(tmpDir, "nonexistent-synapse.yaml")
			}
			t.Cleanup(func() {
				synapseFallbackConfigPath = oldFallback
			})

			for _, k := range keysToClear {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg := LoadConfig(targetPath)
			for _, check := range []struct{ got, want, field string }{
				{cfg.Port, tc.wantPort, "Port"},
				{cfg.InfisicalURL, tc.wantInfURL, "InfisicalURL"},
				{cfg.InfisicalEnvironment, tc.wantEnv, "InfisicalEnvironment"},
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
				if tc.wantDefaultOwner != cfg.DefaultOwner {
					t.Errorf("DefaultOwner = %v, want %v", cfg.DefaultOwner, tc.wantDefaultOwner)
				}
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
		})
	}
}

func TestStringAndRepoHelpers(t *testing.T) {
	t.Parallel()

	zeroSHATests := []struct {
		name string
		sha  string
		want bool
	}{
		{name: "isZeroSHA empty", sha: "", want: true},
		{name: "isZeroSHA 40 zeroes", sha: "0000000000000000000000000000000000000000", want: true},
		{name: "isZeroSHA single zero", sha: "0", want: true},
		{name: "isZeroSHA three zeroes", sha: "000", want: true},
		{name: "isZeroSHA non-zero ending with 1", sha: "0000000000000000000000000000000000000001", want: false},
		{name: "isZeroSHA valid commit sha", sha: "ed197571a381920f540e7a3c55b68008a45912a0", want: false},
	}
	for _, tc := range zeroSHATests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isZeroSHA(tc.sha); got != tc.want {
				t.Errorf("isZeroSHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		})
	}

	normalizeTests := []struct {
		name, input, want string
	}{
		{name: "normalizeRepo empty", input: "", want: ""},
		{name: "normalizeRepo whitespace", input: "   ", want: ""},
		{name: "normalizeRepo aerial prefix", input: "aerial", want: "azylman/aerial"},
		{name: "normalizeRepo fully qualified aerial", input: "azylman/aerial", want: "azylman/aerial"},
		{name: "normalizeRepo trimmed mirrormere", input: "  mirrormere  ", want: "azylman/mirrormere"},
		{name: "normalizeRepo other org repo", input: "otherorg/custom", want: "otherorg/custom"},
	}
	for _, tc := range normalizeTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeRepo(tc.input); got != tc.want {
				t.Errorf("normalizeRepo(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	cleanRepoTests := []struct {
		name, input, want string
	}{
		{name: "cleanRepoName azylman prefix", input: "azylman/aerial", want: "aerial"},
		{name: "cleanRepoName with whitespace", input: "  azylman/mirrormere  ", want: "mirrormere"},
		{name: "cleanRepoName already clean", input: "aerial", want: "aerial"},
		{name: "cleanRepoName foreign repo", input: "otherorg/custom", want: "otherorg/custom"},
		{name: "cleanRepoName empty", input: "", want: ""},
	}
	for _, tc := range cleanRepoTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := cleanRepoName(tc.input); got != tc.want {
				t.Errorf("cleanRepoName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	prefixTests := []struct {
		name, repo, want string
		prNumber         int
	}{
		{name: "formatDirectMessagePrefix pr and repo", prNumber: 42, repo: "azylman/aerial", want: "**(PR: #42, repo: aerial)** "},
		{name: "formatDirectMessagePrefix repo only", prNumber: 0, repo: "azylman/aerial", want: "**(repo: aerial)** "},
		{name: "formatDirectMessagePrefix pr only with empty repo", prNumber: 99, repo: "", want: "**(PR: #99)** "},
		{name: "formatDirectMessagePrefix neither", prNumber: 0, repo: "", want: ""},
	}
	for _, tc := range prefixTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatDirectMessagePrefix(tc.prNumber, tc.repo); got != tc.want {
				t.Errorf("formatDirectMessagePrefix(%d, %q) = %q, want %q", tc.prNumber, tc.repo, got, tc.want)
			}
		})
	}

	dedupeTests := []struct {
		name        string
		input, want []string
	}{
		{name: "dedupeTrimmedStrings duplicates and blanks", input: []string{"foo", "  foo  ", "bar", "", "   ", "baz", "bar"}, want: []string{"foo", "bar", "baz"}},
		{name: "dedupeTrimmedStrings empty slice", input: []string{}, want: nil},
	}
	for _, tc := range dedupeTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dedupeTrimmedStrings(tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("dedupeTrimmedStrings() len = %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("dedupeTrimmedStrings()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}

	if got := readBodyString(errReader{}); got != "" {
		t.Errorf("readBodyString(errReader) = %q, want empty", got)
	}
	if got := readBodyString(strings.NewReader("")); got != "" {
		t.Errorf("readBodyString(empty) = %q, want empty", got)
	}
	if n := len(randomNonce()); n != 32 {
		t.Errorf("len(randomNonce()) = %d, want 32", n)
	}
}

func TestPromptAndContextFormatting(t *testing.T) {
	t.Parallel()

	truncateTests := []struct {
		name, input, wantSuffix, wantSub, wantNotSub string
		wantLen                                      int
	}{
		{name: "truncatePromptDetails sanitizes backticks", input: "error occurred:\n```json\n{\"error\":\"broken\"}\n```\nend", wantSub: "'''json", wantNotSub: "```"},
		{name: "truncatePromptDetails short unchanged", input: "short error message", wantSub: "short error message"},
		{name: "truncatePromptDetails long truncated", input: strings.Repeat("a", 2000), wantSuffix: "... (truncated)", wantLen: maxPromptRunes + len([]rune("... (truncated)"))},
	}
	for _, tc := range truncateTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := truncatePromptDetails(tc.input)
			if tc.wantSuffix != "" && !strings.HasSuffix(got, tc.wantSuffix) {
				t.Errorf("truncatePromptDetails() missing suffix %q", tc.wantSuffix)
			}
			if tc.wantLen > 0 && len([]rune(got)) != tc.wantLen {
				t.Errorf("truncatePromptDetails() len = %d, want %d", len([]rune(got)), tc.wantLen)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Errorf("truncatePromptDetails() missing %q: %s", tc.wantSub, got)
			}
			if tc.wantNotSub != "" && strings.Contains(got, tc.wantNotSub) {
				t.Errorf("truncatePromptDetails() contains unexpected %q: %s", tc.wantNotSub, got)
			}
		})
	}

	errorBlockTests := []struct {
		name, header, details, extraLine, footer, wantExact string
		wantSub                                             []string
	}{
		{name: "formatErrorDetailsBlock all populated", header: "Header", details: "details err", extraLine: "extra line", footer: "footer info", wantSub: []string{"Header", "Error details:\n```\ndetails err\n```", "extra line", "footer info"}},
		{name: "formatErrorDetailsBlock header only", header: "Header", wantExact: "Header"},
	}
	for _, tc := range errorBlockTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatErrorDetailsBlock(tc.header, tc.details, tc.extraLine, tc.footer)
			if tc.wantExact != "" && got != tc.wantExact {
				t.Errorf("formatErrorDetailsBlock() = %q, want %q", got, tc.wantExact)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("formatErrorDetailsBlock() missing %q: %s", sub, got)
				}
			}
		})
	}

	vanadisPromptTests := []struct {
		name    string
		res     *VanadisAdvancementResult
		wantSub []string
		wantLen int
	}{
		{name: "formatVanadisBuildCompletedPrompt nil result", res: nil, wantLen: 0},
		{
			name: "formatVanadisBuildCompletedPrompt with chunks",
			res: &VanadisAdvancementResult{
				BuildID: "build-42", Tier: 2, Objective: "Complete feature migration",
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, Title: "Chunk One", PRNumber: 101},
					{ChunkIndex: 1, Title: "Chunk Two", Repo: "", PRNumber: 0},
				},
			},
			wantSub: []string{
				"Vanadis build build-42 completed (Tier 2, 2 chunks merged).",
				"Objective: Complete feature migration",
				"Merged Chunks Ledger:",
				"- Chunk 1/2: Chunk One (azylman/aerial, PR #101)",
				"- Chunk 2/2: Chunk Two (azylman/aerial)",
				"All Vanadis build chunks have merged into main.",
			},
		},
	}
	for _, tc := range vanadisPromptTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatVanadisBuildCompletedPrompt(tc.res)
			if tc.wantLen == 0 && tc.res == nil && got != "" {
				t.Errorf("formatVanadisBuildCompletedPrompt(nil) = %q, want empty", got)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("formatVanadisBuildCompletedPrompt() missing %q: %s", sub, got)
				}
			}
		})
	}
}

func TestIs4xxClientError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},
		{name: "status 400", err: errors.New("brain prompt returned status 400: bad request"), want: true},
		{name: "status 404", err: errors.New("brain prompt returned status 404: not found"), want: true},
		{name: "status 422", err: errors.New("brain internal message returned status 422: unprocessable"), want: true},
		{name: "status 403", err: errors.New("brain prompt returned status 403: forbidden"), want: true},
		{name: "status 500", err: errors.New("brain prompt returned status 500: internal server error"), want: false},
		{name: "status 502", err: errors.New("brain prompt returned status 502: bad gateway"), want: false},
		{name: "network timeout", err: errors.New("dial tcp 127.0.0.1:8088: i/o timeout"), want: false},
		{name: "invalid snowflake", err: errors.New("invalid discord snowflake channel id: \"abc\""), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := is4xxClientError(tc.err); got != tc.want {
				t.Errorf("%s: is4xxClientError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
			}
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
			got := normalizeWebhookForwardURLs(tc.raw)
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
