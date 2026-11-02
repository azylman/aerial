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
		name          string
		env           map[string]string
		wantPort      string
		wantInfURL    string
		wantCID       string
		wantCSec      string
		wantPID       string
		wantEnv       string
		wantNomadAddr string
		wantNomadTok  string
		wantGHTok     string
		wantGHURL     string
		wantPGURL     string
		wantBrainURL  string
		wantEriURL    string
		wantSysChan   string
		wantDiscTok   string
	}{
		{
			name: "primary infisical and nomad envs",
			env: map[string]string{
				"PORT":                    "9999",
				"INFISICAL_URL":           "http://infisical:8085/",
				"INFISICAL_CLIENT_ID":     "cid",
				"INFISICAL_CLIENT_SECRET": "csec",
				"INFISICAL_PROJECT_ID":    "pid",
				"INFISICAL_ENVIRONMENT":   "staging",
				"NOMAD_ADDR":              "http://nomad:4646/",
				"NOMAD_TOKEN":             "ntok",
			},
			wantPort:      "9999",
			wantInfURL:    "http://infisical:8085",
			wantCID:       "cid",
			wantCSec:      "csec",
			wantPID:       "pid",
			wantEnv:       "staging",
			wantNomadAddr: "http://nomad:4646",
			wantNomadTok:  "ntok",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
		},
		{
			name:          "default fallback loading with empty envs",
			env:           map[string]string{},
			wantPort:      "4020",
			wantInfURL:    "http://infisical.aerial",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
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
	}

	keysToClear := []string{
		"PORT", "INFISICAL_URL", "INFISICAL_CLIENT_ID",
		"INFISICAL_CLIENT_SECRET", "INFISICAL_PROJECT_ID",
		"INFISICAL_ENVIRONMENT", "NOMAD_ADDR",
		"NOMAD_TOKEN", "GITHUB_PAT", "GITHUB_API_URL",
		"DATABASE_URL", "HANGAR_URL", "ERI_URL",
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
				{cfg.NomadAddr, tc.wantNomadAddr, "NomadAddr"},
				{cfg.NomadToken, tc.wantNomadTok, "NomadToken"},
				{cfg.GitHubToken, tc.wantGHTok, "GitHubToken"},
				{cfg.GitHubAPIURL, tc.wantGHURL, "GitHubAPIURL"},
				{cfg.PostgresURL, tc.wantPGURL, "PostgresURL"},
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
		name                   string
		localYAML              string
		fallbackYAML           string
		aerialYAML             string
		useEmptyPath           bool
		missingFallback        bool
		env                    map[string]string
		wantPort               string
		wantInfURL             string
		wantEnv                string
		wantNomadAddr          string
		wantHangarURL          string
		wantBrainURL           string
		wantSysChan            string
		wantAuthors            []string
		wantCollaboratorAlerts []CollaboratorCIAlertRule
	}{
		{
			name: "yaml config without overrides",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
eri_url: "http://brain.custom:8088"
`,
			wantPort:      "4099",
			wantInfURL:    "http://infisical.custom:8085",
			wantEnv:       "staging",
			wantNomadAddr: "http://nomad.custom:4646",
			wantHangarURL: "http://hangar.custom:8087",
			wantBrainURL:  "http://brain.custom:8088",
		},
		{
			name: "yaml config with eri_url",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
eri_url: "http://eri.custom:8088"
`,
			wantPort:      "4099",
			wantInfURL:    "http://infisical.custom:8085",
			wantEnv:       "staging",
			wantNomadAddr: "http://nomad.custom:4646",
			wantHangarURL: "http://hangar.custom:8087",
			wantBrainURL:  "http://eri.custom:8088",
		},
		{
			name: "env overrides yaml values",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
eri_url: "http://brain.custom:8088"
`,
			env: map[string]string{
				"PORT":                  "5000",
				"INFISICAL_ENVIRONMENT": "dev",
			},
			wantPort:      "5000",
			wantInfURL:    "http://infisical.custom:8085",
			wantEnv:       "dev",
			wantNomadAddr: "http://nomad.custom:4646",
			wantHangarURL: "http://hangar.custom:8087",
			wantBrainURL:  "http://brain.custom:8088",
		},
		{
			name:          "zero-byte local config with existing fallback",
			localYAML:     "",
			fallbackYAML:  "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\nnomad_addr: \"http://nomad.fallback:4646\"\n",
			wantPort:      "4088",
			wantInfURL:    "http://infisical.fallback:8085",
			wantNomadAddr: "http://nomad.fallback:4646",
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
			fallbackYAML: "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\nnomad_addr: \"http://nomad.fallback:4646\"\n",
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
			name: "collaborator_ci_alerts loaded from local yaml",
			localYAML: `collaborator_ci_alerts:
  - repo: "collab-repo"
    channel_id: "123456789012345678"
    authors:
      collab-alpha: "<@111111111111111111>"
`,
			wantCollaboratorAlerts: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/collab-repo",
					ChannelID: "123456789012345678",
					Authors: map[string]string{
						"collab-alpha": "<@111111111111111111>",
					},
				},
			},
		},
		{
			name:      "collaborator_ci_alerts loaded from aerial config.yaml fallback",
			localYAML: "port: \"4020\"\n",
			aerialYAML: `collaborator_ci_alerts:
  - repo: "collab-fallback"
    channel_id: "234567890123456789"
    authors:
      collab-beta: "<@222222222222222222>"
`,
			wantCollaboratorAlerts: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/collab-fallback",
					ChannelID: "234567890123456789",
					Authors: map[string]string{
						"collab-beta": "<@222222222222222222>",
					},
				},
			},
		},
		{
			name:      "invalid yaml logs warning and uses defaults",
			localYAML: "invalid: [yaml",
			wantPort:  "4020",
		},
	}

	keysToClear := []string{
		"PORT", "INFISICAL_URL", "INFISICAL_ENVIRONMENT",
		"NOMAD_ADDR", "HANGAR_URL", "ERI_URL", "SYSTEM_CHANNEL_ID",
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
				{cfg.NomadAddr, tc.wantNomadAddr, "NomadAddr"},
				{cfg.HangarURL, tc.wantHangarURL, "HangarURL"},
				{cfg.BrainURL, tc.wantBrainURL, "BrainURL"},
				{cfg.EriURL, tc.wantBrainURL, "EriURL"},
				{cfg.SystemChannelID, tc.wantSysChan, "SystemChannelID"},
			} {
				if check.want != "" && check.got != check.want {
					t.Errorf("%s = %q, want %q", check.field, check.got, check.want)
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
				if len(cfg.CollaboratorCIAlerts) != len(tc.wantCollaboratorAlerts) {
					t.Errorf("CollaboratorCIAlerts len = %d, want %d", len(cfg.CollaboratorCIAlerts), len(tc.wantCollaboratorAlerts))
				} else {
					for i, rule := range tc.wantCollaboratorAlerts {
						if cfg.CollaboratorCIAlerts[i].Repo != rule.Repo {
							t.Errorf("CollaboratorCIAlerts[%d].Repo = %q, want %q", i, cfg.CollaboratorCIAlerts[i].Repo, rule.Repo)
						}
						if cfg.CollaboratorCIAlerts[i].ChannelID != rule.ChannelID {
							t.Errorf("CollaboratorCIAlerts[%d].ChannelID = %q, want %q", i, cfg.CollaboratorCIAlerts[i].ChannelID, rule.ChannelID)
						}
						for k, v := range rule.Authors {
							if cfg.CollaboratorCIAlerts[i].Authors[k] != v {
								t.Errorf("CollaboratorCIAlerts[%d].Authors[%q] = %q, want %q", i, k, cfg.CollaboratorCIAlerts[i].Authors[k], v)
							}
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

func TestNormalizeCollaboratorCIAlerts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		rules []CollaboratorCIAlertRule
		want  []CollaboratorCIAlertRule
	}{
		{
			name:  "empty rules returns empty",
			rules: nil,
			want:  nil,
		},
		{
			name: "normalizes repo, channel id, and lowercases authors",
			rules: []CollaboratorCIAlertRule{
				{
					Repo:      "collab-repo",
					ChannelID: "discord-123456789012345678",
					Authors: map[string]string{
						strings.Trim("x Collab-Alpha x", "x"): " <@111111111111111111> ",
						"COLLAB-BETA":                         "<@222222222222222222>",
					},
				},
			},
			want: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/collab-repo",
					ChannelID: "123456789012345678",
					Authors: map[string]string{
						"collab-alpha": "<@111111111111111111>",
						"collab-beta":  "<@222222222222222222>",
					},
				},
			},
		},
		{
			name: "drops rule with invalid channel snowflake",
			rules: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/collab-repo",
					ChannelID: "invalid-snowflake",
					Authors:   map[string]string{"collab-alpha": "<@111111111111111111>"},
				},
			},
			want: nil,
		},
		{
			name: "drops rule with empty repo",
			rules: []CollaboratorCIAlertRule{
				{
					Repo:      "   ",
					ChannelID: "123456789012345678",
					Authors:   map[string]string{"collab-alpha": "<@111111111111111111>"},
				},
			},
			want: nil,
		},
		{
			name: "drops rule with zero valid authors",
			rules: []CollaboratorCIAlertRule{
				{
					Repo:      "azylman/collab-repo",
					ChannelID: "123456789012345678",
					Authors:   map[string]string{"   ": "<@111111111111111111>", "collab-alpha": "   "},
				},
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeCollaboratorCIAlerts(tc.rules)
			if len(got) != len(tc.want) {
				t.Fatalf("normalizeCollaboratorCIAlerts len = %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i].Repo != tc.want[i].Repo {
					t.Errorf("[%d].Repo = %q, want %q", i, got[i].Repo, tc.want[i].Repo)
				}
				if got[i].ChannelID != tc.want[i].ChannelID {
					t.Errorf("[%d].ChannelID = %q, want %q", i, got[i].ChannelID, tc.want[i].ChannelID)
				}
				if len(got[i].Authors) != len(tc.want[i].Authors) {
					t.Errorf("[%d].Authors len = %d, want %d", i, len(got[i].Authors), len(tc.want[i].Authors))
				}
				for k, v := range tc.want[i].Authors {
					if got[i].Authors[k] != v {
						t.Errorf("[%d].Authors[%q] = %q, want %q", i, k, got[i].Authors[k], v)
					}
				}
			}
		})
	}
}

func TestLookupCollaboratorCIAlert(t *testing.T) {
	t.Parallel()
	rules := []CollaboratorCIAlertRule{
		{
			Repo:      "azylman/collab-repo",
			ChannelID: "123456789012345678",
			Authors: map[string]string{
				"collab-alpha": "<@111111111111111111>",
			},
		},
		{
			Repo:      "otherorg/custom-repo",
			ChannelID: "discord-987654321098765432",
			Authors: map[string]string{
				"collab-beta": "<@222222222222222222>",
			},
		},
	}
	server := &RouterServer{
		cfg: Config{CollaboratorCIAlerts: rules},
	}

	tests := []struct {
		name          string
		srv           *RouterServer
		repo          string
		author        string
		wantChannelID string
		wantMention   string
		wantOK        bool
		wantHasAlerts bool
	}{
		{
			name:          "exact match returns channel and mention",
			srv:           server,
			repo:          "azylman/collab-repo",
			author:        "collab-alpha",
			wantChannelID: "123456789012345678",
			wantMention:   "<@111111111111111111>",
			wantOK:        true,
			wantHasAlerts: true,
		},
		{
			name:          "case-insensitive repo and author match without azylman prefix",
			srv:           server,
			repo:          "COLLAB-REPO",
			author:        "Collab-Alpha",
			wantChannelID: "123456789012345678",
			wantMention:   "<@111111111111111111>",
			wantOK:        true,
			wantHasAlerts: true,
		},
		{
			name:          "custom org repo with session prefix in channel id",
			srv:           server,
			repo:          "otherorg/custom-repo",
			author:        "collab-beta",
			wantChannelID: "987654321098765432",
			wantMention:   "<@222222222222222222>",
			wantOK:        true,
			wantHasAlerts: true,
		},
		{
			name:          "unknown author returns false",
			srv:           server,
			repo:          "azylman/collab-repo",
			author:        "unknown-author",
			wantOK:        false,
			wantHasAlerts: true,
		},
		{
			name:          "unknown repo returns false",
			srv:           server,
			repo:          "azylman/nonexistent-repo",
			author:        "collab-alpha",
			wantOK:        false,
			wantHasAlerts: false,
		},
		{
			name:          "nil router server returns false",
			srv:           nil,
			repo:          "azylman/collab-repo",
			author:        "collab-alpha",
			wantOK:        false,
			wantHasAlerts: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotChannel, gotMention, gotOK := tc.srv.lookupCollaboratorCIAlert(tc.repo, tc.author)
			if gotOK != tc.wantOK {
				t.Fatalf("lookupCollaboratorCIAlert ok = %v, want %v", gotOK, tc.wantOK)
			}
			if gotChannel != tc.wantChannelID {
				t.Errorf("lookupCollaboratorCIAlert channel = %q, want %q", gotChannel, tc.wantChannelID)
			}
			if gotMention != tc.wantMention {
				t.Errorf("lookupCollaboratorCIAlert mention = %q, want %q", gotMention, tc.wantMention)
			}
			if gotHas := tc.srv.hasCollaboratorCIAlertsForRepo(tc.repo); gotHas != tc.wantHasAlerts {
				t.Errorf("hasCollaboratorCIAlertsForRepo = %v, want %v", gotHas, tc.wantHasAlerts)
			}
		})
	}
}
