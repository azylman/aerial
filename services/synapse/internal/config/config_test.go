package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/k8sauth"
)

// TestMain points both file fallbacks at paths that never exist, so no test
// reads the live /share/aerial-config/config.yaml or /config/synapse.yaml.
// Tests that exercise a fallback write their own temp file and repoint it.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "synapse-config-test-")
	if err != nil {
		panic(err)
	}
	SynapseAerialConfigPath = filepath.Join(dir, "absent-aerial-config.yaml")
	SynapseFallbackConfigPath = filepath.Join(dir, "absent-synapse.yaml")
	hermetictest.Main(m, hermetictest.WithCleanup(func() { _ = os.RemoveAll(dir) }))
}

// synapseConfigKeysToClear lists every env key buildConfig reads (besides
// CONFIG_PATH and per-bot token keys), so a poisoned pod env cannot leak in.
var synapseConfigKeysToClear = []string{
	"PORT", "KUBE_API_SERVER", "KUBE_TOKEN", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT",
	"GITHUB_PAT", "GITHUB_API_URL", "GITHUB_WEBHOOK_SECRET", "DISCORD_TOKEN", "AERIAL_BOT_ID", "ERI_URL",
	"NATS_URL", "SYSTEM_CHANNEL_ID", "DEFAULT_OWNER", "AERIAL_DEFAULT_OWNER", "K8S_AUTH_ENABLED", "AUTH_ENABLED",
}

const loadDefaultEriURL = "http://127.0.0.1:8088"

// loadConfigFiles writes the row's synapse.yaml (always, zero bytes when
// empty), fallback.yaml and aerial.yaml into a temp dir, repoints both
// package fallbacks there, clears the env, sets CONFIG_PATH to synapse.yaml
// and then applies env (which may blank CONFIG_PATH). It returns the dir.
func loadConfigFiles(t *testing.T, local, fallback, aerial string, env map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"synapse.yaml": local, "fallback.yaml": fallback, "aerial.yaml": aerial} {
		if body == "" && name != "synapse.yaml" {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}
	oldAerial, oldFallback := SynapseAerialConfigPath, SynapseFallbackConfigPath
	SynapseAerialConfigPath = filepath.Join(dir, "aerial.yaml")
	SynapseFallbackConfigPath = filepath.Join(dir, "fallback.yaml")
	t.Cleanup(func() { SynapseAerialConfigPath, SynapseFallbackConfigPath = oldAerial, oldFallback })
	for _, k := range synapseConfigKeysToClear {
		t.Setenv(k, "")
	}
	t.Setenv("CONFIG_PATH", filepath.Join(dir, "synapse.yaml"))
	for k, v := range env {
		t.Setenv(k, v)
	}
	return dir
}

// loadWant overlays the non-zero fields of overlay onto the config an empty
// file and empty env resolve to.
func loadWant(overlay Config) Config {
	return withField(Config{
		Port: "4020", GitHubAPIURL: "https://api.github.com", GitHubCredential: "app",
		BrainURL: loadDefaultEriURL, EriURL: loadDefaultEriURL, NATSURL: "nats://nats.aerial:4222",
		RestatePRService: "PRObject", RestateRepoService: "RepoObject", BotID: "aerial",
		Bots: loadBot("aerial", "DISCORD_TOKEN", "", loadDefaultEriURL, "", false),
	}, overlay)
}

// loadBot builds a one-bot Bots map.
func loadBot(id, tokenEnv, token, eriURL, sysChan string, owner bool) map[string]BotConfig {
	return map[string]BotConfig{id: {TokenEnv: tokenEnv, DiscordToken: token, EriURL: eriURL, SystemChannelID: sysChan, DefaultOwner: owner}}
}

func TestLoadConfig(t *testing.T) {
	const brainURL, restateURL = "http://brain.example:8088", "http://restate.example:8080"
	alphaEri, betaEri := "http://eri-alpha.example:8088", "http://eri-beta.example:8088"
	tests := []struct {
		name     string
		local    string
		fallback string
		aerial   string
		env      map[string]string
		want     Config
	}{
		// Environment.
		{name: "empty file and env resolve to defaults", want: loadWant(Config{})},
		{name: "port env", env: map[string]string{"PORT": "9999"}, want: loadWant(Config{Port: "9999"})},
		{name: "kube api server env trimmed", env: map[string]string{"KUBE_API_SERVER": "https://kube.example:6443/", "KUBE_TOKEN": "secret-token"}, want: loadWant(Config{KubeAPIServer: "https://kube.example:6443", KubeToken: "secret-token", K8sAuthConfig: k8sauth.Config{APIServerURL: "https://kube.example:6443"}})},
		{name: "in-cluster kube host and port", env: map[string]string{"KUBERNETES_SERVICE_HOST": "192.0.2.1", "KUBERNETES_SERVICE_PORT": "443"}, want: loadWant(Config{KubeAPIServer: "https://192.0.2.1:443", K8sAuthConfig: k8sauth.Config{APIServerURL: "https://192.0.2.1:443"}})},
		{name: "github envs", env: map[string]string{"GITHUB_PAT": "pat123", "GITHUB_API_URL": "https://api.github.test/", "GITHUB_WEBHOOK_SECRET": " hook-key "}, want: loadWant(Config{GitHubToken: "pat123", GitHubAPIURL: "https://api.github.test", GitHubWebhookSecret: "hook-key"})},
		{name: "system channel env", env: map[string]string{"SYSTEM_CHANNEL_ID": "123456789012345678"}, want: loadWant(Config{SystemChannelID: "123456789012345678"})},
		{name: "discord token env", env: map[string]string{"DISCORD_TOKEN": "bot-token"}, want: loadWant(Config{DiscordToken: "bot-token", Bots: loadBot("aerial", "DISCORD_TOKEN", "bot-token", loadDefaultEriURL, "", false)})},
		{name: "eri url env", env: map[string]string{"ERI_URL": alphaEri + "/"}, want: loadWant(Config{BrainURL: alphaEri, EriURL: alphaEri, Bots: loadBot("aerial", "DISCORD_TOKEN", "", alphaEri, "", false)})},
		{name: "nats url env", env: map[string]string{"NATS_URL": "nats://nats.example:4222"}, want: loadWant(Config{NATSURL: "nats://nats.example:4222"})},
		// File resolution.
		{name: "yaml port and eri url", local: "port: \"4099\"\neri_url: \"" + brainURL + "\"\n", want: loadWant(Config{Port: "4099", BrainURL: brainURL, EriURL: brainURL, Bots: loadBot("aerial", "DISCORD_TOKEN", "", brainURL, "", false)})},
		{name: "env overrides yaml", local: "port: \"4099\"\neri_url: \"" + brainURL + "\"\n", env: map[string]string{"PORT": "5000"}, want: loadWant(Config{Port: "5000", BrainURL: brainURL, EriURL: brainURL, Bots: loadBot("aerial", "DISCORD_TOKEN", "", brainURL, "", false)})},
		{name: "stale flux_receiver_url key is ignored", local: "port: \"4099\"\nflux_receiver_url: \"http://flux.example:8080/hook/\"\n", want: loadWant(Config{Port: "4099"})},
		{name: "zero-byte local file uses fallback", fallback: "port: \"4088\"\n", want: loadWant(Config{Port: "4088"})},
		{name: "empty config path uses fallback", fallback: "port: \"4088\"\n", env: map[string]string{"CONFIG_PATH": ""}, want: loadWant(Config{Port: "4088"})},
		{name: "local file wins over fallback", local: "port: \"4077\"\n", fallback: "port: \"4088\"\n", want: loadWant(Config{Port: "4077"})},
		{name: "invalid yaml keeps defaults", local: "invalid: [yaml", want: loadWant(Config{})},
		{name: "invalid fallback yaml keeps defaults", fallback: "port: [4088\n", env: map[string]string{"CONFIG_PATH": ""}, want: loadWant(Config{})},
		{name: "type-mismatched yaml keeps decoded fields", local: "port: \"4099\"\nbots: [\"not-a-map\"]\n", want: loadWant(Config{Port: "4099"})},
		// GitHub settings.
		{name: "minimal yaml keeps app credential, aerial bot and empty app ids", local: "port: \"4020\"\n", want: loadWant(Config{})},
		{name: "github_credential pat is trimmed and lowercased", local: "github_credential: \" PAT \"\n", want: loadWant(Config{GitHubCredential: "pat"})},
		{name: "github_credential app", local: "github_credential: app\n", want: loadWant(Config{})},
		{name: "invalid github_credential falls back to app", local: "github_credential: token\n", want: loadWant(Config{})},
		{name: "github app ids from yaml", local: "github_app_id: 1111\ngithub_app_installation_id: \" 2222 \"\n", want: loadWant(Config{GitHubAppID: "1111", GitHubAppInstallationID: "2222"})},
		{name: "non-integer github app id passes through", local: "github_app_id: not-a-number\n", want: loadWant(Config{GitHubAppID: "not-a-number"})},
		// System channel, authors and collaborator alerts (local and aerial config fallback).
		{name: "system_channel_id from yaml", local: "system_channel_id: \"112233445566778899\"\n", want: loadWant(Config{SystemChannelID: "112233445566778899", Bots: loadBot("aerial", "DISCORD_TOKEN", "", loadDefaultEriURL, "112233445566778899", false)})},
		{name: "system_channel from yaml", local: "system_channel: \"ops-dev\"\n", want: loadWant(Config{SystemChannelID: "ops-dev", Bots: loadBot("aerial", "DISCORD_TOKEN", "", loadDefaultEriURL, "ops-dev", false)})},
		{name: "system_channel from aerial config", local: "port: \"4020\"\n", aerial: "system_channel: \"ops-dev\"\n", want: loadWant(Config{SystemChannelID: "ops-dev", Bots: loadBot("aerial", "DISCORD_TOKEN", "", loadDefaultEriURL, "ops-dev", false)})},
		{name: "system_channel_id from aerial config", local: "port: \"4020\"\n", aerial: "system_channel_id: \"998877665544332211\"\n", want: loadWant(Config{SystemChannelID: "998877665544332211", Bots: loadBot("aerial", "DISCORD_TOKEN", "", loadDefaultEriURL, "998877665544332211", false)})},
		{name: "invalid aerial config is ignored", local: "port: \"4020\"\n", aerial: "system_channel: [x\n", want: loadWant(Config{})},
		{name: "auto_update_pr_authors from yaml", local: "auto_update_pr_authors:\n  - \"author-a\"\n  - \" author-b \"\n  - \"author-a\"\n", want: loadWant(Config{AutoUpdatePRAuthors: []string{"author-a", "author-b"}})},
		{name: "auto_update_pr_authors from aerial config", local: "port: \"4020\"\n", aerial: "auto_update_pr_authors:\n  - \"author-c\"\n", want: loadWant(Config{AutoUpdatePRAuthors: []string{"author-c"}})},
		{
			name:  "collaborator_ci_alerts from yaml qualified by github_owner",
			local: "github_owner: \" acme \"\ncollaborator_ci_alerts:\n  - repo: \"mirrormere\"\n    channel_id: \"123456789012345678\"\n    authors:\n      Collaborator-User: \"<@111222>\"\n",
			want:  loadWant(Config{GitHubOwner: "acme", CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "acme/mirrormere", ChannelID: "123456789012345678", Authors: map[string]string{"collaborator-user": "<@111222>"}}}}),
		},
		{
			name:   "collaborator_ci_alerts from aerial config",
			local:  "port: \"4020\"\n",
			aerial: "collaborator_ci_alerts:\n  - repo: \"acme/aerial\"\n    channel_id: \"987654321098765432\"\n    authors:\n      Collaborator: \"<@333444>\"\n",
			want:   loadWant(Config{CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "acme/aerial", ChannelID: "987654321098765432", Authors: map[string]string{"collaborator": "<@333444>"}}}}),
		},
		// Bot identity and default owner.
		{name: "bot_id and default_owner from yaml", local: "bot_id: \"eri-sub\"\ndefault_owner: true\n", want: loadWant(Config{BotID: "eri-sub", DefaultOwner: true, Bots: loadBot("eri-sub", "DISCORD_TOKEN_ERI_SUB", "", loadDefaultEriURL, "", true)})},
		{name: "bot_id from aerial config", local: "port: \"4020\"\n", aerial: "bot_id: \"aerial-fallback\"\n", want: loadWant(Config{BotID: "aerial-fallback", Bots: loadBot("aerial-fallback", "DISCORD_TOKEN_AERIAL_FALLBACK", "", loadDefaultEriURL, "", false)})},
		{name: "env overrides bot_id and default_owner", local: "bot_id: \"yaml-bot\"\ndefault_owner: false\n", env: map[string]string{"AERIAL_BOT_ID": "env-bot", "DEFAULT_OWNER": "true"}, want: loadWant(Config{BotID: "env-bot", DefaultOwner: true, Bots: loadBot("env-bot", "DISCORD_TOKEN_ENV_BOT", "", loadDefaultEriURL, "", true)})},
		{name: "AERIAL_DEFAULT_OWNER 1 wins over DEFAULT_OWNER", env: map[string]string{"AERIAL_DEFAULT_OWNER": "1", "DEFAULT_OWNER": "false"}, want: loadWant(Config{DefaultOwner: true, Bots: loadBot("aerial", "DISCORD_TOKEN", "", loadDefaultEriURL, "", true)})},
		// Multi-bot parsing.
		{
			name:  "bots map with custom token env and per-bot eri url",
			local: "bots:\n  aerial:\n    token_env: \"CUSTOM_DISCORD_TOKEN\"\n    eri_url: \"" + alphaEri + "/\"\n    system_channel_id: \"111222\"\n    default_owner: true\n  agent-alpha:\n    eri_url: \"" + betaEri + "\"\n",
			env:   map[string]string{"CUSTOM_DISCORD_TOKEN": "tok-aerial", "DISCORD_TOKEN_AGENT_ALPHA": "tok-alpha"},
			want: loadWant(Config{DiscordToken: "tok-aerial", Bots: map[string]BotConfig{
				"aerial":      {TokenEnv: "CUSTOM_DISCORD_TOKEN", DiscordToken: "tok-aerial", EriURL: alphaEri, SystemChannelID: "111222", DefaultOwner: true},
				"agent-alpha": {TokenEnv: "DISCORD_TOKEN_AGENT_ALPHA", DiscordToken: "tok-alpha", EriURL: betaEri},
			}}),
		},
		{
			name:  "bot synthesized from top-level keys when bots omitted",
			local: "bot_id: \"agent-beta\"\ndefault_owner: true\neri_url: \"" + betaEri + "\"\nsystem_channel_id: \"333444\"\n",
			env:   map[string]string{"DISCORD_TOKEN_AGENT_BETA": "tok-beta"},
			want:  loadWant(Config{BotID: "agent-beta", DefaultOwner: true, DiscordToken: "tok-beta", BrainURL: betaEri, EriURL: betaEri, SystemChannelID: "333444", Bots: loadBot("agent-beta", "DISCORD_TOKEN_AGENT_BETA", "tok-beta", betaEri, "333444", true)}),
		},
		{
			name:  "bots map with empty key and implicit aerial default owner",
			local: "bot_id: \"nonexistent\"\ndefault_owner: true\nlegacy_build_workflows:\n  - repository: \"bad\"\nbots:\n  \"\":\n    eri_url: \"http://ignore.example\"\n  aerial:\n    system_channel_id: \"chan1\"\n",
			env:   map[string]string{"DISCORD_TOKEN": "token-for-aerial"},
			want:  loadWant(Config{BotID: "nonexistent", DefaultOwner: true, DiscordToken: "token-for-aerial", Bots: loadBot("aerial", "DISCORD_TOKEN", "token-for-aerial", loadDefaultEriURL, "chan1", true)}),
		},
		{
			name:  "bots map with explicit bot_id gets implicit default owner",
			local: "bot_id: \"agent-alpha\"\ndefault_owner: true\nbots:\n  agent-alpha: {}\n  aerial: {}\n",
			env:   map[string]string{"DISCORD_TOKEN": "tok-aerial"},
			want: loadWant(Config{BotID: "agent-alpha", DefaultOwner: true, DiscordToken: "tok-aerial", Bots: map[string]BotConfig{
				"agent-alpha": {TokenEnv: "DISCORD_TOKEN_AGENT_ALPHA", EriURL: loadDefaultEriURL, DefaultOwner: true},
				"aerial":      {TokenEnv: "DISCORD_TOKEN", DiscordToken: "tok-aerial", EriURL: loadDefaultEriURL},
			}}),
		},
		// Kubernetes auth.
		{name: "K8S_AUTH_ENABLED 1 enables yaml k8s_auth", local: "default_owner: true\nk8s_auth:\n  api_server_url: \"https://k8s.example\"\n", env: map[string]string{"DISCORD_TOKEN": "tok", "K8S_AUTH_ENABLED": "1"}, want: loadWant(Config{DefaultOwner: true, DiscordToken: "tok", Bots: loadBot("aerial", "DISCORD_TOKEN", "tok", loadDefaultEriURL, "", true), K8sAuthEnabled: true, K8sAuthConfig: k8sauth.Config{Enabled: true, APIServerURL: "https://k8s.example"}})},
		{name: "invalid K8S_AUTH_ENABLED keeps yaml value", local: "k8s_auth_enabled: true\nk8s_auth:\n  api_server_url: \"https://k8s.example\"\n", env: map[string]string{"K8S_AUTH_ENABLED": "yes"}, want: loadWant(Config{K8sAuthEnabled: true, K8sAuthConfig: k8sauth.Config{Enabled: true, APIServerURL: "https://k8s.example"}})},
		{name: "K8S_AUTH_ENABLED false overrides yaml true", local: "k8s_auth_enabled: true\n", env: map[string]string{"K8S_AUTH_ENABLED": "false"}, want: loadWant(Config{})},
		{name: "AUTH_ENABLED env is not read", local: "port: \"4020\"\n", env: map[string]string{"AUTH_ENABLED": "true"}, want: loadWant(Config{})},
		// Restate ingress.
		{name: "restate url and services from yaml", local: "restate_url: \"" + restateURL + "/\"\nrestate_pr_service: \"PRObject\"\nrestate_repo_service: \"RepoObject\"\n", want: loadWant(Config{RestateURL: restateURL})},
		{name: "custom restate service names preserved", local: "restate_url: \"" + restateURL + "\"\nrestate_pr_service: \"PRObject-green\"\nrestate_repo_service: \"RepoObject-green\"\n", want: loadWant(Config{RestateURL: restateURL, RestatePRService: "PRObject-green", RestateRepoService: "RepoObject-green"})},
		{name: "blank restate service names use defaults", local: "restate_url: \"" + restateURL + "\"\nrestate_pr_service: \"   \"\nrestate_repo_service: \"\"\n", want: loadWant(Config{RestateURL: restateURL})},
		{name: "RESTATE_URL env never injects the endpoint", local: "port: \"4020\"\n", env: map[string]string{"RESTATE_URL": "http://evil.example:8080"}, want: loadWant(Config{})},
		{name: "non-default bot_id still targets the single services", local: "restate_url: \"" + restateURL + "\"\nbot_id: \"agent-alpha\"\n", want: loadWant(Config{RestateURL: restateURL, BotID: "agent-alpha", Bots: loadBot("agent-alpha", "DISCORD_TOKEN_AGENT_ALPHA", "", loadDefaultEriURL, "", false)})},
		{name: "explicit restate services win over a non-default bot_id", local: "restate_url: \"" + restateURL + "\"\nbot_id: \"agent-alpha\"\nrestate_pr_service: \"PRObject-green\"\nrestate_repo_service: \"RepoObject-green\"\n", want: loadWant(Config{RestateURL: restateURL, BotID: "agent-alpha", RestatePRService: "PRObject-green", RestateRepoService: "RepoObject-green", Bots: loadBot("agent-alpha", "DISCORD_TOKEN_AGENT_ALPHA", "", loadDefaultEriURL, "", false)})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loadConfigFiles(t, tc.local, tc.fallback, tc.aerial, tc.env)
			if got := LoadConfigFromEnv(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("LoadConfigFromEnv() =\n%+v\nwant\n%+v", got, tc.want)
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

var restartBase = Config{
	Port: "4020", DiscordToken: "tok", BotID: "aerial", BrainURL: "http://eri:8080", EriURL: "http://eri:8080",
	KubeAPIServer: "https://192.0.2.1:443", KubeToken: "kube", SystemChannelID: "111", AutoUpdatePRAuthors: []string{"a"},
	RestateURL: "http://restate.example:8080", RestatePRService: "PRObject", RestateRepoService: "RepoObject",
	Bots: map[string]BotConfig{"aerial": {DiscordToken: "tok", EriURL: "http://eri:8080", SystemChannelID: "111", DefaultOwner: true}},
}

// restartNext returns restartBase with overlay's non-zero fields applied.
func restartNext(overlay Config) *Config {
	next := withField(restartBase, overlay)
	return &next
}

func TestRestartRequired(t *testing.T) {
	t.Parallel()
	aerialBot := restartBase.Bots["aerial"]
	alerts := []CollaboratorCIAlertRule{{Repo: "acme/aerial", ChannelID: "111", Authors: map[string]string{"dev": "<@1>"}}}
	tests := []struct {
		name      string
		old, next *Config
		want      bool
	}{
		{"nil old config", nil, &restartBase, true},
		{"nil next config", &restartBase, nil, true},
		{"identical config", &restartBase, restartNext(Config{}), false},
		{"system channel changes live", &restartBase, restartNext(Config{SystemChannelID: "222"}), false},
		{"auto-update authors change live", &restartBase, restartNext(Config{AutoUpdatePRAuthors: []string{"b"}}), false},
		{"default owner changes live", &restartBase, restartNext(Config{DefaultOwner: true}), false},
		{"collaborator alerts change live", &restartBase, restartNext(Config{CollaboratorCIAlerts: alerts}), false},
		{"port requires restart", &restartBase, restartNext(Config{Port: "4021"}), true},
		{"discord token requires restart", &restartBase, restartNext(Config{DiscordToken: "tok2"}), true},
		{"bot id requires restart", &restartBase, restartNext(Config{BotID: "agent-gamma"}), true},
		{"eri url requires restart", &restartBase, restartNext(Config{BrainURL: "http://eri2:8080", EriURL: "http://eri2:8080"}), true},
		{"kube api server requires restart", &restartBase, restartNext(Config{KubeAPIServer: "https://192.0.2.2:443"}), true},
		{"kube token requires restart", &restartBase, restartNext(Config{KubeToken: "kube2"}), true},
		{"nats url requires restart", &restartBase, restartNext(Config{NATSURL: "nats://nats2:4222"}), true},
		{"restate url requires restart", &restartBase, restartNext(Config{RestateURL: "http://restate2.example:8080"}), true},
		{"restate pr service requires restart", &restartBase, restartNext(Config{RestatePRService: "PRObject-green"}), true},
		{"restate repo service requires restart", &restartBase, restartNext(Config{RestateRepoService: "RepoObject-green"}), true},
		{"github owner requires restart", &restartBase, restartNext(Config{GitHubOwner: "acme"}), true},
		{"github app id requires restart", &restartBase, restartNext(Config{GitHubAppID: "1111"}), true},
		{"github app installation id requires restart", &restartBase, restartNext(Config{GitHubAppInstallationID: "2222"}), true},
		{"k8s auth enabled requires restart", &restartBase, restartNext(Config{K8sAuthEnabled: true}), true},
		{"bots count mismatch requires restart", &restartBase, restartNext(Config{Bots: map[string]BotConfig{"aerial": aerialBot, "agent-alpha": {DefaultOwner: true}}}), true},
		{"bots value mismatch requires restart", &restartBase, restartNext(Config{Bots: map[string]BotConfig{"aerial": {DiscordToken: "different"}}}), true},
		{"bots key mismatch requires restart", &restartBase, restartNext(Config{Bots: map[string]BotConfig{"agent-alpha": aerialBot}}), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RestartRequired(tc.old, tc.next); got != tc.want {
				t.Errorf("RestartRequired() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLoadConfigFromEnvStrict(t *testing.T) {
	const restateLine = "restate_url: \"http://restate.example:8080\"\n"
	tests := []struct {
		name           string
		local          string
		fallback       string
		env            map[string]string
		wantPort       string
		wantURL        string
		wantErr        string
		wantRestateErr bool
	}{
		{name: "valid yaml loads like LoadConfig", local: "port: \"5050\"\n" + restateLine, wantPort: "5050", wantURL: "http://restate.example:8080"},
		{name: "valid fallback with empty config path", fallback: restateLine, env: map[string]string{"CONFIG_PATH": ""}, wantPort: "4020", wantURL: "http://restate.example:8080"},
		{name: "pat credential accepted", local: restateLine + "github_credential: \" PAT \"\n", wantPort: "4020", wantURL: "http://restate.example:8080"},
		{name: "missing restate url is rejected", local: "port: \"4020\"\n", wantErr: "restate_url is required", wantRestateErr: true},
		{name: "blank restate url is rejected", local: "restate_url: \"   \"\n", wantErr: "restate_url is required", wantRestateErr: true},
		{name: "RESTATE_URL env never injects the endpoint", local: "port: \"4020\"\n", env: map[string]string{"RESTATE_URL": "http://evil.example:8080"}, wantErr: "restate_url is required", wantRestateErr: true},
		{name: "zero-byte local file without fallback requires restate url", wantErr: "restate_url is required", wantRestateErr: true},
		{name: "invalid github_credential rejected", local: restateLine + "github_credential: token\n", wantErr: `github_credential "token" is not app or pat`},
		{name: "invalid yaml rejected", local: "invalid: [yaml", wantErr: "failed to parse yaml config at DIR/synapse.yaml: yaml: line 1: did not find expected ',' or ']'"},
		{name: "type-mismatched yaml rejected", local: restateLine + "bots: [\"not-a-map\"]\n", wantErr: "failed to parse yaml config at DIR/synapse.yaml: yaml: unmarshal errors:\n  line 2: cannot unmarshal !!seq into map[string]config.BotConfig"},
		{name: "invalid fallback yaml rejected", fallback: "port: [4088\n", env: map[string]string{"CONFIG_PATH": ""}, wantErr: "failed to parse yaml config at DIR/fallback.yaml: yaml: line 1: did not find expected ',' or ']'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := loadConfigFiles(t, tc.local, tc.fallback, "", tc.env)
			got, err := LoadConfigFromEnvStrict()
			gotErr := ""
			if err != nil {
				gotErr = strings.ReplaceAll(err.Error(), dir, "DIR")
			}
			if gotErr != tc.wantErr || errors.Is(err, ErrRestateURLRequired) != tc.wantRestateErr {
				t.Fatalf("LoadConfigFromEnvStrict() err = %q (restate sentinel %v), want %q (%v)", gotErr, errors.Is(err, ErrRestateURLRequired), tc.wantErr, tc.wantRestateErr)
			}
			var gotPort, gotURL string
			if got != nil {
				gotPort, gotURL = got.Port, got.RestateURL
				if want := LoadConfigFromEnv(); !reflect.DeepEqual(*got, want) {
					t.Errorf("LoadConfigFromEnvStrict() = %+v, want LoadConfigFromEnv result %+v", *got, want)
				}
			}
			if gotPort != tc.wantPort || gotURL != tc.wantURL {
				t.Errorf("Port, RestateURL = %q, %q, want %q, %q", gotPort, gotURL, tc.wantPort, tc.wantURL)
			}
		})
	}
}

func TestDefaultOwnerAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *Config
		want string
	}{
		{
			name: "nil config returns aerial",
			cfg:  nil,
			want: "aerial",
		},
		{
			name: "empty bots returns aerial",
			cfg:  &Config{Bots: nil},
			want: "aerial",
		},
		{
			name: "explicit default owner found",
			cfg: &Config{
				Bots: map[string]BotConfig{
					"aerial":      {DefaultOwner: false},
					"agent-alpha": {DefaultOwner: true},
				},
			},
			want: "agent-alpha",
		},
		{
			name: "no explicit default owner falls back to aerial",
			cfg: &Config{
				Bots: map[string]BotConfig{
					"aerial":      {DefaultOwner: false},
					"agent-alpha": {DefaultOwner: false},
				},
			},
			want: "aerial",
		},
		{
			name: "no explicit default owner without aerial falls back to first sorted",
			cfg: &Config{
				Bots: map[string]BotConfig{
					"agent-beta":  {DefaultOwner: false},
					"agent-alpha": {DefaultOwner: false},
				},
			},
			want: "agent-alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.DefaultOwnerAgent(); got != tc.want {
				t.Errorf("cfg.DefaultOwnerAgent() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeCollaboratorCIAlerts(t *testing.T) {
	t.Parallel()

	paddedAuthor := " Collaborator-User "

	tests := []struct {
		name  string
		owner string
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
			name:  "normalizes valid rules and lowercases author logins",
			owner: "acme",
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
					Repo:      "acme/mirrormere",
					ChannelID: "123456789012345678",
					Authors: map[string]string{
						"collaborator-user": "<@111222>",
					},
				},
			},
		},
		{
			name:  "qualified rule repo keeps its owner",
			owner: "acme",
			rules: []CollaboratorCIAlertRule{{Repo: "x/y", ChannelID: "123456789012345678", Authors: map[string]string{"user": "<@1>"}}},
			want:  []CollaboratorCIAlertRule{{Repo: "x/y", ChannelID: "123456789012345678", Authors: map[string]string{"user": "<@1>"}}},
		},
		{
			name:  "empty owner keeps bare rule repo",
			rules: []CollaboratorCIAlertRule{{Repo: "aerial", ChannelID: "123456789012345678", Authors: map[string]string{"user": "<@1>"}}},
			want:  []CollaboratorCIAlertRule{{Repo: "aerial", ChannelID: "123456789012345678", Authors: map[string]string{"user": "<@1>"}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeCollaboratorCIAlerts(tc.owner, tc.rules)
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
			Repo:      "acme/mirrormere",
			ChannelID: "123456789012345678",
			Authors: map[string]string{
				"collaborator-user": "<@111222>",
			},
		},
	}
	cfg := Config{GitHubOwner: "acme", CollaboratorCIAlerts: rules}

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

func TestNormalizeGitHubCredential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		raw    string
		want   string
		wantOK bool
	}{
		{name: "empty selects app", raw: "", want: "app", wantOK: true},
		{name: "whitespace selects app", raw: "   ", want: "app", wantOK: true},
		{name: "app", raw: "app", want: "app", wantOK: true},
		{name: "upper pat", raw: " PAT ", want: "pat", wantOK: true},
		{name: "unknown value", raw: "Token", want: "token", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := NormalizeGitHubCredential(tc.raw)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("NormalizeGitHubCredential(%q) = (%q, %v), want (%q, %v)", tc.raw, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
