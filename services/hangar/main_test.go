package main

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

func TestSanitizeAndEnv_TableDriven(t *testing.T) {
	t.Run("SanitizeLog", func(t *testing.T) {
		tests := []struct {
			name, input, expected string
		}{
			{"clean string", "repository synced cleanly", "repository synced cleanly"},
			{"github pat", "failed with token github_pat_11AAAA_secret_token_123", "failed with token [REDACTED_TOKEN]"},
			{"classic ghp token", "cloning with ghp_1234567890abcdef1234567890abcdef12", "cloning with [REDACTED_TOKEN]"},
			{"basic auth header", "AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46c2VjcmV0", "AUTHORIZATION: [REDACTED_TOKEN]"},
			{"postgres connection string", "connecting to postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable", "connecting to postgres://aerial:[REDACTED_TOKEN]@postgres:5432/aerial?sslmode=disable"},
			{"anthropic api key", "using sk-ant-api03-1234567890abcdef-secret", "using [REDACTED_TOKEN]"},
			{"google api key", "key AIzaSyD1234567890abcdefghijklmnopqrstuv", "key [REDACTED_TOKEN]"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := SanitizeLog(tt.input); got != tt.expected {
					t.Errorf("SanitizeLog() = %q, want %q", got, tt.expected)
				}
			})
		}
	})
}

func TestReconcilerAndPeriodicLoops_TableDriven(t *testing.T) {
	t.Run("StartReconcilerLoop_nil_channel_init", func(t *testing.T) {
		ctx := t.Context()
		d := &SyncDaemon{}
		d.StartReconcilerLoop(ctx)
		if d.reconcileCh == nil {
			t.Errorf("expected reconcileCh to be initialized")
		}
	})

	t.Run("StartReconcilerLoop_debounce_and_execution", func(t *testing.T) {
		oldDebounce, oldCooldown := reconcilerDebounceDuration, reconcilerMinCooldown
		reconcilerDebounceDuration, reconcilerMinCooldown = 5*time.Millisecond, 5*time.Millisecond
		defer func() {
			reconcilerDebounceDuration, reconcilerMinCooldown = oldDebounce, oldCooldown
		}()

		tempDir := t.TempDir()
		nomadExecuted := make(chan struct{}, 10)
		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ConfigDir:  tempDir,
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				select {
				case nomadExecuted <- struct{}{}:
				default:
				}
				return nil, nil, errors.New("nomad fail")
			},
		})
		d.recordPendingNomadChanges(tempDir, []NomadFileChange{
			{JobName: "deljob", Action: "delete"},
			{JobName: "appjob", Action: "apply", Path: "jobs/app.nomad"},
		})

		loopCtx, loopCancel := context.WithCancel(context.Background())
		d.StartReconcilerLoop(loopCtx)

		_ = d.TriggerReconcile(context.Background(), true)
		select {
		case <-nomadExecuted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for nomad executor signal on first trigger")
		}

		d.recordPendingNomadChanges(tempDir, []NomadFileChange{
			{JobName: "deljob2", Action: "delete"},
		})
		d.SetLastReconcile(time.Now().Add(-reconcilerMinCooldown))
		_ = d.TriggerReconcile(context.Background(), true)
		select {
		case <-nomadExecuted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for nomad executor signal on second trigger")
		}
		loopCancel()
	})

	t.Run("StartPeriodicLoop_execution_and_shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		triggered := make(chan struct{}, 1)
		d := NewDaemon(DaemonConfig{Interval: 5 * time.Millisecond, ComposeDir: t.TempDir()})
		d.triggerFn = func() ([]RepoSyncResult, error) {
			select {
			case triggered <- struct{}{}:
			default:
			}
			return nil, errors.New("simulated periodic sync notice")
		}

		d.StartPeriodicLoop(ctx)
		select {
		case <-triggered:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for periodic loop execution")
		}
		cancel()
	})

	t.Run("DaemonLoops_graceful_cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		d := NewDaemon(DaemonConfig{Interval: 10 * time.Millisecond})
		d.StartPeriodicLoop(ctx)
		d.StartReconcilerLoop(ctx)
		cancel()
	})

	t.Run("StartReconcilerLoop_cancel_stops_debounce_timer", func(t *testing.T) {
		oldDebounce, oldCooldown := reconcilerDebounceDuration, reconcilerMinCooldown
		reconcilerDebounceDuration, reconcilerMinCooldown = 50*time.Millisecond, 50*time.Millisecond
		defer func() {
			reconcilerDebounceDuration, reconcilerMinCooldown = oldDebounce, oldCooldown
		}()

		tempDir := t.TempDir()
		nomadExecuted := make(chan struct{}, 1)
		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ConfigDir:  tempDir,
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				nomadExecuted <- struct{}{}
				return nil, nil, nil
			},
		})
		d.recordPendingNomadChanges(tempDir, []NomadFileChange{
			{JobName: "deljob", Action: "delete"},
		})

		loopCtx, loopCancel := context.WithCancel(context.Background())
		d.StartReconcilerLoop(loopCtx)

		_ = d.TriggerReconcile(context.Background(), true)
		loopCancel()

		select {
		case <-nomadExecuted:
			t.Fatal("expected debounce timer to be stopped on ctx.Done, but executor ran")
		case <-time.After(100 * time.Millisecond):
		}
	})
}

func TestNewConfigFromLookup_TableDriven(t *testing.T) {
	tmpConfigFile := filepath.Join(t.TempDir(), "test_hangar.yaml")
	_ = os.WriteFile(tmpConfigFile, []byte("port: \"9999\"\n"), 0o644)
	t.Setenv("CONFIG_PATH", tmpConfigFile)
	if cfg := NewConfigFromEnv(); cfg.Port != "9999" {
		t.Errorf("expected port 9999 from NewConfigFromEnv() with CONFIG_PATH, got %s", cfg.Port)
	}

	emptyConfigFile := filepath.Join(t.TempDir(), "empty.yaml")
	_ = os.WriteFile(emptyConfigFile, []byte("   \n"), 0o644)
	t.Setenv("CONFIG_PATH", emptyConfigFile)
	if cfg := NewConfigFromEnv(); cfg.Port == "" {
		t.Errorf("expected non-empty port from NewConfigFromEnv() with empty CONFIG_PATH file")
	}

	t.Setenv("CONFIG_PATH", filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if cfg := NewConfigFromEnv(); cfg.Port == "" {
		t.Errorf("expected non-empty port from NewConfigFromEnv() with nonexistent CONFIG_PATH")
	}

	t.Setenv("CONFIG_PATH", "")
	if cfg := NewConfigFromEnv(); cfg.Port == "" {
		t.Errorf("expected non-empty port from NewConfigFromEnv() with empty CONFIG_PATH")
	}
	if cfg := NewConfigFromLookup(nil); cfg.Port != "8087" {
		t.Errorf("NewConfigFromLookup(nil) Port = %q, want 8087", cfg.Port)
	}

	structuredYAML := `port: "9090"
sync_interval: "30s"
nomad_addr: "http://nomad.test:4646"
synapse_url: "http://synapse.test:4020"
repos:
  - path: "/custom/aerial-config"
    url: "https://github.com/custom/aerial-config.git"
  - path: "/custom/aerial"
    url: "https://github.com/custom/aerial.git"
  - path: "/custom/mirrormere"
    url: "https://github.com/custom/mirrormere.git"
service_configs:
  - rel_path: "services/voice/voice.yaml"
    nomad_var: "nomad/jobs/voice"
    var_key: "CONFIG_YAML"
asset_bundles:
  - name: "custom-bundle"
    nomad_var: "nomad/jobs/custom"
    sources:
      app: "/custom/app"`

	reposYAML := `port: "8087"
sync_interval: "45s"
repos:
  - path: "/custom/aerial-config"
  - path: "/custom/aerial"
nomad_addr: "http://nomad.cluster:4646"
synapse_url: "http://127.0.0.1:4020"`

	tests := []struct {
		name                     string
		yamlContent              string
		env                      map[string]string
		wantPort                 string
		wantRepos                []string
		wantRepoURLs             map[string]string
		wantInterval             time.Duration
		wantPAT                  string
		wantNomadToken           string
		wantComposeDir           string
		wantConfigDir            string
		wantDiscordToken         string
		wantDiscordChannel       string
		wantDiscordWebhook       string
		wantNomadAddr            string
		wantSynapseURL           string
		wantExtraSecretsLen      int
		checkExtraSecrets        bool
		wantServiceConfigRelPath string
		wantAssetBundleName      string
	}{
		{
			name:        "structured repos and daemon endpoints in YAML",
			yamlContent: structuredYAML,
			wantPort:    "9090",
			wantRepos:   []string{"/custom/aerial-config", "/custom/aerial", "/custom/mirrormere"},
			wantRepoURLs: map[string]string{
				"/custom/aerial-config": "https://github.com/custom/aerial-config.git",
				"/custom/aerial":        "https://github.com/custom/aerial.git",
				"/custom/mirrormere":    "https://github.com/custom/mirrormere.git",
			},
			wantInterval:             30 * time.Second,
			wantComposeDir:           "/custom/aerial",
			wantConfigDir:            "/custom/aerial-config",
			wantNomadAddr:            "http://nomad.test:4646",
			wantSynapseURL:           "http://synapse.test:4020",
			wantServiceConfigRelPath: "services/voice/voice.yaml",
			wantAssetBundleName:      "custom-bundle",
		},
		{
			name: "secret env lookups with GITHUB_PAT, NOMAD_TOKEN, DISCORD tokens, and extraSecrets",
			env: map[string]string{
				"GITHUB_PAT":             "my_secret_pat",
				"NOMAD_TOKEN":            "my_nomad_tok",
				"DISCORD_TOKEN":          "bot_token",
				"SYSTEM_CHANNEL_ID":      "custom-channel",
				"DISCORD_WEBHOOK_URL":    "https://discord.com/api/webhooks/123/abc",
				"POSTGRES_PASSWORD":      "pg_pass",
				"HA_TOKEN":               "ha_tok",
				"GEMINI_HARNESS_API_KEY": "harness_key",
			},
			wantPort:            "8087",
			wantInterval:        60 * time.Second,
			wantComposeDir:      "/share/aerial",
			wantConfigDir:       "/share/aerial-config",
			wantPAT:             "my_secret_pat",
			wantNomadToken:      "my_nomad_tok",
			wantDiscordToken:    "bot_token",
			wantDiscordChannel:  "custom-channel",
			wantDiscordWebhook:  "https://discord.com/api/webhooks/123/abc",
			wantExtraSecretsLen: 5,
			checkExtraSecrets:   true,
		},
		{
			name:           "repos without url in yaml",
			yamlContent:    reposYAML,
			wantPort:       "8087",
			wantInterval:   45 * time.Second,
			wantRepos:      []string{"/custom/aerial-config", "/custom/aerial"},
			wantComposeDir: "/custom/aerial",
			wantConfigDir:  "/custom/aerial-config",
			wantNomadAddr:  "http://nomad.cluster:4646",
			wantSynapseURL: "http://127.0.0.1:4020",
		},
		{
			name:                "empty lookup defaults",
			env:                 map[string]string{},
			wantPort:            "8087",
			wantInterval:        60 * time.Second,
			wantComposeDir:      "/share/aerial",
			wantConfigDir:       "/share/aerial-config",
			wantNomadAddr:       "http://host.docker.internal:4646",
			wantSynapseURL:      "http://127.0.0.1:4020",
			wantExtraSecretsLen: 0,
			checkExtraSecrets:   true,
		},
		{
			name:           "invalid YAML warning fallback to defaults",
			yamlContent:    "invalid: [unclosed yaml",
			wantPort:       "8087",
			wantInterval:   60 * time.Second,
			wantComposeDir: "/share/aerial",
			wantConfigDir:  "/share/aerial-config",
			wantNomadAddr:  "http://host.docker.internal:4646",
			wantSynapseURL: "http://127.0.0.1:4020",
		},
		{
			name:        "non-secret env vars are ignored in favor of YAML",
			yamlContent: structuredYAML,
			env: map[string]string{
				"PORT":                "9999",
				"SYNC_INTERVAL":       "10s",
				"SYNC_REPOS":          "/ignored/repo",
				"NOMAD_ADDR":          "http://ignored.nomad:4646",
				"WEBHOOKS_ROUTER_URL": "http://ignored.router:4020",
			},
			wantPort:       "9090",
			wantInterval:   30 * time.Second,
			wantRepos:      []string{"/custom/aerial-config", "/custom/aerial", "/custom/mirrormere"},
			wantComposeDir: "/custom/aerial",
			wantConfigDir:  "/custom/aerial-config",
			wantNomadAddr:  "http://nomad.test:4646",
			wantSynapseURL: "http://synapse.test:4020",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envMap := make(map[string]string, len(tt.env)+1)
			maps.Copy(envMap, tt.env)
			if tt.yamlContent != "" {
				cfgPath := filepath.Join(t.TempDir(), "hangar.yaml")
				if err := os.WriteFile(cfgPath, []byte(tt.yamlContent), 0o644); err != nil {
					t.Fatalf("failed to write test yaml: %v", err)
				}
				envMap["CONFIG_PATH"] = cfgPath
			}
			cfg := NewConfigFromLookup(func(k string) string { return envMap[k] })

			if tt.wantPort != "" && cfg.Port != tt.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tt.wantPort)
			}
			if len(tt.wantRepos) > 0 {
				if len(cfg.Repos) != len(tt.wantRepos) {
					t.Fatalf("Repos = %v, want %v", cfg.Repos, tt.wantRepos)
				}
				for i := range tt.wantRepos {
					if cfg.Repos[i] != tt.wantRepos[i] {
						t.Errorf("Repos[%d] = %q, want %q", i, cfg.Repos[i], tt.wantRepos[i])
					}
				}
			}
			if len(tt.wantRepoURLs) > 0 {
				for k, wantURL := range tt.wantRepoURLs {
					if gotURL := cfg.RepoURLs[k]; gotURL != wantURL {
						t.Errorf("RepoURLs[%q] = %q, want %q", k, gotURL, wantURL)
					}
				}
			}
			if tt.wantInterval != 0 && cfg.Interval != tt.wantInterval {
				t.Errorf("Interval = %v, want %v", cfg.Interval, tt.wantInterval)
			}
			if tt.wantPAT != "" && cfg.PAT != tt.wantPAT {
				t.Errorf("PAT = %q, want %q", cfg.PAT, tt.wantPAT)
			}
			if tt.wantNomadToken != "" && cfg.NomadToken != tt.wantNomadToken {
				t.Errorf("NomadToken = %q, want %q", cfg.NomadToken, tt.wantNomadToken)
			}
			if tt.wantComposeDir != "" && cfg.ComposeDir != tt.wantComposeDir {
				t.Errorf("ComposeDir = %q, want %q", cfg.ComposeDir, tt.wantComposeDir)
			}
			if tt.wantConfigDir != "" && cfg.ConfigDir != tt.wantConfigDir {
				t.Errorf("ConfigDir = %q, want %q", cfg.ConfigDir, tt.wantConfigDir)
			}
			if tt.wantDiscordToken != "" && cfg.DiscordToken != tt.wantDiscordToken {
				t.Errorf("DiscordToken = %q, want %q", cfg.DiscordToken, tt.wantDiscordToken)
			}
			if tt.wantDiscordChannel != "" && cfg.DiscordChannel != tt.wantDiscordChannel {
				t.Errorf("DiscordChannel = %q, want %q", cfg.DiscordChannel, tt.wantDiscordChannel)
			}
			if tt.wantDiscordWebhook != "" && cfg.DiscordWebhookURL != tt.wantDiscordWebhook {
				t.Errorf("DiscordWebhookURL = %q, want %q", cfg.DiscordWebhookURL, tt.wantDiscordWebhook)
			}
			if tt.wantNomadAddr != "" && cfg.NomadAddr != tt.wantNomadAddr {
				t.Errorf("NomadAddr = %q, want %q", cfg.NomadAddr, tt.wantNomadAddr)
			}
			if tt.wantSynapseURL != "" && cfg.SynapseURL != tt.wantSynapseURL {
				t.Errorf("SynapseURL = %q, want %q", cfg.SynapseURL, tt.wantSynapseURL)
			}
			if tt.checkExtraSecrets && len(cfg.ExtraSecrets) != tt.wantExtraSecretsLen {
				t.Errorf("len(ExtraSecrets) = %d, want %d (%v)", len(cfg.ExtraSecrets), tt.wantExtraSecretsLen, cfg.ExtraSecrets)
			}
			if tt.wantServiceConfigRelPath != "" {
				if len(cfg.ServiceConfigs) != 1 || cfg.ServiceConfigs[0].RelPath != tt.wantServiceConfigRelPath {
					t.Errorf("ServiceConfigs = %v, want RelPath %q", cfg.ServiceConfigs, tt.wantServiceConfigRelPath)
				}
			}
			if tt.wantAssetBundleName != "" {
				if len(cfg.AssetBundles) != 1 || cfg.AssetBundles[0].Name != tt.wantAssetBundleName {
					t.Errorf("AssetBundles = %v, want Name %q", cfg.AssetBundles, tt.wantAssetBundleName)
				}
			}
		})
	}
}

func TestSetupMuxAndDaemonLifecycle_TableDriven(t *testing.T) {
	t.Run("NewDaemon interval configuration", func(t *testing.T) {
		for _, tc := range []struct {
			name           string
			interval, want time.Duration
		}{
			{"custom interval", 50 * time.Millisecond, 50 * time.Millisecond},
			{"default fallback", 0, 60 * time.Second},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if d := NewDaemon(DaemonConfig{Interval: tc.interval}); d.interval != tc.want {
					t.Errorf("expected interval %v, got %v", tc.want, d.interval)
				}
			})
		}
	})

	t.Run("SetupMux endpoints and HTTP methods", func(t *testing.T) {
		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0o755)
		_ = os.MkdirAll(filepath.Join(tmpDir, ".git"), 0o755)
		_ = os.WriteFile(filepath.Join(jobsDir, "test.nomad"), []byte(`job "test" { task "t" { config { image = "ghcr.io/azylman/aerial:latest" } } }`), 0o644)

		tests := []struct {
			name, method, path, body, contentType, reconcileMode string
			syncErr                                              bool
			wantCode                                             int
		}{
			{name: "GET /health 200", method: http.MethodGet, path: "/health", wantCode: http.StatusOK},
			{name: "GET /metrics 200", method: http.MethodGet, path: "/metrics", wantCode: http.StatusOK},
			{name: "GET /status 200", method: http.MethodGet, path: "/status", wantCode: http.StatusOK},
			{name: "POST /status 405", method: http.MethodPost, path: "/status", wantCode: http.StatusMethodNotAllowed},
			{name: "GET /sync 405", method: http.MethodGet, path: "/sync", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /sync 200", method: http.MethodPost, path: "/sync", wantCode: http.StatusOK},
			{name: "POST /sync 500", method: http.MethodPost, path: "/sync", syncErr: true, wantCode: http.StatusInternalServerError},
			{name: "GET /reconcile 405", method: http.MethodGet, path: "/reconcile", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /reconcile 202 async", method: http.MethodPost, path: "/reconcile", wantCode: http.StatusAccepted},
			{name: "POST /reconcile?sync=true 200", method: http.MethodPost, path: "/reconcile?sync=true", reconcileMode: "ok", wantCode: http.StatusOK},
			{name: "POST /reconcile?sync=true 500", method: http.MethodPost, path: "/reconcile?sync=true", reconcileMode: "err", wantCode: http.StatusInternalServerError},
			{name: "POST /reconcile JSON sync=true 200", method: http.MethodPost, path: "/reconcile", body: `{"sync":true}`, contentType: "application/json", reconcileMode: "ok", wantCode: http.StatusOK},
			{name: "POST /reconcile JSON sync=false 202", method: http.MethodPost, path: "/reconcile", body: `{"sync":false}`, contentType: "application/json", wantCode: http.StatusAccepted},
			{name: "POST /events/git_push 200", method: http.MethodPost, path: "/events/git_push", body: `{"repo":"` + tmpDir + `","ref":"refs/heads/main"}`, contentType: "application/json", wantCode: http.StatusOK},
			{name: "GET /events/git_push 405", method: http.MethodGet, path: "/events/git_push", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /events/git_push bad JSON 400", method: http.MethodPost, path: "/events/git_push", body: `bad`, wantCode: http.StatusBadRequest},
			{name: "POST /events/image_ready 202", method: http.MethodPost, path: "/events/image_ready", body: `{"image":"ghcr.io/azylman/aerial:latest"}`, contentType: "application/json", wantCode: http.StatusAccepted},
			{name: "GET /events/image_ready 405", method: http.MethodGet, path: "/events/image_ready", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /events/image_ready bad JSON 400", method: http.MethodPost, path: "/events/image_ready", body: `{bad`, wantCode: http.StatusBadRequest},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := NewDaemon(DaemonConfig{
					Repos:     []string{tmpDir},
					ConfigDir: tmpDir,
					GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						return []byte("abc1234"), nil, nil
					},
					NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
						return []byte("OK"), nil, nil
					},
				})
				if tc.syncErr {
					d.triggerFn = func() ([]RepoSyncResult, error) { return nil, errors.New("fail") }
				}
				switch tc.reconcileMode {
				case "ok":
					d.reconcileFn = func(ctx context.Context) error { return nil }
				case "err":
					d.reconcileFn = func(ctx context.Context) error { return errors.New("fail") }
				}
				var body io.Reader
				if tc.body != "" {
					body = strings.NewReader(tc.body)
				}
				req := httptest.NewRequest(tc.method, tc.path, body)
				if tc.contentType != "" {
					req.Header.Set("Content-Type", tc.contentType)
				}
				rec := httptest.NewRecorder()
				SetupMux(d).ServeHTTP(rec, req)
				if rec.Code != tc.wantCode {
					t.Errorf("%s %s returned %d; want %d", tc.method, tc.path, rec.Code, tc.wantCode)
				}
			})
		}
	})

	t.Run("RunDaemon lifecycle", func(t *testing.T) {
		for _, tc := range []struct {
			name, port, pat   string
			withRepo, wantErr bool
		}{
			{name: "invalid port string fatal error", port: "invalid-port-string", wantErr: true},
			{name: "invalid negative port fatal error", port: "-1", wantErr: true},
			{name: "normal lifecycle with PAT and deterministic sync signaling", port: "0", pat: "ghp_dummy_token_1234567890", withRepo: true, wantErr: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cfg := DaemonConfig{Port: tc.port, PAT: tc.pat}
				if tc.withRepo {
					t.Setenv("DOCKER_CONFIG", t.TempDir())
					repoDir := filepath.Join(t.TempDir(), "repo")
					_ = os.MkdirAll(filepath.Join(repoDir, ".git"), 0o755)
					started := make(chan struct{}, 1)
					cfg.Interval = time.Hour
					cfg.Repos = []string{repoDir}
					cfg.ComposeDir = t.TempDir()
					cfg.ConfigDir = t.TempDir()
					cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						select {
						case started <- struct{}{}:
						default:
						}
						return []byte("abc1234"), nil, nil
					}
					go func() {
						select {
						case <-started:
						case <-time.After(2 * time.Second):
						}
						cancel()
					}()
				}
				err := RunDaemon(ctx, cfg)
				if (err != nil) != tc.wantErr {
					t.Errorf("RunDaemon() error = %v, wantErr = %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("coverage_flush_endpoint", func(t *testing.T) {
		mux := SetupMux(&SyncDaemon{})

		oldFn, oldMetaFn := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
		defer func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldFn, oldMetaFn }()

		called, metaCalled := false, false
		httpkit.WriteCountersDirFn = func(dir string) error {
			called = true
			return nil
		}
		httpkit.WriteMetaDirFn = func(dir string) error {
			metaCalled = true
			return nil
		}

		rec2 := httptest.NewRecorder()
		mux.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec2.Code != http.StatusOK || !called || !metaCalled {
			t.Errorf("expected 200 OK and both flush fns called, got status=%d called=%v metaCalled=%v", rec2.Code, called, metaCalled)
		}

		httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
		rec2b := httptest.NewRecorder()
		mux.ServeHTTP(rec2b, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec2b.Code != http.StatusOK {
			t.Errorf("expected 200 OK when meta flush errors, got %d", rec2b.Code)
		}

		httpkit.WriteCountersDirFn = func(dir string) error { return errors.New("simulated flush error") }
		rec3 := httptest.NewRecorder()
		mux.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec3.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on error, got: %d", rec3.Code)
		}
	})
}

func TestComposeGraph_NoGitsyncDependents(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.yml")
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", composePath, err)
	}
	inDependsOn, currentService := false, ""
	for lineNum, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && strings.HasSuffix(trimmed, ":") {
			currentService, inDependsOn = strings.TrimSuffix(trimmed, ":"), false
		}
		if strings.HasPrefix(line, "    depends_on:") {
			inDependsOn = true
			continue
		}
		if inDependsOn {
			switch {
			case len(line) > 0 && line[0] != ' ':
				inDependsOn, currentService = false, ""
			case strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") && !strings.HasPrefix(line, "    depends_on:") && strings.HasSuffix(trimmed, ":"):
				inDependsOn = false
			case strings.Contains(line, "gitsync:") || trimmed == "- gitsync":
				t.Fatalf("line %d: service %q declares dependency on gitsync (%s); gitsync must have in-degree 0 in Compose DAG", lineNum+1, currentService, trimmed)
			}
		}
	}
}

type roundTripperFunc struct {
	fn func(req *http.Request) (*http.Response, error)
}

func (r *roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.fn(req)
}

func ioNopCloser(r io.Reader) io.ReadCloser {
	return io.NopCloser(r)
}

func TestExecutorsAndDispatchers_TableDriven(t *testing.T) {
	t.Run("runSubprocess CLI mocking", func(t *testing.T) {
		tempDir := t.TempDir()
		binDir := filepath.Join(tempDir, "bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			t.Fatalf("failed to create bin dir: %v", err)
		}
		mockBin := filepath.Join(binDir, "mockbin")
		if runtime.GOOS == "windows" {
			mockBin = filepath.Join(binDir, "mockbin.cmd")
			script := "@echo off\r\nset \"arg=%*\"\r\necho %arg% | findstr /i \"waitloop\" >nul && goto do_wait\r\necho %arg% | findstr /i \"version\" >nul && (echo Mock CLI v2.0 & exit /b 0)\r\necho unknown cmd >&2\r\nexit /b 1\r\n:do_wait\r\ngoto do_wait\r\n"
			_ = os.WriteFile(mockBin, []byte(script), 0o755)
		} else {
			script := "#!/bin/sh\ncase \"$*\" in\n  *\"waitloop\"*) trap 'exit 0' TERM INT; while :; do :; done;;\n  *\"version\"*) echo \"Mock CLI v2.0\"; exit 0;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
			_ = os.WriteFile(mockBin, []byte(script), 0o755)
		}

		for _, tc := range []struct {
			name, arg, wantOut string
			cancelCtx, wantErr bool
		}{
			{name: "subprocess success", arg: "version", wantOut: "Mock CLI v2.0"},
			{name: "subprocess cancel", arg: "waitloop", cancelCtx: true, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
					defer cancel()
				}
				stdout, stderr, err := runSubprocess(ctx, mockBin, tempDir, os.Environ(), 10*time.Second, tc.arg)
				if (err != nil) != tc.wantErr {
					t.Errorf("error = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantOut != "" && !strings.Contains(string(stdout), tc.wantOut) && !strings.Contains(string(stderr), tc.wantOut) {
					t.Errorf("expected output to contain %q, stdout: %s, stderr: %s", tc.wantOut, stdout, stderr)
				}
			})
		}
	})

	t.Run("daemon executor resolution branches", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		if nilDaemon.getGitExecutor() == nil || (&SyncDaemon{}).getGitExecutor() == nil {
			t.Errorf("expected non-nil default git executor")
		}
		customGit := false
		dCustomGit := &SyncDaemon{gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			customGit = true
			return nil, nil, nil
		}}
		_, _, _ = dCustomGit.getGitExecutor()(context.Background(), "")
		if !customGit {
			t.Errorf("expected custom git executor to be called")
		}
		if defaultExec := (&SyncDaemon{pat: "pat"}).getGitExecutor(); defaultExec != nil {
			_, _, _ = defaultExec(context.Background(), t.TempDir(), "version")
		}

		if fnNil := nilDaemon.getDeployDispatcher(); fnNil == nil || fnNil(context.Background(), HangarDeployEvent{}) == nil {
			t.Errorf("expected error from deploy dispatcher on nil daemon")
		}
		if fn := (&SyncDaemon{}).getDeployDispatcher(); fn == nil || fn(context.Background(), HangarDeployEvent{}) == nil {
			t.Errorf("expected error from deploy dispatcher when deployDispatcher is nil")
		}
		customDisp := false
		dCustomDisp := &SyncDaemon{deployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			customDisp = true
			return nil
		}}
		if err := dCustomDisp.getDeployDispatcher()(context.Background(), HangarDeployEvent{}); err != nil || !customDisp {
			t.Errorf("expected custom deploy dispatcher to be called: %v", err)
		}
	})

	t.Run("runGitCommand variations", func(t *testing.T) {
		for _, tc := range []struct {
			name, pat, arg        string
			useTempDir, cancelCtx bool
		}{
			{name: "normal version", arg: "version"},
			{name: "with dir init", useTempDir: true, arg: "init"},
			{name: "with pat version", pat: "mock-pat", arg: "version"},
			{name: "canceled context", cancelCtx: true, arg: "status"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				dir := ""
				if tc.useTempDir {
					dir = t.TempDir()
				}
				_, _, _ = runGitCommand(ctx, dir, tc.pat, tc.arg)
			})
		}
	})

	t.Run("defaultNomadExecutor variations", func(t *testing.T) {
		if nomadExec := defaultNomadExecutor("http://127.0.0.1:4646", "secret-token"); nomadExec != nil {
			_, _, _ = nomadExec(context.Background(), "version")
			cancelCtx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _, _ = nomadExec(cancelCtx, "version")
		}
	})

	t.Run("defaultDeployDispatcher HTTP handling", func(t *testing.T) {
		for _, tc := range []struct {
			name                string
			invalidURL, wantErr bool
			status              int
		}{
			{name: "invalid router URL", invalidURL: true, wantErr: true},
			{name: "HTTP 500 server error response", status: http.StatusInternalServerError, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				routerURL := "://invalid-router-url"
				if !tc.invalidURL {
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(tc.status)
					}))
					defer srv.Close()
					routerURL = srv.URL
				}
				d := &SyncDaemon{synapseURL: routerURL}
				err := d.defaultDeployDispatcher(context.Background(), HangarDeployEvent{JobName: "test"})
				if (err != nil) != tc.wantErr {
					t.Errorf("defaultDeployDispatcher() error = %v, wantErr = %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("DispatchDeployEvent safeguards and synchronization", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		nilDaemon.DispatchDeployEvent(HangarDeployEvent{})

		panicked := make(chan struct{}, 1)
		dPanic := NewDaemon(DaemonConfig{
			DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
				defer func() { panicked <- struct{}{} }()
				panic("simulated panic in dispatcher")
			},
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				panic("simulated panic in monitor")
			},
		})
		dPanic.DispatchDeployEvent(HangarDeployEvent{JobName: "test-panic"})

		select {
		case <-panicked:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for deploy dispatcher panic recovery")
		}
	})
}

type mockErrReadCloser struct{}

func (e *mockErrReadCloser) Read(p []byte) (int, error) { return 0, io.EOF }
func (e *mockErrReadCloser) Close() error               { return errors.New("simulated close error") }

type mockErrResponseWriter struct {
	header http.Header
	err    error
}

func (m *mockErrResponseWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}
func (m *mockErrResponseWriter) Write(p []byte) (int, error) { return 0, m.err }
func (m *mockErrResponseWriter) WriteHeader(statusCode int)  {}

func TestHangarHelpers_TableDriven(t *testing.T) {
	t.Run("closeWarn", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			closer io.Closer
		}{
			{"nil closer", nil},
			{"valid NopCloser", io.NopCloser(strings.NewReader(""))},
			{"mock error closer", &mockErrReadCloser{}},
		} {
			t.Run(tc.name, func(t *testing.T) { httpkit.CloseWarn(tc.closer, tc.name) })
		}
	})

	t.Run("isClientDisconnect", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, tc := range []struct {
			name string
			req  *http.Request
			err  error
			want bool
		}{
			{name: "nil error", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: nil, want: false},
			{name: "canceled request context", req: httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(canceledCtx), err: errors.New("err"), want: true},
			{name: "syscall EPIPE", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: syscall.EPIPE, want: true},
			{name: "syscall ECONNRESET", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: syscall.ECONNRESET, want: true},
			{name: "net ErrClosed", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: net.ErrClosed, want: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := httpkit.IsClientDisconnect(tc.req, tc.err); got != tc.want {
					t.Errorf("isClientDisconnect() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("writeResponse", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		for _, tc := range []struct {
			name string
			w    http.ResponseWriter
			body []byte
			want string
		}{
			{name: "standard ResponseRecorder", w: httptest.NewRecorder(), body: []byte("ok"), want: "ok"},
			{name: "mock error response writer", w: &mockErrResponseWriter{err: errors.New("fail")}, body: []byte("fail")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				httpkit.WriteResponse(tc.w, req, "hangar", tc.body)
				if rec, ok := tc.w.(*httptest.ResponseRecorder); ok && tc.want != "" && rec.Body.String() != tc.want {
					t.Errorf("expected body %q, got %q", tc.want, rec.Body.String())
				}
			})
		}
	})

	t.Run("writeJSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		for _, tc := range []struct {
			name     string
			data     any
			wantCode int
		}{
			{name: "valid JSON map returns 200", data: map[string]string{"foo": "bar"}, wantCode: http.StatusOK},
			{name: "unserializable data returns 500", data: make(chan int), wantCode: http.StatusInternalServerError},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				writeJSON(rec, req, http.StatusOK, tc.data)
				if rec.Code != tc.wantCode {
					t.Errorf("expected status %d, got %d", tc.wantCode, rec.Code)
				}
			})
		}
	})

	t.Run("ReconciliationStatus", func(t *testing.T) {
		var nilD *SyncDaemon
		if st := nilD.GetReconciliationStatus(); st.State != "idle" || st.Stage != "idle" {
			t.Errorf("expected idle for nil daemon, got %+v", st)
		}
		d := NewDaemon(DaemonConfig{})
		if st := d.GetReconciliationStatus(); st.State != "idle" || st.Stage != "idle" {
			t.Errorf("expected idle for daemon, got %+v", st)
		}
		statusResp := d.GetStatus(context.Background())
		if statusResp.Reconciliation == nil || statusResp.Reconciliation.State != "idle" {
			t.Errorf("expected state idle in GetStatus, got %+v", statusResp.Reconciliation)
		}
	})

	t.Run("GetStatus_populates_and_caches_GitHubRepo", func(t *testing.T) {
		remoteCalls := 0
		d := NewDaemon(DaemonConfig{
			Repos: []string{"/share/mirrormere"},
			GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 3 && args[0] == "remote" && args[1] == "get-url" && args[2] == "origin" {
					remoteCalls++
					return []byte("https://github.com/azylman/mirrormere.git\n"), nil, nil
				}
				if len(args) >= 4 && args[0] == "log" && args[1] == "-1" {
					return []byte("abc1234\x002026-09-26T20:00:00Z\n"), nil, nil
				}
				return nil, nil, nil
			},
		})

		st1 := d.GetStatus(context.Background())
		repoSt1, ok := st1.Repos["/share/mirrormere"]
		if !ok || repoSt1.GitHubRepo != "azylman/mirrormere" {
			t.Errorf("expected GitHubRepo azylman/mirrormere, got %q", repoSt1.GitHubRepo)
		}
		if remoteCalls != 1 {
			t.Errorf("expected 1 remote call, got %d", remoteCalls)
		}

		st2 := d.GetStatus(context.Background())
		repoSt2, ok := st2.Repos["/share/mirrormere"]
		if !ok || repoSt2.GitHubRepo != "azylman/mirrormere" {
			t.Errorf("expected cached GitHubRepo azylman/mirrormere, got %q", repoSt2.GitHubRepo)
		}
		if remoteCalls != 1 {
			t.Errorf("expected remoteCalls to remain 1 after cached call, got %d", remoteCalls)
		}
	})

	t.Run("RegistryClient_and_LastReconcile_accessors", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		if c := nilDaemon.getRegistryClient(); c == nil {
			t.Errorf("expected default registry client for nil daemon")
		}

		dDefault := &SyncDaemon{}
		c := dDefault.getRegistryClient()
		if c == nil || c.Timeout != 10*time.Second {
			t.Errorf("expected default registry client with 10s timeout, got %v", c)
		}

		custom := &http.Client{Timeout: 42 * time.Second}
		dCustom := NewDaemon(DaemonConfig{RegistryClient: custom})
		if dCustom.getRegistryClient() != custom {
			t.Errorf("expected custom registry client to be returned")
		}

		now := time.Now().Truncate(time.Second)
		dCustom.SetLastReconcile(now)
		if !dCustom.GetLastReconcile().Equal(now) {
			t.Errorf("expected LastReconcile to equal %v, got %v", now, dCustom.GetLastReconcile())
		}
	})
}

func TestHandleArtifactBundle_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	composeDir := filepath.Join(tmpDir, "aerial")
	configDir := filepath.Join(tmpDir, "aerial-config")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")

	_ = os.MkdirAll(filepath.Join(composeDir, "services", "dashboard", "static"), 0o755)
	_ = os.WriteFile(filepath.Join(composeDir, "services", "dashboard", "static", "index.html"), []byte("<h1>Dashboard</h1>"), 0o644)

	_ = os.MkdirAll(filepath.Join(composeDir, "services", "homepage"), 0o755)
	_ = os.WriteFile(filepath.Join(composeDir, "services", "homepage", "entrypoint.sh"), []byte("#!/bin/sh\n"), 0o755)
	_ = os.MkdirAll(filepath.Join(configDir, "homepage"), 0o755)
	_ = os.WriteFile(filepath.Join(configDir, "homepage", "services.yaml"), []byte("services: []\n"), 0o644)

	_ = os.MkdirAll(filepath.Join(mirrormereDir, "web"), 0o755)
	_ = os.WriteFile(filepath.Join(mirrormereDir, "web", "index.html"), []byte("<h1>Mirrormere</h1>"), 0o644)
	_ = os.MkdirAll(filepath.Join(mirrormereDir, "widgets"), 0o755)
	_ = os.WriteFile(filepath.Join(mirrormereDir, "widgets", "clock.js"), []byte("// clock"), 0o644)

	d := &SyncDaemon{
		composeDir:   composeDir,
		configDir:    configDir,
		repos:        []string{composeDir, configDir},
		assetBundles: testAssetBundles(composeDir, configDir, mirrormereDir),
	}
	handler := SetupMux(d)

	tests := []struct {
		name            string
		method          string
		url             string
		wantStatusCode  int
		wantContentType string
		wantHeaderHash  bool
	}{
		{
			name:            "dashboard tar gz bundle",
			method:          http.MethodGet,
			url:             "/internal/artifacts/dashboard.tar.gz",
			wantStatusCode:  http.StatusOK,
			wantContentType: "application/gzip",
			wantHeaderHash:  true,
		},
		{
			name:            "dashboard raw bundle without extension",
			method:          http.MethodGet,
			url:             "/internal/artifacts/dashboard",
			wantStatusCode:  http.StatusOK,
			wantContentType: "application/gzip",
			wantHeaderHash:  true,
		},
		{
			name:            "homepage bundle with excluded test",
			method:          http.MethodGet,
			url:             "/internal/artifacts/homepage.tar.gz",
			wantStatusCode:  http.StatusOK,
			wantContentType: "application/gzip",
			wantHeaderHash:  true,
		},
		{
			name:            "mirrormere bundle with compose dir fallback resolution",
			method:          http.MethodGet,
			url:             "/internal/artifacts/mirrormere.tar.gz",
			wantStatusCode:  http.StatusOK,
			wantContentType: "application/gzip",
			wantHeaderHash:  true,
		},
		{
			name:            "unknown bundle returns 404",
			method:          http.MethodGet,
			url:             "/internal/artifacts/nonexistent.tar.gz",
			wantStatusCode:  http.StatusNotFound,
			wantContentType: "application/json",
			wantHeaderHash:  false,
		},
		{
			name:            "empty bundle name returns 400",
			method:          http.MethodGet,
			url:             "/internal/artifacts/",
			wantStatusCode:  http.StatusBadRequest,
			wantContentType: "application/json",
			wantHeaderHash:  false,
		},
		{
			name:            "disallowed POST method returns 405",
			method:          http.MethodPost,
			url:             "/internal/artifacts/dashboard.tar.gz",
			wantStatusCode:  http.StatusMethodNotAllowed,
			wantContentType: "text/plain",
			wantHeaderHash:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, tc.url, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatusCode {
				t.Errorf("HandleArtifactBundle(%s %s) code = %d, want %d (body: %s)", tc.method, tc.url, rec.Code, tc.wantStatusCode, rec.Body.String())
			}
			ct := rec.Header().Get("Content-Type")
			if !strings.HasPrefix(ct, tc.wantContentType) {
				t.Errorf("HandleArtifactBundle(%s %s) content-type = %s, want prefix %s", tc.method, tc.url, ct, tc.wantContentType)
			}
			hash := rec.Header().Get("X-Asset-Hash")
			if tc.wantHeaderHash && hash == "" {
				t.Errorf("HandleArtifactBundle(%s %s) missing expected X-Asset-Hash header", tc.method, tc.url)
			}
		})
	}
}
