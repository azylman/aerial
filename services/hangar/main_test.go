package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
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
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return nil, nil, errors.New("compose fail")
			},
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

		d.SetLastReconcile(time.Now())
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
	if cfg := NewConfigFromEnv(); cfg.Port == "" {
		t.Errorf("expected non-empty port from NewConfigFromEnv()")
	}
	if cfg := NewConfigFromLookup(nil); cfg.Port != "8087" {
		t.Errorf("NewConfigFromLookup(nil) Port = %q, want 8087", cfg.Port)
	}

	baseYAML := `
port: "8087"
sync_interval: "45s"
sync_repos:
  - "/repo/a"
  - "/repo/b"
nomad_addr: "http://nomad.cluster:4646"
brain_internal_url: "http://brain.cluster:8088/internal/reload"
service_configs:
  - rel_path: "services/voice/voice.yaml"
    nomad_var: "nomad/jobs/orin-voice"
    var_key: "CONFIG_YAML"
`
	tests := []struct {
		name                     string
		yamlContent              string
		env                      map[string]string
		wantPort                 string
		wantRepos                []string
		wantInterval             time.Duration
		wantPAT                  string
		wantComposeDir           string
		wantConfigDir            string
		wantDiscordToken         string
		wantDiscordChannel       string
		wantDiscordWebhook       string
		wantNomadAddr            string
		wantBrainURL             string
		wantExtraSecretsLen      int
		checkExtraSecrets        bool
		wantServiceConfigRelPath string
	}{
		{
			name: "custom environment lookup with extra secrets",
			env: map[string]string{
				"PORT": "9090", "SYNC_REPOS": "/path/a, /path/b", "SYNC_INTERVAL": "30s",
				"GITHUB_PAT": "my_secret_pat", "AERIAL_CONFIG_REPO_URL": "https://github.com/custom/config.git",
				"AERIAL_PROJECT_DIR": "/custom/aerial", "AERIAL_CONFIG_DIR": "/custom/config",
				"DISCORD_BOT_TOKEN": "bot_token", "DISCORD_CHANNEL": "custom-channel",
				"DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/123/abc",
				"BRAIN_INTERNAL_URL":  "http://brain:9999/reload",
				"POSTGRES_PASSWORD":   "pg_pass", "HA_TOKEN": "ha_tok", "GEMINI_API_KEY": "gem_key",
			},
			wantPort: "9090", wantRepos: []string{"/path/a", "/path/b"}, wantInterval: 30 * time.Second,
			wantPAT: "my_secret_pat", wantComposeDir: "/custom/aerial", wantConfigDir: "/custom/config",
			wantDiscordToken: "bot_token", wantDiscordChannel: "custom-channel",
			wantDiscordWebhook: "https://discord.com/api/webhooks/123/abc", wantBrainURL: "http://brain:9999/reload",
			wantExtraSecretsLen: 4, checkExtraSecrets: true,
		},
		{
			name: "empty lookup defaults", env: map[string]string{},
			wantPort: "8087", wantInterval: 60 * time.Second, wantComposeDir: "/share/aerial",
			wantConfigDir: "/share/aerial-config", wantBrainURL: "http://brain:8088/internal/reload",
			wantExtraSecretsLen: 0, checkExtraSecrets: true,
		},
		{
			name: "CONFIG_PATH YAML loading and service configs", yamlContent: baseYAML,
			wantPort: "8087", wantInterval: 45 * time.Second, wantRepos: []string{"/repo/a", "/repo/b"},
			wantNomadAddr: "http://nomad.cluster:4646", wantBrainURL: "http://brain.cluster:8088/internal/reload",
			wantServiceConfigRelPath: "services/voice/voice.yaml",
		},
		{
			name: "env overrides take precedence over CONFIG_PATH YAML", yamlContent: baseYAML,
			env: map[string]string{
				"PORT": "9999", "SYNC_INTERVAL": "10s", "SYNC_REPOS": "/override/repo",
				"NOMAD_ADDR": "http://override.nomad:4646", "BRAIN_INTERNAL_URL": "http://override.brain:8088/reload",
			},
			wantPort: "9999", wantInterval: 10 * time.Second, wantRepos: []string{"/override/repo"},
			wantNomadAddr: "http://override.nomad:4646", wantBrainURL: "http://override.brain:8088/reload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envMap := make(map[string]string, len(tt.env)+1)
			for k, v := range tt.env {
				envMap[k] = v
			}
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
			if tt.wantInterval != 0 && cfg.Interval != tt.wantInterval {
				t.Errorf("Interval = %v, want %v", cfg.Interval, tt.wantInterval)
			}
			if tt.wantPAT != "" && cfg.PAT != tt.wantPAT {
				t.Errorf("PAT = %q, want %q", cfg.PAT, tt.wantPAT)
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
			if tt.wantBrainURL != "" && cfg.BrainInternalURL != tt.wantBrainURL {
				t.Errorf("BrainInternalURL = %q, want %q", cfg.BrainInternalURL, tt.wantBrainURL)
			}
			if tt.checkExtraSecrets && len(cfg.ExtraSecrets) != tt.wantExtraSecretsLen {
				t.Errorf("len(ExtraSecrets) = %d, want %d (%v)", len(cfg.ExtraSecrets), tt.wantExtraSecretsLen, cfg.ExtraSecrets)
			}
			if tt.wantServiceConfigRelPath != "" {
				if len(cfg.ServiceConfigs) != 1 || cfg.ServiceConfigs[0].RelPath != tt.wantServiceConfigRelPath {
					t.Errorf("ServiceConfigs = %v, want RelPath %q", cfg.ServiceConfigs, tt.wantServiceConfigRelPath)
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
					cfg.ComposeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						return []byte("ok"), nil, nil
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
		orig := os.Getenv("GOCOVERDIR")
		_ = os.Unsetenv("GOCOVERDIR")
		defer func() {
			if orig != "" {
				_ = os.Setenv("GOCOVERDIR", orig)
			} else {
				_ = os.Unsetenv("GOCOVERDIR")
			}
		}()

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 OK without GOCOVERDIR, got: %d", rec.Code)
		}

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
		_ = os.Setenv("GOCOVERDIR", t.TempDir())

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
			if len(line) > 0 && line[0] != ' ' {
				inDependsOn, currentService = false, ""
			} else if strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") && !strings.HasPrefix(line, "    depends_on:") && strings.HasSuffix(trimmed, ":") {
				inDependsOn = false
			} else if strings.Contains(line, "gitsync:") || trimmed == "- gitsync" {
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
	t.Run("defaultComposeExecutor CLI mocking", func(t *testing.T) {
		tempDir := t.TempDir()
		binDir := filepath.Join(tempDir, "bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			t.Fatalf("failed to create bin dir: %v", err)
		}
		if runtime.GOOS == "windows" {
			script := "@echo off\r\nset \"arg=%*\"\r\necho %arg% | findstr /i \"waitloop\" >nul && goto do_wait\r\necho %arg% | findstr /i \"compose\" >nul && (echo Docker Compose mock v2.0 & exit /b 0)\r\necho %arg% | findstr /i \"version\" >nul && (echo Docker mock v24.0 & exit /b 0)\r\necho unknown cmd >&2\r\nexit /b 1\r\n:do_wait\r\ngoto do_wait\r\n"
			_ = os.WriteFile(filepath.Join(binDir, "docker.cmd"), []byte(script), 0o755)
		} else {
			script := "#!/bin/sh\ncase \"$*\" in\n  *\"waitloop\"*) trap 'exit 0' TERM INT; while :; do :; done;;\n  *\"compose\"*) echo \"Docker Compose mock v2.0\"; exit 0;;\n  *\"version\"*) echo \"Docker mock v24.0\"; exit 0;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
			_ = os.WriteFile(filepath.Join(binDir, "docker"), []byte(script), 0o755)
		}
		t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

		for _, tc := range []struct {
			name, arg, wantOut string
			cancelCtx, wantErr bool
		}{
			{name: "compose executor success", arg: "version", wantOut: "Docker Compose mock"},
			{name: "compose executor cancel", arg: "waitloop", cancelCtx: true, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
					defer cancel()
				}
				stdout, stderr, err := defaultComposeExecutor(ctx, tempDir, tc.arg)
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

		if nilDaemon.getComposeExecutor() == nil || (&SyncDaemon{}).getComposeExecutor() == nil {
			t.Errorf("expected non-nil default compose executor")
		}
		customCompose := false
		dCustomCompose := &SyncDaemon{composeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			customCompose = true
			return nil, nil, nil
		}}
		_, _, _ = dCustomCompose.getComposeExecutor()(context.Background(), "", "")
		if !customCompose {
			t.Errorf("expected custom compose executor to be called")
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
				d := &SyncDaemon{webhooksRouterURL: routerURL}
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

type pushEventTestEnv struct {
	tmpDir          string
	aerialDir       string
	aerialConfigDir string
	mirrormereDir   string
	repos           []string
	daemon          *SyncDaemon
	evtCh           chan HangarDeployEvent
	appliedPaths    []string
	varPutJobs      []string
	mu              sync.Mutex
}

func newPushEventTestEnv(t *testing.T, opts ...func(*DaemonConfig)) *pushEventTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")
	_ = os.MkdirAll(aerialDir, 0o755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "nomad", "jobs"), 0o755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0o755)
	_ = os.MkdirAll(mirrormereDir, 0o755)
	_ = os.MkdirAll(filepath.Join(aerialDir, ".git"), 0o755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0o755)
	_ = os.MkdirAll(filepath.Join(mirrormereDir, ".git"), 0o755)

	repos := []string{aerialDir, aerialConfigDir, mirrormereDir}
	evtCh := make(chan HangarDeployEvent, 50)

	cfg := DaemonConfig{
		Repos:      repos,
		ComposeDir: aerialDir,
		ConfigDir:  aerialConfigDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			select {
			case evtCh <- evt:
			default:
			}
			return nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				return []byte("sha_after\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: default-123"), nil, nil
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	d := NewDaemon(cfg)
	return &pushEventTestEnv{
		tmpDir:          tmpDir,
		aerialDir:       aerialDir,
		aerialConfigDir: aerialConfigDir,
		mirrormereDir:   mirrormereDir,
		repos:           repos,
		daemon:          d,
		evtCh:           evtCh,
	}
}

func (e *pushEventTestEnv) collectEvents(count int, timeout time.Duration) []HangarDeployEvent {
	var evts []HangarDeployEvent
	deadline := time.After(timeout)
	for i := 0; i < count; i++ {
		select {
		case evt := <-e.evtCh:
			evts = append(evts, evt)
		case <-deadline:
			return evts
		}
	}
	return evts
}

type mockGitScript struct {
	custom     bool
	beforeSHA  string
	afterSHA   string
	nameOnly   string
	nameStatus string
	failCmd    string
}

func (e *pushEventTestEnv) setGitScript(s mockGitScript) {
	var revCalls int
	e.daemon.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		cmd := strings.Join(args, " ")
		if s.failCmd != "" {
			for _, part := range strings.Split(s.failCmd, ",") {
				if strings.Contains(cmd, strings.TrimSpace(part)) {
					return nil, []byte("git error"), errors.New("simulated " + s.failCmd + " failure")
				}
			}
		}
		switch {
		case len(args) >= 2 && args[0] == "rev-parse" && args[1] == "HEAD":
			revCalls++
			if revCalls == 1 || s.afterSHA == "" {
				if s.beforeSHA == "" {
					return []byte(""), nil, nil
				}
				return []byte(s.beforeSHA + "\n"), nil, nil
			}
			return []byte(s.afterSHA + "\n"), nil, nil
		case strings.Contains(cmd, "--name-only"):
			return []byte(s.nameOnly), nil, nil
		case strings.Contains(cmd, "--name-status"):
			return []byte(s.nameStatus), nil, nil
		default:
			return []byte("ok"), nil, nil
		}
	}
}

func (env *pushEventTestEnv) setupCase(tc gitPushCase) {
	if tc.nomadAddr != "" {
		env.daemon.nomadAddr = tc.nomadAddr
	}
	if tc.serviceFiles {
		_ = os.MkdirAll(filepath.Join(env.aerialConfigDir, "services", "voice"), 0o755)
		_ = os.MkdirAll(filepath.Join(env.aerialConfigDir, "services", "hangar"), 0o755)
		_ = os.WriteFile(filepath.Join(env.aerialConfigDir, "services", "voice", "voice.yaml"), []byte("model: small.en\n"), 0o644)
		hangarContent := "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/orin-voice\"\n    var_key: \"CONFIG_YAML\"\n"
		_ = os.WriteFile(filepath.Join(env.aerialConfigDir, "services", "hangar", "hangar.yaml"), []byte(hangarContent), 0o644)
	}
	targetDir := env.aerialDir
	if strings.Contains(tc.req.Repo, "aerial-config") {
		targetDir = env.aerialConfigDir
	}
	for p, content := range tc.jobFiles {
		fullP := filepath.Join(targetDir, p)
		_ = os.MkdirAll(filepath.Dir(fullP), 0o755)
		_ = os.WriteFile(fullP, []byte(content), 0o644)
	}
	env.daemon.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
			if tc.nomadFailCmd == "validate" {
				return nil, []byte("validate error"), errors.New("invalid job")
			}
			if tc.nomadFailCmd == "complex" && strings.Contains(args[len(args)-1], "invalid.nomad") {
				return nil, []byte("syntax error"), errors.New("invalid job")
			}
			return []byte("OK"), nil, nil
		}
		if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
			env.mu.Lock()
			env.appliedPaths = append(env.appliedPaths, args[len(args)-1])
			env.mu.Unlock()
			if tc.nomadFailCmd == "run" {
				return nil, []byte("driver error: failed to place allocation"), errors.New("nomad run failed")
			}
			if tc.nomadFailCmd == "complex" && strings.Contains(args[len(args)-1], "broken.nomad") {
				return nil, []byte("run error"), errors.New("run failed")
			}
			return []byte("Evaluation ID: default-123"), nil, nil
		}
		if len(args) >= 2 && args[0] == "var" && args[1] == "put" {
			if len(args) >= 4 {
				env.mu.Lock()
				env.varPutJobs = append(env.varPutJobs, args[3])
				env.mu.Unlock()
			}
			if tc.nomadFailCmd == "var" {
				return nil, []byte("nomad var put permission denied"), errors.New("var put failed")
			}
			return []byte("ok"), nil, nil
		}
		return []byte("OK"), nil, nil
	}
	switch tc.registryMode {
	case "not_found":
		env.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "actions/runs") {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`))}, nil
			}
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`))}, nil
		}}}
	case "runs_active":
		env.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "actions/runs") {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 1, "workflow_runs": [{}]}`))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{}`))}, nil
		}}}
	case "digest_ready":
		env.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "actions/runs") {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`))}, nil
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{}`))}
			resp.Header.Set("Docker-Content-Digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
			return resp, nil
		}}}
	}
	if tc.gitScript != (mockGitScript{}) {
		env.setGitScript(tc.gitScript)
	}
}

type gitPushCase struct {
	name                 string
	req                  GitPushEventRequest
	gitScript            mockGitScript
	jobFiles             map[string]string
	serviceFiles         bool
	nomadAddr            string
	nomadFailCmd         string // "validate", "run", "var", "complex"
	registryMode         string // "not_found", "runs_active", "digest_ready"
	wantCode             int
	wantStatus           string
	wantMessage          string
	wantBuilding         bool
	wantNomadChanged     bool
	wantApplied          []string
	wantExpectedJobs     []string
	wantEvents           []string
	wantCommit           string
	wantDeployJob        string
	checkDeferralJob     string
	checkZeroVarPutCalls bool
	checkPendingDeletes  []string
}

func TestExecuteGitPushEvent_TableDriven(t *testing.T) { // t.Run(
	ctx := context.Background()
	jobWithAerialImg := `job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`

	cases := []gitPushCase{
		{
			name:       "missing repo",
			req:        GitPushEventRequest{},
			wantCode:   http.StatusBadRequest,
			wantStatus: "error",
		},
		{
			name:       "non-main branch",
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/feature-branch"},
			wantCode:   http.StatusOK,
			wantStatus: "ignored",
		},
		{
			name:       "unmanaged repository",
			req:        GitPushEventRequest{Repo: "nonexistent-repo", Ref: "refs/heads/main"},
			wantCode:   http.StatusNotFound,
			wantStatus: "error",
		},
		{
			name:       "sync failure",
			gitScript:  mockGitScript{failCmd: "fetch,pull"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
		},
		{
			name:        "peripheral repo",
			req:         GitPushEventRequest{Repo: "mirrormere", Ref: "refs/heads/main"},
			wantCode:    http.StatusOK,
			wantStatus:  "accepted",
			wantMessage: "repository synced",
		},
		{
			name:       "successful sync with target id",
			gitScript:  mockGitScript{beforeSHA: "sha_sync_ok"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_12345", TargetID: "1555405874565091380", PRNumber: 542},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantEvents: []string{"sync_success"},
		},
		{
			name:       "failed sync with target id dispatches sync_failed",
			gitScript:  mockGitScript{failCmd: "fetch,pull"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_fail", TargetID: "1555405874565091380", PRNumber: 542},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantEvents: []string{"sync_failed"},
		},
		{
			name:       "empty current head falls back to req commit",
			gitScript:  mockGitScript{custom: true},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_req_fallback", TargetID: "1555405874565091380", PRNumber: 542},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantEvents: []string{"sync_success"},
			wantCommit: "commit_req_fallback",
		},
		{
			name:       "empty target id still dispatches sync_success",
			gitScript:  mockGitScript{beforeSHA: "sha_sync_ok"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_empty_target"},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantEvents: []string{"sync_success"},
			wantCommit: "sha_sync_ok",
		},
		{
			name:         "build changes",
			gitScript:    mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "services/eri/main.go\nDockerfile\n"},
			req:          GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:     http.StatusOK,
			wantBuilding: true,
		},
		{
			name:             "non-build nomad change applied directly",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "nomad/jobs/migrate.nomad\n", nameStatus: "M nomad/jobs/migrate.nomad\n"},
			jobFiles:         map[string]string{"nomad/jobs/migrate.nomad": `job "migrate" {}`},
			req:              GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", PRNumber: 611, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantApplied:      []string{"migrate"},
			wantExpectedJobs: []string{"migrate"},
			wantDeployJob:    "migrate",
		},
		{
			name:             "nomad apply failure returns 500",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "nomad/jobs/migrate.nomad\n", nameStatus: "M nomad/jobs/migrate.nomad\n"},
			jobFiles:         map[string]string{"nomad/jobs/migrate.nomad": `job "migrate" {}`},
			nomadFailCmd:     "validate",
			req:              GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:         http.StatusInternalServerError,
			wantStatus:       "error",
			wantNomadChanged: true,
		},
		{
			name:       "no changes",
			gitScript:  mockGitScript{beforeSHA: "sha1", nameOnly: "docs/index.html\n"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
		},
		{
			name:      "mixed build and nomad deferral",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "services/vanadis/Dockerfile\nnomad/jobs/vanadis.nomad\nnomad/jobs/coverage-ingest.nomad\n", nameStatus: "A\tnomad/jobs/vanadis.nomad\nM\tnomad/jobs/coverage-ingest.nomad\n"},
			jobFiles: map[string]string{
				"nomad/jobs/vanadis.nomad":         `job "vanadis" { task "vanadis" { config { image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}" } } }`,
				"nomad/jobs/coverage-ingest.nomad": `job "coverage-ingest" { task "flush" { config { image = "golang:1.27.1-alpine" } } }`,
			},
			registryMode:     "not_found",
			req:              GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after", PRNumber: 719, TargetID: "1560318118792790111"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantApplied:      []string{"coverage-ingest"},
			wantExpectedJobs: []string{"coverage-ingest", "vanadis"},
			checkDeferralJob: "vanadis",
		},
		{
			name:      "mixed build and untouched repo nomad job applies untouched job immediately",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "services/hangar/main.go\nnomad/jobs/hangar.nomad\nnomad/jobs/eri.nomad\n", nameStatus: "M\tnomad/jobs/hangar.nomad\nM\tnomad/jobs/eri.nomad\n"},
			jobFiles: map[string]string{
				"nomad/jobs/hangar.nomad": `job "hangar" { task "hangar" { config { image = "ghcr.io/azylman/aerial-hangar:${var.image_tag}" } } }`,
				"nomad/jobs/eri.nomad":    `job "eri" { task "eri" { config { image = "ghcr.io/azylman/aerial-eri:${var.image_tag}" } } }`,
			},
			registryMode:     "digest_ready",
			req:              GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after", PRNumber: 948, TargetID: "1562543935417487501"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantApplied:      []string{"eri"},
			wantExpectedJobs: []string{"eri", "hangar"},
		},
		{
			name: "job rename with containers building defers delete",
			gitScript: mockGitScript{
				beforeSHA:  "sha_rename_before",
				afterSHA:   "sha_rename_after",
				nameOnly:   "services/coven/Dockerfile\nnomad/jobs/memory-mcp.nomad\nnomad/jobs/coven.nomad\n",
				nameStatus: "D\tnomad/jobs/memory-mcp.nomad\nA\tnomad/jobs/coven.nomad\n",
			},
			jobFiles: map[string]string{
				"nomad/jobs/coven.nomad": `job "coven" { task "coven" { config { image = "ghcr.io/azylman/aerial-coven:${var.image_tag}" } } }`,
			},
			registryMode:        "not_found",
			req:                 GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_rename_after", PRNumber: 800, TargetID: "1560318118792790999"},
			wantCode:            http.StatusOK,
			wantBuilding:        true,
			wantNomadChanged:    true,
			wantApplied:         nil,
			wantExpectedJobs:    []string{"coven"},
			checkPendingDeletes: []string{"memory-mcp"},
		},
		{
			name:             "migration change runs db-migrate immediately",
			gitScript:        mockGitScript{beforeSHA: "sha_before_mig", afterSHA: "sha_after_mig", nameOnly: "services/eri/pkg/db/migrations/20261016182900_add_started_at.sql\nservices/eri/pkg/db/vanadis.go\n", nameStatus: "A\tservices/eri/pkg/db/migrations/20261016182900_add_started_at.sql\nM\tservices/eri/pkg/db/vanadis.go\n"},
			jobFiles:         map[string]string{"nomad/jobs/db-migrate.nomad": `job "db-migrate" { type = "batch" task "atlas" { config { image = "arigaio/atlas:1.3.3" } } }`},
			req:              GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after_mig", PRNumber: 751, TargetID: "1560698400780062884"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantApplied:      []string{"db-migrate"},
			wantExpectedJobs: []string{"db-migrate"},
		},
		{
			name:         "config sync notices nomad var put fails gracefully",
			gitScript:    mockGitScript{beforeSHA: "sha123"},
			nomadFailCmd: "var",
			req:          GitPushEventRequest{Repo: "azylman/aerial-config", Commit: "sha123"},
			wantCode:     http.StatusOK,
		},
		{
			name:                 "diff gating untouched service configs skip nomad var put",
			gitScript:            mockGitScript{beforeSHA: "sha_prev", afterSHA: "sha_curr", nameOnly: "services/hangar/hangar.yaml\n"},
			nomadAddr:            "http://127.0.0.1:4646",
			serviceFiles:         true,
			req:                  GitPushEventRequest{Repo: "azylman/aerial-config", Commit: "sha_curr"},
			wantCode:             http.StatusOK,
			checkZeroVarPutCalls: true,
		},
		{
			name:             "aerial-config variable mapping resolves expected_jobs and dispatches on sync_success and deploy_started",
			gitScript:        mockGitScript{beforeSHA: "sha_prev", afterSHA: "sha_curr", nameOnly: "services/voice/voice.yaml\n"},
			nomadAddr:        "http://127.0.0.1:4646",
			serviceFiles:     true,
			req:              GitPushEventRequest{Repo: "azylman/aerial-config", Commit: "sha_curr", PRNumber: 301, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantExpectedJobs: []string{"orin-voice"},
			wantEvents:       []string{"sync_success", "deploy_started"},
		},
		{
			name:             "deferred nomad job due to active build",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/webhooks-router.nomad\n"},
			jobFiles:         map[string]string{"jobs/webhooks-router.nomad": jobWithAerialImg},
			registryMode:     "runs_active",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantExpectedJobs: []string{"webhooks-router"},
		},
		{
			name:             "image ready applies nomad job and dispatches deploy_started",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/webhooks-router.nomad\n"},
			jobFiles:         map[string]string{"jobs/webhooks-router.nomad": jobWithAerialImg},
			registryMode:     "digest_ready",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main", Commit: "commit_abc123", PRNumber: 255, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantApplied:      []string{"webhooks-router"},
			wantExpectedJobs: []string{"webhooks-router"},
			wantEvents:       []string{"sync_success", "deploy_started"},
		},
		{
			name:             "deploy failed dispatches deploy_failed",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/webhooks-router.nomad\n"},
			jobFiles:         map[string]string{"jobs/webhooks-router.nomad": `job "webhooks-router" {}`},
			nomadFailCmd:     "run",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main", Commit: "fail_commit_123"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantEvents:       []string{"sync_success", "deploy_started", "deploy_failed"},
		},
		{
			name:      "complex aerial-config diff",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "D jobs/deleted.nomad\nM jobs/deferred.nomad\nM jobs/invalid.nomad\nM jobs/nonexistent.nomad\nM jobs/broken.nomad\n"},
			jobFiles: map[string]string{
				"jobs/invalid.nomad": `job "invalid" {}`,
				"jobs/broken.nomad":  `job "broken" {}`,
			},
			nomadFailCmd:     "complex",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
		},
		{
			name:       "config diff error returns 500",
			gitScript:  mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", failCmd: "diff"},
			req:        GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newPushEventTestEnv(t)
			env.setupCase(tc)
			resp, code := env.daemon.ExecuteGitPushEvent(ctx, tc.req)
			if code != tc.wantCode {
				t.Fatalf("ExecuteGitPushEvent() code = %d, want %d (resp: %+v)", code, tc.wantCode, resp)
			}
			if tc.wantStatus != "" && resp.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", resp.Status, tc.wantStatus)
			}
			if tc.wantMessage != "" && resp.Message != tc.wantMessage {
				t.Errorf("message = %q, want %q", resp.Message, tc.wantMessage)
			}
			if resp.ContainersBuilding != tc.wantBuilding {
				t.Errorf("ContainersBuilding = %v, want %v", resp.ContainersBuilding, tc.wantBuilding)
			}
			if resp.NomadChanged != tc.wantNomadChanged {
				t.Errorf("NomadChanged = %v, want %v", resp.NomadChanged, tc.wantNomadChanged)
			}
			if tc.wantApplied != nil {
				if len(resp.AppliedJobs) != len(tc.wantApplied) {
					t.Errorf("AppliedJobs = %v, want %v", resp.AppliedJobs, tc.wantApplied)
				} else {
					for i, job := range tc.wantApplied {
						if resp.AppliedJobs[i] != job {
							t.Errorf("AppliedJobs[%d] = %q, want %q", i, resp.AppliedJobs[i], job)
						}
					}
				}
			}
			if tc.wantExpectedJobs != nil {
				if len(resp.ExpectedJobs) != len(tc.wantExpectedJobs) {
					t.Errorf("ExpectedJobs = %v, want %v", resp.ExpectedJobs, tc.wantExpectedJobs)
				} else {
					for i, job := range tc.wantExpectedJobs {
						if resp.ExpectedJobs[i] != job {
							t.Errorf("ExpectedJobs[%d] = %q, want %q", i, resp.ExpectedJobs[i], job)
						}
					}
				}
			}
			if tc.checkZeroVarPutCalls {
				env.mu.Lock()
				if len(env.varPutJobs) != 0 {
					t.Errorf("expected zero var put calls for untouched service configs, got: %v", env.varPutJobs)
				}
				env.mu.Unlock()
			}
			if tc.wantDeployJob != "" {
				evts := env.collectEvents(2, 2*time.Second)
				var found bool
				for _, evt := range evts {
					if evt.JobName == tc.wantDeployJob {
						found = true
						if tc.req.PRNumber != 0 && evt.PRNumber != tc.req.PRNumber {
							t.Errorf("PRNumber = %d, want %d", evt.PRNumber, tc.req.PRNumber)
						}
						if tc.req.TargetID != "" && evt.TargetID != tc.req.TargetID {
							t.Errorf("TargetID = %q, want %q", evt.TargetID, tc.req.TargetID)
						}
						if tc.wantExpectedJobs != nil && len(evt.ExpectedJobs) != len(tc.wantExpectedJobs) {
							t.Errorf("deploy event ExpectedJobs = %v, want %v", evt.ExpectedJobs, tc.wantExpectedJobs)
						}
					}
				}
				if !found {
					t.Errorf("missing %s deploy event in: %+v", tc.wantDeployJob, evts)
				}
			}
			if len(tc.wantEvents) > 0 {
				evts := env.collectEvents(len(tc.wantEvents), 2*time.Second)
				for _, wantEvt := range tc.wantEvents {
					var found bool
					for _, evt := range evts {
						if evt.Event == wantEvt {
							found = true
							if tc.wantCommit != "" && evt.CommitSHA != tc.wantCommit {
								t.Errorf("commitSHA = %q, want %q", evt.CommitSHA, tc.wantCommit)
							}
							if tc.req.PRNumber != 0 && evt.PRNumber != tc.req.PRNumber {
								t.Errorf("PRNumber = %d, want %d", evt.PRNumber, tc.req.PRNumber)
							}
							if tc.req.TargetID != "" && evt.TargetID != tc.req.TargetID {
								t.Errorf("TargetID = %q, want %q", evt.TargetID, tc.req.TargetID)
							}
							if tc.wantExpectedJobs != nil && (wantEvt == "sync_success" || wantEvt == "deploy_started") {
								if len(evt.ExpectedJobs) != len(tc.wantExpectedJobs) {
									t.Errorf("%s ExpectedJobs = %v, want %v", wantEvt, evt.ExpectedJobs, tc.wantExpectedJobs)
								}
							}
							if wantEvt == "deploy_failed" {
								if evt.Status != "failed" || !strings.Contains(evt.Details, "driver error") {
									t.Errorf("expected failed status and driver error details, got: %+v", evt)
								}
							}
						}
					}
					if !found {
						t.Errorf("missing expected event %q in: %+v", wantEvt, evts)
					}
				}
			}
			if tc.checkDeferralJob != "" {
				if pending := env.daemon.drainPendingNomadChanges(); len(pending) != 0 {
					t.Fatalf("expected zero pendingNomadChanges, got %+v", pending)
				}
				if err := env.daemon.ReconcileNomadChanges(ctx, env.aerialDir, []NomadFileChange{{Path: "nomad/jobs/" + tc.checkDeferralJob + ".nomad", Action: "apply", JobName: tc.checkDeferralJob}}); err != nil {
					t.Fatalf("unexpected error in ReconcileNomadChanges: %v", err)
				}
				env.mu.Lock()
				for _, p := range env.appliedPaths {
					if strings.Contains(p, tc.checkDeferralJob+".nomad") {
						t.Fatalf("expected %s.nomad to be deferred when image is missing in registry, got: %v", tc.checkDeferralJob, env.appliedPaths)
					}
				}
				env.mu.Unlock()
			}
			if len(tc.checkPendingDeletes) > 0 {
				pending := env.daemon.drainPendingNomadChanges()
				var gotDeletedJobs []string
				for _, evt := range pending {
					for _, ch := range evt.Changes {
						if ch.Action == "delete" {
							gotDeletedJobs = append(gotDeletedJobs, ch.JobName)
						}
					}
				}
				if len(gotDeletedJobs) != len(tc.checkPendingDeletes) {
					t.Fatalf("pending deleted jobs = %v, want %v", gotDeletedJobs, tc.checkPendingDeletes)
				}
				for i, j := range tc.checkPendingDeletes {
					if gotDeletedJobs[i] != j {
						t.Errorf("pending deleted job[%d] = %q, want %q", i, gotDeletedJobs[i], j)
					}
				}
			}
		})
	}
}

type imageReadyTestEnv struct {
	t           *testing.T
	ctx         context.Context
	tmpDir      string
	jobsDir     string
	coreJobsDir string
	daemon      *SyncDaemon
	evtCh       chan HangarDeployEvent
	runCalls    []string
	runMu       sync.Mutex
	restarts    []string
	restartMu   sync.Mutex
	stoppedJobs []string
	stopMu      sync.Mutex
	valErr      error
	runErr      error
	restartErr  error
	runSignal   chan string
}

func newImageReadyTestEnv(t *testing.T, opts ...func(*DaemonConfig)) *imageReadyTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0o755)

	coreJobsDir := filepath.Join(tmpDir, "core", "nomad", "jobs")
	_ = os.MkdirAll(coreJobsDir, 0o755)

	env := &imageReadyTestEnv{
		t:           t,
		ctx:         context.Background(),
		tmpDir:      tmpDir,
		jobsDir:     jobsDir,
		coreJobsDir: coreJobsDir,
		evtCh:       make(chan HangarDeployEvent, 20),
	}

	cfg := DaemonConfig{
		ConfigDir:  tmpDir,
		ComposeDir: filepath.Join(tmpDir, "core"),
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			select {
			case env.evtCh <- evt:
			default:
			}
			return nil
		},
		RegistryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", "sha256:1111111111111111111111111111111111111111111111111111111111111111")
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" {
				switch args[1] {
				case "validate":
					if env.valErr != nil {
						return nil, []byte("syntax error in HCL"), env.valErr
					}
					return []byte("OK"), nil, nil
				case "run":
					env.runMu.Lock()
					env.runCalls = append(env.runCalls, strings.Join(args, " "))
					env.runMu.Unlock()
					if env.runSignal != nil {
						select {
						case env.runSignal <- strings.Join(args, " "):
						default:
						}
					}
					if env.runErr != nil {
						return nil, []byte("Nomad agent connection refused"), env.runErr
					}
					return []byte("Evaluation ID: eval-success-123"), nil, nil
				case "restart":
					env.restartMu.Lock()
					if len(args) >= 3 {
						env.restarts = append(env.restarts, args[len(args)-1])
					}
					env.restartMu.Unlock()
					if env.restartErr != nil {
						return nil, []byte("restart failed warning"), env.restartErr
					}
					return []byte("Restart triggered"), nil, nil
				case "stop":
					env.stopMu.Lock()
					if len(args) >= 4 {
						env.stoppedJobs = append(env.stoppedJobs, args[len(args)-1])
					}
					env.stopMu.Unlock()
					return []byte("OK"), nil, nil
				case "status":
					return []byte(`{"ID":"status-ok","JobVersion":1,"LatestDeployment":{"ID":"dep-1","Status":"successful"}}`), nil, nil
				}
			}
			return []byte("OK"), nil, nil
		},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	env.daemon = NewDaemon(cfg)
	return env
}

func (e *imageReadyTestEnv) writeJob(relDir, filename, hcl string) string {
	e.t.Helper()
	var targetDir string
	if relDir == "core" {
		targetDir = e.coreJobsDir
	} else {
		targetDir = e.jobsDir
	}
	path := filepath.Join(targetDir, filename)
	if err := os.WriteFile(path, []byte(hcl), 0o644); err != nil {
		e.t.Fatalf("failed writing job file %s: %v", path, err)
	}
	return path
}

func (e *imageReadyTestEnv) getDigest(jobKey string) string {
	e.daemon.nomadDigestsMu.RLock()
	defer e.daemon.nomadDigestsMu.RUnlock()
	return e.daemon.nomadKnownDigests[jobKey]
}

func (e *imageReadyTestEnv) getRunCalls() []string {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	return append([]string(nil), e.runCalls...)
}

func (e *imageReadyTestEnv) getRestartCalls() []string {
	e.restartMu.Lock()
	defer e.restartMu.Unlock()
	return append([]string(nil), e.restarts...)
}

func (e *imageReadyTestEnv) getStoppedJobs() []string {
	e.stopMu.Lock()
	defer e.stopMu.Unlock()
	return append([]string(nil), e.stoppedJobs...)
}

func TestExecuteImageReadyEvent_TableDriven(t *testing.T) { // t.Run(
	ctx := context.Background()

	routerHCL := `variable "image_tag" {
		type    = string
		default = "latest"
	}
	job "router" {
		group "router" {
			task "router" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/aerial-webhooks-router:${var.image_tag}"
				}
			}
		}
	}`
	legacyHCL := `job "legacy" {
		task "legacy" {
			config {
				image = "ghcr.io/azylman/legacy-app:latest"
			}
		}
	}`

	type imageReadyCase struct {
		name                 string
		jobDir               string
		jobFile              string
		jobHCL               string
		req                  ImageReadyEventRequest
		valErr               error
		runErr               error
		restartErr           error
		registryNotFound     bool
		removeJobOnRegistry  bool
		pendingDeletes       []NomadFileChange
		wantCode             int
		wantStatus           string
		wantRuns             int
		wantRests            int
		wantRunArg           string
		wantEvent            string
		wantCachedDigest     string
		wantStoppedJobs      []string
		wantRemainingPending int
	}

	cases := []imageReadyCase{
		{
			name:       "missing image",
			req:        ImageReadyEventRequest{},
			wantCode:   http.StatusBadRequest,
			wantStatus: "error",
		},
		{
			name:       "unknown image",
			req:        ImageReadyEventRequest{Image: "ghcr.io/azylman/nonexistent:latest"},
			wantCode:   http.StatusNotFound,
			wantStatus: "not_found",
		},
		{
			name:    "deploy started dispatch and metadata propagation",
			jobFile: "brain.nomad",
			jobHCL: `job "brain" {
				task "brain" {
					config {
						image = "ghcr.io/azylman/aerial-brain:latest"
					}
				}
			}`,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-brain:latest",
				Digest:    "sha256:digest_brain",
				Repo:      "azylman/aerial",
				CommitSHA: "headsha777",
				PRNumber:  535,
				TargetID:  "1555405874565091380",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  1,
			wantEvent:  "deploy_started",
		},
		{
			name:    "commit SHA variable injection",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:      "azylman/aerial",
				CommitSHA: "sha12345",
				PRNumber:  615,
				TargetID:  "discord_thread_999",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "job run -detach -var=image_tag=sha12345 ",
		},
		{
			name:    "digest fallback variable injection",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest:   "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:    "unparameterized legacy job triggers restart",
			jobFile: "legacy.nomad",
			jobHCL:  legacyHCL,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/legacy-app:latest",
				CommitSHA: "legacy123",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  1,
		},
		{
			name:             "empty sha and digest fallback to latest",
			jobFile:          "router.nomad",
			jobHCL:           routerHCL,
			registryNotFound: true,
			req: ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "-var=image_tag=latest",
		},
		{
			name:    "digest prefix normalization",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:   "azylman/aerial",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:       "unparameterized restart warning succeeds",
			jobFile:    "legacy.nomad",
			jobHCL:     legacyHCL,
			restartErr: errors.New("exit status 1"),
			req: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/legacy-app:latest",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  1,
		},
		{
			name:    "validation failure returns 500",
			jobFile: "val-fail.nomad",
			jobHCL:  `job "val-fail" { task "t" { config { image = "ghcr.io/azylman/val-fail:latest" } } }`,
			valErr:  errors.New("validate failed"),
			req: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/val-fail:latest",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   0,
			wantRests:  0,
		},
		{
			name:                "mid-flight file removal triggers read error",
			jobFile:             "read-fail.nomad",
			jobHCL:              `job "read-fail" { task "t" { config { image = "ghcr.io/azylman/read-fail:latest" } } }`,
			removeJobOnRegistry: true,
			req: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/read-fail:latest",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   0,
			wantRests:  0,
		},
		{
			name:    "nomad job run error dispatches deploy_failed",
			jobFile: "failing.nomad",
			jobHCL:  `job "failing-job" { task "t" { config { image = "ghcr.io/azylman/aerial-broken:latest" } } }`,
			runErr:  errors.New("exit status 1"),
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-broken:latest",
				Digest:    "sha256:abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
				Repo:      "azylman/aerial",
				CommitSHA: "broken_sha",
				PRNumber:  999,
				TargetID:  "thread_broken",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   1,
			wantRests:  0,
		},
		{
			name:       "core composeDir matching and digest caching",
			jobDir:     "core",
			jobFile:    "core.nomad",
			jobHCL:     `job "core-job" { task "t" { config { image = "ghcr.io/azylman/core-service:latest" } } }`,
			restartErr: errors.New("restart warning err"),
			req: ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/core-service:latest",
				Digest: "sha256:core123456",
			},
			wantCode:         http.StatusOK,
			wantStatus:       "accepted",
			wantRuns:         1,
			wantRests:        1,
			wantCachedDigest: "sha256:core123456",
		},
		{
			name:    "drains and stops pending deleted jobs",
			jobFile: "coven.nomad",
			jobHCL: `variable "image_tag" {
				type    = string
				default = "latest"
			}
			job "coven" {
				task "coven" {
					config {
						image = "ghcr.io/azylman/aerial-coven:${var.image_tag}"
					}
				}
			}`,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-coven:latest",
				CommitSHA: "sha_coven_ready",
			},
			pendingDeletes: []NomadFileChange{
				{JobName: "memory-mcp", Action: "delete"},
			},
			wantCode:        http.StatusOK,
			wantStatus:      "accepted",
			wantRuns:        1,
			wantStoppedJobs: []string{"memory-mcp"},
		},
		{
			name:    "drains deletes while preserving non-delete changes",
			jobFile: "coven.nomad",
			jobHCL: `variable "image_tag" {
				type    = string
				default = "latest"
			}
			job "coven" {
				task "coven" {
					config {
						image = "ghcr.io/azylman/aerial-coven:${var.image_tag}"
					}
				}
			}`,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-coven:latest",
				CommitSHA: "sha_coven_ready",
			},
			pendingDeletes: []NomadFileChange{
				{JobName: "memory-mcp", Action: "delete"},
				{JobName: "pending-service", Action: "apply"},
			},
			wantCode:             http.StatusOK,
			wantStatus:           "accepted",
			wantRuns:             1,
			wantStoppedJobs:      []string{"memory-mcp"},
			wantRemainingPending: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			env.valErr = tc.valErr
			env.runErr = tc.runErr
			env.restartErr = tc.restartErr

			var jobPath string
			if tc.jobFile != "" {
				dir := tc.jobDir
				if dir == "" {
					dir = "jobs"
				}
				jobPath = env.writeJob(dir, tc.jobFile, tc.jobHCL)
			}
			if tc.registryNotFound {
				env.daemon.registryClient = &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusNotFound,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("not found")),
							}, nil
						},
					},
				}
			}
			if tc.removeJobOnRegistry {
				env.daemon.registryClient = &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							if jobPath != "" {
								_ = os.Remove(jobPath)
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader(`{"token":"abc"}`)),
							}, nil
						},
					},
				}
			}

			if len(tc.pendingDeletes) > 0 {
				env.daemon.recordPendingNomadChanges(env.tmpDir, tc.pendingDeletes, tc.req.Repo, tc.req.CommitSHA, tc.req.TargetID, fmt.Sprintf("%d", tc.req.PRNumber))
			}

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, tc.req)
			if code != tc.wantCode || resp.Status != tc.wantStatus {
				t.Fatalf("expected code %d status %q, got code %d status %q (%+v)", tc.wantCode, tc.wantStatus, code, resp.Status, resp)
			}
			runs := env.getRunCalls()
			if len(runs) != tc.wantRuns {
				t.Errorf("expected %d runs, got %d (%v)", tc.wantRuns, len(runs), runs)
			}
			if tc.wantRunArg != "" {
				if len(runs) == 0 || !strings.Contains(runs[0], tc.wantRunArg) {
					t.Errorf("expected run call to contain %q, got %v", tc.wantRunArg, runs)
				}
			}
			restarts := env.getRestartCalls()
			if len(restarts) != tc.wantRests {
				t.Errorf("expected %d restarts, got %d (%v)", tc.wantRests, len(restarts), restarts)
			}
			if tc.wantStoppedJobs != nil {
				stopped := env.getStoppedJobs()
				if len(stopped) != len(tc.wantStoppedJobs) {
					t.Fatalf("expected %d stopped jobs, got %d (%v)", len(tc.wantStoppedJobs), len(stopped), stopped)
				}
				for i, j := range tc.wantStoppedJobs {
					if stopped[i] != j {
						t.Errorf("stopped job[%d] = %q, want %q", i, stopped[i], j)
					}
				}
				remainingPending := env.daemon.drainPendingNomadChanges()
				var gotRemainingCount int
				for _, evt := range remainingPending {
					gotRemainingCount += len(evt.Changes)
				}
				if gotRemainingCount != tc.wantRemainingPending {
					t.Fatalf("expected %d remaining pending changes after drain, got %d (%+v)", tc.wantRemainingPending, gotRemainingCount, remainingPending)
				}
			}
			if tc.wantCachedDigest != "" {
				cached := env.getDigest("core-job:" + tc.req.Image)
				if cached != tc.wantCachedDigest {
					t.Errorf("expected cached digest %q, got %q", tc.wantCachedDigest, cached)
				}
			}
			if tc.wantEvent != "" {
				select {
				case evt := <-env.evtCh:
					if evt.Event != tc.wantEvent {
						t.Errorf("expected %s, got %q", tc.wantEvent, evt.Event)
					}
					if evt.JobName != "brain" {
						t.Errorf("expected job_name brain, got %q", evt.JobName)
					}
					if evt.Repo != tc.req.Repo || evt.CommitSHA != tc.req.CommitSHA || evt.PRNumber != tc.req.PRNumber || evt.TargetID != tc.req.TargetID {
						t.Errorf("metadata missing in deploy_started: %+v", evt)
					}
					if evt.Digest != tc.req.Digest || evt.Image != tc.req.Image {
						t.Errorf("digest/image mismatch: %+v", evt)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("timed out waiting for %s", tc.wantEvent)
				}
				select {
				case evt := <-env.evtCh:
					t.Fatalf("unexpected followup event: %+v", evt)
				default:
				}
			}
			if tc.runErr != nil {
				var sawFailed bool
				deadline := time.After(2 * time.Second)
				for !sawFailed {
					select {
					case evt := <-env.evtCh:
						if evt.Event == "deploy_failed" {
							sawFailed = true
							if evt.Repo != tc.req.Repo || evt.CommitSHA != tc.req.CommitSHA || evt.PRNumber != tc.req.PRNumber || evt.TargetID != tc.req.TargetID {
								t.Errorf("metadata missing in deploy_failed event: %+v", evt)
							}
						}
					case <-deadline:
						t.Fatal("timed out waiting for deploy_failed")
					}
				}
			}
		})
	}
}

func TestImageRolloutQueue_TableDriven(t *testing.T) {
	t.Run("enqueue image ready event validation and queueing", func(t *testing.T) {
		jobContent := `job "debounced-test" {
			group "debounced-test" {
				task "debounced-test" {
					driver = "docker"
					config {
						image = "ghcr.io/azylman/debounced-app:latest"
					}
				}
			}
		}`
		env := newImageReadyTestEnv(t)
		env.writeJob("jobs", "debounced-test.nomad", jobContent)
		d := env.daemon

		cases := []struct {
			name       string
			req        ImageReadyEventRequest
			wantCode   int
			wantStatus string
			wantQueue  bool
		}{
			{
				name:       "empty image 400",
				req:        ImageReadyEventRequest{},
				wantCode:   http.StatusBadRequest,
				wantStatus: "error",
			},
			{
				name:       "unknown image 404",
				req:        ImageReadyEventRequest{Image: "ghcr.io/azylman/nonexistent:latest"},
				wantCode:   http.StatusNotFound,
				wantStatus: "not_found",
			},
			{
				name: "valid image 202 accepted and queued",
				req: ImageReadyEventRequest{
					Image:     "ghcr.io/azylman/debounced-app:latest",
					Repo:      "azylman/aerial",
					CommitSHA: "sha123",
					PRNumber:  555,
					TargetID:  "1555405874565091380",
				},
				wantCode:   http.StatusAccepted,
				wantStatus: "accepted",
				wantQueue:  true,
			},
		}

		for _, tc := range cases {
			resp, code := d.EnqueueImageReadyEvent(tc.req)
			if code != tc.wantCode || resp.Status != tc.wantStatus {
				t.Errorf("%s: EnqueueImageReadyEvent() = (%d, %s), want (%d, %s)", tc.name, code, resp.Status, tc.wantCode, tc.wantStatus)
			}
			if tc.wantQueue {
				if len(resp.MatchedJobs) != 1 || resp.MatchedJobs[0] != "debounced-test" {
					t.Errorf("expected matched job debounced-test, got %v", resp.MatchedJobs)
				}
				d.pendingImageMu.Lock()
				rollout, ok := d.pendingImageRollouts["debounced-test"]
				d.pendingImageMu.Unlock()
				if !ok {
					t.Fatalf("expected pendingImageRollouts to contain debounced-test")
				}
				if rollout.JobName != "debounced-test" || rollout.Request.CommitSHA != "sha123" {
					t.Errorf("unexpected rollout stored: %+v", rollout)
				}
			}
		}
	})

	t.Run("debounced loop execution synchronizes on channel without sleep", func(t *testing.T) {
		loopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		env := newImageReadyTestEnv(t)
		env.runSignal = make(chan string, 10)
		env.writeJob("jobs", "service-a.nomad", `job "service-a" { task "a" { config { image = "ghcr.io/azylman/service-a:latest" } } }`)
		env.writeJob("jobs", "service-b.nomad", `job "service-b" { task "b" { config { image = "ghcr.io/azylman/service-b:latest" } } }`)

		d := env.daemon
		d.imageRolloutDebounce = 20 * time.Millisecond
		d.StartImageRolloutLoop(loopCtx)

		_, codeA := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-a:latest",
			CommitSHA: "commit-alpha",
		})
		if codeA != http.StatusAccepted {
			t.Fatalf("expected 202 for service-a, got %d", codeA)
		}

		_, codeB := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-b:latest",
			CommitSHA: "commit-alpha",
		})
		if codeB != http.StatusAccepted {
			t.Fatalf("expected 202 for service-b, got %d", codeB)
		}

		var executed []string
		for i := 0; i < 2; i++ {
			select {
			case call := <-env.runSignal:
				executed = append(executed, call)
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for debounced run %d, got %v", i+1, executed)
			}
		}

		hasA := false
		hasB := false
		for _, call := range executed {
			if strings.Contains(call, "service-a.nomad") {
				hasA = true
			}
			if strings.Contains(call, "service-b.nomad") {
				hasB = true
			}
		}
		if !hasA || !hasB {
			t.Errorf("expected runs for both service-a and service-b, got %v", executed)
		}
	})

	t.Run("panic recovery in rollout loop", func(t *testing.T) {
		loopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var runCount int
		var runMu sync.Mutex
		panickedCh := make(chan struct{}, 1)
		recoveredCh := make(chan struct{}, 2)

		env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				runMu.Lock()
				runCount++
				count := runCount
				runMu.Unlock()
				if count == 1 {
					select {
					case panickedCh <- struct{}{}:
					default:
					}
					panic("simulated panic in rollout executor")
				}
				select {
				case recoveredCh <- struct{}{}:
				default:
				}
				return []byte("OK"), nil, nil
			}
		})

		env.writeJob("jobs", "service-panic.nomad", `job "service-panic" { task "p" { config { image = "ghcr.io/azylman/service-panic:latest" } } }`)

		d := env.daemon
		d.imageRolloutDebounce = 10 * time.Millisecond
		d.StartImageRolloutLoop(loopCtx)

		_, code1 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-panic:latest",
			CommitSHA: "panic-sha",
		})
		if code1 != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", code1)
		}

		select {
		case <-panickedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for first rollout panic")
		}

		_, code2 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-panic:latest",
			CommitSHA: "recovered-sha",
		})
		if code2 != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", code2)
		}

		select {
		case <-recoveredCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for second rollout execution after panic recovery")
		}

		runMu.Lock()
		finalCount := runCount
		runMu.Unlock()
		if finalCount < 2 {
			t.Errorf("expected at least 2 executor attempts (surviving panic), got %d", finalCount)
		}
	})

	t.Run("execute pending rollouts branches and hangar-last ordering", func(t *testing.T) {
		var mu sync.Mutex
		var executedJobs []string

		env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					jobPath := args[len(args)-1]
					body, _ := os.ReadFile(jobPath)
					jobName := ExtractJobName(string(body), filepath.Base(jobPath))
					mu.Lock()
					executedJobs = append(executedJobs, jobName)
					mu.Unlock()
				}
				return []byte("Evaluation ID: 12345"), nil, nil
			}
		})

		env.writeJob("jobs", "hangar.nomad", `job "hangar" { task "h" { config { image = "ghcr.io/azylman/aerial-hangar:latest" } } }`)
		env.writeJob("jobs", "webhooks-router.nomad", `job "webhooks-router" { task "r" { config { image = "ghcr.io/azylman/aerial-webhooks-router:latest" } } }`)

		d := env.daemon

		d.executePendingImageRollouts(context.Background())

		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()
		d.pendingImageMu.Lock()
		d.pendingImageRollouts["img1"] = PendingImageRollout{
			Request: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/aerial-brain:latest",
			},
		}
		d.pendingImageMu.Unlock()
		d.executePendingImageRollouts(cancelCtx)

		d.pendingImageMu.Lock()
		d.pendingImageRollouts["hangar"] = PendingImageRollout{
			JobName: "hangar",
			Request: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/aerial-hangar:latest",
			},
		}
		d.pendingImageMu.Unlock()
		d.pendingImageMu.Lock()
		d.pendingImageRollouts["webhooks-router"] = PendingImageRollout{
			JobName: "webhooks-router",
			Request: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/aerial-webhooks-router:latest",
			},
		}
		d.pendingImageMu.Unlock()

		d.executePendingImageRollouts(context.Background())

		mu.Lock()
		defer mu.Unlock()
		if len(executedJobs) != 2 {
			t.Fatalf("expected 2 executed jobs, got %v", executedJobs)
		}
		if executedJobs[0] != "webhooks-router" || executedJobs[1] != "hangar" {
			t.Fatalf("expected webhooks-router first and hangar last, got %v", executedJobs)
		}
	})
}
