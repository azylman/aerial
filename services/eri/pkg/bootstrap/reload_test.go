package bootstrap

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/env"
)

// blockingReloader records each reload on started and then blocks until
// release is closed.
type blockingReloader struct {
	started chan string
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingReloader) reload(source string) {
	b.calls.Add(1)
	b.started <- source
	<-b.release
}

func TestNewReloadTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		burst              int
		cancelDuringReload bool
		wantReloads        int32
	}{
		{name: "single trigger reloads once", burst: 0, wantReloads: 1},
		{name: "trigger during reload runs one follow-up", burst: 1, wantReloads: 2},
		{name: "burst during reload coalesces into one follow-up", burst: 5, wantReloads: 2},
		{name: "cancel during reload drops pending follow-up", burst: 3, cancelDuringReload: true, wantReloads: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rec := &blockingReloader{started: make(chan string, 8), release: make(chan struct{})}
			trigger := NewReloadTrigger(ctx, rec.reload)

			trigger.Trigger()
			if got := <-rec.started; got != "git-sync webhook" {
				t.Errorf("source = %q, want %q", got, "git-sync webhook")
			}
			for range tc.burst {
				trigger.Trigger()
			}
			if tc.cancelDuringReload {
				cancel()
			}
			close(rec.release)
			for range tc.wantReloads - 1 {
				<-rec.started
			}
			cancel()
			<-trigger.Done()
			// With the worker stopped, Trigger must neither block nor reload.
			trigger.Trigger()

			if got := rec.calls.Load(); got != tc.wantReloads {
				t.Errorf("reloads = %d, want %d", got, tc.wantReloads)
			}
		})
	}
}

func TestReloadTriggerTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilTrigger  bool
		prePending  bool
		wantPending int
	}{
		{name: "nil trigger is a no-op", nilTrigger: true},
		{name: "empty queue takes the request", wantPending: 1},
		{name: "pending request absorbs a second one", prePending: true, wantPending: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilTrigger {
				var trigger *ReloadTrigger
				trigger.Trigger()
				return
			}
			// No worker drains pending, so its length shows what was queued.
			trigger := &ReloadTrigger{pending: make(chan struct{}, 1)}
			if tc.prePending {
				trigger.pending <- struct{}{}
			}
			trigger.Trigger()
			if got := len(trigger.pending); got != tc.wantPending {
				t.Errorf("pending = %d, want %d", got, tc.wantPending)
			}
		})
	}
}

func TestGitSyncReloadHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prePending bool
	}{
		{name: "queues a reload", prePending: false},
		{name: "answers queued when a reload is already pending", prePending: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			trigger := &ReloadTrigger{pending: make(chan struct{}, 1)}
			if tc.prePending {
				trigger.pending <- struct{}{}
			}
			w := httptest.NewRecorder()
			GitSyncReloadHandler(trigger).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/-/reload", http.NoBody))
			if w.Code != http.StatusOK {
				t.Errorf("code = %d, want 200", w.Code)
			}
			if got := w.Body.String(); got != "{\"status\":\"queued\"}" {
				t.Errorf("body = %q", got)
			}
			if got := len(trigger.pending); got != 1 {
				t.Errorf("pending = %d, want 1", got)
			}
		})
	}
}

type fakeDirtyPool struct{ dirty atomic.Bool }

func (p *fakeDirtyPool) MarkDirty() { p.dirty.Store(true) }

// discordRecorder answers every Discord REST call with a stub message and
// keeps the request bodies.
type discordRecorder struct {
	mu     sync.Mutex
	bodies []string
}

func (d *discordRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	d.mu.Lock()
	d.bodies = append(d.bodies, string(body))
	d.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"1","channel_id":"123456789012345678"}`)),
		Request:    req,
	}, nil
}

func (d *discordRecorder) sent() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return strings.Join(d.bodies, "\n")
}

func withSupplier(fn func(candidate *config.Config) error) ReloadOption {
	return func(o *reloadConfigOptions) { o.reloadSupplier = fn }
}

var disabledCreds = config.CredentialSources{
	OAuth: config.CredentialSource{Disabled: true},
	Codex: config.CredentialSource{Disabled: true},
	Muse:  config.CredentialSource{Disabled: true},
}

func cfgData(model, channel string, creds config.CredentialSources) *config.ConfigData {
	return &config.ConfigData{Model: model, SystemChannel: channel, CredentialSources: creds}
}

func newProvisioner(t *testing.T, home string) *env.Provisioner {
	t.Helper()
	p, err := env.New(home, t.TempDir(), disabledCreds)
	if err != nil {
		t.Fatalf("env.New: %v", err)
	}
	return p
}

func TestCreateReloadConfigFunc(t *testing.T) {
	t.Parallel()

	const channel = "123456789012345678"
	// OAuth path on a disabled source fails credential validation, so Sync
	// errors before it provisions anything.
	invalidCreds := disabledCreds
	invalidCreds.OAuth.Path = "/fake/oauth"

	tests := []struct {
		name        string
		candidate   *config.ConfigData // nil = supplier rejects the candidate
		channel     string
		withSession bool
		prov        *env.Provisioner
		wantModel   string
		wantDirty   bool
		wantAlert   bool
	}{
		{name: "rejected candidate keeps lkgc and alerts", channel: channel, withSession: true, wantModel: "old-model", wantAlert: true},
		{name: "rejected candidate without session stays silent", channel: channel, wantModel: "old-model"},
		{name: "rejected candidate with unresolvable channel", withSession: true, wantModel: "old-model"},
		{name: "changed model marks pools dirty", candidate: cfgData("new-model", channel, disabledCreds), channel: channel, withSession: true, wantModel: "new-model", wantDirty: true},
		{name: "unchanged fingerprint preserves pools", candidate: cfgData("old-model", channel, disabledCreds), channel: channel, wantModel: "old-model"},
		{name: "idle provisioner syncs cleanly", candidate: cfgData("new-model", channel, disabledCreds), channel: channel, prov: newProvisioner(t, ""), wantModel: "new-model", wantDirty: true},
		{name: "provisioner sync error still publishes", candidate: cfgData("new-model", channel, invalidCreds), channel: channel, prov: newProvisioner(t, t.TempDir()), wantModel: "new-model", wantDirty: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.NewFromData(cfgData("old-model", tc.channel, disabledCreds))
			pool := &fakeDirtyPool{}
			rec := &discordRecorder{}
			var session *discordgo.Session
			if tc.withSession {
				s, err := discordgo.New("Bot test-token")
				if err != nil {
					t.Fatal(err)
				}
				s.Client = &http.Client{Transport: rec}
				session = s
			}
			var reloads atomic.Int32
			reload := CreateReloadConfigFunc(cfg,
				nil,
				WithDiscordSession(session),
				WithProvisioner(t.Context(), tc.prov),
				WithDirtyPools(pool, nil),
				WithOnReload(func(string) { reloads.Add(1) }),
				withSupplier(func(candidate *config.Config) error {
					if tc.candidate == nil {
						return errors.New("simulated invalid YAML")
					}
					next := *tc.candidate
					candidate.Update(&next)
					return nil
				}),
			)

			reload("test")
			if got := cfg.Current().Model; got != tc.wantModel {
				t.Errorf("model = %q, want %q", got, tc.wantModel)
			}
			if got := pool.dirty.Load(); got != tc.wantDirty {
				t.Errorf("dirty = %v, want %v", got, tc.wantDirty)
			}
			if got := strings.Contains(rec.sent(), "Invalid Configuration File"); got != tc.wantAlert {
				t.Errorf("alert sent = %v, want %v (bodies %q)", got, tc.wantAlert, rec.sent())
			}

			pool.dirty.Store(false)
			reload("test-repeat")
			if pool.dirty.Load() {
				t.Errorf("repeat reload with an unchanged fingerprint marked pools dirty")
			}
			if got := reloads.Load(); got != 2 {
				t.Errorf("onReload calls = %d, want 2", got)
			}
		})
	}
}

// Clears HOME, so it cannot run in parallel.
func TestEnsureConfigDefaults(t *testing.T) {
	testHome := filepath.Join(os.TempDir(), "aerial-test-gemini")
	testData := filepath.Join(os.TempDir(), "aerial-test-data")
	tests := []struct {
		name     string
		cfg      *config.Config
		wantHome string
		wantData string
	}{
		{name: "nil config is ignored", cfg: nil},
		{name: "empty dirs get defaults", cfg: config.NewFromData(&config.ConfigData{}), wantHome: testHome, wantData: testData},
		{name: "set dirs are kept", cfg: config.NewFromData(&config.ConfigData{GeminiHomeDir: "/h", DataDir: "/d"}), wantHome: "/h", wantData: "/d"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", "")
			ensureConfigDefaults(tc.cfg)
			if tc.cfg == nil {
				return
			}
			if got := tc.cfg.Current().GeminiHomeDir; got != tc.wantHome {
				t.Errorf("home = %q, want %q", got, tc.wantHome)
			}
			if got := tc.cfg.Current().DataDir; got != tc.wantData {
				t.Errorf("data = %q, want %q", got, tc.wantData)
			}
		})
	}
}

func fingerprintCfg(model, engine string) *config.Config {
	return config.NewFromData(&config.ConfigData{Model: model, LowEffortModel: "gemini-3.8-flash-low", Engine: engine, CredentialSources: disabledCreds})
}

func TestComputeDaemonReloadFingerprint(t *testing.T) {
	t.Parallel()

	baseFP := computeDaemonReloadFingerprint(fingerprintCfg("gemini-3.8-flash-low", "gemini"))
	if baseFP == "" {
		t.Fatal("expected non-empty base fingerprint")
	}

	tests := []struct {
		name      string
		cfg       *config.Config
		wantSame  bool
		wantEmpty bool
	}{
		{name: "nil config produces empty fingerprint", cfg: nil, wantEmpty: true},
		{name: "identical config produces identical fingerprint", cfg: fingerprintCfg("gemini-3.8-flash-low", "gemini"), wantSame: true},
		{name: "changed model produces different fingerprint", cfg: fingerprintCfg("gemini-2.5-pro", "gemini")},
		{name: "changed engine produces different fingerprint", cfg: fingerprintCfg("gemini-3.8-flash-low", "claude")},
		{name: "voice ambient context changes fingerprint", cfg: config.NewFromData(&config.ConfigData{Model: "gemini-3.8-flash-low", LowEffortModel: "gemini-3.8-flash-low", Engine: "gemini", CredentialSources: disabledCreds, Voice: config.VoiceConfig{AmbientContext: &config.AmbientContextConfig{Template: "hi"}}})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := computeDaemonReloadFingerprint(tc.cfg)
			if tc.wantEmpty {
				if fp != "" {
					t.Fatalf("expected empty fingerprint, got %q", fp)
				}
				return
			}
			if got := fp == baseFP; got != tc.wantSame {
				t.Fatalf("same as base = %v, want %v", got, tc.wantSame)
			}
		})
	}
}

// Sets CONFIG_PATH, so it cannot run in parallel.
func TestDefaultReloadSupplier(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantModel string
	}{
		{name: "valid file loads the candidate", yaml: "model: new-model\nchannels:\n  default: {mode: disabled}\n", wantModel: "new-model"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CONFIG_PATH", path)
			candidate := config.NewFromData(cfgData("old-model", "", disabledCreds))
			if err := defaultReloadSupplier(candidate); err != nil {
				t.Fatalf("defaultReloadSupplier: %v", err)
			}
			if got := candidate.Current().Model; got != tc.wantModel {
				t.Errorf("model = %q, want %q", got, tc.wantModel)
			}
		})
	}
}
