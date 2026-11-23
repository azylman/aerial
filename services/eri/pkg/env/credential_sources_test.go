package env

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestRequiredCredentialSources(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sources    config.CredentialSources
		createFile bool
		wantErr    bool
		wantCause  error
	}{
		{"omitted", config.CredentialSources{}, false, true, nil},
		{"disabled", config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}}, false, false, nil},
		{"missing", config.CredentialSources{OAuth: config.CredentialSource{Path: t.TempDir() + "/missing"}, Codex: config.CredentialSource{Disabled: true}}, false, true, os.ErrNotExist},
		{"directory", config.CredentialSources{OAuth: config.CredentialSource{Path: t.TempDir()}, Codex: config.CredentialSource{Disabled: true}}, false, true, nil},
		{"muse path valid", config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}, Muse: config.CredentialSource{Path: "valid-muse-auth"}}, true, false, nil},
		{"muse path missing", config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}, Muse: config.CredentialSource{Path: t.TempDir() + "/missing-muse"}}, false, true, os.ErrNotExist},
		{"muse disabled", config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}, Muse: config.CredentialSource{Disabled: true}}, false, false, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sources := tc.sources
			if tc.createFile && sources.Muse.Path != "" {
				sources.Muse.Path = writeTestFile(t, filepath.Join(t.TempDir(), sources.Muse.Path), "test-token")
			}
			_, err := New(t.TempDir(), t.TempDir(), sources)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && err != nil {
				if err.Error() == "" {
					t.Errorf("expected non-empty error message")
				}
				if credErr, ok := errors.AsType[*CredentialError](err); ok {
					if credErr.Unwrap() == nil {
						t.Errorf("expected non-nil unwrapped error")
					}
				}
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Errorf("error=%v does not preserve cause %v", err, tc.wantCause)
			}
		})
	}
}

func disabledSources() config.CredentialSources {
	return config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}}
}

func newDisabledProvisioner(t *testing.T, home, data string) *Provisioner {
	t.Helper()
	p, err := New(home, data, disabledSources())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// The same provisioner must use accepted config snapshots without replacing proxy routes.
func TestSyncCredentialSnapshot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		disabled bool
		want     string
	}{
		{"new source applies", false, "fake-new-auth"},
		{"disabled preserves destination", true, "fake-refreshed-auth"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, _, aerialConfigDir := newTestProvisioner(t, map[string]string{"common/01_base.md": "test rule"}, nil)
			if aerialConfigDir == "" {
				t.Fatal("expected non-empty aerialConfigDir")
			}
			p.SetCustomSkillsDir(t.TempDir())
			p.SetSuperpowersDir(t.TempDir())
			p.SetAgentsSkillsDir(t.TempDir())
			p.SetHooksDir(t.TempDir())
			source := writeTestFile(t, filepath.Join(t.TempDir(), "auth"), "fake-new-auth")
			dest := writeTestFile(t, filepath.Join(p.dataDir, "codex", "auth.json"), "fake-refreshed-auth")
			cfg := newTestConfig(func(d *config.ConfigData) {
				d.GeminiHomeDir = p.homeDir
				d.DataDir = p.dataDir
				if !tc.disabled {
					d.CredentialSources.Codex = config.CredentialSource{Path: source}
				}
			})
			routes := NewMCPProxyRoutes("1234")
			p.WithMCPProxy(routes)
			if err := p.Sync(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dest)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("auth=%q want %q", got, tc.want)
			}
			if p.mcpProxy != routes {
				t.Fatal("proxy routes replaced")
			}
		})
	}
}
