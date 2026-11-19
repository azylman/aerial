package transcript

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

func TestShouldRunTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.October, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		now      time.Time
		lastSync time.Time
		interval time.Duration
		expected bool
	}{
		{"interval zero", now, time.Time{}, 0, false},
		{"interval negative", now, time.Time{}, -time.Minute, false},
		{"lastSync zero runs immediately", now, time.Time{}, 5 * time.Minute, true},
		{"lastSync exactly interval elapsed", now, now.Add(-5 * time.Minute), 5 * time.Minute, true},
		{"lastSync more than interval elapsed", now, now.Add(-10 * time.Minute), 5 * time.Minute, true},
		{"lastSync less than interval elapsed", now, now.Add(-4 * time.Minute), 5 * time.Minute, false},
		{"lastSync in future", now, now.Add(1 * time.Minute), 5 * time.Minute, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ShouldRunTranscriptSync(tc.now, tc.lastSync, tc.interval)
			if got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestNewOllamaEmbedder_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cfg       *config.Config
		roots     []string
		wantNil   bool
		testInput string
	}{
		{"nil config returns nil", nil, nil, true, "hello"},
		{"valid config returns embedder func", config.NewFromData(&config.ConfigData{
			Ollama: config.OllamaConfig{BaseURL: "http://127.0.0.1:9", Model: "all-minilm"},
		}), []string{"/tmp/brain"}, false, "sample text"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fn := NewOllamaEmbedder(tc.cfg, tc.roots...)
			if (fn == nil) != tc.wantNil {
				t.Fatalf("expected fn nil=%v, got nil=%v", tc.wantNil, fn == nil)
			}
			if fn != nil {
				// Calling with mock unreachable server returns error without panic
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				_, _ = fn(ctx, tc.testInput)
			}
		})
	}
}

func TestRunTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	brainDir := filepath.Join(tempDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	homeDir := filepath.Join(tempDir, "home")
	geminiBrainDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
	_ = os.MkdirAll(geminiBrainDir, 0o755)

	syncedBrainDir := filepath.Join(tempDir, "synced-brain")
	_ = os.MkdirAll(syncedBrainDir, 0o755)
	writeTranscriptFile(t, syncedBrainDir, "sess-1", `{"step_index":0,"source":"USER","content":"hi"}`+"\n", time.Now().Add(-time.Hour))

	tests := []struct {
		name          string
		nilStore      bool
		brainDir      string
		useConfig     bool
		geminiHomeDir string
		withLLM       bool
		wantErr       bool
	}{
		{"nil store returns empty stats without error", true, brainDir, false, "", false, false},
		{"valid store with existing brainDir", false, brainDir, true, "", false, false},
		{"valid store with non-existent brainDir", false, filepath.Join(tempDir, "nonexistent"), true, "", false, true},
		{"empty brainDir derived from config DataDir", false, "", true, "", true, false},
		{"empty brainDir derived from config GeminiHomeDir", false, "", true, homeDir, false, false},
		{"valid store with transcripts synced", false, syncedBrainDir, true, "", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store db.TranscriptStore
			if !tc.nilStore {
				store = dbtest.NewFakeStore()
			}
			var cfg *config.Config
			if tc.useConfig {
				dataDir := tempDir
				if tc.geminiHomeDir != "" {
					dataDir = ""
				}
				cfg = config.NewFromData(&config.ConfigData{
					DataDir:        dataDir,
					GeminiHomeDir:  tc.geminiHomeDir,
					LowEffortModel: "gemini-3.8-flash-low",
					Ollama:         config.OllamaConfig{BaseURL: "http://127.0.0.1:9", Model: "all-minilm"},
				})
			}
			var llm LLMClientFunc
			if tc.withLLM {
				llm = func(ctx context.Context, prompt string) (string, error) {
					return "summary", nil
				}
			}
			stats, err := RunTranscriptSync(context.Background(), store, cfg, tc.brainDir, llm)
			if (err != nil) != tc.wantErr {
				t.Errorf("RunTranscriptSync error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.nilStore && (stats.Synced != 0 || stats.StepsInserted != 0) {
				t.Errorf("expected 0 stats on nil store, got %+v", stats)
			}
		})
	}
}

func TestConfiguredMCPServerNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cur  *config.ConfigData
		want []string
	}{
		{name: "nil snapshot", cur: nil, want: nil},
		{name: "no servers", cur: &config.ConfigData{}, want: nil},
		{
			name: "union across categories sorted and deduplicated",
			cur: &config.ConfigData{McpServers: config.TargetMcpConfig{
				Common:    map[string]json.RawMessage{"github": nil, "home_assistant": nil},
				Discord:   map[string]json.RawMessage{"discord": nil, "github": nil},
				Voice:     map[string]json.RawMessage{"home": nil},
				Ephemeral: map[string]json.RawMessage{"docket": nil},
			}},
			want: []string{"discord", "docket", "github", "home", "home_assistant"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := configuredMCPServerNames(tc.cur); !slices.Equal(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildSyncOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		cfg                  *config.Config
		brainDir             string
		wantClaudeDir        string
		wantCodexSessionsDir string
		wantMuseSessionsDir  string
	}{
		{
			name:                 "nil config",
			cfg:                  nil,
			brainDir:             "/tmp/brain",
			wantClaudeDir:        "",
			wantCodexSessionsDir: "",
			wantMuseSessionsDir:  "",
		},
		{
			name:                 "config with data dir",
			cfg:                  config.NewFromData(&config.ConfigData{DataDir: "/data/aerial"}),
			brainDir:             "/data/aerial/brain",
			wantClaudeDir:        "/data/aerial/claude/projects",
			wantCodexSessionsDir: "/data/aerial/codex/sessions",
			wantMuseSessionsDir:  "/data/aerial/muse/sessions",
		},
		{
			name:                 "config with home dir",
			cfg:                  config.NewFromData(&config.ConfigData{GeminiHomeDir: "/home/user"}),
			brainDir:             "/home/user/.gemini/antigravity-cli/brain",
			wantClaudeDir:        "/home/user/.claude/projects",
			wantCodexSessionsDir: "/home/user/.codex/sessions",
			wantMuseSessionsDir:  "/home/user/.local/share/muse/sessions",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := buildSyncOptions(tc.cfg, tc.brainDir, nil)
			if opts.ClaudeProjectsDir != tc.wantClaudeDir {
				t.Errorf("ClaudeProjectsDir = %q, want %q", opts.ClaudeProjectsDir, tc.wantClaudeDir)
			}
			if opts.CodexSessionsDir != tc.wantCodexSessionsDir {
				t.Errorf("CodexSessionsDir = %q, want %q", opts.CodexSessionsDir, tc.wantCodexSessionsDir)
			}
			if opts.MuseSessionsDir != tc.wantMuseSessionsDir {
				t.Errorf("MuseSessionsDir = %q, want %q", opts.MuseSessionsDir, tc.wantMuseSessionsDir)
			}
		})
	}
}
