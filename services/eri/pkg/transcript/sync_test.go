package transcript

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

type channelTranscriptStore struct {
	db.TranscriptStore

	notify chan struct{}
	err    error
}

func (s *channelTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	if s.notify != nil {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.TranscriptStore.GetSessionSyncStates(ctx)
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

func TestStartTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	brainDir := filepath.Join(tempDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	tests := []struct {
		name        string
		interval    time.Duration
		withError   bool
		expectTicks int
	}{
		{"custom short interval", 2 * time.Millisecond, false, 2},
		{"default fallback interval", 0, false, 1},
		{"startup and periodic sync error logging", 2 * time.Millisecond, true, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			notify := make(chan struct{}, 10)
			fakeStore := dbtest.NewFakeStore()
			var store db.TranscriptStore
			if tc.withError {
				store = &channelTranscriptStore{TranscriptStore: fakeStore, notify: notify, err: errors.New("simulated sync error")}
			} else {
				store = &channelTranscriptStore{TranscriptStore: fakeStore, notify: notify}
			}
			cfg := config.NewFromData(&config.ConfigData{DataDir: tempDir})
			StartTranscriptSync(ctx, store, cfg, brainDir, nil, tc.interval)
			for i := range tc.expectTicks {
				select {
				case <-notify:
				case <-time.After(2 * time.Second):
					t.Fatalf("timed out waiting for transcript sync tick %d", i+1)
				}
			}
		})
	}
}
