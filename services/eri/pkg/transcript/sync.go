package transcript

import (
	"context"
	"encoding/json"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

func isNilStore(s db.TranscriptStore) bool {
	if s == nil {
		return true
	}
	val := reflect.ValueOf(s)
	return val.Kind() == reflect.Pointer && val.IsNil()
}

func resolveTargetBrainDir(cfg *config.Config, brainDir string) string {
	targetBrainDir := strings.TrimSpace(brainDir)
	if targetBrainDir == "" && cfg != nil {
		if dataDir := strings.TrimSpace(cfg.DataDir()); dataDir != "" {
			targetBrainDir = filepath.Join(dataDir, "brain")
		} else if homeDir := strings.TrimSpace(cfg.GeminiHomeDir()); homeDir != "" {
			targetBrainDir = filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
		}
	}
	return targetBrainDir
}

func buildSyncOptions(cfg *config.Config, targetBrainDir string, llmFunc LLMClientFunc) SyncOptions {
	opts := DefaultSyncOptions()
	if targetBrainDir != "" {
		opts.BrainDir = targetBrainDir
	}
	if cfg != nil {
		if dataDir := strings.TrimSpace(cfg.DataDir()); dataDir != "" {
			opts.ClaudeProjectsDir = filepath.Join(dataDir, "claude", "projects")
			opts.CodexSessionsDir = filepath.Join(dataDir, "codex", "sessions")
			opts.MuseSessionsDir = filepath.Join(dataDir, "muse", "sessions")
		} else if homeDir := strings.TrimSpace(cfg.GeminiHomeDir()); homeDir != "" {
			opts.ClaudeProjectsDir = filepath.Join(homeDir, ".claude", "projects")
			opts.CodexSessionsDir = filepath.Join(homeDir, ".codex", "sessions")
			opts.MuseSessionsDir = filepath.Join(homeDir, ".local", "share", "muse", "sessions")
		}
		opts.MCPServers = configuredMCPServerNames(cfg.Current())
		var roots []string
		if targetBrainDir != "" {
			roots = append(roots, targetBrainDir)
		}
		opts.Embedder = NewOllamaEmbedder(cfg, roots...)
	}
	opts.LLMFunc = llmFunc
	return opts
}

// ShouldRunTranscriptSync returns true if interval > 0 and either lastSync is zero or the elapsed time >= interval.
func ShouldRunTranscriptSync(now, lastSync time.Time, interval time.Duration) bool {
	return interval > 0 && (lastSync.IsZero() || now.Sub(lastSync) >= interval)
}

// NewOllamaEmbedder constructs an EmbedderFunc backed by Ollama using the provided configuration.
func NewOllamaEmbedder(cfg *config.Config, roots ...string) EmbedderFunc {
	if cfg == nil {
		return nil
	}
	client := NewEmbedder(cfg, roots...)
	return func(ctx context.Context, text string) ([]float32, error) {
		return client.GenerateEmbedding(ctx, text, false, 1)
	}
}

// configuredMCPServerNames returns the sorted, de-duplicated MCP server names across every
// mcp_servers category, used to split agy mcp_<server>_<tool> tool names.
func configuredMCPServerNames(cur *config.ConfigData) []string {
	if cur == nil {
		return nil
	}
	var names []string
	for _, group := range []map[string]json.RawMessage{cur.McpServers.Common, cur.McpServers.Discord, cur.McpServers.Voice, cur.McpServers.Ephemeral} {
		for name := range group {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// RunTranscriptSync executes a single synchronization sweep across brain session transcripts.
func RunTranscriptSync(ctx context.Context, store db.TranscriptStore, cfg *config.Config, brainDir string, llmFunc LLMClientFunc) (SyncStats, error) {
	if isNilStore(store) {
		return SyncStats{}, nil
	}

	targetBrainDir := resolveTargetBrainDir(cfg, brainDir)

	syncCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	opts := buildSyncOptions(cfg, targetBrainDir, llmFunc)

	stats, err := SyncTranscripts(syncCtx, store, targetBrainDir, opts.Embedder, llmFunc, opts)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[TranscriptSync] Sync error: %v", err)
		}
		return stats, err
	}
	if stats.Synced > 0 || stats.StepsInserted > 0 {
		log.Printf("[TranscriptSync] Sync complete: %d sessions synced, %d steps inserted, %d summaries generated (%v)",
			stats.Synced, stats.StepsInserted, stats.SummariesGenerated, stats.Duration)
	}
	return stats, nil
}
