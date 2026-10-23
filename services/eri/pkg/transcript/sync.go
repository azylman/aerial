package transcript

import (
	"context"
	"log"
	"path/filepath"
	"reflect"
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

// RunTranscriptSync executes a single synchronization sweep across brain session transcripts.
func RunTranscriptSync(ctx context.Context, store db.TranscriptStore, cfg *config.Config, brainDir string, llmFunc LLMClientFunc) (SyncStats, error) {
	if isNilStore(store) {
		return SyncStats{}, nil
	}

	targetBrainDir := strings.TrimSpace(brainDir)
	if targetBrainDir == "" && cfg != nil {
		if dataDir := strings.TrimSpace(cfg.DataDir()); dataDir != "" {
			targetBrainDir = filepath.Join(dataDir, "brain")
		} else if homeDir := strings.TrimSpace(cfg.GeminiHomeDir()); homeDir != "" {
			targetBrainDir = filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
		}
	}

	var embedder EmbedderFunc
	if cfg != nil {
		var roots []string
		if targetBrainDir != "" {
			roots = append(roots, targetBrainDir)
		}
		cur := cfg.Current()
		if cur != nil {
			_ = cur.Ollama.BaseURL
			_ = cur.Ollama.Model
			_ = cur.LowEffortModel
		}
		embedder = NewOllamaEmbedder(cfg, roots...)
	}

	syncCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	opts := DefaultSyncOptions()
	if targetBrainDir != "" {
		opts.BrainDir = targetBrainDir
	}

	stats, err := SyncTranscripts(syncCtx, store, targetBrainDir, embedder, llmFunc, opts)
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

// StartTranscriptSync starts a background goroutine running RunTranscriptSync immediately and on each ticker interval.
func StartTranscriptSync(ctx context.Context, store db.TranscriptStore, cfg *config.Config, brainDir string, llmFunc LLMClientFunc, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	go func() {
		var lastSync time.Time
		if ShouldRunTranscriptSync(time.Now(), lastSync, interval) {
			if _, err := RunTranscriptSync(ctx, store, cfg, brainDir, llmFunc); err != nil && ctx.Err() == nil {
				log.Printf("[TranscriptSync] Background sync startup error: %v", err)
			}
			lastSync = time.Now()
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if !ShouldRunTranscriptSync(now, lastSync, interval) {
					continue
				}
				if _, err := RunTranscriptSync(ctx, store, cfg, brainDir, llmFunc); err != nil && ctx.Err() == nil {
					log.Printf("[TranscriptSync] Background periodic sync error: %v", err)
				}
				lastSync = now
			}
		}
	}()
}
