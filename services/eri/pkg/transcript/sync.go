package transcript

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
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
		} else if homeDir := strings.TrimSpace(cfg.GeminiHomeDir()); homeDir != "" {
			opts.ClaudeProjectsDir = filepath.Join(homeDir, ".claude", "projects")
		}
		opts.MCPServers = configuredMCPServerNames(cfg.Current())
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

	opts := buildSyncOptions(cfg, targetBrainDir, llmFunc)
	opts.Embedder = embedder

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

// HandleTurnCompletedTranscriptSync processes an incoming TurnCompletedEvent on aerial.turn.completed,
// indexes that session's transcript file into Postgres transcript_steps, and unconditionally emits
// a TranscriptSyncedEvent to aerial.turn.transcript_synced.
func HandleTurnCompletedTranscriptSync(
	ctx context.Context,
	msg pubsub.Message,
	bus pubsub.Bus,
	store db.TranscriptStore,
	cfg *config.Config,
	brainDir string,
	getLLM func() LLMClientFunc,
) error {
	if msg == nil {
		return nil
	}

	var ev pubsub.TurnCompletedEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[TranscriptSync] Warning unmarshaling turn completed event: %v", err)
		return nil
	}

	sessionID := strings.TrimSpace(ev.SessionID)
	conversationID := strings.TrimSpace(ev.ConversationID)
	if conversationID == "" || sessionID == "healthcheck-ping" || sessionID == "" {
		return nil
	}

	targetBrainDir := resolveTargetBrainDir(cfg, brainDir)
	var llmFunc LLMClientFunc
	if getLLM != nil {
		llmFunc = getLLM()
	}
	opts := buildSyncOptions(cfg, targetBrainDir, llmFunc)

	var stats SyncStats
	if !isNilStore(store) {
		var syncErr error
		stats, syncErr = SyncSessionTranscript(ctx, store, targetBrainDir, sessionID, opts)
		if syncErr != nil && !os.IsNotExist(syncErr) && !errors.Is(syncErr, os.ErrNotExist) {
			log.Printf("[TranscriptSync] Warning syncing session transcript for %s: %v", sessionID, syncErr)
		}
	}

	if bus != nil {
		syncedEv := pubsub.TranscriptSyncedEvent{
			SessionID:      sessionID,
			ConversationID: conversationID,
			StepsInserted:  stats.StepsInserted,
			SyncedAt:       time.Now().UTC(),
		}
		msgID := fmt.Sprintf("transcript-synced:%s:%d", sessionID, time.Now().UnixNano())
		if err := bus.Publish(ctx, pubsub.SubjectAerialTurnTranscriptSynced, msgID, syncedEv); err != nil {
			log.Printf("[TranscriptSync] Error publishing transcript synced event for session %s: %v", sessionID, err)
			return err
		}
	}
	return nil
}

// StartTurnTranscriptSyncSubscriber starts a durable queue consumer on aerial.turn.completed named "eri-transcript-sync".
func StartTurnTranscriptSyncSubscriber(
	ctx context.Context,
	bus pubsub.Bus,
	store db.TranscriptStore,
	cfg *config.Config,
	brainDir string,
	getLLM func() LLMClientFunc,
) error {
	if bus == nil {
		return errors.New("transcript: bus is nil")
	}
	_, err := bus.ConsumeQueue(ctx, pubsub.SubjectAerialTurnCompleted, "eri-transcript-sync", func(hCtx context.Context, msg pubsub.Message) error {
		return HandleTurnCompletedTranscriptSync(hCtx, msg, bus, store, cfg, brainDir, getLLM)
	})
	return err
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
