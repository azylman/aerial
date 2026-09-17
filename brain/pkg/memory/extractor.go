package memory

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
)

type LLMClientFunc func(ctx context.Context, prompt string) (string, error)

var extractionMutex sync.Mutex

type ExtractedFactItem struct {
	Category   string  `json:"category"`
	FactText   string  `json:"fact_text"`
	Importance float64 `json:"importance_score"`
}

type ExtractedFactsPayload struct {
	Facts []ExtractedFactItem `json:"facts"`
}

const FactExtractionPrompt = `Analyze the following conversation transcript and extract any key atomic facts, user preferences, system configurations, recurring routines, or persistent operational states.

Requirements:
1. Output ONLY a valid JSON object matching this exact schema:
{
  "facts": [
    {
      "category": "user_preference|system_config|routine|general",
      "fact_text": "1-2 concise sentences stating the fact clearly",
      "importance_score": 1.0
    }
  ]
}
2. If no new important facts are present in the transcript, return {"facts": []}.
3. Do NOT include markdown text formatting outside the JSON block.

TRANSCRIPT:
`

func BackfillMissingEmbeddings(ctx context.Context, database any, client *Client) (int, error) {
	var factStore db.FactStore
	switch v := database.(type) {
	case db.FactStore:
		factStore = v
	case *sql.DB:
		if v != nil {
			factStore = db.NewSQLStore(v)
		}
	}
	if factStore == nil || client == nil {
		return 0, fmt.Errorf("nil database or ollama client")
	}

	missingFacts, err := factStore.GetFactsMissingEmbeddings(ctx, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to query facts missing embeddings: %w", err)
	}

	if len(missingFacts) == 0 {
		return 0, nil
	}

	log.Printf("[Memory] Backfilling vector embeddings for %d legacy fact(s)...", len(missingFacts))
	backfilled := 0

	for _, item := range missingFacts {
		select {
		case <-ctx.Done():
			return backfilled, ctx.Err()
		default:
		}

		if strings.TrimSpace(item.FactText) == "" {
			continue
		}

		emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Failed to generate embedding for fact ID %d: %v", item.ID, err)
			continue
		}
		if len(emb) == 0 {
			continue
		}

		if err := factStore.UpdateFactEmbedding(ctx, item.ID, emb); err != nil {
			log.Printf("[Memory] Error updating embedding for fact ID %d: %v", item.ID, err)
		} else {
			backfilled++
		}
	}
	return backfilled, nil
}

// ExtractActiveConversationFacts queries conversations modified in the last activeHours,
// extracts facts via the primary LLM, generates vector embeddings via Ollama, and stores them in persistent DB.
// Single-flight protected via extractionMutex.
func ExtractActiveConversationFacts(ctx context.Context, database any, client *Client, llmFunc LLMClientFunc, activeHours int) error {
	var factStore db.FactStore
	switch v := database.(type) {
	case db.FactStore:
		factStore = v
	case *sql.DB:
		if v != nil {
			factStore = db.NewSQLStore(v)
		}
	}
	if factStore == nil || client == nil || llmFunc == nil {
		return fmt.Errorf("nil database, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Hourly fact extraction already running, skipping overlapping execution.")
		return nil
	}
	defer extractionMutex.Unlock()

	threadIDs, err := factStore.GetActiveConversationsForExtraction(ctx, activeHours)
	if err != nil {
		return fmt.Errorf("failed to get active conversations: %w", err)
	}

	if len(threadIDs) == 0 {
		log.Printf("[Memory] No active conversations requiring fact extraction.")
		return nil
	}

	log.Printf("[Memory] Starting hourly fact extraction for %d active conversation threads...", len(threadIDs))

	for _, tid := range threadIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := processThreadFacts(ctx, database, client, llmFunc, tid); err != nil {
			log.Printf("[Memory] Error extracting facts for thread %s: %v", tid, err)
		}
	}

	return nil
}

func processThreadFacts(ctx context.Context, database any, client *Client, llmFunc LLMClientFunc, threadID string) (err error) {
	var factStore db.FactStore
	switch v := database.(type) {
	case db.FactStore:
		factStore = v
	case *sql.DB:
		if v != nil {
			factStore = db.NewSQLStore(v)
		}
	}
	if factStore == nil {
		return fmt.Errorf("database is nil")
	}
	start := time.Now()
	var factCount int
	defer func() {
		status := "extracted"
		if err != nil {
			status = "error"
		} else if factCount == 0 {
			status = "empty"
		}
		metrics.RecordFactExtraction(status, time.Since(start))
	}()

	var maxRowID int64
	if ms, ok := factStore.(db.MessageStore); ok {
		maxRowID, err = ms.GetMaxMessageRowID(ctx, threadID)
	}
	if err != nil {
		return fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, err)
	}

	transcript, err := loadThreadTranscript(database, client, threadID)
	if err != nil {
		return fmt.Errorf("transcript unavailable for thread %s: %w", threadID, err)
	}
	if strings.TrimSpace(transcript) == "" {
		log.Printf("[Memory] Empty transcript for thread %s, marking watermark.", threadID)
		if err := factStore.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
			log.Printf("[Memory] Warning updating conversation fact watermark for empty transcript (thread=%s): %v", threadID, err)
		}
		return nil
	}

	prompt := FactExtractionPrompt + transcript
	respText, err := llmFunc(ctx, prompt)
	if err != nil {
		return fmt.Errorf("LLM fact extraction call failed: %w", err)
	}

	factsPayload, err := parseFactsJSON(respText)
	if err != nil {
		return fmt.Errorf("failed to parse extracted facts JSON: %w", err)
	}

	factCount = len(factsPayload.Facts)

	for _, item := range factsPayload.Facts {
		if strings.TrimSpace(item.FactText) == "" {
			continue
		}

		// Generate embedding for document text (isQuery = false)
		emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Failed to generate embedding for fact '%s': %v", item.FactText, err)
			continue
		}

		// Global semantic deduplication: check across the whole database
		var dupFact *db.Fact
		var sim float64
		if len(emb) == db.ExpectedEmbeddingDim {
			dupFact, sim, err = factStore.FindDuplicateFact(ctx, emb, 0.88)
			if err != nil {
				log.Printf("[Memory] Warning: FindDuplicateFact check failed: %v", err)
			}
		}

		if dupFact != nil {
			log.Printf("[Memory] Duplicate fact detected (id=%d, sim=%.2f: %q ~ %q). Reinforcing fact.",
				dupFact.ID, sim, item.FactText, dupFact.FactText)
			metrics.MemoryOperationsTotal.WithLabelValues("extract", "reinforced").Inc()
			if rErr := factStore.ReinforceFact(ctx, dupFact.ID, item.FactText, emb, 0.15); rErr != nil {
				log.Printf("[Memory] Error reinforcing fact id=%d: %v", dupFact.ID, rErr)
			}
		} else {
			id, err := factStore.InsertFact(ctx, item.Category, item.FactText, item.Importance, threadID, emb)
			if err != nil {
				metrics.MemoryOperationsTotal.WithLabelValues("extract", "error").Inc()
				log.Printf("[Memory] Error inserting fact into DB: %v", err)
			} else {
				metrics.MemoryOperationsTotal.WithLabelValues("extract", "stored").Inc()
				log.Printf("[Memory] Extracted and stored new fact [%s] (id=%d): %s", item.Category, id, item.FactText)
			}
		}
	}

	if err := factStore.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
		log.Printf("[Memory] Warning updating conversation fact watermark (thread=%s): %v", threadID, err)
	}
	if err := factStore.UpdateConversationFactExtractedAt(ctx, threadID); err != nil {
		log.Printf("[Memory] Warning updating conversation fact extracted_at (thread=%s): %v", threadID, err)
	}
	return nil
}

func loadThreadTranscript(database any, client *Client, threadID string) (string, error) {
	var roots []string
	if client != nil {
		roots = client.Roots()
	}

	if len(roots) == 0 {
		return "", nil
	}

	idCandidates := []string{threadID}
	if database != nil {
		var sessStore db.SessionStore
		switch v := database.(type) {
		case db.SessionStore:
			sessStore = v
		case *sql.DB:
			if v != nil {
				sessStore = db.NewSQLStore(v)
			}
		}
		if sessStore != nil {
			if sessID, err := sessStore.GetSessionID(context.Background(), threadID); err == nil && sessID != "" {
				idCandidates = append([]string{sessID}, idCandidates...)
			}
		}
	}

	for _, root := range roots {
		for _, id := range idCandidates {
			for _, file := range []string{"transcript_full.jsonl", "transcript.jsonl"} {
				p := filepath.Join(root, id, ".system_generated", "logs", file)
				data, err := os.ReadFile(p)
				if err == nil && len(data) > 0 {
					text := string(data)
					if len(text) > 20000 {
						text = text[len(text)-20000:]
						if idx := strings.Index(text, "\n"); idx != -1 && idx < len(text)-1 {
							text = text[idx+1:]
						}
					}
					return text, nil
				}
			}
		}
	}

	return "", fmt.Errorf("transcript file not found for thread %s", threadID)
}

func parseFactsJSON(raw string) (*ExtractedFactsPayload, error) {
	clean := strings.TrimSpace(raw)
	if idx := strings.Index(clean, "{"); idx != -1 {
		if lastIdx := strings.LastIndex(clean, "}"); lastIdx != -1 && lastIdx > idx {
			clean = clean[idx : lastIdx+1]
		}
	}

	var payload ExtractedFactsPayload
	if err := json.Unmarshal([]byte(clean), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}
