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
)

type LLMClientFunc func(ctx context.Context, prompt string) (string, error)

var (
	extractionMutex sync.Mutex
)

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

// BackfillMissingEmbeddings iterates through any facts with NULL or empty embedding BLOBs
// and generates vector embeddings via Ollama.
func BackfillMissingEmbeddings(ctx context.Context, database *sql.DB, client *Client) (int, error) {
	if database == nil || client == nil {
		return 0, fmt.Errorf("nil database or ollama client")
	}

	rows, err := database.QueryContext(ctx, "SELECT id, fact_text FROM facts WHERE embedding IS NULL OR length(embedding) = 0")
	if err != nil {
		return 0, fmt.Errorf("failed to query facts missing embeddings: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type missingFact struct {
		id   int64
		text string
	}
	var toUpdate []missingFact

	for rows.Next() {
		var mf missingFact
		if err := rows.Scan(&mf.id, &mf.text); err != nil {
			return 0, err
		}
		if strings.TrimSpace(mf.text) != "" {
			toUpdate = append(toUpdate, mf)
		}
	}

	if len(toUpdate) == 0 {
		return 0, nil
	}

	log.Printf("[Memory] Backfilling vector embeddings for %d legacy fact(s)...", len(toUpdate))
	backfilled := 0

	for _, item := range toUpdate {
		select {
		case <-ctx.Done():
			return backfilled, ctx.Err()
		default:
		}

		emb, err := client.GenerateEmbedding(ctx, item.text, false, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Failed to generate embedding for fact ID %d: %v", item.id, err)
			continue
		}
		if len(emb) == 0 {
			continue
		}

		embBytes := db.Float32ToBytes(emb)
		if _, err := database.ExecContext(ctx, "UPDATE facts SET embedding = ? WHERE id = ?", embBytes, item.id); err != nil {
			log.Printf("[Memory] Error updating embedding for fact ID %d: %v", item.id, err)
		} else {
			backfilled++
		}
	}

	if backfilled > 0 {
		log.Printf("[Memory] Successfully backfilled %d vector embedding(s)", backfilled)
	}
	return backfilled, nil
}

// ExtractActiveConversationFacts queries conversations modified in the last activeHours,
// extracts facts via the primary LLM, generates vector embeddings via Ollama, and stores them in SQLite.
// Single-flight protected via extractionMutex.
func ExtractActiveConversationFacts(ctx context.Context, database *sql.DB, client *Client, llmFunc LLMClientFunc, activeHours int) error {
	if database == nil || client == nil || llmFunc == nil {
		return fmt.Errorf("nil database, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Hourly fact extraction already running, skipping overlapping execution.")
		return nil
	}
	defer extractionMutex.Unlock()

	threadIDs, err := db.GetActiveConversationsForExtraction(database, activeHours)
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

func processThreadFacts(ctx context.Context, database *sql.DB, client *Client, llmFunc LLMClientFunc, threadID string) error {
	maxRowID, err := db.GetMaxMessageRowID(database, threadID)
	if err != nil {
		return fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, err)
	}

	transcript, err := loadThreadTranscript(database, threadID)
	if err != nil {
		return fmt.Errorf("transcript unavailable for thread %s: %w", threadID, err)
	}
	if strings.TrimSpace(transcript) == "" {
		log.Printf("[Memory] Empty transcript for thread %s, marking watermark.", threadID)
		_ = db.UpdateConversationFactWatermark(database, threadID, maxRowID)
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

	// Fetch existing facts for deduplication
	existingFacts, _ := db.GetFactsByThreadWithEmbeddings(database, threadID)

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

		// Semantic deduplication: Check if a similar fact already exists
		isDuplicate := false
		if len(emb) > 0 && len(existingFacts) > 0 {
			for _, ef := range existingFacts {
				if len(ef.Embedding) == len(emb) {
					sim := DotProduct(emb, ef.Embedding)
					if (ef.Fact.Category == item.Category && sim >= 0.88) || sim >= 0.90 {
						log.Printf("[Memory] Duplicate fact detected (%q ~ %q, sim=%.2f). Skipping duplicate row.",
							item.FactText, ef.Fact.FactText, sim)
						isDuplicate = true
						break
					}
				}
			}
		}

		if !isDuplicate {
			id, err := db.InsertFact(database, item.Category, item.FactText, item.Importance, threadID, emb)
			if err != nil {
				log.Printf("[Memory] Error inserting fact into DB: %v", err)
			} else {
				log.Printf("[Memory] Extracted and stored new fact [%s] (id=%d): %s", item.Category, id, item.FactText)
				existingFacts = append(existingFacts, db.FactWithEmbedding{
					Fact: db.Fact{
						ID:         id,
						Category:   item.Category,
						FactText:   item.FactText,
						Importance: item.Importance,
						ThreadID:   threadID,
						CreatedAt:  time.Now().UTC(),
					},
					Embedding: emb,
				})
			}
		}
	}

	return db.UpdateConversationFactWatermark(database, threadID, maxRowID)
}

func loadThreadTranscript(database *sql.DB, threadID string) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}

	roots := []string{
		"/data/brain",
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
	}

	idCandidates := []string{threadID}
	if database != nil {
		if sessID, err := db.GetSessionID(database, threadID); err == nil && sessID != "" {
			idCandidates = append([]string{sessID}, idCandidates...)
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
