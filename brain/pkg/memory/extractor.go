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
	transcript, err := loadThreadTranscript(threadID)
	if err != nil || strings.TrimSpace(transcript) == "" {
		// Mark extracted so we don't repeatedly try missing transcripts
		_ = db.UpdateConversationFactExtractedAt(database, threadID)
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

		_, err = db.InsertFact(database, item.Category, item.FactText, item.Importance, threadID, emb)
		if err != nil {
			log.Printf("[Memory] Error inserting fact into DB: %v", err)
		}
	}

	return db.UpdateConversationFactExtractedAt(database, threadID)
}

func loadThreadTranscript(threadID string) (string, error) {
	// Transcript log path
	logPath := filepath.Join("/root/.gemini/antigravity-cli/brain", threadID, ".system_generated", "logs", "transcript.jsonl")
	data, err := os.ReadFile(logPath)
	if err != nil {
		return "", err
	}
	text := string(data)
	if len(text) > 20000 {
		text = text[len(text)-20000:]
	}
	return text, nil
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
