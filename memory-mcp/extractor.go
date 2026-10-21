package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// LLMClientFunc defines the interface function for sending prompts to an LLM.
type LLMClientFunc func(ctx context.Context, prompt string) (string, error)

var extractionMutex sync.Mutex

// ExtractedFactItem represents a structured fact extracted by the LLM.
type ExtractedFactItem struct {
	Action       string  `json:"action,omitempty"`
	SupersedesID int64   `json:"supersedes_id,omitempty"`
	DeleteID     int64   `json:"delete_id,omitempty"`
	Category     string  `json:"category"`
	FactText     string  `json:"fact_text"`
	Importance   float64 `json:"importance_score"`
}

// ExtractedFactsPayload encapsulates the array of extracted facts.
type ExtractedFactsPayload struct {
	Facts []ExtractedFactItem `json:"facts"`
}

type brainLLMRequest struct {
	Prompt string `json:"prompt"`
}

type brainLLMResponse struct {
	Response string `json:"response"`
}

// HTTPBrainLLMClient returns an LLMClientFunc communicating with brain's internal LLM proxy.
func HTTPBrainLLMClient(brainLLMURL string, httpClient *http.Client) LLMClientFunc {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 120 * time.Second}
	}
	cleanURL := strings.TrimSpace(brainLLMURL)
	if cleanURL == "" {
		cleanURL = DefaultBrainLLMURL
	}
	return func(ctx context.Context, prompt string) (string, error) {
		reqBody, err := json.Marshal(brainLLMRequest{Prompt: prompt})
		if err != nil {
			return "", fmt.Errorf("failed to marshal brain LLM request: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cleanURL, bytes.NewReader(reqBody))
		if err != nil {
			return "", fmt.Errorf("failed to create brain LLM request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("brain LLM request failed: %w", err)
		}
		defer resp.Body.Close()

		respBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("failed to read brain LLM response body: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("brain LLM returned status %d: %s", resp.StatusCode, string(respBytes))
		}
		var llmResp brainLLMResponse
		if err := json.Unmarshal(respBytes, &llmResp); err != nil {
			return "", fmt.Errorf("failed to decode brain LLM response: %w", err)
		}
		return llmResp.Response, nil
	}
}

// FactExtractionPrompt is the base prompt template instructing the LLM to extract durable facts.
const FactExtractionPrompt = `Analyze the following conversation transcript and extract any key durable atomic facts, user preferences, long-term system configurations, or recurring household routines.

Requirements:
1. Output ONLY a valid JSON object matching this exact schema:
{
  "facts": [
    {
      "action": "ADD|UPDATE|DELETE|NOOP",
      "supersedes_id": 123,
      "delete_id": 456,
      "category": "user_preference|system_config|routine|general",
      "fact_text": "1-2 concise sentences stating the fact clearly",
      "importance_score": 0.6
    }
  ]
}
2. Actions:
- "ADD": New durable atomic fact not covered by existing facts.
- "UPDATE": Fact updates, modifies, or replaces an existing fact. Set "supersedes_id" to the existing fact's ID and provide the updated fact_text and importance_score.
- "DELETE": Fact explicitly invalidates or contradicts an existing fact that is no longer true. Set "delete_id" to the existing fact's ID.
- "NOOP": Information is already accurately captured by existing facts or is ephemeral.
3. Score importance_score on a 0.1 to 1.0 scale (0.8-1.0 for permanent user preferences/identity, 0.5-0.7 for durable system architecture/routines). Never extract ephemeral PR numbers, commit SHAs, branch names, coverage/LoC metrics, CI/CD deploy statuses, or Vanadis chunk IDs.
4. If no new facts or updates are needed, return {"facts": []}.
5. Do NOT include markdown text formatting outside the JSON block.
`

// BackfillMissingEmbeddings generates embeddings for legacy facts lacking vector representation.
func BackfillMissingEmbeddings(ctx context.Context, store Store, client OllamaClient) (int, error) {
	if store == nil || client == nil {
		return 0, fmt.Errorf("nil database or ollama client")
	}

	missingFacts, err := store.GetFactsMissingEmbeddings(ctx, 0)
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

		if err := store.UpdateFactEmbedding(ctx, item.ID, emb); err != nil {
			log.Printf("[Memory] Error updating embedding for fact ID %d: %v", item.ID, err)
		} else {
			backfilled++
		}
	}
	return backfilled, nil
}

// ExtractActiveConversationFacts queries conversations modified in the last activeHours,
// extracts facts via the primary LLM, generates vector embeddings via Ollama, and reconciles them into persistent DB.
// Single-flight protected via extractionMutex.
func ExtractActiveConversationFacts(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, activeHours int) error {
	if store == nil || client == nil || llmFunc == nil {
		return fmt.Errorf("nil database, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Hourly fact extraction already running, skipping overlapping execution.")
		return nil
	}
	defer extractionMutex.Unlock()

	threadIDs, err := store.GetActiveConversationsForExtraction(ctx, activeHours)
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

		if err := processThreadFacts(ctx, store, client, llmFunc, tid); err != nil {
			log.Printf("[Memory] Error extracting facts for thread %s: %v", tid, err)
		}
	}

	return nil
}

func processThreadFacts(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, threadID string) error {
	if store == nil {
		return fmt.Errorf("database is nil")
	}

	maxRowID, err := store.GetMaxMessageRowID(ctx, threadID)
	if err != nil {
		return fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, err)
	}

	turns, err := store.GetUnextractedAdminTurns(ctx, threadID)
	if err != nil {
		return fmt.Errorf("failed to get unextracted turns for thread %s: %w", threadID, err)
	}
	for _, t := range turns {
		if t.RowID > maxRowID {
			maxRowID = t.RowID
		}
	}

	transcript := formatExtractableTurns(turns)
	if strings.TrimSpace(transcript) == "" {
		log.Printf("[Memory] Empty extractable turns for thread %s, marking watermark.", threadID)
		if err := store.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
			log.Printf("[Memory] Warning updating conversation fact watermark for empty transcript (thread=%s): %v", threadID, err)
		}
		return nil
	}

	// Fetch up to 10 nearest existing facts to provide context for ADD / UPDATE / DELETE reconciliation
	embSnippet := transcript
	if len(embSnippet) > 2000 {
		embSnippet = embSnippet[:2000]
	}

	var existingFacts []Fact
	transcriptEmb, embErr := client.GenerateEmbedding(ctx, embSnippet, true, 1)
	if embErr == nil && len(transcriptEmb) == ExpectedEmbeddingDim {
		facts, findErr := store.FindNearestFacts(ctx, transcriptEmb, 10, 0.50)
		if findErr == nil {
			existingFacts = facts
		}
	}

	prompt := FactExtractionPrompt
	if len(existingFacts) > 0 {
		var b strings.Builder
		b.WriteString("\nEXISTING FACTS (id, category, fact_text):\n")
		for _, f := range existingFacts {
			b.WriteString(fmt.Sprintf("- [%d] [%s] %s\n", f.ID, f.Category, f.FactText))
		}
		prompt += b.String()
	}
	prompt += "\nTRANSCRIPT:\n" + transcript

	respText, err := llmFunc(ctx, prompt)
	if err != nil {
		return fmt.Errorf("LLM fact extraction call failed: %w", err)
	}

	factsPayload, err := parseFactsJSON(respText)
	if err != nil {
		return fmt.Errorf("failed to parse extracted facts JSON: %w", err)
	}

	for _, item := range factsPayload.Facts {
		reconcileExtractedFact(ctx, store, client, item)
	}

	if err := store.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
		log.Printf("[Memory] Warning updating conversation fact watermark (thread=%s): %v", threadID, err)
	}
	return nil
}

func reconcileExtractedFact(ctx context.Context, store Store, client OllamaClient, item ExtractedFactItem) {
	action := strings.ToUpper(strings.TrimSpace(item.Action))
	if action == "NOOP" {
		return
	}

	// 1. DELETE
	if action == "DELETE" || item.DeleteID > 0 {
		targetID := item.DeleteID
		if targetID <= 0 {
			targetID = item.SupersedesID
		}
		if targetID > 0 {
			if err := store.DeleteFact(ctx, targetID); err != nil {
				log.Printf("[Memory] Error deleting fact %d: %v", targetID, err)
			} else {
				log.Printf("[Memory] Deleted obsolete fact ID %d", targetID)
			}
		}
		return
	}

	// 2. UPDATE
	if action == "UPDATE" || item.SupersedesID > 0 {
		targetID := item.SupersedesID
		if targetID > 0 {
			if strings.TrimSpace(item.FactText) == "" {
				return
			}
			importance := item.Importance
			if importance <= 0 {
				importance = 0.60
			}
			category := strings.TrimSpace(item.Category)
			if category == "" {
				category = "general"
			}
			emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
			if err != nil {
				log.Printf("[Memory] Warning: Failed to generate embedding for updated fact: %v", err)
				return
			}
			err = store.UpdateFact(ctx, targetID, category, item.FactText, importance, emb)
			if errors.Is(err, ErrFactNotFound) {
				if _, iErr := store.InsertFact(ctx, category, item.FactText, importance, emb); iErr != nil {
					log.Printf("[Memory] Error inserting fallback fact: %v", iErr)
				} else {
					log.Printf("[Memory] Fact ID %d not found to update; inserted new fact [%s]: %s", targetID, category, item.FactText)
				}
			} else if err != nil {
				log.Printf("[Memory] Error updating fact %d: %v", targetID, err)
			} else {
				log.Printf("[Memory] Updated fact ID %d [%s]: %s", targetID, category, item.FactText)
			}
			return
		}
	}

	// 3. ADD (or default)
	if strings.TrimSpace(item.FactText) == "" {
		return
	}
	importance := item.Importance
	if importance <= 0 {
		importance = 0.60
	}
	category := strings.TrimSpace(item.Category)
	if category == "" {
		category = "general"
	}

	emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to generate embedding for fact '%s': %v", item.FactText, err)
		return
	}

	var dupFact *Fact
	var sim float64
	if len(emb) == ExpectedEmbeddingDim {
		dupFact, sim, err = store.FindDuplicateFact(ctx, emb, 0.85)
		if err != nil {
			log.Printf("[Memory] Warning: FindDuplicateFact check failed: %v", err)
		}
	}

	if dupFact != nil {
		log.Printf("[Memory] Duplicate fact detected (id=%d, sim=%.2f: %q ~ %q). Reinforcing fact.",
			dupFact.ID, sim, item.FactText, dupFact.FactText)
		if rErr := store.ReinforceFact(ctx, dupFact.ID, item.FactText, emb, 0.15); rErr != nil {
			log.Printf("[Memory] Error reinforcing fact id=%d: %v", dupFact.ID, rErr)
		}
	} else {
		id, err := store.InsertFact(ctx, category, item.FactText, importance, emb)
		if err != nil {
			log.Printf("[Memory] Error inserting fact into DB: %v", err)
		} else {
			log.Printf("[Memory] Extracted and stored new fact [%s] (id=%d): %s", category, id, item.FactText)
		}
	}
}

func formatExtractableTurns(turns []Message) string {
	var b strings.Builder
	for _, m := range turns {
		c := strings.TrimSpace(m.Content)
		r := strings.TrimSpace(m.ResponseText)
		if c == "" && r == "" {
			continue
		}
		b.WriteString("User: ")
		b.WriteString(c)
		b.WriteString("\nAssistant: ")
		b.WriteString(r)
		b.WriteString("\n")
	}
	text := b.String()
	if len(text) > 20000 {
		text = text[len(text)-20000:]
		if idx := strings.Index(text, "\n"); idx != -1 && idx < len(text)-1 {
			text = text[idx+1:]
		}
	}
	return text
}

func parseFactsJSON(raw string) (*ExtractedFactsPayload, error) {
	clean := strings.TrimSpace(raw)
	if clean == "" {
		return nil, fmt.Errorf("empty raw facts payload")
	}

	firstBrace := strings.Index(clean, "{")
	if firstBrace == -1 {
		return nil, fmt.Errorf("no json object found in raw output")
	}

	var payload ExtractedFactsPayload
	dec := json.NewDecoder(strings.NewReader(clean[firstBrace:]))
	if err := dec.Decode(&payload); err != nil {
		return nil, err
	}
	return &payload, nil
}
