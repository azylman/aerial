package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"sort"
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
      "category": "user_preference|system_config|routine|general",
      "fact_text": "1-2 concise sentences stating the fact clearly",
      "importance_score": 0.6
    }
  ]
}
2. Extract only durable, atomic facts not ephemeral details.
3. Score importance_score on a 0.1 to 1.0 scale (0.8-1.0 for permanent user preferences/identity, 0.5-0.7 for durable system architecture/routines). Never extract ephemeral PR numbers, commit SHAs, branch names, coverage/LoC metrics, CI/CD deploy statuses, or Vanadis chunk IDs.
4. If no durable facts are present in the conversation, return {"facts": []}.
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

var (
	durableDirectiveRegex   = regexp.MustCompile(`(?i)\b(always|never|strictly|must|invariant|prefers|allergic)\b`)
	ephemeralTelemetryRegex = regexp.MustCompile(`(?i)\b(PR\s*#\d+|pull request #\d+|chunk-\d+|build-\d+-[0-9a-f]+|vanadis_chunk|commit\s+[0-9a-f]{7,40}|auto-merge armed)\b`)
)

// EvaluateCandidateAdmission deterministically evaluates an extracted fact candidate.
func EvaluateCandidateAdmission(item ExtractedFactItem) (admit bool, adjustedImportance float64, reason string) {
	action := strings.ToUpper(strings.TrimSpace(item.Action))
	if action == "NOOP" {
		return false, 0, "noop_action"
	}
	if action == "DELETE" {
		return false, 0, "delete_action"
	}

	trimmedText := strings.TrimSpace(item.FactText)
	if len(trimmedText) < 10 {
		return false, 0, "too_short"
	}

	isDurable := durableDirectiveRegex.MatchString(trimmedText)
	if ephemeralTelemetryRegex.MatchString(trimmedText) && !isDurable {
		return false, 0, "ephemeral_telemetry"
	}

	importance := item.Importance
	if importance <= 0 {
		importance = 0.60
	}

	cleanCategory := strings.ToLower(strings.TrimSpace(item.Category))
	if cleanCategory == "user_preference" || isDurable {
		importance += 0.10
	}

	if importance < 0.10 {
		importance = 0.10
	} else if importance > 1.0 {
		importance = 1.0
	}

	return true, importance, "admitted"
}

func buildCandidateReconcilePrompt(item ExtractedFactItem, neighbors []ScoredFact) string {
	var b strings.Builder
	b.WriteString(`You are a memory reconciliation classifier.
Decide how to reconcile a new candidate fact against existing related facts in memory.

CANDIDATE FACT:
Category: `)
	b.WriteString(item.Category)
	b.WriteString(`
Fact: `)
	b.WriteString(item.FactText)
	b.WriteString(`
Importance: `)
	b.WriteString(fmt.Sprintf("%.2f", item.Importance))
	b.WriteString(`

EXISTING CANDIDATE NEIGHBORS (id, similarity, category, text):
`)
	for _, n := range neighbors {
		b.WriteString(fmt.Sprintf("- ID %d (similarity %.2f) [%s]: %s\n", n.Fact.ID, n.Score, n.Fact.Category, n.Fact.FactText))
	}
	b.WriteString(`
Requirements:
1. Output ONLY a valid JSON object matching this schema:
{
  "facts": [
    {
      "action": "ADD|UPDATE|DELETE|NOOP",
      "supersedes_id": 123,
      "delete_id": 456,
      "category": "user_preference|system_config|routine|general",
      "fact_text": "Fact text",
      "importance_score": 0.6
    }
  ]
}
2. Actions:
- "ADD": Candidate is a distinct fact. Does not replace any neighbor.
- "UPDATE": Candidate modifies or updates a neighbor. Set "supersedes_id" to that neighbor's ID.
- "DELETE": Candidate invalidates a neighbor that is no longer true. Set "delete_id" to that neighbor's ID.
- "NOOP": Candidate is already accurately captured or obsolete.
3. Only reference neighbor IDs explicitly listed above.
4. Output nothing else outside the JSON object.`)
	return b.String()
}

func processThreadFacts(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, threadID string) error {
	if store == nil {
		return fmt.Errorf("database is nil")
	}

	turns, err := store.GetUnextractedAdminTurns(ctx, threadID)
	if err != nil {
		return fmt.Errorf("failed to get unextracted turns for thread %s: %w", threadID, err)
	}

	var maxRowID int64
	for _, t := range turns {
		if t.RowID > maxRowID {
			maxRowID = t.RowID
		}
	}
	if maxRowID == 0 {
		maxRowID, err = store.GetMaxMessageRowID(ctx, threadID)
		if err != nil {
			return fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, err)
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

	priorSummary, priorTurns, ctxErr := store.GetThreadContextBeforeWatermark(ctx, threadID, 2)
	if ctxErr != nil {
		log.Printf("[Memory] Warning: failed to get thread context before watermark (thread=%s): %v", threadID, ctxErr)
		priorSummary = ""
		priorTurns = nil
	}

	var b strings.Builder
	b.WriteString(FactExtractionPrompt)
	b.WriteString("\nInstructions for Coreference Resolution:\nResolve any pronouns (he, she, they, it) or relative references in new turns into standalone atomic facts using prior context.\n")
	if trimmedSummary := strings.TrimSpace(priorSummary); trimmedSummary != "" {
		b.WriteString("\nPRIOR THREAD SUMMARY:\n")
		b.WriteString(trimmedSummary)
		b.WriteString("\n")
	}
	if len(priorTurns) > 0 {
		formattedPrior := formatExtractableTurns(priorTurns)
		if strings.TrimSpace(formattedPrior) != "" {
			b.WriteString("\nPRIOR TURNS (FOR COREFERENCE RESOLUTION ONLY - DO NOT EXTRACT FACTS FROM PRIOR TURNS):\n")
			b.WriteString(formattedPrior)
		}
	}
	b.WriteString("\nNEW UNEXTRACTED TURNS:\n")
	b.WriteString(transcript)
	prompt := b.String()

	respText, err := llmFunc(ctx, prompt)
	if err != nil {
		return fmt.Errorf("LLM fact extraction call failed: %w", err)
	}

	factsPayload, err := parseFactsJSON(respText)
	if err != nil {
		return fmt.Errorf("failed to parse extracted facts JSON: %w", err)
	}

	candidates := factsPayload.Facts
	if len(candidates) > 5 {
		candidates = candidates[:5]
	}

	for _, item := range candidates {
		reconcileCandidateFact(ctx, store, client, llmFunc, item)
	}

	if err := store.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
		log.Printf("[Memory] Warning updating conversation fact watermark (thread=%s): %v", threadID, err)
	}
	return nil
}

func reconcileCandidateFact(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, item ExtractedFactItem) {
	if store == nil || client == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(item.Action), "DELETE") {
		return
	}
	admit, adjustedImportance, _ := EvaluateCandidateAdmission(item)
	if !admit {
		return
	}
	item.Importance = adjustedImportance
	category := strings.TrimSpace(item.Category)
	if category == "" {
		category = "general"
	}

	emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to generate embedding for candidate fact %q: %v", item.FactText, err)
		return
	}

	neighbors, err := store.FindNearestScoredFacts(ctx, emb, 5, 0.60)
	if err != nil {
		log.Printf("[Memory] Warning: FindNearestScoredFacts error for %q: %v", item.FactText, err)
		return
	}

	// Tier 1: len(neighbors) == 0 (maxSim < 0.60)
	if len(neighbors) == 0 {
		if _, err := store.InsertFact(ctx, category, item.FactText, item.Importance, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 1 fact: %v", err)
		}
		return
	}

	// Tier 2: neighbors[0].Score > 0.88
	if neighbors[0].Score > 0.88 {
		if err := store.ReinforceFact(ctx, neighbors[0].Fact.ID, item.FactText, emb, 0.15); err != nil {
			log.Printf("[Memory] Error reinforcing Tier 2 fact id=%d: %v", neighbors[0].Fact.ID, err)
		}
		return
	}

	// Tier 3: 0.60 <= neighbors[0].Score <= 0.88
	if llmFunc == nil {
		if _, err := store.InsertFact(ctx, category, item.FactText, item.Importance, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 3 fallback fact: %v", err)
		}
		return
	}

	reconcilePrompt := buildCandidateReconcilePrompt(item, neighbors)
	resp, err := llmFunc(ctx, reconcilePrompt)
	if err != nil {
		log.Printf("[Memory] Tier 3 reconciliation LLM error: %v", err)
		if _, err := store.InsertFact(ctx, category, item.FactText, item.Importance, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 3 fallback fact on LLM error: %v", err)
		}
		return
	}

	decisionPayload, err := parseFactsJSON(resp)
	if err != nil || len(decisionPayload.Facts) == 0 {
		log.Printf("[Memory] Tier 3 parse error (%v); falling back to ADD", err)
		if _, err := store.InsertFact(ctx, category, item.FactText, item.Importance, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 3 fallback fact on parse error: %v", err)
		}
		return
	}

	dec := decisionPayload.Facts[0]
	validIDs := make(map[int64]bool, len(neighbors))
	for _, n := range neighbors {
		validIDs[n.Fact.ID] = true
	}

	action := strings.ToUpper(strings.TrimSpace(dec.Action))
	switch action {
	case "DELETE":
		targetID := dec.DeleteID
		if targetID <= 0 {
			targetID = dec.SupersedesID
		}
		if validIDs[targetID] {
			if err := store.DeleteFact(ctx, targetID); err != nil {
				log.Printf("[Memory] Error deleting fact %d: %v", targetID, err)
			}
		} else {
			log.Printf("[Memory] Tier 3 delete rejected: ID %d not in whitelisted neighbors (NOOP)", targetID)
		}
	case "UPDATE":
		targetID := dec.SupersedesID
		if targetID <= 0 {
			targetID = dec.DeleteID
		}
		if validIDs[targetID] {
			factText := dec.FactText
			if strings.TrimSpace(factText) == "" {
				factText = item.FactText
			}
			decCat := strings.TrimSpace(dec.Category)
			if decCat == "" {
				decCat = category
			}
			decImp := dec.Importance
			if decImp <= 0 {
				decImp = item.Importance
			}
			if err := store.UpdateFact(ctx, targetID, decCat, factText, decImp, emb); err != nil {
				log.Printf("[Memory] Error updating fact %d: %v", targetID, err)
			}
		} else {
			log.Printf("[Memory] Tier 3 update rejected: ID %d not in whitelisted neighbors; falling back to ADD", targetID)
			if _, err := store.InsertFact(ctx, category, item.FactText, item.Importance, emb); err != nil {
				log.Printf("[Memory] Error inserting fallback fact: %v", err)
			}
		}
	case "NOOP":
		// Do nothing
	default: // "ADD" or fallback
		if _, err := store.InsertFact(ctx, category, item.FactText, item.Importance, emb); err != nil {
			log.Printf("[Memory] Error inserting fact: %v", err)
		}
	}
}

func reconcileExtractedFact(ctx context.Context, store Store, client OllamaClient, item ExtractedFactItem) {
	reconcileCandidateFact(ctx, store, client, nil, item)
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

// FindConsolidationCliques groups facts by category and finds complete-linkage cliques of size [minSize, maxSize]
// where every pair of facts (u, v) in the clique satisfies minSim <= CosineSimilarity(u.Embedding, v.Embedding) <= maxSim.
func FindConsolidationCliques(facts []ScoredFactWithEmbedding, minSim, maxSim float64, minSize, maxSize int) [][]ScoredFactWithEmbedding {
	if minSim <= 0 {
		minSim = 0.70
	}
	if maxSim <= 0 {
		maxSim = 0.88
	}
	if minSize <= 0 {
		minSize = 3
	}
	if maxSize < minSize {
		maxSize = 6
		if maxSize < minSize {
			maxSize = minSize
		}
	}

	catGroups := make(map[string][]ScoredFactWithEmbedding)
	for _, f := range facts {
		if len(f.Embedding) == 0 {
			continue
		}
		cat := strings.TrimSpace(f.Fact.Category)
		if cat == "" {
			cat = "general"
		}
		catGroups[cat] = append(catGroups[cat], f)
	}

	cats := make([]string, 0, len(catGroups))
	for c := range catGroups {
		cats = append(cats, c)
	}
	sort.Strings(cats)

	var cliques [][]ScoredFactWithEmbedding

	for _, cat := range cats {
		group := catGroups[cat]
		sort.Slice(group, func(i, j int) bool {
			return group[i].Fact.ID < group[j].Fact.ID
		})

		used := make(map[int64]bool, len(group))
		for i := 0; i < len(group); i++ {
			if used[group[i].Fact.ID] {
				continue
			}
			clique := []ScoredFactWithEmbedding{group[i]}
			for j := i + 1; j < len(group); j++ {
				if used[group[j].Fact.ID] {
					continue
				}
				if len(clique) >= maxSize {
					break
				}
				cand := group[j]
				matchesAll := true
				for _, member := range clique {
					sim := CosineSimilarity(member.Embedding, cand.Embedding)
					if sim < minSim || sim > maxSim {
						matchesAll = false
						break
					}
				}
				if matchesAll {
					clique = append(clique, cand)
				}
			}
			if len(clique) >= minSize {
				for _, member := range clique {
					used[member.Fact.ID] = true
				}
				cliques = append(cliques, clique)
			}
		}
	}

	return cliques
}

// FactConsolidationPrompt defines the system prompt instructing the LLM to consolidate related facts.
const FactConsolidationPrompt = `You are a memory consolidation engine.
Synthesize the provided cluster of semantically related facts into 1 or at most 2 dense, canonical, and durable facts that preserve all specific information, preferences, constraints, and context without redundancy.

Output strictly a JSON object matching this schema:
{
  "facts": [
    {
      "category": "category_name",
      "fact_text": "Consolidated canonical fact text.",
      "importance_score": 0.8
    }
  ]
}
Rules:
1. Produce at most 2 consolidated facts.
2. Output nothing else outside the JSON object.`

func buildConsolidationPrompt(category string, clique []ScoredFactWithEmbedding) string {
	var b strings.Builder
	b.WriteString(FactConsolidationPrompt)
	b.WriteString(fmt.Sprintf("\n\nCATEGORY: %s\nCLUSTER FACTS TO CONSOLIDATE:\n", category))
	for _, item := range clique {
		b.WriteString(fmt.Sprintf("- (ID: %d, Importance: %.2f) %s\n", item.Fact.ID, item.Fact.Importance, item.Fact.FactText))
	}
	return b.String()
}

// ConsolidateFactClusters discovers semantically tight complete-linkage cliques of facts and synthesizes them into canonical facts.
// Single-flight protected via extractionMutex.
func ConsolidateFactClusters(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc) (int, error) {
	if store == nil || client == nil || llmFunc == nil {
		return 0, fmt.Errorf("nil store, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Fact extraction or dreaming already running, skipping overlapping consolidation.")
		return 0, nil
	}
	defer extractionMutex.Unlock()

	allFacts, err := store.GetAllFactsWithEmbeddings(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get facts with embeddings: %w", err)
	}

	if len(allFacts) == 0 {
		return 0, nil
	}

	cliques := FindConsolidationCliques(allFacts, 0.70, 0.88, 3, 6)
	if len(cliques) == 0 {
		return 0, nil
	}

	log.Printf("[Memory] Consolidating %d candidate fact cluster(s)...", len(cliques))
	var consolidatedCount int

	for _, clique := range cliques {
		select {
		case <-ctx.Done():
			return consolidatedCount, ctx.Err()
		default:
		}

		maxImportance := 0.0
		category := clique[0].Fact.Category
		for _, f := range clique {
			if f.Fact.Importance > maxImportance {
				maxImportance = f.Fact.Importance
			}
		}
		if maxImportance <= 0 {
			maxImportance = 0.70
		}

		prompt := buildConsolidationPrompt(category, clique)
		resp, err := llmFunc(ctx, prompt)
		if err != nil {
			log.Printf("[Memory] Warning: Fact consolidation LLM synthesis failed for cluster in category %q: %v", category, err)
			continue
		}

		payload, err := parseFactsJSON(resp)
		if err != nil || len(payload.Facts) == 0 {
			log.Printf("[Memory] Warning: Fact consolidation payload parse failed: %v", err)
			continue
		}

		candidates := payload.Facts
		if len(candidates) > 2 {
			candidates = candidates[:2]
		}

		var inserted int
		for _, c := range candidates {
			text := strings.TrimSpace(c.FactText)
			if text == "" {
				continue
			}
			cat := strings.TrimSpace(c.Category)
			if cat == "" {
				cat = category
			}
			imp := c.Importance
			if imp <= 0 || imp < maxImportance {
				imp = maxImportance
			}
			if imp > 1.0 {
				imp = 1.0
			}

			emb, err := client.GenerateEmbedding(ctx, text, false, 1)
			if err != nil {
				log.Printf("[Memory] Warning: Failed to generate embedding for consolidated fact %q: %v", text, err)
				continue
			}
			if len(emb) == 0 {
				continue
			}

			_, err = store.InsertFact(ctx, cat, text, imp, emb)
			if err != nil {
				log.Printf("[Memory] Warning: Failed to insert consolidated fact: %v", err)
				continue
			}
			inserted++
		}

		if inserted > 0 {
			for _, src := range clique {
				if err := store.DeleteFact(ctx, src.Fact.ID); err != nil {
					log.Printf("[Memory] Warning: Failed to delete consolidated source fact ID %d: %v", src.Fact.ID, err)
				}
			}
			consolidatedCount++
		}
	}

	return consolidatedCount, nil
}
