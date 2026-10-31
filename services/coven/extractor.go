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
	Action       string `json:"action,omitempty"`
	SupersedesID int64  `json:"supersedes_id,omitempty"`
	DeleteID     int64  `json:"delete_id,omitempty"`
	FactText     string `json:"fact_text"`
}

// ExtractedProceduralItem represents an extracted procedural candidate workflow or guardrail.
type ExtractedProceduralItem struct {
	Domain      string   `json:"domain,omitempty"`
	LoadedSkill string   `json:"loaded_skill,omitempty"`
	Trigger     string   `json:"trigger"`
	Steps       []string `json:"steps,omitempty"`
	Avoid       []string `json:"avoid,omitempty"`
}

// ExtractedFactsPayload encapsulates the array of extracted facts and procedural candidates.
type ExtractedFactsPayload struct {
	Facts                []ExtractedFactItem       `json:"facts"`
	ReinforcedIDs        []int64                   `json:"reinforced_ids,omitempty"`
	ProceduralCandidates []ExtractedProceduralItem `json:"procedural_candidates,omitempty"`
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
      "fact_text": "A single concise sentence stating one atomic proposition clearly"
    }
  ],
  "reinforced_ids": [42],
  "procedural_candidates": [
    {
      "domain": "discord",
      "loaded_skill": "name-of-loaded-skill-or-empty",
      "trigger": "A clear description of when this procedural skill should activate",
      "steps": ["Step 1 description", "Step 2 description"],
      "avoid": ["Pitfall or failure mode to avoid"]
    }
  ]
}
2. Atomicity: Extract only single-proposition atomic facts (one concise sentence and one topic/predicate per fact; never combine distinct topics such as family composition and dietary restrictions into a single compound fact).
3. 30-Day Durability & Settled-State Test: Extract ONLY facts that will remain true and actionable 30+ days from now without requiring a follow-up event. Extract settled end-states (permanent hardware owned, standing preferences, architectural invariants, recurring schedules), NEVER transient in-flight states or plaintext credentials:
   - Never extract pending deliveries, shipping/order statuses, or items "awaiting delivery" / "on the way".
   - Never extract temporary troubleshooting, active debugging sessions, or tasks "currently in progress" / "about to run".
   - Never extract ephemeral PR numbers, commit SHAs, branch names, coverage/LoC metrics, CI/CD deploy statuses, or Vanadis chunk IDs.
   - Never extract literal plaintext passwords, passphrases, API keys, secret tokens, private keys, or PIN codes (only record where credentials are stored, such as a secret manager or vault).
4. Procedural Skills & Friction Guardrails (30-Day Durability Test):
   Extract reusable procedural workflows ("steps") and tool/retry failure guardrails ("avoid") into "procedural_candidates" ONLY when they pass the same 30-day durability test (Rule 3) AND a working resolution was verified:
   - Tool or CLI friction (including 1-liner flag/environment workarounds or multi-step domain workflows) occurs AND the turn resolved it with a generalizable procedure or workaround that will remain valid 30+ days from now (never emit tool/CLI friction workarounds into "facts"; short 1-liners accumulate in "procedural_candidates" until 5 repeats trigger a fix alert).
   - User gives durable procedural corrections or instructions on how to execute a domain workflow or existing skill.
   - Never extract unresolved dead-ends, ephemeral PR/commit/chunk IDs, or negative tool claims ("tool X is broken") without a verified working alternative; format "avoid" items as an imperative rule plus one clause of why.
   If a skill was loaded for this turn, set "loaded_skill" to that skill name; otherwise set it to "".
   If no procedural friction or reusable workflows are present, set "procedural_candidates": [].
5. If retrieved memory facts with [id:N] directly informed the assistant's response or tool calls in this turn, include their integer IDs in "reinforced_ids". If none were used, set "reinforced_ids": [].
6. If no durable facts are present in the conversation, return {"facts": []}.
7. Do NOT include markdown text formatting outside the JSON block.
`

// ExtractActiveConversationFacts queries conversations modified in the last activeHours,
// extracts facts via the primary LLM, generates vector embeddings via Ollama, and reconciles them into persistent DB.
// Single-flight protected via extractionMutex.
func ExtractActiveConversationFacts(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, activeHours int) error {
	start := time.Now()
	if store == nil || client == nil || llmFunc == nil {
		RecordOperation("extract", "error", time.Since(start).Seconds())
		return fmt.Errorf("nil database, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Hourly fact extraction already running, skipping overlapping execution.")
		RecordOperation("extract", "success", time.Since(start).Seconds())
		return nil
	}
	defer extractionMutex.Unlock()

	threadIDs, err := store.GetActiveConversationsForExtraction(ctx, activeHours)
	if err != nil {
		RecordOperation("extract", "error", time.Since(start).Seconds())
		return fmt.Errorf("failed to get active conversations: %w", err)
	}

	if len(threadIDs) == 0 {
		log.Printf("[Memory] No active conversations requiring fact extraction.")
		RecordOperation("extract", "success", time.Since(start).Seconds())
		return nil
	}

	log.Printf("[Memory] Starting hourly fact extraction for %d active conversation threads...", len(threadIDs))

	for _, tid := range threadIDs {
		select {
		case <-ctx.Done():
			RecordOperation("extract", "error", time.Since(start).Seconds())
			return ctx.Err()
		default:
		}

		if err := processThreadFacts(ctx, store, client, llmFunc, tid); err != nil {
			log.Printf("[Memory] Error extracting facts for thread %s: %v", tid, err)
		}
	}

	RecordOperation("extract", "success", time.Since(start).Seconds())
	return nil
}

var (
	durableDirectiveRegex    = regexp.MustCompile(`(?i)\b(always|never|strictly|must|invariant|prefers|allergic)\b`)
	ephemeralTelemetryRegex  = regexp.MustCompile(`(?i)\b(PR\s*#\d+|pull request #\d+|chunk-\d+|build-\d+-[0-9a-f]+|vanadis_chunk|commit\s+[0-9a-f]{7,40}|auto-merge armed|awaiting delivery|currently in progress)\b`)
	plaintextCredentialRegex = regexp.MustCompile(`(?i)\b(ghp_[a-z0-9]{20,}|github_pat_[a-z0-9_]{20,}|sk-[a-z0-9_-]{16,}|xox[baprs]-[a-z0-9-]{10,}|AIza[0-9a-z_-]{30,})\b|-----BEGIN [A-Z ]*PRIVATE KEY-----|(?:\bpin(?:\s+code)?\b[^.;,\n]{0,60}?\b(?:is|equals|=|:)\s*['"\x60]?\d{4,8}\b)|(?:\b(?:password|passphrase|api[_ -]?key|secret[_ -]?key|access[_ -]?token|auth[_ -]?token|client[_ -]?secret)\b[^.;,\n]{0,80}?\b(?:is|equals|=|:)\s*['"\x60]?([^\s'"\x60.;,]{4,}))`)
	nonSecretVerbs           = regexp.MustCompile(`(?i)^(stored|injected|managed|configured|retrieved|loaded|passed|provided|kept|saved|located|fetched|synced|rotated|encrypted|hashed|masked|redacted|required|disabled|enabled|empty|missing|invalid|valid|null|none|never|shared|used|defined|generated|scoped)$`)
)

// EvaluateCandidateAdmission deterministically evaluates an extracted fact candidate.
func EvaluateCandidateAdmission(item ExtractedFactItem) (admit bool, reason string) {
	action := strings.ToUpper(strings.TrimSpace(item.Action))
	if action == "NOOP" {
		return false, "noop_action"
	}
	if action == "DELETE" {
		return false, "delete_action"
	}

	trimmedText := strings.TrimSpace(item.FactText)
	if len(trimmedText) < 10 {
		return false, "too_short"
	}

	for _, m := range plaintextCredentialRegex.FindAllStringSubmatch(trimmedText, -1) {
		if m[2] == "" || !nonSecretVerbs.MatchString(m[2]) {
			return false, "plaintext_credential"
		}
	}

	isDurable := durableDirectiveRegex.MatchString(trimmedText)
	if ephemeralTelemetryRegex.MatchString(trimmedText) && !isDurable {
		return false, "ephemeral_telemetry"
	}

	return true, "admitted"
}

// EvaluateProceduralCandidateAdmission determines whether an extracted procedural candidate should be admitted.
func EvaluateProceduralCandidateAdmission(item ExtractedProceduralItem) (admit bool, reason string) {
	trimmedTrigger := strings.TrimSpace(item.Trigger)
	if trimmedTrigger == "" {
		return false, "empty_trigger"
	}
	if len(trimmedTrigger) < 5 {
		return false, "trigger_too_short"
	}
	if ephemeralTelemetryRegex.MatchString(trimmedTrigger) && !durableDirectiveRegex.MatchString(trimmedTrigger) {
		return false, "ephemeral_telemetry"
	}

	validSteps := false
	for _, s := range item.Steps {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			if ephemeralTelemetryRegex.MatchString(trimmed) && !durableDirectiveRegex.MatchString(trimmed) {
				return false, "ephemeral_telemetry"
			}
			validSteps = true
		}
	}
	validAvoid := false
	for _, a := range item.Avoid {
		if trimmed := strings.TrimSpace(a); trimmed != "" {
			if ephemeralTelemetryRegex.MatchString(trimmed) && !durableDirectiveRegex.MatchString(trimmed) {
				return false, "ephemeral_telemetry"
			}
			validAvoid = true
		}
	}
	if !validSteps && !validAvoid {
		return false, "missing_steps_and_avoid"
	}

	return true, "admitted"
}

// FormatProceduralCandidateEmbeddingText builds the canonical action-space text (Steps + Avoid, falling back to Trigger) for embedding procedural candidates.
func FormatProceduralCandidateEmbeddingText(item ExtractedProceduralItem) string {
	var parts []string
	for _, grp := range []struct {
		prefix string
		items  []string
	}{{"Steps: ", item.Steps}, {"Avoid: ", item.Avoid}} {
		var clean []string
		for _, s := range grp.items {
			if t := strings.TrimSpace(s); t != "" {
				clean = append(clean, t)
			}
		}
		if len(clean) > 0 {
			parts = append(parts, grp.prefix+strings.Join(clean, "; "))
		}
	}
	if len(parts) == 0 {
		return strings.TrimSpace(item.Trigger)
	}
	return strings.Join(parts, "\n")
}

func buildCandidateReconcilePrompt(item ExtractedFactItem, neighbors []ScoredFact) string {
	var b strings.Builder
	b.WriteString(`You are a memory reconciliation classifier.
Decide how to reconcile a new candidate fact against existing related facts in memory.

CANDIDATE FACT:
Fact: `)
	b.WriteString(item.FactText)
	b.WriteString(`

EXISTING CANDIDATE NEIGHBORS (id, similarity, text):
`)
	for _, n := range neighbors {
		b.WriteString(fmt.Sprintf("- ID %d (similarity %.2f): %s\n", n.Fact.ID, n.Score, n.Fact.FactText))
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
      "fact_text": "Fact text"
    }
  ]
}
2. Actions:
- "ADD": Candidate is a distinct fact or covers a different topic/predicate from existing neighbors. Does not replace any neighbor.
- "UPDATE": Candidate modifies or refines the exact same topic/predicate as a neighbor. Set "supersedes_id" to that neighbor's ID. Never merge distinct topics that merely share a subject into a multi-sentence compound fact; use "ADD" instead.
- "DELETE": Candidate invalidates a neighbor that is no longer true. Set "delete_id" to that neighbor's ID.
- "NOOP": Candidate is already accurately captured or corroborated by an existing neighbor. Set "supersedes_id" to that neighbor's ID.
3. Only reference neighbor IDs explicitly listed above.
4. Output nothing else outside the JSON object.`)
	return b.String()
}

func resolveThreadTurnWatermark(ctx context.Context, store Store, threadID string, turns []Message) (int64, error) {
	var maxRowID int64
	for _, t := range turns {
		if t.RowID > maxRowID {
			maxRowID = t.RowID
		}
	}
	if maxRowID == 0 {
		var err error
		maxRowID, err = store.GetMaxMessageRowID(ctx, threadID)
		if err != nil {
			return 0, fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, err)
		}
	}
	return maxRowID, nil
}

func buildFactExtractionPrompt(ctx context.Context, store Store, threadID, transcript string) string {
	priorSummary, priorTurns, ctxErr := store.GetThreadContextBeforeWatermark(ctx, threadID, 2)
	if ctxErr != nil {
		log.Printf("[Memory] Warning: failed to get thread context before watermark (thread=%s): %v", threadID, ctxErr)
		priorSummary = ""
		priorTurns = nil
	}

	retrievedFacts, toolCalls, trajErr := store.GetThreadTrajectoryContext(ctx, threadID)
	if trajErr != nil {
		log.Printf("[Memory] Warning: failed to get thread trajectory context (thread=%s): %v", threadID, trajErr)
		retrievedFacts = nil
		toolCalls = nil
	}
	loadedSkills := ExtractLoadedSkillsFromCalls(toolCalls)

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
	if len(retrievedFacts) > 0 {
		b.WriteString("\nRETRIEVED MEMORY IN TURN:\n")
		for _, rf := range retrievedFacts {
			b.WriteString(rf)
			b.WriteString("\n")
		}
	}
	if len(loadedSkills) > 0 {
		b.WriteString("\nLOADED SKILLS IN TURN:\n")
		for _, ls := range loadedSkills {
			b.WriteString("- ")
			b.WriteString(ls)
			b.WriteString("\n")
		}
	}
	if len(toolCalls) > 0 {
		b.WriteString("\nTOOL CALLS IN TURN:\n")
		for _, tc := range toolCalls {
			b.WriteString(tc)
			b.WriteString("\n")
		}
	}
	b.WriteString("\nNEW UNEXTRACTED TURNS:\n")
	b.WriteString(transcript)
	return b.String()
}

func reinforceExtractedFactIDs(ctx context.Context, store Store, ids []int64) {
	seenReinforced := make(map[int64]bool)
	for _, rid := range ids {
		if rid > 0 && !seenReinforced[rid] {
			seenReinforced[rid] = true
			if err := store.ReinforceFact(ctx, rid); err != nil {
				log.Printf("[Memory] Warning: Failed to reinforce trajectory fact ID %d: %v", rid, err)
			} else {
				RecordFactStored("reinforced")
			}
		}
	}
}

func stageSingleProceduralCandidate(ctx context.Context, store Store, client OllamaClient, threadID string, item ExtractedProceduralItem, pendingExisting []ProceduralCandidate) ([]ProceduralCandidate, bool) {
	admit, reason := EvaluateProceduralCandidateAdmission(item)
	if !admit {
		log.Printf("[Memory] Procedural candidate rejected (%s): trigger=%q", reason, item.Trigger)
		return pendingExisting, true
	}

	emb, err := client.GenerateEmbedding(ctx, FormatProceduralCandidateEmbeddingText(item), false, 1)
	if err != nil {
		log.Printf("[Memory] Warning: failed to generate embedding for procedural candidate trigger %q: %v", item.Trigger, err)
		return pendingExisting, false
	}
	if len(emb) != ExpectedEmbeddingDim {
		log.Printf("[Memory] Warning: procedural candidate embedding dimension %d != %d", len(emb), ExpectedEmbeddingDim)
		return pendingExisting, false
	}

	trimmedLoadedSkill := strings.TrimSpace(item.LoadedSkill)
	if isDuplicateProceduralCandidate(trimmedLoadedSkill, threadID, emb, pendingExisting) {
		log.Printf("[Memory] Skipping near-duplicate procedural candidate: trigger=%q skill=%q", item.Trigger, item.LoadedSkill)
		return pendingExisting, true
	}

	domain := strings.TrimSpace(item.Domain)
	if domain == "" {
		domain = "discord"
	}
	steps := item.Steps
	if steps == nil {
		steps = []string{}
	}
	avoid := item.Avoid
	if avoid == nil {
		avoid = []string{}
	}

	cand := ProceduralCandidate{
		ThreadID:    threadID,
		Domain:      domain,
		LoadedSkill: trimmedLoadedSkill,
		Trigger:     strings.TrimSpace(item.Trigger),
		Steps:       steps,
		Avoid:       avoid,
		Embedding:   emb,
		Status:      "pending",
	}

	newID, err := store.InsertProceduralCandidate(ctx, cand)
	if err != nil {
		log.Printf("[Memory] Warning: failed to insert procedural candidate %q: %v", cand.Trigger, err)
		return pendingExisting, false
	}
	cand.ID = newID
	return append(pendingExisting, cand), true
}

func stageProceduralCandidates(ctx context.Context, store Store, client OllamaClient, threadID string, procCandidates []ExtractedProceduralItem) bool {
	if len(procCandidates) > 3 {
		procCandidates = procCandidates[:3]
	}
	if len(procCandidates) == 0 {
		return true
	}

	allOk := true
	pendingExisting, err := store.GetPendingProceduralCandidates(ctx)
	if err != nil {
		log.Printf("[Memory] Warning: failed to get pending procedural candidates: %v", err)
		allOk = false
		pendingExisting = nil
	}

	for _, item := range procCandidates {
		var ok bool
		pendingExisting, ok = stageSingleProceduralCandidate(ctx, store, client, threadID, item, pendingExisting)
		if !ok {
			allOk = false
		}
	}
	return allOk
}

func processThreadFacts(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, threadID string) error {
	if store == nil {
		return fmt.Errorf("database is nil")
	}

	turns, err := store.GetUnextractedAdminTurns(ctx, threadID)
	if err != nil {
		return fmt.Errorf("failed to get unextracted turns for thread %s: %w", threadID, err)
	}

	maxRowID, err := resolveThreadTurnWatermark(ctx, store, threadID, turns)
	if err != nil {
		return err
	}

	transcript := formatExtractableTurns(turns)
	if strings.TrimSpace(transcript) == "" {
		log.Printf("[Memory] Empty extractable turns for thread %s, marking watermark.", threadID)
		if err := store.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
			log.Printf("[Memory] Warning updating conversation fact watermark for empty transcript (thread=%s): %v", threadID, err)
		}
		return nil
	}

	prompt := buildFactExtractionPrompt(ctx, store, threadID, transcript)
	respText, err := llmFunc(ctx, prompt)
	if err != nil {
		return fmt.Errorf("LLM fact extraction call failed: %w", err)
	}

	factsPayload, err := parseFactsJSON(respText)
	if err != nil {
		return fmt.Errorf("failed to parse extracted facts JSON: %w", err)
	}

	reinforceExtractedFactIDs(ctx, store, factsPayload.ReinforcedIDs)

	candidates := factsPayload.Facts
	if len(candidates) > 5 {
		candidates = candidates[:5]
	}

	allSucceeded := true
	for _, item := range candidates {
		if !reconcileCandidateFact(ctx, store, client, llmFunc, item) {
			allSucceeded = false
		}
	}

	if !stageProceduralCandidates(ctx, store, client, threadID, factsPayload.ProceduralCandidates) {
		allSucceeded = false
	}

	if allSucceeded {
		if err := store.UpdateConversationFactWatermark(ctx, threadID, maxRowID); err != nil {
			log.Printf("[Memory] Warning updating conversation fact watermark (thread=%s): %v", threadID, err)
		}
	}
	return nil
}

func isDuplicateProceduralCandidate(trimmedLoadedSkill, threadID string, emb []float32, pendingExisting []ProceduralCandidate) bool {
	for _, existing := range pendingExisting {
		if strings.EqualFold(strings.TrimSpace(existing.LoadedSkill), trimmedLoadedSkill) && (trimmedLoadedSkill != "" || existing.ThreadID == threadID) {
			if len(existing.Embedding) == ExpectedEmbeddingDim && CosineSimilarity(emb, existing.Embedding) >= 0.90 {
				return true
			}
		}
	}
	return false
}

func applyReconcileDecision(ctx context.Context, store Store, item ExtractedFactItem, emb []float32, neighbors []ScoredFact, dec ExtractedFactItem) bool {
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
				return false
			}
			RecordFactStored("deleted")
			return true
		}
		log.Printf("[Memory] Tier 3 delete rejected: ID %d not in whitelisted neighbors (NOOP)", targetID)
		return true
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
			if err := store.UpdateFact(ctx, targetID, factText, emb); err != nil {
				log.Printf("[Memory] Error updating fact %d: %v", targetID, err)
				return false
			}
			RecordFactStored("updated")
			return true
		}
		log.Printf("[Memory] Tier 3 update rejected: ID %d not in whitelisted neighbors; falling back to ADD", targetID)
		if _, err := store.InsertFact(ctx, item.FactText, emb); err != nil {
			log.Printf("[Memory] Error inserting fallback fact: %v", err)
			return false
		}
		RecordFactStored("inserted")
		return true
	case "NOOP":
		targetID := dec.SupersedesID
		if !validIDs[targetID] {
			targetID = neighbors[0].Fact.ID
		}
		if err := store.ReinforceFact(ctx, targetID); err != nil {
			log.Printf("[Memory] Error reinforcing NOOP fact %d: %v", targetID, err)
			return false
		}
		RecordFactStored("reinforced")
		return true
	default: // "ADD" or fallback
		if _, err := store.InsertFact(ctx, item.FactText, emb); err != nil {
			log.Printf("[Memory] Error inserting fact: %v", err)
			return false
		}
		RecordFactStored("inserted")
		return true
	}
}

func reconcileCandidateFact(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, item ExtractedFactItem) bool {
	if store == nil || client == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(item.Action), "DELETE") {
		return true
	}
	admit, _ := EvaluateCandidateAdmission(item)
	if !admit {
		return true
	}

	emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to generate embedding for candidate fact %q: %v", item.FactText, err)
		return false
	}
	if len(emb) != ExpectedEmbeddingDim {
		log.Printf("[Memory] Warning: Candidate fact embedding dimension %d != %d", len(emb), ExpectedEmbeddingDim)
		return false
	}

	neighbors, err := store.FindNearestScoredFacts(ctx, emb, 5, 0.60)
	if err != nil {
		log.Printf("[Memory] Warning: FindNearestScoredFacts error for %q: %v", item.FactText, err)
		return false
	}

	// Tier 1: len(neighbors) == 0 (maxSim < 0.60)
	if len(neighbors) == 0 {
		if _, err := store.InsertFact(ctx, item.FactText, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 1 fact: %v", err)
			return false
		}
		RecordFactStored("inserted")
		return true
	}

	// Tier 2: neighbors[0].Score > 0.88
	if neighbors[0].Score > 0.88 {
		if err := store.ReinforceFact(ctx, neighbors[0].Fact.ID); err != nil {
			log.Printf("[Memory] Error reinforcing Tier 2 fact id=%d: %v", neighbors[0].Fact.ID, err)
			return false
		}
		RecordFactStored("reinforced")
		return true
	}

	// Tier 3: 0.60 <= neighbors[0].Score <= 0.88
	if llmFunc == nil {
		if _, err := store.InsertFact(ctx, item.FactText, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 3 fallback fact: %v", err)
			return false
		}
		RecordFactStored("inserted")
		return true
	}

	reconcilePrompt := buildCandidateReconcilePrompt(item, neighbors)
	resp, err := llmFunc(ctx, reconcilePrompt)
	if err != nil {
		log.Printf("[Memory] Tier 3 reconciliation LLM error: %v", err)
		if _, err := store.InsertFact(ctx, item.FactText, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 3 fallback fact on LLM error: %v", err)
			return false
		}
		RecordFactStored("inserted")
		return true
	}

	decisionPayload, err := parseFactsJSON(resp)
	if err != nil || len(decisionPayload.Facts) == 0 {
		log.Printf("[Memory] Tier 3 parse error (%v); falling back to ADD", err)
		if _, err := store.InsertFact(ctx, item.FactText, emb); err != nil {
			log.Printf("[Memory] Error inserting Tier 3 fallback fact on parse error: %v", err)
			return false
		}
		RecordFactStored("inserted")
		return true
	}

	return applyReconcileDecision(ctx, store, item, emb, neighbors, decisionPayload.Facts[0])
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

// DefaultConsolidationMinSim is the minimum pairwise cosine similarity for fact consolidation cliques.
const (
	DefaultConsolidationMinSim  = 0.75
	DefaultConsolidationMaxSize = 2
	MaxConsolidationPasses      = 3
)

type factCliqueEdge struct {
	i, j int
	sim  float64
}

type factCandidateMatch struct {
	fact ScoredFactWithEmbedding
	sim  float64
}

func similarityInBounds(sim, minSim, maxSim float64) bool {
	return sim >= minSim && (maxSim >= 1.0 || sim <= maxSim)
}

func buildFactCliqueEdges(group []ScoredFactWithEmbedding, minSim, maxSim float64) []factCliqueEdge {
	var edges []factCliqueEdge
	for i := 0; i < len(group); i++ {
		for j := i + 1; j < len(group); j++ {
			if sim := CosineSimilarity(group[i].Embedding, group[j].Embedding); similarityInBounds(sim, minSim, maxSim) {
				edges = append(edges, factCliqueEdge{i: i, j: j, sim: sim})
			}
		}
	}
	sort.SliceStable(edges, func(a, b int) bool {
		if edges[a].sim != edges[b].sim {
			return edges[a].sim > edges[b].sim
		}
		if group[edges[a].i].Fact.ID != group[edges[b].i].Fact.ID {
			return group[edges[a].i].Fact.ID < group[edges[b].i].Fact.ID
		}
		return group[edges[a].j].Fact.ID < group[edges[b].j].Fact.ID
	})
	return edges
}

func matchesAllFactCliqueMembers(members []ScoredFactWithEmbedding, cand ScoredFactWithEmbedding, minSim, maxSim float64) bool {
	for _, member := range members {
		sim := CosineSimilarity(member.Embedding, cand.Embedding)
		if !similarityInBounds(sim, minSim, maxSim) {
			return false
		}
	}
	return true
}

func expandFactClique(group []ScoredFactWithEmbedding, e factCliqueEdge, used map[int64]bool, minSim, maxSim float64, maxSize int) []ScoredFactWithEmbedding {
	u, v := group[e.i], group[e.j]
	clique := []ScoredFactWithEmbedding{u, v}
	if maxSize <= 2 {
		return clique
	}

	var cands []factCandidateMatch
	for k := 0; k < len(group); k++ {
		if k == e.i || k == e.j || used[group[k].Fact.ID] {
			continue
		}
		simU := CosineSimilarity(u.Embedding, group[k].Embedding)
		simV := CosineSimilarity(v.Embedding, group[k].Embedding)
		if similarityInBounds(simU, minSim, maxSim) && similarityInBounds(simV, minSim, maxSim) {
			cands = append(cands, factCandidateMatch{fact: group[k], sim: min(simU, simV)})
		}
	}
	sort.SliceStable(cands, func(a, b int) bool {
		if cands[a].sim != cands[b].sim {
			return cands[a].sim > cands[b].sim
		}
		return cands[a].fact.Fact.ID < cands[b].fact.Fact.ID
	})

	for _, c := range cands {
		if len(clique) >= maxSize {
			break
		}
		if matchesAllFactCliqueMembers(clique[2:], c.fact, minSim, maxSim) {
			clique = append(clique, c.fact)
		}
	}
	return clique
}

// FindConsolidationCliques finds complete-linkage cliques of size [minSize, maxSize]
// where every pair of facts (u, v) in the clique satisfies minSim <= CosineSimilarity(u.Embedding, v.Embedding) <= maxSim.
func FindConsolidationCliques(facts []ScoredFactWithEmbedding, minSim, maxSim float64, minSize, maxSize int) [][]ScoredFactWithEmbedding {
	if minSim <= 0 {
		minSim = DefaultConsolidationMinSim
	}
	if maxSim <= 0 {
		maxSim = 1.0
	}
	if minSize <= 0 {
		minSize = 2
	}
	if maxSize < minSize {
		maxSize = max(DefaultConsolidationMaxSize, minSize)
	}

	var group []ScoredFactWithEmbedding
	for _, f := range facts {
		if len(f.Embedding) == 0 {
			continue
		}
		group = append(group, f)
	}

	sort.Slice(group, func(i, j int) bool {
		return group[i].Fact.ID < group[j].Fact.ID
	})

	edges := buildFactCliqueEdges(group, minSim, maxSim)

	var cliques [][]ScoredFactWithEmbedding
	used := make(map[int64]bool, len(group))
	for _, e := range edges {
		u, v := group[e.i], group[e.j]
		if used[u.Fact.ID] || used[v.Fact.ID] {
			continue
		}
		clique := expandFactClique(group, e, used, minSim, maxSim, maxSize)
		if len(clique) >= minSize {
			sort.Slice(clique, func(a, b int) bool { return clique[a].Fact.ID < clique[b].Fact.ID })
			for _, member := range clique {
				used[member.Fact.ID] = true
			}
			cliques = append(cliques, clique)
		}
	}

	return cliques
}

func cliqueSignature(clique []ScoredFactWithEmbedding) string {
	ids := make([]int64, len(clique))
	for i, item := range clique {
		ids[i] = item.Fact.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(fmt.Sprintf("%d", id))
	}
	return b.String()
}

// FactConsolidationPrompt defines the system prompt instructing the LLM to consolidate related facts.
const FactConsolidationPrompt = `You are a memory consolidation engine.
Synthesize the provided pair of semantically related facts into 1 concise, canonical, single-topic fact that preserves all specific information, preferences, constraints, and context without redundancy.

Output strictly a JSON object matching this schema:
{
  "facts": [
    {
      "fact_text": "Consolidated canonical fact text."
    }
  ]
}
Rules:
1. If the facts express the same topic/predicate or one updates/refines the other, produce 1 consolidated fact. If the facts already cover distinct, non-redundant sub-topics that should remain separate, return {"facts": []}.
2. Keep the consolidated fact strictly atomic and concise (one single topic/predicate, one sentence under 280 characters). Never cram multiple distinct topics into a long semicolon or participial run-on sentence; if the facts span distinct sub-topics (e.g. household composition vs. dietary restrictions), return {"facts": []}.
3. Facts are ordered by ascending ID (higher ID = newer state). When facts contain conflicting numbers, thresholds, versions, ports, or status values, preserve the newer (higher-ID) state and discard the outdated value.
4. Output nothing else outside the JSON object.`

func buildConsolidationPrompt(clique []ScoredFactWithEmbedding) string {
	var b strings.Builder
	b.WriteString(FactConsolidationPrompt)
	b.WriteString("\n\nCLUSTER FACTS TO CONSOLIDATE:\n")
	for _, item := range clique {
		b.WriteString(fmt.Sprintf("- (ID: %d) %s\n", item.Fact.ID, item.Fact.FactText))
	}
	return b.String()
}

// ConsolidateFactClusters discovers semantically tight complete-linkage cliques of facts and synthesizes them into canonical facts.
// Single-flight protected via extractionMutex. Runs up to MaxConsolidationPasses passes until convergence.
func ConsolidateFactClusters(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc) (int, error) {
	if store == nil || client == nil || llmFunc == nil {
		return 0, fmt.Errorf("nil store, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Fact extraction or dreaming already running, skipping overlapping consolidation.")
		return 0, nil
	}
	defer extractionMutex.Unlock()

	var totalConsolidated int
	triedCliques := make(map[string]bool)

	for pass := 0; pass < MaxConsolidationPasses; pass++ {
		allFacts, err := store.GetAllFactsWithEmbeddings(ctx)
		if err != nil {
			return totalConsolidated, fmt.Errorf("failed to get facts with embeddings: %w", err)
		}
		if len(allFacts) == 0 {
			break
		}

		var cliques [][]ScoredFactWithEmbedding
		for _, c := range FindConsolidationCliques(allFacts, DefaultConsolidationMinSim, 1.0, 2, DefaultConsolidationMaxSize) {
			if sig := cliqueSignature(c); !triedCliques[sig] {
				triedCliques[sig] = true
				cliques = append(cliques, c)
			}
		}
		if len(cliques) == 0 {
			break
		}

		passCount, deleteFailed, err := consolidateCliqueBatch(ctx, store, client, llmFunc, cliques)
		totalConsolidated += passCount
		if err != nil {
			return totalConsolidated, err
		}
		if passCount == 0 || deleteFailed {
			break
		}
	}

	return totalConsolidated, nil
}

func extractConsolidatedTexts(items []ExtractedFactItem) []string {
	var candidates []string
	for _, c := range items {
		if text := strings.TrimSpace(c.FactText); text != "" {
			candidates = append(candidates, text)
			if len(candidates) == 2 {
				break
			}
		}
	}
	return candidates
}

func insertAndReinforceConsolidatedFact(ctx context.Context, store Store, client OllamaClient, text string, maxReinforce int) (int64, bool) {
	emb, err := client.GenerateEmbedding(ctx, text, false, 1)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to generate embedding for consolidated fact %q: %v", text, err)
		return 0, false
	}
	if len(emb) != ExpectedEmbeddingDim {
		return 0, false
	}

	newID, err := store.InsertFact(ctx, text, emb)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to insert consolidated fact: %v", err)
		return 0, false
	}

	reinforceTimes := min(maxReinforce, 10) - 1
	for r := 0; r < reinforceTimes; r++ {
		if err := store.ReinforceFact(ctx, newID); err != nil {
			log.Printf("[Memory] Warning: Failed to reinforce consolidated fact %d: %v", newID, err)
		}
	}
	return newID, true
}

func consolidateSingleFactClique(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, clique []ScoredFactWithEmbedding) (consolidated bool, deleteFailed bool) {
	prompt := buildConsolidationPrompt(clique)
	resp, err := llmFunc(ctx, prompt)
	if err != nil {
		log.Printf("[Memory] Warning: Fact consolidation LLM synthesis failed for cluster: %v", err)
		return false, false
	}

	payload, err := parseFactsJSON(resp)
	if err != nil {
		log.Printf("[Memory] Warning: Fact consolidation payload parse failed: %v", err)
		return false, false
	}

	candidates := extractConsolidatedTexts(payload.Facts)
	if len(candidates) == 0 || len(candidates) >= len(clique) {
		return false, false
	}

	maxReinforce := 1
	for _, src := range clique {
		if src.Fact.ReinforceCount > maxReinforce {
			maxReinforce = src.Fact.ReinforceCount
		}
	}

	var insertedIDs []int64
	for _, text := range candidates {
		if newID, ok := insertAndReinforceConsolidatedFact(ctx, store, client, text, maxReinforce); ok {
			insertedIDs = append(insertedIDs, newID)
		}
	}

	if len(insertedIDs) == 0 {
		return false, false
	}

	for _, src := range clique {
		if err := store.DeleteFact(ctx, src.Fact.ID); err != nil {
			log.Printf("[Memory] Warning: Failed to delete consolidated source fact ID %d: %v", src.Fact.ID, err)
			deleteFailed = true
		}
	}
	return true, deleteFailed
}

func consolidateCliqueBatch(ctx context.Context, store Store, client OllamaClient, llmFunc LLMClientFunc, cliques [][]ScoredFactWithEmbedding) (int, bool, error) {
	log.Printf("[Memory] Consolidating %d candidate fact cluster(s)...", len(cliques))
	var consolidatedCount int
	var deleteFailed bool

	for _, clique := range cliques {
		select {
		case <-ctx.Done():
			return consolidatedCount, deleteFailed, ctx.Err()
		default:
		}

		consolidated, delFail := consolidateSingleFactClique(ctx, store, client, llmFunc, clique)
		if delFail {
			deleteFailed = true
		}
		if consolidated {
			consolidatedCount++
		}
	}

	return consolidatedCount, deleteFailed, nil
}
