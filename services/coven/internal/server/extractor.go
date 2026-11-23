package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

const (
	actionAdd                = "ADD"
	actionDelete             = "DELETE"
	actionNoop               = "NOOP"
	actionUpdate             = "UPDATE"
	rejectEphemeralTelemetry = "ephemeral_telemetry"
)

// LLMClientFunc defines the interface function for sending prompts to an LLM.
type LLMClientFunc func(ctx context.Context, prompt string) (string, error)

// withLLMKey attaches an Idempotency-Key identity to ctx for the next LLM call.
func withLLMKey(ctx context.Context, key string) context.Context {
	return eriwire.WithLLMKey(ctx, key)
}

// hash16 returns the first 16 hex chars of sha256(s) for bounded key parts.
func hash16(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

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

// HTTPBrainLLMClient returns an LLMClientFunc communicating with brain's internal LLM proxy.
func HTTPBrainLLMClient(brainLLMURL string, httpClient *http.Client) LLMClientFunc {
	return eriwire.HTTPInternalLLMClient(brainLLMURL, httpClient)
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

// ExtractThreadFacts extracts and reconciles facts for a single conversation thread.
// It returns any error directly to allow JetStream retries.
func ExtractThreadFacts(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, threadID string, agent ...string) error {
	trimmed := strings.TrimSpace(threadID)
	if s == nil || llmFunc == nil || trimmed == "" {
		return errors.New("extract thread facts: invalid arguments (nil s, nil llmFunc, or empty threadID)")
	}
	targetAgent := ""
	if len(agent) > 0 {
		targetAgent = strings.TrimSpace(agent[0])
	}
	return processThreadFacts(ctx, s, client, llmFunc, trimmed, targetAgent)
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
	if action == actionNoop {
		return false, "noop_action"
	}
	if action == actionDelete {
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
		return false, rejectEphemeralTelemetry
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
		return false, rejectEphemeralTelemetry
	}

	validSteps := false
	for _, s := range item.Steps {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			if ephemeralTelemetryRegex.MatchString(trimmed) && !durableDirectiveRegex.MatchString(trimmed) {
				return false, rejectEphemeralTelemetry
			}
			validSteps = true
		}
	}
	validAvoid := false
	for _, a := range item.Avoid {
		if trimmed := strings.TrimSpace(a); trimmed != "" {
			if ephemeralTelemetryRegex.MatchString(trimmed) && !durableDirectiveRegex.MatchString(trimmed) {
				return false, rejectEphemeralTelemetry
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

func buildCandidateReconcilePrompt(item ExtractedFactItem, neighbors []store.ScoredFact) string {
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
		fmt.Fprintf(&b, "- ID %d (similarity %.2f): %s\n", n.Fact.ID, n.Score, n.Fact.FactText)
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

func resolveThreadTurnWatermark(ctx context.Context, s store.Store, threadID string, turns []store.Message, agent ...string) (int64, error) {
	var maxRowID int64
	for _, t := range turns {
		if t.RowID > maxRowID {
			maxRowID = t.RowID
		}
	}
	if maxRowID == 0 {
		var err error
		maxRowID, err = s.GetMaxMessageRowID(ctx, threadID, agent...)
		if err != nil {
			return 0, fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, err)
		}
	}
	return maxRowID, nil
}

func buildFactExtractionPrompt(ctx context.Context, s store.Store, threadID, transcript string, agent ...string) (string, error) {
	priorSummary, priorTurns, ctxErr := s.GetThreadContextBeforeWatermark(ctx, threadID, 2, agent...)
	if ctxErr != nil {
		return "", fmt.Errorf("failed to get thread context before watermark: %w", ctxErr)
	}

	retrievedFacts, toolCalls, trajErr := s.GetThreadTrajectoryContext(ctx, threadID, agent...)
	if trajErr != nil {
		return "", fmt.Errorf("failed to get thread trajectory context: %w", trajErr)
	}
	loadedSkills := store.ExtractLoadedSkillsFromCalls(toolCalls)

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
	return b.String(), nil
}

func reinforceExtractedFactIDs(ctx context.Context, s store.Store, ids []int64) error {
	seenReinforced := make(map[int64]bool)
	for _, rid := range ids {
		if rid <= 0 || seenReinforced[rid] {
			continue
		}
		seenReinforced[rid] = true
		err := s.ReinforceFact(ctx, rid)
		if errors.Is(err, store.ErrFactNotFound) {
			// The LLM named an ID that does not exist (or was merged away): skip it, never stall.
			slog.InfoContext(ctx, "[Memory] skipping reinforcement of unknown fact", "fact_id", rid)
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to reinforce trajectory fact %d: %w", rid, err)
		}
		RecordFactStored("reinforced")
	}
	return nil
}

func stageSingleProceduralCandidate(ctx context.Context, s store.Store, client store.OllamaClient, threadID string, item ExtractedProceduralItem, pendingExisting []store.ProceduralCandidate, agent ...string) ([]store.ProceduralCandidate, error) {
	admit, reason := EvaluateProceduralCandidateAdmission(item)
	if !admit {
		slog.InfoContext(ctx, "[Memory] procedural candidate rejected", "reason", reason, "trigger", item.Trigger)
		return pendingExisting, nil
	}

	emb, err := client.GenerateEmbedding(ctx, FormatProceduralCandidateEmbeddingText(item), false, 1)
	if err != nil {
		slog.WarnContext(ctx, "[Memory] failed to generate embedding for procedural candidate", "trigger", item.Trigger, "error", err)
		return pendingExisting, fmt.Errorf("failed to generate embedding for candidate %q: %w", item.Trigger, err)
	}
	if len(emb) != store.ExpectedEmbeddingDim {
		slog.WarnContext(ctx, "[Memory] procedural candidate embedding dimension mismatch", "got", len(emb), "want", store.ExpectedEmbeddingDim)
		return pendingExisting, fmt.Errorf("procedural candidate embedding dimension mismatch: got %d, want %d", len(emb), store.ExpectedEmbeddingDim)
	}

	trimmedLoadedSkill := strings.TrimSpace(item.LoadedSkill)
	if isDuplicateProceduralCandidate(trimmedLoadedSkill, threadID, emb, pendingExisting) {
		slog.InfoContext(ctx, "[Memory] skipping near-duplicate procedural candidate", "trigger", item.Trigger, "skill", item.LoadedSkill)
		return pendingExisting, nil
	}

	domain := strings.TrimSpace(item.Domain)
	if domain == "" {
		domain = domainDiscord
	}
	steps := item.Steps
	if steps == nil {
		steps = []string{}
	}
	avoid := item.Avoid
	if avoid == nil {
		avoid = []string{}
	}

	targetAgent := ""
	if len(agent) > 0 {
		targetAgent = strings.TrimSpace(agent[0])
	}

	cand := store.ProceduralCandidate{
		ThreadID:    threadID,
		Domain:      domain,
		LoadedSkill: trimmedLoadedSkill,
		Trigger:     strings.TrimSpace(item.Trigger),
		Steps:       steps,
		Avoid:       avoid,
		Embedding:   emb,
		Status:      "pending",
		Agent:       targetAgent,
	}

	newID, err := s.InsertProceduralCandidate(ctx, cand)
	if err != nil {
		slog.WarnContext(ctx, "[Memory] failed to insert procedural candidate", "trigger", cand.Trigger, "error", err)
		return pendingExisting, fmt.Errorf("failed to insert procedural candidate %q: %w", cand.Trigger, err)
	}
	cand.ID = newID
	return append(pendingExisting, cand), nil
}

func stageProceduralCandidates(ctx context.Context, s store.Store, client store.OllamaClient, threadID string, procCandidates []ExtractedProceduralItem, agent ...string) error {
	if len(procCandidates) > 3 {
		procCandidates = procCandidates[:3]
	}
	if len(procCandidates) == 0 {
		return nil
	}

	pendingExisting, err := s.GetPendingProceduralCandidates(ctx)
	if err != nil {
		slog.WarnContext(ctx, "[Memory] failed to get pending procedural candidates", "error", err)
		return fmt.Errorf("failed to get pending procedural candidates: %w", err)
	}

	for _, item := range procCandidates {
		var stageErr error
		pendingExisting, stageErr = stageSingleProceduralCandidate(ctx, s, client, threadID, item, pendingExisting, agent...)
		if stageErr != nil {
			return stageErr
		}
	}
	return nil
}

func handleEmptyUnextractedTurns(ctx context.Context, s store.Store, threadID string, lastWatermark int64, agent ...string) error {
	if lastWatermark != 0 {
		return nil
	}
	maxRowID, maxErr := s.GetMaxMessageRowID(ctx, threadID, agent...)
	if maxErr != nil {
		return fmt.Errorf("failed to get max message rowid for thread %s: %w", threadID, maxErr)
	}
	if maxRowID <= 0 {
		return nil
	}
	if err := s.UpdateConversationFactWatermark(ctx, threadID, maxRowID, agent...); err != nil {
		return fmt.Errorf("failed to update conversation fact watermark for empty turns: %w", err)
	}
	return nil
}

// extractBatch is one batch of unextracted turns, loaded and ready for the extraction LLM call.
// Exactly one of Ready, Skipped, or Halve > 0 is set unless the thread is fully extracted
// (the zero value), so a dropped load result ends extraction instead of looping.
type extractBatch struct {
	Ready     bool   `json:"ready,omitempty"`
	Skipped   bool   `json:"skipped,omitempty"`
	Halve     int    `json:"halve,omitempty"`
	Prompt    string `json:"prompt,omitempty"`
	Watermark int64  `json:"watermark,omitempty"`
	MaxRowID  int64  `json:"max_row_id,omitempty"`
	Turns     int    `json:"turns,omitempty"`
}

// extractAnswer is one extraction LLM answer. Rejected carries the validation failure
// reason when the answer is not a valid facts payload; TimedOut asks for a smaller batch.
type extractAnswer struct {
	Payload  ExtractedFactsPayload `json:"payload"`
	Rejected string                `json:"rejected,omitempty"`
	TimedOut bool                  `json:"timed_out,omitempty"`
}

// extractLLMKey names one extraction LLM call: the batch starting after watermark, plus the
// durable attempt ordinal once an earlier answer was rejected (eri caches answers by key).
func extractLLMKey(agent, threadID string, watermark int64, attempt int) string {
	key := fmt.Sprintf("coven:extract:%s:%s:%d", agent, threadID, watermark)
	if attempt > 0 {
		key += fmt.Sprintf(":a%d", attempt)
	}
	return key
}

// prepareTurnBatchPrompt builds the extraction prompt for turns. A batch with no
// extractable text advances the watermark to maxRowID and reports skipped.
func prepareTurnBatchPrompt(ctx context.Context, s store.Store, threadID string, turns []store.Message, maxRowID int64, agent string) (string, bool, error) {
	transcript := formatExtractableTurns(turns)
	if strings.TrimSpace(transcript) == "" {
		slog.InfoContext(ctx, "[Memory] empty extractable turns, marking watermark", "thread_id", threadID)
		if err := s.UpdateConversationFactWatermark(ctx, threadID, maxRowID, agent); err != nil {
			return "", false, fmt.Errorf("failed to update conversation fact watermark for empty transcript: %w", err)
		}
		return "", true, nil
	}
	prompt, err := buildFactExtractionPrompt(ctx, s, threadID, transcript, agent)
	if err != nil {
		return "", false, err
	}
	return prompt, false, nil
}

// loadExtractBatch reads the thread watermark and the next batch of unextracted turns.
func loadExtractBatch(ctx context.Context, s store.Store, threadID, agent string, batchSize int) (extractBatch, error) {
	watermark, _, err := s.GetConversationFactWatermark(ctx, threadID, agent)
	if err != nil {
		return extractBatch{}, fmt.Errorf("failed to get conversation fact watermark: %w", err)
	}
	turns, nextBatchSize, err := fetchUnextractedTurnsWithRetry(ctx, s, threadID, agent, batchSize, trace.SpanFromContext(ctx))
	if err != nil {
		return extractBatch{}, err
	}
	if nextBatchSize != batchSize {
		return extractBatch{Halve: nextBatchSize}, nil
	}
	if len(turns) == 0 {
		return extractBatch{}, handleEmptyUnextractedTurns(ctx, s, threadID, watermark, agent)
	}
	maxRowID, err := resolveThreadTurnWatermark(ctx, s, threadID, turns, agent)
	if err != nil {
		return extractBatch{}, err
	}
	if maxRowID <= watermark || maxRowID <= 0 {
		return extractBatch{}, nil
	}
	prompt, skipped, err := prepareTurnBatchPrompt(ctx, s, threadID, turns, maxRowID, agent)
	if err != nil {
		return extractBatch{}, err
	}
	if skipped {
		return extractBatch{Skipped: true}, nil
	}
	return extractBatch{Ready: true, Prompt: prompt, Watermark: watermark, MaxRowID: maxRowID, Turns: len(turns)}, nil
}

// callExtractLLM sends the extraction prompt (Idempotency-Key taken from ctx) and validates the
// answer. Transport errors are returned; an unparseable answer is reported as Rejected.
func callExtractLLM(ctx context.Context, llmFunc LLMClientFunc, prompt string) (extractAnswer, error) {
	resp, err := llmFunc(ctx, prompt)
	if err != nil {
		return extractAnswer{}, fmt.Errorf("LLM fact extraction call failed: %w", err)
	}
	return validateExtractAnswer(resp), nil
}

// validateExtractAnswer parses an extraction answer; an invalid one is data (Rejected), not an error.
func validateExtractAnswer(resp string) extractAnswer {
	payload, err := parseFactsJSON(resp)
	if err != nil {
		return extractAnswer{Rejected: err.Error()}
	}
	return extractAnswer{Payload: *payload}
}

// applyExtractedPayload writes one validated extraction answer and advances the watermark.
func applyExtractedPayload(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, threadID, agent string, payload ExtractedFactsPayload, maxRowID int64) error {
	if err := reinforceExtractedFactIDs(ctx, s, payload.ReinforcedIDs); err != nil {
		return err
	}
	for _, item := range firstFactCandidates(payload.Facts) {
		if err := reconcileCandidateFact(ctx, s, client, llmFunc, item, agent); err != nil {
			return err
		}
	}
	if err := stageProceduralCandidates(ctx, s, client, threadID, payload.ProceduralCandidates, agent); err != nil {
		return fmt.Errorf("failed to stage procedural candidates: %w", err)
	}
	if err := s.UpdateConversationFactWatermark(ctx, threadID, maxRowID, agent); err != nil {
		return fmt.Errorf("failed to update conversation fact watermark: %w", err)
	}
	return nil
}

// firstFactCandidates caps one answer's fact candidates at five.
func firstFactCandidates(items []ExtractedFactItem) []ExtractedFactItem {
	if len(items) > 5 {
		return items[:5]
	}
	return items
}

func fetchUnextractedTurnsWithRetry(ctx context.Context, s store.Store, threadID, targetAgent string, batchSize int, span trace.Span) ([]store.Message, int, error) {
	turns, err := s.GetUnextractedAdminTurns(ctx, threadID, batchSize, targetAgent)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && batchSize > 1 {
			newBatch := max(1, batchSize/2)
			slog.WarnContext(ctx, "[Memory] fetch unextracted turns timed out, halving batch size", "thread_id", threadID, "new_batch_size", newBatch)
			return nil, newBatch, nil
		}
		span.SetStatus(codes.Error, "get unextracted turns failed")
		return nil, 0, fmt.Errorf("failed to get unextracted turns for thread %s: %w", threadID, err)
	}
	return turns, batchSize, nil
}

// processThreadFacts extracts a thread in process (MCP tool and no-Restate fallback). It shares
// batch loading, the LLM call and the writes with CovenThread but returns a rejected answer as
// an error to its caller instead of tracking attempts.
func processThreadFacts(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, threadID string, agent ...string) error {
	if s == nil {
		return fmt.Errorf("database is nil")
	}
	trimmed := strings.TrimSpace(threadID)
	if trimmed == "" {
		return fmt.Errorf("threadID cannot be empty")
	}

	targetAgent := ""
	if len(agent) > 0 {
		targetAgent = strings.TrimSpace(agent[0])
	}
	ctx, span := startExtractSpan(ctx, targetAgent)
	defer span.End()

	batchSize := config.DefaultExtractMaxSteps
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := loadExtractBatch(ctx, s, trimmed, targetAgent, batchSize)
		if err != nil {
			return err
		}
		switch {
		case batch.Halve > 0:
			batchSize = batch.Halve
			continue
		case batch.Skipped:
			batchSize = config.DefaultExtractMaxSteps
			continue
		case !batch.Ready:
			return nil
		}
		span.SetAttributes(attribute.Int("aerial.coven.batch_size", batch.Turns))

		keyCtx := withLLMKey(ctx, extractLLMKey(targetAgent, trimmed, batch.Watermark, 0))
		ans, err := callExtractLLM(keyCtx, llmFunc, batch.Prompt)
		if err == nil && ans.Rejected != "" {
			err = fmt.Errorf("failed to parse extracted facts JSON: %s", ans.Rejected)
		}
		if err == nil {
			err = applyExtractedPayload(keyCtx, s, client, llmFunc, trimmed, targetAgent, ans.Payload, batch.MaxRowID)
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && batch.Turns > 1 {
				batchSize = max(1, batch.Turns/2)
				slog.WarnContext(ctx, "[Memory] extraction timed out, halving batch size", "thread_id", trimmed, "new_batch_size", batchSize)
				continue
			}
			return err
		}
		batchSize = config.DefaultExtractMaxSteps
	}
}

// startExtractSpan starts the coven.extract root span. Coalesced extraction can cover many
// turns, so each thread gets a deliberate root span that the outbound LLM call attaches to;
// a valid incoming context is kept as a span link.
func startExtractSpan(ctx context.Context, agent string) (context.Context, trace.Span) {
	startOpts := []trace.SpanStartOption{trace.WithNewRoot()}
	if trigger := trace.LinkFromContext(ctx); trigger.SpanContext.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trigger))
	}
	ctx, span := tracing.Tracer().Start(ctx, "coven.extract", startOpts...)
	if agent != "" {
		span.SetAttributes(attribute.String("aerial.agent", agent))
	}
	return ctx, span
}

func isDuplicateProceduralCandidate(trimmedLoadedSkill, threadID string, emb []float32, pendingExisting []store.ProceduralCandidate) bool {
	for _, existing := range pendingExisting {
		if strings.EqualFold(strings.TrimSpace(existing.LoadedSkill), trimmedLoadedSkill) && (trimmedLoadedSkill != "" || existing.ThreadID == threadID) {
			if len(existing.Embedding) == store.ExpectedEmbeddingDim && CosineSimilarity(emb, existing.Embedding) >= 0.90 {
				return true
			}
		}
	}
	return false
}

func formatExtractableTurns(turns []store.Message) string {
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
	DreamingWatermarkThreadID   = "__coven_dreaming__"
	DefaultDreamingMinInterval  = 20 * time.Hour
	DefaultConsolidationMinSim  = 0.75
	DefaultConsolidationMaxSize = 2
	MaxConsolidationPasses      = 3
)

type cliqueEdge struct {
	i, j int
	sim  float64
}

type cliqueCandidateMatch[T any] struct {
	item T
	sim  float64
}

func similarityInBounds(sim, minSim, maxSim float64) bool {
	return sim >= minSim && (maxSim >= 1.0 || sim <= maxSim)
}

func normalizeCliqueParams(minSim, maxSim float64, minSize, maxSize int) (float64, float64, int, int) {
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
	return minSim, maxSim, minSize, maxSize
}

func buildCliqueEdges[T any](group []T, minSim, maxSim float64, watermarkID int64, idOf func(T) int64, embOf func(T) []float32) []cliqueEdge {
	var edges []cliqueEdge
	for i := range group {
		embI := embOf(group[i])
		idI := idOf(group[i])
		for j := i + 1; j < len(group); j++ {
			if watermarkID > 0 && idI <= watermarkID && idOf(group[j]) <= watermarkID {
				continue
			}
			sim := CosineSimilarity(embI, embOf(group[j]))
			if similarityInBounds(sim, minSim, maxSim) {
				edges = append(edges, cliqueEdge{i: i, j: j, sim: sim})
			}
		}
	}
	sort.SliceStable(edges, func(a, b int) bool {
		if edges[a].sim != edges[b].sim {
			return edges[a].sim > edges[b].sim
		}
		idA := idOf(group[edges[a].i])
		idB := idOf(group[edges[b].i])
		if idA != idB {
			return idA < idB
		}
		return idOf(group[edges[a].j]) < idOf(group[edges[b].j])
	})
	return edges
}

func matchesAllCliqueMembers[T any](members []T, candEmb []float32, minSim, maxSim float64, embOf func(T) []float32) bool {
	for _, member := range members {
		sim := CosineSimilarity(embOf(member), candEmb)
		if !similarityInBounds(sim, minSim, maxSim) {
			return false
		}
	}
	return true
}

func expandClique[T any](group []T, e cliqueEdge, used map[int64]bool, minSim, maxSim float64, maxSize int, idOf func(T) int64, embOf func(T) []float32) []T {
	u, v := group[e.i], group[e.j]
	clique := []T{u, v}
	if maxSize <= 2 {
		return clique
	}

	embU, embV := embOf(u), embOf(v)
	var cands []cliqueCandidateMatch[T]
	for k := range group {
		if k == e.i || k == e.j || used[idOf(group[k])] {
			continue
		}
		embK := embOf(group[k])
		simU := CosineSimilarity(embU, embK)
		simV := CosineSimilarity(embV, embK)
		if similarityInBounds(simU, minSim, maxSim) && similarityInBounds(simV, minSim, maxSim) {
			cands = append(cands, cliqueCandidateMatch[T]{item: group[k], sim: min(simU, simV)})
		}
	}
	sort.SliceStable(cands, func(a, b int) bool {
		if cands[a].sim != cands[b].sim {
			return cands[a].sim > cands[b].sim
		}
		return idOf(cands[a].item) < idOf(cands[b].item)
	})

	for _, c := range cands {
		if len(clique) >= maxSize {
			break
		}
		if matchesAllCliqueMembers(clique[2:], embOf(c.item), minSim, maxSim, embOf) {
			clique = append(clique, c.item)
		}
	}
	return clique
}

func findCompleteLinkageCliques[T any](items []T, minSim, maxSim float64, minSize, maxSize int, idOf func(T) int64, embOf func(T) []float32, watermark ...int64) [][]T {
	minSim, maxSim, minSize, maxSize = normalizeCliqueParams(minSim, maxSim, minSize, maxSize)

	var group []T
	for _, item := range items {
		if len(embOf(item)) > 0 {
			group = append(group, item)
		}
	}

	sort.Slice(group, func(i, j int) bool {
		return idOf(group[i]) < idOf(group[j])
	})

	var watermarkID int64
	if len(watermark) > 0 {
		watermarkID = watermark[0]
	}

	edges := buildCliqueEdges(group, minSim, maxSim, watermarkID, idOf, embOf)

	var cliques [][]T
	used := make(map[int64]bool, len(group))
	for _, e := range edges {
		u, v := group[e.i], group[e.j]
		if used[idOf(u)] || used[idOf(v)] {
			continue
		}
		clique := expandClique(group, e, used, minSim, maxSim, maxSize, idOf, embOf)
		if len(clique) >= minSize {
			sort.Slice(clique, func(a, b int) bool { return idOf(clique[a]) < idOf(clique[b]) })
			for _, m := range clique {
				used[idOf(m)] = true
			}
			cliques = append(cliques, clique)
		}
	}

	return cliques
}

// FindConsolidationCliques finds complete-linkage cliques of size [minSize, maxSize]
// where every pair of facts (u, v) in the clique satisfies minSim <= CosineSimilarity(u.Embedding, v.Embedding) <= maxSim.
func FindConsolidationCliques(facts []store.ScoredFactWithEmbedding, minSim, maxSim float64, minSize, maxSize int, watermarkFactID ...int64) [][]store.ScoredFactWithEmbedding {
	return findCompleteLinkageCliques(
		facts, minSim, maxSim, minSize, maxSize,
		func(f store.ScoredFactWithEmbedding) int64 { return f.Fact.ID },
		func(f store.ScoredFactWithEmbedding) []float32 { return f.Embedding },
		watermarkFactID...,
	)
}

func maxFactIDFromSlice(facts []store.ScoredFactWithEmbedding, prevMax int64) int64 {
	maxID := prevMax
	for _, f := range facts {
		if f.Fact.ID > maxID {
			maxID = f.Fact.ID
		}
	}
	return maxID
}

func cliqueSignature(clique []store.ScoredFactWithEmbedding) string {
	ids := make([]int64, len(clique))
	for i, item := range clique {
		ids[i] = item.Fact.ID
	}
	slices.Sort(ids)
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(id, 10))
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

func buildConsolidationPrompt(clique []store.ScoredFactWithEmbedding) string {
	var b strings.Builder
	b.WriteString(FactConsolidationPrompt)
	b.WriteString("\n\nCLUSTER FACTS TO CONSOLIDATE:\n")
	for _, item := range clique {
		fmt.Fprintf(&b, "- (ID: %d) %s\n", item.Fact.ID, item.Fact.FactText)
	}
	return b.String()
}

// ConsolidateFactClusters discovers semantically tight complete-linkage cliques of facts and synthesizes them into canonical facts.
// Runs up to MaxConsolidationPasses passes until convergence.
func ConsolidateFactClusters(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc) (int, error) {
	if s == nil || client == nil || llmFunc == nil {
		return 0, fmt.Errorf("nil s, ollama client, or llmFunc")
	}

	watermarkFactID, _, err := s.GetConversationFactWatermark(ctx, DreamingWatermarkThreadID)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to read dreaming watermark, evaluating all cliques: %v", err)
		watermarkFactID = 0
	}

	var totalConsolidated int
	var anyFailed bool
	maxFactID := watermarkFactID
	triedCliques := make(map[string]bool)

	for range MaxConsolidationPasses {
		allFacts, err := s.GetAllFactsWithEmbeddings(ctx)
		if err != nil {
			return totalConsolidated, fmt.Errorf("failed to get facts with embeddings: %w", err)
		}
		maxFactID = maxFactIDFromSlice(allFacts, maxFactID)
		if len(allFacts) == 0 {
			break
		}

		var cliques [][]store.ScoredFactWithEmbedding
		for _, c := range FindConsolidationCliques(allFacts, DefaultConsolidationMinSim, 1.0, 2, DefaultConsolidationMaxSize, watermarkFactID) {
			if sig := cliqueSignature(c); !triedCliques[sig] {
				triedCliques[sig] = true
				cliques = append(cliques, c)
			}
		}
		if len(cliques) == 0 {
			break
		}

		passCount, batchFailed, err := consolidateCliqueBatch(ctx, s, client, llmFunc, cliques)
		totalConsolidated += passCount
		if batchFailed {
			anyFailed = true
		}
		if err != nil {
			return totalConsolidated, err
		}
		if passCount == 0 || batchFailed {
			break
		}
	}

	if !anyFailed {
		if err := s.UpdateConversationFactWatermark(ctx, DreamingWatermarkThreadID, maxFactID); err != nil {
			log.Printf("[Memory] Warning: Failed to update dreaming watermark: %v", err)
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

func storeDB(s store.Store) store.DBTX {
	if cp, ok := s.(factConnProvider); ok {
		return cp.DB()
	}
	return nil
}

func savepointBegin(ctx context.Context, db store.DBTX, sp string) bool {
	if db == nil {
		return false
	}
	_, err := db.ExecContext(ctx, "SAVEPOINT "+sp)
	return err == nil
}

func savepointRollback(ctx context.Context, db store.DBTX, sp string) {
	if db != nil {
		if _, err := db.ExecContext(context.WithoutCancel(ctx), "ROLLBACK TO SAVEPOINT "+sp); err != nil {
			slog.DebugContext(ctx, "failed to rollback savepoint", "savepoint", sp, "error", err)
		}
	}
}

func savepointRelease(ctx context.Context, db store.DBTX, sp string) {
	if db != nil {
		if _, err := db.ExecContext(context.WithoutCancel(ctx), "RELEASE SAVEPOINT "+sp); err != nil {
			slog.DebugContext(ctx, "failed to release savepoint", "savepoint", sp, "error", err)
		}
	}
}

type preparedConsolidatedFact struct {
	text string
	emb  []float32
}

// prepareConsolidatedFacts pre-generates embeddings for candidate facts outside withFactWriteTx
// so network latency to Ollama does not hold the write transaction open.
func prepareConsolidatedFacts(ctx context.Context, client store.OllamaClient, candidates []string) []preparedConsolidatedFact {
	var prepared []preparedConsolidatedFact
	for _, text := range candidates {
		emb, err := client.GenerateEmbedding(ctx, text, false, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Failed to generate embedding for consolidated fact %q: %v", text, err)
			continue
		}
		if len(emb) != store.ExpectedEmbeddingDim {
			continue
		}
		prepared = append(prepared, preparedConsolidatedFact{text: text, emb: emb})
	}
	return prepared
}

// insertAndReinforceConsolidatedFact inserts a prepared consolidated fact under a savepoint and reinforces
// it up to maxReinforce times. Any insertion or reinforcement failure rolls back to its savepoint to prevent
// transaction aborts under withFactWriteTx.
func insertAndReinforceConsolidatedFact(ctx context.Context, s store.Store, item preparedConsolidatedFact, maxReinforce int) (int64, bool) {
	db := storeDB(s)
	hasSavepoint := savepointBegin(ctx, db, "coven_ins_fact")
	newID, err := s.InsertFact(ctx, item.text, item.emb)
	if err != nil {
		if hasSavepoint {
			savepointRollback(ctx, db, "coven_ins_fact")
		}
		log.Printf("[Memory] Warning: Failed to insert consolidated fact: %v", err)
		return 0, false
	}
	if hasSavepoint {
		savepointRelease(ctx, db, "coven_ins_fact")
	}

	reinforceTimes := min(maxReinforce, 10) - 1
	for range reinforceTimes {
		hasSavepoint := savepointBegin(ctx, db, "coven_reinforce_fact")
		if err := s.ReinforceFact(ctx, newID); err != nil {
			if hasSavepoint {
				savepointRollback(ctx, db, "coven_reinforce_fact")
			}
			log.Printf("[Memory] Warning: Failed to reinforce consolidated fact %d: %v", newID, err)
		} else if hasSavepoint {
			savepointRelease(ctx, db, "coven_reinforce_fact")
		}
	}
	return newID, true
}

func deleteConsolidatedSources(ctx context.Context, txStore store.Store, clique []store.ScoredFactWithEmbedding) bool {
	var failed bool
	db := storeDB(txStore)
	for _, src := range clique {
		hasSavepoint := savepointBegin(ctx, db, "coven_del_fact")
		if err := txStore.DeleteFact(ctx, src.Fact.ID); err != nil {
			if hasSavepoint {
				savepointRollback(ctx, db, "coven_del_fact")
			}
			log.Printf("[Memory] Warning: Failed to delete consolidated source fact ID %d: %v", src.Fact.ID, err)
			failed = true
		} else if hasSavepoint {
			savepointRelease(ctx, db, "coven_del_fact")
		}
	}
	return failed
}

func consolidateSingleFactClique(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, clique []store.ScoredFactWithEmbedding) (consolidated bool, cliqueFailed bool) {
	prompt := buildConsolidationPrompt(clique)
	// Consolidation runs across all agents, so the agent segment is "all".
	resp, err := llmFunc(withLLMKey(ctx, "coven:consolidate:all:"+hash16(cliqueSignature(clique))), prompt)
	if err != nil {
		log.Printf("[Memory] Warning: Fact consolidation LLM synthesis failed for cluster: %v", err)
		return false, true
	}

	payload, err := parseFactsJSON(resp)
	if err != nil {
		log.Printf("[Memory] Warning: Fact consolidation payload parse failed: %v", err)
		return false, true
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

	prepared := prepareConsolidatedFacts(ctx, client, candidates)
	if len(prepared) == 0 {
		return false, true
	}

	txErr := withFactWriteTx(ctx, s, func(wCtx context.Context, txStore store.Store) error {
		var insertedIDs []int64
		for _, item := range prepared {
			if newID, ok := insertAndReinforceConsolidatedFact(wCtx, txStore, item, maxReinforce); ok {
				insertedIDs = append(insertedIDs, newID)
			}
		}

		if len(insertedIDs) == 0 {
			return errors.New("no consolidated facts inserted")
		}

		if failed := deleteConsolidatedSources(wCtx, txStore, clique); failed {
			cliqueFailed = true
		}
		consolidated = true
		return nil
	})
	if txErr != nil {
		return false, true
	}
	return consolidated, cliqueFailed
}

func consolidateCliqueBatch(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, cliques [][]store.ScoredFactWithEmbedding) (int, bool, error) {
	log.Printf("[Memory] Consolidating %d candidate fact cluster(s)...", len(cliques))
	var consolidatedCount int
	var batchFailed bool

	for _, clique := range cliques {
		select {
		case <-ctx.Done():
			return consolidatedCount, batchFailed, ctx.Err()
		default:
		}

		consolidated, cliqueFailed := consolidateSingleFactClique(ctx, s, client, llmFunc, clique)
		if cliqueFailed {
			batchFailed = true
		}
		if consolidated {
			consolidatedCount++
		}
	}

	return consolidatedCount, batchFailed, nil
}
