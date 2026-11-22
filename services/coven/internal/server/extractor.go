package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

const (
	actionNoop               = "NOOP"
	rejectEphemeralTelemetry = "ephemeral_telemetry"
)

// LLMClientFunc defines the interface function for sending prompts to an LLM.
type LLMClientFunc func(ctx context.Context, prompt string) (string, error)

type llmKeyCtx struct{}

// withLLMKey attaches an Idempotency-Key identity to ctx for the next LLM call.
func withLLMKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, llmKeyCtx{}, key)
}

func llmKeyFrom(ctx context.Context) string {
	key, ok := ctx.Value(llmKeyCtx{}).(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(key)
}

// hash16 returns the first 16 hex chars of sha256(s) for bounded key parts.
func hash16(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

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
	// eri's /internal/llm is internal, so traceparent is propagated and its span joins ours.
	httpClient = tracing.Client(httpClient)
	cleanURL := strings.TrimSpace(brainLLMURL)
	return func(ctx context.Context, prompt string) (string, error) {
		if cleanURL == "" {
			return "", errors.New("brain LLM URL is not configured")
		}
		reqBody, err := json.Marshal(brainLLMRequest{Prompt: prompt})
		if err != nil {
			return "", fmt.Errorf("failed to marshal brain LLM request: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cleanURL, bytes.NewReader(reqBody))
		if err != nil {
			return "", fmt.Errorf("failed to create brain LLM request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if key := llmKeyFrom(ctx); key != "" {
			req.Header.Set("Idempotency-Key", key)
		}

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
		if rid > 0 && !seenReinforced[rid] {
			seenReinforced[rid] = true
			if err := s.ReinforceFact(ctx, rid); err != nil {
				return fmt.Errorf("failed to reinforce trajectory fact %d: %w", rid, err)
			}
			RecordFactStored("reinforced")
		}
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

func closeConn(conn *sql.Conn) {
	if conn != nil {
		if err := conn.Close(); err != nil {
			slog.Debug("failed to close database connection", "error", err)
		}
	}
}

func resolvePinnedStore(ctx context.Context, s store.Store) (store.Store, store.DBTX, *sql.Conn, error) {
	cp, ok := s.(store.ConnProvider)
	if !ok {
		return s, nil, nil, nil
	}
	conn, cerr := cp.Conn(ctx)
	if cerr != nil && !errors.Is(cerr, store.ErrNoPinnedConn) {
		return nil, nil, nil, fmt.Errorf("failed to pin connection for thread extraction: %w", cerr)
	}
	if conn == nil {
		return s, cp.DB(), nil, nil
	}
	var ownedConn *sql.Conn
	if _, alreadyConn := cp.DB().(*sql.Conn); !alreadyConn {
		// Only close connections created from *sql.DB; pre-pinned connections remain owned by caller.
		ownedConn = conn
	}
	if sqlS, ok := s.(*store.SQLStore); ok {
		s = sqlS.WithDB(conn)
	}
	return s, conn, ownedConn, nil
}

func acquireThreadLock(ctx context.Context, s store.Store, threadID string) (store.Store, store.DBTX, func(), error) {
	updatedS, lockDB, ownedConn, err := resolvePinnedStore(ctx, s)
	if err != nil {
		return nil, nil, nil, err
	}
	s = updatedS

	if lockDB == nil {
		cleanup := func() {
			closeConn(ownedConn)
		}
		return s, nil, cleanup, nil
	}

	locker := store.NewThreadLocker(lockDB)
	unlock, err := locker.Lock(ctx, threadID)
	if err != nil {
		closeConn(ownedConn)
		return nil, nil, nil, err
	}

	cleanup := func() {
		if unlock != nil {
			if uerr := unlock(); uerr != nil {
				slog.ErrorContext(ctx, "[Memory] failed to release thread lock, destroying connection", "error", uerr)
				closeConn(ownedConn)
				ownedConn = nil
			}
		}
		closeConn(ownedConn)
	}
	return s, lockDB, cleanup, nil
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

func processTurnBatch(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, threadID string, turns []store.Message, maxRowID int64, lockDB store.DBTX, agent ...string) error {
	transcript := formatExtractableTurns(turns)
	if strings.TrimSpace(transcript) == "" {
		slog.InfoContext(ctx, "[Memory] empty extractable turns, marking watermark", "thread_id", threadID)
		if err := s.UpdateConversationFactWatermark(ctx, threadID, maxRowID, agent...); err != nil {
			return fmt.Errorf("failed to update conversation fact watermark for empty transcript: %w", err)
		}
		return nil
	}

	prompt, err := buildFactExtractionPrompt(ctx, s, threadID, transcript, agent...)
	if err != nil {
		return err
	}

	respText, err := llmFunc(ctx, prompt)
	if err != nil {
		return fmt.Errorf("LLM fact extraction call failed: %w", err)
	}

	factsPayload, err := parseFactsJSON(respText)
	if err != nil {
		return fmt.Errorf("failed to parse extracted facts JSON: %w", err)
	}

	if err := reinforceExtractedFactIDs(ctx, s, factsPayload.ReinforcedIDs); err != nil {
		return err
	}

	candidates := factsPayload.Facts
	if len(candidates) > 5 {
		candidates = candidates[:5]
	}

	targetAgent := ""
	if len(agent) > 0 {
		targetAgent = agent[0]
	}

	for _, item := range candidates {
		if err := reconcileCandidateFact(ctx, s, client, llmFunc, item, lockDB, targetAgent); err != nil {
			return err
		}
	}

	if err := stageProceduralCandidates(ctx, s, client, threadID, factsPayload.ProceduralCandidates, agent...); err != nil {
		return fmt.Errorf("failed to stage procedural candidates: %w", err)
	}

	if err := s.UpdateConversationFactWatermark(ctx, threadID, maxRowID, agent...); err != nil {
		return fmt.Errorf("failed to update conversation fact watermark: %w", err)
	}

	return nil
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

func executeTurnBatch(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, threadID, targetAgent string, turns []store.Message, maxRowID int64, lockDB store.DBTX) (int, bool, error) {
	if err := processTurnBatch(ctx, s, client, llmFunc, threadID, turns, maxRowID, lockDB, targetAgent); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && len(turns) > 1 {
			newBatch := max(1, len(turns)/2)
			slog.WarnContext(ctx, "[Memory] extraction timed out, halving batch size", "thread_id", threadID, "new_batch_size", newBatch)
			return newBatch, true, nil
		}
		return 0, false, err
	}
	return 0, false, nil
}

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

	lockKey := trimmed
	if targetAgent != "" {
		lockKey = config.ScopedThreadKey(targetAgent, trimmed)
	}

	lockedS, lockDB, cleanup, err := acquireThreadLock(ctx, s, lockKey)
	if err != nil {
		return err
	}
	defer cleanup()
	s = lockedS

	// Coalesced extraction can cover many turns, so each thread gets a deliberate root span
	// that the outbound LLM call attaches to. A valid incoming context is kept as a span link.
	startOpts := []trace.SpanStartOption{trace.WithNewRoot()}
	if trigger := trace.LinkFromContext(ctx); trigger.SpanContext.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trigger))
	}
	ctx, span := tracing.Tracer().Start(ctx, "coven.extract", startOpts...)
	defer span.End()
	if targetAgent != "" {
		span.SetAttributes(attribute.String("aerial.agent", targetAgent))
	}

	batchSize := config.DefaultExtractMaxSteps

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastWatermark, _, err := s.GetConversationFactWatermark(ctx, trimmed, targetAgent)
		if err != nil {
			return fmt.Errorf("failed to get conversation fact watermark: %w", err)
		}

		turns, nextBatchSize, fetchErr := fetchUnextractedTurnsWithRetry(ctx, s, trimmed, targetAgent, batchSize, span)
		if fetchErr != nil {
			return fetchErr
		}
		if nextBatchSize != batchSize {
			batchSize = nextBatchSize
			continue
		}
		if len(turns) == 0 {
			return handleEmptyUnextractedTurns(ctx, s, trimmed, lastWatermark, targetAgent)
		}
		span.SetAttributes(attribute.Int("aerial.coven.batch_size", len(turns)))

		maxRowID, err := resolveThreadTurnWatermark(ctx, s, trimmed, turns, targetAgent)
		if err != nil {
			return err
		}
		if maxRowID <= lastWatermark || maxRowID <= 0 {
			break
		}

		nextBatch, retry, batchErr := executeTurnBatch(withLLMKey(ctx, fmt.Sprintf("coven:extract:%s:%s:%d", targetAgent, trimmed, lastWatermark)), s, client, llmFunc, trimmed, targetAgent, turns, maxRowID, lockDB)
		if batchErr != nil {
			return batchErr
		}
		if retry {
			batchSize = nextBatch
			continue
		}

		batchSize = config.DefaultExtractMaxSteps
	}
	return nil
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

func applyReconcileDecision(ctx context.Context, s store.Store, item ExtractedFactItem, emb []float32, neighbors []store.ScoredFact, dec ExtractedFactItem, agent ...string) error {
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
			if err := s.DeleteFact(ctx, targetID); err != nil {
				return fmt.Errorf("error deleting fact: %w", err)
			}
			RecordFactStored("deleted")
			return nil
		}
		slog.InfoContext(ctx, "[Memory] tier 3 delete rejected: ID not in whitelisted neighbors (NOOP)", "fact_id", targetID)
		return nil
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
			if err := s.UpdateFact(ctx, targetID, factText, emb); err != nil {
				return fmt.Errorf("error updating fact: %w", err)
			}
			RecordFactStored("updated")
			return nil
		}
		slog.InfoContext(ctx, "[Memory] tier 3 update rejected: ID not in whitelisted neighbors; falling back to ADD", "fact_id", targetID)
		if _, err := s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting fallback fact: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	case actionNoop:
		targetID := dec.SupersedesID
		if !validIDs[targetID] {
			targetID = neighbors[0].Fact.ID
		}
		if err := s.ReinforceFact(ctx, targetID); err != nil {
			return fmt.Errorf("error reinforcing NOOP fact: %w", err)
		}
		RecordFactStored("reinforced")
		return nil
	default: // "ADD" or fallback
		if _, err := s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting fact: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	}
}

func reconcileCandidateFact(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, item ExtractedFactItem, lockDB store.DBTX, agent string) error {
	if s == nil || client == nil {
		return fmt.Errorf("nil s or client")
	}
	if strings.EqualFold(strings.TrimSpace(item.Action), "DELETE") {
		return nil
	}
	admit, _ := EvaluateCandidateAdmission(item)
	if !admit {
		return nil
	}

	emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
	if err != nil {
		return fmt.Errorf("failed to generate embedding for candidate fact: %w", err)
	}
	if len(emb) != store.ExpectedEmbeddingDim {
		return fmt.Errorf("candidate fact embedding dimension mismatch: got %d, want %d", len(emb), store.ExpectedEmbeddingDim)
	}

	runReconcile := func(ctx context.Context) error {
		return executeCandidateReconcile(ctx, s, llmFunc, item, emb, agent)
	}

	if lockDB != nil {
		return store.WithFactWriteLock(ctx, lockDB, "coven:facts", runReconcile)
	}
	return runReconcile(ctx)
}

func executeCandidateReconcile(ctx context.Context, s store.Store, llmFunc LLMClientFunc, item ExtractedFactItem, emb []float32, agent ...string) error {
	neighbors, err := s.FindNearestScoredFacts(ctx, emb, 5, 0.60)
	if err != nil {
		return fmt.Errorf("FindNearestScoredFacts error: %w", err)
	}

	// Tier 1: len(neighbors) == 0 (maxSim < 0.60)
	if len(neighbors) == 0 {
		if _, err = s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting tier 1 fact: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	}

	// Tier 2: neighbors[0].Score > 0.88
	if neighbors[0].Score > 0.88 {
		if err = s.ReinforceFact(ctx, neighbors[0].Fact.ID); err != nil {
			return fmt.Errorf("error reinforcing tier 2 fact: %w", err)
		}
		RecordFactStored("reinforced")
		return nil
	}

	// Tier 3: 0.60 <= neighbors[0].Score <= 0.88
	return reconcileTier3Candidate(ctx, s, llmFunc, item, emb, neighbors, agent...)
}

func reconcileTier3Candidate(ctx context.Context, s store.Store, llmFunc LLMClientFunc, item ExtractedFactItem, emb []float32, neighbors []store.ScoredFact, agent ...string) error {
	if llmFunc == nil {
		if _, err := s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting tier 3 fallback fact: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	}

	reconcilePrompt := buildCandidateReconcilePrompt(item, neighbors)
	reconcileAgent := ""
	if len(agent) > 0 {
		reconcileAgent = strings.TrimSpace(agent[0])
	}
	// Candidates have no row ID yet, so the identity is the candidate text hash.
	reconcileCtx := withLLMKey(ctx, "coven:reconcile:"+reconcileAgent+":"+hash16(item.FactText))
	resp, err := llmFunc(reconcileCtx, reconcilePrompt)
	if err != nil {
		slog.WarnContext(ctx, "[Memory] tier 3 reconciliation LLM error", "error", err)
		if _, err = s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting tier 3 fallback fact on LLM error: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	}

	decisionPayload, err := parseFactsJSON(resp)
	if err != nil || len(decisionPayload.Facts) == 0 {
		slog.WarnContext(ctx, "[Memory] tier 3 parse error; falling back to ADD", "error", err)
		if _, err := s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting tier 3 fallback fact on parse error: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	}

	return applyReconcileDecision(ctx, s, item, emb, neighbors, decisionPayload.Facts[0], agent...)
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
// Single-flight protected via extractionMutex. Runs up to MaxConsolidationPasses passes until convergence.
func ConsolidateFactClusters(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc) (int, error) {
	if s == nil || client == nil || llmFunc == nil {
		return 0, fmt.Errorf("nil s, ollama client, or llmFunc")
	}

	if !extractionMutex.TryLock() {
		log.Printf("[Memory] Fact extraction or dreaming already running, skipping overlapping consolidation.")
		return 0, nil
	}
	defer extractionMutex.Unlock()

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

func insertAndReinforceConsolidatedFact(ctx context.Context, s store.Store, client store.OllamaClient, text string, maxReinforce int) (int64, bool) {
	emb, err := client.GenerateEmbedding(ctx, text, false, 1)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to generate embedding for consolidated fact %q: %v", text, err)
		return 0, false
	}
	if len(emb) != store.ExpectedEmbeddingDim {
		return 0, false
	}

	newID, err := s.InsertFact(ctx, text, emb)
	if err != nil {
		log.Printf("[Memory] Warning: Failed to insert consolidated fact: %v", err)
		return 0, false
	}

	reinforceTimes := min(maxReinforce, 10) - 1
	for range reinforceTimes {
		if err := s.ReinforceFact(ctx, newID); err != nil {
			log.Printf("[Memory] Warning: Failed to reinforce consolidated fact %d: %v", newID, err)
		}
	}
	return newID, true
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

	var insertedIDs []int64
	for _, text := range candidates {
		if newID, ok := insertAndReinforceConsolidatedFact(ctx, s, client, text, maxReinforce); ok {
			insertedIDs = append(insertedIDs, newID)
		}
	}

	if len(insertedIDs) == 0 {
		return false, true
	}

	for _, src := range clique {
		if err := s.DeleteFact(ctx, src.Fact.ID); err != nil {
			log.Printf("[Memory] Warning: Failed to delete consolidated source fact ID %d: %v", src.Fact.ID, err)
			cliqueFailed = true
		}
	}
	return true, cliqueFailed
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
