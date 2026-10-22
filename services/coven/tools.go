package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
)

// MemorySearchArgs defines inputs for the memory_search MCP tool.
type MemorySearchArgs struct {
	Query string `json:"query" jsonschema:"Search query text to find semantically relevant facts."`
	Limit int    `json:"limit,omitempty" jsonschema:"Maximum number of facts to return (default 5, max 20)."`
}

// MemorySearchOutput defines the output structure for memory_search.
type MemorySearchOutput struct {
	Facts   []Fact `json:"facts"`
	Count   int    `json:"count"`
	Context string `json:"context,omitempty"`
}

// MemoryStoreArgs defines inputs for the memory_store MCP tool.
type MemoryStoreArgs struct {
	FactText     string `json:"fact_text" jsonschema:"The fact text to persist or update."`
	SupersedesID int64  `json:"supersedes_id,omitempty" jsonschema:"Optional ID of an existing fact to overwrite and supersede."`
}

// MemoryStoreOutput defines the result structure for memory_store.
type MemoryStoreOutput struct {
	Status  string `json:"status"`
	FactID  int64  `json:"fact_id"`
	Action  string `json:"action"`
	Message string `json:"message"`
}

// MemoryForgetArgs defines inputs for the memory_forget MCP tool.
type MemoryForgetArgs struct {
	FactID int64 `json:"fact_id" jsonschema:"ID of the fact to forget and delete."`
}

// MemoryForgetOutput defines the result structure for memory_forget.
type MemoryForgetOutput struct {
	Status  string `json:"status"`
	FactID  int64  `json:"fact_id"`
	Message string `json:"message"`
}

// ContextResponse defines the structure returned by GET /context.
type ContextResponse struct {
	Context string `json:"context"`
	Facts   []Fact `json:"facts"`
}

// ToolHandler provides methods executing MCP tool invocations and HTTP endpoints.
type ToolHandler struct {
	cfg       *Config
	store     Store
	ollama    OllamaClient
	llmClient LLMClientFunc
	publisher SkillPublisher
}

// NewToolHandler creates a new ToolHandler instance.
func NewToolHandler(cfg *Config, store Store, ollama OllamaClient, publisher ...SkillPublisher) *ToolHandler {
	var brainURL string
	if cfg != nil {
		brainURL = cfg.BrainLLMURL
	}
	var pub SkillPublisher
	if len(publisher) > 0 {
		pub = publisher[0]
	}
	return &ToolHandler{
		cfg:       cfg,
		store:     store,
		ollama:    ollama,
		llmClient: HTTPBrainLLMClient(brainURL, nil),
		publisher: pub,
	}
}

// Consolidate runs a complete-linkage clique dreaming pass over all stored facts,
// and if a publisher is configured, triggers procedural skill consolidation.
func (h *ToolHandler) Consolidate(ctx context.Context) (int, error) {
	consolidated, err := ConsolidateFactClusters(ctx, h.store, h.ollama, h.llmClient)
	if err != nil {
		return consolidated, err
	}
	log.Printf("[Memory] Fact consolidation dreaming completed: %d clusters consolidated", consolidated)

	if h.publisher != nil && h.store != nil {
		customDir := DefaultCustomSkillsDir
		if h.cfg != nil && h.cfg.CustomSkillsDir != "" {
			customDir = h.cfg.CustomSkillsDir
		}
		if prNum, err := ConsolidateProceduralSkills(ctx, h.store, h.ollama, h.llmClient, h.publisher, customDir); err != nil {
			log.Printf("[Memory] Procedural skill consolidation failed: %v", err)
		} else if prNum > 0 {
			log.Printf("[Memory] Procedural skill consolidation opened PR #%d", prNum)
		}
	}

	return consolidated, nil
}

// FormatMemoryContext converts retrieved facts into a markdown prompt block.
func FormatMemoryContext(facts []Fact) string {
	if len(facts) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("<retrieved_memory>\n")
	for _, f := range facts {
		sb.WriteString(fmt.Sprintf("- [id:%d] %s\n", f.ID, f.FactText))
	}
	sb.WriteString("</retrieved_memory>\n")
	return sb.String()
}

// MemorySearch executes semantic search with 70/30 Hybrid RRF and MMR diversity re-ranking.
func (h *ToolHandler) MemorySearch(ctx context.Context, args MemorySearchArgs) (MemorySearchOutput, error) {
	query := strings.TrimSpace(args.Query)
	if query == "" {
		return MemorySearchOutput{Facts: []Fact{}}, fmt.Errorf("'query' cannot be empty")
	}

	limit := args.Limit
	if limit <= 0 {
		limit = 5
	}
	if limit > 20 {
		limit = 20
	}

	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	var emb []float32
	if h.ollama != nil {
		vec, err := h.ollama.GenerateEmbedding(ctx, query, true, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Vector search embedding failed: %v. Falling back to sparse search.", err)
		} else {
			emb = vec
		}
	}

	candidates, err := h.store.SearchCandidateFacts(ctx, emb, query, candidateLimit, DefaultMinScore)
	if err != nil {
		return MemorySearchOutput{Facts: []Fact{}}, fmt.Errorf("failed to search candidate facts: %w", err)
	}

	scored := MMRSelect(candidates, 0.70, limit)
	facts := make([]Fact, len(scored))
	for i, s := range scored {
		facts[i] = s.Fact
	}

	return MemorySearchOutput{
		Facts:   facts,
		Count:   len(facts),
		Context: FormatMemoryContext(facts),
	}, nil
}

// MemoryStore persists, updates, or reinforces a fact.
func (h *ToolHandler) MemoryStore(ctx context.Context, args MemoryStoreArgs) (MemoryStoreOutput, error) {
	factText := strings.TrimSpace(args.FactText)
	if factText == "" {
		return MemoryStoreOutput{}, fmt.Errorf("'fact_text' cannot be empty")
	}

	if args.SupersedesID == 0 {
		admit, reason := EvaluateCandidateAdmission(ExtractedFactItem{
			FactText: factText,
		})
		if !admit {
			return MemoryStoreOutput{}, fmt.Errorf("fact rejected by admission gate (%s)", reason)
		}
	}

	if h.ollama == nil {
		return MemoryStoreOutput{}, fmt.Errorf("ollama client is required for embedding generation")
	}

	emb, err := h.ollama.GenerateEmbedding(ctx, factText, false, 1)
	if err != nil {
		return MemoryStoreOutput{}, fmt.Errorf("failed to generate embedding: %w", err)
	}
	if len(emb) != ExpectedEmbeddingDim {
		return MemoryStoreOutput{}, fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(emb))
	}

	// 1. Explicit overwrite via supersedes_id
	if args.SupersedesID > 0 {
		err := h.store.UpdateFact(ctx, args.SupersedesID, factText, emb)
		if err != nil {
			if errors.Is(err, ErrFactNotFound) {
				return MemoryStoreOutput{}, fmt.Errorf("fact with id %d not found to supersede", args.SupersedesID)
			}
			return MemoryStoreOutput{}, fmt.Errorf("failed to supersede fact: %w", err)
		}
		return MemoryStoreOutput{
			Status:  "success",
			FactID:  args.SupersedesID,
			Action:  "updated",
			Message: "Fact updated successfully.",
		}, nil
	}

	// 2. Near-duplicate deduplication via cosine similarity threshold (0.85)
	dup, sim, err := h.store.FindDuplicateFact(ctx, emb, 0.85)
	if err == nil && dup != nil {
		log.Printf("[Memory] Reinforcing duplicate fact %d (sim=%.2f, text=%q)", dup.ID, sim, dup.FactText)
		if err := h.store.ReinforceFact(ctx, dup.ID); err != nil {
			return MemoryStoreOutput{}, fmt.Errorf("failed to reinforce duplicate fact: %w", err)
		}
		return MemoryStoreOutput{
			Status:  "success",
			FactID:  dup.ID,
			Action:  "reinforced",
			Message: "Fact reinforced successfully.",
		}, nil
	}

	// 3. Insert new fact
	id, err := h.store.InsertFact(ctx, factText, emb)
	if err != nil {
		return MemoryStoreOutput{}, fmt.Errorf("failed to insert fact: %w", err)
	}

	return MemoryStoreOutput{
		Status:  "success",
		FactID:  id,
		Action:  "inserted",
		Message: "Fact inserted successfully.",
	}, nil
}

// MemoryForget deletes a fact by its ID.
func (h *ToolHandler) MemoryForget(ctx context.Context, args MemoryForgetArgs) (MemoryForgetOutput, error) {
	if args.FactID <= 0 {
		return MemoryForgetOutput{}, fmt.Errorf("'fact_id' is required and must be positive")
	}

	err := h.store.DeleteFact(ctx, args.FactID)
	if err != nil {
		if errors.Is(err, ErrFactNotFound) {
			return MemoryForgetOutput{}, fmt.Errorf("fact with id %d not found", args.FactID)
		}
		return MemoryForgetOutput{}, fmt.Errorf("failed to delete fact: %w", err)
	}

	return MemoryForgetOutput{
		Status:  "success",
		FactID:  args.FactID,
		Message: "Fact deleted successfully.",
	}, nil
}

// GetFacts retrieves paginated facts matching the filter.
func (h *ToolHandler) GetFacts(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	return h.store.GetFactsPaginated(ctx, filter)
}

// GetContext retrieves relevant memory facts and formats them as a prompt context block.
func (h *ToolHandler) GetContext(ctx context.Context, query string, limit int) (*ContextResponse, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	cleanQuery := strings.TrimSpace(query)
	if cleanQuery == "" {
		return &ContextResponse{
			Context: "",
			Facts:   []Fact{},
		}, nil
	}

	if limit <= 0 {
		limit = 5
	}
	if limit > 20 {
		limit = 20
	}

	candidateLimit := limit * 3
	if candidateLimit < 30 {
		candidateLimit = 30
	}

	var emb []float32
	if h.ollama != nil {
		vec, err := h.ollama.GenerateEmbedding(ctx, cleanQuery, true, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Vector search embedding failed: %v", err)
		} else {
			emb = vec
		}
	}

	candidates, err := h.store.SearchCandidateFacts(ctx, emb, cleanQuery, candidateLimit, DefaultMinScore)
	if err != nil {
		log.Printf("[Memory] Warning: Candidate search failed: %v. Proceeding with empty context.", err)
		return &ContextResponse{
			Context: "",
			Facts:   []Fact{},
		}, nil
	}

	scored := MMRSelect(candidates, 0.70, limit)
	facts := make([]Fact, len(scored))
	for i, s := range scored {
		facts[i] = s.Fact
	}

	return &ContextResponse{
		Context: FormatMemoryContext(facts),
		Facts:   facts,
	}, nil
}
