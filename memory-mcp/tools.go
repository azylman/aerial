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
	Query    string `json:"query" jsonschema:"Search query text to find semantically relevant facts."`
	Category string `json:"category,omitempty" jsonschema:"Optional fact category filter (e.g. user_preference, system_config, identity)."`
	Limit    int    `json:"limit,omitempty" jsonschema:"Maximum number of facts to return (default 5, max 20)."`
}

// MemorySearchOutput defines the output structure for memory_search.
type MemorySearchOutput struct {
	Facts   []Fact `json:"facts"`
	Count   int    `json:"count"`
	Context string `json:"context,omitempty"`
}

// MemoryStoreArgs defines inputs for the memory_store MCP tool.
type MemoryStoreArgs struct {
	FactText     string  `json:"fact_text" jsonschema:"The fact text to persist or update."`
	Category     string  `json:"category,omitempty" jsonschema:"Category of the fact (e.g. user_preference, system_config, identity)."`
	Importance   float64 `json:"importance,omitempty" jsonschema:"Importance score from 0.1 to 1.0 (default 0.7)."`
	SupersedesID int64   `json:"supersedes_id,omitempty" jsonschema:"Optional ID of an existing fact to overwrite and supersede."`
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
	cfg    *Config
	store  Store
	ollama OllamaClient
}

// NewToolHandler creates a new ToolHandler instance.
func NewToolHandler(cfg *Config, store Store, ollama OllamaClient) *ToolHandler {
	return &ToolHandler{
		cfg:    cfg,
		store:  store,
		ollama: ollama,
	}
}

// FormatMemoryContext converts retrieved facts into a markdown prompt block.
func FormatMemoryContext(facts []Fact) string {
	if len(facts) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("<retrieved_memory>\n")
	for _, f := range facts {
		cat := f.Category
		if cat == "" {
			cat = "general"
		}
		sb.WriteString(fmt.Sprintf("- [%s] %s\n", cat, f.FactText))
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

	candidates, err := h.store.SearchCandidateFacts(ctx, emb, query, strings.TrimSpace(args.Category), candidateLimit, DefaultMinScore)
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

	category := strings.TrimSpace(args.Category)
	if category == "" {
		category = "general"
	}

	importance := args.Importance
	if importance <= 0 {
		importance = 0.70
	}
	if importance > 1.0 {
		importance = 1.0
	}

	if args.SupersedesID == 0 {
		admit, adjustedImportance, reason := EvaluateCandidateAdmission(ExtractedFactItem{
			Category:   category,
			FactText:   factText,
			Importance: importance,
		})
		if !admit {
			return MemoryStoreOutput{}, fmt.Errorf("fact rejected by admission gate (%s)", reason)
		}
		importance = adjustedImportance
	}

	var emb []float32
	if h.ollama != nil {
		vec, err := h.ollama.GenerateEmbedding(ctx, factText, false, 1)
		if err != nil {
			log.Printf("[Memory] Warning: Document embedding generation failed: %v", err)
		} else {
			emb = vec
		}
	}

	// 1. Explicit overwrite via supersedes_id
	if args.SupersedesID > 0 {
		err := h.store.UpdateFact(ctx, args.SupersedesID, category, factText, importance, emb)
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
	if len(emb) > 0 {
		dup, sim, err := h.store.FindDuplicateFact(ctx, emb, 0.85)
		if err == nil && dup != nil {
			log.Printf("[Memory] Reinforcing duplicate fact %d (sim=%.2f, text=%q)", dup.ID, sim, dup.FactText)
			if err := h.store.ReinforceFact(ctx, dup.ID, factText, emb, 0.15); err != nil {
				return MemoryStoreOutput{}, fmt.Errorf("failed to reinforce duplicate fact: %w", err)
			}
			return MemoryStoreOutput{
				Status:  "success",
				FactID:  dup.ID,
				Action:  "reinforced",
				Message: "Fact reinforced successfully.",
			}, nil
		}
	}

	// 3. Insert new fact
	id, err := h.store.InsertFact(ctx, category, factText, importance, emb)
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

	candidates, err := h.store.SearchCandidateFacts(ctx, emb, cleanQuery, "", candidateLimit, DefaultMinScore)
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

	if len(facts) > 0 {
		ids := make([]int64, len(facts))
		for i, f := range facts {
			ids[i] = f.ID
		}
		if err := h.store.TouchRetrievedFacts(ctx, ids, 0.02); err != nil {
			log.Printf("[Memory] Warning: TouchRetrievedFacts failed: %v", err)
		}
	}

	return &ContextResponse{
		Context: FormatMemoryContext(facts),
		Facts:   facts,
	}, nil
}
