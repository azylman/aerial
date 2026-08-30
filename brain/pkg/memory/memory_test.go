package memory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func TestFloat32ByteConversions(t *testing.T) {
	input := []float32{0.123, -0.456, 0.789, 1.0, -1.0}
	bytes := db.Float32ToBytes(input)
	output := db.BytesToFloat32(bytes)

	if len(output) != len(input) {
		t.Fatalf("expected len %d, got %d", len(input), len(output))
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("at index %d: expected %f, got %f", i, input[i], output[i])
		}
	}
}

func TestDotProduct(t *testing.T) {
	v1 := []float32{1.0, 0.0, 0.0}
	v2 := []float32{1.0, 0.0, 0.0}
	v3 := []float32{0.0, 1.0, 0.0}

	dot12 := DotProduct(v1, v2)
	if dot12 != 1.0 {
		t.Errorf("expected 1.0, got %f", dot12)
	}

	dot13 := DotProduct(v1, v3)
	if dot13 != 0.0 {
		t.Errorf("expected 0.0, got %f", dot13)
	}
}

func TestRankFacts(t *testing.T) {
	queryVec := []float32{1.0, 0.0, 0.0}
	facts := []db.FactWithEmbedding{
		{
			Fact:      db.Fact{ID: 1, Category: "cat1", FactText: "Fact 1", Importance: 1.0},
			Embedding: []float32{0.9, 0.1, 0.0}, // Dot product = 0.9
		},
		{
			Fact:      db.Fact{ID: 2, Category: "cat2", FactText: "Fact 2", Importance: 1.0},
			Embedding: []float32{0.3, 0.7, 0.0}, // Dot product = 0.3 (below threshold 0.45)
		},
		{
			Fact:      db.Fact{ID: 3, Category: "cat3", FactText: "Fact 3", Importance: 1.0},
			Embedding: []float32{0.95, 0.05, 0.0}, // Dot product = 0.95
		},
	}

	ranked := RankFacts(queryVec, facts, 0.45, 10)
	if len(ranked) != 2 {
		t.Fatalf("expected 2 facts above threshold 0.45, got %d", len(ranked))
	}

	if ranked[0].ID != 3 {
		t.Errorf("expected top fact ID 3 (score 0.95), got %d", ranked[0].ID)
	}
	if ranked[1].ID != 1 {
		t.Errorf("expected second fact ID 1 (score 0.9), got %d", ranked[1].ID)
	}
}

func TestFormatMemoryContext(t *testing.T) {
	facts := []db.Fact{
		{Category: "user_preference", FactText: "Prefers Pacific Time"},
		{Category: "system_config", FactText: "Docker Daemon at unix:///var/run/docker.sock"},
	}

	formatted := FormatMemoryContext(facts)
	expectedSubstrings := []string{
		"<retrieved_memory>",
		"- [user_preference] Prefers Pacific Time",
		"- [system_config] Docker Daemon at unix:///var/run/docker.sock",
		"</retrieved_memory>",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(formatted, sub) {
			t.Errorf("expected formatted memory to contain %q, got: %s", sub, formatted)
		}
	}
}

func TestMockOllamaClient(t *testing.T) {
	var receivedPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req EmbeddingRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		receivedPrompt = req.Prompt

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: []float32{0.1, 0.2, 0.3},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)

	// Test query embedding (should prepend BGE prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	emb, err := client.GenerateEmbedding(ctx, "test query", true, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(emb) != 3 {
		t.Errorf("expected embedding len 3, got %d", len(emb))
	}
	if !strings.HasPrefix(receivedPrompt, BGEQueryPrefix) {
		t.Errorf("expected prompt to start with BGE prefix, got: %s", receivedPrompt)
	}

	// Test document embedding (should NOT prepend BGE prefix)
	embDoc, err := client.GenerateEmbedding(ctx, "test doc", false, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(embDoc) != 3 {
		t.Errorf("expected embedding len 3, got %d", len(embDoc))
	}
	if strings.HasPrefix(receivedPrompt, BGEQueryPrefix) {
		t.Errorf("expected document prompt NOT to have BGE prefix, got: %s", receivedPrompt)
	}
}

func TestParseFactsJSON(t *testing.T) {
	raw := "```json\n{\"facts\":[{\"category\":\"user_preference\",\"fact_text\":\"User likes Go\",\"importance_score\":1.0}]}\n```"
	payload, err := parseFactsJSON(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing facts: %v", err)
	}

	if len(payload.Facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(payload.Facts))
	}
	if payload.Facts[0].FactText != "User likes Go" {
		t.Errorf("expected 'User likes Go', got %q", payload.Facts[0].FactText)
	}
}

func TestDBFactInsertionAndRetrieval(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init in-memory db: %v", err)
	}
	defer func() { _ = database.Close() }()

	emb := []float32{0.5, 0.5, 0.7071}
	id, err := db.InsertFact(database, "user_pref", "User prefers dark mode", 1.0, "thread-123", emb)
	if err != nil {
		t.Fatalf("failed to insert fact: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected valid insert id > 0, got %d", id)
	}

	facts, err := db.GetAllFactsWithEmbeddings(database)
	if err != nil {
		t.Fatalf("failed to get facts with embeddings: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(facts))
	}

	if facts[0].Fact.FactText != "User prefers dark mode" {
		t.Errorf("expected fact text 'User prefers dark mode', got %q", facts[0].Fact.FactText)
	}
	if len(facts[0].Embedding) != 3 {
		t.Fatalf("expected embedding len 3, got %d", len(facts[0].Embedding))
	}
	if facts[0].Embedding[0] != 0.5 || facts[0].Embedding[1] != 0.5 {
		t.Errorf("embedding mismatch: %v", facts[0].Embedding)
	}
}

func TestProcessThreadFactsDeduplicationAndWatermark(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer func() { _ = database.Close() }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: []float32{1.0, 0.0, 0.0},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)

	// Mock LLM function returning the same fact
	llmFunc := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[{"category":"user_pref","fact_text":"User likes matcha","importance_score":1.0}]}`, nil
	}

	now := time.Now().UTC()
	_ = db.InsertMessage(database, db.Message{
		ID: "m1", ThreadID: "thread-test-1", Status: db.StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})

	// Pre-insert an existing identical/similar fact with same vector
	_, _ = db.InsertFact(database, "user_pref", "User likes matcha", 1.0, "thread-test-1", []float32{1.0, 0.0, 0.0})

	// Create a dummy transcript file for thread-test-1
	homeDir, _ := os.UserHomeDir()
	if homeDir == "" {
		homeDir = "/root"
	}
	logDir := filepath.Join(homeDir, ".gemini", "antigravity", "brain", "thread-test-1", ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0755)
	_ = os.WriteFile(filepath.Join(logDir, "transcript.jsonl"), []byte("{\"step\":1,\"content\":\"User likes matcha\"}\n"), 0644)
	defer os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity", "brain", "thread-test-1"))

	ctx := context.Background()
	err = processThreadFacts(ctx, database, client, llmFunc, "thread-test-1")
	if err != nil {
		t.Fatalf("processThreadFacts failed: %v", err)
	}

	// Verify that duplicate fact was NOT inserted (count remains 1)
	facts, err := db.GetFactsByThreadWithEmbeddings(database, "thread-test-1")
	if err != nil {
		t.Fatalf("GetFactsByThreadWithEmbeddings failed: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("Expected exactly 1 fact due to semantic deduplication, got %d", len(facts))
	}

	// Verify watermark was updated and thread is no longer eligible for extraction
	eligible, err := db.GetActiveConversationsForExtraction(database, 12)
	if err != nil {
		t.Fatalf("GetActiveConversationsForExtraction failed: %v", err)
	}
	if len(eligible) != 0 {
		t.Fatalf("Expected 0 eligible threads after extraction watermark, got %v", eligible)
	}
}

