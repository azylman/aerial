package memory

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
)

func TestNew_ConfigPointerInjection(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL:     "http://localhost:11434",
			Model:       "nomic-embed-text",
			QueryPrefix: "search_query: ",
		},
	})
	client := New(cfg)
	if client == nil {
		t.Fatalf("expected non-nil client")
	}
	if client.cfg != cfg {
		t.Errorf("expected client to store injected *config.Config")
	}
}

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

	cfg := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL: server.URL,
		},
	})
	client := New(cfg)

	// Test query embedding without prefix configured (default for all-minilm)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	emb, err := client.GenerateEmbedding(ctx, "test query", true, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(emb) != 3 {
		t.Errorf("expected embedding len 3, got %d", len(emb))
	}
	if receivedPrompt != "test query" {
		t.Errorf("expected clean prompt 'test query', got: %s", receivedPrompt)
	}

	// Test query embedding with QueryPrefix configured in Config
	cfgPrefixed := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL:     server.URL,
			QueryPrefix: "Represent this query: ",
		},
	})
	clientPrefixed := New(cfgPrefixed)

	embPrefixed, err := clientPrefixed.GenerateEmbedding(ctx, "test query 2", true, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(embPrefixed) != 3 {
		t.Errorf("expected embedding len 3, got %d", len(embPrefixed))
	}
	if receivedPrompt != "Represent this query: test query 2" {
		t.Errorf("expected prefixed prompt, got: %s", receivedPrompt)
	}

	// Test document embedding (should NOT prepend prefix)
	embDoc, err := client.GenerateEmbedding(ctx, "test doc", false, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(embDoc) != 3 {
		t.Errorf("expected embedding len 3, got %d", len(embDoc))
	}
	if receivedPrompt != "test doc" {
		t.Errorf("expected document prompt 'test doc', got: %s", receivedPrompt)
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

func setupTestDB(t *testing.T) *sql.DB {
	sqlitePath := filepath.Join(t.TempDir(), "aerial_test_mem.db")
	cfg := config.NewFromData(&config.ConfigData{
		DatabaseURL: sqlitePath,
	})
	database, err := db.New(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize hermetic SQLite test DB: %v", err)
		return nil
	}
	return database
}

func makeDimVector(x, y float32) []float32 {
	v := make([]float32, db.ExpectedEmbeddingDim)
	v[0] = x
	v[1] = y
	return v
}

func TestDBFactInsertionAndRetrieval(t *testing.T) {
	database := setupTestDB(t)
	if database == nil {
		return
	}
	defer func() { _ = database.Close() }()

	emb := makeDimVector(0.5, 0.5)
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
	if len(facts[0].Embedding) != db.ExpectedEmbeddingDim {
		t.Fatalf("expected embedding len %d, got %d", db.ExpectedEmbeddingDim, len(facts[0].Embedding))
	}
	if facts[0].Embedding[0] != 0.5 || facts[0].Embedding[1] != 0.5 {
		t.Errorf("embedding mismatch: %v", facts[0].Embedding[:5])
	}
}

func TestProcessThreadFactsDeduplicationAndWatermark(t *testing.T) {
	database := setupTestDB(t)
	if database == nil {
		return
	}
	defer func() { _ = database.Close() }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: makeDimVector(1.0, 0.0),
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
	_, _ = db.InsertFact(database, "user_pref", "User likes matcha", 1.0, "thread-test-1", makeDimVector(1.0, 0.0))

	// Create a dummy transcript file for thread-test-1
	homeDir, _ := os.UserHomeDir()
	if homeDir == "" {
		homeDir = "/root"
	}
	logDir := filepath.Join(homeDir, ".gemini", "antigravity", "brain", "thread-test-1", ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0755)
	_ = os.WriteFile(filepath.Join(logDir, "transcript.jsonl"), []byte("{\"step\":1,\"content\":\"User likes matcha\"}\n"), 0644)
	defer func() { _ = os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity", "brain", "thread-test-1")) }()

	ctx := context.Background()
	err := processThreadFacts(ctx, database, client, llmFunc, "thread-test-1")
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

func TestExtractQueryText(t *testing.T) {
	// Case 1: Discord prompt format with mention
	discordPrompt := `<USER_REQUEST>
Here's a message someone sent you from Discord:

- id: 1543706746294509568
- channel_id: 1542423172400291873
- thread_id: 1543706746294509568
- author_id: 123456789
- author_username: testuser
- content: <@1542035925603713086> What office do I work from on Thursdays?
- timestamp: 2026-08-30T19:39:29Z
- mentions: [Aerial]
- attachments: []

Please formulate your response and output it clearly. It will be delivered directly to the Discord thread.
</USER_REQUEST>`

	extracted := ExtractQueryText(discordPrompt)
	expected := "What office do I work from on Thursdays?"
	if extracted != expected {
		t.Errorf("expected %q, got %q", expected, extracted)
	}

	// Case 2: XML envelope format
	xmlPrompt := "<USER_REQUEST>\n  Show me system status\n</USER_REQUEST>"
	extractedXML := ExtractQueryText(xmlPrompt)
	if extractedXML != "Show me system status" {
		t.Errorf("expected 'Show me system status', got %q", extractedXML)
	}

	// Case 3: Plain text
	plain := "What is the weather today?"
	extractedPlain := ExtractQueryText(plain)
	if extractedPlain != plain {
		t.Errorf("expected %q, got %q", plain, extractedPlain)
	}

	// Case 4: Empty
	if ExtractQueryText("") != "" {
		t.Errorf("expected empty string for empty input")
	}
}

func TestBackfillMissingEmbeddings(t *testing.T) {
	database := setupTestDB(t)
	if database == nil {
		return
	}
	defer func() { _ = database.Close() }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: makeDimVector(0.5, 0.5),
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)

	// Insert facts: 1 with embedding, 2 without embedding
	_, _ = db.InsertFact(database, "system_config", "Fact 1 with emb", 1.0, "thread-1", makeDimVector(0.1, 0.2))
	_, _ = db.InsertFact(database, "system_config", "Fact 2 missing emb", 1.0, "thread-1", nil)
	_, _ = db.InsertFact(database, "user_pref", "Fact 3 missing emb", 0.9, "thread-1", nil)

	backfilled, err := BackfillMissingEmbeddings(context.Background(), database, client)
	if err != nil {
		t.Fatalf("BackfillMissingEmbeddings failed: %v", err)
	}
	if backfilled != 2 {
		t.Fatalf("expected 2 backfilled facts, got %d", backfilled)
	}

	// Verify all 3 facts now have valid embeddings
	facts, err := db.GetAllFactsWithEmbeddings(database)
	if err != nil {
		t.Fatalf("GetAllFactsWithEmbeddings failed: %v", err)
	}
	if len(facts) != 3 {
		t.Fatalf("expected 3 facts, got %d", len(facts))
	}
	for _, f := range facts {
		if len(f.Embedding) != db.ExpectedEmbeddingDim {
			t.Errorf("expected embedding of length %d for fact %d, got %d", db.ExpectedEmbeddingDim, f.Fact.ID, len(f.Embedding))
		}
	}
}

func TestRetrieveRelevantFacts(t *testing.T) {
	database := setupTestDB(t)
	if database == nil {
		return
	}
	defer func() { _ = database.Close() }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: makeDimVector(1.0, 0.0),
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)

	// Insert facts into DB
	_, _ = db.InsertFact(database, "system_config", "Server port is 8080", 1.0, "thread-1", makeDimVector(0.9, 0.1))
	_, _ = db.InsertFact(database, "routine", "Low scoring fact", 1.0, "thread-1", makeDimVector(0.1, 0.9))

	facts, err := RetrieveRelevantFacts(context.Background(), database, client, "What port is the server?", 5)
	if err != nil {
		t.Fatalf("RetrieveRelevantFacts failed: %v", err)
	}

	if len(facts) != 1 {
		t.Fatalf("expected 1 relevant fact, got %d", len(facts))
	}
	if facts[0].FactText != "Server port is 8080" {
		t.Errorf("expected 'Server port is 8080', got %q", facts[0].FactText)
	}
}

func TestExtractActiveConversationFacts(t *testing.T) {
	database := setupTestDB(t)
	if database == nil {
		return
	}
	defer func() { _ = database.Close() }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: makeDimVector(1.0, 0.0),
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)

	llmFunc := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[{"category":"user_preference","fact_text":"User prefers vim keybindings","importance_score":0.9}]}`, nil
	}

	now := time.Now().UTC()
	_ = db.InsertMessage(database, db.Message{
		ID: "m-extract-1", ThreadID: "thread-active-1", Status: db.StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})

	// Create dummy transcript
	homeDir, _ := os.UserHomeDir()
	if homeDir == "" {
		homeDir = "/root"
	}
	logDir := filepath.Join(homeDir, ".gemini", "antigravity", "brain", "thread-active-1", ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0755)
	_ = os.WriteFile(filepath.Join(logDir, "transcript.jsonl"), []byte("{\"step\":1,\"content\":\"I love vim keybindings\"}\n"), 0644)
	defer func() { _ = os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity", "brain", "thread-active-1")) }()

	ctx := context.Background()
	err := ExtractActiveConversationFacts(ctx, database, client, llmFunc, 12)
	if err != nil {
		t.Fatalf("ExtractActiveConversationFacts failed: %v", err)
	}
}

func TestMemory_ClientCreationAndConfig(t *testing.T) {
	// 1. Explicit baseURL in Config
	cfg1 := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL: "http://custom:11434/",
		},
	})
	c1 := New(cfg1)
	if c1.BaseURL() != "http://custom:11434" {
		t.Errorf("expected trimmed baseURL, got %q", c1.BaseURL())
	}

	// 2. Custom BaseURL in Config
	cfg2 := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL: "http://env-ollama:11434/",
		},
	})
	c2 := New(cfg2)
	if c2.BaseURL() != "http://env-ollama:11434" {
		t.Errorf("expected configured baseURL, got %q", c2.BaseURL())
	}

	// 3. Fallback default
	cfg3 := config.NewFromData(&config.ConfigData{})
	c3 := New(cfg3)
	if c3.BaseURL() != DefaultOllamaURL {
		t.Errorf("expected DefaultOllamaURL %q, got %q", DefaultOllamaURL, c3.BaseURL())
	}

	// 4. Nil config fallback
	cNil := New(nil)
	if cNil.BaseURL() != DefaultOllamaURL {
		t.Errorf("expected DefaultOllamaURL %q for nil cfg, got %q", DefaultOllamaURL, cNil.BaseURL())
	}

	// 5. Compatibility NewClient with string
	cCompat := NewClient("http://custom:11434/")
	if cCompat.BaseURL() != "http://custom:11434" {
		t.Errorf("expected trimmed baseURL from NewClient, got %q", cCompat.BaseURL())
	}
}

func TestMemory_GenerateEmbedding_ErrorsAndRetries(t *testing.T) {
	client := NewClient("http://127.0.0.1:59999") // closed port

	// 1. Empty text
	_, err := client.GenerateEmbedding(context.Background(), "", false, 1)
	if err == nil {
		t.Error("expected error for empty text")
	}

	// 2. Negative retries should be clamped
	_, err = client.GenerateEmbedding(context.Background(), "hello", false, -1)
	if err == nil {
		t.Error("expected connection error")
	}

	// 3. Context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.GenerateEmbedding(ctx, "hello", false, 1)
	if err == nil {
		t.Error("expected context canceled error")
	}

	// 4. Server error 500
	server500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer server500.Close()

	c500 := NewClient(server500.URL)
	_, err = c500.GenerateEmbedding(context.Background(), "hello", false, 0)
	if err == nil {
		t.Error("expected error on HTTP 500")
	}

	// 5. Server returns error in JSON
	serverErrJson := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Error: "model not loaded",
		})
	}))
	defer serverErrJson.Close()

	cErrJson := NewClient(serverErrJson.URL)
	_, err = cErrJson.GenerateEmbedding(context.Background(), "hello", false, 0)
	if err == nil || !strings.Contains(err.Error(), "model not loaded") {
		t.Errorf("expected 'model not loaded' error, got %v", err)
	}

	// 6. Server returns empty embedding array
	serverEmptyEmb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: []float32{},
		})
	}))
	defer serverEmptyEmb.Close()

	cEmptyEmb := NewClient(serverEmptyEmb.URL)
	_, err = cEmptyEmb.GenerateEmbedding(context.Background(), "hello", false, 0)
	if err == nil || !strings.Contains(err.Error(), "empty embedding returned") {
		t.Errorf("expected 'empty embedding returned' error, got %v", err)
	}

	// 7. Server returns non-JSON invalid payload
	serverBadJson := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not valid json"))
	}))
	defer serverBadJson.Close()

	cBadJson := NewClient(serverBadJson.URL)
	_, err = cBadJson.GenerateEmbedding(context.Background(), "hello", false, 0)
	if err == nil {
		t.Error("expected JSON unmarshal error")
	}

	var receivedModel string
	serverModel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req EmbeddingRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		receivedModel = req.Model
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{
			Embedding: []float32{1.0, 2.0},
		})
	}))
	defer serverModel.Close()

	// 8. Custom model via Config
	cfgModel := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL: serverModel.URL,
			Model:   "custom-bge-model",
		},
	})
	cModel := New(cfgModel)
	_, _ = cModel.GenerateEmbedding(context.Background(), "hello", false, 0)
	if receivedModel != "custom-bge-model" {
		t.Errorf("expected custom-bge-model, got %q", receivedModel)
	}
}

func TestMemory_BackfillMissingEmbeddings_EdgeCases(t *testing.T) {
	// 1. Nil args
	if _, err := BackfillMissingEmbeddings(context.Background(), nil, nil); err == nil {
		t.Error("expected error for nil args")
	}

	// 2. Closed DB
	closedDB, err := db.InitDB(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = closedDB.Close()
	c := NewClient("http://127.0.0.1:11434")
	if _, err := BackfillMissingEmbeddings(context.Background(), closedDB, c); err == nil {
		t.Error("expected error for closed DB")
	}

	// 3. 0 missing facts
	memDB, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer memDB.Close()

	n, err := BackfillMissingEmbeddings(context.Background(), memDB, c)
	if err != nil || n != 0 {
		t.Errorf("expected 0 backfilled, got %d, err: %v", n, err)
	}

	// 4. Backfill with embedding error (should continue without panic)
	_, _ = db.InsertFact(memDB, "user_pref", "Test fact", 1.0, "th-1", nil)
	nFail, _ := BackfillMissingEmbeddings(context.Background(), memDB, c)
	if nFail != 0 {
		t.Errorf("expected 0 backfilled on ollama failure, got %d", nFail)
	}

	// 5. Backfill with context cancel during loop
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = BackfillMissingEmbeddings(ctx, memDB, c)
}

func TestMemory_ExtractActiveConversationFacts_EdgeCases(t *testing.T) {
	// 1. Nil args
	if err := ExtractActiveConversationFacts(context.Background(), nil, nil, nil, 12); err == nil {
		t.Error("expected error for nil args")
	}

	// 2. Mutex contention / TryLock already locked
	dummyDB, _ := db.InitDB(":memory:")
	defer dummyDB.Close()
	extractionMutex.Lock()
	err := ExtractActiveConversationFacts(context.Background(), dummyDB, NewClient(""), func(ctx context.Context, p string) (string, error) { return "", nil }, 12)
	extractionMutex.Unlock()
	if err != nil {
		t.Errorf("expected nil error on overlapping execution, got %v", err)
	}

	// 3. Closed DB
	closedDB, _ := db.InitDB(filepath.Join(t.TempDir(), "closed_extract.db"))
	_ = closedDB.Close()
	if err := ExtractActiveConversationFacts(context.Background(), closedDB, NewClient(""), func(ctx context.Context, p string) (string, error) { return "", nil }, 12); err == nil {
		t.Error("expected error for closed DB")
	}

	// 4. 0 active conversations
	memDB, _ := db.InitDB(":memory:")
	defer memDB.Close()
	if err := ExtractActiveConversationFacts(context.Background(), memDB, NewClient(""), func(ctx context.Context, p string) (string, error) { return "", nil }, 12); err != nil {
		t.Errorf("expected nil for 0 conversations, got %v", err)
	}

	// 5. Context cancelled in loop
	now := time.Now().UTC()
	_ = db.InsertMessage(memDB, db.Message{
		ID: "m-cancel-1", ThreadID: "th-cancel", Status: db.StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = ExtractActiveConversationFacts(ctx, memDB, NewClient(""), func(ctx context.Context, p string) (string, error) { return "", nil }, 12)
}

func TestMemory_ProcessThreadFacts_EdgeCases(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	client := NewClient("http://127.0.0.1:11434")
	now := time.Now().UTC()
	_ = db.InsertMessage(database, db.Message{
		ID: "m-pf-1", ThreadID: "th-pf-1", Status: db.StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})

	// 1. Transcript file missing
	err = processThreadFacts(context.Background(), database, client, func(ctx context.Context, prompt string) (string, error) { return "", nil }, "th-pf-1")
	if err == nil || !strings.Contains(err.Error(), "transcript unavailable") {
		t.Errorf("expected transcript unavailable error, got %v", err)
	}

	// 2. Empty transcript file
	homeDir, _ := os.UserHomeDir()
	if homeDir == "" {
		homeDir = "/root"
	}
	logDir := filepath.Join(homeDir, ".gemini", "antigravity", "brain", "th-pf-empty", ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0755)
	_ = os.WriteFile(filepath.Join(logDir, "transcript.jsonl"), []byte("   \n"), 0644)
	defer func() { _ = os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity", "brain", "th-pf-empty")) }()

	_ = db.InsertMessage(database, db.Message{
		ID: "m-pf-empty", ThreadID: "th-pf-empty", Status: db.StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})
	err = processThreadFacts(context.Background(), database, client, func(ctx context.Context, prompt string) (string, error) { return "", nil }, "th-pf-empty")
	if err != nil {
		t.Errorf("expected nil error for empty transcript, got %v", err)
	}

	// 3. Transcript exists, but LLM call fails
	logDir2 := filepath.Join(homeDir, ".gemini", "antigravity", "brain", "th-pf-llm-fail", ".system_generated", "logs")
	_ = os.MkdirAll(logDir2, 0755)
	_ = os.WriteFile(filepath.Join(logDir2, "transcript.jsonl"), []byte("{\"step\":1,\"content\":\"hello\"}\n"), 0644)
	defer func() { _ = os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity", "brain", "th-pf-llm-fail")) }()

	_ = db.InsertMessage(database, db.Message{
		ID: "m-pf-fail", ThreadID: "th-pf-llm-fail", Status: db.StatusCompleted, CreatedAt: now, UpdatedAt: now,
	})
	err = processThreadFacts(context.Background(), database, client, func(ctx context.Context, prompt string) (string, error) {
		return "", fmt.Errorf("llm rate limited")
	}, "th-pf-llm-fail")
	if err == nil || !strings.Contains(err.Error(), "LLM fact extraction call failed") {
		t.Errorf("expected LLM failure error, got %v", err)
	}

	// 4. Transcript exists, LLM returns invalid JSON
	err = processThreadFacts(context.Background(), database, client, func(ctx context.Context, prompt string) (string, error) {
		return "invalid json output", nil
	}, "th-pf-llm-fail")
	if err == nil || !strings.Contains(err.Error(), "failed to parse extracted facts JSON") {
		t.Errorf("expected JSON parse error, got %v", err)
	}

	// 5. Transcript exists, LLM returns facts with empty text and embedding failure
	err = processThreadFacts(context.Background(), database, client, func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[{"category":"user_pref","fact_text":"","importance_score":1.0},{"category":"user_pref","fact_text":"Valid fact","importance_score":1.0}]}`, nil
	}, "th-pf-llm-fail")
	if err != nil {
		t.Errorf("expected nil error when embedding fails (logged warning), got %v", err)
	}
}

func TestMemory_LoadThreadTranscript_LongFileAndSessionLookup(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	homeDir, _ := os.UserHomeDir()
	if homeDir == "" {
		homeDir = "/root"
	}

	sessID := "sess-custom-guid-12345"
	threadID := "thread-mapped-999"
	_ = db.SaveSessionID(database, threadID, sessID)

	// Create long transcript > 20000 bytes in sessID directory under transcript_full.jsonl
	logDir := filepath.Join(homeDir, ".gemini", "antigravity", "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0755)
	longContent := strings.Repeat("{\"step\":1,\"content\":\"long conversation message snippet\"}\n", 500)
	_ = os.WriteFile(filepath.Join(logDir, "transcript_full.jsonl"), []byte(longContent), 0644)
	defer func() { _ = os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity", "brain", sessID)) }()

	text, err := loadThreadTranscript(database, threadID)
	if err != nil {
		t.Fatalf("loadThreadTranscript failed: %v", err)
	}
	if len(text) == 0 || len(text) > 20000 {
		t.Errorf("expected truncated transcript <= 20000 bytes, got %d", len(text))
	}
}

func TestMemory_Search_EdgeCases(t *testing.T) {
	// 1. sanitizeQueryText long text > 1000
	longQuery := strings.Repeat("word ", 300)
	sanitized := sanitizeQueryText(longQuery)
	if len(sanitized) > 1000 {
		t.Errorf("expected length <= 1000, got %d", len(sanitized))
	}

	// 2. DotProduct edge cases
	if DotProduct(nil, []float32{1.0}) != 0 {
		t.Error("expected 0 for nil slice")
	}
	if DotProduct([]float32{1.0}, []float32{1.0, 2.0}) != 0 {
		t.Error("expected 0 for mismatched lengths")
	}

	// 3. RankFacts edge cases
	if RankFacts(nil, nil, 0, 0) != nil {
		t.Error("expected nil for empty inputs")
	}
	facts := []db.FactWithEmbedding{
		{
			Fact:      db.Fact{ID: 1, Category: "user_pref", FactText: "Fact 1", Importance: 1.0},
			Embedding: []float32{1.0}, // mismatched len with queryVec len 2
		},
	}
	ranked := RankFacts([]float32{1.0, 0.0}, facts, -1, -1)
	if len(ranked) != 0 {
		t.Errorf("expected 0 ranked facts due to dimension mismatch, got %d", len(ranked))
	}

	// 4. RetrieveRelevantFacts nil args and long text
	res, err := RetrieveRelevantFacts(context.Background(), nil, nil, "", 5)
	if res != nil || err != nil {
		t.Error("expected nil, nil for nil args")
	}

	// 5. RetrieveRelevantFacts with Ollama failure (graceful fallback)
	memDB, _ := db.InitDB(":memory:")
	defer memDB.Close()
	cFail := NewClient("http://127.0.0.1:59999")
	resFail, errFail := RetrieveRelevantFacts(context.Background(), memDB, cFail, "What is my name?", 5)
	if resFail != nil || errFail != nil {
		t.Errorf("expected nil, nil on vector embedding failure, got %v, %v", resFail, errFail)
	}

	// 6. FormatMemoryContext with empty category defaulting to 'general'
	fmtOut := FormatMemoryContext([]db.Fact{
		{Category: "", FactText: "Fact with no category"},
	})
	if !strings.Contains(fmtOut, "- [general] Fact with no category") {
		t.Errorf("expected '[general]', got %q", fmtOut)
	}
	if FormatMemoryContext(nil) != "" {
		t.Error("expected empty string for nil facts")
	}
}






