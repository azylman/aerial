package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// --- Test Fixtures & Mocks ---

type MockOllamaClient struct {
	embedding []float32
	err       error
	lastText  string
}

func (m *MockOllamaClient) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	m.lastText = text
	if m.err != nil {
		return nil, m.err
	}
	return m.embedding, nil
}

type mockErrReadCloser struct{}

func (*mockErrReadCloser) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func (*mockErrReadCloser) Close() error {
	return nil
}

// --- Unit Tests ---

func TestResolveConfigPath(t *testing.T) {
	oldLocal := localConfigPath
	defer func() {
		localConfigPath = oldLocal
	}()
	tmpDir := t.TempDir()
	existingLocal := filepath.Join(tmpDir, "local.yaml")
	_ = os.WriteFile(existingLocal, []byte("port: '4015'\n"), 0o644)
	missingLocal := filepath.Join(tmpDir, "nonexistent.yaml")

	tests := []struct {
		name      string
		localPath string
		envPath   string
		fallback  string
		want      string
	}{
		{
			name:      "EnvConfigPathSet",
			localPath: missingLocal,
			envPath:   "/custom/path.yaml",
			fallback:  "/default/path.yaml",
			want:      "/custom/path.yaml",
		},
		{
			name:      "LocalConfigFileExists",
			localPath: existingLocal,
			fallback:  "/default/path.yaml",
			want:      existingLocal,
		},
		{
			name:      "LocalConfigPrecedesDefaultConfigPath",
			localPath: existingLocal,
			fallback:  DefaultConfigPath,
			want:      existingLocal,
		},
		{
			name:      "FallbackUsed",
			localPath: missingLocal,
			fallback:  "/default/path.yaml",
			want:      "/default/path.yaml",
		},
		{
			name:      "EmptyFallbackUsesDefaultConfigPath",
			localPath: missingLocal,
			fallback:  "",
			want:      DefaultConfigPath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			localConfigPath = tc.localPath
			t.Setenv("CONFIG_PATH", tc.envPath)
			got := ResolveConfigPath(tc.fallback)
			if got != tc.want {
				t.Errorf("ResolveConfigPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		wantNear float64
	}{
		{"Identical", []float32{1, 0, 0}, []float32{1, 0, 0}, 1.0},
		{"Orthogonal", []float32{1, 0, 0}, []float32{0, 1, 0}, 0.0},
		{"Opposite", []float32{1, 0, 0}, []float32{-1, 0, 0}, -1.0},
		{"DifferentLength", []float32{1, 0}, []float32{1, 0, 0}, 0.0},
		{"EmptyA", []float32{}, []float32{1, 0}, 0.0},
		{"ZeroVectors", []float32{0, 0}, []float32{0, 0}, 0.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CosineSimilarity(tc.a, tc.b)
			if diff := got - tc.wantNear; diff > 1e-4 || diff < -1e-4 {
				t.Errorf("CosineSimilarity() = %f, want ~%f", got, tc.wantNear)
			}
		})
	}
}

func TestMMRSelect(t *testing.T) {
	cand1 := ScoredFactWithEmbedding{
		Fact:      Fact{ID: 1, FactText: "Fact 1"},
		Embedding: []float32{1.0, 0.0, 0.0},
		Score:     0.90,
	}
	cand2 := ScoredFactWithEmbedding{
		Fact:      Fact{ID: 2, FactText: "Fact 2 Duplicate of 1"},
		Embedding: []float32{0.99, 0.01, 0.0},
		Score:     0.88,
	}
	cand3 := ScoredFactWithEmbedding{
		Fact:      Fact{ID: 3, FactText: "Fact 3 Diverse"},
		Embedding: []float32{0.0, 1.0, 0.0},
		Score:     0.85,
	}

	tests := []struct {
		name       string
		candidates []ScoredFactWithEmbedding
		lambda     float64
		limit      int
		wantCount  int
		wantFirst  int64
		wantSecond int64
	}{
		{
			name:       "EmptyCandidates",
			candidates: nil,
			limit:      5,
			wantCount:  0,
		},
		{
			name:       "LimitZero",
			candidates: []ScoredFactWithEmbedding{cand1},
			limit:      0,
			wantCount:  0,
		},
		{
			name:       "DiversitySelectsDiverseOverDuplicate",
			candidates: []ScoredFactWithEmbedding{cand1, cand2, cand3},
			lambda:     0.5,
			limit:      2,
			wantCount:  2,
			wantFirst:  1,
			wantSecond: 3,
		},
		{
			name:       "DefaultLambdaWhenInvalid",
			candidates: []ScoredFactWithEmbedding{cand1, cand2},
			lambda:     -1.0,
			limit:      2,
			wantCount:  2,
			wantFirst:  1,
		},
		{
			name: "AllEqualScores",
			candidates: []ScoredFactWithEmbedding{
				{Fact: Fact{ID: 10}, Embedding: []float32{1, 0}, Score: 0.5},
				{Fact: Fact{ID: 20}, Embedding: []float32{0, 1}, Score: 0.5},
			},
			lambda:    0.7,
			limit:     2,
			wantCount: 2,
			wantFirst: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MMRSelect(tc.candidates, tc.lambda, tc.limit)
			if len(got) != tc.wantCount {
				t.Fatalf("MMRSelect() returned %d facts, want %d", len(got), tc.wantCount)
			}
			if tc.wantCount > 0 && tc.wantFirst > 0 && got[0].Fact.ID != tc.wantFirst {
				t.Errorf("MMRSelect()[0].Fact.ID = %d, want %d", got[0].Fact.ID, tc.wantFirst)
			}
			if tc.wantCount > 1 && tc.wantSecond > 0 && got[1].Fact.ID != tc.wantSecond {
				t.Errorf("MMRSelect()[1].Fact.ID = %d, want %d", got[1].Fact.ID, tc.wantSecond)
			}
		})
	}
}

func TestNewOllamaClient(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *OllamaConfig
		wantURL   string
		wantModel string
		wantCtx   int
	}{
		{
			name:      "NilConfigEmptyURL",
			cfg:       nil,
			wantURL:   "",
			wantModel: DefaultOllamaModel,
			wantCtx:   DefaultOllamaNumCtx,
		},
		{
			name:      "EmptyFieldsEmptyURL",
			cfg:       &OllamaConfig{BaseURL: "  ", Model: "", NumCtx: 0},
			wantURL:   "",
			wantModel: DefaultOllamaModel,
			wantCtx:   DefaultOllamaNumCtx,
		},
		{
			name:      "CustomFieldsTrimSlash",
			cfg:       &OllamaConfig{BaseURL: "http://ollama:11434/", Model: "bge", NumCtx: 256},
			wantURL:   "http://ollama:11434",
			wantModel: "bge",
			wantCtx:   256,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewOllamaClient(tc.cfg)
			if c.baseURL != tc.wantURL || c.model != tc.wantModel || c.numCtx != tc.wantCtx {
				t.Errorf("NewOllamaClient() = %+v, want url=%q model=%q ctx=%d", c, tc.wantURL, tc.wantModel, tc.wantCtx)
			}
		})
	}
}

func TestOllamaClient_GenerateEmbedding(t *testing.T) {
	var mu sync.Mutex
	var gotPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Prompt string `json:"prompt"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		gotPrompt = req.Prompt
		mu.Unlock()
		_, _ = w.Write([]byte(`{"embedding": [0.1, 0.2, 0.3]}`))
	}))
	defer server.Close()

	tests := []struct {
		name       string
		baseURL    string
		input      string
		isQuery    bool
		prefix     string
		wantPrompt string
		wantVecLen int
		wantErr    string
	}{
		{name: "SuccessDocument", baseURL: server.URL, input: "test fact text", prefix: "search: ", wantPrompt: "test fact text", wantVecLen: 3},
		{name: "SuccessQueryWithPrefix", baseURL: server.URL, input: "search query", isQuery: true, prefix: "search: ", wantPrompt: "search: search query", wantVecLen: 3},
		{name: "UnconfiguredBaseURLError", baseURL: "", input: "test text", wantErr: "ollama base URL is not configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			gotPrompt = ""
			mu.Unlock()
			client := NewOllamaClient(&OllamaConfig{BaseURL: tc.baseURL, QueryPrefix: tc.prefix})
			vec, err := client.GenerateEmbedding(context.Background(), tc.input, tc.isQuery, 0)
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr {
				t.Fatalf("GenerateEmbedding() err = %q, want %q", gotErr, tc.wantErr)
			}
			mu.Lock()
			prompt := gotPrompt
			mu.Unlock()
			if len(vec) != tc.wantVecLen || prompt != tc.wantPrompt {
				t.Errorf("GenerateEmbedding() len = %d prompt = %q, want len %d prompt %q", len(vec), prompt, tc.wantVecLen, tc.wantPrompt)
			}
		})
	}
}

func TestFormatMemoryContext(t *testing.T) {
	tests := []struct {
		name         string
		facts        []Fact
		wantContains []string
		wantEmpty    bool
	}{
		{
			name:      "EmptyFacts",
			facts:     nil,
			wantEmpty: true,
		},
		{
			name: "FormattedFacts",
			facts: []Fact{
				{ID: 1, FactText: "Prefers Pacific timezone"},
				{ID: 2, FactText: "Default category fallback"},
			},
			wantContains: []string{
				"<retrieved_memory>",
				"- [id:1] Prefers Pacific timezone",
				"- [id:2] Default category fallback",
				"</retrieved_memory>",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatMemoryContext(tc.facts)
			if tc.wantEmpty && got != "" {
				t.Errorf("FormatMemoryContext() = %q, want empty", got)
			}
			for _, substr := range tc.wantContains {
				if !strings.Contains(got, substr) {
					t.Errorf("FormatMemoryContext() missing %q, got: %s", substr, got)
				}
			}
		})
	}
}

// toolFactRow is one facts row as read back by toolFactRows.
type toolFactRow struct {
	id             int64
	text           string
	reinforceCount int
	sourceAgent    string
}

// toolFactRows reads every facts row ordered by id.
func toolFactRows(t *testing.T, db DBTX) []toolFactRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT id, fact_text, reinforce_count, source_agent FROM facts ORDER BY id`)
	if err != nil {
		t.Fatalf("query facts: %v", err)
	}
	defer rows.Close()
	var got []toolFactRow
	for rows.Next() {
		var r toolFactRow
		if err := rows.Scan(&r.id, &r.text, &r.reinforceCount, &r.sourceAgent); err != nil {
			t.Fatalf("scan facts: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read facts: %v", err)
	}
	return got
}

// toolFactTexts returns the fact texts in order.
func toolFactTexts(facts []Fact) []string {
	texts := make([]string, 0, len(facts))
	for _, f := range facts {
		texts = append(texts, f.FactText)
	}
	return texts
}

func TestToolHandler_MemorySearch(t *testing.T) {
	t.Parallel()
	// Seeded facts: "User lives in Metropolis" (dim 0) and "User works in tech"
	// (dim 1); the query embedding is dim 0. Real hybrid RRF (k=60, 0.70 dense /
	// 0.30 sparse, x60) at reinforce_count 1 and age ~0:
	//   query "Metropolis": Metropolis = (0.70/61 + 0.30/61)*60 ≈ 0.98, tech = 0.70/62*60 ≈ 0.68.
	//   query "tech":       tech = (0.70/62 + 0.30/61)*60 ≈ 0.97, Metropolis = 0.70/61*60 ≈ 0.69.
	// Both clear the 0.20 floor, so the FTS hit decides the order.
	tests := []struct {
		name      string
		args      MemorySearchArgs
		ollamaErr error
		sabotage  string
		wantErr   bool
		wantFacts []string
	}{
		{name: "SuccessfulSearch", args: MemorySearchArgs{Query: "Metropolis", Limit: 5}, wantFacts: []string{"User lives in Metropolis", "User works in tech"}},
		{name: "TextMatchOutranksVector", args: MemorySearchArgs{Query: "tech", Limit: 5}, wantFacts: []string{"User works in tech", "User lives in Metropolis"}},
		{name: "LimitOneKeepsTopHit", args: MemorySearchArgs{Query: "Metropolis", Limit: 1}, wantFacts: []string{"User lives in Metropolis"}},
		{name: "NegativeLimitAndOllamaWarningFallback", args: MemorySearchArgs{Query: "Metropolis", Limit: -1}, ollamaErr: errors.New("ollama offline"), wantFacts: []string{}},
		{name: "HighLimitClamped", args: MemorySearchArgs{Query: "Metropolis", Limit: 100}, wantFacts: []string{"User lives in Metropolis", "User works in tech"}},
		{name: "EmptyQueryError", args: MemorySearchArgs{Query: ""}, wantErr: true},
		{name: "StoreSearchFails", args: MemorySearchArgs{Query: "Metropolis"}, sabotage: sabotageFactSearch, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			store := NewSQLStore(db)
			if _, err := store.InsertFact(context.Background(), "User lives in Metropolis", testEmbedding(1.0)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if _, err := store.InsertFact(context.Background(), "User works in tech", testEmbeddingAt(1, 1.0)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			execSQL(t, db, tc.sabotage)
			ollama := &MockOllamaClient{embedding: testEmbedding(1.0), err: tc.ollamaErr}
			handler := NewToolHandler(&Config{}, store, ollama)
			out, err := handler.MemorySearch(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemorySearch() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got := toolFactTexts(out.Facts); !slices.Equal(got, tc.wantFacts) {
				t.Errorf("facts = %q, want %q", got, tc.wantFacts)
			}
			if out.Count != len(tc.wantFacts) {
				t.Errorf("out.Count = %d, want %d", out.Count, len(tc.wantFacts))
			}
			if out.Context != "" {
				t.Errorf("expected empty out.Context on MemorySearchOutput, got %q", out.Context)
			}
		})
	}
}

func TestToolHandler_MemoryStore(t *testing.T) {
	t.Parallel()
	// Every row starts from one seeded fact: id 1 "Existing Fact" (dim 0, aerial).
	seeded := toolFactRow{id: 1, text: "Existing Fact", reinforceCount: 1, sourceAgent: "aerial"}

	tests := []struct {
		name         string
		args         MemoryStoreArgs
		emb          []float32
		ollamaErr    error
		sabotage     string
		handlerStack string
		wantErr      bool
		wantAction   string
		wantID       int64
		wantRows     []toolFactRow
	}{
		{
			name:       "ReinforceDuplicateFact",
			args:       MemoryStoreArgs{FactText: "Brand new fact"},
			emb:        testEmbedding(1.0),
			wantAction: "reinforced",
			wantID:     1,
			wantRows:   []toolFactRow{{id: 1, text: "Existing Fact", reinforceCount: 2, sourceAgent: "aerial"}},
		},
		{
			name:     "ReinforceDuplicateError",
			args:     MemoryStoreArgs{FactText: "Brand new fact"},
			emb:      testEmbedding(1.0),
			sabotage: sabotageReinforceFact,
			wantErr:  true,
			wantRows: []toolFactRow{seeded},
		},
		{
			name:       "InsertUniqueFact",
			args:       MemoryStoreArgs{FactText: "Unique fact"},
			emb:        testEmbeddingAt(2, 1.0),
			wantAction: "inserted",
			wantID:     2,
			wantRows:   []toolFactRow{seeded, {id: 2, text: "Unique fact", reinforceCount: 1, sourceAgent: "aerial"}},
		},
		{
			name:         "InsertWithTargetAgentAndStack",
			args:         MemoryStoreArgs{FactText: "Target agent fact", TargetAgent: "agent-alpha"},
			emb:          testEmbeddingAt(4, 1.0),
			handlerStack: "agent-beta",
			wantAction:   "inserted",
			wantID:       2,
			wantRows:     []toolFactRow{seeded, {id: 2, text: "Target agent fact", reinforceCount: 1, sourceAgent: "agent-alpha"}},
		},
		{
			name:         "InsertWithStackFallback",
			args:         MemoryStoreArgs{FactText: "Stack fallback fact"},
			emb:          testEmbeddingAt(5, 1.0),
			handlerStack: "agent-beta",
			wantAction:   "inserted",
			wantID:       2,
			wantRows:     []toolFactRow{seeded, {id: 2, text: "Stack fallback fact", reinforceCount: 1, sourceAgent: "agent-beta"}},
		},
		{
			name:      "InsertWithOllamaErrorFails",
			args:      MemoryStoreArgs{FactText: "Fact with error"},
			ollamaErr: errors.New("embed error"),
			wantErr:   true,
			wantRows:  []toolFactRow{seeded},
		},
		{
			name:       "SupersedeExistingFact",
			args:       MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: 1},
			emb:        testEmbedding(1.0),
			wantAction: "updated",
			wantID:     1,
			wantRows:   []toolFactRow{{id: 1, text: "Updated Fact", reinforceCount: 2, sourceAgent: "aerial"}},
		},
		{
			name:     "SupersedeNonexistentFails",
			args:     MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: 999999},
			emb:      testEmbedding(1.0),
			wantErr:  true,
			wantRows: []toolFactRow{seeded},
		},
		{
			name:     "SupersedeStoreError",
			args:     MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: 1},
			emb:      testEmbedding(1.0),
			sabotage: sabotageUpdateFact,
			wantErr:  true,
			wantRows: []toolFactRow{seeded},
		},
		{
			name:     "EmptyFactTextFails",
			args:     MemoryStoreArgs{FactText: ""},
			wantErr:  true,
			wantRows: []toolFactRow{seeded},
		},
		{
			name:     "InsertStoreFails",
			args:     MemoryStoreArgs{FactText: "Failing fact"},
			emb:      testEmbeddingAt(3, 1.0),
			sabotage: sabotageInsertFact,
			wantErr:  true,
			wantRows: []toolFactRow{seeded},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			store := NewSQLStore(db)
			if _, err := store.InsertFact(context.Background(), "Existing Fact", testEmbedding(1.0)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			execSQL(t, db, tc.sabotage)
			ollama := &MockOllamaClient{embedding: tc.emb, err: tc.ollamaErr}
			handler := NewToolHandler(&Config{Stack: tc.handlerStack}, store, ollama)
			out, err := handler.MemoryStore(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemoryStore() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if out.Action != tc.wantAction {
				t.Errorf("out.Action = %q, want %q", out.Action, tc.wantAction)
			}
			if out.FactID != tc.wantID {
				t.Errorf("out.FactID = %d, want %d", out.FactID, tc.wantID)
			}
			if got := toolFactRows(t, db); !slices.Equal(got, tc.wantRows) {
				t.Errorf("facts = %+v, want %+v", got, tc.wantRows)
			}
		})
	}
}

func TestToolHandler_MemoryStoreWithInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		input      FactInput
		wantAction string
		wantErr    bool
		wantRows   []toolFactRow
	}{
		{
			name:       "stamps custom source agent",
			input:      FactInput{FactText: "Durable fact preference for tea", SourceAgent: "agent-alpha"},
			wantAction: "inserted",
			wantRows:   []toolFactRow{{id: 1, text: "Durable fact preference for tea", reinforceCount: 1, sourceAgent: "agent-alpha"}},
		},
		{
			name:       "empty source agent defaults to aerial",
			input:      FactInput{FactText: "Durable fact preference for coffee", SourceAgent: ""},
			wantAction: "inserted",
			wantRows:   []toolFactRow{{id: 1, text: "Durable fact preference for coffee", reinforceCount: 1, sourceAgent: "aerial"}},
		},
		{
			name:    "empty fact text returns error",
			input:   FactInput{FactText: "   ", SourceAgent: "agent-beta"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			ollama := &MockOllamaClient{embedding: testEmbedding(1.0)}
			handler := NewToolHandler(&Config{Stack: "agent-beta"}, NewSQLStore(db), ollama)
			out, err := handler.MemoryStoreWithInput(context.Background(), tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemoryStoreWithInput() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if out.Action != tc.wantAction {
				t.Errorf("out.Action = %q, want %q", out.Action, tc.wantAction)
			}
			if got := toolFactRows(t, db); !slices.Equal(got, tc.wantRows) {
				t.Errorf("facts = %+v, want %+v", got, tc.wantRows)
			}
		})
	}
}

func TestToolHandler_GetContext(t *testing.T) {
	t.Parallel()
	// One seeded fact, id 1 "Profile fact" (dim 0); the query embedding is dim 0,
	// so the vector hit alone scores 0.70/61*60 ≈ 0.69 > 0.20.
	const profileContext = "<retrieved_memory>\n- [id:1] Profile fact\n</retrieved_memory>\n"
	tests := []struct {
		name        string
		query       string
		limit       int
		cancelCtx   bool
		ollamaErr   error
		sabotage    string
		wantErr     bool
		wantContext string
	}{
		{name: "EmptyQuery", query: "", limit: 5},
		{name: "ValidQuery", query: "Profile", limit: 5, wantContext: profileContext},
		{name: "NegativeLimitAndOllamaWarning", query: "Profile", limit: -1, ollamaErr: errors.New("ollama down")},
		{name: "HighLimitClamped", query: "Profile", limit: 50, wantContext: profileContext},
		{name: "CandidateSearchErrorGracefulEmpty", query: "Profile", limit: 5, sabotage: sabotageFactSearch},
		{name: "CanceledContextReturnsError", query: "Profile", limit: 5, cancelCtx: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			store := NewSQLStore(db)
			if _, err := store.InsertFact(context.Background(), "Profile fact", testEmbedding(1.0)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			execSQL(t, db, tc.sabotage)
			ollama := &MockOllamaClient{embedding: testEmbedding(1.0), err: tc.ollamaErr}
			handler := NewToolHandler(&Config{}, store, ollama)
			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			res, err := handler.GetContext(ctx, tc.query, tc.limit)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetContext() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && res.Context != tc.wantContext {
				t.Errorf("res.Context = %q, want %q", res.Context, tc.wantContext)
			}
		})
	}
}
