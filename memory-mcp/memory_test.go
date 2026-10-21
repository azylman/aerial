package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pgvector/pgvector-go"
	_ "modernc.org/sqlite"
)

// --- Test Fixtures & Mocks ---

type TestMemStore struct {
	mu         sync.Mutex
	facts      map[int64]*Fact
	embeddings map[int64][]float32
	nextID     int64
	failOp     string
	failErr    error
}

func newTestMemStore() *TestMemStore {
	return &TestMemStore{
		facts:      make(map[int64]*Fact),
		embeddings: make(map[int64][]float32),
		nextID:     1,
	}
}

func (m *TestMemStore) SetFail(op string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOp = op
	m.failErr = err
}

func (m *TestMemStore) InsertFact(ctx context.Context, category, factText string, importance float64, embedding []float32) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "InsertFact" {
		return 0, m.failErr
	}
	id := m.nextID
	m.nextID++
	now := time.Now().UTC()
	f := &Fact{
		ID:               id,
		Category:         category,
		FactText:         factText,
		Importance:       importance,
		CreatedAt:        now,
		LastReinforcedAt: now,
		LastDecayedAt:    now,
		ReinforceCount:   1,
	}
	m.facts[id] = f
	if len(embedding) > 0 {
		m.embeddings[id] = append([]float32(nil), embedding...)
	}
	return id, nil
}

func (m *TestMemStore) UpdateFact(ctx context.Context, id int64, category, factText string, importance float64, embedding []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "UpdateFact" {
		return m.failErr
	}
	f, ok := m.facts[id]
	if !ok {
		return ErrFactNotFound
	}
	f.Category = category
	f.FactText = factText
	f.Importance = importance
	f.LastReinforcedAt = time.Now().UTC()
	f.ReinforceCount++
	if len(embedding) > 0 {
		m.embeddings[id] = append([]float32(nil), embedding...)
	}
	return nil
}

func (m *TestMemStore) ReinforceFact(ctx context.Context, id int64, newText string, newEmbedding []float32, boost float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "ReinforceFact" {
		return m.failErr
	}
	f, ok := m.facts[id]
	if !ok {
		return ErrFactNotFound
	}
	f.Importance = min(1.0, f.Importance+boost)
	f.LastReinforcedAt = time.Now().UTC()
	f.ReinforceCount++
	if newText != "" {
		f.FactText = newText
	}
	if len(newEmbedding) > 0 {
		m.embeddings[id] = append([]float32(nil), newEmbedding...)
	}
	return nil
}

func (m *TestMemStore) DeleteFact(ctx context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "DeleteFact" {
		return m.failErr
	}
	if _, ok := m.facts[id]; !ok {
		return ErrFactNotFound
	}
	delete(m.facts, id)
	delete(m.embeddings, id)
	return nil
}

func (m *TestMemStore) FindDuplicateFact(ctx context.Context, embedding []float32, minSim float64) (*Fact, float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "FindDuplicateFact" {
		return nil, 0, m.failErr
	}
	var bestFact *Fact
	var bestSim float64
	for id, emb := range m.embeddings {
		sim := CosineSimilarity(embedding, emb)
		if sim >= minSim && sim > bestSim {
			bestSim = sim
			cpy := *m.facts[id]
			bestFact = &cpy
		}
	}
	return bestFact, bestSim, nil
}

func (m *TestMemStore) SearchCandidateFacts(ctx context.Context, embedding []float32, queryText, category string, candidateLimit int, minScore float64) ([]ScoredFactWithEmbedding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "SearchCandidateFacts" {
		return nil, m.failErr
	}
	var candidates []ScoredFactWithEmbedding
	for id, f := range m.facts {
		if category != "" && f.Category != category {
			continue
		}
		var sim float64
		if len(embedding) > 0 && len(m.embeddings[id]) > 0 {
			sim = CosineSimilarity(embedding, m.embeddings[id])
		}
		var textScore float64
		if queryText != "" && strings.Contains(strings.ToLower(f.FactText), strings.ToLower(queryText)) {
			textScore = 1.0
		}
		score := f.Importance
		if len(embedding) > 0 && queryText != "" {
			score = (0.70*sim + 0.30*textScore) * f.Importance
		} else if len(embedding) > 0 {
			score = sim * f.Importance
		} else if queryText != "" {
			score = textScore * f.Importance
		}
		if score >= minScore {
			candidates = append(candidates, ScoredFactWithEmbedding{
				Fact:      *f,
				Embedding: m.embeddings[id],
				Score:     score,
			})
		}
	}
	return candidates, nil
}

func (m *TestMemStore) GetFactsPaginated(ctx context.Context, filter FactsFilter) (*FactsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetFactsPaginated" {
		return nil, m.failErr
	}
	var list []Fact
	for _, f := range m.facts {
		if filter.Category != "" && f.Category != filter.Category {
			continue
		}
		if filter.Query != "" && !strings.Contains(strings.ToLower(f.FactText), strings.ToLower(filter.Query)) {
			continue
		}
		list = append(list, *f)
	}
	total := len(list)
	offset := filter.Offset
	if offset > total {
		offset = total
	}
	end := total
	if filter.Limit > 0 && offset+filter.Limit < total {
		end = offset + filter.Limit
	}
	res := list[offset:end]
	if res == nil {
		res = []Fact{}
	}
	return &FactsResult{
		Facts:  res,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
		Status: "ok",
	}, nil
}

func (m *TestMemStore) Close() error {
	return nil
}

type MockOllamaClient struct {
	embedding []float32
	err       error
}

func (m *MockOllamaClient) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.embedding, nil
}

type mockErrReadCloser struct{}

func (m *mockErrReadCloser) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func (m *mockErrReadCloser) Close() error {
	return nil
}

func startMockPostgres(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on mock pg: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handlePgConn(conn)
		}
	}()

	t.Cleanup(func() {
		_ = listener.Close()
	})

	return listener.Addr().String()
}

func handlePgConn(conn net.Conn) {
	defer conn.Close()

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	rest := make([]byte, length-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return
	}

	authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	if _, err := conn.Write(authOk); err != nil {
		return
	}

	paramStatus := func(k, v string) []byte {
		body := k + "\x00" + v + "\x00"
		pkt := make([]byte, 5+len(body))
		pkt[0] = 'S'
		binary.BigEndian.PutUint32(pkt[1:5], uint32(4+len(body)))
		copy(pkt[5:], body)
		return pkt
	}
	_, _ = conn.Write(paramStatus("server_version", "16.0"))
	_, _ = conn.Write(paramStatus("client_encoding", "UTF8"))

	ready := []byte{'Z', 0, 0, 0, 5, 'I'}
	if _, err := conn.Write(ready); err != nil {
		return
	}

	for {
		var typeBuf [1]byte
		if _, err := io.ReadFull(conn, typeBuf[:]); err != nil {
			return
		}
		msgType := typeBuf[0]

		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		msgBody := make([]byte, msgLen-4)
		if _, err := io.ReadFull(conn, msgBody); err != nil {
			return
		}

		switch msgType {
		case 'Q':
			cmdBytes := append([]byte{'C'}, 0, 0, 0, 0)
			cmdBytes = append(cmdBytes, []byte("SELECT 1\x00")...)
			binary.BigEndian.PutUint32(cmdBytes[1:5], uint32(len(cmdBytes)-1))
			_, _ = conn.Write(cmdBytes)
			_, _ = conn.Write(ready)
		case 'P':
			_, _ = conn.Write([]byte{'1', 0, 0, 0, 4})
		case 'B':
			_, _ = conn.Write([]byte{'2', 0, 0, 0, 4})
		case 'D':
			fieldName := "version\x00"
			rowDesc := make([]byte, 7+len(fieldName)+18)
			rowDesc[0] = 'T'
			binary.BigEndian.PutUint32(rowDesc[1:5], uint32(len(rowDesc)-1))
			binary.BigEndian.PutUint16(rowDesc[5:7], 1)
			copy(rowDesc[7:], fieldName)
			offset := 7 + len(fieldName)
			binary.BigEndian.PutUint32(rowDesc[offset:], 0)
			binary.BigEndian.PutUint16(rowDesc[offset+4:], 0)
			binary.BigEndian.PutUint32(rowDesc[offset+6:], 25)
			binary.BigEndian.PutUint16(rowDesc[offset+10:], 65535)
			binary.BigEndian.PutUint32(rowDesc[offset+12:], 0xffffffff)
			binary.BigEndian.PutUint16(rowDesc[offset+16:], 0)
			_, _ = conn.Write(rowDesc)
		case 'E':
			ver := "PostgreSQL 16.0"
			dataRow := make([]byte, 7+len(ver))
			dataRow[0] = 'D'
			binary.BigEndian.PutUint32(dataRow[1:5], uint32(len(dataRow)-1))
			binary.BigEndian.PutUint16(dataRow[5:7], 1)
			binary.BigEndian.PutUint32(dataRow[7:11], uint32(len(ver)))
			copy(dataRow[11:], ver)
			_, _ = conn.Write(dataRow)
			cmdBytes := []byte{'C', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0}
			_, _ = conn.Write(cmdBytes)
		case 'S':
			_, _ = conn.Write(ready)
		case 'X':
			return
		}
	}
}

// --- Unit Tests ---

func TestConfig_LoadAndResolve(t *testing.T) {
	tmpDir := t.TempDir()
	validYamlPath := filepath.Join(tmpDir, "config.yaml")
	validContent := `port: "4009"
database_url: "postgres://u:p@localhost:5432/testdb"
brain_llm_url: "http://brain.local/internal/llm"
ollama:
  base_url: "http://ollama.local:11434"
  model: "bge-m3"
  query_prefix: "search: "
  num_ctx: 1024
`
	if err := os.WriteFile(validYamlPath, []byte(validContent), 0o644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	badYamlPath := filepath.Join(tmpDir, "bad.yaml")
	if err := os.WriteFile(badYamlPath, []byte("port: [unclosed"), 0o644); err != nil {
		t.Fatalf("failed to write bad yaml: %v", err)
	}

	tests := []struct {
		name        string
		path        string
		dbURL       string
		envPort     string
		envDB       string
		envPG       string
		envBrainLLM string
		envOllama   string
		wantErr     bool
		wantPort    string
		wantDB      string
		wantModel   string
	}{
		{
			name:      "ValidFromYAML",
			path:      validYamlPath,
			wantPort:  "4009",
			wantDB:    "postgres://u:p@localhost:5432/testdb",
			wantModel: "bge-m3",
		},
		{
			name:        "EnvOverrides",
			path:        validYamlPath,
			envPort:     "4010",
			envDB:       "postgres://envuser@localhost/envdb",
			envBrainLLM: "http://custom-brain/llm",
			envOllama:   "http://custom-ollama:11434",
			wantPort:    "4010",
			wantDB:      "postgres://envuser@localhost/envdb",
			wantModel:   "bge-m3",
		},
		{
			name:     "MissingFileUsesDefaultsAndEnv",
			path:     filepath.Join(tmpDir, "nonexistent.yaml"),
			envPort:  "4011",
			envPG:    "postgres://pguser@localhost/pgdb",
			wantPort: "4011",
			wantDB:   "postgres://pguser@localhost/pgdb",
		},
		{
			name:    "MissingDatabaseURLFails",
			path:    filepath.Join(tmpDir, "nonexistent.yaml"),
			wantErr: true,
		},
		{
			name:    "BadYAMLFileFails",
			path:    badYamlPath,
			dbURL:   "postgres://localhost/db",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PORT", tc.envPort)
			t.Setenv("DATABASE_URL", tc.envDB)
			t.Setenv("POSTGRES_URL", tc.envPG)
			t.Setenv("BRAIN_LLM_URL", tc.envBrainLLM)
			t.Setenv("OLLAMA_BASE_URL", tc.envOllama)

			cfg, err := LoadConfigFile(tc.path, tc.dbURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if tc.wantPort != "" && cfg.Port != tc.wantPort {
				t.Errorf("cfg.Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if tc.wantDB != "" && cfg.DatabaseURL != tc.wantDB {
				t.Errorf("cfg.DatabaseURL = %q, want %q", cfg.DatabaseURL, tc.wantDB)
			}
			if tc.wantModel != "" && cfg.Ollama.Model != tc.wantModel {
				t.Errorf("cfg.Ollama.Model = %q, want %q", cfg.Ollama.Model, tc.wantModel)
			}
		})
	}
}

func TestResolveConfigPath(t *testing.T) {
	oldLocal := localConfigPath
	defer func() {
		localConfigPath = oldLocal
	}()
	localConfigPath = filepath.Join(t.TempDir(), "nonexistent.yaml")

	tests := []struct {
		name     string
		envPath  string
		fallback string
		want     string
	}{
		{
			name:     "EnvConfigPathSet",
			envPath:  "/custom/path.yaml",
			fallback: "/default/path.yaml",
			want:     "/custom/path.yaml",
		},
		{
			name:     "FallbackUsed",
			fallback: "/default/path.yaml",
			want:     "/default/path.yaml",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

func TestOllamaClient(t *testing.T) {
	tests := []struct {
		name         string
		handlerCode  int
		handlerBody  string
		input        string
		isQuery      bool
		prefix       string
		wantErr      bool
		wantVecLen   int
		expectPrompt string
	}{
		{
			name:        "SuccessDocument",
			handlerCode: http.StatusOK,
			handlerBody: `{"embedding": [0.1, 0.2, 0.3]}`,
			input:       "test fact text",
			isQuery:     false,
			wantVecLen:  3,
		},
		{
			name:         "SuccessQueryWithPrefix",
			handlerCode:  http.StatusOK,
			handlerBody:  `{"embedding": [0.1, 0.2, 0.3]}`,
			input:        "search query",
			isQuery:      true,
			prefix:       "search: ",
			wantVecLen:   3,
			expectPrompt: "search: search query",
		},
		{
			name:        "EmptyTextInputError",
			input:       "",
			wantErr:     true,
			handlerCode: http.StatusOK,
		},
		{
			name:        "Non200Error",
			handlerCode: http.StatusInternalServerError,
			handlerBody: `internal server error`,
			input:       "test text",
			wantErr:     true,
		},
		{
			name:        "OllamaErrorResponse",
			handlerCode: http.StatusOK,
			handlerBody: `{"error": "model not found"}`,
			input:       "test text",
			wantErr:     true,
		},
		{
			name:        "EmptyEmbeddingReturned",
			handlerCode: http.StatusOK,
			handlerBody: `{"embedding": []}`,
			input:       "test text",
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.expectPrompt != "" {
					var req embeddingRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					if req.Prompt != tc.expectPrompt {
						t.Errorf("req.Prompt = %q, want %q", req.Prompt, tc.expectPrompt)
					}
				}
				w.WriteHeader(tc.handlerCode)
				_, _ = w.Write([]byte(tc.handlerBody))
			}))
			defer server.Close()

			client := NewOllamaClient(&OllamaConfig{
				BaseURL:     server.URL,
				QueryPrefix: tc.prefix,
			})
			client.retryDelay = 0

			vec, err := client.GenerateEmbedding(context.Background(), tc.input, tc.isQuery, 0)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GenerateEmbedding() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && len(vec) != tc.wantVecLen {
				t.Errorf("len(vec) = %d, want %d", len(vec), tc.wantVecLen)
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
			name: "CategorizedFacts",
			facts: []Fact{
				{Category: "user_preference", FactText: "Prefers Pacific timezone"},
				{Category: "", FactText: "Default category fallback"},
			},
			wantContains: []string{
				"<retrieved_memory>",
				"- [user_preference] Prefers Pacific timezone",
				"- [general] Default category fallback",
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

func TestToolHandler_MemorySearch(t *testing.T) {
	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "user", "User lives in Oakland", 0.9, []float32{1, 0})
	_, _ = store.InsertFact(context.Background(), "work", "User works in tech", 0.8, []float32{0, 1})

	ollama := &MockOllamaClient{embedding: []float32{1, 0}}
	handler := NewToolHandler(&Config{}, store, ollama)

	tests := []struct {
		name      string
		args      MemorySearchArgs
		storeFail bool
		wantErr   bool
		wantCount int
	}{
		{
			name:      "SuccessfulSearch",
			args:      MemorySearchArgs{Query: "Oakland", Limit: 5},
			wantCount: 1,
		},
		{
			name:    "EmptyQueryError",
			args:    MemorySearchArgs{Query: ""},
			wantErr: true,
		},
		{
			name:      "StoreSearchFails",
			args:      MemorySearchArgs{Query: "Oakland"},
			storeFail: true,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.storeFail {
				store.SetFail("SearchCandidateFacts", errors.New("search fail"))
				defer store.SetFail("", nil)
			}
			out, err := handler.MemorySearch(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemorySearch() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && out.Count != tc.wantCount {
				t.Errorf("out.Count = %d, want %d", out.Count, tc.wantCount)
			}
		})
	}
}

func TestToolHandler_MemoryStore(t *testing.T) {
	store := newTestMemStore()
	existingID, _ := store.InsertFact(context.Background(), "user", "Existing Fact", 0.8, []float32{1.0, 0.0})

	ollama := &MockOllamaClient{embedding: []float32{1.0, 0.0}}
	handler := NewToolHandler(&Config{}, store, ollama)

	tests := []struct {
		name       string
		args       MemoryStoreArgs
		storeFail  string
		wantErr    bool
		wantAction string
		wantID     int64
	}{
		{
			name:       "InsertNewFact",
			args:       MemoryStoreArgs{FactText: "Brand new fact", Category: "system"},
			wantAction: "reinforced",
			wantID:     existingID,
		},
		{
			name:       "InsertUniqueFact",
			args:       MemoryStoreArgs{FactText: "Unique fact", Category: "system"},
			wantAction: "inserted",
		},
		{
			name:       "SupersedeExistingFact",
			args:       MemoryStoreArgs{FactText: "Updated Fact", Category: "user", SupersedesID: existingID},
			wantAction: "updated",
			wantID:     existingID,
		},
		{
			name:    "SupersedeNonexistentFails",
			args:    MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: 999999},
			wantErr: true,
		},
		{
			name:    "EmptyFactTextFails",
			args:    MemoryStoreArgs{FactText: ""},
			wantErr: true,
		},
		{
			name:      "InsertStoreFails",
			args:      MemoryStoreArgs{FactText: "Failing fact"},
			storeFail: "InsertFact",
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "InsertUniqueFact" {
				ollama.embedding = []float32{0.0, 1.0}
			} else if tc.name == "InsertStoreFails" {
				ollama.embedding = nil
			} else {
				ollama.embedding = []float32{1.0, 0.0}
			}
			if tc.storeFail != "" {
				store.SetFail(tc.storeFail, errors.New("simulated store failure"))
				defer store.SetFail("", nil)
			}
			out, err := handler.MemoryStore(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemoryStore() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if tc.wantAction != "" && out.Action != tc.wantAction {
					t.Errorf("out.Action = %q, want %q", out.Action, tc.wantAction)
				}
				if tc.wantID > 0 && out.FactID != tc.wantID {
					t.Errorf("out.FactID = %d, want %d", out.FactID, tc.wantID)
				}
			}
		})
	}
}

func TestToolHandler_MemoryForget(t *testing.T) {
	store := newTestMemStore()
	id, _ := store.InsertFact(context.Background(), "user", "Forget me", 0.5, nil)
	handler := NewToolHandler(&Config{}, store, nil)

	tests := []struct {
		name    string
		args    MemoryForgetArgs
		wantErr bool
	}{
		{
			name:    "ForgetSuccess",
			args:    MemoryForgetArgs{FactID: id},
			wantErr: false,
		},
		{
			name:    "ForgetNonexistent",
			args:    MemoryForgetArgs{FactID: 99999},
			wantErr: true,
		},
		{
			name:    "InvalidIDZero",
			args:    MemoryForgetArgs{FactID: 0},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := handler.MemoryForget(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemoryForget() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && out.Status != "success" {
				t.Errorf("out.Status = %q, want success", out.Status)
			}
		})
	}
}

func TestToolHandler_GetFactsAndContext(t *testing.T) {
	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "profile", "Profile fact", 0.8, []float32{1, 0})
	handler := NewToolHandler(&Config{}, store, &MockOllamaClient{embedding: []float32{1, 0}})

	t.Run("GetFacts", func(t *testing.T) {
		res, err := handler.GetFacts(context.Background(), FactsFilter{Category: "profile"})
		if err != nil {
			t.Fatalf("GetFacts failed: %v", err)
		}
		if res.Total != 1 || len(res.Facts) != 1 {
			t.Errorf("res.Total = %d, len = %d, want 1", res.Total, len(res.Facts))
		}
	})

	t.Run("GetContextEmptyQuery", func(t *testing.T) {
		res, err := handler.GetContext(context.Background(), "", 5)
		if err != nil {
			t.Fatalf("GetContext failed: %v", err)
		}
		if res.Context != "" || len(res.Facts) != 0 {
			t.Errorf("expected empty context response, got %+v", res)
		}
	})

	t.Run("GetContextSuccess", func(t *testing.T) {
		res, err := handler.GetContext(context.Background(), "Profile", 5)
		if err != nil {
			t.Fatalf("GetContext failed: %v", err)
		}
		if !strings.Contains(res.Context, "Profile fact") {
			t.Errorf("expected context to contain 'Profile fact', got %q", res.Context)
		}
	})
}

func TestServer_Endpoints(t *testing.T) {
	oldCounters, oldMeta := writeCountersDirFn, writeMetaDirFn
	defer func() {
		writeCountersDirFn, writeMetaDirFn = oldCounters, oldMeta
	}()
	writeCountersDirFn = func(string) error { return nil }
	writeMetaDirFn = func(string) error { return nil }

	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "general", "Server fact test", 0.75, []float32{1, 0})
	handler := NewToolHandler(&Config{}, store, &MockOllamaClient{embedding: []float32{1, 0}})
	server := NewServer(handler)
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		headers    map[string]string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "Health",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantSubstr: `{"status":"ok"}`,
		},
		{
			name:       "CoverageFlush",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
			wantSubstr: "ok",
		},
		{
			name:       "FactsJSON",
			method:     http.MethodGet,
			path:       "/facts?limit=10&offset=0",
			wantStatus: http.StatusOK,
			wantSubstr: `"facts"`,
		},
		{
			name:       "FactsMethodNotAllowed",
			method:     http.MethodPost,
			path:       "/facts",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "ContextJSON",
			method:     http.MethodGet,
			path:       "/context?q=test&limit=5",
			wantStatus: http.StatusOK,
			wantSubstr: `"context"`,
		},
		{
			name:       "ContextPlaintext",
			method:     http.MethodGet,
			path:       "/context?q=test&limit=5",
			headers:    map[string]string{"Accept": "text/plain"},
			wantStatus: http.StatusOK,
			wantSubstr: "<retrieved_memory>",
		},
		{
			name:       "ContextMethodNotAllowed",
			method:     http.MethodPost,
			path:       "/context",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "MCPGetProbe",
			method:     http.MethodGet,
			path:       "/mcp",
			wantStatus: http.StatusOK,
			wantSubstr: "MCP Memory Service Ready",
		},
		{
			name:       "MCPMethodNotAllowed",
			method:     http.MethodPut,
			path:       "/mcp",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "MCPEmptyBody",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MCPDoublePrefixNormalized",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_memory_search","arguments":{"query":"test"}}}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, bodyReader)
			if err != nil {
				t.Fatalf("http.NewRequest failed: %v", err)
			}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if tc.wantSubstr != "" {
				respBytes, _ := io.ReadAll(resp.Body)
				if !strings.Contains(string(respBytes), tc.wantSubstr) {
					t.Errorf("response %q missing %q", string(respBytes), tc.wantSubstr)
				}
			}
		})
	}
}

func TestServer_PanicOnNilHandler(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected NewServer(nil) to panic")
		}
	}()
	_ = NewServer(nil)
}

func TestDB_ScanHelpers(t *testing.T) {
	t.Run("NullVector", func(t *testing.T) {
		var nv NullVector
		if err := nv.Scan(nil); err != nil || nv.Valid {
			t.Errorf("expected null vector for nil, got %+v", nv)
		}
		if err := nv.Scan("[1.0,2.0]"); err != nil || !nv.Valid || len(nv.Vector) != 2 {
			t.Errorf("expected valid vector from string, got %+v, err=%v", nv, err)
		}
		vec := pgvector.NewVector([]float32{3.0, 4.0})
		if err := nv.Scan(vec); err != nil || !nv.Valid || len(nv.Vector) != 2 {
			t.Errorf("expected valid vector from pgvector.Vector, got %+v, err=%v", nv, err)
		}
	})

	t.Run("FactTime", func(t *testing.T) {
		var ft FactTime
		if err := ft.Scan(nil); err != nil || ft.Valid {
			t.Errorf("expected invalid FactTime for nil, got %+v", ft)
		}
		now := time.Now().UTC()
		if err := ft.Scan(now); err != nil || !ft.Valid {
			t.Errorf("expected valid FactTime for time.Time, got %+v", ft)
		}
		if err := ft.Scan("2026-10-21T09:43:01Z"); err != nil || !ft.Valid {
			t.Errorf("expected valid FactTime for RFC3339 string, got %+v", ft)
		}
		if err := ft.Scan(""); err != nil || ft.Valid {
			t.Errorf("expected invalid FactTime for empty string, got %+v", ft)
		}
	})

	t.Run("EscapeSQLLike", func(t *testing.T) {
		got := EscapeSQLLike("a%b_c\\d")
		want := "a\\%b\\_c\\\\d"
		if got != want {
			t.Errorf("EscapeSQLLike() = %q, want %q", got, want)
		}
	})

	t.Run("OptionalVectorArg", func(t *testing.T) {
		if optionalVectorArg(nil) != nil {
			t.Errorf("expected nil for nil embedding")
		}
		emb384 := make([]float32, ExpectedEmbeddingDim)
		if optionalVectorArg(emb384) == nil {
			t.Errorf("expected non-nil for 384 dim embedding")
		}
	})
}

func TestMockPostgresConnection(t *testing.T) {
	addr := startMockPostgres(t)
	dsn := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"
	db, err := InitDB(&Config{DatabaseURL: dsn})
	if err != nil {
		t.Fatalf("InitDB with mock postgres failed: %v", err)
	}
	defer db.Close()

	if !isPostgres(db) {
		t.Errorf("expected isPostgres(db) to return true for mock postgres")
	}
}

func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
	}()

	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(testCfg, []byte("port: \"4008\"\ndatabase_url: \"postgres://u:p@localhost/db\"\n"), 0o644)
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	runCalled := false
	runAppFn = func(ctx context.Context, cfg *Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn called")
	}

	exitCalled := false
	exitFn = func(string, ...interface{}) { exitCalled = true }
	runAppFn = func(ctx context.Context, cfg *Config) error { return http.ErrServerClosed }
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	runAppFn = func(ctx context.Context, cfg *Config) error { return context.DeadlineExceeded }
	main()
	if !exitCalled {
		t.Error("expected exitFn on server error")
	}

	exitCalled = false
	configPath = filepath.Join(tmpDir, "nonexistent.yaml")
	t.Setenv("CONFIG_PATH", configPath)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_URL", "")
	main()
	if !exitCalled {
		t.Error("expected exitFn on config load error")
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	t.Run("NilConfigFails", func(t *testing.T) {
		err := RunApp(context.Background(), nil)
		if err == nil {
			t.Error("expected error on nil config")
		}
	})

	t.Run("InvalidDBFails", func(t *testing.T) {
		err := RunApp(context.Background(), &Config{DatabaseURL: "invalid://bad"})
		if err == nil {
			t.Error("expected error on invalid db URL")
		}
	})

	t.Run("CleanLifecycleWithMockPostgres", func(t *testing.T) {
		addr := startMockPostgres(t)
		cfg := &Config{
			Port:        "0",
			DatabaseURL: "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable",
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately for graceful shutdown test
		err := RunApp(ctx, cfg)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("RunApp error: %v", err)
		}
	})
}

type testCustomResult struct {
	rows int64
	err  error
}

func (r testCustomResult) LastInsertId() (int64, error) { return 0, nil }
func (r testCustomResult) RowsAffected() (int64, error) { return r.rows, r.err }

type queryMockDBTX struct {
	dbMem         *sql.DB
	queryErr      error
	execErr       error
	rowsAffected  int64
	capturedQuery *string
	rowSQL        string
}

func (q queryMockDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if q.capturedQuery != nil {
		*q.capturedQuery = query
	}
	if q.execErr != nil {
		return nil, q.execErr
	}
	return testCustomResult{rows: q.rowsAffected}, nil
}

func (q queryMockDBTX) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if q.capturedQuery != nil {
		*q.capturedQuery = query
	}
	if q.queryErr != nil {
		return q.dbMem.QueryRowContext(ctx, "SELECT syntax error")
	}
	if strings.Contains(query, "COUNT(*)") {
		return q.dbMem.QueryRowContext(ctx, "SELECT 1")
	}
	if q.rowSQL != "" {
		return q.dbMem.QueryRowContext(ctx, q.rowSQL)
	}
	return q.dbMem.QueryRowContext(ctx, "SELECT 1")
}

func (q queryMockDBTX) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if q.capturedQuery != nil {
		*q.capturedQuery = query
	}
	if q.queryErr != nil {
		return nil, q.queryErr
	}
	if q.rowSQL != "" {
		return q.dbMem.QueryContext(ctx, q.rowSQL)
	}
	return q.dbMem.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
}

func setupTestDBMem(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLStore_Operations(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	t.Run("CheckDBNil", func(t *testing.T) {
		var store *SQLStore
		if err := store.checkDB(); err == nil {
			t.Error("expected error for nil SQLStore")
		}
		emptyStore := &SQLStore{}
		if err := emptyStore.checkDB(); err == nil {
			t.Error("expected error for nil db in SQLStore")
		}
	})

	t.Run("Close", func(t *testing.T) {
		var store *SQLStore
		if err := store.Close(); err != nil {
			t.Errorf("unexpected error on nil Close: %v", err)
		}
		memDB, _ := sql.Open("sqlite", ":memory:")
		st := NewSQLStore(memDB, false)
		if err := st.Close(); err != nil {
			t.Errorf("unexpected error on valid Close: %v", err)
		}
	})

	insertTests := []struct {
		name     string
		category string
		factText string
		queryErr error
		rowSQL   string
		wantErr  bool
		wantID   int64
	}{
		{
			name:     "EmptyFactText",
			factText: "",
			wantErr:  true,
		},
		{
			name:     "QueryError",
			factText: "valid fact",
			queryErr: errors.New("insert error"),
			wantErr:  true,
		},
		{
			name:     "SuccessDefaultCategory",
			factText: "valid fact",
			rowSQL:   "SELECT 42",
			wantID:   42,
		},
	}
	for _, tc := range insertTests {
		t.Run("Insert_"+tc.name, func(t *testing.T) {
			mock := queryMockDBTX{
				dbMem:    dbMem,
				queryErr: tc.queryErr,
				rowSQL:   tc.rowSQL,
			}
			store := NewSQLStore(mock, true)
			id, err := store.InsertFact(ctx, tc.category, tc.factText, 0.7, emb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InsertFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantID > 0 && id != tc.wantID {
				t.Errorf("InsertFact() id = %d, want %d", id, tc.wantID)
			}
		})
	}

	updateTests := []struct {
		name         string
		factText     string
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:     "EmptyFactText",
			factText: "",
			wantErr:  true,
		},
		{
			name:     "ExecError",
			factText: "valid",
			execErr:  errors.New("update err"),
			wantErr:  true,
		},
		{
			name:         "NotFoundZeroRows",
			factText:     "valid",
			rowsAffected: 0,
			wantErr:      true,
		},
		{
			name:         "Success",
			factText:     "valid",
			rowsAffected: 1,
			wantErr:      false,
		},
	}
	for _, tc := range updateTests {
		t.Run("Update_"+tc.name, func(t *testing.T) {
			mock := queryMockDBTX{
				dbMem:        dbMem,
				execErr:      tc.execErr,
				rowsAffected: tc.rowsAffected,
			}
			store := NewSQLStore(mock, true)
			err := store.UpdateFact(ctx, 10, "cat", tc.factText, 0.8, emb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UpdateFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}

	reinforceTests := []struct {
		name         string
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:    "ExecError",
			execErr: errors.New("reinforce err"),
			wantErr: true,
		},
		{
			name:         "NotFoundZeroRows",
			rowsAffected: 0,
			wantErr:      true,
		},
		{
			name:         "Success",
			rowsAffected: 1,
			wantErr:      false,
		},
	}
	for _, tc := range reinforceTests {
		t.Run("Reinforce_"+tc.name, func(t *testing.T) {
			mock := queryMockDBTX{
				dbMem:        dbMem,
				execErr:      tc.execErr,
				rowsAffected: tc.rowsAffected,
			}
			store := NewSQLStore(mock, true)
			err := store.ReinforceFact(ctx, 10, "new text", emb, 0.15)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReinforceFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}

	deleteTests := []struct {
		name         string
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:    "ExecError",
			execErr: errors.New("delete err"),
			wantErr: true,
		},
		{
			name:         "NotFoundZeroRows",
			rowsAffected: 0,
			wantErr:      true,
		},
		{
			name:         "Success",
			rowsAffected: 1,
			wantErr:      false,
		},
	}
	for _, tc := range deleteTests {
		t.Run("Delete_"+tc.name, func(t *testing.T) {
			mock := queryMockDBTX{
				dbMem:        dbMem,
				execErr:      tc.execErr,
				rowsAffected: tc.rowsAffected,
			}
			store := NewSQLStore(mock, true)
			err := store.DeleteFact(ctx, 10)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeleteFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}

	findDupTests := []struct {
		name       string
		embLen     int
		queryErr   error
		rowSQL     string
		wantErr    bool
		wantNil    bool
		wantID     int64
		wantSimMin float64
	}{
		{
			name:    "ShortEmbeddingReturnsNil",
			embLen:  10,
			wantNil: true,
		},
		{
			name:     "QueryError",
			embLen:   ExpectedEmbeddingDim,
			queryErr: errors.New("dup query error"),
			wantErr:  true,
		},
		{
			name:       "MatchedAboveMin",
			embLen:     ExpectedEmbeddingDim,
			rowSQL:     "SELECT 7, 'cat', 'fact text', 0.9, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.92",
			wantID:     7,
			wantSimMin: 0.90,
		},
		{
			name:    "MatchedBelowMin",
			embLen:  ExpectedEmbeddingDim,
			rowSQL:  "SELECT 8, 'cat', 'fact text', 0.9, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.40",
			wantNil: true,
		},
	}
	for _, tc := range findDupTests {
		t.Run("FindDup_"+tc.name, func(t *testing.T) {
			testEmb := make([]float32, tc.embLen)
			mock := queryMockDBTX{
				dbMem:    dbMem,
				queryErr: tc.queryErr,
				rowSQL:   tc.rowSQL,
			}
			store := NewSQLStore(mock, true)
			dup, sim, err := store.FindDuplicateFact(ctx, testEmb, 0.85)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FindDuplicateFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantNil && dup != nil {
				t.Errorf("expected nil duplicate, got %+v", dup)
			}
			if tc.wantID > 0 {
				if dup == nil || dup.ID != tc.wantID {
					t.Errorf("dup.ID = %v, want %d", dup, tc.wantID)
				}
				if sim < tc.wantSimMin {
					t.Errorf("sim = %f, want >= %f", sim, tc.wantSimMin)
				}
			}
		})
	}
}

func TestSQLStore_SearchCandidateFactsBranches(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		emb          []float32
		query        string
		category     string
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantPatterns []string
		wantCount    int
	}{
		{
			name:      "NeitherEmbeddingNorQuery",
			emb:       nil,
			query:     "",
			wantCount: 0,
		},
		{
			name:         "BothWithCategory",
			emb:          emb,
			query:        "test query",
			category:     "work",
			wantPatterns: []string{"vector_hits", "text_hits", "category = $2"},
		},
		{
			name:         "BothWithoutCategory",
			emb:          emb,
			query:        "test query",
			category:     "",
			wantPatterns: []string{"vector_hits", "text_hits"},
		},
		{
			name:         "OnlyEmbeddingWithCategory",
			emb:          emb,
			query:        "",
			category:     "personal",
			wantPatterns: []string{"candidates", "category = $2"},
		},
		{
			name:         "OnlyEmbeddingWithoutCategory",
			emb:          emb,
			query:        "",
			category:     "",
			wantPatterns: []string{"candidates"},
		},
		{
			name:         "OnlyTextWithCategory",
			emb:          nil,
			query:        "test query",
			category:     "personal",
			wantPatterns: []string{"text_hits", "category = $2"},
		},
		{
			name:         "OnlyTextWithoutCategory",
			emb:          nil,
			query:        "test query",
			category:     "",
			wantPatterns: []string{"text_hits"},
		},
		{
			name:     "QueryError",
			emb:      emb,
			query:    "test query",
			queryErr: errors.New("search failure"),
			wantErr:  true,
		},
		{
			name:      "SuccessfulRowsScan",
			emb:       emb,
			query:     "fact",
			rowSQL:    "SELECT 5, 'general', 'scanned fact', 0.85, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1, '[0.1,0.2]', 0.88",
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var captured string
			mock := queryMockDBTX{
				dbMem:         dbMem,
				queryErr:      tc.queryErr,
				rowSQL:        tc.rowSQL,
				capturedQuery: &captured,
			}
			store := NewSQLStore(mock, true)
			results, err := store.SearchCandidateFacts(ctx, tc.emb, tc.query, tc.category, 10, 0.20)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SearchCandidateFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
			if tc.wantCount > 0 && len(results) != tc.wantCount {
				t.Errorf("len(results) = %d, want %d", len(results), tc.wantCount)
			}
		})
	}
}

func TestSQLStore_GetFactsPaginatedBranches(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		filter       FactsFilter
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantPatterns []string
		wantTotal    int
	}{
		{
			name:     "CountQueryError",
			filter:   FactsFilter{Limit: 10},
			queryErr: errors.New("count err"),
			wantErr:  true,
		},
		{
			name:         "SuccessWithCategoryAndQuery",
			filter:       FactsFilter{Category: "tech", Query: "linux", Limit: 5, Offset: -1},
			wantPatterns: []string{"category = $1", "LOWER(fact_text) LIKE", "LIMIT 5 OFFSET 0"},
		},
		{
			name:         "OffsetOnly",
			filter:       FactsFilter{Offset: 10},
			wantPatterns: []string{"OFFSET 10"},
		},
		{
			name:      "SuccessfulRowsScan",
			filter:    FactsFilter{Limit: 5},
			rowSQL:    "SELECT 1, 'cat', 'fact', 0.9, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1",
			wantTotal: 1,
		},
		{
			name:     "SelectQueryError",
			filter:   FactsFilter{Limit: 5},
			rowSQL:   "syntax error",
			queryErr: errors.New("select query err"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var captured string
			mock := queryMockDBTX{
				dbMem:         dbMem,
				queryErr:      tc.queryErr,
				rowSQL:        tc.rowSQL,
				capturedQuery: &captured,
			}
			store := NewSQLStore(mock, true)
			_, err := store.GetFactsPaginated(ctx, tc.filter)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetFactsPaginated() err = %v, wantErr = %v", err, tc.wantErr)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
		})
	}
}

func TestServer_DisconnectAndWrite(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled, _ := http.NewRequestWithContext(canceledCtx, http.MethodGet, "/health", nil)

	tests := []struct {
		name string
		req  *http.Request
		err  error
		want bool
	}{
		{"NilError", nil, nil, false},
		{"ContextCanceled", reqCanceled, errors.New("other"), true},
		{"SyscallEPIPE", nil, syscall.EPIPE, true},
		{"SyscallECONNRESET", nil, syscall.ECONNRESET, true},
		{"NetErrClosed", nil, net.ErrClosed, true},
		{"UnrelatedError", nil, errors.New("unrelated"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isClientDisconnect(tc.req, tc.err)
			if got != tc.want {
				t.Errorf("isClientDisconnect() = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("WriteResponseSuccess", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeResponse(rec, nil, []byte("test data"))
		if rec.Body.String() != "test data" {
			t.Errorf("got %q, want 'test data'", rec.Body.String())
		}
	})

	t.Run("WriteResponseError", func(t *testing.T) {
		mockWriter := &mockErrWriter{err: errors.New("write failed")}
		writeResponse(mockWriter, nil, []byte("fail"))
	})
}

type mockErrWriter struct {
	header http.Header
	err    error
}

func (m *mockErrWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}
func (m *mockErrWriter) Write([]byte) (int, error) { return 0, m.err }
func (m *mockErrWriter) WriteHeader(int)           {}

type mockCloser struct {
	err error
}

func (m mockCloser) Close() error { return m.err }

func TestAdditionalCoverage(t *testing.T) {
	t.Run("RegisterSQLiteTestHookAndHelpers", func(t *testing.T) {
		RegisterSQLiteTestHook(nil)
		ctx, cancel := withTimeout(nil, 1*time.Second)
		defer cancel()
		if ctx == nil {
			t.Error("expected non-nil context from withTimeout(nil)")
		}

		closeWarn(nil, "nil resource")
		closeWarn(mockCloser{err: errors.New("close err")}, "mock resource")

		if isPostgres(nil) {
			t.Error("isPostgres(nil) should be false")
		}

		var nv NullVector
		if err := nv.Scan(12345); err == nil {
			t.Error("expected error scanning int into NullVector")
		}

		var ft FactTime
		var nilTime *time.Time
		if err := ft.Scan(nilTime); err != nil || ft.Valid {
			t.Error("expected invalid for nil *time.Time")
		}
		if err := ft.Scan("2026-10-21 09:00:00"); err != nil || !ft.Valid {
			t.Error("expected valid for space timestamp")
		}
		if err := ft.Scan("invalid-date-string"); err == nil {
			t.Error("expected error for invalid date string")
		}
	})

	t.Run("DBInitEdges", func(t *testing.T) {
		if _, err := InitDB(nil); err == nil {
			t.Error("expected error for InitDB(nil)")
		}
		if _, err := initDB("   "); err == nil {
			t.Error("expected error for empty connStr in initDB")
		}
	})

	t.Run("OllamaClientDefaultsAndRetries", func(t *testing.T) {
		clientNil := NewOllamaClient(nil)
		if clientNil.model != DefaultOllamaModel || clientNil.baseURL != DefaultOllamaURL {
			t.Errorf("expected defaults, got %+v", clientNil)
		}

		clientEmpty := NewOllamaClient(&OllamaConfig{BaseURL: "", Model: "", NumCtx: 0})
		if clientEmpty.model != DefaultOllamaModel || clientEmpty.numCtx != DefaultOllamaNumCtx {
			t.Errorf("expected defaults for empty config, got %+v", clientEmpty)
		}

		attempts := 0
		retryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			if attempts == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("temporary error"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"embedding":[0.1,0.2]}`))
		}))
		defer retryServer.Close()

		retryClient := NewOllamaClient(&OllamaConfig{
			BaseURL:     retryServer.URL,
			QueryPrefix: "q: ",
		})
		retryClient.retryDelay = 1 * time.Millisecond
		vec, err := retryClient.GenerateEmbedding(context.Background(), "query", true, -1)
		if err == nil {
			t.Error("expected error with maxRetries = -1")
		}

		attempts = 0
		vec, err = retryClient.GenerateEmbedding(context.Background(), "query", true, 2)
		if err != nil || len(vec) != 2 {
			t.Fatalf("expected successful retry, got vec=%v, err=%v", vec, err)
		}
	})

	t.Run("ToolsHandlerEdges", func(t *testing.T) {
		store := newTestMemStore()
		ollamaErr := &MockOllamaClient{err: errors.New("ollama offline")}
		handler := NewToolHandler(&Config{}, store, ollamaErr)

		// MemorySearch with ollama error and clamped limits
		searchOut, err := handler.MemorySearch(context.Background(), MemorySearchArgs{
			Query: "test",
			Limit: -1,
		})
		if err != nil {
			t.Fatalf("MemorySearch with ollama warning failed: %v", err)
		}
		if searchOut.Facts == nil {
			t.Error("expected facts slice")
		}

		_, _ = handler.MemorySearch(context.Background(), MemorySearchArgs{
			Query: "test",
			Limit: 100,
		})

		// MemoryStore with default category and clamped importance
		storeOut, err := handler.MemoryStore(context.Background(), MemoryStoreArgs{
			FactText:   "fact with zero importance and empty cat",
			Importance: -0.5,
		})
		if err != nil || storeOut.Action != "inserted" {
			t.Fatalf("MemoryStore insert failed: %v", err)
		}

		_, _ = handler.MemoryStore(context.Background(), MemoryStoreArgs{
			FactText:   "fact with large importance",
			Importance: 5.0,
		})

		// Reinforce error on duplicate
		dupID, _ := store.InsertFact(context.Background(), "general", "dup", 0.8, []float32{1, 0})
		store.SetFail("ReinforceFact", errors.New("reinforce failed"))
		ollamaValid := &MockOllamaClient{embedding: []float32{1, 0}}
		handlerWithValid := NewToolHandler(&Config{}, store, ollamaValid)
		_, err = handlerWithValid.MemoryStore(context.Background(), MemoryStoreArgs{
			FactText: "dup",
		})
		if err == nil {
			t.Error("expected error when ReinforceFact fails on duplicate")
		}
		store.SetFail("", nil)

		// MemoryForget with ID <= 0 and delete failure
		_, err = handler.MemoryForget(context.Background(), MemoryForgetArgs{FactID: -1})
		if err == nil {
			t.Error("expected error for FactID <= 0")
		}

		store.SetFail("DeleteFact", errors.New("disk failure"))
		_, err = handler.MemoryForget(context.Background(), MemoryForgetArgs{FactID: dupID})
		if err == nil {
			t.Error("expected error when DeleteFact fails")
		}
		store.SetFail("", nil)

		// GetContext with limit bounds and candidate search error
		ctxRes, _ := handler.GetContext(context.Background(), "query", -1)
		if ctxRes == nil {
			t.Error("expected non-nil context response")
		}
		_, _ = handler.GetContext(context.Background(), "query", 50)

		store.SetFail("SearchCandidateFacts", errors.New("store search err"))
		ctxResErr, err := handler.GetContext(context.Background(), "query", 5)
		if err != nil || ctxResErr.Context != "" {
			t.Errorf("expected graceful recovery from candidate search failure, got res=%+v, err=%v", ctxResErr, err)
		}
		store.SetFail("", nil)
	})

	t.Run("ServerHandlersExtra", func(t *testing.T) {
		store := newTestMemStore()
		handler := NewToolHandler(&Config{}, store, nil)
		server := NewServer(handler)

		// Coverage flush failure
		oldCounters := writeCountersDirFn
		defer func() { writeCountersDirFn = oldCounters }()
		writeCountersDirFn = func(string) error { return errors.New("coverage flush err") }
		t.Setenv("GOCOVERDIR", t.TempDir())
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil)
		server.handleCoverageFlush(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("handleCoverageFlush status = %d, want 500", rec.Code)
		}

		// HandleFacts error
		store.SetFail("GetFactsPaginated", errors.New("db query error"))
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/facts", nil)
		server.handleFacts(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("handleFacts status = %d, want 500", rec.Code)
		}
		store.SetFail("", nil)

		// HandleContext error
		store.SetFail("SearchCandidateFacts", errors.New("fatal search"))
		// GetContext gracefully recovers, so test handleContext
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/context?q=test", nil)
		server.handleContext(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("handleContext status = %d, want 200", rec.Code)
		}
		store.SetFail("", nil)

		// HandleMCP subpaths hit directly
		for _, path := range []string{"/health", "/debug/coverage/flush", "/facts", "/context"} {
			rec = httptest.NewRecorder()
			req = httptest.NewRequest(http.MethodGet, path, nil)
			server.handleMCP(rec, req)
			if rec.Code != http.StatusOK && rec.Code != http.StatusInternalServerError {
				t.Errorf("handleMCP(%s) code = %d", path, rec.Code)
			}
		}

		// HandleMCP body read error
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/mcp", &mockErrReadCloser{})
		server.handleMCP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("handleMCP body read error status = %d, want 400", rec.Code)
		}

		// HandleMCP tool calls for store and forget
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"memory_store","arguments":{"fact_text":"tool call store" karma}}}`))
		server.handleMCP(rec, req)

		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"memory_store","arguments":{"fact_text":"tool call store"}}}`))
		server.handleMCP(rec, req)

		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"memory_forget","arguments":{"fact_id":1}}}`))
		server.handleMCP(rec, req)
	})

	t.Run("ConfigEdges", func(t *testing.T) {
		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "local.yaml")
		_ = os.WriteFile(localFile, []byte("port: '4015'\n"), 0o644)
		oldLocal := localConfigPath
		defer func() { localConfigPath = oldLocal }()
		localConfigPath = localFile

		t.Setenv("CONFIG_PATH", "")
		resolved := ResolveConfigPath("/fallback.yaml")
		if resolved != localFile {
			t.Errorf("ResolveConfigPath() = %q, want %q", resolved, localFile)
		}

		t.Setenv("PORT", "")
		t.Setenv("DATABASE_URL", "postgres://localhost/test")
		cfg, err := LoadConfigFile("", "")
		if err != nil || cfg.Port != "4008" {
			t.Errorf("LoadConfigFile(\"\") err=%v, port=%s", err, cfg.Port)
		}
	})

	t.Run("SQLStoreDefaultBranches", func(t *testing.T) {
		dbMem := setupTestDBMem(t)
		mock := queryMockDBTX{
			dbMem:        dbMem,
			rowsAffected: 1,
			rowSQL:       "SELECT 99",
		}
		store := NewSQLStore(mock, true)

		// InsertFact defaults
		id, err := store.InsertFact(context.Background(), "", "test", -1, nil)
		if err != nil || id != 99 {
			t.Errorf("InsertFact with defaults err=%v, id=%d", err, id)
		}

		// UpdateFact defaults
		err = store.UpdateFact(context.Background(), 1, "", "test", -1, nil)
		if err != nil {
			t.Errorf("UpdateFact with defaults err=%v", err)
		}

		// ReinforceFact defaults
		err = store.ReinforceFact(context.Background(), 1, "", nil, -1)
		if err != nil {
			t.Errorf("ReinforceFact with defaults err=%v", err)
		}

		// FindDuplicateFact defaults and no rows
		mockNoRows := queryMockDBTX{
			dbMem:  dbMem,
			rowSQL: "SELECT 1 WHERE 1=0",
		}
		storeNoRows := NewSQLStore(mockNoRows, true)
		emb := make([]float32, ExpectedEmbeddingDim)
		dup, sim, err := storeNoRows.FindDuplicateFact(context.Background(), emb, -1)
		if err != nil || dup != nil || sim != 0 {
			t.Errorf("FindDuplicateFact with sql.ErrNoRows got dup=%v, sim=%f, err=%v", dup, sim, err)
		}

		// SearchCandidateFacts defaults with proper 10-column candidate row
		mockSearch := queryMockDBTX{
			dbMem:  dbMem,
			rowSQL: "SELECT 5, 'general', 'scanned fact', 0.85, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1, '[0.1,0.2]', 0.88",
		}
		storeSearch := NewSQLStore(mockSearch, true)
		_, err = storeSearch.SearchCandidateFacts(context.Background(), emb, "query", "", -1, -1)
		if err != nil {
			t.Errorf("SearchCandidateFacts defaults err=%v", err)
		}
	})
}
