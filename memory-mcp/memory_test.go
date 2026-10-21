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
			name:      "EmptyPathUsesDefaultPort",
			path:      "",
			envDB:     "postgres://localhost/test",
			wantPort:  "4008",
			wantDB:    "postgres://localhost/test",
			wantModel: "all-minilm",
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
		{
			name:    "DirectoryPathReadError",
			path:    tmpDir,
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
			name:      "FallbackUsed",
			localPath: missingLocal,
			fallback:  "/default/path.yaml",
			want:      "/default/path.yaml",
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
			name:      "NilConfigUsesDefaults",
			cfg:       nil,
			wantURL:   DefaultOllamaURL,
			wantModel: DefaultOllamaModel,
			wantCtx:   DefaultOllamaNumCtx,
		},
		{
			name:      "EmptyFieldsUseDefaults",
			cfg:       &OllamaConfig{BaseURL: "  ", Model: "", NumCtx: 0},
			wantURL:   DefaultOllamaURL,
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
	tests := []struct {
		name          string
		handlerCodes  []int
		handlerBodies []string
		input         string
		isQuery       bool
		prefix        string
		maxRetries    int
		retryDelay    time.Duration
		cancelCtx     bool
		wantErr       bool
		wantVecLen    int
		expectPrompt  string
	}{
		{
			name:          "SuccessDocument",
			handlerCodes:  []int{http.StatusOK},
			handlerBodies: []string{`{"embedding": [0.1, 0.2, 0.3]}`},
			input:         "test fact text",
			isQuery:       false,
			wantVecLen:    3,
		},
		{
			name:          "SuccessQueryWithPrefix",
			handlerCodes:  []int{http.StatusOK},
			handlerBodies: []string{`{"embedding": [0.1, 0.2, 0.3]}`},
			input:         "search query",
			isQuery:       true,
			prefix:        "search: ",
			wantVecLen:    3,
			expectPrompt:  "search: search query",
		},
		{
			name:          "RetrySucceedsOnSecondAttempt",
			handlerCodes:  []int{http.StatusInternalServerError, http.StatusOK},
			handlerBodies: []string{`temporary error`, `{"embedding": [0.1, 0.2]}`},
			input:         "retry query",
			isQuery:       true,
			prefix:        "q: ",
			maxRetries:    2,
			retryDelay:    1 * time.Millisecond,
			wantVecLen:    2,
		},
		{
			name:          "NegativeRetriesClampedAndFails",
			handlerCodes:  []int{http.StatusInternalServerError},
			handlerBodies: []string{`temporary error`},
			input:         "query",
			maxRetries:    -1,
			wantErr:       true,
		},
		{
			name:          "ContextCanceledError",
			handlerCodes:  []int{http.StatusInternalServerError},
			handlerBodies: []string{`err`},
			input:         "query",
			maxRetries:    1,
			cancelCtx:     true,
			wantErr:       true,
		},
		{
			name:          "EmptyTextInputError",
			input:         "",
			wantErr:       true,
			handlerCodes:  []int{http.StatusOK},
			handlerBodies: []string{`{}`},
		},
		{
			name:          "Non200Error",
			handlerCodes:  []int{http.StatusInternalServerError},
			handlerBodies: []string{`internal server error`},
			input:         "test text",
			wantErr:       true,
		},
		{
			name:          "InvalidJSONResponse",
			handlerCodes:  []int{http.StatusOK},
			handlerBodies: []string{`{bad json`},
			input:         "test text",
			wantErr:       true,
		},
		{
			name:          "OllamaErrorResponse",
			handlerCodes:  []int{http.StatusOK},
			handlerBodies: []string{`{"error": "model not found"}`},
			input:         "test text",
			wantErr:       true,
		},
		{
			name:          "EmptyEmbeddingReturned",
			handlerCodes:  []int{http.StatusOK},
			handlerBodies: []string{`{"embedding": []}`},
			input:         "test text",
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attempt := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.expectPrompt != "" {
					var req embeddingRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					if req.Prompt != tc.expectPrompt {
						t.Errorf("req.Prompt = %q, want %q", req.Prompt, tc.expectPrompt)
					}
				}
				idx := attempt
				if idx >= len(tc.handlerCodes) {
					idx = len(tc.handlerCodes) - 1
				}
				attempt++
				w.WriteHeader(tc.handlerCodes[idx])
				_, _ = w.Write([]byte(tc.handlerBodies[idx]))
			}))
			defer server.Close()

			client := NewOllamaClient(&OllamaConfig{
				BaseURL:     server.URL,
				QueryPrefix: tc.prefix,
			})
			client.retryDelay = tc.retryDelay

			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			vec, err := client.GenerateEmbedding(ctx, tc.input, tc.isQuery, tc.maxRetries)
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

	tests := []struct {
		name      string
		args      MemorySearchArgs
		ollamaErr error
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
			name:      "NegativeLimitAndOllamaWarningFallback",
			args:      MemorySearchArgs{Query: "Oakland", Limit: -1},
			ollamaErr: errors.New("ollama offline"),
			wantCount: 1,
		},
		{
			name:      "HighLimitClamped",
			args:      MemorySearchArgs{Query: "Oakland", Limit: 100},
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
			ollama := &MockOllamaClient{embedding: []float32{1, 0}, err: tc.ollamaErr}
			handler := NewToolHandler(&Config{}, store, ollama)
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

	tests := []struct {
		name       string
		args       MemoryStoreArgs
		emb        []float32
		ollamaErr  error
		storeFail  string
		wantErr    bool
		wantAction string
		wantID     int64
	}{
		{
			name:       "ReinforceDuplicateFact",
			args:       MemoryStoreArgs{FactText: "Brand new fact", Category: "system"},
			emb:        []float32{1.0, 0.0},
			wantAction: "reinforced",
			wantID:     existingID,
		},
		{
			name:      "ReinforceDuplicateError",
			args:      MemoryStoreArgs{FactText: "Brand new fact"},
			emb:       []float32{1.0, 0.0},
			storeFail: "ReinforceFact",
			wantErr:   true,
		},
		{
			name:       "InsertUniqueFactWithClampedHighImportance",
			args:       MemoryStoreArgs{FactText: "Unique fact", Importance: 5.0},
			emb:        []float32{0.0, 1.0},
			wantAction: "inserted",
		},
		{
			name:       "InsertWithOllamaWarningAndNegativeImportance",
			args:       MemoryStoreArgs{FactText: "Fact without embedding", Importance: -0.5},
			ollamaErr:  errors.New("embed error"),
			wantAction: "inserted",
		},
		{
			name:       "SupersedeExistingFact",
			args:       MemoryStoreArgs{FactText: "Updated Fact", Category: "user", SupersedesID: existingID},
			emb:        []float32{1.0, 0.0},
			wantAction: "updated",
			wantID:     existingID,
		},
		{
			name:    "SupersedeNonexistentFails",
			args:    MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: 999999},
			emb:     []float32{1.0, 0.0},
			wantErr: true,
		},
		{
			name:      "SupersedeStoreError",
			args:      MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: existingID},
			emb:       []float32{1.0, 0.0},
			storeFail: "UpdateFact",
			wantErr:   true,
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
			ollama := &MockOllamaClient{embedding: tc.emb, err: tc.ollamaErr}
			handler := NewToolHandler(&Config{}, store, ollama)
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
		name      string
		args      MemoryForgetArgs
		storeFail bool
		wantErr   bool
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
		{
			name:      "StoreDeleteError",
			args:      MemoryForgetArgs{FactID: 10},
			storeFail: true,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.storeFail {
				store.SetFail("DeleteFact", errors.New("disk failure"))
				defer store.SetFail("", nil)
			}
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

func TestToolHandler_GetFacts(t *testing.T) {
	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "profile", "Profile fact", 0.8, []float32{1, 0})
	handler := NewToolHandler(&Config{}, store, nil)

	tests := []struct {
		name      string
		filter    FactsFilter
		wantTotal int
	}{
		{
			name:      "MatchingCategory",
			filter:    FactsFilter{Category: "profile"},
			wantTotal: 1,
		},
		{
			name:      "NonMatchingCategory",
			filter:    FactsFilter{Category: "other"},
			wantTotal: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := handler.GetFacts(context.Background(), tc.filter)
			if err != nil {
				t.Fatalf("GetFacts() err = %v", err)
			}
			if res.Total != tc.wantTotal {
				t.Errorf("res.Total = %d, want %d", res.Total, tc.wantTotal)
			}
		})
	}
}

func TestToolHandler_GetContext(t *testing.T) {
	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "profile", "Profile fact", 0.8, []float32{1, 0})

	tests := []struct {
		name       string
		query      string
		limit      int
		cancelCtx  bool
		ollamaErr  error
		storeFail  bool
		wantErr    bool
		wantSubstr string
	}{
		{
			name:  "EmptyQuery",
			query: "",
			limit: 5,
		},
		{
			name:       "ValidQuery",
			query:      "Profile",
			limit:      5,
			wantSubstr: "Profile fact",
		},
		{
			name:       "NegativeLimitAndOllamaWarning",
			query:      "Profile",
			limit:      -1,
			ollamaErr:  errors.New("ollama down"),
			wantSubstr: "Profile fact",
		},
		{
			name:       "HighLimitClamped",
			query:      "Profile",
			limit:      50,
			wantSubstr: "Profile fact",
		},
		{
			name:      "CandidateSearchErrorGracefulEmpty",
			query:     "Profile",
			limit:     5,
			storeFail: true,
		},
		{
			name:      "CanceledContextReturnsError",
			query:     "Profile",
			limit:     5,
			cancelCtx: true,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ollama := &MockOllamaClient{embedding: []float32{1, 0}, err: tc.ollamaErr}
			handler := NewToolHandler(&Config{}, store, ollama)
			if tc.storeFail {
				store.SetFail("SearchCandidateFacts", errors.New("search err"))
				defer store.SetFail("", nil)
			}
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
			if !tc.wantErr && tc.wantSubstr != "" && !strings.Contains(res.Context, tc.wantSubstr) {
				t.Errorf("res.Context = %q, missing %q", res.Context, tc.wantSubstr)
			}
		})
	}
}

func TestServer_Endpoints(t *testing.T) {
	oldCounters, oldMeta := writeCountersDirFn, writeMetaDirFn
	defer func() {
		writeCountersDirFn, writeMetaDirFn = oldCounters, oldMeta
	}()

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
		coverDir   string
		counterErr error
		storeFail  string
		directMCP  bool
		errReader  bool
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
			coverDir:   t.TempDir(),
			wantStatus: http.StatusOK,
			wantSubstr: "ok",
		},
		{
			name:       "CoverageFlushError",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			coverDir:   t.TempDir(),
			counterErr: errors.New("flush err"),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "FactsJSON",
			method:     http.MethodGet,
			path:       "/facts?limit=10&offset=0",
			wantStatus: http.StatusOK,
			wantSubstr: `"facts"`,
		},
		{
			name:       "FactsStoreError",
			method:     http.MethodGet,
			path:       "/facts",
			storeFail:  "GetFactsPaginated",
			wantStatus: http.StatusInternalServerError,
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
			name:       "MCPReadError",
			method:     http.MethodPost,
			path:       "/mcp",
			directMCP:  true,
			errReader:  true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MCPDirectHealthRoute",
			method:     http.MethodGet,
			path:       "/health",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectFlushRoute",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectFactsRoute",
			method:     http.MethodGet,
			path:       "/facts",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectContextRoute",
			method:     http.MethodGet,
			path:       "/context?q=test",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDoublePrefixNormalized",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_memory_search","arguments":{"query":"test"}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPStoreToolCall",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"memory_store","arguments":{"fact_text":"tool call store"}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPForgetToolCall",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"memory_forget","arguments":{"fact_id":1}}}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOCOVERDIR", tc.coverDir)
			writeCountersDirFn = func(string) error { return tc.counterErr }
			writeMetaDirFn = func(string) error { return tc.counterErr }
			if tc.storeFail != "" {
				store.SetFail(tc.storeFail, errors.New("store err"))
				defer store.SetFail("", nil)
			}

			if tc.directMCP {
				var rBody io.Reader
				if tc.errReader {
					rBody = &mockErrReadCloser{}
				} else if tc.body != "" {
					rBody = strings.NewReader(tc.body)
				}
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(tc.method, tc.path, rBody)
				server.handleMCP(rec, req)
				if rec.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
				}
				return
			}

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

func TestNullVector_Scan(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		wantValid bool
		wantErr   bool
	}{
		{"NilInput", nil, false, false},
		{"StringVector", "[1.0,2.0]", true, false},
		{"PgvectorValue", pgvector.NewVector([]float32{3.0, 4.0}), true, false},
		{"InvalidIntInput", 12345, false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var nv NullVector
			err := nv.Scan(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NullVector.Scan() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && nv.Valid != tc.wantValid {
				t.Errorf("nv.Valid = %v, want %v", nv.Valid, tc.wantValid)
			}
		})
	}
}

func TestFactTime_Scan(t *testing.T) {
	now := time.Now().UTC()
	var nilTimePtr *time.Time

	tests := []struct {
		name      string
		input     any
		wantValid bool
		wantErr   bool
	}{
		{"NilInput", nil, false, false},
		{"NilTimePointer", nilTimePtr, false, false},
		{"ValidTimePointer", &now, true, false},
		{"ValidTimeValue", now, true, false},
		{"ValidRFC3339String", "2026-10-21T09:43:01Z", true, false},
		{"ValidSpaceString", "2026-10-21 09:00:00", true, false},
		{"EmptyString", "  ", false, false},
		{"InvalidDateString", "not-a-timestamp", false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ft FactTime
			err := ft.Scan(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FactTime.Scan() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && ft.Valid != tc.wantValid {
				t.Errorf("ft.Valid = %v, want %v", ft.Valid, tc.wantValid)
			}
		})
	}
}

func TestEscapeSQLLikeAndHelpers(t *testing.T) {
	RegisterSQLiteTestHook(nil)
	ctx, cancel := withTimeout(nil, 1*time.Second)
	defer cancel()
	if ctx == nil {
		t.Fatal("expected non-nil context from withTimeout(nil)")
	}
	closeWarn(nil, "nil")
	closeWarn(mockCloser{err: errors.New("close err")}, "mock")
	writeResponse(httptest.NewRecorder(), nil, []byte("ok"))
	writeResponse(&mockErrWriter{err: errors.New("write err")}, nil, []byte("fail"))

	if optionalVectorArg(nil) != nil {
		t.Error("expected nil for nil embedding")
	}
	if optionalVectorArg(make([]float32, ExpectedEmbeddingDim)) == nil {
		t.Error("expected non-nil for 384-dim embedding")
	}
	if got := EscapeSQLLike("a%b_c\\d"); got != "a\\%b\\_c\\\\d" {
		t.Errorf("EscapeSQLLike() = %q", got)
	}
}

func TestInitDB(t *testing.T) {
	addr := startMockPostgres(t)
	validDSN := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"

	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"NilConfig", nil, true},
		{"EmptyDSN", &Config{DatabaseURL: "   "}, true},
		{"UnreachableDSN", &Config{DatabaseURL: "postgres://127.0.0.1:1/bad?connect_timeout=1"}, true},
		{"ValidMockPostgres", &Config{DatabaseURL: validDSN}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := InitDB(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InitDB() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if db != nil {
				_ = db.Close()
			}
		})
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

	tests := []struct {
		name         string
		cfgPath      string
		dbURL        string
		runErr       error
		wantRunCall  bool
		wantExitCall bool
	}{
		{
			name:        "CleanExit",
			cfgPath:     testCfg,
			dbURL:       "postgres://user:pass@localhost:5432/db",
			wantRunCall: true,
		},
		{
			name:        "ErrServerClosedIgnored",
			cfgPath:     testCfg,
			dbURL:       "postgres://user:pass@localhost:5432/db",
			runErr:      http.ErrServerClosed,
			wantRunCall: true,
		},
		{
			name:         "RuntimeFailureCallsExit",
			cfgPath:      testCfg,
			dbURL:        "postgres://user:pass@localhost:5432/db",
			runErr:       context.DeadlineExceeded,
			wantRunCall:  true,
			wantExitCall: true,
		},
		{
			name:         "ConfigLoadFailureCallsExit",
			cfgPath:      filepath.Join(tmpDir, "nonexistent.yaml"),
			dbURL:        "",
			wantExitCall: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configPath = tc.cfgPath
			t.Setenv("CONFIG_PATH", tc.cfgPath)
			t.Setenv("DATABASE_URL", tc.dbURL)
			t.Setenv("POSTGRES_URL", "")

			runCalled := false
			exitCalled := false
			runAppFn = func(ctx context.Context, cfg *Config) error {
				runCalled = true
				return tc.runErr
			}
			exitFn = func(string, ...interface{}) {
				exitCalled = true
			}

			main()
			if runCalled != tc.wantRunCall {
				t.Errorf("runCalled = %v, want %v", runCalled, tc.wantRunCall)
			}
			if exitCalled != tc.wantExitCall {
				t.Errorf("exitCalled = %v, want %v", exitCalled, tc.wantExitCall)
			}
		})
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	addr := startMockPostgres(t)
	validDSN := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"

	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "NilConfigFails",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "InvalidDBFails",
			cfg:     &Config{DatabaseURL: "postgres://127.0.0.1:1/bad?connect_timeout=1"},
			wantErr: true,
		},
		{
			name:    "CleanLifecycleWithMockPostgres",
			cfg:     &Config{Port: "0", DatabaseURL: validDSN},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := RunApp(ctx, tc.cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("RunApp() unexpected err = %v", err)
			}
		})
	}
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

func TestSQLStore_InsertFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name       string
		nilDB      bool
		category   string
		factText   string
		importance float64
		queryErr   error
		rowSQL     string
		wantErr    bool
		wantID     int64
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			factText: "valid",
			wantErr:  true,
		},
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
			name:       "SuccessDefaultCategoryAndImportance",
			factText:   "valid fact",
			importance: -1,
			rowSQL:     "SELECT 42",
			wantID:     42,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:    dbMem,
					queryErr: tc.queryErr,
					rowSQL:   tc.rowSQL,
				})
			}
			id, err := store.InsertFact(ctx, tc.category, tc.factText, tc.importance, emb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InsertFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantID > 0 && id != tc.wantID {
				t.Errorf("InsertFact() id = %d, want %d", id, tc.wantID)
			}
		})
	}
}

func TestSQLStore_UpdateFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		category     string
		factText     string
		importance   float64
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			factText: "valid",
			wantErr:  true,
		},
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
			name:         "SuccessWithDefaults",
			factText:     "valid",
			importance:   -1,
			rowsAffected: 1,
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:        dbMem,
					execErr:      tc.execErr,
					rowsAffected: tc.rowsAffected,
				})
			}
			err := store.UpdateFact(ctx, 10, tc.category, tc.factText, tc.importance, emb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UpdateFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSQLStore_ReinforceFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		boost        float64
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
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
			name:         "SuccessDefaultBoost",
			boost:        -1,
			rowsAffected: 1,
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:        dbMem,
					execErr:      tc.execErr,
					rowsAffected: tc.rowsAffected,
				})
			}
			err := store.ReinforceFact(ctx, 10, "new text", emb, tc.boost)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReinforceFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSQLStore_DeleteFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:        dbMem,
					execErr:      tc.execErr,
					rowsAffected: tc.rowsAffected,
				})
			}
			err := store.DeleteFact(ctx, 10)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeleteFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSQLStore_FindDuplicateFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		nilDB      bool
		embLen     int
		minSim     float64
		queryErr   error
		rowSQL     string
		wantErr    bool
		wantNil    bool
		wantID     int64
		wantSimMin float64
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			embLen:  ExpectedEmbeddingDim,
			wantErr: true,
		},
		{
			name:    "ShortEmbeddingReturnsNil",
			embLen:  10,
			wantNil: true,
		},
		{
			name:    "ErrNoRowsReturnsNil",
			embLen:  ExpectedEmbeddingDim,
			minSim:  -1,
			rowSQL:  "SELECT 1 WHERE 1=0",
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
			minSim:     0.85,
			rowSQL:     "SELECT 7, 'cat', 'fact text', 0.9, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.92",
			wantID:     7,
			wantSimMin: 0.90,
		},
		{
			name:    "MatchedBelowMin",
			embLen:  ExpectedEmbeddingDim,
			minSim:  0.85,
			rowSQL:  "SELECT 8, 'cat', 'fact text', 0.9, '2026-10-21 09:00:00', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.40",
			wantNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:    dbMem,
					queryErr: tc.queryErr,
					rowSQL:   tc.rowSQL,
				})
			}
			testEmb := make([]float32, tc.embLen)
			dup, sim, err := store.FindDuplicateFact(ctx, testEmb, tc.minSim)
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

func TestSQLStore_SearchCandidateFacts(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		emb          []float32
		query        string
		category     string
		limit        int
		minScore     float64
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantPatterns []string
		wantCount    int
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			emb:     emb,
			query:   "q",
			wantErr: true,
		},
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
			limit:        10,
			minScore:     0.20,
			wantPatterns: []string{"vector_hits", "text_hits", "category = $2"},
		},
		{
			name:         "BothWithoutCategoryDefaultBounds",
			emb:          emb,
			query:        "test query",
			category:     "",
			limit:        -1,
			minScore:     -1,
			wantPatterns: []string{"vector_hits", "text_hits"},
		},
		{
			name:         "OnlyEmbeddingWithCategory",
			emb:          emb,
			query:        "",
			category:     "personal",
			limit:        10,
			minScore:     0.20,
			wantPatterns: []string{"candidates", "category = $2"},
		},
		{
			name:         "OnlyEmbeddingWithoutCategory",
			emb:          emb,
			query:        "",
			category:     "",
			limit:        10,
			minScore:     0.20,
			wantPatterns: []string{"candidates"},
		},
		{
			name:         "OnlyTextWithCategory",
			emb:          nil,
			query:        "test query",
			category:     "personal",
			limit:        10,
			minScore:     0.20,
			wantPatterns: []string{"text_hits", "category = $2"},
		},
		{
			name:         "OnlyTextWithoutCategory",
			emb:          nil,
			query:        "test query",
			category:     "",
			limit:        10,
			minScore:     0.20,
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
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:         dbMem,
					queryErr:      tc.queryErr,
					rowSQL:        tc.rowSQL,
					capturedQuery: &captured,
				})
			}
			results, err := store.SearchCandidateFacts(ctx, tc.emb, tc.query, tc.category, tc.limit, tc.minScore)
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

func TestSQLStore_GetFactsPaginated(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		filter       FactsFilter
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantPatterns []string
		wantTotal    int
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
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
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:         dbMem,
					queryErr:      tc.queryErr,
					rowSQL:        tc.rowSQL,
					capturedQuery: &captured,
				})
			}
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

func TestSQLStore_Close(t *testing.T) {
	memDB := setupTestDBMem(t)

	tests := []struct {
		name  string
		store *SQLStore
	}{
		{"NilStore", nil},
		{"ValidDBStore", NewSQLStore(memDB)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.store.Close(); err != nil {
				t.Errorf("Close() err = %v", err)
			}
		})
	}
}

func TestIsClientDisconnect(t *testing.T) {
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
}
