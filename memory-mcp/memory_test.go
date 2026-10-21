package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
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
	mu               sync.Mutex
	facts            map[int64]*Fact
	embeddings       map[int64][]float32
	nextID           int64
	failOp           string
	failErr          error
	unextractedTurns map[string][]Message
	activeThreads    []string
	watermarks       map[string]int64
	maxRowIDs        map[string]int64
	threadSummaries  map[string]string
	threadPriorTurns map[string][]Message
}

func newTestMemStore() *TestMemStore {
	return &TestMemStore{
		facts:            make(map[int64]*Fact),
		embeddings:       make(map[int64][]float32),
		nextID:           1,
		unextractedTurns: make(map[string][]Message),
		watermarks:       make(map[string]int64),
		maxRowIDs:        make(map[string]int64),
		threadSummaries:  make(map[string]string),
		threadPriorTurns: make(map[string][]Message),
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

func (m *TestMemStore) GetFactsMissingEmbeddings(ctx context.Context, limit int) ([]Fact, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetFactsMissingEmbeddings" {
		return nil, m.failErr
	}
	var res []Fact
	for id, f := range m.facts {
		if len(m.embeddings[id]) == 0 {
			res = append(res, *f)
			if limit > 0 && len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (m *TestMemStore) UpdateFactEmbedding(ctx context.Context, id int64, embedding []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "UpdateFactEmbedding" {
		return m.failErr
	}
	if _, ok := m.facts[id]; !ok {
		return ErrFactNotFound
	}
	m.embeddings[id] = append([]float32(nil), embedding...)
	return nil
}

func (m *TestMemStore) DecayAndPruneFacts(ctx context.Context, decayStep, pruneFloor float64, pruneAgeDays int) (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "DecayAndPruneFacts" {
		return 0, 0, m.failErr
	}
	if decayStep <= 0 {
		decayStep = 0.05
	}
	if pruneFloor <= 0 {
		pruneFloor = 0.15
	}
	var decayed, pruned int64
	for id, f := range m.facts {
		f.Importance = max(0.0, f.Importance-decayStep)
		f.LastDecayedAt = time.Now().UTC()
		decayed++
		if f.Importance <= pruneFloor {
			delete(m.facts, id)
			delete(m.embeddings, id)
			pruned++
		}
	}
	return decayed, pruned, nil
}

func (m *TestMemStore) GetActiveConversationsForExtraction(ctx context.Context, activeHours int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetActiveConversationsForExtraction" {
		return nil, m.failErr
	}
	return append([]string(nil), m.activeThreads...), nil
}

func (m *TestMemStore) GetUnextractedAdminTurns(ctx context.Context, threadID string) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetUnextractedAdminTurns" {
		return nil, m.failErr
	}
	return append([]Message(nil), m.unextractedTurns[threadID]...), nil
}

func (m *TestMemStore) GetMaxMessageRowID(ctx context.Context, threadID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetMaxMessageRowID" {
		return 0, m.failErr
	}
	return m.maxRowIDs[threadID], nil
}

func (m *TestMemStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "UpdateConversationFactWatermark" {
		return m.failErr
	}
	m.watermarks[threadID] = maxRowID
	return nil
}

func (m *TestMemStore) GetThreadContextBeforeWatermark(ctx context.Context, threadID string, priorTurnLimit int) (string, []Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetThreadContextBeforeWatermark" {
		return "", nil, m.failErr
	}
	summary := m.threadSummaries[threadID]
	turns := m.threadPriorTurns[threadID]
	if priorTurnLimit > 0 && len(turns) > priorTurnLimit {
		turns = turns[len(turns)-priorTurnLimit:]
	}
	return summary, append([]Message(nil), turns...), nil
}

func (m *TestMemStore) FindNearestScoredFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]ScoredFact, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "FindNearestScoredFacts" {
		return nil, m.failErr
	}
	if len(embedding) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 5
	}
	var pairs []ScoredFact
	for id, emb := range m.embeddings {
		if len(emb) == 0 {
			continue
		}
		sim := CosineSimilarity(embedding, emb)
		if sim >= minSim {
			if f, ok := m.facts[id]; ok {
				pairs = append(pairs, ScoredFact{Fact: *f, Score: sim})
			}
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Score > pairs[j].Score
	})
	var res []ScoredFact
	for i := 0; i < len(pairs) && i < limit; i++ {
		res = append(res, pairs[i])
	}
	return res, nil
}

func (m *TestMemStore) FindNearestFacts(ctx context.Context, embedding []float32, limit int, minSim float64) ([]Fact, error) {
	m.mu.Lock()
	if m.failOp == "FindNearestFacts" {
		err := m.failErr
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()
	scored, err := m.FindNearestScoredFacts(ctx, embedding, limit, minSim)
	if err != nil {
		return nil, err
	}
	facts := make([]Fact, len(scored))
	for i, sf := range scored {
		facts[i] = sf.Fact
	}
	return facts, nil
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
	_, _ = store.InsertFact(context.Background(), "user", "User lives in Metropolis", 0.9, []float32{1, 0})
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
			args:      MemorySearchArgs{Query: "Metropolis", Limit: 5},
			wantCount: 1,
		},
		{
			name:      "NegativeLimitAndOllamaWarningFallback",
			args:      MemorySearchArgs{Query: "Metropolis", Limit: -1},
			ollamaErr: errors.New("ollama offline"),
			wantCount: 1,
		},
		{
			name:      "HighLimitClamped",
			args:      MemorySearchArgs{Query: "Metropolis", Limit: 100},
			wantCount: 1,
		},
		{
			name:    "EmptyQueryError",
			args:    MemorySearchArgs{Query: ""},
			wantErr: true,
		},
		{
			name:      "StoreSearchFails",
			args:      MemorySearchArgs{Query: "Metropolis"},
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
	rowsSQL       string
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
		if errors.Is(q.queryErr, sql.ErrNoRows) {
			return q.dbMem.QueryRowContext(ctx, "SELECT 1 WHERE 1 = 0")
		}
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
	if q.rowsSQL != "" {
		return q.dbMem.QueryContext(ctx, q.rowsSQL)
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

func TestMessageMetadata_ValueScan(t *testing.T) {
	tests := []struct {
		name      string
		meta      MessageMetadata
		src       any
		wantErr   bool
		wantEmpty bool
		wantAdmin bool
	}{
		{
			name:      "EmptyValue",
			meta:      MessageMetadata{},
			wantEmpty: true,
		},
		{
			name:      "NonEmptyValue",
			meta:      MessageMetadata{IsAdmin: true, ChannelID: "c1", AuthorUsername: "user1"},
			wantAdmin: true,
		},
		{
			name:      "ScanNil",
			src:       nil,
			wantEmpty: true,
		},
		{
			name:      "ScanBytes",
			src:       []byte(`{"is_admin":true,"channel_id":"c2"}`),
			wantAdmin: true,
		},
		{
			name:      "ScanString",
			src:       `{"is_admin":true,"channel_id":"c3"}`,
			wantAdmin: true,
		},
		{
			name:      "ScanEmptyString",
			src:       `{}`,
			wantEmpty: true,
		},
		{
			name:      "ScanNullString",
			src:       `null`,
			wantEmpty: true,
		},
		{
			name:    "ScanInvalidJSON",
			src:     `{not-valid-json`,
			wantErr: true,
		},
		{
			name:    "ScanUnsupportedType",
			src:     12345,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.src != nil || tc.wantErr {
				var target MessageMetadata
				err := target.Scan(tc.src)
				if (err != nil) != tc.wantErr {
					t.Fatalf("Scan() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && target.IsEmpty() != tc.wantEmpty {
					t.Fatalf("Scan() IsEmpty = %v, wantEmpty = %v", target.IsEmpty(), tc.wantEmpty)
				}
				if !tc.wantErr && target.IsAdmin != tc.wantAdmin {
					t.Fatalf("Scan() IsAdmin = %v, wantAdmin = %v", target.IsAdmin, tc.wantAdmin)
				}
				return
			}
			val, err := tc.meta.Value()
			if err != nil {
				t.Fatalf("Value() unexpected err: %v", err)
			}
			if tc.wantEmpty && val != "{}" {
				t.Fatalf("Value() = %v, want {}", val)
			}
			if tc.wantAdmin && !strings.Contains(val.(string), `"is_admin":true`) {
				t.Fatalf("Value() = %v, want is_admin:true", val)
			}
		})
	}
}

func TestIsExtractableAdminTurn(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want bool
	}{
		{
			name: "CompletedAdminNormalText",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "Here is your answer.",
				Metadata:     MessageMetadata{IsAdmin: true},
			},
			want: true,
		},
		{
			name: "NotCompleted",
			msg: Message{
				Status:       "PENDING",
				ResponseText: "Here is your answer.",
				Metadata:     MessageMetadata{IsAdmin: true},
			},
			want: false,
		},
		{
			name: "NotAdmin",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "Here is your answer.",
				Metadata:     MessageMetadata{IsAdmin: false},
			},
			want: false,
		},
		{
			name: "BracketPrefix",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "[SILENT_TURN]",
				Metadata:     MessageMetadata{IsAdmin: true},
			},
			want: false,
		},
		{
			name: "EmptyResponseText",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "   ",
				Metadata:     MessageMetadata{IsAdmin: true},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsExtractableAdminTurn(tc.msg)
			if got != tc.want {
				t.Fatalf("IsExtractableAdminTurn() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSQLStore_GetFactsMissingEmbeddings(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		limit        int
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantCount    int
		wantPatterns []string
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "QueryError",
			queryErr: errors.New("query failed"),
			wantErr:  true,
		},
		{
			name:    "RowScanError",
			rowSQL:  "SELECT 'not-an-int' AS id",
			wantErr: true,
		},
		{
			name:         "SuccessWithLimit",
			limit:        5,
			wantPatterns: []string{"SELECT id", "embedding IS NULL", "LIMIT 5"},
		},
		{
			name:         "SuccessNoLimit",
			limit:        0,
			wantPatterns: []string{"SELECT id", "embedding IS NULL"},
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
			facts, err := store.GetFactsMissingEmbeddings(ctx, tc.limit)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetFactsMissingEmbeddings() err = %v, wantErr = %v", err, tc.wantErr)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
			if tc.wantCount > 0 && len(facts) != tc.wantCount {
				t.Errorf("len(facts) = %d, want %d", len(facts), tc.wantCount)
			}
		})
	}
}

func TestSQLStore_UpdateFactEmbedding(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		emb          []float32
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			emb:     validEmb,
			wantErr: true,
		},
		{
			name:    "InvalidDimension",
			emb:     []float32{1.0, 2.0},
			wantErr: true,
		},
		{
			name:    "ExecError",
			emb:     validEmb,
			execErr: errors.New("exec error"),
			wantErr: true,
		},
		{
			name:         "NotFoundZeroRows",
			emb:          validEmb,
			rowsAffected: 0,
			wantErr:      true,
		},
		{
			name:         "Success",
			emb:          validEmb,
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
			err := store.UpdateFactEmbedding(ctx, 42, tc.emb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UpdateFactEmbedding() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSQLStore_DecayAndPruneFacts(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		decayStep    float64
		pruneFloor   float64
		pruneAgeDays int
		execErr      error
		rowsAffected int64
		wantErr      bool
		wantDecayed  int64
		wantPruned   int64
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:    "DecayExecError",
			execErr: errors.New("decay failed"),
			wantErr: true,
		},
		{
			name:         "SuccessDefaults",
			decayStep:    -1,
			pruneFloor:   -1,
			pruneAgeDays: -1,
			rowsAffected: 3,
			wantErr:      false,
			wantDecayed:  3,
			wantPruned:   3,
		},
		{
			name:         "SuccessCustom",
			decayStep:    0.1,
			pruneFloor:   0.2,
			pruneAgeDays: 30,
			rowsAffected: 2,
			wantErr:      false,
			wantDecayed:  2,
			wantPruned:   2,
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
			decayed, pruned, err := store.DecayAndPruneFacts(ctx, tc.decayStep, tc.pruneFloor, tc.pruneAgeDays)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DecayAndPruneFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if decayed != tc.wantDecayed || pruned != tc.wantPruned {
					t.Errorf("DecayAndPruneFacts() = (%d, %d), want (%d, %d)", decayed, pruned, tc.wantDecayed, tc.wantPruned)
				}
			}
		})
	}
}

func TestSQLStore_GetActiveConversationsForExtraction(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		activeHours  int
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantPatterns []string
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "QueryError",
			queryErr: errors.New("query failed"),
			wantErr:  true,
		},
		{
			name:    "RowScanError",
			rowSQL:  "SELECT 12345 AS not_a_string",
			wantErr: false,
		},
		{
			name:         "SuccessDefaultHours",
			activeHours:  -1,
			wantPatterns: []string{"SELECT DISTINCT m.thread_id FROM messages m", "is_admin"},
		},
		{
			name:         "SuccessCustomHours",
			activeHours:  48,
			wantPatterns: []string{"SELECT DISTINCT m.thread_id FROM messages m"},
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
			threads, err := store.GetActiveConversationsForExtraction(ctx, tc.activeHours)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetActiveConversationsForExtraction() err = %v, wantErr = %v", err, tc.wantErr)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
			_ = threads
		})
	}
}

func TestSQLStore_GetUnextractedAdminTurns(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		threadID     string
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantEmpty    bool
		wantPatterns []string
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "th-1",
			wantErr:  true,
		},
		{
			name:      "EmptyThreadID",
			threadID:  "   ",
			wantErr:   false,
			wantEmpty: true,
		},
		{
			name:     "QueryError",
			threadID: "th-1",
			queryErr: errors.New("query error"),
			wantErr:  true,
		},
		{
			name:     "RowScanError",
			threadID: "th-1",
			rowSQL:   "SELECT 'id-only' AS id",
			wantErr:  true,
		},
		{
			name:         "Success",
			threadID:     "th-1",
			wantPatterns: []string{"m.thread_id = $1", "m.status = 'COMPLETED'", "is_admin"},
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
			turns, err := store.GetUnextractedAdminTurns(ctx, tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetUnextractedAdminTurns() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantEmpty && len(turns) != 0 {
				t.Errorf("expected empty turns, got %d", len(turns))
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
		})
	}
}

func TestSQLStore_GetMaxMessageRowID(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		threadID     string
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantPatterns []string
		wantID       int64
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "th-1",
			wantErr:  true,
		},
		{
			name:     "EmptyThreadID",
			threadID: "",
			wantErr:  false,
			wantID:   0,
		},
		{
			name:     "QueryError",
			threadID: "th-1",
			queryErr: errors.New("scan error"),
			wantErr:  true,
		},
		{
			name:         "SuccessWithRow",
			threadID:     "th-1",
			rowSQL:       "SELECT 42 AS max_row_id",
			wantPatterns: []string{"SELECT MAX(row_id) FROM messages WHERE thread_id = $1"},
			wantID:       42,
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
			id, err := store.GetMaxMessageRowID(ctx, tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetMaxMessageRowID() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && id != tc.wantID {
				t.Errorf("GetMaxMessageRowID() = %d, want %d", id, tc.wantID)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
		})
	}
}

func TestSQLStore_UpdateConversationFactWatermark(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		nilDB        bool
		threadID     string
		maxRowID     int64
		execErr      error
		wantErr      bool
		wantPatterns []string
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "th-1",
			wantErr:  true,
		},
		{
			name:     "EmptyThreadID",
			threadID: "",
			wantErr:  false,
		},
		{
			name:     "ExecError",
			threadID: "th-1",
			execErr:  errors.New("exec error"),
			wantErr:  true,
		},
		{
			name:         "Success",
			threadID:     "th-1",
			maxRowID:     99,
			wantPatterns: []string{"INSERT INTO sessions (thread_id", "ON CONFLICT(thread_id) DO UPDATE"},
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
					execErr:       tc.execErr,
					capturedQuery: &captured,
				})
			}
			err := store.UpdateConversationFactWatermark(ctx, tc.threadID, tc.maxRowID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UpdateConversationFactWatermark() err = %v, wantErr = %v", err, tc.wantErr)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
		})
	}
}

func TestSQLStore_FindNearestFacts(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		emb          []float32
		limit        int
		minSim       float64
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
			wantErr: true,
		},
		{
			name:      "InvalidEmbeddingDim",
			emb:       []float32{1.0, 2.0},
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:     "QueryError",
			emb:      emb,
			queryErr: errors.New("nearest query error"),
			wantErr:  true,
		},
		{
			name:    "RowScanError",
			emb:     emb,
			rowSQL:  "SELECT 'invalid' AS id",
			wantErr: true,
		},
		{
			name:         "SuccessDefaults",
			emb:          emb,
			limit:        -1,
			minSim:       -1,
			wantPatterns: []string{"WITH candidates AS", "ORDER BY embedding <=> $1", "similarity >="},
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
			facts, err := store.FindNearestFacts(ctx, tc.emb, tc.limit, tc.minSim)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FindNearestFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(captured, pat) {
					t.Errorf("captured query missing %q: %s", pat, captured)
				}
			}
			if tc.wantCount > 0 && len(facts) != tc.wantCount {
				t.Errorf("len(facts) = %d, want %d", len(facts), tc.wantCount)
			}
		})
	}
}

func TestSQLStore_FindNearestScoredFacts(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		emb          []float32
		limit        int
		minSim       float64
		queryErr     error
		rowSQL       string
		wantErr      bool
		wantCount    int
		wantMinScore float64
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			emb:     emb,
			wantErr: true,
		},
		{
			name:      "InvalidEmbeddingDim",
			emb:       []float32{1.0, 2.0},
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:     "QueryError",
			emb:      emb,
			queryErr: errors.New("nearest scored query error"),
			wantErr:  true,
		},
		{
			name:    "RowScanError",
			emb:     emb,
			rowSQL:  "SELECT 'invalid' AS id",
			wantErr: true,
		},
		{
			name:         "SuccessDefaults",
			emb:          emb,
			limit:        -1,
			minSim:       -1,
			rowSQL:       "SELECT 10, 'routine', 'Morning walk', 0.8, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 0.77",
			wantCount:    1,
			wantMinScore: 0.77,
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
			scored, err := store.FindNearestScoredFacts(ctx, tc.emb, tc.limit, tc.minSim)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FindNearestScoredFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantCount > 0 && len(scored) != tc.wantCount {
				t.Errorf("len(scored) = %d, want %d", len(scored), tc.wantCount)
			}
			if tc.wantMinScore > 0 && len(scored) > 0 && scored[0].Score < tc.wantMinScore {
				t.Errorf("scored[0].Score = %f, want >= %f", scored[0].Score, tc.wantMinScore)
			}
		})
	}
}

func TestSQLStore_GetThreadContextBeforeWatermark(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name          string
		nilDB         bool
		threadID      string
		priorLimit    int
		queryErr      error
		rowSQL        string
		rowsSQL       string
		wantErr       bool
		wantSummary   string
		wantTurnCount int
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "t-1",
			wantErr:  true,
		},
		{
			name:     "EmptyThreadID",
			threadID: "   ",
			wantErr:  false,
		},
		{
			name:        "SessionQueryErrNoRows",
			threadID:    "t-1",
			queryErr:    sql.ErrNoRows,
			wantErr:     false,
			wantSummary: "",
		},
		{
			name:     "SessionQueryError",
			threadID: "t-1",
			queryErr: errors.New("db disconnect"),
			wantErr:  true,
		},
		{
			name:        "LastExtractedRowIDZero",
			threadID:    "t-1",
			rowSQL:      "SELECT 'summary text', 0",
			wantSummary: "summary text",
		},
		{
			name:       "MessagesScanError",
			threadID:   "t-1",
			rowSQL:     "SELECT 'summary', 15",
			rowsSQL:    "SELECT 'invalid' AS id",
			priorLimit: 2,
			wantErr:    true,
		},
		{
			name:          "SuccessChronologicalOrder",
			threadID:      "t-1",
			rowSQL:        "SELECT 'Session summary here', 20",
			rowsSQL:       "SELECT 'm2', 12, 't-1', 'second prompt', 'second response', 'COMPLETED', '{\"is_admin\":true}' UNION ALL SELECT 'm1', 8, 't-1', 'first prompt', 'first response', 'COMPLETED', '{\"is_admin\":true}'",
			priorLimit:    -1,
			wantSummary:   "Session summary here",
			wantTurnCount: 2,
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
					rowsSQL:  tc.rowsSQL,
				})
			}
			summary, turns, err := store.GetThreadContextBeforeWatermark(ctx, tc.threadID, tc.priorLimit)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetThreadContextBeforeWatermark() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if summary != tc.wantSummary {
					t.Errorf("summary = %q, want %q", summary, tc.wantSummary)
				}
				if len(turns) != tc.wantTurnCount {
					t.Errorf("len(turns) = %d, want %d", len(turns), tc.wantTurnCount)
				}
				if len(turns) == 2 && turns[0].RowID > turns[1].RowID {
					t.Errorf("turns not chronological: row_id[0]=%d, row_id[1]=%d", turns[0].RowID, turns[1].RowID)
				}
			}
		})
	}
}

func TestHTTPBrainLLMClient(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		statusCode int
		body       string
		prompt     string
		rawURL     string
		wantErr    bool
		wantResp   string
	}{
		{
			name:       "Success200",
			statusCode: http.StatusOK,
			body:       `{"response":"extracted fact output"}`,
			prompt:     "hello",
			wantResp:   "extracted fact output",
		},
		{
			name:       "Non200Status",
			statusCode: http.StatusBadRequest,
			body:       `error prompt too long`,
			prompt:     "hello",
			wantErr:    true,
		},
		{
			name:       "InvalidJSONResponse",
			statusCode: http.StatusOK,
			body:       `not-json`,
			prompt:     "hello",
			wantErr:    true,
		},
		{
			name:       "DefaultURLFallback",
			statusCode: http.StatusOK,
			rawURL:     "http://127.0.0.1:0/bad",
			prompt:     "hello",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var srv *httptest.Server
			if tc.statusCode != 0 {
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Content-Type") != "application/json" {
						http.Error(w, "bad content type", http.StatusBadRequest)
						return
					}
					var req brainLLMRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
			}

			targetURL := tc.rawURL
			if srv != nil {
				targetURL = srv.URL
			}
			fn := HTTPBrainLLMClient(targetURL, nil)
			resp, err := fn(ctx, tc.prompt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HTTPBrainLLMClient() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && resp != tc.wantResp {
				t.Errorf("resp = %q, want %q", resp, tc.wantResp)
			}
		})
	}
}

func TestFormatExtractableTurns(t *testing.T) {
	tests := []struct {
		name      string
		turns     []Message
		wantEmpty bool
		wantSub   string
	}{
		{
			name:      "EmptyTurns",
			turns:     nil,
			wantEmpty: true,
		},
		{
			name: "SkipEmptyContentAndResponse",
			turns: []Message{
				{Content: "   ", ResponseText: "   "},
			},
			wantEmpty: true,
		},
		{
			name: "NormalTurns",
			turns: []Message{
				{Content: "What is my favorite tea?", ResponseText: "You love matcha latte."},
			},
			wantSub: "User: What is my favorite tea?\nAssistant: You love matcha latte.",
		},
		{
			name: "TruncationOver20000Chars",
			turns: []Message{
				{Content: strings.Repeat("A", 15000), ResponseText: strings.Repeat("B", 10000)},
			},
			wantSub: "Assistant: ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatExtractableTurns(tc.turns)
			if tc.wantEmpty && got != "" {
				t.Fatalf("expected empty, got %q", got)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("expected output to contain %q, got length %d", tc.wantSub, len(got))
			}
		})
	}
}

func TestParseFactsJSON(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantErr   bool
		wantCount int
	}{
		{
			name:    "EmptyRaw",
			raw:     "   ",
			wantErr: true,
		},
		{
			name:    "NoJSONBrace",
			raw:     "Just some plain text without braces",
			wantErr: true,
		},
		{
			name:    "InvalidJSON",
			raw:     "Here is: {invalid_json}",
			wantErr: true,
		},
		{
			name:      "ValidJSON",
			raw:       `{"facts":[{"action":"ADD","category":"user_preference","fact_text":"User likes green tea","importance_score":0.8}]}`,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "ValidJSONWithPreamble",
			raw:       "Sure, here are the extracted facts:\n```json\n{\"facts\":[{\"action\":\"DELETE\",\"delete_id\":10,\"category\":\"user_preference\",\"fact_text\":\"old fact\",\"importance_score\":0.5}]}\n```",
			wantErr:   false,
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := parseFactsJSON(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseFactsJSON() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && len(payload.Facts) != tc.wantCount {
				t.Errorf("len(Facts) = %d, want %d", len(payload.Facts), tc.wantCount)
			}
		})
	}
}

func TestBackfillMissingEmbeddings(t *testing.T) {
	ctx := context.Background()
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		ctx          context.Context
		nilStore     bool
		nilClient    bool
		storeFailOp  string
		ollamaFail   bool
		facts        []Fact
		wantBackfill int
		wantErr      bool
	}{
		{
			name:     "NilStoreFails",
			ctx:      ctx,
			nilStore: true,
			wantErr:  true,
		},
		{
			name:      "NilClientFails",
			ctx:       ctx,
			nilClient: true,
			wantErr:   true,
		},
		{
			name:        "StoreQueryFails",
			ctx:         ctx,
			storeFailOp: "GetFactsMissingEmbeddings",
			wantErr:     true,
		},
		{
			name:         "EmptyMissingList",
			ctx:          ctx,
			wantBackfill: 0,
			wantErr:      false,
		},
		{
			name: "SuccessBackfilling",
			ctx:  ctx,
			facts: []Fact{
				{ID: 1, FactText: "Fact without embedding 1"},
				{ID: 2, FactText: "   "},
				{ID: 3, FactText: "Fact without embedding 2"},
			},
			wantBackfill: 2,
		},
		{
			name:       "OllamaFailsContinues",
			ctx:        ctx,
			ollamaFail: true,
			facts: []Fact{
				{ID: 4, FactText: "Fact with failing ollama"},
			},
			wantBackfill: 0,
		},
		{
			name:        "StoreUpdateFailsContinues",
			ctx:         ctx,
			storeFailOp: "UpdateFactEmbedding",
			facts: []Fact{
				{ID: 5, FactText: "Fact with failing store update"},
			},
			wantBackfill: 0,
		},
		{
			name: "ContextCanceledAborts",
			ctx:  canceledCtx,
			facts: []Fact{
				{ID: 6, FactText: "Fact canceled"},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store Store
			if !tc.nilStore {
				mem := newTestMemStore()
				for _, f := range tc.facts {
					mem.facts[f.ID] = &Fact{ID: f.ID, FactText: f.FactText}
				}
				if tc.storeFailOp != "" {
					mem.SetFail(tc.storeFailOp, errors.New("simulated store failure"))
				}
				store = mem
			}

			var client OllamaClient
			if !tc.nilClient {
				mockOllama := &MockOllamaClient{embedding: validEmb}
				if tc.ollamaFail {
					mockOllama.err = errors.New("ollama err")
				}
				client = mockOllama
			}

			count, err := BackfillMissingEmbeddings(tc.ctx, store, client)
			if (err != nil) != tc.wantErr {
				t.Fatalf("BackfillMissingEmbeddings() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && count != tc.wantBackfill {
				t.Errorf("BackfillMissingEmbeddings() = %d, want %d", count, tc.wantBackfill)
			}
		})
	}
}

func TestExtractActiveConversationFacts(t *testing.T) {
	ctx := context.Background()
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name          string
		ctx           context.Context
		nilStore      bool
		nilClient     bool
		nilLLM        bool
		threads       []string
		turns         []Message
		threadMaxID   int64
		storeFailOp   string
		llmResp       string
		llmErr        error
		lockMutex     bool
		seedNearest   bool
		wantErr       bool
		wantWatermark int64
	}{
		{
			name:      "OverlappingExecutionSkipped",
			ctx:       ctx,
			threads:   []string{"th-overlap"},
			lockMutex: true,
			wantErr:   false,
		},
		{
			name:     "NilStoreFails",
			ctx:      ctx,
			nilStore: true,
			wantErr:  true,
		},
		{
			name:      "NilClientFails",
			ctx:       ctx,
			nilClient: true,
			wantErr:   true,
		},
		{
			name:    "NilLLMFails",
			ctx:     ctx,
			nilLLM:  true,
			wantErr: true,
		},
		{
			name:    "NoActiveThreads",
			ctx:     ctx,
			threads: nil,
			wantErr: false,
		},
		{
			name:        "GetActiveConversationsError",
			ctx:         ctx,
			storeFailOp: "GetActiveConversationsForExtraction",
			wantErr:     true,
		},
		{
			name:        "GetMaxMessageRowIDErrorContinues",
			ctx:         ctx,
			threads:     []string{"th-1"},
			storeFailOp: "GetMaxMessageRowID",
			wantErr:     false,
		},
		{
			name:        "GetUnextractedAdminTurnsErrorContinues",
			ctx:         ctx,
			threads:     []string{"th-1"},
			storeFailOp: "GetUnextractedAdminTurns",
			wantErr:     false,
		},
		{
			name:    "EmptyTranscriptMarksWatermark",
			ctx:     ctx,
			threads: []string{"th-empty"},
			turns: []Message{
				{Content: "   ", ResponseText: "   ", RowID: 15},
			},
			wantErr:       false,
			wantWatermark: 15,
		},
		{
			name:          "EmptyTurnsSliceFallsBackToThreadMaxRowID",
			ctx:           ctx,
			threads:       []string{"th-fallback-max"},
			threadMaxID:   42,
			wantErr:       false,
			wantWatermark: 42,
		},
		{
			name:          "WatermarkCappedToExtractedTurnsMaxRowID",
			ctx:           ctx,
			threads:       []string{"th-capped"},
			turns:         []Message{{Content: "Turn 50", ResponseText: "Resp 50", RowID: 50}},
			threadMaxID:   100,
			llmResp:       `{"facts":[]}`,
			wantErr:       false,
			wantWatermark: 50,
		},
		{
			name:        "EmptyTranscriptWatermarkErrorContinues",
			ctx:         ctx,
			threads:     []string{"th-empty-err"},
			turns:       []Message{{Content: " ", ResponseText: " ", RowID: 16}},
			storeFailOp: "UpdateConversationFactWatermark",
			wantErr:     false,
		},
		{
			name:    "LLMCallFailsContinues",
			ctx:     ctx,
			threads: []string{"th-llm-fail"},
			turns: []Message{
				{Content: "turn 1", ResponseText: "resp 1", RowID: 20},
			},
			llmErr:  errors.New("llm down"),
			wantErr: false,
		},
		{
			name:    "ParseJSONFailsContinues",
			ctx:     ctx,
			threads: []string{"th-bad-json"},
			turns: []Message{
				{Content: "turn 1", ResponseText: "resp 1", RowID: 25},
			},
			llmResp: "invalid json string",
			wantErr: false,
		},
		{
			name:        "ExistingFactsIncludedInPrompt",
			ctx:         ctx,
			threads:     []string{"th-existing"},
			turns:       []Message{{Content: "User lives in Metropolis", ResponseText: "Recorded.", RowID: 28}},
			seedNearest: true,
			llmResp:     `{"facts":[]}`,
			wantErr:     false,
		},
		{
			name:    "FullReconciliationSuccess",
			ctx:     ctx,
			threads: []string{"th-full"},
			turns: []Message{
				{Content: "User's address is 123 Example St", ResponseText: "Got it.", RowID: 30},
			},
			llmResp: `{"facts":[
				{"action":"ADD","category":"user_preference","fact_text":"User loves espresso","importance_score":0.9},
				{"action":"UPDATE","supersedes_id":1,"category":"user_preference","fact_text":"User updated preference","importance_score":0.8},
				{"action":"DELETE","delete_id":2,"category":"general","fact_text":"Obsolete fact"},
				{"action":"NOOP"}
			]}`,
			wantErr:       false,
			wantWatermark: 30,
		},
		{
			name:    "ContextCanceledAborts",
			ctx:     canceledCtx,
			threads: []string{"th-cancel-1", "th-cancel-2"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.lockMutex {
				extractionMutex.Lock()
				defer extractionMutex.Unlock()
			}
			var store Store
			var memStore *TestMemStore
			if !tc.nilStore {
				memStore = newTestMemStore()
				memStore.activeThreads = tc.threads
				for _, tid := range tc.threads {
					memStore.unextractedTurns[tid] = tc.turns
					for _, trn := range tc.turns {
						if trn.RowID > memStore.maxRowIDs[tid] {
							memStore.maxRowIDs[tid] = trn.RowID
						}
					}
					if tc.threadMaxID > memStore.maxRowIDs[tid] {
						memStore.maxRowIDs[tid] = tc.threadMaxID
					}
				}
				memStore.facts[1] = &Fact{ID: 1, Category: "user_preference", FactText: "Old preference"}
				memStore.facts[2] = &Fact{ID: 2, Category: "general", FactText: "Obsolete fact"}
				if tc.seedNearest {
					memStore.facts[99] = &Fact{ID: 99, Category: "system_config", FactText: "Existing nearest fact"}
					memStore.embeddings[99] = validEmb
				}
				if tc.storeFailOp != "" {
					memStore.SetFail(tc.storeFailOp, errors.New("simulated error"))
				}
				store = memStore
			}

			var client OllamaClient
			if !tc.nilClient {
				client = &MockOllamaClient{embedding: validEmb}
			}

			var llm LLMClientFunc
			if !tc.nilLLM {
				llm = func(ctx context.Context, prompt string) (string, error) {
					if tc.llmErr != nil {
						return "", tc.llmErr
					}
					return tc.llmResp, nil
				}
			}

			err := ExtractActiveConversationFacts(tc.ctx, store, client, llm, 24)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractActiveConversationFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantWatermark > 0 && memStore != nil {
				for _, tid := range tc.threads {
					if memStore.watermarks[tid] != tc.wantWatermark {
						t.Errorf("watermark for %s = %d, want %d", tid, memStore.watermarks[tid], tc.wantWatermark)
					}
				}
			}
		})
	}
}

func TestReconcileExtractedFact(t *testing.T) {
	ctx := context.Background()
	validEmb := make([]float32, ExpectedEmbeddingDim)
	validEmb[0] = 1.0

	tests := []struct {
		name        string
		item        ExtractedFactItem
		ollamaErr   error
		storeFailOp string
		seedDup     bool
	}{
		{
			name: "NOOPAction",
			item: ExtractedFactItem{Action: "NOOP"},
		},
		{
			name: "DeleteByDeleteIDTooShort",
			item: ExtractedFactItem{Action: "DELETE", DeleteID: 1},
		},
		{
			name: "DeleteBySupersedesIDTooShort",
			item: ExtractedFactItem{Action: "DELETE", SupersedesID: 2},
		},
		{
			name: "DeleteActionIgnored",
			item: ExtractedFactItem{Action: "DELETE", DeleteID: 1, FactText: "Obsolete fact with sufficient length"},
		},
		{
			name: "UpdateEmptyTextSkipped",
			item: ExtractedFactItem{Action: "UPDATE", SupersedesID: 4, FactText: "  "},
		},
		{
			name:      "AddEmbeddingErrorLogged",
			item:      ExtractedFactItem{Action: "ADD", FactText: "New durable fact text candidate"},
			ollamaErr: errors.New("embedding error"),
		},
		{
			name:    "AddDuplicateReinforces",
			item:    ExtractedFactItem{Action: "ADD", FactText: "Duplicate text candidate here"},
			seedDup: true,
		},
		{
			name:        "AddInsertErrorLogged",
			item:        ExtractedFactItem{Action: "ADD", FactText: "Failing insert candidate here"},
			storeFailOp: "InsertFact",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			if tc.seedDup {
				mem.facts[10] = &Fact{ID: 10, FactText: "Duplicate text candidate here", Importance: 0.8}
				mem.embeddings[10] = validEmb
			}
			if tc.storeFailOp != "" {
				mem.SetFail(tc.storeFailOp, errors.New("fail"))
			}

			mockClient := &MockOllamaClient{embedding: validEmb, err: tc.ollamaErr}
			reconcileExtractedFact(ctx, mem, mockClient, tc.item)
		})
	}
}

func TestEvaluateCandidateAdmission(t *testing.T) {
	tests := []struct {
		name           string
		item           ExtractedFactItem
		wantAdmit      bool
		wantImportance float64
		wantReason     string
	}{
		{
			name:           "TooShort",
			item:           ExtractedFactItem{FactText: "short"},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "too_short",
		},
		{
			name:           "EphemeralTelemetryPR",
			item:           ExtractedFactItem{FactText: "Merged PR #1234 into main branch successfully", Importance: 0.7},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "ephemeral_telemetry",
		},
		{
			name:           "EphemeralVanadisChunk",
			item:           ExtractedFactItem{FactText: "Executed vanadis_chunk task cleanly for build", Importance: 0.6},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "ephemeral_telemetry",
		},
		{
			name:           "EphemeralCommitSha",
			item:           ExtractedFactItem{FactText: "Rolled back commit 0123456789abcdef due to panic", Importance: 0.5},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "ephemeral_telemetry",
		},
		{
			name:           "EphemeralAutoMerge",
			item:           ExtractedFactItem{FactText: "Verified that auto-merge armed on the target pull request", Importance: 0.6},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "ephemeral_telemetry",
		},
		{
			name:           "DurableDirectiveOverridesEphemeral",
			item:           ExtractedFactItem{FactText: "Always verify CI before pull request #500 merges", Importance: 0.7},
			wantAdmit:      true,
			wantImportance: 0.80,
			wantReason:     "admitted",
		},
		{
			name:           "UserPreferenceBoost",
			item:           ExtractedFactItem{Category: "user_preference", FactText: "User drinks oat milk latte in the morning", Importance: 0.6},
			wantAdmit:      true,
			wantImportance: 0.70,
			wantReason:     "admitted",
		},
		{
			name:           "DefaultImportanceAndPrefersBoost",
			item:           ExtractedFactItem{Category: "general", FactText: "User prefers dark roast coffee every day", Importance: 0},
			wantAdmit:      true,
			wantImportance: 0.70,
			wantReason:     "admitted",
		},
		{
			name:           "ImportanceClampedHigh",
			item:           ExtractedFactItem{Category: "user_preference", FactText: "User is strictly allergic to peanuts", Importance: 0.95},
			wantAdmit:      true,
			wantImportance: 1.0,
			wantReason:     "admitted",
		},
		{
			name:           "ImportanceClampedLow",
			item:           ExtractedFactItem{Category: "general", FactText: "A random general statement with low value", Importance: 0.05},
			wantAdmit:      true,
			wantImportance: 0.10,
			wantReason:     "admitted",
		},
		{
			name:           "NoopActionRejected",
			item:           ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail", Importance: 0.6},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "noop_action",
		},
		{
			name:           "NoopActionCaseInsensitiveWithWhitespace",
			item:           ExtractedFactItem{Action: "  noop  ", FactText: "User mentioned something ephemeral yesterday", Importance: 0.8},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "noop_action",
		},
		{
			name:           "DeleteActionRejected",
			item:           ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact about favorite drinks", Importance: 0.6},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "delete_action",
		},
		{
			name:           "DeleteActionCaseInsensitiveWithWhitespace",
			item:           ExtractedFactItem{Action: "  delete  ", FactText: "Old preference that is no longer active", Importance: 0.8},
			wantAdmit:      false,
			wantImportance: 0,
			wantReason:     "delete_action",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admit, importance, reason := EvaluateCandidateAdmission(tc.item)
			if admit != tc.wantAdmit {
				t.Errorf("admit = %v, want %v", admit, tc.wantAdmit)
			}
			if tc.wantAdmit && math.Abs(importance-tc.wantImportance) > 1e-6 {
				t.Errorf("importance = %.2f, want %.2f", importance, tc.wantImportance)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestBuildCandidateReconcilePrompt(t *testing.T) {
	candidate := ExtractedFactItem{
		Category:   "user_preference",
		FactText:   "User prefers dark mode across all interfaces",
		Importance: 0.85,
	}
	neighbors := []ScoredFact{
		{
			Fact: Fact{
				ID:       42,
				Category: "user_preference",
				FactText: "User prefers light mode on mobile displays",
			},
			Score: 0.76,
		},
	}
	prompt := buildCandidateReconcilePrompt(candidate, neighbors)
	for _, expected := range []string{
		"CANDIDATE FACT:",
		"Category: user_preference",
		"User prefers dark mode across all interfaces",
		"0.85",
		"EXISTING CANDIDATE NEIGHBORS",
		"ID 42 (similarity 0.76)",
		"User prefers light mode on mobile displays",
		"supersedes_id",
		"delete_id",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("buildCandidateReconcilePrompt missing expected substring %q in:\n%s", expected, prompt)
		}
	}
}

func TestReconcileCandidateFact_Tiers(t *testing.T) {
	ctx := context.Background()
	dim := ExpectedEmbeddingDim
	baseEmb := make([]float32, dim)
	baseEmb[0] = 1.0

	highSimEmb := make([]float32, dim)
	copy(highSimEmb, baseEmb)

	tier3Emb := make([]float32, dim)
	tier3Emb[0] = 0.75
	tier3Emb[1] = 0.6614378

	tests := []struct {
		name           string
		nilStore       bool
		nilClient      bool
		item           ExtractedFactItem
		ollamaErr      error
		storeFailOp    string
		neighborID     int64
		neighborEmb    []float32
		llmResp        string
		llmErr         error
		nilLLM         bool
		wantInserted   bool
		wantReinforced bool
		wantUpdated    bool
		wantDeleted    bool
	}{
		{
			name:     "NilStoreSafe",
			nilStore: true,
			item:     ExtractedFactItem{FactText: "Valid candidate fact text"},
		},
		{
			name:      "NilClientSafe",
			nilClient: true,
			item:      ExtractedFactItem{FactText: "Valid candidate fact text"},
		},
		{
			name: "UnadmittedTooShort",
			item: ExtractedFactItem{FactText: "short"},
		},
		{
			name: "UnadmittedNoopAction",
			item: ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail"},
		},
		{
			name: "UnadmittedDeleteAction",
			item: ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact text should not be inserted or reinforced"},
		},
		{
			name:      "EmbeddingErrorLogged",
			item:      ExtractedFactItem{FactText: "Valid candidate fact text"},
			ollamaErr: errors.New("embed fail"),
		},
		{
			name:        "FindNearestErrorLogged",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			storeFailOp: "FindNearestScoredFacts",
		},
		{
			name:         "Tier1_NoNeighbors_Insert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text", Category: "user_preference"},
			wantInserted: true,
		},
		{
			name:         "Tier1_InsertErrorLogged",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			storeFailOp:  "InsertFact",
			wantInserted: false,
		},
		{
			name:           "Tier2_HighSimReinforce",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    highSimEmb,
			wantReinforced: true,
		},
		{
			name:           "Tier2_ReinforceErrorLogged",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    highSimEmb,
			storeFailOp:    "ReinforceFact",
			wantReinforced: false,
		},
		{
			name:         "Tier3_NilLLM_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			nilLLM:       true,
			wantInserted: true,
		},
		{
			name:         "Tier3_LLMError_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmErr:       errors.New("timeout"),
			wantInserted: true,
		},
		{
			name:         "Tier3_ParseError_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      "invalid json",
			wantInserted: true,
		},
		{
			name:         "Tier3_EmptyFacts_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[]}`,
			wantInserted: true,
		},
		{
			name:        "Tier3_ValidUpdate",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here","importance_score":0.8}]}`,
			wantUpdated: true,
		},
		{
			name:         "Tier3_UnwhitelistedUpdate_FallbackInsert",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[{"action":"UPDATE","supersedes_id":999,"fact_text":"Updated text here"}]}`,
			wantInserted: true,
		},
		{
			name:        "Tier3_UpdateStoreErrorLogged",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here"}]}`,
			storeFailOp: "UpdateFact",
		},
		{
			name:        "Tier3_ValidDelete",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"DELETE","delete_id":10}]}`,
			wantDeleted: true,
		},
		{
			name:        "Tier3_UnwhitelistedDelete_NOOP",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"DELETE","delete_id":999}]}`,
			wantDeleted: false,
		},
		{
			name:        "Tier3_DeleteStoreErrorLogged",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"DELETE","delete_id":10}]}`,
			storeFailOp: "DeleteFact",
		},
		{
			name:        "Tier3_NOOP",
			item:        ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:  10,
			neighborEmb: tier3Emb,
			llmResp:     `{"facts":[{"action":"NOOP"}]}`,
		},
		{
			name:         "Tier3_ADD",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[{"action":"ADD"}]}`,
			wantInserted: true,
		},
		{
			name:         "Tier3_DefaultUnknownAction",
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:   10,
			neighborEmb:  tier3Emb,
			llmResp:      `{"facts":[{"action":"OTHER"}]}`,
			wantInserted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store Store
			var mem *TestMemStore
			if !tc.nilStore {
				mem = newTestMemStore()
				if tc.neighborID > 0 && len(tc.neighborEmb) > 0 {
					mem.facts[tc.neighborID] = &Fact{ID: tc.neighborID, FactText: "Existing neighbor text", Importance: 0.7, ReinforceCount: 1}
					mem.embeddings[tc.neighborID] = tc.neighborEmb
				}
				if tc.storeFailOp != "" {
					mem.SetFail(tc.storeFailOp, errors.New("simulated store failure"))
				}
				store = mem
			}

			var client OllamaClient
			if !tc.nilClient {
				client = &MockOllamaClient{embedding: baseEmb, err: tc.ollamaErr}
			}

			var llm LLMClientFunc
			if !tc.nilLLM {
				llm = func(ctx context.Context, prompt string) (string, error) {
					if tc.llmErr != nil {
						return "", tc.llmErr
					}
					return tc.llmResp, nil
				}
			}

			reconcileCandidateFact(ctx, store, client, llm, tc.item)

			if mem != nil {
				if tc.wantInserted && len(mem.facts) == 0 && tc.neighborID == 0 {
					t.Errorf("expected fact to be inserted, but store has 0 facts")
				}
				if tc.wantReinforced && mem.facts[tc.neighborID].ReinforceCount < 2 {
					t.Errorf("expected neighbor %d to be reinforced, got count %d", tc.neighborID, mem.facts[tc.neighborID].ReinforceCount)
				}
				if tc.wantUpdated && mem.facts[tc.neighborID].FactText != "Updated text here" {
					t.Errorf("expected neighbor %d to be updated with new text, got %q", tc.neighborID, mem.facts[tc.neighborID].FactText)
				}
				if tc.wantDeleted && mem.facts[tc.neighborID] != nil {
					t.Errorf("expected neighbor %d to be deleted from store, but it remains", tc.neighborID)
				}
			}
		})
	}
}

type sequenceOllamaClient struct {
	mu    sync.Mutex
	calls int
}

func (s *sequenceOllamaClient) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[s.calls%ExpectedEmbeddingDim] = 1.0
	s.calls++
	return emb, nil
}

func TestProcessThreadFacts_ContextAndCoreference(t *testing.T) {
	ctx := context.Background()
	dim := ExpectedEmbeddingDim
	baseEmb := make([]float32, dim)
	baseEmb[0] = 1.0

	tests := []struct {
		name                string
		nilStore            bool
		storeFailOp         string
		unextractedErr      bool
		emptyTurns          bool
		withSummary         bool
		withPriorTurns      bool
		llmErr              error
		parseErr            bool
		manyCandidates      bool
		wantCandidateCapped bool
		wantErr             bool
	}{
		{
			name:     "NilStoreError",
			nilStore: true,
			wantErr:  true,
		},
		{
			name:           "UnextractedTurnsError",
			unextractedErr: true,
			wantErr:        true,
		},
		{
			name:       "EmptyTurnsMarksWatermark",
			emptyTurns: true,
			wantErr:    false,
		},
		{
			name:           "PriorSummaryAndTurnsInjected",
			withSummary:    true,
			withPriorTurns: true,
			wantErr:        false,
		},
		{
			name:        "ContextQueryErrorIgnoredAndContinues",
			storeFailOp: "GetThreadContextBeforeWatermark",
			wantErr:     false,
		},
		{
			name:    "LLMErrorFails",
			llmErr:  errors.New("llm error"),
			wantErr: true,
		},
		{
			name:     "ParseErrorFails",
			parseErr: true,
			wantErr:  true,
		},
		{
			name:                "ManyCandidatesCappedAt5",
			manyCandidates:      true,
			wantCandidateCapped: true,
			wantErr:             false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store Store
			var mem *TestMemStore
			if !tc.nilStore {
				mem = newTestMemStore()
				store = mem
				threadID := "th-test"
				if tc.unextractedErr {
					mem.SetFail("GetUnextractedAdminTurns", errors.New("db error"))
				} else if !tc.emptyTurns {
					mem.unextractedTurns[threadID] = []Message{
						{RowID: 10, ThreadID: threadID, Content: "He likes matcha tea", ResponseText: "Understood", Status: "COMPLETED", Metadata: MessageMetadata{IsAdmin: true}},
					}
				}
				if tc.withSummary {
					mem.threadSummaries[threadID] = "User name is Bob"
				}
				if tc.withPriorTurns {
					mem.threadPriorTurns[threadID] = []Message{
						{RowID: 5, ThreadID: threadID, Content: "My name is Bob", ResponseText: "Hello Bob", Status: "COMPLETED", Metadata: MessageMetadata{IsAdmin: true}},
					}
				}
				if tc.storeFailOp != "" {
					mem.SetFail(tc.storeFailOp, errors.New("simulated store failure"))
				}
			}

			var client OllamaClient
			if tc.manyCandidates {
				client = &sequenceOllamaClient{}
			} else {
				client = &MockOllamaClient{embedding: baseEmb}
			}

			var capturedPrompt string
			llm := func(c context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				if tc.llmErr != nil {
					return "", tc.llmErr
				}
				if tc.parseErr {
					return "invalid json response", nil
				}
				if tc.manyCandidates {
					return `{"facts":[
						{"category":"general","fact_text":"Candidate fact text number 1","importance_score":0.6},
						{"category":"general","fact_text":"Candidate fact text number 2","importance_score":0.6},
						{"category":"general","fact_text":"Candidate fact text number 3","importance_score":0.6},
						{"category":"general","fact_text":"Candidate fact text number 4","importance_score":0.6},
						{"category":"general","fact_text":"Candidate fact text number 5","importance_score":0.6},
						{"category":"general","fact_text":"Candidate fact text number 6","importance_score":0.6},
						{"category":"general","fact_text":"Candidate fact text number 7","importance_score":0.6}
					]}`, nil
				}
				return `{"facts":[{"category":"user_preference","fact_text":"Bob likes matcha tea in the afternoon","importance_score":0.8}]}`, nil
			}

			err := processThreadFacts(ctx, store, client, llm, "th-test")
			if (err != nil) != tc.wantErr {
				t.Fatalf("processThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.withSummary && !strings.Contains(capturedPrompt, "PRIOR THREAD SUMMARY:\nUser name is Bob") {
				t.Errorf("captured prompt missing summary: %s", capturedPrompt)
			}
			if tc.withPriorTurns && !strings.Contains(capturedPrompt, "PRIOR TURNS (FOR COREFERENCE RESOLUTION ONLY") {
				t.Errorf("captured prompt missing prior turns: %s", capturedPrompt)
			}
			if tc.wantCandidateCapped && mem != nil {
				// 5 candidates should be inserted, not 7
				if len(mem.facts) != 5 {
					t.Errorf("len(mem.facts) = %d, want 5 (capped)", len(mem.facts))
				}
			}
		})
	}
}

func TestWorker_LifecycleAndTick(t *testing.T) {
	ctx := context.Background()
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name       string
		tick       int64
		nilStore   bool
		nilOllama  bool
		nilLLM     bool
		storeFail  string
		wantDecay  bool
		wantHourly bool
	}{
		{
			name:       "Tick0HourlyExtractionAndBackfill",
			tick:       0,
			wantHourly: true,
		},
		{
			name:       "Tick120HourlyExtractionAndBackfill",
			tick:       120,
			wantHourly: true,
		},
		{
			name:      "Tick60DailyDecay",
			tick:      60,
			wantDecay: true,
		},
		{
			name:      "Tick2940DailyDecay",
			tick:      2940,
			wantDecay: true,
		},
		{
			name: "Tick1Noop",
			tick: 1,
		},
		{
			name:     "NilStoreSafe",
			tick:     0,
			nilStore: true,
		},
		{
			name:      "NilOllamaSafe",
			tick:      0,
			nilOllama: true,
		},
		{
			name:   "NilLLMSafe",
			tick:   0,
			nilLLM: true,
		},
		{
			name:      "DecayErrorHandled",
			tick:      60,
			storeFail: "DecayAndPruneFacts",
			wantDecay: true,
		},
		{
			name:     "DailyDecayNilStore",
			tick:     60,
			nilStore: true,
		},
		{
			name:       "HourlyBackfillErrorHandled",
			tick:       0,
			storeFail:  "GetFactsMissingEmbeddings",
			wantHourly: true,
		},
		{
			name:       "HourlyExtractionErrorHandled",
			tick:       0,
			storeFail:  "GetActiveConversationsForExtraction",
			wantHourly: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			if tc.storeFail != "" {
				mem.SetFail(tc.storeFail, errors.New("store err"))
			}
			var store Store = mem
			if tc.nilStore {
				store = nil
			}
			var client OllamaClient = &MockOllamaClient{embedding: validEmb}
			if tc.nilOllama {
				client = nil
			}
			var llm LLMClientFunc = func(ctx context.Context, prompt string) (string, error) {
				return `{"facts":[]}`, nil
			}
			if tc.nilLLM {
				llm = nil
			}

			w := NewWorker(WorkerConfig{
				Store:        store,
				Ollama:       client,
				LLMClient:    llm,
				InitialDelay: 0,
				TickInterval: -1,
			})

			w.Tick(ctx, tc.tick)
		})
	}
}

func TestWorker_Start(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name         string
		initialDelay time.Duration
		ctx          context.Context
	}{
		{
			name:         "CancelDuringInitialDelay",
			initialDelay: 10 * time.Minute,
			ctx:          canceledCtx,
		},
		{
			name:         "CancelWithZeroDelay",
			initialDelay: 0,
			ctx:          canceledCtx,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWorker(WorkerConfig{
				InitialDelay: tc.initialDelay,
				TickInterval: 10 * time.Millisecond,
			})
			err := w.Start(tc.ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Start() err = %v, want context.Canceled", err)
			}
		})
	}
}
