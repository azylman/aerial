package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

	_ "modernc.org/sqlite"
)

// --- Test Fixtures & Mocks ---

// RegisterSQLiteTestHook allows unit tests to inject in-memory databases.
func RegisterSQLiteTestHook(fn func(string) (*sql.DB, error)) {
	sqliteTestInitHook = fn
}

type TestMemStore struct {
	mu                   sync.Mutex
	facts                map[int64]*Fact
	embeddings           map[int64][]float32
	nextID               int64
	failOp               string
	failErr              error
	unextractedTurns     map[string][]Message
	activeThreads        []string
	watermarks           map[string]int64
	maxRowIDs            map[string]int64
	threadSummaries      map[string]string
	threadPriorTurns     map[string][]Message
	threadRetrievedFacts map[string][]string
	threadToolCalls      map[string][]string
	proceduralCandidates map[int64]*ProceduralCandidate
	nextCandID           int64
}

func newTestMemStore() *TestMemStore {
	return &TestMemStore{
		facts:                make(map[int64]*Fact),
		embeddings:           make(map[int64][]float32),
		nextID:               1,
		unextractedTurns:     make(map[string][]Message),
		watermarks:           make(map[string]int64),
		maxRowIDs:            make(map[string]int64),
		threadSummaries:      make(map[string]string),
		threadPriorTurns:     make(map[string][]Message),
		threadRetrievedFacts: make(map[string][]string),
		threadToolCalls:      make(map[string][]string),
		proceduralCandidates: make(map[int64]*ProceduralCandidate),
		nextCandID:           1,
	}
}

func testEmbedding(firstVal float32) []float32 {
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = firstVal
	return emb
}

func testEmbeddingAt(idx int, val float32) []float32 {
	emb := make([]float32, ExpectedEmbeddingDim)
	if idx >= 0 && idx < ExpectedEmbeddingDim {
		emb[idx] = val
	}
	return emb
}

func (m *TestMemStore) SetFail(op string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOp = op
	m.failErr = err
}

func (m *TestMemStore) InsertFact(ctx context.Context, factText string, embedding []float32) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "InsertFact" {
		return 0, m.failErr
	}
	factText = strings.TrimSpace(factText)
	if factText == "" {
		return 0, fmt.Errorf("fact text cannot be empty")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return 0, fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	id := m.nextID
	m.nextID++
	now := time.Now().UTC()
	f := &Fact{
		ID:               id,
		FactText:         factText,
		CreatedAt:        now,
		LastReinforcedAt: now,
		ReinforceCount:   1,
	}
	m.facts[id] = f
	m.embeddings[id] = append([]float32(nil), embedding...)
	return id, nil
}

func (m *TestMemStore) UpdateFact(ctx context.Context, id int64, factText string, embedding []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "UpdateFact" {
		return m.failErr
	}
	factText = strings.TrimSpace(factText)
	if factText == "" {
		return fmt.Errorf("fact text cannot be empty")
	}
	if len(embedding) != ExpectedEmbeddingDim {
		return fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(embedding))
	}
	f, ok := m.facts[id]
	if !ok {
		return ErrFactNotFound
	}
	f.FactText = factText
	f.LastReinforcedAt = time.Now().UTC()
	f.ReinforceCount++
	m.embeddings[id] = append([]float32(nil), embedding...)
	return nil
}

func (m *TestMemStore) ReinforceFact(ctx context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "ReinforceFact" {
		return m.failErr
	}
	f, ok := m.facts[id]
	if !ok {
		return ErrFactNotFound
	}
	f.LastReinforcedAt = time.Now().UTC()
	f.ReinforceCount++
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
	if len(embedding) != ExpectedEmbeddingDim {
		return nil, 0, nil
	}
	if minSim <= 0 {
		minSim = 0.85
	}
	var bestFact *Fact
	var bestSim float64
	for id, emb := range m.embeddings {
		if len(emb) != ExpectedEmbeddingDim {
			continue
		}
		sim := CosineSimilarity(embedding, emb)
		if sim >= minSim && sim > bestSim {
			bestSim = sim
			cpy := *m.facts[id]
			bestFact = &cpy
		}
	}
	return bestFact, bestSim, nil
}

func (m *TestMemStore) SearchCandidateFacts(ctx context.Context, embedding []float32, queryText string, candidateLimit int, minScore float64) ([]ScoredFactWithEmbedding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "SearchCandidateFacts" {
		return nil, m.failErr
	}
	queryText = strings.TrimSpace(queryText)
	if len(embedding) != ExpectedEmbeddingDim || queryText == "" {
		return nil, nil
	}
	if candidateLimit <= 0 {
		candidateLimit = DefaultCandidateLimit
	}
	if minScore <= 0 {
		minScore = DefaultMinScore
	}
	var candidates []ScoredFactWithEmbedding
	for id, f := range m.facts {
		emb := m.embeddings[id]
		if len(emb) != ExpectedEmbeddingDim {
			continue
		}
		sim := CosineSimilarity(embedding, emb)
		var textScore float64
		if strings.Contains(strings.ToLower(f.FactText), strings.ToLower(queryText)) {
			textScore = 1.0
		}
		score := (DefaultDenseWeight*sim + DefaultSparseWeight*textScore) * (1.0 + 0.15*math.Log(1.0+float64(f.ReinforceCount)))
		if score >= minScore {
			candidates = append(candidates, ScoredFactWithEmbedding{
				Fact:      *f,
				Embedding: emb,
				Score:     score,
			})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	if candidateLimit > 0 && len(candidates) > candidateLimit {
		candidates = candidates[:candidateLimit]
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
		if filter.Query != "" && !strings.Contains(strings.ToLower(f.FactText), strings.ToLower(filter.Query)) {
			continue
		}
		list = append(list, *f)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].ReinforceCount != list[j].ReinforceCount {
			return list[i].ReinforceCount > list[j].ReinforceCount
		}
		if !list[i].LastReinforcedAt.Equal(list[j].LastReinforcedAt) {
			return list[i].LastReinforcedAt.After(list[j].LastReinforcedAt)
		}
		return list[i].ID > list[j].ID
	})
	total := len(list)
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
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

func (m *TestMemStore) GetAllFactsWithEmbeddings(ctx context.Context) ([]ScoredFactWithEmbedding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetAllFactsWithEmbeddings" {
		return nil, m.failErr
	}
	var res []ScoredFactWithEmbedding
	var ids []int64
	for id := range m.facts {
		if emb, ok := m.embeddings[id]; ok && len(emb) > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		res = append(res, ScoredFactWithEmbedding{
			Fact:      *m.facts[id],
			Embedding: m.embeddings[id],
		})
	}
	return res, nil
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

func (m *TestMemStore) GetThreadTrajectoryContext(ctx context.Context, threadID string) (retrievedFacts []string, toolCalls []string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetThreadTrajectoryContext" {
		return nil, nil, m.failErr
	}
	return append([]string(nil), m.threadRetrievedFacts[threadID]...), append([]string(nil), m.threadToolCalls[threadID]...), nil
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

func (m *TestMemStore) InsertProceduralCandidate(ctx context.Context, cand ProceduralCandidate) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "InsertProceduralCandidate" {
		return 0, m.failErr
	}
	if strings.TrimSpace(cand.Trigger) == "" {
		return 0, fmt.Errorf("trigger pattern cannot be empty")
	}
	id := m.nextCandID
	m.nextCandID++
	now := time.Now().UTC()
	cand.ID = id
	if cand.Domain == "" {
		cand.Domain = "discord"
	}
	if cand.Status == "" {
		cand.Status = "pending"
	}
	if cand.Steps == nil {
		cand.Steps = []string{}
	}
	if cand.Avoid == nil {
		cand.Avoid = []string{}
	}
	cand.CreatedAt = now
	cpy := cand
	m.proceduralCandidates[id] = &cpy
	return id, nil
}

func (m *TestMemStore) GetPendingProceduralCandidates(ctx context.Context) ([]ProceduralCandidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetPendingProceduralCandidates" {
		return nil, m.failErr
	}
	var res []ProceduralCandidate
	for _, c := range m.proceduralCandidates {
		if c.Status == "pending" {
			res = append(res, *c)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if !res[i].CreatedAt.Equal(res[j].CreatedAt) {
			return res[i].CreatedAt.Before(res[j].CreatedAt)
		}
		return res[i].ID < res[j].ID
	})
	if len(res) > 100 {
		res = res[:100]
	}
	return res, nil
}

func (m *TestMemStore) MarkProceduralCandidatesConsolidated(ctx context.Context, ids []int64, prNumber int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "MarkProceduralCandidatesConsolidated" {
		return m.failErr
	}
	for _, id := range ids {
		if c, ok := m.proceduralCandidates[id]; ok {
			c.Status = "consolidated"
			c.PRNumber = prNumber
		}
	}
	return nil
}

func (m *TestMemStore) MarkProceduralCandidatesSkipped(ctx context.Context, ids []int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "MarkProceduralCandidatesSkipped" {
		return m.failErr
	}
	for _, id := range ids {
		if c, ok := m.proceduralCandidates[id]; ok {
			c.Status = "skipped"
		}
	}
	return nil
}

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

func TestToolHandler_MemorySearch(t *testing.T) {
	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "User lives in Metropolis", testEmbedding(1.0))
	_, _ = store.InsertFact(context.Background(), "User works in tech", testEmbeddingAt(1, 1.0))

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
			wantCount: 0,
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
			ollama := &MockOllamaClient{embedding: testEmbedding(1.0), err: tc.ollamaErr}
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
	existingID, _ := store.InsertFact(context.Background(), "Existing Fact", testEmbedding(1.0))

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
			args:       MemoryStoreArgs{FactText: "Brand new fact"},
			emb:        testEmbedding(1.0),
			wantAction: "reinforced",
			wantID:     existingID,
		},
		{
			name:      "ReinforceDuplicateError",
			args:      MemoryStoreArgs{FactText: "Brand new fact"},
			emb:       testEmbedding(1.0),
			storeFail: "ReinforceFact",
			wantErr:   true,
		},
		{
			name:       "InsertUniqueFact",
			args:       MemoryStoreArgs{FactText: "Unique fact"},
			emb:        testEmbeddingAt(2, 1.0),
			wantAction: "inserted",
		},
		{
			name:      "InsertWithOllamaErrorFails",
			args:      MemoryStoreArgs{FactText: "Fact with error"},
			ollamaErr: errors.New("embed error"),
			wantErr:   true,
		},
		{
			name:       "SupersedeExistingFact",
			args:       MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: existingID},
			emb:        testEmbedding(1.0),
			wantAction: "updated",
			wantID:     existingID,
		},
		{
			name:    "SupersedeNonexistentFails",
			args:    MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: 999999},
			emb:     testEmbedding(1.0),
			wantErr: true,
		},
		{
			name:      "SupersedeStoreError",
			args:      MemoryStoreArgs{FactText: "Updated Fact", SupersedesID: existingID},
			emb:       testEmbedding(1.0),
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
			emb:       testEmbeddingAt(3, 1.0),
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
	id, _ := store.InsertFact(context.Background(), "Forget me", testEmbedding(1.0))
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
	_, _ = store.InsertFact(context.Background(), "Profile fact", testEmbedding(1.0))
	handler := NewToolHandler(&Config{}, store, nil)

	tests := []struct {
		name      string
		filter    FactsFilter
		wantTotal int
	}{
		{
			name:      "MatchingQuery",
			filter:    FactsFilter{Query: "Profile"},
			wantTotal: 1,
		},
		{
			name:      "NonMatchingQuery",
			filter:    FactsFilter{Query: "other"},
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
	_, _ = store.InsertFact(context.Background(), "Profile fact", testEmbedding(1.0))

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
			wantSubstr: "",
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
			ollama := &MockOllamaClient{embedding: testEmbedding(1.0), err: tc.ollamaErr}
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
	_, _ = store.InsertFact(context.Background(), "Server fact test", testEmbedding(1.0))
	handler := NewToolHandler(&Config{}, store, &MockOllamaClient{embedding: testEmbedding(1.0)})
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
			name:       "ConsolidateOK",
			method:     http.MethodPost,
			path:       "/consolidate",
			wantStatus: http.StatusOK,
			wantSubstr: `"consolidated":0`,
		},
		{
			name:       "ConsolidateStoreError",
			method:     http.MethodPost,
			path:       "/consolidate",
			storeFail:  "GetAllFactsWithEmbeddings",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "ConsolidateMethodNotAllowed",
			method:     http.MethodGet,
			path:       "/consolidate",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "MCPDirectConsolidateRoute",
			method:     http.MethodPost,
			path:       "/consolidate",
			directMCP:  true,
			wantStatus: http.StatusOK,
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

	if got := EscapeSQLLike("a%b_c\\d"); got != "a\\%b\\_c\\\\d" {
		t.Errorf("EscapeSQLLike() = %q", got)
	}
}

func TestOpenDB(t *testing.T) {
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
			db, err := OpenDB(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB() err = %v, wantErr = %v", err, tc.wantErr)
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
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name     string
		nilDB    bool
		factText string
		emb      []float32
		queryErr error
		rowSQL   string
		wantErr  bool
		wantID   int64
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			factText: "valid",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "EmptyFactText",
			factText: "",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "InvalidEmbeddingDim",
			factText: "valid",
			emb:      make([]float32, 10),
			wantErr:  true,
		},
		{
			name:     "QueryError",
			factText: "valid fact",
			emb:      validEmb,
			queryErr: errors.New("insert error"),
			wantErr:  true,
		},
		{
			name:     "SuccessValidFact",
			factText: "valid fact",
			emb:      validEmb,
			rowSQL:   "SELECT 42",
			wantID:   42,
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
			id, err := store.InsertFact(ctx, tc.factText, tc.emb)
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
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []struct {
		name         string
		nilDB        bool
		factText     string
		emb          []float32
		execErr      error
		rowsAffected int64
		wantErr      bool
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			factText: "valid",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "EmptyFactText",
			factText: "",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "InvalidEmbeddingDim",
			factText: "valid",
			emb:      make([]float32, 10),
			wantErr:  true,
		},
		{
			name:     "ExecError",
			factText: "valid",
			emb:      validEmb,
			execErr:  errors.New("update err"),
			wantErr:  true,
		},
		{
			name:         "NotFoundZeroRows",
			factText:     "valid",
			emb:          validEmb,
			rowsAffected: 0,
			wantErr:      true,
		},
		{
			name:         "SuccessValidFact",
			factText:     "valid",
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
			err := store.UpdateFact(ctx, 10, tc.factText, tc.emb)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UpdateFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSQLStore_ReinforceFact(t *testing.T) {
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
			err := store.ReinforceFact(ctx, 10)
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
			rowSQL:     "SELECT 7, 'fact text', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.92",
			wantID:     7,
			wantSimMin: 0.90,
		},
		{
			name:    "MatchedBelowMin",
			embLen:  ExpectedEmbeddingDim,
			minSim:  0.85,
			rowSQL:  "SELECT 8, 'fact text', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.40",
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
			name:      "ShortEmbeddingReturnsNil",
			emb:       make([]float32, 10),
			query:     "fact",
			wantCount: 0,
		},
		{
			name:      "EmptyQueryReturnsNil",
			emb:       emb,
			query:     "",
			wantCount: 0,
		},
		{
			name:         "BothWithDefaultBounds",
			emb:          emb,
			query:        "test query",
			limit:        -1,
			minScore:     -1,
			wantPatterns: []string{"vector_hits", "text_hits", "rrf"},
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
			rowSQL:    "SELECT 5, 'scanned fact', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1, '[0.1,0.2]', 0.88",
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
			results, err := store.SearchCandidateFacts(ctx, tc.emb, tc.query, tc.limit, tc.minScore)
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
			name:         "SuccessWithQuery",
			filter:       FactsFilter{Query: "linux", Limit: 5, Offset: -1},
			wantPatterns: []string{"LOWER(fact_text) LIKE", "LIMIT 5 OFFSET 0"},
		},
		{
			name:         "OffsetOnly",
			filter:       FactsFilter{Offset: 10},
			wantPatterns: []string{"OFFSET 10"},
		},
		{
			name:      "SuccessfulRowsScan",
			filter:    FactsFilter{Limit: 5},
			rowSQL:    "SELECT 1, 'fact', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1",
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

func TestMessageMetadata_Scan(t *testing.T) {
	tests := []struct {
		name      string
		src       any
		wantErr   bool
		wantEmpty bool
		wantAdmin bool
	}{
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
			var target MessageMetadata
			err := target.Scan(tc.src)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Scan() err = %v, wantErr = %v", err, tc.wantErr)
			}
			isEmpty := target.ChannelID == "" && !target.IsAdmin
			if !tc.wantErr && isEmpty != tc.wantEmpty {
				t.Fatalf("Scan() isEmpty = %v, wantEmpty = %v", isEmpty, tc.wantEmpty)
			}
			if !tc.wantErr && target.IsAdmin != tc.wantAdmin {
				t.Fatalf("Scan() IsAdmin = %v, wantAdmin = %v", target.IsAdmin, tc.wantAdmin)
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
			rowSQL:       "SELECT 10, 'Morning walk', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 0.77",
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

func TestSQLStore_GetThreadTrajectoryContext(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name          string
		nilDB         bool
		threadID      string
		queryErr      error
		rowsSQL       string
		wantErr       bool
		wantFacts     []string
		wantToolCalls []string
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
			name:     "QueryError",
			threadID: "t-1",
			queryErr: errors.New("db disconnect"),
			wantErr:  true,
		},
		{
			name:     "ScanError",
			threadID: "t-1",
			rowsSQL:  "SELECT 'only_one_col'",
			wantErr:  true,
		},
		{
			name:     "SuccessParsingAndDeduplication",
			threadID: "t-1",
			rowsSQL: "SELECT 'USER_INPUT', '', '<retrieved_memory>\n- [id:42] User likes matcha\n- [id:42] User likes matcha\n- [id:43] Prefers dark mode\n</retrieved_memory>' " +
				"UNION ALL SELECT 'PLANNER_RESPONSE', 'memory_search', '{\"query\": \"matcha\"}' " +
				"UNION ALL SELECT 'USER_INPUT', '', '<\x5cRETRIEVED_MEMORY>\n- [id:42] User likes matcha\n- [id:44] Morning coffee at 8am\n<\x5c/RETRIEVED_MEMORY>' " +
				"UNION ALL SELECT 'OTHER', '', 'ignored'",
			wantErr: false,
			wantFacts: []string{
				"- [id:42] User likes matcha",
				"- [id:43] Prefers dark mode",
				"- [id:44] Morning coffee at 8am",
			},
			wantToolCalls: []string{
				`memory_search({"query": "matcha"})`,
			},
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
					rowsSQL:  tc.rowsSQL,
				})
			}
			facts, tools, err := store.GetThreadTrajectoryContext(ctx, tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetThreadTrajectoryContext() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if len(facts) != len(tc.wantFacts) {
					t.Fatalf("len(facts) = %d, want %d (%v)", len(facts), len(tc.wantFacts), facts)
				}
				for i := range facts {
					if facts[i] != tc.wantFacts[i] {
						t.Errorf("facts[%d] = %q, want %q", i, facts[i], tc.wantFacts[i])
					}
				}
				if len(tools) != len(tc.wantToolCalls) {
					t.Fatalf("len(tools) = %d, want %d (%v)", len(tools), len(tc.wantToolCalls), tools)
				}
				for i := range tools {
					if tools[i] != tc.wantToolCalls[i] {
						t.Errorf("tools[%d] = %q, want %q", i, tools[i], tc.wantToolCalls[i])
					}
				}
			}
		})
	}
}

func TestTruncateSingleLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "Empty",
			input:    "",
			maxLen:   10,
			expected: "",
		},
		{
			name:     "WhitespaceCollapse",
			input:    "   hello \n\t  world  \r\n test   ",
			maxLen:   50,
			expected: "hello world test",
		},
		{
			name:     "Truncated",
			input:    "this is a long sentence that exceeds the limit",
			maxLen:   10,
			expected: "this is a ",
		},
		{
			name:     "NoLimit",
			input:    "sentence with spaces",
			maxLen:   0,
			expected: "sentence with spaces",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := truncateSingleLine(tc.input, tc.maxLen)
			if res != tc.expected {
				t.Errorf("truncateSingleLine(%q, %d) = %q, want %q", tc.input, tc.maxLen, res, tc.expected)
			}
		})
	}
}

func TestExtractTrajectorySignals(t *testing.T) {
	tests := []struct {
		name          string
		stepType      string
		toolName      string
		content       string
		wantFacts     []string
		wantToolCalls string
	}{
		{
			name:     "UserInputStandardRetrievedMemory",
			stepType: "USER_INPUT",
			content:  "<retrieved_memory>\n- [id:1] User likes matcha\n- [id:2] Prefers dark mode\n</retrieved_memory>",
			wantFacts: []string{
				"- [id:1] User likes matcha",
				"- [id:2] Prefers dark mode",
			},
		},
		{
			name:     "UserInputSanitizedEscapedRetrievedMemory",
			stepType: "USER_INPUT",
			content:  "<\x5cRETRIEVED_MEMORY>\n- [id:10] Fact A\n- [id:10] Fact A\n<\x5c/RETRIEVED_MEMORY>",
			wantFacts: []string{
				"- [id:10] Fact A",
			},
		},
		{
			name:      "UserInputNoRetrievedMemory",
			stepType:  "USER_INPUT",
			content:   "Just regular user content",
			wantFacts: nil,
		},
		{
			name:          "PlannerResponseWithTool",
			stepType:      "PLANNER_RESPONSE",
			toolName:      "memory_search",
			content:       "{\"query\": \n \"something\"}",
			wantToolCalls: "memory_search({\"query\": \"something\"})",
		},
		{
			name:     "PlannerResponseEmptyTool",
			stepType: "PLANNER_RESPONSE",
			toolName: "",
			content:  "some thought",
		},
		{
			name:     "OtherStepType",
			stepType: "SYSTEM",
			content:  "<retrieved_memory>- [id:1] test</retrieved_memory>",
		},
		{
			name:          "PlannerResponseWithSkill",
			stepType:      "PLANNER_RESPONSE",
			toolName:      "read_file",
			content:       `{"Path": "/skills/ha-operations/SKILL.md"}`,
			wantToolCalls: `read_file({"Path": "/skills/ha-operations/SKILL.md"}) [SKILL_LOADED: ha-operations]`,
		},
		{
			name:          "GenericStepWithSkill",
			stepType:      "GENERIC",
			toolName:      "",
			content:       "---\nname: ha-operations\ndescription: test",
			wantToolCalls: `[SKILL_LOADED: ha-operations]`,
		},
		{
			name:          "ToolStepOutputNoSkill",
			stepType:      "TOOL",
			toolName:      "read_file",
			content:       "file content without skill",
			wantToolCalls: `-> OUTPUT(read_file): file content without skill`,
		},
		{
			name:          "RunCommandStepEmptyTool",
			stepType:      "RUN_COMMAND",
			toolName:      "",
			content:       "command line output",
			wantToolCalls: `-> OUTPUT(run_command): command line output`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			facts, tool := extractTrajectorySignals(tc.stepType, tc.toolName, tc.content)
			if len(facts) != len(tc.wantFacts) {
				t.Fatalf("len(facts) = %d, want %d (%v)", len(facts), len(tc.wantFacts), facts)
			}
			for i := range facts {
				if facts[i] != tc.wantFacts[i] {
					t.Errorf("facts[%d] = %q, want %q", i, facts[i], tc.wantFacts[i])
				}
			}
			if tool != tc.wantToolCalls {
				t.Errorf("tool = %q, want %q", tool, tc.wantToolCalls)
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
				memStore.facts[1] = &Fact{ID: 1, FactText: "Old preference"}
				memStore.facts[2] = &Fact{ID: 2, FactText: "Obsolete fact"}
				if tc.seedNearest {
					memStore.facts[99] = &Fact{ID: 99, FactText: "Existing nearest fact"}
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
				mem.facts[10] = &Fact{ID: 10, FactText: "Duplicate text candidate here"}
				mem.embeddings[10] = validEmb
			}
			if tc.storeFailOp != "" {
				mem.SetFail(tc.storeFailOp, errors.New("fail"))
			}

			mockClient := &MockOllamaClient{embedding: validEmb, err: tc.ollamaErr}
			reconcileCandidateFact(ctx, mem, mockClient, nil, tc.item)
		})
	}
}

func TestEvaluateCandidateAdmission(t *testing.T) {
	tests := []struct {
		name       string
		item       ExtractedFactItem
		wantAdmit  bool
		wantReason string
	}{
		{
			name:       "TooShort",
			item:       ExtractedFactItem{FactText: "short"},
			wantAdmit:  false,
			wantReason: "too_short",
		},
		{
			name:       "EphemeralTelemetryPR",
			item:       ExtractedFactItem{FactText: "Merged PR #1234 into main branch successfully"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralVanadisChunk",
			item:       ExtractedFactItem{FactText: "Executed vanadis_chunk task cleanly for build"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralCommitSha",
			item:       ExtractedFactItem{FactText: "Rolled back commit 0123456789abcdef due to panic"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralAutoMerge",
			item:       ExtractedFactItem{FactText: "Verified that auto-merge armed on the target pull request"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralAwaitingDelivery",
			item:       ExtractedFactItem{FactText: "Hardware sensor is awaiting delivery before installation"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "EphemeralCurrentlyInProgress",
			item:       ExtractedFactItem{FactText: "Database migration task is currently in progress on the cluster"},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name:       "DurableDirectiveOverridesEphemeral",
			item:       ExtractedFactItem{FactText: "Always verify CI before pull request #500 merges"},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name:       "ValidDurableFact",
			item:       ExtractedFactItem{FactText: "User drinks oat milk latte in the morning"},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name:       "NoopActionRejected",
			item:       ExtractedFactItem{Action: "NOOP", FactText: "User discussed the weather today in detail"},
			wantAdmit:  false,
			wantReason: "noop_action",
		},
		{
			name:       "NoopActionCaseInsensitiveWithWhitespace",
			item:       ExtractedFactItem{Action: "  noop  ", FactText: "User mentioned something ephemeral yesterday"},
			wantAdmit:  false,
			wantReason: "noop_action",
		},
		{
			name:       "DeleteActionRejected",
			item:       ExtractedFactItem{Action: "DELETE", FactText: "Obsolete fact about favorite drinks"},
			wantAdmit:  false,
			wantReason: "delete_action",
		},
		{
			name:       "DeleteActionCaseInsensitiveWithWhitespace",
			item:       ExtractedFactItem{Action: "  delete  ", FactText: "Old preference that is no longer active"},
			wantAdmit:  false,
			wantReason: "delete_action",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admit, reason := EvaluateCandidateAdmission(tc.item)
			if admit != tc.wantAdmit {
				t.Errorf("admit = %v, want %v", admit, tc.wantAdmit)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestBuildCandidateReconcilePrompt(t *testing.T) {
	candidate := ExtractedFactItem{
		FactText: "User prefers dark mode across all interfaces",
	}
	neighbors := []ScoredFact{
		{
			Fact: Fact{
				ID:       42,
				FactText: "User prefers light mode on mobile displays",
			},
			Score: 0.76,
		},
	}
	prompt := buildCandidateReconcilePrompt(candidate, neighbors)
	for _, expected := range []string{
		"CANDIDATE FACT:",
		"Fact: User prefers dark mode across all interfaces",
		"EXISTING CANDIDATE NEIGHBORS",
		"ID 42 (similarity 0.76)",
		"User prefers light mode on mobile displays",
		"supersedes_id",
		"delete_id",
		"Never merge distinct topics",
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
			item:         ExtractedFactItem{FactText: "Valid candidate fact text"},
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
			llmResp:     `{"facts":[{"action":"UPDATE","supersedes_id":10,"fact_text":"Updated text here"}]}`,
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
			name:           "Tier3_NOOP",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    tier3Emb,
			llmResp:        `{"facts":[{"action":"NOOP"}]}`,
			wantReinforced: true,
		},
		{
			name:           "Tier3_NOOP_ExplicitSupersedesID",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    tier3Emb,
			llmResp:        `{"facts":[{"action":"NOOP","supersedes_id":10}]}`,
			wantReinforced: true,
		},
		{
			name:           "Tier3_NOOP_ReinforceErrorLogged",
			item:           ExtractedFactItem{FactText: "Valid candidate fact text"},
			neighborID:     10,
			neighborEmb:    tier3Emb,
			llmResp:        `{"facts":[{"action":"NOOP"}]}`,
			storeFailOp:    "ReinforceFact",
			wantReinforced: false,
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
					mem.facts[tc.neighborID] = &Fact{ID: tc.neighborID, FactText: "Existing neighbor text", ReinforceCount: 1}
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
		withTrajectory      bool
		withReinforceIDs    bool
		wantReinforcedID    int64
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
		{
			name:           "TrajectoryRetrievedFactsAndToolCallsInjected",
			withTrajectory: true,
			wantErr:        false,
		},
		{
			name:             "TrajectoryReinforcesFactIDs",
			withReinforceIDs: true,
			wantReinforcedID: 42,
			wantErr:          false,
		},
		{
			name:             "TrajectoryReinforceStoreErrorLoggedGracefully",
			withReinforceIDs: true,
			storeFailOp:      "ReinforceFact",
			wantErr:          false,
		},
		{
			name:        "TrajectoryQueryErrorIgnoredAndContinues",
			storeFailOp: "GetThreadTrajectoryContext",
			wantErr:     false,
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
				if tc.withTrajectory {
					mem.threadRetrievedFacts[threadID] = []string{"- [id:42] User likes matcha"}
					mem.threadToolCalls[threadID] = []string{`memory_search("matcha")`}
				}
				if tc.withReinforceIDs {
					mem.facts[42] = &Fact{ID: 42, FactText: "User likes matcha", ReinforceCount: 1}
					mem.embeddings[42] = baseEmb
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
						{"fact_text":"Candidate fact text number 1"},
						{"fact_text":"Candidate fact text number 2"},
						{"fact_text":"Candidate fact text number 3"},
						{"fact_text":"Candidate fact text number 4"},
						{"fact_text":"Candidate fact text number 5"},
						{"fact_text":"Candidate fact text number 6"},
						{"fact_text":"Candidate fact text number 7"}
					]}`, nil
				}
				if tc.withReinforceIDs {
					return `{"facts":[],"reinforced_ids":[42,42,-1,0]}`, nil
				}
				return `{"facts":[{"fact_text":"Bob likes matcha tea in the afternoon"}]}`, nil
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
			if tc.withTrajectory {
				if !strings.Contains(capturedPrompt, "RETRIEVED MEMORY IN TURN:\n- [id:42] User likes matcha") {
					t.Errorf("captured prompt missing retrieved memory: %s", capturedPrompt)
				}
				if !strings.Contains(capturedPrompt, "TOOL CALLS IN TURN:\nmemory_search(\"matcha\")") {
					t.Errorf("captured prompt missing tool calls: %s", capturedPrompt)
				}
			}
			if tc.wantReinforcedID > 0 && mem != nil {
				if f, ok := mem.facts[tc.wantReinforcedID]; !ok || f.ReinforceCount != 2 {
					t.Errorf("fact %d reinforce count = %v, want 2", tc.wantReinforcedID, f)
				}
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

func TestSQLStore_GetAllFactsWithEmbeddings(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name      string
		nilDB     bool
		queryErr  error
		rowSQL    string
		wantErr   bool
		wantCount int
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "QueryError",
			queryErr: errors.New("query error"),
			wantErr:  true,
		},
		{
			name:    "RowScanError",
			rowSQL:  "SELECT 'invalid' AS id",
			wantErr: true,
		},
		{
			name:      "SuccessEmpty",
			wantCount: 0,
			wantErr:   false,
		},
		{
			name:      "SuccessRow",
			rowSQL:    "SELECT 1, 'Morning run', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, '[0.1,0.2]'",
			wantCount: 1,
			wantErr:   false,
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
			facts, err := store.GetAllFactsWithEmbeddings(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetAllFactsWithEmbeddings() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantCount > 0 && len(facts) != tc.wantCount {
				t.Errorf("len(facts) = %d, want %d", len(facts), tc.wantCount)
			}
		})
	}
}

func TestMemStore_GetAllFactsWithEmbeddings(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		storeFail string
		addFacts  bool
		wantErr   bool
		wantCount int
	}{
		{
			name:      "StoreFail",
			storeFail: "GetAllFactsWithEmbeddings",
			wantErr:   true,
		},
		{
			name:      "EmptyStore",
			addFacts:  false,
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:      "ReturnsOnlyFactsWithEmbeddings",
			addFacts:  true,
			wantErr:   false,
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			if tc.storeFail != "" {
				mem.SetFail(tc.storeFail, errors.New("get fail"))
			}
			if tc.addFacts {
				_, _ = mem.InsertFact(ctx, "Fact with emb 1", testEmbedding(1))
				mem.mu.Lock()
				mem.facts[99] = &Fact{ID: 99, FactText: "Fact without emb"}
				mem.mu.Unlock()
				_, _ = mem.InsertFact(ctx, "Fact with emb 2", testEmbedding(0.5))
			}
			facts, err := mem.GetAllFactsWithEmbeddings(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetAllFactsWithEmbeddings() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantCount > 0 && len(facts) != tc.wantCount {
				t.Errorf("len(facts) = %d, want %d", len(facts), tc.wantCount)
			}
		})
	}
}

func makePairwiseSimilarVectors(n int, targetSim float64) [][]float32 {
	// Component 0 has sqrt(targetSim), each vector has sqrt(1 - targetSim) at component i+1
	s := float32(math.Sqrt(targetSim))
	rem := float32(math.Sqrt(1.0 - targetSim))
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		v := make([]float32, ExpectedEmbeddingDim)
		v[0] = s
		if i+1 < ExpectedEmbeddingDim {
			v[i+1] = rem
		}
		vecs[i] = v
	}
	return vecs
}

func makeScoredFactFixture(id int64, text string, emb []float32) ScoredFactWithEmbedding {
	now := time.Now().UTC()
	return ScoredFactWithEmbedding{
		Fact: Fact{
			ID:               id,
			FactText:         text,
			CreatedAt:        now,
			LastReinforcedAt: now,
			ReinforceCount:   1,
		},
		Embedding: emb,
	}
}

func TestFindConsolidationCliques(t *testing.T) {
	vecs := makePairwiseSimilarVectors(8, 0.78) // Pairwise similarity = 0.78 (in [0.75, 1.0])
	looseVecs := makePairwiseSimilarVectors(3, 0.72)
	orthogonalVec := make([]float32, ExpectedEmbeddingDim)
	orthogonalVec[ExpectedEmbeddingDim-1] = 1.0 // Orthogonal to vecs[0..7]

	vecsB := make([][]float32, 3)
	for i := 0; i < 3; i++ {
		v := make([]float32, ExpectedEmbeddingDim)
		v[10] = float32(math.Sqrt(0.78))
		v[11+i] = float32(math.Sqrt(0.22))
		vecsB[i] = v
	}

	seedVec := make([]float32, ExpectedEmbeddingDim)
	seedVec[0] = 1.0
	borderlineVec := make([]float32, ExpectedEmbeddingDim)
	borderlineVec[0] = 0.80
	borderlineVec[1] = -0.60
	tightVec := make([]float32, ExpectedEmbeddingDim)
	tightVec[0] = 0.95
	tightVec[1] = float32(math.Sqrt(1.0 - 0.95*0.95))

	hijackVec := make([]float32, ExpectedEmbeddingDim)
	hijackVec[0], hijackVec[1] = 0.76, float32(math.Sqrt(1.0-0.76*0.76))
	emailVec1 := make([]float32, ExpectedEmbeddingDim)
	emailVec1[0] = 1.0
	emailVec2 := make([]float32, ExpectedEmbeddingDim)
	emailVec2[0], emailVec2[1] = 0.92, -float32(math.Sqrt(1.0-0.92*0.92))

	tests := []struct {
		name      string
		facts     []ScoredFactWithEmbedding
		minSim    float64
		maxSim    float64
		minSize   int
		maxSize   int
		wantCount int
		wantSizes []int
		wantIDs   []int64
	}{
		{
			name:      "EmptyFacts",
			facts:     nil,
			wantCount: 0,
		},
		{
			name: "FactsWithoutEmbeddingsIgnored",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "Fact 1", nil),
				makeScoredFactFixture(2, "Fact 2", []float32{}),
			},
			wantCount: 0,
		},
		{
			name: "SeparateDisjointCliquesFormed",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
				makeScoredFactFixture(3, "A3", vecs[2]),
				makeScoredFactFixture(4, "B1", vecsB[0]),
				makeScoredFactFixture(5, "B2", vecsB[1]),
				makeScoredFactFixture(6, "B3", vecsB[2]),
			},
			wantCount: 2,
			wantSizes: []int{2, 2},
		},
		{
			name: "MinSizeThresholdEnforced",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
			},
			minSize:   3,
			wantCount: 0,
		},
		{
			name: "MaxSizeCapEnforced",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
				makeScoredFactFixture(3, "A3", vecs[2]),
				makeScoredFactFixture(4, "A4", vecs[3]),
				makeScoredFactFixture(5, "A5", vecs[4]),
				makeScoredFactFixture(6, "A6", vecs[5]),
				makeScoredFactFixture(7, "A7", vecs[6]),
			},
			minSize:   3,
			maxSize:   5,
			wantCount: 1,
			wantSizes: []int{5},
		},
		{
			name: "NonCliqueOrthogonalMemberExcluded",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
				makeScoredFactFixture(3, "A3", vecs[2]),
				makeScoredFactFixture(4, "A4", orthogonalVec),
			},
			minSize:   3,
			maxSize:   6,
			wantCount: 1,
			wantSizes: []int{3},
		},
		{
			name: "DefaultsAppliedForInvalidArgs",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
				makeScoredFactFixture(3, "A3", vecs[2]),
			},
			minSim:    -1,
			maxSim:    -1,
			minSize:   -1,
			maxSize:   -1,
			wantCount: 1,
			wantSizes: []int{2},
		},
		{
			name: "MaxSizeLessThanMinSizeDefaults",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
				makeScoredFactFixture(3, "A3", vecs[2]),
			},
			minSize:   3,
			maxSize:   2,
			wantCount: 1,
			wantSizes: []int{3},
		},
		{
			name: "BelowDefaultMinSim070Excluded",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "L1", looseVecs[0]),
				makeScoredFactFixture(2, "L2", looseVecs[1]),
				makeScoredFactFixture(3, "L3", looseVecs[2]),
			},
			wantCount: 0,
		},
		{
			name: "Size2PairAndHighSimIncludedByDefault",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "P1", tightVec),
				makeScoredFactFixture(2, "P2", tightVec),
			},
			wantCount: 1,
			wantSizes: []int{2},
		},
		{
			name: "SimilaritySortedPrefersTighterCliqueAndRejectsInconsistentMember",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "Seed", seedVec),
				makeScoredFactFixture(2, "BorderlineIncompatibleWithTight", borderlineVec),
				makeScoredFactFixture(3, "Tight1", tightVec),
				makeScoredFactFixture(4, "Tight2", tightVec),
			},
			minSize:   3,
			wantCount: 1,
			wantSizes: []int{3},
		},
		{
			name: "GlobalDescendingEdgePreventsLowIDWeakHijack",
			facts: []ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "WeakLowID", hijackVec),
				makeScoredFactFixture(2, "Email1", emailVec1),
				makeScoredFactFixture(3, "Email2", emailVec2),
			},
			wantCount: 1,
			wantSizes: []int{2},
			wantIDs:   []int64{2, 3},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cliques := FindConsolidationCliques(tc.facts, tc.minSim, tc.maxSim, tc.minSize, tc.maxSize)
			if len(cliques) != tc.wantCount {
				t.Fatalf("FindConsolidationCliques() len = %d, want %d", len(cliques), tc.wantCount)
			}
			for i, size := range tc.wantSizes {
				if len(cliques[i]) != size {
					t.Errorf("clique %d size = %d, want %d", i, len(cliques[i]), size)
				}
			}
			if len(tc.wantIDs) > 0 && len(cliques) > 0 {
				for i, wantID := range tc.wantIDs {
					if cliques[0][i].Fact.ID != wantID {
						t.Errorf("clique[0][%d].Fact.ID = %d, want %d", i, cliques[0][i].Fact.ID, wantID)
					}
				}
			}
		})
	}
}

func TestConsolidateFactClusters(t *testing.T) {
	ctx := context.Background()
	vecs := makePairwiseSimilarVectors(4, 0.78)
	validJSON := `{"facts":[{"category":"pref","fact_text":"User prefers dark mode everywhere","importance_score":0.85}]}`
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name                string
		ctx                 context.Context
		nilStore            bool
		nilOllama           bool
		nilLLM              bool
		storeFail           string
		lockMutex           bool
		addFacts            bool
		seedReinforceCounts []int
		llmResp             string
		llmErr              error
		ollamaErr           error
		wantErr             bool
		wantCount           int
		wantRemaining       int
		wantReinforcedCount int
	}{
		{
			name:     "NilStoreError",
			ctx:      ctx,
			nilStore: true,
			wantErr:  true,
		},
		{
			name:      "NilOllamaError",
			ctx:       ctx,
			nilOllama: true,
			wantErr:   true,
		},
		{
			name:    "NilLLMError",
			ctx:     ctx,
			nilLLM:  true,
			wantErr: true,
		},
		{
			name:      "LockContentionSkipped",
			ctx:       ctx,
			lockMutex: true,
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:      "StoreGetAllFactsError",
			ctx:       ctx,
			storeFail: "GetAllFactsWithEmbeddings",
			wantErr:   true,
		},
		{
			name:      "NoFactsInStore",
			ctx:       ctx,
			addFacts:  false,
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:          "LLMErrorContinuesWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmErr:        errors.New("llm down"),
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "ParseErrorContinuesWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       "not-valid-json",
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "OllamaEmbeddingErrorContinuesWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       validJSON,
			ollamaErr:     errors.New("ollama embed down"),
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "StoreInsertFactErrorContinuesWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       validJSON,
			storeFail:     "InsertFact",
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "SuccessConsolidation",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       validJSON,
			wantErr:       false,
			wantCount:     2,
			wantRemaining: 1, // 3 merged via 2 pairwise passes into 1
		},
		{
			name:          "CandidateFactsWithEmptyTextSkippedAndValidMerged",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       `{"facts":[{"fact_text":"","category":"","importance_score":0},{"fact_text":"Valid synthesized fact 1"}]}`,
			wantErr:       false,
			wantCount:     2,
			wantRemaining: 1,
		},
		{
			name:          "TwoCandidateFactsForPairAbortsMergeWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       `{"facts":[{"fact_text":"Valid synthesized fact 1"},{"fact_text":"Valid synthesized fact 2"},{"fact_text":"Extra third fact capped"}]}`,
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "AllEmptyCandidateFactsSkippedWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       `{"facts":[{"fact_text":"   "}]}`,
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "EmptyFactsArraySkippedWithoutDeletion",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       `{"facts":[]}`,
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
		},
		{
			name:          "StoreDeleteFactErrorLoggedGracefully",
			ctx:           ctx,
			addFacts:      true,
			llmResp:       validJSON,
			storeFail:     "DeleteFact",
			wantErr:       false,
			wantCount:     1,
			wantRemaining: 4, // 3 not deleted + 1 inserted in pass 0 before break
		},
		{
			name:     "ContextCanceled",
			ctx:      canceledCtx,
			addFacts: true,
			llmResp:  validJSON,
			wantErr:  true,
		},
		{
			name:                "ConsolidationInheritsMaxReinforcement",
			ctx:                 ctx,
			addFacts:            true,
			seedReinforceCounts: []int{1, 4, 2},
			llmResp:             validJSON,
			wantErr:             false,
			wantCount:           2,
			wantRemaining:       1,
			wantReinforcedCount: 4,
		},
		{
			name:                "ConsolidationClampsReinforceAt10",
			ctx:                 ctx,
			addFacts:            true,
			seedReinforceCounts: []int{15, 12, 11},
			llmResp:             validJSON,
			wantErr:             false,
			wantCount:           2,
			wantRemaining:       1,
			wantReinforcedCount: 10,
		},
		{
			name:                "ConsolidationReinforceStoreErrorLoggedGracefully",
			ctx:                 ctx,
			addFacts:            true,
			seedReinforceCounts: []int{1, 4, 2},
			storeFail:           "ReinforceFact",
			llmResp:             validJSON,
			wantErr:             false,
			wantCount:           2,
			wantRemaining:       1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			if tc.addFacts {
				_, _ = mem.InsertFact(ctx, "Fact A", vecs[0])
				_, _ = mem.InsertFact(ctx, "Fact B", vecs[1])
				_, _ = mem.InsertFact(ctx, "Fact C", vecs[2])
			}
			if len(tc.seedReinforceCounts) == 3 && len(mem.facts) >= 3 {
				mem.facts[1].ReinforceCount = tc.seedReinforceCounts[0]
				mem.facts[2].ReinforceCount = tc.seedReinforceCounts[1]
				mem.facts[3].ReinforceCount = tc.seedReinforceCounts[2]
			}
			if tc.storeFail != "" {
				mem.SetFail(tc.storeFail, errors.New("store failed"))
			}

			var store Store = mem
			if tc.nilStore {
				store = nil
			}
			var client OllamaClient = &MockOllamaClient{embedding: vecs[0], err: tc.ollamaErr}
			if tc.nilOllama {
				client = nil
			}
			var llm LLMClientFunc = func(c context.Context, p string) (string, error) {
				if tc.llmErr != nil {
					return "", tc.llmErr
				}
				return tc.llmResp, nil
			}
			if tc.nilLLM {
				llm = nil
			}

			if tc.lockMutex {
				extractionMutex.Lock()
				defer extractionMutex.Unlock()
			}

			count, err := ConsolidateFactClusters(tc.ctx, store, client, llm)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ConsolidateFactClusters() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if count != tc.wantCount {
				t.Errorf("count = %d, want %d", count, tc.wantCount)
			}
			if tc.wantRemaining > 0 && len(mem.facts) != tc.wantRemaining {
				t.Errorf("len(mem.facts) = %d, want %d", len(mem.facts), tc.wantRemaining)
			}
			if tc.wantReinforcedCount > 0 {
				for _, f := range mem.facts {
					if f.ReinforceCount != tc.wantReinforcedCount {
						t.Errorf("consolidated fact reinforce_count = %d, want %d", f.ReinforceCount, tc.wantReinforcedCount)
					}
				}
			}
		})
	}
}

func TestToolHandler_MemoryStore_AdmissionGate(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		args       MemoryStoreArgs
		wantErr    bool
		wantAction string
	}{
		{
			name:    "TooShortFactRejected",
			args:    MemoryStoreArgs{FactText: "short"},
			wantErr: true,
		},
		{
			name:    "EphemeralTelemetryRejected",
			args:    MemoryStoreArgs{FactText: "PR #100 was merged with auto-merge armed"},
			wantErr: true,
		},
		{
			name:       "DurableTelemetryAdmitted",
			args:       MemoryStoreArgs{FactText: "Auto-merge armed must always remain active for all PRs"},
			wantErr:    false,
			wantAction: "inserted",
		},
		{
			name:    "SupersedesBypassesAdmissionGate",
			args:    MemoryStoreArgs{FactText: "short", SupersedesID: 999},
			wantErr: true, // Fails because ID 999 doesn't exist, NOT admission gate
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			h := NewToolHandler(&Config{}, mem, &MockOllamaClient{embedding: testEmbedding(1.0)})
			out, err := h.MemoryStore(ctx, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemoryStore() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantAction != "" && out.Action != tc.wantAction {
				t.Errorf("out.Action = %s, want %s", out.Action, tc.wantAction)
			}
		})
	}
}

func TestWorker_RunDaily_Dreaming(t *testing.T) {
	ctx := context.Background()
	vecs := makePairwiseSimilarVectors(3, 0.78)

	tests := []struct {
		name         string
		nilOllama    bool
		nilLLM       bool
		llmErr       error
		dreamSuccess bool
	}{
		{
			name:         "DailyDecayAndDreamingSuccess",
			dreamSuccess: true,
		},
		{
			name:   "DailyDecayWithDreamingErrorHandled",
			llmErr: errors.New("dreaming llm error"),
		},
		{
			name:      "NilOllamaSkipsDreaming",
			nilOllama: true,
		},
		{
			name:   "NilLLMSkipsDreaming",
			nilLLM: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			_, _ = mem.InsertFact(ctx, "Fact 1", vecs[0])
			_, _ = mem.InsertFact(ctx, "Fact 2", vecs[1])
			_, _ = mem.InsertFact(ctx, "Fact 3", vecs[2])

			var ollama OllamaClient = &MockOllamaClient{embedding: vecs[0]}
			if tc.nilOllama {
				ollama = nil
			}
			var llm LLMClientFunc = func(c context.Context, p string) (string, error) {
				if tc.llmErr != nil {
					return "", tc.llmErr
				}
				return `{"facts":[{"fact_text":"Consolidated fact"}]}`, nil
			}
			if tc.nilLLM {
				llm = nil
			}

			w := NewWorker(WorkerConfig{
				Store:     mem,
				Ollama:    ollama,
				LLMClient: llm,
			})

			// Tick 60 is daily decay + dreaming
			w.Tick(ctx, 60)

			if tc.dreamSuccess {
				if len(mem.facts) != 1 {
					t.Errorf("len(mem.facts) = %d, want 1 consolidated fact", len(mem.facts))
				}
			}
		})
	}
}

func TestExtractLoadedSkillName(t *testing.T) {
	tests := []struct {
		name     string
		stepType string
		toolName string
		content  string
		want     string
	}{
		{
			name:     "EmptyContent",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  "",
			want:     "",
		},
		{
			name:     "PlannerResponseDirectPath",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `{"Path": "/skills/ha-operations/SKILL.md"}`,
			want:     "ha-operations",
		},
		{
			name:     "PlannerResponseDiscordSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/discord/ha-operations/SKILL.md`,
			want:     "ha-operations",
		},
		{
			name:     "PlannerResponseCommonSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/common/culinary-recipes/SKILL.md`,
			want:     "culinary-recipes",
		},
		{
			name:     "PlannerResponseVoiceSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/voice/voice-assistant/SKILL.md`,
			want:     "voice-assistant",
		},
		{
			name:     "PlannerResponseEphemeralSubdir",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `/skills/ephemeral/test-skill/SKILL.md`,
			want:     "test-skill",
		},
		{
			name:     "PlannerResponseWindowsBackslashes",
			stepType: "PLANNER_RESPONSE",
			toolName: "read_file",
			content:  `C:\data\skills\discord\ha-operations\SKILL.md`,
			want:     "ha-operations",
		},
		{
			name:     "GenericStepYAML",
			stepType: "GENERIC",
			toolName: "",
			content:  "---\nname: ha-operations\ndescription: Manage HA",
			want:     "ha-operations",
		},
		{
			name:     "ToolStepYAMLNumbered",
			stepType: "TOOL",
			toolName: "read_file",
			content:  "1: ---\n2: name: culinary-recipes\n3: description: Cook",
			want:     "culinary-recipes",
		},
		{
			name:     "RunCommandStepYAML",
			stepType: "RUN_COMMAND",
			toolName: "bash",
			content:  "---\nname: my-script-skill\n",
			want:     "my-script-skill",
		},
		{
			name:     "NoSkillFound",
			stepType: "PLANNER_RESPONSE",
			toolName: "run_command",
			content:  "echo hello",
			want:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractLoadedSkillName(tc.stepType, tc.toolName, tc.content)
			if got != tc.want {
				t.Errorf("ExtractLoadedSkillName() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractLoadedSkillsFromCalls(t *testing.T) {
	tests := []struct {
		name      string
		toolCalls []string
		want      []string
	}{
		{
			name:      "Empty",
			toolCalls: nil,
			want:      nil,
		},
		{
			name: "SingleSkill",
			toolCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
			},
			want: []string{"ha-operations"},
		},
		{
			name: "DeduplicatedMultipleCalls",
			toolCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
				"[SKILL_LOADED: ha-operations]",
				"-> OUTPUT(read_file): done",
			},
			want: []string{"ha-operations"},
		},
		{
			name: "MultipleDifferentSkills",
			toolCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
				"[SKILL_LOADED: culinary-recipes]",
			},
			want: []string{"ha-operations", "culinary-recipes"},
		},
		{
			name: "NoLoadedSkills",
			toolCalls: []string{
				"run_command(echo hi)",
				"-> OUTPUT(run_command): hi",
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractLoadedSkillsFromCalls(tc.toolCalls)
			if len(got) != len(tc.want) {
				t.Fatalf("ExtractLoadedSkillsFromCalls() len = %d, want %d (%v)", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("ExtractLoadedSkillsFromCalls()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestEvaluateProceduralCandidateAdmission(t *testing.T) {
	tests := []struct {
		name       string
		item       ExtractedProceduralItem
		wantAdmit  bool
		wantReason string
	}{
		{
			name: "AdmittedWithStepsAndAvoid",
			item: ExtractedProceduralItem{
				Trigger: "restart nomad service when stuck",
				Steps:   []string{"check status", "restart allocation"},
				Avoid:   []string{"do not kill without checking status"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "AdmittedWithStepsOnly",
			item: ExtractedProceduralItem{
				Trigger: "configure home assistant entity",
				Steps:   []string{"call ha_search", "apply entity change"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "AdmittedWithAvoidOnly",
			item: ExtractedProceduralItem{
				Trigger: "deploy docker container on host",
				Avoid:   []string{"do not use host networking mode"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "EmptyTrigger",
			item: ExtractedProceduralItem{
				Trigger: "   ",
				Steps:   []string{"step 1"},
			},
			wantAdmit:  false,
			wantReason: "empty_trigger",
		},
		{
			name: "TriggerTooShort",
			item: ExtractedProceduralItem{
				Trigger: "run",
				Steps:   []string{"step 1"},
			},
			wantAdmit:  false,
			wantReason: "trigger_too_short",
		},
		{
			name: "EphemeralTelemetryPR",
			item: ExtractedProceduralItem{
				Trigger: "verify changes for PR #1234 before deploying",
				Steps:   []string{"run tests"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "EphemeralTelemetryBuildID",
			item: ExtractedProceduralItem{
				Trigger: "execute build-1792688043-932febae in pipeline",
				Steps:   []string{"run tests"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "EphemeralTelemetryDurableOverride",
			item: ExtractedProceduralItem{
				Trigger: "always verify pull request #1234 invariant strictly before commit",
				Steps:   []string{"run verify.sh"},
			},
			wantAdmit:  true,
			wantReason: "admitted",
		},
		{
			name: "EphemeralTelemetryInSteps",
			item: ExtractedProceduralItem{
				Trigger: "reconcile repository branch state",
				Steps:   []string{"check status of PR #1234 before merging"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "EphemeralTelemetryInAvoid",
			item: ExtractedProceduralItem{
				Trigger: "deploy service allocation on cluster",
				Steps:   []string{"run nomad job"},
				Avoid:   []string{"wait for build-1792688043-932febae to finish"},
			},
			wantAdmit:  false,
			wantReason: "ephemeral_telemetry",
		},
		{
			name: "MissingStepsAndAvoid",
			item: ExtractedProceduralItem{
				Trigger: "some valid trigger description",
				Steps:   nil,
				Avoid:   nil,
			},
			wantAdmit:  false,
			wantReason: "missing_steps_and_avoid",
		},
		{
			name: "WhitespaceOnlyStepsAndAvoid",
			item: ExtractedProceduralItem{
				Trigger: "some valid trigger description",
				Steps:   []string{"   ", ""},
				Avoid:   []string{"   "},
			},
			wantAdmit:  false,
			wantReason: "missing_steps_and_avoid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admit, reason := EvaluateProceduralCandidateAdmission(tc.item)
			if admit != tc.wantAdmit {
				t.Errorf("admit = %v, want %v (reason: %q)", admit, tc.wantAdmit, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestSQLStore_ProceduralCandidates(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name        string
		action      string
		nilDB       bool
		cand        ProceduralCandidate
		ids         []int64
		prNum       int
		queryErr    error
		execErr     error
		rowSQL      string
		rowsSQL     string
		wantErr     bool
		wantCount   int
		wantID      int64
		wantNoQuery bool
	}{
		{
			name:    "InsertNilDB",
			action:  "insert",
			nilDB:   true,
			cand:    ProceduralCandidate{Trigger: "valid trigger"},
			wantErr: true,
		},
		{
			name:    "InsertEmptyTrigger",
			action:  "insert",
			cand:    ProceduralCandidate{Trigger: "   "},
			wantErr: true,
		},
		{
			name:     "InsertQueryError",
			action:   "insert",
			cand:     ProceduralCandidate{Trigger: "valid trigger"},
			queryErr: errors.New("insert failed"),
			wantErr:  true,
		},
		{
			name:   "InsertSuccessWithPR",
			action: "insert",
			cand: ProceduralCandidate{
				ThreadID:    "t-100",
				Domain:      "discord",
				LoadedSkill: "ha-operations",
				Trigger:     "turn on porch light when motion detected",
				Steps:       []string{"call ha_search", "call ha_call_write_tool"},
				Avoid:       []string{"avoid loop"},
				Embedding:   testEmbedding(0.5),
				Status:      "pending",
				PRNumber:    42,
			},
			rowSQL:  "SELECT 101",
			wantID:  101,
			wantErr: false,
		},
		{
			name:   "InsertSuccessDefaults",
			action: "insert",
			cand: ProceduralCandidate{
				Trigger:   "valid trigger pattern",
				Embedding: testEmbedding(0.1),
			},
			rowSQL:  "SELECT 102",
			wantID:  102,
			wantErr: false,
		},
		{
			name:    "GetPendingNilDB",
			action:  "get_pending",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "GetPendingQueryError",
			action:   "get_pending",
			queryErr: errors.New("select failed"),
			wantErr:  true,
		},
		{
			name:    "GetPendingScanError",
			action:  "get_pending",
			rowsSQL: "SELECT 'bad_row'",
			wantErr: true,
		},
		{
			name:   "GetPendingSuccessParsing",
			action: "get_pending",
			rowsSQL: "SELECT 1, 't-1', 'discord', 'ha-operations', 'motion detected', '[\"step 1\"]', '[\"avoid 1\"]', '[0.1,0.2]', 'pending', 0, '2026-10-01 12:00:00' " +
				"UNION ALL SELECT 2, 't-2', 'discord', '', 'unloaded skill trigger', '', '', '[0.3,0.4]', 'pending', 5, '2026-10-01 13:00:00'",
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:    "MarkConsolidatedNilDB",
			action:  "mark_consolidated",
			nilDB:   true,
			ids:     []int64{1},
			prNum:   10,
			wantErr: true,
		},
		{
			name:        "MarkConsolidatedEmptyIDs",
			action:      "mark_consolidated",
			ids:         nil,
			prNum:       10,
			wantErr:     false,
			wantNoQuery: true,
		},
		{
			name:    "MarkConsolidatedExecError",
			action:  "mark_consolidated",
			ids:     []int64{1, 2},
			prNum:   10,
			execErr: errors.New("update failed"),
			wantErr: true,
		},
		{
			name:    "MarkConsolidatedSuccess",
			action:  "mark_consolidated",
			ids:     []int64{1, 2},
			prNum:   10,
			wantErr: false,
		},
		{
			name:    "MarkSkippedNilDB",
			action:  "mark_skipped",
			nilDB:   true,
			ids:     []int64{1},
			wantErr: true,
		},
		{
			name:        "MarkSkippedEmptyIDs",
			action:      "mark_skipped",
			ids:         nil,
			wantErr:     false,
			wantNoQuery: true,
		},
		{
			name:    "MarkSkippedExecError",
			action:  "mark_skipped",
			ids:     []int64{3},
			execErr: errors.New("update failed"),
			wantErr: true,
		},
		{
			name:    "MarkSkippedSuccess",
			action:  "mark_skipped",
			ids:     []int64{3, 4},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedQuery string
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:         dbMem,
					queryErr:      tc.queryErr,
					execErr:       tc.execErr,
					rowSQL:        tc.rowSQL,
					rowsSQL:       tc.rowsSQL,
					capturedQuery: &capturedQuery,
				})
			}

			switch tc.action {
			case "insert":
				id, err := store.InsertProceduralCandidate(ctx, tc.cand)
				if (err != nil) != tc.wantErr {
					t.Fatalf("InsertProceduralCandidate() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantID > 0 && id != tc.wantID {
					t.Errorf("InsertProceduralCandidate() id = %d, want %d", id, tc.wantID)
				}
			case "get_pending":
				cands, err := store.GetPendingProceduralCandidates(ctx)
				if (err != nil) != tc.wantErr {
					t.Fatalf("GetPendingProceduralCandidates() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if len(cands) != tc.wantCount {
					t.Errorf("GetPendingProceduralCandidates() count = %d, want %d", len(cands), tc.wantCount)
				}
			case "mark_consolidated":
				err := store.MarkProceduralCandidatesConsolidated(ctx, tc.ids, tc.prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("MarkProceduralCandidatesConsolidated() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantNoQuery && capturedQuery != "" {
					t.Errorf("expected no query for empty IDs, got: %s", capturedQuery)
				}
			case "mark_skipped":
				err := store.MarkProceduralCandidatesSkipped(ctx, tc.ids)
				if (err != nil) != tc.wantErr {
					t.Fatalf("MarkProceduralCandidatesSkipped() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantNoQuery && capturedQuery != "" {
					t.Errorf("expected no query for empty IDs, got: %s", capturedQuery)
				}
			}
		})
	}
}

func TestTestMemStore_ProceduralCandidates(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		failOp     string
		failErr    error
		cand       ProceduralCandidate
		markPR     int
		wantErr    bool
		wantCount  int
		wantStatus string
	}{
		{
			name:    "InsertFail",
			failOp:  "InsertProceduralCandidate",
			failErr: errors.New("insert fail"),
			cand:    ProceduralCandidate{Trigger: "valid"},
			wantErr: true,
		},
		{
			name:    "InsertEmptyTrigger",
			cand:    ProceduralCandidate{Trigger: ""},
			wantErr: true,
		},
		{
			name:       "InsertSuccessAndMarkConsolidated",
			cand:       ProceduralCandidate{Trigger: "valid trigger", Steps: []string{"step 1"}},
			markPR:     42,
			wantErr:    false,
			wantStatus: "consolidated",
		},
		{
			name:       "MarkSkipped",
			cand:       ProceduralCandidate{Trigger: "valid trigger 2"},
			wantErr:    false,
			wantStatus: "skipped",
		},
		{
			name:    "GetPendingFail",
			failOp:  "GetPendingProceduralCandidates",
			failErr: errors.New("get pending fail"),
			wantErr: true,
		},
		{
			name:    "MarkConsolidatedFail",
			failOp:  "MarkProceduralCandidatesConsolidated",
			failErr: errors.New("consolidate fail"),
			wantErr: true,
		},
		{
			name:    "MarkSkippedFail",
			failOp:  "MarkProceduralCandidatesSkipped",
			failErr: errors.New("skip fail"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			if tc.failOp != "" {
				mem.SetFail(tc.failOp, tc.failErr)
			}
			if tc.name == "GetPendingFail" {
				_, err := mem.GetPendingProceduralCandidates(ctx)
				if (err != nil) != tc.wantErr {
					t.Fatalf("GetPendingProceduralCandidates() err = %v", err)
				}
				return
			}
			if tc.name == "MarkConsolidatedFail" {
				err := mem.MarkProceduralCandidatesConsolidated(ctx, []int64{1}, 10)
				if (err != nil) != tc.wantErr {
					t.Fatalf("MarkProceduralCandidatesConsolidated() err = %v", err)
				}
				return
			}
			if tc.name == "MarkSkippedFail" {
				err := mem.MarkProceduralCandidatesSkipped(ctx, []int64{1})
				if (err != nil) != tc.wantErr {
					t.Fatalf("MarkProceduralCandidatesSkipped() err = %v", err)
				}
				return
			}

			id, err := mem.InsertProceduralCandidate(ctx, tc.cand)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InsertProceduralCandidate() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			if tc.wantStatus == "consolidated" {
				if err := mem.MarkProceduralCandidatesConsolidated(ctx, []int64{id}, tc.markPR); err != nil {
					t.Fatalf("MarkProceduralCandidatesConsolidated() err = %v", err)
				}
				if mem.proceduralCandidates[id].Status != "consolidated" || mem.proceduralCandidates[id].PRNumber != tc.markPR {
					t.Errorf("candidate state = %v", mem.proceduralCandidates[id])
				}
			} else if tc.wantStatus == "skipped" {
				if err := mem.MarkProceduralCandidatesSkipped(ctx, []int64{id}); err != nil {
					t.Fatalf("MarkProceduralCandidatesSkipped() err = %v", err)
				}
				if mem.proceduralCandidates[id].Status != "skipped" {
					t.Errorf("candidate status = %q, want skipped", mem.proceduralCandidates[id].Status)
				}
			}

			pending, err := mem.GetPendingProceduralCandidates(ctx)
			if err != nil {
				t.Fatalf("GetPendingProceduralCandidates() err = %v", err)
			}
			if tc.wantStatus != "" && len(pending) != 0 {
				t.Errorf("len(pending) = %d, want 0 after mark", len(pending))
			}
		})
	}
}

func TestFormatProceduralCandidateEmbeddingText(t *testing.T) {
	mk := func(trig string, steps, avoid []string) ExtractedProceduralItem {
		return ExtractedProceduralItem{Trigger: trig, Steps: steps, Avoid: avoid}
	}
	tests := []struct {
		name string
		item ExtractedProceduralItem
		want string
	}{
		{"StepsAndAvoid", mk("find", []string{" include -maxdepth ", "", "target subdir"}, []string{" unconstrained find ", " "}), "Steps: include -maxdepth; target subdir\nAvoid: unconstrained find"},
		{"StepsOnly", mk("find", []string{"include -maxdepth"}, nil), "Steps: include -maxdepth"},
		{"AvoidOnly", mk("find", nil, []string{"unconstrained find"}), "Avoid: unconstrained find"},
		{"FallbackTrigger", mk("  when running find  ", nil, nil), "when running find"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatProceduralCandidateEmbeddingText(tc.item); got != tc.want {
				t.Errorf("FormatProceduralCandidateEmbeddingText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProcessThreadFacts_ProceduralCandidates(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                 string
		llmResp              string
		storeFailOp          string
		storeFailErr         error
		ollamaErr            error
		preexistingCandidate *ProceduralCandidate
		trajectoryCalls      []string
		wantCandidateCount   int
		wantPromptContains   string
		wantEmbeddedText     string
		wantErr              bool
	}{
		{
			name: "SuccessAdmitCandidate",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"domain": "discord",
						"loaded_skill": "ha-operations",
						"trigger": "turning on bedroom light when dark",
						"steps": ["check sensor", "call ha_write_tool"],
						"avoid": ["avoid loop"]
					}
				]
			}`,
			wantCandidateCount: 1,
			wantEmbeddedText:   "Steps: check sensor; call ha_write_tool\nAvoid: avoid loop",
		},
		{
			name: "RejectCandidateAdmissionEmptyTrigger",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"trigger": "   ",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 0,
		},
		{
			name: "RejectCandidateAdmissionEphemeralTelemetry",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"trigger": "deploying PR #999 to production",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 0,
		},
		{
			name: "SkipNearDuplicateSameLoadedSkill",
			preexistingCandidate: &ProceduralCandidate{
				LoadedSkill: "ha-operations",
				Trigger:     "turning on bedroom light when dark",
				Embedding:   testEmbedding(1.0),
				Status:      "pending",
			},
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"loaded_skill": "ha-operations",
						"trigger": "turning on bedroom light when dark",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 1,
		},
		{
			name: "DifferentLoadedSkillAdmitted",
			preexistingCandidate: &ProceduralCandidate{
				LoadedSkill: "other-skill",
				Trigger:     "turning on bedroom light when dark",
				Embedding:   testEmbedding(1.0),
				Status:      "pending",
			},
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{
						"loaded_skill": "ha-operations",
						"trigger": "turning on bedroom light when dark",
						"steps": ["step 1"]
					}
				]
			}`,
			wantCandidateCount: 2,
		},
		{
			name: "CapAtThreeCandidates",
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"loaded_skill": "skill-1", "trigger": "procedural task step number 1", "steps": ["s1"]},
					{"loaded_skill": "skill-2", "trigger": "procedural task step number 2", "steps": ["s2"]},
					{"loaded_skill": "skill-3", "trigger": "procedural task step number 3", "steps": ["s3"]},
					{"loaded_skill": "skill-4", "trigger": "procedural task step number 4", "steps": ["s4"]},
					{"loaded_skill": "skill-5", "trigger": "procedural task step number 5", "steps": ["s5"]}
				]
			}`,
			wantCandidateCount: 3,
		},
		{
			name:      "EmbeddingFailureContinues",
			ollamaErr: errors.New("ollama down"),
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"trigger": "valid procedural task step", "steps": ["s1"]}
				]
			}`,
			wantCandidateCount: 0,
		},
		{
			name:         "StoreGetPendingErrorContinues",
			storeFailOp:  "GetPendingProceduralCandidates",
			storeFailErr: errors.New("db read failure"),
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"trigger": "valid procedural task step", "steps": ["s1"]}
				]
			}`,
			wantCandidateCount: 1,
		},
		{
			name:         "StoreInsertErrorContinues",
			storeFailOp:  "InsertProceduralCandidate",
			storeFailErr: errors.New("db insert failure"),
			llmResp: `{
				"facts": [],
				"procedural_candidates": [
					{"trigger": "valid procedural task step", "steps": ["s1"]}
				]
			}`,
			wantCandidateCount: 0,
		},
		{
			name: "LoadedSkillsInPrompt",
			trajectoryCalls: []string{
				"read_file(...) [SKILL_LOADED: ha-operations]",
			},
			llmResp:            `{"facts": []}`,
			wantPromptContains: "LOADED SKILLS IN TURN:\n- ha-operations",
			wantCandidateCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := newTestMemStore()
			mem.unextractedTurns["th-proc"] = []Message{
				{ID: "m1", RowID: 1, Content: "user prompt", Status: "PENDING"},
			}
			if len(tc.trajectoryCalls) > 0 {
				mem.threadToolCalls["th-proc"] = tc.trajectoryCalls
			}
			if tc.preexistingCandidate != nil {
				_, _ = mem.InsertProceduralCandidate(ctx, *tc.preexistingCandidate)
			}
			if tc.storeFailOp != "" {
				mem.SetFail(tc.storeFailOp, tc.storeFailErr)
			}

			client := &MockOllamaClient{
				embedding: testEmbedding(1.0),
				err:       tc.ollamaErr,
			}

			var capturedPrompt string
			llm := func(_ context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				return tc.llmResp, nil
			}

			err := processThreadFacts(ctx, mem, client, llm, "th-proc")
			if (err != nil) != tc.wantErr {
				t.Fatalf("processThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantPromptContains != "" && !strings.Contains(capturedPrompt, tc.wantPromptContains) {
				t.Errorf("prompt missing %q:\n%s", tc.wantPromptContains, capturedPrompt)
			}
			if len(mem.proceduralCandidates) != tc.wantCandidateCount {
				t.Errorf("candidate count = %d, want %d", len(mem.proceduralCandidates), tc.wantCandidateCount)
			}
			if tc.wantEmbeddedText != "" && client.lastText != tc.wantEmbeddedText {
				t.Errorf("embedded text = %q, want %q", client.lastText, tc.wantEmbeddedText)
			}
		})
	}
}

func testProcCand(id int64, skill, trigger string, avoid []string, emb []float32) ProceduralCandidate {
	return ProceduralCandidate{
		ID:          id,
		Domain:      "discord",
		LoadedSkill: skill,
		Trigger:     trigger,
		Steps:       []string{"step 1", "step 2", "step 3"},
		Avoid:       avoid,
		Embedding:   emb,
		Status:      "pending",
	}
}

func testDiskSkill(name, domain, relPath, content string, emb []float32) DiskSkill {
	return DiskSkill{
		Name:        name,
		Domain:      domain,
		RelPath:     relPath,
		Description: "A test description for " + name,
		RawContent:  content,
		Embedding:   emb,
	}
}

func testEmbedding2D(x, y float32) []float32 {
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = x
	emb[1] = y
	return emb
}

func TestParseSkillFrontmatter(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantName    string
		wantDesc    string
		wantBody    string
		wantErr     bool
		errContains string
	}{
		{
			name: "ValidStandard",
			input: `---
name: my-skill
description: This is a standard valid description.
---
# Content Header
Body content text here.`,
			wantName: "my-skill",
			wantDesc: "This is a standard valid description.",
			wantBody: "# Content Header\nBody content text here.",
		},
		{
			name:     "ValidOuterCodeFencesMarkdown",
			input:    "```markdown\n---\nname: fenced-skill\ndescription: Fenced markdown description here.\n---\n# Fenced Content\n```",
			wantName: "fenced-skill",
			wantDesc: "Fenced markdown description here.",
			wantBody: "# Fenced Content",
		},
		{
			name:     "ValidOuterCodeFencesPlain",
			input:    "```\n---\nname: plain-fence\ndescription: Plain fenced description here.\n---\nBody text\n```",
			wantName: "plain-fence",
			wantDesc: "Plain fenced description here.",
			wantBody: "Body text",
		},
		{
			name:     "ValidCRLF",
			input:    "---\r\nname: crlf-skill\r\ndescription: CRLF description text here.\r\n---\r\nCRLF body text",
			wantName: "crlf-skill",
			wantDesc: "CRLF description text here.",
			wantBody: "CRLF body text",
		},
		{
			name:     "ValidEmptyBody",
			input:    "---\nname: no-body\ndescription: Description without any markdown body.\n---",
			wantName: "no-body",
			wantDesc: "Description without any markdown body.",
			wantBody: "",
		},
		{
			name:        "ErrMissingOpeningDelimiter",
			input:       "name: no-delims\ndescription: something",
			wantErr:     true,
			errContains: "missing frontmatter opening delimiter",
		},
		{
			name:        "ErrInvalidOpeningDelimiter",
			input:       "---invalid opening\nname: bad\n---\n",
			wantErr:     true,
			errContains: "invalid frontmatter opening delimiter",
		},
		{
			name:        "ErrMissingClosingDelimiter",
			input:       "---\nname: unclosed\ndescription: missing closing delimiter\n",
			wantErr:     true,
			errContains: "missing frontmatter closing delimiter",
		},
		{
			name:        "ErrInvalidYAML",
			input:       "---\n: bad : yaml :\n---\nbody",
			wantErr:     true,
			errContains: "invalid frontmatter yaml",
		},
		{
			name:        "ErrEmptyName",
			input:       "---\nname: \"   \"\ndescription: valid description text\n---\nbody",
			wantErr:     true,
			errContains: "frontmatter name and description must be non-empty",
		},
		{
			name:        "ErrEmptyDescription",
			input:       "---\nname: valid-name\ndescription: \"\"\n---\nbody",
			wantErr:     true,
			errContains: "frontmatter name and description must be non-empty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotName, gotDesc, gotBody, err := ParseSkillFrontmatter(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseSkillFrontmatter() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if gotName != tc.wantName {
				t.Errorf("name = %q, want %q", gotName, tc.wantName)
			}
			if gotDesc != tc.wantDesc {
				t.Errorf("desc = %q, want %q", gotDesc, tc.wantDesc)
			}
			if gotBody != tc.wantBody {
				t.Errorf("body = %q, want %q", gotBody, tc.wantBody)
			}
		})
	}
}

func TestScanCustomSkills(t *testing.T) {
	validSkillMD := `---
name: sample-skill
description: This is a sample description for testing scanner.
---
# Sample Skill Instructions
`
	corruptSkillMD := `---
invalid yaml without name or closing
`
	commonMD := strings.Replace(validSkillMD, "sample-skill", "sample-common", 1)
	discordMD := strings.Replace(validSkillMD, "sample-skill", "sample-discord", 1)
	nilEmbMD := strings.Replace(validSkillMD, "sample-skill", "nil-emb", 1)
	errEmbMD := strings.Replace(validSkillMD, "sample-skill", "err-emb", 1)

	type diskEntry struct {
		relPath string
		content string
		isDir   bool
	}

	tests := []struct {
		name            string
		nonExistentRoot bool
		entries         []diskEntry
		mockClient      *MockOllamaClient
		wantSkillLen    int
		wantRelPaths    []string
		wantEmbsNil     bool
		wantErr         bool
		errContains     string
	}{
		{
			name: "ValidSkillsScannedBothDomains",
			entries: []diskEntry{
				{relPath: "common/sample-common/SKILL.md", content: commonMD},
				{relPath: "discord/sample-discord/SKILL.md", content: discordMD},
			},
			mockClient:   &MockOllamaClient{embedding: testEmbedding(1.0)},
			wantSkillLen: 2,
			wantRelPaths: []string{
				"custom-skills/common/sample-common/SKILL.md",
				"custom-skills/discord/sample-discord/SKILL.md",
			},
		},
		{
			name:            "NonExistentDirTreatedAsEmptyWithoutError",
			nonExistentRoot: true,
			wantSkillLen:    0,
		},
		{
			name: "IgnoredNonDirectoryEntriesAndMissingSkillMD",
			entries: []diskEntry{
				{relPath: "common/README.md", content: "not a dir"},
				{relPath: "common/empty-dir", isDir: true},
			},
			wantSkillLen: 0,
		},
		{
			name: "NilOllamaClientLeavesEmbeddingNil",
			entries: []diskEntry{
				{relPath: "discord/nil-emb/SKILL.md", content: nilEmbMD},
			},
			wantSkillLen: 1,
			wantEmbsNil:  true,
		},
		{
			name: "CorruptSkillFileReturnsError",
			entries: []diskEntry{
				{relPath: "discord/bad-skill/SKILL.md", content: corruptSkillMD},
			},
			wantErr:     true,
			errContains: "failed to parse frontmatter",
		},
		{
			name: "OllamaEmbeddingErrorReturnsError",
			entries: []diskEntry{
				{relPath: "discord/err-emb/SKILL.md", content: errEmbMD},
			},
			mockClient:  &MockOllamaClient{err: errors.New("ollama failure")},
			wantErr:     true,
			errContains: "failed to generate embedding",
		},
		{
			name: "ReadDirDomainError",
			entries: []diskEntry{
				{relPath: "common", content: "not a dir"},
			},
			wantErr:     true,
			errContains: "failed to read custom skills domain",
		},
		{
			name: "ReadFileSkillError",
			entries: []diskEntry{
				{relPath: "discord/dir-skill/SKILL.md", isDir: true},
			},
			wantErr:     true,
			errContains: "failed to read skill file",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rootDir := filepath.Join(t.TempDir(), "nonexistent")
			if !tc.nonExistentRoot {
				rootDir = t.TempDir()
				for _, entry := range tc.entries {
					fullPath := filepath.Join(rootDir, entry.relPath)
					if entry.isDir {
						if err := os.MkdirAll(fullPath, 0o755); err != nil {
							t.Fatalf("failed to create dir %s: %v", fullPath, err)
						}
					} else {
						if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
							t.Fatalf("failed to create parent dir for %s: %v", fullPath, err)
						}
						if err := os.WriteFile(fullPath, []byte(entry.content), 0o644); err != nil {
							t.Fatalf("failed to write test file %s: %v", fullPath, err)
						}
					}
				}
			}

			var client OllamaClient
			if tc.mockClient != nil {
				client = tc.mockClient
			}

			skills, err := ScanCustomSkills(context.Background(), rootDir, client)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ScanCustomSkills() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if len(skills) != tc.wantSkillLen {
				t.Fatalf("skills len = %d, want %d", len(skills), tc.wantSkillLen)
			}
			for i, wantRel := range tc.wantRelPaths {
				if skills[i].RelPath != wantRel {
					t.Errorf("skill[%d].RelPath = %q, want %q", i, skills[i].RelPath, wantRel)
				}
			}
			if tc.wantEmbsNil {
				for i, s := range skills {
					if len(s.Embedding) != 0 {
						t.Errorf("skill[%d] embedding is not nil", i)
					}
				}
			}
		})
	}
}

func TestPlanSkillConsolidation(t *testing.T) {
	skillA := testDiskSkill("ha-ops", "discord", "custom-skills/discord/ha-ops/SKILL.md", "content", testEmbedding2D(1.0, 0.0))
	skillB := testDiskSkill("deploy-flow", "common", "custom-skills/common/deploy-flow/SKILL.md", "content", testEmbedding2D(0.0, 1.0))
	diskSkills := []DiskSkill{skillA, skillB}

	tests := []struct {
		name          string
		pending       []ProceduralCandidate
		diskSkills    []DiskSkill
		wantBatches   int
		wantActions   []string
		wantDomains   []string
		wantCandCount []int
		wantSkipped   []int64
	}{
		{
			name: "BlockedCoreSkillsSkipped",
			pending: []ProceduralCandidate{
				testProcCand(1, "self-improvement", "trigger 1", nil, nil),
				testProcCand(2, "writing-plans", "trigger 2", nil, nil),
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
			wantSkipped: []int64{1, 2},
		},
		{
			name: "CaseA_LoadedSkillMatchTwoCandidates",
			pending: []ProceduralCandidate{
				testProcCand(10, "ha-ops", "trigger 1", nil, nil),
				testProcCand(11, "ha-ops", "trigger 2", nil, nil),
			},
			diskSkills:    diskSkills,
			wantBatches:   1,
			wantActions:   []string{"EDIT"},
			wantDomains:   []string{"discord"},
			wantCandCount: []int{2},
		},
		{
			name: "CaseA_LoadedSkillMatchSingleCandidateWithAvoid",
			pending: []ProceduralCandidate{
				testProcCand(20, "ha-ops", "trigger with avoid", []string{"do not do this"}, nil),
			},
			diskSkills:    diskSkills,
			wantBatches:   1,
			wantActions:   []string{"EDIT"},
			wantDomains:   []string{"discord"},
			wantCandCount: []int{1},
		},
		{
			name: "CaseA_LoadedSkillMatchSingleCandidateNoAvoidNotBatched",
			pending: []ProceduralCandidate{
				testProcCand(30, "ha-ops", "single candidate no avoid", nil, nil),
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
		},
		{
			name: "CaseB_UnloadedRoutingMissHighSimilarityToExistingSkill",
			pending: []ProceduralCandidate{
				// Cosine similarity to skillA (1, 0) is 0.80 >= 0.72
				testProcCand(40, "", "routing miss trigger", nil, testEmbedding2D(0.80, 0.60)),
			},
			diskSkills:    diskSkills,
			wantBatches:   1,
			wantActions:   []string{"EDIT"},
			wantDomains:   []string{"discord"},
			wantCandCount: []int{1},
		},
		{
			name: "CaseB_UnmatchedLoadedSkillRoutingMiss",
			pending: []ProceduralCandidate{
				// LoadedSkill "unknown" not in diskSkills, similarity to skillB (0, 1) is 0.80 >= 0.72
				testProcCand(45, "unknown-skill", "routing miss trigger 2", nil, testEmbedding2D(0.60, 0.80)),
			},
			diskSkills:    diskSkills,
			wantBatches:   1,
			wantActions:   []string{"EDIT"},
			wantDomains:   []string{"common"},
			wantCandCount: []int{1},
		},
		{
			name: "CaseC_NovelWorkflowCliqueFormsCreateBatch",
			pending: []ProceduralCandidate{
				// Both candidates have similarity 1.0 to each other, but orthogonal to skillA and skillB
				testProcCand(50, "", "novel workflow step 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(51, "", "novel workflow step 2", nil, testEmbeddingAt(2, 1.0)),
			},
			diskSkills:    diskSkills,
			wantBatches:   1,
			wantActions:   []string{"CREATE"},
			wantDomains:   []string{"discord"},
			wantCandCount: []int{2},
		},
		{
			name: "CaseC_ShortCandidatesUnderMinStepsDoNotFormClique",
			pending: []ProceduralCandidate{
				{ID: 55, Domain: "discord", Trigger: "find depth", Steps: []string{"use -maxdepth"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 56, Domain: "discord", Trigger: "find cmd", Steps: []string{"pass -maxdepth 2"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 57, Domain: "discord", Trigger: "find flag", Steps: []string{"pass -maxdepth 3"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 58, Domain: "discord", Trigger: "find limit", Steps: []string{"pass -maxdepth 4"}, Embedding: testEmbeddingAt(2, 1.0)},
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
		},
		{
			name: "CaseD_ShortCandidatesFiveRepeatsEmitAlertBatch",
			pending: []ProceduralCandidate{
				{ID: 71, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 72, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 73, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 74, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 75, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
			},
			diskSkills:    diskSkills,
			wantBatches:   1,
			wantActions:   []string{"ALERT"},
			wantDomains:   []string{"discord"},
			wantCandCount: []int{5},
		},
		{
			name: "CaseC_DissimilarCandidatesDoNotFormClique",
			pending: []ProceduralCandidate{
				// Orthogonal to each other (sim = 0) and orthogonal to disk skills
				testProcCand(60, "", "cand a", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(61, "", "cand b", nil, testEmbeddingAt(3, 1.0)),
			},
			diskSkills:  diskSkills,
			wantBatches: 0,
		},
		{
			name: "MixedCasesComprehensive",
			pending: []ProceduralCandidate{
				testProcCand(1, "incident-triage", "blocked", nil, nil),
				testProcCand(2, "ha-ops", "case a 1", nil, nil),
				testProcCand(3, "ha-ops", "case a 2", nil, nil),
				testProcCand(4, "", "case b miss", nil, testEmbedding2D(0.0, 1.0)), // sim 1.0 to skillB
				testProcCand(5, "", "case c 1", nil, testEmbeddingAt(4, 1.0)),
				testProcCand(6, "", "case c 2", nil, testEmbeddingAt(4, 1.0)),
			},
			diskSkills:    diskSkills,
			wantBatches:   3,
			wantActions:   []string{"EDIT", "EDIT", "CREATE"},
			wantDomains:   []string{"discord", "common", "discord"},
			wantCandCount: []int{2, 1, 2},
			wantSkipped:   []int64{1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batches, skipped := PlanSkillConsolidation(tc.pending, tc.diskSkills)
			if len(batches) != tc.wantBatches {
				t.Fatalf("batches len = %d, want %d", len(batches), tc.wantBatches)
			}
			for i, wantAct := range tc.wantActions {
				if batches[i].Action != wantAct {
					t.Errorf("batch[%d].Action = %q, want %q", i, batches[i].Action, wantAct)
				}
				if batches[i].Domain != tc.wantDomains[i] {
					t.Errorf("batch[%d].Domain = %q, want %q", i, batches[i].Domain, tc.wantDomains[i])
				}
				if len(batches[i].Candidates) != tc.wantCandCount[i] {
					t.Errorf("batch[%d].Candidates len = %d, want %d", i, len(batches[i].Candidates), tc.wantCandCount[i])
				}
			}
			if len(skipped) != len(tc.wantSkipped) {
				t.Fatalf("skipped len = %d, want %d", len(skipped), len(tc.wantSkipped))
			}
			for i, wantID := range tc.wantSkipped {
				if skipped[i] != wantID {
					t.Errorf("skipped[%d] = %d, want %d", i, skipped[i], wantID)
				}
			}
		})
	}
}

func TestValidateCompiledSkillMarkdown(t *testing.T) {
	validMarkdown := `---
name: custom-workflow
description: This is a robust description with at least twenty characters.
---
# Custom Workflow Title
Detailed procedure description goes here with step-by-step instructions.
`
	targetSkill := &DiskSkill{
		Name:        "custom-workflow",
		Domain:      "discord",
		RelPath:     "custom-skills/discord/custom-workflow/SKILL.md",
		RawContent:  validMarkdown,
		Description: "This is a robust description with at least twenty characters.",
	}
	existingSkills := []DiskSkill{*targetSkill}

	tests := []struct {
		name           string
		action         string
		domain         string
		rawMarkdown    string
		target         *DiskSkill
		existingSkills []DiskSkill
		wantName       string
		wantRelPath    string
		wantErr        bool
		errContains    string
	}{
		{
			name:           "ValidCreateDiscordDomain",
			action:         "CREATE",
			domain:         "discord",
			rawMarkdown:    strings.Replace(validMarkdown, "custom-workflow", "new-workflow", 1),
			existingSkills: existingSkills,
			wantName:       "new-workflow",
			wantRelPath:    "custom-skills/discord/new-workflow/SKILL.md",
		},
		{
			name:           "ValidCreateCommonDomainNormalized",
			action:         "CREATE",
			domain:         "COMMON",
			rawMarkdown:    strings.Replace(validMarkdown, "custom-workflow", "new-common", 1),
			existingSkills: existingSkills,
			wantName:       "new-common",
			wantRelPath:    "custom-skills/common/new-common/SKILL.md",
		},
		{
			name:           "ValidCreateFallbackDomainNormalized",
			action:         "create",
			domain:         "unknown-domain",
			rawMarkdown:    strings.Replace(validMarkdown, "custom-workflow", "fallback-flow", 1),
			existingSkills: existingSkills,
			wantName:       "fallback-flow",
			wantRelPath:    "custom-skills/discord/fallback-flow/SKILL.md",
		},
		{
			name:           "ValidEditRetainsTargetRelPath",
			action:         "EDIT",
			domain:         "discord",
			rawMarkdown:    validMarkdown + "\nAdditional instructions.",
			target:         targetSkill,
			existingSkills: existingSkills,
			wantName:       "custom-workflow",
			wantRelPath:    "custom-skills/discord/custom-workflow/SKILL.md",
		},
		{
			name:        "ErrInvalidFrontmatter",
			action:      "CREATE",
			rawMarkdown: "not valid frontmatter",
			wantErr:     true,
			errContains: "invalid skill markdown frontmatter",
		},
		{
			name:   "ErrInvalidSkillName",
			action: "CREATE",
			rawMarkdown: `---
name: Invalid_Name!
description: Description that is at least twenty characters long.
---
# Title
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "invalid skill name",
		},
		{
			name:   "ErrDescriptionTooShort",
			action: "CREATE",
			rawMarkdown: `---
name: valid-name
description: short
---
# Title
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "out of bounds [20, 500]",
		},
		{
			name:   "ErrMarkdownTooShort",
			action: "CREATE",
			rawMarkdown: `---
name: valid-name
description: This description is long enough to pass validation.
---
short`,
			wantErr:     true,
			errContains: "out of bounds [100, 16384]",
		},
		{
			name:   "ErrBlockedCoreSkillName",
			action: "CREATE",
			rawMarkdown: `---
name: self-improvement
description: This description is long enough to pass validation.
---
# Blocked Skill Body
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "is a blocked core skill",
		},
		{
			name:        "ErrEditNilTarget",
			action:      "EDIT",
			rawMarkdown: validMarkdown,
			target:      nil,
			wantErr:     true,
			errContains: "target skill required for EDIT",
		},
		{
			name:   "ErrEditNameMismatch",
			action: "EDIT",
			target: targetSkill,
			rawMarkdown: `---
name: different-name
description: This description is long enough to pass validation.
---
# Different Name
Body text with enough length to pass the minimum character count of one hundred characters.
`,
			wantErr:     true,
			errContains: "does not match target",
		},
		{
			name:   "ErrEditShrunkMoreThan30Percent",
			action: "EDIT",
			target: &DiskSkill{
				Name:        "custom-workflow",
				RelPath:     "custom-skills/discord/custom-workflow/SKILL.md",
				RawContent:  strings.Repeat("a", 500),
				Description: "long content target",
			},
			rawMarkdown: `---
name: custom-workflow
description: This description is long enough to pass validation.
---
# Small Body
Body text that is at least 100 bytes long so length check passes.
`,
			wantErr:     true,
			errContains: "shrunk by >30%",
		},
		{
			name:           "ErrCreateNameCollision",
			action:         "CREATE",
			rawMarkdown:    validMarkdown,
			existingSkills: existingSkills, // contains custom-workflow
			wantErr:        true,
			errContains:    "collides with existing skill",
		},
		{
			name:        "ErrUnsupportedAction",
			action:      "DELETE",
			rawMarkdown: validMarkdown,
			wantErr:     true,
			errContains: "unsupported action",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name, relPath, _, err := ValidateCompiledSkillMarkdown(tc.action, tc.domain, tc.rawMarkdown, tc.target, tc.existingSkills)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateCompiledSkillMarkdown() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if name != tc.wantName {
				t.Errorf("name = %q, want %q", name, tc.wantName)
			}
			if relPath != tc.wantRelPath {
				t.Errorf("relPath = %q, want %q", relPath, tc.wantRelPath)
			}
		})
	}
}

func TestBuildSkillDesignerPrompt(t *testing.T) {
	target := &DiskSkill{
		Name:        "target-skill",
		Domain:      "discord",
		RelPath:     "custom-skills/discord/target-skill/SKILL.md",
		Description: "Target description",
		RawContent:  "# Target Content",
	}

	tests := []struct {
		name         string
		batch        SkillConsolidationBatch
		wantContains []string
	}{
		{
			name: "EditPromptWithTargetAndCandidates",
			batch: SkillConsolidationBatch{
				Action:      "EDIT",
				Domain:      "discord",
				TargetSkill: target,
				Candidates: []ProceduralCandidate{
					{
						ID:      101,
						Trigger: "run diagnostic command",
						Steps:   []string{"check logs", "verify service"},
						Avoid:   []string{"do not restart without checking logs"},
					},
				},
			},
			wantContains: []string{
				"Action: EDIT",
				"Target Skill Name: target-skill",
				"Existing SKILL.md Content:",
				"# Target Content",
				"Candidate #1 (ID: 101):",
				"Trigger: run diagnostic command",
				"- check logs",
				"- do not restart without checking logs",
			},
		},
		{
			name: "CreatePromptWithDomainAndCandidates",
			batch: SkillConsolidationBatch{
				Action: "CREATE",
				Domain: "common",
				Candidates: []ProceduralCandidate{
					{
						ID:      202,
						Trigger: "backup sqlite database",
						Steps:   []string{"run sqlite3 .backup"},
					},
				},
			},
			wantContains: []string{
				"Action: CREATE",
				"Target Domain: common",
				"Candidate #1 (ID: 202):",
				"Trigger: backup sqlite database",
				"- run sqlite3 .backup",
				"30+ days",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prompt := buildSkillDesignerPrompt(tc.batch)
			for _, want := range tc.wantContains {
				if !strings.Contains(prompt, want) {
					t.Errorf("prompt missing %q:\n%s", want, prompt)
				}
			}
		})
	}
}

func TestFindCandidateCliques(t *testing.T) {
	tests := []struct {
		name        string
		cands       []ProceduralCandidate
		minSim      float64
		minSize     int
		maxSize     int
		wantCliques int
		wantSizes   []int
	}{
		{
			name:        "EmptyCandidates",
			cands:       nil,
			wantCliques: 0,
		},
		{
			name: "FallbackDefaults",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(2, "", "t2", nil, testEmbeddingAt(0, 1.0)),
			},
			minSim:      -1.0,
			minSize:     -1,
			maxSize:     -1,
			wantCliques: 1,
			wantSizes:   []int{2},
		},
		{
			name: "CandidateWithoutEmbeddingSkipped",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, nil),
				testProcCand(2, "", "t2", nil, []float32{}),
			},
			wantCliques: 0,
		},
		{
			name: "CliqueOfSizeThree",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(2, "", "t2", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(3, "", "t3", nil, testEmbeddingAt(0, 1.0)),
			},
			minSim:      0.75,
			minSize:     2,
			maxSize:     5,
			wantCliques: 1,
			wantSizes:   []int{3},
		},
		{
			name: "CandidateMatchesOnlyFirstTwoNotThird",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbedding2D(0.8, 0.6)),
				testProcCand(2, "", "t2", nil, testEmbedding2D(0.8, 0.6)),
				testProcCand(3, "", "t3", nil, testEmbedding2D(1.0, 0.0)),
				testProcCand(4, "", "t4", nil, testEmbedding2D(0.6, 0.8)),
			},
			minSim:      0.75,
			minSize:     2,
			maxSize:     5,
			wantCliques: 1,
			wantSizes:   []int{3},
		},
		{
			name: "MaxSizeBreaksEarly",
			cands: []ProceduralCandidate{
				testProcCand(1, "", "t1", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(2, "", "t2", nil, testEmbeddingAt(0, 1.0)),
				testProcCand(3, "", "t3", nil, testEmbeddingAt(0, 1.0)),
			},
			minSim:      0.75,
			minSize:     2,
			maxSize:     2,
			wantCliques: 1,
			wantSizes:   []int{2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cliques := findCandidateCliques(tc.cands, tc.minSim, tc.minSize, tc.maxSize)
			if len(cliques) != tc.wantCliques {
				t.Fatalf("cliques len = %d, want %d", len(cliques), tc.wantCliques)
			}
			for i, wantSize := range tc.wantSizes {
				if len(cliques[i]) != wantSize {
					t.Errorf("cliques[%d] len = %d, want %d", i, len(cliques[i]), wantSize)
				}
			}
		})
	}
}

type mockGitHubServerConfig struct {
	openPRRefs       []string
	failListPRs      bool
	failGetMainRef   bool
	emptyMainRefSHA  bool
	failGetCommit    bool
	emptyBaseTreeSHA bool
	failCreateTree   bool
	emptyTreeSHA     bool
	failCreateCommit bool
	emptyCommitSHA   bool
	failCreateRef    bool
	failCreatePR     bool
	failDeleteRef    bool
	failAutoMerge    bool
	failBrainReg     bool
}

func newMockGitHubServer(t *testing.T, cfg mockGitHubServerConfig) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/pulls"):
			if cfg.failListPRs {
				http.Error(w, `{"message":"failed to list PRs"}`, http.StatusInternalServerError)
				return
			}
			var prs []map[string]any
			for i, ref := range cfg.openPRRefs {
				prs = append(prs, map[string]any{
					"number": i + 1,
					"head": map[string]string{
						"ref": ref,
						"sha": "head-sha-123",
					},
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(prs)

		case r.Method == http.MethodGet && strings.HasSuffix(path, "/git/ref/heads/main"):
			if cfg.failGetMainRef {
				http.Error(w, `{"message":"not found"}`, http.StatusInternalServerError)
				return
			}
			sha := "main-sha-123"
			if cfg.emptyMainRefSHA {
				sha = ""
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": map[string]string{"sha": sha, "type": "commit"},
			})

		case r.Method == http.MethodGet && strings.Contains(path, "/git/commits/"):
			if cfg.failGetCommit {
				http.Error(w, `{"message":"commit error"}`, http.StatusInternalServerError)
				return
			}
			treeSHA := "base-tree-456"
			if cfg.emptyBaseTreeSHA {
				treeSHA = ""
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sha":  "main-sha-123",
				"tree": map[string]string{"sha": treeSHA},
			})

		case r.Method == http.MethodPost && strings.HasSuffix(path, "/git/trees"):
			if cfg.failCreateTree {
				http.Error(w, `{"message":"tree error"}`, http.StatusInternalServerError)
				return
			}
			treeSHA := "new-tree-789"
			if cfg.emptyTreeSHA {
				treeSHA = ""
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"sha": treeSHA})

		case r.Method == http.MethodPost && strings.HasSuffix(path, "/git/commits"):
			if cfg.failCreateCommit {
				http.Error(w, `{"message":"commit error"}`, http.StatusInternalServerError)
				return
			}
			commitSHA := "new-commit-abc"
			if cfg.emptyCommitSHA {
				commitSHA = ""
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"sha": commitSHA})

		case r.Method == http.MethodPost && strings.HasSuffix(path, "/git/refs"):
			if cfg.failCreateRef {
				http.Error(w, `{"message":"ref error"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"ref": "refs/heads/branch"})

		case r.Method == http.MethodDelete && strings.Contains(path, "/git/refs/heads/"):
			if cfg.failDeleteRef {
				http.Error(w, `{"message":"del error"}`, http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPost && strings.HasSuffix(path, "/pulls"):
			if cfg.failCreatePR {
				http.Error(w, `{"message":"pr error"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":  42,
				"node_id": "PR_kwDO123",
			})

		case r.Method == http.MethodPost && path == "/graphql":
			if cfg.failAutoMerge {
				http.Error(w, `{"errors":[{"message":"automerge error"}]}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"enablePullRequestAutoMerge": map[string]any{},
				},
			})

		case r.Method == http.MethodPost && path == "/internal/pr/register":
			if cfg.failBrainReg {
				http.Error(w, `{"error":"registration failed"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "registered"})

		case r.Method == http.MethodPost && path == "/discord/message":
			if cfg.failBrainReg {
				http.Error(w, `{"error":"discord message failed"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "sent"})

		default:
			http.NotFound(w, r)
		}
	}))
}

func TestGitHubSkillPublisher_PublishSkillChanges(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		token     string
		repo      string
		channelID string
		changes   []SkillFileChange
		serverCfg mockGitHubServerConfig
		wantPR    int
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "MissingToken",
			token:     "",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "missing GITHUB_PAT",
		},
		{
			name:      "ForbiddenAerialRepo",
			token:     "pat-123",
			repo:      "aerial",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "publishing directly to aerial repository is forbidden",
		},
		{
			name:    "EmptyChangesReturnsZero",
			token:   "pat-123",
			repo:    "aerial-config",
			changes: nil,
			wantPR:  0,
			wantErr: false,
		},
		{
			name:      "InvalidRelPathRoot",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "invalid skill relative path",
		},
		{
			name:      "InvalidRelPathBadDomain",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/voice/ha/SKILL.md", Content: "foo"}},
			wantErr:   true,
			errSubstr: "invalid skill relative path",
		},
		{
			name:      "OpenCovenPRAlreadyExists",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{openPRRefs: []string{"update/coven-skills-999"}},
			wantPR:    0,
			wantErr:   false,
		},
		{
			name:      "ListPullsError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failListPRs: true},
			wantErr:   true,
			errSubstr: "failed to list open pull requests",
		},
		{
			name:      "GetMainRefError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failGetMainRef: true},
			wantErr:   true,
			errSubstr: "failed to get heads/main",
		},
		{
			name:      "EmptyMainRefSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyMainRefSHA: true},
			wantErr:   true,
			errSubstr: "empty SHA for heads/main",
		},
		{
			name:      "GetCommitError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failGetCommit: true},
			wantErr:   true,
			errSubstr: "failed to get commit",
		},
		{
			name:      "EmptyBaseTreeSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyBaseTreeSHA: true},
			wantErr:   true,
			errSubstr: "empty tree SHA",
		},
		{
			name:      "CreateTreeError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreateTree: true},
			wantErr:   true,
			errSubstr: "failed to create tree",
		},
		{
			name:      "EmptyCreatedTreeSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyTreeSHA: true},
			wantErr:   true,
			errSubstr: "empty SHA for created tree",
		},
		{
			name:      "CreateCommitError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreateCommit: true},
			wantErr:   true,
			errSubstr: "failed to create commit",
		},
		{
			name:      "EmptyCreatedCommitSHAError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{emptyCommitSHA: true},
			wantErr:   true,
			errSubstr: "empty SHA for created commit",
		},
		{
			name:      "CreateRefError",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreateRef: true},
			wantErr:   true,
			errSubstr: "failed to create ref",
		},
		{
			name:      "CreatePRErrorCleansBranch",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreatePR: true},
			wantErr:   true,
			errSubstr: "failed to create pull request",
		},
		{
			name:      "CreatePRErrorCleansBranchDeleteErrorHandled",
			token:     "pat-123",
			repo:      "aerial-config",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo"}},
			serverCfg: mockGitHubServerConfig{failCreatePR: true, failDeleteRef: true},
			wantErr:   true,
			errSubstr: "failed to create pull request",
		},
		{
			name:      "SuccessSingleChangeWithAutoMergeAndBrainRegister",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "1562837549255495693",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo", CommitSummary: "update ha", Action: "EDIT"}},
			wantPR:    42,
			wantErr:   false,
		},
		{
			name:      "SuccessMultipleChangesWithoutChannelID",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "",
			changes: []SkillFileChange{
				{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo", CommitSummary: "update ha", Action: "EDIT"},
				{RelPath: "custom-skills/common/deploy/SKILL.md", Content: "bar", CommitSummary: "add deploy", Action: "CREATE"},
			},
			wantPR:  42,
			wantErr: false,
		},
		{
			name:      "SuccessAutoMergeAndBrainRegisterFailuresHandledBestEffort",
			token:     "pat-123",
			repo:      "aerial-config",
			channelID: "1562837549255495693",
			changes:   []SkillFileChange{{RelPath: "custom-skills/discord/ha/SKILL.md", Content: "foo", CommitSummary: "update ha", Action: "EDIT"}},
			serverCfg: mockGitHubServerConfig{failAutoMerge: true, failBrainReg: true},
			wantPR:    42,
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newMockGitHubServer(t, tc.serverCfg)
			defer server.Close()

			fixedTime := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
			publisher := NewGitHubSkillPublisher(GitHubSkillPublisherConfig{
				Token:              tc.token,
				Owner:              "azylman",
				Repo:               tc.repo,
				BrainPRRegisterURL: server.URL + "/internal/pr/register",
				SystemChannelID:    tc.channelID,
				BaseURL:            server.URL,
				HTTPClient:         server.Client(),
				Now:                func() time.Time { return fixedTime },
			})

			prNum, err := publisher.PublishSkillChanges(ctx, tc.changes)
			if (err != nil) != tc.wantErr {
				t.Fatalf("PublishSkillChanges() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("err = %q, want substring %q", err.Error(), tc.errSubstr)
				}
				return
			}
			if prNum != tc.wantPR {
				t.Errorf("prNum = %d, want %d", prNum, tc.wantPR)
			}
			_ = publisher.NotifyShortFriction(ctx, "test alert")
			_ = publisher.NotifyShortFriction(ctx, "")
		})
	}
}

type mockSkillPublisher struct {
	prNum    int
	err      error
	alertErr error
	changes  []SkillFileChange
	alerts   []string
}

func (m *mockSkillPublisher) PublishSkillChanges(ctx context.Context, changes []SkillFileChange) (int, error) {
	m.changes = changes
	if m.err != nil {
		return 0, m.err
	}
	return m.prNum, nil
}

func (m *mockSkillPublisher) NotifyShortFriction(ctx context.Context, content string) error {
	m.alerts = append(m.alerts, content)
	return m.alertErr
}

func TestConsolidateProceduralSkills(t *testing.T) {
	ctx := context.Background()

	validSkillMD := `---
name: ha-ops
description: Home assistant operations workflow and automation routines.
---
# Home Assistant Operations
Procedure steps for configuring automations and entities properly.
`
	validNewSkillMD := `---
name: new-flow
description: Novel procedure for orchestrating automated deployments.
---
# New Flow Procedure
1. First step in the procedural execution pipeline.
2. Second step in the procedural execution pipeline.
3. Third step in the procedural execution pipeline.
`

	tests := []struct {
		name                    string
		lockExtraction          bool
		nilStore                bool
		nilPublisher            bool
		nilLLM                  bool
		storeFailOp             string
		cands                   []ProceduralCandidate
		customSkillsDir         string
		llmResp                 string
		llmErr                  error
		pubPR                   int
		pubErr                  error
		alertErr                error
		wantPR                  int
		wantErr                 bool
		wantMarkConsolidate     bool
		wantMarkSkipped         bool
		wantChangesLen          int
		wantAlertsLen           int
		wantConsolidatedIndices []int
		wantPendingIndices      []int
	}{
		{
			name:           "ExtractionMutexLockedReturnsZero",
			lockExtraction: true,
			wantPR:         0,
			wantErr:        false,
		},
		{
			name:     "NilStoreFails",
			nilStore: true,
			wantErr:  true,
		},
		{
			name:         "NilPublisherFails",
			nilPublisher: true,
			wantErr:      true,
		},
		{
			name:    "NilLLMFails",
			nilLLM:  true,
			wantErr: true,
		},
		{
			name:        "GetPendingCandidatesStoreError",
			storeFailOp: "GetPendingProceduralCandidates",
			wantErr:     true,
		},
		{
			name:    "NoPendingCandidatesReturnsZero",
			cands:   nil,
			wantPR:  0,
			wantErr: false,
		},
		{
			name:            "ScanCustomSkillsDirError",
			cands:           []ProceduralCandidate{testProcCand(1, "ha-ops", "trigger", []string{"avoid"}, nil)},
			customSkillsDir: "FILE_AS_DIR",
			wantErr:         true,
		},
		{
			name: "PlanYieldsNoBatchesAllBlockedCoreSkillsSkipped",
			cands: []ProceduralCandidate{
				testProcCand(10, "self-improvement", "trigger 1", nil, nil),
				testProcCand(11, "writing-plans", "trigger 2", nil, nil),
			},
			wantPR:          0,
			wantErr:         false,
			wantMarkSkipped: true,
		},
		{
			name: "LLMInvocationErrorBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(20, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmErr:  errors.New("llm execution error"),
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "LLMMalformedJSONBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(21, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp: "not a json object",
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "DesignerReturnsNOOPCandidateMarkedSkipped",
			cands: []ProceduralCandidate{
				testProcCand(22, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp:         `{"action":"NOOP"}`,
			wantPR:          0,
			wantErr:         false,
			wantMarkSkipped: true,
		},
		{
			name: "DesignerReturnsUnknownActionBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(23, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp: `{"action":"DELETE"}`,
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "DesignerValidationFailsBatchSkipped",
			cands: []ProceduralCandidate{
				testProcCand(24, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp: `{"action":"EDIT","name":"ha-ops","markdown":"short"}`,
			wantPR:  0,
			wantErr: false,
		},
		{
			name: "PublisherFailsReturnsError",
			cands: []ProceduralCandidate{
				testProcCand(25, "", "trigger 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(26, "", "trigger 2", nil, testEmbeddingAt(2, 1.0)),
			},
			llmResp: fmt.Sprintf(`{"action":"CREATE","name":"new-flow","domain":"discord","commit_summary":"add new flow","markdown":%q}`, validNewSkillMD),
			pubErr:  errors.New("github publish error"),
			wantErr: true,
		},
		{
			name: "SuccessCREATEBatch",
			cands: []ProceduralCandidate{
				testProcCand(30, "", "trigger 1", nil, testEmbeddingAt(2, 1.0)),
				testProcCand(31, "", "trigger 2", nil, testEmbeddingAt(2, 1.0)),
			},
			llmResp:             fmt.Sprintf(`{"action":"CREATE","name":"new-flow","domain":"discord","commit_summary":"add new flow","markdown":%q}`, validNewSkillMD),
			pubPR:               88,
			wantPR:              88,
			wantErr:             false,
			wantMarkConsolidate: true,
		},
		{
			name: "SuccessEDITBatch",
			cands: []ProceduralCandidate{
				testProcCand(40, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp:             fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","domain":"discord","commit_summary":"update ha-ops","markdown":%q}`, validSkillMD),
			pubPR:               89,
			wantPR:              89,
			wantErr:             false,
			wantMarkConsolidate: true,
		},
		{
			name: "PublisherReturnsZeroPRAlreadyOpenCandidatesNotConsolidated",
			cands: []ProceduralCandidate{
				testProcCand(50, "ha-ops", "trigger 1", []string{"avoid this"}, nil),
			},
			llmResp:             fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","domain":"discord","commit_summary":"update ha-ops","markdown":%q}`, validSkillMD),
			pubPR:               0,
			wantPR:              0,
			wantErr:             false,
			wantMarkConsolidate: false,
		},
		{
			name: "CollidingBatchesInSamePassDeferred",
			cands: []ProceduralCandidate{
				testProcCand(60, "ha-ops", "trigger 1", []string{"instruction 1"}, nil),
				testProcCand(61, "", "trigger 2", nil, testEmbeddingAt(0, 1.0)),
			},
			llmResp:                 fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","domain":"discord","commit_summary":"update ha-ops","markdown":%q}`, validSkillMD),
			pubPR:                   95,
			wantPR:                  95,
			wantErr:                 false,
			wantChangesLen:          1,
			wantConsolidatedIndices: []int{0},
			wantPendingIndices:      []int{1},
		},
		{
			name: "ShortFrictionAlertBatchNotifiedAndMarkedSkipped",
			cands: []ProceduralCandidate{
				{ID: 81, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 82, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 83, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 84, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 85, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
			},
			wantPR:          0,
			wantErr:         false,
			wantMarkSkipped: true,
			wantAlertsLen:   1,
		},
		{
			name: "ShortFrictionAlertErrorRetainsPending",
			cands: []ProceduralCandidate{
				{ID: 91, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 92, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 93, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 94, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
				{ID: 95, Domain: "discord", Trigger: "psql missing", Steps: []string{"use python"}, Avoid: []string{"avoid psql"}, Embedding: testEmbeddingAt(2, 1.0)},
			},
			alertErr:           errors.New("alert post failed"),
			wantPR:             0,
			wantErr:            false,
			wantAlertsLen:      1,
			wantPendingIndices: []int{0, 1, 2, 3, 4},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.lockExtraction {
				extractionMutex.Lock()
				defer extractionMutex.Unlock()
			}

			tmpDir := t.TempDir()
			skillsDir := filepath.Join(tmpDir, "custom-skills")
			discordDir := filepath.Join(skillsDir, "discord", "ha-ops")
			if err := os.MkdirAll(discordDir, 0o755); err != nil {
				t.Fatalf("failed to create test custom skills dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(discordDir, "SKILL.md"), []byte(validSkillMD), 0o644); err != nil {
				t.Fatalf("failed to write test SKILL.md: %v", err)
			}

			targetSkillsDir := skillsDir
			if tc.customSkillsDir == "FILE_AS_DIR" {
				dummyFile := filepath.Join(tmpDir, "dummy-file")
				_ = os.WriteFile(dummyFile, []byte("not-a-dir"), 0o644)
				targetSkillsDir = dummyFile
			} else if tc.customSkillsDir != "" {
				targetSkillsDir = tc.customSkillsDir
			}

			mem := newTestMemStore()
			if tc.storeFailOp != "" {
				mem.failOp = tc.storeFailOp
				mem.failErr = errors.New("simulated store failure")
			}
			var insertedIDs []int64
			for _, cand := range tc.cands {
				id, _ := mem.InsertProceduralCandidate(ctx, cand)
				insertedIDs = append(insertedIDs, id)
			}

			var store Store = mem
			if tc.nilStore {
				store = nil
			}

			pub := &mockSkillPublisher{
				prNum:    tc.pubPR,
				err:      tc.pubErr,
				alertErr: tc.alertErr,
			}
			var publisher SkillPublisher = pub
			if tc.nilPublisher {
				publisher = nil
			}

			var llm LLMClientFunc = func(c context.Context, prompt string) (string, error) {
				if tc.llmErr != nil {
					return "", tc.llmErr
				}
				return tc.llmResp, nil
			}
			if tc.nilLLM {
				llm = nil
			}

			ollama := &MockOllamaClient{embedding: testEmbeddingAt(0, 1.0)}

			prNum, err := ConsolidateProceduralSkills(ctx, store, ollama, llm, publisher, targetSkillsDir)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ConsolidateProceduralSkills() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if prNum != tc.wantPR {
				t.Errorf("prNum = %d, want %d", prNum, tc.wantPR)
			}

			if tc.wantMarkConsolidate {
				for _, id := range insertedIDs {
					if mem.proceduralCandidates[id].Status != "consolidated" {
						t.Errorf("candidate %d status = %q, want consolidated", id, mem.proceduralCandidates[id].Status)
					}
					if mem.proceduralCandidates[id].PRNumber != tc.wantPR {
						t.Errorf("candidate %d pr_number = %d, want %d", id, mem.proceduralCandidates[id].PRNumber, tc.wantPR)
					}
				}
			}
			if tc.wantMarkSkipped {
				for _, id := range insertedIDs {
					if mem.proceduralCandidates[id].Status != "skipped" {
						t.Errorf("candidate %d status = %q, want skipped", id, mem.proceduralCandidates[id].Status)
					}
				}
			}
			if tc.wantChangesLen > 0 {
				if len(pub.changes) != tc.wantChangesLen {
					t.Errorf("published changes len = %d, want %d", len(pub.changes), tc.wantChangesLen)
				}
			}
			if tc.wantAlertsLen > 0 && len(pub.alerts) != tc.wantAlertsLen {
				t.Errorf("alerts len = %d, want %d", len(pub.alerts), tc.wantAlertsLen)
			}
			for _, idx := range tc.wantConsolidatedIndices {
				if idx < len(insertedIDs) {
					id := insertedIDs[idx]
					if mem.proceduralCandidates[id].Status != "consolidated" {
						t.Errorf("candidate %d status = %q, want consolidated", id, mem.proceduralCandidates[id].Status)
					}
				}
			}
			for _, idx := range tc.wantPendingIndices {
				if idx < len(insertedIDs) {
					id := insertedIDs[idx]
					if mem.proceduralCandidates[id].Status != "pending" {
						t.Errorf("candidate %d status = %q, want pending", id, mem.proceduralCandidates[id].Status)
					}
				}
			}
		})
	}
}

func TestWorker_RunDaily_ProceduralSkills(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	skillsDir := filepath.Join(tmpDir, "custom-skills", "discord", "ha-ops")
	_ = os.MkdirAll(skillsDir, 0o755)
	validMD := `---
name: ha-ops
description: Home assistant operations workflow and automation routines.
---
# Home Assistant Operations
Procedure steps.
`
	_ = os.WriteFile(filepath.Join(skillsDir, "SKILL.md"), []byte(validMD), 0o644)

	mem := newTestMemStore()
	_, _ = mem.InsertProceduralCandidate(ctx, testProcCand(1, "ha-ops", "trigger 1", []string{"avoid"}, nil))

	pub := &mockSkillPublisher{prNum: 55}
	llm := func(c context.Context, prompt string) (string, error) {
		return fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","markdown":%q}`, validMD), nil
	}

	w := NewWorker(WorkerConfig{
		Store:           mem,
		Ollama:          &MockOllamaClient{embedding: testEmbeddingAt(0, 1.0)},
		LLMClient:       llm,
		Publisher:       pub,
		CustomSkillsDir: filepath.Join(tmpDir, "custom-skills"),
	})

	// Tick 60 is daily decay + dreaming + procedural consolidation
	w.Tick(ctx, 60)

	if len(pub.changes) != 1 {
		t.Errorf("publisher changes len = %d, want 1", len(pub.changes))
	}
}

func TestToolHandler_Consolidate_ProceduralSkills(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	skillsDir := filepath.Join(tmpDir, "custom-skills", "discord", "ha-ops")
	_ = os.MkdirAll(skillsDir, 0o755)
	validMD := `---
name: ha-ops
description: Home assistant operations workflow and automation routines.
---
# Home Assistant Operations
Procedure steps.
`
	_ = os.WriteFile(filepath.Join(skillsDir, "SKILL.md"), []byte(validMD), 0o644)

	mem := newTestMemStore()
	_, _ = mem.InsertProceduralCandidate(ctx, testProcCand(1, "ha-ops", "trigger 1", []string{"avoid"}, nil))

	pub := &mockSkillPublisher{prNum: 77}
	cfg := &Config{
		CustomSkillsDir: filepath.Join(tmpDir, "custom-skills"),
	}

	h := NewToolHandler(cfg, mem, &MockOllamaClient{embedding: testEmbeddingAt(0, 1.0)}, pub)
	h.llmClient = func(c context.Context, prompt string) (string, error) {
		return fmt.Sprintf(`{"action":"EDIT","name":"ha-ops","markdown":%q}`, validMD), nil
	}

	count, err := h.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate() err = %v", err)
	}
	if count != 0 {
		t.Errorf("fact consolidate count = %d, want 0", count)
	}
	if len(pub.changes) != 1 {
		t.Errorf("pub.changes len = %d, want 1", len(pub.changes))
	}
}

func TestConfig_LoadAndResolve_Chunk4Fields(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "chunk4.yaml")
	content := `port: "4008"
database_url: "postgres://localhost/test"
custom_skills_dir: "/yaml/custom-skills"
github_pat: "pat-from-yaml"
config_repo_owner: "yaml-owner"
config_repo_name: "yaml-repo"
brain_pr_register_url: "http://yaml-brain/internal/pr/register"
system_channel_id: "1562837549255495693"
`
	if err := os.WriteFile(yamlPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}

	tests := []struct {
		name          string
		path          string
		envSkillsDir  string
		envPAT        string
		envOwner      string
		envRepo       string
		envBrainReg   string
		envChannelID  string
		wantSkillsDir string
		wantPAT       string
		wantOwner     string
		wantRepo      string
		wantBrainReg  string
		wantChannelID string
	}{
		{
			name:          "LoadedFromYAML",
			path:          yamlPath,
			wantSkillsDir: "/yaml/custom-skills",
			wantPAT:       "pat-from-yaml",
			wantOwner:     "yaml-owner",
			wantRepo:      "yaml-repo",
			wantBrainReg:  "http://yaml-brain/internal/pr/register",
			wantChannelID: "1562837549255495693",
		},
		{
			name:          "EnvOverridesYAML",
			path:          yamlPath,
			envSkillsDir:  "/env/custom-skills",
			envPAT:        "pat-from-env",
			envOwner:      "env-owner",
			envRepo:       "env-repo",
			envBrainReg:   "http://env-brain/pr/reg",
			envChannelID:  "1562837549255495694",
			wantSkillsDir: "/env/custom-skills",
			wantPAT:       "pat-from-env",
			wantOwner:     "env-owner",
			wantRepo:      "env-repo",
			wantBrainReg:  "http://env-brain/pr/reg",
			wantChannelID: "1562837549255495694",
		},
		{
			name:          "NonNumericChannelIDCleared",
			path:          yamlPath,
			envChannelID:  "general-channel-name",
			wantSkillsDir: "/yaml/custom-skills",
			wantPAT:       "pat-from-yaml",
			wantOwner:     "yaml-owner",
			wantRepo:      "yaml-repo",
			wantBrainReg:  "http://yaml-brain/internal/pr/register",
			wantChannelID: "",
		},
		{
			name:          "DefaultsWhenEmpty",
			path:          "",
			wantSkillsDir: "/config/custom-skills",
			wantOwner:     "azylman",
			wantRepo:      "aerial-config",
			wantBrainReg:  "http://eri.aerial/internal/pr/register",
			wantChannelID: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://localhost/test")
			t.Setenv("CUSTOM_SKILLS_DIR", tc.envSkillsDir)
			t.Setenv("GITHUB_PAT", tc.envPAT)
			t.Setenv("CONFIG_REPO_OWNER", tc.envOwner)
			t.Setenv("CONFIG_REPO_NAME", tc.envRepo)
			t.Setenv("BRAIN_PR_REGISTER_URL", tc.envBrainReg)
			t.Setenv("SYSTEM_CHANNEL_ID", tc.envChannelID)

			cfg, err := LoadConfigFile(tc.path, "")
			if err != nil {
				t.Fatalf("LoadConfigFile() err = %v", err)
			}
			if cfg.CustomSkillsDir != tc.wantSkillsDir {
				t.Errorf("CustomSkillsDir = %q, want %q", cfg.CustomSkillsDir, tc.wantSkillsDir)
			}
			if cfg.GitHubPAT != tc.wantPAT {
				t.Errorf("GitHubPAT = %q, want %q", cfg.GitHubPAT, tc.wantPAT)
			}
			if cfg.ConfigRepoOwner != tc.wantOwner {
				t.Errorf("ConfigRepoOwner = %q, want %q", cfg.ConfigRepoOwner, tc.wantOwner)
			}
			if cfg.ConfigRepoName != tc.wantRepo {
				t.Errorf("ConfigRepoName = %q, want %q", cfg.ConfigRepoName, tc.wantRepo)
			}
			if cfg.BrainPRRegisterURL != tc.wantBrainReg {
				t.Errorf("BrainPRRegisterURL = %q, want %q", cfg.BrainPRRegisterURL, tc.wantBrainReg)
			}
			if cfg.SystemChannelID != tc.wantChannelID {
				t.Errorf("SystemChannelID = %q, want %q", cfg.SystemChannelID, tc.wantChannelID)
			}
		})
	}
}

func TestFormatSkillCommitAndPRHelpers(t *testing.T) {
	c1 := SkillFileChange{
		RelPath:       "custom-skills/discord/ha/SKILL.md",
		CommitSummary: "update ha",
		Action:        "EDIT",
	}
	c2 := SkillFileChange{
		RelPath:       "custom-skills/common/deploy/SKILL.md",
		CommitSummary: "add deploy",
		Action:        "CREATE",
	}

	tests := []struct {
		name          string
		changes       []SkillFileChange
		targetID      string
		wantCommitSub string
		wantTitle     string
		wantBodySub   string
	}{
		{
			name:          "SingleChange",
			changes:       []SkillFileChange{c1},
			targetID:      "1562837549255495693",
			wantCommitSub: "feat(skills): update ha",
			wantTitle:     "feat(skills): update ha",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"1562837549255495693","vanadis_chunk_id":""} -->`,
		},
		{
			name:          "MultipleChanges",
			changes:       []SkillFileChange{c1, c2},
			targetID:      "",
			wantCommitSub: "feat(skills): update 2 procedural skills",
			wantTitle:     "feat(skills): evolve 2 procedural skills",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"","vanadis_chunk_id":""} -->`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			commitMsg := formatSkillCommitMessage(tc.changes)
			if !strings.Contains(commitMsg, tc.wantCommitSub) {
				t.Errorf("commitMsg = %q, want containing %q", commitMsg, tc.wantCommitSub)
			}
			title := formatSkillPRTitle(tc.changes)
			if title != tc.wantTitle {
				t.Errorf("title = %q, want %q", title, tc.wantTitle)
			}
			body := formatSkillPRBody(tc.changes, tc.targetID)
			if !strings.Contains(body, tc.wantBodySub) {
				t.Errorf("body = %q, want containing %q", body, tc.wantBodySub)
			}
		})
	}

	// Test parseDesignerResponse
	parseTests := []struct {
		name       string
		raw        string
		wantErr    bool
		wantAction string
	}{
		{name: "EmptyRaw", raw: "", wantErr: true},
		{name: "NoJSON", raw: "hello world", wantErr: true},
		{name: "MalformedJSON", raw: "{broken json", wantErr: true},
		{name: "ValidJSON", raw: `{"action":"EDIT","name":"foo"}`, wantErr: false, wantAction: "EDIT"},
		{name: "ValidJSONWithMarkdownPrefix", raw: "```json\n{\"action\":\"CREATE\",\"name\":\"bar\"}\n```", wantErr: false, wantAction: "CREATE"},
	}

	for _, tc := range parseTests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := parseDesignerResponse(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseDesignerResponse(%q) err = %v, wantErr = %v", tc.raw, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if resp.Action != tc.wantAction {
				t.Errorf("Action = %q, want %q", resp.Action, tc.wantAction)
			}
		})
	}
}
