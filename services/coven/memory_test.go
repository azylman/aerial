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
	"slices"
	"sort"
	"strings"
	"sync"
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
	watermarkTimes       map[string]time.Time
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
		watermarkTimes:       make(map[string]time.Time),
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

func validateTestFact(text string, emb []float32) (string, error) {
	cleaned := strings.TrimSpace(text)
	if cleaned == "" {
		return "", fmt.Errorf("fact text cannot be empty")
	}
	if len(emb) != ExpectedEmbeddingDim {
		return "", fmt.Errorf("invalid embedding dimension: expected %d, got %d", ExpectedEmbeddingDim, len(emb))
	}
	return cleaned, nil
}

func (m *TestMemStore) InsertFact(ctx context.Context, factText string, embedding []float32) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "InsertFact" {
		return 0, m.failErr
	}
	cleanText, err := validateTestFact(factText, embedding)
	if err != nil {
		return 0, err
	}
	id := m.nextID
	m.nextID++
	now := time.Now().UTC()
	m.facts[id] = &Fact{
		ID:               id,
		FactText:         cleanText,
		CreatedAt:        now,
		LastReinforcedAt: now,
		ReinforceCount:   1,
	}
	m.embeddings[id] = append([]float32(nil), embedding...)
	return id, nil
}

func (m *TestMemStore) UpdateFact(ctx context.Context, id int64, factText string, embedding []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "UpdateFact" {
		return m.failErr
	}
	cleanText, err := validateTestFact(factText, embedding)
	if err != nil {
		return err
	}
	f, ok := m.facts[id]
	if !ok {
		return ErrFactNotFound
	}
	f.FactText = cleanText
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
	offset := min(max(filter.Offset, 0), total)
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
	slices.Sort(ids)
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

func (m *TestMemStore) GetConversationFactWatermark(ctx context.Context, threadID string) (int64, *time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "GetConversationFactWatermark" {
		return 0, nil, m.failErr
	}
	rowID := m.watermarks[threadID]
	var ts *time.Time
	if m.watermarkTimes != nil {
		if t, ok := m.watermarkTimes[threadID]; ok && !t.IsZero() {
			cpy := t
			ts = &cpy
		}
	}
	return rowID, ts, nil
}

func (m *TestMemStore) UpdateConversationFactWatermark(ctx context.Context, threadID string, maxRowID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOp == "UpdateConversationFactWatermark" {
		return m.failErr
	}
	if maxRowID >= m.watermarks[threadID] {
		m.watermarks[threadID] = maxRowID
	}
	if m.watermarkTimes == nil {
		m.watermarkTimes = make(map[string]time.Time)
	}
	m.watermarkTimes[threadID] = time.Now().UTC()
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

func startMockPostgres(t *testing.T) string {
	t.Helper()
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
	tests := []ollamaEmbeddingTestCase{
		{name: "SuccessDocument", handlerCodes: []int{http.StatusOK}, handlerBodies: []string{`{"embedding": [0.1, 0.2, 0.3]}`}, input: "test fact text", isQuery: false, wantVecLen: 3},
		{name: "SuccessQueryWithPrefix", handlerCodes: []int{http.StatusOK}, handlerBodies: []string{`{"embedding": [0.1, 0.2, 0.3]}`}, input: "search query", isQuery: true, prefix: "search: ", wantVecLen: 3, expectPrompt: "search: search query"},
		{name: "RetrySucceedsOnSecondAttempt", handlerCodes: []int{http.StatusInternalServerError, http.StatusOK}, handlerBodies: []string{`temporary error`, `{"embedding": [0.1, 0.2]}`}, input: "retry query", isQuery: true, prefix: "q: ", maxRetries: 2, retryDelay: 1 * time.Millisecond, wantVecLen: 2},
		{name: "NegativeRetriesClampedAndFails", handlerCodes: []int{http.StatusInternalServerError}, handlerBodies: []string{`temporary error`}, input: "query", maxRetries: -1, wantErr: true},
		{name: "ContextCanceledError", handlerCodes: []int{http.StatusInternalServerError}, handlerBodies: []string{`err`}, input: "query", maxRetries: 1, cancelCtx: true, wantErr: true},
		{name: "EmptyTextInputError", input: "", wantErr: true, handlerCodes: []int{http.StatusOK}, handlerBodies: []string{`{}`}},
		{name: "Non200Error", handlerCodes: []int{http.StatusInternalServerError}, handlerBodies: []string{`internal server error`}, input: "test text", wantErr: true},
		{name: "InvalidJSONResponse", handlerCodes: []int{http.StatusOK}, handlerBodies: []string{`{bad json`}, input: "test text", wantErr: true},
		{name: "OllamaErrorResponse", handlerCodes: []int{http.StatusOK}, handlerBodies: []string{`{"error": "model not found"}`}, input: "test text", wantErr: true},
		{name: "EmptyEmbeddingReturned", handlerCodes: []int{http.StatusOK}, handlerBodies: []string{`{"embedding": []}`}, input: "test text", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runOllamaEmbeddingCase(t, tc)
		})
	}
}

type ollamaEmbeddingTestCase struct {
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
}

func runOllamaEmbeddingCase(t *testing.T, tc ollamaEmbeddingTestCase) {
	t.Helper()
	attempt := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tc.expectPrompt != "" {
			var req struct {
				Prompt string `json:"prompt"`
			}
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
	client.SetRetryDelay(tc.retryDelay)

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
			if !tc.wantErr && out.Context != "" {
				t.Errorf("expected empty out.Context on MemorySearchOutput, got %q", out.Context)
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
