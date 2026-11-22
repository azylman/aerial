package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// queryStrings returns the single text column of every row of query.
func queryStrings(t testing.TB, db store.DBTX, query string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %q: %v", query, err)
	}
	return out
}

// factRows renders every stored fact as "text:reinforce_count" in id order.
func factRows(t testing.TB, db store.DBTX) []string {
	t.Helper()
	return queryStrings(t, db, `SELECT fact_text || ':' || reinforce_count FROM facts ORDER BY id`)
}

// watermarkRow returns the stored last_extracted_rowid for (default stack, thread), 0 when absent.
func watermarkRow(t testing.TB, db store.DBTX, thread string) string {
	t.Helper()
	got := queryStrings(t, db, `SELECT COALESCE(MAX(last_extracted_rowid), 0)::text FROM extraction_watermarks WHERE agent = $1 AND thread_id = $2`, config.DefaultStack, thread)
	return got[0]
}

// seedFacts inserts facts through the production InsertFact, fact i using embs[i].
func seedFacts(t testing.TB, s *store.SQLStore, texts []string, embs [][]float32) {
	t.Helper()
	for i, text := range texts {
		if _, err := s.InsertFact(context.Background(), text, embs[i]); err != nil {
			t.Fatalf("seed fact %q: %v", text, err)
		}
	}
}

// newIndexerServer starts a fake transcripts indexer answering every
// trajectory read with status and the given retrieved facts and tool calls.
func newIndexerServer(t testing.TB, status int, retrieved, toolCalls []string) *store.IndexerClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(store.TrajectoryContext{RetrievedFacts: retrieved, ToolCalls: toolCalls})
	}))
	t.Cleanup(srv.Close)
	return store.NewIndexerClient(srv.URL, config.DefaultStack, srv.Client())
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sequenceOllamaClient returns a different one-hot embedding (dims 0, 1, 2, ...) per call.
type sequenceOllamaClient struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *sequenceOllamaClient) GenerateEmbedding(ctx context.Context, text string, isQuery bool, maxRetries int) ([]float32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	emb := make([]float32, store.ExpectedEmbeddingDim)
	emb[s.calls%store.ExpectedEmbeddingDim] = 1.0
	s.calls++
	return emb, nil
}

type processThreadFactsContextCase struct {
	name          string
	nilStore      bool
	sabotage      string
	turns         []testTurn
	watermark     int64
	summary       string
	facts         []string
	indexerStatus int
	retrieved     []string
	toolCalls     []string
	llmResp       string
	llmErr        string
	ollamaErr     string
	parentTraceID string
	parentSpanID  string
	wantPrompt    []string
	wantFacts     []string
	wantWatermark string
	wantErr       bool
}

func processThreadFactsContextCases() []processThreadFactsContextCase {
	const thread = "th-test"
	newTurn := []testTurn{adminTurn(thread, 10, "He likes matcha tea", "Understood")}
	matchaFact := []string{"Bob likes matcha tea in the afternoon:1"}
	return []processThreadFactsContextCase{
		{
			name:          "LinksToTriggerWhenIncomingContextValid",
			turns:         newTurn,
			parentTraceID: "0af7651916cd43dd8448eb211c80319c",
			parentSpanID:  "b7ad6b7169203331",
			wantPrompt:    []string{"\nNEW UNEXTRACTED TURNS:\nUser: He likes matcha tea\nAssistant: Understood\n"},
			wantFacts:     matchaFact,
			wantWatermark: "10",
		},
		{name: "NilStoreError", nilStore: true, turns: newTurn, wantWatermark: "0", wantErr: true},
		{name: "UnextractedTurnsError", sabotage: sabotageTurnReads, turns: newTurn, wantWatermark: "0", wantErr: true},
		{
			name:          "EmptyTurnsMarksWatermarkAtMaxRow",
			turns:         []testTurn{{thread: thread, rowID: 7, content: "hi", response: "hello", status: "COMPLETED"}},
			wantWatermark: "7",
		},
		{
			name:      "PriorSummaryAndTurnsInjected",
			turns:     []testTurn{adminTurn(thread, 5, "My name is Bob", "Hello Bob"), newTurn[0]},
			watermark: 6,
			summary:   "User name is Bob",
			wantPrompt: []string{
				"PRIOR THREAD SUMMARY:\nUser name is Bob\n",
				"PRIOR TURNS (FOR COREFERENCE RESOLUTION ONLY - DO NOT EXTRACT FACTS FROM PRIOR TURNS):\nUser: My name is Bob\nAssistant: Hello Bob\n\nNEW UNEXTRACTED TURNS:\nUser: He likes matcha tea\nAssistant: Understood\n",
			},
			wantFacts:     matchaFact,
			wantWatermark: "10",
		},
		{name: "ContextQueryErrorFails", sabotage: sabotageSessions, turns: newTurn, wantWatermark: "0", wantErr: true},
		{name: "LLMErrorFails", turns: newTurn, llmErr: "llm error", wantWatermark: "0", wantErr: true},
		{name: "ParseErrorFails", turns: newTurn, llmResp: "invalid json response", wantWatermark: "0", wantErr: true},
		{
			name:  "ManyCandidatesCappedAt5",
			turns: newTurn,
			llmResp: `{"facts":[
				{"fact_text":"Candidate fact text number 1"},
				{"fact_text":"Candidate fact text number 2"},
				{"fact_text":"Candidate fact text number 3"},
				{"fact_text":"Candidate fact text number 4"},
				{"fact_text":"Candidate fact text number 5"},
				{"fact_text":"Candidate fact text number 6"},
				{"fact_text":"Candidate fact text number 7"}
			]}`,
			wantFacts: []string{
				"Candidate fact text number 1:1", "Candidate fact text number 2:1", "Candidate fact text number 3:1",
				"Candidate fact text number 4:1", "Candidate fact text number 5:1",
			},
			wantWatermark: "10",
		},
		{
			name:          "TrajectoryRetrievedFactsAndToolCallsInjected",
			turns:         newTurn,
			indexerStatus: http.StatusOK,
			retrieved:     []string{"- [id:42] User likes matcha"},
			toolCalls:     []string{`memory_search("matcha")`},
			wantPrompt:    []string{"RETRIEVED MEMORY IN TURN:\n- [id:42] User likes matcha\n", "TOOL CALLS IN TURN:\nmemory_search(\"matcha\")\n"},
			wantFacts:     matchaFact,
			wantWatermark: "10",
		},
		{
			name:          "TrajectoryReinforcesFactIDs",
			turns:         newTurn,
			facts:         []string{"User likes matcha"},
			llmResp:       `{"facts":[],"reinforced_ids":[1,1,-1,0]}`,
			wantFacts:     []string{"User likes matcha:2"},
			wantWatermark: "10",
		},
		{
			name:          "TrajectoryReinforceStoreErrorFails",
			sabotage:      sabotageReinforceFact,
			turns:         newTurn,
			facts:         []string{"User likes matcha"},
			llmResp:       `{"facts":[],"reinforced_ids":[1]}`,
			wantFacts:     []string{"User likes matcha:1"},
			wantWatermark: "0",
			wantErr:       true,
		},
		{name: "TrajectoryQueryErrorFails", turns: newTurn, indexerStatus: http.StatusInternalServerError, wantWatermark: "0", wantErr: true},
		{name: "CandidateFactInsertStoreErrorFails", sabotage: sabotageInsertFact, turns: newTurn, wantWatermark: "0", wantErr: true},
		{name: "CandidateFactEmbeddingErrorFails", turns: newTurn, ollamaErr: "embed fail", wantWatermark: "0", wantErr: true},
	}
}

func assertProcessThreadFactsPrompt(t *testing.T, tc processThreadFactsContextCase, capturedPrompt string, llmSpanID trace.SpanID) {
	t.Helper()
	if capturedPrompt == "" {
		return
	}
	assertExtractLinks(t, tc, llmSpanID)
	name, root, attrs := endedSpanSummary(llmSpanID)
	if name != "coven.extract" || !root || attrs["aerial.coven.batch_size"] == "" {
		t.Errorf("LLM call span = %q root=%v attrs=%v, want root coven.extract with aerial.coven.batch_size", name, root, attrs)
	}
	for _, want := range tc.wantPrompt {
		if !strings.Contains(capturedPrompt, want) {
			t.Errorf("captured prompt missing %q:\n%s", want, capturedPrompt)
		}
	}
}

func assertExtractLinks(t *testing.T, tc processThreadFactsContextCase, id trace.SpanID) {
	t.Helper()
	s := endedSpan(id)
	if s == nil {
		return
	}
	links := s.Links()
	if tc.parentTraceID == "" {
		if len(links) != 0 {
			t.Errorf("links = %v, want none", links)
		}
		return
	}
	if len(links) != 1 || links[0].SpanContext.TraceID().String() != tc.parentTraceID || links[0].SpanContext.SpanID().String() != tc.parentSpanID {
		t.Errorf("links = %v, want one to %s/%s", links, tc.parentTraceID, tc.parentSpanID)
	}
}

func runProcessThreadFactsContextCase(t *testing.T, tc processThreadFactsContextCase) {
	t.Helper()
	const thread = "th-test"
	db := newTestDB(t)
	sqlStore := store.NewSQLStore(db)
	seedTurns(t, db, tc.turns...)
	if tc.watermark > 0 {
		seedWatermark(t, db, config.DefaultStack, thread, tc.watermark, nil)
	}
	if tc.summary != "" {
		seedSession(t, db, config.DefaultStack, thread, tc.summary)
	}
	seedFacts(t, sqlStore, tc.facts, [][]float32{testEmbeddingAt(100, 1)})
	if tc.indexerStatus != 0 {
		sqlStore.SetIndexerClient(newIndexerServer(t, tc.indexerStatus, tc.retrieved, tc.toolCalls))
	}
	execSQL(t, db, tc.sabotage)

	var s store.Store = sqlStore
	if tc.nilStore {
		s = nil
	}
	client := &sequenceOllamaClient{}
	if tc.ollamaErr != "" {
		client.err = errors.New(tc.ollamaErr)
	}

	var capturedPrompt string
	var llmSpanID trace.SpanID
	llm := func(c context.Context, prompt string) (string, error) {
		capturedPrompt = prompt
		llmSpanID = trace.SpanContextFromContext(c).SpanID()
		if tc.llmErr != "" {
			return "", errors.New(tc.llmErr)
		}
		if tc.llmResp != "" {
			return tc.llmResp, nil
		}
		return `{"facts":[{"fact_text":"Bob likes matcha tea in the afternoon"}]}`, nil
	}

	ctx := context.Background()
	if tc.parentTraceID != "" {
		tid, _ := trace.TraceIDFromHex(tc.parentTraceID)
		sid, _ := trace.SpanIDFromHex(tc.parentSpanID)
		ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	}
	err := processThreadFacts(ctx, s, client, llm, thread)
	if (err != nil) != tc.wantErr {
		t.Fatalf("processThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertProcessThreadFactsPrompt(t, tc, capturedPrompt, llmSpanID)
	if got := factRows(t, db); !equalStrings(got, tc.wantFacts) {
		t.Errorf("facts = %q, want %q", got, tc.wantFacts)
	}
	if got := watermarkRow(t, db, thread); got != tc.wantWatermark {
		t.Errorf("watermark = %s, want %s", got, tc.wantWatermark)
	}
}

func TestProcessThreadFacts_ContextAndCoreference(t *testing.T) {
	t.Parallel()
	for _, tc := range processThreadFactsContextCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessThreadFactsContextCase(t, tc)
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
				Store:        newTestStore(t),
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

func TestWorker_ExtractThread_NilWorkerReturnsError(t *testing.T) {
	t.Parallel()
	var w *Worker
	if err := w.ExtractThread(context.Background(), "th-1"); err == nil {
		t.Fatal("ExtractThread() expected error for nil worker, got nil")
	}
}

// makePairwiseSimilarVectors// makePairwiseSimilarVectors returns n unit vectors whose pairwise cosine
// similarity is targetSim. Every vector keeps a non-zero component (pgvector
// cosine distance is NaN for zero vectors), so they are safe to store.
func makePairwiseSimilarVectors(n int, targetSim float64) [][]float32 {
	// Component 0 has sqrt(targetSim), each vector has sqrt(1 - targetSim) at component i+1
	s := float32(math.Sqrt(targetSim))
	rem := float32(math.Sqrt(1.0 - targetSim))
	vecs := make([][]float32, n)
	for i := range n {
		v := make([]float32, store.ExpectedEmbeddingDim)
		v[0] = s
		if i+1 < store.ExpectedEmbeddingDim {
			v[i+1] = rem
		}
		if v[0] == 0 && (i+1 >= store.ExpectedEmbeddingDim || rem == 0) {
			v[0] = 1
		}
		vecs[i] = v
	}
	return vecs
}

func makeScoredFactFixture(id int64, text string, emb []float32) store.ScoredFactWithEmbedding {
	now := time.Now().UTC()
	return store.ScoredFactWithEmbedding{
		Fact: store.Fact{
			ID:               id,
			FactText:         text,
			CreatedAt:        now,
			LastReinforcedAt: now,
			ReinforceCount:   1,
		},
		Embedding: emb,
	}
}

type findConsolidationCliquesCase struct {
	name      string
	facts     []store.ScoredFactWithEmbedding
	minSim    float64
	maxSim    float64
	minSize   int
	maxSize   int
	wantCount int
	wantSizes []int
	wantIDs   []int64
}

func findConsolidationCliquesCases(vecs, vecsB, looseVecs [][]float32, orthogonalVec, seedVec, borderlineVec, tightVec, hijackVec, emailVec1, emailVec2 []float32) []findConsolidationCliquesCase {
	return []findConsolidationCliquesCase{
		{
			name:      "EmptyFacts",
			facts:     nil,
			wantCount: 0,
		},
		{
			name: "FactsWithoutEmbeddingsIgnored",
			facts: []store.ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "Fact 1", nil),
				makeScoredFactFixture(2, "Fact 2", []float32{}),
			},
			wantCount: 0,
		},
		{
			name: "SeparateDisjointCliquesFormed",
			facts: []store.ScoredFactWithEmbedding{
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
			facts: []store.ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "A1", vecs[0]),
				makeScoredFactFixture(2, "A2", vecs[1]),
			},
			minSize:   3,
			wantCount: 0,
		},
		{
			name: "MaxSizeCapEnforced",
			facts: []store.ScoredFactWithEmbedding{
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
			facts: []store.ScoredFactWithEmbedding{
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
			facts: []store.ScoredFactWithEmbedding{
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
			facts: []store.ScoredFactWithEmbedding{
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
			facts: []store.ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "L1", looseVecs[0]),
				makeScoredFactFixture(2, "L2", looseVecs[1]),
				makeScoredFactFixture(3, "L3", looseVecs[2]),
			},
			wantCount: 0,
		},
		{
			name: "Size2PairAndHighSimIncludedByDefault",
			facts: []store.ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "P1", tightVec),
				makeScoredFactFixture(2, "P2", tightVec),
			},
			wantCount: 1,
			wantSizes: []int{2},
		},
		{
			name: "SimilaritySortedPrefersTighterCliqueAndRejectsInconsistentMember",
			facts: []store.ScoredFactWithEmbedding{
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
			facts: []store.ScoredFactWithEmbedding{
				makeScoredFactFixture(1, "WeakLowID", hijackVec),
				makeScoredFactFixture(2, "Email1", emailVec1),
				makeScoredFactFixture(3, "Email2", emailVec2),
			},
			wantCount: 1,
			wantSizes: []int{2},
			wantIDs:   []int64{2, 3},
		},
	}
}

func runFindConsolidationCliquesCase(t *testing.T, tc findConsolidationCliquesCase) {
	t.Helper()
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
}

func TestFindConsolidationCliques(t *testing.T) {
	vecs := makePairwiseSimilarVectors(8, 0.78) // Pairwise similarity = 0.78 (in [0.75, 1.0])
	looseVecs := makePairwiseSimilarVectors(3, 0.72)
	orthogonalVec := make([]float32, store.ExpectedEmbeddingDim)
	orthogonalVec[store.ExpectedEmbeddingDim-1] = 1.0 // Orthogonal to vecs[0..7]

	vecsB := make([][]float32, 3)
	for i := range 3 {
		v := make([]float32, store.ExpectedEmbeddingDim)
		v[10] = float32(math.Sqrt(0.78))
		v[11+i] = float32(math.Sqrt(0.22))
		vecsB[i] = v
	}

	seedVec := make([]float32, store.ExpectedEmbeddingDim)
	seedVec[0] = 1.0
	borderlineVec := make([]float32, store.ExpectedEmbeddingDim)
	borderlineVec[0] = 0.80
	borderlineVec[1] = -0.60
	tightVec := make([]float32, store.ExpectedEmbeddingDim)
	tightVec[0] = 0.95
	tightVec[1] = float32(math.Sqrt(1.0 - 0.95*0.95))

	hijackVec := make([]float32, store.ExpectedEmbeddingDim)
	hijackVec[0], hijackVec[1] = 0.76, float32(math.Sqrt(1.0-0.76*0.76))
	emailVec1 := make([]float32, store.ExpectedEmbeddingDim)
	emailVec1[0] = 1.0
	emailVec2 := make([]float32, store.ExpectedEmbeddingDim)
	emailVec2[0], emailVec2[1] = 0.92, -float32(math.Sqrt(1.0-0.92*0.92))

	tests := findConsolidationCliquesCases(vecs, vecsB, looseVecs, orthogonalVec, seedVec, borderlineVec, tightVec, hijackVec, emailVec1, emailVec2)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runFindConsolidationCliquesCase(t, tc)
		})
	}
}

type consolidateFactClustersCase struct {
	name                string
	cancelDuringLLM     bool
	nilStore            bool
	nilOllama           bool
	nilLLM              bool
	lockMutex           bool
	sabotage            string
	facts               []string
	seedWatermark       int64
	seedReinforceCounts []int
	llmResp             string
	llmErr              error
	ollamaErr           error
	wantErr             bool
	wantCount           int
	wantFacts           []string
	wantWatermark       string
}

var consolidationSourceFacts = []string{"Fact A", "Fact B", "Fact C"}

func consolidateFactClustersBasicCases(validJSON string) []consolidateFactClustersCase {
	untouched := []string{"Fact A:1", "Fact B:1", "Fact C:1"}
	merged := []string{"User prefers dark mode everywhere:1"}
	return []consolidateFactClustersCase{
		{name: "NilStoreError", nilStore: true, facts: consolidationSourceFacts, wantErr: true, wantFacts: untouched, wantWatermark: "0"},
		{name: "NilOllamaError", nilOllama: true, wantErr: true, wantWatermark: "0"},
		{name: "NilLLMError", nilLLM: true, wantErr: true, wantWatermark: "0"},
		{name: "LockContentionSkipped", lockMutex: true, facts: consolidationSourceFacts, llmResp: validJSON, wantFacts: untouched, wantWatermark: "0"},
		{name: "StoreGetAllFactsError", sabotage: sabotageFacts, wantErr: true, wantWatermark: "0"},
		{name: "NoFactsInStore", wantWatermark: "0"},
		{name: "LLMErrorContinuesWithoutDeletion", facts: consolidationSourceFacts, llmErr: errors.New("llm down"), wantFacts: untouched, wantWatermark: "0"},
		{name: "ParseErrorContinuesWithoutDeletion", facts: consolidationSourceFacts, llmResp: "not-valid-json", wantFacts: untouched, wantWatermark: "0"},
		{name: "OllamaEmbeddingErrorContinuesWithoutDeletion", facts: consolidationSourceFacts, llmResp: validJSON, ollamaErr: errors.New("ollama embed down"), wantFacts: untouched, wantWatermark: "0"},
		{name: "StoreInsertFactErrorContinuesWithoutDeletion", sabotage: sabotageInsertFact, facts: consolidationSourceFacts, llmResp: validJSON, wantFacts: untouched, wantWatermark: "0"},
		// 3 facts merge via 2 pairwise passes into 1: {1,2} -> 4, then {3,4} -> 5.
		{name: "SuccessConsolidation", facts: consolidationSourceFacts, llmResp: validJSON, wantCount: 2, wantFacts: merged, wantWatermark: "5"},
		{
			name:          "CandidateFactsWithEmptyTextSkippedAndValidMerged",
			facts:         consolidationSourceFacts,
			llmResp:       `{"facts":[{"fact_text":"","category":"","importance_score":0},{"fact_text":"Valid synthesized fact 1"}]}`,
			wantCount:     2,
			wantFacts:     []string{"Valid synthesized fact 1:1"},
			wantWatermark: "5",
		},
	}
}

func consolidateFactClustersAdvancedCases(validJSON string) []consolidateFactClustersCase {
	untouched := []string{"Fact A:1", "Fact B:1", "Fact C:1"}
	merged := []string{"User prefers dark mode everywhere:1"}
	return []consolidateFactClustersCase{
		{
			name:          "TwoCandidateFactsForPairAbortsMergeWithoutDeletion",
			facts:         consolidationSourceFacts,
			llmResp:       `{"facts":[{"fact_text":"Valid synthesized fact 1"},{"fact_text":"Valid synthesized fact 2"},{"fact_text":"Extra third fact capped"}]}`,
			wantFacts:     untouched,
			wantWatermark: "3",
		},
		{name: "AllEmptyCandidateFactsSkippedWithoutDeletion", facts: consolidationSourceFacts, llmResp: `{"facts":[{"fact_text":"   "}]}`, wantFacts: untouched, wantWatermark: "3"},
		{name: "EmptyFactsArraySkippedWithoutDeletion", facts: consolidationSourceFacts, llmResp: `{"facts":[]}`, wantFacts: untouched, wantWatermark: "3"},
		{
			// 3 not deleted + 1 inserted in pass 0 before break; failure keeps the watermark.
			name:          "StoreDeleteFactErrorLoggedGracefully",
			sabotage:      sabotageDeleteFact,
			facts:         consolidationSourceFacts,
			llmResp:       validJSON,
			wantCount:     1,
			wantFacts:     []string{"Fact A:1", "Fact B:1", "Fact C:1", "User prefers dark mode everywhere:1"},
			wantWatermark: "0",
		},
		{
			// Pass 0 has cliques {1,2} and {3,4}; the context dies during the
			// first synthesis, so its insert fails and the second clique aborts.
			name:            "ContextCanceledDuringSynthesis",
			cancelDuringLLM: true,
			facts:           []string{"Fact A", "Fact B", "Fact C", "Fact D"},
			llmResp:         validJSON,
			wantErr:         true,
			wantFacts:       []string{"Fact A:1", "Fact B:1", "Fact C:1", "Fact D:1"},
			wantWatermark:   "0",
		},
		{
			name:                "ConsolidationInheritsMaxReinforcement",
			facts:               consolidationSourceFacts,
			seedReinforceCounts: []int{1, 4, 2},
			llmResp:             validJSON,
			wantCount:           2,
			wantFacts:           []string{"User prefers dark mode everywhere:4"},
			wantWatermark:       "5",
		},
		{
			name:                "ConsolidationClampsReinforceAt10",
			facts:               consolidationSourceFacts,
			seedReinforceCounts: []int{15, 12, 11},
			llmResp:             validJSON,
			wantCount:           2,
			wantFacts:           []string{"User prefers dark mode everywhere:10"},
			wantWatermark:       "5",
		},
		{
			name:                "ConsolidationReinforceStoreErrorLoggedGracefully",
			sabotage:            sabotageReinforceFact,
			facts:               consolidationSourceFacts,
			seedReinforceCounts: []int{1, 4, 2},
			llmResp:             validJSON,
			wantCount:           2,
			wantFacts:           merged,
			wantWatermark:       "5",
		},
		{name: "WatermarkSkipsAlreadyEvaluatedOldCliques", facts: consolidationSourceFacts, seedWatermark: 3, llmResp: validJSON, wantFacts: untouched, wantWatermark: "3"},
		{name: "WatermarkEvaluatesCliqueWithNewFact", facts: consolidationSourceFacts, seedWatermark: 2, llmResp: validJSON, wantCount: 2, wantFacts: merged, wantWatermark: "5"},
		// The watermark table is gone, so its row is not asserted.
		{name: "WatermarkReadErrorFallsBackToFullEvaluation", sabotage: sabotageWatermarks, facts: consolidationSourceFacts, llmResp: validJSON, wantCount: 2, wantFacts: merged},
		{name: "WatermarkUpdateErrorLoggedGracefully", sabotage: sabotageWatermarkWrite, facts: consolidationSourceFacts, llmResp: validJSON, wantCount: 2, wantFacts: merged, wantWatermark: "0"},
	}
}

func seedConsolidateFactStore(t *testing.T, tc consolidateFactClustersCase, vecs [][]float32) (*store.SQLStore, store.DBTX) {
	t.Helper()
	s := newTestStore(t)
	db := s.DB()
	seedFacts(t, s, tc.facts, vecs)
	for i, rc := range tc.seedReinforceCounts {
		execSQL(t, db, `UPDATE facts SET reinforce_count = $1 WHERE id = $2`, rc, i+1)
	}
	if tc.seedWatermark > 0 {
		seedWatermark(t, db, config.DefaultStack, DreamingWatermarkThreadID, tc.seedWatermark, nil)
	}
	execSQL(t, db, tc.sabotage)
	return s, db
}

func runConsolidateFactClustersCase(t *testing.T, tc consolidateFactClustersCase, vecs [][]float32) {
	t.Helper()
	sqlStore, db := seedConsolidateFactStore(t, tc, vecs)

	var s store.Store = sqlStore
	if tc.nilStore {
		s = nil
	}
	var client store.OllamaClient = &MockOllamaClient{embedding: vecs[0], err: tc.ollamaErr}
	if tc.nilOllama {
		client = nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var llm LLMClientFunc
	if !tc.nilLLM {
		llm = func(c context.Context, p string) (string, error) {
			if tc.cancelDuringLLM {
				cancel()
			}
			if tc.llmErr != nil {
				return "", tc.llmErr
			}
			return tc.llmResp, nil
		}
	}

	if tc.lockMutex {
		extractionMutex.Lock()
		defer extractionMutex.Unlock()
	}

	count, err := ConsolidateFactClusters(ctx, s, client, llm)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ConsolidateFactClusters() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if count != tc.wantCount {
		t.Errorf("count = %d, want %d", count, tc.wantCount)
	}
	if tc.sabotage != sabotageFacts {
		if got := factRows(t, db); !equalStrings(got, tc.wantFacts) {
			t.Errorf("facts = %q, want %q", got, tc.wantFacts)
		}
	}
	if tc.sabotage != sabotageWatermarks {
		if got := watermarkRow(t, db, DreamingWatermarkThreadID); got != tc.wantWatermark {
			t.Errorf("dreaming watermark = %s, want %s", got, tc.wantWatermark)
		}
	}
}

// TestConsolidateFactClusters is serial: ConsolidateFactClusters single-flights
// on the package-global extractionMutex.
func TestConsolidateFactClusters(t *testing.T) {
	vecs := makePairwiseSimilarVectors(4, 0.78)
	validJSON := `{"facts":[{"category":"pref","fact_text":"User prefers dark mode everywhere","importance_score":0.85}]}`

	tests := append(consolidateFactClustersBasicCases(validJSON), consolidateFactClustersAdvancedCases(validJSON)...)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runConsolidateFactClustersCase(t, tc, vecs)
		})
	}
}

func TestToolHandler_MemoryStore_AdmissionGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       MemoryStoreArgs
		wantErr    bool
		wantAction string
		wantFacts  []string
	}{
		{name: "TooShortFactRejected", args: MemoryStoreArgs{FactText: "short"}, wantErr: true},
		{name: "EphemeralTelemetryRejected", args: MemoryStoreArgs{FactText: "PR #100 was merged with auto-merge armed"}, wantErr: true},
		{
			name:       "DurableTelemetryAdmitted",
			args:       MemoryStoreArgs{FactText: "Auto-merge armed must always remain active for all PRs"},
			wantAction: "inserted",
			wantFacts:  []string{"Auto-merge armed must always remain active for all PRs:1"},
		},
		// Fails because ID 999 doesn't exist, NOT the admission gate.
		{name: "SupersedesBypassesAdmissionGate", args: MemoryStoreArgs{FactText: "short", SupersedesID: 999}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newTestStore(t)
			h := NewToolHandler(&config.Config{}, s, &MockOllamaClient{embedding: testEmbedding(1.0)})
			out, err := h.MemoryStore(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MemoryStore() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if out.Action != tc.wantAction {
				t.Errorf("out.Action = %q, want %q", out.Action, tc.wantAction)
			}
			if got := factRows(t, s.DB()); !equalStrings(got, tc.wantFacts) {
				t.Errorf("facts = %q, want %q", got, tc.wantFacts)
			}
		})
	}
}

// TestWorker_RunDaily_Dreaming is serial: dreaming single-flights on the
// package-global extractionMutex.
func TestWorker_RunDaily_Dreaming(t *testing.T) {
	vecs := makePairwiseSimilarVectors(3, 0.78)
	untouched := []string{"Fact 1:1", "Fact 2:1", "Fact 3:1"}
	merged := []string{"Consolidated fact:1"}

	tests := []struct {
		name            string
		nilStore        bool
		nilOllama       bool
		nilLLM          bool
		sabotage        string
		llmErr          error
		lastDreamingAgo time.Duration
		wantFacts       []string
	}{
		{name: "DailyDecayAndDreamingSuccess", wantFacts: merged},
		{name: "RecentDreamingWatermarkSkipsDuplicateRun", lastDreamingAgo: 2 * time.Hour, wantFacts: untouched},
		{name: "ExpiredDreamingWatermarkRunsDreaming", lastDreamingAgo: 25 * time.Hour, wantFacts: merged},
		{name: "WatermarkReadErrorRunsDreaming", sabotage: sabotageWatermarks, wantFacts: merged},
		{name: "DailyDecayWithDreamingErrorHandled", llmErr: errors.New("dreaming llm error"), wantFacts: untouched},
		{name: "NilOllamaSkipsDreaming", nilOllama: true, wantFacts: untouched},
		{name: "NilLLMSkipsDreaming", nilLLM: true, wantFacts: untouched},
		{name: "NilStoreSkipsDaily", nilStore: true, wantFacts: untouched},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlStore := newTestStore(t)
			db := sqlStore.DB()
			seedFacts(t, sqlStore, []string{"Fact 1", "Fact 2", "Fact 3"}, vecs)
			if tc.lastDreamingAgo > 0 {
				at := time.Now().UTC().Add(-tc.lastDreamingAgo)
				seedWatermark(t, db, config.DefaultStack, DreamingWatermarkThreadID, 0, &at)
			}
			execSQL(t, db, tc.sabotage)

			var ollama store.OllamaClient = &MockOllamaClient{embedding: vecs[0]}
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
			var s store.Store = sqlStore
			if tc.nilStore {
				s = nil
			}

			w := NewWorker(WorkerConfig{
				Store:     s,
				Ollama:    ollama,
				LLMClient: llm,
			})

			// Tick 60 is daily decay + dreaming
			w.Tick(context.Background(), 60)

			if got := factRows(t, db); !equalStrings(got, tc.wantFacts) {
				t.Errorf("facts = %q, want %q", got, tc.wantFacts)
			}
		})
	}
}
