package main

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	_ "modernc.org/sqlite"
)

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
		ollamaErr           error
		wantWatermark       bool
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
			wantWatermark:  true,
			wantErr:        false,
		},
		{
			name:          "ContextQueryErrorIgnoredAndContinues",
			storeFailOp:   "GetThreadContextBeforeWatermark",
			wantWatermark: true,
			wantErr:       false,
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
			wantWatermark:       true,
			wantErr:             false,
		},
		{
			name:           "TrajectoryRetrievedFactsAndToolCallsInjected",
			withTrajectory: true,
			wantWatermark:  true,
			wantErr:        false,
		},
		{
			name:             "TrajectoryReinforcesFactIDs",
			withReinforceIDs: true,
			wantReinforcedID: 42,
			wantWatermark:    true,
			wantErr:          false,
		},
		{
			name:             "TrajectoryReinforceStoreErrorLoggedGracefully",
			withReinforceIDs: true,
			storeFailOp:      "ReinforceFact",
			wantWatermark:    true,
			wantErr:          false,
		},
		{
			name:          "TrajectoryQueryErrorIgnoredAndContinues",
			storeFailOp:   "GetThreadTrajectoryContext",
			wantWatermark: true,
			wantErr:       false,
		},
		{
			name:          "CandidateFactInsertStoreErrorSkipsWatermark",
			storeFailOp:   "InsertFact",
			wantWatermark: false,
			wantErr:       false,
		},
		{
			name:          "CandidateFactEmbeddingErrorSkipsWatermark",
			ollamaErr:     errors.New("embed fail"),
			wantWatermark: false,
			wantErr:       false,
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
						{RowID: 10, ThreadID: threadID, Content: "He likes matcha tea", ResponseText: "Understood", Status: "COMPLETED", IsAdmin: true},
					}
				}
				if tc.withSummary {
					mem.threadSummaries[threadID] = "User name is Bob"
				}
				if tc.withPriorTurns {
					mem.threadPriorTurns[threadID] = []Message{
						{RowID: 5, ThreadID: threadID, Content: "My name is Bob", ResponseText: "Hello Bob", Status: "COMPLETED", IsAdmin: true},
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
				client = &MockOllamaClient{embedding: baseEmb, err: tc.ollamaErr}
			}

			var capturedPrompt string
			var llmSpanID trace.SpanID
			llm := func(c context.Context, prompt string) (string, error) {
				capturedPrompt = prompt
				llmSpanID = trace.SpanContextFromContext(c).SpanID()
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
			if capturedPrompt != "" {
				name, root, attrs := endedSpanSummary(llmSpanID)
				if name != "coven.extract" || !root || attrs["aerial.coven.batch_size"] == "" {
					t.Errorf("LLM call span = %q root=%v attrs=%v, want root coven.extract with aerial.coven.batch_size", name, root, attrs)
				}
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
			if tc.wantWatermark && mem != nil {
				if mem.watermarks["th-test"] != 10 {
					t.Errorf("watermark = %d, want 10", mem.watermarks["th-test"])
				}
			} else if !tc.wantWatermark && mem != nil {
				if wm, ok := mem.watermarks["th-test"]; ok && wm != 0 {
					t.Errorf("watermark unexpectedly advanced to %d, want not advanced", wm)
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
	for i := range n {
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
	for i := range 3 {
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
		seedWatermark       int64
		seedReinforceCounts []int
		llmResp             string
		llmErr              error
		ollamaErr           error
		wantErr             bool
		wantCount           int
		wantRemaining       int
		wantReinforcedCount int
		wantWatermark       int64
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
			wantWatermark:       5,
		},
		{
			name:          "WatermarkSkipsAlreadyEvaluatedOldCliques",
			ctx:           ctx,
			addFacts:      true,
			seedWatermark: 3,
			llmResp:       validJSON,
			wantErr:       false,
			wantCount:     0,
			wantRemaining: 3,
			wantWatermark: 3,
		},
		{
			name:          "WatermarkEvaluatesCliqueWithNewFact",
			ctx:           ctx,
			addFacts:      true,
			seedWatermark: 2,
			llmResp:       validJSON,
			wantErr:       false,
			wantCount:     2,
			wantRemaining: 1,
			wantWatermark: 5,
		},
		{
			name:          "WatermarkReadErrorFallsBackToFullEvaluation",
			ctx:           ctx,
			addFacts:      true,
			storeFail:     "GetConversationFactWatermark",
			llmResp:       validJSON,
			wantErr:       false,
			wantCount:     2,
			wantRemaining: 1,
			wantWatermark: 5,
		},
		{
			name:          "WatermarkUpdateErrorLoggedGracefully",
			ctx:           ctx,
			addFacts:      true,
			storeFail:     "UpdateConversationFactWatermark",
			llmResp:       validJSON,
			wantErr:       false,
			wantCount:     2,
			wantRemaining: 1,
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
			if tc.seedWatermark > 0 {
				mem.watermarks[DreamingWatermarkThreadID] = tc.seedWatermark
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
			if tc.wantWatermark > 0 && mem.watermarks[DreamingWatermarkThreadID] != tc.wantWatermark {
				t.Errorf("dreaming watermark = %d, want %d", mem.watermarks[DreamingWatermarkThreadID], tc.wantWatermark)
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
		name            string
		nilOllama       bool
		nilLLM          bool
		llmErr          error
		lastDreamingAgo time.Duration
		dreamSuccess    bool
		wantRemaining   int
	}{
		{
			name:          "DailyDecayAndDreamingSuccess",
			dreamSuccess:  true,
			wantRemaining: 1,
		},
		{
			name:            "RecentDreamingWatermarkSkipsDuplicateRun",
			lastDreamingAgo: 2 * time.Hour,
			wantRemaining:   3,
		},
		{
			name:            "ExpiredDreamingWatermarkRunsDreaming",
			lastDreamingAgo: 25 * time.Hour,
			dreamSuccess:    true,
			wantRemaining:   1,
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
			if tc.lastDreamingAgo > 0 {
				mem.watermarkTimes[DreamingWatermarkThreadID] = time.Now().UTC().Add(-tc.lastDreamingAgo)
			}

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

			if tc.wantRemaining > 0 && len(mem.facts) != tc.wantRemaining {
				t.Errorf("len(mem.facts) = %d, want %d", len(mem.facts), tc.wantRemaining)
			}
		})
	}
}
