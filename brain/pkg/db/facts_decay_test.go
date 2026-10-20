package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

type testCustomResult struct {
	rows int64
	err  error
}

func (r testCustomResult) LastInsertId() (int64, error) { return 0, nil }
func (r testCustomResult) RowsAffected() (int64, error) { return r.rows, r.err }

type testMockDBTX struct {
	execFn     func(ctx context.Context, query string, args ...any) (sql.Result, error)
	queryRowFn func(ctx context.Context, query string, args ...any) *sql.Row
	queryFn    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (m testMockDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if m.execFn != nil {
		return m.execFn(ctx, query, args...)
	}
	return testCustomResult{rows: 1}, nil
}

func (m testMockDBTX) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return nil
}

func (m testMockDBTX) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, query, args...)
	}
	return nil, nil
}

func TestFactTimeScan(t *testing.T) {
	now := time.Date(2026, 9, 13, 22, 0, 0, 0, time.UTC)
	var nilTime *time.Time

	timeTests := []struct {
		name               string
		src                any
		wantValid, wantErr bool
		wantTime           time.Time
	}{
		{"nil", nil, false, false, time.Time{}},
		{"nil pointer", nilTime, false, false, time.Time{}},
		{"empty string", "   ", false, false, time.Time{}},
		{"empty bytes", []byte("   "), false, false, time.Time{}},
		{"valid bytes", []byte("2026-09-13T22:00:00Z"), true, false, now},
		{"valid string", "2026-09-13T22:00:00Z", true, false, now},
		{"time.Time value", now, true, false, now},
		{"invalid string", "totally invalid string", false, true, time.Time{}},
		{"invalid type int", 12345, false, true, time.Time{}},
	}
	for _, tc := range timeTests {
		t.Run("fact_time_"+tc.name, func(t *testing.T) {
			var ft FactTime
			err := ft.Scan(tc.src)
			if (err != nil) != tc.wantErr || ft.Valid != tc.wantValid || (tc.wantValid && !ft.Time.Equal(tc.wantTime)) {
				t.Fatalf("Scan(%v) err=%v wantErr=%v valid=%v wantValid=%v time=%v", tc.src, err, tc.wantErr, ft.Valid, tc.wantValid, ft.Time)
			}
		})
	}

	nvTests := []struct {
		name      string
		src       any
		wantValid bool
		wantLen   int
	}{
		{"nil", nil, false, 0},
		{"string", "[0.1,0.2]", true, 2},
		{"bytes", []byte("[0.1,0.2]"), true, 2},
	}
	for _, tc := range nvTests {
		t.Run("null_vector_"+tc.name, func(t *testing.T) {
			var nv NullVector
			if err := nv.Scan(tc.src); err != nil || nv.Valid != tc.wantValid || len(nv.Vector) != tc.wantLen {
				t.Fatalf("Scan(%v) err=%v valid=%v wantValid=%v len=%d", tc.src, err, nv.Valid, tc.wantValid, len(nv.Vector))
			}
		})
	}
}

func TestFactDecayAndReinforcement(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()

	emb1 := make([]float32, ExpectedEmbeddingDim)
	emb1[0] = 1.0

	id, err := store.InsertFact(ctx, "system_config", "Postgres runs on port 5432", 0.50, emb1)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	dup, sim, err := store.FindDuplicateFact(ctx, emb1, 0.88)
	if err != nil || dup == nil || dup.ID != id || sim < 0.99 {
		t.Fatalf("FindDuplicateFact failed: dup=%+v, sim=%f, err=%v", dup, sim, err)
	}

	embOrthogonal := make([]float32, ExpectedEmbeddingDim)
	embOrthogonal[1] = 1.0
	if dupOrth, _, err := store.FindDuplicateFact(ctx, embOrthogonal, 0.88); err != nil || dupOrth != nil {
		t.Fatalf("expected nil duplicate for orthogonal embedding, got %+v", dupOrth)
	}

	embUpdated := make([]float32, ExpectedEmbeddingDim)
	embUpdated[0], embUpdated[1] = 0.95, 0.05
	if err := store.ReinforceFact(ctx, id, "Postgres runs on port 5433", embUpdated, 0.15); err != nil {
		t.Fatalf("ReinforceFact failed: %v", err)
	}

	factsRes, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil || len(factsRes.Facts) != 1 {
		t.Fatalf("GetFactsPaginated failed: len=%d, err=%v", len(factsRes.Facts), err)
	}
	f := factsRes.Facts[0]
	if f.FactText != "Postgres runs on port 5432" || f.Importance < 0.70 || f.ReinforceCount != 2 {
		t.Errorf("unexpected reinforced fact: %+v", f)
	}

	_ = store.ReinforceFact(ctx, id, "", nil, 0.50)
	factsRes2, _ := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if factsRes2.Facts[0].Importance != 1.0 {
		t.Errorf("expected importance 1.0, got %f", factsRes2.Facts[0].Importance)
	}

	store.mu.Lock()
	store.facts[id].Fact.Importance = 0.80
	store.mu.Unlock()

	if decayed, pruned, err := store.DecayAndPruneFacts(ctx, 0.02, 0.10, 30); err != nil || decayed != 1 || pruned != 0 {
		t.Fatalf("DecayAndPruneFacts failed: decayed=%d, pruned=%d, err=%v", decayed, pruned, err)
	}

	store.mu.Lock()
	store.facts[id].Fact.Importance = 0.05
	store.facts[id].Fact.CreatedAt = time.Now().UTC().AddDate(0, 0, -40)
	store.mu.Unlock()

	if _, prunedFinal, err := store.DecayAndPruneFacts(ctx, 0.02, 0.10, 30); err != nil || prunedFinal != 1 {
		t.Fatalf("DecayAndPruneFacts prune failed: %v", err)
	}
	factsResFinal, _ := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if len(factsResFinal.Facts) != 0 {
		t.Errorf("expected 0 facts remaining after prune, got %d", len(factsResFinal.Facts))
	}

	storeMem := NewSQLStore(initTestSQLiteDB(t))
	if dup, sim, err := storeMem.FindDuplicateFact(ctx, []float32{1.0}, 0.88); err != nil || dup != nil || sim != 0 {
		t.Errorf("expected nil duplicate for invalid embedding dimension")
	}
	_, _, _ = storeMem.FindDuplicateFact(nil, emb1, -1.0)
	_ = storeMem.UpdateConversationFactWatermark(ctx, "", 10)
	_ = storeMem.ReinforceFact(nil, 9999, "text", emb1, -0.5)
	_, _, _ = storeMem.DecayAndPruneFacts(nil, -1, -1, -1)

	closedStore := &SQLStore{db: initTestSQLiteDB(t)}
	_ = closedStore.Close()
	for _, s := range []*SQLStore{nil, {db: nil}, closedStore} {
		if _, _, err := s.FindDuplicateFact(ctx, emb1, 0.88); err == nil {
			t.Errorf("expected error on s.FindDuplicateFact")
		}
		if err := s.ReinforceFact(ctx, 1, "text", emb1, 0.15); err == nil {
			t.Errorf("expected error on s.ReinforceFact")
		}
		if _, _, err := s.DecayAndPruneFacts(ctx, 0.02, 0.10, 30); err == nil {
			t.Errorf("expected error on s.DecayAndPruneFacts")
		}
	}
}

func TestFactPostgresQueries(t *testing.T) {
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0
	dbMem := initTestSQLiteDB(t)

	findDupTests := []struct {
		name, querySQL   string
		queryErr         error
		wantNil, wantErr bool
		wantID           int64
		wantSim          float64
	}{
		{name: "no rows", querySQL: "SELECT 1 WHERE 1 = 0", wantNil: true},
		{name: "query error", queryErr: errors.New("pg err"), wantErr: true},
		{name: "matched above min", querySQL: "SELECT 42, 'cat', 'fact', 0.9, '2026-09-13 22:00:00', '2026-09-13 22:00:00', '2026-09-13 22:00:00', 2, 0.95", wantID: 42, wantSim: 0.95},
		{name: "matched below min", querySQL: "SELECT 43, 'cat', 'fact', 0.9, '2026-09-13 22:00:00', '2026-09-13 22:00:00', '2026-09-13 22:00:00', 1, 0.50", wantNil: true, wantSim: 0.50},
	}
	for _, tc := range findDupTests {
		t.Run("find_dup_"+tc.name, func(t *testing.T) {
			mock := testMockDBTX{queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
				if tc.queryErr != nil {
					return dbMem.QueryRowContext(ctx, "SELECT syntax error")
				}
				return dbMem.QueryRowContext(ctx, tc.querySQL)
			}}
			dup, sim, err := (&SQLStore{db: mock, isPostgres: true}).FindDuplicateFact(ctx, emb, 0.88)
			if (err != nil) != tc.wantErr || (tc.wantNil && dup != nil) || (tc.wantID != 0 && (dup == nil || dup.ID != tc.wantID)) || (tc.wantSim != 0 && sim != tc.wantSim) {
				t.Fatalf("FindDuplicateFact failed: dup=%+v, sim=%f, err=%v", dup, sim, err)
			}
		})
	}

	reinforceTests := []struct {
		name, text       string
		factID, rows     int64
		execErr, wantErr error
	}{
		{name: "success text", factID: 10, text: "Postgres Fact", rows: 1},
		{name: "success boost", factID: 10, rows: 1},
		{name: "not found", factID: 999, rows: 0, wantErr: ErrFactNotFound},
		{name: "exec error", factID: 10, execErr: errors.New("pg exec failure"), wantErr: errors.New("failed to reinforce fact")},
	}
	for _, tc := range reinforceTests {
		t.Run("reinforce_"+tc.name, func(t *testing.T) {
			mock := testMockDBTX{execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
				if tc.execErr != nil {
					return nil, tc.execErr
				}
				return testCustomResult{rows: tc.rows}, nil
			}}
			err := (&SQLStore{db: mock, isPostgres: true}).ReinforceFact(ctx, tc.factID, tc.text, emb, 0.15)
			if tc.wantErr != nil && (err == nil || !strings.Contains(err.Error(), tc.wantErr.Error())) {
				t.Fatalf("expected error containing %v, got %v", tc.wantErr, err)
			} else if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	decayTests := []struct {
		name                    string
		execErrStep             int
		wantDecayed, wantPruned int64
		wantErr                 bool
	}{
		{name: "success", wantDecayed: 1, wantPruned: 1},
		{name: "decay failure", execErrStep: 1, wantErr: true},
		{name: "prune failure", execErrStep: 2, wantDecayed: 3, wantErr: true},
	}
	for _, tc := range decayTests {
		t.Run("decay_"+tc.name, func(t *testing.T) {
			calls := 0
			mock := testMockDBTX{execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
				calls++
				if tc.execErrStep == calls {
					return nil, errors.New("pg step error")
				}
				rows := int64(1)
				if calls == 1 && tc.execErrStep == 2 {
					rows = 3
				}
				return testCustomResult{rows: rows}, nil
			}}
			decayed, pruned, err := (&SQLStore{db: mock, isPostgres: true}).DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
			if (err != nil) != tc.wantErr || (tc.wantDecayed != 0 && decayed != tc.wantDecayed) || (tc.wantPruned != 0 && pruned != tc.wantPruned) {
				t.Fatalf("DecayAndPruneFacts failed: decayed=%d pruned=%d err=%v", decayed, pruned, err)
			}
		})
	}

	for _, isTurns := range []bool{false, true} {
		name := "active conversations extraction error"
		if isTurns {
			name = "unextracted admin turns error"
		}
		t.Run(name, func(t *testing.T) {
			store := &SQLStore{db: testMockDBTX{queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				return nil, errors.New("pg query error")
			}}, isPostgres: true}
			var err error
			if isTurns {
				_, err = store.GetUnextractedAdminTurns(ctx, "th-pg")
			} else {
				_, err = store.GetActiveConversationsForExtraction(ctx, 24)
			}
			if err == nil {
				t.Errorf("expected error on query failure")
			}
		})
	}
}

func TestSearchSimilarFacts(t *testing.T) {
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0
	dbMem := initTestSQLiteDB(t)

	if resNil, errNil := (&SQLStore{db: dbMem}).SearchSimilarFacts(ctx, nil, "", 10, 0.2); resNil != nil || errNil != nil {
		t.Errorf("expected nil, nil for empty embedding and queryText, got %v, %v", resNil, errNil)
	}

	fakeStore := NewFakeStore()
	if _, err := fakeStore.InsertFact(ctx, "pref", "User loves matcha latte", 1.0, emb); err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	embOther := make([]float32, ExpectedEmbeddingDim)
	embOther[1] = 1.0
	if _, err := fakeStore.InsertFact(ctx, "pref", "User hates bitter coffee", 0.8, embOther); err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	if hybridRes, err := fakeStore.SearchSimilarFacts(ctx, emb, "matcha", 10, 0.2); err != nil || len(hybridRes) != 1 || hybridRes[0].FactText != "User loves matcha latte" {
		t.Fatalf("unexpected hybrid match: %+v, err: %v", hybridRes, err)
	}
	if sparseRes, err := fakeStore.SearchSimilarFacts(ctx, nil, "bitter coffee", 10, 0.2); err != nil || len(sparseRes) != 1 || sparseRes[0].FactText != "User hates bitter coffee" {
		t.Fatalf("unexpected sparse match: %+v, err: %v", sparseRes, err)
	}

	queryTests := []struct {
		name, queryText, wantErrMsg string
		emb                         []float32
		wantPatterns                []string
		queryErr                    error
	}{
		{name: "hybrid query", emb: emb, queryText: "search text", wantPatterns: []string{"vector_hits", "text_hits", "FULL OUTER JOIN"}},
		{name: "dense query", emb: emb, wantPatterns: []string{"WITH candidates AS", "similarity * importance"}},
		{name: "sparse query", queryText: "only text", wantPatterns: []string{"WITH text_hits AS", "websearch_to_tsquery"}},
		{name: "query error", emb: emb, queryText: "test", queryErr: errors.New("simulated pg query failure"), wantErrMsg: "hybrid search failed"},
	}
	for _, tc := range queryTests {
		t.Run("pg_query_"+tc.name, func(t *testing.T) {
			var capturedQuery string
			mock := testMockDBTX{queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				capturedQuery = query
				if tc.queryErr != nil {
					return nil, tc.queryErr
				}
				return dbMem.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
			}}
			_, err := (&SQLStore{db: mock, isPostgres: true}).SearchSimilarFacts(ctx, tc.emb, tc.queryText, 10, 0.2)
			if tc.wantErrMsg != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrMsg)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrMsg, err)
			} else if tc.wantErrMsg == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, pat := range tc.wantPatterns {
				if !strings.Contains(capturedQuery, pat) {
					t.Errorf("expected query containing %q, got: %s", pat, capturedQuery)
				}
			}
		})
	}
}
