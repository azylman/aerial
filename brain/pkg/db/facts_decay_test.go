package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFactReinforceAndDecay(t *testing.T) {
	store := NewFakeStore()
	ctx := context.Background()

	emb1 := make([]float32, ExpectedEmbeddingDim)
	emb1[0] = 1.0 // normalized vector along dim 0

	// 1. Insert initial fact
	id, err := store.InsertFact(ctx, "system_config", "Postgres runs on port 5432", 0.50, emb1)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	// 2. FindDuplicateFact with matching embedding
	dup, sim, err := store.FindDuplicateFact(ctx, emb1, 0.88)
	if err != nil {
		t.Fatalf("FindDuplicateFact failed: %v", err)
	}
	if dup == nil || dup.ID != id {
		t.Fatalf("expected duplicate fact id=%d, got %+v", id, dup)
	}
	if sim < 0.99 {
		t.Errorf("expected similarity near 1.0, got %f", sim)
	}

	// 3. FindDuplicateFact with orthogonal embedding (< minSim)
	embOrthogonal := make([]float32, ExpectedEmbeddingDim)
	embOrthogonal[1] = 1.0
	dupOrth, simOrth, err := store.FindDuplicateFact(ctx, embOrthogonal, 0.88)
	if err != nil {
		t.Fatalf("FindDuplicateFact orthogonal failed: %v", err)
	}
	if dupOrth != nil {
		t.Errorf("expected nil duplicate for orthogonal embedding, got %+v (sim=%f)", dupOrth, simOrth)
	}

	// 4. ReinforceFact with updated text & embedding
	embUpdated := make([]float32, ExpectedEmbeddingDim)
	embUpdated[0] = 0.95
	embUpdated[1] = 0.05
	err = store.ReinforceFact(ctx, id, "Postgres runs on port 5433", embUpdated, 0.15)
	if err != nil {
		t.Fatalf("ReinforceFact failed: %v", err)
	}

	// Verify reinforced fact has boosted importance (floored at 0.70), updated text, and incremented reinforce_count
	factsRes, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil || len(factsRes.Facts) != 1 {
		t.Fatalf("GetFactsPaginated failed: len=%d, err=%v", len(factsRes.Facts), err)
	}
	f := factsRes.Facts[0]
	if f.FactText != "Postgres runs on port 5433" {
		t.Errorf("expected updated text, got %q", f.FactText)
	}
	if f.Importance < 0.70 {
		t.Errorf("expected importance floored at >= 0.70, got %f", f.Importance)
	}
	if f.ReinforceCount != 2 {
		t.Errorf("expected reinforce_count=2, got %d", f.ReinforceCount)
	}

	// 5. Reinforce to ceiling (1.0)
	_ = store.ReinforceFact(ctx, id, "", nil, 0.50)
	factsRes2, _ := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if factsRes2.Facts[0].Importance != 1.0 {
		t.Errorf("expected importance clamped to 1.0, got %f", factsRes2.Facts[0].Importance)
	}

	// 6. DecayAndPruneFacts: set importance to 0.80
	store.mu.Lock()
	store.facts[id].Fact.Importance = 0.80
	store.mu.Unlock()

	// Run decay: should decrement by 0.02
	decayed, pruned, err := store.DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("DecayAndPruneFacts failed: %v", err)
	}
	if decayed != 1 {
		t.Errorf("expected 1 decayed fact, got %d", decayed)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned facts, got %d", pruned)
	}

	factsRes3, _ := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if factsRes3.Facts[0].Importance > 0.79 || factsRes3.Facts[0].Importance < 0.77 {
		t.Errorf("expected importance ~0.78 after decay, got %f", factsRes3.Facts[0].Importance)
	}

	// 7. Pruning test: set fact to importance 0.05 and CreatedAt to 40 days ago
	fortyDaysAgo := time.Now().UTC().AddDate(0, 0, -40)
	store.mu.Lock()
	store.facts[id].Fact.Importance = 0.05
	store.facts[id].Fact.CreatedAt = fortyDaysAgo
	store.mu.Unlock()

	_, prunedFinal, err := store.DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("DecayAndPruneFacts prune failed: %v", err)
	}
	if prunedFinal != 1 {
		t.Errorf("expected 1 pruned fact, got %d", prunedFinal)
	}

	factsResFinal, _ := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if len(factsResFinal.Facts) != 0 {
		t.Errorf("expected 0 facts remaining after prune, got %d", len(factsResFinal.Facts))
	}
}

func TestSQLStoreReinforceAndDecay(t *testing.T) {
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	mockDB := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return testCustomResult{rows: 1}, nil
		},
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return database.QueryRowContext(ctx, "SELECT 42, 'routine', 'Daily backup at midnight', 0.60, '2026-10-01 12:00:00', '2026-10-01 12:00:00', '2026-10-01 12:00:00', 1, 0.95")
		},
	}
	store := &SQLStore{db: mockDB, isPostgres: true}

	dup, sim, err := store.FindDuplicateFact(ctx, emb, 0.88)
	if err != nil || dup == nil || dup.ID != 42 {
		t.Fatalf("store.FindDuplicateFact failed: dup=%+v, sim=%f, err=%v", dup, sim, err)
	}

	err = store.ReinforceFact(ctx, 42, "Daily backup at 1:00 AM", emb, 0.15)
	if err != nil {
		t.Fatalf("store.ReinforceFact failed: %v", err)
	}

	decayed, pruned, err := store.DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("store.DecayAndPruneFacts failed: %v", err)
	}
	_ = decayed
	_ = pruned

	// Reinforce on non-existent fact (0 rows affected) should return ErrFactNotFound
	mockDBZero := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return testCustomResult{rows: 0}, nil
		},
	}
	storeZero := &SQLStore{db: mockDBZero, isPostgres: true}
	errNotFound := storeZero.ReinforceFact(ctx, 999999, "missing", emb, 0.15)
	if !errors.Is(errNotFound, ErrFactNotFound) {
		t.Errorf("expected ErrFactNotFound, got %v", errNotFound)
	}

	// Nil receiver and nil DB branches
	var nilStore *SQLStore
	if _, _, err := nilStore.FindDuplicateFact(ctx, emb, 0.88); err == nil {
		t.Errorf("expected error on nilStore.FindDuplicateFact")
	}
	if err := nilStore.ReinforceFact(ctx, 1, "text", emb, 0.15); err == nil {
		t.Errorf("expected error on nilStore.ReinforceFact")
	}
	if _, _, err := nilStore.DecayAndPruneFacts(ctx, 0.02, 0.10, 30); err == nil {
		t.Errorf("expected error on nilStore.DecayAndPruneFacts")
	}

	storeNilDB := &SQLStore{db: nil}
	if _, _, err := storeNilDB.FindDuplicateFact(ctx, emb, 0.88); err == nil {
		t.Errorf("expected error on storeNilDB.FindDuplicateFact")
	}
	if err := storeNilDB.ReinforceFact(ctx, 1, "text", emb, 0.15); err == nil {
		t.Errorf("expected error on storeNilDB.ReinforceFact")
	}
	if _, _, err := storeNilDB.DecayAndPruneFacts(ctx, 0.02, 0.10, 30); err == nil {
		t.Errorf("expected error on storeNilDB.DecayAndPruneFacts")
	}

	storeMem := NewSQLStore(database)
	dup, sim, err = storeMem.FindDuplicateFact(ctx, []float32{1.0}, 0.88)
	if err != nil || dup != nil || sim != 0 {
		t.Errorf("expected nil duplicate without error for invalid embedding dimension")
	}

	// minSim <= 0 defaults to 0.88
	_, _, _ = storeMem.FindDuplicateFact(nil, emb, -1.0)
	_ = storeMem.UpdateConversationFactWatermark(ctx, "", 10)

	// boost <= 0 and ctx == nil
	_ = storeMem.ReinforceFact(nil, 9999, "text", emb, -0.5)

	// default arguments and ctx == nil
	_, _, _ = storeMem.DecayAndPruneFacts(nil, -1, -1, -1)

	// Closed DB
	dbClosed, err := InitDB(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = dbClosed.Close()
	closedStore := NewSQLStore(dbClosed)

	_, _, err = closedStore.DecayAndPruneFacts(context.Background(), 0.02, 0.10, 30)
	if err == nil {
		t.Errorf("expected error on closed db for DecayAndPruneFacts")
	}

	err = closedStore.ReinforceFact(context.Background(), 1, "text", emb, 0.15)
	if err == nil {
		t.Errorf("expected error on closed db for ReinforceFact")
	}
}

func TestFactTime_ScanAllBranches(t *testing.T) {
	// 1. Nil & Empty
	var ft FactTime
	if err := ft.Scan(nil); err != nil || ft.Valid {
		t.Errorf("expected valid=false for nil, got err=%v valid=%v", err, ft.Valid)
	}
	var nilTime *time.Time
	if err := ft.Scan(nilTime); err != nil || ft.Valid {
		t.Errorf("expected valid=false for nil *time.Time")
	}
	if err := ft.Scan("   "); err != nil || ft.Valid {
		t.Errorf("expected valid=false for whitespace string")
	}
	if err := ft.Scan([]byte("   ")); err != nil || ft.Valid {
		t.Errorf("expected valid=false for whitespace []byte")
	}
	if err := ft.Scan([]byte("2026-09-13T22:00:00Z")); err != nil || !ft.Valid {
		t.Errorf("expected valid=true for []byte date string")
	}

	// NullVector branches
	var nv NullVector
	if err := nv.Scan(nil); err != nil || nv.Valid {
		t.Errorf("expected valid=false for nil NullVector")
	}
	if err := nv.Scan("[0.1,0.2]"); err != nil || !nv.Valid {
		t.Errorf("expected valid=true for string NullVector")
	}
	if err := nv.Scan([]byte("[0.1,0.2]")); err != nil || !nv.Valid {
		t.Errorf("expected valid=true for []byte NullVector")
	}

	// 2. time.Time
	now := time.Now().UTC()
	if err := ft.Scan(now); err != nil || !ft.Valid || !ft.Time.Equal(now) {
		t.Errorf("expected time.Time match, got err=%v valid=%v time=%v", err, ft.Valid, ft.Time)
	}

	// 3. Valid string
	if err := ft.Scan("2026-09-13T22:00:00Z"); err != nil || !ft.Valid {
		t.Errorf("expected valid time for string, got err=%v", err)
	}

	// 4. Invalid string and invalid type error
	if err := ft.Scan("totally invalid string"); err == nil {
		t.Errorf("expected error on invalid string scan")
	}
	if err := ft.Scan(12345); err == nil {
		t.Errorf("expected error on int scan")
	}
}

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

func TestFacts_PostgresBranchCoverage(t *testing.T) {
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// 1. ReinforceFactWithContext on Postgres
	// a. Success with text
	mockDB := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return testCustomResult{rows: 1}, nil
		},
	}
	err := (&SQLStore{db: mockDB, isPostgres: true}).ReinforceFact(ctx, 10, "Postgres Fact", emb, 0.15)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// b. Success without text (boost only)
	err = (&SQLStore{db: mockDB, isPostgres: true}).ReinforceFact(ctx, 10, "", nil, 0.15)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// c. 0 rows affected -> ErrFactNotFound
	mockDBZero := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return testCustomResult{rows: 0}, nil
		},
	}
	err = (&SQLStore{db: mockDBZero, isPostgres: true}).ReinforceFact(ctx, 999, "", nil, 0.15)
	if !errors.Is(err, ErrFactNotFound) {
		t.Errorf("expected ErrFactNotFound, got %v", err)
	}

	// d. Exec error
	mockDBErr := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, fmt.Errorf("postgres exec error")
		},
	}
	err = (&SQLStore{db: mockDBErr, isPostgres: true}).ReinforceFact(ctx, 10, "", nil, 0.15)
	if err == nil {
		t.Errorf("expected error on postgres exec failure")
	}

	// 2. DecayAndPruneFactsWithContext on Postgres
	// a. Success
	decayed, pruned, err := (&SQLStore{db: mockDB, isPostgres: true}).DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err != nil || decayed != 1 || pruned != 1 {
		t.Errorf("expected decayed=1 pruned=1, got decayed=%d pruned=%d err=%v", decayed, pruned, err)
	}

	// b. Decay error
	decayed, pruned, err = (&SQLStore{db: mockDBErr, isPostgres: true}).DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err == nil {
		t.Errorf("expected error on postgres decay failure")
	}

	// c. Prune error (decay succeeds, prune fails)
	calls := 0
	mockDBPruneErr := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			calls++
			if calls == 1 {
				return testCustomResult{rows: 3}, nil
			}
			return nil, fmt.Errorf("prune error")
		},
	}
	decayed, pruned, err = (&SQLStore{db: mockDBPruneErr, isPostgres: true}).DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err == nil || decayed != 3 {
		t.Errorf("expected decayed=3 with prune error, got decayed=%d err=%v", decayed, err)
	}
}

func TestFacts_Postgres_FindDuplicateFactCoverage(t *testing.T) {
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// 1. sql.ErrNoRows branch
	mockNoRows := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT 1 WHERE 1 = 0")
		},
	}
	dup, sim, err := (&SQLStore{db: mockNoRows, isPostgres: true}).FindDuplicateFact(ctx, emb, 0.88)
	if err != nil || dup != nil || sim != 0 {
		t.Errorf("expected nil duplicate for sql.ErrNoRows, got dup=%v sim=%f err=%v", dup, sim, err)
	}

	// 2. Query error branch
	mockErr := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT syntax error")
		},
	}
	_, _, err = (&SQLStore{db: mockErr, isPostgres: true}).FindDuplicateFact(ctx, emb, 0.88)
	if err == nil {
		t.Errorf("expected error on query failure")
	}

	// 3. Match found with sim >= minSim
	mockMatch := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT 42, 'cat', 'fact', 0.9, '2026-09-13 22:00:00', '2026-09-13 22:00:00', '2026-09-13 22:00:00', 2, 0.95")
		},
	}
	dup, sim, err = (&SQLStore{db: mockMatch, isPostgres: true}).FindDuplicateFact(ctx, emb, 0.88)
	if err != nil || dup == nil || dup.ID != 42 || sim != 0.95 {
		t.Errorf("expected matched fact id=42 sim=0.95, got dup=%+v sim=%f err=%v", dup, sim, err)
	}

	// 4. Match found with sim < minSim
	mockLowSim := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT 43, 'cat', 'fact', 0.9, '2026-09-13 22:00:00', '2026-09-13 22:00:00', '2026-09-13 22:00:00', 1, 0.50")
		},
	}
	dup, sim, err = (&SQLStore{db: mockLowSim, isPostgres: true}).FindDuplicateFact(ctx, emb, 0.88)
	if err != nil || dup != nil || sim != 0.50 {
		t.Errorf("expected nil duplicate with sim=0.50, got dup=%v sim=%f err=%v", dup, sim, err)
	}
}

func TestSearchSimilarFacts_HybridAndPostgresBranches(t *testing.T) {
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// 1. Nil embedding and empty queryText returns nil
	resNil, errNil := (&SQLStore{db: dbMem}).SearchSimilarFacts(ctx, nil, "", 10, 0.2)
	if resNil != nil || errNil != nil {
		t.Errorf("expected nil, nil for empty embedding and queryText, got %v, %v", resNil, errNil)
	}

	// 2. Insert facts for in-memory hybrid evaluation using FakeStore
	fakeStore := NewFakeStore()
	_, err = fakeStore.InsertFact(ctx, "pref", "User loves matcha latte", 1.0, emb)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}
	embOther := make([]float32, ExpectedEmbeddingDim)
	embOther[1] = 1.0
	_, err = fakeStore.InsertFact(ctx, "pref", "User hates bitter coffee", 0.8, embOther)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	// Test hybrid: matches both embedding and text query
	hybridRes, err := fakeStore.SearchSimilarFacts(ctx, emb, "matcha", 10, 0.2)
	if err != nil || len(hybridRes) != 1 {
		t.Fatalf("expected 1 hybrid match, got %d, err: %v", len(hybridRes), err)
	}
	if hybridRes[0].FactText != "User loves matcha latte" {
		t.Errorf("expected 'User loves matcha latte', got %q", hybridRes[0].FactText)
	}

	// Test sparse only (nil embedding)
	sparseRes, err := fakeStore.SearchSimilarFacts(ctx, nil, "bitter coffee", 10, 0.2)
	if err != nil || len(sparseRes) != 1 {
		t.Fatalf("expected 1 sparse match, got %d, err: %v", len(sparseRes), err)
	}
	if sparseRes[0].FactText != "User hates bitter coffee" {
		t.Errorf("expected 'User hates bitter coffee', got %q", sparseRes[0].FactText)
	}

	// 3. Test Postgres query generation branches using testMockDBTX
	// a. Hybrid branch (both embedding and queryText)
	var capturedQuery string
	mockHybrid := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
		},
	}
	_, _ = (&SQLStore{db: mockHybrid, isPostgres: true}).SearchSimilarFacts(ctx, emb, "search text", 10, 0.2)
	if !strings.Contains(capturedQuery, "vector_hits") || !strings.Contains(capturedQuery, "text_hits") || !strings.Contains(capturedQuery, "FULL OUTER JOIN") {
		t.Errorf("expected hybrid RRF query with vector_hits and text_hits, got: %s", capturedQuery)
	}

	// b. Dense-only branch (has embedding, empty queryText)
	capturedQuery = ""
	mockDense := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
		},
	}
	_, _ = (&SQLStore{db: mockDense, isPostgres: true}).SearchSimilarFacts(ctx, emb, "", 10, 0.2)
	if !strings.Contains(capturedQuery, "WITH candidates AS") || !strings.Contains(capturedQuery, "similarity * importance") {
		t.Errorf("expected dense-only query with candidates CTE, got: %s", capturedQuery)
	}

	// c. Sparse-only branch (no embedding, has queryText)
	capturedQuery = ""
	mockSparse := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			capturedQuery = query
			return dbMem.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
		},
	}
	_, _ = (&SQLStore{db: mockSparse, isPostgres: true}).SearchSimilarFacts(ctx, nil, "only text", 10, 0.2)
	if !strings.Contains(capturedQuery, "WITH text_hits AS") || !strings.Contains(capturedQuery, "websearch_to_tsquery") {
		t.Errorf("expected sparse-only query with text_hits CTE, got: %s", capturedQuery)
	}

	// d. Postgres query failure error propagation
	mockErr := testMockDBTX{
		queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("simulated pg query failure")
		},
	}
	_, err = (&SQLStore{db: mockErr, isPostgres: true}).SearchSimilarFacts(ctx, emb, "test", 10, 0.2)
	if err == nil || !strings.Contains(err.Error(), "hybrid search failed") {
		t.Errorf("expected 'hybrid search failed' error, got %v", err)
	}
}



