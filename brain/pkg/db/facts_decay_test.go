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
	database, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	emb1 := make([]float32, ExpectedEmbeddingDim)
	emb1[0] = 1.0 // normalized vector along dim 0

	// 1. Insert initial fact
	id, err := InsertFact(database, "system_config", "Postgres runs on port 5432", 0.50, "th-1", emb1)
	if err != nil {
		t.Fatalf("InsertFact failed: %v", err)
	}

	// 2. FindDuplicateFact with matching embedding
	dup, sim, err := FindDuplicateFact(database, emb1, 0.88)
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
	dupOrth, simOrth, err := FindDuplicateFact(database, embOrthogonal, 0.88)
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
	err = ReinforceFact(database, id, "Postgres runs on port 5433", embUpdated, 0.15)
	if err != nil {
		t.Fatalf("ReinforceFact failed: %v", err)
	}

	// Verify reinforced fact has boosted importance (floored at 0.70), updated text, and incremented reinforce_count
	factsRes, err := GetFactsPaginated(database, FactsFilter{Limit: 10})
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
	_ = ReinforceFact(database, id, "", nil, 0.50)
	factsRes2, _ := GetFactsPaginated(database, FactsFilter{Limit: 10})
	if factsRes2.Facts[0].Importance != 1.0 {
		t.Errorf("expected importance clamped to 1.0, got %f", factsRes2.Facts[0].Importance)
	}

	// 6. DecayAndPruneFacts: artificially set last_reinforced_at and last_decayed_at to 2 days ago
	twoDaysAgo := time.Now().UTC().Add(-48 * time.Hour).Format("2006-01-02 15:04:05")
	_, err = database.Exec("UPDATE facts SET last_reinforced_at = ?, last_decayed_at = ?, importance = 0.80 WHERE id = ?", twoDaysAgo, twoDaysAgo, id)
	if err != nil {
		t.Fatalf("failed to age fact: %v", err)
	}

	// Run decay: should decrement by 0.02
	decayed, pruned, err := DecayAndPruneFacts(database, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("DecayAndPruneFacts failed: %v", err)
	}
	if decayed != 1 {
		t.Errorf("expected 1 decayed fact, got %d", decayed)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned facts, got %d", pruned)
	}

	factsRes3, _ := GetFactsPaginated(database, FactsFilter{Limit: 10})
	if factsRes3.Facts[0].Importance > 0.79 || factsRes3.Facts[0].Importance < 0.77 {
		t.Errorf("expected importance ~0.78 after decay, got %f", factsRes3.Facts[0].Importance)
	}

	// 7. Idempotency test: immediately running decay again should decay 0 facts because last_decayed_at was just updated to now!
	decayedAgain, _, err := DecayAndPruneFacts(database, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("consecutive DecayAndPruneFacts failed: %v", err)
	}
	if decayedAgain != 0 {
		t.Errorf("expected 0 decayed facts on immediate second run, got %d", decayedAgain)
	}

	// 8. Pruning test: set fact to importance 0.05 and last_reinforced_at to 40 days ago
	fortyDaysAgo := time.Now().UTC().AddDate(0, 0, -40).Format("2006-01-02 15:04:05")
	_, _ = database.Exec("UPDATE facts SET importance = 0.05, last_reinforced_at = ?, last_decayed_at = ? WHERE id = ?", fortyDaysAgo, fortyDaysAgo, id)
	_, prunedFinal, err := DecayAndPruneFacts(database, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("DecayAndPruneFacts prune failed: %v", err)
	}
	if prunedFinal != 1 {
		t.Errorf("expected 1 pruned fact, got %d", prunedFinal)
	}

	factsResFinal, _ := GetFactsPaginated(database, FactsFilter{Limit: 10})
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

	store := NewSQLStore(database)
	ctx := context.Background()

	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	id, err := store.InsertFact(ctx, "routine", "Daily backup at midnight", 0.60, "th-daily", emb)
	if err != nil {
		t.Fatalf("store.InsertFact failed: %v", err)
	}

	dup, sim, err := store.FindDuplicateFact(ctx, emb, 0.88)
	if err != nil || dup == nil || dup.ID != id {
		t.Fatalf("store.FindDuplicateFact failed: dup=%+v, sim=%f, err=%v", dup, sim, err)
	}

	err = store.ReinforceFact(ctx, id, "Daily backup at 1:00 AM", emb, 0.15)
	if err != nil {
		t.Fatalf("store.ReinforceFact failed: %v", err)
	}

	decayed, pruned, err := store.DecayAndPruneFacts(ctx, 0.02, 0.10, 30)
	if err != nil {
		t.Fatalf("store.DecayAndPruneFacts failed: %v", err)
	}
	_ = decayed
	_ = pruned

	// Reinforce on non-existent fact should return ErrFactNotFound
	errNotFound := store.ReinforceFact(ctx, 999999, "missing", emb, 0.15)
	if !errors.Is(errNotFound, ErrFactNotFound) && !strings.Contains(errNotFound.Error(), "fact not found") {
		t.Errorf("expected ErrFactNotFound, got %v", errNotFound)
	}
}

func TestFactTime_ScanAllBranches(t *testing.T) {
	// 1. Nil
	var ft FactTime
	if err := ft.Scan(nil); err != nil || ft.Valid {
		t.Errorf("expected valid=false for nil, got err=%v valid=%v", err, ft.Valid)
	}

	// 2. time.Time
	now := time.Now().UTC()
	if err := ft.Scan(now); err != nil || !ft.Valid || !ft.Time.Equal(now) {
		t.Errorf("expected time.Time match, got err=%v valid=%v time=%v", err, ft.Valid, ft.Time)
	}

	// 3. String layouts
	layouts := []string{
		"2026-09-13 22:00:00.123456789 -0700 MST",
		"2026-09-13 22:00:00.123456789 -0700 -0700",
		"2026-09-13 22:00:00 -0700 MST",
		"2026-09-13T22:00:00.123456789Z",
		"2026-09-13T22:00:00Z",
		"2026-09-13 22:00:00",
		"2026-09-13 22:00:00.123456789-07:00",
		"2026-09-13 22:00:00-07:00",
		"2026-09-13 22:00:00.123456789",
		"2026-09-13 22:00:00",
		"2026-09-13T22:00:00Z",
		"2026-09-13T22:00:00",
	}
	for _, l := range layouts {
		var f FactTime
		if err := f.Scan(l); err != nil || !f.Valid {
			t.Errorf("failed to scan layout %q: %v", l, err)
		}
	}

	// 4. Monotonic clock string
	monotonicStr := "2026-09-13 22:00:00 -0700 MST m=+0.001234567"
	var ftMono FactTime
	if err := ftMono.Scan(monotonicStr); err != nil || !ftMono.Valid {
		t.Errorf("failed to scan monotonic string %q: %v", monotonicStr, err)
	}

	monotonicNano := "2026-09-13 22:00:00.123456789 -0700 MST m=+0.001234567"
	if err := ftMono.Scan(monotonicNano); err != nil || !ftMono.Valid {
		t.Errorf("failed to scan monotonic nano string %q: %v", monotonicNano, err)
	}

	// Invalid monotonic string
	var ftInvMono FactTime
	if err := ftInvMono.Scan("invalid date m=+0.123"); err == nil {
		t.Errorf("expected error for invalid monotonic string")
	}

	// 5. Byte slice
	var ftBytes FactTime
	if err := ftBytes.Scan([]byte("2026-09-13 22:00:00")); err != nil || !ftBytes.Valid {
		t.Errorf("failed to scan bytes: %v", err)
	}

	// 6. Unparseable string
	var ftErr FactTime
	if err := ftErr.Scan("totally invalid string"); err == nil {
		t.Errorf("expected error on invalid string scan")
	}

	// 7. Unsupported type
	if err := ftErr.Scan(12345); err == nil {
		t.Errorf("expected error on int scan")
	}
}

func TestFacts_DecayAndReinforce_NilAndEdgeCases(t *testing.T) {
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// 1. FindDuplicateFactWithContext nil DB
	dup, sim, err := FindDuplicateFactWithContext(nil, nil, false, emb, 0.88)
	if err != nil || dup != nil || sim != 0 {
		t.Errorf("expected nil duplicate without error for nil database")
	}

	// Invalid embedding dimension
	dbMem, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbMem.Close()

	dup, sim, err = FindDuplicateFactWithContext(context.Background(), dbMem, false, []float32{1.0}, 0.88)
	if err != nil || dup != nil || sim != 0 {
		t.Errorf("expected nil duplicate without error for invalid embedding dimension")
	}

	// minSim <= 0 defaults to 0.88
	_, _, _ = FindDuplicateFactWithContext(nil, dbMem, false, emb, -1.0)

	// 2. ReinforceFactWithContext nil DB
	err = ReinforceFactWithContext(nil, nil, false, 1, "text", emb, 0.15)
	if err == nil {
		t.Errorf("expected error for nil database in ReinforceFactWithContext")
	}

	// boost <= 0 and ctx == nil
	_ = ReinforceFactWithContext(nil, dbMem, false, 9999, "text", emb, -0.5)

	// 3. DecayAndPruneFactsWithContext nil DB
	_, _, err = DecayAndPruneFactsWithContext(nil, nil, false, 0.02, 0.10, 30)
	if err == nil {
		t.Errorf("expected error for nil database in DecayAndPruneFactsWithContext")
	}

	// default arguments and ctx == nil
	_, _, _ = DecayAndPruneFactsWithContext(nil, dbMem, false, -1, -1, -1)

	// Closed DB
	dbClosed, err := InitDB(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	_ = dbClosed.Close()

	_, _, err = DecayAndPruneFactsWithContext(context.Background(), dbClosed, false, 0.02, 0.10, 30)
	if err == nil {
		t.Errorf("expected error on closed db for DecayAndPruneFacts")
	}

	err = ReinforceFactWithContext(context.Background(), dbClosed, false, 1, "text", emb, 0.15)
	if err == nil {
		t.Errorf("expected error on closed db for ReinforceFact")
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
	err := ReinforceFactWithContext(ctx, mockDB, true, 10, "Postgres Fact", emb, 0.15)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// b. Success without text (boost only)
	err = ReinforceFactWithContext(ctx, mockDB, true, 10, "", nil, 0.15)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// c. 0 rows affected -> ErrFactNotFound
	mockDBZero := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return testCustomResult{rows: 0}, nil
		},
	}
	err = ReinforceFactWithContext(ctx, mockDBZero, true, 999, "", nil, 0.15)
	if !errors.Is(err, ErrFactNotFound) {
		t.Errorf("expected ErrFactNotFound, got %v", err)
	}

	// d. Exec error
	mockDBErr := testMockDBTX{
		execFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, fmt.Errorf("postgres exec error")
		},
	}
	err = ReinforceFactWithContext(ctx, mockDBErr, true, 10, "", nil, 0.15)
	if err == nil {
		t.Errorf("expected error on postgres exec failure")
	}

	// 2. DecayAndPruneFactsWithContext on Postgres
	// a. Success
	decayed, pruned, err := DecayAndPruneFactsWithContext(ctx, mockDB, true, 0.02, 0.10, 30)
	if err != nil || decayed != 1 || pruned != 1 {
		t.Errorf("expected decayed=1 pruned=1, got decayed=%d pruned=%d err=%v", decayed, pruned, err)
	}

	// b. Decay error
	decayed, pruned, err = DecayAndPruneFactsWithContext(ctx, mockDBErr, true, 0.02, 0.10, 30)
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
	decayed, pruned, err = DecayAndPruneFactsWithContext(ctx, mockDBPruneErr, true, 0.02, 0.10, 30)
	if err == nil || decayed != 3 {
		t.Errorf("expected decayed=3 with prune error, got decayed=%d err=%v", decayed, err)
	}
}

func TestSQLStore_ReinforceAndDecay_NilBranches(t *testing.T) {
	ctx := context.Background()
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = 1.0

	// 1. Nil receiver
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

	// 2. Store with nil DB
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
	dup, sim, err := FindDuplicateFactWithContext(ctx, mockNoRows, true, emb, 0.88)
	if err != nil || dup != nil || sim != 0 {
		t.Errorf("expected nil duplicate for sql.ErrNoRows, got dup=%v sim=%f err=%v", dup, sim, err)
	}

	// 2. Query error branch
	mockErr := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT syntax error")
		},
	}
	_, _, err = FindDuplicateFactWithContext(ctx, mockErr, true, emb, 0.88)
	if err == nil {
		t.Errorf("expected error on query failure")
	}

	// 3. Match found with sim >= minSim
	mockMatch := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT 42, 'cat', 'fact', 0.9, 'th1', '2026-09-13 22:00:00', '2026-09-13 22:00:00', '2026-09-13 22:00:00', 2, 0.95")
		},
	}
	dup, sim, err = FindDuplicateFactWithContext(ctx, mockMatch, true, emb, 0.88)
	if err != nil || dup == nil || dup.ID != 42 || sim != 0.95 {
		t.Errorf("expected matched fact id=42 sim=0.95, got dup=%+v sim=%f err=%v", dup, sim, err)
	}

	// 4. Match found with sim < minSim
	mockLowSim := testMockDBTX{
		queryRowFn: func(ctx context.Context, query string, args ...any) *sql.Row {
			return dbMem.QueryRowContext(ctx, "SELECT 43, 'cat', 'fact', 0.9, 'th1', '2026-09-13 22:00:00', '2026-09-13 22:00:00', '2026-09-13 22:00:00', 1, 0.50")
		},
	}
	dup, sim, err = FindDuplicateFactWithContext(ctx, mockLowSim, true, emb, 0.88)
	if err != nil || dup != nil || sim != 0.50 {
		t.Errorf("expected nil duplicate with sim=0.50, got dup=%v sim=%f err=%v", dup, sim, err)
	}
}


